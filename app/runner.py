"""The run engine. A run walks the pipeline's tasks in order, executes real SQL on the warehouse,
applies the pipeline's controls, and records every step so the site can show it live."""
import threading
import time
import traceback

from . import medallion as M
from . import quality as Q
from .config import settings
from .engines import EngineError, get_engine
from .notify import deliver
from .util import now_ms, utcnow, cron_match

_threads = {}


class DataStop(Exception):
    """A check or a control stopped the run on purpose. Retrying will not help until the data or the rule changes."""


class Ctx:
    def __init__(self, store, p, run):
        self.store, self.p, self.run = store, p, run
        self.eng = get_engine(store.connection(p["target"]))
        self.src = store.connection(p["source"]) or store.connection(p["target"])
        self.f = M.fqs(p, self.eng)
        self.n = M.table_names(p)
        self.batch = run["id"]
        self.full = bool(run.get("full")) or p["pattern"] == "full"
        self.task = None
        self.skip_rest = False
        self.drift = []
        self.handled = set()
        self.warnings = []

    def log(self, msg, level="INFO"):
        self.store.log(level, self.p["id"], self.task["id"] if self.task else "", msg, self.run["id"])

    def sql(self, statements):
        for s in statements if isinstance(statements, list) else [statements]:
            self.eng.execute(s)

    def rules(self, layer):
        tables = {w for w in self.p["writes"] if w.startswith(layer + ".")}
        return [r for r in self.store.data["rules"] if r["table"] in tables and r.get("on", True)]

    def mapping(self):
        m = self.store.data["mappings"].get(f"silver.{self.p['table']}")
        return m["rows"] if m else M.default_mapping(self.p)["rows"]

    def stat(self, layer):
        """Refresh the catalog entry of the table this pipeline writes in a layer."""
        name = self.n["bronze"] if layer == "bronze" else self.n[layer]
        tid = f"{layer}.{name}"
        try:
            s = self.eng.stats(layer, name)
            cols = self.eng.columns(layer, name)
        except EngineError:
            return
        with self.store.lock:
            old = self.store.data["tstats"].get(tid, {})
            self.store.data["tstats"][tid] = {"rows": s["rows"], "bytes": s.get("bytes"), "fresh": now_ms(),
                                              "version": int(old.get("version", 0)) + 1, "cols": [[c, t] for c, t in cols]}


# ---------- tasks ----------
def task_bronze(c):
    p, eng, ctl = c.p, c.eng, c.p["ctl"]
    eng.ensure_schemas()
    new_files = []
    if p["kind"] == "files":
        files = eng.list_files(c.src, p)
        new_files = files if c.full else [f for f in files if p["loaded"].get(f["name"]) != f["modified"]]
        c.log(f"Found {len(files)} file{'s' if len(files) != 1 else ''} in the source folder, {len(new_files)} to load")
        if not new_files:
            return _empty(c)
        stage_cols = eng.stage_files(c.src, p, new_files, c.f["stg"])
    else:
        src_fq, src_cols = eng.source_table(p["path"])
        src_cols = [(x, t) for x, t in src_cols if x not in M.LINEAGE and x != "_rescued_data"]   # the source's own lineage is not carried over
        since = None if c.full else p.get("wm")
        c.sql(eng.ctas(c.f["stg"], M.stage_table_sql(p, eng, src_fq, src_cols, since)))
        stage_cols = [x for x, _ in src_cols]
    stage_cols = [x for x in stage_cols if x not in M.LINEAGE and x != "_rescued_data"]
    staged = int(eng.scalar(f"SELECT COUNT(*) AS n FROM {c.f['stg']}") or 0)
    if staged == 0:
        c.sql(eng.drop(c.f["stg"]))
        return _empty(c)

    existing_all = [x for x, _ in eng.columns("bronze", c.n["bronze"])]
    existing = [x for x in existing_all if x not in M.LINEAGE and x != "_rescued_data"] if existing_all else None
    declared = {x["name"] for x in M.parse_cols(p["cfg"].get("columns"))}
    known = set(existing) if existing is not None else declared
    c.drift = [x for x in stage_cols if x not in known]
    if c.drift:
        if ctl.get("onNewColumn") == "stop":
            c.sql(eng.drop(c.f["stg"]))
            raise DataStop(f"new column{'s' if len(c.drift) > 1 else ''} in the source ({', '.join(c.drift)}), and this pipeline is set to stop on schema changes")
        mapped = {r["src"] for r in c.mapping()}
        with c.store.lock:
            for col in c.drift:
                if col in mapped or any(d["table"] == p["writes"][0] and d["col"] == col for d in c.store.data["drift"]):
                    continue
                c.store.data["drift"].insert(0, {"table": p["writes"][0], "col": col, "type": "STRING", "seen": now_ms(), "status": "pending", "pid": p["id"]})
        c.log(f"New column{'s' if len(c.drift) > 1 else ''} in the source: {', '.join(c.drift)}. " +
              ("Kept in the _rescued_data column" if ctl.get("onNewColumn") == "rescue" else "Added to Bronze"), "WARN")
    if existing is not None and ctl.get("onNewColumn") == "rescue" and "_rescued_data" not in existing_all:
        c.sql(eng.add_column(c.f["bronze"], "_rescued_data", "STRING"))
    c.sql(M.bronze_sql(p, eng, stage_cols, existing, c.batch, c.full))
    rows = int(eng.scalar(f"SELECT COUNT(*) AS n FROM {c.f['bronze']} WHERE _batch_id = {eng.lit(c.batch)}") or 0)
    wm = p.get("watermark")
    if p["kind"] == "table" and wm and wm in stage_cols:
        top = eng.scalar(f"SELECT MAX({eng.q(wm)}) AS v FROM {c.f['stg']}")
        if top is not None:
            p["wm"] = str(top)
    c.sql(eng.drop(c.f["stg"]))
    with c.store.lock:
        for f in new_files:
            p["loaded"][f["name"]] = f["modified"]
        if c.batch not in p["pending"]:
            p["pending"] = ([] if c.full else p["pending"]) + [c.batch]
    c.run["rows"]["bronze"] = rows
    c.stat("bronze")
    c.log(f"Landed {rows:,} rows as text with lineage columns")


def _empty(c):
    mode = c.p["ctl"].get("onEmpty", "skip")
    if mode == "fail":
        raise DataStop("the source delivered nothing new, and this pipeline is set to fail on an empty source")
    if mode == "skip":
        c.skip_rest = True
        c.log("The source has nothing new. The run ends here and no table is changed")
        return
    if not c.eng.exists("bronze", c.n["bronze"]):
        raise DataStop("the source is empty and nothing has ever been loaded, so there is nothing to continue with")
    c.log("The source has nothing new. Continuing with no new rows", "WARN")


def _apply(c, rule, checked, failed, note, failures, can_quarantine=False):
    """Record a check result and decide what it does. Returns True when its rows must be quarantined."""
    Q.record(rule, checked, failed)
    label = M.rule_label(rule)
    if not Q.violated(rule, checked, failed):
        c.log(f"Check passed: {label}" + (f" ({note})" if note else "") + (f", {failed:,} of {checked:,} failing is inside the allowed threshold" if failed else ""))
        return False
    what = f"{label} on {rule['table']}: {failed:,} of {checked:,} failed" + (f" ({note})" if note else "")
    sev = rule.get("severity", "warn")
    if sev == "fail":
        failures.append(what)
        c.log(f"Check failed and stops the pipeline: {what}", "ERROR")
        return can_quarantine
    if sev == "quarantine" and can_quarantine:
        c.log(f"Check failed, rows go to quarantine: {what}", "WARN")
        return True
    c.warnings.append(what)
    c.log(f"Check warned: {what}" + ("" if sev == "warn" else ". This check is evaluated after the table is written, so it cannot quarantine rows"), "WARN")
    return False


def task_dq(c, layer):
    p, eng = c.p, c.eng
    gate = layer == "gold" and p["ctl"].get("gate", True)
    ctx = {"batches": [c.batch] if layer == "bronze" else None, "drift": c.drift if layer == "bronze" else None,
           "bronze_rows": c.run["rows"]["bronze"], "silver_rows": c.run["rows"]["silver"], "quarantined": c.run.get("quarantined", 0),
           "in_run": True, "override": {f"gold.{c.n['gold']}": c.f["gold_new"]} if gate else {}}
    failures, n = [], 0
    for rule in c.rules(layer):
        if rule["id"] in c.handled:
            continue
        res = Q.evaluate(rule, eng, p, c.store.data, ctx)
        if res is None:
            continue
        n += 1
        _apply(c, rule, res[0], res[1], res[2], failures)
    if failures:
        if gate:
            c.sql(eng.drop(c.f["gold_new"]))
        raise DataStop("; ".join(failures) + (". Gold was not published, so readers keep the previous version" if gate else ""))
    if gate:
        c.sql(eng.ctas(c.f["gold"], f"SELECT * FROM {c.f['gold_new']}"))
        c.sql(eng.drop(c.f["gold_new"]))
        c.stat("gold")
        c.log("Gold checks passed. The new version is published")
    c.log(f"{n} {layer.capitalize()} check{'s' if n != 1 else ''} evaluated" if n else f"No {layer.capitalize()} checks are switched on")


def task_silver(c):
    p, eng, ctl = c.p, c.eng, c.p["ctl"]
    bronze_cols = [x for x, _ in eng.columns("bronze", c.n["bronze"])]
    if not bronze_cols:
        raise DataStop("the Bronze table does not exist yet")
    mapping = c.mapping()
    row_rules = [r for r in c.rules("silver") if r["type"] in Q.ROW_RULES]
    batches = list(p.get("pending") or [c.batch])
    try:
        statements, flags = M.silver_check_sql(p, eng, mapping, row_rules, bronze_cols, batches, c.full)
        c.sql(statements)
        counts = ", ".join(f"SUM(CASE WHEN {name} THEN 1 ELSE 0 END) AS {name}" for _, name in flags)
        r = eng.query(f"SELECT COUNT(*) AS n{', ' + counts if counts else ''} FROM {c.f['chk']}")[0]
        total = int(r["n"] or 0)
        failures, exclude, reasons = [], [], []
        for rule, name in flags:
            c.handled.add(rule["id"])
            if _apply(c, rule, total, int(r.get(name) or 0), "", failures, can_quarantine=True):
                exclude.append(name)
                reasons.append(f"CASE WHEN {name} THEN {eng.lit(M.rule_label(rule))} END")
        quarantined = 0
        if exclude:
            where = "(" + " OR ".join(exclude) + ")"
            quarantined = int(eng.scalar(f"SELECT COUNT(*) AS n FROM {c.f['chk']} WHERE {where}") or 0)
        share = quarantined / total * 100 if total else 0
        breaker = float(ctl.get("breaker", 5))
        if quarantined and not failures and share > breaker:
            failures.append(f"{share:.1f}% of the rows would be quarantined, above the {breaker:g}% limit")
        if quarantined:
            c.sql(eng.create_if_missing(c.f["quar"], [("_run_id", "STRING"), ("_quarantined_at", "TIMESTAMP"), ("_reason", "STRING"), ("_row", "STRING")]))
            c.sql(f"DELETE FROM {c.f['quar']} WHERE _run_id = {eng.lit(c.run['id'])}")
            c.sql(f"INSERT INTO {c.f['quar']} (_run_id, _quarantined_at, _reason, _row)\n"
                  f"SELECT {eng.lit(c.run['id'])}, {eng.now()}, {eng.join_reasons(reasons)}, _row\nFROM {c.f['chk']}\nWHERE {where}")
            top = eng.query(f"SELECT _reason AS reason, COUNT(*) AS n FROM {c.f['quar']} WHERE _run_id = {eng.lit(c.run['id'])} GROUP BY _reason ORDER BY n DESC LIMIT 1")
            with c.store.lock:
                c.store.data["quarantine"] = [x for x in c.store.data["quarantine"] if x.get("run") != c.run["id"]]
                c.store.data["quarantine"].insert(0, {"table": p["writes"][1], "rows": quarantined, "reason": top[0]["reason"] if top else "",
                                                      "path": c.f["quar"].replace("`", "").replace('"', ""), "ts": now_ms(), "run": c.run["id"], "pid": p["id"]})
            c.run["quarantined"] = quarantined
        if failures:
            raise DataStop("; ".join(failures) + (f". {quarantined:,} bad rows were saved to quarantine with the reason" if quarantined else ""))
        c.sql(M.silver_new_sql(p, eng, mapping, exclude))
        merged = int(eng.scalar(f"SELECT COUNT(*) AS n FROM {c.f['new']}") or 0)
        existing = [x for x, _ in eng.columns("silver", c.n["silver"])] or None
        c.sql(M.silver_write_sql(p, eng, mapping, existing, c.full))
        c.run["rows"]["silver"] = merged
        removed = total - quarantined - merged
        c.log(f"Cast and validated {total:,} rows. {merged:,} merged, {quarantined:,} quarantined" +
              (f", {removed:,} duplicates removed" if removed > 0 else ""))
        with c.store.lock:
            p["pending"] = []
        c.stat("silver")
    finally:
        # the staging tables hold unmasked values, so they never outlive the task
        for key in ("chk", "new"):
            try:
                c.sql(eng.drop(c.f[key]))
            except EngineError:
                pass


def task_gold(c):
    p, eng = c.p, c.eng
    c.sql(M.gold_sql(p, eng))
    target = c.f["gold_new"] if p["ctl"].get("gate", True) else c.f["gold"]
    rows = int(eng.scalar(f"SELECT COUNT(*) AS n FROM {target}") or 0)
    c.run["rows"]["gold"] = rows
    if not p["ctl"].get("gate", True):
        c.stat("gold")
    c.log(f"Built {rows:,} rows" + (". Waiting for the Gold checks before publishing" if p["ctl"].get("gate", True) else " and published"))


def _task_fn(task):
    if task["layer"] == "bronze":
        return task_bronze
    if task["layer"] == "silver":
        return task_silver
    if task["layer"] == "gold":
        return task_gold
    layer = task["id"].replace("dq_", "")
    return lambda c: task_dq(c, layer)


# ---------- run lifecycle ----------
def start_run(store, pid, trigger="manual", full=False, resume=None):
    with store.lock:
        p = store.pipeline(pid)
        if not p:
            raise ValueError(f"There is no pipeline called {pid}")
        if p["status"] == "paused" and trigger != "manual":
            raise ValueError(f"{pid} is paused")
        if any(r["pid"] == pid and r["status"] == "running" for r in store.data["runs"]):
            raise ValueError(f"{pid} already has a run in progress")
        if resume:
            run = resume
            tasks = p["tasks"]
            failed_at = next((i for i, t in enumerate(tasks) if run["tasks"].get(t["id"], {}).get("status") != "success"), 0)
            if tasks[failed_at]["id"] == "dq_gold" and p["ctl"].get("gate", True) and failed_at > 0:
                failed_at -= 1          # the unpublished Gold build was dropped, so build it again
            for t in tasks[failed_at:]:
                run["tasks"][t["id"]] = {"status": "queued", "tries": 0, "start": None, "end": None}
            run.update(status="running", end=None, failMsg="", failedTask=None)
        else:
            run = {"id": store.next_id("run_"), "pid": pid, "start": now_ms(), "end": None, "dur": 0, "status": "running",
                   "trigger": trigger, "rows": {"bronze": 0, "silver": 0, "gold": 0}, "quarantined": 0, "failMsg": "",
                   "failedTask": None, "full": bool(full), "note": "full reload" if full else "",
                   "tasks": {t["id"]: {"status": "queued", "tries": 0, "start": None, "end": None} for t in p["tasks"]}}
            store.data["runs"].insert(0, run)
    store.log("INFO", pid, p["tasks"][0]["id"], f"Run {run['id']} {'resumed' if resume else 'started'} by {trigger}" + (" as a full reload" if full else ""), run["id"])
    store.save(force=True)
    th = threading.Thread(target=_execute, args=(store, pid, run["id"]), daemon=True, name=f"run-{run['id']}")
    _threads[run["id"]] = th
    th.start()
    return run


def _execute(store, pid, run_id):
    p, run = store.pipeline(pid), store.find("runs", run_id)
    try:
        c = Ctx(store, p, run)
    except Exception as e:
        return _finish(store, p, run, None, "failed", str(e))
    fail_msg, status = "", "success"
    for task in p["tasks"]:
        st = run["tasks"][task["id"]]
        if st["status"] == "success":
            continue
        if c.skip_rest:
            st["status"] = "skipped"
            continue
        c.task = task
        fn = _task_fn(task)
        st["start"] = st["start"] or now_ms()
        while True:
            st["status"] = "running"
            st["tries"] += 1
            store.save()
            try:
                fn(c)
                st.update(status="success", end=now_ms())
                break
            except DataStop as e:
                st.update(status="failed", end=now_ms())
                fail_msg = str(e)
                c.log(f"Stopped: {e}", "ERROR")
                break
            except Exception as e:  # warehouse or network problem: worth retrying
                msg = str(e) if isinstance(e, (EngineError, ValueError)) else f"{e.__class__.__name__}: {e}"
                if not isinstance(e, (EngineError, ValueError)):
                    traceback.print_exc()
                if st["tries"] <= p.get("retries", 0):
                    st["status"] = "retry"
                    wait = min(settings.retry_base_seconds * (2 ** (st["tries"] - 1)), 300)
                    c.log(f"Attempt {st['tries']} failed: {msg}. Retry {st['tries']} of {p['retries']} in {wait:g} s", "WARN")
                    store.save()
                    time.sleep(wait)
                    continue
                st.update(status="failed", end=now_ms())
                fail_msg = msg
                c.log(f"Task failed after {st['tries']} attempt{'s' if st['tries'] > 1 else ''}: {msg}", "ERROR")
                break
        if st["status"] == "failed":
            status = "failed"
            run["failedTask"] = task["id"]
            for t in p["tasks"]:
                if run["tasks"][t["id"]]["status"] == "queued":
                    run["tasks"][t["id"]]["status"] = "upstream_failed"
            break
    _finish(store, p, run, c, status, fail_msg)


def _finish(store, p, run, c, status, fail_msg):
    ctl = p["ctl"]
    run.update(status=status, end=now_ms(), failMsg=fail_msg)
    run["dur"] = max(1, round((run["end"] - run["start"]) / 1000))
    last = p["tasks"][-1]["id"]
    if status == "success":
        skipped = c is not None and c.skip_rest
        if not skipped:
            with store.lock:
                p["history"] = (p.get("history") or [])[-19:] + [{"run": run["id"], "rows": run["rows"]["bronze"]}]
        store.log("INFO", p["id"], last, f"Run {run['id']} ended early: nothing new to load" if skipped else
                  f"Run {run['id']} succeeded in {run['dur']} s. {run['rows']['bronze']:,} rows landed, {run['rows']['silver']:,} merged, {run.get('quarantined', 0):,} quarantined", run["id"])
        notes = list(c.warnings) if c else []
        if run.get("quarantined"):
            notes.insert(0, f"{run['quarantined']:,} rows quarantined from {p['writes'][1]}")
        if notes:
            _alert(store, p, "warn", f"{p['name']}: {notes[0]}", " ".join(notes[1:3]) or "The run continued.", ctl.get("alertWarn"), "quality")
        elif ctl.get("alertOk") and not skipped:
            _alert(store, p, "info", f"{p['name']} succeeded", f"{run['rows']['bronze']:,} rows landed.", True, "orchestration", store_it=False)
    else:
        if not run.get("failedTask"):
            run["failedTask"] = p["tasks"][0]["id"]
        store.log("ERROR", p["id"], run["failedTask"], f"Run {run['id']} failed at {run['failedTask']}: {fail_msg}", run["id"])
        _alert(store, p, "bad", f"{p['name']} failed at {run['failedTask']}", f"{fail_msg[:1].upper() + fail_msg[1:]}. Everything downstream was held.",
               ctl.get("alertFail"), "orchestration")
    store.save(force=True)
    _threads.pop(run["id"], None)


def _alert(store, p, sev, title, detail, enabled, view, store_it=True):
    if not enabled:
        store.log("WARN", p["id"], "alerts", f"Not alerted (switched off for this pipeline): {title}")
        return
    sent, missing = deliver(title, detail, p["ctl"])
    where = (" Sent to " + ", ".join(sent) + "." if sent else "") + (" Not delivered to " + ", ".join(missing) + " because that channel is not configured." if missing else "")
    if store_it:
        store.alert("bad" if sev == "bad" else "warn", title, detail + where, view, p["id"])


def retry_run(store, run_id):
    run = store.find("runs", run_id)
    if not run or run["status"] != "failed":
        raise ValueError("Only a failed run can be retried")
    return start_run(store, run["pid"], trigger=run.get("trigger", "manual"), resume=run)


# ---------- scheduler ----------
def tick(store):
    """Start every pipeline whose cron matches this minute, and raise freshness alerts. Safe to call often."""
    t = utcnow().replace(second=0, microsecond=0)
    key = t.strftime("%Y-%m-%dT%H:%M")
    started = []
    for p in list(store.data["pipelines"]):
        if p["status"] != "active" or p.get("lastSched") == key or not cron_match(p.get("cron"), t):
            continue
        p["lastSched"] = key
        try:
            start_run(store, p["id"], trigger="schedule")
            started.append(p["id"])
        except ValueError as e:
            store.log("WARN", p["id"], "scheduler", f"Scheduled run skipped: {e}")
    _freshness(store)
    return started


def _freshness(store):
    for rule in store.data["rules"]:
        if rule["type"] != "freshness" or not rule.get("on", True):
            continue
        p = next((x for x in store.data["pipelines"] if rule["table"] in x["writes"]), None)
        fresh = (store.data["tstats"].get(rule["table"]) or {}).get("fresh")
        if not p or not fresh:
            continue
        age, limit = (now_ms() - fresh) / 60000, Q.minutes(rule.get("param"))
        late = age > limit
        was_late = rule.get("pass", 100) < 100
        Q.record(rule, 1, 1 if late else 0)
        if late and not was_late:
            _alert(store, p, "warn", f"{rule['table']} is late", f"Last written {age:.0f} minutes ago against a target of {limit:.0f}.", p["ctl"].get("alertSla"), "monitoring")


def scheduler_loop(store):
    while True:
        try:
            tick(store)
            store.flush_if_dirty()
        except Exception:
            traceback.print_exc()
        time.sleep(20)
