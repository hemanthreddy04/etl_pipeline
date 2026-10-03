"""Everything the site, the MCP endpoint and the agent can ask the control plane to do."""
import io
import json
import os
import re
import zipfile

import requests

from . import medallion as M
from . import quality as Q
from . import runner
from .cloudauth import gcp_headers, secret_status
from .config import settings
from .engines import ENGINE_TYPES, EngineError, get_engine, norm_type
from .util import cron_valid, ident, now_ms

FILE_TYPES = {"folder": "local", "gcs": "bq", "adls": "adb"}       # file connection -> engine that reads it
DEFAULT_ROLES = [
    {"id": "data_engineer", "name": "Data engineers", "principal": "", "acc": {"bronze": "write", "silver": "write", "gold": "write"}},
    {"id": "analytics_engineer", "name": "Analytics engineers", "principal": "", "acc": {"bronze": "none", "silver": "read", "gold": "write"}},
    {"id": "data_scientist", "name": "Data scientists", "principal": "", "acc": {"bronze": "none", "silver": "read", "gold": "read"}},
    {"id": "analyst", "name": "Analysts and BI tools", "principal": "", "acc": {"bronze": "none", "silver": "none", "gold": "read"}},
]


class NotFound(ValueError):
    pass


def _pipeline(store, pid):
    p = store.pipeline(pid)
    if not p:
        raise NotFound(f"There is no pipeline called {pid}")
    return p


def _engine(store, p):
    return get_engine(store.connection(p["target"]))


def _pipeline_of(store, table_id):
    p = next((x for x in store.data["pipelines"] if table_id in x["writes"]), None)
    if not p:
        raise NotFound(f"No pipeline writes {table_id}")
    return p


# ---------- what the site shows ----------
def tables_view(store):
    out = []
    for p in store.data["pipelines"]:
        conn = store.connection(p["target"]) or {}
        cls = ENGINE_TYPES.get(conn.get("type"))
        declared = M.parse_cols(p["cfg"].get("columns"))
        mapping = (store.data["mappings"].get(p["writes"][1]) or {}).get("rows", [])
        pii_src = {r["src"] for r in mapping if r.get("pii")} | {c["name"] for c in declared if c["pii"]}
        pii_tgt = {r["tgt"] for r in mapping if r.get("pii")}
        names = M.table_names(p)
        gold_kind = (p["cfg"].get("gold") or {}).get("type") or "aggregate"
        spec = {
            "bronze": (names["bronze"], [(c["name"], "STRING") for c in declared] + [(x, "TIMESTAMP" if x == "_ingest_ts" else "DATE" if x == "_ingest_date" else "STRING") for x in M.LINEAGE],
                       "Full reload each run" if p["pattern"] == "full" else "Incremental append, new files or rows only", ["src:" + p["id"]]),
            "silver": (names["silver"], M.silver_columns(p, mapping) if mapping else [],
                       "SCD Type 2, full history" if p["scd"] == "2" else f"Upsert on {', '.join(p['keys'])} (SCD Type 1)", [p["writes"][0]]),
            "gold": (names["gold"], [], {"aggregate": "Rebuilt each run", "dimension": "Rebuilt each run", "sql": "Rebuilt each run from your SQL"}[gold_kind], [p["writes"][1]]),
        }
        for layer, (name, cols, pattern, up) in spec.items():
            tid = f"{layer}.{name}"
            st = store.data["tstats"].get(tid, {})
            live = st.get("cols")
            cols = [(c, t) for c, t in live] if live else cols
            pii = pii_src if layer == "bronze" else pii_tgt
            out.append({
                "id": tid, "layer": layer, "name": name, "platform": p["platform"], "owner": p.get("owner", "data-eng"),
                "tags": (["pii"] if any(c in pii for c, _ in cols) else []) + ([] if st else ["not built yet"]),
                "version": st.get("version", 0), "up": up, "fmt": cls.table_format if cls else "Table", "part": "None", "cluster": "None",
                "pattern": pattern, "rows": st.get("rows", 0), "gb": (st.get("bytes") or 0) / 1e9, "fresh": st.get("fresh") or 0,
                "ret": "", "built": bool(st), "pid": p["id"],
                "cols": [{"name": c, "type": str(t), "key": c in p["keys"] and layer != "gold", "pii": c in pii,
                          "mask": "hash" if c in pii and layer != "bronze" and p["ctl"].get("mask") else "none", "desc": ""} for c, t in cols],
            })
    return out


def state_view(store):
    d = store.data
    return {
        "settings": settings.public(), "connections": d["connections"], "pipelines": d["pipelines"], "tables": tables_view(store),
        "sources": [{"id": "src:" + p["id"], "name": p["path"], "conn": p["source"]} for p in d["pipelines"]],
        "runs": d["runs"][:300], "logs": d["logs"][:500], "alerts": d["alerts"], "rules": d["rules"], "quarantine": d["quarantine"],
        "mappings": d["mappings"], "drift": d["drift"], "audit": d["audit"][:200], "approvals": d["approvals"][:40],
        "roles": d["roles"] or DEFAULT_ROLES, "now": now_ms(),
    }


# ---------- connections ----------
def save_connection(store, body, create):
    cid = ident(str(body.get("id", "")).strip().lower(), "connection name")
    typ = body.get("type")
    if typ not in ENGINE_TYPES and typ not in FILE_TYPES:
        raise ValueError("Choose a connection type")
    endpoint = str(body.get("endpoint") or "").strip()
    if not endpoint and typ != "bq":
        raise ValueError("Fill in where this system is")
    options = {k: str(v).strip() for k, v in (body.get("options") or {}).items() if str(v).strip()}
    with store.lock:
        existing = store.connection(cid)
        if create and existing:
            raise ValueError(f"A connection named {cid} already exists")
        if not create and not existing:
            raise NotFound(f"There is no connection called {cid}")
        conn = existing or {"id": cid, "status": "idle", "tested": 0, "note": ""}
        conn.update(type=typ, endpoint=endpoint, auth=str(body.get("auth") or ""), secret=str(body.get("secret") or "").strip(), options=options,
                    cloud={"local": "local", "folder": "local", "bq": "gcp", "gcs": "gcp", "adb": "azure", "adls": "azure"}[typ])
        if not existing:
            store.data["connections"].append(conn)
    store.audit("Added connection" if create else "Edited connection", cid, typ)
    return test_connection(store, cid)


def test_connection(store, cid):
    conn = store.connection(cid)
    if not conn:
        raise NotFound(f"There is no connection called {cid}")
    start = now_ms()
    try:
        if conn["type"] in ENGINE_TYPES:
            eng = get_engine(conn)
            note = eng.test()
            eng.ensure_schemas()
        elif conn["type"] == "folder":
            if not os.path.isdir(conn["endpoint"]):
                raise EngineError(f"Folder {conn['endpoint']} does not exist on the machine running this service")
            note = f"{len(os.listdir(conn['endpoint']))} entries in the folder"
        elif conn["type"] == "gcs":
            bucket = conn["endpoint"].replace("gs://", "").strip("/").split("/")[0]
            r = requests.get(f"https://storage.googleapis.com/storage/v1/b/{bucket}/o", params={"maxResults": 1}, headers=gcp_headers(), timeout=30)
            if not r.ok:
                raise EngineError(f"Bucket {bucket} is not readable ({r.status_code}). The service account needs Storage Object Viewer on it.")
            note = "Bucket is readable"
        else:
            adb = next((c for c in store.data["connections"] if c["type"] == "adb"), None)
            if not adb:
                raise EngineError("Storage paths are read through a Databricks SQL warehouse. Add the Databricks connection first.")
            rows = get_engine(adb).query(f"LIST {get_engine(adb).lit(conn['endpoint'].rstrip('/') + '/')}")
            note = f"{len(rows)} entries listed through {adb['id']}"
        conn.update(status="healthy", note=note)
    except Exception as e:
        conn.update(status="down", note=str(e)[:400])
    conn["tested"] = now_ms()
    conn["ms"] = now_ms() - start
    store.log("INFO" if conn["status"] == "healthy" else "ERROR", "connections", cid, f"Connection test {'passed' if conn['status'] == 'healthy' else 'failed'} in {conn['ms']} ms: {conn['note']}")
    store.save()
    return conn


def delete_connection(store, cid):
    used = [p["id"] for p in store.data["pipelines"] if cid in (p["target"], p["source"])]
    if used:
        raise ValueError(f"{cid} is used by {', '.join(used)}. Delete or repoint those pipelines first.")
    with store.lock:
        store.data["connections"] = [c for c in store.data["connections"] if c["id"] != cid]
    store.audit("Removed connection", cid)


# ---------- pipelines ----------
def _new_rule(store, table, column, typ, param, severity):
    return {"id": store.next_id("dq"), "table": table, "column": column or "*", "type": typ, "param": param or "", "severity": severity,
            "on": True, "min": 100, "pass": 100, "checked": 0, "failed": 0, "last": 0}


def _prepare(store, cfg):
    target = store.connection(cfg.get("target") or "")
    if not target or target["type"] not in ENGINE_TYPES:
        raise ValueError("Choose the warehouse this pipeline writes to")
    cls = ENGINE_TYPES[target["type"]]
    p = M.build_pipeline(cfg, target, cls)
    if p["kind"] == "files":
        src = store.connection(p["source"])
        if not src or FILE_TYPES.get(src["type"]) != target["type"]:
            need = {"local": "a local folder", "bq": "a Cloud Storage bucket", "adb": "an ADLS or cloud storage path"}[target["type"]]
            raise ValueError(f"Files for {cls.label} come from {need} connection. Choose one as the source.")
    else:
        p["source"] = target["id"]
    if not cron_valid(p["cron"]):
        raise ValueError("The schedule is not a valid five field cron expression")
    return p, target


def deploy_pipeline(store, cfg, by="pipeline builder"):
    p, target = _prepare(store, cfg)
    with store.lock:
        if store.pipeline(p["id"]):
            raise ValueError(f"{p['id']} already exists. Use another dataset name, or delete the existing pipeline.")
        clash = next((x["id"] for x in store.data["pipelines"] if set(x["writes"]) & set(p["writes"])), None)
        if clash:
            raise ValueError(f"{clash} already writes one of these tables. Choose another Gold table name.")
        p["created"] = now_ms()
        gold_sql_check = M.gold_select(p, get_engine(target))          # fails early on a bad Gold definition
        del gold_sql_check
        store.data["pipelines"].append(p)
        store.data["mappings"][p["writes"][1]] = M.default_mapping(p)
        for t, c, ty, pa, sev in M.default_rules(p):
            store.data["rules"].append(_new_rule(store, t, c, ty, pa, sev))
    store.audit("Deployed pipeline", p["id"], f"By {by}. Writes {', '.join(p['writes'])}")
    store.log("INFO", p["id"], p["tasks"][0]["id"], f"Pipeline {p['id']} registered on {p['engine']}")
    return p


def plan_sql(store, cfg=None, pid=None):
    if pid:
        p = _pipeline(store, pid)
        mapping = (store.data["mappings"].get(p["writes"][1]) or M.default_mapping(p))["rows"]
        rules = store.data["rules"]
    else:
        p, _ = _prepare(store, cfg)
        mapping = M.default_mapping(p)["rows"]
        rules = [dict(zip(("table", "column", "type", "param", "severity"), r), id=f"dq{i}", on=True, min=100) for i, r in enumerate(M.default_rules(p))]
    return M.plan(p, _engine(store, p), mapping, rules)


def delete_pipeline(store, pid, drop_tables=False):
    p = _pipeline(store, pid)
    if any(r["pid"] == pid and r["status"] == "running" for r in store.data["runs"]):
        raise ValueError("Wait for the run in progress to finish before deleting this pipeline")
    dropped = []
    if drop_tables:
        eng, f = _engine(store, p), None
        f = M.fqs(p, eng)
        for key in ("bronze", "stg", "silver", "chk", "new", "quar", "gold", "gold_new"):
            try:
                eng.execute(eng.drop(f[key]))
                if key in ("bronze", "silver", "gold", "quar"):
                    dropped.append(f[key])
            except EngineError as e:
                store.log("WARN", pid, "delete", f"Could not drop {f[key]}: {e}")
    with store.lock:
        d = store.data
        d["pipelines"] = [x for x in d["pipelines"] if x["id"] != pid]
        gone = set(p["writes"])
        d["rules"] = [r for r in d["rules"] if r["table"] not in gone]
        d["quarantine"] = [q for q in d["quarantine"] if q["table"] not in gone]
        d["drift"] = [x for x in d["drift"] if x["table"] not in gone]
        d["runs"] = [r for r in d["runs"] if r["pid"] != pid]
        d["alerts"] = [a for a in d["alerts"] if a.get("pid") != pid]
        for t in gone:
            d["mappings"].pop(t, None)
            d["tstats"].pop(t, None)
    store.audit("Deleted pipeline", pid, "Tables dropped" if drop_tables else "Tables kept in the warehouse")
    return {"dropped": dropped}


def set_schedule(store, pid, body):
    p = _pipeline(store, pid)
    cron = str(body.get("cron") or p["cron"]).strip()
    if not cron_valid(cron):
        raise ValueError("That is not a valid cron expression. Use five fields, for example 0 * * * *")
    p.update(cron=cron, retries=max(0, min(5, int(body.get("retries", p["retries"]) or 0))), sla=max(1, int(body.get("sla", p["sla"]) or p["sla"])))
    store.audit("Changed schedule", pid, f"{cron}, {p['retries']} retries")
    return p


def toggle_pipeline(store, pid):
    p = _pipeline(store, pid)
    p["status"] = "active" if p["status"] == "paused" else "paused"
    store.audit("Paused pipeline" if p["status"] == "paused" else "Resumed pipeline", pid)
    return p


def set_control(store, pid, key, value):
    p = _pipeline(store, pid)
    if key == "retries":
        p["retries"] = max(0, min(5, int(float(value or 0))))
    elif key == "breaker":
        p["ctl"]["breaker"] = max(0.0, min(100.0, float(value or 0)))
    elif key in ("onNewColumn", "onEmpty"):
        allowed = {"onNewColumn": ("accept", "rescue", "stop"), "onEmpty": ("skip", "continue", "fail")}[key]
        if value not in allowed:
            raise ValueError(f"{key} must be one of {', '.join(allowed)}")
        p["ctl"][key] = value
    elif key in M.DEFAULT_CTL:
        p["ctl"][key] = bool(value)
    else:
        raise ValueError(f"Unknown control {key}")
    store.audit("Changed control", pid, f"{key} = {p['retries'] if key == 'retries' else p['ctl'][key]}")
    return p


# ---------- mappings and checks ----------
def save_mapping(store, table, rows):
    p = _pipeline_of(store, table)
    clean, seen = [], set()
    for r in rows:
        tgt = ident(str(r.get("tgt") or "").strip(), "target column")
        if tgt in seen:
            raise ValueError(f"Target column {tgt} is mapped twice")
        seen.add(tgt)
        clean.append({"src": str(r.get("src") or "").strip(), "stype": "STRING", "tgt": tgt, "ttype": norm_type(r.get("ttype")),
                      "tx": str(r.get("tx") or "trim").strip(), "nullable": bool(r.get("nullable", True)), "pii": bool(r.get("pii"))})
    missing = [k for k in p["keys"] if k not in seen]
    if missing:
        raise ValueError(f"The business key {', '.join(missing)} must stay in the mapping")
    with store.lock:
        store.data["mappings"][table] = {"source": p["writes"][0], "rows": clean}
        for r in clean:   # a required column gets a not-null check if it has none
            if not r["nullable"] and not any(x["table"] == table and x["column"] == r["tgt"] and x["type"] == "not_null" for x in store.data["rules"]):
                store.data["rules"].append(_new_rule(store, table, r["tgt"], "not_null", "", "fail"))
    store.audit("Saved mapping", table, f"{len(clean)} columns")
    return store.data["mappings"][table]


def add_rule(store, body):
    table = str(body.get("table") or "")
    p = _pipeline_of(store, table)
    typ = body.get("type")
    if typ not in Q.DIMENSION:
        raise ValueError("Choose a check type")
    col = str(body.get("column") or "*").strip()
    if col != "*":
        ident(col, "column")
    severity = body.get("severity") if body.get("severity") in ("fail", "quarantine", "warn") else "warn"
    rule = _new_rule(store, table, col, typ, str(body.get("param") or "").strip(), severity)
    if typ in Q.ROW_RULES and typ != "custom_sql" and col == "*":
        raise ValueError("This check needs a column name")
    if M.rule_flag(rule, _engine(store, p), "x") is None and typ in ("range", "accepted_values", "regex", "referential"):
        raise ValueError({"range": "Give the range as two numbers, for example 0 to 100000", "accepted_values": "List the allowed values, separated by commas",
                          "regex": "Give the pattern the value must match", "referential": "Name the reference as layer.table.column"}[typ])
    with store.lock:
        store.data["rules"].append(rule)
    store.audit("Added check", f"{table}.{col}", typ, who=body.get("_by", "you"))
    return rule


def update_rule(store, rid, body):
    r = store.find("rules", rid)
    if not r:
        raise NotFound("That check no longer exists")
    if "on" in body:
        r["on"] = bool(body["on"])
    if "min" in body:
        r["min"] = max(0.0, min(100.0, float(body["min"] or 0)))
    if body.get("severity") in ("fail", "quarantine", "warn"):
        r["severity"] = body["severity"]
    store.audit("Changed check", f"{r['table']}.{r['column']}", f"{r['type']}: on={r['on']}, pass at least {r['min']:g}%, on failure {r['severity']}")
    return r


def delete_rule(store, rid):
    with store.lock:
        r = store.find("rules", rid)
        store.data["rules"] = [x for x in store.data["rules"] if x["id"] != rid]
    if r:
        store.audit("Removed check", f"{r['table']}.{r['column']}", r["type"])


def recommend_rules(store, pid, layer):
    p = _pipeline(store, pid)
    mapping = {t: m["rows"] for t, m in store.data["mappings"].items()}
    added = 0
    with store.lock:
        for t, c, ty, pa, sev in Q.recommended(p, layer, mapping):
            if any(r["table"] == t and r["column"] == c and r["type"] == ty for r in store.data["rules"]):
                continue
            store.data["rules"].append(_new_rule(store, t, c, ty, pa, sev))
            added += 1
    store.audit("Added recommended checks", pid, f"{added} on {layer}")
    return {"added": added}


def run_checks(store, table=None):
    """Evaluate every enabled check against the tables as they are now. Nothing is written or quarantined."""
    done, problems, skipped = 0, 0, 0
    for rule in store.data["rules"]:
        if not rule.get("on", True) or (table and rule["table"] != table):
            continue
        p = next((x for x in store.data["pipelines"] if rule["table"] in x["writes"]), None)
        if not p or rule["table"] not in store.data["tstats"]:
            skipped += 1
            continue
        try:
            res = Q.evaluate(rule, _engine(store, p), p, store.data)
        except EngineError as e:
            store.log("WARN", p["id"], "checks", f"Check {M.rule_label(rule)} on {rule['table']} could not run: {e}")
            skipped += 1
            continue
        if res is None:
            skipped += 1
            continue
        Q.record(rule, res[0], res[1])
        done += 1
        problems += 1 if Q.violated(rule, res[0], res[1]) else 0
    store.audit("Ran checks", table or "all tables", f"{done} evaluated, {problems} failing, {skipped} not applicable yet")
    return {"evaluated": done, "failing": problems, "skipped": skipped}


# ---------- tables, quarantine, drift ----------
def _table_ref(store, table_id):
    p = _pipeline_of(store, table_id)
    layer, name = table_id.split(".", 1)
    return p, _engine(store, p), layer, name


def preview_table(store, table_id, limit=5):
    p, eng, layer, name = _table_ref(store, table_id)
    if not eng.exists(layer, name):
        return {"columns": [], "rows": [], "note": "This table has not been built yet. Run the pipeline first."}
    rows = eng.query(f"SELECT * FROM {eng.fq(layer, name)} LIMIT {max(1, min(50, int(limit)))}")
    return {"columns": list(rows[0].keys()) if rows else [c for c, _ in eng.columns(layer, name)], "rows": rows, "note": ""}


def refresh_tables(store):
    for p in store.data["pipelines"]:
        try:
            eng = _engine(store, p)
        except EngineError:
            continue
        names = M.table_names(p)
        for layer in ("bronze", "silver", "gold"):
            name = names["bronze"] if layer == "bronze" else names[layer]
            tid = f"{layer}.{name}"
            try:
                cols = eng.columns(layer, name)
                if not cols:
                    continue
                s = eng.stats(layer, name)
            except EngineError:
                continue
            old = store.data["tstats"].get(tid, {})
            store.data["tstats"][tid] = {"rows": s["rows"], "bytes": s.get("bytes"), "fresh": old.get("fresh") or now_ms(),
                                         "version": old.get("version", 1), "cols": [[c, t] for c, t in cols]}
    store.save()


def quarantine_rows(store, table_id, run_id):
    p, eng, _, name = _table_ref(store, table_id)
    f = M.fqs(p, eng)
    rows = eng.query(f"SELECT _reason, _row FROM {f['quar']} WHERE _run_id = {eng.lit(run_id)} LIMIT 50")
    out = []
    for r in rows:
        try:
            data = json.loads(r["_row"])
        except (TypeError, ValueError):
            data = {"row": r["_row"]}
        out.append({"reason": r["_reason"], "row": data})
    return {"rows": out}


def discard_quarantine(store, table_id, run_id):
    p, eng, _, _ = _table_ref(store, table_id)
    f = M.fqs(p, eng)
    eng.execute(f"DELETE FROM {f['quar']} WHERE _run_id = {eng.lit(run_id)}")
    with store.lock:
        store.data["quarantine"] = [q for q in store.data["quarantine"] if not (q["table"] == table_id and q.get("run") == run_id)]
    store.audit("Discarded quarantined rows", table_id, run_id)


def drift_action(store, table_id, col, action):
    d = next((x for x in store.data["drift"] if x["table"] == table_id and x["col"] == col), None)
    if not d:
        raise NotFound("That column is no longer waiting for a decision")
    p = _pipeline_of(store, table_id)
    if action == "map":
        silver = p["writes"][1]
        m = store.data["mappings"].setdefault(silver, M.default_mapping(p))
        if not any(r["tgt"] == col for r in m["rows"]):
            m["rows"].append({"src": col, "stype": "STRING", "tgt": ident(col, "column"), "ttype": "STRING", "tx": "trim", "nullable": True, "pii": False})
        d["status"] = "mapped"
        store.audit("Mapped new column", silver, f"{col}. It is filled from the next run on")
    else:
        d["status"] = "ignored"
        store.audit("Kept new column in Bronze only", table_id, col)
    return d


# ---------- compute and governance ----------
def compute_view(store):
    out = []
    for conn in store.data["connections"]:
        if conn["type"] not in ENGINE_TYPES:
            continue
        try:
            items = get_engine(conn).compute()
        except Exception as e:
            items = [{"id": conn["id"], "kind": ENGINE_TYPES[conn["type"]].label, "spec": str(e)[:200], "state": "down", "canToggle": False}]
        used = [p["id"] for p in store.data["pipelines"] if p["target"] == conn["id"]]
        for it in items:
            out.append({**it, "conn": conn["id"], "platform": ENGINE_TYPES[conn["type"]].platform, "used": used})
    return out


def toggle_compute(store, cid, wid, start):
    conn = store.connection(cid)
    eng = get_engine(conn)
    if not hasattr(eng, "toggle_compute"):
        raise ValueError("This engine has no compute to start or stop")
    eng.toggle_compute(wid, start)
    store.audit("Started compute" if start else "Stopped compute", f"{cid}/{wid}")


def grants(store, apply=False, roles=None):
    """Turn the access matrix into GRANT statements for each warehouse, and optionally run them."""
    if roles is not None:
        store.data["roles"] = roles
        store.audit("Changed access matrix", "roles")
    roles = store.data["roles"] or DEFAULT_ROLES
    out = []
    for conn in store.data["connections"]:
        if conn["type"] not in ("bq", "adb"):
            continue
        eng = get_engine(conn)
        for role in roles:
            who = (role.get("principal") or "").strip()
            if not who:
                continue
            for layer in ("bronze", "silver", "gold"):
                acc = role["acc"].get(layer, "none")
                if acc == "none":
                    continue
                if conn["type"] == "bq":
                    r = "roles/bigquery.dataEditor" if acc == "write" else "roles/bigquery.dataViewer"
                    out.append({"conn": conn["id"], "sql": f"GRANT `{r}` ON SCHEMA `{eng.project}.{eng.schema(layer)}` TO {json.dumps(who)}"})
                else:
                    privs = "USE SCHEMA, SELECT, MODIFY" if acc == "write" else "USE SCHEMA, SELECT"
                    out.append({"conn": conn["id"], "sql": f"GRANT {privs} ON SCHEMA `{eng.catalog}`.`{eng.schema(layer)}` TO `{who.replace('`', '')}`"})
    if apply:
        for g in out:
            try:
                get_engine(store.connection(g["conn"])).execute(g["sql"])
                g["result"] = "applied"
            except EngineError as e:
                g["result"] = f"failed: {e}"
        store.audit("Applied grants", "access", f"{sum(1 for g in out if g.get('result') == 'applied')} of {len(out)} statements applied")
    return {"statements": out}


def erase(store, column, value, confirm=False):
    """Delete every row for one person from Silver and Gold tables that carry the given column."""
    ident(column, "column")
    plan = []
    for t in tables_view(store):
        if t["layer"] == "bronze" or not t["built"] or not any(c["name"] == column for c in t["cols"]):
            continue
        p, eng, layer, name = _table_ref(store, t["id"])
        fq = eng.fq(layer, name)
        n = int(eng.scalar(f"SELECT COUNT(*) AS n FROM {fq} WHERE {eng.to_str(eng.q(column))} = {eng.lit(value)}") or 0)
        plan.append({"table": t["id"], "rows": n})
        if confirm and n:
            eng.execute(f"DELETE FROM {fq} WHERE {eng.to_str(eng.q(column))} = {eng.lit(value)}")
    if confirm:
        store.audit("Erased a person", column, f"{sum(x['rows'] for x in plan)} rows deleted from {len([x for x in plan if x['rows']])} tables")
    return {"tables": plan, "done": bool(confirm)}


def secrets_view(store):
    out = []
    for c in store.data["connections"]:
        if c.get("secret"):
            out.append({"name": c["secret"], "used": c["id"], "found": secret_status(c["secret"])})
    return out


def export_zip(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in files:
            path = re.sub(r"\.\.+", ".", str(f.get("path") or "file.txt")).lstrip("/")
            z.writestr(path, str(f.get("content") or ""))
    return buf.getvalue()


def list_source_files(store, cid, path, fmt="csv"):
    src = store.connection(cid)
    if not src or src["type"] not in FILE_TYPES:
        raise ValueError("Choose a file connection")
    eng_conn = next((c for c in store.data["connections"] if c["type"] == FILE_TYPES[src["type"]]), None)
    if not eng_conn:
        raise ValueError("Add the warehouse connection that reads this storage first")
    files = get_engine(eng_conn).list_files(src, {"path": path, "format": fmt})
    return {"files": files[:200], "total": len(files)}


def detect_columns(store, cfg):
    """Look at the real source and suggest the column list: names from the file header or table, types from the values."""
    target = store.connection(cfg.get("target") or "")
    if not target or target["type"] not in ENGINE_TYPES:
        raise ValueError("Choose the warehouse first")
    eng = get_engine(target)
    if cfg.get("kind") == "table":
        _, cols = eng.source_table(str(cfg.get("path") or ""))
        cols = [(c, norm_type(t)) for c, t in cols if c not in M.LINEAGE]
    else:
        src = store.connection(cfg.get("conn") or "")
        if not src or FILE_TYPES.get(src["type"]) != target["type"]:
            raise ValueError("Choose the file connection that belongs to this warehouse")
        files = eng.list_files(src, cfg)
        if not files:
            raise ValueError("No files of that format were found in the folder")
        eng.ensure_schemas()
        stage = eng.fq("bronze", ident(M.slug(cfg.get("name") or "dataset"), "dataset name") + "__peek")
        try:
            names = eng.stage_files(src, cfg, files[-1:], stage)
            probes = []
            for c in names:
                v = f"NULLIF(TRIM({eng.q(c)}), '')"
                probes.append(f"SUM(CASE WHEN {v} IS NOT NULL THEN 1 ELSE 0 END) AS {eng.q('n_' + c)}")
                for tag, typ in (("i", "BIGINT"), ("d", "DECIMAL(18,2)"), ("t", "TIMESTAMP"), ("b", "BOOLEAN")):
                    probes.append(f"SUM(CASE WHEN {eng.try_cast(v, typ)} IS NOT NULL THEN 1 ELSE 0 END) AS {eng.q(tag + '_' + c)}")
                probes.append(f"SUM(CASE WHEN LENGTH({v}) = 10 AND {eng.try_cast(v, 'DATE')} IS NOT NULL THEN 1 ELSE 0 END) AS {eng.q('y_' + c)}")
            r = eng.query(f"SELECT {', '.join(probes)} FROM {stage}")[0] if names else {}
            cols = []
            for c in names:
                n = int(r.get("n_" + c) or 0)
                hit = lambda tag: n > 0 and int(r.get(tag + "_" + c) or 0) >= 0.9 * n   # noqa: E731  (the few misfits are what the checks catch)
                cols.append((c, "BIGINT" if hit("i") else "DECIMAL(18,2)" if hit("d") else "DATE" if hit("y") else "TIMESTAMP" if hit("t") else "STRING"))
        finally:
            eng.execute(eng.drop(stage))
    key = next((c for c, _ in cols if c.lower() == "id" or c.lower().endswith("_id")), cols[0][0] if cols else "")
    lines = [f"{c}:{t}" + (":key" if c == key else ":pii" if re.search(r"email|phone|full_name|first_name|last_name|address", c, re.I) else "") for c, t in cols]
    return {"columns": "\n".join(lines), "count": len(cols)}


def start(store, pid, full=False, trigger="manual"):
    run = runner.start_run(store, pid, trigger=trigger, full=full)
    store.audit("Triggered run", pid, run["id"] + (", full reload" if full else ""), who="you" if trigger == "manual" else trigger)
    return run
