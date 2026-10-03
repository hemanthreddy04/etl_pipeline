"""The control plane's tools. The same list serves the MCP endpoint, the built-in agent and the Try button
on the site. A tool that changes something waits for a person's approval unless that is switched off."""
import re

from . import runner, service
from .engines import get_engine
from .util import now_ms

STR = {"type": "string"}


def _obj(props, required=()):
    return {"type": "object", "properties": props, "required": list(required)}


def _run_sql(store, a):
    sql = str(a.get("sql") or "").strip().rstrip(";")
    if not re.match(r"^(select|with)\b", sql, re.I) or ";" in sql:
        raise ValueError("Only a single SELECT statement is allowed through this tool")
    conn = store.connection(a.get("connection") or "") or next((c for c in store.data["connections"] if c["type"] in ("local", "bq", "adb")), None)
    if not conn:
        raise ValueError("There is no warehouse connection yet")
    rows = get_engine(conn).query(f"SELECT * FROM ({sql}) q LIMIT 50")
    return {"connection": conn["id"], "rows": rows, "row_count": len(rows)}


def _run_status(store, a):
    pid = a.get("pipeline")
    run = next((r for r in store.data["runs"] if r["pid"] == pid), None)
    if not run:
        return {"pipeline": pid, "state": "never run"}
    return {"pipeline": pid, "run": run["id"], "state": run["status"], "failed_task": run.get("failedTask"), "reason": run.get("failMsg"),
            "rows": run["rows"], "quarantined": run.get("quarantined", 0), "tasks": {k: v["status"] for k, v in run["tasks"].items()}}


TOOLS = [
    {"name": "list_pipelines", "write": False, "desc": "List pipelines with their schedule, state and last run.", "schema": _obj({}),
     "fn": lambda s, a: {"pipelines": [{"id": p["id"], "engine": p["engine"], "schedule": p["cron"], "state": p["status"], "writes": p["writes"],
                                       "last_run": (next((r["status"] for r in s.data["runs"] if r["pid"] == p["id"]), None))} for p in s.data["pipelines"]]}},
    {"name": "list_connections", "write": False, "desc": "List connections by name, type and health. Secrets are never returned.", "schema": _obj({}),
     "fn": lambda s, a: {"connections": [{"id": c["id"], "type": c["type"], "endpoint": c["endpoint"], "status": c.get("status")} for c in s.data["connections"]]}},
    {"name": "list_tables", "write": False, "desc": "List Bronze, Silver and Gold tables with row counts and freshness.", "schema": _obj({"layer": STR}),
     "fn": lambda s, a: {"tables": [{"table": t["id"], "rows": t["rows"], "built": t["built"], "pipeline": t["pid"]} for t in service.tables_view(s) if not a.get("layer") or t["layer"] == a["layer"]]}},
    {"name": "get_table_schema", "write": False, "desc": "Columns and types of one table, named as layer.table.", "schema": _obj({"table": STR}, ["table"]),
     "fn": lambda s, a: next(({"table": t["id"], "columns": [{"name": c["name"], "type": c["type"], "key": c["key"], "personal_data": c["pii"]} for c in t["cols"]]}
                              for t in service.tables_view(s) if t["id"] == a.get("table")), {"error": f"table {a.get('table')} not found"})},
    {"name": "preview_table", "write": False, "desc": "First rows of a table, named as layer.table.", "schema": _obj({"table": STR, "limit": {"type": "integer"}}, ["table"]),
     "fn": lambda s, a: service.preview_table(s, a["table"], a.get("limit", 5))},
    {"name": "run_sql", "write": False, "desc": "Run one read-only SELECT on a warehouse connection. At most 50 rows come back.",
     "schema": _obj({"sql": STR, "connection": STR}, ["sql"]), "fn": _run_sql},
    {"name": "list_source_files", "write": False, "desc": "List files under a folder of a file connection.",
     "schema": _obj({"connection": STR, "path": STR, "format": STR}, ["connection", "path"]),
     "fn": lambda s, a: service.list_source_files(s, a["connection"], a["path"], a.get("format", "csv"))},
    {"name": "get_run_status", "write": False, "desc": "State of the latest run of a pipeline, with task states and the failure reason.",
     "schema": _obj({"pipeline": STR}, ["pipeline"]), "fn": _run_status},
    {"name": "get_run_logs", "write": False, "desc": "Log lines of a run, oldest first.", "schema": _obj({"run": STR}, ["run"]),
     "fn": lambda s, a: {"lines": [f"{l['level']} [{l['task']}] {l['msg']}" for l in reversed([l for l in s.data["logs"] if l.get("runId") == a.get("run")])][:80]}},
    {"name": "list_checks", "write": False, "desc": "Data quality checks, optionally for one table.", "schema": _obj({"table": STR}),
     "fn": lambda s, a: {"checks": [{"id": r["id"], "table": r["table"], "column": r["column"], "type": r["type"], "rule": r["param"], "on_failure": r["severity"],
                                    "enabled": r.get("on", True), "pass_rate": r["pass"]} for r in s.data["rules"] if not a.get("table") or r["table"] == a["table"]]}},
    {"name": "get_approval", "write": False, "desc": "Result of a change that was waiting for approval.", "schema": _obj({"id": STR}, ["id"]),
     "fn": lambda s, a: next(({"id": x["id"], "status": x["status"], "result": x.get("result")} for x in s.data["approvals"] if x["id"] == a.get("id")), {"error": "unknown approval"})},
    {"name": "run_pipeline", "write": True, "desc": "Start a run of a pipeline. Set full to reload everything.",
     "schema": _obj({"pipeline": STR, "full": {"type": "boolean"}}, ["pipeline"]),
     "fn": lambda s, a: {"run": service.start(s, a["pipeline"], bool(a.get("full")), trigger=a.get("_by", "tool"))["id"], "state": "running"}},
    {"name": "retry_run", "write": True, "desc": "Retry a failed run from the task that failed.", "schema": _obj({"run": STR}, ["run"]),
     "fn": lambda s, a: {"run": runner.retry_run(s, a["run"])["id"], "state": "running"}},
    {"name": "create_pipeline", "write": True,
     "desc": "Create a Bronze, Silver and Gold pipeline. columns is one 'name:TYPE' per line, with ':key' on the business key and ':pii' on personal data. "
             "kind is files or table. gold.type is aggregate, dimension or sql.",
     "schema": _obj({"name": STR, "target": STR, "conn": STR, "kind": STR, "format": STR, "path": STR, "pattern": STR, "watermark": STR, "columns": STR,
                     "scd": STR, "onBad": STR, "cron": STR,
                     "gold": _obj({"name": STR, "type": STR, "dateCol": STR, "dims": STR, "measures": STR, "sql": STR})}, ["name", "target", "columns"]),
     "fn": lambda s, a: {"pipeline": service.deploy_pipeline(s, {k: v for k, v in a.items() if not k.startswith("_")}, by=a.get("_by", "tool"))["id"]}},
    {"name": "add_check", "write": True, "desc": "Add a data quality check. severity is fail, quarantine or warn.",
     "schema": _obj({"table": STR, "column": STR, "type": STR, "param": STR, "severity": STR}, ["table", "type"]),
     "fn": lambda s, a: {"check": service.add_rule(s, a)["id"]}},
    {"name": "set_control", "write": True, "desc": "Change one control of a pipeline, for example onEmpty, onNewColumn, dedup, mask, breaker, gate or retries.",
     "schema": _obj({"pipeline": STR, "key": STR, "value": {}}, ["pipeline", "key", "value"]),
     "fn": lambda s, a: {"controls": service.set_control(s, a["pipeline"], a["key"], a["value"])["ctl"]}},
    {"name": "pause_pipeline", "write": True, "desc": "Pause or resume the schedule of a pipeline.", "schema": _obj({"pipeline": STR}, ["pipeline"]),
     "fn": lambda s, a: {"state": service.toggle_pipeline(s, a["pipeline"])["status"]}},
]
BY_NAME = {t["name"]: t for t in TOOLS}


def needs_approval(store, tool):
    return tool["write"] and store.data["toolApproval"].get(tool["name"], True)


def describe(store):
    return [{"name": t["name"], "desc": t["desc"], "write": t["write"], "approve": needs_approval(store, t), "schema": t["schema"]} for t in TOOLS]


def call(store, name, args, by, approved=False):
    """Run a tool. A change that needs approval is parked and returns {'status': 'waiting_for_approval'}."""
    tool = BY_NAME.get(name)
    if not tool:
        raise ValueError(f"Unknown tool {name}")
    args = dict(args or {})
    if needs_approval(store, tool) and not approved:
        req = {"id": store.next_id("ap"), "ts": now_ms(), "tool": name, "args": args, "by": by, "status": "pending", "result": None}
        with store.lock:
            store.data["approvals"].insert(0, req)
        store.save()
        return {"status": "waiting_for_approval", "approval_id": req["id"], "note": "A person must approve this change on the MCP page of the control plane."}
    args["_by"] = by
    result = tool["fn"](store, args)
    if tool["write"]:
        store.audit("Tool call", name, f"by {by}", who=by)
    return result


def decide(store, approval_id, ok):
    req = next((x for x in store.data["approvals"] if x["id"] == approval_id), None)
    if not req or req["status"] != "pending":
        raise ValueError("That request is no longer waiting")
    if not ok:
        req.update(status="rejected", result={"error": "rejected by a person"})
    else:
        try:
            req.update(status="approved", result=call(store, req["tool"], req["args"], req["by"], approved=True))
        except Exception as e:
            req.update(status="failed", result={"error": str(e)})
    store.audit("Approved change" if ok else "Rejected change", req["tool"], f"requested by {req['by']}")
    return req
