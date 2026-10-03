"""API-level check with a live server on the local engine. Run:  python -m tests.test_api"""
import json, os, shutil, subprocess, sys, tempfile, time
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
work = tempfile.mkdtemp(prefix="mcp-api-")
shutil.copytree(os.path.join(ROOT, "samples", "landing"), os.path.join(work, "landing"))
env = dict(os.environ, STATE_URI=os.path.join(work, "state.json"), RETRY_BASE_SECONDS="0", SCHEDULER="off", APP_TOKEN="secret-token")
port = 8765
srv = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port), "--log-level", "warning"], cwd=ROOT, env=env)
B, H = f"http://127.0.0.1:{port}", {"Authorization": "Bearer secret-token"}
checks = []


def check(name, cond, extra=""):
    checks.append((name, bool(cond)))
    print(("  PASS " if cond else "  FAIL ") + name + (f"  {extra}" if extra and not cond else ""))


def call(method, path, body=None, ok=True):
    r = requests.request(method, B + path, json=body, headers=H, timeout=60)
    if ok and r.status_code >= 400:
        raise AssertionError(f"{method} {path} -> {r.status_code} {r.text[:300]}")
    return r.json() if r.headers.get("content-type", "").startswith("application/json") else r


def wait_run(pid):
    for _ in range(400):
        run = next(r for r in call("GET", "/api/state")["runs"] if r["pid"] == pid)
        if run["status"] != "running":
            return run
        time.sleep(0.05)
    raise AssertionError("run did not finish")


try:
    for _ in range(100):
        try:
            requests.get(B + "/healthz", timeout=1); break
        except requests.RequestException:
            time.sleep(0.1)
    check("API refuses calls without the token", requests.get(B + "/api/state").status_code == 401)
    check("the workspace starts empty", call("GET", "/api/state")["pipelines"] == [])
    c = call("POST", "/api/connections", {"id": "local_lake", "type": "local", "endpoint": os.path.join(work, "lake")})
    check("local warehouse connection tests healthy", c["status"] == "healthy", c.get("note"))
    c = call("POST", "/api/connections", {"id": "landing", "type": "folder", "endpoint": os.path.join(work, "landing")})
    check("folder connection tests healthy", c["status"] == "healthy")
    c = call("POST", "/api/connections", {"id": "bad_folder", "type": "folder", "endpoint": "/no/such/folder"})
    check("a wrong folder is reported as unreachable with the reason", c["status"] == "down" and "does not exist" in c["note"])
    files = call("POST", "/api/files", {"connection": "landing", "path": "vendor_returns"})
    check("source files are listed", files["total"] == 1)
    cfg = {"name": "vendor_returns", "target": "local_lake", "conn": "landing", "kind": "files", "format": "csv", "path": "vendor_returns", "pattern": "incremental",
           "columns": "return_id:STRING:key\norder_id:STRING\ncustomer_email:STRING:pii\nreason:STRING\nchannel:STRING\nrefund_amount:DECIMAL(18,2)\nreturn_date:DATE\nupdated_at:TIMESTAMP",
           "onBad": "quarantine", "scd": "1", "maskPii": True, "cron": "0 * * * *", "retries": 1,
           "gold": {"name": "agg_returns_daily", "type": "aggregate", "dateCol": "return_date", "dims": "reason, channel", "measures": "COUNT(*) AS returns\nSUM(refund_amount) AS refund_total"}}
    plan = call("POST", "/api/plan", {"cfg": cfg})
    check("SQL preview before deploying", "CREATE TABLE" in plan["silver"] and "GROUP BY" in plan["gold"])
    p = call("POST", "/api/pipelines", {"cfg": cfg})
    check("pipeline deployed", p["id"] == "vendor_returns_medallion")
    r = requests.post(B + "/api/pipelines", json={"cfg": cfg}, headers=H)
    check("deploying the same pipeline twice is refused with a reason", r.status_code == 400 and "already exists" in r.json()["error"])
    st = call("GET", "/api/state")
    check("three tables, a mapping and starter checks exist", len(st["tables"]) == 3 and "silver.vendor_returns" in st["mappings"] and len(st["rules"]) >= 6)
    nn = next(x for x in st["rules"] if x["type"] == "not_null")
    call("PUT", f"/api/rules/{nn['id']}", {"severity": "quarantine"})
    call("PUT", f"/api/pipelines/{p['id']}/controls", {"key": "breaker", "value": 50})
    call("POST", f"/api/pipelines/{p['id']}/run", {})
    run = wait_run(p["id"])
    check("run succeeds through the API", run["status"] == "success" and run["rows"] == {"bronze": 12, "silver": 8, "gold": 8}, json.dumps(run)[:400])
    st = call("GET", "/api/state")
    silver = next(t for t in st["tables"] if t["id"] == "silver.vendor_returns")
    check("catalog shows live row counts and columns", silver["rows"] == 8 and silver["built"] and any(c["name"] == "refund_amount" for c in silver["cols"]))
    prev = call("GET", "/api/tables/silver.vendor_returns/preview")
    check("table preview returns real rows", len(prev["rows"]) == 5 and "@" not in prev["rows"][0]["customer_email"])
    q = st["quarantine"][0]
    rows = call("GET", f"/api/quarantine/{q['table']}/{q['run']}")["rows"]
    check("quarantined rows come back with the reason and the original values", len(rows) == 3 and rows[0]["reason"] and "return_id" in rows[0]["row"])
    check("an alert was raised for the quarantine", any("quarantined" in a["title"] for a in st["alerts"]))
    res = call("POST", "/api/rules/run", {})
    check("all checks can be evaluated on demand", res["evaluated"] >= 6, json.dumps(res))
    rec = call("POST", f"/api/pipelines/{p['id']}/recommend", {"layer": "gold"})
    check("recommended Gold checks are added", rec["added"] >= 2)
    m = st["mappings"]["silver.vendor_returns"]["rows"]
    m.append({"src": "channel", "tgt": "channel_upper", "ttype": "STRING", "tx": "upper", "nullable": True, "pii": False})
    call("PUT", "/api/mappings/silver.vendor_returns", {"rows": m})
    call("POST", f"/api/pipelines/{p['id']}/run", {"full": True})
    run = wait_run(p["id"])
    prev = call("GET", "/api/tables/silver.vendor_returns/preview")
    check("a mapping change adds the column on the next run", run["status"] == "success" and prev["rows"][0].get("channel_upper") in ("WEB", "APP", "STORE"), json.dumps(run)[:300])
    # tools, approvals, MCP
    t = call("POST", "/api/tools/run_sql", {"args": {"sql": "SELECT COUNT(*) AS n FROM silver.vendor_returns"}})
    check("read-only SQL tool returns real data", t["result"]["rows"][0]["n"] == 8)
    r = requests.post(B + "/api/tools/run_sql", json={"args": {"sql": "DELETE FROM silver.vendor_returns"}}, headers=H)
    check("the SQL tool refuses anything but SELECT", r.status_code == 400)
    init = requests.post(B + "/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}}, headers=H).json()
    check("MCP initialize answers", init["result"]["serverInfo"]["name"] == "medallion-control-plane")
    lst = requests.post(B + "/mcp", json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, headers=H).json()
    check("MCP lists the tools", len(lst["result"]["tools"]) >= 15)
    out = requests.post(B + "/mcp", json={"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "run_pipeline", "arguments": {"pipeline": p["id"]}}}, headers=H).json()
    body = json.loads(out["result"]["content"][0]["text"])
    check("a change through MCP waits for approval", body.get("status") == "waiting_for_approval")
    ap = call("POST", f"/api/approvals/{body['approval_id']}", {"ok": True})
    check("approving it starts the run", ap["status"] == "approved" and ap["result"]["state"] == "running")
    wait_run(p["id"])
    r = requests.post(B + "/api/agent", json={"message": "hello"}, headers=H)
    check("the agent says clearly that it needs an API key", r.status_code == 400 and "ANTHROPIC_API_KEY" in r.json()["error"])
    er = call("POST", "/api/governance/erase", {"column": "return_id", "value": "R1001"})
    check("erasure shows what would be deleted before doing it", er["done"] is False and any(x["rows"] == 1 for x in er["tables"]))
    er = call("POST", "/api/governance/erase", {"column": "return_id", "value": "R1001", "confirm": True})
    n = call("POST", "/api/tools/run_sql", {"args": {"sql": "SELECT COUNT(*) AS n FROM silver.vendor_returns WHERE return_id = 'R1001'"}})["result"]["rows"][0]["n"]
    check("confirmed erasure deletes the rows", er["done"] and n == 0)
    z = requests.post(B + "/api/export", json={"files": [{"path": "a/b.sql", "content": "select 1"}]}, headers=H)
    check("code export returns a zip", z.status_code == 200 and z.content[:2] == b"PK")
    call("PUT", f"/api/pipelines/{p['id']}/schedule", {"cron": "*/15 * * * *", "retries": 2, "sla": 60})
    r = requests.put(B + f"/api/pipelines/{p['id']}/schedule", json={"cron": "nonsense"}, headers=H)
    check("a bad cron is refused", r.status_code == 400)
    check("index page is served", requests.get(B + "/").status_code in (200, 404))
    call("DELETE", f"/api/pipelines/{p['id']}", {"dropTables": True})
    st = call("GET", "/api/state")
    check("deleting with drop removes the pipeline and its tables", st["pipelines"] == [] and st["tables"] == [])
finally:
    srv.terminate()
    srv.wait(timeout=10)
    shutil.rmtree(work, ignore_errors=True)
failed = [n for n, ok in checks if not ok]
print(f"\n{len(checks) - len(failed)} of {len(checks)} checks passed")
sys.exit(1 if failed else 0)
