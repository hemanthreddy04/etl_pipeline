"""Run the shared scenario against the real Python service and save every answer."""
import json, os, shutil, subprocess, sys, tempfile, time
import requests
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
steps = json.load(open(os.path.join(HERE, "steps.json")))
work = tempfile.mkdtemp(prefix="mcp-gold-")
os.makedirs(os.path.join(work, "landing"))
env = dict(os.environ, STATE_URI=os.path.join(work, "state.json"), RETRY_BASE_SECONDS="0", SCHEDULER="off", APP_TOKEN="")
port = 8791
srv = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port), "--log-level", "warning"], cwd=ROOT, env=env)
B = f"http://127.0.0.1:{port}"
def call(method, path, body=None):
    r = requests.request(method, B + path, json=body if method != "GET" else None, timeout=60)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text[:200]}
    return {"status": r.status_code, "body": data}
def state():
    return call("GET", "/api/state")["body"]
out = []
try:
    for _ in range(100):
        try:
            requests.get(B + "/healthz", timeout=1); break
        except requests.RequestException:
            time.sleep(0.1)
    for s in steps:
        op = s["op"]
        if op == "api":
            res = call(s["method"], s["path"], s.get("body"))
        elif op == "conn":
            res = call("POST", "/api/connections", {"id": s["id"], "type": s["type"], "endpoint": os.path.join(work, "lake" if s["type"] == "local" else "landing")})
        elif op == "file":
            d = os.path.join(work, "landing", s["folder"]); os.makedirs(d, exist_ok=True)
            open(os.path.join(d, s["name"]), "w", encoding="utf-8", newline="").write(s["text"])
            res = {"status": 200, "body": {"name": s["folder"] + "/" + s["name"]}}
        elif op == "wait":
            for _ in range(600):
                run = [r for r in state()["runs"] if r["pid"] == s["pid"]][s.get("nth", 0)]
                if run["status"] != "running":
                    break
                time.sleep(0.03)
            res = {"status": 200, "body": run}
        elif op == "state":
            res = {"status": 200, "body": state()}
        elif op == "rule":
            r = next(x for x in state()["rules"] if all(x.get(k) == v for k, v in s["match"].items()))
            res = call("PUT", f"/api/rules/{r['id']}", s["body"])
        elif op == "retry":
            run = [r for r in state()["runs"] if r["pid"] == s["pid"]][s.get("nth", 0)]
            res = call("POST", f"/api/runs/{run['id']}/retry", {})
        elif op == "quarantine":
            qs = [q for q in state()["quarantine"] if q["table"] == s["table"]]
            res = {"status": 200, "body": [call("GET", f"/api/quarantine/{q['table']}/{q['run']}") for q in qs]}
            if s.get("discard") and qs:
                res["body"].append(call("DELETE", f"/api/quarantine/{qs[0]['table']}/{qs[0]['run']}", {}))
        elif op == "mapping":
            rows = state()["mappings"][s["table"]]["rows"]
            rows = [r for r in rows if r["tgt"] not in s.get("drop", [])] + s.get("add", [])
            res = call("PUT", f"/api/mappings/{s['table']}", {"rows": rows})
        elif op == "approve":
            a = next(x for x in state()["approvals"] if x["status"] == "pending")
            res = call("POST", f"/api/approvals/{a['id']}", {"ok": s["ok"]})
        elif op == "logs":
            run = next(r for r in state()["runs"] if r["pid"] == s["pid"])
            res = call("POST", "/api/tools/get_run_logs", {"args": {"run": run["id"]}})
        else:
            raise SystemExit("unknown op " + op)
        out.append(res)
finally:
    srv.terminate(); srv.wait(timeout=10); shutil.rmtree(work, ignore_errors=True)
json.dump(out, open(os.path.join(HERE, "golden.json"), "w"))
print(len(out), "steps recorded;", sum(1 for r in out if r["status"] >= 400), "answered with an error status;", sum(1 for r in out if r["status"] >= 500), "server errors")
