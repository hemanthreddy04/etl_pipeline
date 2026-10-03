"""End-to-end check of the run engine on the local engine. Run:  python -m tests.test_engine_local"""
import json, os, shutil, sys, tempfile, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
work = tempfile.mkdtemp(prefix="mcp-test-")
os.environ.update(STATE_URI=os.path.join(work, "state.json"), RETRY_BASE_SECONDS="0", SCHEDULER="off")

from app import medallion as M, quality as Q, runner as R          # noqa: E402
from app.engines import get_engine, SQLiteEngine                    # noqa: E402
from app.store import Store                                         # noqa: E402

landing = os.path.join(work, "landing")
shutil.copytree(os.path.join(ROOT, "samples", "landing"), landing)
store = Store()
store.data["connections"] += [
    {"id": "local_lake", "type": "local", "endpoint": os.path.join(work, "lake"), "options": {}},
    {"id": "landing", "type": "folder", "endpoint": landing, "options": {}}]
cfg = {"name": "vendor_returns", "conn": "landing", "kind": "files", "format": "csv", "path": "vendor_returns", "pattern": "incremental",
       "columns": "return_id:STRING:key\norder_id:STRING\ncustomer_email:STRING:pii\nreason:STRING\nchannel:STRING\nrefund_amount:DECIMAL(18,2)\nreturn_date:DATE\nupdated_at:TIMESTAMP",
       "onBad": "quarantine", "scd": "1", "maskPii": True, "cron": "0 * * * *", "retries": 1,
       "gold": {"name": "agg_returns_daily", "type": "aggregate", "dateCol": "return_date", "dims": "reason, channel",
                "measures": "COUNT(*) AS returns\nSUM(refund_amount) AS refund_total"}}


def deploy(cfg):
    p = M.build_pipeline(cfg, store.connection("local_lake"), SQLiteEngine)
    store.data["pipelines"].append(p)
    store.data["mappings"][p["writes"][1]] = M.default_mapping(p)
    for t, c, ty, pa, sev in M.default_rules(p):
        store.data["rules"].append({"id": store.next_id("dq"), "table": t, "column": c, "type": ty, "param": pa, "severity": sev,
                                    "on": True, "min": 100, "pass": 100, "checked": 0, "failed": 0, "last": 0})
    return p


def run(pid, **kw):
    r = R.start_run(store, pid, **kw)
    return wait(r)


def wait(r):
    for _ in range(600):
        if r["status"] != "running":
            return r
        time.sleep(0.05)
    raise AssertionError("run did not finish")


def show(r):
    print(f"  {r['id']}: {r['status']} rows={r['rows']} quarantined={r['quarantined']} "
          f"tasks={[t['status'] for t in r['tasks'].values()]} {('| ' + r['failMsg']) if r['failMsg'] else ''}")


p = deploy(cfg)
eng = get_engine(store.connection("local_lake"))
checks = []


def check(name, cond):
    checks.append((name, bool(cond)))
    print(("  PASS " if cond else "  FAIL ") + name)


print("1. first run: 12 rows, 1 uncastable amount, 1 missing key, 1 bad date, 1 duplicate")
r = run(p["id"]); show(r)
check("run fails because a key is missing (not_null is set to stop)", r["status"] == "failed" and r["failedTask"] == "silver_vendor_returns")
check("12 rows landed in Bronze as text", r["rows"]["bronze"] == 12)
check("bad rows were saved to quarantine with a reason", eng.scalar('SELECT COUNT(*) FROM silver."vendor_returns__quarantine"') == 3)

print("2. set not_null to quarantine, raise the breaker, retry from the failed task")
nn = next(x for x in store.data["rules"] if x["type"] == "not_null"); nn["severity"] = "quarantine"
p["ctl"]["breaker"] = 50
r = wait(R.retry_run(store, r["id"])); show(r)
check("retry succeeds without reloading Bronze", r["status"] == "success" and r["tasks"]["bronze_vendor_returns"]["tries"] == 1)
check("3 rows quarantined, 8 merged after removing 1 duplicate", r["quarantined"] == 3 and r["rows"]["silver"] == 8)
silver = eng.query('SELECT * FROM silver."vendor_returns" ORDER BY return_id')
check("amounts are numbers and dates are dates", isinstance(silver[0]["refund_amount"], float) and silver[0]["return_date"] == "2026-09-30")
check("personal data is hashed", len(silver[0]["customer_email"]) == 64 and "@" not in silver[0]["customer_email"])
gold = eng.query('SELECT * FROM gold."agg_returns_daily"')
check("Gold totals equal Silver totals", abs(sum(g["refund_total"] for g in gold) - sum(s["refund_amount"] for s in silver)) < 0.001)
check("staging tables with unmasked values are gone", not eng.exists("silver", "vendor_returns__chk") and not eng.exists("silver", "vendor_returns__new"))

print("3. run again with nothing new")
r = run(p["id"]); show(r)
check("ends early and changes nothing", r["status"] == "success" and list(r["tasks"].values())[-1]["status"] == "skipped")

print("4. a new file arrives with a new column, an update to an existing key, and new rows")
with open(os.path.join(landing, "vendor_returns", "2026-10-02.csv"), "w") as f:
    f.write("return_id,order_id,customer_email,reason,channel,refund_amount,return_date,updated_at,eco_score\n"
            "R1001,O501,ava.keller@example.com,damaged,web,59.90,2026-09-30,2026-10-02 08:00:00,A\n"
            "R1012,O512,eva.novak@example.com,damaged,web,20.00,2026-10-02,2026-10-02 08:30:00,B\n"
            "R1013,O513,ben.cole@example.com,wrong size,app,31.00,2026-10-02,2026-10-02 09:00:00,C\n")
r = run(p["id"]); show(r)
check("only the new file is loaded (3 rows)", r["rows"]["bronze"] == 3)
check("schema drift is recorded for eco_score", any(d["col"] == "eco_score" and d["status"] == "pending" for d in store.data["drift"]))
check("Bronze accepted the new column", "eco_score" in [c for c, _ in eng.columns("bronze", "vendor_returns_raw")])
check("the existing key was updated, not duplicated (10 rows, R1001 = 59.90)",
      eng.scalar('SELECT COUNT(*) FROM silver."vendor_returns"') == 10 and eng.scalar("SELECT refund_amount FROM silver.\"vendor_returns\" WHERE return_id='R1001'") == 59.9)

print("5. a range check set to stop, then a threshold that allows it")
rule = {"id": store.next_id("dq"), "table": "silver.vendor_returns", "column": "refund_amount", "type": "range", "param": "0 to 100",
        "severity": "fail", "on": True, "min": 100, "pass": 100, "checked": 0, "failed": 0, "last": 0}
store.data["rules"].append(rule)
r = run(p["id"], full=True); show(r)
check("full reload stops on the range check", r["status"] == "failed" and "range" in r["failMsg"])
rule["min"] = 80
r = wait(R.retry_run(store, r["id"])); show(r)
check("inside the allowed threshold the same data passes", r["status"] == "success")

print("6. Gold gate: a reconciliation that cannot hold keeps the old Gold")
before = eng.query('SELECT * FROM gold."agg_returns_daily"')
bad = {"id": store.next_id("dq"), "table": "gold.agg_returns_daily", "column": "refund_total", "type": "reconciliation",
       "param": "within 0.1% of SUM(refund_amount) * 2 in Silver", "severity": "fail", "on": True, "min": 100, "pass": 100, "checked": 0, "failed": 0, "last": 0}
store.data["rules"].append(bad)
r = run(p["id"], full=True); show(r)
check("run stops at the Gold checks", r["status"] == "failed" and r["failedTask"] == "dq_gold")
check("published Gold is unchanged", [g["refund_total"] for g in eng.query('SELECT * FROM gold."agg_returns_daily"')] == [g["refund_total"] for g in before])
bad["param"] = "within 0.1% of SUM(refund_amount) in Silver"
r = wait(R.retry_run(store, r["id"])); show(r)
check("with the right rule the retry rebuilds and publishes Gold", r["status"] == "success")

print("7. SCD Type 2 pipeline from a table source")
cfg2 = dict(cfg, name="returns_history", kind="table", conn="local_lake", path="bronze.vendor_returns_raw", scd="2", watermark="updated_at",
            gold={"name": "dim_returns", "type": "dimension"})
p2 = deploy(cfg2)
next(x for x in store.data["rules"] if x["table"] == "silver.returns_history" and x["type"] == "not_null")["severity"] = "quarantine"
p2["ctl"]["breaker"] = 50
r = run(p2["id"]); show(r)
check("SCD2 first load succeeds", r["status"] == "success")
versions = eng.query("SELECT return_id, is_current FROM silver.\"returns_history\" WHERE return_id = 'R1001'")
check("one current row per key after the first load", len(versions) == 1 and versions[0]["is_current"] == 1)
eng.execute("UPDATE bronze.\"vendor_returns_raw\" SET refund_amount = '61.00', updated_at = '2026-10-03 08:00:00' WHERE return_id = 'R1001'")
r = run(p2["id"]); show(r)
versions = eng.query("SELECT refund_amount, is_current, valid_to FROM silver.\"returns_history\" WHERE return_id = 'R1001' ORDER BY valid_from, is_current")
check("a change closes the old version and opens a new one", len(versions) == 2 and sum(v["is_current"] for v in versions) == 1)

print("8. static check run and SQL preview")
for rule_ in store.data["rules"]:
    pp = next(x for x in store.data["pipelines"] if rule_["table"] in x["writes"])
    Q.evaluate(rule_, eng, pp, store.data)
plan = M.plan(p, eng, store.data["mappings"]["silver.vendor_returns"]["rows"], store.data["rules"])
check("preview has SQL for every layer", all(len(plan[k]) > 50 for k in ("bronze", "silver", "gold")))

failed = [n for n, ok in checks if not ok]
print(f"\n{len(checks) - len(failed)} of {len(checks)} checks passed")
shutil.rmtree(work, ignore_errors=True)
sys.exit(1 if failed else 0)
