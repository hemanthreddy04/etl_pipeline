"""Print every statement shape the run engine sends to BigQuery and Databricks, for review. No network is used."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("STATE_URI", "/tmp/unused-state.json"); os.environ.setdefault("SCHEDULER", "off")
from app import medallion as M, quality as Q
from app.config import settings
from app.engines.bigquery_engine import BigQueryEngine
from app.engines.databricks_engine import DatabricksEngine

cfg = {"name": "orders", "kind": "files", "format": "csv", "path": "orders", "pattern": "incremental", "onBad": "quarantine", "scd": "1", "maskPii": True,
       "columns": "order_id:STRING:key\ncustomer_email:STRING:pii\nstatus:STRING\namount:DECIMAL(18,2)\norder_date:DATE\nupdated_at:TIMESTAMP",
       "gold": {"name": "agg_orders_daily", "type": "aggregate", "dateCol": "order_date", "dims": "status", "measures": "COUNT(*) AS orders\nSUM(amount) AS revenue"}}
engines = {"bigquery": BigQueryEngine({"id": "bq", "type": "bq", "endpoint": "my-project", "options": {"location": "US"}}, settings),
           "databricks": DatabricksEngine({"id": "adb", "type": "adb", "endpoint": "https://adb-1.2.azuredatabricks.net", "secret": "X", "options": {"warehouse": "abc", "catalog": "main"}}, settings)}
which = sys.argv[1] if len(sys.argv) > 1 else None
for name, eng in engines.items():
    if which and which != name:
        continue
    for scd in ("1", "2"):
        c = dict(cfg, scd=scd)
        p = M.build_pipeline(c, {"id": "x"}, type(eng))
        mapping = M.default_mapping(p)["rows"]
        rules = [dict(zip(("table", "column", "type", "param", "severity"), r), id=f"dq{i}", on=True, min=100) for i, r in enumerate(M.default_rules(p))]
        rules += [{"id": "dq90", "table": "silver.orders", "column": "amount", "type": "range", "param": "0 to 100000", "severity": "quarantine", "on": True, "min": 100},
                  {"id": "dq91", "table": "silver.orders", "column": "status", "type": "accepted_values", "param": "placed, paid, it's", "severity": "warn", "on": True, "min": 100},
                  {"id": "dq92", "table": "silver.orders", "column": "customer_email", "type": "regex", "param": r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$", "severity": "warn", "on": True, "min": 100},
                  {"id": "dq93", "table": "silver.orders", "column": "status", "type": "referential", "param": "exists in silver.statuses.code", "severity": "warn", "on": True, "min": 100}]
        cols = [x["name"] for x in M.parse_cols(c["columns"])]
        f = M.fqs(p, eng)
        print(f"\n################ {name.upper()}  SCD{scd} ################")
        if scd == "1":
            print("-- bronze: first load"); print(";\n".join(M.bronze_sql(p, eng, cols, None, "run_1", False)) + ";")
            print("-- bronze: later load with a new column"); print(";\n".join(M.bronze_sql(p, eng, cols + ["eco"], cols, "run_2", False)) + ";")
            p["ctl"]["onNewColumn"] = "rescue"
            print("-- bronze: rescue mode"); print(";\n".join(M.bronze_sql(p, eng, cols + ["eco"], cols, "run_2", False)) + ";"); p["ctl"]["onNewColumn"] = "accept"
            p2 = dict(p, kind="table", path="raw.orders", watermark="updated_at")
            print("-- bronze: stage from a table"); print(";\n".join(eng.ctas(f["stg"], M.stage_table_sql(p2, eng, "`src`", [("order_id", "STRING"), ("updated_at", "TIMESTAMP")], "2026-01-01"))) + ";")
        chk, flags = M.silver_check_sql(p, eng, mapping, [r for r in rules if r["table"] == "silver.orders"], cols + M.LINEAGE, ["run_1"], False)
        print("-- silver: checked staging"); print(";\n".join(chk) + ";")
        names = [n for _, n in flags]
        print("-- silver: counts"); print("SELECT COUNT(*) AS n, " + ", ".join(f"SUM(CASE WHEN {n} THEN 1 ELSE 0 END) AS {n}" for n in names) + f" FROM {f['chk']};")
        print("-- silver: quarantine")
        print(eng.create_if_missing(f["quar"], [("_run_id", "STRING"), ("_quarantined_at", "TIMESTAMP"), ("_reason", "STRING"), ("_row", "STRING")]) + ";")
        reasons = [f"CASE WHEN {n} THEN {eng.lit(M.rule_label(r))} END" for r, n in flags[:3]]
        print(f"INSERT INTO {f['quar']} (_run_id, _quarantined_at, _reason, _row)\nSELECT 'run_1', {eng.now()}, {eng.join_reasons(reasons)}, _row\nFROM {f['chk']}\nWHERE ({' OR '.join(names[:3])});")
        print("-- silver: rows to merge"); print(";\n".join(M.silver_new_sql(p, eng, mapping, names[:2])) + ";")
        print("-- silver: first write"); print(";\n".join(M.silver_write_sql(p, eng, mapping, None, False)) + ";")
        print("-- silver: later write"); print(";\n".join(M.silver_write_sql(p, eng, mapping, [x for x, _ in M.silver_columns(p, mapping)][:-1], False)) + ";")
        if scd == "1":
            print("-- gold"); print(";\n".join(M.gold_sql(p, eng)) + ";")
            pd = dict(p, cfg=dict(c, gold={"name": "dim_orders", "type": "dimension"}), gold="dim_orders"); print("-- gold dimension"); print(M.gold_select(pd, eng) + ";")
            print("-- check queries")
            for r in rules[:1] + [{"table": "silver.orders", "column": "order_id", "type": "unique", "param": ""}, {"table": "gold.agg_orders_daily", "column": "revenue", "type": "reconciliation", "param": "within 0.1% of SUM(amount) in Silver"}]:
                class Rec(BigQueryEngine if name == "bigquery" else DatabricksEngine):
                    def query(self, sql): print(sql + ";"); return [{"n": 1, "d": 1, "f": 0, "v": 1}]
                    def scalar(self, sql): print(sql + ";"); return 1
                rec = Rec(eng.conn, settings)
                Q.evaluate(dict(r, id="x", on=True, min=100), rec, p, {"tstats": {}, "drift": [], "mappings": {}}, {"batches": ["run_1"]})
