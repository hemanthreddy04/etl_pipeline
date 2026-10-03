"""Exercises the HTTP plumbing of the BigQuery and Databricks engines against small fake servers.
It proves requests, polling, paging and result parsing. It does NOT prove the SQL runs on the real services.
Run:  python -m tests.test_cloud_plumbing"""
import json, os, sys, threading
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.update(STATE_URI="/tmp/unused.json", SCHEDULER="off", GOOGLE_OAUTH_ACCESS_TOKEN="fake", DBX_TOKEN="fake")
from app.config import settings                                   # noqa: E402
from app.engines import bigquery_engine as bqm                    # noqa: E402
from app.engines.base import EngineError                           # noqa: E402
from app.engines.bigquery_engine import BigQueryEngine            # noqa: E402
from app.engines.databricks_engine import DatabricksEngine        # noqa: E402

seen, polls = [], {"n": 0}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code); self.send_header("content-type", "application/json"); self.send_header("content-length", str(len(raw))); self.end_headers(); self.wfile.write(raw)

    def _body(self):
        n = int(self.headers.get("content-length") or 0)
        return json.loads(self.rfile.read(n) or b"{}") if n else {}

    def do_GET(self):
        p = self.path
        seen.append(("GET", p))
        if "/queries/job1" in p:
            polls["n"] += 1
            return self._send(200, {"jobComplete": polls["n"] >= 2, "jobReference": {"jobId": "job1", "location": "US"},
                                    "schema": {"fields": [{"name": "n", "type": "INTEGER"}, {"name": "ok", "type": "BOOLEAN"}, {"name": "v", "type": "NUMERIC"}]},
                                    "rows": [{"f": [{"v": "7"}, {"v": "true"}, {"v": "1.5"}]}]})
        if p.endswith("/tables/orders_raw"):
            return self._send(200, {"numRows": "12", "numBytes": "2048", "schema": {"fields": [{"name": "order_id", "type": "STRING"}]}})
        if "/tables/" in p:
            return self._send(404, {"error": {"message": "Not found"}})
        if "/b/landing/o?" in p or p.endswith("/b/landing/o"):
            return self._send(200, {"items": [{"name": "orders/a.csv", "size": "10", "updated": "2026-10-01T08:00:00.000Z"}, {"name": "orders/readme.txt", "size": "1", "updated": "2026-10-01T08:00:00.000Z"}]})
        if "/o/orders%2Fa.csv" in p:
            return self._send(200, b"order id,Amount,2nd col\n1,2,3\n")
        if p.startswith("/api/2.0/sql/statements/st2"):
            return self._send(200, {"statement_id": "st2", "status": {"state": "SUCCEEDED"}, "manifest": {"schema": {"columns": [{"name": "n", "type_name": "LONG"}]}}, "result": {"data_array": [["5"]]}})
        if p.startswith("/api/2.0/sql/warehouses/wh1"):
            return self._send(200, {"id": "wh1", "name": "main wh"})
        if p.startswith("/api/2.0/sql/warehouses"):
            return self._send(200, {"warehouses": [{"id": "wh1", "name": "main wh", "state": "RUNNING", "cluster_size": "Small", "auto_stop_mins": 10}]})
        self._send(404, {"error": {"message": "unknown"}})

    def do_POST(self):
        p, body = self.path, self._body()
        seen.append(("POST", p, body, self.headers.get("authorization")))
        if p.endswith("/queries"):
            q = body["query"]
            if "BOOM" in q:
                return self._send(400, {"error": {"message": "Syntax error at BOOM"}})
            if "SLOW" in q:
                polls["n"] = 0
                return self._send(200, {"jobComplete": False, "jobReference": {"jobId": "job1", "location": "US"}})
            return self._send(200, {"jobComplete": True, "jobReference": {"jobId": "j0"}, "numDmlAffectedRows": "3", "totalBytesProcessed": "100",
                                    "schema": {"fields": [{"name": "ok", "type": "INTEGER"}]}, "rows": [{"f": [{"v": "1"}]}]})
        if p.endswith("/datasets"):
            return self._send(409, {"error": {"message": "Already Exists"}})
        if p.startswith("/api/2.0/sql/statements"):
            st = body["statement"]
            if "BOOM" in st:
                return self._send(200, {"statement_id": "st1", "status": {"state": "FAILED", "error": {"message": "PARSE_SYNTAX_ERROR near BOOM"}}})
            if "SLOW" in st:
                return self._send(200, {"statement_id": "st2", "status": {"state": "PENDING"}})
            if st.startswith("DESCRIBE TABLE"):
                return self._send(200, {"statement_id": "s", "status": {"state": "SUCCEEDED"}, "manifest": {"schema": {"columns": [{"name": "col_name", "type_name": "STRING"}, {"name": "data_type", "type_name": "STRING"}]}},
                                        "result": {"data_array": [["order_id", "string"], ["amount", "decimal(18,2)"], ["", ""], ["# Partitioning", ""]]}})
            if st.startswith("LIST"):
                return self._send(200, {"statement_id": "s", "status": {"state": "SUCCEEDED"}, "manifest": {"schema": {"columns": [{"name": "path", "type_name": "STRING"}, {"name": "name", "type_name": "STRING"}, {"name": "size", "type_name": "LONG"}, {"name": "modification_time", "type_name": "LONG"}]}},
                                        "result": {"data_array": [["abfss://x/a.csv", "a.csv", "10", "1700000000000"], ["abfss://x/sub/", "sub/", "0", "0"]]}})
            return self._send(200, {"statement_id": "s", "status": {"state": "SUCCEEDED"}, "manifest": {"schema": {"columns": [{"name": "num_affected_rows", "type_name": "LONG"}]}}, "result": {"data_array": [["4"]]}})
        self._send(404, {"error": {"message": "unknown"}})


srv = HTTPServer(("127.0.0.1", 0), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{srv.server_port}"
bqm.BQ, bqm.GCS = base, base
checks = []


def check(name, cond):
    checks.append((name, bool(cond)))
    print(("  PASS " if cond else "  FAIL ") + name)


bq = BigQueryEngine({"id": "bq", "type": "bq", "endpoint": "proj", "options": {"location": "US"}}, settings)
check("BigQuery: a query returns typed rows", bq.query("SELECT 1 AS ok") == [{"ok": 1}])
check("BigQuery: the token is sent as a Bearer header", seen[-1][3] == "Bearer fake")
check("BigQuery: DML reports affected rows", bq.execute("DELETE x")["affected"] == 3)
check("BigQuery: a slow job is polled until complete and parsed", bq.query("SELECT SLOW") == [{"n": 7, "ok": True, "v": 1.5}] and polls["n"] == 2)
try:
    bq.query("SELECT BOOM"); ok = False
except EngineError as e:
    ok = "Syntax error at BOOM" in str(e)
check("BigQuery: an error from the service becomes a readable message", ok)
check("BigQuery: columns and stats come from table metadata", bq.columns("bronze", "orders_raw") == [("order_id", "STRING")] and bq.stats("bronze", "orders_raw") == {"rows": 12, "bytes": 2048})
check("BigQuery: a missing table is reported as no columns", bq.columns("bronze", "nope") == [])
bq.ensure_schemas(); check("BigQuery: an existing dataset is not an error", True)
src, cfg = {"endpoint": "gs://landing"}, {"path": "orders", "format": "csv"}
files = bq.list_files(src, cfg)
check("Cloud Storage: files of the chosen format are listed, relative to the folder", [f["name"] for f in files] == ["a.csv"] and files[0]["modified"] > 0)
cols = bq.stage_files(src, cfg, files, bq.fq("bronze", "orders__stg"))
ddl = [x[2]["query"] for x in seen if x[0] == "POST" and x[1].endswith("/queries") and "EXTERNAL TABLE" in x[2]["query"]]
check("BigQuery: header names are made safe and the external table is created then dropped", cols == ["order_id", "Amount", "c_2nd_col"] and "uris = ['gs://landing/orders/a.csv']" in ddl[0] and "DROP EXTERNAL TABLE" in ddl[-1])

db = DatabricksEngine({"id": "adb", "type": "adb", "endpoint": base, "secret": "DBX_TOKEN", "options": {"warehouse": "wh1", "catalog": "main"}}, settings)
check("Databricks: DML reports affected rows", db.execute("DELETE x")["affected"] == 4)
check("Databricks: the token from the secret reference is sent", seen[-1][3] == "Bearer fake" and seen[-1][2]["warehouse_id"] == "wh1")
check("Databricks: a pending statement is polled and parsed", db.query("SELECT SLOW") == [{"n": 5}])
try:
    db.query("SELECT BOOM"); ok = False
except EngineError as e:
    ok = "PARSE_SYNTAX_ERROR" in str(e)
check("Databricks: a failed statement becomes a readable message", ok)
check("Databricks: DESCRIBE output is read up to the partition section", db.columns("bronze", "orders_raw") == [("order_id", "STRING"), ("amount", "DECIMAL(18,2)")])
files = db.list_files({"endpoint": "abfss://x"}, {"path": "", "format": "csv"})
check("Databricks: LIST output becomes a file list", [f["name"] for f in files] == ["a.csv"] and files[0]["modified"] == 1700000000000)
check("Databricks: the connection test reads the warehouse", "main wh" in db.test())
check("Databricks: warehouses are listed as compute", db.compute()[0]["state"] == "running" and db.compute()[0]["inUse"])
failed = [n for n, ok in checks if not ok]
print(f"\n{len(checks) - len(failed)} of {len(checks)} checks passed")
sys.exit(1 if failed else 0)
