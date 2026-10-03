"""Azure Databricks engine over the SQL Statement Execution API. Statements run on a SQL warehouse,
tables are Delta tables in Unity Catalog, and files are read with read_files()."""
import re
import time

import requests

from ..cloudauth import read_secret
from ..util import ident
from .base import Engine, EngineError


class DatabricksEngine(Engine):
    kind = "adb"
    platform = "azure"
    label = "Azure Databricks"
    table_format = "Delta"
    file_connection = "adls"

    def __init__(self, conn, settings):
        super().__init__(conn, settings)
        self.host = (conn.get("endpoint") or "").strip().rstrip("/")
        if self.host and not self.host.startswith("http"):
            self.host = "https://" + self.host
        self.warehouse = (self.opt.get("warehouse") or "").strip()
        self.catalog = ident((self.opt.get("catalog") or "main").strip(), "catalog")
        if not self.host or not self.warehouse:
            raise EngineError("The Databricks connection needs a workspace URL and a SQL warehouse id")

    def _headers(self):
        return {"Authorization": f"Bearer {read_secret(self.conn.get('secret'))}"}

    def _call(self, method, path, **kw):
        try:
            r = requests.request(method, self.host + path, headers=self._headers(), timeout=90, **kw)
        except requests.RequestException as e:
            raise EngineError(f"Could not reach Databricks: {e}") from e
        if not r.ok:
            try:
                msg = r.json().get("message") or r.text[:300]
            except Exception:
                msg = r.text[:300]
            raise EngineError(f"Databricks answered {r.status_code}: {msg}")
        return r.json() if r.text else {}

    # names
    def fq(self, layer, name):
        return f"`{self.catalog}`.`{self.schema(layer)}`.`{ident(name, 'table name')}`"

    # running SQL
    def _run(self, sql):
        j = self._call("POST", "/api/2.0/sql/statements/", json={
            "statement": sql, "warehouse_id": self.warehouse, "catalog": self.catalog, "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE", "format": "JSON_ARRAY", "disposition": "INLINE"})
        deadline = time.time() + 3600
        while j.get("status", {}).get("state") in ("PENDING", "RUNNING"):
            if time.time() > deadline:
                raise EngineError("The Databricks statement did not finish within an hour")
            time.sleep(2)
            j = self._call("GET", f"/api/2.0/sql/statements/{j['statement_id']}")
        state = j.get("status", {}).get("state")
        if state != "SUCCEEDED":
            raise EngineError(j.get("status", {}).get("error", {}).get("message") or f"Statement ended in state {state}")
        return j

    @staticmethod
    def _value(v, t):
        if v is None:
            return None
        try:
            if t in ("INT", "LONG", "SHORT", "BYTE"):
                return int(v)
            if t in ("DOUBLE", "FLOAT", "DECIMAL"):
                return float(v)
            if t == "BOOLEAN":
                return str(v).lower() == "true"
        except (TypeError, ValueError):
            return v
        return v

    def query(self, sql):
        j = self._run(sql)
        cols = j.get("manifest", {}).get("schema", {}).get("columns", [])
        return [{c["name"]: self._value(v, c.get("type_name")) for c, v in zip(cols, row)} for row in j.get("result", {}).get("data_array", []) or []]

    def execute(self, sql):
        rows = self.query(sql)
        n = rows[0].get("num_affected_rows") if rows and isinstance(rows[0], dict) else None
        return {"affected": int(n) if n is not None else None}

    def test(self):
        w = self._call("GET", f"/api/2.0/sql/warehouses/{self.warehouse}")
        self.query("SELECT 1 AS ok")
        return f"Query ran on warehouse {w.get('name', self.warehouse)}, catalog {self.catalog}"

    # dialect
    def sha256(self, expr):
        return f"sha2(CAST({expr} AS STRING), 256)"

    def regex_ok(self, expr, pattern):
        return f"(CAST({expr} AS STRING) RLIKE {self.lit(pattern)})"

    def regexp_replace(self, expr, pattern, repl):
        return f"regexp_replace({expr}, {self.lit(pattern)}, {self.lit(repl)})"

    def join_reasons(self, exprs):
        return "concat_ws('; ', " + ", ".join(exprs) + ")"

    def row_json(self, alias, cols):
        return "to_json(named_struct(" + ", ".join(f"{self.lit(c)}, {alias}.{self.q(c)}" for c in cols) + "))"

    def now(self):
        return "current_timestamp()"

    def today(self):
        return "current_date()"

    def add_column(self, fq, col, generic):
        return f"ALTER TABLE {fq} ADD COLUMNS ({self.q(col)} {self.typ(generic)})"

    # metadata
    def ensure_schemas(self):
        for layer in ("bronze", "silver", "gold"):
            self.execute(f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{self.schema(layer)}`")

    def _describe(self, fq):
        try:
            rows = self.query(f"DESCRIBE TABLE {fq}")
        except EngineError as e:
            if "NOT_FOUND" in str(e).upper() or "not found" in str(e).lower() or "cannot be found" in str(e).lower():
                return []
            raise
        out = []
        for r in rows:
            name = r.get("col_name") or ""
            if not name or name.startswith("#"):
                break
            out.append((name, str(r.get("data_type") or "").upper()))
        return out

    def columns(self, layer, name):
        return self._describe(self.fq(layer, name))

    def stats(self, layer, name):
        fq = self.fq(layer, name)
        s = {"rows": int(self.scalar(f"SELECT COUNT(*) AS n FROM {fq}") or 0), "bytes": None}
        try:
            d = self.query(f"DESCRIBE DETAIL {fq}")
            s["bytes"] = int(d[0].get("sizeInBytes") or 0) if d else None
        except EngineError:
            pass
        return s

    def source_table(self, path):
        parts = str(path).split(".")
        if len(parts) == 2:
            parts = [self.catalog] + parts
        if len(parts) != 3:
            raise EngineError("Name the source table as schema.table or catalog.schema.table")
        fq = ".".join(f"`{ident(p, 'table name')}`" for p in parts)
        cols = self._describe(fq)
        if not cols:
            raise EngineError(f"Source table {path} does not exist or is not readable")
        return fq, cols

    # files
    @staticmethod
    def _folder(src, cfg):
        return (src.get("endpoint") or "").rstrip("/") + "/" + str(cfg.get("path") or "").strip("/") + "/"

    def list_files(self, src, cfg):
        fmt = cfg.get("format", "csv")
        rows = self.query(f"LIST {self.lit(self._folder(src, cfg))}")
        out = []
        for r in rows:
            name = (r.get("name") or "").rstrip("/")
            if not name.lower().endswith("." + fmt) and not (fmt == "json" and name.lower().endswith((".jsonl", ".ndjson"))):
                continue
            out.append({"name": name, "size": int(r.get("size") or 0), "modified": int(r.get("modification_time") or 0)})
        return sorted(out, key=lambda x: x["name"])

    def stage_files(self, src, cfg, files, stage_fq, since=None):
        fmt = cfg.get("format", "csv")
        opts = f"format => '{fmt}'" + (", header => true, inferColumnTypes => false" if fmt == "csv" else "")
        names = ", ".join(self.lit(f["name"]) for f in files)
        if len(files) > 1000:
            raise EngineError(f"{len(files)} new files in one run is more than the 1,000 this loader takes at once. Run a full reload, or load in smaller batches.")
        self.execute(f"CREATE OR REPLACE TABLE {stage_fq} AS\nSELECT *, _metadata.file_name AS _source_file\n"
                     f"FROM read_files({self.lit(self._folder(src, cfg))}, {opts})\nWHERE _metadata.file_name IN ({names})")
        cols = [c for c, _ in self._describe(stage_fq) if c not in ("_source_file", "_rescued_data")]
        return cols

    # compute
    def compute(self):
        try:
            ws = self._call("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
        except EngineError as e:
            return [{"id": self.warehouse, "kind": "Databricks SQL warehouse", "spec": str(e), "state": "down", "canToggle": False}]
        return [{"id": w["id"], "kind": "Databricks SQL warehouse", "name": w.get("name"),
                 "spec": f"{w.get('cluster_size', '')}, {'serverless' if w.get('enable_serverless_compute') else 'classic'}, stops after {w.get('auto_stop_mins', '?')} idle minutes",
                 "state": {"RUNNING": "running", "STOPPED": "terminated", "STARTING": "starting", "STOPPING": "stopping"}.get(w.get("state"), "terminated"),
                 "min": w.get("min_num_clusters"), "max": w.get("max_num_clusters"), "canToggle": True, "inUse": w["id"] == self.warehouse} for w in ws]

    def toggle_compute(self, wid, start):
        if not re.match(r"^[A-Za-z0-9_-]+$", str(wid)):
            raise EngineError("That is not a valid warehouse id")
        self._call("POST", f"/api/2.0/sql/warehouses/{wid}/{'start' if start else 'stop'}")
