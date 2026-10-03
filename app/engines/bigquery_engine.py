"""BigQuery engine over the REST API. Files come from Cloud Storage through an external table,
which gives the real file name of every row (_FILE_NAME) without running any Spark."""
import csv
import io
import time
from urllib.parse import quote

import requests

from ..cloudauth import default_gcp_project, gcp_headers
from ..util import ident, slug
from .base import Engine, EngineError, norm_type

BQ = "https://bigquery.googleapis.com/bigquery/v2"
GCS = "https://storage.googleapis.com/storage/v1"
_FORMATS = {"csv": "CSV", "json": "NEWLINE_DELIMITED_JSON", "parquet": "PARQUET", "avro": "AVRO"}
_EXT = {"csv": ".csv", "json": ".json", "parquet": ".parquet", "avro": ".avro"}


class BigQueryEngine(Engine):
    kind = "bq"
    platform = "gcp"
    label = "BigQuery"
    table_format = "BigQuery table"
    file_connection = "gcs"

    def __init__(self, conn, settings):
        super().__init__(conn, settings)
        self.project = (conn.get("endpoint") or "").strip() or default_gcp_project()
        self.location = self.opt.get("location") or "US"
        if not self.project:
            raise EngineError("The BigQuery connection needs a project id")

    # names
    def fq(self, layer, name):
        return f"`{self.project}.{self.schema(layer)}.{ident(name, 'table name')}`"

    # running SQL
    def _call(self, method, url, **kw):
        try:
            r = requests.request(method, url, headers=gcp_headers(), timeout=90, **kw)
        except requests.RequestException as e:
            raise EngineError(f"Could not reach Google Cloud: {e}") from e
        return r

    @staticmethod
    def _error(r):
        try:
            return r.json()["error"]["message"]
        except Exception:
            return f"HTTP {r.status_code}: {r.text[:300]}"

    def _run(self, sql, max_results=1000):
        r = self._call("POST", f"{BQ}/projects/{self.project}/queries",
                       json={"query": sql, "useLegacySql": False, "location": self.location, "timeoutMs": 30000, "maxResults": max_results})
        if not r.ok:
            raise EngineError(self._error(r))
        j = r.json()
        ref = j.get("jobReference", {})
        deadline = time.time() + 3600
        while not j.get("jobComplete"):
            if time.time() > deadline:
                raise EngineError("The BigQuery job did not finish within an hour")
            r = self._call("GET", f"{BQ}/projects/{self.project}/queries/{ref.get('jobId')}",
                           params={"location": ref.get("location", self.location), "timeoutMs": 30000, "maxResults": max_results})
            if not r.ok:
                raise EngineError(self._error(r))
            j = r.json()
        if j.get("errors"):
            raise EngineError(j["errors"][0].get("message", "BigQuery reported an error"))
        return j

    @staticmethod
    def _value(v, t):
        if v is None:
            return None
        try:
            if t in ("INTEGER", "INT64"):
                return int(v)
            if t in ("FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
                return float(v)
            if t in ("BOOLEAN", "BOOL"):
                return str(v).lower() == "true"
            if t == "TIMESTAMP":
                return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(float(v)))
        except (TypeError, ValueError):
            return v
        return v

    def execute(self, sql):
        j = self._run(sql, 0)
        n = j.get("numDmlAffectedRows")
        return {"affected": int(n) if n is not None else None, "bytes": int(j.get("totalBytesProcessed") or 0)}

    def query(self, sql):
        j = self._run(sql)
        fields = j.get("schema", {}).get("fields", [])
        return [{f["name"]: self._value(c.get("v"), f.get("type")) for f, c in zip(fields, row.get("f", []))} for row in j.get("rows", [])]

    def test(self):
        self.query("SELECT 1 AS ok")
        return f"Query ran in project {self.project} ({self.location})"

    # dialect
    def typ(self, generic):
        t = norm_type(generic)
        return {"INT": "INT64", "BIGINT": "INT64", "DOUBLE": "FLOAT64", "BOOLEAN": "BOOL"}.get(t, "NUMERIC" if t.startswith("DECIMAL") else t)

    def try_cast(self, expr, generic):
        return f"SAFE_CAST({expr} AS {self.typ(generic)})"

    def sha256(self, expr):
        return f"TO_HEX(SHA256(CAST({expr} AS STRING)))"

    def regex_ok(self, expr, pattern):
        return f"REGEXP_CONTAINS(CAST({expr} AS STRING), {self.lit(pattern)})"

    def regexp_replace(self, expr, pattern, repl):
        return f"REGEXP_REPLACE({expr}, {self.lit(pattern)}, {self.lit(repl)})"

    def join_reasons(self, exprs):
        return "ARRAY_TO_STRING([" + ", ".join(exprs) + "], '; ')"

    def row_json(self, alias, cols):
        return "TO_JSON_STRING(STRUCT(" + ", ".join(f"{alias}.{self.q(c)} AS {self.q(c)}" for c in cols) + "))"

    def add_column(self, fq, col, generic):
        return f"ALTER TABLE {fq} ADD COLUMN IF NOT EXISTS {self.q(col)} {self.typ(generic)}"

    # metadata
    def ensure_schemas(self):
        for layer in ("bronze", "silver", "gold"):
            r = self._call("POST", f"{BQ}/projects/{self.project}/datasets",
                           json={"datasetReference": {"projectId": self.project, "datasetId": self.schema(layer)}, "location": self.location})
            if not r.ok and r.status_code != 409:
                raise EngineError(f"Could not create dataset {self.schema(layer)}: {self._error(r)}")

    def _table(self, project, dataset, name):
        r = self._call("GET", f"{BQ}/projects/{project}/datasets/{dataset}/tables/{name}")
        if r.status_code == 404:
            return None
        if not r.ok:
            raise EngineError(self._error(r))
        return r.json()

    def columns(self, layer, name):
        t = self._table(self.project, self.schema(layer), name)
        return [(f["name"], f["type"]) for f in t.get("schema", {}).get("fields", [])] if t else []

    def stats(self, layer, name):
        t = self._table(self.project, self.schema(layer), name)
        if not t:
            return {"rows": 0, "bytes": None}
        return {"rows": int(t.get("numRows") or 0), "bytes": int(t.get("numBytes") or 0)}

    def source_table(self, path):
        parts = str(path).split(".")
        if len(parts) == 2:
            parts = [self.project] + parts
        if len(parts) != 3:
            raise EngineError("Name the source table as dataset.table or project.dataset.table")
        t = self._table(*parts)
        if not t:
            raise EngineError(f"Source table {path} does not exist or is not readable")
        return f"`{'.'.join(parts)}`", [(f["name"], f["type"]) for f in t.get("schema", {}).get("fields", [])]

    # files
    @staticmethod
    def _bucket(src, cfg):
        url = (src.get("endpoint") or "").replace("gs://", "").strip("/")
        bucket, _, base = url.partition("/")
        prefix = "/".join(x for x in (base, str(cfg.get("path") or "").strip("/")) if x)
        return bucket, (prefix + "/" if prefix else "")

    def list_files(self, src, cfg):
        fmt = cfg.get("format", "csv")
        bucket, prefix = self._bucket(src, cfg)
        out, token = [], None
        while True:
            r = self._call("GET", f"{GCS}/b/{bucket}/o", params={"prefix": prefix, "pageToken": token, "fields": "items(name,size,updated),nextPageToken"})
            if not r.ok:
                raise EngineError(f"Could not list gs://{bucket}/{prefix}: {self._error(r)}")
            j = r.json()
            for it in j.get("items", []):
                low = it["name"].lower()
                if low.endswith("/") or not (low.endswith(_EXT[fmt]) or (fmt == "json" and low.endswith((".jsonl", ".ndjson")))):
                    continue
                ts = time.mktime(time.strptime(it["updated"][:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone
                out.append({"name": it["name"][len(prefix):], "size": int(it.get("size", 0)), "modified": int(ts * 1000)})
            token = j.get("nextPageToken")
            if not token or len(out) > 20000:
                break
        return sorted(out, key=lambda x: x["name"])

    def _csv_header(self, bucket, obj):
        r = requests.get(f"{GCS}/b/{bucket}/o/{quote(obj, safe='')}", params={"alt": "media"},
                         headers={**gcp_headers(), "Range": "bytes=0-65535"}, timeout=60)
        if r.status_code not in (200, 206):
            raise EngineError(f"Could not read the header of gs://{bucket}/{obj}: HTTP {r.status_code}")
        first = next(csv.reader(io.StringIO(r.content.decode("utf-8-sig", errors="replace"))), [])
        cols = []
        for h in first:
            c = slug(h) if not h.strip().replace("_", "a").isalnum() or h.strip()[:1].isdigit() else h.strip()
            while c in cols:
                c += "_"
            cols.append(c)
        if not cols:
            raise EngineError(f"gs://{bucket}/{obj} has no header row")
        return cols

    def stage_files(self, src, cfg, files, stage_fq, since=None):
        fmt = cfg.get("format", "csv")
        if fmt not in _FORMATS:
            raise EngineError(f"Unsupported file format {fmt}")
        bucket, prefix = self._bucket(src, cfg)
        if len(files) > 1000:
            raise EngineError(f"{len(files)} new files in one run is more than the 1,000 this loader takes at once. Run a full reload, or load in smaller batches.")
        uris = ", ".join(self.lit(f"gs://{bucket}/{prefix}{f['name']}") for f in files)
        ext = stage_fq[:-1] + "_ext`"
        if fmt == "csv":
            cols = self._csv_header(bucket, prefix + files[0]["name"])
            schema = "(" + ", ".join(f"{self.q(c)} STRING" for c in cols) + ") "
            options = f"format = 'CSV', uris = [{uris}], skip_leading_rows = 1, allow_jagged_rows = true, allow_quoted_newlines = true"
        else:
            schema, options = "", f"format = '{_FORMATS[fmt]}', uris = [{uris}]"
        self.execute(f"CREATE OR REPLACE EXTERNAL TABLE {ext} {schema}OPTIONS ({options})")
        try:
            if fmt != "csv":
                dataset, name = ext.strip("`").split(".")[1:]
                t = self._table(self.project, dataset, name) or {}
                fields = t.get("schema", {}).get("fields", [])
                cols = [f["name"] for f in fields]
                select = ", ".join((f"TO_JSON_STRING({self.q(f['name'])})" if f["type"] in ("RECORD", "STRUCT") or f.get("mode") == "REPEATED"
                                    else f"CAST({self.q(f['name'])} AS STRING)") + f" AS {self.q(f['name'])}" for f in fields)
            else:
                select = ", ".join(self.q(c) for c in cols)
            self.execute(f"CREATE OR REPLACE TABLE {stage_fq} AS\nSELECT {select}, SUBSTR(_FILE_NAME, {len('gs://' + bucket + '/' + prefix) + 1}) AS _source_file\nFROM {ext}")
        finally:
            self.execute(f"DROP EXTERNAL TABLE IF EXISTS {ext}")
        return cols

    def compute(self):
        return [{"id": "bigquery", "kind": "BigQuery on-demand", "spec": f"Project {self.project}, location {self.location}", "state": "running", "canToggle": False}]
