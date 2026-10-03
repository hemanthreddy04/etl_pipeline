"""Local engine on SQLite. It needs nothing installed, so the whole flow can be run and tested on a laptop
or in CI before any cloud account is involved. Bronze, Silver and Gold are three attached database files."""
import csv
import datetime as dt
import glob
import hashlib
import json
import os
import re
import sqlite3
import threading

from ..util import ident, slug
from .base import Engine, EngineError, norm_type

_INT = re.compile(r"^[+-]?\d+$")
_NUM = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")
_DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d")
_TS_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f",
               "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M", "%Y-%m-%d")


def _try_cast(value, target):
    """Return the value in the target type, or NULL when it cannot be converted."""
    if value is None:
        return None
    t = norm_type(target)
    s = str(value).strip()
    if s == "":
        return None
    try:
        if t in ("INT", "BIGINT"):
            if isinstance(value, float) and value.is_integer():
                return int(value)
            return int(s) if _INT.match(s) else None
        if t == "DOUBLE" or t.startswith("DECIMAL"):
            return float(s) if _NUM.match(s) else None
        if t == "BOOLEAN":
            low = s.lower()
            return 1 if low in ("true", "t", "1", "yes", "y") else 0 if low in ("false", "f", "0", "no", "n") else None
        if t == "DATE":
            for f in _DATE_FORMATS + _TS_FORMATS:
                try:
                    return dt.datetime.strptime(s, f).strftime("%Y-%m-%d")
                except ValueError:
                    pass
            return None
        if t == "TIMESTAMP":
            for f in _TS_FORMATS:
                try:
                    return dt.datetime.strptime(s, f).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass
            return None
    except (ValueError, OverflowError):
        return None
    return s


def _regexp(pattern, value):
    return 0 if value is None else 1 if re.search(pattern, str(value)) else 0


def _join_nonnull(sep, *parts):
    return sep.join(str(p) for p in parts if p is not None)


class SQLiteEngine(Engine):
    kind = "local"
    platform = "local"
    label = "Local SQLite"
    table_format = "SQLite table"
    file_connection = "folder"

    def __init__(self, conn, settings):
        super().__init__(conn, settings)
        self.dir = os.path.abspath(conn["endpoint"])
        self._tls = threading.local()

    def _db(self):
        c = getattr(self._tls, "c", None)
        if c is None:
            os.makedirs(self.dir, exist_ok=True)
            c = sqlite3.connect(os.path.join(self.dir, "lake.db"), timeout=60, isolation_level=None)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            for layer in ("bronze", "silver", "gold"):
                c.execute(f"ATTACH DATABASE ? AS {ident(self.schema(layer))}", (os.path.join(self.dir, self.schema(layer) + ".db"),))
            c.create_function("try_cast", 2, _try_cast, deterministic=True)
            c.create_function("sha256", 1, lambda v: None if v is None else hashlib.sha256(str(v).encode()).hexdigest(), deterministic=True)
            c.create_function("regexp", 2, _regexp, deterministic=True)
            c.create_function("regexp_replace", 3, lambda v, p, r: None if v is None else re.sub(p, r, str(v)), deterministic=True)
            c.create_function("initcap", 1, lambda v: None if v is None else str(v).title(), deterministic=True)
            c.create_function("join_nonnull", -1, _join_nonnull, deterministic=True)
            self._tls.c = c
        return c

    # names
    def q(self, name):
        return f'"{ident(name, "column or table name")}"'

    def fq(self, layer, name):
        return f"{self.schema(layer)}.{self.q(name)}"

    def lit(self, value):
        return "'" + str(value).replace("'", "''") + "'"

    # running SQL
    def execute(self, sql):
        try:
            cur = self._db().execute(sql)
            return {"affected": cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else None}
        except sqlite3.Error as e:
            raise EngineError(f"{e}") from e

    def query(self, sql):
        try:
            return [dict(r) for r in self._db().execute(sql).fetchall()]
        except sqlite3.Error as e:
            raise EngineError(f"{e}") from e

    # dialect
    def typ(self, generic):
        t = norm_type(generic)
        return {"STRING": "TEXT", "INT": "INTEGER", "BIGINT": "INTEGER", "DOUBLE": "REAL", "BOOLEAN": "INTEGER",
                "DATE": "TEXT", "TIMESTAMP": "TEXT"}.get(t, "REAL")

    def try_cast(self, expr, generic):
        return f"try_cast({expr}, {self.lit(norm_type(generic))})"

    def sha256(self, expr):
        return f"sha256({expr})"

    def regex_ok(self, expr, pattern):
        return f"({expr} REGEXP {self.lit(pattern)})"

    def regexp_replace(self, expr, pattern, repl):
        return f"regexp_replace({expr}, {self.lit(pattern)}, {self.lit(repl)})"

    def initcap(self, expr):
        return f"initcap({expr})"

    def join_reasons(self, exprs):
        return "join_nonnull('; ', " + ", ".join(exprs) + ")"

    def row_json(self, alias, cols):
        return "json_object(" + ", ".join(f"{self.lit(c)}, {alias}.{self.q(c)}" for c in cols) + ")"

    def concat(self, parts):
        return "(" + " || ".join(parts) + ")"

    def now(self):
        return "strftime('%Y-%m-%d %H:%M:%S', 'now')"

    def today(self):
        return "date('now')"

    # statements
    def ctas(self, fq, select):
        return [f"DROP TABLE IF EXISTS {fq}", f"CREATE TABLE {fq} AS\n{select}"]

    def truncate(self, fq):
        return f"DELETE FROM {fq}"

    def upsert(self, target, source, keys, cols):
        on = " AND ".join(f"s.{self.q(k)} = t.{self.q(k)}" for k in keys)
        names = ", ".join(self.q(c) for c in cols)
        return [f"DELETE FROM {target} AS t WHERE EXISTS (SELECT 1 FROM {source} s WHERE {on})",
                f"INSERT INTO {target} ({names})\nSELECT {names} FROM {source}"]

    # metadata
    def ensure_schemas(self):
        self._db()

    def columns(self, layer, name):
        rows = self.query(f"PRAGMA {self.schema(layer)}.table_info({self.q(name)})")
        return [(r["name"], r["type"] or "TEXT") for r in rows]

    def stats(self, layer, name):
        s = super().stats(layer, name)
        path = os.path.join(self.dir, self.schema(layer) + ".db")
        s["bytes"] = None if not os.path.exists(path) else None
        return s

    def source_table(self, path):
        parts = str(path).split(".")
        if len(parts) != 2:
            raise EngineError("Name the source table as layer.table, for example bronze.orders_raw")
        schema, name = ident(parts[0], "schema"), ident(parts[1], "table")
        rows = self.query(f"PRAGMA {schema}.table_info({self.q(name)})")
        if not rows:
            raise EngineError(f"Source table {path} does not exist")
        return f"{schema}.{self.q(name)}", [(r["name"], r["type"] or "TEXT") for r in rows]

    # files
    _EXT = {"csv": ("*.csv",), "json": ("*.json", "*.jsonl", "*.ndjson")}

    def _folder(self, src, cfg):
        return os.path.join(os.path.abspath(src["endpoint"]), str(cfg.get("path") or "").strip("/"))

    def list_files(self, src, cfg):
        fmt = cfg.get("format", "csv")
        if fmt not in self._EXT:
            raise EngineError(f"The local engine reads CSV and JSON files. {fmt.upper()} needs BigQuery or Databricks.")
        folder = self._folder(src, cfg)
        if not os.path.isdir(folder):
            raise EngineError(f"Folder {folder} does not exist")
        out = []
        for pat in self._EXT[fmt]:
            for f in glob.glob(os.path.join(folder, "**", pat), recursive=True):
                st = os.stat(f)
                out.append({"name": os.path.relpath(f, folder), "size": st.st_size, "modified": int(st.st_mtime * 1000)})
        return sorted(out, key=lambda x: x["name"])

    def stage_files(self, src, cfg, files, stage_fq, since=None):
        folder, fmt = self._folder(src, cfg), cfg.get("format", "csv")
        rows, cols = [], []

        def col(name):
            c = slug(name) if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name.strip()) else name.strip()
            if c not in cols:
                cols.append(c)
            return c

        for f in files:
            path = os.path.join(folder, f["name"])
            if fmt == "csv":
                with open(path, newline="", encoding="utf-8-sig") as fh:
                    reader = csv.reader(fh)
                    header = next(reader, None)
                    if not header:
                        continue
                    names = [col(h) for h in header]
                    for rec in reader:
                        if not any(x.strip() for x in rec):
                            continue
                        rows.append({**{n: (rec[i] if i < len(rec) else None) for i, n in enumerate(names)}, "_source_file": f["name"]})
            else:
                with open(path, encoding="utf-8") as fh:
                    text = fh.read().strip()
                items = json.loads(text) if text.startswith("[") else [json.loads(line) for line in text.splitlines() if line.strip()]
                for obj in items:
                    rec = {}
                    for k, v in obj.items():
                        rec[col(k)] = None if v is None else v if isinstance(v, str) else json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                    rec["_source_file"] = f["name"]
                    rows.append(rec)
        db = self._db()
        db.execute(self.drop(stage_fq))
        all_cols = cols + ["_source_file"]
        db.execute(f"CREATE TABLE {stage_fq} (" + ", ".join(f"{self.q(c)} TEXT" for c in all_cols) + ")")
        if rows:
            db.execute("BEGIN")
            db.executemany(f"INSERT INTO {stage_fq} VALUES (" + ", ".join("?" for _ in all_cols) + ")",
                           [[r.get(c) for c in all_cols] for r in rows])
            db.execute("COMMIT")
        return cols

    def compute(self):
        return [{"id": "sqlite", "kind": "In-process SQLite", "spec": f"Files under {self.dir}", "state": "running", "canToggle": False}]
