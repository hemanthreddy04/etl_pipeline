"""One interface, three warehouses.

An Engine runs SQL and knows the handful of places where the dialects differ: type names, safe casts,
hashing, regular expressions, upserts and how files are read. Everything else in the service is written
once against this interface.
"""
import re

from ..util import ident

LAYERS = ("bronze", "silver", "gold")
GENERIC_TYPES = ["STRING", "INT", "BIGINT", "DECIMAL(18,2)", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP"]
_ALIASES = {"INT64": "BIGINT", "INTEGER": "INT", "NUMERIC": "DECIMAL(18,2)", "BIGNUMERIC": "DECIMAL(18,2)", "FLOAT64": "DOUBLE",
            "FLOAT": "DOUBLE", "REAL": "DOUBLE", "BOOL": "BOOLEAN", "VARCHAR": "STRING", "TEXT": "STRING", "DATETIME": "TIMESTAMP"}


class EngineError(Exception):
    """The warehouse rejected a statement or could not be reached."""


def norm_type(t):
    t = str(t or "STRING").strip().upper()
    if t in _ALIASES:
        return _ALIASES[t]
    if re.match(r"^DECIMAL\(\d+,\s*\d+\)$", t):
        return t.replace(" ", "")
    if t == "DECIMAL":
        return "DECIMAL(18,2)"
    return t if t in GENERIC_TYPES else "STRING"


class Engine:
    kind = ""
    platform = ""
    label = ""
    table_format = "Table"
    file_connection = ""        # which connection type supplies files for this engine

    def __init__(self, conn, settings):
        self.conn = conn
        self.opt = conn.get("options") or {}
        self.prefix = settings.schema_prefix
        if self.prefix:
            ident(self.prefix, "schema prefix")

    # ----- names -----
    def schema(self, layer):
        return f"{self.prefix}{layer}"

    def q(self, name):
        return f"`{ident(name, 'column or table name')}`"

    def fq(self, layer, name):
        raise NotImplementedError

    def lit(self, value):
        return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"

    # ----- running SQL -----
    def execute(self, sql):
        """Run a statement. Returns {'affected': n or None}."""
        raise NotImplementedError

    def query(self, sql):
        """Run a query and return a list of dicts."""
        raise NotImplementedError

    def scalar(self, sql):
        rows = self.query(sql)
        return list(rows[0].values())[0] if rows else None

    def test(self):
        self.query("SELECT 1 AS ok")
        return "Query ran"

    # ----- dialect -----
    def typ(self, generic):
        return norm_type(generic)

    def try_cast(self, expr, generic):
        return f"TRY_CAST({expr} AS {self.typ(generic)})"

    def to_str(self, expr):
        return f"CAST({expr} AS {self.typ('STRING')})"

    def sha256(self, expr):
        raise NotImplementedError

    def regex_ok(self, expr, pattern):
        raise NotImplementedError

    def regexp_replace(self, expr, pattern, repl):
        raise NotImplementedError

    def initcap(self, expr):
        return f"INITCAP({expr})"

    def join_reasons(self, exprs):
        raise NotImplementedError

    def row_json(self, alias, cols):
        raise NotImplementedError

    def concat(self, parts):
        return "CONCAT(" + ", ".join(parts) + ")"

    def now(self):
        return "CURRENT_TIMESTAMP()"

    def today(self):
        return "CURRENT_DATE()"

    # ----- statements that differ -----
    def ctas(self, fq, select):
        return [f"CREATE OR REPLACE TABLE {fq} AS\n{select}"]

    def create_if_missing(self, fq, cols):
        return f"CREATE TABLE IF NOT EXISTS {fq} (" + ", ".join(f"{self.q(n)} {self.typ(t)}" for n, t in cols) + ")"

    def drop(self, fq):
        return f"DROP TABLE IF EXISTS {fq}"

    def truncate(self, fq):
        return f"TRUNCATE TABLE {fq}"

    def add_column(self, fq, col, generic):
        return f"ALTER TABLE {fq} ADD COLUMN {self.q(col)} {self.typ(generic)}"

    def upsert(self, target, source, keys, cols):
        on = " AND ".join(f"t.{self.q(k)} = s.{self.q(k)}" for k in keys)
        sets = ", ".join(f"{self.q(c)} = s.{self.q(c)}" for c in cols if c not in keys)
        names = ", ".join(self.q(c) for c in cols)
        vals = ", ".join(f"s.{self.q(c)}" for c in cols)
        matched = f"\nWHEN MATCHED THEN UPDATE SET {sets}" if sets else ""
        return [f"MERGE INTO {target} t\nUSING {source} s\nON {on}{matched}\nWHEN NOT MATCHED THEN INSERT ({names}) VALUES ({vals})"]

    # ----- metadata -----
    def ensure_schemas(self):
        raise NotImplementedError

    def columns(self, layer, name):
        """[(column, type)] or [] when the table does not exist."""
        raise NotImplementedError

    def exists(self, layer, name):
        return bool(self.columns(layer, name))

    def stats(self, layer, name):
        """{'rows': n, 'bytes': n or None}"""
        return {"rows": int(self.scalar(f"SELECT COUNT(*) AS n FROM {self.fq(layer, name)}") or 0), "bytes": None}

    def source_table(self, path):
        """Resolve a table the user named as a source. Returns (fully qualified name, [(column, type)])."""
        raise NotImplementedError

    # ----- files -----
    def list_files(self, src, cfg):
        """[{'name', 'size', 'modified'}] for the files under the source folder."""
        raise NotImplementedError

    def stage_files(self, src, cfg, files, stage_fq, since=None):
        """Create the staging table from the given files, every column as text plus _source_file.
        Returns the list of data column names."""
        raise NotImplementedError

    def compute(self):
        """Compute resources this engine can show. [{'id','kind','spec','state','canToggle'}]"""
        return []
