"""Turns a pipeline definition into the SQL that builds each layer.

The same functions feed the run engine and the preview in the builder, so what you read is what runs.
"""
import re

from .engines.base import norm_type
from .util import ident, slug

LINEAGE = ["_ingest_ts", "_ingest_date", "_source_file", "_batch_id"]
PRESETS = ("", "trim", "lower", "upper", "initcap", "cast", "digits only", "passthrough")
DEFAULT_CTL = {"onNewColumn": "accept", "onEmpty": "skip", "dedup": True, "mask": True, "breaker": 5, "gate": True,
               "alertFail": True, "alertWarn": True, "alertSla": True, "alertOk": False, "email": True, "chat": False, "pager": False}


def parse_cols(text):
    out = []
    for line in str(text or "").splitlines():
        parts = [x.strip() for x in line.split(":")]
        if not parts or not parts[0]:
            continue
        flag = parts[2].lower() if len(parts) > 2 else ""
        name = parts[0] if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", parts[0]) else slug(parts[0])
        out.append({"name": name, "type": norm_type(parts[1] if len(parts) > 1 and parts[1] else "STRING"),
                    "key": "key" in flag, "pii": "pii" in flag})
    return out


def table_names(p):
    t, g = p["table"], p["gold"]
    return {"bronze": t + "_raw", "stg": t + "__stg", "silver": t, "chk": t + "__chk", "new": t + "__new",
            "quar": t + "__quarantine", "gold": g, "gold_new": g + "__new"}


def fqs(p, eng):
    n = table_names(p)
    layer = {"bronze": "bronze", "stg": "bronze", "silver": "silver", "chk": "silver", "new": "silver", "quar": "silver",
             "gold": "gold", "gold_new": "gold"}
    return {k: eng.fq(layer[k], v) for k, v in n.items()}


def build_pipeline(cfg, target_conn, engine_cls):
    """Validate a builder configuration and turn it into a pipeline record."""
    t = ident(slug(cfg.get("name")), "dataset name")
    cols = parse_cols(cfg.get("columns"))
    if not cols:
        raise ValueError("Add at least one column")
    gold = cfg.get("gold") or {}
    g = ident(slug(gold.get("name") or f"agg_{t}"), "Gold table name")
    keys = [c["name"] for c in cols if c["key"]] or [cols[0]["name"]]
    kind = cfg.get("kind") if cfg.get("kind") in ("files", "table") else "files"
    b, s = f"bronze_{t}", f"silver_{t}"
    tasks = [
        {"id": b, "label": f"Bronze {t}", "layer": "bronze", "deps": []},
        {"id": "dq_bronze", "label": "Bronze checks", "layer": "ops", "deps": [b]},
        {"id": s, "label": f"Silver {t}", "layer": "silver", "deps": ["dq_bronze"]},
        {"id": "dq_silver", "label": "Silver checks", "layer": "ops", "deps": [s]},
        {"id": f"gold_{g}", "label": f"Gold {g}", "layer": "gold", "deps": ["dq_silver"]},
        {"id": "dq_gold", "label": "Gold checks", "layer": "ops", "deps": [f"gold_{g}"]},
    ]
    ctl = dict(DEFAULT_CTL)
    ctl["mask"] = bool(cfg.get("maskPii", True))
    ctl["onNewColumn"] = {"failOnNewColumns": "stop", "rescue": "rescue"}.get(cfg.get("evolve"), "accept")
    return {
        "id": f"{t}_medallion", "name": f"{t}_medallion", "table": t, "gold": g, "target": target_conn["id"],
        "source": cfg.get("conn") or target_conn["id"], "platform": engine_cls.platform, "engine": engine_cls.label,
        "kind": kind, "format": cfg.get("format") or "csv", "path": str(cfg.get("path") or t).strip(),
        "pattern": "full" if cfg.get("pattern") == "full" else "incremental", "watermark": (cfg.get("watermark") or "").strip(),
        "scd": "2" if str(cfg.get("scd")) == "2" else "1", "keys": keys, "cfg": cfg, "status": "active",
        "cron": cfg.get("cron") or "0 * * * *", "retries": max(0, min(5, int(cfg.get("retries") or 0))), "sla": 120,
        "owner": "data-eng", "ctl": ctl, "tasks": tasks, "writes": [f"bronze.{t}_raw", f"silver.{t}", f"gold.{g}"],
        "orch": "Control plane scheduler", "mode": ("Batch, full load" if cfg.get("pattern") == "full" else "Batch, incremental"),
        "compute": engine_cls.label, "loaded": {}, "wm": None, "pending": [], "history": [], "lastSched": "",
    }


def default_mapping(p):
    cols = parse_cols(p["cfg"].get("columns"))
    mask = bool(p["cfg"].get("maskPii", True))
    return {"source": f"bronze.{p['table']}_raw",
            "rows": [{"src": c["name"], "stype": "STRING", "tgt": c["name"], "ttype": c["type"],
                      "tx": "trim" if c["type"] == "STRING" else "cast", "nullable": not c["key"], "pii": bool(c["pii"] and mask)} for c in cols]}


def default_rules(p):
    cols, t, on_bad = parse_cols(p["cfg"].get("columns")), p["table"], p["cfg"].get("onBad")
    rules = [("bronze." + t + "_raw", "*", "row_count", "at least 1 row per run", "fail")]
    for k in p["keys"]:
        rules.append((f"silver.{t}", k, "not_null", "", "fail"))
        rules.append((f"silver.{t}", k, "unique", "one current row per key" if p["scd"] == "2" else "", "fail"))
    for c in cols:
        if c["type"] != "STRING":
            rules.append((f"silver.{t}", c["name"], "castable", c["type"], "fail" if on_bad == "stop" else "quarantine"))
    rules.append((f"gold.{p['gold']}", "*", "row_count", "at least 1 row", "fail"))
    return rules


# ---------- Bronze ----------
def stage_table_sql(p, eng, src_fq, src_cols, since):
    wm = p.get("watermark")
    select = ", ".join(f"{eng.to_str(eng.q(c))} AS {eng.q(c)}" for c, _ in src_cols)
    where = f"\nWHERE {eng.to_str(eng.q(wm))} > {eng.lit(since)}" if wm and since else ""
    return f"SELECT {select}, {eng.lit('table:' + p['path'])} AS _source_file\nFROM {src_fq}{where}"


def bronze_sql(p, eng, stage_cols, existing, batch, full):
    """existing: data columns already in the Bronze table, or None when the table does not exist yet."""
    f, ctl = fqs(p, eng), p["ctl"]
    rescue = ctl.get("onNewColumn") == "rescue"
    lineage = f"{eng.now()} AS _ingest_ts, {eng.today()} AS _ingest_date, s._source_file AS _source_file, {eng.lit(batch)} AS _batch_id"
    if existing is None:
        cols = ", ".join(f"s.{eng.q(c)} AS {eng.q(c)}" for c in stage_cols)
        extra = f", CAST(NULL AS {eng.typ('STRING')}) AS _rescued_data" if rescue else ""
        return eng.ctas(f["bronze"], f"SELECT {cols}, {lineage}{extra}\nFROM {f['stg']} s")
    new = [c for c in stage_cols if c not in existing]
    out, data = [], list(existing)
    if new and not rescue:
        out += [eng.add_column(f["bronze"], c, "STRING") for c in new]
        data += new
    out.append(eng.truncate(f["bronze"]) if full else f"DELETE FROM {f['bronze']} WHERE _batch_id = {eng.lit(batch)}")
    names = [eng.q(c) for c in data] + LINEAGE
    values = [f"s.{eng.q(c)}" if c in stage_cols else f"CAST(NULL AS {eng.typ('STRING')})" for c in data]
    select = ", ".join(values) + f", {eng.now()}, {eng.today()}, s._source_file, {eng.lit(batch)}"
    if rescue:
        names.append("_rescued_data")
        select += ", " + (eng.row_json("s", new) if new else f"CAST(NULL AS {eng.typ('STRING')})")
    out.append(f"INSERT INTO {f['bronze']} ({', '.join(names)})\nSELECT {select}\nFROM {f['stg']} s")
    return out


# ---------- Silver ----------
def map_expr(r, eng, bronze_cols, alias="b"):
    """Returns (text expression before the cast, typed expression)."""
    src, tx = r.get("src") or "", (r.get("tx") or "").strip()
    ttype = norm_type(r.get("ttype"))
    low = tx.lower()
    if low in PRESETS:
        if src not in bronze_cols:
            base = f"CAST(NULL AS {eng.typ('STRING')})"
        elif src.startswith("_"):
            base = f"{alias}.{eng.q(src)}"
        else:
            ref = f"NULLIF(TRIM({alias}.{eng.q(src)}), '')"
            base = {"lower": f"LOWER({ref})", "upper": f"UPPER({ref})", "initcap": eng.initcap(ref),
                    "digits only": f"NULLIF({eng.regexp_replace(ref, '[^0-9]', '')}, '')"}.get(low, ref)
    else:
        base = f"({tx})"
    if ttype == "STRING":
        return base, base
    if src.startswith("_") and low in PRESETS:
        return base, base
    return base, eng.try_cast(base, ttype)


def parse_range(param):
    nums = [float(x) for x in re.findall(r"-?\d+(?:\.\d+)?", str(param or ""))]
    low = str(param or "").lower()
    if not nums:
        return None, None
    if len(nums) >= 2:
        return nums[0], nums[1]
    if any(w in low for w in ("or less", "at most", "<=", "below", "up to")):
        return None, nums[0]
    return nums[0], None


def _num(x):
    return str(int(x)) if float(x).is_integer() else str(x)


def rule_flag(rule, eng, ref, mapping_rows=None):
    """SQL that is TRUE for a bad row, written against the alias `ref`. None when the rule is not a per-row rule."""
    col, typ, param = rule.get("column"), rule.get("type"), str(rule.get("param") or "")
    if typ == "custom_sql":
        return f"NOT COALESCE(({param}), FALSE)" if param.strip() else None
    if not col or col == "*" or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", col):
        return None
    c = f"{ref}.{eng.q(col)}"
    if typ == "not_null":
        return f"{c} IS NULL"
    if typ == "castable":
        if mapping_rows is None:
            return f"({c} IS NOT NULL AND TRIM({c}) <> '' AND {eng.try_cast(c, param or 'STRING')} IS NULL)"
        row = next((r for r in mapping_rows if r["tgt"] == col), None)
        if not row or norm_type(row.get("ttype")) == "STRING":
            return None
        return f"({ref}.{eng.q(col + '__raw')} IS NOT NULL AND {c} IS NULL)"
    if typ == "range":
        lo, hi = parse_range(param)
        conds = ([f"{c} < {_num(lo)}"] if lo is not None else []) + ([f"{c} > {_num(hi)}"] if hi is not None else [])
        return "(" + " OR ".join(conds) + ")" if conds else None
    if typ == "accepted_values":
        vals = [v.strip().strip("'\"") for v in param.split(",") if v.strip()]
        return f"({c} IS NOT NULL AND {eng.to_str(c)} NOT IN ({', '.join(eng.lit(v) for v in vals)}))" if vals else None
    if typ == "regex":
        return f"({c} IS NOT NULL AND NOT {eng.regex_ok(c, param)})" if param else None
    if typ == "referential":
        m = re.search(r"(bronze|silver|gold)\.([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", param)
        if not m:
            return None
        other = eng.fq(m.group(1), m.group(2))
        return f"({c} IS NOT NULL AND {c} NOT IN (SELECT {eng.q(m.group(3))} FROM {other} WHERE {eng.q(m.group(3))} IS NOT NULL))"
    return None


def rule_label(rule):
    name = rule["type"].replace("_", " ")
    extra = f" ({rule['param']})" if rule.get("param") and rule["type"] not in ("custom_sql",) else ""
    return f"{rule['column']}: {name}{extra}" if rule.get("column") not in (None, "*") else f"{name}{extra}"


def silver_check_sql(p, eng, mapping_rows, rules, bronze_cols, batches, full):
    """Builds the checked staging table: every mapped column typed, plus one flag column per row-level rule.
    Returns (statements, [(rule, flag column)])."""
    f = fqs(p, eng)
    data_cols = [c for c in bronze_cols if c not in LINEAGE and c != "_rescued_data"]
    cast_rules = {r["column"] for r in rules if r["type"] == "castable"}
    inner = []
    for r in mapping_rows:
        base, typed = map_expr(r, eng, bronze_cols)
        inner.append(f"{typed} AS {eng.q(r['tgt'])}")
        if r["tgt"] in cast_rules and norm_type(r.get("ttype")) != "STRING":
            inner.append(f"{base} AS {eng.q(r['tgt'] + '__raw')}")
    inner.append("b._ingest_ts AS _ingest_ts")
    inner.append(f"{eng.row_json('b', data_cols)} AS _row")
    where = "" if full or not batches else "\n  WHERE b._batch_id IN (" + ", ".join(eng.lit(x) for x in batches) + ")"
    flags = []
    for r in rules:
        cond = rule_flag(r, eng, "s", mapping_rows)
        if cond:
            flags.append((r, "_f_" + re.sub(r"[^A-Za-z0-9_]", "_", r["id"]), cond))
    flag_sql = "".join(f",\n  COALESCE({cond}, FALSE) AS {name}" for _, name, cond in flags)
    select = "SELECT s.*" + flag_sql + "\nFROM (\n  SELECT " + ",\n    ".join(inner) + f"\n  FROM {f['bronze']} b{where}\n) s"
    return eng.ctas(f["chk"], select), [(r, name) for r, name, _ in flags]


def silver_new_sql(p, eng, mapping_rows, exclude_flags):
    """The rows that will be merged: bad rows left out, duplicates removed, personal data masked."""
    f, ctl, keys = fqs(p, eng), p["ctl"], p["keys"]
    cols = []
    for r in mapping_rows:
        c, t = f"y.{eng.q(r['tgt'])}", norm_type(r.get("ttype"))
        if r.get("pii") and ctl.get("mask") and t == "STRING":
            cols.append(f"{eng.sha256(f'LOWER({c})')} AS {eng.q(r['tgt'])}")
        else:
            cols.append(f"CAST({c} AS {eng.typ(t)}) AS {eng.q(r['tgt'])}")
    cols.append("y._ingest_ts AS _ingest_ts")
    if p["scd"] == "2":
        tracked = [r["tgt"] for r in mapping_rows if r["tgt"] not in keys]
        parts = []
        for c in tracked:
            parts += [f"COALESCE({eng.to_str('y.' + eng.q(c))}, '')", "'|'"]
        cols.append(f"{eng.sha256(eng.concat(parts or [chr(39) + chr(39)]))} AS row_hash")
    where = "\n  WHERE NOT (" + " OR ".join(f"x.{n}" for n in exclude_flags) + ")" if exclude_flags else ""
    if ctl.get("dedup", True):
        part = ", ".join(f"x.{eng.q(k)}" for k in keys)
        src = f"(\n  SELECT x.*, ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY x._ingest_ts DESC) AS _rn\n  FROM {f['chk']} x{where}\n) y\nWHERE y._rn = 1"
    else:
        src = f"(\n  SELECT x.*\n  FROM {f['chk']} x{where}\n) y"
    return eng.ctas(f["new"], "SELECT " + ",\n  ".join(cols) + "\nFROM " + src)


def silver_columns(p, mapping_rows):
    cols = [(r["tgt"], norm_type(r.get("ttype"))) for r in mapping_rows] + [("_ingest_ts", "TIMESTAMP")]
    if p["scd"] == "2":
        cols += [("row_hash", "STRING"), ("valid_from", "TIMESTAMP"), ("valid_to", "TIMESTAMP"), ("is_current", "BOOLEAN")]
    return cols


def silver_write_sql(p, eng, mapping_rows, existing, full):
    """existing: column names already in the Silver table, or None when it does not exist yet."""
    f, keys = fqs(p, eng), p["keys"]
    base = [r["tgt"] for r in mapping_rows] + ["_ingest_ts"]
    names = ", ".join(eng.q(c) for c in base)
    scd2 = p["scd"] == "2"
    ts = eng.typ("TIMESTAMP")
    if existing is None:
        if scd2:
            return eng.ctas(f["silver"], f"SELECT {names}, row_hash, _ingest_ts AS valid_from, CAST(NULL AS {ts}) AS valid_to, TRUE AS is_current\nFROM {f['new']}")
        return eng.ctas(f["silver"], f"SELECT {names}\nFROM {f['new']}")
    out = [eng.add_column(f["silver"], c, t) for c, t in silver_columns(p, mapping_rows) if c not in existing]
    if scd2:
        on = " AND ".join(f"s.{eng.q(k)} = t.{eng.q(k)}" for k in keys)
        if full:
            out.append(eng.truncate(f["silver"]))
        else:
            out.append(f"UPDATE {f['silver']} AS t\nSET is_current = FALSE, valid_to = {eng.now()}\nWHERE t.is_current\n"
                       f"  AND EXISTS (SELECT 1 FROM {f['new']} s WHERE {on} AND s.row_hash <> t.row_hash)")
        out.append(f"INSERT INTO {f['silver']} ({names}, row_hash, valid_from, valid_to, is_current)\n"
                   f"SELECT {', '.join('s.' + eng.q(c) for c in base)}, s.row_hash, s._ingest_ts, CAST(NULL AS {ts}), TRUE\nFROM {f['new']} s\n"
                   f"WHERE NOT EXISTS (SELECT 1 FROM {f['silver']} t WHERE {on} AND t.is_current AND t.row_hash = s.row_hash)")
        return out
    if full:
        return out + [eng.truncate(f["silver"]), f"INSERT INTO {f['silver']} ({names})\nSELECT {names}\nFROM {f['new']}"]
    return out + eng.upsert(f["silver"], f["new"], keys, base)


# ---------- Gold ----------
def gold_select(p, eng):
    f, g, t = fqs(p, eng), p["cfg"].get("gold") or {}, p["table"]
    current = "\nWHERE is_current" if p["scd"] == "2" else ""
    kind = g.get("type") or "aggregate"
    if kind == "sql":
        sql = str(g.get("sql") or "").strip().rstrip(";")
        if not re.match(r"^\s*(select|with)\b", sql, re.I):
            raise ValueError("The Gold SQL must be a single SELECT statement")
        sql = sql.replace("{silver}", f["silver"]).replace("{bronze}", f["bronze"])
        return re.sub(r"\{(bronze|silver|gold)\.([A-Za-z0-9_]+)\}", lambda m: eng.fq(m.group(1), m.group(2)), sql)
    if kind == "dimension":
        cols = parse_cols(p["cfg"].get("columns"))
        keys = p["keys"] + (["valid_from"] if p["scd"] == "2" else [])
        parts = []
        for k in keys:
            parts += [f"COALESCE({eng.to_str(eng.q(k))}, '')", "'|'"]
        attrs = [eng.q(c["name"]) for c in cols] + (["valid_from", "valid_to", "is_current"] if p["scd"] == "2" else [])
        return (f"SELECT {eng.sha256(eng.concat(parts))} AS {eng.q(t + '_sk')},\n  " + ",\n  ".join(attrs) +
                f",\n  {eng.now()} AS _built_at\nFROM {f['silver']}")
    grain = [ident(x.strip(), "Gold column") for x in ([g.get("dateCol") or ""] + str(g.get("dims") or "").split(",")) if x.strip()]
    measures = [m.strip().rstrip(",") for m in str(g.get("measures") or "COUNT(*) AS row_count").splitlines() if m.strip()]
    select = ",\n  ".join([eng.q(x) for x in grain] + measures + [f"{eng.now()} AS _built_at"])
    group = "\nGROUP BY " + ", ".join(eng.q(x) for x in grain) if grain else ""
    return f"SELECT\n  {select}\nFROM {f['silver']}{current}{group}"


def gold_sql(p, eng):
    f = fqs(p, eng)
    return eng.ctas(f["gold_new"] if p["ctl"].get("gate", True) else f["gold"], gold_select(p, eng))


def plan(p, eng, mapping_rows, rules):
    """The statements a run would execute, for display. Column lists assume the declared columns."""
    declared = [c["name"] for c in parse_cols(p["cfg"].get("columns"))]
    f = fqs(p, eng)
    silver_rules = [r for r in rules if r["table"] == f"silver.{p['table']}" and r.get("on", True)]
    chk, flags = silver_check_sql(p, eng, mapping_rows, silver_rules, declared + LINEAGE, ["<run id>"], p["pattern"] == "full")
    excl = [name for r, name in flags if r["severity"] in ("quarantine", "fail")]
    source = "-- Files are staged into " + f["stg"] + " as text, one column per source column\n" if p["kind"] == "files" else ""
    return {
        "bronze": source + ";\n\n".join(bronze_sql(p, eng, declared, None, "<run id>", False)) + ";",
        "silver": ";\n\n".join(chk + silver_new_sql(p, eng, mapping_rows, excl) + silver_write_sql(p, eng, mapping_rows, [c for c, _ in silver_columns(p, mapping_rows)], p["pattern"] == "full")) + ";",
        "gold": ";\n\n".join(gold_sql(p, eng) + ([f"-- after the Gold checks pass\n" + eng.ctas(f["gold"], f"SELECT * FROM {f['gold_new']}")[-1]] if p["ctl"].get("gate", True) else [])) + ";",
    }
