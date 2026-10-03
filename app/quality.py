"""Data quality checks. Each rule becomes SQL that counts how many rows (or how many table-level facts) fail.

Row-level rules: not_null, castable, range, accepted_values, regex, referential, custom_sql.
Table-level rules: unique, row_count, freshness, reconciliation, volume_anomaly, schema.
"""
import re

from .engines.base import norm_type
from .medallion import LINEAGE, fqs, map_expr, rule_flag
from .util import now_ms

DIMENSION = {"not_null": "Completeness", "unique": "Uniqueness", "range": "Validity", "accepted_values": "Validity",
             "regex": "Validity", "castable": "Validity", "referential": "Consistency", "freshness": "Timeliness",
             "row_count": "Completeness", "volume_anomaly": "Completeness", "reconciliation": "Accuracy",
             "schema": "Consistency", "custom_sql": "Consistency"}
ROW_RULES = ("not_null", "castable", "range", "accepted_values", "regex", "referential", "custom_sql")


def first_number(text, default):
    m = re.search(r"-?\d+(?:\.\d+)?", str(text or ""))
    return float(m.group(0)) if m else default


def minutes(text, default=120):
    m = re.search(r"(\d+(?:\.\d+)?)\s*(minute|min|hour|hr|day)", str(text or "").lower())
    if not m:
        return default
    n = float(m.group(1))
    return n * (1 if m.group(2).startswith("min") else 60 if m.group(2).startswith("h") else 1440)


def pass_rate(checked, failed):
    return 100.0 if not checked else round(max(0.0, 100.0 * (1 - failed / checked)), 4)


def violated(rule, checked, failed):
    return pass_rate(checked, failed) < float(rule.get("min", 100) if rule.get("min") is not None else 100)


def record(rule, checked, failed):
    rule.update({"checked": int(checked), "failed": int(failed), "pass": pass_rate(checked, failed), "last": now_ms()})


def evaluate(rule, eng, p, state, ctx=None):
    """Count failures for one rule against the table as it stands.

    ctx (optional, during a run): {'batches': [...], 'override': {table id: fq}, 'drift': [...], 'bronze_rows': n,
    'silver_rows': n, 'quarantined': n, 'history': [...]}.
    Returns (checked, failed, note) or None when the rule cannot be evaluated here.
    """
    ctx = ctx or {}
    layer, name = rule["table"].split(".", 1)
    f = fqs(p, eng)
    fq = (ctx.get("override") or {}).get(rule["table"]) or eng.fq(layer, name)
    typ, col, param = rule["type"], rule.get("column") or "*", str(rule.get("param") or "")
    scope = ""
    if layer == "bronze" and ctx.get("batches"):
        scope = " WHERE x._batch_id IN (" + ", ".join(eng.lit(b) for b in ctx["batches"]) + ")"

    if typ == "schema":
        drift = ctx.get("drift")
        if drift is None:
            pending = [d for d in state.get("drift", []) if d["table"] == rule["table"] and d["status"] == "pending"]
            return 1, (1 if pending else 0), ("new columns are waiting for a decision" if pending else "")
        return 1, (1 if drift else 0), ("new columns: " + ", ".join(drift) if drift else "")

    if typ == "freshness":
        fresh = (state.get("tstats", {}).get(rule["table"]) or {}).get("fresh")
        if ctx.get("in_run"):
            return 1, 0, "written by this run"
        if not fresh:
            return None
        age = (now_ms() - fresh) / 60000
        limit = minutes(param)
        return 1, (1 if age > limit else 0), f"{age:.0f} minutes old, limit {limit:.0f}"

    if typ == "volume_anomaly":
        rows, hist = ctx.get("bronze_rows"), [h["rows"] for h in (p.get("history") or [])][-7:]
        if rows is None or len(hist) < 3:
            return None
        avg = sum(hist) / len(hist)
        tol = first_number(param, 30)
        off = abs(rows - avg) / avg * 100 if avg else 0
        return 1, (1 if off > tol else 0), f"{rows} rows against a recent average of {avg:.0f} ({off:.0f}% away, limit {tol:.0f}%)"

    if typ == "row_count":
        need = int(first_number(param, 1))
        n = int(eng.scalar(f"SELECT COUNT(*) AS n FROM {fq} x{scope}") or 0)
        return 1, (1 if n < need else 0), f"{n} rows, at least {need} expected"

    if typ == "unique":
        cols = [col] if col != "*" else p["keys"]
        where = " WHERE x.is_current" if layer == "silver" and p["scd"] == "2" else scope
        expr = f"x.{eng.q(cols[0])}"
        if len(cols) > 1:
            parts = []
            for c in cols:
                parts += [f"COALESCE({eng.to_str('x.' + eng.q(c))}, '')", "'|'"]
            expr = eng.concat(parts)
        r = eng.query(f"SELECT COUNT(*) AS n, COUNT(DISTINCT {expr}) AS d FROM {fq} x{where}")[0]
        n, d = int(r["n"] or 0), int(r["d"] or 0)
        return n, max(0, n - d), ""

    if typ == "reconciliation":
        tol = first_number(param, 0.1)
        if layer == "gold" and col != "*":
            m = re.search(r"of\s+(.+?)\s+in\s+silver", param, re.I)
            silver_expr = m.group(1) if m else f"SUM({eng.q(col)})"
            where = " WHERE is_current" if p["scd"] == "2" else ""
            g = float(eng.scalar(f"SELECT SUM({eng.q(col)}) AS v FROM {fq}") or 0)
            s = float(eng.scalar(f"SELECT {silver_expr} AS v FROM {f['silver']}{where}") or 0)
            off = abs(g - s) / abs(s) * 100 if s else (0 if g == 0 else 100)
            return 1, (1 if off > tol else 0), f"Gold {g:.2f} against Silver {s:.2f} ({off:.3f}% apart, tolerance {tol}%)"
        if layer == "silver" and ctx.get("bronze_rows") is not None:
            b, s, qn = ctx["bronze_rows"], ctx.get("silver_rows") or 0, ctx.get("quarantined") or 0
            lost = max(0, b - s - qn)
            off = lost / b * 100 if b else 0
            return 1, (1 if off > tol else 0), f"{b} rows in, {s} merged, {qn} quarantined, {lost} removed as duplicates ({off:.2f}%, tolerance {tol}%)"
        return None

    if typ in ROW_RULES:
        if typ == "castable" and layer == "silver":
            # after the merge a bad value is already NULL, so look at the raw text in Bronze
            mapping = (state.get("mappings", {}).get(rule["table"]) or {}).get("rows", [])
            row = next((r for r in mapping if r["tgt"] == col), None)
            if not row or norm_type(row.get("ttype")) == "STRING":
                return None
            bronze_cols = [c for c, _ in eng.columns("bronze", p["table"] + "_raw")]
            if not bronze_cols:
                return None
            base, typed = map_expr(row, eng, bronze_cols)
            r = eng.query(f"SELECT COUNT(*) AS n, SUM(CASE WHEN {base} IS NOT NULL AND {typed} IS NULL THEN 1 ELSE 0 END) AS f FROM {f['bronze']} b")[0]
            return int(r["n"] or 0), int(r["f"] or 0), "measured on the raw values in Bronze"
        cond = rule_flag(rule, eng, "x")
        if not cond:
            return None
        where = scope or (" WHERE x.is_current" if layer == "silver" and p["scd"] == "2" else "")
        r = eng.query(f"SELECT COUNT(*) AS n, SUM(CASE WHEN {cond} THEN 1 ELSE 0 END) AS f FROM {fq} x{where}")[0]
        return int(r["n"] or 0), int(r["f"] or 0), ""
    return None


def recommended(p, layer, tables):
    """The usual starting checks for a layer. Returns [(table, column, type, param, severity)]."""
    out = []
    t = p["table"]
    if layer == "bronze":
        tid = f"bronze.{t}_raw"
        out += [(tid, "*", "row_count", "at least 1 row per run", "fail"), (tid, "*", "schema", "no unannounced columns", "warn"),
                (tid, "*", "volume_anomaly", "within 30% of the recent average", "warn")]
    if layer == "silver":
        tid = f"silver.{t}"
        for k in p["keys"]:
            out += [(tid, k, "not_null", "", "fail"), (tid, k, "unique", "one current row per key" if p["scd"] == "2" else "", "fail")]
        mapping = tables.get(tid, [])
        for r in mapping:
            if norm_type(r.get("ttype")) != "STRING" and r["tgt"] not in LINEAGE:
                out.append((tid, r["tgt"], "castable", norm_type(r.get("ttype")), "quarantine"))
        out.append((tid, "*", "reconciliation", "within 5% of the rows that arrived in Bronze", "warn"))
    if layer == "gold":
        tid = f"gold.{p['gold']}"
        out += [(tid, "*", "row_count", "at least 1 row", "fail"), (tid, "*", "freshness", "newer than 2 hours", "warn")]
        g = p["cfg"].get("gold") or {}
        if (g.get("type") or "aggregate") == "aggregate":
            for line in str(g.get("measures") or "").splitlines():
                m = re.match(r"^\s*SUM\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)", line, re.I)
                if m:
                    out.append((tid, m.group(2), "reconciliation", f"within 0.1% of SUM({m.group(1)}) in Silver", "fail"))
    return out
