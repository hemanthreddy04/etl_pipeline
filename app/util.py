import datetime as dt
import re
import time

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def ident(name, what="name"):
    """Names that end up inside SQL must be plain identifiers."""
    if not isinstance(name, str) or not IDENT.match(name):
        raise ValueError(f"{what} '{name}' may only use letters, digits and underscores, and must not start with a digit")
    return name


def slug(text):
    s = re.sub(r"[^a-z0-9_]", "_", str(text or "").strip().lower()).strip("_")
    if not s:
        return "dataset"
    return s if not s[0].isdigit() else "c_" + s


def now_ms():
    return int(time.time() * 1000)


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


# ---------- cron (five fields, UTC) ----------
def _field(expr, value, low):
    if expr == "*":
        return True
    for part in expr.split(","):
        m = re.match(r"^(\*|\d+)(?:-(\d+))?(?:/(\d+))?$", part)
        if not m:
            return False
        a = low if m.group(1) == "*" else int(m.group(1))
        if m.group(1) == "*":
            b = 99
        elif m.group(2) is not None:
            b = int(m.group(2))
        else:
            b = 99 if m.group(3) else a
        step = int(m.group(3)) if m.group(3) else 1
        if a <= value <= b and (value - a) % step == 0:
            return True
    return False


_CRON_FIELD = re.compile(r"^(\*|\d+)(-\d+)?(/\d+)?(,(\*|\d+)(-\d+)?(/\d+)?)*$")


def cron_valid(cron):
    f = str(cron or "").split()
    return len(f) == 5 and all(_CRON_FIELD.match(x) for x in f)


def cron_match(cron, t):
    f = str(cron or "").split()
    if len(f) != 5:
        return False
    dow = (t.weekday() + 1) % 7   # cron: 0 is Sunday
    return (_field(f[0], t.minute, 0) and _field(f[1], t.hour, 0) and _field(f[2], t.day, 1)
            and _field(f[3], t.month, 1) and _field(f[4], dow, 0))
