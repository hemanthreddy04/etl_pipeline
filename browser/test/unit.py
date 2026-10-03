import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app.engines.sqlite_engine import _try_cast
vals = ["12", " 12 ", "+7", "-3", "1.0", "1e3", ".5", "5.", "abc", "", "  ", None, 3, 4.0, 4.5, "12,90", "TRUE", "y", "No", "2", "2026-10-01", "2026/10/01", "2026-2-3", "2026-02-30", "2024-02-29", "2023-02-29",
        "2026-10-01 08:00:00", "2026-10-01T08:00:00", "2026-10-01T08:00:00Z", "2026-10-01 08:00:00Z", "2026-10-01 08:00", "2026-10-01T08:00", "2026-10-01 08:00:00.123456", "2026-10-01T08:00:00.5Z",
        "2026-10-01 24:00:00", "2026-10-01 8:5:9", "2026-13-01 10:06:00", "not a date", "01/10/2026", "0001-01-01", "99999999999999999999", "1_000", "0x10", "١٢", "2026-10-01 08:00:60", "20261001", "2026-10-01 ", "inf", "nan", "1e400"]
types = ["INT", "BIGINT", "DOUBLE", "DECIMAL(18,2)", "BOOLEAN", "DATE", "TIMESTAMP", "STRING", "weird"]
json.dump([[v, t, _try_cast(v, t)] for v in vals for t in types], open(sys.argv[1], "w"))
