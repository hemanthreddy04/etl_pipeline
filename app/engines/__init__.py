"""Engines by connection type. An engine is created once per connection and reused."""
import threading

from ..config import settings
from .base import Engine, EngineError, LAYERS, norm_type  # noqa: F401
from .bigquery_engine import BigQueryEngine
from .databricks_engine import DatabricksEngine
from .sqlite_engine import SQLiteEngine

ENGINE_TYPES = {"local": SQLiteEngine, "bq": BigQueryEngine, "adb": DatabricksEngine}
_cache, _lock = {}, threading.Lock()


def get_engine(conn):
    if not conn or conn.get("type") not in ENGINE_TYPES:
        raise EngineError("This pipeline has no warehouse connection. Add a BigQuery, Databricks or local connection first.")
    key = (conn["id"], conn.get("endpoint"), str(conn.get("options")), conn.get("secret"))
    with _lock:
        if key not in _cache:
            _cache[key] = ENGINE_TYPES[conn["type"]](conn, settings)
        return _cache[key]
