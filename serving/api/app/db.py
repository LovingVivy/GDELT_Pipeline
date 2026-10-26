"""Kết nối ClickHouse dùng chung cho mọi request."""
import os
import threading

import clickhouse_connect
from clickhouse_connect import common

# Một client dùng chung cho nhiều luồng thì không được gắn session id.
common.set_setting("autogenerate_session_id", False)

_client = None
_lock = threading.Lock()


def client():
    global _client
    with _lock:
        if _client is None:
            _client = clickhouse_connect.get_client(
                host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                username=os.getenv("CLICKHOUSE_USER", "gdelt"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                database=os.getenv("CLICKHOUSE_DB", "gdelt"),
            )
        return _client


def rows(sql: str, **params) -> list[dict]:
    """Chạy truy vấn có tham số ({ten:Kieu}) và trả về danh sách dict."""
    result = client().query(sql, parameters=params)
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def row(sql: str, **params) -> dict:
    found = rows(sql, **params)
    return found[0] if found else {}
