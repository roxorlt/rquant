"""Module 16: read-only SQL over the Serving replica.

Defence in depth: admin only; one SELECT/WITH statement; keyword/file-function denylist;
every referenced table must be on the allowlist (``RQUANT_WEB_QUERY_TABLES`` comma list
overrides the default); 1000-row cap; and the Serving connection is read-only with
external access disabled and configuration locked (see ``ServingSource.query_sandboxed``).
"""

from __future__ import annotations

import os
import re

import duckdb
from pydantic import BaseModel

MAX_ROWS = 1000
_START = re.compile(r"^\s*(select|with)\b", re.I)
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|create|drop|alter|attach|detach|copy|export|import|install|load|"
    r"pragma|set|reset|call|checkpoint|vacuum|use)\b|read_\w+|glob\s*\(|\bduckdb_\w+|"
    r"information_schema|pg_catalog|sqlite_master|getenv|current_setting", re.I)

DEFAULT_TABLES = frozenset({
    "alert_ack", "alert_rule", "benchmark_daily", "canvas_definition", "canvas_hit",
    "dashboard_summary", "manual_watchlist", "market_overview", "market_snapshot",
    "monitor_event", "paper_accounts", "paper_holdings", "runtime_services", "screen_result",
    "signals", "stock_basic", "strategy_summary", "strategy_trade",
})


def allowed_tables() -> frozenset[str]:
    raw = os.environ.get("RQUANT_WEB_QUERY_TABLES", "")
    names = {t.strip().lower() for t in raw.split(",") if t.strip()}
    return frozenset(names) or DEFAULT_TABLES


def referenced_tables(body: str) -> set[str]:
    """Parse only (no execution) on a private in-memory connection with no file access."""
    con = duckdb.connect(":memory:", config={"enable_external_access": False})
    try:
        # qualified names come back as "schema.table AS alias"; drop the alias part
        return {re.split(r"\s+as\s+", t, maxsplit=1, flags=re.I)[0].strip().lower()
                for t in con.get_table_names(body, qualified=True)}
    except duckdb.Error as exc:
        raise QueryError(f"无法解析查询: {str(exc)[:200]}") from exc
    finally:
        con.close()


class QueryError(ValueError):
    pass


class QueryRequest(BaseModel):
    sql: str


class QueryResult(BaseModel):
    columns: list[str]
    rows: list[list[object]]
    truncated: bool


def guard(sql: str) -> str:
    body = sql.strip().rstrip(";").strip()
    if not body or ";" in body:
        raise QueryError("只允许一条语句")
    if not _START.match(body):
        raise QueryError("只允许 SELECT / WITH 查询")
    if _FORBIDDEN.search(body):
        raise QueryError("包含不允许的关键字或文件函数")
    allowed = allowed_tables()
    bad = sorted(t for t in referenced_tables(body) if t.removeprefix("main.") not in allowed)
    if bad:
        raise QueryError(f"表不在允许列表: {', '.join(bad)}")
    return f"SELECT * FROM ({body}) AS q LIMIT {MAX_ROWS + 1}"


def shape(rows: list[dict[str, object]]) -> QueryResult:
    cols = list(rows[0].keys()) if rows else []
    data = [[_cell(r[c]) for c in cols] for r in rows[:MAX_ROWS]]
    return QueryResult(columns=cols, rows=data, truncated=len(rows) > MAX_ROWS)


def _cell(v: object) -> object:
    return v if v is None or isinstance(v, int | float | str | bool) else str(v)
