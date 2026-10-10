"""Module 16: read-only SQL over the Serving replica (admin only, SELECT only, capped)."""

from __future__ import annotations

import re

from pydantic import BaseModel

MAX_ROWS = 1000
_START = re.compile(r"^\s*(select|with)\b", re.I)
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|create|drop|alter|attach|detach|copy|export|import|install|load|"
    r"pragma|set|call|checkpoint|vacuum)\b|read_(csv|parquet|json)|glob\(", re.I)


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
    return f"SELECT * FROM ({body}) AS q LIMIT {MAX_ROWS + 1}"


def shape(rows: list[dict[str, object]]) -> QueryResult:
    cols = list(rows[0].keys()) if rows else []
    data = [[_cell(r[c]) for c in cols] for r in rows[:MAX_ROWS]]
    return QueryResult(columns=cols, rows=data, truncated=len(rows) > MAX_ROWS)


def _cell(v: object) -> object:
    return v if v is None or isinstance(v, int | float | str | bool) else str(v)
