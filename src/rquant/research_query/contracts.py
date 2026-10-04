"""Typed SQL/result contracts; no settings or database access at import."""

from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    field_validator,
)

MAX_SQL_BYTES = 32 * 1024
MAX_RESULT_BYTES = 16 * 1024 * 1024
MAX_ROWS = 10_000
MAX_SECONDS = 30.0


def validate_sql(sql: str) -> str:
    if not sql.strip() or len(sql.encode("utf-8")) > MAX_SQL_BYTES or "\x00" in sql:
        raise ValueError("SQL 内容为空或超过 32 KiB")
    # The existing SELECT guard receives lexical tokens, not quoted user text.
    # DuckDB's actual parser/type check still runs inside the restricted child.
    tokens = re.sub(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\n]*|/\*[\s\S]*?\*/", " quoted ", sql)
    tokens = re.sub(r"\bquoted\b", "", tokens).strip()
    from rquant.dashboard.serving_only_page_data import _validate_bounded_select

    try:
        _validate_bounded_select(tokens, ())
    except Exception as exc:
        raise ValueError("只接受一条 SELECT 或 WITH 查询") from exc
    return sql


class QueryModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class QueryRequest(QueryModel):
    sql: str
    mode: Literal["query", "explain"] = "query"

    _sql = field_validator("sql")(validate_sql)


class QueryColumn(QueryModel):
    name: str
    data_type: str


class QuerySpecialValue(QueryModel):
    kind: Literal["decimal", "date", "binary", "nonfinite", "interval", "structured"]
    text: str


QueryValue = str | StrictInt | StrictFloat | StrictBool | None | QuerySpecialValue


class QueryResult(QueryModel):
    status: Literal["ready", "partial", "failed", "timeout", "unavailable", "busy"]
    columns: tuple[QueryColumn, ...] = ()
    rows: tuple[tuple[QueryValue, ...], ...] = Field(default=(), max_length=MAX_ROWS)
    elapsed_ms: float = Field(ge=0)
    source_at: datetime | None = None
    snapshot_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    message: str = ""


def cell_text(value: QueryValue) -> str:
    if value is None:
        return ""
    if isinstance(value, QuerySpecialValue):
        return value.text
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def csv_cell(value: QueryValue) -> str:
    text = cell_text(value)
    # Excel also treats formula prefixes after whitespace as executable input.
    return "'" + text if text.lstrip(" \t\r\n").startswith(("=", "+", "-", "@")) else text


def result_csv(result: QueryResult) -> str:
    if result.status not in {"ready", "partial"}:
        raise ValueError("尚无可导出的结果")
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow([csv_cell(column.name) for column in result.columns])
    writer.writerows([[csv_cell(value) for value in row] for row in result.rows])
    return output.getvalue()
