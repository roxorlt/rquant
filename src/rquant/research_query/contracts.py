"""Typed SQL/result contracts; no settings or database access at import."""

from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Annotated, Literal

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
MAX_SAFE_INTEGER = 2**53 - 1
_DOLLAR_QUOTE = re.compile(r"\$(?:[^\W\d]\w*)?\$")


def _sql_tokens(sql: str) -> str:
    """Mask DuckDB quoted text and comments, keeping actual SQL tokens visible."""
    tokens: list[str] = []
    index = 0
    while index < len(sql):
        if sql.startswith("--", index):
            while index < len(sql) and sql[index] not in "\r\n":
                index += 1
            tokens.append(" ")
        elif sql.startswith("/*", index):
            depth = 1
            index += 2
            while index < len(sql) and depth:
                if sql.startswith("/*", index):
                    depth += 1
                    index += 2
                elif sql.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise ValueError("SQL 注释未结束")
            tokens.append(" ")
        else:
            boundary = index == 0 or not (sql[index - 1].isalnum() or sql[index - 1] in "_$")
            escaped = boundary and sql[index] in "eE" and sql[index : index + 2].endswith("'")
            delimiter = _DOLLAR_QUOTE.match(sql, index) if boundary else None
            if delimiter is not None:
                end = sql.find(delimiter[0], delimiter.end())
                if end < 0:
                    raise ValueError("SQL 字符串未结束")
                index = end + len(delimiter[0])
                tokens.append(" ")
            elif sql[index] in "'\"" or escaped:
                if escaped:
                    index += 1
                quote = sql[index]
                index += 1
                while index < len(sql):
                    if escaped and sql[index] == "\\":
                        index += 2
                    elif sql[index] == quote:
                        index += 1
                        if index < len(sql) and sql[index] == quote:
                            index += 1
                        else:
                            break
                    else:
                        index += 1
                else:
                    raise ValueError("SQL 引号未结束")
                tokens.append(" ")
            else:
                tokens.append(sql[index])
                index += 1
    return "".join(tokens).strip()


def validate_sql(sql: str) -> str:
    if not sql.strip() or len(sql.encode("utf-8")) > MAX_SQL_BYTES or "\x00" in sql:
        raise ValueError("SQL 内容为空或超过 32 KiB")
    # The existing SELECT guard receives lexical tokens, not quoted user text.
    # DuckDB's actual parser/type check still runs inside the restricted child.
    tokens = _sql_tokens(sql)
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
    kind: Literal["integer", "decimal", "date", "binary", "nonfinite", "interval", "structured"]
    text: str


QueryValue = (
    str
    | Annotated[StrictInt, Field(ge=-MAX_SAFE_INTEGER, le=MAX_SAFE_INTEGER)]
    | StrictFloat
    | StrictBool
    | None
    | QuerySpecialValue
)


class QueryResult(QueryModel):
    status: Literal["ready", "partial", "failed", "timeout", "unavailable", "busy"]
    columns: tuple[QueryColumn, ...] = ()
    rows: tuple[tuple[QueryValue, ...], ...] = Field(default=(), max_length=MAX_ROWS)
    elapsed_ms: float = Field(ge=0)
    source_at: datetime | None = None
    snapshot_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    message: str = ""

    @field_validator("rows", mode="before")
    @classmethod
    def safe_integer_cells(cls, rows: object) -> object:
        # StrictFloat can accept Python ints; reject lossy fallback before union validation.
        if isinstance(rows, (list, tuple)):
            for row in rows:
                if isinstance(row, (list, tuple)) and any(
                    isinstance(value, int)
                    and not isinstance(value, bool)
                    and abs(value) > MAX_SAFE_INTEGER
                    for value in row
                ):
                    raise ValueError("超出精确范围的整数必须使用有类型文本")
        return rows


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
