"""A retrospective SSE calendar from frozen facts, without a historical visibility claim."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Literal, Self

import duckdb
from pydantic import model_validator

from rquant.backtest.contracts import SSECalendar
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.trade_calendar import (
    TradeCalendarDay,
    TradeCalendarGapError,
    validate_authoritative_trade_calendar_rows,
)

MAX_CALENDAR_DAYS = 3660
_SOURCE_MODE = "retrospective_trade_calendar"
_SOURCE_TABLE = "trade_calendar"
_EXCHANGE = "SSE"
_COLUMNS = {
    "exchange": ("VARCHAR", True),
    "cal_date": ("DATE", True),
    "is_open": ("BOOLEAN", True),
    "pretrade_date": ("DATE", False),
    "source": ("VARCHAR", True),
    "updated_at": ("TIMESTAMP WITH TIME ZONE", True),
}


class CalendarSourceError(ValueError):
    """The frozen trade calendar cannot support an exact SSE backtest range."""


def _identity(rows: tuple[TradeCalendarDay, ...]) -> str:
    return canonical_sha256(
        {"source_mode": _SOURCE_MODE, "source_table": _SOURCE_TABLE, "rows": rows}
    )


class RetrospectiveSSECalendarSnapshot(RuntimeContractModel):
    """Verified historical calendar facts; no proof of their first visible time."""

    source_mode: Literal["retrospective_trade_calendar"] = _SOURCE_MODE
    source_table: Literal["trade_calendar"] = _SOURCE_TABLE
    requested_start: date
    requested_end: date
    rows: tuple[TradeCalendarDay, ...]
    calendar: SSECalendar

    @property
    def source_identity(self) -> str:
        return self.calendar.source_identity

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        if not self.rows or self.requested_start > self.requested_end:
            raise ValueError("retrospective calendar range or rows are invalid")
        row_dates = tuple(row.cal_date for row in self.rows)
        if row_dates != tuple(sorted(set(row_dates))):
            raise ValueError("retrospective calendar rows must be ordered and unique")
        if len(row_dates) > MAX_CALENDAR_DAYS:
            raise ValueError(f"retrospective calendar exceeds {MAX_CALENDAR_DAYS} civil days")
        checked = validate_authoritative_trade_calendar_rows(
            self.rows, exchange=_EXCHANGE, start=row_dates[0], end=row_dates[-1]
        )
        if tuple(checked) != self.rows:
            raise ValueError("retrospective calendar rows differ from validated facts")
        open_dates = tuple(row.cal_date for row in self.rows if row.is_open)
        if not open_dates or open_dates[0] >= self.requested_start:
            raise ValueError("retrospective calendar needs a preceding SSE open day")
        if open_dates[-1] <= self.requested_end:
            raise ValueError("retrospective calendar needs a following SSE open day")
        if not any(self.requested_start <= day <= self.requested_end for day in open_dates):
            raise ValueError("retrospective calendar range contains no SSE open day")
        if self.rows[0].cal_date != open_dates[0] or self.rows[-1].cal_date != open_dates[-1]:
            raise ValueError("retrospective calendar coverage must end at open anchors")
        if self.calendar.dates != open_dates or self.calendar.source_identity != _identity(
            self.rows
        ):
            raise ValueError("SSE calendar disagrees with verified retrospective facts")
        return self


def _check_range(start: date, end: date) -> None:
    if type(start) is not date or type(end) is not date:
        raise CalendarSourceError("calendar start and end must be civil dates")
    if start > end:
        raise CalendarSourceError("calendar start must not be after end")
    if (end - start).days + 1 > MAX_CALENDAR_DAYS:
        raise CalendarSourceError(f"calendar range exceeds {MAX_CALENDAR_DAYS} civil days")


def _check_schema(connection: duckdb.DuckDBPyConnection) -> None:
    try:
        columns = connection.execute("PRAGMA table_info('trade_calendar')").fetchall()
    except duckdb.CatalogException as exc:
        raise CalendarSourceError("frozen trade_calendar schema is missing") from exc
    actual = {name: (data_type, bool(not_null)) for _, name, data_type, not_null, *_ in columns}
    if any(actual.get(name) != required for name, required in _COLUMNS.items()):
        raise CalendarSourceError("frozen trade_calendar schema is missing or incompatible")


def _read_rows(
    connection: duckdb.DuckDBPyConnection, start: date, end: date
) -> tuple[TradeCalendarDay, ...]:
    _check_schema(connection)
    preceding, following = connection.execute(
        "SELECT MAX(cal_date) FILTER (WHERE cal_date < ?), "
        "MIN(cal_date) FILTER (WHERE cal_date > ?) "
        "FROM trade_calendar WHERE exchange = ? AND is_open = TRUE",
        [start, end, _EXCHANGE],
    ).fetchone()
    if preceding is None:
        raise CalendarSourceError("no preceding SSE open day in frozen trade_calendar")
    if following is None:
        raise CalendarSourceError("no following SSE open day in frozen trade_calendar")
    if (following - preceding).days + 1 > MAX_CALENDAR_DAYS:
        raise CalendarSourceError(f"verified calendar exceeds {MAX_CALENDAR_DAYS} civil days")
    raw = connection.execute(
        "SELECT exchange, cal_date, is_open, pretrade_date, source, updated_at "
        "FROM trade_calendar WHERE exchange = ? AND cal_date BETWEEN ? AND ? "
        "ORDER BY cal_date",
        [_EXCHANGE, preceding, following],
    ).fetchall()
    seen: set[date] = set()
    rows: list[TradeCalendarDay] = []
    for exchange, cal_date, is_open, pretrade_date, source, updated_at in raw:
        if cal_date in seen:
            raise CalendarSourceError(f"duplicate SSE trade calendar date: {cal_date}")
        seen.add(cal_date)
        try:
            rows.append(
                TradeCalendarDay(
                    exchange=exchange,
                    cal_date=cal_date,
                    is_open=is_open,
                    pretrade_date=pretrade_date,
                    source=source,
                    updated_at=updated_at,
                )
            )
        except ValueError as exc:
            raise CalendarSourceError(f"invalid SSE trade calendar row {cal_date}: {exc}") from exc
    return tuple(rows)


def verify_retrospective_sse_calendar(
    connection: duckdb.DuckDBPyConnection, *, start: date, end: date
) -> RetrospectiveSSECalendarSnapshot:
    """Verify calendar rows within the caller's existing read-only transaction."""
    _check_range(start, end)
    try:
        rows = _read_rows(connection, start, end)
        open_dates = tuple(row.cal_date for row in rows if row.is_open)
        if not any(start <= day <= end for day in open_dates):
            raise CalendarSourceError("retrospective calendar range contains no SSE open day")
        calendar = SSECalendar(
            source_identity=_identity(rows),
            coverage_start=open_dates[0],
            coverage_end=open_dates[-1],
            dates=open_dates,
        )
        return RetrospectiveSSECalendarSnapshot(
            requested_start=start,
            requested_end=end,
            rows=rows,
            calendar=calendar,
        )
    except duckdb.Error as exc:
        raise CalendarSourceError("frozen trade_calendar is unavailable") from exc
    except (TradeCalendarGapError, ValueError) as exc:
        if isinstance(exc, CalendarSourceError):
            raise
        raise CalendarSourceError(str(exc)) from exc


def load_retrospective_sse_calendar(
    frozen_path: Path, *, start: date, end: date
) -> RetrospectiveSSECalendarSnapshot:
    """Load all civil rows around a requested range from one read-only DuckDB snapshot."""
    _check_range(start, end)
    if not frozen_path.is_file():
        raise CalendarSourceError("an existing frozen DuckDB file is required")
    try:
        connection = duckdb.connect(str(frozen_path), read_only=True)
    except duckdb.Error as exc:
        raise CalendarSourceError("frozen DuckDB cannot be opened read-only") from exc
    try:
        connection.execute("BEGIN TRANSACTION")
        try:
            snapshot = verify_retrospective_sse_calendar(connection, start=start, end=end)
        except Exception:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")
        return snapshot
    except duckdb.Error as exc:
        raise CalendarSourceError("frozen trade_calendar is unavailable") from exc
    finally:
        connection.close()
