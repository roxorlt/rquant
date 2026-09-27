"""Read bounded daily-bar coverage evidence from one fixed DuckDB replica connection."""

import math
from datetime import date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_audit_coverage import (
    CalendarDay,
    CalendarEvidence,
    DailyBarCount,
    DailyBarCountEvidence,
    DailyBarCoverageReport,
    DailyBarCoverageRequest,
    audit_daily_bar_coverage,
)
from rquant.data_audit_quality import (
    DailyBarQualityReport,
    DailyBarQualityRequest,
    DailyBarQualityRow,
    FieldNullCount,
    audit_daily_bar_quality,
)
from rquant.suspension import (
    SuspensionCoverage,
    SuspensionEvent,
    SuspensionSnapshot,
    _snapshot_hash,
)

MAX_AUDIT_DAYS = 3660
MAX_DAILY_QUALITY_ROWS = 10_000
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_DAILY_QUALITY_COLUMNS = (
    "ts_code",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
)
_DAILY_QUALITY_NUMERIC_COLUMNS = _DAILY_QUALITY_COLUMNS[1:]


class DailyBarNullFieldSpec(BaseModel):
    """An explicitly selected daily_bar NULL check, with an exact rational threshold."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    field_name: Literal[
        "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"
    ]
    max_null_numerator: int = Field(ge=0, strict=True)
    max_null_denominator: int = Field(ge=1, strict=True)

    @model_validator(mode="after")
    def validate_threshold(self) -> "DailyBarNullFieldSpec":
        if self.max_null_numerator > self.max_null_denominator:
            raise ValueError("null threshold numerator cannot exceed denominator")
        return self


def audit_daily_bar_coverage_from_connection(
    connection: duckdb.DuckDBPyConnection,
    *,
    snapshot_id: str,
    audit_start: date,
    completed_through: date,
) -> DailyBarCoverageReport:
    """Measure date presence through a caller-verified completed SSE session.

    The caller owns the already fixed replica connection, its generation ID, and
    completion cutoff. This function does not open a database or infer today's date.
    """
    if not snapshot_id or not snapshot_id.strip():
        raise ValueError("snapshot_id must be non-empty")
    if audit_start > completed_through:
        raise ValueError("audit range must end on or after audit_start")
    range_days = (completed_through - audit_start).days + 1
    if range_days > MAX_AUDIT_DAYS:
        raise ValueError(f"audit range exceeds {MAX_AUDIT_DAYS}-day limit")

    try:
        mode = connection.execute("SELECT current_setting('access_mode')").fetchone()
    except duckdb.Error as exc:
        raise ValueError("cannot verify read-only DuckDB connection") from exc
    if mode is None or str(mode[0]).lower() != "read_only":
        raise ValueError("coverage evidence requires a read-only DuckDB connection")

    try:
        calendar_rows = connection.execute(
            """
            SELECT exchange, cal_date, is_open
            FROM trade_calendar
            WHERE exchange = 'SSE' AND cal_date BETWEEN ? AND ?
            ORDER BY cal_date
            LIMIT ?
            """,
            [audit_start, completed_through, MAX_AUDIT_DAYS + 1],
        ).fetchall()
    except duckdb.Error as exc:
        raise ValueError("SSE calendar evidence unavailable") from exc
    if len(calendar_rows) > range_days:
        raise ValueError("SSE calendar evidence exceeds audit range row limit")
    if any(row[0] != "SSE" for row in calendar_rows):
        raise ValueError("SSE calendar evidence contains another exchange")
    calendar = CalendarEvidence(
        snapshot_id=snapshot_id,
        exchange="SSE",
        days=tuple(CalendarDay(day=row[1], is_open=row[2]) for row in calendar_rows),
    )
    if len(calendar.days) != range_days or any(
        item.day != audit_start + timedelta(days=index) for index, item in enumerate(calendar.days)
    ):
        raise ValueError("SSE calendar must contain each date in the audit range exactly once")
    if not calendar.days[-1].is_open:
        raise ValueError("completed_through must be an open SSE trading day")

    try:
        count_rows = connection.execute(
            """
            SELECT trade_date, COUNT(*) AS row_count
            FROM daily_bar
            WHERE trade_date BETWEEN ? AND ?
            GROUP BY trade_date
            ORDER BY trade_date
            LIMIT ?
            """,
            [audit_start, completed_through, MAX_AUDIT_DAYS + 1],
        ).fetchall()
    except duckdb.Error as exc:
        raise ValueError("daily_bar evidence unavailable") from exc
    if len(count_rows) > range_days:
        raise ValueError("daily_bar evidence exceeds audit range row limit")

    request = DailyBarCoverageRequest(
        audit_start=audit_start,
        completed_through=completed_through,
        calendar=calendar,
        daily_bar_counts=DailyBarCountEvidence(
            snapshot_id=snapshot_id,
            days=tuple(DailyBarCount(day=row[0], row_count=row[1]) for row in count_rows),
        ),
    )
    return audit_daily_bar_coverage(request)


def _complete_suspension_snapshot(
    connection: duckdb.DuckDBPyConnection,
    trade_date: date,
) -> SuspensionSnapshot | None:
    """Only a matching complete Tushare query snapshot proves event absence."""

    try:
        coverage_rows = connection.execute(
            "SELECT source, trade_date, coverage_state, row_count, snapshot_hash, queried_at "
            "FROM stock_suspend_coverage WHERE source = 'tushare' AND trade_date = ? LIMIT 2",
            (trade_date,),
        ).fetchall()
        if len(coverage_rows) != 1:
            return None
        event_rows = connection.execute(
            "SELECT source, ts_code, trade_date, suspend_type, suspend_timing, "
            "session_scope, available_at, ingested_at FROM stock_suspend_event "
            "WHERE source = 'tushare' AND trade_date = ? "
            "ORDER BY ts_code, suspend_type, suspend_timing LIMIT ?",
            (trade_date, MAX_DAILY_QUALITY_ROWS + 1),
        ).fetchall()
    except duckdb.Error:
        return None
    if len(event_rows) > MAX_DAILY_QUALITY_ROWS:
        return None
    try:
        snapshot = SuspensionSnapshot(
            coverage=SuspensionCoverage(
                **dict(
                    zip(
                        (
                            "source",
                            "trade_date",
                            "coverage_state",
                            "row_count",
                            "snapshot_hash",
                            "queried_at",
                        ),
                        coverage_rows[0],
                        strict=True,
                    )
                )
            ),
            events=tuple(
                SuspensionEvent(
                    **dict(
                        zip(
                            (
                                "source",
                                "ts_code",
                                "trade_date",
                                "suspend_type",
                                "suspend_timing",
                                "session_scope",
                                "available_at",
                                "ingested_at",
                            ),
                            row,
                            strict=True,
                        )
                    )
                )
                for row in event_rows
            ),
        )
    except ValueError:
        return None
    if snapshot.coverage.coverage_state != "complete":
        return None
    market_close = datetime.combine(trade_date, time(15), tzinfo=_SHANGHAI)
    if snapshot.coverage.queried_at < market_close:
        return None
    event_keys = {
        (event.source, event.ts_code, event.trade_date, event.suspend_type, event.suspend_timing)
        for event in snapshot.events
    }
    if len(event_keys) != len(snapshot.events):
        return None
    if snapshot.coverage.snapshot_hash != _snapshot_hash(snapshot.events):
        return None
    if any(
        event.available_at > snapshot.coverage.queried_at
        or event.ingested_at > snapshot.coverage.queried_at
        for event in snapshot.events
    ):
        return None
    return snapshot


def audit_daily_bar_quality_from_connection(
    connection: duckdb.DuckDBPyConnection,
    *,
    snapshot_id: str,
    completed_trade_date: date,
    null_fields: tuple[DailyBarNullFieldSpec, ...],
) -> DailyBarQualityReport:
    """Audit one caller-completed SSE day from one fixed read-only replica generation.

    Completion and snapshot identity remain the caller's responsibility. The local
    `daily_state` price limits are derived estimates, not authoritative limits.
    """

    if not snapshot_id or not snapshot_id.strip():
        raise ValueError("snapshot_id must be non-empty")
    if not null_fields or len(null_fields) > len(_DAILY_QUALITY_NUMERIC_COLUMNS):
        raise ValueError("null_fields must name at least one supported daily_bar field")
    field_names = [field.field_name for field in null_fields]
    if len(field_names) != len(set(field_names)):
        raise ValueError("null_fields must have unique field names")
    try:
        mode = connection.execute("SELECT current_setting('access_mode')").fetchone()
    except duckdb.Error as exc:
        raise ValueError("cannot verify read-only DuckDB connection") from exc
    if mode is None or str(mode[0]).lower() != "read_only":
        raise ValueError("quality evidence requires a read-only DuckDB connection")

    try:
        calendar_rows = connection.execute(
            "SELECT is_open FROM trade_calendar WHERE exchange = 'SSE' AND cal_date = ? LIMIT 2",
            (completed_trade_date,),
        ).fetchall()
    except duckdb.Error as exc:
        raise ValueError("SSE calendar evidence unavailable") from exc
    if len(calendar_rows) != 1 or calendar_rows[0][0] is not True:
        raise ValueError("completed_trade_date must be one open SSE trading day")

    try:
        bar_rows = connection.execute(
            "SELECT " + ", ".join(_DAILY_QUALITY_COLUMNS) + " FROM daily_bar "
            "WHERE trade_date = ? ORDER BY ts_code LIMIT ?",
            (completed_trade_date, MAX_DAILY_QUALITY_ROWS + 1),
        ).fetchall()
    except duckdb.Error as exc:
        raise ValueError("daily_bar quality evidence unavailable") from exc
    if len(bar_rows) > MAX_DAILY_QUALITY_ROWS:
        raise ValueError("daily_bar quality evidence exceeds 10,000-row limit")
    codes = [row[0] for row in bar_rows]
    if any(not isinstance(code, str) or not code.strip() for code in codes):
        raise ValueError("daily_bar contains an invalid ts_code")
    if len(codes) != len(set(codes)):
        raise ValueError("daily_bar ts_code values must be unique for a day")

    suspension = _complete_suspension_snapshot(connection, completed_trade_date)
    event_codes = {event.ts_code for event in suspension.events} if suspension else set()
    quality_rows: list[DailyBarQualityRow] = []
    for row in bar_rows:
        code = row[0]
        suspension_authoritative = suspension is not None and code not in event_codes
        try:
            quality_rows.append(
                DailyBarQualityRow(
                    ts_code=code,
                    close=row[_DAILY_QUALITY_COLUMNS.index("close")],
                    volume=row[_DAILY_QUALITY_COLUMNS.index("vol")],
                    limit_up=None,
                    limit_down=None,
                    limits_authoritative=False,
                    is_suspended=False if suspension_authoritative else None,
                    suspension_authoritative=suspension_authoritative,
                )
            )
        except ValueError as exc:
            raise ValueError("daily_bar contains invalid or non-finite price or volume") from exc

    fields: list[FieldNullCount] = []
    for field in null_fields:
        column = _DAILY_QUALITY_COLUMNS.index(field.field_name)
        null_rows = 0
        for bar in bar_rows:
            value = bar[column]
            if value is None:
                null_rows += 1
            elif (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(f"daily_bar.{field.field_name} contains a non-finite value")
        fields.append(
            FieldNullCount(
                field_name=field.field_name,
                observed_rows=len(bar_rows),
                null_rows=null_rows,
                max_null_numerator=field.max_null_numerator,
                max_null_denominator=field.max_null_denominator,
            )
        )
    return audit_daily_bar_quality(
        DailyBarQualityRequest(
            snapshot_id=snapshot_id,
            dataset_id="daily_bar",
            trade_date=completed_trade_date,
            rows=tuple(quality_rows),
            fields=tuple(fields),
        )
    )
