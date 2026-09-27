"""Read bounded daily-bar coverage evidence from one fixed DuckDB replica connection."""

from datetime import date, timedelta

import duckdb

from rquant.data_audit_coverage import (
    CalendarDay,
    CalendarEvidence,
    DailyBarCount,
    DailyBarCountEvidence,
    DailyBarCoverageReport,
    DailyBarCoverageRequest,
    audit_daily_bar_coverage,
)

MAX_AUDIT_DAYS = 3660


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
