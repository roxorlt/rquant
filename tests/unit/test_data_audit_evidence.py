"""Coverage evidence is read from one bounded, read-only DuckDB snapshot."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from rquant.data_audit_coverage import DailyBarCoverageReport
from rquant.storage.schema import DAILY_BAR_DDL, TRADE_CALENDAR_DDL


def _database(
    path: Path,
    calendar: list[tuple[str, date, bool]],
    bars: list[tuple[str, date]],
    *,
    allow_duplicate_calendar: bool = False,
) -> Path:
    with duckdb.connect(str(path)) as connection:
        connection.execute(TRADE_CALENDAR_DDL)
        connection.execute(DAILY_BAR_DDL)
        if allow_duplicate_calendar:
            connection.execute("CREATE TABLE calendar_without_key AS SELECT * FROM trade_calendar")
            connection.execute("DROP TABLE trade_calendar")
            connection.execute("ALTER TABLE calendar_without_key RENAME TO trade_calendar")
        connection.executemany(
            """
            INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at)
            VALUES (?, ?, ?, 'synthetic', ?)
            """,
            [(*row, datetime(2026, 1, 1, tzinfo=UTC)) for row in calendar],
        )
        if bars:
            connection.executemany(
                "INSERT INTO daily_bar (ts_code, trade_date) VALUES (?, ?)", bars
            )
    return path


def _audit(
    path: Path,
    *,
    start: date = date(2026, 1, 29),
    completed: date = date(2026, 2, 5),
    snapshot_id: str = "replica-generation-1",
) -> DailyBarCoverageReport:
    from rquant.data_audit_evidence import audit_daily_bar_coverage_from_connection

    with duckdb.connect(str(path), read_only=True) as connection:
        return audit_daily_bar_coverage_from_connection(
            connection,
            snapshot_id=snapshot_id,
            audit_start=start,
            completed_through=completed,
        )


def _calendar(start: date, end: date, *, exchange: str = "SSE") -> list[tuple[str, date, bool]]:
    return [
        (exchange, day, day.weekday() < 5)
        for offset in range((end - start).days + 1)
        for day in [start + timedelta(days=offset)]
    ]


def test_sql_evidence_reports_cross_month_gap_and_closed_day_rows(tmp_path: Path) -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 5)
    path = _database(
        tmp_path / "coverage.duckdb",
        _calendar(start, end),
        [
            ("600000.SH", date(2026, 1, 29)),
            ("000001.SZ", date(2026, 1, 29)),
            ("600000.SH", date(2026, 1, 31)),
            ("600000.SH", date(2026, 2, 5)),
        ],
    )

    report = _audit(path)

    assert report.snapshot_id == "replica-generation-1"
    assert report.exchange == "SSE"
    assert report.completed_through == end
    assert [
        (item.month, item.expected_open_days, item.covered_open_days, item.coverage_ratio)
        for item in report.monthly
    ] == [
        (date(2026, 1, 1), 2, 1, Decimal("0.5000")),
        (date(2026, 2, 1), 4, 1, Decimal("0.2500")),
    ]
    assert [(gap.start, gap.end, gap.missing_open_days) for gap in report.gaps] == [
        (date(2026, 1, 30), date(2026, 2, 4), 4)
    ]
    assert [(row.day, row.row_count) for row in report.closed_day_rows] == [(date(2026, 1, 31), 1)]


@pytest.mark.parametrize("problem", ["missing", "duplicate", "other_exchange"])
def test_refuses_untrustworthy_sse_calendar(tmp_path: Path, problem: str) -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 5)
    calendar = _calendar(start, end)
    if problem == "missing":
        calendar = [row for row in calendar if row[1] != date(2026, 2, 1)]
    elif problem == "duplicate":
        calendar.append(calendar[0])
    else:
        calendar = _calendar(start, end, exchange="SZSE")
    path = _database(
        tmp_path / "untrustworthy.duckdb",
        calendar,
        [],
        allow_duplicate_calendar=problem == "duplicate",
    )

    with pytest.raises(ValueError, match="calendar"):
        _audit(path)


def test_other_exchange_rows_do_not_poison_complete_sse_calendar(tmp_path: Path) -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 5)
    path = _database(
        tmp_path / "two-exchanges.duckdb",
        _calendar(start, end) + _calendar(start, end, exchange="SZSE"),
        [("600000.SH", end)],
    )

    report = _audit(path)

    assert report.exchange == "SSE"
    assert report.monthly[-1].covered_open_days == 1


def test_incomplete_calendar_fails_before_reading_daily_bars(tmp_path: Path) -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 5)
    calendar = [row for row in _calendar(start, end) if row[1] != date(2026, 2, 1)]
    path = _database(tmp_path / "incomplete.duckdb", calendar, [])
    with duckdb.connect(str(path)) as connection:
        connection.execute("DROP TABLE daily_bar")

    with pytest.raises(ValueError, match="calendar"):
        _audit(path)


def test_refuses_closed_cutoff_and_missing_table(tmp_path: Path) -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 5)
    path = _database(tmp_path / "closed.duckdb", _calendar(start, end), [])
    with pytest.raises(ValueError, match="completed_through"):
        _audit(path, completed=date(2026, 2, 1))

    with duckdb.connect(str(path)) as connection:
        connection.execute("DROP TABLE trade_calendar")
    with pytest.raises(ValueError, match="calendar"):
        _audit(path)


def test_refuses_writable_connection_blank_generation_and_over_budget_range(tmp_path: Path) -> None:
    from rquant.data_audit_evidence import audit_daily_bar_coverage_from_connection

    start, end = date(2026, 1, 29), date(2026, 2, 5)
    path = _database(tmp_path / "budget.duckdb", _calendar(start, end), [])
    with duckdb.connect(str(path)) as connection, pytest.raises(ValueError, match="read-only"):
        audit_daily_bar_coverage_from_connection(
            connection,
            snapshot_id="replica-generation-1",
            audit_start=start,
            completed_through=end,
        )
    with pytest.raises(ValueError, match="snapshot_id"):
        _audit(path, snapshot_id="  ")
    with pytest.raises(ValueError, match="range|limit"):
        _audit(path, start=date(2010, 1, 1))
