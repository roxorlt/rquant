"""Hand-computable evidence for the daily-bar coverage core."""

from datetime import date, timedelta
from decimal import Decimal

import pytest

from rquant.data_audit_coverage import (
    CalendarDay,
    CalendarEvidence,
    DailyBarCount,
    DailyBarCountEvidence,
    DailyBarCoverageRequest,
    audit_daily_bar_coverage,
)


def _days(start: date, end: date, open_days: set[date]) -> tuple[CalendarDay, ...]:
    return tuple(
        CalendarDay(
            day=start + timedelta(days=offset), is_open=start + timedelta(days=offset) in open_days
        )
        for offset in range((end - start).days + 1)
    )


def _request(
    start: date,
    end: date,
    open_days: set[date],
    rows: tuple[DailyBarCount, ...] = (),
    *,
    calendar_snapshot: str = "snapshot-1",
    count_snapshot: str = "snapshot-1",
) -> DailyBarCoverageRequest:
    return DailyBarCoverageRequest(
        audit_start=start,
        completed_through=end,
        calendar=CalendarEvidence(
            snapshot_id=calendar_snapshot, exchange="SSE", days=_days(start, end, open_days)
        ),
        daily_bar_counts=DailyBarCountEvidence(snapshot_id=count_snapshot, days=rows),
    )


def test_monthly_coverage_cross_month_trading_gap_and_closed_day_rows() -> None:
    start, end = date(2026, 1, 29), date(2026, 2, 5)
    open_days = {
        date(2026, 1, 29),
        date(2026, 1, 30),
        date(2026, 2, 2),
        date(2026, 2, 3),
        date(2026, 2, 4),
        date(2026, 2, 5),
    }
    request = _request(
        start,
        end,
        open_days,
        (
            DailyBarCount(day=date(2026, 1, 29), row_count=4000),
            DailyBarCount(day=date(2026, 1, 31), row_count=7),
            DailyBarCount(day=date(2026, 2, 3), row_count=0),
            DailyBarCount(day=date(2026, 2, 5), row_count=4100),
        ),
    )

    report = audit_daily_bar_coverage(request)

    assert report.snapshot_id == "snapshot-1"
    assert report.exchange == "SSE"
    assert report.audit_start == start
    assert report.completed_through == end
    assert [
        (
            item.month,
            item.expected_open_days,
            item.covered_open_days,
            item.coverage_ratio,
            item.status,
        )
        for item in report.monthly
    ] == [
        (date(2026, 1, 1), 2, 1, Decimal("0.5000"), "measured"),
        (date(2026, 2, 1), 4, 1, Decimal("0.2500"), "measured"),
    ]
    assert [(gap.start, gap.end, gap.missing_open_days) for gap in report.gaps] == [
        (date(2026, 1, 30), date(2026, 2, 4), 4)
    ]
    assert [(item.day, item.row_count) for item in report.closed_day_rows] == [
        (date(2026, 1, 31), 7)
    ]


def test_leading_and_trailing_gaps_are_separate() -> None:
    start, end = date(2026, 3, 2), date(2026, 3, 9)
    open_days = {
        date(2026, 3, 2),
        date(2026, 3, 3),
        date(2026, 3, 4),
        date(2026, 3, 5),
        date(2026, 3, 6),
        date(2026, 3, 9),
    }
    report = audit_daily_bar_coverage(
        _request(
            start,
            end,
            open_days,
            (DailyBarCount(day=date(2026, 3, 4), row_count=1),),
        )
    )

    assert [(gap.start, gap.end, gap.missing_open_days) for gap in report.gaps] == [
        (date(2026, 3, 2), date(2026, 3, 3), 2),
        (date(2026, 3, 5), date(2026, 3, 9), 3),
    ]
    assert report.monthly[0].coverage_ratio == Decimal("0.1667")


def test_month_with_no_expected_session_is_not_reported_as_fully_covered() -> None:
    report = audit_daily_bar_coverage(
        _request(
            date(2026, 1, 31),
            date(2026, 2, 2),
            {date(2026, 2, 2)},
            (DailyBarCount(day=date(2026, 2, 2), row_count=1),),
        )
    )

    assert [
        (
            item.month,
            item.expected_open_days,
            item.covered_open_days,
            item.coverage_ratio,
            item.status,
        )
        for item in report.monthly
    ] == [
        (date(2026, 1, 1), 0, 0, None, "no_expected_sessions"),
        (date(2026, 2, 1), 1, 1, Decimal("1.0000"), "measured"),
    ]
    assert report.gaps == ()


@pytest.mark.parametrize("invalid", ["missing", "duplicate", "unsorted", "extra"])
def test_rejects_calendar_without_exact_sorted_daily_coverage(invalid: str) -> None:
    start, end = date(2026, 2, 2), date(2026, 2, 4)
    valid = _days(start, end, {start, end})
    variants = {
        "missing": (valid[0], valid[2]),
        "duplicate": (valid[0], valid[1], valid[1], valid[2]),
        "unsorted": (valid[1], valid[0], valid[2]),
        "extra": (*valid, CalendarDay(day=date(2026, 2, 5), is_open=True)),
    }

    with pytest.raises(ValueError, match="calendar"):
        DailyBarCoverageRequest(
            audit_start=start,
            completed_through=end,
            calendar=CalendarEvidence(
                snapshot_id="snapshot-1", exchange="SSE", days=variants[invalid]
            ),
            daily_bar_counts=DailyBarCountEvidence(snapshot_id="snapshot-1", days=()),
        )


@pytest.mark.parametrize("invalid", ["duplicate", "unsorted", "outside", "negative"])
def test_rejects_invalid_daily_counts(invalid: str) -> None:
    start, end = date(2026, 2, 2), date(2026, 2, 4)
    first = DailyBarCount(day=start, row_count=1)
    last = DailyBarCount(day=end, row_count=1)
    variants = {
        "duplicate": (first, first),
        "unsorted": (last, first),
        "outside": (DailyBarCount(day=date(2026, 2, 1), row_count=1),),
    }

    with pytest.raises(ValueError, match="row_count|daily_bar_counts"):
        _request(
            start,
            end,
            {start, end},
            variants.get(invalid, ())
            if invalid != "negative"
            else (DailyBarCount(day=start, row_count=-1),),
        )


def test_rejects_mismatched_snapshot_and_closed_cutoff() -> None:
    start, end = date(2026, 2, 1), date(2026, 2, 2)
    with pytest.raises(ValueError, match="snapshot"):
        _request(start, end, {end}, calendar_snapshot="snapshot-1", count_snapshot="snapshot-2")
    with pytest.raises(ValueError, match="completed_through"):
        _request(start, end, set())


def test_calendar_requires_explicit_sse_exchange() -> None:
    with pytest.raises(ValueError, match="exchange"):
        CalendarEvidence(
            snapshot_id="snapshot-1", days=(CalendarDay(day=date(2026, 2, 2), is_open=True),)
        )


def test_rejects_backwards_interval() -> None:
    with pytest.raises(ValueError, match="audit_start|completed_through"):
        _request(date(2026, 2, 3), date(2026, 2, 2), set())
