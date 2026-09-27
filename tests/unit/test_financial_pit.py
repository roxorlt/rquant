"""Financial point-in-time selection from typed facts and an explicit SSE calendar."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from rquant.financial_pit import (
    FinancialFact,
    FinancialPITSelection,
    SSECalendar,
    SSECalendarDay,
    select_financial_fact,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
ANNOUNCED = date(2026, 9, 24)
FIRST_SEEN = datetime(2026, 9, 24, 16, 0, tzinfo=SHANGHAI)
OPEN = frozenset({date(2026, 9, 24), date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30)})


def _calendar(
    *,
    start: date = date(2026, 9, 24),
    end: date = date(2026, 10, 2),
    open_dates: frozenset[date] = OPEN,
) -> SSECalendar:
    days = tuple(
        SSECalendarDay(day=start + timedelta(days=index), is_open=day in open_dates)
        for index in range((end - start).days + 1)
        for day in (start + timedelta(days=index),)
    )
    return SSECalendar(coverage_start=start, coverage_end=end, days=days)


def _fact(**changes: object) -> FinancialFact:
    fields: dict[str, object] = {
        "source_api": "income",
        "field": "revenue",
        "ts_code": "600000.SH",
        "report_period": date(2026, 6, 30),
        "report_type": "1",
        "ann_date": ANNOUNCED,
        "f_ann_date": None,
        "first_observed_at": FIRST_SEEN,
        "value": Decimal("100.00"),
        "update_flag": "0",
    }
    fields.update(changes)
    return FinancialFact(**fields)


def _select(
    facts: tuple[FinancialFact, ...],
    as_of: datetime,
    calendar: SSECalendar | None = None,
) -> FinancialPITSelection:
    return select_financial_fact(facts, as_of=as_of, calendar=calendar or _calendar())


def test_first_updated_supplier_version_cannot_rewrite_the_past() -> None:
    fact = _fact(
        update_flag="1",
        first_observed_at=datetime(2026, 10, 1, 16, 0, tzinfo=SHANGHAI),
    )

    historical = _select((fact,), datetime(2026, 9, 28, 10, 0, tzinfo=SHANGHAI))
    at_observation = _select((fact,), fact.first_observed_at)
    later = _select((fact,), fact.first_observed_at + timedelta(microseconds=1))

    assert historical.status == "unknown"
    assert historical.fact is None
    assert at_observation.status == "unknown"
    assert later.status == "selected"
    assert later.fact == fact


def test_missing_open_day_remains_unknown_with_explicit_coverage_boundary() -> None:
    complete = _calendar()
    missing = SSECalendar(
        coverage_start=complete.coverage_start,
        coverage_end=complete.coverage_end,
        days=tuple(day for day in complete.days if day.day != date(2026, 9, 28)),
    )

    decision = _select((_fact(),), datetime(2026, 9, 28, 10, 0, tzinfo=SHANGHAI), missing)

    assert decision.status == "unknown"
    assert decision.reason == "invalid_calendar"
    assert decision.fact is None


def test_weekend_and_holiday_require_next_open_0930_even_after_announcement() -> None:
    fact = _fact()
    assert _select((fact,), datetime(2026, 9, 24, 17, 0, tzinfo=SHANGHAI)).status == "unknown"
    assert _select((fact,), datetime(2026, 9, 27, 12, 0, tzinfo=SHANGHAI)).status == "unknown"
    assert _select((fact,), datetime(2026, 9, 28, 9, 29, 59, tzinfo=SHANGHAI)).status == "unknown"

    decision = _select((fact,), datetime(2026, 9, 28, 9, 30, tzinfo=SHANGHAI))

    assert decision.status == "selected"
    assert decision.fact == fact
    assert decision.next_open_at == datetime(2026, 9, 28, 9, 30, tzinfo=SHANGHAI)
    assert len(decision.content_sha256) == 64


def test_later_actual_announcement_date_controls_next_open_session() -> None:
    fact = _fact(f_ann_date=date(2026, 9, 29))

    assert _select((fact,), datetime(2026, 9, 29, 10, 0, tzinfo=SHANGHAI)).status == "unknown"
    decision = _select((fact,), datetime(2026, 9, 30, 9, 30, tzinfo=SHANGHAI))

    assert decision.fact == fact
    assert decision.next_open_at == datetime(2026, 9, 30, 9, 30, tzinfo=SHANGHAI)


@pytest.mark.parametrize(
    "calendar",
    [
        SSECalendar(coverage_start=date(2026, 9, 24), coverage_end=date(2026, 10, 2), days=()),
        SSECalendar(
            coverage_start=date(2026, 9, 24),
            coverage_end=date(2026, 10, 2),
            days=_calendar().days + (_calendar().days[1],),
        ),
        SSECalendar(
            coverage_start=date(2026, 9, 24),
            coverage_end=date(2026, 10, 2),
            days=_calendar().days + (SSECalendarDay(day=date(2026, 9, 25), is_open=True),),
        ),
        SSECalendar(
            coverage_start=date(2026, 9, 24),
            coverage_end=date(2026, 10, 2),
            days=tuple(day for day in _calendar().days if day.day != date(2026, 9, 25)),
        ),
    ],
)
def test_empty_duplicate_conflicting_or_gapped_calendar_fails_closed(
    calendar: SSECalendar,
) -> None:
    result = _select((_fact(),), datetime(2026, 9, 28, 10, 0, tzinfo=SHANGHAI), calendar)

    assert result.status == "unknown"
    assert result.reason == "invalid_calendar"


def test_calendar_must_cover_publication_and_decision_and_contain_next_open_day() -> None:
    short_start = _calendar(start=date(2026, 9, 25))
    short_end = _calendar(end=date(2026, 9, 27))
    no_future_open = _calendar(
        start=date(2026, 9, 28),
        end=date(2026, 9, 30),
        open_dates=frozenset({date(2026, 9, 28), date(2026, 9, 29)}),
    )

    at_decision = datetime(2026, 9, 28, 10, 0, tzinfo=SHANGHAI)
    assert _select((_fact(),), at_decision, short_start).status == "unknown"
    assert _select((_fact(),), at_decision, short_end).status == "unknown"
    assert (
        _select(
            (_fact(ann_date=date(2026, 9, 29)),),
            datetime(2026, 9, 30, 10, 0, tzinfo=SHANGHAI),
            no_future_open,
        ).reason
        == "no_next_open_day"
    )


def test_missing_announcement_observation_or_value_is_unknown_not_zero() -> None:
    as_of = datetime(2026, 9, 28, 10, 0, tzinfo=SHANGHAI)
    for fact in (
        _fact(ann_date=None),
        _fact(first_observed_at=None),
        _fact(first_observed_at=datetime(2026, 9, 24, 16, 0)),
        _fact(value=None),
    ):
        decision = _select((fact,), as_of)
        assert decision.status == "unknown"
        assert decision.fact is None


def test_revision_selection_preserves_older_version_until_new_one_is_observed() -> None:
    old = _fact()
    revised = _fact(
        value=Decimal("120.00"),
        update_flag="1",
        first_observed_at=datetime(2026, 9, 29, 13, 0, tzinfo=SHANGHAI),
    )

    historical = _select((revised, old), datetime(2026, 9, 29, 12, 0, tzinfo=SHANGHAI))
    revised_now = _select((old, revised), datetime(2026, 9, 29, 13, 1, tzinfo=SHANGHAI))

    assert historical.fact == old
    assert revised_now.fact == revised
    assert historical.content_sha256 != revised_now.content_sha256


def test_later_blocker_masks_old_value_until_a_visible_recovery() -> None:
    old = _fact()
    blocker = _fact(
        value=None,
        first_observed_at=datetime(2026, 9, 29, 10, tzinfo=SHANGHAI),
    )
    recovery = _fact(
        value=Decimal("120"),
        ann_date=date(2026, 9, 29),
        first_observed_at=datetime(2026, 9, 29, 11, tzinfo=SHANGHAI),
    )

    assert _select((old, blocker), datetime(2026, 9, 29, 9, 59, tzinfo=SHANGHAI)).fact == old
    masked = _select((old, blocker, recovery), datetime(2026, 9, 29, 12, tzinfo=SHANGHAI))
    assert masked.status == "unknown"
    assert masked.reason == "missing_value"
    restored = _select((old, blocker, recovery), datetime(2026, 9, 30, 9, 30, tzinfo=SHANGHAI))
    assert restored.status == "selected"
    assert restored.fact == recovery


def test_future_observation_does_not_require_future_calendar_to_select_old_version() -> None:
    old = _fact()
    future = _fact(
        ann_date=date(2026, 10, 3),
        first_observed_at=datetime(2026, 10, 3, 12, 0, tzinfo=SHANGHAI),
        value=Decimal("130"),
        update_flag="1",
    )

    decision = _select((old, future), datetime(2026, 9, 28, 10, 0, tzinfo=SHANGHAI))

    assert decision.status == "selected"
    assert decision.fact == old


def test_same_instant_conflicting_versions_are_unknown() -> None:
    first = _fact()
    conflict = _fact(value=Decimal("101.00"))

    decision = _select((first, conflict), datetime(2026, 9, 28, 10, 0, tzinfo=SHANGHAI))

    assert decision.status == "unknown"
    assert decision.reason == "conflicting_versions"
    assert decision.fact is None


def test_exact_repeated_pull_is_idempotent_and_utc_offset_does_not_change_digest() -> None:
    first = _fact()
    offset_equivalent = _fact(first_observed_at=FIRST_SEEN.astimezone(UTC))
    as_of = datetime(2026, 9, 28, 10, 0, tzinfo=SHANGHAI)

    single = _select((first,), as_of)
    repeated = _select((first, first, offset_equivalent), as_of)

    assert repeated.status == "selected"
    assert repeated.fact == first
    assert repeated.content_sha256 == single.content_sha256


def test_digest_binds_dates_observation_value_and_logical_key() -> None:
    base = _fact()
    as_of = datetime(2026, 10, 1, 17, 0, tzinfo=SHANGHAI)
    variants = (
        _fact(ann_date=date(2026, 9, 25)),
        _fact(f_ann_date=date(2026, 9, 29)),
        _fact(first_observed_at=FIRST_SEEN + timedelta(minutes=1)),
        _fact(value=Decimal("101")),
        _fact(field="net_profit"),
    )
    baseline = _select((base,), as_of).content_sha256

    assert all(_select((variant,), as_of).content_sha256 != baseline for variant in variants)


def test_digest_preserves_all_decimal_digits_without_context_rounding() -> None:
    as_of = datetime(2026, 9, 28, 10, 0, tzinfo=SHANGHAI)
    first = _fact(value=Decimal("1.234567890123456789012345678901"))
    second = _fact(value=Decimal("1.234567890123456789012345678902"))

    assert _select((first,), as_of).content_sha256 != _select((second,), as_of).content_sha256
    assert (
        _select((_fact(value=Decimal("100.00")),), as_of).content_sha256
        == _select((_fact(value=Decimal("100")),), as_of).content_sha256
    )


def test_revisions_from_different_logical_keys_cannot_be_compared() -> None:
    result = _select(
        (_fact(field="revenue"), _fact(field="net_profit")),
        datetime(2026, 9, 28, 10, 0, tzinfo=SHANGHAI),
    )

    assert result.status == "unknown"
    assert result.reason == "mixed_logical_keys"
    assert result.fact is None


def test_naive_decision_time_is_unknown() -> None:
    result = _select((_fact(),), datetime(2026, 9, 28, 10, 0))

    assert result.status == "unknown"
    assert result.fact is None


def test_decision_time_outside_exchange_clock_range_is_unknown() -> None:
    result = _select((_fact(),), datetime.max.replace(tzinfo=UTC))

    assert result.status == "unknown"
    assert result.fact is None


def test_domain_dates_reject_timestamps_instead_of_silently_truncating_them() -> None:
    midnight = datetime(2026, 9, 24, tzinfo=UTC)

    with pytest.raises(ValidationError):
        SSECalendarDay(day=midnight, is_open=True)
    with pytest.raises(ValidationError):
        _fact(ann_date=midnight)
