"""IC decay pairs frozen base-date factors with later evaluation-date returns."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rquant.factor import (
    DecisionTime,
    FactorTimeSeriesInput,
    FeatureCatalog,
    FeatureObservation,
    build_factor_definition,
)
from rquant.factor.result import FactorForwardReturn, FactorResearchRequest

_TZ = timezone(timedelta(hours=8))
_DAYS = (date(2026, 7, 17), date(2026, 7, 20), date(2026, 7, 21))
_STOCKS = ("A", "B", "C")
_DECISIONS = tuple(datetime(day.year, day.month, day.day, 9, 25, tzinfo=_TZ) for day in _DAYS)
_ENDS = (_DECISIONS[1], _DECISIONS[2], datetime(2026, 7, 22, 9, 25, tzinfo=_TZ))
_AS_OF = _ENDS[2] + timedelta(hours=1)
_FACTORS: tuple[tuple[float | None, ...], ...] = (
    (1.0, 2.0, 3.0),
    (3.0, 2.0, 1.0),
    (1.0, 2.0, 3.0),
)
_RETURNS: tuple[tuple[float | None, ...], ...] = (
    (0.1, 0.2, 0.3),
    (0.3, 0.2, 0.1),
    (0.2, None, None),
)


def _request(
    *,
    factors: tuple[tuple[float | None, ...], ...] = _FACTORS,
    returns: tuple[tuple[float | None, ...], ...] = _RETURNS,
    missing_by_date: dict[int, str] | None = None,
    as_of: datetime = _AS_OF,
    observations: tuple[FeatureObservation, ...] | None = None,
    forward_returns: tuple[FactorForwardReturn, ...] | None = None,
    evaluation_days: tuple[date, ...] | None = None,
) -> FactorResearchRequest:
    if observations is None:
        observations = tuple(
            FeatureObservation(
                stock_code=stock,
                trade_date=day,
                column="close",
                value=factors[day_index][stock_index],
                first_visible_at=_DECISIONS[day_index] - timedelta(minutes=1),
            )
            for day_index, day in enumerate(_DAYS)
            for stock_index, stock in enumerate(_STOCKS)
        )
    if forward_returns is None:
        forward_returns = tuple(
            FactorForwardReturn(
                stock_code=stock,
                decision_date=day,
                decision_at=_DECISIONS[day_index],
                return_end_at=_ENDS[day_index],
                value=(
                    returns[day_index][stock_index]
                    if not missing_by_date or day_index not in missing_by_date
                    else None
                ),
                missing_reason=(
                    missing_by_date[day_index]
                    if missing_by_date and day_index in missing_by_date
                    else "missing_price"
                    if returns[day_index][stock_index] is None
                    else None
                ),
                first_available_at=(
                    _ENDS[day_index]
                    if returns[day_index][stock_index] is not None
                    and (not missing_by_date or day_index not in missing_by_date)
                    else None
                ),
                expected_available_at=(
                    _ENDS[day_index] + timedelta(hours=5, minutes=35)
                    if missing_by_date and missing_by_date.get(day_index) == "visibility_pending"
                    else None
                ),
            )
            for day_index, day in enumerate(_DAYS)
            for stock_index, stock in enumerate(_STOCKS)
            if evaluation_days is None or day in evaluation_days
        )
    return FactorResearchRequest(
        factor_input=FactorTimeSeriesInput(
            definition=build_factor_definition(
                factor_id="close_factor",
                name_zh="收盘价因子",
                category="technical",
                direction="higher_is_better",
                version=1,
                earliest_available_date=_DAYS[0],
                expression="close",
                feature_catalog=FeatureCatalog(columns=("close",)),
            ),
            universe=_STOCKS,
            trading_days=_DAYS,
            decision_times=tuple(
                DecisionTime(trade_date=day, decision_at=decision)
                for day, decision in zip(_DAYS, _DECISIONS, strict=True)
            ),
            observations=observations,
        ),
        evaluation_days=evaluation_days,
        forward_returns=forward_returns,
        as_of=as_of,
        factor_source_id="factor-snapshot",
        return_source_id="return-snapshot",
        return_price_basis="forward_adjusted",
        holding_sessions=1,
    )


def test_hand_calculated_weekend_decay_and_missing_periods_are_explicit() -> None:
    from rquant.factor.decay import evaluate_factor_ic_decay

    request = _request()
    result = evaluate_factor_ic_decay(request)

    assert result.factor_id == "close_factor"
    assert result.factor_source_id == "factor-snapshot"
    assert result.return_source_id == "return-snapshot"
    assert result.holding_sessions == 1
    assert result.evaluation_days == _DAYS
    assert len(result.input_sha256) == 64
    assert [period.lag for period in result.periods] == list(range(1, 11))
    first, second, third, fourth = result.periods[:4]
    assert (first.source_day_count, first.valid_pair_count) == (3, 7)
    assert [(day.base_date, day.target_date, day.valid_pair_count) for day in first.days] == [
        (_DAYS[0], _DAYS[0], 3),
        (_DAYS[1], _DAYS[1], 3),
        (_DAYS[2], _DAYS[2], 1),
    ]
    assert first.ic_summary is not None
    assert first.ic_summary.normal_ic.source_day_count == 3
    assert first.ic_summary.normal_ic.valid_day_count == 2
    assert first.ic_summary.normal_ic.mean == pytest.approx(1.0)
    assert first.ic_summary.rank_ic.mean == pytest.approx(1.0)
    assert (second.source_day_count, second.valid_pair_count) == (2, 4)
    assert second.days[0].base_date == _DAYS[0]
    assert second.days[0].target_date == _DAYS[1]
    assert second.ic_summary is not None
    assert second.ic_summary.normal_ic.mean == pytest.approx(-1.0)
    assert second.ic_summary.rank_ic.mean == pytest.approx(-1.0)
    assert third.status == "no_valid_days"
    assert (third.source_day_count, third.valid_pair_count) == (1, 1)
    assert third.ic_summary is not None
    assert third.ic_summary.normal_ic.status == "no_valid_days"
    assert third.days[0].normal_ic.status == "insufficient_samples"
    assert fourth.status == "no_target_period"
    assert fourth.source_day_count == fourth.valid_pair_count == 0
    assert fourth.ic_summary is None
    assert all(period.status == "no_target_period" for period in result.periods[3:])
    assert evaluate_factor_ic_decay(request).model_dump() == result.model_dump()


def test_target_returns_change_only_pairs_using_that_target_date() -> None:
    from rquant.factor.decay import evaluate_factor_ic_decay

    factors = (_FACTORS[0], (None, None, None), (None, None, None))
    first = evaluate_factor_ic_decay(_request(factors=factors))
    changed_returns = (_RETURNS[0], (0.1, 0.2, 0.3), _RETURNS[2])
    second = evaluate_factor_ic_decay(_request(factors=factors, returns=changed_returns))

    assert first.periods[0].ic_summary == second.periods[0].ic_summary
    assert first.periods[1].ic_summary != second.periods[1].ic_summary
    assert first.periods[2].ic_summary == second.periods[2].ic_summary


def test_target_date_factors_never_replace_base_date_factors() -> None:
    from rquant.factor.decay import evaluate_factor_ic_decay

    first = evaluate_factor_ic_decay(_request())
    changed_factors = (_FACTORS[0], (1.0, 2.0, 3.0), _FACTORS[2])
    second = evaluate_factor_ic_decay(_request(factors=changed_factors))

    assert first.periods[1].days[0].normal_ic == second.periods[1].days[0].normal_ic
    assert first.periods[1].days[0].rank_ic == second.periods[1].days[0].rank_ic


@pytest.mark.parametrize("reason", ["visibility_pending", "window_unfinished"])
def test_unavailable_target_window_never_enters_later_period(reason: str) -> None:
    from rquant.factor.decay import evaluate_factor_ic_decay

    as_of = _ENDS[2] + timedelta(minutes=1) if reason == "visibility_pending" else _DECISIONS[2]
    result = evaluate_factor_ic_decay(_request(missing_by_date={2: reason}, as_of=as_of))

    assert result.periods[2].source_day_count == 1
    assert result.periods[2].valid_pair_count == 0
    assert result.periods[2].status == "no_valid_days"
    assert result.periods[2].ic_summary is not None
    assert result.periods[2].ic_summary.normal_ic.mean is None
    assert result.periods[2].days[0].normal_ic.status == "insufficient_samples"


def test_one_missing_stock_and_zero_variance_keep_daily_reason_counts() -> None:
    from rquant.factor.decay import evaluate_factor_ic_decay

    changed_factors = ((2.0, 2.0, 2.0), _FACTORS[1], _FACTORS[2])
    result = evaluate_factor_ic_decay(_request(factors=changed_factors))

    assert result.periods[0].days[0].normal_ic.status == "zero_variance"
    assert result.periods[0].days[0].rank_ic.status == "zero_variance"
    assert result.periods[0].days[2].valid_pair_count == 1
    assert result.periods[0].days[2].normal_ic.status == "insufficient_samples"


def test_reordered_input_grid_keeps_output_and_request_identity() -> None:
    from rquant.factor.decay import evaluate_factor_ic_decay

    original = _request()
    reordered = _request(
        observations=tuple(reversed(original.factor_input.observations)),
        forward_returns=tuple(reversed(original.forward_returns)),
    )

    assert (
        evaluate_factor_ic_decay(reordered).model_dump()
        == evaluate_factor_ic_decay(original).model_dump()
    )


def test_explicit_evaluation_sequence_defines_lags_and_no_future_values() -> None:
    from rquant.factor.decay import evaluate_factor_ic_decay

    only_friday_tuesday = (_DAYS[0], _DAYS[2])
    request = _request(evaluation_days=only_friday_tuesday)
    result = evaluate_factor_ic_decay(request)

    assert result.evaluation_days == only_friday_tuesday
    assert result.periods[1].days[0].target_date == _DAYS[2]
    assert result.periods[1].days[0].normal_ic.status == "insufficient_samples"
    assert result.periods[2].status == "no_target_period"
    assert result.periods[2].ic_summary is None

    bad_observations = list(request.factor_input.observations)
    bad_observations[0] = bad_observations[0].model_copy(
        update={"first_visible_at": _DECISIONS[0] + timedelta(seconds=1)}
    )
    with pytest.raises(ValidationError, match="visible"):
        _request(observations=tuple(bad_observations))
