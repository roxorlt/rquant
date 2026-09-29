"""Offline factor expression values use explicit market days and visible observations."""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from rquant.factor.definition import FactorDefinition, build_factor_definition
from rquant.factor.expression import FeatureCatalog

_MARKET_TZ = timezone(timedelta(hours=8))
_A = "000001.SZ"
_B = "000002.SZ"
_DAYS = (date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5))

if TYPE_CHECKING:
    from rquant.factor.time_series import (
        DecisionTime,
        FactorTimeSeriesInput,
        FactorTimeSeriesResult,
        FactorTimeSeriesValue,
        FeatureObservation,
    )


def _definition(expression: str, *, earliest_available_date: date = _DAYS[0]) -> FactorDefinition:
    return build_factor_definition(
        factor_id="synthetic_factor",
        name_zh="合成因子",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=earliest_available_date,
        expression=expression,
        feature_catalog=FeatureCatalog(columns=("close", "volume")),
    )


def _at(day: date, hour: int = 14, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=_MARKET_TZ)


def _observation(
    day: date,
    stock_code: str,
    column: str,
    value: float | None,
    *,
    visible_at: datetime | None = None,
) -> FeatureObservation:
    from rquant.factor.time_series import FeatureObservation

    return FeatureObservation(
        stock_code=stock_code,
        trade_date=day,
        column=column,
        value=value,
        first_visible_at=visible_at or _at(day),
    )


def _input(
    expression: str,
    days: tuple[date, ...],
    observations: tuple[FeatureObservation, ...],
    *,
    universe: tuple[str, ...] = (_A,),
    decision_times: tuple[DecisionTime, ...] | None = None,
    earliest_available_date: date = _DAYS[0],
) -> FactorTimeSeriesInput:
    from rquant.factor.time_series import DecisionTime, FactorTimeSeriesInput

    return FactorTimeSeriesInput(
        definition=_definition(expression, earliest_available_date=earliest_available_date),
        universe=universe,
        trading_days=days,
        decision_times=decision_times
        if decision_times is not None
        else tuple(DecisionTime(trade_date=day, decision_at=_at(day, 15)) for day in days),
        observations=observations,
    )


def _run(data: FactorTimeSeriesInput) -> FactorTimeSeriesResult:
    from rquant.factor.time_series import evaluate_factor_time_series

    return evaluate_factor_time_series(data)


def _point(result: FactorTimeSeriesResult, day: date, stock_code: str) -> FactorTimeSeriesValue:
    return next(
        point
        for point in result.values
        if point.trade_date == day and point.stock_code == stock_code
    )


def _base_observations() -> tuple[FeatureObservation, ...]:
    rows: list[FeatureObservation] = []
    for index, day in enumerate(_DAYS):
        rows.extend(
            (
                _observation(day, _A, "close", float(index + 1)),
                _observation(day, _A, "volume", float((index + 1) * 2)),
                _observation(day, _B, "close", 4.0),
                _observation(day, _B, "volume", 1.0),
            )
        )
    return tuple(rows)


def test_ref_uses_previous_calendar_day_without_forward_filling() -> None:
    days = _DAYS[:3]
    result = _run(
        _input(
            "ref(close, 1)",
            days,
            (_observation(days[0], _A, "close", 1.0), _observation(days[2], _A, "close", 3.0)),
        )
    )

    assert [(point.trade_date, point.value, point.missing_reason) for point in result.values] == [
        (days[0], None, "insufficient_history"),
        (days[1], 1.0, None),
        (days[2], None, "missing_observation"),
    ]


@pytest.mark.parametrize(
    ("expression", "expected", "visible_day_index"),
    [
        ("ref(close, 2)", 1.0, 0),
        ("ts_mean(close, 3)", 2.0, 2),
        ("ts_std(close, 3)", 1.0, 2),
        ("ts_delta(close, 2)", 2.0, 2),
        ("ts_rank(close, 3)", 1.0, 2),
        ("ts_corr(close, volume, 3)", 1.0, 2),
        ("ts_mean(ref(close, 1), 2) + ts_delta(volume, 1) / 2", 2.5, 2),
    ],
)
def test_time_series_operators_match_hand_calculated_values(
    expression: str, expected: float, visible_day_index: int
) -> None:
    result = _run(_input(expression, _DAYS, _base_observations(), universe=(_A, _B)))
    point = _point(result, _DAYS[2], _A)
    assert point.value == pytest.approx(expected)
    assert point.missing_reason is None
    assert point.latest_visible_at == _at(_DAYS[visible_day_index])
    assert result.factor_id == "synthetic_factor"
    assert result.version == 1
    assert result == _run(_input(expression, _DAYS, _base_observations(), universe=(_A, _B)))


def test_rank_uses_average_tie_rank_and_std_of_constant_series_is_zero() -> None:
    observations = _base_observations()
    ranked = _run(_input("ts_rank(close, 3)", _DAYS, observations, universe=(_A, _B)))
    std = _run(_input("ts_std(close, 3)", _DAYS, observations, universe=(_A, _B)))
    assert _point(ranked, _DAYS[2], _B).value == pytest.approx(2 / 3)
    assert _point(std, _DAYS[2], _B).value == 0.0


def test_nonperfect_pearson_correlation_matches_hand_calculation() -> None:
    days = _DAYS[:3]
    observations = tuple(
        row
        for index, day in enumerate(days)
        for row in (
            _observation(day, _A, "close", float(index + 1)),
            _observation(day, _A, "volume", float((1, 2, 4)[index])),
        )
    )
    result = _run(_input("ts_corr(close, volume, 3)", days, observations))
    assert _point(result, days[2], _A).value == pytest.approx(math.sqrt(27 / 28))


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("-close * 2", -2.0),
        ("+close", 1.0),
        ("close + volume", 3.0),
        ("close - volume", -1.0),
        ("close * volume", 2.0),
        ("close / volume", 0.5),
        ("close < volume", 1.0),
        ("close <= volume", 1.0),
        ("close > volume", 0.0),
        ("close >= volume", 0.0),
    ],
)
def test_arithmetic_and_comparison_operators(expression: str, expected: float) -> None:
    day = _DAYS[0]
    observations = (_observation(day, _A, "close", 1.0), _observation(day, _A, "volume", 2.0))
    result = _run(_input(expression, (day,), observations))
    assert _point(result, day, _A).value == expected


def test_correlation_rejects_zero_variance_and_single_sample_window() -> None:
    observations = _base_observations()
    correlation = _run(_input("ts_corr(close, volume, 3)", _DAYS, observations, universe=(_A, _B)))
    one_sample = _run(_input("ts_corr(close, volume, 1)", _DAYS, observations, universe=(_A, _B)))
    assert _point(correlation, _DAYS[2], _B).missing_reason == "zero_variance"
    assert _point(one_sample, _DAYS[2], _A).missing_reason == "insufficient_samples"


def test_comparison_returns_numeric_boolean_and_tracks_latest_visibility() -> None:
    observations = tuple(
        row
        for row in _base_observations()
        if (row.trade_date, row.stock_code, row.column) != (_DAYS[2], _A, "close")
    ) + (_observation(_DAYS[2], _A, "close", 3.0, visible_at=_at(_DAYS[2], 14, 20)),)
    result = _run(_input("close > ref(close, 1)", _DAYS, observations, universe=(_A, _B)))
    assert _point(result, _DAYS[2], _A).value == 1.0
    assert _point(result, _DAYS[2], _A).latest_visible_at == _at(_DAYS[2], 14, 20)
    assert _point(result, _DAYS[2], _B).value == 0.0


def test_rolling_window_does_not_fill_missing_or_explicitly_absent_values() -> None:
    days = _DAYS[:3]
    observations = (
        _observation(days[0], _A, "close", 1.0),
        _observation(days[2], _A, "close", 3.0),
        _observation(days[0], _B, "close", 4.0),
        _observation(days[1], _B, "close", None),
        _observation(days[2], _B, "close", 4.0),
    )
    result = _run(_input("ts_mean(close, 3)", days, observations, universe=(_A, _B)))
    assert _point(result, days[2], _A).missing_reason == "missing_observation"
    assert _point(result, days[2], _B).missing_reason == "missing_value"
    assert _point(result, days[0], _A).missing_reason == "insufficient_history"


def test_delta_reads_its_calendar_offset_even_if_middle_day_is_missing() -> None:
    days = _DAYS[:3]
    observations = (
        _observation(days[0], _A, "close", 1.0),
        _observation(days[2], _A, "close", 3.0),
    )
    result = _run(_input("ts_delta(close, 2)", days, observations))
    assert _point(result, days[2], _A).value == 2.0


def test_delta_reports_absorbed_subtraction_as_precision_limit() -> None:
    days = _DAYS[:2]
    observations = (
        _observation(days[0], _A, "close", 1.0),
        _observation(days[1], _A, "close", 1e20),
    )
    result = _run(_input("ts_delta(close, 1)", days, observations))
    assert _point(result, days[1], _A).missing_reason == "precision_limit"


@pytest.mark.parametrize(
    ("expression", "close", "volume", "reason"),
    [
        ("close / (volume - volume)", 1.0, 2.0, "zero_division"),
        ("close / volume", 1e308, 1e-308, "non_finite_result"),
        ("close * volume", 1e-300, 1e-300, "precision_limit"),
        ("close + volume", 1e20, 1.0, "precision_limit"),
        ("close - volume", 1e20, 1.0, "precision_limit"),
    ],
)
def test_invalid_arithmetic_returns_stable_missing_reason(
    expression: str, close: float, volume: float, reason: str
) -> None:
    day = _DAYS[0]
    result = _run(
        _input(
            expression,
            (day,),
            (_observation(day, _A, "close", close), _observation(day, _A, "volume", volume)),
        )
    )
    point = _point(result, day, _A)
    assert point.value is None
    assert point.missing_reason == reason


@pytest.mark.parametrize(
    "expression",
    [
        "industry_neutralize(close)",
        "size_neutralize(close)",
        "close + industry_neutralize(volume)",
    ],
)
def test_neutralization_without_context_is_explicitly_unavailable(expression: str) -> None:
    data = _input(expression, (_DAYS[0],), _base_observations()[:2])
    result = _run(data)
    assert _point(result, _DAYS[0], _A).missing_reason == "missing_context"


def test_observation_after_its_decision_time_is_rejected() -> None:
    day = _DAYS[0]
    observation = _observation(day, _A, "close", 1.0, visible_at=_at(day, 16))
    with pytest.raises(ValidationError) as error:
        _input("close", (day,), (observation,))
    assert error.value.errors()[0]["type"] == "factor_future_observation"


def test_duplicate_or_outside_observations_are_rejected() -> None:
    day = _DAYS[0]
    good = _observation(day, _A, "close", 1.0)
    cases = (
        ((good, good), "factor_duplicate_observation"),
        ((_observation(day, _B, "close", 1.0),), "factor_unknown_stock"),
        ((_observation(day, _A, "outside", 1.0),), "factor_unknown_column"),
        ((_observation(_DAYS[1], _A, "close", 1.0),), "factor_unknown_trade_date"),
    )
    for observations, expected_type in cases:
        with pytest.raises(ValidationError) as error:
            _input("close", (day,), observations)
        assert error.value.errors()[0]["type"] == expected_type


def test_calendar_and_decision_moments_must_match_exactly() -> None:
    from rquant.factor.time_series import DecisionTime

    with pytest.raises(ValidationError) as error:
        _input("close", (_DAYS[1], _DAYS[0]), ())
    assert error.value.errors()[0]["type"] == "factor_invalid_calendar"

    with pytest.raises(ValidationError) as error:
        _input(
            "close",
            _DAYS[:2],
            (),
            decision_times=(DecisionTime(trade_date=_DAYS[0], decision_at=_at(_DAYS[0], 15)),),
        )
    assert error.value.errors()[0]["type"] == "factor_decision_mismatch"


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_non_finite_input_observation_is_rejected(value: float) -> None:
    with pytest.raises(ValidationError):
        _observation(_DAYS[0], _A, "close", value)


def test_dates_before_definition_availability_stay_empty_but_can_supply_history() -> None:
    days = (date(2023, 12, 29), _DAYS[0])
    result = _run(
        _input(
            "ts_mean(close, 2)",
            days,
            (_observation(days[0], _A, "close", 1.0), _observation(days[1], _A, "close", 2.0)),
            earliest_available_date=days[1],
        )
    )
    assert _point(result, days[0], _A).missing_reason == "before_available_date"
    assert _point(result, days[1], _A).value == pytest.approx(1.5)


def test_contracts_reject_naive_times_and_results_are_frozen() -> None:
    from rquant.factor.time_series import DecisionTime, FeatureObservation

    day = _DAYS[0]
    with pytest.raises(ValidationError):
        DecisionTime(trade_date=day, decision_at=datetime(2024, 1, 2, 15))
    with pytest.raises(ValidationError):
        FeatureObservation(
            stock_code=_A,
            trade_date=day,
            column="close",
            value=1.0,
            first_visible_at=datetime(2024, 1, 2, 14),
        )
    result = _run(_input("close", (day,), (_observation(day, _A, "close", 1.0),)))
    with pytest.raises(ValidationError):
        result.values[0].value = 2.0


def test_factor_package_exports_time_series_contracts() -> None:
    import rquant.factor as factor

    assert factor.FactorTimeSeriesInput.__name__ == "FactorTimeSeriesInput"
    assert factor.evaluate_factor_time_series.__name__ == "evaluate_factor_time_series"


def test_constant_result_is_date_major_and_round_trips_without_observations() -> None:
    from rquant.factor.time_series import FactorTimeSeriesInput, FactorTimeSeriesResult

    days = _DAYS[:2]
    data = _input("2.5", days, (), universe=(_B, _A))
    assert FactorTimeSeriesInput.model_validate_json(data.model_dump_json()) == data
    result = _run(data)
    assert [
        (point.trade_date, point.stock_code, point.value, point.latest_visible_at)
        for point in result.values
    ] == [
        (days[0], _B, 2.5, None),
        (days[0], _A, 2.5, None),
        (days[1], _B, 2.5, None),
        (days[1], _A, 2.5, None),
    ]
    assert FactorTimeSeriesResult.model_validate_json(result.model_dump_json()) == result


def test_stock_and_observation_budgets_reject_entire_input() -> None:
    from rquant.factor.time_series import MAX_OBSERVATIONS, MAX_STOCKS

    with pytest.raises(ValidationError):
        _input(
            "close",
            (_DAYS[0],),
            (),
            universe=tuple(f"{index:06d}.SZ" for index in range(MAX_STOCKS + 1)),
        )

    one = _observation(_DAYS[0], _A, "close", 1.0)
    with pytest.raises(ValidationError):
        _input("close", (_DAYS[0],), (one,) * (MAX_OBSERVATIONS + 1))
