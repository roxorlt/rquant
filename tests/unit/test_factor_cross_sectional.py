"""Hand-calculated cross sections over fixed stocks and decision dates."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from rquant.factor.definition import build_factor_definition
from rquant.factor.expression import FeatureCatalog
from rquant.factor.time_series import (
    DecisionTime,
    FactorTimeSeriesInput,
    FactorTimeSeriesResult,
    FactorTimeSeriesValue,
    FeatureObservation,
    evaluate_factor_time_series,
)

_TZ = timezone(timedelta(hours=8))
_A, _B, _C, _D = "A", "B", "C", "D"
_DAYS = (date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4))


def _at(day: date, hour: int = 14, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=_TZ)


def _row(day: date, stock: str, value: float | None, *, minute: int = 0) -> FeatureObservation:
    return FeatureObservation(
        stock_code=stock,
        trade_date=day,
        column="close",
        value=value,
        first_visible_at=_at(day, minute=minute),
    )


def _run(
    expression: str,
    days: tuple[date, ...],
    rows: tuple[FeatureObservation, ...],
    *,
    universe: tuple[str, ...] = (_A, _B, _C),
) -> FactorTimeSeriesResult:
    definition = build_factor_definition(
        factor_id="cross_section",
        name_zh="横截面样本",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=days[0],
        expression=expression,
        feature_catalog=FeatureCatalog(columns=("close",)),
    )
    data = FactorTimeSeriesInput(
        definition=definition,
        universe=universe,
        trading_days=days,
        decision_times=tuple(
            DecisionTime(trade_date=day, decision_at=_at(day, 15)) for day in days
        ),
        observations=rows,
    )
    return evaluate_factor_time_series(data)


def _point(result: FactorTimeSeriesResult, day: date, stock: str) -> FactorTimeSeriesValue:
    return next(
        point for point in result.values if point.trade_date == day and point.stock_code == stock
    )


def test_rank_uses_same_day_valid_pool_average_ties_and_one_stock() -> None:
    first, second = _DAYS[:2]
    rows = (
        _row(first, _A, 1.0),
        _row(first, _B, 2.0),
        _row(first, _C, 2.0, minute=40),
        _row(first, _D, None),
        _row(second, _B, 5.0),
        _row(second, _C, None),
    )
    result = _run("cs_rank(close)", (first, second), rows, universe=(_A, _B, _C, _D))

    assert _point(result, first, _A).value == pytest.approx(1 / 3)
    assert _point(result, first, _B).value == pytest.approx(5 / 6)
    assert _point(result, first, _C).value == pytest.approx(5 / 6)
    assert all(
        _point(result, first, stock).latest_visible_at == _at(first, minute=40)
        for stock in (_A, _B, _C)
    )
    assert _point(result, first, _D).missing_reason == "missing_value"
    assert _point(result, second, _B).value == 1.0
    assert _point(result, second, _A).missing_reason == "missing_observation"
    assert _point(result, second, _C).missing_reason == "missing_value"
    assert _point(result, second, _D).missing_reason == "missing_observation"


def test_zscore_uses_population_std_and_distinguishes_small_or_constant_pool() -> None:
    first, second, third = _DAYS
    rows = (
        _row(first, _A, 1.0),
        _row(first, _B, 2.0, minute=25),
        _row(first, _C, 3.0),
        _row(second, _A, 7.0),
        _row(second, _B, None),
        _row(third, _A, 4.0),
        _row(third, _B, 4.0),
        _row(third, _C, 4.0),
    )
    result = _run("cs_zscore(close)", _DAYS, rows)

    assert _point(result, first, _A).value == pytest.approx(-((3 / 2) ** 0.5))
    assert _point(result, first, _B).value == 0.0
    assert _point(result, first, _C).value == pytest.approx((3 / 2) ** 0.5)
    assert _point(result, first, _A).latest_visible_at == _at(first, minute=25)
    assert _point(result, second, _A).missing_reason == "insufficient_samples"
    assert _point(result, second, _B).missing_reason == "missing_value"
    assert _point(result, second, _C).missing_reason == "missing_observation"
    assert all(
        _point(result, third, stock).missing_reason == "zero_variance" for stock in (_A, _B, _C)
    )


def test_winsorize_uses_median_mad_and_clips_to_median_when_mad_is_zero() -> None:
    first, second = _DAYS[:2]
    rows = tuple(
        _row(day, stock, value, minute=45 if stock == _D else 0)
        for day, values in ((first, (0.0, 1.0, 2.0, 100.0)), (second, (1.0, 1.0, 1.0, 100.0)))
        for stock, value in zip((_A, _B, _C, _D), values, strict=True)
    )
    result = _run("cs_winsorize(close, 1)", (first, second), rows, universe=(_A, _B, _C, _D))

    assert _point(result, first, _A).value == pytest.approx(1.5 - 1.4826)
    assert _point(result, first, _B).value == 1.0
    assert _point(result, first, _C).value == 2.0
    assert _point(result, first, _D).value == pytest.approx(1.5 + 1.4826)
    assert all(_point(result, second, stock).value == 1.0 for stock in (_A, _B, _C, _D))
    assert _point(result, first, _A).latest_visible_at == _at(first, minute=45)


def test_ref_and_rolling_windows_nest_on_both_sides_of_cross_section() -> None:
    first, second = _DAYS[:2]
    rows = (
        _row(first, _A, 1.0),
        _row(first, _B, 5.0),
        _row(first, _C, 9.0, minute=25),
        _row(second, _A, 10.0),
        _row(second, _B, 0.0),
        _row(second, _C, 5.0, minute=45),
    )
    earlier_rank = _run("ref(cs_rank(close), 1)", (first, second), rows)
    rank_of_earlier = _run("cs_rank(ref(close, 1))", (first, second), rows)
    rolling_rank = _run("ts_mean(cs_rank(close), 2)", (first, second), rows)
    rank_of_rolling = _run("cs_rank(ts_mean(close, 2))", (first, second), rows)

    assert [_point(earlier_rank, second, stock).value for stock in (_A, _B, _C)] == pytest.approx(
        (1 / 3, 2 / 3, 1.0)
    )
    assert [
        _point(rank_of_earlier, second, stock).value for stock in (_A, _B, _C)
    ] == pytest.approx((1 / 3, 2 / 3, 1.0))
    assert _point(rank_of_earlier, first, _A).missing_reason == "insufficient_history"
    assert _point(earlier_rank, second, _A).latest_visible_at == _at(first, minute=25)
    assert [_point(rolling_rank, second, stock).value for stock in (_A, _B, _C)] == pytest.approx(
        (2 / 3, 1 / 2, 5 / 6)
    )
    assert [
        _point(rank_of_rolling, second, stock).value for stock in (_A, _B, _C)
    ] == pytest.approx((2 / 3, 1 / 3, 1.0))
    assert _point(rank_of_rolling, second, _A).latest_visible_at == _at(second, minute=45)


def test_cross_section_of_ref_excludes_missing_prior_day_without_filling() -> None:
    first, second = _DAYS[:2]
    rows = (
        _row(first, _A, 1.0),
        _row(first, _C, 3.0, minute=35),
        _row(second, _A, 10.0),
        _row(second, _B, 20.0),
        _row(second, _C, 30.0),
    )
    result = _run("cs_rank(ref(close, 1))", (first, second), rows)

    assert _point(result, first, _A).missing_reason == "insufficient_history"
    assert _point(result, second, _A).value == 0.5
    assert _point(result, second, _B).missing_reason == "missing_observation"
    assert _point(result, second, _C).value == 1.0
    assert _point(result, second, _A).latest_visible_at == _at(first, minute=35)


def test_other_stocks_same_day_value_and_visibility_affect_only_that_date() -> None:
    first, second = _DAYS[:2]
    base = (_row(first, _A, 1.0), _row(first, _B, 2.0), _row(second, _A, 1.0))
    original = _run(
        "cs_rank(close)",
        (first, second),
        base + (_row(second, _B, 2.0, minute=20),),
        universe=(_A, _B),
    )
    changed = _run(
        "cs_rank(close)",
        (first, second),
        base + (_row(second, _B, 0.0, minute=50),),
        universe=(_A, _B),
    )

    assert _point(original, first, _A) == _point(changed, first, _A)
    assert _point(original, second, _A).value == 0.5
    assert _point(changed, second, _A).value == 1.0
    assert _point(original, second, _A).latest_visible_at == _at(second, minute=20)
    assert _point(changed, second, _A).latest_visible_at == _at(second, minute=50)
    assert changed == _run(
        "cs_rank(close)",
        (first, second),
        base + (_row(second, _B, 0.0, minute=50),),
        universe=(_A, _B),
    )


def test_extreme_cross_sectional_statistics_are_finite_or_explicitly_missing() -> None:
    day = _DAYS[0]
    rows = (_row(day, _A, -1e308), _row(day, _B, 1e308))
    zscore = _run("cs_zscore(close)", (day,), rows, universe=(_A, _B))
    winsorized = _run("cs_winsorize(close, 10)", (day,), rows, universe=(_A, _B))

    assert _point(zscore, day, _A).value == pytest.approx(-1.0)
    assert _point(zscore, day, _B).value == pytest.approx(1.0)
    assert _point(winsorized, day, _A).missing_reason == "non_finite_result"
    assert _point(winsorized, day, _B).missing_reason == "non_finite_result"
