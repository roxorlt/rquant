"""Point-in-time industry and size residuals over small hand-calculated pools."""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

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

if TYPE_CHECKING:
    from rquant.factor.time_series import IndustryObservation, MarketCapObservation

_TZ = timezone(timedelta(hours=8))
_DAY, _NEXT = date(2024, 1, 2), date(2024, 1, 3)
_STOCKS = ("A", "B", "C", "D")


def _at(day: date, hour: int = 14, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=_TZ)


def _feature(day: date, stock: str, value: float | None) -> FeatureObservation:
    return FeatureObservation(
        stock_code=stock,
        trade_date=day,
        column="close",
        value=value,
        first_visible_at=_at(day),
    )


def _industry(day: date, stock: str, value: str | None, *, minute: int = 0) -> IndustryObservation:
    from rquant.factor.time_series import IndustryObservation

    return IndustryObservation(
        stock_code=stock,
        trade_date=day,
        industry=value,
        first_visible_at=_at(day, minute=minute),
    )


def _size(day: date, stock: str, value: float | None, *, minute: int = 0) -> MarketCapObservation:
    from rquant.factor.time_series import MarketCapObservation

    return MarketCapObservation(
        stock_code=stock,
        trade_date=day,
        market_cap=value,
        first_visible_at=_at(day, minute=minute),
    )


def _run(
    expression: str,
    *,
    days: tuple[date, ...] = (_DAY,),
    stocks: tuple[str, ...] = _STOCKS,
    features: tuple[FeatureObservation, ...] = (),
    industries: tuple[IndustryObservation, ...] = (),
    sizes: tuple[MarketCapObservation, ...] = (),
) -> FactorTimeSeriesResult:
    definition = build_factor_definition(
        factor_id="neutralization",
        name_zh="中性化样本",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=days[0],
        expression=expression,
        feature_catalog=FeatureCatalog(columns=("close",)),
    )
    data = FactorTimeSeriesInput(
        definition=definition,
        universe=stocks,
        trading_days=days,
        decision_times=tuple(
            DecisionTime(trade_date=day, decision_at=_at(day, 15)) for day in days
        ),
        observations=features,
        industry_observations=industries,
        market_cap_observations=sizes,
    )
    return evaluate_factor_time_series(data)


def _point(result: FactorTimeSeriesResult, day: date, stock: str) -> FactorTimeSeriesValue:
    return next(row for row in result.values if row.trade_date == day and row.stock_code == stock)


def test_industry_neutralization_subtracts_same_day_group_mean_and_tracks_visibility() -> None:
    features = tuple(
        _feature(_DAY, stock, value) for stock, value in zip(_STOCKS, (1, 3, 10, 20), strict=True)
    )
    industries = (
        _industry(_DAY, "A", "Tech"),
        _industry(_DAY, "B", "Tech", minute=40),
        _industry(_DAY, "C", "Bank"),
        _industry(_DAY, "D", "Bank", minute=50),
    )
    result = _run("industry_neutralize(close)", features=features, industries=industries)

    assert [_point(result, _DAY, stock).value for stock in _STOCKS] == pytest.approx((-1, 1, -5, 5))
    assert _point(result, _DAY, "A").latest_visible_at == _at(_DAY, minute=40)
    assert _point(result, _DAY, "C").latest_visible_at == _at(_DAY, minute=50)


def test_industry_missing_context_and_single_peer_have_explicit_outcomes() -> None:
    result = _run(
        "industry_neutralize(close)",
        stocks=("A", "B", "C"),
        features=(_feature(_DAY, "A", 1), _feature(_DAY, "B", 3), _feature(_DAY, "C", None)),
        industries=(_industry(_DAY, "A", "Tech"), _industry(_DAY, "C", "Tech")),
    )
    assert _point(result, _DAY, "A").missing_reason == "insufficient_samples"
    assert _point(result, _DAY, "B").missing_reason == "missing_context"
    assert _point(result, _DAY, "C").missing_reason == "missing_value"


def test_size_neutralization_returns_log_cap_regression_residuals() -> None:
    features = tuple(
        _feature(_DAY, stock, value) for stock, value in zip(_STOCKS[:3], (1, 4, 5), strict=True)
    )
    sizes = tuple(
        _size(_DAY, stock, value, minute=index * 10)
        for index, (stock, value) in enumerate(
            zip(_STOCKS[:3], (1.0, math.e, math.e**2), strict=True)
        )
    )
    result = _run("size_neutralize(close)", stocks=_STOCKS[:3], features=features, sizes=sizes)

    assert [_point(result, _DAY, stock).value for stock in _STOCKS[:3]] == pytest.approx(
        (-1 / 3, 2 / 3, -1 / 3)
    )
    assert all(
        _point(result, _DAY, stock).latest_visible_at == _at(_DAY, minute=20)
        for stock in _STOCKS[:3]
    )


def test_size_neutralization_requires_three_samples_and_varying_cap() -> None:
    features = tuple(
        _feature(_DAY, stock, value) for stock, value in zip(_STOCKS[:3], (1, 2, 3), strict=True)
    )
    two = _run(
        "size_neutralize(close)",
        stocks=_STOCKS[:3],
        features=features,
        sizes=(_size(_DAY, "A", 1), _size(_DAY, "B", 2)),
    )
    assert _point(two, _DAY, "A").missing_reason == "insufficient_samples"
    assert _point(two, _DAY, "B").missing_reason == "insufficient_samples"
    assert _point(two, _DAY, "C").missing_reason == "missing_context"

    constant = _run(
        "size_neutralize(close)",
        stocks=_STOCKS[:3],
        features=features,
        sizes=tuple(_size(_DAY, stock, 10) for stock in _STOCKS[:3]),
    )
    assert all(
        _point(constant, _DAY, stock).missing_reason == "zero_variance" for stock in _STOCKS[:3]
    )


def test_context_validation_rejects_duplicate_future_naive_and_invalid_size() -> None:
    from rquant.factor.time_series import IndustryObservation, MarketCapObservation

    features = (_feature(_DAY, "A", 1),)
    good_industry = _industry(_DAY, "A", "Tech")
    good_size = _size(_DAY, "A", 1)
    cases = (
        ({"industries": (good_industry, good_industry)}, "factor_duplicate_industry"),
        (
            {
                "industries": (
                    IndustryObservation(
                        stock_code="A",
                        trade_date=_DAY,
                        industry="Tech",
                        first_visible_at=_at(_DAY, 16),
                    ),
                )
            },
            "factor_future_context",
        ),
        ({"sizes": (good_size, good_size)}, "factor_duplicate_market_cap"),
        (
            {
                "sizes": (
                    MarketCapObservation(
                        stock_code="A",
                        trade_date=_DAY,
                        market_cap=1,
                        first_visible_at=_at(_DAY, 16),
                    ),
                )
            },
            "factor_future_context",
        ),
    )
    for kwargs, expected in cases:
        with pytest.raises(ValidationError) as error:
            _run("close", stocks=("A",), features=features, **kwargs)
        assert error.value.errors()[0]["type"] == expected

    with pytest.raises(ValidationError):
        IndustryObservation(
            stock_code="A",
            trade_date=_DAY,
            industry="Tech",
            first_visible_at=datetime(2024, 1, 2, 14),
        )
    for value in (0, -1, math.nan, math.inf, -math.inf):
        with pytest.raises(ValidationError):
            _size(_DAY, "A", value)


def test_nested_neutralization_and_ref_use_context_of_each_evaluation_day() -> None:
    days = (_DAY, _NEXT)
    features = tuple(
        _feature(day, stock, value)
        for day, values in ((_DAY, (1, 3, 10)), (_NEXT, (10, 20, 30)))
        for stock, value in zip(_STOCKS[:3], values, strict=True)
    )
    industries = tuple(
        _industry(day, stock, group)
        for day, groups in ((_DAY, ("X", "X", "Y")), (_NEXT, ("X", "Y", "Y")))
        for stock, group in zip(_STOCKS[:3], groups, strict=True)
    )
    earlier = _run(
        "ref(industry_neutralize(close), 1)",
        days=days,
        stocks=_STOCKS[:3],
        features=features,
        industries=industries,
    )
    neutralized_prior = _run(
        "industry_neutralize(ref(close, 1))",
        days=days,
        stocks=_STOCKS[:3],
        features=features,
        industries=industries,
    )
    nested_rank = _run(
        "cs_rank(industry_neutralize(close))",
        days=days,
        stocks=_STOCKS[:3],
        features=features,
        industries=industries,
    )

    assert _point(earlier, _NEXT, "A").value == -1
    assert _point(earlier, _NEXT, "C").missing_reason == "insufficient_samples"
    assert _point(neutralized_prior, _NEXT, "A").missing_reason == "insufficient_samples"
    assert _point(neutralized_prior, _NEXT, "B").value == pytest.approx(-3.5)
    assert _point(nested_rank, _DAY, "A").value == 0.5
    assert _point(nested_rank, _DAY, "B").value == 1.0


def test_existing_plain_factor_input_needs_no_context() -> None:
    result = _run("close", stocks=("A",), features=(_feature(_DAY, "A", 2),))
    assert _point(result, _DAY, "A").value == 2


def test_nested_size_rank_is_independent_of_universe_and_row_order() -> None:
    stocks = _STOCKS[:3]
    features = tuple(
        _feature(_DAY, stock, value) for stock, value in zip(stocks, (1, 4, 5), strict=True)
    )
    sizes = tuple(
        _size(_DAY, stock, value)
        for stock, value in zip(stocks, (1.0, math.e, math.e**3), strict=True)
    )
    forward = _run("cs_rank(size_neutralize(close))", stocks=stocks, features=features, sizes=sizes)
    reversed_result = _run(
        "cs_rank(size_neutralize(close))",
        stocks=stocks[::-1],
        features=features[::-1],
        sizes=sizes[::-1],
    )
    for stock, rank in zip(stocks, (1 / 3, 1.0, 2 / 3), strict=True):
        assert _point(forward, _DAY, stock).value == pytest.approx(rank)
        assert _point(reversed_result, _DAY, stock).value == pytest.approx(rank)
