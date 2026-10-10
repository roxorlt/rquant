from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from rquant.portfolio import (
    ExposureInput,
    IndustryReturn,
    IndustryWeight,
    PortfolioAttributionError,
    attribute_brinson_fachler,
    calculate_industry_exposure,
)


def hand_calculated_input() -> ExposureInput:
    return ExposureInput(
        industries=(
            IndustryWeight(industry_l1="A", portfolio_weight="0.5", benchmark_weight="0.4"),
            IndustryWeight(industry_l1="B", portfolio_weight="0.3", benchmark_weight="0.6"),
        ),
        portfolio_cash_weight=Decimal("0.2"),
        benchmark_cash_weight=Decimal("0"),
    )


def hand_calculated_returns() -> tuple[IndustryReturn, ...]:
    return (
        IndustryReturn(industry_l1="A", portfolio_return="0.12", benchmark_return="0.10"),
        IndustryReturn(industry_l1="B", portfolio_return="-0.02", benchmark_return="0"),
    )


def test_exposure_lists_industry_deviation_and_cash_separately() -> None:
    result = calculate_industry_exposure(hand_calculated_input())

    assert [
        (row.kind, row.industry_l1, row.portfolio_weight, row.benchmark_weight, row.deviation)
        for row in result.rows
    ] == [
        ("industry", "A", Decimal("0.5"), Decimal("0.4"), Decimal("0.1")),
        ("industry", "B", Decimal("0.3"), Decimal("0.6"), Decimal("-0.3")),
        ("cash", None, Decimal("0.2"), Decimal("0"), Decimal("0.2")),
    ]


def test_brinson_fachler_hand_calculation_reconciles_with_cash() -> None:
    result = attribute_brinson_fachler(hand_calculated_input(), hand_calculated_returns())

    assert result.portfolio_return == Decimal("0.054")
    assert result.benchmark_return == Decimal("0.04")
    assert result.active_return == Decimal("0.014")
    assert [
        (row.kind, row.industry_l1, row.allocation, row.selection_and_interaction, row.total)
        for row in result.rows
    ] == [
        ("industry", "A", Decimal("0.006"), Decimal("0.010"), Decimal("0.016")),
        ("industry", "B", Decimal("0.012"), Decimal("-0.006"), Decimal("0.006")),
        ("cash", None, Decimal("-0.008"), Decimal("0"), Decimal("-0.008")),
    ]
    assert sum(row.total for row in result.rows) == result.active_return
    assert result.residual == 0


def test_input_order_does_not_change_exposure_or_attribution() -> None:
    original = hand_calculated_input()
    reversed_input = ExposureInput(
        industries=tuple(reversed(original.industries)),
        portfolio_cash_weight=original.portfolio_cash_weight,
        benchmark_cash_weight=original.benchmark_cash_weight,
    )
    assert calculate_industry_exposure(original) == calculate_industry_exposure(reversed_input)
    assert attribute_brinson_fachler(
        original, hand_calculated_returns()
    ) == attribute_brinson_fachler(reversed_input, tuple(reversed(hand_calculated_returns())))


def test_unknown_industry_is_explicit_in_exposure_and_blocks_attribution() -> None:
    spec = ExposureInput(
        industries=(
            IndustryWeight(industry_l1="A", portfolio_weight="0.5", benchmark_weight="1"),
            IndustryWeight(industry_l1=None, portfolio_weight="0.5", benchmark_weight="0"),
        ),
        portfolio_cash_weight=Decimal("0"),
        benchmark_cash_weight=Decimal("0"),
    )
    exposure = calculate_industry_exposure(spec)

    assert [(row.kind, row.industry_l1, row.portfolio_weight) for row in exposure.rows] == [
        ("industry", "A", Decimal("0.5")),
        ("unknown", None, Decimal("0.5")),
        ("cash", None, Decimal("0")),
    ]
    with pytest.raises(PortfolioAttributionError, match="未知行业"):
        attribute_brinson_fachler(
            spec, (IndustryReturn(industry_l1="A", portfolio_return="0.1", benchmark_return="0.1"),)
        )


def test_portfolio_only_industry_still_requires_benchmark_industry_return() -> None:
    spec = ExposureInput(
        industries=(
            IndustryWeight(industry_l1="A", portfolio_weight="1", benchmark_weight="0"),
            IndustryWeight(industry_l1="B", portfolio_weight="0", benchmark_weight="1"),
        ),
        portfolio_cash_weight=Decimal("0"),
        benchmark_cash_weight=Decimal("0"),
    )
    with pytest.raises(PortfolioAttributionError, match="基准.*A"):
        attribute_brinson_fachler(spec, (IndustryReturn(industry_l1="B", benchmark_return="0.05"),))


def test_portfolio_weight_requires_its_own_industry_return() -> None:
    spec = ExposureInput(
        industries=(IndustryWeight(industry_l1="A", portfolio_weight="1", benchmark_weight="1"),),
        portfolio_cash_weight=Decimal("0"),
        benchmark_cash_weight=Decimal("0"),
    )
    with pytest.raises(PortfolioAttributionError, match="组合.*A"):
        attribute_brinson_fachler(spec, (IndustryReturn(industry_l1="A", benchmark_return="0.05"),))


def test_cash_only_portfolio_and_benchmark_have_zero_active_return() -> None:
    spec = ExposureInput(
        industries=(),
        portfolio_cash_weight=Decimal("1"),
        benchmark_cash_weight=Decimal("1"),
    )
    result = attribute_brinson_fachler(spec, ())
    assert [(row.kind, row.total) for row in result.rows] == [("cash", Decimal("0"))]
    assert result.active_return == 0
    assert result.residual == 0


def test_duplicate_return_industry_is_rejected() -> None:
    with pytest.raises(PortfolioAttributionError, match="重复"):
        attribute_brinson_fachler(
            hand_calculated_input(),
            (
                hand_calculated_returns()[0],
                hand_calculated_returns()[0],
                hand_calculated_returns()[1],
            ),
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"industry_l1": "A", "portfolio_weight": Decimal("-0.1"), "benchmark_weight": Decimal("0")},
        {"industry_l1": "A", "portfolio_weight": Decimal("NaN"), "benchmark_weight": Decimal("0")},
        {"industry_l1": "A", "portfolio_weight": Decimal("1.1"), "benchmark_weight": Decimal("0")},
    ],
)
def test_invalid_industry_weight_is_rejected(kwargs: dict) -> None:
    with pytest.raises(ValidationError):
        IndustryWeight(**kwargs)


def test_weight_totals_must_include_cash_and_equal_one() -> None:
    with pytest.raises(ValidationError, match="权重"):
        ExposureInput(
            industries=(
                IndustryWeight(industry_l1="A", portfolio_weight="0.9", benchmark_weight="1"),
            ),
            portfolio_cash_weight=Decimal("0"),
            benchmark_cash_weight=Decimal("0"),
        )


def test_duplicate_industry_is_rejected_before_exposure() -> None:
    with pytest.raises(ValidationError, match="重复"):
        ExposureInput(
            industries=(
                IndustryWeight(industry_l1="A", portfolio_weight="0.5", benchmark_weight="0.5"),
                IndustryWeight(industry_l1="A", portfolio_weight="0.5", benchmark_weight="0.5"),
            ),
            portfolio_cash_weight=Decimal("0"),
            benchmark_cash_weight=Decimal("0"),
        )


def test_nonfinite_industry_return_is_rejected() -> None:
    with pytest.raises(ValidationError):
        IndustryReturn(industry_l1="A", portfolio_return=Decimal("Infinity"), benchmark_return="0")
