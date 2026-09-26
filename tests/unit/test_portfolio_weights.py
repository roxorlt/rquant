from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from rquant.portfolio import (
    PortfolioAllocationError,
    PortfolioCandidate,
    PortfolioWeightRule,
    allocate_target_weights,
)


def candidate(code: str, score: str = "0", industry: str | None = None) -> PortfolioCandidate:
    return PortfolioCandidate(ts_code=code, rank_score=Decimal(score), industry_l1=industry)


def test_equal_weight_selects_stable_top_two_and_reserves_cash() -> None:
    result = allocate_target_weights(
        [candidate("000003.SZ", "3"), candidate("000002.SZ", "5"), candidate("000001.SZ", "5")],
        PortfolioWeightRule(method="equal", max_positions=2, cash_reserve=Decimal("0.10")),
        capital=Decimal("1000.00"),
    )

    assert [
        (item.ts_code, item.target_amount, item.exclusion_reason) for item in result.positions
    ] == [
        ("000001.SZ", Decimal("450.00"), None),
        ("000002.SZ", Decimal("450.00"), None),
        ("000003.SZ", Decimal("0.00"), "rank_limit"),
    ]
    assert result.invested_weight == Decimal("0.90")
    assert result.cash_weight == Decimal("0.10")
    assert result.cash_amount == Decimal("100.00")


def test_rank_score_weights_use_the_scores_and_ignore_input_order() -> None:
    ordered = [
        candidate("000001.SZ", "3"),
        candidate("000002.SZ", "2"),
        candidate("000003.SZ", "1"),
    ]
    rule = PortfolioWeightRule(method="rank_score", max_positions=3)

    forward = allocate_target_weights(ordered, rule, capital=Decimal("600.00"))
    backward = allocate_target_weights(list(reversed(ordered)), rule, capital=Decimal("600.00"))

    assert forward == backward
    assert [(item.ts_code, item.target_amount) for item in forward.positions] == [
        ("000001.SZ", Decimal("300.00")),
        ("000002.SZ", Decimal("200.00")),
        ("000003.SZ", Decimal("100.00")),
    ]


def test_stock_and_industry_caps_both_bind_with_proportional_redistribution() -> None:
    result = allocate_target_weights(
        [candidate("A", "6", "X"), candidate("B", "3", "X"), candidate("C", "1", "Y")],
        PortfolioWeightRule(
            method="rank_score",
            max_positions=3,
            max_stock_weight=Decimal("0.35"),
            max_industry_weight=Decimal("0.60"),
            cash_reserve=Decimal("0.10"),
        ),
        capital=Decimal("1000.00"),
    )

    assert [(item.ts_code, item.target_amount) for item in result.positions] == [
        ("A", Decimal("350.00")),
        ("B", Decimal("250.00")),
        ("C", Decimal("300.00")),
    ]
    assert result.cash_amount == Decimal("100.00")
    assert sum(item.target_weight for item in result.positions[:2]) == Decimal("0.60")


def test_capped_stock_surplus_flows_to_remaining_scores() -> None:
    result = allocate_target_weights(
        [candidate("A", "8"), candidate("B", "1"), candidate("C", "1")],
        PortfolioWeightRule(method="rank_score", max_positions=3, max_stock_weight=Decimal("0.50")),
        capital=Decimal("100.00"),
    )
    assert [item.target_amount for item in result.positions] == [
        Decimal("50.00"),
        Decimal("25.00"),
        Decimal("25.00"),
    ]


def test_minimum_amount_prunes_small_positions_and_does_not_restore_them() -> None:
    result = allocate_target_weights(
        [candidate("A", "6"), candidate("B", "3"), candidate("C", "1")],
        PortfolioWeightRule(
            method="rank_score", max_positions=3, min_target_amount=Decimal("20.00")
        ),
        capital=Decimal("100.00"),
    )

    assert [
        (item.ts_code, item.target_amount, item.exclusion_reason) for item in result.positions
    ] == [
        ("A", Decimal("66.67"), None),
        ("B", Decimal("33.33"), None),
        ("C", Decimal("0.00"), "below_minimum"),
    ]
    assert result.invested_amount == Decimal("100.00")


def test_cent_rounding_keeps_investment_and_constraints_exact() -> None:
    result = allocate_target_weights(
        [candidate("A"), candidate("B"), candidate("C")],
        PortfolioWeightRule(method="equal", max_positions=3, cash_reserve=Decimal("0.10")),
        capital=Decimal("100.00"),
    )
    assert [item.target_amount for item in result.positions] == [
        Decimal("30.00"),
        Decimal("30.00"),
        Decimal("30.00"),
    ]
    assert result.cash_amount == Decimal("10.00")
    assert sum(item.target_weight for item in result.positions) + result.cash_weight == 1


def test_cent_remainder_uses_stable_rank_tie_break() -> None:
    result = allocate_target_weights(
        [candidate("B"), candidate("A"), candidate("C")],
        PortfolioWeightRule(max_positions=3),
        capital=Decimal("100.00"),
    )
    assert [(item.ts_code, item.target_amount) for item in result.positions] == [
        ("A", Decimal("33.34")),
        ("B", Decimal("33.33")),
        ("C", Decimal("33.33")),
    ]


@pytest.mark.parametrize(
    ("rule", "match"),
    [
        (PortfolioWeightRule(max_positions=2, max_stock_weight=Decimal("0.40")), "单票"),
        (PortfolioWeightRule(max_positions=2, max_industry_weight=Decimal("0.60")), "行业"),
        (PortfolioWeightRule(max_positions=1, min_target_amount=Decimal("101.00")), "最小"),
    ],
)
def test_infeasible_constraints_raise_clear_domain_error(
    rule: PortfolioWeightRule, match: str
) -> None:
    with pytest.raises(PortfolioAllocationError, match=match):
        allocate_target_weights(
            [candidate("A", "2", "X"), candidate("B", "1", "X")],
            rule,
            capital=Decimal("100.00"),
        )


def test_equal_weight_minimum_drops_lower_ranked_tie_then_reallocates() -> None:
    result = allocate_target_weights(
        [candidate("B", "1", "X"), candidate("A", "2", "X")],
        PortfolioWeightRule(max_positions=2, min_target_amount=Decimal("60.00")),
        capital=Decimal("100.00"),
    )

    assert [
        (item.ts_code, item.target_amount, item.exclusion_reason) for item in result.positions
    ] == [
        ("A", Decimal("100.00"), None),
        ("B", Decimal("0.00"), "below_minimum"),
    ]


def test_high_precision_score_order_keeps_the_real_higher_score() -> None:
    lower = candidate("A", "1.0000000000000000000000000000")
    higher = candidate("B", "1.0000000000000000000000000001")
    rule = PortfolioWeightRule(method="rank_score", max_positions=1)

    forward = allocate_target_weights([lower, higher], rule, capital=Decimal("100.00"))
    backward = allocate_target_weights([higher, lower], rule, capital=Decimal("100.00"))

    assert forward == backward
    assert [(item.ts_code, item.target_amount) for item in forward.positions] == [
        ("B", Decimal("100.00")),
        ("A", Decimal("0.00")),
    ]


def test_unknown_industry_cannot_bypass_industry_cap() -> None:
    with pytest.raises(PortfolioAllocationError, match="行业"):
        allocate_target_weights(
            [candidate("A", "1"), candidate("B", "1", "X")],
            PortfolioWeightRule(max_positions=2, max_industry_weight=Decimal("0.60")),
            capital=Decimal("100.00"),
        )


def test_zero_investment_is_all_cash() -> None:
    result = allocate_target_weights(
        [candidate("A", "1")],
        PortfolioWeightRule(max_positions=1, cash_reserve=Decimal("1")),
        capital=Decimal("100.00"),
    )
    assert result.invested_amount == 0
    assert result.cash_weight == 1
    assert result.positions[0].status == "excluded"
    assert result.positions[0].exclusion_reason == "zero_weight"


def test_zero_capital_is_all_cash_without_division() -> None:
    result = allocate_target_weights(
        [candidate("A", "2")],
        PortfolioWeightRule(max_positions=1),
        capital=Decimal("0.00"),
    )
    assert result.invested_amount == Decimal("0.00")
    assert result.cash_amount == Decimal("0.00")
    assert result.cash_weight == 1


def test_all_zero_scores_reject_score_weighting() -> None:
    with pytest.raises(PortfolioAllocationError, match="排名分"):
        allocate_target_weights(
            [candidate("A"), candidate("B")],
            PortfolioWeightRule(method="rank_score", max_positions=2),
            capital=Decimal("100.00"),
        )


def test_positive_weight_target_below_one_cent_is_infeasible() -> None:
    with pytest.raises(PortfolioAllocationError, match="金额精确到分"):
        allocate_target_weights(
            [candidate("A")],
            PortfolioWeightRule(max_positions=1, cash_reserve=Decimal("0.50")),
            capital=Decimal("0.01"),
        )


def test_rounding_cannot_exceed_stock_cap() -> None:
    with pytest.raises(PortfolioAllocationError, match="金额精确到分"):
        allocate_target_weights(
            [candidate("A"), candidate("B")],
            PortfolioWeightRule(max_positions=2, max_stock_weight=Decimal("0.50")),
            capital=Decimal("0.01"),
        )


def test_zero_cent_position_is_reported_as_excluded() -> None:
    result = allocate_target_weights(
        [candidate("A"), candidate("B"), candidate("C")],
        PortfolioWeightRule(max_positions=3),
        capital=Decimal("0.02"),
    )
    assert [
        (item.ts_code, item.target_amount, item.exclusion_reason) for item in result.positions
    ] == [
        ("A", Decimal("0.01"), None),
        ("B", Decimal("0.01"), None),
        ("C", Decimal("0.00"), "zero_weight"),
    ]


def test_near_one_cash_reserve_retains_exact_fraction_before_cent_rounding() -> None:
    capital = Decimal("1000000000000000000000000000000.00")
    reserve = Decimal("0.999999999999999999999999999999")
    result = allocate_target_weights(
        [candidate("A")],
        PortfolioWeightRule(max_positions=1, cash_reserve=reserve),
        capital=capital,
    )
    assert result.positions[0].target_amount == Decimal("1.00")
    assert result.positions[0].status == "selected"
    assert result.invested_weight + result.cash_weight == 1


def test_duplicate_stock_codes_are_rejected() -> None:
    with pytest.raises(PortfolioAllocationError, match="重复"):
        allocate_target_weights(
            [candidate("A"), candidate("A")],
            PortfolioWeightRule(max_positions=2),
            capital=Decimal("100.00"),
        )


@pytest.mark.parametrize(
    ("model", "values"),
    [
        (PortfolioCandidate, {"ts_code": "A", "rank_score": Decimal("NaN")}),
        (PortfolioCandidate, {"ts_code": "A", "rank_score": Decimal("-1")}),
        (PortfolioWeightRule, {"max_positions": 1, "max_stock_weight": Decimal("Infinity")}),
        (PortfolioWeightRule, {"max_positions": 1, "max_stock_weight": Decimal("0")}),
        (PortfolioWeightRule, {"max_positions": 1, "cash_reserve": Decimal("1.1")}),
        (PortfolioWeightRule, {"max_positions": 1, "min_target_amount": Decimal("-1")}),
        (PortfolioWeightRule, {"max_positions": 1, "min_target_amount": Decimal("0.001")}),
        (PortfolioWeightRule, {"max_positions": True}),
    ],
)
def test_invalid_candidate_or_rule_is_rejected(model: type, values: dict) -> None:
    with pytest.raises(ValidationError):
        model(**values)


@pytest.mark.parametrize("capital", [Decimal("NaN"), Decimal("-1"), Decimal("10.001")])
def test_invalid_capital_is_rejected(capital: Decimal) -> None:
    with pytest.raises(PortfolioAllocationError, match="资金"):
        allocate_target_weights(
            [candidate("A")], PortfolioWeightRule(max_positions=1), capital=capital
        )
