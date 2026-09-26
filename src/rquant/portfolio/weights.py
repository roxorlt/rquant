"""Deterministic target holdings, independent of accounts and execution state.

Caps refer to fractions of total capital, including the cash reserve. Continuous
allocation uses exact rational arithmetic. Money is rounded down to cents, then
remaining whole cents go to the largest fractional remainders if caps permit.
Reported weights are rounded down to at least 18 decimal places; cash is the
complement, so reported weights add to one without exceeding portfolio caps.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from decimal import ROUND_DOWN, Decimal, localcontext
from fractions import Fraction
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

_CENT = Decimal("0.01")
_ONE = Decimal("1")


class PortfolioAllocationError(ValueError):
    """The target allocation cannot satisfy the supplied constraints."""


class PortfolioCandidate(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    ts_code: str
    rank_score: Decimal = Field(default=Decimal("0"), ge=0, allow_inf_nan=False)
    industry_l1: str | None = None

    @field_validator("ts_code")
    @classmethod
    def normalize_code(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("股票代码不能为空")
        return normalized

    @field_validator("industry_l1")
    @classmethod
    def normalize_industry(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip() or None


class PortfolioWeightRule(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    method: Literal["equal", "rank_score"] = "equal"
    max_positions: int = Field(strict=True, gt=0)
    max_stock_weight: Decimal = Field(default=Decimal("1"), gt=0, le=1, allow_inf_nan=False)
    max_industry_weight: Decimal | None = Field(default=None, gt=0, le=1, allow_inf_nan=False)
    cash_reserve: Decimal = Field(default=Decimal("0"), ge=0, le=1, allow_inf_nan=False)
    min_target_amount: Decimal = Field(default=Decimal("0"), ge=0, allow_inf_nan=False)

    @field_validator("min_target_amount")
    @classmethod
    def require_whole_cents(cls, value: Decimal) -> Decimal:
        if (Fraction(value) * 100).denominator != 1:
            raise ValueError("最小目标金额须精确到分")
        return value


class TargetPosition(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    ts_code: str
    industry_l1: str | None
    rank_score: Decimal
    target_weight: Decimal
    target_amount: Decimal
    status: Literal["selected", "excluded"]
    exclusion_reason: Literal["rank_limit", "below_minimum", "zero_weight"] | None = None


class PortfolioTarget(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    positions: tuple[TargetPosition, ...]
    invested_weight: Decimal
    invested_amount: Decimal
    cash_weight: Decimal
    cash_amount: Decimal


def _validate_capital(capital: Decimal) -> int:
    if not isinstance(capital, Decimal) or not capital.is_finite() or capital < 0:
        raise PortfolioAllocationError("资金必须是有限的非负金额，精确到分")
    cents = Fraction(capital) * 100
    if cents.denominator != 1:
        raise PortfolioAllocationError("资金必须精确到分")
    return int(cents)


def _money_from_cents(cents: int) -> Decimal:
    with localcontext() as context:
        context.prec = len(str(abs(cents))) + 3
        return Decimal(cents).scaleb(-2).quantize(_CENT)


def _weight_from_cents(cents: int, total_cents: int) -> Decimal:
    if cents == 0 or total_cents == 0:
        return Decimal("0")
    places = max(18, len(str(total_cents)) + 6)
    with localcontext() as context:
        context.prec = places + len(str(total_cents)) + 10
        return (Decimal(cents) / Decimal(total_cents)).quantize(
            Decimal(1).scaleb(-places), rounding=ROUND_DOWN
        )


def _continuous_allocation(
    indexes: set[int],
    ranked: Sequence[PortfolioCandidate],
    rule: PortfolioWeightRule,
    target: Fraction,
) -> dict[int, Fraction]:
    scores = {
        index: Fraction(1 if rule.method == "equal" else ranked[index].rank_score)
        for index in indexes
    }
    active = {index for index in indexes if scores[index] > 0}
    stock_cap = Fraction(rule.max_stock_weight)
    industry_cap = (
        Fraction(rule.max_industry_weight) if rule.max_industry_weight is not None else None
    )
    allocated = {index: Fraction(0) for index in indexes}
    remaining = target

    if not active:
        raise PortfolioAllocationError("排名分全为零，无法达到投资目标")
    if len(active) * stock_cap < target:
        raise PortfolioAllocationError("单票上限不足以达到投资目标")
    if industry_cap is not None:
        counts: dict[str, int] = defaultdict(int)
        for index in active:
            counts[ranked[index].industry_l1 or ""] += 1
        if sum(min(count * stock_cap, industry_cap) for count in counts.values()) < target:
            raise PortfolioAllocationError("行业上限不足以达到投资目标")

    while remaining > 0:
        if not active:
            raise PortfolioAllocationError("候选可分配上限不足以达到投资目标")
        score_sum = sum((scores[index] for index in active), Fraction(0))
        alpha = remaining / score_sum
        for index in active:
            alpha = min(alpha, (stock_cap - allocated[index]) / scores[index])
        if industry_cap is not None:
            groups = {ranked[index].industry_l1 for index in active}
            for group in groups:
                group_score = sum(
                    (scores[index] for index in active if ranked[index].industry_l1 == group),
                    Fraction(0),
                )
                group_used = sum(
                    (
                        amount
                        for index, amount in allocated.items()
                        if ranked[index].industry_l1 == group
                    ),
                    Fraction(0),
                )
                alpha = min(alpha, (industry_cap - group_used) / group_score)
        if alpha <= 0:
            raise PortfolioAllocationError("单票或行业上限不足以达到投资目标")
        for index in active:
            allocated[index] += alpha * scores[index]
        remaining -= alpha * score_sum
        if remaining == 0:
            break
        active = {index for index in active if allocated[index] < stock_cap}
        if industry_cap is not None:
            group_used = defaultdict(Fraction)
            for index, amount in allocated.items():
                group_used[ranked[index].industry_l1] += amount
            active = {
                index for index in active if group_used[ranked[index].industry_l1] < industry_cap
            }
    return allocated


def _round_to_cents(
    weights: dict[int, Fraction],
    ranked: Sequence[PortfolioCandidate],
    rule: PortfolioWeightRule,
    total_cents: int,
    target_cents: int,
) -> dict[int, int]:
    raw = {index: weight * total_cents for index, weight in weights.items()}
    amounts = {index: int(amount) for index, amount in raw.items()}
    remaining = target_cents - sum(amounts.values())
    stock_limit = Fraction(rule.max_stock_weight) * total_cents
    industry_limit = (
        Fraction(rule.max_industry_weight) * total_cents
        if rule.max_industry_weight is not None
        else None
    )
    while remaining:
        order = sorted(raw, key=lambda index: (-(raw[index] - amounts[index]), index))
        for index in order:
            industry_used = sum(
                amount
                for other, amount in amounts.items()
                if ranked[other].industry_l1 == ranked[index].industry_l1
            )
            if amounts[index] + 1 > stock_limit or (
                industry_limit is not None and industry_used + 1 > industry_limit
            ):
                continue
            amounts[index] += 1
            remaining -= 1
            break
        else:
            raise PortfolioAllocationError("金额精确到分后，单票或行业上限无法达到投资目标")
    return amounts


def allocate_target_weights(
    candidates: Sequence[PortfolioCandidate],
    rule: PortfolioWeightRule,
    *,
    capital: Decimal,
) -> PortfolioTarget:
    """Return a deterministic top-N target or raise if the constraints are infeasible.

    Stocks below the minimum target amount are removed before reallocation and
    never re-enter. The minimum is a target-holding check, not an order-size rule.
    """
    total_cents = _validate_capital(capital)
    if not isinstance(rule, PortfolioWeightRule) or any(
        not isinstance(item, PortfolioCandidate) for item in candidates
    ):
        raise PortfolioAllocationError("候选与仓位规则必须使用已校验的数据模型")
    ranked = sorted(candidates, key=lambda item: item.ts_code)
    ranked.sort(key=lambda item: item.rank_score, reverse=True)
    if len({item.ts_code for item in ranked}) != len(ranked):
        raise PortfolioAllocationError("候选股票代码重复")

    top_count = min(len(ranked), rule.max_positions)
    reasons: dict[int, Literal["rank_limit", "below_minimum", "zero_weight"]] = {
        index: "rank_limit" for index in range(top_count, len(ranked))
    }
    target = Fraction(1) - Fraction(rule.cash_reserve)
    amounts = {index: 0 for index in range(top_count)}
    if total_cents and target:
        if not top_count:
            raise PortfolioAllocationError("没有候选股票，无法达到投资目标")
        if rule.max_industry_weight is not None and any(
            not ranked[index].industry_l1 for index in range(top_count)
        ):
            raise PortfolioAllocationError("启用行业上限时，入选候选缺少申万一级行业")
        eligible = set(range(top_count))
        if rule.method == "rank_score":
            for index in tuple(eligible):
                if ranked[index].rank_score == 0:
                    eligible.remove(index)
                    reasons[index] = "zero_weight"
        target_cents = int(target * total_cents)
        if target_cents == 0:
            raise PortfolioAllocationError("金额精确到分后，投资目标不足一分")
        minimum_cents = int(Fraction(rule.min_target_amount) * 100)
        while True:
            if not eligible:
                if any(reason == "below_minimum" for reason in reasons.values()):
                    raise PortfolioAllocationError("最小目标金额剔除后，没有可投资的候选")
                raise PortfolioAllocationError("排名分全为零，无法达到投资目标")
            try:
                weights = _continuous_allocation(eligible, ranked, rule, target)
                amounts = _round_to_cents(weights, ranked, rule, total_cents, target_cents)
            except PortfolioAllocationError as exc:
                if any(reason == "below_minimum" for reason in reasons.values()):
                    raise PortfolioAllocationError(f"最小目标金额剔除后，{exc}") from exc
                raise
            below_minimum = {index for index in eligible if amounts[index] < minimum_cents}
            if not below_minimum:
                break
            drop_index = min(below_minimum, key=lambda index: (amounts[index], -index))
            reasons[drop_index] = "below_minimum"
            eligible.remove(drop_index)
        for index in eligible:
            if amounts[index] == 0:
                reasons[index] = "zero_weight"
    else:
        for index in range(top_count):
            reasons[index] = "zero_weight"

    positions = tuple(
        TargetPosition(
            ts_code=item.ts_code,
            industry_l1=item.industry_l1,
            rank_score=item.rank_score,
            target_weight=_weight_from_cents(amounts.get(index, 0), total_cents),
            target_amount=_money_from_cents(amounts.get(index, 0)),
            status="excluded" if index in reasons else "selected",
            exclusion_reason=reasons.get(index),
        )
        for index, item in enumerate(ranked)
    )
    places = max(18, len(str(total_cents)) + 6)
    with localcontext() as context:
        context.prec = places + len(str(len(positions))) + 10
        invested_weight = sum((item.target_weight for item in positions), Decimal("0"))
        cash_weight = _ONE - invested_weight
    invested_cents = sum(amounts.values())
    return PortfolioTarget(
        positions=positions,
        invested_weight=invested_weight,
        invested_amount=_money_from_cents(invested_cents),
        cash_weight=cash_weight,
        cash_amount=_money_from_cents(total_cents - invested_cents),
    )
