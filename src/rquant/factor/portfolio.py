"""Pure quantile portfolio curves from complete, non-overlapping factor observations."""

from __future__ import annotations

from datetime import date, datetime
from math import fsum, isfinite

from pydantic import AwareDatetime, BaseModel, ConfigDict

from rquant.factor.evaluate import (
    _GROUP_COUNTS,
    FactorEvaluationInput,
    FactorSample,
    FiniteFloat,
    GroupingStatus,
    _market_date,
    _mean,
    _partition_groups,
    _sorted_samples,
)


class PortfolioGroupPoint(BaseModel):
    """One group's completed return, compounded return and target-weight change."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    group_number: int
    member_count: int
    period_return: FiniteFloat
    cumulative_return: FiniteFloat
    target_weight_turnover: FiniteFloat | None


class PortfolioGroupingDay(BaseModel):
    """One requested grouping count on one decision date."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    group_count: int
    status: GroupingStatus
    source_sample_count: int
    effective_sample_count: int
    groups: tuple[PortfolioGroupPoint, ...]
    long_short_return: FiniteFloat | None
    long_short_cumulative_return: FiniteFloat | None


class FactorPortfolioDay(BaseModel):
    """Completed common return window and its portfolio diagnostics."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    decision_date: date
    decision_at: AwareDatetime
    return_end_at: AwareDatetime
    source_sample_count: int
    effective_sample_count: int
    groupings: tuple[PortfolioGroupingDay, ...]


class FactorPortfolioDiagnostics(BaseModel):
    """Decision-day ordered quantile portfolio observations."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    days: tuple[FactorPortfolioDay, ...]


def _compound(gross_value: float, period_return: float) -> float:
    next_gross = gross_value * (1 + period_return)
    if not isfinite(next_gross):
        raise ValueError("non-finite cumulative return")
    return next_gross


def _target_weight_change(previous: tuple[str, ...], current: tuple[str, ...]) -> float:
    previous_weight = 1 / len(previous)
    current_weight = 1 / len(current)
    previous_members = set(previous)
    current_members = set(current)
    return 0.5 * fsum(
        abs(
            (current_weight if code in current_members else 0)
            - (previous_weight if code in previous_members else 0)
        )
        for code in sorted(previous_members | current_members)
    )


def _common_window(samples: list[FactorSample], universe: set[str]) -> tuple[datetime, datetime]:
    if len(samples) != len(universe) or {sample.stock_code for sample in samples} != universe:
        raise ValueError("each decision date must cover the complete universe")
    decision_at = samples[0].decision_at
    return_end_at = samples[0].return_end_at
    if any(sample.decision_at != decision_at for sample in samples):
        raise ValueError("all stocks on a date must have the same decision_at")
    if any(sample.return_end_at != return_end_at for sample in samples):
        raise ValueError("all stocks on a date must have the same return_end_at")
    if any(sample.forward_return < -1 for sample in samples):
        raise ValueError("stock forward return below -100% cannot be compounded")
    return decision_at, return_end_at


def evaluate_factor_portfolios(data: FactorEvaluationInput) -> FactorPortfolioDiagnostics:
    """Compound complete quantile returns and compare adjacent equal-weight targets."""
    data = FactorEvaluationInput.model_validate(data)
    universe = set(data.universe)
    by_date: dict[date, list[FactorSample]] = {}
    for sample in data.samples:
        by_date.setdefault(_market_date(sample.decision_at), []).append(sample)

    previous_end: datetime | None = None
    previous_members: dict[int, tuple[tuple[str, ...], ...]] = {}
    group_gross: dict[int, tuple[float, ...]] = {}
    long_short_gross: dict[int, float] = {}
    days: list[FactorPortfolioDay] = []

    for decision_date in sorted(by_date):
        samples = by_date[decision_date]
        decision_at, return_end_at = _common_window(samples, universe)
        if previous_end is not None and previous_end > decision_at:
            raise ValueError("consecutive return windows overlap")
        previous_end = return_end_at
        sorted_samples = _sorted_samples(samples, data.direction)
        groupings: list[PortfolioGroupingDay] = []
        for group_count in _GROUP_COUNTS:
            partition = _partition_groups(sorted_samples, group_count)
            if partition is None:
                groupings.append(
                    PortfolioGroupingDay(
                        group_count=group_count,
                        status="insufficient_samples",
                        source_sample_count=len(samples),
                        effective_sample_count=len(samples),
                        groups=(),
                        long_short_return=None,
                        long_short_cumulative_return=None,
                    )
                )
                continue

            previous = previous_members.get(group_count)
            prior_gross = group_gross.get(group_count, (1.0,) * group_count)
            current_members = tuple(
                tuple(sample.stock_code for sample in members) for members in partition
            )
            current_gross: list[float] = []
            groups: list[PortfolioGroupPoint] = []
            for index, members in enumerate(partition):
                period_return = _mean([sample.forward_return for sample in members])
                if not isfinite(period_return) or period_return < -1:
                    raise ValueError("non-finite or below -100% group return")
                gross = _compound(prior_gross[index], period_return)
                current_gross.append(gross)
                groups.append(
                    PortfolioGroupPoint(
                        group_number=index + 1,
                        member_count=len(members),
                        period_return=period_return,
                        cumulative_return=gross - 1,
                        target_weight_turnover=None
                        if previous is None
                        else _target_weight_change(previous[index], current_members[index]),
                    )
                )
            spread = groups[-1].period_return - groups[0].period_return
            if not isfinite(spread):
                raise ValueError("non-finite long-short return")
            if spread < -1:
                raise ValueError("long-short return below -100% cannot be compounded")
            spread_gross = _compound(long_short_gross.get(group_count, 1.0), spread)
            groupings.append(
                PortfolioGroupingDay(
                    group_count=group_count,
                    status="ok",
                    source_sample_count=len(samples),
                    effective_sample_count=len(samples),
                    groups=tuple(groups),
                    long_short_return=spread,
                    long_short_cumulative_return=spread_gross - 1,
                )
            )
            group_gross[group_count] = tuple(current_gross)
            long_short_gross[group_count] = spread_gross
            previous_members[group_count] = current_members
        days.append(
            FactorPortfolioDay(
                decision_date=decision_date,
                decision_at=decision_at,
                return_end_at=return_end_at,
                source_sample_count=len(samples),
                effective_sample_count=len(samples),
                groupings=tuple(groupings),
            )
        )
    return FactorPortfolioDiagnostics(days=tuple(days))
