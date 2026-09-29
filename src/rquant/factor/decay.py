"""Pure IC decay across a frozen sequence of factor evaluation dates."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from rquant.factor.evaluate import (
    CorrelationResult,
    DailyFactorResult,
    FactorEvaluation,
    FactorEvaluationInput,
    FactorSample,
    evaluate_factor,
)
from rquant.factor.summary import FactorICSummary, summarize_factor_ic
from rquant.factor.time_series import evaluate_factor_time_series

if TYPE_CHECKING:
    from rquant.factor.result import FactorResearchRequest

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
DecayPeriodStatus = Literal["evaluated", "no_valid_days", "no_target_period"]


class FactorICDecayDay(BaseModel):
    """One base-date factor cross-section against a target-date return window."""

    model_config = _IMMUTABLE

    base_date: date
    target_date: date
    valid_pair_count: int = Field(ge=0)
    normal_ic: CorrelationResult
    rank_ic: CorrelationResult


class FactorICDecayPeriod(BaseModel):
    """One evaluation-sequence lag, including unavailable observations."""

    model_config = _IMMUTABLE

    lag: int = Field(ge=1, le=10)
    status: DecayPeriodStatus
    source_day_count: int = Field(ge=0)
    valid_pair_count: int = Field(ge=0)
    days: tuple[FactorICDecayDay, ...]
    ic_summary: FactorICSummary | None


class FactorICDecayResult(BaseModel):
    """Bounded, deterministic decay result for one validated research request."""

    model_config = _IMMUTABLE

    factor_id: str
    factor_version: int
    factor_source_id: str
    return_source_id: str
    return_price_basis: Literal["raw", "forward_adjusted", "backward_adjusted"]
    holding_sessions: Literal[1, 5, 10, 20]
    universe: tuple[str, ...]
    evaluation_days: tuple[date, ...]
    as_of: AwareDatetime
    input_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    periods: tuple[FactorICDecayPeriod, ...] = Field(min_length=10, max_length=10)


def _request_identity(request: FactorResearchRequest) -> str:
    from rquant.factor.result import _digest

    factor_input = request.factor_input
    ordered_input = factor_input.model_copy(
        update={
            "observations": tuple(
                sorted(
                    factor_input.observations,
                    key=lambda row: (row.trade_date, row.stock_code, row.column),
                )
            ),
            "industry_observations": tuple(
                sorted(
                    factor_input.industry_observations,
                    key=lambda row: (row.trade_date, row.stock_code),
                )
            ),
            "market_cap_observations": tuple(
                sorted(
                    factor_input.market_cap_observations,
                    key=lambda row: (row.trade_date, row.stock_code),
                )
            ),
        }
    )
    ordered_request = request.model_copy(
        update={
            "factor_input": ordered_input,
            "forward_returns": tuple(
                sorted(request.forward_returns, key=lambda row: (row.decision_date, row.stock_code))
            ),
        }
    )
    return _digest(ordered_request)


def _empty_daily_result(base_date: date) -> DailyFactorResult:
    missing = CorrelationResult(
        status="insufficient_samples",
        value=None,
        source_sample_count=0,
        effective_sample_count=0,
    )
    return DailyFactorResult(
        decision_date=base_date,
        source_sample_count=0,
        effective_sample_count=0,
        normal_ic=missing,
        rank_ic=missing,
        groupings=(),
    )


def evaluate_factor_ic_decay(request: FactorResearchRequest) -> FactorICDecayResult:
    """Pair base factors with returns k-1 evaluation dates later, for k=1..10."""
    from rquant.factor.result import FactorResearchRequest

    checked = FactorResearchRequest.model_validate(request)
    factor_values = evaluate_factor_time_series(checked.factor_input)
    evaluation_days = (
        checked.evaluation_days
        if checked.evaluation_days is not None
        else checked.factor_input.trading_days
    )
    decisions = {item.trade_date: item.decision_at for item in checked.factor_input.decision_times}
    points = {(point.trade_date, point.stock_code): point for point in factor_values.values}
    returns = {(row.decision_date, row.stock_code): row for row in checked.forward_returns}
    periods: list[FactorICDecayPeriod] = []

    for lag in range(1, 11):
        target_days = evaluation_days[lag - 1 :]
        if not target_days:
            periods.append(
                FactorICDecayPeriod(
                    lag=lag,
                    status="no_target_period",
                    source_day_count=0,
                    valid_pair_count=0,
                    days=(),
                    ic_summary=None,
                )
            )
            continue

        base_days = evaluation_days[: len(target_days)]
        samples: list[FactorSample] = []
        paired_counts: dict[date, int] = {}
        for base_date, target_date in zip(base_days, target_days, strict=True):
            count = 0
            for stock_code in sorted(checked.factor_input.universe):
                point = points[(base_date, stock_code)]
                row = returns[(target_date, stock_code)]
                if point.value is None or row.value is None:
                    continue
                count += 1
                samples.append(
                    FactorSample(
                        stock_code=stock_code,
                        decision_at=decisions[base_date],
                        factor_visible_at=point.latest_visible_at or decisions[base_date],
                        factor_value=point.value,
                        return_end_at=row.return_end_at,
                        forward_return=row.value,
                    )
                )
            paired_counts[base_date] = count

        evaluated = (
            evaluate_factor(
                FactorEvaluationInput(
                    universe=checked.factor_input.universe,
                    as_of=checked.as_of,
                    direction=checked.factor_input.definition.direction,
                    samples=tuple(samples),
                )
            )
            if samples
            else FactorEvaluation(days=())
        )
        evaluated_by_date = {day.decision_date: day for day in evaluated.days}
        aligned_days = tuple(
            evaluated_by_date[base_date]
            if base_date in evaluated_by_date
            else _empty_daily_result(base_date)
            for base_date in base_days
        )
        summary = summarize_factor_ic(FactorEvaluation(days=aligned_days))
        days = tuple(
            FactorICDecayDay(
                base_date=base_date,
                target_date=target_date,
                valid_pair_count=paired_counts[base_date],
                normal_ic=day.normal_ic,
                rank_ic=day.rank_ic,
            )
            for base_date, target_date, day in zip(
                base_days, target_days, aligned_days, strict=True
            )
        )
        periods.append(
            FactorICDecayPeriod(
                lag=lag,
                status=(
                    "no_valid_days"
                    if summary.normal_ic.valid_day_count == summary.rank_ic.valid_day_count == 0
                    else "evaluated"
                ),
                source_day_count=len(base_days),
                valid_pair_count=sum(paired_counts.values()),
                days=days,
                ic_summary=summary,
            )
        )

    return FactorICDecayResult(
        factor_id=factor_values.factor_id,
        factor_version=factor_values.version,
        factor_source_id=checked.factor_source_id,
        return_source_id=checked.return_source_id,
        return_price_basis=checked.return_price_basis,
        holding_sessions=checked.holding_sessions,
        universe=checked.factor_input.universe,
        evaluation_days=evaluation_days,
        as_of=checked.as_of,
        input_sha256=_request_identity(checked),
        periods=tuple(periods),
    )
