"""Pure assembly of one frozen factor batch and its completed forward returns."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from itertools import pairwise
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.factor.definition import FactorDefinition
from rquant.factor.evaluate import (
    DailyFactorResult,
    FactorEvaluationInput,
    FactorSample,
    evaluate_factor,
)
from rquant.factor.portfolio import FactorPortfolioDiagnostics, evaluate_factor_portfolios
from rquant.factor.summary import FactorICSummary, summarize_factor_ic
from rquant.factor.time_series import (
    MAX_RESULT_POINTS,
    FactorTimeSeriesInput,
    FiniteValue,
    MissingReason,
    evaluate_factor_time_series,
)

_MARKET_TZ = timezone(timedelta(hours=8))
_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")

ReturnMissingReason = Literal[
    "window_unfinished", "missing_price", "suspended", "source_unavailable"
]
ReturnPriceBasis = Literal["raw", "forward_adjusted", "backward_adjusted"]
HoldingSessions = Literal[1, 5, 10, 20]
ResearchDayStatus = Literal["evaluated", "no_samples"]
ResearchSummaryStatus = Literal["evaluated", "no_samples"]
ResearchPortfolioStatus = Literal["available", "insufficient_data"]
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class FactorForwardReturn(BaseModel):
    """One requested stock/date's return fact or explicitly missing return."""

    model_config = _IMMUTABLE

    stock_code: str
    decision_date: date
    decision_at: AwareDatetime
    return_end_at: AwareDatetime
    value: FiniteValue | None
    missing_reason: ReturnMissingReason | None
    first_available_at: AwareDatetime | None

    @field_validator("stock_code")
    @classmethod
    def _valid_stock_code(cls, value: str) -> str:
        if not value or value.strip() != value or not value.isprintable():
            raise ValueError("stock_code must be nonempty, printable and trimmed")
        return value

    @model_validator(mode="after")
    def _valid_return(self) -> FactorForwardReturn:
        if self.decision_at.astimezone(_MARKET_TZ).date() != self.decision_date:
            raise ValueError("decision_date must match decision_at in market time")
        if self.return_end_at <= self.decision_at:
            raise ValueError("return_end_at must follow decision_at")
        if (self.value is None) == (self.missing_reason is None):
            raise ValueError("return needs exactly one of value or missing_reason")
        if self.value is None:
            if self.first_available_at is not None:
                raise ValueError("missing return must not have first_available_at")
        else:
            if self.value < -1:
                raise ValueError("return must not be below -100%")
            if self.first_available_at is None:
                raise ValueError("present return needs first_available_at")
            if self.first_available_at < self.return_end_at:
                raise ValueError("first_available_at must not precede return_end_at")
        return self


class FactorResearchRequest(BaseModel):
    """Aligned frozen feature and forward-return facts for one research run."""

    model_config = _IMMUTABLE

    factor_input: FactorTimeSeriesInput
    evaluation_days: tuple[date, ...] | None = None
    forward_returns: tuple[FactorForwardReturn, ...] = Field(
        min_length=1, max_length=MAX_RESULT_POINTS
    )
    as_of: AwareDatetime
    factor_source_id: str = Field(min_length=1, max_length=256)
    return_source_id: str = Field(min_length=1, max_length=256)
    return_price_basis: ReturnPriceBasis
    holding_sessions: HoldingSessions

    @model_validator(mode="before")
    @classmethod
    def _migrate_daily_frequency(cls, value: object) -> object:
        if not isinstance(value, dict) or "rebalance_frequency" not in value:
            return value
        if value["rebalance_frequency"] != "daily":
            raise ValueError("legacy rebalance_frequency cannot state exact holding_sessions")
        if "holding_sessions" in value:
            raise ValueError("specify holding_sessions or legacy rebalance_frequency, not both")
        return {
            **{key: item for key, item in value.items() if key != "rebalance_frequency"},
            "holding_sessions": 1,
        }

    @field_validator("factor_source_id", "return_source_id")
    @classmethod
    def _valid_source_id(cls, value: str) -> str:
        if value.strip() != value or not value.isprintable():
            raise ValueError("source identity must be printable and trimmed")
        return value

    @model_validator(mode="after")
    def _aligned_return_grid(self) -> FactorResearchRequest:
        evaluation_days = (
            self.evaluation_days
            if self.evaluation_days is not None
            else self.factor_input.trading_days
        )
        if not evaluation_days:
            raise ValueError("evaluation_days must be nonempty")
        if any(left >= right for left, right in pairwise(evaluation_days)):
            raise ValueError("evaluation_days must ascend without duplicates")
        evaluation_set = set(evaluation_days)
        if not evaluation_set.issubset(self.factor_input.trading_days):
            raise ValueError("evaluation_days must be a subset of the calculation calendar")
        if any(item.decision_at > self.as_of for item in self.factor_input.decision_times):
            raise ValueError("as_of must not precede a decision_at")
        universe = set(self.factor_input.universe)
        decision_by_date = {
            item.trade_date: item.decision_at
            for item in self.factor_input.decision_times
            if item.trade_date in evaluation_set
        }
        seen: set[tuple[date, str]] = set()
        end_by_date: dict[date, datetime] = {}
        for row in self.forward_returns:
            if row.stock_code not in universe or row.decision_date not in decision_by_date:
                raise ValueError("return stock or decision date is outside requested grid")
            key = (row.decision_date, row.stock_code)
            if key in seen:
                raise ValueError("duplicate forward return for a stock and decision date")
            seen.add(key)
            if row.decision_at != decision_by_date[row.decision_date]:
                raise ValueError("return decision_at differs from frozen factor decision_at")
            prior_end = end_by_date.setdefault(row.decision_date, row.return_end_at)
            if row.return_end_at != prior_end:
                raise ValueError("stocks on one date need the same return_end_at")
            if row.value is not None:
                if row.return_end_at > self.as_of or row.first_available_at > self.as_of:
                    raise ValueError("present return must be available by as_of")
            elif row.return_end_at > self.as_of:
                if row.missing_reason != "window_unfinished":
                    raise ValueError("unmatured return must use window_unfinished")
            elif row.missing_reason == "window_unfinished":
                raise ValueError("window_unfinished requires return_end_at after as_of")
        if len(seen) != len(universe) * len(decision_by_date):
            raise ValueError("forward returns must cover the complete requested grid")
        evaluation_decisions = tuple(
            item for item in self.factor_input.decision_times if item.trade_date in evaluation_set
        )
        for previous, current in pairwise(evaluation_decisions):
            if end_by_date[previous.trade_date] > current.decision_at:
                raise ValueError("consecutive forward-return windows overlap")
        return self


class FactorMissingCount(BaseModel):
    model_config = _IMMUTABLE

    reason: MissingReason
    count: int = Field(ge=1)


class ReturnMissingCount(BaseModel):
    model_config = _IMMUTABLE

    reason: ReturnMissingReason
    count: int = Field(ge=1)


class FactorDayCoverage(BaseModel):
    """Paired sample coverage; factor and return absences can overlap."""

    model_config = _IMMUTABLE

    expected_count: int = Field(ge=1)
    valid_count: int = Field(ge=0)
    factor_missing_count: int = Field(ge=0)
    return_missing_count: int = Field(ge=0)
    factor_missing_by_reason: tuple[FactorMissingCount, ...]
    return_missing_by_reason: tuple[ReturnMissingCount, ...]


class FactorResearchDay(BaseModel):
    """One requested date, including dates with no testable pairs."""

    model_config = _IMMUTABLE

    decision_date: date
    status: ResearchDayStatus
    coverage: FactorDayCoverage
    evaluation: DailyFactorResult | None


class FactorResearchResult(BaseModel):
    """Deterministic, immutable output for a single aligned offline request."""

    model_config = _IMMUTABLE

    definition: FactorDefinition
    factor_id: str
    factor_version: int
    factor_source_id: str
    return_source_id: str
    return_price_basis: ReturnPriceBasis
    holding_sessions: HoldingSessions
    universe: tuple[str, ...]
    trading_days: tuple[date, ...]
    as_of: AwareDatetime
    input_sha256: Sha256
    days: tuple[FactorResearchDay, ...]
    summary_status: ResearchSummaryStatus
    ic_summary: FactorICSummary | None
    portfolio_status: ResearchPortfolioStatus
    portfolio_diagnostics: FactorPortfolioDiagnostics | None
    sha256: Sha256


def _digest(value: BaseModel | dict[str, object]) -> str:
    payload = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def assemble_factor_research_result(request: FactorResearchRequest) -> FactorResearchResult:
    """Validate a complete return grid, then reuse the existing pure factor evaluators."""
    checked = FactorResearchRequest.model_validate(request)
    factor_values = evaluate_factor_time_series(checked.factor_input)
    evaluation_days = (
        checked.evaluation_days
        if checked.evaluation_days is not None
        else checked.factor_input.trading_days
    )
    points = {(point.trade_date, point.stock_code): point for point in factor_values.values}
    returns = {(row.decision_date, row.stock_code): row for row in checked.forward_returns}
    samples: list[FactorSample] = []
    coverages: list[FactorDayCoverage] = []
    evaluation_set = set(evaluation_days)
    for decision in checked.factor_input.decision_times:
        day = decision.trade_date
        if day not in evaluation_set:
            continue
        factor_reasons: Counter[MissingReason] = Counter()
        return_reasons: Counter[ReturnMissingReason] = Counter()
        valid_count = 0
        for stock in checked.factor_input.universe:
            point = points[(day, stock)]
            row = returns[(day, stock)]
            if point.missing_reason is not None:
                factor_reasons[point.missing_reason] += 1
            if row.missing_reason is not None:
                return_reasons[row.missing_reason] += 1
            if point.value is None or row.value is None:
                continue
            valid_count += 1
            samples.append(
                FactorSample(
                    stock_code=stock,
                    decision_at=decision.decision_at,
                    factor_visible_at=point.latest_visible_at or decision.decision_at,
                    factor_value=point.value,
                    return_end_at=row.return_end_at,
                    forward_return=row.value,
                )
            )
        coverages.append(
            FactorDayCoverage(
                expected_count=len(checked.factor_input.universe),
                valid_count=valid_count,
                factor_missing_count=sum(factor_reasons.values()),
                return_missing_count=sum(return_reasons.values()),
                factor_missing_by_reason=tuple(
                    FactorMissingCount(reason=reason, count=count)
                    for reason, count in sorted(factor_reasons.items())
                ),
                return_missing_by_reason=tuple(
                    ReturnMissingCount(reason=reason, count=count)
                    for reason, count in sorted(return_reasons.items())
                ),
            )
        )

    evaluation_input = (
        FactorEvaluationInput(
            universe=checked.factor_input.universe,
            as_of=checked.as_of,
            direction=checked.factor_input.definition.direction,
            samples=tuple(samples),
        )
        if samples
        else None
    )
    evaluation = evaluate_factor(evaluation_input) if evaluation_input is not None else None
    evaluations_by_date = (
        {day.decision_date: day for day in evaluation.days} if evaluation is not None else {}
    )
    days = tuple(
        FactorResearchDay(
            decision_date=day,
            status="evaluated" if coverage.valid_count else "no_samples",
            coverage=coverage,
            evaluation=evaluations_by_date.get(day),
        )
        for day, coverage in zip(evaluation_days, coverages, strict=True)
    )
    summary = summarize_factor_ic(evaluation) if evaluation is not None else None
    complete = all(coverage.valid_count == coverage.expected_count for coverage in coverages)
    portfolio = (
        evaluate_factor_portfolios(evaluation_input)
        if complete and evaluation_input is not None
        else None
    )
    fields: dict[str, object] = {
        "definition": checked.factor_input.definition,
        "factor_id": factor_values.factor_id,
        "factor_version": factor_values.version,
        "factor_source_id": checked.factor_source_id,
        "return_source_id": checked.return_source_id,
        "return_price_basis": checked.return_price_basis,
        "holding_sessions": checked.holding_sessions,
        "universe": checked.factor_input.universe,
        "trading_days": evaluation_days,
        "as_of": checked.as_of,
        "input_sha256": _digest(checked),
        "days": days,
        "summary_status": "evaluated" if evaluation is not None else "no_samples",
        "ic_summary": summary,
        "portfolio_status": "available" if portfolio is not None else "insufficient_data",
        "portfolio_diagnostics": portfolio,
    }
    serializable = FactorResearchResult.model_construct(**fields, sha256="0" * 64)
    content = serializable.model_dump(mode="json", exclude={"sha256"})
    return FactorResearchResult(**fields, sha256=_digest(content))
