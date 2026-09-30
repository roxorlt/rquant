"""Whole-day factor statistics over trusted retrospective source bindings.

Only the current cross-section, prior target members, and bounded daily
diagnostics are retained. A supplied digest is not provider coverage proof.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from itertools import pairwise
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from rquant.factor.definition import FactorDefinition
from rquant.factor.evaluate import (
    _GROUP_COUNTS,
    DailyFactorResult,
    FactorEvaluation,
    FactorSample,
    FiniteFloat,
    GroupingStatus,
    _average_ranks,
    _correlation_result,
    _grouping_result,
    _partition_groups,
    _sorted_samples,
)
from rquant.factor.portfolio import _compound, _target_weight_change
from rquant.factor.result import (
    FactorForwardReturn,
    FactorMissingCount,
    HoldingSessions,
    ReturnMissingCount,
    ReturnPriceBasis,
)
from rquant.factor.summary import FactorICSummary, summarize_factor_ic
from rquant.factor.time_series import MAX_TRADE_DAYS, FactorTimeSeriesValue, MissingReason
from rquant.factor.universe import (
    MAX_UNIVERSE_SECURITIES,
    FactorUniverseResult,
    ObservedTime,
    Sha256,
    SourceId,
    UniverseSelection,
)
from rquant.runtime_contracts import canonical_sha256

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_MARKET_TZ = timezone(timedelta(hours=8))
StreamErrorReason = Literal[
    "missing_batch",
    "unexpected_batch",
    "date_order_mismatch",
    "request_binding_mismatch",
    "source_binding_mismatch",
    "selection_mismatch",
    "pool_observation_after_as_of",
    "decision_after_as_of",
    "return_not_available",
    "return_missing_reason_mismatch",
    "window_overlap",
]


class FactorDailyStreamError(ValueError):
    """Stable refusal of the whole final result."""

    def __init__(self, reason: StreamErrorReason) -> None:
        self.reason = reason
        super().__init__(reason)


class FactorDailyStreamSources(BaseModel):
    """Caller-declared identities of the pool, computed factors, and returns."""

    model_config = _IMMUTABLE

    source_mode: Literal["historical_retrospective"]
    universe_source_id: SourceId
    universe_source_sha256: Sha256
    factor_source_id: SourceId
    factor_source_sha256: Sha256
    return_source_id: SourceId
    return_source_sha256: Sha256


class FactorDailyStreamRequest(BaseModel):
    model_config = _IMMUTABLE

    definition: FactorDefinition
    selection: UniverseSelection
    evaluation_days: tuple[date, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    as_of: ObservedTime
    sources: FactorDailyStreamSources
    return_price_basis: ReturnPriceBasis
    holding_sessions: HoldingSessions

    @field_validator("evaluation_days")
    @classmethod
    def _ascending_schedule(cls, days: tuple[date, ...]) -> tuple[date, ...]:
        if any(left >= right for left, right in pairwise(days)):
            raise PydanticCustomError(
                "factor_daily_stream_invalid_schedule", "evaluation dates must ascend uniquely"
            )
        return days


class FactorDailyStreamBatch(BaseModel):
    model_config = _IMMUTABLE

    request_sha256: Sha256
    sources: FactorDailyStreamSources
    universe: FactorUniverseResult
    decision_at: ObservedTime
    return_end_at: ObservedTime
    factor_values: tuple[FactorTimeSeriesValue, ...] = Field(max_length=MAX_UNIVERSE_SECURITIES)
    forward_returns: tuple[FactorForwardReturn, ...] = Field(max_length=MAX_UNIVERSE_SECURITIES)

    @field_validator("universe")
    @classmethod
    def _checked_pool(cls, pool: FactorUniverseResult) -> FactorUniverseResult:
        if len(set(pool.stock_codes)) != len(pool.stock_codes):
            raise PydanticCustomError(
                "factor_daily_stream_duplicate_pool_stock", "pool contains duplicate stocks"
            )
        removed = sum(pool.excluded.model_dump().values())
        is_index = pool.selection in ("hs300", "zz1000")
        if (
            pool.selected_count != len(pool.stock_codes)
            or pool.input_count != pool.selected_count + removed
            or pool.input_count > pool.security_count
            or (not is_index and pool.input_count != pool.security_count)
            or (is_index and removed != 0)
        ):
            raise PydanticCustomError(
                "factor_daily_stream_invalid_pool_counts", "pool counts are inconsistent"
            )
        if is_index != (pool.index_observed_at is not None):
            raise PydanticCustomError(
                "factor_daily_stream_invalid_pool_index_observation",
                "index observation presence differs from selection",
            )
        moments = (pool.security_observed_at,)
        if pool.index_observed_at is not None:
            moments += (pool.index_observed_at,)
        if any(pool.trade_date > moment.astimezone(_MARKET_TZ).date() for moment in moments):
            raise PydanticCustomError(
                "factor_daily_stream_pool_date_after_observation",
                "pool date follows its Shanghai observation day",
            )
        return FactorUniverseResult.model_validate(
            {**pool.model_dump(mode="python"), "stock_codes": tuple(sorted(pool.stock_codes))}
        )

    @field_validator("factor_values")
    @classmethod
    def _checked_factors(
        cls, rows: tuple[FactorTimeSeriesValue, ...]
    ) -> tuple[FactorTimeSeriesValue, ...]:
        # The older value model does not revalidate copied instance fields.
        checked = tuple(FactorTimeSeriesValue.model_validate(row.model_dump()) for row in rows)
        if len({row.stock_code for row in checked}) != len(checked):
            raise PydanticCustomError(
                "factor_daily_stream_duplicate_factor_row", "duplicate factor stock row"
            )
        return tuple(sorted(checked, key=lambda row: row.stock_code))

    @field_validator("forward_returns")
    @classmethod
    def _checked_returns(
        cls, rows: tuple[FactorForwardReturn, ...]
    ) -> tuple[FactorForwardReturn, ...]:
        if len({row.stock_code for row in rows}) != len(rows):
            raise PydanticCustomError(
                "factor_daily_stream_duplicate_return_row", "duplicate return stock row"
            )
        return tuple(sorted(rows, key=lambda row: row.stock_code))

    @model_validator(mode="after")
    def _complete_day(self) -> FactorDailyStreamBatch:
        day = self.universe.trade_date
        if self.decision_at.astimezone(_MARKET_TZ).date() != day:
            raise PydanticCustomError(
                "factor_daily_stream_decision_date_mismatch",
                "decision instant differs from pool day",
            )
        if self.return_end_at <= self.decision_at:
            raise PydanticCustomError(
                "factor_daily_stream_invalid_return_window", "return end must follow decision"
            )
        codes = self.universe.stock_codes
        if tuple(row.stock_code for row in self.factor_values) != codes:
            raise PydanticCustomError(
                "factor_daily_stream_factor_grid_mismatch", "factor rows differ from complete pool"
            )
        if tuple(row.stock_code for row in self.forward_returns) != codes:
            raise PydanticCustomError(
                "factor_daily_stream_return_grid_mismatch", "return rows differ from complete pool"
            )
        for factor in self.factor_values:
            if factor.trade_date != day:
                raise PydanticCustomError(
                    "factor_daily_stream_factor_date_mismatch", "factor row differs from pool day"
                )
            visible = factor.latest_visible_at
            if visible is not None and (factor.value is None or visible > self.decision_at):
                raise PydanticCustomError(
                    "factor_daily_stream_factor_visibility_invalid", "factor visibility is invalid"
                )
        for row in self.forward_returns:
            if row.decision_date != day:
                raise PydanticCustomError(
                    "factor_daily_stream_return_date_mismatch", "return row differs from pool day"
                )
            if row.decision_at != self.decision_at or row.return_end_at != self.return_end_at:
                raise PydanticCustomError(
                    "factor_daily_stream_return_window_mismatch",
                    "return row differs from common window",
                )
        return self


class FactorDailyStreamCoverage(BaseModel):
    """Factor and return absence counts may overlap on the same stock."""

    model_config = _IMMUTABLE

    expected_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)
    valid_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)
    factor_missing_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)
    return_missing_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)
    factor_missing_by_reason: tuple[FactorMissingCount, ...]
    return_missing_by_reason: tuple[ReturnMissingCount, ...]


class FactorDailyStreamGroupPoint(BaseModel):
    model_config = _IMMUTABLE

    group_number: int = Field(ge=1, le=10)
    member_count: int = Field(ge=1, le=MAX_UNIVERSE_SECURITIES)
    period_return: FiniteFloat
    cumulative_return: FiniteFloat | None
    target_weight_turnover: FiniteFloat | None


class FactorDailyStreamGrouping(BaseModel):
    """Cumulative sleeve spread is a diagnostic, not a tradable net asset value."""

    model_config = _IMMUTABLE

    group_count: Literal[3, 5, 10]
    status: GroupingStatus
    cumulative_status: Literal["available", "gap"]
    source_sample_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)
    effective_sample_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)
    groups: tuple[FactorDailyStreamGroupPoint, ...] = Field(max_length=10)
    long_short_return: FiniteFloat | None
    long_short_cumulative_spread: FiniteFloat | None


class FactorDailyStreamDay(BaseModel):
    model_config = _IMMUTABLE

    trade_date: date
    decision_at: ObservedTime
    return_end_at: ObservedTime
    status: Literal["complete", "partial", "no_samples"]
    coverage: FactorDailyStreamCoverage
    evaluation: DailyFactorResult
    portfolio_groupings: tuple[FactorDailyStreamGrouping, ...] = Field(min_length=3, max_length=3)


class FactorDailyStreamResult(BaseModel):
    """Bounded diagnostics and ordered digests; no raw per-stock facts."""

    model_config = _IMMUTABLE

    request: FactorDailyStreamRequest
    request_sha256: Sha256
    batch_sha256s: tuple[Sha256, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    days: tuple[FactorDailyStreamDay, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    ic_summary: FactorICSummary
    input_sha256: Sha256
    sha256: Sha256


def factor_daily_stream_request_sha256(request: FactorDailyStreamRequest) -> str:
    return canonical_sha256(FactorDailyStreamRequest.model_validate(request))


def _check_bound_day(
    request: FactorDailyStreamRequest,
    request_sha: str,
    batch: FactorDailyStreamBatch,
    expected_day: date,
    previous_end: datetime | None,
) -> None:
    if batch.universe.trade_date != expected_day:
        raise FactorDailyStreamError("date_order_mismatch")
    if batch.request_sha256 != request_sha:
        raise FactorDailyStreamError("request_binding_mismatch")
    if batch.sources != request.sources:
        raise FactorDailyStreamError("source_binding_mismatch")
    if batch.universe.selection != request.selection:
        raise FactorDailyStreamError("selection_mismatch")
    if batch.decision_at > request.as_of:
        raise FactorDailyStreamError("decision_after_as_of")
    if batch.universe.security_observed_at > request.as_of or (
        batch.universe.index_observed_at is not None
        and batch.universe.index_observed_at > request.as_of
    ):
        raise FactorDailyStreamError("pool_observation_after_as_of")
    if previous_end is not None and previous_end > batch.decision_at:
        raise FactorDailyStreamError("window_overlap")
    for row in batch.forward_returns:
        if row.value is not None:
            if row.return_end_at > request.as_of or row.first_available_at > request.as_of:
                raise FactorDailyStreamError("return_not_available")
        elif (
            (row.return_end_at > request.as_of and row.missing_reason != "window_unfinished")
            or (row.return_end_at <= request.as_of and row.missing_reason == "window_unfinished")
            or (
                row.missing_reason == "visibility_pending"
                and (
                    row.expected_available_at is None or request.as_of >= row.expected_available_at
                )
            )
        ):
            raise FactorDailyStreamError("return_missing_reason_mismatch")


def _pair_day(
    batch: FactorDailyStreamBatch,
) -> tuple[list[FactorSample], FactorDailyStreamCoverage]:
    returned = {row.stock_code: row for row in batch.forward_returns}
    factor_reasons: Counter[MissingReason] = Counter()
    return_reasons = Counter(
        row.missing_reason for row in batch.forward_returns if row.missing_reason is not None
    )
    samples: list[FactorSample] = []
    for factor in batch.factor_values:
        if factor.missing_reason is not None:
            factor_reasons[factor.missing_reason] += 1
        row = returned[factor.stock_code]
        if factor.value is None or row.value is None:
            continue
        samples.append(
            FactorSample(
                stock_code=factor.stock_code,
                decision_at=batch.decision_at,
                factor_visible_at=factor.latest_visible_at or batch.decision_at,
                factor_value=factor.value,
                return_end_at=batch.return_end_at,
                forward_return=row.value,
            )
        )
    coverage = FactorDailyStreamCoverage(
        expected_count=batch.universe.selected_count,
        valid_count=len(samples),
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
    return samples, coverage


def _evaluate_day(
    request: FactorDailyStreamRequest,
    batch: FactorDailyStreamBatch,
    previous_members: dict[int, tuple[tuple[str, ...], ...]],
    group_gross: dict[int, tuple[float, ...]],
    gaps: set[int],
) -> FactorDailyStreamDay:
    samples, coverage = _pair_day(batch)
    sign = 1 if request.definition.direction == "higher_is_better" else -1
    factors = [sign * sample.factor_value for sample in samples]
    returns = [sample.forward_return for sample in samples]
    ordered = _sorted_samples(samples, request.definition.direction)
    evaluation = DailyFactorResult(
        decision_date=batch.universe.trade_date,
        source_sample_count=len(samples),
        effective_sample_count=len(samples),
        normal_ic=_correlation_result(factors, returns),
        rank_ic=_correlation_result(_average_ranks(factors), _average_ranks(returns)),
        groupings=tuple(_grouping_result(ordered, count) for count in _GROUP_COUNTS),
    )
    complete = coverage.valid_count == coverage.expected_count
    groupings: list[FactorDailyStreamGrouping] = []
    for count, grouping in zip(_GROUP_COUNTS, evaluation.groupings, strict=True):
        partition = _partition_groups(ordered, count)
        if not complete or partition is None:
            gaps.add(count)
            previous_members.pop(count, None)
            group_gross.pop(count, None)
        if partition is None:
            groupings.append(
                FactorDailyStreamGrouping(
                    group_count=count,
                    status="insufficient_samples",
                    cumulative_status="gap",
                    source_sample_count=len(samples),
                    effective_sample_count=len(samples),
                    groups=(),
                    long_short_return=None,
                    long_short_cumulative_spread=None,
                )
            )
            continue
        previous = previous_members.get(count)
        current_members = tuple(tuple(sample.stock_code for sample in part) for part in partition)
        prior_gross = group_gross.get(count, (1.0,) * count)
        current_gross: list[float] = []
        groups: list[FactorDailyStreamGroupPoint] = []
        for index, group in enumerate(grouping.groups):
            gross = (
                None if count in gaps else _compound(prior_gross[index], group.mean_forward_return)
            )
            if gross is not None:
                current_gross.append(gross)
            groups.append(
                FactorDailyStreamGroupPoint(
                    group_number=group.group_number,
                    member_count=group.member_count,
                    period_return=group.mean_forward_return,
                    cumulative_return=None if gross is None else gross - 1,
                    target_weight_turnover=None
                    if previous is None
                    else _target_weight_change(previous[index], current_members[index]),
                )
            )
        groupings.append(
            FactorDailyStreamGrouping(
                group_count=count,
                status="ok",
                cumulative_status="gap" if count in gaps else "available",
                source_sample_count=len(samples),
                effective_sample_count=len(samples),
                groups=tuple(groups),
                long_short_return=groups[-1].period_return - groups[0].period_return,
                long_short_cumulative_spread=(
                    None
                    if count in gaps
                    else groups[-1].cumulative_return - groups[0].cumulative_return
                ),
            )
        )
        if complete:
            previous_members[count] = current_members
        if count not in gaps:
            group_gross[count] = tuple(current_gross)
    return FactorDailyStreamDay(
        trade_date=batch.universe.trade_date,
        decision_at=batch.decision_at,
        return_end_at=batch.return_end_at,
        status="no_samples" if not samples else "complete" if complete else "partial",
        coverage=coverage,
        evaluation=evaluation,
        portfolio_groupings=tuple(groupings),
    )


def evaluate_factor_daily_stream(
    request: FactorDailyStreamRequest, batches: Iterable[FactorDailyStreamBatch]
) -> FactorDailyStreamResult:
    """Evaluate complete daily cross-sections once; return only final diagnostics."""
    request = FactorDailyStreamRequest.model_validate(request)
    request_sha = factor_daily_stream_request_sha256(request)
    previous_members: dict[int, tuple[tuple[str, ...], ...]] = {}
    group_gross: dict[int, tuple[float, ...]] = {}
    gaps: set[int] = set()
    days: list[FactorDailyStreamDay] = []
    hashes: list[str] = []
    iterator = iter(batches)
    previous_end: datetime | None = None
    for expected_day in request.evaluation_days:
        try:
            raw_batch = next(iterator)
        except StopIteration:
            raise FactorDailyStreamError("missing_batch") from None
        batch = FactorDailyStreamBatch.model_validate(raw_batch)
        _check_bound_day(request, request_sha, batch, expected_day, previous_end)
        hashes.append(canonical_sha256(batch))
        days.append(_evaluate_day(request, batch, previous_members, group_gross, gaps))
        previous_end = batch.return_end_at
        # Release the last yielded objects before advancing a one-shot producer.
        del batch, raw_batch
    try:
        next(iterator)
    except StopIteration:
        pass
    else:
        raise FactorDailyStreamError("unexpected_batch")
    ordered_hashes = tuple(hashes)
    fields = {
        "request": request,
        "request_sha256": request_sha,
        "batch_sha256s": ordered_hashes,
        "days": tuple(days),
        "ic_summary": summarize_factor_ic(
            FactorEvaluation(days=tuple(day.evaluation for day in days))
        ),
        "input_sha256": canonical_sha256((request_sha, ordered_hashes)),
    }
    return FactorDailyStreamResult(**fields, sha256=canonical_sha256(fields))
