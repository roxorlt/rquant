"""Bounded IC decay over evaluation periods from complete daily statistics facts."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.factor.daily_stream import (
    FactorDailyStreamBatch,
    FactorDailyStreamRequest,
    FactorDailyStreamResult,
    factor_daily_stream_request_sha256,
)
from rquant.factor.decay import FactorICDecayDay, FactorICDecayPeriod
from rquant.factor.evaluate import (
    CorrelationResult,
    DailyFactorResult,
    FactorEvaluation,
    FactorEvaluationInput,
    FactorSample,
    evaluate_factor,
)
from rquant.factor.summary import summarize_factor_ic
from rquant.factor.time_series import MAX_TRADE_DAYS
from rquant.factor.universe import MAX_UNIVERSE_SECURITIES, Sha256, StockCode
from rquant.runtime_contracts import canonical_sha256

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_LAGS = 10


class FactorICDecayStreamRequest(BaseModel):
    model_config = _IMMUTABLE

    statistics_request: FactorDailyStreamRequest
    computation_stock_codes: tuple[StockCode, ...] = Field(
        min_length=1, max_length=MAX_UNIVERSE_SECURITIES
    )

    @field_validator("computation_stock_codes")
    @classmethod
    def _fixed_scope(cls, codes: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(codes)) != len(codes):
            raise ValueError("duplicate decay computation code")
        return tuple(sorted(codes))


def factor_ic_decay_stream_request_sha256(request: FactorICDecayStreamRequest) -> str:
    return canonical_sha256(FactorICDecayStreamRequest.model_validate(request))


def _summary_day(day: FactorICDecayDay) -> DailyFactorResult:
    return DailyFactorResult(
        decision_date=day.base_date,
        source_sample_count=day.valid_pair_count,
        effective_sample_count=day.valid_pair_count,
        normal_ic=day.normal_ic,
        rank_ic=day.rank_ic,
        groupings=(),
    )


class FactorICDecayStreamResult(BaseModel):
    """At most ten × 1,024 scalar IC diagnostics, bound to completed statistics."""

    model_config = _IMMUTABLE

    request: FactorICDecayStreamRequest
    request_sha256: Sha256
    statistics_request_sha256: Sha256
    statistics_input_sha256: Sha256
    batch_sha256s: tuple[Sha256, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    periods: tuple[FactorICDecayPeriod, ...] = Field(min_length=_LAGS, max_length=_LAGS)
    input_sha256: Sha256
    sha256: Sha256

    @field_validator("periods")
    @classmethod
    def _checked_periods(
        cls, periods: tuple[FactorICDecayPeriod, ...]
    ) -> tuple[FactorICDecayPeriod, ...]:
        return tuple(
            FactorICDecayPeriod.model_validate(period.model_dump(), strict=True)
            for period in periods
        )

    @model_validator(mode="after")
    def _completed_inputs(self) -> FactorICDecayStreamResult:
        days = self.request.statistics_request.evaluation_days
        if (
            self.request_sha256 != factor_ic_decay_stream_request_sha256(self.request)
            or self.statistics_request_sha256
            != factor_daily_stream_request_sha256(self.request.statistics_request)
            or len(self.batch_sha256s) != len(days)
            or self.statistics_input_sha256
            != canonical_sha256((self.statistics_request_sha256, self.batch_sha256s))
            or self.input_sha256
            != canonical_sha256((self.request_sha256, self.statistics_input_sha256))
        ):
            raise ValueError("decay completed input binding differs")
        for lag, period in enumerate(self.periods, start=1):
            targets = days[lag - 1 :]
            bases = days[: len(targets)]
            if (
                period.lag != lag
                or period.source_day_count != len(targets)
                or tuple((day.base_date, day.target_date) for day in period.days)
                != tuple(zip(bases, targets, strict=True))
                or period.valid_pair_count != sum(day.valid_pair_count for day in period.days)
                or any(
                    day.valid_pair_count > len(self.request.computation_stock_codes)
                    or any(
                        result.source_sample_count != day.valid_pair_count
                        or result.effective_sample_count != day.valid_pair_count
                        for result in (day.normal_ic, day.rank_ic)
                    )
                    for day in period.days
                )
            ):
                raise ValueError("decay period differs from evaluation schedule")
            expected_summary = (
                summarize_factor_ic(
                    FactorEvaluation(days=tuple(_summary_day(day) for day in period.days))
                )
                if targets
                else None
            )
            expected_status = (
                "no_target_period"
                if expected_summary is None
                else (
                    "no_valid_days"
                    if expected_summary.normal_ic.valid_day_count
                    == expected_summary.rank_ic.valid_day_count
                    == 0
                    else "evaluated"
                )
            )
            if period.ic_summary != expected_summary or period.status != expected_status:
                raise ValueError("decay period summary differs")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("decay result digest differs")
        return self


@dataclass(frozen=True, slots=True)
class _FactorFrame:
    trade_date: date
    decision_at: datetime
    factors: dict[str, tuple[float, datetime]]


class FactorICDecayStream:
    """Cache valid base factors independently of their same-period returns."""

    def __init__(self, request: FactorICDecayStreamRequest) -> None:
        self.request = FactorICDecayStreamRequest.model_validate(request)
        self._request_sha = factor_ic_decay_stream_request_sha256(self.request)
        self._statistics_sha = factor_daily_stream_request_sha256(self.request.statistics_request)
        self._scope = frozenset(self.request.computation_stock_codes)
        self._frames: deque[_FactorFrame] = deque(maxlen=_LAGS)
        self._days: list[list[FactorICDecayDay]] = [[] for _ in range(_LAGS)]
        self._hashes: list[str] = []
        self._previous_end: datetime | None = None
        self._closed = False
        self._completion: FactorICDecayStreamResult | None = None

    @property
    def cached_period_count(self) -> int:
        return len(self._frames)

    @property
    def cached_factor_count(self) -> int:
        return sum(len(frame.factors) for frame in self._frames)

    @property
    def completion(self) -> FactorICDecayStreamResult | None:
        return self._completion

    def close(self) -> None:
        self._closed = True
        self._frames.clear()
        self._days.clear()
        self._hashes.clear()
        self._previous_end = None

    def consume(self, batch: FactorDailyStreamBatch) -> None:
        if self._closed:
            raise ValueError("decay_stream_closed")
        try:
            batch = FactorDailyStreamBatch.model_validate(batch)
            request = self.request.statistics_request
            index = len(self._hashes)
            if (
                index >= len(request.evaluation_days)
                or batch.universe.trade_date != request.evaluation_days[index]
                or batch.request_sha256 != self._statistics_sha
                or batch.sources != request.sources
                or batch.universe.selection != request.selection
                or not set(batch.universe.stock_codes) <= self._scope
            ):
                raise ValueError("decay_batch_binding_mismatch")
            if (
                batch.decision_at > request.as_of
                or batch.universe.security_observed_at > request.as_of
                or (
                    batch.universe.index_observed_at is not None
                    and batch.universe.index_observed_at > request.as_of
                )
                or (self._previous_end is not None and self._previous_end > batch.decision_at)
                or any(
                    row.value is not None
                    and (
                        row.return_end_at > request.as_of or row.first_available_at > request.as_of
                    )
                    for row in batch.forward_returns
                )
            ):
                raise ValueError("decay_batch_time_mismatch")
            frame = _FactorFrame(
                trade_date=batch.universe.trade_date,
                decision_at=batch.decision_at,
                factors={
                    row.stock_code: (row.value, row.latest_visible_at or batch.decision_at)
                    for row in batch.factor_values
                    if row.value is not None
                },
            )
            self._frames.append(frame)
            returns = {
                row.stock_code: row.value for row in batch.forward_returns if row.value is not None
            }
            for lag, base in enumerate(reversed(self._frames), start=1):
                samples = tuple(
                    FactorSample(
                        stock_code=code,
                        decision_at=base.decision_at,
                        factor_visible_at=base.factors[code][1],
                        factor_value=base.factors[code][0],
                        return_end_at=batch.return_end_at,
                        forward_return=returns[code],
                    )
                    for code in sorted(base.factors.keys() & returns.keys())
                )
                if samples:
                    evaluated = evaluate_factor(
                        FactorEvaluationInput(
                            universe=self.request.computation_stock_codes,
                            as_of=request.as_of,
                            direction=request.definition.direction,
                            samples=samples,
                        )
                    ).days[0]
                    normal, rank = evaluated.normal_ic, evaluated.rank_ic
                else:
                    normal = rank = CorrelationResult(
                        status="insufficient_samples",
                        value=None,
                        source_sample_count=0,
                        effective_sample_count=0,
                    )
                self._days[lag - 1].append(
                    FactorICDecayDay(
                        base_date=base.trade_date,
                        target_date=batch.universe.trade_date,
                        valid_pair_count=len(samples),
                        normal_ic=normal,
                        rank_ic=rank,
                    )
                )
            self._hashes.append(canonical_sha256(batch))
            self._previous_end = batch.return_end_at
        except BaseException:
            self.close()
            raise

    def finish(self, statistics_result: FactorDailyStreamResult) -> FactorICDecayStreamResult:
        if self._closed:
            raise ValueError("decay_stream_closed")
        try:
            statistics = FactorDailyStreamResult.model_validate(statistics_result)
            hashes = tuple(self._hashes)
            if (
                statistics.request != self.request.statistics_request
                or statistics.request_sha256 != self._statistics_sha
                or len(hashes) != len(self.request.statistics_request.evaluation_days)
                or statistics.batch_sha256s != hashes
                or tuple(day.trade_date for day in statistics.days)
                != self.request.statistics_request.evaluation_days
                or statistics.input_sha256 != canonical_sha256((self._statistics_sha, hashes))
                or statistics.sha256 != canonical_sha256(statistics.model_dump(exclude={"sha256"}))
            ):
                raise ValueError("decay_statistics_completion_mismatch")
            periods: list[FactorICDecayPeriod] = []
            for lag, day_list in enumerate(self._days, start=1):
                days = tuple(day_list)
                summary = (
                    summarize_factor_ic(
                        FactorEvaluation(days=tuple(_summary_day(day) for day in days))
                    )
                    if days
                    else None
                )
                status = (
                    "no_target_period"
                    if summary is None
                    else (
                        "no_valid_days"
                        if summary.normal_ic.valid_day_count == summary.rank_ic.valid_day_count == 0
                        else "evaluated"
                    )
                )
                periods.append(
                    FactorICDecayPeriod(
                        lag=lag,
                        status=status,
                        source_day_count=len(days),
                        valid_pair_count=sum(day.valid_pair_count for day in days),
                        days=days,
                        ic_summary=summary,
                    )
                )
            fields = {
                "request": self.request,
                "request_sha256": self._request_sha,
                "statistics_request_sha256": self._statistics_sha,
                "statistics_input_sha256": statistics.input_sha256,
                "batch_sha256s": hashes,
                "periods": tuple(periods),
                "input_sha256": canonical_sha256((self._request_sha, statistics.input_sha256)),
            }
            self._completion = FactorICDecayStreamResult(**fields, sha256=canonical_sha256(fields))
            return self._completion
        finally:
            self.close()
