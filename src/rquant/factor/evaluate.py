"""Daily cross-sectional factor IC and equal-weight quantile returns."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from math import fsum, sqrt
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

_MARKET_TZ = timezone(timedelta(hours=8))
_GROUP_COUNTS = (3, 5, 10)

FiniteFloat = Annotated[float, Field(strict=True, allow_inf_nan=False)]
FactorDirection = Literal["higher_is_better", "lower_is_better"]
CorrelationStatus = Literal["ok", "insufficient_samples", "zero_variance"]
GroupingStatus = Literal["ok", "insufficient_samples"]


class FactorSample(BaseModel):
    """One stock's visible factor and completed forward return at a decision time."""

    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")

    stock_code: str
    decision_at: AwareDatetime
    factor_visible_at: AwareDatetime
    factor_value: FiniteFloat
    return_end_at: AwareDatetime
    forward_return: FiniteFloat

    @field_validator("stock_code")
    @classmethod
    def _nonempty_stock_code(cls, value: str) -> str:
        if not value or value.strip() != value:
            raise ValueError("stock_code must be nonempty and trimmed")
        return value

    @model_validator(mode="after")
    def _valid_time_range(self) -> FactorSample:
        if self.factor_visible_at > self.decision_at:
            raise ValueError("factor_visible_at must not follow decision_at")
        if self.return_end_at <= self.decision_at:
            raise ValueError("return_end_at must follow decision_at")
        return self


class FactorEvaluationInput(BaseModel):
    """Frozen universe, analysis clock, direction and daily samples."""

    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")

    universe: tuple[str, ...] = Field(min_length=1)
    as_of: AwareDatetime
    direction: FactorDirection
    samples: tuple[FactorSample, ...] = Field(min_length=1)

    @field_validator("universe")
    @classmethod
    def _valid_universe(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not code or code.strip() != code for code in value):
            raise ValueError("universe stock codes must be nonempty and trimmed")
        if len(set(value)) != len(value):
            raise ValueError("duplicate stock code in universe")
        return value

    @model_validator(mode="after")
    def _valid_samples(self) -> FactorEvaluationInput:
        universe = set(self.universe)
        seen: set[tuple[date, str]] = set()
        for sample in self.samples:
            if sample.stock_code not in universe:
                raise ValueError("sample stock_code is outside universe")
            if sample.return_end_at > self.as_of:
                raise ValueError("return_end_at must not follow as_of")
            key = (_market_date(sample.decision_at), sample.stock_code)
            if key in seen:
                raise ValueError("duplicate stock sample on a decision date")
            seen.add(key)
        return self


class CorrelationResult(BaseModel):
    """One cross-sectional correlation, including an explicit unavailable state."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: CorrelationStatus
    value: FiniteFloat | None
    source_sample_count: int
    effective_sample_count: int


class GroupReturn(BaseModel):
    """One equal-weight quantile portfolio's observed forward return."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    group_number: int
    member_count: int
    mean_forward_return: FiniteFloat


class GroupingResult(BaseModel):
    """All groups for one requested quantile count on one decision date."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    group_count: int
    status: GroupingStatus
    source_sample_count: int
    effective_sample_count: int
    groups: tuple[GroupReturn, ...]


class DailyFactorResult(BaseModel):
    """Independent cross-sectional evaluation for one market date."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    decision_date: date
    source_sample_count: int
    effective_sample_count: int
    normal_ic: CorrelationResult
    rank_ic: CorrelationResult
    groupings: tuple[GroupingResult, ...]


class FactorEvaluation(BaseModel):
    """Daily results sorted by decision date."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    days: tuple[DailyFactorResult, ...]


def _market_date(moment: datetime) -> date:
    return moment.astimezone(_MARKET_TZ).date()


def _mean(values: list[float]) -> float:
    count = len(values)
    return fsum(value / count for value in values)


def _pearson(left: list[float], right: list[float]) -> float | None:
    if len(set(left)) == 1 or len(set(right)) == 1:
        return None
    left_scale = max(abs(value) for value in left)
    right_scale = max(abs(value) for value in right)
    left_scaled = [value / left_scale for value in left]
    right_scaled = [value / right_scale for value in right]
    left_mean = _mean(left_scaled)
    right_mean = _mean(right_scaled)
    left_centered = [value - left_mean for value in left_scaled]
    right_centered = [value - right_mean for value in right_scaled]
    left_square = fsum(value * value for value in left_centered)
    right_square = fsum(value * value for value in right_centered)
    if left_square == 0 or right_square == 0:
        return None
    numerator = fsum(a * b for a, b in zip(left_centered, right_centered, strict=True))
    return max(-1.0, min(1.0, numerator / sqrt(left_square * right_square)))


def _average_ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: values[index])
    ranks = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        average_rank = (start + 1 + end) / 2
        for position in range(start, end):
            ranks[order[position]] = average_rank
        start = end
    return ranks


def _correlation_result(left: list[float], right: list[float]) -> CorrelationResult:
    count = len(left)
    if count < 2:
        return CorrelationResult(
            status="insufficient_samples",
            value=None,
            source_sample_count=count,
            effective_sample_count=count,
        )
    value = _pearson(left, right)
    return CorrelationResult(
        status="zero_variance" if value is None else "ok",
        value=value,
        source_sample_count=count,
        effective_sample_count=count,
    )


def _grouping_result(sorted_samples: list[FactorSample], group_count: int) -> GroupingResult:
    count = len(sorted_samples)
    if count < group_count:
        return GroupingResult(
            group_count=group_count,
            status="insufficient_samples",
            source_sample_count=count,
            effective_sample_count=count,
            groups=(),
        )
    base_size, extra = divmod(count, group_count)
    groups: list[GroupReturn] = []
    offset = 0
    for group_number in range(1, group_count + 1):
        size = base_size + (1 if group_number <= extra else 0)
        members = sorted_samples[offset : offset + size]
        groups.append(
            GroupReturn(
                group_number=group_number,
                member_count=size,
                mean_forward_return=_mean([sample.forward_return for sample in members]),
            )
        )
        offset += size
    return GroupingResult(
        group_count=group_count,
        status="ok",
        source_sample_count=count,
        effective_sample_count=count,
        groups=tuple(groups),
    )


def evaluate_factor(data: FactorEvaluationInput) -> FactorEvaluation:
    """Evaluate each A-share market date without reading prices or external state."""
    by_date: dict[date, list[FactorSample]] = {}
    for sample in data.samples:
        by_date.setdefault(_market_date(sample.decision_at), []).append(sample)

    sign = 1 if data.direction == "higher_is_better" else -1
    days: list[DailyFactorResult] = []
    for decision_date in sorted(by_date):
        samples = by_date[decision_date]
        factors = [sign * sample.factor_value for sample in samples]
        returns = [sample.forward_return for sample in samples]
        sorted_samples = sorted(
            samples,
            key=lambda sample: (sign * sample.factor_value, sample.stock_code),
        )
        count = len(samples)
        days.append(
            DailyFactorResult(
                decision_date=decision_date,
                source_sample_count=count,
                effective_sample_count=count,
                normal_ic=_correlation_result(factors, returns),
                rank_ic=_correlation_result(_average_ranks(factors), _average_ranks(returns)),
                groupings=tuple(
                    _grouping_result(sorted_samples, group_count) for group_count in _GROUP_COUNTS
                ),
            )
        )
    return FactorEvaluation(days=tuple(days))
