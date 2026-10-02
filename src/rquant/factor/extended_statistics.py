"""Bounded diagnostics of the same processed factors, returns and daily labels."""

from __future__ import annotations

from collections import Counter
from datetime import date
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.factor.evaluate import (
    CorrelationResult,
    FactorSample,
    FiniteFloat,
    _average_ranks,
    _correlation_result,
)
from rquant.factor.neutralization_context import FactorNeutralizationSources
from rquant.factor.summary import ICSeriesSummary, _summarize_series

if TYPE_CHECKING:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")


class FactorExtendedStatisticsRequest(BaseModel):
    model_config = _IMMUTABLE
    ic_method: Literal["rank", "normal"]
    sources: FactorNeutralizationSources | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @model_validator(mode="after")
    def _industry(self) -> FactorExtendedStatisticsRequest:
        if self.sources is not None and self.sources.industry is None:
            raise ValueError("extended industry statistics require their industry binding")
        return self


class FactorIndustryMissingCount(BaseModel):
    model_config = _IMMUTABLE
    reason: Literal["missing", "ambiguous", "boundary_unverified"]
    count: int = Field(ge=1, le=7000)


class FactorIndustryCoverageDay(BaseModel):
    model_config = _IMMUTABLE
    trade_date: date
    panel_date: date
    expected_count: int = Field(ge=0, le=7000)
    valid_label_count: int = Field(ge=0, le=7000)
    paired_count: int = Field(ge=0, le=7000)
    missing_by_reason: tuple[FactorIndustryMissingCount, ...] = Field(max_length=3)

    @model_validator(mode="after")
    def _counts(self) -> FactorIndustryCoverageDay:
        if (
            self.panel_date >= self.trade_date
            or self.paired_count > self.valid_label_count
            or self.valid_label_count + sum(c.count for c in self.missing_by_reason)
            != self.expected_count
            or len({c.reason for c in self.missing_by_reason}) != len(self.missing_by_reason)
        ):
            raise ValueError("industry coverage grid or counts differ")
        return self


class FactorIndustryICSummary(BaseModel):
    model_config = _IMMUTABLE
    l1_code: str = Field(pattern=r"^\d{6}\.SI$")
    l1_name: str = Field(min_length=1, max_length=80)
    sample_count: int = Field(ge=0, le=7000 * 1024)
    ic_summary: ICSeriesSummary


class FactorRankAutocorrelationPoint(BaseModel):
    model_config = _IMMUTABLE
    trade_date: date
    previous_trade_date: date | None
    common_count: int = Field(ge=0, le=7000)
    status: Literal[
        "first_period", "no_common_members", "insufficient_samples", "zero_variance", "ok"
    ]
    value: FiniteFloat | None

    @model_validator(mode="after")
    def _value(self) -> FactorRankAutocorrelationPoint:
        if (
            (self.status == "first_period") != (self.previous_trade_date is None)
            or (
                self.previous_trade_date is not None and self.previous_trade_date >= self.trade_date
            )
            or (self.status == "ok") != (self.value is not None)
            or (self.value is not None and not -1 <= self.value <= 1)
        ):
            raise ValueError("autocorrelation status or dates differ")
        return self


class FactorExtendedStatistics(BaseModel):
    model_config = _IMMUTABLE
    ic_method: Literal["rank", "normal"]
    industry_status: Literal["available", "unavailable"]
    industry_reason: str | None = Field(default=None, max_length=80)
    industry_summaries: tuple[FactorIndustryICSummary, ...] = Field(max_length=31)
    industry_coverage_days: tuple[FactorIndustryCoverageDay, ...] = Field(max_length=1024)
    autocorrelation_points: tuple[FactorRankAutocorrelationPoint, ...] = Field(
        min_length=1, max_length=1024
    )

    @model_validator(mode="after")
    def _grid(self) -> FactorExtendedStatistics:
        days = tuple(p.trade_date for p in self.autocorrelation_points)
        if (
            tuple(sorted(set(days))) != days
            or tuple(p.previous_trade_date for p in self.autocorrelation_points)
            != (None, *days[:-1])
            or len({p.l1_code for p in self.industry_summaries}) != len(self.industry_summaries)
        ):
            raise ValueError("extended statistics date or industry grid differs")
        if self.industry_status == "unavailable":
            if not self.industry_reason or self.industry_summaries or self.industry_coverage_days:
                raise ValueError("unavailable industry statistics must retain only their reason")
        elif tuple(p.trade_date for p in self.industry_coverage_days) != days or any(
            p.ic_summary.source_day_count != len(days) for p in self.industry_summaries
        ):
            raise ValueError("industry statistics do not cover the exact evaluation dates")
        return self


class FactorExtendedStatisticsAccumulator:
    """One prior factor vector; all remaining retained facts are bounded scalars."""

    def __init__(self, request: FactorExtendedStatisticsRequest, direction: str) -> None:
        self.request = request
        self.sign = 1 if direction == "higher_is_better" else -1
        self.previous: dict[str, float] = {}
        self.previous_day: date | None = None
        self.points: list[FactorRankAutocorrelationPoint] = []
        self.coverage: list[FactorIndustryCoverageDay] = []
        self.series: dict[str, list[CorrelationResult]] = {}
        self.names: dict[str, str] = {}
        self.samples: Counter[str] = Counter()

    def consume(self, batch: FactorDailyStreamBatch, samples: list[FactorSample]) -> None:
        if len(self.points) >= 1024:
            raise ValueError("extended statistics exceed the evaluation budget")
        current = {v.stock_code: v.value for v in batch.factor_values if v.value is not None}
        common = sorted(current.keys() & self.previous.keys())
        if self.previous_day is None:
            status, value = "first_period", None
        elif not common:
            status, value = "no_common_members", None
        else:
            correlation = _correlation_result(
                _average_ranks([self.previous[c] for c in common]),
                _average_ranks([current[c] for c in common]),
            )
            status, value = correlation.status, correlation.value
        self.points.append(
            FactorRankAutocorrelationPoint(
                trade_date=batch.universe.trade_date,
                previous_trade_date=self.previous_day,
                common_count=len(common),
                status=status,
                value=value,
            )
        )
        self.previous, self.previous_day = current, batch.universe.trade_date
        if self.request.sources is None:
            return
        context = batch.context
        if context is None or context.industry_facts is None:
            raise ValueError("industry diagnostic batch lacks its original facts")
        codes = set(batch.universe.stock_codes)
        labels = {f.stock_code: f for f in context.industry_facts if f.stock_code in codes}
        missing = Counter(f.status for f in labels.values() if f.status != "valid")
        valid = {code: f for code, f in labels.items() if f.status == "valid"}
        grouped: dict[str, list[FactorSample]] = {}
        for fact in valid.values():
            if fact.l1_code in self.names and self.names[fact.l1_code] != fact.l1_name:
                raise ValueError("industry name differs within its original source")
            self.names[fact.l1_code] = fact.l1_name
            grouped.setdefault(fact.l1_code, [])
        if len(self.names) > 31:
            raise ValueError("industry statistics exceed the original directory budget")
        for sample in samples:
            if sample.stock_code in valid:
                grouped[valid[sample.stock_code].l1_code].append(sample)
        empty = _correlation_result([], [])
        for industry in self.names:
            series = self.series.setdefault(industry, [empty] * len(self.coverage))
            rows = grouped.get(industry, [])
            left = [self.sign * row.factor_value for row in rows]
            right = [row.forward_return for row in rows]
            if self.request.ic_method == "rank":
                left, right = _average_ranks(left), _average_ranks(right)
            series.append(_correlation_result(left, right))
            self.samples[industry] += len(rows)
        self.coverage.append(
            FactorIndustryCoverageDay(
                trade_date=batch.universe.trade_date,
                panel_date=context.panel_date,
                expected_count=len(codes),
                valid_label_count=len(valid),
                paired_count=sum(len(rows) for rows in grouped.values()),
                missing_by_reason=tuple(
                    FactorIndustryMissingCount(reason=reason, count=count)
                    for reason, count in sorted(missing.items())
                ),
            )
        )

    def finish(self) -> FactorExtendedStatistics:
        present = self.request.sources is not None
        return FactorExtendedStatistics(
            ic_method=self.request.ic_method,
            industry_status="available" if present else "unavailable",
            industry_reason=(
                "缺少可核验的行业来源，未生成行业 IC。"
                if not present
                else (
                    "缺少明确行业标签的股票未参与行业 IC。"
                    if any(c.missing_by_reason for c in self.coverage)
                    else None
                )
            ),
            industry_summaries=tuple(
                FactorIndustryICSummary(
                    l1_code=code,
                    l1_name=self.names[code],
                    sample_count=self.samples[code],
                    ic_summary=_summarize_series(tuple(self.series[code])),
                )
                for code in sorted(self.series)
            ),
            industry_coverage_days=tuple(self.coverage),
            autocorrelation_points=tuple(self.points),
        )

    def close(self) -> None:
        self.previous.clear()
        self.points.clear()
        self.coverage.clear()
        self.series.clear()
        self.names.clear()
        self.samples.clear()
