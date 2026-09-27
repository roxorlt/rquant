"""Long-window IC summaries of already evaluated daily factor results."""

from __future__ import annotations

from math import exp, fsum, isfinite, lgamma, log, log1p, sqrt
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.factor.evaluate import CorrelationResult, FactorEvaluation, FiniteFloat

ICSummaryStatus = Literal[
    "ok", "no_valid_days", "insufficient_samples", "zero_variance", "precision_limit"
]


class ICSeriesSummary(BaseModel):
    """Statistics for one IC definition across an ordered set of decision dates."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: ICSummaryStatus
    source_day_count: int = Field(ge=0)
    valid_day_count: int = Field(ge=0)
    insufficient_day_count: int = Field(ge=0)
    zero_variance_day_count: int = Field(ge=0)
    mean: FiniteFloat | None
    sample_std: FiniteFloat | None
    ir: FiniteFloat | None
    positive_rate: FiniteFloat | None
    strong_signal_rate: FiniteFloat | None
    t_value: FiniteFloat | None
    p_value: FiniteFloat | None
    skewness: FiniteFloat | None
    excess_kurtosis: FiniteFloat | None


class FactorICSummary(BaseModel):
    """Separate NormalIC and RankIC long-window summaries."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    normal_ic: ICSeriesSummary
    rank_ic: ICSeriesSummary


def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    tiny = 1e-300
    c = 1.0
    d = 1.0 - (a + b) * x / (a + 1.0)
    if abs(d) < tiny:
        d = tiny
    d = 1.0 / d
    result = d
    for step in range(1, 201):
        doubled = 2 * step
        numerator = step * (b - step) * x / ((a - 1.0 + doubled) * (a + doubled))
        d = 1.0 + numerator * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + numerator / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        result *= d * c

        numerator = -(a + step) * (a + b + step) * x / ((a + doubled) * (a + 1.0 + doubled))
        d = 1.0 + numerator * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + numerator / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        result *= delta
        if abs(delta - 1.0) < 3e-14:
            return result
    raise ArithmeticError("Student t probability did not converge")


def _regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    coefficient = exp(lgamma(a + b) - lgamma(a) - lgamma(b) + a * log(x) + b * log1p(-x))
    if x < (a + 1.0) / (a + b + 2.0):
        return coefficient * _beta_continued_fraction(a, b, x) / a
    return 1.0 - coefficient * _beta_continued_fraction(b, a, 1.0 - x) / b


def _two_sided_student_p(t_value: float, degrees_of_freedom: int) -> float:
    if t_value == 0.0:
        return 1.0
    denominator = degrees_of_freedom + t_value * t_value
    if not isfinite(denominator):
        return 0.0
    x = degrees_of_freedom / denominator
    return min(1.0, max(0.0, _regularized_incomplete_beta(degrees_of_freedom / 2, 0.5, x)))


def _summarize_series(results: tuple[CorrelationResult, ...]) -> ICSeriesSummary:
    values: list[float] = []
    insufficient_day_count = 0
    zero_variance_day_count = 0
    for result in results:
        if result.status == "ok":
            if result.value is None or not isfinite(result.value):
                raise ValueError("ok daily IC must have a finite value")
            if not -1.0 <= result.value <= 1.0:
                raise ValueError("daily IC must be in the correlation range [-1, 1]")
            values.append(result.value)
        elif result.status == "insufficient_samples":
            if result.value is not None:
                raise ValueError("unavailable daily IC must not carry a value")
            insufficient_day_count += 1
        elif result.status == "zero_variance":
            if result.value is not None:
                raise ValueError("unavailable daily IC must not carry a value")
            zero_variance_day_count += 1
        else:
            raise ValueError("unknown daily IC status")

    count = len(values)
    common = {
        "source_day_count": len(results),
        "valid_day_count": count,
        "insufficient_day_count": insufficient_day_count,
        "zero_variance_day_count": zero_variance_day_count,
    }
    if count == 0:
        return ICSeriesSummary(
            status="no_valid_days",
            **common,
            mean=None,
            sample_std=None,
            ir=None,
            positive_rate=None,
            strong_signal_rate=None,
            t_value=None,
            p_value=None,
            skewness=None,
            excess_kurtosis=None,
        )

    positive_rate = sum(value > 0.0 for value in values) / count
    strong_signal_rate = sum(abs(value) > 0.02 for value in values) / count
    if count == 1:
        return ICSeriesSummary(
            status="insufficient_samples",
            **common,
            mean=values[0],
            sample_std=None,
            ir=None,
            positive_rate=positive_rate,
            strong_signal_rate=strong_signal_rate,
            t_value=None,
            p_value=None,
            skewness=None,
            excess_kurtosis=None,
        )

    raw_sum = fsum(values)
    raw_mean = raw_sum / count
    reference = values[0]
    offsets = [value - reference for value in values]
    offset_scale = max(abs(value) for value in offsets)
    if offset_scale == 0.0:
        return ICSeriesSummary(
            status="zero_variance",
            **common,
            mean=reference,
            sample_std=0.0,
            ir=None,
            positive_rate=positive_rate,
            strong_signal_rate=strong_signal_rate,
            t_value=None,
            p_value=None,
            skewness=None,
            excess_kurtosis=None,
        )

    scaled_offsets = [value / offset_scale for value in offsets]
    scaled_offset_mean = fsum(offsets) / offset_scale / count
    centered = [value - scaled_offset_mean for value in scaled_offsets]
    scale = max(abs(value) for value in centered)
    normalized = [value / scale for value in centered]
    normalized_sum_of_squares = fsum(value * value for value in normalized)
    scaled_sample_std = scale * sqrt(normalized_sum_of_squares / (count - 1))
    raw_sample_std = offset_scale * scaled_sample_std
    population_std = sqrt(normalized_sum_of_squares / count)
    standardized = [value / population_std for value in normalized]
    # Divide by the factor below one first so a subnormal sum is not lost prematurely.
    ratio = (
        (raw_sum / offset_scale) / scaled_sample_std
        if offset_scale <= 1.0
        else (raw_sum / scaled_sample_std) / offset_scale
    )
    ir_candidate = ratio / count
    t_candidate = ratio / sqrt(count)
    ir = None if raw_sum != 0.0 and ir_candidate == 0.0 else ir_candidate
    t_value = None if raw_sum != 0.0 and t_candidate == 0.0 else t_candidate
    p_candidate = _two_sided_student_p(t_value, count - 1) if t_value is not None else None
    p_value = None if t_value not in (None, 0.0) and p_candidate in (0.0, 1.0) else p_candidate
    mean = None if raw_sum != 0.0 and raw_mean == 0.0 else raw_mean
    sample_std = None if raw_sample_std == 0.0 else raw_sample_std
    return ICSeriesSummary(
        status=(
            "precision_limit"
            if any(value is None for value in (mean, sample_std, ir, t_value, p_value))
            else "ok"
        ),
        **common,
        mean=mean,
        sample_std=sample_std,
        ir=ir,
        positive_rate=positive_rate,
        strong_signal_rate=strong_signal_rate,
        t_value=t_value,
        p_value=p_value,
        skewness=fsum(value**3 for value in standardized) / count,
        excess_kurtosis=fsum(value**4 for value in standardized) / count - 3.0,
    )


def summarize_factor_ic(evaluation: FactorEvaluation) -> FactorICSummary:
    """Summarize one caller-aligned factor, universe and return window; no PIT checks here."""
    previous_date = None
    for day in evaluation.days:
        if previous_date is not None and day.decision_date <= previous_date:
            raise ValueError("decision_date must be unique and ascending")
        previous_date = day.decision_date
    return FactorICSummary(
        normal_ic=_summarize_series(tuple(day.normal_ic for day in evaluation.days)),
        rank_ic=_summarize_series(tuple(day.rank_ic for day in evaluation.days)),
    )
