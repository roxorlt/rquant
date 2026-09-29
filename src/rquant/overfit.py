"""Pure PSR and minimum track record length calculations for observed Sharpe ratios."""

from __future__ import annotations

from math import ceil, isfinite, sqrt
from statistics import NormalDist
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

FiniteFloat = Annotated[float, Field(strict=True, allow_inf_nan=False)]
_NORMAL = NormalDist()
_MIN_OBSERVATIONS = 30


class SinglePeriodSharpeInput(BaseModel):
    """Sharpe and moments at one frequency; convert annualized Sharpe before calling.

    The caller verifies that observations are independent, returns and costs share
    one basis, and no future information enters the estimate. These facts cannot
    be inferred from the summary statistics supplied here.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")

    observed_sharpe_per_period: FiniteFloat
    benchmark_sharpe_per_period: FiniteFloat
    skewness: FiniteFloat
    pearson_kurtosis: FiniteFloat
    independent_observations: int = Field(strict=True, ge=_MIN_OBSERVATIONS)

    @model_validator(mode="after")
    def _valid_moments_and_variance(self) -> SinglePeriodSharpeInput:
        # Pearson's moment inequality excludes impossible skew/kurtosis pairs.
        minimum_kurtosis = 1.0 + self.skewness * self.skewness
        if not isfinite(minimum_kurtosis) or self.pearson_kurtosis < minimum_kurtosis:
            raise ValueError("pearson_kurtosis must be at least 1 + skewness squared")
        variance = _estimation_variance_factor(self)
        if not isfinite(variance) or variance <= 0.0:
            raise ValueError("Sharpe estimation variance factor must be finite and positive")
        return self


class ProbabilisticSharpeResult(BaseModel):
    """Probability that the estimated Sharpe exceeds its specified benchmark."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inputs: SinglePeriodSharpeInput
    estimation_variance_factor: FiniteFloat = Field(gt=0.0)
    probability: FiniteFloat = Field(ge=0.0, le=1.0)


class MinimumTrackRecordLengthResult(BaseModel):
    """Formula estimate and conservative reportable count, or an explicit unavailable state."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inputs: SinglePeriodSharpeInput
    confidence: FiniteFloat = Field(gt=0.5, lt=1.0)
    status: Literal["reachable", "unreachable"]
    estimated_observations: FiniteFloat | None
    minimum_observations: int | None

    @model_validator(mode="after")
    def _consistent_status(self) -> MinimumTrackRecordLengthResult:
        if self.status == "unreachable":
            if self.estimated_observations is not None or self.minimum_observations is not None:
                raise ValueError("unreachable length cannot carry a finite observation count")
        elif self.estimated_observations is None or self.minimum_observations is None:
            raise ValueError("reachable length requires estimated and minimum observations")
        return self


def _estimation_variance_factor(inputs: SinglePeriodSharpeInput) -> float:
    sharpe = inputs.observed_sharpe_per_period
    return (
        1.0 - inputs.skewness * sharpe + (inputs.pearson_kurtosis - 1.0) * (sharpe * sharpe) / 4.0
    )


def probabilistic_sharpe_ratio_per_period(
    inputs: SinglePeriodSharpeInput,
) -> ProbabilisticSharpeResult:
    """Apply the Bailey–López de Prado PSR approximation to per-period Sharpe."""

    inputs = SinglePeriodSharpeInput.model_validate(inputs)
    variance = _estimation_variance_factor(inputs)
    delta = inputs.observed_sharpe_per_period - inputs.benchmark_sharpe_per_period
    z_score = delta * sqrt(inputs.independent_observations - 1) / sqrt(variance)
    probability = _NORMAL.cdf(z_score)
    return ProbabilisticSharpeResult(
        inputs=inputs,
        estimation_variance_factor=variance,
        probability=probability,
    )


def minimum_track_record_length_per_period(
    inputs: SinglePeriodSharpeInput,
    *,
    confidence: float,
) -> MinimumTrackRecordLengthResult:
    """Estimate independent periods needed to exceed a per-period Sharpe benchmark."""

    inputs = SinglePeriodSharpeInput.model_validate(inputs)
    if (
        isinstance(confidence, bool)
        or not isinstance(confidence, (int, float))
        or not isfinite(confidence)
        or not 0.5 < confidence < 1.0
    ):
        raise ValueError("confidence must be finite and strictly between 0.5 and 1")

    delta = inputs.observed_sharpe_per_period - inputs.benchmark_sharpe_per_period
    if delta <= 0.0:
        return MinimumTrackRecordLengthResult(
            inputs=inputs,
            confidence=confidence,
            status="unreachable",
            estimated_observations=None,
            minimum_observations=None,
        )

    variance = _estimation_variance_factor(inputs)
    ratio = _NORMAL.inv_cdf(confidence) / delta
    estimated = 1.0 + variance * ratio * ratio
    if not isfinite(estimated):
        raise ValueError("estimated observation count exceeds finite numeric range")
    return MinimumTrackRecordLengthResult(
        inputs=inputs,
        confidence=confidence,
        status="reachable",
        estimated_observations=estimated,
        minimum_observations=max(_MIN_OBSERVATIONS, ceil(estimated)),
    )
