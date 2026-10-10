"""Pure PSR and minimum track record length calculations for observed Sharpe ratios."""

from __future__ import annotations

from math import ceil, e, isfinite, sqrt
from statistics import NormalDist
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

FiniteFloat = Annotated[float, Field(strict=True, allow_inf_nan=False)]
_NORMAL = NormalDist()
_MIN_OBSERVATIONS = 30
_EULER_MASCHERONI = 0.5772156649015329


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


class DeflatedSharpeInput(BaseModel):
    """Single-period selected Sharpe and complete independent-trial family evidence.

    Convert annualized Sharpe before calling. The caller must verify the trial
    family's completeness and independence; a raw run count is not a substitute.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")

    selected_strategy: SinglePeriodSharpeInput
    independent_trial_count: int = Field(strict=True, ge=1)
    family_sharpe_std_per_period: FiniteFloat = Field(ge=0.0)

    @model_validator(mode="after")
    def _zero_reference_baseline(self) -> DeflatedSharpeInput:
        if self.selected_strategy.benchmark_sharpe_per_period != 0.0:
            raise ValueError("selected_strategy benchmark must be zero for DSR")
        return self


class DeflatedSharpeResult(BaseModel):
    """Probability above the expected maximum noise Sharpe for an independent family."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inputs: DeflatedSharpeInput
    expected_max_noise_sharpe_per_period: FiniteFloat = Field(ge=0.0)
    probability: FiniteFloat = Field(ge=0.0, le=1.0)


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


def deflated_sharpe_ratio_per_period(inputs: DeflatedSharpeInput) -> DeflatedSharpeResult:
    """Deflate selected per-period Sharpe against the family's expected noise maximum."""

    inputs = DeflatedSharpeInput.model_validate(inputs)
    count = inputs.independent_trial_count
    dispersion = inputs.family_sharpe_std_per_period
    if count == 1 or dispersion == 0.0:
        threshold = 0.0
    else:
        try:
            first_probability = 1.0 - 1.0 / count
            second_probability = 1.0 - 1.0 / (count * e)
            if first_probability >= 1.0 or second_probability >= 1.0:
                raise ValueError("independent trial count exceeds quantile precision")
            expected_max_standard_normal = (1.0 - _EULER_MASCHERONI) * _NORMAL.inv_cdf(
                first_probability
            ) + _EULER_MASCHERONI * _NORMAL.inv_cdf(second_probability)
            threshold = dispersion * expected_max_standard_normal
        except OverflowError as exc:
            raise ValueError("expected noise Sharpe exceeds finite numeric range") from exc
        if not isfinite(threshold):
            raise ValueError("expected noise Sharpe exceeds finite numeric range")

    adjusted = inputs.selected_strategy.model_copy(
        update={"benchmark_sharpe_per_period": threshold}
    )
    psr = probabilistic_sharpe_ratio_per_period(adjusted)
    return DeflatedSharpeResult(
        inputs=inputs,
        expected_max_noise_sharpe_per_period=threshold,
        probability=psr.probability,
    )
