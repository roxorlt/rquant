"""Offline combinatorially symmetric cross-validation for backtest overfitting."""

from __future__ import annotations

from datetime import date
from itertools import combinations
from math import fsum, isfinite, log, sqrt
from statistics import fmean
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

FiniteFloat = Annotated[float, Field(strict=True, allow_inf_nan=False)]
CandidateId = Annotated[str, Field(strict=True, min_length=1, max_length=128)]
StrictDate = Annotated[date, Field(strict=True)]
_MAX_CANDIDATES = 64
_MAX_OBSERVATIONS = 4_096
_MAX_RETURN_VALUES = 262_144
_MIN_HALF_OBSERVATIONS = 30


class CSCVInput(BaseModel):
    """Aligned single-period returns for a complete candidate family."""

    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")

    candidate_ids: tuple[CandidateId, ...] = Field(min_length=2, max_length=_MAX_CANDIDATES)
    period_end_dates: tuple[StrictDate, ...] = Field(max_length=_MAX_OBSERVATIONS)
    returns_by_observation: tuple[tuple[FiniteFloat, ...], ...] = Field(
        max_length=_MAX_OBSERVATIONS
    )
    slice_count: int = Field(strict=True)

    @model_validator(mode="after")
    def _validate_family_shape(self) -> CSCVInput:
        if len(set(self.candidate_ids)) != len(self.candidate_ids) or any(
            not candidate_id.strip() for candidate_id in self.candidate_ids
        ):
            raise ValueError("candidate IDs must be nonblank and unique")
        if len(self.period_end_dates) != len(self.returns_by_observation):
            raise ValueError("dates and return rows must cover the same observations")
        if any(
            current <= previous
            for previous, current in zip(
                self.period_end_dates, self.period_end_dates[1:], strict=False
            )
        ):
            raise ValueError("period-end dates must be strictly increasing")
        if any(len(row) != len(self.candidate_ids) for row in self.returns_by_observation):
            raise ValueError("every return row must cover every candidate")
        if len(self.period_end_dates) * len(self.candidate_ids) > _MAX_RETURN_VALUES:
            raise ValueError("return matrix exceeds the bounded value budget")
        if self.slice_count not in (4, 6, 8, 10):
            raise ValueError("slice_count must be one of 4, 6, 8, 10")
        if len(self.period_end_dates) % self.slice_count != 0:
            raise ValueError("observations must divide evenly into CSCV slices")
        if len(self.period_end_dates) // 2 < _MIN_HALF_OBSERVATIONS:
            raise ValueError("each half-sample needs at least 30 observations")
        return self


class CSCVSplitResult(BaseModel):
    """One IS/OOS split, with Sharpe arrays ordered like the input candidates."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    in_sample_slices: tuple[int, ...]
    out_of_sample_slices: tuple[int, ...]
    selected_candidate_id: str
    in_sample_sharpe_by_candidate: tuple[FiniteFloat, ...]
    out_of_sample_sharpe_by_candidate: tuple[FiniteFloat, ...]
    out_of_sample_rank: int
    out_of_sample_relative_rank: FiniteFloat
    logit: FiniteFloat


class CSCVPBOResult(BaseModel):
    """Exact split count and fraction of selected candidates below OOS median."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inputs: CSCVInput
    split_count: int
    below_median_count: int
    probability_of_backtest_overfitting: FiniteFloat
    splits: tuple[CSCVSplitResult, ...]


def _sample_sharpe(values: tuple[float, ...]) -> float:
    """Sample-standard-deviation Sharpe at the original observation frequency."""

    try:
        mean = fmean(values)
        variance = fsum((value - mean) ** 2 for value in values) / (len(values) - 1)
        standard_deviation = sqrt(variance)
        if standard_deviation == 0.0:
            raise ValueError("sample Sharpe is undefined for zero-variance returns")
        sharpe = mean / standard_deviation
    except OverflowError as exc:
        raise ValueError("sample Sharpe exceeds finite numeric range") from exc
    if not isfinite(sharpe):
        raise ValueError("sample Sharpe exceeds finite numeric range")
    return sharpe


def calculate_cscv_pbo(inputs: CSCVInput) -> CSCVPBOResult:
    """Enumerate all balanced slices; rank each IS winner strictly in its OOS half."""

    inputs = CSCVInput.model_validate(inputs)
    slice_size = len(inputs.period_end_dates) // inputs.slice_count
    rows_by_slice = tuple(
        inputs.returns_by_observation[index * slice_size : (index + 1) * slice_size]
        for index in range(inputs.slice_count)
    )
    candidate_indexes = range(len(inputs.candidate_ids))
    splits = []
    for in_sample_slices in combinations(range(inputs.slice_count), inputs.slice_count // 2):
        in_sample_slice_set = set(in_sample_slices)
        out_of_sample_slices = tuple(
            index for index in range(inputs.slice_count) if index not in in_sample_slice_set
        )
        in_sample_sharpes = tuple(
            _sample_sharpe(
                tuple(
                    row[candidate_index]
                    for slice_index in in_sample_slices
                    for row in rows_by_slice[slice_index]
                )
            )
            for candidate_index in candidate_indexes
        )
        out_of_sample_sharpes = tuple(
            _sample_sharpe(
                tuple(
                    row[candidate_index]
                    for slice_index in out_of_sample_slices
                    for row in rows_by_slice[slice_index]
                )
            )
            for candidate_index in candidate_indexes
        )
        if len(set(in_sample_sharpes)) != len(in_sample_sharpes) or len(
            set(out_of_sample_sharpes)
        ) != len(out_of_sample_sharpes):
            raise ValueError("tied IS/OOS Sharpe values make strict CSCV ranks undefined")
        selected_index = max(candidate_indexes, key=in_sample_sharpes.__getitem__)
        selected_oos_sharpe = out_of_sample_sharpes[selected_index]
        rank = 1 + sum(value < selected_oos_sharpe for value in out_of_sample_sharpes)
        relative_rank = rank / (len(inputs.candidate_ids) + 1)
        splits.append(
            CSCVSplitResult(
                in_sample_slices=in_sample_slices,
                out_of_sample_slices=out_of_sample_slices,
                selected_candidate_id=inputs.candidate_ids[selected_index],
                in_sample_sharpe_by_candidate=in_sample_sharpes,
                out_of_sample_sharpe_by_candidate=out_of_sample_sharpes,
                out_of_sample_rank=rank,
                out_of_sample_relative_rank=relative_rank,
                logit=log(rank / (len(inputs.candidate_ids) + 1 - rank)),
            )
        )

    below_median_count = sum(split.logit < 0.0 for split in splits)
    return CSCVPBOResult(
        inputs=inputs,
        split_count=len(splits),
        below_median_count=below_median_count,
        probability_of_backtest_overfitting=below_median_count / len(splits),
        splits=tuple(splits),
    )
