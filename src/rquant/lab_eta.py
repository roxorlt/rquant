"""Pure deterministic ETA projection for Strategy Lab shard telemetry.

Cold-start estimates use a fixed 0.75x/1.0x/1.5x interval around each
planner-provided static duration. No historical capability is inferred for
legacy shards without a work plan.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.lab_shard_protocol import LabShardTelemetry, LabShardWorkPlan

LabEtaStatus = Literal[
    "queued",
    "running",
    "checkpointed",
    "paused",
    "succeeded",
    "failed",
    "cancelled",
]
LabEtaEstimator = Literal["static", "ewma", "mixed", "terminal", "unavailable", "unknown"]

_EWMA_ALPHA = 0.5
_INTERVAL_Z = 1.645
_STATIC_LOW_FACTOR = 0.75
_STATIC_HIGH_FACTOR = 1.5


class LabEtaProjectionError(ValueError):
    """ETA arithmetic cannot be represented as a finite duration or datetime."""


class LabEtaModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


def _normalize_utc_datetime(
    value: datetime,
    *,
    naive_message: str,
    range_message: str,
) -> datetime:
    if value.tzinfo is None:
        raise ValueError(naive_message)
    try:
        offset = value.utcoffset()
    except (OverflowError, ValueError) as exc:
        raise ValueError(range_message) from exc
    if offset is None:
        raise ValueError(naive_message)
    try:
        return value.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise ValueError(range_message) from exc


class LabEtaCompletedShard(LabEtaModel):
    shard_id: UUID
    completion_sequence: int = Field(strict=True, ge=1)
    telemetry: LabShardTelemetry


class LabEtaRemainingShard(LabEtaModel):
    shard_id: UUID
    work_plan: LabShardWorkPlan | None


class LabEtaInput(LabEtaModel):
    job_id: UUID
    status: LabEtaStatus
    as_of: datetime
    completed: tuple[LabEtaCompletedShard, ...] = ()
    remaining: tuple[LabEtaRemainingShard, ...] = ()

    @model_validator(mode="after")
    def validate_input(self) -> LabEtaInput:
        normalized_as_of = _normalize_utc_datetime(
            self.as_of,
            naive_message="as_of must be timezone-aware",
            range_message="as_of is outside the UTC datetime domain",
        )
        sequences = tuple(item.completion_sequence for item in self.completed)
        if len(sequences) != len(set(sequences)):
            raise ValueError("completion_sequence must be unique within a job")
        completed_ids = {item.shard_id for item in self.completed}
        remaining_ids = {item.shard_id for item in self.remaining}
        if len(completed_ids) != len(self.completed) or len(remaining_ids) != len(self.remaining):
            raise ValueError("ETA shard identities must be unique")
        if completed_ids & remaining_ids:
            raise ValueError("completed and remaining ETA shards must be disjoint")
        object.__setattr__(self, "as_of", normalized_as_of)
        return self


class LabEtaDurationRange(LabEtaModel):
    low_ms: float = Field(ge=0, allow_inf_nan=False)
    center_ms: float = Field(ge=0, allow_inf_nan=False)
    high_ms: float = Field(ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_order(self) -> LabEtaDurationRange:
        if not self.low_ms <= self.center_ms <= self.high_ms:
            raise ValueError("ETA duration bounds must contain the center")
        return self


class LabEtaFinishWindow(LabEtaModel):
    low: datetime
    center: datetime
    high: datetime

    @model_validator(mode="after")
    def validate_window(self) -> LabEtaFinishWindow:
        values = (self.low, self.center, self.high)
        normalized = tuple(
            _normalize_utc_datetime(
                value,
                naive_message="ETA finish timestamps must be timezone-aware",
                range_message="ETA finish timestamp is outside the UTC datetime domain",
            )
            for value in values
        )
        if not normalized[0] <= normalized[1] <= normalized[2]:
            raise ValueError("ETA finish bounds must contain the center")
        object.__setattr__(self, "low", normalized[0])
        object.__setattr__(self, "center", normalized[1])
        object.__setattr__(self, "high", normalized[2])
        return self


class LabEtaEstimate(LabEtaModel):
    job_id: UUID
    status: LabEtaStatus
    as_of: datetime
    estimator: LabEtaEstimator
    completed_telemetry_shards: int = Field(ge=0)
    remaining_shards: int = Field(ge=0)
    remaining_duration: LabEtaDurationRange | None
    finish_at: LabEtaFinishWindow | None


class _EwmaState(LabEtaModel):
    count: int = Field(ge=1)
    mu_ms_per_unit: float = Field(gt=0, allow_inf_nan=False)
    variance: float = Field(ge=0, allow_inf_nan=False)


def _ewma_states(
    completed: tuple[LabEtaCompletedShard, ...],
) -> dict[tuple[str, str], _EwmaState]:
    states: dict[tuple[str, str], _EwmaState] = {}
    for sample in sorted(completed, key=lambda item: item.completion_sequence):
        telemetry = sample.telemetry
        key = (telemetry.phase, telemetry.work_unit_name)
        value = telemetry.duration_ms / telemetry.work_units
        old = states.get(key)
        if old is None:
            states[key] = _EwmaState(count=1, mu_ms_per_unit=value, variance=0)
            continue
        old_mu = old.mu_ms_per_unit
        new_mu = _EWMA_ALPHA * value + (1 - _EWMA_ALPHA) * old_mu
        new_variance = (
            _EWMA_ALPHA * (value - old_mu) * (value - new_mu) + (1 - _EWMA_ALPHA) * old.variance
        )
        states[key] = _EwmaState(
            count=old.count + 1,
            mu_ms_per_unit=new_mu,
            variance=max(0, new_variance),
        )
    return states


def _static_range(work_plan: LabShardWorkPlan) -> tuple[float, float, float]:
    center = float(work_plan.static_duration_ms)
    return (
        center * _STATIC_LOW_FACTOR,
        center,
        center * _STATIC_HIGH_FACTOR,
    )


def _ewma_range(
    work_plan: LabShardWorkPlan,
    state: _EwmaState,
) -> tuple[float, float, float]:
    mu = state.mu_ms_per_unit
    standard_deviation = math.sqrt(state.variance)
    low_per_unit = max(1, 0.25 * mu, mu - _INTERVAL_Z * standard_deviation)
    high_per_unit = min(4 * mu, mu + _INTERVAL_Z * standard_deviation)
    return (
        low_per_unit * work_plan.work_units,
        mu * work_plan.work_units,
        high_per_unit * work_plan.work_units,
    )


def _finish_window(as_of: datetime, duration: LabEtaDurationRange) -> LabEtaFinishWindow:
    try:
        return LabEtaFinishWindow(
            low=as_of + timedelta(milliseconds=duration.low_ms),
            center=as_of + timedelta(milliseconds=duration.center_ms),
            high=as_of + timedelta(milliseconds=duration.high_ms),
        )
    except (OverflowError, ValueError) as exc:
        raise LabEtaProjectionError("ETA finish window is outside the datetime domain") from exc


def _duration_range(*, low_ms: float, center_ms: float, high_ms: float) -> LabEtaDurationRange:
    try:
        return LabEtaDurationRange(
            low_ms=low_ms,
            center_ms=center_ms,
            high_ms=high_ms,
        )
    except (OverflowError, ValueError) as exc:
        raise LabEtaProjectionError("ETA duration is outside the numeric domain") from exc


def estimate_lab_eta(value: LabEtaInput) -> LabEtaEstimate:
    eta_input = LabEtaInput.model_validate(value)
    completed_count = len(eta_input.completed)
    remaining_count = len(eta_input.remaining)
    common = {
        "job_id": eta_input.job_id,
        "status": eta_input.status,
        "as_of": eta_input.as_of,
        "completed_telemetry_shards": completed_count,
        "remaining_shards": remaining_count,
    }
    if eta_input.status in {"failed", "cancelled", "checkpointed", "paused"}:
        return LabEtaEstimate(
            **common,
            estimator="unavailable",
            remaining_duration=None,
            finish_at=None,
        )
    if eta_input.status == "succeeded":
        duration = _duration_range(low_ms=0, center_ms=0, high_ms=0)
        return LabEtaEstimate(
            **{**common, "remaining_shards": 0},
            estimator="terminal",
            remaining_duration=duration,
            finish_at=_finish_window(eta_input.as_of, duration),
        )
    if any(item.work_plan is None for item in eta_input.remaining):
        return LabEtaEstimate(
            **common,
            estimator="unknown",
            remaining_duration=None,
            finish_at=None,
        )

    states = _ewma_states(eta_input.completed) if completed_count >= 3 else {}
    low_ms = 0.0
    center_ms = 0.0
    high_ms = 0.0
    used_static = False
    used_ewma = False
    for remaining in sorted(eta_input.remaining, key=lambda item: item.shard_id.int):
        plan = remaining.work_plan
        assert plan is not None
        state = states.get((plan.phase, plan.work_unit_name))
        if state is not None and state.count >= 3:
            low, center, high = _ewma_range(plan, state)
            used_ewma = True
        else:
            low, center, high = _static_range(plan)
            used_static = True
        low_ms += low
        center_ms += center
        high_ms += high
    duration = _duration_range(
        low_ms=low_ms,
        center_ms=center_ms,
        high_ms=high_ms,
    )
    estimator: LabEtaEstimator
    if used_ewma and used_static:
        estimator = "mixed"
    elif used_ewma:
        estimator = "ewma"
    else:
        estimator = "static"
    return LabEtaEstimate(
        **common,
        estimator=estimator,
        remaining_duration=duration,
        finish_at=_finish_window(eta_input.as_of, duration),
    )
