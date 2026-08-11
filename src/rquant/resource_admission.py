"""Fail-closed resource admission contracts for isolated research work."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from enum import StrEnum
from types import MappingProxyType
from typing import Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from rquant.lab_shard_protocol import LabShardWorkPlan
from rquant.research_run_spec import ResearchRunSpec, ResourceClass
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)


class TradingSession(StrEnum):
    PRE_MARKET = "pre_market"
    MORNING = "morning"
    LUNCH = "lunch"
    AFTERNOON = "afternoon"
    POST_MARKET = "post_market"
    CLOSED = "closed"


class AdmissionOutcome(StrEnum):
    ADMITTED = "admitted"
    DEFERRED = "deferred"
    REJECTED = "rejected"


_LIVE_SESSIONS = frozenset(
    {
        TradingSession.PRE_MARKET,
        TradingSession.MORNING,
        TradingSession.LUNCH,
        TradingSession.AFTERNOON,
    }
)

MICROSECONDS_PER_SECOND = 1_000_000
MAX_RESOURCE_CAPACITY_BYTES = (1 << 63) - 1
MAX_RESOURCE_COUNT = (1 << 63) - 1
_MAX_DURATION_MICROSECONDS = (1 << 63) - 1
MAX_ADMISSION_DURATION_MS = int(timedelta(days=30) / timedelta(milliseconds=1))
MAX_ADMISSION_RETRY_SECONDS = int(timedelta(days=1) / timedelta(seconds=1))
_MAX_SAFE_RESOURCE_OBSERVED_AT = datetime.max.replace(tzinfo=UTC) - timedelta(
    seconds=MAX_ADMISSION_RETRY_SECONDS
)


def seconds_to_microseconds(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError(f"{label} must be a finite non-negative duration")
    try:
        seconds = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{label} must be a finite non-negative duration") from exc
    if not seconds.is_finite() or seconds < 0:
        raise ValueError(f"{label} must be a finite non-negative duration")
    scaled = seconds * MICROSECONDS_PER_SECOND
    if scaled > _MAX_DURATION_MICROSECONDS:
        raise ValueError(f"{label} exceeds the supported duration range")
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))


def timedelta_microseconds(value: timedelta) -> int:
    return value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds


def _normalize_duration_inputs(
    value: object,
    *,
    fields: tuple[tuple[str, str], ...],
) -> object:
    if not isinstance(value, Mapping):
        return value
    normalized = dict(value)
    for seconds_name, microseconds_name in fields:
        if seconds_name not in normalized:
            continue
        if microseconds_name in normalized:
            raise ValueError(f"{seconds_name} and {microseconds_name} cannot both be provided")
        normalized[microseconds_name] = seconds_to_microseconds(
            normalized.pop(seconds_name),
            label=seconds_name,
        )
    return normalized


class ResourceSnapshot(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    session: TradingSession
    live_slo_applicable: bool = Field(strict=True)
    live_backlog_age_microseconds: int = Field(strict=True, ge=0, le=_MAX_DURATION_MICROSECONDS)
    live_p95_latency_microseconds: int = Field(strict=True, ge=0, le=_MAX_DURATION_MICROSECONDS)
    available_memory_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    available_disk_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    io_pressure_pct: float = Field(strict=True, ge=0, le=100, allow_inf_nan=False)
    cpu_load_pct: float = Field(strict=True, ge=0, le=100, allow_inf_nan=False)
    source_quota_remaining: int = Field(strict=True, ge=0, le=MAX_RESOURCE_COUNT)
    live_healthy: bool = Field(strict=True)

    @field_validator("observed_at")
    @classmethod
    def validate_arithmetic_range(cls, value: datetime) -> datetime:
        if value > _MAX_SAFE_RESOURCE_OBSERVED_AT:
            raise ValueError("observed_at is outside the safe admission arithmetic range")
        return value

    @model_validator(mode="before")
    @classmethod
    def normalize_duration_inputs(cls, value: object) -> object:
        return _normalize_duration_inputs(
            value,
            fields=(
                ("live_backlog_age_seconds", "live_backlog_age_microseconds"),
                ("live_p95_latency_seconds", "live_p95_latency_microseconds"),
            ),
        )

    @model_validator(mode="before")
    @classmethod
    def derive_live_slo_scope(cls, value: object) -> object:
        if isinstance(value, Mapping) and "live_slo_applicable" not in value:
            copied = dict(value)
            copied["live_slo_applicable"] = TradingSession(copied["session"]) in _LIVE_SESSIONS
            return copied
        return value

    @model_validator(mode="after")
    def validate_live_slo_scope(self) -> Self:
        expected = self.session in _LIVE_SESSIONS
        if self.live_slo_applicable is not expected:
            raise ValueError("live_slo_applicable must match the trading session")
        return self

    @property
    def live_backlog_age_seconds(self) -> float:
        return self.live_backlog_age_microseconds / MICROSECONDS_PER_SECOND

    @property
    def live_p95_latency_seconds(self) -> float:
        return self.live_p95_latency_microseconds / MICROSECONDS_PER_SECOND


class SourceQuotaLease(RuntimeContractModel):
    lease_id: str = ""
    source: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    units: int = Field(strict=True, gt=0, le=MAX_RESOURCE_COUNT)
    granted_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    quota_reset_at: AwareUtcDatetime
    released_at: AwareUtcDatetime | None = None

    def identity_payload(self) -> dict[str, object]:
        return {
            "source": self.source,
            "owner": self.owner,
            "units": self.units,
            "granted_at": self.granted_at,
            "expires_at": self.expires_at,
            "quota_reset_at": self.quota_reset_at,
        }

    @model_validator(mode="after")
    def validate_lease(self) -> Self:
        if self.expires_at <= self.granted_at:
            raise ValueError("expires_at must follow granted_at")
        if self.quota_reset_at < self.expires_at:
            raise ValueError("quota_reset_at cannot precede expires_at")
        if self.released_at is not None and self.released_at < self.granted_at:
            raise ValueError("released_at cannot precede granted_at")

        expected_id = canonical_sha256(self.identity_payload())
        if self.lease_id and self.lease_id != expected_id:
            raise ValueError("lease_id does not match canonical lease content")
        object.__setattr__(self, "lease_id", expected_id)
        return self


class ResearchAdapterSourceUsage(RuntimeContractModel):
    """Auditable source contract for one research adapter execution."""

    adapter_id: str = Field(min_length=1, max_length=200)
    external: bool = Field(strict=True)
    immutable_snapshot: bool = Field(strict=True)
    source: str | None = Field(default=None, min_length=1, max_length=200)
    expected_calls: int = Field(strict=True, ge=0, le=MAX_RESOURCE_COUNT)
    actual_calls: int = Field(strict=True, ge=0, le=MAX_RESOURCE_COUNT)
    quota_lease: SourceQuotaLease | None = None

    @model_validator(mode="after")
    def validate_source_usage(self) -> Self:
        if self.actual_calls > self.expected_calls:
            raise ValueError("actual source calls exceed the declared estimate")
        if self.external:
            if self.immutable_snapshot:
                raise ValueError("external adapter cannot claim an immutable local snapshot")
            if self.source is None or self.expected_calls == 0 or self.quota_lease is None:
                raise ValueError("external adapter requires source, calls, and quota lease")
            if self.quota_lease.source != self.source:
                raise ValueError("external adapter quota lease source conflicts")
            if self.quota_lease.units < self.expected_calls:
                raise ValueError("external adapter quota lease is below declared calls")
        elif (
            not self.immutable_snapshot
            or self.source is not None
            or self.expected_calls != 0
            or self.actual_calls != 0
            or self.quota_lease is not None
        ):
            raise ValueError("zero quota is reserved for immutable local snapshot adapters")
        return self


class ResearchAdapterSourceUsageError(ValueError):
    pass


def require_research_adapter_source_usage(
    *,
    adapter_id: str,
    usage: ResearchAdapterSourceUsage | None,
) -> ResearchAdapterSourceUsage:
    if usage is None:
        raise ResearchAdapterSourceUsageError("research adapter source usage is required")
    if usage.adapter_id != adapter_id:
        raise ResearchAdapterSourceUsageError("research adapter source usage identity conflicts")
    return usage


class ResourceReservationIdentity(RuntimeContractModel):
    """Immutable execution-attempt identity for one resource reservation."""

    job_id: UUID
    run_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    shard_id: UUID
    attempt_id: UUID
    claim_generation: int = Field(strict=True, ge=1, le=MAX_RESOURCE_COUNT)
    scheduler_fencing_token: int = Field(strict=True, ge=1, le=MAX_RESOURCE_COUNT)
    worker_id: str = Field(min_length=1, max_length=200)


class ResourceReservationLease(RuntimeContractModel):
    lease_id: str = ""
    identity: ResourceReservationIdentity
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_memory_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    expected_disk_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    expected_quota_units: int = Field(strict=True, ge=0, le=MAX_RESOURCE_COUNT)
    granted_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.expires_at <= self.granted_at:
            raise ValueError("expires_at must follow granted_at")
        expected_id = canonical_sha256(self.identity)
        if self.lease_id and self.lease_id != expected_id:
            raise ValueError("lease_id does not match reservation identity")
        object.__setattr__(self, "lease_id", expected_id)
        return self


class AdmissionPolicy(RuntimeContractModel):
    allow_live_session: bool = Field(strict=True)
    max_live_shard_duration_ms: int = Field(
        default=5_000,
        strict=True,
        gt=0,
        le=MAX_ADMISSION_DURATION_MS,
    )
    max_snapshot_age_microseconds: int = Field(
        default=5 * MICROSECONDS_PER_SECOND,
        strict=True,
        gt=0,
        le=_MAX_DURATION_MICROSECONDS,
    )
    max_live_backlog_age_microseconds: int = Field(strict=True, ge=0, le=_MAX_DURATION_MICROSECONDS)
    max_live_p95_latency_microseconds: int = Field(strict=True, ge=0, le=_MAX_DURATION_MICROSECONDS)
    min_available_memory_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    min_available_disk_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    max_io_pressure_pct: float = Field(strict=True, ge=0, le=100, allow_inf_nan=False)
    max_cpu_load_pct: float = Field(strict=True, ge=0, le=100, allow_inf_nan=False)
    max_expected_memory_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    max_expected_disk_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    max_expected_quota_units: int = Field(strict=True, ge=0, le=MAX_RESOURCE_COUNT)
    retry_delay_seconds: int = Field(
        strict=True,
        gt=0,
        le=MAX_ADMISSION_RETRY_SECONDS,
    )

    @model_validator(mode="before")
    @classmethod
    def normalize_duration_inputs(cls, value: object) -> object:
        return _normalize_duration_inputs(
            value,
            fields=(
                ("max_snapshot_age_seconds", "max_snapshot_age_microseconds"),
                (
                    "max_live_backlog_age_seconds",
                    "max_live_backlog_age_microseconds",
                ),
                (
                    "max_live_p95_latency_seconds",
                    "max_live_p95_latency_microseconds",
                ),
            ),
        )

    @property
    def max_snapshot_age_seconds(self) -> float:
        return self.max_snapshot_age_microseconds / MICROSECONDS_PER_SECOND

    @property
    def max_live_backlog_age_seconds(self) -> float:
        return self.max_live_backlog_age_microseconds / MICROSECONDS_PER_SECOND

    @property
    def max_live_p95_latency_seconds(self) -> float:
        return self.max_live_p95_latency_microseconds / MICROSECONDS_PER_SECOND


class LabAdmissionCostProfile(RuntimeContractModel):
    """Deterministic upper-bound estimate for one immutable research shard."""

    base_memory_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    memory_bytes_per_work_step: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    work_units_per_step: int = Field(strict=True, gt=0, le=MAX_RESOURCE_COUNT)
    memory_bytes_per_duration_step: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    duration_ms_per_step: int = Field(strict=True, gt=0, le=MAX_ADMISSION_DURATION_MS)
    max_memory_bytes: int = Field(strict=True, gt=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    base_disk_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    disk_bytes_per_work_step: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    disk_bytes_per_duration_step: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    max_disk_bytes: int = Field(strict=True, gt=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    preemptible: bool = Field(strict=True)

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.base_memory_bytes > self.max_memory_bytes:
            raise ValueError("base memory cannot exceed maximum memory")
        if self.base_disk_bytes > self.max_disk_bytes:
            raise ValueError("base disk cannot exceed maximum disk")
        return self


_MIB = 1024**2
_GIB = 1024**3
LAB_ADMISSION_COST_PROFILES: Mapping[ResourceClass, LabAdmissionCostProfile] = MappingProxyType(
    {
        ResourceClass.INTERACTIVE: LabAdmissionCostProfile(
            base_memory_bytes=256 * _MIB,
            memory_bytes_per_work_step=64 * _MIB,
            work_units_per_step=500,
            memory_bytes_per_duration_step=32 * _MIB,
            duration_ms_per_step=15 * 60 * 1_000,
            max_memory_bytes=1536 * _MIB,
            base_disk_bytes=128 * _MIB,
            disk_bytes_per_work_step=32 * _MIB,
            disk_bytes_per_duration_step=64 * _MIB,
            max_disk_bytes=4 * _GIB,
            preemptible=True,
        ),
        ResourceClass.STANDARD: LabAdmissionCostProfile(
            base_memory_bytes=512 * _MIB,
            memory_bytes_per_work_step=64 * _MIB,
            work_units_per_step=1_000,
            memory_bytes_per_duration_step=64 * _MIB,
            duration_ms_per_step=60 * 60 * 1_000,
            max_memory_bytes=4 * _GIB,
            base_disk_bytes=256 * _MIB,
            disk_bytes_per_work_step=64 * _MIB,
            disk_bytes_per_duration_step=256 * _MIB,
            max_disk_bytes=16 * _GIB,
            preemptible=True,
        ),
        ResourceClass.HEAVY: LabAdmissionCostProfile(
            base_memory_bytes=1 * _GIB,
            memory_bytes_per_work_step=128 * _MIB,
            work_units_per_step=5_000,
            memory_bytes_per_duration_step=128 * _MIB,
            duration_ms_per_step=6 * 60 * 60 * 1_000,
            max_memory_bytes=8 * _GIB,
            base_disk_bytes=512 * _MIB,
            disk_bytes_per_work_step=256 * _MIB,
            disk_bytes_per_duration_step=1 * _GIB,
            max_disk_bytes=64 * _GIB,
            preemptible=False,
        ),
    }
)


def _ceil_div(value: int, divisor: int) -> int:
    return (value + divisor - 1) // divisor


def derive_lab_admission_request(
    *,
    job_id: str | UUID,
    spec: ResearchRunSpec,
    work_plan: LabShardWorkPlan | None,
) -> AdmissionRequest:
    """Estimate one shard without inspecting data or contacting an external source.

    Current Strategy Lab adapters replay immutable local snapshots, so their source
    quota cost is exactly zero. A future adapter that calls a market-data source must
    introduce a versioned request mapping instead of silently changing this contract.
    """

    validated_spec = ResearchRunSpec.model_validate(spec)
    if work_plan is None:
        raise ValueError("resource admission requires an explicit shard work plan")
    validated_plan = LabShardWorkPlan.model_validate(work_plan)
    profile = LAB_ADMISSION_COST_PROFILES[validated_spec.resource_class]
    work_steps = _ceil_div(validated_plan.work_units, profile.work_units_per_step)
    duration_steps = _ceil_div(
        validated_plan.static_duration_ms,
        profile.duration_ms_per_step,
    )
    expected_memory = min(
        profile.max_memory_bytes,
        profile.base_memory_bytes
        + work_steps * profile.memory_bytes_per_work_step
        + duration_steps * profile.memory_bytes_per_duration_step,
    )
    expected_disk = min(
        profile.max_disk_bytes,
        profile.base_disk_bytes
        + work_steps * profile.disk_bytes_per_work_step
        + duration_steps * profile.disk_bytes_per_duration_step,
    )
    return AdmissionRequest(
        job_id=str(job_id),
        resource_class=validated_spec.resource_class,
        expected_memory_bytes=expected_memory,
        expected_disk_bytes=expected_disk,
        expected_quota_units=0,
        expected_duration_ms=validated_plan.static_duration_ms,
        source=None,
        preemptible=profile.preemptible,
        read_only=True,
        deadline=validated_spec.deadline,
    )


class AdmissionRequest(RuntimeContractModel):
    job_id: str = Field(min_length=1)
    resource_class: ResourceClass
    expected_memory_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    expected_disk_bytes: int = Field(strict=True, ge=0, le=MAX_RESOURCE_CAPACITY_BYTES)
    expected_quota_units: int = Field(strict=True, ge=0, le=MAX_RESOURCE_COUNT)
    expected_duration_ms: int = Field(
        default=1,
        strict=True,
        gt=0,
        le=MAX_ADMISSION_DURATION_MS,
    )
    source: str | None = Field(default=None, min_length=1)
    preemptible: bool = Field(strict=True)
    read_only: bool = Field(strict=True)
    deadline: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_quota_source(self) -> Self:
        if self.expected_quota_units > 0 and self.source is None:
            raise ValueError("source is required when expected_quota_units is positive")
        return self


class AdmissionDecision(RuntimeContractModel):
    outcome: AdmissionOutcome
    reason_codes: tuple[str, ...] = ()
    observed_at: AwareUtcDatetime
    retry_at: AwareUtcDatetime | None = None
    quota_lease: SourceQuotaLease | None = None

    @field_validator("reason_codes")
    @classmethod
    def validate_reason_codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not reason for reason in value):
            raise ValueError("reason_codes cannot contain empty values")
        if len(value) != len(set(value)):
            raise ValueError("reason_codes must be unique")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.retry_at is not None and self.retry_at <= self.observed_at:
            raise ValueError("retry_at must follow observed_at")
        if self.outcome is AdmissionOutcome.ADMITTED:
            if self.reason_codes or self.retry_at is not None:
                raise ValueError("admitted decision cannot have reasons or retry_at")
        elif self.outcome is AdmissionOutcome.DEFERRED:
            if not self.reason_codes:
                raise ValueError("deferred decision requires reasons")
            if self.retry_at is None:
                raise ValueError("deferred decision requires retry_at")
        else:
            if not self.reason_codes:
                raise ValueError("rejected decision requires reasons")
            if self.retry_at is not None:
                raise ValueError("rejected decision cannot have retry_at")
        return self


def evaluate_admission(
    request: AdmissionRequest,
    snapshot: ResourceSnapshot,
    policy: AdmissionPolicy,
    quota_lease: SourceQuotaLease | None = None,
) -> AdmissionDecision:
    if not request.read_only:
        return AdmissionDecision(
            outcome=AdmissionOutcome.REJECTED,
            reason_codes=("non_read_only",),
            observed_at=snapshot.observed_at,
        )

    reasons: set[str] = set()
    if snapshot.session in _LIVE_SESSIONS:
        if not policy.allow_live_session:
            reasons.add("live_session_blocked")
        elif not request.preemptible:
            reasons.add("non_preemptible_live_session")
        elif request.expected_duration_ms > policy.max_live_shard_duration_ms:
            reasons.add("live_duration_exceeded")
    if snapshot.live_slo_applicable:
        if not snapshot.live_healthy:
            reasons.add("live_unhealthy")
        if snapshot.live_backlog_age_microseconds > policy.max_live_backlog_age_microseconds:
            reasons.add("live_backlog_stale")
        if snapshot.live_p95_latency_microseconds > policy.max_live_p95_latency_microseconds:
            reasons.add("live_latency_high")
    if snapshot.io_pressure_pct > policy.max_io_pressure_pct:
        reasons.add("io_pressure_high")
    if snapshot.cpu_load_pct > policy.max_cpu_load_pct:
        reasons.add("cpu_load_high")

    if request.expected_memory_bytes > policy.max_expected_memory_bytes:
        reasons.add("expected_memory_cost_exceeded")
    if request.expected_disk_bytes > policy.max_expected_disk_bytes:
        reasons.add("expected_disk_cost_exceeded")
    if request.expected_quota_units > policy.max_expected_quota_units:
        reasons.add("expected_quota_cost_exceeded")
    if (
        snapshot.available_memory_bytes - request.expected_memory_bytes
        < policy.min_available_memory_bytes
    ):
        reasons.add("insufficient_memory")
    if (
        snapshot.available_disk_bytes - request.expected_disk_bytes
        < policy.min_available_disk_bytes
    ):
        reasons.add("insufficient_disk")
    if snapshot.source_quota_remaining < request.expected_quota_units:
        reasons.add("insufficient_source_quota")
    if request.deadline <= snapshot.observed_at:
        reasons.add("deadline_expired")
    elif request.expected_duration_ms * 1_000 > timedelta_microseconds(
        request.deadline - snapshot.observed_at
    ):
        reasons.add("deadline_insufficient")

    retry_at = snapshot.observed_at + timedelta(seconds=policy.retry_delay_seconds)
    if request.expected_quota_units > 0:
        if quota_lease is None:
            reasons.add("quota_lease_missing")
        elif quota_lease.owner != request.job_id:
            reasons.add("quota_lease_owner_mismatch")
        elif quota_lease.source != request.source:
            reasons.add("quota_lease_source_mismatch")
        elif quota_lease.released_at is not None:
            reasons.add("quota_lease_released")
        elif quota_lease.granted_at > snapshot.observed_at:
            reasons.add("quota_lease_not_active")
        elif quota_lease.expires_at <= snapshot.observed_at:
            reasons.add("quota_lease_expired")
            retry_at = max(retry_at, quota_lease.quota_reset_at)
        elif quota_lease.units < request.expected_quota_units:
            reasons.add("quota_lease_insufficient")

    if reasons:
        return AdmissionDecision(
            outcome=AdmissionOutcome.DEFERRED,
            reason_codes=tuple(sorted(reasons)),
            observed_at=snapshot.observed_at,
            retry_at=retry_at,
            quota_lease=quota_lease,
        )
    return AdmissionDecision(
        outcome=AdmissionOutcome.ADMITTED,
        observed_at=snapshot.observed_at,
        quota_lease=quota_lease,
    )
