"""Fail-closed resource admission contracts for isolated research work."""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum
from typing import Self

from pydantic import Field, field_validator, model_validator

from rquant.research_run_spec import ResourceClass
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


class ResourceSnapshot(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    session: TradingSession
    live_backlog_age_seconds: float = Field(ge=0, allow_inf_nan=False)
    live_p95_latency_seconds: float = Field(ge=0, allow_inf_nan=False)
    available_memory_bytes: int = Field(ge=0)
    available_disk_bytes: int = Field(ge=0)
    io_pressure_pct: float = Field(ge=0, le=100, allow_inf_nan=False)
    cpu_load_pct: float = Field(ge=0, le=100, allow_inf_nan=False)
    source_quota_remaining: int = Field(ge=0)
    live_healthy: bool


class SourceQuotaLease(RuntimeContractModel):
    lease_id: str = ""
    source: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    units: int = Field(gt=0)
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


class AdmissionPolicy(RuntimeContractModel):
    allow_live_session: bool
    max_live_backlog_age_seconds: float = Field(ge=0, allow_inf_nan=False)
    max_live_p95_latency_seconds: float = Field(ge=0, allow_inf_nan=False)
    min_available_memory_bytes: int = Field(ge=0)
    min_available_disk_bytes: int = Field(ge=0)
    max_io_pressure_pct: float = Field(ge=0, le=100, allow_inf_nan=False)
    max_cpu_load_pct: float = Field(ge=0, le=100, allow_inf_nan=False)
    max_expected_memory_bytes: int = Field(ge=0)
    max_expected_disk_bytes: int = Field(ge=0)
    max_expected_quota_units: int = Field(ge=0)
    retry_delay_seconds: int = Field(gt=0)


class AdmissionRequest(RuntimeContractModel):
    job_id: str = Field(min_length=1)
    resource_class: ResourceClass
    expected_memory_bytes: int = Field(ge=0)
    expected_disk_bytes: int = Field(ge=0)
    expected_quota_units: int = Field(ge=0)
    source: str | None = Field(default=None, min_length=1)
    preemptible: bool
    read_only: bool
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


_LIVE_SESSIONS = frozenset(
    {
        TradingSession.PRE_MARKET,
        TradingSession.MORNING,
        TradingSession.LUNCH,
        TradingSession.AFTERNOON,
    }
)


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
    if not snapshot.live_healthy:
        reasons.add("live_unhealthy")
    if snapshot.live_backlog_age_seconds > policy.max_live_backlog_age_seconds:
        reasons.add("live_backlog_stale")
    if snapshot.live_p95_latency_seconds > policy.max_live_p95_latency_seconds:
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
