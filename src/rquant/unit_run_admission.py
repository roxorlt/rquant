"""Pure manual-run admission against a trusted, immutable evidence snapshot."""

from __future__ import annotations

from datetime import date, time, timedelta
from enum import StrEnum
from typing import Self
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_TRADING_WRITE_START = time(9, 15)
_TRADING_WRITE_END = time(15, 10)
_MAX_MIN_INTERVAL_SECONDS = 366 * 24 * 60 * 60


class UnitEffect(StrEnum):
    READ_ONLY = "read_only"
    WRITES_PRIMARY = "writes_primary"


class UnitRunRefusal(StrEnum):
    UNIT_NOT_ALLOWED = "unit_not_allowed"
    POLICY_DISABLED = "policy_disabled"
    CLOCK_UNAVAILABLE = "clock_unavailable"
    CALENDAR_UNAVAILABLE = "calendar_unavailable"
    CALENDAR_MISMATCH = "calendar_mismatch"
    UNIT_STATE_UNKNOWN = "unit_state_unknown"
    UNIT_STATE_CONFLICT = "unit_state_conflict"
    UNIT_ACTIVE = "unit_active"
    OUTSIDE_WINDOW = "outside_window"
    LAST_START_UNKNOWN = "last_start_unknown"
    LAST_START_CONFLICT = "last_start_conflict"
    LAST_START_IN_FUTURE = "last_start_in_future"
    MIN_INTERVAL = "min_interval"
    WRITER_STATE_UNKNOWN = "writer_state_unknown"
    WRITER_BUSY = "writer_busy"
    TRADING_SESSION_WRITE_BLOCKED = "trading_session_write_blocked"


class UnitRunPolicy(RuntimeContractModel):
    unit_name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.@-]*\.service$", max_length=255)
    enabled: bool = Field(strict=True)
    effect: UnitEffect
    window_start: time
    window_end: time
    min_interval_seconds: int = Field(strict=True, gt=0, le=_MAX_MIN_INTERVAL_SECONDS)

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if (
            self.window_start.tzinfo is not None
            or self.window_end.tzinfo is not None
            or self.window_start >= self.window_end
        ):
            raise ValueError("window must be a non-crossing Shanghai wall-time interval")
        return self


class UnitRunSnapshot(RuntimeContractModel):
    requested_unit: str = Field(strict=True, max_length=255)
    policy: UnitRunPolicy | None
    evaluated_at: AwareUtcDatetime | None
    sse_session_date: date | None
    is_sse_trading_day: bool | None = Field(strict=True)
    systemd_active: bool | None = Field(strict=True)
    systemd_running: bool | None = Field(strict=True)
    last_start_evidence_available: bool | None = Field(strict=True)
    last_started_at: AwareUtcDatetime | None
    writer_mutex_held: bool | None = Field(strict=True)


class UnitRunDecision(RuntimeContractModel):
    allowed: bool = Field(strict=True)
    reason: UnitRunRefusal | None = None

    @model_validator(mode="after")
    def validate_decision(self) -> Self:
        if self.allowed != (self.reason is None):
            raise ValueError("allowed decisions have no reason; refusals require one")
        return self


def _refuse(reason: UnitRunRefusal) -> UnitRunDecision:
    return UnitRunDecision(allowed=False, reason=reason)


def admit_unit_run(snapshot: UnitRunSnapshot) -> UnitRunDecision:
    """Decide for this snapshot only; execution must acquire a lock and recheck."""

    policy = snapshot.policy
    if policy is None or snapshot.requested_unit != policy.unit_name:
        return _refuse(UnitRunRefusal.UNIT_NOT_ALLOWED)
    if not policy.enabled:
        return _refuse(UnitRunRefusal.POLICY_DISABLED)

    now = snapshot.evaluated_at
    if now is None:
        return _refuse(UnitRunRefusal.CLOCK_UNAVAILABLE)
    local_now = now.astimezone(_SHANGHAI)
    if snapshot.sse_session_date is None or snapshot.is_sse_trading_day is None:
        return _refuse(UnitRunRefusal.CALENDAR_UNAVAILABLE)
    if snapshot.sse_session_date != local_now.date():
        return _refuse(UnitRunRefusal.CALENDAR_MISMATCH)

    if snapshot.systemd_active is None or snapshot.systemd_running is None:
        return _refuse(UnitRunRefusal.UNIT_STATE_UNKNOWN)
    if snapshot.systemd_running and not snapshot.systemd_active:
        return _refuse(UnitRunRefusal.UNIT_STATE_CONFLICT)
    if snapshot.systemd_active or snapshot.systemd_running:
        return _refuse(UnitRunRefusal.UNIT_ACTIVE)

    local_time = local_now.time()
    if not policy.window_start <= local_time < policy.window_end:
        return _refuse(UnitRunRefusal.OUTSIDE_WINDOW)

    if snapshot.last_start_evidence_available is not True:
        if snapshot.last_started_at is not None:
            return _refuse(UnitRunRefusal.LAST_START_CONFLICT)
        return _refuse(UnitRunRefusal.LAST_START_UNKNOWN)
    if snapshot.last_started_at is not None:
        if snapshot.last_started_at > now:
            return _refuse(UnitRunRefusal.LAST_START_IN_FUTURE)
        if now - snapshot.last_started_at < timedelta(seconds=policy.min_interval_seconds):
            return _refuse(UnitRunRefusal.MIN_INTERVAL)

    if policy.effect is UnitEffect.WRITES_PRIMARY:
        if snapshot.writer_mutex_held is None:
            return _refuse(UnitRunRefusal.WRITER_STATE_UNKNOWN)
        if snapshot.writer_mutex_held:
            return _refuse(UnitRunRefusal.WRITER_BUSY)
        if (
            snapshot.is_sse_trading_day
            and _TRADING_WRITE_START <= local_time <= _TRADING_WRITE_END
        ):
            return _refuse(UnitRunRefusal.TRADING_SESSION_WRITE_BLOCKED)

    return UnitRunDecision(allowed=True)
