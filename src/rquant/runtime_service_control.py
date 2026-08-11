"""Per-service singleton authority and durable runtime health heartbeats."""

from __future__ import annotations

import fcntl
import json
import math
import os
import tempfile
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from threading import Event
from types import MappingProxyType
from typing import Annotated, Self

from pydantic import Field, StringConstraints, field_serializer, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
StepDuration = Annotated[float, Field(ge=0, allow_inf_nan=False)]
_STEP_DURATION_WINDOW = 20


class RuntimeServiceAlreadyRunningError(RuntimeError):
    pass


class RuntimeServicePlane(StrEnum):
    LIVE = "live"
    SERVING = "serving"
    RESEARCH = "research"


class RuntimeServiceStatus(StrEnum):
    MISSING = "missing"
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPED = "stopped"


class RuntimeServiceSpec(RuntimeContractModel):
    service_id: str = Field(min_length=1)
    plane: RuntimeServicePlane
    stale_after: timedelta
    producer_commit: CommitSha

    @field_validator("stale_after")
    @classmethod
    def validate_stale_after(cls, value: timedelta) -> timedelta:
        if value <= timedelta(0):
            raise ValueError("stale_after must be positive")
        return value

    @property
    def identity(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class RuntimeStepResult(RuntimeContractModel):
    input_sequence: int = Field(default=-1, ge=-1)
    output_sequence: int = Field(default=-1, ge=-1)
    processed_count: int = Field(default=0, ge=0)
    backlog_count: int = Field(default=0, ge=0)
    source_generations: Mapping[str, Sha256] = Field(default_factory=dict)
    degraded_reasons: tuple[str, ...] = ()

    @field_validator("source_generations")
    @classmethod
    def freeze_source_generations(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if any(not key for key in value):
            raise ValueError("source generation names cannot be empty")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("source_generations")
    def serialize_source_generations(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @field_validator("degraded_reasons")
    @classmethod
    def validate_degraded_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not reason for reason in value):
            raise ValueError("degraded reasons cannot be empty")
        if len(value) != len(set(value)):
            raise ValueError("degraded reasons must be unique")
        return tuple(sorted(value))


class RuntimeServiceHeartbeat(RuntimeContractModel):
    service_id: str = Field(min_length=1)
    spec_fingerprint: Sha256
    run_id: Sha256
    generation: int = Field(ge=1)
    status: RuntimeServiceStatus
    started_at: AwareUtcDatetime
    heartbeat_at: AwareUtcDatetime
    last_success_at: AwareUtcDatetime | None = None
    stopped_at: AwareUtcDatetime | None = None
    input_sequence: int = Field(default=-1, ge=-1)
    output_sequence: int = Field(default=-1, ge=-1)
    processed_count: int = Field(default=0, ge=0)
    backlog_count: int = Field(default=0, ge=0)
    consecutive_failures: int = Field(default=0, ge=0)
    total_failures: int = Field(default=0, ge=0)
    total_successes: int = Field(default=0, ge=0)
    last_step_duration_seconds: StepDuration | None = None
    p95_step_duration_seconds: StepDuration | None = None
    recent_step_durations_seconds: tuple[StepDuration, ...] = Field(
        default=(),
        max_length=_STEP_DURATION_WINDOW,
    )
    source_generations: Mapping[str, Sha256] = Field(default_factory=dict)
    degraded_reasons: tuple[str, ...] = ()
    last_error: str | None = None
    stop_reason: str | None = None

    @field_validator("source_generations")
    @classmethod
    def freeze_source_generations(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if any(not key for key in value):
            raise ValueError("source generation names cannot be empty")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("source_generations")
    def serialize_source_generations(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @field_validator("degraded_reasons")
    @classmethod
    def validate_degraded_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not reason for reason in value):
            raise ValueError("degraded reasons cannot be empty")
        if len(value) != len(set(value)):
            raise ValueError("degraded reasons must be unique")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        if self.status is RuntimeServiceStatus.MISSING:
            raise ValueError("persisted heartbeat cannot have missing status")
        if self.status is RuntimeServiceStatus.STOPPED:
            if self.stopped_at is None or self.stop_reason is None:
                raise ValueError("stopped heartbeat requires stopped_at and stop_reason")
        elif self.stopped_at is not None or self.stop_reason is not None:
            raise ValueError("active heartbeat cannot contain stop fields")
        if self.last_success_at is not None and self.last_success_at < self.started_at:
            raise ValueError("last_success_at cannot precede service start")
        durations = self.recent_step_durations_seconds
        if not durations:
            if (
                self.last_step_duration_seconds is not None
                or self.p95_step_duration_seconds is not None
            ):
                raise ValueError("step latency summaries require a duration window")
        else:
            if self.last_step_duration_seconds != durations[-1]:
                raise ValueError("last step duration must match the duration window tail")
            expected_p95 = _nearest_rank_p95(durations)
            if self.p95_step_duration_seconds != expected_p95:
                raise ValueError("p95 step duration does not match the duration window")
        return self


class RuntimeServiceHealth(RuntimeContractModel):
    service_id: str = Field(min_length=1)
    plane: RuntimeServicePlane
    status: RuntimeServiceStatus
    stale: bool
    observed_at: AwareUtcDatetime
    heartbeat: RuntimeServiceHeartbeat | None = None


Clock = Callable[[], datetime]


def _error_text(error: BaseException) -> str:
    message = str(error).strip()
    return type(error).__name__ if not message else f"{type(error).__name__}: {message}"


def _nearest_rank_p95(durations: tuple[float, ...]) -> float:
    ordered = tuple(sorted(durations))
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


def _duration_updates(
    current: RuntimeServiceHeartbeat,
    duration_seconds: float | None,
) -> dict[str, object]:
    if duration_seconds is None:
        return {}
    if not isinstance(duration_seconds, int | float) or isinstance(duration_seconds, bool):
        raise TypeError("duration_seconds must be a finite number")
    duration = float(duration_seconds)
    if not math.isfinite(duration) or duration < 0:
        raise ValueError("duration_seconds must be finite and non-negative")
    window = (*current.recent_step_durations_seconds, duration)[-_STEP_DURATION_WINDOW:]
    return {
        "last_step_duration_seconds": duration,
        "p95_step_duration_seconds": _nearest_rank_p95(window),
        "recent_step_durations_seconds": window,
    }


class RuntimeServiceControl:
    """One service owns one lock and atomically replaces only its heartbeat file."""

    def __init__(
        self,
        root: Path,
        *,
        spec: RuntimeServiceSpec,
        clock: Clock | None = None,
    ) -> None:
        self.root = Path(root).resolve()
        self.spec = spec
        self._clock = clock or (lambda: datetime.now(UTC))
        identity = canonical_sha256({"service_id": spec.service_id})
        self._heartbeat_path = self.root / "heartbeats" / f"{identity}.json"
        self._lock_path = self.root / "locks" / f"{identity}.lock"
        self._lock_descriptor = -1
        self._heartbeat: RuntimeServiceHeartbeat | None = None
        self._prepare_directories()

    def _prepare_directories(self) -> None:
        for path in (self.root, self._heartbeat_path.parent, self._lock_path.parent):
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            if path.is_symlink() or not path.is_dir():
                raise ValueError(f"runtime control path is unsafe: {path}")
            path.chmod(0o700)

    @classmethod
    def _path_for(cls, root: Path, spec: RuntimeServiceSpec) -> Path:
        identity = canonical_sha256({"service_id": spec.service_id})
        return Path(root).resolve() / "heartbeats" / f"{identity}.json"

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb", closefd=True) as stream:
                descriptor = -1
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            path.chmod(0o600)
            directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary)

    def _publish(self, heartbeat: RuntimeServiceHeartbeat) -> RuntimeServiceHeartbeat:
        if heartbeat.service_id != self.spec.service_id:
            raise ValueError("heartbeat service identity does not match control")
        payload = json.dumps(
            heartbeat.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        self._atomic_write(self._heartbeat_path, payload)
        self._heartbeat = heartbeat
        return heartbeat

    @staticmethod
    def _validated_update(
        current: RuntimeServiceHeartbeat,
        **updates: object,
    ) -> RuntimeServiceHeartbeat:
        payload = current.model_dump(mode="python")
        payload.update(updates)
        return RuntimeServiceHeartbeat.model_validate(payload)

    def start(self) -> RuntimeServiceHeartbeat:
        if self._lock_descriptor >= 0:
            raise RuntimeServiceAlreadyRunningError("runtime service control is already started")
        descriptor = os.open(
            self._lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise RuntimeServiceAlreadyRunningError(
                f"runtime service {self.spec.service_id} is already running"
            ) from exc
        self._lock_descriptor = descriptor
        previous = self.read_heartbeat(self.root, self.spec)
        generation = 1 if previous is None else previous.generation + 1
        now = normalize_aware_utc(self._clock())
        heartbeat = RuntimeServiceHeartbeat(
            service_id=self.spec.service_id,
            spec_fingerprint=self.spec.identity,
            run_id=canonical_sha256(
                {
                    "service_id": self.spec.service_id,
                    "generation": generation,
                    "started_at": now,
                    "pid": os.getpid(),
                }
            ),
            generation=generation,
            status=RuntimeServiceStatus.STARTING,
            started_at=now,
            heartbeat_at=now,
        )
        return self._publish(heartbeat)

    def _require_active(self) -> RuntimeServiceHeartbeat:
        if self._lock_descriptor < 0 or self._heartbeat is None:
            raise RuntimeError("runtime service control is not active")
        return self._heartbeat

    def record_success(
        self,
        result: RuntimeStepResult,
        *,
        duration_seconds: float | None = None,
    ) -> RuntimeServiceHeartbeat:
        current = self._require_active()
        if result.input_sequence < current.input_sequence:
            raise ValueError("input sequence cannot regress")
        if result.output_sequence < current.output_sequence:
            raise ValueError("output sequence cannot regress")
        now = normalize_aware_utc(self._clock())
        return self._publish(
            self._validated_update(
                current,
                status=(
                    RuntimeServiceStatus.DEGRADED
                    if result.degraded_reasons
                    else RuntimeServiceStatus.RUNNING
                ),
                heartbeat_at=now,
                last_success_at=now,
                input_sequence=result.input_sequence,
                output_sequence=result.output_sequence,
                processed_count=result.processed_count,
                backlog_count=result.backlog_count,
                consecutive_failures=0,
                total_successes=current.total_successes + 1,
                source_generations=result.source_generations,
                degraded_reasons=result.degraded_reasons,
                last_error=None,
                **_duration_updates(current, duration_seconds),
            )
        )

    def record_failure(
        self,
        error: Exception,
        *,
        duration_seconds: float | None = None,
    ) -> RuntimeServiceHeartbeat:
        current = self._require_active()
        return self._publish(
            self._validated_update(
                current,
                status=RuntimeServiceStatus.DEGRADED,
                heartbeat_at=normalize_aware_utc(self._clock()),
                consecutive_failures=current.consecutive_failures + 1,
                total_failures=current.total_failures + 1,
                degraded_reasons=(),
                last_error=_error_text(error),
                **_duration_updates(current, duration_seconds),
            )
        )

    def stop(
        self,
        *,
        reason: str,
        error: BaseException | None = None,
    ) -> RuntimeServiceHeartbeat:
        if not reason:
            raise ValueError("stop reason cannot be empty")
        current = self._require_active()
        now = normalize_aware_utc(self._clock())
        stopped = self._publish(
            self._validated_update(
                current,
                status=RuntimeServiceStatus.STOPPED,
                heartbeat_at=now,
                stopped_at=now,
                stop_reason=reason,
                last_error=_error_text(error) if error is not None else current.last_error,
            )
        )
        fcntl.flock(self._lock_descriptor, fcntl.LOCK_UN)
        os.close(self._lock_descriptor)
        self._lock_descriptor = -1
        return stopped

    @classmethod
    def read_heartbeat(
        cls,
        root: Path,
        spec: RuntimeServiceSpec,
    ) -> RuntimeServiceHeartbeat | None:
        path = cls._path_for(root, spec)
        if not path.exists():
            return None
        try:
            heartbeat = RuntimeServiceHeartbeat.model_validate_json(path.read_bytes())
        except (OSError, ValueError) as exc:
            raise ValueError(f"runtime heartbeat is invalid: {spec.service_id}") from exc
        if heartbeat.service_id != spec.service_id or heartbeat.spec_fingerprint != spec.identity:
            raise ValueError("runtime heartbeat does not match the requested service spec")
        return heartbeat


def run_service_loop(
    control: RuntimeServiceControl,
    *,
    step: Callable[[], RuntimeStepResult],
    stop_event: Event,
    interval_seconds: float,
    max_iterations: int | None = None,
    monotonic_clock: Callable[[], float] = time.monotonic,
) -> RuntimeServiceHeartbeat:
    if interval_seconds < 0:
        raise ValueError("interval_seconds cannot be negative")
    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be positive")
    control.start()
    completed = 0
    try:
        while not stop_event.is_set() and (max_iterations is None or completed < max_iterations):
            started = monotonic_clock()
            try:
                result = step()
            except Exception as error:
                control.record_failure(
                    error,
                    duration_seconds=monotonic_clock() - started,
                )
            else:
                control.record_success(
                    result,
                    duration_seconds=monotonic_clock() - started,
                )
            completed += 1
            if max_iterations is None or completed < max_iterations:
                stop_event.wait(interval_seconds)
    except BaseException as error:
        control.stop(reason="unhandled service crash", error=error)
        raise
    return control.stop(reason="loop completed")


def inspect_runtime_health(
    root: Path,
    *,
    specs: tuple[RuntimeServiceSpec, ...],
    observed_at: datetime,
) -> tuple[RuntimeServiceHealth, ...]:
    observed = normalize_aware_utc(observed_at)
    health: list[RuntimeServiceHealth] = []
    for spec in sorted(specs, key=lambda item: item.service_id):
        heartbeat = RuntimeServiceControl.read_heartbeat(root, spec)
        status = RuntimeServiceStatus.MISSING if heartbeat is None else heartbeat.status
        stale = heartbeat is None or observed - heartbeat.heartbeat_at > spec.stale_after
        health.append(
            RuntimeServiceHealth(
                service_id=spec.service_id,
                plane=spec.plane,
                status=status,
                stale=stale,
                observed_at=observed,
                heartbeat=heartbeat,
            )
        )
    return tuple(health)


__all__ = [
    "RuntimeServiceAlreadyRunningError",
    "RuntimeServiceControl",
    "RuntimeServiceHealth",
    "RuntimeServiceHeartbeat",
    "RuntimeServicePlane",
    "RuntimeServiceSpec",
    "RuntimeServiceStatus",
    "RuntimeStepResult",
    "inspect_runtime_health",
    "run_service_loop",
]
