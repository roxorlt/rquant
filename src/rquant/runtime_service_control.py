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
    #: Whether this iteration opened the read-only replica, and what the read cost. `None`
    #: for a role that does not read it at all, which is 21 of the 25 (#256).
    replica_opened: bool | None = None
    replica_read_bytes: int | None = Field(default=None, ge=0)

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
    #: Set while every iteration is failing on one artifact another role has not created
    #: yet. Without a failure threshold -- deliberately, so that a peer that has not
    #: started stops taking the process down and firing `OnFailure` -- "waiting" and
    #: "wedged" look the same on a dashboard: DEGRADED, forever. These three say which
    #: file, since when, and for how long, so a readiness probe or a later alerting rule
    #: has something to read other than the prose in `last_error`.
    waiting_for: str | None = None
    waiting_since: AwareUtcDatetime | None = None
    waited_seconds: StepDuration | None = None
    #: What this process had to move aside on the way in, once per run: the strategy's
    #: archived runner database, the route ledger's rotated source, the candidate
    #: authority's re-bind (#248). Stamped by `start()` and carried unchanged for the
    #: life of the run. It is a *file* field on purpose -- the serving payload embeds
    #: `RuntimeServiceHeartbeatProjection`, which is frozen at the v0.33.1 field set
    #: (#237), so nothing here reaches a published schema.
    generation_events: tuple[str, ...] = ()
    #: How long this iteration is waiting before retrying, when the same failure keeps
    #: coming back. A DEGRADED loop with no backoff is not free: on 2026-09-09 the two
    #: source roles that could not read a candidate store re-walked and re-hashed it every
    #: two seconds, and a 4-vCPU host sat at load 11-12 with ~47% system time -- the
    #: 15-minute backups went from 8 to 14 minutes and the monitor watchdog timed out
    #: (#254). `None` while the loop is healthy or on the first failure of a kind. Also a
    #: *file* field, for the reason `generation_events` gives above.
    failure_backoff_seconds: StepDuration | None = None
    #: What the backoff is counting: the failure this loop keeps getting, as
    #: `<exception type>` plus the artifact a peer wait names. A different failure resets
    #: the backoff, because a loop that alternates between two faults is not idle.
    failure_kind: str | None = None
    #: What this iteration did with the 10 GB read-only replica. On 2026-09-08 and
    #: 2026-09-09 the 17:00 daily pipeline stalled in its `daily_state` stage while these
    #: roles were running and finished a minute after they were stopped; memory was not
    #: the constraint (9 GB free), the page cache was -- four roles scanned the replica
    #: every iteration, the notifier's every two seconds. `replica_opened` is False on an
    #: iteration that recognised the generation it already read and did not open the
    #: database at all; `replica_read_bytes` is what this process read from the filesystem
    #: while the loader ran, from `/proc/self/io` `rchar` where the platform will say and
    #: `None` where it will not. `None`/`None` for the 21 roles that never read it (#256).
    #: Both are *file* fields, for the reason `generation_events` gives above.
    replica_opened: bool | None = None
    replica_read_bytes: int | None = Field(default=None, ge=0)

    @field_validator("failure_kind")
    @classmethod
    def validate_failure_kind(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("failure kind cannot be blank")
        return value

    @field_validator("generation_events")
    @classmethod
    def validate_generation_events(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not event.strip() for event in value):
            raise ValueError("generation events cannot be empty")
        if len(value) != len(set(value)):
            raise ValueError("generation events must be unique")
        return tuple(sorted(value))

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
        waiting = (self.waiting_for, self.waiting_since, self.waited_seconds)
        if any(value is not None for value in waiting) and not all(
            value is not None for value in waiting
        ):
            raise ValueError("waiting fields must be published as one group")
        if self.waiting_since is not None:
            if self.waiting_since < self.started_at:
                raise ValueError("waiting_since cannot precede service start")
            if self.waiting_since > self.heartbeat_at:
                raise ValueError("waiting_since cannot follow the heartbeat")
            expected = (self.heartbeat_at - self.waiting_since).total_seconds()
            if self.waited_seconds != expected:
                raise ValueError("waited_seconds must equal heartbeat_at minus waiting_since")
        return self


class _PublishedSchemaNames:
    """A namespace whose only job is to keep a published schema name off the module.

    `runtime.serving.runtime-health` hashes each of its nine fields against the whole
    `$defs` of `RuntimeHealthPayload`, and a `$defs` entry is keyed and titled by the
    Python class name. `RuntimeServiceHeartbeat` is therefore part of what that channel
    published in v0.33.1, not merely an internal identifier -- renaming the class the
    payload embeds is as breaking as renaming a field. Nesting keeps that published name
    while the module-level alias below says what the model is.

    Nesting is also what keeps the *class* resolvable. The other way to pin the published
    name is to rewrite `__qualname__` on a module-level model and rebuild it; `pickle`
    then looks the class up by module and qualname, finds the heartbeat file model there
    instead, and refuses with "it's not the same object as
    rquant.runtime_service_control.RuntimeServiceHeartbeat". Nothing here pickles a
    heartbeat -- instances of these models cannot be pickled either way, because
    `source_generations` is a `mappingproxy` -- but a class that lies about where it
    lives is not worth the three lines it saves.
    """

    #: The serving projection of a heartbeat: the v0.33.1 field set, frozen.
    #:
    #: The health payload used to embed `RuntimeServiceHeartbeat` itself, so every field
    #: added to the heartbeat file model rewrote the serving contract. Package J added
    #: three (#231) and the v0.33.2 installer then refused every host running the
    #: generation before it (#237). This model is the wire shape; the file model is free
    #: to grow, and a field only reaches serving when somebody adds it here and drives a
    #: schema version bump through a rollout.
    #:
    #: No docstring, deliberately: pydantic publishes `__doc__` as the `$defs`
    #: description, so prose here would move all nine field hashes of the channel.
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

        @classmethod
        def from_heartbeat(
            cls,
            heartbeat: RuntimeServiceHeartbeat,
        ) -> RuntimeServiceHeartbeatProjection:
            """Carry every projected field across; drop whatever serving does not publish.

            Driven off `model_fields` rather than a written-out argument list so that a
            field added here cannot be forgotten on this side, and so the only way to
            stop publishing a field is to delete it from the model -- which the release
            snapshot gate then refuses.
            """

            return cls.model_validate({name: getattr(heartbeat, name) for name in cls.model_fields})


RuntimeServiceHeartbeatProjection = _PublishedSchemaNames.RuntimeServiceHeartbeat


def project_heartbeat(
    heartbeat: RuntimeServiceHeartbeat | None,
) -> RuntimeServiceHeartbeatProjection | None:
    if heartbeat is None:
        return None
    return RuntimeServiceHeartbeatProjection.from_heartbeat(heartbeat)


class RuntimeServiceHealth(RuntimeContractModel):
    service_id: str = Field(min_length=1)
    plane: RuntimeServicePlane
    status: RuntimeServiceStatus
    stale: bool
    observed_at: AwareUtcDatetime
    #: The projection, never the file model: this field is published on
    #: `runtime.serving.runtime-health`, so its shape is a contract (#237).
    heartbeat: RuntimeServiceHeartbeatProjection | None = None

    @field_validator("heartbeat", mode="before")
    @classmethod
    def reject_unprojected_heartbeat(cls, value: object) -> object:
        # Both models are named RuntimeServiceHeartbeat on the wire, so pydantic's own
        # "input should be an instance of RuntimeServiceHeartbeat" reads as nonsense here.
        if isinstance(value, RuntimeServiceHeartbeat):
            raise ValueError(
                "serving health carries the heartbeat projection, not the heartbeat file "
                "model; convert with project_heartbeat() (#237)"
            )
        return value


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


def _waiting_updates(
    current: RuntimeServiceHeartbeat,
    error: BaseException,
    *,
    now: datetime,
) -> dict[str, object]:
    """How long this service has been failing on one absent peer artifact, if it is.

    The clock starts at the first iteration that named this artifact and keeps running
    while it keeps naming the same one; anything else -- a different artifact, a
    different kind of failure, a success -- clears it. So `waited_seconds` answers "how
    long has this been stuck on this file", which is what an operator and a readiness
    probe both want, and which `consecutive_failures` cannot answer once a service has
    waited for two different peers in one run.
    """

    from rquant.runtime_peer_artifacts import PeerArtifactUnavailableError

    if not isinstance(error, PeerArtifactUnavailableError):
        return {"waiting_for": None, "waiting_since": None, "waited_seconds": None}
    waiting_for = str(error.path)
    since = (
        current.waiting_since
        if current.waiting_for == waiting_for and current.waiting_since is not None
        else now
    )
    return {
        "waiting_for": waiting_for,
        "waiting_since": since,
        "waited_seconds": (now - since).total_seconds(),
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

    @classmethod
    def _lock_path_for(cls, root: Path, spec: RuntimeServiceSpec) -> Path:
        identity = canonical_sha256({"service_id": spec.service_id})
        return Path(root).resolve() / "locks" / f"{identity}.lock"

    @classmethod
    def _service_lock_is_held(cls, root: Path, spec: RuntimeServiceSpec) -> bool:
        """Whether some process is holding this service's singleton lock right now.

        This is the liveness question the heartbeat itself cannot answer: it records no pid,
        and a pid would be a stale number the moment it was written. The lock is the same
        one `start()` takes, so "nobody holds it" is exactly "no process is running this
        service". Anything that stops the probe from answering counts as held, so an
        unreadable lock never turns into a licence to overwrite a live service's heartbeat.
        """

        path = cls._lock_path_for(root, spec)
        try:
            descriptor = os.open(path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
        except FileNotFoundError:
            return False
        except OSError:
            return True
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        else:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return False
        finally:
            os.close(descriptor)

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

    def start(self, *, generation_events: tuple[str, ...] = ()) -> RuntimeServiceHeartbeat:
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
        # This process now holds the singleton lock, so no other process is running this
        # service and the liveness probe would only find itself.
        previous = self.read_heartbeat(self.root, self.spec, owns_service_lock=True)
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
            generation_events=generation_events,
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
                waiting_for=None,
                waiting_since=None,
                waited_seconds=None,
                failure_backoff_seconds=None,
                failure_kind=None,
                replica_opened=result.replica_opened,
                replica_read_bytes=result.replica_read_bytes,
                **_duration_updates(current, duration_seconds),
            )
        )

    def record_failure(
        self,
        error: Exception,
        *,
        duration_seconds: float | None = None,
        backoff_seconds: float | None = None,
        failure_kind: str | None = None,
        replica_cost: tuple[bool, int | None] | None = None,
    ) -> RuntimeServiceHeartbeat:
        current = self._require_active()
        now = normalize_aware_utc(self._clock())
        opened, read_bytes = (None, None) if replica_cost is None else replica_cost
        return self._publish(
            self._validated_update(
                current,
                status=RuntimeServiceStatus.DEGRADED,
                heartbeat_at=now,
                consecutive_failures=current.consecutive_failures + 1,
                total_failures=current.total_failures + 1,
                degraded_reasons=(),
                last_error=_error_text(error),
                failure_backoff_seconds=backoff_seconds,
                failure_kind=failure_kind,
                #: What this iteration did with the replica before it raised, when the role
                #: can say (#260). Without it the heartbeat reported nothing for exactly the
                #: iterations that failed, and the previous iteration's numbers would have
                #: read as this one's -- so a role that cannot say still reports neither.
                replica_opened=opened,
                replica_read_bytes=read_bytes,
                **_waiting_updates(current, error, now=now),
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
                #: the wait is still the same wait; only the clock moved
                waited_seconds=(
                    None
                    if current.waiting_since is None
                    else (now - current.waiting_since).total_seconds()
                ),
                #: a stopped run is not backing off any more; `failure_kind` stays, because
                #: it is what the last failure was, which is worth reading after the fact
                failure_backoff_seconds=None,
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
        *,
        owns_service_lock: bool = False,
    ) -> RuntimeServiceHeartbeat | None:
        """This service's current heartbeat, `None` if there is none this spec can claim.

        A heartbeat written under a different spec used to be refused unconditionally, which
        is right while that other instance is still running and wrong once it has stopped.
        Every generation change that alters a service spec — settings, plane — leaves such a
        file behind, so publishing sequence 3 in the first Route A window left both serving
        roles unable to start at all until the files were moved aside by hand (#216).

        A stopped instance's heartbeat is superseded, not a conflict: `stop()` is the only
        writer of `status=stopped` together with `stopped_at`, and releasing the singleton
        lock is the last thing it does. So the two facts together — the record says stopped,
        and nobody holds the lock — say the writer is gone and its file describes a service
        that no longer exists. It is reported as "no heartbeat", and the next `start()`
        replaces the file. Anything else keeps failing closed: a spec mismatch whose writer
        may still be alive, and a heartbeat that never reached `stop()` at all (a kill, a
        crashed host), both still refuse and still want a human to look.

        `owns_service_lock` is for `start()`, which has already taken the lock and would
        otherwise see its own hold as somebody else's.
        """

        path = cls._path_for(root, spec)
        if not path.exists():
            return None
        try:
            heartbeat = RuntimeServiceHeartbeat.model_validate_json(path.read_bytes())
        except (OSError, ValueError) as exc:
            raise ValueError(f"runtime heartbeat is invalid: {spec.service_id}") from exc
        if heartbeat.service_id != spec.service_id:
            raise ValueError("runtime heartbeat does not match the requested service spec")
        if heartbeat.spec_fingerprint != spec.identity:
            superseded = (
                heartbeat.status is RuntimeServiceStatus.STOPPED
                and heartbeat.stopped_at is not None
                and (owns_service_lock or not cls._service_lock_is_held(root, spec))
            )
            if not superseded:
                raise ValueError("runtime heartbeat does not match the requested service spec")
            return None
        return heartbeat


#: The longest a role may sit between retries of one failure it keeps getting. Bounded
#: from above by the tightest `stale_after_seconds` in the production profile, which is
#: **30** (`runtime_production_profile.py:991`, and it is the two source roles this whole
#: package is about): a heartbeat is written once per failure, so a backoff longer than
#: `stale_after` would put the role on the health plane as `stale` -- a second, invented
#: symptom on top of the real one. Twenty seconds still takes a two-second loop from 1800
#: iterations an hour to about 190, which is the whole point (#254).
MAX_FAILURE_BACKOFF_SECONDS = 20.0

#: How long a single wait may block before the loop looks at `stop_event` again. The event
#: already wakes the wait on its own; this bounds the stop latency anyway, because the
#: handler that sets it runs in the main thread and the process must never need `SIGKILL`
#: (2026-09-09: stopping `watchlist-quote` inside its failing loop exceeded
#: `TimeoutStopSec`, was killed, and left the unit `failed` -- one more real push).
_STOP_POLL_SECONDS = 0.25


def failure_kind_of(error: BaseException) -> str:
    """What "the same failure again" means for the backoff.

    The exception type, plus the artifact a peer wait names -- two roles waiting on two
    different files are not the same wait, and a loop that alternates between two faults
    is not idle and must not be slowed down as if it were. Deliberately *not* the message:
    those carry timestamps and sequence numbers, and would make every iteration look new.
    """

    kind = f"{type(error).__module__}.{type(error).__qualname__}"
    if is_peer_wait(error):
        return f"{kind}:{error.path}"  # type: ignore[attr-defined]
    return kind


def is_peer_wait(error: BaseException) -> bool:
    """Whether this failure is "the owner has not got here yet" rather than a fault.

    Peer waits are **not** backed off. A role waiting for a peer is not burning the host
    -- it is failing on a `lstat` -- and slowing it down would put the whole cold start's
    convergence at one backoff per edge: with `broker -> strategy -> router` no longer an
    ordered start (#252), the chain's worst case is exactly a sum of these waits, and
    making each one twenty seconds would trade a fixed order for a slow one. The expensive
    loop #254 is about is the other kind: an integrity failure whose path re-walks and
    re-hashes a store every iteration.
    """

    from rquant.runtime_peer_artifacts import PeerArtifactUnavailableError

    return isinstance(error, PeerArtifactUnavailableError)


def _failure_backoff_seconds(
    *,
    interval_seconds: float,
    consecutive: int,
    cap: float,
    peer_wait: bool = False,
) -> float | None:
    """`None` on the first failure of a kind, then doubling from the interval up to `cap`.

    The first failure of any kind is free, so a single blip never slows a healthy loop.
    From the second on it doubles, which is what makes an all-day DEGRADED loop cost
    nothing: on 2026-09-09 the two source roles retried a store they could not read every
    two seconds all morning, and re-walking and re-hashing it each time put a 4-vCPU host
    at load 11-12 (#254). A peer wait is never backed off -- see `is_peer_wait`.
    """

    if peer_wait or consecutive < 2:
        return None
    base = max(interval_seconds, _STOP_POLL_SECONDS)
    delay = min(base * float(2 ** min(consecutive - 1, 32)), cap)
    return None if delay <= interval_seconds else delay


def _wait_for_stop(
    stop_event: Event,
    delay: float,
    *,
    monotonic_clock: Callable[[], float],
) -> bool:
    """Wait `delay`, or until a stop is requested. `True` means stop now.

    One `wait()` for a delay inside a poll slice, so a role whose interval is short is
    driven exactly as it was before. Longer than that, the wait is sliced: `Event.set()`
    called from a signal handler runs on this very thread, and a handler that has to take
    the event's own lock to hand the news over is the one way this can be slower than the
    unit's `TimeoutStopSec`. Slicing costs four wake-ups a second while a role is backing
    off and bounds the stop latency without depending on that at all.
    """

    if delay <= 0:
        return stop_event.is_set()
    if delay <= _STOP_POLL_SECONDS:
        return stop_event.wait(delay) or stop_event.is_set()
    deadline = monotonic_clock() + delay
    while True:
        if stop_event.wait(_STOP_POLL_SECONDS) or stop_event.is_set():
            return True
        if monotonic_clock() >= deadline:
            return False


def _iteration_replica_cost(step: object) -> tuple[bool, int | None] | None:
    """`(opened, read_bytes)` for the iteration that just raised, or `None` (#260).

    A role that reads the read-only replica hangs its gate's `iteration_summary` on its own
    step, the way `generation_events` is hung there; a role that reads no replica has no
    attribute and reports neither rather than a fabricated zero. Reading it must never be
    able to replace the failure being recorded, so anything this probe raises is discarded
    and read as "cannot say": the heartbeat's cost is a diagnostic, the error is the news.
    """

    summary = getattr(step, "replica_iteration_summary", None)
    if not callable(summary):
        return None
    try:
        reported = summary()
        if reported is None:
            return None
        opened, read_bytes = reported
        return bool(opened), None if read_bytes is None else int(read_bytes)
    except Exception:  # noqa: BLE001 - a diagnostic may not displace the real failure
        return None


def run_service_loop(
    control: RuntimeServiceControl,
    *,
    step: Callable[[], RuntimeStepResult],
    stop_event: Event,
    interval_seconds: float,
    max_iterations: int | None = None,
    monotonic_clock: Callable[[], float] = time.monotonic,
    max_failure_backoff_seconds: float = MAX_FAILURE_BACKOFF_SECONDS,
) -> RuntimeServiceHeartbeat:
    if interval_seconds < 0:
        raise ValueError("interval_seconds cannot be negative")
    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be positive")
    if max_failure_backoff_seconds < 0:
        raise ValueError("max_failure_backoff_seconds cannot be negative")
    # The rotations a role performs happen while its step is being built, before this
    # control exists, so the step is what carries them out to the heartbeat (#248). One
    # stamp at start is enough: they describe this run, not this iteration.
    events = getattr(step, "generation_events", ())
    control.start(generation_events=tuple(events))
    completed = 0
    repeated_kind: str | None = None
    repeated_count = 0
    try:
        while not stop_event.is_set() and (max_iterations is None or completed < max_iterations):
            started = monotonic_clock()
            delay = interval_seconds
            try:
                result = step()
            except Exception as error:
                kind = failure_kind_of(error)
                repeated_count = repeated_count + 1 if kind == repeated_kind else 1
                repeated_kind = kind
                backoff = _failure_backoff_seconds(
                    interval_seconds=interval_seconds,
                    consecutive=repeated_count,
                    cap=max_failure_backoff_seconds,
                    peer_wait=is_peer_wait(error),
                )
                control.record_failure(
                    error,
                    duration_seconds=monotonic_clock() - started,
                    backoff_seconds=backoff,
                    failure_kind=kind,
                    replica_cost=_iteration_replica_cost(step),
                )
                if backoff is not None:
                    delay = backoff
            else:
                repeated_kind = None
                repeated_count = 0
                control.record_success(
                    result,
                    duration_seconds=monotonic_clock() - started,
                )
            completed += 1
            if max_iterations is None or completed < max_iterations:
                _wait_for_stop(stop_event, delay, monotonic_clock=monotonic_clock)
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
                heartbeat=project_heartbeat(heartbeat),
            )
        )
    return tuple(health)


__all__ = [
    "MAX_FAILURE_BACKOFF_SECONDS",
    "RuntimeServiceAlreadyRunningError",
    "RuntimeServiceControl",
    "RuntimeServiceHealth",
    "RuntimeServiceHeartbeat",
    "RuntimeServiceHeartbeatProjection",
    "RuntimeServicePlane",
    "RuntimeServiceSpec",
    "RuntimeServiceStatus",
    "RuntimeStepResult",
    "failure_kind_of",
    "inspect_runtime_health",
    "is_peer_wait",
    "project_heartbeat",
    "run_service_loop",
]
