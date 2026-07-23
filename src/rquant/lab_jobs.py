"""SQLite single-writer ledger for durable Strategy Lab jobs."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from urllib.parse import quote
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabCommandEnvelope,
    LabCommandReceipt,
    PauseJobCommand,
    RequestContentConflictError,
    ResumeJobCommand,
    RetryJobCommand,
    SubmitJobCommand,
)
from rquant.research_run_spec import (
    ResearchJobType,
    ResearchRunSpec,
    ResourceClass,
)


class SchedulerLeaseUnavailableError(RuntimeError):
    """Another scheduler owns the unexpired singleton lease."""


class SchedulerLeaseFencedError(RuntimeError):
    """A mutation was attempted with a stale scheduler lease."""


class StaleJobVersionError(RuntimeError):
    """Optimistic job version does not match the durable ledger."""


class InvalidJobTransitionError(RuntimeError):
    """The requested state transition is outside the frozen state matrix."""


class InvalidStoredJobError(RuntimeError):
    """Stored spec content or denormalized query columns were tampered with."""


class CancelConfirmationRequiredError(RuntimeError):
    """Cancellation must preserve intent until a worker claim is invalidated."""


class LabDatabaseIdentityError(RuntimeError):
    """The configured SQLite file is not this ledger at a supported version."""


_APPLICATION_ID = 0x52514A42
_SCHEMA_VERSION = 1


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    CHECKPOINTED = "checkpointed"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ControlIntent(StrEnum):
    NONE = "none"
    PAUSE_REQUESTED = "pause_requested"
    CANCEL_REQUESTED = "cancel_requested"


class ShardStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    CHECKPOINTED = "checkpointed"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class LabRecordModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class LabConnectionPragmas(LabRecordModel):
    journal_mode: str
    synchronous: int
    foreign_keys: int
    busy_timeout_ms: int


class LabLeaseRecord(LabRecordModel):
    lease_id: int = Field(ge=1)
    lease_name: str
    owner_id: str
    token: UUID
    fencing_token: int = Field(ge=1)
    acquired_at: datetime
    heartbeat_at: datetime
    expires_at: datetime
    released_at: datetime | None = None


class LabCommandRecord(LabRecordModel):
    request_id: UUID
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    command_type: str
    job_id: UUID
    envelope: LabCommandEnvelope
    receipt: LabCommandReceipt
    received_at: datetime
    applied_at: datetime


class LabJobRecord(LabRecordModel):
    job_id: UUID
    spec: ResearchRunSpec
    spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    job_type: ResearchJobType
    resource_class: ResourceClass
    deadline: datetime
    status: JobStatus
    control_intent: ControlIntent
    version: int = Field(ge=0)
    attempt_count: int = Field(ge=0)
    max_attempts: int = Field(ge=1)
    recoverable: bool
    scheduler_fencing_token: int | None = Field(default=None, ge=1)
    created_at: datetime
    updated_at: datetime


class LabShardRecord(LabRecordModel):
    shard_id: UUID
    job_id: UUID
    shard_index: int = Field(ge=0)
    status: ShardStatus
    version: int = Field(ge=0)
    attempt_count: int = Field(ge=0)
    max_attempts: int = Field(ge=1)
    worker_id: str | None = None
    scheduler_fencing_token: int | None = Field(default=None, ge=1)
    checkpoint_json: str | None = None
    created_at: datetime
    updated_at: datetime


class LabEventRecord(LabRecordModel):
    event_id: int = Field(ge=1)
    job_id: UUID
    request_id: UUID | None = None
    event_type: str
    prior_status: JobStatus | None = None
    new_status: JobStatus
    job_version: int = Field(ge=0)
    reason: str
    scheduler_fencing_token: int | None = Field(default=None, ge=1)
    created_at: datetime


class LabArtifactRecord(LabRecordModel):
    artifact_id: UUID
    job_id: UUID
    shard_id: UUID | None = None
    artifact_type: str
    uri: str
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime


_ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.CHECKPOINTED,
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        }
    ),
    JobStatus.CHECKPOINTED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("ledger timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _dump_time(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _load_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return _utc(parsed)


def _validate_database_identity(
    connection: sqlite3.Connection,
    *,
    allow_unclaimed_empty: bool,
) -> bool:
    application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
    user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if application_id == _APPLICATION_ID:
        if user_version != _SCHEMA_VERSION:
            raise LabDatabaseIdentityError(
                "lab jobs SQLite user_version mismatch: "
                f"expected {_SCHEMA_VERSION}, found {user_version}"
            )
        return False
    if application_id != 0:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite application_id mismatch: "
            f"expected {_APPLICATION_ID}, found {application_id}"
        )
    if user_version != 0:
        raise LabDatabaseIdentityError(
            f"unclaimed SQLite has unsupported user_version {user_version}"
        )
    objects = connection.execute(
        """
        SELECT name FROM sqlite_master
        WHERE name NOT LIKE 'sqlite_%'
          AND type IN ('table', 'index', 'view', 'trigger')
        LIMIT 1
        """
    ).fetchone()
    if not allow_unclaimed_empty or objects is not None:
        detail = "not empty" if objects is not None else "unclaimed"
        raise LabDatabaseIdentityError(f"lab jobs SQLite is {detail}")
    return True


class LabJobReader:
    """Read-only view of committed WAL state; never creates the database."""

    def __init__(self, path: Path, *, busy_timeout_ms: int = 5_000) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{quote(str(self.path.resolve()))}?mode=ro"
        connection = sqlite3.connect(
            uri,
            uri=True,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA query_only = ON")
        try:
            _validate_database_identity(
                connection,
                allow_unclaimed_empty=False,
            )
        except BaseException:
            connection.close()
            raise
        return connection

    @staticmethod
    def _job_from_row(row: sqlite3.Row) -> LabJobRecord:
        try:
            spec = ResearchRunSpec.model_validate_json(str(row["spec_json"]))
            stored_hash = str(row["spec_hash"])
            stored_job_type = ResearchJobType(str(row["job_type"]))
            stored_resource = ResourceClass(str(row["resource_class"]))
            stored_deadline = _load_time(str(row["deadline"]))
            if spec.spec_hash != stored_hash:
                raise ValueError("spec hash mismatch")
            if spec.job_type is not stored_job_type:
                raise ValueError("job_type does not match spec")
            if spec.resource_class is not stored_resource:
                raise ValueError("resource_class does not match spec")
            if spec.deadline != stored_deadline:
                raise ValueError("deadline does not match spec")
            return LabJobRecord(
                job_id=UUID(str(row["job_id"])),
                spec=spec,
                spec_hash=stored_hash,
                job_type=stored_job_type,
                resource_class=stored_resource,
                deadline=stored_deadline,
                status=JobStatus(str(row["status"])),
                control_intent=ControlIntent(str(row["control_intent"])),
                version=int(row["version"]),
                attempt_count=int(row["attempt_count"]),
                max_attempts=int(row["max_attempts"]),
                recoverable=bool(row["recoverable"]),
                scheduler_fencing_token=(
                    int(row["scheduler_fencing_token"])
                    if row["scheduler_fencing_token"] is not None
                    else None
                ),
                created_at=_load_time(str(row["created_at"])),
                updated_at=_load_time(str(row["updated_at"])),
            )
        except Exception as exc:
            if isinstance(exc, InvalidStoredJobError):
                raise
            raise InvalidStoredJobError(f"invalid stored lab job {row['job_id']}: {exc}") from exc

    @staticmethod
    def _lease_from_row(row: sqlite3.Row) -> LabLeaseRecord:
        return LabLeaseRecord(
            lease_id=int(row["lease_id"]),
            lease_name=str(row["lease_name"]),
            owner_id=str(row["owner_id"]),
            token=UUID(str(row["token"])),
            fencing_token=int(row["fencing_token"]),
            acquired_at=_load_time(str(row["acquired_at"])),
            heartbeat_at=_load_time(str(row["heartbeat_at"])),
            expires_at=_load_time(str(row["expires_at"])),
            released_at=(
                _load_time(str(row["released_at"])) if row["released_at"] is not None else None
            ),
        )

    def get_job(self, job_id: UUID) -> LabJobRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM lab_job WHERE job_id = ?",
                (str(job_id),),
            ).fetchone()
        return None if row is None else self._job_from_row(row)

    def get_command(self, request_id: UUID) -> LabCommandRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM lab_command WHERE request_id = ?",
                (str(request_id),),
            ).fetchone()
        if row is None:
            return None
        try:
            envelope = LabCommandEnvelope.model_validate_json(str(row["command_json"]))
            receipt = LabCommandReceipt.model_validate_json(str(row["receipt_json"]))
            if envelope.request_id != request_id or receipt.request_id != request_id:
                raise ValueError("request id mismatch")
            if not (envelope.content_hash == receipt.content_hash == str(row["content_hash"])):
                raise ValueError("content hash mismatch")
            if envelope.command.command_type != str(row["command_type"]):
                raise ValueError("command type mismatch")
            if envelope.command.job_id != UUID(str(row["job_id"])):
                raise ValueError("job id mismatch")
            if receipt.status != str(row["status"]):
                raise ValueError("receipt status mismatch")
            if receipt.reason != str(row["reason"]):
                raise ValueError("receipt reason mismatch")
            return LabCommandRecord(
                request_id=request_id,
                content_hash=envelope.content_hash,
                command_type=envelope.command.command_type,
                job_id=envelope.command.job_id,
                envelope=envelope,
                receipt=receipt,
                received_at=_load_time(str(row["received_at"])),
                applied_at=_load_time(str(row["applied_at"])),
            )
        except Exception as exc:
            raise InvalidStoredJobError(f"invalid stored lab command {request_id}: {exc}") from exc

    def list_events(self, job_id: UUID) -> tuple[LabEventRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM lab_event WHERE job_id = ? ORDER BY event_id",
                (str(job_id),),
            ).fetchall()
        return tuple(
            LabEventRecord(
                event_id=int(row["event_id"]),
                job_id=UUID(str(row["job_id"])),
                request_id=(
                    UUID(str(row["request_id"])) if row["request_id"] is not None else None
                ),
                event_type=str(row["event_type"]),
                prior_status=(
                    JobStatus(str(row["prior_status"])) if row["prior_status"] is not None else None
                ),
                new_status=JobStatus(str(row["new_status"])),
                job_version=int(row["job_version"]),
                reason=str(row["reason"]),
                scheduler_fencing_token=(
                    int(row["scheduler_fencing_token"])
                    if row["scheduler_fencing_token"] is not None
                    else None
                ),
                created_at=_load_time(str(row["created_at"])),
            )
            for row in rows
        )

    def list_leases(self) -> tuple[LabLeaseRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM lab_lease ORDER BY lease_id").fetchall()
        return tuple(self._lease_from_row(row) for row in rows)

    def list_shards(self, job_id: UUID) -> tuple[LabShardRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM lab_shard WHERE job_id = ? ORDER BY shard_index",
                (str(job_id),),
            ).fetchall()
        return tuple(
            LabShardRecord(
                shard_id=UUID(str(row["shard_id"])),
                job_id=UUID(str(row["job_id"])),
                shard_index=int(row["shard_index"]),
                status=ShardStatus(str(row["status"])),
                version=int(row["version"]),
                attempt_count=int(row["attempt_count"]),
                max_attempts=int(row["max_attempts"]),
                worker_id=(str(row["worker_id"]) if row["worker_id"] else None),
                scheduler_fencing_token=(
                    int(row["scheduler_fencing_token"])
                    if row["scheduler_fencing_token"] is not None
                    else None
                ),
                checkpoint_json=(
                    str(row["checkpoint_json"]) if row["checkpoint_json"] is not None else None
                ),
                created_at=_load_time(str(row["created_at"])),
                updated_at=_load_time(str(row["updated_at"])),
            )
            for row in rows
        )

    def list_artifacts(self, job_id: UUID) -> tuple[LabArtifactRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM lab_artifact WHERE job_id = ? ORDER BY created_at, artifact_id",
                (str(job_id),),
            ).fetchall()
        return tuple(
            LabArtifactRecord(
                artifact_id=UUID(str(row["artifact_id"])),
                job_id=UUID(str(row["job_id"])),
                shard_id=(UUID(str(row["shard_id"])) if row["shard_id"] is not None else None),
                artifact_type=str(row["artifact_type"]),
                uri=str(row["uri"]),
                content_hash=str(row["content_hash"]),
                created_at=_load_time(str(row["created_at"])),
            )
            for row in rows
        )

    def execute_for_test(self, statement: str) -> None:
        with self._connect() as connection:
            connection.execute(statement)


class LabJobStore:
    """The scheduler-owned writer for the Strategy Lab SQLite ledger."""

    APPLICATION_ID = _APPLICATION_ID
    SCHEMA_VERSION = _SCHEMA_VERSION
    LEASE_NAME = "strategy-lab-scheduler"

    def __init__(self, path: Path, *, busy_timeout_ms: int = 5_000) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            unclaimed = _validate_database_identity(
                connection,
                allow_unclaimed_empty=True,
            )
            if unclaimed:
                connection.execute(f"PRAGMA application_id = {self.APPLICATION_ID}")
            for statement in _SCHEMA_STATEMENTS:
                connection.execute(statement)
            connection.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION}")
            connection.commit()
            connection.execute("PRAGMA journal_mode = WAL")
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def connection_pragmas(self) -> LabConnectionPragmas:
        with self._connect() as connection:
            return LabConnectionPragmas(
                journal_mode=str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
                synchronous=int(connection.execute("PRAGMA synchronous").fetchone()[0]),
                foreign_keys=int(connection.execute("PRAGMA foreign_keys").fetchone()[0]),
                busy_timeout_ms=int(connection.execute("PRAGMA busy_timeout").fetchone()[0]),
            )

    def acquire_scheduler_lease(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        now: datetime,
    ) -> LabLeaseRecord:
        if not owner_id.strip():
            raise ValueError("owner_id must not be empty")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        acquired_at = _utc(now)
        with self._transaction() as connection:
            active = connection.execute(
                """
                SELECT * FROM lab_lease
                WHERE lease_name = ? AND released_at IS NULL
                ORDER BY lease_id DESC LIMIT 1
                """,
                (self.LEASE_NAME,),
            ).fetchone()
            if active is not None:
                if _load_time(str(active["expires_at"])) > acquired_at:
                    raise SchedulerLeaseUnavailableError(
                        f"scheduler lease is held by {active['owner_id']}"
                    )
                connection.execute(
                    "UPDATE lab_lease SET released_at = ? WHERE lease_id = ?",
                    (_dump_time(acquired_at), int(active["lease_id"])),
                )
            latest = connection.execute(
                "SELECT COALESCE(MAX(fencing_token), 0) FROM lab_lease WHERE lease_name = ?",
                (self.LEASE_NAME,),
            ).fetchone()
            fencing_token = int(latest[0]) + 1
            token = uuid4()
            expires_at = acquired_at + timedelta(seconds=lease_seconds)
            cursor = connection.execute(
                """
                INSERT INTO lab_lease (
                    lease_name, owner_id, token, fencing_token,
                    acquired_at, heartbeat_at, expires_at, released_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    self.LEASE_NAME,
                    owner_id.strip(),
                    str(token),
                    fencing_token,
                    _dump_time(acquired_at),
                    _dump_time(acquired_at),
                    _dump_time(expires_at),
                ),
            )
            lease_id = int(cursor.lastrowid)
        return LabLeaseRecord(
            lease_id=lease_id,
            lease_name=self.LEASE_NAME,
            owner_id=owner_id.strip(),
            token=token,
            fencing_token=fencing_token,
            acquired_at=acquired_at,
            heartbeat_at=acquired_at,
            expires_at=expires_at,
        )

    @staticmethod
    def _validate_lease(
        connection: sqlite3.Connection,
        lease: LabLeaseRecord,
        *,
        now: datetime,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM lab_lease WHERE lease_id = ?",
            (lease.lease_id,),
        ).fetchone()
        current = _utc(now)
        if (
            row is None
            or str(row["token"]) != str(lease.token)
            or int(row["fencing_token"]) != lease.fencing_token
            or row["released_at"] is not None
            or _load_time(str(row["expires_at"])) <= current
        ):
            raise SchedulerLeaseFencedError("scheduler lease is stale or expired")
        active = connection.execute(
            """
            SELECT lease_id FROM lab_lease
            WHERE lease_name = ? AND released_at IS NULL
            ORDER BY fencing_token DESC LIMIT 1
            """,
            (lease.lease_name,),
        ).fetchone()
        if active is None or int(active["lease_id"]) != lease.lease_id:
            raise SchedulerLeaseFencedError("scheduler lease has been superseded")
        return row

    def renew_scheduler_lease(
        self,
        lease: LabLeaseRecord,
        *,
        lease_seconds: int,
        now: datetime,
    ) -> LabLeaseRecord:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        heartbeat_at = _utc(now)
        expires_at = heartbeat_at + timedelta(seconds=lease_seconds)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=heartbeat_at)
            connection.execute(
                """
                UPDATE lab_lease
                SET heartbeat_at = ?, expires_at = ?
                WHERE lease_id = ? AND token = ? AND fencing_token = ?
                """,
                (
                    _dump_time(heartbeat_at),
                    _dump_time(expires_at),
                    lease.lease_id,
                    str(lease.token),
                    lease.fencing_token,
                ),
            )
        return lease.model_copy(update={"heartbeat_at": heartbeat_at, "expires_at": expires_at})

    def release_scheduler_lease(
        self,
        lease: LabLeaseRecord,
        *,
        now: datetime,
    ) -> LabLeaseRecord:
        released_at = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=released_at)
            connection.execute(
                "UPDATE lab_lease SET released_at = ? WHERE lease_id = ?",
                (_dump_time(released_at), lease.lease_id),
            )
        return lease.model_copy(update={"released_at": released_at})

    @staticmethod
    def _insert_event(
        connection: sqlite3.Connection,
        *,
        job_id: UUID,
        request_id: UUID | None,
        event_type: str,
        prior_status: JobStatus | None,
        new_status: JobStatus,
        job_version: int,
        reason: str,
        fencing_token: int | None,
        now: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO lab_event (
                job_id, request_id, event_type, prior_status, new_status,
                job_version, reason, scheduler_fencing_token, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(job_id),
                str(request_id) if request_id is not None else None,
                event_type,
                prior_status.value if prior_status is not None else None,
                new_status.value,
                job_version,
                reason,
                fencing_token,
                _dump_time(now),
            ),
        )

    @staticmethod
    def _load_job_row(
        connection: sqlite3.Connection,
        job_id: UUID,
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM lab_job WHERE job_id = ?",
            (str(job_id),),
        ).fetchone()

    def _transition_in_transaction(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        target_status: JobStatus,
        lease: LabLeaseRecord,
        reason: str,
        now: datetime,
        request_id: UUID | None,
        recoverable: bool | None,
        event_type: str,
        allow_cancel_confirmation: bool = False,
    ) -> sqlite3.Row:
        source = JobStatus(str(row["status"]))
        control_intent = ControlIntent(str(row["control_intent"]))
        if source is JobStatus.RUNNING:
            if control_intent is ControlIntent.CANCEL_REQUESTED and not (
                target_status is JobStatus.CANCELLED and allow_cancel_confirmation
            ):
                raise CancelConfirmationRequiredError(
                    "cancel_requested blocks later lifecycle results"
                )
            if target_status is JobStatus.CANCELLED and (
                control_intent is not ControlIntent.CANCEL_REQUESTED
                or not allow_cancel_confirmation
            ):
                raise CancelConfirmationRequiredError(
                    "running cancellation requires requested confirmation"
                )
        if target_status not in _ALLOWED_TRANSITIONS[source]:
            raise InvalidJobTransitionError(
                f"invalid lab job transition {source.value}->{target_status.value}"
            )
        row_fence = row["scheduler_fencing_token"]
        if source is JobStatus.RUNNING and (
            row_fence is None or int(row_fence) != lease.fencing_token
        ):
            raise SchedulerLeaseFencedError("running job belongs to a different scheduler fence")
        version = int(row["version"]) + 1
        attempt_count = int(row["attempt_count"])
        if source is JobStatus.QUEUED and target_status is JobStatus.RUNNING:
            attempt_count += 1
        next_recoverable = bool(row["recoverable"])
        if target_status is JobStatus.FAILED:
            next_recoverable = bool(recoverable)
        next_fence = row_fence
        if target_status is JobStatus.RUNNING:
            next_fence = lease.fencing_token
        connection.execute(
            """
            UPDATE lab_job
            SET status = ?, control_intent = ?, version = ?, attempt_count = ?,
                recoverable = ?, scheduler_fencing_token = ?, updated_at = ?
            WHERE job_id = ? AND version = ?
            """,
            (
                target_status.value,
                ControlIntent.NONE.value,
                version,
                attempt_count,
                int(next_recoverable),
                next_fence,
                _dump_time(now),
                str(row["job_id"]),
                int(row["version"]),
            ),
        )
        self._insert_event(
            connection,
            job_id=UUID(str(row["job_id"])),
            request_id=request_id,
            event_type=event_type,
            prior_status=source,
            new_status=target_status,
            job_version=version,
            reason=reason,
            fencing_token=lease.fencing_token,
            now=now,
        )
        updated = self._load_job_row(connection, UUID(str(row["job_id"])))
        assert updated is not None
        return updated

    def _set_control_intent_in_transaction(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        control_intent: ControlIntent,
        lease: LabLeaseRecord,
        reason: str,
        now: datetime,
        request_id: UUID,
    ) -> sqlite3.Row:
        status = JobStatus(str(row["status"]))
        row_fence = row["scheduler_fencing_token"]
        if status is JobStatus.RUNNING and (
            row_fence is None or int(row_fence) != lease.fencing_token
        ):
            raise SchedulerLeaseFencedError("running job belongs to a different scheduler fence")
        version = int(row["version"]) + 1
        connection.execute(
            """
            UPDATE lab_job
            SET control_intent = ?, version = ?, updated_at = ?
            WHERE job_id = ? AND version = ?
            """,
            (
                control_intent.value,
                version,
                _dump_time(now),
                str(row["job_id"]),
                int(row["version"]),
            ),
        )
        self._insert_event(
            connection,
            job_id=UUID(str(row["job_id"])),
            request_id=request_id,
            event_type="control_intent_changed",
            prior_status=status,
            new_status=status,
            job_version=version,
            reason=reason,
            fencing_token=lease.fencing_token,
            now=now,
        )
        updated = self._load_job_row(connection, UUID(str(row["job_id"])))
        assert updated is not None
        return updated

    def transition_job(
        self,
        job_id: UUID,
        *,
        expected_version: int,
        target_status: JobStatus,
        lease: LabLeaseRecord,
        reason: str,
        now: datetime,
        recoverable: bool | None = None,
    ) -> LabJobRecord:
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            row = self._load_job_row(connection, job_id)
            if row is None:
                raise KeyError(str(job_id))
            if int(row["version"]) != expected_version:
                raise StaleJobVersionError(
                    f"expected job version {expected_version}, found {row['version']}"
                )
            updated = self._transition_in_transaction(
                connection,
                row,
                target_status=target_status,
                lease=lease,
                reason=reason,
                now=current,
                request_id=None,
                recoverable=recoverable,
                event_type="job_transitioned",
            )
            record = LabJobReader._job_from_row(updated)
        return record

    def confirm_cancelled_job(
        self,
        job_id: UUID,
        *,
        expected_version: int,
        lease: LabLeaseRecord,
        reason: str,
        now: datetime,
    ) -> LabJobRecord:
        """Confirm terminal cancellation after the active worker claim is invalid."""
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            row = self._load_job_row(connection, job_id)
            if row is None:
                raise KeyError(str(job_id))
            if int(row["version"]) != expected_version:
                raise StaleJobVersionError(
                    f"expected job version {expected_version}, found {row['version']}"
                )
            if (
                JobStatus(str(row["status"])) is not JobStatus.RUNNING
                or ControlIntent(str(row["control_intent"])) is not ControlIntent.CANCEL_REQUESTED
            ):
                raise CancelConfirmationRequiredError("job does not have an active cancel request")
            updated = self._transition_in_transaction(
                connection,
                row,
                target_status=JobStatus.CANCELLED,
                lease=lease,
                reason=reason,
                now=current,
                request_id=None,
                recoverable=None,
                event_type="job_cancel_confirmed",
                allow_cancel_confirmation=True,
            )
            record = LabJobReader._job_from_row(updated)
        return record

    @staticmethod
    def _receipt_for_rejection(
        envelope: LabCommandEnvelope,
        *,
        reason: str,
        job_version: int | None,
    ) -> LabCommandReceipt:
        return LabCommandReceipt(
            request_id=envelope.request_id,
            content_hash=envelope.content_hash,
            job_id=envelope.command.job_id,
            status="rejected",
            reason=reason,
            job_version=job_version,
        )

    @staticmethod
    def _record_command(
        connection: sqlite3.Connection,
        envelope: LabCommandEnvelope,
        receipt: LabCommandReceipt,
        *,
        now: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO lab_command (
                request_id, content_hash, command_type, job_id, command_json,
                status, reason, receipt_json, received_at, applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(envelope.request_id),
                envelope.content_hash,
                envelope.command.command_type,
                str(envelope.command.job_id),
                envelope.model_dump_json(),
                receipt.status,
                receipt.reason,
                receipt.model_dump_json(),
                _dump_time(now),
                _dump_time(now),
            ),
        )

    def _apply_existing_or_conflict(
        self,
        connection: sqlite3.Connection,
        envelope: LabCommandEnvelope,
    ) -> LabCommandReceipt | None:
        row = connection.execute(
            "SELECT content_hash, receipt_json FROM lab_command WHERE request_id = ?",
            (str(envelope.request_id),),
        ).fetchone()
        if row is None:
            return None
        if str(row["content_hash"]) != envelope.content_hash:
            raise RequestContentConflictError(
                f"request_id {envelope.request_id} already has different content"
            )
        receipt = LabCommandReceipt.model_validate_json(str(row["receipt_json"]))
        if receipt.content_hash != envelope.content_hash:
            raise InvalidStoredJobError("stored receipt content hash mismatch")
        return receipt

    def apply_command(
        self,
        envelope: LabCommandEnvelope,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabCommandReceipt:
        validated = LabCommandEnvelope.model_validate(envelope)
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            existing = self._apply_existing_or_conflict(connection, validated)
            if existing is not None:
                return existing
            receipt = self._apply_new_command(
                connection,
                validated,
                lease=lease,
                now=current,
            )
            self._record_command(connection, validated, receipt, now=current)
        return receipt

    def _apply_new_command(
        self,
        connection: sqlite3.Connection,
        envelope: LabCommandEnvelope,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabCommandReceipt:
        command = envelope.command
        row = self._load_job_row(connection, command.job_id)
        if isinstance(command, SubmitJobCommand):
            if row is not None:
                return self._receipt_for_rejection(
                    envelope,
                    reason="job_id_reused",
                    job_version=int(row["version"]),
                )
            spec_json = command.spec.model_dump_json(round_trip=True)
            connection.execute(
                """
                INSERT INTO lab_job (
                    job_id, spec_json, spec_hash, job_type, resource_class,
                    deadline, status, control_intent, version, attempt_count,
                    max_attempts, recoverable, scheduler_fencing_token,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 0, NULL, ?, ?)
                """,
                (
                    str(command.job_id),
                    spec_json,
                    command.spec.spec_hash,
                    command.spec.job_type.value,
                    command.spec.resource_class.value,
                    _dump_time(command.spec.deadline),
                    JobStatus.QUEUED.value,
                    ControlIntent.NONE.value,
                    command.max_attempts,
                    _dump_time(now),
                    _dump_time(now),
                ),
            )
            self._insert_event(
                connection,
                job_id=command.job_id,
                request_id=envelope.request_id,
                event_type="job_submitted",
                prior_status=None,
                new_status=JobStatus.QUEUED,
                job_version=0,
                reason="submitted",
                fencing_token=lease.fencing_token,
                now=now,
            )
            return LabCommandReceipt(
                request_id=envelope.request_id,
                content_hash=envelope.content_hash,
                job_id=command.job_id,
                status="applied",
                reason="submitted",
                job_version=0,
            )

        if row is None:
            return self._receipt_for_rejection(
                envelope,
                reason="job_not_found",
                job_version=None,
            )
        version = int(row["version"])
        if version != command.expected_version:
            return self._receipt_for_rejection(
                envelope,
                reason=f"stale_version:{version}",
                job_version=version,
            )
        source = JobStatus(str(row["status"]))
        control_intent = ControlIntent(str(row["control_intent"]))
        target: JobStatus
        if isinstance(command, CancelJobCommand):
            if source not in {
                JobStatus.QUEUED,
                JobStatus.RUNNING,
                JobStatus.CHECKPOINTED,
            }:
                return self._receipt_for_rejection(
                    envelope,
                    reason=f"invalid_state:{source.value}",
                    job_version=version,
                )
            if source is JobStatus.RUNNING:
                updated = self._set_control_intent_in_transaction(
                    connection,
                    row,
                    control_intent=ControlIntent.CANCEL_REQUESTED,
                    lease=lease,
                    reason=command.reason,
                    now=now,
                    request_id=envelope.request_id,
                )
                return LabCommandReceipt(
                    request_id=envelope.request_id,
                    content_hash=envelope.content_hash,
                    job_id=command.job_id,
                    status="applied",
                    reason="cancel_requested",
                    job_version=int(updated["version"]),
                )
            target = JobStatus.CANCELLED
        elif isinstance(command, PauseJobCommand):
            if source is not JobStatus.RUNNING:
                return self._receipt_for_rejection(
                    envelope,
                    reason=f"invalid_state:{source.value}",
                    job_version=version,
                )
            if control_intent is not ControlIntent.NONE:
                return self._receipt_for_rejection(
                    envelope,
                    reason=f"invalid_intent:{control_intent.value}",
                    job_version=version,
                )
            updated = self._set_control_intent_in_transaction(
                connection,
                row,
                control_intent=ControlIntent.PAUSE_REQUESTED,
                lease=lease,
                reason=command.reason,
                now=now,
                request_id=envelope.request_id,
            )
            return LabCommandReceipt(
                request_id=envelope.request_id,
                content_hash=envelope.content_hash,
                job_id=command.job_id,
                status="applied",
                reason="pause_requested",
                job_version=int(updated["version"]),
            )
        elif isinstance(command, ResumeJobCommand):
            if source is JobStatus.RUNNING and control_intent is ControlIntent.PAUSE_REQUESTED:
                updated = self._set_control_intent_in_transaction(
                    connection,
                    row,
                    control_intent=ControlIntent.NONE,
                    lease=lease,
                    reason=command.reason,
                    now=now,
                    request_id=envelope.request_id,
                )
                return LabCommandReceipt(
                    request_id=envelope.request_id,
                    content_hash=envelope.content_hash,
                    job_id=command.job_id,
                    status="applied",
                    reason="pause_withdrawn",
                    job_version=int(updated["version"]),
                )
            if source is not JobStatus.CHECKPOINTED:
                return self._receipt_for_rejection(
                    envelope,
                    reason=f"invalid_state:{source.value}",
                    job_version=version,
                )
            target = JobStatus.RUNNING
        elif isinstance(command, RetryJobCommand):
            if source is not JobStatus.FAILED:
                return self._receipt_for_rejection(
                    envelope,
                    reason=f"invalid_state:{source.value}",
                    job_version=version,
                )
            if not bool(row["recoverable"]):
                return self._receipt_for_rejection(
                    envelope,
                    reason="not_recoverable",
                    job_version=version,
                )
            if int(row["attempt_count"]) >= int(row["max_attempts"]):
                return self._receipt_for_rejection(
                    envelope,
                    reason="attempts_exhausted",
                    job_version=version,
                )
            next_version = version + 1
            connection.execute(
                """
                UPDATE lab_job
                SET status = ?, control_intent = ?, version = ?, recoverable = 0,
                    scheduler_fencing_token = NULL, updated_at = ?
                WHERE job_id = ? AND version = ?
                """,
                (
                    JobStatus.QUEUED.value,
                    ControlIntent.NONE.value,
                    next_version,
                    _dump_time(now),
                    str(command.job_id),
                    version,
                ),
            )
            self._insert_event(
                connection,
                job_id=command.job_id,
                request_id=envelope.request_id,
                event_type="job_retried",
                prior_status=source,
                new_status=JobStatus.QUEUED,
                job_version=next_version,
                reason=command.reason,
                fencing_token=lease.fencing_token,
                now=now,
            )
            return LabCommandReceipt(
                request_id=envelope.request_id,
                content_hash=envelope.content_hash,
                job_id=command.job_id,
                status="applied",
                reason="retried",
                job_version=next_version,
            )
        else:  # pragma: no cover - discriminated union is exhaustive
            raise TypeError(type(command).__name__)

        action_reason = {
            "cancel": "cancelled",
            "resume": "resumed",
        }[command.command_type]
        updated = self._transition_in_transaction(
            connection,
            row,
            target_status=target,
            lease=lease,
            reason=command.reason,
            now=now,
            request_id=envelope.request_id,
            recoverable=None,
            event_type=f"job_{action_reason}",
        )
        next_version = int(updated["version"])
        return LabCommandReceipt(
            request_id=envelope.request_id,
            content_hash=envelope.content_hash,
            job_id=command.job_id,
            status="applied",
            reason=action_reason,
            job_version=next_version,
        )

    def recover_expired_jobs(
        self,
        lease: LabLeaseRecord,
        *,
        now: datetime,
    ) -> tuple[LabJobRecord, ...]:
        current = _utc(now)
        recovered: list[LabJobRecord] = []
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            rows = connection.execute(
                """
                SELECT * FROM lab_job
                WHERE status = ?
                  AND (
                    scheduler_fencing_token IS NULL
                    OR scheduler_fencing_token <> ?
                  )
                ORDER BY created_at, job_id
                """,
                (JobStatus.RUNNING.value, lease.fencing_token),
            ).fetchall()
            for row in rows:
                version = int(row["version"]) + 1
                intent = ControlIntent(str(row["control_intent"]))
                target_status = (
                    JobStatus.CANCELLED
                    if intent is ControlIntent.CANCEL_REQUESTED
                    else JobStatus.CHECKPOINTED
                )
                connection.execute(
                    """
                    UPDATE lab_job
                    SET status = ?, control_intent = ?, version = ?,
                        scheduler_fencing_token = ?, updated_at = ?
                    WHERE job_id = ? AND version = ?
                    """,
                    (
                        target_status.value,
                        ControlIntent.NONE.value,
                        version,
                        lease.fencing_token,
                        _dump_time(current),
                        str(row["job_id"]),
                        int(row["version"]),
                    ),
                )
                self._insert_event(
                    connection,
                    job_id=UUID(str(row["job_id"])),
                    request_id=None,
                    event_type="lease_recovered",
                    prior_status=JobStatus.RUNNING,
                    new_status=target_status,
                    job_version=version,
                    reason=(
                        "scheduler lease expired after cancel request"
                        if target_status is JobStatus.CANCELLED
                        else "scheduler lease expired"
                    ),
                    fencing_token=lease.fencing_token,
                    now=current,
                )
                updated = self._load_job_row(
                    connection,
                    UUID(str(row["job_id"])),
                )
                assert updated is not None
                recovered.append(LabJobReader._job_from_row(updated))
        return tuple(recovered)


_STATUS_VALUES = ",".join(f"'{status.value}'" for status in JobStatus)
_CONTROL_INTENT_VALUES = ",".join(f"'{intent.value}'" for intent in ControlIntent)
_SHARD_STATUS_VALUES = ",".join(f"'{status.value}'" for status in ShardStatus)
_SCHEMA_STATEMENTS = (
    f"""
    CREATE TABLE IF NOT EXISTS lab_job (
        job_id TEXT PRIMARY KEY,
        spec_json TEXT NOT NULL,
        spec_hash TEXT NOT NULL,
        job_type TEXT NOT NULL,
        resource_class TEXT NOT NULL,
        deadline TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ({_STATUS_VALUES})),
        control_intent TEXT NOT NULL CHECK (
            control_intent IN ({_CONTROL_INTENT_VALUES})
        ),
        version INTEGER NOT NULL CHECK (version >= 0),
        attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
        max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
        recoverable INTEGER NOT NULL CHECK (recoverable IN (0, 1)),
        scheduler_fencing_token INTEGER,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS lab_command (
        request_id TEXT PRIMARY KEY,
        content_hash TEXT NOT NULL,
        command_type TEXT NOT NULL,
        job_id TEXT NOT NULL,
        command_json TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('applied', 'rejected')),
        reason TEXT NOT NULL,
        receipt_json TEXT NOT NULL,
        received_at TEXT NOT NULL,
        applied_at TEXT NOT NULL
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS lab_shard (
        shard_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL REFERENCES lab_job(job_id) ON DELETE CASCADE,
        shard_index INTEGER NOT NULL CHECK (shard_index >= 0),
        status TEXT NOT NULL CHECK (status IN ({_SHARD_STATUS_VALUES})),
        version INTEGER NOT NULL CHECK (version >= 0),
        attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
        max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
        worker_id TEXT,
        scheduler_fencing_token INTEGER,
        checkpoint_json TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE (job_id, shard_index)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS lab_event (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT NOT NULL REFERENCES lab_job(job_id) ON DELETE CASCADE,
        request_id TEXT,
        event_type TEXT NOT NULL,
        prior_status TEXT CHECK (prior_status IS NULL OR prior_status IN ({_STATUS_VALUES})),
        new_status TEXT NOT NULL CHECK (new_status IN ({_STATUS_VALUES})),
        job_version INTEGER NOT NULL CHECK (job_version >= 0),
        reason TEXT NOT NULL,
        scheduler_fencing_token INTEGER,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS lab_lease (
        lease_id INTEGER PRIMARY KEY AUTOINCREMENT,
        lease_name TEXT NOT NULL,
        owner_id TEXT NOT NULL,
        token TEXT NOT NULL UNIQUE,
        fencing_token INTEGER NOT NULL CHECK (fencing_token >= 1),
        acquired_at TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        released_at TEXT,
        UNIQUE (lease_name, fencing_token)
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ux_lab_lease_active
    ON lab_lease(lease_name) WHERE released_at IS NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS lab_artifact (
        artifact_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL REFERENCES lab_job(job_id) ON DELETE CASCADE,
        shard_id TEXT REFERENCES lab_shard(shard_id) ON DELETE SET NULL,
        artifact_type TEXT NOT NULL,
        uri TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_lab_job_status ON lab_job(status, deadline)",
    "CREATE INDEX IF NOT EXISTS ix_lab_event_job ON lab_event(job_id, event_id)",
    "CREATE INDEX IF NOT EXISTS ix_lab_artifact_job ON lab_artifact(job_id, created_at)",
)
