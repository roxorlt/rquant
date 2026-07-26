"""SQLite single-writer ledger for durable Strategy Lab jobs."""

from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from urllib.parse import quote
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from rquant.lab_artifact_protocol import (
    LabArtifactCommitEnvelope,
    LabArtifactCommitReceipt,
)
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
from rquant.lab_shard_protocol import (
    LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE,
    LAB_SHARD_DURATION_MS_MIN,
    LAB_SHARD_THROUGHPUT_MAX_EXCLUSIVE,
    SQLITE_SIGNED_INTEGER_MAX,
    LabReportReceipt,
    LabShardClaim,
    LabShardDefinition,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabShardTelemetry,
    LabShardWorkPlan,
    LabWorkerReport,
    LabWorkerStopped,
)
from rquant.research_run_spec import (
    ResearchJobType,
    ResearchRunSpec,
    ResourceClass,
)

if TYPE_CHECKING:
    from rquant.lab_artifacts import LabArtifactIndexEvidence, LabVerifiedSealedBinding
    from rquant.lab_eta import LabEtaEstimate, LabEtaInput


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


class ShardPlanConflictError(RuntimeError):
    """A job was already bound to a different immutable shard plan."""


class ArtifactCommitDeadlineExpiredError(RuntimeError):
    """A staged artifact success crossed its job deadline before commit."""


_APPLICATION_ID = 0x52514A42
_LEGACY_SCHEMA_VERSION = 1
_V2_SCHEMA_VERSION = 2
_V3_SCHEMA_VERSION = 3
_PREVIOUS_SCHEMA_VERSION = 4
_SCHEMA_VERSION = 5
RESULT_CONTRACT_VERSION = "p1.4a-telemetry-v1"
COMPLETE_RESULT_CONTRACT_VERSION = "p1.4b-complete-result-v1"
LAB_ETA_COMPLETED_LIMIT_MAX = 256
_EMPTY_PAYLOAD_JSON = "{}"
_EMPTY_PAYLOAD_HASH = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
_LEGACY_PLAN_HASH = "0" * 64
_ATTEMPTS_EXHAUSTED_FAILURE_JSON = '{"reason":"attempts_exhausted"}'
_PARENT_ATTEMPTS_EXHAUSTED_FAILURE_JSON = '{"reason":"parent_failed_attempts_exhausted"}'
_PARENT_RECOVERABLE_FAILURE_JSON = '{"reason":"parent_failed_recoverable"}'
_DEADLINE_EXCEEDED_FAILURE_JSON = '{"reason":"deadline_exceeded"}'


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


class LabResultState(StrEnum):
    PENDING = "pending"
    READY = "ready"
    SEALED = "sealed"
    LEGACY_UNSEALED = "legacy_unsealed"


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
    receipt_job_version: int | None = Field(ge=0)
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
    result_contract_version: str | None = Field(default=None, min_length=1)
    requires_complete_result: bool
    result_state: LabResultState
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
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    adapter_id: str
    adapter_version: str
    payload_json: str
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    phase: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]*$")
    work_unit_name: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]*$")
    work_units: int | None = Field(
        default=None,
        strict=True,
        ge=1,
        le=SQLITE_SIGNED_INTEGER_MAX,
    )
    static_duration_ms: int | None = Field(
        default=None,
        strict=True,
        ge=1,
        le=SQLITE_SIGNED_INTEGER_MAX,
    )
    duration_ms: float | None = Field(
        default=None,
        ge=LAB_SHARD_DURATION_MS_MIN,
        lt=LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE,
        allow_inf_nan=False,
    )
    throughput_units_per_second: float | None = Field(
        default=None,
        gt=0,
        lt=LAB_SHARD_THROUGHPUT_MAX_EXCLUSIVE,
        allow_inf_nan=False,
    )
    completion_sequence: int | None = Field(default=None, strict=True, ge=1)
    worker_id: str | None = None
    scheduler_fencing_token: int | None = Field(default=None, ge=1)
    claim_token: UUID | None = None
    claim_generation: int = Field(ge=0)
    claimed_at: datetime | None = None
    heartbeat_at: datetime | None = None
    lease_expires_at: datetime | None = None
    result_manifest_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    failure_json: str | None = None
    finished_at: datetime | None = None
    checkpoint_json: str | None = None
    created_at: datetime
    updated_at: datetime

    @property
    def work_plan(self) -> LabShardWorkPlan | None:
        values = (self.phase, self.work_unit_name, self.work_units, self.static_duration_ms)
        if all(value is None for value in values):
            return None
        return LabShardWorkPlan(
            phase=self.phase,
            work_unit_name=self.work_unit_name,
            work_units=self.work_units,
            static_duration_ms=self.static_duration_ms,
        )

    @property
    def telemetry(self) -> LabShardTelemetry | None:
        plan = self.work_plan
        if self.duration_ms is None and self.throughput_units_per_second is None:
            return None
        return LabShardTelemetry(
            **plan.model_dump() if plan is not None else {},
            duration_ms=self.duration_ms,
            throughput_units_per_second=self.throughput_units_per_second,
        )


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


class LabWorkerReportRecord(LabRecordModel):
    report: LabWorkerReport
    receipt: LabReportReceipt
    claim_generation: int = Field(ge=1)
    scheduler_fencing_token: int = Field(ge=1)
    received_at: datetime
    applied_at: datetime


class LabArtifactCommitRecord(LabRecordModel):
    envelope: LabArtifactCommitEnvelope
    receipt: LabArtifactCommitReceipt
    received_at: datetime
    applied_at: datetime


class LabStagedArtifactCommit:
    """A closed-surface SQLite transaction awaiting artifact exit verification."""

    __slots__ = (
        "_connection",
        "_closed",
        "_lease_identity",
        "_precommit_validator",
        "receipt",
    )

    def __init__(
        self,
        connection: sqlite3.Connection,
        receipt: LabArtifactCommitReceipt,
        *,
        lease: LabLeaseRecord,
        precommit_validator: Callable[[LabLeaseRecord, datetime], None],
    ) -> None:
        self._connection = connection
        self._closed = False
        self._lease_identity = (
            lease.lease_id,
            lease.lease_name,
            lease.owner_id,
            lease.token,
            lease.fencing_token,
        )
        self._precommit_validator = precommit_validator
        self.receipt = receipt

    @staticmethod
    def _raise_lifecycle_errors(label: str, errors: list[BaseException]) -> None:
        if len(errors) == 1:
            raise errors[0]
        raise BaseExceptionGroup(label, errors)

    def _rollback_and_close(self, primary: BaseException | None = None) -> None:
        errors = [primary] if primary is not None else []
        try:
            self._connection.rollback()
        except BaseException as exc:
            errors.append(exc)
        try:
            self._connection.close()
        except BaseException as exc:
            errors.append(exc)
        self._closed = True
        if errors:
            self._raise_lifecycle_errors("staged artifact transaction rollback failed", errors)

    def commit(
        self,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabArtifactCommitReceipt:
        if self._closed:
            raise RuntimeError("artifact commit stage is already closed")
        final_lease_identity = (
            lease.lease_id,
            lease.lease_name,
            lease.owner_id,
            lease.token,
            lease.fencing_token,
        )
        if final_lease_identity != self._lease_identity:
            self._rollback_and_close(
                SchedulerLeaseFencedError(
                    "staged artifact commit lease identity changed before commit"
                )
            )
        try:
            self._precommit_validator(lease, _utc(now))
        except BaseException as exc:
            self._rollback_and_close(exc)
        try:
            self._connection.commit()
        except BaseException as exc:
            self._rollback_and_close(exc)
        try:
            self._connection.close()
        except BaseException:
            self._closed = True
            raise
        self._closed = True
        return self.receipt

    def rollback(self) -> None:
        if self._closed:
            return
        self._rollback_and_close()


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
    if value.tzinfo is None:
        raise ValueError("ledger timestamps must be timezone-aware")
    try:
        offset = value.utcoffset()
    except (OverflowError, ValueError) as exc:
        raise ValueError("ledger timestamp is outside the UTC datetime domain") from exc
    if offset is None:
        raise ValueError("ledger timestamps must be timezone-aware")
    try:
        return value.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise ValueError("ledger timestamp is outside the UTC datetime domain") from exc


def _dump_time(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _load_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return _utc(parsed)


def _strict_sqlite_int(
    value: object,
    *,
    field: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if type(value) is not int:
        raise InvalidStoredJobError(
            f"{field} must be a SQLite integer, found {type(value).__name__}"
        )
    if minimum is not None and value < minimum:
        raise InvalidStoredJobError(f"{field} must be >= {minimum}, found {value}")
    if maximum is not None and value > maximum:
        raise InvalidStoredJobError(f"{field} must be <= {maximum}, found {value}")
    return value


def _strict_nullable_sqlite_int(
    value: object,
    *,
    field: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    if value is None:
        return None
    return _strict_sqlite_int(value, field=field, minimum=minimum, maximum=maximum)


def _strict_nullable_sqlite_real(
    value: object,
    *,
    field: str,
    positive: bool = False,
    minimum_inclusive: float | None = None,
    maximum_exclusive: float | None = None,
) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidStoredJobError(f"{field} must be a SQLite real, found {type(value).__name__}")
    converted = float(value)
    if not math.isfinite(converted) or (positive and converted <= 0):
        qualifier = "finite and positive" if positive else "finite"
        raise InvalidStoredJobError(f"{field} must be {qualifier}, found {converted}")
    if minimum_inclusive is not None and converted < minimum_inclusive:
        raise InvalidStoredJobError(f"{field} must be >= {minimum_inclusive}, found {converted}")
    if maximum_exclusive is not None and converted >= maximum_exclusive:
        raise InvalidStoredJobError(f"{field} must be < {maximum_exclusive}, found {converted}")
    return converted


def _strict_sqlite_bool(value: object, *, field: str) -> bool:
    integer = _strict_sqlite_int(value, field=field)
    if integer not in {0, 1}:
        raise InvalidStoredJobError(f"{field} must be SQLite integer 0 or 1, found {integer}")
    return bool(integer)


def _command_record_from_row(
    row: sqlite3.Row,
    *,
    expected_request_id: UUID | None = None,
) -> LabCommandRecord:
    stored_request = str(row["request_id"])
    try:
        request_id = UUID(stored_request)
        envelope = LabCommandEnvelope.model_validate_json(str(row["command_json"]))
        receipt = LabCommandReceipt.model_validate_json(str(row["receipt_json"]))
        content_hash = str(row["content_hash"])
        command_type = str(row["command_type"])
        job_id = UUID(str(row["job_id"]))
        status = str(row["status"])
        reason = str(row["reason"])
        receipt_job_version = _strict_nullable_sqlite_int(
            row["receipt_job_version"],
            field="lab_command.receipt_job_version",
            minimum=0,
        )
        if expected_request_id is not None and request_id != expected_request_id:
            raise ValueError("request id does not match lookup key")
        if not (envelope.request_id == receipt.request_id == request_id):
            raise ValueError("request id mismatch")
        if not (envelope.content_hash == receipt.content_hash == content_hash):
            raise ValueError("content hash mismatch")
        if envelope.command.command_type != command_type:
            raise ValueError("command type mismatch")
        if not (envelope.command.job_id == receipt.job_id == job_id):
            raise ValueError("job id mismatch")
        if receipt.status != status:
            raise ValueError("receipt status mismatch")
        if receipt.reason != reason:
            raise ValueError("receipt reason mismatch")
        if receipt.job_version != receipt_job_version:
            raise ValueError("receipt job version mismatch")
        return LabCommandRecord(
            request_id=request_id,
            content_hash=content_hash,
            command_type=command_type,
            job_id=job_id,
            envelope=envelope,
            receipt=receipt,
            receipt_job_version=receipt_job_version,
            received_at=_load_time(str(row["received_at"])),
            applied_at=_load_time(str(row["applied_at"])),
        )
    except Exception as exc:
        raise InvalidStoredJobError(f"invalid stored lab command {stored_request}: {exc}") from exc


def _receipt_job_version_from_json(payload: str) -> int | None:
    return LabCommandReceipt.model_validate_json(payload).job_version


def _worker_report_record_from_row(
    row: sqlite3.Row,
    *,
    expected_report_id: UUID | None = None,
) -> LabWorkerReportRecord:
    stored_id = str(row["report_id"])
    try:
        report = LabWorkerReport.model_validate_json(str(row["report_json"]))
        receipt = LabReportReceipt.model_validate_json(str(row["receipt_json"]))
        report_id = UUID(stored_id)
        content_hash = str(row["content_hash"])
        job_id = UUID(str(row["job_id"]))
        shard_id = UUID(str(row["shard_id"]))
        report_type = str(row["report_type"])
        claim_generation = _strict_sqlite_int(
            row["claim_generation"],
            field="lab_worker_report.claim_generation",
            minimum=1,
        )
        scheduler_fencing_token = _strict_sqlite_int(
            row["scheduler_fencing_token"],
            field="lab_worker_report.scheduler_fencing_token",
            minimum=1,
        )
        if expected_report_id is not None and report_id != expected_report_id:
            raise ValueError("report id does not match lookup key")
        if not (report.report_id == receipt.report_id == report_id):
            raise ValueError("report id mismatch")
        if not (report.content_hash == receipt.content_hash == content_hash):
            raise ValueError("content hash mismatch")
        if not (report.job_id == receipt.job_id == job_id):
            raise ValueError("job id mismatch")
        if not (report.shard_id == receipt.shard_id == shard_id):
            raise ValueError("shard id mismatch")
        if report.body.report_type != report_type:
            raise ValueError("report type mismatch")
        if report.claim_generation != claim_generation:
            raise ValueError("claim generation mismatch")
        if report.scheduler_fencing_token != scheduler_fencing_token:
            raise ValueError("scheduler fencing token mismatch")
        if receipt.status != str(row["status"]):
            raise ValueError("receipt status mismatch")
        if receipt.reason != str(row["reason"]):
            raise ValueError("receipt reason mismatch")
        return LabWorkerReportRecord(
            report=report,
            receipt=receipt,
            claim_generation=claim_generation,
            scheduler_fencing_token=scheduler_fencing_token,
            received_at=_load_time(str(row["received_at"])),
            applied_at=_load_time(str(row["applied_at"])),
        )
    except Exception as exc:
        if isinstance(exc, InvalidStoredJobError):
            raise
        raise InvalidStoredJobError(f"invalid stored worker report {stored_id}: {exc}") from exc


def _canonical_model_json(model: BaseModel) -> str:
    return json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _artifact_commit_record_from_row(
    row: sqlite3.Row,
    *,
    expected_request_id: UUID | None = None,
) -> LabArtifactCommitRecord:
    stored_id = str(row["request_id"])
    try:
        request_id = UUID(stored_id)
        envelope = LabArtifactCommitEnvelope.model_validate_json(str(row["commit_json"]))
        receipt = LabArtifactCommitReceipt.model_validate_json(str(row["receipt_json"]))
        if expected_request_id is not None and request_id != expected_request_id:
            raise ValueError("artifact commit request id does not match lookup key")
        if not (envelope.request_id == receipt.request_id == request_id):
            raise ValueError("artifact commit request id mismatch")
        if not (envelope.content_hash == receipt.content_hash == str(row["content_hash"])):
            raise ValueError("artifact commit content hash mismatch")
        if not (envelope.commit.job_id == receipt.job_id == UUID(str(row["job_id"]))):
            raise ValueError("artifact commit job id mismatch")
        if receipt.status != str(row["status"]):
            raise ValueError("artifact commit receipt status mismatch")
        if receipt.reason != str(row["reason"]):
            raise ValueError("artifact commit receipt reason mismatch")
        version = _strict_nullable_sqlite_int(
            row["receipt_job_version"],
            field="lab_artifact_commit.receipt_job_version",
            minimum=0,
        )
        if receipt.job_version != version:
            raise ValueError("artifact commit receipt version mismatch")
        if _canonical_model_json(envelope) != str(row["commit_json"]):
            raise ValueError("artifact commit JSON is not canonical")
        if _canonical_model_json(receipt) != str(row["receipt_json"]):
            raise ValueError("artifact commit receipt JSON is not canonical")
        return LabArtifactCommitRecord(
            envelope=envelope,
            receipt=receipt,
            received_at=_load_time(str(row["received_at"])),
            applied_at=_load_time(str(row["applied_at"])),
        )
    except Exception as exc:
        if isinstance(exc, InvalidStoredJobError):
            raise
        raise InvalidStoredJobError(f"invalid stored artifact commit {stored_id}: {exc}") from exc


def _validate_v2_schema(connection: sqlite3.Connection) -> None:
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(lab_command)").fetchall()
    }
    if "receipt_job_version" not in columns:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v2 is missing lab_command.receipt_job_version"
        )


def _shard_primary_key_columns(connection: sqlite3.Connection) -> tuple[str, ...]:
    rows = connection.execute("PRAGMA table_info(lab_shard)").fetchall()
    return tuple(
        str(row[1])
        for row in sorted(
            rows,
            key=lambda row: _strict_sqlite_int(
                row[5], field="lab_shard.primary_key_position", minimum=0
            ),
        )
        if _strict_sqlite_int(row[5], field="lab_shard.primary_key_position", minimum=0) > 0
    )


def _validate_v3_schema(connection: sqlite3.Connection) -> None:
    _validate_v2_schema(connection)
    shard_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(lab_shard)").fetchall()
    }
    required_shard_columns = {
        "plan_hash",
        "adapter_id",
        "adapter_version",
        "payload_json",
        "payload_hash",
        "claim_token",
        "claim_generation",
        "claimed_at",
        "heartbeat_at",
        "lease_expires_at",
        "result_manifest_hash",
        "failure_json",
        "finished_at",
    }
    missing = sorted(required_shard_columns - shard_columns)
    if missing:
        raise LabDatabaseIdentityError(
            f"lab jobs SQLite v3 is missing lab_shard columns: {', '.join(missing)}"
        )
    shard_primary_key = _shard_primary_key_columns(connection)
    if shard_primary_key != ("job_id", "shard_id"):
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v3 lab_shard primary key must be (job_id, shard_id)"
        )
    report_table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'lab_worker_report'"
    ).fetchone()
    if report_table is None:
        raise LabDatabaseIdentityError("lab jobs SQLite v3 is missing lab_worker_report")
    report_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(lab_worker_report)").fetchall()
    }
    required_report_columns = {
        "report_id",
        "content_hash",
        "job_id",
        "shard_id",
        "report_type",
        "report_json",
        "status",
        "reason",
        "receipt_json",
        "claim_generation",
        "scheduler_fencing_token",
        "received_at",
        "applied_at",
    }
    missing_report_columns = sorted(required_report_columns - report_columns)
    if missing_report_columns:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v3 is missing lab_worker_report columns: "
            + ", ".join(missing_report_columns)
        )
    state_table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'lab_scheduler_state'"
    ).fetchone()
    if state_table is None:
        raise LabDatabaseIdentityError("lab jobs SQLite v3 is missing lab_scheduler_state")
    state_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(lab_scheduler_state)").fetchall()
    }
    required_state_columns = {
        "state_key",
        "claim_cursor_created_at",
        "claim_cursor_job_id",
        "updated_at",
    }
    missing_state_columns = sorted(required_state_columns - state_columns)
    if missing_state_columns:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v3 is missing lab_scheduler_state columns: "
            + ", ".join(missing_state_columns)
        )


def _canonical_index_predicate(sql: str) -> str | None:
    tokens = sql.strip().rstrip(";").split()
    where_positions = tuple(
        position for position, token in enumerate(tokens) if token.upper() == "WHERE"
    )
    if len(where_positions) != 1:
        return None
    return "".join(tokens[where_positions[0] + 1 :])


def _validate_v4_index(
    connection: sqlite3.Connection,
    *,
    name: str,
    unique: bool,
    partial: bool,
    key_columns: tuple[tuple[str, bool], ...],
    predicate: str | None,
) -> None:
    matching = tuple(
        row
        for row in connection.execute("PRAGMA index_list(lab_shard)").fetchall()
        if str(row[1]) == name
    )
    if len(matching) != 1:
        raise LabDatabaseIdentityError(f"lab jobs SQLite v4 telemetry index {name} is missing")
    index_row = matching[0]
    actual_unique = _strict_sqlite_int(index_row[2], field=f"{name}.unique", minimum=0)
    actual_partial = _strict_sqlite_int(index_row[4], field=f"{name}.partial", minimum=0)
    if actual_unique != int(unique) or str(index_row[3]) != "c" or actual_partial != int(partial):
        raise LabDatabaseIdentityError(
            f"lab jobs SQLite v4 telemetry index {name} has invalid identity flags"
        )

    xinfo = connection.execute(f'PRAGMA index_xinfo("{name}")').fetchall()
    actual_keys = tuple(
        row for row in xinfo if _strict_sqlite_int(row[5], field=f"{name}.key", minimum=0) == 1
    )
    if len(actual_keys) != len(key_columns):
        raise LabDatabaseIdentityError(
            f"lab jobs SQLite v4 telemetry index {name} has invalid key column count"
        )
    for position, (row, expected) in enumerate(zip(actual_keys, key_columns, strict=True)):
        expected_name, expected_desc = expected
        sequence = _strict_sqlite_int(row[0], field=f"{name}.seqno", minimum=0)
        column_id = _strict_sqlite_int(row[1], field=f"{name}.cid")
        descending = _strict_sqlite_int(row[3], field=f"{name}.desc", minimum=0)
        if (
            sequence != position
            or column_id < 0
            or row[2] is None
            or str(row[2]) != expected_name
            or descending != int(expected_desc)
            or str(row[4]).upper() != "BINARY"
        ):
            raise LabDatabaseIdentityError(
                f"lab jobs SQLite v4 telemetry index {name} has invalid key structure"
            )

    sql_row = connection.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type = 'index' AND tbl_name = 'lab_shard' AND name = ?
        """,
        (name,),
    ).fetchone()
    if sql_row is None or sql_row[0] is None:
        raise LabDatabaseIdentityError(
            f"lab jobs SQLite v4 telemetry index {name} has no explicit DDL"
        )
    if _canonical_index_predicate(str(sql_row[0])) != predicate:
        raise LabDatabaseIdentityError(
            f"lab jobs SQLite v4 telemetry index {name} has invalid partial predicate"
        )


def _validate_v4_schema(connection: sqlite3.Connection) -> None:
    _validate_v3_schema(connection)
    job_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(lab_job)").fetchall()
    }
    if "result_contract_version" not in job_columns:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v4 is missing lab_job.result_contract_version"
        )
    shard_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(lab_shard)").fetchall()
    }
    required = {
        "phase",
        "work_unit_name",
        "work_units",
        "static_duration_ms",
        "duration_ms",
        "throughput_units_per_second",
        "completion_sequence",
    }
    missing = sorted(required - shard_columns)
    if missing:
        raise LabDatabaseIdentityError(
            f"lab jobs SQLite v4 is missing lab_shard columns: {', '.join(missing)}"
        )
    _validate_v4_index(
        connection,
        name="ix_lab_shard_job_completion_sequence",
        unique=True,
        partial=True,
        key_columns=(("job_id", False), ("completion_sequence", True)),
        predicate="status='succeeded'ANDcompletion_sequenceISNOTNULL",
    )
    _validate_v4_index(
        connection,
        name="ix_lab_shard_job_status_index",
        unique=False,
        partial=False,
        key_columns=(("job_id", False), ("status", False), ("shard_index", False)),
        predicate=None,
    )


def _canonical_table_sql(sql: str) -> str:
    canonical = "".join(sql.split()).lower()
    return canonical.replace("ifnotexists", "")


def _validate_v5_table_sql(
    connection: sqlite3.Connection,
    *,
    table: str,
    expected: str,
) -> None:
    row = connection.execute(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    if (
        row is None
        or row[0] is None
        or _canonical_table_sql(str(row[0])) != _canonical_table_sql(expected)
    ):
        raise LabDatabaseIdentityError(f"lab jobs SQLite v5 table {table} has invalid constraints")


def _validate_v5_column_identity(
    connection: sqlite3.Connection,
    *,
    table: str,
    column: str,
    declared_type: str,
    not_null: bool,
    primary_key_position: int,
    default: str | None,
) -> None:
    rows = {
        str(row[1]): row for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    }
    row = rows.get(column)
    if row is None:
        raise LabDatabaseIdentityError(f"lab jobs SQLite v5 table {table} is missing {column}")
    actual = (
        str(row[2]).upper(),
        _strict_sqlite_int(row[3], field=f"{table}.{column}.notnull", minimum=0),
        _strict_sqlite_int(row[5], field=f"{table}.{column}.pk", minimum=0),
        str(row[4]) if row[4] is not None else None,
    )
    expected = (
        declared_type.upper(),
        int(not_null),
        primary_key_position,
        default,
    )
    if actual != expected:
        raise LabDatabaseIdentityError(
            f"lab jobs SQLite v5 table {table} column {column} has invalid constraints"
        )


def _v5_index_identities(
    connection: sqlite3.Connection,
    *,
    table: str,
) -> set[tuple[bool, str, bool, tuple[str, ...]]]:
    identities: set[tuple[bool, str, bool, tuple[str, ...]]] = set()
    for row in connection.execute(f'PRAGMA index_list("{table}")').fetchall():
        name = str(row[1])
        columns = tuple(
            str(info[2]) for info in connection.execute(f'PRAGMA index_info("{name}")').fetchall()
        )
        identities.add(
            (
                bool(_strict_sqlite_int(row[2], field=f"{name}.unique", minimum=0)),
                str(row[3]),
                bool(_strict_sqlite_int(row[4], field=f"{name}.partial", minimum=0)),
                columns,
            )
        )
    return identities


def _validate_v5_key_and_foreign_key_constraints(
    connection: sqlite3.Connection,
) -> None:
    commit_indexes = _v5_index_identities(connection, table="lab_artifact_commit")
    if commit_indexes != {(True, "pk", False, ("request_id",))}:
        raise LabDatabaseIdentityError("lab jobs SQLite v5 artifact commit primary key is invalid")
    result_indexes = _v5_index_identities(
        connection,
        table="lab_job_result_artifact",
    )
    if result_indexes != {
        (True, "pk", False, ("job_id",)),
        (True, "u", False, ("commit_request_id",)),
    }:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v5 result artifact primary/unique keys are invalid"
        )
    foreign_keys = {
        (
            str(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]).upper(),
            str(row[6]).upper(),
            str(row[7]).upper(),
        )
        for row in connection.execute("PRAGMA foreign_key_list(lab_job_result_artifact)").fetchall()
    }
    if foreign_keys != {
        (
            "lab_job",
            "job_id",
            "job_id",
            "NO ACTION",
            "RESTRICT",
            "NONE",
        ),
        (
            "lab_artifact_commit",
            "commit_request_id",
            "request_id",
            "NO ACTION",
            "RESTRICT",
            "NONE",
        ),
    }:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v5 result artifact foreign keys are invalid"
        )


def _validate_v5_schema(connection: sqlite3.Connection) -> None:
    _validate_v4_schema(connection)
    _validate_v5_table_sql(
        connection,
        table="lab_job",
        expected=_V5_JOB_TABLE_STATEMENT,
    )
    _validate_v5_table_sql(
        connection,
        table="lab_artifact_commit",
        expected=_V5_ARTIFACT_COMMIT_TABLE_STATEMENT,
    )
    _validate_v5_table_sql(
        connection,
        table="lab_job_result_artifact",
        expected=_V5_RESULT_ARTIFACT_TABLE_STATEMENT,
    )
    job_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(lab_job)").fetchall()
    }
    required_job_columns = {"result_state", "requires_complete_result"}
    missing_job_columns = sorted(required_job_columns - job_columns)
    if missing_job_columns:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v5 is missing lab_job columns: " + ", ".join(missing_job_columns)
        )
    required_tables = {"lab_artifact_commit", "lab_job_result_artifact"}
    existing_tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    missing = sorted(required_tables - existing_tables)
    if missing:
        raise LabDatabaseIdentityError(
            f"lab jobs SQLite v5 is missing tables: {', '.join(missing)}"
        )
    required_commit_columns = {
        "request_id",
        "content_hash",
        "job_id",
        "commit_json",
        "status",
        "reason",
        "receipt_json",
        "receipt_job_version",
        "received_at",
        "applied_at",
    }
    commit_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(lab_artifact_commit)").fetchall()
    }
    if commit_columns != required_commit_columns:
        raise LabDatabaseIdentityError("lab jobs SQLite v5 lab_artifact_commit has invalid columns")
    required_result_columns = {
        "job_id",
        "commit_request_id",
        "sealed_path",
        "manifest_hash",
        "complete_result_hash",
        "bundle_device",
        "bundle_inode",
        "evidence_json",
        "indexed_at",
    }
    result_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(lab_job_result_artifact)").fetchall()
    }
    if result_columns != required_result_columns:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v5 lab_job_result_artifact has invalid columns"
        )
    _validate_v5_column_identity(
        connection,
        table="lab_job",
        column="result_state",
        declared_type="TEXT",
        not_null=True,
        primary_key_position=0,
        default="'pending'",
    )
    _validate_v5_column_identity(
        connection,
        table="lab_job",
        column="requires_complete_result",
        declared_type="INTEGER",
        not_null=True,
        primary_key_position=0,
        default="0",
    )
    _validate_v5_column_identity(
        connection,
        table="lab_artifact_commit",
        column="request_id",
        declared_type="TEXT",
        not_null=False,
        primary_key_position=1,
        default=None,
    )
    _validate_v5_column_identity(
        connection,
        table="lab_job_result_artifact",
        column="job_id",
        declared_type="TEXT",
        not_null=False,
        primary_key_position=1,
        default=None,
    )
    _validate_v5_column_identity(
        connection,
        table="lab_job_result_artifact",
        column="commit_request_id",
        declared_type="TEXT",
        not_null=True,
        primary_key_position=0,
        default=None,
    )
    _validate_v5_key_and_foreign_key_constraints(connection)
    required_triggers = {
        "trg_lab_job_complete_result_insert",
        "trg_lab_job_complete_result_update",
        "trg_lab_job_complete_result_marker_immutable",
        "trg_lab_result_artifact_insert",
        "trg_lab_result_artifact_no_update",
        "trg_lab_result_artifact_no_delete",
        "trg_lab_artifact_commit_no_update",
        "trg_lab_artifact_commit_no_delete",
    }
    existing_triggers = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
    }
    missing_triggers = sorted(required_triggers - existing_triggers)
    if missing_triggers:
        raise LabDatabaseIdentityError(
            f"lab jobs SQLite v5 is missing triggers: {', '.join(missing_triggers)}"
        )
    expected_trigger_sql = {
        "trg_lab_job_complete_result_insert": _V5_JOB_RESULT_INSERT_TRIGGER,
        "trg_lab_job_complete_result_update": _V5_JOB_RESULT_UPDATE_TRIGGER,
        "trg_lab_job_complete_result_marker_immutable": (_V5_JOB_RESULT_MARKER_IMMUTABLE_TRIGGER),
        "trg_lab_result_artifact_insert": _V5_RESULT_ARTIFACT_INSERT_TRIGGER,
        "trg_lab_result_artifact_no_update": _V5_RESULT_ARTIFACT_NO_UPDATE_TRIGGER,
        "trg_lab_result_artifact_no_delete": _V5_RESULT_ARTIFACT_NO_DELETE_TRIGGER,
        "trg_lab_artifact_commit_no_update": _V5_ARTIFACT_COMMIT_NO_UPDATE_TRIGGER,
        "trg_lab_artifact_commit_no_delete": _V5_ARTIFACT_COMMIT_NO_DELETE_TRIGGER,
    }
    for name, expected_sql in expected_trigger_sql.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            (name,),
        ).fetchone()
        assert row is not None
        actual = " ".join(str(row[0]).strip().rstrip(";").split()).upper()
        expected = " ".join(expected_sql.strip().rstrip(";").split()).upper()
        actual = actual.replace(" IF NOT EXISTS ", " ")
        expected = expected.replace(" IF NOT EXISTS ", " ")
        if actual != expected:
            raise LabDatabaseIdentityError(
                f"lab jobs SQLite v5 trigger {name} has invalid structure"
            )


def _migrate_v1_to_v2(connection: sqlite3.Connection) -> None:
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(lab_command)").fetchall()
    }
    if "receipt_job_version" in columns:
        raise LabDatabaseIdentityError("lab jobs SQLite v1 unexpectedly has receipt_job_version")
    connection.execute(
        """
        ALTER TABLE lab_command
        ADD COLUMN receipt_job_version INTEGER CHECK (
            receipt_job_version IS NULL OR (
                typeof(receipt_job_version) = 'integer'
                AND receipt_job_version >= 0
            )
        )
        """
    )
    rows = connection.execute(
        "SELECT request_id, receipt_json FROM lab_command ORDER BY request_id"
    ).fetchall()
    for row in rows:
        job_version = _receipt_job_version_from_json(str(row["receipt_json"]))
        connection.execute(
            "UPDATE lab_command SET receipt_job_version = ? WHERE request_id = ?",
            (job_version, str(row["request_id"])),
        )
    for row in connection.execute("SELECT * FROM lab_command ORDER BY request_id").fetchall():
        _command_record_from_row(row)


def _migrate_v2_to_v3(connection: sqlite3.Connection) -> None:
    _validate_v2_schema(connection)
    existing = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(lab_shard)").fetchall()
    }
    if "claim_generation" in existing:
        raise LabDatabaseIdentityError("lab jobs SQLite v2 unexpectedly has v3 shard columns")
    additions = (
        f"ALTER TABLE lab_shard ADD COLUMN plan_hash TEXT NOT NULL DEFAULT '{_LEGACY_PLAN_HASH}'",
        "ALTER TABLE lab_shard ADD COLUMN adapter_id TEXT NOT NULL DEFAULT 'legacy-v2'",
        "ALTER TABLE lab_shard ADD COLUMN adapter_version TEXT NOT NULL DEFAULT 'v0'",
        "ALTER TABLE lab_shard ADD COLUMN payload_json TEXT NOT NULL "
        f"DEFAULT '{_EMPTY_PAYLOAD_JSON}'",
        "ALTER TABLE lab_shard ADD COLUMN payload_hash TEXT NOT NULL "
        f"DEFAULT '{_EMPTY_PAYLOAD_HASH}'",
        "ALTER TABLE lab_shard ADD COLUMN claim_token TEXT",
        """
        ALTER TABLE lab_shard ADD COLUMN claim_generation INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(claim_generation) = 'integer' AND claim_generation >= 0)
        """,
        "ALTER TABLE lab_shard ADD COLUMN claimed_at TEXT",
        "ALTER TABLE lab_shard ADD COLUMN heartbeat_at TEXT",
        "ALTER TABLE lab_shard ADD COLUMN lease_expires_at TEXT",
        "ALTER TABLE lab_shard ADD COLUMN result_manifest_hash TEXT",
        "ALTER TABLE lab_shard ADD COLUMN failure_json TEXT",
        "ALTER TABLE lab_shard ADD COLUMN finished_at TEXT",
    )
    for statement in additions:
        connection.execute(statement)
    _prepare_legacy_shard_id_migration(connection)
    _migrate_global_shard_primary_key(connection, include_worker_reports=False)
    connection.execute(_V3_REPORT_TABLE_STATEMENT)
    connection.execute(_V3_REPORT_INDEX_STATEMENT)
    connection.execute(_V3_SCHEDULER_STATE_TABLE_STATEMENT)
    _normalize_legacy_terminal_shards(connection)
    _normalize_v2_exhausted_nonterminal_jobs(connection)
    _normalize_v2_legacy_nonterminal_shards(connection)
    _validate_v3_schema(connection)


def _migrate_v3_to_v4(connection: sqlite3.Connection) -> None:
    _validate_v3_schema(connection)
    job_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(lab_job)").fetchall()
    }
    shard_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(lab_shard)").fetchall()
    }
    if "result_contract_version" in job_columns or "phase" in shard_columns:
        raise LabDatabaseIdentityError("lab jobs SQLite v3 unexpectedly has v4 telemetry columns")
    additions = (
        """
        ALTER TABLE lab_job ADD COLUMN result_contract_version TEXT
        CHECK (
            result_contract_version IS NULL
            OR (typeof(result_contract_version) = 'text' AND length(result_contract_version) > 0)
        )
        """,
        """
        ALTER TABLE lab_shard ADD COLUMN phase TEXT
        CHECK (phase IS NULL OR (typeof(phase) = 'text' AND length(phase) > 0))
        """,
        """
        ALTER TABLE lab_shard ADD COLUMN work_unit_name TEXT
        CHECK (
            work_unit_name IS NULL
            OR (typeof(work_unit_name) = 'text' AND length(work_unit_name) > 0)
        )
        """,
        f"""
        ALTER TABLE lab_shard ADD COLUMN work_units INTEGER
        CHECK (
            work_units IS NULL
            OR (typeof(work_units) = 'integer'
                AND work_units >= 1
                AND work_units <= {SQLITE_SIGNED_INTEGER_MAX})
        )
        """,
        f"""
        ALTER TABLE lab_shard ADD COLUMN static_duration_ms INTEGER
        CHECK (
            (phase IS NULL AND work_unit_name IS NULL
             AND work_units IS NULL AND static_duration_ms IS NULL)
            OR
            (phase IS NOT NULL AND work_unit_name IS NOT NULL
             AND work_units IS NOT NULL
             AND typeof(static_duration_ms) = 'integer'
             AND static_duration_ms >= 1
             AND static_duration_ms <= {SQLITE_SIGNED_INTEGER_MAX})
        )
        """,
        f"""
        ALTER TABLE lab_shard ADD COLUMN duration_ms REAL
        CHECK (
            duration_ms IS NULL
            OR (typeof(duration_ms) IN ('integer', 'real')
                AND duration_ms >= {LAB_SHARD_DURATION_MS_MIN}
                AND duration_ms < {LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE})
        )
        """,
        f"""
        ALTER TABLE lab_shard ADD COLUMN throughput_units_per_second REAL
        CHECK (
            (duration_ms IS NULL AND throughput_units_per_second IS NULL)
            OR
            (duration_ms IS NOT NULL
             AND typeof(throughput_units_per_second) IN ('integer', 'real')
             AND throughput_units_per_second > 0
             AND throughput_units_per_second < {LAB_SHARD_THROUGHPUT_MAX_EXCLUSIVE})
        )
        """,
        """
        ALTER TABLE lab_shard ADD COLUMN completion_sequence INTEGER
        CHECK (
            completion_sequence IS NULL
            OR (typeof(completion_sequence) = 'integer'
                AND completion_sequence >= 1
                AND status = 'succeeded'
                AND duration_ms IS NOT NULL
                AND throughput_units_per_second IS NOT NULL)
        )
        """,
    )
    for statement in additions:
        connection.execute(statement)
    connection.execute(_V4_COMPLETION_INDEX_STATEMENT)
    connection.execute(_V4_STATUS_INDEX_STATEMENT)
    _validate_v4_schema(connection)


def _migrate_v4_to_v5(connection: sqlite3.Connection) -> None:
    _validate_v4_schema(connection)
    columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(lab_job)").fetchall()}
    if "result_state" in columns:
        raise LabDatabaseIdentityError("lab jobs SQLite v4 unexpectedly has result_state")
    result_values = ",".join(f"'{state.value}'" for state in LabResultState)
    connection.execute(
        f"""
        ALTER TABLE lab_job ADD COLUMN result_state TEXT NOT NULL DEFAULT 'pending'
        CHECK (result_state IN ({result_values}))
        """
    )
    connection.execute(
        """
        ALTER TABLE lab_job ADD COLUMN requires_complete_result INTEGER NOT NULL DEFAULT 0
        CHECK (
            typeof(requires_complete_result) = 'integer'
            AND requires_complete_result IN (0, 1)
        )
        """
    )
    connection.execute(
        """
        UPDATE lab_job
        SET result_state = ?
        WHERE status = ?
        """,
        (LabResultState.LEGACY_UNSEALED.value, JobStatus.SUCCEEDED.value),
    )


def _normalize_legacy_terminal_shards(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        UPDATE lab_shard
        SET worker_id = NULL, scheduler_fencing_token = NULL,
            claim_token = NULL, claimed_at = NULL, heartbeat_at = NULL,
            lease_expires_at = NULL, checkpoint_json = NULL,
            finished_at = COALESCE(finished_at, updated_at, created_at),
            updated_at = COALESCE(updated_at, finished_at, created_at)
        WHERE adapter_id = 'legacy-v2' AND status IN (?, ?, ?)
        """,
        (
            ShardStatus.SUCCEEDED.value,
            ShardStatus.FAILED.value,
            ShardStatus.CANCELLED.value,
        ),
    )


def _normalize_v2_exhausted_nonterminal_jobs(connection: sqlite3.Connection) -> None:
    exhausted_rows = connection.execute(
        """
        SELECT job.job_id, job.version AS job_version,
               shard.shard_id AS exhausted_shard_id,
               COALESCE(shard.updated_at, shard.created_at,
                        job.updated_at, job.created_at) AS failed_at
        FROM lab_job AS job
        JOIN lab_shard AS shard ON shard.job_id = job.job_id
        WHERE shard.adapter_id = 'legacy-v2'
          AND job.status IN (?, ?, ?)
          AND shard.status IN (?, ?, ?)
          AND shard.attempt_count >= shard.max_attempts
        ORDER BY job.job_id, shard.shard_index
        """,
        (
            JobStatus.QUEUED.value,
            JobStatus.RUNNING.value,
            JobStatus.CHECKPOINTED.value,
            ShardStatus.QUEUED.value,
            ShardStatus.RUNNING.value,
            ShardStatus.CHECKPOINTED.value,
        ),
    ).fetchall()
    seen_jobs: set[str] = set()
    for row in exhausted_rows:
        job_id = str(row["job_id"])
        if job_id in seen_jobs:
            continue
        seen_jobs.add(job_id)
        failed_at = str(row["failed_at"])
        shard_cursor = connection.execute(
            """
            UPDATE lab_shard
            SET status = ?, version = version + 1,
                worker_id = NULL, scheduler_fencing_token = NULL,
                claim_token = NULL, claimed_at = NULL,
                heartbeat_at = NULL, lease_expires_at = NULL,
                result_manifest_hash = NULL,
                failure_json = CASE WHEN shard_id = ? THEN ? ELSE ? END,
                checkpoint_json = NULL, finished_at = ?, updated_at = ?
            WHERE job_id = ? AND status IN (?, ?, ?)
            """,
            (
                ShardStatus.FAILED.value,
                str(row["exhausted_shard_id"]),
                _ATTEMPTS_EXHAUSTED_FAILURE_JSON,
                _PARENT_ATTEMPTS_EXHAUSTED_FAILURE_JSON,
                failed_at,
                failed_at,
                job_id,
                ShardStatus.QUEUED.value,
                ShardStatus.RUNNING.value,
                ShardStatus.CHECKPOINTED.value,
            ),
        )
        if shard_cursor.rowcount < 1:
            raise LabDatabaseIdentityError(
                "exhausted legacy-v2 job has no nonterminal shard to fail"
            )
        job_version = _strict_sqlite_int(row["job_version"], field="lab_job.version", minimum=0)
        job_cursor = connection.execute(
            """
            UPDATE lab_job
            SET status = ?, control_intent = ?, version = ?, recoverable = 0,
                scheduler_fencing_token = NULL, updated_at = ?
            WHERE job_id = ? AND version = ? AND status IN (?, ?, ?)
            """,
            (
                JobStatus.FAILED.value,
                ControlIntent.NONE.value,
                job_version + 1,
                failed_at,
                job_id,
                job_version,
                JobStatus.QUEUED.value,
                JobStatus.RUNNING.value,
                JobStatus.CHECKPOINTED.value,
            ),
        )
        if job_cursor.rowcount != 1:
            raise LabDatabaseIdentityError("exhausted legacy-v2 job changed during migration")


def _normalize_v2_legacy_nonterminal_shards(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        UPDATE lab_shard
        SET status = ?, version = version + 1,
            worker_id = NULL, scheduler_fencing_token = NULL,
            claim_token = NULL, claimed_at = NULL, heartbeat_at = NULL,
            lease_expires_at = NULL, result_manifest_hash = NULL,
            failure_json = NULL, checkpoint_json = NULL, finished_at = NULL,
            updated_at = COALESCE(updated_at, created_at)
        WHERE adapter_id = 'legacy-v2'
          AND status IN (?, ?, ?)
          AND (
            status <> ? OR worker_id IS NOT NULL
            OR scheduler_fencing_token IS NOT NULL
            OR claim_token IS NOT NULL OR claimed_at IS NOT NULL
            OR heartbeat_at IS NOT NULL OR lease_expires_at IS NOT NULL
            OR checkpoint_json IS NOT NULL
          )
        """,
        (
            ShardStatus.QUEUED.value,
            ShardStatus.QUEUED.value,
            ShardStatus.RUNNING.value,
            ShardStatus.CHECKPOINTED.value,
            ShardStatus.QUEUED.value,
        ),
    )


def _prepare_legacy_shard_id_migration(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS lab_shard_id_migration (
            job_id TEXT NOT NULL,
            old_shard_id TEXT NOT NULL,
            new_shard_id TEXT NOT NULL,
            PRIMARY KEY (job_id, old_shard_id),
            UNIQUE (job_id, new_shard_id)
        )
        """
    )
    rows = connection.execute(
        """
        SELECT job_id, shard_id, shard_index, adapter_id, adapter_version,
               plan_hash, payload_json
        FROM lab_shard
        WHERE adapter_id = 'legacy-v2'
        ORDER BY job_id, shard_index
        """
    ).fetchall()
    for row in rows:
        definition = LabShardDefinition.from_payload(
            shard_index=_strict_sqlite_int(
                row["shard_index"], field="lab_shard.shard_index", minimum=0
            ),
            adapter_id=str(row["adapter_id"]),
            adapter_version=str(row["adapter_version"]),
            plan_hash=str(row["plan_hash"]),
            payload_json=str(row["payload_json"]),
        )
        connection.execute(
            """
            INSERT INTO lab_shard_id_migration (
                job_id, old_shard_id, new_shard_id
            ) VALUES (?, ?, ?)
            """,
            (
                str(row["job_id"]),
                str(row["shard_id"]),
                str(definition.shard_id),
            ),
        )


def _migrate_global_shard_primary_key(
    connection: sqlite3.Connection,
    *,
    include_worker_reports: bool,
) -> None:
    connection.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS lab_shard_id_migration (
            job_id TEXT NOT NULL,
            old_shard_id TEXT NOT NULL,
            new_shard_id TEXT NOT NULL,
            PRIMARY KEY (job_id, old_shard_id),
            UNIQUE (job_id, new_shard_id)
        )
        """
    )
    report_suffix = ""
    if include_worker_reports:
        connection.execute("ALTER TABLE lab_worker_report RENAME TO lab_worker_report_global_shard")
        report_suffix = "_global_shard"
    connection.execute("ALTER TABLE lab_artifact RENAME TO lab_artifact_global_shard")
    connection.execute("ALTER TABLE lab_shard RENAME TO lab_shard_global_shard")
    connection.execute(_V3_SHARD_TABLE_STATEMENT)
    connection.execute(
        """
        INSERT INTO lab_shard (
            shard_id, job_id, shard_index, status, version,
            attempt_count, max_attempts, plan_hash, adapter_id,
            adapter_version, payload_json, payload_hash, worker_id,
            scheduler_fencing_token, claim_token, claim_generation,
            claimed_at, heartbeat_at, lease_expires_at,
            result_manifest_hash, failure_json, finished_at,
            checkpoint_json, created_at, updated_at
        )
        SELECT
            COALESCE(mapping.new_shard_id, shard.shard_id),
            shard.job_id, shard.shard_index, shard.status, shard.version,
            attempt_count, max_attempts, plan_hash, adapter_id,
            adapter_version, payload_json, payload_hash, worker_id,
            scheduler_fencing_token, claim_token, claim_generation,
            claimed_at, heartbeat_at, lease_expires_at,
            result_manifest_hash, failure_json, finished_at,
            checkpoint_json, created_at, updated_at
        FROM lab_shard_global_shard AS shard
        LEFT JOIN lab_shard_id_migration AS mapping
          ON mapping.job_id = shard.job_id
         AND mapping.old_shard_id = shard.shard_id
        """
    )
    connection.execute(_V3_ARTIFACT_TABLE_STATEMENT)
    connection.execute(
        """
        INSERT INTO lab_artifact (
            artifact_id, job_id, shard_id, artifact_type, uri,
            content_hash, created_at
        )
        SELECT
            artifact.artifact_id, artifact.job_id,
            COALESCE(mapping.new_shard_id, artifact.shard_id),
            artifact.artifact_type, artifact.uri,
            artifact.content_hash, artifact.created_at
        FROM lab_artifact_global_shard AS artifact
        LEFT JOIN lab_shard_id_migration AS mapping
          ON mapping.job_id = artifact.job_id
         AND mapping.old_shard_id = artifact.shard_id
        """
    )
    if include_worker_reports:
        connection.execute(_V3_REPORT_TABLE_STATEMENT)
        connection.execute(
            f"""
            INSERT INTO lab_worker_report (
                report_id, content_hash, job_id, shard_id, report_type,
                report_json, status, reason, receipt_json, claim_generation,
                scheduler_fencing_token, received_at, applied_at
            )
            SELECT
                report.report_id, report.content_hash, report.job_id,
                COALESCE(mapping.new_shard_id, report.shard_id),
                report.report_type, report.report_json, report.status,
                report.reason, report.receipt_json, report.claim_generation,
                report.scheduler_fencing_token, report.received_at,
                report.applied_at
            FROM lab_worker_report{report_suffix} AS report
            LEFT JOIN lab_shard_id_migration AS mapping
              ON mapping.job_id = report.job_id
             AND mapping.old_shard_id = report.shard_id
            """
        )
        connection.execute("DROP TABLE lab_worker_report_global_shard")
    connection.execute("DROP TABLE lab_artifact_global_shard")
    connection.execute("DROP TABLE lab_shard_global_shard")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS ix_lab_artifact_job ON lab_artifact(job_id, created_at)"
    )
    if include_worker_reports:
        connection.execute(_V3_REPORT_INDEX_STATEMENT)
    connection.execute("DROP TABLE lab_shard_id_migration")


def _validate_database_identity(
    connection: sqlite3.Connection,
    *,
    allow_unclaimed_empty: bool,
    accepted_versions: frozenset[int] | None = None,
) -> bool:
    try:
        application_id = _strict_sqlite_int(
            connection.execute("PRAGMA application_id").fetchone()[0],
            field="PRAGMA application_id",
            minimum=0,
        )
        user_version = _strict_sqlite_int(
            connection.execute("PRAGMA user_version").fetchone()[0],
            field="PRAGMA user_version",
            minimum=0,
        )
    except InvalidStoredJobError as exc:
        raise LabDatabaseIdentityError(str(exc)) from exc
    versions = accepted_versions or frozenset({_SCHEMA_VERSION})
    if application_id == _APPLICATION_ID:
        if user_version not in versions:
            expected = ", ".join(str(version) for version in sorted(versions))
            raise LabDatabaseIdentityError(
                "lab jobs SQLite user_version mismatch: "
                f"expected one of [{expected}], found {user_version}"
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
            _validate_v5_schema(connection)
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
            record = LabJobRecord(
                job_id=UUID(str(row["job_id"])),
                spec=spec,
                spec_hash=stored_hash,
                job_type=stored_job_type,
                resource_class=stored_resource,
                deadline=stored_deadline,
                status=JobStatus(str(row["status"])),
                control_intent=ControlIntent(str(row["control_intent"])),
                version=_strict_sqlite_int(row["version"], field="lab_job.version", minimum=0),
                attempt_count=_strict_sqlite_int(
                    row["attempt_count"], field="lab_job.attempt_count", minimum=0
                ),
                max_attempts=_strict_sqlite_int(
                    row["max_attempts"], field="lab_job.max_attempts", minimum=1
                ),
                recoverable=_strict_sqlite_bool(row["recoverable"], field="lab_job.recoverable"),
                scheduler_fencing_token=_strict_nullable_sqlite_int(
                    row["scheduler_fencing_token"],
                    field="lab_job.scheduler_fencing_token",
                    minimum=1,
                ),
                result_contract_version=(
                    str(row["result_contract_version"])
                    if row["result_contract_version"] is not None
                    else None
                ),
                requires_complete_result=_strict_sqlite_bool(
                    row["requires_complete_result"],
                    field="lab_job.requires_complete_result",
                ),
                result_state=LabResultState(str(row["result_state"])),
                created_at=_load_time(str(row["created_at"])),
                updated_at=_load_time(str(row["updated_at"])),
            )
            if record.result_state is LabResultState.READY and (
                record.status is not JobStatus.RUNNING
                or record.result_contract_version != COMPLETE_RESULT_CONTRACT_VERSION
            ):
                raise ValueError("ready result state requires a running complete-result job")
            if record.result_state is LabResultState.SEALED and (
                record.status is not JobStatus.SUCCEEDED
                or record.result_contract_version != COMPLETE_RESULT_CONTRACT_VERSION
            ):
                raise ValueError("sealed result state requires a succeeded complete-result job")
            if record.status is JobStatus.SUCCEEDED and record.result_state not in {
                LabResultState.SEALED,
                LabResultState.LEGACY_UNSEALED,
            }:
                raise ValueError("succeeded job has no authoritative result state")
            if record.result_state is LabResultState.LEGACY_UNSEALED and (
                record.status is not JobStatus.SUCCEEDED or record.requires_complete_result
            ):
                raise ValueError("legacy_unsealed is only valid for migrated legacy succeeded jobs")
            return record
        except Exception as exc:
            if isinstance(exc, InvalidStoredJobError):
                raise
            raise InvalidStoredJobError(f"invalid stored lab job {row['job_id']}: {exc}") from exc

    @staticmethod
    def _lease_from_row(row: sqlite3.Row) -> LabLeaseRecord:
        try:
            return LabLeaseRecord(
                lease_id=_strict_sqlite_int(row["lease_id"], field="lab_lease.lease_id", minimum=1),
                lease_name=str(row["lease_name"]),
                owner_id=str(row["owner_id"]),
                token=UUID(str(row["token"])),
                fencing_token=_strict_sqlite_int(
                    row["fencing_token"], field="lab_lease.fencing_token", minimum=1
                ),
                acquired_at=_load_time(str(row["acquired_at"])),
                heartbeat_at=_load_time(str(row["heartbeat_at"])),
                expires_at=_load_time(str(row["expires_at"])),
                released_at=(
                    _load_time(str(row["released_at"])) if row["released_at"] is not None else None
                ),
            )
        except Exception as exc:
            if isinstance(exc, InvalidStoredJobError):
                raise
            raise InvalidStoredJobError(
                f"invalid stored lab lease {row['lease_id']}: {exc}"
            ) from exc

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> LabEventRecord:
        try:
            return LabEventRecord(
                event_id=_strict_sqlite_int(row["event_id"], field="lab_event.event_id", minimum=1),
                job_id=UUID(str(row["job_id"])),
                request_id=(
                    UUID(str(row["request_id"])) if row["request_id"] is not None else None
                ),
                event_type=str(row["event_type"]),
                prior_status=(
                    JobStatus(str(row["prior_status"])) if row["prior_status"] is not None else None
                ),
                new_status=JobStatus(str(row["new_status"])),
                job_version=_strict_sqlite_int(
                    row["job_version"], field="lab_event.job_version", minimum=0
                ),
                reason=str(row["reason"]),
                scheduler_fencing_token=_strict_nullable_sqlite_int(
                    row["scheduler_fencing_token"],
                    field="lab_event.scheduler_fencing_token",
                    minimum=1,
                ),
                created_at=_load_time(str(row["created_at"])),
            )
        except Exception as exc:
            if isinstance(exc, InvalidStoredJobError):
                raise
            raise InvalidStoredJobError(
                f"invalid stored lab event {row['event_id']}: {exc}"
            ) from exc

    @staticmethod
    def _shard_from_row(row: sqlite3.Row) -> LabShardRecord:
        try:
            record = LabShardRecord(
                shard_id=UUID(str(row["shard_id"])),
                job_id=UUID(str(row["job_id"])),
                shard_index=_strict_sqlite_int(
                    row["shard_index"], field="lab_shard.shard_index", minimum=0
                ),
                status=ShardStatus(str(row["status"])),
                version=_strict_sqlite_int(row["version"], field="lab_shard.version", minimum=0),
                attempt_count=_strict_sqlite_int(
                    row["attempt_count"], field="lab_shard.attempt_count", minimum=0
                ),
                max_attempts=_strict_sqlite_int(
                    row["max_attempts"], field="lab_shard.max_attempts", minimum=1
                ),
                worker_id=(str(row["worker_id"]) if row["worker_id"] else None),
                scheduler_fencing_token=_strict_nullable_sqlite_int(
                    row["scheduler_fencing_token"],
                    field="lab_shard.scheduler_fencing_token",
                    minimum=1,
                ),
                checkpoint_json=(
                    str(row["checkpoint_json"]) if row["checkpoint_json"] is not None else None
                ),
                plan_hash=str(row["plan_hash"]),
                adapter_id=str(row["adapter_id"]),
                adapter_version=str(row["adapter_version"]),
                payload_json=str(row["payload_json"]),
                payload_hash=str(row["payload_hash"]),
                phase=(str(row["phase"]) if row["phase"] is not None else None),
                work_unit_name=(
                    str(row["work_unit_name"]) if row["work_unit_name"] is not None else None
                ),
                work_units=_strict_nullable_sqlite_int(
                    row["work_units"],
                    field="lab_shard.work_units",
                    minimum=1,
                    maximum=SQLITE_SIGNED_INTEGER_MAX,
                ),
                static_duration_ms=_strict_nullable_sqlite_int(
                    row["static_duration_ms"],
                    field="lab_shard.static_duration_ms",
                    minimum=1,
                    maximum=SQLITE_SIGNED_INTEGER_MAX,
                ),
                duration_ms=_strict_nullable_sqlite_real(
                    row["duration_ms"],
                    field="lab_shard.duration_ms",
                    positive=True,
                    minimum_inclusive=LAB_SHARD_DURATION_MS_MIN,
                    maximum_exclusive=LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE,
                ),
                throughput_units_per_second=_strict_nullable_sqlite_real(
                    row["throughput_units_per_second"],
                    field="lab_shard.throughput_units_per_second",
                    positive=True,
                    maximum_exclusive=LAB_SHARD_THROUGHPUT_MAX_EXCLUSIVE,
                ),
                completion_sequence=_strict_nullable_sqlite_int(
                    row["completion_sequence"],
                    field="lab_shard.completion_sequence",
                    minimum=1,
                ),
                claim_token=(
                    UUID(str(row["claim_token"])) if row["claim_token"] is not None else None
                ),
                claim_generation=_strict_sqlite_int(
                    row["claim_generation"],
                    field="lab_shard.claim_generation",
                    minimum=0,
                ),
                claimed_at=(
                    _load_time(str(row["claimed_at"])) if row["claimed_at"] is not None else None
                ),
                heartbeat_at=(
                    _load_time(str(row["heartbeat_at"]))
                    if row["heartbeat_at"] is not None
                    else None
                ),
                lease_expires_at=(
                    _load_time(str(row["lease_expires_at"]))
                    if row["lease_expires_at"] is not None
                    else None
                ),
                result_manifest_hash=(
                    str(row["result_manifest_hash"])
                    if row["result_manifest_hash"] is not None
                    else None
                ),
                failure_json=(
                    str(row["failure_json"]) if row["failure_json"] is not None else None
                ),
                finished_at=(
                    _load_time(str(row["finished_at"])) if row["finished_at"] is not None else None
                ),
                created_at=_load_time(str(row["created_at"])),
                updated_at=_load_time(str(row["updated_at"])),
            )
            is_legacy = record.adapter_id == "legacy-v2"
            if is_legacy:
                if (
                    record.adapter_version != "v0"
                    or record.plan_hash != _LEGACY_PLAN_HASH
                    or record.payload_json != _EMPTY_PAYLOAD_JSON
                    or record.payload_hash != _EMPTY_PAYLOAD_HASH
                ):
                    raise ValueError("legacy shard identity mismatch")
            else:
                LabShardDefinition(
                    shard_id=record.shard_id,
                    shard_index=record.shard_index,
                    adapter_id=record.adapter_id,
                    adapter_version=record.adapter_version,
                    plan_hash=record.plan_hash,
                    payload_json=record.payload_json,
                    payload_hash=record.payload_hash,
                    work_plan=record.work_plan,
                )
            plan_values = (
                record.phase,
                record.work_unit_name,
                record.work_units,
                record.static_duration_ms,
            )
            if not (
                all(value is None for value in plan_values)
                or all(value is not None for value in plan_values)
            ):
                raise ValueError("shard work plan must be entirely present or absent")
            telemetry_values = (
                record.duration_ms,
                record.throughput_units_per_second,
                record.completion_sequence,
            )
            if not (
                all(value is None for value in telemetry_values)
                or all(value is not None for value in telemetry_values)
            ):
                raise ValueError("shard completion telemetry must be entirely present or absent")
            if record.duration_ms is not None:
                if record.work_plan is None:
                    raise ValueError("shard telemetry is missing its work plan")
                LabShardTelemetry(
                    **record.work_plan.model_dump(),
                    duration_ms=record.duration_ms,
                    throughput_units_per_second=record.throughput_units_per_second,
                )
                if record.status is not ShardStatus.SUCCEEDED:
                    raise ValueError("non-succeeded shard retains completion telemetry")
            if (
                record.status is ShardStatus.SUCCEEDED
                and record.work_plan is not None
                and record.duration_ms is None
            ):
                raise ValueError("telemetry-planned succeeded shard is missing telemetry")
            if record.status is ShardStatus.RUNNING and any(
                value is None
                for value in (
                    record.worker_id,
                    record.scheduler_fencing_token,
                    record.claim_token,
                    record.claimed_at,
                    record.heartbeat_at,
                    record.lease_expires_at,
                )
            ):
                raise ValueError("running shard is missing claim identity")
            if record.status is ShardStatus.QUEUED and record.attempt_count >= record.max_attempts:
                raise ValueError("queued shard exhausted attempts")
            if (
                record.claimed_at is not None
                and record.heartbeat_at is not None
                and record.heartbeat_at < record.claimed_at
            ):
                raise ValueError("shard heartbeat predates claim")
            if (
                record.claimed_at is not None
                and record.lease_expires_at is not None
                and record.lease_expires_at <= record.claimed_at
            ):
                raise ValueError("shard claim lease is not positive")
            if record.status is ShardStatus.SUCCEEDED and record.finished_at is None:
                raise ValueError("succeeded shard is missing result identity")
            if (
                record.status is ShardStatus.SUCCEEDED
                and not is_legacy
                and record.result_manifest_hash is None
            ):
                raise ValueError("succeeded shard is missing result identity")
            if record.status is ShardStatus.FAILED and record.finished_at is None:
                raise ValueError("failed shard is missing failure identity")
            if (
                record.status is ShardStatus.FAILED
                and not is_legacy
                and record.failure_json is None
            ):
                raise ValueError("failed shard is missing failure identity")
            if record.status is ShardStatus.CANCELLED and record.finished_at is None:
                raise ValueError("cancelled shard is missing finished_at")
            if record.status in {
                ShardStatus.SUCCEEDED,
                ShardStatus.FAILED,
                ShardStatus.CANCELLED,
            } and any(
                value is not None
                for value in (
                    record.worker_id,
                    record.scheduler_fencing_token,
                    record.claim_token,
                    record.claimed_at,
                    record.heartbeat_at,
                    record.lease_expires_at,
                )
            ):
                raise ValueError("terminal shard retains claim identity")
            return record
        except Exception as exc:
            if isinstance(exc, InvalidStoredJobError):
                raise
            raise InvalidStoredJobError(
                f"invalid stored lab shard {row['shard_id']}: {exc}"
            ) from exc

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
        return _command_record_from_row(row, expected_request_id=request_id)

    def list_events(self, job_id: UUID) -> tuple[LabEventRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM lab_event WHERE job_id = ? ORDER BY event_id",
                (str(job_id),),
            ).fetchall()
        return tuple(self._event_from_row(row) for row in rows)

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
        return tuple(self._shard_from_row(row) for row in rows)

    def get_eta_input(
        self,
        job_id: UUID,
        *,
        as_of: datetime,
        completed_limit: int = LAB_ETA_COMPLETED_LIMIT_MAX,
    ) -> LabEtaInput | None:
        from rquant.lab_eta import LabEtaCompletedShard, LabEtaInput, LabEtaRemainingShard

        if not 3 <= completed_limit <= LAB_ETA_COMPLETED_LIMIT_MAX:
            raise ValueError(
                f"completed telemetry limit must be between 3 and {LAB_ETA_COMPLETED_LIMIT_MAX}"
            )
        current = _utc(as_of)
        with self._connect() as connection:
            job_row = connection.execute(
                "SELECT status FROM lab_job WHERE job_id = ?",
                (str(job_id),),
            ).fetchone()
            if job_row is None:
                return None
            completed_rows = connection.execute(
                """
                SELECT shard_id, phase, work_unit_name, work_units,
                       static_duration_ms, duration_ms,
                       throughput_units_per_second, completion_sequence
                FROM lab_shard
                WHERE job_id = ? AND status = 'succeeded'
                  AND completion_sequence IS NOT NULL
                ORDER BY completion_sequence DESC
                LIMIT ?
                """,
                (str(job_id), completed_limit),
            ).fetchall()
            remaining_rows = connection.execute(
                """
                SELECT shard_id, phase, work_unit_name, work_units,
                       static_duration_ms
                FROM lab_shard INDEXED BY ix_lab_shard_job_status_index
                WHERE job_id = ?
                  AND status IN ('queued', 'running', 'checkpointed')
                ORDER BY shard_index, shard_id
                """,
                (str(job_id),),
            ).fetchall()

        completed: list[LabEtaCompletedShard] = []
        for row in completed_rows:
            telemetry = LabShardTelemetry(
                phase=str(row["phase"]),
                work_unit_name=str(row["work_unit_name"]),
                work_units=_strict_sqlite_int(
                    row["work_units"],
                    field="lab_shard.work_units",
                    minimum=1,
                    maximum=SQLITE_SIGNED_INTEGER_MAX,
                ),
                static_duration_ms=_strict_sqlite_int(
                    row["static_duration_ms"],
                    field="lab_shard.static_duration_ms",
                    minimum=1,
                    maximum=SQLITE_SIGNED_INTEGER_MAX,
                ),
                duration_ms=_strict_nullable_sqlite_real(
                    row["duration_ms"],
                    field="lab_shard.duration_ms",
                    positive=True,
                    minimum_inclusive=LAB_SHARD_DURATION_MS_MIN,
                    maximum_exclusive=LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE,
                ),
                throughput_units_per_second=_strict_nullable_sqlite_real(
                    row["throughput_units_per_second"],
                    field="lab_shard.throughput_units_per_second",
                    positive=True,
                    maximum_exclusive=LAB_SHARD_THROUGHPUT_MAX_EXCLUSIVE,
                ),
            )
            completed.append(
                LabEtaCompletedShard(
                    shard_id=UUID(str(row["shard_id"])),
                    completion_sequence=_strict_sqlite_int(
                        row["completion_sequence"],
                        field="lab_shard.completion_sequence",
                        minimum=1,
                    ),
                    telemetry=telemetry,
                )
            )
        completed.sort(key=lambda item: item.completion_sequence)

        remaining: list[LabEtaRemainingShard] = []
        for row in remaining_rows:
            plan_values = (
                row["phase"],
                row["work_unit_name"],
                row["work_units"],
                row["static_duration_ms"],
            )
            if all(value is None for value in plan_values):
                plan = None
            elif all(value is not None for value in plan_values):
                plan = LabShardWorkPlan(
                    phase=str(row["phase"]),
                    work_unit_name=str(row["work_unit_name"]),
                    work_units=_strict_sqlite_int(
                        row["work_units"],
                        field="lab_shard.work_units",
                        minimum=1,
                        maximum=SQLITE_SIGNED_INTEGER_MAX,
                    ),
                    static_duration_ms=_strict_sqlite_int(
                        row["static_duration_ms"],
                        field="lab_shard.static_duration_ms",
                        minimum=1,
                        maximum=SQLITE_SIGNED_INTEGER_MAX,
                    ),
                )
            else:
                raise InvalidStoredJobError(
                    "lab_shard work plan must be entirely present or absent"
                )
            remaining.append(
                LabEtaRemainingShard(
                    shard_id=UUID(str(row["shard_id"])),
                    work_plan=plan,
                )
            )
        return LabEtaInput(
            job_id=job_id,
            status=str(job_row["status"]),
            as_of=current,
            completed=tuple(completed),
            remaining=tuple(remaining),
        )

    def estimate_eta(
        self,
        job_id: UUID,
        *,
        as_of: datetime,
        completed_limit: int = LAB_ETA_COMPLETED_LIMIT_MAX,
    ) -> LabEtaEstimate | None:
        from rquant.lab_eta import estimate_lab_eta

        projection = self.get_eta_input(
            job_id,
            as_of=as_of,
            completed_limit=completed_limit,
        )
        return None if projection is None else estimate_lab_eta(projection)

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

    def get_worker_report(self, report_id: UUID) -> LabWorkerReportRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM lab_worker_report WHERE report_id = ?",
                (str(report_id),),
            ).fetchone()
        if row is None:
            return None
        return _worker_report_record_from_row(row, expected_report_id=report_id)

    def get_artifact_commit(self, request_id: UUID) -> LabArtifactCommitRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM lab_artifact_commit WHERE request_id = ?",
                (str(request_id),),
            ).fetchone()
        if row is None:
            return None
        return _artifact_commit_record_from_row(row, expected_request_id=request_id)

    def get_result_artifact(self, job_id: UUID) -> LabArtifactIndexEvidence | None:
        from rquant.lab_artifacts import LabArtifactIndexEvidence

        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM lab_job_result_artifact WHERE job_id = ?",
                (str(job_id),),
            ).fetchone()
        if row is None:
            return None
        try:
            evidence = LabArtifactIndexEvidence.model_validate_json(str(row["evidence_json"]))
            if evidence.job_id != job_id:
                raise ValueError("artifact evidence job id mismatch")
            if (
                str(evidence.sealed_path),
                evidence.manifest_hash,
                evidence.complete_result_hash,
                evidence.bundle_device,
                evidence.bundle_inode,
                _dump_time(evidence.indexed_at),
            ) != (
                str(row["sealed_path"]),
                str(row["manifest_hash"]),
                str(row["complete_result_hash"]),
                _strict_sqlite_int(
                    row["bundle_device"],
                    field="lab_job_result_artifact.bundle_device",
                    minimum=0,
                ),
                _strict_sqlite_int(
                    row["bundle_inode"],
                    field="lab_job_result_artifact.bundle_inode",
                    minimum=1,
                ),
                str(row["indexed_at"]),
            ):
                raise ValueError("artifact evidence conflicts with indexed columns")
            if _canonical_model_json(evidence) != str(row["evidence_json"]):
                raise ValueError("artifact evidence JSON is not canonical")
            return evidence
        except Exception as exc:
            if isinstance(exc, InvalidStoredJobError):
                raise
            raise InvalidStoredJobError(f"invalid stored result artifact {job_id}: {exc}") from exc

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

    def _connect(self, *, validate_identity: bool = True) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            if validate_identity:
                _validate_database_identity(
                    connection,
                    allow_unclaimed_empty=False,
                )
                _validate_v5_schema(connection)
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA synchronous = FULL")
        except BaseException:
            connection.close()
            raise
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            _validate_database_identity(
                connection,
                allow_unclaimed_empty=False,
            )
            _validate_v5_schema(connection)
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect(validate_identity=False)
        try:
            connection.execute("BEGIN IMMEDIATE")
            unclaimed = _validate_database_identity(
                connection,
                allow_unclaimed_empty=True,
                accepted_versions=frozenset(
                    {
                        _LEGACY_SCHEMA_VERSION,
                        _V2_SCHEMA_VERSION,
                        _V3_SCHEMA_VERSION,
                        _PREVIOUS_SCHEMA_VERSION,
                        _SCHEMA_VERSION,
                    }
                ),
            )
            starting_version = _strict_sqlite_int(
                connection.execute("PRAGMA user_version").fetchone()[0],
                field="PRAGMA user_version",
                minimum=0,
            )
            if unclaimed:
                connection.execute(f"PRAGMA application_id = {self.APPLICATION_ID}")
            elif starting_version == _LEGACY_SCHEMA_VERSION:
                _migrate_v1_to_v2(connection)
                for statement in _V2_SCHEMA_STATEMENTS:
                    connection.execute(statement)
                _migrate_v2_to_v3(connection)
                _migrate_v3_to_v4(connection)
                _migrate_v4_to_v5(connection)
            elif starting_version == _V2_SCHEMA_VERSION:
                _migrate_v2_to_v3(connection)
                _migrate_v3_to_v4(connection)
                _migrate_v4_to_v5(connection)
            elif starting_version == _V3_SCHEMA_VERSION:
                shard_primary_key = _shard_primary_key_columns(connection)
                if shard_primary_key == ("shard_id",):
                    _migrate_global_shard_primary_key(
                        connection,
                        include_worker_reports=True,
                    )
                elif shard_primary_key != ("job_id", "shard_id"):
                    raise LabDatabaseIdentityError(
                        "lab jobs SQLite v3 has an unsupported lab_shard primary key"
                    )
                _migrate_v3_to_v4(connection)
                _migrate_v4_to_v5(connection)
            elif starting_version == _PREVIOUS_SCHEMA_VERSION:
                _migrate_v4_to_v5(connection)
            for statement in _SCHEMA_STATEMENTS:
                connection.execute(statement)
            _normalize_legacy_terminal_shards(connection)
            _validate_v5_schema(connection)
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
                synchronous=_strict_sqlite_int(
                    connection.execute("PRAGMA synchronous").fetchone()[0],
                    field="PRAGMA synchronous",
                    minimum=0,
                ),
                foreign_keys=_strict_sqlite_int(
                    connection.execute("PRAGMA foreign_keys").fetchone()[0],
                    field="PRAGMA foreign_keys",
                    minimum=0,
                ),
                busy_timeout_ms=_strict_sqlite_int(
                    connection.execute("PRAGMA busy_timeout").fetchone()[0],
                    field="PRAGMA busy_timeout",
                    minimum=0,
                ),
            )

    @staticmethod
    def _artifact_binding_identity(
        evidence: LabArtifactIndexEvidence,
    ) -> tuple[object, ...]:
        return (
            evidence.job_id,
            evidence.sealed_path,
            evidence.manifest_hash,
            evidence.complete_result_hash,
            evidence.bundle_device,
            evidence.bundle_inode,
            evidence.file_identities,
        )

    @staticmethod
    def _result_artifact_from_row(
        row: sqlite3.Row,
    ) -> LabArtifactIndexEvidence:
        from rquant.lab_artifacts import LabArtifactIndexEvidence

        evidence = LabArtifactIndexEvidence.model_validate_json(str(row["evidence_json"]))
        if (
            str(evidence.job_id),
            str(evidence.sealed_path),
            evidence.manifest_hash,
            evidence.complete_result_hash,
            evidence.bundle_device,
            evidence.bundle_inode,
            _dump_time(evidence.indexed_at),
            _canonical_model_json(evidence),
        ) != (
            str(row["job_id"]),
            str(row["sealed_path"]),
            str(row["manifest_hash"]),
            str(row["complete_result_hash"]),
            _strict_sqlite_int(
                row["bundle_device"],
                field="lab_job_result_artifact.bundle_device",
                minimum=0,
            ),
            _strict_sqlite_int(
                row["bundle_inode"],
                field="lab_job_result_artifact.bundle_inode",
                minimum=1,
            ),
            str(row["indexed_at"]),
            str(row["evidence_json"]),
        ):
            raise InvalidStoredJobError("stored result artifact evidence is inconsistent")
        return evidence

    @staticmethod
    def _record_artifact_commit(
        connection: sqlite3.Connection,
        envelope: LabArtifactCommitEnvelope,
        receipt: LabArtifactCommitReceipt,
        *,
        now: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO lab_artifact_commit (
                request_id, content_hash, job_id, commit_json, status, reason,
                receipt_json, receipt_job_version, received_at, applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(envelope.request_id),
                envelope.content_hash,
                str(envelope.commit.job_id),
                _canonical_model_json(envelope),
                receipt.status,
                receipt.reason,
                _canonical_model_json(receipt),
                receipt.job_version,
                _dump_time(now),
                _dump_time(now),
            ),
        )

    def _reject_artifact_commit(
        self,
        connection: sqlite3.Connection,
        envelope: LabArtifactCommitEnvelope,
        *,
        reason: str,
        job_version: int | None,
        now: datetime,
    ) -> LabArtifactCommitReceipt:
        receipt = LabArtifactCommitReceipt.from_envelope(
            envelope,
            status="rejected",
            reason=reason,
            accepted_at=now,
            job_version=job_version,
        )
        self._record_artifact_commit(connection, envelope, receipt, now=now)
        return receipt

    def _apply_artifact_commit_in_transaction(
        self,
        connection: sqlite3.Connection,
        envelope: LabArtifactCommitEnvelope,
        binding: LabVerifiedSealedBinding,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabArtifactCommitReceipt:
        from rquant.lab_artifacts import LabArtifactIndexEvidence

        existing_commit = connection.execute(
            "SELECT * FROM lab_artifact_commit WHERE request_id = ?",
            (str(envelope.request_id),),
        ).fetchone()
        if existing_commit is not None:
            record = _artifact_commit_record_from_row(
                existing_commit,
                expected_request_id=envelope.request_id,
            )
            if record.envelope != envelope:
                raise RequestContentConflictError(
                    f"request_id {envelope.request_id} already has different artifact content"
                )
            if record.receipt.status == "accepted":
                indexed_row = connection.execute(
                    "SELECT * FROM lab_job_result_artifact WHERE job_id = ?",
                    (str(envelope.commit.job_id),),
                ).fetchone()
                if indexed_row is None:
                    raise InvalidStoredJobError(
                        "accepted artifact commit is missing its result index"
                    )
                indexed = self._result_artifact_from_row(indexed_row)
                if self._artifact_binding_identity(indexed) != self._artifact_binding_identity(
                    binding.evidence
                ):
                    raise InvalidStoredJobError(
                        "accepted artifact commit no longer matches bound index evidence"
                    )
            return record.receipt

        commit = envelope.commit
        manifest = binding.sealed.manifest
        expected_claim = (
            manifest.job_id,
            manifest.spec_hash,
            manifest.plan_hash,
            manifest.adapter_id,
            manifest.adapter_version,
            manifest.result_contract_version,
            manifest.code_sha,
            manifest.dataset_snapshot,
            binding.sealed.manifest_hash,
            manifest.complete_result_hash,
            binding.sealed.path,
        )
        actual_claim = (
            commit.job_id,
            commit.spec_hash,
            commit.plan_hash,
            commit.adapter_id,
            commit.adapter_version,
            commit.result_contract_version,
            commit.code_sha,
            commit.dataset_snapshot,
            commit.manifest_hash,
            commit.complete_result_hash,
            commit.sealed_path,
        )
        if actual_claim != expected_claim:
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason="artifact_identity_mismatch",
                job_version=None,
                now=now,
            )
        if binding.evidence != LabArtifactIndexEvidence(
            job_id=manifest.job_id,
            sealed_path=binding.sealed.path,
            manifest_hash=binding.sealed.manifest_hash,
            complete_result_hash=manifest.complete_result_hash,
            bundle_device=binding.sealed.device,
            bundle_inode=binding.sealed.inode,
            file_identities=binding.sealed.file_identities,
            indexed_at=binding.evidence.indexed_at,
        ):
            raise InvalidStoredJobError("artifact binding evidence is internally inconsistent")

        job_row = self._load_job_row(connection, commit.job_id)
        if job_row is None:
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason="job_not_found",
                job_version=None,
                now=now,
            )
        job = LabJobReader._job_from_row(job_row)
        indexed_row = connection.execute(
            "SELECT * FROM lab_job_result_artifact WHERE job_id = ?",
            (str(commit.job_id),),
        ).fetchone()
        if indexed_row is not None:
            indexed = self._result_artifact_from_row(indexed_row)
            if self._artifact_binding_identity(indexed) != self._artifact_binding_identity(
                binding.evidence
            ):
                return self._reject_artifact_commit(
                    connection,
                    envelope,
                    reason="artifact_index_conflict",
                    job_version=job.version,
                    now=now,
                )
            if (
                job.status is not JobStatus.SUCCEEDED
                or job.result_state is not LabResultState.SEALED
            ):
                raise InvalidStoredJobError("result index exists for a non-sealed job")
            receipt = LabArtifactCommitReceipt.from_envelope(
                envelope,
                status="accepted",
                reason="artifact_already_committed",
                accepted_at=now,
                job_version=job.version,
            )
            self._record_artifact_commit(connection, envelope, receipt, now=now)
            return receipt

        if job.status is not JobStatus.RUNNING:
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason=f"invalid_state:{job.status.value}",
                job_version=job.version,
                now=now,
            )
        if job.result_state is not LabResultState.READY:
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason=f"invalid_result_state:{job.result_state.value}",
                job_version=job.version,
                now=now,
            )
        if job.control_intent is not ControlIntent.NONE:
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason=f"control_intent:{job.control_intent.value}",
                job_version=job.version,
                now=now,
            )
        if job.deadline <= now:
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason="deadline_expired",
                job_version=job.version,
                now=now,
            )
        if (
            job.scheduler_fencing_token != lease.fencing_token
            or job.result_contract_version != COMPLETE_RESULT_CONTRACT_VERSION
        ):
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason="job_fence_or_contract_mismatch",
                job_version=job.version,
                now=now,
            )
        if (
            job.spec_hash,
            job.spec.code_sha,
            job.spec.dataset_snapshot,
            job.result_contract_version,
        ) != (
            commit.spec_hash,
            commit.code_sha,
            commit.dataset_snapshot,
            commit.result_contract_version,
        ):
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason="job_identity_mismatch",
                job_version=job.version,
                now=now,
            )
        shard_rows = connection.execute(
            "SELECT * FROM lab_shard WHERE job_id = ? ORDER BY shard_index",
            (str(commit.job_id),),
        ).fetchall()
        if not shard_rows or any(
            ShardStatus(str(row["status"])) is not ShardStatus.SUCCEEDED for row in shard_rows
        ):
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason="shards_not_succeeded",
                job_version=job.version,
                now=now,
            )
        shard_identity = {
            (str(row["plan_hash"]), str(row["adapter_id"]), str(row["adapter_version"]))
            for row in shard_rows
        }
        if shard_identity != {(commit.plan_hash, commit.adapter_id, commit.adapter_version)}:
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason="shard_plan_identity_mismatch",
                job_version=job.version,
                now=now,
            )

        next_version = job.version + 1
        receipt = LabArtifactCommitReceipt.from_envelope(
            envelope,
            status="accepted",
            reason="artifact_committed",
            accepted_at=now,
            job_version=next_version,
        )
        self._record_artifact_commit(connection, envelope, receipt, now=now)
        evidence_json = _canonical_model_json(binding.evidence)
        connection.execute(
            """
            INSERT INTO lab_job_result_artifact (
                job_id, commit_request_id, sealed_path, manifest_hash,
                complete_result_hash, bundle_device, bundle_inode,
                evidence_json, indexed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(commit.job_id),
                str(envelope.request_id),
                str(binding.evidence.sealed_path),
                binding.evidence.manifest_hash,
                binding.evidence.complete_result_hash,
                binding.evidence.bundle_device,
                binding.evidence.bundle_inode,
                evidence_json,
                _dump_time(binding.evidence.indexed_at),
            ),
        )
        cursor = connection.execute(
            """
            UPDATE lab_job
            SET status = ?, result_state = ?, version = ?, updated_at = ?
            WHERE job_id = ? AND version = ? AND status = ?
              AND result_state = ? AND control_intent = ?
              AND scheduler_fencing_token = ?
            """,
            (
                JobStatus.SUCCEEDED.value,
                LabResultState.SEALED.value,
                next_version,
                _dump_time(now),
                str(commit.job_id),
                job.version,
                JobStatus.RUNNING.value,
                LabResultState.READY.value,
                ControlIntent.NONE.value,
                lease.fencing_token,
            ),
        )
        if cursor.rowcount != 1:
            raise StaleJobVersionError("job changed while committing complete result artifact")
        self._insert_event(
            connection,
            job_id=commit.job_id,
            request_id=envelope.request_id,
            event_type="job_result_sealed",
            prior_status=JobStatus.RUNNING,
            new_status=JobStatus.SUCCEEDED,
            job_version=next_version,
            reason="verified complete result artifact indexed",
            fencing_token=lease.fencing_token,
            now=now,
        )
        return receipt

    def _validate_staged_artifact_success(
        self,
        connection: sqlite3.Connection,
        envelope: LabArtifactCommitEnvelope,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> None:
        job_row = self._load_job_row(connection, envelope.commit.job_id)
        if job_row is None:
            raise InvalidStoredJobError("staged artifact success lost its job")
        deadline = _load_time(str(job_row["deadline"]))
        if deadline <= now:
            raise ArtifactCommitDeadlineExpiredError(
                "job deadline expired during artifact exit verification"
            )
        job_fence = _strict_nullable_sqlite_int(
            job_row["scheduler_fencing_token"],
            field="lab_job.scheduler_fencing_token",
            minimum=1,
        )
        if job_fence != lease.fencing_token:
            raise SchedulerLeaseFencedError("job fence changed during artifact exit verification")
        if (
            JobStatus(str(job_row["status"])) is not JobStatus.SUCCEEDED
            or LabResultState(str(job_row["result_state"])) is not LabResultState.SEALED
            or ControlIntent(str(job_row["control_intent"])) is not ControlIntent.NONE
        ):
            raise InvalidStoredJobError("staged artifact success changed before SQLite commit")
        indexed = connection.execute(
            "SELECT commit_request_id FROM lab_job_result_artifact WHERE job_id = ?",
            (str(envelope.commit.job_id),),
        ).fetchone()
        if indexed is None or str(indexed["commit_request_id"]) != str(envelope.request_id):
            raise InvalidStoredJobError("staged artifact success lost its result index")

    def stage_artifact_commit(
        self,
        envelope: LabArtifactCommitEnvelope,
        binding: LabVerifiedSealedBinding,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabStagedArtifactCommit:
        from rquant.lab_artifacts import LabVerifiedSealedBinding

        validated = LabArtifactCommitEnvelope.model_validate(envelope)
        verified_binding = LabVerifiedSealedBinding.model_validate(binding)
        current = _utc(now)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            _validate_database_identity(connection, allow_unclaimed_empty=False)
            _validate_v5_schema(connection)
            self._validate_lease(connection, lease, now=current)
            existed_before_apply = (
                connection.execute(
                    "SELECT 1 FROM lab_artifact_commit WHERE request_id = ?",
                    (str(validated.request_id),),
                ).fetchone()
                is not None
            )
            receipt = self._apply_artifact_commit_in_transaction(
                connection,
                validated,
                verified_binding,
                lease=lease,
                now=current,
            )
            staged_new_success = (
                not existed_before_apply
                and receipt.status == "accepted"
                and receipt.reason == "artifact_committed"
            )

            def validate_before_commit(
                final_lease: LabLeaseRecord,
                final_now: datetime,
            ) -> None:
                self._validate_lease(connection, final_lease, now=final_now)
                if staged_new_success:
                    self._validate_staged_artifact_success(
                        connection,
                        validated,
                        lease=final_lease,
                        now=final_now,
                    )

            return LabStagedArtifactCommit(
                connection,
                receipt,
                lease=lease,
                precommit_validator=validate_before_commit,
            )
        except BaseException:
            connection.rollback()
            connection.close()
            raise

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
                    (
                        _dump_time(acquired_at),
                        _strict_sqlite_int(
                            active["lease_id"], field="lab_lease.lease_id", minimum=1
                        ),
                    ),
                )
            latest = connection.execute(
                "SELECT COALESCE(MAX(fencing_token), 0) FROM lab_lease WHERE lease_name = ?",
                (self.LEASE_NAME,),
            ).fetchone()
            fencing_token = (
                _strict_sqlite_int(latest[0], field="lab_lease.max_fencing_token", minimum=0) + 1
            )
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
            lease_id = _strict_sqlite_int(
                cursor.lastrowid,
                field="lab_lease.lastrowid",
                minimum=1,
            )
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
            or _strict_sqlite_int(row["fencing_token"], field="lab_lease.fencing_token", minimum=1)
            != lease.fencing_token
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
        if (
            active is None
            or _strict_sqlite_int(active["lease_id"], field="lab_lease.lease_id", minimum=1)
            != lease.lease_id
        ):
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

    @staticmethod
    def _active_shard_count(connection: sqlite3.Connection, job_id: UUID) -> int:
        value = connection.execute(
            "SELECT COUNT(*) FROM lab_shard WHERE job_id = ? AND status = ?",
            (str(job_id), ShardStatus.RUNNING.value),
        ).fetchone()[0]
        return _strict_sqlite_int(value, field="lab_shard.active_count", minimum=0)

    @staticmethod
    def _terminalize_claimed_shard(
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        target_status: ShardStatus,
        now: datetime,
        result_manifest_hash: str | None = None,
        failure_json: str | None = None,
        telemetry: LabShardTelemetry | None = None,
        completion_sequence: int | None = None,
    ) -> bool:
        if target_status not in {
            ShardStatus.SUCCEEDED,
            ShardStatus.FAILED,
            ShardStatus.CANCELLED,
        }:
            raise ValueError(f"nonterminal shard target: {target_status.value}")
        version = _strict_sqlite_int(row["version"], field="lab_shard.version", minimum=0)
        cursor = connection.execute(
            """
            UPDATE lab_shard
            SET status = ?, version = ?, worker_id = NULL,
                scheduler_fencing_token = NULL, claim_token = NULL,
                claimed_at = NULL, heartbeat_at = NULL,
                lease_expires_at = NULL, result_manifest_hash = ?,
                failure_json = ?, checkpoint_json = NULL,
                finished_at = ?, updated_at = ?, duration_ms = ?,
                throughput_units_per_second = ?, completion_sequence = ?
            WHERE job_id = ? AND shard_id = ? AND version = ? AND status = ?
            """,
            (
                target_status.value,
                version + 1,
                result_manifest_hash,
                failure_json,
                _dump_time(now),
                _dump_time(now),
                telemetry.duration_ms if telemetry is not None else None,
                (telemetry.throughput_units_per_second if telemetry is not None else None),
                completion_sequence,
                str(row["job_id"]),
                str(row["shard_id"]),
                version,
                ShardStatus.RUNNING.value,
            ),
        )
        return cursor.rowcount == 1

    @staticmethod
    def _terminalize_nonterminal_shards(
        connection: sqlite3.Connection,
        job_id: UUID,
        *,
        target_status: ShardStatus,
        now: datetime,
    ) -> int:
        if target_status is not ShardStatus.CANCELLED:
            raise ValueError(f"unsupported bulk shard target: {target_status.value}")
        cursor = connection.execute(
            """
            UPDATE lab_shard
            SET status = ?, version = version + 1,
                worker_id = NULL, scheduler_fencing_token = NULL,
                claim_token = NULL, claimed_at = NULL,
                heartbeat_at = NULL, lease_expires_at = NULL,
                result_manifest_hash = NULL, failure_json = NULL,
                checkpoint_json = NULL, finished_at = ?, updated_at = ?
            WHERE job_id = ? AND status IN (?, ?, ?)
            """,
            (
                target_status.value,
                _dump_time(now),
                _dump_time(now),
                str(job_id),
                ShardStatus.QUEUED.value,
                ShardStatus.RUNNING.value,
                ShardStatus.CHECKPOINTED.value,
            ),
        )
        return cursor.rowcount

    def _fail_job_tree(
        self,
        connection: sqlite3.Connection,
        job_row: sqlite3.Row,
        *,
        failed_shard_id: UUID,
        failed_shard_failure_json: str,
        sibling_failure_json: str,
        recoverable: bool,
        lease: LabLeaseRecord,
        now: datetime,
        reason: str,
    ) -> sqlite3.Row:
        job_id = UUID(str(job_row["job_id"]))
        exhausted_candidate = connection.execute(
            """
            SELECT 1 FROM lab_shard
            WHERE job_id = ? AND status <> ?
              AND attempt_count >= max_attempts
            LIMIT 1
            """,
            (str(job_id), ShardStatus.SUCCEEDED.value),
        ).fetchone()
        tree_recoverable = recoverable and exhausted_candidate is None
        effective_sibling_failure_json = (
            _PARENT_ATTEMPTS_EXHAUSTED_FAILURE_JSON
            if recoverable and not tree_recoverable
            else sibling_failure_json
        )
        job_row = self._adopt_running_job_fence(
            connection,
            job_row,
            lease=lease,
            now=now,
        )
        cursor = connection.execute(
            """
            UPDATE lab_shard
            SET status = ?, version = version + 1,
                worker_id = NULL, scheduler_fencing_token = NULL,
                claim_token = NULL, claimed_at = NULL,
                heartbeat_at = NULL, lease_expires_at = NULL,
                result_manifest_hash = NULL,
                failure_json = CASE WHEN shard_id = ? THEN ? ELSE ? END,
                checkpoint_json = NULL, finished_at = ?, updated_at = ?
            WHERE job_id = ? AND status IN (?, ?, ?)
            """,
            (
                ShardStatus.FAILED.value,
                str(failed_shard_id),
                failed_shard_failure_json,
                effective_sibling_failure_json,
                _dump_time(now),
                _dump_time(now),
                str(job_id),
                ShardStatus.QUEUED.value,
                ShardStatus.RUNNING.value,
                ShardStatus.CHECKPOINTED.value,
            ),
        )
        if cursor.rowcount < 1:
            raise InvalidStoredJobError("failed job has no nonterminal shard to terminalize")
        source = JobStatus(str(job_row["status"]))
        if source in {JobStatus.QUEUED, JobStatus.CHECKPOINTED}:
            stored_version = _strict_sqlite_int(
                job_row["version"], field="lab_job.version", minimum=0
            )
            version = stored_version + 1
            job_cursor = connection.execute(
                """
                UPDATE lab_job
                SET status = ?, control_intent = ?, version = ?, recoverable = ?,
                    scheduler_fencing_token = NULL, result_state = ?, updated_at = ?
                WHERE job_id = ? AND version = ? AND status = ?
                """,
                (
                    JobStatus.FAILED.value,
                    ControlIntent.NONE.value,
                    version,
                    int(tree_recoverable),
                    LabResultState.PENDING.value,
                    _dump_time(now),
                    str(job_id),
                    stored_version,
                    source.value,
                ),
            )
            if job_cursor.rowcount != 1:
                raise StaleJobVersionError(
                    "inactive job changed while terminalizing exhausted shard tree"
                )
            self._insert_event(
                connection,
                job_id=job_id,
                request_id=None,
                event_type="job_failed",
                prior_status=source,
                new_status=JobStatus.FAILED,
                job_version=version,
                reason=reason,
                fencing_token=lease.fencing_token,
                now=now,
            )
            updated = self._load_job_row(connection, job_id)
            assert updated is not None
            return updated
        return self._transition_in_transaction(
            connection,
            job_row,
            target_status=JobStatus.FAILED,
            lease=lease,
            reason=reason,
            now=now,
            request_id=None,
            recoverable=tree_recoverable,
            event_type="job_failed",
        )

    def _fail_job_tree_after_attempts_exhausted(
        self,
        connection: sqlite3.Connection,
        job_row: sqlite3.Row,
        *,
        exhausted_shard_id: UUID,
        lease: LabLeaseRecord,
        now: datetime,
        reason: str,
    ) -> sqlite3.Row:
        return self._fail_job_tree(
            connection,
            job_row,
            failed_shard_id=exhausted_shard_id,
            failed_shard_failure_json=_ATTEMPTS_EXHAUSTED_FAILURE_JSON,
            sibling_failure_json=_PARENT_ATTEMPTS_EXHAUSTED_FAILURE_JSON,
            recoverable=False,
            lease=lease,
            now=now,
            reason=reason,
        )

    def _adopt_running_job_fence(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        lease: LabLeaseRecord,
        now: datetime,
        event_type: str = "scheduler_takeover",
        reason: str = "running shards fenced and reclaimed",
    ) -> sqlite3.Row:
        if JobStatus(str(row["status"])) is not JobStatus.RUNNING:
            return row
        current_fence = _strict_nullable_sqlite_int(
            row["scheduler_fencing_token"],
            field="lab_job.scheduler_fencing_token",
            minimum=1,
        )
        if current_fence == lease.fencing_token:
            return row
        job_id = UUID(str(row["job_id"]))
        version = _strict_sqlite_int(row["version"], field="lab_job.version", minimum=0)
        cursor = connection.execute(
            """
            UPDATE lab_job
            SET version = ?, scheduler_fencing_token = ?, updated_at = ?
            WHERE job_id = ? AND version = ? AND status = ?
            """,
            (
                version + 1,
                lease.fencing_token,
                _dump_time(now),
                str(job_id),
                version,
                JobStatus.RUNNING.value,
            ),
        )
        if cursor.rowcount != 1:
            raise SchedulerLeaseFencedError("running job changed during scheduler takeover")
        self._insert_event(
            connection,
            job_id=job_id,
            request_id=None,
            event_type=event_type,
            prior_status=JobStatus.RUNNING,
            new_status=JobStatus.RUNNING,
            job_version=version + 1,
            reason=reason,
            fencing_token=lease.fencing_token,
            now=now,
        )
        updated = self._load_job_row(connection, job_id)
        assert updated is not None
        return updated

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
            if target_status is JobStatus.CANCELLED and not allow_cancel_confirmation:
                raise CancelConfirmationRequiredError(
                    "running cancellation requires requested confirmation"
                )
        if target_status not in _ALLOWED_TRANSITIONS[source]:
            raise InvalidJobTransitionError(
                f"invalid lab job transition {source.value}->{target_status.value}"
            )
        row_fence = _strict_nullable_sqlite_int(
            row["scheduler_fencing_token"],
            field="lab_job.scheduler_fencing_token",
            minimum=1,
        )
        if source is JobStatus.RUNNING and (row_fence is None or row_fence != lease.fencing_token):
            raise SchedulerLeaseFencedError("running job belongs to a different scheduler fence")
        stored_version = _strict_sqlite_int(row["version"], field="lab_job.version", minimum=0)
        version = stored_version + 1
        attempt_count = _strict_sqlite_int(
            row["attempt_count"], field="lab_job.attempt_count", minimum=0
        )
        if source is JobStatus.QUEUED and target_status is JobStatus.RUNNING:
            attempt_count += 1
        next_recoverable = _strict_sqlite_bool(row["recoverable"], field="lab_job.recoverable")
        if target_status is JobStatus.FAILED:
            next_recoverable = bool(recoverable)
        next_fence = row_fence
        if target_status is JobStatus.RUNNING:
            next_fence = lease.fencing_token
        result_state = LabResultState(str(row["result_state"]))
        if target_status is JobStatus.SUCCEEDED:
            raise InvalidJobTransitionError("job success requires a verified artifact commit")
        elif target_status in {JobStatus.FAILED, JobStatus.CANCELLED}:
            result_state = LabResultState.PENDING
        connection.execute(
            """
            UPDATE lab_job
            SET status = ?, control_intent = ?, version = ?, attempt_count = ?,
                recoverable = ?, scheduler_fencing_token = ?, result_state = ?,
                updated_at = ?
            WHERE job_id = ? AND version = ?
            """,
            (
                target_status.value,
                ControlIntent.NONE.value,
                version,
                attempt_count,
                int(next_recoverable),
                next_fence,
                result_state.value,
                _dump_time(now),
                str(row["job_id"]),
                stored_version,
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
        row_fence = _strict_nullable_sqlite_int(
            row["scheduler_fencing_token"],
            field="lab_job.scheduler_fencing_token",
            minimum=1,
        )
        if status is JobStatus.RUNNING and (row_fence is None or row_fence != lease.fencing_token):
            raise SchedulerLeaseFencedError("running job belongs to a different scheduler fence")
        stored_version = _strict_sqlite_int(row["version"], field="lab_job.version", minimum=0)
        version = stored_version + 1
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
                stored_version,
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
            stored_version = _strict_sqlite_int(row["version"], field="lab_job.version", minimum=0)
            if stored_version != expected_version:
                raise StaleJobVersionError(
                    f"expected job version {expected_version}, found {row['version']}"
                )
            if (
                connection.execute(
                    "SELECT 1 FROM lab_shard WHERE job_id = ? LIMIT 1",
                    (str(job_id),),
                ).fetchone()
                is not None
            ):
                raise InvalidJobTransitionError("sharded jobs require shard control-plane APIs")
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
            stored_version = _strict_sqlite_int(row["version"], field="lab_job.version", minimum=0)
            if stored_version != expected_version:
                raise StaleJobVersionError(
                    f"expected job version {expected_version}, found {row['version']}"
                )
            if (
                JobStatus(str(row["status"])) is not JobStatus.RUNNING
                or ControlIntent(str(row["control_intent"])) is not ControlIntent.CANCEL_REQUESTED
            ):
                raise CancelConfirmationRequiredError("job does not have an active cancel request")
            self._terminalize_nonterminal_shards(
                connection,
                job_id,
                target_status=ShardStatus.CANCELLED,
                now=current,
            )
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
                status, reason, receipt_json, receipt_job_version,
                received_at, applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                receipt.job_version,
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
            "SELECT * FROM lab_command WHERE request_id = ?",
            (str(envelope.request_id),),
        ).fetchone()
        if row is None:
            return None
        record = _command_record_from_row(row, expected_request_id=envelope.request_id)
        if record.content_hash != envelope.content_hash:
            raise RequestContentConflictError(
                f"request_id {envelope.request_id} already has different content"
            )
        return record.receipt

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
            return self._apply_submit_command(
                connection,
                envelope,
                command,
                existing_row=row,
                lease=lease,
                now=now,
            )
        if row is None:
            return self._receipt_for_rejection(
                envelope,
                reason="job_not_found",
                job_version=None,
            )
        version = _strict_sqlite_int(row["version"], field="lab_job.version", minimum=0)
        if version != command.expected_version:
            return self._receipt_for_rejection(
                envelope,
                reason=f"stale_version:{version}",
                job_version=version,
            )
        source = JobStatus(str(row["status"]))
        control_intent = ControlIntent(str(row["control_intent"]))
        if isinstance(command, CancelJobCommand):
            return self._apply_cancel_command(
                connection,
                envelope,
                command,
                row=row,
                version=version,
                source=source,
                lease=lease,
                now=now,
            )
        if isinstance(command, PauseJobCommand):
            return self._apply_pause_command(
                connection,
                envelope,
                command,
                row=row,
                version=version,
                source=source,
                control_intent=control_intent,
                lease=lease,
                now=now,
            )
        if isinstance(command, ResumeJobCommand):
            return self._apply_resume_command(
                connection,
                envelope,
                command,
                row=row,
                version=version,
                source=source,
                control_intent=control_intent,
                lease=lease,
                now=now,
            )
        if isinstance(command, RetryJobCommand):
            return self._apply_retry_command(
                connection,
                envelope,
                command,
                row=row,
                version=version,
                source=source,
                lease=lease,
                now=now,
            )
        raise TypeError(type(command).__name__)  # pragma: no cover

    def _apply_submit_command(
        self,
        connection: sqlite3.Connection,
        envelope: LabCommandEnvelope,
        command: SubmitJobCommand,
        *,
        existing_row: sqlite3.Row | None,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabCommandReceipt:
        if command.spec.schema_version != 2:
            return self._receipt_for_rejection(
                envelope,
                reason="unsupported_spec_version",
                job_version=None,
            )
        if existing_row is not None:
            return self._receipt_for_rejection(
                envelope,
                reason="job_id_reused",
                job_version=_strict_sqlite_int(
                    existing_row["version"], field="lab_job.version", minimum=0
                ),
            )
        spec_json = command.spec.model_dump_json(round_trip=True)
        connection.execute(
            """
            INSERT INTO lab_job (
                job_id, spec_json, spec_hash, job_type, resource_class,
                deadline, status, control_intent, version, attempt_count,
                max_attempts, recoverable, scheduler_fencing_token,
                created_at, updated_at, result_state, requires_complete_result
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 0, NULL, ?, ?, ?, 1)
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
                LabResultState.PENDING.value,
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

    def _apply_cancel_command(
        self,
        connection: sqlite3.Connection,
        envelope: LabCommandEnvelope,
        command: CancelJobCommand,
        *,
        row: sqlite3.Row,
        version: int,
        source: JobStatus,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabCommandReceipt:
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
            shard_count = connection.execute(
                "SELECT COUNT(*) FROM lab_shard WHERE job_id = ?",
                (str(command.job_id),),
            ).fetchone()[0]
            if (
                _strict_sqlite_int(
                    shard_count,
                    field="lab_shard.count",
                    minimum=0,
                )
                > 0
                and self._active_shard_count(connection, command.job_id) == 0
            ):
                self._terminalize_nonterminal_shards(
                    connection,
                    command.job_id,
                    target_status=ShardStatus.CANCELLED,
                    now=now,
                )
                updated = self._transition_in_transaction(
                    connection,
                    row,
                    target_status=JobStatus.CANCELLED,
                    lease=lease,
                    reason=command.reason,
                    now=now,
                    request_id=envelope.request_id,
                    recoverable=None,
                    event_type="job_cancelled",
                    allow_cancel_confirmation=True,
                )
                return LabCommandReceipt(
                    request_id=envelope.request_id,
                    content_hash=envelope.content_hash,
                    job_id=command.job_id,
                    status="applied",
                    reason="cancelled",
                    job_version=_strict_sqlite_int(
                        updated["version"], field="lab_job.version", minimum=0
                    ),
                )
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
                job_version=_strict_sqlite_int(
                    updated["version"], field="lab_job.version", minimum=0
                ),
            )
        self._terminalize_nonterminal_shards(
            connection,
            command.job_id,
            target_status=ShardStatus.CANCELLED,
            now=now,
        )
        updated = self._transition_in_transaction(
            connection,
            row,
            target_status=JobStatus.CANCELLED,
            lease=lease,
            reason=command.reason,
            now=now,
            request_id=envelope.request_id,
            recoverable=None,
            event_type="job_cancelled",
        )
        next_version = _strict_sqlite_int(updated["version"], field="lab_job.version", minimum=0)
        return LabCommandReceipt(
            request_id=envelope.request_id,
            content_hash=envelope.content_hash,
            job_id=command.job_id,
            status="applied",
            reason="cancelled",
            job_version=next_version,
        )

    def _apply_pause_command(
        self,
        connection: sqlite3.Connection,
        envelope: LabCommandEnvelope,
        command: PauseJobCommand,
        *,
        row: sqlite3.Row,
        version: int,
        source: JobStatus,
        control_intent: ControlIntent,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabCommandReceipt:
        if source is not JobStatus.RUNNING:
            return self._receipt_for_rejection(
                envelope,
                reason=f"invalid_state:{source.value}",
                job_version=version,
            )
        result_state = LabResultState(str(row["result_state"]))
        if result_state is LabResultState.READY:
            return self._receipt_for_rejection(
                envelope,
                reason=f"invalid_result_state:{result_state.value}",
                job_version=version,
            )
        if control_intent is not ControlIntent.NONE:
            return self._receipt_for_rejection(
                envelope,
                reason=f"invalid_intent:{control_intent.value}",
                job_version=version,
            )
        shard_count, active_count = connection.execute(
            """
            SELECT COUNT(*), SUM(CASE WHEN status = ? THEN 1 ELSE 0 END)
            FROM lab_shard WHERE job_id = ?
            """,
            (ShardStatus.RUNNING.value, str(command.job_id)),
        ).fetchone()
        if (
            _strict_sqlite_int(shard_count, field="lab_shard.count", minimum=0) > 0
            and _strict_sqlite_int(active_count, field="lab_shard.active_count", minimum=0) == 0
        ):
            updated = self._transition_in_transaction(
                connection,
                row,
                target_status=JobStatus.CHECKPOINTED,
                lease=lease,
                reason=command.reason,
                now=now,
                request_id=envelope.request_id,
                recoverable=None,
                event_type="job_checkpointed",
            )
            return LabCommandReceipt(
                request_id=envelope.request_id,
                content_hash=envelope.content_hash,
                job_id=command.job_id,
                status="applied",
                reason="checkpointed",
                job_version=_strict_sqlite_int(
                    updated["version"], field="lab_job.version", minimum=0
                ),
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
            job_version=_strict_sqlite_int(updated["version"], field="lab_job.version", minimum=0),
        )

    def _apply_resume_command(
        self,
        connection: sqlite3.Connection,
        envelope: LabCommandEnvelope,
        command: ResumeJobCommand,
        *,
        row: sqlite3.Row,
        version: int,
        source: JobStatus,
        control_intent: ControlIntent,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabCommandReceipt:
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
                job_version=_strict_sqlite_int(
                    updated["version"], field="lab_job.version", minimum=0
                ),
            )
        if source is not JobStatus.CHECKPOINTED:
            return self._receipt_for_rejection(
                envelope,
                reason=f"invalid_state:{source.value}",
                job_version=version,
            )
        updated = self._transition_in_transaction(
            connection,
            row,
            target_status=JobStatus.RUNNING,
            lease=lease,
            reason=command.reason,
            now=now,
            request_id=envelope.request_id,
            recoverable=None,
            event_type="job_resumed",
        )
        return LabCommandReceipt(
            request_id=envelope.request_id,
            content_hash=envelope.content_hash,
            job_id=command.job_id,
            status="applied",
            reason="resumed",
            job_version=_strict_sqlite_int(updated["version"], field="lab_job.version", minimum=0),
        )

    def _apply_retry_command(
        self,
        connection: sqlite3.Connection,
        envelope: LabCommandEnvelope,
        command: RetryJobCommand,
        *,
        row: sqlite3.Row,
        version: int,
        source: JobStatus,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabCommandReceipt:
        if source is not JobStatus.FAILED:
            return self._receipt_for_rejection(
                envelope,
                reason=f"invalid_state:{source.value}",
                job_version=version,
            )
        job_recoverable = _strict_sqlite_bool(row["recoverable"], field="lab_job.recoverable")
        exhausted_candidate = connection.execute(
            """
            SELECT 1 FROM lab_shard
            WHERE job_id = ? AND status <> ?
              AND attempt_count >= max_attempts
            LIMIT 1
            """,
            (str(command.job_id), ShardStatus.SUCCEEDED.value),
        ).fetchone()
        if exhausted_candidate is not None:
            if not job_recoverable:
                return self._receipt_for_rejection(
                    envelope,
                    reason="not_recoverable",
                    job_version=version,
                )
            next_version = version + 1
            cursor = connection.execute(
                """
                UPDATE lab_job
                SET recoverable = 0, version = ?, updated_at = ?
                WHERE job_id = ? AND version = ? AND status = ?
                """,
                (
                    next_version,
                    _dump_time(now),
                    str(command.job_id),
                    version,
                    JobStatus.FAILED.value,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleJobVersionError("failed job changed while fencing exhausted shard retry")
            self._insert_event(
                connection,
                job_id=command.job_id,
                request_id=envelope.request_id,
                event_type="job_retry_rejected",
                prior_status=JobStatus.FAILED,
                new_status=JobStatus.FAILED,
                job_version=next_version,
                reason="shard attempts exhausted",
                fencing_token=lease.fencing_token,
                now=now,
            )
            return self._receipt_for_rejection(
                envelope,
                reason="shard_attempts_exhausted",
                job_version=next_version,
            )
        if not job_recoverable:
            return self._receipt_for_rejection(
                envelope,
                reason="not_recoverable",
                job_version=version,
            )
        attempt_count = _strict_sqlite_int(
            row["attempt_count"], field="lab_job.attempt_count", minimum=0
        )
        max_attempts = _strict_sqlite_int(
            row["max_attempts"], field="lab_job.max_attempts", minimum=1
        )
        if attempt_count >= max_attempts:
            return self._receipt_for_rejection(
                envelope,
                reason="attempts_exhausted",
                job_version=version,
            )
        next_version = version + 1
        connection.execute(
            """
            UPDATE lab_shard
            SET status = ?, version = version + 1,
                worker_id = NULL, scheduler_fencing_token = NULL,
                claim_token = NULL, claimed_at = NULL,
                heartbeat_at = NULL, lease_expires_at = NULL,
                result_manifest_hash = NULL, failure_json = NULL,
                finished_at = NULL, checkpoint_json = NULL,
                updated_at = ?
            WHERE job_id = ? AND status <> ?
            """,
            (
                ShardStatus.QUEUED.value,
                _dump_time(now),
                str(command.job_id),
                ShardStatus.SUCCEEDED.value,
            ),
        )
        connection.execute(
            """
            UPDATE lab_job
            SET status = ?, control_intent = ?, version = ?, recoverable = 0,
                scheduler_fencing_token = NULL, result_state = ?, updated_at = ?
            WHERE job_id = ? AND version = ?
            """,
            (
                JobStatus.QUEUED.value,
                ControlIntent.NONE.value,
                next_version,
                LabResultState.PENDING.value,
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

    def plan_job(
        self,
        job_id: UUID,
        definitions: tuple[LabShardDefinition, ...],
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> tuple[LabShardRecord, ...]:
        if not definitions:
            raise ValueError("a shard plan must contain at least one definition")
        validated = tuple(LabShardDefinition.model_validate(item) for item in definitions)
        ordered = tuple(sorted(validated, key=lambda item: item.shard_index))
        if tuple(item.shard_index for item in ordered) != tuple(range(len(ordered))):
            raise ValueError("shard indexes must be unique and contiguous from zero")
        plan_hashes = {item.plan_hash for item in ordered}
        if len(plan_hashes) != 1:
            raise ValueError("all shard definitions must share one plan_hash")
        work_plan_presence = tuple(item.work_plan is not None for item in ordered)
        if any(work_plan_presence) and not all(work_plan_presence):
            raise ValueError("a shard plan cannot mix telemetry and legacy definitions")
        result_contract_version = COMPLETE_RESULT_CONTRACT_VERSION
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            job_row = self._load_job_row(connection, job_id)
            if job_row is None:
                raise KeyError(f"lab job not found: {job_id}")
            existing_rows = connection.execute(
                "SELECT * FROM lab_shard WHERE job_id = ? ORDER BY shard_index",
                (str(job_id),),
            ).fetchall()
            if existing_rows:
                records = tuple(LabJobReader._shard_from_row(row) for row in existing_rows)
                stored_identity = tuple(
                    (
                        record.shard_id,
                        record.shard_index,
                        record.adapter_id,
                        record.adapter_version,
                        record.plan_hash,
                        record.payload_json,
                        record.payload_hash,
                        record.work_plan,
                    )
                    for record in records
                )
                requested_identity = tuple(
                    (
                        item.shard_id,
                        item.shard_index,
                        item.adapter_id,
                        item.adapter_version,
                        item.plan_hash,
                        item.payload_json,
                        item.payload_hash,
                        item.work_plan,
                    )
                    for item in ordered
                )
                if stored_identity != requested_identity:
                    raise ShardPlanConflictError(
                        f"job {job_id} is already bound to a different plan"
                    )
                stored_contract = (
                    str(job_row["result_contract_version"])
                    if job_row["result_contract_version"] is not None
                    else None
                )
                if stored_contract not in {
                    None,
                    RESULT_CONTRACT_VERSION,
                    COMPLETE_RESULT_CONTRACT_VERSION,
                }:
                    raise ShardPlanConflictError(
                        f"job {job_id} result contract does not match its shard plan"
                    )
                return records
            status = JobStatus(str(job_row["status"]))
            if status is not JobStatus.QUEUED:
                raise InvalidJobTransitionError(
                    f"cannot plan lab job while status is {status.value}"
                )
            max_attempts = _strict_sqlite_int(
                job_row["max_attempts"], field="lab_job.max_attempts", minimum=1
            )
            connection.execute(
                "UPDATE lab_job SET result_contract_version = ? WHERE job_id = ?",
                (result_contract_version, str(job_id)),
            )
            for item in ordered:
                work_plan = item.work_plan
                connection.execute(
                    """
                    INSERT INTO lab_shard (
                        shard_id, job_id, shard_index, status, version,
                        attempt_count, max_attempts, plan_hash, adapter_id,
                        adapter_version, payload_json, payload_hash,
                        worker_id, scheduler_fencing_token, claim_token,
                        claim_generation, claimed_at, heartbeat_at,
                        lease_expires_at, result_manifest_hash, failure_json,
                        finished_at, checkpoint_json, created_at, updated_at,
                        phase, work_unit_name, work_units, static_duration_ms,
                        duration_ms, throughput_units_per_second,
                        completion_sequence
                    ) VALUES (
                        ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?,
                        NULL, NULL, NULL, 0, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?,
                        NULL, NULL, NULL
                    )
                    """,
                    (
                        str(item.shard_id),
                        str(job_id),
                        item.shard_index,
                        ShardStatus.QUEUED.value,
                        max_attempts,
                        item.plan_hash,
                        item.adapter_id,
                        item.adapter_version,
                        item.payload_json,
                        item.payload_hash,
                        _dump_time(current),
                        _dump_time(current),
                        work_plan.phase if work_plan is not None else None,
                        work_plan.work_unit_name if work_plan is not None else None,
                        work_plan.work_units if work_plan is not None else None,
                        work_plan.static_duration_ms if work_plan is not None else None,
                    ),
                )
            rows = connection.execute(
                "SELECT * FROM lab_shard WHERE job_id = ? ORDER BY shard_index",
                (str(job_id),),
            ).fetchall()
            records = tuple(LabJobReader._shard_from_row(row) for row in rows)
        return records

    def fail_unplanned_job(
        self,
        job_id: UUID,
        *,
        reason: str,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> bool:
        failure_reason = reason.strip()
        if not failure_reason:
            raise ValueError("unplanned job failure reason must not be empty")
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            row = self._load_job_row(connection, job_id)
            if row is None:
                raise KeyError(f"lab job not found: {job_id}")
            if JobStatus(str(row["status"])) is not JobStatus.QUEUED:
                return False
            shard_count = connection.execute(
                "SELECT COUNT(*) FROM lab_shard WHERE job_id = ?",
                (str(job_id),),
            ).fetchone()[0]
            if _strict_sqlite_int(
                shard_count,
                field="lab_shard.unplanned_count",
                minimum=0,
            ):
                return False
            version = _strict_sqlite_int(row["version"], field="lab_job.version", minimum=0)
            cursor = connection.execute(
                """
                UPDATE lab_job
                SET status = ?, control_intent = ?, version = ?, recoverable = 0,
                    scheduler_fencing_token = NULL, result_state = ?, updated_at = ?
                WHERE job_id = ? AND version = ? AND status = ?
                """,
                (
                    JobStatus.FAILED.value,
                    ControlIntent.NONE.value,
                    version + 1,
                    LabResultState.PENDING.value,
                    _dump_time(current),
                    str(job_id),
                    version,
                    JobStatus.QUEUED.value,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleJobVersionError("unplanned job changed while recording plan failure")
            self._insert_event(
                connection,
                job_id=job_id,
                request_id=None,
                event_type="job_plan_failed",
                prior_status=JobStatus.QUEUED,
                new_status=JobStatus.FAILED,
                job_version=version + 1,
                reason=failure_reason,
                fencing_token=lease.fencing_token,
                now=current,
            )
        return True

    def expire_deadline_jobs(
        self,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> tuple[UUID, ...]:
        current = _utc(now)
        expired: list[UUID] = []
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            rows = connection.execute(
                """
                SELECT * FROM lab_job
                WHERE status IN (?, ?, ?)
                  AND deadline <= ?
                ORDER BY deadline, created_at, job_id
                """,
                (
                    JobStatus.QUEUED.value,
                    JobStatus.RUNNING.value,
                    JobStatus.CHECKPOINTED.value,
                    _dump_time(current),
                ),
            ).fetchall()
            for row in rows:
                job_id = UUID(str(row["job_id"]))
                source = JobStatus(str(row["status"]))
                version = _strict_sqlite_int(row["version"], field="lab_job.version", minimum=0)
                connection.execute(
                    """
                    UPDATE lab_shard
                    SET status = ?, version = version + 1,
                        worker_id = NULL, scheduler_fencing_token = NULL,
                        claim_token = NULL, claimed_at = NULL,
                        heartbeat_at = NULL, lease_expires_at = NULL,
                        result_manifest_hash = NULL, failure_json = ?,
                        checkpoint_json = NULL, finished_at = ?, updated_at = ?
                    WHERE job_id = ? AND status IN (?, ?, ?)
                    """,
                    (
                        ShardStatus.FAILED.value,
                        _DEADLINE_EXCEEDED_FAILURE_JSON,
                        _dump_time(current),
                        _dump_time(current),
                        str(job_id),
                        ShardStatus.QUEUED.value,
                        ShardStatus.RUNNING.value,
                        ShardStatus.CHECKPOINTED.value,
                    ),
                )
                cursor = connection.execute(
                    """
                    UPDATE lab_job
                    SET status = ?, control_intent = ?, version = ?, recoverable = 0,
                        scheduler_fencing_token = NULL, result_state = ?, updated_at = ?
                    WHERE job_id = ? AND version = ? AND status = ?
                    """,
                    (
                        JobStatus.FAILED.value,
                        ControlIntent.NONE.value,
                        version + 1,
                        LabResultState.PENDING.value,
                        _dump_time(current),
                        str(job_id),
                        version,
                        source.value,
                    ),
                )
                if cursor.rowcount != 1:
                    raise StaleJobVersionError(
                        "job changed while applying ResearchRunSpec deadline"
                    )
                self._insert_event(
                    connection,
                    job_id=job_id,
                    request_id=None,
                    event_type="job_deadline_exceeded",
                    prior_status=source,
                    new_status=JobStatus.FAILED,
                    job_version=version + 1,
                    reason="ResearchRunSpec deadline exceeded",
                    fencing_token=lease.fencing_token,
                    now=current,
                )
                expired.append(job_id)
        return tuple(expired)

    def list_unplanned_jobs(self, *, limit: int = 64) -> tuple[LabJobRecord, ...]:
        if limit < 1:
            raise ValueError("unplanned job limit must be positive")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT job.*
                FROM lab_job AS job
                WHERE job.status = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM lab_shard AS shard
                      WHERE shard.job_id = job.job_id
                  )
                ORDER BY job.created_at, job.job_id
                LIMIT ?
                """,
                (JobStatus.QUEUED.value, limit),
            ).fetchall()
        return tuple(LabJobReader._job_from_row(row) for row in rows)

    @staticmethod
    def _definition_from_shard_row(row: sqlite3.Row) -> LabShardDefinition:
        return LabShardDefinition(
            shard_id=UUID(str(row["shard_id"])),
            shard_index=_strict_sqlite_int(
                row["shard_index"], field="lab_shard.shard_index", minimum=0
            ),
            adapter_id=str(row["adapter_id"]),
            adapter_version=str(row["adapter_version"]),
            plan_hash=str(row["plan_hash"]),
            payload_json=str(row["payload_json"]),
            payload_hash=str(row["payload_hash"]),
            work_plan=(
                LabShardWorkPlan(
                    phase=str(row["phase"]),
                    work_unit_name=str(row["work_unit_name"]),
                    work_units=_strict_sqlite_int(
                        row["work_units"],
                        field="lab_shard.work_units",
                        minimum=1,
                        maximum=SQLITE_SIGNED_INTEGER_MAX,
                    ),
                    static_duration_ms=_strict_sqlite_int(
                        row["static_duration_ms"],
                        field="lab_shard.static_duration_ms",
                        minimum=1,
                        maximum=SQLITE_SIGNED_INTEGER_MAX,
                    ),
                )
                if row["phase"] is not None
                else None
            ),
        )

    def _recover_stale_shards_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> set[UUID]:
        stale_rows = connection.execute(
            """
            SELECT s.* FROM lab_shard AS s
            JOIN lab_job AS j ON j.job_id = s.job_id
            WHERE s.status = ?
              AND j.status = ?
              AND (
                s.scheduler_fencing_token IS NULL
                OR s.scheduler_fencing_token <> ?
                OR s.lease_expires_at IS NULL
                OR s.lease_expires_at <= ?
              )
            ORDER BY j.created_at, s.shard_index
            """,
            (
                ShardStatus.RUNNING.value,
                JobStatus.RUNNING.value,
                lease.fencing_token,
                _dump_time(now),
            ),
        ).fetchall()
        reclaimed_job_ids: set[UUID] = set()
        paused_job_ids: set[UUID] = set()
        cancelled_job_ids: set[UUID] = set()
        exhausted_rows = connection.execute(
            """
            SELECT s.job_id, s.shard_id FROM lab_shard AS s
            JOIN lab_job AS j ON j.job_id = s.job_id
            WHERE j.status IN (?, ?, ?) AND j.control_intent <> ?
              AND s.status IN (?, ?)
              AND s.attempt_count >= s.max_attempts
            ORDER BY j.created_at, s.shard_index
            """,
            (
                JobStatus.QUEUED.value,
                JobStatus.RUNNING.value,
                JobStatus.CHECKPOINTED.value,
                ControlIntent.CANCEL_REQUESTED.value,
                ShardStatus.QUEUED.value,
                ShardStatus.CHECKPOINTED.value,
            ),
        ).fetchall()
        failed_job_causes: dict[UUID, UUID] = {}
        for exhausted in exhausted_rows:
            failed_job_causes.setdefault(
                UUID(str(exhausted["job_id"])),
                UUID(str(exhausted["shard_id"])),
            )
        for stale in stale_rows:
            job_id = UUID(str(stale["job_id"]))
            job_row = self._load_job_row(connection, job_id)
            assert job_row is not None
            attempt_count = _strict_sqlite_int(
                stale["attempt_count"], field="lab_shard.attempt_count", minimum=0
            )
            max_attempts = _strict_sqlite_int(
                stale["max_attempts"], field="lab_shard.max_attempts", minimum=1
            )
            if (
                ControlIntent(str(job_row["control_intent"])) is not ControlIntent.CANCEL_REQUESTED
                and attempt_count >= max_attempts
            ):
                failed_job_causes.setdefault(job_id, UUID(str(stale["shard_id"])))
        for stale in stale_rows:
            job_id = UUID(str(stale["job_id"]))
            if job_id in failed_job_causes:
                continue
            job_row = self._load_job_row(connection, job_id)
            assert job_row is not None
            version = _strict_sqlite_int(stale["version"], field="lab_shard.version", minimum=0)
            intent = ControlIntent(str(job_row["control_intent"]))
            if intent is ControlIntent.CANCEL_REQUESTED:
                if self._terminalize_claimed_shard(
                    connection,
                    stale,
                    target_status=ShardStatus.CANCELLED,
                    now=now,
                ):
                    cancelled_job_ids.add(job_id)
                continue
            cursor = connection.execute(
                """
                UPDATE lab_shard
                SET status = ?, version = ?, worker_id = NULL,
                    scheduler_fencing_token = NULL, claim_token = NULL,
                    claimed_at = NULL, heartbeat_at = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE job_id = ? AND shard_id = ? AND version = ? AND status = ?
                """,
                (
                    ShardStatus.QUEUED.value,
                    version + 1,
                    _dump_time(now),
                    str(stale["job_id"]),
                    str(stale["shard_id"]),
                    version,
                    ShardStatus.RUNNING.value,
                ),
            )
            if cursor.rowcount == 1:
                reclaimed_job_ids.add(job_id)
        idle_control_rows = connection.execute(
            """
            SELECT j.job_id, j.control_intent FROM lab_job AS j
            WHERE j.status = ? AND j.control_intent IN (?, ?)
              AND EXISTS (
                SELECT 1 FROM lab_shard AS planned
                WHERE planned.job_id = j.job_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM lab_shard AS s
                WHERE s.job_id = j.job_id AND s.status = ?
              )
            """,
            (
                JobStatus.RUNNING.value,
                ControlIntent.PAUSE_REQUESTED.value,
                ControlIntent.CANCEL_REQUESTED.value,
                ShardStatus.RUNNING.value,
            ),
        ).fetchall()
        for row in idle_control_rows:
            job_id = UUID(str(row["job_id"]))
            if ControlIntent(str(row["control_intent"])) is ControlIntent.PAUSE_REQUESTED:
                paused_job_ids.add(job_id)
            else:
                cancelled_job_ids.add(job_id)
        for failed_job_id in sorted(failed_job_causes, key=str):
            failed_job = self._load_job_row(connection, failed_job_id)
            assert failed_job is not None
            if JobStatus(str(failed_job["status"])) not in {
                JobStatus.QUEUED,
                JobStatus.RUNNING,
                JobStatus.CHECKPOINTED,
            }:
                continue
            self._fail_job_tree_after_attempts_exhausted(
                connection,
                failed_job,
                exhausted_shard_id=failed_job_causes[failed_job_id],
                lease=lease,
                now=now,
                reason="shard attempts exhausted during stale reclaim",
            )
        convergence_job_ids = reclaimed_job_ids | paused_job_ids | cancelled_job_ids
        for job_id in sorted(convergence_job_ids, key=str):
            job_row = self._load_job_row(connection, job_id)
            assert job_row is not None
            if JobStatus(str(job_row["status"])) is not JobStatus.RUNNING:
                continue
            intent = ControlIntent(str(job_row["control_intent"]))
            if self._active_shard_count(connection, job_id) != 0:
                continue
            if intent not in {
                ControlIntent.PAUSE_REQUESTED,
                ControlIntent.CANCEL_REQUESTED,
            }:
                continue
            job_row = self._adopt_running_job_fence(
                connection,
                job_row,
                lease=lease,
                now=now,
            )
            if intent is ControlIntent.PAUSE_REQUESTED:
                self._transition_in_transaction(
                    connection,
                    job_row,
                    target_status=JobStatus.CHECKPOINTED,
                    lease=lease,
                    reason="all active shard leases expired during pause",
                    now=now,
                    request_id=None,
                    recoverable=None,
                    event_type="job_checkpointed",
                )
            else:
                self._terminalize_nonterminal_shards(
                    connection,
                    job_id,
                    target_status=ShardStatus.CANCELLED,
                    now=now,
                )
                self._transition_in_transaction(
                    connection,
                    job_row,
                    target_status=JobStatus.CANCELLED,
                    lease=lease,
                    reason="all active shard leases expired during cancel",
                    now=now,
                    request_id=None,
                    recoverable=None,
                    event_type="job_cancel_confirmed",
                    allow_cancel_confirmation=True,
                )
        return reclaimed_job_ids | paused_job_ids | cancelled_job_ids | set(failed_job_causes)

    def recover_stale_shards(
        self,
        lease: LabLeaseRecord,
        *,
        now: datetime,
    ) -> tuple[UUID, ...]:
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            recovered = self._recover_stale_shards_in_transaction(
                connection,
                lease=lease,
                now=current,
            )
        return tuple(sorted(recovered, key=str))

    def claim_next_shard(
        self,
        *,
        worker_id: str,
        shard_lease_seconds: int,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabShardClaim | None:
        worker = worker_id.strip()
        if not worker:
            raise ValueError("worker_id must not be empty")
        if shard_lease_seconds < 1:
            raise ValueError("shard_lease_seconds must be positive")
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            self._recover_stale_shards_in_transaction(
                connection,
                lease=lease,
                now=current,
            )
            active_worker = connection.execute(
                """
                SELECT 1 FROM lab_shard
                WHERE status = ? AND worker_id = ?
                  AND scheduler_fencing_token = ?
                  AND lease_expires_at > ?
                LIMIT 1
                """,
                (
                    ShardStatus.RUNNING.value,
                    worker,
                    lease.fencing_token,
                    _dump_time(current),
                ),
            ).fetchone()
            if active_worker is not None:
                return None
            claim_cursor = connection.execute(
                """
                SELECT claim_cursor_created_at, claim_cursor_job_id
                FROM lab_scheduler_state
                WHERE state_key = 'claim_job_cursor'
                """
            ).fetchone()
            if claim_cursor is None:
                job_candidate = connection.execute(
                    """
                    SELECT j.job_id, j.created_at
                    FROM lab_job AS j
                    WHERE j.status IN (?, ?)
                      AND j.control_intent = ?
                      AND j.deadline > ?
                      AND EXISTS (
                        SELECT 1 FROM lab_shard AS s
                        WHERE s.job_id = j.job_id
                          AND s.status = ?
                          AND s.attempt_count < s.max_attempts
                      )
                    ORDER BY j.created_at, j.job_id
                    LIMIT 1
                    """,
                    (
                        JobStatus.QUEUED.value,
                        JobStatus.RUNNING.value,
                        ControlIntent.NONE.value,
                        _dump_time(current),
                        ShardStatus.QUEUED.value,
                    ),
                ).fetchone()
            else:
                try:
                    cursor_created_at = _load_time(str(claim_cursor["claim_cursor_created_at"]))
                    cursor_job_id = UUID(str(claim_cursor["claim_cursor_job_id"]))
                except (TypeError, ValueError) as exc:
                    raise InvalidStoredJobError("invalid persisted claim job cursor") from exc
                cursor_created_at_dump = _dump_time(cursor_created_at)
                job_candidate = connection.execute(
                    """
                    SELECT j.job_id, j.created_at
                    FROM lab_job AS j
                    WHERE j.status IN (?, ?)
                      AND j.control_intent = ?
                      AND j.deadline > ?
                      AND EXISTS (
                        SELECT 1 FROM lab_shard AS s
                        WHERE s.job_id = j.job_id
                          AND s.status = ?
                          AND s.attempt_count < s.max_attempts
                      )
                    ORDER BY CASE
                        WHEN j.created_at > ?
                          OR (j.created_at = ? AND j.job_id > ?)
                        THEN 0 ELSE 1 END,
                        j.created_at, j.job_id
                    LIMIT 1
                    """,
                    (
                        JobStatus.QUEUED.value,
                        JobStatus.RUNNING.value,
                        ControlIntent.NONE.value,
                        _dump_time(current),
                        ShardStatus.QUEUED.value,
                        cursor_created_at_dump,
                        cursor_created_at_dump,
                        str(cursor_job_id),
                    ),
                ).fetchone()
            if job_candidate is None:
                return None
            try:
                job_id = UUID(str(job_candidate["job_id"]))
                job_created_at = _load_time(str(job_candidate["created_at"]))
            except (TypeError, ValueError) as exc:
                raise InvalidStoredJobError("invalid claimable job identity") from exc
            row = connection.execute(
                """
                SELECT * FROM lab_shard
                WHERE job_id = ? AND status = ?
                  AND attempt_count < max_attempts
                ORDER BY shard_index, shard_id
                LIMIT 1
                """,
                (
                    str(job_id),
                    ShardStatus.QUEUED.value,
                ),
            ).fetchone()
            if row is None:
                raise InvalidStoredJobError("claimable job has no claimable shard")
            job_row = self._load_job_row(connection, job_id)
            assert job_row is not None
            job_status = JobStatus(str(job_row["status"]))
            if job_status is JobStatus.QUEUED:
                job_row = self._transition_in_transaction(
                    connection,
                    job_row,
                    target_status=JobStatus.RUNNING,
                    lease=lease,
                    reason="first shard claimed",
                    now=current,
                    request_id=None,
                    recoverable=None,
                    event_type="job_started",
                )
            else:
                job_row = self._adopt_running_job_fence(
                    connection,
                    job_row,
                    lease=lease,
                    now=current,
                )
            shard_version = _strict_sqlite_int(row["version"], field="lab_shard.version", minimum=0)
            generation = (
                _strict_sqlite_int(
                    row["claim_generation"],
                    field="lab_shard.claim_generation",
                    minimum=0,
                )
                + 1
            )
            attempt_count = (
                _strict_sqlite_int(row["attempt_count"], field="lab_shard.attempt_count", minimum=0)
                + 1
            )
            claim_token = uuid4()
            expires_at = current + timedelta(seconds=shard_lease_seconds)
            cursor = connection.execute(
                """
                UPDATE lab_shard
                SET status = ?, version = ?, attempt_count = ?, worker_id = ?,
                    scheduler_fencing_token = ?, claim_token = ?,
                    claim_generation = ?, claimed_at = ?, heartbeat_at = ?,
                    lease_expires_at = ?, result_manifest_hash = NULL,
                    failure_json = NULL, finished_at = NULL, updated_at = ?
                WHERE job_id = ? AND shard_id = ? AND version = ? AND status = ?
                """,
                (
                    ShardStatus.RUNNING.value,
                    shard_version + 1,
                    attempt_count,
                    worker,
                    lease.fencing_token,
                    str(claim_token),
                    generation,
                    _dump_time(current),
                    _dump_time(current),
                    _dump_time(expires_at),
                    _dump_time(current),
                    str(row["job_id"]),
                    str(row["shard_id"]),
                    shard_version,
                    ShardStatus.QUEUED.value,
                ),
            )
            if cursor.rowcount != 1:
                return None
            connection.execute(
                """
                INSERT INTO lab_scheduler_state (
                    state_key, claim_cursor_created_at,
                    claim_cursor_job_id, updated_at
                ) VALUES ('claim_job_cursor', ?, ?, ?)
                ON CONFLICT(state_key) DO UPDATE SET
                    claim_cursor_created_at = excluded.claim_cursor_created_at,
                    claim_cursor_job_id = excluded.claim_cursor_job_id,
                    updated_at = excluded.updated_at
                """,
                (
                    _dump_time(job_created_at),
                    str(job_id),
                    _dump_time(current),
                ),
            )
            definition = self._definition_from_shard_row(row)
            spec_hash = str(job_row["spec_hash"])
            claim = LabShardClaim(
                job_id=job_id,
                spec_hash=spec_hash,
                definition=definition,
                worker_id=worker,
                claim_token=claim_token,
                claim_generation=generation,
                scheduler_fencing_token=lease.fencing_token,
                claimed_at=current,
                lease_expires_at=expires_at,
            )
        return claim

    def list_active_claims(
        self,
        lease: LabLeaseRecord,
        *,
        now: datetime,
        initial_lease_seconds: int,
    ) -> tuple[LabShardClaim, ...]:
        if initial_lease_seconds < 1:
            raise ValueError("initial_lease_seconds must be positive")
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            rows = connection.execute(
                """
                SELECT s.*, j.spec_hash AS job_spec_hash
                FROM lab_shard AS s
                JOIN lab_job AS j ON j.job_id = s.job_id
                WHERE s.status = ?
                  AND s.scheduler_fencing_token = ?
                  AND s.lease_expires_at > ?
                ORDER BY s.job_id, s.shard_index, s.shard_id
                """,
                (
                    ShardStatus.RUNNING.value,
                    lease.fencing_token,
                    _dump_time(current),
                ),
            ).fetchall()
            claims: list[LabShardClaim] = []
            for row in rows:
                try:
                    claimed_at = _load_time(str(row["claimed_at"]))
                    claims.append(
                        LabShardClaim(
                            job_id=UUID(str(row["job_id"])),
                            spec_hash=str(row["job_spec_hash"]),
                            definition=self._definition_from_shard_row(row),
                            worker_id=str(row["worker_id"]),
                            claim_token=UUID(str(row["claim_token"])),
                            claim_generation=_strict_sqlite_int(
                                row["claim_generation"],
                                field="lab_shard.claim_generation",
                                minimum=1,
                            ),
                            scheduler_fencing_token=_strict_sqlite_int(
                                row["scheduler_fencing_token"],
                                field="lab_shard.scheduler_fencing_token",
                                minimum=1,
                            ),
                            claimed_at=claimed_at,
                            lease_expires_at=claimed_at + timedelta(seconds=initial_lease_seconds),
                        )
                    )
                except Exception as exc:
                    raise InvalidStoredJobError(
                        f"invalid active claim for shard {row['shard_id']}: {exc}"
                    ) from exc
        return tuple(claims)

    def list_accepted_success_claim_tokens(
        self,
        lease: LabLeaseRecord,
        *,
        now: datetime,
    ) -> frozenset[UUID]:
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            rows = connection.execute(
                """
                SELECT * FROM lab_worker_report
                WHERE status = 'accepted'
                  AND report_type = 'shard_succeeded'
                ORDER BY applied_at, report_id
                """
            ).fetchall()
            tokens: set[UUID] = set()
            for row in rows:
                report_id = UUID(str(row["report_id"]))
                record = _worker_report_record_from_row(
                    row,
                    expected_report_id=report_id,
                )
                if not isinstance(record.report.body, LabShardSucceeded):
                    raise InvalidStoredJobError(
                        f"accepted success report {report_id} has invalid body"
                    )
                tokens.add(record.report.claim_token)
        return frozenset(tokens)

    def accepted_success_claim_tokens_for(
        self,
        lease: LabLeaseRecord,
        *,
        now: datetime,
        claims: tuple[LabShardClaim, ...],
    ) -> frozenset[UUID]:
        """Return accepted success evidence only for a bounded authority batch."""
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            tokens: set[UUID] = set()
            for claim in claims:
                rows = connection.execute(
                    """
                    SELECT * FROM lab_worker_report
                    WHERE job_id = ? AND shard_id = ?
                      AND claim_generation = ?
                      AND scheduler_fencing_token = ?
                      AND status = 'accepted'
                      AND report_type = 'shard_succeeded'
                    ORDER BY applied_at, report_id
                    LIMIT 2
                    """,
                    (
                        str(claim.job_id),
                        str(claim.shard_id),
                        claim.claim_generation,
                        claim.scheduler_fencing_token,
                    ),
                ).fetchall()
                for row in rows:
                    report_id = UUID(str(row["report_id"]))
                    record = _worker_report_record_from_row(
                        row,
                        expected_report_id=report_id,
                    )
                    if not isinstance(record.report.body, LabShardSucceeded):
                        raise InvalidStoredJobError(
                            f"accepted success report {report_id} has invalid body"
                        )
                    if record.report.claim_token == claim.claim_token:
                        tokens.add(claim.claim_token)
        return frozenset(tokens)

    @staticmethod
    def _report_receipt(
        report: LabWorkerReport,
        *,
        status: Literal["accepted", "rejected"],
        reason: str,
        now: datetime,
    ) -> LabReportReceipt:
        return LabReportReceipt.from_report(
            report,
            status=status,
            reason=reason,
            accepted_at=now,
        )

    @staticmethod
    def _record_worker_report(
        connection: sqlite3.Connection,
        report: LabWorkerReport,
        receipt: LabReportReceipt,
        *,
        now: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO lab_worker_report (
                report_id, content_hash, job_id, shard_id, report_type,
                report_json, status, reason, receipt_json, claim_generation,
                scheduler_fencing_token, received_at, applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(report.report_id),
                report.content_hash,
                str(report.job_id),
                str(report.shard_id),
                report.body.report_type,
                report.model_dump_json(),
                receipt.status,
                receipt.reason,
                receipt.model_dump_json(),
                report.claim_generation,
                report.scheduler_fencing_token,
                _dump_time(now),
                _dump_time(now),
            ),
        )

    @staticmethod
    def _worker_report_rejection_reason(
        report: LabWorkerReport,
        *,
        lease: LabLeaseRecord,
        job_row: sqlite3.Row,
        shard_row: sqlite3.Row,
        now: datetime,
    ) -> str | None:
        if str(shard_row["job_id"]) != str(report.job_id):
            return "job_shard_mismatch"
        if str(job_row["spec_hash"]) != report.spec_hash:
            return "spec_hash_mismatch"
        if str(shard_row["payload_hash"]) != report.payload_hash:
            return "payload_hash_mismatch"
        if report.scheduler_fencing_token != lease.fencing_token:
            return "stale_scheduler_fence"
        if (
            _strict_nullable_sqlite_int(
                shard_row["scheduler_fencing_token"],
                field="lab_shard.scheduler_fencing_token",
                minimum=1,
            )
            != report.scheduler_fencing_token
        ):
            return "stale_shard_fence"
        if ShardStatus(str(shard_row["status"])) is not ShardStatus.RUNNING:
            return f"invalid_shard_state:{shard_row['status']}"
        if str(shard_row["worker_id"] or "") != report.worker_id:
            return "stale_claim_worker"
        if str(shard_row["claim_token"] or "") != str(report.claim_token):
            return "stale_claim_token"
        if (
            _strict_sqlite_int(
                shard_row["claim_generation"],
                field="lab_shard.claim_generation",
                minimum=0,
            )
            != report.claim_generation
        ):
            return "stale_claim_generation"
        if (
            shard_row["lease_expires_at"] is None
            or _load_time(str(shard_row["lease_expires_at"])) <= now
        ):
            return "claim_lease_expired"
        if JobStatus(str(job_row["status"])) is not JobStatus.RUNNING:
            return f"invalid_job_state:{job_row['status']}"
        if min(now, _utc(report.reported_at)) < max(
            _load_time(str(shard_row["claimed_at"])),
            _load_time(str(shard_row["heartbeat_at"])),
        ):
            return "backdated_report"
        intent = ControlIntent(str(job_row["control_intent"]))
        if intent is ControlIntent.CANCEL_REQUESTED and not isinstance(
            report.body, LabWorkerStopped
        ):
            return "cancel_requested"
        if isinstance(report.body, LabShardSucceeded):
            expected_plan = LabJobStore._definition_from_shard_row(shard_row).work_plan
            reported_telemetry = report.body.telemetry
            if expected_plan is None:
                if reported_telemetry is not None:
                    return "unexpected_shard_telemetry"
            elif reported_telemetry is None:
                return "missing_shard_telemetry"
            else:
                reported_plan = LabShardWorkPlan(
                    phase=reported_telemetry.phase,
                    work_unit_name=reported_telemetry.work_unit_name,
                    work_units=reported_telemetry.work_units,
                    static_duration_ms=reported_telemetry.static_duration_ms,
                )
                if reported_plan != expected_plan:
                    return "shard_telemetry_plan_mismatch"
        return None

    def _apply_heartbeat_report(
        self,
        connection: sqlite3.Connection,
        report: LabWorkerReport,
        body: LabShardHeartbeat,
        *,
        shard_row: sqlite3.Row,
        shard_version: int,
        now: datetime,
    ) -> str:
        existing_expiry = _load_time(str(shard_row["lease_expires_at"]))
        expires_at = max(
            existing_expiry,
            now + timedelta(seconds=body.lease_extension_seconds),
        )
        connection.execute(
            """
            UPDATE lab_shard
            SET heartbeat_at = ?, lease_expires_at = ?, version = ?, updated_at = ?
            WHERE job_id = ? AND shard_id = ? AND version = ?
            """,
            (
                _dump_time(now),
                _dump_time(expires_at),
                shard_version + 1,
                _dump_time(now),
                str(report.job_id),
                str(report.shard_id),
                shard_version,
            ),
        )
        return "heartbeat_extended"

    def _apply_succeeded_report(
        self,
        connection: sqlite3.Connection,
        report: LabWorkerReport,
        body: LabShardSucceeded,
        *,
        lease: LabLeaseRecord,
        job_row: sqlite3.Row,
        shard_row: sqlite3.Row,
        now: datetime,
    ) -> str:
        completion_sequence: int | None = None
        if body.telemetry is not None:
            latest = connection.execute(
                """
                SELECT MAX(completion_sequence) FROM lab_shard
                WHERE job_id = ? AND status = 'succeeded'
                  AND completion_sequence IS NOT NULL
                """,
                (str(report.job_id),),
            ).fetchone()[0]
            completion_sequence = (
                0
                if latest is None
                else _strict_sqlite_int(
                    latest,
                    field="lab_shard.max_completion_sequence",
                    minimum=1,
                )
            ) + 1
        terminalized = self._terminalize_claimed_shard(
            connection,
            shard_row,
            target_status=ShardStatus.SUCCEEDED,
            now=now,
            result_manifest_hash=body.result_manifest_hash,
            telemetry=body.telemetry,
            completion_sequence=completion_sequence,
        )
        assert terminalized
        remaining = connection.execute(
            """
            SELECT COUNT(*) FROM lab_shard
            WHERE job_id = ? AND status <> ?
            """,
            (str(report.job_id), ShardStatus.SUCCEEDED.value),
        ).fetchone()[0]
        remaining_count = _strict_sqlite_int(
            remaining, field="lab_shard.remaining_count", minimum=0
        )
        if remaining_count == 0:
            if job_row["result_contract_version"] == COMPLETE_RESULT_CONTRACT_VERSION:
                stored_version = _strict_sqlite_int(
                    job_row["version"], field="lab_job.version", minimum=0
                )
                row_fence = _strict_nullable_sqlite_int(
                    job_row["scheduler_fencing_token"],
                    field="lab_job.scheduler_fencing_token",
                    minimum=1,
                )
                if row_fence != lease.fencing_token:
                    raise SchedulerLeaseFencedError(
                        "running job belongs to a different scheduler fence"
                    )
                next_version = stored_version + 1
                cursor = connection.execute(
                    """
                    UPDATE lab_job
                    SET control_intent = ?, result_state = ?, version = ?, updated_at = ?
                    WHERE job_id = ? AND version = ? AND status = ?
                    """,
                    (
                        ControlIntent.NONE.value,
                        LabResultState.READY.value,
                        next_version,
                        _dump_time(now),
                        str(report.job_id),
                        stored_version,
                        JobStatus.RUNNING.value,
                    ),
                )
                if cursor.rowcount != 1:
                    raise StaleJobVersionError("job changed while marking result ready")
                self._insert_event(
                    connection,
                    job_id=report.job_id,
                    request_id=None,
                    event_type="job_result_ready",
                    prior_status=JobStatus.RUNNING,
                    new_status=JobStatus.RUNNING,
                    job_version=next_version,
                    reason="all shards succeeded; complete result artifact required",
                    fencing_token=lease.fencing_token,
                    now=now,
                )
            else:
                self._transition_in_transaction(
                    connection,
                    job_row,
                    target_status=JobStatus.FAILED,
                    lease=lease,
                    reason=(
                        "all shards succeeded but the legacy result contract cannot "
                        "produce a complete artifact"
                    ),
                    now=now,
                    request_id=None,
                    recoverable=False,
                    event_type="job_failed_legacy_result_contract",
                )
        elif ControlIntent(str(job_row["control_intent"])) is ControlIntent.PAUSE_REQUESTED:
            active_count = connection.execute(
                "SELECT COUNT(*) FROM lab_shard WHERE job_id = ? AND status = ?",
                (str(report.job_id), ShardStatus.RUNNING.value),
            ).fetchone()[0]
            if (
                _strict_sqlite_int(
                    active_count,
                    field="lab_shard.active_count",
                    minimum=0,
                )
                == 0
            ):
                self._transition_in_transaction(
                    connection,
                    job_row,
                    target_status=JobStatus.CHECKPOINTED,
                    lease=lease,
                    reason="pause boundary reached",
                    now=now,
                    request_id=None,
                    recoverable=None,
                    event_type="job_checkpointed",
                )
        return "shard_succeeded"

    def _apply_failed_report(
        self,
        connection: sqlite3.Connection,
        report: LabWorkerReport,
        body: LabShardFailed,
        *,
        lease: LabLeaseRecord,
        job_row: sqlite3.Row,
        shard_row: sqlite3.Row,
        now: datetime,
    ) -> str:
        attempt_count = _strict_sqlite_int(
            shard_row["attempt_count"],
            field="lab_shard.attempt_count",
            minimum=0,
        )
        max_attempts = _strict_sqlite_int(
            shard_row["max_attempts"],
            field="lab_shard.max_attempts",
            minimum=1,
        )
        if attempt_count >= max_attempts:
            self._fail_job_tree_after_attempts_exhausted(
                connection,
                job_row,
                exhausted_shard_id=report.shard_id,
                lease=lease,
                now=now,
                reason="worker reported exhausted shard failure",
            )
            return "shard_failed_attempts_exhausted"
        self._fail_job_tree(
            connection,
            job_row,
            failed_shard_id=report.shard_id,
            failed_shard_failure_json=body.failure_json,
            sibling_failure_json=_PARENT_RECOVERABLE_FAILURE_JSON,
            recoverable=True,
            lease=lease,
            now=now,
            reason="worker reported shard failure",
        )
        return "shard_failed"

    def _apply_worker_stopped_report(
        self,
        connection: sqlite3.Connection,
        report: LabWorkerReport,
        body: LabWorkerStopped,
        *,
        lease: LabLeaseRecord,
        job_row: sqlite3.Row,
        shard_row: sqlite3.Row,
        shard_version: int,
        now: datetime,
    ) -> str:
        del body
        intent = ControlIntent(str(job_row["control_intent"]))
        if intent is ControlIntent.CANCEL_REQUESTED:
            terminalized = self._terminalize_claimed_shard(
                connection,
                shard_row,
                target_status=ShardStatus.CANCELLED,
                now=now,
            )
            assert terminalized
            self._terminalize_nonterminal_shards(
                connection,
                report.job_id,
                target_status=ShardStatus.CANCELLED,
                now=now,
            )
            if self._active_shard_count(connection, report.job_id) == 0:
                self._transition_in_transaction(
                    connection,
                    job_row,
                    target_status=JobStatus.CANCELLED,
                    lease=lease,
                    reason="all worker claims stopped",
                    now=now,
                    request_id=None,
                    recoverable=None,
                    event_type="job_cancel_confirmed",
                    allow_cancel_confirmation=True,
                )
            return "worker_stopped_cancelled"
        attempt_count = _strict_sqlite_int(
            shard_row["attempt_count"],
            field="lab_shard.attempt_count",
            minimum=0,
        )
        max_attempts = _strict_sqlite_int(
            shard_row["max_attempts"],
            field="lab_shard.max_attempts",
            minimum=1,
        )
        if attempt_count >= max_attempts:
            self._fail_job_tree_after_attempts_exhausted(
                connection,
                job_row,
                exhausted_shard_id=report.shard_id,
                lease=lease,
                now=now,
                reason="shard attempts exhausted after worker stopped",
            )
            return "worker_stopped_attempts_exhausted"
        connection.execute(
            """
            UPDATE lab_shard
            SET status = ?, version = ?, worker_id = NULL,
                scheduler_fencing_token = NULL, claim_token = NULL,
                claimed_at = NULL, heartbeat_at = NULL,
                lease_expires_at = NULL, updated_at = ?
            WHERE job_id = ? AND shard_id = ? AND version = ?
            """,
            (
                ShardStatus.QUEUED.value,
                shard_version + 1,
                _dump_time(now),
                str(report.job_id),
                str(report.shard_id),
                shard_version,
            ),
        )
        if (
            intent is ControlIntent.PAUSE_REQUESTED
            and self._active_shard_count(connection, report.job_id) == 0
        ):
            self._transition_in_transaction(
                connection,
                job_row,
                target_status=JobStatus.CHECKPOINTED,
                lease=lease,
                reason="worker stopped at pause boundary",
                now=now,
                request_id=None,
                recoverable=None,
                event_type="job_checkpointed",
            )
        return "worker_stopped"

    def _apply_worker_report_body(
        self,
        connection: sqlite3.Connection,
        report: LabWorkerReport,
        *,
        lease: LabLeaseRecord,
        job_row: sqlite3.Row,
        shard_row: sqlite3.Row,
        now: datetime,
    ) -> str:
        shard_version = _strict_sqlite_int(
            shard_row["version"], field="lab_shard.version", minimum=0
        )
        body = report.body
        if isinstance(body, LabShardHeartbeat):
            return self._apply_heartbeat_report(
                connection,
                report,
                body,
                shard_row=shard_row,
                shard_version=shard_version,
                now=now,
            )
        if isinstance(body, LabShardSucceeded):
            return self._apply_succeeded_report(
                connection,
                report,
                body,
                lease=lease,
                job_row=job_row,
                shard_row=shard_row,
                now=now,
            )
        if isinstance(body, LabShardFailed):
            return self._apply_failed_report(
                connection,
                report,
                body,
                lease=lease,
                job_row=job_row,
                shard_row=shard_row,
                now=now,
            )
        if isinstance(body, LabWorkerStopped):
            return self._apply_worker_stopped_report(
                connection,
                report,
                body,
                lease=lease,
                job_row=job_row,
                shard_row=shard_row,
                shard_version=shard_version,
                now=now,
            )
        raise TypeError(type(body).__name__)  # pragma: no cover

    def apply_worker_report(
        self,
        report: LabWorkerReport,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabReportReceipt:
        validated = LabWorkerReport.model_validate(report)
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            existing = connection.execute(
                "SELECT * FROM lab_worker_report WHERE report_id = ?",
                (str(validated.report_id),),
            ).fetchone()
            if existing is not None:
                record = _worker_report_record_from_row(
                    existing, expected_report_id=validated.report_id
                )
                if record.report.content_hash != validated.content_hash:
                    raise RequestContentConflictError(
                        f"report_id {validated.report_id} already has different content"
                    )
                return record.receipt
            shard_row = connection.execute(
                "SELECT * FROM lab_shard WHERE job_id = ? AND shard_id = ?",
                (str(validated.job_id), str(validated.shard_id)),
            ).fetchone()
            job_row = self._load_job_row(connection, validated.job_id)
            if job_row is None or shard_row is None:
                receipt = self._report_receipt(
                    validated,
                    status="rejected",
                    reason="job_not_found" if job_row is None else "shard_not_found",
                    now=current,
                )
                self._record_worker_report(connection, validated, receipt, now=current)
                return receipt

            rejection = self._worker_report_rejection_reason(
                validated,
                lease=lease,
                job_row=job_row,
                shard_row=shard_row,
                now=current,
            )
            if rejection is not None:
                receipt = self._report_receipt(
                    validated,
                    status="rejected",
                    reason=rejection,
                    now=current,
                )
                self._record_worker_report(connection, validated, receipt, now=current)
                return receipt

            reason = self._apply_worker_report_body(
                connection,
                validated,
                lease=lease,
                job_row=job_row,
                shard_row=shard_row,
                now=current,
            )
            receipt = self._report_receipt(
                validated,
                status="accepted",
                reason=reason,
                now=current,
            )
            self._record_worker_report(connection, validated, receipt, now=current)
        return receipt

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
                  AND NOT EXISTS (
                    SELECT 1 FROM lab_shard
                    WHERE lab_shard.job_id = lab_job.job_id
                  )
                  AND (
                    scheduler_fencing_token IS NULL
                    OR scheduler_fencing_token <> ?
                  )
                ORDER BY created_at, job_id
                """,
                (JobStatus.RUNNING.value, lease.fencing_token),
            ).fetchall()
            for row in rows:
                stored_version = _strict_sqlite_int(
                    row["version"], field="lab_job.version", minimum=0
                )
                version = stored_version + 1
                _strict_nullable_sqlite_int(
                    row["scheduler_fencing_token"],
                    field="lab_job.scheduler_fencing_token",
                    minimum=1,
                )
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
                        scheduler_fencing_token = ?, result_state = ?, updated_at = ?
                    WHERE job_id = ? AND version = ?
                    """,
                    (
                        target_status.value,
                        ControlIntent.NONE.value,
                        version,
                        lease.fencing_token,
                        LabResultState.PENDING.value,
                        _dump_time(current),
                        str(row["job_id"]),
                        stored_version,
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
            ready_rows = connection.execute(
                """
                SELECT * FROM lab_job AS job
                WHERE job.status = ? AND job.result_state = ?
                  AND job.result_contract_version = ?
                  AND job.control_intent = ?
                  AND job.scheduler_fencing_token <> ?
                  AND EXISTS (
                    SELECT 1 FROM lab_shard AS shard
                    WHERE shard.job_id = job.job_id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM lab_shard AS shard
                    WHERE shard.job_id = job.job_id AND shard.status <> ?
                  )
                ORDER BY job.created_at, job.job_id
                """,
                (
                    JobStatus.RUNNING.value,
                    LabResultState.READY.value,
                    COMPLETE_RESULT_CONTRACT_VERSION,
                    ControlIntent.NONE.value,
                    lease.fencing_token,
                    ShardStatus.SUCCEEDED.value,
                ),
            ).fetchall()
            for row in ready_rows:
                updated = self._adopt_running_job_fence(
                    connection,
                    row,
                    lease=lease,
                    now=current,
                    event_type="job_result_ready_recovered",
                    reason="ready result adopted by replacement scheduler",
                )
                recovered.append(LabJobReader._job_from_row(updated))
        return tuple(recovered)


_STATUS_VALUES = ",".join(f"'{status.value}'" for status in JobStatus)
_CONTROL_INTENT_VALUES = ",".join(f"'{intent.value}'" for intent in ControlIntent)
_SHARD_STATUS_VALUES = ",".join(f"'{status.value}'" for status in ShardStatus)
_V2_SCHEMA_STATEMENTS = (
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
        receipt_job_version INTEGER CHECK (
            receipt_job_version IS NULL OR (
                typeof(receipt_job_version) = 'integer'
                AND receipt_job_version >= 0
            )
        ),
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

_V3_SHARD_TABLE_STATEMENT = f"""
CREATE TABLE IF NOT EXISTS lab_shard (
    shard_id TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES lab_job(job_id) ON DELETE CASCADE,
    shard_index INTEGER NOT NULL CHECK (
        typeof(shard_index) = 'integer' AND shard_index >= 0
    ),
    status TEXT NOT NULL CHECK (status IN ({_SHARD_STATUS_VALUES})),
    version INTEGER NOT NULL CHECK (typeof(version) = 'integer' AND version >= 0),
    attempt_count INTEGER NOT NULL CHECK (
        typeof(attempt_count) = 'integer' AND attempt_count >= 0
    ),
    max_attempts INTEGER NOT NULL CHECK (
        typeof(max_attempts) = 'integer' AND max_attempts >= 1
    ),
    plan_hash TEXT NOT NULL DEFAULT '{_LEGACY_PLAN_HASH}',
    adapter_id TEXT NOT NULL DEFAULT 'legacy-v2',
    adapter_version TEXT NOT NULL DEFAULT 'v0',
    payload_json TEXT NOT NULL DEFAULT '{_EMPTY_PAYLOAD_JSON}',
    payload_hash TEXT NOT NULL DEFAULT '{_EMPTY_PAYLOAD_HASH}',
    worker_id TEXT,
    scheduler_fencing_token INTEGER,
    claim_token TEXT,
    claim_generation INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(claim_generation) = 'integer' AND claim_generation >= 0
    ),
    claimed_at TEXT,
    heartbeat_at TEXT,
    lease_expires_at TEXT,
    result_manifest_hash TEXT,
    failure_json TEXT,
    finished_at TEXT,
    checkpoint_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, shard_id),
    UNIQUE (job_id, shard_index)
)
"""

_V4_JOB_TABLE_STATEMENT = f"""
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
    updated_at TEXT NOT NULL,
    result_contract_version TEXT CHECK (
        result_contract_version IS NULL
        OR (typeof(result_contract_version) = 'text'
            AND length(result_contract_version) > 0)
    )
)
"""

_RESULT_STATE_VALUES = ",".join(f"'{state.value}'" for state in LabResultState)
_V5_JOB_TABLE_STATEMENT = f"""
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
    updated_at TEXT NOT NULL,
    result_contract_version TEXT CHECK (
        result_contract_version IS NULL
        OR (typeof(result_contract_version) = 'text'
            AND length(result_contract_version) > 0)
    ),
    result_state TEXT NOT NULL DEFAULT 'pending'
        CHECK (result_state IN ({_RESULT_STATE_VALUES})),
    requires_complete_result INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(requires_complete_result) = 'integer'
        AND requires_complete_result IN (0, 1)
    )
)
"""

_V4_SHARD_TABLE_STATEMENT = f"""
CREATE TABLE IF NOT EXISTS lab_shard (
    shard_id TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES lab_job(job_id) ON DELETE CASCADE,
    shard_index INTEGER NOT NULL CHECK (
        typeof(shard_index) = 'integer' AND shard_index >= 0
    ),
    status TEXT NOT NULL CHECK (status IN ({_SHARD_STATUS_VALUES})),
    version INTEGER NOT NULL CHECK (typeof(version) = 'integer' AND version >= 0),
    attempt_count INTEGER NOT NULL CHECK (
        typeof(attempt_count) = 'integer' AND attempt_count >= 0
    ),
    max_attempts INTEGER NOT NULL CHECK (
        typeof(max_attempts) = 'integer' AND max_attempts >= 1
    ),
    plan_hash TEXT NOT NULL DEFAULT '{_LEGACY_PLAN_HASH}',
    adapter_id TEXT NOT NULL DEFAULT 'legacy-v2',
    adapter_version TEXT NOT NULL DEFAULT 'v0',
    payload_json TEXT NOT NULL DEFAULT '{_EMPTY_PAYLOAD_JSON}',
    payload_hash TEXT NOT NULL DEFAULT '{_EMPTY_PAYLOAD_HASH}',
    worker_id TEXT,
    scheduler_fencing_token INTEGER,
    claim_token TEXT,
    claim_generation INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(claim_generation) = 'integer' AND claim_generation >= 0
    ),
    claimed_at TEXT,
    heartbeat_at TEXT,
    lease_expires_at TEXT,
    result_manifest_hash TEXT,
    failure_json TEXT,
    finished_at TEXT,
    checkpoint_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    phase TEXT CHECK (
        phase IS NULL OR (typeof(phase) = 'text' AND length(phase) > 0)
    ),
    work_unit_name TEXT CHECK (
        work_unit_name IS NULL
        OR (typeof(work_unit_name) = 'text' AND length(work_unit_name) > 0)
    ),
    work_units INTEGER CHECK (
        work_units IS NULL
        OR (typeof(work_units) = 'integer'
            AND work_units >= 1
            AND work_units <= {SQLITE_SIGNED_INTEGER_MAX})
    ),
    static_duration_ms INTEGER CHECK (
        (phase IS NULL AND work_unit_name IS NULL
         AND work_units IS NULL AND static_duration_ms IS NULL)
        OR
        (phase IS NOT NULL AND work_unit_name IS NOT NULL
         AND work_units IS NOT NULL
         AND typeof(static_duration_ms) = 'integer'
         AND static_duration_ms >= 1
         AND static_duration_ms <= {SQLITE_SIGNED_INTEGER_MAX})
    ),
    duration_ms REAL CHECK (
        duration_ms IS NULL
        OR (typeof(duration_ms) IN ('integer', 'real')
            AND duration_ms >= {LAB_SHARD_DURATION_MS_MIN}
            AND duration_ms < {LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE})
    ),
    throughput_units_per_second REAL CHECK (
        (duration_ms IS NULL AND throughput_units_per_second IS NULL)
        OR
        (duration_ms IS NOT NULL
         AND typeof(throughput_units_per_second) IN ('integer', 'real')
         AND throughput_units_per_second > 0
         AND throughput_units_per_second < {LAB_SHARD_THROUGHPUT_MAX_EXCLUSIVE})
    ),
    completion_sequence INTEGER CHECK (
        completion_sequence IS NULL
        OR (typeof(completion_sequence) = 'integer'
            AND completion_sequence >= 1
            AND status = 'succeeded'
            AND duration_ms IS NOT NULL
            AND throughput_units_per_second IS NOT NULL)
    ),
    PRIMARY KEY (job_id, shard_id),
    UNIQUE (job_id, shard_index)
)
"""

_V3_ARTIFACT_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_artifact (
    artifact_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES lab_job(job_id) ON DELETE CASCADE,
    shard_id TEXT,
    artifact_type TEXT NOT NULL,
    uri TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id, shard_id)
        REFERENCES lab_shard(job_id, shard_id) ON DELETE CASCADE
)
"""

_V3_REPORT_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_worker_report (
    report_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    job_id TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    report_type TEXT NOT NULL CHECK (
        report_type IN ('heartbeat', 'shard_succeeded', 'shard_failed', 'worker_stopped')
    ),
    report_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('accepted', 'rejected')),
    reason TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    claim_generation INTEGER NOT NULL CHECK (
        typeof(claim_generation) = 'integer' AND claim_generation >= 1
    ),
    scheduler_fencing_token INTEGER NOT NULL CHECK (
        typeof(scheduler_fencing_token) = 'integer'
        AND scheduler_fencing_token >= 1
    ),
    received_at TEXT NOT NULL,
    applied_at TEXT NOT NULL
)
"""

_V3_REPORT_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_worker_report_shard
ON lab_worker_report(job_id, shard_id, applied_at)
"""

_V4_COMPLETION_INDEX_STATEMENT = """
CREATE UNIQUE INDEX IF NOT EXISTS ix_lab_shard_job_completion_sequence
ON lab_shard(job_id, completion_sequence DESC)
WHERE status = 'succeeded' AND completion_sequence IS NOT NULL
"""

_V4_STATUS_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_shard_job_status_index
ON lab_shard(job_id, status, shard_index)
"""

_V3_SCHEDULER_STATE_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_scheduler_state (
    state_key TEXT PRIMARY KEY CHECK (
        typeof(state_key) = 'text' AND state_key = 'claim_job_cursor'
    ),
    claim_cursor_created_at TEXT NOT NULL CHECK (
        typeof(claim_cursor_created_at) = 'text'
    ),
    claim_cursor_job_id TEXT NOT NULL CHECK (
        typeof(claim_cursor_job_id) = 'text'
    ),
    updated_at TEXT NOT NULL CHECK (typeof(updated_at) = 'text')
)
"""

_V5_ARTIFACT_COMMIT_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_artifact_commit (
    request_id TEXT PRIMARY KEY CHECK (
        typeof(request_id) = 'text' AND length(request_id) = 36
    ),
    content_hash TEXT NOT NULL CHECK (
        typeof(content_hash) = 'text' AND length(content_hash) = 64
        AND content_hash NOT GLOB '*[^0-9a-f]*'
    ),
    job_id TEXT NOT NULL CHECK (
        typeof(job_id) = 'text' AND length(job_id) = 36
    ),
    commit_json TEXT NOT NULL CHECK (
        typeof(commit_json) = 'text' AND length(commit_json) > 0
        AND json_valid(commit_json)
    ),
    status TEXT NOT NULL CHECK (status IN ('accepted', 'rejected')),
    reason TEXT NOT NULL CHECK (typeof(reason) = 'text' AND length(reason) > 0),
    receipt_json TEXT NOT NULL CHECK (
        typeof(receipt_json) = 'text' AND length(receipt_json) > 0
        AND json_valid(receipt_json)
    ),
    receipt_job_version INTEGER CHECK (
        receipt_job_version IS NULL
        OR (typeof(receipt_job_version) = 'integer' AND receipt_job_version >= 0)
    ),
    received_at TEXT NOT NULL CHECK (
        typeof(received_at) = 'text' AND length(received_at) > 0
    ),
    applied_at TEXT NOT NULL CHECK (
        typeof(applied_at) = 'text' AND length(applied_at) > 0
    )
)
"""

_V5_RESULT_ARTIFACT_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_job_result_artifact (
    job_id TEXT PRIMARY KEY REFERENCES lab_job(job_id) ON DELETE RESTRICT,
    commit_request_id TEXT NOT NULL UNIQUE
        REFERENCES lab_artifact_commit(request_id) ON DELETE RESTRICT,
    sealed_path TEXT NOT NULL CHECK (
        typeof(sealed_path) = 'text' AND length(sealed_path) > 0
    ),
    manifest_hash TEXT NOT NULL CHECK (
        typeof(manifest_hash) = 'text' AND length(manifest_hash) = 64
        AND manifest_hash NOT GLOB '*[^0-9a-f]*'
    ),
    complete_result_hash TEXT NOT NULL CHECK (
        typeof(complete_result_hash) = 'text' AND length(complete_result_hash) = 64
        AND complete_result_hash NOT GLOB '*[^0-9a-f]*'
    ),
    bundle_device INTEGER NOT NULL CHECK (
        typeof(bundle_device) = 'integer' AND bundle_device >= 0
    ),
    bundle_inode INTEGER NOT NULL CHECK (
        typeof(bundle_inode) = 'integer' AND bundle_inode >= 1
    ),
    evidence_json TEXT NOT NULL CHECK (
        typeof(evidence_json) = 'text' AND length(evidence_json) > 0
        AND json_valid(evidence_json)
    ),
    indexed_at TEXT NOT NULL CHECK (
        typeof(indexed_at) = 'text' AND length(indexed_at) > 0
    )
)
"""

_V5_JOB_RESULT_UPDATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_job_complete_result_update
BEFORE UPDATE OF status, result_state, result_contract_version,
                 requires_complete_result ON lab_job
WHEN (
    NEW.result_state = 'legacy_unsealed'
    AND NOT (
        OLD.requires_complete_result = 0
        AND OLD.status = 'succeeded'
        AND OLD.result_state = 'legacy_unsealed'
        AND NEW.requires_complete_result = 0
        AND NEW.status = 'succeeded'
    )
 )
 OR (
    NEW.requires_complete_result = 1
    AND (
      (NEW.status = 'succeeded' AND (
        NEW.result_state <> 'sealed'
        OR NOT EXISTS (
            SELECT 1 FROM lab_job_result_artifact artifact
            WHERE artifact.job_id = NEW.job_id
        )
      ))
      OR (NEW.result_state = 'sealed' AND NEW.status <> 'succeeded')
      OR (NEW.result_state = 'ready' AND (
        NEW.status <> 'running'
        OR NOT EXISTS (
            SELECT 1 FROM lab_shard shard WHERE shard.job_id = NEW.job_id
        )
        OR EXISTS (
            SELECT 1 FROM lab_shard shard
            WHERE shard.job_id = NEW.job_id AND shard.status <> 'succeeded'
        )
      ))
    )
 )
BEGIN
    SELECT RAISE(ABORT, 'complete result marker requires indexed sealed artifact');
END
"""

_V5_JOB_RESULT_INSERT_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_job_complete_result_insert
BEFORE INSERT ON lab_job
WHEN NEW.requires_complete_result <> 1
 OR NEW.status = 'succeeded'
 OR NEW.result_state IN ('ready', 'sealed', 'legacy_unsealed')
BEGIN
    SELECT RAISE(ABORT, 'new jobs require complete result; legacy_unsealed cannot be inserted');
END
"""

_V5_JOB_RESULT_MARKER_IMMUTABLE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_job_complete_result_marker_immutable
BEFORE UPDATE OF requires_complete_result ON lab_job
WHEN NEW.requires_complete_result <> OLD.requires_complete_result
BEGIN
    SELECT RAISE(ABORT, 'requires_complete_result is immutable');
END
"""

_V5_RESULT_ARTIFACT_INSERT_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_result_artifact_insert
BEFORE INSERT ON lab_job_result_artifact
WHEN NOT EXISTS (
    SELECT 1 FROM lab_artifact_commit artifact_commit
    WHERE artifact_commit.request_id = NEW.commit_request_id
      AND artifact_commit.job_id = NEW.job_id
      AND artifact_commit.status = 'accepted'
)
BEGIN
    SELECT RAISE(ABORT, 'result artifact requires accepted commit receipt');
END
"""

_V5_RESULT_ARTIFACT_NO_UPDATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_result_artifact_no_update
BEFORE UPDATE ON lab_job_result_artifact
BEGIN
    SELECT RAISE(ABORT, 'complete result artifact index is immutable');
END
"""

_V5_RESULT_ARTIFACT_NO_DELETE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_result_artifact_no_delete
BEFORE DELETE ON lab_job_result_artifact
BEGIN
    SELECT RAISE(ABORT, 'complete result artifact index is immutable');
END
"""

_V5_ARTIFACT_COMMIT_NO_UPDATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_artifact_commit_no_update
BEFORE UPDATE ON lab_artifact_commit
BEGIN
    SELECT RAISE(ABORT, 'artifact commit receipt is immutable');
END
"""

_V5_ARTIFACT_COMMIT_NO_DELETE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_artifact_commit_no_delete
BEFORE DELETE ON lab_artifact_commit
BEGIN
    SELECT RAISE(ABORT, 'artifact commit receipt is immutable');
END
"""

_V4_SCHEMA_STATEMENTS = tuple(
    (
        _V4_JOB_TABLE_STATEMENT
        if "CREATE TABLE IF NOT EXISTS lab_job" in statement
        else _V4_SHARD_TABLE_STATEMENT
        if "CREATE TABLE IF NOT EXISTS lab_shard" in statement
        else _V3_ARTIFACT_TABLE_STATEMENT
        if "CREATE TABLE IF NOT EXISTS lab_artifact" in statement
        else statement
    )
    for statement in _V2_SCHEMA_STATEMENTS
) + (
    _V3_REPORT_TABLE_STATEMENT,
    _V3_REPORT_INDEX_STATEMENT,
    _V3_SCHEDULER_STATE_TABLE_STATEMENT,
    _V4_COMPLETION_INDEX_STATEMENT,
    _V4_STATUS_INDEX_STATEMENT,
)

_SCHEMA_STATEMENTS = tuple(
    _V5_JOB_TABLE_STATEMENT if statement == _V4_JOB_TABLE_STATEMENT else statement
    for statement in _V4_SCHEMA_STATEMENTS
) + (
    _V5_ARTIFACT_COMMIT_TABLE_STATEMENT,
    _V5_RESULT_ARTIFACT_TABLE_STATEMENT,
    _V5_JOB_RESULT_INSERT_TRIGGER,
    _V5_JOB_RESULT_UPDATE_TRIGGER,
    _V5_JOB_RESULT_MARKER_IMMUTABLE_TRIGGER,
    _V5_RESULT_ARTIFACT_INSERT_TRIGGER,
    _V5_RESULT_ARTIFACT_NO_UPDATE_TRIGGER,
    _V5_RESULT_ARTIFACT_NO_DELETE_TRIGGER,
    _V5_ARTIFACT_COMMIT_NO_UPDATE_TRIGGER,
    _V5_ARTIFACT_COMMIT_NO_DELETE_TRIGGER,
)
