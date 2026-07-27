"""SQLite single-writer ledger for durable Strategy Lab jobs."""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Literal, Self
from urllib.parse import quote
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5
from weakref import ReferenceType, ref

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.lab_artifact_protocol import (
    LabArtifactCommitEnvelope,
    LabArtifactCommitReceipt,
    LabFinalizerAuthorityClaims,
    LabFinalizerAuthorityShardEvidence,
    LabFinalizerAuthorityVerificationKeyProvider,
    authenticate_artifact_commit_identity,
)
from rquant.lab_artifacts import LabArtifactIndexEvidence
from rquant.lab_eta import LabEtaEstimate, LabEtaInput, LabEtaStatus
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
from rquant.lab_result_digest import (
    LabResultDigestPolicy,
    LabResultDigestProvenanceError,
    resolve_success_digest_provenance,
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
    from rquant.lab_artifacts import LabVerifiedSealedBinding


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
_SUBMIT_AUTH_FUNCTION = "rquant_lab_submit_authorized"
_RETRY_AUTH_FUNCTION = "rquant_lab_retry_authorized"
_READY_TERMINAL_AUTH_FUNCTION = "rquant_lab_ready_terminal_authorized"
_ARTIFACT_COMMIT_AUTH_FUNCTION = "rquant_lab_artifact_commit_authorized"
_ARTIFACT_INDEX_AUTH_FUNCTION = "rquant_lab_artifact_index_authorized"
_ARTIFACT_SUCCESS_AUTH_FUNCTION = "rquant_lab_artifact_success_authorized"
LAB_ETA_COMPLETED_LIMIT_MAX = 256
MAX_JOB_SHARDS = 128
LAB_JOB_LIST_LIMIT_MAX = 100
LAB_JOB_DETAIL_SHARD_LIMIT_MAX = 256
LAB_JOB_DETAIL_EVENT_LIMIT_MAX = 512
LAB_JOB_DETAIL_ARTIFACT_LIMIT_MAX = 128
LAB_JOB_FILTER_TUPLE_INPUT_MAX = 32
_EMPTY_PAYLOAD_JSON = "{}"
_EMPTY_PAYLOAD_HASH = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
_LEGACY_PLAN_HASH = "0" * 64
_ATTEMPTS_EXHAUSTED_FAILURE_JSON = '{"reason":"attempts_exhausted"}'
_PARENT_ATTEMPTS_EXHAUSTED_FAILURE_JSON = '{"reason":"parent_failed_attempts_exhausted"}'
_PARENT_RECOVERABLE_FAILURE_JSON = '{"reason":"parent_failed_recoverable"}'
_DEADLINE_EXCEEDED_FAILURE_JSON = '{"reason":"deadline_exceeded"}'


class _LabWriteAuthorization:
    """Transaction-scoped application integrity capabilities for ledger triggers.

    These fixed-name SQLite UDFs are not a process-identity or cryptographic
    security boundary. A process with physical write access to the database can
    replace the database, its triggers, or the UDF implementations. The security
    boundary is filesystem/process ownership enforcing the scheduler as the sole
    application writer; these grants constrain accidental or unapproved SQL on a
    store-owned connection to the exact transaction currently being applied.
    """

    __slots__ = (
        "_artifact_commit",
        "_artifact_index",
        "_artifact_success",
        "_connection_ref",
        "_epoch",
        "_ready_terminal",
        "_retry",
        "_submit",
    )

    def __init__(self, connection: _LabJobStoreConnection) -> None:
        self._connection_ref: ReferenceType[_LabJobStoreConnection] = ref(connection)
        self._epoch = 0
        self._submit: tuple[int, str, str] | None = None
        self._retry: tuple[int, str, int, int] | None = None
        self._ready_terminal: tuple[int, str, str, int, int, int, int | None] | None = None
        self._artifact_commit: tuple[int, str, str, str] | None = None
        self._artifact_index: tuple[int, str, str, str] | None = None
        self._artifact_success: tuple[int, str, str, str, int, int] | None = None

    def _require_transaction(self) -> int:
        connection = self._connection_ref()
        if connection is None or not connection.in_transaction:
            raise RuntimeError("lab SQL authorization requires an active transaction")
        return self._epoch

    def _is_current_transaction(self, epoch: int) -> bool:
        connection = self._connection_ref()
        return connection is not None and connection.in_transaction and epoch == self._epoch

    def expire_transaction_boundary(self) -> None:
        if any(
            grant is not None
            for grant in (
                self._submit,
                self._retry,
                self._ready_terminal,
                self._artifact_commit,
                self._artifact_index,
                self._artifact_success,
            )
        ):
            self._epoch += 1
        self._submit = None
        self._retry = None
        self._ready_terminal = None
        self._artifact_commit = None
        self._artifact_index = None
        self._artifact_success = None

    @contextmanager
    def authorize_submit(self, job_id: UUID, spec_json: str) -> Iterator[None]:
        if self._submit is not None:
            raise RuntimeError("submit SQL authorization is already active")
        epoch = self._require_transaction()
        self._submit = (epoch, str(job_id), spec_json)
        try:
            yield
        finally:
            if self._submit is not None and self._submit[0] == epoch:
                self._submit = None

    @contextmanager
    def authorize_retry(
        self,
        job_id: UUID,
        old_version: int,
        new_version: int,
    ) -> Iterator[None]:
        if self._retry is not None:
            raise RuntimeError("retry SQL authorization is already active")
        epoch = self._require_transaction()
        self._retry = (epoch, str(job_id), old_version, new_version)
        try:
            yield
        finally:
            if self._retry is not None and self._retry[0] == epoch:
                self._retry = None

    @contextmanager
    def authorize_artifact_commit(
        self,
        request_id: UUID,
        commit_json: str,
        receipt_json: str,
    ) -> Iterator[None]:
        if self._artifact_commit is not None:
            raise RuntimeError("artifact commit SQL authorization is already active")
        epoch = self._require_transaction()
        self._artifact_commit = (epoch, str(request_id), commit_json, receipt_json)
        try:
            yield
        finally:
            if self._artifact_commit is not None and self._artifact_commit[0] == epoch:
                self._artifact_commit = None

    @contextmanager
    def authorize_ready_terminal(
        self,
        job_id: UUID,
        target_status: JobStatus,
        old_version: int,
        new_version: int,
        recoverable: int,
        scheduler_fencing_token: int | None,
    ) -> Iterator[None]:
        if self._ready_terminal is not None:
            raise RuntimeError("ready terminal SQL authorization is already active")
        epoch = self._require_transaction()
        self._ready_terminal = (
            epoch,
            str(job_id),
            target_status.value,
            old_version,
            new_version,
            recoverable,
            scheduler_fencing_token,
        )
        try:
            yield
        finally:
            if self._ready_terminal is not None and self._ready_terminal[0] == epoch:
                self._ready_terminal = None

    @contextmanager
    def authorize_artifact_index(
        self,
        job_id: UUID,
        request_id: UUID,
        evidence_json: str,
    ) -> Iterator[None]:
        if self._artifact_index is not None:
            raise RuntimeError("artifact index SQL authorization is already active")
        epoch = self._require_transaction()
        self._artifact_index = (epoch, str(job_id), str(request_id), evidence_json)
        try:
            yield
        finally:
            if self._artifact_index is not None and self._artifact_index[0] == epoch:
                self._artifact_index = None

    @contextmanager
    def authorize_artifact_success(
        self,
        job_id: UUID,
        request_id: UUID,
        evidence_json: str,
        old_version: int,
        new_version: int,
    ) -> Iterator[None]:
        if self._artifact_success is not None:
            raise RuntimeError("artifact success SQL authorization is already active")
        epoch = self._require_transaction()
        self._artifact_success = (
            epoch,
            str(job_id),
            str(request_id),
            evidence_json,
            old_version,
            new_version,
        )
        try:
            yield
        finally:
            if self._artifact_success is not None and self._artifact_success[0] == epoch:
                self._artifact_success = None

    def submit_authorized(self, job_id: object, spec_json: object) -> int:
        grant = self._submit
        return int(
            grant is not None
            and self._is_current_transaction(grant[0])
            and grant[1:] == (str(job_id), str(spec_json))
        )

    def retry_authorized(
        self,
        job_id: object,
        old_version: object,
        new_version: object,
    ) -> int:
        grant = self._retry
        return int(
            grant is not None
            and self._is_current_transaction(grant[0])
            and grant[1:] == (str(job_id), old_version, new_version)
        )

    def artifact_commit_authorized(
        self,
        request_id: object,
        commit_json: object,
        receipt_json: object,
    ) -> int:
        grant = self._artifact_commit
        return int(
            grant is not None
            and self._is_current_transaction(grant[0])
            and grant[1:] == (str(request_id), str(commit_json), str(receipt_json))
        )

    def ready_terminal_authorized(
        self,
        job_id: object,
        target_status: object,
        old_version: object,
        new_version: object,
        recoverable: object,
        scheduler_fencing_token: object,
    ) -> int:
        grant = self._ready_terminal
        return int(
            grant is not None
            and self._is_current_transaction(grant[0])
            and grant[1:]
            == (
                str(job_id),
                str(target_status),
                old_version,
                new_version,
                recoverable,
                scheduler_fencing_token,
            )
        )

    def artifact_index_authorized(
        self,
        job_id: object,
        request_id: object,
        evidence_json: object,
    ) -> int:
        grant = self._artifact_index
        return int(
            grant is not None
            and self._is_current_transaction(grant[0])
            and grant[1:] == (str(job_id), str(request_id), str(evidence_json))
        )

    def artifact_success_authorized(
        self,
        job_id: object,
        request_id: object,
        evidence_json: object,
        old_version: object,
        new_version: object,
    ) -> int:
        grant = self._artifact_success
        return int(
            grant is not None
            and self._is_current_transaction(grant[0])
            and grant[1:]
            == (
                str(job_id),
                str(request_id),
                str(evidence_json),
                old_version,
                new_version,
            )
        )


_SqlParameters = Iterable[object] | Mapping[str, object]


class _LabJobStoreCursor(sqlite3.Cursor):
    def _expire_authorization(self) -> None:
        connection = self.connection
        if isinstance(connection, _LabJobStoreConnection):
            connection._expire_write_authorization()

    def execute(
        self,
        sql: str,
        parameters: _SqlParameters = (),
        /,
    ) -> sqlite3.Cursor:
        try:
            return super().execute(sql, parameters)
        except sqlite3.Error:
            self._expire_authorization()
            raise

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Iterable[_SqlParameters],
        /,
    ) -> sqlite3.Cursor:
        try:
            return super().executemany(sql, seq_of_parameters)
        except sqlite3.Error:
            self._expire_authorization()
            raise

    def executescript(self, sql_script: str, /) -> sqlite3.Cursor:
        try:
            return super().executescript(sql_script)
        except sqlite3.Error:
            self._expire_authorization()
            raise


class _LabJobStoreConnection(sqlite3.Connection):
    write_authorization: _LabWriteAuthorization

    def cursor(
        self,
        factory: type[sqlite3.Cursor] | None = None,
    ) -> sqlite3.Cursor:
        if factory is not None and factory is not _LabJobStoreCursor:
            self._expire_write_authorization()
            raise TypeError("lab job store cursor factory must preserve authorization cleanup")
        return super().cursor(_LabJobStoreCursor)

    def execute(
        self,
        sql: str,
        parameters: _SqlParameters = (),
        /,
    ) -> sqlite3.Cursor:
        return self.cursor().execute(sql, parameters)

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Iterable[_SqlParameters],
        /,
    ) -> sqlite3.Cursor:
        return self.cursor().executemany(sql, seq_of_parameters)

    def executescript(self, sql_script: str, /) -> sqlite3.Cursor:
        return self.cursor().executescript(sql_script)

    def _expire_write_authorization(self) -> None:
        authorization = getattr(self, "write_authorization", None)
        if authorization is not None:
            authorization.expire_transaction_boundary()

    def _trace_transaction_boundary(self, statement: str) -> None:
        tokens = statement.lstrip().split(maxsplit=1)
        keyword = tokens[0].upper() if tokens else ""
        if (keyword in {"BEGIN", "SAVEPOINT"} and not self.in_transaction) or keyword in {
            "COMMIT",
            "END",
            "RELEASE",
            "ROLLBACK",
        }:
            self._expire_write_authorization()

    def commit(self) -> None:
        self._expire_write_authorization()
        super().commit()

    def rollback(self) -> None:
        self._expire_write_authorization()
        super().rollback()

    def close(self) -> None:
        self._expire_write_authorization()
        self.set_trace_callback(None)
        super().close()


def _write_authorization(connection: sqlite3.Connection) -> _LabWriteAuthorization:
    if not isinstance(connection, _LabJobStoreConnection):
        raise RuntimeError("lab write authorization requires a store-owned connection")
    return connection.write_authorization


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


_JOB_STATUS_TO_ETA_STATUS: dict[JobStatus, LabEtaStatus] = {
    JobStatus.QUEUED: "queued",
    JobStatus.RUNNING: "running",
    JobStatus.CHECKPOINTED: "checkpointed",
    JobStatus.SUCCEEDED: "succeeded",
    JobStatus.FAILED: "failed",
    JobStatus.CANCELLED: "cancelled",
}


def _effective_lab_eta_status(
    *,
    status: JobStatus,
    control_intent: ControlIntent,
) -> LabEtaStatus:
    if control_intent is ControlIntent.PAUSE_REQUESTED:
        return "paused"
    return _JOB_STATUS_TO_ETA_STATUS[status]


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


class LabFinalizationShardEvidence(LabRecordModel):
    shard: LabShardRecord
    accepted_success: LabWorkerReportRecord

    @model_validator(mode="after")
    def validate_accepted_success(self) -> LabFinalizationShardEvidence:
        report = self.accepted_success.report
        receipt = self.accepted_success.receipt
        body = report.body
        if not isinstance(body, LabShardSucceeded):
            raise ValueError("finalization evidence requires a shard_succeeded report")
        if (
            self.shard.status is not ShardStatus.SUCCEEDED
            or self.accepted_success.receipt.status != "accepted"
            or self.accepted_success.receipt.reason != "shard_succeeded"
        ):
            raise ValueError("finalization evidence requires an accepted succeeded shard")
        if (
            report.job_id,
            report.shard_id,
            report.payload_hash,
            report.claim_generation,
            body.result_manifest_hash,
        ) != (
            self.shard.job_id,
            self.shard.shard_id,
            self.shard.payload_hash,
            self.shard.claim_generation,
            self.shard.result_manifest_hash,
        ):
            raise ValueError("accepted success report conflicts with shard identity")
        if (
            receipt.job_id,
            receipt.shard_id,
            receipt.worker_id,
            receipt.claim_token,
            receipt.claim_generation,
            receipt.scheduler_fencing_token,
            receipt.report_type,
            receipt.result_manifest_hash,
        ) != (
            report.job_id,
            report.shard_id,
            report.worker_id,
            report.claim_token,
            report.claim_generation,
            report.scheduler_fencing_token,
            "shard_succeeded",
            body.result_manifest_hash,
        ):
            raise ValueError("accepted success receipt conflicts with attempt identity")
        if self.shard.attempt_count != report.claim_generation:
            raise ValueError("accepted success attempt conflicts with shard generation")
        return self


class LabFinalizationReadyEpoch(LabRecordModel):
    """Stable identity of the ledger's currently observable ready result event."""

    job_version: int = Field(ge=0)
    event: LabEventRecord

    @model_validator(mode="after")
    def validate_ready_event(self) -> LabFinalizationReadyEpoch:
        if (
            self.event.event_type != "job_result_ready"
            or self.event.prior_status is not JobStatus.RUNNING
            or self.event.new_status is not JobStatus.RUNNING
            or self.event.request_id is not None
            or self.event.job_version != self.job_version
        ):
            raise ValueError("ready epoch requires its exact job_result_ready event")
        return self


class LabFinalizationSnapshot(LabRecordModel):
    job: LabJobRecord
    ready_epoch: LabFinalizationReadyEpoch
    shards: tuple[LabFinalizationShardEvidence, ...]

    @model_validator(mode="after")
    def validate_complete_graph(self) -> LabFinalizationSnapshot:
        if (
            self.job.status is not JobStatus.RUNNING
            or self.job.result_state is not LabResultState.READY
            or self.job.result_contract_version != COMPLETE_RESULT_CONTRACT_VERSION
            or not self.job.requires_complete_result
            or self.job.control_intent is not ControlIntent.NONE
        ):
            raise ValueError("finalization snapshot requires a ready complete-result job")
        if (
            self.ready_epoch.job_version != self.job.version
            or self.ready_epoch.event.job_id != self.job.job_id
            or self.ready_epoch.event.scheduler_fencing_token != self.job.scheduler_fencing_token
        ):
            raise ValueError("finalization ready epoch conflicts with the ready job")
        if not self.shards:
            raise ValueError("finalization snapshot requires at least one shard")
        indexes = tuple(item.shard.shard_index for item in self.shards)
        if indexes != tuple(range(len(self.shards))):
            raise ValueError("finalization shards must be complete and ordered by shard_index")
        if len({item.shard.shard_id for item in self.shards}) != len(self.shards):
            raise ValueError("finalization shard identities must be unique")
        for item in self.shards:
            if (
                item.shard.job_id != self.job.job_id
                or item.accepted_success.report.job_id != self.job.job_id
                or item.accepted_success.report.spec_hash != self.job.spec_hash
            ):
                raise ValueError("finalization shard graph conflicts with job identity")
        aggregate_identity = {
            (
                item.shard.plan_hash,
                item.shard.adapter_id,
                item.shard.adapter_version,
            )
            for item in self.shards
        }
        if len(aggregate_identity) != 1:
            raise ValueError("finalization shards do not share one aggregate identity")
        return self


class LabArtifactCommitRecord(LabRecordModel):
    envelope: LabArtifactCommitEnvelope
    receipt: LabArtifactCommitReceipt
    received_at: datetime
    applied_at: datetime


class LabJobListFilters(LabRecordModel):
    statuses: tuple[JobStatus, ...] = Field(default=(), max_length=LAB_JOB_FILTER_TUPLE_INPUT_MAX)
    job_types: tuple[ResearchJobType, ...] = Field(
        default=(), max_length=LAB_JOB_FILTER_TUPLE_INPUT_MAX
    )
    resource_classes: tuple[ResourceClass, ...] = Field(
        default=(), max_length=LAB_JOB_FILTER_TUPLE_INPUT_MAX
    )
    created_from: datetime | None = None
    created_before: datetime | None = None
    keyword: str | None = Field(default=None, min_length=1, max_length=200)

    @field_validator("created_from", "created_before", mode="before")
    @classmethod
    def validate_filter_time(cls, value: object) -> object:
        return None if value is None else _utc(value)  # type: ignore[arg-type]

    @field_validator("statuses", "job_types", "resource_classes")
    @classmethod
    def canonicalize_enum_filter(cls, value: tuple[StrEnum, ...]) -> tuple[StrEnum, ...]:
        return tuple(sorted(set(value), key=lambda item: item.value))

    @model_validator(mode="after")
    def validate_filter_range(self) -> LabJobListFilters:
        if (
            self.created_from is not None
            and self.created_before is not None
            and self.created_from >= self.created_before
        ):
            raise ValueError("created_from must precede created_before")
        return self


LAB_JOB_LIST_FILTER_SQL_PARAMETER_MAX = (
    len(JobStatus) + len(ResearchJobType) + len(ResourceClass) + 5
)
LAB_JOB_LIST_QUERY_PARAMETER_MAX = LAB_JOB_LIST_FILTER_SQL_PARAMETER_MAX + 4


class CommandAvailability(LabRecordModel):
    pause: bool
    resume: bool
    cancel: bool
    retry: bool


class LabJobProgress(LabRecordModel):
    total_shards: int = Field(ge=0)
    terminal_shards: int = Field(ge=0)
    succeeded_shards: int = Field(ge=0)
    failed_shards: int = Field(ge=0)
    cancelled_shards: int = Field(ge=0)
    fraction: float = Field(ge=0, le=1, allow_inf_nan=False)
    phase: str | None = None


class LabHeartbeatStatus(LabRecordModel):
    active_shards: int = Field(ge=0)
    latest_heartbeat_at: datetime | None
    stale_after_seconds: float = Field(gt=0, allow_inf_nan=False)
    stale: bool


class LabFirstFailure(LabRecordModel):
    shard_id: UUID
    shard_index: int = Field(ge=0)
    failure: LabShardFailed
    finished_at: datetime


class LabJobSummary(LabRecordModel):
    job_id: UUID
    strategy_name: str
    spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    job_type: ResearchJobType
    resource_class: ResourceClass
    status: JobStatus
    control_intent: ControlIntent
    result_state: LabResultState
    version: int = Field(ge=0)
    deadline: datetime
    created_at: datetime
    updated_at: datetime
    progress: LabJobProgress
    command_availability: CommandAvailability


class LabJobPage(LabRecordModel):
    items: tuple[LabJobSummary, ...]
    total_count: int = Field(ge=0)
    has_more: bool
    next_cursor: str | None


class LabFinalizationCandidate(LabRecordModel):
    job_id: UUID
    job_version: int = Field(ge=0)
    spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    updated_at: datetime


class LabFinalizationCandidatePage(LabRecordModel):
    items: tuple[LabFinalizationCandidate, ...]
    total_count: int = Field(ge=0)
    has_more: bool
    next_cursor: str | None


class LabJobDetail(LabRecordModel):
    job: LabJobRecord
    progress: LabJobProgress
    heartbeat: LabHeartbeatStatus
    command_availability: CommandAvailability
    eta: LabEtaEstimate | None
    first_failure: LabFirstFailure | None
    shards: tuple[LabShardRecord, ...]
    shard_count: int = Field(ge=0)
    shards_truncated: bool
    events: tuple[LabEventRecord, ...]
    event_count: int = Field(ge=0)
    events_truncated: bool
    artifacts: tuple[LabArtifactRecord, ...]
    artifact_count: int = Field(ge=0)
    artifacts_truncated: bool
    result_evidence: LabArtifactIndexEvidence | None


class LabArtifactPreviewAuthority(LabRecordModel):
    job: LabJobRecord
    evidence: LabArtifactIndexEvidence

    @model_validator(mode="after")
    def validate_preview_authority(self) -> LabArtifactPreviewAuthority:
        if (
            self.job.status is not JobStatus.SUCCEEDED
            or self.job.result_state is not LabResultState.SEALED
            or self.evidence.job_id != self.job.job_id
        ):
            raise ValueError("artifact preview authority requires one succeeded sealed job")
        return self


class LabJobCommandContext(LabRecordModel):
    job: LabJobRecord
    availability: CommandAvailability


def command_availability_for_job(
    job: LabJobRecord,
    *,
    has_exhausted_non_succeeded_shard: bool = False,
) -> CommandAvailability:
    pause = (
        job.status is JobStatus.RUNNING
        and job.result_state is not LabResultState.READY
        and job.control_intent is ControlIntent.NONE
    )
    resume = job.status is JobStatus.CHECKPOINTED or (
        job.status is JobStatus.RUNNING and job.control_intent is ControlIntent.PAUSE_REQUESTED
    )
    cancel = job.status in {
        JobStatus.QUEUED,
        JobStatus.RUNNING,
        JobStatus.CHECKPOINTED,
    }
    retry = (
        job.status is JobStatus.FAILED
        and job.recoverable
        and job.attempt_count < job.max_attempts
        and not has_exhausted_non_succeeded_shard
    )
    return CommandAvailability(
        pause=pause,
        resume=resume,
        cancel=cancel,
        retry=retry,
    )


class _LabStagedArtifactCommit:
    """Internal staged transaction that rolls back unless explicitly committed."""

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

    def __enter__(self) -> Self:
        if self._closed:
            raise RuntimeError("artifact commit stage is already closed")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        del exc_type, traceback
        if self._closed:
            return False
        try:
            self.rollback()
        except BaseException as cleanup_error:
            if exc is not None:
                self._raise_lifecycle_errors(
                    "staged artifact transaction body and rollback failed",
                    [exc, cleanup_error],
                )
            raise
        return False

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

    def close(self) -> None:
        self.rollback()


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


def _canonical_uuid_text(value: object, *, field: str) -> UUID:
    if type(value) is not str:
        raise InvalidStoredJobError(
            f"{field} must be canonical UUID text, found {type(value).__name__}"
        )
    try:
        parsed = UUID(value)
    except (AttributeError, ValueError) as exc:
        raise InvalidStoredJobError(f"{field} is not UUID text") from exc
    if value != str(parsed):
        raise InvalidStoredJobError(f"{field} is not canonical lowercase UUID text")
    return parsed


_SHARD_ROW_VALID_FUNCTION = "rquant_lab_shard_row_valid"
_SHARD_HASH_RE = re.compile(r"[0-9a-f]{64}")
_SHARD_PLAN_NAME_RE = re.compile(r"[a-z][a-z0-9_]*")


def _canonical_shard_payload(value: str) -> str:
    def reject_float(_value: str) -> float:
        raise ValueError("floating-point shard payload values are not allowed")

    def reject_constant(_value: str) -> object:
        raise ValueError("non-finite shard payload values are not allowed")

    parsed = json.loads(
        value,
        parse_float=reject_float,
        parse_constant=reject_constant,
    )
    if not isinstance(parsed, dict):
        raise ValueError("shard payload must encode a JSON object")
    return json.dumps(
        parsed,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sqlite_shard_row_valid(
    shard_id_value: object,
    job_id_value: object,
    shard_index_value: object,
    status_value: object,
    version_value: object,
    attempt_count_value: object,
    max_attempts_value: object,
    plan_hash_value: object,
    adapter_id_value: object,
    adapter_version_value: object,
    payload_json_value: object,
    payload_hash_value: object,
    worker_id_value: object,
    scheduler_fencing_token_value: object,
    claim_token_value: object,
    claim_generation_value: object,
    claimed_at_value: object,
    heartbeat_at_value: object,
    lease_expires_at_value: object,
    result_manifest_hash_value: object,
    failure_json_value: object,
    finished_at_value: object,
    checkpoint_json_value: object,
    created_at_value: object,
    updated_at_value: object,
    phase_value: object,
    work_unit_name_value: object,
    work_units_value: object,
    static_duration_ms_value: object,
    duration_ms_value: object,
    throughput_value: object,
    completion_sequence_value: object,
) -> int:
    try:
        shard_id = _canonical_uuid_text(shard_id_value, field="lab_shard.shard_id")
        if not shard_id.int:
            raise ValueError("persisted shard_id cannot use the constructor sentinel")
        _canonical_uuid_text(job_id_value, field="lab_shard.job_id")
        shard_index = _strict_sqlite_int(
            shard_index_value,
            field="lab_shard.shard_index",
            minimum=0,
        )
        status = ShardStatus(str(status_value))
        _strict_sqlite_int(version_value, field="lab_shard.version", minimum=0)
        attempt_count = _strict_sqlite_int(
            attempt_count_value,
            field="lab_shard.attempt_count",
            minimum=0,
        )
        max_attempts = _strict_sqlite_int(
            max_attempts_value,
            field="lab_shard.max_attempts",
            minimum=1,
        )
        plan_hash = str(plan_hash_value)
        adapter_id = str(adapter_id_value)
        adapter_version = str(adapter_version_value)
        payload_json = str(payload_json_value)
        payload_hash = str(payload_hash_value)
        if _SHARD_HASH_RE.fullmatch(plan_hash) is None:
            raise ValueError("invalid shard plan hash")
        if _SHARD_HASH_RE.fullmatch(payload_hash) is None:
            raise ValueError("invalid shard payload hash")

        worker_id = str(worker_id_value) if worker_id_value else None
        scheduler_fencing_token = _strict_nullable_sqlite_int(
            scheduler_fencing_token_value,
            field="lab_shard.scheduler_fencing_token",
            minimum=1,
        )
        claim_token = (
            _canonical_uuid_text(claim_token_value, field="lab_shard.claim_token")
            if claim_token_value is not None
            else None
        )
        _strict_sqlite_int(
            claim_generation_value,
            field="lab_shard.claim_generation",
            minimum=0,
        )

        def optional_time(value: object) -> datetime | None:
            return _load_time(str(value)) if value is not None else None

        claimed_at = optional_time(claimed_at_value)
        heartbeat_at = optional_time(heartbeat_at_value)
        lease_expires_at = optional_time(lease_expires_at_value)
        finished_at = optional_time(finished_at_value)
        _load_time(str(created_at_value))
        _load_time(str(updated_at_value))
        result_manifest_hash = (
            str(result_manifest_hash_value) if result_manifest_hash_value is not None else None
        )
        if (
            result_manifest_hash is not None
            and _SHARD_HASH_RE.fullmatch(result_manifest_hash) is None
        ):
            raise ValueError("invalid shard result manifest hash")
        failure_json = str(failure_json_value) if failure_json_value is not None else None
        str(checkpoint_json_value) if checkpoint_json_value is not None else None

        phase = str(phase_value) if phase_value is not None else None
        work_unit_name = str(work_unit_name_value) if work_unit_name_value is not None else None
        if phase is not None and _SHARD_PLAN_NAME_RE.fullmatch(phase) is None:
            raise ValueError("invalid shard phase")
        if work_unit_name is not None and _SHARD_PLAN_NAME_RE.fullmatch(work_unit_name) is None:
            raise ValueError("invalid shard work unit name")
        work_units = _strict_nullable_sqlite_int(
            work_units_value,
            field="lab_shard.work_units",
            minimum=1,
            maximum=SQLITE_SIGNED_INTEGER_MAX,
        )
        static_duration_ms = _strict_nullable_sqlite_int(
            static_duration_ms_value,
            field="lab_shard.static_duration_ms",
            minimum=1,
            maximum=SQLITE_SIGNED_INTEGER_MAX,
        )
        duration_ms = _strict_nullable_sqlite_real(
            duration_ms_value,
            field="lab_shard.duration_ms",
            positive=True,
            minimum_inclusive=LAB_SHARD_DURATION_MS_MIN,
            maximum_exclusive=LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE,
        )
        throughput = _strict_nullable_sqlite_real(
            throughput_value,
            field="lab_shard.throughput_units_per_second",
            positive=True,
            maximum_exclusive=LAB_SHARD_THROUGHPUT_MAX_EXCLUSIVE,
        )
        completion_sequence = _strict_nullable_sqlite_int(
            completion_sequence_value,
            field="lab_shard.completion_sequence",
            minimum=1,
        )

        plan_values = (phase, work_unit_name, work_units, static_duration_ms)
        if not (
            all(value is None for value in plan_values)
            or all(value is not None for value in plan_values)
        ):
            raise ValueError("shard work plan must be entirely present or absent")
        has_work_plan = phase is not None

        is_legacy = adapter_id == "legacy-v2"
        if is_legacy:
            if (
                adapter_version != "v0"
                or plan_hash != _LEGACY_PLAN_HASH
                or payload_json != _EMPTY_PAYLOAD_JSON
                or payload_hash != _EMPTY_PAYLOAD_HASH
            ):
                raise ValueError("legacy shard identity mismatch")
        else:
            canonical_payload = _canonical_shard_payload(payload_json.strip())
            canonical_payload_hash = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
            if payload_hash != canonical_payload_hash:
                raise ValueError("shard payload hash mismatch")
            canonical_adapter_id = adapter_id.strip()
            canonical_adapter_version = adapter_version.strip()
            if not canonical_adapter_id or not canonical_adapter_version:
                raise ValueError("shard adapter identity is empty")
            shard_identity: dict[str, object] = {
                "adapter_id": canonical_adapter_id,
                "adapter_version": canonical_adapter_version,
                "payload_hash": canonical_payload_hash,
                "plan_hash": plan_hash,
                "shard_index": shard_index,
            }
            if has_work_plan:
                shard_identity["work_plan"] = {
                    "phase": phase,
                    "static_duration_ms": static_duration_ms,
                    "work_unit_name": work_unit_name,
                    "work_units": work_units,
                }
            shard_name = json.dumps(
                shard_identity,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            expected_shard_id = uuid5(
                NAMESPACE_URL,
                f"rquant:lab-shard:{shard_name}",
            )
            if shard_id != expected_shard_id:
                raise ValueError("shard id does not match deterministic definition")

        telemetry_values = (duration_ms, throughput, completion_sequence)
        if not (
            all(value is None for value in telemetry_values)
            or all(value is not None for value in telemetry_values)
        ):
            raise ValueError("shard completion telemetry must be entirely present or absent")
        if duration_ms is not None:
            if not has_work_plan or work_units is None or throughput is None:
                raise ValueError("shard telemetry is missing its work plan")
            observed_work_units = throughput * (duration_ms * 0.001)
            if not math.isclose(
                observed_work_units,
                float(work_units),
                rel_tol=1e-12,
                abs_tol=1e-9,
            ):
                raise ValueError("shard throughput does not match duration and work units")
            if status is not ShardStatus.SUCCEEDED:
                raise ValueError("non-succeeded shard retains completion telemetry")
        if status is ShardStatus.SUCCEEDED and has_work_plan and duration_ms is None:
            raise ValueError("telemetry-planned succeeded shard is missing telemetry")

        claim_values = (
            worker_id,
            scheduler_fencing_token,
            claim_token,
            claimed_at,
            heartbeat_at,
            lease_expires_at,
        )
        if status is ShardStatus.RUNNING and any(value is None for value in claim_values):
            raise ValueError("running shard is missing claim identity")
        if status is ShardStatus.QUEUED and attempt_count >= max_attempts:
            raise ValueError("queued shard exhausted attempts")
        if claimed_at is not None and heartbeat_at is not None and heartbeat_at < claimed_at:
            raise ValueError("shard heartbeat predates claim")
        if (
            claimed_at is not None
            and lease_expires_at is not None
            and lease_expires_at <= claimed_at
        ):
            raise ValueError("shard claim lease is not positive")
        if status is ShardStatus.SUCCEEDED and (
            finished_at is None or (not is_legacy and result_manifest_hash is None)
        ):
            raise ValueError("succeeded shard is missing result identity")
        if status is ShardStatus.FAILED and (
            finished_at is None or (not is_legacy and failure_json is None)
        ):
            raise ValueError("failed shard is missing failure identity")
        if status is ShardStatus.CANCELLED and finished_at is None:
            raise ValueError("cancelled shard is missing finished_at")
        if status in {
            ShardStatus.SUCCEEDED,
            ShardStatus.FAILED,
            ShardStatus.CANCELLED,
        } and any(value is not None for value in claim_values):
            raise ValueError("terminal shard retains claim identity")
        return 1
    except Exception:
        return 0


def _command_record_from_row(
    row: sqlite3.Row,
    *,
    expected_request_id: UUID | None = None,
) -> LabCommandRecord:
    stored_request = str(row["request_id"])
    try:
        request_id = _canonical_uuid_text(
            row["request_id"],
            field="lab_command.request_id",
        )
        envelope = LabCommandEnvelope.model_validate_json(str(row["command_json"]))
        receipt = LabCommandReceipt.model_validate_json(str(row["receipt_json"]))
        content_hash = str(row["content_hash"])
        command_type = str(row["command_type"])
        job_id = _canonical_uuid_text(row["job_id"], field="lab_command.job_id")
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
        if envelope.model_dump_json() != str(row["command_json"]):
            raise ValueError("command JSON is not canonical")
        if receipt.model_dump_json() != str(row["receipt_json"]):
            raise ValueError("command receipt JSON is not canonical")
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
        report_id = _canonical_uuid_text(
            row["report_id"],
            field="lab_worker_report.report_id",
        )
        content_hash = str(row["content_hash"])
        job_id = _canonical_uuid_text(row["job_id"], field="lab_worker_report.job_id")
        shard_id = _canonical_uuid_text(
            row["shard_id"],
            field="lab_worker_report.shard_id",
        )
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
        if report.canonical_json() != str(row["report_json"]):
            raise ValueError("worker report JSON is not canonical")
        if receipt.model_dump_json() != str(row["receipt_json"]):
            raise ValueError("worker report receipt JSON is not canonical")
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
        request_id = _canonical_uuid_text(
            row["request_id"],
            field="lab_artifact_commit.request_id",
        )
        envelope = LabArtifactCommitEnvelope.model_validate_json(str(row["commit_json"]))
        receipt = LabArtifactCommitReceipt.model_validate_json(str(row["receipt_json"]))
        if expected_request_id is not None and request_id != expected_request_id:
            raise ValueError("artifact commit request id does not match lookup key")
        if not (envelope.request_id == receipt.request_id == request_id):
            raise ValueError("artifact commit request id mismatch")
        if not (envelope.content_hash == receipt.content_hash == str(row["content_hash"])):
            raise ValueError("artifact commit content hash mismatch")
        job_id = _canonical_uuid_text(row["job_id"], field="lab_artifact_commit.job_id")
        if not (envelope.commit.job_id == receipt.job_id == job_id):
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


def _result_artifact_evidence_from_row(
    row: sqlite3.Row,
    *,
    expected_job_id: UUID | None = None,
) -> LabArtifactIndexEvidence:
    from rquant.lab_artifacts import LabArtifactIndexEvidence

    try:
        evidence = LabArtifactIndexEvidence.model_validate_json(str(row["evidence_json"]))
        if expected_job_id is not None and evidence.job_id != expected_job_id:
            raise ValueError("artifact evidence job id mismatch")
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
            raise ValueError("artifact evidence conflicts with indexed columns")
        return evidence
    except Exception as exc:
        if isinstance(exc, InvalidStoredJobError):
            raise
        raise InvalidStoredJobError(
            f"invalid stored result artifact {row['job_id']}: {exc}"
        ) from exc


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


_SQL_TWO_CHARACTER_OPERATORS = frozenset({"||", "<<", ">>", "<=", ">=", "==", "!=", "<>", "->"})


def _normalized_sql_tokens(sql: str) -> tuple[tuple[str, str], ...]:
    tokens: list[tuple[str, str]] = []
    position = 0
    while position < len(sql):
        character = sql[position]
        if character.isspace():
            position += 1
            continue
        if sql.startswith("--", position):
            line_endings = tuple(
                ending
                for ending in (
                    sql.find("\n", position + 2),
                    sql.find("\r", position + 2),
                )
                if ending >= 0
            )
            position = len(sql) if not line_endings else min(line_endings) + 1
            continue
        if sql.startswith("/*", position):
            comment_end = sql.find("*/", position + 2)
            if comment_end < 0:
                raise ValueError("unterminated SQL block comment")
            position = comment_end + 2
            continue
        if character in {"'", '"', "`"}:
            quote = character
            end = position + 1
            while end < len(sql):
                if sql[end] != quote:
                    end += 1
                    continue
                if end + 1 < len(sql) and sql[end + 1] == quote:
                    end += 2
                    continue
                end += 1
                break
            else:
                raise ValueError("unterminated quoted SQL token")
            tokens.append(("quoted", sql[position:end]))
            position = end
            continue
        if character == "[":
            end = sql.find("]", position + 1)
            if end < 0:
                raise ValueError("unterminated bracketed SQL identifier")
            tokens.append(("quoted", sql[position : end + 1]))
            position = end + 1
            continue
        if character.isalnum() or character in {"_", "$"}:
            end = position + 1
            while end < len(sql) and (sql[end].isalnum() or sql[end] in {"_", "$"}):
                end += 1
            tokens.append(("word", sql[position:end].casefold()))
            position = end
            continue
        operator = sql[position : position + 2]
        if operator in _SQL_TWO_CHARACTER_OPERATORS:
            tokens.append(("operator", operator))
            position += 2
            continue
        tokens.append(("operator", character))
        position += 1

    if tokens and tokens[-1] == ("operator", ";"):
        tokens.pop()
    without_optional_exists: list[tuple[str, str]] = []
    position = 0
    while position < len(tokens):
        if tokens[position : position + 3] == [
            ("word", "if"),
            ("word", "not"),
            ("word", "exists"),
        ]:
            position += 3
            continue
        without_optional_exists.append(tokens[position])
        position += 1
    return tuple(without_optional_exists)


def _sql_ddl_equivalent(expected: str, actual: str) -> bool:
    try:
        return _normalized_sql_tokens(expected) == _normalized_sql_tokens(actual)
    except ValueError:
        return False


def _canonical_index_predicate(sql: str) -> tuple[tuple[str, str], ...] | None:
    try:
        tokens = _normalized_sql_tokens(sql)
    except ValueError:
        return None
    where_positions = tuple(
        position for position, token in enumerate(tokens) if token == ("word", "where")
    )
    if len(where_positions) != 1:
        return None
    return tokens[where_positions[0] + 1 :]


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
    expected_predicate = None if predicate is None else _normalized_sql_tokens(predicate)
    if _canonical_index_predicate(str(sql_row[0])) != expected_predicate:
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
        predicate="status = 'succeeded' AND completion_sequence IS NOT NULL",
    )
    _validate_v4_index(
        connection,
        name="ix_lab_shard_job_status_index",
        unique=False,
        partial=False,
        key_columns=(("job_id", False), ("status", False), ("shard_index", False)),
        predicate=None,
    )


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
    if row is None or row[0] is None or not _sql_ddl_equivalent(expected, str(row[0])):
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
    # Ledger identity covers persistent main-schema triggers; TEMP triggers are
    # connection-local instrumentation and do not alter the database file.
    existing_triggers = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
    }
    expected_triggers = frozenset(_V5_EXPECTED_TRIGGER_SQL)
    missing_triggers = sorted(expected_triggers - existing_triggers)
    unexpected_triggers = sorted(existing_triggers - expected_triggers)
    if missing_triggers or unexpected_triggers:
        details: list[str] = []
        if missing_triggers:
            details.append(f"missing triggers: {', '.join(missing_triggers)}")
        if unexpected_triggers:
            details.append(f"unexpected triggers: {', '.join(unexpected_triggers)}")
        raise LabDatabaseIdentityError(
            f"lab jobs SQLite v5 trigger set is invalid: {'; '.join(details)}"
        )
    for name, expected_sql in _V5_EXPECTED_TRIGGER_SQL.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            (name,),
        ).fetchone()
        assert row is not None
        if row[0] is None or not _sql_ddl_equivalent(expected_sql, str(row[0])):
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
    """Read and validate committed ledger state without filesystem artifact I/O.

    Sealed bundle liveness is reverified only by the explicit artifact-store
    binding APIs. Reader validation proves persisted graph consistency, not the
    current identity or availability of paths recorded by that graph.
    """

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
        connection.create_function(
            _SHARD_ROW_VALID_FUNCTION,
            32,
            _sqlite_shard_row_valid,
            deterministic=True,
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
    def _after_finalization_job_read(_job_id: UUID) -> None:
        """Fault-injection boundary after the snapshot's first authoritative read."""

    @staticmethod
    def _job_from_row(row: sqlite3.Row) -> LabJobRecord:
        try:
            spec = ResearchRunSpec.model_validate_json(str(row["spec_json"]))
            stored_hash = str(row["spec_hash"])
            stored_job_type = ResearchJobType(str(row["job_type"]))
            stored_resource = ResourceClass(str(row["resource_class"]))
            stored_deadline = _load_time(str(row["deadline"]))
            if spec.model_dump_json(round_trip=True) != str(row["spec_json"]):
                raise ValueError("spec JSON is not canonical")
            if spec.spec_hash != stored_hash:
                raise ValueError("spec hash mismatch")
            if spec.job_type is not stored_job_type:
                raise ValueError("job_type does not match spec")
            if spec.resource_class is not stored_resource:
                raise ValueError("resource_class does not match spec")
            if spec.deadline != stored_deadline:
                raise ValueError("deadline does not match spec")
            record = LabJobRecord(
                job_id=_canonical_uuid_text(row["job_id"], field="lab_job.job_id"),
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
                token=_canonical_uuid_text(row["token"], field="lab_lease.token"),
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
                job_id=_canonical_uuid_text(row["job_id"], field="lab_event.job_id"),
                request_id=(
                    _canonical_uuid_text(row["request_id"], field="lab_event.request_id")
                    if row["request_id"] is not None
                    else None
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
                shard_id=_canonical_uuid_text(row["shard_id"], field="lab_shard.shard_id"),
                job_id=_canonical_uuid_text(row["job_id"], field="lab_shard.job_id"),
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
                    _canonical_uuid_text(row["claim_token"], field="lab_shard.claim_token")
                    if row["claim_token"] is not None
                    else None
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
            if not record.shard_id.int:
                raise ValueError("persisted shard_id cannot use the constructor sentinel")
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

    @classmethod
    def _validate_complete_result_graph(
        cls,
        connection: sqlite3.Connection,
        job: LabJobRecord,
    ) -> LabArtifactIndexEvidence | None:
        index_row = connection.execute(
            "SELECT * FROM lab_job_result_artifact WHERE job_id = ?",
            (str(job.job_id),),
        ).fetchone()

        shard_aggregate = connection.execute(
            """
            SELECT COUNT(*) AS shard_count,
                   COALESCE(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END), 0)
                       AS succeeded_count,
                   COUNT(DISTINCT plan_hash) AS plan_hash_count,
                   MIN(plan_hash) AS plan_hash,
                   COUNT(DISTINCT adapter_id) AS adapter_id_count,
                   MIN(adapter_id) AS adapter_id,
                   COUNT(DISTINCT adapter_version) AS adapter_version_count,
                   MIN(adapter_version) AS adapter_version,
                   COALESCE(MIN(rquant_lab_shard_row_valid(
                       shard_id, job_id, shard_index, status, version,
                       attempt_count, max_attempts, plan_hash, adapter_id,
                       adapter_version, payload_json, payload_hash, worker_id,
                       scheduler_fencing_token, claim_token, claim_generation,
                       claimed_at, heartbeat_at, lease_expires_at,
                       result_manifest_hash, failure_json, finished_at,
                       checkpoint_json, created_at, updated_at, phase,
                       work_unit_name, work_units, static_duration_ms,
                       duration_ms, throughput_units_per_second,
                       completion_sequence
                   )), 1) AS rows_valid
            FROM lab_shard
            WHERE job_id = ?
            """,
            (str(job.job_id),),
        ).fetchone()
        assert shard_aggregate is not None
        shard_count = _strict_sqlite_int(
            shard_aggregate["shard_count"],
            field="lab_shard.aggregate.shard_count",
            minimum=0,
        )
        succeeded_count = _strict_sqlite_int(
            shard_aggregate["succeeded_count"],
            field="lab_shard.aggregate.succeeded_count",
            minimum=0,
        )
        rows_valid = _strict_sqlite_int(
            shard_aggregate["rows_valid"],
            field="lab_shard.aggregate.rows_valid",
            minimum=0,
            maximum=1,
        )
        if rows_valid != 1:
            raise InvalidStoredJobError("job contains an invalid stored lab shard")

        if not job.requires_complete_result:
            if index_row is not None:
                raise InvalidStoredJobError("legacy job unexpectedly has a complete result index")
            return None

        if job.result_state is LabResultState.PENDING:
            if index_row is not None:
                raise InvalidStoredJobError("pending job unexpectedly has a result index")
            if (
                job.status is JobStatus.RUNNING
                and job.result_contract_version == COMPLETE_RESULT_CONTRACT_VERSION
                and shard_count > 0
                and succeeded_count == shard_count
            ):
                raise InvalidStoredJobError(
                    "running job with all shards succeeded must be result ready"
                )
            return None

        if shard_count == 0 or succeeded_count != shard_count:
            raise InvalidStoredJobError(
                "ready or sealed complete result job requires succeeded shards"
            )
        if job.result_state is LabResultState.READY:
            if index_row is not None:
                raise InvalidStoredJobError("ready job unexpectedly has a result index")
            return None
        if job.result_state is not LabResultState.SEALED or index_row is None:
            raise InvalidStoredJobError("sealed job is missing its complete result index")

        evidence = _result_artifact_evidence_from_row(
            index_row,
            expected_job_id=job.job_id,
        )
        request_id = _canonical_uuid_text(
            index_row["commit_request_id"],
            field="lab_job_result_artifact.commit_request_id",
        )
        commit_row = connection.execute(
            "SELECT * FROM lab_artifact_commit WHERE request_id = ?",
            (str(request_id),),
        ).fetchone()
        if commit_row is None:
            raise InvalidStoredJobError("result index is missing its accepted commit")
        record = _artifact_commit_record_from_row(
            commit_row,
            expected_request_id=request_id,
        )
        commit = record.envelope.commit
        if (
            record.receipt.status,
            record.receipt.reason,
            record.receipt.job_version,
            commit.job_id,
            commit.spec_hash,
            commit.code_sha,
            commit.dataset_snapshot,
            commit.result_contract_version,
            commit.sealed_path,
            commit.manifest_hash,
            commit.complete_result_hash,
        ) != (
            "accepted",
            "artifact_committed",
            job.version,
            job.job_id,
            job.spec_hash,
            job.spec.code_sha,
            job.spec.dataset_snapshot,
            COMPLETE_RESULT_CONTRACT_VERSION,
            evidence.sealed_path,
            evidence.manifest_hash,
            evidence.complete_result_hash,
        ):
            raise InvalidStoredJobError(
                "accepted commit, result index, and sealed job identities conflict"
            )
        aggregate_identity = (
            _strict_sqlite_int(
                shard_aggregate["plan_hash_count"],
                field="lab_shard.aggregate.plan_hash_count",
                minimum=0,
            ),
            str(shard_aggregate["plan_hash"]),
            _strict_sqlite_int(
                shard_aggregate["adapter_id_count"],
                field="lab_shard.aggregate.adapter_id_count",
                minimum=0,
            ),
            str(shard_aggregate["adapter_id"]),
            _strict_sqlite_int(
                shard_aggregate["adapter_version_count"],
                field="lab_shard.aggregate.adapter_version_count",
                minimum=0,
            ),
            str(shard_aggregate["adapter_version"]),
        )
        if aggregate_identity != (
            1,
            commit.plan_hash,
            1,
            commit.adapter_id,
            1,
            commit.adapter_version,
        ):
            raise InvalidStoredJobError("accepted commit identity conflicts with succeeded shards")
        return evidence

    @staticmethod
    def _encode_cursor(updated_at: datetime, job_id: UUID) -> str:
        payload = json.dumps(
            [_dump_time(updated_at), str(job_id)],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        return urlsafe_b64encode(payload).decode("ascii").rstrip("=")

    @staticmethod
    def _decode_cursor(cursor: str) -> tuple[str, UUID]:
        try:
            padding = "=" * (-len(cursor) % 4)
            payload = urlsafe_b64decode(f"{cursor}{padding}".encode("ascii"))
            value = json.loads(payload)
            if (
                not isinstance(value, list)
                or len(value) != 2
                or not all(isinstance(item, str) for item in value)
            ):
                raise ValueError
            updated_at = _dump_time(_load_time(value[0]))
            job_id = _canonical_uuid_text(value[1], field="cursor.job_id")
            canonical = LabJobReader._encode_cursor(_load_time(updated_at), job_id)
            if canonical != cursor:
                raise ValueError
            return updated_at, job_id
        except Exception as exc:
            raise ValueError("invalid opaque job cursor") from exc

    @staticmethod
    def _progress_from_row(row: sqlite3.Row) -> LabJobProgress:
        total = _strict_sqlite_int(row["shard_count"], field="shard_count", minimum=0)
        if total > MAX_JOB_SHARDS:
            raise InvalidStoredJobError(
                f"job shard count exceeds authoritative shard limit {MAX_JOB_SHARDS}"
            )
        succeeded = _strict_sqlite_int(row["succeeded_count"], field="succeeded_count", minimum=0)
        failed = _strict_sqlite_int(row["failed_count"], field="failed_count", minimum=0)
        cancelled = _strict_sqlite_int(row["cancelled_count"], field="cancelled_count", minimum=0)
        terminal = succeeded + failed + cancelled
        if terminal > total:
            raise InvalidStoredJobError("terminal shard count exceeds total shard count")
        return LabJobProgress(
            total_shards=total,
            terminal_shards=terminal,
            succeeded_shards=succeeded,
            failed_shards=failed,
            cancelled_shards=cancelled,
            fraction=(terminal / total if total else 0),
            phase=(str(row["active_phase"]) if row["active_phase"] is not None else None),
        )

    @staticmethod
    def _artifact_from_row(row: sqlite3.Row) -> LabArtifactRecord:
        try:
            return LabArtifactRecord(
                artifact_id=_canonical_uuid_text(
                    row["artifact_id"], field="lab_artifact.artifact_id"
                ),
                job_id=_canonical_uuid_text(row["job_id"], field="lab_artifact.job_id"),
                shard_id=(
                    _canonical_uuid_text(row["shard_id"], field="lab_artifact.shard_id")
                    if row["shard_id"] is not None
                    else None
                ),
                artifact_type=str(row["artifact_type"]),
                uri=str(row["uri"]),
                content_hash=str(row["content_hash"]),
                created_at=_load_time(str(row["created_at"])),
            )
        except Exception as exc:
            raise InvalidStoredJobError(
                f"invalid stored lab artifact {row['artifact_id']}: {exc}"
            ) from exc

    @staticmethod
    def _summary_stats_sql() -> str:
        return """
            WITH shard_stats AS (
                SELECT job_id,
                       COUNT(*) AS shard_count,
                       SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END)
                           AS succeeded_count,
                       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)
                           AS failed_count,
                       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END)
                           AS cancelled_count,
                       MAX(CASE WHEN status <> 'succeeded' AND attempt_count >= max_attempts
                                THEN 1 ELSE 0 END) AS has_exhausted,
                       COALESCE(MIN(rquant_lab_shard_row_valid(
                           shard_id, job_id, shard_index, status, version,
                           attempt_count, max_attempts, plan_hash, adapter_id,
                           adapter_version, payload_json, payload_hash, worker_id,
                           scheduler_fencing_token, claim_token, claim_generation,
                           claimed_at, heartbeat_at, lease_expires_at,
                           result_manifest_hash, failure_json, finished_at,
                           checkpoint_json, created_at, updated_at, phase,
                           work_unit_name, work_units, static_duration_ms,
                           duration_ms, throughput_units_per_second,
                           completion_sequence
                       )), 1) AS rows_valid,
                       MAX(CASE WHEN status = 'running' THEN heartbeat_at END)
                           AS latest_heartbeat_at,
                       SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END)
                           AS active_count
                FROM lab_shard GROUP BY job_id
            )
        """

    @staticmethod
    def _summary_columns_sql() -> str:
        return """
            j.*,
            COALESCE(ss.shard_count, 0) AS shard_count,
            COALESCE(ss.succeeded_count, 0) AS succeeded_count,
            COALESCE(ss.failed_count, 0) AS failed_count,
            COALESCE(ss.cancelled_count, 0) AS cancelled_count,
            COALESCE(ss.has_exhausted, 0) AS has_exhausted,
            COALESCE(ss.rows_valid, 1) AS rows_valid,
            COALESCE(ss.active_count, 0) AS active_count,
            ss.latest_heartbeat_at AS latest_heartbeat_at,
            (SELECT s.phase FROM lab_shard AS s
             WHERE s.job_id = j.job_id
               AND s.status IN ('running', 'queued', 'checkpointed')
             ORDER BY CASE s.status WHEN 'running' THEN 0
                                    WHEN 'checkpointed' THEN 1 ELSE 2 END,
                      s.shard_index, s.shard_id
             LIMIT 1) AS active_phase,
            CASE WHEN EXISTS (
                SELECT 1 FROM lab_job_result_artifact AS result
                WHERE result.job_id = j.job_id
            ) THEN 1 ELSE 0 END AS has_result_index,
            (SELECT result.evidence_json FROM lab_job_result_artifact AS result
             WHERE result.job_id = j.job_id) AS result_evidence_json
        """

    @staticmethod
    def _job_filters_sql(
        filters: LabJobListFilters,
    ) -> tuple[list[str], list[object]]:
        clauses: list[str] = []
        parameters: list[object] = []
        for column, values in (
            ("j.status", filters.statuses),
            ("j.job_type", filters.job_types),
            ("j.resource_class", filters.resource_classes),
        ):
            if values:
                clauses.append(f"{column} IN ({','.join('?' for _ in values)})")
                parameters.extend(item.value for item in values)
        if filters.created_from is not None:
            clauses.append("j.created_at >= ?")
            parameters.append(_dump_time(filters.created_from))
        if filters.created_before is not None:
            clauses.append("j.created_at < ?")
            parameters.append(_dump_time(filters.created_before))
        if filters.keyword is not None:
            escaped = (
                filters.keyword.casefold()
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
            )
            clauses.append(
                "(LOWER(json_extract(j.spec_json, '$.parameters.strategy_name')) "
                "LIKE ? ESCAPE '\\' OR LOWER(j.job_id) LIKE ? ESCAPE '\\' "
                "OR LOWER(j.spec_hash) LIKE ? ESCAPE '\\')"
            )
            parameters.extend((f"%{escaped}%",) * 3)
        if len(parameters) > LAB_JOB_LIST_FILTER_SQL_PARAMETER_MAX:
            raise ValueError("job list filters exceed the SQL parameter budget")
        return clauses, parameters

    @classmethod
    def _summary_from_row(cls, row: sqlite3.Row) -> LabJobSummary:
        job = cls._job_from_row(row)
        progress = cls._progress_from_row(row)
        rows_valid = _strict_sqlite_bool(row["rows_valid"], field="rows_valid")
        if not rows_valid:
            raise InvalidStoredJobError("job summary contains an invalid stored lab shard")
        has_result_index = _strict_sqlite_bool(row["has_result_index"], field="has_result_index")
        if (job.result_state is LabResultState.SEALED) != has_result_index:
            raise InvalidStoredJobError("job summary result index conflicts with result state")
        if has_result_index:
            try:
                evidence = LabArtifactIndexEvidence.model_validate_json(
                    str(row["result_evidence_json"])
                )
            except Exception as exc:
                raise InvalidStoredJobError("job summary result evidence is not canonical") from exc
            if evidence.job_id != job.job_id or _canonical_model_json(evidence) != str(
                row["result_evidence_json"]
            ):
                raise InvalidStoredJobError(
                    "job summary result evidence conflicts with job identity"
                )
        has_exhausted = _strict_sqlite_bool(row["has_exhausted"], field="has_exhausted")
        return LabJobSummary(
            job_id=job.job_id,
            strategy_name=job.spec.parameters.strategy_name,
            spec_hash=job.spec_hash,
            job_type=job.job_type,
            resource_class=job.resource_class,
            status=job.status,
            control_intent=job.control_intent,
            result_state=job.result_state,
            version=job.version,
            deadline=job.deadline,
            created_at=job.created_at,
            updated_at=job.updated_at,
            progress=progress,
            command_availability=command_availability_for_job(
                job,
                has_exhausted_non_succeeded_shard=has_exhausted,
            ),
        )

    def list_jobs(
        self,
        *,
        filters: LabJobListFilters | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> LabJobPage:
        if not 1 <= limit <= LAB_JOB_LIST_LIMIT_MAX:
            raise ValueError(f"limit must be between 1 and {LAB_JOB_LIST_LIMIT_MAX}")
        selected_filters = LabJobListFilters.model_validate(filters or LabJobListFilters())
        clauses, parameters = self._job_filters_sql(selected_filters)
        total_where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        page_clauses = list(clauses)
        page_parameters = list(parameters)
        if cursor is not None:
            cursor_time, cursor_id = self._decode_cursor(cursor)
            page_clauses.append("(j.updated_at < ? OR (j.updated_at = ? AND j.job_id < ?))")
            page_parameters.extend((cursor_time, cursor_time, str(cursor_id)))
        if len(page_parameters) + 1 > LAB_JOB_LIST_QUERY_PARAMETER_MAX:
            raise ValueError("job list query exceeds the SQL parameter budget")
        page_where = f" WHERE {' AND '.join(page_clauses)}" if page_clauses else ""
        with self._connect() as connection:
            total_row = connection.execute(
                f"SELECT COUNT(*) AS total_count FROM lab_job AS j{total_where}",
                parameters,
            ).fetchone()
            rows = connection.execute(
                f"{self._summary_stats_sql()} "
                f"SELECT {self._summary_columns_sql()} FROM lab_job AS j "
                f"LEFT JOIN shard_stats AS ss ON ss.job_id = j.job_id{page_where} "
                "ORDER BY j.updated_at DESC, j.job_id DESC LIMIT ?",
                (*page_parameters, limit + 1),
            ).fetchall()
        assert total_row is not None
        total_count = _strict_sqlite_int(total_row["total_count"], field="total_count", minimum=0)
        has_more = len(rows) > limit
        visible = rows[:limit]
        items = tuple(self._summary_from_row(row) for row in visible)
        next_cursor = (
            self._encode_cursor(items[-1].updated_at, items[-1].job_id)
            if has_more and items
            else None
        )
        return LabJobPage(
            items=items,
            total_count=total_count,
            has_more=has_more,
            next_cursor=next_cursor,
        )

    def list_finalization_candidates(
        self,
        *,
        limit: int = 50,
        cursor: str | None = None,
    ) -> LabFinalizationCandidatePage:
        if not 1 <= limit <= LAB_JOB_LIST_LIMIT_MAX:
            raise ValueError(f"limit must be between 1 and {LAB_JOB_LIST_LIMIT_MAX}")
        clauses = [
            "j.status = 'running'",
            "j.control_intent = 'none'",
            "j.result_state = 'ready'",
            "j.requires_complete_result = 1",
            "j.result_contract_version = ?",
        ]
        parameters: list[object] = [COMPLETE_RESULT_CONTRACT_VERSION]
        total_where = f" WHERE {' AND '.join(clauses)}"
        if cursor is not None:
            cursor_time, cursor_id = self._decode_cursor(cursor)
            clauses.append("(j.updated_at < ? OR (j.updated_at = ? AND j.job_id < ?))")
            parameters.extend((cursor_time, cursor_time, str(cursor_id)))
        with self._connect() as connection:
            total_row = connection.execute(
                f"SELECT COUNT(*) AS total_count FROM lab_job AS j{total_where}",
                (COMPLETE_RESULT_CONTRACT_VERSION,),
            ).fetchone()
            rows = connection.execute(
                f"SELECT j.* FROM lab_job AS j WHERE {' AND '.join(clauses)} "
                "ORDER BY j.updated_at DESC, j.job_id DESC LIMIT ?",
                (*parameters, limit + 1),
            ).fetchall()
        assert total_row is not None
        has_more = len(rows) > limit
        jobs = tuple(self._job_from_row(row) for row in rows[:limit])
        items = tuple(
            LabFinalizationCandidate(
                job_id=job.job_id,
                job_version=job.version,
                spec_hash=job.spec_hash,
                updated_at=job.updated_at,
            )
            for job in jobs
        )
        return LabFinalizationCandidatePage(
            items=items,
            total_count=_strict_sqlite_int(
                total_row["total_count"], field="total_count", minimum=0
            ),
            has_more=has_more,
            next_cursor=(
                self._encode_cursor(items[-1].updated_at, items[-1].job_id)
                if has_more and items
                else None
            ),
        )

    @staticmethod
    def _eta_input_from_rows(
        *,
        job_id: UUID,
        status: str,
        as_of: datetime,
        completed_rows: Iterable[sqlite3.Row],
        remaining_rows: Iterable[sqlite3.Row],
    ) -> LabEtaInput:
        from rquant.lab_eta import LabEtaCompletedShard, LabEtaRemainingShard

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
                    shard_id=_canonical_uuid_text(row["shard_id"], field="lab_shard.shard_id"),
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
                    shard_id=_canonical_uuid_text(row["shard_id"], field="lab_shard.shard_id"),
                    work_plan=plan,
                )
            )
        return LabEtaInput(
            job_id=job_id,
            status=status,
            as_of=as_of,
            completed=tuple(completed),
            remaining=tuple(remaining),
        )

    def get_job_detail(
        self,
        job_id: UUID,
        *,
        as_of: datetime,
        shard_limit: int = 100,
        event_limit: int = 100,
        artifact_limit: int = 50,
        heartbeat_stale_after: timedelta = timedelta(minutes=2),
        completed_telemetry_limit: int = LAB_ETA_COMPLETED_LIMIT_MAX,
    ) -> LabJobDetail | None:
        if not 1 <= shard_limit <= LAB_JOB_DETAIL_SHARD_LIMIT_MAX:
            raise ValueError(f"shard_limit must be between 1 and {LAB_JOB_DETAIL_SHARD_LIMIT_MAX}")
        if not 1 <= event_limit <= LAB_JOB_DETAIL_EVENT_LIMIT_MAX:
            raise ValueError(f"event_limit must be between 1 and {LAB_JOB_DETAIL_EVENT_LIMIT_MAX}")
        if not 1 <= artifact_limit <= LAB_JOB_DETAIL_ARTIFACT_LIMIT_MAX:
            raise ValueError(
                f"artifact_limit must be between 1 and {LAB_JOB_DETAIL_ARTIFACT_LIMIT_MAX}"
            )
        if not 3 <= completed_telemetry_limit <= LAB_ETA_COMPLETED_LIMIT_MAX:
            raise ValueError(
                f"completed telemetry limit must be between 3 and {LAB_ETA_COMPLETED_LIMIT_MAX}"
            )
        current = _utc(as_of)
        stale_seconds = heartbeat_stale_after.total_seconds()
        if not math.isfinite(stale_seconds) or stale_seconds <= 0:
            raise ValueError("heartbeat_stale_after must be a positive finite duration")

        with self._connect() as connection:
            connection.execute("BEGIN")
            job_row = connection.execute(
                "SELECT * FROM lab_job WHERE job_id = ?", (str(job_id),)
            ).fetchone()
            if job_row is None:
                connection.execute("COMMIT")
                return None
            job = self._job_from_row(job_row)
            result_evidence = self._validate_complete_result_graph(connection, job)
            stats_row = connection.execute(
                f"{self._summary_stats_sql()} "
                f"SELECT {self._summary_columns_sql()} FROM lab_job AS j "
                "LEFT JOIN shard_stats AS ss ON ss.job_id = j.job_id WHERE j.job_id = ?",
                (str(job_id),),
            ).fetchone()
            assert stats_row is not None
            progress = self._progress_from_row(stats_row)
            has_exhausted = _strict_sqlite_bool(stats_row["has_exhausted"], field="has_exhausted")

            shard_rows = connection.execute(
                "SELECT * FROM lab_shard WHERE job_id = ? ORDER BY shard_index, shard_id LIMIT ?",
                (str(job_id), shard_limit + 1),
            ).fetchall()
            event_rows = connection.execute(
                "SELECT *, COUNT(*) OVER() AS bounded_total FROM lab_event "
                "WHERE job_id = ? ORDER BY event_id DESC LIMIT ?",
                (str(job_id), event_limit + 1),
            ).fetchall()
            failure_row = connection.execute(
                "SELECT report.*, shard.shard_index FROM lab_worker_report AS report "
                "JOIN lab_shard AS shard ON shard.shard_id = report.shard_id "
                "WHERE report.job_id = ? AND report.status = 'accepted' "
                "AND report.report_type = 'shard_failed' "
                "ORDER BY report.applied_at, report.report_id LIMIT 1",
                (str(job_id),),
            ).fetchone()
            artifact_rows = connection.execute(
                "SELECT *, COUNT(*) OVER() AS bounded_total FROM lab_artifact "
                "WHERE job_id = ? ORDER BY created_at, artifact_id LIMIT ?",
                (str(job_id), artifact_limit + 1),
            ).fetchall()

            if job.status in {
                JobStatus.QUEUED,
                JobStatus.RUNNING,
                JobStatus.CHECKPOINTED,
            }:
                completed_rows = connection.execute(
                    "SELECT shard_id, phase, work_unit_name, work_units, "
                    "static_duration_ms, duration_ms, throughput_units_per_second, "
                    "completion_sequence FROM lab_shard "
                    "WHERE job_id = ? AND status = 'succeeded' "
                    "AND completion_sequence IS NOT NULL "
                    "ORDER BY completion_sequence DESC LIMIT ?",
                    (str(job_id), completed_telemetry_limit),
                ).fetchall()
                remaining_rows = connection.execute(
                    "SELECT shard_id, phase, work_unit_name, work_units, static_duration_ms "
                    "FROM lab_shard WHERE job_id = ? "
                    "AND status IN ('queued', 'running', 'checkpointed') "
                    "ORDER BY shard_index, shard_id LIMIT ?",
                    (str(job_id), MAX_JOB_SHARDS + 1),
                ).fetchall()
                if len(remaining_rows) > MAX_JOB_SHARDS:
                    raise InvalidStoredJobError(
                        f"job remaining shards exceed authoritative shard limit {MAX_JOB_SHARDS}"
                    )
            else:
                completed_rows = ()
                remaining_rows = ()
            connection.execute("COMMIT")

        shards_truncated = len(shard_rows) > shard_limit
        shards = tuple(self._shard_from_row(row) for row in shard_rows[:shard_limit])
        events_truncated = len(event_rows) > event_limit
        events = tuple(self._event_from_row(row) for row in event_rows[:event_limit])
        event_count = (
            _strict_sqlite_int(event_rows[0]["bounded_total"], field="event_count", minimum=0)
            if event_rows
            else 0
        )
        artifacts_truncated = len(artifact_rows) > artifact_limit
        artifacts = tuple(self._artifact_from_row(row) for row in artifact_rows[:artifact_limit])
        artifact_count = (
            _strict_sqlite_int(artifact_rows[0]["bounded_total"], field="artifact_count", minimum=0)
            if artifact_rows
            else 0
        )
        first_failure = None
        if failure_row is not None:
            report_id = _canonical_uuid_text(
                failure_row["report_id"], field="lab_worker_report.report_id"
            )
            failed_report = _worker_report_record_from_row(
                failure_row,
                expected_report_id=report_id,
            )
            if not isinstance(failed_report.report.body, LabShardFailed):
                raise InvalidStoredJobError("accepted first failure has the wrong report body")
            first_failure = LabFirstFailure(
                shard_id=_canonical_uuid_text(failure_row["shard_id"], field="lab_shard.shard_id"),
                shard_index=_strict_sqlite_int(
                    failure_row["shard_index"],
                    field="lab_shard.shard_index",
                    minimum=0,
                ),
                failure=failed_report.report.body,
                finished_at=failed_report.applied_at,
            )
        latest_heartbeat = (
            _load_time(str(stats_row["latest_heartbeat_at"]))
            if stats_row["latest_heartbeat_at"] is not None
            else None
        )
        active_count = _strict_sqlite_int(
            stats_row["active_count"], field="active_count", minimum=0
        )
        heartbeat = LabHeartbeatStatus(
            active_shards=active_count,
            latest_heartbeat_at=latest_heartbeat,
            stale_after_seconds=stale_seconds,
            stale=(
                active_count > 0
                and (latest_heartbeat is None or current - latest_heartbeat > heartbeat_stale_after)
            ),
        )
        from rquant.lab_eta import estimate_lab_eta

        eta = estimate_lab_eta(
            self._eta_input_from_rows(
                job_id=job_id,
                status=_effective_lab_eta_status(
                    status=job.status,
                    control_intent=job.control_intent,
                ),
                as_of=current,
                completed_rows=completed_rows,
                remaining_rows=remaining_rows,
            )
        )
        return LabJobDetail(
            job=job,
            progress=progress,
            heartbeat=heartbeat,
            command_availability=command_availability_for_job(
                job,
                has_exhausted_non_succeeded_shard=has_exhausted,
            ),
            eta=eta,
            first_failure=first_failure,
            shards=shards,
            shard_count=progress.total_shards,
            shards_truncated=shards_truncated,
            events=events,
            event_count=event_count,
            events_truncated=events_truncated,
            artifacts=artifacts,
            artifact_count=artifact_count,
            artifacts_truncated=artifacts_truncated,
            result_evidence=result_evidence,
        )

    def get_job(self, job_id: UUID) -> LabJobRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM lab_job WHERE job_id = ?",
                (str(job_id),),
            ).fetchone()
            if row is None:
                return None
            job = self._job_from_row(row)
            self._validate_complete_result_graph(connection, job)
            return job

    def get_command_context(self, job_id: UUID) -> LabJobCommandContext | None:
        with self._connect() as connection:
            row = connection.execute(
                f"{self._summary_stats_sql()} "
                f"SELECT {self._summary_columns_sql()} FROM lab_job AS j "
                "LEFT JOIN shard_stats AS ss ON ss.job_id = j.job_id WHERE j.job_id = ?",
                (str(job_id),),
            ).fetchone()
            if row is None:
                return None
            job = self._job_from_row(row)
            self._validate_complete_result_graph(connection, job)
            summary = self._summary_from_row(row)
            return LabJobCommandContext(
                job=job,
                availability=summary.command_availability,
            )

    def get_artifact_preview_authority(
        self,
        job_id: UUID,
    ) -> LabArtifactPreviewAuthority | None:
        with self._connect() as connection:
            connection.execute("BEGIN")
            row = connection.execute(
                "SELECT * FROM lab_job WHERE job_id = ?",
                (str(job_id),),
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            job = self._job_from_row(row)
            evidence = self._validate_complete_result_graph(connection, job)
            if (
                job.status is not JobStatus.SUCCEEDED
                or job.result_state is not LabResultState.SEALED
                or evidence is None
            ):
                connection.execute("COMMIT")
                return None
            authority = LabArtifactPreviewAuthority(job=job, evidence=evidence)
            connection.execute("COMMIT")
            return authority

    def get_finalization_snapshot(self, job_id: UUID) -> LabFinalizationSnapshot | None:
        """Return one validated ready-result graph from a single read transaction."""

        connection = self._connect()
        lifecycle_errors: list[BaseException] = []
        try:
            connection.execute("BEGIN")
            job_row = connection.execute(
                "SELECT * FROM lab_job WHERE job_id = ?",
                (str(job_id),),
            ).fetchone()
            if job_row is None:
                connection.execute("COMMIT")
                return None
            job = self._job_from_row(job_row)
            self._validate_complete_result_graph(connection, job)
            self._after_finalization_job_read(job_id)
            if (
                job.status is not JobStatus.RUNNING
                or job.result_state is not LabResultState.READY
                or job.result_contract_version != COMPLETE_RESULT_CONTRACT_VERSION
                or not job.requires_complete_result
                or job.control_intent is not ControlIntent.NONE
            ):
                connection.execute("COMMIT")
                return None

            ready_event_rows = connection.execute(
                """
                SELECT * FROM lab_event
                WHERE job_id = ? AND event_type = 'job_result_ready'
                  AND job_version = ?
                ORDER BY event_id
                """,
                (str(job_id), job.version),
            ).fetchall()
            if len(ready_event_rows) != 1:
                raise InvalidStoredJobError(
                    "ready finalization snapshot requires exactly one ready epoch event"
                )
            ready_event = self._event_from_row(ready_event_rows[0])

            shard_rows = connection.execute(
                "SELECT * FROM lab_shard WHERE job_id = ? ORDER BY shard_index",
                (str(job_id),),
            ).fetchall()
            shards = tuple(self._shard_from_row(row) for row in shard_rows)
            if not shards or any(shard.status is not ShardStatus.SUCCEEDED for shard in shards):
                raise InvalidStoredJobError(
                    "ready finalization snapshot requires all and only succeeded shards"
                )

            report_rows = connection.execute(
                """
                SELECT * FROM lab_worker_report
                WHERE job_id = ? AND status = 'accepted'
                  AND report_type = 'shard_succeeded'
                ORDER BY shard_id, applied_at, report_id
                """,
                (str(job_id),),
            ).fetchall()
            reports_by_shard: dict[UUID, list[LabWorkerReportRecord]] = {}
            for row in report_rows:
                report_id = _canonical_uuid_text(
                    row["report_id"],
                    field="lab_worker_report.report_id",
                )
                record = _worker_report_record_from_row(
                    row,
                    expected_report_id=report_id,
                )
                reports_by_shard.setdefault(record.report.shard_id, []).append(record)

            shard_ids = {shard.shard_id for shard in shards}
            if set(reports_by_shard) != shard_ids or any(
                len(records) != 1 for records in reports_by_shard.values()
            ):
                raise InvalidStoredJobError(
                    "each finalization shard requires exactly one accepted success report"
                )
            try:
                snapshot = LabFinalizationSnapshot(
                    job=job,
                    ready_epoch=LabFinalizationReadyEpoch(
                        job_version=job.version,
                        event=ready_event,
                    ),
                    shards=tuple(
                        LabFinalizationShardEvidence(
                            shard=shard,
                            accepted_success=reports_by_shard[shard.shard_id][0],
                        )
                        for shard in shards
                    ),
                )
            except Exception as exc:
                raise InvalidStoredJobError(
                    f"invalid finalization snapshot for job {job_id}: {exc}"
                ) from exc
            connection.execute("COMMIT")
            return snapshot
        except BaseException as exc:
            lifecycle_errors.append(exc)
            if connection.in_transaction:
                try:
                    connection.rollback()
                except BaseException as rollback_error:
                    lifecycle_errors.append(rollback_error)
        finally:
            try:
                connection.close()
            except BaseException as close_error:
                lifecycle_errors.append(close_error)
            if len(lifecycle_errors) == 1:
                raise lifecycle_errors[0]
            if lifecycle_errors:
                raise BaseExceptionGroup(
                    "finalization snapshot query and cleanup failed",
                    lifecycle_errors,
                )

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
                "SELECT * FROM lab_shard WHERE job_id = ? ORDER BY shard_index LIMIT ?",
                (str(job_id), MAX_JOB_SHARDS + 1),
            ).fetchall()
        if len(rows) > MAX_JOB_SHARDS:
            raise InvalidStoredJobError(
                f"job shards exceed authoritative shard limit {MAX_JOB_SHARDS}"
            )
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
                "SELECT status, control_intent FROM lab_job WHERE job_id = ?",
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
                LIMIT ?
                """,
                (str(job_id), MAX_JOB_SHARDS + 1),
            ).fetchall()

        if len(remaining_rows) > MAX_JOB_SHARDS:
            raise InvalidStoredJobError(
                f"job remaining shards exceed authoritative shard limit {MAX_JOB_SHARDS}"
            )

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
                    shard_id=_canonical_uuid_text(
                        row["shard_id"],
                        field="lab_shard.shard_id",
                    ),
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
                    shard_id=_canonical_uuid_text(
                        row["shard_id"],
                        field="lab_shard.shard_id",
                    ),
                    work_plan=plan,
                )
            )
        return LabEtaInput(
            job_id=job_id,
            status=_effective_lab_eta_status(
                status=JobStatus(str(job_row["status"])),
                control_intent=ControlIntent(str(job_row["control_intent"])),
            ),
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
                artifact_id=_canonical_uuid_text(
                    row["artifact_id"],
                    field="lab_artifact.artifact_id",
                ),
                job_id=_canonical_uuid_text(row["job_id"], field="lab_artifact.job_id"),
                shard_id=(
                    _canonical_uuid_text(row["shard_id"], field="lab_artifact.shard_id")
                    if row["shard_id"] is not None
                    else None
                ),
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
        connection = self._connect()
        lifecycle_errors: list[BaseException] = []
        result: LabArtifactCommitRecord | None = None
        try:
            connection.execute("BEGIN")
            row = connection.execute(
                "SELECT * FROM lab_artifact_commit WHERE request_id = ?",
                (str(request_id),),
            ).fetchone()
            if row is not None:
                result = _artifact_commit_record_from_row(
                    row,
                    expected_request_id=request_id,
                )
                if result.receipt.status == "accepted":
                    job_row = connection.execute(
                        "SELECT * FROM lab_job WHERE job_id = ?",
                        (str(result.receipt.job_id),),
                    ).fetchone()
                    if job_row is None:
                        raise InvalidStoredJobError("accepted artifact commit lost its job")
                    job = self._job_from_row(job_row)
                    evidence = self._validate_complete_result_graph(connection, job)
                    if evidence is None:
                        raise InvalidStoredJobError(
                            "accepted artifact commit lost its result index"
                        )
                    index_row = connection.execute(
                        "SELECT commit_request_id FROM lab_job_result_artifact WHERE job_id = ?",
                        (str(job.job_id),),
                    ).fetchone()
                    assert index_row is not None
                    primary_request_id = _canonical_uuid_text(
                        index_row["commit_request_id"],
                        field="lab_job_result_artifact.commit_request_id",
                    )
                    commit = result.envelope.commit
                    shard_identity = {
                        (str(shard[0]), str(shard[1]), str(shard[2]))
                        for shard in connection.execute(
                            """
                            SELECT plan_hash, adapter_id, adapter_version
                            FROM lab_shard WHERE job_id = ?
                            """,
                            (str(job.job_id),),
                        ).fetchall()
                    }
                    if (
                        result.receipt.reason
                        not in {"artifact_committed", "artifact_already_committed"}
                        or result.receipt.job_version != job.version
                        or (
                            result.receipt.reason == "artifact_committed"
                            and request_id != primary_request_id
                        )
                        or commit.job_id != job.job_id
                        or commit.spec_hash != job.spec_hash
                        or commit.code_sha != job.spec.code_sha
                        or commit.dataset_snapshot != job.spec.dataset_snapshot
                        or commit.result_contract_version != COMPLETE_RESULT_CONTRACT_VERSION
                        or commit.sealed_path != evidence.sealed_path
                        or commit.manifest_hash != evidence.manifest_hash
                        or commit.complete_result_hash != evidence.complete_result_hash
                        or shard_identity
                        != {(commit.plan_hash, commit.adapter_id, commit.adapter_version)}
                    ):
                        raise InvalidStoredJobError(
                            "accepted artifact commit conflicts with the sealed result graph"
                        )
            connection.execute("COMMIT")
            return result
        except BaseException as exc:
            lifecycle_errors.append(exc)
            if connection.in_transaction:
                try:
                    connection.rollback()
                except BaseException as rollback_error:
                    lifecycle_errors.append(rollback_error)
        finally:
            try:
                connection.close()
            except BaseException as close_error:
                lifecycle_errors.append(close_error)
            if len(lifecycle_errors) == 1:
                raise lifecycle_errors[0]
            if lifecycle_errors:
                raise BaseExceptionGroup(
                    "artifact commit query and cleanup failed",
                    lifecycle_errors,
                )

    def get_result_artifact(self, job_id: UUID) -> LabArtifactIndexEvidence | None:
        with self._connect() as connection:
            job_row = connection.execute(
                "SELECT * FROM lab_job WHERE job_id = ?",
                (str(job_id),),
            ).fetchone()
            if job_row is None:
                return None
            job = self._job_from_row(job_row)
            return self._validate_complete_result_graph(connection, job)

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

    def _connect(self, *, validate_identity: bool = True) -> _LabJobStoreConnection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
            factory=_LabJobStoreConnection,
        )
        authorization = _LabWriteAuthorization(connection)
        connection.write_authorization = authorization
        connection.set_trace_callback(connection._trace_transaction_boundary)
        connection.create_function(
            _SUBMIT_AUTH_FUNCTION,
            2,
            authorization.submit_authorized,
        )
        connection.create_function(
            _RETRY_AUTH_FUNCTION,
            3,
            authorization.retry_authorized,
        )
        connection.create_function(
            _READY_TERMINAL_AUTH_FUNCTION,
            6,
            authorization.ready_terminal_authorized,
        )
        connection.create_function(
            _ARTIFACT_COMMIT_AUTH_FUNCTION,
            3,
            authorization.artifact_commit_authorized,
        )
        connection.create_function(
            _ARTIFACT_INDEX_AUTH_FUNCTION,
            3,
            authorization.artifact_index_authorized,
        )
        connection.create_function(
            _ARTIFACT_SUCCESS_AUTH_FUNCTION,
            5,
            authorization.artifact_success_authorized,
        )
        connection.create_function(
            _SHARD_ROW_VALID_FUNCTION,
            32,
            _sqlite_shard_row_valid,
            deterministic=True,
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

    def execute_for_test(self, statement: str) -> None:
        with self._connect() as connection:
            connection.execute(statement)

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
        return _result_artifact_evidence_from_row(row)

    @staticmethod
    def _record_artifact_commit(
        connection: sqlite3.Connection,
        envelope: LabArtifactCommitEnvelope,
        receipt: LabArtifactCommitReceipt,
        *,
        now: datetime,
    ) -> None:
        commit_json = _canonical_model_json(envelope)
        receipt_json = _canonical_model_json(receipt)
        with _write_authorization(connection).authorize_artifact_commit(
            envelope.request_id,
            commit_json,
            receipt_json,
        ):
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
                    commit_json,
                    receipt.status,
                    receipt.reason,
                    receipt_json,
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

    @staticmethod
    def _finalizer_authority_matches_ready_graph(
        connection: sqlite3.Connection,
        envelope: LabArtifactCommitEnvelope,
        job: LabJobRecord,
        shard_rows: list[sqlite3.Row],
        claims: LabFinalizerAuthorityClaims,
    ) -> bool:
        ready_rows = connection.execute(
            """
            SELECT * FROM lab_event
            WHERE job_id = ? AND event_type = 'job_result_ready'
              AND job_version = ?
            ORDER BY event_id
            """,
            (str(job.job_id), job.version),
        ).fetchall()
        if len(ready_rows) != 1:
            return False
        ready_event = LabJobReader._event_from_row(ready_rows[0])
        if ready_event.scheduler_fencing_token is None:
            return False

        report_rows = connection.execute(
            """
            SELECT * FROM lab_worker_report
            WHERE job_id = ? AND status = 'accepted'
              AND report_type = 'shard_succeeded'
            ORDER BY shard_id, applied_at, report_id
            """,
            (str(job.job_id),),
        ).fetchall()
        reports_by_shard: dict[UUID, list[LabWorkerReportRecord]] = {}
        for row in report_rows:
            report_id = _canonical_uuid_text(
                row["report_id"],
                field="lab_worker_report.report_id",
            )
            record = _worker_report_record_from_row(row, expected_report_id=report_id)
            reports_by_shard.setdefault(record.report.shard_id, []).append(record)

        shards = tuple(LabJobReader._shard_from_row(row) for row in shard_rows)
        if set(reports_by_shard) != {shard.shard_id for shard in shards} or any(
            len(records) != 1 for records in reports_by_shard.values()
        ):
            return False
        try:
            snapshot = LabFinalizationSnapshot(
                job=job,
                ready_epoch=LabFinalizationReadyEpoch(
                    job_version=job.version,
                    event=ready_event,
                ),
                shards=tuple(
                    LabFinalizationShardEvidence(
                        shard=shard,
                        accepted_success=reports_by_shard[shard.shard_id][0],
                    )
                    for shard in shards
                ),
            )
        except ValueError as exc:
            raise InvalidStoredJobError(
                "artifact authority graph is internally inconsistent"
            ) from exc
        expected = LabFinalizerAuthorityClaims(
            request_id=envelope.request_id,
            commit_content_hash=hashlib.sha256(envelope.commit.canonical_json_bytes()).hexdigest(),
            job_id=job.job_id,
            ready_event_id=ready_event.event_id,
            ready_job_version=job.version,
            scheduler_fencing_token=ready_event.scheduler_fencing_token,
            spec_hash=job.spec_hash,
            finalizer_code_sha=job.spec.code_sha,
            shards=tuple(
                LabFinalizerAuthorityShardEvidence(
                    shard_index=evidence.shard.shard_index,
                    shard_id=evidence.shard.shard_id,
                    payload_hash=evidence.shard.payload_hash,
                    plan_hash=evidence.shard.plan_hash,
                    result_manifest_hash=evidence.shard.result_manifest_hash or "",
                    accepted_report_content_hash=(evidence.accepted_success.report.content_hash),
                    claim_token=evidence.accepted_success.report.claim_token,
                    claim_generation=evidence.accepted_success.report.claim_generation,
                    scheduler_fencing_token=(
                        evidence.accepted_success.report.scheduler_fencing_token
                    ),
                )
                for evidence in snapshot.shards
            ),
            artifact_manifest_hash=envelope.commit.manifest_hash,
            complete_result_hash=envelope.commit.complete_result_hash,
        )
        return claims == expected

    def _apply_artifact_commit_in_transaction(
        self,
        connection: sqlite3.Connection,
        envelope: LabArtifactCommitEnvelope,
        binding: LabVerifiedSealedBinding,
        *,
        authority_key_provider: LabFinalizerAuthorityVerificationKeyProvider,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabArtifactCommitReceipt:
        from rquant.lab_artifacts import LabArtifactIndexEvidence

        authenticated = authenticate_artifact_commit_identity(
            envelope,
            key_provider=authority_key_provider,
        )

        existing_commit = connection.execute(
            "SELECT * FROM lab_artifact_commit WHERE request_id = ?",
            (str(envelope.request_id),),
        ).fetchone()
        if existing_commit is not None:
            record = _artifact_commit_record_from_row(
                existing_commit,
                expected_request_id=envelope.request_id,
            )
            existing_authenticated = authenticate_artifact_commit_identity(
                record.envelope,
                key_provider=authority_key_provider,
            )
            if existing_authenticated != authenticated:
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

        authority_claims = authenticated.claims

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
            not job.requires_complete_result
            or job.result_contract_version != COMPLETE_RESULT_CONTRACT_VERSION
        ):
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason="job_contract_mismatch",
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
        if not self._finalizer_authority_matches_ready_graph(
            connection,
            envelope,
            job,
            shard_rows,
            authority_claims,
        ):
            return self._reject_artifact_commit(
                connection,
                envelope,
                reason="finalizer_authority_graph_mismatch",
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
        authorization = _write_authorization(connection)
        with authorization.authorize_artifact_index(
            commit.job_id,
            envelope.request_id,
            evidence_json,
        ):
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
        with authorization.authorize_artifact_success(
            commit.job_id,
            envelope.request_id,
            evidence_json,
            job.version,
            next_version,
        ):
            cursor = connection.execute(
                """
                UPDATE lab_job
                SET status = ?, result_state = ?, version = ?,
                    scheduler_fencing_token = ?, updated_at = ?
                WHERE job_id = ? AND version = ? AND status = ?
                  AND result_state = ? AND control_intent = ?
                """,
                (
                    JobStatus.SUCCEEDED.value,
                    LabResultState.SEALED.value,
                    next_version,
                    lease.fencing_token,
                    _dump_time(now),
                    str(commit.job_id),
                    job.version,
                    JobStatus.RUNNING.value,
                    LabResultState.READY.value,
                    ControlIntent.NONE.value,
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

    @contextmanager
    def stage_artifact_commit(
        self,
        envelope: LabArtifactCommitEnvelope,
        binding: LabVerifiedSealedBinding,
        *,
        authority_key_provider: LabFinalizerAuthorityVerificationKeyProvider,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> Iterator[_LabStagedArtifactCommit]:
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
                authority_key_provider=authority_key_provider,
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

            staged = _LabStagedArtifactCommit(
                connection,
                receipt,
                lease=lease,
                precommit_validator=validate_before_commit,
            )
        except BaseException:
            connection.rollback()
            connection.close()
            raise
        with staged:
            yield staged

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
        job_id = _canonical_uuid_text(job_row["job_id"], field="lab_job.job_id")
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
        if LabResultState(str(row["result_state"])) is LabResultState.READY:
            return row
        current_fence = _strict_nullable_sqlite_int(
            row["scheduler_fencing_token"],
            field="lab_job.scheduler_fencing_token",
            minimum=1,
        )
        if current_fence == lease.fencing_token:
            return row
        job_id = _canonical_uuid_text(row["job_id"], field="lab_job.job_id")
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
        source_result_state = LabResultState(str(row["result_state"]))
        if (
            source is JobStatus.RUNNING
            and source_result_state is not LabResultState.READY
            and (row_fence is None or row_fence != lease.fencing_token)
        ):
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
        if target_status is JobStatus.RUNNING or source_result_state is LabResultState.READY:
            next_fence = lease.fencing_token
        result_state = source_result_state
        if target_status is JobStatus.SUCCEEDED:
            raise InvalidJobTransitionError("job success requires a verified artifact commit")
        elif target_status in {JobStatus.FAILED, JobStatus.CANCELLED}:
            result_state = LabResultState.PENDING
        ready_terminal_scope = nullcontext()
        if LabResultState(str(row["result_state"])) is LabResultState.READY:
            ready_terminal_scope = _write_authorization(connection).authorize_ready_terminal(
                _canonical_uuid_text(row["job_id"], field="lab_job.job_id"),
                target_status,
                stored_version,
                version,
                int(next_recoverable),
                next_fence,
            )
        with ready_terminal_scope:
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
            job_id=_canonical_uuid_text(row["job_id"], field="lab_job.job_id"),
            request_id=request_id,
            event_type=event_type,
            prior_status=source,
            new_status=target_status,
            job_version=version,
            reason=reason,
            fencing_token=lease.fencing_token,
            now=now,
        )
        updated = self._load_job_row(
            connection,
            _canonical_uuid_text(row["job_id"], field="lab_job.job_id"),
        )
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
            job_id=_canonical_uuid_text(row["job_id"], field="lab_job.job_id"),
            request_id=request_id,
            event_type="control_intent_changed",
            prior_status=status,
            new_status=status,
            job_version=version,
            reason=reason,
            fencing_token=lease.fencing_token,
            now=now,
        )
        updated = self._load_job_row(
            connection,
            _canonical_uuid_text(row["job_id"], field="lab_job.job_id"),
        )
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
        with _write_authorization(connection).authorize_submit(
            command.job_id,
            spec_json,
        ):
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
        with _write_authorization(connection).authorize_retry(
            command.job_id,
            version,
            next_version,
        ):
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
        if len(definitions) > MAX_JOB_SHARDS:
            raise ValueError(f"a shard plan may contain at most {MAX_JOB_SHARDS} shards")
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
                "SELECT * FROM lab_shard WHERE job_id = ? ORDER BY shard_index LIMIT ?",
                (str(job_id), MAX_JOB_SHARDS + 1),
            ).fetchall()
            if len(existing_rows) > MAX_JOB_SHARDS:
                raise InvalidStoredJobError(
                    f"stored shard plan exceeds authoritative limit {MAX_JOB_SHARDS}"
                )
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
                "SELECT * FROM lab_shard WHERE job_id = ? ORDER BY shard_index LIMIT ?",
                (str(job_id), MAX_JOB_SHARDS + 1),
            ).fetchall()
            if len(rows) > MAX_JOB_SHARDS:  # pragma: no cover - guarded before insertion
                raise InvalidStoredJobError(
                    f"stored shard plan exceeds authoritative limit {MAX_JOB_SHARDS}"
                )
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
                job_id = _canonical_uuid_text(row["job_id"], field="lab_job.job_id")
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
                ready_terminal_scope = nullcontext()
                if LabResultState(str(row["result_state"])) is LabResultState.READY:
                    ready_terminal_scope = _write_authorization(
                        connection
                    ).authorize_ready_terminal(
                        job_id,
                        JobStatus.FAILED,
                        version,
                        version + 1,
                        0,
                        None,
                    )
                with ready_terminal_scope:
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
            shard_id=_canonical_uuid_text(row["shard_id"], field="lab_shard.shard_id"),
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
                _canonical_uuid_text(exhausted["job_id"], field="lab_shard.job_id"),
                _canonical_uuid_text(exhausted["shard_id"], field="lab_shard.shard_id"),
            )
        for stale in stale_rows:
            job_id = _canonical_uuid_text(stale["job_id"], field="lab_shard.job_id")
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
                failed_job_causes.setdefault(
                    job_id,
                    _canonical_uuid_text(stale["shard_id"], field="lab_shard.shard_id"),
                )
        for stale in stale_rows:
            job_id = _canonical_uuid_text(stale["job_id"], field="lab_shard.job_id")
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
            job_id = _canonical_uuid_text(row["job_id"], field="lab_job.job_id")
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
                    cursor_job_id = _canonical_uuid_text(
                        claim_cursor["claim_cursor_job_id"],
                        field="lab_scheduler_state.claim_cursor_job_id",
                    )
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
                job_id = _canonical_uuid_text(
                    job_candidate["job_id"],
                    field="lab_job.job_id",
                )
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
                            job_id=_canonical_uuid_text(
                                row["job_id"],
                                field="lab_shard.job_id",
                            ),
                            spec_hash=str(row["job_spec_hash"]),
                            definition=self._definition_from_shard_row(row),
                            worker_id=str(row["worker_id"]),
                            claim_token=_canonical_uuid_text(
                                row["claim_token"],
                                field="lab_shard.claim_token",
                            ),
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
                report_id = _canonical_uuid_text(
                    row["report_id"],
                    field="lab_worker_report.report_id",
                )
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
                    report_id = _canonical_uuid_text(
                        row["report_id"],
                        field="lab_worker_report.report_id",
                    )
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
                report.canonical_json(),
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
        result_digest_policy: LabResultDigestPolicy,
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
            job = LabJobReader._job_from_row(job_row)
            try:
                resolve_success_digest_provenance(
                    expected_job_code_sha=job.spec.code_sha,
                    result_manifest_schema_version=(report.body.result_manifest_schema_version),
                    content_digest_algorithm=report.body.content_digest_algorithm,
                    worker_code_sha=report.body.worker_code_sha,
                    policy=result_digest_policy,
                )
            except LabResultDigestProvenanceError:
                return "unsupported_result_digest_provenance"
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
        result_digest_policy: LabResultDigestPolicy | None = None,
    ) -> LabReportReceipt:
        validated = LabWorkerReport.model_validate(report)
        digest_policy = LabResultDigestPolicy.model_validate(
            result_digest_policy or LabResultDigestPolicy()
        )
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
                result_digest_policy=digest_policy,
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
                    job_id=_canonical_uuid_text(row["job_id"], field="lab_job.job_id"),
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
                    _canonical_uuid_text(row["job_id"], field="lab_job.job_id"),
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
        AND json_type(commit_json, '$') IS 'object'
        AND json_type(commit_json, '$.request_id') IS 'text'
        AND json_type(commit_json, '$.content_hash') IS 'text'
        AND json_type(commit_json, '$.commit') IS 'object'
        AND json_type(commit_json, '$.schema_version') IS 'integer'
        AND json_type(commit_json, '$.commit.schema_version') IS 'integer'
        AND json_type(commit_json, '$.commit.job_id') IS 'text'
        AND json_type(commit_json, '$.commit.spec_hash') IS 'text'
        AND json_type(commit_json, '$.commit.plan_hash') IS 'text'
        AND json_type(commit_json, '$.commit.adapter_id') IS 'text'
        AND json_type(commit_json, '$.commit.adapter_version') IS 'text'
        AND json_type(
            commit_json, '$.commit.result_contract_version'
        ) IS 'text'
        AND json_type(commit_json, '$.commit.code_sha') IS 'text'
        AND json_type(commit_json, '$.commit.manifest_hash') IS 'text'
        AND json_type(
            commit_json, '$.commit.complete_result_hash'
        ) IS 'text'
        AND json_type(commit_json, '$.commit.sealed_path') IS 'text'
        AND (
            json_type(commit_json, '$.commit.dataset_snapshot') IS 'null'
            OR (
                json_type(
                    commit_json, '$.commit.dataset_snapshot'
                ) IS 'object'
                AND json_remove(
                    json_extract(
                        commit_json, '$.commit.dataset_snapshot'
                    ),
                    '$.snapshot_id',
                    '$.binding_hash',
                    '$.audit_run_id'
                ) = '{}'
                AND json_type(
                    commit_json, '$.commit.dataset_snapshot.snapshot_id'
                ) IS 'text'
                AND length(json_extract(
                    commit_json, '$.commit.dataset_snapshot.snapshot_id'
                )) = 64
                AND json_extract(
                    commit_json, '$.commit.dataset_snapshot.snapshot_id'
                ) NOT GLOB '*[^0-9a-f]*'
                AND json_type(
                    commit_json, '$.commit.dataset_snapshot.binding_hash'
                ) IS 'text'
                AND length(json_extract(
                    commit_json, '$.commit.dataset_snapshot.binding_hash'
                )) = 64
                AND json_extract(
                    commit_json, '$.commit.dataset_snapshot.binding_hash'
                ) NOT GLOB '*[^0-9a-f]*'
                AND (
                    json_type(
                        commit_json, '$.commit.dataset_snapshot.audit_run_id'
                    ) IS 'null'
                    OR (
                        json_type(
                            commit_json, '$.commit.dataset_snapshot.audit_run_id'
                        ) IS 'text'
                        AND length(json_extract(
                            commit_json, '$.commit.dataset_snapshot.audit_run_id'
                        )) = 64
                        AND json_extract(
                            commit_json, '$.commit.dataset_snapshot.audit_run_id'
                        ) NOT GLOB '*[^0-9a-f]*'
                    )
                )
            )
        )
    ),
    status TEXT NOT NULL CHECK (status IN ('accepted', 'rejected')),
    reason TEXT NOT NULL CHECK (typeof(reason) = 'text' AND length(reason) > 0),
    receipt_json TEXT NOT NULL CHECK (
        typeof(receipt_json) = 'text' AND length(receipt_json) > 0
        AND json_valid(receipt_json)
        AND json_type(receipt_json, '$') IS 'object'
        AND json_type(receipt_json, '$.request_id') IS 'text'
        AND json_type(receipt_json, '$.content_hash') IS 'text'
        AND json_type(receipt_json, '$.job_id') IS 'text'
        AND json_type(receipt_json, '$.status') IS 'text'
        AND json_type(receipt_json, '$.schema_version') IS 'integer'
        AND json_type(receipt_json, '$.reason') IS 'text'
        AND json_type(receipt_json, '$.accepted_at') IS 'text'
        AND (
            json_type(receipt_json, '$.job_version') IS 'null'
            OR json_type(receipt_json, '$.job_version') IS 'integer'
        )
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
        AND json_type(evidence_json, '$') IS 'object'
        AND json_type(evidence_json, '$.job_id') IS 'text'
        AND json_type(evidence_json, '$.sealed_path') IS 'text'
        AND json_type(evidence_json, '$.manifest_hash') IS 'text'
        AND json_type(evidence_json, '$.complete_result_hash') IS 'text'
        AND json_type(evidence_json, '$.file_identities') IS 'array'
        AND json_array_length(evidence_json, '$.file_identities') > 0
        AND json_type(evidence_json, '$.schema_version') IS 'integer'
        AND json_type(evidence_json, '$.bundle_device') IS 'integer'
        AND json_type(evidence_json, '$.bundle_inode') IS 'integer'
        AND json_type(evidence_json, '$.indexed_at') IS 'text'
    ),
    indexed_at TEXT NOT NULL CHECK (
        typeof(indexed_at) = 'text' AND length(indexed_at) > 0
    )
)
"""


def _dataset_snapshot_match_sql(
    commit_json_expression: str,
    job_spec_expression: str,
) -> str:
    return f"""
(
    (
        json_type(
            {commit_json_expression}, '$.commit.dataset_snapshot'
        ) IS 'null'
        AND json_type({job_spec_expression}, '$.dataset_snapshot') IS 'null'
    )
    OR (
        json_type(
            {commit_json_expression}, '$.commit.dataset_snapshot'
        ) IS 'object'
        AND json_type({job_spec_expression}, '$.dataset_snapshot') IS 'object'
        AND json_remove(
            json_extract(
                {commit_json_expression}, '$.commit.dataset_snapshot'
            ),
            '$.snapshot_id',
            '$.binding_hash',
            '$.audit_run_id'
        ) = '{{}}'
        AND json_remove(
            json_extract({job_spec_expression}, '$.dataset_snapshot'),
            '$.snapshot_id',
            '$.binding_hash',
            '$.audit_run_id'
        ) = '{{}}'
        AND json_type(
            {commit_json_expression}, '$.commit.dataset_snapshot.snapshot_id'
        ) IS 'text'
        AND json_type(
            {job_spec_expression}, '$.dataset_snapshot.snapshot_id'
        ) IS 'text'
        AND json_extract(
            {commit_json_expression}, '$.commit.dataset_snapshot.snapshot_id'
        ) = json_extract({job_spec_expression}, '$.dataset_snapshot.snapshot_id')
        AND json_type(
            {commit_json_expression}, '$.commit.dataset_snapshot.binding_hash'
        ) IS 'text'
        AND json_type(
            {job_spec_expression}, '$.dataset_snapshot.binding_hash'
        ) IS 'text'
        AND json_extract(
            {commit_json_expression}, '$.commit.dataset_snapshot.binding_hash'
        ) = json_extract({job_spec_expression}, '$.dataset_snapshot.binding_hash')
        AND json_type(
            {commit_json_expression}, '$.commit.dataset_snapshot.audit_run_id'
        ) IS json_type({job_spec_expression}, '$.dataset_snapshot.audit_run_id')
        AND json_extract(
            {commit_json_expression}, '$.commit.dataset_snapshot.audit_run_id'
        ) IS json_extract({job_spec_expression}, '$.dataset_snapshot.audit_run_id')
    )
)
"""


_NEW_COMMIT_DATASET_SNAPSHOT_MATCH = _dataset_snapshot_match_sql(
    "NEW.commit_json",
    "job.spec_json",
)
_STORED_COMMIT_DATASET_SNAPSHOT_MATCH = _dataset_snapshot_match_sql(
    "artifact_commit.commit_json",
    "NEW.spec_json",
)


_V5_JOB_RESULT_UPDATE_TRIGGER = f"""
CREATE TRIGGER IF NOT EXISTS trg_lab_job_complete_result_update
BEFORE UPDATE OF status, control_intent, version, recoverable,
                 scheduler_fencing_token, result_state,
                 result_contract_version, requires_complete_result ON lab_job
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
      (NEW.result_state = 'pending' AND (
        NEW.status = 'succeeded'
        OR (
            OLD.requires_complete_result = 1
            AND OLD.result_state = 'ready'
            AND NEW.status NOT IN ('failed', 'cancelled')
        )
        OR EXISTS (
            SELECT 1 FROM lab_job_result_artifact artifact
            WHERE artifact.job_id = NEW.job_id
        )
        OR (
            NEW.status = 'running'
            AND NEW.result_contract_version IS '{COMPLETE_RESULT_CONTRACT_VERSION}'
            AND EXISTS (
                SELECT 1 FROM lab_shard shard WHERE shard.job_id = NEW.job_id
            )
            AND NOT EXISTS (
                SELECT 1 FROM lab_shard shard
                WHERE shard.job_id = NEW.job_id AND shard.status <> 'succeeded'
            )
        )
      ))
      OR (NEW.result_state = 'ready' AND (
        NEW.status <> 'running'
        OR NEW.control_intent <> 'none'
        OR NEW.result_contract_version IS NOT '{COMPLETE_RESULT_CONTRACT_VERSION}'
        OR EXISTS (
            SELECT 1 FROM lab_job_result_artifact artifact
            WHERE artifact.job_id = NEW.job_id
        )
        OR NOT EXISTS (
            SELECT 1 FROM lab_shard shard WHERE shard.job_id = NEW.job_id
        )
        OR EXISTS (
            SELECT 1 FROM lab_shard shard
            WHERE shard.job_id = NEW.job_id AND shard.status <> 'succeeded'
        )
      ))
      OR (NEW.result_state = 'sealed' AND (
        OLD.requires_complete_result <> 1
        OR OLD.status <> 'running'
        OR OLD.result_state <> 'ready'
        OR OLD.control_intent <> 'none'
        OR OLD.result_contract_version IS NOT '{COMPLETE_RESULT_CONTRACT_VERSION}'
        OR NEW.status <> 'succeeded'
        OR NEW.control_intent <> 'none'
        OR NEW.result_contract_version IS NOT '{COMPLETE_RESULT_CONTRACT_VERSION}'
        OR NEW.version <> OLD.version + 1
        OR NOT EXISTS (
            SELECT 1 FROM lab_shard shard WHERE shard.job_id = NEW.job_id
        )
        OR EXISTS (
            SELECT 1 FROM lab_shard shard
            WHERE shard.job_id = NEW.job_id AND shard.status <> 'succeeded'
        )
        OR NOT EXISTS (
            SELECT 1
            FROM lab_job_result_artifact artifact
            JOIN lab_artifact_commit artifact_commit
              ON artifact_commit.request_id = artifact.commit_request_id
            WHERE artifact.job_id = NEW.job_id
              AND artifact_commit.job_id = NEW.job_id
              AND artifact_commit.status = 'accepted'
              AND artifact_commit.reason = 'artifact_committed'
              AND artifact_commit.receipt_job_version = NEW.version
              AND json_extract(
                    artifact_commit.commit_json, '$.commit.spec_hash'
                  ) = NEW.spec_hash
              AND json_extract(
                    artifact_commit.commit_json, '$.commit.code_sha'
                  ) = json_extract(NEW.spec_json, '$.code_sha')
              AND {_STORED_COMMIT_DATASET_SNAPSHOT_MATCH}
              AND json_extract(
                    artifact_commit.commit_json,
                    '$.commit.result_contract_version'
                  ) = NEW.result_contract_version
              AND json_extract(
                    artifact_commit.commit_json,
                    '$.commit.manifest_hash'
                  ) = artifact.manifest_hash
              AND json_extract(
                    artifact_commit.commit_json,
                    '$.commit.complete_result_hash'
                  ) = artifact.complete_result_hash
              AND json_extract(
                    artifact_commit.commit_json,
                    '$.commit.sealed_path'
                  ) = artifact.sealed_path
              AND NOT EXISTS (
                  SELECT 1 FROM lab_shard shard
                  WHERE shard.job_id = NEW.job_id
                    AND (
                      shard.plan_hash <> json_extract(
                          artifact_commit.commit_json, '$.commit.plan_hash'
                      )
                      OR shard.adapter_id <> json_extract(
                          artifact_commit.commit_json, '$.commit.adapter_id'
                      )
                      OR shard.adapter_version <> json_extract(
                          artifact_commit.commit_json, '$.commit.adapter_version'
                      )
                    )
              )
        )
        OR {_ARTIFACT_SUCCESS_AUTH_FUNCTION}(
            NEW.job_id,
            (SELECT commit_request_id FROM lab_job_result_artifact
             WHERE job_id = NEW.job_id),
            (SELECT evidence_json FROM lab_job_result_artifact
             WHERE job_id = NEW.job_id),
            OLD.version,
            NEW.version
        ) <> 1
      ))
    )
 )
 OR (
    OLD.status IN ('succeeded', 'cancelled')
    AND NEW.status <> OLD.status
 )
 OR (
    OLD.status = 'failed'
    AND NEW.status NOT IN ('failed', 'queued')
 )
 OR (
    OLD.status = 'failed'
    AND NEW.status = 'queued'
    AND (
      {_RETRY_AUTH_FUNCTION}(NEW.job_id, OLD.version, NEW.version) <> 1
      OR OLD.recoverable <> 1
      OR NEW.control_intent <> 'none'
      OR NEW.version <> OLD.version + 1
      OR NEW.recoverable <> 0
      OR NEW.scheduler_fencing_token IS NOT NULL
      OR NEW.result_state <> 'pending'
      OR NEW.requires_complete_result <> OLD.requires_complete_result
      OR NEW.result_contract_version IS NOT OLD.result_contract_version
    )
 )
BEGIN
    SELECT RAISE(ABORT, 'lab job result transition is not authorized or consistent');
END
"""

_V5_JOB_RESULT_INSERT_TRIGGER = f"""
CREATE TRIGGER IF NOT EXISTS trg_lab_job_complete_result_insert
BEFORE INSERT ON lab_job
WHEN {_SUBMIT_AUTH_FUNCTION}(NEW.job_id, NEW.spec_json) <> 1
 OR NEW.status <> 'queued'
 OR NEW.control_intent <> 'none'
 OR NEW.version <> 0
 OR NEW.attempt_count <> 0
 OR NEW.recoverable <> 0
 OR NEW.scheduler_fencing_token IS NOT NULL
 OR NEW.result_contract_version IS NOT NULL
 OR NEW.result_state <> 'pending'
 OR NEW.requires_complete_result <> 1
 OR NEW.created_at <> NEW.updated_at
BEGIN
    SELECT RAISE(ABORT, 'lab job submit insert is not authorized');
END
"""

_V5_JOB_EXISTING_KEY_NO_INSERT_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_job_existing_key_no_insert
BEFORE INSERT ON lab_job
WHEN EXISTS (
    SELECT 1 FROM lab_job existing WHERE existing.job_id = NEW.job_id
)
BEGIN
    SELECT RAISE(ABORT, 'existing job key cannot be inserted or replaced');
END
"""

_V5_JOB_ID_IMMUTABLE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_job_id_immutable
BEFORE UPDATE OF job_id ON lab_job
WHEN NEW.job_id IS NOT OLD.job_id
BEGIN
    SELECT RAISE(ABORT, 'lab job_id is immutable');
END
"""

# A ready row has no in-place lifecycle updates. Cancellation/deadline handling
# uses the narrow terminal capability below; successful completion uses only the
# artifact capability and may change the fence as part of that same transaction.
_V5_COMPLETE_RESULT_READY_JOB_UPDATE_TRIGGER = f"""
CREATE TRIGGER IF NOT EXISTS trg_lab_complete_result_ready_job_update
BEFORE UPDATE ON lab_job
WHEN OLD.requires_complete_result = 1
 AND OLD.result_state = 'ready'
 AND NOT (
    NEW.job_id IS OLD.job_id
    AND NEW.spec_json IS OLD.spec_json
    AND NEW.spec_hash IS OLD.spec_hash
    AND NEW.job_type IS OLD.job_type
    AND NEW.resource_class IS OLD.resource_class
    AND NEW.deadline IS OLD.deadline
    AND NEW.attempt_count IS OLD.attempt_count
    AND NEW.max_attempts IS OLD.max_attempts
    AND NEW.created_at IS OLD.created_at
    AND NEW.result_contract_version IS OLD.result_contract_version
    AND NEW.requires_complete_result IS OLD.requires_complete_result
    AND (
      (
        NEW.status = 'succeeded'
        AND NEW.control_intent = 'none'
        AND NEW.version = OLD.version + 1
        AND NEW.recoverable IS OLD.recoverable
        AND typeof(NEW.scheduler_fencing_token) = 'integer'
        AND NEW.scheduler_fencing_token >= 1
        AND NEW.result_state = 'sealed'
        AND {_ARTIFACT_SUCCESS_AUTH_FUNCTION}(
            NEW.job_id,
            (SELECT commit_request_id FROM lab_job_result_artifact
             WHERE job_id = NEW.job_id),
            (SELECT evidence_json FROM lab_job_result_artifact
             WHERE job_id = NEW.job_id),
            OLD.version,
            NEW.version
        ) = 1
      )
      OR (
        NEW.status IN ('failed', 'cancelled')
        AND NEW.control_intent = 'none'
        AND NEW.version = OLD.version + 1
        AND NEW.result_state = 'pending'
        AND {_READY_TERMINAL_AUTH_FUNCTION}(
            NEW.job_id,
            NEW.status,
            OLD.version,
            NEW.version,
            NEW.recoverable,
            NEW.scheduler_fencing_token
        ) = 1
      )
    )
 )
BEGIN
    SELECT RAISE(ABORT, 'complete ready job ledger row is immutable');
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

_V5_COMPLETE_RESULT_JOB_NO_DELETE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_complete_result_job_no_delete
BEFORE DELETE ON lab_job
WHEN OLD.requires_complete_result = 1
BEGIN
    SELECT RAISE(ABORT, 'complete result job ledger row cannot be deleted');
END
"""

_V5_COMPLETE_RESULT_SEALED_JOB_NO_UPDATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_complete_result_sealed_job_no_update
BEFORE UPDATE ON lab_job
WHEN OLD.requires_complete_result = 1 AND OLD.result_state = 'sealed'
BEGIN
    SELECT RAISE(ABORT, 'complete sealed job ledger row is immutable');
END
"""

_V5_ARTIFACT_COMMIT_INSERT_TRIGGER = f"""
CREATE TRIGGER IF NOT EXISTS trg_lab_artifact_commit_insert
BEFORE INSERT ON lab_artifact_commit
WHEN {_ARTIFACT_COMMIT_AUTH_FUNCTION}(
        NEW.request_id, NEW.commit_json, NEW.receipt_json
     ) <> 1
 OR json_extract(NEW.commit_json, '$.request_id') <> NEW.request_id
 OR json_extract(NEW.commit_json, '$.content_hash') <> NEW.content_hash
 OR json_extract(NEW.commit_json, '$.commit.job_id') <> NEW.job_id
 OR json_extract(NEW.receipt_json, '$.request_id') <> NEW.request_id
 OR json_extract(NEW.receipt_json, '$.content_hash') <> NEW.content_hash
 OR json_extract(NEW.receipt_json, '$.job_id') <> NEW.job_id
 OR json_extract(NEW.receipt_json, '$.status') <> NEW.status
 OR json_extract(NEW.receipt_json, '$.reason') <> NEW.reason
 OR json_extract(NEW.receipt_json, '$.job_version') IS NOT NEW.receipt_job_version
 OR (
    NEW.status = 'accepted'
    AND NOT (
      (
        NEW.reason = 'artifact_committed'
        AND EXISTS (
            SELECT 1 FROM lab_job job
            WHERE job.job_id = NEW.job_id
              AND NEW.receipt_job_version = job.version + 1
              AND job.requires_complete_result = 1
              AND job.status = 'running'
              AND job.result_state = 'ready'
              AND job.control_intent = 'none'
              AND job.result_contract_version IS '{COMPLETE_RESULT_CONTRACT_VERSION}'
              AND json_extract(
                    NEW.commit_json, '$.commit.spec_hash'
                  ) = job.spec_hash
              AND json_extract(
                    NEW.commit_json, '$.commit.code_sha'
                  ) = json_extract(job.spec_json, '$.code_sha')
              AND {_NEW_COMMIT_DATASET_SNAPSHOT_MATCH}
              AND json_extract(
                    NEW.commit_json, '$.commit.result_contract_version'
                  ) = job.result_contract_version
        )
        AND EXISTS (
            SELECT 1 FROM lab_shard shard WHERE shard.job_id = NEW.job_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM lab_shard shard
            WHERE shard.job_id = NEW.job_id
              AND (
                shard.status <> 'succeeded'
                OR shard.plan_hash <> json_extract(
                    NEW.commit_json, '$.commit.plan_hash'
                )
                OR shard.adapter_id <> json_extract(
                    NEW.commit_json, '$.commit.adapter_id'
                )
                OR shard.adapter_version <> json_extract(
                    NEW.commit_json, '$.commit.adapter_version'
                )
              )
        )
      )
      OR (
        NEW.reason = 'artifact_already_committed'
        AND EXISTS (
            SELECT 1
            FROM lab_job job
            JOIN lab_job_result_artifact artifact
              ON artifact.job_id = job.job_id
            WHERE job.job_id = NEW.job_id
              AND NEW.receipt_job_version = job.version
              AND job.requires_complete_result = 1
              AND job.status = 'succeeded'
              AND job.result_state = 'sealed'
              AND job.result_contract_version IS '{COMPLETE_RESULT_CONTRACT_VERSION}'
              AND json_extract(
                    NEW.commit_json, '$.commit.spec_hash'
                  ) = job.spec_hash
              AND json_extract(
                    NEW.commit_json, '$.commit.code_sha'
                  ) = json_extract(job.spec_json, '$.code_sha')
              AND {_NEW_COMMIT_DATASET_SNAPSHOT_MATCH}
              AND json_extract(
                    NEW.commit_json, '$.commit.result_contract_version'
                  ) = job.result_contract_version
              AND artifact.manifest_hash = json_extract(
                    NEW.commit_json, '$.commit.manifest_hash'
                  )
              AND artifact.complete_result_hash = json_extract(
                    NEW.commit_json, '$.commit.complete_result_hash'
                  )
              AND artifact.sealed_path = json_extract(
                    NEW.commit_json, '$.commit.sealed_path'
                  )
              AND EXISTS (
                  SELECT 1 FROM lab_shard shard WHERE shard.job_id = NEW.job_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM lab_shard shard
                  WHERE shard.job_id = NEW.job_id
                    AND (
                      shard.status <> 'succeeded'
                      OR shard.plan_hash <> json_extract(
                          NEW.commit_json, '$.commit.plan_hash'
                      )
                      OR shard.adapter_id <> json_extract(
                          NEW.commit_json, '$.commit.adapter_id'
                      )
                      OR shard.adapter_version <> json_extract(
                          NEW.commit_json, '$.commit.adapter_version'
                      )
                    )
              )
        )
      )
    )
 )
BEGIN
    SELECT RAISE(ABORT, 'artifact commit insert is not authorized or consistent');
END
"""

_V5_RESULT_ARTIFACT_INSERT_TRIGGER = f"""
CREATE TRIGGER IF NOT EXISTS trg_lab_result_artifact_insert
BEFORE INSERT ON lab_job_result_artifact
WHEN {_ARTIFACT_INDEX_AUTH_FUNCTION}(
        NEW.job_id, NEW.commit_request_id, NEW.evidence_json
     ) <> 1
 OR json_extract(NEW.evidence_json, '$.job_id') <> NEW.job_id
 OR json_extract(NEW.evidence_json, '$.sealed_path') <> NEW.sealed_path
 OR json_extract(NEW.evidence_json, '$.manifest_hash') <> NEW.manifest_hash
 OR json_extract(
        NEW.evidence_json, '$.complete_result_hash'
    ) <> NEW.complete_result_hash
 OR json_extract(NEW.evidence_json, '$.bundle_device') <> NEW.bundle_device
 OR json_extract(NEW.evidence_json, '$.bundle_inode') <> NEW.bundle_inode
 OR NOT EXISTS (
    SELECT 1
    FROM lab_artifact_commit artifact_commit
    JOIN lab_job job ON job.job_id = artifact_commit.job_id
    WHERE artifact_commit.request_id = NEW.commit_request_id
      AND artifact_commit.job_id = NEW.job_id
      AND artifact_commit.status = 'accepted'
      AND artifact_commit.reason = 'artifact_committed'
      AND artifact_commit.receipt_job_version = job.version + 1
      AND job.requires_complete_result = 1
      AND job.status = 'running'
      AND job.result_state = 'ready'
      AND job.control_intent = 'none'
      AND job.result_contract_version IS '{COMPLETE_RESULT_CONTRACT_VERSION}'
      AND json_extract(
            artifact_commit.commit_json, '$.commit.manifest_hash'
          ) = NEW.manifest_hash
      AND json_extract(
            artifact_commit.commit_json, '$.commit.complete_result_hash'
          ) = NEW.complete_result_hash
      AND json_extract(
            artifact_commit.commit_json, '$.commit.sealed_path'
          ) = NEW.sealed_path
      AND EXISTS (
          SELECT 1 FROM lab_shard shard WHERE shard.job_id = NEW.job_id
      )
      AND NOT EXISTS (
          SELECT 1 FROM lab_shard shard
          WHERE shard.job_id = NEW.job_id
            AND (
              shard.status <> 'succeeded'
              OR shard.plan_hash <> json_extract(
                  artifact_commit.commit_json, '$.commit.plan_hash'
              )
              OR shard.adapter_id <> json_extract(
                  artifact_commit.commit_json, '$.commit.adapter_id'
              )
              OR shard.adapter_version <> json_extract(
                  artifact_commit.commit_json, '$.commit.adapter_version'
              )
            )
      )
 )
BEGIN
    SELECT RAISE(ABORT, 'result artifact insert is not authorized or consistent');
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

_V5_COMPLETE_RESULT_SHARD_NO_INSERT_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_complete_result_shard_no_insert
BEFORE INSERT ON lab_shard
WHEN NOT EXISTS (
        SELECT 1 FROM lab_job job
        WHERE job.job_id = NEW.job_id
    )
    OR EXISTS (
        SELECT 1 FROM lab_job job
        WHERE job.job_id = NEW.job_id
          AND job.requires_complete_result = 1
          AND job.result_state IN ('ready', 'sealed')
    )
BEGIN
    SELECT RAISE(ABORT, 'lab shard parent is missing or immutable');
END
"""

_V5_COMPLETE_RESULT_SHARD_NO_UPDATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_complete_result_shard_no_update
BEFORE UPDATE ON lab_shard
WHEN NEW.job_id IS NOT OLD.job_id
    OR NOT EXISTS (
        SELECT 1 FROM lab_job job
        WHERE job.job_id = NEW.job_id
    )
    OR EXISTS (
        SELECT 1 FROM lab_job job
        WHERE job.job_id = OLD.job_id
          AND job.requires_complete_result = 1
          AND job.result_state IN ('ready', 'sealed')
    )
    OR EXISTS (
        SELECT 1 FROM lab_job job
        WHERE job.job_id = NEW.job_id
          AND job.requires_complete_result = 1
          AND job.result_state IN ('ready', 'sealed')
    )
BEGIN
    SELECT RAISE(ABORT, 'lab shard ownership or complete result set is immutable');
END
"""

_V5_COMPLETE_RESULT_SHARD_NO_DELETE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_complete_result_shard_no_delete
BEFORE DELETE ON lab_shard
WHEN EXISTS (
    SELECT 1 FROM lab_job job
    WHERE job.job_id = OLD.job_id
      AND job.requires_complete_result = 1
      AND job.result_state IN ('ready', 'sealed')
)
BEGIN
    SELECT RAISE(ABORT, 'complete result shard set is immutable');
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

_V5_EXPECTED_TRIGGER_SQL = {
    "trg_lab_complete_result_job_no_delete": _V5_COMPLETE_RESULT_JOB_NO_DELETE_TRIGGER,
    "trg_lab_job_existing_key_no_insert": _V5_JOB_EXISTING_KEY_NO_INSERT_TRIGGER,
    "trg_lab_job_id_immutable": _V5_JOB_ID_IMMUTABLE_TRIGGER,
    "trg_lab_complete_result_ready_job_update": _V5_COMPLETE_RESULT_READY_JOB_UPDATE_TRIGGER,
    "trg_lab_complete_result_sealed_job_no_update": (
        _V5_COMPLETE_RESULT_SEALED_JOB_NO_UPDATE_TRIGGER
    ),
    "trg_lab_job_complete_result_insert": _V5_JOB_RESULT_INSERT_TRIGGER,
    "trg_lab_job_complete_result_update": _V5_JOB_RESULT_UPDATE_TRIGGER,
    "trg_lab_job_complete_result_marker_immutable": _V5_JOB_RESULT_MARKER_IMMUTABLE_TRIGGER,
    "trg_lab_artifact_commit_insert": _V5_ARTIFACT_COMMIT_INSERT_TRIGGER,
    "trg_lab_result_artifact_insert": _V5_RESULT_ARTIFACT_INSERT_TRIGGER,
    "trg_lab_result_artifact_no_update": _V5_RESULT_ARTIFACT_NO_UPDATE_TRIGGER,
    "trg_lab_result_artifact_no_delete": _V5_RESULT_ARTIFACT_NO_DELETE_TRIGGER,
    "trg_lab_complete_result_shard_no_insert": _V5_COMPLETE_RESULT_SHARD_NO_INSERT_TRIGGER,
    "trg_lab_complete_result_shard_no_update": _V5_COMPLETE_RESULT_SHARD_NO_UPDATE_TRIGGER,
    "trg_lab_complete_result_shard_no_delete": _V5_COMPLETE_RESULT_SHARD_NO_DELETE_TRIGGER,
    "trg_lab_artifact_commit_no_update": _V5_ARTIFACT_COMMIT_NO_UPDATE_TRIGGER,
    "trg_lab_artifact_commit_no_delete": _V5_ARTIFACT_COMMIT_NO_DELETE_TRIGGER,
}

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
    _V5_COMPLETE_RESULT_JOB_NO_DELETE_TRIGGER,
    _V5_COMPLETE_RESULT_READY_JOB_UPDATE_TRIGGER,
    _V5_COMPLETE_RESULT_SEALED_JOB_NO_UPDATE_TRIGGER,
    _V5_ARTIFACT_COMMIT_INSERT_TRIGGER,
    _V5_RESULT_ARTIFACT_INSERT_TRIGGER,
    _V5_RESULT_ARTIFACT_NO_UPDATE_TRIGGER,
    _V5_RESULT_ARTIFACT_NO_DELETE_TRIGGER,
    _V5_COMPLETE_RESULT_SHARD_NO_INSERT_TRIGGER,
    _V5_COMPLETE_RESULT_SHARD_NO_UPDATE_TRIGGER,
    _V5_COMPLETE_RESULT_SHARD_NO_DELETE_TRIGGER,
    _V5_ARTIFACT_COMMIT_NO_UPDATE_TRIGGER,
    _V5_ARTIFACT_COMMIT_NO_DELETE_TRIGGER,
    _V5_JOB_EXISTING_KEY_NO_INSERT_TRIGGER,
    _V5_JOB_ID_IMMUTABLE_TRIGGER,
)
