"""SQLite single-writer ledger for durable Strategy Lab jobs."""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import os
import re
import sqlite3
from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from threading import Lock
from types import TracebackType
from typing import TYPE_CHECKING, Literal, Protocol, Self
from urllib.parse import quote
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5
from weakref import ReferenceType, ref

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
from rquant.current_claim_authority import PersistentCurrentClaimAuthority
from rquant.lab_artifact_protocol import (
    LabArtifactCommitEnvelope,
    LabArtifactCommitReceipt,
    LabFinalizerAuthorityClaims,
    LabFinalizerAuthorityShardEvidence,
    LabFinalizerAuthorityVerificationKeyProvider,
    authenticate_artifact_commit_identity,
)
from rquant.lab_artifacts import LabArtifactIndexEvidence
from rquant.lab_claim_finalizer_trust import (
    LabClaimFinalizerPublicationAttestation,
    LabClaimFinalizerTrustCertificate,
    LabClaimFinalizerTrustError,
    LabClaimFinalizerTrustVerifier,
    build_lab_claim_finalizer_publication_attestation,
    require_lab_claim_finalizer_publication_attestation,
)
from rquant.lab_claim_publication import (
    V2_UNASSIGNED_WORKER_ID,
    ClaimPublicationAuditAction,
    ClaimPublicationStatus,
    HeldDraft,
    LabClaimPublicationAuditRecord,
    LabClaimPublicationFinalizerAuthority,
    LabClaimPublicationFinalizerRootKey,
    LabClaimPublicationIdentity,
    LabClaimPublicationMutation,
    LabClaimPublicationObservationDegradation,
    LabClaimPublicationRecord,
    LabClaimPublicationRolloutEvidence,
    LabClaimPublicationRolloutEvidenceOutboxItem,
    LabClaimSpoolReceiptVerifier,
    PublishReceipt,
    QueueBinding,
    ReadyBinding,
    require_v2_spool_receipt_provenance,
    source_stage_store_authority_from_canonical_bytes,
)
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
    LabShardClaimV2,
    LabShardDefinition,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabShardTelemetry,
    LabShardWorkPlan,
    LabWorkerReport,
    LabWorkerStopped,
    StrategyShardPayloadV2,
    parse_strategy_shard_payload,
    validate_strategy_shard_payload_utf8,
)
from rquant.lab_source_stage import (
    LabSourceStageBinding,
    LabSourceStageState,
    LabSourceStageStore,
    LabSourceStageStoreAuthority,
)
from rquant.research_run_spec import (
    ResearchJobType,
    ResearchRunSpec,
    ResourceClass,
)
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2JobOutcomeStatus,
    canonical_job_model_bytes,
)
from rquant.source_operation_contracts import (
    CurrentClaimAuthorityProtocol,
    CurrentClaimConsumptionBindingV2,
    CurrentClaimConsumptionV2,
    SourceAttemptBindingV2,
    SourceOperationContractError,
    SourceUsePlanV2,
    require_current_claim_consumption_v2,
    require_source_use_plan_v2,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_json_loads,
    strict_model_validate_canonical_json,
    strict_model_validate_json,
)

if TYPE_CHECKING:
    from rquant.lab_artifacts import LabVerifiedSealedBinding


class LabSqliteIdentityAuthority(Protocol):
    path: Path
    database_generation: tuple[int, int]

    def assert_current(self) -> None: ...

    def open_verified_connection(
        self,
        opener: Callable[[Path], sqlite3.Connection],
    ) -> sqlite3.Connection: ...


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


class LabIntegrityDegradedError(RuntimeError):
    """A bounded or full ledger audit failed, so a daemon must fail closed."""


class CancelConfirmationRequiredError(RuntimeError):
    """Cancellation must preserve intent until a worker claim is invalidated."""


class LabDatabaseIdentityError(RuntimeError):
    """The configured SQLite file is not this ledger at a supported version."""


class FormalSubmissionAuthorityError(ValueError):
    """A formal submission lacks exact, independently resolved ownership evidence."""


class ShardPlanConflictError(RuntimeError):
    """A job was already bound to a different immutable shard plan."""


class ArtifactCommitDeadlineExpiredError(RuntimeError):
    """A staged artifact success crossed its job deadline before commit."""


class ClaimPublicationConflictError(RuntimeError):
    """A claim-publication attempt conflicts with durable attempt identity or bytes."""


class InvalidClaimPublicationTransitionError(RuntimeError):
    """A claim-publication transition violates the frozen publication state matrix."""


_APPLICATION_ID = 0x52514A42
_LEGACY_SCHEMA_VERSION = 1
_V2_SCHEMA_VERSION = 2
_V3_SCHEMA_VERSION = 3
_V4_SCHEMA_VERSION = 4
_V5_SCHEMA_VERSION = 5
_V6_SCHEMA_VERSION = 6
_V7_SCHEMA_VERSION = 7
_V8_SCHEMA_VERSION = 8
_V9_SCHEMA_VERSION = 9
_V10_SCHEMA_VERSION = 10
_V11_SCHEMA_VERSION = 11
_V12_SCHEMA_VERSION = 12
_V13_SCHEMA_VERSION = 13
_V14_SCHEMA_VERSION = 14
_V15_SCHEMA_VERSION = 15
_PREVIOUS_SCHEMA_VERSION = _V15_SCHEMA_VERSION
_SCHEMA_VERSION = 16
LAB_JOBS_SCHEMA_GENERATION = _SCHEMA_VERSION
RESULT_CONTRACT_VERSION = "p1.4a-telemetry-v1"
COMPLETE_RESULT_CONTRACT_VERSION = "p1.4b-complete-result-v1"
_SUBMIT_AUTH_FUNCTION = "rquant_lab_submit_authorized"
_RETRY_AUTH_FUNCTION = "rquant_lab_retry_authorized"
_READY_TERMINAL_AUTH_FUNCTION = "rquant_lab_ready_terminal_authorized"
_ARTIFACT_COMMIT_AUTH_FUNCTION = "rquant_lab_artifact_commit_authorized"
_ARTIFACT_INDEX_AUTH_FUNCTION = "rquant_lab_artifact_index_authorized"
_ARTIFACT_SUCCESS_AUTH_FUNCTION = "rquant_lab_artifact_success_authorized"
_CLAIM_PUBLICATION_AUTH_FUNCTION = "rquant_lab_claim_publication_authorized"
_CLAIM_PUBLICATION_AUDIT_AUTH_FUNCTION = "rquant_lab_claim_publication_audit_authorized"
_LEDGER_CHAIN_STEP_FUNCTION = "rquant_lab_ledger_chain_step"
_ROLLOUT_EVIDENCE_REASON_CODE = "rollout_evidence_pending"
_ROLLOUT_EVIDENCE_REASON_HASH = hashlib.sha256(
    _ROLLOUT_EVIDENCE_REASON_CODE.encode("ascii")
).hexdigest()
_ROLLOUT_EVIDENCE_INITIAL_ERROR_CLASS = "RolloutEvidencePending"
LAB_ETA_COMPLETED_LIMIT_MAX = 256
MAX_JOB_SHARDS = 128
STALE_RECOVERY_BATCH_SIZE = 32
PRECLAIM_CANDIDATE_BATCH_SIZE = 32
PRECLAIM_FAIR_SCAN_INTERVAL = 4
IDLE_CONTROL_AFTER_BATCH_SIZE = STALE_RECOVERY_BATCH_SIZE // 2
IDLE_CONTROL_BEFORE_BATCH_SIZE = STALE_RECOVERY_BATCH_SIZE - IDLE_CONTROL_AFTER_BATCH_SIZE
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
        "_claim_publication",
        "_claim_publication_audit",
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
        self._claim_publication: tuple[int, str, str, int, str] | None = None
        self._claim_publication_audit: tuple[int, str, str, str, str] | None = None

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
                self._claim_publication,
                self._claim_publication_audit,
            )
        ):
            self._epoch += 1
        self._submit = None
        self._retry = None
        self._ready_terminal = None
        self._artifact_commit = None
        self._artifact_index = None
        self._artifact_success = None
        self._claim_publication = None
        self._claim_publication_audit = None

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

    @contextmanager
    def authorize_claim_publication(
        self,
        record: LabClaimPublicationRecord,
    ) -> Iterator[None]:
        if self._claim_publication is not None:
            raise RuntimeError("claim publication SQL authorization is already active")
        epoch = self._require_transaction()
        self._claim_publication = (
            epoch,
            str(record.identity.attempt_id),
            record.status.value,
            record.version,
            record.record_commitment,
        )
        try:
            yield
        finally:
            if self._claim_publication is not None and self._claim_publication[0] == epoch:
                self._claim_publication = None

    @contextmanager
    def authorize_claim_publication_audit(
        self,
        audit: LabClaimPublicationAuditRecord,
    ) -> Iterator[None]:
        if self._claim_publication_audit is not None:
            raise RuntimeError("claim publication audit SQL authorization is already active")
        epoch = self._require_transaction()
        self._claim_publication_audit = (
            epoch,
            str(audit.audit_ref),
            str(audit.attempt_id),
            audit.action.value,
            audit.audit_hash,
        )
        try:
            yield
        finally:
            if (
                self._claim_publication_audit is not None
                and self._claim_publication_audit[0] == epoch
            ):
                self._claim_publication_audit = None

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

    def claim_publication_authorized(
        self,
        attempt_id: object,
        status: object,
        version: object,
        record_commitment: object,
    ) -> int:
        grant = self._claim_publication
        return int(
            grant is not None
            and self._is_current_transaction(grant[0])
            and grant[1:]
            == (
                str(attempt_id),
                str(status),
                version,
                str(record_commitment),
            )
        )

    def claim_publication_audit_authorized(
        self,
        audit_ref: object,
        attempt_id: object,
        action: object,
        audit_hash: object,
    ) -> int:
        grant = self._claim_publication_audit
        return int(
            grant is not None
            and self._is_current_transaction(grant[0])
            and grant[1:]
            == (
                str(audit_ref),
                str(attempt_id),
                str(action),
                str(audit_hash),
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
        connection = self.connection
        if isinstance(connection, _LabJobStoreConnection):
            connection._assert_identity_current()
        try:
            result = super().execute(sql, parameters)
        except sqlite3.Error:
            self._expire_authorization()
            raise
        if isinstance(connection, _LabJobStoreConnection):
            connection._assert_identity_current()
        return result

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Iterable[_SqlParameters],
        /,
    ) -> sqlite3.Cursor:
        connection = self.connection
        if isinstance(connection, _LabJobStoreConnection):
            connection._assert_identity_current()
        try:
            result = super().executemany(sql, seq_of_parameters)
        except sqlite3.Error:
            self._expire_authorization()
            raise
        if isinstance(connection, _LabJobStoreConnection):
            connection._assert_identity_current()
        return result

    def executescript(self, sql_script: str, /) -> sqlite3.Cursor:
        connection = self.connection
        if isinstance(connection, _LabJobStoreConnection):
            connection._assert_identity_current()
        try:
            result = super().executescript(sql_script)
        except sqlite3.Error:
            self._expire_authorization()
            raise
        if isinstance(connection, _LabJobStoreConnection):
            connection._assert_identity_current()
        return result


class _LabJobStoreConnection(sqlite3.Connection):
    write_authorization: _LabWriteAuthorization
    identity_authority: LabSqliteIdentityAuthority | None = None
    _identity_failed: bool = False

    def _assert_identity_current(self) -> None:
        authority = self.identity_authority
        if authority is None:
            return
        try:
            authority.assert_current()
        except BaseException:
            self._identity_failed = True
            raise

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
        self._assert_identity_current()
        self._expire_write_authorization()
        super().commit()
        self._assert_identity_current()

    def rollback(self) -> None:
        self._expire_write_authorization()
        super().rollback()

    def close(self) -> None:
        identity_error: BaseException | None = None
        if not self._identity_failed:
            try:
                self._assert_identity_current()
            except BaseException as exc:
                identity_error = exc
        self._expire_write_authorization()
        self.set_trace_callback(None)
        super().close()
        if identity_error is not None:
            raise identity_error

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()
        return False


class _LabJobReaderCursor(sqlite3.Cursor):
    def execute(
        self,
        sql: str,
        parameters: _SqlParameters = (),
        /,
    ) -> sqlite3.Cursor:
        connection = self.connection
        if isinstance(connection, _LabJobReaderConnection):
            connection._assert_identity_current()
        result = super().execute(sql, parameters)
        if isinstance(connection, _LabJobReaderConnection):
            connection._assert_identity_current()
        return result


class _LabJobReaderConnection(sqlite3.Connection):
    identity_authority: LabSqliteIdentityAuthority | None = None
    database_generation: tuple[int, int] | None = None
    _identity_failed: bool = False

    def _assert_identity_current(self) -> None:
        authority = self.identity_authority
        if authority is None:
            return
        try:
            authority.assert_current()
        except BaseException:
            self._identity_failed = True
            raise

    def cursor(
        self,
        factory: type[sqlite3.Cursor] | None = None,
    ) -> sqlite3.Cursor:
        if factory is not None and factory is not _LabJobReaderCursor:
            raise TypeError("lab job reader cursor factory must preserve identity fencing")
        return super().cursor(_LabJobReaderCursor)

    def execute(
        self,
        sql: str,
        parameters: _SqlParameters = (),
        /,
    ) -> sqlite3.Cursor:
        return self.cursor().execute(sql, parameters)

    def commit(self) -> None:
        self._assert_identity_current()
        super().commit()
        self._assert_identity_current()

    def rollback(self) -> None:
        super().rollback()

    def close(self) -> None:
        identity_error: BaseException | None = None
        if not self._identity_failed:
            try:
                self._assert_identity_current()
            except BaseException as exc:
                identity_error = exc
        super().close()
        if identity_error is not None:
            raise identity_error

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()
        return False


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


class LabPreclaimRejection(LabRecordModel):
    job_id: UUID
    shard_id: UUID
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reason: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")


class LabClaimSelection(LabRecordModel):
    claim: LabShardClaim | LabShardClaimV2 | None = None
    rejections: tuple[LabPreclaimRejection, ...] = ()


class CurrentSchedulerFenceReceipt(LabRecordModel):
    """Public, exact proof that one scheduler lease still owns one stage attempt."""

    receipt_version: Literal[1] = 1
    canonical_job_store_path: str = Field(min_length=1)
    database_generation: tuple[int, int]
    store_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    application_id: int = Field(ge=1)
    schema_version: int = Field(ge=1)
    implementation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    binding: LabSourceStageBinding
    owner_id: str = Field(min_length=1, max_length=200)
    scheduler_fencing_token: int = Field(ge=1)
    lease_id: int = Field(ge=1)
    lease_commitment: str = Field(pattern=r"^[0-9a-f]{64}$")
    issued_at: datetime
    expires_at: datetime
    row_commitment: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt_commitment: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_commitments(self) -> CurrentSchedulerFenceReceipt:
        issued = _utc(self.issued_at)
        expires = _utc(self.expires_at)
        if expires <= issued:
            raise ValueError("scheduler fence receipt timestamps are invalid")
        if not Path(self.canonical_job_store_path).is_absolute():
            raise ValueError("scheduler fence receipt path must be absolute")
        expected_row = _scheduler_fence_row_commitment(
            owner_id=self.owner_id,
            scheduler_fencing_token=self.scheduler_fencing_token,
            lease_id=self.lease_id,
            lease_commitment=self.lease_commitment,
            issued_at=issued,
            expires_at=expires,
        )
        if self.row_commitment != expected_row:
            raise ValueError("scheduler fence receipt row commitment is invalid")
        expected_receipt = _scheduler_fence_receipt_commitment(
            canonical_job_store_path=self.canonical_job_store_path,
            database_generation=self.database_generation,
            store_id=self.store_id,
            application_id=self.application_id,
            schema_version=self.schema_version,
            implementation_digest=self.implementation_digest,
            binding=self.binding,
            row_commitment=self.row_commitment,
        )
        if self.receipt_commitment != expected_receipt:
            raise ValueError("scheduler fence receipt commitment is invalid")
        object.__setattr__(self, "issued_at", issued)
        object.__setattr__(self, "expires_at", expires)
        return self


class JobStoreSchedulerFenceVerifier:
    """Verify and hold an exact JobStore scheduler lease across a stage commit."""

    def __init__(self, store: LabJobStore) -> None:
        if type(store) is not LabJobStore:
            raise TypeError("scheduler fence verifier requires an exact LabJobStore")
        self._store = store

    @contextmanager
    def hold_current(
        self,
        receipt: CurrentSchedulerFenceReceipt,
        *,
        binding: LabSourceStageBinding,
        now: datetime,
    ) -> Iterator[None]:
        current = _utc(now)
        connection = self._store._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            _validate_database_identity(connection, allow_unclaimed_empty=False)
            _validate_current_schema(connection)
            self._verify_in_connection(
                connection,
                receipt=receipt,
                binding=binding,
                now=current,
            )
            yield
            self._verify_in_connection(
                connection,
                receipt=receipt,
                binding=binding,
                now=current,
            )
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def verify_current(
        self,
        receipt: CurrentSchedulerFenceReceipt,
        *,
        binding: LabSourceStageBinding,
        now: datetime,
    ) -> None:
        with self.hold_current(receipt, binding=binding, now=now):
            return

    def _verify_in_connection(
        self,
        connection: sqlite3.Connection,
        *,
        receipt: CurrentSchedulerFenceReceipt,
        binding: LabSourceStageBinding,
        now: datetime,
    ) -> None:
        if binding != receipt.binding:
            raise SchedulerLeaseFencedError("scheduler fence receipt binding is not exact")
        authority = self._store._scheduler_fence_authority(connection)
        if (
            receipt.canonical_job_store_path != authority["canonical_job_store_path"]
            or receipt.database_generation != authority["database_generation"]
            or receipt.store_id != authority["store_id"]
            or receipt.application_id != authority["application_id"]
            or receipt.schema_version != authority["schema_version"]
            or receipt.implementation_digest != authority["implementation_digest"]
        ):
            raise SchedulerLeaseFencedError("scheduler fence receipt authority changed")
        row = connection.execute(
            "SELECT * FROM lab_lease WHERE lease_id = ?", (receipt.lease_id,)
        ).fetchone()
        if row is None:
            raise SchedulerLeaseFencedError("scheduler fence receipt lease is missing")
        commitment = _scheduler_fence_lease_commitment(row)
        active = connection.execute(
            "SELECT lease_id FROM lab_lease WHERE lease_name = ? AND released_at IS NULL "
            "ORDER BY fencing_token DESC LIMIT 1",
            (LabJobStore.LEASE_NAME,),
        ).fetchone()
        if (
            str(row["lease_name"]) != LabJobStore.LEASE_NAME
            or str(row["owner_id"]) != receipt.owner_id
            or _strict_sqlite_int(row["fencing_token"], field="lab_lease.fencing_token", minimum=1)
            != receipt.scheduler_fencing_token
            or commitment != receipt.lease_commitment
            or _load_time(str(row["acquired_at"])) != receipt.issued_at
            or _load_time(str(row["expires_at"])) != receipt.expires_at
            or row["released_at"] is not None
            or receipt.expires_at <= now
            or active is None
            or _strict_sqlite_int(active["lease_id"], field="lab_lease.lease_id", minimum=1)
            != receipt.lease_id
        ):
            raise SchedulerLeaseFencedError("scheduler fence receipt is stale")


_FROZEN_JOB_STORE_SCHEDULER_FENCE_HOLD_CURRENT = JobStoreSchedulerFenceVerifier.hold_current

_FROZEN_JOB_STORE_CURRENT_CLAIM_HOLD_CURRENT = PersistentCurrentClaimAuthority.hold_current
_FROZEN_JOB_STORE_CURRENT_CLAIM_HOLD_CURRENT_SOURCE_DIGEST = hashlib.sha256(
    inspect.getsource(_FROZEN_JOB_STORE_CURRENT_CLAIM_HOLD_CURRENT).encode("utf-8")
).hexdigest()


@contextmanager
def hold_trusted_current_claim(
    authority: CurrentClaimAuthorityProtocol,
    *,
    binding: CurrentClaimConsumptionBindingV2,
    now: datetime,
    _expected_authority_type: type[PersistentCurrentClaimAuthority] = (
        PersistentCurrentClaimAuthority
    ),
    _hold_current: Callable[..., object] = _FROZEN_JOB_STORE_CURRENT_CLAIM_HOLD_CURRENT,
    _source_digest: str = _FROZEN_JOB_STORE_CURRENT_CLAIM_HOLD_CURRENT_SOURCE_DIGEST,
) -> Iterator[CurrentClaimConsumptionV2]:
    """Freeze the only current-authority dispatch allowed around C/D CAS."""

    if type(authority) is not _expected_authority_type:
        raise ClaimPublicationConflictError("current_authority_guard_untrusted")
    # The call target is captured in defaults, not looked up through a mutable
    # module/class attribute. The comparisons turn common monkeypatch attempts
    # into a fail-closed integrity error rather than silently dispatching them.
    if (
        globals().get("_FROZEN_JOB_STORE_CURRENT_CLAIM_HOLD_CURRENT") is not _hold_current
        or PersistentCurrentClaimAuthority.hold_current is not _hold_current
    ):
        raise ClaimPublicationConflictError("current_authority_guard_dispatch_tampered")
    try:
        observed_source_digest = hashlib.sha256(
            inspect.getsource(_hold_current).encode("utf-8")
        ).hexdigest()
    except (OSError, TypeError):
        raise ClaimPublicationConflictError("current_authority_guard_dispatch_tampered") from None
    if observed_source_digest != _source_digest:
        raise ClaimPublicationConflictError("current_authority_guard_dispatch_tampered")
    with _hold_current(
        authority,
        binding=binding,
        now=now,
    ) as receipt:
        yield receipt


_TEST_ONLY_SCHEDULER_FENCE_HOLD_HOOK_LOCK = Lock()
_TEST_ONLY_SCHEDULER_FENCE_HOLD_HOOK: (
    Callable[[CurrentSchedulerFenceReceipt, LabSourceStageBinding, datetime], Iterator[None]] | None
) = None


@contextmanager
def _test_only_scheduler_fence_hold_hook(
    hook: Callable[[CurrentSchedulerFenceReceipt, LabSourceStageBinding, datetime], Iterator[None]],
) -> Iterator[None]:
    """Pause an already-held trusted fence in a narrow test scope only."""

    global _TEST_ONLY_SCHEDULER_FENCE_HOLD_HOOK
    with _TEST_ONLY_SCHEDULER_FENCE_HOLD_HOOK_LOCK:
        if _TEST_ONLY_SCHEDULER_FENCE_HOLD_HOOK is not None:
            raise RuntimeError("scheduler fence test hook is already active")
        _TEST_ONLY_SCHEDULER_FENCE_HOLD_HOOK = hook
    try:
        yield
    finally:
        with _TEST_ONLY_SCHEDULER_FENCE_HOLD_HOOK_LOCK:
            _TEST_ONLY_SCHEDULER_FENCE_HOLD_HOOK = None


@contextmanager
def hold_current_trusted_scheduler_fence(
    verifier: JobStoreSchedulerFenceVerifier,
    receipt: CurrentSchedulerFenceReceipt,
    *,
    binding: LabSourceStageBinding,
    now: datetime,
    _hold_current: Callable[
        [
            JobStoreSchedulerFenceVerifier,
            CurrentSchedulerFenceReceipt,
        ],
        Iterator[None],
    ] = _FROZEN_JOB_STORE_SCHEDULER_FENCE_HOLD_CURRENT,
) -> Iterator[None]:
    """Hold the module-load-time JobStore fence dispatch for Stage mutations."""

    if type(verifier) is not JobStoreSchedulerFenceVerifier:
        raise SchedulerLeaseFencedError("scheduler fence verifier is not trusted")
    if type(receipt) is not CurrentSchedulerFenceReceipt or receipt.binding != binding:
        raise SchedulerLeaseFencedError("scheduler fence receipt binding is not exact")
    with _hold_current(verifier, receipt, binding=binding, now=now):
        with _TEST_ONLY_SCHEDULER_FENCE_HOLD_HOOK_LOCK:
            hook = _TEST_ONLY_SCHEDULER_FENCE_HOLD_HOOK
        if hook is None:
            yield
            return
        with hook(receipt, binding, now):
            yield


def _scheduler_fence_lease_commitment(row: Mapping[str, object]) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "contract": "rquant-current-scheduler-fence-lease/v1",
                "lease_id": _strict_sqlite_int(
                    row["lease_id"], field="lab_lease.lease_id", minimum=1
                ),
                "owner_id": str(row["owner_id"]),
                "token": str(row["token"]),
                "fencing_token": _strict_sqlite_int(
                    row["fencing_token"], field="lab_lease.fencing_token", minimum=1
                ),
                "acquired_at": _dump_time(_load_time(str(row["acquired_at"]))),
                "expires_at": _dump_time(_load_time(str(row["expires_at"]))),
            }
        )
    ).hexdigest()


def _scheduler_fence_row_commitment(
    *,
    owner_id: str,
    scheduler_fencing_token: int,
    lease_id: int,
    lease_commitment: str,
    issued_at: datetime,
    expires_at: datetime,
) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "contract": "rquant-current-scheduler-fence-row/v1",
                "owner_id": owner_id,
                "scheduler_fencing_token": scheduler_fencing_token,
                "lease_id": lease_id,
                "lease_commitment": lease_commitment,
                "issued_at": _dump_time(_utc(issued_at)),
                "expires_at": _dump_time(_utc(expires_at)),
            }
        )
    ).hexdigest()


def _scheduler_fence_receipt_commitment(
    *,
    canonical_job_store_path: str,
    database_generation: tuple[int, int],
    store_id: str,
    application_id: int,
    schema_version: int,
    implementation_digest: str,
    binding: LabSourceStageBinding,
    row_commitment: str,
) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "contract": "rquant-current-scheduler-fence-receipt/v1",
                "canonical_job_store_path": canonical_job_store_path,
                "database_generation": database_generation,
                "store_id": store_id,
                "application_id": application_id,
                "schema_version": schema_version,
                "implementation_digest": implementation_digest,
                "binding": binding.model_dump(mode="json"),
                "row_commitment": row_commitment,
            }
        )
    ).hexdigest()


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


class _LabJobListCursor(LabRecordModel):
    cursor_type: Literal["lab_job_list"] = "lab_job_list"
    schema_version: Literal[1] = 1
    filter_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime
    job_id: UUID

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: datetime) -> datetime:
        return _utc(value)


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
    """One live-query page, not a database snapshot.

    ``total_count`` for an unfiltered page comes from the writer-maintained
    summary row. Filtered totals are deliberately unknown: exact totals for
    arbitrary date and substring predicates require an unbounded history scan.
    ``has_more`` only reports whether the current live result has another row
    after this page's immutable keyset boundary. Mutable filter membership,
    including status, may change between page requests.
    """

    items: tuple[LabJobSummary, ...]
    total_count: int | None = Field(default=None, ge=0)
    has_more: bool
    next_cursor: str | None


class LabGraphIntegrityTableCounts(LabRecordModel):
    lab_job: int = Field(ge=0)
    lab_shard: int = Field(ge=0)
    lab_event: int = Field(ge=0)
    lab_lease: int = Field(ge=0)
    lab_artifact: int = Field(ge=0)
    lab_command: int = Field(ge=0)
    lab_worker_report: int = Field(ge=0)
    lab_artifact_commit: int = Field(ge=0)
    lab_job_result_artifact: int = Field(ge=0)
    lab_scheduler_state: int = Field(ge=0)
    lab_claim_publication: int = Field(default=0, ge=0)
    lab_claim_publication_audit: int = Field(default=0, ge=0)
    lab_ledger_chain_entry: int = Field(ge=1)


class LabGraphIntegrityReceipt(LabRecordModel):
    """Verified whole-ledger audit summary pinned to one immutable read generation."""

    schema_version: Literal[2] = 2
    database_generation: tuple[int, int]
    mutation_epoch: int = Field(ge=0)
    chain_generation: int = Field(ge=0)
    chain_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    table_counts: LabGraphIntegrityTableCounts
    receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @staticmethod
    def _content_hash(
        *,
        database_generation: tuple[int, int],
        mutation_epoch: int,
        chain_generation: int,
        chain_head_hash: str,
        table_counts: LabGraphIntegrityTableCounts,
    ) -> str:
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "schema_version": 2,
                    "database_generation": database_generation,
                    "mutation_epoch": mutation_epoch,
                    "chain_generation": chain_generation,
                    "chain_head_hash": chain_head_hash,
                    "table_counts": table_counts.model_dump(mode="json"),
                }
            )
        ).hexdigest()

    @classmethod
    def verified(
        cls,
        *,
        database_generation: tuple[int, int],
        mutation_epoch: int,
        chain_generation: int,
        chain_head_hash: str,
        table_counts: LabGraphIntegrityTableCounts,
    ) -> LabGraphIntegrityReceipt:
        return cls(
            database_generation=database_generation,
            mutation_epoch=mutation_epoch,
            chain_generation=chain_generation,
            chain_head_hash=chain_head_hash,
            table_counts=table_counts,
            receipt_hash=cls._content_hash(
                database_generation=database_generation,
                mutation_epoch=mutation_epoch,
                chain_generation=chain_generation,
                chain_head_hash=chain_head_hash,
                table_counts=table_counts,
            ),
        )

    @model_validator(mode="after")
    def validate_generation_binding(self) -> LabGraphIntegrityReceipt:
        if any(type(value) is not int or value < 0 for value in self.database_generation):
            raise ValueError("database generation must contain non-negative integers")
        expected = self._content_hash(
            database_generation=self.database_generation,
            mutation_epoch=self.mutation_epoch,
            chain_generation=self.chain_generation,
            chain_head_hash=self.chain_head_hash,
            table_counts=self.table_counts,
        )
        if self.receipt_hash != expected:
            raise ValueError("integrity receipt hash does not bind its generation and summary")
        return self


class LabIncrementalIntegrityReceipt(LabRecordModel):
    """Bounded chain-tail receipt used on daemon lifecycle boundaries.

    This receipt deliberately does not claim to validate every historical row.
    Daemons validate the current append-only tail before accepting more work;
    ``audit_integrity`` remains the periodic full-graph authority.
    """

    schema_version: Literal[1] = 1
    database_generation: tuple[int, int]
    mutation_epoch: int = Field(ge=0)
    chain_generation: int = Field(ge=0)
    chain_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checked_chain_entries: int = Field(ge=1, le=129)
    receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @staticmethod
    def _content_hash(
        *,
        database_generation: tuple[int, int],
        mutation_epoch: int,
        chain_generation: int,
        chain_head_hash: str,
        checked_chain_entries: int,
    ) -> str:
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "schema_version": 1,
                    "database_generation": database_generation,
                    "mutation_epoch": mutation_epoch,
                    "chain_generation": chain_generation,
                    "chain_head_hash": chain_head_hash,
                    "checked_chain_entries": checked_chain_entries,
                }
            )
        ).hexdigest()

    @classmethod
    def verified(
        cls,
        *,
        database_generation: tuple[int, int],
        mutation_epoch: int,
        chain_generation: int,
        chain_head_hash: str,
        checked_chain_entries: int,
    ) -> LabIncrementalIntegrityReceipt:
        return cls(
            database_generation=database_generation,
            mutation_epoch=mutation_epoch,
            chain_generation=chain_generation,
            chain_head_hash=chain_head_hash,
            checked_chain_entries=checked_chain_entries,
            receipt_hash=cls._content_hash(
                database_generation=database_generation,
                mutation_epoch=mutation_epoch,
                chain_generation=chain_generation,
                chain_head_hash=chain_head_hash,
                checked_chain_entries=checked_chain_entries,
            ),
        )

    @model_validator(mode="after")
    def validate_generation_binding(self) -> LabIncrementalIntegrityReceipt:
        if any(type(value) is not int or value < 0 for value in self.database_generation):
            raise ValueError("database generation must contain non-negative integers")
        expected = self._content_hash(
            database_generation=self.database_generation,
            mutation_epoch=self.mutation_epoch,
            chain_generation=self.chain_generation,
            chain_head_hash=self.chain_head_hash,
            checked_chain_entries=self.checked_chain_entries,
        )
        if self.receipt_hash != expected:
            raise ValueError("incremental integrity receipt hash does not bind its chain tail")
        return self


class LabHighWaterObserver(Protocol):
    """Compare-and-advance observer backed by an independent authority process.

    The Lab process holds no signing or write capability over the observed
    high-water: implementations submit the watermark (bound to the graph audit
    receipt hash) to an external authority and raise on any rollback, replay,
    or degraded condition so readers fail closed.
    """

    def observe(
        self,
        *,
        database_generation: tuple[int, int],
        schema_generation: int,
        mutation_epoch: int,
        chain_generation: int,
        chain_head_hash: str,
        receipt_kind: Literal["incremental", "full"],
        receipt_hash: str,
    ) -> object: ...


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
        "_mutation_guard",
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
        mutation_guard: Callable[[], object] | None = None,
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
        self._mutation_guard = mutation_guard
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
            if self._mutation_guard is not None:
                self._mutation_guard()
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


@dataclass(frozen=True)
class _ClaimPublicationDecision:
    mutation: LabClaimPublicationMutation | None = None
    error: RuntimeError | None = None

    def resolved(self) -> LabClaimPublicationMutation:
        if self.error is not None:
            raise self.error
        if self.mutation is None:
            raise RuntimeError("claim publication decision is incomplete")
        return self.mutation


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


def _strict_sqlite_blob(value: object, *, field: str) -> bytes:
    if type(value) is not bytes:
        raise InvalidStoredJobError(f"{field} must be a SQLite blob, found {type(value).__name__}")
    return value


_LEDGER_CHAIN_GENESIS_HASH = hashlib.sha256(
    canonical_json_bytes({"contract": "rquant-lab-ledger-chain/v1", "generation": 0})
).hexdigest()


def _ledger_chain_step(
    previous_hash: object,
    chain_generation: object,
    mutation_epoch: object,
) -> str:
    """Return the canonical append-only head for one committed ledger mutation."""

    if type(previous_hash) is not str or re.fullmatch(r"[0-9a-f]{64}", previous_hash) is None:
        raise ValueError("ledger chain previous hash is invalid")
    if type(chain_generation) is not int or chain_generation < 0:
        raise ValueError("ledger chain generation is invalid")
    if type(mutation_epoch) is not int or mutation_epoch < 0:
        raise ValueError("ledger chain mutation epoch is invalid")
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "contract": "rquant-lab-ledger-chain/v1",
                "previous_hash": previous_hash,
                "chain_generation": chain_generation,
                "mutation_epoch": mutation_epoch,
            }
        )
    ).hexdigest()


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
_PAYLOAD_PROTOCOL_VALID_FUNCTION = "rquant_lab_payload_protocol_valid"
_STRATEGY_NAME_FUNCTION = "rquant_lab_strategy_name"
_SHARD_HASH_RE = re.compile(r"[0-9a-f]{64}")
_SHARD_PLAN_NAME_RE = re.compile(r"[a-z][a-z0-9_]*")


def _canonical_shard_payload(value: str) -> str:
    def reject_float(_value: str) -> float:
        raise ValueError("floating-point shard payload values are not allowed")

    def reject_constant(_value: str) -> object:
        raise ValueError("non-finite shard payload values are not allowed")

    validate_strategy_shard_payload_utf8(value, field="lab_shard.payload_json")
    parsed = strict_json_loads(
        value,
        parse_float=reject_float,
        parse_constant=reject_constant,
    )
    if not isinstance(parsed, dict):
        raise ValueError("shard payload must encode a JSON object")
    return canonical_json_bytes(parsed).decode("utf-8")


def _payload_protocol_version(value: str) -> int:
    canonical = _canonical_shard_payload(value)
    if value != canonical:
        raise ValueError("shard payload JSON is not canonical")
    payload = strict_json_loads(canonical)
    assert isinstance(payload, dict)
    schema_version = payload.get("schema_version")
    if schema_version is None:
        return 1
    if type(schema_version) is not int or schema_version not in {1, 2}:
        raise ValueError("shard payload has an unsupported protocol version")
    if schema_version == 2:
        parsed = parse_strategy_shard_payload(canonical)
        if not isinstance(parsed, StrategyShardPayloadV2):
            raise ValueError("shard payload v2 parser dispatch failed")
    return schema_version


def _sqlite_payload_protocol_valid(payload_json: object, protocol_version: object) -> int:
    try:
        return int(
            type(payload_json) is str
            and _payload_protocol_version(payload_json)
            == _strict_sqlite_int(
                protocol_version,
                field="lab_shard.payload_protocol_version",
                minimum=1,
                maximum=2,
            )
        )
    except (TypeError, ValueError, InvalidStoredJobError):
        return 0


def _canonical_stored_json_object(value: str, *, field: str) -> str:
    parsed = strict_json_loads(value)
    if not isinstance(parsed, dict):
        raise ValueError(f"{field} must encode a JSON object")
    canonical = canonical_json_bytes(parsed).decode("utf-8")
    if value != canonical:
        raise ValueError(f"{field} JSON is not canonical")
    return canonical


def _sqlite_strategy_name(spec_json: object) -> str:
    if type(spec_json) is not str:
        raise ValueError("stored strategy spec must be text")
    spec = strict_model_validate_canonical_json(ResearchRunSpec, spec_json)
    return spec.parameters.strategy_name


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
        checkpoint_json = str(checkpoint_json_value) if checkpoint_json_value is not None else None
        if failure_json is not None:
            _canonical_stored_json_object(failure_json, field="lab_shard.failure_json")
        if checkpoint_json is not None:
            _canonical_stored_json_object(checkpoint_json, field="lab_shard.checkpoint_json")

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
            canonical_payload = _canonical_shard_payload(payload_json)
            if payload_json != canonical_payload:
                raise ValueError("shard payload JSON is not canonical")
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
            shard_name = canonical_json_bytes(shard_identity).decode("utf-8")
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
        envelope = strict_model_validate_canonical_json(
            LabCommandEnvelope,
            str(row["command_json"]),
        )
        receipt = strict_model_validate_canonical_json(
            LabCommandReceipt,
            str(row["receipt_json"]),
        )
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
    """Read a legacy receipt only while the v1 migration rewrites canonical bytes."""

    return strict_model_validate_json(LabCommandReceipt, payload).job_version


def _worker_report_record_from_row(
    row: sqlite3.Row,
    *,
    expected_report_id: UUID | None = None,
) -> LabWorkerReportRecord:
    stored_id = str(row["report_id"])
    try:
        report = strict_model_validate_canonical_json(
            LabWorkerReport,
            str(row["report_json"]),
        )
        receipt = strict_model_validate_canonical_json(
            LabReportReceipt,
            str(row["receipt_json"]),
        )
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
    return canonical_model_json_bytes(model).decode("utf-8")


def _claim_publication_record_from_values(
    values: Mapping[str, object],
) -> LabClaimPublicationRecord:
    material = dict(values)
    identity_value = material.get("identity")
    material["identity"] = LabClaimPublicationIdentity.model_validate(identity_value)
    provisional = LabClaimPublicationRecord.model_construct(
        **material,
        record_commitment="0" * 64,
    )
    material["record_commitment"] = provisional.recomputed_commitment()
    return LabClaimPublicationRecord.model_validate(material)


def _claim_publication_record_from_row(row: sqlite3.Row) -> LabClaimPublicationRecord:
    stored_id = str(row["attempt_id"])
    try:

        def blob(name: str) -> bytes | None:
            return (
                _strict_sqlite_blob(row[name], field=f"lab_claim_publication.{name}")
                if row[name] is not None
                else None
            )

        def text(name: str) -> str | None:
            return str(row[name]) if row[name] is not None else None

        return LabClaimPublicationRecord(
            identity=LabClaimPublicationIdentity(
                attempt_id=_canonical_uuid_text(
                    row["attempt_id"], field="lab_claim_publication.attempt_id"
                ),
                job_id=_canonical_uuid_text(row["job_id"], field="lab_claim_publication.job_id"),
                shard_id=_canonical_uuid_text(
                    row["shard_id"], field="lab_claim_publication.shard_id"
                ),
                claim_token=_canonical_uuid_text(
                    row["claim_token"], field="lab_claim_publication.claim_token"
                ),
                claim_generation=_strict_sqlite_int(
                    row["claim_generation"],
                    field="lab_claim_publication.claim_generation",
                    minimum=1,
                ),
                scheduler_fencing_token=_strict_sqlite_int(
                    row["scheduler_fencing_token"],
                    field="lab_claim_publication.scheduler_fencing_token",
                    minimum=1,
                ),
                worker_id=str(row["worker_id"]),
                spec_hash=str(row["spec_hash"]),
                plan_hash=str(row["plan_hash"]),
                payload_hash=str(row["payload_hash"]),
            ),
            claim_preimage_bytes=_strict_sqlite_blob(
                row["claim_preimage_bytes"],
                field="lab_claim_publication.claim_preimage_bytes",
            ),
            claim_preimage_hash=str(row["claim_preimage_hash"]),
            claim_protocol=str(row["claim_protocol"]),
            claim_protocol_version=str(row["claim_protocol_version"]),
            source_wait_deadline=_load_time(str(row["source_wait_deadline"])),
            publication_deadline=_load_time(str(row["publication_deadline"])),
            source_stage_authority_bytes=_strict_sqlite_blob(
                row["source_stage_authority_bytes"],
                field="lab_claim_publication.source_stage_authority_bytes",
            ),
            source_stage_authority_hash=str(row["source_stage_authority_hash"]),
            source_stage_binding_bytes=blob("source_stage_binding_bytes"),
            source_stage_binding_hash=text("source_stage_binding_hash"),
            source_intent_bytes=blob("source_intent_bytes"),
            source_intent_hash=text("source_intent_hash"),
            source_operation_id=text("source_operation_id"),
            source_operation_hash=text("source_operation_hash"),
            queued_source_stage_record_hash=text("queued_source_stage_record_hash"),
            ready_source_stage_record_bytes=blob("ready_source_stage_record_bytes"),
            ready_source_stage_record_hash=text("ready_source_stage_record_hash"),
            verified_source_outcome_hash=text("verified_source_outcome_hash"),
            verified_evidence_chain_hash=text("verified_evidence_chain_hash"),
            source_use_plan_bytes=blob("source_use_plan_bytes"),
            source_use_plan_hash=text("source_use_plan_hash"),
            final_claim_bytes=blob("final_claim_bytes"),
            final_claim_hash=text("final_claim_hash"),
            current_claim_receipt_bytes=blob("current_claim_receipt_bytes"),
            current_claim_receipt_hash=text("current_claim_receipt_hash"),
            spool_receipt_bytes=blob("spool_receipt_bytes"),
            spool_receipt_hash=text("spool_receipt_hash"),
            status=ClaimPublicationStatus(str(row["status"])),
            version=_strict_sqlite_int(
                row["version"], field="lab_claim_publication.version", minimum=0
            ),
            created_at=_load_time(str(row["created_at"])),
            updated_at=_load_time(str(row["updated_at"])),
            queued_at=(_load_time(str(row["queued_at"])) if row["queued_at"] is not None else None),
            ready_at=(_load_time(str(row["ready_at"])) if row["ready_at"] is not None else None),
            published_at=(
                _load_time(str(row["published_at"])) if row["published_at"] is not None else None
            ),
            aborted_at=(
                _load_time(str(row["aborted_at"])) if row["aborted_at"] is not None else None
            ),
            terminal_reason=text("terminal_reason"),
            record_commitment=str(row["record_commitment"]),
        )
    except Exception as exc:
        if isinstance(exc, InvalidStoredJobError):
            raise
        raise InvalidStoredJobError(f"invalid stored claim publication record {stored_id}") from exc


def _claim_publication_audit_from_row(
    row: sqlite3.Row,
) -> LabClaimPublicationAuditRecord:
    stored_id = str(row["audit_ref"])
    try:
        return LabClaimPublicationAuditRecord(
            audit_ref=_canonical_uuid_text(
                row["audit_ref"],
                field="lab_claim_publication_audit.audit_ref",
            ),
            attempt_id=_canonical_uuid_text(
                row["attempt_id"],
                field="lab_claim_publication_audit.attempt_id",
            ),
            action=ClaimPublicationAuditAction(str(row["action"])),
            prior_status=(
                ClaimPublicationStatus(str(row["prior_status"]))
                if row["prior_status"] is not None
                else None
            ),
            new_status=ClaimPublicationStatus(str(row["new_status"])),
            reason_code=str(row["reason_code"]),
            record_commitment=str(row["record_commitment"]),
            occurred_at=_load_time(str(row["occurred_at"])),
            audit_hash=str(row["audit_hash"]),
        )
    except Exception as exc:
        if isinstance(exc, InvalidStoredJobError):
            raise
        raise InvalidStoredJobError(f"invalid stored claim publication audit {stored_id}") from exc


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
        envelope = strict_model_validate_canonical_json(
            LabArtifactCommitEnvelope,
            str(row["commit_json"]),
        )
        receipt = strict_model_validate_canonical_json(
            LabArtifactCommitReceipt,
            str(row["receipt_json"]),
        )
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
        evidence = strict_model_validate_canonical_json(
            LabArtifactIndexEvidence,
            str(row["evidence_json"]),
        )
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


def _validate_v5_schema(
    connection: sqlite3.Connection,
    *,
    allow_epoch_triggers: bool = False,
    extra_trigger_sql: Mapping[str, str] | None = None,
) -> None:
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
    expected_trigger_sql = dict(_V5_EXPECTED_TRIGGER_SQL)
    if allow_epoch_triggers:
        expected_trigger_sql.update(_LEDGER_EPOCH_TRIGGER_SQL)
    if extra_trigger_sql is not None:
        expected_trigger_sql.update(extra_trigger_sql)
    expected_triggers = frozenset(expected_trigger_sql)
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
    for name, expected_sql in expected_trigger_sql.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            (name,),
        ).fetchone()
        assert row is not None
        if row[0] is None or not _sql_ddl_equivalent(expected_sql, str(row[0])):
            raise LabDatabaseIdentityError(
                f"lab jobs SQLite v5 trigger {name} has invalid structure"
            )


def _validate_v6_schema(
    connection: sqlite3.Connection,
    *,
    allow_v7_triggers: bool = True,
    extra_trigger_sql: Mapping[str, str] | None = None,
) -> None:
    accepted_extra_triggers: dict[str, str] = {}
    if allow_v7_triggers:
        accepted_extra_triggers.update(_LAB_JOB_SUMMARY_TRIGGER_SQL)
    if extra_trigger_sql is not None:
        accepted_extra_triggers.update(extra_trigger_sql)
    _validate_v5_schema(
        connection,
        allow_epoch_triggers=True,
        extra_trigger_sql=accepted_extra_triggers or None,
    )
    _validate_v5_table_sql(
        connection,
        table="lab_ledger_epoch",
        expected=_LEDGER_EPOCH_TABLE_STATEMENT,
    )
    columns = {
        str(row[1]): row
        for row in connection.execute("PRAGMA table_info(lab_ledger_epoch)").fetchall()
    }
    if set(columns) != {"singleton", "mutation_epoch"}:
        raise LabDatabaseIdentityError("lab jobs SQLite v6 epoch table has invalid columns")
    rows = connection.execute("SELECT singleton, mutation_epoch FROM lab_ledger_epoch").fetchall()
    if (
        len(rows) != 1
        or type(rows[0][0]) is not int
        or rows[0][0] != 1
        or type(rows[0][1]) is not int
        or rows[0][1] < 0
    ):
        raise LabDatabaseIdentityError("lab jobs SQLite v6 epoch authority is invalid")


def _validate_v7_schema(
    connection: sqlite3.Connection,
    *,
    allow_v8_triggers: bool = False,
    extra_trigger_sql: Mapping[str, str] | None = None,
) -> None:
    accepted_extra_triggers: dict[str, str] = {}
    if allow_v8_triggers:
        accepted_extra_triggers.update(_V8_PUBLICATION_TRIGGER_SQL)
    if extra_trigger_sql is not None:
        accepted_extra_triggers.update(extra_trigger_sql)
    _validate_v6_schema(
        connection,
        allow_v7_triggers=True,
        extra_trigger_sql=accepted_extra_triggers or None,
    )
    for table, statement in (
        ("lab_ledger_chain", _LEDGER_CHAIN_TABLE_STATEMENT),
        ("lab_ledger_chain_entry", _LEDGER_CHAIN_ENTRY_TABLE_STATEMENT),
        ("lab_job_list_summary", _LAB_JOB_LIST_SUMMARY_TABLE_STATEMENT),
        (
            "lab_finalization_candidate_summary",
            _LAB_FINALIZATION_CANDIDATE_SUMMARY_TABLE_STATEMENT,
        ),
    ):
        _validate_v5_table_sql(connection, table=table, expected=statement)
    chain_rows = connection.execute(
        "SELECT singleton, chain_generation, head_hash FROM lab_ledger_chain"
    ).fetchall()
    if (
        len(chain_rows) != 1
        or type(chain_rows[0][0]) is not int
        or chain_rows[0][0] != 1
        or type(chain_rows[0][1]) is not int
        or chain_rows[0][1] < 0
        or type(chain_rows[0][2]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", chain_rows[0][2]) is None
    ):
        raise LabDatabaseIdentityError("lab jobs SQLite v7 chain authority is invalid")
    latest = connection.execute(
        "SELECT mutation_epoch, previous_hash, entry_hash FROM lab_ledger_chain_entry "
        "WHERE chain_generation = ?",
        (chain_rows[0][1],),
    ).fetchone()
    if (
        latest is None
        or type(latest[0]) is not int
        or latest[0] < 0
        or type(latest[1]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", latest[1]) is None
        or type(latest[2]) is not str
        or latest[2] != chain_rows[0][2]
    ):
        raise LabDatabaseIdentityError("lab jobs SQLite v7 chain head is invalid")
    for table in ("lab_job_list_summary", "lab_finalization_candidate_summary"):
        rows = connection.execute(f"SELECT singleton, total_count FROM {table}").fetchall()
        if (
            len(rows) != 1
            or type(rows[0][0]) is not int
            or rows[0][0] != 1
            or type(rows[0][1]) is not int
            or rows[0][1] < 0
        ):
            raise LabDatabaseIdentityError(f"lab jobs SQLite v7 {table} is invalid")


def _validate_v8_schema(
    connection: sqlite3.Connection,
    *,
    extra_trigger_sql: Mapping[str, str] | None = None,
) -> None:
    for table, statement in (
        ("lab_claim_publication", _CLAIM_PUBLICATION_TABLE_STATEMENT),
        ("lab_claim_publication_audit", _CLAIM_PUBLICATION_AUDIT_TABLE_STATEMENT),
    ):
        _validate_v5_table_sql(connection, table=table, expected=statement)
    _validate_v7_schema(
        connection,
        allow_v8_triggers=True,
        extra_trigger_sql=extra_trigger_sql,
    )
    for name, statement in (
        (
            "ix_lab_claim_publication_held_deadline",
            _CLAIM_PUBLICATION_HELD_DEADLINE_INDEX_STATEMENT,
        ),
        (
            "ix_lab_claim_publication_reconcile_deadline",
            _CLAIM_PUBLICATION_RECONCILE_INDEX_STATEMENT,
        ),
        (
            "ix_lab_claim_publication_audit_attempt",
            _CLAIM_PUBLICATION_AUDIT_INDEX_STATEMENT,
        ),
    ):
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
        if row is None or row[0] is None or not _sql_ddl_equivalent(statement, str(row[0])):
            raise LabDatabaseIdentityError(f"lab jobs SQLite v8 index {name} is invalid")


def _validate_v9_schema(
    connection: sqlite3.Connection,
    *,
    extra_trigger_sql: Mapping[str, str] | None = None,
    allow_v10_indexes: bool = False,
) -> None:
    _validate_v8_schema(connection, extra_trigger_sql=extra_trigger_sql)
    expected_indexes = [("ix_lab_shard_active_claims", _ACTIVE_CLAIMS_INDEX_STATEMENT)]
    if not allow_v10_indexes:
        expected_indexes.append(("ix_lab_shard_stale_recovery", _STALE_RECOVERY_INDEX_STATEMENT))
    for name, statement in expected_indexes:
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
        if row is None or row[0] is None or not _sql_ddl_equivalent(statement, str(row[0])):
            raise LabDatabaseIdentityError(f"lab jobs SQLite v9 index {name} is invalid")


def _validate_v10_schema(connection: sqlite3.Connection) -> None:
    _validate_v9_schema(
        connection,
        extra_trigger_sql=_V10_PAYLOAD_PROTOCOL_TRIGGER_SQL,
        allow_v10_indexes=True,
    )
    _validate_v5_column_identity(
        connection,
        table="lab_shard",
        column="payload_protocol_version",
        declared_type="INTEGER",
        not_null=True,
        primary_key_position=0,
        default="1",
    )
    for name, statement in (
        ("ix_lab_shard_stale_recovery", _V10_STALE_RECOVERY_INDEX_STATEMENT),
        ("ix_lab_shard_v2_reconciliation", _V10_V2_RECONCILIATION_INDEX_STATEMENT),
    ):
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
        if row is None or row[0] is None or not _sql_ddl_equivalent(statement, str(row[0])):
            raise LabDatabaseIdentityError(f"lab jobs SQLite v10 index {name} is invalid")


def _validate_v11_schema(
    connection: sqlite3.Connection,
    *,
    allow_v12_idle_control_indexes: bool = False,
) -> None:
    _validate_v10_schema(connection)
    _validate_v5_table_sql(
        connection,
        table="lab_recovery_cursor",
        expected=_RECOVERY_CURSOR_TABLE_STATEMENT,
    )
    for name, statement in (
        (
            "ix_lab_shard_exhausted_queued_v1_recovery",
            _V11_EXHAUSTED_QUEUED_V1_RECOVERY_INDEX_STATEMENT,
        ),
        (
            "ix_lab_shard_exhausted_checkpointed_v1_recovery",
            _V11_EXHAUSTED_CHECKPOINTED_V1_RECOVERY_INDEX_STATEMENT,
        ),
        *(
            ()
            if allow_v12_idle_control_indexes
            else (("ix_lab_job_idle_control_recovery", _V11_IDLE_CONTROL_RECOVERY_INDEX_STATEMENT),)
        ),
    ):
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
        if row is None or row[0] is None or not _sql_ddl_equivalent(statement, str(row[0])):
            raise LabDatabaseIdentityError(f"lab jobs SQLite v11 index {name} is invalid")
    for row in connection.execute(
        "SELECT cursor_created_at, cursor_job_id FROM lab_recovery_cursor"
    ).fetchall():
        try:
            _load_time(str(row["cursor_created_at"]))
            _canonical_uuid_text(
                row["cursor_job_id"],
                field="lab_recovery_cursor.cursor_job_id",
            )
        except (TypeError, ValueError, InvalidStoredJobError) as exc:
            raise LabDatabaseIdentityError(
                "lab jobs SQLite v11 recovery cursor is invalid"
            ) from exc


def _validate_v12_schema(connection: sqlite3.Connection) -> None:
    _validate_v11_schema(connection, allow_v12_idle_control_indexes=True)
    for name, statement in (
        ("ix_lab_job_idle_control_recovery", _V12_IDLE_CONTROL_RECOVERY_INDEX_STATEMENT),
        ("ix_lab_shard_idle_control_eligibility", _V12_IDLE_CONTROL_SHARD_INDEX_STATEMENT),
    ):
        row = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
        if row is None or row[0] is None or not _sql_ddl_equivalent(statement, str(row[0])):
            raise LabDatabaseIdentityError(f"lab jobs SQLite v12 index {name} is invalid")


def _validate_v13_schema(connection: sqlite3.Connection) -> None:
    _validate_v12_schema(connection)
    row = connection.execute(
        "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
        ("ix_lab_shard_preclaim_candidate",),
    ).fetchone()
    if (
        row is None
        or row[0] is None
        or not _sql_ddl_equivalent(
            _V13_PRECLAIM_CANDIDATE_INDEX_STATEMENT,
            str(row[0]),
        )
    ):
        raise LabDatabaseIdentityError("lab jobs SQLite v13 preclaim candidate index is invalid")


def _validate_v14_schema(connection: sqlite3.Connection) -> None:
    _validate_v13_schema(connection)
    state_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(lab_scheduler_state)").fetchall()
    }
    required_columns = {
        "claim_cursor_shard_index",
        "claim_cursor_shard_id",
        "claim_cursor_sequence",
    }
    missing = sorted(required_columns - state_columns)
    if missing:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v14 scheduler cursor columns are missing: " + ", ".join(missing)
        )
    fair_cursor = connection.execute(
        "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'lab_preclaim_fair_cursor'"
    ).fetchone()
    if fair_cursor is None:
        raise LabDatabaseIdentityError("lab jobs SQLite v14 is missing fair preclaim cursor")


def _validate_v15_schema(connection: sqlite3.Connection) -> None:
    _validate_v14_schema(connection)
    required = {
        "lab_claim_publication_finalizer_lease",
        "lab_claim_publication_finalizer_observation",
        "lab_claim_publication_finalizer_root_anchor",
    }
    observed = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table'"
        ).fetchall()
    }
    missing = sorted(required - observed)
    if missing:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v15 finalizer authority tables are missing: " + ", ".join(missing)
        )
    columns = {
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(lab_claim_publication_finalizer_lease)"
        ).fetchall()
    }
    if "root_descriptor" not in columns:
        raise LabDatabaseIdentityError("lab jobs SQLite v15 finalizer root descriptor is missing")


def _validate_v16_finalizer_strict_support(connection: sqlite3.Connection) -> None:
    try:
        connection.execute("SELECT strict FROM pragma_table_list WHERE 0").fetchall()
    except Exception as exc:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v16 finalizer STRICT contract is unavailable or invalid"
        ) from exc


def _validate_v16_finalizer_strict_tables(connection: sqlite3.Connection) -> None:
    for table, statement in _V16_FINALIZER_STRICT_TABLES:
        try:
            _validate_v5_table_sql(connection, table=table, expected=statement)
            rows = connection.execute(
                "SELECT strict FROM pragma_table_list "
                "WHERE schema = 'main' AND name = ? AND type = 'table'",
                (table,),
            ).fetchall()
            if len(rows) != 1 or type(rows[0][0]) is not int or rows[0][0] != 1:
                raise ValueError("STRICT table metadata is invalid")
        except Exception as exc:
            raise LabDatabaseIdentityError(
                f"lab jobs SQLite v16 finalizer STRICT contract is unavailable or invalid: {table}"
            ) from exc


def _validate_v16_schema(connection: sqlite3.Connection) -> None:
    _validate_v15_schema(connection)
    _validate_v16_finalizer_strict_support(connection)
    _validate_v16_finalizer_strict_tables(connection)
    required_columns = {
        "lab_claim_publication_finalizer_observation": {
            "observation_ref",
            "attempt_id",
            "authority_fencing_token",
            "event_type",
            "reason_code",
            "record_commitment",
            "observed_at",
        },
        "lab_claim_publication_finalizer_attestation": {
            "attempt_id",
            "publication_status",
            "certificate_bytes",
            "certificate_hash",
            "attestation_bytes",
            "attestation_hash",
            "created_at",
        },
        "lab_claim_publication_finalizer_trust_cache": {
            "singleton",
            "certificate_bytes",
            "certificate_hash",
            "cached_at",
        },
        "lab_claim_publication_finalizer_observation_degradation": {
            "degradation_ref",
            "attempt_id",
            "publication_identity_hash",
            "authority_fencing_token",
            "event_type",
            "reason_code",
            "reason_code_hash",
            "error_class",
            "next_retry_at",
            "created_at",
            "drained_at",
        },
    }
    for table, expected in required_columns.items():
        columns = {
            str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        missing = sorted(expected - columns)
        if missing:
            raise LabDatabaseIdentityError(
                f"lab jobs SQLite v16 {table} columns are missing: " + ", ".join(missing)
            )
    required_indexes = {
        "ix_lab_claim_publication_finalizer_observation_attempt",
        "ix_lab_claim_publication_finalizer_degradation_due",
    }
    observed_indexes = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'index'"
        ).fetchall()
    }
    missing_indexes = sorted(required_indexes - observed_indexes)
    if missing_indexes:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v16 finalizer indexes are missing: " + ", ".join(missing_indexes)
        )


def _validate_current_schema(connection: sqlite3.Connection) -> None:
    """Validate the complete schema required by every current runtime operation."""

    try:
        _validate_v16_schema(connection)
    except LabDatabaseIdentityError as exc:
        raise LabDatabaseIdentityError("lab jobs SQLite v16 current schema is invalid") from exc


def _migrate_v13_to_v14(connection: sqlite3.Connection) -> None:
    """Persist shard-level keyset positions for bounded preclaim fairness."""

    _validate_v13_schema(connection)
    columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(lab_scheduler_state)").fetchall()
    }
    if "claim_cursor_shard_index" not in columns:
        connection.execute(
            "ALTER TABLE lab_scheduler_state ADD COLUMN claim_cursor_shard_index INTEGER"
        )
    if "claim_cursor_shard_id" not in columns:
        connection.execute("ALTER TABLE lab_scheduler_state ADD COLUMN claim_cursor_shard_id TEXT")
    if "claim_cursor_sequence" not in columns:
        connection.execute(
            """
            ALTER TABLE lab_scheduler_state
            ADD COLUMN claim_cursor_sequence INTEGER NOT NULL DEFAULT 0
            CHECK (typeof(claim_cursor_sequence) = 'integer' AND claim_cursor_sequence >= 0)
            """
        )
    connection.execute(_V14_PRECLAIM_FAIR_CURSOR_TABLE_STATEMENT)


def _migrate_v14_to_v15(connection: sqlite3.Connection) -> None:
    """Install durable fenced authority and redacted observation tables."""

    _validate_v14_schema(connection)
    connection.execute(_V15_FINALIZER_LEASE_TABLE_STATEMENT)
    connection.execute(_V15_FINALIZER_ROOT_ANCHOR_TABLE_STATEMENT)
    columns = {
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(lab_claim_publication_finalizer_lease)"
        ).fetchall()
    }
    if "root_descriptor" not in columns:
        connection.execute(
            "ALTER TABLE lab_claim_publication_finalizer_lease "
            "ADD COLUMN root_descriptor TEXT NOT NULL DEFAULT ''"
        )
    connection.execute(_V15_FINALIZER_OBSERVATION_TABLE_STATEMENT)
    connection.execute(_V15_FINALIZER_OBSERVATION_INDEX_STATEMENT)


def _migrate_v15_to_v16(connection: sqlite3.Connection) -> None:
    """Add untrusted certificate cache and signed C/D attestations.

    v15's local HMAC anchor deliberately has no migration path to trust: V2
    callers must present an externally verified certificate after this upgrade.
    """

    _validate_v15_schema(connection)
    connection.execute(_V16_FINALIZER_TRUST_CACHE_TABLE_STATEMENT)
    connection.execute(_V16_FINALIZER_ATTESTATION_TABLE_STATEMENT)
    connection.execute(_V16_FINALIZER_OBSERVATION_DEGRADATION_TABLE_STATEMENT)
    connection.execute(_V16_FINALIZER_OBSERVATION_DEGRADATION_INDEX_STATEMENT)


def _migrate_v7_to_v8(connection: sqlite3.Connection) -> None:
    """Install the additive claim-publication authority on a valid v7 ledger."""

    _validate_v7_schema(connection, allow_v8_triggers=False)
    for statement in _V8_SCHEMA_STATEMENTS[len(_V7_SCHEMA_STATEMENTS) :]:
        connection.execute(statement)


def _migrate_v8_to_v9(connection: sqlite3.Connection) -> None:
    """Install bounded active-claim and stale-recovery indexes on a v8 ledger."""

    _validate_v8_schema(connection)
    for statement in _V9_SCHEMA_STATEMENTS[len(_V8_SCHEMA_STATEMENTS) :]:
        connection.execute(statement)


def _migrate_v9_to_v10(connection: sqlite3.Connection) -> None:
    """Persist and validate the shard payload protocol before recovery can index it."""

    _validate_v9_schema(connection)
    columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(lab_shard)").fetchall()}
    if "payload_protocol_version" in columns:
        _validate_v10_schema(connection)
        return
    connection.execute(
        """
        ALTER TABLE lab_shard ADD COLUMN payload_protocol_version INTEGER
        NOT NULL DEFAULT 1
        CHECK (
            typeof(payload_protocol_version) = 'integer'
            AND payload_protocol_version IN (1, 2)
        )
        """
    )
    rows = connection.execute(
        "SELECT job_id, shard_id, payload_json FROM lab_shard ORDER BY job_id, shard_id"
    ).fetchall()
    try:
        for row in rows:
            protocol_version = _payload_protocol_version(str(row["payload_json"]))
            connection.execute(
                """
                UPDATE lab_shard SET payload_protocol_version = ?
                WHERE job_id = ? AND shard_id = ?
                """,
                (protocol_version, str(row["job_id"]), str(row["shard_id"])),
            )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LabDatabaseIdentityError(
            "lab jobs SQLite v10 payload protocol backfill failed"
        ) from exc
    connection.execute("DROP INDEX ix_lab_shard_stale_recovery")
    for statement in _V10_SCHEMA_STATEMENTS[len(_V9_SCHEMA_STATEMENTS) :]:
        connection.execute(statement)


def _migrate_v10_to_v11(connection: sqlite3.Connection) -> None:
    """Install bounded exhausted and idle-control recovery authorities."""

    _validate_v10_schema(connection)
    for statement in _V11_SCHEMA_STATEMENTS[len(_V10_SCHEMA_STATEMENTS) :]:
        connection.execute(statement)


def _migrate_v11_to_v12(connection: sqlite3.Connection) -> None:
    """Make idle-control recovery candidates bounded, eligible, and fair."""

    _validate_v11_schema(connection)
    connection.execute("DROP INDEX ix_lab_job_idle_control_recovery")
    for statement in _V12_SCHEMA_STATEMENTS[len(_V11_SCHEMA_STATEMENTS) - 1 :]:
        connection.execute(statement)


def _migrate_v12_to_v13(connection: sqlite3.Connection) -> None:
    """Install the bounded protocol-discriminated preclaim candidate index."""

    _validate_v12_schema(connection)
    connection.execute(_V13_PRECLAIM_CANDIDATE_INDEX_STATEMENT)


def _migrate_v6_to_v7(connection: sqlite3.Connection) -> None:
    """Install idempotent chain and read-summary authorities for a v6 ledger."""

    _validate_v6_schema(connection, allow_v7_triggers=False)
    for statement in _V7_SCHEMA_STATEMENTS[len(_V6_SCHEMA_STATEMENTS) :]:
        connection.execute(statement)
    epoch = _strict_sqlite_int(
        connection.execute(
            "SELECT mutation_epoch FROM lab_ledger_epoch WHERE singleton = 1"
        ).fetchone()[0],
        field="lab_ledger_epoch.mutation_epoch",
        minimum=0,
    )
    existing = connection.execute("SELECT COUNT(*) FROM lab_ledger_chain_entry").fetchone()[0]
    if existing == 1:
        migrated_head = _ledger_chain_step(_LEDGER_CHAIN_GENESIS_HASH, 0, epoch)
        connection.execute(
            "UPDATE lab_ledger_chain SET chain_generation = 0, head_hash = ? WHERE singleton = 1",
            (migrated_head,),
        )
        connection.execute(
            "UPDATE lab_ledger_chain_entry "
            "SET mutation_epoch = ?, previous_hash = ?, entry_hash = ? "
            "WHERE chain_generation = 0",
            (epoch, _LEDGER_CHAIN_GENESIS_HASH, migrated_head),
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
        """
        SELECT request_id, command_json, receipt_json
        FROM lab_command ORDER BY request_id
        """
    ).fetchall()
    for row in rows:
        envelope = strict_model_validate_json(
            LabCommandEnvelope,
            str(row["command_json"]),
        )
        receipt = strict_model_validate_json(
            LabCommandReceipt,
            str(row["receipt_json"]),
        )
        job_version = _receipt_job_version_from_json(str(row["receipt_json"]))
        connection.execute(
            """
            UPDATE lab_command
            SET command_json = ?, receipt_json = ?, receipt_job_version = ?
            WHERE request_id = ?
            """,
            (
                _canonical_model_json(envelope),
                _canonical_model_json(receipt),
                job_version,
                str(row["request_id"]),
            ),
        )
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'lab_job'"
    ).fetchone():
        for row in connection.execute("SELECT job_id, spec_json FROM lab_job").fetchall():
            spec = strict_model_validate_json(ResearchRunSpec, str(row["spec_json"]))
            connection.execute(
                "UPDATE lab_job SET spec_json = ? WHERE job_id = ?",
                (_canonical_model_json(spec), str(row["job_id"])),
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

    def __init__(
        self,
        path: Path,
        *,
        busy_timeout_ms: int = 5_000,
        identity_authority: LabSqliteIdentityAuthority | None = None,
        highwater_observer: LabHighWaterObserver | None = None,
        production_mode: bool = False,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.identity_authority = identity_authority
        if production_mode and highwater_observer is None:
            raise ValueError("production Lab reader requires the external high-water authority")
        self.highwater_observer = highwater_observer
        self._validated_graph_generation: tuple[object, ...] | None = None
        self._validated_graph_receipt: LabGraphIntegrityReceipt | None = None
        self._integrity_anchor: tuple[tuple[int, int], int, int, str] | None = None
        self.graph_validation_runs = 0
        self.graph_validation_peak_batch = 0
        if identity_authority is not None and identity_authority.path != self.path:
            raise ValueError("SQLite identity authority path mismatch")

    def _connect(self) -> sqlite3.Connection:
        def open_readonly(path: Path) -> sqlite3.Connection:
            authority = self.identity_authority
            database_path = path if authority else path.resolve()
            before: os.stat_result | None = None
            if authority is None:
                try:
                    before = database_path.stat(follow_symlinks=False)
                except FileNotFoundError:
                    raise sqlite3.OperationalError("unable to open database file") from None
            uri = f"file:{quote(str(database_path))}?mode=ro"
            connection = sqlite3.connect(
                uri,
                uri=True,
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
                factory=_LabJobReaderConnection,
            )
            if authority is not None:
                connection.database_generation = authority.database_generation
            else:
                assert before is not None
                after = database_path.stat(follow_symlinks=False)
                if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                    connection.close()
                    raise LabDatabaseIdentityError(
                        "lab jobs SQLite file generation changed while opening"
                    )
                connection.database_generation = (before.st_dev, before.st_ino)
            return connection

        connection = (
            self.identity_authority.open_verified_connection(open_readonly)
            if self.identity_authority is not None
            else open_readonly(self.path)
        )
        if not isinstance(connection, _LabJobReaderConnection):
            connection.close()
            raise TypeError("lab SQLite authority returned an incompatible reader connection")
        if self.identity_authority is not None:
            connection.identity_authority = self.identity_authority
        connection.create_function(
            _SHARD_ROW_VALID_FUNCTION,
            32,
            _sqlite_shard_row_valid,
            deterministic=True,
        )
        connection.create_function(
            _PAYLOAD_PROTOCOL_VALID_FUNCTION,
            2,
            _sqlite_payload_protocol_valid,
            deterministic=True,
        )
        connection.create_function(
            _LEDGER_CHAIN_STEP_FUNCTION,
            3,
            _ledger_chain_step,
            deterministic=True,
        )
        connection.create_function(
            _STRATEGY_NAME_FUNCTION,
            1,
            _sqlite_strategy_name,
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
            _validate_current_schema(connection)
        except BaseException:
            connection.close()
            raise
        return connection

    def _storage_revision(self) -> tuple[tuple[int, int, int, int, int] | None, ...]:
        revisions: list[tuple[int, int, int, int, int] | None] = []
        # Opening a read connection may update SQLite's shared-memory sidecar;
        # it is not a content revision. The database and WAL files are.
        for suffix in ("", "-wal"):
            try:
                observed = Path(f"{self.path}{suffix}").stat(follow_symlinks=False)
            except FileNotFoundError:
                revisions.append(None)
            else:
                revisions.append(
                    (
                        observed.st_dev,
                        observed.st_ino,
                        observed.st_size,
                        observed.st_mtime_ns,
                        observed.st_ctime_ns,
                    )
                )
        return tuple(revisions)

    @contextmanager
    def _read_snapshot(self, *, label: str) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        lifecycle_errors: list[BaseException] = []
        try:
            connection.execute("BEGIN")
            yield connection
            connection.execute("COMMIT")
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
                f"{label} query and cleanup failed",
                lifecycle_errors,
            )

    @staticmethod
    def _after_finalization_job_read(_job_id: UUID) -> None:
        """Fault-injection boundary after the snapshot's first authoritative read."""

    @staticmethod
    def _job_from_row(row: sqlite3.Row) -> LabJobRecord:
        try:
            spec = strict_model_validate_canonical_json(
                ResearchRunSpec,
                str(row["spec_json"]),
            )
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
            payload_json = str(row["payload_json"])
            failure_json = str(row["failure_json"]) if row["failure_json"] is not None else None
            checkpoint_json = (
                str(row["checkpoint_json"]) if row["checkpoint_json"] is not None else None
            )
            if (
                str(row["adapter_id"]) != "legacy-v2"
                and _canonical_shard_payload(payload_json) != payload_json
            ):
                raise ValueError("shard payload JSON is not canonical")
            if _payload_protocol_version(payload_json) != _strict_sqlite_int(
                row["payload_protocol_version"],
                field="lab_shard.payload_protocol_version",
                minimum=1,
                maximum=2,
            ):
                raise ValueError("shard payload protocol does not match payload JSON")
            if failure_json is not None:
                _canonical_stored_json_object(failure_json, field="lab_shard.failure_json")
            if checkpoint_json is not None:
                _canonical_stored_json_object(checkpoint_json, field="lab_shard.checkpoint_json")
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
                checkpoint_json=checkpoint_json,
                plan_hash=str(row["plan_hash"]),
                adapter_id=str(row["adapter_id"]),
                adapter_version=str(row["adapter_version"]),
                payload_json=payload_json,
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
                failure_json=failure_json,
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
            value = strict_json_loads(payload)
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
    def _job_list_filter_identity(filters: LabJobListFilters) -> str:
        return hashlib.sha256(_canonical_model_json(filters).encode("ascii")).hexdigest()

    @classmethod
    def _encode_job_list_cursor(
        cls,
        *,
        created_at: datetime,
        job_id: UUID,
        filters: LabJobListFilters,
    ) -> str:
        value = _LabJobListCursor(
            filter_identity=cls._job_list_filter_identity(filters),
            created_at=created_at,
            job_id=job_id,
        )
        payload = _canonical_model_json(value).encode("ascii")
        return urlsafe_b64encode(payload).decode("ascii").rstrip("=")

    @classmethod
    def _decode_job_list_cursor(
        cls,
        cursor: str,
        *,
        filters: LabJobListFilters,
    ) -> _LabJobListCursor:
        try:
            padding = "=" * (-len(cursor) % 4)
            payload = urlsafe_b64decode(f"{cursor}{padding}".encode("ascii"))
            value = strict_model_validate_canonical_json(_LabJobListCursor, payload)
            canonical = (
                urlsafe_b64encode(_canonical_model_json(value).encode("ascii"))
                .decode("ascii")
                .rstrip("=")
            )
            if canonical != cursor:
                raise ValueError
        except Exception as exc:
            raise ValueError("invalid opaque job list cursor") from exc
        if value.filter_identity != cls._job_list_filter_identity(filters):
            raise ValueError("job list cursor filter identity does not match filters")
        return value

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
    def _summary_stats_sql(
        *,
        leading_ctes: str | None = None,
        shard_job_scope: str | None = None,
    ) -> str:
        cte_prefix = f"{leading_ctes}," if leading_ctes is not None else ""
        shard_scope = (
            f"WHERE job_id IN (SELECT job_id FROM {shard_job_scope})"
            if shard_job_scope is not None
            else ""
        )
        return f"""
            WITH {cte_prefix} shard_stats AS (
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
                FROM lab_shard {shard_scope} GROUP BY job_id
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
        *,
        include_keyword: bool = True,
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
        if include_keyword and filters.keyword is not None:
            escaped = (
                filters.keyword.casefold()
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
            )
            clauses.append(
                f"(LOWER({_STRATEGY_NAME_FUNCTION}(j.spec_json)) "
                "LIKE ? ESCAPE '\\' OR LOWER(j.job_id) LIKE ? ESCAPE '\\' "
                "OR LOWER(j.spec_hash) LIKE ? ESCAPE '\\')"
            )
            parameters.extend((f"%{escaped}%",) * 3)
        if len(parameters) > LAB_JOB_LIST_FILTER_SQL_PARAMETER_MAX:
            raise ValueError("job list filters exceed the SQL parameter budget")
        return clauses, parameters

    def _stream_validated_rows(
        self,
        connection: sqlite3.Connection,
        *,
        table: str,
        validator: Callable[[sqlite3.Row], object],
    ) -> int:
        cursor = connection.execute(f"SELECT * FROM {table}")
        count = 0
        while True:
            rows = cursor.fetchmany(64)
            if not rows:
                return count
            count += len(rows)
            self.graph_validation_peak_batch = max(
                self.graph_validation_peak_batch,
                len(rows),
            )
            for row in rows:
                validator(row)

    def _validate_ledger_chain(
        self,
        connection: sqlite3.Connection,
        *,
        expected_generation: int,
        expected_head_hash: str,
    ) -> int:
        cursor = connection.execute(
            "SELECT chain_generation, mutation_epoch, previous_hash, entry_hash "
            "FROM lab_ledger_chain_entry ORDER BY chain_generation"
        )
        expected_entry_generation = 0
        previous_hash = _LEDGER_CHAIN_GENESIS_HASH
        count = 0
        while True:
            rows = cursor.fetchmany(64)
            if not rows:
                break
            self.graph_validation_peak_batch = max(self.graph_validation_peak_batch, len(rows))
            for row in rows:
                generation = _strict_sqlite_int(
                    row["chain_generation"],
                    field="lab_ledger_chain_entry.chain_generation",
                    minimum=0,
                )
                _strict_sqlite_int(
                    row["mutation_epoch"],
                    field="lab_ledger_chain_entry.mutation_epoch",
                    minimum=0,
                )
                entry_previous = row["previous_hash"]
                entry_hash = row["entry_hash"]
                if (
                    generation != expected_entry_generation
                    or entry_previous != previous_hash
                    or type(entry_hash) is not str
                    or re.fullmatch(r"[0-9a-f]{64}", entry_hash) is None
                ):
                    raise InvalidStoredJobError("Lab job graph chain entry is invalid")
                previous_hash = entry_hash
                expected_entry_generation += 1
                count += 1
        if (
            count == 0
            or expected_entry_generation - 1 != expected_generation
            or previous_hash != expected_head_hash
        ):
            raise InvalidStoredJobError("Lab job graph chain head conflicts with its entries")
        return count

    @staticmethod
    def _chain_authority_from_rows(
        connection: sqlite3.Connection,
    ) -> tuple[int, int, str]:
        epoch_row = connection.execute(
            "SELECT mutation_epoch FROM lab_ledger_epoch WHERE singleton = 1"
        ).fetchone()
        if epoch_row is None:
            raise InvalidStoredJobError("Lab job graph mutation epoch is missing")
        epoch = _strict_sqlite_int(
            epoch_row["mutation_epoch"],
            field="lab_ledger_epoch.mutation_epoch",
            minimum=0,
        )
        chain_row = connection.execute(
            "SELECT chain_generation, head_hash FROM lab_ledger_chain WHERE singleton = 1"
        ).fetchone()
        if chain_row is None:
            raise InvalidStoredJobError("Lab job graph chain authority is missing")
        chain_generation = _strict_sqlite_int(
            chain_row["chain_generation"],
            field="lab_ledger_chain.chain_generation",
            minimum=0,
        )
        chain_head_hash = chain_row["head_hash"]
        if (
            type(chain_head_hash) is not str
            or re.fullmatch(r"[0-9a-f]{64}", chain_head_hash) is None
        ):
            raise InvalidStoredJobError("Lab job graph chain head is invalid")
        return epoch, chain_generation, chain_head_hash

    @staticmethod
    def _validate_incremental_summaries(connection: sqlite3.Connection) -> None:
        for table in ("lab_job_list_summary", "lab_finalization_candidate_summary"):
            row = connection.execute(
                f"SELECT singleton, total_count FROM {table} WHERE singleton = 1"
            ).fetchone()
            if (
                row is None
                or _strict_sqlite_int(
                    row["singleton"],
                    field=f"{table}.singleton",
                    minimum=1,
                )
                != 1
            ):
                raise InvalidStoredJobError(f"{table} authority is missing")
            _strict_sqlite_int(
                row["total_count"],
                field=f"{table}.total_count",
                minimum=0,
            )

    def _validate_chain_tail(
        self,
        connection: sqlite3.Connection,
        *,
        epoch: int,
        chain_generation: int,
        chain_head_hash: str,
        max_chain_entries: int,
    ) -> int:
        start_generation = max(0, chain_generation - max_chain_entries)
        rows = connection.execute(
            "SELECT chain_generation, mutation_epoch, previous_hash, entry_hash "
            "FROM lab_ledger_chain_entry "
            "WHERE chain_generation >= ? AND chain_generation <= ? "
            "ORDER BY chain_generation",
            (start_generation, chain_generation),
        ).fetchall()
        if not rows:
            raise InvalidStoredJobError("Lab job graph chain tail is missing")
        expected_generation = start_generation
        previous_hash: str | None = None
        for row in rows:
            generation = _strict_sqlite_int(
                row["chain_generation"],
                field="lab_ledger_chain_entry.chain_generation",
                minimum=0,
            )
            entry_epoch = _strict_sqlite_int(
                row["mutation_epoch"],
                field="lab_ledger_chain_entry.mutation_epoch",
                minimum=0,
            )
            entry_previous = row["previous_hash"]
            entry_hash = row["entry_hash"]
            if (
                generation != expected_generation
                or type(entry_previous) is not str
                or re.fullmatch(r"[0-9a-f]{64}", entry_previous) is None
                or type(entry_hash) is not str
                or re.fullmatch(r"[0-9a-f]{64}", entry_hash) is None
            ):
                raise InvalidStoredJobError("Lab job graph chain tail is invalid")
            if previous_hash is not None and entry_previous != previous_hash:
                raise InvalidStoredJobError("Lab job graph chain tail linkage is invalid")
            if generation == 0 and entry_previous != _LEDGER_CHAIN_GENESIS_HASH:
                raise InvalidStoredJobError("Lab job graph chain genesis is invalid")
            previous_hash = entry_hash
            expected_generation += 1
            if generation == chain_generation and entry_epoch != epoch:
                raise InvalidStoredJobError(
                    "Lab job graph chain tail conflicts with mutation epoch"
                )
        if expected_generation - 1 != chain_generation or previous_hash != chain_head_hash:
            raise InvalidStoredJobError("Lab job graph chain tail conflicts with its authority")
        return len(rows)

    def _advance_integrity_anchor(
        self,
        *,
        database_generation: tuple[int, int],
        mutation_epoch: int,
        chain_generation: int,
        chain_head_hash: str,
        receipt_kind: Literal["incremental", "full"],
        receipt_hash: str,
    ) -> None:
        """Advance the reader-held receipt anchor without permitting rollback.

        The SQLite ledger is protected by the runtime's private identity
        authority, while this in-process receipt anchors successive reads. A
        same-inode writer cannot silently rewind epoch and chain state after a
        daemon has observed a newer committed generation.  When an external
        high-water observer is configured, every watermark is also submitted
        (bound to the audit receipt hash) to the independent compare-and-advance
        authority, which fails closed on rollback, replay, or degradation.
        """

        observed = (
            database_generation,
            mutation_epoch,
            chain_generation,
            chain_head_hash,
        )
        previous = self._integrity_anchor
        if previous is not None:
            (
                previous_database_generation,
                previous_epoch,
                previous_chain_generation,
                previous_head,
            ) = previous
            if database_generation != previous_database_generation:
                raise LabDatabaseIdentityError(
                    "Lab job graph database generation changed after audit"
                )
            if chain_generation < previous_chain_generation:
                raise InvalidStoredJobError(
                    "Lab job graph chain generation rolled back after audit"
                )
            if chain_generation == previous_chain_generation:
                if mutation_epoch != previous_epoch or chain_head_hash != previous_head:
                    raise InvalidStoredJobError("Lab job graph chain authority changed in place")
                return
            if mutation_epoch <= previous_epoch:
                raise InvalidStoredJobError("Lab job graph mutation epoch rolled back after audit")
        if self.highwater_observer is not None:
            self.highwater_observer.observe(
                database_generation=database_generation,
                schema_generation=_SCHEMA_VERSION,
                mutation_epoch=mutation_epoch,
                chain_generation=chain_generation,
                chain_head_hash=chain_head_hash,
                receipt_kind=receipt_kind,
                receipt_hash=receipt_hash,
            )
        self._integrity_anchor = observed

    def audit_incremental(
        self,
        *,
        max_chain_entries: int = 16,
    ) -> LabIncrementalIntegrityReceipt:
        """Validate the current authority and a bounded append-only chain tail.

        The scheduler and finalizer call this before they accept more work. It
        catches epoch rollback, current summary damage, and tail corruption in
        O(max_chain_entries) SQLite work; ``audit_integrity`` is retained for
        periodic whole-history validation.
        """

        if not 1 <= max_chain_entries <= 128:
            raise ValueError("max_chain_entries must be between 1 and 128")
        with self._read_snapshot(label="lab job incremental integrity audit") as connection:
            if not isinstance(connection, _LabJobReaderConnection):
                raise InvalidStoredJobError("Lab job graph database generation is unavailable")
            database_generation = connection.database_generation
            if database_generation is None:
                raise InvalidStoredJobError("Lab job graph database generation is unavailable")
            storage_revision = self._storage_revision()
            epoch, chain_generation, chain_head_hash = self._chain_authority_from_rows(connection)
            checked_chain_entries = self._validate_chain_tail(
                connection,
                epoch=epoch,
                chain_generation=chain_generation,
                chain_head_hash=chain_head_hash,
                max_chain_entries=max_chain_entries,
            )
            self._validate_incremental_summaries(connection)
            final_epoch, final_chain_generation, final_chain_head_hash = (
                self._chain_authority_from_rows(connection)
            )
            if (final_epoch, final_chain_generation, final_chain_head_hash) != (
                epoch,
                chain_generation,
                chain_head_hash,
            ) or self._storage_revision() != storage_revision:
                raise InvalidStoredJobError("Lab job graph changed during incremental validation")
            receipt = LabIncrementalIntegrityReceipt.verified(
                database_generation=database_generation,
                mutation_epoch=epoch,
                chain_generation=chain_generation,
                chain_head_hash=chain_head_hash,
                checked_chain_entries=checked_chain_entries,
            )
            self._advance_integrity_anchor(
                database_generation=database_generation,
                mutation_epoch=epoch,
                chain_generation=chain_generation,
                chain_head_hash=chain_head_hash,
                receipt_kind="incremental",
                receipt_hash=receipt.receipt_hash,
            )
            return receipt

    def _validate_authoritative_graph(
        self,
        connection: sqlite3.Connection,
    ) -> LabGraphIntegrityReceipt:
        """Validate every persistent authority for an explicit audit receipt."""

        epoch, chain_generation, chain_head_hash = self._chain_authority_from_rows(connection)
        if not isinstance(connection, _LabJobReaderConnection):
            raise InvalidStoredJobError("Lab job graph database generation is unavailable")
        database_generation = connection.database_generation
        if database_generation is None:
            raise InvalidStoredJobError("Lab job graph database generation is unavailable")
        storage_revision = self._storage_revision()
        graph_generation = (
            *database_generation,
            chain_generation,
            chain_head_hash,
            storage_revision,
        )
        if self._validated_graph_generation == graph_generation:
            if self._validated_graph_receipt is None:
                raise InvalidStoredJobError("Lab job graph audit receipt is missing")
            return self._validated_graph_receipt
        self.graph_validation_runs += 1
        chain_entry_count = self._validate_ledger_chain(
            connection,
            expected_generation=chain_generation,
            expected_head_hash=chain_head_hash,
        )
        foreign_key_error = connection.execute("PRAGMA foreign_key_check").fetchone()
        if foreign_key_error is not None:
            raise InvalidStoredJobError("Lab job graph contains a foreign-key violation")
        validators: tuple[tuple[str, Callable[[sqlite3.Row], object]], ...] = (
            ("lab_shard", self._shard_from_row),
            ("lab_event", self._event_from_row),
            ("lab_lease", self._lease_from_row),
            ("lab_artifact", self._artifact_from_row),
            ("lab_command", _command_record_from_row),
            ("lab_worker_report", _worker_report_record_from_row),
            ("lab_artifact_commit", _artifact_commit_record_from_row),
            ("lab_job_result_artifact", _result_artifact_evidence_from_row),
            ("lab_claim_publication", _claim_publication_record_from_row),
            ("lab_claim_publication_audit", _claim_publication_audit_from_row),
        )
        job_cursor = connection.execute("SELECT * FROM lab_job")
        table_counts: dict[str, int] = {"lab_job": 0}
        while True:
            rows = job_cursor.fetchmany(64)
            if not rows:
                break
            self.graph_validation_peak_batch = max(
                self.graph_validation_peak_batch,
                len(rows),
            )
            table_counts["lab_job"] += len(rows)
            for row in rows:
                job = self._job_from_row(row)
                self._validate_complete_result_graph(connection, job)
        for table, validator in validators:
            table_counts[table] = self._stream_validated_rows(
                connection,
                table=table,
                validator=validator,
            )

        cursor = connection.execute("SELECT * FROM lab_scheduler_state")
        scheduler_state_count = 0
        while True:
            batch = cursor.fetchmany(64)
            if not batch:
                break
            scheduler_state_count += len(batch)
            self.graph_validation_peak_batch = max(
                self.graph_validation_peak_batch,
                len(batch),
            )
            for row in batch:
                try:
                    if str(row["state_key"]) != "claim_job_cursor":
                        raise ValueError("state key is unsupported")
                    _load_time(str(row["claim_cursor_created_at"]))
                    _canonical_uuid_text(
                        row["claim_cursor_job_id"],
                        field="lab_scheduler_state.claim_cursor_job_id",
                    )
                    _load_time(str(row["updated_at"]))
                except Exception as exc:
                    raise InvalidStoredJobError("invalid stored scheduler state") from exc
        final_epoch_row = connection.execute(
            "SELECT mutation_epoch FROM lab_ledger_epoch WHERE singleton = 1"
        ).fetchone()
        if (
            final_epoch_row is None
            or _strict_sqlite_int(
                final_epoch_row["mutation_epoch"],
                field="lab_ledger_epoch.mutation_epoch",
                minimum=0,
            )
            != epoch
        ):
            raise InvalidStoredJobError("Lab job graph changed during validation")
        final_chain_row = connection.execute(
            "SELECT chain_generation, head_hash FROM lab_ledger_chain WHERE singleton = 1"
        ).fetchone()
        if (
            final_chain_row is None
            or _strict_sqlite_int(
                final_chain_row["chain_generation"],
                field="lab_ledger_chain.chain_generation",
                minimum=0,
            )
            != chain_generation
            or final_chain_row["head_hash"] != chain_head_hash
        ):
            raise InvalidStoredJobError("Lab job graph chain changed during validation")
        if self._storage_revision() != storage_revision:
            raise InvalidStoredJobError("Lab job graph storage changed during validation")
        table_counts["lab_scheduler_state"] = scheduler_state_count
        table_counts["lab_ledger_chain_entry"] = chain_entry_count
        receipt = LabGraphIntegrityReceipt.verified(
            database_generation=database_generation,
            mutation_epoch=epoch,
            chain_generation=chain_generation,
            chain_head_hash=chain_head_hash,
            table_counts=LabGraphIntegrityTableCounts.model_validate(table_counts),
        )
        self._advance_integrity_anchor(
            database_generation=database_generation,
            mutation_epoch=epoch,
            chain_generation=chain_generation,
            chain_head_hash=chain_head_hash,
            receipt_kind="full",
            receipt_hash=receipt.receipt_hash,
        )
        self._validated_graph_generation = graph_generation
        self._validated_graph_receipt = receipt
        return receipt

    def audit_integrity(self) -> LabGraphIntegrityReceipt:
        """Explicitly validate the complete ledger and return its generation-bound receipt."""

        with self._read_snapshot(label="lab job graph integrity audit") as connection:
            return self._validate_authoritative_graph(connection)

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
        if (
            job.requires_complete_result
            and job.result_state in {LabResultState.READY, LabResultState.SEALED}
            and (progress.total_shards == 0 or progress.succeeded_shards != progress.total_shards)
        ):
            raise InvalidStoredJobError(
                "ready or sealed job summary requires all and only succeeded shards"
            )
        if (
            job.requires_complete_result
            and job.result_state is LabResultState.PENDING
            and job.status is JobStatus.RUNNING
            and job.result_contract_version == COMPLETE_RESULT_CONTRACT_VERSION
            and progress.total_shards > 0
            and progress.succeeded_shards == progress.total_shards
        ):
            raise InvalidStoredJobError(
                "running job summary with all shards succeeded must be result ready"
            )
        if has_result_index:
            try:
                evidence = strict_model_validate_canonical_json(
                    LabArtifactIndexEvidence,
                    str(row["result_evidence_json"]),
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
        """Return a live filtered page ordered by immutable job creation identity.

        The cursor is filter-bound and protects keyset continuity for jobs that
        existed in the initial ordering. It does not freeze mutable filter
        membership or counts; callers needing snapshot semantics need a
        separately versioned snapshot API.
        """
        if not 1 <= limit <= LAB_JOB_LIST_LIMIT_MAX:
            raise ValueError(f"limit must be between 1 and {LAB_JOB_LIST_LIMIT_MAX}")
        selected_filters = LabJobListFilters.model_validate(filters or LabJobListFilters())
        clauses, parameters = self._job_filters_sql(selected_filters)
        page_clauses = list(clauses)
        page_parameters = list(parameters)
        if cursor is not None:
            decoded_cursor = self._decode_job_list_cursor(cursor, filters=selected_filters)
            cursor_time = _dump_time(decoded_cursor.created_at)
            page_clauses.append("(j.created_at < ? OR (j.created_at = ? AND j.job_id < ?))")
            page_parameters.extend((cursor_time, cursor_time, str(decoded_cursor.job_id)))
        if len(page_parameters) + 1 > LAB_JOB_LIST_QUERY_PARAMETER_MAX:
            raise ValueError("job list query exceeds the SQL parameter budget")
        page_where = f" WHERE {' AND '.join(page_clauses)}" if page_clauses else ""
        try:
            with self._read_snapshot(label="job list") as connection:
                total_row = (
                    connection.execute(
                        "SELECT total_count FROM lab_job_list_summary WHERE singleton = 1"
                    ).fetchone()
                    if not clauses
                    else None
                )
                page_cte = (
                    "page_jobs AS MATERIALIZED ("
                    f"SELECT j.* FROM lab_job AS j{page_where} "
                    "ORDER BY j.created_at DESC, j.job_id DESC LIMIT ?"
                    ")"
                )
                stats_sql = self._summary_stats_sql(
                    leading_ctes=page_cte,
                    shard_job_scope="page_jobs",
                )
                rows = connection.execute(
                    f"{stats_sql} "
                    f"SELECT {self._summary_columns_sql()} FROM page_jobs AS j "
                    "LEFT JOIN shard_stats AS ss ON ss.job_id = j.job_id "
                    "ORDER BY j.created_at DESC, j.job_id DESC",
                    (*page_parameters, limit + 1),
                ).fetchall()
                for row in rows[:limit]:
                    page_job = self._job_from_row(row)
                    if page_job.result_state is LabResultState.SEALED:
                        self._validate_complete_result_graph(connection, page_job)
        except sqlite3.OperationalError as exc:
            if (
                selected_filters.keyword is not None
                and "user-defined function raised exception" in str(exc)
            ):
                raise InvalidStoredJobError(
                    "invalid stored lab job encountered while filtering strategy names"
                ) from exc
            raise
        total_count = (
            _strict_sqlite_int(total_row["total_count"], field="total_count", minimum=0)
            if total_row is not None
            else None
        )
        has_more = len(rows) > limit
        visible = rows[:limit]
        items = tuple(self._summary_from_row(row) for row in visible)
        next_cursor = (
            self._encode_job_list_cursor(
                created_at=items[-1].created_at,
                job_id=items[-1].job_id,
                filters=selected_filters,
            )
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
        if cursor is not None:
            cursor_time, cursor_id = self._decode_cursor(cursor)
            clauses.append("(j.updated_at < ? OR (j.updated_at = ? AND j.job_id < ?))")
            parameters.extend((cursor_time, cursor_time, str(cursor_id)))
        with self._read_snapshot(label="finalization candidate list") as connection:
            total_row = connection.execute(
                "SELECT total_count FROM lab_finalization_candidate_summary WHERE singleton = 1"
            ).fetchone()
            rows = connection.execute(
                f"SELECT j.* FROM lab_job AS j WHERE {' AND '.join(clauses)} "
                "ORDER BY j.updated_at DESC, j.job_id DESC LIMIT ?",
                (*parameters, limit + 1),
            ).fetchall()
            snapshots = tuple(
                self._finalization_snapshot_from_row(connection, row) for row in rows[:limit]
            )
        assert total_row is not None
        has_more = len(rows) > limit
        if any(snapshot is None for snapshot in snapshots):
            raise InvalidStoredJobError("finalization candidate changed inside its read snapshot")
        jobs = tuple(snapshot.job for snapshot in snapshots if snapshot is not None)
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

        with self._read_snapshot(label="job detail") as connection:
            job_row = connection.execute(
                "SELECT * FROM lab_job WHERE job_id = ?", (str(job_id),)
            ).fetchone()
            if job_row is None:
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
            if progress.total_shards > MAX_JOB_SHARDS:
                raise InvalidStoredJobError(
                    f"job shard count exceeds authoritative shard limit {MAX_JOB_SHARDS}"
                )
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
                eta_shard_count = len(completed_rows) + len(remaining_rows)
                if eta_shard_count > MAX_JOB_SHARDS or eta_shard_count > progress.total_shards:
                    raise InvalidStoredJobError(
                        "job ETA shard sample exceeds the authoritative shard count"
                    )
            else:
                completed_rows = ()
                remaining_rows = ()

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

    def _finalization_snapshot_from_row(
        self,
        connection: sqlite3.Connection,
        job_row: sqlite3.Row,
        *,
        invoke_job_read_boundary: bool = False,
    ) -> LabFinalizationSnapshot | None:
        job = self._job_from_row(job_row)
        self._validate_complete_result_graph(connection, job)
        if invoke_job_read_boundary:
            self._after_finalization_job_read(job.job_id)
        if (
            job.status is not JobStatus.RUNNING
            or job.result_state is not LabResultState.READY
            or job.result_contract_version != COMPLETE_RESULT_CONTRACT_VERSION
            or not job.requires_complete_result
            or job.control_intent is not ControlIntent.NONE
        ):
            return None

        ready_event_rows = connection.execute(
            """
            SELECT * FROM lab_event
            WHERE job_id = ? AND event_type = 'job_result_ready'
              AND job_version = ?
            ORDER BY event_id
            """,
            (str(job.job_id), job.version),
        ).fetchall()
        if len(ready_event_rows) != 1:
            raise InvalidStoredJobError(
                "ready finalization snapshot requires exactly one ready epoch event"
            )
        ready_event = self._event_from_row(ready_event_rows[0])

        shard_rows = connection.execute(
            "SELECT * FROM lab_shard WHERE job_id = ? ORDER BY shard_index",
            (str(job.job_id),),
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
            (str(job.job_id),),
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
            return LabFinalizationSnapshot(
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
                f"invalid finalization snapshot for job {job.job_id}: {exc}"
            ) from exc

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
            snapshot = self._finalization_snapshot_from_row(
                connection,
                job_row,
                invoke_job_read_boundary=True,
            )
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
        if not 3 <= completed_limit <= LAB_ETA_COMPLETED_LIMIT_MAX:
            raise ValueError(
                f"completed telemetry limit must be between 3 and {LAB_ETA_COMPLETED_LIMIT_MAX}"
            )
        current = _utc(as_of)
        with self._read_snapshot(label="ETA input") as connection:
            job_row = connection.execute(
                "SELECT status, control_intent FROM lab_job WHERE job_id = ?",
                (str(job_id),),
            ).fetchone()
            if job_row is None:
                return None
            shard_count_probe = connection.execute(
                "SELECT 1 FROM lab_shard WHERE job_id = ? LIMIT ?",
                (str(job_id), MAX_JOB_SHARDS + 1),
            ).fetchall()
            if len(shard_count_probe) > MAX_JOB_SHARDS:
                raise InvalidStoredJobError(
                    f"job shard count exceeds authoritative shard limit {MAX_JOB_SHARDS}"
                )
            authoritative_shard_count = len(shard_count_probe)
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
            eta_shard_count = len(completed_rows) + len(remaining_rows)
            if eta_shard_count > MAX_JOB_SHARDS or eta_shard_count > authoritative_shard_count:
                raise InvalidStoredJobError(
                    "ETA shard sample exceeds the authoritative shard count"
                )
            eta_input = self._eta_input_from_rows(
                job_id=job_id,
                status=_effective_lab_eta_status(
                    status=JobStatus(str(job_row["status"])),
                    control_intent=ControlIntent(str(job_row["control_intent"])),
                ),
                as_of=current,
                completed_rows=completed_rows,
                remaining_rows=remaining_rows,
            )
        return eta_input

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

    def __init__(
        self,
        path: Path,
        *,
        busy_timeout_ms: int = 5_000,
        identity_authority: LabSqliteIdentityAuthority | None = None,
        mutation_guard: Callable[[], object] | None = None,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.identity_authority = identity_authority
        self.mutation_guard = mutation_guard
        if identity_authority is not None and identity_authority.path != self.path:
            raise ValueError("SQLite identity authority path mismatch")

    def _connect(self, *, validate_identity: bool = True) -> _LabJobStoreConnection:
        def open_writable(path: Path) -> _LabJobStoreConnection:
            target: Path | str = path
            use_uri = False
            if self.identity_authority is not None:
                target = f"file:{quote(str(path))}?mode=rw"
                use_uri = True
            return sqlite3.connect(
                target,
                uri=use_uri,
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
                factory=_LabJobStoreConnection,
            )

        connection = (
            self.identity_authority.open_verified_connection(open_writable)
            if self.identity_authority is not None
            else open_writable(self.path)
        )
        if not isinstance(connection, _LabJobStoreConnection):
            connection.close()
            raise TypeError("lab SQLite authority returned an incompatible connection")
        authorization = _LabWriteAuthorization(connection)
        connection.identity_authority = self.identity_authority
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
            _CLAIM_PUBLICATION_AUTH_FUNCTION,
            4,
            authorization.claim_publication_authorized,
        )
        connection.create_function(
            _CLAIM_PUBLICATION_AUDIT_AUTH_FUNCTION,
            4,
            authorization.claim_publication_audit_authorized,
        )
        connection.create_function(
            _SHARD_ROW_VALID_FUNCTION,
            32,
            _sqlite_shard_row_valid,
            deterministic=True,
        )
        connection.create_function(
            _PAYLOAD_PROTOCOL_VALID_FUNCTION,
            2,
            _sqlite_payload_protocol_valid,
            deterministic=True,
        )
        connection.create_function(
            _LEDGER_CHAIN_STEP_FUNCTION,
            3,
            _ledger_chain_step,
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
                _validate_current_schema(connection)
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
            _validate_current_schema(connection)
            yield connection
            if self.mutation_guard is not None:
                self.mutation_guard()
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _read_transaction(self) -> Iterator[sqlite3.Connection]:
        """Return one read-only, internally consistent snapshot without a write lock."""

        connection = self._connect()
        try:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN DEFERRED")
            _validate_database_identity(
                connection,
                allow_unclaimed_empty=False,
            )
            _validate_current_schema(connection)
            yield connection
            connection.rollback()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        if self.identity_authority is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect(validate_identity=False)
        try:
            _validate_v16_finalizer_strict_support(connection)
            _validate_database_identity(
                connection,
                allow_unclaimed_empty=True,
                accepted_versions=frozenset(
                    {
                        _LEGACY_SCHEMA_VERSION,
                        _V2_SCHEMA_VERSION,
                        _V3_SCHEMA_VERSION,
                        _V4_SCHEMA_VERSION,
                        _V5_SCHEMA_VERSION,
                        _V6_SCHEMA_VERSION,
                        _V7_SCHEMA_VERSION,
                        _V8_SCHEMA_VERSION,
                        _V9_SCHEMA_VERSION,
                        _V10_SCHEMA_VERSION,
                        _V11_SCHEMA_VERSION,
                        _V12_SCHEMA_VERSION,
                        _V13_SCHEMA_VERSION,
                        _V14_SCHEMA_VERSION,
                        _V15_SCHEMA_VERSION,
                        _SCHEMA_VERSION,
                    }
                ),
            )
            connection.execute("BEGIN IMMEDIATE")
            unclaimed = _validate_database_identity(
                connection,
                allow_unclaimed_empty=True,
                accepted_versions=frozenset(
                    {
                        _LEGACY_SCHEMA_VERSION,
                        _V2_SCHEMA_VERSION,
                        _V3_SCHEMA_VERSION,
                        _V4_SCHEMA_VERSION,
                        _V5_SCHEMA_VERSION,
                        _V6_SCHEMA_VERSION,
                        _V7_SCHEMA_VERSION,
                        _V8_SCHEMA_VERSION,
                        _V9_SCHEMA_VERSION,
                        _V10_SCHEMA_VERSION,
                        _V11_SCHEMA_VERSION,
                        _V12_SCHEMA_VERSION,
                        _V13_SCHEMA_VERSION,
                        _V14_SCHEMA_VERSION,
                        _V15_SCHEMA_VERSION,
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
            elif starting_version == _V4_SCHEMA_VERSION:
                _migrate_v4_to_v5(connection)
            elif starting_version == _V5_SCHEMA_VERSION:
                _validate_v5_schema(connection)
            elif starting_version == _V6_SCHEMA_VERSION:
                _migrate_v6_to_v7(connection)
            elif starting_version == _V7_SCHEMA_VERSION:
                _migrate_v7_to_v8(connection)
                _migrate_v8_to_v9(connection)
            elif starting_version == _V8_SCHEMA_VERSION:
                _migrate_v8_to_v9(connection)
            elif starting_version == _V9_SCHEMA_VERSION:
                _validate_v9_schema(connection)
            elif starting_version == _V10_SCHEMA_VERSION:
                _validate_v10_schema(connection)
            elif starting_version == _V11_SCHEMA_VERSION:
                _validate_v11_schema(connection)
            elif starting_version == _V12_SCHEMA_VERSION:
                _validate_v12_schema(connection)
            elif starting_version == _V13_SCHEMA_VERSION:
                _validate_v13_schema(connection)
            elif starting_version == _V14_SCHEMA_VERSION:
                _validate_v14_schema(connection)
            elif starting_version == _V15_SCHEMA_VERSION:
                _migrate_v15_to_v16(connection)
                _validate_v16_schema(connection)
            elif starting_version == _SCHEMA_VERSION:
                _validate_v16_schema(connection)
            for statement in _SCHEMA_STATEMENTS:
                connection.execute(statement)
            if starting_version != _SCHEMA_VERSION:
                if starting_version not in {
                    _V11_SCHEMA_VERSION,
                    _V12_SCHEMA_VERSION,
                    _V13_SCHEMA_VERSION,
                    _V14_SCHEMA_VERSION,
                    _V15_SCHEMA_VERSION,
                }:
                    if starting_version != _V10_SCHEMA_VERSION:
                        _migrate_v9_to_v10(connection)
                    _migrate_v10_to_v11(connection)
                if starting_version not in {
                    _V12_SCHEMA_VERSION,
                    _V13_SCHEMA_VERSION,
                    _V14_SCHEMA_VERSION,
                    _V15_SCHEMA_VERSION,
                }:
                    _migrate_v11_to_v12(connection)
                if starting_version not in {
                    _V13_SCHEMA_VERSION,
                    _V14_SCHEMA_VERSION,
                    _V15_SCHEMA_VERSION,
                }:
                    _migrate_v12_to_v13(connection)
                if starting_version not in {_V14_SCHEMA_VERSION, _V15_SCHEMA_VERSION}:
                    _migrate_v13_to_v14(connection)
                if starting_version < _V15_SCHEMA_VERSION:
                    _migrate_v14_to_v15(connection)
                _migrate_v15_to_v16(connection)
            _normalize_legacy_terminal_shards(connection)
            if starting_version < _V15_SCHEMA_VERSION:
                _migrate_v14_to_v15(connection)
            if starting_version < _SCHEMA_VERSION:
                _migrate_v15_to_v16(connection)
            _validate_v16_schema(connection)
            connection.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION}")
            if self.mutation_guard is not None:
                self.mutation_guard()
            connection.commit()
            if self.mutation_guard is not None:
                self.mutation_guard()
            journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(journal_mode).lower() != "wal":
                raise LabDatabaseIdentityError("lab jobs SQLite could not enable WAL mode")
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
            _validate_current_schema(connection)
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
                mutation_guard=self.mutation_guard,
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

    def _scheduler_fence_authority(self, connection: sqlite3.Connection) -> dict[str, object]:
        canonical_path = self.path.resolve(strict=True)
        observed = canonical_path.stat(follow_symlinks=False)
        application_id = _strict_sqlite_int(
            connection.execute("PRAGMA application_id").fetchone()[0],
            field="PRAGMA application_id",
            minimum=1,
        )
        schema_version = _strict_sqlite_int(
            connection.execute("PRAGMA user_version").fetchone()[0],
            field="PRAGMA user_version",
            minimum=1,
        )
        implementation_digest = hashlib.sha256(
            canonical_json_bytes(
                {
                    "class": "rquant.lab_jobs.LabJobStore",
                    "claim_contract": "rquant-current-scheduler-fence/v1",
                    "schema_version": _SCHEMA_VERSION,
                }
            )
        ).hexdigest()
        store_id = hashlib.sha256(
            canonical_json_bytes(
                {
                    "canonical_path": str(canonical_path),
                    "database_generation": (observed.st_dev, observed.st_ino),
                    "application_id": application_id,
                    "schema_version": schema_version,
                    "implementation_digest": implementation_digest,
                }
            )
        ).hexdigest()
        return {
            "canonical_job_store_path": str(canonical_path),
            "database_generation": (observed.st_dev, observed.st_ino),
            "store_id": store_id,
            "application_id": application_id,
            "schema_version": schema_version,
            "implementation_digest": implementation_digest,
        }

    def issue_current_scheduler_fence_receipt(
        self,
        *,
        lease: LabLeaseRecord,
        binding: LabSourceStageBinding,
        now: datetime,
    ) -> CurrentSchedulerFenceReceipt:
        current = _utc(now)
        with self._read_transaction() as connection:
            row = self._validate_lease(connection, lease, now=current)
            authority = self._scheduler_fence_authority(connection)
            lease_commitment = _scheduler_fence_lease_commitment(row)
            row_commitment = _scheduler_fence_row_commitment(
                owner_id=lease.owner_id,
                scheduler_fencing_token=lease.fencing_token,
                lease_id=lease.lease_id,
                lease_commitment=lease_commitment,
                issued_at=lease.acquired_at,
                expires_at=lease.expires_at,
            )
            return CurrentSchedulerFenceReceipt(
                **authority,
                binding=binding,
                owner_id=lease.owner_id,
                scheduler_fencing_token=lease.fencing_token,
                lease_id=lease.lease_id,
                lease_commitment=lease_commitment,
                issued_at=lease.acquired_at,
                expires_at=lease.expires_at,
                row_commitment=row_commitment,
                receipt_commitment=_scheduler_fence_receipt_commitment(
                    **authority,
                    binding=binding,
                    row_commitment=row_commitment,
                ),
            )

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
        if job_id in self._jobs_requiring_v2_reconciliation(connection, (job_id,)):
            return job_row
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

    @staticmethod
    def _jobs_requiring_v2_reconciliation(
        connection: sqlite3.Connection,
        job_ids: tuple[UUID, ...],
    ) -> frozenset[UUID]:
        """Return a bounded batch of jobs that generic failure recovery must not mutate."""

        if not job_ids:
            return frozenset()
        unique_ids = tuple(sorted(set(job_ids), key=str))
        placeholders = ", ".join("?" for _ in unique_ids)
        rows = connection.execute(
            f"""
            SELECT DISTINCT shard.job_id
            FROM lab_shard AS shard
            WHERE shard.job_id IN ({placeholders})
              AND (
                  shard.payload_protocol_version = 2
                  OR EXISTS (
                      SELECT 1 FROM lab_claim_publication AS publication
                      WHERE publication.job_id = shard.job_id
                  )
              )
            """,
            tuple(str(job_id) for job_id in unique_ids),
        ).fetchall()
        return frozenset(
            _canonical_uuid_text(row["job_id"], field="lab_shard.job_id") for row in rows
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
                _canonical_model_json(envelope),
                receipt.status,
                receipt.reason,
                _canonical_model_json(receipt),
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
        submission_authority: Callable[[LabCommandEnvelope, datetime], None] | None = None,
    ) -> LabCommandReceipt:
        validated = LabCommandEnvelope.model_validate(envelope)
        current = _utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            existing = self._apply_existing_or_conflict(connection, validated)
            if existing is not None:
                return existing
            command = validated.command
            if isinstance(command, SubmitJobCommand) and command.spec.schema_version == 3:
                if submission_authority is None:
                    raise FormalSubmissionAuthorityError(
                        "formal v3 submission requires authoritative ownership validation"
                    )
                submission_authority(validated, current)
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
        if command.spec.schema_version not in {2, 3}:
            return self._receipt_for_rejection(
                envelope,
                reason="unsupported_spec_version",
                job_version=None,
            )
        if command.spec.schema_version == 3 and not command.spec.catalog_owner_eligible:
            return self._receipt_for_rejection(
                envelope,
                reason="v3_catalog_owner_identity_required",
                job_version=None,
            )
        if command.spec.schema_version == 2 and command.spec.research_status != "exploratory":
            return self._receipt_for_rejection(
                envelope,
                reason="v2_formal_requires_exploratory_migration",
                job_version=None,
            )
        if command.spec.schema_version == 3:
            submission_reason = "submitted_v3_owned"
        else:
            submission_reason = "submitted_legacy_v2_exploratory_non_owner"
        if existing_row is not None:
            return self._receipt_for_rejection(
                envelope,
                reason="job_id_reused",
                job_version=_strict_sqlite_int(
                    existing_row["version"], field="lab_job.version", minimum=0
                ),
            )
        spec_json = _canonical_model_json(command.spec)
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
            reason=submission_reason,
            fencing_token=lease.fencing_token,
            now=now,
        )
        return LabCommandReceipt(
            request_id=envelope.request_id,
            content_hash=envelope.content_hash,
            job_id=command.job_id,
            status="applied",
            reason=submission_reason,
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
        protocol_versions = tuple(_payload_protocol_version(item.payload_json) for item in ordered)
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
            for item, protocol_version in zip(ordered, protocol_versions, strict=True):
                work_plan = item.work_plan
                connection.execute(
                    """
                    INSERT INTO lab_shard (
                        shard_id, job_id, shard_index, status, version,
                        attempt_count, max_attempts, plan_hash, adapter_id,
                        adapter_version, payload_json, payload_hash, payload_protocol_version,
                        worker_id, scheduler_fencing_token, claim_token,
                        claim_generation, claimed_at, heartbeat_at,
                        lease_expires_at, result_manifest_hash, failure_json,
                        finished_at, checkpoint_json, created_at, updated_at,
                        phase, work_unit_name, work_units, static_duration_ms,
                        duration_ms, throughput_units_per_second,
                        completion_sequence
                    ) VALUES (
                        ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?,
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
                        protocol_version,
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

    @staticmethod
    def _external_payload_v2(payload_json: str) -> StrategyShardPayloadV2 | None:
        """Recognize only an explicit V2 payload; all prior payloads remain local V1."""

        try:
            validate_strategy_shard_payload_utf8(payload_json, field="lab_shard.payload_json")
            decoded = strict_json_loads(payload_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(decoded, dict) or decoded.get("schema_version") != 2:
            return None
        try:
            payload = parse_strategy_shard_payload(payload_json)
        except (TypeError, ValueError) as exc:
            raise InvalidStoredJobError(f"invalid v2 strategy shard payload: {exc}") from exc
        if not isinstance(payload, StrategyShardPayloadV2):  # pragma: no cover - parser dispatch
            raise InvalidStoredJobError("v2 strategy shard payload has an invalid protocol")
        return payload

    def _recover_stale_shards_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        lease: LabLeaseRecord,
        now: datetime,
        reclaimed_shards: set[tuple[UUID, int, UUID]] | None = None,
    ) -> set[UUID]:
        stale_rows = connection.execute(
            """
            SELECT s.* FROM lab_shard AS s
            JOIN lab_job AS j ON j.job_id = s.job_id
            WHERE s.status = ?
              AND j.status = ?
              AND s.payload_protocol_version = 1
              AND (
                s.scheduler_fencing_token IS NULL
                OR s.scheduler_fencing_token <> ?
                OR s.lease_expires_at IS NULL
                OR s.lease_expires_at <= ?
              )
            ORDER BY s.job_id, s.shard_index, s.shard_id
            LIMIT ?
            """,
            (
                ShardStatus.RUNNING.value,
                JobStatus.RUNNING.value,
                lease.fencing_token,
                _dump_time(now),
                STALE_RECOVERY_BATCH_SIZE,
            ),
        ).fetchall()
        reclaimed_job_ids: set[UUID] = set()
        paused_job_ids: set[UUID] = set()
        cancelled_job_ids: set[UUID] = set()
        exhausted_by_status: list[list[sqlite3.Row]] = []
        for exhausted_status in (ShardStatus.QUEUED, ShardStatus.CHECKPOINTED):
            exhausted_status_predicate, exhausted_index_name = (
                ("s.status = 'queued'", "ix_lab_shard_exhausted_queued_v1_recovery")
                if exhausted_status is ShardStatus.QUEUED
                else (
                    "s.status = 'checkpointed'",
                    "ix_lab_shard_exhausted_checkpointed_v1_recovery",
                )
            )
            exhausted_by_status.append(
                connection.execute(
                    f"""
                    SELECT s.job_id, s.shard_id, s.shard_index
                    FROM lab_shard AS s INDEXED BY {exhausted_index_name}
                    CROSS JOIN lab_job AS j ON j.job_id = s.job_id
                    WHERE j.status IN (?, ?, ?) AND j.control_intent <> ?
                      AND {exhausted_status_predicate}
                      AND s.attempt_count >= s.max_attempts
                      AND s.payload_protocol_version = 1
                    ORDER BY s.job_id, s.shard_index, s.shard_id
                    LIMIT ?
                    """,
                    (
                        JobStatus.QUEUED.value,
                        JobStatus.RUNNING.value,
                        JobStatus.CHECKPOINTED.value,
                        ControlIntent.CANCEL_REQUESTED.value,
                        STALE_RECOVERY_BATCH_SIZE,
                    ),
                ).fetchall()
            )
        exhausted_share = STALE_RECOVERY_BATCH_SIZE // len(exhausted_by_status)
        exhausted_rows = [
            row for status_rows in exhausted_by_status for row in status_rows[:exhausted_share]
        ]
        exhausted_overflow = sorted(
            (row for status_rows in exhausted_by_status for row in status_rows[exhausted_share:]),
            key=lambda row: (str(row["job_id"]), int(row["shard_index"]), str(row["shard_id"])),
        )
        exhausted_rows.extend(exhausted_overflow[: STALE_RECOVERY_BATCH_SIZE - len(exhausted_rows)])
        exhausted_rows.sort(
            key=lambda row: (str(row["job_id"]), int(row["shard_index"]), str(row["shard_id"]))
        )
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
        reconciliation_job_ids = self._jobs_requiring_v2_reconciliation(
            connection,
            tuple(failed_job_causes),
        )
        for fenced_job_id in reconciliation_job_ids:
            failed_job_causes.pop(fenced_job_id, None)
        for stale in stale_rows:
            job_id = _canonical_uuid_text(stale["job_id"], field="lab_shard.job_id")
            if job_id in reconciliation_job_ids:
                continue
            if job_id in failed_job_causes:
                continue
            if self._stale_v2_shard_requires_publication_reconciliation(stale):
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
                if reclaimed_shards is not None:
                    reclaimed_shards.add(
                        (
                            job_id,
                            _strict_sqlite_int(
                                stale["shard_index"],
                                field="lab_shard.shard_index",
                                minimum=0,
                            ),
                            _canonical_uuid_text(stale["shard_id"], field="lab_shard.shard_id"),
                        )
                    )
        idle_cursor = connection.execute(
            """
            SELECT cursor_created_at, cursor_job_id
            FROM lab_recovery_cursor WHERE cursor_key = 'idle_control'
            """
        ).fetchone()
        idle_after_candidates: list[sqlite3.Row]
        idle_before_candidates: list[sqlite3.Row] = []
        idle_cursor_advance: sqlite3.Row | None = None
        if idle_cursor is None:
            idle_after_candidates = connection.execute(
                """
                SELECT j.job_id, j.control_intent, j.created_at FROM lab_job AS j
                INDEXED BY ix_lab_job_idle_control_recovery
                WHERE j.status = 'running'
                  AND j.control_intent IN ('pause_requested', 'cancel_requested')
                  AND EXISTS (
                      SELECT 1 FROM lab_shard AS planned
                      INDEXED BY ix_lab_shard_idle_control_eligibility
                      WHERE planned.job_id = j.job_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM lab_shard AS active
                      INDEXED BY ix_lab_shard_idle_control_eligibility
                      WHERE active.job_id = j.job_id AND active.status = 'running'
                  )
                ORDER BY j.created_at, j.job_id
                LIMIT ?
                """,
                (IDLE_CONTROL_AFTER_BATCH_SIZE,),
            ).fetchall()
            if idle_after_candidates:
                idle_cursor_advance = idle_after_candidates[-1]
        else:
            try:
                cursor_created_at = _dump_time(_load_time(str(idle_cursor["cursor_created_at"])))
                cursor_job_id = _canonical_uuid_text(
                    idle_cursor["cursor_job_id"],
                    field="lab_recovery_cursor.cursor_job_id",
                )
            except (TypeError, ValueError, InvalidStoredJobError) as exc:
                raise InvalidStoredJobError(
                    "invalid persisted idle-control recovery cursor"
                ) from exc
            idle_after_candidates = connection.execute(
                """
                SELECT j.job_id, j.control_intent, j.created_at FROM lab_job AS j
                INDEXED BY ix_lab_job_idle_control_recovery
                WHERE j.status = 'running'
                  AND j.control_intent IN ('pause_requested', 'cancel_requested')
                  AND EXISTS (
                      SELECT 1 FROM lab_shard AS planned
                      INDEXED BY ix_lab_shard_idle_control_eligibility
                      WHERE planned.job_id = j.job_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM lab_shard AS active
                      INDEXED BY ix_lab_shard_idle_control_eligibility
                      WHERE active.job_id = j.job_id AND active.status = 'running'
                  )
                  AND (j.created_at > ? OR (j.created_at = ? AND j.job_id > ?))
                ORDER BY j.created_at, j.job_id
                LIMIT ?
                """,
                (
                    cursor_created_at,
                    cursor_created_at,
                    str(cursor_job_id),
                    IDLE_CONTROL_AFTER_BATCH_SIZE,
                ),
            ).fetchall()
            if idle_after_candidates:
                idle_cursor_advance = idle_after_candidates[-1]
            idle_before_candidates = connection.execute(
                """
                SELECT j.job_id, j.control_intent, j.created_at FROM lab_job AS j
                INDEXED BY ix_lab_job_idle_control_recovery
                WHERE j.status = 'running'
                  AND j.control_intent IN ('pause_requested', 'cancel_requested')
                  AND EXISTS (
                      SELECT 1 FROM lab_shard AS planned
                      INDEXED BY ix_lab_shard_idle_control_eligibility
                      WHERE planned.job_id = j.job_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM lab_shard AS active
                      INDEXED BY ix_lab_shard_idle_control_eligibility
                      WHERE active.job_id = j.job_id AND active.status = 'running'
                  )
                  AND (j.created_at < ? OR (j.created_at = ? AND j.job_id <= ?))
                ORDER BY j.created_at, j.job_id
                LIMIT ?
                """,
                (
                    cursor_created_at,
                    cursor_created_at,
                    str(cursor_job_id),
                    IDLE_CONTROL_BEFORE_BATCH_SIZE,
                ),
            ).fetchall()
        if idle_cursor_advance is not None:
            connection.execute(
                """
                INSERT INTO lab_recovery_cursor (
                    cursor_key, cursor_created_at, cursor_job_id, updated_at
                ) VALUES ('idle_control', ?, ?, ?)
                ON CONFLICT(cursor_key) DO UPDATE SET
                    cursor_created_at = excluded.cursor_created_at,
                    cursor_job_id = excluded.cursor_job_id,
                    updated_at = excluded.updated_at
                """,
                (
                    str(idle_cursor_advance["created_at"]),
                    str(idle_cursor_advance["job_id"]),
                    _dump_time(now),
                ),
            )
        for row in idle_after_candidates + idle_before_candidates:
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

    def _stale_v2_shard_requires_publication_reconciliation(
        self,
        shard_row: sqlite3.Row,
    ) -> bool:
        """Fence every explicit V2 shard until an explicit reconciler owns its next attempt.

        Generic shard recovery cannot prove that an external source operation stopped,
        including after the publication ledger records ABORTED.  The immutable
        publication record and its audit chain therefore remain the durable
        reconciliation work item instead of permitting an implicit new attempt.
        """

        payload = self._external_payload_v2(str(shard_row["payload_json"]))
        if payload is None:
            return False
        return payload is not None

    @staticmethod
    def _append_claim_publication_audit(
        connection: sqlite3.Connection,
        record: LabClaimPublicationRecord,
        *,
        action: ClaimPublicationAuditAction,
        prior_status: ClaimPublicationStatus | None,
        reason_code: str,
        now: datetime,
    ) -> LabClaimPublicationAuditRecord:
        values: dict[str, object] = {
            "audit_ref": uuid4(),
            "attempt_id": record.identity.attempt_id,
            "action": action,
            "prior_status": prior_status,
            "new_status": record.status,
            "reason_code": reason_code,
            "record_commitment": record.record_commitment,
            "occurred_at": _utc(now),
        }
        provisional = LabClaimPublicationAuditRecord.model_construct(
            **values,
            audit_hash="0" * 64,
        )
        audit = LabClaimPublicationAuditRecord.model_validate(
            {**values, "audit_hash": provisional.recomputed_hash()}
        )
        with _write_authorization(connection).authorize_claim_publication_audit(audit):
            connection.execute(
                """
                INSERT INTO lab_claim_publication_audit (
                    audit_ref, attempt_id, action, prior_status, new_status,
                    reason_code, record_commitment, occurred_at, audit_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(audit.audit_ref),
                    str(audit.attempt_id),
                    audit.action.value,
                    audit.prior_status.value if audit.prior_status is not None else None,
                    audit.new_status.value,
                    audit.reason_code,
                    audit.record_commitment,
                    _dump_time(audit.occurred_at),
                    audit.audit_hash,
                ),
            )
        return audit

    @classmethod
    def _publication_conflict_decision(
        cls,
        connection: sqlite3.Connection,
        record: LabClaimPublicationRecord,
        *,
        reason_code: str,
        now: datetime,
        error_type: type[RuntimeError] = ClaimPublicationConflictError,
    ) -> _ClaimPublicationDecision:
        cls._append_claim_publication_audit(
            connection,
            record,
            action=ClaimPublicationAuditAction.CONFLICT,
            prior_status=record.status,
            reason_code=reason_code,
            now=now,
        )
        return _ClaimPublicationDecision(error=error_type(reason_code))

    @staticmethod
    def _validate_claim_publication_shard_binding(
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        *,
        now: datetime,
    ) -> None:
        row = connection.execute(
            """
            SELECT j.status AS job_status, s.status, s.claim_token, s.claim_generation,
                   s.scheduler_fencing_token, s.worker_id, s.lease_expires_at,
                   s.plan_hash, s.payload_hash, j.spec_hash
            FROM lab_shard AS s
            JOIN lab_job AS j ON j.job_id = s.job_id
            WHERE s.job_id = ? AND s.shard_id = ?
            """,
            (str(identity.job_id), str(identity.shard_id)),
        ).fetchone()
        if row is None:
            raise ClaimPublicationConflictError("attempt_identity_conflict")
        try:
            matches = (
                str(row["job_status"]) == JobStatus.RUNNING.value
                and str(row["status"]) == ShardStatus.RUNNING.value
                and _canonical_uuid_text(
                    row["claim_token"],
                    field="lab_shard.claim_token",
                )
                == identity.claim_token
                and _strict_sqlite_int(
                    row["claim_generation"],
                    field="lab_shard.claim_generation",
                    minimum=1,
                )
                == identity.claim_generation
                and _strict_sqlite_int(
                    row["scheduler_fencing_token"],
                    field="lab_shard.scheduler_fencing_token",
                    minimum=1,
                )
                == identity.scheduler_fencing_token
                and str(row["worker_id"]) == identity.worker_id
                and str(row["spec_hash"]) == identity.spec_hash
                and str(row["plan_hash"]) == identity.plan_hash
                and str(row["payload_hash"]) == identity.payload_hash
                and row["lease_expires_at"] is not None
                and _load_time(str(row["lease_expires_at"])) > _utc(now)
            )
        except InvalidStoredJobError:
            raise
        if not matches:
            raise ClaimPublicationConflictError("attempt_identity_conflict")

    @staticmethod
    def _claim_publication_matches_held(
        record: LabClaimPublicationRecord,
        held: HeldDraft,
        source_stage_authority: LabSourceStageStoreAuthority,
    ) -> bool:
        authority_bytes = canonical_model_json_bytes(source_stage_authority)
        return (
            record.identity == held.identity
            and record.claim_preimage_bytes == held.claim_preimage_bytes
            and record.claim_preimage_hash == held.claim_preimage_hash
            and record.claim_protocol == held.claim_protocol
            and record.claim_protocol_version == held.claim_protocol_version
            and record.source_wait_deadline == held.source_wait_deadline
            and record.publication_deadline == held.publication_deadline
            and record.source_stage_authority_bytes == authority_bytes
            and record.source_stage_authority_hash == hashlib.sha256(authority_bytes).hexdigest()
            and record.status is ClaimPublicationStatus.HELD_SOURCE
        )

    @staticmethod
    def _claim_publication_matches_queue(
        record: LabClaimPublicationRecord,
        binding: QueueBinding,
    ) -> bool:
        return (
            record.source_stage_binding_bytes == binding.source_stage_binding_bytes
            and record.source_stage_binding_hash == binding.source_stage_binding_hash
            and record.source_intent_bytes == binding.source_intent_bytes
            and record.source_intent_hash == binding.source_intent_hash
            and record.source_operation_id == binding.source_operation_id
            and record.source_operation_hash == binding.source_operation_hash
        )

    @staticmethod
    def _claim_publication_matches_ready(
        record: LabClaimPublicationRecord,
        binding: ReadyBinding,
    ) -> bool:
        return (
            record.ready_source_stage_record_bytes == binding.ready_source_stage_record_bytes
            and record.ready_source_stage_record_hash == binding.ready_source_stage_record_hash
            and record.verified_source_outcome_hash == binding.verified_source_outcome_hash
            and record.verified_evidence_chain_hash == binding.verified_evidence_chain_hash
            and record.source_use_plan_bytes == binding.source_use_plan_bytes
            and record.source_use_plan_hash == binding.source_use_plan_hash
            and record.final_claim_bytes == binding.final_claim_bytes
            and record.final_claim_hash == binding.final_claim_hash
            and record.current_claim_receipt_bytes == binding.current_claim_receipt_bytes
            and record.current_claim_receipt_hash == binding.current_claim_receipt_hash
        )

    @staticmethod
    def _claim_publication_matches_receipt(
        record: LabClaimPublicationRecord,
        receipt: PublishReceipt,
    ) -> bool:
        return (
            record.spool_receipt_bytes == receipt.spool_receipt_bytes
            and record.spool_receipt_hash == receipt.spool_receipt_hash
        )

    @staticmethod
    def _publication_values(record: LabClaimPublicationRecord) -> dict[str, object]:
        return {
            name: getattr(record, name)
            for name in type(record).model_fields
            if name != "record_commitment"
        }

    @staticmethod
    def _claim_publication_snapshot_matches(
        record: LabClaimPublicationRecord,
        expected: LabClaimPublicationRecord,
    ) -> bool:
        return (
            record.status is expected.status
            and record.version == expected.version
            and record.record_commitment == expected.record_commitment
            and record.source_stage_authority_hash == expected.source_stage_authority_hash
        )

    def _read_claim_publication_for_external_validation(
        self,
        identity: LabClaimPublicationIdentity,
    ) -> LabClaimPublicationRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM lab_claim_publication WHERE attempt_id = ?",
                (str(identity.attempt_id),),
            ).fetchone()
        if row is None:
            raise ClaimPublicationConflictError("attempt_identity_conflict")
        record = _claim_publication_record_from_row(row)
        if record.identity != identity:
            raise ClaimPublicationConflictError("attempt_identity_conflict")
        return record

    @staticmethod
    def _sqlite_data_version(connection: sqlite3.Connection) -> int:
        row = connection.execute("PRAGMA data_version").fetchone()
        if row is None:
            raise InvalidStoredJobError("PRAGMA data_version did not return a value")
        return _strict_sqlite_int(row[0], field="PRAGMA data_version", minimum=0)

    def _read_ready_claim_publication_snapshot(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        expected: LabClaimPublicationRecord,
        now: datetime,
    ) -> LabClaimPublicationRecord:
        """Read the ready record and its shard identity in one non-blocking snapshot."""

        with self._connect() as connection:
            connection.execute("PRAGMA query_only = ON")
            before_data_version = self._sqlite_data_version(connection)
            connection.execute("BEGIN DEFERRED")
            try:
                row = connection.execute(
                    "SELECT * FROM lab_claim_publication WHERE attempt_id = ?",
                    (str(identity.attempt_id),),
                ).fetchone()
                if row is None:
                    raise ClaimPublicationConflictError("attempt_identity_conflict")
                record = _claim_publication_record_from_row(row)
                if record.identity != identity:
                    raise ClaimPublicationConflictError("attempt_identity_conflict")
                if not self._claim_publication_snapshot_matches(record, expected):
                    raise ClaimPublicationConflictError("publication_cas_conflict")
                self._validate_claim_publication_shard_binding(
                    connection,
                    identity,
                    now=now,
                )
            finally:
                connection.rollback()
            after_data_version = self._sqlite_data_version(connection)
        if after_data_version != before_data_version:
            raise ClaimPublicationConflictError("publication_cas_conflict")
        return record

    def _prevalidate_claim_publication_mutation(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        lease: LabLeaseRecord,
        now: datetime,
        allow_higher_fence_recovery: bool = False,
    ) -> None:
        with self._connect() as connection:
            self._validate_lease(connection, lease, now=now)
        if identity.scheduler_fencing_token != lease.fencing_token and (
            not allow_higher_fence_recovery
            or identity.scheduler_fencing_token > lease.fencing_token
        ):
            raise SchedulerLeaseFencedError("publication_fence_conflict")

    def _publication_replay(
        self,
        connection: sqlite3.Connection,
        record: LabClaimPublicationRecord,
        *,
        reason_code: str,
        now: datetime,
    ) -> _ClaimPublicationDecision:
        audit = self._append_claim_publication_audit(
            connection,
            record,
            action=ClaimPublicationAuditAction.REPLAYED,
            prior_status=record.status,
            reason_code=reason_code,
            now=now,
        )
        return _ClaimPublicationDecision(
            mutation=LabClaimPublicationMutation(
                record=record,
                audit_ref=audit.audit_ref,
                replayed=True,
            )
        )

    @staticmethod
    def _publication_terminal_read(record: LabClaimPublicationRecord) -> _ClaimPublicationDecision:
        return _ClaimPublicationDecision(
            mutation=LabClaimPublicationMutation(
                record=record,
                audit_ref=None,
                replayed=True,
            )
        )

    @classmethod
    def _validate_ready_claim_for_publication_record(
        cls,
        record: LabClaimPublicationRecord,
        identity: LabClaimPublicationIdentity,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        now: datetime,
        allow_published: bool,
    ) -> LabShardClaimV2:
        allowed_statuses = {ClaimPublicationStatus.READY_TO_PUBLISH}
        if allow_published:
            allowed_statuses.add(ClaimPublicationStatus.PUBLISHED)
        if record.status not in allowed_statuses:
            raise InvalidClaimPublicationTransitionError("transition_not_allowed")
        try:
            preimage = strict_model_validate_canonical_json(
                LabShardClaimV2,
                record.claim_preimage_bytes.decode("utf-8"),
            )
            final_claim = strict_model_validate_canonical_json(
                LabShardClaimV2,
                (record.final_claim_bytes or b"").decode("utf-8"),
            )
            stored_plan = strict_model_validate_canonical_json(
                SourceUsePlanV2,
                (record.source_use_plan_bytes or b"").decode("utf-8"),
            )
            stored_receipt = strict_model_validate_canonical_json(
                CurrentClaimConsumptionV2,
                (record.current_claim_receipt_bytes or b"").decode("utf-8"),
            )
        except (TypeError, UnicodeDecodeError, ValueError) as exc:
            raise ClaimPublicationConflictError("ready_binding_conflict") from exc
        if not isinstance(preimage, LabShardClaimV2) or not isinstance(
            final_claim, LabShardClaimV2
        ):
            raise ClaimPublicationConflictError("ready_binding_conflict")
        if not isinstance(stored_plan, SourceUsePlanV2) or not isinstance(
            stored_receipt, CurrentClaimConsumptionV2
        ):
            raise ClaimPublicationConflictError("ready_binding_conflict")
        verified_plan = require_source_use_plan_v2(
            stored_plan,
            keyring=keyring,
            audience=audience,
            now=now,
        )
        current_receipt = require_current_claim_consumption_v2(
            current_claim_authority=current_claim_authority,
            plan=verified_plan,
            keyring=keyring,
            now=now,
        )
        if (
            preimage != LabShardClaimV2.model_validate(preimage, strict=True)
            or LabClaimPublicationIdentity.from_claim(preimage) != identity
            or final_claim != preimage.bind_source_use_plan(verified_plan)
            or stored_receipt != current_receipt
            or stored_receipt.signed_plan != verified_plan
        ):
            raise ClaimPublicationConflictError("ready_binding_conflict")
        return final_claim

    def validate_ready_claim_for_publication(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        now: datetime,
    ) -> LabShardClaimV2:
        validated_identity = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        current = _utc(now)
        phase_one_record = self._read_claim_publication_for_external_validation(validated_identity)
        final_claim = self._validate_ready_claim_for_publication_record(
            phase_one_record,
            validated_identity,
            current_claim_authority=current_claim_authority,
            keyring=keyring,
            audience=audience,
            now=current,
            allow_published=False,
        )
        snapshot_record = self._read_ready_claim_publication_snapshot(
            validated_identity,
            expected=phase_one_record,
            now=current,
        )
        rechecked_record = self._read_claim_publication_for_external_validation(validated_identity)
        if not self._claim_publication_snapshot_matches(rechecked_record, snapshot_record):
            raise ClaimPublicationConflictError("publication_cas_conflict")
        return final_claim

    def validate_published_claim_for_worker(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        now: datetime,
    ) -> LabShardClaimV2:
        """Read-only D validation for a V2 worker before its spool consume."""

        validated_identity = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        current = _utc(now)
        record = self._read_claim_publication_for_external_validation(validated_identity)
        final_claim = self._validate_ready_claim_for_publication_record(
            record,
            validated_identity,
            current_claim_authority=current_claim_authority,
            keyring=keyring,
            audience=audience,
            now=current,
            allow_published=True,
        )
        self._read_ready_claim_publication_snapshot(
            validated_identity,
            expected=record,
            now=current,
        )
        return final_claim

    def _load_claim_publication_for_mutation(
        self,
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        *,
        lease: LabLeaseRecord,
        now: datetime,
        allow_higher_fence_recovery: bool = False,
    ) -> LabClaimPublicationRecord:
        self._validate_lease(connection, lease, now=now)
        if identity.scheduler_fencing_token != lease.fencing_token and (
            not allow_higher_fence_recovery
            or identity.scheduler_fencing_token > lease.fencing_token
        ):
            raise SchedulerLeaseFencedError("publication_fence_conflict")
        row = connection.execute(
            "SELECT * FROM lab_claim_publication WHERE attempt_id = ?",
            (str(identity.attempt_id),),
        ).fetchone()
        if row is None:
            raise ClaimPublicationConflictError("attempt_identity_conflict")
        record = _claim_publication_record_from_row(row)
        if record.identity != identity:
            decision = self._publication_conflict_decision(
                connection,
                record,
                reason_code="attempt_identity_conflict",
                now=now,
            )
            return decision.resolved().record
        return record

    def _update_claim_publication_in_transaction(
        self,
        connection: sqlite3.Connection,
        prior: LabClaimPublicationRecord,
        transitioned: LabClaimPublicationRecord,
        *,
        reason_code: str,
        now: datetime,
    ) -> _ClaimPublicationDecision:
        with _write_authorization(connection).authorize_claim_publication(transitioned):
            cursor = connection.execute(
                """
                UPDATE lab_claim_publication
                SET status = ?, version = ?, source_stage_binding_bytes = ?,
                    source_stage_binding_hash = ?, source_intent_bytes = ?,
                    source_intent_hash = ?, source_operation_id = ?, source_operation_hash = ?,
                    queued_source_stage_record_hash = ?, ready_source_stage_record_bytes = ?,
                    ready_source_stage_record_hash = ?, verified_source_outcome_hash = ?,
                    verified_evidence_chain_hash = ?, source_use_plan_bytes = ?,
                    source_use_plan_hash = ?, final_claim_bytes = ?, final_claim_hash = ?,
                    current_claim_receipt_bytes = ?, current_claim_receipt_hash = ?,
                    spool_receipt_bytes = ?, spool_receipt_hash = ?, updated_at = ?,
                    queued_at = ?, ready_at = ?, published_at = ?, aborted_at = ?,
                    terminal_reason = ?, record_commitment = ?
                WHERE attempt_id = ? AND status = ? AND version = ? AND record_commitment = ?
                    AND source_stage_authority_hash = ?
                """,
                (
                    transitioned.status.value,
                    transitioned.version,
                    transitioned.source_stage_binding_bytes,
                    transitioned.source_stage_binding_hash,
                    transitioned.source_intent_bytes,
                    transitioned.source_intent_hash,
                    transitioned.source_operation_id,
                    transitioned.source_operation_hash,
                    transitioned.queued_source_stage_record_hash,
                    transitioned.ready_source_stage_record_bytes,
                    transitioned.ready_source_stage_record_hash,
                    transitioned.verified_source_outcome_hash,
                    transitioned.verified_evidence_chain_hash,
                    transitioned.source_use_plan_bytes,
                    transitioned.source_use_plan_hash,
                    transitioned.final_claim_bytes,
                    transitioned.final_claim_hash,
                    transitioned.current_claim_receipt_bytes,
                    transitioned.current_claim_receipt_hash,
                    transitioned.spool_receipt_bytes,
                    transitioned.spool_receipt_hash,
                    _dump_time(transitioned.updated_at),
                    _dump_time(transitioned.queued_at) if transitioned.queued_at else None,
                    _dump_time(transitioned.ready_at) if transitioned.ready_at else None,
                    _dump_time(transitioned.published_at) if transitioned.published_at else None,
                    _dump_time(transitioned.aborted_at) if transitioned.aborted_at else None,
                    transitioned.terminal_reason,
                    transitioned.record_commitment,
                    str(prior.identity.attempt_id),
                    prior.status.value,
                    prior.version,
                    prior.record_commitment,
                    prior.source_stage_authority_hash,
                ),
            )
        if cursor.rowcount != 1:
            raise ClaimPublicationConflictError("publication_cas_conflict")
        audit = self._append_claim_publication_audit(
            connection,
            transitioned,
            action=ClaimPublicationAuditAction.TRANSITIONED,
            prior_status=prior.status,
            reason_code=reason_code,
            now=now,
        )
        return _ClaimPublicationDecision(
            mutation=LabClaimPublicationMutation(
                record=transitioned,
                audit_ref=audit.audit_ref,
                replayed=False,
            )
        )

    def _create_held_claim_publication_in_transaction(
        self,
        connection: sqlite3.Connection,
        held: HeldDraft,
        *,
        source_stage_authority: LabSourceStageStoreAuthority,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> _ClaimPublicationDecision:
        """Write an A record without probing the shard table or source systems."""

        if not connection.in_transaction:
            raise RuntimeError("claim publication creation requires an active transaction")
        validated = HeldDraft.model_validate(held.model_dump())
        validated_authority = LabSourceStageStoreAuthority.model_validate(
            source_stage_authority.model_dump()
        )
        authority_bytes = canonical_model_json_bytes(validated_authority)
        authority_hash = hashlib.sha256(authority_bytes).hexdigest()
        current = _utc(now)
        self._validate_lease(connection, lease, now=current)
        if validated.identity.scheduler_fencing_token != lease.fencing_token:
            raise SchedulerLeaseFencedError("publication_fence_conflict")
        existing_row = connection.execute(
            "SELECT * FROM lab_claim_publication WHERE attempt_id = ?",
            (str(validated.identity.attempt_id),),
        ).fetchone()
        if existing_row is not None:
            existing = _claim_publication_record_from_row(existing_row)
            if self._claim_publication_matches_held(existing, validated, validated_authority):
                return self._publication_replay(
                    connection,
                    existing,
                    reason_code="held_source_replay",
                    now=current,
                )
            return self._publication_conflict_decision(
                connection,
                existing,
                reason_code="attempt_content_conflict",
                now=current,
            )
        record = _claim_publication_record_from_values(
            {
                "identity": validated.identity,
                "claim_preimage_bytes": validated.claim_preimage_bytes,
                "claim_preimage_hash": validated.claim_preimage_hash,
                "claim_protocol": validated.claim_protocol,
                "claim_protocol_version": validated.claim_protocol_version,
                "source_wait_deadline": validated.source_wait_deadline,
                "publication_deadline": validated.publication_deadline,
                "source_stage_authority_bytes": authority_bytes,
                "source_stage_authority_hash": authority_hash,
                "status": ClaimPublicationStatus.HELD_SOURCE,
                "version": 0,
                "created_at": current,
                "updated_at": current,
            }
        )
        try:
            with _write_authorization(connection).authorize_claim_publication(record):
                connection.execute(
                    """
                    INSERT INTO lab_claim_publication (
                        attempt_id, job_id, shard_id, claim_token, claim_generation,
                        scheduler_fencing_token, worker_id, spec_hash, plan_hash, payload_hash,
                        claim_preimage_bytes, claim_preimage_hash, claim_protocol,
                        claim_protocol_version, source_wait_deadline, publication_deadline,
                        source_stage_authority_bytes, source_stage_authority_hash,
                        status, version, created_at, updated_at, record_commitment
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?
                    )
                    """,
                    (
                        str(record.identity.attempt_id),
                        str(record.identity.job_id),
                        str(record.identity.shard_id),
                        str(record.identity.claim_token),
                        record.identity.claim_generation,
                        record.identity.scheduler_fencing_token,
                        record.identity.worker_id,
                        record.identity.spec_hash,
                        record.identity.plan_hash,
                        record.identity.payload_hash,
                        record.claim_preimage_bytes,
                        record.claim_preimage_hash,
                        record.claim_protocol,
                        record.claim_protocol_version,
                        _dump_time(record.source_wait_deadline),
                        _dump_time(record.publication_deadline),
                        record.source_stage_authority_bytes,
                        record.source_stage_authority_hash,
                        record.status.value,
                        record.version,
                        _dump_time(record.created_at),
                        _dump_time(record.updated_at),
                        record.record_commitment,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            if "UNIQUE constraint failed" not in str(exc):
                raise
            raise ClaimPublicationConflictError("claim_identity_conflict") from exc
        audit = self._append_claim_publication_audit(
            connection,
            record,
            action=ClaimPublicationAuditAction.CREATED,
            prior_status=None,
            reason_code="attempt_created",
            now=current,
        )
        return _ClaimPublicationDecision(
            mutation=LabClaimPublicationMutation(
                record=record, audit_ref=audit.audit_ref, replayed=False
            )
        )

    def create_held_claim_publication(
        self,
        held: HeldDraft,
        *,
        source_stage_store: LabSourceStageStore,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabClaimPublicationMutation:
        source_stage_authority = LabSourceStageStoreAuthority.model_validate(
            source_stage_store.authority.model_dump()
        )
        with self._transaction() as connection:
            decision = self._create_held_claim_publication_in_transaction(
                connection,
                HeldDraft.model_validate(held.model_dump()),
                source_stage_authority=source_stage_authority,
                lease=lease,
                now=now,
            )
        return decision.resolved()

    @staticmethod
    def _queue_binding_for_identity(
        binding: QueueBinding,
        identity: LabClaimPublicationIdentity,
    ) -> None:
        stage_binding = strict_model_validate_canonical_json(
            LabSourceStageBinding,
            binding.source_stage_binding_bytes.decode("utf-8"),
        )
        if (
            stage_binding.job_id,
            stage_binding.shard_id,
            stage_binding.attempt_id,
            stage_binding.claim_token,
            stage_binding.claim_generation,
            stage_binding.scheduler_fencing_token,
            stage_binding.worker_id,
            stage_binding.spec_hash,
            stage_binding.plan_hash,
        ) != (
            identity.job_id,
            identity.shard_id,
            identity.attempt_id,
            identity.claim_token,
            identity.claim_generation,
            identity.scheduler_fencing_token,
            identity.worker_id,
            identity.spec_hash,
            identity.plan_hash,
        ):
            raise ClaimPublicationConflictError("source_stage_binding_conflict")

    @staticmethod
    def _source_stage_store_for_record(
        record: LabClaimPublicationRecord,
    ) -> LabSourceStageStore:
        authority = source_stage_store_authority_from_canonical_bytes(
            record.source_stage_authority_bytes
        )
        try:
            store = LabSourceStageStore(
                Path(authority.canonical_stage_db_path),
                queue_store_path=Path(authority.canonical_queue_db_path),
            )
            observed = store.authority
            if (
                observed.model_dump(mode="python") != authority.model_dump(mode="python")
                or observed.authority_hash != authority.authority_hash
            ):
                raise ValueError("source-stage authority identity changed")
            return store
        except Exception as exc:
            raise ClaimPublicationConflictError("source_stage_authority_conflict") from exc

    @classmethod
    def _require_queued_source_stage(
        cls,
        record: LabClaimPublicationRecord,
        binding: QueueBinding,
        identity: LabClaimPublicationIdentity,
    ) -> str:
        cls._queue_binding_for_identity(binding, identity)
        stage_binding = strict_model_validate_canonical_json(
            LabSourceStageBinding,
            binding.source_stage_binding_bytes.decode("utf-8"),
        )
        stage_record = cls._source_stage_store_for_record(record).get(stage_binding)
        if (
            stage_record is None
            or stage_record.state is not LabSourceStageState.QUEUED
            or stage_record.binding != stage_binding
            or stage_record.intent_bytes != binding.source_intent_bytes
            or stage_record.intent_hash != binding.source_intent_hash
            or stage_record.operation_id != binding.source_operation_id
            or stage_record.operation_hash != binding.source_operation_hash
            or stage_record.record_hash == "0" * 64
        ):
            raise ClaimPublicationConflictError("queued_source_stage_conflict")
        return stage_record.record_hash

    def _queue_claim_publication_in_transaction(
        self,
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        binding: QueueBinding,
        *,
        expected: LabClaimPublicationRecord,
        queued_stage_record_hash: str | None,
        lease: LabLeaseRecord,
        now: datetime,
        allow_higher_fence_recovery: bool,
    ) -> _ClaimPublicationDecision:
        if not connection.in_transaction:
            raise RuntimeError("claim publication queue requires an active transaction")
        current = _utc(now)
        validated = QueueBinding.model_validate(binding.model_dump())
        record = self._load_claim_publication_for_mutation(
            connection,
            identity,
            lease=lease,
            now=current,
            allow_higher_fence_recovery=allow_higher_fence_recovery,
        )
        if not self._claim_publication_snapshot_matches(record, expected):
            if (
                record.status is ClaimPublicationStatus.SOURCE_QUEUED
                and self._claim_publication_matches_queue(record, validated)
            ):
                return self._publication_replay(
                    connection, record, reason_code="source_queued_replay", now=current
                )
            raise ClaimPublicationConflictError("publication_cas_conflict")
        if record.status is ClaimPublicationStatus.SOURCE_QUEUED:
            if self._claim_publication_matches_queue(record, validated):
                return self._publication_replay(
                    connection, record, reason_code="source_queued_replay", now=current
                )
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="source_queued_content_conflict",
                now=current,
            )
        if record.status in {ClaimPublicationStatus.PUBLISHED, ClaimPublicationStatus.ABORTED}:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="terminal_status_immutable",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        if record.status is not ClaimPublicationStatus.HELD_SOURCE:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="transition_not_allowed",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        self._validate_claim_publication_shard_binding(
            connection,
            identity,
            now=current,
        )
        if queued_stage_record_hash is None:
            raise RuntimeError("queued source stage validation was not completed")
        operation_row = connection.execute(
            "SELECT * FROM lab_claim_publication WHERE source_operation_id = ?",
            (validated.source_operation_id,),
        ).fetchone()
        if operation_row is not None and str(operation_row["attempt_id"]) != str(
            identity.attempt_id
        ):
            other = _claim_publication_record_from_row(operation_row)
            return self._publication_conflict_decision(
                connection,
                other,
                reason_code="source_operation_conflict",
                now=current,
            )
        values = self._publication_values(record)
        values.update(
            {
                **validated.model_dump(mode="python"),
                "queued_source_stage_record_hash": queued_stage_record_hash,
                "status": ClaimPublicationStatus.SOURCE_QUEUED,
                "version": 1,
                "updated_at": current,
                "queued_at": current,
            }
        )
        return self._update_claim_publication_in_transaction(
            connection,
            record,
            _claim_publication_record_from_values(values),
            reason_code="held_source_to_source_queued",
            now=current,
        )

    def queue_claim_publication(
        self,
        identity: LabClaimPublicationIdentity,
        binding: QueueBinding,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabClaimPublicationMutation:
        return self._queue_claim_publication(
            identity,
            binding,
            lease=lease,
            now=now,
            allow_higher_fence_recovery=False,
        )

    def _queue_claim_publication_after_scheduler_takeover(
        self,
        identity: LabClaimPublicationIdentity,
        binding: QueueBinding,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabClaimPublicationMutation:
        return self._queue_claim_publication(
            identity,
            binding,
            lease=lease,
            now=now,
            allow_higher_fence_recovery=True,
        )

    def _queue_claim_publication(
        self,
        identity: LabClaimPublicationIdentity,
        binding: QueueBinding,
        *,
        lease: LabLeaseRecord,
        now: datetime,
        allow_higher_fence_recovery: bool,
    ) -> LabClaimPublicationMutation:
        validated_identity = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        validated_binding = QueueBinding.model_validate(binding.model_dump())
        phase_one_record = self._read_claim_publication_for_external_validation(validated_identity)
        self._prevalidate_claim_publication_mutation(
            validated_identity,
            lease=lease,
            now=now,
            allow_higher_fence_recovery=allow_higher_fence_recovery,
        )
        queued_stage_record_hash: str | None = None
        if phase_one_record.status is ClaimPublicationStatus.HELD_SOURCE:
            queued_stage_record_hash = self._require_queued_source_stage(
                phase_one_record,
                validated_binding,
                validated_identity,
            )
        with self._transaction() as connection:
            decision = self._queue_claim_publication_in_transaction(
                connection,
                validated_identity,
                validated_binding,
                expected=phase_one_record,
                queued_stage_record_hash=queued_stage_record_hash,
                lease=lease,
                now=now,
                allow_higher_fence_recovery=allow_higher_fence_recovery,
            )
        return decision.resolved()

    @classmethod
    def _ready_binding_for_record(
        cls,
        record: LabClaimPublicationRecord,
        signed_plan: SourceUsePlanV2,
        final_bound_claim: LabShardClaimV2,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        now: datetime,
    ) -> ReadyBinding:
        plan = SourceUsePlanV2.model_validate(signed_plan.model_dump())
        final_claim = LabShardClaimV2.model_validate(final_bound_claim.model_dump())
        preimage = strict_model_validate_canonical_json(
            LabShardClaimV2,
            record.claim_preimage_bytes.decode("utf-8"),
        )
        queued_stage_binding = strict_model_validate_canonical_json(
            LabSourceStageBinding,
            (record.source_stage_binding_bytes or b"").decode("utf-8"),
        )
        stage_record = cls._source_stage_store_for_record(record).get(queued_stage_binding)
        if stage_record is None:
            raise ClaimPublicationConflictError("ready_source_stage_conflict")
        stage_record_bytes = canonical_job_model_bytes(stage_record)
        if (
            stage_record.state is not LabSourceStageState.READY
            or stage_record.ready_at is None
            or stage_record.ready_at > now
            or stage_record.record_hash == "0" * 64
            or stage_record.binding != queued_stage_binding
            or stage_record.intent_bytes != record.source_intent_bytes
            or stage_record.intent_hash != record.source_intent_hash
            or stage_record.operation_id != record.source_operation_id
            or stage_record.operation_hash != record.source_operation_hash
            or stage_record.outcome is None
            or stage_record.outcome.status is not SourceBrokerV2JobOutcomeStatus.SUCCESS
        ):
            raise ClaimPublicationConflictError("ready_source_stage_conflict")
        verified_plan = require_source_use_plan_v2(
            plan,
            keyring=keyring,
            audience=audience,
            now=now,
        )
        receipt = require_current_claim_consumption_v2(
            current_claim_authority=current_claim_authority,
            plan=verified_plan,
            keyring=keyring,
            now=now,
        )
        receipt_bytes = canonical_model_json_bytes(receipt)
        if (
            verified_plan != plan
            or verified_plan.operation_id != record.source_operation_id
            or verified_plan.attempt_binding != preimage.attempt_binding
            or verified_plan.lease_expires_at != preimage.lease_expires_at
            or current_claim_authority.authority_id != verified_plan.single_use_authority_id
            or final_claim != preimage.bind_source_use_plan(verified_plan)
            or receipt.signed_plan != verified_plan
            or receipt.committed_at < stage_record.ready_at
        ):
            raise ClaimPublicationConflictError("ready_binding_conflict")
        return ReadyBinding(
            ready_source_stage_record_bytes=stage_record_bytes,
            ready_source_stage_record_hash=hashlib.sha256(stage_record_bytes).hexdigest(),
            verified_source_outcome_hash=stage_record.outcome.outcome_hash,
            verified_evidence_chain_hash=stage_record.outcome.evidence_chain_hash,
            source_use_plan_bytes=canonical_model_json_bytes(verified_plan),
            source_use_plan_hash=hashlib.sha256(
                canonical_model_json_bytes(verified_plan)
            ).hexdigest(),
            final_claim_bytes=canonical_model_json_bytes(final_claim),
            final_claim_hash=hashlib.sha256(canonical_model_json_bytes(final_claim)).hexdigest(),
            current_claim_receipt_bytes=receipt_bytes,
            current_claim_receipt_hash=hashlib.sha256(receipt_bytes).hexdigest(),
        )

    def _mark_claim_publication_ready_in_transaction(
        self,
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        *,
        expected: LabClaimPublicationRecord,
        ready_binding: ReadyBinding | None,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> _ClaimPublicationDecision:
        if not connection.in_transaction:
            raise RuntimeError("claim publication ready requires an active transaction")
        current = _utc(now)
        record = self._load_claim_publication_for_mutation(
            connection, identity, lease=lease, now=current
        )
        if not self._claim_publication_snapshot_matches(record, expected):
            if (
                record.status is ClaimPublicationStatus.READY_TO_PUBLISH
                and ready_binding is not None
                and self._claim_publication_matches_ready(record, ready_binding)
            ):
                self._validate_claim_publication_shard_binding(
                    connection,
                    identity,
                    now=current,
                )
                return self._publication_replay(
                    connection, record, reason_code="ready_to_publish_replay", now=current
                )
            raise ClaimPublicationConflictError("publication_cas_conflict")
        if record.status in {ClaimPublicationStatus.PUBLISHED, ClaimPublicationStatus.ABORTED}:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="terminal_status_immutable",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        if record.status not in {
            ClaimPublicationStatus.SOURCE_QUEUED,
            ClaimPublicationStatus.READY_TO_PUBLISH,
        }:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="transition_not_allowed",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        self._validate_claim_publication_shard_binding(
            connection,
            identity,
            now=current,
        )
        if ready_binding is None:
            raise RuntimeError("ready publication validation was not completed")
        validated = ready_binding
        if record.status is ClaimPublicationStatus.READY_TO_PUBLISH:
            if self._claim_publication_matches_ready(record, validated):
                return self._publication_replay(
                    connection, record, reason_code="ready_to_publish_replay", now=current
                )
            return self._publication_conflict_decision(
                connection, record, reason_code="ready_content_conflict", now=current
            )
        values = self._publication_values(record)
        values.update(
            {
                **validated.model_dump(mode="python"),
                "status": ClaimPublicationStatus.READY_TO_PUBLISH,
                "version": 2,
                "updated_at": current,
                "ready_at": current,
            }
        )
        return self._update_claim_publication_in_transaction(
            connection,
            record,
            _claim_publication_record_from_values(values),
            reason_code="source_queued_to_ready_to_publish",
            now=current,
        )

    def mark_claim_publication_ready(
        self,
        identity: LabClaimPublicationIdentity,
        signed_plan: SourceUsePlanV2,
        final_bound_claim: LabShardClaimV2,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabClaimPublicationMutation:
        validated_identity = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        validated_plan = SourceUsePlanV2.model_validate(signed_plan.model_dump())
        validated_final_claim = LabShardClaimV2.model_validate(final_bound_claim.model_dump())
        current = _utc(now)
        phase_one_record = self._read_claim_publication_for_external_validation(validated_identity)
        self._prevalidate_claim_publication_mutation(
            validated_identity,
            lease=lease,
            now=current,
        )
        ready_binding: ReadyBinding | None = None
        if phase_one_record.status in {
            ClaimPublicationStatus.SOURCE_QUEUED,
            ClaimPublicationStatus.READY_TO_PUBLISH,
        }:
            ready_binding = self._ready_binding_for_record(
                phase_one_record,
                validated_plan,
                validated_final_claim,
                current_claim_authority=current_claim_authority,
                keyring=keyring,
                audience=audience,
                now=current,
            )
        with self._transaction() as connection:
            decision = self._mark_claim_publication_ready_in_transaction(
                connection,
                validated_identity,
                expected=phase_one_record,
                ready_binding=ready_binding,
                lease=lease,
                now=now,
            )
        return decision.resolved()

    def _publish_claim_publication_in_transaction(
        self,
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        spool_receipt: PublishReceipt,
        *,
        expected: LabClaimPublicationRecord,
        validated_ready_claim: LabShardClaimV2 | None,
        spool_receipt_verifier: LabClaimSpoolReceiptVerifier | None,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> _ClaimPublicationDecision:
        if not connection.in_transaction:
            raise RuntimeError("claim publication publish requires an active transaction")
        current = _utc(now)
        validated = PublishReceipt.model_validate(spool_receipt.model_dump())
        terminal_row = connection.execute(
            "SELECT * FROM lab_claim_publication WHERE attempt_id = ?",
            (str(identity.attempt_id),),
        ).fetchone()
        if terminal_row is not None:
            terminal_record = _claim_publication_record_from_row(terminal_row)
            if terminal_record.identity != identity:
                raise ClaimPublicationConflictError("attempt_identity_conflict")
            if terminal_record.status is ClaimPublicationStatus.PUBLISHED:
                terminal_claim = strict_model_validate_canonical_json(
                    LabShardClaimV2,
                    terminal_record.final_claim_bytes or b"",
                )
                require_v2_spool_receipt_provenance(
                    validated,
                    final_claim=terminal_claim,
                    verifier=spool_receipt_verifier,
                )
                if self._claim_publication_matches_receipt(terminal_record, validated):
                    return self._publication_terminal_read(terminal_record)
                return self._publication_conflict_decision(
                    connection,
                    terminal_record,
                    reason_code="published_receipt_conflict",
                    now=current,
                )
            if not self._claim_publication_snapshot_matches(terminal_record, expected):
                raise ClaimPublicationConflictError("publication_cas_conflict")
        record = self._load_claim_publication_for_mutation(
            connection, identity, lease=lease, now=current
        )
        if not self._claim_publication_snapshot_matches(record, expected):
            raise ClaimPublicationConflictError("publication_cas_conflict")
        if record.status is ClaimPublicationStatus.ABORTED:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="terminal_status_immutable",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        if record.status is not ClaimPublicationStatus.READY_TO_PUBLISH:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="transition_not_allowed",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        if validated_ready_claim is None:
            raise RuntimeError("ready claim validation was not completed")
        require_v2_spool_receipt_provenance(
            validated,
            final_claim=validated_ready_claim,
            verifier=spool_receipt_verifier,
        )
        self._validate_claim_publication_shard_binding(
            connection,
            identity,
            now=current,
        )
        values = self._publication_values(record)
        values.update(
            {
                **validated.model_dump(mode="python"),
                "status": ClaimPublicationStatus.PUBLISHED,
                "version": 3,
                "updated_at": current,
                "published_at": current,
            }
        )
        return self._update_claim_publication_in_transaction(
            connection,
            record,
            _claim_publication_record_from_values(values),
            reason_code="ready_to_publish_to_published",
            now=current,
        )

    def publish_claim_publication(
        self,
        identity: LabClaimPublicationIdentity,
        spool_receipt: PublishReceipt,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        lease: LabLeaseRecord,
        now: datetime,
        spool_receipt_verifier: LabClaimSpoolReceiptVerifier | None = None,
    ) -> LabClaimPublicationMutation:
        validated_identity = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        validated_receipt = PublishReceipt.model_validate(spool_receipt.model_dump())
        current = _utc(now)
        phase_one_record = self._read_claim_publication_for_external_validation(validated_identity)
        if phase_one_record.status is not ClaimPublicationStatus.PUBLISHED:
            self._prevalidate_claim_publication_mutation(
                validated_identity,
                lease=lease,
                now=current,
            )
        validated_ready_claim: LabShardClaimV2 | None = None
        if phase_one_record.status is ClaimPublicationStatus.READY_TO_PUBLISH:
            validated_ready_claim = self._validate_ready_claim_for_publication_record(
                phase_one_record,
                validated_identity,
                current_claim_authority=current_claim_authority,
                keyring=keyring,
                audience=audience,
                now=current,
                allow_published=False,
            )
        elif phase_one_record.status is ClaimPublicationStatus.PUBLISHED:
            final_claim = strict_model_validate_canonical_json(
                LabShardClaimV2,
                phase_one_record.final_claim_bytes or b"",
            )
            require_v2_spool_receipt_provenance(
                validated_receipt,
                final_claim=final_claim,
                verifier=spool_receipt_verifier,
            )
        with self._transaction() as connection:
            decision = self._publish_claim_publication_in_transaction(
                connection,
                validated_identity,
                validated_receipt,
                expected=phase_one_record,
                validated_ready_claim=validated_ready_claim,
                spool_receipt_verifier=spool_receipt_verifier,
                lease=lease,
                now=now,
            )
        return decision.resolved()

    @staticmethod
    def _finalizer_authority_binding(
        connection: sqlite3.Connection, *, path: Path
    ) -> dict[str, object]:
        canonical_path = path.resolve(strict=True)
        observed = canonical_path.stat(follow_symlinks=False)
        application_id = _strict_sqlite_int(
            connection.execute("PRAGMA application_id").fetchone()[0],
            field="PRAGMA application_id",
            minimum=1,
        )
        schema_version = _strict_sqlite_int(
            connection.execute("PRAGMA user_version").fetchone()[0],
            field="PRAGMA user_version",
            minimum=1,
        )
        implementation_digest = hashlib.sha256(
            canonical_json_bytes(
                {
                    "class": "rquant.lab_jobs.LabJobStore",
                    "claim_contract": "rquant-claim-publication-finalizer-authority/v2",
                    "schema_version": _SCHEMA_VERSION,
                }
            )
        ).hexdigest()
        generation = (observed.st_dev, observed.st_ino)
        store_id = hashlib.sha256(
            canonical_json_bytes(
                {
                    "canonical_path": str(canonical_path),
                    "database_generation": generation,
                    "application_id": application_id,
                    "schema_version": schema_version,
                    "implementation_digest": implementation_digest,
                }
            )
        ).hexdigest()
        return {
            "canonical_job_store_path": str(canonical_path),
            "database_generation": generation,
            "store_id": store_id,
            "schema_version": schema_version,
            "implementation_digest": implementation_digest,
        }

    @staticmethod
    def _finalizer_authority_commitment(
        *,
        store_id: str,
        owner_id: str,
        lease_id: int,
        fencing_token: int,
        root_descriptor: str,
        root_key_digest: str,
        acquired_at: datetime,
        expires_at: datetime,
    ) -> str:
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "contract": "rquant-claim-publication-finalizer-lease/v2",
                    "store_id": store_id,
                    "owner_id": owner_id,
                    "lease_id": lease_id,
                    "fencing_token": fencing_token,
                    "root_descriptor": root_descriptor,
                    "root_key_digest": root_key_digest,
                    "acquired_at": _dump_time(_utc(acquired_at)),
                    "expires_at": _dump_time(_utc(expires_at)),
                }
            )
        ).hexdigest()

    @staticmethod
    def _finalizer_authority_mac_payload(
        *,
        binding: Mapping[str, object],
        owner_id: str,
        lease_id: int,
        fencing_token: int,
        acquired_at: datetime,
        expires_at: datetime,
        lease_commitment: str,
    ) -> bytes:
        return canonical_json_bytes(
            {
                "contract": "rquant-claim-publication-finalizer-authority/v3",
                "store_id": binding["store_id"],
                "database_generation": binding["database_generation"],
                "implementation_digest": binding["implementation_digest"],
                "owner_id": owner_id,
                "lease_id": lease_id,
                "fencing_token": fencing_token,
                "acquired_at": _dump_time(_utc(acquired_at)),
                "expires_at": _dump_time(_utc(expires_at)),
                "lease_commitment": lease_commitment,
            }
        )

    @staticmethod
    def _require_finalizer_root_anchor(
        connection: sqlite3.Connection,
        root_key: LabClaimPublicationFinalizerRootKey,
    ) -> None:
        row = connection.execute(
            "SELECT root_descriptor, root_key_digest "
            "FROM lab_claim_publication_finalizer_root_anchor WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise ClaimPublicationConflictError("finalizer_root_unbootstrapped")
        if (
            str(row["root_descriptor"]) != root_key.descriptor
            or str(row["root_key_digest"]) != root_key.key_digest
        ):
            raise ClaimPublicationConflictError("finalizer_root_conflict")

    def _acquire_claim_publication_finalizer_authority(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        root_key: LabClaimPublicationFinalizerRootKey,
        trust_certificate: LabClaimFinalizerTrustCertificate,
        trust_verifier: LabClaimFinalizerTrustVerifier,
        runtime_signer: object,
        now: datetime,
    ) -> LabClaimPublicationFinalizerAuthority:
        """Issue the sole durable C/D capability; not part of Scheduler composition."""

        owner = owner_id.strip()
        if not owner or lease_seconds < 1:
            raise ValueError("finalizer authority owner and lease must be valid")
        current = _utc(now)
        if type(root_key) is not LabClaimPublicationFinalizerRootKey:
            raise TypeError("finalizer authority requires an exact root key")
        with self._transaction() as connection:
            binding = self._finalizer_authority_binding(connection, path=self.path)
            try:
                trust_verifier.require_certificate(
                    trust_certificate,
                    store_id=str(binding["store_id"]),
                    database_generation=binding["database_generation"],  # type: ignore[arg-type]
                    schema_version=int(binding["schema_version"]),
                    now=current,
                )
                trust_verifier.require_runtime_signer(trust_certificate, runtime_signer)  # type: ignore[arg-type]
            except (LabClaimFinalizerTrustError, TypeError, ValueError) as exc:
                raise ClaimPublicationConflictError("finalizer_external_trust_invalid") from exc
            certificate_bytes = canonical_model_json_bytes(trust_certificate)
            connection.execute(
                """
                INSERT INTO lab_claim_publication_finalizer_trust_cache (
                    singleton, certificate_bytes, certificate_hash, cached_at
                ) VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    certificate_bytes = excluded.certificate_bytes,
                    certificate_hash = excluded.certificate_hash,
                    cached_at = excluded.cached_at
                """,
                (
                    certificate_bytes,
                    hashlib.sha256(certificate_bytes).hexdigest(),
                    _dump_time(current),
                ),
            )
            existing = connection.execute(
                "SELECT * FROM lab_claim_publication_finalizer_lease WHERE singleton = 1"
            ).fetchone()
            if (
                existing is not None
                and existing["released_at"] is None
                and _load_time(str(existing["expires_at"])) > current
                and str(existing["owner_id"]) != owner
            ):
                raise ClaimPublicationConflictError("finalizer_authority_unavailable")
            if (
                existing is not None
                and existing["released_at"] is None
                and _load_time(str(existing["expires_at"])) > current
                and str(existing["owner_id"]) == owner
            ):
                descriptor = str(existing["root_descriptor"])
                digest = str(existing["token_commitment"])
                if descriptor != root_key.descriptor or digest != root_key.key_digest:
                    raise ClaimPublicationConflictError("finalizer_authority_root_conflict")
                acquired_at = _load_time(str(existing["acquired_at"]))
                expires_at = _load_time(str(existing["expires_at"]))
                lease_id = _strict_sqlite_int(
                    existing["lease_id"], field="finalizer.lease_id", minimum=1
                )
                fence = _strict_sqlite_int(
                    existing["fencing_token"], field="finalizer.fencing_token", minimum=1
                )
                commitment = str(existing["lease_commitment"])
                authority_mac = root_key.sign(
                    self._finalizer_authority_mac_payload(
                        binding=binding,
                        owner_id=owner,
                        lease_id=lease_id,
                        fencing_token=fence,
                        acquired_at=acquired_at,
                        expires_at=expires_at,
                        lease_commitment=commitment,
                    )
                )
                return LabClaimPublicationFinalizerAuthority(
                    **binding,
                    owner_id=owner,
                    lease_id=lease_id,
                    fencing_token=fence,
                    root_key=root_key,
                    expires_at=expires_at,
                    lease_commitment=commitment,
                    authority_mac=authority_mac,
                    trust_certificate=trust_certificate,
                    trust_verifier=trust_verifier,
                    runtime_signer=runtime_signer,
                )
            prior_fence = (
                0
                if existing is None
                else _strict_sqlite_int(
                    existing["fencing_token"],
                    field="lab_claim_publication_finalizer_lease.fencing_token",
                    minimum=1,
                )
            )
            prior_lease_id = (
                0
                if existing is None
                else _strict_sqlite_int(
                    existing["lease_id"],
                    field="lab_claim_publication_finalizer_lease.lease_id",
                    minimum=1,
                )
            )
            lease_id = prior_lease_id + 1
            fence = prior_fence + 1
            expires_at = current + timedelta(seconds=lease_seconds)
            commitment = self._finalizer_authority_commitment(
                store_id=str(binding["store_id"]),
                owner_id=owner,
                lease_id=lease_id,
                fencing_token=fence,
                root_descriptor=root_key.descriptor,
                root_key_digest=root_key.key_digest,
                acquired_at=current,
                expires_at=expires_at,
            )
            authority_mac = root_key.sign(
                self._finalizer_authority_mac_payload(
                    binding=binding,
                    owner_id=owner,
                    lease_id=lease_id,
                    fencing_token=fence,
                    acquired_at=current,
                    expires_at=expires_at,
                    lease_commitment=commitment,
                )
            )
            connection.execute(
                """
                INSERT INTO lab_claim_publication_finalizer_lease (
                    singleton, canonical_job_store_path, database_device, database_inode,
                    store_id, schema_version, implementation_digest, owner_id, lease_id,
                    fencing_token, root_descriptor, token_commitment, lease_commitment, acquired_at,
                    heartbeat_at, expires_at, released_at
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(singleton) DO UPDATE SET
                    canonical_job_store_path = excluded.canonical_job_store_path,
                    database_device = excluded.database_device,
                    database_inode = excluded.database_inode,
                    store_id = excluded.store_id,
                    schema_version = excluded.schema_version,
                    implementation_digest = excluded.implementation_digest,
                    owner_id = excluded.owner_id,
                    lease_id = excluded.lease_id,
                    fencing_token = excluded.fencing_token,
                    root_descriptor = excluded.root_descriptor,
                    token_commitment = excluded.token_commitment,
                    lease_commitment = excluded.lease_commitment,
                    acquired_at = excluded.acquired_at,
                    heartbeat_at = excluded.heartbeat_at,
                    expires_at = excluded.expires_at,
                    released_at = NULL
                """,
                (
                    binding["canonical_job_store_path"],
                    binding["database_generation"][0],
                    binding["database_generation"][1],
                    binding["store_id"],
                    binding["schema_version"],
                    binding["implementation_digest"],
                    owner,
                    lease_id,
                    fence,
                    root_key.descriptor,
                    root_key.key_digest,
                    commitment,
                    _dump_time(current),
                    _dump_time(current),
                    _dump_time(expires_at),
                ),
            )
        return LabClaimPublicationFinalizerAuthority(
            **binding,
            owner_id=owner,
            lease_id=lease_id,
            fencing_token=fence,
            root_key=root_key,
            expires_at=expires_at,
            lease_commitment=commitment,
            authority_mac=authority_mac,
            trust_certificate=trust_certificate,
            trust_verifier=trust_verifier,
            runtime_signer=runtime_signer,
        )

    def _renew_claim_publication_finalizer_authority(
        self,
        authority: LabClaimPublicationFinalizerAuthority,
        *,
        lease_seconds: int,
        now: datetime,
    ) -> LabClaimPublicationFinalizerAuthority:
        if lease_seconds < 1:
            raise ValueError("finalizer authority lease must be positive")
        current = _utc(now)
        with self._transaction() as connection:
            self._require_claim_publication_finalizer_authority(connection, authority, now=current)
            binding = self._finalizer_authority_binding(connection, path=self.path)
            expires_at = current + timedelta(seconds=lease_seconds)
            root_row = connection.execute(
                "SELECT root_descriptor, token_commitment, acquired_at "
                "FROM lab_claim_publication_finalizer_lease WHERE singleton = 1"
            ).fetchone()
            root_descriptor = str(root_row["root_descriptor"])
            root_key_digest = str(root_row["token_commitment"])
            acquired_at = _load_time(str(root_row["acquired_at"]))
            commitment = self._finalizer_authority_commitment(
                store_id=str(binding["store_id"]),
                owner_id=authority.owner_id,
                lease_id=authority.lease_id,
                fencing_token=authority.fencing_token,
                root_descriptor=root_descriptor,
                root_key_digest=root_key_digest,
                acquired_at=acquired_at,
                expires_at=expires_at,
            )
            connection.execute(
                """
                UPDATE lab_claim_publication_finalizer_lease
                SET heartbeat_at = ?, expires_at = ?, lease_commitment = ?
                WHERE singleton = 1
                """,
                (_dump_time(current), _dump_time(expires_at), commitment),
            )
        authority_mac = authority._root_key.sign(
            self._finalizer_authority_mac_payload(
                binding=binding,
                owner_id=authority.owner_id,
                lease_id=authority.lease_id,
                fencing_token=authority.fencing_token,
                acquired_at=acquired_at,
                expires_at=expires_at,
                lease_commitment=commitment,
            )
        )
        return LabClaimPublicationFinalizerAuthority(
            **binding,
            owner_id=authority.owner_id,
            lease_id=authority.lease_id,
            fencing_token=authority.fencing_token,
            root_key=authority._root_key,
            expires_at=expires_at,
            lease_commitment=commitment,
            authority_mac=authority_mac,
            trust_certificate=authority._trust_certificate,
            trust_verifier=authority._trust_verifier,
            runtime_signer=authority._runtime_signer,
        )

    def _release_claim_publication_finalizer_authority(
        self,
        authority: LabClaimPublicationFinalizerAuthority,
        *,
        now: datetime,
    ) -> None:
        current = _utc(now)
        with self._transaction() as connection:
            self._require_claim_publication_finalizer_authority(connection, authority, now=current)
            connection.execute(
                """
                UPDATE lab_claim_publication_finalizer_lease
                SET released_at = ? WHERE singleton = 1
                """,
                (_dump_time(current),),
            )

    def _require_claim_publication_finalizer_authority(
        self,
        connection: sqlite3.Connection,
        authority: LabClaimPublicationFinalizerAuthority,
        *,
        now: datetime,
    ) -> None:
        if type(authority) is not LabClaimPublicationFinalizerAuthority:
            raise ClaimPublicationConflictError("finalizer_authority_conflict")
        binding = self._finalizer_authority_binding(connection, path=self.path)
        if authority._trust_certificate is None or authority._trust_verifier is None:
            raise ClaimPublicationConflictError("finalizer_external_trust_invalid")
        try:
            authority._trust_verifier.require_certificate(
                authority._trust_certificate,
                store_id=str(binding["store_id"]),
                database_generation=binding["database_generation"],  # type: ignore[arg-type]
                schema_version=int(binding["schema_version"]),
                now=_utc(now),
            )
            authority._trust_verifier.require_runtime_signer(
                authority._trust_certificate,
                authority._runtime_signer,  # type: ignore[arg-type]
            )
        except (LabClaimFinalizerTrustError, TypeError, ValueError) as exc:
            raise ClaimPublicationConflictError("finalizer_external_trust_invalid") from exc
        row = connection.execute(
            "SELECT * FROM lab_claim_publication_finalizer_lease WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise ClaimPublicationConflictError("finalizer_authority_missing")
        root_descriptor = str(row["root_descriptor"])
        root_key_digest = str(row["token_commitment"])
        acquired_at = _load_time(str(row["acquired_at"]))
        expires_at = _load_time(str(row["expires_at"]))
        matches = (
            authority.canonical_job_store_path == binding["canonical_job_store_path"]
            and authority.database_generation == binding["database_generation"]
            and authority.store_id == binding["store_id"]
            and authority.schema_version == binding["schema_version"]
            and authority.implementation_digest == binding["implementation_digest"]
            and str(row["canonical_job_store_path"]) == binding["canonical_job_store_path"]
            and (
                _strict_sqlite_int(row["database_device"], field="finalizer.database_device"),
                _strict_sqlite_int(row["database_inode"], field="finalizer.database_inode"),
            )
            == binding["database_generation"]
            and str(row["store_id"]) == binding["store_id"]
            and _strict_sqlite_int(
                row["schema_version"], field="finalizer.schema_version", minimum=1
            )
            == binding["schema_version"]
            and str(row["implementation_digest"]) == binding["implementation_digest"]
            and str(row["owner_id"]) == authority.owner_id
            and _strict_sqlite_int(row["lease_id"], field="finalizer.lease_id", minimum=1)
            == authority.lease_id
            and _strict_sqlite_int(row["fencing_token"], field="finalizer.fencing_token", minimum=1)
            == authority.fencing_token
            and str(row["lease_commitment"]) == authority.lease_commitment
            and authority.expires_at == expires_at
            and authority.root_mac_matches(
                self._finalizer_authority_mac_payload(
                    binding=binding,
                    owner_id=str(row["owner_id"]),
                    lease_id=_strict_sqlite_int(
                        row["lease_id"], field="finalizer.lease_id", minimum=1
                    ),
                    fencing_token=_strict_sqlite_int(
                        row["fencing_token"], field="finalizer.fencing_token", minimum=1
                    ),
                    acquired_at=acquired_at,
                    expires_at=expires_at,
                    lease_commitment=str(row["lease_commitment"]),
                ),
                root_descriptor=root_descriptor,
                key_digest=root_key_digest,
            )
            and row["released_at"] is None
            and _load_time(str(row["expires_at"])) > _utc(now)
        )
        if not matches:
            raise ClaimPublicationConflictError("finalizer_authority_conflict")

    def _load_claim_publication_for_finalizer_mutation(
        self,
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> LabClaimPublicationRecord:
        self._require_claim_publication_finalizer_authority(connection, authority, now=_utc(now))
        row = connection.execute(
            "SELECT * FROM lab_claim_publication WHERE attempt_id = ?",
            (str(identity.attempt_id),),
        ).fetchone()
        if row is None:
            raise ClaimPublicationConflictError("attempt_identity_conflict")
        record = _claim_publication_record_from_row(row)
        if record.identity != identity:
            raise ClaimPublicationConflictError("attempt_identity_conflict")
        return record

    def _build_finalizer_attestation(
        self,
        connection: sqlite3.Connection,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        record: LabClaimPublicationRecord,
        now: datetime,
    ) -> tuple[bytes, bytes]:
        if (
            authority._trust_certificate is None
            or authority._trust_verifier is None
            or authority._runtime_signer is None
            or record.status
            not in {ClaimPublicationStatus.READY_TO_PUBLISH, ClaimPublicationStatus.PUBLISHED}
        ):
            raise ClaimPublicationConflictError("finalizer_external_trust_invalid")
        binding = self._finalizer_authority_binding(connection, path=self.path)
        try:
            certificate_bytes, attestation_bytes = (
                build_lab_claim_finalizer_publication_attestation(
                    certificate=authority._trust_certificate,
                    signer=authority._runtime_signer,  # type: ignore[arg-type]
                    attempt_id=str(record.identity.attempt_id),
                    claim_generation=record.identity.claim_generation,
                    scheduler_fencing_token=record.identity.scheduler_fencing_token,
                    finalizer_fencing_token=authority.fencing_token,
                    publication_status=record.status.value,
                    source_use_plan_hash=record.source_use_plan_hash or "",
                    final_claim_hash=record.final_claim_hash or "",
                    spool_receipt_hash=record.spool_receipt_hash,
                    store_id=str(binding["store_id"]),
                    schema_version=int(binding["schema_version"]),
                )
            )
            require_lab_claim_finalizer_publication_attestation(
                verifier=authority._trust_verifier,
                certificate_bytes=certificate_bytes,
                attestation_bytes=attestation_bytes,
                store_id=str(binding["store_id"]),
                database_generation=binding["database_generation"],  # type: ignore[arg-type]
                schema_version=int(binding["schema_version"]),
                now=now,
                attempt_id=str(record.identity.attempt_id),
                claim_generation=record.identity.claim_generation,
                scheduler_fencing_token=record.identity.scheduler_fencing_token,
                finalizer_fencing_token=authority.fencing_token,
                publication_status=record.status.value,
                source_use_plan_hash=record.source_use_plan_hash or "",
                final_claim_hash=record.final_claim_hash or "",
                spool_receipt_hash=record.spool_receipt_hash,
            )
        except (LabClaimFinalizerTrustError, TypeError, ValueError) as exc:
            raise ClaimPublicationConflictError("finalizer_publication_signature_invalid") from exc
        return certificate_bytes, attestation_bytes

    @staticmethod
    def _persist_finalizer_attestation(
        connection: sqlite3.Connection,
        *,
        record: LabClaimPublicationRecord,
        certificate_bytes: bytes,
        attestation_bytes: bytes,
        now: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO lab_claim_publication_finalizer_attestation (
                attempt_id, publication_status, certificate_bytes, certificate_hash,
                attestation_bytes, attestation_hash, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(record.identity.attempt_id),
                record.status.value,
                certificate_bytes,
                hashlib.sha256(certificate_bytes).hexdigest(),
                attestation_bytes,
                hashlib.sha256(attestation_bytes).hexdigest(),
                _dump_time(_utc(now)),
            ),
        )

    def _insert_claim_publication_rollout_evidence_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        record: LabClaimPublicationRecord,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> None:
        """Enqueue the signed D record without extending the frozen v16 schema."""

        if not connection.in_transaction:
            raise RuntimeError("rollout evidence enqueue requires an active transaction")
        evidence = LabClaimPublicationRolloutEvidence.from_record(record)
        degradation_ref = uuid5(
            NAMESPACE_URL,
            "|".join(
                (
                    "rquant-claim-publication-rollout-evidence/v1",
                    str(evidence.attempt_id),
                    evidence.evidence_hash,
                )
            ),
        )
        connection.execute(
            """
            INSERT INTO lab_claim_publication_finalizer_observation_degradation (
                degradation_ref, attempt_id, publication_identity_hash,
                authority_fencing_token, event_type, reason_code, reason_code_hash,
                error_class, next_retry_at, created_at, drained_at
            ) VALUES (?, ?, ?, ?, 'published', ?, ?, ?, ?, ?, NULL)
            """,
            (
                str(degradation_ref),
                str(evidence.attempt_id),
                evidence.evidence_hash,
                authority.fencing_token,
                _ROLLOUT_EVIDENCE_REASON_CODE,
                _ROLLOUT_EVIDENCE_REASON_HASH,
                _ROLLOUT_EVIDENCE_INITIAL_ERROR_CLASS,
                _dump_time(_utc(now)),
                _dump_time(_utc(now)),
            ),
        )

    def validate_finalizer_publication_attestation(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        trust_verifier: LabClaimFinalizerTrustVerifier,
        publication_status: ClaimPublicationStatus,
        now: datetime,
    ) -> None:
        """Verify the external C/D trust chain before a worker may consume V2 work."""

        validated = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        current = _utc(now)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM lab_claim_publication WHERE attempt_id = ?",
                (str(validated.attempt_id),),
            ).fetchone()
            attestation_row = connection.execute(
                """
                SELECT certificate_bytes, certificate_hash, attestation_bytes, attestation_hash
                FROM lab_claim_publication_finalizer_attestation
                WHERE attempt_id = ? AND publication_status = ?
                """,
                (str(validated.attempt_id), publication_status.value),
            ).fetchone()
            if row is None or attestation_row is None:
                raise ClaimPublicationConflictError("finalizer_publication_signature_missing")
            record = _claim_publication_record_from_row(row)
            if record.identity != validated or record.status is not publication_status:
                raise ClaimPublicationConflictError("finalizer_publication_signature_invalid")
            certificate_bytes = _strict_sqlite_blob(
                attestation_row["certificate_bytes"],
                field="lab_claim_publication_finalizer_attestation.certificate_bytes",
            )
            attestation_bytes = _strict_sqlite_blob(
                attestation_row["attestation_bytes"],
                field="lab_claim_publication_finalizer_attestation.attestation_bytes",
            )
            if hashlib.sha256(certificate_bytes).hexdigest() != str(
                attestation_row["certificate_hash"]
            ) or hashlib.sha256(attestation_bytes).hexdigest() != str(
                attestation_row["attestation_hash"]
            ):
                raise ClaimPublicationConflictError("finalizer_publication_signature_invalid")
            try:
                attestation = strict_model_validate_canonical_json(
                    LabClaimFinalizerPublicationAttestation,
                    attestation_bytes,
                )
                binding = self._finalizer_authority_binding(connection, path=self.path)
                require_lab_claim_finalizer_publication_attestation(
                    verifier=trust_verifier,
                    certificate_bytes=certificate_bytes,
                    attestation_bytes=attestation_bytes,
                    store_id=str(binding["store_id"]),
                    database_generation=binding["database_generation"],  # type: ignore[arg-type]
                    schema_version=int(binding["schema_version"]),
                    now=current,
                    attempt_id=str(record.identity.attempt_id),
                    claim_generation=record.identity.claim_generation,
                    scheduler_fencing_token=record.identity.scheduler_fencing_token,
                    finalizer_fencing_token=attestation.finalizer_fencing_token,
                    publication_status=publication_status.value,
                    source_use_plan_hash=record.source_use_plan_hash or "",
                    final_claim_hash=record.final_claim_hash or "",
                    spool_receipt_hash=record.spool_receipt_hash,
                )
            except (LabClaimFinalizerTrustError, TypeError, ValueError) as exc:
                raise ClaimPublicationConflictError(
                    "finalizer_publication_signature_invalid"
                ) from exc

    def validate_finalizer_published_attestation(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        trust_verifier: LabClaimFinalizerTrustVerifier,
        now: datetime,
    ) -> None:
        self.validate_finalizer_publication_attestation(
            identity,
            trust_verifier=trust_verifier,
            publication_status=ClaimPublicationStatus.PUBLISHED,
            now=now,
        )

    def validate_finalizer_ready_attestation(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        trust_verifier: LabClaimFinalizerTrustVerifier,
        now: datetime,
    ) -> None:
        self.validate_finalizer_publication_attestation(
            identity,
            trust_verifier=trust_verifier,
            publication_status=ClaimPublicationStatus.READY_TO_PUBLISH,
            now=now,
        )

    def _finalizer_mark_claim_publication_ready_in_transaction(
        self,
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        *,
        expected: LabClaimPublicationRecord,
        ready_binding: ReadyBinding | None,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> _ClaimPublicationDecision:
        if not connection.in_transaction:
            raise RuntimeError("finalizer ready requires an active transaction")
        current = _utc(now)
        record = self._load_claim_publication_for_finalizer_mutation(
            connection, identity, authority=authority, now=current
        )
        if not self._claim_publication_snapshot_matches(record, expected):
            if (
                record.status is ClaimPublicationStatus.READY_TO_PUBLISH
                and ready_binding is not None
                and self._claim_publication_matches_ready(record, ready_binding)
            ):
                return self._publication_replay(
                    connection, record, reason_code="finalizer_ready_replay", now=current
                )
            raise ClaimPublicationConflictError("publication_cas_conflict")
        if record.status is ClaimPublicationStatus.PUBLISHED:
            return self._publication_terminal_read(record)
        if record.status is ClaimPublicationStatus.ABORTED:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="terminal_status_immutable",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        if record.status not in {
            ClaimPublicationStatus.SOURCE_QUEUED,
            ClaimPublicationStatus.READY_TO_PUBLISH,
        }:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="transition_not_allowed",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        self._validate_claim_publication_shard_binding(connection, identity, now=current)
        if ready_binding is None:
            raise RuntimeError("finalizer ready validation was not completed")
        if record.status is ClaimPublicationStatus.READY_TO_PUBLISH:
            if self._claim_publication_matches_ready(record, ready_binding):
                return self._publication_replay(
                    connection, record, reason_code="finalizer_ready_replay", now=current
                )
            return self._publication_conflict_decision(
                connection, record, reason_code="ready_content_conflict", now=current
            )
        values = self._publication_values(record)
        values.update(
            {
                **ready_binding.model_dump(mode="python"),
                "status": ClaimPublicationStatus.READY_TO_PUBLISH,
                "version": 2,
                "updated_at": current,
                "ready_at": current,
            }
        )
        updated = _claim_publication_record_from_values(values)
        certificate_bytes, attestation_bytes = self._build_finalizer_attestation(
            connection,
            authority=authority,
            record=updated,
            now=current,
        )
        decision = self._update_claim_publication_in_transaction(
            connection,
            record,
            updated,
            reason_code="finalizer_source_queued_to_ready_to_publish",
            now=current,
        )
        self._persist_finalizer_attestation(
            connection,
            record=updated,
            certificate_bytes=certificate_bytes,
            attestation_bytes=attestation_bytes,
            now=current,
        )
        return decision

    def finalizer_mark_claim_publication_ready(
        self,
        identity: LabClaimPublicationIdentity,
        signed_plan: SourceUsePlanV2,
        final_bound_claim: LabShardClaimV2,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> LabClaimPublicationMutation:
        """CAS C using a finalizer capability, never a scheduler lease."""

        if type(authority) is not LabClaimPublicationFinalizerAuthority:
            raise ClaimPublicationConflictError("finalizer_authority_conflict")
        validated_identity = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        validated_plan = SourceUsePlanV2.model_validate(signed_plan.model_dump())
        validated_claim = LabShardClaimV2.model_validate(final_bound_claim.model_dump())
        current = _utc(now)
        phase_one_record = self._read_claim_publication_for_external_validation(validated_identity)
        ready_binding: ReadyBinding | None = None
        if phase_one_record.status in {
            ClaimPublicationStatus.SOURCE_QUEUED,
            ClaimPublicationStatus.READY_TO_PUBLISH,
        }:
            ready_binding = self._ready_binding_for_record(
                phase_one_record,
                validated_plan,
                validated_claim,
                current_claim_authority=current_claim_authority,
                keyring=keyring,
                audience=audience,
                now=current,
            )
        if phase_one_record.status in {
            ClaimPublicationStatus.SOURCE_QUEUED,
            ClaimPublicationStatus.READY_TO_PUBLISH,
        }:
            binding = CurrentClaimConsumptionBindingV2.from_plan(validated_plan)
            with (
                hold_trusted_current_claim(
                    current_claim_authority,
                    binding=binding,
                    now=current,
                ),
                self._transaction() as connection,
            ):
                decision = self._finalizer_mark_claim_publication_ready_in_transaction(
                    connection,
                    validated_identity,
                    expected=phase_one_record,
                    ready_binding=ready_binding,
                    authority=authority,
                    now=current,
                )
        else:
            with self._transaction() as connection:
                decision = self._finalizer_mark_claim_publication_ready_in_transaction(
                    connection,
                    validated_identity,
                    expected=phase_one_record,
                    ready_binding=ready_binding,
                    authority=authority,
                    now=current,
                )
        return decision.resolved()

    def _finalizer_publish_claim_publication_in_transaction(
        self,
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        spool_receipt: PublishReceipt,
        *,
        expected: LabClaimPublicationRecord,
        validated_ready_claim: LabShardClaimV2 | None,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> _ClaimPublicationDecision:
        if not connection.in_transaction:
            raise RuntimeError("finalizer publish requires an active transaction")
        current = _utc(now)
        validated = PublishReceipt.model_validate(spool_receipt.model_dump())
        record = self._load_claim_publication_for_finalizer_mutation(
            connection, identity, authority=authority, now=current
        )
        if not self._claim_publication_snapshot_matches(record, expected):
            if (
                record.status is ClaimPublicationStatus.PUBLISHED
                and self._claim_publication_matches_receipt(record, validated)
            ):
                return self._publication_terminal_read(record)
            raise ClaimPublicationConflictError("publication_cas_conflict")
        if record.status is ClaimPublicationStatus.PUBLISHED:
            if self._claim_publication_matches_receipt(record, validated):
                return self._publication_terminal_read(record)
            return self._publication_conflict_decision(
                connection, record, reason_code="published_receipt_conflict", now=current
            )
        if record.status is ClaimPublicationStatus.ABORTED:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="terminal_status_immutable",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        if record.status is not ClaimPublicationStatus.READY_TO_PUBLISH:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="transition_not_allowed",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        if validated_ready_claim is None:
            raise RuntimeError("finalizer publish validation was not completed")
        self._validate_claim_publication_shard_binding(connection, identity, now=current)
        values = self._publication_values(record)
        values.update(
            {
                **validated.model_dump(mode="python"),
                "status": ClaimPublicationStatus.PUBLISHED,
                "version": 3,
                "updated_at": current,
                "published_at": current,
            }
        )
        updated = _claim_publication_record_from_values(values)
        certificate_bytes, attestation_bytes = self._build_finalizer_attestation(
            connection,
            authority=authority,
            record=updated,
            now=current,
        )
        decision = self._update_claim_publication_in_transaction(
            connection,
            record,
            updated,
            reason_code="finalizer_ready_to_published",
            now=current,
        )
        self._persist_finalizer_attestation(
            connection,
            record=updated,
            certificate_bytes=certificate_bytes,
            attestation_bytes=attestation_bytes,
            now=current,
        )
        self._insert_claim_publication_rollout_evidence_in_transaction(
            connection,
            record=updated,
            authority=authority,
            now=current,
        )
        return decision

    def finalizer_publish_claim_publication(
        self,
        identity: LabClaimPublicationIdentity,
        spool_receipt: PublishReceipt,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        spool_receipt_verifier: LabClaimSpoolReceiptVerifier,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> LabClaimPublicationMutation:
        """CAS D after an externally verified typed receipt has become durable."""

        if type(authority) is not LabClaimPublicationFinalizerAuthority:
            raise ClaimPublicationConflictError("finalizer_authority_conflict")
        validated_identity = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        validated_receipt = PublishReceipt.model_validate(spool_receipt.model_dump())
        current = _utc(now)
        phase_one_record = self._read_claim_publication_for_external_validation(validated_identity)
        validated_ready_claim: LabShardClaimV2 | None = None
        if phase_one_record.status is ClaimPublicationStatus.READY_TO_PUBLISH:
            if authority._trust_verifier is None:
                raise ClaimPublicationConflictError("finalizer_external_trust_invalid")
            self.validate_finalizer_ready_attestation(
                validated_identity,
                trust_verifier=authority._trust_verifier,
                now=current,
            )
            validated_ready_claim = self.validate_ready_claim_for_publication(
                validated_identity,
                current_claim_authority=current_claim_authority,
                keyring=keyring,
                audience=audience,
                now=current,
            )
            require_v2_spool_receipt_provenance(
                validated_receipt,
                final_claim=validated_ready_claim,
                verifier=spool_receipt_verifier,
            )
        elif phase_one_record.status is ClaimPublicationStatus.PUBLISHED:
            final_claim = strict_model_validate_canonical_json(
                LabShardClaimV2,
                phase_one_record.final_claim_bytes or b"",
            )
            require_v2_spool_receipt_provenance(
                validated_receipt,
                final_claim=final_claim,
                verifier=spool_receipt_verifier,
            )
        if phase_one_record.status is ClaimPublicationStatus.READY_TO_PUBLISH:
            stored_plan = strict_model_validate_canonical_json(
                SourceUsePlanV2,
                phase_one_record.source_use_plan_bytes or b"",
            )
            with (
                hold_trusted_current_claim(
                    current_claim_authority,
                    binding=CurrentClaimConsumptionBindingV2.from_plan(stored_plan),
                    now=current,
                ),
                self._transaction() as connection,
            ):
                decision = self._finalizer_publish_claim_publication_in_transaction(
                    connection,
                    validated_identity,
                    validated_receipt,
                    expected=phase_one_record,
                    validated_ready_claim=validated_ready_claim,
                    authority=authority,
                    now=current,
                )
        else:
            with self._transaction() as connection:
                decision = self._finalizer_publish_claim_publication_in_transaction(
                    connection,
                    validated_identity,
                    validated_receipt,
                    expected=phase_one_record,
                    validated_ready_claim=validated_ready_claim,
                    authority=authority,
                    now=current,
                )
        return decision.resolved()

    def _insert_claim_publication_finalizer_observation(
        self,
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        observation_fencing_token: int,
        event_type: Literal["ready", "published", "replayed", "blocked"],
        reason_code: str,
        now: datetime,
    ) -> None:
        record = self._load_claim_publication_for_finalizer_mutation(
            connection,
            identity,
            authority=authority,
            now=now,
        )
        observation_ref = uuid5(
            NAMESPACE_URL,
            "|".join(
                (
                    "rquant-claim-publication-finalizer-observation/v1",
                    str(identity.attempt_id),
                    str(observation_fencing_token),
                    event_type,
                    reason_code,
                    record.record_commitment,
                )
            ),
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO lab_claim_publication_finalizer_observation (
                observation_ref, attempt_id, authority_fencing_token, event_type,
                reason_code, record_commitment, observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(observation_ref),
                str(identity.attempt_id),
                observation_fencing_token,
                event_type,
                reason_code,
                record.record_commitment,
                _dump_time(now),
            ),
        )

    def finalizer_record_claim_publication_observation(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        event_type: Literal["ready", "published", "replayed", "blocked"],
        reason_code: str,
        now: datetime,
    ) -> None:
        """Persist redacted finalizer progress without recording payload bytes."""

        if type(authority) is not LabClaimPublicationFinalizerAuthority:
            raise ClaimPublicationConflictError("finalizer_authority_conflict")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", reason_code):
            raise ValueError("finalizer observation reason is invalid")
        current = _utc(now)
        validated = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        with self._transaction() as connection:
            self._insert_claim_publication_finalizer_observation(
                connection,
                validated,
                authority=authority,
                observation_fencing_token=authority.fencing_token,
                event_type=event_type,
                reason_code=reason_code,
                now=current,
            )

    def finalizer_record_claim_publication_observation_degradation(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        event_type: Literal["ready", "published", "replayed", "blocked"],
        reason_code: str,
        error_class: str,
        next_retry_at: datetime,
        now: datetime,
    ) -> None:
        """Persist bounded, redacted retry metadata on a separate transaction."""

        if type(authority) is not LabClaimPublicationFinalizerAuthority:
            raise ClaimPublicationConflictError("finalizer_authority_conflict")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", reason_code):
            raise ValueError("finalizer degradation reason is invalid")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", error_class):
            raise ValueError("finalizer degradation error class is invalid")
        current = _utc(now)
        retry_at = _utc(next_retry_at)
        if retry_at < current:
            raise ValueError("finalizer degradation retry time is invalid")
        validated = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        identity_hash = hashlib.sha256(canonical_model_json_bytes(validated)).hexdigest()
        reason_hash = hashlib.sha256(reason_code.encode("ascii")).hexdigest()
        degradation_ref = uuid5(
            NAMESPACE_URL,
            "|".join(
                (
                    "rquant-claim-publication-finalizer-observation-degradation/v1",
                    str(validated.attempt_id),
                    identity_hash,
                    str(authority.fencing_token),
                    event_type,
                    reason_hash,
                    error_class,
                )
            ),
        )
        with self._transaction() as connection:
            self._load_claim_publication_for_finalizer_mutation(
                connection,
                validated,
                authority=authority,
                now=current,
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO lab_claim_publication_finalizer_observation_degradation (
                    degradation_ref, attempt_id, publication_identity_hash,
                    authority_fencing_token, event_type, reason_code, reason_code_hash,
                    error_class, next_retry_at, created_at, drained_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    str(degradation_ref),
                    str(validated.attempt_id),
                    identity_hash,
                    authority.fencing_token,
                    event_type,
                    reason_code,
                    reason_hash,
                    error_class,
                    _dump_time(retry_at),
                    _dump_time(current),
                ),
            )

    def _claim_publication_rollout_evidence_item_from_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> LabClaimPublicationRolloutEvidenceOutboxItem:
        reason_code = str(row["reason_code"])
        if (
            reason_code != _ROLLOUT_EVIDENCE_REASON_CODE
            or str(row["reason_code_hash"]) != _ROLLOUT_EVIDENCE_REASON_HASH
            or str(row["event_type"]) != "published"
        ):
            raise ClaimPublicationConflictError("rollout_evidence_outbox_binding_invalid")
        error_class = str(row["error_class"])
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", error_class):
            raise ClaimPublicationConflictError("rollout_evidence_outbox_binding_invalid")
        attempt_id = _canonical_uuid_text(row["attempt_id"], field="rollout_evidence.attempt_id")
        publication_row = connection.execute(
            "SELECT * FROM lab_claim_publication WHERE attempt_id = ?",
            (str(attempt_id),),
        ).fetchone()
        if publication_row is None:
            raise ClaimPublicationConflictError("rollout_evidence_publication_missing")
        record = _claim_publication_record_from_row(publication_row)
        evidence = LabClaimPublicationRolloutEvidence.from_record(record)
        if evidence.evidence_hash != str(row["publication_identity_hash"]):
            raise ClaimPublicationConflictError("rollout_evidence_outbox_binding_invalid")
        attestation_row = connection.execute(
            """
            SELECT certificate_bytes, certificate_hash, attestation_bytes, attestation_hash
            FROM lab_claim_publication_finalizer_attestation
            WHERE attempt_id = ? AND publication_status = 'PUBLISHED'
            """,
            (str(attempt_id),),
        ).fetchone()
        if attestation_row is None:
            raise ClaimPublicationConflictError("rollout_evidence_attestation_missing")
        certificate_bytes = _strict_sqlite_blob(
            attestation_row["certificate_bytes"],
            field="rollout_evidence.certificate_bytes",
        )
        attestation_bytes = _strict_sqlite_blob(
            attestation_row["attestation_bytes"],
            field="rollout_evidence.attestation_bytes",
        )
        if hashlib.sha256(certificate_bytes).hexdigest() != str(
            attestation_row["certificate_hash"]
        ) or hashlib.sha256(attestation_bytes).hexdigest() != str(
            attestation_row["attestation_hash"]
        ):
            raise ClaimPublicationConflictError("rollout_evidence_attestation_invalid")
        return LabClaimPublicationRolloutEvidenceOutboxItem(
            degradation_ref=_canonical_uuid_text(
                row["degradation_ref"], field="rollout_evidence.degradation_ref"
            ),
            evidence=evidence,
            record=record,
            authority_fencing_token=_strict_sqlite_int(
                row["authority_fencing_token"],
                field="rollout_evidence.authority_fencing_token",
                minimum=1,
            ),
            next_retry_at=_load_time(str(row["next_retry_at"])),
            created_at=_load_time(str(row["created_at"])),
        )

    def list_due_claim_publication_rollout_evidence(
        self,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
        limit: int = 32,
    ) -> tuple[LabClaimPublicationRolloutEvidenceOutboxItem, ...]:
        """Return one authority-fenced bounded batch without trusting retry metadata."""

        if type(authority) is not LabClaimPublicationFinalizerAuthority:
            raise ClaimPublicationConflictError("finalizer_authority_conflict")
        if not 1 <= limit <= 100:
            raise ValueError("rollout evidence drain limit must be between 1 and 100")
        current = _utc(now)
        with self._transaction() as connection:
            self._require_claim_publication_finalizer_authority(connection, authority, now=current)
            rows = connection.execute(
                """
                SELECT * FROM lab_claim_publication_finalizer_observation_degradation
                WHERE drained_at IS NULL AND next_retry_at <= ? AND reason_code = ?
                ORDER BY next_retry_at, created_at, degradation_ref
                LIMIT ?
                """,
                (_dump_time(current), _ROLLOUT_EVIDENCE_REASON_CODE, limit),
            ).fetchall()
            return tuple(
                self._claim_publication_rollout_evidence_item_from_row(connection, row)
                for row in rows
            )

    def finalizer_ack_claim_publication_rollout_evidence(
        self,
        item: LabClaimPublicationRolloutEvidenceOutboxItem,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> None:
        """Acknowledge only the exact row whose external rollout insert succeeded."""

        validated = LabClaimPublicationRolloutEvidenceOutboxItem.model_validate(item)
        current = _utc(now)
        with self._transaction() as connection:
            self._require_claim_publication_finalizer_authority(connection, authority, now=current)
            row = connection.execute(
                """
                SELECT * FROM lab_claim_publication_finalizer_observation_degradation
                WHERE degradation_ref = ?
                """,
                (str(validated.degradation_ref),),
            ).fetchone()
            if row is None:
                raise ClaimPublicationConflictError("rollout_evidence_outbox_missing")
            current_item = self._claim_publication_rollout_evidence_item_from_row(connection, row)
            if current_item != validated:
                raise ClaimPublicationConflictError("rollout_evidence_outbox_binding_invalid")
            if row["drained_at"] is None:
                changed = connection.execute(
                    """
                    UPDATE lab_claim_publication_finalizer_observation_degradation
                    SET drained_at = ?
                    WHERE degradation_ref = ? AND drained_at IS NULL
                    """,
                    (_dump_time(current), str(validated.degradation_ref)),
                ).rowcount
                if changed != 1:
                    raise ClaimPublicationConflictError("rollout_evidence_ack_conflict")

    def finalizer_defer_claim_publication_rollout_evidence(
        self,
        item: LabClaimPublicationRolloutEvidenceOutboxItem,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        error_class: str,
        next_retry_at: datetime,
        now: datetime,
    ) -> None:
        """Retain a redacted recorder outage for a later exact replay."""

        validated = LabClaimPublicationRolloutEvidenceOutboxItem.model_validate(item)
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", error_class):
            raise ValueError("rollout evidence error class is invalid")
        current = _utc(now)
        retry_at = _utc(next_retry_at)
        if retry_at <= current:
            raise ValueError("rollout evidence retry time is invalid")
        with self._transaction() as connection:
            self._require_claim_publication_finalizer_authority(connection, authority, now=current)
            row = connection.execute(
                """
                SELECT * FROM lab_claim_publication_finalizer_observation_degradation
                WHERE degradation_ref = ? AND drained_at IS NULL
                """,
                (str(validated.degradation_ref),),
            ).fetchone()
            if row is None:
                raise ClaimPublicationConflictError("rollout_evidence_outbox_missing")
            current_item = self._claim_publication_rollout_evidence_item_from_row(connection, row)
            if current_item != validated:
                raise ClaimPublicationConflictError("rollout_evidence_outbox_binding_invalid")
            changed = connection.execute(
                """
                UPDATE lab_claim_publication_finalizer_observation_degradation
                SET error_class = ?, next_retry_at = ?
                WHERE degradation_ref = ? AND drained_at IS NULL
                """,
                (error_class, _dump_time(retry_at), str(validated.degradation_ref)),
            ).rowcount
            if changed != 1:
                raise ClaimPublicationConflictError("rollout_evidence_defer_conflict")

    def count_pending_claim_publication_rollout_evidence(self) -> int:
        with self._read_transaction() as connection:
            return int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM lab_claim_publication_finalizer_observation_degradation
                    WHERE drained_at IS NULL AND reason_code = ?
                    """,
                    (_ROLLOUT_EVIDENCE_REASON_CODE,),
                ).fetchone()[0]
            )

    def count_pending_claim_publication_observation_degradations(self) -> int:
        with self._read_transaction() as connection:
            return int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM lab_claim_publication_finalizer_observation_degradation
                    WHERE drained_at IS NULL AND reason_code <> ?
                    """,
                    (_ROLLOUT_EVIDENCE_REASON_CODE,),
                ).fetchone()[0]
            )

    def count_nonterminal_claim_publications(self) -> int:
        with self._read_transaction() as connection:
            return int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM lab_claim_publication
                    WHERE status NOT IN ('PUBLISHED', 'ABORTED')
                    """
                ).fetchone()[0]
            )

    def list_reconciled_claim_publication_rollout_evidence(
        self,
    ) -> tuple[LabClaimPublicationRolloutEvidence, ...]:
        """Require one drained local outbox row for every durable PUBLISHED record."""

        with self._read_transaction() as connection:
            publication_rows = connection.execute(
                """
                SELECT * FROM lab_claim_publication
                WHERE status = 'PUBLISHED'
                ORDER BY attempt_id
                """
            ).fetchall()
            evidence: list[LabClaimPublicationRolloutEvidence] = []
            for publication_row in publication_rows:
                record = _claim_publication_record_from_row(publication_row)
                outbox_rows = connection.execute(
                    """
                    SELECT * FROM lab_claim_publication_finalizer_observation_degradation
                    WHERE attempt_id = ? AND reason_code = ?
                    ORDER BY degradation_ref
                    """,
                    (str(record.identity.attempt_id), _ROLLOUT_EVIDENCE_REASON_CODE),
                ).fetchall()
                if len(outbox_rows) != 1 or outbox_rows[0]["drained_at"] is None:
                    raise ClaimPublicationConflictError("rollout_evidence_reconciliation_gap")
                item = self._claim_publication_rollout_evidence_item_from_row(
                    connection, outbox_rows[0]
                )
                evidence.append(item.evidence)
            return tuple(evidence)

    def list_claim_publication_finalizer_observation_degradations(
        self,
        attempt_id: UUID,
    ) -> tuple[LabClaimPublicationObservationDegradation, ...]:
        with self._read_transaction() as connection:
            rows = connection.execute(
                """
                SELECT * FROM lab_claim_publication_finalizer_observation_degradation
                WHERE attempt_id = ? AND drained_at IS NULL AND reason_code <> ?
                ORDER BY created_at, degradation_ref
                """,
                (str(attempt_id), _ROLLOUT_EVIDENCE_REASON_CODE),
            ).fetchall()
        return tuple(
            LabClaimPublicationObservationDegradation(
                degradation_ref=UUID(str(row["degradation_ref"])),
                attempt_id=UUID(str(row["attempt_id"])),
                publication_identity_hash=str(row["publication_identity_hash"]),
                authority_fencing_token=_strict_sqlite_int(
                    row["authority_fencing_token"],
                    field="finalizer_degradation.authority_fencing_token",
                    minimum=1,
                ),
                event_type=str(row["event_type"]),
                reason_code=str(row["reason_code"]),
                reason_code_hash=str(row["reason_code_hash"]),
                error_class=str(row["error_class"]),
                next_retry_at=_load_time(str(row["next_retry_at"])),
                created_at=_load_time(str(row["created_at"])),
            )
            for row in rows
        )

    def finalizer_drain_claim_publication_observation_degradations(
        self,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
        limit: int = 32,
    ) -> int:
        """Retry one bounded due batch and mark each exact row drained atomically."""

        if type(authority) is not LabClaimPublicationFinalizerAuthority:
            raise ClaimPublicationConflictError("finalizer_authority_conflict")
        if not 1 <= limit <= 100:
            raise ValueError("finalizer degradation drain limit must be between 1 and 100")
        current = _utc(now)
        drained = 0
        with self._transaction() as connection:
            self._require_claim_publication_finalizer_authority(
                connection,
                authority,
                now=current,
            )
            rows = connection.execute(
                """
                SELECT * FROM lab_claim_publication_finalizer_observation_degradation
                WHERE drained_at IS NULL AND next_retry_at <= ? AND reason_code <> ?
                ORDER BY next_retry_at, created_at, degradation_ref
                LIMIT ?
                """,
                (_dump_time(current), _ROLLOUT_EVIDENCE_REASON_CODE, limit),
            ).fetchall()
            for row in rows:
                attempt_id = UUID(str(row["attempt_id"]))
                publication_row = connection.execute(
                    "SELECT * FROM lab_claim_publication WHERE attempt_id = ?",
                    (str(attempt_id),),
                ).fetchone()
                if publication_row is None:
                    raise ClaimPublicationConflictError("attempt_identity_conflict")
                record = _claim_publication_record_from_row(publication_row)
                identity_hash = hashlib.sha256(
                    canonical_model_json_bytes(record.identity)
                ).hexdigest()
                reason_code = str(row["reason_code"])
                if identity_hash != str(row["publication_identity_hash"]) or hashlib.sha256(
                    reason_code.encode("ascii")
                ).hexdigest() != str(row["reason_code_hash"]):
                    raise ClaimPublicationConflictError("finalizer_degradation_binding_invalid")
                self._insert_claim_publication_finalizer_observation(
                    connection,
                    record.identity,
                    authority=authority,
                    observation_fencing_token=_strict_sqlite_int(
                        row["authority_fencing_token"],
                        field="finalizer_degradation.authority_fencing_token",
                        minimum=1,
                    ),
                    event_type=str(row["event_type"]),  # type: ignore[arg-type]
                    reason_code=reason_code,
                    now=current,
                )
                drained += connection.execute(
                    """
                    UPDATE lab_claim_publication_finalizer_observation_degradation
                    SET drained_at = ?
                    WHERE degradation_ref = ? AND drained_at IS NULL
                    """,
                    (_dump_time(current), str(row["degradation_ref"])),
                ).rowcount
        return drained

    def list_claim_publication_finalizer_observations(
        self,
        attempt_id: UUID,
    ) -> tuple[tuple[str, str, str, int], ...]:
        """Return only redacted observation fields for operators and tests."""

        with self._read_transaction() as connection:
            rows = connection.execute(
                """
                SELECT event_type, reason_code, record_commitment, authority_fencing_token
                FROM lab_claim_publication_finalizer_observation
                WHERE attempt_id = ?
                ORDER BY observed_at, observation_ref
                """,
                (str(attempt_id),),
            ).fetchall()
        return tuple(
            (
                str(row["event_type"]),
                str(row["reason_code"]),
                str(row["record_commitment"]),
                _strict_sqlite_int(
                    row["authority_fencing_token"],
                    field="lab_claim_publication_finalizer_observation.authority_fencing_token",
                    minimum=1,
                ),
            )
            for row in rows
        )

    def _abort_claim_publication_in_transaction(
        self,
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        *,
        terminal_reason: str,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> _ClaimPublicationDecision:
        if not connection.in_transaction:
            raise RuntimeError("claim publication abort requires an active transaction")
        if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", terminal_reason) is None:
            raise ValueError("terminal_reason must be a stable code")
        current = _utc(now)
        record = self._load_claim_publication_for_mutation(
            connection, identity, lease=lease, now=current
        )
        if record.status is ClaimPublicationStatus.ABORTED:
            if record.terminal_reason == terminal_reason:
                return self._publication_replay(
                    connection, record, reason_code="aborted_replay", now=current
                )
            return self._publication_conflict_decision(
                connection, record, reason_code="terminal_reason_conflict", now=current
            )
        if record.status is ClaimPublicationStatus.PUBLISHED:
            return self._publication_conflict_decision(
                connection,
                record,
                reason_code="terminal_status_immutable",
                now=current,
                error_type=InvalidClaimPublicationTransitionError,
            )
        values = self._publication_values(record)
        values.update(
            {
                "status": ClaimPublicationStatus.ABORTED,
                "version": record.version + 1,
                "updated_at": current,
                "aborted_at": current,
                "terminal_reason": terminal_reason,
            }
        )
        return self._update_claim_publication_in_transaction(
            connection,
            record,
            _claim_publication_record_from_values(values),
            reason_code=f"{record.status.value.lower()}_to_aborted",
            now=current,
        )

    def abort_claim_publication(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        terminal_reason: str,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> LabClaimPublicationMutation:
        validated_identity = LabClaimPublicationIdentity.model_validate(identity.model_dump())
        with self._transaction() as connection:
            decision = self._abort_claim_publication_in_transaction(
                connection,
                validated_identity,
                terminal_reason=terminal_reason,
                lease=lease,
                now=now,
            )
        return decision.resolved()

    def get_claim_publication(
        self,
        attempt_id: UUID,
    ) -> LabClaimPublicationRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM lab_claim_publication WHERE attempt_id = ?",
                (str(attempt_id),),
            ).fetchone()
        return _claim_publication_record_from_row(row) if row is not None else None

    def list_claim_publication_audit(
        self,
        attempt_id: UUID,
    ) -> tuple[LabClaimPublicationAuditRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM lab_claim_publication_audit
                WHERE attempt_id = ?
                ORDER BY occurred_at, audit_ref
                """,
                (str(attempt_id),),
            ).fetchall()
        return tuple(_claim_publication_audit_from_row(row) for row in rows)

    def _list_claim_publications_by_deadline(
        self,
        *,
        statuses: tuple[ClaimPublicationStatus, ...],
        deadline_column: Literal["source_wait_deadline", "publication_deadline"],
        now: datetime,
        limit: int,
    ) -> tuple[LabClaimPublicationRecord, ...]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        if not statuses:
            raise ValueError("at least one claim publication status is required")
        current = _utc(now)
        placeholders = ", ".join("?" for _ in statuses)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM lab_claim_publication
                WHERE status IN ({placeholders}) AND {deadline_column} <= ?
                ORDER BY {deadline_column}, attempt_id
                LIMIT ?
                """,
                (*tuple(status.value for status in statuses), _dump_time(current), limit),
            ).fetchall()
        return tuple(_claim_publication_record_from_row(row) for row in rows)

    def list_expired_source_claim_publications(
        self,
        *,
        now: datetime,
        limit: int = 100,
    ) -> tuple[LabClaimPublicationRecord, ...]:
        return self._list_claim_publications_by_deadline(
            statuses=(
                ClaimPublicationStatus.HELD_SOURCE,
                ClaimPublicationStatus.SOURCE_QUEUED,
            ),
            deadline_column="source_wait_deadline",
            now=now,
            limit=limit,
        )

    def list_expired_held_claim_publications(
        self,
        *,
        now: datetime,
        limit: int = 100,
    ) -> tuple[LabClaimPublicationRecord, ...]:
        return self.list_expired_source_claim_publications(now=now, limit=limit)

    def list_claim_publication_reconcile_candidates(
        self,
        *,
        now: datetime,
        limit: int = 100,
    ) -> tuple[LabClaimPublicationRecord, ...]:
        return self._list_claim_publications_by_deadline(
            statuses=(ClaimPublicationStatus.READY_TO_PUBLISH,),
            deadline_column="publication_deadline",
            now=now,
            limit=limit,
        )

    def list_claim_publication_finalizer_candidates(
        self,
        *,
        limit: int = 32,
    ) -> tuple[LabClaimPublicationRecord, ...]:
        """Return one bounded C/D candidate batch for the authority-owned daemon."""

        if not 1 <= limit <= 100:
            raise ValueError("claim finalizer candidate limit must be between 1 and 100")
        with self._read_transaction() as connection:
            rows = connection.execute(
                """
                SELECT * FROM lab_claim_publication
                WHERE status IN ('SOURCE_QUEUED', 'READY_TO_PUBLISH')
                ORDER BY CASE status WHEN 'READY_TO_PUBLISH' THEN 0 ELSE 1 END,
                         updated_at, attempt_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(_claim_publication_record_from_row(row) for row in rows)

    def list_v2_reconciliation_candidates(
        self,
        *,
        now: datetime,
        limit: int = 100,
    ) -> tuple[LabClaimPublicationRecord, ...]:
        """Return a bounded, index-backed cross-stage V2 recovery batch.

        The two publication deadline indexes have different clocks, so each
        status is read through its matching index and the small merged batch is
        ordered in memory.  This deliberately avoids a full-ledger scan.
        """

        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        _utc(now)
        candidates: dict[UUID, LabClaimPublicationRecord] = {}
        for status, deadline_column in (
            (ClaimPublicationStatus.HELD_SOURCE, "source_wait_deadline"),
            (ClaimPublicationStatus.SOURCE_QUEUED, "source_wait_deadline"),
            (ClaimPublicationStatus.READY_TO_PUBLISH, "publication_deadline"),
        ):
            with self._connect() as connection:
                rows = connection.execute(
                    f"""
                    SELECT * FROM lab_claim_publication
                    WHERE status = ?
                    ORDER BY {deadline_column}, attempt_id
                    LIMIT ?
                    """,
                    (status.value, limit),
                ).fetchall()
            records = tuple(_claim_publication_record_from_row(row) for row in rows)
            candidates.update({record.identity.attempt_id: record for record in records})
        ordered = sorted(
            candidates.values(),
            key=lambda record: (
                record.updated_at,
                record.identity.attempt_id.hex,
            ),
        )
        return tuple(ordered[:limit])

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

    def _claim_preclaim_candidate_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        job_id: UUID,
        job_created_at: datetime,
        job_row: sqlite3.Row,
        worker: str,
        shard_lease_seconds: int,
        lease: LabLeaseRecord,
        now: datetime,
        source_stage_store: LabSourceStageStore | None,
        source_wait_deadline: datetime | None,
        publication_deadline: datetime | None,
        v2_precondition: Callable[[StrategyShardPayloadV2, LabShardClaimV2, datetime], None] | None,
        prevalidated_v2: frozenset[tuple[UUID, UUID, str, int, int, str, int, str]] | None,
        use_fair_cursor: bool,
    ) -> LabShardClaim | LabShardClaimV2 | LabPreclaimRejection | None:
        definition = self._definition_from_shard_row(row)
        spec_hash = str(job_row["spec_hash"])
        payload = self._external_payload_v2(definition.payload_json)
        prospective_claim: LabShardClaimV2 | None = None
        source_deadline: datetime | None = None
        publish_deadline: datetime | None = None
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
            _strict_sqlite_int(row["attempt_count"], field="lab_shard.attempt_count", minimum=0) + 1
        )
        claim_token = uuid4()
        expires_at = now + timedelta(seconds=shard_lease_seconds)
        if payload is not None:
            if prevalidated_v2 is not None:
                snapshot_identity = (
                    job_id,
                    definition.shard_id,
                    definition.payload_hash,
                    shard_version,
                    attempt_count - 1,
                    spec_hash,
                    generation,
                    worker,
                )
                if snapshot_identity not in prevalidated_v2:
                    return LabPreclaimRejection(
                        job_id=job_id,
                        shard_id=definition.shard_id,
                        payload_hash=definition.payload_hash,
                        reason="source_preclaim_rejected",
                    )
            if type(source_stage_store) is not LabSourceStageStore:
                raise ValueError("v2 claim requires an exact source_stage_store")
            if source_wait_deadline is None or publication_deadline is None:
                raise ValueError("v2 claim requires explicit source and publication deadlines")
            source_deadline = _utc(source_wait_deadline)
            publish_deadline = _utc(publication_deadline)
            if source_deadline <= now:
                raise ValueError("source_wait_deadline must be after claim time")
            if publish_deadline < source_deadline:
                raise ValueError("source_wait_deadline must not exceed publication_deadline")
            if publish_deadline > expires_at:
                raise ValueError("publication_deadline must not exceed shard lease expiry")
            prospective_claim = LabShardClaimV2.from_current_attempt(
                definition=definition,
                attempt_binding=SourceAttemptBindingV2(
                    job_id=job_id,
                    spec_hash=spec_hash,
                    shard_id=definition.shard_id,
                    attempt_id=claim_token,
                    claim_generation=generation,
                    scheduler_fencing_token=lease.fencing_token,
                    worker_id=worker,
                ),
                claimed_at=now,
                lease_expires_at=expires_at,
            )
            if v2_precondition is not None:
                try:
                    v2_precondition(payload, prospective_claim, now)
                except (SourceOperationContractError, ValueError):
                    return LabPreclaimRejection(
                        job_id=job_id,
                        shard_id=definition.shard_id,
                        payload_hash=definition.payload_hash,
                        reason="source_preclaim_rejected",
                    )
        job_status = JobStatus(str(job_row["status"]))
        if job_status is JobStatus.QUEUED:
            self._transition_in_transaction(
                connection,
                job_row,
                target_status=JobStatus.RUNNING,
                lease=lease,
                reason="first shard claimed",
                now=now,
                request_id=None,
                recoverable=None,
                event_type="job_started",
            )
        else:
            self._adopt_running_job_fence(
                connection,
                job_row,
                lease=lease,
                now=now,
            )
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
                _dump_time(now),
                _dump_time(now),
                _dump_time(expires_at),
                _dump_time(now),
                str(row["job_id"]),
                str(row["shard_id"]),
                shard_version,
                ShardStatus.QUEUED.value,
            ),
        )
        if cursor.rowcount != 1:
            return None
        self._store_preclaim_cursor(
            connection,
            job_created_at=job_created_at,
            job_id=job_id,
            shard_index=_strict_sqlite_int(
                row["shard_index"],
                field="lab_shard.shard_index",
                minimum=0,
            ),
            shard_id=_canonical_uuid_text(row["shard_id"], field="lab_shard.shard_id"),
            now=now,
            use_fair_cursor=use_fair_cursor,
        )
        if payload is None:
            return LabShardClaim(
                job_id=job_id,
                spec_hash=spec_hash,
                definition=definition,
                worker_id=worker,
                claim_token=claim_token,
                claim_generation=generation,
                scheduler_fencing_token=lease.fencing_token,
                claimed_at=now,
                lease_expires_at=expires_at,
            )
        assert source_deadline is not None and publish_deadline is not None
        assert prospective_claim is not None
        source_stage_authority = LabSourceStageStoreAuthority.model_validate(
            source_stage_store.authority.model_dump()
        )
        preimage_bytes = canonical_model_json_bytes(prospective_claim)
        self._create_held_claim_publication_in_transaction(
            connection,
            HeldDraft(
                identity=LabClaimPublicationIdentity.from_claim(prospective_claim),
                claim_preimage_bytes=preimage_bytes,
                claim_preimage_hash=hashlib.sha256(preimage_bytes).hexdigest(),
                source_wait_deadline=source_deadline,
                publication_deadline=publish_deadline,
            ),
            source_stage_authority=source_stage_authority,
            lease=lease,
            now=now,
        ).resolved()
        return prospective_claim

    def _prevalidate_v2_preclaim_candidates(
        self,
        *,
        worker: str,
        shard_lease_seconds: int,
        lease: LabLeaseRecord,
        now: datetime,
        source_wait_deadline: datetime | None,
        publication_deadline: datetime | None,
        v2_precondition: Callable[[StrategyShardPayloadV2, LabShardClaimV2, datetime], None],
    ) -> frozenset[tuple[UUID, UUID, str, int, int, str, int, str]]:
        """Validate a bounded v2 snapshot before acquiring the scheduler write lock."""

        if source_wait_deadline is None or publication_deadline is None:
            raise ValueError("v2 claim requires explicit source and publication deadlines")
        source_deadline = _utc(source_wait_deadline)
        publish_deadline = _utc(publication_deadline)
        expires_at = now + timedelta(seconds=shard_lease_seconds)
        if (
            source_deadline <= now
            or publish_deadline < source_deadline
            or publish_deadline > expires_at
        ):
            # The write phase preserves the legacy deferred-deadline behavior for a
            # selected candidate; an earlier V1 candidate must still remain claimable.
            return frozenset()
        with self._read_transaction() as connection:
            cursor = connection.execute(
                """
                SELECT claim_cursor_created_at, claim_cursor_job_id,
                       claim_cursor_shard_index, claim_cursor_shard_id,
                       claim_cursor_sequence
                FROM lab_scheduler_state WHERE state_key = 'claim_job_cursor'
                """
            ).fetchone()
            cursor_predicate = ""
            cursor_parameters: tuple[object, ...] = ()
            if cursor is not None:
                sequence = _strict_sqlite_int(
                    cursor["claim_cursor_sequence"],
                    field="lab_scheduler_state.claim_cursor_sequence",
                    minimum=0,
                )
                if sequence % PRECLAIM_FAIR_SCAN_INTERVAL == PRECLAIM_FAIR_SCAN_INTERVAL - 1:
                    cursor = connection.execute(
                        """
                        SELECT claim_cursor_created_at, claim_cursor_job_id,
                               claim_cursor_shard_index, claim_cursor_shard_id
                        FROM lab_preclaim_fair_cursor WHERE singleton = 1
                        """
                    ).fetchone()
                if (
                    cursor is not None
                    and cursor["claim_cursor_created_at"] is not None
                    and cursor["claim_cursor_job_id"] is not None
                    and cursor["claim_cursor_shard_index"] is not None
                    and cursor["claim_cursor_shard_id"] is not None
                ):
                    cursor_created_at = _load_time(str(cursor["claim_cursor_created_at"]))
                    cursor_job_id = _canonical_uuid_text(
                        cursor["claim_cursor_job_id"],
                        field="lab_scheduler_state.claim_cursor_job_id",
                    )
                    cursor_shard_index = _strict_sqlite_int(
                        cursor["claim_cursor_shard_index"],
                        field="lab_scheduler_state.claim_cursor_shard_index",
                        minimum=0,
                    )
                    cursor_shard_id = _canonical_uuid_text(
                        cursor["claim_cursor_shard_id"],
                        field="lab_scheduler_state.claim_cursor_shard_id",
                    )
                    cursor_predicate = """
                      AND (j.created_at > ? OR (
                          j.created_at = ? AND (j.job_id > ? OR (
                              j.job_id = ? AND (s.shard_index > ? OR (
                                  s.shard_index = ? AND s.shard_id > ?
                              ))
                          ))
                      ))
                    """
                    cursor_parameters = (
                        _dump_time(cursor_created_at),
                        _dump_time(cursor_created_at),
                        str(cursor_job_id),
                        str(cursor_job_id),
                        cursor_shard_index,
                        cursor_shard_index,
                        str(cursor_shard_id),
                    )
            rows = connection.execute(
                f"""
                SELECT s.*, j.spec_hash AS job_spec_hash
                FROM lab_shard AS s
                JOIN lab_job AS j ON j.job_id = s.job_id
                WHERE s.status = ? AND s.payload_protocol_version = 2
                  AND s.attempt_count < s.max_attempts
                  AND j.status IN (?, ?) AND j.control_intent = ? AND j.deadline > ?
                  {cursor_predicate}
                ORDER BY j.created_at, j.job_id, s.shard_index, s.shard_id
                LIMIT ?
                """,
                (
                    ShardStatus.QUEUED.value,
                    JobStatus.QUEUED.value,
                    JobStatus.RUNNING.value,
                    ControlIntent.NONE.value,
                    _dump_time(now),
                    *cursor_parameters,
                    PRECLAIM_CANDIDATE_BATCH_SIZE,
                ),
            ).fetchall()
            if not rows and cursor_predicate:
                rows = connection.execute(
                    """
                    SELECT s.*, j.spec_hash AS job_spec_hash
                    FROM lab_shard AS s
                    JOIN lab_job AS j ON j.job_id = s.job_id
                    WHERE s.status = ? AND s.payload_protocol_version = 2
                      AND s.attempt_count < s.max_attempts
                      AND j.status IN (?, ?) AND j.control_intent = ? AND j.deadline > ?
                    ORDER BY j.created_at, j.job_id, s.shard_index, s.shard_id
                    LIMIT ?
                    """,
                    (
                        ShardStatus.QUEUED.value,
                        JobStatus.QUEUED.value,
                        JobStatus.RUNNING.value,
                        ControlIntent.NONE.value,
                        _dump_time(now),
                        PRECLAIM_CANDIDATE_BATCH_SIZE,
                    ),
                ).fetchall()
        validated: set[tuple[UUID, UUID, str, int, int, str, int, str]] = set()
        for row in rows:
            definition = self._definition_from_shard_row(row)
            payload = self._external_payload_v2(definition.payload_json)
            if payload is None:  # pragma: no cover - schema trigger guards this invariant
                raise InvalidStoredJobError("v2 shard payload is not externally authorized")
            job_id = _canonical_uuid_text(row["job_id"], field="lab_shard.job_id")
            shard_version = _strict_sqlite_int(row["version"], field="lab_shard.version", minimum=0)
            attempt_count = _strict_sqlite_int(
                row["attempt_count"], field="lab_shard.attempt_count", minimum=0
            )
            generation = (
                _strict_sqlite_int(
                    row["claim_generation"], field="lab_shard.claim_generation", minimum=0
                )
                + 1
            )
            prospective = LabShardClaimV2.from_current_attempt(
                definition=definition,
                attempt_binding=SourceAttemptBindingV2(
                    job_id=job_id,
                    spec_hash=str(row["job_spec_hash"]),
                    shard_id=definition.shard_id,
                    attempt_id=uuid4(),
                    claim_generation=generation,
                    scheduler_fencing_token=lease.fencing_token,
                    worker_id=worker,
                ),
                claimed_at=now,
                lease_expires_at=expires_at,
            )
            try:
                v2_precondition(payload, prospective, now)
            except (SourceOperationContractError, ValueError):
                continue
            validated.add(
                (
                    job_id,
                    definition.shard_id,
                    definition.payload_hash,
                    shard_version,
                    attempt_count,
                    str(row["job_spec_hash"]),
                    generation,
                    worker,
                )
            )
        return frozenset(validated)

    def _store_preclaim_cursor(
        self,
        connection: sqlite3.Connection,
        *,
        job_created_at: datetime,
        job_id: UUID,
        shard_index: int,
        shard_id: UUID,
        now: datetime,
        use_fair_cursor: bool,
    ) -> None:
        """Advance the durable preclaim keyset cursor in the claim transaction."""

        sequence_row = connection.execute(
            "SELECT COALESCE(MAX(claim_cursor_sequence), 0) + 1 FROM lab_scheduler_state"
        ).fetchone()
        assert sequence_row is not None
        sequence = _strict_sqlite_int(
            sequence_row[0],
            field="lab_scheduler_state.claim_cursor_sequence",
            minimum=1,
        )
        if use_fair_cursor:
            connection.execute(
                """
                UPDATE lab_scheduler_state
                SET claim_cursor_sequence = ?, updated_at = ?
                WHERE state_key = 'claim_job_cursor'
                """,
                (sequence, _dump_time(now)),
            )
            connection.execute(
                """
                INSERT INTO lab_preclaim_fair_cursor (
                    singleton, claim_cursor_created_at, claim_cursor_job_id,
                    claim_cursor_shard_index, claim_cursor_shard_id, updated_at
                ) VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    claim_cursor_created_at = excluded.claim_cursor_created_at,
                    claim_cursor_job_id = excluded.claim_cursor_job_id,
                    claim_cursor_shard_index = excluded.claim_cursor_shard_index,
                    claim_cursor_shard_id = excluded.claim_cursor_shard_id,
                    updated_at = excluded.updated_at
                """,
                (
                    _dump_time(job_created_at),
                    str(job_id),
                    shard_index,
                    str(shard_id),
                    _dump_time(now),
                ),
            )
            return
        connection.execute(
            """
            INSERT INTO lab_scheduler_state (
                state_key, claim_cursor_created_at, claim_cursor_job_id,
                claim_cursor_shard_index, claim_cursor_shard_id,
                claim_cursor_sequence, updated_at
            ) VALUES ('claim_job_cursor', ?, ?, ?, ?, ?, ?)
            ON CONFLICT(state_key) DO UPDATE SET
                claim_cursor_created_at = excluded.claim_cursor_created_at,
                claim_cursor_job_id = excluded.claim_cursor_job_id,
                claim_cursor_shard_index = excluded.claim_cursor_shard_index,
                claim_cursor_shard_id = excluded.claim_cursor_shard_id,
                claim_cursor_sequence = excluded.claim_cursor_sequence,
                updated_at = excluded.updated_at
            """,
            (
                _dump_time(job_created_at),
                str(job_id),
                shard_index,
                str(shard_id),
                sequence,
                _dump_time(now),
            ),
        )

    def claim_next_shard(
        self,
        *,
        worker_id: str,
        shard_lease_seconds: int,
        lease: LabLeaseRecord,
        now: datetime,
        source_stage_store: LabSourceStageStore | None = None,
        source_wait_deadline: datetime | None = None,
        publication_deadline: datetime | None = None,
        allowed_payload_protocol_versions: tuple[int, ...] = (1, 2),
        v2_precondition: (
            Callable[[StrategyShardPayloadV2, LabShardClaimV2, datetime], None] | None
        ) = None,
        include_diagnostics: bool = False,
    ) -> LabShardClaim | LabShardClaimV2 | LabClaimSelection | None:
        rejections: list[LabPreclaimRejection] = []

        def selected(
            claim: LabShardClaim | LabShardClaimV2 | None,
        ) -> LabShardClaim | LabShardClaimV2 | LabClaimSelection | None:
            if include_diagnostics:
                return LabClaimSelection(claim=claim, rejections=tuple(rejections))
            return claim

        worker = worker_id.strip()
        if not worker:
            raise ValueError("worker_id must not be empty")
        if shard_lease_seconds < 1:
            raise ValueError("shard_lease_seconds must be positive")
        allowed_protocols = tuple(sorted(set(allowed_payload_protocol_versions)))
        if not allowed_protocols or any(version not in {1, 2} for version in allowed_protocols):
            raise ValueError("allowed_payload_protocol_versions must contain only 1 or 2")
        protocol_placeholders = ", ".join("?" for _ in allowed_protocols)
        current = _utc(now)
        prevalidated_v2: frozenset[tuple[UUID, UUID, str, int, int, str, int, str]] | None = None
        if v2_precondition is not None and 2 in allowed_protocols:
            prevalidated_v2 = self._prevalidate_v2_preclaim_candidates(
                worker=worker,
                shard_lease_seconds=shard_lease_seconds,
                lease=lease,
                now=current,
                source_wait_deadline=source_wait_deadline,
                publication_deadline=publication_deadline,
                v2_precondition=v2_precondition,
            )
            v2_precondition = None

        with self._transaction() as connection:
            self._validate_lease(connection, lease, now=current)
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
                return selected(None)
            claim_cursor = connection.execute(
                """
                SELECT claim_cursor_created_at, claim_cursor_job_id,
                       claim_cursor_shard_index, claim_cursor_shard_id,
                       claim_cursor_sequence
                FROM lab_scheduler_state
                WHERE state_key = 'claim_job_cursor'
                """
            ).fetchone()
            use_fair_cursor = False
            if claim_cursor is not None:
                sequence = _strict_sqlite_int(
                    claim_cursor["claim_cursor_sequence"],
                    field="lab_scheduler_state.claim_cursor_sequence",
                    minimum=0,
                )
                use_fair_cursor = sequence % PRECLAIM_FAIR_SCAN_INTERVAL == (
                    PRECLAIM_FAIR_SCAN_INTERVAL - 1
                )
                if use_fair_cursor:
                    claim_cursor = connection.execute(
                        """
                        SELECT claim_cursor_created_at, claim_cursor_job_id,
                               claim_cursor_shard_index, claim_cursor_shard_id
                        FROM lab_preclaim_fair_cursor WHERE singleton = 1
                        """
                    ).fetchone()
            reclaimed_shards: set[tuple[UUID, int, UUID]] = set()
            fair_work_available = use_fair_cursor and (
                connection.execute(
                    f"""
                    SELECT 1 FROM lab_shard AS s
                    JOIN lab_job AS j ON j.job_id = s.job_id
                    WHERE s.status = ?
                      AND s.payload_protocol_version IN ({protocol_placeholders})
                      AND s.attempt_count < s.max_attempts
                      AND j.status IN (?, ?) AND j.control_intent = ? AND j.deadline > ?
                    LIMIT 1
                    """,
                    (
                        ShardStatus.QUEUED.value,
                        *allowed_protocols,
                        JobStatus.QUEUED.value,
                        JobStatus.RUNNING.value,
                        ControlIntent.NONE.value,
                        _dump_time(current),
                    ),
                ).fetchone()
                is not None
            )
            if not fair_work_available:
                self._recover_stale_shards_in_transaction(
                    connection,
                    lease=lease,
                    now=current,
                    reclaimed_shards=reclaimed_shards,
                )
            for reclaimed_job_id, _reclaimed_shard_index, reclaimed_shard_id in sorted(
                reclaimed_shards,
                key=lambda item: (str(item[0]), item[1], str(item[2])),
            ):
                row = connection.execute(
                    f"""
                    SELECT s.* FROM lab_shard AS s
                    JOIN lab_job AS j ON j.job_id = s.job_id
                    WHERE s.job_id = ? AND s.shard_id = ?
                      AND s.status = ?
                      AND s.payload_protocol_version IN ({protocol_placeholders})
                      AND s.attempt_count < s.max_attempts
                      AND j.status IN (?, ?) AND j.control_intent = ? AND j.deadline > ?
                    """,
                    (
                        str(reclaimed_job_id),
                        str(reclaimed_shard_id),
                        ShardStatus.QUEUED.value,
                        *allowed_protocols,
                        JobStatus.QUEUED.value,
                        JobStatus.RUNNING.value,
                        ControlIntent.NONE.value,
                        _dump_time(current),
                    ),
                ).fetchone()
                if row is None:
                    continue
                job_row = self._load_job_row(connection, reclaimed_job_id)
                assert job_row is not None
                candidate = self._claim_preclaim_candidate_in_transaction(
                    connection,
                    row=row,
                    job_id=reclaimed_job_id,
                    job_created_at=_load_time(str(job_row["created_at"])),
                    job_row=job_row,
                    worker=worker,
                    shard_lease_seconds=shard_lease_seconds,
                    lease=lease,
                    now=current,
                    source_stage_store=source_stage_store,
                    source_wait_deadline=source_wait_deadline,
                    publication_deadline=publication_deadline,
                    v2_precondition=v2_precondition,
                    prevalidated_v2=prevalidated_v2,
                    use_fair_cursor=use_fair_cursor,
                )
                if isinstance(candidate, LabPreclaimRejection):
                    rejections.append(candidate)
                    continue
                if candidate is not None:
                    return selected(candidate)
            cursor_created_at: datetime | None = None
            cursor_job_id: UUID | None = None
            cursor_shard_index: int | None = None
            cursor_shard_id: UUID | None = None
            job_candidate: sqlite3.Row | None = None
            job_candidate_uses_shard_cursor = False
            if claim_cursor is None:
                job_candidate = connection.execute(
                    f"""
                    SELECT j.job_id, j.created_at
                    FROM lab_job AS j
                    WHERE j.status IN (?, ?)
                      AND j.control_intent = ?
                      AND j.deadline > ?
                      AND EXISTS (
                        SELECT 1 FROM lab_shard AS s
                        WHERE s.job_id = j.job_id
                          AND s.status = ?
                          AND s.payload_protocol_version IN ({protocol_placeholders})
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
                        *allowed_protocols,
                    ),
                ).fetchone()
            else:
                try:
                    cursor_created_at = _load_time(str(claim_cursor["claim_cursor_created_at"]))
                    cursor_job_id = _canonical_uuid_text(
                        claim_cursor["claim_cursor_job_id"],
                        field="lab_scheduler_state.claim_cursor_job_id",
                    )
                    cursor_index_value = claim_cursor["claim_cursor_shard_index"]
                    cursor_id_value = claim_cursor["claim_cursor_shard_id"]
                    if (cursor_index_value is None) != (cursor_id_value is None):
                        raise ValueError("persisted shard cursor is incomplete")
                    if cursor_index_value is not None:
                        cursor_shard_index = _strict_sqlite_int(
                            cursor_index_value,
                            field="lab_scheduler_state.claim_cursor_shard_index",
                            minimum=0,
                        )
                        cursor_shard_id = _canonical_uuid_text(
                            cursor_id_value,
                            field="lab_scheduler_state.claim_cursor_shard_id",
                        )
                except (TypeError, ValueError) as exc:
                    raise InvalidStoredJobError("invalid persisted claim job cursor") from exc
                cursor_created_at_dump = _dump_time(cursor_created_at)
                if cursor_shard_index is not None and cursor_shard_id is not None:
                    job_candidate = connection.execute(
                        f"""
                        SELECT j.job_id, j.created_at
                        FROM lab_job AS j
                        WHERE j.job_id = ? AND j.created_at = ?
                          AND j.status IN (?, ?)
                          AND j.control_intent = ? AND j.deadline > ?
                          AND EXISTS (
                            SELECT 1 FROM lab_shard AS s
                            WHERE s.job_id = j.job_id AND s.status = ?
                              AND s.payload_protocol_version IN ({protocol_placeholders})
                              AND s.attempt_count < s.max_attempts
                              AND (s.shard_index > ? OR (
                                  s.shard_index = ? AND s.shard_id > ?
                              ))
                          )
                        """,
                        (
                            str(cursor_job_id),
                            cursor_created_at_dump,
                            JobStatus.QUEUED.value,
                            JobStatus.RUNNING.value,
                            ControlIntent.NONE.value,
                            _dump_time(current),
                            ShardStatus.QUEUED.value,
                            *allowed_protocols,
                            cursor_shard_index,
                            cursor_shard_index,
                            str(cursor_shard_id),
                        ),
                    ).fetchone()
                    job_candidate_uses_shard_cursor = job_candidate is not None
                if job_candidate is None:
                    job_candidate = connection.execute(
                        f"""
                        SELECT j.job_id, j.created_at
                        FROM lab_job AS j
                        WHERE j.status IN (?, ?)
                          AND j.control_intent = ? AND j.deadline > ?
                          AND (j.created_at > ? OR (
                              j.created_at = ? AND j.job_id > ?
                          ))
                          AND EXISTS (
                            SELECT 1 FROM lab_shard AS s
                            WHERE s.job_id = j.job_id AND s.status = ?
                              AND s.payload_protocol_version IN ({protocol_placeholders})
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
                            cursor_created_at_dump,
                            cursor_created_at_dump,
                            str(cursor_job_id),
                            ShardStatus.QUEUED.value,
                            *allowed_protocols,
                        ),
                    ).fetchone()
                if job_candidate is None:
                    job_candidate = connection.execute(
                        f"""
                        SELECT j.job_id, j.created_at
                        FROM lab_job AS j
                        WHERE j.status IN (?, ?)
                          AND j.control_intent = ? AND j.deadline > ?
                          AND EXISTS (
                            SELECT 1 FROM lab_shard AS s
                            WHERE s.job_id = j.job_id AND s.status = ?
                              AND s.payload_protocol_version IN ({protocol_placeholders})
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
                            *allowed_protocols,
                        ),
                    ).fetchone()
            if job_candidate is None:
                return selected(None)
            try:
                job_id = _canonical_uuid_text(
                    job_candidate["job_id"],
                    field="lab_job.job_id",
                )
                job_created_at = _load_time(str(job_candidate["created_at"]))
            except (TypeError, ValueError) as exc:
                raise InvalidStoredJobError("invalid claimable job identity") from exc
            shard_cursor_parameters: tuple[object, ...] = ()
            shard_cursor_predicate = ""
            if (
                job_candidate_uses_shard_cursor
                and cursor_job_id == job_id
                and cursor_shard_index is not None
                and cursor_shard_id is not None
            ):
                shard_cursor_predicate = """
                  AND (shard_index > ? OR (shard_index = ? AND shard_id > ?))
                """
                shard_cursor_parameters = (
                    cursor_shard_index,
                    cursor_shard_index,
                    str(cursor_shard_id),
                )
            rows = connection.execute(
                f"""
                SELECT * FROM lab_shard INDEXED BY ix_lab_shard_preclaim_candidate
                WHERE job_id = ? AND status = 'queued'
                  AND payload_protocol_version IN ({protocol_placeholders})
                  AND attempt_count < max_attempts
                  {shard_cursor_predicate}
                ORDER BY shard_index, shard_id
                LIMIT ?
                """,
                (
                    str(job_id),
                    *allowed_protocols,
                    *shard_cursor_parameters,
                    PRECLAIM_CANDIDATE_BATCH_SIZE,
                ),
            ).fetchall()
            if not rows:
                raise InvalidStoredJobError("claimable job has no claimable shard")
            job_row = self._load_job_row(connection, job_id)
            assert job_row is not None
            deferred_deadline_error: ValueError | None = None
            for row in rows:
                try:
                    candidate = self._claim_preclaim_candidate_in_transaction(
                        connection,
                        row=row,
                        job_id=job_id,
                        job_created_at=job_created_at,
                        job_row=job_row,
                        worker=worker,
                        shard_lease_seconds=shard_lease_seconds,
                        lease=lease,
                        now=current,
                        source_stage_store=source_stage_store,
                        source_wait_deadline=source_wait_deadline,
                        publication_deadline=publication_deadline,
                        v2_precondition=v2_precondition,
                        prevalidated_v2=prevalidated_v2,
                        use_fair_cursor=use_fair_cursor,
                    )
                except ValueError as exc:
                    if str(exc) != "publication_deadline must not exceed shard lease expiry":
                        raise
                    deferred_deadline_error = exc
                    candidate = LabPreclaimRejection(
                        job_id=job_id,
                        shard_id=_canonical_uuid_text(row["shard_id"], field="lab_shard.shard_id"),
                        payload_hash=str(row["payload_hash"]),
                        reason="source_preclaim_rejected",
                    )
                if isinstance(candidate, LabPreclaimRejection):
                    rejections.append(candidate)
                    continue
                if candidate is not None:
                    return selected(candidate)
            if rejections:
                last_rows = rows
                last_job_id = job_id
                last_job_created_at = job_created_at
                if len(rows) < PRECLAIM_CANDIDATE_BATCH_SIZE:
                    next_job = connection.execute(
                        f"""
                        SELECT j.job_id, j.created_at
                        FROM lab_job AS j
                        WHERE j.status IN (?, ?)
                          AND j.control_intent = ? AND j.deadline > ?
                          AND (j.created_at > ? OR (
                              j.created_at = ? AND j.job_id > ?
                          ))
                          AND EXISTS (
                            SELECT 1 FROM lab_shard AS s
                            WHERE s.job_id = j.job_id AND s.status = ?
                              AND s.payload_protocol_version IN ({protocol_placeholders})
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
                            _dump_time(job_created_at),
                            _dump_time(job_created_at),
                            str(job_id),
                            ShardStatus.QUEUED.value,
                            *allowed_protocols,
                        ),
                    ).fetchone()
                    if next_job is None:
                        next_job = connection.execute(
                            f"""
                            SELECT j.job_id, j.created_at
                            FROM lab_job AS j
                            WHERE j.job_id <> ? AND j.status IN (?, ?)
                              AND j.control_intent = ? AND j.deadline > ?
                              AND EXISTS (
                                SELECT 1 FROM lab_shard AS s
                                WHERE s.job_id = j.job_id AND s.status = ?
                                  AND s.payload_protocol_version IN ({protocol_placeholders})
                                  AND s.attempt_count < s.max_attempts
                              )
                            ORDER BY j.created_at, j.job_id
                            LIMIT 1
                            """,
                            (
                                str(job_id),
                                JobStatus.QUEUED.value,
                                JobStatus.RUNNING.value,
                                ControlIntent.NONE.value,
                                _dump_time(current),
                                ShardStatus.QUEUED.value,
                                *allowed_protocols,
                            ),
                        ).fetchone()
                    if next_job is None and deferred_deadline_error is not None:
                        raise deferred_deadline_error
                    if next_job is not None:
                        next_job_id = _canonical_uuid_text(
                            next_job["job_id"], field="lab_job.job_id"
                        )
                        next_job_created_at = _load_time(str(next_job["created_at"]))
                        next_rows = connection.execute(
                            f"""
                            SELECT * FROM lab_shard INDEXED BY ix_lab_shard_preclaim_candidate
                            WHERE job_id = ? AND status = 'queued'
                              AND payload_protocol_version IN ({protocol_placeholders})
                              AND attempt_count < max_attempts
                            ORDER BY shard_index, shard_id
                            LIMIT ?
                            """,
                            (
                                str(next_job_id),
                                *allowed_protocols,
                                PRECLAIM_CANDIDATE_BATCH_SIZE - len(rows),
                            ),
                        ).fetchall()
                        next_job_row = self._load_job_row(connection, next_job_id)
                        assert next_job_row is not None
                        for row in next_rows:
                            candidate = self._claim_preclaim_candidate_in_transaction(
                                connection,
                                row=row,
                                job_id=next_job_id,
                                job_created_at=next_job_created_at,
                                job_row=next_job_row,
                                worker=worker,
                                shard_lease_seconds=shard_lease_seconds,
                                lease=lease,
                                now=current,
                                source_stage_store=source_stage_store,
                                source_wait_deadline=source_wait_deadline,
                                publication_deadline=publication_deadline,
                                v2_precondition=v2_precondition,
                                prevalidated_v2=prevalidated_v2,
                                use_fair_cursor=use_fair_cursor,
                            )
                            if isinstance(candidate, LabPreclaimRejection):
                                rejections.append(candidate)
                                continue
                            if candidate is not None:
                                return selected(candidate)
                        if next_rows:
                            last_rows = next_rows
                            last_job_id = next_job_id
                            last_job_created_at = next_job_created_at
                last_row = last_rows[-1]
                self._store_preclaim_cursor(
                    connection,
                    job_created_at=last_job_created_at,
                    job_id=last_job_id,
                    shard_index=_strict_sqlite_int(
                        last_row["shard_index"],
                        field="lab_shard.shard_index",
                        minimum=0,
                    ),
                    shard_id=_canonical_uuid_text(
                        last_row["shard_id"],
                        field="lab_shard.shard_id",
                    ),
                    now=current,
                    use_fair_cursor=use_fair_cursor,
                )
        return selected(None)

    def claim_next_source_stage(
        self,
        *,
        shard_lease_seconds: int,
        lease: LabLeaseRecord,
        now: datetime,
        source_stage_store: LabSourceStageStore,
        source_wait_deadline: datetime,
        publication_deadline: datetime,
        v2_precondition: Callable[[StrategyShardPayloadV2, LabShardClaimV2, datetime], None],
        include_diagnostics: bool = False,
    ) -> LabShardClaimV2 | LabClaimSelection | None:
        """Create a V2 source attempt without selecting a real worker.

        ``v2-unassigned`` is a protocol route, not a worker identity.  A worker
        can atomically consume a published V2 entry after the D ledger gate.
        """

        selected = self.claim_next_shard(
            worker_id=V2_UNASSIGNED_WORKER_ID,
            shard_lease_seconds=shard_lease_seconds,
            lease=lease,
            now=now,
            source_stage_store=source_stage_store,
            source_wait_deadline=source_wait_deadline,
            publication_deadline=publication_deadline,
            allowed_payload_protocol_versions=(2,),
            v2_precondition=v2_precondition,
            include_diagnostics=include_diagnostics,
        )
        if isinstance(selected, LabClaimSelection) or selected is None:
            return selected
        if not isinstance(
            selected, LabShardClaimV2
        ):  # pragma: no cover - protocol filter invariant
            raise InvalidStoredJobError("source-stage claim did not select a V2 attempt")
        return selected

    def list_active_claims(
        self,
        lease: LabLeaseRecord,
        *,
        now: datetime,
        initial_lease_seconds: int,
    ) -> tuple[LabShardClaim | LabShardClaimV2, ...]:
        if initial_lease_seconds < 1:
            raise ValueError("initial_lease_seconds must be positive")
        current = _utc(now)
        with self._read_transaction() as connection:
            self._validate_lease(connection, lease, now=current)
            rows = connection.execute(
                """
                SELECT
                    publication.*,
                    s.job_id AS shard_job_id,
                    s.shard_id AS shard_shard_id,
                    s.shard_index AS shard_shard_index,
                    s.claim_token AS shard_claim_token,
                    s.claim_generation AS shard_claim_generation,
                    s.scheduler_fencing_token AS shard_scheduler_fencing_token,
                    s.worker_id AS shard_worker_id,
                    s.claimed_at AS shard_claimed_at,
                    s.adapter_id AS shard_adapter_id,
                    s.adapter_version AS shard_adapter_version,
                    s.plan_hash AS shard_plan_hash,
                    s.payload_json AS shard_payload_json,
                    s.payload_hash AS shard_payload_hash,
                    s.phase AS shard_phase,
                    s.work_unit_name AS shard_work_unit_name,
                    s.work_units AS shard_work_units,
                    s.static_duration_ms AS shard_static_duration_ms,
                    j.spec_hash AS job_spec_hash
                FROM lab_shard AS s
                JOIN lab_job AS j ON j.job_id = s.job_id
                LEFT JOIN lab_claim_publication AS publication
                  ON publication.attempt_id = s.claim_token
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
            claims: list[LabShardClaim | LabShardClaimV2] = []
            for row in rows:
                try:
                    claimed_at = _load_time(str(row["shard_claimed_at"]))
                    definition = LabShardDefinition(
                        shard_id=_canonical_uuid_text(
                            row["shard_shard_id"], field="lab_shard.shard_id"
                        ),
                        shard_index=_strict_sqlite_int(
                            row["shard_shard_index"],
                            field="lab_shard.shard_index",
                            minimum=0,
                        ),
                        adapter_id=str(row["shard_adapter_id"]),
                        adapter_version=str(row["shard_adapter_version"]),
                        plan_hash=str(row["shard_plan_hash"]),
                        payload_json=str(row["shard_payload_json"]),
                        payload_hash=str(row["shard_payload_hash"]),
                        work_plan=(
                            LabShardWorkPlan(
                                phase=str(row["shard_phase"]),
                                work_unit_name=str(row["shard_work_unit_name"]),
                                work_units=_strict_sqlite_int(
                                    row["shard_work_units"],
                                    field="lab_shard.work_units",
                                    minimum=1,
                                    maximum=SQLITE_SIGNED_INTEGER_MAX,
                                ),
                                static_duration_ms=_strict_sqlite_int(
                                    row["shard_static_duration_ms"],
                                    field="lab_shard.static_duration_ms",
                                    minimum=1,
                                    maximum=SQLITE_SIGNED_INTEGER_MAX,
                                ),
                            )
                            if row["shard_phase"] is not None
                            else None
                        ),
                    )
                    payload = self._external_payload_v2(definition.payload_json)
                    claim_token = _canonical_uuid_text(
                        row["shard_claim_token"], field="lab_shard.claim_token"
                    )
                    if payload is not None:
                        if row["attempt_id"] is None:
                            continue
                        publication = _claim_publication_record_from_row(row)
                        if publication.status not in {
                            ClaimPublicationStatus.READY_TO_PUBLISH,
                            ClaimPublicationStatus.PUBLISHED,
                        }:
                            continue
                        if publication.final_claim_bytes is None:
                            raise InvalidStoredJobError("visible v2 publication has no final claim")
                        final_claim = strict_model_validate_canonical_json(
                            LabShardClaimV2,
                            publication.final_claim_bytes.decode("utf-8"),
                        )
                        if canonical_model_json_bytes(final_claim) != publication.final_claim_bytes:
                            raise InvalidStoredJobError(
                                "visible v2 publication final claim is not canonical"
                            )
                        if (
                            LabClaimPublicationIdentity.from_claim(final_claim)
                            != publication.identity
                            or final_claim.claim_token != claim_token
                        ):
                            raise InvalidStoredJobError(
                                "visible v2 publication does not match the running shard attempt"
                            )
                        claims.append(final_claim)
                        continue
                    claims.append(
                        LabShardClaim(
                            job_id=_canonical_uuid_text(
                                row["shard_job_id"],
                                field="lab_shard.job_id",
                            ),
                            spec_hash=str(row["job_spec_hash"]),
                            definition=definition,
                            worker_id=str(row["shard_worker_id"]),
                            claim_token=claim_token,
                            claim_generation=_strict_sqlite_int(
                                row["shard_claim_generation"],
                                field="lab_shard.claim_generation",
                                minimum=1,
                            ),
                            scheduler_fencing_token=_strict_sqlite_int(
                                row["shard_scheduler_fencing_token"],
                                field="lab_shard.scheduler_fencing_token",
                                minimum=1,
                            ),
                            claimed_at=claimed_at,
                            lease_expires_at=claimed_at + timedelta(seconds=initial_lease_seconds),
                        )
                    )
                except Exception as exc:
                    raise InvalidStoredJobError(
                        f"invalid active claim for shard {row['shard_shard_id']}: {exc}"
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
                _canonical_model_json(report),
                receipt.status,
                receipt.reason,
                _canonical_model_json(receipt),
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

_V5_SCHEMA_STATEMENTS = tuple(
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

_LEDGER_EPOCH_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_ledger_epoch (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    mutation_epoch INTEGER NOT NULL
        CHECK (typeof(mutation_epoch) = 'integer' AND mutation_epoch >= 0)
)
"""
_LEDGER_CHAIN_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_ledger_chain (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    chain_generation INTEGER NOT NULL
        CHECK (typeof(chain_generation) = 'integer' AND chain_generation >= 0),
    head_hash TEXT NOT NULL
        CHECK (typeof(head_hash) = 'text' AND length(head_hash) = 64)
)
"""
_LEDGER_CHAIN_ENTRY_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_ledger_chain_entry (
    chain_generation INTEGER PRIMARY KEY
        CHECK (typeof(chain_generation) = 'integer' AND chain_generation >= 0),
    mutation_epoch INTEGER NOT NULL
        CHECK (typeof(mutation_epoch) = 'integer' AND mutation_epoch >= 0),
    previous_hash TEXT NOT NULL
        CHECK (typeof(previous_hash) = 'text' AND length(previous_hash) = 64),
    entry_hash TEXT NOT NULL
        CHECK (typeof(entry_hash) = 'text' AND length(entry_hash) = 64)
)
"""
_LAB_JOB_LIST_SUMMARY_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_job_list_summary (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    total_count INTEGER NOT NULL
        CHECK (typeof(total_count) = 'integer' AND total_count >= 0)
)
"""
_LAB_FINALIZATION_CANDIDATE_SUMMARY_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_finalization_candidate_summary (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    total_count INTEGER NOT NULL
        CHECK (typeof(total_count) = 'integer' AND total_count >= 0)
)
"""
_FINALIZATION_CANDIDATE_PREDICATE = """
status = 'running'
AND control_intent = 'none'
AND result_state = 'ready'
AND requires_complete_result = 1
AND result_contract_version = 'p1.4b-complete-result-v1'
""".strip()


def _finalization_candidate_predicate(reference: str = "") -> str:
    return "\nAND ".join(
        (
            f"{reference}status = 'running'",
            f"{reference}control_intent = 'none'",
            f"{reference}result_state = 'ready'",
            f"{reference}requires_complete_result = 1",
            f"{reference}result_contract_version = '{COMPLETE_RESULT_CONTRACT_VERSION}'",
        )
    )


_LEDGER_EPOCH_TABLES = (
    "lab_job",
    "lab_command",
    "lab_shard",
    "lab_event",
    "lab_lease",
    "lab_artifact",
    "lab_worker_report",
    "lab_scheduler_state",
    "lab_artifact_commit",
    "lab_job_result_artifact",
)


def _ledger_epoch_trigger_statement(table: str, action: str) -> str:
    return f"""
CREATE TRIGGER IF NOT EXISTS trg_lab_epoch_{table}_{action.lower()}
AFTER {action} ON {table}
BEGIN
    INSERT INTO lab_ledger_chain_entry (
        chain_generation, mutation_epoch, previous_hash, entry_hash
    )
    SELECT chain.chain_generation + 1,
           epoch.mutation_epoch + 1,
           chain.head_hash,
           lower(hex(randomblob(32)))
    FROM lab_ledger_chain AS chain
    JOIN lab_ledger_epoch AS epoch ON epoch.singleton = 1
    WHERE chain.singleton = 1;
    UPDATE lab_ledger_epoch
    SET mutation_epoch = mutation_epoch + 1
    WHERE singleton = 1;
    UPDATE lab_ledger_chain
    SET chain_generation = chain_generation + 1,
        head_hash = (
            SELECT entry_hash FROM lab_ledger_chain_entry
            WHERE chain_generation = lab_ledger_chain.chain_generation + 1
        )
    WHERE singleton = 1;
END
"""


_LEDGER_EPOCH_TRIGGER_SQL = {
    f"trg_lab_epoch_{table}_{action.lower()}": _ledger_epoch_trigger_statement(table, action)
    for table in _LEDGER_EPOCH_TABLES
    for action in ("INSERT", "UPDATE", "DELETE")
}
_LAB_JOB_SUMMARY_TRIGGER_SQL = {
    "trg_lab_job_list_summary_insert": (
        """
CREATE TRIGGER IF NOT EXISTS trg_lab_job_list_summary_insert
AFTER INSERT ON lab_job
BEGIN
    UPDATE lab_job_list_summary SET total_count = total_count + 1 WHERE singleton = 1;
    UPDATE lab_finalization_candidate_summary
    SET total_count = total_count + CASE WHEN """
        + _finalization_candidate_predicate("NEW.")
        + """ THEN 1 ELSE 0 END
    WHERE singleton = 1;
END
"""
    ),
    "trg_lab_job_list_summary_delete": (
        """
CREATE TRIGGER IF NOT EXISTS trg_lab_job_list_summary_delete
AFTER DELETE ON lab_job
BEGIN
    UPDATE lab_job_list_summary SET total_count = total_count - 1 WHERE singleton = 1;
    UPDATE lab_finalization_candidate_summary
    SET total_count = total_count - CASE WHEN """
        + _finalization_candidate_predicate("OLD.")
        + """ THEN 1 ELSE 0 END
    WHERE singleton = 1;
END
"""
    ),
    "trg_lab_job_list_summary_update": (
        """
CREATE TRIGGER IF NOT EXISTS trg_lab_job_list_summary_update
AFTER UPDATE OF status, control_intent, result_state, requires_complete_result,
                result_contract_version ON lab_job
BEGIN
    UPDATE lab_finalization_candidate_summary
    SET total_count = total_count
        + CASE WHEN """
        + _finalization_candidate_predicate("NEW.")
        + """ THEN 1 ELSE 0 END
        - CASE WHEN """
        + _finalization_candidate_predicate("OLD.")
        + """ THEN 1 ELSE 0 END
    WHERE singleton = 1;
END
"""
    ),
}
_V6_SCHEMA_STATEMENTS = _V5_SCHEMA_STATEMENTS + (
    _LEDGER_EPOCH_TABLE_STATEMENT,
    "INSERT OR IGNORE INTO lab_ledger_epoch (singleton, mutation_epoch) VALUES (1, 0)",
    *_LEDGER_EPOCH_TRIGGER_SQL.values(),
)
_LEDGER_CHAIN_SEED_STATEMENT = f"""
INSERT OR IGNORE INTO lab_ledger_chain (
    singleton, chain_generation, head_hash
) VALUES (1, 0, '{_LEDGER_CHAIN_GENESIS_HASH}')
"""
_LEDGER_CHAIN_ENTRY_SEED_STATEMENT = f"""
INSERT OR IGNORE INTO lab_ledger_chain_entry (
    chain_generation, mutation_epoch, previous_hash, entry_hash
) VALUES (0, 0, '{_LEDGER_CHAIN_GENESIS_HASH}', '{_LEDGER_CHAIN_GENESIS_HASH}')
"""
_LAB_JOB_LIST_SUMMARY_SEED_STATEMENT = """
INSERT OR IGNORE INTO lab_job_list_summary (singleton, total_count)
SELECT 1, COUNT(*) FROM lab_job
"""
_LAB_FINALIZATION_CANDIDATE_SUMMARY_SEED_STATEMENT = f"""
INSERT OR IGNORE INTO lab_finalization_candidate_summary (singleton, total_count)
SELECT 1, COUNT(*) FROM lab_job
WHERE {_FINALIZATION_CANDIDATE_PREDICATE}
"""
_LAB_JOB_CREATED_KEYSET_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_job_created_keyset
ON lab_job(created_at DESC, job_id DESC)
"""
_LAB_JOB_FINALIZATION_CANDIDATE_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_job_finalization_candidates
ON lab_job(
    status, control_intent, result_state, requires_complete_result,
    result_contract_version, updated_at DESC, job_id DESC
)
"""
_CANONICAL_UTC_TIMESTAMP_GLOB = (
    "'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:"
    "[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]+00:00'"
)
_V7_SCHEMA_STATEMENTS = _V6_SCHEMA_STATEMENTS + (
    _LEDGER_CHAIN_TABLE_STATEMENT,
    _LEDGER_CHAIN_ENTRY_TABLE_STATEMENT,
    _LAB_JOB_LIST_SUMMARY_TABLE_STATEMENT,
    _LAB_FINALIZATION_CANDIDATE_SUMMARY_TABLE_STATEMENT,
    _LEDGER_CHAIN_SEED_STATEMENT,
    _LEDGER_CHAIN_ENTRY_SEED_STATEMENT,
    _LAB_JOB_LIST_SUMMARY_SEED_STATEMENT,
    _LAB_FINALIZATION_CANDIDATE_SUMMARY_SEED_STATEMENT,
    _LAB_JOB_CREATED_KEYSET_INDEX_STATEMENT,
    _LAB_JOB_FINALIZATION_CANDIDATE_INDEX_STATEMENT,
    *_LAB_JOB_SUMMARY_TRIGGER_SQL.values(),
)
_CLAIM_PUBLICATION_TABLE_STATEMENT = f"""
CREATE TABLE IF NOT EXISTS lab_claim_publication (
    attempt_id TEXT PRIMARY KEY CHECK (typeof(attempt_id) = 'text'),
    job_id TEXT NOT NULL CHECK (typeof(job_id) = 'text'),
    shard_id TEXT NOT NULL CHECK (typeof(shard_id) = 'text'),
    claim_token TEXT NOT NULL CHECK (typeof(claim_token) = 'text'),
    claim_generation INTEGER NOT NULL CHECK (
        typeof(claim_generation) = 'integer' AND claim_generation >= 1
    ),
    scheduler_fencing_token INTEGER NOT NULL CHECK (
        typeof(scheduler_fencing_token) = 'integer' AND scheduler_fencing_token >= 1
    ),
    worker_id TEXT NOT NULL CHECK (typeof(worker_id) = 'text'),
    spec_hash TEXT NOT NULL CHECK (typeof(spec_hash) = 'text' AND length(spec_hash) = 64),
    plan_hash TEXT NOT NULL CHECK (typeof(plan_hash) = 'text' AND length(plan_hash) = 64),
    payload_hash TEXT NOT NULL CHECK (typeof(payload_hash) = 'text' AND length(payload_hash) = 64),
    claim_preimage_bytes BLOB NOT NULL CHECK (typeof(claim_preimage_bytes) = 'blob'),
    claim_preimage_hash TEXT NOT NULL CHECK (
        typeof(claim_preimage_hash) = 'text' AND length(claim_preimage_hash) = 64
    ),
    claim_protocol TEXT NOT NULL CHECK (claim_protocol = 'rquant-lab-shard-claim'),
    claim_protocol_version TEXT NOT NULL CHECK (claim_protocol_version = 'v2'),
    source_wait_deadline TEXT NOT NULL CHECK (
        typeof(source_wait_deadline) = 'text'
        AND length(source_wait_deadline) = 32
        AND source_wait_deadline GLOB {_CANONICAL_UTC_TIMESTAMP_GLOB}
        AND julianday(source_wait_deadline) IS NOT NULL
AND substr(source_wait_deadline, 1, 10) = strftime('%Y-%m-%d', source_wait_deadline, '+0 days')
AND substr(source_wait_deadline, 12, 8) = strftime('%H:%M:%S', source_wait_deadline, '+0 seconds')
    ),
    publication_deadline TEXT NOT NULL CHECK (
        typeof(publication_deadline) = 'text'
        AND length(publication_deadline) = 32
        AND publication_deadline GLOB {_CANONICAL_UTC_TIMESTAMP_GLOB}
        AND julianday(publication_deadline) IS NOT NULL
AND substr(publication_deadline, 1, 10) = strftime('%Y-%m-%d', publication_deadline, '+0 days')
AND substr(publication_deadline, 12, 8) = strftime('%H:%M:%S', publication_deadline, '+0 seconds')
    ),
    source_stage_authority_bytes BLOB NOT NULL CHECK (
        typeof(source_stage_authority_bytes) = 'blob'
    ),
    source_stage_authority_hash TEXT NOT NULL CHECK (
        typeof(source_stage_authority_hash) = 'text'
        AND length(source_stage_authority_hash) = 64
    ),
    source_stage_binding_bytes BLOB,
    source_stage_binding_hash TEXT CHECK (
        source_stage_binding_hash IS NULL
        OR (
            typeof(source_stage_binding_hash) = 'text'
            AND length(source_stage_binding_hash) = 64
        )
    ),
    source_intent_bytes BLOB,
    source_intent_hash TEXT CHECK (
        source_intent_hash IS NULL
        OR (typeof(source_intent_hash) = 'text' AND length(source_intent_hash) = 64)
    ),
    source_operation_id TEXT UNIQUE CHECK (
        source_operation_id IS NULL
        OR (typeof(source_operation_id) = 'text' AND length(source_operation_id) = 64)
    ),
    source_operation_hash TEXT CHECK (
        source_operation_hash IS NULL
        OR (typeof(source_operation_hash) = 'text' AND length(source_operation_hash) = 64)
    ),
    queued_source_stage_record_hash TEXT CHECK (
        queued_source_stage_record_hash IS NULL
        OR (
            typeof(queued_source_stage_record_hash) = 'text'
            AND length(queued_source_stage_record_hash) = 64
            AND queued_source_stage_record_hash <> (
                '0000000000000000000000000000000000000000000000000000000000000000'
            )
        )
    ),
    ready_source_stage_record_bytes BLOB,
    ready_source_stage_record_hash TEXT CHECK (
        ready_source_stage_record_hash IS NULL
        OR (
            typeof(ready_source_stage_record_hash) = 'text'
            AND length(ready_source_stage_record_hash) = 64
        )
    ),
    verified_source_outcome_hash TEXT CHECK (
        verified_source_outcome_hash IS NULL
        OR (
            typeof(verified_source_outcome_hash) = 'text'
            AND length(verified_source_outcome_hash) = 64
        )
    ),
    verified_evidence_chain_hash TEXT CHECK (
        verified_evidence_chain_hash IS NULL
        OR (
            typeof(verified_evidence_chain_hash) = 'text'
            AND length(verified_evidence_chain_hash) = 64
        )
    ),
    source_use_plan_bytes BLOB,
    source_use_plan_hash TEXT CHECK (
        source_use_plan_hash IS NULL
        OR (typeof(source_use_plan_hash) = 'text' AND length(source_use_plan_hash) = 64)
    ),
    final_claim_bytes BLOB,
    final_claim_hash TEXT CHECK (
        final_claim_hash IS NULL
        OR (typeof(final_claim_hash) = 'text' AND length(final_claim_hash) = 64)
    ),
    current_claim_receipt_bytes BLOB,
    current_claim_receipt_hash TEXT CHECK (
        current_claim_receipt_hash IS NULL
        OR (typeof(current_claim_receipt_hash) = 'text' AND length(current_claim_receipt_hash) = 64)
    ),
    spool_receipt_bytes BLOB,
    spool_receipt_hash TEXT CHECK (
        spool_receipt_hash IS NULL
        OR (typeof(spool_receipt_hash) = 'text' AND length(spool_receipt_hash) = 64)
    ),
    status TEXT NOT NULL CHECK (
        status IN ('HELD_SOURCE','SOURCE_QUEUED','READY_TO_PUBLISH','PUBLISHED','ABORTED')
    ),
    version INTEGER NOT NULL CHECK (typeof(version) = 'integer' AND version >= 0),
    created_at TEXT NOT NULL CHECK (
        typeof(created_at) = 'text'
        AND length(created_at) = 32
        AND created_at GLOB {_CANONICAL_UTC_TIMESTAMP_GLOB}
    ),
    updated_at TEXT NOT NULL CHECK (
        typeof(updated_at) = 'text'
        AND length(updated_at) = 32
        AND updated_at GLOB {_CANONICAL_UTC_TIMESTAMP_GLOB}
    ),
    queued_at TEXT,
    ready_at TEXT,
    published_at TEXT,
    aborted_at TEXT,
    terminal_reason TEXT,
    record_commitment TEXT NOT NULL CHECK (
        typeof(record_commitment) = 'text' AND length(record_commitment) = 64
    ),
    CHECK (source_wait_deadline <= publication_deadline),
    UNIQUE (
        job_id,
        shard_id,
        claim_token,
        claim_generation,
        scheduler_fencing_token,
        worker_id,
        spec_hash,
        plan_hash,
        payload_hash
    ),
    FOREIGN KEY (job_id, shard_id) REFERENCES lab_shard(job_id, shard_id) ON DELETE RESTRICT,
    CHECK ((source_stage_binding_bytes IS NULL) = (source_stage_binding_hash IS NULL)),
    CHECK ((source_intent_bytes IS NULL) = (source_intent_hash IS NULL)),
    CHECK ((ready_source_stage_record_bytes IS NULL) = (ready_source_stage_record_hash IS NULL)),
    CHECK ((source_use_plan_bytes IS NULL) = (source_use_plan_hash IS NULL)),
    CHECK ((final_claim_bytes IS NULL) = (final_claim_hash IS NULL)),
    CHECK ((current_claim_receipt_bytes IS NULL) = (current_claim_receipt_hash IS NULL)),
    CHECK ((spool_receipt_bytes IS NULL) = (spool_receipt_hash IS NULL)),
    CHECK (
        (
            status = 'HELD_SOURCE'
            AND version = 0
            AND source_stage_binding_bytes IS NULL
            AND source_intent_bytes IS NULL
            AND source_operation_id IS NULL
            AND source_operation_hash IS NULL
            AND queued_source_stage_record_hash IS NULL
            AND ready_source_stage_record_bytes IS NULL
            AND verified_source_outcome_hash IS NULL
            AND verified_evidence_chain_hash IS NULL
            AND source_use_plan_bytes IS NULL
            AND final_claim_bytes IS NULL
            AND current_claim_receipt_bytes IS NULL
            AND spool_receipt_bytes IS NULL
            AND queued_at IS NULL
            AND ready_at IS NULL
            AND published_at IS NULL
            AND aborted_at IS NULL
            AND terminal_reason IS NULL
        )
        OR (
            status = 'SOURCE_QUEUED'
            AND version = 1
            AND source_stage_binding_bytes IS NOT NULL
            AND source_intent_bytes IS NOT NULL
            AND source_operation_id IS NOT NULL
            AND source_operation_hash IS NOT NULL
            AND queued_source_stage_record_hash IS NOT NULL
            AND ready_source_stage_record_bytes IS NULL
            AND verified_source_outcome_hash IS NULL
            AND verified_evidence_chain_hash IS NULL
            AND source_use_plan_bytes IS NULL
            AND final_claim_bytes IS NULL
            AND current_claim_receipt_bytes IS NULL
            AND spool_receipt_bytes IS NULL
            AND queued_at IS NOT NULL
            AND ready_at IS NULL
            AND published_at IS NULL
            AND aborted_at IS NULL
            AND terminal_reason IS NULL
        )
        OR (
            status = 'READY_TO_PUBLISH'
            AND version = 2
            AND source_stage_binding_bytes IS NOT NULL
            AND source_intent_bytes IS NOT NULL
            AND source_operation_id IS NOT NULL
            AND source_operation_hash IS NOT NULL
            AND queued_source_stage_record_hash IS NOT NULL
            AND ready_source_stage_record_bytes IS NOT NULL
            AND verified_source_outcome_hash IS NOT NULL
            AND verified_evidence_chain_hash IS NOT NULL
            AND source_use_plan_bytes IS NOT NULL
            AND final_claim_bytes IS NOT NULL
            AND current_claim_receipt_bytes IS NOT NULL
            AND spool_receipt_bytes IS NULL
            AND queued_at IS NOT NULL
            AND ready_at IS NOT NULL
            AND published_at IS NULL
            AND aborted_at IS NULL
            AND terminal_reason IS NULL
        )
        OR (
            status = 'PUBLISHED'
            AND version = 3
            AND source_stage_binding_bytes IS NOT NULL
            AND source_intent_bytes IS NOT NULL
            AND source_operation_id IS NOT NULL
            AND source_operation_hash IS NOT NULL
            AND queued_source_stage_record_hash IS NOT NULL
            AND ready_source_stage_record_bytes IS NOT NULL
            AND verified_source_outcome_hash IS NOT NULL
            AND verified_evidence_chain_hash IS NOT NULL
            AND source_use_plan_bytes IS NOT NULL
            AND final_claim_bytes IS NOT NULL
            AND current_claim_receipt_bytes IS NOT NULL
            AND spool_receipt_bytes IS NOT NULL
            AND queued_at IS NOT NULL
            AND ready_at IS NOT NULL
            AND published_at IS NOT NULL
            AND aborted_at IS NULL
            AND terminal_reason IS NULL
        )
        OR (
            status = 'ABORTED'
            AND version IN (1, 2, 3)
            AND aborted_at IS NOT NULL
            AND published_at IS NULL
            AND spool_receipt_bytes IS NULL
            AND terminal_reason IS NOT NULL
            AND (
                (
                    version = 1
                    AND source_stage_binding_bytes IS NULL
                    AND source_intent_bytes IS NULL
                    AND source_operation_id IS NULL
                    AND source_operation_hash IS NULL
                    AND queued_source_stage_record_hash IS NULL
                    AND ready_source_stage_record_bytes IS NULL
                    AND verified_source_outcome_hash IS NULL
                    AND verified_evidence_chain_hash IS NULL
                    AND source_use_plan_bytes IS NULL
                    AND final_claim_bytes IS NULL
                    AND current_claim_receipt_bytes IS NULL
                    AND queued_at IS NULL
                    AND ready_at IS NULL
                )
                OR (
                    version = 2
                    AND source_stage_binding_bytes IS NOT NULL
                    AND source_intent_bytes IS NOT NULL
                    AND source_operation_id IS NOT NULL
                    AND source_operation_hash IS NOT NULL
                    AND queued_source_stage_record_hash IS NOT NULL
                    AND ready_source_stage_record_bytes IS NULL
                    AND verified_source_outcome_hash IS NULL
                    AND verified_evidence_chain_hash IS NULL
                    AND source_use_plan_bytes IS NULL
                    AND final_claim_bytes IS NULL
                    AND current_claim_receipt_bytes IS NULL
                    AND queued_at IS NOT NULL
                    AND ready_at IS NULL
                )
                OR (
                    version = 3
                    AND source_stage_binding_bytes IS NOT NULL
                    AND source_intent_bytes IS NOT NULL
                    AND source_operation_id IS NOT NULL
                    AND source_operation_hash IS NOT NULL
                    AND queued_source_stage_record_hash IS NOT NULL
                    AND ready_source_stage_record_bytes IS NOT NULL
                    AND verified_source_outcome_hash IS NOT NULL
                    AND verified_evidence_chain_hash IS NOT NULL
                    AND source_use_plan_bytes IS NOT NULL
                    AND final_claim_bytes IS NOT NULL
                    AND current_claim_receipt_bytes IS NOT NULL
                    AND queued_at IS NOT NULL
                    AND ready_at IS NOT NULL
                )
            )
        )
    )
)
"""
_CLAIM_PUBLICATION_AUDIT_TABLE_STATEMENT = f"""
CREATE TABLE IF NOT EXISTS lab_claim_publication_audit (
    audit_ref TEXT PRIMARY KEY CHECK (typeof(audit_ref) = 'text'),
    attempt_id TEXT NOT NULL CHECK (typeof(attempt_id) = 'text'),
    action TEXT NOT NULL CHECK (action IN ('created','transitioned','replayed','conflict')),
    prior_status TEXT CHECK (
        prior_status IS NULL
        OR prior_status IN ('HELD_SOURCE','SOURCE_QUEUED','READY_TO_PUBLISH','PUBLISHED','ABORTED')
    ),
    new_status TEXT NOT NULL CHECK (
        new_status IN ('HELD_SOURCE','SOURCE_QUEUED','READY_TO_PUBLISH','PUBLISHED','ABORTED')
    ),
    reason_code TEXT NOT NULL CHECK (typeof(reason_code) = 'text'),
    record_commitment TEXT NOT NULL CHECK (
        typeof(record_commitment) = 'text' AND length(record_commitment) = 64
    ),
    occurred_at TEXT NOT NULL CHECK (
        typeof(occurred_at) = 'text'
        AND length(occurred_at) = 32
        AND occurred_at GLOB {_CANONICAL_UTC_TIMESTAMP_GLOB}
    ),
    audit_hash TEXT NOT NULL CHECK (typeof(audit_hash) = 'text' AND length(audit_hash) = 64),
    FOREIGN KEY (attempt_id) REFERENCES lab_claim_publication(attempt_id) ON DELETE RESTRICT
)
"""
_CLAIM_PUBLICATION_HELD_DEADLINE_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_claim_publication_held_deadline
ON lab_claim_publication(status, source_wait_deadline, attempt_id)
WHERE status IN ('HELD_SOURCE','SOURCE_QUEUED')
"""
_CLAIM_PUBLICATION_RECONCILE_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_claim_publication_reconcile_deadline
ON lab_claim_publication(status, publication_deadline, attempt_id)
WHERE status = 'READY_TO_PUBLISH'
"""
_CLAIM_PUBLICATION_AUDIT_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_claim_publication_audit_attempt
ON lab_claim_publication_audit(attempt_id, occurred_at, audit_ref)
"""
_CLAIM_PUBLICATION_INSERT_AUTH_TRIGGER = f"""
CREATE TRIGGER IF NOT EXISTS trg_lab_claim_publication_insert_auth
BEFORE INSERT ON lab_claim_publication
WHEN {_CLAIM_PUBLICATION_AUTH_FUNCTION}(
    NEW.attempt_id, NEW.status, NEW.version, NEW.record_commitment
) <> 1
BEGIN
    SELECT RAISE(ABORT, 'publication mutation is not authorized');
END
"""
_CLAIM_PUBLICATION_UPDATE_AUTH_TRIGGER = f"""
CREATE TRIGGER IF NOT EXISTS trg_lab_claim_publication_update_auth
BEFORE UPDATE ON lab_claim_publication
WHEN {_CLAIM_PUBLICATION_AUTH_FUNCTION}(
    NEW.attempt_id, NEW.status, NEW.version, NEW.record_commitment
) <> 1
BEGIN
    SELECT RAISE(ABORT, 'publication mutation is not authorized');
END
"""
_CLAIM_PUBLICATION_AUDIT_INSERT_AUTH_TRIGGER = f"""
CREATE TRIGGER IF NOT EXISTS trg_lab_claim_publication_audit_insert_auth
BEFORE INSERT ON lab_claim_publication_audit
WHEN {_CLAIM_PUBLICATION_AUDIT_AUTH_FUNCTION}(
    NEW.audit_ref, NEW.attempt_id, NEW.action, NEW.audit_hash
) <> 1
BEGIN
    SELECT RAISE(ABORT, 'publication audit mutation is not authorized');
END
"""
_CLAIM_PUBLICATION_UPDATE_GUARD_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_claim_publication_update_guard
BEFORE UPDATE ON lab_claim_publication
WHEN OLD.status IN ('PUBLISHED','ABORTED')
 OR NEW.attempt_id IS NOT OLD.attempt_id
 OR NEW.job_id IS NOT OLD.job_id
 OR NEW.shard_id IS NOT OLD.shard_id
 OR NEW.claim_token IS NOT OLD.claim_token
 OR NEW.claim_generation IS NOT OLD.claim_generation
 OR NEW.scheduler_fencing_token IS NOT OLD.scheduler_fencing_token
 OR NEW.worker_id IS NOT OLD.worker_id
 OR NEW.spec_hash IS NOT OLD.spec_hash
 OR NEW.plan_hash IS NOT OLD.plan_hash
 OR NEW.payload_hash IS NOT OLD.payload_hash
 OR NEW.claim_preimage_bytes IS NOT OLD.claim_preimage_bytes
 OR NEW.claim_preimage_hash IS NOT OLD.claim_preimage_hash
 OR NEW.claim_protocol IS NOT OLD.claim_protocol
 OR NEW.claim_protocol_version IS NOT OLD.claim_protocol_version
 OR NEW.source_wait_deadline IS NOT OLD.source_wait_deadline
 OR NEW.publication_deadline IS NOT OLD.publication_deadline
 OR NEW.source_stage_authority_bytes IS NOT OLD.source_stage_authority_bytes
 OR NEW.source_stage_authority_hash IS NOT OLD.source_stage_authority_hash
 OR NEW.created_at IS NOT OLD.created_at
 OR NOT (
    (OLD.status = 'HELD_SOURCE' AND NEW.status IN ('SOURCE_QUEUED','ABORTED'))
    OR (OLD.status = 'SOURCE_QUEUED' AND NEW.status IN ('READY_TO_PUBLISH','ABORTED'))
    OR (OLD.status = 'READY_TO_PUBLISH' AND NEW.status IN ('PUBLISHED','ABORTED'))
 )
 OR NEW.version <> OLD.version + 1
 OR (
    OLD.status <> 'HELD_SOURCE'
    AND (
        NEW.source_stage_binding_bytes IS NOT OLD.source_stage_binding_bytes
        OR NEW.source_stage_binding_hash IS NOT OLD.source_stage_binding_hash
        OR NEW.source_intent_bytes IS NOT OLD.source_intent_bytes
        OR NEW.source_intent_hash IS NOT OLD.source_intent_hash
        OR NEW.source_operation_id IS NOT OLD.source_operation_id
        OR NEW.source_operation_hash IS NOT OLD.source_operation_hash
        OR NEW.queued_source_stage_record_hash IS NOT OLD.queued_source_stage_record_hash
        OR NEW.queued_at IS NOT OLD.queued_at
    )
 )
 OR (
    OLD.status <> 'SOURCE_QUEUED'
    AND (
        NEW.ready_source_stage_record_bytes IS NOT OLD.ready_source_stage_record_bytes
        OR NEW.ready_source_stage_record_hash IS NOT OLD.ready_source_stage_record_hash
        OR NEW.verified_source_outcome_hash IS NOT OLD.verified_source_outcome_hash
        OR NEW.verified_evidence_chain_hash IS NOT OLD.verified_evidence_chain_hash
        OR NEW.source_use_plan_bytes IS NOT OLD.source_use_plan_bytes
        OR NEW.source_use_plan_hash IS NOT OLD.source_use_plan_hash
        OR NEW.final_claim_bytes IS NOT OLD.final_claim_bytes
        OR NEW.final_claim_hash IS NOT OLD.final_claim_hash
        OR NEW.current_claim_receipt_bytes IS NOT OLD.current_claim_receipt_bytes
        OR NEW.current_claim_receipt_hash IS NOT OLD.current_claim_receipt_hash
        OR NEW.ready_at IS NOT OLD.ready_at
    )
 )
 OR (
    OLD.status <> 'READY_TO_PUBLISH'
    AND (
        NEW.spool_receipt_bytes IS NOT OLD.spool_receipt_bytes
        OR NEW.spool_receipt_hash IS NOT OLD.spool_receipt_hash
        OR NEW.published_at IS NOT OLD.published_at
    )
 )
 OR (
    NEW.status <> 'ABORTED'
    AND (
        NEW.aborted_at IS NOT OLD.aborted_at
        OR NEW.terminal_reason IS NOT OLD.terminal_reason
    )
 )
BEGIN
    SELECT RAISE(ABORT, 'terminal publication is immutable');
END
"""
_CLAIM_PUBLICATION_NO_DELETE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_claim_publication_no_delete
BEFORE DELETE ON lab_claim_publication
BEGIN
    SELECT RAISE(ABORT, 'claim publication is immutable');
END
"""
_CLAIM_PUBLICATION_AUDIT_NO_UPDATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_claim_publication_audit_no_update
BEFORE UPDATE ON lab_claim_publication_audit
BEGIN
    SELECT RAISE(ABORT, 'claim publication audit is immutable');
END
"""
_CLAIM_PUBLICATION_AUDIT_NO_DELETE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_claim_publication_audit_no_delete
BEFORE DELETE ON lab_claim_publication_audit
BEGIN
    SELECT RAISE(ABORT, 'claim publication audit is immutable');
END
"""

_V8_LEDGER_EPOCH_TRIGGER_SQL = {
    f"trg_lab_epoch_{table}_{action.lower()}": _ledger_epoch_trigger_statement(table, action)
    for table in ("lab_claim_publication", "lab_claim_publication_audit")
    for action in ("INSERT", "UPDATE", "DELETE")
}
_V8_PUBLICATION_TRIGGER_SQL = {
    "trg_lab_claim_publication_insert_auth": _CLAIM_PUBLICATION_INSERT_AUTH_TRIGGER,
    "trg_lab_claim_publication_update_auth": _CLAIM_PUBLICATION_UPDATE_AUTH_TRIGGER,
    "trg_lab_claim_publication_audit_insert_auth": (_CLAIM_PUBLICATION_AUDIT_INSERT_AUTH_TRIGGER),
    "trg_lab_claim_publication_update_guard": _CLAIM_PUBLICATION_UPDATE_GUARD_TRIGGER,
    "trg_lab_claim_publication_no_delete": _CLAIM_PUBLICATION_NO_DELETE_TRIGGER,
    "trg_lab_claim_publication_audit_no_update": _CLAIM_PUBLICATION_AUDIT_NO_UPDATE_TRIGGER,
    "trg_lab_claim_publication_audit_no_delete": _CLAIM_PUBLICATION_AUDIT_NO_DELETE_TRIGGER,
    **_V8_LEDGER_EPOCH_TRIGGER_SQL,
}
_V8_SCHEMA_STATEMENTS = _V7_SCHEMA_STATEMENTS + (
    _CLAIM_PUBLICATION_TABLE_STATEMENT,
    _CLAIM_PUBLICATION_AUDIT_TABLE_STATEMENT,
    _CLAIM_PUBLICATION_HELD_DEADLINE_INDEX_STATEMENT,
    _CLAIM_PUBLICATION_RECONCILE_INDEX_STATEMENT,
    _CLAIM_PUBLICATION_AUDIT_INDEX_STATEMENT,
    *_V8_PUBLICATION_TRIGGER_SQL.values(),
)
_ACTIVE_CLAIMS_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_shard_active_claims
ON lab_shard(
    status, scheduler_fencing_token, lease_expires_at,
    job_id, shard_index, shard_id
)
WHERE status = 'running'
"""
_STALE_RECOVERY_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_shard_stale_recovery
ON lab_shard(status, lease_expires_at, job_id, shard_index, shard_id)
WHERE status = 'running'
"""
_V9_SCHEMA_STATEMENTS = _V8_SCHEMA_STATEMENTS + (
    _ACTIVE_CLAIMS_INDEX_STATEMENT,
    _STALE_RECOVERY_INDEX_STATEMENT,
)
_V10_STALE_RECOVERY_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_shard_stale_recovery
ON lab_shard(
    status, payload_protocol_version, job_id, shard_index, shard_id, lease_expires_at
)
WHERE status = 'running' AND payload_protocol_version = 1
"""
_V10_V2_RECONCILIATION_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_shard_v2_reconciliation
ON lab_shard(job_id, shard_id, lease_expires_at)
WHERE status = 'running' AND payload_protocol_version = 2
"""
_V10_PAYLOAD_PROTOCOL_INSERT_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_shard_payload_protocol_insert
BEFORE INSERT ON lab_shard
WHEN (
    json_valid(NEW.payload_json) <> 1
    OR NEW.payload_protocol_version <> CASE
        WHEN json_type(NEW.payload_json, '$.schema_version') IS NULL THEN 1
        WHEN json_type(NEW.payload_json, '$.schema_version') = 'integer'
             AND json_extract(NEW.payload_json, '$.schema_version') IN (1, 2)
            THEN json_extract(NEW.payload_json, '$.schema_version')
        ELSE 0
    END
)
BEGIN
    SELECT RAISE(ABORT, 'shard payload protocol conflicts with payload_json');
END
"""
_V10_PAYLOAD_PROTOCOL_UPDATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_lab_shard_payload_protocol_update
BEFORE UPDATE OF payload_json, payload_protocol_version ON lab_shard
WHEN (
    json_valid(NEW.payload_json) <> 1
    OR NEW.payload_protocol_version <> CASE
        WHEN json_type(NEW.payload_json, '$.schema_version') IS NULL THEN 1
        WHEN json_type(NEW.payload_json, '$.schema_version') = 'integer'
             AND json_extract(NEW.payload_json, '$.schema_version') IN (1, 2)
            THEN json_extract(NEW.payload_json, '$.schema_version')
        ELSE 0
    END
)
BEGIN
    SELECT RAISE(ABORT, 'shard payload protocol conflicts with payload_json');
END
"""
_V10_PAYLOAD_PROTOCOL_TRIGGER_SQL = {
    "trg_lab_shard_payload_protocol_insert": _V10_PAYLOAD_PROTOCOL_INSERT_TRIGGER,
    "trg_lab_shard_payload_protocol_update": _V10_PAYLOAD_PROTOCOL_UPDATE_TRIGGER,
}
_V10_SCHEMA_STATEMENTS = _V9_SCHEMA_STATEMENTS + (
    _V10_STALE_RECOVERY_INDEX_STATEMENT,
    _V10_V2_RECONCILIATION_INDEX_STATEMENT,
    *_V10_PAYLOAD_PROTOCOL_TRIGGER_SQL.values(),
)
_RECOVERY_CURSOR_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_recovery_cursor (
    cursor_key TEXT PRIMARY KEY CHECK (
        typeof(cursor_key) = 'text' AND cursor_key = 'idle_control'
    ),
    cursor_created_at TEXT NOT NULL CHECK (typeof(cursor_created_at) = 'text'),
    cursor_job_id TEXT NOT NULL CHECK (
        typeof(cursor_job_id) = 'text' AND length(cursor_job_id) = 36
    ),
    updated_at TEXT NOT NULL CHECK (typeof(updated_at) = 'text')
)
"""
_V11_EXHAUSTED_QUEUED_V1_RECOVERY_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_shard_exhausted_queued_v1_recovery
ON lab_shard(status, payload_protocol_version, job_id, shard_index, shard_id)
WHERE payload_protocol_version = 1
  AND status = 'queued'
  AND attempt_count >= max_attempts
"""
_V11_EXHAUSTED_CHECKPOINTED_V1_RECOVERY_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_shard_exhausted_checkpointed_v1_recovery
ON lab_shard(status, payload_protocol_version, job_id, shard_index, shard_id)
WHERE payload_protocol_version = 1
  AND status = 'checkpointed'
  AND attempt_count >= max_attempts
"""
_V11_IDLE_CONTROL_RECOVERY_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_job_idle_control_recovery
ON lab_job(status, created_at, job_id)
WHERE status = 'running'
"""
_V11_SCHEMA_STATEMENTS = _V10_SCHEMA_STATEMENTS + (
    _RECOVERY_CURSOR_TABLE_STATEMENT,
    _V11_EXHAUSTED_QUEUED_V1_RECOVERY_INDEX_STATEMENT,
    _V11_EXHAUSTED_CHECKPOINTED_V1_RECOVERY_INDEX_STATEMENT,
    _V11_IDLE_CONTROL_RECOVERY_INDEX_STATEMENT,
)
_V12_IDLE_CONTROL_RECOVERY_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_job_idle_control_recovery
ON lab_job(status, created_at, job_id)
WHERE status = 'running'
  AND control_intent IN ('pause_requested', 'cancel_requested')
"""
_V12_IDLE_CONTROL_SHARD_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_shard_idle_control_eligibility
ON lab_shard(job_id, status)
"""
_V12_SCHEMA_STATEMENTS = _V11_SCHEMA_STATEMENTS[:-1] + (
    _V12_IDLE_CONTROL_RECOVERY_INDEX_STATEMENT,
    _V12_IDLE_CONTROL_SHARD_INDEX_STATEMENT,
)
_V13_PRECLAIM_CANDIDATE_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_shard_preclaim_candidate
ON lab_shard(job_id, shard_index, shard_id, payload_protocol_version)
WHERE status = 'queued'
"""
_V13_SCHEMA_STATEMENTS = _V12_SCHEMA_STATEMENTS + (_V13_PRECLAIM_CANDIDATE_INDEX_STATEMENT,)
_V14_PRECLAIM_FAIR_CURSOR_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_preclaim_fair_cursor (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    claim_cursor_created_at TEXT,
    claim_cursor_job_id TEXT,
    claim_cursor_shard_index INTEGER,
    claim_cursor_shard_id TEXT,
    updated_at TEXT NOT NULL CHECK (typeof(updated_at) = 'text'),
    CHECK (
        (claim_cursor_created_at IS NULL AND claim_cursor_job_id IS NULL
         AND claim_cursor_shard_index IS NULL AND claim_cursor_shard_id IS NULL)
        OR
        (typeof(claim_cursor_created_at) = 'text' AND typeof(claim_cursor_job_id) = 'text'
         AND typeof(claim_cursor_shard_index) = 'integer'
         AND claim_cursor_shard_index >= 0 AND typeof(claim_cursor_shard_id) = 'text')
    )
)
"""
_V15_FINALIZER_LEASE_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_claim_publication_finalizer_lease (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    canonical_job_store_path TEXT NOT NULL CHECK (typeof(canonical_job_store_path) = 'text'),
    database_device INTEGER NOT NULL CHECK (typeof(database_device) = 'integer'),
    database_inode INTEGER NOT NULL CHECK (typeof(database_inode) = 'integer'),
    store_id TEXT NOT NULL CHECK (typeof(store_id) = 'text' AND length(store_id) = 64),
    schema_version INTEGER NOT NULL CHECK (
        typeof(schema_version) = 'integer' AND schema_version >= 1
    ),
    implementation_digest TEXT NOT NULL CHECK (
        typeof(implementation_digest) = 'text' AND length(implementation_digest) = 64
    ),
    owner_id TEXT NOT NULL CHECK (typeof(owner_id) = 'text' AND length(owner_id) BETWEEN 1 AND 200),
    lease_id INTEGER NOT NULL CHECK (typeof(lease_id) = 'integer' AND lease_id >= 1),
    fencing_token INTEGER NOT NULL CHECK (typeof(fencing_token) = 'integer' AND fencing_token >= 1),
    root_descriptor TEXT NOT NULL CHECK (
        typeof(root_descriptor) = 'text' AND length(root_descriptor) BETWEEN 1 AND 200
    ),
    token_commitment TEXT NOT NULL CHECK (
        typeof(token_commitment) = 'text' AND length(token_commitment) = 64
    ),
    lease_commitment TEXT NOT NULL CHECK (
        typeof(lease_commitment) = 'text' AND length(lease_commitment) = 64
    ),
    acquired_at TEXT NOT NULL CHECK (typeof(acquired_at) = 'text'),
    heartbeat_at TEXT NOT NULL CHECK (typeof(heartbeat_at) = 'text'),
    expires_at TEXT NOT NULL CHECK (typeof(expires_at) = 'text'),
    released_at TEXT,
    CHECK (expires_at > acquired_at)
)
"""
_V15_FINALIZER_ROOT_ANCHOR_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_claim_publication_finalizer_root_anchor (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    root_descriptor TEXT NOT NULL CHECK (
        typeof(root_descriptor) = 'text' AND length(root_descriptor) BETWEEN 1 AND 200
    ),
    root_key_digest TEXT NOT NULL CHECK (
        typeof(root_key_digest) = 'text' AND length(root_key_digest) = 64
    )
) STRICT
"""
_V15_FINALIZER_OBSERVATION_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_claim_publication_finalizer_observation (
    observation_ref TEXT PRIMARY KEY CHECK (typeof(observation_ref) = 'text'),
    attempt_id TEXT NOT NULL CHECK (typeof(attempt_id) = 'text'),
    authority_fencing_token INTEGER NOT NULL CHECK (
        typeof(authority_fencing_token) = 'integer' AND authority_fencing_token >= 1
    ),
    event_type TEXT NOT NULL CHECK (event_type IN ('ready','published','replayed','blocked')),
    reason_code TEXT NOT NULL CHECK (
        typeof(reason_code) = 'text' AND length(reason_code) BETWEEN 1 AND 64
    ),
    record_commitment TEXT CHECK (
        record_commitment IS NULL
        OR (typeof(record_commitment) = 'text' AND length(record_commitment) = 64)
    ),
    observed_at TEXT NOT NULL CHECK (typeof(observed_at) = 'text'),
    UNIQUE(attempt_id, authority_fencing_token, event_type, reason_code, record_commitment),
    FOREIGN KEY (attempt_id) REFERENCES lab_claim_publication(attempt_id) ON DELETE RESTRICT
)
"""
_V15_FINALIZER_OBSERVATION_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_claim_publication_finalizer_observation_attempt
ON lab_claim_publication_finalizer_observation(attempt_id, observed_at, observation_ref)
"""
_V16_FINALIZER_ATTESTATION_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_claim_publication_finalizer_attestation (
    attempt_id TEXT NOT NULL CHECK (typeof(attempt_id) = 'text'),
    publication_status TEXT NOT NULL CHECK (
        publication_status IN ('READY_TO_PUBLISH', 'PUBLISHED')
    ),
    certificate_bytes BLOB NOT NULL CHECK (typeof(certificate_bytes) = 'blob'),
    certificate_hash TEXT NOT NULL CHECK (
        typeof(certificate_hash) = 'text' AND length(certificate_hash) = 64
    ),
    attestation_bytes BLOB NOT NULL CHECK (typeof(attestation_bytes) = 'blob'),
    attestation_hash TEXT NOT NULL CHECK (
        typeof(attestation_hash) = 'text' AND length(attestation_hash) = 64
    ),
    created_at TEXT NOT NULL CHECK (typeof(created_at) = 'text'),
    PRIMARY KEY (attempt_id, publication_status),
    FOREIGN KEY (attempt_id) REFERENCES lab_claim_publication(attempt_id) ON DELETE RESTRICT
) STRICT
"""
_V16_FINALIZER_TRUST_CACHE_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_claim_publication_finalizer_trust_cache (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    certificate_bytes BLOB NOT NULL CHECK (typeof(certificate_bytes) = 'blob'),
    certificate_hash TEXT NOT NULL CHECK (
        typeof(certificate_hash) = 'text' AND length(certificate_hash) = 64
    ),
    cached_at TEXT NOT NULL CHECK (typeof(cached_at) = 'text')
) STRICT
"""
_V16_FINALIZER_OBSERVATION_DEGRADATION_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS lab_claim_publication_finalizer_observation_degradation (
    degradation_ref TEXT PRIMARY KEY CHECK (typeof(degradation_ref) = 'text'),
    attempt_id TEXT NOT NULL CHECK (typeof(attempt_id) = 'text'),
    publication_identity_hash TEXT NOT NULL CHECK (
        typeof(publication_identity_hash) = 'text' AND length(publication_identity_hash) = 64
    ),
    authority_fencing_token INTEGER NOT NULL CHECK (
        typeof(authority_fencing_token) = 'integer' AND authority_fencing_token >= 1
    ),
    event_type TEXT NOT NULL CHECK (event_type IN ('ready','published','replayed','blocked')),
    reason_code TEXT NOT NULL CHECK (
        typeof(reason_code) = 'text' AND length(reason_code) BETWEEN 1 AND 64
    ),
    reason_code_hash TEXT NOT NULL CHECK (
        typeof(reason_code_hash) = 'text' AND length(reason_code_hash) = 64
    ),
    error_class TEXT NOT NULL CHECK (
        typeof(error_class) = 'text' AND length(error_class) BETWEEN 1 AND 128
    ),
    next_retry_at TEXT NOT NULL CHECK (typeof(next_retry_at) = 'text'),
    created_at TEXT NOT NULL CHECK (typeof(created_at) = 'text'),
    drained_at TEXT,
    UNIQUE(
        attempt_id, publication_identity_hash, authority_fencing_token,
        event_type, reason_code_hash, error_class
    ),
    FOREIGN KEY (attempt_id) REFERENCES lab_claim_publication(attempt_id) ON DELETE RESTRICT
) STRICT
"""
_V16_FINALIZER_OBSERVATION_DEGRADATION_INDEX_STATEMENT = """
CREATE INDEX IF NOT EXISTS ix_lab_claim_publication_finalizer_degradation_due
ON lab_claim_publication_finalizer_observation_degradation(
    drained_at, next_retry_at, created_at, degradation_ref
)
"""
_V16_FINALIZER_STRICT_TABLES = (
    (
        "lab_claim_publication_finalizer_root_anchor",
        _V15_FINALIZER_ROOT_ANCHOR_TABLE_STATEMENT,
    ),
    (
        "lab_claim_publication_finalizer_attestation",
        _V16_FINALIZER_ATTESTATION_TABLE_STATEMENT,
    ),
    (
        "lab_claim_publication_finalizer_trust_cache",
        _V16_FINALIZER_TRUST_CACHE_TABLE_STATEMENT,
    ),
    (
        "lab_claim_publication_finalizer_observation_degradation",
        _V16_FINALIZER_OBSERVATION_DEGRADATION_TABLE_STATEMENT,
    ),
)
_SCHEMA_STATEMENTS = _V9_SCHEMA_STATEMENTS
