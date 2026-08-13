"""Singleton control loop for the durable Strategy Lab command inbox."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from collections.abc import Callable
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field

from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
from rquant.contained_subprocess import run_contained
from rquant.lab_artifact_protocol import (
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
    LabFinalizerAuthorityAuthenticationError,
    LabFinalizerAuthorityVerificationKeyProvider,
    verify_finalizer_authority,
)
from rquant.lab_artifacts import LabArtifactError, LabJobArtifactStore, LabVerifiedSealedBinding
from rquant.lab_claim_publication import (
    ClaimPublicationStatus,
    LabClaimPublicationIdentity,
    LabClaimPublicationRecord,
    QueueBinding,
)
from rquant.lab_job_protocol import (
    InvalidCommandEnvelopeError,
    LabCommandEnvelope,
    LabCommandSpool,
    LabSpoolFileIdentity,
    RequestContentConflictError,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    CurrentSchedulerFenceReceipt,
    FormalSubmissionAuthorityError,
    JobStoreSchedulerFenceVerifier,
    LabClaimSelection,
    LabIntegrityDegradedError,
    LabJobReader,
    LabJobStore,
    LabLeaseRecord,
    SchedulerLeaseFencedError,
)
from rquant.lab_logging import _safe_structured_log
from rquant.lab_result_digest import LabResultDigestPolicy
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool, LabShardClaim, LabShardClaimV2
from rquant.lab_source_stage import (
    LabSourceStageAuthorityError,
    LabSourceStageBinding,
    LabSourceStageError,
    LabSourceStageState,
    LabSourceStageStore,
    LabSourceStageWriterLease,
)
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2JobIntentEnvelope,
    canonical_job_model_bytes,
)
from rquant.source_broker_v2_queue import (
    SourceBrokerV2SchedulerQueue,
    SourceBrokerV2SchedulerQueueBackpressureError,
    SourceBrokerV2SchedulerQueueError,
)
from rquant.source_broker_v2_runner import SourceBrokerV2JobRunnerState
from rquant.source_operation_contracts import (
    SourceOperationContractError,
    build_source_broker_v2_scheduler_intent,
)
from rquant.strategy_job_adapters import StrategyJobAdapterRegistry
from rquant.strict_json import (
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
    strict_model_validate_json,
)


class SchedulerTickResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    lease_acquired: bool
    processed: int = Field(ge=0)
    applied: int = Field(ge=0)
    rejected: int = Field(ge=0)
    quarantined: int = Field(ge=0)
    recovered: int = Field(ge=0)
    reports_processed: int = Field(default=0, ge=0)
    reports_accepted: int = Field(default=0, ge=0)
    reports_rejected: int = Field(default=0, ge=0)
    reports_quarantined: int = Field(default=0, ge=0)
    artifact_commits_processed: int = Field(default=0, ge=0)
    artifact_commits_accepted: int = Field(default=0, ge=0)
    artifact_commits_rejected: int = Field(default=0, ge=0)
    artifact_commits_quarantined: int = Field(default=0, ge=0)
    artifact_commit_quarantine_failures: int = Field(default=0, ge=0)
    deadlines_expired: int = Field(default=0, ge=0)
    plans_created: int = Field(default=0, ge=0)
    plans_failed: int = Field(default=0, ge=0)
    claims_published: int = Field(default=0, ge=0)
    claims_replayed: int = Field(default=0, ge=0)
    claim_delivery_failures: int = Field(default=0, ge=0)
    claims_reconciled: int = Field(default=0, ge=0)
    claim_reconcile_failures: int = Field(default=0, ge=0)
    claims_revoked: int = Field(default=0, ge=0)
    claims_retired: int = Field(default=0, ge=0)
    claim_revoke_failures: int = Field(default=0, ge=0)
    source_stage_queued: int = Field(default=0, ge=0)
    source_stage_pending: int = Field(default=0, ge=0)
    source_stage_ready: int = Field(default=0, ge=0)
    source_stage_deferred: int = Field(default=0, ge=0)
    source_stage_failed: int = Field(default=0, ge=0)
    source_stage_reconcile_required: int = Field(default=0, ge=0)
    claims_held: int = Field(default=0, ge=0)
    claims_blocked_by_source_stage: int = Field(default=0, ge=0)
    preclaim_blocked: int = Field(default=0, ge=0)


class _ClaimAuthorityTick(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    claims_published: int = Field(default=0, ge=0)
    claims_replayed: int = Field(default=0, ge=0)
    delivery_failures: int = Field(default=0, ge=0)
    claims_reconciled: int = Field(default=0, ge=0)
    reconcile_failures: int = Field(default=0, ge=0)
    claims_revoked: int = Field(default=0, ge=0)
    claims_retired: int = Field(default=0, ge=0)
    revoke_failures: int = Field(default=0, ge=0)


@dataclass
class _SourceStageTick:
    queued: int = 0
    pending: int = 0
    ready: int = 0
    deferred: int = 0
    failed: int = 0
    reconcile_required: int = 0
    claims_held: int = 0
    claims_blocked: int = 0

    def add(self, other: _SourceStageTick) -> None:
        self.queued += other.queued
        self.pending += other.pending
        self.ready += other.ready
        self.deferred += other.deferred
        self.failed += other.failed
        self.reconcile_required += other.reconcile_required
        self.claims_held += other.claims_held
        self.claims_blocked += other.claims_blocked


def _system_clock() -> datetime:
    return datetime.now(UTC)


def _safe_plan_failure(exc: Exception) -> str:
    message = " ".join((str(exc) or type(exc).__name__).split())
    return f"{type(exc).__name__}: {message[:400]}"


def _safe_error_message(exc: Exception) -> str:
    return " ".join((str(exc) or type(exc).__name__).split())[:400]


class LabJobLifecycleSynchronizer(Protocol):
    def validate_submission(
        self,
        envelope: LabCommandEnvelope,
        *,
        observed_at: datetime,
    ) -> None: ...

    def recover(self, *, observed_at: datetime) -> object: ...

    def synchronize(self, job_id: UUID, *, observed_at: datetime) -> object: ...


class LabIncrementalIntegrityAuditor(Protocol):
    def audit_incremental(self, *, max_chain_entries: int) -> object: ...


class LabFullIntegrityAuditState(BaseModel):
    """Crash-safe, outside-ledger schedule state for whole-history audits."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = Field(default=1, ge=1, le=1)
    last_attempt_at: datetime
    last_completed_at: datetime | None = None
    receipt_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    degraded_reason: str | None = Field(default=None, max_length=512)


class LabFullIntegrityAuditStateStore:
    """A bounded, atomically published due marker independent of Lab SQLite.

    All reads and writes are anchored through a directory descriptor with
    ``O_NOFOLLOW`` and ``fstat`` identity binding — the marker path is never
    re-resolved between check and use.
    """

    _MAX_BYTES = 16_384

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def _open_parent(self) -> int:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.path.parent, flags)
        except OSError as exc:
            raise LabIntegrityDegradedError(
                "full audit due marker directory cannot be opened safely"
            ) from exc
        try:
            observed = os.fstat(descriptor)
            if not observed.st_mode & 0o040000:
                raise LabIntegrityDegradedError("full audit due marker directory is invalid")
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    def load(self) -> LabFullIntegrityAuditState | None:
        try:
            parent_descriptor = self._open_parent()
        except LabIntegrityDegradedError:
            if not self.path.parent.exists():
                return None
            raise
        try:
            try:
                descriptor = os.open(
                    self.path.name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=parent_descriptor,
                )
            except FileNotFoundError:
                return None
            except OSError as exc:
                raise LabIntegrityDegradedError(
                    "full audit due marker cannot be opened safely"
                ) from exc
            try:
                before = os.fstat(descriptor)
                if (
                    not before.st_mode & 0o100000
                    or before.st_nlink != 1
                    or before.st_size > self._MAX_BYTES
                ):
                    raise LabIntegrityDegradedError("full audit due marker identity is invalid")
                payload = os.read(descriptor, self._MAX_BYTES + 1)
                after = os.fstat(descriptor)
                if len(payload) > self._MAX_BYTES or (
                    before.st_dev,
                    before.st_ino,
                    before.st_size,
                    before.st_mtime_ns,
                ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                    raise LabIntegrityDegradedError("full audit due marker changed while reading")
            finally:
                os.close(descriptor)
        finally:
            os.close(parent_descriptor)
        try:
            state = strict_model_validate_json(LabFullIntegrityAuditState, payload)
        except Exception as exc:
            raise LabIntegrityDegradedError("full audit due marker is invalid") from exc
        if canonical_model_json_bytes(state) != payload:
            raise LabIntegrityDegradedError("full audit due marker is not canonical")
        return state

    def save(self, state: LabFullIntegrityAuditState) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        payload = canonical_model_json_bytes(state)
        if len(payload) > self._MAX_BYTES:
            raise LabIntegrityDegradedError("full audit due marker exceeds its size bound")
        temporary_name = f".{self.path.name}.{os.getpid()}.tmp"
        parent_descriptor = self._open_parent()
        descriptor = -1
        try:
            descriptor = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=parent_descriptor,
            )
            os.write(descriptor, payload)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(
                temporary_name,
                self.path.name,
                src_dir_fd=parent_descriptor,
                dst_dir_fd=parent_descriptor,
            )
            os.fsync(parent_descriptor)
        except OSError as exc:
            raise LabIntegrityDegradedError("full audit due marker could not be published") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            with suppress(FileNotFoundError, OSError):
                os.unlink(temporary_name, dir_fd=parent_descriptor)
            os.close(parent_descriptor)


class LabScheduler:
    """Own the writer lease and apply a bounded number of durable commands."""

    def __init__(
        self,
        *,
        store: LabJobStore,
        spool: LabCommandSpool,
        owner_id: str,
        lease_seconds: int,
        heartbeat_seconds: int,
        poll_interval_ms: int,
        max_commands_per_tick: int = 64,
        report_spool: LabReportSpool | None = None,
        claim_spool: LabClaimSpool | None = None,
        claim_worker_ids: tuple[str, ...] = (),
        shard_lease_seconds: int = 300,
        max_reports_per_tick: int = 64,
        adapter_registry: StrategyJobAdapterRegistry | None = None,
        max_plans_per_tick: int = 64,
        max_claims_per_tick: int = 16,
        max_claim_authority_per_tick: int = 128,
        artifact_commit_spool: LabArtifactCommitSpool | None = None,
        artifact_store: LabJobArtifactStore | None = None,
        finalizer_authority_key_provider: (
            LabFinalizerAuthorityVerificationKeyProvider | None
        ) = None,
        max_artifact_commits_per_tick: int = 64,
        result_digest_policy: LabResultDigestPolicy | None = None,
        lifecycle_synchronizer: LabJobLifecycleSynchronizer | None = None,
        runtime_guard: Callable[[], str] | None = None,
        require_authority_manifest: bool = False,
        authority_manifest_loader: Callable[[], object] | None = None,
        integrity_auditor: LabIncrementalIntegrityAuditor | None = None,
        max_integrity_chain_entries: int = 16,
        full_integrity_command: tuple[str, ...] | None = None,
        full_integrity_state_store: LabFullIntegrityAuditStateStore | None = None,
        full_integrity_interval_seconds: int = 3_600,
        full_integrity_budget_seconds: float = 30.0,
        full_integrity_remediation_authorizer: Callable[[], None] | None = None,
        full_integrity_degradation_reporter: Callable[[str], None] | None = None,
        source_stage_store: LabSourceStageStore | None = None,
        source_scheduler_queue: SourceBrokerV2SchedulerQueue | None = None,
        source_manifest_keyring: VerifyOnlyEd25519Keyring | None = None,
        source_authorization_keyring: VerifyOnlyEd25519Keyring | None = None,
        source_wait_timeout_seconds: int | None = None,
        publication_timeout_seconds: int | None = None,
        source_stage_writer_lease_seconds: int | None = None,
        source_stage_owner_id: str | None = None,
        max_source_stage_per_tick: int = 32,
        v2_emit_permit: Callable[[str], object] | None = None,
        clock: Callable[[], datetime] = _system_clock,
    ) -> None:
        if not owner_id.strip():
            raise ValueError("owner_id must not be empty")
        if heartbeat_seconds < 1:
            raise ValueError("heartbeat_seconds must be positive")
        if lease_seconds < 3 * heartbeat_seconds:
            raise ValueError("lease_seconds must be at least 3 * heartbeat_seconds")
        if poll_interval_ms < 1:
            raise ValueError("poll_interval_ms must be positive")
        if max_commands_per_tick < 1:
            raise ValueError("max_commands_per_tick must be positive")
        if max_commands_per_tick > 256:
            raise ValueError("max_commands_per_tick exceeds safety limit 256")
        if shard_lease_seconds < 1:
            raise ValueError("shard_lease_seconds must be positive")
        if max_reports_per_tick < 1:
            raise ValueError("max_reports_per_tick must be positive")
        if max_reports_per_tick > 256:
            raise ValueError("max_reports_per_tick exceeds safety limit 256")
        if max_plans_per_tick < 1:
            raise ValueError("max_plans_per_tick must be positive")
        if max_plans_per_tick > 256:
            raise ValueError("max_plans_per_tick exceeds safety limit 256")
        if max_claims_per_tick < 1:
            raise ValueError("max_claims_per_tick must be positive")
        if max_claims_per_tick > 128:
            raise ValueError("max_claims_per_tick exceeds safety limit 128")
        if max_claim_authority_per_tick < 1:
            raise ValueError("max_claim_authority_per_tick must be positive")
        if max_claim_authority_per_tick > 512:
            raise ValueError("max_claim_authority_per_tick exceeds safety limit 512")
        if max_artifact_commits_per_tick < 1:
            raise ValueError("max_artifact_commits_per_tick must be positive")
        if max_artifact_commits_per_tick > 256:
            raise ValueError("max_artifact_commits_per_tick exceeds safety limit 256")
        if not 1 <= max_source_stage_per_tick <= 128:
            raise ValueError("max_source_stage_per_tick must be between 1 and 128")
        if not 1 <= max_integrity_chain_entries <= 128:
            raise ValueError("max_integrity_chain_entries must be between 1 and 128")
        if full_integrity_interval_seconds < 60:
            raise ValueError("full_integrity_interval_seconds must be at least 60")
        if not 0.01 <= full_integrity_budget_seconds <= 300:
            raise ValueError("full_integrity_budget_seconds must be between 0.01 and 300")
        if (artifact_commit_spool is None) != (artifact_store is None):
            raise ValueError("artifact commit spool and artifact store must be configured together")
        if artifact_commit_spool is not None and finalizer_authority_key_provider is None:
            raise ValueError("finalizer authority key provider is required for artifact commits")
        normalized_workers = tuple(worker.strip() for worker in claim_worker_ids)
        if any(not worker for worker in normalized_workers):
            raise ValueError("claim_worker_ids must not contain empty values")
        if len(set(normalized_workers)) != len(normalized_workers):
            raise ValueError("claim_worker_ids must be unique")
        source_dependencies = (
            source_stage_store,
            source_scheduler_queue,
            source_manifest_keyring,
            source_authorization_keyring,
        )
        source_stage_enabled = all(value is not None for value in source_dependencies)
        if any(value is not None for value in source_dependencies) and not source_stage_enabled:
            raise ValueError("source-stage dependencies must be configured together")
        if source_stage_enabled:
            if type(source_stage_store) is not LabSourceStageStore:
                raise TypeError("source_stage_store must be an exact LabSourceStageStore")
            if type(source_scheduler_queue) is not SourceBrokerV2SchedulerQueue:
                raise TypeError("source_scheduler_queue must be an exact scheduler queue")
            if not isinstance(source_manifest_keyring, VerifyOnlyEd25519Keyring):
                raise TypeError("source_manifest_keyring must be verify-only")
            if not isinstance(source_authorization_keyring, VerifyOnlyEd25519Keyring):
                raise TypeError("source_authorization_keyring must be verify-only")
            if source_wait_timeout_seconds is None or publication_timeout_seconds is None:
                raise ValueError("source-stage deadlines must be configured together")
            if source_stage_owner_id is None or not source_stage_owner_id.strip():
                raise ValueError("source_stage_owner_id is required for source-stage scheduling")
            if source_wait_timeout_seconds < 1 or publication_timeout_seconds < 1:
                raise ValueError("source-stage deadlines must be positive")
            if source_wait_timeout_seconds > publication_timeout_seconds:
                raise ValueError("source_wait_timeout_seconds must not exceed publication timeout")
            if publication_timeout_seconds > shard_lease_seconds:
                raise ValueError("publication timeout must not exceed shard lease")
            writer_lease_seconds = source_stage_writer_lease_seconds or shard_lease_seconds
            if not 1 <= writer_lease_seconds <= 3_600:
                raise ValueError("source-stage writer lease must be between 1 and 3600 seconds")
            if writer_lease_seconds < publication_timeout_seconds:
                raise ValueError("source-stage writer lease must cover publication timeout")
            if (
                source_scheduler_queue.db_path.resolve()
                != source_stage_store.queue_store_path.resolve()
            ):
                raise ValueError("source scheduler queue does not match source-stage authority")
            if claim_spool is not None or normalized_workers:
                raise ValueError(
                    "V2 source-stage scheduler must not receive claim spool or worker identities"
                )
        elif any(
            value is not None
            for value in (
                source_wait_timeout_seconds,
                publication_timeout_seconds,
                source_stage_writer_lease_seconds,
                source_stage_owner_id,
            )
        ):
            raise ValueError("source-stage timing requires source-stage dependencies")
        self.store = store
        self.spool = spool
        self.owner_id = owner_id.strip()
        self.lease_seconds = lease_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.poll_interval_ms = poll_interval_ms
        self.max_commands_per_tick = max_commands_per_tick
        self.report_spool = report_spool
        self.claim_spool = claim_spool
        self.claim_worker_ids = normalized_workers
        self.shard_lease_seconds = shard_lease_seconds
        self.max_reports_per_tick = max_reports_per_tick
        self.adapter_registry = adapter_registry
        self.max_plans_per_tick = max_plans_per_tick
        self.max_claims_per_tick = max_claims_per_tick
        self.max_claim_authority_per_tick = max_claim_authority_per_tick
        self.artifact_commit_spool = artifact_commit_spool
        self.artifact_store = artifact_store
        self.finalizer_authority_key_provider = finalizer_authority_key_provider
        self.max_artifact_commits_per_tick = max_artifact_commits_per_tick
        self.result_digest_policy = LabResultDigestPolicy.model_validate(
            result_digest_policy or LabResultDigestPolicy()
        )
        self.integrity_auditor = integrity_auditor or LabJobReader(
            store.path,
            busy_timeout_ms=store.busy_timeout_ms,
            identity_authority=store.identity_authority,
        )
        self.max_integrity_chain_entries = max_integrity_chain_entries
        if full_integrity_command is not None and (
            not full_integrity_command
            or any(not isinstance(value, str) or not value for value in full_integrity_command)
        ):
            raise ValueError("full_integrity_command is invalid")
        self.full_integrity_command = full_integrity_command
        self.full_integrity_state_store = full_integrity_state_store
        self.full_integrity_interval_seconds = full_integrity_interval_seconds
        self.full_integrity_budget_seconds = full_integrity_budget_seconds
        self.full_integrity_remediation_authorizer = full_integrity_remediation_authorizer
        self.full_integrity_degradation_reporter = full_integrity_degradation_reporter
        self.source_stage_store = source_stage_store
        self.source_scheduler_queue = source_scheduler_queue
        self.source_manifest_keyring = source_manifest_keyring
        self.source_authorization_keyring = source_authorization_keyring
        self.source_wait_timeout_seconds = source_wait_timeout_seconds
        self.publication_timeout_seconds = publication_timeout_seconds
        self.source_stage_writer_lease_seconds = (
            (source_stage_writer_lease_seconds or shard_lease_seconds)
            if source_stage_enabled
            else None
        )
        self.source_stage_owner_id = source_stage_owner_id.strip() if source_stage_enabled else None
        self.max_source_stage_per_tick = max_source_stage_per_tick
        self._v2_emit_permit_provider = v2_emit_permit
        authority_path = self.store.path.parent / "job-center-authority.json"
        final_artifact_root = (
            artifact_store.root
            if artifact_store is not None
            else self.store.path.parent / "final-artifacts"
        )
        self._authority_guard: Callable[[], object] | None = None
        if (
            require_authority_manifest
            and authority_manifest_loader is None
            and not authority_path.exists()
        ):
            from rquant.lab_daemon import LabDaemonConfigurationError

            raise LabDaemonConfigurationError(
                "Job Center authority manifest is required for the production scheduler"
            )
        if authority_manifest_loader is not None or authority_path.exists():
            from rquant.lab_daemon import (
                build_experiment_lifecycle_coordinator,
                load_lab_job_center_authority_manifest,
            )

            if runtime_guard is None:
                raise RuntimeError(
                    "Job Center authority auto-composition requires verified runtime SHA"
                )

            if authority_manifest_loader is None:

                def load_current_authority() -> object:
                    return load_lab_job_center_authority_manifest(
                        authority_path,
                        expected_code_sha=runtime_guard(),
                        expected_research_root=self.store.path.parent,
                        expected_lab_jobs_path=self.store.path,
                        expected_command_spool_path=self.spool.root,
                        expected_final_artifact_root=final_artifact_root,
                    )

            else:
                load_current_authority = authority_manifest_loader

            authority = load_current_authority()
            self._authority_guard = load_current_authority
            if lifecycle_synchronizer is None:
                lifecycle_synchronizer = build_experiment_lifecycle_coordinator(
                    authority,
                    reader=LabJobReader(self.store.path),
                    spool=self.spool,
                    clock=clock,
                )
        self.lifecycle_synchronizer = lifecycle_synchronizer
        self.runtime_guard = runtime_guard
        self.clock = clock
        self.lease: LabLeaseRecord | None = None
        self._claim_cursor = 0
        self._claim_cursor_fence: int | None = None
        self._source_stage_lease: LabSourceStageWriterLease | None = None
        self._source_stage_fence_verifier = (
            JobStoreSchedulerFenceVerifier(store) if source_stage_enabled else None
        )
        self._lifecycle_recovered = False
        self._stop = Event()

    def _verify_runtime(self) -> None:
        if self.runtime_guard is not None:
            self.runtime_guard()
        if self._authority_guard is not None:
            self._authority_guard()

    def _synchronize_lifecycle(self, job_id: UUID, *, observed_at: datetime) -> None:
        if self.lifecycle_synchronizer is not None:
            self.lifecycle_synchronizer.synchronize(job_id, observed_at=observed_at)

    def _audit_integrity(self, *, phase: str) -> None:
        try:
            self.integrity_auditor.audit_incremental(
                max_chain_entries=self.max_integrity_chain_entries
            )
        except Exception as exc:
            message = _safe_error_message(exc)
            _safe_structured_log(
                "error",
                "lab_integrity_audit_degraded",
                message=message,
                component="lab_scheduler",
                owner_id=self.owner_id,
                phase=phase,
                error_type=type(exc).__name__,
            )
            raise LabIntegrityDegradedError(
                f"{phase}: incremental ledger audit degraded: {message}"
            ) from exc

    def _audit_full_integrity_if_due(self, *, phase: str) -> None:
        """Run the expensive audit on a persistent cadence, outside the hot path.

        The call is time-bounded.  A timeout is a degraded health condition and
        stops this scheduler tick; the due marker is persisted so subsequent
        ticks do not create an unbounded pile of audit threads.
        """

        if self.full_integrity_state_store is None or self.full_integrity_command is None:
            return
        now = self.clock()
        state = self.full_integrity_state_store.load()
        if state is not None and state.degraded_reason is not None:
            raise LabIntegrityDegradedError(
                f"{phase}: full ledger audit remains degraded: {state.degraded_reason}"
            )
        if state is not None and now < state.last_attempt_at + timedelta(
            seconds=self.full_integrity_interval_seconds
        ):
            return
        receipt_hash, reason = self._run_full_integrity_audit_subprocess()
        if reason == "full ledger audit exceeded its resource budget":
            reason = self._persist_full_integrity_degradation(now=now, reason=reason)
            raise LabIntegrityDegradedError(f"{phase}: {reason}")
        if receipt_hash is None:
            failure = reason or "full ledger audit returned no receipt"
            failure = self._persist_full_integrity_degradation(now=now, reason=failure)
            raise LabIntegrityDegradedError(f"{phase}: full ledger audit degraded: {failure}")
        self.full_integrity_state_store.save(
            LabFullIntegrityAuditState(
                last_attempt_at=now,
                last_completed_at=now,
                receipt_hash=receipt_hash,
            )
        )

    def _persist_full_integrity_degradation(self, *, now: datetime, reason: str) -> str:
        """Fence degraded audit health outside the runner-owned state file."""

        persisted_reason = reason
        if self.full_integrity_degradation_reporter is not None:
            try:
                self.full_integrity_degradation_reporter(reason)
            except Exception as exc:
                persisted_reason = (
                    f"{reason}; independent degradation authority failed: "
                    f"{_safe_error_message(exc)}"
                )
        assert self.full_integrity_state_store is not None
        self.full_integrity_state_store.save(
            LabFullIntegrityAuditState(
                last_attempt_at=now,
                degraded_reason=persisted_reason,
            )
        )
        return persisted_reason

    def _run_full_integrity_audit_subprocess(self) -> tuple[str | None, str | None]:
        """Execute the sealed audit command with a hard, killable deadline."""

        if self.full_integrity_command is None:
            return None, "full ledger audit is unavailable"
        try:
            result = run_contained(
                list(self.full_integrity_command),
                cwd=Path.cwd(),
                deadline_monotonic=time.monotonic() + self.full_integrity_budget_seconds,
                check=False,
                text=True,
                may_spawn_background_descendants=False,
            )
        except subprocess.TimeoutExpired:
            return None, "full ledger audit exceeded its resource budget"
        except (OSError, subprocess.SubprocessError) as exc:
            return None, f"full ledger audit command failed: {_safe_error_message(exc)}"
        if result.returncode != 0:
            detail = _safe_error_message(RuntimeError(result.stderr or result.stdout or ""))
            return None, f"full ledger audit command failed: {detail}"
        lines = [line for line in (result.stdout or "").splitlines() if line.strip()]
        if len(lines) != 1:
            return None, "full ledger audit command returned an ambiguous receipt"
        try:
            document = json.loads(lines[0])
        except (TypeError, ValueError) as exc:
            return None, f"full ledger audit command receipt is invalid: {_safe_error_message(exc)}"
        receipt_hash = document.get("receipt_hash") if isinstance(document, dict) else None
        if type(receipt_hash) is not str or len(receipt_hash) != 64:
            return None, "full ledger audit command returned an invalid receipt"
        return receipt_hash, None

    def remediate_full_integrity(self) -> None:
        """Clear a persisted audit degradation only after an authorized clean audit."""

        if self.full_integrity_state_store is None or self.full_integrity_command is None:
            raise LabIntegrityDegradedError("full ledger audit remediation is unavailable")
        if self.full_integrity_remediation_authorizer is None:
            raise LabIntegrityDegradedError("full ledger audit remediation is not authorized")
        self.full_integrity_remediation_authorizer()
        state = self.full_integrity_state_store.load()
        if state is None or state.degraded_reason is None:
            raise LabIntegrityDegradedError("full ledger audit is not awaiting remediation")
        now = self.clock()
        receipt_hash, reason = self._run_full_integrity_audit_subprocess()
        if receipt_hash is None:
            failure = reason or "full ledger audit remediation returned no receipt"
            failure = self._persist_full_integrity_degradation(now=now, reason=failure)
            raise LabIntegrityDegradedError(f"full ledger audit remediation failed: {failure}")
        self.full_integrity_state_store.save(
            LabFullIntegrityAuditState(
                last_attempt_at=now,
                last_completed_at=now,
                receipt_hash=receipt_hash,
            )
        )

    def _validate_submission_authority(
        self,
        envelope: LabCommandEnvelope,
        observed_at: datetime,
    ) -> None:
        if self.lifecycle_synchronizer is None:
            raise FormalSubmissionAuthorityError(
                "formal v3 job requires Experiment lifecycle authority"
            )
        self.lifecycle_synchronizer.validate_submission(
            envelope,
            observed_at=observed_at,
        )

    def _expire_deadlines(
        self,
        lease: LabLeaseRecord,
        *,
        observed_at: datetime,
    ) -> int:
        expired = self.store.expire_deadline_jobs(lease=lease, now=observed_at)
        for job_id in expired:
            self._synchronize_lifecycle(job_id, observed_at=observed_at)
        return len(expired)

    @staticmethod
    def _after_artifact_commit_staged(
        _entry: LabArtifactCommitSpoolEntry,
        _binding: LabVerifiedSealedBinding,
    ) -> None:
        """Fault-injection boundary before the bound artifact exit verification."""

    @staticmethod
    def _after_artifact_commit_sqlite_commit(
        _entry: LabArtifactCommitSpoolEntry,
    ) -> None:
        """Fault-injection boundary after SQLite commit and before spool ack."""

    @staticmethod
    def _is_artifact_verification_error(exc: BaseException) -> bool:
        if isinstance(exc, LabArtifactError):
            return True
        if isinstance(exc, BaseExceptionGroup):
            return any(
                LabScheduler._is_artifact_verification_error(item) for item in exc.exceptions
            )
        return False

    def _quarantine_artifact_commit(
        self,
        entry_or_path: LabArtifactCommitSpoolEntry | LabSpoolFileIdentity | Path,
        *,
        reason: str,
    ) -> bool:
        if self.artifact_commit_spool is None:  # pragma: no cover - caller invariant
            raise RuntimeError("artifact commit spool is not configured")
        self._verify_runtime()
        try:
            self.artifact_commit_spool.quarantine(entry_or_path, reason=reason)
        except Exception as exc:
            source = entry_or_path.path if hasattr(entry_or_path, "path") else entry_or_path
            _safe_structured_log(
                "error",
                "artifact_commit_quarantine_failed",
                message=_safe_error_message(exc),
                component="lab_scheduler",
                owner_id=self.owner_id,
                pending_path=str(source),
                quarantine_reason=reason,
                error_type=type(exc).__name__,
            )
            return False
        return True

    def _seed_claim_cursor(self, lease: LabLeaseRecord) -> None:
        if self._claim_cursor_fence == lease.fencing_token:
            return
        self._claim_cursor_fence = lease.fencing_token
        self._claim_cursor = (
            (lease.fencing_token - 1) % len(self.claim_worker_ids) if self.claim_worker_ids else 0
        )

    def _start_tick(self) -> bool:
        if self.lease is None:
            now = self.clock()
            self._verify_runtime()
            lease = self.store.acquire_scheduler_lease(
                owner_id=self.owner_id,
                lease_seconds=self.lease_seconds,
                now=now,
            )
            self.lease = lease
            self._seed_claim_cursor(lease)
            return True
        now = self.clock()
        if now >= self.lease.heartbeat_at + timedelta(seconds=self.heartbeat_seconds):
            self._verify_runtime()
            self.lease = self.store.renew_scheduler_lease(
                self.lease,
                lease_seconds=self.lease_seconds,
                now=now,
            )
        return False

    def _mutation_context(self) -> tuple[LabLeaseRecord, datetime]:
        if self.lease is None:  # pragma: no cover - run_once always starts the tick
            raise RuntimeError("scheduler lease has not been acquired")
        now = self.clock()
        if now >= self.lease.heartbeat_at + timedelta(seconds=self.heartbeat_seconds):
            self._verify_runtime()
            self.lease = self.store.renew_scheduler_lease(
                self.lease,
                lease_seconds=self.lease_seconds,
                now=now,
            )
            now = self.clock()
        return self.lease, now

    def _reconcile_claim_authority(
        self,
        lease: LabLeaseRecord,
        *,
        now: datetime,
        new_claim_tokens: frozenset[UUID],
    ) -> _ClaimAuthorityTick:
        if self._source_stage_enabled or self.claim_spool is None:
            return _ClaimAuthorityTick()
        active = self.store.list_active_claims(
            lease,
            now=now,
            initial_lease_seconds=self.shard_lease_seconds,
        )
        active_by_token = {claim.claim_token: claim for claim in active}
        published = 0
        replayed = 0
        delivery_failures = 0
        hook_claims: list[LabShardClaim] = []
        for active_claim in active:
            try:
                self._verify_runtime()
                self.claim_spool.publish(active_claim)
            except Exception as exc:
                delivery_failures += 1
                _safe_structured_log(
                    "error",
                    "claim_publish_failed",
                    message=_safe_error_message(exc),
                    component="lab_scheduler",
                    owner_id=self.owner_id,
                    job_id=str(active_claim.job_id),
                    shard_id=str(active_claim.shard_id),
                    claim_token=str(active_claim.claim_token),
                    error_type=type(exc).__name__,
                )
            else:
                if active_claim.claim_token in new_claim_tokens:
                    published += 1
                else:
                    replayed += 1
                if self.claim_spool.is_current(active_claim):
                    hook_claims.append(active_claim)
        try:
            hot_batch = self.claim_spool.hot_delivery_batch(
                limit=self.max_claim_authority_per_tick,
            )
        except Exception as exc:
            _safe_structured_log(
                "error",
                "claim_authority_scan_failed",
                message=_safe_error_message(exc),
                component="lab_scheduler",
                owner_id=self.owner_id,
                error_type=type(exc).__name__,
            )
            return _ClaimAuthorityTick(
                claims_published=published,
                claims_replayed=replayed,
                delivery_failures=delivery_failures,
                revoke_failures=1,
            )
        stale = tuple(
            delivery
            for delivery in hot_batch.claims
            if active_by_token.get(delivery.claim_token) != delivery
        )
        try:
            accepted_success_tokens = self.store.accepted_success_claim_tokens_for(
                lease,
                now=now,
                claims=stale,
            )
        except Exception as exc:
            _safe_structured_log(
                "error",
                "claim_success_evidence_failed",
                message=_safe_error_message(exc),
                component="lab_scheduler",
                owner_id=self.owner_id,
                candidate_count=len(stale),
                error_type=type(exc).__name__,
            )
            return _ClaimAuthorityTick(
                claims_published=published,
                claims_replayed=replayed,
                delivery_failures=delivery_failures,
                revoke_failures=1,
            )
        revoked = 0
        retired = 0
        revoke_failures = 0
        for delivery in stale:
            try:
                if delivery.claim_token in accepted_success_tokens:
                    self._verify_runtime()
                    self.claim_spool.retire(
                        delivery,
                        outcome="accepted",
                        reason="scheduler accepted shard success",
                    )
                else:
                    self._verify_runtime()
                    self.claim_spool.revoke(
                        delivery,
                        reason="sqlite claim is no longer active",
                    )
                    self._verify_runtime()
                    self.claim_spool.retire(
                        delivery,
                        outcome="revoked",
                        reason="sqlite claim is no longer active",
                    )
            except Exception as exc:
                revoke_failures += 1
                _safe_structured_log(
                    "error",
                    "claim_retire_failed",
                    message=_safe_error_message(exc),
                    component="lab_scheduler",
                    owner_id=self.owner_id,
                    job_id=str(delivery.job_id),
                    shard_id=str(delivery.shard_id),
                    claim_token=str(delivery.claim_token),
                    error_type=type(exc).__name__,
                )
            else:
                retired += 1
                if delivery.claim_token not in accepted_success_tokens:
                    revoked += 1
        reconciled = 0
        reconcile_failures = 0
        hook_claim_by_token = {claim.claim_token: claim for claim in hook_claims}
        self._verify_runtime()
        for outcome in self.claim_spool.reconcile_claims(tuple(hook_claims)):
            if outcome.status == "reconciled":
                reconciled += 1
            elif outcome.status == "failed":
                reconcile_failures += 1
                hook_claim = hook_claim_by_token[outcome.claim_token]
                _safe_structured_log(
                    "warning",
                    "claim_reconcile_failed",
                    message=outcome.error or "unknown reconciliation failure",
                    component="lab_scheduler",
                    owner_id=self.owner_id,
                    job_id=str(hook_claim.job_id),
                    shard_id=str(hook_claim.shard_id),
                    claim_token=str(hook_claim.claim_token),
                )
        return _ClaimAuthorityTick(
            claims_published=published,
            claims_replayed=replayed,
            delivery_failures=delivery_failures,
            claims_reconciled=reconciled,
            reconcile_failures=reconcile_failures,
            claims_revoked=revoked,
            claims_retired=retired,
            revoke_failures=revoke_failures,
        )

    @property
    def _source_stage_enabled(self) -> bool:
        return self.source_stage_store is not None

    @staticmethod
    def _after_source_stage_queued(_record: LabClaimPublicationRecord) -> None:
        """Fault-injection boundary after durable QUEUED evidence and before queue enqueue."""

    @staticmethod
    def _after_source_queue_enqueued(_record: LabClaimPublicationRecord) -> None:
        """Fault-injection boundary after idempotent queue enqueue and before PENDING."""

    @staticmethod
    def _after_source_stage_pending(_record: LabClaimPublicationRecord) -> None:
        """Fault-injection boundary after PENDING evidence is durable."""

    @staticmethod
    def _after_source_claim_held(_record: LabClaimPublicationRecord) -> None:
        """Fault-injection boundary after HELD_SOURCE and before source-stage writes."""

    @staticmethod
    def _source_stage_binding(identity: LabClaimPublicationIdentity) -> LabSourceStageBinding:
        return LabSourceStageBinding(
            job_id=identity.job_id,
            shard_id=identity.shard_id,
            claim_token=identity.claim_token,
            attempt_id=identity.attempt_id,
            claim_generation=identity.claim_generation,
            scheduler_fencing_token=identity.scheduler_fencing_token,
            worker_id=identity.worker_id,
            spec_hash=identity.spec_hash,
            plan_hash=identity.plan_hash,
        )

    @contextmanager
    def _v2_emit_permit(self, record: LabClaimPublicationRecord):
        """Acquire the rollout fence before any V2 stage/queue side effect."""

        if self._v2_emit_permit_provider is None:
            yield True
            return
        holder = ":".join(
            (
                self.owner_id,
                str(record.identity.attempt_id),
                str(record.identity.claim_token),
                str(record.identity.shard_id),
            )
        )
        with ExitStack() as stack:
            try:
                permit = stack.enter_context(  # type: ignore[arg-type]
                    self._v2_emit_permit_provider(holder)
                )
                if permit is None:
                    raise RuntimeError("rollout emit permit provider returned no receipt")
            except Exception as exc:
                self._source_stage_log(
                    "source_stage_emit_gated",
                    record,
                    reason="finalizer_rollout_emit_permit_denied",
                    level="error",
                    error=exc,
                )
                yield False
                return
            else:
                yield permit

    def _source_stage_writer(
        self,
        *,
        now: datetime,
        scheduler_lease: LabLeaseRecord,
        binding: LabSourceStageBinding | None,
    ) -> LabSourceStageWriterLease | None:
        if not self._source_stage_enabled:
            return None
        assert self.source_stage_store is not None
        assert self.source_stage_owner_id is not None
        if (
            self._source_stage_lease is not None
            and self._source_stage_lease.expires_at > now
            and self._source_stage_lease.fencing_token == scheduler_lease.fencing_token
        ):
            return self._source_stage_lease
        failure: Exception | None = None
        try:
            if binding is None:
                raise ValueError("source-stage writer requires an exact attempt binding")
            receipt = self._source_stage_receipt(
                lease=scheduler_lease,
                binding=binding,
                now=now,
            )
            self._source_stage_lease = self.source_stage_store.acquire_writer_lease(
                owner_id=self.source_stage_owner_id,
                lease_seconds=float(self.source_stage_writer_lease_seconds or 0),
                now=now,
                scheduler_fence_receipt=receipt,
                scheduler_fence_verifier=self._source_stage_fence_verifier,
                binding=binding,
            )
        except Exception as acquire_exc:
            failure = acquire_exc
            if binding is not None:
                try:
                    request_id = uuid5(
                        NAMESPACE_URL,
                        "rquant-source-stage-adoption/v1/"
                        f"{self.source_stage_owner_id}/{scheduler_lease.fencing_token}/{binding.binding_hash}",
                    )
                    self._source_stage_lease = self.source_stage_store.adopt_writer_lease(
                        owner_id=self.source_stage_owner_id,
                        scheduler_fence_receipt=self._source_stage_receipt(
                            lease=scheduler_lease,
                            binding=binding,
                            now=now,
                        ),
                        scheduler_fence_verifier=self._source_stage_fence_verifier,
                        request_id=request_id,
                        binding=binding,
                        reason="scheduler_restart_recovery",
                        lease_seconds=float(self.source_stage_writer_lease_seconds or 0),
                        now=now,
                    )
                    return self._source_stage_lease
                except Exception as adoption_exc:
                    failure = adoption_exc
            _safe_structured_log(
                "warning",
                "source_stage_writer_unavailable",
                message=(
                    _safe_error_message(failure or RuntimeError("writer lease unavailable"))
                    + f" [{type(failure).__name__ if failure is not None else 'RuntimeError'}]"
                ),
                component="lab_scheduler",
                owner_id=self.owner_id,
                reason="source_stage_writer_unavailable",
                error_type=type(failure).__name__ if failure is not None else "RuntimeError",
            )
            return None
        if self._source_stage_lease.fencing_token != scheduler_lease.fencing_token:
            _safe_structured_log(
                "error",
                "source_stage_writer_fence_mismatch",
                message="source-stage writer fence does not match current scheduler authority",
                component="lab_scheduler",
                owner_id=self.owner_id,
                reason="source_stage_writer_fence_mismatch",
                scheduler_fencing_token=scheduler_lease.fencing_token,
                writer_fencing_token=self._source_stage_lease.fencing_token,
            )
            self._source_stage_lease = None
            return None
        return self._source_stage_lease

    def _source_stage_receipt(
        self,
        *,
        lease: LabLeaseRecord,
        binding: LabSourceStageBinding,
        now: datetime,
    ) -> CurrentSchedulerFenceReceipt:
        if self._source_stage_fence_verifier is None:
            raise RuntimeError("source-stage scheduler fence verifier is unavailable")
        return self.store.issue_current_scheduler_fence_receipt(
            lease=lease,
            binding=binding,
            now=now,
        )

    def _source_stage_log(
        self,
        event: str,
        record: LabClaimPublicationRecord,
        *,
        reason: str,
        level: str = "warning",
        error: Exception | None = None,
    ) -> None:
        _safe_structured_log(
            level,
            event,
            message=reason if error is None else _safe_error_message(error),
            component="lab_scheduler",
            owner_id=self.owner_id,
            reason=reason,
            status=record.status.value,
            job_id=str(record.identity.job_id),
            shard_id=str(record.identity.shard_id),
            claim_token=str(record.identity.claim_token),
            claim_generation=record.identity.claim_generation,
            scheduler_fencing_token=record.identity.scheduler_fencing_token,
            error_type=type(error).__name__ if error is not None else None,
        )

    def _abort_source_publication(
        self,
        record: LabClaimPublicationRecord,
        *,
        reason: str,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> bool:
        try:
            self.store.abort_claim_publication(
                record.identity,
                terminal_reason=reason,
                lease=lease,
                now=now,
            )
        except Exception as exc:
            self._source_stage_log(
                "source_stage_abort_failed",
                record,
                reason="source_stage_abort_failed",
                level="error",
                error=exc,
            )
            return False
        return True

    def _reconcile_source_stage(
        self,
        record: LabClaimPublicationRecord,
        *,
        binding: LabSourceStageBinding | None,
        writer: LabSourceStageWriterLease | None,
        now: datetime,
        reason: str,
    ) -> _SourceStageTick:
        result = _SourceStageTick(reconcile_required=1)
        if binding is not None and writer is not None and self.source_stage_store is not None:
            try:
                if self.lease is None:
                    raise SchedulerLeaseFencedError("scheduler lease is unavailable")
                self.source_stage_store.mark_reconcile_required(
                    binding,
                    code=reason,
                    lease=writer,
                    now=now,
                    scheduler_fence_receipt=self._source_stage_receipt(
                        lease=self.lease, binding=binding, now=now
                    ),
                    scheduler_fence_verifier=self._source_stage_fence_verifier,
                )
            except Exception as exc:
                self._source_stage_log(
                    "source_stage_reconcile_mark_failed",
                    record,
                    reason=reason,
                    level="error",
                    error=exc,
                )
        self._source_stage_log("source_stage_reconcile_required", record, reason=reason)
        return result

    def _source_intent_for_held(
        self,
        record: LabClaimPublicationRecord,
        *,
        now: datetime,
    ) -> tuple[LabSourceStageBinding, SourceBrokerV2JobIntentEnvelope]:
        assert self.source_manifest_keyring is not None
        assert self.source_authorization_keyring is not None
        try:
            claim = strict_model_validate_canonical_json(
                LabShardClaimV2,
                record.claim_preimage_bytes.decode("utf-8"),
            )
            if canonical_model_json_bytes(claim) != record.claim_preimage_bytes:
                raise ValueError("claim preimage is not canonical")
            binding = self._source_stage_binding(record.identity)
            if LabClaimPublicationIdentity.from_claim(claim) != record.identity:
                raise ValueError("claim preimage identity conflicts with publication")
            intent = build_source_broker_v2_scheduler_intent(
                claim.strategy_payload,
                claim=claim,
                manifest_keyring=self.source_manifest_keyring,
                authorization_keyring=self.source_authorization_keyring,
                deadline=record.source_wait_deadline,
                now=now,
            )
        except (SourceOperationContractError, ValueError, TypeError, UnicodeDecodeError) as exc:
            raise SourceOperationContractError(
                "source authorization is invalid or expired"
            ) from exc
        return binding, intent

    def _v2_preclaim_precondition(
        self,
        *,
        captured_now: datetime,
    ) -> Callable[[object, LabShardClaimV2, datetime], None] | None:
        if not self._source_stage_enabled:
            return None
        assert self.source_manifest_keyring is not None
        assert self.source_authorization_keyring is not None
        assert self.source_wait_timeout_seconds is not None
        deadline = captured_now + timedelta(seconds=self.source_wait_timeout_seconds)

        def verify(payload: object, claim: LabShardClaimV2, now: datetime) -> None:
            if now != captured_now:
                raise SourceOperationContractError(
                    "preclaim clock conflicts with captured tick time"
                )
            build_source_broker_v2_scheduler_intent(
                payload,
                claim=claim,
                manifest_keyring=self.source_manifest_keyring,
                authorization_keyring=self.source_authorization_keyring,
                deadline=deadline,
                now=captured_now,
            )

        return verify

    def _advance_source_queued(
        self,
        record: LabClaimPublicationRecord,
        *,
        binding: LabSourceStageBinding,
        lease: LabLeaseRecord,
        writer: LabSourceStageWriterLease | None,
        now: datetime,
        permit_held: bool = False,
    ) -> _SourceStageTick:
        assert self.source_stage_store is not None
        assert self.source_scheduler_queue is not None
        if writer is None:
            self._source_stage_log(
                "source_stage_blocked",
                record,
                reason="source_stage_writer_unavailable",
            )
            return _SourceStageTick(deferred=1, claims_blocked=1)
        try:
            stage = self.source_stage_store.get(binding)
        except LabSourceStageAuthorityError as exc:
            self._source_stage_log(
                "source_stage_authority_mismatch",
                record,
                reason="source_stage_authority_mismatch",
                level="error",
                error=exc,
            )
            return _SourceStageTick(reconcile_required=1)
        if stage is None:
            return self._reconcile_source_stage(
                record,
                binding=binding,
                writer=writer,
                now=now,
                reason="source_stage_missing",
            )
        if stage.state is LabSourceStageState.PENDING:
            if stage.writer_lease_expires_at is not None and stage.writer_lease_expires_at <= now:
                return self._reconcile_source_stage(
                    record,
                    binding=binding,
                    writer=writer,
                    now=now,
                    reason="source_stage_writer_expired",
                )
            try:
                published = (
                    self.source_scheduler_queue.get_state(stage.operation_id)
                    is SourceBrokerV2JobRunnerState.PUBLISHED
                )
            except SourceBrokerV2SchedulerQueueError:
                return self._reconcile_source_stage(
                    record,
                    binding=binding,
                    writer=writer,
                    now=now,
                    reason="source_queue_uncertain",
                )
            if published:
                try:
                    ready = self.source_stage_store.bind_published_outcome(
                        binding,
                        lease=writer,
                        now=now,
                        scheduler_fence_receipt=self._source_stage_receipt(
                            lease=lease,
                            binding=binding,
                            now=now,
                        ),
                        scheduler_fence_verifier=self._source_stage_fence_verifier,
                    )
                except LabSourceStageError:
                    return self._reconcile_source_stage(
                        record,
                        binding=binding,
                        writer=writer,
                        now=now,
                        reason="source_stage_ready_uncertain",
                    )
                if ready.state is LabSourceStageState.READY:
                    return _SourceStageTick(ready=1)
                return self._reconcile_source_stage(
                    record,
                    binding=binding,
                    writer=writer,
                    now=now,
                    reason="source_stage_ready_conflict",
                )
            return _SourceStageTick(pending=1, deferred=1)
        if stage.state is LabSourceStageState.READY:
            return _SourceStageTick(ready=1)
        if stage.state is LabSourceStageState.FAILED:
            return _SourceStageTick(failed=1)
        if stage.state is LabSourceStageState.RECONCILE_REQUIRED:
            return _SourceStageTick(reconcile_required=1)
        if stage.state is not LabSourceStageState.QUEUED:
            return self._reconcile_source_stage(
                record,
                binding=binding,
                writer=writer,
                now=now,
                reason="source_stage_state_conflict",
            )
        if stage.intent_bytes != record.source_intent_bytes:
            return self._reconcile_source_stage(
                record,
                binding=binding,
                writer=writer,
                now=now,
                reason="source_stage_intent_conflict",
            )
        if not permit_held:
            with self._v2_emit_permit(record) as permitted:
                if not permitted:
                    return _SourceStageTick(deferred=1, claims_blocked=1)
                return self._advance_source_queued(
                    record,
                    binding=binding,
                    lease=lease,
                    writer=writer,
                    now=now,
                    permit_held=True,
                )
        try:
            operation_id = self.source_scheduler_queue.enqueue_intent_bytes(stage.intent_bytes)
        except SourceBrokerV2SchedulerQueueBackpressureError as exc:
            self._source_stage_log(
                "source_stage_queue_deferred",
                record,
                reason="source_queue_backpressure",
                error=exc,
            )
            return _SourceStageTick(queued=1, deferred=1)
        except SourceBrokerV2SchedulerQueueError:
            return self._reconcile_source_stage(
                record,
                binding=binding,
                writer=writer,
                now=now,
                reason="source_queue_uncertain",
            )
        if operation_id != record.source_operation_id:
            return self._reconcile_source_stage(
                record,
                binding=binding,
                writer=writer,
                now=now,
                reason="source_operation_conflict",
            )
        self._after_source_queue_enqueued(record)
        try:
            pending = self.source_stage_store.begin_external(
                binding,
                stage.intent,
                lease=writer,
                now=now,
                scheduler_fence_receipt=self._source_stage_receipt(
                    lease=lease, binding=binding, now=now
                ),
                scheduler_fence_verifier=self._source_stage_fence_verifier,
            )
        except LabSourceStageError:
            return self._reconcile_source_stage(
                record,
                binding=binding,
                writer=writer,
                now=now,
                reason="source_stage_pending_uncertain",
            )
        if pending.state is not LabSourceStageState.PENDING:
            return self._reconcile_source_stage(
                record,
                binding=binding,
                writer=writer,
                now=now,
                reason="source_stage_pending_conflict",
            )
        self._after_source_stage_pending(record)
        return _SourceStageTick(pending=1)

    def _emit_v2_source_candidate(
        self,
        record: LabClaimPublicationRecord,
        *,
        binding: LabSourceStageBinding,
        intent: SourceBrokerV2JobIntentEnvelope,
        lease: LabLeaseRecord,
        writer: LabSourceStageWriterLease,
        now: datetime,
    ) -> _SourceStageTick:
        """One new V2 emit saga, fenced across stage, ledger, queue, and pending."""

        assert self.source_stage_store is not None
        with self._v2_emit_permit(record) as permitted:
            if not permitted:
                return _SourceStageTick(deferred=1, claims_held=1, claims_blocked=1)
            try:
                stage = self.source_stage_store.enqueue_external(
                    binding,
                    intent,
                    lease=writer,
                    now=now,
                    scheduler_fence_receipt=self._source_stage_receipt(
                        lease=lease, binding=binding, now=now
                    ),
                    scheduler_fence_verifier=self._source_stage_fence_verifier,
                )
            except LabSourceStageAuthorityError as exc:
                self._source_stage_log(
                    "source_stage_authority_mismatch",
                    record,
                    reason="source_stage_authority_mismatch",
                    level="error",
                    error=exc,
                )
                return _SourceStageTick(reconcile_required=1, claims_held=1)
            except LabSourceStageError:
                return self._reconcile_source_stage(
                    record,
                    binding=binding,
                    writer=writer,
                    now=now,
                    reason="source_stage_queue_conflict",
                )
            if stage.state is not LabSourceStageState.QUEUED:
                return self._reconcile_source_stage(
                    record,
                    binding=binding,
                    writer=writer,
                    now=now,
                    reason="source_stage_queue_state_conflict",
                )
            intent_bytes = canonical_job_model_bytes(intent)
            binding_bytes = canonical_model_json_bytes(binding)
            try:
                queued = self.store._queue_claim_publication_after_scheduler_takeover(
                    record.identity,
                    QueueBinding(
                        source_stage_binding_bytes=binding_bytes,
                        source_stage_binding_hash=hashlib.sha256(binding_bytes).hexdigest(),
                        source_intent_bytes=intent_bytes,
                        source_intent_hash=hashlib.sha256(intent_bytes).hexdigest(),
                        source_operation_id=intent.operation_id,
                        source_operation_hash=intent.operation_hash,
                    ),
                    lease=lease,
                    now=now,
                ).record
            except Exception as exc:
                self._source_stage_log(
                    "source_stage_ledger_queue_failed",
                    record,
                    reason="source_stage_ledger_queue_failed",
                    level="error",
                    error=exc,
                )
                return _SourceStageTick(reconcile_required=1, claims_held=1)
            self._after_source_stage_queued(queued)
            advanced = self._advance_source_queued(
                queued,
                binding=binding,
                lease=lease,
                writer=writer,
                now=now,
                permit_held=True,
            )
            advanced.claims_held += 1
            return advanced

    def _advance_source_publication(
        self,
        record: LabClaimPublicationRecord,
        *,
        lease: LabLeaseRecord,
        now: datetime,
        writer: LabSourceStageWriterLease | None,
    ) -> _SourceStageTick:
        if record.status is ClaimPublicationStatus.READY_TO_PUBLISH:
            return _SourceStageTick(ready=1)
        if not self._source_stage_enabled:
            self._source_stage_log(
                "source_stage_blocked",
                record,
                reason="source_stage_dependencies_missing",
            )
            return _SourceStageTick(deferred=1, claims_blocked=1)
        assert self.source_stage_store is not None
        if record.status is ClaimPublicationStatus.HELD_SOURCE:
            if now >= record.source_wait_deadline:
                if self._abort_source_publication(
                    record,
                    reason="source_wait_expired",
                    lease=lease,
                    now=now,
                ):
                    return _SourceStageTick(failed=1, claims_held=1)
                return _SourceStageTick(reconcile_required=1, claims_held=1)
            if writer is None:
                return _SourceStageTick(deferred=1, claims_held=1, claims_blocked=1)
            try:
                binding, intent = self._source_intent_for_held(record, now=now)
            except SourceOperationContractError as exc:
                self._source_stage_log(
                    "source_stage_authorization_rejected",
                    record,
                    reason="source_authorization_invalid",
                    error=exc,
                )
                if self._abort_source_publication(
                    record,
                    reason="source_authorization_invalid",
                    lease=lease,
                    now=now,
                ):
                    return _SourceStageTick(failed=1, claims_held=1)
                return _SourceStageTick(reconcile_required=1, claims_held=1)
            return self._emit_v2_source_candidate(
                record,
                binding=binding,
                intent=intent,
                lease=lease,
                writer=writer,
                now=now,
            )
        if record.status is ClaimPublicationStatus.SOURCE_QUEUED:
            if (
                record.source_stage_binding_bytes is None
                or record.source_intent_bytes is None
                or record.source_operation_id is None
            ):
                return _SourceStageTick(reconcile_required=1)
            try:
                binding = strict_model_validate_canonical_json(
                    LabSourceStageBinding,
                    record.source_stage_binding_bytes.decode("utf-8"),
                )
            except (UnicodeDecodeError, TypeError, ValueError) as exc:
                self._source_stage_log(
                    "source_stage_binding_invalid",
                    record,
                    reason="source_stage_binding_invalid",
                    level="error",
                    error=exc,
                )
                return _SourceStageTick(reconcile_required=1)
            return self._advance_source_queued(
                record,
                binding=binding,
                lease=lease,
                writer=writer,
                now=now,
            )
        return _SourceStageTick(reconcile_required=1)

    def _recover_source_stage(
        self,
        *,
        lease: LabLeaseRecord,
        now: datetime,
    ) -> _SourceStageTick:
        result = _SourceStageTick()
        candidates = self.store.list_v2_reconciliation_candidates(
            now=now,
            limit=self.max_source_stage_per_tick,
        )
        for record in candidates:
            binding = self._source_stage_binding(record.identity)
            writer = (
                self._source_stage_writer(
                    now=now,
                    scheduler_lease=lease,
                    binding=binding,
                )
                if self._source_stage_enabled
                else None
            )
            result.add(
                self._advance_source_publication(
                    record,
                    lease=lease,
                    now=now,
                    writer=writer,
                )
            )
        return result

    def run_once(self) -> SchedulerTickResult:
        self._verify_runtime()
        self._audit_integrity(phase="scheduler_pre_tick")
        self._audit_full_integrity_if_due(phase="scheduler_pre_tick")
        acquired = self._start_tick()
        recovered = 0
        if acquired:
            lease, recovery_now = self._mutation_context()
            self._verify_runtime()
            if self.lifecycle_synchronizer is not None and not self._lifecycle_recovered:
                self.lifecycle_synchronizer.recover(observed_at=recovery_now)
                self._lifecycle_recovered = True
            recovered_jobs = self.store.recover_expired_jobs(lease, now=recovery_now)
            recovered = len(recovered_jobs)
            for job in recovered_jobs:
                self._synchronize_lifecycle(job.job_id, observed_at=recovery_now)
        else:
            lease, recovery_now = self._mutation_context()
        self._verify_runtime()
        source_stage = self._recover_source_stage(lease=lease, now=recovery_now)
        authority_now = recovery_now
        processed = 0
        applied = 0
        rejected = 0
        quarantined = 0
        deadline_lease = lease
        deadline_now = recovery_now
        for path in self.spool.pending_paths(limit=self.max_commands_per_tick):
            self._verify_runtime()
            try:
                entry = self.spool.load(path)
            except InvalidCommandEnvelopeError as exc:
                self._verify_runtime()
                self.spool.quarantine(
                    exc.file_identity or path,
                    reason=f"invalid_envelope:{exc}",
                )
                quarantined += 1
                continue
            self._verify_runtime()
            command = entry.envelope.command
            if (
                isinstance(command, SubmitJobCommand)
                and command.spec.schema_version == 3
                and self.lifecycle_synchronizer is None
            ):
                raise RuntimeError("formal v3 job requires Experiment lifecycle authority")
            lease, mutation_now = self._mutation_context()
            authority_now = mutation_now
            deadline_lease = lease
            deadline_now = mutation_now
            try:
                self._verify_runtime()
                receipt = self.store.apply_command(
                    entry.envelope,
                    lease=lease,
                    now=mutation_now,
                    submission_authority=self._validate_submission_authority,
                )
            except FormalSubmissionAuthorityError as exc:
                self._verify_runtime()
                self.spool.quarantine(
                    entry,
                    reason=f"formal_submission_authority:{exc}",
                )
                quarantined += 1
                continue
            except RequestContentConflictError as exc:
                self._verify_runtime()
                self.spool.quarantine(
                    entry,
                    reason=f"request_content_conflict:{exc}",
                )
                quarantined += 1
                continue
            processed += 1
            if receipt.status == "applied":
                applied += 1
                self._synchronize_lifecycle(
                    entry.envelope.command.job_id,
                    observed_at=mutation_now,
                )
            else:
                rejected += 1
            self._verify_runtime()
            self.spool.ack(entry, receipt)
        self._verify_runtime()
        deadlines_expired = self._expire_deadlines(
            deadline_lease,
            observed_at=deadline_now,
        )
        reports_processed = 0
        reports_accepted = 0
        reports_rejected = 0
        reports_quarantined = 0
        if self.report_spool is not None:
            for path in self.report_spool.pending_paths(limit=self.max_reports_per_tick):
                self._verify_runtime()
                try:
                    entry = self.report_spool.load(path)
                except InvalidCommandEnvelopeError as exc:
                    self._verify_runtime()
                    self.report_spool.quarantine(
                        exc.file_identity or path,
                        reason=f"invalid_report:{exc}",
                    )
                    reports_quarantined += 1
                    continue
                self._verify_runtime()
                lease, mutation_now = self._mutation_context()
                authority_now = mutation_now
                try:
                    self._verify_runtime()
                    receipt = self.store.apply_worker_report(
                        entry.report,
                        lease=lease,
                        now=mutation_now,
                        result_digest_policy=self.result_digest_policy,
                    )
                except RequestContentConflictError as exc:
                    self._verify_runtime()
                    _safe_structured_log(
                        "error",
                        "report_content_conflict",
                        message=_safe_error_message(exc),
                        component="lab_scheduler",
                        owner_id=self.owner_id,
                        job_id=str(entry.report.job_id),
                        shard_id=str(entry.report.shard_id),
                        report_id=str(entry.report.report_id),
                        error_type=type(exc).__name__,
                    )
                    self._verify_runtime()
                    self.report_spool.quarantine(
                        entry,
                        reason=f"report_content_conflict:{exc}",
                    )
                    reports_quarantined += 1
                    continue
                reports_processed += 1
                if receipt.status == "accepted":
                    reports_accepted += 1
                    self._synchronize_lifecycle(
                        entry.report.job_id,
                        observed_at=mutation_now,
                    )
                else:
                    reports_rejected += 1
                    _safe_structured_log(
                        "warning",
                        "worker_report_rejected",
                        message=receipt.reason,
                        component="lab_scheduler",
                        owner_id=self.owner_id,
                        job_id=str(entry.report.job_id),
                        shard_id=str(entry.report.shard_id),
                        claim_token=str(entry.report.claim_token),
                        report_id=str(entry.report.report_id),
                        report_type=entry.report.body.report_type,
                    )
                self._verify_runtime()
                self.report_spool.ack(entry, receipt)
        artifact_commits_processed = 0
        artifact_commits_accepted = 0
        artifact_commits_rejected = 0
        artifact_commits_quarantined = 0
        artifact_commit_quarantine_failures = 0
        if self.artifact_commit_spool is not None and self.artifact_store is not None:
            for path in self.artifact_commit_spool.fair_pending_paths(
                limit=self.max_artifact_commits_per_tick + 64
            ):
                self._verify_runtime()
                if artifact_commits_processed >= self.max_artifact_commits_per_tick:
                    break
                try:
                    entry = self.artifact_commit_spool.load(path)
                except InvalidCommandEnvelopeError as exc:
                    artifact_isolated = self._quarantine_artifact_commit(
                        exc.file_identity or path,
                        reason=f"invalid_artifact_commit:{exc}",
                    )
                    artifact_commits_quarantined += int(artifact_isolated)
                    artifact_commit_quarantine_failures += int(not artifact_isolated)
                    continue
                self._verify_runtime()
                try:
                    verify_finalizer_authority(
                        entry.envelope,
                        key_provider=self.finalizer_authority_key_provider,
                    )
                except LabFinalizerAuthorityAuthenticationError as exc:
                    artifact_isolated = self._quarantine_artifact_commit(
                        entry,
                        reason=(f"artifact_authority_unauthenticated:{_safe_error_message(exc)}"),
                    )
                    artifact_commits_quarantined += int(artifact_isolated)
                    artifact_commit_quarantine_failures += int(not artifact_isolated)
                    continue
                _lease, verification_now = self._mutation_context()
                authority_now = verification_now
                staged = None
                receipt = None
                try:
                    with (
                        self.artifact_store.artifact_commit_lifecycle(),
                        ExitStack() as staged_scope,
                    ):
                        with self.artifact_store.bind_verified_sealed(
                            entry.envelope.commit.sealed_path,
                            indexed_at=verification_now,
                        ) as binding:
                            lease, mutation_now = self._mutation_context()
                            authority_now = mutation_now
                            self._verify_runtime()
                            deadlines_expired += self._expire_deadlines(
                                lease,
                                observed_at=mutation_now,
                            )
                            staged = staged_scope.enter_context(
                                self.store.stage_artifact_commit(
                                    entry.envelope,
                                    binding,
                                    authority_key_provider=(self.finalizer_authority_key_provider),
                                    lease=lease,
                                    now=mutation_now,
                                )
                            )
                            self._after_artifact_commit_staged(entry, binding)
                        assert staged is not None
                        if self.lease is None:  # pragma: no cover - active tick invariant
                            raise RuntimeError("scheduler lease disappeared before artifact commit")
                        self._verify_runtime()
                        artifact_completed_at = self.clock()
                        receipt = staged.commit(
                            lease=self.lease,
                            now=artifact_completed_at,
                        )
                except RequestContentConflictError as exc:
                    artifact_isolated = self._quarantine_artifact_commit(
                        entry,
                        reason=f"artifact_commit_content_conflict:{exc}",
                    )
                    artifact_commits_quarantined += int(artifact_isolated)
                    artifact_commit_quarantine_failures += int(not artifact_isolated)
                    continue
                except LabFinalizerAuthorityAuthenticationError as exc:
                    artifact_isolated = self._quarantine_artifact_commit(
                        entry,
                        reason=f"artifact_authority_unauthenticated:{_safe_error_message(exc)}",
                    )
                    artifact_commits_quarantined += int(artifact_isolated)
                    artifact_commit_quarantine_failures += int(not artifact_isolated)
                    continue
                except BaseException as exc:
                    if receipt is not None:
                        raise
                    if not isinstance(exc, Exception) or not self._is_artifact_verification_error(
                        exc
                    ):
                        raise
                    artifact_isolated = self._quarantine_artifact_commit(
                        entry,
                        reason=f"artifact_verification_failed:{_safe_error_message(exc)}",
                    )
                    artifact_commits_quarantined += int(artifact_isolated)
                    artifact_commit_quarantine_failures += int(not artifact_isolated)
                    continue
                assert receipt is not None
                self._after_artifact_commit_sqlite_commit(entry)
                self._verify_runtime()
                artifact_commits_processed += 1
                if receipt.status == "accepted":
                    artifact_commits_accepted += 1
                    self._synchronize_lifecycle(
                        entry.envelope.commit.job_id,
                        observed_at=artifact_completed_at,
                    )
                else:
                    artifact_commits_rejected += 1
                self._verify_runtime()
                self.artifact_commit_spool.ack(entry, receipt)
        plans_created = 0
        plans_failed = 0
        if self.adapter_registry is not None:
            for job in self.store.list_unplanned_jobs(limit=self.max_plans_per_tick):
                self._verify_runtime()
                try:
                    definitions = self.adapter_registry.plan(job.spec)
                except Exception as exc:
                    lease, mutation_now = self._mutation_context()
                    authority_now = mutation_now
                    self._verify_runtime()
                    _safe_structured_log(
                        "error",
                        "adapter_plan_failed",
                        message=_safe_error_message(exc),
                        component="lab_scheduler",
                        owner_id=self.owner_id,
                        job_id=str(job.job_id),
                        error_type=type(exc).__name__,
                    )
                    self._verify_runtime()
                    failed = self.store.fail_unplanned_job(
                        job.job_id,
                        reason=f"adapter plan failed: {_safe_plan_failure(exc)}",
                        lease=lease,
                        now=mutation_now,
                    )
                    if failed:
                        self._synchronize_lifecycle(job.job_id, observed_at=mutation_now)
                    plans_failed += 1
                    continue
                lease, mutation_now = self._mutation_context()
                authority_now = mutation_now
                self._verify_runtime()
                self.store.plan_job(
                    job.job_id,
                    definitions,
                    lease=lease,
                    now=mutation_now,
                )
                self._synchronize_lifecycle(job.job_id, observed_at=mutation_now)
                plans_created += 1
        new_claim_tokens: set[UUID] = set()
        claims_created = 0
        preclaim_blocked = 0
        if self._source_stage_enabled:
            for _ in range(self.max_claims_per_tick):
                self._verify_runtime()
                lease, mutation_now = self._mutation_context()
                authority_now = mutation_now
                deadlines_expired += self._expire_deadlines(lease, observed_at=mutation_now)
                self._verify_runtime()
                selection = self.store.claim_next_source_stage(
                    shard_lease_seconds=self.shard_lease_seconds,
                    lease=lease,
                    now=mutation_now,
                    source_stage_store=self.source_stage_store,
                    source_wait_deadline=mutation_now
                    + timedelta(seconds=self.source_wait_timeout_seconds),
                    publication_deadline=mutation_now
                    + timedelta(seconds=self.publication_timeout_seconds),
                    v2_precondition=self._v2_preclaim_precondition(captured_now=mutation_now),
                    include_diagnostics=True,
                )
                assert isinstance(selection, LabClaimSelection)
                preclaim_blocked += len(selection.rejections)
                for rejection in selection.rejections:
                    _safe_structured_log(
                        "warning",
                        "source_preclaim_rejected",
                        message=rejection.reason,
                        job_id=str(rejection.job_id),
                        shard_id=str(rejection.shard_id),
                        payload_hash=rejection.payload_hash,
                        reason=rejection.reason,
                    )
                claim = selection.claim
                if claim is None:
                    break
                if not isinstance(claim, LabShardClaimV2):  # pragma: no cover - store contract
                    raise RuntimeError("source-stage scheduler received a legacy worker claim")
                self._synchronize_lifecycle(claim.job_id, observed_at=mutation_now)
                held = self.store.get_claim_publication(claim.claim_token)
                if held is None:
                    _safe_structured_log(
                        "error",
                        "source_stage_held_record_missing",
                        message="v2 claim has no durable HELD publication record",
                        component="lab_scheduler",
                        owner_id=self.owner_id,
                        job_id=str(claim.job_id),
                        shard_id=str(claim.shard_id),
                        claim_token=str(claim.claim_token),
                        claim_generation=claim.claim_generation,
                        scheduler_fencing_token=claim.scheduler_fencing_token,
                        reason="source_stage_held_record_missing",
                    )
                    source_stage.reconcile_required += 1
                else:
                    self._after_source_claim_held(held)
                    source_stage.add(
                        self._advance_source_publication(
                            held,
                            lease=lease,
                            now=mutation_now,
                            writer=self._source_stage_writer(
                                now=mutation_now,
                                scheduler_lease=lease,
                                binding=self._source_stage_binding(held.identity),
                            ),
                        )
                    )
                claims_created += 1
        elif self.claim_spool is not None and self.claim_worker_ids:
            worker_count = len(self.claim_worker_ids)
            start = self._claim_cursor
            inspected = 0
            while inspected < worker_count and claims_created < self.max_claims_per_tick:
                self._verify_runtime()
                worker_id = self.claim_worker_ids[(start + inspected) % worker_count]
                inspected += 1
                lease, mutation_now = self._mutation_context()
                authority_now = mutation_now
                self._verify_runtime()
                deadlines_expired += self._expire_deadlines(
                    lease,
                    observed_at=mutation_now,
                )
                self._verify_runtime()
                selection = self.store.claim_next_shard(
                    worker_id=worker_id,
                    shard_lease_seconds=self.shard_lease_seconds,
                    lease=lease,
                    now=mutation_now,
                    source_stage_store=(
                        self.source_stage_store if self._source_stage_enabled else None
                    ),
                    source_wait_deadline=(
                        mutation_now + timedelta(seconds=self.source_wait_timeout_seconds)
                        if (
                            self._source_stage_enabled
                            and self.source_wait_timeout_seconds is not None
                        )
                        else None
                    ),
                    publication_deadline=(
                        mutation_now + timedelta(seconds=self.publication_timeout_seconds)
                        if (
                            self._source_stage_enabled
                            and self.publication_timeout_seconds is not None
                        )
                        else None
                    ),
                    allowed_payload_protocol_versions=(
                        (1, 2) if self._source_stage_enabled else (1,)
                    ),
                    v2_precondition=self._v2_preclaim_precondition(captured_now=mutation_now),
                    include_diagnostics=True,
                )
                assert isinstance(selection, LabClaimSelection)
                preclaim_blocked += len(selection.rejections)
                if selection.rejections:
                    for rejection in selection.rejections:
                        _safe_structured_log(
                            "warning",
                            "source_preclaim_rejected",
                            message=rejection.reason,
                            job_id=str(rejection.job_id),
                            shard_id=str(rejection.shard_id),
                            payload_hash=rejection.payload_hash,
                            reason=rejection.reason,
                        )
                claim = selection.claim
                if claim is None:
                    continue
                self._synchronize_lifecycle(claim.job_id, observed_at=mutation_now)
                if isinstance(claim, LabShardClaimV2):
                    held = self.store.get_claim_publication(claim.claim_token)
                    if held is None:
                        _safe_structured_log(
                            "error",
                            "source_stage_held_record_missing",
                            message="v2 claim has no durable HELD publication record",
                            component="lab_scheduler",
                            owner_id=self.owner_id,
                            job_id=str(claim.job_id),
                            shard_id=str(claim.shard_id),
                            claim_token=str(claim.claim_token),
                            claim_generation=claim.claim_generation,
                            scheduler_fencing_token=claim.scheduler_fencing_token,
                            reason="source_stage_held_record_missing",
                        )
                        source_stage.reconcile_required += 1
                    else:
                        self._after_source_claim_held(held)
                        source_stage.add(
                            self._advance_source_publication(
                                held,
                                lease=lease,
                                now=mutation_now,
                                writer=self._source_stage_writer(
                                    now=mutation_now,
                                    scheduler_lease=lease,
                                    binding=self._source_stage_binding(held.identity),
                                ),
                            )
                        )
                else:
                    new_claim_tokens.add(claim.claim_token)
                claims_created += 1
            self._claim_cursor = (start + inspected) % worker_count
        self._verify_runtime()
        authority = self._reconcile_claim_authority(
            lease,
            now=authority_now,
            new_claim_tokens=frozenset(new_claim_tokens),
        )
        self._audit_integrity(phase="scheduler_post_tick")
        return SchedulerTickResult(
            lease_acquired=acquired,
            processed=processed,
            applied=applied,
            rejected=rejected,
            quarantined=quarantined,
            recovered=recovered,
            reports_processed=reports_processed,
            reports_accepted=reports_accepted,
            reports_rejected=reports_rejected,
            reports_quarantined=reports_quarantined,
            artifact_commits_processed=artifact_commits_processed,
            artifact_commits_accepted=artifact_commits_accepted,
            artifact_commits_rejected=artifact_commits_rejected,
            artifact_commits_quarantined=artifact_commits_quarantined,
            artifact_commit_quarantine_failures=artifact_commit_quarantine_failures,
            deadlines_expired=deadlines_expired,
            plans_created=plans_created,
            plans_failed=plans_failed,
            claims_published=authority.claims_published,
            claims_replayed=authority.claims_replayed,
            claim_delivery_failures=authority.delivery_failures,
            claims_reconciled=authority.claims_reconciled,
            claim_reconcile_failures=authority.reconcile_failures,
            claims_revoked=authority.claims_revoked,
            claims_retired=authority.claims_retired,
            claim_revoke_failures=authority.revoke_failures,
            source_stage_queued=source_stage.queued,
            source_stage_pending=source_stage.pending,
            source_stage_ready=source_stage.ready,
            source_stage_deferred=source_stage.deferred,
            source_stage_failed=source_stage.failed,
            source_stage_reconcile_required=source_stage.reconcile_required,
            claims_held=source_stage.claims_held,
            claims_blocked_by_source_stage=source_stage.claims_blocked,
            preclaim_blocked=preclaim_blocked,
        )

    def request_stop(self) -> None:
        self._stop.set()

    def release(self) -> None:
        if self.lease is None:
            return
        lease = self.lease
        self.lease = None
        try:
            self.store.release_scheduler_lease(lease, now=self.clock())
        except SchedulerLeaseFencedError:
            return

    def run_forever(self) -> None:
        try:
            while not self._stop.is_set():
                result = self.run_once()
                self._log_tick_anomalies(result)
                self._stop.wait(self.poll_interval_ms / 1_000)
        finally:
            self.release()

    def _log_tick_anomalies(self, result: SchedulerTickResult) -> None:
        anomaly_counts = {
            name: value
            for name, value in {
                "quarantined": result.quarantined,
                "reports_rejected": result.reports_rejected,
                "reports_quarantined": result.reports_quarantined,
                "artifact_commits_rejected": result.artifact_commits_rejected,
                "artifact_commits_quarantined": result.artifact_commits_quarantined,
                "artifact_commit_quarantine_failures": (result.artifact_commit_quarantine_failures),
                "plans_failed": result.plans_failed,
                "claim_delivery_failures": result.claim_delivery_failures,
                "claim_reconcile_failures": result.claim_reconcile_failures,
                "claims_revoked": result.claims_revoked,
                "claim_revoke_failures": result.claim_revoke_failures,
                "source_stage_failed": result.source_stage_failed,
                "source_stage_reconcile_required": result.source_stage_reconcile_required,
                "claims_blocked_by_source_stage": result.claims_blocked_by_source_stage,
                "preclaim_blocked": result.preclaim_blocked,
            }.items()
            if value
        }
        if not anomaly_counts:
            return
        _safe_structured_log(
            "warning",
            "tick_anomalies",
            message="Strategy Lab scheduler tick completed with anomalies",
            component="lab_scheduler",
            owner_id=self.owner_id,
            anomaly_counts=anomaly_counts,
        )
