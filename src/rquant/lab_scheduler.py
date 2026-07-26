"""Singleton control loop for the durable Strategy Lab command inbox."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from rquant.lab_artifact_protocol import (
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
    LabFinalizerAuthorityAuthenticationError,
    LabFinalizerAuthorityVerificationKeyProvider,
)
from rquant.lab_artifacts import LabArtifactError, LabJobArtifactStore, LabVerifiedSealedBinding
from rquant.lab_job_protocol import (
    InvalidCommandEnvelopeError,
    LabCommandSpool,
    LabSpoolFileIdentity,
    RequestContentConflictError,
)
from rquant.lab_jobs import (
    LabJobStore,
    LabLeaseRecord,
    SchedulerLeaseFencedError,
)
from rquant.lab_logging import _safe_structured_log
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool, LabShardClaim
from rquant.strategy_job_adapters import StrategyJobAdapterRegistry


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


def _system_clock() -> datetime:
    return datetime.now(UTC)


def _safe_plan_failure(exc: Exception) -> str:
    message = " ".join((str(exc) or type(exc).__name__).split())
    return f"{type(exc).__name__}: {message[:400]}"


def _safe_error_message(exc: Exception) -> str:
    return " ".join((str(exc) or type(exc).__name__).split())[:400]


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
        if shard_lease_seconds < 1:
            raise ValueError("shard_lease_seconds must be positive")
        if max_reports_per_tick < 1:
            raise ValueError("max_reports_per_tick must be positive")
        if max_plans_per_tick < 1:
            raise ValueError("max_plans_per_tick must be positive")
        if max_claims_per_tick < 1:
            raise ValueError("max_claims_per_tick must be positive")
        if max_claim_authority_per_tick < 1:
            raise ValueError("max_claim_authority_per_tick must be positive")
        if max_artifact_commits_per_tick < 1:
            raise ValueError("max_artifact_commits_per_tick must be positive")
        if (artifact_commit_spool is None) != (artifact_store is None):
            raise ValueError("artifact commit spool and artifact store must be configured together")
        if artifact_commit_spool is not None and finalizer_authority_key_provider is None:
            raise ValueError("finalizer authority key provider is required for artifact commits")
        normalized_workers = tuple(worker.strip() for worker in claim_worker_ids)
        if any(not worker for worker in normalized_workers):
            raise ValueError("claim_worker_ids must not contain empty values")
        if len(set(normalized_workers)) != len(normalized_workers):
            raise ValueError("claim_worker_ids must be unique")
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
        self.clock = clock
        self.lease: LabLeaseRecord | None = None
        self._claim_cursor = 0
        self._claim_cursor_fence: int | None = None
        self._stop = Event()

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
        if self.claim_spool is None:
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
                    self.claim_spool.retire(
                        delivery,
                        outcome="accepted",
                        reason="scheduler accepted shard success",
                    )
                else:
                    self.claim_spool.revoke(
                        delivery,
                        reason="sqlite claim is no longer active",
                    )
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

    def run_once(self) -> SchedulerTickResult:
        acquired = self._start_tick()
        recovered = 0
        if acquired:
            lease, recovery_now = self._mutation_context()
            recovered = len(self.store.recover_expired_jobs(lease, now=recovery_now))
        else:
            lease, recovery_now = self._mutation_context()
        recovered += len(
            self.store.recover_stale_shards(
                lease,
                now=recovery_now,
            )
        )
        authority_now = recovery_now
        processed = 0
        applied = 0
        rejected = 0
        quarantined = 0
        deadline_lease = lease
        deadline_now = recovery_now
        for path in self.spool.pending_paths(limit=self.max_commands_per_tick):
            try:
                entry = self.spool.load(path)
            except InvalidCommandEnvelopeError as exc:
                self.spool.quarantine(
                    exc.file_identity or path,
                    reason=f"invalid_envelope:{exc}",
                )
                quarantined += 1
                continue
            lease, mutation_now = self._mutation_context()
            authority_now = mutation_now
            deadline_lease = lease
            deadline_now = mutation_now
            try:
                receipt = self.store.apply_command(
                    entry.envelope,
                    lease=lease,
                    now=mutation_now,
                )
            except RequestContentConflictError as exc:
                self.spool.quarantine(
                    entry,
                    reason=f"request_content_conflict:{exc}",
                )
                quarantined += 1
                continue
            processed += 1
            if receipt.status == "applied":
                applied += 1
            else:
                rejected += 1
            self.spool.ack(entry, receipt)
        deadlines_expired = len(
            self.store.expire_deadline_jobs(
                lease=deadline_lease,
                now=deadline_now,
            )
        )
        reports_processed = 0
        reports_accepted = 0
        reports_rejected = 0
        reports_quarantined = 0
        if self.report_spool is not None:
            for path in self.report_spool.pending_paths(limit=self.max_reports_per_tick):
                try:
                    entry = self.report_spool.load(path)
                except InvalidCommandEnvelopeError as exc:
                    self.report_spool.quarantine(
                        exc.file_identity or path,
                        reason=f"invalid_report:{exc}",
                    )
                    reports_quarantined += 1
                    continue
                lease, mutation_now = self._mutation_context()
                authority_now = mutation_now
                try:
                    receipt = self.store.apply_worker_report(
                        entry.report,
                        lease=lease,
                        now=mutation_now,
                    )
                except RequestContentConflictError as exc:
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
                    self.report_spool.quarantine(
                        entry,
                        reason=f"report_content_conflict:{exc}",
                    )
                    reports_quarantined += 1
                    continue
                reports_processed += 1
                if receipt.status == "accepted":
                    reports_accepted += 1
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
                            deadlines_expired += len(
                                self.store.expire_deadline_jobs(
                                    lease=lease,
                                    now=mutation_now,
                                )
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
                        receipt = staged.commit(
                            lease=self.lease,
                            now=self.clock(),
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
                artifact_commits_processed += 1
                if receipt.status == "accepted":
                    artifact_commits_accepted += 1
                else:
                    artifact_commits_rejected += 1
                self.artifact_commit_spool.ack(entry, receipt)
        plans_created = 0
        plans_failed = 0
        if self.adapter_registry is not None:
            for job in self.store.list_unplanned_jobs(limit=self.max_plans_per_tick):
                try:
                    definitions = self.adapter_registry.plan(job.spec)
                except Exception as exc:
                    lease, mutation_now = self._mutation_context()
                    authority_now = mutation_now
                    _safe_structured_log(
                        "error",
                        "adapter_plan_failed",
                        message=_safe_error_message(exc),
                        component="lab_scheduler",
                        owner_id=self.owner_id,
                        job_id=str(job.job_id),
                        error_type=type(exc).__name__,
                    )
                    self.store.fail_unplanned_job(
                        job.job_id,
                        reason=f"adapter plan failed: {_safe_plan_failure(exc)}",
                        lease=lease,
                        now=mutation_now,
                    )
                    plans_failed += 1
                    continue
                lease, mutation_now = self._mutation_context()
                authority_now = mutation_now
                self.store.plan_job(
                    job.job_id,
                    definitions,
                    lease=lease,
                    now=mutation_now,
                )
                plans_created += 1
        new_claim_tokens: set[UUID] = set()
        claims_created = 0
        if self.claim_spool is not None and self.claim_worker_ids:
            worker_count = len(self.claim_worker_ids)
            start = self._claim_cursor
            inspected = 0
            while inspected < worker_count and claims_created < self.max_claims_per_tick:
                worker_id = self.claim_worker_ids[(start + inspected) % worker_count]
                inspected += 1
                lease, mutation_now = self._mutation_context()
                authority_now = mutation_now
                deadlines_expired += len(
                    self.store.expire_deadline_jobs(
                        lease=lease,
                        now=mutation_now,
                    )
                )
                claim = self.store.claim_next_shard(
                    worker_id=worker_id,
                    shard_lease_seconds=self.shard_lease_seconds,
                    lease=lease,
                    now=mutation_now,
                )
                if claim is None:
                    continue
                new_claim_tokens.add(claim.claim_token)
                claims_created += 1
            self._claim_cursor = (start + inspected) % worker_count
        authority = self._reconcile_claim_authority(
            lease,
            now=authority_now,
            new_claim_tokens=frozenset(new_claim_tokens),
        )
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
