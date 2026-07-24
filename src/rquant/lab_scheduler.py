"""Singleton control loop for the durable Strategy Lab command inbox."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from threading import Event
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from rquant.lab_job_protocol import (
    InvalidCommandEnvelopeError,
    LabCommandSpool,
    RequestContentConflictError,
)
from rquant.lab_jobs import (
    LabJobStore,
    LabLeaseRecord,
    SchedulerLeaseFencedError,
)
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
    deadlines_expired: int = Field(default=0, ge=0)
    plans_created: int = Field(default=0, ge=0)
    plans_failed: int = Field(default=0, ge=0)
    claims_published: int = Field(default=0, ge=0)
    claims_replayed: int = Field(default=0, ge=0)
    claim_delivery_failures: int = Field(default=0, ge=0)
    claims_reconciled: int = Field(default=0, ge=0)
    claim_reconcile_failures: int = Field(default=0, ge=0)
    claims_revoked: int = Field(default=0, ge=0)
    claim_revoke_failures: int = Field(default=0, ge=0)


def _system_clock() -> datetime:
    return datetime.now(UTC)


def _safe_plan_failure(exc: Exception) -> str:
    message = " ".join((str(exc) or type(exc).__name__).split())
    return f"{type(exc).__name__}: {message[:400]}"


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
        self.clock = clock
        self.lease: LabLeaseRecord | None = None
        self._claim_cursor = 0
        self._claim_cursor_fence: int | None = None
        self._stop = Event()

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
        attempted: set[UUID],
    ) -> tuple[int, int, tuple[LabShardClaim, ...]]:
        if self.claim_spool is None:
            return 0, 0, ()
        active = self.store.list_active_claims(
            lease,
            now=now,
            initial_lease_seconds=self.shard_lease_seconds,
        )
        active_by_token = {claim.claim_token: claim for claim in active}
        accepted_success_tokens = self.store.list_accepted_success_claim_tokens(
            lease,
            now=now,
        )
        revoked = 0
        failures = 0
        try:
            deliveries = self.claim_spool.delivery_claims()
        except Exception:
            return 0, 1, active
        for delivery in deliveries:
            if active_by_token.get(delivery.claim_token) == delivery:
                continue
            if delivery.claim_token in accepted_success_tokens:
                continue
            if delivery.claim_token in attempted:
                continue
            attempted.add(delivery.claim_token)
            try:
                self.claim_spool.revoke(
                    delivery,
                    reason="sqlite claim is no longer active",
                )
            except Exception:
                failures += 1
            else:
                revoked += 1
        return revoked, failures, active

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
        attempted_revokes: set[UUID] = set()
        claims_revoked, claim_revoke_failures, active_claims = self._reconcile_claim_authority(
            lease,
            now=recovery_now,
            attempted=attempted_revokes,
        )
        claims_replayed = 0
        claim_delivery_failures = 0
        if self.claim_spool is not None:
            for active_claim in active_claims:
                try:
                    self.claim_spool.publish(active_claim)
                except Exception:
                    claim_delivery_failures += 1
                else:
                    claims_replayed += 1
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
                self.report_spool.ack(entry, receipt)
        plans_created = 0
        plans_failed = 0
        if self.adapter_registry is not None:
            for job in self.store.list_unplanned_jobs(limit=self.max_plans_per_tick):
                try:
                    definitions = self.adapter_registry.plan(job.spec)
                except Exception as exc:
                    lease, mutation_now = self._mutation_context()
                    authority_now = mutation_now
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
        claims_published = 0
        if self.claim_spool is not None and self.claim_worker_ids:
            worker_count = len(self.claim_worker_ids)
            start = self._claim_cursor
            inspected = 0
            while inspected < worker_count and claims_published < self.max_claims_per_tick:
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
                try:
                    self.claim_spool.publish(claim)
                except Exception:
                    claim_delivery_failures += 1
                else:
                    claims_published += 1
            self._claim_cursor = (start + inspected) % worker_count
        if self.claim_spool is not None:
            final_revoked, final_revoke_failures, _active = self._reconcile_claim_authority(
                lease,
                now=authority_now,
                attempted=attempted_revokes,
            )
            claims_revoked += final_revoked
            claim_revoke_failures += final_revoke_failures
        claims_reconciled = 0
        claim_reconcile_failures = 0
        if self.claim_spool is not None:
            for outcome in self.claim_spool.reconcile_current():
                if outcome.status == "reconciled":
                    claims_reconciled += 1
                elif outcome.status == "failed":
                    claim_reconcile_failures += 1
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
            deadlines_expired=deadlines_expired,
            plans_created=plans_created,
            plans_failed=plans_failed,
            claims_published=claims_published,
            claims_replayed=claims_replayed,
            claim_delivery_failures=claim_delivery_failures,
            claims_reconciled=claims_reconciled,
            claim_reconcile_failures=claim_reconcile_failures,
            claims_revoked=claims_revoked,
            claim_revoke_failures=claim_revoke_failures,
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
                self.run_once()
                self._stop.wait(self.poll_interval_ms / 1_000)
        finally:
            self.release()
