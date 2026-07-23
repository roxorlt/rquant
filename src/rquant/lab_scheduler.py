"""Singleton control loop for the durable Strategy Lab command inbox."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from threading import Event

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
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool


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
    claims_published: int = Field(default=0, ge=0)


def _system_clock() -> datetime:
    return datetime.now(UTC)


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
        self.max_claims_per_tick = max_claims_per_tick
        self.clock = clock
        self.lease: LabLeaseRecord | None = None
        self._stop = Event()

    def _start_tick(self) -> bool:
        if self.lease is None:
            now = self.clock()
            lease = self.store.acquire_scheduler_lease(
                owner_id=self.owner_id,
                lease_seconds=self.lease_seconds,
                now=now,
            )
            self.lease = lease
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

    def run_once(self) -> SchedulerTickResult:
        acquired = self._start_tick()
        recovered = 0
        if acquired:
            lease, recovery_now = self._mutation_context()
            recovered = len(self.store.recover_expired_jobs(lease, now=recovery_now))
        processed = 0
        applied = 0
        rejected = 0
        quarantined = 0
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
        claims_published = 0
        if self.claim_spool is not None:
            for worker_id in self.claim_worker_ids[: self.max_claims_per_tick]:
                lease, mutation_now = self._mutation_context()
                claim = self.store.claim_next_shard(
                    worker_id=worker_id,
                    shard_lease_seconds=self.shard_lease_seconds,
                    lease=lease,
                    now=mutation_now,
                )
                if claim is None:
                    continue
                self.claim_spool.publish(claim)
                claims_published += 1
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
            claims_published=claims_published,
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
