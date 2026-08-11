from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore, ShardStatus
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import (
    LabAcknowledgedReport,
    LabClaimSpool,
    LabReportReceipt,
    LabReportSpool,
    LabReportSpoolEntry,
    LabRevokedClaim,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabWorkerReport,
)

from .test_lab_jobs import NOW, _lease, _submit, _submit_job
from .test_lab_shard_control_plane import _cancel, _definition, _pause, _report, _success


def _scheduler(
    tmp_path: Path,
    *,
    clock: list,
    report_spool: LabReportSpool | None = None,
    claim_spool: LabClaimSpool | None = None,
    claim_worker_ids: tuple[str, ...] = (),
    max_reports: int = 64,
    max_claims: int = 16,
) -> tuple[LabJobStore, LabScheduler]:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    scheduler = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "commands"),
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        report_spool=report_spool,
        claim_spool=claim_spool,
        claim_worker_ids=claim_worker_ids,
        shard_lease_seconds=30,
        max_reports_per_tick=max_reports,
        max_claims_per_tick=max_claims,
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    assert scheduler.lease is not None
    return store, scheduler


def _planned_job(store: LabJobStore, scheduler: LabScheduler, *, count: int = 1):
    assert scheduler.lease is not None
    job = _submit_job(store, scheduler.lease)
    store.plan_job(
        job.job_id,
        tuple(_definition(index) for index in range(count)),
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=1),
    )
    return job


def test_scheduler_publishes_only_bounded_claims(tmp_path: Path) -> None:
    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    store, scheduler = _scheduler(
        tmp_path,
        clock=clock,
        claim_spool=claims,
        claim_worker_ids=("worker-a", "worker-b"),
        max_claims=1,
    )
    job = _planned_job(store, scheduler, count=2)
    clock[0] = NOW + timedelta(seconds=2)

    result = scheduler.run_once()

    assert result.claims_published == 1
    assert len(claims.pending()) == 1
    shards = LabJobReader(store.path).list_shards(job.job_id)
    assert sum(shard.status.value == "running" for shard in shards) == 1


def test_scheduler_repairs_max_attempts_one_claim_after_pending_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    store, scheduler = _scheduler(
        tmp_path,
        clock=clock,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
    )
    assert scheduler.lease is not None
    job = _submit_job(store, scheduler.lease, max_attempts=1)
    store.plan_job(
        job.job_id,
        (_definition(0),),
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=1),
    )
    original_publish = claims._publish_no_clobber
    failed = False

    def fail_pending_once(target: Path, payload: bytes) -> bool:
        nonlocal failed
        if target.parent == claims.pending_dir and not failed:
            failed = True
            raise OSError("injected claim pending failure")
        return original_publish(target, payload)

    monkeypatch.setattr(claims, "_publish_no_clobber", fail_pending_once)
    clock[0] = NOW + timedelta(seconds=2)

    first = scheduler.run_once()

    assert first.claim_delivery_failures == 1
    assert claims.pending() == ()
    shard = LabJobReader(store.path).list_shards(job.job_id)[0]
    assert shard.status is ShardStatus.RUNNING
    assert shard.attempt_count == 1

    clock[0] = NOW + timedelta(seconds=3)
    repaired = scheduler.run_once()

    assert repaired.claims_replayed == 1
    assert repaired.claim_delivery_failures == 0
    assert len(claims.pending()) == 1
    assert claims.current(job.job_id, shard.shard_id).claim == claims.pending()[0].claim
    assert LabJobReader(store.path).list_shards(job.job_id)[0].attempt_count == 1


def test_scheduler_rotates_fairly_across_bounded_claim_ticks(tmp_path: Path) -> None:
    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    store, scheduler = _scheduler(
        tmp_path,
        clock=clock,
        claim_spool=claims,
        claim_worker_ids=("worker-a", "worker-b", "worker-c"),
        max_claims=1,
    )
    _planned_job(store, scheduler, count=3)

    published = []
    for offset in (2, 3, 4):
        clock[0] = NOW + timedelta(seconds=offset)
        published.append(scheduler.run_once().claims_published)

    assert published == [1, 1, 1]
    assert [entry.claim.worker_id for entry in claims.pending()] == [
        "worker-a",
        "worker-b",
        "worker-c",
    ]


def test_scheduler_restart_seeds_rotation_from_new_fencing_generation(
    tmp_path: Path,
) -> None:
    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    store, first = _scheduler(
        tmp_path,
        clock=clock,
        claim_spool=claims,
        claim_worker_ids=("worker-a", "worker-b", "worker-c"),
        max_claims=1,
    )
    _planned_job(store, first, count=2)
    clock[0] = NOW + timedelta(seconds=2)
    assert first.run_once().claims_published == 1
    first.release()

    restarted = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        claim_spool=claims,
        claim_worker_ids=("worker-a", "worker-b", "worker-c"),
        shard_lease_seconds=30,
        max_claims_per_tick=1,
        clock=lambda: clock[0],
    )
    clock[0] = NOW + timedelta(seconds=3)

    result = restarted.run_once()

    assert result.claims_published == 1
    assert result.claims_revoked == 1
    assert [entry.claim.worker_id for entry in claims.pending()] == [
        "worker-b",
    ]


def test_scheduler_consumes_only_bounded_reports(tmp_path: Path) -> None:
    clock = [NOW]
    reports = LabReportSpool(tmp_path / "reports")
    store, scheduler = _scheduler(tmp_path, clock=clock, report_spool=reports, max_reports=1)
    job = _planned_job(store, scheduler, count=2)
    assert scheduler.lease is not None
    claims = tuple(
        store.claim_next_shard(
            worker_id=f"worker-{index}",
            shard_lease_seconds=30,
            lease=scheduler.lease,
            now=NOW + timedelta(seconds=2),
        )
        for index in range(2)
    )
    assert all(claim is not None for claim in claims)
    for claim in claims:
        assert claim is not None
        reports.publish(_report(claim, LabShardHeartbeat(lease_extension_seconds=30)))

    clock[0] = NOW + timedelta(seconds=3)
    first = scheduler.run_once()
    assert first.reports_processed == 1
    assert len(reports.pending()) == 1
    second = scheduler.run_once()
    assert second.reports_processed == 1
    assert reports.pending() == ()
    assert len(LabJobReader(store.path).list_shards(job.job_id)) == 2


class _CrashBeforeReportAckSpool(LabReportSpool):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.crash = True

    def ack(
        self,
        entry: LabReportSpoolEntry,
        receipt: LabReportReceipt,
    ) -> LabAcknowledgedReport:
        if self.crash:
            self.crash = False
            raise RuntimeError("simulated report crash after ledger commit")
        return super().ack(entry, receipt)


def test_report_commit_before_ack_crash_replays_exactly_once(tmp_path: Path) -> None:
    clock = [NOW]
    reports = _CrashBeforeReportAckSpool(tmp_path / "reports")
    store, scheduler = _scheduler(tmp_path, clock=clock, report_spool=reports)
    job = _planned_job(store, scheduler)
    assert scheduler.lease is not None
    claim = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    report = _report(claim, LabShardHeartbeat(lease_extension_seconds=30))
    reports.publish(report)
    clock[0] = NOW + timedelta(seconds=3)

    with pytest.raises(RuntimeError, match="after ledger commit"):
        scheduler.run_once()
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lab_worker_report").fetchone()[0] == 1
    assert len(reports.pending()) == 1

    replay = scheduler.run_once()
    assert replay.reports_processed == 1
    assert reports.pending() == ()
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lab_worker_report").fetchone()[0] == 1
    assert LabJobReader(store.path).get_job(job.job_id) is not None


def test_bad_and_symlink_reports_do_not_block_valid_report(tmp_path: Path) -> None:
    clock = [NOW]
    reports = LabReportSpool(tmp_path / "reports")
    store, scheduler = _scheduler(tmp_path, clock=clock, report_spool=reports)
    _planned_job(store, scheduler)
    assert scheduler.lease is not None
    claim = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    bad = reports.pending_dir / f"00000000000000000000-{uuid4()}.json"
    bad.write_text("{broken", encoding="utf-8")
    victim = tmp_path / "victim.json"
    victim.write_text("do-not-touch", encoding="utf-8")
    symlink = reports.pending_dir / f"00000000000000000001-{uuid4()}.json"
    symlink.symlink_to(victim)
    reports.publish(_report(claim, LabShardHeartbeat(lease_extension_seconds=30)))
    clock[0] = NOW + timedelta(seconds=3)

    result = scheduler.run_once()

    assert result.reports_quarantined == 2
    assert result.reports_processed == 1
    assert result.reports_accepted == 1
    assert victim.read_text(encoding="utf-8") == "do-not-touch"
    assert reports.pending() == ()


def test_scheduler_runtime_drift_after_invalid_report_load_does_not_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [NOW]
    reports = LabReportSpool(tmp_path / "reports")
    _store, scheduler = _scheduler(tmp_path, clock=clock, report_spool=reports)
    pending = reports.pending_dir / f"00000000000000000000-{uuid4()}.json"
    pending.write_text("{broken", encoding="utf-8")
    drifted = False
    original_load = reports.load

    def drift_after_load(path: Path) -> object:
        nonlocal drifted
        try:
            return original_load(path)
        finally:
            drifted = True

    def runtime_guard() -> str:
        if drifted:
            raise LabDaemonConfigurationError("runtime checkout drifted")
        return "1" * 40

    monkeypatch.setattr(reports, "load", drift_after_load)
    scheduler.runtime_guard = runtime_guard

    with pytest.raises(LabDaemonConfigurationError, match="drifted"):
        scheduler.run_once()

    assert pending.exists()
    assert tuple(reports.quarantine_dir.iterdir()) == ()


def test_oversized_heartbeat_is_quarantined_before_scheduler_or_ledger(
    tmp_path: Path,
) -> None:
    clock = [NOW]
    reports = LabReportSpool(tmp_path / "reports")
    store, scheduler = _scheduler(tmp_path, clock=clock, report_spool=reports)
    job = _planned_job(store, scheduler)
    assert scheduler.lease is not None
    claim = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    report = _report(claim, LabShardHeartbeat(lease_extension_seconds=30))
    payload = report.model_dump(mode="json")
    payload["body"]["lease_extension_seconds"] = 3_601
    hash_payload = {key: value for key, value in payload.items() if key != "content_hash"}
    hash_payload["reported_at"] = report.reported_at.isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    payload["content_hash"] = hashlib.sha256(
        json.dumps(
            hash_payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    pending = reports.pending_dir / f"00000000000000000000-{report.report_id}.json"
    pending.write_text(json.dumps(payload), encoding="utf-8")
    before = LabJobReader(store.path).list_shards(job.job_id)[0]
    clock[0] = NOW + timedelta(seconds=3)

    result = scheduler.run_once()

    assert result.reports_quarantined == 1
    assert result.reports_processed == 0
    assert LabJobReader(store.path).get_worker_report(report.report_id) is None
    after = LabJobReader(store.path).list_shards(job.job_id)[0]
    assert after == before
    assert reports.pending() == ()


def test_unknown_job_and_shard_reports_are_rejected_without_blocking_tick(
    tmp_path: Path,
) -> None:
    clock = [NOW]
    reports = LabReportSpool(tmp_path / "reports")
    store, scheduler = _scheduler(tmp_path, clock=clock, report_spool=reports)
    job = _planned_job(store, scheduler)
    assert scheduler.lease is not None
    claim = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None

    def report_with_identity(*, job_id, shard_id) -> LabWorkerReport:
        return LabWorkerReport(
            report_id=uuid4(),
            job_id=job_id,
            shard_id=shard_id,
            spec_hash=claim.spec_hash,
            payload_hash=claim.payload_hash,
            worker_id=claim.worker_id,
            claim_token=claim.claim_token,
            claim_generation=claim.claim_generation,
            scheduler_fencing_token=claim.scheduler_fencing_token,
            reported_at=NOW + timedelta(seconds=3),
            body=LabShardHeartbeat(lease_extension_seconds=30),
        )

    unknown_job = report_with_identity(job_id=uuid4(), shard_id=claim.shard_id)
    unknown_shard = report_with_identity(job_id=job.job_id, shard_id=uuid4())
    valid = _report(claim, LabShardHeartbeat(lease_extension_seconds=30))
    for report in (unknown_job, unknown_shard, valid):
        reports.publish(report)
    clock[0] = NOW + timedelta(seconds=3)

    result = scheduler.run_once()

    assert result.reports_processed == 3
    assert result.reports_rejected == 2
    assert result.reports_accepted == 1
    assert reports.pending() == ()
    reader = LabJobReader(store.path)
    assert reader.get_worker_report(unknown_job.report_id).receipt.reason == "job_not_found"
    assert reader.get_worker_report(unknown_shard.report_id).receipt.reason == "shard_not_found"


def test_scheduler_takeover_does_not_checkpoint_sharded_job_before_reclaim(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    old = _lease(store, seconds=10)
    job = _submit_job(store, old)
    store.plan_job(
        job.job_id,
        (_definition(0),),
        lease=old,
        now=NOW + timedelta(seconds=1),
    )
    old_claim = store.claim_next_shard(
        worker_id="worker-old",
        shard_lease_seconds=30,
        lease=old,
        now=NOW + timedelta(seconds=2),
    )
    assert old_claim is not None
    claims = LabClaimSpool(tmp_path / "claims")
    clock = [NOW + timedelta(seconds=11)]
    scheduler = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        claim_spool=claims,
        claim_worker_ids=("worker-new",),
        shard_lease_seconds=30,
        clock=lambda: clock[0],
    )

    result = scheduler.run_once()

    assert result.claims_published == 1
    fresh = claims.pending()[0].claim
    assert fresh.claim_generation == 2
    assert fresh.scheduler_fencing_token > old_claim.scheduler_fencing_token
    assert LabJobReader(store.path).get_job(job.job_id).status.value == "running"


@pytest.mark.parametrize(
    ("intent", "expected_job", "expected_shard"),
    [
        ("pause", JobStatus.CHECKPOINTED, ShardStatus.QUEUED),
        ("cancel", JobStatus.CANCELLED, ShardStatus.CANCELLED),
    ],
)
def test_scheduler_takeover_without_workers_converges_expired_control_intent(
    tmp_path: Path,
    intent: str,
    expected_job: JobStatus,
    expected_shard: ShardStatus,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    old = _lease(store, seconds=10)
    job = _submit_job(store, old)
    store.plan_job(
        job.job_id,
        (_definition(0),),
        lease=old,
        now=NOW + timedelta(seconds=1),
    )
    claim = store.claim_next_shard(
        worker_id="lost-worker",
        shard_lease_seconds=5,
        lease=old,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    claims = LabClaimSpool(tmp_path / "claims")
    claims.publish(claim)
    if intent == "pause":
        _pause(store, old, job.job_id, offset=3)
    else:
        assert _cancel(store, old, job.job_id, offset=3).status == "applied"
    clock = [NOW + timedelta(seconds=11)]
    scheduler = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        claim_spool=claims,
        claim_worker_ids=(),
        clock=lambda: clock[0],
    )

    result = scheduler.run_once()

    reader = LabJobReader(store.path)
    after = reader.get_job(job.job_id)
    shard = reader.list_shards(job.job_id)[0]
    assert result.recovered >= 1
    assert after is not None and after.status is expected_job
    assert shard.status is expected_shard
    assert shard.worker_id is None
    assert shard.claim_token is None
    assert result.claims_revoked == 1
    assert claims.pending() == ()
    assert isinstance(claims.publish(claim), LabRevokedClaim)


@pytest.mark.parametrize(
    ("max_attempts", "expected_job", "expected_shard"),
    [
        (3, JobStatus.RUNNING, ShardStatus.QUEUED),
        (1, JobStatus.FAILED, ShardStatus.FAILED),
    ],
)
def test_scheduler_without_workers_recovers_expired_uncontrolled_shard(
    tmp_path: Path,
    max_attempts: int,
    expected_job: JobStatus,
    expected_shard: ShardStatus,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    old = _lease(store, seconds=10)
    job = _submit_job(store, old, max_attempts=max_attempts)
    store.plan_job(
        job.job_id,
        (_definition(0),),
        lease=old,
        now=NOW + timedelta(seconds=1),
    )
    claim = store.claim_next_shard(
        worker_id="lost-worker",
        shard_lease_seconds=5,
        lease=old,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    clock = [NOW + timedelta(seconds=11)]
    scheduler = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        claim_worker_ids=(),
        clock=lambda: clock[0],
    )

    result = scheduler.run_once()

    reader = LabJobReader(store.path)
    after = reader.get_job(job.job_id)
    shard = reader.list_shards(job.job_id)[0]
    assert result.recovered >= 1
    assert after is not None and after.status is expected_job
    assert shard.status is expected_shard
    assert shard.worker_id is None
    assert shard.claim_token is None


def test_scheduler_takeover_revokes_terminal_max_attempts_claim(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    old = _lease(store, seconds=10)
    job = _submit_job(store, old, max_attempts=1)
    store.plan_job(
        job.job_id,
        (_definition(0),),
        lease=old,
        now=NOW + timedelta(seconds=1),
    )
    claim = store.claim_next_shard(
        worker_id="lost-worker",
        shard_lease_seconds=5,
        lease=old,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    claims = LabClaimSpool(tmp_path / "claims")
    claims.publish(claim)
    clock = [NOW + timedelta(seconds=11)]
    scheduler = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        claim_spool=claims,
        claim_worker_ids=(),
        shard_lease_seconds=5,
        clock=lambda: clock[0],
    )

    result = scheduler.run_once()

    assert result.claims_revoked == 1
    assert result.claim_revoke_failures == 0
    assert claims.pending() == ()
    assert isinstance(claims.publish(claim), LabRevokedClaim)
    reader = LabJobReader(store.path)
    assert reader.get_job(job.job_id).status is JobStatus.FAILED
    assert reader.list_shards(job.job_id)[0].status is ShardStatus.FAILED


def test_scheduler_takeover_revokes_pending_only_delivery(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    old = _lease(store, seconds=10)
    job = _submit_job(store, old, max_attempts=1)
    store.plan_job(
        job.job_id,
        (_definition(0),),
        lease=old,
        now=NOW + timedelta(seconds=1),
    )
    claim = store.claim_next_shard(
        worker_id="lost-worker",
        shard_lease_seconds=5,
        lease=old,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    claims = LabClaimSpool(tmp_path / "claims")
    original_publish_current = claims._publish_current_locked
    claims._publish_current_locked = lambda _marker: (_ for _ in ()).throw(
        OSError("injected current write failure")
    )
    with pytest.raises(OSError, match="current write"):
        claims.publish(claim)
    claims._publish_current_locked = original_publish_current
    assert len(claims.pending()) == 1
    clock = [NOW + timedelta(seconds=11)]
    scheduler = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        claim_spool=claims,
        claim_worker_ids=(),
        shard_lease_seconds=5,
        clock=lambda: clock[0],
    )

    result = scheduler.run_once()

    assert result.claims_revoked == 1
    assert claims.pending() == ()
    assert isinstance(claims.publish(claim), LabRevokedClaim)


def test_scheduler_restart_retries_failed_claim_revoke(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    old = _lease(store, seconds=10)
    job = _submit_job(store, old, max_attempts=1)
    store.plan_job(
        job.job_id,
        (_definition(0),),
        lease=old,
        now=NOW + timedelta(seconds=1),
    )
    claim = store.claim_next_shard(
        worker_id="lost-worker",
        shard_lease_seconds=5,
        lease=old,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    claims = LabClaimSpool(tmp_path / "claims")
    claims.publish(claim)
    original_revoke = claims.revoke
    failed = False

    def fail_once(claim_to_revoke, *, reason: str):
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("injected revoke failure")
        return original_revoke(claim_to_revoke, reason=reason)

    monkeypatch.setattr(claims, "revoke", fail_once)
    clock = [NOW + timedelta(seconds=11)]
    first = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        claim_spool=claims,
        claim_worker_ids=(),
        shard_lease_seconds=5,
        clock=lambda: clock[0],
    )

    result = first.run_once()
    assert result.claim_revoke_failures == 1
    assert len(claims.pending()) == 1
    first.release()

    restarted_claims = LabClaimSpool(tmp_path / "claims")
    clock[0] += timedelta(seconds=1)
    restarted = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "commands"),
        owner_id="scheduler-c",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        claim_spool=restarted_claims,
        claim_worker_ids=(),
        shard_lease_seconds=5,
        clock=lambda: clock[0],
    )

    retried = restarted.run_once()

    assert retried.claims_revoked == 1
    assert retried.claim_revoke_failures == 0
    assert restarted_claims.pending() == ()
    assert isinstance(restarted_claims.publish(claim), LabRevokedClaim)


def test_scheduler_deadline_expiry_revokes_running_delivery(tmp_path: Path) -> None:
    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    store, scheduler = _scheduler(
        tmp_path,
        clock=clock,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
    )
    assert scheduler.lease is not None
    base = _submit()
    envelope = _submit(
        spec=base.command.spec.model_copy(update={"deadline": NOW + timedelta(seconds=3)})
    )
    store.apply_command(envelope, lease=scheduler.lease, now=NOW)
    store.plan_job(
        envelope.command.job_id,
        (_definition(0),),
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=1),
    )
    clock[0] = NOW + timedelta(seconds=2)
    assert scheduler.run_once().claims_published == 1
    claim = claims.pending()[0].claim

    clock[0] = NOW + timedelta(seconds=3)
    expired = scheduler.run_once()

    assert expired.deadlines_expired == 1
    assert expired.claims_revoked == 1
    assert claims.pending() == ()
    assert isinstance(claims.publish(claim), LabRevokedClaim)


def test_scheduler_does_not_revoke_consumed_claim_after_accepted_success(
    tmp_path: Path,
) -> None:
    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    store, scheduler = _scheduler(
        tmp_path,
        clock=clock,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
    )
    job = _planned_job(store, scheduler)
    clock[0] = NOW + timedelta(seconds=2)
    assert scheduler.run_once().claims_published == 1
    claim = claims.consume(claims.pending()[0])
    consumed_path = claims.ack_dir / f"{claim.claim_token}.json"
    consumed_payload = consumed_path.read_bytes()
    reports.publish(
        _report(
            claim,
            LabShardSucceeded.current(
                result_manifest_hash="a" * 64,
                worker_code_sha="1" * 40,
            ),
        )
    )
    clock[0] = NOW + timedelta(seconds=3)

    result = scheduler.run_once()

    assert result.reports_accepted == 1
    assert result.claims_revoked == 0
    assert consumed_path.read_bytes() == consumed_payload
    assert not claims.is_revoked(claim)
    completed_shards = LabJobReader(store.path).get_job(job.job_id)
    assert completed_shards is not None and completed_shards.status is JobStatus.RUNNING
    assert completed_shards.result_state.value == "ready"


def test_scheduler_report_commit_before_ack_replay_does_not_duplicate_telemetry(
    tmp_path: Path,
) -> None:
    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    reports = _CrashBeforeReportAckSpool(tmp_path / "reports")
    store, scheduler = _scheduler(
        tmp_path,
        clock=clock,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
    )
    assert scheduler.lease is not None
    job = _submit_job(store, scheduler.lease)
    store.plan_job(
        job.job_id,
        (_definition(0, with_work_plan=True),),
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=1),
    )
    clock[0] = NOW + timedelta(seconds=2)
    assert scheduler.run_once().claims_published == 1
    claim = claims.consume(claims.pending()[0])
    report = _report(claim, _success(claim, duration_ms=500))
    reports.publish(report)
    clock[0] = NOW + timedelta(seconds=3)

    with pytest.raises(RuntimeError, match="report crash after ledger commit"):
        scheduler.run_once()
    committed = LabJobReader(store.path).list_shards(job.job_id)[0]
    assert committed.duration_ms == 500
    assert committed.completion_sequence == 1
    assert len(reports.pending()) == 1

    clock[0] = NOW + timedelta(seconds=4)
    replay = scheduler.run_once()
    after_replay = LabJobReader(store.path).list_shards(job.job_id)[0]

    assert replay.reports_accepted == 1
    assert reports.pending() == ()
    assert after_replay.duration_ms == 500
    assert after_replay.completion_sequence == 1


def test_scheduler_terminal_failure_preserves_consumed_history_and_revokes_separately(
    tmp_path: Path,
) -> None:
    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    store, scheduler = _scheduler(
        tmp_path,
        clock=clock,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
    )
    assert scheduler.lease is not None
    job = _submit_job(store, scheduler.lease, max_attempts=1)
    store.plan_job(
        job.job_id,
        (_definition(0),),
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=1),
    )
    clock[0] = NOW + timedelta(seconds=2)
    assert scheduler.run_once().claims_published == 1
    claim = claims.consume(claims.pending()[0])
    consumed_path = claims.ack_dir / f"{claim.claim_token}.json"
    consumed_payload = consumed_path.read_bytes()
    reports.publish(
        _report(
            claim,
            LabShardFailed(failure_json='{"reason":"fixture failure"}'),
        )
    )
    clock[0] = NOW + timedelta(seconds=3)

    result = scheduler.run_once()

    assert result.reports_accepted == 1
    assert result.claims_revoked == 1
    assert consumed_path.read_bytes() == consumed_payload
    assert claims.is_revoked(claim)
    assert claims.revocation(claim.claim_token).path.parent == claims.archived_revoked_dir
    assert LabJobReader(store.path).get_job(job.job_id).status is JobStatus.FAILED
