from __future__ import annotations

import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabCommandEnvelope,
    PauseJobCommand,
    RequestContentConflictError,
    ResumeJobCommand,
    RetryJobCommand,
)
from rquant.lab_jobs import (
    ControlIntent,
    InvalidStoredJobError,
    JobStatus,
    LabJobReader,
    LabJobStore,
    LabLeaseRecord,
    ShardPlanConflictError,
    ShardStatus,
)
from rquant.lab_shard_protocol import (
    LabShardClaim,
    LabShardDefinition,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabWorkerReport,
    LabWorkerStopped,
)

from .test_lab_jobs import NOW, _lease, _submit_job

PLAN_HASH = "4" * 64


def _definition(index: int, *, plan_hash: str = PLAN_HASH) -> LabShardDefinition:
    return LabShardDefinition.from_payload(
        shard_index=index,
        adapter_id="n-shape-replay",
        adapter_version="v1",
        plan_hash=plan_hash,
        payload_json=f'{{"hold_days":{index + 1}}}',
    )


def _setup(
    tmp_path: Path,
    *,
    count: int = 1,
    max_attempts: int = 3,
    scheduler_lease_seconds: int = 600,
) -> tuple[LabJobStore, LabLeaseRecord, UUID]:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store, seconds=scheduler_lease_seconds)
    job = _submit_job(store, lease, max_attempts=max_attempts)
    planned = store.plan_job(
        job.job_id,
        tuple(_definition(index) for index in range(count)),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    assert len(planned) == count
    return store, lease, job.job_id


def _claim(
    store: LabJobStore,
    lease: LabLeaseRecord,
    *,
    worker: str = "worker-a",
    now_offset: int = 2,
    duration: int = 30,
) -> LabShardClaim:
    claim = store.claim_next_shard(
        worker_id=worker,
        shard_lease_seconds=duration,
        lease=lease,
        now=NOW + timedelta(seconds=now_offset),
    )
    assert claim is not None
    return claim


def _report(
    claim: LabShardClaim,
    body: LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
    *,
    offset: int = 3,
    report_id: UUID | None = None,
) -> LabWorkerReport:
    return LabWorkerReport.from_claim(
        claim,
        report_id=report_id or uuid4(),
        reported_at=NOW + timedelta(seconds=offset),
        body=body,
    )


def _pause(store: LabJobStore, lease: LabLeaseRecord, job_id: UUID, *, offset: int) -> None:
    job = LabJobReader(store.path).get_job(job_id)
    assert job is not None
    receipt = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=PauseJobCommand(
                job_id=job_id,
                expected_version=job.version,
                reason="pause after current shard",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=offset),
    )
    assert receipt.status == "applied"


def _cancel(store: LabJobStore, lease: LabLeaseRecord, job_id: UUID, *, offset: int):
    job = LabJobReader(store.path).get_job(job_id)
    assert job is not None
    return store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=CancelJobCommand(
                job_id=job_id,
                expected_version=job.version,
                reason="cancel job",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=offset),
    )


def _assert_control_plane_invariants(
    store: LabJobStore,
    job_id: UUID,
    *,
    lease: LabLeaseRecord,
    now_offset: int,
) -> None:
    reader = LabJobReader(store.path)
    job = reader.get_job(job_id)
    assert job is not None
    shards = reader.list_shards(job_id)
    terminal = {
        ShardStatus.SUCCEEDED,
        ShardStatus.FAILED,
        ShardStatus.CANCELLED,
    }
    for shard in shards:
        if shard.status in terminal:
            assert (
                shard.worker_id,
                shard.scheduler_fencing_token,
                shard.claim_token,
                shard.claimed_at,
                shard.heartbeat_at,
                shard.lease_expires_at,
            ) == (None, None, None, None, None, None)

    if job.status is not JobStatus.RUNNING:
        return
    now = NOW + timedelta(seconds=now_offset)
    active = any(
        shard.status is ShardStatus.RUNNING
        and shard.scheduler_fencing_token == lease.fencing_token
        and shard.lease_expires_at is not None
        and shard.lease_expires_at > now
        for shard in shards
    )
    claimable = any(
        shard.status is ShardStatus.QUEUED and shard.attempt_count < shard.max_attempts
        for shard in shards
    )
    if job.control_intent is ControlIntent.NONE:
        assert active or claimable
    else:
        assert active, f"{job.control_intent.value} must converge when no live claim remains"


def test_plan_job_is_deterministic_idempotent_and_replan_conflicts(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    first = LabJobReader(store.path).list_shards(job_id)

    replay = store.plan_job(
        job_id,
        (_definition(0), _definition(1)),
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    assert replay == first

    with pytest.raises(ShardPlanConflictError, match="different plan"):
        store.plan_job(
            job_id,
            (_definition(0, plan_hash="5" * 64),),
            lease=lease,
            now=NOW + timedelta(seconds=3),
        )
    assert LabJobReader(store.path).list_shards(job_id) == first


def test_same_deterministic_plan_is_job_scoped_in_ledger(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    first_job = _submit_job(store, lease)
    second_job = _submit_job(store, lease)
    definitions = (_definition(0), _definition(1))

    first = store.plan_job(
        first_job.job_id,
        definitions,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    second = store.plan_job(
        second_job.job_id,
        definitions,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    assert tuple(shard.shard_id for shard in first) == tuple(shard.shard_id for shard in second)
    with sqlite3.connect(store.path) as connection:
        primary_key = tuple(
            str(row[1])
            for row in sorted(
                connection.execute("PRAGMA table_info(lab_shard)"),
                key=lambda row: int(row[5]),
            )
            if int(row[5]) > 0
        )
    assert primary_key == ("job_id", "shard_id")


def test_job_scoped_claim_mutates_only_one_matching_shard(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    jobs = (_submit_job(store, lease), _submit_job(store, lease))
    for job in jobs:
        store.plan_job(
            job.job_id,
            (_definition(0),),
            lease=lease,
            now=NOW + timedelta(seconds=1),
        )

    claim = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )

    assert claim is not None
    shards = tuple(LabJobReader(store.path).list_shards(job.job_id)[0] for job in jobs)
    assert sum(shard.status is ShardStatus.RUNNING for shard in shards) == 1
    assert sum(shard.status is ShardStatus.QUEUED for shard in shards) == 1


def test_two_workers_can_claim_only_one_shard(tmp_path: Path) -> None:
    store, lease, _job_id = _setup(tmp_path)

    def claim(worker: str) -> LabShardClaim | None:
        return LabJobStore(store.path).claim_next_shard(
            worker_id=worker,
            shard_lease_seconds=30,
            lease=lease,
            now=NOW + timedelta(seconds=2),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = tuple(executor.map(claim, ("worker-a", "worker-b")))
    claimed = [item for item in claims if item is not None]
    assert len(claimed) == 1
    assert claimed[0].claim_generation == 1


def test_cross_process_restart_claim_is_still_exactly_once(tmp_path: Path) -> None:
    store, lease, _job_id = _setup(tmp_path)
    script = """
import sys
from datetime import datetime
from pathlib import Path
from rquant.lab_jobs import LabJobStore, LabLeaseRecord
claim = LabJobStore(Path(sys.argv[1])).claim_next_shard(
    worker_id=sys.argv[2],
    shard_lease_seconds=30,
    lease=LabLeaseRecord.model_validate_json(sys.argv[3]),
    now=datetime.fromisoformat(sys.argv[4]),
)
print("NONE" if claim is None else claim.model_dump_json())
"""
    processes = tuple(
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                str(store.path),
                worker,
                lease.model_dump_json(),
                (NOW + timedelta(seconds=2)).isoformat(),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for worker in ("worker-a", "worker-b")
    )
    outputs = tuple(process.communicate(timeout=10) for process in processes)
    assert all(process.returncode == 0 for process in processes), outputs
    claims = [
        LabShardClaim.model_validate_json(stdout.strip())
        for stdout, _stderr in outputs
        if stdout.strip() != "NONE"
    ]
    assert len(claims) == 1
    assert claims[0].claim_generation == 1
    assert (
        LabJobStore(store.path).claim_next_shard(
            worker_id="worker-after-restart",
            shard_lease_seconds=30,
            lease=lease,
            now=NOW + timedelta(seconds=3),
        )
        is None
    )


def test_one_worker_cannot_receive_second_live_claim(tmp_path: Path) -> None:
    store, lease, _job_id = _setup(tmp_path, count=2)
    first = _claim(store, lease, worker="worker-a")

    second = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    assert first.worker_id == "worker-a"
    assert second is None


def test_heartbeat_only_extends_current_token(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease, duration=10)
    accepted = store.apply_worker_report(
        _report(claim, LabShardHeartbeat(lease_extension_seconds=30)),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert accepted.status == "accepted"
    assert LabJobReader(store.path).list_shards(job_id)[0].lease_expires_at == NOW + timedelta(
        seconds=33
    )

    stale_claim = LabShardClaim.model_validate(
        {**claim.model_dump(mode="json"), "claim_token": str(uuid4())}
    )
    rejected = store.apply_worker_report(
        _report(stale_claim, LabShardHeartbeat(lease_extension_seconds=60), offset=4),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    assert rejected.status == "rejected"
    assert "claim" in rejected.reason
    assert LabJobReader(store.path).list_shards(job_id)[0].lease_expires_at == NOW + timedelta(
        seconds=33
    )


def test_heartbeat_never_shortens_an_existing_future_lease(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease, duration=300)

    receipt = store.apply_worker_report(
        _report(claim, LabShardHeartbeat(lease_extension_seconds=10), offset=3),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    assert receipt.status == "accepted"
    shard = LabJobReader(store.path).list_shards(job_id)[0]
    assert shard.heartbeat_at == NOW + timedelta(seconds=3)
    assert shard.lease_expires_at == claim.lease_expires_at
    _assert_control_plane_invariants(store, job_id, lease=lease, now_offset=3)


@pytest.mark.parametrize(
    "body",
    [
        LabShardHeartbeat(lease_extension_seconds=30),
        LabShardSucceeded(result_manifest_hash="6" * 64),
        LabShardFailed(failure_json='{"code":"late"}'),
    ],
)
def test_expired_claim_reclaim_rejects_all_old_report_types(
    tmp_path: Path,
    body: LabShardHeartbeat | LabShardSucceeded | LabShardFailed,
) -> None:
    store, lease, job_id = _setup(tmp_path)
    old = _claim(store, lease, duration=5)
    fresh = _claim(store, lease, worker="worker-b", now_offset=8, duration=30)
    assert fresh.shard_id == old.shard_id
    assert fresh.claim_token != old.claim_token
    assert fresh.claim_generation == 2

    rejected = store.apply_worker_report(
        _report(old, body, offset=9),
        lease=lease,
        now=NOW + timedelta(seconds=9),
    )
    assert rejected.status == "rejected"
    shard = LabJobReader(store.path).list_shards(job_id)[0]
    assert shard.status is ShardStatus.RUNNING
    assert shard.worker_id == "worker-b"
    assert shard.claim_generation == 2


def test_scheduler_takeover_fences_old_report_and_reclaims_shard(tmp_path: Path) -> None:
    store, old_lease, job_id = _setup(tmp_path, scheduler_lease_seconds=10)
    old = _claim(store, old_lease, duration=5)
    new_lease = store.acquire_scheduler_lease(
        owner_id="scheduler-b",
        lease_seconds=60,
        now=NOW + timedelta(seconds=11),
    )
    fresh = _claim(store, new_lease, worker="worker-b", now_offset=12)
    assert fresh.scheduler_fencing_token > old.scheduler_fencing_token
    assert fresh.claim_generation == 2

    rejected = store.apply_worker_report(
        _report(old, LabShardSucceeded(result_manifest_hash="6" * 64), offset=13),
        lease=new_lease,
        now=NOW + timedelta(seconds=13),
    )
    assert rejected.status == "rejected"
    assert "fenc" in rejected.reason
    assert LabJobReader(store.path).list_shards(job_id)[0].claim_token == fresh.claim_token


def test_retry_atomically_fences_old_nonterminal_claims(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=3)
    failed_claim = _claim(store, lease, worker="worker-failed")
    stale_claim = _claim(store, lease, worker="worker-stale", now_offset=3)
    failed = store.apply_worker_report(
        _report(
            failed_claim,
            LabShardFailed(failure_json='{"kind":"source"}'),
            offset=4,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    assert failed.status == "accepted"
    failed_job = LabJobReader(store.path).get_job(job_id)
    assert failed_job is not None and failed_job.status is JobStatus.FAILED

    retry = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=RetryJobCommand(
                job_id=job_id,
                expected_version=failed_job.version,
                reason="source recovered",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    assert retry.status == "applied"
    reset = LabJobReader(store.path).list_shards(job_id)
    assert all(shard.status is ShardStatus.QUEUED for shard in reset)
    assert all(shard.claim_token is None for shard in reset)

    _claim(store, lease, worker="worker-new-a", now_offset=6)
    fresh_for_same_shard = _claim(store, lease, worker="worker-new-b", now_offset=7)
    assert fresh_for_same_shard.shard_id == stale_claim.shard_id
    assert fresh_for_same_shard.claim_generation > stale_claim.claim_generation

    stale = store.apply_worker_report(
        _report(
            stale_claim,
            LabShardSucceeded(result_manifest_hash="8" * 64),
            offset=8,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=8),
    )
    assert stale.status == "rejected"
    assert stale.reason in {
        "stale_claim_worker",
        "stale_claim_token",
        "stale_claim_generation",
    }
    current = LabJobReader(store.path).list_shards(job_id)[stale_claim.shard_index]
    assert current.status is ShardStatus.RUNNING
    assert current.result_manifest_hash is None


def test_report_commit_replay_is_exactly_once_and_conflict_is_rejected(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease)
    report_id = uuid4()
    report = _report(
        claim,
        LabShardSucceeded(result_manifest_hash="6" * 64),
        report_id=report_id,
    )
    first = store.apply_worker_report(report, lease=lease, now=NOW + timedelta(seconds=3))
    replay = store.apply_worker_report(report, lease=lease, now=NOW + timedelta(seconds=4))
    assert replay == first
    assert LabJobReader(store.path).get_worker_report(report_id).receipt == first
    assert LabJobReader(store.path).get_job(job_id).status is JobStatus.SUCCEEDED

    conflict = _report(
        claim,
        LabShardFailed(failure_json='{"code":"conflict"}'),
        report_id=report_id,
        offset=5,
    )
    with pytest.raises(RequestContentConflictError):
        store.apply_worker_report(conflict, lease=lease, now=NOW + timedelta(seconds=5))


def test_pause_during_shard_checkpoints_then_resume_claims_next(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    first = _claim(store, lease)
    _pause(store, lease, job_id, offset=3)

    receipt = store.apply_worker_report(
        _report(first, LabShardSucceeded(result_manifest_hash="6" * 64), offset=4),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    assert receipt.status == "accepted"
    job = LabJobReader(store.path).get_job(job_id)
    assert job is not None and job.status is JobStatus.CHECKPOINTED
    assert (
        store.claim_next_shard(
            worker_id="worker-b",
            shard_lease_seconds=30,
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )
        is None
    )

    resume = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=ResumeJobCommand(
                job_id=job_id,
                expected_version=job.version,
                reason="resume remaining shard",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )
    assert resume.status == "applied"
    second = _claim(store, lease, worker="worker-b", now_offset=7)
    assert second.shard_index == 1


def test_pause_at_idle_shard_boundary_checkpoints_immediately(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    first = _claim(store, lease)
    success = store.apply_worker_report(
        _report(first, LabShardSucceeded(result_manifest_hash="6" * 64), offset=3),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert success.status == "accepted"
    boundary = LabJobReader(store.path).get_job(job_id)
    assert boundary is not None and boundary.status is JobStatus.RUNNING
    assert all(
        shard.status is not ShardStatus.RUNNING
        for shard in LabJobReader(store.path).list_shards(job_id)
    )

    _pause(store, lease, job_id, offset=4)

    checkpointed = LabJobReader(store.path).get_job(job_id)
    assert checkpointed is not None
    assert checkpointed.status is JobStatus.CHECKPOINTED
    assert checkpointed.control_intent is ControlIntent.NONE
    assert (
        store.claim_next_shard(
            worker_id="worker-b",
            shard_lease_seconds=30,
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )
        is None
    )
    resume = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=ResumeJobCommand(
                job_id=job_id,
                expected_version=checkpointed.version,
                reason="resume after boundary pause",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )
    assert resume.status == "applied"
    assert _claim(store, lease, worker="worker-b", now_offset=7).shard_index == 1


def test_pause_checkpoints_when_all_active_claims_expire_and_requeue(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=3)
    first = _claim(store, lease, worker="worker-a", duration=5)
    second = _claim(
        store,
        lease,
        worker="worker-b",
        now_offset=3,
        duration=5,
    )
    _pause(store, lease, job_id, offset=4)
    before = LabJobReader(store.path).list_shards(job_id)

    assert (
        store.claim_next_shard(
            worker_id="worker-c",
            shard_lease_seconds=30,
            lease=lease,
            now=NOW + timedelta(seconds=9),
        )
        is None
    )

    checkpointed = LabJobReader(store.path).get_job(job_id)
    reclaimed = LabJobReader(store.path).list_shards(job_id)
    assert checkpointed is not None
    assert checkpointed.status is JobStatus.CHECKPOINTED
    assert checkpointed.control_intent is ControlIntent.NONE
    assert [shard.status for shard in reclaimed] == [
        ShardStatus.QUEUED,
        ShardStatus.QUEUED,
        ShardStatus.QUEUED,
    ]
    assert [shard.version for shard in reclaimed] == [
        before[0].version + 1,
        before[1].version + 1,
        before[2].version,
    ]
    for shard in reclaimed:
        assert shard.worker_id is None
        assert shard.scheduler_fencing_token is None
        assert shard.claim_token is None
        assert shard.claimed_at is None
        assert shard.heartbeat_at is None
        assert shard.lease_expires_at is None

    stale = store.apply_worker_report(
        _report(first, LabShardSucceeded(result_manifest_hash="9" * 64), offset=10),
        lease=lease,
        now=NOW + timedelta(seconds=10),
    )
    assert stale.status == "rejected"
    assert LabJobReader(store.path).list_shards(job_id) == reclaimed

    resume = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=ResumeJobCommand(
                job_id=job_id,
                expected_version=checkpointed.version,
                reason="resume after expired workers",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=11),
    )
    assert resume.status == "applied"
    fresh = _claim(store, lease, worker="worker-c", now_offset=12)
    assert fresh.shard_id == first.shard_id
    assert fresh.claim_generation == first.claim_generation + 1
    assert fresh.claim_token != first.claim_token
    assert second.shard_id != fresh.shard_id


def test_pause_waits_for_every_already_running_shard_before_checkpoint(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=3)
    first = _claim(store, lease, worker="worker-a")
    second = _claim(store, lease, worker="worker-b", now_offset=3)
    _pause(store, lease, job_id, offset=4)

    first_receipt = store.apply_worker_report(
        _report(first, LabShardSucceeded(result_manifest_hash="6" * 64), offset=5),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    mid_job = LabJobReader(store.path).get_job(job_id)
    assert first_receipt.status == "accepted"
    assert mid_job is not None and mid_job.status is JobStatus.RUNNING
    assert mid_job.control_intent is ControlIntent.PAUSE_REQUESTED
    assert (
        store.claim_next_shard(
            worker_id="worker-c",
            shard_lease_seconds=30,
            lease=lease,
            now=NOW + timedelta(seconds=6),
        )
        is None
    )

    second_receipt = store.apply_worker_report(
        _report(second, LabShardSucceeded(result_manifest_hash="7" * 64), offset=7),
        lease=lease,
        now=NOW + timedelta(seconds=7),
    )
    paused = LabJobReader(store.path).get_job(job_id)
    assert second_receipt.status == "accepted"
    assert paused is not None and paused.status is JobStatus.CHECKPOINTED


def test_final_shard_success_wins_even_when_pause_was_requested(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease)
    _pause(store, lease, job_id, offset=3)
    receipt = store.apply_worker_report(
        _report(claim, LabShardSucceeded(result_manifest_hash="6" * 64), offset=4),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    job = LabJobReader(store.path).get_job(job_id)
    assert receipt.status == "accepted"
    assert job is not None and job.status is JobStatus.SUCCEEDED
    assert job.control_intent is ControlIntent.NONE


def test_cancel_at_idle_shard_boundary_terminalizes_immediately(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    first = _claim(store, lease)
    success = store.apply_worker_report(
        _report(first, LabShardSucceeded(result_manifest_hash="6" * 64), offset=3),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert success.status == "accepted"
    boundary = LabJobReader(store.path).get_job(job_id)
    assert boundary is not None and boundary.status is JobStatus.RUNNING

    receipt = _cancel(store, lease, job_id, offset=4)

    assert receipt.status == "applied"
    assert receipt.reason == "cancelled"
    job = LabJobReader(store.path).get_job(job_id)
    shards = LabJobReader(store.path).list_shards(job_id)
    assert job is not None and job.status is JobStatus.CANCELLED
    assert [shard.status for shard in shards] == [
        ShardStatus.SUCCEEDED,
        ShardStatus.CANCELLED,
    ]
    _assert_control_plane_invariants(store, job_id, lease=lease, now_offset=4)


def test_claim_tick_does_not_confirm_unsharded_legacy_cancel(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    queued = _submit_job(store, lease)
    running = store.transition_job(
        queued.job_id,
        expected_version=queued.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="legacy worker started",
        now=NOW + timedelta(seconds=1),
    )
    cancel = _cancel(store, lease, running.job_id, offset=2)
    assert cancel.reason == "cancel_requested"

    claim = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    assert claim is None
    requested = LabJobReader(store.path).get_job(running.job_id)
    assert requested is not None and requested.status is JobStatus.RUNNING
    assert requested.control_intent is ControlIntent.CANCEL_REQUESTED


def test_cancel_requested_job_converges_when_active_claim_expires(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    claim = _claim(store, lease, duration=5)
    cancel = _cancel(store, lease, job_id, offset=3)
    assert cancel.status == "applied" and cancel.reason == "cancel_requested"

    fresh = store.claim_next_shard(
        worker_id="worker-b",
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=8),
    )

    assert fresh is None
    job = LabJobReader(store.path).get_job(job_id)
    assert job is not None and job.status is JobStatus.CANCELLED
    assert all(
        shard.status is ShardStatus.CANCELLED
        for shard in LabJobReader(store.path).list_shards(job_id)
    )
    _assert_control_plane_invariants(store, job_id, lease=lease, now_offset=8)
    late = store.apply_worker_report(
        _report(claim, LabWorkerStopped(reason="late stop"), offset=9),
        lease=lease,
        now=NOW + timedelta(seconds=9),
    )
    assert late.status == "rejected"


def test_cancel_requested_job_converges_during_scheduler_takeover(tmp_path: Path) -> None:
    store, old_lease, job_id = _setup(
        tmp_path,
        count=2,
        scheduler_lease_seconds=10,
    )
    old_claim = _claim(store, old_lease, duration=30)
    cancel = _cancel(store, old_lease, job_id, offset=3)
    assert cancel.status == "applied"
    new_lease = store.acquire_scheduler_lease(
        owner_id="scheduler-b",
        lease_seconds=60,
        now=NOW + timedelta(seconds=11),
    )

    fresh = store.claim_next_shard(
        worker_id="worker-b",
        shard_lease_seconds=30,
        lease=new_lease,
        now=NOW + timedelta(seconds=12),
    )

    assert fresh is None
    job = LabJobReader(store.path).get_job(job_id)
    assert job is not None and job.status is JobStatus.CANCELLED
    _assert_control_plane_invariants(store, job_id, lease=new_lease, now_offset=12)
    late = store.apply_worker_report(
        _report(old_claim, LabWorkerStopped(reason="old scheduler"), offset=13),
        lease=new_lease,
        now=NOW + timedelta(seconds=13),
    )
    assert late.status == "rejected"


def test_stale_reclaim_exhaustion_fails_shard_and_parent_job(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, max_attempts=1)
    _claim(store, lease, duration=5)

    claim = store.claim_next_shard(
        worker_id="worker-b",
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=8),
    )

    assert claim is None
    job = LabJobReader(store.path).get_job(job_id)
    shard = LabJobReader(store.path).list_shards(job_id)[0]
    assert job is not None and job.status is JobStatus.FAILED
    assert job.recoverable is False
    assert shard.status is ShardStatus.FAILED
    assert shard.failure_json == '{"reason":"attempts_exhausted"}'
    assert shard.finished_at == NOW + timedelta(seconds=8)
    _assert_control_plane_invariants(store, job_id, lease=lease, now_offset=8)


def test_worker_stopped_at_attempt_limit_fails_shard_and_parent_job(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, max_attempts=1)
    claim = _claim(store, lease)

    receipt = store.apply_worker_report(
        _report(claim, LabWorkerStopped(reason="worker shutting down"), offset=3),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    assert receipt.status == "accepted"
    job = LabJobReader(store.path).get_job(job_id)
    shard = LabJobReader(store.path).list_shards(job_id)[0]
    assert job is not None and job.status is JobStatus.FAILED
    assert job.recoverable is False
    assert shard.status is ShardStatus.FAILED
    assert shard.failure_json == '{"reason":"attempts_exhausted"}'
    _assert_control_plane_invariants(store, job_id, lease=lease, now_offset=3)


def test_cancel_first_rejects_success_then_stopped_confirms_cancel(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease)
    cancel = _cancel(store, lease, job_id, offset=3)
    assert cancel.status == "applied" and cancel.reason == "cancel_requested"

    late_success = store.apply_worker_report(
        _report(claim, LabShardSucceeded(result_manifest_hash="6" * 64), offset=4),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    assert late_success.status == "rejected"
    stopped = store.apply_worker_report(
        _report(claim, LabWorkerStopped(reason="cancel observed"), offset=5),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    assert stopped.status == "accepted"
    assert LabJobReader(store.path).get_job(job_id).status is JobStatus.CANCELLED


def test_worker_stopped_cancel_versions_and_clears_all_remaining_shards(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=3)
    claim = _claim(store, lease)
    with sqlite3.connect(store.path) as connection:
        for shard_index, status in (
            (1, ShardStatus.QUEUED),
            (2, ShardStatus.CHECKPOINTED),
        ):
            connection.execute(
                """
                UPDATE lab_shard
                SET status = ?, worker_id = ?, scheduler_fencing_token = ?,
                    claim_token = ?, claim_generation = 1,
                    claimed_at = ?, heartbeat_at = ?, lease_expires_at = ?,
                    checkpoint_json = ?
                WHERE job_id = ? AND shard_index = ?
                """,
                (
                    status.value,
                    f"stale-worker-{shard_index}",
                    lease.fencing_token,
                    str(uuid4()),
                    NOW.isoformat(timespec="microseconds"),
                    NOW.isoformat(timespec="microseconds"),
                    (NOW + timedelta(seconds=30)).isoformat(timespec="microseconds"),
                    '{"cursor":1}',
                    str(job_id),
                    shard_index,
                ),
            )
    before = LabJobReader(store.path).list_shards(job_id)
    cancel = _cancel(store, lease, job_id, offset=3)
    assert cancel.status == "applied"

    stopped = store.apply_worker_report(
        _report(claim, LabWorkerStopped(reason="cancel observed"), offset=4),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )

    assert stopped.status == "accepted"
    shards = LabJobReader(store.path).list_shards(job_id)
    assert all(shard.status is ShardStatus.CANCELLED for shard in shards)
    assert [shard.version for shard in shards] == [shard.version + 1 for shard in before]
    for shard in shards:
        assert shard.worker_id is None
        assert shard.scheduler_fencing_token is None
        assert shard.claim_token is None
        assert shard.claimed_at is None
        assert shard.heartbeat_at is None
        assert shard.lease_expires_at is None
        assert shard.checkpoint_json is None
        assert shard.finished_at == NOW + timedelta(seconds=4)


def test_queued_cancel_atomically_terminalizes_nonterminal_shards(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    before = LabJobReader(store.path).list_shards(job_id)

    receipt = _cancel(store, lease, job_id, offset=2)

    assert receipt.status == "applied"
    job = LabJobReader(store.path).get_job(job_id)
    shards = LabJobReader(store.path).list_shards(job_id)
    assert job is not None and job.status is JobStatus.CANCELLED
    assert all(shard.status is ShardStatus.CANCELLED for shard in shards)
    assert [shard.version for shard in shards] == [shard.version + 1 for shard in before]
    assert all(shard.finished_at == NOW + timedelta(seconds=2) for shard in shards)


def test_checkpointed_cancel_preserves_success_and_terminalizes_remaining_shards(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    claim = _claim(store, lease)
    _pause(store, lease, job_id, offset=3)
    success = store.apply_worker_report(
        _report(claim, LabShardSucceeded(result_manifest_hash="6" * 64), offset=4),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    assert success.status == "accepted"
    before = LabJobReader(store.path).list_shards(job_id)
    assert LabJobReader(store.path).get_job(job_id).status is JobStatus.CHECKPOINTED

    receipt = _cancel(store, lease, job_id, offset=5)

    assert receipt.status == "applied"
    shards = LabJobReader(store.path).list_shards(job_id)
    assert shards[0] == before[0]
    assert shards[1].status is ShardStatus.CANCELLED
    assert shards[1].version == before[1].version + 1
    assert shards[1].finished_at == NOW + timedelta(seconds=5)
    assert LabJobReader(store.path).get_job(job_id).status is JobStatus.CANCELLED


def test_explicit_cancel_confirmation_atomically_invalidates_running_claim(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    _claim(store, lease)
    cancel = _cancel(store, lease, job_id, offset=3)
    assert cancel.job_version is not None

    cancelled = store.confirm_cancelled_job(
        job_id,
        expected_version=cancel.job_version,
        lease=lease,
        reason="worker supervisor confirmed stop",
        now=NOW + timedelta(seconds=4),
    )

    assert cancelled.status is JobStatus.CANCELLED
    shards = LabJobReader(store.path).list_shards(job_id)
    assert {shard.status for shard in shards} == {ShardStatus.CANCELLED}
    assert all(shard.finished_at == NOW + timedelta(seconds=4) for shard in shards)


def test_explicit_cancel_confirmation_clears_full_claim_identity_and_versions(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    _claim(store, lease)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            """
            UPDATE lab_shard
            SET worker_id = ?, scheduler_fencing_token = ?, claim_token = ?,
                claimed_at = ?, heartbeat_at = ?, lease_expires_at = ?,
                checkpoint_json = ?
            WHERE job_id = ? AND shard_index = 1
            """,
            (
                "stale-worker",
                lease.fencing_token,
                str(uuid4()),
                NOW.isoformat(timespec="microseconds"),
                NOW.isoformat(timespec="microseconds"),
                (NOW + timedelta(seconds=30)).isoformat(timespec="microseconds"),
                '{"cursor":1}',
                str(job_id),
            ),
        )
    before = LabJobReader(store.path).list_shards(job_id)
    cancel = _cancel(store, lease, job_id, offset=3)
    assert cancel.job_version is not None

    store.confirm_cancelled_job(
        job_id,
        expected_version=cancel.job_version,
        lease=lease,
        reason="worker supervisor confirmed stop",
        now=NOW + timedelta(seconds=4),
    )

    shards = LabJobReader(store.path).list_shards(job_id)
    assert [shard.version for shard in shards] == [shard.version + 1 for shard in before]
    assert all(shard.checkpoint_json is None for shard in shards)
    _assert_control_plane_invariants(store, job_id, lease=lease, now_offset=4)


@pytest.mark.parametrize(
    ("body", "status"),
    [
        (LabShardSucceeded(result_manifest_hash="6" * 64), ShardStatus.SUCCEEDED),
        (LabShardFailed(failure_json='{"reason":"worker"}'), ShardStatus.FAILED),
    ],
)
def test_terminal_worker_reports_clear_complete_claim_identity(
    tmp_path: Path,
    body: LabShardSucceeded | LabShardFailed,
    status: ShardStatus,
) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease)

    receipt = store.apply_worker_report(
        _report(claim, body, offset=3),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    assert receipt.status == "accepted"
    assert LabJobReader(store.path).list_shards(job_id)[0].status is status
    _assert_control_plane_invariants(store, job_id, lease=lease, now_offset=3)


def test_running_job_keeps_an_active_or_claimable_progress_path(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=2, max_attempts=2)
    claim = _claim(store, lease)
    stopped = store.apply_worker_report(
        _report(claim, LabWorkerStopped(reason="cooperative restart"), offset=3),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert stopped.status == "accepted"
    _assert_control_plane_invariants(store, job_id, lease=lease, now_offset=3)

    _claim(store, lease, worker="worker-b", now_offset=4)
    _assert_control_plane_invariants(store, job_id, lease=lease, now_offset=4)


def test_success_first_makes_later_cancel_a_terminal_rejection(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease)
    success = store.apply_worker_report(
        _report(claim, LabShardSucceeded(result_manifest_hash="6" * 64)),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert success.status == "accepted"
    cancel = _cancel(store, lease, job_id, offset=4)
    assert cancel.status == "rejected"
    assert cancel.reason == "invalid_state:succeeded"


def test_reader_fails_closed_on_worker_report_tamper(tmp_path: Path) -> None:
    store, lease, _job_id = _setup(tmp_path)
    claim = _claim(store, lease)
    report = _report(claim, LabShardHeartbeat(lease_extension_seconds=30))
    store.apply_worker_report(report, lease=lease, now=NOW + timedelta(seconds=3))

    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE lab_worker_report SET content_hash = ? WHERE report_id = ?",
            ("f" * 64, str(report.report_id)),
        )
    with pytest.raises(InvalidStoredJobError, match="worker report"):
        LabJobReader(store.path).get_worker_report(report.report_id)


def test_reader_fails_closed_on_shard_payload_identity_tamper(tmp_path: Path) -> None:
    store, _lease_record, job_id = _setup(tmp_path)
    shard = LabJobReader(store.path).list_shards(job_id)[0]
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE lab_shard SET payload_json = ? WHERE shard_id = ?",
            ('{"hold_days":999}', str(shard.shard_id)),
        )

    with pytest.raises(InvalidStoredJobError, match="lab shard"):
        LabJobReader(store.path).list_shards(job_id)
