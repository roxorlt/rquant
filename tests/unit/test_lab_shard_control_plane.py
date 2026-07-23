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
    scheduler_lease_seconds: int = 600,
) -> tuple[LabJobStore, LabLeaseRecord, UUID]:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store, seconds=scheduler_lease_seconds)
    job = _submit_job(store, lease)
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
