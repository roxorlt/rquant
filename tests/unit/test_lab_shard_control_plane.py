from __future__ import annotations

import math
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Barrier, Event
from uuid import UUID, uuid4

import pytest

import rquant.lab_jobs as lab_jobs
from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabCommandEnvelope,
    LabCommandReceipt,
    PauseJobCommand,
    RequestContentConflictError,
    ResumeJobCommand,
    RetryJobCommand,
)
from rquant.lab_jobs import (
    MAX_JOB_SHARDS,
    ControlIntent,
    InvalidJobTransitionError,
    InvalidStoredJobError,
    JobStatus,
    LabJobReader,
    LabJobStore,
    LabLeaseRecord,
    ShardPlanConflictError,
    ShardStatus,
)
from rquant.lab_result_digest import (
    LabLegacyContentDigestProvenance,
    LabResultDigestPolicy,
)
from rquant.lab_shard_protocol import (
    LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE,
    LAB_SHARD_DURATION_MS_MIN,
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
from rquant.strategy_job_adapters import StrategyShardPayload

from .test_lab_jobs import NOW, _lease, _submit, _submit_job
from .test_strategy_job_adapters import _p13_frozen_claim

PLAN_HASH = "4" * 64


def _noncanonical_uuid(value: UUID, style: str) -> str:
    canonical = str(value)
    return {
        "uppercase": canonical.upper(),
        "braces": f"{{{canonical}}}",
        "urn": f"urn:uuid:{canonical}",
        "whitespace": f" {canonical}",
    }[style]


def _register_unprivileged_job_functions(connection: sqlite3.Connection) -> None:
    connection.create_function(
        lab_jobs._ARTIFACT_SUCCESS_AUTH_FUNCTION,
        5,
        lambda *_args: 0,
    )
    connection.create_function(
        lab_jobs._RETRY_AUTH_FUNCTION,
        3,
        lambda *_args: 0,
    )
    connection.create_function(
        lab_jobs._READY_TERMINAL_AUTH_FUNCTION,
        6,
        lambda *_args: 0,
    )


def _definition(
    index: int,
    *,
    plan_hash: str = PLAN_HASH,
    with_work_plan: bool = False,
) -> LabShardDefinition:
    return LabShardDefinition.from_payload(
        shard_index=index,
        adapter_id="n-shape-replay",
        adapter_version="v1",
        plan_hash=plan_hash,
        payload_json=f'{{"hold_days":{index + 1}}}',
        work_plan=(
            LabShardWorkPlan(
                phase="strategy_replay",
                work_unit_name="parameter_case",
                work_units=index + 1,
                static_duration_ms=(index + 1) * 1_000,
            )
            if with_work_plan
            else None
        ),
    )


def _setup(
    tmp_path: Path,
    *,
    count: int = 1,
    max_attempts: int = 3,
    scheduler_lease_seconds: int = 600,
    with_work_plan: bool = False,
) -> tuple[LabJobStore, LabLeaseRecord, UUID]:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store, seconds=scheduler_lease_seconds)
    job = _submit_job(store, lease, max_attempts=max_attempts)
    planned = store.plan_job(
        job.job_id,
        tuple(_definition(index, with_work_plan=with_work_plan) for index in range(count)),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    assert len(planned) == count
    return store, lease, job.job_id


def test_plan_job_rejects_more_than_authoritative_shard_limit_before_insert(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    job = _submit_job(store, lease)
    definitions = tuple(_definition(index) for index in range(MAX_JOB_SHARDS + 1))

    with pytest.raises(ValueError, match=f"at most {MAX_JOB_SHARDS} shards"):
        store.plan_job(
            job.job_id,
            definitions,
            lease=lease,
            now=NOW + timedelta(seconds=1),
        )

    assert LabJobReader(store.path).list_shards(job.job_id) == ()


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
    if isinstance(body, LabShardSucceeded) and body.result_manifest_schema_version is None:
        body = LabShardSucceeded.current(
            result_manifest_hash=body.result_manifest_hash,
            worker_code_sha="1" * 40,
            telemetry=body.telemetry,
        )
    return LabWorkerReport.from_claim(
        claim,
        report_id=report_id or uuid4(),
        reported_at=NOW + timedelta(seconds=offset),
        body=body,
    )


def _success(claim: LabShardClaim, *, duration_ms: float) -> LabShardSucceeded:
    plan = claim.definition.work_plan
    assert plan is not None
    return LabShardSucceeded.current(
        result_manifest_hash=f"{claim.shard_index + 1:x}" * 64,
        worker_code_sha="1" * 40,
        telemetry=LabShardTelemetry(
            phase=plan.phase,
            work_unit_name=plan.work_unit_name,
            work_units=plan.work_units,
            static_duration_ms=plan.static_duration_ms,
            duration_ms=duration_ms,
            throughput_units_per_second=plan.work_units / (duration_ms / 1_000),
        ),
    )


def test_scheduler_rejects_current_job_success_without_digest_provenance(
    tmp_path: Path,
) -> None:
    store, lease, _ = _setup(tmp_path)
    claim = _claim(store, lease)
    unprovenanced = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW + timedelta(seconds=3),
        body=LabShardSucceeded(result_manifest_hash="6" * 64),
    )

    rejected = store.apply_worker_report(
        unprovenanced,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    wrong_code = store.apply_worker_report(
        _report(
            claim,
            LabShardSucceeded.current(
                result_manifest_hash="6" * 64,
                worker_code_sha="2" * 40,
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    accepted = store.apply_worker_report(
        _report(
            claim,
            LabShardSucceeded.current(
                result_manifest_hash="6" * 64,
                worker_code_sha="1" * 40,
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    assert rejected.status == "rejected"
    assert rejected.reason == "unsupported_result_digest_provenance"
    assert wrong_code.status == "rejected"
    assert wrong_code.reason == "unsupported_result_digest_provenance"
    assert accepted.status == "accepted"


def test_scheduler_accepts_unversioned_legacy_success_only_for_exact_allowlist(
    tmp_path: Path,
) -> None:
    legacy_code_sha = "53dc0afe74d5af44f1d4a4bcda149d6a5b52c854"
    base = _submit().command.spec
    legacy_spec = type(base).model_validate(
        {**base.model_dump(mode="python"), "code_sha": legacy_code_sha}
    )
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    job = _submit_job(store, lease)
    custom = _submit(job_id=uuid4(), spec=legacy_spec)
    assert store.apply_command(custom, lease=lease, now=NOW).status == "applied"
    store.plan_job(
        custom.command.job_id,
        (_definition(0),),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    claim = _claim(store, lease)
    report = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW + timedelta(seconds=3),
        body=LabShardSucceeded(result_manifest_hash="6" * 64),
    )
    wrong_policy = LabResultDigestPolicy(
        legacy_allowlist=(LabLegacyContentDigestProvenance(code_sha="0" * 40),)
    )
    exact_policy = LabResultDigestPolicy(
        legacy_allowlist=(LabLegacyContentDigestProvenance(code_sha=legacy_code_sha),)
    )

    wrong = store.apply_worker_report(
        report,
        lease=lease,
        now=NOW + timedelta(seconds=3),
        result_digest_policy=wrong_policy,
    )
    accepted = store.apply_worker_report(
        LabWorkerReport.from_claim(
            claim,
            report_id=uuid4(),
            reported_at=NOW + timedelta(seconds=3),
            body=LabShardSucceeded(result_manifest_hash="6" * 64),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
        result_digest_policy=exact_policy,
    )

    assert job.job_id != custom.command.job_id
    assert wrong.status == "rejected"
    assert wrong.reason == "unsupported_result_digest_provenance"
    assert accepted.status == "accepted"


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
        assert not (
            shard.status is ShardStatus.QUEUED and shard.attempt_count >= shard.max_attempts
        )
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
        assert active or claimable or job.result_state.value == "ready"
    else:
        assert active, f"{job.control_intent.value} must converge when no live claim remains"


def _seed_checkpointed_sibling(
    store: LabJobStore,
    lease: LabLeaseRecord,
    job_id: UUID,
    *,
    shard_index: int,
) -> None:
    with sqlite3.connect(store.path) as connection:
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
                ShardStatus.CHECKPOINTED.value,
                "checkpoint-worker",
                lease.fencing_token,
                str(uuid4()),
                NOW.isoformat(timespec="microseconds"),
                NOW.isoformat(timespec="microseconds"),
                (NOW + timedelta(seconds=30)).isoformat(timespec="microseconds"),
                '{"cursor":7}',
                str(job_id),
                shard_index,
            ),
        )


def _assert_exhausted_job_tree(
    store: LabJobStore,
    job_id: UUID,
    *,
    exhausted_index: int,
    before_versions: tuple[int, ...],
    finished_offset: int,
) -> None:
    reader = LabJobReader(store.path)
    job = reader.get_job(job_id)
    shards = reader.list_shards(job_id)
    assert job is not None and job.status is JobStatus.FAILED
    assert job.recoverable is False
    assert all(shard.status is ShardStatus.FAILED for shard in shards)
    assert tuple(shard.version for shard in shards) == tuple(
        version + 1 for version in before_versions
    )
    for shard in shards:
        expected_reason = (
            '{"reason":"attempts_exhausted"}'
            if shard.shard_index == exhausted_index
            else '{"reason":"parent_failed_attempts_exhausted"}'
        )
        assert shard.failure_json == expected_reason
        assert shard.finished_at == NOW + timedelta(seconds=finished_offset)
        assert shard.checkpoint_json is None
        assert (
            shard.worker_id,
            shard.scheduler_fencing_token,
            shard.claim_token,
            shard.claimed_at,
            shard.heartbeat_at,
            shard.lease_expires_at,
        ) == (None, None, None, None, None, None)
    assert not any(
        shard.status is ShardStatus.RUNNING
        or (shard.status is ShardStatus.QUEUED and shard.attempt_count < shard.max_attempts)
        for shard in shards
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


@pytest.mark.parametrize("uuid_style", ["uppercase", "braces", "urn", "whitespace"])
def test_claim_readers_reject_noncanonical_persisted_claim_tokens(
    tmp_path: Path,
    uuid_style: str,
) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease)
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_shard SET claim_token = ? WHERE job_id = ? AND shard_id = ?",
            (
                _noncanonical_uuid(claim.claim_token, uuid_style),
                str(job_id),
                str(claim.shard_id),
            ),
        )

    reader = LabJobReader(store.path)
    with pytest.raises(InvalidStoredJobError):
        reader.get_job(job_id)
    with pytest.raises(InvalidStoredJobError):
        reader.list_shards(job_id)
    with pytest.raises(InvalidStoredJobError):
        store.list_active_claims(
            lease,
            now=NOW + timedelta(seconds=3),
            initial_lease_seconds=30,
        )


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


def test_heartbeat_rejects_scheduler_time_before_claimed_at(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease, now_offset=10, duration=30)
    before = LabJobReader(store.path).list_shards(job_id)[0]

    receipt = store.apply_worker_report(
        _report(claim, LabShardHeartbeat(lease_extension_seconds=10), offset=11),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )

    assert receipt.status == "rejected"
    assert receipt.reason == "backdated_report"
    assert LabJobReader(store.path).list_shards(job_id)[0] == before


def test_heartbeat_rejects_scheduler_time_before_previous_heartbeat(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease, duration=300)
    first = store.apply_worker_report(
        _report(claim, LabShardHeartbeat(lease_extension_seconds=10), offset=10),
        lease=lease,
        now=NOW + timedelta(seconds=10),
    )
    assert first.status == "accepted"
    before = LabJobReader(store.path).list_shards(job_id)[0]

    receipt = store.apply_worker_report(
        _report(claim, LabShardHeartbeat(lease_extension_seconds=20), offset=12),
        lease=lease,
        now=NOW + timedelta(seconds=9),
    )

    assert receipt.status == "rejected"
    assert receipt.reason == "backdated_report"
    assert LabJobReader(store.path).list_shards(job_id)[0] == before


@pytest.mark.parametrize(
    "body",
    [
        LabShardHeartbeat(lease_extension_seconds=30),
        LabShardSucceeded(result_manifest_hash="6" * 64),
        LabShardFailed(failure_json='{"reason":"backdated"}'),
        LabWorkerStopped(reason="backdated stop"),
    ],
)
@pytest.mark.parametrize("backdated_source", ["scheduler_now", "reported_at"])
def test_all_report_types_reject_backdated_scheduler_or_reported_time(
    tmp_path: Path,
    body: LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
    backdated_source: str,
) -> None:
    store, lease, job_id = _setup(tmp_path)
    if backdated_source == "scheduler_now":
        claim = _claim(store, lease, now_offset=10, duration=300)
        report = _report(claim, body, offset=11)
        apply_at = NOW + timedelta(seconds=9)
    else:
        claim = _claim(store, lease, duration=300)
        heartbeat = store.apply_worker_report(
            _report(claim, LabShardHeartbeat(lease_extension_seconds=30), offset=10),
            lease=lease,
            now=NOW + timedelta(seconds=10),
        )
        assert heartbeat.status == "accepted"
        report = _report(claim, body, offset=9)
        apply_at = NOW + timedelta(seconds=11)
    before = LabJobReader(store.path).list_shards(job_id)[0]

    receipt = store.apply_worker_report(
        report,
        lease=lease,
        now=apply_at,
    )

    assert receipt.status == "rejected"
    assert receipt.reason == "backdated_report"
    persisted = LabJobReader(store.path).get_worker_report(report.report_id)
    assert persisted is not None and persisted.receipt == receipt
    after = LabJobReader(store.path).list_shards(job_id)[0]
    assert after == before
    assert after.finished_at is None


def test_reader_rejects_heartbeat_before_claimed_at(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path)
    _claim(store, lease, now_offset=10)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE lab_shard SET heartbeat_at = ? WHERE job_id = ?",
            (
                (NOW + timedelta(seconds=9)).isoformat(timespec="microseconds"),
                str(job_id),
            ),
        )

    with pytest.raises(InvalidStoredJobError, match="heartbeat predates claim"):
        LabJobReader(store.path).list_shards(job_id)


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
    reset_by_shard_id = {shard.shard_id: shard for shard in reset}
    for shard in reset:
        assert (
            shard.worker_id,
            shard.scheduler_fencing_token,
            shard.claim_token,
            shard.claimed_at,
            shard.heartbeat_at,
            shard.lease_expires_at,
            shard.result_manifest_hash,
            shard.failure_json,
            shard.finished_at,
            shard.checkpoint_json,
        ) == (None, None, None, None, None, None, None, None, None, None)

    fresh_claims = tuple(
        _claim(store, lease, worker=worker, now_offset=offset)
        for worker, offset in (
            ("worker-new-a", 6),
            ("worker-new-b", 7),
            ("worker-new-c", 8),
        )
    )
    assert {claim.shard_id for claim in fresh_claims} == {shard.shard_id for shard in reset}
    fresh_for_stale_shard = next(
        claim for claim in fresh_claims if claim.shard_id == stale_claim.shard_id
    )
    assert fresh_for_stale_shard.scheduler_fencing_token == stale_claim.scheduler_fencing_token
    assert fresh_for_stale_shard.claim_token != stale_claim.claim_token
    assert fresh_for_stale_shard.claim_generation == stale_claim.claim_generation + 1

    before_old_report = next(
        shard
        for shard in LabJobReader(store.path).list_shards(job_id)
        if shard.shard_id == stale_claim.shard_id
    )
    assert (
        before_old_report.attempt_count == reset_by_shard_id[stale_claim.shard_id].attempt_count + 1
    )

    stale = store.apply_worker_report(
        _report(
            stale_claim,
            LabShardSucceeded(result_manifest_hash="8" * 64),
            offset=9,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=9),
    )
    assert stale.status == "rejected"
    assert stale.reason == "stale_claim_worker"
    current = next(
        shard
        for shard in LabJobReader(store.path).list_shards(job_id)
        if shard.shard_id == stale_claim.shard_id
    )
    assert current == before_old_report
    assert current.status is ShardStatus.RUNNING
    assert current.result_manifest_hash is None


def test_retry_commit_is_atomically_visible_to_independent_reader(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path, count=3)
    failed_claim = _claim(store, lease, worker="worker-failed")
    _claim(store, lease, worker="worker-stale", now_offset=3)
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
    before_job = LabJobReader(store.path).get_job(job_id)
    before_shards = LabJobReader(store.path).list_shards(job_id)
    assert before_job is not None and before_job.status is JobStatus.FAILED

    entered_precommit = Event()
    release_precommit = Event()

    def block_precommit() -> None:
        entered_precommit.set()
        assert release_precommit.wait(timeout=5)

    retry_store = LabJobStore(store.path, mutation_guard=block_precommit)
    envelope = LabCommandEnvelope(
        request_id=uuid4(),
        command=RetryJobCommand(
            job_id=job_id,
            expected_version=before_job.version,
            reason="test retry visibility",
        ),
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            retry_store.apply_command,
            envelope,
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )
        assert entered_precommit.wait(timeout=5)
        reader = LabJobReader(store.path)
        assert reader.get_job(job_id) == before_job
        assert reader.list_shards(job_id) == before_shards
        release_precommit.set()
        assert future.result(timeout=5).status == "applied"

    after_job = LabJobReader(store.path).get_job(job_id)
    after_shards = LabJobReader(store.path).list_shards(job_id)
    assert after_job is not None and after_job.status is JobStatus.QUEUED
    assert all(shard.status is ShardStatus.QUEUED for shard in after_shards)
    assert all(shard.claim_token is None for shard in after_shards)


def test_retry_races_old_report_without_persisting_old_result(tmp_path: Path) -> None:
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

    barrier = Barrier(2)
    retry_envelope = LabCommandEnvelope(
        request_id=uuid4(),
        command=RetryJobCommand(
            job_id=job_id,
            expected_version=failed_job.version,
            reason="race old report",
        ),
    )
    stale_report = _report(
        stale_claim,
        LabShardSucceeded(result_manifest_hash="8" * 64),
        offset=5,
    )

    def retry() -> LabCommandReceipt:
        barrier.wait()
        return LabJobStore(store.path, busy_timeout_ms=5_000).apply_command(
            retry_envelope,
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )

    def report_old_claim() -> LabReportReceipt:
        barrier.wait()
        return LabJobStore(store.path, busy_timeout_ms=5_000).apply_worker_report(
            stale_report,
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        retry_future = executor.submit(retry)
        report_future = executor.submit(report_old_claim)
        retry_receipt = retry_future.result(timeout=5)
        report_receipt = report_future.result(timeout=5)

    assert retry_receipt.status == "applied"
    assert report_receipt.status == "rejected"
    assert report_receipt.reason == "stale_shard_fence"
    final_job = LabJobReader(store.path).get_job(job_id)
    final_shards = LabJobReader(store.path).list_shards(job_id)
    assert final_job is not None and final_job.status is JobStatus.QUEUED
    assert all(shard.status is ShardStatus.QUEUED for shard in final_shards)
    assert all(shard.result_manifest_hash is None for shard in final_shards)
    assert all(shard.claim_token is None for shard in final_shards)


def test_recoverable_shard_failure_terminalizes_tree_before_atomic_retry(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=4, max_attempts=3)
    failed_claim = _claim(store, lease, worker="worker-failed")
    active_claim = _claim(store, lease, worker="worker-active", now_offset=3)
    _seed_checkpointed_sibling(store, lease, job_id, shard_index=3)
    before = LabJobReader(store.path).list_shards(job_id)

    failed = store.apply_worker_report(
        _report(
            failed_claim,
            LabShardFailed(failure_json='{"kind":"recoverable"}'),
            offset=4,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )

    assert failed.status == "accepted"
    failed_job = LabJobReader(store.path).get_job(job_id)
    failed_shards = LabJobReader(store.path).list_shards(job_id)
    assert failed_job is not None and failed_job.status is JobStatus.FAILED
    assert failed_job.recoverable is True
    assert all(shard.status is ShardStatus.FAILED for shard in failed_shards)
    assert tuple(shard.version for shard in failed_shards) == tuple(
        shard.version + 1 for shard in before
    )
    for shard in failed_shards:
        assert (
            shard.worker_id,
            shard.scheduler_fencing_token,
            shard.claim_token,
            shard.claimed_at,
            shard.heartbeat_at,
            shard.lease_expires_at,
            shard.checkpoint_json,
        ) == (None, None, None, None, None, None, None)
    assert failed_shards[failed_claim.shard_index].failure_json == '{"kind":"recoverable"}'
    assert all(
        shard.failure_json == '{"reason":"parent_failed_recoverable"}'
        for shard in failed_shards
        if shard.shard_index != failed_claim.shard_index
    )
    late = store.apply_worker_report(
        _report(active_claim, LabWorkerStopped(reason="late active sibling"), offset=5),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    assert late.status == "rejected"

    retried = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=RetryJobCommand(
                job_id=job_id,
                expected_version=failed_job.version,
                reason="retry coherent tree",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )

    assert retried.status == "applied"
    retried_job = LabJobReader(store.path).get_job(job_id)
    retried_shards = LabJobReader(store.path).list_shards(job_id)
    assert retried_job is not None and retried_job.status is JobStatus.QUEUED
    assert all(shard.status is ShardStatus.QUEUED for shard in retried_shards)
    assert tuple(shard.version for shard in retried_shards) == tuple(
        shard.version + 1 for shard in failed_shards
    )
    assert all(
        shard.failure_json is None
        and shard.finished_at is None
        and shard.worker_id is None
        and shard.claim_token is None
        for shard in retried_shards
    )


def test_retry_rejects_mixed_exhausted_failed_tree_without_mutation(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=2, max_attempts=2)
    first = _claim(store, lease, worker="worker-first")
    second = _claim(store, lease, worker="worker-second", now_offset=3)
    stopped = store.apply_worker_report(
        _report(second, LabWorkerStopped(reason="retry sibling"), offset=4),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    assert stopped.status == "accepted"
    exhausted = _claim(store, lease, worker="worker-second-retry", now_offset=5)
    assert exhausted.shard_id == second.shard_id
    assert exhausted.claim_generation == second.claim_generation + 1
    assert exhausted.definition.shard_index == 1

    failed = store.apply_worker_report(
        _report(
            first,
            LabShardFailed(failure_json='{"reason":"mixed-attempt-failure"}'),
            offset=6,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )
    before_job = LabJobReader(store.path).get_job(job_id)
    before_shards = LabJobReader(store.path).list_shards(job_id)

    assert failed.status == "accepted"
    assert before_job is not None and before_job.status is JobStatus.FAILED
    assert before_job.recoverable is False
    assert tuple(shard.attempt_count for shard in before_shards) == (1, 2)
    assert all(shard.status is ShardStatus.FAILED for shard in before_shards)

    retry = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=RetryJobCommand(
                job_id=job_id,
                expected_version=before_job.version,
                reason="must reject mixed exhausted tree",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=7),
    )

    assert retry.status == "rejected"
    assert retry.reason == "not_recoverable"
    assert LabJobReader(store.path).get_job(job_id) == before_job
    assert LabJobReader(store.path).list_shards(job_id) == before_shards


def test_retry_converges_legacy_recoverable_exhausted_tree_without_restoring_shards(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=2, max_attempts=2)
    finished_at = (NOW + timedelta(seconds=2)).isoformat(timespec="microseconds")
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute(
            """
            UPDATE lab_job
            SET status = ?, recoverable = 1, version = version + 1
            WHERE job_id = ?
            """,
            (JobStatus.FAILED.value, str(job_id)),
        )
        connection.execute(
            """
            UPDATE lab_shard
            SET status = ?, version = version + 1,
                attempt_count = CASE WHEN shard_index = 0 THEN 1 ELSE 2 END,
                failure_json = '{"reason":"legacy-mixed-tree"}',
                finished_at = ?, updated_at = ?
            WHERE job_id = ?
            """,
            (ShardStatus.FAILED.value, finished_at, finished_at, str(job_id)),
        )
    before_job = LabJobReader(store.path).get_job(job_id)
    before_shards = LabJobReader(store.path).list_shards(job_id)
    assert before_job is not None and before_job.recoverable is True

    retry = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=RetryJobCommand(
                job_id=job_id,
                expected_version=before_job.version,
                reason="converge legacy mixed tree",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    after_job = LabJobReader(store.path).get_job(job_id)

    assert retry.status == "rejected"
    assert retry.reason == "shard_attempts_exhausted"
    assert after_job is not None and after_job.status is JobStatus.FAILED
    assert after_job.recoverable is False
    assert after_job.version == before_job.version + 1
    assert LabJobReader(store.path).list_shards(job_id) == before_shards


@pytest.mark.parametrize(
    "parent_status",
    [JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.CHECKPOINTED],
)
def test_recovery_fails_tree_with_exhausted_queued_sibling(
    tmp_path: Path,
    parent_status: JobStatus,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=2, max_attempts=2)
    if parent_status is JobStatus.RUNNING:
        _claim(store, lease, worker="active-worker", duration=300)
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        if parent_status is JobStatus.CHECKPOINTED:
            connection.execute(
                """
                UPDATE lab_job SET status = ?, version = version + 1
                WHERE job_id = ?
                """,
                (JobStatus.CHECKPOINTED.value, str(job_id)),
            )
        connection.execute(
            """
            UPDATE lab_shard SET attempt_count = max_attempts
            WHERE job_id = ? AND shard_index = 1 AND status = ?
            """,
            (str(job_id), ShardStatus.QUEUED.value),
        )

    recovered = store.recover_stale_shards(
        lease,
        now=NOW + timedelta(seconds=3),
    )
    job = LabJobReader(store.path).get_job(job_id)
    shards = LabJobReader(store.path).list_shards(job_id)

    assert recovered == (job_id,)
    assert job is not None and job.status is JobStatus.FAILED
    assert job.recoverable is False
    assert all(shard.status is ShardStatus.FAILED for shard in shards)
    assert all(shard.claim_token is None for shard in shards)


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
    replayed_job = LabJobReader(store.path).get_job(job_id)
    assert replayed_job is not None and replayed_job.status is JobStatus.RUNNING
    assert replayed_job.result_state.value == "ready"

    conflict = _report(
        claim,
        LabShardFailed(failure_json='{"code":"conflict"}'),
        report_id=report_id,
        offset=5,
    )
    with pytest.raises(RequestContentConflictError):
        store.apply_worker_report(conflict, lease=lease, now=NOW + timedelta(seconds=5))


@pytest.mark.parametrize("uuid_style", ["uppercase", "braces", "urn", "whitespace"])
@pytest.mark.parametrize("uuid_field", ["report_id", "job_id", "shard_id", "report_json"])
def test_report_readers_reject_noncanonical_persisted_uuid_text(
    tmp_path: Path,
    uuid_style: str,
    uuid_field: str,
) -> None:
    store, lease, _job_id = _setup(tmp_path)
    claim = _claim(store, lease)
    report = _report(
        claim,
        LabShardSucceeded(result_manifest_hash="6" * 64),
    )
    receipt = store.apply_worker_report(
        report,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert receipt.status == "accepted"
    values = {
        "report_id": report.report_id,
        "job_id": report.job_id,
        "shard_id": report.shard_id,
        "report_json": report.report_id,
    }
    malformed = _noncanonical_uuid(values[uuid_field], uuid_style)
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        if uuid_field == "report_json":
            connection.execute(
                "UPDATE lab_worker_report SET report_json = replace(report_json, ?, ?)",
                (str(report.report_id), malformed),
            )
        else:
            update_sql = {
                "report_id": "UPDATE lab_worker_report SET report_id = ?",
                "job_id": "UPDATE lab_worker_report SET job_id = ?",
                "shard_id": "UPDATE lab_worker_report SET shard_id = ?",
            }[uuid_field]
            connection.execute(
                update_sql,
                (malformed,),
            )

    if uuid_field == "report_id":
        with pytest.raises(InvalidStoredJobError):
            store.list_accepted_success_claim_tokens(
                lease,
                now=NOW + timedelta(seconds=4),
            )
    else:
        with pytest.raises(InvalidStoredJobError):
            LabJobReader(store.path).get_worker_report(report.report_id)


def test_telemetry_completion_sequence_is_acceptance_ordered_exactly_once_and_restart_safe(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=2, with_work_plan=True)
    first_claim = _claim(store, lease, worker="worker-a", now_offset=2)
    second_claim = _claim(store, lease, worker="worker-b", now_offset=3)
    second_report = _report(second_claim, _success(second_claim, duration_ms=2_000), offset=4)
    first_report = _report(first_claim, _success(first_claim, duration_ms=500), offset=5)

    second_receipt = store.apply_worker_report(
        second_report,
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    first_receipt = store.apply_worker_report(
        first_report,
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    replay = LabJobStore(store.path).apply_worker_report(
        second_report,
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )
    shards = LabJobReader(store.path).list_shards(job_id)
    job = LabJobReader(store.path).get_job(job_id)

    assert second_receipt.status == first_receipt.status == "accepted"
    assert replay == second_receipt
    assert [shard.completion_sequence for shard in shards] == [2, 1]
    assert [shard.duration_ms for shard in shards] == [500, 2_000]
    assert shards[0].throughput_units_per_second == pytest.approx(2)
    assert shards[1].throughput_units_per_second == pytest.approx(1)
    assert job is not None
    assert job.result_contract_version == "p1.4b-complete-result-v1"
    assert job.status is JobStatus.RUNNING
    assert job.result_state.value == "ready"


def test_p13_inflight_completion_fails_without_creating_legacy_result(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    frozen = _p13_frozen_claim()
    payload = StrategyShardPayload.model_validate_json(frozen.definition.payload_json)
    submitted = _submit(job_id=frozen.job_id, spec=payload.spec)
    assert store.apply_command(submitted, lease=lease, now=NOW).status == "applied"
    store.plan_job(
        frozen.job_id,
        (frozen.definition,),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute(
            "UPDATE lab_job SET result_contract_version = NULL WHERE job_id = ?",
            (str(frozen.job_id),),
        )
    claim = store.claim_next_shard(
        worker_id=frozen.worker_id,
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None and claim.definition == frozen.definition

    receipt = store.apply_worker_report(
        _report(
            claim,
            LabShardSucceeded(result_manifest_hash="a" * 64),
            offset=3,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    shard = LabJobReader(store.path).list_shards(frozen.job_id)[0]
    job = LabJobReader(store.path).get_job(frozen.job_id)

    assert receipt.status == "accepted"
    assert shard.status is ShardStatus.SUCCEEDED
    assert shard.duration_ms is None
    assert shard.throughput_units_per_second is None
    assert shard.completion_sequence is None
    assert job is not None and job.result_contract_version is None
    assert job.status is JobStatus.FAILED
    assert job.result_state.value == "pending"
    assert job.recoverable is False
    events = LabJobReader(store.path).list_events(frozen.job_id)
    assert events[-1].event_type == "job_failed_legacy_result_contract"
    assert events[-1].reason == (
        "all shards succeeded but the legacy result contract cannot produce a complete artifact"
    )


@pytest.mark.parametrize(
    ("work_units", "duration_ms", "throughput"),
    [
        (
            999_999_999,
            LAB_SHARD_DURATION_MS_MIN,
            999_999_999 / (LAB_SHARD_DURATION_MS_MIN / 1_000),
        ),
        (
            1,
            math.nextafter(LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE, 0.0),
            1 / (math.nextafter(LAB_SHARD_DURATION_MS_MAX_EXCLUSIVE, 0.0) / 1_000),
        ),
    ],
)
def test_validated_near_bound_telemetry_commits_without_sqlite_integrity_error(
    tmp_path: Path,
    work_units: int,
    duration_ms: float,
    throughput: float,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    job = _submit_job(store, lease)
    plan = LabShardWorkPlan(
        phase="strategy_replay",
        work_unit_name="parameter_case",
        work_units=work_units,
        static_duration_ms=SQLITE_SIGNED_INTEGER_MAX,
    )
    definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id="numeric-boundary",
        adapter_version="v1",
        plan_hash="9" * 64,
        payload_json="{}",
        work_plan=plan,
    )
    store.plan_job(
        job.job_id,
        (definition,),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    claim = _claim(store, lease)
    telemetry = LabShardTelemetry(
        **plan.model_dump(),
        duration_ms=duration_ms,
        throughput_units_per_second=throughput,
    )

    receipt = store.apply_worker_report(
        _report(
            claim,
            LabShardSucceeded(
                result_manifest_hash="b" * 64,
                telemetry=telemetry,
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    shard = LabJobReader(store.path).list_shards(job.job_id)[0]

    assert receipt.status == "accepted"
    assert shard.telemetry == telemetry


def test_stale_and_plan_mismatched_success_reports_cannot_write_telemetry(
    tmp_path: Path,
) -> None:
    store, old_lease, job_id = _setup(
        tmp_path,
        with_work_plan=True,
        scheduler_lease_seconds=5,
    )
    old_claim = _claim(store, old_lease, duration=3)
    new_lease = _lease(store, owner="scheduler-b", now=NOW + timedelta(seconds=8))
    fresh_claim = _claim(store, new_lease, worker="worker-b", now_offset=9)
    stale = store.apply_worker_report(
        _report(old_claim, _success(old_claim, duration_ms=123), offset=10),
        lease=new_lease,
        now=NOW + timedelta(seconds=10),
    )
    plan = fresh_claim.definition.work_plan
    assert plan is not None
    mismatched = LabShardSucceeded(
        result_manifest_hash="a" * 64,
        telemetry=LabShardTelemetry(
            phase="wrong_phase",
            work_unit_name=plan.work_unit_name,
            work_units=plan.work_units,
            static_duration_ms=plan.static_duration_ms,
            duration_ms=1_000,
            throughput_units_per_second=plan.work_units,
        ),
    )
    mismatch = store.apply_worker_report(
        _report(fresh_claim, mismatched, offset=11),
        lease=new_lease,
        now=NOW + timedelta(seconds=11),
    )
    shard = LabJobReader(store.path).list_shards(job_id)[0]

    assert stale.status == "rejected"
    assert mismatch.status == "rejected"
    assert shard.status is ShardStatus.RUNNING
    assert shard.duration_ms is None
    assert shard.throughput_units_per_second is None
    assert shard.completion_sequence is None


def test_report_insert_crash_rolls_back_telemetry_then_replay_commits_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, lease, job_id = _setup(tmp_path, with_work_plan=True)
    claim = _claim(store, lease)
    report = _report(claim, _success(claim, duration_ms=750))
    original = LabJobStore._record_worker_report

    def persist_then_crash(*args, **kwargs) -> None:
        original(*args, **kwargs)
        raise RuntimeError("simulated crash before ledger commit")

    monkeypatch.setattr(
        LabJobStore,
        "_record_worker_report",
        staticmethod(persist_then_crash),
    )
    with pytest.raises(RuntimeError, match="before ledger commit"):
        store.apply_worker_report(report, lease=lease, now=NOW + timedelta(seconds=3))

    rolled_back = LabJobReader(store.path).list_shards(job_id)[0]
    assert rolled_back.status is ShardStatus.RUNNING
    assert rolled_back.duration_ms is None
    assert rolled_back.completion_sequence is None
    assert LabJobReader(store.path).get_worker_report(report.report_id) is None

    monkeypatch.setattr(LabJobStore, "_record_worker_report", staticmethod(original))
    receipt = LabJobStore(store.path).apply_worker_report(
        report,
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    committed = LabJobReader(store.path).list_shards(job_id)[0]

    assert receipt.status == "accepted"
    assert committed.duration_ms == 750
    assert committed.completion_sequence == 1


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
    fresh_claims = tuple(
        _claim(store, lease, worker=worker, now_offset=offset)
        for worker, offset in (
            ("worker-c", 12),
            ("worker-d", 13),
            ("worker-e", 14),
        )
    )
    assert {claim.shard_id for claim in fresh_claims} == {shard.shard_id for shard in reclaimed}
    fresh_for_first = next(claim for claim in fresh_claims if claim.shard_id == first.shard_id)
    assert fresh_for_first.scheduler_fencing_token == first.scheduler_fencing_token
    assert fresh_for_first.claim_generation == first.claim_generation + 1
    assert fresh_for_first.claim_token != first.claim_token
    assert second.shard_id != fresh_for_first.shard_id
    current_for_first = next(
        shard
        for shard in LabJobReader(store.path).list_shards(job_id)
        if shard.shard_id == first.shard_id
    )
    reclaimed_first = next(shard for shard in reclaimed if shard.shard_id == first.shard_id)
    assert current_for_first.attempt_count == reclaimed_first.attempt_count + 1


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


def test_final_shard_success_marks_complete_result_ready_and_clears_pause(
    tmp_path: Path,
) -> None:
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
    assert job is not None and job.status is JobStatus.RUNNING
    assert job.result_state.value == "ready"
    assert job.result_contract_version == "p1.4b-complete-result-v1"
    assert job.control_intent is ControlIntent.NONE
    assert (
        store.claim_next_shard(
            worker_id="worker-b",
            shard_lease_seconds=30,
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )
        is None
    )
    assert LabJobReader(store.path).list_events(job_id)[-1].event_type == "job_result_ready"
    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="authorized|function|consistent"),
    ):
        connection.execute(
            """
            UPDATE lab_job SET status = 'succeeded', result_state = 'sealed'
            WHERE job_id = ?
            """,
            (str(job_id),),
        )
    unchanged = LabJobReader(store.path).get_job(job_id)
    assert unchanged is not None and unchanged.result_state.value == "ready"


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


def test_worker_stopped_exhaustion_terminalizes_every_nonterminal_sibling(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=4, max_attempts=1)
    exhausted = _claim(store, lease, worker="worker-exhausted")
    active_sibling = _claim(store, lease, worker="worker-active", now_offset=3)
    _seed_checkpointed_sibling(store, lease, job_id, shard_index=3)
    before = LabJobReader(store.path).list_shards(job_id)

    receipt = store.apply_worker_report(
        _report(exhausted, LabWorkerStopped(reason="worker lost"), offset=4),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )

    assert receipt.status == "accepted"
    assert receipt.reason == "worker_stopped_attempts_exhausted"
    _assert_exhausted_job_tree(
        store,
        job_id,
        exhausted_index=exhausted.shard_index,
        before_versions=tuple(shard.version for shard in before),
        finished_offset=4,
    )
    late = store.apply_worker_report(
        _report(active_sibling, LabWorkerStopped(reason="late sibling"), offset=5),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    assert late.status == "rejected"


def test_explicit_shard_failure_at_attempt_limit_terminalizes_job_tree(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=3, max_attempts=1)
    exhausted = _claim(store, lease, worker="worker-exhausted")
    active_sibling = _claim(store, lease, worker="worker-active", now_offset=3)
    before = LabJobReader(store.path).list_shards(job_id)

    receipt = store.apply_worker_report(
        _report(
            exhausted,
            LabShardFailed(failure_json='{"reason":"explicit"}'),
            offset=4,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )

    assert receipt.status == "accepted"
    assert receipt.reason == "shard_failed_attempts_exhausted"
    _assert_exhausted_job_tree(
        store,
        job_id,
        exhausted_index=exhausted.shard_index,
        before_versions=tuple(shard.version for shard in before),
        finished_offset=4,
    )
    failed_job = LabJobReader(store.path).get_job(job_id)
    assert failed_job is not None and failed_job.recoverable is False
    retry = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=RetryJobCommand(
                job_id=job_id,
                expected_version=failed_job.version,
                reason="must not retry exhausted explicit failure",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    assert retry.status == "rejected"
    assert retry.reason == "not_recoverable"
    late = store.apply_worker_report(
        _report(active_sibling, LabWorkerStopped(reason="late sibling"), offset=6),
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )
    assert late.status == "rejected"


def test_stale_reclaim_exhaustion_terminalizes_every_nonterminal_sibling(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=4, max_attempts=1)
    exhausted = _claim(store, lease, worker="worker-exhausted", duration=5)
    active_sibling = _claim(
        store,
        lease,
        worker="worker-active",
        now_offset=3,
        duration=30,
    )
    _seed_checkpointed_sibling(store, lease, job_id, shard_index=3)
    before = LabJobReader(store.path).list_shards(job_id)

    claim = store.claim_next_shard(
        worker_id="worker-new",
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=8),
    )

    assert claim is None
    _assert_exhausted_job_tree(
        store,
        job_id,
        exhausted_index=exhausted.shard_index,
        before_versions=tuple(shard.version for shard in before),
        finished_offset=8,
    )
    late = store.apply_worker_report(
        _report(active_sibling, LabWorkerStopped(reason="late sibling"), offset=9),
        lease=lease,
        now=NOW + timedelta(seconds=9),
    )
    assert late.status == "rejected"


def test_main_claim_cursor_is_persistent_across_store_restart(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store, seconds=600)
    old = _submit_job(store, lease)
    store.plan_job(
        old.job_id,
        tuple(_definition(index) for index in range(32)),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    new_envelope = _submit()
    assert (
        store.apply_command(
            new_envelope,
            lease=lease,
            now=NOW + timedelta(seconds=2),
        ).status
        == "applied"
    )
    new_job_id = new_envelope.command.job_id
    store.plan_job(
        new_job_id,
        (_definition(0),),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    first = _claim(store, lease, worker="worker-old", now_offset=4, duration=300)
    restarted = LabJobStore(store.path)
    restarted.initialize()
    second = _claim(
        restarted,
        lease,
        worker="worker-new",
        now_offset=5,
        duration=300,
    )

    assert first.job_id == old.job_id
    assert second.job_id == old.job_id
    assert second.shard_index == first.shard_index + 1
    assert second.job_id != new_job_id


def test_retried_large_job_preserves_main_cursor_before_fair_scan(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store, seconds=600)
    old = _submit_job(store, lease)
    store.plan_job(
        old.job_id,
        tuple(_definition(index) for index in range(32)),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    new_envelope = _submit()
    store.apply_command(new_envelope, lease=lease, now=NOW + timedelta(seconds=2))
    new_job_id = new_envelope.command.job_id
    store.plan_job(
        new_job_id,
        (_definition(0), _definition(1)),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    old_claim = _claim(store, lease, worker="worker-old", now_offset=4)
    failed = store.apply_worker_report(
        _report(
            old_claim,
            LabShardFailed(failure_json='{"reason":"retryable"}'),
            offset=5,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    assert failed.status == "accepted"
    failed_job = LabJobReader(store.path).get_job(old.job_id)
    assert failed_job is not None and failed_job.status is JobStatus.FAILED
    retried = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=RetryJobCommand(
                job_id=old.job_id,
                expected_version=failed_job.version,
                reason="retry large job",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )
    assert retried.status == "applied"

    claim_after_retry = _claim(
        LabJobStore(store.path),
        lease,
        worker="worker-after-retry",
        now_offset=7,
    )

    assert claim_after_retry.job_id == old.job_id
    assert claim_after_retry.shard_index == old_claim.shard_index + 1
    assert claim_after_retry.job_id != new_job_id


@pytest.mark.parametrize(
    "target_status",
    [JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CHECKPOINTED],
)
def test_public_transition_rejects_direct_lifecycle_change_for_sharded_job(
    tmp_path: Path,
    target_status: JobStatus,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=2)
    _claim(store, lease)
    before_job = LabJobReader(store.path).get_job(job_id)
    before_shards = LabJobReader(store.path).list_shards(job_id)
    assert before_job is not None and before_job.status is JobStatus.RUNNING

    with pytest.raises(
        InvalidJobTransitionError,
        match="sharded jobs require shard control-plane APIs",
    ):
        store.transition_job(
            job_id,
            expected_version=before_job.version,
            target_status=target_status,
            lease=lease,
            reason="forbidden direct transition",
            now=NOW + timedelta(seconds=3),
            recoverable=True if target_status is JobStatus.FAILED else None,
        )

    assert LabJobReader(store.path).get_job(job_id) == before_job
    assert LabJobReader(store.path).list_shards(job_id) == before_shards


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


def test_ready_result_can_still_be_cancelled_before_artifact_commit(tmp_path: Path) -> None:
    store, lease, job_id = _setup(tmp_path)
    claim = _claim(store, lease)
    success = store.apply_worker_report(
        _report(claim, LabShardSucceeded(result_manifest_hash="6" * 64)),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert success.status == "accepted"
    cancel = _cancel(store, lease, job_id, offset=4)
    assert cancel.status == "applied"
    assert cancel.reason == "cancelled"
    cancelled = LabJobReader(store.path).get_job(job_id)
    assert cancelled is not None and cancelled.status is JobStatus.CANCELLED
    assert cancelled.result_state.value == "pending"


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
