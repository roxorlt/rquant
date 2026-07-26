from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Thread
from uuid import uuid4

import pandas as pd
import pytest

from rquant.lab_artifact_protocol import (
    LabAcknowledgedArtifactCommit,
    LabArtifactCommit,
    LabArtifactCommitEnvelope,
    LabArtifactCommitReceipt,
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
)
from rquant.lab_artifacts import LabJobArtifactStore, LabSealedJobArtifact
from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabAcknowledgedCommand,
    LabCommandEnvelope,
    LabCommandReceipt,
    LabCommandSpool,
    LabSpoolEntry,
    PauseJobCommand,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    COMPLETE_RESULT_CONTRACT_VERSION,
    ArtifactCommitDeadlineExpiredError,
    JobStatus,
    LabJobReader,
    LabJobRecord,
    LabJobStore,
    LabResultState,
    SchedulerLeaseFencedError,
    SchedulerLeaseUnavailableError,
)
from rquant.lab_scheduler import LabScheduler, SchedulerTickResult
from rquant.lab_shard_protocol import LabShardSucceeded, LabWorkerReport
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ResearchJobType,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)

from .test_lab_shard_control_plane import PLAN_HASH, _definition

NOW = datetime(2026, 7, 24, 1, 0, tzinfo=UTC)


def _spec(*, deadline: datetime | None = None) -> ResearchRunSpec:
    return ResearchRunSpec(
        job_type=ResearchJobType.STRATEGY_REPLAY,
        parameters=ResearchRunParameters(
            strategy_name="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 14),
        ),
        code_sha="1" * 40,
        dataset_snapshot=DatasetSnapshotIdentity(
            snapshot_id="a" * 64,
            binding_hash="b" * 64,
            audit_run_id="d" * 64,
        ),
        feature_contract=FeatureContractIdentity(
            contract_id="intraday-core",
            contract_version="v1",
            contract_hash="c" * 64,
        ),
        execution_costs=ExecutionCostSpec(
            commission_bps=Decimal("2.5"),
            stamp_duty_bps=Decimal("5"),
            transfer_fee_bps=Decimal("0.1"),
            slippage_bps=Decimal("3"),
        ),
        random_seed=20260724,
        resource_class=ResourceClass.STANDARD,
        deadline=deadline or datetime(2026, 7, 25, 2, tzinfo=UTC),
        research_status="comparable",
    )


def _envelope(*, spec: ResearchRunSpec | None = None) -> LabCommandEnvelope:
    return LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(job_id=uuid4(), spec=spec or _spec(), max_attempts=3),
    )


def _components(tmp_path: Path) -> tuple[LabJobStore, LabCommandSpool]:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    return store, LabCommandSpool(tmp_path / "commands")


def _scheduler(
    store: LabJobStore,
    spool: LabCommandSpool,
    *,
    owner: str = "scheduler-a",
    now: datetime = NOW,
    batch_size: int = 32,
) -> LabScheduler:
    return LabScheduler(
        store=store,
        spool=spool,
        owner_id=owner,
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        max_commands_per_tick=batch_size,
        clock=lambda: now,
    )


def test_run_once_consumes_submit_but_keeps_job_queued_without_adapter(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    envelope = _envelope()
    spool.publish(envelope)
    scheduler = _scheduler(store, spool)

    result = scheduler.run_once()
    job = LabJobReader(store.path).get_job(envelope.command.job_id)

    assert isinstance(result, SchedulerTickResult)
    assert result.lease_acquired is True
    assert result.processed == 1
    assert result.applied == 1
    assert result.rejected == 0
    assert result.quarantined == 0
    assert result.recovered == 0
    assert job is not None
    assert job.status is JobStatus.QUEUED
    assert spool.pending() == ()


def test_scheduler_commits_verified_complete_result_before_ack(tmp_path: Path) -> None:
    store, command_spool = _components(tmp_path)
    commit_spool = LabArtifactCommitSpool(tmp_path / "artifact-commits")
    artifact_store = LabJobArtifactStore(tmp_path / "artifacts")
    clock = [NOW]
    scheduler = LabScheduler(
        store=store,
        spool=command_spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        artifact_commit_spool=commit_spool,
        artifact_store=artifact_store,
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    assert scheduler.lease is not None
    envelope = _envelope()
    assert store.apply_command(envelope, lease=scheduler.lease, now=NOW).status == "applied"
    store.plan_job(
        envelope.command.job_id,
        (_definition(0),),
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=1),
    )
    claim = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    report = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW + timedelta(seconds=3),
        body=LabShardSucceeded(result_manifest_hash="9" * 64),
    )
    assert (
        store.apply_worker_report(
            report,
            lease=scheduler.lease,
            now=NOW + timedelta(seconds=3),
        ).status
        == "accepted"
    )
    job = LabJobReader(store.path).get_job(envelope.command.job_id)
    assert job is not None and job.result_state is LabResultState.READY
    sealed = artifact_store.seal_candidate(
        artifact_store.prepare_candidate(
            job_id=job.job_id,
            spec=job.spec,
            plan_hash=PLAN_HASH,
            adapter_id="n-shape-replay",
            adapter_version="v1",
            result_contract_version=COMPLETE_RESULT_CONTRACT_VERSION,
            metrics={"shards": 1},
            report_markdown="# Complete result\n",
            tables={"result": pd.DataFrame({"value": [1]})},
        )
    )
    commit = LabArtifactCommit(
        job_id=job.job_id,
        spec_hash=sealed.manifest.spec_hash,
        plan_hash=sealed.manifest.plan_hash,
        adapter_id=sealed.manifest.adapter_id,
        adapter_version=sealed.manifest.adapter_version,
        result_contract_version=sealed.manifest.result_contract_version,
        code_sha=sealed.manifest.code_sha,
        dataset_snapshot=sealed.manifest.dataset_snapshot,
        manifest_hash=sealed.manifest_hash,
        complete_result_hash=sealed.manifest.complete_result_hash,
        sealed_path=sealed.path,
    )
    published = commit_spool.publish(LabArtifactCommitEnvelope(request_id=uuid4(), commit=commit))
    clock[0] = NOW + timedelta(seconds=4)

    tick = scheduler.run_once()

    completed = LabJobReader(store.path).get_job(job.job_id)
    evidence = LabJobReader(store.path).get_result_artifact(job.job_id)
    assert tick.artifact_commits_processed == 1
    assert tick.artifact_commits_accepted == 1
    assert tick.artifact_commits_rejected == 0
    assert tick.artifact_commits_quarantined == 0
    assert completed is not None and completed.status is JobStatus.SUCCEEDED
    assert completed.result_state is LabResultState.SEALED
    assert evidence is not None
    assert evidence.manifest_hash == sealed.manifest_hash
    assert evidence.complete_result_hash == sealed.manifest.complete_result_hash
    assert commit_spool.pending() == ()
    assert (
        commit_spool.load_receipt(
            commit_spool.ack_dir / f"{published.envelope.request_id}.json"
        ).status
        == "accepted"
    )
    with sqlite3.connect(store.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE lab_job_result_artifact SET sealed_path = sealed_path WHERE job_id = ?",
                (str(job.job_id),),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "DELETE FROM lab_job_result_artifact WHERE job_id = ?",
                (str(job.job_id),),
            )


def _ready_artifact_commit_scenario(
    tmp_path: Path,
    *,
    scheduler_type: type[LabScheduler] = LabScheduler,
    commit_spool_type: type[LabArtifactCommitSpool] = LabArtifactCommitSpool,
    publish: bool = True,
    deadline: datetime | None = None,
) -> tuple[
    LabJobStore,
    LabScheduler,
    LabArtifactCommitSpool,
    LabJobArtifactStore,
    LabJobRecord,
    LabSealedJobArtifact,
    LabArtifactCommitEnvelope,
    list[datetime],
]:
    store, command_spool = _components(tmp_path)
    commit_spool = commit_spool_type(tmp_path / "artifact-commits")
    artifact_store = LabJobArtifactStore(tmp_path / "artifacts")
    clock = [NOW]
    scheduler = scheduler_type(
        store=store,
        spool=command_spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        artifact_commit_spool=commit_spool,
        artifact_store=artifact_store,
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    assert scheduler.lease is not None
    submit = _envelope(spec=_spec(deadline=deadline))
    assert store.apply_command(submit, lease=scheduler.lease, now=NOW).status == "applied"
    store.plan_job(
        submit.command.job_id,
        (_definition(0),),
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=1),
    )
    claim = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    success = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW + timedelta(seconds=3),
        body=LabShardSucceeded(result_manifest_hash="9" * 64),
    )
    assert (
        store.apply_worker_report(
            success,
            lease=scheduler.lease,
            now=NOW + timedelta(seconds=3),
        ).status
        == "accepted"
    )
    job = LabJobReader(store.path).get_job(submit.command.job_id)
    assert job is not None and job.result_state is LabResultState.READY
    sealed = artifact_store.seal_candidate(
        artifact_store.prepare_candidate(
            job_id=job.job_id,
            spec=job.spec,
            plan_hash=PLAN_HASH,
            adapter_id="n-shape-replay",
            adapter_version="v1",
            result_contract_version=COMPLETE_RESULT_CONTRACT_VERSION,
            metrics={"shards": 1},
            report_markdown="# Complete result\n",
            tables={"result": pd.DataFrame({"value": [1]})},
        )
    )
    envelope = LabArtifactCommitEnvelope(
        request_id=uuid4(),
        commit=LabArtifactCommit(
            job_id=job.job_id,
            spec_hash=sealed.manifest.spec_hash,
            plan_hash=sealed.manifest.plan_hash,
            adapter_id=sealed.manifest.adapter_id,
            adapter_version=sealed.manifest.adapter_version,
            result_contract_version=sealed.manifest.result_contract_version,
            code_sha=sealed.manifest.code_sha,
            dataset_snapshot=sealed.manifest.dataset_snapshot,
            manifest_hash=sealed.manifest_hash,
            complete_result_hash=sealed.manifest.complete_result_hash,
            sealed_path=sealed.path,
        ),
    )
    if publish:
        commit_spool.publish(envelope)
    clock[0] = NOW + timedelta(seconds=4)
    return store, scheduler, commit_spool, artifact_store, job, sealed, envelope, clock


class _CrashBeforeArtifactCommitScheduler(LabScheduler):
    @staticmethod
    def _after_artifact_commit_staged(
        _entry: LabArtifactCommitSpoolEntry,
        _binding: object,
    ) -> None:
        raise RuntimeError("simulated crash before SQLite commit")


def test_artifact_commit_crash_before_sqlite_commit_rolls_back_and_replays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            scheduler_type=_CrashBeforeArtifactCommitScheduler,
        )
    )

    with pytest.raises(RuntimeError, match="before SQLite commit"):
        scheduler.run_once()

    pending = LabJobReader(store.path).get_job(job.job_id)
    assert pending is not None and pending.result_state is LabResultState.READY
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert len(spool.pending()) == 1

    monkeypatch.setattr(
        scheduler,
        "_after_artifact_commit_staged",
        lambda _entry, _binding: None,
    )
    replay = scheduler.run_once()

    completed = LabJobReader(store.path).get_job(job.job_id)
    assert replay.artifact_commits_accepted == 1
    assert completed is not None and completed.result_state is LabResultState.SEALED
    assert spool.pending() == ()


class _ReplaceBoundArtifactScheduler(LabScheduler):
    @staticmethod
    def _after_artifact_commit_staged(
        entry: LabArtifactCommitSpoolEntry,
        _binding: object,
    ) -> None:
        bundle = entry.envelope.commit.sealed_path
        report = bundle / "report.md"
        displaced = bundle.parent.parent / "displaced-report.md"
        os.chmod(bundle, 0o700)
        os.rename(report, displaced)
        report.write_bytes(displaced.read_bytes())
        os.chmod(report, 0o400)
        os.chmod(bundle, 0o500)


def test_artifact_final_check_failure_rolls_back_sqlite_and_quarantines(
    tmp_path: Path,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            scheduler_type=_ReplaceBoundArtifactScheduler,
        )
    )

    tick = scheduler.run_once()

    pending = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_quarantined == 1
    assert pending is not None and pending.result_state is LabResultState.READY
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert spool.pending() == ()


class _CrashBeforeArtifactAckSpool(LabArtifactCommitSpool):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.crash = True

    def ack(
        self,
        entry: LabArtifactCommitSpoolEntry,
        receipt: LabArtifactCommitReceipt,
    ) -> LabAcknowledgedArtifactCommit:
        if self.crash:
            self.crash = False
            raise RuntimeError("simulated crash after artifact SQLite commit")
        return super().ack(entry, receipt)


def test_artifact_commit_after_sqlite_before_ack_replays_same_receipt(
    tmp_path: Path,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            commit_spool_type=_CrashBeforeArtifactAckSpool,
        )
    )

    with pytest.raises(RuntimeError, match="after artifact SQLite commit"):
        scheduler.run_once()
    committed = LabJobReader(store.path).get_job(job.job_id)
    first = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert committed is not None and committed.result_state is LabResultState.SEALED
    assert first is not None and first.receipt.status == "accepted"
    assert len(spool.pending()) == 1

    replay = scheduler.run_once()

    assert replay.artifact_commits_accepted == 1
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) == first
    assert (
        len(
            [
                event
                for event in LabJobReader(store.path).list_events(job.job_id)
                if event.event_type == "job_result_sealed"
            ]
        )
        == 1
    )
    assert spool.pending() == ()


def test_same_artifact_index_is_idempotent_across_distinct_requests(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    first_tick = scheduler.run_once()
    replay = LabArtifactCommitEnvelope(request_id=uuid4(), commit=envelope.commit)
    spool.publish(replay)

    second_tick = scheduler.run_once()

    second = LabJobReader(store.path).get_artifact_commit(replay.request_id)
    assert first_tick.artifact_commits_accepted == 1
    assert second_tick.artifact_commits_accepted == 1
    assert second is not None and second.receipt.reason == "artifact_already_committed"
    assert (
        len(
            [
                event
                for event in LabJobReader(store.path).list_events(job.job_id)
                if event.event_type == "job_result_sealed"
            ]
        )
        == 1
    )


def test_sealed_candidate_without_commit_keeps_job_ready(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )

    tick = scheduler.run_once()

    unchanged = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_processed == 0
    assert unchanged is not None and unchanged.result_state is LabResultState.READY
    assert spool.pending() == ()


def test_artifact_commit_identity_mismatch_is_rejected_without_index(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    mismatched = LabArtifactCommitEnvelope(
        request_id=envelope.request_id,
        commit=LabArtifactCommit.model_validate(
            {
                **envelope.commit.model_dump(),
                "manifest_hash": "0" * 64,
            }
        ),
    )
    spool.publish(mismatched)

    tick = scheduler.run_once()

    unchanged = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert tick.artifact_commits_rejected == 1
    assert unchanged is not None and unchanged.result_state is LabResultState.READY
    assert record is not None and record.receipt.reason == "artifact_identity_mismatch"
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert spool.pending() == ()


def test_cancel_wins_ready_artifact_commit_race_without_reviving_job(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    assert scheduler.lease is not None
    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=job.job_id,
            expected_version=job.version,
            reason="cancel before artifact commit",
        ),
    )
    assert (
        store.apply_command(
            cancel,
            lease=scheduler.lease,
            now=clock[0],
        ).status
        == "applied"
    )

    tick = scheduler.run_once()

    cancelled = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert tick.artifact_commits_rejected == 1
    assert cancelled is not None and cancelled.status is JobStatus.CANCELLED
    assert cancelled.result_state is LabResultState.PENDING
    assert record is not None and record.receipt.reason == "invalid_state:cancelled"
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert spool.pending() == ()


def test_pause_is_rejected_after_complete_result_becomes_ready(tmp_path: Path) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, _envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    assert scheduler.lease is not None
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(
            job_id=job.job_id,
            expected_version=job.version,
            reason="pause after shards completed",
        ),
    )

    receipt = store.apply_command(
        pause,
        lease=scheduler.lease,
        now=clock[0],
    )

    unchanged = LabJobReader(store.path).get_job(job.job_id)
    assert receipt.status == "rejected"
    assert receipt.reason == "invalid_result_state:ready"
    assert unchanged == job


def test_deadline_wins_ready_artifact_commit_race(tmp_path: Path) -> None:
    deadline = NOW + timedelta(seconds=4)
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, deadline=deadline)
    )

    tick = scheduler.run_once()

    expired = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert tick.deadlines_expired == 1
    assert tick.artifact_commits_rejected == 1
    assert expired is not None and expired.status is JobStatus.FAILED
    assert expired.result_state is LabResultState.PENDING
    assert record is not None and record.receipt.reason == "invalid_state:failed"
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert spool.pending() == ()


def test_deadline_crossed_during_artifact_verification_wins_commit_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deadline = NOW + timedelta(seconds=5)
    store, scheduler, spool, artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, deadline=deadline)
    )
    original_bind = artifacts.bind_verified_sealed

    @contextmanager
    def advance_clock_during_verification(
        path: Path,
        *,
        indexed_at: datetime,
    ) -> Iterator[object]:
        with original_bind(path, indexed_at=indexed_at) as binding:
            clock[0] = deadline + timedelta(seconds=1)
            yield binding

    monkeypatch.setattr(
        artifacts,
        "bind_verified_sealed",
        advance_clock_during_verification,
    )

    tick = scheduler.run_once()

    expired = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert tick.deadlines_expired == 1
    assert tick.artifact_commits_rejected == 1
    assert expired is not None and expired.status is JobStatus.FAILED
    assert expired.result_state is LabResultState.PENDING
    assert record is not None and record.receipt.reason == "invalid_state:failed"
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert spool.pending() == ()


def test_deadline_crossed_during_artifact_exit_check_rolls_back_then_rejects_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deadline = NOW + timedelta(seconds=5)
    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, deadline=deadline)
    )

    def cross_deadline_after_stage(
        _entry: LabArtifactCommitSpoolEntry,
        _binding: object,
    ) -> None:
        clock[0] = deadline + timedelta(seconds=1)

    monkeypatch.setattr(
        scheduler,
        "_after_artifact_commit_staged",
        cross_deadline_after_stage,
    )

    with pytest.raises(ArtifactCommitDeadlineExpiredError, match="deadline"):
        scheduler.run_once()

    rolled_back = LabJobReader(store.path).get_job(job.job_id)
    assert rolled_back == job
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert len(spool.pending()) == 1
    assert not any(
        event.event_type == "job_result_sealed"
        for event in LabJobReader(store.path).list_events(job.job_id)
    )

    monkeypatch.setattr(
        scheduler,
        "_after_artifact_commit_staged",
        lambda _entry, _binding: None,
    )
    replay = scheduler.run_once()

    failed = LabJobReader(store.path).get_job(job.job_id)
    receipt = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert replay.deadlines_expired == 1
    assert replay.artifact_commits_rejected == 1
    assert failed is not None and failed.status is JobStatus.FAILED
    assert failed.result_state is LabResultState.PENDING
    assert receipt is not None and receipt.receipt.status == "rejected"
    assert receipt.receipt.reason == "invalid_state:failed"
    assert spool.pending() == ()


def test_lease_expired_during_artifact_exit_check_rolls_back_for_takeover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )

    def expire_lease_after_stage(
        _entry: LabArtifactCommitSpoolEntry,
        _binding: object,
    ) -> None:
        clock[0] = NOW + timedelta(seconds=61)

    monkeypatch.setattr(
        scheduler,
        "_after_artifact_commit_staged",
        expire_lease_after_stage,
    )

    with pytest.raises(SchedulerLeaseFencedError, match="expired"):
        scheduler.run_once()

    rolled_back = LabJobReader(store.path).get_job(job.job_id)
    assert rolled_back == job
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert len(spool.pending()) == 1

    replacement = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "replacement-final-check-commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        artifact_commit_spool=spool,
        artifact_store=artifacts,
        clock=lambda: clock[0],
    )
    replay = replacement.run_once()

    committed = LabJobReader(store.path).get_job(job.job_id)
    receipt = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert replay.recovered == 1
    assert replay.artifact_commits_accepted == 1
    assert committed is not None and committed.result_state is LabResultState.SEALED
    assert receipt is not None and receipt.receipt.status == "accepted"
    assert spool.pending() == ()


def test_new_scheduler_recovers_ready_job_fence_before_commit(tmp_path: Path) -> None:
    store, scheduler, spool, artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.release()
    clock[0] = NOW + timedelta(seconds=5)
    replacement = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "replacement-commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        artifact_commit_spool=spool,
        artifact_store=artifacts,
        clock=lambda: clock[0],
    )

    tick = replacement.run_once()

    committed = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert replacement.lease is not None
    assert tick.recovered == 1
    assert tick.artifact_commits_accepted == 1
    assert committed is not None and committed.result_state is LabResultState.SEALED
    assert committed.scheduler_fencing_token == replacement.lease.fencing_token
    assert record is not None
    assert record.receipt.reason == "artifact_committed"
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is not None


def test_stale_scheduler_lease_cannot_stage_ready_artifact_commit(tmp_path: Path) -> None:
    store, scheduler, _spool, artifacts, job, sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    assert scheduler.lease is not None
    stale_lease = scheduler.lease
    scheduler.release()
    clock[0] = NOW + timedelta(seconds=5)
    store.acquire_scheduler_lease(
        owner_id="scheduler-b",
        lease_seconds=60,
        now=clock[0],
    )

    with (
        artifacts.bind_verified_sealed(sealed.path, indexed_at=clock[0]) as binding,
        pytest.raises(SchedulerLeaseFencedError, match="lease"),
    ):
        store.stage_artifact_commit(
            envelope,
            binding,
            lease=stale_lease,
            now=clock[0],
        )

    unchanged = LabJobReader(store.path).get_job(job.job_id)
    assert unchanged == job
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None


def test_replayed_request_with_changed_content_is_quarantined(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            commit_spool_type=_CrashBeforeArtifactAckSpool,
        )
    )
    with pytest.raises(RuntimeError, match="after artifact SQLite commit"):
        scheduler.run_once()
    pending = spool.pending()[0]
    changed = LabArtifactCommitEnvelope(
        request_id=envelope.request_id,
        commit=LabArtifactCommit.model_validate(
            {
                **envelope.commit.model_dump(),
                "manifest_hash": "0" * 64,
            }
        ),
    )
    pending.path.write_text(changed.model_dump_json(), encoding="utf-8")

    tick = scheduler.run_once()

    committed = LabJobReader(store.path).get_job(job.job_id)
    original = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert tick.artifact_commits_quarantined == 1
    assert committed is not None and committed.result_state is LabResultState.SEALED
    assert original is not None and original.envelope == envelope
    assert spool.pending() == ()


def test_run_once_processes_only_bounded_batch(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    for _ in range(3):
        spool.publish(_envelope())
    scheduler = _scheduler(store, spool, batch_size=2)

    result = scheduler.run_once()

    assert result.processed == 2
    assert len(spool.pending()) == 1


class _CrashBeforeAckSpool(LabCommandSpool):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.crash = True

    def ack(
        self,
        entry: LabSpoolEntry,
        receipt: LabCommandReceipt,
    ) -> LabAcknowledgedCommand:
        if self.crash:
            self.crash = False
            raise RuntimeError("simulated crash after ledger commit")
        return super().ack(entry, receipt)


def test_commit_before_ack_crash_replays_without_duplicate_effect(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = _CrashBeforeAckSpool(tmp_path / "commands")
    envelope = _envelope()
    spool.publish(envelope)
    scheduler = _scheduler(store, spool)

    with pytest.raises(RuntimeError, match="after ledger commit"):
        scheduler.run_once()
    reader = LabJobReader(store.path)
    first_events = reader.list_events(envelope.command.job_id)
    assert len(first_events) == 1
    assert len(spool.pending()) == 1

    replay = scheduler.run_once()

    assert replay.processed == 1
    assert replay.applied == 1
    assert len(reader.list_events(envelope.command.job_id)) == 1
    assert spool.pending() == ()


def test_each_command_mutation_uses_a_fresh_clock_value(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    envelopes = (_envelope(), _envelope())
    for envelope in envelopes:
        spool.publish(envelope)
    moments = iter(
        (
            NOW,
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=2),
            NOW + timedelta(seconds=3),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: next(moments),
    )

    scheduler.run_once()

    reader = LabJobReader(store.path)
    applied_at = {reader.get_command(envelope.request_id).applied_at for envelope in envelopes}
    assert applied_at == {
        NOW + timedelta(seconds=2),
        NOW + timedelta(seconds=3),
    }


class _SlowAckSpool(LabCommandSpool):
    def __init__(self, root: Path, current: list[datetime]) -> None:
        super().__init__(root)
        self.current = current

    def ack(
        self,
        entry: LabSpoolEntry,
        receipt: LabCommandReceipt,
    ) -> LabAcknowledgedCommand:
        acknowledged = super().ack(entry, receipt)
        self.current[0] += timedelta(seconds=70)
        return acknowledged


def test_slow_ack_expiry_fences_next_command_in_same_tick(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    current = [NOW]
    spool = _SlowAckSpool(tmp_path / "commands", current)
    for _ in range(2):
        spool.publish(_envelope())
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: current[0],
    )

    with pytest.raises(SchedulerLeaseFencedError):
        scheduler.run_once()

    assert len(spool.pending()) == 1


def test_bad_json_is_quarantined_and_does_not_block_valid_command(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    bad = spool.pending_dir / "00000000-0000-0000-0000-000000000000.json"
    bad.write_text("{broken", encoding="utf-8")
    valid = _envelope()
    spool.publish(valid)

    result = _scheduler(store, spool).run_once()

    assert result.processed == 1
    assert result.quarantined == 1
    assert result.applied == 1
    assert not bad.exists()
    assert len(tuple(spool.quarantine_dir.glob("*.bad"))) == 1
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None


def test_malformed_filename_is_quarantined_across_restart_without_blocking(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    bad = spool.pending_dir / "not-a-command.json"
    bad.write_text("{broken", encoding="utf-8")
    valid = _envelope()
    spool.publish(valid)
    restarted = LabCommandSpool(spool.root)

    result = _scheduler(store, restarted).run_once()

    assert result.quarantined == 1
    assert result.processed == 1
    assert result.applied == 1
    assert restarted.pending() == ()
    assert not bad.exists()
    assert len(tuple(restarted.quarantine_dir.glob("not-a-command.json*.bad"))) == 1
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None
    assert LabCommandSpool(spool.root).pending() == ()


def test_pending_symlink_is_recorded_without_touching_target_or_blocking_after_restart(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    victim = tmp_path / "external-target.json"
    victim.write_text("do-not-touch", encoding="utf-8")
    symlink = spool.pending_dir / "not-a-command.json"
    symlink.symlink_to(victim)
    valid = _envelope()
    spool.publish(valid)
    restarted = LabCommandSpool(spool.root)

    result = _scheduler(store, restarted).run_once()

    assert result.quarantined == 1
    assert result.processed == 1
    assert result.applied == 1
    assert not symlink.exists()
    assert not symlink.is_symlink()
    assert victim.read_text(encoding="utf-8") == "do-not-touch"
    artifacts = tuple(restarted.quarantine_dir.glob("not-a-command.json*.symlink.bad.json"))
    assert len(artifacts) == 1
    assert artifacts[0].is_file()
    assert not artifacts[0].is_symlink()
    metadata = json.loads(artifacts[0].read_text(encoding="utf-8"))
    assert metadata["original_name"] == "not-a-command.json"
    assert metadata["link_target"] == str(victim)
    assert "invalid_envelope" in metadata["reason"]
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None
    assert LabCommandSpool(spool.root).pending() == ()


def test_semantic_request_conflict_is_quarantined_and_does_not_block_next_command(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)
    scheduler.run_once()
    assert scheduler.lease is not None
    request_id = uuid4()
    accepted = LabCommandEnvelope(
        request_id=request_id,
        command=SubmitJobCommand(job_id=uuid4(), spec=_spec(), max_attempts=3),
    )
    store.apply_command(accepted, lease=scheduler.lease, now=NOW)
    conflict = LabCommandEnvelope(
        request_id=request_id,
        command=SubmitJobCommand(job_id=uuid4(), spec=_spec(), max_attempts=3),
    )
    valid = _envelope()
    spool.publish(conflict)
    spool.publish(valid)

    result = scheduler.run_once()

    assert result.quarantined == 1
    assert result.processed == 1
    assert result.applied == 1
    assert spool.pending() == ()
    quarantine_records = tuple(spool.quarantine_dir.glob("*.bad.json"))
    assert len(quarantine_records) == 1
    assert "request_content_conflict" in quarantine_records[0].read_text(encoding="utf-8")
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None


def test_second_scheduler_is_refused_while_first_lease_is_valid(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    first = _scheduler(store, spool, owner="scheduler-a")
    second = _scheduler(store, spool, owner="scheduler-b")
    first.run_once()

    with pytest.raises(SchedulerLeaseUnavailableError):
        second.run_once()


def test_scheduler_takeover_recovers_old_running_job_to_checkpointed(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    old = store.acquire_scheduler_lease(
        owner_id="scheduler-old",
        lease_seconds=10,
        now=NOW,
    )
    envelope = _envelope()
    store.apply_command(envelope, lease=old, now=NOW)
    store.transition_job(
        envelope.command.job_id,
        expected_version=0,
        target_status=JobStatus.RUNNING,
        lease=old,
        reason="started",
        now=NOW + timedelta(seconds=1),
    )
    takeover = _scheduler(
        store,
        spool,
        owner="scheduler-new",
        now=NOW + timedelta(seconds=11),
    )

    result = takeover.run_once()
    recovered = LabJobReader(store.path).get_job(envelope.command.job_id)

    assert result.lease_acquired is True
    assert result.recovered == 1
    assert recovered is not None
    assert recovered.status is JobStatus.CHECKPOINTED


def test_subsequent_tick_renews_existing_lease(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    current = [NOW]
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: current[0],
    )
    first = scheduler.run_once()
    current[0] = NOW + timedelta(seconds=20)

    second = scheduler.run_once()

    assert first.lease_acquired is True
    assert second.lease_acquired is False
    assert scheduler.lease is not None
    assert scheduler.lease.heartbeat_at == NOW + timedelta(seconds=20)
    assert len(LabJobReader(store.path).list_leases()) == 1


def test_tick_before_heartbeat_deadline_does_not_write_lease(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    current = [NOW]
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: current[0],
    )
    scheduler.run_once()
    current[0] = NOW + timedelta(seconds=5)

    scheduler.run_once()

    assert scheduler.lease is not None
    assert scheduler.lease.heartbeat_at == NOW
    assert LabJobReader(store.path).list_leases()[0].heartbeat_at == NOW


def test_run_forever_stops_cooperatively_and_releases_lease(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)
    calls = 0
    original = scheduler.run_once

    def one_tick() -> SchedulerTickResult:
        nonlocal calls
        calls += 1
        result = original()
        scheduler.request_stop()
        return result

    scheduler.run_once = one_tick  # type: ignore[method-assign]
    thread = Thread(target=scheduler.run_forever)
    thread.start()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert calls == 1
    assert LabJobReader(store.path).list_leases()[-1].released_at is not None


def test_run_forever_logs_only_nonzero_structured_tick_anomalies(tmp_path: Path) -> None:
    from loguru import logger

    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)
    baseline = scheduler.run_once()
    scheduler.release()
    records: list[dict[str, object]] = []
    sink = logger.add(
        lambda message: records.append(dict(message.record["extra"])),
        level="WARNING",
    )

    def anomaly_tick() -> SchedulerTickResult:
        scheduler.request_stop()
        return baseline.model_copy(
            update={
                "plans_failed": 1,
                "claim_revoke_failures": 2,
            }
        )

    scheduler.run_once = anomaly_tick  # type: ignore[method-assign]
    try:
        scheduler._log_tick_anomalies(baseline)
        scheduler.run_forever()
    finally:
        logger.remove(sink)

    scheduler_records = [record for record in records if record.get("component") == "lab_scheduler"]
    assert scheduler_records == [
        {
            "component": "lab_scheduler",
            "owner_id": "scheduler-a",
            "failure": "tick_anomalies",
            "anomaly_counts": {
                "plans_failed": 1,
                "claim_revoke_failures": 2,
            },
        }
    ]
