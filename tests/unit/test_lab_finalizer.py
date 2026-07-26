from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pandas as pd
import pytest

from rquant.lab_artifact_protocol import (
    LabAcknowledgedArtifactCommit,
    LabArtifactCommitReceipt,
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
)
from rquant.lab_artifacts import LabArtifactError, LabJobArtifactStore
from rquant.lab_finalizer import (
    LabFinalizationIntegrityError,
    LabFinalizer,
    LabFinalizerResult,
    LabSealedShardBundleReader,
)
from rquant.lab_job_protocol import (
    LabCommandEnvelope,
    LabCommandSpool,
    PauseJobCommand,
    ResumeJobCommand,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    ControlIntent,
    InvalidStoredJobError,
    JobStatus,
    LabFinalizationShardEvidence,
    LabFinalizationSnapshot,
    LabJobReader,
    LabJobStore,
    LabResultState,
    LabWorkerReportRecord,
)
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabReportReceipt,
    LabReportSpool,
    LabShardSucceeded,
    LabWorkerReport,
)
from rquant.lab_worker import LabShardResultManifest, canonical_shard_frame_json
from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

from .test_lab_jobs import _create_v4_job_fixture
from .test_lab_worker import NOW, RecordingRegistry, _nshape_compare_spec, _worker


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


class _Scenario:
    def __init__(
        self,
        *,
        root: Path,
        store: LabJobStore,
        scheduler: LabScheduler,
        job_id: UUID,
        artifact_store: LabJobArtifactStore,
        commit_spool: LabArtifactCommitSpool,
    ) -> None:
        self.root = root
        self.store = store
        self.scheduler = scheduler
        self.job_id = job_id
        self.artifact_store = artifact_store
        self.commit_spool = commit_spool

    def finalizer(self) -> LabFinalizer:
        return LabFinalizer(
            reader=LabJobReader(self.store.path),
            shard_artifact_root=self.root / "artifacts",
            artifact_store=self.artifact_store,
            commit_spool=self.commit_spool,
            adapter_registry=default_strategy_job_adapter_registry(),
        )


class _StaticSnapshotReader:
    def __init__(self, snapshot: LabFinalizationSnapshot) -> None:
        self.snapshot = snapshot

    def get_finalization_snapshot(self, job_id: UUID) -> LabFinalizationSnapshot | None:
        return self.snapshot if self.snapshot.job.job_id == job_id else None


def _ready_scenario(
    tmp_path: Path,
    *,
    hold_days: tuple[int, ...] = (1, 2),
    commit_spool_type: type[LabArtifactCommitSpool] = LabArtifactCommitSpool,
    pause_resume_before_ready: bool = False,
) -> _Scenario:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commands = LabCommandSpool(tmp_path / "commands")
    commit_spool = commit_spool_type(tmp_path / "artifact-commits")
    artifact_store = LabJobArtifactStore(tmp_path / "job-artifacts")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=hold_days),
                max_attempts=2,
            ),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        artifact_commit_spool=commit_spool,
        artifact_store=artifact_store,
        clock=lambda: NOW,
    )
    scheduler.run_once()
    if pause_resume_before_ready:
        running = LabJobReader(store.path).get_job(job_id)
        assert running is not None and scheduler.lease is not None
        commands.publish(
            LabCommandEnvelope(
                request_id=uuid4(),
                command=PauseJobCommand(
                    job_id=job_id,
                    expected_version=running.version,
                    reason="test pause before ready",
                ),
            )
        )
        scheduler.run_once()
        paused = LabJobReader(store.path).get_job(job_id)
        assert paused is not None and paused.control_intent is ControlIntent.PAUSE_REQUESTED
        commands.publish(
            LabCommandEnvelope(
                request_id=uuid4(),
                command=ResumeJobCommand(
                    job_id=job_id,
                    expected_version=paused.version,
                    reason="test resume before ready",
                ),
            )
        )
        scheduler.run_once()
        resumed = LabJobReader(store.path).get_job(job_id)
        assert resumed is not None and resumed.control_intent is ControlIntent.NONE
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(),
        claims=claims,
        reports=reports,
    )
    for _ in hold_days:
        assert worker.run_once().status == "succeeded"
        scheduler.run_once()
    job = LabJobReader(store.path).get_job(job_id)
    assert job is not None
    assert job.status is JobStatus.RUNNING
    assert job.result_state is LabResultState.READY
    return _Scenario(
        root=tmp_path,
        store=store,
        scheduler=scheduler,
        job_id=job_id,
        artifact_store=artifact_store,
        commit_spool=commit_spool,
    )


def _ack_artifact_commit(
    scenario: _Scenario,
    *,
    status: str,
    reason: str,
) -> LabArtifactCommitReceipt:
    entry = scenario.commit_spool.pending()[0]
    receipt = LabArtifactCommitReceipt.from_envelope(
        entry.envelope,
        status=status,
        reason=reason,
        accepted_at=NOW,
        job_version=None,
    )
    scenario.commit_spool.ack(entry, receipt)
    return receipt


def _leave_prepared_candidate(
    scenario: _Scenario,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    finalizer = scenario.finalizer()

    def crash(_value: object) -> None:
        raise RuntimeError("candidate prepared")

    monkeypatch.setattr(finalizer, "_after_candidate_prepared", crash)
    with pytest.raises(RuntimeError, match="candidate prepared"):
        finalizer.finalize(scenario.job_id)
    candidates = tuple(scenario.artifact_store.candidates_root.iterdir())
    assert len(candidates) == 1
    return candidates[0]


def _insert_duplicate_accepted_success(path: Path, snapshot: LabFinalizationSnapshot) -> None:
    original = snapshot.shards[0].accepted_success
    report_payload = original.report.model_dump(mode="python")
    report_payload.update({"report_id": uuid4(), "content_hash": ""})
    report = LabWorkerReport.model_validate(report_payload)
    receipt = LabReportReceipt.from_report(
        report,
        status="accepted",
        reason="shard_succeeded",
        accepted_at=original.receipt.accepted_at + timedelta(microseconds=1),
    )
    with sqlite3.connect(path) as connection:
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
                report.model_dump_json(),
                receipt.status,
                receipt.reason,
                receipt.model_dump_json(),
                report.claim_generation,
                report.scheduler_fencing_token,
                original.received_at.isoformat(timespec="microseconds"),
                original.applied_at.isoformat(timespec="microseconds"),
            ),
        )


def _attempt_path(root: Path, evidence: LabFinalizationShardEvidence) -> Path:
    report = evidence.accepted_success.report
    return (
        root
        / "artifacts"
        / "jobs"
        / str(report.job_id)
        / "shards"
        / str(report.shard_id)
        / "attempts"
        / (
            f"{report.scheduler_fencing_token:020d}-"
            f"{report.claim_generation:020d}-{report.claim_token}"
        )
    )


def _persist_attempt_manifest(attempt: Path, manifest: LabShardResultManifest) -> None:
    path = attempt / "manifest.json"
    os.chmod(attempt, 0o700)
    os.chmod(path, 0o600)
    path.write_text(manifest.canonical_json(), encoding="utf-8")
    os.chmod(path, 0o400)
    os.chmod(attempt, 0o500)


def _evidence_for_manifest(
    evidence: LabFinalizationShardEvidence,
    manifest: LabShardResultManifest,
) -> LabFinalizationShardEvidence:
    original = evidence.accepted_success
    report_payload = original.report.model_dump(mode="python")
    report_payload.update(
        {
            "body": LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
            "content_hash": "",
        }
    )
    report = LabWorkerReport.model_validate(report_payload)
    receipt = LabReportReceipt.from_report(
        report,
        status="accepted",
        reason="shard_succeeded",
        accepted_at=original.receipt.accepted_at,
    )
    accepted = LabWorkerReportRecord(
        report=report,
        receipt=receipt,
        claim_generation=report.claim_generation,
        scheduler_fencing_token=report.scheduler_fencing_token,
        received_at=original.received_at,
        applied_at=original.applied_at,
    )
    shard = evidence.shard.model_copy(update={"result_manifest_hash": manifest.manifest_hash})
    return LabFinalizationShardEvidence(shard=shard, accepted_success=accepted)


def _rewrite_parquet_dtype(
    attempt: Path,
    evidence: LabFinalizationShardEvidence,
) -> LabFinalizationShardEvidence:
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    artifact = manifest.artifacts[0]
    parquet = attempt / artifact.file_name
    os.chmod(attempt, 0o700)
    os.chmod(parquet, 0o600)
    frame = pd.read_parquet(parquet)
    frame["hold_days"] = frame["hold_days"].astype(str)
    frame.to_parquet(parquet, index=False)
    persisted = pd.read_parquet(parquet)
    payload = parquet.read_bytes()
    changed_artifact = artifact.model_copy(
        update={
            "row_count": len(persisted),
            "columns": tuple(persisted.columns),
            "file_size": len(payload),
            "file_sha256": hashlib.sha256(payload).hexdigest(),
            "content_sha256": hashlib.sha256(
                canonical_shard_frame_json(persisted).encode("utf-8")
            ).hexdigest(),
        }
    )
    changed_manifest = LabShardResultManifest.model_validate(
        manifest.model_copy(update={"artifacts": (changed_artifact,)})
    )
    os.chmod(parquet, 0o400)
    _persist_attempt_manifest(attempt, changed_manifest)
    return _evidence_for_manifest(evidence, changed_manifest)


def test_finalization_snapshot_is_single_readonly_transaction_and_strong_graph(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path)
    reader = LabJobReader(scenario.store.path)
    connect_calls = 0
    original_connect = reader._connect
    inserted = False

    def count_connects() -> sqlite3.Connection:
        nonlocal connect_calls
        connect_calls += 1
        return original_connect()

    def insert_after_job_read(snapshot_job_id: UUID) -> None:
        nonlocal inserted
        if inserted:
            return
        inserted = True
        baseline = LabJobReader(scenario.store.path).get_finalization_snapshot(snapshot_job_id)
        assert baseline is not None
        _insert_duplicate_accepted_success(scenario.store.path, baseline)

    monkeypatch.setattr(reader, "_connect", count_connects)
    monkeypatch.setattr(reader, "_after_finalization_job_read", insert_after_job_read)

    snapshot = reader.get_finalization_snapshot(scenario.job_id)

    assert isinstance(snapshot, LabFinalizationSnapshot)
    assert connect_calls == 1
    assert snapshot.job.status is JobStatus.RUNNING
    assert snapshot.job.result_state is LabResultState.READY
    assert snapshot.ready_epoch.job_version == snapshot.job.version
    assert snapshot.ready_epoch.event.event_type == "job_result_ready"
    assert [item.shard.shard_index for item in snapshot.shards] == [0, 1]
    assert all(item.accepted_success.receipt.status == "accepted" for item in snapshot.shards)
    with pytest.raises(InvalidStoredJobError, match="exactly one accepted success"):
        LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)


def test_finalization_ready_epoch_is_stable_for_repeated_readonly_snapshots(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    reader = LabJobReader(scenario.store.path)

    first = reader.get_finalization_snapshot(scenario.job_id)
    second = reader.get_finalization_snapshot(scenario.job_id)

    assert first is not None and second is not None
    assert first.ready_epoch == second.ready_epoch
    assert first.ready_epoch.job_version == first.job.version
    assert first.job.control_intent is ControlIntent.NONE


def test_finalization_snapshot_rejects_ready_event_from_a_different_fence(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    assert snapshot.ready_epoch.event.scheduler_fencing_token is not None
    changed_event = snapshot.ready_epoch.event.model_copy(
        update={"scheduler_fencing_token": (snapshot.ready_epoch.event.scheduler_fencing_token + 1)}
    )
    changed_epoch = snapshot.ready_epoch.model_copy(update={"event": changed_event})

    with pytest.raises(ValueError, match="ready epoch conflicts"):
        LabFinalizationSnapshot(
            job=snapshot.job,
            ready_epoch=changed_epoch,
            shards=snapshot.shards,
        )


def test_pause_resume_before_completion_produces_one_stable_ready_epoch(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(
        tmp_path,
        hold_days=(1,),
        pause_resume_before_ready=True,
    )
    reader = LabJobReader(scenario.store.path)
    snapshot = reader.get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    events = reader.list_events(scenario.job_id)

    assert [event.event_type for event in events].count("control_intent_changed") == 2
    assert [event.event_type for event in events].count("job_result_ready") == 1
    assert snapshot.ready_epoch.job_version == snapshot.job.version
    assert snapshot.ready_epoch.event == next(
        event for event in events if event.event_type == "job_result_ready"
    )


def test_finalizer_builds_deterministic_complete_artifact_and_commit(tmp_path: Path) -> None:
    scenario = _ready_scenario(tmp_path)
    finalizer = scenario.finalizer()

    first = finalizer.finalize(scenario.job_id)
    pending = scenario.commit_spool.pending()
    assert first.status == "published"
    assert len(pending) == 1
    envelope = pending[0].envelope
    sealed = scenario.artifact_store.verify_sealed(envelope.commit.sealed_path)
    metrics_before = (sealed.path / "metrics.json").read_bytes()
    report_before = (sealed.path / "report.md").read_bytes()
    metrics = json.loads(metrics_before)
    assert metrics["job_id"] == str(scenario.job_id)
    assert metrics["result_hash"]
    assert metrics["shard_count"] == 2
    assert [item["shard_index"] for item in metrics["shards"]] == [0, 1]
    assert [item["name"] for item in metrics["tables"]] == ["trades"]
    assert b"generated_at" not in metrics_before

    second = scenario.finalizer().finalize(scenario.job_id)
    third = scenario.finalizer().finalize(scenario.job_id)

    assert isinstance(second, LabFinalizerResult)
    assert third.request_id == second.request_id == first.request_id == envelope.request_id
    assert (
        third.manifest_hash == second.manifest_hash == first.manifest_hash == sealed.manifest_hash
    )
    assert len(scenario.commit_spool.pending()) == 1
    assert (sealed.path / "metrics.json").read_bytes() == metrics_before
    assert (sealed.path / "report.md").read_bytes() == report_before


def test_finalizer_reports_rejected_ack_as_rejected_not_acknowledged(tmp_path: Path) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    first = scenario.finalizer().finalize(scenario.job_id)
    receipt = _ack_artifact_commit(
        scenario,
        status="rejected",
        reason="synthetic_rejection",
    )

    replay = scenario.finalizer().finalize(scenario.job_id)

    assert first.status == "published"
    assert receipt.status == "rejected"
    assert replay.status == "rejected"
    assert replay.rejection_reason == "synthetic_rejection"
    assert replay.request_id == first.request_id


def test_artifact_commit_request_identity_changes_only_for_new_ready_epoch(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    published = scenario.finalizer().finalize(scenario.job_id)
    sealed = scenario.artifact_store.verify_sealed(
        scenario.artifact_store.sealed_root / scenario.job_id.hex
    )

    same_epoch = LabFinalizer._envelope(sealed, snapshot.ready_epoch)
    next_event = snapshot.ready_epoch.event.model_copy(
        update={
            "event_id": snapshot.ready_epoch.event.event_id + 1,
            "job_version": snapshot.ready_epoch.job_version + 2,
        }
    )
    next_epoch = snapshot.ready_epoch.model_copy(
        update={
            "job_version": snapshot.ready_epoch.job_version + 2,
            "event": next_event,
        }
    )
    after_pause_resume = LabFinalizer._envelope(sealed, next_epoch)

    assert same_epoch.request_id == published.request_id
    assert after_pause_resume.commit == same_epoch.commit
    assert after_pause_resume.request_id != same_epoch.request_id


def test_rejected_commit_is_not_reused_after_a_new_ready_epoch(tmp_path: Path) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    original_snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert original_snapshot is not None
    rejected = scenario.finalizer().finalize(scenario.job_id)
    _ack_artifact_commit(
        scenario,
        status="rejected",
        reason="synthetic_pause_race",
    )
    next_event = original_snapshot.ready_epoch.event.model_copy(
        update={
            "event_id": original_snapshot.ready_epoch.event.event_id + 1,
            "job_version": original_snapshot.ready_epoch.job_version + 2,
        }
    )
    next_epoch = original_snapshot.ready_epoch.model_copy(
        update={
            "job_version": original_snapshot.ready_epoch.job_version + 2,
            "event": next_event,
        }
    )
    next_job = original_snapshot.job.model_copy(update={"version": next_epoch.job_version})
    next_snapshot = LabFinalizationSnapshot(
        job=next_job,
        ready_epoch=next_epoch,
        shards=original_snapshot.shards,
    )
    finalizer = LabFinalizer(
        reader=_StaticSnapshotReader(next_snapshot),  # type: ignore[arg-type]
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
    )

    republished = finalizer.finalize(scenario.job_id)

    assert republished.status == "published"
    assert republished.request_id != rejected.request_id
    assert scenario.commit_spool.pending()[0].envelope.request_id == republished.request_id


def test_finalizer_never_writes_the_sqlite_ledger(tmp_path: Path) -> None:
    scenario = _ready_scenario(tmp_path)
    before = scenario.store.path.read_bytes()
    before_stat = scenario.store.path.stat()

    scenario.finalizer().finalize(scenario.job_id)

    after_stat = scenario.store.path.stat()
    assert scenario.store.path.read_bytes() == before
    assert (after_stat.st_size, after_stat.st_mtime_ns, after_stat.st_ctime_ns) == (
        before_stat.st_size,
        before_stat.st_mtime_ns,
        before_stat.st_ctime_ns,
    )


@pytest.mark.parametrize(
    ("hook_name", "message"),
    [
        ("_after_candidate_prepared", "after candidate"),
        ("_after_artifact_sealed", "after seal"),
        ("_after_commit_published", "after publish"),
    ],
)
def test_finalizer_recovers_idempotently_after_each_crash_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hook_name: str,
    message: str,
) -> None:
    scenario = _ready_scenario(tmp_path)
    finalizer = scenario.finalizer()

    def crash(_value: object) -> None:
        raise RuntimeError(message)

    monkeypatch.setattr(finalizer, hook_name, crash)
    with pytest.raises(RuntimeError, match=message):
        finalizer.finalize(scenario.job_id)

    recovered = scenario.finalizer().finalize(scenario.job_id)

    assert recovered.status == "published"
    assert len(scenario.commit_spool.pending()) == 1
    assert not tuple(scenario.artifact_store.candidates_root.iterdir())
    assert (
        scenario.artifact_store.verify_sealed(
            scenario.artifact_store.sealed_root / scenario.job_id.hex
        ).manifest_hash
        == recovered.manifest_hash
    )


def test_finalizer_recovers_rename_completed_interrupted_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path)
    finalizer = scenario.finalizer()
    original = scenario.artifact_store._finalize_bound_directories
    crashed = False

    def crash_after_rename(bound: object) -> None:
        nonlocal crashed
        original(bound)  # type: ignore[arg-type]
        if not crashed:
            crashed = True
            raise RuntimeError("rename completed")

    monkeypatch.setattr(
        scenario.artifact_store,
        "_finalize_bound_directories",
        crash_after_rename,
    )
    with pytest.raises(RuntimeError, match="rename completed"):
        finalizer.finalize(scenario.job_id)

    recovered = scenario.finalizer().finalize(scenario.job_id)

    assert recovered.status == "published"
    assert len(scenario.commit_spool.pending()) == 1
    assert not tuple(scenario.artifact_store.candidates_root.iterdir())


def test_finalizer_recovers_explicit_torn_seal_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    candidate = _leave_prepared_candidate(scenario, monkeypatch)
    intent = scenario.artifact_store.seal_intents_root / f"{scenario.job_id.hex}.json"
    intent.write_bytes(b"{")
    os.chmod(intent, 0o600)
    record = next(
        item for item in scenario.artifact_store.list_candidate_recovery() if item.path == candidate
    )
    assert record.status == "recoverable_torn"

    recovered = scenario.finalizer().finalize(scenario.job_id)

    assert recovered.status == "published"
    assert not tuple(scenario.artifact_store.candidates_root.iterdir())
    assert any(
        item.read_bytes() == b"{"
        for item in scenario.artifact_store.seal_intents_quarantine_root.iterdir()
    )


def test_invalid_existing_candidate_retries_keep_active_evidence_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    original = _leave_prepared_candidate(scenario, monkeypatch)
    original_inode = original.stat().st_ino
    report = original / "report.md"
    os.chmod(original, 0o700)
    os.chmod(report, 0o600)
    report.write_bytes(b"corrupt preserved evidence\n")
    os.chmod(report, 0o400)
    os.chmod(original, 0o500)

    counts: list[int] = []
    for _ in range(3):
        with pytest.raises(
            LabFinalizationIntegrityError,
            match="invalid filesystem evidence",
        ):
            scenario.finalizer().finalize(scenario.job_id)
        counts.append(len(tuple(scenario.artifact_store.candidates_root.iterdir())))

    assert counts == [1, 1, 1]
    assert original.exists() and original.stat().st_ino == original_inode
    assert (original / "report.md").read_bytes() == b"corrupt preserved evidence\n"


def test_mismatched_existing_candidate_retries_keep_active_evidence_bounded(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    shard = snapshot.shards[0].shard
    original = scenario.artifact_store.prepare_candidate(
        job_id=scenario.job_id,
        spec=snapshot.job.spec,
        plan_hash=shard.plan_hash,
        adapter_id=shard.adapter_id,
        adapter_version=shard.adapter_version,
        result_contract_version="p1.4b-complete-result-v1",
        metrics={"schema_version": 1, "mismatched": True},
        report_markdown="# Mismatched candidate\n",
        tables={"trades": pd.DataFrame([{"hold_days": 99, "ret_pct": -99.0}])},
    )
    original_inode = original.inode

    counts: list[int] = []
    for _ in range(3):
        with pytest.raises(
            LabFinalizationIntegrityError,
            match="conflicts with current aggregate result",
        ):
            scenario.finalizer().finalize(scenario.job_id)
        counts.append(len(tuple(scenario.artifact_store.candidates_root.iterdir())))

    assert counts == [1, 1, 1]
    assert original.path.exists() and original.path.stat().st_ino == original_inode
    assert scenario.artifact_store.verify_candidate(original) == original.manifest


def test_broken_sealed_target_retries_do_not_accumulate_owned_candidates(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    scenario.finalizer().finalize(scenario.job_id)
    sealed = scenario.artifact_store.sealed_root / scenario.job_id.hex
    sealed_inode = sealed.stat().st_ino
    report = sealed / "report.md"
    os.chmod(sealed, 0o700)
    os.chmod(report, 0o600)
    report.write_bytes(b"broken sealed evidence\n")
    os.chmod(report, 0o400)
    os.chmod(sealed, 0o500)

    counts: list[int] = []
    for _ in range(3):
        with pytest.raises(
            LabFinalizationIntegrityError,
            match="neither complete nor recoverable",
        ):
            scenario.finalizer().finalize(scenario.job_id)
        counts.append(len(tuple(scenario.artifact_store.candidates_root.iterdir())))

    assert counts == [0, 0, 0]
    assert sealed.exists() and sealed.stat().st_ino == sealed_inode
    assert report.read_bytes() == b"broken sealed evidence\n"


def test_owned_candidate_cleanup_failure_preserves_primary_and_cleanup_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    original = _leave_prepared_candidate(scenario, monkeypatch)
    report = original / "report.md"
    os.chmod(original, 0o700)
    os.chmod(report, 0o600)
    report.write_bytes(b"corrupt preserved evidence\n")
    os.chmod(report, 0o400)
    os.chmod(original, 0o500)

    def fail_cleanup(*_args: object, **_kwargs: object) -> object:
        raise LabArtifactError("cleanup failed")

    monkeypatch.setattr(
        scenario.artifact_store,
        "quarantine_recovery_record",
        fail_cleanup,
    )

    with pytest.raises(ExceptionGroup) as raised:
        scenario.finalizer().finalize(scenario.job_id)

    messages = tuple(str(error) for error in raised.value.exceptions)
    assert any("invalid filesystem evidence" in message for message in messages)
    assert any("cleanup failed" in message for message in messages)


def test_scheduler_commit_before_ack_replays_without_duplicate_result(tmp_path: Path) -> None:
    scenario = _ready_scenario(tmp_path, commit_spool_type=_CrashBeforeArtifactAckSpool)
    published = scenario.finalizer().finalize(scenario.job_id)

    with pytest.raises(RuntimeError, match="after artifact SQLite commit"):
        scenario.scheduler.run_once()

    committed = LabJobReader(scenario.store.path).get_job(scenario.job_id)
    first = LabJobReader(scenario.store.path).get_artifact_commit(published.request_id)
    assert committed is not None and committed.result_state is LabResultState.SEALED
    assert first is not None and first.receipt.status == "accepted"
    assert len(scenario.commit_spool.pending()) == 1

    replay = scenario.scheduler.run_once()

    assert replay.artifact_commits_accepted == 1
    assert LabJobReader(scenario.store.path).get_artifact_commit(published.request_id) == first
    assert (
        len(
            [
                event
                for event in LabJobReader(scenario.store.path).list_events(scenario.job_id)
                if event.event_type == "job_result_sealed"
            ]
        )
        == 1
    )
    assert scenario.commit_spool.pending() == ()


def test_finalizer_skips_nonready_and_migrated_legacy_jobs(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "queued.sqlite3")
    store.initialize()
    queued_id = uuid4()
    command_spool = LabCommandSpool(tmp_path / "queued-commands")
    command_spool.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=queued_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
                max_attempts=2,
            ),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=command_spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        clock=lambda: NOW,
    )
    scheduler.run_once()
    finalizer = LabFinalizer(
        reader=LabJobReader(store.path),
        shard_artifact_root=tmp_path / "queued-shards",
        artifact_store=LabJobArtifactStore(tmp_path / "queued-artifacts"),
        commit_spool=LabArtifactCommitSpool(tmp_path / "queued-commits"),
    )
    assert finalizer.finalize(queued_id).status == "not_ready"

    legacy_path = tmp_path / "legacy.sqlite3"
    legacy_id = _create_v4_job_fixture(legacy_path, status=JobStatus.SUCCEEDED)
    LabJobStore(legacy_path).initialize()
    legacy_finalizer = LabFinalizer(
        reader=LabJobReader(legacy_path),
        shard_artifact_root=tmp_path / "legacy-shards",
        artifact_store=LabJobArtifactStore(tmp_path / "legacy-artifacts"),
        commit_spool=LabArtifactCommitSpool(tmp_path / "legacy-commits"),
    )
    assert legacy_finalizer.finalize(legacy_id).status == "not_ready"
    assert legacy_finalizer.commit_spool.pending() == ()

    failed_path = tmp_path / "failed.sqlite3"
    failed_id = _create_v4_job_fixture(failed_path, status=JobStatus.FAILED)
    LabJobStore(failed_path).initialize()
    failed_finalizer = LabFinalizer(
        reader=LabJobReader(failed_path),
        shard_artifact_root=tmp_path / "failed-shards",
        artifact_store=LabJobArtifactStore(tmp_path / "failed-artifacts"),
        commit_spool=LabArtifactCommitSpool(tmp_path / "failed-commits"),
    )
    assert failed_finalizer.finalize(failed_id).status == "not_ready"
    assert failed_finalizer.commit_spool.pending() == ()


def test_finalizer_does_not_refinalize_succeeded_job(tmp_path: Path) -> None:
    scenario = _ready_scenario(tmp_path)
    published = scenario.finalizer().finalize(scenario.job_id)
    assert scenario.scheduler.run_once().artifact_commits_accepted == 1
    job = LabJobReader(scenario.store.path).get_job(scenario.job_id)
    assert job is not None and job.status is JobStatus.SUCCEEDED

    result = scenario.finalizer().finalize(scenario.job_id)

    assert result.status == "not_ready"
    assert result.request_id is None
    assert LabJobReader(scenario.store.path).get_artifact_commit(published.request_id) is not None
    assert scenario.commit_spool.pending() == ()


@pytest.mark.parametrize("mutation", ["missing", "manifest", "parquet_symlink"])
def test_finalizer_fails_closed_when_exact_accepted_attempt_is_tampered(
    tmp_path: Path,
    mutation: str,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    report = evidence.accepted_success.report
    attempt = (
        tmp_path
        / "artifacts"
        / "jobs"
        / str(scenario.job_id)
        / "shards"
        / str(evidence.shard.shard_id)
        / "attempts"
        / (
            f"{report.scheduler_fencing_token:020d}-"
            f"{report.claim_generation:020d}-{report.claim_token}"
        )
    )
    os.chmod(attempt, 0o700)
    if mutation == "missing":
        (attempt / "manifest.json").unlink()
    elif mutation == "manifest":
        manifest_path = attempt / "manifest.json"
        os.chmod(manifest_path, 0o600)
        manifest_path.write_text("{}", encoding="utf-8")
    else:
        manifest = json.loads((attempt / "manifest.json").read_text(encoding="utf-8"))
        parquet = attempt / manifest["artifacts"][0]["file_name"]
        os.chmod(parquet, 0o600)
        payload = parquet.read_bytes()
        parquet.unlink()
        target = tmp_path / "outside.parquet"
        target.write_bytes(payload)
        parquet.symlink_to(target)

    with pytest.raises(LabFinalizationIntegrityError):
        scenario.finalizer().finalize(scenario.job_id)
    assert scenario.commit_spool.pending() == ()


def test_bundle_reader_rejects_hardlinked_parquet_from_accepted_attempt(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    parquet = attempt / manifest.artifacts[0].file_name
    os.link(parquet, tmp_path / "hardlink.parquet")

    with pytest.raises(
        LabFinalizationIntegrityError,
        match="not a private regular file",
    ):
        LabSealedShardBundleReader(tmp_path / "artifacts").read(evidence)


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("file_sha256", "artifact bytes conflict"),
        ("content_sha256", "Parquet content conflicts"),
    ],
)
def test_bundle_reader_rejects_trusted_manifest_hash_with_forged_parquet_hash(
    tmp_path: Path,
    field: str,
    message: str,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    artifact = manifest.artifacts[0].model_copy(update={field: "0" * 64})
    changed = LabShardResultManifest.model_validate(
        manifest.model_copy(update={"artifacts": (artifact,)})
    )
    _persist_attempt_manifest(attempt, changed)
    accepted = _evidence_for_manifest(evidence, changed)

    with pytest.raises(LabFinalizationIntegrityError, match=message):
        LabSealedShardBundleReader(tmp_path / "artifacts").read(accepted)


def test_finalizer_rejects_cross_shard_dtype_tamper_after_real_bundle_reads(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1, 2))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    changed_evidence = _rewrite_parquet_dtype(
        _attempt_path(tmp_path, snapshot.shards[1]),
        snapshot.shards[1],
    )
    changed_snapshot = LabFinalizationSnapshot(
        job=snapshot.job,
        ready_epoch=snapshot.ready_epoch,
        shards=(snapshot.shards[0], changed_evidence),
    )
    finalizer = LabFinalizer(
        reader=_StaticSnapshotReader(changed_snapshot),  # type: ignore[arg-type]
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
    )

    with pytest.raises(
        LabFinalizationIntegrityError,
        match="could not be aggregated",
    ):
        finalizer.finalize(scenario.job_id)
    assert scenario.commit_spool.pending() == ()


def test_bundle_reader_rejects_same_byte_path_replacement_after_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    report = evidence.accepted_success.report
    attempt = (
        tmp_path
        / "artifacts"
        / "jobs"
        / str(scenario.job_id)
        / "shards"
        / str(evidence.shard.shard_id)
        / "attempts"
        / (
            f"{report.scheduler_fencing_token:020d}-"
            f"{report.claim_generation:020d}-{report.claim_token}"
        )
    )
    replaced = False

    def replace_after_read(name: str) -> None:
        nonlocal replaced
        if replaced or name != "manifest.json":
            return
        replaced = True
        path = attempt / name
        payload = path.read_bytes()
        displaced = tmp_path / "displaced-manifest.json"
        os.chmod(attempt, 0o700)
        path.rename(displaced)
        path.write_bytes(payload)
        os.chmod(path, 0o400)
        os.chmod(attempt, 0o500)

    reader = LabSealedShardBundleReader(tmp_path / "artifacts")
    monkeypatch.setattr(reader, "_after_file_read", replace_after_read)

    with pytest.raises(
        LabFinalizationIntegrityError,
        match="file identity changed while reading",
    ):
        reader.read(evidence)
