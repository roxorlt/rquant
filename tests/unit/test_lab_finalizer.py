from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tracemalloc
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import rquant.lab_finalizer as lab_finalizer_module
from rquant.lab_artifact_protocol import (
    LabAcknowledgedArtifactCommit,
    LabArtifactCommitReceipt,
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
    LabFinalizerAuthorityKey,
)
from rquant.lab_artifacts import LabArtifactError, LabJobArtifactStore
from rquant.lab_finalizer import (
    LabArtifactRoundtripPeakUsage,
    LabFinalizationCodeMismatchError,
    LabFinalizationCodeProviderError,
    LabFinalizationIntegrityError,
    LabFinalizationResourceLimitError,
    LabFinalizer,
    LabFinalizerJobLimits,
    LabFinalizerMetrics,
    LabFinalizerResult,
    LabFinalizerShardSummary,
    LabFinalizerTableSummary,
    LabSealedShardBundleReader,
    LabShardBundleLimits,
)
from rquant.lab_job_protocol import (
    LabCommandEnvelope,
    LabCommandSpool,
    PauseJobCommand,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    ControlIntent,
    InvalidStoredJobError,
    JobStatus,
    LabArtifactCommitRecord,
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
from rquant.lab_worker import LabShardResultManifest, canonical_shard_frame_digest
from rquant.strategy_job_adapters import (
    LabJobExecutionResult,
    LabShardMetric,
    LabShardTable,
    default_strategy_job_adapter_registry,
)

from .test_lab_jobs import _create_v4_job_fixture
from .test_lab_worker import NOW, RecordingRegistry, _nshape_compare_spec, _worker

TEST_AUTHORITY_KEY = LabFinalizerAuthorityKey(
    key_id="finalizer-test-key",
    secret=b"f" * 32,
)


def _authority_key_provider() -> LabFinalizerAuthorityKey:
    return TEST_AUTHORITY_KEY


def _authority_verification_key_provider(
    key_id: str,
) -> LabFinalizerAuthorityKey | None:
    return TEST_AUTHORITY_KEY if key_id == TEST_AUTHORITY_KEY.key_id else None


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
            verified_code_sha_provider=lambda: "1" * 40,
            finalizer_authority_key_provider=_authority_key_provider,
        )


class _TamperedBundleSnapshotReader:
    """Inject typed bundle evidence only; never model job-state transitions."""

    def __init__(self, snapshot: LabFinalizationSnapshot) -> None:
        self.snapshot = snapshot

    def get_finalization_snapshot(self, job_id: UUID) -> LabFinalizationSnapshot | None:
        return self.snapshot if self.snapshot.job.job_id == job_id else None


class _CallbackSnapshotReader:
    def __init__(self, reader: LabJobReader, callback: Callable[[], None]) -> None:
        self.reader = reader
        self.callback = callback
        self.called = False

    def get_finalization_snapshot(self, job_id: UUID) -> LabFinalizationSnapshot | None:
        snapshot = self.reader.get_finalization_snapshot(job_id)
        if snapshot is not None and not self.called:
            self.called = True
            self.callback()
        return snapshot

    def get_artifact_commit(self, request_id: UUID) -> LabArtifactCommitRecord | None:
        return self.reader.get_artifact_commit(request_id)


def _ready_scenario(
    tmp_path: Path,
    *,
    hold_days: tuple[int, ...] = (1, 2),
    commit_spool_type: type[LabArtifactCommitSpool] = LabArtifactCommitSpool,
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
        finalizer_authority_key_provider=_authority_verification_key_provider,
        clock=lambda: NOW,
    )
    scheduler.run_once()
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


def _candidate_evidence_counts(store: LabJobArtifactStore) -> tuple[int, int]:
    return (
        len(tuple(store.candidates_root.iterdir())),
        len(tuple(store.quarantine_root.iterdir())),
    )


def _flatten_errors(error: BaseException) -> tuple[str, ...]:
    if isinstance(error, BaseExceptionGroup):
        return tuple(message for nested in error.exceptions for message in _flatten_errors(nested))
    return (str(error),)


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
            "content_sha256": canonical_shard_frame_digest(persisted),
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


def test_ready_pause_is_rejected_and_same_ready_epoch_request_remains_stable(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    reader = LabJobReader(scenario.store.path)
    before = reader.get_finalization_snapshot(scenario.job_id)
    assert before is not None and scenario.scheduler.lease is not None
    published = scenario.finalizer().finalize(scenario.job_id)
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(
            job_id=scenario.job_id,
            expected_version=before.job.version,
            reason="pause after complete result became ready",
        ),
    )
    receipt = scenario.store.apply_command(
        pause,
        lease=scenario.scheduler.lease,
        now=NOW,
    )
    after = reader.get_finalization_snapshot(scenario.job_id)
    replay = scenario.finalizer().finalize(scenario.job_id)

    assert receipt.status == "rejected"
    assert receipt.reason == "invalid_result_state:ready"
    assert after == before
    assert replay.request_id == published.request_id
    assert _candidate_evidence_counts(scenario.artifact_store) == (0, 0)


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
    assert metrics["finalizer_code_sha"] == "1" * 40
    assert metrics["result_hash"]
    assert metrics["shard_count"] == 2
    assert [item["shard_index"] for item in metrics["shards"]] == [0, 1]
    assert [item["name"] for item in metrics["tables"]] == ["trades"]
    assert b"generated_at" not in metrics_before

    replay_counts: list[tuple[int, int]] = []
    replays = []
    for _ in range(5):
        replays.append(scenario.finalizer().finalize(scenario.job_id))
        replay_counts.append(_candidate_evidence_counts(scenario.artifact_store))
    second, third = replays[:2]

    assert isinstance(second, LabFinalizerResult)
    assert third.request_id == second.request_id == first.request_id == envelope.request_id
    assert (
        third.manifest_hash == second.manifest_hash == first.manifest_hash == sealed.manifest_hash
    )
    assert len(scenario.commit_spool.pending()) == 1
    assert replay_counts == [(0, 0)] * 5
    assert (sealed.path / "metrics.json").read_bytes() == metrics_before
    assert (sealed.path / "report.md").read_bytes() == report_before


def test_finalizer_markdown_encodes_all_dynamic_text_as_indented_canonical_json() -> None:
    dangerous = "`fence` <script>alert(1)</script>\nnext|cell"
    metrics = LabFinalizerMetrics(
        job_id=uuid4(),
        spec_hash="1" * 64,
        plan_hash="2" * 64,
        adapter_id="adapter`<unsafe>",
        adapter_version="v1|next\nline",
        result_contract_version="contract`value",
        finalizer_code_sha="1" * 40,
        result_hash="3" * 64,
        shard_count=1,
        shards=(
            LabFinalizerShardSummary(
                shard_index=0,
                shard_id=uuid4(),
                result_manifest_hash="4" * 64,
                metrics=(LabShardMetric(name="note", value=dangerous),),
            ),
        ),
        tables=(
            LabFinalizerTableSummary(
                name="trades",
                row_count=1,
                columns=(dangerous,),
            ),
        ),
    )

    first = LabFinalizer._report(metrics)
    second = LabFinalizer._report(metrics)
    dynamic_lines = [line for line in first.splitlines() if line and not line.startswith("#")]

    assert first == second
    assert all(line.startswith("    ") for line in dynamic_lines)
    assert all(json.loads(line[4:]) for line in dynamic_lines)
    assert dangerous not in first


def test_bundle_reader_preserves_primary_and_descriptor_close_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest_path = attempt / "manifest.json"
    os.chmod(attempt, 0o700)
    os.chmod(manifest_path, 0o600)
    manifest_path.write_bytes(b"{}")
    os.chmod(manifest_path, 0o400)
    os.chmod(attempt, 0o500)
    original_close = lab_finalizer_module.os.close
    injected = False

    def close_with_fault(descriptor: int) -> None:
        nonlocal injected
        original_close(descriptor)
        if not injected:
            injected = True
            raise OSError("descriptor close failed")

    monkeypatch.setattr(lab_finalizer_module.os, "close", close_with_fault)

    with pytest.raises(BaseExceptionGroup) as raised:
        LabSealedShardBundleReader(tmp_path / "artifacts").read(evidence)

    messages = _flatten_errors(raised.value)
    assert any("manifest is invalid" in message for message in messages)
    assert any("descriptor close failed" in message for message in messages)


def test_bounded_descriptor_read_retries_eintr_and_rejects_short_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "bounded.bin"
    path.write_bytes(b"abc")
    descriptor = os.open(path, os.O_RDONLY)
    original_read = lab_finalizer_module.os.read
    interrupted = False

    def read_with_eintr(fd: int, size: int) -> bytes:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise InterruptedError
        return original_read(fd, size)

    monkeypatch.setattr(lab_finalizer_module.os, "read", read_with_eintr)
    try:
        assert (
            lab_finalizer_module._read_descriptor_bounded(
                descriptor,
                expected_size=3,
                max_bytes=3,
            )
            == b"abc"
        )
        with pytest.raises(LabFinalizationIntegrityError, match="ended before"):
            lab_finalizer_module._read_descriptor_bounded(
                descriptor,
                expected_size=4,
                max_bytes=4,
            )
    finally:
        os.close(descriptor)


def test_descriptor_stream_preserves_operation_and_stream_close_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "stream.bin"
    path.write_bytes(b"abc")
    descriptor = os.open(path, os.O_RDONLY)
    original_close = lab_finalizer_module.os.close

    class FaultyStream:
        def __init__(self, owned_descriptor: int) -> None:
            self.owned_descriptor = owned_descriptor

        def close(self) -> None:
            original_close(self.owned_descriptor)
            raise OSError("stream close failed")

    def faulty_fdopen(owned_descriptor: int, _mode: str) -> object:
        return FaultyStream(owned_descriptor)

    def operation(_stream: object) -> object:
        raise RuntimeError("stream operation failed")

    monkeypatch.setattr(lab_finalizer_module.os, "fdopen", faulty_fdopen)
    try:
        with pytest.raises(BaseExceptionGroup) as raised:
            lab_finalizer_module._run_with_descriptor_stream(
                descriptor,
                operation,
                label="faulted stream",
            )
    finally:
        original_close(descriptor)

    assert _flatten_errors(raised.value) == (
        "stream operation failed",
        "stream close failed",
    )


def test_finalizer_rejects_spool_ack_without_authoritative_ledger_commit(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    first = scenario.finalizer().finalize(scenario.job_id)
    receipt = _ack_artifact_commit(
        scenario,
        status="accepted",
        reason="not_committed",
    )

    with pytest.raises(
        LabFinalizationIntegrityError,
        match="acknowledgement.*ledger|ledger.*acknowledgement",
    ):
        scenario.finalizer().finalize(scenario.job_id)

    assert first.status == "published"
    assert receipt.status == "accepted"
    assert receipt.job_version is None
    assert not scenario.commit_spool.pending()
    assert _candidate_evidence_counts(scenario.artifact_store) == (0, 0)


def test_pending_commit_without_ledger_runs_full_finalization_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    first = scenario.finalizer().finalize(scenario.job_id)
    replay = scenario.finalizer()
    calls = {"read": 0, "aggregate": 0, "preview": 0}
    original_read = replay.bundle_reader.read
    original_aggregate = replay.adapter_registry.aggregate_results
    original_preview = replay.artifact_store.preview_candidate

    def read(*args: object, **kwargs: object) -> object:
        calls["read"] += 1
        return original_read(*args, **kwargs)  # type: ignore[arg-type]

    def aggregate(*args: object, **kwargs: object) -> object:
        calls["aggregate"] += 1
        return original_aggregate(*args, **kwargs)  # type: ignore[arg-type]

    def preview(*args: object, **kwargs: object) -> object:
        calls["preview"] += 1
        return original_preview(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(replay.bundle_reader, "read", read)
    monkeypatch.setattr(replay.adapter_registry, "aggregate_results", aggregate)
    monkeypatch.setattr(replay.artifact_store, "preview_candidate", preview)

    second = replay.finalize(scenario.job_id)

    assert second.status == "published"
    assert second.request_id == first.request_id
    assert calls == {"read": 1, "aggregate": 1, "preview": 1}


def test_forged_sealed_and_uncommitted_pending_cannot_bypass_aggregation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    reader = LabJobReader(scenario.store.path)
    snapshot = reader.get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    shard = snapshot.shards[0].shard
    forged = scenario.artifact_store.seal_candidate(
        scenario.artifact_store.prepare_candidate(
            job_id=scenario.job_id,
            spec=snapshot.job.spec,
            plan_hash=shard.plan_hash,
            adapter_id=shard.adapter_id,
            adapter_version=shard.adapter_version,
            result_contract_version="p1.4b-complete-result-v1",
            metrics={"schema_version": 1, "forged": True},
            report_markdown="# Forged\n",
            tables={"trades": pd.DataFrame([{"hold_days": 999, "ret_pct": 999.0}])},
        )
    )
    forged_envelope = scenario.finalizer()._envelope(
        forged,
        snapshot,
        finalizer_code_sha="1" * 40,
    )
    scenario.commit_spool.publish(forged_envelope)
    finalizer = scenario.finalizer()
    calls = {"read": 0, "aggregate": 0, "preview": 0}
    original_read = finalizer.bundle_reader.read
    original_aggregate = finalizer.adapter_registry.aggregate_results
    original_preview = finalizer.artifact_store.preview_candidate

    def read(*args: object, **kwargs: object) -> object:
        calls["read"] += 1
        return original_read(*args, **kwargs)  # type: ignore[arg-type]

    def aggregate(*args: object, **kwargs: object) -> object:
        calls["aggregate"] += 1
        return original_aggregate(*args, **kwargs)  # type: ignore[arg-type]

    def preview(*args: object, **kwargs: object) -> object:
        calls["preview"] += 1
        return original_preview(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(finalizer.bundle_reader, "read", read)
    monkeypatch.setattr(finalizer.adapter_registry, "aggregate_results", aggregate)
    monkeypatch.setattr(finalizer.artifact_store, "preview_candidate", preview)

    with pytest.raises(
        LabFinalizationIntegrityError,
        match="sealed artifact conflicts|uncommitted artifact commit",
    ):
        finalizer.finalize(scenario.job_id)

    scheduler_result = scenario.scheduler.run_once()
    job = reader.get_job(scenario.job_id)
    assert calls == {"read": 1, "aggregate": 1, "preview": 1}
    assert scheduler_result.artifact_commits_accepted == 0
    assert scenario.commit_spool.pending() == ()
    assert job is not None and job.result_state is LabResultState.READY


def test_runtime_code_sha_mismatch_fails_before_any_finalization_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    finalizer = LabFinalizer(
        reader=LabJobReader(scenario.store.path),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "2" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("code mismatch reached finalization I/O")

    monkeypatch.setattr(finalizer.bundle_reader, "read", forbidden)
    monkeypatch.setattr(finalizer.artifact_store, "verify_sealed", forbidden)
    monkeypatch.setattr(finalizer.artifact_store, "preview_candidate", forbidden)
    monkeypatch.setattr(finalizer.commit_spool, "publish", forbidden)

    with pytest.raises(LabFinalizationCodeMismatchError) as raised:
        finalizer.finalize(scenario.job_id)

    assert raised.value.expected == "1" * 40
    assert raised.value.actual == "2" * 40
    assert tuple(scenario.artifact_store.sealed_root.iterdir()) == ()
    assert scenario.commit_spool.pending() == ()


@pytest.mark.parametrize(
    "runtime_code_sha",
    ("1" * 39, "1" * 41, "G" * 40, "A" * 40),
)
def test_finalizer_rejects_invalid_verified_runtime_code_sha_before_io(
    tmp_path: Path,
    runtime_code_sha: str,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    finalizer = LabFinalizer(
        reader=LabJobReader(scenario.store.path),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        verified_code_sha_provider=lambda: runtime_code_sha,
        finalizer_authority_key_provider=_authority_key_provider,
    )

    with pytest.raises(LabFinalizationCodeProviderError, match="invalid commit"):
        finalizer.finalize(scenario.job_id)


@pytest.mark.parametrize(
    "runtime_code_sha",
    (None, 1, b"1" * 40, object()),
    ids=("none", "integer", "bytes", "object"),
)
def test_finalizer_rejects_non_string_verified_runtime_code_sha_before_io(
    tmp_path: Path,
    runtime_code_sha: object,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    finalizer = LabFinalizer(
        reader=LabJobReader(scenario.store.path),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        verified_code_sha_provider=lambda: runtime_code_sha,  # type: ignore[arg-type]
        finalizer_authority_key_provider=_authority_key_provider,
    )

    with pytest.raises(LabFinalizationCodeProviderError, match="invalid commit"):
        finalizer.finalize(scenario.job_id)
    assert scenario.commit_spool.pending() == ()
    assert tuple(scenario.artifact_store.sealed_root.iterdir()) == ()


def test_finalizer_calls_verified_code_provider_for_every_finalize(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    values = iter(("1" * 40, "2" * 40))
    calls = 0

    def provider() -> str:
        nonlocal calls
        calls += 1
        return next(values)

    finalizer = LabFinalizer(
        reader=LabJobReader(scenario.store.path),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        verified_code_sha_provider=provider,
        finalizer_authority_key_provider=_authority_key_provider,
    )

    assert finalizer.finalize(scenario.job_id).status == "published"
    with pytest.raises(LabFinalizationCodeMismatchError):
        finalizer.finalize(scenario.job_id)
    assert calls == 2


def test_finalizer_provider_failure_is_typed_and_has_no_side_effect(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))

    def provider() -> str:
        raise RuntimeError("git authority unavailable")

    finalizer = LabFinalizer(
        reader=LabJobReader(scenario.store.path),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        verified_code_sha_provider=provider,
        finalizer_authority_key_provider=_authority_key_provider,
    )

    with pytest.raises(LabFinalizationCodeProviderError, match="provider failed"):
        finalizer.finalize(scenario.job_id)
    assert scenario.commit_spool.pending() == ()
    assert tuple(scenario.artifact_store.sealed_root.iterdir()) == ()


def test_sealed_without_durable_commit_evidence_rebuilds_before_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    crashing = scenario.finalizer()

    def crash(_sealed: object) -> None:
        raise RuntimeError("sealed before publish")

    monkeypatch.setattr(crashing, "_after_artifact_sealed", crash)
    with pytest.raises(RuntimeError, match="sealed before publish"):
        crashing.finalize(scenario.job_id)
    assert scenario.commit_spool.pending() == ()

    replay = scenario.finalizer()
    calls = {"read": 0, "aggregate": 0, "preview": 0}
    original_read = replay.bundle_reader.read
    original_aggregate = replay.adapter_registry.aggregate_results
    original_preview = replay.artifact_store.preview_candidate

    def read(*args: object, **kwargs: object) -> object:
        calls["read"] += 1
        return original_read(*args, **kwargs)  # type: ignore[arg-type]

    def aggregate(*args: object, **kwargs: object) -> object:
        calls["aggregate"] += 1
        return original_aggregate(*args, **kwargs)  # type: ignore[arg-type]

    def preview(*args: object, **kwargs: object) -> object:
        calls["preview"] += 1
        return original_preview(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(replay.bundle_reader, "read", read)
    monkeypatch.setattr(replay.adapter_registry, "aggregate_results", aggregate)
    monkeypatch.setattr(replay.artifact_store, "preview_candidate", preview)

    result = replay.finalize(scenario.job_id)

    assert result.status == "published"
    assert calls == {"read": 1, "aggregate": 1, "preview": 1}


def test_accepted_ack_fast_replay_requires_exact_scheduler_ledger_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    first = scenario.finalizer().finalize(scenario.job_id)
    scheduler_result = None

    def commit() -> None:
        nonlocal scheduler_result
        scheduler_result = scenario.scheduler.run_once()

    reader = _CallbackSnapshotReader(LabJobReader(scenario.store.path), commit)
    replay = LabFinalizer(
        reader=reader,  # type: ignore[arg-type]
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("accepted ACK replay entered expensive finalization")

    monkeypatch.setattr(replay.bundle_reader, "read", forbidden)
    monkeypatch.setattr(replay.adapter_registry, "aggregate_results", forbidden)
    monkeypatch.setattr(replay.artifact_store, "preview_candidate", forbidden)

    result = replay.finalize(scenario.job_id)
    ledger = LabJobReader(scenario.store.path).get_artifact_commit(first.request_id)
    acknowledged = scenario.commit_spool.inspect(first.request_id)

    assert scheduler_result is not None and scheduler_result.artifact_commits_accepted == 1
    assert isinstance(acknowledged, LabAcknowledgedArtifactCommit)
    assert ledger is not None
    assert ledger.envelope.request_id == result.request_id == first.request_id
    assert ledger.receipt == acknowledged.receipt
    assert result.status == "acknowledged"


def test_rejected_ack_fast_replay_requires_exact_scheduler_ledger_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    first = scenario.finalizer().finalize(scenario.job_id)
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    scenario.scheduler.clock = lambda: snapshot.job.deadline + timedelta(seconds=1)
    scenario.scheduler.lease = None
    monkeypatch.setattr(scenario.store, "expire_deadline_jobs", lambda **_kwargs: ())
    scheduler_result = None

    def reject() -> None:
        nonlocal scheduler_result
        scheduler_result = scenario.scheduler.run_once()

    reader = _CallbackSnapshotReader(LabJobReader(scenario.store.path), reject)
    replay = LabFinalizer(
        reader=reader,  # type: ignore[arg-type]
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("rejected ACK replay entered expensive finalization")

    monkeypatch.setattr(replay.bundle_reader, "read", forbidden)
    monkeypatch.setattr(replay.adapter_registry, "aggregate_results", forbidden)
    monkeypatch.setattr(replay.artifact_store, "preview_candidate", forbidden)

    results = [replay.finalize(scenario.job_id) for _ in range(3)]
    result = results[0]
    ledger = LabJobReader(scenario.store.path).get_artifact_commit(first.request_id)
    acknowledged = scenario.commit_spool.inspect(first.request_id)

    assert scheduler_result is not None and scheduler_result.artifact_commits_rejected == 1
    assert isinstance(acknowledged, LabAcknowledgedArtifactCommit)
    assert ledger is not None
    assert ledger.envelope.request_id == result.request_id == first.request_id
    assert ledger.receipt == acknowledged.receipt
    assert all(item.request_id == first.request_id for item in results)
    assert all(item.status == "rejected" for item in results)
    assert all(item.rejection_reason == "deadline_expired" for item in results)


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
    assert not tuple(scenario.artifact_store.quarantine_root.iterdir())
    assert (
        scenario.artifact_store.verify_sealed(
            scenario.artifact_store.sealed_root / scenario.job_id.hex
        ).manifest_hash
        == recovered.manifest_hash
    )


def test_finalizer_reuses_verified_transition_key_pending_after_signing_rotation(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    old_key = LabFinalizerAuthorityKey(key_id="finalizer-old", secret=b"o" * 32)
    new_key = LabFinalizerAuthorityKey(key_id="finalizer-new", secret=b"n" * 32)
    active = [old_key]
    keyring = {old_key.key_id: old_key, new_key.key_id: new_key}

    def signing_key() -> LabFinalizerAuthorityKey:
        return active[0]

    def finalizer() -> LabFinalizer:
        return LabFinalizer(
            reader=LabJobReader(scenario.store.path),
            shard_artifact_root=tmp_path / "artifacts",
            artifact_store=scenario.artifact_store,
            commit_spool=scenario.commit_spool,
            adapter_registry=default_strategy_job_adapter_registry(),
            verified_code_sha_provider=lambda: "1" * 40,
            finalizer_authority_key_provider=signing_key,
            finalizer_authority_verification_key_provider=keyring.get,
        )

    first = finalizer().finalize(scenario.job_id)
    first_pending = scenario.commit_spool.pending()
    assert len(first_pending) == 1
    first_proof = first_pending[0].envelope.authority_proof
    assert first_proof is not None and first_proof.key_id == old_key.key_id

    active[0] = new_key
    replay = finalizer().finalize(scenario.job_id)
    pending = scenario.commit_spool.pending()

    assert replay.status == "published"
    assert replay.request_id == first.request_id
    assert len(pending) == 1
    assert pending[0].envelope == first_pending[0].envelope


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

    counts: list[tuple[int, int]] = []
    for _ in range(3):
        with pytest.raises(
            LabFinalizationIntegrityError,
            match="invalid filesystem evidence",
        ):
            scenario.finalizer().finalize(scenario.job_id)
        counts.append(_candidate_evidence_counts(scenario.artifact_store))

    assert counts == [(1, 0)] * 3
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

    counts: list[tuple[int, int]] = []
    for _ in range(3):
        with pytest.raises(
            LabFinalizationIntegrityError,
            match="conflicts with current aggregate result",
        ):
            scenario.finalizer().finalize(scenario.job_id)
        counts.append(_candidate_evidence_counts(scenario.artifact_store))

    assert counts == [(1, 0)] * 3
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

    counts: list[tuple[int, int]] = []
    for _ in range(3):
        with pytest.raises(
            LabFinalizationIntegrityError,
            match="neither complete nor recoverable",
        ):
            scenario.finalizer().finalize(scenario.job_id)
        counts.append(_candidate_evidence_counts(scenario.artifact_store))

    assert counts == [(0, 0)] * 3
    assert sealed.exists() and sealed.stat().st_ino == sealed_inode
    assert report.read_bytes() == b"broken sealed evidence\n"


def test_matching_sealed_artifact_bypasses_conflicting_candidate_without_mutation(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    published = scenario.finalizer().finalize(scenario.job_id)
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    shard = snapshot.shards[0].shard
    conflict = scenario.artifact_store.prepare_candidate(
        job_id=scenario.job_id,
        spec=snapshot.job.spec,
        plan_hash=shard.plan_hash,
        adapter_id=shard.adapter_id,
        adapter_version=shard.adapter_version,
        result_contract_version="p1.4b-complete-result-v1",
        metrics={"schema_version": 1, "conflict": True},
        report_markdown="# Preserved conflict\n",
        tables={"trades": pd.DataFrame([{"hold_days": 99, "ret_pct": -99.0}])},
    )
    baseline = _candidate_evidence_counts(scenario.artifact_store)
    conflict_identity = (conflict.path.stat().st_dev, conflict.path.stat().st_ino)

    replays: list[LabFinalizerResult] = []
    counts: list[tuple[int, int]] = []
    for _ in range(3):
        replays.append(scenario.finalizer().finalize(scenario.job_id))
        counts.append(_candidate_evidence_counts(scenario.artifact_store))

    assert all(item.request_id == published.request_id for item in replays)
    assert counts == [baseline] * 3
    assert baseline == (1, 0)
    assert (conflict.path.stat().st_dev, conflict.path.stat().st_ino) == conflict_identity


def test_matching_sealed_replay_retries_exact_redundant_candidate_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    finalizer = scenario.finalizer()
    plans = []
    original_prepare = scenario.artifact_store.prepare_candidate_from_plan

    def capture(plan: object) -> object:
        plans.append(plan)
        return original_prepare(plan)  # type: ignore[arg-type]

    monkeypatch.setattr(
        scenario.artifact_store,
        "prepare_candidate_from_plan",
        capture,
    )
    published = finalizer.finalize(scenario.job_id)
    assert len(plans) == 1
    monkeypatch.setattr(
        scenario.artifact_store,
        "prepare_candidate_from_plan",
        original_prepare,
    )
    seal_intent = scenario.artifact_store.seal_intents_root / f"{scenario.job_id.hex}.json"
    os.chmod(scenario.artifact_store.seal_intents_root, 0o700)
    seal_intent.unlink()
    redundant = scenario.artifact_store.prepare_candidate_from_plan(plans[0])
    redundant_record = next(
        record
        for record in scenario.artifact_store.list_candidate_recovery()
        if record.path == redundant.path
    )
    assert redundant_record.status in {"recoverable", "needs_authority", "recoverable_torn"}
    assert redundant_record.job_id == scenario.job_id
    assert redundant_record.manifest_hash == published.manifest_hash
    original_quarantine = scenario.artifact_store.quarantine_recovery_record
    failed = False

    def fail_once(*args: object, **kwargs: object) -> object:
        nonlocal failed
        if not failed:
            failed = True
            raise LabArtifactError("redundant cleanup failed")
        return original_quarantine(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        scenario.artifact_store,
        "quarantine_recovery_record",
        fail_once,
    )

    with pytest.raises(
        LabFinalizationIntegrityError,
        match="redundant matching candidate.*isolated",
    ):
        scenario.finalizer().finalize(scenario.job_id)
    assert _candidate_evidence_counts(scenario.artifact_store) == (1, 0)
    assert redundant.path.exists()

    recovered = scenario.finalizer().finalize(scenario.job_id)
    after_recovery = _candidate_evidence_counts(scenario.artifact_store)
    replay = scenario.finalizer().finalize(scenario.job_id)

    assert recovered.request_id == replay.request_id == published.request_id
    assert after_recovery == (0, 1)
    assert _candidate_evidence_counts(scenario.artifact_store) == after_recovery


def test_owned_candidate_cleanup_failure_preserves_primary_and_cleanup_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))

    def fail_seal(*_args: object, **_kwargs: object) -> object:
        raise LabArtifactError("seal failed")

    def fail_cleanup(*_args: object, **_kwargs: object) -> object:
        raise LabArtifactError("cleanup failed")

    monkeypatch.setattr(
        scenario.artifact_store,
        "seal_candidate",
        fail_seal,
    )
    monkeypatch.setattr(
        scenario.artifact_store,
        "quarantine_recovery_record",
        fail_cleanup,
    )

    with pytest.raises(ExceptionGroup) as raised:
        scenario.finalizer().finalize(scenario.job_id)

    messages = tuple(str(error) for error in raised.value.exceptions)
    assert any("job artifact could not be sealed" in message for message in messages)
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


def test_ledger_commit_before_spool_ack_is_authoritative_fast_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, commit_spool_type=_CrashBeforeArtifactAckSpool)
    published = scenario.finalizer().finalize(scenario.job_id)

    def commit_without_ack() -> None:
        with pytest.raises(RuntimeError, match="after artifact SQLite commit"):
            scenario.scheduler.run_once()

    replay = LabFinalizer(
        reader=_CallbackSnapshotReader(
            LabJobReader(scenario.store.path),
            commit_without_ack,
        ),  # type: ignore[arg-type]
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("ledger-authoritative replay entered expensive finalization")

    monkeypatch.setattr(replay.bundle_reader, "read", forbidden)
    monkeypatch.setattr(replay.adapter_registry, "aggregate_results", forbidden)
    monkeypatch.setattr(replay.artifact_store, "preview_candidate", forbidden)

    result = replay.finalize(scenario.job_id)
    ledger = LabJobReader(scenario.store.path).get_artifact_commit(published.request_id)

    assert ledger is not None and ledger.receipt.status == "accepted"
    assert result.status == "acknowledged"
    assert result.request_id == published.request_id
    assert len(scenario.commit_spool.pending()) == 1
    assert scenario.scheduler.run_once().artifact_commits_accepted == 1
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
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
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
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
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
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
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


def test_bundle_reader_rejects_sparse_oversized_file_before_read_or_allocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    artifact = manifest.artifacts[0]
    parquet = attempt / artifact.file_name
    oversized = 8 * 1024 * 1024
    os.chmod(attempt, 0o700)
    os.chmod(parquet, 0o600)
    with parquet.open("r+b") as stream:
        stream.truncate(oversized)
    changed = manifest.model_copy(
        update={"artifacts": (artifact.model_copy(update={"file_size": oversized}),)}
    )
    os.chmod(parquet, 0o400)
    _persist_attempt_manifest(attempt, changed)
    accepted = _evidence_for_manifest(evidence, changed)

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("oversized Parquet reached pandas")

    monkeypatch.setattr(lab_finalizer_module.pd, "read_parquet", forbidden)
    reader = LabSealedShardBundleReader(
        tmp_path / "artifacts",
        limits=LabShardBundleLimits(
            max_single_file_bytes=1024,
            max_bundle_total_bytes=16 * 1024 * 1024,
        ),
    )

    with pytest.raises(LabFinalizationIntegrityError, match="single file.*limit"):
        reader.read(accepted)


def test_bundle_reader_rejects_parquet_decompression_amplification_before_pandas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    artifact = manifest.artifacts[0]
    parquet = attempt / artifact.file_name
    frame = pd.DataFrame(
        {
            "hold_days": pd.Series(range(5_000), dtype="int64"),
            "ret_pct": pd.Series(
                [f"{index:08d}-" + ("x" * 1_000) for index in range(5_000)],
                dtype="string",
            ),
        }
    )
    os.chmod(attempt, 0o700)
    os.chmod(parquet, 0o600)
    frame.to_parquet(parquet, index=False)
    persisted = pd.read_parquet(parquet)
    payload = parquet.read_bytes()
    changed_artifact = artifact.model_copy(
        update={
            "row_count": len(persisted),
            "columns": tuple(persisted.columns),
            "file_size": len(payload),
            "file_sha256": hashlib.sha256(payload).hexdigest(),
            "content_sha256": canonical_shard_frame_digest(persisted),
        }
    )
    changed = manifest.model_copy(update={"artifacts": (changed_artifact,)})
    os.chmod(parquet, 0o400)
    _persist_attempt_manifest(attempt, changed)
    accepted = _evidence_for_manifest(evidence, changed)

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("amplified Parquet reached pandas")

    monkeypatch.setattr(lab_finalizer_module.pd, "read_parquet", forbidden)
    reader = LabSealedShardBundleReader(
        tmp_path / "artifacts",
        limits=LabShardBundleLimits(max_parquet_uncompressed_bytes=64 * 1024),
    )

    with pytest.raises(LabFinalizationIntegrityError, match="uncompressed.*limit"):
        reader.read(accepted)


def test_bundle_reader_rejects_dictionary_string_pandas_amplification_before_conversion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    artifact = manifest.artifacts[0]
    parquet = attempt / artifact.file_name
    rows = 20_000
    table = pa.table(
        {
            "hold_days": pa.array(range(rows), type=pa.int64()),
            "ret_pct": pa.array(["repeated-value"] * rows).dictionary_encode(),
        }
    )
    os.chmod(attempt, 0o700)
    os.chmod(parquet, 0o600)
    pq.write_table(table, parquet)
    payload = parquet.read_bytes()
    changed_artifact = artifact.model_copy(
        update={
            "row_count": rows,
            "columns": tuple(table.column_names),
            "file_size": len(payload),
            "file_sha256": hashlib.sha256(payload).hexdigest(),
        }
    )
    changed = LabShardResultManifest.model_validate(
        manifest.model_copy(update={"artifacts": (changed_artifact,)})
    )
    os.chmod(parquet, 0o400)
    _persist_attempt_manifest(attempt, changed)
    accepted = _evidence_for_manifest(evidence, changed)
    conversions = 0

    def forbidden(_table: object) -> None:
        nonlocal conversions
        conversions += 1
        raise AssertionError("oversized dictionary table reached pandas conversion")

    reader = LabSealedShardBundleReader(
        tmp_path / "artifacts",
        limits=LabShardBundleLimits(
            max_row_count=rows,
            max_arrow_table_bytes=2 * 1024 * 1024,
            max_materialized_dataframe_bytes=512 * 1024,
        ),
    )
    monkeypatch.setattr(reader, "_before_arrow_to_pandas", forbidden)

    with pytest.raises(
        LabFinalizationIntegrityError,
        match="estimated pandas.*memory limit",
    ):
        reader.read(accepted)

    assert conversions == 0


@pytest.mark.parametrize(
    "arrow_type",
    (pa.bool_(), pa.null()),
    ids=("nullable-bool", "null"),
)
def test_bundle_reader_rejects_nullable_object_amplification_before_conversion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    arrow_type: pa.DataType,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    artifact = manifest.artifacts[0]
    parquet = attempt / artifact.file_name
    rows = 100_000
    table = pa.table(
        {
            "hold_days": pa.array([None] * rows, type=arrow_type),
            "ret_pct": pa.array([None] * rows, type=arrow_type),
        }
    )
    os.chmod(attempt, 0o700)
    os.chmod(parquet, 0o600)
    pq.write_table(table, parquet)
    payload = parquet.read_bytes()
    changed = manifest.model_copy(
        update={
            "artifacts": (
                artifact.model_copy(
                    update={
                        "row_count": rows,
                        "columns": tuple(table.column_names),
                        "file_size": len(payload),
                        "file_sha256": hashlib.sha256(payload).hexdigest(),
                    }
                ),
            )
        }
    )
    os.chmod(parquet, 0o400)
    _persist_attempt_manifest(attempt, changed)
    accepted = _evidence_for_manifest(evidence, changed)
    conversions: list[int] = []
    reader = LabSealedShardBundleReader(
        tmp_path / "artifacts",
        limits=LabShardBundleLimits(
            max_row_count=rows * 2,
            max_materialized_dataframe_bytes=1024 * 1024,
        ),
    )
    monkeypatch.setattr(
        reader,
        "_before_arrow_to_pandas",
        lambda _table: conversions.append(1),
    )

    with pytest.raises(
        LabFinalizationIntegrityError,
        match="estimated pandas.*memory limit",
    ):
        reader.read(accepted)
    assert conversions == []


def test_descriptor_reads_bound_repeated_interruptions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"bounded-read")
    descriptor = os.open(path, os.O_RDONLY)
    original = os.read
    attempts = 0

    def interrupted(fd: int, size: int) -> bytes:
        nonlocal attempts
        attempts += 1
        if attempts <= 2:
            raise InterruptedError
        return original(fd, size)

    monkeypatch.setattr(lab_finalizer_module.os, "read", interrupted)
    try:
        assert (
            lab_finalizer_module._read_descriptor_bounded(
                descriptor,
                expected_size=len(b"bounded-read"),
                max_bytes=1024,
                max_consecutive_interrupted_reads=2,
            )
            == b"bounded-read"
        )
    finally:
        os.close(descriptor)


def test_descriptor_reads_fail_after_interruption_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"never-read")
    descriptor = os.open(path, os.O_RDONLY)
    attempts = 0

    def interrupted(_fd: int, _size: int) -> bytes:
        nonlocal attempts
        attempts += 1
        raise InterruptedError

    monkeypatch.setattr(lab_finalizer_module.os, "read", interrupted)
    try:
        with pytest.raises(LabFinalizationIntegrityError, match="interrupted read limit"):
            lab_finalizer_module._sha256_descriptor_bounded(
                descriptor,
                expected_size=len(b"never-read"),
                max_consecutive_interrupted_reads=3,
            )
    finally:
        os.close(descriptor)
    assert attempts == 4


def test_finalizer_rejects_cumulative_shard_budget_before_aggregate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1, 2))
    finalizer = LabFinalizer(
        reader=LabJobReader(scenario.store.path),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
        job_limits=LabFinalizerJobLimits(max_total_shard_rows=1),
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("cumulative shard overflow reached aggregate")

    monkeypatch.setattr(finalizer.adapter_registry, "aggregate_results", forbidden)
    pandas_attempts: list[int] = []
    monkeypatch.setattr(
        finalizer.bundle_reader,
        "_before_arrow_to_pandas",
        lambda _table: pandas_attempts.append(1),
    )

    with pytest.raises(
        LabFinalizationResourceLimitError,
        match="total shard row",
    ):
        finalizer.finalize(scenario.job_id)

    assert tuple(scenario.artifact_store.sealed_root.iterdir()) == ()
    assert scenario.commit_spool.pending() == ()
    assert pandas_attempts == []


def test_finalizer_applies_final_artifact_payload_budget(
    tmp_path: Path,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    finalizer = LabFinalizer(
        reader=LabJobReader(scenario.store.path),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
        job_limits=LabFinalizerJobLimits(
            max_final_artifact_payload_bytes=512,
            max_final_artifact_single_payload_bytes=512,
        ),
    )

    with pytest.raises(
        LabFinalizationResourceLimitError,
        match="final artifact payload",
    ):
        finalizer.finalize(scenario.job_id)

    assert tuple(scenario.artifact_store.candidates_root.iterdir()) == ()
    assert scenario.commit_spool.pending() == ()


def test_artifact_roundtrip_peak_usage_accounts_for_all_live_copies() -> None:
    usage = LabArtifactRoundtripPeakUsage(
        source_dataframe_bytes=25 * 1024 * 1024,
        roundtrip_dataframe_bytes=25 * 1024 * 1024,
        arrow_working_bytes=25 * 1024 * 1024,
        payload_bytes=4 * 1024 * 1024,
        payload_copy_bytes=4 * 1024 * 1024,
        hash_scratch_bytes=128 * 1024,
    )

    assert usage.peak_resident_bytes == (83 * 1024 * 1024) + (128 * 1024)


def test_finalizer_rejects_roundtrip_peak_before_artifact_serialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    first = snapshot.shards[0].shard
    value = "x" * (25 * 1024)
    frame = pd.DataFrame({"wide": [value] * 1024})
    result = LabJobExecutionResult(
        spec_hash=snapshot.job.spec_hash,
        plan_hash=first.plan_hash,
        adapter_id=first.adapter_id,
        adapter_version=first.adapter_version,
        tables=(LabShardTable(name="trades", frame=frame),),
    )
    finalizer = LabFinalizer(
        reader=LabJobReader(scenario.store.path),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
        job_limits=LabFinalizerJobLimits(
            max_aggregate_dataframe_bytes=30 * 1024 * 1024,
            max_final_artifact_payload_bytes=4 * 1024 * 1024,
            max_final_artifact_single_payload_bytes=4 * 1024 * 1024,
            max_peak_resident_bytes=50 * 1024 * 1024,
        ),
    )
    monkeypatch.setattr(
        finalizer.adapter_registry,
        "aggregate_results",
        lambda _spec, _results: result,
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("roundtrip peak overflow reached artifact serialization")

    monkeypatch.setattr(finalizer.artifact_store, "preview_candidate", forbidden)

    with pytest.raises(
        LabFinalizationResourceLimitError,
        match="artifact roundtrip peak resident bytes",
    ):
        finalizer.finalize(scenario.job_id)


def test_finalizer_rejects_oversized_manifest_before_read_or_model_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    changed = LabShardResultManifest.model_validate(
        manifest.model_copy(
            update={"metrics": (LabShardMetric(name="oversized", value="x" * (2 * 1024 * 1024)),)}
        )
    )
    _persist_attempt_manifest(attempt, changed)
    accepted = _evidence_for_manifest(evidence, changed)
    tampered = snapshot.model_copy(update={"shards": (accepted,)})
    finalizer = LabFinalizer(
        reader=_TamperedBundleSnapshotReader(tampered),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
        job_limits=LabFinalizerJobLimits(max_peak_resident_bytes=512 * 1024),
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("oversized manifest reached Pydantic model parsing")

    monkeypatch.setattr(LabShardResultManifest, "model_validate_json", forbidden)
    tracemalloc.start()
    with pytest.raises(LabFinalizationResourceLimitError, match="manifest.*peak"):
        finalizer.finalize(scenario.job_id)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert peak <= 1024 * 1024


@pytest.mark.parametrize("resource", ["count", "value"])
def test_bundle_reader_preflights_metric_resources_before_model_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    resource: str,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    if resource == "count":
        metrics = (
            LabShardMetric(name="first", value=1),
            LabShardMetric(name="second", value=2),
        )
        limits = LabShardBundleLimits(max_metric_count=1)
        message = "metric count"
    else:
        metrics = (LabShardMetric(name="wide", value="x" * 4096),)
        limits = LabShardBundleLimits(max_metric_value_bytes=128)
        message = "metric value"
    changed = LabShardResultManifest.model_validate(
        manifest.model_copy(update={"metrics": metrics})
    )
    _persist_attempt_manifest(attempt, changed)
    accepted = _evidence_for_manifest(evidence, changed)

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("oversized metrics reached Pydantic model parsing")

    monkeypatch.setattr(LabShardResultManifest, "model_validate_json", forbidden)
    with pytest.raises(LabFinalizationResourceLimitError, match=message):
        LabSealedShardBundleReader(
            tmp_path / "artifacts",
            limits=limits,
        ).inspect(accepted)


def test_finalizer_budgets_snapshot_control_before_bundle_inspection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    changed_shard = evidence.shard.model_copy(update={"payload_json": "x" * (1024 * 1024)})
    tampered = snapshot.model_copy(
        update={"shards": (evidence.model_copy(update={"shard": changed_shard}),)}
    )
    finalizer = LabFinalizer(
        reader=_TamperedBundleSnapshotReader(tampered),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
        job_limits=LabFinalizerJobLimits(max_peak_resident_bytes=512 * 1024),
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("oversized snapshot reached bundle inspection")

    monkeypatch.setattr(finalizer.bundle_reader, "inspect", forbidden)
    with pytest.raises(LabFinalizationResourceLimitError, match="snapshot control"):
        finalizer.finalize(scenario.job_id)


def test_finalizer_reserves_snapshot_bytes_during_manifest_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    changed_manifest = LabShardResultManifest.model_validate(
        manifest.model_copy(
            update={"metrics": (LabShardMetric(name="wide", value="x" * (128 * 1024)),)}
        )
    )
    _persist_attempt_manifest(attempt, changed_manifest)
    accepted = _evidence_for_manifest(evidence, changed_manifest)
    changed_shard = accepted.shard.model_copy(update={"payload_json": "x" * (1024 * 1024)})
    tampered = snapshot.model_copy(
        update={"shards": (accepted.model_copy(update={"shard": changed_shard}),)}
    )
    finalizer = LabFinalizer(
        reader=_TamperedBundleSnapshotReader(tampered),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
        bundle_limits=LabShardBundleLimits(max_metric_value_bytes=256 * 1024),
        job_limits=LabFinalizerJobLimits(max_peak_resident_bytes=2 * 1024 * 1024),
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("combined snapshot and manifest peak reached model parsing")

    monkeypatch.setattr(LabShardResultManifest, "model_validate_json", forbidden)
    with pytest.raises(LabFinalizationResourceLimitError, match="manifest.*peak"):
        finalizer.finalize(scenario.job_id)


@pytest.mark.parametrize("resource", ["rows", "columns"])
def test_bundle_reader_rejects_manifest_shape_limits_before_parquet_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    resource: str,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]
    attempt = _attempt_path(tmp_path, evidence)
    manifest = LabShardResultManifest.model_validate_json((attempt / "manifest.json").read_bytes())
    artifact = manifest.artifacts[0]
    if resource == "rows":
        changed_artifact = artifact.model_copy(update={"row_count": 2})
        limits = LabShardBundleLimits(max_row_count=1)
        message = "row count.*limit"
    else:
        changed_artifact = artifact.model_copy(
            update={"columns": (*artifact.columns, "unsafe_extra_column")}
        )
        limits = LabShardBundleLimits(max_column_count=len(artifact.columns))
        message = "column count.*limit"
    changed = LabShardResultManifest.model_validate(
        manifest.model_copy(update={"artifacts": (changed_artifact,)})
    )
    _persist_attempt_manifest(attempt, changed)
    accepted = _evidence_for_manifest(evidence, changed)

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("manifest shape limit reached Parquet metadata")

    monkeypatch.setattr(lab_finalizer_module.pq, "ParquetFile", forbidden)

    with pytest.raises(LabFinalizationIntegrityError, match=message):
        LabSealedShardBundleReader(
            tmp_path / "artifacts",
            limits=limits,
        ).read(accepted)


def test_bundle_reader_rejects_materialized_dataframe_memory_before_content_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    snapshot = LabJobReader(scenario.store.path).get_finalization_snapshot(scenario.job_id)
    assert snapshot is not None
    evidence = snapshot.shards[0]

    def forbidden(_frame: pd.DataFrame) -> str:
        raise AssertionError("oversized DataFrame reached canonical content hashing")

    monkeypatch.setattr(lab_finalizer_module, "canonical_shard_frame_digest", forbidden)

    with pytest.raises(LabFinalizationIntegrityError, match="DataFrame.*memory limit"):
        LabSealedShardBundleReader(
            tmp_path / "artifacts",
            limits=LabShardBundleLimits(max_materialized_dataframe_bytes=1),
        ).read(evidence)


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
        reader=_TamperedBundleSnapshotReader(changed_snapshot),  # type: ignore[arg-type]
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=scenario.artifact_store,
        commit_spool=scenario.commit_spool,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: "1" * 40,
        finalizer_authority_key_provider=_authority_key_provider,
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
