from __future__ import annotations

import builtins
import inspect
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import pytest

from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabReportSpool,
    LabShardClaim,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabWorkerStopped,
)
from rquant.research_run_spec import DatasetSnapshotIdentity, ResearchRunSpec
from rquant.strategy_job_adapters import (
    LabShardExecutionResult,
    LabShardTable,
    ValidatedStrategyShard,
    default_strategy_job_adapter_registry,
)
from tests.unit.test_strategy_job_adapters import _claim, _nshape_compare_spec

NOW = datetime(2026, 7, 24, 0, 1, tzinfo=UTC)


@contextmanager
def _store_factory(store: object = object()) -> Iterator[object]:
    yield store


class RecordingRegistry:
    def __init__(
        self,
        *,
        delay_seconds: float = 0.0,
        failure: BaseException | None = None,
    ) -> None:
        self.delegate = default_strategy_job_adapter_registry()
        self.delay_seconds = delay_seconds
        self.failure = failure
        self.executions = 0
        self.stores: list[object] = []

    def validate_claim(self, claim: LabShardClaim) -> ValidatedStrategyShard:
        return self.delegate.validate_claim(claim)

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        self.executions += 1
        self.stores.append(store)
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        if self.failure is not None:
            raise self.failure
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(
                    name="trades",
                    frame=pd.DataFrame([{"hold_days": validated.shard.hold_days, "ret_pct": 1.25}]),
                ),
            ),
        )


def _worker(
    tmp_path: Path,
    *,
    worker_id: str = "worker-a",
    registry: RecordingRegistry | None = None,
    claims: LabClaimSpool | None = None,
    reports: LabReportSpool | None = None,
    heartbeat_interval_seconds: float = 60.0,
    exploratory_store_factory=_store_factory,
    metadata_store_factory=None,
    lake_root: Path | None = None,
):
    from rquant.lab_worker import LabWorker

    return LabWorker(
        worker_id=worker_id,
        claim_spool=claims or LabClaimSpool(tmp_path / "claims"),
        report_spool=reports or LabReportSpool(tmp_path / "reports"),
        artifact_root=tmp_path / "artifacts",
        adapter_registry=registry or RecordingRegistry(),
        exploratory_store_factory=exploratory_store_factory,
        metadata_store_factory=metadata_store_factory,
        research_lake_root=lake_root,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        lease_extension_seconds=30,
        poll_interval_ms=5,
        clock=lambda: NOW,
    )


def _retry_claim(claim: LabShardClaim) -> LabShardClaim:
    return LabShardClaim(
        job_id=claim.job_id,
        spec_hash=claim.spec_hash,
        definition=claim.definition,
        worker_id=claim.worker_id,
        claim_token=uuid4(),
        claim_generation=claim.claim_generation + 1,
        scheduler_fencing_token=claim.scheduler_fencing_token,
        claimed_at=claim.claimed_at + timedelta(minutes=1),
        lease_expires_at=claim.lease_expires_at + timedelta(minutes=1),
    )


def _reports(spool: LabReportSpool):
    return tuple(entry.report for entry in spool.pending())


def test_worker_consumes_only_its_owned_unexpired_claim(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    wrong = _claim(_nshape_compare_spec()).model_copy(update={"worker_id": "worker-b"})
    owned = _claim(_nshape_compare_spec())
    claims.publish(wrong)
    claims.publish(owned)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )

    result = worker.run_once()

    assert result.status == "succeeded"
    assert registry.executions == 1
    assert [entry.claim for entry in claims.pending()] == [wrong]
    report = _reports(reports)[-1]
    assert isinstance(report.body, LabShardSucceeded)
    assert report.job_id == owned.job_id
    assert report.spec_hash == owned.spec_hash
    assert report.payload_hash == owned.payload_hash
    assert report.claim_generation == owned.claim_generation
    assert report.scheduler_fencing_token == owned.scheduler_fencing_token
    assert report.claim_token == owned.claim_token
    assert report.worker_id == owned.worker_id


def test_worker_leaves_expired_claim_for_lease_recovery(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    expired = _claim(_nshape_compare_spec()).model_copy(
        update={
            "claimed_at": NOW - timedelta(minutes=2),
            "lease_expires_at": NOW - timedelta(minutes=1),
        }
    )
    claims.publish(expired)
    worker = _worker(tmp_path, registry=registry, claims=claims)

    result = worker.run_once()

    assert result.status == "idle"
    assert registry.executions == 0
    assert [entry.claim for entry in claims.pending()] == [expired]


def test_worker_skips_superseded_claim_without_consuming_it(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    stale = _claim(_nshape_compare_spec())
    fresh = _retry_claim(stale)
    claims.publish(stale)
    claims.publish(fresh)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )

    worker.run_once()

    assert [entry.claim for entry in claims.pending()] == [stale]
    assert registry.executions == 1
    assert _reports(reports)[-1].claim_token == fresh.claim_token


def test_worker_heartbeats_during_long_shard_then_succeeds(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(delay_seconds=0.06),
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.01,
    )

    worker.run_once()

    bodies = tuple(report.body for report in _reports(reports))
    assert any(isinstance(body, LabShardHeartbeat) for body in bodies)
    assert isinstance(bodies[-1], LabShardSucceeded)


def test_worker_reports_typed_failure_without_sealing(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(failure=RuntimeError("fixture failed")),
        claims=claims,
        reports=reports,
    )

    result = worker.run_once()

    assert result.status == "failed"
    report = _reports(reports)[-1]
    assert isinstance(report.body, LabShardFailed)
    assert "fixture failed" in report.body.failure_json
    assert not worker.sealed_bundle_path(claim).exists()


def test_stop_before_execution_reports_worker_stopped(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )
    worker.request_stop()

    result = worker.run_once()

    assert result.status == "stopped"
    assert registry.executions == 0
    assert isinstance(_reports(reports)[-1].body, LabWorkerStopped)


def test_bundle_is_atomic_canonical_and_idempotently_reused(tmp_path: Path) -> None:
    from rquant.lab_worker import LabShardResultManifest

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )

    first = worker.run_once()
    sealed = worker.sealed_bundle_path(claim)
    manifest_path = sealed / "manifest.json"
    manifest = LabShardResultManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    claims.publish(_retry_claim(claim))
    second = worker.run_once()

    assert first.manifest_hash == manifest.manifest_hash
    assert second.manifest_hash == first.manifest_hash
    assert manifest_path.read_text(encoding="utf-8") == manifest.canonical_json()
    assert (sealed / manifest.artifacts[0].file_name).is_file()
    assert registry.executions == 1
    assert not tuple((tmp_path / "artifacts" / ".tmp").rglob("*"))


def test_conflicting_sealed_bundle_fails_closed(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )
    worker.run_once()
    (worker.sealed_bundle_path(claim) / "manifest.json").write_text("{}", encoding="utf-8")
    claims.publish(_retry_claim(claim))

    result = worker.run_once()

    assert result.status == "failed"
    assert registry.executions == 1
    assert isinstance(_reports(reports)[-1].body, LabShardFailed)


class _MetadataStore:
    def __init__(self, identity: DatasetSnapshotIdentity) -> None:
        self.snapshot = SimpleNamespace(
            snapshot_id=identity.snapshot_id,
            status="ready",
        )
        self.binding = SimpleNamespace(
            snapshot_id=identity.snapshot_id,
            binding_hash=identity.binding_hash,
            status="ready",
        )
        self.audit = SimpleNamespace(
            audit_run_id=identity.audit_run_id,
            status="completed",
        )

    def get_dataset_snapshot(self, snapshot_id: str):
        return self.snapshot if snapshot_id == self.snapshot.snapshot_id else None

    def get_dataset_snapshot_binding(self, snapshot_id: str):
        return self.binding if snapshot_id == self.binding.snapshot_id else None

    def get_data_audit_run(self, audit_run_id: str):
        return self.audit if audit_run_id == self.audit.audit_run_id else None


def _formal_spec() -> ResearchRunSpec:
    identity = DatasetSnapshotIdentity(
        snapshot_id="a" * 64,
        binding_hash="b" * 64,
        audit_run_id="c" * 64,
    )
    return _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={
            "dataset_snapshot": identity,
            "research_status": "comparable",
        }
    )


def test_formal_job_opens_verified_research_execution_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    spec = _formal_spec()
    identity = spec.dataset_snapshot
    assert identity is not None
    metadata = _MetadataStore(identity)
    opened: list[tuple[object, Path]] = []
    session_store = object()

    class FakeResearchExecutionSession:
        def __init__(self, *, binding: object, lake_root: Path) -> None:
            opened.append((binding, lake_root))

        def __enter__(self) -> object:
            return session_store

        def __exit__(self, *_: object) -> None:
            return None

    @contextmanager
    def metadata_factory() -> Iterator[object]:
        yield metadata

    monkeypatch.setattr(
        lab_worker,
        "ResearchExecutionSession",
        FakeResearchExecutionSession,
    )
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    claims.publish(_claim(spec))
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        exploratory_store_factory=None,
        metadata_store_factory=metadata_factory,
        lake_root=tmp_path / "lake",
    )

    result = worker.run_once()

    assert result.status == "succeeded"
    assert opened == [(metadata.binding, tmp_path / "lake")]
    assert registry.stores == [session_store]


def test_formal_snapshot_identity_mismatch_fails_before_execution(tmp_path: Path) -> None:
    spec = _formal_spec()
    identity = spec.dataset_snapshot
    assert identity is not None
    metadata = _MetadataStore(identity)
    metadata.binding.binding_hash = "d" * 64

    @contextmanager
    def metadata_factory() -> Iterator[object]:
        yield metadata

    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    claims.publish(_claim(spec))
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        exploratory_store_factory=None,
        metadata_store_factory=metadata_factory,
        lake_root=tmp_path / "lake",
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert registry.executions == 0


def test_worker_module_has_no_control_db_network_or_notification_imports() -> None:
    import rquant.lab_worker as lab_worker
    import rquant.strategy_job_adapters as adapters

    source = (inspect.getsource(lab_worker) + inspect.getsource(adapters)).lower()
    forbidden = (
        "lab_jobs",
        "sqlite3",
        "duckdbstore",
        "tushare",
        "akshare",
        "ashare",
        "mootdx",
        "notifier",
        "rquant.notify",
    )

    assert not [name for name in forbidden if name in source]


def test_worker_execution_runtime_blocks_provider_and_notification_imports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claims.publish(_claim(_nshape_compare_spec()))
    original_import = builtins.__import__
    forbidden_prefixes = (
        "rquant.adapter",
        "rquant.notify",
        "tushare",
        "akshare",
        "ashare",
        "mootdx",
    )

    def guarded_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name.lower().startswith(forbidden_prefixes):
            raise AssertionError(f"forbidden worker import: {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    worker = _worker(tmp_path, claims=claims, reports=reports)

    result = worker.run_once()

    assert result.status == "succeeded"
    assert isinstance(_reports(reports)[-1].body, LabShardSucceeded)


def test_crash_without_report_is_reclaimed_by_existing_lease_recovery(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler

    class WorkerCrash(BaseException):
        pass

    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spec = _nshape_compare_spec(hold_days=(1,))
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(job_id=uuid4(), spec=spec, max_attempts=2),
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
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    original = claims.pending()[0].claim
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(failure=WorkerCrash()),
        claims=claims,
        reports=reports,
    )

    with pytest.raises(WorkerCrash):
        worker.run_once()

    assert claims.pending() == ()
    assert reports.pending() == ()
    clock[0] = NOW + timedelta(seconds=21)
    result = scheduler.run_once()
    recovered = claims.pending()[0].claim

    assert result.recovered == 1
    assert recovered.shard_id == original.shard_id
    assert recovered.claim_generation == original.claim_generation + 1
    assert recovered.claim_token != original.claim_token
