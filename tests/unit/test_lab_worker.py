from __future__ import annotations

import builtins
import inspect
import json
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import pytest

from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabReportReceipt,
    LabReportSpool,
    LabShardClaim,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabWorkerReport,
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


def _accept_report(
    report: LabWorkerReport,
    _timeout_seconds: float,
    _stop: object,
) -> LabReportReceipt:
    return LabReportReceipt.from_report(
        report,
        status="accepted",
        reason=f"accepted:{report.body.report_type}",
        accepted_at=NOW,
    )


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

    def for_spec(self, spec: ResearchRunSpec):
        return self.delegate.for_spec(spec)

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
    lease_extension_seconds: int = 30,
    receipt_timeout_seconds: float = 0.2,
    exploratory_store_factory=_store_factory,
    metadata_store_factory=None,
    lake_root: Path | None = None,
    receipt_waiter=_accept_report,
    verified_code_sha_provider=lambda: "1" * 40,
    clock=lambda: NOW,
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
        lease_extension_seconds=lease_extension_seconds,
        poll_interval_ms=5,
        receipt_timeout_seconds=receipt_timeout_seconds,
        receipt_waiter=receipt_waiter,
        verified_code_sha_provider=verified_code_sha_provider,
        clock=clock,
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


def _sigterm_publication_child(root_value: str, phase: str) -> None:
    root = Path(root_value)
    claims = LabClaimSpool(root / "claims")
    reports = LabReportSpool(root / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker_ref: list[object] = []

    def receipt_waiter(
        report: LabWorkerReport,
        timeout_seconds: float,
        stop: object,
    ) -> LabReportReceipt:
        if phase == "after" and isinstance(report.body, LabShardSucceeded):
            worker = worker_ref[0]
            return worker._wait_for_receipt(report, timeout_seconds, stop)
        return _accept_report(report, timeout_seconds, stop)

    worker = _worker(
        root,
        claims=claims,
        reports=reports,
        receipt_waiter=receipt_waiter,
    )
    worker_ref.append(worker)
    original_publish = reports.publish
    signalled = False

    def signal_at_boundary(report: LabWorkerReport) -> object:
        nonlocal signalled
        before = phase == "before" and isinstance(report.body, LabShardHeartbeat)
        after = phase == "after" and isinstance(report.body, LabShardSucceeded)
        if not signalled and (before or after):
            signalled = True
            os.kill(os.getpid(), signal.SIGTERM)
        return original_publish(report)

    reports.publish = signal_at_boundary  # type: ignore[method-assign]
    signal.signal(signal.SIGTERM, lambda _signum, _frame: worker.request_stop())
    result = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))
    (root / "result.json").write_text(
        json.dumps(
            {
                "failed": sum(isinstance(body, LabShardFailed) for body in bodies),
                "sealed": worker.sealed_bundle_path(claim).exists(),
                "status": result.status,
                "stopped": sum(isinstance(body, LabWorkerStopped) for body in bodies),
                "succeeded": sum(isinstance(body, LabShardSucceeded) for body in bodies),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _crash_after_atomic_rename_child(root_value: str) -> None:
    root = Path(root_value)
    claims = LabClaimSpool(root / "claims")
    reports = LabReportSpool(root / "reports")
    worker = _worker(root, claims=claims, reports=reports)
    original_publish = reports.publish

    def crash_before_success_publish(report: LabWorkerReport) -> object:
        if isinstance(report.body, LabShardSucceeded):
            os._exit(77)
        return original_publish(report)

    reports.publish = crash_before_success_publish  # type: ignore[method-assign]
    worker.run_once()
    os._exit(78)


def _run_worker_child(
    helper_name: str,
    root: Path,
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    source = (
        f"from tests.unit.test_lab_worker import {helper_name}; "
        f"{helper_name}(*__import__('sys').argv[1:])"
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(Path(__file__).parents[2])
    process = subprocess.Popen(
        [sys.executable, "-c", source, str(root), *arguments],
        cwd=Path(__file__).parents[2],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=4)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate(timeout=1)
        pytest.fail(
            f"worker child timed out: stdout={stdout!r} stderr={stderr!r}"
        )
    return subprocess.CompletedProcess(
        process.args,
        process.returncode,
        stdout,
        stderr,
    )


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

    assert claims.pending() == ()
    assert len(tuple(claims.quarantine_dir.glob("*.json"))) == 1
    assert registry.executions == 1
    assert _reports(reports)[-1].claim_token == fresh.claim_token


def test_consumed_new_generation_prevents_old_claim_resurrection(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    stale = _claim(_nshape_compare_spec(hold_days=(1,)))
    fresh = _retry_claim(stale).model_copy(update={"worker_id": "worker-b"})
    claims.publish(stale)
    claims.consume(claims.publish(fresh))

    result = _worker(
        tmp_path,
        worker_id="worker-a",
        registry=registry,
        claims=claims,
        reports=reports,
    ).run_once()

    assert result.status == "idle"
    assert registry.executions == 0
    assert reports.pending() == ()
    assert claims.pending() == ()
    assert len(tuple(claims.quarantine_dir.glob("*.json"))) == 1
    assert LabClaimSpool(claims.root).current(stale.job_id, stale.shard_id).claim == fresh


def test_worker_fails_closed_when_claim_high_water_marker_is_missing(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    (claims.current_dir / f"{claim.job_id}.{claim.shard_id}.json").unlink()

    result = _worker(tmp_path, registry=registry, claims=claims).run_once()

    assert result.status == "idle"
    assert registry.executions == 0
    assert [entry.claim for entry in claims.pending()] == [claim]


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


def test_background_heartbeat_covers_candidate_serialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.01,
    )
    original_write = worker._write_bundle

    def slow_write(*args: object, **kwargs: object):
        time.sleep(0.05)
        return original_write(*args, **kwargs)

    monkeypatch.setattr(worker, "_write_bundle", slow_write)

    result = worker.run_once()

    heartbeats = [
        report for report in _reports(reports) if isinstance(report.body, LabShardHeartbeat)
    ]
    assert result.status == "succeeded"
    assert len(heartbeats) >= 2  # periodic during candidate write, then synchronous final fence


def test_slow_candidate_loses_one_second_lease_to_new_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler

    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=uuid4(),
                spec=_nshape_compare_spec(hold_days=(1,)),
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
        shard_lease_seconds=1,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    original = claims.pending()[0].claim
    published: list[LabWorkerReport] = []
    original_publish = reports.publish

    def capture_publish(report: LabWorkerReport):
        published.append(report)
        return original_publish(report)

    monkeypatch.setattr(reports, "publish", capture_publish)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.05,
        lease_extension_seconds=1,
        receipt_timeout_seconds=0.5,
        receipt_waiter=None,
        clock=lambda: clock[0],
    )
    write_started = threading.Event()
    release_write = threading.Event()
    original_write = worker._write_bundle

    def slow_write(*args: object, **kwargs: object):
        write_started.set()
        assert release_write.wait(timeout=3)
        return original_write(*args, **kwargs)

    monkeypatch.setattr(worker, "_write_bundle", slow_write)
    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.append(worker.run_once()))
    thread.start()
    write_timeout = time.monotonic() + 1
    while not write_started.is_set() and time.monotonic() < write_timeout:
        scheduler.run_once()
        time.sleep(0.01)
    assert write_started.is_set()
    time.sleep(1.05)
    clock[0] = NOW + timedelta(seconds=2)
    recovery = scheduler.run_once()
    replacement = claims.pending()[0].claim
    release_write.set()
    timeout_at = time.monotonic() + 2
    while thread.is_alive() and time.monotonic() < timeout_at:
        scheduler.run_once()
        time.sleep(0.01)
    thread.join(timeout=0.2)
    scheduler.release()

    assert not thread.is_alive()
    assert recovery.recovered == 1
    assert replacement.claim_generation == original.claim_generation + 1
    assert outcomes[0].status == "failed"
    assert not worker.sealed_bundle_path(original).exists()
    assert not any(
        isinstance(report.body, LabShardSucceeded)
        and report.claim_token == original.claim_token
        for report in published
    )


def test_final_fence_rejection_prevents_seal_and_success(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)

    def reject_fence(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        return LabReportReceipt.from_report(
            report,
            status="rejected",
            reason="claim_generation_mismatch",
            accepted_at=NOW,
        )

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=reject_fence,
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert not worker.sealed_bundle_path(claim).exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_heartbeat_publish_failure_returns_failed_without_sealing(tmp_path: Path) -> None:
    class FailFirstHeartbeatSpool(LabReportSpool):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self.failed = False

        def publish(self, report: LabWorkerReport):
            if isinstance(report.body, LabShardHeartbeat) and not self.failed:
                self.failed = True
                raise OSError("injected heartbeat publish failure")
            return super().publish(report)

    claims = LabClaimSpool(tmp_path / "claims")
    reports = FailFirstHeartbeatSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(delay_seconds=0.06),
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.01,
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert isinstance(_reports(reports)[-1].body, LabShardFailed)
    assert not worker.sealed_bundle_path(claim).exists()


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


def test_worker_deadline_before_execute_fails_without_running_shard(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(update={"deadline": NOW})
    claim = _claim(spec)
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert registry.executions == 0
    assert not worker.sealed_bundle_path(claim).exists()


def test_worker_deadline_after_execute_prevents_fence_and_seal(tmp_path: Path) -> None:
    clock = [NOW]
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"deadline": NOW + timedelta(seconds=1)}
    )

    class DeadlineRegistry(RecordingRegistry):
        def execute_shard(
            self,
            validated: ValidatedStrategyShard,
            store: object,
        ) -> LabShardExecutionResult:
            result = super().execute_shard(validated, store)
            clock[0] = spec.deadline
            return result

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(spec)
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=DeadlineRegistry(),
        claims=claims,
        reports=reports,
        clock=lambda: clock[0],
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert not worker.sealed_bundle_path(claim).exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_worker_deadline_during_bundle_write_prevents_atomic_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [NOW]
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"deadline": NOW + timedelta(seconds=1)}
    )
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(spec)
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        clock=lambda: clock[0],
    )
    original_write = worker._write_bundle

    def write_across_deadline(*args: object, **kwargs: object):
        manifest = original_write(*args, **kwargs)
        clock[0] = spec.deadline
        return manifest

    monkeypatch.setattr(worker, "_write_bundle", write_across_deadline)

    result = worker.run_once()

    assert result.status == "failed"
    assert not worker.sealed_bundle_path(claim).exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_stop_triggered_at_atomic_rename_rolls_back_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(tmp_path, claims=claims, reports=reports)
    sealed = worker.sealed_bundle_path(claim)
    original_rename = lab_worker.os.rename

    def rename_then_stop(source: object, target: object) -> None:
        original_rename(source, target)
        if Path(target) == sealed:
            worker.request_stop()

    monkeypatch.setattr(lab_worker.os, "rename", rename_then_stop)

    result = worker.run_once()

    assert result.status == "stopped"
    assert not sealed.exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_deadline_triggered_at_atomic_rename_rolls_back_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    clock = [NOW]
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"deadline": NOW + timedelta(seconds=1)}
    )
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(spec)
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        clock=lambda: clock[0],
    )
    sealed = worker.sealed_bundle_path(claim)
    original_rename = lab_worker.os.rename

    def rename_then_expire(source: object, target: object) -> None:
        original_rename(source, target)
        if Path(target) == sealed:
            clock[0] = spec.deadline

    monkeypatch.setattr(lab_worker.os, "rename", rename_then_expire)

    result = worker.run_once()

    assert result.status == "failed"
    assert not sealed.exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_lease_expiry_at_atomic_rename_rolls_back_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        lease_extension_seconds=1,
        clock=lambda: clock[0],
    )
    sealed = worker.sealed_bundle_path(claim)
    original_rename = lab_worker.os.rename

    def rename_then_expire_lease(source: object, target: object) -> None:
        original_rename(source, target)
        if Path(target) == sealed:
            clock[0] = NOW + timedelta(seconds=1)

    monkeypatch.setattr(lab_worker.os, "rename", rename_then_expire_lease)

    result = worker.run_once()

    assert result.status == "failed"
    assert not sealed.exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_high_water_change_at_atomic_rename_rolls_back_old_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(tmp_path, claims=claims, reports=reports)
    sealed = worker.sealed_bundle_path(claim)
    replacement = _retry_claim(claim)
    original_rename = lab_worker.os.rename

    def rename_then_replace_claim(source: object, target: object) -> None:
        original_rename(source, target)
        if Path(target) == sealed:
            claims.publish(replacement)

    monkeypatch.setattr(lab_worker.os, "rename", rename_then_replace_claim)

    result = worker.run_once()

    assert result.status == "failed"
    assert claims.current(claim.job_id, claim.shard_id).claim == replacement
    assert not sealed.exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


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
    assert reports.pending() == ()
    assert [entry.claim for entry in claims.pending()] == [claim]


def test_stop_during_execution_reports_stopped_without_sealing(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(delay_seconds=0.08),
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.01,
    )
    outcomes: list[object] = []
    thread = threading.Thread(target=lambda: outcomes.append(worker.run_once()))
    thread.start()
    time.sleep(0.02)
    worker.request_stop()
    thread.join(timeout=2)

    assert outcomes[0].status == "stopped"
    assert isinstance(_reports(reports)[-1].body, LabWorkerStopped)
    assert not worker.sealed_bundle_path(claim).exists()


def test_bundle_is_canonical_and_obsolete_attempt_is_reclaimed_across_retry(
    tmp_path: Path,
) -> None:
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

    validated = worker.adapter_registry.validate_claim(claim)
    first_result = registry.execute_shard(validated, object())
    first = worker._seal_result(claim, first_result)
    sealed = worker.sealed_bundle_path(claim)
    manifest_path = sealed / "manifest.json"
    manifest = LabShardResultManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    assert first.manifest_hash == manifest.manifest_hash
    assert manifest_path.read_text(encoding="utf-8") == manifest.canonical_json()
    assert (sealed / manifest.artifacts[0].file_name).is_file()
    retry = _retry_claim(claim)
    claims.publish(retry)
    assert not sealed.exists()
    second = worker.run_once()

    assert second.manifest_hash != first.manifest_hash
    assert manifest.claim_token == claim.claim_token
    assert manifest.claim_generation == claim.claim_generation
    assert manifest.scheduler_fencing_token == claim.scheduler_fencing_token
    assert worker.sealed_bundle_path(retry) != sealed
    assert worker.sealed_bundle_path(retry).is_dir()
    assert registry.executions == 2
    assert not tuple((tmp_path / "artifacts" / ".tmp").rglob("*"))


def test_same_attempt_conflicting_result_fails_closed(tmp_path: Path) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker.adapter_registry.validate_claim(claim)

    def result(value: int) -> LabShardExecutionResult:
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(LabShardTable(name="trades", frame=pd.DataFrame([{"value": value}])),),
        )

    first = worker._seal_result(claim, result(1))
    with pytest.raises(Exception, match="conflict"):
        worker._seal_result(claim, result(2))
    same = worker._seal_result(claim, result(1))

    assert same.manifest_hash == first.manifest_hash
    persisted = pd.read_parquet(
        worker.sealed_bundle_path(claim) / first.artifacts[0].file_name
    )
    assert persisted["value"].tolist() == [1]


def test_concurrent_same_attempt_conflicting_results_have_one_atomic_winner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    workers = (_worker(tmp_path), _worker(tmp_path))
    validated = workers[0].adapter_registry.validate_claim(claim)
    sealed = workers[0].sealed_bundle_path(claim)
    rename_barrier = threading.Barrier(2)
    original_rename = lab_worker.os.rename

    def synchronized_rename(source: object, target: object) -> None:
        if Path(target) == sealed:
            rename_barrier.wait(timeout=1)
        original_rename(source, target)

    monkeypatch.setattr(lab_worker.os, "rename", synchronized_rename)
    outcomes: list[object] = []

    def seal(worker: object, value: int) -> None:
        result = LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(name="trades", frame=pd.DataFrame([{"value": value}])),
            ),
        )
        try:
            outcomes.append(worker._seal_result(claim, result))
        except Exception as exc:
            outcomes.append(exc)

    threads = (
        threading.Thread(target=seal, args=(workers[0], 1)),
        threading.Thread(target=seal, args=(workers[1], 2)),
    )
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert sum(isinstance(outcome, Exception) for outcome in outcomes) == 1
    assert any("conflict" in str(outcome) for outcome in outcomes if isinstance(outcome, Exception))
    persisted = pd.read_parquet(sealed / "000-trades.parquet")
    assert persisted["value"].tolist() in ([1], [2])


def test_bundle_validation_rejects_extra_unknown_file(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(tmp_path, claims=claims)
    assert worker.run_once().status == "succeeded"
    (worker.sealed_bundle_path(claim) / "extra.bin").write_bytes(b"unexpected")

    with pytest.raises(Exception, match="unexpected"):
        worker._validate_bundle(worker.sealed_bundle_path(claim), claim)


def test_new_generation_reclaims_only_obsolete_crash_temporary(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    new = _retry_claim(old)
    worker = _worker(tmp_path, claims=claims)
    obsolete = worker._temporary_bundle_path(old)
    obsolete.mkdir(parents=True)
    (obsolete / "partial.parquet").write_bytes(b"partial")
    current = worker._temporary_bundle_path(new)
    current.mkdir(parents=True)
    (current / "still-active").write_bytes(b"active")
    claims.publish(new)

    worker._reclaim_obsolete_temporaries(new)

    assert not obsolete.exists()
    assert (current / "still-active").read_bytes() == b"active"


def test_current_attempt_reclaims_known_crash_candidate_directory(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path, claims=claims)
    abandoned = worker._temporary_bundle_path(claim) / uuid4().hex
    abandoned.mkdir(parents=True)
    (abandoned / "partial.parquet").write_bytes(b"partial")
    claims.publish(claim)

    worker._reclaim_obsolete_temporaries(claim)

    assert not abandoned.exists()
    assert not worker._temporary_bundle_path(claim).exists()


def test_obsolete_temporary_symlink_is_rejected_without_following(tmp_path: Path) -> None:
    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    new = _retry_claim(old)
    worker = _worker(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("safe", encoding="utf-8")
    obsolete = worker._temporary_bundle_path(old)
    obsolete.parent.mkdir(parents=True)
    obsolete.symlink_to(outside, target_is_directory=True)

    with pytest.raises(Exception, match="symlink"):
        worker._reclaim_obsolete_temporaries(new)

    assert (outside / "keep").read_text(encoding="utf-8") == "safe"


def test_obsolete_temporary_parent_symlink_is_rejected_without_following(
    tmp_path: Path,
) -> None:
    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    new = _retry_claim(old)
    worker = _worker(tmp_path)
    outside = tmp_path / "outside-parent"
    outside.mkdir()
    temporary_base = tmp_path / "artifacts" / ".tmp"
    temporary_base.mkdir(parents=True)
    (temporary_base / str(old.job_id)).symlink_to(outside, target_is_directory=True)
    obsolete = worker._temporary_bundle_path(old)
    obsolete.mkdir(parents=True)
    (obsolete / "keep").write_text("safe", encoding="utf-8")

    with pytest.raises(Exception, match="symlink"):
        worker._reclaim_obsolete_temporaries(new)

    assert (obsolete / "keep").read_text(encoding="utf-8") == "safe"


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
    claims.publish(claim)

    result = worker.run_once()

    assert result.status == "failed"
    assert registry.executions == 2
    assert isinstance(_reports(reports)[-1].body, LabShardFailed)


class _MetadataStore:
    def __init__(self, identity: DatasetSnapshotIdentity) -> None:
        from rquant.data_quality import STAGE1_AUDIT_RULE_SET_VERSION

        eligibility_hash = "e" * 64
        as_of_time = datetime(2026, 2, 11, tzinfo=UTC)
        self.snapshot = SimpleNamespace(
            snapshot_id=identity.snapshot_id,
            status="ready",
            strategy_name="n_shape",
            as_of_time=as_of_time,
            code_commit="1" * 40,
            manifest_id="m" * 64,
            table_watermarks={
                "manifest_start_date": "2026-01-01",
                "manifest_end_date": "2026-02-10",
                "eligibility_resolution_hash": eligibility_hash,
            },
        )
        eligibility_artifact = SimpleNamespace(
            dataset_id="strategy_eligibility",
            table_name="strategy_eligibility",
            artifact_key=f"strategy_eligibility:{eligibility_hash}",
        )
        manifest = SimpleNamespace(
            strategy_name="n_shape",
            code_commit="1" * 40,
            as_of_time=as_of_time,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 2, 10),
            eligibility_resolution_hash=eligibility_hash,
            eligibility_expected_dates=100,
            eligibility_complete_dates=100,
            artifacts=(eligibility_artifact,),
        )
        self.binding = SimpleNamespace(
            snapshot_id=identity.snapshot_id,
            binding_hash=identity.binding_hash,
            status="ready",
            manifest=manifest,
        )
        self.audit = SimpleNamespace(
            audit_run_id=identity.audit_run_id,
            status="completed",
            rule_set_version=STAGE1_AUDIT_RULE_SET_VERSION,
            range_start=date(2026, 1, 1),
            range_end=date(2026, 2, 10),
            as_of_date=date(2026, 2, 10),
            p0_count=0,
        )
        self.coverages = tuple(
            SimpleNamespace(
                snapshot_id=identity.snapshot_id,
                coverage_scope=scope,
                expected_count=100,
                available_count=100,
                coverage_ratio=1.0,
            )
            for scope in ("eligibility", "baseline", "entry", "exit")
        )

    def get_dataset_snapshot(self, snapshot_id: str):
        return self.snapshot if snapshot_id == self.snapshot.snapshot_id else None

    def get_dataset_snapshot_binding(self, snapshot_id: str):
        return self.binding if snapshot_id == self.binding.snapshot_id else None

    def get_data_audit_run(self, audit_run_id: str):
        return self.audit if audit_run_id == self.audit.audit_run_id else None

    def list_dataset_coverages(self, snapshot_id: str):
        return self.coverages if snapshot_id == self.snapshot.snapshot_id else ()

    def list_open_data_quality_issues(self, **_kwargs: object):
        return ()


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


def test_formal_runtime_clean_code_sha_must_match_spec(tmp_path: Path) -> None:
    spec = _formal_spec()
    identity = spec.dataset_snapshot
    assert identity is not None

    @contextmanager
    def metadata_factory() -> Iterator[object]:
        yield _MetadataStore(identity)

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
        verified_code_sha_provider=lambda: "f" * 40,
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert registry.executions == 0


def test_formal_reuses_full_research_gate_evidence_checks(tmp_path: Path) -> None:
    spec = _formal_spec()
    identity = spec.dataset_snapshot
    assert identity is not None
    metadata = _MetadataStore(identity)
    metadata.snapshot.strategy_name = "wrong_strategy"
    metadata.audit.p0_count = 1

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


def test_success_receipt_timeout_stays_pending_without_failed_report(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
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
        clock=lambda: NOW,
    )
    scheduler.run_once()
    registry = RecordingRegistry()

    def delayed_success(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardSucceeded):
            raise TimeoutError("delayed success receipt")
        return _accept_report(report, _timeout_seconds, _stop)

    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
        receipt_waiter=delayed_success,
    )

    first = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))

    assert first.status == "awaiting_receipt"
    assert first.report_id is not None
    assert first.manifest_hash is not None
    assert sum(isinstance(body, LabShardSucceeded) for body in bodies) == 1
    assert not any(isinstance(body, LabShardFailed) for body in bodies)

    scheduler.run_once()
    worker.receipt_waiter = worker._wait_for_receipt
    second = worker.run_once()
    job = LabJobReader(store.path).get_job(job_id)
    scheduler.release()

    assert second.status == "succeeded"
    assert second.report_id == first.report_id
    assert registry.executions == 1
    assert job is not None and job.status is JobStatus.SUCCEEDED


def test_success_receipt_transport_error_is_unknown_without_failed_report(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)

    def unavailable_receipt(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardSucceeded):
            raise OSError("receipt channel unavailable")
        return _accept_report(report, _timeout_seconds, _stop)

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=unavailable_receipt,
    )

    result = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))

    assert result.status == "unknown"
    assert result.report_id is not None
    assert result.manifest_hash is not None
    assert sum(isinstance(body, LabShardSucceeded) for body in bodies) == 1
    assert not any(isinstance(body, LabShardFailed) for body in bodies)


def test_success_publish_failure_retries_same_report_without_reexecution(
    tmp_path: Path,
) -> None:
    class FailFirstSuccessSpool(LabReportSpool):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self.failed = False

        def publish(self, report: LabWorkerReport):
            if isinstance(report.body, LabShardSucceeded) and not self.failed:
                self.failed = True
                raise OSError("injected success publish failure")
            return super().publish(report)

    claims = LabClaimSpool(tmp_path / "claims")
    reports = FailFirstSuccessSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )

    first = worker.run_once()
    second = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))

    assert first.status == "unknown"
    assert second.status == "succeeded"
    assert first.report_id == second.report_id
    assert registry.executions == 1
    assert sum(isinstance(body, LabShardSucceeded) for body in bodies) == 1
    assert not any(isinstance(body, LabShardFailed) for body in bodies)


def test_stop_after_success_publish_keeps_single_reported_terminal(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = None

    def stop_after_success(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardSucceeded):
            assert worker is not None
            worker.request_stop()
            raise InterruptedError("stop after success publish")
        return _accept_report(report, _timeout_seconds, _stop)

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=stop_after_success,
    )

    result = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))

    assert result.status == "reported"
    assert sum(isinstance(body, LabShardSucceeded) for body in bodies) == 1
    assert not any(isinstance(body, LabShardFailed | LabWorkerStopped) for body in bodies)


@pytest.mark.parametrize(
    ("phase", "expected"),
    [
        (
            "before",
            {"failed": 0, "sealed": False, "status": "stopped", "stopped": 1, "succeeded": 0},
        ),
        (
            "after",
            {"failed": 0, "sealed": True, "status": "reported", "stopped": 0, "succeeded": 1},
        ),
    ],
)
def test_real_sigterm_never_deadlocks_success_publication_boundary(
    tmp_path: Path,
    phase: str,
    expected: dict[str, object],
) -> None:
    root = tmp_path / phase
    root.mkdir()

    completed = _run_worker_child("_sigterm_publication_child", root, phase)

    assert completed.returncode == 0, completed.stderr
    assert json.loads((root / "result.json").read_text(encoding="utf-8")) == expected


def test_rejected_success_receipt_returns_failed_without_second_terminal_report(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)

    def reject_success(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        return LabReportReceipt.from_report(
            report,
            status="rejected" if isinstance(report.body, LabShardSucceeded) else "accepted",
            reason="stale_success" if isinstance(report.body, LabShardSucceeded) else "accepted",
            accepted_at=NOW,
        )

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=reject_success,
    )

    result = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))

    assert result.status == "failed"
    assert sum(isinstance(body, LabShardSucceeded) for body in bodies) == 1
    assert not any(isinstance(body, LabShardFailed) for body in bodies)
    assert not worker.sealed_bundle_path(claim).exists()


def test_worker_waits_for_real_scheduler_receipts_before_completion(tmp_path: Path) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
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
        clock=lambda: NOW,
    )
    scheduler.run_once()
    claim = claims.pending()[0].claim
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=None,
    )
    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.append(worker.run_once()))

    thread.start()
    timeout_at = time.monotonic() + 2
    while thread.is_alive() and time.monotonic() < timeout_at:
        scheduler.run_once()
        time.sleep(0.01)
    thread.join(timeout=0.2)
    scheduler.release()

    assert not thread.is_alive()
    assert outcomes[0].status == "succeeded"
    assert worker.sealed_bundle_path(claim).is_dir()
    job = LabJobReader(store.path).get_job(job_id)
    assert job is not None
    assert job.status is JobStatus.SUCCEEDED
    receipts = tuple(
        reports.load_receipt(path)
        for path in sorted(reports.ack_dir.glob("*.json"))
    )
    assert len(receipts) == 2
    assert all(receipt.status == "accepted" for receipt in receipts)


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


def test_hard_crash_after_rename_is_reclaimed_before_generation_two_runs(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_worker import LabArtifactReclaimer

    clock = [NOW]
    artifact_root = tmp_path / "artifacts"
    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=reports,
    )
    claims = LabClaimSpool(
        tmp_path / "claims",
        claim_advance_hook=reclaimer.reclaim,
    )
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
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
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    generation_one = claims.pending()[0].claim
    crashed = _run_worker_child("_crash_after_atomic_rename_child", tmp_path)
    sealed_one = reclaimer.sealed_bundle_path(generation_one)

    assert crashed.returncode == 77, crashed.stderr
    assert sealed_one.is_dir()

    clock[0] = NOW + timedelta(seconds=21)
    recovery = scheduler.run_once()
    generation_two = claims.pending()[0].claim

    assert recovery.recovered == 1
    assert generation_two.claim_generation == generation_one.claim_generation + 1
    assert not sealed_one.exists()

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=None,
        clock=lambda: clock[0],
    )
    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.append(worker.run_once()))
    thread.start()
    timeout_at = time.monotonic() + 3
    while thread.is_alive() and time.monotonic() < timeout_at:
        scheduler.run_once()
        time.sleep(0.01)
    thread.join(timeout=0.2)
    job = LabJobReader(store.path).get_job(job_id)
    scheduler.release()

    assert not thread.is_alive()
    assert outcomes[0].status == "succeeded"
    assert worker.sealed_bundle_path(generation_two).is_dir()
    assert job is not None and job.status is JobStatus.SUCCEEDED


def test_pending_success_evidence_blocks_obsolete_sealed_reclamation(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    claims = LabClaimSpool(
        tmp_path / "claims",
        claim_advance_hook=reclaimer.reclaim,
    )
    generation_one = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(generation_one)
    worker = _worker(tmp_path, claims=claims, reports=reports)
    validated = worker.adapter_registry.validate_claim(generation_one)
    result = RecordingRegistry().execute_shard(validated, object())
    manifest = worker._seal_result(generation_one, result)
    success = LabWorkerReport.from_claim(
        generation_one,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    reports.publish(success)

    with pytest.raises(LabArtifactConflictError, match="pending success"):
        claims.publish(_retry_claim(generation_one))

    assert claims.current(generation_one.job_id, generation_one.shard_id).claim == generation_one
    assert worker.sealed_bundle_path(generation_one).is_dir()


def test_rejected_success_receipt_allows_obsolete_sealed_reclamation(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    generation_one = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path, reports=reports)
    validated = worker.adapter_registry.validate_claim(generation_one)
    result = RecordingRegistry().execute_shard(validated, object())
    manifest = worker._seal_result(generation_one, result)
    success = LabWorkerReport.from_claim(
        generation_one,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    entry = reports.publish(success)
    reports.ack(
        entry,
        LabReportReceipt.from_report(
            success,
            status="rejected",
            reason="claim_generation_mismatch",
            accepted_at=NOW,
        ),
    )

    reclaimer.reclaim(_retry_claim(generation_one))

    assert not worker.sealed_bundle_path(generation_one).exists()


def test_reclaimer_preserves_current_attempt_and_rejects_unsafe_entries(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    current = _retry_claim(_claim(_nshape_compare_spec(hold_days=(1,))))
    worker = _worker(tmp_path, reports=reports)
    validated = worker.adapter_registry.validate_claim(current)
    result = RecordingRegistry().execute_shard(validated, object())
    worker._seal_result(current, result)
    current_path = worker.sealed_bundle_path(current)

    reclaimer.reclaim(current)
    assert current_path.is_dir()

    attempts_root = current_path.parent
    unknown = attempts_root / "unknown-attempt"
    unknown.mkdir()
    with pytest.raises(LabArtifactConflictError, match="invalid temporary attempt"):
        reclaimer.reclaim(current)
    assert unknown.is_dir()
    unknown.rmdir()

    future = _retry_claim(current)
    future_path = reclaimer.sealed_bundle_path(future)
    future_path.mkdir()
    with pytest.raises(LabArtifactConflictError, match="future sealed attempt"):
        reclaimer.reclaim(current)
    assert future_path.is_dir()
    future_path.rmdir()

    outside = tmp_path / "outside-sealed"
    outside.mkdir()
    (outside / "keep").write_text("safe", encoding="utf-8")
    obsolete = current.model_copy(
        update={
            "claim_generation": current.claim_generation - 1,
            "claim_token": uuid4(),
        }
    )
    obsolete_path = reclaimer.sealed_bundle_path(obsolete)
    obsolete_path.symlink_to(outside, target_is_directory=True)
    with pytest.raises(LabArtifactConflictError, match="symlink"):
        reclaimer.reclaim(current)
    assert (outside / "keep").read_text(encoding="utf-8") == "safe"


def test_unread_accepted_success_receipt_preserves_terminal_artifact(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    clock = [NOW]
    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    claims = LabClaimSpool(
        tmp_path / "claims",
        claim_advance_hook=reclaimer.reclaim,
    )
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
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
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    generation_one = claims.pending()[0].claim

    def delay_success_receipt(
        report: LabWorkerReport,
        timeout_seconds: float,
        stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardSucceeded):
            raise TimeoutError("leave accepted receipt unread")
        return _accept_report(report, timeout_seconds, stop)

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=delay_success_receipt,
        clock=lambda: clock[0],
    )
    pending = worker.run_once()
    scheduler.run_once()
    job = LabJobReader(store.path).get_job(job_id)

    assert pending.status == "awaiting_receipt"
    assert job is not None and job.status is JobStatus.SUCCEEDED
    assert reports.ack_dir.joinpath(f"{pending.report_id}.json").is_file()
    with pytest.raises(LabArtifactConflictError, match="accepted success"):
        claims.publish(_retry_claim(generation_one))

    assert claims.current(generation_one.job_id, generation_one.shard_id).claim == generation_one
    assert worker.sealed_bundle_path(generation_one).is_dir()
    scheduler.release()
