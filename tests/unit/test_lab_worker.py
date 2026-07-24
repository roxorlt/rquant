from __future__ import annotations

import builtins
import inspect
import json
import os
import shutil
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
from typing import Literal
from uuid import UUID, uuid4

import pandas as pd
import pytest

from rquant.lab_job_protocol import InvalidCommandEnvelopeError
from rquant.lab_shard_protocol import (
    LabClaimRevokedError,
    LabClaimSpool,
    LabClaimSupersededError,
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
    quarantine_reconcile_interval_seconds: float = 300.0,
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
        quarantine_reconcile_interval_seconds=quarantine_reconcile_interval_seconds,
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


def _crash_reclaimer_after_tombstone_rename_child(
    root_value: str,
    claim_payload: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    root = Path(root_value)
    reclaimer = LabArtifactReclaimer(
        artifact_root=root / "artifacts",
        report_spool=LabReportSpool(root / "reports"),
    )
    original_delete = reclaimer._delete_isolated_tombstone

    def crash_before_tombstone_delete(path: Path, ledger: object) -> None:
        if path.name.startswith(".reclaim-"):
            os._exit(79)
        original_delete(path, ledger)

    reclaimer._delete_isolated_tombstone = crash_before_tombstone_delete  # type: ignore[method-assign]
    reclaimer.reclaim(LabShardClaim.model_validate_json(claim_payload))
    os._exit(80)


def _crash_reclaimer_after_prepared_ledger_child(
    root_value: str,
    claim_payload: str,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactReclaimer

    root = Path(root_value)
    reclaimer = LabArtifactReclaimer(
        artifact_root=root / "artifacts",
        report_spool=LabReportSpool(root / "reports"),
    )
    original_rename = lab_worker_module.os.rename

    def crash_before_isolation(source: Path, target: Path) -> None:
        if Path(target).name.startswith(".reclaim-v1-"):
            os._exit(81)
        original_rename(source, target)

    lab_worker_module.os.rename = crash_before_isolation
    reclaimer.reclaim(LabShardClaim.model_validate_json(claim_payload))
    os._exit(82)


def _crash_sealed_rollback_after_payload_isolation_child(
    root_value: str,
    claim_payload: str,
) -> None:
    from rquant.lab_worker import LabSealedShardBundle

    root = Path(root_value)
    claim = LabShardClaim.model_validate_json(claim_payload)
    worker = _worker(root)
    sealed = worker.sealed_bundle_path(claim)
    manifest = worker._validate_bundle(sealed, claim)
    device, inode = worker._bundle_file_identity(sealed)
    bundle = LabSealedShardBundle(
        path=sealed,
        manifest=manifest,
        created=True,
        device=device,
        inode=inode,
    )
    original_promote = worker.artifact_reclaimer._promote_garbage_bundle

    def crash_before_deferred_gc(source: Path, target: Path) -> None:
        if source.parent == worker.artifact_reclaimer.garbage_staging_dir:
            os._exit(83)
        original_promote(source, target)

    worker.artifact_reclaimer._promote_garbage_bundle = crash_before_deferred_gc  # type: ignore[method-assign]
    worker._rollback_sealed(claim, bundle)
    os._exit(84)


def _crash_sealed_rollback_after_prepared_phase_child(
    root_value: str,
    claim_payload: str,
    crash_phase: str,
) -> None:
    from rquant.lab_worker import LabSealedShardBundle

    root = Path(root_value)
    claim = LabShardClaim.model_validate_json(claim_payload)
    worker = _worker(root)
    sealed = worker.sealed_bundle_path(claim)
    manifest = worker._validate_bundle(sealed, claim)
    device, inode = worker._bundle_file_identity(sealed)
    bundle = LabSealedShardBundle(
        path=sealed,
        manifest=manifest,
        created=True,
        device=device,
        inode=inode,
    )
    reclaimer = worker.artifact_reclaimer
    method_by_phase = {
        "intent": "_write_prepared_intent",
        "staging": "_ensure_garbage_staging",
        "global_owner": "_ensure_global_garbage_owner",
        "bundle_owner": "_ensure_bundle_garbage_owner",
        "prepared_ledger": "_write_garbage_ledger",
    }
    method_name = method_by_phase[crash_phase]
    original = getattr(reclaimer, method_name)

    def crash_after_phase(*args, **kwargs):
        result = original(*args, **kwargs)
        state = kwargs.get("state")
        if len(args) > 1:
            state = args[1]
        if crash_phase != "prepared_ledger" or state == "prepared":
            os._exit(85)
        return result

    setattr(reclaimer, method_name, crash_after_phase)
    worker._rollback_sealed(claim, bundle)
    os._exit(86)


def _publish_report_child(root_value: str, report_payload: str) -> None:
    root = Path(root_value)
    report = LabWorkerReport.model_validate_json(report_payload)
    LabReportSpool(root / "reports").publish(report)


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
        pytest.fail(f"worker child timed out: stdout={stdout!r} stderr={stderr!r}")
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


def test_worker_does_not_rehash_large_quarantine_for_each_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    spec = _nshape_compare_spec(hold_days=(1,))
    for _ in range(2):
        claims.publish(_claim(spec))
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        quarantine_reconcile_interval_seconds=3_600,
    )
    hash_calls = 0

    def simulated_large_recovery(*, max_entries: int) -> None:
        nonlocal hash_calls
        assert max_entries == 16
        hash_calls += max_entries

    monkeypatch.setattr(
        worker.artifact_reclaimer,
        "recover_active",
        simulated_large_recovery,
    )

    first = worker.run_once()
    second = worker.run_once()

    assert first.status == "succeeded"
    assert second.status == "succeeded"
    assert hash_calls == 16
    assert registry.executions == 2


def test_unrelated_quarantine_recovery_failure_is_typed_and_does_not_block_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(tmp_path, claims=claims)

    def fail_recovery(*, max_entries: int) -> None:
        assert max_entries == 16
        raise RuntimeError("unrelated deferred quarantine is corrupt")

    monkeypatch.setattr(worker.artifact_reclaimer, "recover_active", fail_recovery)

    result = worker.run_once()

    assert result.status == "succeeded"
    assert len(result.health_warnings) == 1
    assert result.health_warnings[0].category == "quarantine_reconcile_failed"
    assert result.health_warnings[0].error_type == "RuntimeError"


@pytest.mark.parametrize("bundle_count", [1, 10, 40])
def test_bounded_quarantine_recovery_never_rehashes_deferred_payloads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bundle_count: int,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    for index in range(bundle_count):
        victim = tmp_path / "artifacts" / "cold" / f"result-{index:03d}.bin"
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.write_bytes(f"cold-{index}".encode())
        assert reclaimer.logical_quarantine_tree(victim, purpose=f"cold fixture {index}")

    inventory_calls = 0

    def reject_inventory(_path: Path) -> tuple[object, ...]:
        nonlocal inventory_calls
        inventory_calls += 1
        raise AssertionError("bounded recovery must not traverse deferred payload inventory")

    monkeypatch.setattr(reclaimer, "_garbage_inventory", reject_inventory)

    result = reclaimer.recover_active(max_entries=3)

    assert result.inspected == 0
    assert result.cold_metadata_checked == 1
    assert len(tuple(reclaimer.garbage_cold_intent_dir.iterdir())) == bundle_count
    assert inventory_calls == 0


def test_bounded_quarantine_recovery_is_fair_across_restarts(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    artifact_root = tmp_path / "artifacts"
    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=reports,
    )
    victims: list[Path] = []
    for index in range(5):
        victim = artifact_root / "active" / f"result-{index:03d}.bin"
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.write_bytes(f"active-{index}".encode())
        owner = reclaimer._garbage_owner(victim, purpose=f"active fixture {index}")
        reclaimer._write_prepared_intent(reclaimer._prepared_intent(owner))
        victims.append(victim)

    for _ in range(5):
        restarted = LabArtifactReclaimer(
            artifact_root=artifact_root,
            report_spool=LabReportSpool(tmp_path / "reports"),
        )
        result = restarted.recover_active(max_entries=1)
        assert result.inspected == 1

    assert all(not victim.exists() for victim in victims)
    assert restarted.quarantine_summary().bundle_count == 5


def test_bounded_quarantine_recovery_migrates_legacy_intent_once(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    artifact_root = tmp_path / "artifacts"
    reports = LabReportSpool(tmp_path / "reports")
    legacy = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=reports,
    )
    legacy.garbage_legacy_complete_path.unlink()
    victim = artifact_root / "legacy-active" / "result.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"legacy-active")
    owner = legacy._garbage_owner(victim, purpose="legacy active fixture")
    intent = legacy._prepared_intent(owner)
    legacy._prepared_intent_path(owner.garbage_id).write_text(
        intent.canonical_json(),
        encoding="utf-8",
    )

    restarted = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    result = restarted.recover_active(max_entries=1)

    assert result.inspected == 1
    assert result.reconciled == 1
    assert not victim.exists()
    assert tuple(restarted.garbage_active_intent_dir.iterdir()) == ()
    assert len(tuple(restarted.garbage_cold_intent_dir.iterdir())) == 1
    assert restarted.garbage_legacy_complete_path.is_file()


def test_damaged_cold_quarantine_warns_without_blocking_unrelated_claim(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    registry = RecordingRegistry()
    worker = _worker(tmp_path, claims=claims, registry=registry)
    victim = tmp_path / "artifacts" / "cold-damage" / "result.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"retained-cold-result")
    assert worker.artifact_reclaimer.logical_quarantine_tree(
        victim,
        purpose="cold health fixture",
    )
    deferred = next(worker.artifact_reclaimer.garbage_deferred_dir.iterdir())
    (deferred / "unexpected.bin").write_bytes(b"foreign-metadata")

    result = worker.run_once()

    assert result.status == "succeeded"
    assert registry.executions == 1
    assert len(result.health_warnings) == 1
    assert result.health_warnings[0].category == "quarantine_reconcile_failed"
    assert result.health_warnings[0].error_type == "LabArtifactConflictError"
    assert (deferred / "unexpected.bin").read_bytes() == b"foreign-metadata"


def test_success_receipt_timeout_emits_structured_worker_warning(tmp_path: Path) -> None:
    from loguru import logger

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)

    def delayed_success_receipt(
        report: LabWorkerReport,
        timeout_seconds: float,
        stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardSucceeded):
            raise TimeoutError("scheduler receipt delayed")
        return _accept_report(report, timeout_seconds, stop)

    worker = _worker(
        tmp_path,
        claims=claims,
        receipt_waiter=delayed_success_receipt,
    )
    records: list[dict[str, object]] = []
    sink = logger.add(
        lambda message: records.append(dict(message.record["extra"])),
        level="WARNING",
    )
    try:
        result = worker.run_once()
    finally:
        logger.remove(sink)

    assert result.status == "awaiting_receipt"
    timeout_records = [
        record for record in records if record.get("failure") == "success_receipt_timeout"
    ]
    assert len(timeout_records) == 1
    assert timeout_records[0]["component"] == "lab_worker"
    assert timeout_records[0]["worker_id"] == "worker-a"
    assert timeout_records[0]["job_id"] == str(claim.job_id)
    assert timeout_records[0]["shard_id"] == str(claim.shard_id)
    assert timeout_records[0]["report_id"] == str(result.report_id)


def test_adapter_runtime_error_emits_structured_worker_failure(tmp_path: Path) -> None:
    from loguru import logger

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=RecordingRegistry(failure=RuntimeError("adapter probe failed")),
    )
    records: list[dict[str, object]] = []
    sink = logger.add(
        lambda message: records.append(
            {
                "extra": dict(message.record["extra"]),
                "level": message.record["level"].name,
            }
        ),
        level="WARNING",
    )
    try:
        result = worker.run_once()
    finally:
        logger.remove(sink)

    assert result.status == "failed"
    failures = [
        record
        for record in records
        if record["extra"].get("failure") == "shard_execution_failed"  # type: ignore[union-attr]
    ]
    assert len(failures) == 1
    assert failures[0]["level"] == "ERROR"
    extra = failures[0]["extra"]
    assert extra["component"] == "lab_worker"  # type: ignore[index]
    assert extra["phase"] == "execute"  # type: ignore[index]
    assert extra["job_id"] == str(claim.job_id)  # type: ignore[index]
    assert extra["shard_id"] == str(claim.shard_id)  # type: ignore[index]
    assert extra["claim_generation"] == claim.claim_generation  # type: ignore[index]
    assert extra["error_type"] == "RuntimeError"  # type: ignore[index]


def test_worker_failure_logging_error_does_not_change_tick_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module

    claims = LabClaimSpool(tmp_path / "claims")
    claims.publish(_claim(_nshape_compare_spec(hold_days=(1,))))
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=RecordingRegistry(failure=RuntimeError("adapter failure")),
    )

    def fail_logging(**_fields: object) -> object:
        raise RuntimeError("logging transport failed")

    monkeypatch.setattr(lab_worker_module.logger, "bind", fail_logging)

    assert worker.run_once().status == "failed"


@pytest.mark.parametrize("mode", ["idle", "stopped"])
def test_idle_and_cooperative_stop_do_not_log_worker_failure(
    tmp_path: Path,
    mode: str,
) -> None:
    from loguru import logger

    worker = _worker(tmp_path)
    if mode == "stopped":
        worker.request_stop()
    failures: list[dict[str, object]] = []
    sink = logger.add(
        lambda message: failures.append(dict(message.record["extra"])),
        level="WARNING",
    )
    try:
        result = worker.run_once()
    finally:
        logger.remove(sink)

    assert result.status == mode
    assert not [record for record in failures if record.get("failure") == "shard_execution_failed"]


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


def test_worker_never_executes_revoked_claim_after_cleanup_interruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    original_unlink_current = claims._unlink_current_locked
    monkeypatch.setattr(
        claims,
        "_unlink_current_locked",
        lambda _claim: (_ for _ in ()).throw(OSError("injected cleanup interruption")),
    )
    with pytest.raises(OSError, match="cleanup interruption"):
        claims.revoke(claim, reason="sqlite terminal")
    monkeypatch.setattr(claims, "_unlink_current_locked", original_unlink_current)

    result = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    ).run_once()

    assert result.status == "idle"
    assert registry.executions == 0
    assert reports.pending() == ()
    assert claims.pending() == ()


def test_worker_rechecks_admission_after_consume_before_open_or_execute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    admission_attempted = threading.Event()
    release = threading.Event()
    original_admit = claims.admit_execution

    def pause_before_admission(exact_claim: LabShardClaim):
        admission_attempted.set()
        assert release.wait(2)
        return original_admit(exact_claim)

    monkeypatch.setattr(claims, "admit_execution", pause_before_admission)
    stores_opened = 0

    @contextmanager
    def counted_store() -> Iterator[object]:
        nonlocal stores_opened
        stores_opened += 1
        yield object()

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        exploratory_store_factory=counted_store,
    )
    results: list[object] = []
    thread = threading.Thread(target=lambda: results.append(worker.run_once()))
    thread.start()
    assert admission_attempted.wait(2)

    claims.revoke(claim, reason="scheduler terminalized consumed claim")
    release.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert results[0].status == "stopped"
    assert stores_opened == 0
    assert registry.executions == 0
    assert not worker.sealed_bundle_path(claim).exists()


def test_worker_admission_then_revoke_allows_compute_but_never_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    admitted = threading.Event()
    release = threading.Event()
    original_admit = claims.admit_execution

    def admit_then_pause(exact_claim: LabShardClaim):
        receipt = original_admit(exact_claim)
        admitted.set()
        assert release.wait(2)
        return receipt

    monkeypatch.setattr(claims, "admit_execution", admit_then_pause)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        heartbeat_interval_seconds=0.01,
    )
    results: list[object] = []
    thread = threading.Thread(target=lambda: results.append(worker.run_once()))
    thread.start()
    assert admitted.wait(2)

    claims.revoke(claim, reason="scheduler revoked admitted execution")
    release.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert results[0].status == "failed"
    assert registry.executions == 1
    assert claims.execution_admission(claim.claim_token).admission.claim == claim
    assert claims.revocation(claim.claim_token).revocation.claim == claim
    assert not worker.sealed_bundle_path(claim).exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_revoke_during_execute_is_fenced_before_seal_and_success(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    executing = threading.Event()
    release = threading.Event()

    class BlockingRegistry(RecordingRegistry):
        def execute_shard(
            self,
            validated: ValidatedStrategyShard,
            store: object,
        ) -> LabShardExecutionResult:
            executing.set()
            assert release.wait(2)
            return super().execute_shard(validated, store)

    registry = BlockingRegistry()
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        heartbeat_interval_seconds=0.01,
    )
    results: list[object] = []
    thread = threading.Thread(target=lambda: results.append(worker.run_once()))
    thread.start()
    assert executing.wait(2)

    claims.revoke(claim, reason="scheduler revoked running attempt")
    release.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert results[0].status == "failed"
    assert registry.executions == 1
    assert not worker.sealed_bundle_path(claim).exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


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
        isinstance(report.body, LabShardSucceeded) and report.claim_token == original.claim_token
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
    claims.reconcile_current()
    assert not sealed.exists()
    second = worker.run_once()

    assert second.manifest_hash != first.manifest_hash
    assert manifest.claim_token == claim.claim_token
    assert manifest.claim_generation == claim.claim_generation
    assert manifest.scheduler_fencing_token == claim.scheduler_fencing_token
    assert worker.sealed_bundle_path(retry) != sealed
    assert worker.sealed_bundle_path(retry).is_dir()
    assert registry.executions == 2
    assert not tuple(
        path for path in (tmp_path / "artifacts" / ".tmp").rglob("*") if path.is_file()
    )


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
    persisted = pd.read_parquet(worker.sealed_bundle_path(claim) / first.artifacts[0].file_name)
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
            tables=(LabShardTable(name="trades", frame=pd.DataFrame([{"value": value}])),),
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


def test_generation_three_recovers_crash_after_temporary_tree_isolation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    generation_two = _retry_claim(old)
    generation_three = _retry_claim(generation_two)
    worker = _worker(tmp_path)
    obsolete = worker._temporary_bundle_path(old)
    nested = obsolete / "nested"
    nested.mkdir(parents=True)
    (obsolete / "manifest.partial").write_bytes(b"manifest")
    (nested / "artifact.partial").write_bytes(b"artifact")
    original_promote = worker.artifact_reclaimer._promote_garbage_bundle
    interrupted = False

    def crash_before_pending(source: Path, target: Path) -> None:
        nonlocal interrupted
        if source.parent == worker.artifact_reclaimer.garbage_staging_dir and not interrupted:
            interrupted = True
            raise InterruptedError("crash after temporary tree isolation")
        original_promote(source, target)

    monkeypatch.setattr(
        worker.artifact_reclaimer,
        "_promote_garbage_bundle",
        crash_before_pending,
    )
    with pytest.raises(InterruptedError, match="tree isolation"):
        worker._reclaim_obsolete_temporaries(generation_two)

    assert not obsolete.exists()
    staging = tuple(worker.artifact_reclaimer.garbage_staging_dir.iterdir())
    assert len(staging) == 1
    assert (staging[0] / "owner.json").is_file()
    assert (staging[0] / "payload" / "nested" / "artifact.partial").is_file()
    monkeypatch.setattr(
        worker.artifact_reclaimer,
        "_promote_garbage_bundle",
        original_promote,
    )

    restarted = _worker(tmp_path)
    restarted._reclaim_obsolete_temporaries(generation_three)
    restarted.artifact_reclaimer.collect_garbage()

    assert not obsolete.exists()
    assert tuple(restarted.artifact_reclaimer.garbage_staging_dir.iterdir()) == ()
    entries = restarted.artifact_reclaimer.quarantine_entries()
    assert len(entries) == 1
    assert entries[0].state == "deferred_gc"
    assert (entries[0].bundle_path / "payload" / "nested" / "artifact.partial").is_file()


def test_temporary_tree_quarantine_retains_complete_inventory_across_restarts(
    tmp_path: Path,
) -> None:
    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    current = _retry_claim(old)
    worker = _worker(tmp_path)
    obsolete = worker._temporary_bundle_path(old)
    nested = obsolete / "nested"
    nested.mkdir(parents=True)
    first = obsolete / "first.partial"
    second = nested / "second.partial"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    worker._reclaim_obsolete_temporaries(current)
    for _ in range(3):
        restarted = _worker(tmp_path)
        restarted.artifact_reclaimer.collect_garbage()

    entry = restarted.artifact_reclaimer.quarantine_entries()[0]
    payload = entry.bundle_path / "payload"
    assert (payload / "first.partial").read_bytes() == b"first"
    assert (payload / "nested" / "second.partial").read_bytes() == b"second"
    assert entry.retained_bytes == len(b"first") + len(b"second")


@pytest.mark.parametrize("mutation", ["extra", "replace", "hardlink", "symlink"])
def test_temporary_tree_gc_rejects_mutated_owned_payload(
    tmp_path: Path,
    mutation: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError

    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    current = _retry_claim(old)
    worker = _worker(tmp_path)
    obsolete = worker._temporary_bundle_path(old)
    obsolete.mkdir(parents=True)
    original = obsolete / "partial.bin"
    original.write_bytes(b"original")
    worker._reclaim_obsolete_temporaries(current)
    deferred = tuple(worker.artifact_reclaimer.garbage_deferred_dir.iterdir())[0]
    payload = deferred / "payload"
    external = tmp_path / f"external-{mutation}"
    external.write_bytes(b"external")
    if mutation == "extra":
        (payload / "extra.bin").write_bytes(b"extra")
    elif mutation == "replace":
        os.replace(external, payload / "partial.bin")
    elif mutation == "hardlink":
        original_payload = payload / "partial.bin"
        external.unlink()
        os.link(original_payload, external)
    else:
        (payload / "partial.bin").unlink()
        (payload / "partial.bin").symlink_to(external)

    restarted = _worker(tmp_path)
    with pytest.raises(LabArtifactConflictError):
        restarted.artifact_reclaimer.collect_garbage()

    assert deferred.exists()
    if mutation in {"hardlink", "symlink"}:
        assert external.exists()


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
    assert worker._temporary_bundle_path(claim).is_dir()
    assert tuple(worker._temporary_bundle_path(claim).iterdir()) == ()
    assert worker.artifact_reclaimer.quarantine_summary().bundle_count == 1


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


def test_obsolete_temporary_hardlink_is_rejected_without_deleting_tree(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError

    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    new = _retry_claim(old)
    worker = _worker(tmp_path)
    obsolete = worker._temporary_bundle_path(old)
    nested = obsolete / "nested"
    nested.mkdir(parents=True)
    partial = nested / "partial.parquet"
    partial.write_bytes(b"partial")
    external = tmp_path / "external-partial.parquet"
    os.link(partial, external)

    with pytest.raises(LabArtifactConflictError, match="hard link"):
        worker._reclaim_obsolete_temporaries(new)

    assert obsolete.is_dir()
    assert partial.read_bytes() == b"partial"
    assert external.read_bytes() == b"partial"
    assert partial.stat().st_nlink == 2


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
    validated = registry.validate_claim(claim)
    worker._seal_result(claim, registry.execute_shard(validated, object()))
    (worker.sealed_bundle_path(claim) / "manifest.json").write_text("{}", encoding="utf-8")

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


def test_legacy_formal_spec_uses_canonical_snapshot_strategy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.research_gate import ResearchGateRequest

    spec = _formal_spec().model_copy(
        update={
            "parameters": _formal_spec().parameters.model_copy(
                update={"strategy_name": "NShapeCompare"}
            )
        }
    )
    requests: list[ResearchGateRequest] = []

    @contextmanager
    def gated_store(
        request: ResearchGateRequest,
        **_kwargs: object,
    ) -> Iterator[tuple[object, object]]:
        requests.append(request)
        yield object(), object()

    @contextmanager
    def metadata_factory() -> Iterator[object]:
        yield object()

    monkeypatch.setattr(lab_worker, "open_gated_research_store", gated_store)
    claims = LabClaimSpool(tmp_path / "claims")
    claims.publish(_claim(spec))
    worker = _worker(
        tmp_path,
        claims=claims,
        exploratory_store_factory=None,
        metadata_store_factory=metadata_factory,
        lake_root=tmp_path / "lake",
    )

    result = worker.run_once()

    assert result.status == "succeeded"
    assert len(requests) == 1
    assert requests[0].strategy_name == "n_shape"
    assert requests[0].dataset_snapshot_id == spec.dataset_snapshot.snapshot_id
    assert requests[0].dataset_binding_hash == spec.dataset_snapshot.binding_hash


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
    receipts = tuple(reports.load_receipt(path) for path in sorted(reports.ack_dir.glob("*.json")))
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


def test_stale_pending_success_does_not_block_obsolete_sealed_reclamation(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

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

    generation_two = _retry_claim(generation_one)
    claims.publish(generation_two)
    outcomes = claims.reconcile_current()

    assert claims.current(generation_one.job_id, generation_one.shard_id).claim == generation_two
    assert outcomes[0].status == "reconciled"
    assert not worker.sealed_bundle_path(generation_one).exists()
    assert reports.pending()[0].report == success


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
    from rquant.lab_worker import LabArtifactReclaimer

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
    generation_two = _retry_claim(generation_one)
    claims.publish(generation_two)
    outcomes = claims.reconcile_current()

    assert claims.current(generation_one.job_id, generation_one.shard_id).claim == generation_two
    assert outcomes[0].status == "failed"
    assert "accepted success" in outcomes[0].error
    assert worker.sealed_bundle_path(generation_one).is_dir()
    scheduler.release()


def _sealed_obsolete_attempt(
    tmp_path: Path,
    reports: LabReportSpool,
) -> tuple[LabShardClaim, LabShardClaim, Path, object]:
    old_claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    current_claim = _retry_claim(old_claim)
    worker = _worker(
        tmp_path,
        claims=LabClaimSpool(tmp_path / "worker-claims"),
        reports=reports,
    )
    validated = worker.adapter_registry.validate_claim(old_claim)
    result = RecordingRegistry().execute_shard(validated, object())
    manifest = worker._seal_result(old_claim, result)
    return old_claim, current_claim, worker.sealed_bundle_path(old_claim), manifest


def test_pending_success_published_after_preflight_scan_is_stale_and_reclaimed(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, sealed, manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    scanned = threading.Event()
    release = threading.Event()
    original_scan = reclaimer._assert_no_terminal_success_evidence

    def pause_after_scan(
        claim: LabShardClaim,
        candidate_manifest: object,
        durable_claim: LabShardClaim,
    ) -> None:
        original_scan(claim, candidate_manifest, durable_claim)
        scanned.set()
        assert release.wait(2)

    reclaimer._assert_no_terminal_success_evidence = pause_after_scan  # type: ignore[method-assign]
    errors: list[BaseException] = []

    def reclaim() -> None:
        try:
            reclaimer.reclaim(current_claim)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=reclaim)
    thread.start()
    assert scanned.wait(2)
    success = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    reports.publish(success)
    release.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert errors == []
    assert not sealed.exists()
    assert reports.pending()[0].report == success


def test_accepted_receipt_published_after_preflight_scan_preserves_attempt(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, sealed, manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    scanned = threading.Event()
    release = threading.Event()
    original_scan = reclaimer._assert_no_terminal_success_evidence

    def pause_after_scan(
        claim: LabShardClaim,
        candidate_manifest: object,
        durable_claim: LabShardClaim,
    ) -> None:
        original_scan(claim, candidate_manifest, durable_claim)
        scanned.set()
        assert release.wait(2)

    reclaimer._assert_no_terminal_success_evidence = pause_after_scan  # type: ignore[method-assign]
    errors: list[BaseException] = []
    thread = threading.Thread(
        target=lambda: _capture_reclaimer_error(reclaimer, current_claim, errors)
    )
    thread.start()
    assert scanned.wait(2)
    success = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    entry = reports.publish(success)
    reports.ack(
        entry,
        LabReportReceipt.from_report(
            success,
            status="accepted",
            reason="accepted before claim advance",
            accepted_at=NOW,
        ),
    )
    release.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], LabArtifactConflictError)
    assert sealed.is_dir()


def _capture_reclaimer_error(
    reclaimer: object,
    claim: LabShardClaim,
    errors: list[BaseException],
) -> None:
    try:
        reclaimer.reclaim(claim)
    except BaseException as exc:
        errors.append(exc)


def test_claim_high_water_is_durable_before_reclaimer_hook_runs(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    old_claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    current_claim = _retry_claim(old_claim)
    claims.publish(old_claim)
    observed: list[LabShardClaim] = []

    def observe_marker(claim: LabShardClaim) -> None:
        observed.append(claims.current(claim.job_id, claim.shard_id).claim)

    claims.set_claim_advance_hook(observe_marker)
    claims.publish(current_claim)

    assert observed == []
    outcomes = claims.reconcile_current()

    assert observed == [current_claim]
    assert outcomes[0].status == "reconciled"


def test_report_publish_uses_cross_process_evidence_lock(tmp_path: Path) -> None:
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    report = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash="a" * 64),
    )
    source = (
        "from tests.unit.test_lab_worker import _publish_report_child; "
        "_publish_report_child(*__import__('sys').argv[1:])"
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(Path(__file__).parents[2])

    with reports.evidence_lock():
        process = subprocess.Popen(
            [sys.executable, "-c", source, str(tmp_path), report.model_dump_json()],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
        )
        time.sleep(0.1)
        assert process.poll() is None
        assert reports.pending_locked() == ()

    stdout, stderr = process.communicate(timeout=2)

    assert process.returncode == 0, (stdout, stderr)
    assert tuple(entry.report for entry in reports.pending()) == (report,)


def test_two_reclaimers_are_idempotent_for_same_obsolete_attempt(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimers = tuple(
        LabArtifactReclaimer(
            artifact_root=tmp_path / "artifacts",
            report_spool=LabReportSpool(tmp_path / "reports"),
        )
        for _ in range(2)
    )
    barrier = threading.Barrier(2)
    for reclaimer in reclaimers:
        original_scan = reclaimer._assert_no_terminal_success_evidence

        def synchronized_scan(
            claim: LabShardClaim,
            manifest: object,
            durable_claim: LabShardClaim,
            *,
            scan=original_scan,
        ) -> None:
            scan(claim, manifest, durable_claim)
            barrier.wait(timeout=2)

        reclaimer._assert_no_terminal_success_evidence = synchronized_scan  # type: ignore[method-assign]
    errors: list[BaseException] = []
    threads = tuple(
        threading.Thread(
            target=_capture_reclaimer_error,
            args=(reclaimer, current_claim, errors),
        )
        for reclaimer in reclaimers
    )
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert not sealed.exists()


def test_reclaimer_recovers_verified_tombstone_after_process_crash(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_tombstone_rename_child",
        tmp_path,
        current_claim.model_dump_json(),
    )

    assert completed.returncode == 79, completed.stderr
    assert not sealed.exists()
    tombstones = tuple(sealed.parent.glob(".reclaim-*"))
    assert len(tombstones) == 1

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.reclaim(current_claim)

    assert tuple(sealed.parent.iterdir()) == ()


def test_reclaimer_recovers_prepared_ledger_after_crash_before_rename(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_prepared_ledger_child",
        tmp_path,
        current_claim.model_dump_json(),
    )

    assert completed.returncode == 81, completed.stderr
    assert sealed.is_dir()
    assert len(tuple((tmp_path / "artifacts" / ".reclaim-ledger").rglob("*.json"))) == 1

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.reclaim(current_claim)

    assert not sealed.exists()
    ledgers = tuple((tmp_path / "artifacts" / ".reclaim-ledger").rglob("*.json"))
    assert len(ledgers) == 1
    assert '"state":"deferred_gc"' in ledgers[0].read_text(encoding="utf-8")
    assert restarted.quarantine_summary().bundle_count == 1


def test_newer_high_water_can_finish_verified_older_reclaim_ledger(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, generation_two, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_tombstone_rename_child",
        tmp_path,
        generation_two.model_dump_json(),
    )
    assert completed.returncode == 79, completed.stderr
    generation_three = _retry_claim(generation_two)
    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )

    restarted.reclaim(generation_three)

    assert not sealed.exists()
    assert tuple(sealed.parent.glob(".reclaim-*")) == ()
    ledgers = tuple((tmp_path / "artifacts" / ".reclaim-ledger").rglob("*.json"))
    assert len(ledgers) == 1
    assert '"state":"deferred_gc"' in ledgers[0].read_text(encoding="utf-8")


def test_accepted_success_protects_tombstone_during_restart(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, sealed, manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_tombstone_rename_child",
        tmp_path,
        current_claim.model_dump_json(),
    )
    assert completed.returncode == 79, completed.stderr
    tombstone = tuple(sealed.parent.glob(".reclaim-*"))[0]
    success = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    entry = reports.publish(success)
    reports.ack(
        entry,
        LabReportReceipt.from_report(
            success,
            status="accepted",
            reason="accepted before restart",
            accepted_at=NOW,
        ),
    )
    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )

    with pytest.raises(LabArtifactConflictError, match="accepted success"):
        restarted.reclaim(current_claim)

    assert tombstone.is_dir()


def test_source_and_same_identity_tombstone_are_preserved_as_conflict(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, sealed, manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    tombstone = sealed.parent / reclaimer._tombstone_name(old_claim, manifest)
    shutil.copytree(sealed, tombstone)

    with pytest.raises(LabArtifactConflictError, match="source and tombstone"):
        reclaimer.reclaim(current_claim)

    assert sealed.is_dir()
    assert tombstone.is_dir()


def test_unknown_reclaim_tombstone_remains_fail_closed(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    unknown = sealed.parent / ".reclaim-v1-unknown"
    unknown.mkdir()
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )

    with pytest.raises(LabArtifactConflictError):
        reclaimer.reclaim(current_claim)

    assert unknown.is_dir()
    assert sealed.is_dir()


@pytest.mark.parametrize("file_name", ["manifest.json", "artifact"])
def test_reclaimer_rejects_hardlinked_bundle_file_without_deleting(
    tmp_path: Path,
    file_name: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    target = (
        sealed / "manifest.json" if file_name == "manifest.json" else next(sealed.glob("*.parquet"))
    )
    external = tmp_path / f"external-{target.name}"
    os.link(target, external)
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )

    with pytest.raises(LabArtifactConflictError, match="hard link"):
        reclaimer.reclaim(current_claim)

    assert sealed.is_dir()
    assert target.is_file()
    assert external.is_file()
    assert target.stat().st_nlink == 2


def test_seal_rejects_hardlink_created_at_atomic_rename_without_deleting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactConflictError

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker.adapter_registry.validate_claim(claim)
    result = RecordingRegistry().execute_shard(validated, object())
    sealed = worker.sealed_bundle_path(claim)
    external = tmp_path / "external-rename-artifact.parquet"
    original_rename = lab_worker_module.os.rename

    def hardlink_at_rename(source: Path, target: Path) -> None:
        original_rename(source, target)
        if Path(target) == sealed:
            os.link(next(sealed.glob("*.parquet")), external)

    monkeypatch.setattr(lab_worker_module.os, "rename", hardlink_at_rename)

    with pytest.raises(LabArtifactConflictError, match="hard link"):
        worker._seal_result(claim, result)

    assert sealed.is_dir()
    assert external.is_file()


def test_reclaimer_rejects_hardlinked_ledger_without_deleting_tombstone(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_tombstone_rename_child",
        tmp_path,
        current_claim.model_dump_json(),
    )
    assert completed.returncode == 79, completed.stderr
    tombstone = tuple(sealed.parent.glob(".reclaim-*"))[0]
    ledger = tuple((tmp_path / "artifacts" / ".reclaim-ledger").rglob("*.json"))[0]
    external = tmp_path / "external-ledger.json"
    os.link(ledger, external)
    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )

    with pytest.raises(LabArtifactConflictError, match="hard link"):
        restarted.reclaim(current_claim)

    assert tombstone.is_dir()
    assert ledger.is_file()
    assert external.is_file()


def test_inventory_replacement_after_validation_is_restored_and_not_deleted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    original_inventory = reclaimer._inventory_entry
    validated_in_tombstone = 0
    saved_original = tmp_path / "saved-original-manifest.json"

    def replace_after_validation(path: Path, *, relative_path: str):
        nonlocal validated_in_tombstone
        observed = original_inventory(path, relative_path=relative_path)
        if path.parent.name.startswith(".reclaim-v1-") and relative_path == "manifest.json":
            validated_in_tombstone += 1
            if validated_in_tombstone == 2:
                os.replace(path, saved_original)
                path.write_bytes(b"replacement-must-survive")
        return observed

    monkeypatch.setattr(reclaimer, "_inventory_entry", replace_after_validation)

    with pytest.raises(LabArtifactConflictError, match="owner inventory"):
        reclaimer.reclaim(current_claim)

    tombstone = tuple(sealed.parent.glob(".reclaim-v1-*"))[0]
    assert (tombstone / "manifest.json").read_bytes() == b"replacement-must-survive"
    assert saved_original.is_file()


def test_ledger_replacement_after_validation_is_restored_and_not_deleted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_tombstone_rename_child",
        tmp_path,
        current_claim.model_dump_json(),
    )
    assert completed.returncode == 79, completed.stderr
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    ledger = tuple((tmp_path / "artifacts" / ".reclaim-ledger").rglob("*.json"))[0]
    saved_original = tmp_path / "saved-original-ledger.json"
    original_load = reclaimer._load_ledger
    replaced = False

    def replace_after_load(path: Path):
        nonlocal replaced
        loaded = original_load(path)
        if path == ledger and not replaced:
            replaced = True
            os.replace(path, saved_original)
            path.write_bytes(b"replacement-ledger-must-survive")
        return loaded

    monkeypatch.setattr(reclaimer, "_load_ledger", replace_after_load)

    with pytest.raises(LabArtifactConflictError, match="changed before deletion"):
        reclaimer._remove_ledger(ledger)

    assert ledger.read_bytes() == b"replacement-ledger-must-survive"
    assert saved_original.is_file()
    assert tuple(sealed.parent.glob(".reclaim-v1-*"))


def test_reclaimer_retains_complete_inventory_in_deferred_quarantine(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    reclaimer.reclaim(current_claim)

    entry = reclaimer.quarantine_entries()[0]
    payload = entry.bundle_path / "payload"
    assert not sealed.exists()
    assert tuple(sealed.parent.glob(".reclaim-*")) == ()
    assert (payload / "manifest.json").is_file()
    assert len(tuple(payload.glob("*.parquet"))) == 1
    assert entry.state == "deferred_gc"
    assert entry.retained_bytes > 0


@pytest.mark.parametrize("mutation", ["unknown", "replace"])
def test_deferred_reclaim_rejects_unknown_or_replaced_file(
    tmp_path: Path,
    mutation: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    reclaimer.reclaim(current_claim)
    deferred = reclaimer.quarantine_entries()[0].bundle_path / "payload"
    if mutation == "unknown":
        (deferred / "intruder").write_text("unexpected", encoding="utf-8")
    else:
        remaining = deferred / "manifest.json"
        replacement = deferred / ".replacement"
        replacement.write_bytes(remaining.read_bytes())
        os.replace(replacement, remaining)
    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )

    with pytest.raises(LabArtifactConflictError):
        restarted.reclaim(current_claim)

    assert deferred.is_dir()


def test_accepted_success_protects_deferred_quarantine_bytes(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, sealed, manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    reclaimer.reclaim(current_claim)
    deferred = reclaimer.quarantine_entries()[0].bundle_path
    success = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    entry = reports.publish(success)
    reports.ack(
        entry,
        LabReportReceipt.from_report(
            success,
            status="accepted",
            reason="accepted before partial restart",
            accepted_at=NOW,
        ),
    )

    with pytest.raises(LabArtifactConflictError, match="accepted success"):
        LabArtifactReclaimer(
            artifact_root=tmp_path / "artifacts",
            report_spool=LabReportSpool(tmp_path / "reports"),
        ).reclaim(current_claim)

    assert deferred.is_dir()
    assert (deferred / "payload" / "manifest.json").is_file()


def test_reclaimer_cleans_only_recognized_single_link_ledger_temporaries(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    current = _retry_claim(_claim(_nshape_compare_spec(hold_days=(1,))))
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    ledger_dir = reclaimer._ledger_dir(current)
    ledger_dir.mkdir(parents=True)
    temporary = ledger_dir / f".reclaim-ledger-tmp-v1-{uuid4().hex}.tmp"
    temporary.write_bytes(b"x" * 32)

    reclaimer.reclaim(current)
    reclaimer.reclaim(current)

    assert not temporary.exists()


@pytest.mark.parametrize("kind", ["unknown", "symlink", "hardlink"])
def test_reclaimer_rejects_unsafe_ledger_temporary(
    tmp_path: Path,
    kind: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    current = _retry_claim(_claim(_nshape_compare_spec(hold_days=(1,))))
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    ledger_dir = reclaimer._ledger_dir(current)
    ledger_dir.mkdir(parents=True)
    external = tmp_path / "external-ledger-temp"
    external.write_bytes(b"preserve")
    if kind == "unknown":
        temporary = ledger_dir / ".unknown.tmp"
        temporary.write_bytes(b"unknown")
    else:
        temporary = ledger_dir / f".reclaim-ledger-tmp-v1-{uuid4().hex}.tmp"
        if kind == "symlink":
            temporary.symlink_to(external)
        else:
            os.link(external, temporary)

    with pytest.raises(LabArtifactConflictError):
        reclaimer.reclaim(current)

    assert os.path.lexists(temporary)
    assert external.read_bytes() == b"preserve"


def test_ledger_temporary_replacement_after_identity_check_survives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    current = _retry_claim(_claim(_nshape_compare_spec(hold_days=(1,))))
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    ledger_dir = reclaimer._ledger_dir(current)
    ledger_dir.mkdir(parents=True)
    temporary = ledger_dir / f".reclaim-ledger-tmp-v1-{uuid4().hex}.tmp"
    temporary.write_bytes(b"original")
    saved_original = tmp_path / "saved-ledger-temporary"
    original_identity = reclaimer._regular_file_identity
    replaced = False

    def replace_after_identity(path: Path, *, label: str):
        nonlocal replaced
        identity = original_identity(path, label=label)
        if path == temporary and not replaced:
            replaced = True
            os.replace(path, saved_original)
            path.write_bytes(b"replacement-temporary-must-survive")
        return identity

    monkeypatch.setattr(reclaimer, "_regular_file_identity", replace_after_identity)

    with pytest.raises(LabArtifactConflictError, match="changed before deletion"):
        reclaimer._cleanup_ledger_temporaries(ledger_dir)

    assert temporary.read_bytes() == b"replacement-temporary-must-survive"
    assert saved_original.is_file()


def test_logical_quarantine_retains_bytes_and_reports_deferred_gc(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "logical-delete" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"retained-until-p7")
    expected = reclaimer._regular_file_identity(victim, label="deferred gc fixture")

    assert reclaimer._safe_remove_regular_child(
        victim,
        expected=expected,
        label="deferred gc fixture",
    )
    reclaimer.collect_garbage()

    entries = reclaimer.quarantine_entries()
    summary = reclaimer.quarantine_summary()
    assert not victim.exists()
    assert len(entries) == 1
    assert entries[0].state == "deferred_gc"
    assert (entries[0].bundle_path / "payload").read_bytes() == b"retained-until-p7"
    assert summary.bundle_count == 1
    assert summary.retained_bytes == len(b"retained-until-p7")


def test_quarantine_summary_uses_verified_ledgers_without_rehashing_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "summary" / "retained.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"ledger-sized-payload")
    assert reclaimer.logical_quarantine_tree(victim, purpose="summary fixture")

    def forbid_payload_hash(_path: Path) -> tuple[object, ...]:
        raise AssertionError("summary must not rehash immutable deferred payload")

    monkeypatch.setattr(reclaimer, "_garbage_inventory", forbid_payload_hash)

    first = reclaimer.quarantine_summary()
    second = reclaimer.quarantine_summary()

    assert first == second
    assert first.bundle_count == 1
    assert first.retained_bytes == len(b"ledger-sized-payload")


def test_p13_reclaim_critical_paths_have_no_physical_delete_calls() -> None:
    from rquant.lab_worker import LabArtifactReclaimer, LabWorker

    source = "\n".join(
        inspect.getsource(method)
        for method in (
            LabWorker._cleanup_temporary,
            LabWorker._rollback_sealed,
            LabArtifactReclaimer._cleanup_ledger_temporaries,
            LabArtifactReclaimer._delete_isolated_tombstone,
            LabArtifactReclaimer._safe_remove_regular_child,
        )
    )

    assert ".unlink(" not in source
    assert ".rmdir(" not in source
    assert "rmtree(" not in source


def test_owner_only_staging_resumes_source_isolation_after_restart(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    victim = tmp_path / "artifacts" / "owner-only" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"owner-before-payload")
    owner = reclaimer._garbage_owner(victim, purpose="owner-only crash fixture")
    staging = reclaimer.garbage_staging_dir / owner.garbage_id.hex
    staging.mkdir(mode=0o700)
    reclaimer._write_garbage_owner(staging, owner)

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.collect_garbage()

    entry = restarted.quarantine_entries()[0]
    assert not victim.exists()
    assert entry.state == "deferred_gc"
    assert (entry.bundle_path / "payload").read_bytes() == b"owner-before-payload"


@pytest.mark.parametrize(
    "derived_state",
    ["missing", "empty_staging", "global_owner_only", "both_owners_without_ledger"],
)
def test_prepared_intent_rebuilds_incomplete_derived_state(
    tmp_path: Path,
    derived_state: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    victim = tmp_path / "artifacts" / "prepared-intent" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(f"intent-{derived_state}".encode())
    owner = reclaimer._garbage_owner(victim, purpose="prepared intent recovery fixture")
    intent = reclaimer._prepared_intent(owner)
    intent_path = reclaimer._write_prepared_intent(intent)
    staging = reclaimer.garbage_staging_dir / owner.garbage_id.hex
    global_owner = reclaimer.garbage_owner_dir / f"{owner.garbage_id.hex}.json"
    if derived_state in {"empty_staging", "both_owners_without_ledger"}:
        staging.mkdir(mode=0o700)
    if derived_state in {"global_owner_only", "both_owners_without_ledger"}:
        global_owner.write_text(owner.canonical_json(), encoding="utf-8")
    if derived_state == "both_owners_without_ledger":
        (staging / "owner.json").write_text(owner.canonical_json(), encoding="utf-8")

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.collect_garbage()

    entry = restarted.quarantine_entries()[0]
    assert intent_path.is_file()
    assert not victim.exists()
    assert entry.state == "deferred_gc"
    assert (entry.bundle_path / "payload").read_bytes() == f"intent-{derived_state}".encode()
    assert tuple(restarted.garbage_staging_dir.iterdir()) == ()
    assert len(tuple(restarted.garbage_ledger_dir.glob(f"{owner.garbage_id.hex}-*.json"))) == 3


def test_prepared_intent_publish_is_no_clobber_and_idempotent(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "intent-publish" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"authoritative-intent")
    owner = reclaimer._garbage_owner(victim, purpose="intent no-clobber fixture")
    intent = reclaimer._prepared_intent(owner)

    first = reclaimer._write_prepared_intent(intent)
    second = reclaimer._write_prepared_intent(intent)
    assert first == second
    assert first.read_text(encoding="utf-8") == intent.canonical_json()
    assert intent.state == "prepared"
    assert intent.source_relative_path == owner.original_relative_path
    assert intent.staging_relative_path == f".garbage-v1/staging/{owner.garbage_id.hex}"
    assert intent.owner.inventory == owner.inventory
    assert len(intent.intent_hash) == 64

    replacement = first.with_suffix(".replacement")
    replacement.write_text("foreign-intent", encoding="utf-8")
    os.replace(replacement, first)
    with pytest.raises(LabArtifactConflictError):
        reclaimer._write_prepared_intent(intent)
    assert first.read_text(encoding="utf-8") == "foreign-intent"


def test_partial_prepared_intent_temporary_is_isolated_and_does_not_block(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "partial-intent" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"source-survives-partial-intent")
    owner = reclaimer._garbage_owner(victim, purpose="partial intent fixture")
    temporary = reclaimer.garbage_intent_temp_dir / (
        f".prepared-intent-tmp-v1-{owner.garbage_id.hex}-{uuid4().hex}.tmp"
    )
    temporary.write_bytes(b"{" + b"x" * 31)

    reclaimer.collect_garbage()
    assert not temporary.exists()
    assert len(tuple(reclaimer.garbage_intent_orphan_dir.iterdir())) == 1
    assert victim.is_file()

    assert reclaimer.logical_quarantine_tree(victim, purpose="partial intent fixture")
    assert reclaimer.quarantine_summary().bundle_count == 1


@pytest.mark.parametrize("publish_boundary", ["before_link", "after_link"])
def test_complete_prepared_intent_temporary_recovers_atomic_publish_boundary(
    tmp_path: Path,
    publish_boundary: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "intent-link-crash" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(f"intent-{publish_boundary}".encode())
    owner = reclaimer._garbage_owner(victim, purpose="intent link crash fixture")
    intent = reclaimer._prepared_intent(owner)
    temporary = reclaimer.garbage_intent_temp_dir / (
        f".prepared-intent-tmp-v1-{owner.garbage_id.hex}-{uuid4().hex}.tmp"
    )
    temporary.write_text(intent.canonical_json(), encoding="utf-8")
    target = reclaimer._prepared_intent_path(owner.garbage_id)
    if publish_boundary == "after_link":
        os.link(temporary, target)
        assert temporary.lstat().st_nlink == 2

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.collect_garbage()

    assert not temporary.exists()
    assert target.lstat().st_nlink == 1
    assert restarted._load_prepared_intent(target) == intent
    assert restarted.quarantine_summary().bundle_count == 1


@pytest.mark.parametrize(
    "mutation",
    ["different_global_owner", "bundle_owner_symlink", "global_owner_hardlink", "staging_extra"],
)
def test_prepared_intent_rejects_conflicting_derived_state(
    tmp_path: Path,
    mutation: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "derived-conflict" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"derived-state-source")
    owner = reclaimer._garbage_owner(victim, purpose="derived conflict fixture")
    intent = reclaimer._prepared_intent(owner)
    reclaimer._write_prepared_intent(intent)
    staging = reclaimer.garbage_staging_dir / owner.garbage_id.hex
    staging.mkdir(mode=0o700)
    global_owner = reclaimer.garbage_owner_dir / f"{owner.garbage_id.hex}.json"
    bundle_owner = staging / "owner.json"
    if mutation == "different_global_owner":
        global_owner.write_text("different", encoding="utf-8")
    elif mutation == "bundle_owner_symlink":
        external = tmp_path / "external-owner.json"
        external.write_text(owner.canonical_json(), encoding="utf-8")
        bundle_owner.symlink_to(external)
    elif mutation == "global_owner_hardlink":
        global_owner.write_text(owner.canonical_json(), encoding="utf-8")
        os.link(global_owner, tmp_path / "external-owner-hardlink.json")
    else:
        (staging / "unexpected.bin").write_bytes(b"foreign")

    with pytest.raises(LabArtifactConflictError):
        reclaimer.collect_garbage()

    assert victim.read_bytes() == b"derived-state-source"


@pytest.mark.parametrize("legacy_state", ["global_owner", "both_owners"])
def test_legacy_partial_staging_reconstructs_unique_prepared_intent(
    tmp_path: Path,
    legacy_state: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "legacy-partial.bin"
    victim.write_bytes(f"legacy-{legacy_state}".encode())
    owner = reclaimer._garbage_owner(victim, purpose="legacy prepared fixture")
    global_owner = reclaimer.garbage_owner_dir / f"{owner.garbage_id.hex}.json"
    global_owner.write_text(owner.canonical_json(), encoding="utf-8")
    if legacy_state == "both_owners":
        staging = reclaimer.garbage_staging_dir / owner.garbage_id.hex
        staging.mkdir(mode=0o700)
        (staging / "owner.json").write_text(owner.canonical_json(), encoding="utf-8")

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.collect_garbage()

    entries = restarted.quarantine_entries()
    assert len(entries) == 1
    assert entries[0].state == "deferred_gc"
    assert not victim.exists()
    assert len(tuple(restarted.garbage_intent_dir.glob("*.json"))) == 1


@pytest.mark.parametrize("business_shape", ["unique_file", "multiple_files", "directory"])
def test_recovers_legacy_empty_staging_without_intent(
    tmp_path: Path,
    business_shape: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    live_result = tmp_path / "artifacts" / "live-result.bin"
    live_result.write_bytes(b"live-result-must-not-move")
    business_files = [live_result]
    if business_shape == "multiple_files":
        second = tmp_path / "artifacts" / "second-result.bin"
        second.write_bytes(b"second-result-must-not-move")
        business_files.append(second)
    elif business_shape == "directory":
        nested = tmp_path / "artifacts" / "business-run" / "nested" / "result.bin"
        nested.parent.mkdir(parents=True)
        nested.write_bytes(b"nested-result-must-not-move")
        business_files.append(nested)
    identities = {
        path: (path.lstat().st_dev, path.lstat().st_ino, path.read_bytes())
        for path in business_files
    }
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)

    def reject_business_tree_scan(*args, **kwargs):
        raise AssertionError("legacy empty staging must not scan the artifact business tree")

    monkeypatch.setattr(lab_worker_module.os, "walk", reject_business_tree_scan)

    reclaimer.collect_garbage()

    assert not staging.exists()
    for path, identity in identities.items():
        assert path.is_file()
        assert (path.lstat().st_dev, path.lstat().st_ino, path.read_bytes()) == identity
    assert tuple(reclaimer.garbage_intent_dir.iterdir()) == ()
    assert tuple(reclaimer.garbage_deferred_dir.iterdir()) == ()
    orphans = tuple(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    assert len(orphans) == 1
    orphan = orphans[0]
    assert tuple(orphan.iterdir()) == ()
    ledgers = tuple(reclaimer.garbage_orphan_metadata_dir.glob(f"{orphan.name}.json"))
    assert len(ledgers) == 1
    metadata = json.loads(ledgers[0].read_text(encoding="utf-8"))
    assert metadata["reason"] == "no_proven_source"
    assert metadata["original_staging_relative_path"] == f".garbage-v1/staging/{legacy_id}"
    assert not hasattr(reclaimer, "_legacy_unique_active_source")


def test_legacy_empty_staging_rename_replacement_restores_business_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    expected_empty = (staging.lstat().st_dev, staging.lstat().st_ino)
    preserved_empty = tmp_path / "preserved-empty-staging"
    original_rename = lab_worker_module.os.rename
    replacement_identity: tuple[int, int] | None = None

    def replace_at_orphan_rename(source: Path, target: Path) -> None:
        nonlocal replacement_identity
        if Path(source) == staging and Path(target).parent == reclaimer.garbage_intent_orphan_dir:
            original_rename(source, preserved_empty)
            staging.mkdir(mode=0o700)
            (staging / "result.bin").write_bytes(b"business-result-must-survive")
            observed = staging.lstat()
            replacement_identity = (observed.st_dev, observed.st_ino)
        original_rename(source, target)

    monkeypatch.setattr(lab_worker_module.os, "rename", replace_at_orphan_rename)

    with pytest.raises(LabArtifactConflictError, match="changed during orphan isolation"):
        reclaimer.collect_garbage()

    assert replacement_identity is not None
    assert (staging.lstat().st_dev, staging.lstat().st_ino) == replacement_identity
    assert (staging / "result.bin").read_bytes() == b"business-result-must-survive"
    assert (preserved_empty.lstat().st_dev, preserved_empty.lstat().st_ino) == expected_empty
    assert tuple(reclaimer.garbage_intent_orphan_dir.rglob("orphan.json")) == ()
    assert tuple(reclaimer.garbage_orphan_metadata_dir.iterdir()) == ()


def test_legacy_empty_staging_normal_orphan_binds_original_inode(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    before = staging.lstat()

    reclaimer.collect_garbage()

    orphans = tuple(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    assert len(orphans) == 1
    orphan = orphans[0]
    after = orphan.lstat()
    assert (after.st_dev, after.st_ino, after.st_nlink) == (
        before.st_dev,
        before.st_ino,
        before.st_nlink,
    )
    assert tuple(orphan.iterdir()) == ()
    ledger = reclaimer.garbage_orphan_metadata_dir / f"{orphan.name}.json"
    metadata = json.loads(ledger.read_text(encoding="utf-8"))
    assert metadata["reason"] == "no_proven_source"
    assert metadata["orphan_relative_path"].endswith(orphan.name)
    assert metadata["expected_device"] == before.st_dev
    assert metadata["expected_inode"] == before.st_ino
    assert metadata["expected_file_type"] == "directory"
    assert metadata["expected_nlink"] == before.st_nlink
    assert metadata["expected_empty"] is True


def test_external_orphan_identity_rejects_empty_replacement_after_entry_lstat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    reclaimer.collect_garbage()
    orphan = next(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    ledger = reclaimer.garbage_orphan_metadata_dir / f"{orphan.name}.json"
    metadata = reclaimer._load_external_orphan_metadata(ledger)
    original_orphan_identity = (orphan.lstat().st_dev, orphan.lstat().st_ino)
    original_ledger = (ledger.lstat().st_dev, ledger.lstat().st_ino, ledger.read_bytes())
    preserved_orphan = tmp_path / "preserved-entry-orphan"
    original_lstat = Path.lstat
    replaced = False
    replacement_identity: tuple[int, int] | None = None

    def replace_after_entry_lstat(path: Path) -> os.stat_result:
        nonlocal replaced, replacement_identity
        observed = original_lstat(path)
        if path == orphan and not replaced:
            replaced = True
            os.rename(orphan, preserved_orphan)
            orphan.mkdir(mode=0o700)
            replacement = original_lstat(orphan)
            replacement_identity = (replacement.st_dev, replacement.st_ino)
        return observed

    monkeypatch.setattr(Path, "lstat", replace_after_entry_lstat)

    with pytest.raises(LabArtifactConflictError, match="orphan identity conflicts"):
        reclaimer._assert_external_orphan_identity(orphan, metadata)

    assert replaced
    assert replacement_identity is not None
    assert (original_lstat(preserved_orphan).st_dev, original_lstat(preserved_orphan).st_ino) == (
        original_orphan_identity
    )
    assert (original_lstat(orphan).st_dev, original_lstat(orphan).st_ino) == replacement_identity
    assert tuple(preserved_orphan.iterdir()) == ()
    assert tuple(orphan.iterdir()) == ()
    assert (original_lstat(ledger).st_dev, original_lstat(ledger).st_ino, ledger.read_bytes()) == (
        original_ledger
    )


@pytest.mark.parametrize(
    ("a_payload", "b_payload", "expects_conflict"),
    [
        (None, None, False),
        (None, b"b-must-not-affect-a", False),
        (b"a-must-be-observed", None, True),
    ],
)
def test_external_orphan_identity_fd_enumeration_resists_path_aba(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    a_payload: bytes | None,
    b_payload: bytes | None,
    expects_conflict: bool,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    reclaimer.collect_garbage()
    orphan = next(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    ledger = reclaimer.garbage_orphan_metadata_dir / f"{orphan.name}.json"
    metadata = reclaimer._load_external_orphan_metadata(ledger)
    if a_payload is not None:
        (orphan / "a.bin").write_bytes(a_payload)
        observed_a = orphan.lstat()
        metadata = reclaimer._legacy_empty_staging_orphan_metadata(
            metadata.staging_id,
            metadata.orphan_token,
            reclaimer._directory_identity(observed_a),
        )
        ledger.write_text(metadata.canonical_json(), encoding="utf-8")
    replacement = tmp_path / "aba-replacement"
    replacement.mkdir(mode=0o700)
    if b_payload is not None:
        (replacement / "b.bin").write_bytes(b_payload)
    parked_a = tmp_path / "aba-parked-a"
    original_listdir = lab_worker_module.os.listdir
    original_a = (orphan.lstat().st_dev, orphan.lstat().st_ino)
    original_b = (replacement.lstat().st_dev, replacement.lstat().st_ino)
    original_ledger = (ledger.lstat().st_dev, ledger.lstat().st_ino, ledger.read_bytes())
    swapped = False

    def swap_around_fd_enumeration(path: int | str | bytes | os.PathLike[str]) -> list[str]:
        nonlocal swapped
        if isinstance(path, int) and not swapped:
            swapped = True
            os.rename(orphan, parked_a)
            os.rename(replacement, orphan)
            try:
                return original_listdir(path)
            finally:
                os.rename(orphan, replacement)
                os.rename(parked_a, orphan)
        return original_listdir(path)

    monkeypatch.setattr(lab_worker_module.os, "listdir", swap_around_fd_enumeration)

    if expects_conflict:
        with pytest.raises(LabArtifactConflictError, match="orphan identity conflicts"):
            reclaimer._assert_external_orphan_identity(orphan, metadata)
    else:
        reclaimer._assert_external_orphan_identity(orphan, metadata)

    assert swapped
    assert (orphan.lstat().st_dev, orphan.lstat().st_ino) == original_a
    assert (replacement.lstat().st_dev, replacement.lstat().st_ino) == original_b
    assert ({child.name: child.read_bytes() for child in orphan.iterdir()}) == (
        {"a.bin": a_payload} if a_payload is not None else {}
    )
    assert ({child.name: child.read_bytes() for child in replacement.iterdir()}) == (
        {"b.bin": b_payload} if b_payload is not None else {}
    )
    assert (ledger.lstat().st_dev, ledger.lstat().st_ino, ledger.read_bytes()) == original_ledger


def test_external_orphan_metadata_entry_replacement_never_writes_business_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    preserved_orphan = tmp_path / "preserved-validated-orphan"
    original_metadata = reclaimer._ensure_legacy_empty_staging_orphan_metadata

    def replace_at_external_metadata_entry(
        orphan: Path,
        staging_id: UUID,
        orphan_token: UUID | None,
        expected_identity: tuple[int, int, int, int],
    ) -> None:
        os.rename(orphan, preserved_orphan)
        orphan.mkdir(mode=0o700)
        (orphan / "business.bin").write_bytes(b"business-directory-must-stay-pristine")
        original_metadata(orphan, staging_id, orphan_token, expected_identity)

    monkeypatch.setattr(
        reclaimer,
        "_ensure_legacy_empty_staging_orphan_metadata",
        replace_at_external_metadata_entry,
    )

    with pytest.raises(LabArtifactConflictError, match="orphan identity conflicts"):
        reclaimer.collect_garbage()

    orphans = tuple(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    assert len(orphans) == 1
    assert {child.name for child in orphans[0].iterdir()} == {"business.bin"}
    assert (orphans[0] / "business.bin").read_bytes() == b"business-directory-must-stay-pristine"
    assert tuple(reclaimer.garbage_orphan_metadata_dir.iterdir()) == ()
    assert tuple(orphans[0].glob("orphan.json")) == ()
    assert preserved_orphan.is_dir()


def test_external_orphan_metadata_is_authoritative_after_orphan_replacement(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    reclaimer.collect_garbage()
    orphan = next(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    ledger = reclaimer.garbage_orphan_metadata_dir / f"{orphan.name}.json"
    expected_ledger = ledger.read_bytes()
    preserved_orphan = tmp_path / "preserved-ledger-orphan"
    os.rename(orphan, preserved_orphan)
    orphan.mkdir(mode=0o700)
    (orphan / "business.bin").write_bytes(b"replacement-must-not-be-claimed")

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    with pytest.raises(LabArtifactConflictError, match="orphan identity conflicts"):
        restarted.collect_garbage()

    assert ledger.read_bytes() == expected_ledger
    assert {child.name for child in orphan.iterdir()} == {"business.bin"}
    assert (orphan / "business.bin").read_bytes() == b"replacement-must-not-be-claimed"
    assert tuple(orphan.glob("orphan.json")) == ()
    assert tuple(preserved_orphan.iterdir()) == ()


def test_legacy_internal_orphan_metadata_is_read_only_compatible(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    staging_id = uuid4()
    orphan = reclaimer.garbage_intent_orphan_dir / f"legacy-empty-staging-{staging_id.hex}"
    orphan.mkdir(mode=0o700)
    metadata = reclaimer._legacy_empty_staging_orphan_metadata(staging_id, None)
    marker = orphan / "orphan.json"
    marker.write_text(metadata.canonical_json(), encoding="utf-8")
    before = marker.lstat()
    before_bytes = marker.read_bytes()

    reclaimer.collect_garbage()

    after = marker.lstat()
    assert (after.st_dev, after.st_ino, after.st_nlink) == (
        before.st_dev,
        before.st_ino,
        before.st_nlink,
    )
    assert marker.read_bytes() == before_bytes
    assert {child.name for child in orphan.iterdir()} == {"orphan.json"}
    assert tuple(reclaimer.garbage_orphan_metadata_dir.iterdir()) == ()


def test_legacy_empty_staging_rename_occupation_preserves_both_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    original_rename = lab_worker_module.os.rename

    def occupy_after_orphan_rename(source: Path, target: Path) -> None:
        original_rename(source, target)
        if Path(source) == staging and Path(target).parent == reclaimer.garbage_intent_orphan_dir:
            staging.mkdir(mode=0o700)
            (staging / "concurrent.bin").write_bytes(b"concurrent-source-must-survive")

    monkeypatch.setattr(lab_worker_module.os, "rename", occupy_after_orphan_rename)

    with pytest.raises(LabArtifactConflictError, match="changed during orphan isolation"):
        reclaimer.collect_garbage()

    assert (staging / "concurrent.bin").read_bytes() == b"concurrent-source-must-survive"
    orphans = tuple(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    assert len(orphans) == 1
    assert tuple(orphans[0].iterdir()) == ()
    assert tuple(reclaimer.garbage_intent_orphan_dir.rglob("orphan.json")) == ()
    assert tuple(reclaimer.garbage_orphan_metadata_dir.iterdir()) == ()


@pytest.mark.parametrize("failure", ["missing", "both", "identity"])
def test_owner_only_staging_fails_closed_on_source_conflict(
    tmp_path: Path,
    failure: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "owner-conflict" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"expected-source")
    owner = reclaimer._garbage_owner(victim, purpose="owner-only conflict fixture")
    staging = reclaimer.garbage_staging_dir / owner.garbage_id.hex
    staging.mkdir(mode=0o700)
    reclaimer._write_garbage_owner(staging, owner)
    if failure == "missing":
        victim.unlink()
    elif failure == "both":
        (staging / "payload").write_bytes(victim.read_bytes())
    else:
        replacement = victim.with_suffix(".replacement")
        replacement.write_bytes(b"different-source")
        os.replace(replacement, victim)

    with pytest.raises(LabArtifactConflictError):
        reclaimer.collect_garbage()

    assert staging.is_dir()
    if failure != "missing":
        assert victim.is_file()


def test_sealed_rollback_crash_resumes_as_deferred_gc(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker.adapter_registry.validate_claim(claim)
    prepared = worker._prepare_result(
        claim,
        RecordingRegistry().execute_shard(validated, object()),
    )
    bundle = worker._publish_candidate(
        claim,
        prepared,
        deadline=None,
        effective_expiry=None,
        validate_concurrent_race=True,
    )
    original_promote = worker.artifact_reclaimer._promote_garbage_bundle
    interrupted = False

    def interrupt_after_rollback_isolation(source: Path, target: Path) -> None:
        nonlocal interrupted
        if source.parent == worker.artifact_reclaimer.garbage_staging_dir and not interrupted:
            interrupted = True
            raise InterruptedError("crash after sealed rollback isolation")
        original_promote(source, target)

    monkeypatch.setattr(
        worker.artifact_reclaimer,
        "_promote_garbage_bundle",
        interrupt_after_rollback_isolation,
    )
    with pytest.raises(InterruptedError, match="sealed rollback isolation"):
        worker._rollback_sealed(claim, bundle)

    assert not bundle.path.exists()
    assert len(tuple(worker.artifact_reclaimer.garbage_staging_dir.iterdir())) == 1
    monkeypatch.setattr(
        worker.artifact_reclaimer,
        "_promote_garbage_bundle",
        original_promote,
    )
    restarted = _worker(tmp_path)
    restarted.artifact_reclaimer.collect_garbage()
    restarted.artifact_reclaimer.reclaim(_retry_claim(claim))

    entries = restarted.artifact_reclaimer.quarantine_entries()
    assert any("sealed rollback" in entry.owner.purpose for entry in entries)
    assert all(entry.state == "deferred_gc" for entry in entries)


def test_sealed_rollback_hard_crash_resumes_in_new_process(tmp_path: Path) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker.adapter_registry.validate_claim(claim)
    worker._seal_result(
        claim,
        RecordingRegistry().execute_shard(validated, object()),
    )
    sealed = worker.sealed_bundle_path(claim)

    crashed = _run_worker_child(
        "_crash_sealed_rollback_after_payload_isolation_child",
        tmp_path,
        claim.model_dump_json(),
    )

    assert crashed.returncode == 83, crashed.stderr
    assert not sealed.exists()
    assert len(tuple(worker.artifact_reclaimer.garbage_staging_dir.iterdir())) == 1

    restarted = _worker(tmp_path)
    restarted.artifact_reclaimer.collect_garbage()
    restarted.artifact_reclaimer.reclaim(_retry_claim(claim))

    entries = restarted.artifact_reclaimer.quarantine_entries()
    assert len(entries) == 1
    assert entries[0].state == "deferred_gc"
    assert "sealed rollback" in entries[0].owner.purpose


@pytest.mark.parametrize(
    "crash_phase",
    ["intent", "staging", "global_owner", "bundle_owner", "prepared_ledger"],
)
def test_sealed_rollback_recovers_every_prepared_intent_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_phase: str,
) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker.adapter_registry.validate_claim(claim)
    prepared = worker._prepare_result(
        claim,
        RecordingRegistry().execute_shard(validated, object()),
    )
    bundle = worker._publish_candidate(
        claim,
        prepared,
        deadline=None,
        effective_expiry=None,
        validate_concurrent_race=True,
    )
    reclaimer = worker.artifact_reclaimer
    method_by_phase = {
        "intent": "_write_prepared_intent",
        "staging": "_ensure_garbage_staging",
        "global_owner": "_ensure_global_garbage_owner",
        "bundle_owner": "_ensure_bundle_garbage_owner",
        "prepared_ledger": "_write_garbage_ledger",
    }
    method_name = method_by_phase[crash_phase]
    original = getattr(reclaimer, method_name)
    interrupted = False

    def interrupt_after_phase(*args, **kwargs):
        nonlocal interrupted
        result = original(*args, **kwargs)
        state = kwargs.get("state")
        if len(args) > 1:
            state = args[1]
        should_interrupt = crash_phase != "prepared_ledger" or state == "prepared"
        if should_interrupt and not interrupted:
            interrupted = True
            raise InterruptedError(f"crash after {crash_phase}")
        return result

    monkeypatch.setattr(reclaimer, method_name, interrupt_after_phase)
    with pytest.raises(InterruptedError, match=crash_phase):
        worker._rollback_sealed(claim, bundle)
    monkeypatch.setattr(reclaimer, method_name, original)

    restarted = _worker(tmp_path)
    restarted.artifact_reclaimer.collect_garbage()
    restarted.artifact_reclaimer.reclaim(_retry_claim(claim))
    first = restarted.artifact_reclaimer.quarantine_summary()
    restarted.artifact_reclaimer.collect_garbage()
    second = restarted.artifact_reclaimer.quarantine_summary()

    assert not bundle.path.exists()
    assert first == second
    assert first.bundle_count == 1
    assert first.retained_bytes > 0


@pytest.mark.parametrize(
    "crash_phase",
    ["intent", "staging", "global_owner", "bundle_owner", "prepared_ledger"],
)
def test_sealed_rollback_hard_exit_recovers_every_prepared_intent_boundary(
    tmp_path: Path,
    crash_phase: str,
) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker.adapter_registry.validate_claim(claim)
    worker._seal_result(
        claim,
        RecordingRegistry().execute_shard(validated, object()),
    )

    crashed = _run_worker_child(
        "_crash_sealed_rollback_after_prepared_phase_child",
        tmp_path,
        claim.model_dump_json(),
        crash_phase,
    )

    assert crashed.returncode == 85, crashed.stderr
    restarted = _worker(tmp_path)
    restarted.artifact_reclaimer.collect_garbage()
    restarted.artifact_reclaimer.reclaim(_retry_claim(claim))
    summary = restarted.artifact_reclaimer.quarantine_summary()
    assert summary.bundle_count == 1
    assert summary.retained_bytes > 0


def test_quarantine_preserves_payload_replaced_after_final_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "logical-delete" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"validated-original")
    expected = reclaimer._regular_file_identity(victim, label="replacement fixture")
    reclaimer._safe_remove_regular_child(
        victim,
        expected=expected,
        label="replacement fixture",
    )
    saved_original = tmp_path / "saved-garbage-original.bin"
    original_validate = reclaimer._validate_garbage_bundle
    replaced = False

    def replace_after_validation(bundle: Path):
        nonlocal replaced
        owner = original_validate(bundle)
        if bundle.parent == reclaimer.garbage_deferred_dir and not replaced:
            replaced = True
            payload = bundle / "payload"
            os.replace(payload, saved_original)
            payload.write_bytes(b"external-replacement-must-survive")
        return owner

    monkeypatch.setattr(reclaimer, "_validate_garbage_bundle", replace_after_validation)

    reclaimer.collect_garbage()
    with pytest.raises(LabArtifactConflictError):
        reclaimer.collect_garbage()

    preserved = tuple(reclaimer.garbage_deferred_dir.rglob("payload"))
    assert len(preserved) == 1
    assert preserved[0].read_bytes() == b"external-replacement-must-survive"
    assert saved_original.read_bytes() == b"validated-original"


def test_quarantine_preserves_bundle_replaced_after_final_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "logical-delete" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"validated-original")
    expected = reclaimer._regular_file_identity(victim, label="owned replacement fixture")
    reclaimer._safe_remove_regular_child(
        victim,
        expected=expected,
        label="owned replacement fixture",
    )
    saved_original = tmp_path / "saved-owned-bundle"
    original_validate = reclaimer._validate_garbage_bundle
    replaced = False

    def replace_after_validation(bundle: Path):
        nonlocal replaced
        owner = original_validate(bundle)
        if bundle.parent == reclaimer.garbage_deferred_dir and not replaced:
            replaced = True
            os.rename(bundle, saved_original)
            bundle.mkdir()
            (bundle / "foreign.bin").write_bytes(b"foreign-must-survive")
        return owner

    monkeypatch.setattr(reclaimer, "_validate_garbage_bundle", replace_after_validation)

    reclaimer.collect_garbage()
    with pytest.raises(LabArtifactConflictError):
        reclaimer.collect_garbage()

    deferred = tuple(reclaimer.garbage_deferred_dir.iterdir())
    assert len(deferred) == 1
    assert (deferred[0] / "foreign.bin").read_bytes() == b"foreign-must-survive"
    assert (saved_original / "payload").read_bytes() == b"validated-original"


def test_deferred_bundle_remains_enumerable_across_restarts(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    victim = tmp_path / "artifacts" / "logical-delete" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"restart-owned-payload")
    expected = reclaimer._regular_file_identity(victim, label="restart fixture")
    reclaimer._safe_remove_regular_child(
        victim,
        expected=expected,
        label="restart fixture",
    )
    for _ in range(3):
        restarted = LabArtifactReclaimer(
            artifact_root=tmp_path / "artifacts",
            report_spool=LabReportSpool(tmp_path / "reports"),
        )
        restarted.collect_garbage()

    entry = restarted.quarantine_entries()[0]
    assert entry.state == "deferred_gc"
    assert (entry.bundle_path / "payload").read_bytes() == b"restart-owned-payload"


def test_quarantine_rejects_unknown_deferred_bundle(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    unknown = reclaimer.garbage_deferred_dir / uuid4().hex
    unknown.mkdir()

    with pytest.raises(LabArtifactConflictError):
        reclaimer.collect_garbage()

    assert unknown.is_dir()


def test_quarantine_fails_closed_when_payload_and_bundle_owner_disappear(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "logical-delete" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"final-cleanup-crash")
    identity = reclaimer._regular_file_identity(victim, label="final cleanup fixture")
    reclaimer._safe_remove_regular_child(
        victim,
        expected=identity,
        label="final cleanup fixture",
    )
    deferred = tuple(reclaimer.garbage_deferred_dir.iterdir())[0]
    (deferred / "payload").unlink()
    (deferred / "owner.json").unlink()

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    with pytest.raises(LabArtifactConflictError):
        restarted.collect_garbage()

    assert deferred.is_dir()
    assert tuple(restarted.garbage_ledger_dir.iterdir())


def test_reclaimer_does_not_isolate_directory_replaced_at_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    saved = sealed.parent / "saved-original"
    original_rename = lab_worker_module.os.rename

    def replace_at_rename(source: Path, target: Path) -> None:
        if Path(source) == sealed:
            original_rename(source, saved)
            sealed.mkdir()
            (sealed / "victim.txt").write_text("preserve", encoding="utf-8")
        original_rename(source, target)

    monkeypatch.setattr(lab_worker_module.os, "rename", replace_at_rename)
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )

    with pytest.raises(LabArtifactConflictError, match="replaced during isolation"):
        reclaimer.reclaim(current_claim)

    assert (sealed / "victim.txt").read_text(encoding="utf-8") == "preserve"
    assert saved.is_dir()
    assert tuple(sealed.parent.glob(".reclaim-*")) == ()


def test_stale_success_rejection_retries_failed_reconciliation(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_worker import LabArtifactReclaimer

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
    old_claim = claims.pending()[0].claim
    sealing_worker = _worker(
        tmp_path,
        claims=LabClaimSpool(tmp_path / "sealing-claims"),
        reports=reports,
    )
    validated = sealing_worker.adapter_registry.validate_claim(old_claim)
    result = RecordingRegistry().execute_shard(validated, object())
    manifest = sealing_worker._seal_result(old_claim, result)
    sealed = sealing_worker.sealed_bundle_path(old_claim)
    failed_once = False

    def flaky_reclaim(claim: LabShardClaim) -> None:
        nonlocal failed_once
        assert claims.current(claim.job_id, claim.shard_id).claim == claim
        if claim.claim_generation > old_claim.claim_generation and not failed_once:
            failed_once = True
            raise RuntimeError("injected first reconciliation failure")
        reclaimer.reclaim(claim)

    claims.set_claim_advance_hook(flaky_reclaim)
    clock[0] = NOW + timedelta(seconds=21)
    recovery = scheduler.run_once()
    fresh_claim = claims.current(old_claim.job_id, old_claim.shard_id).claim
    assert recovery.claim_reconcile_failures == 1
    assert sealed.is_dir()
    success = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=clock[0],
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    reports.publish(success)
    assert fresh_claim.claim_generation == old_claim.claim_generation + 1
    processed = scheduler.run_once()
    receipt = reports.load_receipt(reports.ack_dir / f"{success.report_id}.json")
    scheduler.release()

    assert processed.reports_rejected == 1
    assert processed.claims_reconciled == 1
    assert processed.claim_reconcile_failures == 0
    assert receipt.status == "rejected"
    assert not sealed.exists()


def test_scheduler_retires_accepted_success_from_hot_claim_authority(tmp_path: Path) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler

    reports = LabReportSpool(tmp_path / "reports")
    claims = LabClaimSpool(tmp_path / "claims")
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
    claim = claims.consume(claims.pending()[0])
    success = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash="a" * 64),
    )
    reports.publish(success)

    result = scheduler.run_once()

    assert result.reports_accepted == 1
    with pytest.raises(InvalidCommandEnvelopeError):
        claims.current(claim.job_id, claim.shard_id)
    retired = claims.retired_high_water(claim.job_id, claim.shard_id)
    assert retired.claim == claim
    assert retired.outcome == "accepted"


def test_scheduler_authority_fairly_retires_orphan_current_across_restart(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler

    claims = LabClaimSpool(tmp_path / "claims")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spec = _nshape_compare_spec(hold_days=(1,))
    for _ in range(2):
        commands.publish(
            LabCommandEnvelope(
                request_id=uuid4(),
                command=SubmitJobCommand(job_id=uuid4(), spec=spec, max_attempts=2),
            )
        )

    def scheduler(owner_id: str) -> LabScheduler:
        return LabScheduler(
            store=store,
            spool=commands,
            owner_id=owner_id,
            lease_seconds=60,
            heartbeat_seconds=10,
            poll_interval_ms=5,
            claim_spool=claims,
            claim_worker_ids=("worker-a", "worker-b"),
            shard_lease_seconds=20,
            max_claim_authority_per_tick=2,
            adapter_registry=default_strategy_job_adapter_registry(),
            clock=lambda: NOW,
        )

    first = scheduler("scheduler-a")
    first.run_once()
    orphan = _claim(spec)
    claims.consume(claims.publish(orphan))
    first.run_once()
    first.release()

    restarted = scheduler("scheduler-b")
    for _ in range(8):
        restarted.run_once()
        try:
            retired = claims.retired_high_water(orphan.job_id, orphan.shard_id)
        except InvalidCommandEnvelopeError:
            continue
        assert retired.claim == orphan
        assert retired.outcome == "revoked"
        break
    else:
        pytest.fail("orphan current claim was starved by persistent pending deliveries")

    with pytest.raises((LabClaimRevokedError, LabClaimSupersededError)):
        claims.admit_execution(orphan)
    assert len(claims.pending()) >= 2


def test_scheduler_retries_revocation_retirement_from_hot_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_shard_protocol import LabRetiredClaimAuthority

    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=uuid4(),
                spec=_nshape_compare_spec(hold_days=(1,)),
                max_attempts=1,
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
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    claim = claims.consume(claims.pending()[0])
    original_retire = claims.retire
    failed_once = False

    def fail_first_retire(
        stale: LabShardClaim,
        *,
        outcome: Literal["accepted", "revoked"],
        reason: str,
    ) -> LabRetiredClaimAuthority:
        nonlocal failed_once
        if outcome == "revoked" and not failed_once:
            failed_once = True
            raise OSError("injected cold archive outage")
        return original_retire(stale, outcome=outcome, reason=reason)

    monkeypatch.setattr(claims, "retire", fail_first_retire)
    clock[0] = NOW + timedelta(seconds=21)
    interrupted = scheduler.run_once()

    assert interrupted.claim_revoke_failures == 1
    assert claims.hot_delivery_batch(limit=8).claims == (claim,)

    recovered = scheduler.run_once()

    assert recovered.claim_revoke_failures == 0
    assert recovered.claims_retired == 1
    assert claims.hot_delivery_batch(limit=8).claims == ()
    assert claims.retired_high_water(claim.job_id, claim.shard_id).outcome == "revoked"


def test_scheduler_tick_history_work_is_active_plus_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_shard_protocol import LabClaimDeliveryReceipt, LabConsumedClaim

    reports = LabReportSpool(tmp_path / "reports")
    claims = LabClaimSpool(tmp_path / "claims")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    for _ in range(3):
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
        claim_worker_ids=("worker-a", "worker-b", "worker-c"),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: NOW,
    )
    scheduler.run_once()
    active = tuple(claims.consume(entry) for entry in claims.pending())
    assert len(active) == 3
    for index in range(5_000):
        token = UUID(int=index + 10_000)
        (claims.ack_dir / f"{token}.json").write_bytes(b"{}")
        report_id = UUID(int=index + 20_000)
        (reports.ack_dir / f"{report_id}.json").write_bytes(b"{}")

    consumed_parses = 0
    report_parses = 0
    original_consumed = claims._load_consumed_locked
    original_receipt = reports.load_receipt

    def count_consumed(token: UUID) -> LabConsumedClaim:
        nonlocal consumed_parses
        consumed_parses += 1
        path = claims.ack_dir / f"{token}.json"
        if path.read_bytes() == b"{}":
            return LabConsumedClaim(
                path=path,
                receipt=LabClaimDeliveryReceipt(claim=active[0]),
            )
        return original_consumed(token)

    def count_report(path: Path) -> LabReportReceipt:
        nonlocal report_parses
        report_parses += 1
        return original_receipt(path)

    monkeypatch.setattr(claims, "_load_consumed_locked", count_consumed)
    monkeypatch.setattr(reports, "load_receipt", count_report)

    result = scheduler.run_once()

    assert result.claim_revoke_failures == 0
    assert consumed_parses <= 12
    assert report_parses == 0
