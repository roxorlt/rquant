"""One factor worker run leaves only durable, fenced terminal evidence."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Event
from time import monotonic

import pytest

from rquant.data_metadata import DatasetSnapshot, DatasetSnapshotBinding
from rquant.factor.job_ledger import FactorEvaluationJobLedger
from rquant.factor.job_runner import FactorEvaluationCompletion
from rquant.factor.job_worker import FactorJobWorkerResult, run_one_factor_job
from rquant.factor_snapshot_admission import (
    FactorSnapshotAdmissionDecision,
    FactorSnapshotAdmissionError,
    FactorSnapshotAdmissionFailure,
)
from tests.unit.test_factor_historical_adapter import _admitted
from tests.unit.test_factor_job_ledger import NOW, _Clock, _ledger, _sealed
from tests.unit.test_factor_job_runner import _artifact_root, _spec


class _UnusedMetadata:
    def get_dataset_snapshot(self, _snapshot_id: str) -> DatasetSnapshot | None:
        raise AssertionError("mocked runner must not query metadata")

    def get_dataset_snapshot_binding(self, _snapshot_id: str) -> DatasetSnapshotBinding | None:
        raise AssertionError("mocked runner must not query metadata")


def _once(
    ledger: FactorEvaluationJobLedger,
    tmp_path: Path,
    artifact_root: Path,
    *,
    join_timeout: float = 0.5,
) -> FactorJobWorkerResult:
    return run_one_factor_job(
        ledger,
        metadata_store=_UnusedMetadata(),
        lake_root=tmp_path / "unused-lake",
        artifact_root=artifact_root,
        runner_now=lambda: NOW,
        lease_seconds=1,
        heartbeat_interval_seconds=0.01,
        heartbeat_join_timeout_seconds=join_timeout,
    )


def _wait_until(check: Callable[[], bool], *, timeout: float = 2.0) -> None:
    deadline = monotonic() + timeout
    while not check():
        if monotonic() >= deadline:
            pytest.fail("timed out waiting for worker interleaving")
        Event().wait(0.005)


def test_real_synthetic_job_succeeds_once_and_reopens_with_one_result(tmp_path: Path) -> None:
    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        spec = _spec(snapshot, binding)
        ledger_root = tmp_path / "ledger"
        ledger_root.mkdir(mode=0o700)
        clock = _Clock(snapshot.as_of_time)
        ledger = FactorEvaluationJobLedger(ledger_root / "factor-jobs.sqlite3", clock=clock)
        identity = ledger.initialize()
        submitted = ledger.submit("request-1", spec)
        artifact_root = _artifact_root(tmp_path)

        outcome = run_one_factor_job(
            ledger,
            metadata_store=store,
            lake_root=tmp_path / "lake",
            artifact_root=artifact_root,
            runner_now=lambda: snapshot.as_of_time,
            lease_seconds=5,
            heartbeat_interval_seconds=0.05,
        )

        assert outcome.status == "succeeded"
        assert outcome.job_id == submitted.job_id
        assert outcome.record is not None
        assert outcome.record.status == "succeeded"
        assert outcome.record.completion is not None
        assert outcome.record.completion.spec_sha256 == spec.spec_sha256
        reopened = FactorEvaluationJobLedger.open_existing(identity, clock=clock)
        assert reopened.get(submitted.job_id) == outcome.record
        assert reopened.submit("request-2", spec).job_id == submitted.job_id
        assert reopened.list_recent() == (outcome.record,)
        assert (
            run_one_factor_job(
                reopened,
                metadata_store=store,
                lake_root=tmp_path / "lake",
                artifact_root=artifact_root,
                runner_now=lambda: snapshot.as_of_time,
                lease_seconds=5,
                heartbeat_interval_seconds=0.05,
            ).status
            == "idle"
        )


def test_real_unavailable_snapshot_records_source_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        spec = _spec(snapshot, binding)
        ledger_root = tmp_path / "ledger"
        ledger_root.mkdir(mode=0o700)
        ledger = FactorEvaluationJobLedger(
            ledger_root / "factor-jobs.sqlite3", clock=_Clock(snapshot.as_of_time)
        )
        ledger.initialize()
        job = ledger.submit("request-1", spec)
        artifact_root = _artifact_root(tmp_path)
        monkeypatch.setattr(store, "get_dataset_snapshot", lambda _snapshot_id: None)

        outcome = run_one_factor_job(
            ledger,
            metadata_store=store,
            lake_root=tmp_path / "lake",
            artifact_root=artifact_root,
            runner_now=lambda: snapshot.as_of_time,
            lease_seconds=5,
            heartbeat_interval_seconds=0.05,
        )

        assert outcome.status == "failed"
        assert outcome.record is not None
        assert outcome.record.failure_code == "source_unavailable"
        assert ledger.get(job.job_id) == outcome.record
        assert list(artifact_root.iterdir()) == []


def test_long_compute_renews_lease_and_uses_latest_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_worker as module

    spec, root, completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = _ledger(tmp_path, clock=clock)
    job = ledger.submit("request-1", spec)

    def compute(*_args: object, **_kwargs: object) -> FactorEvaluationCompletion:
        _wait_until(lambda: ledger.get(job.job_id).version >= 2)
        clock.instant = NOW + timedelta(milliseconds=500)
        initial_expiry = NOW + timedelta(seconds=1)
        _wait_until(lambda: ledger.get(job.job_id).lease_expires_at > initial_expiry)
        clock.instant = NOW + timedelta(milliseconds=1100)
        return completion

    monkeypatch.setattr(module, "run_factor_evaluation_job", compute)
    outcome = _once(ledger, tmp_path, root)
    assert outcome.status == "succeeded"
    assert outcome.record is not None and outcome.record.version >= 4
    assert outcome.record.completion == completion


def test_inflight_heartbeat_finishes_before_terminal_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_worker as module

    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)
    entered = Event()
    release = Event()
    original = ledger.heartbeat

    def delayed_heartbeat(*args: object, **kwargs: object) -> object:
        entered.set()
        assert release.wait(2)
        return original(*args, **kwargs)

    def compute(*_args: object, **_kwargs: object) -> FactorEvaluationCompletion:
        assert entered.wait(2)
        return completion

    monkeypatch.setattr(ledger, "heartbeat", delayed_heartbeat)
    monkeypatch.setattr(module, "run_factor_evaluation_job", compute)
    with ThreadPoolExecutor(max_workers=1) as executor:
        result = executor.submit(_once, ledger, tmp_path, root)
        try:
            assert entered.wait(2)
            assert ledger.get(job.job_id).status == "running"
            assert not result.done()
        finally:
            release.set()
        outcome = result.result(timeout=3)
    assert outcome.status == "succeeded"
    assert outcome.record is not None and outcome.record.version == 3


def test_another_claim_fences_heartbeat_and_old_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_worker as module

    spec, root, completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = _ledger(tmp_path, clock=clock)
    job = ledger.submit("request-1", spec)
    attempted = Event()
    original = ledger.heartbeat

    def fenced_heartbeat(*args: object, **kwargs: object) -> object:
        attempted.set()
        return original(*args, **kwargs)

    def compute(*_args: object, **_kwargs: object) -> FactorEvaluationCompletion:
        clock.instant = NOW + timedelta(seconds=2)
        replacement = ledger.claim(lease_seconds=10)
        assert replacement is not None and replacement.job.job_id == job.job_id
        assert attempted.wait(2)
        return completion

    monkeypatch.setattr(ledger, "heartbeat", fenced_heartbeat)
    monkeypatch.setattr(module, "run_factor_evaluation_job", compute)
    outcome = _once(ledger, tmp_path, root)
    assert outcome.status == "lease_lost" and outcome.record is None
    state = ledger.get(job.job_id)
    assert state.status == "running" and state.attempts == 2


def test_heartbeat_error_never_reports_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_worker as module

    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)
    attempted = Event()

    def failed_heartbeat(*_args: object, **_kwargs: object) -> None:
        attempted.set()
        raise RuntimeError("internal heartbeat path /private/secret")

    def compute(*_args: object, **_kwargs: object) -> FactorEvaluationCompletion:
        assert attempted.wait(2)
        return completion

    monkeypatch.setattr(ledger, "heartbeat", failed_heartbeat)
    monkeypatch.setattr(module, "run_factor_evaluation_job", compute)
    outcome = _once(ledger, tmp_path, root)
    assert outcome.status == "lease_lost"
    assert ledger.get(job.job_id).status == "running"
    assert "secret" not in outcome.model_dump_json()


@pytest.mark.parametrize(
    ("failure", "reason"),
    (
        (RuntimeError("compute /private/secret"), "evaluation_failed"),
        (TimeoutError("runner clock deadline"), "evaluation_failed"),
        (OSError("I/O stage unknown /private/secret"), "internal_error"),
        (
            FactorSnapshotAdmissionError(
                FactorSnapshotAdmissionDecision(
                    allowed=False,
                    research_status="exploratory",
                    snapshot_id="a" * 64,
                    binding_hash="b" * 64,
                    as_of_time=None,
                    source_mode="historical_retrospective",
                    source_read_boundary=None,
                    failures=(
                        FactorSnapshotAdmissionFailure(
                            code="snapshot_missing", message="/private/secret"
                        ),
                    ),
                )
            ),
            "source_unavailable",
        ),
    ),
)
def test_runner_failure_records_only_safe_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    reason: str,
) -> None:
    from rquant.factor import job_worker as module

    spec, root, _completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)

    def failed_runner(*_args: object, **_kwargs: object) -> None:
        raise failure

    monkeypatch.setattr(module, "run_factor_evaluation_job", failed_runner)
    outcome = _once(ledger, tmp_path, root)
    assert outcome.status == "failed"
    assert outcome.record is not None and outcome.record.failure_code == reason
    assert ledger.get(job.job_id) == outcome.record
    assert "secret" not in outcome.model_dump_json()


def test_deadline_expiry_before_completion_cannot_be_rewritten_by_old_runner_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_worker as module

    spec, root, completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = _ledger(tmp_path, clock=clock)
    job = ledger.submit("request-1", spec)

    def expired_compute(*_args: object, **_kwargs: object) -> FactorEvaluationCompletion:
        clock.instant = spec.deadline
        return completion

    monkeypatch.setattr(module, "run_factor_evaluation_job", expired_compute)
    outcome = _once(ledger, tmp_path, root)
    assert outcome.status == "lease_lost"
    assert ledger.get(job.job_id).status == "running"
    assert ledger.claim(lease_seconds=1) is None
    state = ledger.get(job.job_id)
    assert state.status == "failed" and state.failure_code == "deadline_expired"


def test_sealed_artifact_survives_complete_failure_and_next_claim_reuses_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_worker as module

    spec, root, completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = _ledger(tmp_path, clock=clock)
    job = ledger.submit("request-1", spec)
    monkeypatch.setattr(module, "run_factor_evaluation_job", lambda *_args, **_kwargs: completion)
    original = ledger.complete

    def interrupted_complete(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("transaction interrupted /private/secret")

    monkeypatch.setattr(ledger, "complete", interrupted_complete)
    first = _once(ledger, tmp_path, root)
    assert first.status == "lease_lost"
    assert ledger.get(job.job_id).status == "running"
    assert (root / completion.artifact_filename).is_file()
    monkeypatch.setattr(ledger, "complete", original)
    clock.instant = NOW + timedelta(seconds=2)
    second = _once(ledger, tmp_path, root)
    assert second.status == "succeeded"
    assert second.record is not None and second.record.job_id == job.job_id
    assert second.record.attempts == 2
    assert (root / completion.artifact_filename).is_file()


def test_failure_write_fenced_by_new_claim_returns_lease_lost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_worker as module

    spec, root, _completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = _ledger(tmp_path, clock=clock)
    job = ledger.submit("request-1", spec)

    def failed_compute(*_args: object, **_kwargs: object) -> None:
        clock.instant = NOW + timedelta(seconds=2)
        replacement = ledger.claim(lease_seconds=10)
        assert replacement is not None and replacement.job.job_id == job.job_id
        raise RuntimeError("old execution failed")

    monkeypatch.setattr(module, "run_factor_evaluation_job", failed_compute)
    outcome = _once(ledger, tmp_path, root)
    assert outcome.status == "lease_lost"
    state = ledger.get(job.job_id)
    assert state.status == "running" and state.attempts == 2


def test_join_timeout_leaves_running_job_for_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_worker as module

    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)
    entered = Event()
    release = Event()
    exited = Event()
    original = ledger.heartbeat

    def blocked_heartbeat(*args: object, **kwargs: object) -> object:
        entered.set()
        try:
            assert release.wait(2)
            return original(*args, **kwargs)
        finally:
            exited.set()

    def compute(*_args: object, **_kwargs: object) -> FactorEvaluationCompletion:
        assert entered.wait(2)
        return completion

    monkeypatch.setattr(ledger, "heartbeat", blocked_heartbeat)
    monkeypatch.setattr(module, "run_factor_evaluation_job", compute)
    try:
        outcome = _once(ledger, tmp_path, root, join_timeout=0.02)
        assert outcome.status == "lease_lost"
        assert ledger.get(job.job_id).status == "running"
    finally:
        release.set()
        assert exited.wait(2)


@pytest.mark.parametrize(
    ("lease_seconds", "heartbeat_interval_seconds", "heartbeat_join_timeout_seconds"),
    ((0, 0.01, 0.5), (1, 0.5, 0.5), (1, 0.01, 31.0)),
)
def test_invalid_timing_is_rejected_before_claim(
    tmp_path: Path,
    lease_seconds: int,
    heartbeat_interval_seconds: float,
    heartbeat_join_timeout_seconds: float,
) -> None:
    spec, root, _completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)
    with pytest.raises(ValueError, match="worker"):
        run_one_factor_job(
            ledger,
            metadata_store=_UnusedMetadata(),
            lake_root=tmp_path / "unused-lake",
            artifact_root=root,
            runner_now=lambda: NOW,
            lease_seconds=lease_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            heartbeat_join_timeout_seconds=heartbeat_join_timeout_seconds,
        )
    assert ledger.get(job.job_id).status == "queued"
