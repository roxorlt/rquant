"""A read-only plan task is durable across worker crashes and source changes."""

from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from rquant.backfill_plan_artifact import (
    capture_backfill_snapshot_identity,
    load_daily_bar_backfill_plan,
)
from rquant.backfill_plan_jobs import (
    BackfillPlanArtifactUnavailableError,
    BackfillPlanJobRequest,
    BackfillPlanJobStore,
    BackfillPlanJobWorker,
)
from tests.unit.test_backfill_plan_artifact import END, OBSERVED, START, _snapshot
from tests.unit.test_backfill_plan_core import _assumptions, _database


class _Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 2, 6, 2, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: int) -> None:
        self.now += timedelta(seconds=seconds)


def _request(snapshot: Path, *, key: str = "request-00000001") -> BackfillPlanJobRequest:
    return BackfillPlanJobRequest(
        idempotency_key=key,
        snapshot_path=snapshot,
        snapshot_file_identity=capture_backfill_snapshot_identity(snapshot),
        snapshot_label="fixed-replica",
        evidence_code_revision="revision-1",
        audit_start=START,
        completed_through=END,
        observed_at=OBSERVED,
        assumptions=_assumptions(),
    )


def _store(tmp_path: Path, clock: _Clock) -> BackfillPlanJobStore:
    return BackfillPlanJobStore(
        state_path=tmp_path / "jobs.sqlite",
        plan_directory=tmp_path / "plans",
        clock=clock,
        lease_seconds=30,
    )


def test_submit_is_durable_idempotent_and_rejects_changed_request(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    request = _request(snapshot)
    first = _store(tmp_path, clock).submit(request)

    reopened = _store(tmp_path, clock)
    assert reopened.submit(request) == first
    assert reopened.status(first.task_id) == first
    assert reopened.lookup_by_key(request.idempotency_key) == (request, first)
    assert first.status == "queued"
    assert first.attempts == 0

    with pytest.raises(ValueError, match="idempotency"):
        reopened.submit(request.model_copy(update={"evidence_code_revision": "revision-2"}))


def test_duplicate_submission_keeps_original_receipt_after_source_rotation(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    request = _request(snapshot)
    queued = store.submit(request)
    replacement = _database(tmp_path / "other.duckdb", START, END, [])
    os.replace(replacement, snapshot)

    assert store.submit(request) == queued


def test_unknown_key_lookup_does_not_allocate_or_open_snapshot(tmp_path: Path) -> None:
    clock = _Clock()
    store = _store(tmp_path, clock)
    assert store.lookup_by_key("request-00000001") is None
    with store._connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM backfill_plan_job").fetchone()[0] == 0


def test_request_rejects_unbounded_range_and_unsafe_path(tmp_path: Path) -> None:
    request = _request(_snapshot(tmp_path))
    with pytest.raises(ValidationError, match="3,660|3660|range"):
        BackfillPlanJobRequest.model_validate(
            {**request.model_dump(), "audit_start": START - timedelta(days=3661)}
        )
    with pytest.raises(ValidationError, match="absolute"):
        BackfillPlanJobRequest.model_validate(
            {**request.model_dump(), "snapshot_path": Path("replica.duckdb")}
        )


def test_worker_reports_success_only_after_plan_is_published_and_verified(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    before = snapshot.read_bytes()
    clock = _Clock()
    store = _store(tmp_path, clock)
    submitted = store.submit(_request(snapshot))

    finished = BackfillPlanJobWorker(store).run_one()

    assert finished is not None
    assert finished.task_id == submitted.task_id
    assert finished.status == "succeeded"
    assert finished.attempts == 1
    assert finished.plan_hash is not None
    plan_path = tmp_path / "plans" / f"daily-bar-backfill-plan-v1-{finished.plan_hash}.json"
    assert load_daily_bar_backfill_plan(plan_path).content_sha256 == finished.plan_hash
    assert snapshot.read_bytes() == before
    assert _store(tmp_path, clock).status(finished.task_id) == finished
    assert BackfillPlanJobWorker(store).run_one() is None


def test_zero_gap_plan_is_success_with_zero_missing_dates(tmp_path: Path) -> None:
    from datetime import timedelta

    days = [START + timedelta(days=offset) for offset in range((END - START).days + 1)]
    snapshot = _database(
        tmp_path / "replica.duckdb",
        START,
        END,
        [("600000.SH", day) for day in days if day.weekday() < 5],
    )
    clock = _Clock()
    store = _store(tmp_path, clock)
    store.submit(_request(snapshot))

    finished = BackfillPlanJobWorker(store).run_one()

    assert finished is not None and finished.status == "succeeded"
    assert finished.plan_hash is not None
    plan = load_daily_bar_backfill_plan(
        tmp_path / "plans" / f"daily-bar-backfill-plan-v1-{finished.plan_hash}.json"
    )
    assert plan.missing_dates == ()
    assert plan.estimate.logical_operations.total == 0


def test_crash_after_publish_retries_same_request_without_false_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    queued = store.submit(_request(snapshot))

    def crash(*args: object, **kwargs: object) -> None:
        raise SystemExit("crash after artifact publication")

    with monkeypatch.context() as patch:
        patch.setattr(store, "_finish_success", crash)
        with pytest.raises(SystemExit, match="crash after"):
            BackfillPlanJobWorker(store).run_one()

    assert store.status(queued.task_id).status == "running"
    artifact_names = [path.name for path in (tmp_path / "plans").glob("*.json")]
    assert len(artifact_names) == 1
    clock.advance(31)

    recovered = BackfillPlanJobWorker(_store(tmp_path, clock)).run_one()

    assert recovered is not None and recovered.status == "succeeded"
    assert recovered.attempts == 2
    assert [path.name for path in (tmp_path / "plans").glob("*.json")] == artifact_names


def test_crash_after_hash_before_publish_recovers_despite_link_ctime_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import backfill_plan_artifact as artifact

    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    queued = store.submit(_request(snapshot))
    before_ctime = snapshot.stat().st_ctime_ns

    def crash(*args: object, **kwargs: object) -> None:
        raise SystemExit("crash before publish")

    with monkeypatch.context() as patch:
        patch.setattr(artifact, "_publish_plan", crash)
        with pytest.raises(SystemExit, match="crash before publish"):
            BackfillPlanJobWorker(store).run_one()

    assert store.status(queued.task_id).status == "running"
    assert snapshot.stat().st_ctime_ns != before_ctime
    clock.advance(31)
    recovered = BackfillPlanJobWorker(_store(tmp_path, clock)).run_one()
    assert recovered is not None and recovered.status == "succeeded"
    assert recovered.attempts == 2


def test_retry_rejects_mutated_content_even_if_size_and_mtime_are_restored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import backfill_plan_artifact as artifact

    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    store.submit(_request(snapshot))

    def crash(*args: object, **kwargs: object) -> None:
        raise SystemExit("crash before publish")

    with monkeypatch.context() as patch:
        patch.setattr(artifact, "_publish_plan", crash)
        with pytest.raises(SystemExit):
            BackfillPlanJobWorker(store).run_one()

    before = snapshot.stat()
    with snapshot.open("r+b") as handle:
        handle.seek(1024)
        original = handle.read(1)
        handle.seek(1024)
        handle.write(bytes([original[0] ^ 1]))
    os.utime(snapshot, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert snapshot.stat().st_size == before.st_size
    assert snapshot.stat().st_mtime_ns == before.st_mtime_ns
    clock.advance(31)

    result = BackfillPlanJobWorker(_store(tmp_path, clock)).run_one()

    assert result is not None and result.status == "failed"
    assert result.error_code == "snapshot_changed"


def test_changed_snapshot_fails_without_publishing_and_retry_keeps_identity(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    request = _request(snapshot)
    queued = store.submit(request)
    replacement = _database(
        tmp_path / "other.duckdb", START, END, [("600000.SH", START)]
    )
    os.replace(replacement, snapshot)

    failed = BackfillPlanJobWorker(store).run_one()

    assert failed is not None and failed.status == "failed"
    assert failed.error_code == "snapshot_changed"
    assert failed.plan_hash is None
    assert not (tmp_path / "plans").exists()
    assert store.retry_failed(queued.task_id).status == "queued"
    assert BackfillPlanJobWorker(store).run_one().status == "failed"


def test_invalid_evidence_is_failure_and_not_success(tmp_path: Path) -> None:
    snapshot = tmp_path / "empty.duckdb"
    with duckdb.connect(str(snapshot)):
        pass
    clock = _Clock()
    store = _store(tmp_path, clock)
    store.submit(_request(snapshot))

    failed = BackfillPlanJobWorker(store).run_one()

    assert failed is not None and failed.status == "failed"
    assert failed.error_code == "invalid_evidence"
    assert failed.plan_hash is None


def test_damaged_result_is_never_read_as_success(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    request = _request(snapshot)
    store.submit(request)
    result = BackfillPlanJobWorker(store).run_one()
    assert result is not None and result.plan_hash is not None
    path = tmp_path / "plans" / f"daily-bar-backfill-plan-v1-{result.plan_hash}.json"
    path.chmod(0o600)
    path.write_bytes(b"broken")

    with pytest.raises(BackfillPlanArtifactUnavailableError):
        store.status(result.task_id)
    with pytest.raises(BackfillPlanArtifactUnavailableError):
        store.submit(request)


def test_stale_worker_cannot_finish_after_expired_lease(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    queued = store.submit(_request(snapshot))
    original = store._claim()
    assert original is not None
    clock.advance(31)
    replacement = store._claim()
    assert replacement is not None and replacement.task_id == queued.task_id
    assert replacement.token != original.token

    with pytest.raises(RuntimeError, match="lease"):
        store._finish_failure(original, "internal_error")
    assert store.status(queued.task_id).status == "running"
    assert store.status(queued.task_id).attempts == 2


def test_submit_does_not_open_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant import backfill_plan_artifact as artifact

    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("submit opened DuckDB")

    monkeypatch.setattr(duckdb, "connect", forbidden)
    monkeypatch.setattr(artifact, "_file_sha256", forbidden)
    assert store.submit(_request(snapshot)).status == "queued"


def test_alias_or_wal_cannot_enter_the_fixed_source_task(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    alias = tmp_path / "alias.duckdb"
    alias.symlink_to(snapshot)
    with pytest.raises(OSError):
        capture_backfill_snapshot_identity(alias)

    clock = _Clock()
    store = _store(tmp_path, clock)
    request = _request(snapshot)
    wal_path = Path(f"{snapshot}.wal")
    wal_path.write_bytes(b"unsealed")
    with pytest.raises(ValueError, match="WAL"):
        store.submit(request)
    wal_path.unlink()
    wal_path.symlink_to(tmp_path / "missing-wal")
    with pytest.raises(ValueError, match="WAL"):
        store.submit(request)


def test_identical_bytes_replaced_after_submit_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import backfill_plan_jobs as jobs

    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    store.submit(_request(snapshot))
    original_publish = jobs.create_and_publish_daily_bar_backfill_plan

    def replace_before_publish(**kwargs: object) -> Path:
        replacement = tmp_path / "replacement.duckdb"
        shutil.copyfile(snapshot, replacement)
        os.replace(replacement, snapshot)
        return original_publish(**kwargs)

    monkeypatch.setattr(jobs, "create_and_publish_daily_bar_backfill_plan", replace_before_publish)
    failed = BackfillPlanJobWorker(store).run_one()

    assert failed is not None and failed.status == "failed"
    assert failed.error_code == "snapshot_changed"
    assert failed.plan_hash is None


def test_worker_does_not_use_network_or_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import socket

    snapshot = _snapshot(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    store.submit(_request(snapshot))
    monkeypatch.setenv("RQUANT_DISABLE_DOTENV", "1")

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("network was touched")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    assert BackfillPlanJobWorker(store).run_one().status == "succeeded"
