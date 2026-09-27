"""A daily-bar audit command persists without writing or trusting the primary DB."""

from __future__ import annotations

import os
import shutil
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import (
    capture_data_audit_replica_identity,
    load_data_audit_report,
)
from rquant.data_audit_report_jobs import (
    DataAuditReportJobRequest,
    DataAuditReportJobStore,
    DataAuditReportJobWorker,
)
from tests.unit.test_data_audit_report import END, START, _database


class _Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 9, 30, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: int) -> None:
        self.now += timedelta(seconds=seconds)


def _sources(tmp_path: Path) -> tuple[Path, Path]:
    primary = _database(tmp_path / "primary.duckdb")
    replica = tmp_path / "replica.duckdb"
    shutil.copyfile(primary, replica)
    return primary, replica


def _request(
    primary: Path,
    replica: Path,
    *,
    key: str = "audit-command-0001",
) -> DataAuditReportJobRequest:
    return DataAuditReportJobRequest(
        idempotency_key=key,
        primary_path=primary,
        replica_path=replica,
        replica_file_identity=capture_data_audit_replica_identity(primary, replica),
        audit_start=START,
        observed_through=END,
        null_fields=(
            DailyBarNullFieldSpec(field_name="close", max_null_numerator=0, max_null_denominator=1),
        ),
    )


def _store(tmp_path: Path, clock: _Clock, *, lease_seconds: int = 30) -> DataAuditReportJobStore:
    return DataAuditReportJobStore(
        state_path=tmp_path / "audit-jobs.sqlite",
        report_directory=tmp_path / "reports",
        clock=clock,
        lease_seconds=lease_seconds,
    )


def test_submission_is_durable_idempotent_and_never_opens_duckdb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, replica = _sources(tmp_path)
    request = _request(primary, replica)
    clock = _Clock()
    store = _store(tmp_path, clock)

    def forbidden_connect(*args: object, **kwargs: object) -> None:
        raise AssertionError("submission opened DuckDB")

    with monkeypatch.context() as patch:
        patch.setattr(duckdb, "connect", forbidden_connect)
        first = store.submit(request)
        reopened = _store(tmp_path, clock)
        assert reopened.submit(request) == first
        assert reopened.lookup_by_key(request.idempotency_key) == (request, first)

    assert first.status == "queued"
    assert first.attempts == 0
    with pytest.raises(ValueError, match="idempotency"):
        store.submit(request.model_copy(update={"audit_start": START + timedelta(days=1)}))


def test_active_command_and_ten_minute_cooldown_apply_only_to_new_keys(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    first_request = _request(primary, replica)
    first = store.submit(first_request)

    with pytest.raises(ValueError, match="active"):
        store.submit(_request(primary, replica, key="audit-command-0002"))
    assert store.submit(first_request) == first
    assert DataAuditReportJobWorker(store).run_one().status == "succeeded"
    with pytest.raises(ValueError, match="cooldown"):
        store.submit(_request(primary, replica, key="audit-command-0002"))
    assert store.submit(first_request).status == "succeeded"
    clock.advance(600)
    assert store.submit(_request(primary, replica, key="audit-command-0002")).status == "queued"


def test_worker_publishes_verified_hash_and_bounded_safe_events(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    original = replica.read_bytes()
    clock = _Clock()
    store = _store(tmp_path, clock)
    queued = store.submit(_request(primary, replica))

    finished = DataAuditReportJobWorker(store).run_one()

    assert finished is not None and finished.task_id == queued.task_id
    assert finished.status == "succeeded" and finished.attempts == 1
    assert finished.report_hash is not None
    report = load_data_audit_report(
        tmp_path / "reports" / f"data-audit-v1-{finished.report_hash}.json"
    )
    assert report.content_hash == finished.report_hash
    assert report.source.mode == "production_unverified"
    assert report.collection_status == "collection_unconfirmed"
    assert report.source.snapshot_label.startswith("sha256:")
    assert replica.read_bytes() == original
    assert [event.event_type for event in store.list_events(queued.task_id)] == [
        "queued",
        "started",
        "source_check",
        "succeeded",
    ]
    assert store.latest_success() == finished


def test_rotated_replica_fails_and_does_not_replace_previous_success(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    store.submit(_request(primary, replica))
    success = DataAuditReportJobWorker(store).run_one()
    assert success is not None and success.report_hash is not None
    previous = tmp_path / "reports" / f"data-audit-v1-{success.report_hash}.json"
    before = previous.read_bytes()
    clock.advance(600)
    second_request = _request(primary, replica, key="audit-command-0002")
    queued = store.submit(second_request)
    replacement = _database(tmp_path / "replacement.duckdb")
    os.replace(replacement, replica)
    assert store.submit(second_request) == queued

    failed = DataAuditReportJobWorker(store).run_one()

    assert failed is not None and failed.task_id == queued.task_id
    assert failed.status == "failed" and failed.error_code == "replica_changed"
    assert failed.report_hash is None
    assert store.latest_success() == success
    assert previous.read_bytes() == before
    assert all(str(primary) not in str(event) for event in store.list_events(queued.task_id))


def test_crash_after_publish_recovers_from_persisted_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, replica = _sources(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    queued = store.submit(_request(primary, replica))

    def crash(*args: object, **kwargs: object) -> None:
        raise SystemExit("simulated crash")

    with monkeypatch.context() as patch:
        patch.setattr(store, "_finish_success", crash)
        with pytest.raises(SystemExit, match="simulated crash"):
            DataAuditReportJobWorker(store).run_one()

    assert store.status(queued.task_id).status == "running"
    assert len(list((tmp_path / "reports").glob("*.json"))) == 1
    clock.advance(31)
    recovered = DataAuditReportJobWorker(_store(tmp_path, clock)).run_one()
    assert recovered is not None and recovered.status == "succeeded"
    assert recovered.attempts == 2
    assert len(list((tmp_path / "reports").glob("*.json"))) == 1


def test_source_digest_is_durable_before_pinning_and_recovers_after_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report as artifact

    primary, replica = _sources(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    queued = store.submit(_request(primary, replica))

    def crash_before_pin(*args: object, **kwargs: object) -> None:
        raise SystemExit("crash before pin")

    with monkeypatch.context() as patch:
        patch.setattr(artifact.os, "link", crash_before_pin)
        with pytest.raises(SystemExit, match="crash before pin"):
            DataAuditReportJobWorker(store).run_one()

    with store._connect() as connection:
        digest = connection.execute(
            "SELECT replica_sha256 FROM data_audit_report_job WHERE task_id = ?",
            (queued.task_id,),
        ).fetchone()[0]
    assert isinstance(digest, str) and len(digest) == 64
    clock.advance(31)
    recovered = DataAuditReportJobWorker(_store(tmp_path, clock)).run_one()
    assert recovered is not None and recovered.status == "succeeded"


def test_recovery_rejects_same_inode_content_change_with_restored_mtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report as artifact

    primary, replica = _sources(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    task = store.submit(_request(primary, replica))

    def crash_before_pin(*args: object, **kwargs: object) -> None:
        raise SystemExit("crash before pin")

    with monkeypatch.context() as patch:
        patch.setattr(artifact.os, "link", crash_before_pin)
        with pytest.raises(SystemExit):
            DataAuditReportJobWorker(store).run_one()

    before = replica.stat()
    with replica.open("r+b") as handle:
        handle.seek(1024)
        original = handle.read(1)
        handle.seek(1024)
        handle.write(bytes([original[0] ^ 1]))
    os.utime(replica, ns=(before.st_atime_ns, before.st_mtime_ns))
    clock.advance(31)

    failed = DataAuditReportJobWorker(_store(tmp_path, clock)).run_one()
    assert failed is not None and failed.status == "failed"
    assert failed.error_code == "replica_changed"
    assert store.status(task.task_id) == failed
    assert not (tmp_path / "reports").exists()


def test_report_failure_preserves_previous_artifact_and_stores_only_safe_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report_jobs as jobs

    primary, replica = _sources(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    store.submit(_request(primary, replica))
    previous = DataAuditReportJobWorker(store).run_one()
    assert previous is not None and previous.report_hash is not None
    artifact = tmp_path / "reports" / f"data-audit-v1-{previous.report_hash}.json"
    before = artifact.read_bytes()
    clock.advance(600)
    next_task = store.submit(_request(primary, replica, key="audit-command-0002"))

    def report_failure(**kwargs: object) -> Path:
        raise ValueError("sensitive path /secret/snapshot.duckdb and raw detail")

    monkeypatch.setattr(jobs, "create_and_publish_data_audit_report", report_failure)
    failed = DataAuditReportJobWorker(store).run_one()

    assert failed is not None and failed.task_id == next_task.task_id
    assert failed.error_code == "invalid_evidence"
    assert store.latest_success() == previous
    assert artifact.read_bytes() == before
    with store._connect() as connection:
        raw = connection.execute(
            "SELECT error_code FROM data_audit_report_job WHERE task_id = ?",
            (next_task.task_id,),
        ).fetchone()[0]
    assert raw == "invalid_evidence"
    assert all("secret" not in str(event) for event in store.list_events(next_task.task_id))


def test_expired_old_claim_cannot_publish_status_over_new_claim(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    task = store.submit(_request(primary, replica))
    old = store._claim()
    assert old is not None
    clock.advance(31)
    resumed = store._claim()
    assert resumed is not None and resumed.token != old.token

    with pytest.raises(RuntimeError, match="lease"):
        store._finish_failure(old, "invalid_evidence")
    assert store.status(task.task_id).status == "running"
    assert store._finish_failure(resumed, "internal_error").error_code == "internal_error"


def test_repeated_recovery_keeps_only_bounded_safe_events(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    clock = _Clock()
    store = _store(tmp_path, clock)
    task = store.submit(_request(primary, replica))
    for _ in range(36):
        claim = store._claim()
        assert claim is not None
        clock.advance(31)

    events = store.list_events(task.task_id, limit=32)
    assert len(events) == 32
    assert events[0].event_type == "resumed"
    assert [event.event_id for event in events] == sorted(event.event_id for event in events)
    assert all(event.error_code is None for event in events)


def test_heartbeat_prevents_second_claim_during_long_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report_jobs as jobs

    primary, replica = _sources(tmp_path)

    def clock() -> datetime:
        return datetime.now(UTC)

    store = DataAuditReportJobStore(
        state_path=tmp_path / "audit-jobs.sqlite",
        report_directory=tmp_path / "reports",
        clock=clock,
        lease_seconds=3,
    )
    store.submit(_request(primary, replica))
    entered = threading.Event()
    release = threading.Event()
    real_create = jobs.create_and_publish_data_audit_report

    def delayed_create(**kwargs: object) -> Path:
        entered.set()
        assert release.wait(6)
        return real_create(**kwargs)

    monkeypatch.setattr(jobs, "create_and_publish_data_audit_report", delayed_create)
    result: list[object] = []
    runner = threading.Thread(
        target=lambda: result.append(DataAuditReportJobWorker(store).run_one())
    )
    runner.start()
    try:
        assert entered.wait(3)
        time.sleep(3.3)
        assert store._claim() is None
    finally:
        release.set()
        runner.join(6)
    assert not runner.is_alive()
    assert result[0].status == "succeeded"


@pytest.mark.parametrize("kind", ["fifo", "symlink", "wal", "primary_alias"])
def test_submission_rejects_unsafe_replica(tmp_path: Path, kind: str) -> None:
    primary, replica = _sources(tmp_path)
    if kind == "fifo":
        replica.unlink()
        os.mkfifo(replica)
    elif kind == "symlink":
        replica.unlink()
        replica.symlink_to(primary)
    elif kind == "wal":
        Path(f"{replica}.wal").write_bytes(b"pending")
    else:
        replica.unlink()
        os.link(primary, replica)
    clock = _Clock()
    store = _store(tmp_path, clock)
    with pytest.raises((OSError, ValueError)):
        store.submit(_request(primary, replica))
    assert store.latest() is None
