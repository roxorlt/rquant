"""Only a trusted PageControl backend may admit a read-only audit report task."""

from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report_jobs import (
    DataAuditReportArtifactUnavailableError,
    DataAuditReportJobWorker,
)
from rquant.data_audit_report_page_backend import (
    DataAuditReportPageBackend,
    DataAuditReportPageBackendConfig,
)
from rquant.page_control import (
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SubmitDataAuditReport,
    parse_page_control_command,
)
from rquant.page_control_service import build_page_control_service
from tests.unit.test_data_audit_report import END, START, _database

NOW = datetime(2026, 9, 30, 8, tzinfo=UTC)
NULL_FIELDS = (
    DailyBarNullFieldSpec(field_name="close", max_null_numerator=0, max_null_denominator=1),
)


def _sources(tmp_path: Path) -> tuple[Path, Path]:
    primary = _database(tmp_path / "primary.duckdb")
    replica = tmp_path / "replica.duckdb"
    shutil.copyfile(primary, replica)
    return primary, replica


def _command(
    command_id: str = "audit-report-command-0001", *, actor_id: str = "admin"
) -> SubmitDataAuditReport:
    return SubmitDataAuditReport(
        command_id=command_id,
        requested_at=NOW,
        actor_id=actor_id,
        audit_start=START,
        observed_through=END,
    )


def _backend(tmp_path: Path, primary: Path, replica: Path) -> DataAuditReportPageBackend:
    return DataAuditReportPageBackend(
        DataAuditReportPageBackendConfig(
            primary_path=primary,
            replica_path=replica,
            state_path=tmp_path / "audit-jobs.sqlite",
            report_directory=tmp_path / "reports",
            null_fields=NULL_FIELDS,
        ),
        clock=lambda: NOW,
    )


def _service(
    tmp_path: Path, backend: DataAuditReportPageBackend | None, *, now: datetime = NOW
) -> PageControlService:
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite")
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "page-data",
            log_dir=tmp_path / "page-logs",
            data_audit_report_backend=backend,
            clock=lambda: now,
            lease_seconds=1,
        ),
    )


def test_command_rejects_browser_owned_source_and_unbounded_dates() -> None:
    command = _command()
    payload = command.model_dump(mode="json")
    assert parse_page_control_command(payload) == command
    for field, value in (
        ("primary_path", "/tmp/primary.duckdb"),
        ("replica_path", "/tmp/replica.duckdb"),
        ("replica_file_identity", {"device": 1}),
        ("state_path", "/tmp/audit.sqlite"),
        ("null_fields", []),
    ):
        with pytest.raises(ValidationError):
            parse_page_control_command({**payload, field: value})
    with pytest.raises(ValidationError, match="range|3660"):
        parse_page_control_command(
            {**payload, "audit_start": (END - timedelta(days=3660)).isoformat()}
        )


def test_page_control_only_queues_task_and_replay_is_stable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, replica = _sources(tmp_path)
    backend = _backend(tmp_path, primary, replica)
    command = _command()

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("admission opened or streamed DuckDB")

    with monkeypatch.context() as patch:
        patch.setattr(duckdb, "connect", forbidden)
        first = _service(tmp_path, backend).submit(command)
        replay = _service(tmp_path, backend).submit(command)

    assert first.status is PageControlStatus.SUCCEEDED
    assert replay == first
    assert first.result is not None and first.result["outcome"] == "task_queued"
    assert set(first.result) == {"outcome", "task_id"}
    task_id = first.result["task_id"]
    assert backend.store.status(task_id).status == "queued"
    assert backend.store.status(task_id).attempts == 0
    assert not (tmp_path / "reports").exists()


def test_crash_after_queue_recovers_old_task_before_rotated_replica(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, replica = _sources(tmp_path)
    backend = _backend(tmp_path, primary, replica)
    command = _command()
    real_submit = backend.submit

    def crash_after_submit(value: SubmitDataAuditReport) -> object:
        real_submit(value)
        raise KeyboardInterrupt("after queue")

    with monkeypatch.context() as patch:
        patch.setattr(backend, "submit", crash_after_submit)
        with pytest.raises(KeyboardInterrupt, match="after queue"):
            _service(tmp_path, backend).submit(command)

    admitted = backend.store.lookup_by_key(backend.idempotency_key(command))
    assert admitted is not None
    old_task_id = admitted[1].task_id
    replacement = _database(tmp_path / "replacement.duckdb")
    os.replace(replacement, replica)
    recovered = _service(
        tmp_path, _backend(tmp_path, primary, replica), now=NOW + timedelta(seconds=2)
    ).submit(command)
    assert recovered.status is PageControlStatus.SUCCEEDED
    assert recovered.result == {"outcome": "task_queued", "task_id": old_task_id}


def test_crash_after_queue_recovers_binding_even_if_completed_artifact_is_damaged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, replica = _sources(tmp_path)
    backend = _backend(tmp_path, primary, replica)
    command = _command("audit-damaged-artifact-0001")
    real_submit = backend.submit

    def crash_after_submit(value: SubmitDataAuditReport) -> object:
        real_submit(value)
        raise KeyboardInterrupt("after queue")

    with monkeypatch.context() as patch:
        patch.setattr(backend, "submit", crash_after_submit)
        with pytest.raises(KeyboardInterrupt, match="after queue"):
            _service(tmp_path, backend).submit(command)

    completed = DataAuditReportJobWorker(backend.store).run_one()
    assert completed is not None and completed.report_hash is not None
    artifact = tmp_path / "reports" / f"data-audit-v1-{completed.report_hash}.json"
    artifact.chmod(0o600)
    artifact.write_bytes(b"damaged")
    with pytest.raises(DataAuditReportArtifactUnavailableError):
        backend.store.status(completed.task_id)

    restarted = _service(
        tmp_path, _backend(tmp_path, primary, replica), now=NOW + timedelta(seconds=2)
    ).submit(command)
    assert restarted.status is PageControlStatus.SUCCEEDED
    assert restarted.result == {"outcome": "task_queued", "task_id": completed.task_id}


def test_same_id_different_actor_or_dates_conflicts(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    backend = _backend(tmp_path, primary, replica)
    service = _service(tmp_path, backend)
    assert service.submit(_command()).status is PageControlStatus.SUCCEEDED
    with pytest.raises(ValueError, match="different payload"):
        service.submit(_command(actor_id="other-admin"))
    with pytest.raises(ValueError, match="different payload"):
        service.submit(_command().model_copy(update={"observed_through": START}))
    assert backend.store.latest() is not None


def test_missing_backend_or_replica_wal_fails_without_task(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    backend = _backend(tmp_path, primary, replica)
    missing = _service(tmp_path, None).submit(_command("audit-no-backend-0001"))
    assert missing.status is PageControlStatus.FAILED
    assert backend.store.latest() is None

    Path(f"{replica}.wal").write_bytes(b"unsealed")
    unavailable = _service(tmp_path, backend).submit(_command("audit-unsealed-0001"))
    assert unavailable.status is PageControlStatus.FAILED
    assert backend.store.latest() is None


def test_service_factory_accepts_explicit_backend_without_default_wiring(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    backend = _backend(tmp_path, primary, replica)
    service = build_page_control_service(
        outbox_path=tmp_path / "page-control.sqlite",
        data_dir=tmp_path / "page-data",
        log_dir=tmp_path / "page-logs",
        allowed_lab_export_roots=(),
        data_audit_report_backend=backend,
        load_default_lab_backend=False,
        clock=lambda: NOW,
    )

    receipt = service.submit(_command("audit-factory-0001"))
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert receipt.result is not None and receipt.result["outcome"] == "task_queued"


def test_replica_symlink_fifo_sidecar_and_primary_alias_fail_closed(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    backend = _backend(tmp_path, primary, replica)
    service = _service(tmp_path, backend)

    Path(f"{replica}.shm").write_bytes(b"unsealed")
    assert service.submit(_command("audit-shm-0001")).status is PageControlStatus.FAILED
    Path(f"{replica}.shm").unlink()

    replica.unlink()
    replica.symlink_to(primary)
    assert service.submit(_command("audit-symlink-0001")).status is PageControlStatus.FAILED
    replica.unlink()

    os.mkfifo(replica)
    assert service.submit(_command("audit-fifo-0001")).status is PageControlStatus.FAILED
    replica.unlink()

    os.link(primary, replica)
    assert service.submit(_command("audit-primary-alias-0001")).status is PageControlStatus.FAILED
    assert backend.store.latest() is None


def test_trusted_paths_reject_alias_symlink_fifo_and_noncanonical(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    state = tmp_path / "audit-jobs.sqlite"
    defaults = dict(
        primary_path=primary,
        replica_path=replica,
        state_path=state,
        report_directory=tmp_path / "reports",
        null_fields=NULL_FIELDS,
    )
    with pytest.raises(ValidationError, match="canonical"):
        DataAuditReportPageBackendConfig(
            **{**defaults, "state_path": tmp_path / "x" / ".." / "jobs.sqlite"}
        )
    state.symlink_to(replica)
    with pytest.raises(ValueError, match="state|symlink"):
        _backend(tmp_path, primary, replica)
    state.unlink()
    os.mkfifo(state)
    with pytest.raises(ValueError, match="state|regular"):
        _backend(tmp_path, primary, replica)
    state.unlink()
    os.link(replica, state)
    with pytest.raises(ValueError, match="alias"):
        _backend(tmp_path, primary, replica)


def test_audit_backend_absence_does_not_affect_other_page_commands(tmp_path: Path) -> None:
    from rquant.page_control import AppendNlQueryLog

    command = AppendNlQueryLog(
        command_id="unrelated-query-log-0001",
        requested_at=NOW,
        query="close > 10",
        outcome="success",
    )
    receipt = _service(tmp_path, None).submit(command)
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert receipt.result is not None


def test_cutoff_uses_shanghai_close_and_rejects_future_day(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    backend = _backend(tmp_path, primary, replica)
    command = _command().model_copy(update={"observed_through": NOW.date() + timedelta(days=1)})
    with pytest.raises(ValueError, match="Shanghai|close|future"):
        backend.submit(command)
    assert backend.store.latest() is None

    morning = DataAuditReportPageBackend(backend.config, clock=lambda: NOW - timedelta(hours=2))
    today = _command().model_copy(update={"observed_through": NOW.date()})
    with pytest.raises(ValueError, match="Shanghai|close|future"):
        morning.submit(today)
    assert morning.store.latest() is None


def test_worker_receipt_keeps_collection_unverified(tmp_path: Path) -> None:
    from rquant.data_audit_report import load_data_audit_report

    primary, replica = _sources(tmp_path)
    backend = _backend(tmp_path, primary, replica)
    accepted = _service(tmp_path, backend).submit(_command())
    assert accepted.result is not None
    completed = DataAuditReportJobWorker(backend.store).run_one()
    assert completed is not None and completed.status == "succeeded"
    report = load_data_audit_report(
        tmp_path / "reports" / f"data-audit-v1-{completed.report_hash}.json"
    )
    assert report.source.mode == "production_unverified"
    assert report.collection_status == "collection_unconfirmed"
