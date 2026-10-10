"""PageControl admits a read-only plan task without accepting browser file identity."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from rquant.backfill_plan_jobs import (
    BackfillPlanArtifactUnavailableError,
    BackfillPlanJobWorker,
)
from rquant.backfill_plan_page_backend import (
    BackfillPlanPageBackend,
    BackfillPlanPageBackendConfig,
)
from rquant.page_control import (
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SubmitBackfillPlan,
    parse_page_control_command,
)
from tests.unit.test_backfill_plan_artifact import END, START, _snapshot
from tests.unit.test_backfill_plan_core import _assumptions

NOW = datetime(2026, 2, 6, 2, tzinfo=UTC)


def _command(
    *,
    command_id: str = "backfill-plan-request-0001",
    actor_id: str = "admin",
) -> SubmitBackfillPlan:
    return SubmitBackfillPlan(
        command_id=command_id,
        requested_at=NOW,
        actor_id=actor_id,
        audit_start=START,
        completed_through=END,
    )


def _backend(tmp_path: Path, replica: Path) -> BackfillPlanPageBackend:
    tmp_path.mkdir(parents=True, exist_ok=True)
    primary = tmp_path / "primary.duckdb"
    primary.touch(exist_ok=True)
    return BackfillPlanPageBackend(
        BackfillPlanPageBackendConfig(
            primary_path=primary,
            replica_path=replica,
            state_path=tmp_path / "plan-jobs.sqlite",
            plan_directory=tmp_path / "plans",
            evidence_code_revision="test-revision",
            assumptions=_assumptions(),
        ),
        clock=lambda: NOW,
    )


def _service(
    tmp_path: Path,
    backend: BackfillPlanPageBackend | None,
    *,
    now: datetime = NOW,
) -> PageControlService:
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite")
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "page-data",
            log_dir=tmp_path / "page-logs",
            backfill_plan_backend=backend,
            clock=lambda: now,
            lease_seconds=1,
        ),
    )


def test_browser_command_rejects_source_identity_and_unbounded_dates() -> None:
    payload = _command().model_dump(mode="json")
    with pytest.raises(ValidationError):
        parse_page_control_command({**payload, "snapshot_path": "/tmp/main.duckdb"})
    with pytest.raises(ValidationError, match="range|3660"):
        SubmitBackfillPlan.model_validate(
            {**payload, "audit_start": (START - timedelta(days=3661)).isoformat()}
        )


def test_page_control_success_only_admits_a_task_and_replay_is_stable(tmp_path: Path) -> None:
    replica = _snapshot(tmp_path)
    backend = _backend(tmp_path, replica)
    command = _command()
    first = _service(tmp_path, backend).submit(command)

    assert first.status is PageControlStatus.SUCCEEDED
    assert first.result is not None
    assert first.result["outcome"] == "task_queued"
    assert "plan_hash" not in first.result
    task_id = first.result["task_id"]
    assert backend.store.status(task_id).status == "queued"
    assert _service(tmp_path, backend).submit(command) == first
    assert backend.store.status(task_id).attempts == 0


def test_crash_after_task_enqueue_recovers_before_rebinding_rotated_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    replica = _snapshot(tmp_path)
    backend = _backend(tmp_path, replica)
    command = _command()
    original_submit = backend.submit

    def crash_after_submit(value: SubmitBackfillPlan) -> object:
        original_submit(value)
        raise KeyboardInterrupt("crash after task enqueue")

    with monkeypatch.context() as patch:
        patch.setattr(backend, "submit", crash_after_submit)
        with pytest.raises(KeyboardInterrupt):
            _service(tmp_path, backend).submit(command)

    previous = backend.store.lookup_by_key(backend.idempotency_key(command))
    assert previous is not None
    original_task_id = previous[1].task_id
    (tmp_path / "new-source").mkdir()
    replacement = _snapshot(tmp_path / "new-source")
    os.replace(replacement, replica)
    recovered_backend = _backend(tmp_path, replica)
    recovered = _service(tmp_path, recovered_backend, now=NOW + timedelta(seconds=2)).submit(
        command
    )

    assert recovered.status is PageControlStatus.SUCCEEDED
    assert recovered.result == {"outcome": "task_queued", "task_id": original_task_id}
    assert recovered_backend.store.status(original_task_id).attempts == 0


def test_admission_recovers_even_if_completed_plan_is_later_damaged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    replica = _snapshot(tmp_path)
    backend = _backend(tmp_path, replica)
    command = _command(command_id="backfill-plan-damaged-artifact-0001")
    original_submit = backend.submit

    def crash_after_submit(value: SubmitBackfillPlan) -> object:
        original_submit(value)
        raise KeyboardInterrupt("crash after task enqueue")

    with monkeypatch.context() as patch:
        patch.setattr(backend, "submit", crash_after_submit)
        with pytest.raises(KeyboardInterrupt):
            _service(tmp_path, backend).submit(command)

    completed = BackfillPlanJobWorker(backend.store).run_one()
    assert completed is not None and completed.plan_hash is not None
    artifact = tmp_path / "plans" / f"daily-bar-backfill-plan-v1-{completed.plan_hash}.json"
    artifact.chmod(0o600)
    artifact.write_bytes(b"damaged")
    with pytest.raises(BackfillPlanArtifactUnavailableError):
        backend.store.status(completed.task_id)

    restarted = _service(
        tmp_path, _backend(tmp_path, replica), now=NOW + timedelta(seconds=2)
    ).submit(command)
    assert restarted.status is PageControlStatus.SUCCEEDED
    assert restarted.result == {"outcome": "task_queued", "task_id": completed.task_id}


def test_missing_backend_and_unavailable_source_fail_without_a_task(tmp_path: Path) -> None:
    replica = _snapshot(tmp_path)
    backend = _backend(tmp_path, replica)
    receipt = _service(tmp_path, None).submit(_command(command_id="missing-backend-0001"))
    assert receipt.status is PageControlStatus.FAILED
    assert (
        backend.store.lookup_by_key(
            backend.idempotency_key(_command(command_id="missing-backend-0001"))
        )
        is None
    )

    wal_path = Path(f"{replica}.wal")
    wal_path.write_bytes(b"unsealed")
    unavailable = _service(tmp_path, backend).submit(_command(command_id="unavailable-source-0001"))
    assert unavailable.status is PageControlStatus.FAILED
    assert (
        backend.store.lookup_by_key(
            backend.idempotency_key(_command(command_id="unavailable-source-0001"))
        )
        is None
    )


def test_same_command_id_with_different_actor_conflicts_before_new_task(tmp_path: Path) -> None:
    backend = _backend(tmp_path, _snapshot(tmp_path))
    service = _service(tmp_path, backend)
    first = service.submit(_command(actor_id="admin-a"))
    assert first.status is PageControlStatus.SUCCEEDED

    with pytest.raises(ValueError, match="different payload"):
        service.submit(_command(actor_id="admin-b"))
    with pytest.raises(ValueError, match="different payload"):
        service.submit(_command(actor_id="admin-a").model_copy(update={"completed_through": START}))
    assert backend.store.lookup_by_key(backend.idempotency_key(_command())) is not None


def test_trusted_backend_rejects_primary_alias_and_never_opens_duckdb_on_submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import backfill_plan_artifact as artifact

    replica = _snapshot(tmp_path)
    primary = tmp_path / "primary.duckdb"
    os.link(replica, primary)
    alias_backend = BackfillPlanPageBackend(
        BackfillPlanPageBackendConfig(
            primary_path=primary,
            replica_path=replica,
            state_path=tmp_path / "alias-jobs.sqlite",
            plan_directory=tmp_path / "alias-plans",
            evidence_code_revision="test-revision",
            assumptions=_assumptions(),
        ),
        clock=lambda: NOW,
    )
    alias_receipt = _service(tmp_path, alias_backend).submit(
        _command(command_id="primary-alias-0001")
    )
    assert alias_receipt.status is PageControlStatus.FAILED
    assert (
        alias_backend.store.lookup_by_key(
            alias_backend.idempotency_key(_command(command_id="primary-alias-0001"))
        )
        is None
    )

    primary.unlink()
    backend = _backend(tmp_path / "separate", replica)

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("submit touched DuckDB or streamed the source")

    monkeypatch.setattr(duckdb, "connect", forbidden)
    monkeypatch.setattr(artifact, "_file_sha256", forbidden)
    accepted = _service(tmp_path / "separate", backend).submit(
        _command(command_id="offline-submit-0001")
    )
    assert accepted.status is PageControlStatus.SUCCEEDED
