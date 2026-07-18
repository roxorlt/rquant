"""SQLite-backed manifest state for resumable historical backfills."""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier

import pytest


def _manifest(
    *,
    manifest_id: str = "manifest-1",
    task_count: int = 1,
    max_attempts: int = 3,
):
    from rquant.backfill_state import (
        BackfillEligibilityInput,
        BackfillManifestInput,
        BackfillTaskInput,
    )

    return BackfillManifestInput(
        manifest_id=manifest_id,
        payload={"strategy": "growth_board_surge", "version": 1},
        tasks=tuple(
            BackfillTaskInput(
                task_id=f"task-{index}",
                payload={"ts_code": f"30000{index}.SZ", "trade_date": "2026-07-13"},
                max_attempts=max_attempts,
            )
            for index in range(task_count)
        ),
        eligibility=(
            BackfillEligibilityInput(
                eligibility_id="eligibility-1",
                payload={"ts_code": "300001.SZ", "trade_date": "2026-07-13"},
            ),
        ),
    )


def test_manifest_tasks_and_eligibility_persist_atomically_and_idempotently(
    tmp_path: Path,
) -> None:
    from rquant.backfill_state import (
        BackfillStateStore,
        ManifestContentConflictError,
    )

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    manifest = _manifest(task_count=2)

    store.persist_manifest(manifest)
    store.persist_manifest(manifest)

    assert store.load_manifest(manifest.manifest_id) == manifest
    status = store.get_manifest_status(manifest.manifest_id)
    assert status.task_count == 2
    assert status.eligibility_count == 1

    changed = manifest.model_copy(
        update={
            "payload": {"strategy": "growth_board_surge", "version": 2},
            "tasks": manifest.tasks
            + (
                manifest.tasks[0].model_copy(
                    update={"task_id": "task-added"},
                ),
            ),
        }
    )
    with pytest.raises(ManifestContentConflictError, match="manifest-1"):
        store.persist_manifest(changed)

    assert store.load_manifest(manifest.manifest_id) == manifest
    assert store.get_manifest_status(manifest.manifest_id).task_count == 2


def test_load_manifest_rejects_tampered_task_content(
    tmp_path: Path,
) -> None:
    from rquant.backfill_state import (
        BackfillStateStore,
        ManifestContentConflictError,
    )

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    manifest = _manifest()
    store.persist_manifest(manifest)
    connection = sqlite3.connect(store.path)
    try:
        connection.execute(
            """
            UPDATE backfill_task
            SET payload_json = '{"trade_date":"2026-07-20","ts_code":"300001.SZ"}'
            WHERE manifest_id = ? AND task_id = ?
            """,
            (manifest.manifest_id, manifest.tasks[0].task_id),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(ManifestContentConflictError, match="content hash"):
        store.load_manifest(manifest.manifest_id)


def test_store_enables_wal_and_busy_timeout_on_every_connection(tmp_path: Path) -> None:
    from rquant.backfill_state import BackfillStateStore

    store = BackfillStateStore(
        tmp_path / "backfill.sqlite3",
        busy_timeout_ms=1_234,
    )

    connection = store._connect()
    try:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        busy_timeout = connection.execute("PRAGMA busy_timeout").fetchone()[0]
    finally:
        connection.close()

    assert journal_mode == "wal"
    assert busy_timeout == 1_234


def test_begin_immediate_claim_allows_only_one_worker_to_claim_a_task(
    tmp_path: Path,
) -> None:
    from rquant.backfill_state import BackfillStateStore

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    store.persist_manifest(_manifest())
    barrier = Barrier(2)
    now = datetime(2026, 7, 13, 1, 0, tzinfo=UTC)

    def claim(worker_id: str):
        barrier.wait()
        return store.claim_task(
            "manifest-1",
            worker_id=worker_id,
            lease_seconds=60,
            now=now,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(claim, ("worker-a", "worker-b")))

    claimed = [claim for claim in claims if claim is not None]
    assert len(claimed) == 1
    assert claimed[0].attempt == 1

    connection = sqlite3.connect(store.path, timeout=0.1, isolation_level=None)
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("ROLLBACK")
    finally:
        connection.close()


def test_cursor_claims_large_manifest_in_one_forward_pass(tmp_path: Path) -> None:
    from rquant.backfill_state import BackfillStateStore

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    store.persist_manifest(_manifest(task_count=100))
    cursor = -1
    claimed_ids: list[str] = []

    while True:
        claim = store.claim_task(
            "manifest-1",
            worker_id="worker",
            lease_seconds=60,
            after_ordinal=cursor,
        )
        if claim is None:
            break
        assert claim.ordinal > cursor
        cursor = claim.ordinal
        claimed_ids.append(claim.task_id)
        store.mark_task_succeeded(claim, duration_seconds=0)

    assert len(claimed_ids) == 100
    assert len(set(claimed_ids)) == 100


def test_running_task_is_reclaimed_only_after_its_lease_expires(
    tmp_path: Path,
) -> None:
    from rquant.backfill_state import BackfillStateStore, StaleTaskClaimError

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    store.persist_manifest(_manifest())
    started_at = datetime(2026, 7, 13, 1, 0, tzinfo=UTC)

    first = store.claim_task(
        "manifest-1",
        worker_id="worker-a",
        lease_seconds=60,
        now=started_at,
    )
    assert first is not None
    assert (
        store.claim_task(
            "manifest-1",
            worker_id="worker-b",
            lease_seconds=60,
            now=started_at + timedelta(seconds=59),
        )
        is None
    )

    reclaimed = store.claim_task(
        "manifest-1",
        worker_id="worker-b",
        lease_seconds=60,
        now=started_at + timedelta(seconds=61),
    )
    assert reclaimed is not None
    assert reclaimed.task_id == first.task_id
    assert reclaimed.attempt == 2
    assert reclaimed.claim_token != first.claim_token

    with pytest.raises(StaleTaskClaimError, match="task-0"):
        store.mark_task_succeeded(
            first,
            duration_seconds=10,
            now=started_at + timedelta(seconds=62),
        )

    store.mark_task_succeeded(
        reclaimed,
        duration_seconds=10,
        now=started_at + timedelta(seconds=63),
    )
    assert (
        store.claim_task(
            "manifest-1",
            worker_id="worker-c",
            lease_seconds=60,
            retry_failed=True,
            now=started_at + timedelta(minutes=3),
        )
        is None
    )


def test_final_expired_attempt_gets_one_recovery_only_claim(tmp_path: Path) -> None:
    from rquant.backfill_state import BackfillStateStore

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    store.persist_manifest(_manifest(max_attempts=1))
    started_at = datetime(2026, 7, 13, 1, 0, tzinfo=UTC)
    first = store.claim_task(
        "manifest-1",
        worker_id="worker-a",
        lease_seconds=60,
        now=started_at,
    )
    assert first is not None

    recovery = store.claim_task(
        "manifest-1",
        worker_id="worker-b",
        lease_seconds=60,
        now=started_at + timedelta(seconds=61),
    )

    assert recovery is not None
    assert recovery.recovery_only is True
    assert recovery.attempt == 1
    assert recovery.claim_token != first.claim_token
    assert (
        store.claim_task(
            "manifest-1",
            worker_id="worker-c",
            lease_seconds=60,
            now=started_at + timedelta(seconds=122),
        )
        is None
    )
    status = store.get_manifest_status("manifest-1")
    task = store.get_task("manifest-1", "task-0")
    assert status.terminal is True
    assert task.failure is not None
    assert task.failure.code == "lease_expired"


def test_renew_claim_fences_worker_after_lease_was_recovered(tmp_path: Path) -> None:
    from rquant.backfill_state import BackfillStateStore, StaleTaskClaimError

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    store.persist_manifest(_manifest(max_attempts=1))
    started_at = datetime(2026, 7, 13, 1, 0, tzinfo=UTC)
    first = store.claim_task(
        "manifest-1",
        worker_id="worker-a",
        lease_seconds=60,
        now=started_at,
    )
    assert first is not None
    recovery = store.claim_task(
        "manifest-1",
        worker_id="worker-b",
        lease_seconds=60,
        now=started_at + timedelta(seconds=61),
    )
    assert recovery is not None

    with pytest.raises(StaleTaskClaimError, match="task-0"):
        store.renew_task_claim(
            first,
            lease_seconds=60,
            now=started_at + timedelta(seconds=62),
        )


def test_renew_claim_extends_current_lease(tmp_path: Path) -> None:
    from rquant.backfill_state import BackfillStateStore

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    store.persist_manifest(_manifest())
    started_at = datetime(2026, 7, 13, 1, 0, tzinfo=UTC)
    claim = store.claim_task(
        "manifest-1",
        worker_id="worker-a",
        lease_seconds=60,
        now=started_at,
    )
    assert claim is not None

    renewed = store.renew_task_claim(
        claim,
        lease_seconds=120,
        now=started_at + timedelta(seconds=30),
    )

    assert renewed.claim_token == claim.claim_token
    assert renewed.lease_expires_at == started_at + timedelta(seconds=150)


def test_retryable_structured_failure_requires_opt_in_and_obeys_attempt_limit(
    tmp_path: Path,
) -> None:
    from rquant.backfill_state import BackfillFailure, BackfillStateStore

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    store.persist_manifest(_manifest(max_attempts=2))
    now = datetime(2026, 7, 13, 1, 0, tzinfo=UTC)
    first = store.claim_task(
        "manifest-1",
        worker_id="worker-a",
        lease_seconds=60,
        now=now,
    )
    assert first is not None

    failure = BackfillFailure(
        code="source_timeout",
        message="Tushare request timed out",
        retryable=True,
        details={"provider": "tushare", "http_status": 504},
    )
    store.mark_task_failed(first, failure=failure, now=now + timedelta(seconds=5))

    task = store.get_task("manifest-1", "task-0")
    assert task.failure == failure
    assert (
        store.claim_task(
            "manifest-1",
            worker_id="worker-b",
            lease_seconds=60,
            now=now + timedelta(seconds=6),
        )
        is None
    )

    second = store.claim_task(
        "manifest-1",
        worker_id="worker-b",
        lease_seconds=60,
        retry_failed=True,
        now=now + timedelta(seconds=6),
    )
    assert second is not None
    assert second.attempt == 2
    store.mark_task_failed(
        second,
        failure=failure.model_copy(update={"message": "timed out again"}),
        now=now + timedelta(seconds=10),
    )

    assert (
        store.claim_task(
            "manifest-1",
            worker_id="worker-c",
            lease_seconds=60,
            retry_failed=True,
            now=now + timedelta(seconds=11),
        )
        is None
    )
    status = store.get_manifest_status("manifest-1")
    assert status.failed == 1
    assert status.terminal is True


def test_non_retryable_failure_cannot_be_claimed_again(tmp_path: Path) -> None:
    from rquant.backfill_state import BackfillFailure, BackfillStateStore

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    store.persist_manifest(_manifest(max_attempts=3))
    now = datetime(2026, 7, 13, 1, 0, tzinfo=UTC)
    claim = store.claim_task(
        "manifest-1",
        worker_id="worker-a",
        lease_seconds=60,
        now=now,
    )
    assert claim is not None
    store.mark_task_failed(
        claim,
        failure=BackfillFailure(
            code="invalid_scope",
            message="planned scope is invalid",
            retryable=False,
        ),
        now=now + timedelta(seconds=1),
    )

    assert (
        store.claim_task(
            "manifest-1",
            worker_id="worker-b",
            lease_seconds=60,
            retry_failed=True,
            now=now + timedelta(seconds=2),
        )
        is None
    )


def test_retry_metrics_accumulate_across_attempts(tmp_path: Path) -> None:
    from rquant.backfill_state import (
        BackfillFailure,
        BackfillStateStore,
        BackfillTaskMetrics,
    )

    store = BackfillStateStore(tmp_path / "backfill.sqlite3")
    store.persist_manifest(_manifest(max_attempts=2))
    now = datetime(2026, 7, 13, 1, 0, tzinfo=UTC)
    first = store.claim_task(
        "manifest-1",
        worker_id="worker-a",
        lease_seconds=60,
        now=now,
    )
    assert first is not None
    store.mark_task_failed(
        first,
        failure=BackfillFailure(
            code="source_timeout",
            message="first request failed",
            retryable=True,
        ),
        metrics=BackfillTaskMetrics(request_count=1, returned_rows=100),
        now=now + timedelta(seconds=1),
    )
    second = store.claim_task(
        "manifest-1",
        worker_id="worker-b",
        lease_seconds=60,
        retry_failed=True,
        now=now + timedelta(seconds=2),
    )
    assert second is not None
    store.mark_task_succeeded(
        second,
        duration_seconds=2,
        metrics=BackfillTaskMetrics(
            request_count=1,
            returned_rows=241,
            written_rows=241,
            covered_sessions=1,
        ),
        now=now + timedelta(seconds=4),
    )

    metrics = store.get_task("manifest-1", "task-0").metrics
    assert metrics.request_count == 2
    assert metrics.returned_rows == 341
    assert metrics.written_rows == 241
    assert metrics.covered_sessions == 1


def test_success_duration_updates_ewma_eta(tmp_path: Path) -> None:
    from rquant.backfill_state import BackfillStateStore, BackfillTaskMetrics

    store = BackfillStateStore(tmp_path / "backfill.sqlite3", ewma_alpha=0.5)
    store.persist_manifest(_manifest(task_count=3))
    now = datetime(2026, 7, 13, 1, 0, tzinfo=UTC)

    first = store.claim_task("manifest-1", worker_id="worker", lease_seconds=60, now=now)
    assert first is not None
    first_metrics = BackfillTaskMetrics(
        request_count=2,
        returned_rows=482,
        written_rows=482,
        covered_sessions=2,
        allowed_missing_sessions=0,
    )
    store.mark_task_succeeded(
        first,
        duration_seconds=10,
        metrics=first_metrics,
        now=now + timedelta(seconds=10),
    )
    second = store.claim_task(
        "manifest-1",
        worker_id="worker",
        lease_seconds=60,
        now=now + timedelta(seconds=11),
    )
    assert second is not None
    store.mark_task_succeeded(
        second,
        duration_seconds=20,
        metrics=BackfillTaskMetrics(
            request_count=1,
            returned_rows=241,
            written_rows=241,
            covered_sessions=1,
            allowed_missing_sessions=1,
        ),
        now=now + timedelta(seconds=31),
    )

    assert store.get_task("manifest-1", first.task_id).metrics == first_metrics
    status = store.get_manifest_status("manifest-1")
    assert status.succeeded == 2
    assert status.pending == 1
    assert status.ewma_duration_seconds == pytest.approx(15.0)
    assert status.eta_seconds == pytest.approx(15.0)
    assert status.request_count == 3
    assert status.returned_rows == 723
    assert status.written_rows == 723
    assert status.covered_sessions == 3
    assert status.allowed_missing_sessions == 1


def test_readonly_store_requires_existing_database_without_creating_it(
    tmp_path: Path,
) -> None:
    from rquant.backfill_state import BackfillStateStore

    path = tmp_path / "missing.sqlite3"

    with pytest.raises(ValueError, match="read-only backfill state"):
        BackfillStateStore(path, read_only=True)

    assert not path.exists()


def test_readonly_store_loads_state_but_rejects_writes(
    tmp_path: Path,
) -> None:
    from rquant.backfill_state import BackfillStateStore

    path = tmp_path / "backfill.sqlite3"
    writable = BackfillStateStore(path)
    manifest = _manifest()
    writable.persist_manifest(manifest)

    readonly = BackfillStateStore(path, read_only=True)

    assert readonly.load_manifest(manifest.manifest_id) is not None
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        readonly.persist_manifest(manifest)
