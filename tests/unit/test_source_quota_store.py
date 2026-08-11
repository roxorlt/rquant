from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from multiprocessing import get_context
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest

from rquant.source_quota_store import (
    SourceQuotaAttemptOutcome,
    SourceQuotaConflictError,
    SourceQuotaExhaustedError,
    SourceQuotaStore,
)

START = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
END = START + timedelta(minutes=1)


def _open_store_in_process(path: str, start: Any, results: Any) -> None:
    start.wait()
    try:
        SourceQuotaStore(Path(path), busy_timeout_ms=5_000)
    except Exception as exc:
        results.put(f"{type(exc).__name__}:{exc}")
    else:
        results.put("ok")


def _store(path: Path) -> SourceQuotaStore:
    store = SourceQuotaStore(path)
    store.declare_window(
        source="tushare.rt_min",
        window_id="20260731T0930",
        starts_at=START,
        resets_at=END,
        total_units=500,
    )
    return store


def test_acquire_is_idempotent_and_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    store = _store(path)

    first = store.acquire(
        source="tushare.rt_min",
        owner="market-minute:poll-1",
        units=1,
        now=START,
        expires_at=START + timedelta(seconds=10),
    )
    same = SourceQuotaStore(path).acquire(
        source="tushare.rt_min",
        owner="market-minute:poll-1",
        units=1,
        now=START + timedelta(seconds=1),
        expires_at=START + timedelta(seconds=10),
    )

    assert same == first
    assert store.remaining("tushare.rt_min", now=START) == 499


def test_parallel_reservations_cannot_overallocate_window(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    store = SourceQuotaStore(path)
    store.declare_window(
        source="source",
        window_id="window",
        starts_at=START,
        resets_at=END,
        total_units=2,
    )
    SourceQuotaStore(path).acquire(
        source="source",
        owner="worker-1",
        units=2,
        now=START,
        expires_at=START + timedelta(seconds=30),
    )

    with pytest.raises(SourceQuotaExhaustedError, match="remaining=0"):
        store.acquire(
            source="source",
            owner="worker-2",
            units=1,
            now=START,
            expires_at=START + timedelta(seconds=30),
        )


def test_consumed_units_stay_spent_but_released_unused_units_return(tmp_path: Path) -> None:
    store = _store(tmp_path / "quota.sqlite3")
    lease = store.acquire(
        source="tushare.rt_min",
        owner="market-minute:poll-1",
        units=10,
        now=START,
        expires_at=START + timedelta(seconds=30),
    )

    store.consume(
        lease.lease_id,
        usage_id="request-1",
        units=3,
        now=START + timedelta(seconds=1),
    )
    store.consume(
        lease.lease_id,
        usage_id="request-1",
        units=3,
        now=START + timedelta(seconds=2),
    )
    released = store.release(lease.lease_id, now=START + timedelta(seconds=2))

    assert released.released_at == START + timedelta(seconds=2)
    assert store.remaining("tushare.rt_min", now=START + timedelta(seconds=3)) == 497


def test_expired_unused_reservation_returns_automatically(tmp_path: Path) -> None:
    store = _store(tmp_path / "quota.sqlite3")
    store.acquire(
        source="tushare.rt_min",
        owner="market-minute:poll-1",
        units=10,
        now=START,
        expires_at=START + timedelta(seconds=5),
    )

    assert store.remaining("tushare.rt_min", now=START + timedelta(seconds=6)) == 500


def test_window_and_owner_retries_reject_conflicting_contracts(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    store = _store(path)
    with pytest.raises(SourceQuotaConflictError, match="window"):
        store.declare_window(
            source="tushare.rt_min",
            window_id="20260731T0930",
            starts_at=START,
            resets_at=END,
            total_units=100,
        )
    store.acquire(
        source="tushare.rt_min",
        owner="market-minute:poll-1",
        units=1,
        now=START,
        expires_at=START + timedelta(seconds=10),
    )
    with pytest.raises(SourceQuotaConflictError, match="owner"):
        store.acquire(
            source="tushare.rt_min",
            owner="market-minute:poll-1",
            units=2,
            now=START + timedelta(seconds=1),
            expires_at=START + timedelta(seconds=10),
        )


def test_consume_is_bounded_and_window_must_be_active(tmp_path: Path) -> None:
    store = _store(tmp_path / "quota.sqlite3")
    lease = store.acquire(
        source="tushare.rt_min",
        owner="market-minute:poll-1",
        units=1,
        now=START,
        expires_at=START + timedelta(seconds=10),
    )
    with pytest.raises(SourceQuotaConflictError, match="reserved"):
        store.consume(
            lease.lease_id,
            usage_id="request-too-large",
            units=2,
            now=START + timedelta(seconds=1),
        )
    store.consume(
        lease.lease_id,
        usage_id="request-1",
        units=1,
        now=START + timedelta(seconds=1),
    )
    with pytest.raises(SourceQuotaConflictError, match="usage_id"):
        store.consume(
            lease.lease_id,
            usage_id="request-1",
            units=2,
            now=START + timedelta(seconds=2),
        )
    with pytest.raises(SourceQuotaExhaustedError, match="active window"):
        store.remaining("tushare.rt_min", now=END)


def test_attempt_recovers_inflight_as_unknown_without_releasing_its_quota(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    store = _store(path)

    attempt = store.begin_attempt(
        source="tushare.rt_min",
        owner="daily-close:request-1",
        attempt_id="request-1",
        units=10,
        now=START,
        expires_at=START + timedelta(seconds=5),
    )
    store.mark_dispatched(attempt.attempt_id, now=START + timedelta(seconds=1))

    recovered = SourceQuotaStore(path).recover_attempt(
        attempt.attempt_id,
        now=START + timedelta(seconds=6),
    )

    assert recovered.outcome is SourceQuotaAttemptOutcome.UNKNOWN
    assert recovered.committed_at == START + timedelta(seconds=6)
    assert store.remaining("tushare.rt_min", now=START + timedelta(seconds=6)) == 490


def test_attempt_completion_is_idempotent_and_parallel_attempts_cannot_overspend(
    tmp_path: Path,
) -> None:
    store = SourceQuotaStore(tmp_path / "quota.sqlite3")
    store.declare_window(
        source="source",
        window_id="window",
        starts_at=START,
        resets_at=END,
        total_units=2,
    )
    first = store.begin_attempt(
        source="source",
        owner="worker-1",
        attempt_id="attempt-1",
        units=2,
        now=START,
        expires_at=START + timedelta(seconds=30),
    )
    with pytest.raises(SourceQuotaExhaustedError, match="remaining=0"):
        SourceQuotaStore(tmp_path / "quota.sqlite3").begin_attempt(
            source="source",
            owner="worker-2",
            attempt_id="attempt-2",
            units=1,
            now=START,
            expires_at=START + timedelta(seconds=30),
        )

    store.mark_dispatched(first.attempt_id, now=START + timedelta(seconds=1))
    committed = store.commit_attempt(
        first.attempt_id,
        outcome=SourceQuotaAttemptOutcome.SUCCESS,
        now=START + timedelta(seconds=2),
    )
    repeated = store.commit_attempt(
        first.attempt_id,
        outcome=SourceQuotaAttemptOutcome.SUCCESS,
        now=START + timedelta(seconds=3),
    )

    assert repeated == committed
    assert store.remaining("source", now=START + timedelta(seconds=3)) == 0


def test_release_rejects_a_lease_linked_to_a_pending_attempt(tmp_path: Path) -> None:
    store = _store(tmp_path / "quota.sqlite3")
    attempt = store.begin_attempt(
        source="tushare.rt_min",
        owner="market-minute:pending",
        attempt_id="pending",
        units=1,
        now=START,
        expires_at=START + timedelta(seconds=10),
    )

    with pytest.raises(SourceQuotaConflictError, match="pending attempt"):
        store.release(attempt.lease_id, now=START + timedelta(seconds=1))
    with pytest.raises(SourceQuotaConflictError, match="attempt lease"):
        store.consume(
            attempt.lease_id,
            usage_id="legacy-bypass",
            units=1,
            now=START + timedelta(seconds=1),
        )

    store.mark_dispatched(attempt.attempt_id, now=START + timedelta(seconds=1))
    with pytest.raises(SourceQuotaConflictError, match="pending attempt"):
        store.release(attempt.lease_id, now=START + timedelta(seconds=2))


def test_attempt_completion_clamps_wall_clock_rollback_and_preserves_audit(
    tmp_path: Path,
) -> None:
    monotonic_values = iter((100, 200, 300))
    store = SourceQuotaStore(
        tmp_path / "quota.sqlite3",
        boot_id="boot-a",
        monotonic_ns=lambda: next(monotonic_values),
    )
    store.declare_window(
        source="source",
        window_id="window",
        starts_at=START,
        resets_at=END,
        total_units=1,
    )
    attempt = store.begin_attempt(
        source="source",
        owner="owner",
        attempt_id="rollback",
        units=1,
        now=START + timedelta(seconds=10),
        expires_at=START + timedelta(seconds=30),
    )
    dispatched = store.mark_dispatched(attempt.attempt_id, now=START + timedelta(seconds=9))
    committed = store.commit_attempt(
        attempt.attempt_id,
        outcome=SourceQuotaAttemptOutcome.SUCCESS,
        now=START + timedelta(seconds=8),
    )

    assert dispatched.dispatched_at == START + timedelta(seconds=10)
    assert committed.committed_at == dispatched.dispatched_at
    assert committed.lifecycle_sequence == 3
    assert committed.clock_rollback_count == 2


def test_stale_recovery_uses_boot_identity_and_converges_across_windows(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    old = SourceQuotaStore(path, boot_id="boot-old", monotonic_ns=lambda: 100)
    old.declare_window(
        source="source",
        window_id="day-one",
        starts_at=START,
        resets_at=START + timedelta(days=1),
        total_units=1,
    )
    first = old.begin_attempt(
        source="source",
        owner="old-owner",
        attempt_id="old-attempt",
        units=1,
        now=START,
        expires_at=START + timedelta(minutes=1),
    )
    old.mark_dispatched(first.attempt_id, now=START)

    restarted = SourceQuotaStore(path, boot_id="boot-new", monotonic_ns=lambda: 5)
    recovered = restarted.recover_stale_attempts(
        source="source",
        now=START + timedelta(days=2),
        min_age=timedelta(minutes=5),
    )

    assert [item.attempt_id for item in recovered] == ["old-attempt"]
    assert recovered[0].outcome is SourceQuotaAttemptOutcome.UNKNOWN


def test_stale_recovery_does_not_recover_an_active_same_boot_attempt(tmp_path: Path) -> None:
    monotonic_values = iter((1_000, 1_500))
    store = SourceQuotaStore(
        tmp_path / "quota.sqlite3",
        boot_id="boot-a",
        monotonic_ns=lambda: next(monotonic_values),
    )
    store.declare_window(
        source="source",
        window_id="window",
        starts_at=START,
        resets_at=END,
        total_units=1,
    )
    store.begin_attempt(
        source="source",
        owner="owner",
        attempt_id="active",
        units=1,
        now=START,
        expires_at=START + timedelta(seconds=30),
    )

    assert (
        store.recover_stale_attempts(
            source="source",
            now=START + timedelta(hours=1),
            min_age=timedelta(seconds=1),
        )
        == ()
    )


def test_concurrent_capacity_one_attempts_cannot_double_spend(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    store = SourceQuotaStore(path)
    store.declare_window(
        source="source",
        window_id="window",
        starts_at=START,
        resets_at=END,
        total_units=1,
    )
    barrier = Barrier(2)

    def reserve(index: int) -> str:
        barrier.wait()
        try:
            SourceQuotaStore(path).begin_attempt(
                source="source",
                owner=f"owner-{index}",
                attempt_id=f"attempt-{index}",
                units=1,
                now=START,
                expires_at=START + timedelta(seconds=30),
            )
        except SourceQuotaExhaustedError:
            return "exhausted"
        return "reserved"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(reserve, (1, 2)))

    assert sorted(outcomes) == ["exhausted", "reserved"]
    assert store.remaining("source", now=START) == 0


def test_v2_pending_attempt_is_additively_migrated_and_recoverable(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            f"""
            CREATE TABLE quota_window (
                source TEXT NOT NULL, window_id TEXT NOT NULL, starts_at TEXT NOT NULL,
                resets_at TEXT NOT NULL, total_units INTEGER NOT NULL,
                PRIMARY KEY(source, window_id)
            );
            CREATE TABLE quota_lease (
                lease_id TEXT PRIMARY KEY, source TEXT NOT NULL, window_id TEXT NOT NULL,
                owner TEXT NOT NULL, units INTEGER NOT NULL, used_units INTEGER NOT NULL,
                granted_at TEXT NOT NULL, expires_at TEXT NOT NULL,
                quota_reset_at TEXT NOT NULL, released_at TEXT,
                UNIQUE(source, window_id, owner)
            );
            CREATE TABLE quota_usage (
                usage_id TEXT PRIMARY KEY, lease_id TEXT NOT NULL,
                units INTEGER NOT NULL, consumed_at TEXT NOT NULL
            );
            CREATE TABLE quota_attempt (
                attempt_id TEXT PRIMARY KEY, source TEXT NOT NULL, owner TEXT NOT NULL,
                lease_id TEXT NOT NULL UNIQUE, units INTEGER NOT NULL,
                prepared_at TEXT NOT NULL, dispatched_at TEXT,
                outcome TEXT NOT NULL, committed_at TEXT
            );
            INSERT INTO quota_window VALUES (
                'source', 'legacy', '{START.isoformat()}',
                '{(START + timedelta(days=1)).isoformat()}', 1
            );
            INSERT INTO quota_lease VALUES (
                '{"a" * 64}', 'source', 'legacy', 'legacy-owner', 1, 0,
                '{START.isoformat()}', '{(START + timedelta(minutes=1)).isoformat()}',
                '{(START + timedelta(days=1)).isoformat()}', NULL
            );
            INSERT INTO quota_attempt VALUES (
                'legacy-attempt', 'source', 'legacy-owner', '{"a" * 64}', 1,
                '{START.isoformat()}', '{START.isoformat()}', 'pending', NULL
            );
            PRAGMA user_version = 2;
            """
        )

    migrated = SourceQuotaStore(path, boot_id="boot-new", monotonic_ns=lambda: 0)
    recovered = migrated.recover_stale_attempts(
        source="source",
        now=START + timedelta(days=2),
        min_age=timedelta(hours=1),
    )

    assert recovered[0].attempt_id == "legacy-attempt"
    assert recovered[0].outcome is SourceQuotaAttemptOutcome.UNKNOWN
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3


def test_v2_migration_is_serialized_across_processes(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE quota_window (
                source TEXT NOT NULL, window_id TEXT NOT NULL, starts_at TEXT NOT NULL,
                resets_at TEXT NOT NULL, total_units INTEGER NOT NULL,
                PRIMARY KEY(source, window_id)
            );
            CREATE TABLE quota_lease (
                lease_id TEXT PRIMARY KEY, source TEXT NOT NULL, window_id TEXT NOT NULL,
                owner TEXT NOT NULL, units INTEGER NOT NULL, used_units INTEGER NOT NULL,
                granted_at TEXT NOT NULL, expires_at TEXT NOT NULL,
                quota_reset_at TEXT NOT NULL, released_at TEXT,
                UNIQUE(source, window_id, owner)
            );
            CREATE TABLE quota_usage (
                usage_id TEXT PRIMARY KEY, lease_id TEXT NOT NULL,
                units INTEGER NOT NULL, consumed_at TEXT NOT NULL
            );
            CREATE TABLE quota_attempt (
                attempt_id TEXT PRIMARY KEY, source TEXT NOT NULL, owner TEXT NOT NULL,
                lease_id TEXT NOT NULL UNIQUE, units INTEGER NOT NULL,
                prepared_at TEXT NOT NULL, dispatched_at TEXT,
                outcome TEXT NOT NULL, committed_at TEXT
            );
            PRAGMA user_version = 2;
            """
        )

    context = get_context("spawn")
    start = context.Event()
    results = context.Queue()
    processes = [
        context.Process(target=_open_store_in_process, args=(str(path), start, results))
        for _ in range(6)
    ]
    for process in processes:
        process.start()
    start.set()
    for process in processes:
        process.join(timeout=10)

    assert all(not process.is_alive() for process in processes)
    assert [results.get(timeout=2) for _ in processes] == ["ok"] * len(processes)
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        columns = {row[1] for row in connection.execute("PRAGMA table_info(quota_attempt)")}
    assert {
        "boot_id",
        "last_monotonic_ns",
        "lifecycle_sequence",
        "clock_rollback_count",
    } <= columns
