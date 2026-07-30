from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.source_quota_store import (
    SourceQuotaConflictError,
    SourceQuotaExhaustedError,
    SourceQuotaStore,
)

START = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
END = START + timedelta(minutes=1)


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
