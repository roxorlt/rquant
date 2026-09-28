from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.manual_watchlist import (
    ManualWatchlistDelete,
    ManualWatchlistKey,
    ManualWatchlistRepository,
    ManualWatchlistScan,
    ManualWatchlistUpsert,
    WatchlistCapacityError,
    WatchlistTransactionError,
    WatchlistVersionConflictError,
)

NOW = datetime(2026, 9, 28, 2, 0, tzinfo=UTC)
OWNER = "alice"
CODE = "600000.SH"


def _store(tmp_path: Path) -> tuple[sqlite3.Connection, ManualWatchlistRepository]:
    connection = sqlite3.connect(tmp_path / "watchlist.sqlite3", isolation_level=None, timeout=0)
    store = ManualWatchlistRepository(connection)
    connection.execute("BEGIN IMMEDIATE")
    store.install_schema()
    assert connection.in_transaction
    connection.commit()
    return connection, store


def _upsert(
    store: ManualWatchlistRepository,
    *,
    code: str = CODE,
    expected_version: int | None = None,
    source: str = "detail",
    levels: tuple[Decimal, ...] = (Decimal("10.00"),),
    expires_at: datetime | None = None,
    now: datetime = NOW,
):
    return store.upsert(
        ManualWatchlistUpsert(
            owner_id=OWNER,
            ts_code=code,
            expected_version=expected_version,
            source=source,
            price_levels=levels,
            expires_at=expires_at,
        ),
        now=now,
    )


def _get(store: ManualWatchlistRepository, code: str = CODE, *, now: datetime = NOW):
    return store.get(ManualWatchlistKey(owner_id=OWNER, ts_code=code), now=now)


def test_create_update_delete_and_tombstone_recreate_require_exact_versions(tmp_path: Path) -> None:
    connection, store = _store(tmp_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        first = _upsert(store)
        assert connection.in_transaction
        assert (first.status, first.version, first.source, first.price_levels) == (
            "active",
            1,
            "detail",
            (Decimal("10.00"),),
        )
        with pytest.raises(WatchlistVersionConflictError):
            _upsert(store)
        with pytest.raises(WatchlistVersionConflictError):
            _upsert(store, expected_version=2)
        updated = _upsert(
            store,
            expected_version=1,
            source="screen_result",
            levels=(Decimal("9.50"), Decimal("10.25")),
        )
        assert (updated.status, updated.version, updated.source) == (
            "active",
            2,
            "screen_result",
        )
        with pytest.raises(WatchlistVersionConflictError):
            store.delete(
                ManualWatchlistDelete(owner_id=OWNER, ts_code=CODE, expected_version=1),
                now=NOW,
            )
        deleted = store.delete(
            ManualWatchlistDelete(owner_id=OWNER, ts_code=CODE, expected_version=2),
            now=NOW,
        )
        assert (deleted.status, deleted.version) == ("deleted", 3)
        assert deleted.source is None
        assert deleted.price_levels == ()
        assert deleted.expires_at is None
        assert deleted.updated_at is None
        with pytest.raises(WatchlistVersionConflictError):
            _upsert(store, expected_version=None)
        with pytest.raises(WatchlistVersionConflictError):
            _upsert(store, expected_version=2)
        revived = _upsert(store, expected_version=3, source="pool_member")
        assert (revived.status, revived.version, revived.source) == ("active", 4, "pool_member")
        connection.commit()
        assert _get(store) == revived
    finally:
        connection.close()


def test_failed_write_and_rollback_do_not_leave_partial_or_committed_rows(tmp_path: Path) -> None:
    connection, store = _store(tmp_path)
    reader = sqlite3.connect(tmp_path / "watchlist.sqlite3", isolation_level=None, timeout=0)
    try:
        connection.execute("BEGIN IMMEDIATE")
        _upsert(store)
        assert connection.in_transaction
        assert reader.execute("SELECT COUNT(*) FROM manual_watchlist").fetchone() == (0,)
        connection.rollback()
        assert _get(store) is None
        connection.execute("BEGIN IMMEDIATE")
        _upsert(store)
        with pytest.raises(WatchlistVersionConflictError):
            store.delete(
                ManualWatchlistDelete(owner_id=OWNER, ts_code=CODE, expected_version=2),
                now=NOW,
            )
        assert (_get(store).version, connection.in_transaction) == (1, True)
        connection.commit()
        assert reader.execute("SELECT version FROM manual_watchlist").fetchone() == (1,)
    finally:
        reader.close()
        connection.close()


def test_all_writes_require_a_caller_owned_transaction(tmp_path: Path) -> None:
    connection, store = _store(tmp_path)
    try:
        with pytest.raises(WatchlistTransactionError):
            _upsert(store)
        with pytest.raises(WatchlistTransactionError):
            store.delete(
                ManualWatchlistDelete(owner_id=OWNER, ts_code=CODE, expected_version=1),
                now=NOW,
            )
        with pytest.raises(WatchlistTransactionError):
            store.install_schema()
        assert connection.in_transaction is False
    finally:
        connection.close()


def test_two_connections_serialize_writes_then_stale_version_conflicts(tmp_path: Path) -> None:
    first, first_store = _store(tmp_path)
    second = sqlite3.connect(tmp_path / "watchlist.sqlite3", isolation_level=None, timeout=0)
    second_store = ManualWatchlistRepository(second)
    try:
        first.execute("BEGIN IMMEDIATE")
        _upsert(first_store)
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            second.execute("BEGIN IMMEDIATE")
        first.commit()
        second.execute("BEGIN IMMEDIATE")
        with pytest.raises(WatchlistVersionConflictError):
            _upsert(second_store, expected_version=None)
        assert second.in_transaction
        second.rollback()
        assert _get(first_store).version == 1
    finally:
        second.close()
        first.close()


def test_expiry_boundary_preserves_version_and_requires_cas_to_renew(tmp_path: Path) -> None:
    connection, store = _store(tmp_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        first = _upsert(store, expires_at=NOW + timedelta(seconds=1))
        assert _get(store, now=NOW + timedelta(milliseconds=999)).status == "active"
        assert _get(store, now=NOW + timedelta(seconds=1)).status == "expired"
        with pytest.raises(WatchlistVersionConflictError):
            store.delete(
                ManualWatchlistDelete(owner_id=OWNER, ts_code=CODE, expected_version=first.version),
                now=NOW + timedelta(seconds=1),
            )
        with pytest.raises(WatchlistVersionConflictError):
            _upsert(store, expected_version=None, now=NOW + timedelta(seconds=1))
        renewed = _upsert(store, expected_version=1, now=NOW + timedelta(seconds=1))
        assert (renewed.status, renewed.version, renewed.expires_at) == ("active", 2, None)
        connection.commit()
    finally:
        connection.close()


def test_scan_is_bounded_sorted_and_excludes_tombstones_but_reports_expired(tmp_path: Path) -> None:
    connection, store = _store(tmp_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        _upsert(store, code="600003.SH")
        _upsert(store, code="600001.SH", expires_at=NOW)
        _upsert(store, code="600002.SH")
        _upsert(store, code="600000.SH")
        store.delete(
            ManualWatchlistDelete(owner_id=OWNER, ts_code="600002.SH", expected_version=1),
            now=NOW,
        )
        connection.commit()
        first = store.scan(ManualWatchlistScan(owner_id=OWNER, now=NOW, limit=2))
        assert [(item.ts_code, item.status) for item in first.entries] == [
            ("600000.SH", "active"),
            ("600001.SH", "expired"),
        ]
        assert first.next_after_ts_code == "600001.SH"
        second = store.scan(
            ManualWatchlistScan(
                owner_id=OWNER, now=NOW, limit=2, after_ts_code=first.next_after_ts_code
            )
        )
        assert [(item.ts_code, item.status) for item in second.entries] == [("600003.SH", "active")]
        assert second.next_after_ts_code is None
        assert _get(store, "600002.SH").status == "deleted"
    finally:
        connection.close()


def test_500_effective_member_limit_rejects_501st_without_partial_write(tmp_path: Path) -> None:
    connection, store = _store(tmp_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        for number in range(500):
            _upsert(store, code=f"{number:06d}.SH")
        with pytest.raises(WatchlistCapacityError):
            _upsert(store, code="600001.SH")
        assert _get(store, "600001.SH") is None
        assert _upsert(store, code="000000.SH", expected_version=1).version == 2
        assert connection.execute(
            "SELECT COUNT(*) FROM manual_watchlist WHERE deleted = 0"
        ).fetchone() == (500,)
        connection.commit()
    finally:
        connection.close()


def test_expired_member_does_not_consume_capacity_and_renewal_does(tmp_path: Path) -> None:
    connection, store = _store(tmp_path)
    later = NOW + timedelta(seconds=1)
    try:
        connection.execute("BEGIN IMMEDIATE")
        _upsert(store, code="600001.SH", expires_at=later)
        for number in range(499):
            _upsert(store, code=f"{number:06d}.SH")
        assert _upsert(store, code="600002.SH", now=later).status == "active"
        with pytest.raises(WatchlistCapacityError):
            _upsert(store, code="600001.SH", expected_version=1, now=later)
        assert _get(store, "600001.SH", now=later).status == "expired"
        connection.commit()
    finally:
        connection.close()


@pytest.mark.parametrize(
    "change",
    [
        {"owner_id": "   "},
        {"owner_id": " alice "},
        {"ts_code": "bad"},
        {"source": "external"},
        {"price_levels": (Decimal("0"),)},
        {"price_levels": (Decimal("NaN"),)},
        {"price_levels": (Decimal("Infinity"),)},
        {"price_levels": (Decimal("10"), Decimal("10"))},
        {"price_levels": (Decimal("11"), Decimal("10"))},
        {"price_levels": tuple(Decimal(number) for number in range(1, 10))},
        {"expires_at": datetime(2026, 9, 28, 3, 0)},
        {"expected_version": 0},
    ],
)
def test_invalid_write_contract_is_rejected(change: dict[str, object]) -> None:
    values: dict[str, object] = {
        "owner_id": OWNER,
        "ts_code": CODE,
        "expected_version": None,
        "source": "detail",
        "price_levels": (),
        "expires_at": None,
    }
    with pytest.raises(ValidationError):
        ManualWatchlistUpsert.model_validate({**values, **change})


def test_expiry_normalizes_an_aware_offset_and_naive_now_is_rejected(tmp_path: Path) -> None:
    connection, store = _store(tmp_path)
    try:
        expiry = (NOW + timedelta(hours=1)).astimezone(timezone(timedelta(hours=8)))
        connection.execute("BEGIN IMMEDIATE")
        first = _upsert(store, expires_at=expiry)
        assert first.expires_at == NOW + timedelta(hours=1)
        with pytest.raises(ValueError, match="timezone-aware"):
            _upsert(store, code="600001.SH", now=datetime(2026, 9, 28, 10, 0))
        connection.commit()
        with pytest.raises(ValueError, match="timezone-aware"):
            _get(store, now=datetime(2026, 9, 28, 10, 0))
        with pytest.raises(ValidationError):
            ManualWatchlistScan(owner_id=OWNER, now=datetime(2026, 9, 28, 10, 0))
    finally:
        connection.close()


def test_scan_rejects_noncanonical_owner_identity() -> None:
    with pytest.raises(ValidationError):
        ManualWatchlistScan(owner_id=" alice ", now=NOW)
