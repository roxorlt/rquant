from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.notification_state import NotificationStateStore
from rquant.signal_bus import SignalBusStore, SignalRouteReceipt
from rquant.signal_route_spool import (
    ReadonlySignalRouteSpool,
    SignalRouteSpool,
    publish_signal_bus_prefix,
)
from tests.unit.test_notification_state import _published_source, _signal


def _linked_source(
    tmp_path: Path, *, signal_count: int = 1
) -> tuple[ReadonlySignalRouteSpool, datetime]:
    if signal_count:
        source = _published_source(
            tmp_path,
            signals=tuple(_signal(str(index + 2)) for index in range(signal_count)),
        )
    else:
        bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
        root = tmp_path / "signal-spool"
        publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
        source = ReadonlySignalRouteSpool(root)
    inspected_at = datetime.now(UTC)
    link = SignalRouteSpool(source.paths.root).publish_bus_prefix_link(
        bus=SignalBusStore(tmp_path / "signal-bus.sqlite3"), observed_at=inspected_at
    )
    assert link is not None
    return source, inspected_at


def _replicated(
    tmp_path: Path,
    source: ReadonlySignalRouteSpool,
    inspected_at: datetime,
    *,
    through_sequence: int | None = None,
    source_inspected_at: datetime | None = None,
) -> NotificationStateStore:
    descriptor = source.source_descriptor()
    high = descriptor.high_watermark
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=high if through_sequence is None else through_sequence,
        limit=10,
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    store.replicate(
        descriptor,
        records,
        observed_at=inspected_at,
        source_inspected_at=source_inspected_at,
    )
    return store


def test_verified_prefix_survives_restart_and_is_read_only(tmp_path: Path) -> None:
    source, inspected_at = _linked_source(tmp_path, signal_count=2)
    store = _replicated(tmp_path, source, inspected_at, source_inspected_at=inspected_at)
    link_path = source.paths.root / "bus-prefix-link.json"
    link_before = link_path.read_bytes()
    watcher = sqlite3.connect(f"file:{store.path}?mode=ro", uri=True, isolation_level=None)
    try:
        version_before = watcher.execute("PRAGMA data_version").fetchone()[0]
        first = store.verified_bus_spool_prefix(source, observed_at=inspected_at)
        restarted = NotificationStateStore(store.path)
        second = restarted.verified_bus_spool_prefix(
            ReadonlySignalRouteSpool(source.paths.root), observed_at=inspected_at
        )
        version_after = watcher.execute("PRAGMA data_version").fetchone()[0]
    finally:
        watcher.close()

    assert first is not None
    assert second == first
    assert first.link == source.bus_prefix_link()
    assert first.notification_source_inspected_at == inspected_at
    assert first.notification_routed_rows_sha256 == first.link.routed_rows_sha256
    assert first.notification_state_revision >= 1
    assert first.upstream_complete is False
    assert version_after == version_before
    assert link_path.read_bytes() == link_before


def test_empty_prefix_requires_explicit_notification_observation(tmp_path: Path) -> None:
    source, inspected_at = _linked_source(tmp_path, signal_count=0)
    store = _replicated(tmp_path, source, inspected_at)
    assert store.verified_bus_spool_prefix(source, observed_at=inspected_at) is None

    store.replicate(
        source.source_descriptor(),
        (),
        observed_at=inspected_at + timedelta(seconds=1),
        source_inspected_at=inspected_at + timedelta(seconds=1),
    )
    result = store.verified_bus_spool_prefix(
        source, observed_at=inspected_at + timedelta(seconds=1)
    )
    assert result is not None
    assert result.link.bus_prefix.source_high_watermark == 0
    assert result.upstream_complete is False


def test_missing_link_and_lagging_replication_are_unavailable(tmp_path: Path) -> None:
    source = _published_source(tmp_path)
    inspected_at = datetime.now(UTC)
    store = _replicated(tmp_path, source, inspected_at, source_inspected_at=inspected_at)
    assert store.verified_bus_spool_prefix(source, observed_at=inspected_at) is None

    linked = SignalRouteSpool(source.paths.root).publish_bus_prefix_link(
        bus=SignalBusStore(tmp_path / "signal-bus.sqlite3"), observed_at=inspected_at
    )
    assert linked is not None
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE notification_replication_source "
            "SET last_global_sequence = 0, last_signal_id = NULL"
        )
    assert store.verified_bus_spool_prefix(source, observed_at=inspected_at) is None


def test_damaged_spool_records_directory_is_unavailable(tmp_path: Path) -> None:
    source, inspected_at = _linked_source(tmp_path)
    store = _replicated(tmp_path, source, inspected_at, source_inspected_at=inspected_at)
    assert store.verified_bus_spool_prefix(source, observed_at=inspected_at) is not None

    source.paths.records.rename(source.paths.root / "records-unavailable")

    assert store.verified_bus_spool_prefix(source, observed_at=inspected_at) is None


@pytest.mark.parametrize(
    "mutation",
    (
        "generation",
        "source_high",
        "last_signal_id",
        "observation_early",
        "observation_future",
        "missing_receipt",
        "missing_sequence",
        "signal_payload",
        "received_at",
        "receipt_hash",
        "receipt_content",
    ),
)
def test_changed_notification_prefix_is_unavailable(tmp_path: Path, mutation: str) -> None:
    source, inspected_at = _linked_source(tmp_path, signal_count=2)
    store = _replicated(tmp_path, source, inspected_at, source_inspected_at=inspected_at)
    assert store.verified_bus_spool_prefix(source, observed_at=inspected_at) is not None

    with sqlite3.connect(store.path) as connection:
        if mutation in {"missing_receipt", "receipt_hash", "receipt_content"}:
            operation = "delete" if mutation == "missing_receipt" else "update"
            connection.execute(
                "DROP TRIGGER notification_source_route_receipt_immutable_" + operation
            )
        if mutation == "generation":
            connection.execute(
                "UPDATE notification_replication_source SET source_generation_id = ?",
                ("0" * 64,),
            )
        elif mutation == "source_high":
            connection.execute(
                "UPDATE notification_source_observation SET source_high_watermark = 1"
            )
        elif mutation == "last_signal_id":
            connection.execute(
                "UPDATE notification_replication_source SET last_signal_id = ?",
                ("0" * 64,),
            )
        elif mutation == "observation_early":
            connection.execute(
                "UPDATE notification_source_observation SET inspected_at = ?",
                ((inspected_at - timedelta(seconds=1)).isoformat(),),
            )
        elif mutation == "observation_future":
            connection.execute(
                "UPDATE notification_source_observation SET inspected_at = ?",
                ((inspected_at + timedelta(seconds=1)).isoformat(),),
            )
        elif mutation == "missing_receipt":
            connection.execute(
                "DELETE FROM notification_source_route_receipt WHERE global_sequence = 1"
            )
        elif mutation == "missing_sequence":
            connection.execute("DELETE FROM signal_envelope WHERE global_sequence = 1")
        elif mutation == "signal_payload":
            connection.execute(
                "UPDATE signal_envelope SET payload_json = '{}' WHERE global_sequence = 1"
            )
        elif mutation == "received_at":
            connection.execute(
                "UPDATE signal_envelope SET received_at = ? WHERE global_sequence = 1",
                ((inspected_at - timedelta(seconds=10)).isoformat(),),
            )
        elif mutation == "receipt_hash":
            connection.execute(
                "UPDATE notification_source_route_receipt SET receipt_hash = ? "
                "WHERE global_sequence = 1",
                ("0" * 64,),
            )
        elif mutation == "receipt_content":
            row = connection.execute(
                "SELECT receipt_json FROM notification_source_route_receipt "
                "WHERE global_sequence = 1"
            ).fetchone()
            receipt = SignalRouteReceipt.model_validate_json(row[0])
            changed = receipt.model_copy(
                update={"routed_at": receipt.routed_at - timedelta(seconds=1)}
            )
            payload = json.dumps(
                changed.model_dump(mode="json"),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            connection.execute(
                "UPDATE notification_source_route_receipt "
                "SET receipt_json = ?, receipt_hash = ? WHERE global_sequence = 1",
                (payload, hashlib.sha256(payload.encode()).hexdigest()),
            )

    assert store.verified_bus_spool_prefix(source, observed_at=inspected_at) is None


def test_prefix_over_ten_thousand_rows_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, inspected_at = _linked_source(tmp_path)
    store = _replicated(tmp_path, source, inspected_at, source_inspected_at=inspected_at)
    link = source.bus_prefix_link()
    assert link is not None
    oversized = link.model_copy(
        update={
            "bus_prefix": link.bus_prefix.model_copy(
                update={"source_high_watermark": 10_001, "prefix_row_count": 10_001}
            )
        }
    )
    monkeypatch.setattr(source, "bus_prefix_link", lambda: oversized)

    assert store.verified_bus_spool_prefix(source, observed_at=inspected_at) is None
