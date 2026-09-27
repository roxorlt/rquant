from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.signal_bus import SignalBusStore
from rquant.signal_route_spool import (
    ReadonlySignalRouteSpool,
    SignalRouteSpool,
    publish_signal_bus_prefix,
)
from tests.unit.test_signal_bus import _signal
from tests.unit.test_signal_route_spool import _route_two


def test_new_empty_bus_records_a_stable_start_but_not_signal_coverage(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    before = datetime.now(UTC)
    store = SignalBusStore(path)
    after = datetime.now(UTC)

    receipt = store.observed_prefix_receipt(observed_at=after)

    assert receipt is not None
    assert before <= receipt.source_created_at <= after
    assert receipt.source_high_watermark == receipt.prefix_row_count == 0
    assert receipt.source_generation_id == store.source_descriptor().generation_id
    assert SignalBusStore(path).observed_prefix_receipt(observed_at=after) == receipt
    assert receipt.upstream_complete is False


def test_old_bus_never_gets_a_retroactive_lifecycle_start(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = SignalBusStore(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "DELETE FROM signal_bus_metadata WHERE metadata_key = 'source_created_at'"
        )

    reopened = SignalBusStore(path)
    assert reopened.observed_prefix_receipt(observed_at=datetime.now(UTC)) is None
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "SELECT metadata_value FROM signal_bus_metadata "
                "WHERE metadata_key = 'source_created_at'"
            ).fetchone()
            is None
        )
    assert reopened.source_descriptor().generation_id == store.source_descriptor().generation_id


def test_metadata_loss_cannot_turn_an_existing_file_into_a_new_source(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    SignalBusStore(path)
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM signal_bus_metadata")

    reopened = SignalBusStore(path)
    assert reopened.observed_prefix_receipt(observed_at=datetime.now(UTC)) is None


def test_receipt_binds_exact_continuous_bus_rows_in_one_read(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = SignalBusStore(path)
    inspected_at = datetime.now(UTC) + timedelta(seconds=1)
    store.ingest(_signal("a"), received_at=inspected_at)
    store.ingest(_signal("e"), received_at=inspected_at)

    receipt = store.observed_prefix_receipt(observed_at=inspected_at)
    assert receipt is not None
    assert receipt.prefix_row_count == receipt.source_high_watermark == 2
    assert len(receipt.prefix_rows_sha256) == 64
    assert receipt.source_inspected_at == inspected_at

    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM signal_envelope WHERE global_sequence = 1")
    assert store.observed_prefix_receipt(observed_at=inspected_at) is None


def test_corrupt_bus_payload_withholds_receipt(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = SignalBusStore(path)
    observed_at = datetime.now(UTC)
    store.ingest(_signal("a"), received_at=observed_at)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE signal_envelope SET payload_json = '{}' WHERE global_sequence = 1"
        )
    assert store.observed_prefix_receipt(observed_at=observed_at) is None


def test_naive_lifecycle_clock_withholds_receipt(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = SignalBusStore(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE signal_bus_metadata SET metadata_value = '2026-09-27T12:00:00' "
            "WHERE metadata_key = 'source_created_at'"
        )
    assert store.observed_prefix_receipt(observed_at=datetime.now(UTC)) is None


def test_uncommitted_ingest_never_enters_source_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = SignalBusStore(path)

    def crash(_connection: sqlite3.Connection) -> None:
        raise RuntimeError("crash before commit")

    monkeypatch.setattr(store, "_before_commit", crash)
    with pytest.raises(RuntimeError, match="crash before commit"):
        store.ingest(_signal("a"), received_at=datetime.now(UTC))

    receipt = SignalBusStore(path).observed_prefix_receipt(observed_at=datetime.now(UTC))
    assert receipt is not None
    assert receipt.prefix_row_count == receipt.source_high_watermark == 0


def test_receipt_only_matches_a_caught_up_spool_of_the_same_generation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    bus = SignalBusStore(path)
    _route_two(bus, tmp_path)
    receipt = bus.observed_prefix_receipt(observed_at=datetime.now(UTC))
    assert receipt is not None

    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=1)
    reader = ReadonlySignalRouteSpool(root)
    lagging = reader.source_descriptor()
    lagging_records = reader.routed_after_global_sequence(
        after_sequence=0, through_sequence=lagging.high_watermark, limit=10
    )
    assert not receipt.matches_routed_prefix(lagging, lagging_records)

    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    caught_up = reader.source_descriptor()
    complete_records = reader.routed_after_global_sequence(
        after_sequence=0, through_sequence=caught_up.high_watermark, limit=10
    )
    assert receipt.matches_routed_prefix(caught_up, complete_records)
    changed_time = complete_records[0].model_copy(
        update={"received_at": complete_records[0].received_at + timedelta(seconds=1)}
    )
    assert not receipt.matches_routed_prefix(caught_up, (changed_time, complete_records[1]))

    path.unlink()
    rebuilt = SignalBusStore(path).observed_prefix_receipt(observed_at=datetime.now(UTC))
    assert rebuilt is not None
    assert not rebuilt.matches_routed_prefix(caught_up, complete_records)


def test_receipt_rejects_future_or_precreation_cutoffs(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    created = bus.observed_prefix_receipt(observed_at=datetime.now(UTC))
    assert created is not None
    assert (
        bus.observed_prefix_receipt(
            observed_at=created.source_created_at - timedelta(microseconds=1)
        )
        is None
    )

    received_at = datetime.now(UTC) + timedelta(minutes=2)
    bus.ingest(_signal("a"), received_at=received_at)
    assert bus.observed_prefix_receipt(observed_at=received_at - timedelta(microseconds=1)) is None
    assert bus.observed_prefix_receipt(observed_at=received_at) is not None


def test_receipt_fails_closed_on_bounded_prefix_limit(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    inspected_at = datetime.now(UTC)
    bus.ingest(_signal("a"), received_at=inspected_at)
    bus.ingest(_signal("e"), received_at=inspected_at)

    assert bus.observed_prefix_receipt(observed_at=inspected_at, max_records=1) is None
