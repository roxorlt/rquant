from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import rquant.signal_route_spool as spool_module
from rquant.signal_bus import SignalBusStore
from rquant.signal_route_spool import (
    ReadonlySignalRouteSpool,
    SignalRouteSpool,
    publish_signal_bus_prefix,
)
from tests.unit.test_signal_route_spool import _route_signals, _route_two, _signal


def _routed_source(tmp_path: Path) -> tuple[SignalBusStore, SignalRouteSpool]:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    spool = SignalRouteSpool(tmp_path / "signal-spool")
    return bus, spool


def test_caught_up_bus_spool_link_survives_restart_with_one_cutoff(tmp_path: Path) -> None:
    bus, spool = _routed_source(tmp_path)
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)
    cutoff = datetime.now(UTC)

    linked = spool.publish_bus_prefix_link(bus=bus, observed_at=cutoff)

    assert linked is not None
    assert linked.bus_prefix.source_inspected_at == cutoff
    assert linked.bus_prefix.source_high_watermark == 2
    assert linked.bus_prefix.upstream_complete is False
    assert ReadonlySignalRouteSpool(spool.paths.root).bus_prefix_link() == linked
    assert ReadonlySignalRouteSpool(spool.paths.root).bus_prefix_link() == linked


def test_lagging_spool_cannot_publish_bus_link(tmp_path: Path) -> None:
    bus, spool = _routed_source(tmp_path)
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=1)

    assert spool.publish_bus_prefix_link(bus=bus, observed_at=datetime.now(UTC)) is None
    assert ReadonlySignalRouteSpool(spool.paths.root).bus_prefix_link() is None


def test_empty_link_is_only_an_observed_empty_prefix(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    spool = SignalRouteSpool(tmp_path / "signal-spool")
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)

    linked = spool.publish_bus_prefix_link(bus=bus, observed_at=datetime.now(UTC))

    assert linked is not None
    assert linked.bus_prefix.source_high_watermark == 0
    assert linked.bus_prefix.upstream_complete is False
    assert ReadonlySignalRouteSpool(spool.paths.root).bus_prefix_link() == linked


def test_wrong_bus_generation_never_replaces_old_spool_link(tmp_path: Path) -> None:
    bus, spool = _routed_source(tmp_path)
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)
    old = spool.publish_bus_prefix_link(bus=bus, observed_at=datetime.now(UTC))
    assert old is not None

    bus.path.unlink()
    rebuilt = SignalBusStore(bus.path)
    assert spool.publish_bus_prefix_link(bus=rebuilt, observed_at=datetime.now(UTC)) is None
    assert ReadonlySignalRouteSpool(spool.paths.root).bus_prefix_link() == old


def test_link_rejects_route_created_after_bus_cutoff(tmp_path: Path) -> None:
    bus, spool = _routed_source(tmp_path)
    cutoff = datetime.now(UTC)
    with sqlite3.connect(bus.path) as connection:
        connection.execute(
            "UPDATE signal_route_receipt SET routed_at = ? WHERE source_sequence = 1",
            ((cutoff + timedelta(minutes=1)).isoformat(timespec="microseconds"),),
        )
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)

    assert spool.publish_bus_prefix_link(bus=bus, observed_at=cutoff) is None
    assert ReadonlySignalRouteSpool(spool.paths.root).bus_prefix_link() is None


def test_changed_digest_withholds_read_link(tmp_path: Path) -> None:
    bus, spool = _routed_source(tmp_path)
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)
    linked = spool.publish_bus_prefix_link(bus=bus, observed_at=datetime.now(UTC))
    assert linked is not None

    path = spool.paths.root / "bus-prefix-link.json"
    payload = json.loads(path.read_text())
    payload["bus_prefix"]["prefix_rows_sha256"] = "0" * 64
    path.write_text(json.dumps(payload))
    assert ReadonlySignalRouteSpool(spool.paths.root).bus_prefix_link() is None


def test_same_reader_rechecks_record_file_after_it_was_damaged(tmp_path: Path) -> None:
    bus, spool = _routed_source(tmp_path)
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)
    linked = spool.publish_bus_prefix_link(bus=bus, observed_at=datetime.now(UTC))
    assert linked is not None
    reader = ReadonlySignalRouteSpool(spool.paths.root)
    assert reader.bus_prefix_link() == linked

    (spool.paths.records / "00000000000000000001.json").write_text("{}")

    assert reader.bus_prefix_link() is None


@pytest.mark.parametrize("column", ("target_manifest_hash", "decision_fingerprint"))
def test_changed_bus_route_ledger_withholds_new_link(tmp_path: Path, column: str) -> None:
    bus, spool = _routed_source(tmp_path)
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)
    with sqlite3.connect(bus.path) as connection:
        connection.execute(
            f"UPDATE signal_route_receipt SET {column} = ? WHERE source_sequence = 1",
            ("0" * 64,),
        )

    assert spool.publish_bus_prefix_link(bus=bus, observed_at=datetime.now(UTC)) is None
    assert ReadonlySignalRouteSpool(spool.paths.root).bus_prefix_link() is None


def test_advanced_spool_pointer_withholds_stale_link(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    first = _signal("1")
    second = _signal("2")
    _route_signals(bus, tmp_path, (first,))
    spool = SignalRouteSpool(tmp_path / "signal-spool")
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)
    linked = spool.publish_bus_prefix_link(bus=bus, observed_at=datetime.now(UTC))
    assert linked is not None
    reader = ReadonlySignalRouteSpool(spool.paths.root)
    assert reader.bus_prefix_link() == linked

    _route_signals(bus, tmp_path, (first, second))
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)
    assert reader.bus_prefix_link() is None
    assert ReadonlySignalRouteSpool(spool.paths.root).bus_prefix_link() is None


def test_crash_before_link_replace_never_exposes_unwritten_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bus, spool = _routed_source(tmp_path)
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)
    real_replace = spool_module._atomic_replace_at

    def fail_link(directory_descriptor: int, name: str, payload: bytes) -> None:
        if name == "bus-prefix-link.json":
            raise RuntimeError("crash before receipt replace")
        real_replace(directory_descriptor, name, payload)

    monkeypatch.setattr(spool_module, "_atomic_replace_at", fail_link)
    with pytest.raises(RuntimeError, match="crash before receipt replace"):
        spool.publish_bus_prefix_link(bus=bus, observed_at=datetime.now(UTC))
    assert ReadonlySignalRouteSpool(spool.paths.root).bus_prefix_link() is None
