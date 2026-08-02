from __future__ import annotations

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import rquant.signal_route_spool as signal_route_spool_module
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_route_spool import (
    ReadonlySignalRouteSpool,
    SignalRouteSpool,
    SignalRouteSpoolIntegrityError,
    publish_signal_bus_prefix,
)
from rquant.signal_router_runtime import (
    RouteSourceDescriptor,
    RoutingDecision,
    RunnerSignalBatch,
    SignalRouteCursorStore,
    SourceSnapshot,
    route_runner_signals,
)
from rquant.strategy_runner import RunnerSignalRecord

NOW = datetime(2026, 7, 31, 2, 30, tzinfo=UTC)
POLICY = "a" * 64
GENERATION = "b" * 64
SPEC = "c" * 64


def _signal(seed: str) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="d" * 64,
        feature_snapshot_id="e" * 64,
        event_time=NOW - timedelta(seconds=1),
        available_at=NOW,
        candidate_id=f"60000{ord(seed) % 10}.SH",
        action=SignalAction.WATCH,
        reason_codes=("spool-test",),
        evidence={},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="f" * 40,
    )


class _Source:
    def __init__(self, records: tuple[RunnerSignalRecord, ...]) -> None:
        self.records = records

    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(
                descriptor=RouteSourceDescriptor(
                    source_id="n-shape-v1",
                    generation_id=GENERATION,
                    strategy_spec_fingerprint=SPEC,
                    first_sequence=1,
                    high_watermark=len(self.records),
                )
            ),
            after_sequence=after_sequence,
            limit=limit,
            records=tuple(record for record in self.records if record.sequence > after_sequence)[
                :limit
            ],
        )


def _route_signals(
    bus: SignalBusStore,
    tmp_path: Path,
    signals: tuple[SignalEnvelope, ...],
) -> None:
    source = _Source(
        tuple(
            RunnerSignalRecord(sequence=sequence, signal=signal)
            for sequence, signal in enumerate(signals, start=1)
        )
    )
    route_runner_signals(
        source_id="n-shape-v1",
        source=source,
        bus=bus,
        cursors=SignalRouteCursorStore(
            tmp_path / "cursor.sqlite3",
            routing_policy_fingerprint=POLICY,
        ),
        routed_at=NOW,
        target_resolver=lambda _signal: RoutingDecision.route(
            routing_policy_fingerprint=POLICY,
            targets=(
                DeliveryTarget(
                    recipient_id="admin",
                    channel=DeliveryChannel.PUSHDEER,
                ),
            ),
        ),
        limit=10,
    )


def _route_two(bus: SignalBusStore, tmp_path: Path) -> tuple[SignalEnvelope, SignalEnvelope]:
    first = _signal("1")
    second = _signal("2")
    _route_signals(bus, tmp_path, (first, second))
    return first, second


def _count_record_reads(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    real_read = signal_route_spool_module._read_file_at
    record_reads: list[str] = []
    record_reads_lock = threading.Lock()

    def counted_read(
        directory_descriptor: int,
        name: str,
        *,
        label: str,
        max_bytes: int,
    ) -> bytes:
        if label.startswith("routed-signal record"):
            with record_reads_lock:
                record_reads.append(name)
        return real_read(
            directory_descriptor,
            name,
            label=label,
            max_bytes=max_bytes,
        )

    monkeypatch.setattr(signal_route_spool_module, "_read_file_at", counted_read)
    return record_reads


def test_router_publishes_bounded_immutable_prefix_for_readonly_consumers(
    tmp_path: Path,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    first, second = _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"

    summary = publish_signal_bus_prefix(
        bus=bus,
        spool=SignalRouteSpool(root),
        limit=1,
    )
    reader = ReadonlySignalRouteSpool(root)
    descriptor = reader.source_descriptor()
    records = reader.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )

    assert summary.published_count == 1
    assert summary.source_high_watermark == 2
    assert summary.published_high_watermark == 1
    assert descriptor.generation_id == bus.source_descriptor().generation_id
    assert descriptor.high_watermark == 1
    assert [record.signal.signal_id for record in records] == [first.signal_id]
    assert records[0].receipt.targets == (
        DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),
    )

    caught_up = publish_signal_bus_prefix(
        bus=bus,
        spool=SignalRouteSpool(root),
        limit=10,
    )
    assert caught_up.published_count == 1
    assert caught_up.published_high_watermark == 2
    assert [
        record.signal.signal_id
        for record in reader.routed_after_global_sequence(
            after_sequence=0,
            through_sequence=2,
            limit=10,
        )
    ] == [first.signal_id, second.signal_id]


def test_signal_route_spool_replay_is_idempotent_and_generation_is_immutable(
    tmp_path: Path,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    spool = SignalRouteSpool(tmp_path / "signal-spool")

    first = publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)
    replay = publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)

    assert first.published_count == 2
    assert replay.published_count == 0
    assert replay.published_high_watermark == 2

    rebuilt = SignalBusStore(tmp_path / "other-bus.sqlite3")
    with pytest.raises(SignalRouteSpoolIntegrityError, match="generation"):
        publish_signal_bus_prefix(bus=rebuilt, spool=spool, limit=10)


def test_readonly_signal_route_spool_rejects_a_missing_sequence(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    (root / "records" / "00000000000000000001.json").unlink()

    reader = ReadonlySignalRouteSpool(root)
    with pytest.raises(SignalRouteSpoolIntegrityError, match="missing|gap"):
        reader.routed_after_global_sequence(
            after_sequence=0,
            through_sequence=2,
            limit=10,
        )


def test_signal_route_spool_hides_records_routed_after_observation_time(
    tmp_path: Path,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)

    records = ReadonlySignalRouteSpool(root).signals_after_global_sequence(
        after_sequence=0,
        through_sequence=2,
        observed_at=NOW - timedelta(microseconds=1),
        limit=10,
    )

    assert records == ()


@pytest.mark.parametrize(
    "mutation",
    (
        "signal",
        "targets",
        "disposition",
        "reason",
        "fingerprint",
    ),
)
def test_readonly_signal_route_spool_rejects_any_record_payload_tampering(
    tmp_path: Path,
    mutation: str,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    path = root / "records" / "00000000000000000001.json"
    payload = json.loads(path.read_text())
    record = payload["record"]

    if mutation == "signal":
        record["signal"]["reason_codes"] = ["tampered"]
    elif mutation == "targets":
        record["receipt"]["targets"][0]["recipient_id"] = "attacker"
    elif mutation == "disposition":
        record["receipt"].update(
            disposition="no_target",
            reason_code="tampered",
            targets=[],
            target_count=0,
        )
    elif mutation == "reason":
        record["receipt"]["reason_code"] = "tampered"
    else:
        record["receipt"]["decision_fingerprint"] = "0" * 64
    path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))

    reader = ReadonlySignalRouteSpool(root)
    with pytest.raises(SignalRouteSpoolIntegrityError, match="hash|invalid"):
        reader.routed_after_global_sequence(
            after_sequence=0,
            through_sequence=2,
            limit=10,
        )


def test_source_descriptor_rejects_a_broken_previous_record_hash(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    path = root / "records" / "00000000000000000002.json"
    payload = json.loads(path.read_text())
    payload["previous_record_hash"] = "0" * 64
    path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))

    with pytest.raises(SignalRouteSpoolIntegrityError, match="chain|hash"):
        ReadonlySignalRouteSpool(root).source_descriptor()


def test_source_descriptor_rejects_a_pointer_head_hash_mismatch(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    path = root / "current.json"
    payload = json.loads(path.read_text())
    payload["last_record_hash"] = "0" * 64
    path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))

    with pytest.raises(SignalRouteSpoolIntegrityError, match="head hash"):
        ReadonlySignalRouteSpool(root).source_descriptor()


def test_source_descriptor_rejects_a_deleted_pointer_with_published_records(
    tmp_path: Path,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    (root / "current.json").unlink()

    with pytest.raises(SignalRouteSpoolIntegrityError, match="pointer.*missing"):
        ReadonlySignalRouteSpool(root).source_descriptor()


def test_routed_reader_hides_a_future_route_after_clock_rollback(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)

    records = ReadonlySignalRouteSpool(root).routed_after_global_sequence(
        after_sequence=0,
        through_sequence=2,
        observed_at=NOW - timedelta(microseconds=1),
        limit=10,
    )

    assert records == ()


def test_routed_reader_defaults_to_current_time_for_pit_visibility(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    monkeypatch.setattr(
        "rquant.signal_route_spool._utc_now",
        lambda: NOW - timedelta(microseconds=1),
    )

    records = ReadonlySignalRouteSpool(root).routed_after_global_sequence(
        after_sequence=0,
        through_sequence=2,
        limit=10,
    )

    assert records == ()


def test_source_descriptor_rejects_current_pointer_replaced_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    replacement = root / "replacement.json"
    replacement.write_bytes((root / "current.json").read_bytes())
    real_read = os.read
    reads = 0

    def replace_current_on_second_read(descriptor: int, size: int) -> bytes:
        nonlocal reads
        reads += 1
        if reads == 2:
            os.replace(replacement, root / "current.json")
        return real_read(descriptor, size)

    monkeypatch.setattr("rquant.signal_route_spool.os.read", replace_current_on_second_read)

    with pytest.raises(SignalRouteSpoolIntegrityError, match="changed during read"):
        ReadonlySignalRouteSpool(root).source_descriptor()


def test_readonly_signal_route_spool_rejects_symlinked_metadata_and_records(
    tmp_path: Path,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    outside = tmp_path / "outside.json"
    outside.write_bytes((root / "current.json").read_bytes())
    (root / "current.json").unlink()
    (root / "current.json").symlink_to(outside)

    with pytest.raises(SignalRouteSpoolIntegrityError, match="unsafe|metadata"):
        ReadonlySignalRouteSpool(root).source_descriptor()

    (root / "current.json").unlink()
    (root / "current.json").write_bytes(outside.read_bytes())
    record = root / "records" / "00000000000000000001.json"
    outside.write_bytes(record.read_bytes())
    record.unlink()
    record.symlink_to(outside)

    with pytest.raises(SignalRouteSpoolIntegrityError, match="unsafe|record"):
        ReadonlySignalRouteSpool(root).source_descriptor()


def test_readonly_spool_verifies_full_chain_once_and_reuses_unchanged_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    first, second = _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    record_reads = _count_record_reads(monkeypatch)
    reader = ReadonlySignalRouteSpool(root)

    descriptor = reader.source_descriptor()
    assert record_reads == [
        "00000000000000000001.json",
        "00000000000000000002.json",
    ]

    record_reads.clear()
    records = reader.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )

    assert [record.signal.signal_id for record in records] == [
        first.signal_id,
        second.signal_id,
    ]
    assert record_reads == []


def test_new_readonly_spool_instance_reverifies_the_full_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    record_reads = _count_record_reads(monkeypatch)

    ReadonlySignalRouteSpool(root).source_descriptor()
    record_reads.clear()
    ReadonlySignalRouteSpool(root).source_descriptor()

    assert record_reads == [
        "00000000000000000001.json",
        "00000000000000000002.json",
    ]


def test_concurrent_initial_reads_verify_the_chain_only_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    record_reads = _count_record_reads(monkeypatch)
    reader = ReadonlySignalRouteSpool(root)
    barrier = threading.Barrier(8)

    def read_descriptor() -> int:
        barrier.wait()
        return reader.source_descriptor().high_watermark

    with ThreadPoolExecutor(max_workers=8) as executor:
        watermarks = tuple(executor.map(lambda _index: read_descriptor(), range(8)))

    assert watermarks == (2,) * 8
    assert record_reads == [
        "00000000000000000001.json",
        "00000000000000000002.json",
    ]


def test_readonly_spool_verifies_only_new_records_after_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    first, second = _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    record_reads = _count_record_reads(monkeypatch)
    reader = ReadonlySignalRouteSpool(root)
    reader.source_descriptor()
    record_reads.clear()

    third = _signal("3")
    _route_signals(bus, tmp_path, (first, second, third))
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    record_reads.clear()
    records = reader.routed_after_global_sequence(
        after_sequence=2,
        through_sequence=3,
        limit=10,
    )

    assert [record.signal.signal_id for record in records] == [third.signal_id]
    assert record_reads == ["00000000000000000003.json"]


def test_cached_readonly_spool_rejects_a_pointer_rollback(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    reader = ReadonlySignalRouteSpool(root)
    reader.source_descriptor()

    first_record = json.loads((root / "records" / "00000000000000000001.json").read_text())
    pointer_path = root / "current.json"
    pointer = json.loads(pointer_path.read_text())
    pointer["source"]["high_watermark"] = 1
    pointer["last_record_hash"] = first_record["record_hash"]
    pointer_path.write_text(json.dumps(pointer, separators=(",", ":"), sort_keys=True))

    with pytest.raises(SignalRouteSpoolIntegrityError, match="regressed|rollback"):
        reader.source_descriptor()


def test_cached_readonly_spool_rejects_a_changed_head_at_same_watermark(
    tmp_path: Path,
) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    reader = ReadonlySignalRouteSpool(root)
    reader.source_descriptor()

    pointer_path = root / "current.json"
    pointer = json.loads(pointer_path.read_text())
    pointer["last_record_hash"] = "0" * 64
    pointer_path.write_text(json.dumps(pointer, separators=(",", ":"), sort_keys=True))

    with pytest.raises(SignalRouteSpoolIntegrityError, match="head|watermark"):
        reader.source_descriptor()


def test_cached_readonly_spool_rejects_a_changed_source_generation(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    reader = ReadonlySignalRouteSpool(root)
    reader.source_descriptor()

    source_path = root / "source.json"
    source = json.loads(source_path.read_text())
    source["generation_id"] = "0" * 64
    source_path.write_text(json.dumps(source, separators=(",", ":"), sort_keys=True))

    with pytest.raises(SignalRouteSpoolIntegrityError, match="generation"):
        reader.source_descriptor()


def test_cached_readonly_spool_rejects_a_tampered_appended_record(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    first, second = _route_two(bus, tmp_path)
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    reader = ReadonlySignalRouteSpool(root)
    reader.source_descriptor()

    third = _signal("3")
    _route_signals(bus, tmp_path, (first, second, third))
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    path = root / "records" / "00000000000000000003.json"
    payload = json.loads(path.read_text())
    payload["record"]["signal"]["reason_codes"] = ["tampered"]
    path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))

    with pytest.raises(SignalRouteSpoolIntegrityError, match="hash|invalid"):
        reader.source_descriptor()
