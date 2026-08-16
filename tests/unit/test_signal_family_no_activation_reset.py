"""Executable Phase-A no-activation probes and adjacent route regressions."""

from __future__ import annotations

import gc
import hashlib
import inspect
import sqlite3
from pathlib import Path

import pytest

import rquant.signal_bus as signal_bus
import rquant.signal_route_spool as spool
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.signal_bus import LegacySignalWriteActivationError
from rquant.signal_contracts import CurrentSignalEnvelope, SignalEnvelope, parse_signal_envelope
from rquant.signal_family_differential_gate import BoundaryProbeResultV1, load_policy
from tests.r07_differential_probe_runner import run_boundary_probe_subprocess
from tests.unit.test_signal_contracts import (
    _CURRENT_CANONICAL_FIXTURES,
    _LEGACY_CANONICAL_FIXTURES,
)
from tests.unit.test_signal_dual_read_r06 import _insert_literal_signal_rows, _store

ROOT = Path(__file__).parents[2]
POLICY_PATH = ROOT / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json"
CURRENT_LITERAL = _CURRENT_CANONICAL_FIXTURES[0][2]
LEGACY_LITERAL = _LEGACY_CANONICAL_FIXTURES[0][3]


def _current_signal() -> CurrentSignalEnvelope:
    signal = parse_signal_envelope(CURRENT_LITERAL)
    assert type(signal) is CurrentSignalEnvelope
    return signal


@pytest.mark.parametrize(
    "inventory_id",
    [f"R07-B{index:02d}" for index in range(1, 18)],
    ids=[f"R07-B{index:02d}" for index in range(1, 18)],
)
def test_r07_dynamic_boundary_probe(
    inventory_id: str,
    tmp_path: Path,
) -> None:
    policy = load_policy(POLICY_PATH)
    payload = run_boundary_probe_subprocess(
        policy_path=POLICY_PATH,
        inventory_id=inventory_id,
        tmp_path=tmp_path,
    )
    result = BoundaryProbeResultV1.model_validate(payload)
    expected = next(item for item in policy.boundary_probes if item.inventory_id == inventory_id)
    assert (result.probe_id, result.setup_id) == (expected.probe_id, expected.setup_id)

    assert result.passed
    assert result.reached_count == 1
    assert all(count == 0 for count in result.mutation_guard_counts.values())
    assert result.before_snapshot_digest == result.after_snapshot_digest
    if inventory_id == "R07-B03":
        assert result.exception_phase == "consumption"
        assert result.sentinel_after_invocation == 0
        assert result.sentinel_after_consumption == 1
        assert result.yielded_count == 0


def test_signal_bus_private_ingest_preflights_before_connection_mutation(tmp_path: Path) -> None:
    path = tmp_path / "private-ingest.sqlite3"
    store = _store(path)
    connection = store._connect()
    before_changes = connection.total_changes
    try:
        with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
            store._ingest_in_transaction(
                connection,
                _current_signal(),  # type: ignore[arg-type]
                received_at=None,
            )
        assert connection.total_changes == before_changes
        assert not connection.in_transaction
    finally:
        connection.close()


def test_signal_bus_route_empty_targets_rejects_stored_current_before_database_mutation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "route-empty.sqlite3"
    store = _store(path)
    current = _current_signal()
    assert current.signal_id is not None
    _insert_literal_signal_rows(
        path,
        (("current", CurrentSignalEnvelope, current.signal_id, CURRENT_LITERAL),),
        routed=False,
    )

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.route(current.signal_id, (), now=current.available_at)

    assert store.outbox_records() == ()


def test_route_preflight_never_clones_database_or_wal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "route-no-clone.sqlite3"
    store = _store(path)
    current = _current_signal()
    legacy = parse_signal_envelope(LEGACY_LITERAL)
    assert current.signal_id is not None
    assert type(legacy) is SignalEnvelope
    _insert_literal_signal_rows(
        path,
        (
            ("legacy", SignalEnvelope, legacy.signal_id, LEGACY_LITERAL),
            ("current", CurrentSignalEnvelope, current.signal_id, CURRENT_LITERAL),
        ),
        routed=False,
    )

    def reject_clone(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("route preflight attempted a database clone")

    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_bytes", reject_clone)
        patch.setattr(signal_bus, "TemporaryDirectory", reject_clone, raising=False)
        with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
            store.route(current.signal_id, (), now=current.available_at)
        assert store.route(legacy.signal_id, (), now=legacy.available_at) == ()

    assert store.outbox_records() == ()


def test_route_readonly_preflight_observes_uncheckpointed_wal_rows_for_both_families(
    tmp_path: Path,
) -> None:
    path = tmp_path / "route-wal.sqlite3"
    store = _store(path)
    current = _current_signal()
    legacy = parse_signal_envelope(LEGACY_LITERAL)
    assert current.signal_id is not None
    assert type(legacy) is SignalEnvelope

    writer = sqlite3.connect(path, isolation_level=None)
    try:
        writer.execute("PRAGMA wal_autocheckpoint = 0")
        writer.execute("BEGIN IMMEDIATE")
        for sequence, signal_id, literal in (
            (1, legacy.signal_id, LEGACY_LITERAL),
            (2, current.signal_id, CURRENT_LITERAL),
        ):
            writer.execute(
                """
                INSERT INTO signal_envelope(
                    global_sequence, signal_id, payload_hash, payload_json, received_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    sequence,
                    signal_id,
                    hashlib.sha256(literal).hexdigest(),
                    literal.decode("utf-8"),
                    current.available_at.isoformat().replace("+00:00", "Z"),
                ),
            )
        writer.execute("COMMIT")
        assert path.with_name(f"{path.name}-wal").stat().st_size > 0

        with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
            store.route(current.signal_id, (), now=current.available_at)
        assert store.route(legacy.signal_id, (), now=legacy.available_at) == ()
    finally:
        writer.close()

    assert store.outbox_records() == ()


def test_route_empty_target_preflight_sees_wal_committed_during_readonly_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "route-wal-open-race.sqlite3"
    store = _store(path)
    current = _current_signal()
    legacy = parse_signal_envelope(LEGACY_LITERAL)
    assert current.signal_id is not None
    assert type(legacy) is SignalEnvelope
    _insert_literal_signal_rows(
        path,
        (("legacy", SignalEnvelope, legacy.signal_id, LEGACY_LITERAL),),
        routed=False,
    )
    gc.collect()
    assert not path.with_name(f"{path.name}-wal").exists()

    original_connect = store._connect_readonly
    open_options: list[dict[str, object]] = []
    writers: list[sqlite3.Connection] = []

    def commit_current_before_open(**options: object) -> sqlite3.Connection:
        open_options.append(options)
        if not writers:
            writer = sqlite3.connect(path, isolation_level=None)
            writer.execute("PRAGMA wal_autocheckpoint = 0")
            writer.execute("BEGIN IMMEDIATE")
            writer.execute(
                """
                INSERT INTO signal_envelope(
                    global_sequence, signal_id, payload_hash, payload_json, received_at
                ) VALUES (2, ?, ?, ?, ?)
                """,
                (
                    current.signal_id,
                    hashlib.sha256(CURRENT_LITERAL).hexdigest(),
                    CURRENT_LITERAL.decode("utf-8"),
                    current.available_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            writer.execute("COMMIT")
            writers.append(writer)
        return original_connect(**options)  # type: ignore[arg-type]

    monkeypatch.setattr(store, "_connect_readonly", commit_current_before_open)
    try:
        with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
            store.route(current.signal_id, (), now=current.available_at)
        assert store.route(legacy.signal_id, (), now=legacy.available_at) == ()
        assert open_options == [{}, {}]
    finally:
        for writer in writers:
            writer.close()

    assert store.outbox_records() == ()


def test_route_transaction_rechecks_current_row_committed_after_readonly_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "route-recheck.sqlite3"
    store = _store(path)
    current = _current_signal()
    assert current.signal_id is not None
    original_preflight = store._preflight_stored_legacy_signal

    def insert_after_preflight(signal_id: str, *, operation: str) -> None:
        original_preflight(signal_id, operation=operation)
        _insert_literal_signal_rows(
            path,
            (("current", CurrentSignalEnvelope, current.signal_id, CURRENT_LITERAL),),
            routed=False,
        )

    monkeypatch.setattr(store, "_preflight_stored_legacy_signal", insert_after_preflight)
    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.route(
            current.signal_id,
            (DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
            now=current.available_at,
        )

    assert store.signal(current.signal_id) == current
    assert store.outbox_records() == ()


def test_decoder_and_read_models_remain_constructible_without_writer_authority() -> None:
    with pytest.raises(spool.SignalRouteSpoolIntegrityError):
        spool.decode_current_signal_route_spool_record(b'{"schema_version":3}')
    assert inspect.isclass(spool.CurrentSignalBusRoutedRecord)
    assert inspect.isclass(spool.CurrentSignalRouteSpoolRecord)
