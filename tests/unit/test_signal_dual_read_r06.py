from __future__ import annotations

import hashlib
import inspect
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import rquant.signal_bus as signal_bus
from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    OutboxStatus,
)
from rquant.notification_state import NotificationStateStore
from rquant.notification_worker import (
    NotificationDelivery,
    NotificationProvider,
    run_notification_batch,
)
from rquant.runtime_notification_providers import format_signal_notification
from rquant.runtime_routing_policy import (
    FrozenRoutingPolicyResolver,
    RoutingPolicyDocument,
)
from rquant.signal_bus import (
    RouteDecisionKind,
    RouteSourceDescriptor,
    SignalBusRoutedRecord,
    SignalBusSignalRecord,
    SignalBusSourceDescriptor,
    SignalBusStore,
    SignalRouteReceipt,
    routing_decision_fingerprint,
)
from rquant.signal_contracts import (
    CurrentSignalEnvelope,
    SignalEnvelope,
    SignalEnvelopeFamily,
    parse_signal_envelope,
)
from rquant.signal_router_runtime import (
    ReadonlyStrategyRunnerSignalSource,
    RoutingDecision,
    RunnerSignalBatch,
    SignalRouteCursorStore,
    SourceSnapshot,
    _query_signal_records,
    route_runner_signals,
)
from tests.unit.test_signal_contracts import (
    _CURRENT_CANONICAL_FIXTURES,
    _LEGACY_CANONICAL_FIXTURES,
)
from tests.unit.test_signal_router_runtime import _write_runner_source

LegacySignalWriteActivationError = getattr(
    signal_bus,
    "LegacySignalWriteActivationError",
    type("MissingLegacySignalWriteActivationError", (TypeError,), {}),
)

NOW = datetime(2026, 7, 31, 1, 40, tzinfo=UTC)
POLICY = "e" * 64
GENERATION = "f" * 64
SPEC = "1" * 64

_LITERAL_FAMILY_FIXTURES = (
    (
        "legacy-v1",
        SignalEnvelope,
        _LEGACY_CANONICAL_FIXTURES[0][2],
        _LEGACY_CANONICAL_FIXTURES[0][3],
    ),
    (
        "legacy-v2",
        SignalEnvelope,
        _LEGACY_CANONICAL_FIXTURES[2][2],
        _LEGACY_CANONICAL_FIXTURES[2][3],
    ),
    (
        "legacy-v3",
        SignalEnvelope,
        _LEGACY_CANONICAL_FIXTURES[4][2],
        _LEGACY_CANONICAL_FIXTURES[4][3],
    ),
    (
        "current-git",
        CurrentSignalEnvelope,
        _CURRENT_CANONICAL_FIXTURES[0][1],
        _CURRENT_CANONICAL_FIXTURES[0][2],
    ),
    (
        "current-manifest",
        CurrentSignalEnvelope,
        _CURRENT_CANONICAL_FIXTURES[1][1],
        _CURRENT_CANONICAL_FIXTURES[1][2],
    ),
)
_CURRENT_FIXTURES = _LITERAL_FAMILY_FIXTURES[-2:]


def _store(path: Path) -> SignalBusStore:
    return SignalBusStore(
        path,
        retry_base_delay=timedelta(seconds=5),
        retry_max_delay=timedelta(seconds=30),
        max_attempts=3,
    )


def _encoded_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _insert_literal_signal_rows(
    path: Path,
    fixtures: tuple[tuple[str, type[SignalEnvelopeFamily], str, bytes], ...],
    *,
    routed: bool,
) -> None:
    target_manifest = "[]"
    target_manifest_hash = hashlib.sha256(target_manifest.encode()).hexdigest()
    with sqlite3.connect(path) as connection:
        if routed:
            connection.execute(
                """
                INSERT INTO signal_route_source(
                    source_id, generation_id, strategy_spec_fingerprint,
                    routing_policy_fingerprint, first_sequence,
                    observed_high_watermark, last_sequence, last_signal_id,
                    registered_at, updated_at
                ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
                """,
                (
                    "r06-literal-source",
                    GENERATION,
                    SPEC,
                    POLICY,
                    len(fixtures),
                    len(fixtures),
                    fixtures[-1][2],
                    _encoded_time(NOW),
                    _encoded_time(NOW),
                ),
            )
        for sequence, (_name, _expected_type, signal_id, literal) in enumerate(
            fixtures,
            start=1,
        ):
            payload = literal.decode("utf-8")
            connection.execute(
                """
                INSERT INTO signal_envelope(
                    global_sequence, signal_id, payload_hash, payload_json, received_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    sequence,
                    signal_id,
                    hashlib.sha256(literal).hexdigest(),
                    payload,
                    _encoded_time(NOW),
                ),
            )
            if routed:
                connection.execute(
                    """
                    INSERT INTO signal_route_receipt(
                        source_id, source_sequence, signal_id, decision_fingerprint,
                        disposition, reason_code, target_manifest_hash,
                        target_manifest_json, routed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "r06-literal-source",
                        sequence,
                        signal_id,
                        f"{sequence:064x}",
                        "no_target",
                        "r06_fixture_no_target",
                        target_manifest_hash,
                        target_manifest,
                        _encoded_time(NOW),
                    ),
                )
        connection.execute(
            """
            UPDATE signal_bus_metadata SET metadata_value = ?
            WHERE metadata_key = 'signal_high_watermark'
            """,
            (str(len(fixtures)),),
        )


def _route_receipt(signal_id: str, *, sequence: int = 1) -> SignalRouteReceipt:
    return SignalRouteReceipt(
        source_id="r06-literal-source",
        source_sequence=sequence,
        signal_id=signal_id,
        decision_fingerprint=f"{sequence:064x}",
        disposition="no_target",
        reason_code="r06_fixture_no_target",
        target_manifest_hash=hashlib.sha256(b"[]").hexdigest(),
        targets=(),
        target_count=0,
        routed_at=NOW,
    )


def _routed_record(literal: bytes) -> SignalBusRoutedRecord:
    signal = parse_signal_envelope(literal)
    assert signal.signal_id is not None
    return SignalBusRoutedRecord(
        global_sequence=1,
        signal_id=signal.signal_id,
        payload_hash=hashlib.sha256(literal).hexdigest(),
        payload_json=literal.decode(),
        signal=signal,
        received_at=NOW,
        receipt=_route_receipt(signal.signal_id),
    )


def _database_snapshot(path: Path) -> tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]:
    with sqlite3.connect(path) as connection:
        tables = tuple(
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
        )
        return tuple(
            (table, tuple(connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')))
            for table in tables
        )


def _insert_pending_outbox(
    path: Path,
    *,
    signal: SignalEnvelopeFamily,
    target: DeliveryTarget,
) -> str:
    assert signal.signal_id is not None
    outbox_id = target.delivery_key(signal.signal_id)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO delivery_outbox(
                outbox_id, signal_id, global_sequence, recipient_id, channel,
                status, expires_at, attempt_count, next_attempt_at,
                lease_owner, lease_started_at, lease_until, last_error,
                created_at, updated_at
            ) VALUES (?, ?, 1, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, NULL, ?, ?)
            """,
            (
                outbox_id,
                signal.signal_id,
                target.recipient_id,
                target.channel.value,
                OutboxStatus.PENDING.value,
                _encoded_time(signal.expires_at),
                _encoded_time(NOW),
                _encoded_time(NOW),
            ),
        )
    return outbox_id


def test_literal_legacy_and_current_rows_roundtrip_every_bus_read_path(
    tmp_path: Path,
) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    _insert_literal_signal_rows(path, _LITERAL_FAMILY_FIXTURES, routed=True)

    expected_types = tuple(item[1] for item in _LITERAL_FAMILY_FIXTURES)
    expected_ids = tuple(item[2] for item in _LITERAL_FAMILY_FIXTURES)

    by_sequence = tuple(store.signal(index) for index in range(1, 6))
    by_id = tuple(store.signal(signal_id) for signal_id in expected_ids)
    bounded = store.signals_after_global_sequence(
        after_sequence=0,
        through_sequence=5,
        observed_at=NOW,
        limit=10,
    )
    routed = store.routed_signals_after_global_sequence(
        after_sequence=0,
        through_sequence=5,
        limit=10,
    )

    for observed in (by_sequence, by_id):
        assert tuple(type(signal) for signal in observed) == expected_types
        assert tuple(signal.signal_id for signal in observed if signal is not None) == expected_ids
    for observed in (bounded, routed):
        assert tuple(type(record.signal) for record in observed) == expected_types
        assert tuple(record.signal.signal_id for record in observed) == expected_ids
        assert tuple(record.signal_id for record in observed) == expected_ids


@pytest.mark.parametrize(
    ("_name", "expected_type", "expected_id", "literal"),
    _CURRENT_FIXTURES,
    ids=[item[0] for item in _CURRENT_FIXTURES],
)
def test_notification_state_worker_formatter_and_policy_keep_current_identity(
    tmp_path: Path,
    _name: str,
    expected_type: type[SignalEnvelopeFamily],
    expected_id: str,
    literal: bytes,
) -> None:
    path = tmp_path / "notification-state.sqlite3"
    store = NotificationStateStore(path)
    _insert_literal_signal_rows(path, ((_name, expected_type, expected_id, literal),), routed=False)
    signal = store.signal(expected_id)
    assert signal is not None
    assert type(signal) is expected_type
    assert signal.signal_id == expected_id

    target = DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER)
    outbox_id = _insert_pending_outbox(path, signal=signal, target=target)

    class RecordingProvider(NotificationProvider):
        def __init__(self) -> None:
            self.deliveries: list[NotificationDelivery] = []

        def deliver(self, delivery: NotificationDelivery) -> str:
            self.deliveries.append(delivery)
            return "r06-provider-receipt"

    provider = RecordingProvider()
    ticks = iter((NOW, NOW + timedelta(seconds=1), NOW + timedelta(seconds=2)))
    summary = run_notification_batch(
        store,
        {DeliveryChannel.PUSHDEER: provider},
        worker_id="r06-worker",
        now=NOW,
        lease_for=timedelta(seconds=30),
        limit=1,
        clock=lambda: next(ticks),
    )

    assert summary.succeeded_count == 1
    assert store.outbox_record(outbox_id).status is OutboxStatus.SUCCEEDED  # type: ignore[union-attr]
    delivered = provider.deliveries[0].signal
    assert type(delivered) is expected_type
    assert delivered.signal_id == expected_id

    title, body = format_signal_notification(delivered)
    assert title == "[rQuant] 600000.SH 买入观察"
    assert f"- 信号 ID：`{expected_id}`" in body
    assert '"volume_ratio": 2.5' in body

    resolver = FrozenRoutingPolicyResolver.from_document(
        source_path=tmp_path / "r06-policy.json",
        content_sha256=POLICY,
        policy=RoutingPolicyDocument(
            default_no_target_reason="r06_no_target",
            rules=(
                {
                    "strategy_id": "n-shape",
                    "strategy_version": "2.1.0",
                    "action": "b_intent",
                    "recipient_id": "admin",
                    "channel": "pushdeer",
                    "enabled": True,
                },
            ),
        ),
    )
    decision = resolver(delivered)
    assert decision.targets == (target,)


def test_family_consumers_match_legacy_formatting_and_routing_on_common_fields(
    tmp_path: Path,
) -> None:
    signals = tuple(parse_signal_envelope(item[3]) for item in _LITERAL_FAMILY_FIXTURES)
    resolver = FrozenRoutingPolicyResolver.from_document(
        source_path=tmp_path / "r06-policy.json",
        content_sha256=POLICY,
        policy=RoutingPolicyDocument(
            default_no_target_reason="r06_no_target",
            rules=(
                {
                    "strategy_id": "n-shape",
                    "strategy_version": "2.1.0",
                    "action": "b_intent",
                    "recipient_id": "admin",
                    "channel": "pushdeer",
                    "enabled": True,
                },
            ),
        ),
    )

    formatted = tuple(format_signal_notification(signal) for signal in signals)
    normalized_bodies = tuple(
        body.replace(str(signal.signal_id), "<signal-id>")
        for signal, (_title, body) in zip(signals, formatted, strict=True)
    )

    assert len(set(title for title, _body in formatted)) == 1
    assert len(set(normalized_bodies)) == 1
    assert len({resolver(signal) for signal in signals}) == 1
    assert tuple(signal.signal_id for signal in signals) == tuple(
        item[2] for item in _LITERAL_FAMILY_FIXTURES
    )


@pytest.mark.parametrize(
    ("_name", "_expected_type", "_expected_id", "literal"),
    _CURRENT_FIXTURES,
    ids=[item[0] for item in _CURRENT_FIXTURES],
)
def test_current_bus_and_notification_writers_are_typed_gates_with_zero_mutation(
    tmp_path: Path,
    _name: str,
    _expected_type: type[SignalEnvelopeFamily],
    _expected_id: str,
    literal: bytes,
) -> None:
    current = parse_signal_envelope(literal)
    assert type(current) is CurrentSignalEnvelope
    assert current.signal_id is not None

    ingest_path = tmp_path / "ingest.sqlite3"
    ingest_store = _store(ingest_path)
    before = _database_snapshot(ingest_path)
    with pytest.raises(LegacySignalWriteActivationError, match="reader-only"):
        ingest_store.ingest(current, received_at=NOW)  # type: ignore[arg-type]
    assert _database_snapshot(ingest_path) == before

    commit_path = tmp_path / "commit.sqlite3"
    commit_store = _store(commit_path)
    descriptor = RouteSourceDescriptor(
        source_id="r06-current-source",
        generation_id=GENERATION,
        strategy_spec_fingerprint=SPEC,
        first_sequence=1,
        high_watermark=1,
    )
    decision_fingerprint = routing_decision_fingerprint(
        routing_policy_fingerprint=POLICY,
        decision_kind=RouteDecisionKind.NO_TARGET,
        targets=(),
        reason_code="r06_no_target",
    )
    before = _database_snapshot(commit_path)
    with pytest.raises(LegacySignalWriteActivationError, match="reader-only"):
        commit_store.commit_source_route(
            descriptor=descriptor,
            routing_policy_fingerprint=POLICY,
            source_sequence=1,
            signal=current,  # type: ignore[arg-type]
            decision_kind=RouteDecisionKind.NO_TARGET,
            decision_fingerprint=decision_fingerprint,
            reason_code="r06_no_target",
            targets=(),
            routed_at=NOW,
        )
    assert _database_snapshot(commit_path) == before

    route_path = tmp_path / "route.sqlite3"
    route_store = _store(route_path)
    _insert_literal_signal_rows(
        route_path,
        ((_name, CurrentSignalEnvelope, current.signal_id, literal),),
        routed=False,
    )
    before = _database_snapshot(route_path)
    with pytest.raises(LegacySignalWriteActivationError, match="reader-only"):
        route_store.route(
            current.signal_id,
            (DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
            now=NOW,
        )
    assert _database_snapshot(route_path) == before

    notification_path = tmp_path / "notification.sqlite3"
    notification_store = NotificationStateStore(notification_path)
    record = _routed_record(literal)
    source = SignalBusSourceDescriptor(
        generation_id=GENERATION,
        first_global_sequence=1,
        high_watermark=1,
    )
    before = _database_snapshot(notification_path)
    with pytest.raises(LegacySignalWriteActivationError, match="reader-only"):
        notification_store.replicate(source, (record,), observed_at=NOW)
    assert _database_snapshot(notification_path) == before


def test_current_router_input_is_gated_before_bus_or_outbox_mutation(tmp_path: Path) -> None:
    current = parse_signal_envelope(_CURRENT_FIXTURES[0][3])
    assert type(current) is CurrentSignalEnvelope
    path = tmp_path / "router-bus.sqlite3"
    bus = _store(path)

    class CurrentSource:
        @staticmethod
        def read_batch(*, after_sequence: int, limit: int) -> RunnerSignalBatch:
            return RunnerSignalBatch(
                snapshot=SourceSnapshot(
                    descriptor=RouteSourceDescriptor(
                        source_id="r06-current-source",
                        generation_id=GENERATION,
                        strategy_spec_fingerprint=SPEC,
                        first_sequence=1,
                        high_watermark=1,
                    )
                ),
                after_sequence=after_sequence,
                limit=limit,
                records=({"sequence": 1, "signal": current},),
            )

    before = _database_snapshot(path)
    with pytest.raises(LegacySignalWriteActivationError, match="reader-only"):
        route_runner_signals(
            source_id="r06-current-source",
            source=CurrentSource(),
            bus=bus,
            cursors=SignalRouteCursorStore(
                tmp_path / "compatibility-cursor.sqlite3",
                routing_policy_fingerprint=POLICY,
            ),
            routed_at=NOW,
            target_resolver=lambda _signal: RoutingDecision.no_target(
                routing_policy_fingerprint=POLICY,
                reason_code="r06_no_target",
            ),
            limit=1,
        )
    assert _database_snapshot(path) == before


@pytest.mark.parametrize(
    "literal",
    (
        b"{",
        _CURRENT_FIXTURES[0][3].replace(
            b'"rquant.signal-envelope/v1"',
            b'"rquant.signal-envelope/v999"',
        ),
        _CURRENT_FIXTURES[0][3][:-1]
        + b',"producer_commit":"dddddddddddddddddddddddddddddddddddddddd"}',
        _CURRENT_FIXTURES[0][3].replace(
            b'"strategy_id":"n-shape"',
            b'"strategy_id":"n-shape","strategy_id":"n-shape"',
        ),
        _CURRENT_FIXTURES[0][3].replace(
            _CURRENT_FIXTURES[0][2].encode(),
            b"0" * 64,
        ),
    ),
    ids=("malformed", "unknown", "mixed", "duplicate-key", "corrupt-id"),
)
def test_invalid_stored_json_fails_closed_through_dispatcher(
    tmp_path: Path,
    literal: bytes,
) -> None:
    path = tmp_path / "invalid.sqlite3"
    store = _store(path)
    row_id = "7" * 64
    _insert_literal_signal_rows(
        path,
        (("invalid", CurrentSignalEnvelope, row_id, literal),),
        routed=False,
    )

    with pytest.raises((TypeError, ValueError)):
        store.signal(row_id)
    with pytest.raises((TypeError, ValueError)):
        store.signals_after_global_sequence(
            after_sequence=0,
            through_sequence=1,
            observed_at=NOW,
            limit=1,
        )


def test_runner_reader_rejects_duplicate_signal_keys_through_domain_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    signal = parse_signal_envelope(_LITERAL_FAMILY_FIXTURES[0][3])
    assert type(signal) is SignalEnvelope
    _write_runner_source(path, signal=signal)
    payload = (
        _LITERAL_FAMILY_FIXTURES[0][3]
        .replace(
            b'"strategy_id":"n-shape"',
            b'"strategy_id":"n-shape","strategy_id":"n-shape"',
        )
        .decode()
    )
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE runner_signal SET payload_json = ?", (payload,))
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
    )

    with pytest.raises(ValueError, match="runner signal payload is invalid"):
        source.read_batch(after_sequence=0, limit=1)


@pytest.mark.parametrize(
    ("_name", "expected_type", "expected_id", "literal"),
    _CURRENT_FIXTURES,
    ids=[item[0] for item in _CURRENT_FIXTURES],
)
def test_runner_db_reader_returns_exact_current_family(
    tmp_path: Path,
    _name: str,
    expected_type: type[SignalEnvelopeFamily],
    expected_id: str,
    literal: bytes,
) -> None:
    path = tmp_path / "runner-current.sqlite3"
    legacy = parse_signal_envelope(_LITERAL_FAMILY_FIXTURES[0][3])
    assert type(legacy) is SignalEnvelope
    _write_runner_source(path, signal=legacy)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE runner_signal SET signal_id = ?, payload_json = ?",
            (expected_id, literal.decode()),
        )
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
    )

    record = source.read_batch(after_sequence=0, limit=1).records[0]

    assert type(record.signal) is expected_type
    assert record.signal.signal_id == expected_id


@pytest.mark.parametrize(
    ("_name", "_expected_type", "_expected_id", "literal"),
    _LITERAL_FAMILY_FIXTURES[:3],
    ids=[item[0] for item in _LITERAL_FAMILY_FIXTURES[:3]],
)
def test_legacy_bus_writer_preserves_literal_canonical_bytes(
    tmp_path: Path,
    _name: str,
    _expected_type: type[SignalEnvelopeFamily],
    _expected_id: str,
    literal: bytes,
) -> None:
    signal = parse_signal_envelope(literal)
    assert type(signal) is SignalEnvelope
    store = _store(tmp_path / "legacy.sqlite3")

    store.ingest(signal, received_at=NOW)

    assert store.signal_payload(signal.signal_id).encode() == literal


def test_allowed_signal_readers_use_dispatcher_without_signal_reserialization() -> None:
    project_root = Path(__file__).parents[2]
    allowed_sources = (
        "src/rquant/signal_bus.py",
        "src/rquant/notification_state.py",
        "src/rquant/notification_worker.py",
        "src/rquant/runtime_notification_providers.py",
        "src/rquant/runtime_routing_policy.py",
        "src/rquant/signal_router_runtime.py",
    )
    for relative in allowed_sources:
        source = (project_root / relative).read_text()
        assert "SignalEnvelope.model_validate" not in source

    reader_functions: tuple[Callable[..., object], ...] = (
        SignalBusSignalRecord.validate_identity,
        SignalBusStore.signals_after_global_sequence,
        SignalBusStore.routed_signals_after_global_sequence,
        SignalBusStore.signal,
        NotificationStateStore.serving_snapshot,
        NotificationDelivery.validate_delivery,
        format_signal_notification,
        _query_signal_records,
    )
    for reader in reader_functions:
        source = inspect.getsource(reader)
        assert "parse_signal_envelope" in source or "signal.model_dump" not in source
        assert "signal.model_dump" not in source
