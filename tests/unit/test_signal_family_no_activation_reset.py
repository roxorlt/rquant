"""Exhaustive Phase-A fail-before-mutation behavior for current-family inputs."""

from __future__ import annotations

import base64
import gc
import hashlib
import inspect
import sqlite3
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest

import rquant.daily_notification_producer as daily_notification
import rquant.daily_summary_stage as daily_summary
import rquant.notification_state as notification_state
import rquant.paper_signal_worker as paper_signal_worker
import rquant.runtime_builder_signal as runtime_builder_signal
import rquant.runtime_serving_authority as runtime_serving_authority
import rquant.runtime_serving_snapshot as runtime_serving_snapshot
import rquant.signal_bus as signal_bus
import rquant.signal_route_spool as spool
import rquant.signal_router_runtime as signal_router_runtime
import rquant.strategy_runner as strategy_runner
from rquant.daily_notification_producer import DailyNotificationProducer
from rquant.daily_pool_stage import DailyDownstreamArtifactStore
from rquant.daily_summary_stage import DailySummaryStage
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.notification_state import NotificationServingSnapshot, NotificationStateStore
from rquant.paper_signal_worker import PaperSignalQueueStore
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPublisher,
    ServingSourceAuthorityUnavailableError,
)
from rquant.runtime_serving_snapshot import (
    SIGNALS_DATASET_ID,
    SignalDeliveryPayload,
    SignalDeliveryReadPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingSignalRecord
from rquant.signal_bus import (
    LegacySignalWriteActivationError,
    RouteDecisionKind,
    RouteSourceDescriptor,
    SignalBusRoutedRecord,
    SignalBusSourceDescriptor,
    routing_decision_fingerprint,
)
from rquant.signal_contracts import CurrentSignalEnvelope, SignalEnvelope, parse_signal_envelope
from rquant.signal_family_differential_gate import (
    BOUNDARY_PROBES,
    BoundaryReachedSentinelV1,
    ConstructorIdentityFenceSentinelV1,
)
from rquant.signal_router_runtime import (
    RoutingDecision,
    RunnerSignalBatch,
    SignalRouteCursorStore,
    SourceSnapshot,
    route_runner_signals,
)
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_paper_signal_dual_read_r06 import _policy as _paper_policy
from tests.unit.test_signal_contracts import (
    _CURRENT_CANONICAL_FIXTURES,
    _LEGACY_CANONICAL_FIXTURES,
)
from tests.unit.test_signal_dual_read_r06 import (
    GENERATION,
    POLICY,
    SPEC,
    _database_bytes_snapshot,
    _database_snapshot,
    _insert_literal_signal_rows,
    _routed_record,
    _store,
)
from tests.unit.test_strategy_runner import (
    NOW as RUNNER_NOW,
)
from tests.unit.test_strategy_runner import (
    _entry_decision,
    _envelope,
    _frame,
)
from tests.unit.test_strategy_runner import (
    _store as _runner_store,
)

NOW = datetime(2026, 8, 16, 2, 30, tzinfo=UTC)
CURRENT_LITERAL = _CURRENT_CANONICAL_FIXTURES[0][2]
LEGACY_LITERAL = _LEGACY_CANONICAL_FIXTURES[0][3]


def _registry_clock() -> datetime:
    return NOW


def _current_signal() -> CurrentSignalEnvelope:
    signal = parse_signal_envelope(CURRENT_LITERAL)
    assert type(signal) is CurrentSignalEnvelope
    return signal


def _tree_snapshot(root: Path) -> tuple[tuple[str, str, bytes], ...]:
    if not root.exists():
        return ()
    return tuple(
        (
            "directory" if path.is_dir() else "file",
            path.relative_to(root).as_posix(),
            b"" if path.is_dir() else path.read_bytes(),
        )
        for path in sorted(root.rglob("*"))
    )


def _sqlite_snapshot(path: Path) -> tuple[object, ...]:
    gc.collect()
    database_bytes = _database_bytes_snapshot(path)
    with TemporaryDirectory(prefix="rquant-r07-sqlite-snapshot-") as directory:
        snapshot_root = Path(directory)
        snapshot_path = snapshot_root / path.name
        for name, payload in database_bytes:
            if not name.endswith("-shm"):
                (snapshot_root / name).write_bytes(payload)
        rows = _database_snapshot(snapshot_path)
    return (rows, database_bytes)


def _prime_sqlite_readonly_sidecars(path: Path) -> None:
    gc.collect()
    connection = sqlite3.connect(
        f"file:{path}?mode=ro",
        uri=True,
        isolation_level=None,
    )
    try:
        connection.execute("PRAGMA query_only = ON")
        connection.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
    finally:
        connection.close()


def _source_descriptor() -> RouteSourceDescriptor:
    return RouteSourceDescriptor(
        source_id="r07-current-source",
        generation_id=GENERATION,
        strategy_spec_fingerprint=SPEC,
        first_sequence=1,
        high_watermark=1,
    )


def _decision_fingerprint() -> str:
    return routing_decision_fingerprint(
        routing_policy_fingerprint=POLICY,
        decision_kind=RouteDecisionKind.NO_TARGET,
        targets=(),
        reason_code="r07_no_target",
    )


def _forbidden_mutation(calls: list[str], label: str) -> Callable[..., object]:
    def forbidden(*_args: object, **_kwargs: object) -> object:
        calls.append(label)
        raise AssertionError(f"mutation guard reached: {label}")

    return forbidden


def _install_boundary_guard(
    monkeypatch: pytest.MonkeyPatch,
    module: object,
    calls: list[str],
) -> None:
    original = module.require_legacy_signal_write  # type: ignore[attr-defined]

    def guarded(signal: object, *, operation: str) -> object:
        calls.append(operation)
        return original(signal, operation=operation)

    monkeypatch.setattr(module, "require_legacy_signal_write", guarded)


def _assert_boundary_sentinel(
    inventory_id: str,
    *,
    reached_count: int,
    mutation_guards: tuple[list[str], ...] = (),
) -> None:
    probe = next(item for item in BOUNDARY_PROBES if item.inventory_id == inventory_id)
    sentinel = BoundaryReachedSentinelV1(
        sentinel_id=f"sentinel-{inventory_id.lower()}",
        inventory_id=inventory_id,
        source_span=probe.source_span,
        ast_digest=probe.boundary_ast_sha256,
        reached_count=reached_count,
        mutation_reached_count=sum(len(calls) for calls in mutation_guards),
    )
    assert sentinel.passed
    assert all(not calls for calls in mutation_guards)


def test_strategy_runner_rejects_a_substituted_current_constructor_without_row_or_byte_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "runner.sqlite3"
    store = _runner_store(path)
    current = _current_signal()
    constructor_calls: list[str] = []
    mutation_calls: list[str] = []

    def substitute_current_constructor(**_values: object) -> CurrentSignalEnvelope:
        constructor_calls.append("SignalEnvelope")
        return current

    monkeypatch.setattr(strategy_runner, "SignalEnvelope", substitute_current_constructor)
    monkeypatch.setattr(
        store,
        "_connect",
        _forbidden_mutation(mutation_calls, "StrategyRunnerStore._connect"),
    )
    before = _sqlite_snapshot(path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only") as exc_info:
        store.process_batch(
            _envelope(),
            _frame(),
            feature_payload=None,
            source_receipt=None,
            dataset_snapshot_id="d" * 64,
            observed_at=RUNNER_NOW,
            evaluator=_entry_decision,
        )

    assert _sqlite_snapshot(path) == before
    assert constructor_calls == []
    assert mutation_calls == []
    replacement_identity = str(id(substitute_current_constructor))
    observed_identity = str(id(strategy_runner.SignalEnvelope))
    assert ConstructorIdentityFenceSentinelV1(
        sentinel_id="sentinel-r07-b01",
        inventory_id="R07-B01",
        replaced_global="strategy_runner.SignalEnvelope",
        expected_replacement_identity=replacement_identity,
        observed_identity=observed_identity,
        reached_count=int(
            "StrategyRunnerStore.process_batch is legacy-only" in str(exc_info.value)
        ),
    ).passed


def _daily_stage(tmp_path: Path) -> DailySummaryStage:
    return DailySummaryStage(
        signal_bus=_store(tmp_path / "daily-bus.sqlite3"),
        strategy_version="daily-close-dag/v1",
        producer_commit="a" * 40,
        clock=lambda: NOW,
        artifact_store=DailyDownstreamArtifactStore(tmp_path / "daily-artifacts"),
        canonical_reader_factory=lambda: DuckDBStore(
            tmp_path / "unused.duckdb",
            read_only=True,
        ),
    )


@pytest.mark.parametrize("producer", ("summary", "cli-error"))
def test_daily_signal_constructors_reject_current_family_substitution_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    producer: str,
) -> None:
    current = _current_signal()
    stage = _daily_stage(tmp_path)
    constructor_calls: list[str] = []
    guard_calls: list[str] = []
    mutation_calls: list[str] = []

    def substitute_current_constructor(**_values: object) -> CurrentSignalEnvelope:
        constructor_calls.append("SignalEnvelope")
        return current

    module = daily_summary if producer == "summary" else daily_notification
    _install_boundary_guard(monkeypatch, module, guard_calls)
    monkeypatch.setattr(module, "SignalEnvelope", substitute_current_constructor)
    monkeypatch.setattr(
        stage._notification_producer._signal_bus,
        "_write_transaction",
        _forbidden_mutation(mutation_calls, "SignalBusStore._write_transaction"),
    )
    before = _tree_snapshot(tmp_path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        if producer == "cli-error":
            daily_notification.build_daily_error_signal(
                component="screen",
                error=RuntimeError("private detail"),
                trade_date=date(2026, 8, 16),
                observed_at=NOW,
                producer_commit="a" * 40,
                ttl=timedelta(days=7),
            )
        else:
            stage.build_signal(
                trade_date=date(2026, 8, 16),
                canonical_generation_id="b" * 64,
                canonical_receipt_id="c" * 64,
                canonical_content_hash="d" * 64,
                screen_hits={"n-shape-pool1": 1},
                pool2_active_count=0,
                errors=(),
                event_time=NOW,
            )

    assert _tree_snapshot(tmp_path) == before
    assert constructor_calls == ["SignalEnvelope"]
    assert len(guard_calls) == 1
    inventory_id = "R07-B02" if producer == "summary" else "R07-B04"
    _assert_boundary_sentinel(
        inventory_id,
        reached_count=len(guard_calls),
        mutation_guards=(mutation_calls,),
    )


def test_r07_b03_sentinel_is_lazy_and_stops_before_first_yield(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = _current_signal()
    stage = _daily_stage(tmp_path)
    constructor_calls: list[str] = []
    guard_calls: list[str] = []
    mutation_calls: list[str] = []

    def substitute_current_constructor(**_values: object) -> CurrentSignalEnvelope:
        constructor_calls.append("SignalEnvelope")
        return current

    original_guard = daily_summary.require_legacy_signal_write

    def guarded(signal: object, *, operation: str) -> object:
        guard_calls.append(operation)
        return original_guard(signal, operation=operation)

    monkeypatch.setattr(daily_summary, "SignalEnvelope", substitute_current_constructor)
    monkeypatch.setattr(daily_summary, "require_legacy_signal_write", guarded)
    monkeypatch.setattr(
        stage._notification_producer._signal_bus,
        "_write_transaction",
        _forbidden_mutation(mutation_calls, "SignalBusStore._write_transaction"),
    )
    canonical = SimpleNamespace(
        generation_id="b" * 64,
        receipt_id="c" * 64,
        db_content_sha256="d" * 64,
        trade_date=date(2026, 8, 16),
    )
    before = _tree_snapshot(tmp_path)
    pending = stage._error_signals(canonical, ("screen",), NOW)

    assert constructor_calls == []
    assert guard_calls == []
    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        tuple(pending)

    assert constructor_calls == ["SignalEnvelope"]
    assert guard_calls == ["DailySummaryStage._error_signals"]
    assert _tree_snapshot(tmp_path) == before
    _assert_boundary_sentinel(
        "R07-B03",
        reached_count=len(guard_calls),
        mutation_guards=(mutation_calls,),
    )


def test_daily_notification_emit_preflights_before_calling_the_bus(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mutations: list[str] = []
    guard_calls: list[str] = []

    class _Bus:
        def ingest(self, *_args: object, **_kwargs: object) -> object:
            mutations.append("ingest")
            raise AssertionError("producer reached the bus")

    producer = DailyNotificationProducer(signal_bus=_Bus())  # type: ignore[arg-type]
    _install_boundary_guard(monkeypatch, daily_notification, guard_calls)
    before = _tree_snapshot(tmp_path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        producer.emit(_current_signal(), received_at=NOW)  # type: ignore[arg-type]

    assert _tree_snapshot(tmp_path) == before
    assert mutations == []
    _assert_boundary_sentinel(
        "R07-B05",
        reached_count=len(guard_calls),
        mutation_guards=(mutations,),
    )


def test_signal_bus_ingest_preflights_before_database_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "ingest.sqlite3"
    store = _store(path)
    guard_calls: list[str] = []
    transaction_calls: list[str] = []
    _install_boundary_guard(monkeypatch, signal_bus, guard_calls)
    monkeypatch.setattr(
        store,
        "_write_transaction",
        _forbidden_mutation(transaction_calls, "SignalBusStore._write_transaction"),
    )
    before = _sqlite_snapshot(path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.ingest(_current_signal(), received_at=NOW)  # type: ignore[arg-type]

    assert _sqlite_snapshot(path) == before
    _assert_boundary_sentinel(
        "R07-B06",
        reached_count=len(guard_calls),
        mutation_guards=(transaction_calls,),
    )


def test_signal_bus_private_ingest_preflights_before_connection_mutation(tmp_path: Path) -> None:
    path = tmp_path / "private-ingest.sqlite3"
    store = _store(path)
    before = _sqlite_snapshot(path)
    connection = store._connect()
    before_changes = connection.total_changes
    try:
        with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
            store._ingest_in_transaction(
                connection,
                _current_signal(),  # type: ignore[arg-type]
                received_at=NOW,
            )
        assert connection.total_changes == before_changes
        assert not connection.in_transaction
    finally:
        connection.close()

    assert _sqlite_snapshot(path) == before


def test_signal_bus_commit_preflights_before_source_signal_receipt_or_outbox_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "commit.sqlite3"
    store = _store(path)
    guard_calls: list[str] = []
    transaction_calls: list[str] = []
    _install_boundary_guard(monkeypatch, signal_bus, guard_calls)
    monkeypatch.setattr(
        store,
        "_write_transaction",
        _forbidden_mutation(transaction_calls, "SignalBusStore._write_transaction"),
    )
    before = _sqlite_snapshot(path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.commit_source_route(
            descriptor=_source_descriptor(),
            routing_policy_fingerprint=POLICY,
            source_sequence=1,
            signal=_current_signal(),  # type: ignore[arg-type]
            decision_kind=RouteDecisionKind.NO_TARGET,
            decision_fingerprint=_decision_fingerprint(),
            reason_code="r07_no_target",
            targets=(),
            routed_at=NOW,
        )

    assert _sqlite_snapshot(path) == before
    _assert_boundary_sentinel(
        "R07-B08",
        reached_count=len(guard_calls),
        mutation_guards=(transaction_calls,),
    )


def test_signal_bus_route_rejects_stored_current_before_outbox_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "route.sqlite3"
    store = _store(path)
    current = _current_signal()
    assert current.signal_id is not None
    _insert_literal_signal_rows(
        path,
        (("current", CurrentSignalEnvelope, current.signal_id, CURRENT_LITERAL),),
        routed=False,
    )
    _prime_sqlite_readonly_sidecars(path)
    before = _sqlite_snapshot(path)
    guard_calls: list[str] = []
    transaction_calls: list[str] = []
    original_guard = signal_bus.require_legacy_signal_write

    def guarded(signal: object, *, operation: str) -> object:
        guard_calls.append(operation)
        return original_guard(signal, operation=operation)

    monkeypatch.setattr(signal_bus, "require_legacy_signal_write", guarded)
    monkeypatch.setattr(
        store,
        "_write_transaction",
        _forbidden_mutation(transaction_calls, "SignalBusStore._write_transaction"),
    )

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.route(
            current.signal_id,
            (DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
            now=NOW,
        )

    assert _sqlite_snapshot(path) == before
    assert guard_calls == ["SignalBusStore.route"]
    _assert_boundary_sentinel(
        "R07-B07",
        reached_count=len(guard_calls),
        mutation_guards=(transaction_calls,),
    )


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
    _prime_sqlite_readonly_sidecars(path)
    before = _sqlite_snapshot(path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.route(current.signal_id, (), now=NOW)

    assert _sqlite_snapshot(path) == before


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
            store.route(current.signal_id, (), now=NOW)
        assert store.route(legacy.signal_id, (), now=NOW) == ()

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
                    NOW.isoformat().replace("+00:00", "Z"),
                ),
            )
        writer.execute("COMMIT")
        assert path.with_name(f"{path.name}-wal").stat().st_size > 0

        with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
            store.route(current.signal_id, (), now=NOW)
        assert store.route(legacy.signal_id, (), now=NOW) == ()
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
                    NOW.isoformat().replace("+00:00", "Z"),
                ),
            )
            writer.execute("COMMIT")
            writers.append(writer)
            assert path.with_name(f"{path.name}-wal").stat().st_size > 0
        return original_connect(**options)  # type: ignore[arg-type]

    monkeypatch.setattr(store, "_connect_readonly", commit_current_before_open)
    try:
        with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
            store.route(current.signal_id, (), now=NOW)
        assert store.route(legacy.signal_id, (), now=NOW) == ()
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
            now=NOW,
        )

    assert store.signal(current.signal_id) == current
    assert store.outbox_records() == ()


def test_route_runner_preflights_full_batch_before_bus_cursor_or_source_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus_path = tmp_path / "router.sqlite3"
    cursor_path = tmp_path / "cursor.sqlite3"
    bus = _store(bus_path)
    current = _current_signal()
    source_calls: list[tuple[int, int]] = []
    guard_calls: list[str] = []
    mutation_calls: list[str] = []

    class _Source:
        @staticmethod
        def read_batch(*, after_sequence: int, limit: int) -> RunnerSignalBatch:
            source_calls.append((after_sequence, limit))
            return RunnerSignalBatch(
                snapshot=SourceSnapshot(descriptor=_source_descriptor()),
                after_sequence=after_sequence,
                limit=limit,
                records=({"sequence": 1, "signal": current},),
            )

    original_guard = signal_router_runtime.require_legacy_signal_write

    def guarded(signal: object, *, operation: str) -> object:
        guard_calls.append(operation)
        return original_guard(signal, operation=operation)

    monkeypatch.setattr(signal_router_runtime, "require_legacy_signal_write", guarded)
    cursors = SignalRouteCursorStore(
        cursor_path,
        routing_policy_fingerprint=POLICY,
    )
    monkeypatch.setattr(
        cursors,
        "bind",
        _forbidden_mutation(mutation_calls, "SignalRouteCursorStore.bind"),
    )
    monkeypatch.setattr(
        bus,
        "bind_route_source",
        _forbidden_mutation(mutation_calls, "SignalBusStore.bind_route_source"),
    )
    monkeypatch.setattr(
        bus,
        "commit_source_route",
        _forbidden_mutation(mutation_calls, "SignalBusStore.commit_source_route"),
    )
    _prime_sqlite_readonly_sidecars(bus_path)
    before_bus = _sqlite_snapshot(bus_path)
    before_tree = _tree_snapshot(tmp_path)
    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        route_runner_signals(
            source_id="r07-current-source",
            source=_Source(),
            bus=bus,
            cursors=cursors,
            routed_at=NOW,
            target_resolver=lambda _signal: RoutingDecision.no_target(
                routing_policy_fingerprint=POLICY,
                reason_code="r07_no_target",
            ),
            limit=1,
        )

    assert _sqlite_snapshot(bus_path) == before_bus
    assert _tree_snapshot(tmp_path) == before_tree
    assert not cursor_path.exists()
    assert source_calls == [(0, 1)]
    assert guard_calls == ["route_runner_signals"]
    _assert_boundary_sentinel(
        "R07-B09",
        reached_count=len(guard_calls),
        mutation_guards=(mutation_calls,),
    )


def _current_routed_record() -> SignalBusRoutedRecord:
    return _routed_record(CURRENT_LITERAL)


def test_signal_route_spool_publish_preflights_before_lock_source_record_or_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "spool"
    route_spool = spool.SignalRouteSpool(root)
    boundary_calls: list[str] = []
    open_calls: list[str] = []
    original_preflight = spool._require_legacy_spool_publish_input

    def preflight(**kwargs: object) -> None:
        if kwargs.get("records"):
            boundary_calls.append("current-record")
        original_preflight(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(spool, "_require_legacy_spool_publish_input", preflight)
    monkeypatch.setattr(
        spool,
        "_open_root_directory",
        _forbidden_mutation(open_calls, "signal_route_spool._open_root_directory"),
    )
    before = _tree_snapshot(root)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        route_spool.publish(
            source=SignalBusSourceDescriptor(generation_id="f" * 64, high_watermark=1),
            records=(_current_routed_record(),),
        )

    assert _tree_snapshot(root) == before
    _assert_boundary_sentinel(
        "R07-B10",
        reached_count=len(boundary_calls),
        mutation_guards=(open_calls,),
    )


def test_signal_route_spool_prefix_preflights_before_initial_empty_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "prefix-spool"
    route_spool = spool.SignalRouteSpool(root)
    source = SignalBusSourceDescriptor(generation_id="f" * 64, high_watermark=1)
    current = _current_routed_record()

    class _Bus:
        @staticmethod
        def source_descriptor() -> SignalBusSourceDescriptor:
            return source

        @staticmethod
        def routed_signals_after_global_sequence(
            *, after_sequence: int, through_sequence: int, limit: int
        ) -> tuple[SignalBusRoutedRecord, ...]:
            assert (after_sequence, through_sequence, limit) == (0, 1, 1)
            return (current,)

    before = _tree_snapshot(root)
    preflight_calls: list[str] = []
    boundary_calls: list[str] = []
    original_preflight = spool._require_legacy_spool_publish_input

    def preflight(**kwargs: object) -> None:
        preflight_calls.append("_require_legacy_spool_publish_input")
        if kwargs.get("records"):
            boundary_calls.append("current-record")
        original_preflight(**kwargs)  # type: ignore[arg-type]

    publish_calls: list[str] = []
    original_publish = route_spool.publish

    def publish(**kwargs: object) -> object:
        publish_calls.append("publish")
        return original_publish(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(spool, "_require_legacy_spool_publish_input", preflight)
    monkeypatch.setattr(route_spool, "publish", publish)
    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        spool.publish_signal_bus_prefix(
            bus=_Bus(),  # type: ignore[arg-type]
            spool=route_spool,
            limit=1,
        )

    assert _tree_snapshot(root) == before
    assert preflight_calls == [
        "_require_legacy_spool_publish_input",
        "_require_legacy_spool_publish_input",
    ]
    assert publish_calls == []
    _assert_boundary_sentinel(
        "R07-B11",
        reached_count=len(boundary_calls),
        mutation_guards=(publish_calls,),
    )


def test_notification_replicate_preflights_all_records_before_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "notification.sqlite3"
    store = NotificationStateStore(path)
    guard_calls: list[str] = []
    transaction_calls: list[str] = []
    _install_boundary_guard(monkeypatch, notification_state, guard_calls)
    monkeypatch.setattr(
        store,
        "_write_transaction",
        _forbidden_mutation(transaction_calls, "NotificationStateStore._write_transaction"),
    )
    before = _sqlite_snapshot(path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.replicate(
            SignalBusSourceDescriptor(generation_id=GENERATION, high_watermark=1),
            (_current_routed_record(),),
            observed_at=NOW,
        )

    assert _sqlite_snapshot(path) == before
    _assert_boundary_sentinel(
        "R07-B12",
        reached_count=len(guard_calls),
        mutation_guards=(transaction_calls,),
    )


@pytest.mark.parametrize("ingest_form", ("direct", "stored-bytes"))
def test_paper_queue_ingest_forms_preflight_before_queue_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ingest_form: str,
) -> None:
    path = tmp_path / f"paper-{ingest_form}.sqlite3"
    queue = PaperSignalQueueStore(path, policy=_paper_policy())
    current = _current_signal()
    guard_calls: list[str] = []
    connect_calls: list[str] = []
    kwargs: dict[str, object] = {}
    if ingest_form == "stored-bytes":
        kwargs = {
            "payload_json": CURRENT_LITERAL.decode("utf-8"),
            "payload_hash": hashlib.sha256(CURRENT_LITERAL).hexdigest(),
            "payload_size": len(CURRENT_LITERAL),
        }
    _install_boundary_guard(monkeypatch, paper_signal_worker, guard_calls)
    monkeypatch.setattr(
        queue,
        "_connect",
        _forbidden_mutation(connect_calls, "PaperSignalQueueStore._connect"),
    )
    before = _sqlite_snapshot(path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        queue.ingest(current, received_at=NOW, **kwargs)

    assert _sqlite_snapshot(path) == before
    inventory_id = "R07-B13" if ingest_form == "direct" else "R07-B14"
    _assert_boundary_sentinel(
        inventory_id,
        reached_count=len(guard_calls),
        mutation_guards=(connect_calls,),
    )


def test_signal_delivery_payload_rejects_current_before_authority_input_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_record = ServingSignalRecord(global_sequence=1, signal=_current_signal())
    guard_calls: list[str] = []
    _install_boundary_guard(monkeypatch, runtime_serving_snapshot, guard_calls)
    before = _tree_snapshot(tmp_path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        SignalDeliveryPayload(
            signals=(current_record,),
            routes=(),
            deliveries=(),
            projections=(),
        )

    assert _tree_snapshot(tmp_path) == before
    _assert_boundary_sentinel("R07-B15", reached_count=len(guard_calls))


def _current_serving_snapshot() -> NotificationServingSnapshot:
    return NotificationServingSnapshot(
        observed_at=NOW,
        sequence=1,
        visible_signal_count=1,
        returned_signal_count=1,
        omitted_signal_count=0,
        truncated=False,
        payload=SignalDeliveryReadPayload(
            signals=(ServingSignalRecord(global_sequence=1, signal=_current_signal()),)
        ),
    )


def _serving_snapshot_value(snapshot: NotificationServingSnapshot) -> tuple[object, ...]:
    return (
        snapshot.observed_at,
        snapshot.sequence,
        snapshot.visible_signal_count,
        snapshot.returned_signal_count,
        snapshot.omitted_signal_count,
        snapshot.truncated,
        snapshot.payload.model_dump(mode="json"),
        snapshot.projection_generation_id,
        tuple(sorted(snapshot.projection_source_receipts.items())),
    )


def test_publish_signal_authority_rejects_before_reader_or_publisher_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mutations: list[str] = []
    store_calls: list[dict[str, object]] = []
    snapshot = _current_serving_snapshot()

    class _Store:
        @staticmethod
        def serving_snapshot(**kwargs: object) -> NotificationServingSnapshot:
            store_calls.append(kwargs)
            return snapshot

    class _Publisher:
        producer_commit = "a" * 40

        @staticmethod
        def publish(_result: SourceReadResult) -> object:
            mutations.append("publish")
            raise AssertionError("current payload reached authority publication")

    class _Reader:
        expected_producer_commit = "a" * 40

        @staticmethod
        def __call__(_observed_at: datetime) -> SourceReadResult:
            mutations.append("read")
            raise ServingSourceAuthorityUnavailableError("missing")

    guard_calls: list[str] = []
    original_guard = runtime_serving_snapshot.require_legacy_signal_write

    def guarded(signal: object, *, operation: str) -> object:
        guard_calls.append(operation)
        return original_guard(signal, operation=operation)

    monkeypatch.setattr(runtime_serving_snapshot, "require_legacy_signal_write", guarded)
    before = _serving_snapshot_value(snapshot)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        runtime_builder_signal._publish_signal_authority(
            store=_Store(),  # type: ignore[arg-type]
            publisher=_Publisher(),  # type: ignore[arg-type]
            reader=_Reader(),  # type: ignore[arg-type]
            previous_reader=None,
            observed_at=NOW,
            history_limit=10,
        )

    assert mutations == []
    assert _serving_snapshot_value(snapshot) == before
    assert store_calls == [{"observed_at": NOW, "history_limit": 10}]
    assert guard_calls == ["SignalDeliveryPayload"]
    _assert_boundary_sentinel(
        "R07-B16",
        reached_count=len(guard_calls),
        mutation_guards=(mutations,),
    )


def _current_source_result() -> SourceReadResult:
    provisional = SourceReadResult(
        dataset_id=SIGNALS_DATASET_ID,
        generation_id="0" * 64,
        sequence=1,
        event_time=NOW,
        published_at=NOW,
        status=FreshnessStatus.FRESH,
        payload=_current_serving_snapshot().payload,
    )
    values = provisional.model_dump(mode="python", exclude={"generation_id"})
    return SourceReadResult.model_validate({**values, "generation_id": canonical_sha256(values)})


def test_serving_authority_publisher_rejects_current_before_generation_or_pointer_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "authority"
    publisher = ServingSourceAuthorityPublisher(
        root=root,
        producer_commit="a" * 40,
        dataset_id=SIGNALS_DATASET_ID,
        payload_kind="signal_delivery",
        clock=lambda: NOW,
    )
    guard_calls: list[str] = []
    open_calls: list[str] = []
    _install_boundary_guard(monkeypatch, signal_bus, guard_calls)
    monkeypatch.setattr(
        runtime_serving_authority,
        "_open_or_create_root",
        _forbidden_mutation(open_calls, "runtime_serving_authority._open_or_create_root"),
    )
    before = _tree_snapshot(tmp_path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        publisher.publish(_current_source_result())

    assert _tree_snapshot(tmp_path) == before
    assert not root.exists()
    _assert_boundary_sentinel(
        "R07-B17",
        reached_count=len(guard_calls),
        mutation_guards=(open_calls,),
    )


def test_decoder_and_read_models_remain_constructible_without_writer_authority() -> None:
    raw = base64.b64decode("eyJzY2hlbWFfdmVyc2lvbiI6M30=", validate=True)
    with pytest.raises(spool.SignalRouteSpoolIntegrityError):
        spool.decode_current_signal_route_spool_record(raw)
    assert inspect.isclass(spool.CurrentSignalBusRoutedRecord)
    assert inspect.isclass(spool.CurrentSignalRouteSpoolRecord)
