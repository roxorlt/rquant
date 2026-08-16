"""Exhaustive Phase-A fail-before-mutation and no-reachability evidence."""

from __future__ import annotations

import ast
import base64
import functools
import gc
import hashlib
import inspect
import sqlite3
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from types import FunctionType, MethodType, ModuleType, SimpleNamespace
from typing import get_args

import pytest

import rquant.daily_notification_producer as daily_notification
import rquant.daily_summary_stage as daily_summary
import rquant.runtime_builder_signal as runtime_builder_signal
import rquant.runtime_service_builtin as runtime_service_builtin
import rquant.runtime_service_main as runtime_service_main
import rquant.signal_bus as signal_bus
import rquant.signal_route_spool as spool
import rquant.strategy_runner as strategy_runner
from rquant.daily_notification_producer import DailyNotificationProducer
from rquant.daily_pool_stage import DailyDownstreamArtifactStore
from rquant.daily_summary_stage import DailySummaryStage
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.notification_state import NotificationServingSnapshot, NotificationStateStore
from rquant.paper_signal_worker import PaperSignalQueueStore
from rquant.runtime_builder_daily_orchestrator import daily_pipeline_orchestrator_builder
from rquant.runtime_builder_paper import paper_broker_builder, paper_consumer_builder
from rquant.runtime_builder_serving import serving_publisher_builder
from rquant.runtime_builder_shadow import shadow_session_builder
from rquant.runtime_builder_signal import notifier_builder, signal_router_builder
from rquant.runtime_builder_strategy import strategy_live_builder
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


def test_strategy_runner_rejects_a_substituted_current_constructor_without_row_or_byte_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "runner.sqlite3"
    store = _runner_store(path)
    current = _current_signal()
    monkeypatch.setattr(strategy_runner, "SignalEnvelope", lambda **_values: current)
    before = _sqlite_snapshot(path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.process_batch(
            _envelope(),
            _frame(),
            dataset_snapshot_id="d" * 64,
            observed_at=RUNNER_NOW,
            evaluator=_entry_decision,
        )

    assert _sqlite_snapshot(path) == before


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


@pytest.mark.parametrize("producer", ("summary", "stage-error", "cli-error"))
def test_daily_signal_constructors_reject_current_family_substitution_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    producer: str,
) -> None:
    current = _current_signal()
    stage = _daily_stage(tmp_path)
    before = _tree_snapshot(tmp_path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        if producer == "cli-error":
            monkeypatch.setattr(daily_notification, "SignalEnvelope", lambda **_values: current)
            daily_notification.build_daily_error_signal(
                component="screen",
                error=RuntimeError("private detail"),
                trade_date=date(2026, 8, 16),
                observed_at=NOW,
                producer_commit="a" * 40,
            )
        elif producer == "summary":
            monkeypatch.setattr(daily_summary, "SignalEnvelope", lambda **_values: current)
            stage.build_signal(
                trade_date=date(2026, 8, 16),
                canonical_generation_id="b" * 64,
                canonical_receipt_id="c" * 64,
                canonical_content_hash="d" * 64,
                screen_hits={"n-shape-pool1": 1},
                pool2_active_count=0,
                errors=(),
            )
        else:
            monkeypatch.setattr(daily_summary, "SignalEnvelope", lambda **_values: current)
            canonical = SimpleNamespace(
                generation_id="b" * 64,
                receipt_id="c" * 64,
                db_content_sha256="d" * 64,
                trade_date=date(2026, 8, 16),
            )
            tuple(stage._error_signals(canonical, ("screen",), NOW))

    assert _tree_snapshot(tmp_path) == before


def test_daily_notification_emit_preflights_before_calling_the_bus() -> None:
    mutations: list[str] = []

    class _Bus:
        def ingest(self, *_args: object, **_kwargs: object) -> object:
            mutations.append("ingest")
            raise AssertionError("producer reached the bus")

    producer = DailyNotificationProducer(signal_bus=_Bus())  # type: ignore[arg-type]

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        producer.emit(_current_signal(), received_at=NOW)  # type: ignore[arg-type]

    assert mutations == []


def test_signal_bus_ingest_preflights_before_database_mutation(tmp_path: Path) -> None:
    path = tmp_path / "ingest.sqlite3"
    store = _store(path)
    before = _sqlite_snapshot(path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.ingest(_current_signal(), received_at=NOW)  # type: ignore[arg-type]

    assert _sqlite_snapshot(path) == before


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
) -> None:
    path = tmp_path / "commit.sqlite3"
    store = _store(path)
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


def test_signal_bus_route_rejects_stored_current_before_outbox_mutation(tmp_path: Path) -> None:
    path = tmp_path / "route.sqlite3"
    store = _store(path)
    current = _current_signal()
    assert current.signal_id is not None
    _insert_literal_signal_rows(
        path,
        (("current", CurrentSignalEnvelope, current.signal_id, CURRENT_LITERAL),),
        routed=False,
    )
    before = _sqlite_snapshot(path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.route(
            current.signal_id,
            (DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
            now=NOW,
        )

    assert _sqlite_snapshot(path) == before


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
) -> None:
    bus_path = tmp_path / "router.sqlite3"
    cursor_path = tmp_path / "cursor.sqlite3"
    bus = _store(bus_path)
    current = _current_signal()

    class _Source:
        @staticmethod
        def read_batch(*, after_sequence: int, limit: int) -> RunnerSignalBatch:
            return RunnerSignalBatch(
                snapshot=SourceSnapshot(descriptor=_source_descriptor()),
                after_sequence=after_sequence,
                limit=limit,
                records=({"sequence": 1, "signal": current},),
            )

    before_bus = _sqlite_snapshot(bus_path)
    before_tree = _tree_snapshot(tmp_path)
    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        route_runner_signals(
            source_id="r07-current-source",
            source=_Source(),
            bus=bus,
            cursors=SignalRouteCursorStore(
                cursor_path,
                routing_policy_fingerprint=POLICY,
            ),
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


def _current_routed_record() -> SignalBusRoutedRecord:
    return _routed_record(CURRENT_LITERAL)


def test_signal_route_spool_publish_preflights_before_lock_source_record_or_pointer(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    route_spool = spool.SignalRouteSpool(root)
    before = _tree_snapshot(root)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        route_spool.publish(
            source=SignalBusSourceDescriptor(generation_id="f" * 64, high_watermark=1),
            records=(_current_routed_record(),),
        )

    assert _tree_snapshot(root) == before


def test_signal_route_spool_prefix_preflights_before_initial_empty_publication(
    tmp_path: Path,
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
    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        spool.publish_signal_bus_prefix(
            bus=_Bus(),  # type: ignore[arg-type]
            spool=route_spool,
            limit=1,
        )

    assert _tree_snapshot(root) == before


def test_notification_replicate_preflights_all_records_before_transaction(
    tmp_path: Path,
) -> None:
    path = tmp_path / "notification.sqlite3"
    store = NotificationStateStore(path)
    before = _sqlite_snapshot(path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        store.replicate(
            SignalBusSourceDescriptor(generation_id=GENERATION, high_watermark=1),
            (_current_routed_record(),),
            observed_at=NOW,
        )

    assert _sqlite_snapshot(path) == before


@pytest.mark.parametrize("ingest_form", ("direct", "stored-bytes"))
def test_paper_queue_ingest_forms_preflight_before_queue_transaction(
    tmp_path: Path,
    ingest_form: str,
) -> None:
    path = tmp_path / f"paper-{ingest_form}.sqlite3"
    queue = PaperSignalQueueStore(path, policy=_paper_policy())
    current = _current_signal()
    before = _sqlite_snapshot(path)
    kwargs: dict[str, object] = {}
    if ingest_form == "stored-bytes":
        kwargs = {
            "payload_json": CURRENT_LITERAL.decode("utf-8"),
            "payload_hash": hashlib.sha256(CURRENT_LITERAL).hexdigest(),
            "payload_size": len(CURRENT_LITERAL),
        }

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        queue.ingest(current, received_at=NOW, **kwargs)

    assert _sqlite_snapshot(path) == before


def test_signal_delivery_payload_rejects_current_before_authority_input_exists() -> None:
    current_record = ServingSignalRecord(global_sequence=1, signal=_current_signal())

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        SignalDeliveryPayload(signals=(current_record,))


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


def test_publish_signal_authority_rejects_before_reader_or_publisher_callbacks() -> None:
    mutations: list[str] = []
    snapshot = _current_serving_snapshot()

    class _Store:
        @staticmethod
        def serving_snapshot(**_kwargs: object) -> NotificationServingSnapshot:
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
) -> None:
    root = tmp_path / "authority"
    publisher = ServingSourceAuthorityPublisher(
        root=root,
        producer_commit="a" * 40,
        dataset_id=SIGNALS_DATASET_ID,
        payload_kind="signal_delivery",
        clock=lambda: NOW,
    )
    before = _tree_snapshot(tmp_path)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        publisher.publish(_current_source_result())

    assert _tree_snapshot(tmp_path) == before
    assert not root.exists()


def test_r07_exports_no_current_family_writer_or_activation_api() -> None:
    forbidden = {
        "append",
        "capability",
        "create",
        "cursor",
        "cutover",
        "drain",
        "environment",
        "migration",
        "overlay",
        "publish_v3",
    }
    assert not (forbidden & set(spool.__all__))


_PRODUCTION_MODULES = (
    "runtime_service_main.py",
    "runtime_service_builtin.py",
    "runtime_builder_strategy.py",
    "runtime_builder_signal.py",
    "runtime_builder_shadow.py",
    "runtime_builder_paper.py",
    "runtime_builder_serving.py",
    "runtime_builder_daily_orchestrator.py",
)
_FORBIDDEN_EXACT_NAMES = {
    "CurrentSignalRouteSpoolWriter",
    "SignalRouteSpoolV3Writer",
    "current_signal_writer",
    "publish_v3",
    "r07_activation",
    "r07_capability",
    "r07_cursor",
    "r07_cutover",
    "r07_drain",
    "r07_environment",
    "r07_flag",
    "r07_migration",
    "r07_overlay",
    "v3_activation",
    "v3_capability",
    "v3_cursor",
    "v3_cutover",
    "v3_drain",
    "v3_environment",
    "v3_flag",
    "v3_migration",
    "v3_overlay",
    "v3_writer",
}


def _ast_identifiers(tree: ast.AST) -> set[str]:
    identifiers = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    identifiers.update(node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute))
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Import, ast.ImportFrom)):
            continue
        for alias in node.names:
            identifiers.add(alias.name)
            identifiers.add(alias.name.rsplit(".", 1)[-1])
            if alias.asname is not None:
                identifiers.add(alias.asname)
    return identifiers


def test_production_builder_sources_have_no_v3_writer_or_activation_symbols() -> None:
    root = Path(__file__).parents[2] / "src" / "rquant"
    for name in _PRODUCTION_MODULES:
        tree = ast.parse((root / name).read_text(encoding="utf-8"))
        assert not (_ast_identifiers(tree) & _FORBIDDEN_EXACT_NAMES), name


def test_ast_inspection_retains_original_import_names_and_aliases() -> None:
    tree = ast.parse(
        "from rquant.signal_route_spool import SignalRouteSpoolV3Writer as harmless_reader\n"
    )

    assert {
        "SignalRouteSpoolV3Writer",
        "harmless_reader",
    } <= _ast_identifiers(tree)


def _children(value: object) -> Iterator[tuple[str, object]]:
    if isinstance(value, ModuleType):
        if value.__name__ != "rquant" and not value.__name__.startswith("rquant."):
            return
        for key, item in vars(value).items():
            if key.startswith("__"):
                continue
            if isinstance(item, ModuleType):
                if item.__name__ == "rquant" or item.__name__.startswith("rquant."):
                    yield f"module.{key}", item
                continue
            owner = getattr(item, "__module__", None)
            type_owner = getattr(type(item), "__module__", None)
            if (
                isinstance(item, (Mapping, tuple, list, set, frozenset, functools.partial))
                or owner == "rquant"
                or (isinstance(owner, str) and owner.startswith("rquant."))
                or type_owner == "rquant"
                or (isinstance(type_owner, str) and type_owner.startswith("rquant."))
            ):
                yield f"module.{key}", item
        return
    if isinstance(value, functools.partial):
        yield "partial.func", value.func
        yield from ((f"partial.arg[{index}]", item) for index, item in enumerate(value.args))
        yield from ((f"partial.kwarg.{key}", item) for key, item in (value.keywords or {}).items())
    if isinstance(value, MethodType):
        yield "method.func", value.__func__
        yield "method.self", value.__self__
    if isinstance(value, FunctionType):
        yield from (
            (f"default[{index}]", item) for index, item in enumerate(value.__defaults__ or ())
        )
        yield from (
            (f"kwdefault.{key}", item) for key, item in (value.__kwdefaults__ or {}).items()
        )
        for index, cell in enumerate(value.__closure__ or ()):
            try:
                yield f"closure[{index}]", cell.cell_contents
            except ValueError:
                continue
        yield from ((f"annotation.{key}", item) for key, item in value.__annotations__.items())
        for name in dict.fromkeys(value.__code__.co_names):
            if name in value.__globals__:
                yield f"global.{name}", value.__globals__[name]
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield f"mapping-key[{key!r}]", key
            yield f"mapping-value[{key!r}]", item
    elif isinstance(value, (tuple, list, set, frozenset)):
        yield from ((f"item[{index}]", item) for index, item in enumerate(value))
    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, dict):
        yield from ((f"attribute.{key}", item) for key, item in attributes.items())
    for slot in getattr(type(value), "__slots__", ()):
        if isinstance(slot, str) and hasattr(value, slot):
            yield f"slot.{slot}", getattr(value, slot)


_MAX_REACHABILITY_NODES = 10_000
_MAX_REACHABILITY_DEPTH = 64


def _walk_reachable(
    roots: Mapping[str, object],
    *,
    max_nodes: int = _MAX_REACHABILITY_NODES,
    max_depth: int = _MAX_REACHABILITY_DEPTH,
) -> Iterator[tuple[str, object]]:
    if type(max_nodes) is not int or max_nodes < 1:
        raise ValueError("reachability max_nodes must be a positive native integer")
    if type(max_depth) is not int or max_depth < 0:
        raise ValueError("reachability max_depth must be a nonnegative native integer")
    atomic = (str, bytes, bytearray, int, float, bool, Path, datetime, date, Enum)
    seen: set[int] = set()
    pending = [(path, value, 0) for path, value in roots.items()]
    while pending:
        path, value, depth = pending.pop()
        identity = id(value)
        if identity in seen:
            continue
        if len(seen) >= max_nodes:
            raise AssertionError("reachability node limit exceeded")
        seen.add(identity)
        yield path, value
        if value is None or isinstance(value, atomic + (type,)):
            continue
        children: list[tuple[str, object]] = []
        for label, child in _children(value):
            if len(children) >= max_nodes:
                raise AssertionError("reachability node limit exceeded")
            children.append((label, child))
        if children and depth >= max_depth:
            raise AssertionError("reachability depth limit exceeded")
        pending.extend((f"{path}.{label}", child, depth + 1) for label, child in children)


_READONLY_CURRENT_API = (
    CurrentSignalEnvelope,
    spool.CurrentSignalBusRoutedRecord,
    spool.CurrentSignalRouteSpoolRecord,
    spool.current_signal_bus_routed_record_json_bytes,
    spool.current_signal_route_spool_record_json_bytes,
    spool.decode_current_signal_route_spool_record,
    spool.verify_current_signal_route_spool_fixture,
)
_CURRENT_PERSISTENCE_INPUT_TYPES = (
    CurrentSignalEnvelope,
    spool.CurrentSignalBusRoutedRecord,
    spool.CurrentSignalRouteSpoolRecord,
)
_DURABLE_PUBLICATION_RESULT_TYPES = (
    spool.CurrentSignalRouteSpoolRecord,
    spool.SignalRouteSpoolPointer,
    spool.SignalRouteSpoolPublishSummary,
)
_DURABLE_SPOOL_OPERATIONS = (
    spool._atomic_replace_at,
    spool._immutable_write_at,
    spool._write_all,
    spool._write_temporary_at,
)


def _same_as_any(value: object, candidates: tuple[object, ...]) -> bool:
    return any(value is candidate for candidate in candidates)


def _resolve_annotation_attribute(node: ast.AST, namespace: Mapping[str, object]) -> object | None:
    if isinstance(node, ast.Name):
        return namespace.get(node.id)
    if not isinstance(node, ast.Attribute):
        return None
    parent = _resolve_annotation_attribute(node.value, namespace)
    if isinstance(parent, ModuleType):
        return vars(parent).get(node.attr)
    if isinstance(parent, type):
        return vars(parent).get(node.attr)
    return None


def _annotation_identities(
    annotation: object,
    namespace: Mapping[str, object],
) -> tuple[object, ...]:
    if annotation is inspect.Signature.empty:
        return ()
    if isinstance(annotation, str):
        try:
            expression = ast.parse(annotation, mode="eval")
        except SyntaxError:
            return ()
        resolved: list[object] = []
        for node in ast.walk(expression):
            if not isinstance(node, (ast.Name, ast.Attribute)):
                continue
            value = _resolve_annotation_attribute(node, namespace)
            if value is not None and not _same_as_any(value, tuple(resolved)):
                resolved.append(value)
        return tuple(resolved)
    nested = tuple(
        item
        for argument in get_args(annotation)
        for item in _annotation_identities(argument, namespace)
    )
    return (annotation, *nested)


def _callable_functions(value: object) -> Iterator[FunctionType]:
    if isinstance(value, functools.partial):
        yield from _callable_functions(value.func)
        return
    if isinstance(value, MethodType):
        yield value.__func__
        return
    if isinstance(value, FunctionType):
        yield value
        return
    if isinstance(value, type):
        attributes = vars(value)
    else:
        attributes = vars(type(value)) if callable(value) else {}
    for item in attributes.values():
        if isinstance(item, (classmethod, staticmethod)):
            item = item.__func__
        if isinstance(item, FunctionType):
            yield item


def _function_references(function: FunctionType) -> tuple[object, ...]:
    referenced: list[object] = []
    for item in (
        *(function.__defaults__ or ()),
        *(function.__kwdefaults__ or {}).values(),
    ):
        if not _same_as_any(item, tuple(referenced)):
            referenced.append(item)
    for cell in function.__closure__ or ():
        try:
            item = cell.cell_contents
        except ValueError:
            continue
        if not _same_as_any(item, tuple(referenced)):
            referenced.append(item)
    for name in dict.fromkeys(function.__code__.co_names):
        if name in function.__globals__:
            item = function.__globals__[name]
            if not _same_as_any(item, tuple(referenced)):
                referenced.append(item)
    modules = tuple(item for item in referenced if isinstance(item, ModuleType))
    for module in modules:
        attributes = vars(module)
        for name in dict.fromkeys(function.__code__.co_names):
            item = attributes.get(name)
            if item is not None and not _same_as_any(item, tuple(referenced)):
                referenced.append(item)
    return tuple(referenced)


def _exposes_durable_current_persistence(value: object) -> bool:
    for function in _callable_functions(value):
        signature = inspect.signature(function, eval_str=False)
        inputs = tuple(
            identity
            for parameter in signature.parameters.values()
            for identity in _annotation_identities(parameter.annotation, function.__globals__)
        )
        results = _annotation_identities(signature.return_annotation, function.__globals__)
        references = _function_references(function)
        current_input = any(
            _same_as_any(identity, _CURRENT_PERSISTENCE_INPUT_TYPES)
            for identity in (*inputs, *references)
        )
        durable_output = any(
            _same_as_any(identity, _DURABLE_PUBLICATION_RESULT_TYPES) for identity in results
        )
        durable_operation = any(
            _same_as_any(identity, _DURABLE_SPOOL_OPERATIONS) for identity in references
        )
        if current_input and (durable_output or durable_operation):
            return True
    return False


def _forbidden_reachable(path: str, value: object) -> str | None:
    if _same_as_any(value, (_registry_clock, *_READONLY_CURRENT_API)):
        return None
    if _exposes_durable_current_persistence(value):
        return f"{path}: durable current-family persistence capability"
    return None


class SignalRouteSpoolV3Writer:
    def __call__(
        self,
        record: spool.CurrentSignalRouteSpoolRecord,
    ) -> spool.SignalRouteSpoolPointer:
        raise AssertionError(f"synthetic writer must never run: {record.record_hash}")


_SYNTHETIC_V3_WRITER = SignalRouteSpoolV3Writer()


def _synthetic_callback_with_forbidden_global() -> object:
    return _SYNTHETIC_V3_WRITER


def test_reachability_walk_follows_referenced_callback_globals() -> None:
    violations = [
        violation
        for path, value in _walk_reachable(
            {"synthetic.callback": _synthetic_callback_with_forbidden_global}
        )
        if (violation := _forbidden_reachable(path, value)) is not None
    ]

    assert violations == [
        "synthetic.callback.global._SYNTHETIC_V3_WRITER: "
        "durable current-family persistence capability"
    ]


def test_reachability_detects_renamed_callable_behind_project_module_indirection() -> None:
    durable_operation = spool._immutable_write_at
    synthetic_module = ModuleType("rquant.synthetic_reachability_fixture")

    class DurableSpoolPublisher:
        def __call__(
            self,
            record: spool.CurrentSignalRouteSpoolRecord,
        ) -> spool.SignalRouteSpoolPointer:
            raise AssertionError(
                f"synthetic writer must never run: {durable_operation} {record.record_hash}"
            )

    DurableSpoolPublisher.__name__ = "Opaque"
    DurableSpoolPublisher.__qualname__ = "Opaque"
    DurableSpoolPublisher.__module__ = synthetic_module.__name__
    synthetic_module.alias = DurableSpoolPublisher()

    def callback() -> object:
        return synthetic_module

    violations = [
        violation
        for path, value in _walk_reachable({"synthetic.callback": callback})
        if (violation := _forbidden_reachable(path, value)) is not None
    ]

    assert violations == [
        "synthetic.callback.closure[0].module.alias: durable current-family persistence capability"
    ]


def test_reachability_walk_is_cycle_safe_and_fails_closed_at_bounds() -> None:
    cycle: list[object] = []
    cycle.append(cycle)

    assert [path for path, _value in _walk_reachable({"cycle": cycle})] == ["cycle"]
    with pytest.raises(AssertionError, match="node limit"):
        tuple(
            _walk_reachable(
                {"first": object(), "second": object()},
                max_nodes=1,
            )
        )
    with pytest.raises(AssertionError, match="depth limit"):
        tuple(_walk_reachable({"root": [[[None]]]}, max_depth=1))


def test_all_direct_builders_and_both_builtin_registries_have_no_reachable_v3_authority() -> None:
    clock = _registry_clock
    roots: dict[str, object] = {
        "direct.strategy_live": strategy_live_builder(clock=clock),
        "direct.signal_router": signal_router_builder(clock=clock),
        "direct.notifier": notifier_builder(clock=clock),
        "direct.shadow_session": shadow_session_builder(clock=clock),
        "direct.paper_consumer": paper_consumer_builder(clock=clock),
        "direct.paper_broker": paper_broker_builder(clock=clock),
        "direct.serving_publisher": serving_publisher_builder(
            snapshot_loader=None,
            clock=clock,
        ),
        "direct.daily_orchestrator": daily_pipeline_orchestrator_builder(clock=clock),
        "registry.builtin": runtime_service_builtin.build_builtin_registry(
            runtime_capabilities={},
            clock=clock,
        ),
        "registry.main": runtime_service_main.build_builtin_registry(runtime_capabilities={}),
    }

    reachable = tuple(_walk_reachable(roots))
    violations = [
        violation
        for path, value in reachable
        if (violation := _forbidden_reachable(path, value)) is not None
    ]
    assert len(reachable) >= 100
    assert violations == []
    assert {path.split(".", 1)[0] for path, _value in reachable} == {
        "direct",
        "registry",
    }


def test_decoder_and_read_models_remain_constructible_without_writer_authority() -> None:
    raw = base64.b64decode("eyJzY2hlbWFfdmVyc2lvbiI6M30=", validate=True)
    with pytest.raises(spool.SignalRouteSpoolIntegrityError):
        spool.decode_current_signal_route_spool_record(raw)
    assert inspect.isclass(spool.CurrentSignalBusRoutedRecord)
    assert inspect.isclass(spool.CurrentSignalRouteSpoolRecord)
