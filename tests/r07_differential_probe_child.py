"""Isolated child harness for the R07 B01..B17 policy probes."""
# ruff: noqa: E402

from __future__ import annotations

import os

if os.environ.pop("RQUANT_R07_FAIL_DOTENV_READ", "") == "1":
    from pathlib import Path as _DotenvPath
    from typing import NoReturn as _NoReturn

    from pydantic_settings.sources import DotEnvSettingsSource as _DotEnvSettingsSource

    def _reject_dotenv_read(
        _self: _DotEnvSettingsSource,
        file_path: _DotenvPath,
    ) -> _NoReturn:
        raise AssertionError(f"isolated probe attempted dotenv read: {file_path}")

    _DotEnvSettingsSource._read_env_file = _reject_dotenv_read

_SENSITIVE_PROBE_ENVIRONMENT = frozenset(
    {
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "DEEPSEEK_API_KEY",
        "PANORAMA_COOKIE_SECRET",
        "PANORAMA_GATE_TOKEN",
        "PUSHDEER_KEYS",
        "PUSHPLUS_TOKENS",
        "RQUANT_PANORAMA_GATE_TOKEN",
        "TUSHARE_TOKEN_BACKUP",
    }
)
for _name in _SENSITIVE_PROBE_ENVIRONMENT:
    os.environ.pop(_name, None)
os.environ["RQUANT_DISABLE_DOTENV"] = "1"
import argparse
import base64
import gc
import hashlib
import json
import sqlite3
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

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
from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
from rquant.runtime_serving_snapshot import (
    SIGNALS_DATASET_ID,
    SignalDeliveryPayload,
    SignalDeliveryReadPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingSignalRecord
from rquant.signal_bus import (
    RouteDecisionKind,
    RouteSourceDescriptor,
    SignalBusRoutedRecord,
    SignalBusSourceDescriptor,
    routing_decision_fingerprint,
)
from rquant.signal_contracts import CurrentSignalEnvelope, parse_signal_envelope
from rquant.signal_family_differential_gate import (
    BoundaryProbeResultV1,
    BoundaryReachedSentinelV1,
    ConstructorIdentityFenceSentinelV1,
    ProbeSetupV1,
    R07PolicyV1,
    load_policy,
    resolve_fixture_values,
)
from rquant.signal_router_runtime import (
    RoutingDecision,
    RunnerSignalBatch,
    SignalRouteCursorStore,
    SourceSnapshot,
    route_runner_signals,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_paper_signal_dual_read_r06 import _policy as _paper_policy
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
from tests.unit.test_strategy_runner import NOW as RUNNER_NOW
from tests.unit.test_strategy_runner import _entry_decision, _envelope, _frame
from tests.unit.test_strategy_runner import _store as _runner_store

NOW = datetime(2026, 8, 16, 2, 30, tzinfo=UTC)
ROOT = Path(__file__).parents[1]


@dataclass(frozen=True)
class _Snapshot:
    logical: object
    exact: object

    @property
    def digest(self) -> str:
        return hashlib.sha256(canonical_json_bytes(_canonicalize(self.logical))).hexdigest()


@dataclass
class _Harness:
    policy: R07PolicyV1
    tmp_path: Path
    monkeypatch: pytest.MonkeyPatch
    actual: dict[str, object] = field(default_factory=dict)
    bindings: dict[str, object] = field(default_factory=dict)
    mutation_counts: dict[str, int] = field(default_factory=dict)
    setup_call_counts: dict[str, int] = field(default_factory=dict)
    sentinel_count: int = 0
    observed_identity: str = ""
    snapshotter: Callable[[], _Snapshot] | None = None
    entrypoint: Callable[..., object] | None = None

    def mutation_guard(self, owner: object, attribute: str, guard_id: str) -> None:
        self.mutation_counts[guard_id] = 0

        def forbidden(*_args: object, **_kwargs: object) -> object:
            self.mutation_counts[guard_id] += 1
            raise AssertionError(f"mutation guard reached: {guard_id}")

        self.monkeypatch.setattr(owner, attribute, forbidden)

    def record_setup_call(self, target: str) -> None:
        self.setup_call_counts[target] = self.setup_call_counts.get(target, 0) + 1


def _canonicalize(value: object) -> object:
    if isinstance(value, BaseModel):
        return {
            "model": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": value.model_dump(mode="json"),
        }
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest(), "size": len(value)}
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_canonicalize(item) for item in value]
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _canonicalize(item) for key, item in sorted(value.items())}
    if value is None or type(value) in (str, bool, int, float):
        return value
    return {"type": f"{type(value).__module__}.{type(value).__qualname__}"}


def _tree_snapshot(root: Path) -> _Snapshot:
    if not root.exists():
        return _Snapshot((), ())
    exact = tuple(
        (
            "directory" if path.is_dir() else "file",
            path.relative_to(root).as_posix(),
            b"" if path.is_dir() else path.read_bytes(),
        )
        for path in sorted(root.rglob("*"))
    )
    logical = tuple((kind, relative, len(payload)) for kind, relative, payload in exact)
    return _Snapshot(logical, exact)


def _sqlite_snapshot(path: Path) -> _Snapshot:
    gc.collect()
    database_bytes = _database_bytes_snapshot(path)
    with TemporaryDirectory(prefix="rquant-r07-probe-snapshot-") as directory:
        snapshot_root = Path(directory)
        snapshot_path = snapshot_root / path.name
        for name, payload in database_bytes:
            if not name.endswith("-shm"):
                (snapshot_root / name).write_bytes(payload)
        rows = _database_snapshot(snapshot_path)
    logical_rows = []
    for table, table_rows in rows:
        if table == "signal_bus_metadata":
            normalized = tuple(
                (key, "<setup-generated>" if key == "source_generation_id" else value)
                for key, value in table_rows
            )
        elif table == "runner_source_identity":
            normalized = tuple((row[0], "<setup-generated>") for row in table_rows)
        else:
            normalized = table_rows
        logical_rows.append((table, normalized))
    return _Snapshot(tuple(logical_rows), (rows, database_bytes))


def _prime_sqlite_readonly_sidecars(path: Path) -> None:
    gc.collect()
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, isolation_level=None)
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


def _daily_stage(tmp_path: Path) -> DailySummaryStage:
    return DailySummaryStage(
        signal_bus=_store(tmp_path / "daily-bus.sqlite3"),
        strategy_version="daily-close-dag/v1",
        producer_commit="a" * 40,
        clock=lambda: NOW,
        artifact_store=DailyDownstreamArtifactStore(tmp_path / "daily-artifacts"),
        canonical_reader_factory=lambda: DuckDBStore(tmp_path / "unused.duckdb", read_only=True),
    )


def _current_serving_snapshot(current: CurrentSignalEnvelope) -> NotificationServingSnapshot:
    return NotificationServingSnapshot(
        observed_at=NOW,
        sequence=1,
        visible_signal_count=1,
        returned_signal_count=1,
        omitted_signal_count=0,
        truncated=False,
        payload=SignalDeliveryReadPayload(
            signals=(ServingSignalRecord(global_sequence=1, signal=current),)
        ),
    )


def _current_source_result(snapshot: NotificationServingSnapshot) -> SourceReadResult:
    provisional = SourceReadResult(
        dataset_id=SIGNALS_DATASET_ID,
        generation_id="0" * 64,
        sequence=1,
        event_time=NOW,
        published_at=NOW,
        status=FreshnessStatus.FRESH,
        payload=snapshot.payload,
    )
    values = provisional.model_dump(mode="python", exclude={"generation_id"})
    return SourceReadResult.model_validate({**values, "generation_id": canonical_sha256(values)})


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


def _install_current_guard(harness: _Harness, module: object) -> None:
    original = module.require_legacy_signal_write  # type: ignore[attr-defined]

    def guarded(signal: object, *, operation: str) -> object:
        if type(signal) is CurrentSignalEnvelope:
            harness.sentinel_count += 1
        return original(signal, operation=operation)

    harness.monkeypatch.setattr(module, "require_legacy_signal_write", guarded)


def _fixture(harness: _Harness, fixture_id: str) -> object:
    if fixture_id in harness.actual:
        return harness.actual[fixture_id]
    fixtures = {item.fixture_id: item for item in harness.policy.fixtures}
    current = {item.fixture_id: item for item in harness.policy.current_fixtures}
    if fixture_id in current:
        declared = current[fixture_id]
        raw = base64.b64decode(declared.canonical_model_bytes, validate=True)
        if declared.allowed_form == "stored_bytes":
            return raw
        parsed = parse_signal_envelope(raw)
        if type(parsed) is not CurrentSignalEnvelope:
            raise AssertionError("current fixture parser returned the wrong exact model")
        return parsed
    fixture = fixtures[fixture_id]
    if fixture.kind in ("tuple", "list"):
        children = [_fixture(harness, child_id) for child_id in fixture.value]
        return tuple(children) if fixture.kind == "tuple" else children
    return resolve_fixture_values(harness.policy.fixtures)[fixture_id]


def _binding_descriptor(value: object) -> object:
    if isinstance(value, BaseModel):
        return _canonicalize(value)
    if isinstance(value, bytes):
        return _canonicalize(value)
    if isinstance(value, (tuple, list)):
        return [_binding_descriptor(item) for item in value]
    if callable(value):
        module = getattr(value, "__module__", "")
        if module == "__main__":
            module = "tests.r07_differential_probe_runner"
        qualname = getattr(value, "__qualname__", type(value).__qualname__)
        return {"callable": f"{module}.{qualname}"}
    if value is None or type(value) in (str, bool, int, float):
        return _canonicalize(value)
    value_type = type(value)
    module = value_type.__module__
    if module == "__main__":
        module = "tests.r07_differential_probe_runner"
    return {"type": f"{module}.{value_type.__qualname__}"}


def _setup_result_digest(harness: _Harness, setup: ProbeSetupV1) -> str:
    observations = []
    for step in setup.steps:
        if step.expected_binding not in harness.bindings:
            raise AssertionError(f"setup binding missing: {step.expected_binding}")
        observations.append(
            {
                "kind": step.kind,
                "target": step.target,
                "fixture_ids": list(step.fixture_ids),
                "expected_binding": step.expected_binding,
                "observed": _binding_descriptor(harness.bindings[step.expected_binding]),
            }
        )
    return hashlib.sha256(
        canonical_json_bytes({"setup_id": setup.setup_id, "observations": observations})
    ).hexdigest()


def _bind_common_fixtures(harness: _Harness) -> CurrentSignalEnvelope:
    object_fixture = next(
        item for item in harness.policy.current_fixtures if item.fixture_id == "current.object"
    )
    raw = base64.b64decode(object_fixture.canonical_model_bytes, validate=True)
    current = parse_signal_envelope(raw)
    if type(current) is not CurrentSignalEnvelope:
        raise AssertionError("current.object is not the exact declared current model")
    harness.actual.update(
        {
            "current.object": current,
            "current.stored-bytes": raw,
            "time.now": NOW,
            "time.runner": RUNNER_NOW,
            "date.trade": date(2026, 8, 16),
            "duration.seven-days": timedelta(days=7),
            "callable.entry-decision": _entry_decision,
            "callable.target-resolver": lambda _signal: RoutingDecision.no_target(
                routing_policy_fingerprint=POLICY,
                reason_code="r07_no_target",
            ),
            "arg.delivery-targets": (
                DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),
            ),
            "arg.source-descriptor": _source_descriptor(),
            "arg.decision-kind": RouteDecisionKind.NO_TARGET,
            "arg.decision-fingerprint": _decision_fingerprint(),
            "arg.bus-source-descriptor": SignalBusSourceDescriptor(
                generation_id="f" * 64,
                high_watermark=1,
            ),
        }
    )
    return current


def _constructor_replacement(
    harness: _Harness,
    module: object,
    target: str,
) -> Callable[..., object]:
    current = _fixture(harness, "current.object")

    def replacement(**_values: object) -> object:
        harness.record_setup_call(target)
        return current

    harness.setup_call_counts[target] = 0
    harness.monkeypatch.setattr(module, "SignalEnvelope", replacement)
    return replacement


def _configure_probe(harness: _Harness, inventory_id: str) -> None:
    current = _bind_common_fixtures(harness)

    def tree_snapshot() -> _Snapshot:
        return _tree_snapshot(harness.tmp_path)

    if inventory_id == "R07-B01":
        path = harness.tmp_path / "runner.sqlite3"
        store = _runner_store(path)
        harness.actual.update(
            {
                "receiver.strategy-store": store,
                "arg.runner-envelope": _envelope(),
                "arg.runner-frame": _frame(),
            }
        )
        replacement = _constructor_replacement(
            harness,
            strategy_runner,
            "rquant.strategy_runner.SignalEnvelope",
        )
        original_fence = strategy_runner._legacy_signal_constructor_identity_matches

        def identity_fence(candidate: object, expected: object) -> bool:
            if candidate is replacement:
                harness.sentinel_count += 1
                harness.observed_identity = str(id(candidate))
            return original_fence(candidate, expected)

        harness.monkeypatch.setattr(
            strategy_runner,
            "_legacy_signal_constructor_identity_matches",
            identity_fence,
        )
        harness.mutation_guard(store, "_connect", "StrategyRunnerStore._connect")
        harness.bindings.update(
            {
                "receiver.strategy-store": store,
                "constructor.strategy.SignalEnvelope": replacement,
            }
        )
        harness.entrypoint = store.process_batch
        harness.snapshotter = lambda: _sqlite_snapshot(path)
        return

    if inventory_id in {"R07-B02", "R07-B03"}:
        stage = _daily_stage(harness.tmp_path)
        replacement = _constructor_replacement(
            harness,
            daily_summary,
            "rquant.daily_summary_stage.SignalEnvelope",
        )
        _install_current_guard(harness, daily_summary)
        harness.mutation_guard(
            stage._notification_producer._signal_bus,
            "_write_transaction",
            "SignalBusStore._write_transaction",
        )
        harness.actual["receiver.daily-stage"] = stage
        harness.bindings.update(
            {
                "receiver.daily-stage": stage,
                "constructor.daily_summary.SignalEnvelope": replacement,
            }
        )
        if inventory_id == "R07-B02":
            harness.actual["arg.screen-hits"] = {"n-shape-pool1": 1}
            harness.entrypoint = stage.build_signal
        else:
            canonical = SimpleNamespace(
                generation_id="b" * 64,
                receipt_id="c" * 64,
                db_content_sha256="d" * 64,
                trade_date=date(2026, 8, 16),
            )
            harness.actual["arg.daily-canonical"] = canonical
            harness.entrypoint = stage._error_signals
        harness.snapshotter = tree_snapshot
        return

    if inventory_id == "R07-B04":
        replacement = _constructor_replacement(
            harness,
            daily_notification,
            "rquant.daily_notification_producer.SignalEnvelope",
        )
        _install_current_guard(harness, daily_notification)
        stage = _daily_stage(harness.tmp_path)
        harness.mutation_guard(
            stage._notification_producer._signal_bus,
            "_write_transaction",
            "SignalBusStore._write_transaction",
        )
        harness.actual["arg.runtime-error"] = RuntimeError("private detail")
        harness.bindings["constructor.daily_notification.SignalEnvelope"] = replacement
        harness.entrypoint = daily_notification.build_daily_error_signal
        harness.snapshotter = tree_snapshot
        return

    if inventory_id == "R07-B05":

        class Bus:
            def ingest(self, *_args: object, **_kwargs: object) -> object:
                raise AssertionError("unpatched bus ingest")

        bus = Bus()
        producer = DailyNotificationProducer(signal_bus=bus)  # type: ignore[arg-type]
        _install_current_guard(harness, daily_notification)
        harness.mutation_guard(bus, "ingest", "DailyNotificationProducer.signal_bus.ingest")
        harness.actual["receiver.daily-producer"] = producer
        harness.bindings["receiver.daily-producer"] = producer
        harness.entrypoint = producer.emit
        harness.snapshotter = tree_snapshot
        return

    if inventory_id in {"R07-B06", "R07-B07", "R07-B08"}:
        path = harness.tmp_path / f"{inventory_id.lower()}.sqlite3"
        store = _store(path)
        harness.actual["receiver.signal-bus"] = store
        harness.bindings["receiver.signal-bus"] = store
        if inventory_id == "R07-B07":
            if current.signal_id is None:
                raise AssertionError("current fixture is missing signal_id")
            raw = _fixture(harness, "current.stored-bytes")
            if not isinstance(raw, bytes):
                raise AssertionError("stored current fixture is not bytes")
            _insert_literal_signal_rows(
                path,
                (("current", CurrentSignalEnvelope, current.signal_id, raw),),
                routed=False,
            )
            _prime_sqlite_readonly_sidecars(path)
            harness.actual["arg.current-signal-id"] = current.signal_id
            harness.bindings["row.signal-bus.current"] = current.signal_id
            harness.entrypoint = store.route
        elif inventory_id == "R07-B06":
            harness.entrypoint = store.ingest
        else:
            harness.entrypoint = store.commit_source_route
        _install_current_guard(harness, signal_bus)
        harness.mutation_guard(store, "_write_transaction", "SignalBusStore._write_transaction")
        harness.snapshotter = lambda: _sqlite_snapshot(path)
        return

    if inventory_id == "R07-B09":
        bus_path = harness.tmp_path / "router.sqlite3"
        cursor_path = harness.tmp_path / "cursor.sqlite3"
        bus = _store(bus_path)

        class Source:
            def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
                if (after_sequence, limit) != (0, 1):
                    raise AssertionError("source.read_batch call shape drift")
                harness.record_setup_call("source.read_batch")
                return RunnerSignalBatch(
                    snapshot=SourceSnapshot(descriptor=_source_descriptor()),
                    after_sequence=after_sequence,
                    limit=limit,
                    records=({"sequence": 1, "signal": current},),
                )

        source = Source()
        cursors = SignalRouteCursorStore(cursor_path, routing_policy_fingerprint=POLICY)
        harness.setup_call_counts["source.read_batch"] = 0
        harness.actual.update(
            {
                "receiver.router-source": source,
                "receiver.signal-bus": bus,
                "receiver.route-cursors": cursors,
            }
        )
        harness.bindings.update(
            {
                "receiver.router-source": source,
                "receiver.signal-bus": bus,
                "receiver.route-cursors": cursors,
                "source.read_batch": source.read_batch,
            }
        )
        _install_current_guard(harness, signal_router_runtime)
        harness.mutation_guard(cursors, "bind", "SignalRouteCursorStore.bind")
        harness.mutation_guard(bus, "bind_route_source", "SignalBusStore.bind_route_source")
        harness.mutation_guard(bus, "commit_source_route", "SignalBusStore.commit_source_route")
        _prime_sqlite_readonly_sidecars(bus_path)
        harness.entrypoint = route_runner_signals

        def snapshot() -> _Snapshot:
            database = _sqlite_snapshot(bus_path)
            tree = _tree_snapshot(harness.tmp_path)
            return _Snapshot((database.logical, tree.logical), (database.exact, tree.exact))

        harness.snapshotter = snapshot
        return

    if inventory_id in {"R07-B10", "R07-B11"}:
        root = harness.tmp_path / "spool"
        route_spool = spool.SignalRouteSpool(root)
        current_record = _routed_record(
            base64.b64decode(
                next(
                    item.canonical_model_bytes
                    for item in harness.policy.current_fixtures
                    if item.fixture_id == "current.object"
                ),
                validate=True,
            )
        )
        harness.actual.update(
            {
                "receiver.route-spool": route_spool,
                "record.current-routed": current_record,
            }
        )
        original_preflight = spool._require_legacy_spool_publish_input

        def preflight(**kwargs: object) -> None:
            records = kwargs.get("records")
            if isinstance(records, tuple) and any(
                type(record.signal) is CurrentSignalEnvelope for record in records
            ):
                harness.sentinel_count += 1
            original_preflight(**kwargs)  # type: ignore[arg-type]

        harness.monkeypatch.setattr(spool, "_require_legacy_spool_publish_input", preflight)
        harness.bindings["receiver.route-spool"] = route_spool
        if inventory_id == "R07-B10":
            harness.bindings["batch.current-routed"] = (current_record,)
            harness.mutation_guard(
                spool,
                "_open_root_directory",
                "signal_route_spool._open_root_directory",
            )
            harness.entrypoint = route_spool.publish
        else:
            source_descriptor = _fixture(harness, "arg.bus-source-descriptor")

            class Bus:
                @staticmethod
                def source_descriptor() -> object:
                    return source_descriptor

                @staticmethod
                def routed_signals_after_global_sequence(
                    *, after_sequence: int, through_sequence: int, limit: int
                ) -> tuple[SignalBusRoutedRecord, ...]:
                    if (after_sequence, through_sequence, limit) != (0, 1, 1):
                        raise AssertionError("bus routed-result call shape drift")
                    harness.record_setup_call("bus.routed_signals_after_global_sequence")
                    return (current_record,)

            bus = Bus()
            harness.setup_call_counts["bus.routed_signals_after_global_sequence"] = 0
            harness.actual["receiver.prefix-bus"] = bus
            harness.bindings.update(
                {
                    "receiver.prefix-bus": bus,
                    "bus.routed_signals_after_global_sequence": (
                        bus.routed_signals_after_global_sequence
                    ),
                }
            )
            harness.mutation_guard(route_spool, "publish", "SignalRouteSpool.publish")
            harness.entrypoint = spool.publish_signal_bus_prefix
        harness.snapshotter = lambda: _tree_snapshot(root)
        return

    if inventory_id == "R07-B12":
        path = harness.tmp_path / "notification.sqlite3"
        store = NotificationStateStore(path)
        current_record = _routed_record(
            base64.b64decode(
                next(
                    item.canonical_model_bytes
                    for item in harness.policy.current_fixtures
                    if item.fixture_id == "current.object"
                ),
                validate=True,
            )
        )
        harness.actual.update(
            {
                "receiver.notification-store": store,
                "record.current-routed": current_record,
            }
        )
        harness.bindings.update(
            {
                "receiver.notification-store": store,
                "batch.current-routed": (current_record,),
            }
        )
        _install_current_guard(harness, notification_state)
        harness.mutation_guard(
            store,
            "_write_transaction",
            "NotificationStateStore._write_transaction",
        )
        harness.entrypoint = store.replicate
        harness.snapshotter = lambda: _sqlite_snapshot(path)
        return

    if inventory_id in {"R07-B13", "R07-B14"}:
        path = harness.tmp_path / f"{inventory_id.lower()}.sqlite3"
        queue = PaperSignalQueueStore(path, policy=_paper_policy())
        harness.actual["receiver.paper-queue"] = queue
        harness.bindings["receiver.paper-queue"] = queue
        _install_current_guard(harness, paper_signal_worker)
        harness.mutation_guard(queue, "_connect", "PaperSignalQueueStore._connect")
        harness.entrypoint = queue.ingest
        harness.snapshotter = lambda: _sqlite_snapshot(path)
        return

    if inventory_id == "R07-B15":
        serving_record = ServingSignalRecord(global_sequence=1, signal=current)
        harness.actual["record.serving-current"] = serving_record
        harness.bindings["batch.serving-current"] = (serving_record,)
        _install_current_guard(harness, runtime_serving_snapshot)
        harness.entrypoint = SignalDeliveryPayload
        harness.snapshotter = tree_snapshot
        return

    if inventory_id == "R07-B16":
        snapshot_value = _current_serving_snapshot(current)

        class Store:
            @staticmethod
            def serving_snapshot(**kwargs: object) -> NotificationServingSnapshot:
                if kwargs != {"observed_at": NOW, "history_limit": 10}:
                    raise AssertionError("store.serving_snapshot call shape drift")
                harness.record_setup_call("store.serving_snapshot")
                return snapshot_value

        class Publisher:
            producer_commit = "a" * 40

            @staticmethod
            def publish(_result: SourceReadResult) -> object:
                raise AssertionError("unpatched publisher")

        class Reader:
            expected_producer_commit = "a" * 40

            @staticmethod
            def __call__(_observed_at: datetime) -> SourceReadResult:
                raise AssertionError("unpatched reader")

        store = Store()
        publisher = Publisher()
        reader = Reader()
        harness.setup_call_counts["store.serving_snapshot"] = 0
        harness.actual.update(
            {
                "receiver.authority-store": store,
                "receiver.authority-publisher": publisher,
                "receiver.authority-reader": reader,
            }
        )
        harness.bindings.update(
            {
                "receiver.authority-store": store,
                "receiver.authority-publisher": publisher,
                "receiver.authority-reader": reader,
                "store.serving_snapshot": store.serving_snapshot,
            }
        )
        _install_current_guard(harness, runtime_serving_snapshot)
        harness.mutation_guard(publisher, "publish", "authority.publisher.publish")
        harness.mutation_guard(Reader, "__call__", "authority.reader.__call__")
        harness.entrypoint = runtime_builder_signal._publish_signal_authority

        def snapshot() -> _Snapshot:
            value = _serving_snapshot_value(snapshot_value)
            tree = _tree_snapshot(harness.tmp_path)
            return _Snapshot((value, tree.logical), (value, tree.exact))

        harness.snapshotter = snapshot
        return

    if inventory_id == "R07-B17":
        root = harness.tmp_path / "authority"
        publisher = ServingSourceAuthorityPublisher(
            root=root,
            producer_commit="a" * 40,
            dataset_id=SIGNALS_DATASET_ID,
            payload_kind="signal_delivery",
            clock=lambda: NOW,
        )
        snapshot_value = _current_serving_snapshot(current)
        source_result = _current_source_result(snapshot_value)
        harness.actual.update(
            {
                "receiver.serving-publisher": publisher,
                "arg.current-source-result": source_result,
            }
        )
        harness.bindings["receiver.serving-publisher"] = publisher
        _install_current_guard(harness, signal_bus)
        harness.mutation_guard(
            runtime_serving_authority,
            "_open_or_create_root",
            "runtime_serving_authority._open_or_create_root",
        )
        harness.entrypoint = publisher.publish
        harness.snapshotter = lambda: _tree_snapshot(harness.tmp_path)
        return

    raise AssertionError(f"unsupported dynamic boundary: {inventory_id}")


def _setup_calls_pass(probe_variant: str, counts: dict[str, int]) -> bool:
    for target, count in counts.items():
        if probe_variant == "constructor_identity" and target.endswith("SignalEnvelope"):
            if count != 0:
                return False
        elif count != 1:
            return False
    return True


def run_boundary_probe(
    *,
    policy: R07PolicyV1,
    inventory_id: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    enforce_policy_digests: bool = True,
) -> BoundaryProbeResultV1:
    probe = next(item for item in policy.boundary_probes if item.inventory_id == inventory_id)
    if probe.variant == "static_only":
        raise ValueError("the dynamic runner accepts only R07-B01 through R07-B17")
    setup = next(item for item in policy.probe_setups if item.setup_id == probe.setup_id)
    harness = _Harness(policy=policy, tmp_path=tmp_path, monkeypatch=monkeypatch)
    _configure_probe(harness, inventory_id)
    if harness.entrypoint is None or harness.snapshotter is None:
        raise AssertionError("probe setup did not bind entrypoint and snapshotter")
    if tuple(harness.mutation_counts) != probe.mutation_expectation.guard_ids:
        raise AssertionError("installed mutation guards do not match policy order")

    setup_digest = _setup_result_digest(harness, setup)
    before = harness.snapshotter()
    exception: BaseException | None = None
    exception_phase = "invocation"
    yielded_count = 0
    sentinel_after_invocation = 0
    sentinel_after_consumption = 0
    try:
        args = tuple(_fixture(harness, fixture_id) for fixture_id in probe.positional_fixture_ids)
        kwargs = {
            name: _fixture(harness, fixture_id)
            for name, fixture_id in probe.keyword_fixture_ids.items()
        }
        result = harness.entrypoint(*args, **kwargs)
        sentinel_after_invocation = harness.sentinel_count
        if probe.call_result_action == "consume_tuple":
            exception_phase = "consumption"

            class CountingIterator:
                def __init__(self, source: object) -> None:
                    self._iterator = iter(source)  # type: ignore[arg-type]

                def __iter__(self) -> CountingIterator:
                    return self

                def __next__(self) -> object:
                    nonlocal yielded_count
                    item = next(self._iterator)
                    yielded_count += 1
                    return item

            tuple(CountingIterator(result))
        sentinel_after_consumption = harness.sentinel_count
    except BaseException as exc:  # evidence must record unexpected categories too
        exception = exc
        if exception_phase == "invocation":
            sentinel_after_invocation = harness.sentinel_count
        sentinel_after_consumption = harness.sentinel_count
    after = harness.snapshotter()

    if probe.sentinel_kind == "constructor_identity_fence":
        replacement = harness.bindings["constructor.strategy.SignalEnvelope"]
        sentinel_passed = ConstructorIdentityFenceSentinelV1(
            sentinel_id=probe.sentinel_id,
            inventory_id=probe.inventory_id,
            replaced_global="rquant.strategy_runner.SignalEnvelope",
            expected_replacement_identity=str(id(replacement)),
            observed_identity=harness.observed_identity,
            reached_count=harness.sentinel_count,
        ).passed
    else:
        sentinel_passed = BoundaryReachedSentinelV1(
            sentinel_id=probe.sentinel_id,
            inventory_id=probe.inventory_id,
            source_span=probe.source_span,
            ast_digest=probe.boundary_ast_sha256,
            reached_count=harness.sentinel_count,
            mutation_reached_count=sum(harness.mutation_counts.values()),
        ).passed

    call_shape_digest = hashlib.sha256(
        canonical_json_bytes(probe.call_shape.model_dump(mode="json"))
    ).hexdigest()
    digest_matches = (
        setup_digest == setup.setup_result_digest
        and before.digest == probe.before_snapshot_digest
        and after.digest == probe.after_snapshot_digest
    )
    if not enforce_policy_digests:
        digest_matches = True
    passed = all(
        (
            exception is not None,
            type(exception).__name__ == probe.expected_exception if exception else False,
            exception_phase == probe.expected_exception_phase,
            sentinel_passed,
            before.exact == after.exact,
            before.digest == after.digest,
            yielded_count == probe.expected_yielded_count,
            all(count == 0 for count in harness.mutation_counts.values()),
            _setup_calls_pass(probe.variant, harness.setup_call_counts),
            digest_matches,
            probe.call_shape.positional_fixture_ids == probe.positional_fixture_ids,
            probe.call_shape.keyword_fixture_ids == probe.keyword_fixture_ids,
            probe.call_shape.call_result_action == probe.call_result_action,
        )
    )
    return BoundaryProbeResultV1.with_digest(
        probe_id=probe.probe_id,
        inventory_id=probe.inventory_id,
        setup_id=setup.setup_id,
        setup_result_digest=setup_digest,
        call_shape_digest=call_shape_digest,
        exception_type=type(exception).__name__ if exception else "none",
        exception_phase=exception_phase,
        sentinel_id=probe.sentinel_id,
        sentinel_kind=probe.sentinel_kind,
        sentinel_after_invocation=sentinel_after_invocation,
        sentinel_after_consumption=sentinel_after_consumption,
        reached_count=harness.sentinel_count,
        mutation_guard_counts=harness.mutation_counts,
        setup_call_counts=harness.setup_call_counts,
        yielded_count=yielded_count,
        before_snapshot_digest=before.digest,
        after_snapshot_digest=after.digest,
        passed=passed,
    )


def _parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-id", required=True)
    parser.add_argument("--tmp-path", type=Path, required=True)
    parser.add_argument("--policy-path", type=Path, required=True)
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    args = _parse_args(arguments)
    policy = load_policy(args.policy_path)
    monkeypatch = pytest.MonkeyPatch()
    try:
        result = run_boundary_probe(
            policy=policy,
            inventory_id=args.inventory_id,
            tmp_path=args.tmp_path,
            monkeypatch=monkeypatch,
        )
    finally:
        monkeypatch.undo()
    sys.stdout.write(json.dumps(result.model_dump(mode="json"), sort_keys=True))
    return int(not result.passed)


if __name__ == "__main__":
    raise SystemExit(main())
