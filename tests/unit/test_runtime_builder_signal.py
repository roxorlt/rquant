from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxStatus
from rquant.notification_state import NotificationReplicationError, NotificationStateStore
from rquant.notification_worker import NotificationDelivery
from rquant.runtime_builder_signal import (
    build_shadow_runner_sources,
    notifier_builder,
    signal_router_builder,
)
from rquant.runtime_notification_providers import (
    NotificationTransportResult,
    build_environment_notification_provider_loader,
)
from rquant.runtime_service_builtin import build_builtin_registry
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
    load_runtime_service_manifest,
)
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityIntegrityError,
    ServingSourceAuthorityReader,
)
from rquant.runtime_serving_snapshot import SIGNALS_DATASET_ID
from rquant.runtime_shadow_validation import ShadowStrategyBinding
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_route_spool import SignalRouteSpool, publish_signal_bus_prefix
from rquant.signal_router_runtime import (
    ReadonlySignalRouteAuthority,
    RouteSourceDescriptor,
    RoutingDecision,
    RunnerSignalBatch,
    SignalRouteConflictError,
    SignalRouteCursorStore,
    SourceSnapshot,
    route_runner_signals,
)
from rquant.strategy_runner import RunnerSignalRecord, StrategyRunnerStore
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)

NOW = datetime(2026, 7, 31, 2, 30, tzinfo=UTC)
COMMIT = "a" * 40
POLICY = "b" * 64
GENERATION = "c" * 64
SPEC = "d" * 64
EVALUATOR = "2" * 64
REGISTRATION = "1" * 64


def _signal(seed: str = "e") -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="f" * 64,
        feature_snapshot_id="1" * 64,
        event_time=NOW - timedelta(seconds=1),
        available_at=NOW,
        candidate_id="600000.SH",
        action=SignalAction.WATCH,
        reason_codes=("test",),
        evidence={},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit=COMMIT,
    )


class _Source:
    def __init__(self, records: tuple[RunnerSignalRecord, ...]) -> None:
        self.records = records
        self.requests: list[tuple[int, int]] = []

    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
        self.requests.append((after_sequence, limit))
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


class _Provider:
    def __init__(self) -> None:
        self.deliveries: list[NotificationDelivery] = []

    def deliver(self, delivery: NotificationDelivery) -> str:
        self.deliveries.append(delivery)
        return f"receipt:{delivery.record.outbox_id}"


class _RecordingTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[DeliveryChannel, str]] = []

    def send(
        self,
        *,
        channel: DeliveryChannel,
        endpoint: str,
        credential: str,
        title: str,
        body: str,
    ) -> NotificationTransportResult:
        del endpoint, title, body
        self.calls.append((channel, credential))
        return NotificationTransportResult.accepted()


def _router_manifest(
    tmp_path: Path,
    **setting_overrides: object,
) -> RuntimeServiceManifest:
    settings: dict[str, object] = {
        "signal_bus_path": str(tmp_path / "signal-bus.sqlite3"),
        "signal_spool_root": str(tmp_path / "signal-spool"),
        "source_id": "n-shape-v1",
        "routing_policy_fingerprint": POLICY,
        "batch_limit": 1,
    }
    settings.update(setting_overrides)
    return RuntimeServiceManifest(
        service_id="signal-router.n-shape-v1",
        service_kind=RuntimeServiceKind.SIGNAL_ROUTER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=1,
        stale_after_seconds=10,
        producer_commit=COMMIT,
        settings=settings,
    )


def _strategy_spec() -> StrategySpec:
    return StrategySpec(
        strategy_id="n-shape",
        version=1,
        feature_contract_id="intraday-pit",
        min_feature_contract_version=1,
        required_features=(),
        optional_features=(),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="arm",
                to_state=StrategyLifecycleState.ARMED,
            ),
        ),
        parameters={},
        allowed_actions=(SignalAction.WATCH.value,),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit=COMMIT,
    )


def _frozen_policy(path: Path) -> str:
    content = json.dumps(
        {
            "default_no_target_reason": "no_matching_recipient",
            "rules": [
                {
                    "strategy_id": "n-shape",
                    "strategy_version": "1",
                    "action": "watch",
                    "recipient_id": "admin",
                    "channel": "pushdeer",
                    "enabled": True,
                }
            ],
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    path.write_bytes(content)
    timestamp = NOW.timestamp() - 1
    os.utime(path, (timestamp, timestamp))
    path.chmod(0o444)
    return hashlib.sha256(content).hexdigest()


def _authoritative_router_manifest(
    tmp_path: Path,
    *,
    signal: SignalEnvelope | None = None,
    **setting_overrides: object,
) -> tuple[RuntimeServiceManifest, StrategyRunnerStore]:
    store = StrategyRunnerStore(
        tmp_path / "runner.sqlite3",
        spec=_strategy_spec(),
        evaluator_contract_fingerprint=EVALUATOR,
    )
    if signal is not None:
        with sqlite3.connect(store.path) as connection:
            connection.execute(
                """
                    INSERT INTO runner_signal(
                        signal_id, feature_sequence, candidate_id, action,
                        entry_signal_id, candidate_occurrence_id,
                        event_time, available_at, expires_at, payload_json
                    ) VALUES (?, 0, ?, ?, NULL, NULL, ?, ?, ?, ?)
                    """,
                (
                    signal.signal_id,
                    signal.candidate_id,
                    signal.action.value,
                    signal.event_time.isoformat().replace("+00:00", "Z"),
                    signal.available_at.isoformat().replace("+00:00", "Z"),
                    signal.expires_at.isoformat().replace("+00:00", "Z"),
                    json.dumps(
                        signal.model_dump(mode="json"),
                        ensure_ascii=True,
                        allow_nan=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )
    policy_path = tmp_path / "routing-policy.json"
    policy_fingerprint = _frozen_policy(policy_path)
    authority_settings: dict[str, object] = {
        "runner_state_path": str(store.path.resolve()),
        "expected_strategy_registration_fingerprint": REGISTRATION,
        "expected_strategy_spec_fingerprint": store.spec.spec_fingerprint,
        "expected_evaluator_contract_fingerprint": EVALUATOR,
        "routing_policy_path": str(policy_path.resolve()),
        "routing_policy_fingerprint": policy_fingerprint,
    }
    authority_settings.update(setting_overrides)
    manifest = _router_manifest(tmp_path, **authority_settings)
    manifest_path = tmp_path / "signal-router-manifest.json"
    manifest_path.write_text(
        json.dumps(
            manifest.model_dump(mode="json"),
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    manifest_path.chmod(0o600)
    return load_runtime_service_manifest(manifest_path, expected_commit=COMMIT), store


def _notifier_manifest(
    tmp_path: Path,
    **setting_overrides: object,
) -> RuntimeServiceManifest:
    settings: dict[str, object] = {
        "signal_spool_root": str(tmp_path / "signal-spool"),
        "notification_state_path": str(tmp_path / "notification-state.sqlite3"),
        "worker_id": "notifier-1",
        "batch_limit": 10,
        "lease_seconds": 30,
    }
    settings.update(setting_overrides)
    return RuntimeServiceManifest(
        service_id="notifier.admin",
        service_kind=RuntimeServiceKind.NOTIFIER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=1,
        stale_after_seconds=10,
        producer_commit=COMMIT,
        settings=settings,
    )


def _route_target(_signal: SignalEnvelope) -> RoutingDecision:
    return RoutingDecision.route(
        routing_policy_fingerprint=POLICY,
        targets=(
            DeliveryTarget(
                recipient_id="admin",
                channel=DeliveryChannel.PUSHDEER,
            ),
        ),
    )


def test_authoritative_router_persists_zero_signal_drain_authority(
    tmp_path: Path,
) -> None:
    manifest, store = _authoritative_router_manifest(tmp_path)
    step = signal_router_builder(clock=lambda: NOW)(manifest)

    result = step()

    assert result.input_sequence == 0
    evidence = ReadonlySignalRouteAuthority(
        path=Path(str(manifest.settings["signal_bus_path"])),
        expected_routing_policy_fingerprint=str(manifest.settings["routing_policy_fingerprint"]),
    ).read_drain_evidence(
        source_id="n-shape-v1",
        runner_generation_id=store.source_generation_id,
        strategy_spec_fingerprint=store.spec.spec_fingerprint,
        trade_date=date(2026, 7, 31),
        segment_start_sequence=0,
        routed_through_sequence=0,
        observed_at=NOW,
    )
    assert evidence.routed_through_sequence == 0


def test_signal_builder_constructs_real_shadow_source_from_manifest_authority(
    tmp_path: Path,
) -> None:
    manifest, store = _authoritative_router_manifest(tmp_path)
    binding = ShadowStrategyBinding(
        strategy_id="n-shape",
        strategy_version=1,
        definition_fingerprint=REGISTRATION,
        executable_fingerprint=EVALUATOR,
    )

    sources = build_shadow_runner_sources(
        manifest=manifest,
        bindings={"n-shape-v1": binding},
    )

    assert sources[0][0] == binding
    batch = sources[0][1].read_batch(after_sequence=0, limit=1)
    assert batch.snapshot.descriptor.generation_id == store.source_generation_id

    with pytest.raises(ValueError, match="binding|source"):
        build_shadow_runner_sources(manifest=manifest, bindings={})

    forged = binding.model_copy(update={"definition_fingerprint": "3" * 64})
    with pytest.raises(ValueError, match="definition identity"):
        build_shadow_runner_sources(
            manifest=manifest,
            bindings={"n-shape-v1": forged},
        )


def _seed_outbox(
    tmp_path: Path,
    *,
    signal_count: int = 1,
    recipient_id: str = "admin",
) -> NotificationStateStore:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    records = tuple(
        RunnerSignalRecord(
            sequence=index,
            signal=_signal(hex(index + 13)[2:]),
        )
        for index in range(1, signal_count + 1)
    )
    route_runner_signals(
        source_id="n-shape-v1",
        source=_Source(records),
        bus=bus,
        cursors=SignalRouteCursorStore(
            tmp_path / "route-cursor.sqlite3",
            routing_policy_fingerprint=POLICY,
        ),
        routed_at=NOW,
        target_resolver=lambda _signal: RoutingDecision.route(
            routing_policy_fingerprint=POLICY,
            targets=(
                DeliveryTarget(
                    recipient_id=recipient_id,
                    channel=DeliveryChannel.PUSHDEER,
                ),
            ),
        ),
        limit=signal_count,
    )
    publish_signal_bus_prefix(
        bus=bus,
        spool=SignalRouteSpool(tmp_path / "signal-spool"),
        limit=10,
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    return store


def test_signal_router_maps_committed_cursor_and_remaining_backlog(tmp_path: Path) -> None:
    source = _Source(
        (
            RunnerSignalRecord(sequence=1, signal=_signal("2")),
            RunnerSignalRecord(sequence=2, signal=_signal("3")),
        )
    )
    loaded: list[str] = []
    step = signal_router_builder(
        source_loader=lambda source_id: (loaded.append(source_id), source)[1],
        target_resolver=_route_target,
        clock=lambda: NOW,
    )(_router_manifest(tmp_path))

    result = step()

    assert loaded == ["n-shape-v1"]
    assert result.input_sequence == 2
    assert result.output_sequence == 1
    assert result.processed_count == 1
    assert result.backlog_count == 1
    assert result.source_generations["n-shape-v1"] == GENERATION
    assert len(result.source_generations["signal_route_spool"]) == 64
    assert result.degraded_reasons == ()


def test_single_signal_router_routes_multiple_strategy_sources_with_one_bus_writer(
    tmp_path: Path,
) -> None:
    signals = {
        "n-shape-v1": _signal("4"),
        "growth-board-v1": _signal("5"),
    }

    class NamedSource:
        def __init__(self, source_id: str) -> None:
            self.source_id = source_id

        def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
            records = (
                (RunnerSignalRecord(sequence=1, signal=signals[self.source_id]),)
                if after_sequence == 0 and limit > 0
                else ()
            )
            return RunnerSignalBatch(
                snapshot=SourceSnapshot(
                    descriptor=RouteSourceDescriptor(
                        source_id=self.source_id,
                        generation_id=hashlib.sha256(self.source_id.encode()).hexdigest(),
                        strategy_spec_fingerprint=SPEC,
                        first_sequence=1,
                        high_watermark=1,
                    )
                ),
                after_sequence=after_sequence,
                limit=limit,
                records=records,
            )

    manifest = _router_manifest(
        tmp_path,
        source_id=None,
        sources=[
            {"source_id": "n-shape-v1"},
            {"source_id": "growth-board-v1"},
        ],
        batch_limit=2,
    )
    step = signal_router_builder(
        source_loader=lambda source_id: NamedSource(source_id),
        target_resolver=_route_target,
        clock=lambda: NOW,
    )(manifest)

    result = step()

    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    assert result.input_sequence == 2
    assert result.output_sequence == 2
    assert result.processed_count == 2
    assert result.backlog_count == 0
    assert len(bus.route_receipts("n-shape-v1")) == 1
    assert len(bus.route_receipts("growth-board-v1")) == 1


def test_multi_source_router_uses_one_cutoff_and_does_not_starve_later_sources(
    tmp_path: Path,
) -> None:
    source_records = {
        "n-shape-v1": (
            RunnerSignalRecord(sequence=1, signal=_signal("6")),
            RunnerSignalRecord(sequence=2, signal=_signal("7")),
        ),
        "growth-board-v1": (RunnerSignalRecord(sequence=1, signal=_signal("8")),),
    }

    class NamedSource:
        def __init__(self, source_id: str) -> None:
            self.source_id = source_id

        def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
            records = tuple(
                record
                for record in source_records[self.source_id]
                if record.sequence > after_sequence
            )[:limit]
            return RunnerSignalBatch(
                snapshot=SourceSnapshot(
                    descriptor=RouteSourceDescriptor(
                        source_id=self.source_id,
                        generation_id=hashlib.sha256(self.source_id.encode()).hexdigest(),
                        strategy_spec_fingerprint=SPEC,
                        first_sequence=1,
                        high_watermark=len(source_records[self.source_id]),
                    )
                ),
                after_sequence=after_sequence,
                limit=limit,
                records=records,
            )

    observed_times = iter((NOW, NOW + timedelta(seconds=1), NOW + timedelta(seconds=2)))
    step = signal_router_builder(
        source_loader=lambda source_id: NamedSource(source_id),
        target_resolver=_route_target,
        clock=lambda: next(observed_times),
    )(
        _router_manifest(
            tmp_path,
            source_id=None,
            sources=[
                {"source_id": "n-shape-v1"},
                {"source_id": "growth-board-v1"},
            ],
            batch_limit=1,
        )
    )

    first = step()
    second = step()

    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    n_shape = bus.route_receipts("n-shape-v1")
    growth = bus.route_receipts("growth-board-v1")
    assert first.processed_count == 1
    assert second.processed_count == 1
    assert len(n_shape) == 1
    assert len(growth) == 1
    assert n_shape[0].routed_at == NOW
    assert growth[0].routed_at == NOW + timedelta(seconds=1)


def test_signal_router_default_manifest_authorities_route_from_real_runner_store(
    tmp_path: Path,
) -> None:
    manifest, store = _authoritative_router_manifest(tmp_path, signal=_signal())
    step = build_builtin_registry(clock=lambda: NOW).build(manifest)

    result = step()

    assert result.input_sequence == 1
    assert result.output_sequence == 1
    assert result.source_generations["n-shape-v1"] == store.source_generation_id
    assert len(result.source_generations["signal_route_spool"]) == 64
    outbox = SignalBusStore(tmp_path / "signal-bus.sqlite3").outbox_records()
    assert len(outbox) == 1
    assert outbox[0].target == DeliveryTarget(
        recipient_id="admin",
        channel=DeliveryChannel.PUSHDEER,
    )


def test_signal_router_default_path_requires_complete_manifest_authority(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="authority"):
        signal_router_builder(clock=lambda: NOW)(_router_manifest(tmp_path))


def test_signal_router_rejects_mixed_manifest_and_injected_authorities(
    tmp_path: Path,
) -> None:
    manifest, _store = _authoritative_router_manifest(tmp_path)

    with pytest.raises(ValueError, match="combined|authority"):
        signal_router_builder(
            source_loader=lambda _source_id: _Source(()),
            target_resolver=_route_target,
            clock=lambda: NOW,
        )(manifest)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"runner_state_path": "relative.sqlite3"}, "absolute.*normalized"),
        ({"routing_policy_path": "relative.json"}, "absolute.*normalized"),
        ({"expected_strategy_spec_fingerprint": "3" * 64}, "strategy spec"),
        ({"routing_policy_fingerprint": "4" * 64}, "fingerprint"),
    ],
)
def test_signal_router_manifest_authority_identity_failures_are_closed(
    tmp_path: Path,
    overrides: dict[str, object],
    message: str,
) -> None:
    manifest, _store = _authoritative_router_manifest(tmp_path, **overrides)

    with pytest.raises((ValidationError, ValueError, RuntimeError), match=message):
        signal_router_builder(clock=lambda: NOW)(manifest)


def test_signal_router_pause_or_resolver_failure_never_advances_cursor(
    tmp_path: Path,
) -> None:
    source = _Source((RunnerSignalRecord(sequence=1, signal=_signal()),))
    resolver_calls: list[str] = []
    paused = signal_router_builder(
        source_loader=lambda _source_id: source,
        target_resolver=lambda signal: (
            resolver_calls.append(signal.signal_id),
            _route_target(signal),
        )[1],
        clock=lambda: NOW,
    )(_router_manifest(tmp_path, paused=True))

    paused_result = paused()

    assert resolver_calls == []
    assert paused_result.output_sequence == 0
    assert paused_result.backlog_count == 1
    assert paused_result.degraded_reasons == ("signal_router:paused",)
    assert source.requests == [(0, 0)]

    def fail(_signal: SignalEnvelope) -> RoutingDecision:
        raise RuntimeError("routing registry unavailable")

    active = signal_router_builder(
        source_loader=lambda _source_id: source,
        target_resolver=fail,
        clock=lambda: NOW,
    )(_router_manifest(tmp_path))
    with pytest.raises(RuntimeError, match="routing registry unavailable"):
        active()

    assert (
        SignalBusStore(tmp_path / "signal-bus.sqlite3").route_cursor("n-shape-v1").last_sequence
        == 0
    )


@pytest.mark.parametrize(
    ("returned_after_sequence", "returned_limit", "records"),
    [
        (1, 0, ()),
        (0, 1, (RunnerSignalRecord(sequence=1, signal=_signal()),)),
    ],
)
def test_signal_router_paused_rejects_a_mismatched_source_batch_without_effects(
    tmp_path: Path,
    returned_after_sequence: int,
    returned_limit: int,
    records: tuple[RunnerSignalRecord, ...],
) -> None:
    requests: list[tuple[int, int]] = []
    resolver_calls: list[str] = []

    class MaliciousSource:
        @staticmethod
        def read_batch(*, after_sequence: int, limit: int) -> RunnerSignalBatch:
            requests.append((after_sequence, limit))
            return RunnerSignalBatch(
                snapshot=SourceSnapshot(
                    descriptor=RouteSourceDescriptor(
                        source_id="n-shape-v1",
                        generation_id=GENERATION,
                        strategy_spec_fingerprint=SPEC,
                        first_sequence=1,
                        high_watermark=1,
                    )
                ),
                after_sequence=returned_after_sequence,
                limit=returned_limit,
                records=records,
            )

    step = signal_router_builder(
        source_loader=lambda _source_id: MaliciousSource(),
        target_resolver=lambda signal: (
            resolver_calls.append(signal.signal_id),
            _route_target(signal),
        )[1],
        clock=lambda: NOW,
    )(_router_manifest(tmp_path, paused=True))

    with pytest.raises(SignalRouteConflictError, match="batch request"):
        step()

    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    assert requests == [(0, 0)]
    assert resolver_calls == []
    assert bus.route_cursor("n-shape-v1").last_sequence == 0
    assert bus.route_receipts("n-shape-v1") == ()
    assert bus.outbox_records() == ()


def test_notifier_loads_providers_outside_manifest_and_maps_backlog(tmp_path: Path) -> None:
    state = _seed_outbox(tmp_path)
    provider = _Provider()
    loader_calls: list[bool] = []
    step = notifier_builder(
        provider_loader=lambda: (
            loader_calls.append(True),
            {DeliveryChannel.PUSHDEER: provider},
        )[1],
        clock=lambda: NOW,
    )(_notifier_manifest(tmp_path))

    result = step()

    assert loader_calls == [True]
    assert len(provider.deliveries) == 1
    assert result.input_sequence == 1
    assert result.output_sequence == 1
    assert result.processed_count == 1
    assert result.backlog_count == 0
    assert len(result.source_generations["signal_route_spool"]) == 64
    assert result.degraded_reasons == ()
    assert state.outbox_records()[0].status is OutboxStatus.SUCCEEDED


def test_notifier_default_loader_uses_scoped_environment_before_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _seed_outbox(tmp_path)
    observed: dict[str, object] = {}

    def default_loader(**kwargs: object) -> Callable[[], dict[DeliveryChannel, _Provider]]:
        observed.update(kwargs)
        return lambda: {DeliveryChannel.PUSHDEER: _Provider()}

    monkeypatch.setattr(
        "rquant.runtime_notification_providers.build_environment_notification_provider_loader",
        default_loader,
    )
    step = notifier_builder(clock=lambda: NOW)(_notifier_manifest(tmp_path))

    result = step()

    assert result.processed_count == 1
    assert state.outbox_records()[0].status is OutboxStatus.SUCCEEDED
    assert observed == {
        "pushdeer_recipient_id": "admin",
        "pushplus_recipient_id": "admin",
        "environment": None,
    }


def test_notifier_migrates_legacy_admin_outbox_to_frozen_device_recipients_once(
    tmp_path: Path,
) -> None:
    state = _seed_outbox(tmp_path)
    transport = _RecordingTransport()
    provider_loader = build_environment_notification_provider_loader(
        environment={
            "PUSHDEER_KEYS": "iphone-key,mac-key",
            "PUSHDEER_RECIPIENT_IDS": "admin.iphone,admin.mac",
        },
        transport=transport,
    )
    step = notifier_builder(
        provider_loader=provider_loader,
        clock=lambda: NOW,
    )(_notifier_manifest(tmp_path))

    first = step()
    second = step()
    records = state.outbox_records()
    migrations = state.recipient_migration_audits()

    assert first.processed_count == 2
    assert second.processed_count == 0
    assert transport.calls == [
        (DeliveryChannel.PUSHDEER, "iphone-key"),
        (DeliveryChannel.PUSHDEER, "mac-key"),
    ]
    assert tuple(record.target.recipient_id for record in records) == (
        "admin.iphone",
        "admin.mac",
    )
    assert all(record.status is OutboxStatus.SUCCEEDED for record in records)
    assert len(migrations) == 1
    assert migrations[0].outcome == "migrated"
    assert migrations[0].target_recipient_ids == ("admin.iphone", "admin.mac")


def test_notifier_preserves_succeeded_legacy_admin_without_device_redelivery(
    tmp_path: Path,
) -> None:
    state = _seed_outbox(tmp_path)
    notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(_notifier_manifest(tmp_path))()
    assert state.outbox_records()[0].status is OutboxStatus.SUCCEEDED

    transport = _RecordingTransport()
    provider_loader = build_environment_notification_provider_loader(
        environment={
            "PUSHDEER_KEYS": "iphone-key,mac-key",
            "PUSHDEER_RECIPIENT_IDS": "admin.iphone,admin.mac",
        },
        transport=transport,
    )
    step = notifier_builder(
        provider_loader=provider_loader,
        clock=lambda: NOW + timedelta(seconds=1),
    )(_notifier_manifest(tmp_path))

    result = step()

    assert result.processed_count == 0
    assert transport.calls == []
    assert state.outbox_records()[0].target.recipient_id == "admin"
    assert state.outbox_records()[0].status is OutboxStatus.SUCCEEDED
    assert state.recipient_migration_audits()[0].outcome == "preserved_succeeded"


def test_notifier_unknown_active_recipient_fails_before_claim(tmp_path: Path) -> None:
    state = _seed_outbox(tmp_path, recipient_id="unknown-user")
    transport = _RecordingTransport()
    provider_loader = build_environment_notification_provider_loader(
        environment={
            "PUSHDEER_KEYS": "iphone-key,mac-key",
            "PUSHDEER_RECIPIENT_IDS": "admin.iphone,admin.mac",
        },
        transport=transport,
    )
    step = notifier_builder(
        provider_loader=provider_loader,
        clock=lambda: NOW,
    )(_notifier_manifest(tmp_path))

    with pytest.raises(NotificationReplicationError, match="recipient is unknown"):
        step()

    record = state.outbox_records()[0]
    assert record.target.recipient_id == "unknown-user"
    assert record.status is OutboxStatus.PENDING
    assert record.attempt_count == 0
    assert state.recipient_migration_audits() == ()
    assert transport.calls == []


def test_notifier_rejects_changes_to_frozen_recipient_alias(tmp_path: Path) -> None:
    _seed_outbox(tmp_path)
    first_transport = _RecordingTransport()
    first_step = notifier_builder(
        provider_loader=build_environment_notification_provider_loader(
            environment={
                "PUSHDEER_KEYS": "iphone-key,mac-key",
                "PUSHDEER_RECIPIENT_IDS": "admin.iphone,admin.mac",
            },
            transport=first_transport,
        ),
        clock=lambda: NOW,
    )(_notifier_manifest(tmp_path))
    first_step()

    changed_transport = _RecordingTransport()
    changed_step = notifier_builder(
        provider_loader=build_environment_notification_provider_loader(
            environment={
                "PUSHDEER_KEYS": "phone-key,mac-key",
                "PUSHDEER_RECIPIENT_IDS": "admin.phone,admin.mac",
            },
            transport=changed_transport,
        ),
        clock=lambda: NOW + timedelta(seconds=1),
    )(_notifier_manifest(tmp_path))

    with pytest.raises(NotificationReplicationError, match="frozen migration"):
        changed_step()
    assert changed_transport.calls == []


def test_notifier_missing_default_capabilities_never_claims_outbox(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "PUSHDEER_KEYS",
        "PUSHPLUS_TOKENS",
        "PUSHDEER_ENDPOINT",
        "PUSHPLUS_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    state = _seed_outbox(tmp_path)
    step = notifier_builder(clock=lambda: NOW)(_notifier_manifest(tmp_path))

    with pytest.raises(RuntimeError, match="notification capability"):
        step()

    record = state.outbox_records()[0]
    assert record.status is OutboxStatus.PENDING
    assert record.attempt_count == 0


def test_notifier_pause_or_provider_loader_failure_never_claims_outbox(
    tmp_path: Path,
) -> None:
    state = _seed_outbox(tmp_path)
    loader_calls: list[bool] = []
    paused = notifier_builder(
        provider_loader=lambda: (loader_calls.append(True), {})[1],
        clock=lambda: NOW,
    )(_notifier_manifest(tmp_path, paused=True))

    paused_result = paused()

    assert loader_calls == []
    assert paused_result.backlog_count == 1
    assert paused_result.degraded_reasons == ("notifier:paused",)
    assert state.outbox_records() == ()

    def fail_loader() -> dict[DeliveryChannel, _Provider]:
        raise RuntimeError("secret store unavailable")

    active = notifier_builder(
        provider_loader=fail_loader,
        clock=lambda: NOW,
    )(_notifier_manifest(tmp_path))
    with pytest.raises(RuntimeError, match="secret store unavailable"):
        active()

    record = state.outbox_records()[0]
    assert record.status is OutboxStatus.PENDING
    assert record.attempt_count == 0


def test_notifier_publishes_owned_signal_delivery_authority_after_writeback(
    tmp_path: Path,
) -> None:
    state = _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(
        _notifier_manifest(
            tmp_path,
            serving_authority_root=str(authority_root),
            serving_history_limit=10,
        )
    )

    result = step()
    published = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
    )(NOW)

    assert result.source_generations["signals_serving_authority"] == published.generation_id
    assert published.dataset_id == SIGNALS_DATASET_ID
    assert published.sequence > 0
    assert published.status.value == "fresh"
    assert len(published.payload.signals) == 1
    assert len(published.payload.routes) == 1
    assert published.payload.deliveries[0].status is OutboxStatus.SUCCEEDED
    assert state.replication_cursor().last_global_sequence == 1


def test_notifier_builtin_refreshes_signal_page_projections_from_replica(
    tmp_path: Path,
) -> None:
    _seed_outbox(tmp_path)
    replica = (tmp_path / "rquant_ro.duckdb").resolve()
    connection = duckdb.connect(str(replica))
    try:
        connection.execute(
            """
            CREATE TABLE screen_result (
                trade_date DATE, preset_name VARCHAR, ts_code VARCHAR, name VARCHAR,
                close DOUBLE, pct_chg DOUBLE, extra JSON, created_at TIMESTAMP
            );
            INSERT INTO screen_result VALUES
              ('2026-07-31', 'n-shape-pool1', '600000.SH', 'PF', 10.6, 6, '{}',
               '2026-07-31 10:05:00');
            CREATE TABLE minute_bar (
                ts_code VARCHAR, trade_time TIMESTAMP, freq VARCHAR, open DOUBLE,
                high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE,
                source VARCHAR, created_at TIMESTAMP
            );
            INSERT INTO minute_bar VALUES
              ('600000.SH', '2026-07-31 09:30:00', '1min', 10, 10, 10, 10,
               100, 1000, 'tushare', '2026-07-31 09:31:00');
            """
        )
    finally:
        connection.close()
    surge_live_root = (tmp_path / "surge_live").resolve()
    surge_live_root.mkdir()
    (surge_live_root / "runtime_config.json").write_text(
        json.dumps(
            {
                "day": "2026-07-31",
                "boards": ["main", "gem"],
                "k_rough": 1.2,
                "k_cum": 2.5,
                "ratio_cap": 8.0,
                "skip_first_minutes": 0,
                "tushare_rate_per_min": 2,
                "require_price_strength": True,
                "max_room_to_limit_pct": 1.0,
            }
        ),
        encoding="utf-8",
    )
    source_timestamp = (NOW - timedelta(seconds=1)).timestamp()
    os.utime(
        surge_live_root / "runtime_config.json",
        (source_timestamp, source_timestamp),
    )
    authority_root = (tmp_path / "serving-signals").resolve()
    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(
        _notifier_manifest(
            tmp_path,
            serving_authority_root=str(authority_root),
            page_projection_database_path=str(replica),
            page_projection_surge_live_root=str(surge_live_root),
        )
    )

    step()
    published = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
    )(NOW)

    projections = {item.table_name: item for item in published.payload.projections}
    assert projections["screen_bounds"].rows[0]["preset_name"] == "n-shape-pool1"
    assert projections["minute_coverage"].rows[0]["source"] == "all"
    assert projections["surge_runtime_config"].rows[0]["boards_json"] == '["main","gem"]'


def test_notifier_takes_over_signals_authority_from_exact_previous_commit(
    tmp_path: Path,
) -> None:
    state = _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    old_step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(
        _notifier_manifest(
            tmp_path,
            serving_authority_root=str(authority_root),
        )
    )
    old_step()
    old_result = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
    )(NOW)

    next_commit = "9" * 40
    next_manifest = _notifier_manifest(
        tmp_path,
        paused=True,
        serving_authority_root=str(authority_root),
        serving_previous_producer_commit=COMMIT,
    ).model_copy(update={"producer_commit": next_commit})
    next_clock = NOW + timedelta(seconds=1)
    next_step = notifier_builder(
        provider_loader=lambda: {},
        clock=lambda: next_clock,
    )(next_manifest)

    first = next_step()
    second = next_step()
    next_result = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=next_commit,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
    )(next_clock)
    handoffs = state.serving_authority_handoffs()

    assert next_result.payload == old_result.payload
    assert next_result.status is old_result.status
    assert next_result.reason == old_result.reason
    assert next_result.sequence == old_result.sequence + 1
    assert next_result.generation_id != old_result.generation_id
    assert first.source_generations["signals_serving_authority"] == next_result.generation_id
    assert second.source_generations["signals_serving_authority"] == next_result.generation_id
    assert len(handoffs) == 1
    assert handoffs[0].previous_producer_commit == COMMIT
    assert handoffs[0].next_producer_commit == next_commit
    assert handoffs[0].previous_generation_id == old_result.generation_id
    assert handoffs[0].previous_sequence == old_result.sequence
    assert handoffs[0].next_sequence == next_result.sequence


def test_notifier_rejects_authority_takeover_from_unlisted_commit(tmp_path: Path) -> None:
    _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(
        _notifier_manifest(
            tmp_path,
            serving_authority_root=str(authority_root),
        )
    )()

    next_manifest = _notifier_manifest(
        tmp_path,
        paused=True,
        serving_authority_root=str(authority_root),
        serving_previous_producer_commit="8" * 40,
    ).model_copy(update={"producer_commit": "9" * 40})
    next_step = notifier_builder(
        provider_loader=lambda: {},
        clock=lambda: NOW + timedelta(seconds=1),
    )(next_manifest)

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="producer_commit"):
        next_step()


def test_notifier_paused_publishes_current_state_without_advancing_cursor(
    tmp_path: Path,
) -> None:
    state = _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    provider_calls: list[bool] = []
    step = notifier_builder(
        provider_loader=lambda: (provider_calls.append(True), {})[1],
        clock=lambda: NOW,
    )(
        _notifier_manifest(
            tmp_path,
            paused=True,
            serving_authority_root=str(authority_root),
        )
    )

    result = step()
    published = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
    )(NOW)

    assert provider_calls == []
    assert state.replication_cursor().last_global_sequence == 0
    assert published.sequence == 0
    assert published.payload.signals == ()
    assert result.degraded_reasons == ("notifier:paused",)


def test_notifier_marks_truncated_serving_history_degraded(tmp_path: Path) -> None:
    _seed_outbox(tmp_path, signal_count=2)
    authority_root = (tmp_path / "serving-signals").resolve()
    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(
        _notifier_manifest(
            tmp_path,
            serving_authority_root=str(authority_root),
            serving_history_limit=1,
        )
    )

    result = step()
    published = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
    )(NOW)

    assert published.status.value == "degraded"
    assert published.reason == "history_limit_truncated:1"
    assert len(published.payload.signals) == 1
    assert "notifier:serving_history_truncated:1" in result.degraded_reasons


def test_notifier_authority_publish_failure_fails_the_step(tmp_path: Path) -> None:
    _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    authority_root.write_text("not a directory")
    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(
        _notifier_manifest(
            tmp_path,
            serving_authority_root=str(authority_root),
        )
    )

    with pytest.raises(ServingSourceAuthorityIntegrityError):
        step()


def test_notifier_does_not_consume_routes_beyond_observed_at(tmp_path: Path) -> None:
    rollback_time = NOW - timedelta(seconds=1)
    signal = SignalEnvelope.model_validate(
        {
            **_signal().model_dump(mode="python", exclude={"signal_id"}),
            "event_time": rollback_time - timedelta(seconds=2),
            "available_at": rollback_time - timedelta(seconds=1),
        }
    )
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    route_runner_signals(
        source_id="n-shape-v1",
        source=_Source((RunnerSignalRecord(sequence=1, signal=signal),)),
        bus=bus,
        cursors=SignalRouteCursorStore(
            tmp_path / "route-cursor.sqlite3",
            routing_policy_fingerprint=POLICY,
        ),
        routed_at=rollback_time,
        target_resolver=_route_target,
        limit=10,
    )
    with sqlite3.connect(bus.path) as connection:
        connection.execute(
            "UPDATE signal_route_receipt SET routed_at = ? WHERE source_sequence = 1",
            (NOW.isoformat(timespec="microseconds").replace("+00:00", "Z"),),
        )
    publish_signal_bus_prefix(
        bus=bus,
        spool=SignalRouteSpool(tmp_path / "signal-spool"),
        limit=10,
    )
    state = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    step = notifier_builder(
        provider_loader=lambda: {},
        clock=lambda: rollback_time,
    )(_notifier_manifest(tmp_path))

    result = step()

    assert state.replication_cursor().last_global_sequence == 0
    assert state.outbox_records() == ()
    assert result.input_sequence == 1
    assert result.output_sequence == 0
    assert result.backlog_count == 1


def test_notifier_serving_authority_preserves_complete_no_target_receipt(
    tmp_path: Path,
) -> None:
    signal = _signal()
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    route_runner_signals(
        source_id="n-shape-v1",
        source=_Source((RunnerSignalRecord(sequence=1, signal=signal),)),
        bus=bus,
        cursors=SignalRouteCursorStore(
            tmp_path / "route-cursor.sqlite3",
            routing_policy_fingerprint=POLICY,
        ),
        routed_at=NOW,
        target_resolver=lambda _signal: RoutingDecision.no_target(
            routing_policy_fingerprint=POLICY,
            reason_code="recipient-opted-out",
        ),
        limit=10,
    )
    spool = SignalRouteSpool(tmp_path / "signal-spool")
    publish_signal_bus_prefix(bus=bus, spool=spool, limit=10)
    original = bus.routed_signals_after_global_sequence(
        after_sequence=0,
        through_sequence=1,
        limit=10,
    )[0].receipt
    authority_root = (tmp_path / "serving-signals").resolve()
    step = notifier_builder(
        provider_loader=lambda: {},
        clock=lambda: NOW,
    )(
        _notifier_manifest(
            tmp_path,
            serving_authority_root=str(authority_root),
        )
    )

    result = step()
    published = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
    )(NOW)

    assert result.output_sequence == 1
    assert published.payload.routes == (original,)
    assert published.payload.routes[0].reason_code == "recipient-opted-out"
    assert published.payload.routes[0].decision_fingerprint == original.decision_fingerprint
    assert published.payload.routes[0].targets == ()
    assert published.payload.deliveries == ()


@pytest.mark.parametrize(
    ("builder_name", "manifest_factory", "overrides", "message"),
    [
        ("router", _router_manifest, {"signal_bus_path": "relative.sqlite3"}, "absolute"),
        ("router", _router_manifest, {"batch_limit": True}, "integer"),
        ("router", _router_manifest, {"batch_limit": 1_001}, "less than or equal"),
        ("notifier", _notifier_manifest, {"signal_spool_root": "relative"}, "absolute"),
        (
            "notifier",
            _notifier_manifest,
            {"serving_authority_root": "relative"},
            "absolute",
        ),
        (
            "notifier",
            _notifier_manifest,
            {"serving_history_limit": 0},
            "greater than or equal",
        ),
        ("notifier", _notifier_manifest, {"batch_limit": 0}, "greater than or equal"),
        ("notifier", _notifier_manifest, {"import_path": "evil.module:provider"}, "extra"),
    ],
)
def test_signal_runtime_settings_fail_closed(
    tmp_path: Path,
    builder_name: str,
    manifest_factory: Callable[..., RuntimeServiceManifest],
    overrides: dict[str, object],
    message: str,
) -> None:
    builder = (
        signal_router_builder(
            source_loader=lambda _source_id: _Source(()),
            target_resolver=_route_target,
            clock=lambda: NOW,
        )
        if builder_name == "router"
        else notifier_builder(provider_loader=lambda: {}, clock=lambda: NOW)
    )
    with pytest.raises(ValidationError, match=message):
        builder(manifest_factory(tmp_path, **overrides))


@pytest.mark.parametrize(
    ("builder_name", "manifest_factory"),
    [("router", _router_manifest), ("notifier", _notifier_manifest)],
)
def test_signal_runtime_builders_require_live_plane_and_exact_kind(
    tmp_path: Path,
    builder_name: str,
    manifest_factory: Callable[..., RuntimeServiceManifest],
) -> None:
    builder = (
        signal_router_builder(
            source_loader=lambda _source_id: _Source(()),
            target_resolver=_route_target,
            clock=lambda: NOW,
        )
        if builder_name == "router"
        else notifier_builder(provider_loader=lambda: {}, clock=lambda: NOW)
    )
    manifest = manifest_factory(tmp_path)
    wrong_plane = RuntimeServiceManifest.model_validate(
        {**manifest.model_dump(mode="json"), "plane": "serving"}
    )
    with pytest.raises(ValueError, match="live plane"):
        builder(wrong_plane)

    other_kind = (
        RuntimeServiceKind.NOTIFIER
        if builder_name == "router"
        else RuntimeServiceKind.SIGNAL_ROUTER
    )
    wrong_kind = RuntimeServiceManifest.model_validate(
        {**manifest.model_dump(mode="json"), "service_kind": other_kind.value}
    )
    with pytest.raises(ValueError, match="kind"):
        builder(wrong_kind)
