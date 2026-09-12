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
from rquant.runtime_peer_artifacts import PeerArtifactUnavailableError
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
from rquant.serving_page_projection_source import DuckDBSignalPageProjectionSource
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


def _page_projection_replica(tmp_path: Path, *, synced_at: datetime) -> Path:
    """The five-minute replica the notifier's page projection reads, with a sane mtime."""

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
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    stamp = synced_at.timestamp()
    os.utime(replica, (stamp, stamp))
    return replica


def test_notifier_reports_what_each_iteration_did_with_the_replica(
    tmp_path: Path,
) -> None:
    """#256: the first iteration opens the replica, the second recognises the generation."""

    _seed_outbox(tmp_path)
    replica = _page_projection_replica(tmp_path, synced_at=NOW - timedelta(minutes=1))
    authority_root = (tmp_path / "serving-signals").resolve()
    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(
        _notifier_manifest(
            tmp_path,
            serving_authority_root=str(authority_root),
            page_projection_database_path=str(replica),
        )
    )

    first = step()
    second = step()

    assert first.replica_opened is True
    assert (second.replica_opened, second.replica_read_bytes) == (False, 0)


def test_the_notifier_begins_each_iteration_s_replica_accounting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review MF-5: a wiring assertion, and the docstring says why it has to be one.

    Every return path of this step publishes the page projection -- both branches guard on
    the serving authority, and `NotifierSettings` refuses a projection database without a
    serving authority root -- so this role asks the gate on *every* iteration, and removing
    `begin_replica_iteration()` cannot currently be observed through the result. That makes
    the call defensive rather than load-bearing, which is exactly why it needs a guard of
    its own: the day someone adds an early return above the publish, the summary would
    silently start reporting the previous iteration's read, and MF-1 would be back for this
    role only.
    """

    _seed_outbox(tmp_path)
    replica = _page_projection_replica(tmp_path, synced_at=NOW - timedelta(minutes=1))
    authority_root = (tmp_path / "serving-signals").resolve()
    begun: list[int] = []
    original = DuckDBSignalPageProjectionSource.begin_replica_iteration

    def counted(self: DuckDBSignalPageProjectionSource) -> None:
        begun.append(1)
        original(self)

    monkeypatch.setattr(
        DuckDBSignalPageProjectionSource, "begin_replica_iteration", counted
    )
    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(
        _notifier_manifest(
            tmp_path,
            serving_authority_root=str(authority_root),
            page_projection_database_path=str(replica),
        )
    )

    step()
    step()

    assert len(begun) == 2


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


# ---------------------------------------------------------------------------------------
# #260: the signals pointer this role's own previous generation left on disk
# ---------------------------------------------------------------------------------------


#: the service id `_bundle_inputs` installs a notifier under, so the generation tree the
#: installer writes carries a manifest keyed by exactly this name
INSTALLED_NOTIFIER_SERVICE = "notifier-admin"
SECOND_COMMIT = "b" * 40


@pytest.fixture
def sealed_bundle_installs(monkeypatch: pytest.MonkeyPatch) -> None:
    """The installer's credential sealing, stubbed the way its own module stubs it.

    Requested rather than imported: `isolated_root_credential_sealer` is autouse in its
    home module and importing it would make it autouse for this whole file.
    """

    from tests.unit.test_runtime_deployment_bundle import (
        _CredentialRecoveryStub,
        _CredentialTransactionStub,
    )

    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._recover_runtime_credentials",
        lambda **_kwargs: _CredentialRecoveryStub(),
        raising=False,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda credentials: _CredentialTransactionStub(dict(credentials)),
    )


@pytest.fixture
def two_notifier_generations(tmp_path: Path, sealed_bundle_installs: None) -> Path:
    """A runtime root the installer really wrote twice, `a…` then `b…`.

    The lineage reads the generation tree, not a fixture: each directory is named by
    `canonical_sha256` of its own basis and the basis records the sha256 of the manifest it
    installed for this service, so what these tests exercise is the same evidence the host
    has after a release.
    """

    from tests.unit.test_runtime_deployment_bundle import (
        _bundle_inputs,
        install_runtime_deployment_bundle,
    )

    root = tmp_path / "runtime"
    for commit in (COMMIT, SECOND_COMMIT):
        manifests, capabilities = _bundle_inputs(root)
        install_runtime_deployment_bundle(
            root,
            producer_commit=commit,
            manifests=tuple(
                manifest.model_copy(update={"producer_commit": commit})
                for manifest in manifests
            ),
            capability_env=capabilities,
        )
    return root


def _installed_notifier_manifest(tmp_path: Path, **overrides: object) -> RuntimeServiceManifest:
    """This generation's notifier manifest, under the service id the bundle installed."""

    return _notifier_manifest(tmp_path, **overrides).model_copy(
        update={
            "service_id": INSTALLED_NOTIFIER_SERVICE,
            "producer_commit": SECOND_COMMIT,
        }
    )


def _publish_previous_generation_pointer(authority_root: Path) -> str:
    """`current.json` as the previous generation's notifier left it -- written by that role.

    Running the builder under the older commit is what makes this the real thing rather
    than a hand-rolled pointer: the bytes on disk are the bytes the role publishes.
    """

    previous_step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW,
    )(
        _installed_notifier_manifest(
            authority_root.parent,
            serving_authority_root=str(authority_root),
        ).model_copy(update={"producer_commit": COMMIT})
    )
    previous_step()
    return (authority_root / "current.json").read_text(encoding="utf-8")


def test_notifier_carries_the_signals_pointer_its_previous_generation_published(
    tmp_path: Path,
    two_notifier_generations: Path,
) -> None:
    """#260: the eighth window's failure, in the world that produces it.

    The signals authority belongs to this role, and a release does not republish it: after
    the handover `current.json` still carries the previous generation's commit, and
    comparing it against this one took `notifier.admin.shadow.v1` DEGRADED every two
    seconds. `serving.publisher.v1` reads the very same file and was given this predicate
    in #253; the notifier now asks the same question of the same generation tree.
    """

    _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    pointer = _publish_previous_generation_pointer(authority_root)
    assert COMMIT in pointer

    manifest = _installed_notifier_manifest(
        tmp_path,
        serving_authority_root=str(authority_root),
    )
    blind = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW + timedelta(seconds=1),
    )(manifest)
    with pytest.raises(ServingSourceAuthorityIntegrityError, match="producer_commit"):
        blind()

    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW + timedelta(seconds=1),
        runtime_root=two_notifier_generations,
    )(manifest)
    result = step()

    assert len(result.source_generations["signals_serving_authority"]) == 64
    assert result.degraded_reasons == ()


def test_an_explicitly_named_previous_commit_outranks_the_lineage(
    tmp_path: Path,
    two_notifier_generations: Path,
) -> None:
    """Package R review SF-1: the two paths overlap, and the configured one wins.

    Both answer the same question -- "is the pointer on disk our own previous
    generation's?" -- but they answer it differently: the lineage accepts the pointer and
    leaves it alone, while `serving_previous_producer_commit` is an operator instructing
    a takeover, which writes a `record_serving_authority_handoff` row, advances the
    sequence and republishes the pointer under this commit. Letting the lineage answer
    first made the configured takeover silently not happen, audit row included, for
    exactly the generation an operator would configure it for.
    """

    state = _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    assert COMMIT in _publish_previous_generation_pointer(authority_root)

    step = notifier_builder(
        provider_loader=lambda: {},
        clock=lambda: NOW + timedelta(seconds=1),
        runtime_root=two_notifier_generations,
    )(
        _installed_notifier_manifest(
            tmp_path,
            paused=True,
            serving_authority_root=str(authority_root),
            serving_previous_producer_commit=COMMIT,
        )
    )

    result = step()

    pointer = (authority_root / "current.json").read_text(encoding="utf-8")
    handoffs = state.serving_authority_handoffs()
    assert len(handoffs) == 1
    assert handoffs[0].previous_producer_commit == COMMIT
    assert handoffs[0].next_producer_commit == SECOND_COMMIT
    #: the takeover republished under this commit, which is what the lineage path does
    #: not do -- it carries the previous generation's pointer unchanged
    assert SECOND_COMMIT in pointer
    assert COMMIT not in pointer
    assert result.source_generations["signals_serving_authority"]
    #: and nothing was reported as carried across, because nothing was
    assert getattr(step, "generation_events", ()) == ()


def test_a_named_commit_that_is_not_the_pointer_refuses_even_on_our_own_lineage(
    tmp_path: Path,
    two_notifier_generations: Path,
) -> None:
    """Package S review SF-C: the priority's other face, and it is a new refusal.

    The pointer on disk was written by a generation this runtime root installed, so the
    lineage would carry it and the round would be healthy. Naming a *different* commit
    takes the lineage away from the primary reader and there is nothing left to fall back
    on, so the round fails closed instead. That is the intended reading of the rule --
    an operator naming X while the pointer says Y is exactly where accepting Y on
    ancestry would swallow the instruction a second time -- but it is a failure mode this
    configuration did not have before, and it is pinned here rather than left to be
    rediscovered on a trading day.
    """

    _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    assert COMMIT in _publish_previous_generation_pointer(authority_root)

    manifest = _installed_notifier_manifest(
        tmp_path,
        paused=True,
        serving_authority_root=str(authority_root),
    )
    #: the same world, same pointer, same runtime root: only the named commit differs
    carried = notifier_builder(
        provider_loader=lambda: {},
        clock=lambda: NOW + timedelta(seconds=1),
        runtime_root=two_notifier_generations,
    )(manifest)
    assert carried().source_generations["signals_serving_authority"]

    named = manifest.model_copy(
        update={
            "settings": {
                **manifest.settings,
                "serving_previous_producer_commit": "e" * 40,
            }
        }
    )
    step = notifier_builder(
        provider_loader=lambda: {},
        clock=lambda: NOW + timedelta(seconds=1),
        runtime_root=two_notifier_generations,
    )(named)

    with pytest.raises(
        ServingSourceAuthorityIntegrityError,
        match="current pointer producer_commit does not match expected commit",
    ):
        step()


def test_notifier_still_refuses_a_signals_pointer_from_no_generation_of_ours(
    tmp_path: Path,
    two_notifier_generations: Path,
) -> None:
    """The other half: only our own past is carried, and the refusal is word for word."""

    from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
    from rquant.runtime_serving_snapshot import SignalDeliveryPayload, SourceReadResult
    from rquant.serving_contracts import FreshnessStatus

    _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    values: dict[str, object] = {
        "dataset_id": SIGNALS_DATASET_ID,
        "sequence": 1,
        "event_time": NOW,
        "published_at": NOW,
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": SignalDeliveryPayload(),
    }
    from rquant.runtime_contracts import canonical_sha256

    values["generation_id"] = canonical_sha256(values)
    ServingSourceAuthorityPublisher(
        root=authority_root,
        producer_commit="e" * 40,
        dataset_id=SIGNALS_DATASET_ID,
        payload_kind="signal_delivery",
        clock=lambda: NOW,
    ).publish(SourceReadResult.model_validate(values))

    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW + timedelta(seconds=1),
        runtime_root=two_notifier_generations,
    )(
        _installed_notifier_manifest(
            tmp_path,
            serving_authority_root=str(authority_root),
        )
    )

    with pytest.raises(
        ServingSourceAuthorityIntegrityError,
        match="current pointer producer_commit does not match expected commit",
    ):
        step()


def test_a_runtime_root_that_cannot_say_leaves_the_notifier_exactly_as_strict(
    tmp_path: Path,
) -> None:
    """Route B publishes no legacy bundle at all, and a bare build has no root either."""

    _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    _publish_previous_generation_pointer(authority_root)

    for runtime_root in (None, tmp_path / "not-a-runtime-root"):
        step = notifier_builder(
            provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
            clock=lambda: NOW + timedelta(seconds=1),
            runtime_root=runtime_root,
        )(
            _installed_notifier_manifest(
                tmp_path,
                serving_authority_root=str(authority_root),
            )
        )
        with pytest.raises(ServingSourceAuthorityIntegrityError, match="producer_commit"):
            step()


def test_the_notifier_run_says_which_generations_pointer_it_inherited(
    tmp_path: Path,
    two_notifier_generations: Path,
) -> None:
    """The handover is stamped on the run rather than left invisible, as serving stamps it."""

    from rquant.runtime_generation_lineage import load_runtime_generation_tree

    _seed_outbox(tmp_path)
    authority_root = (tmp_path / "serving-signals").resolve()
    manifest = _installed_notifier_manifest(
        tmp_path,
        serving_authority_root=str(authority_root),
    )

    def build() -> object:
        return notifier_builder(
            provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
            clock=lambda: NOW + timedelta(seconds=1),
            runtime_root=two_notifier_generations,
        )(manifest)

    #: nothing published yet, so there is nothing to have inherited
    assert getattr(build(), "generation_events", ()) == ()

    _publish_previous_generation_pointer(authority_root)
    lineage = load_runtime_generation_tree(two_notifier_generations).lineage(
        INSTALLED_NOTIFIER_SERVICE
    )
    events = getattr(build(), "generation_events", ())

    assert len(events) == 1
    assert lineage.previous[0].generation_id in events[0]
    assert SIGNALS_DATASET_ID in events[0]


def test_a_failing_notifier_iteration_still_says_what_it_did_with_the_replica(
    tmp_path: Path,
) -> None:
    """#260's side observation: the failing round had opened the replica and said `null`.

    The page projection is published before the serving authority on every return path, so
    an iteration that fails at the authority has already opened the database. The step
    hands the loop its gate's own summary, which is how the MF-1 rule reaches a failed
    round, and a notifier with no projection still hands it nothing.
    """

    _seed_outbox(tmp_path)
    replica = _page_projection_replica(tmp_path, synced_at=NOW - timedelta(minutes=1))
    authority_root = (tmp_path / "serving-signals").resolve()
    reading_manifest = _installed_notifier_manifest(
        tmp_path,
        serving_authority_root=str(authority_root),
        page_projection_database_path=str(replica),
    )
    #: no runtime root is given below, so this pointer is refused -- which is the failing
    #: iteration this test needs, and the one the window actually saw
    _publish_previous_generation_pointer(authority_root)

    bare = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW + timedelta(seconds=1),
    )(_installed_notifier_manifest(tmp_path, serving_authority_root=str(authority_root)))
    assert getattr(bare, "replica_iteration_summary", None) is None

    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: NOW + timedelta(seconds=1),
    )(reading_manifest)
    summary = getattr(step, "replica_iteration_summary", None)
    assert callable(summary)
    assert summary() == (False, 0)

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="producer_commit"):
        step()

    opened, read_bytes = summary()
    assert opened is True
    assert read_bytes is None or read_bytes >= 0


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


# ---------------------------------------------------------------------------------------
# Starting before the strategies have written anything (#232, #220)
# ---------------------------------------------------------------------------------------


def _without_runner_database(tmp_path: Path) -> RuntimeServiceManifest:
    """A real router authority whose one strategy has not created its database yet."""

    manifest, store = _authoritative_router_manifest(tmp_path)
    for path in sorted(store.path.parent.glob(f"{store.path.name}*")):
        path.unlink()
    assert not store.path.exists()
    return manifest


def test_the_router_creates_its_own_artifacts_before_it_looks_for_a_runner(
    tmp_path: Path,
) -> None:
    """#220: only this role creates the bus, and it used to die before doing so.

    `strategy_live` opens `live/signal-bus/signal_bus.sqlite3` read-only and its sandbox
    grants it `live/strategies/%i` alone, so the plane could not start in either order:
    the router checked every runner database first and exited, and the strategies were
    waiting for the bus that check stood in front of.
    """

    manifest = _without_runner_database(tmp_path)

    step = build_builtin_registry(clock=lambda: NOW).build(manifest)

    assert (tmp_path / "signal-bus.sqlite3").is_file()
    assert (tmp_path / "signal-spool").is_dir()

    #: and the wait says which file, on every iteration, without leaving the loop
    with pytest.raises(PeerArtifactUnavailableError, match="runner source") as raised:
        step()
    assert str(Path(str(manifest.settings["runner_state_path"]))) in str(raised.value)


def test_the_route_spool_source_document_is_published_before_the_wait(
    tmp_path: Path,
) -> None:
    """`paper_broker` and `notifier` need `spool/source.json`, not a routed signal."""

    manifest = _without_runner_database(tmp_path)
    step = build_builtin_registry(clock=lambda: NOW).build(manifest)

    with pytest.raises(PeerArtifactUnavailableError):
        step()

    spool = tmp_path / "signal-spool"
    assert (spool / "records").is_dir()
    assert (spool / "source.json").is_file()


def test_a_runner_database_that_appears_later_is_routed_from(tmp_path: Path) -> None:
    """The waiting is a wait, not a permanent state: the next iteration picks it up."""

    manifest = _without_runner_database(tmp_path)
    runner_path = Path(str(manifest.settings["runner_state_path"]))

    step = build_builtin_registry(clock=lambda: NOW).build(manifest)
    with pytest.raises(PeerArtifactUnavailableError):
        step()

    #: the strategy starts and does what it now does first of all
    restarted = StrategyRunnerStore(
        runner_path,
        spec=_strategy_spec(),
        evaluator_contract_fingerprint=EVALUATOR,
    )
    result = step()

    assert result.source_generations["n-shape-v1"] == restarted.source_generation_id


def test_a_runner_database_that_exists_and_is_unreadable_still_fails_closed(
    tmp_path: Path,
) -> None:
    """Absence defers. A file that is there and is not a runner database refuses, now."""

    manifest = _without_runner_database(tmp_path)
    Path(str(manifest.settings["runner_state_path"])).write_bytes(b"not a sqlite database")

    with pytest.raises(ValueError, match="runner source") as raised:
        build_builtin_registry(clock=lambda: NOW).build(manifest)
    assert not isinstance(raised.value, PeerArtifactUnavailableError)


def test_a_runner_path_replaced_by_a_symlink_still_fails_closed(tmp_path: Path) -> None:
    """The substitution `_require_safe_path` exists to catch, over the deferred open."""

    manifest = _without_runner_database(tmp_path)
    runner_path = Path(str(manifest.settings["runner_state_path"]))
    elsewhere = tmp_path / "elsewhere.sqlite3"
    StrategyRunnerStore(
        elsewhere,
        spec=_strategy_spec(),
        evaluator_contract_fingerprint=EVALUATOR,
    )
    runner_path.symlink_to(elsewhere)

    with pytest.raises(ValueError, match="symlink"):
        build_builtin_registry(clock=lambda: NOW).build(manifest)


def test_the_signal_bus_exists_even_when_a_runner_source_refuses(tmp_path: Path) -> None:
    """Why the bus is created first, and not merely before the routing step.

    Only this role creates `live/signal-bus/signal_bus.sqlite3`, and `strategy_live` opens
    it read-only while building its own step. A router that refuses because one strategy's
    database is unreadable must still have left the bus, or that one strategy's state takes
    every strategy on the plane down with it — the shape of #220.
    """

    manifest = _without_runner_database(tmp_path)
    Path(str(manifest.settings["runner_state_path"])).write_bytes(b"not a sqlite database")

    with pytest.raises(ValueError):
        build_builtin_registry(clock=lambda: NOW).build(manifest)

    assert (tmp_path / "signal-bus.sqlite3").is_file()
    assert (tmp_path / "signal-spool").is_dir()


def test_a_router_that_refuses_over_its_settings_creates_nothing(tmp_path: Path) -> None:
    """The bus is created early for the plane's sake, not as a side effect of refusing.

    `signal_router` owns three artifacts nobody else creates, and it builds them before
    it opens any strategy's runner database so that the live plane's start order stops
    being a cycle (#220). That is a reason to create them before reading *files*, not
    before deciding whether this manifest describes a router at all: a role that is going
    to refuse over its own settings must leave the directory as it found it.
    """

    manifest = _router_manifest(tmp_path)

    with pytest.raises(ValueError, match="authority"):
        build_builtin_registry(clock=lambda: NOW).build(manifest)

    assert not (tmp_path / "signal-bus.sqlite3").exists()
    assert not (tmp_path / "signal-spool").exists()


def test_the_notifier_never_writes_into_the_page_control_root(tmp_path: Path) -> None:
    """#241: the notifier reads the outbox; the page-control service owns the directory.

    `rquant-runtime-notifier@.service` grants `control/notifiers/%i`,
    `live/notifications/%i` and `-control/schema-rollouts`, and lists
    `control/page-control.sqlite3` under `ReadOnlyPaths`. `rquant-page-control.service`
    is the one unit whose `ReadWritePaths` covers `…/data/runtime/control`.

    The first attempt at this made the scratch directory land in `live/notifications/%i`
    and this test passed, because a test runs in one temporary directory. On a host it
    would not have: systemd makes every granted path its own bind mount and Linux
    `link()` refuses across mounts, so the notifier would have traded `EROFS` for `EXDEV`.
    The reader now pins with a descriptor and writes nothing, so this asserts the stronger
    thing -- the step writes nowhere under the runtime root at all.
    """

    from tests.runtime_readonly_sandbox import readonly_runtime, tree_state
    from tests.unit.test_serving_page_projection_source import (
        _save_signed_canvas_catalog_record,
        _signal_projection_database,
    )

    runtime_root = tmp_path / "runtime"
    control = runtime_root / "control"
    notifications = runtime_root / "live" / "notifications" / "svc-1"
    control.mkdir(parents=True)
    notifications.mkdir(parents=True)
    _seed_outbox(notifications)
    replica = (tmp_path / "rquant_ro.duckdb").resolve()
    _signal_projection_database(replica)
    outbox, catalog, _command, _receipt, authority = _save_signed_canvas_catalog_record(
        control,
        command_id="notifier-canvas",
    )
    public_key = authority.keyring._keys[authority.keyring.active_key_id].decode("utf-8")
    #: the canvas catalog record and its publication receipt are stamped by the real
    #: PageControl service while this test runs, and the projection refuses evidence
    #: dated after the instant it is asked for, so the step has to observe the present
    observed = datetime.now(UTC) + timedelta(minutes=1)
    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: observed,
    )(
        _notifier_manifest(
            notifications,
            serving_authority_root=str(notifications / "serving-authority"),
            page_projection_database_path=str(replica),
            page_projection_canvas_catalog_root=str(catalog),
            page_projection_canvas_receipt_root=str(
                catalog.parent / "canvas-publication-receipts"
            ),
            page_projection_page_control_outbox_path=str(outbox.path),
            page_projection_canvas_active_key_id=authority.keyring.active_key_id,
            page_projection_canvas_active_public_key_pem=public_key,
        )
    )
    before = tree_state(control)

    with readonly_runtime(runtime_root, writable=()) as violations:
        result = step()

    assert violations == [], violations
    assert tree_state(control) == before
    assert result.degraded_reasons == ()
