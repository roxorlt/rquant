from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxStatus
from rquant.notification_worker import NotificationDelivery
from rquant.runtime_builder_signal import notifier_builder, signal_router_builder
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.signal_bus import RouteSourceDescriptor, SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_router_runtime import RoutingDecision
from rquant.strategy_runner import RunnerSignalRecord

NOW = datetime(2026, 7, 31, 2, 30, tzinfo=UTC)
COMMIT = "a" * 40
POLICY = "b" * 64
GENERATION = "c" * 64
SPEC = "d" * 64


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

    def descriptor(self) -> RouteSourceDescriptor:
        return RouteSourceDescriptor(
            source_id="n-shape-v1",
            generation_id=GENERATION,
            strategy_spec_fingerprint=SPEC,
            first_sequence=1,
            high_watermark=len(self.records),
        )

    def signals_after(self, *, sequence: int) -> tuple[RunnerSignalRecord, ...]:
        return tuple(record for record in self.records if record.sequence > sequence)


class _Provider:
    def __init__(self) -> None:
        self.deliveries: list[NotificationDelivery] = []

    def deliver(self, delivery: NotificationDelivery) -> str:
        self.deliveries.append(delivery)
        return f"receipt:{delivery.record.outbox_id}"


def _router_manifest(
    tmp_path: Path,
    **setting_overrides: object,
) -> RuntimeServiceManifest:
    settings: dict[str, object] = {
        "signal_bus_path": str(tmp_path / "signal-bus.sqlite3"),
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


def _notifier_manifest(
    tmp_path: Path,
    **setting_overrides: object,
) -> RuntimeServiceManifest:
    settings: dict[str, object] = {
        "signal_bus_path": str(tmp_path / "signal-bus.sqlite3"),
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


def _seed_outbox(path: Path) -> SignalBusStore:
    store = SignalBusStore(path)
    signal = _signal()
    store.ingest(signal, received_at=NOW)
    store.route(
        signal.signal_id,
        (
            DeliveryTarget(
                recipient_id="admin",
                channel=DeliveryChannel.PUSHDEER,
            ),
        ),
        now=NOW,
    )
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
    assert result.source_generations == {"n-shape-v1": GENERATION}
    assert result.degraded_reasons == ()


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

    def fail(_signal: SignalEnvelope) -> RoutingDecision:
        raise RuntimeError("routing registry unavailable")

    active = signal_router_builder(
        source_loader=lambda _source_id: source,
        target_resolver=fail,
        clock=lambda: NOW,
    )(_router_manifest(tmp_path))
    with pytest.raises(RuntimeError, match="routing registry unavailable"):
        active()

    assert SignalBusStore(tmp_path / "signal-bus.sqlite3").route_cursor(
        "n-shape-v1"
    ).last_sequence == 0


def test_notifier_loads_providers_outside_manifest_and_maps_backlog(tmp_path: Path) -> None:
    bus = _seed_outbox(tmp_path / "signal-bus.sqlite3")
    generation = bus.source_descriptor().generation_id
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
    assert result.output_sequence == -1
    assert result.processed_count == 1
    assert result.backlog_count == 0
    assert result.source_generations == {"signal_bus": generation}
    assert result.degraded_reasons == ()
    assert bus.outbox_records()[0].status is OutboxStatus.SUCCEEDED


def test_notifier_pause_or_provider_loader_failure_never_claims_outbox(
    tmp_path: Path,
) -> None:
    bus = _seed_outbox(tmp_path / "signal-bus.sqlite3")
    loader_calls: list[bool] = []
    paused = notifier_builder(
        provider_loader=lambda: (loader_calls.append(True), {})[1],
        clock=lambda: NOW,
    )(_notifier_manifest(tmp_path, paused=True))

    paused_result = paused()

    assert loader_calls == []
    assert paused_result.backlog_count == 1
    assert paused_result.degraded_reasons == ("notifier:paused",)
    assert bus.outbox_records()[0].status is OutboxStatus.PENDING

    def fail_loader() -> dict[DeliveryChannel, _Provider]:
        raise RuntimeError("secret store unavailable")

    active = notifier_builder(
        provider_loader=fail_loader,
        clock=lambda: NOW,
    )(_notifier_manifest(tmp_path))
    with pytest.raises(RuntimeError, match="secret store unavailable"):
        active()

    record = bus.outbox_records()[0]
    assert record.status is OutboxStatus.PENDING
    assert record.attempt_count == 0


@pytest.mark.parametrize(
    ("builder_name", "manifest_factory", "overrides", "message"),
    [
        ("router", _router_manifest, {"signal_bus_path": "relative.sqlite3"}, "absolute"),
        ("router", _router_manifest, {"batch_limit": True}, "integer"),
        ("router", _router_manifest, {"batch_limit": 1_001}, "less than or equal"),
        ("notifier", _notifier_manifest, {"signal_bus_path": "relative.sqlite3"}, "absolute"),
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
