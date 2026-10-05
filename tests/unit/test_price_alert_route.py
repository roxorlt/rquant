import json
import sqlite3
from pathlib import Path

import pytest

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.price_alert_route import (
    PriceAlertOwnerTargets,
    PriceAlertRecipientPolicy,
    route_price_alert_event,
)
from rquant.price_alert_runtime_contracts import verify_price_alert_activation
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.signal_bus import SignalBusStore
from tests.unit.test_price_alert_event_contracts import AT
from tests.unit.test_price_alert_runtime_store import round_input, store_fixture


def route_fixture(tmp_path: Path):
    producer, producer_activation, frequency = store_fixture(tmp_path)
    item = producer.commit_round(
        round_input(producer_activation, frequency), policy=frequency
    ).events[0]
    source = producer.source_descriptor()
    policy = PriceAlertRecipientPolicy(
        generation_id="e" * 64,
        owners=(
            PriceAlertOwnerTargets(
                owner_id="alice",
                targets=(
                    DeliveryTarget(channel=DeliveryChannel.PUSHDEER, recipient_id="alice.phone"),
                ),
            ),
        ),
    )
    body = json.loads((tmp_path / "runtime.json").read_text())
    body["service_kind"] = "signal_router"
    flags = body["settings"]["price_alert_runtime"]
    flags.update(
        evaluation_enabled=False,
        event_write_enabled=False,
        routing_enabled=True,
        recipient_policy_sha256=policy.sha256,
    )
    path = tmp_path / "router.json"
    path.write_text(json.dumps(body))
    path.chmod(0o600)
    import hashlib

    activation = verify_price_alert_activation(
        path,
        runtime_root=tmp_path,
        expected_manifest_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        expected_commit="b" * 40,
        expected_kind=RuntimeServiceKind.SIGNAL_ROUTER,
    )
    bus = SignalBusStore(tmp_path / "bus.sqlite3")
    bus.install_price_alert_route_v1(activation)
    return producer, bus, activation, policy, source, item


def test_dedicated_event_routes_to_original_queue_and_replays_exactly(tmp_path: Path) -> None:
    producer, bus, activation, policy, source, item = route_fixture(tmp_path)
    one = route_price_alert_event(
        bus,
        activation=activation,
        policy=policy,
        source=source,
        record=item,
        source_inspected_at=AT,
        routed_at=AT,
    )
    two = route_price_alert_event(
        bus,
        activation=activation,
        policy=policy,
        source=source,
        record=item,
        source_inspected_at=AT,
        routed_at=AT,
    )
    assert one == two
    assert one.receipt.disposition == "routed"
    assert bus.source_descriptor().high_watermark == 1
    assert len(bus.outbox_records()) == 1
    assert bus.notification_event(item.event.event_id).event == item.event
    assert (
        bus.claim_due(
            "old", now=AT, lease_for=__import__("datetime").timedelta(seconds=10), limit=10
        )
        == ()
    )
    with pytest.raises((TypeError, ValueError)):
        bus.ingest(item.event, received_at=AT)
    with pytest.raises((TypeError, ValueError)):
        bus.route(item.event.event_id, targets=policy.owners[0].targets, routed_at=AT)
    producer.close()


@pytest.mark.parametrize(
    "point", ["source", "event", "receipt", "outbox", "cursor", "before_commit"]
)
def test_actual_bus_failure_is_one_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    producer, bus, activation, policy, source, item = route_fixture(tmp_path)

    def fail(name: str) -> None:
        if name == point:
            raise OSError("injected route failure")

    monkeypatch.setattr(bus, "_price_alert_failpoint", fail)
    with pytest.raises(OSError):
        route_price_alert_event(
            bus,
            activation=activation,
            policy=policy,
            source=source,
            record=item,
            source_inspected_at=AT,
            routed_at=AT,
        )
    assert bus.source_descriptor().high_watermark == 0
    with sqlite3.connect(bus.path) as connection:
        for table in (
            "signal_envelope",
            "delivery_outbox",
            "price_alert_route_source",
            "price_alert_route_receipt",
        ):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    monkeypatch.setattr(bus, "_price_alert_failpoint", lambda _: None)
    assert (
        route_price_alert_event(
            bus,
            activation=activation,
            policy=policy,
            source=source,
            record=item,
            source_inspected_at=AT,
            routed_at=AT,
        ).global_sequence
        == 1
    )
    producer.close()


def test_same_channel_two_devices_are_exact_targets_no_duplicate_or_cross_owner() -> None:
    targets = tuple(
        DeliveryTarget(channel=DeliveryChannel.PUSHDEER, recipient_id=value)
        for value in ("alice.mac", "alice.phone")
    )
    policy = PriceAlertRecipientPolicy(
        generation_id="e" * 64, owners=(PriceAlertOwnerTargets(owner_id="alice", targets=targets),)
    )
    assert policy.targets_for("bob") == ()
    assert policy.targets_for("alice") == targets
    with pytest.raises(ValueError):
        PriceAlertOwnerTargets(owner_id="alice", targets=(targets[0], targets[0]))
