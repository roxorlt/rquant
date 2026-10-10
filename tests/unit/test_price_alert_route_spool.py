import json
from pathlib import Path

import pytest

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.signal_bus import RouteDecisionKind, RouteSourceDescriptor, routing_decision_fingerprint
from rquant.signal_route_spool import (
    PriceAlertRouteSpoolRecord,
    ReadonlyNotificationEventRouteSpool,
    SignalRouteSpool,
    publish_mixed_notification_bus_prefix,
)
from tests.unit.test_price_alert_event_contracts import AT
from tests.unit.test_price_alert_route import route_fixture
from tests.unit.test_signal_bus import _signal


def legacy_route(bus, seed: str, sequence: int):
    item = _signal(seed, available_at=AT)
    targets = (DeliveryTarget(recipient_id="old.admin", channel=DeliveryChannel.PUSHDEER),)
    bus.commit_source_route(
        descriptor=RouteSourceDescriptor(
            source_id="legacy",
            generation_id="f" * 64,
            strategy_spec_fingerprint="1" * 64,
            first_sequence=1,
            high_watermark=sequence,
        ),
        routing_policy_fingerprint="e" * 64,
        source_sequence=sequence,
        signal=item,
        decision_kind=RouteDecisionKind.ROUTE,
        decision_fingerprint=routing_decision_fingerprint(
            routing_policy_fingerprint="e" * 64,
            decision_kind=RouteDecisionKind.ROUTE,
            targets=targets,
            reason_code=None,
        ),
        reason_code=None,
        targets=targets,
        routed_at=AT,
    )
    return item


def mixed_fixture(tmp_path: Path):
    producer, bus, activation, policy, source, price = route_fixture(tmp_path)
    one = legacy_route(bus, "a", 1)
    bus.commit_price_alert_route(
        activation=activation,
        policy=policy,
        source=source,
        record=price,
        source_inspected_at=AT,
        routed_at=AT,
    )
    three = legacy_route(bus, "c", 2)
    return producer, bus, activation, one, price, three


def test_exact_v2_v4_v2_one_chain_and_all_off_legacy_append(tmp_path: Path) -> None:
    producer, bus, activation, one, price, three = mixed_fixture(tmp_path)
    spool = SignalRouteSpool(tmp_path / "spool")
    published = publish_mixed_notification_bus_prefix(
        bus=bus, spool=spool, limit=100, observed_at=AT
    )
    assert published.published_count == 3
    reader = ReadonlyNotificationEventRouteSpool(tmp_path / "spool")
    rows = reader.routed_after_global_sequence(
        after_sequence=0, through_sequence=3, limit=100, observed_at=AT
    )
    assert rows[0].signal == one
    assert rows[1].event == price.event
    assert rows[2].signal == three
    original = (tmp_path / "spool/records/00000000000000000002.json").read_bytes()
    assert PriceAlertRouteSpoolRecord.model_validate_json(original).schema_version == 4
    body = json.loads((tmp_path / "router.json").read_text())
    for stage in ("evaluation", "event_write", "routing", "delivery"):
        body["settings"]["price_alert_runtime"][stage + "_enabled"] = False
    (tmp_path / "router.json").write_text(json.dumps(body))
    four = legacy_route(bus, "d", 3)
    next_result = publish_mixed_notification_bus_prefix(
        bus=bus, spool=spool, limit=100, observed_at=AT
    )
    assert next_result.published_count == 1
    assert reader.source_descriptor().high_watermark == 4
    assert (
        reader.routed_after_global_sequence(
            after_sequence=3, through_sequence=4, limit=100, observed_at=AT
        )[0].signal
        == four
    )
    assert (tmp_path / "spool/records/00000000000000000002.json").read_bytes() == original
    assert reader.observed_prefix_receipt(observed_at=AT).upstream_complete is False
    producer.close()


def test_immutable_entry_then_pointer_failure_recovers_original_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import signal_route_spool as module

    producer, bus, activation, one, price, three = mixed_fixture(tmp_path)
    spool = SignalRouteSpool(tmp_path / "spool")
    original = module._atomic_replace_at

    def fail(descriptor, name, payload, **kwargs):
        if name == "current.json":
            raise OSError("pointer commit failed")
        return original(descriptor, name, payload, **kwargs)

    monkeypatch.setattr(module, "_atomic_replace_at", fail)
    with pytest.raises(OSError):
        publish_mixed_notification_bus_prefix(bus=bus, spool=spool, limit=100, observed_at=AT)
    frozen = (tmp_path / "spool/records/00000000000000000002.json").read_bytes()
    monkeypatch.setattr(module, "_atomic_replace_at", original)
    assert (
        publish_mixed_notification_bus_prefix(
            bus=bus, spool=spool, limit=100, observed_at=AT
        ).published_high_watermark
        == 3
    )
    assert (tmp_path / "spool/records/00000000000000000002.json").read_bytes() == frozen
    producer.close()


def test_old_publisher_rejects_price_and_mixed_rejects_altered_bytes(tmp_path: Path) -> None:
    producer, bus, activation, one, price, three = mixed_fixture(tmp_path)
    spool = SignalRouteSpool(tmp_path / "spool")
    record = bus.routed_notification_events_after_global_sequence(
        after_sequence=1, through_sequence=2, observed_at=AT, limit=100
    )[0]
    with pytest.raises(TypeError):
        spool.publish(source=bus.source_descriptor(), records=(record,))
    publish_mixed_notification_bus_prefix(bus=bus, spool=spool, limit=100, observed_at=AT)
    path = tmp_path / "spool/records/00000000000000000002.json"
    value = json.loads(path.read_bytes())
    value["record"]["event"]["rule_name"] = "altered"
    path.write_text(json.dumps(value))
    with pytest.raises((TypeError, ValueError, RuntimeError)):
        ReadonlyNotificationEventRouteSpool(tmp_path / "spool").source_descriptor()
    producer.close()
