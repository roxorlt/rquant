from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pytest

AT = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)


def condition_event():
    from rquant.condition_alert_runtime_contracts import ConditionAlertEventEnvelope
    from rquant.delivery_contracts import DeliveryChannel

    return ConditionAlertEventEnvelope.create(
        owner_id="alice",
        rule_id="full-rule",
        rule_name="完整条件",
        priority="P1",
        channels=(DeliveryChannel.PUSHDEER,),
        rule_version=2,
        rule_body_hash="1" * 64,
        scope_version="2" * 64,
        member_digest="3" * 64,
        ts_code="600000.SH",
        stock_name="样本",
        trigger_kind="matched",
        previous_truth="false",
        truth="true",
        source_identity="4" * 64,
        raw_batch_id="5" * 64,
        feature_snapshot_id="6" * 64,
        daily_anchor_date=date(2026, 7, 30),
        event_time=AT - timedelta(seconds=2),
        decision_time=AT,
        available_at=AT,
        expires_at=AT + timedelta(minutes=5),
        evaluation_contract_sha256="7" * 64,
        frequency_policy_sha256="8" * 64,
        frequency_bucket="2026-07-31T01:40:00Z",
        producer_manifest_sha256="9" * 64,
        producer_commit="a" * 40,
        source_epoch="b" * 64,
    )


def condition_record(*, sequence: int = 1, bus_generation: str = "c" * 64):
    from rquant.condition_alert_route import (
        ConditionAlertBusRoutedRecord,
        ConditionAlertRouteReceipt,
    )
    from rquant.condition_alert_runtime_contracts import ConditionAlertSourceDescriptor
    from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
    from rquant.price_alert_route import target_manifest_hash

    event = condition_event()
    source = ConditionAlertSourceDescriptor(
        source_id="condition-runtime",
        ledger_id="d" * 64,
        source_epoch=event.source_epoch,
        generation_id="e" * 64,
        producer_manifest_sha256=event.producer_manifest_sha256,
        evaluation_contract_sha256=event.evaluation_contract_sha256,
        frequency_policy_sha256=event.frequency_policy_sha256,
        routing_policy_sha256="f" * 64,
        first_sequence=1,
        high_watermark=1,
    )
    targets = (DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),)
    receipt = ConditionAlertRouteReceipt.create(
        source_id=source.source_id,
        source_sequence=1,
        event_id=event.event_id,
        owner_id=event.owner_id,
        rule_id=event.rule_id,
        rule_version=event.rule_version,
        scope_version=event.scope_version,
        disposition="routed",
        reason_code=None,
        routing_policy_sha256=source.routing_policy_sha256,
        target_manifest_hash=target_manifest_hash(targets),
        targets=targets,
        target_count=1,
        source_inspected_at=AT,
        routed_at=AT,
    )
    return ConditionAlertBusRoutedRecord(
        global_sequence=sequence,
        event_id=event.event_id,
        payload_hash=event.sha256,
        payload_json=event.wire_bytes().decode(),
        event=event,
        received_at=AT,
        bus_generation_id=bus_generation,
        source=source,
        source_sequence=1,
        receipt=receipt,
    )


def test_condition_event_is_separate_canonical_and_rejects_future_daily_facts() -> None:
    from rquant.condition_alert_runtime_contracts import parse_condition_alert_event

    event = condition_event()
    assert parse_condition_alert_event(event.wire_bytes()) == event
    assert sha256(event.wire_bytes()).hexdigest() == event.sha256
    with pytest.raises((TypeError, ValueError)):
        parse_condition_alert_event(event.wire_bytes().replace(b'"matched"', b'"unknown"'))
    with pytest.raises(ValueError):
        type(event).model_validate_json(
            event.wire_bytes().replace(b'"2026-07-30"', b'"2026-07-31"')
        )


def test_condition_v5_wrapper_binds_exact_source_payload_and_legacy_chain_head() -> None:
    from rquant.signal_route_spool import (
        ConditionAlertRouteSpoolRecord,
        _decode_notification_spool_record,
    )

    record = condition_record(sequence=3)
    entry = ConditionAlertRouteSpoolRecord.create(record=record, previous_record_hash="d" * 64)
    assert entry.schema_version == 5
    assert entry.record_schema == "rquant.condition-alert-route-record/v1"
    assert _decode_notification_spool_record(entry.wire_bytes(), sequence=3) == entry
    with pytest.raises((TypeError, ValueError)):
        _decode_notification_spool_record(
            entry.wire_bytes().replace(b'"schema_version":5', b'"schema_version":3'), sequence=3
        )
    with pytest.raises((TypeError, ValueError)):
        ConditionAlertRouteSpoolRecord.create(
            record=record.model_copy(update={"event_id": "e" * 64}), previous_record_hash=None
        )


def test_condition_bus_binding_rejects_substituted_payload_and_source_generation() -> None:
    record = condition_record()
    with pytest.raises(ValueError):
        type(record).model_validate_json(
            record.wire_bytes().replace(
                b'"source_epoch":"' + b"b" * 64 + b'"', b'"source_epoch":"' + b"e" * 64 + b'"', 1
            )
        )
    with pytest.raises(ValueError):
        type(record).model_validate(record.model_copy(update={"payload_json": "{}"}))


def condition_route_fixture(tmp_path: Path, bus=None, *, produced_source=None):
    from rquant.condition_alert_route import ConditionAlertRecipientPolicy
    from rquant.condition_alert_runtime_contracts import (
        ConditionAlertProducerEventRecord,
        verify_condition_alert_activation,
    )
    from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
    from rquant.price_alert_route import PriceAlertOwnerTargets
    from rquant.runtime_service_entrypoint import RuntimeServiceKind
    from rquant.signal_bus import SignalBusStore

    sample = condition_record()
    policy = ConditionAlertRecipientPolicy(
        generation_id="e" * 64,
        owners=(
            PriceAlertOwnerTargets(
                owner_id="alice",
                targets=(DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
            ),
        ),
    )
    source = sample.source if produced_source is None else produced_source
    settings = {
        name: getattr(source, name)
        for name in (
            "source_id",
            "source_epoch",
            "ledger_id",
            "generation_id",
            "evaluation_contract_sha256",
            "frequency_policy_sha256",
            "routing_policy_sha256",
        )
    }
    settings.update(
        recipient_policy_sha256=policy.sha256,
        routing_enabled=True,
        evaluation_enabled=False,
        event_write_enabled=False,
        delivery_enabled=False,
    )
    path = tmp_path / "condition-router.json"
    path.write_text(
        json.dumps(
            {
                "service_id": "condition.router",
                "service_kind": "signal_router",
                "plane": "live",
                "interval_seconds": 1,
                "stale_after_seconds": 10,
                "producer_commit": "a" * 40,
                "settings": {"condition_alert_runtime": settings},
            }
        )
    )
    path.chmod(0o600)
    activation = verify_condition_alert_activation(
        path,
        runtime_root=tmp_path,
        expected_manifest_sha256=sha256(path.read_bytes()).hexdigest(),
        expected_commit="a" * 40,
        expected_kind=RuntimeServiceKind.SIGNAL_ROUTER,
    )
    actual = bus if bus is not None else SignalBusStore(tmp_path / "condition-bus.sqlite3")
    actual.install_condition_alert_route_v1(activation)
    producer_record = ConditionAlertProducerEventRecord(
        sequence=1,
        event=sample.event,
        payload_json=sample.payload_json,
        payload_sha256=sample.payload_hash,
    )
    return actual, activation, policy, source, producer_record


def test_condition_route_uses_original_bus_outbox_and_exact_replay(tmp_path: Path) -> None:
    bus, activation, policy, source, item = condition_route_fixture(tmp_path)
    one = bus.commit_condition_alert_route(
        activation=activation,
        policy=policy,
        source=source,
        record=item,
        source_inspected_at=AT,
        routed_at=AT,
    )
    two = bus.commit_condition_alert_route(
        activation=activation,
        policy=policy,
        source=source,
        record=item,
        source_inspected_at=AT,
        routed_at=AT,
    )
    assert one == two
    assert bus.notification_event(item.event.event_id).event == item.event
    assert bus.source_descriptor().high_watermark == 1
    assert len(bus.outbox_records()) == 1
    assert bus.claim_due("old", now=AT, lease_for=timedelta(seconds=10), limit=10) == ()


@pytest.mark.parametrize(
    "point", ["source", "event", "receipt", "outbox", "cursor", "before_commit"]
)
def test_condition_route_failure_rolls_back_original_bus_and_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    bus, activation, policy, source, item = condition_route_fixture(tmp_path)

    def fail(name: str) -> None:
        if name == point:
            raise OSError("condition transaction failure")

    monkeypatch.setattr(bus, "_condition_alert_failpoint", fail)
    with pytest.raises(OSError):
        bus.commit_condition_alert_route(
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
            "condition_alert_route_source",
            "condition_alert_route_receipt",
        ):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
