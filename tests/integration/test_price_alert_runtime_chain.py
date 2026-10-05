"""Synthetic data through the real public command and runtime builders; no socket/network."""

import json
import sqlite3
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxStatus
from rquant.notification_state import NotificationStateStore
from rquant.page_control import PageControlOutbox, PageControlStatus, SetPriceAlertRuleEnabled
from rquant.price_alert_admission import PriceAlertAdmission
from rquant.price_alert_route import PriceAlertOwnerTargets, PriceAlertRecipientPolicy
from rquant.price_alert_runtime_projection import PriceAlertCancellationReceipt
from rquant.runtime_builder_price_alert import (
    price_alert_runtime_builder,
    verify_price_role_manifest,
)
from rquant.runtime_builder_signal import notifier_builder, signal_router_builder
from rquant.runtime_service_builtin import watchlist_quote_source_builder
from rquant.runtime_service_entrypoint import RuntimeServiceManifest
from rquant.runtime_serving_authority import ServingSourceAuthorityReader
from rquant.serving_manual_watchlist_projection import build_manual_watchlist_projections
from rquant.serving_page_projection_source import _ReadonlyPageControlAuditReader
from rquant.serving_price_alert_rule_projection import build_price_alert_rule_projections
from rquant.signal_bus import SignalBusStore
from tests.support.web_serving_fixture import build_web_fixture
from tests.unit.test_price_alert_notification_provider import FakeTransport, provider
from tests.unit.test_price_alert_rule_commands import _member, _save, _service
from tests.unit.test_price_alert_runtime_builders import quote_manifest, runtime_fixture
from tests.unit.test_runtime_builder_signal import (
    _notifier_manifest,
    _route_target,
    _router_manifest,
    _Source,
)
from tests.unit.test_web_price_alert_rules import client_for


def test_actual_price_command_producer_router_notifier_serving_api_chain(tmp_path: Path) -> None:
    (
        now,
        producer_path,
        producer_manifest,
        producer_cap,
        frequency,
        requests,
        scope_root,
        calendar,
    ) = runtime_fixture(tmp_path)
    current = [now]
    control = PageControlOutbox(tmp_path / "control.sqlite3")
    control.activate_manual_watchlist(now - timedelta(days=1))
    control.activate_price_alert_rules(now - timedelta(days=1))
    with patch("tests.unit.test_price_alert_rule_commands.NOW", now):
        _member(control.path, "alice")
    admission = PriceAlertAdmission(_service(control, tmp_path, now=now))
    command = _save("chain-save", rule_id="rule/a", requested_at=now)
    receipt = admission.submit(command, authenticated_owner_id="alice")
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert admission.submit(command, authenticated_owner_id="alice") == receipt
    audit = _ReadonlyPageControlAuditReader(control.path)
    publication_sequence = [0]

    def publish_scope(root: Path, *, runtime_projections=()):
        sequence = publication_sequence[0] + 1
        publication_sequence[0] = sequence
        with audit.snapshot():
            rule_projections = build_price_alert_rule_projections(
                audit.price_alert_rule_snapshot(), observed_at=current[0]
            )
            member_projections = build_manual_watchlist_projections(
                audit.manual_watchlist_snapshot(), observed_at=current[0]
            )
        with patch(
            "tests.support.web_serving_fixture.FIXTURE_BUILT_AT",
            current[0] - timedelta(minutes=sequence),
        ):
            return build_web_fixture(
                root,
                "baseline",
                sequence=sequence,
                signal_projections=(*runtime_projections, *rule_projections, *member_projections),
            )

    publish_scope(scope_root)
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
    policy_path = tmp_path / "price-policy.json"
    policy_path.write_bytes(policy.wire_bytes())
    policy_path.chmod(0o600)
    peer = dict(
        producer_manifest_path=str(producer_path),
        ledger_path=str(tmp_path / "runtime.sqlite3"),
        recipient_policy_path=str(policy_path),
        scope_serving_root=str(scope_root),
    )
    producer_flags = producer_manifest.model_dump(mode="json")["settings"]["price_alert_runtime"]

    def role_manifest(base: RuntimeServiceManifest, name: str):
        path = tmp_path / f"{name}.json"
        body = base.model_dump(mode="json")
        body["producer_commit"] = "b" * 40
        flags = {
            **producer_flags,
            "evaluation_enabled": False,
            "event_write_enabled": False,
            "routing_enabled": name == "router",
            "delivery_enabled": name == "notifier",
            "recipient_policy_sha256": policy.sha256,
        }
        body["settings"].update(
            price_alert_runtime=flags,
            price_alert_runtime_manifest_path=str(path),
            price_alert_peer=peer,
        )
        path.write_text(json.dumps(body))
        path.chmod(0o600)
        model = RuntimeServiceManifest.model_validate_json(path.read_bytes())
        return model, verify_price_role_manifest(model, runtime_root=tmp_path)

    router_manifest, router_cap = role_manifest(
        _router_manifest(tmp_path, batch_limit=100), "router"
    )
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    bus.install_price_alert_route_v1(router_cap)
    source = _Source(())
    router = signal_router_builder(
        source_loader=lambda _: source,
        target_resolver=_route_target,
        clock=lambda: current[0],
        runtime_root=tmp_path,
    )(router_manifest)
    notify_root = tmp_path / "notification-authority"
    notify_manifest, notify_cap = role_manifest(
        _notifier_manifest(tmp_path, serving_authority_root=str(notify_root)), "notifier"
    )
    state = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    state.install_price_alert_delivery_v1(notify_cap)
    transport = FakeTransport()
    notifier = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: provider(transport)},
        clock=lambda: current[0],
        runtime_root=tmp_path,
    )(notify_manifest)

    def quote_provider(codes, *, timeout_seconds, on_started):
        on_started(current[0])
        return pd.DataFrame(
            [
                dict(
                    ts_code=code,
                    price=11.0,
                    open=10.0,
                    high=11.0,
                    low=10.0,
                    volume=10.0,
                    amount=100.0,
                    source_observed_at=current[0],
                )
                for code in codes
            ]
        )

    quote = watchlist_quote_source_builder(
        provider_factory=lambda: quote_provider, universe_loader=None, clock=lambda: current[0]
    )(quote_manifest(tmp_path, calendar))
    producer = price_alert_runtime_builder(clock=lambda: current[0], runtime_root=tmp_path)(
        producer_manifest
    )
    try:
        assert quote().batch_published
        assert producer().output_sequence == 1
        assert router().output_sequence == 1
        assert notifier().output_sequence == 1
        assert (
            len(transport.calls) == 1 and state.outbox_records()[0].status is OutboxStatus.SUCCEEDED
        )
        first = state.notification_event(state.outbox_records()[0].signal_id).event
        assert first.price == "11.0" and first.threshold == "10.00"
        current[0] = now + timedelta(seconds=5)
        assert quote().batch_published
        assert producer().output_sequence == 1
        router()
        notifier()
        assert len(transport.calls) == 1
        current[0] = now + timedelta(seconds=60)
        publish_scope(scope_root)
        assert quote().batch_published and producer().output_sequence == 2
        router()
        notifier()
        assert len(transport.calls) == 2 and len(state.attempts()) == 2
        producer.close()
        producer = price_alert_runtime_builder(clock=lambda: current[0], runtime_root=tmp_path)(
            producer_manifest
        )
        assert producer().output_sequence == 2
        router()
        notifier()
        assert len(transport.calls) == 2
        actual = ServingSourceAuthorityReader(
            root=notify_root,
            expected_producer_commit="b" * 40,
            expected_dataset_id="signals",
            expected_payload_kind="signal_delivery",
        )(current[0])
        api_root = tmp_path / "api-serving"
        publish_scope(api_root, runtime_projections=actual.payload.projections)
        with client_for(api_root, clock=lambda: current[0]) as client:
            result = client.get(
                "/api/v1/monitor/price-rules/runtime", headers={"x-rquant-user": "alice"}
            )
            assert result.status_code == 200 and result.json()["data"]["status_label"] == "正常"
            events = client.get(
                "/api/v1/monitor/price-rules/events", headers={"x-rquant-user": "alice"}
            )
            assert events.status_code == 200 and len(events.json()["data"]["items"]) == 2
            assert all(
                row["notifications"][0]["label"] == "已提交"
                for row in events.json()["data"]["items"]
            )
            assert "已送达" not in events.text
        current[0] = now + timedelta(seconds=120)
        publish_scope(scope_root)
        assert quote().batch_published and producer().output_sequence == 3
        disabled = SetPriceAlertRuleEnabled(
            command_id="chain-disable",
            requested_at=current[0],
            rule_id="rule/a",
            expected_version=1,
            enabled=False,
        )
        disabled_admission = PriceAlertAdmission(_service(control, tmp_path, now=current[0]))
        assert (
            disabled_admission.submit(disabled, authenticated_owner_id="alice").status
            is PageControlStatus.SUCCEEDED
        )
        publish_scope(scope_root)
        router()
        notifier()
        records = state.outbox_records()
        assert len(records) == 3 and records[-1].status is OutboxStatus.DEAD_LETTER
        with sqlite3.connect(state.path) as connection:
            cancellation = PriceAlertCancellationReceipt.model_validate_json(
                connection.execute(
                    "SELECT body_json FROM price_alert_unadmitted_cancel WHERE outbox_id=?",
                    (records[-1].outbox_id,),
                ).fetchone()[0]
            )
        assert records[-1].attempt_count == 0 and cancellation.event_id == records[-1].signal_id
        assert (
            len(transport.calls) == 2
            and len(state.attempts()) == 2
            and not state.unknown_deliveries()
        )
        print(
            json.dumps(
                {
                    "page_control_saved": True,
                    "actual_quote_gateway": True,
                    "producer_event_count": 3,
                    "original_router_spool_sequence": 3,
                    "original_notifier_cursor": state.replication_cursor().last_global_sequence,
                    "transport_calls": len(transport.calls),
                    "original_attempts": len(state.attempts()),
                    "cancelled_before_admission": True,
                    "real_source_authority_serving_api": True,
                    "source_manifest_sha256": sha256(producer_path.read_bytes()).hexdigest(),
                }
            )
        )
    finally:
        producer.close()
        router.close()
        notifier.close()
