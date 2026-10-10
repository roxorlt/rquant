from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from rquant.delivery_contracts import DeliveryChannel
from rquant.notification_worker import run_notification_batch
from rquant.price_alert_runtime_store import ReadonlyPriceAlertRuntimeStore
from tests.unit.test_price_alert_event_contracts import AT
from tests.unit.test_price_alert_notification_admission import admission_fixture
from tests.unit.test_price_alert_notification_provider import FakeTransport, provider
from tests.unit.test_web_price_alert_rules import client_for


def test_price_serving_snapshot_separates_actual_price_from_legacy_and_admission(
    tmp_path: Path,
) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    peer = ReadonlyPriceAlertRuntimeStore(producer.path, activation=producer.activation)
    snapshot = state.serving_price_enabled_snapshot(
        producer=peer, activation=cap, observed_at=AT, history_limit=100, shadow=False
    )
    assert snapshot.payload.signals == snapshot.payload.routes == snapshot.payload.deliveries == ()
    tables = {row.table_name: row for row in snapshot.payload.projections}
    assert len(tables["price_alert_runtime_event"].rows) == 1
    assert tables["price_alert_runtime_attempt"].rows[0]["body_json"].find('"admission":null') >= 0
    transport = FakeTransport()
    run_notification_batch(
        state,
        {DeliveryChannel.PUSHDEER: provider(transport)},
        worker_id="worker",
        now=AT,
        clock=lambda: AT,
        lease_for=__import__("datetime").timedelta(seconds=10),
        limit=10,
        price_activation=cap,
    )
    snapshot = state.serving_price_enabled_snapshot(
        producer=peer, activation=cap, observed_at=AT, history_limit=100, shadow=False
    )
    tables = {row.table_name: row for row in snapshot.payload.projections}
    assert len(transport.calls) == 1
    assert '"admission":{' in tables["price_alert_runtime_attempt"].rows[0]["body_json"]
    assert '"succeeded":true' in tables["price_alert_runtime_attempt"].rows[0]["body_json"]
    peer.close()
    producer.close()


def published_runtime(
    tmp_path: Path,
    *,
    changed: bool = False,
    notified: bool = False,
    missing_recipient: bool = False,
):
    from rquant.serving_manual_watchlist_projection import (
        ManualWatchlistAuthoritySnapshot,
        build_manual_watchlist_projections,
    )
    from rquant.serving_price_alert_rule_projection import (
        PriceAlertRuleAuthoritySnapshot,
        build_price_alert_rule_projections,
    )
    from tests.support.web_serving_fixture import build_web_fixture

    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    peer = ReadonlyPriceAlertRuntimeStore(producer.path, activation=producer.activation)
    if missing_recipient:
        from tests.unit.test_price_alert_notification_admission import changed_authority

        cap, latest = changed_authority(tmp_path, authority, "recipient", at=AT)
        state.apply_price_alert_delivery_authority(
            latest, activation=cap, expected_revision=1, applied_at=AT
        )
    if notified:
        run_notification_batch(
            state,
            {DeliveryChannel.PUSHDEER: provider()},
            worker_id="worker",
            now=AT,
            clock=lambda: AT,
            lease_for=timedelta(seconds=10),
            limit=10,
            price_activation=cap,
        )
    snapshot = state.serving_price_enabled_snapshot(
        producer=peer, activation=cap, observed_at=AT, history_limit=100, shadow=False
    )
    rows = authority.scope.rules
    if changed:
        rows = (rows[0].model_copy(update={"version": 2, "updated_at": AT}),)
    price = PriceAlertRuleAuthoritySnapshot.create(activated_at=AT - timedelta(days=1), rows=rows)
    members = ManualWatchlistAuthoritySnapshot.create(
        activated_at=AT - timedelta(days=1), rows=authority.scope.members
    )
    root = tmp_path / "serving"
    with patch("tests.support.web_serving_fixture.FIXTURE_BUILT_AT", AT):
        build_web_fixture(
            root,
            "baseline",
            signal_projections=(
                *snapshot.payload.projections,
                *build_price_alert_rule_projections(price, observed_at=AT),
                *build_manual_watchlist_projections(members, observed_at=AT),
            ),
        )
    peer.close()
    producer.close()
    return root


@pytest.mark.parametrize("path", ["runtime", "events"])
def test_read_private_owner_only_no_query_capability(tmp_path: Path, path: str) -> None:
    root = published_runtime(tmp_path)
    url = f"/api/v1/monitor/price-rules/{path}"
    with client_for(root, clock=lambda: AT) as client:
        assert client.get(url).status_code == 401
        assert (
            client.get(url + "?owner_id=alice", headers={"x-rquant-user": "alice"}).status_code
            == 422
        )
        assert client.get(url, headers={"x-rquant-user": "alice"}).status_code == 200
        other = client.get(url, headers={"x-rquant-user": "bob"})
        assert other.status_code == 200 and other.json()["data"]["items"] == []
    with client_for(root, private=False, clock=lambda: AT) as client:
        assert client.get(url, headers={"x-rquant-user": "alice"}).status_code == 503


def test_actual_web_price_facts_precise_price_and_accepted_not_delivered(tmp_path: Path) -> None:
    root = published_runtime(tmp_path, notified=True)
    with client_for(root, clock=lambda: AT) as client:
        response = client.get(
            "/api/v1/monitor/price-rules/runtime", headers={"x-rquant-user": "alice"}
        )
        assert response.status_code == 200
        assert response.json()["data"]["status_label"] == "正常"
        assert response.json()["data"]["items"][0]["state"] == "triggered"
        response = client.get(
            "/api/v1/monitor/price-rules/events", headers={"x-rquant-user": "alice"}
        )
        event = response.json()["data"]["items"][0]
        assert event["price"] == "10.000000000000000000002"
        assert event["notifications"][0]["label"] == "已提交"
        assert "已送达" not in response.text and "private-test-key" not in response.text


@pytest.mark.parametrize("path", ["runtime", "events"])
def test_actual_generation_drift_retires_stats_with_409(tmp_path: Path, path: str) -> None:
    root = published_runtime(tmp_path, changed=True)
    with client_for(root, clock=lambda: AT) as client:
        response = client.get(
            f"/api/v1/monitor/price-rules/{path}", headers={"x-rquant-user": "alice"}
        )
        assert response.status_code == 409 and "等待运行端同步" in response.text
        assert "last_triggered_at" not in response.text


def test_future_unknown_receipt_makes_only_the_new_price_domain_unavailable(tmp_path: Path) -> None:
    from rquant.price_alert_runtime_projection import PriceAlertRuntimeState
    from rquant.runtime_notification_providers import NotificationTransportDisposition

    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    peer = ReadonlyPriceAlertRuntimeStore(producer.path, activation=producer.activation)
    transport = FakeTransport(NotificationTransportDisposition.UNKNOWN)
    run_notification_batch(
        state,
        {DeliveryChannel.PUSHDEER: provider(transport)},
        worker_id="worker",
        now=AT,
        clock=lambda: AT,
        lease_for=timedelta(seconds=10),
        limit=10,
        price_activation=cap,
    )
    import sqlite3

    with sqlite3.connect(state.path) as connection:
        connection.execute(
            "UPDATE delivery_unknown SET observed_at=?",
            ((AT + timedelta(seconds=1)).isoformat(timespec="microseconds"),),
        )
    snapshot = state.serving_price_enabled_snapshot(
        producer=peer, activation=cap, observed_at=AT, history_limit=100, shadow=False
    )
    tables = {item.table_name: item for item in snapshot.payload.projections}
    value = PriceAlertRuntimeState.model_validate_json(
        tables["price_alert_runtime_state"].rows[0]["body_json"]
    )
    assert value.availability == "unavailable" and value.event_count == 0
    assert snapshot.payload.signals == () and state.unknown_deliveries()
    peer.close()
    producer.close()


def test_read_failure_retires_price_facts_and_preserves_legacy_snapshot(
    tmp_path: Path, monkeypatch
) -> None:
    from rquant.price_alert_runtime_projection import PriceAlertRuntimeState

    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    peer = ReadonlyPriceAlertRuntimeStore(producer.path, activation=producer.activation)
    monkeypatch.setattr(
        peer,
        "runtime_snapshot",
        lambda **_: (_ for _ in ()).throw(OSError("private ledger unavailable")),
    )
    original = state.serving_snapshot(observed_at=AT, history_limit=100)
    snapshot = state.serving_price_enabled_snapshot(
        producer=peer, activation=cap, observed_at=AT, history_limit=100, shadow=False
    )
    tables = {item.table_name: item for item in snapshot.payload.projections}
    value = PriceAlertRuntimeState.model_validate_json(
        tables["price_alert_runtime_state"].rows[0]["body_json"]
    )
    assert (
        value.availability == "unavailable"
        and value.rule_count == value.event_count == value.attempt_count == 0
    )
    assert (
        snapshot.payload.signals == original.payload.signals
        and snapshot.payload.routes == original.payload.routes
    )
    peer.close()
    producer.close()


def test_fresh_evaluation_without_current_owner_target_cannot_be_normal(tmp_path: Path) -> None:
    root = published_runtime(tmp_path, missing_recipient=True)
    with client_for(root, clock=lambda: AT) as client:
        response = client.get(
            "/api/v1/monitor/price-rules/runtime", headers={"x-rquant-user": "alice"}
        )
        assert response.status_code == 200
        assert response.json()["data"]["status_label"] == "注意"
        assert response.json()["data"]["message"] == "未配置通知接收人。"
