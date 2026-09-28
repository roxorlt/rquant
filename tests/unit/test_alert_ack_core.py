from __future__ import annotations

import json
import socket
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.alert_ack import alert_window_start, stable_alert_id, unique_alert_ids
from rquant.page_control import (
    AckAlert,
    PageControlClient,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    PageControlUnavailableError,
)
from rquant.page_control_service import handler_for
from rquant.signal_contracts import SignalAction, SignalEnvelope

NOW = datetime(2026, 9, 27, 2, 0, tzinfo=UTC)


def _signal() -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="2.0.0",
        parameter_fingerprint="a" * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=NOW - timedelta(seconds=2),
        available_at=NOW,
        candidate_id="600000.SH",
        action=SignalAction.WATCH,
        reason_codes=("volume",),
        evidence={"ratio": 1.8},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="d" * 40,
    )


def _monitor() -> dict[str, object]:
    return {
        "trade_date": date(2026, 9, 27),
        "trigger_time": datetime(2026, 9, 27, 10, 3, 4, 123456),
        "ts_code": "600000.SH",
        "level": "attack_break_high",
        "trigger_price": 11.5,
        "level_price": 11.0,
        "trigger_type": "break",
        "pool": "趋势",
    }


def _surge() -> dict[str, object]:
    return {
        "trade_date": "2026-09-27",
        "confirmed_at": "10:03",
        "ts_code": "300001.SZ",
        "name": "测试",
        "theme": "题材",
        "price": 11.5,
        "pct_chg": 3.1,
        "cum_amount": 12000000.0,
        "rel_cum": 1.8,
        "room_to_limit_pct": None,
        "status": "confirmed",
    }


def _command(
    command_id: str = "ack-a",
    *,
    actor_id: str = "alice",
    generation_id: str = "a" * 64,
    requested_at: datetime = NOW,
) -> AckAlert:
    return AckAlert(
        command_id=command_id,
        requested_at=requested_at,
        generation_id=generation_id,
        alert_id="b" * 64,
        actor_id=actor_id,
    )


def _consumer(outbox: PageControlOutbox, root: Path) -> PageControlConsumer:
    return PageControlConsumer(
        outbox=outbox,
        data_dir=root / "data",
        log_dir=root / "logs",
        clock=lambda: NOW,
        consumer_id="ack-consumer",
    )


def test_trigger_identity_binds_full_published_facts_and_rejects_delivery_facts() -> None:
    signal = _signal()
    signal_id = stable_alert_id("signal", signal)
    assert signal_id == stable_alert_id("signal", signal.model_dump(mode="json"))
    revised_signal = signal.model_dump(mode="json")
    revised_signal["evidence"] = {"ratio": 1.9}
    revised_signal.pop("signal_id")
    assert stable_alert_id("signal", revised_signal) != signal_id
    tampered = signal.model_dump(mode="json")
    tampered["evidence"] = {"ratio": 1.9}
    with pytest.raises(ValueError, match="signal_id|identity"):
        stable_alert_id("signal", tampered)
    with pytest.raises(ValueError, match="signal_id|identity"):
        stable_alert_id("signal", {"signal_id": signal.signal_id})

    monitor = _monitor()
    first = stable_alert_id("monitor_event", monitor)
    assert len(first) == 64
    assert stable_alert_id("monitor_event", {**monitor, "body_upper": 8.0}) == first
    assert stable_alert_id("monitor_event", {**monitor, "level_price": 11.1}) != first
    assert stable_alert_id("monitor_event", {**monitor, "trigger_price": -0.0}) == stable_alert_id(
        "monitor_event", {**monitor, "trigger_price": 0.0}
    )
    with pytest.raises(ValueError, match="finite"):
        stable_alert_id("monitor_event", {**monitor, "trigger_price": float("inf")})
    with pytest.raises(ValueError, match="time|date"):
        stable_alert_id("monitor_event", {**monitor, "trigger_time": "bad"})

    surge = _surge()
    surge_id = stable_alert_id("surge_event", surge)
    assert stable_alert_id("surge_event", {**surge, "name": "测\u8bd5"}) == surge_id
    assert stable_alert_id("surge_event", {**surge, "price": 11.6}) != surge_id
    assert stable_alert_id("surge_event", {**surge, "theme": "e\u0301"}) == stable_alert_id(
        "surge_event", {**surge, "theme": "\u00e9"}
    )
    with pytest.raises(ValueError, match="finite"):
        stable_alert_id("surge_event", {**surge, "rel_cum": float("nan")})
    with pytest.raises(ValueError, match="session"):
        stable_alert_id("surge_event", {**surge, "confirmed_at": "12:00"})
    with pytest.raises(ValueError, match="required"):
        stable_alert_id("surge_event", {**surge, "status": None})
    with pytest.raises(ValueError, match="required"):
        stable_alert_id("monitor_event", {**monitor, "ts_code": None})
    with pytest.raises(ValueError, match="code"):
        stable_alert_id("monitor_event", {**monitor, "ts_code": "bad-code"})
    with pytest.raises(ValueError, match="level"):
        stable_alert_id("monitor_event", {**monitor, "level": "unknown"})
    with pytest.raises(ValueError, match="status"):
        stable_alert_id("surge_event", {**surge, "status": "unknown"})
    with pytest.raises(ValueError, match="missing"):
        stable_alert_id("surge_event", {k: v for k, v in surge.items() if k != "status"})
    with pytest.raises(ValueError, match="not confirmable"):
        stable_alert_id("deliveries", surge)
    with pytest.raises(ValueError, match="not confirmable"):
        stable_alert_id("legacy_notification", surge)
    with pytest.raises(ValueError, match="duplicate"):
        unique_alert_ids("surge_event", [surge, dict(surge)])


def test_ack_window_uses_thirty_shanghai_calendar_days_and_activation() -> None:
    count_as_of = datetime(2026, 9, 27, 15, 0, tzinfo=UTC)
    assert alert_window_start(
        count_as_of=count_as_of,
        activated_at=datetime(2026, 8, 1, tzinfo=UTC),
    ) == datetime(2026, 8, 28, 16, 0, tzinfo=UTC)
    assert alert_window_start(
        count_as_of=count_as_of,
        activated_at=datetime(2026, 9, 25, tzinfo=UTC),
    ) == datetime(2026, 9, 25, tzinfo=UTC)


def test_monitor_writer_rows_and_serving_utc_rows_share_all_four_level_identities() -> None:
    from rquant.monitor import RealtimeQuote, WatchItem, check_attack_signals

    item = WatchItem(
        ts_code="002415.SZ",
        pool="pool2",
        limit_up_date=date(2026, 9, 24),
        body_upper=13.20,
        body_lower=11.80,
        body=1.40,
        level_40=12.36,
        level_30=12.22,
        level_20=12.08,
        stop_strong=11.80,
        stop_weak=11.52,
        reference_date=date(2026, 9, 25),
        t_high=13.00,
        t_close=12.50,
        limit_up_price_next=13.75,
    )
    quote = RealtimeQuote(
        ts_code=item.ts_code,
        price=13.58,
        low=12.60,
        open=12.88,
        high=13.60,
    )
    events = check_attack_signals(item, quote)
    assert {event["level"] for event in events} == {
        "attack_open_strength",
        "attack_break_high",
        "attack_strong_carry",
        "attack_near_limit",
    }
    writer_at = datetime(2026, 9, 25, 10, 3, 4, 123456)
    published_at = "2026-09-25T02:03:04.123456+00:00"
    for event in events:
        writer_row = {
            "trade_date": date(2026, 9, 25),
            "trigger_time": writer_at,
            "ts_code": item.ts_code,
            "pool": item.pool,
            **event,
        }
        serving_row = {
            **writer_row,
            "trade_date": "2026-09-25",
            "trigger_time": published_at,
        }
        assert stable_alert_id("monitor_event", writer_row) == stable_alert_id(
            "monitor_event", serving_row
        )


def test_monitor_local_naive_and_published_utc_timestamp_have_one_identity() -> None:
    writer_row = _monitor()
    published_row = {
        **writer_row,
        "trade_date": "2026-09-27",
        "trigger_time": "2026-09-27T02:03:04.123456+00:00",
    }
    assert stable_alert_id("monitor_event", writer_row) == stable_alert_id(
        "monitor_event", published_row
    )


def test_new_ack_is_fail_closed_until_serving_eligibility_is_verified(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outbox.activate_alert_ack(NOW - timedelta(days=1))
    service = PageControlService(outbox=outbox, consumer=_consumer(outbox, tmp_path))
    with pytest.raises(ValueError, match="eligibility|verified"):
        service.submit(_command())
    assert outbox.receipt("ack-a") is None
    assert outbox.acknowledgment("b" * 64) is None


def test_exact_old_ack_retry_can_finish_pending_command_without_revalidation(
    tmp_path: Path,
) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outbox.activate_alert_ack(NOW - timedelta(days=1))
    command = _command(generation_id="c" * 64)
    outbox.enqueue_verified_ack(command)
    service = PageControlService(outbox=outbox, consumer=_consumer(outbox, tmp_path))
    receipt = service.submit(command)
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert service.submit(command) == receipt
    with pytest.raises(ValueError, match="different|conflict"):
        service.submit(_command(generation_id="d" * 64))
    with pytest.raises(ValueError, match="eligibility|verified"):
        service.submit(_command("new-command", generation_id="c" * 64))
    assert outbox.acknowledgment(command.alert_id).confirmation_id == command.command_id


def test_same_ack_command_concurrent_enqueue_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_alert_ack(NOW - timedelta(days=1))
    command = _command()
    barrier = threading.Barrier(2)
    guard = threading.Lock()
    paused = 0

    class PausingConnection(sqlite3.Connection):
        def execute(self, sql: str, parameters: tuple[object, ...] = ()) -> sqlite3.Cursor:
            nonlocal paused
            cursor = super().execute(sql, parameters)
            if sql.strip().startswith("SELECT * FROM page_control_command WHERE command_id = ?"):
                with guard:
                    should_pause = paused < 2
                    if should_pause:
                        paused += 1
                if should_pause:
                    with suppress(threading.BrokenBarrierError):
                        barrier.wait(timeout=0.5)
            return cursor

    def connect() -> sqlite3.Connection:
        connection = sqlite3.connect(path, timeout=5.0, factory=PausingConnection)
        connection.row_factory = sqlite3.Row
        return connection

    monkeypatch.setattr(outbox, "_connect", connect)
    with ThreadPoolExecutor(max_workers=2) as workers:
        receipts = list(workers.map(outbox.enqueue_verified_ack, (command, command)))
    assert receipts[0] == receipts[1]
    assert outbox.receipt(command.command_id) == receipts[0]


def test_atomic_ack_replay_and_two_actor_competition(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_alert_ack(NOW - timedelta(days=1))
    with pytest.raises(ValueError, match="activated|earlier"):
        outbox.activate_alert_ack(NOW - timedelta(days=2))
    first = _command()
    second = _command("ack-b", actor_id="bob")
    outbox.enqueue_verified_ack(first)
    outbox.enqueue_verified_ack(second)
    claims = outbox.claim_records(limit=2, owner_id="ack-consumer", now=NOW)
    with ThreadPoolExecutor(max_workers=2) as workers:
        receipts = list(workers.map(outbox.complete_ack, claims))
    assert {receipt.status for receipt in receipts} == {PageControlStatus.SUCCEEDED}
    acknowledged = outbox.acknowledgment(first.alert_id)
    assert acknowledged is not None
    assert acknowledged.confirmation_id in {"ack-a", "ack-b"}
    assert acknowledged.actor_id in {"alice", "bob"}
    assert {receipt.result["confirmation_id"] for receipt in receipts} == {
        acknowledged.confirmation_id
    }
    assert outbox.effect("ack-a").status.value == "succeeded"
    assert outbox.effect("ack-b").status.value == "succeeded"
    restarted = PageControlOutbox(path)
    assert restarted.lookup_ack_command(first) == restarted.receipt(first.command_id)
    assert restarted.lookup_ack_command(second) == restarted.receipt(second.command_id)
    assert restarted.acknowledgment(first.alert_id) == acknowledged
    with pytest.raises(ValueError, match="different|conflict"):
        restarted.lookup_ack_command(_command(actor_id="mallory"))
    with pytest.raises(ValueError, match="different|conflict"):
        restarted.lookup_ack_command(_command(requested_at=NOW + timedelta(seconds=1)))
    with pytest.raises(ValueError, match="different|conflict"):
        restarted.enqueue_verified_ack(_command(actor_id="mallory"))


def test_ack_transaction_rollback_before_effect_and_restart_retry(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_alert_ack(NOW - timedelta(days=1))
    command = _command()
    outbox.enqueue_verified_ack(command)
    claim = outbox.claim_records(limit=1, owner_id="ack-consumer", lease_seconds=1, now=NOW)[0]
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_ack_effect BEFORE INSERT ON page_control_effect
            WHEN NEW.effect_kind = 'ack_alert'
            BEGIN SELECT RAISE(ABORT, 'simulated crash before commit'); END
            """
        )
    with pytest.raises(sqlite3.IntegrityError, match="simulated crash"):
        outbox.complete_ack(claim)
    assert outbox.acknowledgment(command.alert_id) is None
    assert outbox.effect(command.command_id) is None
    assert outbox.receipt(command.command_id).status is PageControlStatus.PROCESSING
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER fail_ack_effect")
    restarted = PageControlOutbox(path)
    retry_claim = restarted.claim_records(
        limit=1, owner_id="ack-consumer", now=NOW + timedelta(seconds=2)
    )[0]
    success = restarted.complete_ack(retry_claim)
    assert success.status is PageControlStatus.SUCCEEDED
    assert restarted.lookup_ack_command(command) == success
    assert restarted.acknowledgment(command.alert_id).confirmation_id == command.command_id


def test_readonly_lookup_refuses_other_command_kind_and_preserves_absence(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    assert outbox.lookup_ack_command(_command()) is None
    assert outbox.receipt("ack-a") is None


def _http_post(
    service: PageControlService,
    path: str,
    payload: dict[str, object],
) -> tuple[int, dict[str, object]]:
    body = json.dumps(payload).encode()
    request = (
        f"POST {path} HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n\r\n"
    ).encode() + body
    server_socket, client_socket = socket.socketpair()
    try:
        client_socket.sendall(request)
        client_socket.shutdown(socket.SHUT_WR)
        handler_for(service)(server_socket, ("127.0.0.1", 0), object())
        server_socket.close()
        response = bytearray()
        while chunk := client_socket.recv(65536):
            response.extend(chunk)
    finally:
        server_socket.close()
        client_socket.close()
    head, response_body = bytes(response).split(b"\r\n\r\n", 1)
    status = int(head.split(b" ", 2)[1])
    return status, json.loads(response_body) if b"Content-Type: application/json" in head else {}


def test_loopback_lookup_is_readonly_and_conflicts_without_leaking_receipt(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outbox.activate_alert_ack(NOW - timedelta(days=1))
    service = PageControlService(outbox=outbox, consumer=_consumer(outbox, tmp_path))
    command = _command()
    status, body = _http_post(service, "/v1/commands/lookup", command.model_dump(mode="json"))
    assert status == 200 and body == {"found": False}
    assert outbox.receipt(command.command_id) is None
    status, _ = _http_post(service, "/v1/commands", command.model_dump(mode="json"))
    assert status == 400
    assert outbox.receipt(command.command_id) is None

    outbox.enqueue_verified_ack(command)
    claim = outbox.claim_records(limit=1, owner_id="ack-consumer", now=NOW)[0]
    receipt = outbox.complete_ack(claim)
    status, body = _http_post(service, "/v1/commands/lookup", command.model_dump(mode="json"))
    assert status == 200 and body == {"found": True, "receipt": receipt.model_dump(mode="json")}
    status, body = _http_post(
        service,
        "/v1/commands/lookup",
        _command(actor_id="mallory").model_dump(mode="json"),
    )
    assert status == 409
    assert "receipt" not in body


def test_bad_future_ack_does_not_turn_successful_http_receipt_into_error(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outbox.activate_alert_ack(NOW - timedelta(days=1))
    good = _command("good-ack")
    bad = _command("bad-ack", requested_at=NOW + timedelta(minutes=6)).model_copy(
        update={"alert_id": "c" * 64}
    )
    outbox.enqueue_verified_ack(good)
    outbox.enqueue_verified_ack(bad)
    service = PageControlService(outbox=outbox, consumer=_consumer(outbox, tmp_path))
    status, body = _http_post(service, "/v1/commands", good.model_dump(mode="json"))
    assert status == 200
    assert body["status"] == "succeeded"
    assert body["result"]["confirmation_id"] == good.command_id
    assert outbox.receipt(good.command_id).status is PageControlStatus.SUCCEEDED
    assert outbox.acknowledgment(good.alert_id).confirmation_id == good.command_id
    assert outbox.receipt(bad.command_id).status is PageControlStatus.FAILED
    assert outbox.acknowledgment(bad.alert_id) is None


def test_page_client_lookup_distinguishes_absence_receipt_and_unavailability() -> None:
    command = _command()
    client = PageControlClient(lookup_transport=lambda _payload: {"found": False})
    assert client.lookup_ack_command(command) is None

    receipt = {
        "command_id": command.command_id,
        "status": "succeeded",
        "enqueued_at": NOW.isoformat(),
        "completed_at": NOW.isoformat(),
        "result": {"confirmation_id": command.command_id},
        "error": None,
    }
    client = PageControlClient(
        lookup_transport=lambda _payload: {"found": True, "receipt": receipt}
    )
    assert client.lookup_ack_command(command).result == receipt["result"]
    client = PageControlClient(lookup_transport=lambda _payload: {"found": "false"})
    with pytest.raises(ValueError, match="lookup response"):
        client.lookup_ack_command(command)
    client = PageControlClient(
        lookup_transport=lambda _payload: {
            "found": True,
            "receipt": {**receipt, "command_id": "someone-else"},
        }
    )
    with pytest.raises(ValueError, match="lookup response"):
        client.lookup_ack_command(command)

    def unavailable(_payload: dict[str, object]) -> dict[str, object]:
        raise OSError("offline")

    client = PageControlClient(lookup_transport=unavailable)
    with pytest.raises(PageControlUnavailableError):
        client.lookup_ack_command(command)
