from datetime import timedelta
from pathlib import Path

import pytest

from rquant.delivery_contracts import DeliveryChannel, OutboxStatus
from rquant.notification_worker import run_notification_batch
from rquant.runtime_notification_providers import (
    NotificationTransportDisposition,
    NotificationTransportResult,
    RecipientNotificationCapabilities,
    RecipientScopedNotificationProvider,
    format_price_alert_notification,
)
from tests.unit.test_price_alert_event_contracts import AT
from tests.unit.test_price_alert_notification_admission import (
    admission_fixture,
    changed_authority,
    claim,
)


class FakeTransport:
    def __init__(self, disposition=NotificationTransportDisposition.ACCEPTED):
        self.calls = []
        self.disposition = disposition

    def send(self, **facts):
        self.calls.append(facts)
        return NotificationTransportResult(disposition=self.disposition)


def provider(transport=None):
    return RecipientScopedNotificationProvider(
        channel=DeliveryChannel.PUSHDEER,
        endpoint="https://offline.invalid/send",
        capabilities=RecipientNotificationCapabilities(
            credentials={DeliveryChannel.PUSHDEER: {"alice.phone": "private-test-key"}}
        ),
        transport=transport or FakeTransport(),
    )


def test_real_provider_requires_single_fresh_commit_and_static_preparation(tmp_path: Path) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    transport = FakeTransport()
    adapter = provider(transport)
    record = claim(state, cap)
    prepared = adapter.prepare_price(state.notification_event(record.signal_id), record)
    committed = state.admit_price_alert_delivery(
        record, activation=cap, worker_id="worker", expected_revision=1, admitted_at=AT
    )
    receipt = state.price_alert_send_admission(record.outbox_id, 1)
    with pytest.raises(TypeError):
        adapter.deliver_price(prepared, receipt, store=state, record=record, now=AT)
    assert not transport.calls
    # A rejected forged capability must not destroy a genuine preparation.
    result = adapter.deliver_price(prepared, committed, store=state, record=record, now=AT)
    assert result.startswith("pushdeer:") and len(transport.calls) == 1
    with pytest.raises((TypeError, ValueError)):
        adapter.deliver_price(prepared, committed, store=state, record=record, now=AT)
    assert len(transport.calls) == 1
    producer.close()


@pytest.mark.parametrize("order", ["apply_first", "admit_first", "external_only"])
@pytest.mark.parametrize("change", ["disable", "rule_version", "member", "recipient"])
def test_actual_transport_calls_match_committed_cancellation_order(
    tmp_path: Path, monkeypatch, order: str, change: str
) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    transport = FakeTransport()
    adapter = provider(transport)
    new_cap, new = changed_authority(tmp_path, authority, change)
    if order == "apply_first":
        original = adapter.prepare_price

        def prepare(event, record):
            result = original(event, record)
            state.apply_price_alert_delivery_authority(
                new, activation=new_cap, expected_revision=1, applied_at=AT + timedelta(seconds=1)
            )
            return result

        monkeypatch.setattr(adapter, "prepare_price", prepare)
    elif order == "admit_first":
        original = adapter.deliver_price

        def deliver(prepared, admitted, **facts):
            state.apply_price_alert_delivery_authority(
                new, activation=new_cap, expected_revision=1, applied_at=AT + timedelta(seconds=1)
            )
            return original(prepared, admitted, **facts)

        monkeypatch.setattr(adapter, "deliver_price", deliver)
    # Time advances after static preparation; no network or concurrent timing luck.
    calls = 0

    def clock():
        nonlocal calls
        calls += 1
        return AT + timedelta(seconds=1)

    summary = run_notification_batch(
        state,
        {DeliveryChannel.PUSHDEER: adapter},
        worker_id="worker",
        now=AT,
        clock=clock,
        lease_for=timedelta(seconds=10),
        limit=10,
        price_activation=cap,
    )
    assert len(transport.calls) == (0 if order == "apply_first" else 1)
    assert state.outbox_records()[0].status is (
        OutboxStatus.DEAD_LETTER if order == "apply_first" else OutboxStatus.SUCCEEDED
    )
    assert summary.not_attempted_count == (1 if order == "apply_first" else 0)
    producer.close()


@pytest.mark.parametrize(
    "failure",
    [
        "before_admission_commit",
        "after_admission_commit",
        "after_provider",
        "unknown",
        "rejected",
        "invalid_receipt",
        "scope_unknown",
    ],
)
def test_worker_crash_boundaries_keep_original_unknown_and_retry(
    tmp_path: Path, monkeypatch, failure: str
) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    transport = FakeTransport(
        NotificationTransportDisposition.UNKNOWN
        if failure == "unknown"
        else NotificationTransportDisposition.REJECTED
        if failure == "rejected"
        else NotificationTransportDisposition.ACCEPTED
    )
    adapter = provider(transport)
    if failure.endswith("admission_commit"):
        monkeypatch.setattr(
            state,
            "_price_alert_failpoint",
            lambda point: (_ for _ in ()).throw(OSError("injected")) if point == failure else None,
        )
    elif failure == "after_provider":
        monkeypatch.setattr(
            state,
            "complete_success",
            lambda *args, **kwargs: (_ for _ in ()).throw(OSError("writeback lost")),
        )
    elif failure == "invalid_receipt":
        original = adapter.deliver_price

        def invalid(*args, **kwargs):
            original(*args, **kwargs)
            return False

        monkeypatch.setattr(adapter, "deliver_price", invalid)
    elif failure == "scope_unknown":
        from rquant.price_alert_runtime_projection import PriceAlertDeliveryAuthorityInput
        from rquant.price_alert_runtime_source import UnavailablePriceAlertScope

        original = adapter.prepare_price

        def unavailable(event, record):
            result = original(event, record)
            value = PriceAlertDeliveryAuthorityInput(
                scope=UnavailablePriceAlertScope(inspected_at=AT),
                policy=authority.policy,
                owner_policy_manifest_sha256=authority.owner_policy_manifest_sha256,
                delivery_enabled=True,
                inspected_at=AT,
            )
            state.apply_price_alert_delivery_authority(
                value, activation=cap, expected_revision=1, applied_at=AT
            )
            return result

        monkeypatch.setattr(adapter, "prepare_price", unavailable)
    summary = run_notification_batch(
        state,
        {DeliveryChannel.PUSHDEER: adapter},
        worker_id="worker",
        now=AT,
        clock=lambda: AT,
        lease_for=timedelta(seconds=10),
        limit=10,
        price_activation=cap,
    )
    assert len(transport.calls) == (
        0
        if failure in {"before_admission_commit", "after_admission_commit", "scope_unknown"}
        else 1
    )
    if failure in {"after_admission_commit", "after_provider", "unknown", "invalid_receipt"}:
        assert summary.unknown_count == 1
        assert state.unknown_deliveries()
        assert (
            run_notification_batch(
                state,
                {DeliveryChannel.PUSHDEER: adapter},
                worker_id="worker",
                now=AT,
                clock=lambda: AT,
                lease_for=timedelta(seconds=10),
                limit=10,
                price_activation=cap,
            ).claimed_count
            == 0
        )
    elif failure == "rejected":
        assert summary.failed_count == 1
        assert state.outbox_records()[0].next_attempt_at == AT + timedelta(seconds=5)
    else:
        assert summary.not_attempted_count == 1
        assert state.outbox_records()[0].attempt_count == 0
    producer.close()


def test_formatter_is_plain_bounded_and_preserves_exact_price() -> None:
    from tests.unit.test_price_alert_event_contracts import event

    value = event(rule_name="[名字](https://invalid)\n\x00**到价**")
    title, body = format_price_alert_notification(value)
    assert "\n" not in body and "\x00" not in body and "[" not in body and "*" not in body
    assert value.price in body and value.ts_code in body
    assert value.event_id not in body and value.owner_id not in body
    assert len((title + body).encode()) <= 1024


def test_revocation_cancels_a_future_retry_and_preserves_the_real_rejected_attempt(
    tmp_path: Path,
) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    transport = FakeTransport(NotificationTransportDisposition.REJECTED)
    adapter = provider(transport)
    run_notification_batch(
        state,
        {DeliveryChannel.PUSHDEER: adapter},
        worker_id="worker",
        now=AT,
        clock=lambda: AT,
        lease_for=timedelta(seconds=10),
        limit=10,
        price_activation=cap,
    )
    previous = state.outbox_records()[0]
    assert previous.status is OutboxStatus.RETRY and previous.attempt_count == 1
    admission = state.price_alert_send_admission(previous.outbox_id, 1)
    attempts = state.attempts()
    new_cap, new = changed_authority(tmp_path, authority, "disable")
    state.apply_price_alert_delivery_authority(
        new, activation=new_cap, expected_revision=1, applied_at=AT + timedelta(seconds=1)
    )
    cancelled = state.cancel_price_unadmitted(
        previous.outbox_id, expected_revision=2, cancelled_at=AT + timedelta(seconds=1)
    )
    assert cancelled is not None and cancelled.attempt_no_before == cancelled.attempt_no_after == 1
    assert state.outbox_records()[0].status is OutboxStatus.DEAD_LETTER
    assert (
        state.attempts() == attempts
        and state.price_alert_send_admission(previous.outbox_id, 1) == admission
    )
    run_notification_batch(
        state,
        {DeliveryChannel.PUSHDEER: adapter},
        worker_id="worker",
        now=AT + timedelta(seconds=6),
        clock=lambda: AT + timedelta(seconds=6),
        lease_for=timedelta(seconds=10),
        limit=10,
        price_activation=new_cap,
    )
    assert len(transport.calls) == 1
    producer.close()
