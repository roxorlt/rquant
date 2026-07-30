from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    OutboxAttempt,
    OutboxRecord,
    OutboxStatus,
    RouterDisposition,
    RouterReceipt,
)
from rquant.runtime_contracts import RuntimeContractModel

_SIGNAL_ID = "a" * 64
_NOW = datetime(2026, 7, 31, 2, 0, tzinfo=UTC)


def _target() -> DeliveryTarget:
    return DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER)


def _outbox_kwargs() -> dict[str, object]:
    return {
        "signal_id": _SIGNAL_ID,
        "target": _target(),
        "status": OutboxStatus.PENDING,
        "expires_at": _NOW + timedelta(hours=1),
        "attempt_count": 0,
        "next_attempt_at": _NOW + timedelta(minutes=1),
        "created_at": _NOW,
        "updated_at": _NOW,
    }


def test_delivery_target_key_is_deterministic_and_scoped_to_signal_recipient_channel() -> None:
    target = _target()

    assert target.delivery_key(_SIGNAL_ID) == _target().delivery_key(_SIGNAL_ID)
    assert target.delivery_key(_SIGNAL_ID) != target.delivery_key("b" * 64)
    assert target.delivery_key(_SIGNAL_ID) != DeliveryTarget(
        recipient_id="collaborator",
        channel=DeliveryChannel.PUSHDEER,
    ).delivery_key(_SIGNAL_ID)
    assert target.delivery_key(_SIGNAL_ID) != DeliveryTarget(
        recipient_id="admin",
        channel=DeliveryChannel.PUSHPLUS,
    ).delivery_key(_SIGNAL_ID)

    with pytest.raises(ValueError, match="signal_id"):
        target.delivery_key("invalid")


@pytest.mark.parametrize(
    "disposition",
    [RouterDisposition.ACCEPTED, RouterDisposition.DUPLICATE],
)
def test_accepted_and_duplicate_router_receipts_require_global_sequence(
    disposition: RouterDisposition,
) -> None:
    receipt = RouterReceipt(
        signal_id=_SIGNAL_ID,
        disposition=disposition,
        global_sequence=42,
        received_at=_NOW,
    )

    assert receipt.global_sequence == 42
    with pytest.raises(ValidationError, match="global_sequence"):
        RouterReceipt(
            signal_id=_SIGNAL_ID,
            disposition=disposition,
            received_at=_NOW,
        )


def test_quarantined_router_receipt_requires_reason_and_forbids_sequence() -> None:
    receipt = RouterReceipt(
        signal_id=_SIGNAL_ID,
        disposition=RouterDisposition.QUARANTINED,
        reason="unknown strategy contract",
        received_at=_NOW,
    )
    assert receipt.global_sequence is None

    with pytest.raises(ValidationError, match="reason"):
        RouterReceipt(
            signal_id=_SIGNAL_ID,
            disposition=RouterDisposition.QUARANTINED,
            received_at=_NOW,
        )
    with pytest.raises(ValidationError, match="global_sequence"):
        RouterReceipt(
            signal_id=_SIGNAL_ID,
            disposition=RouterDisposition.QUARANTINED,
            global_sequence=42,
            reason="bad envelope",
            received_at=_NOW,
        )


def test_outbox_id_is_derived_from_signal_and_target_and_verified() -> None:
    generated = OutboxRecord(**_outbox_kwargs())
    assert generated.outbox_id == generated.target.delivery_key(generated.signal_id)

    supplied = OutboxRecord(outbox_id=generated.outbox_id, **_outbox_kwargs())
    assert supplied == generated

    changed_schedule = _outbox_kwargs()
    changed_schedule["next_attempt_at"] = _NOW + timedelta(minutes=2)
    assert OutboxRecord(**changed_schedule).outbox_id == generated.outbox_id

    with pytest.raises(ValidationError, match="outbox_id"):
        OutboxRecord(outbox_id="0" * 64, **_outbox_kwargs())


def test_outbox_lease_fields_are_paired_and_only_valid_while_leased() -> None:
    leased = _outbox_kwargs()
    leased.update(
        status=OutboxStatus.LEASED,
        next_attempt_at=None,
        lease_owner="notifier-1",
        lease_until=_NOW + timedelta(minutes=5),
    )
    assert OutboxRecord(**leased).lease_owner == "notifier-1"

    missing_owner = dict(leased)
    missing_owner["lease_owner"] = None
    with pytest.raises(ValidationError, match="lease_owner|lease_until"):
        OutboxRecord(**missing_owner)

    pending_with_lease = _outbox_kwargs()
    pending_with_lease.update(
        lease_owner="notifier-1",
        lease_until=_NOW + timedelta(minutes=5),
    )
    with pytest.raises(ValidationError, match="leased"):
        OutboxRecord(**pending_with_lease)


def test_retry_requires_future_schedule_and_last_error() -> None:
    retry = _outbox_kwargs()
    retry.update(
        status=OutboxStatus.RETRY,
        attempt_count=1,
        last_error="provider timeout",
        next_attempt_at=_NOW + timedelta(minutes=2),
    )
    assert OutboxRecord(**retry).status is OutboxStatus.RETRY

    no_error = dict(retry)
    no_error["last_error"] = None
    with pytest.raises(ValidationError, match="last_error"):
        OutboxRecord(**no_error)

    no_schedule = dict(retry)
    no_schedule["next_attempt_at"] = None
    with pytest.raises(ValidationError, match="next_attempt_at"):
        OutboxRecord(**no_schedule)


@pytest.mark.parametrize("status", [OutboxStatus.EXPIRED, OutboxStatus.DEAD_LETTER])
def test_expired_and_dead_letter_records_require_error_and_are_terminal(
    status: OutboxStatus,
) -> None:
    terminal = _outbox_kwargs()
    terminal.update(
        status=status,
        next_attempt_at=None,
        last_error="delivery window exhausted",
        updated_at=_NOW + timedelta(hours=1),
    )
    assert OutboxRecord(**terminal).status is status

    no_error = dict(terminal)
    no_error["last_error"] = None
    with pytest.raises(ValidationError, match="last_error"):
        OutboxRecord(**no_error)

    scheduled = dict(terminal)
    scheduled["next_attempt_at"] = _NOW + timedelta(minutes=1)
    with pytest.raises(ValidationError, match="terminal|next_attempt_at"):
        OutboxRecord(**scheduled)


def test_succeeded_record_is_terminal() -> None:
    succeeded = _outbox_kwargs()
    succeeded.update(status=OutboxStatus.SUCCEEDED, next_attempt_at=None)
    assert OutboxRecord(**succeeded).status is OutboxStatus.SUCCEEDED

    succeeded["next_attempt_at"] = _NOW + timedelta(minutes=1)
    with pytest.raises(ValidationError, match="terminal|next_attempt_at"):
        OutboxRecord(**succeeded)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("expires_at", _NOW),
        ("updated_at", _NOW - timedelta(seconds=1)),
        ("next_attempt_at", _NOW - timedelta(seconds=1)),
    ],
)
def test_outbox_enforces_time_ordering(field: str, value: datetime) -> None:
    kwargs = _outbox_kwargs()
    kwargs[field] = value

    with pytest.raises(ValidationError, match=field):
        OutboxRecord(**kwargs)


def test_active_outbox_cannot_remain_pending_after_expiry() -> None:
    kwargs = _outbox_kwargs()
    kwargs.update(
        updated_at=_NOW + timedelta(hours=1),
        next_attempt_at=None,
    )

    with pytest.raises(ValidationError, match="active outbox.*expires_at"):
        OutboxRecord(**kwargs)


def test_outbox_attempt_requires_exactly_receipt_or_error_and_completion_order() -> None:
    success = OutboxAttempt(
        outbox_id="b" * 64,
        attempt_no=1,
        started_at=_NOW,
        completed_at=_NOW + timedelta(seconds=1),
        success=True,
        provider_receipt="pushdeer-message-1",
    )
    assert success.provider_receipt == "pushdeer-message-1"

    failure = OutboxAttempt(
        outbox_id="b" * 64,
        attempt_no=2,
        started_at=_NOW,
        completed_at=_NOW + timedelta(seconds=1),
        success=False,
        error="timeout",
    )
    assert failure.error == "timeout"

    with pytest.raises(ValidationError, match="provider_receipt|error"):
        OutboxAttempt(
            outbox_id="b" * 64,
            attempt_no=3,
            started_at=_NOW,
            completed_at=_NOW + timedelta(seconds=1),
            success=True,
            error="contradiction",
        )
    with pytest.raises(ValidationError, match="completed_at"):
        OutboxAttempt(
            outbox_id="b" * 64,
            attempt_no=4,
            started_at=_NOW,
            completed_at=_NOW - timedelta(seconds=1),
            success=False,
            error="clock moved backwards",
        )


def test_delivery_contracts_are_frozen_reject_unknown_and_normalize_utc() -> None:
    receipt = RouterReceipt(
        signal_id=_SIGNAL_ID,
        disposition=RouterDisposition.ACCEPTED,
        global_sequence=1,
        received_at=datetime(
            2026,
            7,
            31,
            10,
            0,
            tzinfo=timezone(timedelta(hours=8)),
        ),
    )
    assert isinstance(receipt, RuntimeContractModel)
    assert receipt.received_at == _NOW
    assert receipt.received_at.tzinfo is UTC

    with pytest.raises(ValidationError):
        receipt.reason = "changed"
    with pytest.raises(ValidationError):
        DeliveryTarget(recipient_id="admin", channel="pushdeer", unknown=True)


def test_delivery_enum_values_are_stable() -> None:
    assert [channel.value for channel in DeliveryChannel] == ["pushdeer", "pushplus"]
    assert [status.value for status in OutboxStatus] == [
        "pending",
        "leased",
        "retry",
        "succeeded",
        "expired",
        "dead_letter",
    ]
