from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest

from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    OutboxRecord,
    OutboxStatus,
)
from rquant.notification_worker import (
    ConfirmedDeliveryFailureError,
    NotificationDelivery,
    NotificationProvider,
    UnknownDeliveryOutcomeError,
)
from rquant.runtime_notification_providers import (
    ExistingClientNotificationTransport,
    NotificationTransportResult,
    RecipientNotificationCapabilities,
    build_notification_provider_loader,
    format_signal_notification,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope

NOW = datetime(2026, 7, 31, 1, 35, tzinfo=UTC)


def _signal() -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="2.1.0",
        parameter_fingerprint="a" * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=NOW - timedelta(minutes=2),
        available_at=NOW - timedelta(minutes=1),
        candidate_id="600000.SH",
        action=SignalAction.B_INTENT,
        reason_codes=("above_vwap", "same_minute_volume"),
        evidence={"volume_ratio": 2.5, "levels": {"resistance": 10.2}},
        expires_at=NOW + timedelta(minutes=10),
        producer_commit="d" * 40,
    )


def _delivery(
    *,
    recipient_id: str = "admin",
    channel: DeliveryChannel = DeliveryChannel.PUSHDEER,
) -> NotificationDelivery:
    signal = _signal()
    target = DeliveryTarget(recipient_id=recipient_id, channel=channel)
    lease_until = NOW + timedelta(minutes=2)
    record = OutboxRecord(
        signal_id=signal.signal_id,
        target=target,
        status=OutboxStatus.LEASED,
        expires_at=signal.expires_at,
        attempt_count=1,
        lease_owner="notifier-1",
        lease_until=lease_until,
        created_at=NOW - timedelta(minutes=1),
        updated_at=NOW,
    )
    return NotificationDelivery(signal=signal, record=record, deadline=lease_until)


class RecordingTransport:
    def __init__(
        self,
        result: NotificationTransportResult | BaseException,
    ) -> None:
        self.result = result
        self.calls: list[dict[str, str]] = []

    def send(
        self,
        *,
        channel: DeliveryChannel,
        endpoint: str,
        credential: str,
        title: str,
        body: str,
    ) -> NotificationTransportResult:
        self.calls.append(
            {
                "channel": channel.value,
                "endpoint": endpoint,
                "credential": credential,
                "title": title,
                "body": body,
            }
        )
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


def _providers(
    capabilities: Mapping[DeliveryChannel, Mapping[str, str]],
    transport: RecordingTransport | ExistingClientNotificationTransport,
) -> Mapping[DeliveryChannel, NotificationProvider]:
    loader = build_notification_provider_loader(
        capability_loader=lambda: RecipientNotificationCapabilities(capabilities),
        endpoints={
            DeliveryChannel.PUSHDEER: "https://pushdeer.invalid/send",
            DeliveryChannel.PUSHPLUS: "https://pushplus.invalid/send",
        },
        transport=transport,
    )
    return loader()


def test_formats_signal_as_deterministic_readable_markdown() -> None:
    title, body = format_signal_notification(_signal())

    assert title == "[rQuant] 600000.SH 买入观察"
    assert "n-shape 2.1.0" in body
    assert "2026-07-31 09:33:00 +08:00" in body
    assert "above_vwap、same_minute_volume" in body
    assert '"volume_ratio": 2.5' in body
    assert format_signal_notification(_signal()) == (title, body)


def test_pushdeer_delivery_uses_only_target_recipient_credential() -> None:
    transport = RecordingTransport(NotificationTransportResult.accepted())
    providers = _providers(
        {
            DeliveryChannel.PUSHDEER: {
                "admin": "PDU_admin_secret",
                "observer": "PDU_observer_secret",
            }
        },
        transport,
    )

    receipt = providers[DeliveryChannel.PUSHDEER].deliver(_delivery())

    assert len(transport.calls) == 1
    assert transport.calls[0]["credential"] == "PDU_admin_secret"
    assert "PDU_observer_secret" not in repr(transport.calls[0])
    assert receipt.startswith("pushdeer:")
    assert "secret" not in receipt


def test_pushplus_delivery_is_recipient_scoped_and_receipt_is_deterministic() -> None:
    transport = RecordingTransport(NotificationTransportResult.accepted())
    providers = _providers(
        {DeliveryChannel.PUSHPLUS: {"analyst": "pushplus_private_token"}},
        transport,
    )
    delivery = _delivery(
        recipient_id="analyst",
        channel=DeliveryChannel.PUSHPLUS,
    )

    first = providers[DeliveryChannel.PUSHPLUS].deliver(delivery)
    second = providers[DeliveryChannel.PUSHPLUS].deliver(delivery)

    assert first == second
    assert transport.calls[0]["credential"] == "pushplus_private_token"
    assert transport.calls[0]["channel"] == "pushplus"


def test_missing_recipient_is_confirmed_failure_without_transport_call() -> None:
    transport = RecordingTransport(NotificationTransportResult.accepted())
    providers = _providers(
        {DeliveryChannel.PUSHDEER: {"admin": "PDU_admin_secret"}},
        transport,
    )

    with pytest.raises(
        ConfirmedDeliveryFailureError,
        match="recipient is not allowed for pushdeer",
    ) as captured:
        providers[DeliveryChannel.PUSHDEER].deliver(
            _delivery(recipient_id="unknown")
        )

    assert transport.calls == []
    assert "PDU_admin_secret" not in str(captured.value)


def test_confirmed_provider_rejection_is_safe_to_retry() -> None:
    transport = RecordingTransport(NotificationTransportResult.rejected())
    providers = _providers(
        {DeliveryChannel.PUSHDEER: {"admin": "PDU_admin_secret"}},
        transport,
    )

    with pytest.raises(
        ConfirmedDeliveryFailureError,
        match="provider rejected delivery",
    ) as captured:
        providers[DeliveryChannel.PUSHDEER].deliver(_delivery())

    assert "PDU_admin_secret" not in str(captured.value)


def test_ambiguous_transport_result_and_exception_are_unknown_and_redacted() -> None:
    secret = "PDU_do_not_leak"
    for result in (
        NotificationTransportResult.unknown(),
        TimeoutError(f"timeout after sending {secret}"),
    ):
        transport = RecordingTransport(result)
        providers = _providers(
            {DeliveryChannel.PUSHDEER: {"admin": secret}},
            transport,
        )

        with pytest.raises(
            UnknownDeliveryOutcomeError,
            match="delivery outcome is unknown",
        ) as captured:
            providers[DeliveryChannel.PUSHDEER].deliver(_delivery())

        assert secret not in str(captured.value)


def test_provider_loader_only_returns_channels_with_capabilities() -> None:
    transport = RecordingTransport(NotificationTransportResult.accepted())
    providers = _providers(
        {DeliveryChannel.PUSHPLUS: {"analyst": "private_token"}},
        transport,
    )

    assert tuple(providers) == (DeliveryChannel.PUSHPLUS,)


def test_capability_repr_and_validation_never_expose_secrets() -> None:
    secret = "PDU_highly_sensitive"
    capabilities = RecipientNotificationCapabilities(
        {DeliveryChannel.PUSHDEER: {"admin": secret}}
    )

    assert secret not in repr(capabilities)
    assert capabilities.channels == (DeliveryChannel.PUSHDEER,)
    with pytest.raises(ValueError, match="credential must be nonempty") as captured:
        RecipientNotificationCapabilities(
            {DeliveryChannel.PUSHDEER: {"admin": " "}}
        )
    assert secret not in str(captured.value)


class FakePushDeerClient:
    def __init__(
        self,
        keys: list[str],
        endpoint: str,
        *,
        result: list[tuple[bool, str | None]],
    ) -> None:
        self.keys = keys
        self.endpoint = endpoint
        self.result = result

    def push(self, title: str, body: str) -> list[tuple[bool, str | None]]:
        del title, body
        return self.result


class FakePushPlusClient(FakePushDeerClient):
    pass


def test_existing_client_success_is_adapted_but_false_is_conservatively_unknown() -> None:
    success_transport = ExistingClientNotificationTransport(
        pushdeer_client_factory=lambda keys, endpoint: FakePushDeerClient(
            keys,
            endpoint,
            result=[(True, None)],
        )
    )
    success_provider = _providers(
        {DeliveryChannel.PUSHDEER: {"admin": "PDU_admin_secret"}},
        success_transport,
    )[DeliveryChannel.PUSHDEER]
    assert success_provider.deliver(_delivery()).startswith("pushdeer:")

    failed_transport = ExistingClientNotificationTransport(
        pushdeer_client_factory=lambda keys, endpoint: FakePushDeerClient(
            keys,
            endpoint,
            result=[(False, "timeout may have happened after send")],
        )
    )
    failed_provider = _providers(
        {DeliveryChannel.PUSHDEER: {"admin": "PDU_admin_secret"}},
        failed_transport,
    )[DeliveryChannel.PUSHDEER]
    with pytest.raises(UnknownDeliveryOutcomeError):
        failed_provider.deliver(_delivery())


def test_existing_pushplus_client_receives_only_the_selected_token() -> None:
    created: list[FakePushPlusClient] = []

    def create(tokens: list[str], endpoint: str) -> FakePushPlusClient:
        client = FakePushPlusClient(tokens, endpoint, result=[(True, None)])
        created.append(client)
        return client

    transport = ExistingClientNotificationTransport(pushplus_client_factory=create)
    provider = _providers(
        {
            DeliveryChannel.PUSHPLUS: {
                "analyst": "analyst_token",
                "observer": "observer_token",
            }
        },
        transport,
    )[DeliveryChannel.PUSHPLUS]

    provider.deliver(
        _delivery(recipient_id="analyst", channel=DeliveryChannel.PUSHPLUS)
    )

    assert len(created) == 1
    assert created[0].keys == ["analyst_token"]


def test_wrong_channel_is_rejected_before_transport() -> None:
    transport = RecordingTransport(NotificationTransportResult.accepted())
    provider = _providers(
        {DeliveryChannel.PUSHDEER: {"admin": "PDU_admin_secret"}},
        transport,
    )[DeliveryChannel.PUSHDEER]

    with pytest.raises(ConfirmedDeliveryFailureError, match="channel mismatch"):
        provider.deliver(_delivery(channel=DeliveryChannel.PUSHPLUS))
    assert transport.calls == []
