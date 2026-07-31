"""Recipient-scoped notification providers for the isolated runtime."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, Self
from zoneinfo import ZoneInfo

from rquant.delivery_contracts import DeliveryChannel
from rquant.notification_worker import (
    ConfirmedDeliveryFailureError,
    NotificationDelivery,
    NotificationProvider,
    UnknownDeliveryOutcomeError,
)
from rquant.notify.client import PushDeerClient, PushPlusClient
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.signal_contracts import SignalAction, SignalEnvelope

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_ACTION_LABELS = {
    SignalAction.WATCH: "重点观察",
    SignalAction.B_INTENT: "买入观察",
    SignalAction.REDUCE: "减仓观察",
    SignalAction.S_INTENT: "卖出观察",
    SignalAction.CANCEL: "取消信号",
}


class NotificationTransportDisposition(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class NotificationTransportResult(RuntimeContractModel):
    disposition: NotificationTransportDisposition

    @classmethod
    def accepted(cls) -> Self:
        return cls(disposition=NotificationTransportDisposition.ACCEPTED)

    @classmethod
    def rejected(cls) -> Self:
        return cls(disposition=NotificationTransportDisposition.REJECTED)

    @classmethod
    def unknown(cls) -> Self:
        return cls(disposition=NotificationTransportDisposition.UNKNOWN)


class RecipientNotificationCapabilities:
    """In-memory recipient credentials whose representation is always redacted."""

    __slots__ = ("_credentials",)

    def __init__(
        self,
        credentials: Mapping[DeliveryChannel, Mapping[str, str]],
    ) -> None:
        if not isinstance(credentials, Mapping):
            raise TypeError("notification capabilities must be a mapping")
        normalized: dict[DeliveryChannel, Mapping[str, str]] = {}
        for channel, recipients in credentials.items():
            if not isinstance(channel, DeliveryChannel):
                raise TypeError("capability channel must be a DeliveryChannel")
            if not isinstance(recipients, Mapping):
                raise TypeError("recipient capabilities must be a mapping")
            channel_credentials: dict[str, str] = {}
            for raw_recipient_id, raw_credential in recipients.items():
                if not isinstance(raw_recipient_id, str):
                    raise TypeError("recipient_id must be a string")
                recipient_id = raw_recipient_id.strip()
                if not recipient_id:
                    raise ValueError("recipient_id must be nonempty")
                if recipient_id in channel_credentials:
                    raise ValueError("recipient_id must be unique within a channel")
                if not isinstance(raw_credential, str):
                    raise TypeError("credential must be a string")
                credential = raw_credential.strip()
                if not credential:
                    raise ValueError("credential must be nonempty")
                channel_credentials[recipient_id] = credential
            if channel_credentials:
                normalized[channel] = MappingProxyType(channel_credentials)
        self._credentials = MappingProxyType(normalized)

    @property
    def channels(self) -> tuple[DeliveryChannel, ...]:
        return tuple(sorted(self._credentials, key=lambda channel: channel.value))

    def credential_for(
        self,
        channel: DeliveryChannel,
        recipient_id: str,
    ) -> str | None:
        recipients = self._credentials.get(channel)
        if recipients is None:
            return None
        return recipients.get(recipient_id)

    def __repr__(self) -> str:
        counts = {
            channel.value: len(self._credentials[channel]) for channel in self.channels
        }
        return f"RecipientNotificationCapabilities(counts={counts!r}, values=<redacted>)"


class NotificationTransport(Protocol):
    def send(
        self,
        *,
        channel: DeliveryChannel,
        endpoint: str,
        credential: str,
        title: str,
        body: str,
    ) -> NotificationTransportResult: ...


class _PushClient(Protocol):
    def push(self, title: str, body: str) -> list[tuple[bool, str | None]]: ...


PushClientFactory = Callable[[list[str], str], _PushClient]


class ExistingClientNotificationTransport:
    """Adapt the legacy clients while treating their collapsed failures as unknown."""

    def __init__(
        self,
        *,
        pushdeer_client_factory: PushClientFactory = PushDeerClient,
        pushplus_client_factory: PushClientFactory = PushPlusClient,
    ) -> None:
        self._factories = {
            DeliveryChannel.PUSHDEER: pushdeer_client_factory,
            DeliveryChannel.PUSHPLUS: pushplus_client_factory,
        }

    def send(
        self,
        *,
        channel: DeliveryChannel,
        endpoint: str,
        credential: str,
        title: str,
        body: str,
    ) -> NotificationTransportResult:
        factory = self._factories[channel]
        results = factory([credential], endpoint).push(title, body)
        if len(results) != 1:
            return NotificationTransportResult.unknown()
        success, _error = results[0]
        if success is True:
            return NotificationTransportResult.accepted()
        return NotificationTransportResult.unknown()


def _format_shanghai(value: datetime) -> str:
    localized = value.astimezone(_SHANGHAI)
    offset = localized.strftime("%z")
    return f"{localized:%Y-%m-%d %H:%M:%S} {offset[:3]}:{offset[3:]}"


def format_signal_notification(signal: SignalEnvelope) -> tuple[str, str]:
    """Render a stable, readable Markdown representation of a strategy signal."""

    action_label = _ACTION_LABELS[signal.action]
    title = f"[rQuant] {signal.candidate_id} {action_label}"
    evidence = signal.model_dump(mode="json")["evidence"]
    evidence_json = json.dumps(
        evidence,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        separators=(",", ": "),
    )
    body = "\n".join(
        (
            f"## {signal.candidate_id} | {action_label}",
            f"- 策略：{signal.strategy_id} {signal.strategy_version}",
            f"- 事件时间：{_format_shanghai(signal.event_time)}",
            f"- 可见时间：{_format_shanghai(signal.available_at)}",
            f"- 原因：{'、'.join(signal.reason_codes)}",
            f"- 信号 ID：`{signal.signal_id}`",
            "",
            "### 证据",
            "```json",
            evidence_json,
            "```",
        )
    )
    return title, body


class RecipientScopedNotificationProvider(NotificationProvider):
    def __init__(
        self,
        *,
        channel: DeliveryChannel,
        endpoint: str,
        capabilities: RecipientNotificationCapabilities,
        transport: NotificationTransport,
    ) -> None:
        if not endpoint.strip():
            raise ValueError("notification endpoint must be nonempty")
        self._channel = channel
        self._endpoint = endpoint.strip()
        self._capabilities = capabilities
        self._transport = transport

    def deliver(self, delivery: NotificationDelivery) -> str:
        target = delivery.record.target
        if target.channel is not self._channel:
            raise ConfirmedDeliveryFailureError("notification channel mismatch")
        credential = self._capabilities.credential_for(
            self._channel,
            target.recipient_id,
        )
        if credential is None:
            raise ConfirmedDeliveryFailureError(
                f"recipient is not allowed for {self._channel.value}"
            )

        title, body = format_signal_notification(delivery.signal)
        try:
            result = self._transport.send(
                channel=self._channel,
                endpoint=self._endpoint,
                credential=credential,
                title=title,
                body=body,
            )
        except Exception:
            raise UnknownDeliveryOutcomeError(
                "notification delivery outcome is unknown"
            ) from None

        if not isinstance(result, NotificationTransportResult):
            raise UnknownDeliveryOutcomeError(
                "notification delivery outcome is unknown"
            )
        if result.disposition is NotificationTransportDisposition.REJECTED:
            raise ConfirmedDeliveryFailureError("provider rejected delivery")
        if result.disposition is NotificationTransportDisposition.UNKNOWN:
            raise UnknownDeliveryOutcomeError(
                "notification delivery outcome is unknown"
            )

        receipt = canonical_sha256(
            {
                "contract": "runtime-notification-receipt/v1",
                "channel": self._channel,
                "recipient_id": target.recipient_id,
                "outbox_id": delivery.record.outbox_id,
                "signal_id": delivery.signal.signal_id,
                "title": title,
                "body": body,
            }
        )
        return f"{self._channel.value}:{receipt}"


CapabilityInput = RecipientNotificationCapabilities | Mapping[
    DeliveryChannel, Mapping[str, str]
]
CapabilityLoader = Callable[[], CapabilityInput]


def build_notification_provider_loader(
    *,
    capability_loader: CapabilityLoader,
    endpoints: Mapping[DeliveryChannel, str],
    transport: NotificationTransport | None = None,
) -> Callable[[], Mapping[DeliveryChannel, NotificationProvider]]:
    """Build the notifier's injected provider loader without reading a manifest."""

    endpoint_by_channel = dict(endpoints)
    if any(not isinstance(channel, DeliveryChannel) for channel in endpoint_by_channel):
        raise TypeError("endpoint mapping keys must be DeliveryChannel values")
    delivery_transport = transport or ExistingClientNotificationTransport()

    def load() -> Mapping[DeliveryChannel, NotificationProvider]:
        loaded = capability_loader()
        capabilities = (
            loaded
            if isinstance(loaded, RecipientNotificationCapabilities)
            else RecipientNotificationCapabilities(loaded)
        )
        providers: dict[DeliveryChannel, NotificationProvider] = {}
        for channel in capabilities.channels:
            endpoint = endpoint_by_channel.get(channel)
            if endpoint is None or not endpoint.strip():
                raise ValueError(f"notification endpoint missing for {channel.value}")
            providers[channel] = RecipientScopedNotificationProvider(
                channel=channel,
                endpoint=endpoint,
                capabilities=capabilities,
                transport=delivery_transport,
            )
        return MappingProxyType(providers)

    return load


__all__ = [
    "ExistingClientNotificationTransport",
    "NotificationTransport",
    "NotificationTransportDisposition",
    "NotificationTransportResult",
    "RecipientNotificationCapabilities",
    "RecipientScopedNotificationProvider",
    "build_notification_provider_loader",
    "format_signal_notification",
]
