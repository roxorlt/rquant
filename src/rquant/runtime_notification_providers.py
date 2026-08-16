"""Recipient-scoped notification providers for the isolated runtime."""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, Self
from urllib.parse import urlsplit
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
from rquant.signal_contracts import SignalAction, SignalEnvelopeFamily

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_ACTION_LABELS = {
    SignalAction.WATCH: "重点观察",
    SignalAction.B_INTENT: "买入观察",
    SignalAction.REDUCE: "减仓观察",
    SignalAction.S_INTENT: "卖出观察",
    SignalAction.CANCEL: "取消信号",
}


def _require_https_endpoint(endpoint: str) -> str:
    normalized = endpoint.strip()
    parsed = urlsplit(normalized)
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise ValueError("notification endpoint must use HTTPS")
    return normalized


class NotificationTransportDisposition(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class NotificationRecipientPreflightStatus(StrEnum):
    READY = "ready"
    MIGRATION_REQUIRED = "migration_required"


class NotificationRecipientAlias(RuntimeContractModel):
    channel: DeliveryChannel
    source_recipient_id: str
    target_recipient_ids: tuple[str, ...]

    def model_post_init(self, __context: object) -> None:
        del __context
        if not self.source_recipient_id.strip():
            raise ValueError("recipient alias source must be nonempty")
        if not self.target_recipient_ids:
            raise ValueError("recipient alias requires at least one target")
        if any(not target.strip() for target in self.target_recipient_ids):
            raise ValueError("recipient alias targets must be nonempty")
        if len(self.target_recipient_ids) != len(set(self.target_recipient_ids)):
            raise ValueError("recipient alias targets must be unique")
        if self.source_recipient_id in self.target_recipient_ids:
            raise ValueError("recipient alias source cannot also be a target")

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(
            {
                "contract": "notification-recipient-alias/v1",
                **self.model_dump(mode="python"),
            }
        )


class NotificationRecipientPreflight(RuntimeContractModel):
    status: NotificationRecipientPreflightStatus
    inferred_channels: tuple[DeliveryChannel, ...] = ()
    aliases: tuple[NotificationRecipientAlias, ...] = ()


class RecipientScopedProviderRegistry(Mapping[DeliveryChannel, NotificationProvider]):
    """Providers plus the frozen logical-to-device recipient migration contract."""

    __slots__ = ("_providers", "_recipient_ids", "_aliases", "recipient_preflight")

    def __init__(
        self,
        *,
        providers: Mapping[DeliveryChannel, NotificationProvider],
        recipient_ids: Mapping[DeliveryChannel, tuple[str, ...]],
        aliases: tuple[NotificationRecipientAlias, ...] = (),
        inferred_channels: tuple[DeliveryChannel, ...] = (),
    ) -> None:
        self._providers = MappingProxyType(dict(providers))
        self._recipient_ids = MappingProxyType(
            {
                channel: tuple(values)
                for channel, values in sorted(recipient_ids.items(), key=lambda item: item[0].value)
            }
        )
        alias_map: dict[DeliveryChannel, dict[str, tuple[str, ...]]] = {}
        for alias in aliases:
            channel_aliases = alias_map.setdefault(alias.channel, {})
            if alias.source_recipient_id in channel_aliases:
                raise ValueError("recipient alias source must be unique per channel")
            allowed = set(self._recipient_ids.get(alias.channel, ()))
            if not set(alias.target_recipient_ids) <= allowed:
                raise ValueError("recipient alias targets must have physical capabilities")
            channel_aliases[alias.source_recipient_id] = alias.target_recipient_ids
        self._aliases = MappingProxyType(
            {
                channel: MappingProxyType(values)
                for channel, values in sorted(alias_map.items(), key=lambda item: item[0].value)
            }
        )
        self.recipient_preflight = NotificationRecipientPreflight(
            status=(
                NotificationRecipientPreflightStatus.MIGRATION_REQUIRED
                if aliases
                else NotificationRecipientPreflightStatus.READY
            ),
            inferred_channels=tuple(sorted(inferred_channels, key=lambda item: item.value)),
            aliases=tuple(
                sorted(
                    aliases,
                    key=lambda item: (item.channel.value, item.source_recipient_id),
                )
            ),
        )

    def __getitem__(self, key: DeliveryChannel) -> NotificationProvider:
        return self._providers[key]

    def __iter__(self) -> Iterator[DeliveryChannel]:
        return iter(self._providers)

    def __len__(self) -> int:
        return len(self._providers)

    @property
    def recipient_ids(self) -> Mapping[DeliveryChannel, tuple[str, ...]]:
        return self._recipient_ids

    @property
    def recipient_aliases(self) -> Mapping[DeliveryChannel, Mapping[str, tuple[str, ...]]]:
        return self._aliases


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
        counts = {channel.value: len(self._credentials[channel]) for channel in self.channels}
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
        try:
            endpoint = _require_https_endpoint(endpoint)
        except ValueError:
            return NotificationTransportResult.unknown()
        factory = self._factories[channel]
        results = factory([credential], endpoint).push(title, body)
        if len(results) != 1:
            return NotificationTransportResult.unknown()
        if results[0][0] is True:
            return NotificationTransportResult.accepted()
        return NotificationTransportResult.unknown()


def _format_shanghai(value: datetime) -> str:
    localized = value.astimezone(_SHANGHAI)
    offset = localized.strftime("%z")
    return f"{localized:%Y-%m-%d %H:%M:%S} {offset[:3]}:{offset[3:]}"


def _notification_json_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _notification_json_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_notification_json_value(item) for item in value]
    return value


def format_signal_notification(signal: SignalEnvelopeFamily) -> tuple[str, str]:
    """Render a stable, readable Markdown representation of a strategy signal."""

    action_label = _ACTION_LABELS[signal.action]
    title = f"[rQuant] {signal.candidate_id} {action_label}"
    evidence = _notification_json_value(signal.evidence)
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
            raise UnknownDeliveryOutcomeError("notification delivery outcome is unknown") from None

        if not isinstance(result, NotificationTransportResult):
            raise UnknownDeliveryOutcomeError("notification delivery outcome is unknown")
        if result.disposition is NotificationTransportDisposition.REJECTED:
            raise ConfirmedDeliveryFailureError("provider rejected delivery")
        if result.disposition is NotificationTransportDisposition.UNKNOWN:
            raise UnknownDeliveryOutcomeError("notification delivery outcome is unknown")

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


CapabilityInput = RecipientNotificationCapabilities | Mapping[DeliveryChannel, Mapping[str, str]]
CapabilityLoader = Callable[[], CapabilityInput]


def build_notification_provider_loader(
    *,
    capability_loader: CapabilityLoader,
    endpoints: Mapping[DeliveryChannel, str],
    transport: NotificationTransport | None = None,
    recipient_aliases: tuple[NotificationRecipientAlias, ...] = (),
    inferred_channels: tuple[DeliveryChannel, ...] = (),
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
        return RecipientScopedProviderRegistry(
            providers=providers,
            recipient_ids={
                channel: tuple(
                    sorted(
                        capabilities._credentials[channel],
                    )
                )
                for channel in capabilities.channels
            },
            aliases=recipient_aliases,
            inferred_channels=inferred_channels,
        )

    return load


def build_environment_notification_provider_loader(
    *,
    pushdeer_recipient_id: str = "admin",
    pushplus_recipient_id: str = "admin",
    environment: Mapping[str, str] | None = None,
    transport: NotificationTransport | None = None,
) -> Callable[[], Mapping[DeliveryChannel, NotificationProvider]]:
    """Build providers from the process's already-scoped systemd capabilities."""

    recipient_ids = {
        DeliveryChannel.PUSHDEER: pushdeer_recipient_id.strip(),
        DeliveryChannel.PUSHPLUS: pushplus_recipient_id.strip(),
    }
    if any(not recipient_id for recipient_id in recipient_ids.values()):
        raise ValueError("notification recipient ids must be nonempty")

    def load() -> Mapping[DeliveryChannel, NotificationProvider]:
        source = os.environ if environment is None else environment
        capability_names = {
            DeliveryChannel.PUSHDEER: "PUSHDEER_KEYS",
            DeliveryChannel.PUSHPLUS: "PUSHPLUS_TOKENS",
        }
        endpoint_names = {
            DeliveryChannel.PUSHDEER: "PUSHDEER_ENDPOINT",
            DeliveryChannel.PUSHPLUS: "PUSHPLUS_ENDPOINT",
        }
        default_endpoints = {
            DeliveryChannel.PUSHDEER: "https://api2.pushdeer.com/message/push",
            DeliveryChannel.PUSHPLUS: "https://www.pushplus.plus/send",
        }
        recipient_names = {
            DeliveryChannel.PUSHDEER: "PUSHDEER_RECIPIENT_IDS",
            DeliveryChannel.PUSHPLUS: "PUSHPLUS_RECIPIENT_IDS",
        }
        capabilities: dict[DeliveryChannel, dict[str, str]] = {}
        endpoints: dict[DeliveryChannel, str] = {}
        aliases: list[NotificationRecipientAlias] = []
        inferred_channels: list[DeliveryChannel] = []
        for channel in DeliveryChannel:
            raw = source.get(capability_names[channel], "").strip()
            if not raw:
                continue
            credentials = [item.strip() for item in raw.split(",")]
            if any(not item for item in credentials):
                raise ValueError(f"invalid {channel.value} notification capability")
            recipient_value = source.get(recipient_names[channel], "").strip()
            if recipient_value:
                recipients = [item.strip() for item in recipient_value.split(",")]
            elif len(credentials) == 1:
                recipients = [recipient_ids[channel]]
            else:
                recipients = [
                    f"{recipient_ids[channel]}.device-{index:02d}"
                    for index in range(1, len(credentials) + 1)
                ]
                inferred_channels.append(channel)
            if any(not item for item in recipients):
                raise ValueError(f"{channel.value} recipient ids must be nonempty")
            if len(recipients) != len(credentials):
                raise ValueError(
                    f"{channel.value} recipient ids must map one-to-one to device credentials"
                )
            if len(recipients) != len(set(recipients)):
                raise ValueError(f"{channel.value} device recipient ids must be unique")
            if len(recipients) > 1 and recipient_ids[channel] not in recipients:
                aliases.append(
                    NotificationRecipientAlias(
                        channel=channel,
                        source_recipient_id=recipient_ids[channel],
                        target_recipient_ids=tuple(recipients),
                    )
                )
            capabilities[channel] = dict(zip(recipients, credentials, strict=True))
            endpoint = source.get(endpoint_names[channel], "").strip() or default_endpoints[channel]
            if transport is None:
                endpoint = _require_https_endpoint(endpoint)
            endpoints[channel] = endpoint
        if not capabilities:
            raise RuntimeError("at least one notification capability is required")
        return build_notification_provider_loader(
            capability_loader=lambda: capabilities,
            endpoints=endpoints,
            transport=transport,
            recipient_aliases=tuple(aliases),
            inferred_channels=tuple(inferred_channels),
        )()

    return load


__all__ = [
    "ExistingClientNotificationTransport",
    "NotificationTransport",
    "NotificationTransportDisposition",
    "NotificationTransportResult",
    "NotificationRecipientAlias",
    "NotificationRecipientPreflight",
    "NotificationRecipientPreflightStatus",
    "RecipientNotificationCapabilities",
    "RecipientScopedProviderRegistry",
    "RecipientScopedNotificationProvider",
    "build_environment_notification_provider_loader",
    "build_notification_provider_loader",
    "format_signal_notification",
]
