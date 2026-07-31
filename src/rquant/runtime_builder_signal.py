"""Runtime builders for durable signal routing and notification delivery."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol

from pydantic import Field, StrictBool, StrictInt, field_validator, model_validator

from rquant.delivery_contracts import DeliveryChannel, OutboxStatus
from rquant.notification_worker import (
    NotificationProvider,
    run_notification_batch,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.signal_bus import RouteSourceDescriptor, SignalBusStore
from rquant.signal_router_runtime import (
    RunnerSignalSource,
    SignalRouteCursorStore,
    TargetResolver,
    route_runner_signals,
)

_MAX_BATCH_LIMIT = 1_000
_ACTIVE_OUTBOX_STATUSES = frozenset(
    {OutboxStatus.PENDING, OutboxStatus.RETRY, OutboxStatus.LEASED}
)


class SignalSourceLoader(Protocol):
    def __call__(self, source_id: str) -> RunnerSignalSource: ...


class ProviderLoader(Protocol):
    def __call__(self) -> Mapping[DeliveryChannel, NotificationProvider]: ...


class _SignalBusSettings(RuntimeContractModel):
    signal_bus_path: Path
    busy_timeout_ms: StrictInt = Field(default=5_000, ge=1, le=60_000)
    retry_base_seconds: StrictInt = Field(default=5, ge=1, le=3_600)
    retry_max_seconds: StrictInt = Field(default=300, ge=1, le=86_400)
    max_attempts: StrictInt = Field(default=5, ge=1, le=100)

    @field_validator("signal_bus_path")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("signal bus path must be absolute")
        return value

    @model_validator(mode="after")
    def validate_retry_window(self) -> _SignalBusSettings:
        if self.retry_max_seconds < self.retry_base_seconds:
            raise ValueError("retry_max_seconds must be at least retry_base_seconds")
        return self

    def open_store(self) -> SignalBusStore:
        return SignalBusStore(
            self.signal_bus_path,
            busy_timeout_ms=self.busy_timeout_ms,
            retry_base_delay=timedelta(seconds=self.retry_base_seconds),
            retry_max_delay=timedelta(seconds=self.retry_max_seconds),
            max_attempts=self.max_attempts,
        )


class SignalRouterSettings(_SignalBusSettings):
    source_id: str = Field(min_length=1)
    routing_policy_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    batch_limit: StrictInt = Field(ge=1, le=_MAX_BATCH_LIMIT)
    paused: StrictBool = False


class NotifierSettings(_SignalBusSettings):
    worker_id: str = Field(min_length=1)
    batch_limit: StrictInt = Field(ge=1, le=_MAX_BATCH_LIMIT)
    lease_seconds: StrictInt = Field(ge=1, le=3_600)
    paused: StrictBool = False


def _require_manifest(
    manifest: RuntimeServiceManifest,
    *,
    kind: RuntimeServiceKind,
) -> None:
    if manifest.service_kind is not kind:
        raise ValueError(f"runtime service kind must be {kind.value}")
    if manifest.plane is not RuntimeServicePlane.LIVE:
        raise ValueError(f"{kind.value} must run on the live plane")


def _active_outbox_count(store: SignalBusStore) -> int:
    return sum(
        record.status in _ACTIVE_OUTBOX_STATUSES for record in store.outbox_records()
    )


def _validated_providers(
    providers: Mapping[DeliveryChannel, NotificationProvider],
) -> dict[DeliveryChannel, NotificationProvider]:
    if not isinstance(providers, Mapping):
        raise TypeError("provider loader must return a mapping")
    validated: dict[DeliveryChannel, NotificationProvider] = {}
    for channel, provider in providers.items():
        if not isinstance(channel, DeliveryChannel):
            raise TypeError("provider mapping keys must be DeliveryChannel values")
        if not callable(getattr(provider, "deliver", None)):
            raise TypeError(f"provider for {channel.value} must implement deliver()")
        validated[channel] = provider
    return validated


def signal_router_builder(
    *,
    source_loader: SignalSourceLoader,
    target_resolver: TargetResolver,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        _require_manifest(manifest, kind=RuntimeServiceKind.SIGNAL_ROUTER)
        settings = SignalRouterSettings.model_validate(dict(manifest.settings))
        bus = settings.open_store()
        cursors = SignalRouteCursorStore(
            settings.signal_bus_path,
            routing_policy_fingerprint=settings.routing_policy_fingerprint,
            busy_timeout_ms=settings.busy_timeout_ms,
        )

        def step() -> RuntimeStepResult:
            source = source_loader(settings.source_id)
            descriptor = RouteSourceDescriptor.model_validate(source.descriptor())
            if descriptor.source_id != settings.source_id:
                raise ValueError("loaded signal source does not match source_id")
            current = bus.route_cursor(settings.source_id)
            if settings.paused:
                return RuntimeStepResult(
                    input_sequence=descriptor.high_watermark,
                    output_sequence=current.last_sequence,
                    backlog_count=max(
                        0, descriptor.high_watermark - current.last_sequence
                    ),
                    source_generations={settings.source_id: descriptor.generation_id},
                    degraded_reasons=("signal_router:paused",),
                )

            summary = route_runner_signals(
                source_id=settings.source_id,
                source=source,
                bus=bus,
                cursors=cursors,
                routed_at=clock(),
                target_resolver=target_resolver,
                limit=settings.batch_limit,
            )
            return RuntimeStepResult(
                input_sequence=descriptor.high_watermark,
                output_sequence=summary.last_sequence,
                processed_count=summary.last_sequence - summary.started_after_sequence,
                backlog_count=max(0, descriptor.high_watermark - summary.last_sequence),
                source_generations={settings.source_id: descriptor.generation_id},
            )

        return step

    return build


def notifier_builder(
    *,
    provider_loader: ProviderLoader,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        _require_manifest(manifest, kind=RuntimeServiceKind.NOTIFIER)
        settings = NotifierSettings.model_validate(dict(manifest.settings))
        store = settings.open_store()

        def step() -> RuntimeStepResult:
            descriptor = store.source_descriptor()
            if settings.paused:
                return RuntimeStepResult(
                    input_sequence=descriptor.high_watermark,
                    backlog_count=_active_outbox_count(store),
                    source_generations={"signal_bus": descriptor.generation_id},
                    degraded_reasons=("notifier:paused",),
                )

            providers = _validated_providers(provider_loader())
            summary = run_notification_batch(
                store,
                providers,
                worker_id=settings.worker_id,
                now=clock(),
                lease_for=timedelta(seconds=settings.lease_seconds),
                limit=settings.batch_limit,
                clock=clock,
            )
            observed = store.source_descriptor()
            degraded: list[str] = []
            if summary.failed_count:
                degraded.append(f"notifier:confirmed_failures:{summary.failed_count}")
            if summary.unknown_count:
                degraded.append(f"notifier:unknown_outcomes:{summary.unknown_count}")
            if summary.not_attempted_count:
                degraded.append(f"notifier:not_attempted:{summary.not_attempted_count}")
            return RuntimeStepResult(
                input_sequence=observed.high_watermark,
                processed_count=summary.claimed_count,
                backlog_count=_active_outbox_count(store),
                source_generations={"signal_bus": observed.generation_id},
                degraded_reasons=tuple(degraded),
            )

        return step

    return build


__all__ = [
    "NotifierSettings",
    "ProviderLoader",
    "SignalRouterSettings",
    "SignalSourceLoader",
    "notifier_builder",
    "signal_router_builder",
]
