"""Runtime builders for durable signal routing and notification delivery."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from pydantic import Field, StrictBool, StrictInt, field_validator, model_validator

from rquant.delivery_contracts import DeliveryChannel, OutboxStatus
from rquant.notification_state import NotificationServingSnapshot, NotificationStateStore
from rquant.notification_worker import (
    NotificationProvider,
    run_notification_batch,
)
from rquant.runtime_contracts import (
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_routing_policy import load_frozen_routing_policy
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.runtime_shadow_validation import ShadowStrategyBinding
from rquant.signal_bus import (
    SignalBusRoutedRecord,
    SignalBusStore,
    SignalRouteConflictError,
)
from rquant.signal_route_spool import (
    ReadonlySignalRouteSpool,
    SignalRouteSpool,
    publish_signal_bus_prefix,
)
from rquant.signal_router_runtime import (
    ReadonlyStrategyRunnerSignalSource,
    RouteSourceDescriptor,
    RunnerSignalBatch,
    RunnerSignalSource,
    SignalRouteCursorStore,
    TargetResolver,
    route_runner_signals,
)

if TYPE_CHECKING:
    from rquant.runtime_serving_authority import (
        ServingSourceAuthorityPublisher,
        ServingSourceAuthorityReader,
    )
    from rquant.runtime_serving_snapshot import SourceReadResult
    from rquant.serving_page_projection_source import SignalPageProjectionProducer

_MAX_BATCH_LIMIT = 1_000
_SIGNALS_DATASET_ID = "signals"
_ACTIVE_OUTBOX_STATUSES = frozenset({OutboxStatus.PENDING, OutboxStatus.RETRY, OutboxStatus.LEASED})


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


class SignalRouterSourceSettings(RuntimeContractModel):
    source_id: str = Field(min_length=1)
    runner_state_path: Path | None = None
    expected_strategy_registration_fingerprint: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    expected_strategy_spec_fingerprint: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    expected_evaluator_contract_fingerprint: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )

    @field_validator("runner_state_path")
    @classmethod
    def require_absolute_normalized_runner_path(
        cls,
        value: Path | None,
    ) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute() or value != Path(os.path.abspath(value)):
            raise ValueError("authority path must be absolute and normalized")
        return value

    @model_validator(mode="after")
    def validate_authority_group(self) -> SignalRouterSourceSettings:
        configured = (
            self.runner_state_path,
            self.expected_strategy_registration_fingerprint,
            self.expected_strategy_spec_fingerprint,
            self.expected_evaluator_contract_fingerprint,
        )
        if any(value is not None for value in configured) and not all(
            value is not None for value in configured
        ):
            raise ValueError("signal router manifest authority must be complete")
        return self

    @property
    def has_manifest_authority(self) -> bool:
        return self.runner_state_path is not None


class SignalRouterSettings(_SignalBusSettings):
    signal_spool_root: Path
    source_id: str | None = Field(default=None, min_length=1)
    sources: tuple[SignalRouterSourceSettings, ...] = ()
    routing_policy_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    runner_state_path: Path | None = None
    expected_strategy_registration_fingerprint: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    expected_strategy_spec_fingerprint: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    expected_evaluator_contract_fingerprint: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    routing_policy_path: Path | None = None
    batch_limit: StrictInt = Field(ge=1, le=_MAX_BATCH_LIMIT)
    paused: StrictBool = False

    @field_validator("signal_spool_root", "runner_state_path", "routing_policy_path")
    @classmethod
    def require_absolute_normalized_authority_path(
        cls,
        value: Path | None,
    ) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute() or value != Path(os.path.abspath(value)):
            raise ValueError("authority path must be absolute and normalized")
        return value

    @model_validator(mode="after")
    def validate_source_group(self) -> SignalRouterSettings:
        if self.sources and self.source_id is not None:
            raise ValueError("signal router must use either source_id or sources")
        if not self.sources and self.source_id is None:
            raise ValueError("signal router requires at least one source")
        if self.sources and any(
            value is not None
            for value in (
                self.runner_state_path,
                self.expected_strategy_registration_fingerprint,
                self.expected_strategy_spec_fingerprint,
                self.expected_evaluator_contract_fingerprint,
            )
        ):
            raise ValueError("multi-source router authority belongs inside each source")
        source_ids = [source.source_id for source in self.source_settings]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("signal router source_id values must be unique")
        if self.routing_policy_path is not None and not all(
            source.has_manifest_authority for source in self.source_settings
        ):
            raise ValueError("signal router manifest authority must be complete")
        if self.routing_policy_path is None and any(
            source.has_manifest_authority for source in self.source_settings
        ):
            raise ValueError("signal router manifest authority must be complete")
        return self

    @property
    def source_settings(self) -> tuple[SignalRouterSourceSettings, ...]:
        if self.sources:
            return self.sources
        assert self.source_id is not None
        return (
            SignalRouterSourceSettings(
                source_id=self.source_id,
                runner_state_path=self.runner_state_path,
                expected_strategy_registration_fingerprint=(
                    self.expected_strategy_registration_fingerprint
                ),
                expected_strategy_spec_fingerprint=(self.expected_strategy_spec_fingerprint),
                expected_evaluator_contract_fingerprint=(
                    self.expected_evaluator_contract_fingerprint
                ),
            ),
        )

    @property
    def has_manifest_authority(self) -> bool:
        return self.routing_policy_path is not None and all(
            source.has_manifest_authority for source in self.source_settings
        )


class NotifierSettings(RuntimeContractModel):
    signal_spool_root: Path
    notification_state_path: Path
    worker_id: str = Field(min_length=1)
    batch_limit: StrictInt = Field(ge=1, le=_MAX_BATCH_LIMIT)
    lease_seconds: StrictInt = Field(ge=1, le=3_600)
    busy_timeout_ms: StrictInt = Field(default=5_000, ge=1, le=60_000)
    retry_base_seconds: StrictInt = Field(default=5, ge=1, le=3_600)
    retry_max_seconds: StrictInt = Field(default=300, ge=1, le=86_400)
    max_attempts: StrictInt = Field(default=5, ge=1, le=100)
    pushdeer_recipient_id: str = Field(default="admin", min_length=1)
    pushplus_recipient_id: str = Field(default="admin", min_length=1)
    serving_authority_root: Path | None = None
    page_projection_database_path: Path | None = None
    page_projection_surge_live_root: Path | None = None
    page_projection_canvas_catalog_root: Path | None = None
    page_projection_canvas_receipt_root: Path | None = None
    page_projection_page_control_outbox_path: Path | None = None
    page_projection_canvas_active_key_id: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$",
    )
    page_projection_canvas_active_public_key_pem: str | None = Field(
        default=None,
        min_length=1,
        max_length=16_384,
    )
    page_projection_canvas_previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    serving_previous_producer_commit: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{40}$",
    )
    serving_history_limit: StrictInt = Field(default=1_000, ge=1, le=10_000)
    paused: StrictBool = False

    @field_validator(
        "signal_spool_root",
        "notification_state_path",
        "page_projection_database_path",
        "page_projection_surge_live_root",
        "page_projection_canvas_catalog_root",
        "page_projection_canvas_receipt_root",
        "page_projection_page_control_outbox_path",
        "serving_authority_root",
    )
    @classmethod
    def require_absolute_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute() or value != Path(os.path.abspath(value)):
            raise ValueError("notification runtime path must be absolute and normalized")
        return value

    @model_validator(mode="after")
    def validate_retry_window(self) -> NotifierSettings:
        if self.retry_max_seconds < self.retry_base_seconds:
            raise ValueError("retry_max_seconds must be at least retry_base_seconds")
        if self.page_projection_database_path is not None and self.serving_authority_root is None:
            raise ValueError("page projection database requires a signals serving authority root")
        if (
            self.page_projection_surge_live_root is not None
            and self.page_projection_database_path is None
        ):
            raise ValueError("surge live projection requires a page projection database")
        if (
            self.page_projection_canvas_catalog_root is not None
            and self.page_projection_database_path is None
        ):
            raise ValueError("canvas catalog projection requires a page projection database")
        canvas_authority = (
            self.page_projection_canvas_receipt_root,
            self.page_projection_page_control_outbox_path,
            self.page_projection_canvas_active_key_id,
            self.page_projection_canvas_active_public_key_pem,
        )
        if self.page_projection_canvas_catalog_root is not None and any(
            value is None for value in canvas_authority
        ):
            raise ValueError("canvas catalog projection requires its full public authority")
        if self.page_projection_canvas_catalog_root is None and any(
            value is not None for value in canvas_authority
        ):
            raise ValueError("canvas projection authority requires a catalog root")
        if (
            self.page_projection_canvas_active_key_id
            in self.page_projection_canvas_previous_public_key_pems
        ):
            raise ValueError("canvas projection active key cannot also be previous")
        return self

    def open_store(self) -> NotificationStateStore:
        return NotificationStateStore(
            self.notification_state_path,
            busy_timeout_ms=self.busy_timeout_ms,
            retry_base_delay=timedelta(seconds=self.retry_base_seconds),
            retry_max_delay=timedelta(seconds=self.retry_max_seconds),
            max_attempts=self.max_attempts,
        )


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
    return sum(record.status in _ACTIVE_OUTBOX_STATUSES for record in store.outbox_records())


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


def _inspect_signal_source(
    *,
    source_id: str,
    source: RunnerSignalSource,
    after_sequence: int,
) -> RouteSourceDescriptor:
    batch = RunnerSignalBatch.model_validate(
        source.read_batch(after_sequence=after_sequence, limit=0)
    )
    if batch.after_sequence != after_sequence or batch.limit != 0:
        raise SignalRouteConflictError(
            "source batch request does not match the router cursor and limit"
        )
    descriptor = batch.snapshot.descriptor
    if descriptor.source_id != source_id:
        raise ValueError("loaded signal source does not match source_id")
    return descriptor


def _read_routed_prefix_at(
    source: ReadonlySignalRouteSpool,
    *,
    after_sequence: int,
    through_sequence: int,
    observed_at: datetime,
    limit: int,
) -> tuple[SignalBusRoutedRecord, ...]:
    cutoff = normalize_aware_utc(observed_at)
    visible: list[SignalBusRoutedRecord] = []
    for record in source.routed_after_global_sequence(
        after_sequence=after_sequence,
        through_sequence=through_sequence,
        limit=limit,
    ):
        if (
            record.signal.available_at > cutoff
            or record.received_at > cutoff
            or record.receipt.routed_at > cutoff
        ):
            break
        visible.append(record)
    return tuple(visible)


def _signal_source_result(
    snapshot: NotificationServingSnapshot,
    *,
    published_at: datetime,
) -> SourceReadResult:
    from rquant.runtime_serving_snapshot import SignalDeliveryPayload, SourceReadResult
    from rquant.serving_contracts import FreshnessStatus

    status = FreshnessStatus.DEGRADED if snapshot.truncated else FreshnessStatus.FRESH
    reason = (
        f"history_limit_truncated:{snapshot.omitted_signal_count}" if snapshot.truncated else None
    )
    writer_payload = SignalDeliveryPayload(
        signals=snapshot.payload.signals,
        routes=snapshot.payload.routes,
        deliveries=snapshot.payload.deliveries,
        projections=snapshot.payload.projections,
    )
    provisional = SourceReadResult(
        dataset_id=_SIGNALS_DATASET_ID,
        generation_id="0" * 64,
        sequence=snapshot.sequence,
        event_time=snapshot.observed_at,
        published_at=published_at,
        status=status,
        reason=reason,
        payload=writer_payload,
    )
    generation_id = canonical_sha256(
        provisional.model_dump(mode="python", exclude={"generation_id"})
    )
    return SourceReadResult.model_validate(
        {
            **provisional.model_dump(mode="python"),
            "generation_id": generation_id,
        }
    )


def _publish_signal_authority(
    *,
    store: NotificationStateStore,
    publisher: ServingSourceAuthorityPublisher,
    reader: ServingSourceAuthorityReader,
    previous_reader: ServingSourceAuthorityReader | None,
    observed_at: datetime,
    history_limit: int,
) -> tuple[str, int]:
    from rquant.runtime_serving_authority import (
        ServingSourceAuthorityIntegrityError,
        ServingSourceAuthorityUnavailableError,
    )

    snapshot = store.serving_snapshot(
        observed_at=observed_at,
        history_limit=history_limit,
    )
    result = _signal_source_result(snapshot, published_at=observed_at)
    try:
        current = reader(observed_at)
    except ServingSourceAuthorityUnavailableError:
        current = None
    except ServingSourceAuthorityIntegrityError:
        if previous_reader is None:
            raise
        current = previous_reader(observed_at)
        if current.sequence > result.sequence:
            raise RuntimeError("signals serving authority is ahead of notifier state") from None
        if current.sequence == result.sequence:
            business_content = {
                "dataset_id": result.dataset_id,
                "status": result.status,
                "reason": result.reason,
                "payload": result.payload,
            }
            if canonical_sha256(business_content) != canonical_sha256(
                {
                    "dataset_id": current.dataset_id,
                    "status": current.status,
                    "reason": current.reason,
                    "payload": current.payload,
                }
            ):
                raise RuntimeError(
                    "signals serving authority content changed without a state revision"
                ) from None
            store.record_serving_authority_handoff(
                previous_producer_commit=previous_reader.expected_producer_commit,
                next_producer_commit=publisher.producer_commit,
                previous_generation_id=current.generation_id,
                business_content_hash=canonical_sha256(business_content),
                previous_sequence=current.sequence,
                observed_at=observed_at,
            )
            snapshot = store.serving_snapshot(
                observed_at=observed_at,
                history_limit=history_limit,
            )
            result = _signal_source_result(snapshot, published_at=observed_at)
    if current is not None and current.sequence == result.sequence:
        if (
            current.payload != result.payload
            or current.status is not result.status
            or current.reason != result.reason
        ):
            raise RuntimeError("signals serving authority content changed without a state revision")
        return current.generation_id, snapshot.omitted_signal_count
    pointer = publisher.publish(result)
    return pointer.generation_id, snapshot.omitted_signal_count


def signal_router_builder(
    *,
    source_loader: SignalSourceLoader | None = None,
    target_resolver: TargetResolver | None = None,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    if (source_loader is None) != (target_resolver is None):
        raise ValueError("signal router dependencies must be provided together")

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        _require_manifest(manifest, kind=RuntimeServiceKind.SIGNAL_ROUTER)
        settings = SignalRouterSettings.model_validate(dict(manifest.settings))
        injected = source_loader is not None and target_resolver is not None
        if injected and settings.has_manifest_authority:
            raise ValueError(
                "manifest authority cannot be combined with injected router dependencies"
            )
        if not injected and not settings.has_manifest_authority:
            raise ValueError("default signal router requires complete manifest authority")

        if injected:
            resolved_source_loader = source_loader
            resolved_target_resolver = target_resolver
        else:
            if settings.routing_policy_path is None:
                raise ValueError("default signal router authority is unavailable")
            authoritative_sources: dict[str, RunnerSignalSource] = {}
            for source_settings in settings.source_settings:
                if (
                    source_settings.runner_state_path is None
                    or source_settings.expected_strategy_spec_fingerprint is None
                    or source_settings.expected_evaluator_contract_fingerprint is None
                ):
                    raise ValueError("default signal router authority is unavailable")
                authoritative_sources[source_settings.source_id] = (
                    ReadonlyStrategyRunnerSignalSource(
                        source_id=source_settings.source_id,
                        path=source_settings.runner_state_path,
                        expected_strategy_spec_fingerprint=(
                            source_settings.expected_strategy_spec_fingerprint
                        ),
                        expected_evaluator_contract_fingerprint=(
                            source_settings.expected_evaluator_contract_fingerprint
                        ),
                        busy_timeout_ms=settings.busy_timeout_ms,
                    )
                )
            authoritative_policy = load_frozen_routing_policy(
                settings.routing_policy_path,
                routing_policy_fingerprint=settings.routing_policy_fingerprint,
                observed_at=clock(),
            )

            def load_authoritative_source(source_id: str) -> RunnerSignalSource:
                try:
                    return authoritative_sources[source_id]
                except KeyError as exc:
                    raise ValueError("signal source is not in manifest authority") from exc

            resolved_source_loader = load_authoritative_source
            resolved_target_resolver = authoritative_policy

        if resolved_source_loader is None or resolved_target_resolver is None:
            raise RuntimeError("signal router dependencies are unavailable")
        bus = settings.open_store()
        signal_spool = SignalRouteSpool(settings.signal_spool_root)
        cursors = SignalRouteCursorStore(
            settings.signal_bus_path,
            routing_policy_fingerprint=settings.routing_policy_fingerprint,
            busy_timeout_ms=settings.busy_timeout_ms,
        )

        def step() -> RuntimeStepResult:
            before_publish = publish_signal_bus_prefix(
                bus=bus,
                spool=signal_spool,
                limit=settings.batch_limit,
            )
            if before_publish.published_high_watermark < before_publish.source_high_watermark:
                return RuntimeStepResult(
                    input_sequence=before_publish.source_high_watermark,
                    output_sequence=before_publish.published_high_watermark,
                    processed_count=before_publish.published_count,
                    backlog_count=(
                        before_publish.source_high_watermark
                        - before_publish.published_high_watermark
                    ),
                    source_generations={
                        "signal_route_spool": before_publish.source_generation_id,
                    },
                    degraded_reasons=("signal_router:spool_catchup",),
                )
            sources: dict[str, RunnerSignalSource] = {}
            descriptors: dict[str, RouteSourceDescriptor] = {}
            cursor_sequences: dict[str, int] = {}
            source_order: dict[str, int] = {}
            input_sequence = 0
            output_sequence = 0
            generations = {
                "signal_route_spool": before_publish.source_generation_id,
            }
            observed_at = clock()
            for index, source_settings in enumerate(settings.source_settings):
                source_id = source_settings.source_id
                source = resolved_source_loader(source_id)
                current = bus.route_cursor(source_id)
                descriptor = _inspect_signal_source(
                    source_id=source_id,
                    source=source,
                    after_sequence=current.last_sequence,
                )
                bus.bind_route_source(
                    descriptor,
                    routing_policy_fingerprint=settings.routing_policy_fingerprint,
                    observed_at=observed_at,
                )
                sources[source_id] = source
                descriptors[source_id] = descriptor
                cursor_sequences[source_id] = current.last_sequence
                source_order[source_id] = index
                input_sequence += descriptor.high_watermark
                output_sequence += current.last_sequence
                generations[source_id] = descriptor.generation_id

            if settings.paused:
                return RuntimeStepResult(
                    input_sequence=input_sequence,
                    output_sequence=output_sequence,
                    backlog_count=max(0, input_sequence - output_sequence),
                    source_generations=generations,
                    degraded_reasons=("signal_router:paused",),
                )

            remaining = settings.batch_limit
            processed_count = 0
            deferred_sources: set[str] = set()
            while remaining > 0:
                eligible = sorted(
                    (
                        source_id
                        for source_id, descriptor in descriptors.items()
                        if source_id not in deferred_sources
                        and cursor_sequences[source_id] < descriptor.high_watermark
                    ),
                    key=lambda source_id: (
                        cursor_sequences[source_id],
                        source_order[source_id],
                    ),
                )
                if not eligible:
                    break
                made_progress = False
                for source_id in eligible:
                    if remaining <= 0:
                        break
                    previous_high_watermark = descriptors[source_id].high_watermark
                    summary = route_runner_signals(
                        source_id=source_id,
                        source=sources[source_id],
                        bus=bus,
                        cursors=cursors,
                        routed_at=observed_at,
                        target_resolver=resolved_target_resolver,
                        limit=1,
                    )
                    processed = summary.last_sequence - summary.started_after_sequence
                    input_sequence += summary.source_high_watermark - previous_high_watermark
                    descriptors[source_id] = descriptors[source_id].model_copy(
                        update={"high_watermark": summary.source_high_watermark}
                    )
                    generations[source_id] = summary.source_generation_id
                    if processed == 0:
                        deferred_sources.add(source_id)
                        continue
                    made_progress = True
                    cursor_sequences[source_id] = summary.last_sequence
                    output_sequence += processed
                    processed_count += processed
                    remaining -= processed
                if not made_progress:
                    break
            published = publish_signal_bus_prefix(
                bus=bus,
                spool=signal_spool,
                limit=settings.batch_limit,
            )
            return RuntimeStepResult(
                input_sequence=input_sequence,
                output_sequence=output_sequence,
                processed_count=processed_count,
                backlog_count=max(0, input_sequence - output_sequence),
                source_generations={
                    **generations,
                    "signal_route_spool": published.source_generation_id,
                },
                degraded_reasons=(
                    ("signal_router:spool_backlog",)
                    if published.published_high_watermark < published.source_high_watermark
                    else ()
                ),
            )

        return step

    return build


def build_shadow_runner_sources(
    *,
    manifest: RuntimeServiceManifest,
    bindings: Mapping[str, ShadowStrategyBinding],
) -> tuple[tuple[ShadowStrategyBinding, ReadonlyStrategyRunnerSignalSource], ...]:
    """Construct production shadow readers from the router's frozen source authority."""

    _require_manifest(manifest, kind=RuntimeServiceKind.SIGNAL_ROUTER)
    settings = SignalRouterSettings.model_validate(dict(manifest.settings))
    if not settings.has_manifest_authority:
        raise ValueError("shadow runner sources require manifest source authority")
    expected_ids = {item.source_id for item in settings.source_settings}
    if set(bindings) != expected_ids:
        raise ValueError("shadow source bindings must exactly cover router source authority")
    result = []
    for source_settings in settings.source_settings:
        if (
            source_settings.runner_state_path is None
            or source_settings.expected_strategy_spec_fingerprint is None
            or source_settings.expected_evaluator_contract_fingerprint is None
            or source_settings.expected_strategy_registration_fingerprint is None
        ):
            raise ValueError("shadow runner source authority is incomplete")
        binding = ShadowStrategyBinding.model_validate(bindings[source_settings.source_id])
        if (
            binding.definition_fingerprint
            != source_settings.expected_strategy_registration_fingerprint
        ):
            raise ValueError("shadow binding definition identity does not match runner authority")
        if (
            binding.executable_fingerprint
            != source_settings.expected_evaluator_contract_fingerprint
        ):
            raise ValueError("shadow binding executable identity does not match runner authority")
        source = ReadonlyStrategyRunnerSignalSource(
            source_id=source_settings.source_id,
            path=source_settings.runner_state_path,
            expected_strategy_spec_fingerprint=(source_settings.expected_strategy_spec_fingerprint),
            expected_evaluator_contract_fingerprint=(
                source_settings.expected_evaluator_contract_fingerprint
            ),
            busy_timeout_ms=settings.busy_timeout_ms,
        )
        strategy_id, strategy_version, _spec_fingerprint = source.strategy_identity()
        if strategy_id != binding.strategy_id or strategy_version != binding.strategy_version:
            raise ValueError("shadow binding strategy identity does not match runner authority")
        result.append((binding, source))
    return tuple(result)


def notifier_builder(
    *,
    provider_loader: ProviderLoader | None = None,
    capability_environment: Mapping[str, str] | None = None,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        _require_manifest(manifest, kind=RuntimeServiceKind.NOTIFIER)
        settings = NotifierSettings.model_validate(dict(manifest.settings))
        store = settings.open_store()
        source = ReadonlySignalRouteSpool(settings.signal_spool_root)
        authority_publisher: ServingSourceAuthorityPublisher | None = None
        authority_reader: ServingSourceAuthorityReader | None = None
        previous_authority_reader: ServingSourceAuthorityReader | None = None
        page_projection_producer: SignalPageProjectionProducer | None = None
        if settings.serving_authority_root is not None:
            from rquant.runtime_serving_authority import (
                ServingSourceAuthorityPublisher,
                ServingSourceAuthorityReader,
            )

            authority_publisher = ServingSourceAuthorityPublisher(
                root=settings.serving_authority_root,
                producer_commit=manifest.producer_commit,
                dataset_id=_SIGNALS_DATASET_ID,
                payload_kind="signal_delivery",
                clock=clock,
            )
            authority_reader = ServingSourceAuthorityReader(
                root=settings.serving_authority_root,
                expected_producer_commit=manifest.producer_commit,
                expected_dataset_id=_SIGNALS_DATASET_ID,
                expected_payload_kind="signal_delivery",
            )
            if settings.serving_previous_producer_commit is not None:
                if settings.serving_previous_producer_commit == manifest.producer_commit:
                    raise ValueError(
                        "serving previous producer commit must differ from current commit"
                    )
                previous_authority_reader = ServingSourceAuthorityReader(
                    root=settings.serving_authority_root,
                    expected_producer_commit=settings.serving_previous_producer_commit,
                    expected_dataset_id=_SIGNALS_DATASET_ID,
                    expected_payload_kind="signal_delivery",
                )
            if settings.page_projection_database_path is not None:
                from rquant.canvas_publication_receipt import Ed25519CanvasPublicationKeyring
                from rquant.serving_page_projection_source import (
                    DuckDBSignalPageProjectionSource,
                    SignalPageProjectionProducer,
                )

                canvas_keyring = None
                if settings.page_projection_canvas_catalog_root is not None:
                    canvas_keyring = Ed25519CanvasPublicationKeyring(
                        active_key_id=(settings.page_projection_canvas_active_key_id or ""),
                        active_public_key=(
                            settings.page_projection_canvas_active_public_key_pem or ""
                        ).encode("utf-8"),
                        previous_public_keys={
                            key_id: public_key.encode("utf-8")
                            for key_id, public_key in (
                                settings.page_projection_canvas_previous_public_key_pems.items()
                            )
                        },
                    )

                page_projection_producer = SignalPageProjectionProducer(
                    source=DuckDBSignalPageProjectionSource(
                        settings.page_projection_database_path,
                        surge_live_root=settings.page_projection_surge_live_root,
                        canvas_catalog_root=settings.page_projection_canvas_catalog_root,
                        canvas_receipt_root=settings.page_projection_canvas_receipt_root,
                        canvas_publication_keyring=canvas_keyring,
                        page_control_outbox=(settings.page_projection_page_control_outbox_path),
                    ),
                    store=store,
                )
        if provider_loader is None:
            from rquant.runtime_notification_providers import (
                build_environment_notification_provider_loader,
            )

            resolved_provider_loader = build_environment_notification_provider_loader(
                pushdeer_recipient_id=settings.pushdeer_recipient_id,
                pushplus_recipient_id=settings.pushplus_recipient_id,
                environment=capability_environment,
            )
        else:
            resolved_provider_loader = provider_loader

        def step() -> RuntimeStepResult:
            descriptor = source.source_descriptor()
            cursor = store.replication_cursor()
            if settings.paused:
                source_generations = {
                    "signal_route_spool": descriptor.generation_id,
                }
                degraded = ["notifier:paused"]
                if authority_publisher is not None and authority_reader is not None:
                    authority_observed_at = clock()
                    if page_projection_producer is not None:
                        page_projection_producer.publish(authority_observed_at)
                    generation_id, omitted = _publish_signal_authority(
                        store=store,
                        publisher=authority_publisher,
                        reader=authority_reader,
                        previous_reader=previous_authority_reader,
                        observed_at=authority_observed_at,
                        history_limit=settings.serving_history_limit,
                    )
                    source_generations["signals_serving_authority"] = generation_id
                    if omitted:
                        degraded.append(f"notifier:serving_history_truncated:{omitted}")
                return RuntimeStepResult(
                    input_sequence=descriptor.high_watermark,
                    output_sequence=cursor.last_global_sequence,
                    backlog_count=(
                        descriptor.high_watermark
                        - cursor.last_global_sequence
                        + _active_outbox_count(store)
                    ),
                    source_generations=source_generations,
                    degraded_reasons=tuple(degraded),
                )

            observed_at = clock()
            visible_records = _read_routed_prefix_at(
                source,
                after_sequence=cursor.last_global_sequence,
                through_sequence=descriptor.high_watermark,
                observed_at=observed_at,
                limit=settings.batch_limit,
            )
            replicated = store.replicate(
                descriptor,
                visible_records,
                observed_at=observed_at,
            )
            loaded_providers = resolved_provider_loader()
            recipient_migration = None
            inferred_channels: tuple[DeliveryChannel, ...] = ()
            from rquant.runtime_notification_providers import (
                RecipientScopedProviderRegistry,
            )

            if isinstance(loaded_providers, RecipientScopedProviderRegistry):
                recipient_migration = store.apply_recipient_alias_migrations(
                    recipient_ids=loaded_providers.recipient_ids,
                    aliases=loaded_providers.recipient_aliases,
                    observed_at=observed_at,
                )
                inferred_channels = loaded_providers.recipient_preflight.inferred_channels
            providers = _validated_providers(loaded_providers)
            summary = run_notification_batch(
                store,
                providers,
                worker_id=settings.worker_id,
                now=observed_at,
                lease_for=timedelta(seconds=settings.lease_seconds),
                limit=settings.batch_limit,
                clock=clock,
            )
            degraded: list[str] = []
            if summary.failed_count:
                degraded.append(f"notifier:confirmed_failures:{summary.failed_count}")
            if summary.unknown_count:
                degraded.append(f"notifier:unknown_outcomes:{summary.unknown_count}")
            if summary.not_attempted_count:
                degraded.append(f"notifier:not_attempted:{summary.not_attempted_count}")
            if recipient_migration is not None and recipient_migration.migrated_outbox_count:
                degraded.append(
                    "notifier:recipient_migration:"
                    f"{recipient_migration.migrated_outbox_count}->"
                    f"{recipient_migration.created_outbox_count}"
                )
            degraded.extend(
                f"notifier:recipient_ids_inferred:{channel.value}" for channel in inferred_channels
            )
            source_generations = {
                "signal_route_spool": descriptor.generation_id,
            }
            if authority_publisher is not None and authority_reader is not None:
                authority_observed_at = clock()
                if page_projection_producer is not None:
                    page_projection_producer.publish(authority_observed_at)
                generation_id, omitted = _publish_signal_authority(
                    store=store,
                    publisher=authority_publisher,
                    reader=authority_reader,
                    previous_reader=previous_authority_reader,
                    observed_at=authority_observed_at,
                    history_limit=settings.serving_history_limit,
                )
                source_generations["signals_serving_authority"] = generation_id
                if omitted:
                    degraded.append(f"notifier:serving_history_truncated:{omitted}")
            return RuntimeStepResult(
                input_sequence=descriptor.high_watermark,
                output_sequence=replicated.ended_at_sequence,
                processed_count=summary.claimed_count,
                backlog_count=(
                    descriptor.high_watermark
                    - replicated.ended_at_sequence
                    + _active_outbox_count(store)
                ),
                source_generations=source_generations,
                degraded_reasons=tuple(degraded),
            )

        return step

    return build


__all__ = [
    "NotifierSettings",
    "ProviderLoader",
    "SignalRouterSettings",
    "SignalRouterSourceSettings",
    "SignalSourceLoader",
    "build_shadow_runner_sources",
    "notifier_builder",
    "signal_router_builder",
]
