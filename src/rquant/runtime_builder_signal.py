"""Runtime builders for durable signal routing and notification delivery."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from loguru import logger
from pydantic import Field, StrictBool, StrictInt, field_validator, model_validator

from rquant.condition_alert_runtime_contracts import ConditionAlertActivationSettings
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxStatus
from rquant.formula_pool_serving_projection import FormulaPoolServingConfig
from rquant.notification_state import NotificationServingSnapshot, NotificationStateStore
from rquant.notification_worker import (
    NotificationProvider,
    run_notification_batch,
)
from rquant.price_alert_runtime_contracts import PriceAlertActivationSettings
from rquant.runtime_builder_condition_alert import (
    ConditionAlertPeerSettings,
    apply_condition_role_scope,
    open_condition_role_peer,
    route_condition_role,
)
from rquant.runtime_builder_price_alert import (
    PriceAlertPeerSettings,
    apply_price_role_scope,
    open_price_role_peer,
    route_price_role,
)
from rquant.runtime_contracts import (
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_generation_lineage import (
    RuntimeGenerationLineageError,
    load_runtime_generation_tree,
    previous_strategy_spec_generations,
    producer_commit_lineage,
    strategy_runner_identity_lineage_for_instance,
)
from rquant.runtime_peer_artifacts import DeferredPeerArtifact
from rquant.runtime_routing_policy import load_frozen_routing_policy
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.runtime_shadow_validation import ShadowStrategyBinding
from rquant.screen.intraday_source import IntradaySourceConfig
from rquant.signal_bus import (
    SignalBusRoutedRecord,
    SignalBusStore,
    SignalRouteConflictError,
)
from rquant.notifier_operator import MonitorControlReadSettings, read_monitor_control_state
from rquant.signal_route_spool import (
    ReadonlyNotificationEventRouteSpool,
    SignalRouteSpool,
    publish_mixed_notification_bus_prefix,
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
    from rquant.price_alert_runtime_contracts import PriceAlertRuntimeActivation
    from rquant.price_alert_runtime_store import ReadonlyPriceAlertRuntimeStore
    from rquant.runtime_serving_authority import (
        ServingSourceAuthorityPublisher,
        ServingSourceAuthorityReader,
    )
    from rquant.runtime_serving_snapshot import SourceReadResult
    from rquant.screen.intraday_source import IntradayScreenProjectionSource
    from rquant.serving_page_projection_source import SignalPageProjectionProducer

_MAX_BATCH_LIMIT = 1_000
_BUS_PREFIX_LINK_MIN_INTERVAL_SECONDS = 60.0
_SIGNALS_DATASET_ID = "signals"
_ACTIVE_OUTBOX_STATUSES = frozenset({OutboxStatus.PENDING, OutboxStatus.RETRY, OutboxStatus.LEASED})


def build_intraday_page_source(
    config: IntradaySourceConfig,
    *,
    manifest: RuntimeServiceManifest,
    control_root: Path,
) -> IntradayScreenProjectionSource:
    from rquant.screen.intraday_source import IntradayScreenProjectionSource

    trusted = next(
        (
            item
            for item in config.schema_gate.registry.consumers
            if item.consumer_id == config.schema_gate.consumer_id
        ),
        None,
    )
    if (
        trusted is None
        or trusted.service_id != manifest.service_id
        or config.schema_gate.consumer_commit != manifest.producer_commit
        or config.cursor_root != control_root / "intraday-screen-cursors"
    ):
        raise ValueError("intraday publisher capability or cursor ownership changed")
    if config.quote_source is not None:
        quote_gate = config.quote_source.schema_gate
        trusted_quote = next(
            (
                item
                for item in quote_gate.registry.consumers
                if item.consumer_id == quote_gate.consumer_id
            ),
            None,
        )
        if (
            trusted_quote is None
            or trusted_quote.service_id != manifest.service_id
            or quote_gate.consumer_commit != manifest.producer_commit
        ):
            raise ValueError("quote publisher consumer capability changed")
    return IntradayScreenProjectionSource(config)


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

    def open_store(
        self,
        *,
        previous_generation_of_strategy_spec: Mapping[str, str] | None = None,
    ) -> SignalBusStore:
        return SignalBusStore(
            self.signal_bus_path,
            busy_timeout_ms=self.busy_timeout_ms,
            retry_base_delay=timedelta(seconds=self.retry_base_seconds),
            retry_max_delay=timedelta(seconds=self.retry_max_seconds),
            max_attempts=self.max_attempts,
            previous_generation_of_strategy_spec=previous_generation_of_strategy_spec,
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
    condition_alert_runtime: ConditionAlertActivationSettings | None = None
    condition_alert_runtime_manifest_path: Path | None = None
    condition_alert_peer: ConditionAlertPeerSettings | None = None
    price_alert_runtime: PriceAlertActivationSettings | None = None
    price_alert_runtime_manifest_path: Path | None = None
    price_alert_peer: PriceAlertPeerSettings | None = None
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
        if any(
            value is not None
            for value in (
                self.price_alert_runtime,
                self.price_alert_runtime_manifest_path,
                self.price_alert_peer,
            )
        ) and not all(
            value is not None
            for value in (
                self.price_alert_runtime,
                self.price_alert_runtime_manifest_path,
                self.price_alert_peer,
            )
        ):
            raise ValueError("price router authority must be complete")
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
    monitor_control: MonitorControlReadSettings | None = None
    merge_enabled: StrictBool = False
    merge_owner_id: str | None = Field(default=None, min_length=1, max_length=128)
    condition_alert_runtime: ConditionAlertActivationSettings | None = None
    condition_alert_runtime_manifest_path: Path | None = None
    condition_alert_peer: ConditionAlertPeerSettings | None = None
    price_alert_runtime: PriceAlertActivationSettings | None = None
    price_alert_runtime_manifest_path: Path | None = None
    price_alert_peer: PriceAlertPeerSettings | None = None
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
    page_projection_user_presets_root: Path | None = None
    page_projection_canvas_receipt_root: Path | None = None
    page_projection_page_control_outbox_path: Path | None = None
    page_projection_formula_pool_config: FormulaPoolServingConfig | None = None
    page_projection_intraday: IntradaySourceConfig | None = None
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
    #: An operator naming the exact generation whose signals pointer this one takes over.
    #: It **wins over the lineage** #260 gave this role: a configured takeover is an
    #: instruction to take the pointer and record having done so -- it writes a
    #: `record_serving_authority_handoff` row, advances the sequence and republishes the
    #: pointer under this commit -- while the lineage silently accepts a carried pointer
    #: and leaves it as it is. Letting the lineage answer first would swallow the
    #: instruction along with its audit row (package R review SF-1), so when this is set
    #: the primary reader is not given the lineage predicate at all and the takeover
    #: branch is the one that runs. Unset -- which is every production profile -- the
    #: lineage is what answers.
    #:
    #: **The other face of that: setting this narrows what is accepted.** Because the
    #: primary reader loses the lineage entirely, a commit named here that is *not* the
    #: one on disk fails the round closed with `ServingSourceAuthorityIntegrityError`,
    #: even where the pointer on disk is a generation this runtime root installed and the
    #: lineage would have carried it (package S review SF-C). That is deliberate: an
    #: operator naming X while the pointer says Y is the case where quietly accepting Y on
    #: ancestry would swallow the instruction a second time, and a role that degrades
    #: loudly is the one an operator can see. Set this only to the commit actually on
    #: disk, and unset it once the takeover has happened.
    serving_previous_producer_commit: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{40}$",
    )
    serving_history_limit: StrictInt = Field(default=1_000, ge=1, le=10_000)
    #: The emergency stop. A paused notifier replicates nothing, claims nothing and
    #: delivers nothing; it still publishes the `signals` serving authority, built from
    #: whatever its store already holds.
    paused: StrictBool = False
    #: #281, shadow mode: the live branch, with the transport replaced. Everything a live
    #: notifier does happens -- replication, the recipient preflight and alias migration,
    #: the leased batch, the attempt rows, the serving authority -- and no byte reaches
    #: PushDeer or PushPlus. This is not a second kind of pause: `paused` stops the work,
    #: `suppress_delivery` only stops the send.
    suppress_delivery: StrictBool = False

    @field_validator(
        "signal_spool_root",
        "notification_state_path",
        "page_projection_database_path",
        "page_projection_surge_live_root",
        "page_projection_canvas_catalog_root",
        "page_projection_user_presets_root",
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
        if any(
            value is not None
            for value in (
                self.price_alert_runtime,
                self.price_alert_runtime_manifest_path,
                self.price_alert_peer,
            )
        ) and not all(
            value is not None
            for value in (
                self.price_alert_runtime,
                self.price_alert_runtime_manifest_path,
                self.price_alert_peer,
            )
        ):
            raise ValueError("price notifier authority must be complete")
        if self.price_alert_peer is not None and (
            self.batch_limit > 100
            or self.max_attempts != 5
            or self.retry_base_seconds != 5
            or self.retry_max_seconds != 300
        ):
            raise ValueError("price notifier requires the frozen batch and retry bounds")
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
        if self.page_projection_user_presets_root is not None and (
            self.page_projection_database_path is None
            or self.page_projection_page_control_outbox_path is None
        ):
            raise ValueError("pool projection requires a database and PageControl audit")
        if self.page_projection_formula_pool_config is not None and (
            self.page_projection_database_path is None
            or self.serving_authority_root is None
            or self.page_projection_page_control_outbox_path is None
        ):
            raise ValueError(
                "formula pool projection requires a database, signals authority "
                "and PageControl audit"
            )
        canvas_authority = (
            self.page_projection_canvas_receipt_root,
            self.page_projection_canvas_active_key_id,
            self.page_projection_canvas_active_public_key_pem,
        )
        if self.page_projection_canvas_catalog_root is not None and (
            any(value is None for value in canvas_authority)
            or self.page_projection_page_control_outbox_path is None
        ):
            raise ValueError("canvas catalog projection requires its full public authority")
        if self.page_projection_canvas_catalog_root is None and any(
            value is not None for value in canvas_authority
        ):
            raise ValueError("canvas projection authority requires a catalog root")
        if (
            self.page_projection_page_control_outbox_path is not None
            and self.page_projection_canvas_catalog_root is None
            and self.page_projection_user_presets_root is None
            and self.page_projection_formula_pool_config is None
        ):
            raise ValueError("PageControl audit requires a canvas or pool projection")
        if (
            self.page_projection_canvas_active_key_id
            in self.page_projection_canvas_previous_public_key_pems
        ):
            raise ValueError("canvas projection active key cannot also be previous")
        return self

    @model_validator(mode="after")
    def merge_budget_and_owner(self) -> NotifierSettings:
        if self.merge_enabled and (
            self.merge_owner_id is None or self.batch_limit > 100 or self.max_attempts > 5
            or self.retry_base_seconds != 5 or self.retry_max_seconds != 300
        ):
            raise ValueError("notification merge requires an owner and original attempt/batch/retry limits")
        if not self.merge_enabled and self.merge_owner_id is not None:
            raise ValueError("merge owner requires explicit notification merge activation")
        return self

    def open_store(self, *, merge_binding: object | None = None) -> NotificationStateStore:
        from rquant.delivery_contracts import NotificationMergeBinding

        if self.merge_enabled:
            if (type(merge_binding) is not NotificationMergeBinding
                    or merge_binding.owner_id != self.merge_owner_id
                    or merge_binding.mode != ("shadow" if self.suppress_delivery else "live")):
                raise ValueError("notification merge requires its actual installed runtime binding")
        elif merge_binding is not None:
            raise ValueError("disabled notification merge cannot receive a binding")
        return NotificationStateStore(
            self.notification_state_path,
            busy_timeout_ms=self.busy_timeout_ms,
            retry_base_delay=timedelta(seconds=self.retry_base_seconds),
            retry_max_delay=timedelta(seconds=self.retry_max_seconds),
            max_attempts=self.max_attempts,
            merge_binding=merge_binding,
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


def _shadow_providers(
    providers: Mapping[DeliveryChannel, NotificationProvider],
) -> Mapping[DeliveryChannel, NotificationProvider]:
    """Every channel's provider replaced by the suppressing one, the registry kept.

    It is the **provider** that is swapped, not the transport under it. Replacing the
    transport would mean handing one to `build_environment_notification_provider_loader`,
    and a loader given an external transport skips `_require_https_endpoint`
    (`runtime_notification_providers.py`), so the endpoint check the production loader
    performs would stop happening in shadow -- less faithful, not more. The heartbeat
    reason keeps the name `notifier:shadow_transport`, which is what an operator reads.

    #281, risk (g): what the loader built stays built. The credential is read, the
    recipient ids are resolved, the alias migration contract and the preflight verdict are
    the real ones -- a `RecipientScopedProviderRegistry` in, a `RecipientScopedProviderRegistry`
    out, carrying the same `recipient_ids`, aliases and inferred channels -- so switching a
    notifier from shadow to live changes where the bytes go and nothing else.
    """

    from rquant.runtime_notification_providers import (
        RecipientScopedProviderRegistry,
        SuppressedNotificationProvider,
    )

    suppressed = SuppressedNotificationProvider()
    if not isinstance(providers, RecipientScopedProviderRegistry):
        return {channel: suppressed for channel in providers}
    return RecipientScopedProviderRegistry(
        providers={channel: suppressed for channel in providers},
        recipient_ids=dict(providers.recipient_ids),
        aliases=providers.recipient_preflight.aliases,
        inferred_channels=providers.recipient_preflight.inferred_channels,
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


def _loaded_notification_targets(
    providers: Mapping[DeliveryChannel, NotificationProvider],
) -> tuple[DeliveryTarget, ...] | None:
    from rquant.runtime_notification_providers import (
        RecipientNotificationCapabilities, RecipientScopedNotificationProvider,
        RecipientScopedProviderRegistry,
    )

    if type(providers) is not RecipientScopedProviderRegistry:
        return None
    targets = []
    for channel, recipients in providers.recipient_ids.items():
        provider = providers.get(channel)
        if (type(provider) is not RecipientScopedNotificationProvider
                or type(provider._capabilities) is not RecipientNotificationCapabilities
                or provider._channel is not channel):
            return None
        for recipient in recipients:
            if provider._capabilities.credential_for(channel, recipient) is None:
                return None
            targets.append(DeliveryTarget(channel=channel, recipient_id=recipient))
    # The original alias migration admits the logical receiver only when every
    # corresponding physical receiver has a loaded original credential.
    for alias in providers.recipient_preflight.aliases:
        if not all(DeliveryTarget(channel=alias.channel, recipient_id=value) in targets
                   for value in alias.target_recipient_ids):
            return None
        targets.append(DeliveryTarget(channel=alias.channel, recipient_id=alias.source_recipient_id))
    return tuple(sorted(set(targets), key=lambda row: (row.channel.value, row.recipient_id)))


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
    source: ReadonlyNotificationEventRouteSpool,
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
            (record.event.available_at if hasattr(record, "event") else record.signal.available_at)
            > cutoff
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
    from rquant.serving_read_models import ServingProjectionPayload

    status = FreshnessStatus.DEGRADED if snapshot.truncated else FreshnessStatus.FRESH
    reason = (
        f"history_limit_truncated:{snapshot.omitted_signal_count}" if snapshot.truncated else None
    )
    projections = snapshot.payload.projections
    if snapshot.signal_observed_prefix is not None:
        receipt = snapshot.signal_observed_prefix
        projections += (
            ServingProjectionPayload(
                table_name="signal_observed_prefix",
                available_at=receipt.source_inspected_at,
                rows=(receipt.model_dump(mode="json"),),
            ),
        )
    writer_payload = SignalDeliveryPayload(
        signals=snapshot.payload.signals,
        routes=snapshot.payload.routes,
        deliveries=snapshot.payload.deliveries,
        projections=projections,
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
    price_peer: ReadonlyPriceAlertRuntimeStore | None = None,
    price_activation: PriceAlertRuntimeActivation | None = None,
    price_shadow: bool = False,
    price_paused: bool = False,
    condition_peer: object | None = None,
    condition_activation: object | None = None,
) -> tuple[str, int]:
    from rquant.runtime_serving_authority import (
        ServingSourceAuthorityIntegrityError,
        ServingSourceAuthorityUnavailableError,
    )

    def read_snapshot() -> NotificationServingSnapshot:
        if condition_peer is not None:
            return store.serving_condition_enabled_snapshot(
                condition_producer=condition_peer,
                condition_activation=condition_activation,
                observed_at=observed_at,
                history_limit=history_limit,
                price_producer=price_peer,
                price_activation=price_activation,
                shadow=price_shadow,
            )
        if price_peer is not None:
            return store.serving_price_enabled_snapshot(
                producer=price_peer,
                activation=price_activation,
                observed_at=observed_at,
                history_limit=history_limit,
                shadow=price_shadow,
            )
        return store.serving_snapshot(observed_at=observed_at, history_limit=history_limit)

    if price_paused and price_peer is not None:
        try:
            prior = reader(observed_at)
        except ServingSourceAuthorityUnavailableError:
            prior = None
        if prior is not None:
            return prior.generation_id, 0
    snapshot = read_snapshot()
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
            snapshot = read_snapshot()
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


def _strategy_service_id(runner_state_path: Path | None, runtime_root: Path | None) -> str | None:
    """`live/strategies/<instance>/runner.sqlite3` -> the strategy service it belongs to."""

    if runner_state_path is None or runtime_root is None:
        return None
    try:
        tree = load_runtime_generation_tree(runtime_root)
    except RuntimeGenerationLineageError:
        return None
    return tree.service_id_for_instance(runner_state_path.parent.name)


def _runner_source_opener(
    source_settings: SignalRouterSourceSettings,
    *,
    busy_timeout_ms: int,
    runtime_root: Path | None = None,
) -> Callable[[], RunnerSignalSource]:
    """The router's own reader for one strategy's runner database, unchanged."""

    runner_state_path = source_settings.runner_state_path
    spec_fingerprint = source_settings.expected_strategy_spec_fingerprint
    evaluator_fingerprint = source_settings.expected_evaluator_contract_fingerprint
    assert runner_state_path is not None
    assert spec_fingerprint is not None
    assert evaluator_fingerprint is not None

    # `live/strategies/<instance>/runner.sqlite3`: the instance directory is all the
    # router knows about whose database this is, and the current generation's basis maps
    # it back to a service id so a runner still carrying our own previous generation's
    # identity is waited for rather than refused (#248).
    previous_generation_of_identity = strategy_runner_identity_lineage_for_instance(
        runtime_root,
        instance=runner_state_path.parent.name,
    )

    def open_source() -> RunnerSignalSource:
        return ReadonlyStrategyRunnerSignalSource(
            source_id=source_settings.source_id,
            path=runner_state_path,
            expected_strategy_spec_fingerprint=spec_fingerprint,
            expected_evaluator_contract_fingerprint=evaluator_fingerprint,
            busy_timeout_ms=busy_timeout_ms,
            previous_generation_of_identity=previous_generation_of_identity,
        )

    return open_source


def signal_router_builder(
    *,
    source_loader: SignalSourceLoader | None = None,
    target_resolver: TargetResolver | None = None,
    clock: Callable[[], datetime],
    monotonic_clock: Callable[[], float] = time.monotonic,
    runtime_root: Path | None = None,
) -> RuntimeServiceBuilder:
    if (source_loader is None) != (target_resolver is None):
        raise ValueError("signal router dependencies must be provided together")

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        _require_manifest(manifest, kind=RuntimeServiceKind.SIGNAL_ROUTER)
        settings = SignalRouterSettings.model_validate(
            manifest.model_dump(mode="python")["settings"]
        )
        price_activation, price_peer, price_policy = None, None, None
        if settings.price_alert_peer is not None:
            price_activation, price_peer, price_policy = open_price_role_peer(
                manifest, settings.price_alert_peer, runtime_root=runtime_root
            )
        condition_activation, condition_peer, condition_policy = None, None, None
        fields = (
            settings.condition_alert_runtime,
            settings.condition_alert_runtime_manifest_path,
            settings.condition_alert_peer,
        )
        if any(value is not None for value in fields) and not all(
            value is not None for value in fields
        ):
            raise ValueError("condition peer authority must be complete")
        if settings.condition_alert_peer is not None:
            condition_activation, condition_peer, condition_policy = open_condition_role_peer(
                manifest,
                settings.condition_alert_peer,
                runtime_root=runtime_root,
                borrowed=price_peer,
            )
        injected = source_loader is not None and target_resolver is not None
        if injected and settings.has_manifest_authority:
            raise ValueError(
                "manifest authority cannot be combined with injected router dependencies"
            )
        if not injected and not settings.has_manifest_authority:
            raise ValueError("default signal router requires complete manifest authority")

        # Whether this manifest describes a router at all is settled before anything is
        # created -- the gate above and these two -- because a role that is going to
        # refuse over its own settings must leave the directory as it found it. Nothing
        # here touches the filesystem.
        #
        # These two and `has_manifest_authority` above are the same predicate:
        # `has_manifest_authority` is "a routing policy path and every source complete",
        # and `SignalRouterSettings` / `SignalRouterSourceSettings` refuse the shapes in
        # between outright. So each half alone is unobservable, and the mutation for this
        # ordering has to move both to say anything at all.
        if not injected:
            if settings.routing_policy_path is None:
                raise ValueError("default signal router authority is unavailable")
            for source_settings in settings.source_settings:
                if (
                    source_settings.runner_state_path is None
                    or source_settings.expected_strategy_spec_fingerprint is None
                    or source_settings.expected_evaluator_contract_fingerprint is None
                ):
                    raise ValueError("default signal router authority is unavailable")

        # The bus, the route spool and the cursor store are this role's own artifacts and
        # nobody else creates them: `strategy_live` opens the bus read-only and its
        # sandbox grants it `live/strategies/%i` alone. They are opened before any
        # strategy's runner database is looked at, so a router that starts first breaks
        # the cycle instead of dying inside it (#220).
        # The strategy spec fingerprints our own earlier generations published, so the
        # route ledger can carry one source row across a release instead of conflicting
        # on it every iteration (#248 shape 2). Route B, or a router with no manifest
        # authority, gets an empty map and the old refusal.
        bus = settings.open_store(
            previous_generation_of_strategy_spec=previous_strategy_spec_generations(
                runtime_root,
                service_ids=tuple(
                    service_id
                    for service_id in (
                        _strategy_service_id(source_settings.runner_state_path, runtime_root)
                        for source_settings in settings.source_settings
                    )
                    if service_id is not None
                ),
            )
        )
        if condition_peer is not None:
            if settings.condition_alert_peer.install_namespace:
                bus.install_condition_alert_route_v1(condition_activation)
            else:
                from rquant.condition_alert_route import _require_condition_history

                with bus._read_snapshot() as connection:
                    _require_condition_history(connection)
        signal_spool = SignalRouteSpool(settings.signal_spool_root)
        cursors = SignalRouteCursorStore(
            settings.signal_bus_path,
            routing_policy_fingerprint=settings.routing_policy_fingerprint,
            busy_timeout_ms=settings.busy_timeout_ms,
        )

        if injected:
            resolved_source_loader = source_loader
            resolved_target_resolver = target_resolver
        else:
            assert settings.routing_policy_path is not None
            deferred_sources: dict[str, DeferredPeerArtifact[RunnerSignalSource]] = {}
            for source_settings in settings.source_settings:
                deferred_sources[source_settings.source_id] = DeferredPeerArtifact(
                    reader="signal_router",
                    artifact="runner source",
                    path=source_settings.runner_state_path,
                    open_artifact=_runner_source_opener(
                        source_settings,
                        busy_timeout_ms=settings.busy_timeout_ms,
                        runtime_root=runtime_root,
                    ),
                )
            # A runner database that is already on disk is opened and checked now, so a
            # source that exists and does not match its published identity still refuses
            # to start. One that is absent is waited for inside the loop instead.
            for deferred in deferred_sources.values():
                deferred.probe()
            authoritative_policy = load_frozen_routing_policy(
                settings.routing_policy_path,
                routing_policy_fingerprint=settings.routing_policy_fingerprint,
                observed_at=clock(),
            )

            def load_authoritative_source(source_id: str) -> RunnerSignalSource:
                try:
                    deferred = deferred_sources[source_id]
                except KeyError as exc:
                    raise ValueError("signal source is not in manifest authority") from exc
                return deferred.get()

            resolved_source_loader = load_authoritative_source
            resolved_target_resolver = authoritative_policy

        if resolved_source_loader is None or resolved_target_resolver is None:
            raise RuntimeError("signal router dependencies are unavailable")

        last_prefix_attempt_tick: float | None = None

        def step() -> RuntimeStepResult:
            nonlocal last_prefix_attempt_tick
            observed_at = clock()
            before_publish = publish_mixed_notification_bus_prefix(
                bus=bus,
                spool=signal_spool,
                limit=min(settings.batch_limit, 100),
                observed_at=observed_at,
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
                    # This return is above every bind, so no watermark was even looked at.
                    watermark_advanced=False,
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
            #: True as soon as one source's `observed_high_watermark` actually moves this
            #: iteration; `False` on every idle one, which is most of them (#271).
            watermark_advanced = False
            for index, source_settings in enumerate(settings.source_settings):
                source_id = source_settings.source_id
                source = resolved_source_loader(source_id)
                current = bus.route_cursor(source_id)
                descriptor = _inspect_signal_source(
                    source_id=source_id,
                    source=source,
                    after_sequence=current.last_sequence,
                )
                binding = bus.bind_route_source_observed(
                    descriptor,
                    routing_policy_fingerprint=settings.routing_policy_fingerprint,
                    observed_at=observed_at,
                )
                watermark_advanced = watermark_advanced or binding.watermark_advanced
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
                    # The binds above already ran -- a paused router still observes its
                    # sources -- so this carries what they did.
                    watermark_advanced=watermark_advanced,
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
                    # `route_runner_signals` binds the source again from its own frozen
                    # descriptor, so a source that grew between the bind above and this
                    # read moves the watermark row in there instead of here.
                    if summary.source_high_watermark > previous_high_watermark:
                        watermark_advanced = True
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
            if price_peer is not None:
                price_high, price_last, price_count = route_price_role(
                    bus,
                    peer=price_peer,
                    activation=price_activation,
                    policy=price_policy,
                    observed_at=observed_at,
                    limit=min(remaining, 100),
                )
                input_sequence += price_high
                output_sequence += price_last
                processed_count += price_count
                remaining -= price_count
            if condition_peer is not None and remaining > 0:
                high, last, count = route_condition_role(
                    bus=bus,
                    peer=condition_peer,
                    activation=condition_activation,
                    policy=condition_policy,
                    observed_at=observed_at,
                    limit=min(remaining, 100),
                )
                input_sequence += high
                output_sequence += last
                processed_count += count
            published = publish_mixed_notification_bus_prefix(
                bus=bus,
                spool=signal_spool,
                limit=min(settings.batch_limit, 100),
                observed_at=observed_at,
            )
            if published.published_high_watermark >= published.source_high_watermark:
                try:
                    tick = monotonic_clock()
                    if (
                        last_prefix_attempt_tick is None
                        or tick < last_prefix_attempt_tick
                        or tick - last_prefix_attempt_tick >= _BUS_PREFIX_LINK_MIN_INTERVAL_SECONDS
                    ):
                        last_prefix_attempt_tick = tick
                        signal_spool.publish_bus_prefix_link(bus=bus, observed_at=observed_at)
                except Exception as exc:
                    logger.warning("signal bus prefix link unavailable: {}", exc)
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
                watermark_advanced=watermark_advanced,
            )

        if price_peer is not None:
            step.close = price_peer.close
        elif condition_peer is not None:
            step.close = condition_peer.ledger.close
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
    runtime_root: Path | None = None,
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        _require_manifest(manifest, kind=RuntimeServiceKind.NOTIFIER)
        settings = NotifierSettings.model_validate(manifest.model_dump(mode="python")["settings"])
        price_activation, price_peer, price_policy = None, None, None
        if settings.price_alert_peer is not None:
            price_activation, price_peer, price_policy = open_price_role_peer(
                manifest, settings.price_alert_peer, runtime_root=runtime_root
            )
        condition_activation, condition_peer, condition_policy = None, None, None
        fields = (
            settings.condition_alert_runtime,
            settings.condition_alert_runtime_manifest_path,
            settings.condition_alert_peer,
        )
        if any(value is not None for value in fields) and not all(
            value is not None for value in fields
        ):
            raise ValueError("condition peer authority must be complete")
        if settings.condition_alert_peer is not None:
            condition_activation, condition_peer, condition_policy = open_condition_role_peer(
                manifest,
                settings.condition_alert_peer,
                runtime_root=runtime_root,
                borrowed=price_peer,
            )
        merge_binding = None
        if settings.merge_enabled:
            from rquant.delivery_contracts import NotificationMergeBinding

            if runtime_root is None:
                raise ValueError("notification merge requires the installed runtime generation")
            installed = load_runtime_generation_tree(runtime_root)
            original = installed.lineage(manifest.service_id).current
            if original.manifest != manifest:
                raise ValueError("notification merge differs from its actual installed manifest")
            merge_binding = NotificationMergeBinding(
                owner_id=settings.merge_owner_id, source_id="signal-route-spool/v1",
                installation_sha256=manifest.manifest_fingerprint,
                role_revision=manifest.service_spec.identity,
                generation_id=installed.current_generation_id,
                mode="shadow" if settings.suppress_delivery else "live",
            )
        store = settings.open_store(merge_binding=merge_binding)
        if settings.monitor_control is not None:
            if runtime_root is None or not settings.merge_enabled:
                raise ValueError("notifier controls require the original installed merge owner")
            current_control = read_monitor_control_state(settings.monitor_control, runtime_root=runtime_root, now=clock())
            if current_control.installation.notifier_manifest_sha256 != manifest.manifest_fingerprint:
                raise ValueError("notifier controls differ from this original role")
        if condition_peer is not None:
            if settings.condition_alert_peer.install_namespace:
                store.install_condition_alert_delivery_v1(condition_activation)
            else:
                from rquant.condition_alert_runtime_projection import _require_condition_delivery

                with store._read_snapshot() as connection:
                    _require_condition_delivery(connection)
        source = ReadonlyNotificationEventRouteSpool(settings.signal_spool_root)
        authority_publisher: ServingSourceAuthorityPublisher | None = None
        authority_reader: ServingSourceAuthorityReader | None = None
        previous_authority_reader: ServingSourceAuthorityReader | None = None
        page_projection_producer: SignalPageProjectionProducer | None = None
        build_events: tuple[str, ...] = ()
        if settings.serving_authority_root is not None:
            from rquant.runtime_serving_authority import (
                ServingSourceAuthorityPublisher,
                ServingSourceAuthorityReader,
                serving_source_pointer_handover,
            )

            authority_publisher = ServingSourceAuthorityPublisher(
                root=settings.serving_authority_root,
                producer_commit=manifest.producer_commit,
                dataset_id=_SIGNALS_DATASET_ID,
                payload_kind="signal_delivery",
                clock=clock,
            )
            # This role both owns and reads `serving-authority/current.json`, and a release
            # does not republish it: the pointer on disk after a handover still carries the
            # commit of our own previous generation, and comparing it against this one took
            # `notifier.admin.shadow.v1` DEGRADED every two seconds of the eighth window
            # (#260). `serving.publisher.v1` reads the very same file and was given this
            # predicate for the same reason in #253, so the judgement is shared rather than
            # duplicated: a commit `producer_commit_lineage` cannot trace back to a
            # generation installed under this runtime root is still refused, unchanged.
            # Unlike serving, this role may rewrite the pointer -- and does, on its own next
            # publish, as soon as its notification state revises.
            # An explicitly configured takeover outranks the lineage: it is an operator
            # naming one generation to take over from, and it is the only path that writes
            # the handoff row. It also narrows this reader: with the commit set there is no
            # lineage left to fall back on, so a pointer that is not the named commit is
            # refused even when this runtime root installed the generation that wrote it
            # (review SF-C). See `serving_previous_producer_commit` for the whole rule.
            authority_reader = ServingSourceAuthorityReader(
                root=settings.serving_authority_root,
                expected_producer_commit=manifest.producer_commit,
                expected_dataset_id=_SIGNALS_DATASET_ID,
                expected_payload_kind="signal_delivery",
                previous_generation_of_producer_commit=(
                    None
                    if settings.serving_previous_producer_commit is not None
                    else producer_commit_lineage(
                        runtime_root,
                        service_id=manifest.service_id,
                    )
                ),
            )
            build_events = tuple(
                event
                for event in (serving_source_pointer_handover(authority_reader),)
                if event is not None
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
                from rquant.readside_replica_gate import NOTIFIER_PAGE_PROJECTION_PROFILE
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
                        #: #268: this is the role whose generation read is the multi-
                        #: gigabyte `minute_bar` aggregate, and the only one of the four
                        #: that reads all day rather than inside a window of its own. It
                        #: re-opens the replica at most every fifteen minutes, and never
                        #: between 09:20 and 09:40, where on 2026-09-14 it was one of the
                        #: readers that kept the production monitor off the disk for ten
                        #: minutes after the open.
                        read_profile=NOTIFIER_PAGE_PROJECTION_PROFILE,
                        clock=clock,
                        surge_live_root=settings.page_projection_surge_live_root,
                        canvas_catalog_root=settings.page_projection_canvas_catalog_root,
                        user_presets_root=settings.page_projection_user_presets_root,
                        canvas_receipt_root=settings.page_projection_canvas_receipt_root,
                        canvas_publication_keyring=canvas_keyring,
                        #: #241: the outbox belongs to the page-control service and its
                        #: directory is read-only in this unit. The reader pins the
                        #: generation it reads with an open descriptor and writes nothing
                        #: anywhere, so this role needs no scratch directory of its own.
                        page_control_outbox=(settings.page_projection_page_control_outbox_path),
                        formula_pool_config=settings.page_projection_formula_pool_config,
                        #: #255: the DuckDB build on the production host refuses
                        #: `/proc/self/fd/<n>` as well, and the branch that used to run
                        #: instead hard-linked beside the database -- `EROFS`, every
                        #: iteration. This role is deliberately given **no** control root
                        #: to copy into, so it reads the generation in place and checks
                        #: its identity afterwards: the projection it is pointed at is the
                        #: production read-only replica (about 10 GB, #250), two orders of
                        #: magnitude over `_MAX_PINNED_COPY_BYTES`, so a copy could never
                        #: be taken anyway -- and #241's stronger property holds, that this
                        #: step writes nothing anywhere under the runtime root.
                    ),
                    store=store,
                    intraday_source=(
                        build_intraday_page_source(
                            settings.page_projection_intraday,
                            manifest=manifest,
                            control_root=settings.notification_state_path.parent,
                        )
                        if settings.page_projection_intraday is not None
                        else None
                    ),
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

        def _replica_cost() -> dict[str, object]:
            """What **this** iteration did with the read-only replica (#256, review MF-1).

            Empty for a notifier with no page projection configured, so a role that has no
            replica to read reports `None` rather than a fabricated zero. Where there is
            one, the answer always describes this iteration: `begin_iteration()` below
            clears the previous one's report without clearing its cache.
            """

            if page_projection_producer is None:
                return {}
            opened, read_bytes = page_projection_producer.source.replica_iteration_summary()
            return {
                "replica_opened": opened,
                "replica_read_bytes": read_bytes,
                "replica_skipped_by_floor": (
                    page_projection_producer.source.replica_iteration_skipped_by_floor()
                ),
            }

        def step() -> RuntimeStepResult:
            suppress_delivery = settings.suppress_delivery
            if settings.monitor_control is not None:
                from rquant.notifier_operator import notifier_mode_role_revision

                applied = read_monitor_control_state(settings.monitor_control, runtime_root=runtime_root, now=clock())
                if applied.installation.notifier_manifest_sha256 != manifest.manifest_fingerprint:
                    raise ValueError("notifier actual installed role changed")
                suppress_delivery = applied.mode.mode == "shadow"
                store.merge_binding = type(merge_binding).model_validate(merge_binding.model_dump() | {
                    "mode": applied.mode.mode,
                    "role_revision": notifier_mode_role_revision(
                        manifest.service_spec.identity, applied.mode)})

                def current_mode() -> bool:
                    latest = read_monitor_control_state(settings.monitor_control, runtime_root=runtime_root, now=clock())
                    return latest.installation == applied.installation and latest.mode == applied.mode

                store.merge_binding_guard = current_mode
                store.record_applied_notifier_mode(applied.mode)
            #: Whether this iteration put a row in `notification_projection_authority`.
            #: `None` until a publish happens at all, so a notifier configured without a
            #: page projection reports nothing rather than a fabricated False (#271).
            projection_published: bool | None = None
            if page_projection_producer is not None:
                page_projection_producer.source.begin_replica_iteration()
            # The clock is a lower bound for the verified spool read, never a later claim.
            source_inspected_at = clock()
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
                        projection_published = page_projection_producer.publish(
                            authority_observed_at
                        ).written
                    generation_id, omitted = _publish_signal_authority(
                        store=store,
                        publisher=authority_publisher,
                        reader=authority_reader,
                        previous_reader=previous_authority_reader,
                        observed_at=authority_observed_at,
                        history_limit=settings.serving_history_limit,
                        price_peer=price_peer,
                        price_activation=price_activation,
                        price_shadow=suppress_delivery,
                        price_paused=True,
                        condition_peer=condition_peer,
                        condition_activation=condition_activation,
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
                    projection_published=projection_published,
                    **_replica_cost(),
                )

            observed_at = clock()
            if price_activation is not None:
                apply_price_role_scope(
                    store,
                    activation=price_activation,
                    policy=price_policy,
                    serving_root=settings.price_alert_peer.scope_serving_root,
                    observed_at=observed_at,
                )
            if condition_peer is not None:
                apply_condition_role_scope(
                    store,
                    peer=condition_peer,
                    activation=condition_activation,
                    policy=condition_policy,
                    serving_root=settings.condition_alert_peer.scope_serving_root,
                    observed_at=observed_at,
                )
            visible_records = _read_routed_prefix_at(
                source,
                after_sequence=cursor.last_global_sequence,
                through_sequence=descriptor.high_watermark,
                observed_at=observed_at,
                limit=min(settings.batch_limit, 100),
            )
            with store._read_snapshot() as connection:
                mixed_installed = (
                    connection.execute(
                        "SELECT 1 FROM signal_bus_metadata WHERE metadata_key='m"
                        "ixed_notification_history'"
                    ).fetchone()
                    is not None
                )
            replicate = (
                store.replicate_mixed_notification_events if mixed_installed else store.replicate
            )
            replicated = replicate(
                descriptor,
                visible_records,
                observed_at=observed_at,
                source_inspected_at=source_inspected_at,
            )
            loaded_providers = resolved_provider_loader()
            if settings.merge_enabled:
                targets = _loaded_notification_targets(loaded_providers)
                store.runtime_available_targets = targets
                store.runtime_capability_observed_at = None if targets is None else clock()
            if suppress_delivery:
                loaded_providers = _shadow_providers(loaded_providers)
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
                price_activation=price_activation,
                condition_activation=condition_activation,
            )
            degraded: list[str] = []
            if suppress_delivery:
                #: the heartbeat of a shadow notifier never reads as a clean live one
                degraded.append("notifier:shadow_transport")
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
                    projection_published = page_projection_producer.publish(
                        authority_observed_at
                    ).written
                generation_id, omitted = _publish_signal_authority(
                    store=store,
                    publisher=authority_publisher,
                    reader=authority_reader,
                    previous_reader=previous_authority_reader,
                    observed_at=authority_observed_at,
                    history_limit=settings.serving_history_limit,
                    price_peer=price_peer,
                    price_activation=price_activation,
                    price_shadow=suppress_delivery,
                    condition_peer=condition_peer,
                    condition_activation=condition_activation,
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
                projection_published=projection_published,
                **_replica_cost(),
            )

        if build_events:
            step.generation_events = build_events
        if page_projection_producer is not None:
            #: An iteration that raises never reaches `_replica_cost()`, so the heartbeat
            #: said `replica_opened=null` for exactly the iterations #260 made fail -- and
            #: those had opened the replica, because the page projection is published
            #: before the serving authority on every return path. `run_service_loop` reads
            #: this on the failure path so the same MF-1 rule reaches a failed round:
            #: never asked the gate is `(False, 0)`, opened it is `(True, bytes)`, and a
            #: loader that raised part-way still counts as opened (package Q SF-7).
            step.replica_iteration_summary = (
                page_projection_producer.source.replica_iteration_summary
            )
            step.replica_iteration_skipped_by_floor = (
                page_projection_producer.source.replica_iteration_skipped_by_floor
            )
        if price_peer is not None:
            step.close = price_peer.close
        elif condition_peer is not None:
            step.close = condition_peer.ledger.close

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
