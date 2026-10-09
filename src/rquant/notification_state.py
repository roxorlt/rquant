"""Notifier-owned durable outbox replicated from an immutable signal spool."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, field_serializer, field_validator, model_validator

from rquant.alert_ack import alert_event_at, alert_window_start
from rquant.condition_alert_route import ConditionAlertBusRoutedRecord
from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    OutboxRecord,
    OutboxStatus,
    NotificationMergeBinding,
    NotificationMergeGroup,
    NotificationChannelStatistics,
    NotificationRuntimeWindow,
    NotificationRuntimeChannelState,
    NotificationRuntimeAttemptView,
    NotificationRuntimeGroupView,
    PhysicalPostBinding,
    PhysicalPostObservation,
    RouterDisposition,
)
from rquant.price_alert_route import PriceAlertBusRoutedRecord
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.signal_bus import (
    SignalBusIntegrityError,
    SignalBusRoutedRecord,
    SignalBusSourceDescriptor,
    SignalBusStore,
    SignalBusWatermarkError,
    SignalRouteReceipt,
    _require_consistent_high_watermark,
    parse_stored_signal,
    require_legacy_signal_write,
)
from rquant.signal_contracts import SignalEnvelopeFamily
from rquant.signal_observed_prefix import SignalObservedPrefixReceipt, signal_window_digest
from rquant.signal_route_spool import (
    ReadonlySignalRouteSpool,
    SignalBusSpoolPrefixReceipt,
    SignalRouteSpoolIntegrityError,
    _routed_prefix_digest,
)

if TYPE_CHECKING:
    from rquant.condition_alert_runtime import (
        ConditionAlertRuntimeStore,
        ConditionProducerRuntimeSnapshot,
    )
    from rquant.condition_alert_runtime_contracts import ConditionAlertRuntimeActivation
    from rquant.condition_alert_runtime_projection import (
        ConditionAlertAdmittedDelivery,
        ConditionAlertCancellationReceipt,
        ConditionAlertDeliveryAuthorityInput,
        ConditionAlertDeliveryAuthoritySnapshot,
        ConditionAlertSendAdmission,
    )
    from rquant.price_alert_runtime_contracts import PriceAlertRuntimeActivation
    from rquant.price_alert_runtime_store import (
        PriceProducerRuntimeSnapshot,
        ReadonlyPriceAlertRuntimeStore,
    )
    from rquant.runtime_serving_snapshot import SignalDeliveryReadPayload
    from rquant.serving_read_models import ServingSignalRecord

from rquant.serving_manual_watchlist_projection import validate_manual_watchlist_projections
from rquant.serving_price_alert_rule_projection import validate_price_alert_rule_projections
from rquant.serving_read_models import ServingProjectionPayload

_REQUIRED_NOTIFICATION_PROJECTION_TABLES = frozenset(
    {
        "screen_result",
        "pool2_watch",
        "market_snapshot",
        "market_overview",
        "intraday_kline",
        "screen_bounds",
        "minute_coverage",
        "canvas_diagnostic",
        "canvas_latest_trade_date",
        "canvas_hit",
        "canvas_definition",
    }
)
_OPTIONAL_NOTIFICATION_PROJECTION_TABLES = frozenset(
    {
        "pulse_history",
        "pulse_alert",
        "surge_runtime_config",
        "monitor_event",
        "surge_event",
        "alert_ack_state",
        "alert_ack",
        "manual_watchlist_state",
        "manual_watchlist",
        "price_alert_rule_state",
        "price_alert_rule",
        "condition_alert_rule_state",
        "condition_alert_rule",
        "condition_alert_runtime_state",
        "condition_alert_runtime",
        "condition_alert_runtime_event",
        "legacy_notification",
        "legacy_notification_status",
        "pool_definition",
        "screen_run_receipt",
        "screen_run_evidence",
        "intraday_screen_source",
        "intraday_feature_snapshot",
        "pool_membership",
        "pool_member_return",
        "formula_pool_state",
        "formula_pool_definition",
        "formula_pool_latest_result",
    }
)
_NOTIFICATION_PROJECTION_TABLES = (
    _REQUIRED_NOTIFICATION_PROJECTION_TABLES | _OPTIONAL_NOTIFICATION_PROJECTION_TABLES
)
_MAX_SERVING_DELIVERIES = 10_000
_MAX_SIGNAL_COVERAGE_PREFIX = 10_000
_SIGNAL_OBSERVATION_INTERVAL = timedelta(minutes=1)


class NotificationReplicationError(RuntimeError):
    """Published signal history conflicts with notifier-owned replication state."""


class NotificationReplicationCursor(RuntimeContractModel):
    source_id: str | None = Field(default=None, min_length=1)
    source_generation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    first_global_sequence: int = Field(default=1, ge=1)
    observed_high_watermark: int = Field(default=0, ge=0)
    last_global_sequence: int = Field(default=0, ge=0)
    last_signal_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    updated_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def validate_progress(self) -> Self:
        if (self.source_id is None) != (self.source_generation_id is None):
            raise ValueError("notification source id and generation must be bound together")
        if self.last_global_sequence > self.observed_high_watermark:
            raise ValueError("notification cursor exceeds the observed source watermark")
        if (self.last_global_sequence == 0) != (self.last_signal_id is None):
            raise ValueError("notification cursor and last signal identity disagree")
        return self


class NotificationReplicationSummary(RuntimeContractModel):
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_high_watermark: int = Field(ge=0)
    started_after_sequence: int = Field(ge=0)
    ended_at_sequence: int = Field(ge=0)
    replicated_count: int = Field(ge=0)


class NotificationBusSpoolPrefixVerification(RuntimeContractModel):
    """Historical three-copy prefix agreement without upstream completeness."""

    link: SignalBusSpoolPrefixReceipt
    notification_source_inspected_at: AwareUtcDatetime
    notification_state_revision: int = Field(ge=0)
    notification_routed_rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    upstream_complete: Literal[False] = False

    @model_validator(mode="after")
    def validate_agreement(self) -> Self:
        if self.notification_source_inspected_at < self.link.bus_prefix.source_inspected_at:
            raise ValueError("notification observation predates bus prefix")
        if self.notification_routed_rows_sha256 != self.link.routed_rows_sha256:
            raise ValueError("notification and spool route digests differ")
        return self


class NotificationAuthorityHandoff(RuntimeContractModel):
    handoff_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    next_producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    previous_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    business_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_sequence: int = Field(ge=0)
    next_sequence: int = Field(ge=1)
    observed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_handoff(self) -> Self:
        if self.previous_producer_commit == self.next_producer_commit:
            raise ValueError("authority handoff requires a different producer commit")
        if self.next_sequence != self.previous_sequence + 1:
            raise ValueError("authority handoff must advance exactly one sequence")
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"handoff_id", "observed_at"})
        )
        if self.handoff_id != expected:
            raise ValueError("authority handoff id does not match canonical identity")
        return self


class NotificationRecipientMigrationAudit(RuntimeContractModel):
    migration_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    alias_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_outbox_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    signal_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    channel: DeliveryChannel
    source_recipient_id: str = Field(min_length=1)
    target_recipient_ids: tuple[str, ...]
    target_outbox_ids: tuple[str, ...]
    outcome: Literal["migrated", "preserved_succeeded", "preserved_terminal"]
    original_record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_at: AwareUtcDatetime


class NotificationRecipientMigrationSummary(RuntimeContractModel):
    alias_binding_count: int = Field(ge=0)
    migrated_outbox_count: int = Field(ge=0)
    created_outbox_count: int = Field(ge=0)
    preserved_outbox_count: int = Field(ge=0)
    audit_ids: tuple[str, ...]


#: A projection authority carries **two** identities (#271), and the split is the whole
#: of the fix.
#:
#: `content_id` names what is published: the projections, their source receipts, and the
#: schema version. Nothing that moves on its own is in it, so an iteration that finds the
#: same projection computes the same `content_id` as the last one -- which is what lets
#: `publish_projection_authority` recognise its own last publication and write nothing.
#: Until v0.33.13 there was no such field: the id was hashed over every field of the
#: snapshot, `observed_at` (the notifier's own clock) and `available_at` (the receipts'
#: `published_at`, which the producer stamps from the same clock) included. So the same
#: projection was a new generation on every one of `notifier.admin.shadow.v1`'s two-second
#: iterations, the dedup lookup never matched, and the role inserted a row, committed it
#: and -- WAL with `synchronous = FULL` -- fsynced it all day whatever the replica held.
#:
#: `generation_id` names **this publication** of that content: the content id plus the two
#: instants. It stays per-publication on purpose, because the table is keyed by it and
#: `serving_snapshot` orders by `available_at DESC, observed_at DESC` -- so publishing a
#: content is always a new row, later than every row before it, and "the latest row" is
#: "the most recently published content". That is what the first cut of this fix got
#: wrong: with the id hashed over content alone, a projection that reverted byte for byte
#: to an earlier form matched that earlier *row*, wrote nothing, and left the intermediate
#: generation being served for as long as the revert lasted (review SF-1).
_AUTHORITY_CONTENT_IDENTITY = ("schema_version", "source_receipts", "projections")

#: The same rule one level down. A source receipt's `published_at` is the iteration clock
#: as well, and its `receipt_id` is carried into the authority's `source_receipts` -- so
#: leaving it in the hash would have made the authority's content differ every iteration
#: no matter what the authority itself excluded. Everything else on the receipt
#: (`dataset_id`, the source's own `generation_id`, `sequence`, `event_time`, `status`,
#: `projections`) is derived from the content it receipts for. These receipts are built
#: and consumed inside one publish and are never persisted on their own, so unlike the
#: authority above this rule has no older form to read.
_SOURCE_RECEIPT_IDENTITY_EXCLUDED = frozenset({"receipt_id", "published_at"})


class NotificationProjectionSourceReceipt(RuntimeContractModel):
    """Canonical receipt for one already-verified PIT projection authority."""

    dataset_id: str = Field(min_length=1)
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    sequence: int = Field(ge=0)
    event_time: AwareUtcDatetime
    published_at: AwareUtcDatetime
    status: FreshnessStatus = FreshnessStatus.FRESH
    projections: tuple[ServingProjectionPayload, ...] = Field(min_length=1)
    receipt_id: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("projections")
    @classmethod
    def canonicalize_source_projections(
        cls,
        value: tuple[ServingProjectionPayload, ...],
    ) -> tuple[ServingProjectionPayload, ...]:
        return tuple(sorted(value, key=lambda projection: projection.table_name))

    @model_validator(mode="after")
    def validate_receipt(self) -> Self:
        table_names = tuple(projection.table_name for projection in self.projections)
        if len(table_names) != len(set(table_names)):
            raise ValueError("notification projection source contains duplicate tables")
        if self.status is not FreshnessStatus.FRESH:
            raise ValueError("notification projection source must be fresh")
        if self.event_time > self.published_at:
            raise ValueError("notification projection source event time exceeds publication")
        if any(projection.available_at > self.published_at for projection in self.projections):
            raise ValueError("notification projection source contains future evidence")
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude=_SOURCE_RECEIPT_IDENTITY_EXCLUDED)
        )
        if self.receipt_id != expected:
            raise ValueError("notification projection source receipt does not match content")
        return self

    @classmethod
    def create(
        cls,
        *,
        dataset_id: str,
        generation_id: str,
        sequence: int,
        event_time: datetime,
        published_at: datetime,
        projections: tuple[ServingProjectionPayload, ...],
    ) -> NotificationProjectionSourceReceipt:
        values = {
            "dataset_id": dataset_id,
            "generation_id": generation_id,
            "sequence": sequence,
            "event_time": normalize_aware_utc(event_time),
            "published_at": normalize_aware_utc(published_at),
            "status": FreshnessStatus.FRESH,
            "projections": tuple(sorted(projections, key=lambda item: item.table_name)),
        }
        identity = {
            name: value
            for name, value in values.items()
            if name not in _SOURCE_RECEIPT_IDENTITY_EXCLUDED
        }
        return cls(**values, receipt_id=canonical_sha256(identity))


class NotificationProjectionAuthoritySnapshot(RuntimeContractModel):
    """One bounded PIT publication for every notification-owned page projection."""

    schema_version: int = Field(default=1, ge=1)
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    source_receipts: Mapping[str, str] = Field(min_length=1)
    projections: tuple[ServingProjectionPayload, ...]
    #: What is published, without either clock. `None` only on a row written before
    #: v0.33.13, whose `generation_id` is the older whole-snapshot hash; those are read
    #: and never produced.
    content_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("source_receipts", mode="after")
    @classmethod
    def freeze_source_receipts(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if any(not key or len(receipt) != 64 for key, receipt in value.items()):
            raise ValueError("notification projection source receipts are invalid")
        if any(
            any(character not in "0123456789abcdef" for character in receipt)
            for receipt in value.values()
        ):
            raise ValueError("notification projection source receipts are invalid")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("source_receipts")
    def serialize_source_receipts(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @field_validator("projections")
    @classmethod
    def canonicalize_projections(
        cls,
        value: tuple[ServingProjectionPayload, ...],
    ) -> tuple[ServingProjectionPayload, ...]:
        return tuple(sorted(value, key=lambda projection: projection.table_name))

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        table_names = tuple(projection.table_name for projection in self.projections)
        published = set(table_names)
        if (
            len(table_names) != len(published)
            or not _REQUIRED_NOTIFICATION_PROJECTION_TABLES.issubset(published)
            or not published.issubset(_NOTIFICATION_PROJECTION_TABLES)
        ):
            raise ValueError(
                "notification authority must publish exactly the notification projections "
                "required by the core contract and only registered optional projections"
            )
        validate_manual_watchlist_projections(
            {projection.table_name: projection for projection in self.projections}
        )
        validate_price_alert_rule_projections(
            {projection.table_name: projection for projection in self.projections}
        )
        from rquant.condition_alert_runtime_projection import (
            validate_condition_rule_projections,
            validate_condition_runtime_projections,
        )

        validate_condition_rule_projections({p.table_name: p for p in self.projections})
        validate_condition_runtime_projections({p.table_name: p for p in self.projections})
        if self.available_at > self.observed_at:
            raise ValueError("notification projection availability exceeds observation time")
        if any(projection.available_at > self.available_at for projection in self.projections):
            raise ValueError("notification projection contains future source evidence")
        if self.content_id is None:
            #: A row written before v0.33.13: one hash over the whole snapshot, both
            #: clocks included. Read as it was written; `create()` never produces this.
            legacy = self.model_dump(mode="python", exclude={"generation_id", "content_id"})
            if self.generation_id != canonical_sha256(legacy):
                raise ValueError("notification projection generation does not match content")
            return self
        if self.content_id != canonical_sha256(self.content_identity()):
            raise ValueError("notification projection content does not match its identity")
        if self.generation_id != canonical_sha256(self._publication_identity()):
            raise ValueError("notification projection generation does not match content")
        return self

    def content_identity(self) -> dict[str, object]:
        """What `content_id` is the hash of: this publication minus both of its clocks."""

        values = self.model_dump(mode="python")
        return {name: values[name] for name in _AUTHORITY_CONTENT_IDENTITY}

    def _publication_identity(self) -> dict[str, object]:
        return {
            "content_id": self.content_id,
            "observed_at": self.observed_at,
            "available_at": self.available_at,
        }

    @classmethod
    def create(
        cls,
        *,
        observed_at: datetime,
        available_at: datetime,
        source_receipts: Mapping[str, str],
        projections: tuple[ServingProjectionPayload, ...],
    ) -> NotificationProjectionAuthoritySnapshot:
        values = {
            "schema_version": 1,
            "observed_at": normalize_aware_utc(observed_at),
            "available_at": normalize_aware_utc(available_at),
            "source_receipts": dict(source_receipts),
            "projections": tuple(sorted(projections, key=lambda item: item.table_name)),
        }
        content_id = canonical_sha256({name: values[name] for name in _AUTHORITY_CONTENT_IDENTITY})
        generation_id = canonical_sha256(
            {
                "content_id": content_id,
                "observed_at": values["observed_at"],
                "available_at": values["available_at"],
            }
        )
        return cls(**values, content_id=content_id, generation_id=generation_id)

    @classmethod
    def create_from_sources(
        cls,
        *,
        observed_at: datetime,
        sources: tuple[NotificationProjectionSourceReceipt, ...],
    ) -> NotificationProjectionAuthoritySnapshot:
        observed = normalize_aware_utc(observed_at)
        validated = tuple(
            NotificationProjectionSourceReceipt.model_validate(source) for source in sources
        )
        if not validated:
            raise ValueError("notification projection sources cannot be empty")
        dataset_ids = tuple(source.dataset_id for source in validated)
        if len(dataset_ids) != len(set(dataset_ids)):
            raise ValueError("notification projection sources must have unique dataset ids")
        if any(source.published_at > observed for source in validated):
            raise ValueError("notification projection source contains future publication")
        projections = tuple(
            projection
            for source in sorted(validated, key=lambda item: item.dataset_id)
            for projection in source.projections
        )
        return cls.create(
            observed_at=observed,
            available_at=max(source.published_at for source in validated),
            source_receipts={source.dataset_id: source.receipt_id for source in validated},
            projections=projections,
        )


@dataclass(frozen=True, slots=True)
class NotificationProjectionPublication:
    """What one `publish_projection_authority` call did.

    `written` is the answer to "did this iteration actually put a row in the database",
    which is what the notifier's heartbeat reports as `projection_published` and what the
    #271 e2e asserts against `sqlite3.Connection.total_changes`. It is not derivable from
    `generation_id`: the same id comes back whether the row was inserted now or an hour
    ago.
    """

    generation_id: str
    written: bool


@dataclass(frozen=True)
class NotificationServingSnapshot:
    observed_at: AwareUtcDatetime
    sequence: int
    visible_signal_count: int
    returned_signal_count: int
    omitted_signal_count: int
    truncated: bool
    payload: SignalDeliveryReadPayload
    projection_generation_id: str | None = None
    projection_source_receipts: Mapping[str, str] = dataclass_field(default_factory=dict)
    signal_observed_prefix: SignalObservedPrefixReceipt | None = None

    def __post_init__(self) -> None:
        if any(
            value < 0
            for value in (
                self.sequence,
                self.visible_signal_count,
                self.returned_signal_count,
                self.omitted_signal_count,
            )
        ):
            raise ValueError("notification serving counts must be non-negative")
        if self.returned_signal_count != len(self.payload.signals):
            raise ValueError("returned signal count does not match payload")
        if self.visible_signal_count != (self.returned_signal_count + self.omitted_signal_count):
            raise ValueError("visible signal count does not match truncation counts")
        if self.truncated != (self.omitted_signal_count > 0):
            raise ValueError("truncated flag does not match omitted signal count")
        if (self.projection_generation_id is None) != (not self.projection_source_receipts):
            raise ValueError("notification projection identity and receipts must be bound")


class NotificationStateStore(SignalBusStore):
    """Own notification replication, outbox leases, and delivery evidence."""

    def __init__(
        self,
        path: Path | str,
        *,
        source_id: str = "signal-route-spool/v1",
        merge_binding: NotificationMergeBinding | None = None,
        **kwargs: object,
    ) -> None:
        normalized = source_id.strip()
        if not normalized:
            raise ValueError("notification source_id must not be empty")
        self.replication_source_id = normalized
        if merge_binding is not None and (
            type(merge_binding) is not NotificationMergeBinding
            or merge_binding.source_id != normalized
        ):
            raise ValueError("merge binding must match the original notification source")
        self.merge_binding = merge_binding
        self.merge_binding_guard: Callable[[], bool] | None = None
        self.runtime_available_targets: tuple[DeliveryTarget, ...] | None = None
        self.runtime_capability_observed_at: datetime | None = None
        super().__init__(path, **kwargs)

    def _merge_claim_ids(
        self, connection: sqlite3.Connection, *, now: datetime, limit: int,
        include_price: bool, include_condition: bool, include_builtin: bool = False,
    ) -> tuple[str, ...] | None:
        binding = self.merge_binding
        if binding is None:
            return None
        if limit > 100:
            raise ValueError("merged notification claim exceeds one hundred members")
        current = normalize_aware_utc(now)
        first = connection.execute("SELECT metadata_value FROM signal_bus_metadata WHERE metadata_key='notification_merge_observed_from'").fetchone()
        if first is None:
            connection.execute("INSERT INTO signal_bus_metadata VALUES('notification_merge_observed_from',?)", (current.isoformat(),))
            connection.execute("UPDATE notification_state_revision SET revision=revision+1 WHERE singleton=1")
        self._prune_merge_metadata(connection, now=current)
        encoded = current.isoformat()
        rows = connection.execute("""
            SELECT d.*, s.payload_json, s.payload_hash FROM delivery_outbox d
            JOIN signal_envelope s ON s.signal_id=d.signal_id
            WHERE d.status IN ('pending','retry') AND d.expires_at>?
              AND (d.next_attempt_at IS NULL OR d.next_attempt_at<=?)
            ORDER BY COALESCE(d.next_attempt_at,d.created_at),d.global_sequence,d.outbox_id
            LIMIT 10001
        """, (encoded, encoded)).fetchall()
        if len(rows) > 10000:
            raise SignalBusIntegrityError("notification merge member capacity exceeded")
        for row in rows:
            payload = json.loads(row["payload_json"])
            if hashlib.sha256(row["payload_json"].encode()).hexdigest() != row["payload_hash"]:
                raise SignalBusIntegrityError("notification merge original payload changed")
            tag = payload.get("envelope_schema")
            family = {"rquant.price-alert-event/v1": "price",
                      "rquant.condition-alert-event/v1": "condition",
                      "rquant.builtin-condition-alert-event/v1": "builtin"}.get(tag, "signal")
            if (family == "price" and not include_price) or (
                family == "condition" and not include_condition
            ) or (family == "builtin" and not include_builtin):
                continue
            member_binding = self._payload_merge_binding(payload)
            if member_binding.owner_id != binding.owner_id:
                if family == "price":
                    from rquant.price_alert_runtime_projection import _head

                    authority = _head(connection)
                elif family in {"condition", "builtin"}:
                    from rquant.condition_alert_runtime_projection import condition_delivery_authority

                    authority = condition_delivery_authority(connection)
                else:
                    continue
                if authority is None or member_binding.owner_id not in {row.owner_id for row in authority.policy.owners}:
                    continue
            existing = connection.execute(
                "SELECT group_id FROM notification_merge_member WHERE outbox_id=?",
                (row["outbox_id"],),
            ).fetchone()
            if existing is not None:
                continue
            from rquant.runtime_notification_providers import format_merged_member_payload

            part_bytes = len(format_merged_member_payload(row["payload_json"]).encode())
            if part_bytes + len("1条提醒".encode()) > 64 * 1024:
                raise SignalBusIntegrityError("complete original notification exceeds merge request budget")
            target = DeliveryTarget(recipient_id=row["recipient_id"], channel=row["channel"])
            cohort = self._merge_cohort(connection, payload, target, family)
            group_row = connection.execute("""
                SELECT g.* FROM notification_merge_group g
                WHERE g.cohort_sha256=? AND g.status='waiting' AND g.due_at>?
                  AND (SELECT COUNT(*) FROM notification_merge_member m WHERE m.group_id=g.group_id)<?
                ORDER BY g.opened_at,g.group_id LIMIT 1
            """, (cohort, encoded, limit)).fetchone()
            if group_row is not None:
                sizes = connection.execute("SELECT COUNT(*),SUM(rendered_utf8_bytes) FROM notification_merge_member "
                                           "WHERE group_id=?", (group_row["group_id"],)).fetchone()
                count = int(sizes[0]) + 1
                rendered_size = int(sizes[1]) + part_bytes + (count - 1) * len("\n\n---\n\n".encode()) + len(f"{count}条提醒".encode())
                if rendered_size > 64 * 1024:
                    group_row = None
            if group_row is None:
                group_id = canonical_sha256({"contract": "notification-merge-group/v1",
                                            "cohort": cohort, "opened_at": current,
                                            "first_outbox_id": row["outbox_id"]})
                group = NotificationMergeGroup(
                    group_id=group_id, binding=member_binding, cohort_sha256=cohort,
                    target=target, family=family, opened_at=current,
                    due_at=current + timedelta(seconds=30), status="waiting",
                    members=(row["outbox_id"],),
                )
                material = group.model_dump_json(exclude={"members", "status"})
                if len(material.encode()) > 16 * 1024:
                    raise SignalBusIntegrityError("notification merge group capacity exceeded")
                connection.execute("INSERT INTO notification_merge_group VALUES(?,?,?,?,?,?)",
                                   (group_id, cohort, encoded, group.due_at.isoformat(), "waiting", material))
            else:
                group_id = str(group_row["group_id"])
            ordinal = int(connection.execute(
                "SELECT COUNT(*) FROM notification_merge_member WHERE group_id=?", (group_id,),
            ).fetchone()[0])
            connection.execute("INSERT INTO notification_merge_member VALUES(?,?,?,?,?,?)",
                               (row["outbox_id"], group_id, row["signal_id"], row["payload_hash"], ordinal, part_bytes))
        result: list[str] = []
        self._check_merge_capacity(connection)
        groups = self._merge_groups(connection)
        for group in groups:
            if not self.merge_binding_matches(group.binding) or group.status not in {"waiting", "failed"} or current < group.due_at:
                continue
            members = self._rows_for_outbox_ids(connection, group.members)
            active = [row for row in members if row["status"] in {"pending", "retry"} and
                      datetime.fromisoformat(row["expires_at"]) > current]
            if any(row["status"] == "leased" for row in members):
                continue
            if any(row["next_attempt_at"] is not None and datetime.fromisoformat(row["next_attempt_at"]) > current
                   for row in active):
                continue
            if len(result) + len(active) <= limit:
                result.extend(str(row["outbox_id"]) for row in active)
        self._check_merge_capacity(connection)
        return tuple(result)

    def _merge_cohort(
        self, connection: sqlite3.Connection, payload: Mapping[str, object],
        target: DeliveryTarget, family: str,
    ) -> str:
        keys = ("strategy_id", "strategy_version", "parameter_fingerprint",
                "dataset_snapshot_id", "feature_snapshot_id", "producer_commit",
                "owner_id", "rule_id", "rule_version", "rule_body_hash", "scope_version",
                "member_digest", "source_identity", "evaluation_contract_sha256",
                "frequency_policy_sha256", "producer_manifest_sha256", "source_epoch",
                "rule_body_sha256", "membership_version", "member_binding_sha256",
                "scope_generation_id", "scope_manifest_sha256", "calendar_content_sha256",
                "quote_source_generation_id")
        source_row = connection.execute("SELECT source_id,source_generation_id FROM notification_replication_source "
                                        "WHERE singleton=1").fetchone()
        local_generation = connection.execute("SELECT metadata_value FROM signal_bus_metadata "
                                              "WHERE metadata_key='source_generation_id'").fetchone()
        if local_generation is None:
            raise SignalBusIntegrityError("merge original bus generation is unavailable")
        return canonical_sha256({"binding": self._payload_merge_binding(payload), "target": target, "family": family,
                                 "actual_bus_generation": local_generation[0],
                                 "actual_replication_source": None if source_row is None else tuple(source_row),
                                 "source": {key: payload[key] for key in keys if key in payload}})

    def _payload_merge_binding(self, payload: Mapping[str, object]) -> NotificationMergeBinding:
        if self.merge_binding is None:
            raise ValueError("original merge installation is disabled")
        values = self.merge_binding.model_dump(mode="python") | {"owner_id": payload.get("owner_id", self.merge_binding.owner_id)}
        return NotificationMergeBinding.model_validate(values)

    def merge_binding_matches(self, binding: NotificationMergeBinding) -> bool:
        return (self.merge_binding is not None and type(binding) is NotificationMergeBinding
            and binding.model_dump(exclude={"owner_id"}) == self.merge_binding.model_dump(exclude={"owner_id"})
            and (self.merge_binding_guard is None or self.merge_binding_guard() is True))

    def record_applied_notifier_mode(self, mode: object) -> None:
        from rquant.notifier_operator import NotifierModeState

        if type(mode) is not NotifierModeState or self.merge_binding is None or mode.mode != self.merge_binding.mode:
            raise ValueError("notifier applied mode differs from its original installed owner")
        with self._write_transaction() as connection:
            if not self.merge_binding_matches(self.merge_binding):
                raise ValueError("notifier original mode changed before application")
            body = mode.model_dump_json()
            row = connection.execute("SELECT metadata_value FROM signal_bus_metadata WHERE metadata_key='notification_notifier_mode'").fetchone()
            if row is None or row[0] != body:
                connection.execute("INSERT INTO signal_bus_metadata VALUES('notification_notifier_mode',?) "
                    "ON CONFLICT(metadata_key) DO UPDATE SET metadata_value=excluded.metadata_value", (body,))
                connection.execute("UPDATE notification_state_revision SET revision=revision+1 WHERE singleton=1")
                self._before_commit(connection)

    def validate_merge_cohort(
        self, connection: sqlite3.Connection, group: NotificationMergeGroup, record: OutboxRecord,
    ) -> None:
        row = connection.execute("SELECT payload_json,payload_hash FROM signal_envelope WHERE signal_id=?",
                                 (record.signal_id,)).fetchone()
        member = connection.execute("SELECT * FROM notification_merge_member WHERE outbox_id=?", (record.outbox_id,)).fetchone()
        if (row is None or member is None or member["group_id"] != group.group_id
                or row["payload_hash"] != hashlib.sha256(row["payload_json"].encode()).hexdigest()
                or member["payload_sha256"] != row["payload_hash"]
                or group.cohort_sha256 != self._merge_cohort(connection, json.loads(row["payload_json"]), record.target, group.family)):
            raise SignalBusIntegrityError("merge original source, generation or complete member payload changed")

    def _merge_groups(self, connection: sqlite3.Connection) -> tuple[NotificationMergeGroup, ...]:
        rows = connection.execute(
            "SELECT g.* FROM notification_merge_group g ORDER BY "
            "(SELECT MIN(COALESCE(d.next_attempt_at,d.created_at)) FROM notification_merge_member m "
            "JOIN delivery_outbox d ON d.outbox_id=m.outbox_id WHERE m.group_id=g.group_id),"
            "(SELECT MIN(d.global_sequence) FROM notification_merge_member m "
            "JOIN delivery_outbox d ON d.outbox_id=m.outbox_id WHERE m.group_id=g.group_id),g.group_id"
        ).fetchall()
        result: list[NotificationMergeGroup] = []
        for row in rows:
            if len(row["material_json"].encode()) > 16 * 1024:
                raise SignalBusIntegrityError("notification merge group capacity exceeded")
            members = connection.execute(
                "SELECT outbox_id FROM notification_merge_member WHERE group_id=? ORDER BY ordinal",
                (row["group_id"],),
            ).fetchall()
            material = json.loads(row["material_json"])
            group = NotificationMergeGroup(**material, status=row["status"],
                                           members=tuple(str(item[0]) for item in members))
            if (group.group_id != row["group_id"] or group.cohort_sha256 != row["cohort_sha256"]
                    or group.opened_at.isoformat() != row["opened_at"] or group.due_at.isoformat() != row["due_at"]):
                raise SignalBusIntegrityError("notification merge material differs from its same-ledger record")
            result.append(group)
        return tuple(result)

    def merge_groups(self) -> tuple[NotificationMergeGroup, ...]:
        if self.merge_binding is None:
            return ()
        with self._read_snapshot() as connection:
            self._check_merge_capacity(connection)
            return self._merge_groups(connection)

    def _prune_merge_metadata(self, connection: sqlite3.Connection, *, now: datetime) -> int:
        if self.merge_binding is None:
            return 0
        removed = 0
        terminal = {OutboxStatus.SUCCEEDED.value, OutboxStatus.EXPIRED.value, OutboxStatus.DEAD_LETTER.value}
        for group in self._merge_groups(connection):
            if group.status in {"intent", "unknown"} or not group.due_at + timedelta(days=7) < now:
                continue
            members = self._rows_for_outbox_ids(connection, group.members)
            if len(members) != len(group.members) or any(row["status"] not in terminal for row in members):
                continue
            if connection.execute("SELECT 1 FROM delivery_unknown WHERE outbox_id IN "
                    "(SELECT value FROM json_each(?)) LIMIT 1", (json.dumps(group.members),)).fetchone() is not None:
                continue
            closed_at = max(datetime.fromisoformat(row["updated_at"]) for row in members)
            calls = connection.execute("SELECT intent_json,observation_json FROM notification_physical_attempt WHERE group_id=?",
                (group.group_id,)).fetchall()
            unresolved = False
            for raw_intent, raw_observation in calls:
                intent = PhysicalPostBinding.model_validate_json(raw_intent)
                observation = None if raw_observation is None else PhysicalPostObservation.model_validate_json(raw_observation)
                if (observation is None or observation.binding != intent
                        or observation.disposition == "unknown" or intent.group_id != group.group_id):
                    unresolved = True
                    break
                closed_at = max(closed_at, observation.completed_at)
            if unresolved or not closed_at + timedelta(days=7) < now:
                continue
            connection.execute("DELETE FROM notification_physical_attempt WHERE group_id=?", (group.group_id,))
            connection.execute("DELETE FROM notification_merge_member WHERE group_id=?", (group.group_id,))
            connection.execute("DELETE FROM notification_merge_group WHERE group_id=?", (group.group_id,))
            removed += 1
        if removed:
            cutoff = (now - timedelta(days=7)).isoformat()
            connection.execute("INSERT INTO signal_bus_metadata(metadata_key,metadata_value) VALUES('notification_merge_retained_after',?) "
                "ON CONFLICT(metadata_key) DO UPDATE SET metadata_value=excluded.metadata_value", (cutoff,))
        return removed

    def prune_merge_metadata(self, *, now: datetime) -> int:
        if self.merge_binding is None:
            return 0
        with self._write_transaction() as connection:
            removed = self._prune_merge_metadata(connection, now=normalize_aware_utc(now))
            if removed:
                self._before_commit(connection)
            return removed

    def _check_merge_capacity(self, connection: sqlite3.Connection) -> None:
        group_count, group_bytes = connection.execute(
            "SELECT COUNT(*),COALESCE(SUM(length(CAST(material_json AS BLOB))),0) FROM notification_merge_group"
        ).fetchone()
        member_count = connection.execute("SELECT COUNT(*) FROM notification_merge_member").fetchone()[0]
        call_count, call_bytes = connection.execute("""
            SELECT COUNT(*), COALESCE(SUM(length(CAST(intent_json AS BLOB))+
                   COALESCE(length(CAST(observation_json AS BLOB)),0)),0)
            FROM notification_physical_attempt
        """).fetchone()
        if (group_count > 1024 or member_count > 10000 or call_count > 5120
                or group_bytes + member_count * 264 + call_bytes > 8 * 1024 * 1024):
            raise SignalBusIntegrityError("notification merge metadata capacity exceeded")

    def commit_merge_intent(
        self, group: NotificationMergeGroup, records: tuple[OutboxRecord, ...], *,
        worker_id: str, now: datetime, request_sha256: str,
        request_utf8_bytes: int,
        consume: Callable[[], None] | None = None,
    ) -> PhysicalPostBinding:
        current = normalize_aware_utc(now)
        if self.merge_binding is None or not self.merge_binding_matches(group.binding) or not records:
            raise ValueError("merge intent does not match the current installed notifier")
        binding = PhysicalPostBinding(
            group_id=group.group_id, owner_id=group.binding.owner_id, target=group.target,
            members=tuple({"outbox_id": row.outbox_id, "attempt_no": row.attempt_count}
                          for row in records), request_sha256=request_sha256,
            request_utf8_bytes=request_utf8_bytes, issued_at=current,
        )
        with self._write_transaction() as connection:
            actual = next((item for item in self._merge_groups(connection) if item.group_id == group.group_id), None)
            if actual != group or group.status not in {"waiting", "failed"} or current < group.due_at:
                raise ValueError("merge group changed or is not due")
            originals = self._rows_for_outbox_ids(connection, group.members)
            expected = {str(r["outbox_id"]) for r in originals if r["status"] == "leased"}
            if expected != {r.outbox_id for r in records}:
                raise ValueError("merge intent omits a still-leased original group member")
            for record in records:
                if record.outbox_id not in group.members or record.target != group.target:
                    raise ValueError("merge member belongs to another receiver or group")
                row = connection.execute("SELECT d.*,s.payload_hash FROM delivery_outbox d JOIN signal_envelope s "
                                         "ON s.signal_id=d.signal_id WHERE d.outbox_id=?", (record.outbox_id,)).fetchone()
                member = connection.execute("SELECT * FROM notification_merge_member WHERE outbox_id=?",
                                            (record.outbox_id,)).fetchone()
                if (row is None or member is None or self._outbox_from_row(row) != record
                        or member["payload_sha256"] != row["payload_hash"] or member["signal_id"] != record.signal_id):
                    raise SignalBusIntegrityError("merge intent original member or sealed payload changed")
                self._verify_lease(row, worker_id=worker_id, attempt_no=record.attempt_count, completed_at=current)
                self.validate_merge_cohort(connection, group, record)
                if current >= record.expires_at:
                    raise ValueError("merge member original TTL expired")
                self._validate_merge_member_authority(connection, group.family, record, current)
            # Holding the original writer transaction fences concurrent authority and
            # lease changes while the existing single-use admission rights are consumed.
            if consume is not None:
                consume()
            if not self.merge_binding_matches(group.binding):
                raise ValueError("notifier original mode changed before committed send permission")
            if group.binding.mode == "live":
                connection.execute("INSERT INTO notification_physical_attempt VALUES(?,?,?,?,?)",
                                   (binding.physical_id(), group.group_id, binding.model_dump_json(), None, "intent"))
            connection.execute("UPDATE notification_merge_group SET status='intent' WHERE group_id=?", (group.group_id,))
            self._check_merge_capacity(connection)
            self._before_commit(connection)
        return binding

    def _validate_merge_member_authority(
        self, connection: sqlite3.Connection, family: str, record: OutboxRecord, now: datetime,
    ) -> None:
        if family == "price":
            from rquant.price_alert_route import notification_record
            from rquant.price_alert_runtime_projection import _admission, _head, _valid_current_event

            head = _head(connection)
            admission = _admission(connection, record.outbox_id, record.attempt_count)
            if (head is None or admission is None or admission.authority_revision != head.authority_revision
                    or _valid_current_event(head, notification_record(connection, record.signal_id), record.target, now) is not None):
                raise ValueError("merge price source, rule, role or recipient changed")
        elif family in {"condition", "builtin"}:
            from rquant.condition_alert_route import notification_record
            from rquant.condition_alert_runtime_projection import (
                _invalid_condition_event, condition_delivery_authority,
                condition_send_admission, fresh_condition_authority,
            )

            event = notification_record(connection, record.signal_id)
            head = fresh_condition_authority(condition_delivery_authority(connection), now, event)
            admission = condition_send_admission(connection, record.outbox_id, record.attempt_count)
            if (admission is None or admission.authority_revision != head.authority_revision
                    or _invalid_condition_event(head, event, record.target, now) is not None):
                raise ValueError("merge condition source, rule, role or recipient changed")

    def record_physical_post(self, observed: PhysicalPostObservation) -> None:
        if type(observed) is not PhysicalPostObservation or observed.key_slot != 0:
            raise TypeError("physical observation requires the original single-recipient POST")
        binding = observed.binding
        with self._write_transaction() as connection:
            row = connection.execute("SELECT * FROM notification_physical_attempt WHERE physical_id=?",
                                     (binding.physical_id(),)).fetchone()
            if row is None or PhysicalPostBinding.model_validate_json(row["intent_json"]) != binding:
                raise ValueError("physical observation differs from original committed intent")
            encoded = observed.model_dump_json()
            if row["observation_json"] is not None:
                if row["observation_json"] != encoded:
                    raise ValueError("physical observation already has a different actual reply")
                return
            connection.execute("UPDATE notification_physical_attempt SET observation_json=?,outcome=? WHERE physical_id=?",
                               (encoded, observed.disposition, binding.physical_id()))
            self._check_merge_capacity(connection)
            self._before_commit(connection)

    def physical_post(self, binding: PhysicalPostBinding) -> PhysicalPostObservation | None:
        with self._read_snapshot() as connection:
            row = connection.execute("SELECT * FROM notification_physical_attempt WHERE physical_id=?",
                                     (binding.physical_id(),)).fetchone()
            if row is None or PhysicalPostBinding.model_validate_json(row["intent_json"]) != binding:
                raise ValueError("original physical intent is unavailable")
            return None if row["observation_json"] is None else PhysicalPostObservation.model_validate_json(row["observation_json"])

    def merge_preparation_committed(
        self, group: NotificationMergeGroup, records: tuple[OutboxRecord, ...],
    ) -> bool:
        with self._read_snapshot() as connection:
            actual = next(g for g in self._merge_groups(connection) if g.group_id == group.group_id)
            if actual.status not in {"waiting", "failed"}:
                return True
            if group.family == "price":
                from rquant.price_alert_runtime_projection import _admission

                return any(_admission(connection, r.outbox_id, r.attempt_count) is not None for r in records)
            if group.family in {"condition", "builtin"}:
                from rquant.condition_alert_runtime_projection import condition_send_admission

                return any(condition_send_admission(connection, r.outbox_id, r.attempt_count) is not None for r in records)
            return False

    def complete_merge(
        self, binding: PhysicalPostBinding, records: tuple[OutboxRecord, ...], *,
        worker_id: str, completed_at: datetime,
    ) -> str:
        if tuple((r.outbox_id, r.attempt_count) for r in records) != tuple(
            (r.outbox_id, r.attempt_no) for r in binding.members
        ):
            raise ValueError("merge completion differs from original intent members")
        with self._write_transaction() as connection:
            group = next(g for g in self._merge_groups(connection) if g.group_id == binding.group_id)
            if not self.merge_binding_matches(group.binding) or group.status != "intent":
                raise ValueError("merge intent has already completed or changed")
            shadow = group.binding.mode == "shadow"
            observed = None
            if not shadow:
                row = connection.execute("SELECT * FROM notification_physical_attempt WHERE physical_id=?",
                                         (binding.physical_id(),)).fetchone()
                if row is None or PhysicalPostBinding.model_validate_json(row["intent_json"]) != binding:
                    raise ValueError("merge completion lacks its original committed intent")
                observed = None if row["observation_json"] is None else PhysicalPostObservation.model_validate_json(row["observation_json"])
            disposition = "accepted" if shadow else "unknown" if observed is None else observed.disposition
            if disposition == "unknown":
                for record in records:
                    self._record_unknown_in_transaction(
                        connection, record.outbox_id, worker_id=worker_id,
                        attempt_no=record.attempt_count, observed_at=completed_at,
                        reason="merge physical request or result writeback is unresolved; no automatic resend",
                        provider_receipt=None,
                    )
                connection.execute("UPDATE notification_merge_group SET status='unknown' WHERE group_id=?", (group.group_id,))
                self._before_commit(connection)
                return "unknown"
            receipt = ("shadow:" if shadow else "channel:") + binding.physical_id()
            for record in records:
                self._complete_in_transaction(
                    connection, record.outbox_id, worker_id=worker_id, attempt_no=record.attempt_count,
                    completed_at=completed_at, success=disposition == "accepted",
                    provider_receipt=receipt if disposition == "accepted" else None,
                    error="channel definitely rejected group request" if disposition == "rejected" else None,
                )
            status = "shadow" if shadow else "succeeded" if disposition == "accepted" else "failed"
            connection.execute("UPDATE notification_merge_group SET status=? WHERE group_id=?", (status, group.group_id))
            self._check_merge_capacity(connection)
            self._before_commit(connection)
            return disposition

    def merge_channel_stats(self) -> tuple[NotificationChannelStatistics, ...]:
        if self.merge_binding is None:
            return ()
        with self._read_snapshot() as connection:
            return self._merge_channel_stats(connection)

    def _merge_channel_stats(self, connection: sqlite3.Connection, *,
        observed_at: datetime | None = None, covered_from: datetime | None = None,
    ) -> tuple[NotificationChannelStatistics, ...]:
        self._check_merge_capacity(connection)
        groups = self._merge_groups(connection)
        if observed_at is not None and any(g.opened_at > observed_at for g in groups):
            raise ValueError("notification groups are not visible at the original owner cutoff")
        keys = {(g.binding.owner_id, g.target, g.binding.mode, canonical_sha256(g.binding)) for g in groups}
        result: list[NotificationChannelStatistics] = []
        for owner, target, mode, binding_sha in sorted(keys, key=lambda k: (k[0], k[1].channel, k[1].recipient_id, k[2], k[3])):
            selected = [g for g in groups if (g.binding.owner_id, g.target, g.binding.mode, canonical_sha256(g.binding)) == (owner, target, mode, binding_sha)]
            group_ids = tuple(g.group_id for g in selected)
            members = tuple(item for g in selected for item in g.members)
            original = connection.execute("SELECT outbox_id,attempt_no,started_at,completed_at FROM delivery_attempt "
                "WHERE outbox_id IN (SELECT value FROM json_each(?))", (json.dumps(members),)).fetchall()
            attempts = set()
            for row in original:
                started, completed = datetime.fromisoformat(row[2]), datetime.fromisoformat(row[3])
                if observed_at is not None and completed > observed_at:
                    raise ValueError("notification attempt is not yet visible")
                if covered_from is None or started >= covered_from:
                    attempts.add((row[0], row[1]))
            leases = connection.execute("SELECT outbox_id,attempt_count,lease_started_at FROM delivery_outbox "
                "WHERE lease_started_at IS NOT NULL AND outbox_id IN (SELECT value FROM json_each(?))", (json.dumps(members),)).fetchall()
            # A lease proves a claim, so validate its cutoff without counting a send.
            for row in leases:
                started = datetime.fromisoformat(row[2])
                if observed_at is not None and started > observed_at:
                    raise ValueError("notification claim is not yet visible")
            # A missing call receipt is possible, never an invented actual POST.
            calls = connection.execute("SELECT observation_json,intent_json FROM notification_physical_attempt WHERE group_id IN "
                "(SELECT value FROM json_each(?))", (json.dumps(group_ids),)).fetchall()
            actual, possible = [], 0
            for call in calls:
                intent = PhysicalPostBinding.model_validate_json(call[1])
                if observed_at is not None and intent.issued_at > observed_at:
                    raise ValueError("notification physical intent is not yet visible")
                if covered_from is None or intent.issued_at >= covered_from:
                    attempts.update((m.outbox_id, m.attempt_no) for m in intent.members)
                if call[0] is None:
                    possible += int(covered_from is None or intent.issued_at >= covered_from)
                    continue
                observation = PhysicalPostObservation.model_validate_json(call[0])
                if observation.binding != intent or observed_at is not None and observation.completed_at > observed_at:
                    raise ValueError("notification physical result differs from the original same-read intent")
                if covered_from is None or observation.called_at >= covered_from:
                    actual.append(observation)
            # Unresolved original delivery evidence still counts an original
            # member attempt, without claiming any physical request occurred.
            unknown = connection.execute("SELECT outbox_id,attempt_no,observed_at FROM delivery_unknown WHERE outbox_id IN "
                "(SELECT value FROM json_each(?))", (json.dumps(members),)).fetchall()
            for row in unknown:
                at = datetime.fromisoformat(row[2])
                if observed_at is not None and at > observed_at:
                    raise ValueError("notification unknown result is not yet visible")
                if covered_from is None or at >= covered_from:
                    attempts.add((row[0], row[1]))
            accepted = [o for o in actual if o.disposition == "accepted"]
            logical = tuple(member for group in selected if covered_from is None or group.opened_at >= covered_from for member in group.members)
            result.append(NotificationChannelStatistics(owner_id=owner, target=target, mode=mode,
                binding_sha256=binding_sha, logical_count=len(logical), member_attempts=len(attempts),
                member_retries=sum(attempt > 1 for _, attempt in attempts), physical_requests=len(actual),
                accepted_count=len(accepted), rejected_count=sum(o.disposition == "rejected" for o in actual),
                unknown_count=sum(o.disposition == "unknown" for o in actual), possible_requests=possible,
                last_accepted_at=max((o.completed_at for o in accepted), default=None)))
        return tuple(result)

    def _monitor_runtime_projections(self, connection: sqlite3.Connection, *, observed_at: datetime,
        history_limit: int, builtin_facts: object | None = None,
    ) -> tuple[ServingProjectionPayload, ...]:
        from rquant.monitor_builtin_runtime import builtin_serving_projections
        from rquant.notifier_operator import NotifierModeState

        self._check_merge_capacity(connection)
        first = connection.execute("SELECT metadata_value FROM signal_bus_metadata WHERE metadata_key='notification_merge_observed_from'").fetchone()
        retained = connection.execute("SELECT metadata_value FROM signal_bus_metadata WHERE metadata_key='notification_merge_retained_after'").fetchone()
        start = None if first is None else datetime.fromisoformat(first[0])
        if start is not None and retained is not None:
            start = max(start, datetime.fromisoformat(retained[0]))
        if start is not None and start > observed_at:
            raise ValueError("notification observation coverage is future")
        groups = self._merge_groups(connection)
        if any(group.opened_at > observed_at for group in groups):
            raise ValueError("notification group is future at its same-read cutoff")
        physical = self.path.stat()
        source_row = connection.execute("SELECT source_id,source_generation_id FROM notification_replication_source WHERE singleton=1").fetchone()
        original_generation = connection.execute("SELECT metadata_value FROM signal_bus_metadata WHERE metadata_key='source_generation_id'").fetchone()
        source_sha = canonical_sha256({"contract": "rquant.notification-runtime-source/v1", "binding": self.merge_binding,
            "replication_source": None if source_row is None else tuple(source_row), "bus_generation": original_generation[0],
            "ledger_path": str(self.path), "device": physical.st_dev, "inode": physical.st_ino,
            "covered_from": start, "covered_through": observed_at})
        mode_row = connection.execute("SELECT metadata_value FROM signal_bus_metadata WHERE metadata_key='notification_notifier_mode'").fetchone()
        mode = None if mode_row is None else NotifierModeState.model_validate_json(mode_row[0])
        if mode is not None and (self.merge_binding is None or mode.mode != self.merge_binding.mode or mode.accepted_at is not None and mode.accepted_at > observed_at):
            raise ValueError("notification applied mode differs from its original owner cutoff/binding")
        selected = tuple(sorted(groups, key=lambda row: (row.opened_at, row.group_id), reverse=True)[:min(history_limit, 512)])
        header = NotificationRuntimeWindow(state="ready" if start is not None else "unavailable",
            reason="observed_window" if start is not None else "no_observed_window", observed_at=observed_at,
            binding=self.merge_binding, source_receipt_sha256=source_sha, covered_from=start, covered_through=observed_at,
            complete=start is not None, history_count=len(groups), returned_history_count=len(selected), truncated=len(selected)<len(groups),
            applied_revision=None if mode is None else mode.revision, applied_command_id=None if mode is None else mode.command_id,
            monitor_installation_sha256=None if mode is None else mode.installation_sha256,
            capability_observed_at=self.runtime_capability_observed_at, available_targets=self.runtime_available_targets)
        states = [{"owner_id": "", "channel": "", "body_json": header.model_dump_json()}]
        if start is not None:
            statistics = self._merge_channel_stats(connection, observed_at=observed_at, covered_from=start)
            for owner, channel in sorted({(row.owner_id, row.target.channel) for row in statistics}):
                rows = tuple(row for row in statistics if row.owner_id == owner and row.target.channel == channel)
                targets = tuple(sorted({row.target for row in rows}, key=lambda row: row.recipient_id))
                body = NotificationRuntimeChannelState(owner_id=owner, channel=channel,
                    recipient_scope_ref=canonical_sha256(targets), source_receipt_sha256=source_sha,
                    mode=self.merge_binding.mode, observed_at=observed_at, covered_from=start, covered_through=observed_at,
                    targets=targets, statistics=rows, logical_count=sum(row.logical_count for row in rows),
                    member_attempts=sum(row.member_attempts for row in rows), member_retries=sum(row.member_retries for row in rows),
                    physical_requests=sum(row.physical_requests for row in rows), accepted_count=sum(row.accepted_count for row in rows),
                    rejected_count=sum(row.rejected_count for row in rows), physical_unknown_count=sum(row.unknown_count for row in rows),
                    possible_requests=sum(row.possible_requests for row in rows),
                    last_accepted_at=max((row.last_accepted_at for row in rows if row.last_accepted_at is not None), default=None),
                    applied_revision=None if mode is None else mode.revision,
                    accepted_pct=(round(100 * sum(row.accepted_count for row in rows) / sum(row.physical_requests for row in rows), 1)
                        if sum(row.physical_requests for row in rows) and not any(row.unknown_count or row.possible_requests for row in rows)
                        else None))
                states.append({"owner_id": owner, "channel": channel.value, "body_json": body.model_dump_json()})
        history = []
        for group in selected:
            rows = connection.execute("SELECT intent_json,observation_json FROM notification_physical_attempt WHERE group_id=? ORDER BY physical_id", (group.group_id,)).fetchall()
            view = NotificationRuntimeGroupView(group=group, inspected_at=observed_at, source_receipt_sha256=source_sha,
                attempts=tuple(NotificationRuntimeAttemptView(intent=PhysicalPostBinding.model_validate_json(row[0]),
                    observation=None if row[1] is None else PhysicalPostObservation.model_validate_json(row[1])) for row in rows))
            history.append({"group_id": group.group_id, "owner_id": group.binding.owner_id,
                "channel": group.target.channel.value, "body_json": view.model_dump_json()})
        return (ServingProjectionPayload(table_name="notification_runtime_state", available_at=observed_at, rows=tuple(states)),
            ServingProjectionPayload(table_name="notification_runtime_delivery", available_at=observed_at, rows=tuple(history)),
            *builtin_serving_projections(builtin_facts, observed_at=observed_at))

    def _initialize(self) -> None:
        super()._initialize()
        if self.merge_binding is not None:
            with self._connect() as connection:
                connection.executescript("""
                    CREATE TABLE IF NOT EXISTS notification_merge_group (
                        group_id TEXT PRIMARY KEY, cohort_sha256 TEXT NOT NULL,
                        opened_at TEXT NOT NULL, due_at TEXT NOT NULL,
                        status TEXT NOT NULL, material_json TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS notification_merge_member (
                        outbox_id TEXT PRIMARY KEY REFERENCES delivery_outbox(outbox_id),
                        group_id TEXT NOT NULL REFERENCES notification_merge_group(group_id),
                        signal_id TEXT NOT NULL REFERENCES signal_envelope(signal_id),
                        payload_sha256 TEXT NOT NULL, ordinal INTEGER NOT NULL,
                        rendered_utf8_bytes INTEGER NOT NULL CHECK(rendered_utf8_bytes BETWEEN 1 AND 65536),
                        UNIQUE(group_id, ordinal)
                    );
                    CREATE TABLE IF NOT EXISTS notification_physical_attempt (
                        physical_id TEXT PRIMARY KEY,
                        group_id TEXT NOT NULL REFERENCES notification_merge_group(group_id),
                        intent_json TEXT NOT NULL, observation_json TEXT,
                        outcome TEXT NOT NULL
                    );
                """)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS notification_replication_source (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    source_id TEXT NOT NULL,
                    source_generation_id TEXT NOT NULL,
                    first_global_sequence INTEGER NOT NULL CHECK(first_global_sequence >= 1),
                    observed_high_watermark INTEGER NOT NULL CHECK(observed_high_watermark >= 0),
                    last_global_sequence INTEGER NOT NULL CHECK(last_global_sequence >= 0),
                    last_signal_id TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS notification_source_route_receipt (
                    global_sequence INTEGER PRIMARY KEY
                        REFERENCES signal_envelope(global_sequence),
                    signal_id TEXT NOT NULL UNIQUE
                        REFERENCES signal_envelope(signal_id),
                    receipt_hash TEXT NOT NULL,
                    receipt_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS notification_source_observation (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    source_id TEXT NOT NULL,
                    source_generation_id TEXT NOT NULL,
                    first_global_sequence INTEGER NOT NULL,
                    source_high_watermark INTEGER NOT NULL,
                    inspected_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS notification_state_revision (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    revision INTEGER NOT NULL CHECK(revision >= 0)
                );

                INSERT OR IGNORE INTO notification_state_revision(singleton, revision)
                VALUES (1, 0);

                CREATE TABLE IF NOT EXISTS notification_authority_handoff (
                    handoff_id TEXT PRIMARY KEY,
                    previous_producer_commit TEXT NOT NULL,
                    next_producer_commit TEXT NOT NULL,
                    previous_generation_id TEXT NOT NULL UNIQUE,
                    business_content_hash TEXT NOT NULL,
                    previous_sequence INTEGER NOT NULL CHECK(previous_sequence >= 0),
                    next_sequence INTEGER NOT NULL CHECK(next_sequence >= 1),
                    observed_at TEXT NOT NULL,
                    UNIQUE(previous_producer_commit, next_producer_commit),
                    CHECK(next_sequence = previous_sequence + 1)
                );

                CREATE TABLE IF NOT EXISTS notification_recipient_alias_binding (
                    channel TEXT NOT NULL,
                    source_recipient_id TEXT NOT NULL,
                    target_recipient_ids_json TEXT NOT NULL,
                    alias_fingerprint TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(channel, source_recipient_id)
                );

                CREATE TABLE IF NOT EXISTS notification_recipient_migration_audit (
                    migration_id TEXT PRIMARY KEY,
                    alias_fingerprint TEXT NOT NULL,
                    source_outbox_id TEXT NOT NULL UNIQUE,
                    signal_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    source_recipient_id TEXT NOT NULL,
                    target_recipient_ids_json TEXT NOT NULL,
                    target_outbox_ids_json TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    original_record_hash TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS notification_projection_authority (
                    generation_id TEXT PRIMARY KEY,
                    observed_at TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    source_receipts_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    content_id TEXT
                );
                """
            )
            #: A database created before v0.33.13 has the table without `content_id`
            #: (#271). `ADD COLUMN` is a schema-only change -- it rewrites no row and fires
            #: none of the immutability triggers below -- and the column is nullable, so
            #: every row already there keeps saying "written under the older rule", which
            #: is exactly what `validate_snapshot` reads it as.
            columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(notification_projection_authority)"
                ).fetchall()
            }
            if "content_id" not in columns:
                connection.execute(
                    "ALTER TABLE notification_projection_authority ADD COLUMN content_id TEXT"
                )
            for table in (
                "signal_envelope",
                "notification_source_route_receipt",
                "delivery_outbox",
                "delivery_attempt",
                "delivery_unknown",
                "notification_projection_authority",
                "notification_source_observation",
            ):
                for operation in ("INSERT", "UPDATE", "DELETE"):
                    trigger = f"notification_revision_{table}_{operation.lower()}"
                    connection.execute(
                        f"""
                        CREATE TRIGGER IF NOT EXISTS {trigger}
                        AFTER {operation} ON {table}
                        BEGIN
                            UPDATE notification_state_revision
                            SET revision = revision + 1
                            WHERE singleton = 1;
                        END
                        """
                    )
            for operation in ("UPDATE", "DELETE"):
                connection.execute(
                    f"""
                    CREATE TRIGGER IF NOT EXISTS
                        notification_source_route_receipt_immutable_{operation.lower()}
                    BEFORE {operation} ON notification_source_route_receipt
                    BEGIN
                        SELECT RAISE(ABORT, 'notification source route receipt is immutable');
                    END
                    """
                )
                connection.execute(
                    f"""
                    CREATE TRIGGER IF NOT EXISTS
                        notification_projection_authority_immutable_{operation.lower()}
                    BEFORE {operation} ON notification_projection_authority
                    BEGIN
                        SELECT RAISE(ABORT, 'notification projection authority is immutable');
                    END
                    """
                )
                for table in (
                    "notification_recipient_alias_binding",
                    "notification_recipient_migration_audit",
                ):
                    connection.execute(
                        f"""
                        CREATE TRIGGER IF NOT EXISTS {table}_immutable_{operation.lower()}
                        BEFORE {operation} ON {table}
                        BEGIN
                            SELECT RAISE(ABORT, '{table} is immutable');
                        END
                        """
                    )
                connection.execute(
                    f"""
                    CREATE TRIGGER IF NOT EXISTS
                        notification_authority_handoff_immutable_{operation.lower()}
                    BEFORE {operation} ON notification_authority_handoff
                    BEGIN
                        SELECT RAISE(ABORT, 'notification authority handoff is immutable');
                    END
                    """
                )

    #: The row `serving_snapshot` would serve **to this caller's clock** (#271, reviews
    #: SF-1 and DF-3). Same order *and* the same point-in-time filter as
    #: `serving_snapshot`: without the filter this asked "newest overall", so one row
    #: stamped ahead of the clock -- a clock that went backwards is the only way to get
    #: one -- would have made the gate skip a publication the reader could not yet see,
    #: which is SF-1's shape again by another route. Monotonic clocks make that
    #: unreachable and the filter is two index-free comparisons on one row, so it is
    #: stated rather than assumed.
    #:
    #: `payload_json` is deliberately **not** selected (review DF-1). The table has no
    #: index on `available_at`, so this is a scan plus a temporary B-tree for the sort,
    #: and dragging a 20 KB payload per row through that sorter cost 29 ms at 5k rows and
    #: 400-650 ms at 80k -- about 25x what the same query costs without it. The legacy
    #: branch below fetches the payload by primary key, for the one row that needs it.
    _LATEST_PROJECTION_AUTHORITY_SQL = """
        SELECT generation_id, content_id
        FROM notification_projection_authority
        WHERE available_at <= ? AND observed_at <= ?
        ORDER BY available_at DESC, observed_at DESC, generation_id DESC
        LIMIT 1
    """

    #: One row by primary key, for a latest row written before v0.33.13.
    _PROJECTION_AUTHORITY_PAYLOAD_SQL = """
        SELECT payload_json FROM notification_projection_authority WHERE generation_id = ?
    """

    @classmethod
    def _latest_published_content(
        cls,
        connection: sqlite3.Connection,
        observed_text: str,
    ) -> tuple[str, str] | None:
        """`(generation_id, content_id)` of what is being served at `observed_text`.

        Rows written before v0.33.13 have no `content_id`, so theirs is derived from the
        payload -- one primary-key read, and only while such a row is still the latest.
        After the next publication the latest row carries its own and this never runs.
        """

        row = connection.execute(
            cls._LATEST_PROJECTION_AUTHORITY_SQL,
            (observed_text, observed_text),
        ).fetchone()
        if row is None:
            return None
        generation_id = str(row["generation_id"])
        content_id = row["content_id"]
        if content_id is not None:
            return generation_id, str(content_id)
        payload = connection.execute(
            cls._PROJECTION_AUTHORITY_PAYLOAD_SQL,
            (generation_id,),
        ).fetchone()
        legacy = NotificationProjectionAuthoritySnapshot.model_validate_json(
            payload["payload_json"]
        )
        return generation_id, canonical_sha256(legacy.content_identity())

    def publish_projection_authority(
        self,
        snapshot: NotificationProjectionAuthoritySnapshot,
    ) -> NotificationProjectionPublication:
        """Publish this projection content, or recognise that it is already the latest.

        `written` is False for a call whose content is what the most recent publication
        already holds. That is the ordinary case for `notifier.admin.shadow.v1`, whose
        loop runs every two seconds against a replica that is replaced every five minutes:
        the content is the same content for hundreds of iterations at a time, and this
        method touches nothing at all for those -- no write transaction, no commit, no
        fsync (#271). The read below runs on the read-only connection precisely so that
        the common case does not take the database's write lock.

        The comparison is against the **latest visible** row rather than against any row
        carrying this content, so a projection that reverts to an earlier form is
        published again as a new row instead of silently leaving the intermediate
        generation in front of it (review SF-1). `generation_id` is per publication for
        the same reason: it is the row key, and two publications of one content have to be
        two rows.
        """

        validated = NotificationProjectionAuthoritySnapshot.model_validate(snapshot)
        observed_text = validated.observed_at.isoformat(timespec="microseconds")
        available_text = validated.available_at.isoformat(timespec="microseconds")
        connection = self._connect_readonly()
        try:
            latest = self._latest_published_content(connection, observed_text)
        finally:
            connection.close()
        if latest is not None and latest[1] == validated.content_id:
            return NotificationProjectionPublication(generation_id=latest[0], written=False)
        payload_json = validated.model_dump_json()
        receipts_json = json.dumps(
            dict(validated.source_receipts),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        with self._write_transaction() as connection:
            # Another writer may have published between the read above and this lock.
            latest = self._latest_published_content(connection, observed_text)
            if latest is not None and latest[1] == validated.content_id:
                return NotificationProjectionPublication(generation_id=latest[0], written=False)
            try:
                connection.execute(
                    """
                    INSERT INTO notification_projection_authority(
                        generation_id, content_id, observed_at, available_at,
                        source_receipts_json, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        validated.generation_id,
                        validated.content_id,
                        observed_text,
                        available_text,
                        receipts_json,
                        payload_json,
                    ),
                )
            except sqlite3.IntegrityError as error:
                #: This snapshot was published before and something else has been
                #: published since, so it is no longer what the latest row holds -- a
                #: caller replaying an old snapshot object, never the notifier's loop,
                #: which builds a new one from the clock on every iteration (review DF-2).
                raise NotificationReplicationError(
                    "notification projection generation was already published"
                ) from error
        return NotificationProjectionPublication(
            generation_id=validated.generation_id,
            written=True,
        )

    def replication_cursor(self) -> NotificationReplicationCursor:
        connection = self._connect_readonly()
        try:
            row = connection.execute(
                "SELECT * FROM notification_replication_source WHERE singleton = 1"
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            return NotificationReplicationCursor()
        return self._cursor_from_row(row)

    def verified_bus_spool_prefix(
        self,
        spool: ReadonlySignalRouteSpool,
        *,
        observed_at: datetime,
    ) -> NotificationBusSpoolPrefixVerification | None:
        """Read one notifier snapshot against a verified historical bus/spool link."""
        if not isinstance(spool, ReadonlySignalRouteSpool):
            raise TypeError("spool must be a ReadonlySignalRouteSpool")
        observed = normalize_aware_utc(observed_at)
        try:
            link = spool.bus_prefix_link()
        except SignalRouteSpoolIntegrityError:
            return None
        if link is None:
            return None
        prefix = link.bus_prefix
        high = prefix.source_high_watermark
        if high > _MAX_SIGNAL_COVERAGE_PREFIX or prefix.source_inspected_at > observed:
            return None

        try:
            with self._read_snapshot() as connection:
                revision_row = connection.execute(
                    "SELECT revision FROM notification_state_revision WHERE singleton = 1"
                ).fetchone()
                cursor_row = connection.execute(
                    "SELECT * FROM notification_replication_source WHERE singleton = 1"
                ).fetchone()
                observation = connection.execute(
                    "SELECT * FROM notification_source_observation WHERE singleton = 1"
                ).fetchone()
                if revision_row is None or cursor_row is None or observation is None:
                    return None
                cursor = self._cursor_from_row(cursor_row)
                inspected_at = normalize_aware_utc(
                    datetime.fromisoformat(str(observation["inspected_at"]))
                )
                if (
                    cursor.source_id != self.replication_source_id
                    or observation["source_id"] != self.replication_source_id
                    or cursor.source_generation_id != prefix.source_generation_id
                    or observation["source_generation_id"] != prefix.source_generation_id
                    or cursor.first_global_sequence != prefix.first_global_sequence
                    or observation["first_global_sequence"] != prefix.first_global_sequence
                    or cursor.observed_high_watermark != high
                    or observation["source_high_watermark"] != high
                    or cursor.last_global_sequence != high
                    or cursor.updated_at is None
                    or cursor.updated_at > observed
                    or inspected_at < prefix.source_inspected_at
                    or inspected_at > observed
                    or _require_consistent_high_watermark(connection) != high
                ):
                    return None
                rows = connection.execute(
                    """
                    SELECT signal.global_sequence, signal.signal_id, signal.payload_hash,
                           signal.payload_json,
                           length(CAST(signal.payload_json AS BLOB)) AS payload_size,
                           signal.received_at,
                           receipt.signal_id AS receipt_signal_id, receipt.receipt_hash,
                           receipt.receipt_json
                    FROM signal_envelope AS signal
                    LEFT JOIN notification_source_route_receipt AS receipt
                      ON receipt.global_sequence = signal.global_sequence
                    WHERE signal.global_sequence <= ?
                    ORDER BY signal.global_sequence
                    LIMIT ?
                    """,
                    (high, _MAX_SIGNAL_COVERAGE_PREFIX + 1),
                ).fetchall()
                if len(rows) != high:
                    return None
                records: list[SignalBusRoutedRecord] = []
                for expected, row in enumerate(rows, start=1):
                    if (
                        row["global_sequence"] != expected
                        or row["receipt_signal_id"] != row["signal_id"]
                        or not isinstance(row["receipt_hash"], str)
                        or not isinstance(row["receipt_json"], str)
                    ):
                        return None
                    payload_json = str(row["payload_json"])
                    signal = parse_stored_signal(
                        signal_id=str(row["signal_id"]),
                        payload_hash=str(row["payload_hash"]),
                        payload_json=payload_json,
                        payload_size=int(row["payload_size"]),
                    )
                    receipt_bytes = row["receipt_json"].encode("utf-8")
                    if hashlib.sha256(receipt_bytes).hexdigest() != row["receipt_hash"]:
                        return None
                    receipt = SignalRouteReceipt.model_validate_json(receipt_bytes)
                    records.append(
                        SignalBusRoutedRecord(
                            global_sequence=expected,
                            signal_id=str(row["signal_id"]),
                            payload_hash=str(row["payload_hash"]),
                            payload_json=payload_json,
                            signal=signal,
                            received_at=datetime.fromisoformat(str(row["received_at"])),
                            receipt=receipt,
                        )
                    )
                descriptor = SignalBusSourceDescriptor(
                    generation_id=prefix.source_generation_id,
                    first_global_sequence=prefix.first_global_sequence,
                    high_watermark=high,
                )
                routed = tuple(records)
                if cursor.last_signal_id != (routed[-1].signal_id if routed else None):
                    return None
                route_digest = _routed_prefix_digest(routed)
                if (
                    not prefix.matches_routed_prefix(descriptor, routed)
                    or route_digest != link.routed_rows_sha256
                ):
                    return None
                return NotificationBusSpoolPrefixVerification(
                    link=link,
                    notification_source_inspected_at=inspected_at,
                    notification_state_revision=int(revision_row["revision"]),
                    notification_routed_rows_sha256=route_digest,
                )
        except (
            sqlite3.DatabaseError,
            SignalBusIntegrityError,
            SignalBusWatermarkError,
            TypeError,
            ValueError,
        ):
            return None

    def replicate(
        self,
        source: SignalBusSourceDescriptor,
        records: tuple[SignalBusRoutedRecord, ...],
        *,
        observed_at: datetime,
        source_inspected_at: datetime | None = None,
    ) -> NotificationReplicationSummary:
        observed = normalize_aware_utc(observed_at)
        inspected = (
            None if source_inspected_at is None else normalize_aware_utc(source_inspected_at)
        )
        if inspected is not None and inspected > observed:
            raise ValueError("source inspection cannot follow replication observation")
        source = SignalBusSourceDescriptor.model_validate(source)
        records = tuple(SignalBusRoutedRecord.model_validate(record) for record in records)
        for record in records:
            require_legacy_signal_write(
                record.signal,
                operation="NotificationStateStore.replicate",
            )
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM notification_replication_source WHERE singleton = 1"
            ).fetchone()
            stored_cursor: NotificationReplicationCursor | None = None
            if row is None:
                started_after = source.first_global_sequence - 1
                observed_high = started_after
                last_signal_id = None
            else:
                cursor = self._cursor_from_row(row)
                stored_cursor = cursor
                if cursor.source_id != self.replication_source_id:
                    raise NotificationReplicationError("notification source identity changed")
                if cursor.source_generation_id != source.generation_id:
                    raise NotificationReplicationError("notification source generation changed")
                if cursor.first_global_sequence != source.first_global_sequence:
                    raise NotificationReplicationError("notification source start changed")
                if source.high_watermark < cursor.observed_high_watermark:
                    raise NotificationReplicationError("notification source watermark regressed")
                started_after = cursor.last_global_sequence
                observed_high = cursor.observed_high_watermark
                last_signal_id = cursor.last_signal_id

            expected = started_after + 1
            previous_input_sequence: int | None = None
            replicated_count = 0
            for record in records:
                if (
                    previous_input_sequence is not None
                    and record.global_sequence != previous_input_sequence + 1
                ):
                    raise NotificationReplicationError(
                        "notification replay records are not contiguous"
                    )
                previous_input_sequence = record.global_sequence
                if record.global_sequence <= started_after:
                    self._verify_replayed_record(connection, record)
                    continue
                if record.global_sequence != expected:
                    raise NotificationReplicationError(
                        f"notification source sequence gap: expected {expected}, "
                        f"observed {record.global_sequence}"
                    )
                if record.global_sequence > source.high_watermark:
                    raise NotificationReplicationError(
                        "notification record exceeds source high watermark"
                    )
                receipt, _changed = self._ingest_in_transaction(
                    connection,
                    record.signal,
                    received_at=record.received_at,
                )
                if receipt.disposition is RouterDisposition.QUARANTINED:
                    raise NotificationReplicationError(
                        "notification source payload conflicts with local state"
                    )
                if receipt.global_sequence != record.global_sequence:
                    raise NotificationReplicationError(
                        "notification local sequence differs from source sequence"
                    )
                self._store_source_receipt(connection, record)
                if record.receipt.targets:
                    self._route_in_transaction(
                        connection,
                        signal_id=record.signal_id,
                        targets=record.receipt.targets,
                        routed_at=record.receipt.routed_at,
                    )
                self._after_replicated_signal()
                expected += 1
                last_signal_id = record.signal_id
                replicated_count += 1

            ended_at = expected - 1
            if source.high_watermark < ended_at:
                raise NotificationReplicationError(
                    "notification cursor exceeds source high watermark"
                )
            high_watermark = max(observed_high, source.high_watermark)
            #: The cursor is written only when it has somewhere to move (#271). The three
            #: columns above are the cursor; `updated_at` was the fourth, and it is the
            #: iteration clock, so writing the row unconditionally dirtied a page,
            #: committed it and -- `journal_mode=WAL` with `synchronous=FULL` -- fsynced
            #: it on every one of `notifier.admin.shadow.v1`'s two-second iterations,
            #: whether or not a single signal had arrived. `updated_at` now says when this
            #: cursor last advanced, which is the question anybody reading it was asking.
            if (
                stored_cursor is None
                or stored_cursor.observed_high_watermark != high_watermark
                or stored_cursor.last_global_sequence != ended_at
                or stored_cursor.last_signal_id != last_signal_id
            ):
                timestamp = observed.isoformat(timespec="microseconds")
                connection.execute(
                    """
                    INSERT INTO notification_replication_source(
                        singleton, source_id, source_generation_id,
                        first_global_sequence, observed_high_watermark,
                        last_global_sequence, last_signal_id, updated_at
                    ) VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(singleton) DO UPDATE SET
                        observed_high_watermark = excluded.observed_high_watermark,
                        last_global_sequence = excluded.last_global_sequence,
                        last_signal_id = excluded.last_signal_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        self.replication_source_id,
                        source.generation_id,
                        source.first_global_sequence,
                        high_watermark,
                        ended_at,
                        last_signal_id,
                        timestamp,
                    ),
                )
            if inspected is not None:
                previous_observation = connection.execute(
                    "SELECT * FROM notification_source_observation WHERE singleton = 1"
                ).fetchone()
                if (
                    previous_observation is None
                    or previous_observation["source_id"] != self.replication_source_id
                    or previous_observation["source_generation_id"] != source.generation_id
                    or previous_observation["first_global_sequence"] != source.first_global_sequence
                    or previous_observation["source_high_watermark"] != source.high_watermark
                    or inspected
                    - normalize_aware_utc(
                        datetime.fromisoformat(str(previous_observation["inspected_at"]))
                    )
                    >= _SIGNAL_OBSERVATION_INTERVAL
                ):
                    connection.execute(
                        """
                        INSERT INTO notification_source_observation(
                            singleton, source_id, source_generation_id,
                            first_global_sequence, source_high_watermark, inspected_at
                        ) VALUES (1, ?, ?, ?, ?, ?)
                        ON CONFLICT(singleton) DO UPDATE SET
                            source_id = excluded.source_id,
                            source_generation_id = excluded.source_generation_id,
                            first_global_sequence = excluded.first_global_sequence,
                            source_high_watermark = excluded.source_high_watermark,
                            inspected_at = excluded.inspected_at
                        """,
                        (
                            self.replication_source_id,
                            source.generation_id,
                            source.first_global_sequence,
                            source.high_watermark,
                            inspected.isoformat(timespec="microseconds"),
                        ),
                    )
            self._before_commit(connection)

        return NotificationReplicationSummary(
            source_generation_id=source.generation_id,
            source_high_watermark=source.high_watermark,
            started_after_sequence=started_after,
            ended_at_sequence=ended_at,
            replicated_count=replicated_count,
        )

    def replicate_mixed_notification_events(
        self,
        source: SignalBusSourceDescriptor,
        records: tuple[
            SignalBusRoutedRecord | PriceAlertBusRoutedRecord | ConditionAlertBusRoutedRecord, ...
        ],
        *,
        observed_at: datetime,
        source_inspected_at: datetime,
    ) -> NotificationReplicationSummary:
        from rquant.condition_alert_route import _condition_record, copy_condition_record
        from rquant.price_alert_route import (
            _history_installed,
            _price_ingest,
            _price_outbox,
            _price_record,
        )
        from rquant.strict_json import canonical_json_bytes

        observed = normalize_aware_utc(observed_at)
        inspected = normalize_aware_utc(source_inspected_at)
        if (
            type(source) is not SignalBusSourceDescriptor
            or type(records) is not tuple
            or len(records) > 100
        ):
            raise TypeError("mixed replication requires exact source and bounded tuple records")
        source = SignalBusSourceDescriptor.model_validate(source)
        if inspected > observed:
            raise ValueError("mixed source inspection follows the replication clock")
        previous = None
        checked = []
        for record in records:
            if type(record) is SignalBusRoutedRecord:
                record = SignalBusRoutedRecord.model_validate(record)
                require_legacy_signal_write(
                    record.signal, operation="mixed committed legacy replication"
                )
                available = record.signal.available_at
            elif type(record) is ConditionAlertBusRoutedRecord:
                record = ConditionAlertBusRoutedRecord.model_validate_json(record.wire_bytes())
                if record.bus_generation_id != source.generation_id:
                    raise ValueError("mixed condition proof belongs to another actual bus")
                available = record.event.available_at
            elif type(record) is PriceAlertBusRoutedRecord:
                record = PriceAlertBusRoutedRecord.model_validate(record)
                if record.bus_generation_id != source.generation_id:
                    raise ValueError("mixed price proof belongs to another actual bus")
                available = record.event.available_at
            else:
                raise TypeError("mixed replication rejects substituted and current-family records")
            if (
                record.global_sequence > source.high_watermark
                or max(available, record.received_at, record.receipt.routed_at) > inspected
                or (previous is not None and record.global_sequence != previous + 1)
            ):
                raise ValueError("mixed replication input is future or discontinuous")
            previous = record.global_sequence
            checked.append(record)
        with self._write_transaction() as connection:
            _history_installed(connection)
            cursor_row = connection.execute(
                "SELECT * FROM notification_replication_source WHERE singleton=1"
            ).fetchone()
            if cursor_row is None:
                started_after = source.first_global_sequence - 1
                observed_high = started_after
                last_id = None
            else:
                cursor = self._cursor_from_row(cursor_row)
                if (
                    cursor.source_id != self.replication_source_id
                    or cursor.source_generation_id != source.generation_id
                    or cursor.first_global_sequence != source.first_global_sequence
                    or source.high_watermark < cursor.observed_high_watermark
                ):
                    raise ValueError("mixed replication original source changed or regressed")
                started_after, observed_high, last_id = (
                    cursor.last_global_sequence,
                    cursor.observed_high_watermark,
                    cursor.last_signal_id,
                )
            expected = started_after + 1
            replicated = 0
            for record in checked:
                is_price = type(record) is PriceAlertBusRoutedRecord
                is_condition = type(record) is ConditionAlertBusRoutedRecord
                if record.global_sequence <= started_after:
                    if is_condition:
                        if _condition_record(connection, record.event_id) != record:
                            raise ValueError(
                                "mixed condition replay differs from original sealed proof"
                            )
                    elif is_price:
                        if _price_record(connection, record.event_id) != record:
                            raise ValueError(
                                "mixed price replay differs from original sealed proof"
                            )
                    else:
                        self._verify_replayed_record(connection, record)
                    continue
                if record.global_sequence != expected:
                    raise ValueError(
                        "mixed replication must advance the original contiguous cursor"
                    )
                if is_condition:
                    copy_condition_record(connection, record)
                    last_id = record.event_id
                elif is_price:
                    sequence, added = _price_ingest(connection, record.event, record.received_at)
                    if not added or sequence != record.global_sequence:
                        raise ValueError(
                            "mixed price local identity/sequence conflicts with its actual source"
                        )
                    descriptor = record.source
                    immutable = canonical_json_bytes(
                        descriptor.model_dump(mode="json", exclude={"high_watermark"})
                    )
                    source_row = connection.execute(
                        "SELECT * FROM price_alert_route_source WHERE source_id=?",
                        (descriptor.source_id,),
                    ).fetchone()
                    if source_row is None:
                        if record.source_sequence != descriptor.first_sequence:
                            raise ValueError("mixed price source begins with a gap")
                        connection.execute(
                            "INSERT INTO price_alert_route_source VALUES(?,?,?,?)",
                            (
                                descriptor.source_id,
                                immutable,
                                descriptor.high_watermark,
                                record.source_sequence,
                            ),
                        )
                    else:
                        if (
                            bytes(source_row["body"]) != immutable
                            or record.source_sequence != source_row["last_sequence"] + 1
                            or descriptor.high_watermark < source_row["high_watermark"]
                        ):
                            raise ValueError(
                                "mixed price producer source changed or is discontinuous"
                            )
                        connection.execute(
                            (
                                "UPDATE price_alert_route_source SET high_watermark=?,la"
                                "st_sequence=? WHERE source_id=?"
                            ),
                            (
                                descriptor.high_watermark,
                                record.source_sequence,
                                descriptor.source_id,
                            ),
                        )
                    connection.execute(
                        "INSERT INTO price_alert_route_receipt VALUES(?,?,?,?,?,?)",
                        (
                            record.event_id,
                            descriptor.source_id,
                            record.source_sequence,
                            descriptor.wire_bytes(),
                            record.receipt.wire_bytes(),
                            record.bus_generation_id,
                        ),
                    )
                    _price_outbox(connection, record)
                    last_id = record.event_id
                else:
                    receipt, _changed = self._ingest_in_transaction(
                        connection, record.signal, received_at=record.received_at
                    )
                    if (
                        receipt.disposition is RouterDisposition.QUARANTINED
                        or receipt.global_sequence != record.global_sequence
                    ):
                        raise ValueError("mixed legacy local identity or sequence conflicts")
                    self._store_source_receipt(connection, record)
                    if record.receipt.targets:
                        self._route_in_transaction(
                            connection,
                            signal_id=record.signal_id,
                            targets=record.receipt.targets,
                            routed_at=record.receipt.routed_at,
                        )
                    last_id = record.signal_id
                self._after_replicated_signal()
                expected += 1
                replicated += 1
            ended_at = expected - 1
            high = max(observed_high, source.high_watermark)
            if (
                cursor_row is None
                or cursor_row["last_global_sequence"] != ended_at
                or cursor_row["observed_high_watermark"] != high
            ):
                connection.execute(
                    (
                        "INSERT INTO notification_replication_source "
                        "VALUES(1,?,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE "
                        "SET observed_high_watermark=excluded.observed_high_watermark"
                        ",last_global_sequence=excluded.last_global_sequence,last_sig"
                        "nal_id=excluded.last_signal_id,updated_at=excluded.updated_a"
                        "t"
                    ),
                    (
                        self.replication_source_id,
                        source.generation_id,
                        source.first_global_sequence,
                        high,
                        ended_at,
                        last_id,
                        observed.isoformat(timespec="microseconds"),
                    ),
                )
            previous_observation = connection.execute(
                "SELECT * FROM notification_source_observation WHERE singleton=1"
            ).fetchone()
            if (
                previous_observation is None
                or previous_observation["source_id"] != self.replication_source_id
                or previous_observation["source_generation_id"] != source.generation_id
                or previous_observation["first_global_sequence"] != source.first_global_sequence
                or previous_observation["source_high_watermark"] != source.high_watermark
                or inspected
                - normalize_aware_utc(
                    datetime.fromisoformat(str(previous_observation["inspected_at"]))
                )
                >= _SIGNAL_OBSERVATION_INTERVAL
            ):
                connection.execute(
                    (
                        "INSERT INTO notification_source_observation "
                        "VALUES(1,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET "
                        "source_id=excluded.source_id,source_generation_id=excluded.s"
                        "ource_generation_id,first_global_sequence=excluded.first_glo"
                        "bal_sequence,source_high_watermark=excluded.source_high_wate"
                        "rmark,inspected_at=excluded.inspected_at"
                    ),
                    (
                        self.replication_source_id,
                        source.generation_id,
                        source.first_global_sequence,
                        source.high_watermark,
                        inspected.isoformat(timespec="microseconds"),
                    ),
                )
            self._before_commit(connection)
        return NotificationReplicationSummary(
            source_generation_id=source.generation_id,
            source_high_watermark=source.high_watermark,
            started_after_sequence=started_after,
            ended_at_sequence=ended_at,
            replicated_count=replicated,
        )

    def _observed_signal_prefix_from_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        observed_at: datetime,
        selected: tuple[ServingSignalRecord, ...],
        truncated: bool,
    ) -> SignalObservedPrefixReceipt | None:
        """Verify the observed spool prefix inside the serving read transaction."""
        if truncated:
            return None
        observation = connection.execute(
            "SELECT * FROM notification_source_observation WHERE singleton = 1"
        ).fetchone()
        cursor_row = connection.execute(
            "SELECT * FROM notification_replication_source WHERE singleton = 1"
        ).fetchone()
        if observation is None or cursor_row is None:
            return None
        cursor = self._cursor_from_row(cursor_row)
        inspected_at = normalize_aware_utc(datetime.fromisoformat(str(observation["inspected_at"])))
        high = int(observation["source_high_watermark"])
        if (
            inspected_at > observed_at
            or cursor.updated_at is None
            or cursor.updated_at > observed_at
            or cursor.source_id != self.replication_source_id
            or observation["source_id"] != cursor.source_id
            or observation["source_generation_id"] != cursor.source_generation_id
            or observation["first_global_sequence"] != cursor.first_global_sequence
            or observation["first_global_sequence"] != 1
            or high != cursor.observed_high_watermark
            or high != cursor.last_global_sequence
            or high > _MAX_SIGNAL_COVERAGE_PREFIX
        ):
            return None
        try:
            local_high = _require_consistent_high_watermark(connection)
        except SignalBusWatermarkError:
            return None
        if local_high != high:
            return None
        rows = connection.execute(
            """
            SELECT signal.global_sequence, signal.signal_id, signal.payload_hash,
                   signal.payload_json, signal.received_at,
                   receipt.signal_id AS receipt_signal_id, receipt.receipt_hash,
                   receipt.receipt_json
            FROM signal_envelope AS signal
            LEFT JOIN notification_source_route_receipt AS receipt
              ON receipt.global_sequence = signal.global_sequence
            WHERE signal.global_sequence <= ?
            ORDER BY signal.global_sequence
            LIMIT ?
            """,
            (high, _MAX_SIGNAL_COVERAGE_PREFIX + 1),
        ).fetchall()
        if len(rows) != high:
            return None
        from rquant.serving_read_models import ServingSignalRecord

        first = alert_window_start(
            count_as_of=inspected_at,
            activated_at=datetime(1970, 1, 1, tzinfo=UTC),
        )
        window_records: list[ServingSignalRecord] = []
        prefix_rows: list[tuple[int, str, str, str]] = []
        for expected, row in enumerate(rows, start=1):
            if (
                row["global_sequence"] != expected
                or row["receipt_signal_id"] != row["signal_id"]
                or not isinstance(row["receipt_hash"], str)
                or not isinstance(row["receipt_json"], str)
            ):
                return None
            signal = parse_stored_signal(
                signal_id=str(row["signal_id"]),
                payload_hash=str(row["payload_hash"]),
                payload_json=str(row["payload_json"]),
                payload_size=len(str(row["payload_json"]).encode("utf-8")),
            )
            route_bytes = str(row["receipt_json"]).encode("utf-8")
            if hashlib.sha256(route_bytes).hexdigest() != row["receipt_hash"]:
                return None
            route = SignalRouteReceipt.model_validate_json(route_bytes)
            if route.signal_id != signal.signal_id:
                return None
            event_at = alert_event_at("signal", signal)
            received_at = normalize_aware_utc(datetime.fromisoformat(str(row["received_at"])))
            if event_at <= inspected_at and (
                signal.available_at > inspected_at
                or received_at > inspected_at
                or route.routed_at > inspected_at
            ):
                return None
            if first <= event_at <= inspected_at:
                window_records.append(ServingSignalRecord(global_sequence=expected, signal=signal))
            prefix_rows.append(
                (expected, str(row["signal_id"]), str(row["payload_hash"]), row["receipt_hash"])
            )
        selected_window = tuple(
            record
            for record in selected
            if first <= alert_event_at("signal", record.signal) <= inspected_at
        )
        digest = signal_window_digest(window_records)
        if (
            len(selected_window) != len(window_records)
            or signal_window_digest(selected_window) != digest
        ):
            return None
        return SignalObservedPrefixReceipt(
            source_generation_id=str(observation["source_generation_id"]),
            first_global_sequence=1,
            source_high_watermark=high,
            source_inspected_at=inspected_at,
            window_start=first,
            window_end=inspected_at,
            window_row_count=len(window_records),
            window_rows_sha256=digest,
            prefix_row_count=len(prefix_rows),
            prefix_rows_sha256=canonical_sha256(
                {"contract": "signal-source-prefix/v1", "rows": prefix_rows}
            ),
        )

    def _condition_alert_failpoint(self, stage: str) -> None:
        del stage

    def install_condition_alert_delivery_v1(
        self, activation: ConditionAlertRuntimeActivation
    ) -> None:
        from rquant.condition_alert_runtime_projection import install_condition_alert_delivery

        install_condition_alert_delivery(self, activation)

    def condition_alert_delivery_authority(self) -> ConditionAlertDeliveryAuthoritySnapshot | None:
        from rquant.condition_alert_runtime_projection import condition_delivery_authority

        with self._read_snapshot() as connection:
            return condition_delivery_authority(connection)

    def apply_condition_alert_delivery_authority(
        self,
        value: ConditionAlertDeliveryAuthorityInput,
        *,
        activation: ConditionAlertRuntimeActivation,
        expected_revision: int,
        applied_at: datetime,
        builtin_inspection: object | None = None,
    ) -> ConditionAlertDeliveryAuthoritySnapshot:
        from rquant.condition_alert_runtime_projection import (
            apply_condition_alert_delivery_authority,
        )

        return apply_condition_alert_delivery_authority(
            self,
            value,
            activation=activation,
            expected_revision=expected_revision,
            applied_at=applied_at,
            builtin_inspection=builtin_inspection,
        )

    def condition_alert_send_admission(
        self, outbox_id: str, attempt_no: int
    ) -> ConditionAlertSendAdmission | None:
        from rquant.condition_alert_runtime_projection import condition_send_admission

        with self._read_snapshot() as connection:
            return condition_send_admission(connection, outbox_id, attempt_no)

    def admit_condition_alert_delivery(
        self,
        record: OutboxRecord,
        *,
        activation: ConditionAlertRuntimeActivation,
        worker_id: str,
        expected_revision: int,
        admitted_at: datetime,
    ) -> ConditionAlertAdmittedDelivery | None:
        from rquant.condition_alert_runtime_projection import admit_condition_alert_delivery

        return admit_condition_alert_delivery(
            self,
            record,
            activation=activation,
            worker_id=worker_id,
            expected_revision=expected_revision,
            admitted_at=admitted_at,
        )

    def cancel_condition_unadmitted(
        self,
        outbox_id: str,
        *,
        worker_id: str | None,
        expected_revision: int,
        cancelled_at: datetime,
    ) -> ConditionAlertCancellationReceipt | None:
        from rquant.condition_alert_runtime_projection import cancel_condition_unadmitted

        return cancel_condition_unadmitted(
            self,
            outbox_id,
            worker_id=worker_id,
            expected_revision=expected_revision,
            cancelled_at=cancelled_at,
        )

    def claim_due_with_condition_activation(
        self,
        worker_id: str,
        *,
        activation: ConditionAlertRuntimeActivation,
        now: datetime,
        lease_for: timedelta,
        limit: int,
        include_price: bool = False,
    ) -> tuple[OutboxRecord, ...]:
        from rquant.condition_alert_runtime_contracts import require_verified_condition_activation
        from rquant.condition_alert_runtime_projection import (
            ConditionAlertAuthorityUnavailable,
            condition_delivery_authority,
            fresh_condition_authority,
            builtin_condition_authority_ready,
        )

        binding = require_verified_condition_activation(activation, "notifier")
        ready, builtin_ready = False, False
        if binding.delivery_enabled:
            try:
                with self._read_snapshot() as connection:
                    fresh_condition_authority(
                        condition_delivery_authority(connection), normalize_aware_utc(now)
                    )
                ready = True
            except ConditionAlertAuthorityUnavailable:
                pass
            with self._read_snapshot() as connection:
                builtin_ready = builtin_condition_authority_ready(condition_delivery_authority(connection), normalize_aware_utc(now))
        return self._claim_due(
            worker_id,
            now=now,
            lease_for=lease_for,
            limit=limit,
            include_price=include_price,
            include_condition=ready,
            include_builtin=builtin_ready,
        )

    def install_price_alert_delivery_v1(self, activation: object) -> None:
        from rquant.price_alert_runtime_projection import install_price_alert_delivery

        install_price_alert_delivery(self, activation)

    def apply_price_alert_delivery_authority(
        self, value: object, *, activation: object, expected_revision: int, applied_at: datetime
    ) -> object:
        from rquant.price_alert_runtime_projection import apply_price_alert_delivery_authority

        return apply_price_alert_delivery_authority(
            self,
            value,
            activation=activation,
            expected_revision=expected_revision,
            applied_at=applied_at,
        )

    def price_alert_delivery_authority(self) -> object:
        from rquant.price_alert_runtime_projection import _head

        with self._read_snapshot() as connection:
            return _head(connection)

    def price_alert_send_admission(self, outbox_id: str, attempt_no: int) -> object:
        from rquant.price_alert_runtime_projection import _admission

        with self._read_snapshot() as connection:
            return _admission(connection, outbox_id, attempt_no)

    def claim_due_with_price_activation(
        self, worker_id: str, *, activation: object, now: datetime, lease_for: timedelta, limit: int
    ) -> tuple:
        from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation
        from rquant.price_alert_runtime_projection import (
            PriceAlertAuthorityUnavailable,
            _fresh_authority,
            _head,
        )

        binding = require_verified_price_alert_activation(activation, "notifier")
        if not binding.delivery_enabled:
            return self.claim_due(worker_id, now=now, lease_for=lease_for, limit=limit)
        try:
            with self._read_snapshot() as connection:
                _fresh_authority(_head(connection), normalize_aware_utc(now))
        except PriceAlertAuthorityUnavailable:
            return self.claim_due(worker_id, now=now, lease_for=lease_for, limit=limit)
        return self._claim_due(
            worker_id, now=now, lease_for=lease_for, limit=limit, include_price=True
        )

    def admit_price_alert_delivery(
        self,
        record: object,
        *,
        activation: object,
        worker_id: str,
        expected_revision: int,
        admitted_at: datetime,
    ) -> object:
        from rquant.price_alert_runtime_projection import admit_price_alert_delivery

        return admit_price_alert_delivery(
            self,
            record,
            activation=activation,
            worker_id=worker_id,
            expected_revision=expected_revision,
            admitted_at=admitted_at,
        )

    def cancel_price_unadmitted(
        self,
        outbox_id: str,
        *,
        expected_revision: int,
        cancelled_at: datetime,
        worker_id: str | None = None,
    ) -> object:
        from rquant.price_alert_runtime_projection import cancel_price_unadmitted

        return cancel_price_unadmitted(
            self,
            outbox_id,
            expected_revision=expected_revision,
            cancelled_at=cancelled_at,
            worker_id=worker_id,
        )

    def serving_snapshot(
        self,
        *,
        observed_at: datetime,
        history_limit: int,
    ) -> NotificationServingSnapshot:
        return self._serving_snapshot(observed_at=observed_at, history_limit=history_limit)

    def serving_condition_enabled_snapshot(
        self,
        *,
        condition_producer: ConditionAlertRuntimeStore,
        condition_activation: ConditionAlertRuntimeActivation,
        observed_at: datetime,
        history_limit: int,
        price_producer: ReadonlyPriceAlertRuntimeStore | None = None,
        price_activation: PriceAlertRuntimeActivation | None = None,
        shadow: bool = False,
    ) -> NotificationServingSnapshot:
        from rquant.condition_alert_runtime import (
            ConditionAlertRuntimeStore,
            condition_producer_snapshot,
        )
        from rquant.condition_alert_runtime_contracts import require_verified_condition_activation

        require_verified_condition_activation(condition_activation, "notifier")
        if type(condition_producer) is not ConditionAlertRuntimeStore:
            raise TypeError("condition projection needs its actual borrowed producer")
        try:
            facts = condition_producer_snapshot(condition_producer, observed_at=observed_at)
        except (ValueError, OSError, sqlite3.Error):
            facts = None
        builtin_facts = None
        if self.merge_binding is not None:
            from rquant.monitor_builtin_runtime import read_builtin_serving_facts, read_installed_builtin_captures

            try:
                captured = read_installed_builtin_captures(condition_producer, observed_at=observed_at)
                builtin_facts = read_builtin_serving_facts(condition_producer, captured=captured,
                    observed_at=observed_at, history_limit=history_limit)
            except (ValueError, OSError, sqlite3.Error):
                builtin_facts = None
        price_facts = None
        if price_producer is not None:
            from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation

            require_verified_price_alert_activation(price_activation, "notifier")
            try:
                price_facts = price_producer.runtime_snapshot(observed_at=observed_at)
            except (ValueError, OSError, sqlite3.Error):
                price_facts = None
        return self._serving_snapshot(
            observed_at=observed_at,
            history_limit=history_limit,
            price_facts=price_facts,
            price_shadow=shadow,
            price_domain_unavailable=price_producer is not None and price_facts is None,
            condition_facts=facts,
            condition_domain_unavailable=facts is None,
            builtin_facts=builtin_facts,
        )

    def serving_price_enabled_snapshot(
        self,
        *,
        producer: ReadonlyPriceAlertRuntimeStore,
        activation: PriceAlertRuntimeActivation,
        observed_at: datetime,
        history_limit: int,
        shadow: bool,
    ) -> NotificationServingSnapshot:
        from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation
        from rquant.price_alert_runtime_store import ReadonlyPriceAlertRuntimeStore

        require_verified_price_alert_activation(activation, "notifier")
        if type(producer) is not ReadonlyPriceAlertRuntimeStore or type(shadow) is not bool:
            raise TypeError("price Serving requires its actual registered read-only producer")
        unavailable = False
        try:
            facts = producer.runtime_snapshot(observed_at=observed_at)
        except (OSError, ValueError, sqlite3.Error):
            facts, unavailable = None, True
        return self._serving_snapshot(
            observed_at=observed_at,
            history_limit=history_limit,
            price_facts=facts,
            price_shadow=shadow,
            price_domain_unavailable=unavailable,
        )

    def _serving_snapshot(
        self,
        *,
        observed_at: datetime,
        history_limit: int,
        price_facts: PriceProducerRuntimeSnapshot | None = None,
        price_shadow: bool = False,
        price_domain_unavailable: bool = False,
        condition_facts: ConditionProducerRuntimeSnapshot | None = None,
        condition_domain_unavailable: bool = False,
        builtin_facts: object | None = None,
    ) -> NotificationServingSnapshot:
        from rquant.runtime_serving_snapshot import SignalDeliveryReadPayload
        from rquant.serving_read_models import (
            _MAX_OWNER_PROJECTION_BYTES,
            PAGE_PROJECTION_CONTRACTS,
            ServingProjectionInput,
            ServingReadModelInput,
            ServingSignalRecord,
            _projection_json_bytes,
        )

        observed = normalize_aware_utc(observed_at)
        if (
            not isinstance(history_limit, int)
            or isinstance(history_limit, bool)
            or not 1 <= history_limit <= 10_000
        ):
            raise ValueError("history_limit must be an integer between 1 and 10000")

        connection = self._connect_readonly()
        try:
            connection.execute("BEGIN")
            revision_row = connection.execute(
                "SELECT revision FROM notification_state_revision WHERE singleton = 1"
            ).fetchone()
            if revision_row is None:
                raise RuntimeError("notification state revision is missing")
            observed_text = observed.isoformat(timespec="microseconds")
            projection_row = connection.execute(
                """
                SELECT payload_json
                FROM notification_projection_authority
                WHERE available_at <= ? AND observed_at <= ?
                ORDER BY available_at DESC, observed_at DESC, generation_id DESC
                LIMIT 1
                """,
                (observed_text, observed_text),
            ).fetchone()
            projection_snapshot = (
                None
                if projection_row is None
                else NotificationProjectionAuthoritySnapshot.model_validate_json(
                    projection_row["payload_json"]
                )
            )
            observed_text = observed.isoformat(timespec="microseconds").replace("+00:00", "Z")
            verified: dict[
                int,
                tuple[SignalEnvelopeFamily, SignalRouteReceipt, bool],
            ] = {}
            callback_errors: list[Exception] = []

            def notification_visible(
                global_sequence: int,
                stored_signal_id: str,
                payload_hash: str,
                payload_json: str,
                payload_size: int,
                received_at_text: str,
                receipt_signal_id: str,
                receipt_hash: str,
                receipt_json: str,
            ) -> int:
                sequence = int(global_sequence)
                cached = verified.get(sequence)
                if cached is not None:
                    return int(cached[2])
                try:
                    signal = parse_stored_signal(
                        signal_id=str(stored_signal_id),
                        payload_hash=str(payload_hash),
                        payload_json=str(payload_json),
                        payload_size=int(payload_size),
                    )
                    receipt_payload = str(receipt_json)
                    if hashlib.sha256(receipt_payload.encode()).hexdigest() != str(receipt_hash):
                        raise NotificationReplicationError(
                            "stored notification receipt hash does not match its payload"
                        )
                    route = SignalRouteReceipt.model_validate_json(receipt_payload)
                    if (
                        receipt_signal_id != stored_signal_id
                        or route.signal_id != receipt_signal_id
                    ):
                        raise NotificationReplicationError(
                            "stored notification receipt signal_id does not match signal payload"
                        )
                    received_at = normalize_aware_utc(datetime.fromisoformat(str(received_at_text)))
                    visible = (
                        signal.available_at <= observed
                        and received_at <= observed
                        and route.routed_at <= observed
                    )
                    verified[sequence] = (signal, route, visible)
                    return int(visible)
                except Exception as exc:
                    if not callback_errors:
                        callback_errors.append(exc)
                    return 0

            connection.create_function(
                "rquant_notification_visible",
                9,
                notification_visible,
            )
            visible_predicate = """
                rquant_notification_visible(
                    signal.global_sequence,
                    signal.signal_id,
                    signal.payload_hash,
                    signal.payload_json,
                    length(CAST(signal.payload_json AS BLOB)),
                    signal.received_at,
                    receipt.signal_id,
                    receipt.receipt_hash,
                    receipt.receipt_json
                ) = 1
            """
            visible_row = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM notification_source_route_receipt AS receipt
                JOIN signal_envelope AS signal
                  ON signal.global_sequence = receipt.global_sequence
                WHERE {visible_predicate}
                """,
            ).fetchone()
            if callback_errors:
                raise callback_errors[0]
            visible_signal_count = 0 if visible_row is None else int(visible_row[0])
            rows = connection.execute(
                f"""
                SELECT signal.global_sequence
                FROM notification_source_route_receipt AS receipt
                JOIN signal_envelope AS signal
                  ON signal.global_sequence = receipt.global_sequence
                WHERE {visible_predicate}
                ORDER BY signal.global_sequence DESC
                LIMIT ?
                """,
                (history_limit,),
            ).fetchall()
            if callback_errors:
                raise callback_errors[0]

            selected: list[tuple[ServingSignalRecord, SignalRouteReceipt]] = []
            for row in reversed(rows):
                signal, route, visible = verified[int(row["global_sequence"])]
                if not visible:
                    raise NotificationReplicationError(
                        "SQL-visible notification evidence was not dispatcher-visible"
                    )
                selected.append(
                    (
                        ServingSignalRecord(
                            global_sequence=row["global_sequence"],
                            signal=signal,
                        ),
                        route,
                    )
                )

            signal_records = tuple(item[0] for item in selected)
            routes = tuple(item[1] for item in selected)
            signal_ids = tuple(record.signal.signal_id for record in signal_records)
            deliveries = ()
            if signal_ids:
                placeholders = ",".join("?" for _ in signal_ids)
                delivery_rows = connection.execute(
                    f"""
                    SELECT * FROM delivery_outbox
                    WHERE signal_id IN ({placeholders})
                      AND created_at <= ?
                      AND updated_at <= ?
                    ORDER BY global_sequence, recipient_id, channel, outbox_id
                    LIMIT ?
                    """,
                    (
                        *signal_ids,
                        observed_text,
                        observed_text,
                        _MAX_SERVING_DELIVERIES + 1,
                    ),
                ).fetchall()
                if len(delivery_rows) > _MAX_SERVING_DELIVERIES:
                    raise NotificationReplicationError(
                        "notification serving deliveries exceed the bounded projection limit"
                    )
                deliveries = tuple(self._outbox_from_row(row) for row in delivery_rows)
            omitted = visible_signal_count - len(selected)
            observed_prefix = self._observed_signal_prefix_from_transaction(
                connection,
                observed_at=observed,
                selected=signal_records,
                truncated=omitted > 0,
            )
            price_projections = ()

            def require_joint_projection_budget(
                values: tuple[ServingProjectionPayload, ...],
            ) -> None:
                owners: dict[str, int] = {}
                legacy = () if projection_snapshot is None else projection_snapshot.projections
                for value in legacy + values:
                    owner = PAGE_PROJECTION_CONTRACTS[value.table_name].owner_dataset_id
                    bound = ServingProjectionInput.bind(
                        value, owner_dataset_id=owner, owner_generation_id="0" * 64
                    )
                    owners[owner] = owners.get(owner, 0) + _projection_json_bytes(bound)
                if any(size > _MAX_OWNER_PROJECTION_BYTES for size in owners.values()):
                    raise ValueError("notification projections exceed their owner byte budget")

            if price_facts is not None or price_domain_unavailable:
                from rquant.price_alert_runtime_projection import (
                    price_runtime_projections,
                    unavailable_price_runtime_projections,
                )

                try:
                    if price_domain_unavailable:
                        price_projections = unavailable_price_runtime_projections(
                            observed_at=observed, shadow=price_shadow
                        )
                    else:
                        price_projections = price_runtime_projections(
                            connection,
                            producer=price_facts,
                            observed_at=observed,
                            shadow=price_shadow,
                        )
                    require_joint_projection_budget(price_projections)
                except (TypeError, ValueError, OSError, sqlite3.Error):
                    price_projections = unavailable_price_runtime_projections(
                        observed_at=observed, shadow=price_shadow
                    )
                    require_joint_projection_budget(price_projections)
            condition_projections = ()
            if condition_facts is not None or condition_domain_unavailable:
                from rquant.condition_alert_runtime_projection import (
                    condition_runtime_projections,
                    unavailable_condition_runtime_projections,
                )

                try:
                    condition_projections = (
                        condition_runtime_projections(
                            connection,
                            producer=condition_facts,
                            observed_at=observed,
                            history_limit=history_limit,
                        )
                        if condition_facts is not None
                        else unavailable_condition_runtime_projections(observed_at=observed)
                    )
                    from rquant.condition_alert_runtime_projection import (
                        validate_condition_runtime_projections,
                    )

                    validate_condition_runtime_projections(
                        {item.table_name: item for item in condition_projections}
                    )
                    require_joint_projection_budget(price_projections + condition_projections)
                except (TypeError, ValueError, sqlite3.Error):
                    condition_projections = unavailable_condition_runtime_projections(
                        observed_at=observed
                    )
                    require_joint_projection_budget(price_projections + condition_projections)
            monitor_projections = ()
            if self.merge_binding is not None:
                monitor_projections = self._monitor_runtime_projections(connection, observed_at=observed,
                    history_limit=history_limit, builtin_facts=builtin_facts)
                require_joint_projection_budget(price_projections + condition_projections + monitor_projections)
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

        coherent = ServingReadModelInput(
            observed_at=observed,
            signals=signal_records,
            routes=routes,
            deliveries=deliveries,
        )
        payload = SignalDeliveryReadPayload(
            signals=coherent.signals,
            routes=coherent.routes,
            deliveries=coherent.deliveries,
            projections=(() if projection_snapshot is None else projection_snapshot.projections)
            + price_projections
            + condition_projections
            + monitor_projections,
        )
        return NotificationServingSnapshot(
            observed_at=observed,
            sequence=int(revision_row["revision"]),
            visible_signal_count=visible_signal_count,
            returned_signal_count=len(selected),
            omitted_signal_count=omitted,
            truncated=omitted > 0,
            payload=payload,
            projection_generation_id=(
                None if projection_snapshot is None else projection_snapshot.generation_id
            ),
            projection_source_receipts=(
                {} if projection_snapshot is None else projection_snapshot.source_receipts
            ),
            signal_observed_prefix=observed_prefix,
        )

    def apply_recipient_alias_migrations(
        self,
        *,
        recipient_ids: Mapping[DeliveryChannel, tuple[str, ...]],
        aliases: Mapping[DeliveryChannel, Mapping[str, tuple[str, ...]]],
        observed_at: datetime,
    ) -> NotificationRecipientMigrationSummary:
        observed = normalize_aware_utc(observed_at)
        allowed = self._normalize_recipient_ids(recipient_ids)
        normalized_aliases = self._normalize_recipient_aliases(aliases, allowed=allowed)
        migrated = 0
        created = 0
        preserved = 0
        audit_ids: list[str] = []
        with self._write_transaction() as connection:
            for (channel, source_recipient_id), targets in normalized_aliases.items():
                alias_identity = {
                    "contract": "notification-recipient-alias/v1",
                    "channel": channel,
                    "source_recipient_id": source_recipient_id,
                    "target_recipient_ids": targets,
                }
                alias_fingerprint = canonical_sha256(alias_identity)
                target_json = json.dumps(targets, ensure_ascii=True, separators=(",", ":"))
                existing = connection.execute(
                    """
                    SELECT target_recipient_ids_json, alias_fingerprint
                    FROM notification_recipient_alias_binding
                    WHERE channel = ? AND source_recipient_id = ?
                    """,
                    (channel.value, source_recipient_id),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO notification_recipient_alias_binding(
                            channel, source_recipient_id, target_recipient_ids_json,
                            alias_fingerprint, created_at
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            channel.value,
                            source_recipient_id,
                            target_json,
                            alias_fingerprint,
                            observed.isoformat(timespec="microseconds"),
                        ),
                    )
                elif (
                    existing["target_recipient_ids_json"] != target_json
                    or existing["alias_fingerprint"] != alias_fingerprint
                ):
                    raise NotificationReplicationError(
                        "notification recipient alias conflicts with frozen migration"
                    )

            rows = connection.execute(
                "SELECT * FROM delivery_outbox WHERE signal_id NOT IN "
                "(SELECT signal_id FROM signal_envelope WHERE "
                "json_extract(payload_json,'$.envelope_schema')='rquant.price"
                "-alert-event/v1' OR json_extract(payload_json,'$.envelope_schema')="
                "'rquant.condition-alert-event/v1') ORDER BY global_sequence, outbox_id"
            ).fetchall()
            for row in rows:
                channel = DeliveryChannel(row["channel"])
                recipient_id = str(row["recipient_id"])
                if recipient_id in allowed.get(channel, ()):
                    continue
                targets = normalized_aliases.get((channel, recipient_id))
                record = self._outbox_from_row(row)
                if targets is None:
                    if (
                        record.status
                        in {
                            OutboxStatus.PENDING,
                            OutboxStatus.RETRY,
                            OutboxStatus.LEASED,
                        }
                        and record.expires_at > observed
                    ):
                        raise NotificationReplicationError(
                            f"active notification recipient is unknown: "
                            f"{channel.value}/{recipient_id}"
                        )
                    continue

                original_hash = canonical_sha256(record)
                alias_fingerprint = canonical_sha256(
                    {
                        "contract": "notification-recipient-alias/v1",
                        "channel": channel,
                        "source_recipient_id": recipient_id,
                        "target_recipient_ids": targets,
                    }
                )
                migration_identity = {
                    "contract": "notification-recipient-migration/v1",
                    "alias_fingerprint": alias_fingerprint,
                    "source_outbox_id": record.outbox_id,
                    "original_record_hash": original_hash,
                }
                migration_id = canonical_sha256(migration_identity)
                existing_audit = connection.execute(
                    """
                    SELECT * FROM notification_recipient_migration_audit
                    WHERE source_outbox_id = ?
                    """,
                    (record.outbox_id,),
                ).fetchone()
                if existing_audit is not None:
                    audit = self._recipient_migration_from_row(existing_audit)
                    if (
                        audit.migration_id != migration_id
                        or audit.alias_fingerprint != alias_fingerprint
                        or audit.original_record_hash != original_hash
                    ):
                        raise NotificationReplicationError(
                            "notification recipient migration conflicts with audit history"
                        )
                    audit_ids.append(audit.migration_id)
                    continue

                target_outbox_ids: tuple[str, ...] = ()
                if record.status is OutboxStatus.SUCCEEDED:
                    outcome = "preserved_succeeded"
                    preserved += 1
                elif record.status in {OutboxStatus.EXPIRED, OutboxStatus.DEAD_LETTER} or (
                    record.expires_at <= observed
                ):
                    outcome = "preserved_terminal"
                    preserved += 1
                else:
                    if record.status is not OutboxStatus.PENDING or record.attempt_count != 0:
                        raise NotificationReplicationError(
                            "notification recipient migration has ambiguous delivery history"
                        )
                    attempt = connection.execute(
                        """
                        SELECT 1 FROM delivery_attempt WHERE outbox_id = ?
                        UNION ALL
                        SELECT 1 FROM delivery_unknown WHERE outbox_id = ?
                        LIMIT 1
                        """,
                        (record.outbox_id, record.outbox_id),
                    ).fetchone()
                    if attempt is not None:
                        raise NotificationReplicationError(
                            "notification recipient migration has delivery evidence"
                        )
                    generated_ids: list[str] = []
                    for target_recipient_id in targets:
                        target = DeliveryTarget(
                            recipient_id=target_recipient_id,
                            channel=channel,
                        )
                        outbox_id = target.delivery_key(record.signal_id)
                        if (
                            connection.execute(
                                "SELECT 1 FROM delivery_outbox WHERE outbox_id = ?",
                                (outbox_id,),
                            ).fetchone()
                            is not None
                        ):
                            raise NotificationReplicationError(
                                "notification recipient migration target already exists"
                            )
                        connection.execute(
                            """
                            INSERT INTO delivery_outbox(
                                outbox_id, signal_id, global_sequence, recipient_id, channel,
                                status, expires_at, attempt_count, next_attempt_at,
                                lease_owner, lease_started_at, lease_until, last_error,
                                created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL,
                                      NULL, ?, ?)
                            """,
                            (
                                outbox_id,
                                record.signal_id,
                                row["global_sequence"],
                                target_recipient_id,
                                channel.value,
                                OutboxStatus.PENDING.value,
                                record.expires_at.isoformat(timespec="microseconds"),
                                record.created_at.isoformat(timespec="microseconds"),
                                observed.isoformat(timespec="microseconds"),
                            ),
                        )
                        generated_ids.append(outbox_id)
                    connection.execute(
                        "DELETE FROM delivery_outbox WHERE outbox_id = ?",
                        (record.outbox_id,),
                    )
                    target_outbox_ids = tuple(generated_ids)
                    outcome = "migrated"
                    migrated += 1
                    created += len(generated_ids)

                audit = NotificationRecipientMigrationAudit(
                    migration_id=migration_id,
                    alias_fingerprint=alias_fingerprint,
                    source_outbox_id=record.outbox_id,
                    signal_id=record.signal_id,
                    channel=channel,
                    source_recipient_id=recipient_id,
                    target_recipient_ids=targets,
                    target_outbox_ids=target_outbox_ids,
                    outcome=outcome,
                    original_record_hash=original_hash,
                    observed_at=observed,
                )
                connection.execute(
                    """
                    INSERT INTO notification_recipient_migration_audit(
                        migration_id, alias_fingerprint, source_outbox_id, signal_id,
                        channel, source_recipient_id, target_recipient_ids_json,
                        target_outbox_ids_json, outcome, original_record_hash, observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        audit.migration_id,
                        audit.alias_fingerprint,
                        audit.source_outbox_id,
                        audit.signal_id,
                        audit.channel.value,
                        audit.source_recipient_id,
                        json.dumps(
                            audit.target_recipient_ids,
                            ensure_ascii=True,
                            separators=(",", ":"),
                        ),
                        json.dumps(
                            audit.target_outbox_ids,
                            ensure_ascii=True,
                            separators=(",", ":"),
                        ),
                        audit.outcome,
                        audit.original_record_hash,
                        audit.observed_at.isoformat(timespec="microseconds"),
                    ),
                )
                audit_ids.append(audit.migration_id)
            self._before_commit(connection)

        return NotificationRecipientMigrationSummary(
            alias_binding_count=len(normalized_aliases),
            migrated_outbox_count=migrated,
            created_outbox_count=created,
            preserved_outbox_count=preserved,
            audit_ids=tuple(audit_ids),
        )

    def recipient_migration_audits(self) -> tuple[NotificationRecipientMigrationAudit, ...]:
        connection = self._connect_readonly()
        try:
            rows = connection.execute(
                """
                SELECT * FROM notification_recipient_migration_audit
                ORDER BY observed_at, migration_id
                """
            ).fetchall()
        finally:
            connection.close()
        return tuple(self._recipient_migration_from_row(row) for row in rows)

    def record_serving_authority_handoff(
        self,
        *,
        previous_producer_commit: str,
        next_producer_commit: str,
        previous_generation_id: str,
        business_content_hash: str,
        previous_sequence: int,
        observed_at: datetime,
    ) -> NotificationAuthorityHandoff:
        observed = normalize_aware_utc(observed_at)
        identity = {
            "previous_producer_commit": previous_producer_commit,
            "next_producer_commit": next_producer_commit,
            "previous_generation_id": previous_generation_id,
            "business_content_hash": business_content_hash,
            "previous_sequence": previous_sequence,
            "next_sequence": previous_sequence + 1,
        }
        handoff = NotificationAuthorityHandoff(
            handoff_id=canonical_sha256(identity),
            observed_at=observed,
            **identity,
        )
        with self._write_transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM notification_authority_handoff WHERE handoff_id = ?",
                (handoff.handoff_id,),
            ).fetchone()
            if existing is not None:
                return self._handoff_from_row(existing)
            revision_row = connection.execute(
                "SELECT revision FROM notification_state_revision WHERE singleton = 1"
            ).fetchone()
            if revision_row is None:
                raise RuntimeError("notification state revision is missing")
            if int(revision_row["revision"]) != previous_sequence:
                raise NotificationReplicationError(
                    "notification authority handoff sequence does not match local state"
                )
            try:
                connection.execute(
                    """
                    INSERT INTO notification_authority_handoff(
                        handoff_id, previous_producer_commit, next_producer_commit,
                        previous_generation_id, business_content_hash,
                        previous_sequence, next_sequence, observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        handoff.handoff_id,
                        handoff.previous_producer_commit,
                        handoff.next_producer_commit,
                        handoff.previous_generation_id,
                        handoff.business_content_hash,
                        handoff.previous_sequence,
                        handoff.next_sequence,
                        handoff.observed_at.isoformat(timespec="microseconds"),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise NotificationReplicationError(
                    "notification authority handoff conflicts with frozen history"
                ) from exc
            connection.execute(
                """
                UPDATE notification_state_revision
                SET revision = ?
                WHERE singleton = 1 AND revision = ?
                """,
                (handoff.next_sequence, handoff.previous_sequence),
            )
            self._before_commit(connection)
        return handoff

    def serving_authority_handoffs(self) -> tuple[NotificationAuthorityHandoff, ...]:
        connection = self._connect_readonly()
        try:
            rows = connection.execute(
                """
                SELECT * FROM notification_authority_handoff
                ORDER BY next_sequence, handoff_id
                """
            ).fetchall()
        finally:
            connection.close()
        return tuple(self._handoff_from_row(row) for row in rows)

    @staticmethod
    def _receipt_payload(record: SignalBusRoutedRecord) -> str:
        return json.dumps(
            record.receipt.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )

    def _store_source_receipt(
        self,
        connection: sqlite3.Connection,
        record: SignalBusRoutedRecord,
    ) -> None:
        payload = self._receipt_payload(record)
        payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        existing = connection.execute(
            """
            SELECT signal_id, receipt_hash, receipt_json
            FROM notification_source_route_receipt
            WHERE global_sequence = ? OR signal_id = ?
            """,
            (record.global_sequence, record.signal_id),
        ).fetchone()
        if existing is not None:
            if (
                existing["signal_id"] != record.signal_id
                or existing["receipt_hash"] != payload_hash
                or existing["receipt_json"] != payload
            ):
                raise NotificationReplicationError(
                    "notification source route receipt conflicts with local state"
                )
            return
        connection.execute(
            """
            INSERT INTO notification_source_route_receipt(
                global_sequence, signal_id, receipt_hash, receipt_json
            ) VALUES (?, ?, ?, ?)
            """,
            (record.global_sequence, record.signal_id, payload_hash, payload),
        )

    def _verify_replayed_record(
        self,
        connection: sqlite3.Connection,
        record: SignalBusRoutedRecord,
    ) -> None:
        signal_row = connection.execute(
            """
            SELECT signal_id, payload_hash, payload_json, received_at
            FROM signal_envelope
            WHERE global_sequence = ?
            """,
            (record.global_sequence,),
        ).fetchone()
        if (
            signal_row is None
            or signal_row["signal_id"] != record.signal_id
            or signal_row["payload_hash"] != record.payload_hash
            or signal_row["payload_json"] != record.payload_json
            or normalize_aware_utc(datetime.fromisoformat(signal_row["received_at"]))
            != normalize_aware_utc(record.received_at)
        ):
            raise NotificationReplicationError(
                "notification replay signal conflicts with local state"
            )
        self._store_source_receipt(connection, record)

    def _after_replicated_signal(self) -> None:
        """Fault-injection boundary before the atomic replication commit."""

    @staticmethod
    def _cursor_from_row(row: sqlite3.Row) -> NotificationReplicationCursor:
        updated = datetime.fromisoformat(str(row["updated_at"]))
        return NotificationReplicationCursor(
            source_id=row["source_id"],
            source_generation_id=row["source_generation_id"],
            first_global_sequence=row["first_global_sequence"],
            observed_high_watermark=row["observed_high_watermark"],
            last_global_sequence=row["last_global_sequence"],
            last_signal_id=row["last_signal_id"],
            updated_at=updated,
        )

    @staticmethod
    def _handoff_from_row(row: sqlite3.Row) -> NotificationAuthorityHandoff:
        return NotificationAuthorityHandoff(
            handoff_id=row["handoff_id"],
            previous_producer_commit=row["previous_producer_commit"],
            next_producer_commit=row["next_producer_commit"],
            previous_generation_id=row["previous_generation_id"],
            business_content_hash=row["business_content_hash"],
            previous_sequence=row["previous_sequence"],
            next_sequence=row["next_sequence"],
            observed_at=datetime.fromisoformat(row["observed_at"]),
        )

    @staticmethod
    def _normalize_recipient_ids(
        recipient_ids: Mapping[DeliveryChannel, tuple[str, ...]],
    ) -> dict[DeliveryChannel, tuple[str, ...]]:
        normalized: dict[DeliveryChannel, tuple[str, ...]] = {}
        for channel, values in recipient_ids.items():
            if not isinstance(channel, DeliveryChannel):
                raise TypeError("notification recipient channel must be a DeliveryChannel")
            recipients = tuple(value.strip() for value in values)
            if not recipients or any(not value for value in recipients):
                raise ValueError("notification physical recipient ids must be nonempty")
            if len(recipients) != len(set(recipients)):
                raise ValueError("notification physical recipient ids must be unique")
            normalized[channel] = recipients
        return normalized

    @staticmethod
    def _normalize_recipient_aliases(
        aliases: Mapping[DeliveryChannel, Mapping[str, tuple[str, ...]]],
        *,
        allowed: Mapping[DeliveryChannel, tuple[str, ...]],
    ) -> dict[tuple[DeliveryChannel, str], tuple[str, ...]]:
        normalized: dict[tuple[DeliveryChannel, str], tuple[str, ...]] = {}
        for channel, channel_aliases in aliases.items():
            if not isinstance(channel, DeliveryChannel):
                raise TypeError("notification alias channel must be a DeliveryChannel")
            for source, raw_targets in channel_aliases.items():
                source_id = source.strip()
                targets = tuple(target.strip() for target in raw_targets)
                if not source_id or not targets or any(not target for target in targets):
                    raise ValueError("notification recipient alias is incomplete")
                if source_id in targets or len(targets) != len(set(targets)):
                    raise ValueError("notification recipient alias is ambiguous")
                if not set(targets) <= set(allowed.get(channel, ())):
                    raise ValueError("notification recipient alias target has no capability")
                normalized[(channel, source_id)] = targets
        return normalized

    @staticmethod
    def _recipient_migration_from_row(
        row: sqlite3.Row,
    ) -> NotificationRecipientMigrationAudit:
        return NotificationRecipientMigrationAudit(
            migration_id=row["migration_id"],
            alias_fingerprint=row["alias_fingerprint"],
            source_outbox_id=row["source_outbox_id"],
            signal_id=row["signal_id"],
            channel=DeliveryChannel(row["channel"]),
            source_recipient_id=row["source_recipient_id"],
            target_recipient_ids=tuple(json.loads(row["target_recipient_ids_json"])),
            target_outbox_ids=tuple(json.loads(row["target_outbox_ids_json"])),
            outcome=row["outcome"],
            original_record_hash=row["original_record_hash"],
            observed_at=datetime.fromisoformat(row["observed_at"]),
        )


__all__ = [
    "NotificationAuthorityHandoff",
    "NotificationProjectionAuthoritySnapshot",
    "NotificationProjectionSourceReceipt",
    "NotificationRecipientMigrationAudit",
    "NotificationRecipientMigrationSummary",
    "NotificationReplicationCursor",
    "NotificationReplicationError",
    "NotificationReplicationSummary",
    "NotificationServingSnapshot",
    "NotificationStateStore",
]
