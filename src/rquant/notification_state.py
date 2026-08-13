"""Notifier-owned durable outbox replicated from an immutable signal spool."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, field_serializer, field_validator, model_validator

from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    OutboxStatus,
    RouterDisposition,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.signal_bus import (
    SignalBusRoutedRecord,
    SignalBusSourceDescriptor,
    SignalBusStore,
    SignalRouteReceipt,
)
from rquant.signal_contracts import SignalEnvelope

if TYPE_CHECKING:
    from rquant.runtime_serving_snapshot import SignalDeliveryPayload

from rquant.serving_read_models import ServingProjectionPayload

_REQUIRED_NOTIFICATION_PROJECTION_TABLES = frozenset(
    {
        "screen_result",
        "pool2_watch",
        "monitor_event",
        "surge_event",
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
    {"pulse_history", "pulse_alert", "surge_runtime_config"}
)
_NOTIFICATION_PROJECTION_TABLES = (
    _REQUIRED_NOTIFICATION_PROJECTION_TABLES | _OPTIONAL_NOTIFICATION_PROJECTION_TABLES
)
_MAX_SERVING_DELIVERIES = 10_000


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
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"receipt_id"}))
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
        return cls(**values, receipt_id=canonical_sha256(values))


class NotificationProjectionAuthoritySnapshot(RuntimeContractModel):
    """One bounded PIT publication for every notification-owned page projection."""

    schema_version: int = Field(default=1, ge=1)
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    source_receipts: Mapping[str, str] = Field(min_length=1)
    projections: tuple[ServingProjectionPayload, ...]
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
        if self.available_at > self.observed_at:
            raise ValueError("notification projection availability exceeds observation time")
        if any(projection.available_at > self.available_at for projection in self.projections):
            raise ValueError("notification projection contains future source evidence")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"generation_id"}))
        if self.generation_id != expected:
            raise ValueError("notification projection generation does not match content")
        return self

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
        return cls(**values, generation_id=canonical_sha256(values))

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


@dataclass(frozen=True)
class NotificationServingSnapshot:
    observed_at: AwareUtcDatetime
    sequence: int
    visible_signal_count: int
    returned_signal_count: int
    omitted_signal_count: int
    truncated: bool
    payload: SignalDeliveryPayload
    projection_generation_id: str | None = None
    projection_source_receipts: Mapping[str, str] = dataclass_field(default_factory=dict)

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
        **kwargs: object,
    ) -> None:
        normalized = source_id.strip()
        if not normalized:
            raise ValueError("notification source_id must not be empty")
        self.replication_source_id = normalized
        super().__init__(path, **kwargs)

    def _initialize(self) -> None:
        super()._initialize()
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
                    payload_json TEXT NOT NULL
                );
                """
            )
            for table in (
                "signal_envelope",
                "notification_source_route_receipt",
                "delivery_outbox",
                "delivery_attempt",
                "delivery_unknown",
                "notification_projection_authority",
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

    def publish_projection_authority(
        self,
        snapshot: NotificationProjectionAuthoritySnapshot,
    ) -> str:
        validated = NotificationProjectionAuthoritySnapshot.model_validate(snapshot)
        payload_json = validated.model_dump_json()
        observed_text = validated.observed_at.isoformat(timespec="microseconds")
        available_text = validated.available_at.isoformat(timespec="microseconds")
        receipts_json = json.dumps(
            dict(validated.source_receipts),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        with self._write_transaction() as connection:
            existing = connection.execute(
                "SELECT payload_json FROM notification_projection_authority "
                "WHERE generation_id = ?",
                (validated.generation_id,),
            ).fetchone()
            if existing is not None:
                if (
                    NotificationProjectionAuthoritySnapshot.model_validate_json(
                        existing["payload_json"]
                    )
                    != validated
                ):
                    raise NotificationReplicationError(
                        "notification projection generation conflicts with immutable content"
                    )
                return validated.generation_id
            connection.execute(
                """
                INSERT INTO notification_projection_authority(
                    generation_id, observed_at, available_at,
                    source_receipts_json, payload_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    validated.generation_id,
                    observed_text,
                    available_text,
                    receipts_json,
                    payload_json,
                ),
            )
        return validated.generation_id

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

    def replicate(
        self,
        source: SignalBusSourceDescriptor,
        records: tuple[SignalBusRoutedRecord, ...],
        *,
        observed_at: datetime,
    ) -> NotificationReplicationSummary:
        observed = normalize_aware_utc(observed_at)
        source = SignalBusSourceDescriptor.model_validate(source)
        records = tuple(SignalBusRoutedRecord.model_validate(record) for record in records)
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM notification_replication_source WHERE singleton = 1"
            ).fetchone()
            if row is None:
                started_after = source.first_global_sequence - 1
                observed_high = started_after
                last_signal_id = None
            else:
                cursor = self._cursor_from_row(row)
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
                    max(observed_high, source.high_watermark),
                    ended_at,
                    last_signal_id,
                    timestamp,
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

    def serving_snapshot(
        self,
        *,
        observed_at: datetime,
        history_limit: int,
    ) -> NotificationServingSnapshot:
        from rquant.runtime_serving_snapshot import SignalDeliveryPayload
        from rquant.serving_read_models import ServingReadModelInput, ServingSignalRecord

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
            visible_predicate = """
                julianday(signal.received_at) <= julianday(?)
                AND julianday(json_extract(signal.payload_json, '$.available_at'))
                    <= julianday(?)
                AND julianday(json_extract(receipt.receipt_json, '$.routed_at'))
                    <= julianday(?)
            """
            visible_row = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM notification_source_route_receipt AS receipt
                JOIN signal_envelope AS signal
                  ON signal.global_sequence = receipt.global_sequence
                 AND signal.signal_id = receipt.signal_id
                WHERE {visible_predicate}
                """,
                (observed_text, observed_text, observed_text),
            ).fetchone()
            visible_signal_count = 0 if visible_row is None else int(visible_row[0])
            rows = connection.execute(
                f"""
                SELECT signal.global_sequence, signal.payload_json, signal.received_at,
                       receipt.receipt_json
                FROM notification_source_route_receipt AS receipt
                JOIN signal_envelope AS signal
                  ON signal.global_sequence = receipt.global_sequence
                 AND signal.signal_id = receipt.signal_id
                WHERE {visible_predicate}
                ORDER BY signal.global_sequence DESC
                LIMIT ?
                """,
                (observed_text, observed_text, observed_text, history_limit),
            ).fetchall()

            selected: list[tuple[ServingSignalRecord, SignalRouteReceipt]] = []
            for row in reversed(rows):
                signal = SignalEnvelope.model_validate_json(row["payload_json"])
                route = SignalRouteReceipt.model_validate_json(row["receipt_json"])
                received_at = normalize_aware_utc(datetime.fromisoformat(row["received_at"]))
                if (
                    signal.available_at > observed
                    or received_at > observed
                    or route.routed_at > observed
                ):
                    raise NotificationReplicationError(
                        "SQL-visible notification evidence is future-dated"
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
        payload = SignalDeliveryPayload(
            signals=coherent.signals,
            routes=coherent.routes,
            deliveries=coherent.deliveries,
            projections=(() if projection_snapshot is None else projection_snapshot.projections),
        )
        omitted = visible_signal_count - len(selected)
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
                "SELECT * FROM delivery_outbox ORDER BY global_sequence, outbox_id"
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
