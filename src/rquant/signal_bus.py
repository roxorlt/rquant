"""Durable single-writer signal routing and notification outbox state."""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Self

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    OutboxAttempt,
    OutboxRecord,
    OutboxStatus,
    RouterDisposition,
    RouterReceipt,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)
from rquant.signal_contracts import (
    CurrentSignalEnvelope,
    SignalEnvelope,
    SignalEnvelopeFamily,
    parse_signal_envelope,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class SignalBusLeaseError(RuntimeError):
    """A stale or unrelated worker attempted to finish a delivery lease."""


class LegacySignalWriteActivationError(TypeError):
    """A current-family signal reached a legacy-only durable writer."""


class SignalRouteConflictError(RuntimeError):
    """An immutable source identity or route receipt was changed."""


class SignalRouteSequenceError(RuntimeError):
    """A source sequence or high watermark regressed, skipped, or disappeared."""


class SignalBusSourceSequenceError(RuntimeError):
    """The append-only global signal sequence was truncated or requested unsafely."""


class RouteReceiptDisposition(StrEnum):
    ROUTED = "routed"
    NO_TARGET = "no_target"
    EXPIRED = "expired"


class RouteDecisionKind(StrEnum):
    ROUTE = "route"
    NO_TARGET = "no_target"


class RouteSourceDescriptor(RuntimeContractModel):
    source_id: str = Field(min_length=1)
    generation_id: Sha256
    strategy_spec_fingerprint: Sha256
    first_sequence: int = Field(ge=1)
    high_watermark: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_watermark(self) -> Self:
        if self.high_watermark < self.first_sequence - 1:
            raise ValueError("high_watermark cannot precede the source start")
        return self


class SignalBusSourceDescriptor(RuntimeContractModel):
    source_id: str = Field(default="signal-bus/global-sequence/v1", min_length=1)
    generation_id: Sha256
    first_global_sequence: int = Field(default=1, ge=1)
    high_watermark: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.high_watermark < self.first_global_sequence - 1:
            raise ValueError("high_watermark cannot precede the source start")
        return self


def require_legacy_signal_write(
    signal: SignalEnvelopeFamily,
    *,
    operation: str,
) -> SignalEnvelope:
    if isinstance(signal, CurrentSignalEnvelope):
        raise LegacySignalWriteActivationError(
            f"{operation} is legacy-only in this reader-only release; "
            "current-family writes are not activated"
        )
    if not isinstance(signal, SignalEnvelope):
        raise TypeError(f"{operation} requires a SignalEnvelope object")
    return signal


class SignalBusSignalRecord(RuntimeContractModel):
    global_sequence: int = Field(ge=1)
    signal_id: Sha256
    payload_hash: Sha256
    payload_json: str = Field(min_length=1)
    signal: SignalEnvelopeFamily
    received_at: AwareUtcDatetime

    @field_validator("signal", mode="before")
    @classmethod
    def dispatch_signal_family(cls, value: object) -> SignalEnvelopeFamily:
        if isinstance(value, (SignalEnvelope, CurrentSignalEnvelope)):
            return value
        if isinstance(value, (Mapping, str, bytes, bytearray)):
            return parse_signal_envelope(value)
        raise TypeError("signal must be a stored signal envelope")

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.signal.signal_id != self.signal_id:
            raise ValueError("signal_id does not match signal payload")
        stored_signal = parse_signal_envelope(self.payload_json)
        if type(stored_signal) is not type(self.signal) or stored_signal != self.signal:
            raise ValueError("signal does not match payload_json")
        if self.payload_hash != self.canonical_payload_hash:
            raise ValueError("payload_hash does not match payload_json")
        return self

    @property
    def canonical_payload_hash(self) -> str:
        return _payload_hash(self.payload_json)


class SignalBusRoutedRecord(SignalBusSignalRecord):
    receipt: SignalRouteReceipt

    @model_validator(mode="after")
    def validate_route_identity(self) -> Self:
        if self.receipt.signal_id != self.signal_id:
            raise ValueError("route receipt signal_id does not match signal payload")
        return self


class SignalRouteCursor(RuntimeContractModel):
    source_id: str = Field(min_length=1)
    generation_id: Sha256 | None = None
    strategy_spec_fingerprint: Sha256 | None = None
    routing_policy_fingerprint: Sha256 | None = None
    first_sequence: int = Field(default=1, ge=1)
    observed_high_watermark: int = Field(default=0, ge=0)
    last_sequence: int = Field(ge=0)
    last_signal_id: Sha256 | None = None
    updated_at: AwareUtcDatetime | None = None


class SignalRouteReceipt(RuntimeContractModel):
    source_id: str = Field(min_length=1)
    source_sequence: int = Field(ge=1)
    signal_id: Sha256
    decision_fingerprint: Sha256
    disposition: RouteReceiptDisposition
    reason_code: str | None = Field(default=None, min_length=1)
    target_manifest_hash: Sha256
    targets: tuple[DeliveryTarget, ...]
    target_count: int = Field(ge=0)
    routed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_targets(self) -> Self:
        if self.target_count != len(self.targets):
            raise ValueError("target_count does not match targets")
        if self.disposition is RouteReceiptDisposition.NO_TARGET:
            if self.targets or self.reason_code is None:
                raise ValueError("no-target receipts require a reason and no targets")
        elif not self.targets or self.reason_code is not None:
            raise ValueError("routed receipts require targets and forbid a reason")
        return self


class SignalRouteCommitResult(RuntimeContractModel):
    receipt: SignalRouteReceipt
    duplicate: bool


class _RouteSourceBindingRequest(RuntimeContractModel):
    descriptor: RouteSourceDescriptor
    routing_policy_fingerprint: Sha256
    observed_at: AwareUtcDatetime


class _SourceRouteCommitRequest(RuntimeContractModel):
    descriptor: RouteSourceDescriptor
    routing_policy_fingerprint: Sha256
    source_sequence: int = Field(ge=1)
    signal: SignalEnvelope
    decision_kind: RouteDecisionKind
    decision_fingerprint: Sha256
    reason_code: str | None = Field(default=None, min_length=1)
    targets: tuple[DeliveryTarget, ...]
    routed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_decision(self) -> Self:
        if self.decision_kind is RouteDecisionKind.ROUTE:
            if not self.targets or self.reason_code is not None:
                raise ValueError("ROUTE requires targets and forbids reason_code")
        elif self.targets or self.reason_code is None:
            raise ValueError("NO_TARGET requires reason_code and forbids targets")
        expected = routing_decision_fingerprint(
            routing_policy_fingerprint=self.routing_policy_fingerprint,
            decision_kind=self.decision_kind,
            targets=self.targets,
            reason_code=self.reason_code,
        )
        if self.decision_fingerprint != expected:
            raise ValueError("decision_fingerprint does not match routing decision")
        return self


class QuarantinedSignal(RuntimeContractModel):
    signal_id: Sha256
    payload_hash: Sha256
    payload_json: str = Field(min_length=1)
    received_at: AwareUtcDatetime
    reason: str = Field(min_length=1)


class UnknownDeliveryEvidence(RuntimeContractModel):
    outbox_id: Sha256
    attempt_no: int = Field(ge=1)
    worker_id: str = Field(min_length=1)
    observed_at: AwareUtcDatetime
    reason: str = Field(min_length=1)
    provider_receipt: str | None = Field(default=None, min_length=1)


def _normalize_time(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _encode_time(value: datetime) -> str:
    return _normalize_time(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decode_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _signal_payload(signal: SignalEnvelope) -> str:
    return json.dumps(
        signal.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _payload_hash(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def canonical_delivery_targets(
    targets: Iterable[DeliveryTarget],
) -> tuple[DeliveryTarget, ...]:
    validated = tuple(DeliveryTarget.model_validate(target) for target in targets)
    unique = {(target.recipient_id, target.channel.value): target for target in validated}
    return tuple(unique[key] for key in sorted(unique))


def _target_manifest_payload(targets: tuple[DeliveryTarget, ...]) -> str:
    return json.dumps(
        [target.model_dump(mode="json") for target in targets],
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def routing_decision_fingerprint(
    *,
    routing_policy_fingerprint: str,
    decision_kind: RouteDecisionKind,
    targets: Iterable[DeliveryTarget],
    reason_code: str | None,
) -> str:
    canonical_targets = canonical_delivery_targets(targets)
    return canonical_sha256(
        {
            "contract": "routing-decision/v1",
            "routing_policy_fingerprint": routing_policy_fingerprint,
            "decision_kind": decision_kind,
            "reason_code": reason_code,
            "targets": canonical_targets,
        }
    )


def _retry_policy_fingerprint(
    retry_base_delay: timedelta,
    retry_max_delay: timedelta,
    max_attempts: int,
) -> str:
    payload = {
        "max_attempts": max_attempts,
        "retry_base_delay_microseconds": int(retry_base_delay / timedelta(microseconds=1)),
        "retry_max_delay_microseconds": int(retry_max_delay / timedelta(microseconds=1)),
        "schema_version": 1,
    }
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return _payload_hash(encoded)


class SignalBusStore:
    """Serialize signal, routing, and delivery state transitions through SQLite."""

    def __init__(
        self,
        path: Path | str,
        *,
        busy_timeout_ms: int = 5_000,
        retry_base_delay: timedelta = timedelta(seconds=5),
        retry_max_delay: timedelta = timedelta(minutes=5),
        max_attempts: int = 5,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        if retry_base_delay <= timedelta(0):
            raise ValueError("retry_base_delay must be positive")
        if retry_max_delay < retry_base_delay:
            raise ValueError("retry_max_delay must be at least retry_base_delay")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.max_attempts = max_attempts
        self.retry_policy_fingerprint = _retry_policy_fingerprint(
            retry_base_delay,
            retry_max_delay,
            max_attempts,
        )
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
        except BaseException:
            connection.close()
            raise
        return connection

    def _connect_readonly(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            f"file:{self.path}?mode=ro",
            uri=True,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA query_only = ON")
        except BaseException:
            connection.close()
            raise
        return connection

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        try:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS signal_envelope (
                    global_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT NOT NULL UNIQUE,
                    payload_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    received_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS signal_quarantine (
                    quarantine_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT NOT NULL REFERENCES signal_envelope(signal_id),
                    payload_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    UNIQUE(signal_id, payload_hash)
                );

                CREATE TABLE IF NOT EXISTS delivery_outbox (
                    outbox_id TEXT PRIMARY KEY,
                    signal_id TEXT NOT NULL REFERENCES signal_envelope(signal_id),
                    global_sequence INTEGER NOT NULL,
                    recipient_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    status TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
                    next_attempt_at TEXT,
                    lease_owner TEXT,
                    lease_started_at TEXT,
                    lease_until TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(signal_id, recipient_id, channel),
                    FOREIGN KEY(global_sequence)
                        REFERENCES signal_envelope(global_sequence)
                );

                CREATE TABLE IF NOT EXISTS delivery_attempt (
                    outbox_id TEXT NOT NULL REFERENCES delivery_outbox(outbox_id),
                    attempt_no INTEGER NOT NULL CHECK(attempt_no >= 1),
                    started_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    success INTEGER NOT NULL CHECK(success IN (0, 1)),
                    provider_receipt TEXT,
                    error TEXT,
                    PRIMARY KEY(outbox_id, attempt_no)
                );

                CREATE TABLE IF NOT EXISTS delivery_unknown (
                    outbox_id TEXT NOT NULL REFERENCES delivery_outbox(outbox_id),
                    attempt_no INTEGER NOT NULL CHECK(attempt_no >= 1),
                    worker_id TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    provider_receipt TEXT,
                    PRIMARY KEY(outbox_id, attempt_no)
                );

                CREATE INDEX IF NOT EXISTS idx_delivery_outbox_due
                ON delivery_outbox(status, next_attempt_at, global_sequence, created_at);

                CREATE INDEX IF NOT EXISTS idx_delivery_outbox_signal
                ON delivery_outbox(signal_id, recipient_id, channel);

                CREATE TABLE IF NOT EXISTS signal_bus_metadata (
                    metadata_key TEXT PRIMARY KEY,
                    metadata_value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS signal_route_source (
                    source_id TEXT PRIMARY KEY,
                    generation_id TEXT NOT NULL,
                    strategy_spec_fingerprint TEXT NOT NULL,
                    routing_policy_fingerprint TEXT NOT NULL,
                    first_sequence INTEGER NOT NULL CHECK(first_sequence >= 1),
                    observed_high_watermark INTEGER NOT NULL CHECK(observed_high_watermark >= 0),
                    last_sequence INTEGER NOT NULL CHECK(last_sequence >= 0),
                    last_signal_id TEXT,
                    registered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS signal_route_receipt (
                    source_id TEXT NOT NULL REFERENCES signal_route_source(source_id),
                    source_sequence INTEGER NOT NULL CHECK(source_sequence >= 1),
                    signal_id TEXT NOT NULL UNIQUE REFERENCES signal_envelope(signal_id),
                    decision_fingerprint TEXT NOT NULL,
                    disposition TEXT NOT NULL,
                    reason_code TEXT,
                    target_manifest_hash TEXT NOT NULL,
                    target_manifest_json TEXT NOT NULL,
                    routed_at TEXT NOT NULL,
                    PRIMARY KEY(source_id, source_sequence)
                );

                CREATE INDEX IF NOT EXISTS idx_signal_route_receipt_source
                ON signal_route_receipt(source_id, source_sequence);
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO signal_bus_metadata(metadata_key, metadata_value)
                VALUES ('retry_policy_fingerprint', ?)
                """,
                (self.retry_policy_fingerprint,),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO signal_bus_metadata(metadata_key, metadata_value)
                VALUES ('source_generation_id', ?)
                """,
                (secrets.token_hex(32),),
            )
            observed_max = connection.execute(
                "SELECT COALESCE(MAX(global_sequence), 0) AS value FROM signal_envelope"
            ).fetchone()
            connection.execute(
                """
                INSERT OR IGNORE INTO signal_bus_metadata(metadata_key, metadata_value)
                VALUES ('signal_high_watermark', ?)
                """,
                (str(int(observed_max["value"])),),
            )
            observed = connection.execute(
                """
                SELECT metadata_value
                FROM signal_bus_metadata
                WHERE metadata_key = 'retry_policy_fingerprint'
                """
            ).fetchone()
            if observed is None or observed["metadata_value"] != self.retry_policy_fingerprint:
                raise ValueError("retry policy does not match the persisted signal bus policy")
        finally:
            connection.close()

    def _before_commit(self, _connection: sqlite3.Connection) -> None:
        """Fault-injection boundary for proving whole-transition rollback."""

    def ingest(
        self,
        signal: SignalEnvelope,
        *,
        received_at: datetime | None = None,
    ) -> RouterReceipt:
        require_legacy_signal_write(signal, operation="SignalBusStore.ingest")
        received = _normalize_time(received_at or datetime.now(UTC))
        with self._write_transaction() as connection:
            receipt, changed = self._ingest_in_transaction(
                connection,
                signal,
                received_at=received,
            )
            if changed:
                self._before_commit(connection)
            return receipt

    def _ingest_in_transaction(
        self,
        connection: sqlite3.Connection,
        signal: SignalEnvelope,
        *,
        received_at: datetime,
    ) -> tuple[RouterReceipt, bool]:
        require_legacy_signal_write(signal, operation="SignalBusStore.ingest")
        signal_id = signal.signal_id
        if signal_id is None:
            raise ValueError("signal_id must be materialized before ingest")
        payload = _signal_payload(signal)
        content_hash = _payload_hash(payload)

        existing = connection.execute(
            """
            SELECT global_sequence, payload_hash, payload_json
            FROM signal_envelope
            WHERE signal_id = ?
            """,
            (signal_id,),
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] == content_hash and existing["payload_json"] == payload:
                return (
                    RouterReceipt(
                        signal_id=signal_id,
                        disposition=RouterDisposition.DUPLICATE,
                        global_sequence=existing["global_sequence"],
                        received_at=received_at,
                    ),
                    False,
                )
            reason = "signal_id already exists with different canonical payload"
            before_changes = connection.total_changes
            connection.execute(
                """
                INSERT OR IGNORE INTO signal_quarantine(
                    signal_id, payload_hash, payload_json, received_at, reason
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (signal_id, content_hash, payload, _encode_time(received_at), reason),
            )
            return (
                RouterReceipt(
                    signal_id=signal_id,
                    disposition=RouterDisposition.QUARANTINED,
                    reason=reason,
                    received_at=received_at,
                ),
                connection.total_changes > before_changes,
            )

        cursor = connection.execute(
            """
            INSERT INTO signal_envelope(
                signal_id, payload_hash, payload_json, received_at
            ) VALUES (?, ?, ?, ?)
            """,
            (signal_id, content_hash, payload, _encode_time(received_at)),
        )
        sequence = int(cursor.lastrowid)
        connection.execute(
            """
            UPDATE signal_bus_metadata
            SET metadata_value = ?
            WHERE metadata_key = 'signal_high_watermark'
              AND CAST(metadata_value AS INTEGER) < ?
            """,
            (str(sequence), sequence),
        )
        return (
            RouterReceipt(
                signal_id=signal_id,
                disposition=RouterDisposition.ACCEPTED,
                global_sequence=sequence,
                received_at=received_at,
            ),
            True,
        )

    def source_descriptor(self) -> SignalBusSourceDescriptor:
        connection = self._connect_readonly()
        try:
            rows = connection.execute(
                """
                SELECT metadata_key, metadata_value
                FROM signal_bus_metadata
                WHERE metadata_key IN ('source_generation_id', 'signal_high_watermark')
                """
            ).fetchall()
        finally:
            connection.close()
        metadata = {str(row["metadata_key"]): str(row["metadata_value"]) for row in rows}
        generation_id = metadata.get("source_generation_id")
        high_watermark = metadata.get("signal_high_watermark")
        if generation_id is None or high_watermark is None:
            raise RuntimeError("signal bus source metadata is incomplete")
        return SignalBusSourceDescriptor(
            generation_id=generation_id,
            high_watermark=int(high_watermark),
        )

    def signals_after_global_sequence(
        self,
        *,
        after_sequence: int,
        through_sequence: int,
        observed_at: datetime,
        limit: int,
    ) -> tuple[SignalBusSignalRecord, ...]:
        if (
            not isinstance(after_sequence, int)
            or isinstance(after_sequence, bool)
            or after_sequence < 0
        ):
            raise ValueError("after_sequence must be a non-negative integer")
        if (
            not isinstance(through_sequence, int)
            or isinstance(through_sequence, bool)
            or through_sequence < after_sequence
        ):
            raise ValueError("through_sequence must be an integer at least after_sequence")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")
        visible_at = _normalize_time(observed_at)

        connection = self._connect_readonly()
        try:
            connection.execute("BEGIN")
            watermark_row = connection.execute(
                """
                SELECT metadata_value
                FROM signal_bus_metadata
                WHERE metadata_key = 'signal_high_watermark'
                """
            ).fetchone()
            if watermark_row is None:
                raise RuntimeError("signal bus high watermark is missing")
            current_high_watermark = int(watermark_row["metadata_value"])
            if through_sequence > current_high_watermark:
                raise SignalBusSourceSequenceError(
                    "requested high watermark exceeds the signal bus high watermark"
                )
            rows = connection.execute(
                """
                SELECT global_sequence, signal_id, payload_hash, payload_json, received_at
                FROM signal_envelope
                WHERE global_sequence > ? AND global_sequence <= ?
                ORDER BY global_sequence
                LIMIT ?
                """,
                (after_sequence, through_sequence, limit),
            ).fetchall()
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

        if through_sequence > after_sequence and not rows:
            raise SignalBusSourceSequenceError(
                "signal sequence is truncated before the requested high watermark"
            )
        records: list[SignalBusSignalRecord] = []
        expected_sequence = after_sequence + 1
        for row in rows:
            sequence = int(row["global_sequence"])
            if sequence != expected_sequence:
                raise SignalBusSourceSequenceError(
                    f"signal sequence gap: expected {expected_sequence}, observed {sequence}"
                )
            expected_sequence += 1
            signal = parse_signal_envelope(row["payload_json"])
            received_at = _require_time(row["received_at"])
            if signal.available_at > visible_at or received_at > visible_at:
                break
            records.append(
                SignalBusSignalRecord(
                    global_sequence=sequence,
                    signal_id=row["signal_id"],
                    payload_hash=row["payload_hash"],
                    payload_json=row["payload_json"],
                    signal=signal,
                    received_at=received_at,
                )
            )
        return tuple(records)

    def routed_signals_after_global_sequence(
        self,
        *,
        after_sequence: int,
        through_sequence: int,
        limit: int,
    ) -> tuple[SignalBusRoutedRecord, ...]:
        if (
            not isinstance(after_sequence, int)
            or isinstance(after_sequence, bool)
            or after_sequence < 0
        ):
            raise ValueError("after_sequence must be a non-negative integer")
        if (
            not isinstance(through_sequence, int)
            or isinstance(through_sequence, bool)
            or through_sequence < after_sequence
        ):
            raise ValueError("through_sequence must be an integer at least after_sequence")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")

        connection = self._connect_readonly()
        try:
            connection.execute("BEGIN")
            watermark_row = connection.execute(
                """
                SELECT metadata_value
                FROM signal_bus_metadata
                WHERE metadata_key = 'signal_high_watermark'
                """
            ).fetchone()
            if watermark_row is None:
                raise RuntimeError("signal bus high watermark is missing")
            current_high_watermark = int(watermark_row["metadata_value"])
            if through_sequence > current_high_watermark:
                raise SignalBusSourceSequenceError(
                    "requested high watermark exceeds the signal bus high watermark"
                )
            rows = connection.execute(
                """
                SELECT signal.global_sequence,
                       signal.signal_id,
                       signal.payload_hash,
                       signal.payload_json,
                       signal.received_at,
                       receipt.source_id,
                       receipt.source_sequence,
                       receipt.decision_fingerprint,
                       receipt.disposition,
                       receipt.reason_code,
                       receipt.target_manifest_hash,
                       receipt.target_manifest_json,
                       receipt.routed_at
                FROM signal_envelope AS signal
                JOIN signal_route_receipt AS receipt
                  ON receipt.signal_id = signal.signal_id
                WHERE signal.global_sequence > ? AND signal.global_sequence <= ?
                ORDER BY signal.global_sequence
                LIMIT ?
                """,
                (after_sequence, through_sequence, limit),
            ).fetchall()
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

        if through_sequence > after_sequence and not rows:
            raise SignalBusSourceSequenceError(
                "routed signal sequence is missing below the requested high watermark"
            )
        records: list[SignalBusRoutedRecord] = []
        expected_sequence = after_sequence + 1
        for row in rows:
            sequence = int(row["global_sequence"])
            if sequence != expected_sequence:
                raise SignalBusSourceSequenceError(
                    f"routed signal sequence gap: expected {expected_sequence}, observed {sequence}"
                )
            expected_sequence += 1
            signal = parse_signal_envelope(row["payload_json"])
            records.append(
                SignalBusRoutedRecord(
                    global_sequence=sequence,
                    signal_id=row["signal_id"],
                    payload_hash=row["payload_hash"],
                    payload_json=row["payload_json"],
                    signal=signal,
                    received_at=_require_time(row["received_at"]),
                    receipt=self._route_receipt_from_row(row),
                )
            )
        return tuple(records)

    def signal(self, identifier: int | str) -> SignalEnvelopeFamily | None:
        payload = self.signal_payload(identifier)
        if payload is None:
            return None
        return parse_signal_envelope(payload)

    def signal_payload(self, identifier: int | str) -> str | None:
        column = "global_sequence" if isinstance(identifier, int) else "signal_id"
        connection = self._connect()
        try:
            row = connection.execute(
                f"SELECT payload_json FROM signal_envelope WHERE {column} = ?",
                (identifier,),
            ).fetchone()
            return None if row is None else str(row["payload_json"])
        finally:
            connection.close()

    def quarantines(self, signal_id: str | None = None) -> tuple[QuarantinedSignal, ...]:
        connection = self._connect()
        try:
            if signal_id is None:
                rows = connection.execute(
                    """
                    SELECT signal_id, payload_hash, payload_json, received_at, reason
                    FROM signal_quarantine
                    ORDER BY quarantine_sequence
                    """
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT signal_id, payload_hash, payload_json, received_at, reason
                    FROM signal_quarantine
                    WHERE signal_id = ?
                    ORDER BY quarantine_sequence
                    """,
                    (signal_id,),
                ).fetchall()
            return tuple(
                QuarantinedSignal(
                    signal_id=row["signal_id"],
                    payload_hash=row["payload_hash"],
                    payload_json=row["payload_json"],
                    received_at=_require_time(row["received_at"]),
                    reason=row["reason"],
                )
                for row in rows
            )
        finally:
            connection.close()

    def route(
        self,
        signal_id: str,
        targets: Iterable[DeliveryTarget],
        *,
        now: datetime,
    ) -> tuple[OutboxRecord, ...]:
        routed_at = _normalize_time(now)
        unique_targets = canonical_delivery_targets(targets)
        if not unique_targets:
            return ()

        with self._write_transaction() as connection:
            frozen = connection.execute(
                """
                SELECT target_manifest_json FROM signal_route_receipt
                WHERE signal_id = ?
                """,
                (signal_id,),
            ).fetchone()
            if frozen is not None:
                frozen_targets = canonical_delivery_targets(
                    DeliveryTarget.model_validate(item)
                    for item in json.loads(frozen["target_manifest_json"])
                )
                if frozen_targets != unique_targets:
                    raise SignalRouteConflictError(
                        "targets conflict with the frozen target manifest"
                    )
            rows, changed = self._route_in_transaction(
                connection,
                signal_id=signal_id,
                targets=unique_targets,
                routed_at=routed_at,
            )
            if changed:
                self._before_commit(connection)
            return tuple(self._outbox_from_row(row) for row in rows)

    def _route_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        signal_id: str,
        targets: tuple[DeliveryTarget, ...],
        routed_at: datetime,
    ) -> tuple[list[sqlite3.Row], bool]:
        signal_row = connection.execute(
            """
            SELECT global_sequence, payload_json
            FROM signal_envelope
            WHERE signal_id = ?
            """,
            (signal_id,),
        ).fetchone()
        if signal_row is None:
            raise KeyError(f"signal {signal_id!r} does not exist")
        signal = parse_signal_envelope(signal_row["payload_json"])
        require_legacy_signal_write(signal, operation="SignalBusStore.route")
        if routed_at < signal.available_at:
            raise ValueError("signal cannot be routed before available_at")
        expired = routed_at >= signal.expires_at
        changed = False
        for target in targets:
            outbox_id = target.delivery_key(signal_id)
            existing = connection.execute(
                "SELECT status FROM delivery_outbox WHERE outbox_id = ?",
                (outbox_id,),
            ).fetchone()
            if existing is not None:
                if expired and existing["status"] in {
                    OutboxStatus.PENDING.value,
                    OutboxStatus.RETRY.value,
                }:
                    connection.execute(
                        """
                        UPDATE delivery_outbox
                        SET status = ?, next_attempt_at = NULL,
                            last_error = ?, updated_at = ?
                        WHERE outbox_id = ?
                        """,
                        (
                            OutboxStatus.EXPIRED.value,
                            "signal expired before routing",
                            _encode_time(routed_at),
                            outbox_id,
                        ),
                    )
                    changed = True
                continue

            created_at = signal.available_at if expired else routed_at
            connection.execute(
                """
                INSERT INTO delivery_outbox(
                    outbox_id, signal_id, global_sequence, recipient_id, channel,
                    status, expires_at, attempt_count, next_attempt_at,
                    lease_owner, lease_started_at, lease_until, last_error,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, ?, ?, ?)
                """,
                (
                    outbox_id,
                    signal_id,
                    signal_row["global_sequence"],
                    target.recipient_id,
                    target.channel.value,
                    (OutboxStatus.EXPIRED.value if expired else OutboxStatus.PENDING.value),
                    _encode_time(signal.expires_at),
                    "signal expired before routing" if expired else None,
                    _encode_time(created_at),
                    _encode_time(routed_at),
                ),
            )
            changed = True
        rows = self._select_outbox_rows(
            connection,
            signal_id=signal_id,
            target_keys={(target.recipient_id, target.channel.value) for target in targets},
        )
        return rows, changed

    def bind_route_source(
        self,
        descriptor: RouteSourceDescriptor,
        *,
        routing_policy_fingerprint: str,
        observed_at: datetime,
    ) -> SignalRouteCursor:
        request = _RouteSourceBindingRequest(
            descriptor=descriptor,
            routing_policy_fingerprint=routing_policy_fingerprint,
            observed_at=observed_at,
        )
        with self._write_transaction() as connection:
            row = self._bind_route_source_in_transaction(connection, request)
            return self._route_cursor_from_row(row)

    def _bind_route_source_in_transaction(
        self,
        connection: sqlite3.Connection,
        request: _RouteSourceBindingRequest,
    ) -> sqlite3.Row:
        descriptor = request.descriptor
        row = connection.execute(
            "SELECT * FROM signal_route_source WHERE source_id = ?",
            (descriptor.source_id,),
        ).fetchone()
        now_text = _encode_time(request.observed_at)
        if row is None:
            connection.execute(
                """
                INSERT INTO signal_route_source(
                    source_id, generation_id, strategy_spec_fingerprint,
                    routing_policy_fingerprint, first_sequence,
                    observed_high_watermark, last_sequence, last_signal_id,
                    registered_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    descriptor.source_id,
                    descriptor.generation_id,
                    descriptor.strategy_spec_fingerprint,
                    request.routing_policy_fingerprint,
                    descriptor.first_sequence,
                    descriptor.high_watermark,
                    descriptor.first_sequence - 1,
                    now_text,
                    now_text,
                ),
            )
        else:
            immutable_fields = (
                ("generation_id", descriptor.generation_id, "generation"),
                (
                    "strategy_spec_fingerprint",
                    descriptor.strategy_spec_fingerprint,
                    "strategy spec",
                ),
                (
                    "routing_policy_fingerprint",
                    request.routing_policy_fingerprint,
                    "routing policy",
                ),
                ("first_sequence", descriptor.first_sequence, "first sequence"),
            )
            for column, expected, label in immutable_fields:
                if row[column] != expected:
                    raise SignalRouteConflictError(
                        f"source {descriptor.source_id!r} {label} changed"
                    )
            observed_high = int(row["observed_high_watermark"])
            last_sequence = int(row["last_sequence"])
            if descriptor.high_watermark < observed_high:
                raise SignalRouteSequenceError(
                    f"source high watermark regressed from {observed_high} "
                    f"to {descriptor.high_watermark}"
                )
            if descriptor.high_watermark < last_sequence:
                raise SignalRouteSequenceError(
                    "source high watermark is behind the committed cursor"
                )
            connection.execute(
                """
                UPDATE signal_route_source
                SET observed_high_watermark = ?, updated_at = ?
                WHERE source_id = ?
                """,
                (descriptor.high_watermark, now_text, descriptor.source_id),
            )
        bound = connection.execute(
            "SELECT * FROM signal_route_source WHERE source_id = ?",
            (descriptor.source_id,),
        ).fetchone()
        assert bound is not None
        return bound

    def route_cursor(self, source_id: str) -> SignalRouteCursor:
        normalized = source_id.strip()
        if not normalized:
            raise ValueError("source_id must not be empty")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM signal_route_source WHERE source_id = ?",
                (normalized,),
            ).fetchone()
        if row is None:
            return SignalRouteCursor(source_id=normalized, last_sequence=0)
        return self._route_cursor_from_row(row)

    def route_receipts(
        self,
        source_id: str | None = None,
    ) -> tuple[SignalRouteReceipt, ...]:
        with self._connect() as connection:
            if source_id is None:
                rows = connection.execute(
                    """
                    SELECT * FROM signal_route_receipt
                    ORDER BY source_id, source_sequence
                    """
                ).fetchall()
            else:
                normalized = source_id.strip()
                if not normalized:
                    raise ValueError("source_id must not be empty")
                rows = connection.execute(
                    """
                    SELECT * FROM signal_route_receipt
                    WHERE source_id = ? ORDER BY source_sequence
                    """,
                    (normalized,),
                ).fetchall()
        return tuple(self._route_receipt_from_row(row) for row in rows)

    def commit_source_route(
        self,
        *,
        descriptor: RouteSourceDescriptor,
        routing_policy_fingerprint: str,
        source_sequence: int,
        signal: SignalEnvelope,
        decision_kind: RouteDecisionKind,
        decision_fingerprint: str,
        reason_code: str | None,
        targets: Iterable[DeliveryTarget],
        routed_at: datetime,
    ) -> SignalRouteCommitResult:
        require_legacy_signal_write(signal, operation="SignalBusStore.commit_source_route")
        request = _SourceRouteCommitRequest(
            descriptor=descriptor,
            routing_policy_fingerprint=routing_policy_fingerprint,
            source_sequence=source_sequence,
            signal=signal,
            decision_kind=decision_kind,
            decision_fingerprint=decision_fingerprint,
            reason_code=reason_code,
            targets=canonical_delivery_targets(targets),
            routed_at=routed_at,
        )
        manifest_json = _target_manifest_payload(request.targets)
        manifest_hash = _payload_hash(manifest_json)
        signal_id = request.signal.signal_id
        if signal_id is None:
            raise ValueError("signal_id must be materialized before routing")

        with self._write_transaction() as connection:
            source_row = self._bind_route_source_in_transaction(
                connection,
                _RouteSourceBindingRequest(
                    descriptor=request.descriptor,
                    routing_policy_fingerprint=request.routing_policy_fingerprint,
                    observed_at=request.routed_at,
                ),
            )
            existing = connection.execute(
                """
                SELECT * FROM signal_route_receipt
                WHERE source_id = ? AND source_sequence = ?
                """,
                (request.descriptor.source_id, request.source_sequence),
            ).fetchone()
            if existing is not None:
                stored = self._route_receipt_from_row(existing)
                if (
                    stored.signal_id != signal_id
                    or stored.decision_fingerprint != request.decision_fingerprint
                    or stored.target_manifest_hash != manifest_hash
                    or stored.reason_code != request.reason_code
                ):
                    raise SignalRouteConflictError(
                        "source sequence was already routed with a different decision"
                    )
                ingest_receipt, _changed = self._ingest_in_transaction(
                    connection,
                    request.signal,
                    received_at=request.routed_at,
                )
                if ingest_receipt.disposition is RouterDisposition.QUARANTINED:
                    raise SignalRouteConflictError(
                        "source sequence signal payload conflicts with the stored signal"
                    )
                return SignalRouteCommitResult(receipt=stored, duplicate=True)

            other_source = connection.execute(
                "SELECT source_id, source_sequence FROM signal_route_receipt WHERE signal_id = ?",
                (signal_id,),
            ).fetchone()
            if other_source is not None:
                raise SignalRouteConflictError(
                    "signal identity is already owned by another source receipt"
                )
            current_sequence = int(source_row["last_sequence"])
            expected_sequence = current_sequence + 1
            if request.source_sequence != expected_sequence:
                raise SignalRouteSequenceError(
                    f"expected runner sequence {expected_sequence}, got {request.source_sequence}"
                )
            if request.source_sequence > request.descriptor.high_watermark:
                raise SignalRouteSequenceError(
                    "source sequence exceeds the declared high watermark"
                )
            if request.signal.available_at > request.routed_at:
                raise ValueError("future signal cannot be committed to the route ledger")

            existing_targets = {
                (row["recipient_id"], row["channel"])
                for row in connection.execute(
                    """
                    SELECT recipient_id, channel FROM delivery_outbox
                    WHERE signal_id = ?
                    """,
                    (signal_id,),
                ).fetchall()
            }
            desired_targets = {
                (target.recipient_id, target.channel.value) for target in request.targets
            }
            if existing_targets and existing_targets != desired_targets:
                raise SignalRouteConflictError(
                    "existing outbox targets conflict with the frozen target manifest"
                )
            if request.decision_kind is RouteDecisionKind.NO_TARGET and existing_targets:
                raise SignalRouteConflictError(
                    "no-target decision conflicts with existing outbox targets"
                )

            ingest_receipt, _changed = self._ingest_in_transaction(
                connection,
                request.signal,
                received_at=request.routed_at,
            )
            if ingest_receipt.disposition is RouterDisposition.QUARANTINED:
                raise SignalRouteConflictError("signal payload was quarantined")

            if request.decision_kind is RouteDecisionKind.NO_TARGET:
                disposition = RouteReceiptDisposition.NO_TARGET
            else:
                self._route_in_transaction(
                    connection,
                    signal_id=signal_id,
                    targets=request.targets,
                    routed_at=request.routed_at,
                )
                disposition = (
                    RouteReceiptDisposition.EXPIRED
                    if request.routed_at >= request.signal.expires_at
                    else RouteReceiptDisposition.ROUTED
                )
            connection.execute(
                """
                INSERT INTO signal_route_receipt(
                    source_id, source_sequence, signal_id, decision_fingerprint,
                    disposition, reason_code, target_manifest_hash,
                    target_manifest_json, routed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.descriptor.source_id,
                    request.source_sequence,
                    signal_id,
                    request.decision_fingerprint,
                    disposition.value,
                    request.reason_code,
                    manifest_hash,
                    manifest_json,
                    _encode_time(request.routed_at),
                ),
            )
            connection.execute(
                """
                UPDATE signal_route_source
                SET last_sequence = ?, last_signal_id = ?, updated_at = ?
                WHERE source_id = ?
                """,
                (
                    request.source_sequence,
                    signal_id,
                    _encode_time(request.routed_at),
                    request.descriptor.source_id,
                ),
            )
            self._before_commit(connection)
            receipt_row = connection.execute(
                """
                SELECT * FROM signal_route_receipt
                WHERE source_id = ? AND source_sequence = ?
                """,
                (request.descriptor.source_id, request.source_sequence),
            ).fetchone()
            assert receipt_row is not None
            return SignalRouteCommitResult(
                receipt=self._route_receipt_from_row(receipt_row),
                duplicate=False,
            )

    def claim_due(
        self,
        worker_id: str,
        *,
        now: datetime,
        lease_for: timedelta,
        limit: int,
    ) -> tuple[OutboxRecord, ...]:
        worker = worker_id.strip()
        claimed_at = _normalize_time(now)
        if not worker:
            raise ValueError("worker_id must not be empty")
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        if limit < 1:
            raise ValueError("limit must be positive")
        now_text = _encode_time(claimed_at)

        with self._write_transaction() as connection:
            connection.execute(
                """
                UPDATE delivery_outbox
                SET status = ?, next_attempt_at = NULL, last_error = ?, updated_at = ?
                WHERE status IN (?, ?) AND expires_at <= ?
                """,
                (
                    OutboxStatus.EXPIRED.value,
                    "signal expired before delivery claim",
                    now_text,
                    OutboxStatus.PENDING.value,
                    OutboxStatus.RETRY.value,
                    now_text,
                ),
            )
            candidates = connection.execute(
                """
                SELECT *
                FROM delivery_outbox
                WHERE status IN (?, ?)
                  AND expires_at > ?
                  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                ORDER BY COALESCE(next_attempt_at, created_at),
                         global_sequence, created_at, outbox_id
                LIMIT ?
                """,
                (
                    OutboxStatus.PENDING.value,
                    OutboxStatus.RETRY.value,
                    now_text,
                    now_text,
                    limit,
                ),
            ).fetchall()
            claimed_ids: list[str] = []
            for row in candidates:
                expires_at = _require_time(row["expires_at"])
                lease_until = min(claimed_at + lease_for, expires_at)
                connection.execute(
                    """
                    UPDATE delivery_outbox
                    SET status = ?, attempt_count = attempt_count + 1,
                        next_attempt_at = NULL, lease_owner = ?,
                        lease_started_at = ?, lease_until = ?, updated_at = ?
                    WHERE outbox_id = ?
                    """,
                    (
                        OutboxStatus.LEASED.value,
                        worker,
                        now_text,
                        _encode_time(lease_until),
                        now_text,
                        row["outbox_id"],
                    ),
                )
                claimed_ids.append(row["outbox_id"])
            if candidates or connection.total_changes:
                self._before_commit(connection)
            rows = self._rows_for_outbox_ids(connection, claimed_ids)
            return tuple(self._outbox_from_row(row) for row in rows)

    def complete_success(
        self,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        completed_at: datetime,
        provider_receipt: str,
    ) -> OutboxRecord:
        if not provider_receipt.strip():
            raise ValueError("provider_receipt must not be empty")
        return self._complete(
            outbox_id,
            worker_id=worker_id,
            attempt_no=attempt_no,
            completed_at=completed_at,
            success=True,
            provider_receipt=provider_receipt,
            error=None,
        )

    def complete_failure(
        self,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        completed_at: datetime,
        error: str,
    ) -> OutboxRecord:
        if not error.strip():
            raise ValueError("error must not be empty")
        return self._complete(
            outbox_id,
            worker_id=worker_id,
            attempt_no=attempt_no,
            completed_at=completed_at,
            success=False,
            provider_receipt=None,
            error=error,
        )

    def _complete(
        self,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        completed_at: datetime,
        success: bool,
        provider_receipt: str | None,
        error: str | None,
    ) -> OutboxRecord:
        completed = _normalize_time(completed_at)
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM delivery_outbox WHERE outbox_id = ?",
                (outbox_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"outbox {outbox_id!r} does not exist")
            self._verify_lease(
                row,
                worker_id=worker_id,
                attempt_no=attempt_no,
                completed_at=completed,
            )
            started_at = _require_time(row["lease_started_at"])
            connection.execute(
                """
                INSERT INTO delivery_attempt(
                    outbox_id, attempt_no, started_at, completed_at,
                    success, provider_receipt, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    outbox_id,
                    attempt_no,
                    _encode_time(started_at),
                    _encode_time(completed),
                    int(success),
                    provider_receipt,
                    error,
                ),
            )
            if success:
                status = OutboxStatus.SUCCEEDED
                next_attempt_at = None
                last_error = None
            else:
                assert error is not None
                expires_at = _require_time(row["expires_at"])
                retry_at = completed + self._retry_delay(attempt_no)
                if completed >= expires_at or retry_at >= expires_at:
                    status = OutboxStatus.EXPIRED
                    next_attempt_at = None
                    last_error = f"delivery window expired after failure: {error}"
                elif attempt_no >= self.max_attempts:
                    status = OutboxStatus.DEAD_LETTER
                    next_attempt_at = None
                    last_error = error
                else:
                    status = OutboxStatus.RETRY
                    next_attempt_at = retry_at
                    last_error = error
            connection.execute(
                """
                UPDATE delivery_outbox
                SET status = ?, next_attempt_at = ?, lease_owner = NULL,
                    lease_started_at = NULL, lease_until = NULL,
                    last_error = ?, updated_at = ?
                WHERE outbox_id = ?
                """,
                (
                    status.value,
                    (_encode_time(next_attempt_at) if next_attempt_at is not None else None),
                    last_error,
                    _encode_time(completed),
                    outbox_id,
                ),
            )
            self._before_commit(connection)
            updated = connection.execute(
                "SELECT * FROM delivery_outbox WHERE outbox_id = ?",
                (outbox_id,),
            ).fetchone()
            assert updated is not None
            return self._outbox_from_row(updated)

    def _retry_delay(self, attempt_no: int) -> timedelta:
        multiplier = 1 << max(attempt_no - 1, 0)
        delay = self.retry_base_delay * multiplier
        return min(delay, self.retry_max_delay)

    def _verify_lease(
        self,
        row: sqlite3.Row,
        *,
        worker_id: str,
        attempt_no: int,
        completed_at: datetime,
    ) -> None:
        if row["status"] != OutboxStatus.LEASED.value:
            raise SignalBusLeaseError("outbox does not have an active lease")
        if row["lease_owner"] != worker_id:
            raise SignalBusLeaseError("lease owner does not match worker")
        if row["attempt_count"] != attempt_no:
            raise SignalBusLeaseError("attempt number does not match active lease")
        started_at = _require_time(row["lease_started_at"])
        lease_until = _require_time(row["lease_until"])
        if completed_at < started_at:
            raise SignalBusLeaseError("completion precedes lease start")
        if completed_at >= lease_until:
            raise SignalBusLeaseError("delivery lease has expired")

    def release_unattempted(
        self,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        released_at: datetime,
        reason: str,
    ) -> OutboxRecord:
        """Release a lease only when the provider was provably never called."""

        reason = reason.strip()
        if not reason:
            raise ValueError("reason must not be empty")
        released = _normalize_time(released_at)
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM delivery_outbox WHERE outbox_id = ?",
                (outbox_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"outbox {outbox_id!r} does not exist")
            if row["status"] != OutboxStatus.LEASED.value:
                raise SignalBusLeaseError("outbox does not have an active lease")
            if row["lease_owner"] != worker_id:
                raise SignalBusLeaseError("lease owner does not match worker")
            if row["attempt_count"] != attempt_no:
                raise SignalBusLeaseError("attempt number does not match active lease")
            started_at = _require_time(row["lease_started_at"])
            if released < started_at:
                raise SignalBusLeaseError("release precedes lease start")
            expires_at = _require_time(row["expires_at"])
            expired = released >= expires_at
            status = OutboxStatus.EXPIRED if expired else OutboxStatus.RETRY
            connection.execute(
                """
                UPDATE delivery_outbox
                SET status = ?, attempt_count = attempt_count - 1,
                    next_attempt_at = ?, lease_owner = NULL,
                    lease_started_at = NULL, lease_until = NULL,
                    last_error = ?, updated_at = ?
                WHERE outbox_id = ?
                """,
                (
                    status.value,
                    None if expired else _encode_time(released),
                    f"not attempted: {reason}",
                    _encode_time(released),
                    outbox_id,
                ),
            )
            self._before_commit(connection)
            updated = connection.execute(
                "SELECT * FROM delivery_outbox WHERE outbox_id = ?",
                (outbox_id,),
            ).fetchone()
            assert updated is not None
            return self._outbox_from_row(updated)

    def record_unknown_delivery(
        self,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        observed_at: datetime,
        reason: str,
        provider_receipt: str | None,
    ) -> UnknownDeliveryEvidence:
        """Persist evidence when a provider outcome or its write-back is uncertain."""

        reason = reason.strip()
        receipt = provider_receipt.strip() if provider_receipt is not None else None
        if not reason:
            raise ValueError("reason must not be empty")
        if provider_receipt is not None and not receipt:
            raise ValueError("provider_receipt must not be empty when present")
        observed = _normalize_time(observed_at)
        evidence = UnknownDeliveryEvidence(
            outbox_id=outbox_id,
            attempt_no=attempt_no,
            worker_id=worker_id,
            observed_at=observed,
            reason=reason,
            provider_receipt=receipt,
        )
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM delivery_outbox WHERE outbox_id = ?",
                (outbox_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"outbox {outbox_id!r} does not exist")
            if row["status"] != OutboxStatus.LEASED.value:
                raise SignalBusLeaseError("outbox does not have an active lease")
            if row["lease_owner"] != worker_id:
                raise SignalBusLeaseError("lease owner does not match worker")
            if row["attempt_count"] != attempt_no:
                raise SignalBusLeaseError("attempt number does not match active lease")
            started_at = _require_time(row["lease_started_at"])
            if observed < started_at:
                raise SignalBusLeaseError("unknown outcome precedes lease start")
            existing = connection.execute(
                """
                SELECT * FROM delivery_unknown
                WHERE outbox_id = ? AND attempt_no = ?
                """,
                (outbox_id, attempt_no),
            ).fetchone()
            if existing is not None:
                restored = self._unknown_from_row(existing)
                if restored != evidence:
                    raise SignalBusLeaseError("unknown delivery evidence is immutable")
                return restored
            connection.execute(
                """
                INSERT INTO delivery_unknown(
                    outbox_id, attempt_no, worker_id, observed_at,
                    reason, provider_receipt
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    outbox_id,
                    attempt_no,
                    worker_id,
                    _encode_time(observed),
                    reason,
                    receipt,
                ),
            )
            connection.execute(
                """
                UPDATE delivery_outbox
                SET last_error = ?, updated_at = ?
                WHERE outbox_id = ?
                """,
                (
                    f"delivery outcome unknown: {reason}",
                    _encode_time(observed),
                    outbox_id,
                ),
            )
            self._before_commit(connection)
            return evidence

    def unknown_deliveries(
        self,
        outbox_id: str | None = None,
    ) -> tuple[UnknownDeliveryEvidence, ...]:
        connection = self._connect()
        try:
            if outbox_id is None:
                rows = connection.execute(
                    """
                    SELECT * FROM delivery_unknown
                    ORDER BY observed_at, outbox_id, attempt_no
                    """
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT * FROM delivery_unknown
                    WHERE outbox_id = ? ORDER BY attempt_no
                    """,
                    (outbox_id,),
                ).fetchall()
            return tuple(self._unknown_from_row(row) for row in rows)
        finally:
            connection.close()

    def recover_expired_leases(self, *, now: datetime) -> tuple[OutboxRecord, ...]:
        recovered_at = _normalize_time(now)
        now_text = _encode_time(recovered_at)
        with self._write_transaction() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM delivery_outbox
                WHERE status = ? AND lease_until <= ?
                ORDER BY lease_until, global_sequence, outbox_id
                """,
                (OutboxStatus.LEASED.value, now_text),
            ).fetchall()
            recovered_ids: list[str] = []
            for row in rows:
                expires_at = _require_time(row["expires_at"])
                if recovered_at >= expires_at:
                    status = OutboxStatus.EXPIRED
                    next_attempt_at = None
                    last_error = "delivery outcome unknown after signal expiry"
                else:
                    status = OutboxStatus.DEAD_LETTER
                    next_attempt_at = None
                    last_error = "delivery outcome unknown after lease expiry"
                if row["last_error"] and str(row["last_error"]).startswith(
                    "delivery outcome unknown:"
                ):
                    last_error = f"{last_error}: {row['last_error']}"
                connection.execute(
                    """
                    UPDATE delivery_outbox
                    SET status = ?, next_attempt_at = ?, lease_owner = NULL,
                        lease_started_at = NULL, lease_until = NULL,
                        last_error = ?, updated_at = ?
                    WHERE outbox_id = ?
                    """,
                    (
                        status.value,
                        (_encode_time(next_attempt_at) if next_attempt_at is not None else None),
                        last_error,
                        now_text,
                        row["outbox_id"],
                    ),
                )
                recovered_ids.append(row["outbox_id"])
            if rows:
                self._before_commit(connection)
            updated = self._rows_for_outbox_ids(connection, recovered_ids)
            return tuple(self._outbox_from_row(row) for row in updated)

    def outbox_record(self, outbox_id: str) -> OutboxRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM delivery_outbox WHERE outbox_id = ?",
                (outbox_id,),
            ).fetchone()
            return None if row is None else self._outbox_from_row(row)
        finally:
            connection.close()

    def outbox_records(
        self,
        *,
        signal_id: str | None = None,
        status: OutboxStatus | None = None,
    ) -> tuple[OutboxRecord, ...]:
        clauses: list[str] = []
        parameters: list[object] = []
        if signal_id is not None:
            clauses.append("signal_id = ?")
            parameters.append(signal_id)
        if status is not None:
            clauses.append("status = ?")
            parameters.append(status.value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        connection = self._connect()
        try:
            rows = connection.execute(
                f"""
                SELECT * FROM delivery_outbox
                {where}
                ORDER BY global_sequence, recipient_id, channel, outbox_id
                """,
                parameters,
            ).fetchall()
            return tuple(self._outbox_from_row(row) for row in rows)
        finally:
            connection.close()

    def attempts(self, outbox_id: str | None = None) -> tuple[OutboxAttempt, ...]:
        connection = self._connect()
        try:
            if outbox_id is None:
                rows = connection.execute(
                    """
                    SELECT * FROM delivery_attempt
                    ORDER BY started_at, outbox_id, attempt_no
                    """
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT * FROM delivery_attempt
                    WHERE outbox_id = ?
                    ORDER BY attempt_no
                    """,
                    (outbox_id,),
                ).fetchall()
            return tuple(self._attempt_from_row(row) for row in rows)
        finally:
            connection.close()

    @staticmethod
    def _select_outbox_rows(
        connection: sqlite3.Connection,
        *,
        signal_id: str,
        target_keys: set[tuple[str, str]],
    ) -> list[sqlite3.Row]:
        rows = connection.execute(
            """
            SELECT * FROM delivery_outbox
            WHERE signal_id = ?
            ORDER BY recipient_id, channel, outbox_id
            """,
            (signal_id,),
        ).fetchall()
        return [row for row in rows if (row["recipient_id"], row["channel"]) in target_keys]

    @staticmethod
    def _rows_for_outbox_ids(
        connection: sqlite3.Connection,
        outbox_ids: list[str],
    ) -> list[sqlite3.Row]:
        if not outbox_ids:
            return []
        placeholders = ",".join("?" for _ in outbox_ids)
        rows = connection.execute(
            f"""
            SELECT * FROM delivery_outbox
            WHERE outbox_id IN ({placeholders})
            ORDER BY global_sequence, recipient_id, channel, outbox_id
            """,
            outbox_ids,
        ).fetchall()
        return list(rows)

    @staticmethod
    def _outbox_from_row(row: sqlite3.Row) -> OutboxRecord:
        return OutboxRecord(
            outbox_id=row["outbox_id"],
            signal_id=row["signal_id"],
            target=DeliveryTarget(
                recipient_id=row["recipient_id"],
                channel=DeliveryChannel(row["channel"]),
            ),
            status=OutboxStatus(row["status"]),
            expires_at=_require_time(row["expires_at"]),
            attempt_count=row["attempt_count"],
            next_attempt_at=_decode_time(row["next_attempt_at"]),
            lease_owner=row["lease_owner"],
            lease_until=_decode_time(row["lease_until"]),
            last_error=row["last_error"],
            created_at=_require_time(row["created_at"]),
            updated_at=_require_time(row["updated_at"]),
        )

    @staticmethod
    def _attempt_from_row(row: sqlite3.Row) -> OutboxAttempt:
        return OutboxAttempt(
            outbox_id=row["outbox_id"],
            attempt_no=row["attempt_no"],
            started_at=_require_time(row["started_at"]),
            completed_at=_require_time(row["completed_at"]),
            success=bool(row["success"]),
            provider_receipt=row["provider_receipt"],
            error=row["error"],
        )

    @staticmethod
    def _unknown_from_row(row: sqlite3.Row) -> UnknownDeliveryEvidence:
        return UnknownDeliveryEvidence(
            outbox_id=row["outbox_id"],
            attempt_no=row["attempt_no"],
            worker_id=row["worker_id"],
            observed_at=_require_time(row["observed_at"]),
            reason=row["reason"],
            provider_receipt=row["provider_receipt"],
        )

    @staticmethod
    def _route_cursor_from_row(row: sqlite3.Row) -> SignalRouteCursor:
        return SignalRouteCursor(
            source_id=row["source_id"],
            generation_id=row["generation_id"],
            strategy_spec_fingerprint=row["strategy_spec_fingerprint"],
            routing_policy_fingerprint=row["routing_policy_fingerprint"],
            first_sequence=row["first_sequence"],
            observed_high_watermark=row["observed_high_watermark"],
            last_sequence=row["last_sequence"],
            last_signal_id=row["last_signal_id"],
            updated_at=_decode_time(row["updated_at"]),
        )

    @staticmethod
    def _route_receipt_from_row(row: sqlite3.Row) -> SignalRouteReceipt:
        raw_targets = json.loads(row["target_manifest_json"])
        if not isinstance(raw_targets, list):
            raise ValueError("stored target manifest must be a list")
        targets = tuple(DeliveryTarget.model_validate(item) for item in raw_targets)
        if _payload_hash(_target_manifest_payload(targets)) != row["target_manifest_hash"]:
            raise ValueError("stored target manifest hash does not match its payload")
        return SignalRouteReceipt(
            source_id=row["source_id"],
            source_sequence=row["source_sequence"],
            signal_id=row["signal_id"],
            decision_fingerprint=row["decision_fingerprint"],
            disposition=RouteReceiptDisposition(row["disposition"]),
            reason_code=row["reason_code"],
            target_manifest_hash=row["target_manifest_hash"],
            targets=targets,
            target_count=len(targets),
            routed_at=_require_time(row["routed_at"]),
        )


def _require_time(value: str | None) -> datetime:
    decoded = _decode_time(value)
    if decoded is None:
        raise ValueError("stored datetime is unexpectedly NULL")
    return decoded
