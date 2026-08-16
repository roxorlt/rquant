"""Durable ordered consumption from the signal bus into the paper queue."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Protocol, Self

from pydantic import Field, StringConstraints, model_validator

from rquant.paper_signal_worker import (
    PaperSignalQueueRecord,
    PaperSignalQueueStatus,
    PaperSignalQueueStore,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.signal_bus import (
    SignalBusSignalRecord,
    SignalBusSourceDescriptor,
    SignalBusSourceSequenceError,
    parse_stored_signal,
    require_legacy_signal_write,
)
from rquant.signal_contracts import SignalEnvelopeFamily

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class PaperSignalConsumerSourceError(RuntimeError):
    """The signal bus source no longer matches the persisted consumer history."""


class PaperSignalSource(Protocol):
    def source_descriptor(self) -> SignalBusSourceDescriptor: ...

    def signals_after_global_sequence(
        self,
        *,
        after_sequence: int,
        through_sequence: int,
        observed_at: datetime,
        limit: int,
    ) -> tuple[SignalBusSignalRecord, ...]: ...


class PaperSignalReceiptStatus(StrEnum):
    BOUND = "bound"
    DELEGATED = "delegated"


class PaperSignalConsumerCursor(RuntimeContractModel):
    consumer_id: str = Field(min_length=1)
    source_id: str | None = Field(default=None, min_length=1)
    source_generation_id: Sha256 | None = None
    first_global_sequence: int = Field(default=1, ge=1)
    observed_high_watermark: int = Field(default=0, ge=0)
    last_global_sequence: int = Field(default=0, ge=0)
    last_signal_id: Sha256 | None = None
    updated_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def validate_progress(self) -> Self:
        if self.last_global_sequence > self.observed_high_watermark:
            raise ValueError("consumer cursor cannot exceed observed source high watermark")
        if (self.source_id is None) != (self.source_generation_id is None):
            raise ValueError("source id and generation must be bound together")
        if self.last_global_sequence == 0 and self.last_signal_id is not None:
            raise ValueError("empty cursor cannot have a last signal id")
        if self.last_global_sequence > 0 and self.last_signal_id is None:
            raise ValueError("advanced cursor requires a last signal id")
        return self


class PaperSignalConsumerReceipt(RuntimeContractModel):
    source_generation_id: Sha256
    global_sequence: int = Field(ge=1)
    signal_id: Sha256
    payload_hash: Sha256
    payload_json: str = Field(min_length=1)
    status: PaperSignalReceiptStatus
    bound_at: AwareUtcDatetime
    delegated_at: AwareUtcDatetime | None = None
    queue_status: PaperSignalQueueStatus | None = None

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        if self.status is PaperSignalReceiptStatus.BOUND:
            if self.delegated_at is not None or self.queue_status is not None:
                raise ValueError("bound receipt cannot contain delegation evidence")
        elif self.delegated_at is None or self.queue_status is None:
            raise ValueError("delegated receipt requires queue evidence")
        return self


class PaperSignalConsumerSummary(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    source_generation_id: Sha256
    source_high_watermark: int = Field(ge=0)
    started_after_sequence: int = Field(ge=0)
    ended_at_sequence: int = Field(ge=0)
    delegated_count: int = Field(ge=0)
    replayed_count: int = Field(ge=0)
    has_deferred_signals: bool


class _BindResult(RuntimeContractModel):
    receipt: PaperSignalConsumerReceipt
    replayed: bool


def _decode_time(value: str | None) -> datetime | None:
    return None if value is None else normalize_aware_utc(datetime.fromisoformat(value))


class PaperSignalConsumerStateStore:
    """Own the paper consumer cursor independently from both durable endpoints."""

    def __init__(
        self,
        path: Path | str,
        *,
        consumer_id: str = "paper-signal-consumer/v1",
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if not consumer_id.strip():
            raise ValueError("consumer_id must not be empty")
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.consumer_id = consumer_id.strip()
        self.busy_timeout_ms = busy_timeout_ms
        self.consumer_fingerprint = canonical_sha256(
            {"consumer_id": self.consumer_id, "contract": "paper-signal-consumer/v1"}
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
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS paper_consumer_metadata (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    consumer_id TEXT NOT NULL,
                    consumer_fingerprint TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_consumer_source (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    source_id TEXT NOT NULL,
                    source_generation_id TEXT NOT NULL,
                    first_global_sequence INTEGER NOT NULL CHECK(first_global_sequence >= 1),
                    observed_high_watermark INTEGER NOT NULL CHECK(observed_high_watermark >= 0),
                    last_global_sequence INTEGER NOT NULL CHECK(last_global_sequence >= 0),
                    last_signal_id TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_consumer_receipt (
                    global_sequence INTEGER PRIMARY KEY CHECK(global_sequence >= 1),
                    source_generation_id TEXT NOT NULL,
                    signal_id TEXT NOT NULL UNIQUE,
                    payload_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    bound_at TEXT NOT NULL,
                    delegated_at TEXT,
                    queue_status TEXT
                );
                """
            )
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM paper_consumer_metadata WHERE singleton = 1"
                ).fetchone()
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO paper_consumer_metadata(
                            singleton, consumer_id, consumer_fingerprint
                        ) VALUES (1, ?, ?)
                        """,
                        (self.consumer_id, self.consumer_fingerprint),
                    )
                elif (
                    row["consumer_id"] != self.consumer_id
                    or row["consumer_fingerprint"] != self.consumer_fingerprint
                ):
                    raise ValueError("paper consumer identity does not match persisted state")
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def validate_source(self, descriptor: SignalBusSourceDescriptor) -> PaperSignalConsumerCursor:
        """Check source continuity without changing consumer state."""

        cursor = self.cursor()
        if cursor.source_id is None:
            if descriptor.first_global_sequence != 1:
                raise PaperSignalConsumerSourceError(
                    "signal source is truncated before the consumer was initialized"
                )
            return cursor
        if cursor.source_id != descriptor.source_id:
            raise PaperSignalConsumerSourceError("signal source id changed")
        if cursor.source_generation_id != descriptor.generation_id:
            raise PaperSignalConsumerSourceError("signal source generation changed")
        if cursor.first_global_sequence != descriptor.first_global_sequence:
            raise PaperSignalConsumerSourceError("signal source start was truncated or changed")
        if descriptor.high_watermark < cursor.observed_high_watermark:
            raise PaperSignalConsumerSourceError("signal source high watermark rolled back")
        if descriptor.high_watermark < cursor.last_global_sequence:
            raise PaperSignalConsumerSourceError(
                "signal source high watermark precedes the consumer cursor"
            )
        return cursor

    def observe_source(
        self,
        descriptor: SignalBusSourceDescriptor,
        *,
        observed_at: datetime,
    ) -> PaperSignalConsumerCursor:
        observed = normalize_aware_utc(observed_at)
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM paper_consumer_source WHERE singleton = 1"
            ).fetchone()
            if row is None:
                if descriptor.first_global_sequence != 1:
                    raise PaperSignalConsumerSourceError(
                        "signal source is truncated before the consumer was initialized"
                    )
                connection.execute(
                    """
                    INSERT INTO paper_consumer_source(
                        singleton, source_id, source_generation_id,
                        first_global_sequence, observed_high_watermark,
                        last_global_sequence, last_signal_id, updated_at
                    ) VALUES (1, ?, ?, ?, ?, 0, NULL, ?)
                    """,
                    (
                        descriptor.source_id,
                        descriptor.generation_id,
                        descriptor.first_global_sequence,
                        descriptor.high_watermark,
                        observed.isoformat(),
                    ),
                )
            else:
                self._verify_source_row(row, descriptor)
                if descriptor.high_watermark < int(row["observed_high_watermark"]):
                    raise PaperSignalConsumerSourceError("signal source high watermark rolled back")
                if descriptor.high_watermark < int(row["last_global_sequence"]):
                    raise PaperSignalConsumerSourceError(
                        "signal source high watermark precedes the consumer cursor"
                    )
                connection.execute(
                    """
                    UPDATE paper_consumer_source
                    SET observed_high_watermark = ?, updated_at = ?
                    WHERE singleton = 1
                    """,
                    (descriptor.high_watermark, observed.isoformat()),
                )
        return self.cursor()

    @staticmethod
    def _verify_source_row(
        row: sqlite3.Row,
        descriptor: SignalBusSourceDescriptor,
    ) -> None:
        if row["source_id"] != descriptor.source_id:
            raise PaperSignalConsumerSourceError("signal source id changed")
        if row["source_generation_id"] != descriptor.generation_id:
            raise PaperSignalConsumerSourceError("signal source generation changed")
        if int(row["first_global_sequence"]) != descriptor.first_global_sequence:
            raise PaperSignalConsumerSourceError("signal source start was truncated or changed")

    def bind(
        self,
        record: SignalBusSignalRecord,
        descriptor: SignalBusSourceDescriptor,
        *,
        bound_at: datetime,
    ) -> _BindResult:
        self._verified_record_signal(record)
        bound = normalize_aware_utc(bound_at)
        if record.signal.available_at > bound or record.received_at > bound:
            raise ValueError("future signal cannot be bound by paper consumer")
        with self._write_transaction() as connection:
            source = self._required_source(connection)
            self._verify_source_row(source, descriptor)
            existing = connection.execute(
                "SELECT * FROM paper_consumer_receipt WHERE global_sequence = ?",
                (record.global_sequence,),
            ).fetchone()
            if existing is not None:
                receipt = self._receipt_from_row(existing)
                self._verify_receipt_identity(receipt, record, descriptor)
                return _BindResult(receipt=receipt, replayed=True)
            if record.global_sequence != int(source["last_global_sequence"]) + 1:
                raise PaperSignalConsumerSourceError(
                    "signal sequence gap or truncated consumer source"
                )
            if record.global_sequence > int(source["observed_high_watermark"]):
                raise PaperSignalConsumerSourceError(
                    "signal sequence exceeds the observed source high watermark"
                )
            connection.execute(
                """
                INSERT INTO paper_consumer_receipt(
                    global_sequence, source_generation_id, signal_id,
                    payload_hash, payload_json, status, bound_at,
                    delegated_at, queue_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                """,
                (
                    record.global_sequence,
                    descriptor.generation_id,
                    record.signal_id,
                    record.payload_hash,
                    record.payload_json,
                    PaperSignalReceiptStatus.BOUND.value,
                    bound.isoformat(),
                ),
            )
        receipt = self.receipt(record.global_sequence)
        assert receipt is not None
        return _BindResult(receipt=receipt, replayed=False)

    @staticmethod
    def _verify_receipt_identity(
        receipt: PaperSignalConsumerReceipt,
        record: SignalBusSignalRecord,
        descriptor: SignalBusSourceDescriptor,
    ) -> None:
        expected = (
            descriptor.generation_id,
            record.signal_id,
            record.payload_hash,
            record.payload_json,
        )
        actual = (
            receipt.source_generation_id,
            receipt.signal_id,
            receipt.payload_hash,
            receipt.payload_json,
        )
        if actual != expected:
            raise PaperSignalConsumerSourceError(
                "signal payload identity changed for a persisted sequence"
            )

    def complete(
        self,
        record: SignalBusSignalRecord,
        descriptor: SignalBusSourceDescriptor,
        queue_record: PaperSignalQueueRecord,
        *,
        delegated_at: datetime,
    ) -> PaperSignalConsumerReceipt:
        self._verified_record_signal(record)
        delegated = normalize_aware_utc(delegated_at)
        if queue_record.signal != record.signal:
            raise ValueError("paper queue record does not match the bound signal")
        with self._write_transaction() as connection:
            source = self._required_source(connection)
            self._verify_source_row(source, descriptor)
            row = connection.execute(
                "SELECT * FROM paper_consumer_receipt WHERE global_sequence = ?",
                (record.global_sequence,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unbound paper signal sequence: {record.global_sequence}")
            receipt = self._receipt_from_row(row)
            self._verify_receipt_identity(receipt, record, descriptor)
            if receipt.status is PaperSignalReceiptStatus.DELEGATED:
                return receipt
            last_sequence = int(source["last_global_sequence"])
            if record.global_sequence != last_sequence + 1:
                raise PaperSignalConsumerSourceError(
                    "consumer cursor cannot skip a bound signal sequence"
                )
            connection.execute(
                """
                UPDATE paper_consumer_receipt
                SET status = ?, delegated_at = ?, queue_status = ?
                WHERE global_sequence = ? AND status = ?
                """,
                (
                    PaperSignalReceiptStatus.DELEGATED.value,
                    delegated.isoformat(),
                    queue_record.status.value,
                    record.global_sequence,
                    PaperSignalReceiptStatus.BOUND.value,
                ),
            )
            connection.execute(
                """
                UPDATE paper_consumer_source
                SET last_global_sequence = ?, last_signal_id = ?, updated_at = ?
                WHERE singleton = 1 AND last_global_sequence = ?
                """,
                (
                    record.global_sequence,
                    record.signal_id,
                    delegated.isoformat(),
                    last_sequence,
                ),
            )
        completed = self.receipt(record.global_sequence)
        assert completed is not None
        return completed

    @staticmethod
    def _required_source(connection: sqlite3.Connection) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM paper_consumer_source WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("paper consumer source is not initialized")
        return row

    def cursor(self) -> PaperSignalConsumerCursor:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM paper_consumer_source WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return PaperSignalConsumerCursor(consumer_id=self.consumer_id)
        return PaperSignalConsumerCursor(
            consumer_id=self.consumer_id,
            source_id=row["source_id"],
            source_generation_id=row["source_generation_id"],
            first_global_sequence=row["first_global_sequence"],
            observed_high_watermark=row["observed_high_watermark"],
            last_global_sequence=row["last_global_sequence"],
            last_signal_id=row["last_signal_id"],
            updated_at=_decode_time(row["updated_at"]),
        )

    def receipt(self, global_sequence: int) -> PaperSignalConsumerReceipt | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM paper_consumer_receipt WHERE global_sequence = ?",
                (global_sequence,),
            ).fetchone()
        return None if row is None else self._receipt_from_row(row)

    def receipts(self) -> tuple[PaperSignalConsumerReceipt, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM paper_consumer_receipt ORDER BY global_sequence"
            ).fetchall()
        return tuple(self._receipt_from_row(row) for row in rows)

    @staticmethod
    def _receipt_from_row(row: sqlite3.Row) -> PaperSignalConsumerReceipt:
        parse_stored_signal(
            signal_id=str(row["signal_id"]),
            payload_hash=str(row["payload_hash"]),
            payload_json=str(row["payload_json"]),
            payload_size=len(str(row["payload_json"]).encode("utf-8")),
        )
        return PaperSignalConsumerReceipt(
            source_generation_id=row["source_generation_id"],
            global_sequence=row["global_sequence"],
            signal_id=row["signal_id"],
            payload_hash=row["payload_hash"],
            payload_json=row["payload_json"],
            status=PaperSignalReceiptStatus(row["status"]),
            bound_at=_decode_time(row["bound_at"]),
            delegated_at=_decode_time(row["delegated_at"]),
            queue_status=(
                PaperSignalQueueStatus(row["queue_status"])
                if row["queue_status"] is not None
                else None
            ),
        )

    @staticmethod
    def _verified_record_signal(record: SignalBusSignalRecord) -> SignalEnvelopeFamily:
        signal = parse_stored_signal(
            signal_id=record.signal_id,
            payload_hash=record.payload_hash,
            payload_json=record.payload_json,
            payload_size=len(record.payload_json.encode("utf-8")),
        )
        if type(signal) is not type(record.signal) or signal != record.signal:
            raise PaperSignalConsumerSourceError("signal record does not match its stored payload")
        return signal

    def _after_paper_ingest(
        self,
        _record: SignalBusSignalRecord,
        _queue_record: PaperSignalQueueRecord,
    ) -> None:
        """Fault-injection boundary between the two durable SQLite endpoints."""


def consume_signal_bus_to_paper(
    bus: PaperSignalSource,
    queue: PaperSignalQueueStore,
    state: PaperSignalConsumerStateStore,
    *,
    observed_at: datetime,
    limit: int,
) -> PaperSignalConsumerSummary:
    observed = normalize_aware_utc(observed_at)
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("limit must be a positive integer")
    descriptor = bus.source_descriptor()
    starting_cursor = state.validate_source(descriptor)
    try:
        records = bus.signals_after_global_sequence(
            after_sequence=starting_cursor.last_global_sequence,
            through_sequence=descriptor.high_watermark,
            observed_at=observed,
            limit=limit,
        )
    except SignalBusSourceSequenceError as error:
        raise PaperSignalConsumerSourceError(str(error)) from error
    verified_records = tuple(state._verified_record_signal(record) for record in records)
    for signal in verified_records:
        require_legacy_signal_write(
            signal,
            operation="consume_signal_bus_to_paper",
        )
    starting_cursor = state.observe_source(descriptor, observed_at=observed)

    delegated_count = 0
    replayed_count = 0
    for record, signal in zip(records, verified_records, strict=True):
        binding = state.bind(record, descriptor, bound_at=observed)
        if binding.replayed:
            replayed_count += 1
        if binding.receipt.status is PaperSignalReceiptStatus.DELEGATED:
            continue
        queue_record = queue.ingest(
            signal,
            received_at=observed,
            payload_json=record.payload_json,
            payload_hash=record.payload_hash,
            payload_size=len(record.payload_json.encode("utf-8")),
        )
        state._after_paper_ingest(record, queue_record)
        state.complete(
            record,
            descriptor,
            queue_record,
            delegated_at=observed,
        )
        delegated_count += 1

    ending_cursor = state.cursor()
    return PaperSignalConsumerSummary(
        observed_at=observed,
        source_generation_id=descriptor.generation_id,
        source_high_watermark=descriptor.high_watermark,
        started_after_sequence=starting_cursor.last_global_sequence,
        ended_at_sequence=ending_cursor.last_global_sequence,
        delegated_count=delegated_count,
        replayed_count=replayed_count,
        has_deferred_signals=(ending_cursor.last_global_sequence < descriptor.high_watermark),
    )
