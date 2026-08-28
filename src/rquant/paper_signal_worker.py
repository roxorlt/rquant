"""Persistent signal intake and next-minute execution for the paper broker."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Self

from pydantic import Field, StringConstraints, field_serializer, field_validator, model_validator

from rquant.paper_broker import (
    BrokerExecutionContext,
    NoExecutableSellQuantityError,
    PaperBrokerStore,
)
from rquant.paper_contracts import (
    PaperOrder,
    PaperOrderIntent,
    PaperOrderType,
    PaperSellQuantityAuthority,
    PaperSide,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.signal_bus import parse_stored_signal, require_legacy_signal_write
from rquant.signal_contracts import (
    CurrentSignalEnvelope,
    SignalAction,
    SignalEnvelope,
    SignalEnvelopeFamily,
    parse_signal_envelope,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]


def _entry_signal_id(signal: SignalEnvelopeFamily) -> str | None:
    if signal.action is SignalAction.B_INTENT:
        return None
    value = signal.evidence.get("entry_signal_id")
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("SELL/REDUCE signal requires a lowercase SHA-256 entry_signal_id")
    if value == signal.signal_id:
        raise ValueError("entry_signal_id cannot equal the exit signal_id")
    return value


def _sell_tranche_fraction(signal: SignalEnvelopeFamily) -> Decimal:
    if signal.action not in {SignalAction.REDUCE, SignalAction.S_INTENT}:
        raise ValueError("sell tranche is only valid for SELL/REDUCE signals")
    raw = signal.evidence.get("sell_tranche_fraction")
    try:
        fraction = Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("SELL/REDUCE signal requires sell_tranche_fraction") from exc
    if signal.action is SignalAction.S_INTENT and fraction != Decimal("1"):
        raise ValueError("S_INTENT sell_tranche_fraction must equal one")
    if signal.action is SignalAction.REDUCE and not Decimal("0") < fraction < Decimal("1"):
        raise ValueError("REDUCE sell_tranche_fraction must be between zero and one")
    return fraction


class PaperSignalQueueStatus(StrEnum):
    PENDING = "pending"
    PREPARED = "prepared"
    COMPLETED = "completed"
    IGNORED = "ignored"
    EXPIRED = "expired"


class PaperSignalPolicy(RuntimeContractModel):
    account_id: str = Field(min_length=1)
    execution_lag: timedelta
    action_quantities: Mapping[SignalAction, int]
    producer_commit: CommitSha

    @field_validator("execution_lag")
    @classmethod
    def validate_execution_lag(cls, value: timedelta) -> timedelta:
        if value <= timedelta(0):
            raise ValueError("execution_lag must be positive")
        return value

    @field_validator("action_quantities")
    @classmethod
    def validate_action_quantities(
        cls,
        value: Mapping[SignalAction, int],
    ) -> Mapping[SignalAction, int]:
        normalized = {SignalAction(action): quantity for action, quantity in value.items()}
        executable = {
            SignalAction.B_INTENT,
            SignalAction.REDUCE,
            SignalAction.S_INTENT,
        }
        if set(normalized) != executable:
            raise ValueError("action_quantities must exactly cover executable signal actions")
        if any(quantity <= 0 or quantity % 100 for quantity in normalized.values()):
            raise ValueError("paper quantities must be positive 100-share lots")
        return MappingProxyType(dict(sorted(normalized.items(), key=lambda item: item[0].value)))

    @field_serializer("action_quantities")
    def serialize_action_quantities(
        self,
        value: Mapping[SignalAction, int],
    ) -> dict[str, int]:
        return {action.value: quantity for action, quantity in value.items()}

    @property
    def fingerprint(self) -> str:
        return self.semantic_fingerprint

    @property
    def semantic_fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="json", exclude={"producer_commit"}))

    @property
    def provenance_fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class PaperQuoteSnapshot(RuntimeContractModel):
    snapshot_id: Sha256 | None = None
    ts_code: str = Field(min_length=1)
    event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    context: BrokerExecutionContext
    producer_commit: CommitSha
    constraint_snapshot_id: Sha256 | None = None
    constraint_batch_id: Sha256 | None = None
    constraint_authority_sha256: Sha256 | None = None
    constraint_source_snapshot_ids: Mapping[str, Sha256] = Field(default_factory=dict)

    @field_validator("constraint_source_snapshot_ids")
    @classmethod
    def freeze_constraint_source_snapshot_ids(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if any(not key for key in value):
            raise ValueError("constraint source snapshot ids cannot have empty keys")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("constraint_source_snapshot_ids")
    def serialize_constraint_source_snapshot_ids(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_quote(self) -> Self:
        if self.event_time > self.available_at:
            raise ValueError("quote event_time cannot exceed available_at")
        authority_bound = (
            self.constraint_snapshot_id,
            self.constraint_batch_id,
            self.constraint_authority_sha256,
        )
        if any(value is not None for value in authority_bound) != all(
            value is not None for value in authority_bound
        ):
            raise ValueError("constraint authority evidence must be configured together")
        if not all(value is not None for value in authority_bound):
            if self.constraint_source_snapshot_ids:
                raise ValueError(
                    "constraint source snapshots require constraint authority evidence"
                )
        elif not self.constraint_source_snapshot_ids:
            raise ValueError("constraint authority evidence requires source snapshots")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"snapshot_id"}))
        if self.snapshot_id is None:
            object.__setattr__(self, "snapshot_id", expected)
        elif self.snapshot_id != expected:
            raise ValueError("snapshot_id does not match quote content")
        return self


class PaperSignalQueueRecord(RuntimeContractModel):
    signal: SignalEnvelopeFamily
    status: PaperSignalQueueStatus
    due_at: AwareUtcDatetime
    received_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    last_error: str | None = None
    execution_id: Sha256 | None = None
    quote: PaperQuoteSnapshot | None = None
    intent: PaperOrderIntent | None = None
    order: PaperOrder | None = None

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if self.updated_at < self.received_at:
            raise ValueError("updated_at cannot precede received_at")
        if self.status is PaperSignalQueueStatus.PENDING:
            if (
                self.execution_id is not None
                or self.quote is not None
                or self.intent is not None
                or self.order is not None
            ):
                raise ValueError("pending signal cannot contain execution evidence")
        elif self.status is PaperSignalQueueStatus.PREPARED:
            if (
                self.execution_id is None
                or self.quote is None
                or self.intent is None
                or self.order is not None
            ):
                raise ValueError("prepared signal requires execution identity, quote, and intent")
        elif self.status is PaperSignalQueueStatus.COMPLETED:
            if (
                self.execution_id is None
                or self.quote is None
                or self.intent is None
                or self.order is None
            ):
                raise ValueError(
                    "completed signal requires execution identity, quote, intent, and order"
                )
        elif self.status is PaperSignalQueueStatus.IGNORED:
            if (
                self.execution_id is not None
                or self.quote is not None
                or self.intent is not None
                or self.order is not None
            ):
                raise ValueError("ignored signal cannot contain execution evidence")
        elif self.order is not None or not (
            (self.execution_id is None and self.quote is None and self.intent is None)
            or (
                self.execution_id is not None and self.quote is not None and self.intent is not None
            )
        ):
            raise ValueError(
                "expired signal may retain execution identity, quote, and intent as one audit set"
            )
        return self


class PaperSignalRunSummary(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    due_count: int = Field(ge=0)
    completed_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)


QuoteResolver = Callable[[SignalEnvelopeFamily, datetime], PaperQuoteSnapshot]


def _json(model: RuntimeContractModel) -> str:
    return json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _error_text(error: BaseException) -> str:
    message = str(error).strip() or "no detail"
    return f"{type(error).__name__}: {message}"


def _prepared_execution_id(
    *,
    signal: SignalEnvelopeFamily,
    intent: PaperOrderIntent,
    quote: PaperQuoteSnapshot,
    due_at: datetime,
) -> str:
    return canonical_sha256(
        {
            "producer": "paper_signal_worker",
            "signal_id": signal.signal_id,
            "intent_id": intent.intent_id,
            "quote_snapshot_id": quote.snapshot_id,
            "due_at": due_at,
        }
    )


class PaperSignalQueueStore:
    """Own immutable prepared intents before calling the independently durable broker."""

    def __init__(
        self,
        path: Path,
        *,
        policy: PaperSignalPolicy,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.policy = policy
        self.busy_timeout_ms = busy_timeout_ms
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _enable_wal(self, connection: sqlite3.Connection) -> None:
        deadline = time.monotonic() + self.busy_timeout_ms / 1_000
        while True:
            try:
                mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
                if mode != "wal":
                    mode = str(
                        connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
                    ).lower()
                if mode != "wal":
                    raise RuntimeError("paper signal queue requires WAL mode")
                return
            except sqlite3.OperationalError as error:
                if "locked" not in str(error).lower() or time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            self._enable_wal(connection)
            try:
                connection.executescript(
                    """
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS paper_signal_metadata (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    policy_fingerprint TEXT NOT NULL,
                    policy_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_signal_queue (
                    signal_id TEXT PRIMARY KEY,
                    signal_json TEXT NOT NULL,
                    signal_hash TEXT NOT NULL,
                    signal_size INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    due_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_error TEXT,
                    execution_id TEXT,
                    quote_json TEXT,
                    intent_json TEXT,
                    order_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_paper_signal_due
                ON paper_signal_queue(status, due_at, signal_id);
                CREATE TABLE IF NOT EXISTS paper_signal_prepare_history (
                    signal_id TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK(revision >= 1),
                    execution_id TEXT,
                    quote_json TEXT NOT NULL,
                    intent_json TEXT NOT NULL,
                    replaced_at TEXT NOT NULL,
                    PRIMARY KEY(signal_id, revision)
                );
                """
                )
                queue_columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(paper_signal_queue)"
                    ).fetchall()
                }
                if "execution_id" not in queue_columns:
                    connection.execute(
                        "ALTER TABLE paper_signal_queue ADD COLUMN execution_id TEXT"
                    )
                if "signal_hash" not in queue_columns:
                    connection.execute("ALTER TABLE paper_signal_queue ADD COLUMN signal_hash TEXT")
                if "signal_size" not in queue_columns:
                    connection.execute(
                        "ALTER TABLE paper_signal_queue ADD COLUMN signal_size INTEGER"
                    )
                signal_rows = connection.execute(
                    """
                    SELECT signal_id, signal_json, signal_hash, signal_size
                    FROM paper_signal_queue
                    """
                ).fetchall()
                for signal_row in signal_rows:
                    payload_json = str(signal_row["signal_json"])
                    payload_bytes = payload_json.encode("utf-8")
                    payload_hash = (
                        sha256(payload_bytes).hexdigest()
                        if signal_row["signal_hash"] is None
                        else str(signal_row["signal_hash"])
                    )
                    payload_size = (
                        len(payload_bytes)
                        if signal_row["signal_size"] is None
                        else int(signal_row["signal_size"])
                    )
                    parse_stored_signal(
                        signal_id=str(signal_row["signal_id"]),
                        payload_hash=payload_hash,
                        payload_json=payload_json,
                        payload_size=payload_size,
                    )
                    if signal_row["signal_hash"] is None or signal_row["signal_size"] is None:
                        connection.execute(
                            """
                            UPDATE paper_signal_queue
                            SET signal_hash = ?, signal_size = ? WHERE signal_id = ?
                            """,
                            (payload_hash, payload_size, signal_row["signal_id"]),
                        )
                if "expires_at" not in queue_columns:
                    connection.execute("ALTER TABLE paper_signal_queue ADD COLUMN expires_at TEXT")
                    legacy_rows = connection.execute(
                        "SELECT signal_id, signal_json FROM paper_signal_queue"
                    ).fetchall()
                    for legacy_row in legacy_rows:
                        try:
                            payload_json = str(legacy_row["signal_json"])
                            legacy_signal = parse_stored_signal(
                                signal_id=str(legacy_row["signal_id"]),
                                payload_hash=sha256(payload_json.encode("utf-8")).hexdigest(),
                                payload_json=payload_json,
                                payload_size=len(payload_json.encode("utf-8")),
                            )
                        except (TypeError, ValueError):
                            continue
                        connection.execute(
                            """
                            UPDATE paper_signal_queue SET expires_at = ? WHERE signal_id = ?
                            """,
                            (legacy_signal.expires_at.isoformat(), legacy_row["signal_id"]),
                        )
                history_columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(paper_signal_prepare_history)"
                    ).fetchall()
                }
                if "execution_id" not in history_columns:
                    connection.execute(
                        "ALTER TABLE paper_signal_prepare_history ADD COLUMN execution_id TEXT"
                    )
                connection.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_signal_execution
                    ON paper_signal_queue(execution_id) WHERE execution_id IS NOT NULL
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_paper_signal_expiry
                    ON paper_signal_queue(status, expires_at, signal_id)
                    """
                )
                row = connection.execute(
                    """
                    SELECT policy_fingerprint, policy_json
                    FROM paper_signal_metadata WHERE singleton = 1
                    """
                ).fetchone()
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO paper_signal_metadata(
                            singleton, policy_fingerprint, policy_json
                        ) VALUES (1, ?, ?)
                        """,
                        (self.policy.fingerprint, _json(self.policy)),
                    )
                else:
                    try:
                        persisted_policy = PaperSignalPolicy.model_validate_json(row["policy_json"])
                    except (ValueError, TypeError) as exc:
                        raise ValueError(
                            "paper signal policy does not match persisted queue policy"
                        ) from exc
                    stored_fingerprint = str(row["policy_fingerprint"])
                    semantics_match = (
                        persisted_policy.semantic_fingerprint == self.policy.semantic_fingerprint
                    )
                    is_current = stored_fingerprint == self.policy.semantic_fingerprint
                    is_legacy = stored_fingerprint == persisted_policy.provenance_fingerprint
                    if not semantics_match or not (is_current or is_legacy):
                        raise ValueError(
                            "paper signal policy does not match persisted queue policy"
                        )
                    if is_legacy:
                        connection.execute(
                            """
                            UPDATE paper_signal_metadata SET policy_fingerprint = ?
                            WHERE singleton = 1
                            """,
                            (self.policy.semantic_fingerprint,),
                        )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def ingest(
        self,
        signal: SignalEnvelopeFamily,
        *,
        received_at: datetime,
        payload_json: str | None = None,
        payload_hash: str | None = None,
        payload_size: int | None = None,
    ) -> PaperSignalQueueRecord:
        payload_values = (payload_json, payload_hash, payload_size)
        if any(value is None for value in payload_values) and any(
            value is not None for value in payload_values
        ):
            raise ValueError("stored paper signal payload metadata must be configured together")
        if type(signal) is CurrentSignalEnvelope:
            require_legacy_signal_write(
                signal,
                operation="PaperSignalQueueStore.ingest",
            )
        if payload_json is None:
            if type(signal) is not SignalEnvelope:
                raise TypeError("paper queue direct ingestion requires a SignalEnvelope")
            parsed_signal = parse_signal_envelope(signal.model_dump(mode="json"))
            if type(parsed_signal) is not SignalEnvelope:
                raise TypeError("paper queue direct ingestion requires a SignalEnvelope")
            signal_json = _json(parsed_signal)
            signal_hash = sha256(signal_json.encode("utf-8")).hexdigest()
            signal_size = len(signal_json.encode("utf-8"))
        else:
            assert payload_hash is not None and payload_size is not None
            parsed_signal = parse_stored_signal(
                signal_id=signal.signal_id,
                payload_hash=payload_hash,
                payload_json=payload_json,
                payload_size=payload_size,
            )
            if type(parsed_signal) is not type(signal) or parsed_signal != signal:
                raise ValueError("stored paper signal does not match supplied signal")
            signal_json = payload_json
            signal_hash = payload_hash
            signal_size = payload_size
        received = normalize_aware_utc(received_at)
        if received < parsed_signal.available_at:
            raise ValueError("received_at cannot precede signal available_at")
        due_at = max(
            parsed_signal.available_at, parsed_signal.event_time + self.policy.execution_lag
        )
        if parsed_signal.action not in self.policy.action_quantities:
            status = PaperSignalQueueStatus.IGNORED
            error = f"action {parsed_signal.action.value} is not executable by paper broker"
        elif due_at >= parsed_signal.expires_at:
            status = PaperSignalQueueStatus.EXPIRED
            error = "signal expires before earliest paper execution"
        else:
            status = PaperSignalQueueStatus.PENDING
            error = None

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT * FROM paper_signal_queue WHERE signal_id = ?",
                    (parsed_signal.signal_id,),
                ).fetchone()
                if existing is not None:
                    if existing["signal_json"] != signal_json:
                        raise ValueError("signal_id already has different paper queue content")
                    connection.rollback()
                    return self._record_from_row(existing)
                connection.execute(
                    """
                    INSERT INTO paper_signal_queue(
                        signal_id, signal_json, status, due_at, expires_at,
                        received_at, updated_at, last_error, signal_hash, signal_size
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        parsed_signal.signal_id,
                        signal_json,
                        status.value,
                        due_at.isoformat(),
                        parsed_signal.expires_at.isoformat(),
                        received.isoformat(),
                        received.isoformat(),
                        error,
                        signal_hash,
                        signal_size,
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        record = self.record(parsed_signal.signal_id)
        assert record is not None
        return record

    def due_records(self, *, now: datetime, limit: int) -> tuple[PaperSignalQueueRecord, ...]:
        observed = normalize_aware_utc(now)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                expiring_rows = connection.execute(
                    """
                    SELECT * FROM paper_signal_queue
                    WHERE status = ? AND expires_at <= ?
                    """,
                    (PaperSignalQueueStatus.PENDING.value, observed.isoformat()),
                ).fetchall()
                for row in expiring_rows:
                    self._record_from_row(row)
                rows = connection.execute(
                    """
                    SELECT * FROM paper_signal_queue
                    WHERE (
                        status = ? OR (status = ? AND expires_at > ?)
                    ) AND due_at <= ?
                    ORDER BY due_at, signal_id LIMIT ?
                    """,
                    (
                        PaperSignalQueueStatus.PREPARED.value,
                        PaperSignalQueueStatus.PENDING.value,
                        observed.isoformat(),
                        observed.isoformat(),
                        limit,
                    ),
                ).fetchall()
                records = tuple(self._record_from_row(row) for row in rows)
                connection.execute(
                    """
                    UPDATE paper_signal_queue
                    SET status = ?,
                        last_error = 'signal expired before paper execution',
                        updated_at = ?
                    WHERE status = ? AND expires_at <= ?
                    """,
                    (
                        PaperSignalQueueStatus.EXPIRED.value,
                        observed.isoformat(),
                        PaperSignalQueueStatus.PENDING.value,
                        observed.isoformat(),
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return records

    def prepare(
        self,
        signal_id: str,
        *,
        quote: PaperQuoteSnapshot,
        prepared_at: datetime,
        sell_quantity_authority: PaperSellQuantityAuthority | None = None,
    ) -> PaperSignalQueueRecord:
        prepared = normalize_aware_utc(prepared_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._required_row(connection, signal_id)
                record = self._record_from_row(row)
                if record.status is PaperSignalQueueStatus.PREPARED:
                    connection.rollback()
                    return record
                if record.status is not PaperSignalQueueStatus.PENDING:
                    raise ValueError(f"cannot prepare paper signal in {record.status.value}")
                if prepared < record.due_at:
                    raise ValueError("paper signal is not due")
                if prepared >= record.signal.expires_at:
                    raise ValueError("paper signal expired before preparation")
                if quote.ts_code != record.signal.candidate_id:
                    raise ValueError("quote ts_code does not match signal candidate")
                if quote.available_at > prepared:
                    raise ValueError("quote is not available at paper decision time")
                action = record.signal.action
                side = PaperSide.BUY if action is SignalAction.B_INTENT else PaperSide.SELL
                if side is PaperSide.BUY:
                    if sell_quantity_authority is not None:
                        raise ValueError("BUY preparation cannot carry SELL quantity authority")
                    quantity = self.policy.action_quantities[action]
                else:
                    self._validate_sell_authority(
                        record.signal,
                        sell_quantity_authority,
                        decision_cutoff=prepared,
                    )
                    assert sell_quantity_authority is not None
                    quantity = sell_quantity_authority.requested_quantity
                intent = PaperOrderIntent(
                    signal_id=record.signal.signal_id,
                    entry_signal_id=_entry_signal_id(record.signal),
                    sell_quantity_authority=sell_quantity_authority,
                    account_id=self.policy.account_id,
                    ts_code=record.signal.candidate_id,
                    side=side,
                    order_type=PaperOrderType.MARKET,
                    quantity=quantity,
                    event_time=record.signal.event_time,
                    available_at=record.signal.available_at,
                    expires_at=record.signal.expires_at,
                    earliest_execution_at=record.due_at,
                    price_snapshot_id=quote.snapshot_id,
                    producer_commit=self.policy.producer_commit,
                )
                execution_id = _prepared_execution_id(
                    signal=record.signal,
                    intent=intent,
                    quote=quote,
                    due_at=record.due_at,
                )
                connection.execute(
                    """
                    UPDATE paper_signal_queue
                    SET status = ?, execution_id = ?, quote_json = ?, intent_json = ?,
                        last_error = NULL, updated_at = ?
                    WHERE signal_id = ? AND status = ?
                    """,
                    (
                        PaperSignalQueueStatus.PREPARED.value,
                        execution_id,
                        _json(quote),
                        _json(intent),
                        prepared.isoformat(),
                        signal_id,
                        PaperSignalQueueStatus.PENDING.value,
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        result = self.record(signal_id)
        assert result is not None
        return result

    def complete(
        self,
        signal_id: str,
        *,
        order: PaperOrder,
        completed_at: datetime,
    ) -> PaperSignalQueueRecord:
        completed = normalize_aware_utc(completed_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._required_row(connection, signal_id)
                record = self._record_from_row(row)
                if record.status is PaperSignalQueueStatus.COMPLETED:
                    if record.order != order:
                        raise ValueError("completed paper order evidence is immutable")
                    connection.rollback()
                    return record
                if record.status is not PaperSignalQueueStatus.PREPARED or record.intent is None:
                    raise ValueError("paper signal must be prepared before completion")
                if order.intent_id != record.intent.intent_id:
                    raise ValueError("paper order does not match prepared intent")
                connection.execute(
                    """
                    UPDATE paper_signal_queue
                    SET status = ?, order_json = ?, last_error = NULL, updated_at = ?
                    WHERE signal_id = ?
                    """,
                    (
                        PaperSignalQueueStatus.COMPLETED.value,
                        _json(order),
                        completed.isoformat(),
                        signal_id,
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        result = self.record(signal_id)
        assert result is not None
        return result

    def refresh_prepared(
        self,
        signal_id: str,
        *,
        quote: PaperQuoteSnapshot,
        prepared_at: datetime,
        sell_quantity_authority: PaperSellQuantityAuthority | None = None,
    ) -> PaperSignalQueueRecord:
        """Replace an unsubmitted prepared quote while retaining prior audit evidence."""

        prepared = normalize_aware_utc(prepared_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._required_row(connection, signal_id)
                record = self._record_from_row(row)
                if (
                    record.status is not PaperSignalQueueStatus.PREPARED
                    or record.quote is None
                    or record.intent is None
                ):
                    raise ValueError("paper signal must be prepared before quote refresh")
                if prepared >= record.signal.expires_at:
                    raise ValueError("paper signal expired before quote refresh")
                if quote.ts_code != record.signal.candidate_id:
                    raise ValueError("quote ts_code does not match signal candidate")
                if quote.available_at > prepared:
                    raise ValueError("quote is not available at paper decision time")
                action = record.signal.action
                side = PaperSide.BUY if action is SignalAction.B_INTENT else PaperSide.SELL
                if side is PaperSide.BUY:
                    if sell_quantity_authority is not None:
                        raise ValueError("BUY preparation cannot carry SELL quantity authority")
                    quantity = self.policy.action_quantities[action]
                else:
                    self._validate_sell_authority(
                        record.signal,
                        sell_quantity_authority,
                        decision_cutoff=prepared,
                    )
                    assert sell_quantity_authority is not None
                    quantity = sell_quantity_authority.requested_quantity
                intent = PaperOrderIntent(
                    signal_id=record.signal.signal_id,
                    entry_signal_id=_entry_signal_id(record.signal),
                    sell_quantity_authority=sell_quantity_authority,
                    account_id=self.policy.account_id,
                    ts_code=record.signal.candidate_id,
                    side=side,
                    order_type=PaperOrderType.MARKET,
                    quantity=quantity,
                    event_time=record.signal.event_time,
                    available_at=record.signal.available_at,
                    expires_at=record.signal.expires_at,
                    earliest_execution_at=record.due_at,
                    price_snapshot_id=quote.snapshot_id,
                    producer_commit=self.policy.producer_commit,
                )
                execution_id = _prepared_execution_id(
                    signal=record.signal,
                    intent=intent,
                    quote=quote,
                    due_at=record.due_at,
                )
                if (
                    quote == record.quote
                    and intent == record.intent
                    and execution_id == record.execution_id
                ):
                    connection.rollback()
                    return record
                revision = int(
                    connection.execute(
                        """
                        SELECT COALESCE(MAX(revision), 0) + 1
                        FROM paper_signal_prepare_history WHERE signal_id = ?
                        """,
                        (signal_id,),
                    ).fetchone()[0]
                )
                connection.execute(
                    """
                    INSERT INTO paper_signal_prepare_history(
                        signal_id, revision, execution_id, quote_json, intent_json, replaced_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        signal_id,
                        revision,
                        record.execution_id,
                        _json(record.quote),
                        _json(record.intent),
                        prepared.isoformat(),
                    ),
                )
                connection.execute(
                    """
                    UPDATE paper_signal_queue
                    SET execution_id = ?, quote_json = ?, intent_json = ?,
                        last_error = NULL, updated_at = ?
                    WHERE signal_id = ? AND status = ?
                    """,
                    (
                        execution_id,
                        _json(quote),
                        _json(intent),
                        prepared.isoformat(),
                        signal_id,
                        PaperSignalQueueStatus.PREPARED.value,
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        result = self.record(signal_id)
        assert result is not None
        return result

    def ignore(
        self,
        signal_id: str,
        *,
        reason: str,
        observed_at: datetime,
    ) -> PaperSignalQueueRecord:
        message = reason.strip()
        if not message:
            raise ValueError("ignore reason must not be empty")
        observed = normalize_aware_utc(observed_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._required_row(connection, signal_id)
                record = self._record_from_row(row)
                if record.status is not PaperSignalQueueStatus.PENDING:
                    raise ValueError(f"cannot ignore paper signal in {record.status.value}")
                connection.execute(
                    """
                    UPDATE paper_signal_queue
                    SET status = ?, last_error = ?, updated_at = ?
                    WHERE signal_id = ? AND status = ?
                    """,
                    (
                        PaperSignalQueueStatus.IGNORED.value,
                        message,
                        observed.isoformat(),
                        signal_id,
                        PaperSignalQueueStatus.PENDING.value,
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        result = self.record(signal_id)
        assert result is not None
        return result

    def expire_prepared_unsubmitted(
        self,
        signal_id: str,
        *,
        observed_at: datetime,
    ) -> PaperSignalQueueRecord:
        observed = normalize_aware_utc(observed_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._required_row(connection, signal_id)
                record = self._record_from_row(row)
                if record.status is not PaperSignalQueueStatus.PREPARED:
                    raise ValueError(
                        f"cannot expire prepared paper signal in {record.status.value}"
                    )
                if observed < record.signal.expires_at:
                    raise ValueError("prepared paper signal has not expired")
                connection.execute(
                    """
                    UPDATE paper_signal_queue
                    SET status = ?, last_error = ?, updated_at = ?
                    WHERE signal_id = ? AND status = ?
                    """,
                    (
                        PaperSignalQueueStatus.EXPIRED.value,
                        "prepared signal expired after authoritative broker absence",
                        observed.isoformat(),
                        signal_id,
                        PaperSignalQueueStatus.PREPARED.value,
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        result = self.record(signal_id)
        assert result is not None
        return result

    def _validate_sell_authority(
        self,
        signal: SignalEnvelopeFamily,
        authority: PaperSellQuantityAuthority | None,
        *,
        decision_cutoff: datetime,
    ) -> None:
        entry_signal_id = _entry_signal_id(signal)
        if authority is None:
            raise ValueError("SELL preparation requires quantity authority")
        if (
            authority.exit_signal_id != signal.signal_id
            or authority.entry_signal_id != entry_signal_id
            or authority.account_id != self.policy.account_id
            or authority.ts_code != signal.candidate_id
            or authority.action != signal.action.name
            or authority.decision_cutoff != decision_cutoff
            or authority.tranche_fraction != _sell_tranche_fraction(signal)
        ):
            raise ValueError("SELL quantity authority does not bind the queued signal")

    def record_error(
        self,
        signal_id: str,
        *,
        error: str,
        observed_at: datetime,
    ) -> PaperSignalQueueRecord:
        message = error.strip()
        if not message:
            raise ValueError("error must not be empty")
        observed = normalize_aware_utc(observed_at)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE paper_signal_queue
                SET last_error = ?, updated_at = ?
                WHERE signal_id = ? AND status IN (?, ?)
                """,
                (
                    message,
                    observed.isoformat(),
                    signal_id,
                    PaperSignalQueueStatus.PENDING.value,
                    PaperSignalQueueStatus.PREPARED.value,
                ),
            )
        result = self.record(signal_id)
        assert result is not None
        return result

    def record(self, signal_id: str) -> PaperSignalQueueRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM paper_signal_queue WHERE signal_id = ?",
                (signal_id,),
            ).fetchone()
        return None if row is None else self._record_from_row(row)

    @staticmethod
    def _required_row(connection: sqlite3.Connection, signal_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM paper_signal_queue WHERE signal_id = ?",
            (signal_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown paper signal: {signal_id}")
        return row

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> PaperSignalQueueRecord:
        signal = parse_stored_signal(
            signal_id=str(row["signal_id"]),
            payload_hash=str(row["signal_hash"]),
            payload_json=str(row["signal_json"]),
            payload_size=int(row["signal_size"]),
        )
        return PaperSignalQueueRecord(
            signal=signal,
            status=PaperSignalQueueStatus(row["status"]),
            due_at=datetime.fromisoformat(row["due_at"]),
            received_at=datetime.fromisoformat(row["received_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            last_error=row["last_error"],
            execution_id=row["execution_id"],
            quote=(
                PaperQuoteSnapshot.model_validate_json(row["quote_json"])
                if row["quote_json"] is not None
                else None
            ),
            intent=(
                PaperOrderIntent.model_validate_json(row["intent_json"])
                if row["intent_json"] is not None
                else None
            ),
            order=(
                PaperOrder.model_validate_json(row["order_json"])
                if row["order_json"] is not None
                else None
            ),
        )


def run_paper_signal_batch(
    queue: PaperSignalQueueStore,
    broker: PaperBrokerStore,
    *,
    now: datetime,
    trade_date: date,
    quote_resolver: QuoteResolver,
    limit: int,
) -> PaperSignalRunSummary:
    observed = normalize_aware_utc(now)
    due = queue.due_records(now=observed, limit=limit)
    completed = 0
    failed = 0
    for record in due:
        try:
            broker.require_trusted_ledger()
            prepared = record
            if record.status is PaperSignalQueueStatus.PENDING:
                sell_authority = _sell_quantity_authority(
                    broker,
                    record.signal,
                    decision_cutoff=observed,
                    trade_date=trade_date,
                )
                quote = quote_resolver(record.signal, observed)
                prepared = queue.prepare(
                    record.signal.signal_id,
                    quote=quote,
                    prepared_at=observed,
                    sell_quantity_authority=sell_authority,
                )
            elif record.status is PaperSignalQueueStatus.PREPARED:
                if record.intent is None or record.execution_id is None:
                    raise RuntimeError(
                        "prepared paper signal lacks immutable intent or execution identity"
                    )
                execution_order = broker.order_for_execution(record.execution_id)
                intent_order = broker.order_for_intent(record.intent.intent_id)
                if (execution_order is None) != (intent_order is None):
                    raise RuntimeError(
                        "prepared execution identity and intent do not resolve together"
                    )
                if (
                    execution_order is not None
                    and intent_order is not None
                    and execution_order.order_id != intent_order.order_id
                ):
                    raise RuntimeError(
                        "prepared execution identity and intent resolve to different orders"
                    )
                existing_order = execution_order or intent_order
                if existing_order is not None:
                    broker.reconcile()
                    queue.complete(
                        record.signal.signal_id,
                        order=existing_order,
                        completed_at=observed,
                    )
                    completed += 1
                    continue
                if observed >= record.signal.expires_at:
                    queue.expire_prepared_unsubmitted(
                        record.signal.signal_id,
                        observed_at=observed,
                    )
                    continue
                sell_authority = _sell_quantity_authority(
                    broker,
                    record.signal,
                    decision_cutoff=observed,
                    trade_date=trade_date,
                )
                quote = quote_resolver(record.signal, observed)
                prepared = queue.refresh_prepared(
                    record.signal.signal_id,
                    quote=quote,
                    prepared_at=observed,
                    sell_quantity_authority=sell_authority,
                )
            if prepared.quote is None or prepared.intent is None:
                raise RuntimeError("prepared paper signal lacks immutable execution evidence")
            if prepared.execution_id is None:
                raise RuntimeError("prepared paper signal lacks immutable execution identity")
            order = broker.submit_intent(
                prepared.intent,
                execution_id=prepared.execution_id,
                decision_time=observed,
                trade_date=trade_date,
                quote=prepared.quote.context,
            )
            queue.complete(
                prepared.signal.signal_id,
                order=order,
                completed_at=observed,
            )
            completed += 1
        except NoExecutableSellQuantityError as error:
            if record.status is PaperSignalQueueStatus.PENDING:
                queue.ignore(
                    record.signal.signal_id,
                    reason=str(error),
                    observed_at=observed,
                )
                continue
            queue.record_error(
                record.signal.signal_id,
                error=_error_text(error),
                observed_at=observed,
            )
            failed += 1
        except Exception as error:
            queue.record_error(
                record.signal.signal_id,
                error=_error_text(error),
                observed_at=observed,
            )
            failed += 1
    return PaperSignalRunSummary(
        observed_at=observed,
        due_count=len(due),
        completed_count=completed,
        failed_count=failed,
    )


def _sell_quantity_authority(
    broker: PaperBrokerStore,
    signal: SignalEnvelopeFamily,
    *,
    decision_cutoff: datetime,
    trade_date: date,
) -> PaperSellQuantityAuthority | None:
    if signal.action is SignalAction.B_INTENT:
        return None
    entry_signal_id = _entry_signal_id(signal)
    assert entry_signal_id is not None
    return broker.sell_quantity_authority(
        exit_signal_id=str(signal.signal_id),
        entry_signal_id=entry_signal_id,
        ts_code=signal.candidate_id,
        action=signal.action.name,
        tranche_fraction=_sell_tranche_fraction(signal),
        decision_cutoff=decision_cutoff,
        trade_date=trade_date,
    )


__all__ = [
    "PaperQuoteSnapshot",
    "PaperSignalPolicy",
    "PaperSignalQueueRecord",
    "PaperSignalQueueStatus",
    "PaperSignalQueueStore",
    "PaperSignalRunSummary",
    "QuoteResolver",
    "run_paper_signal_batch",
]
