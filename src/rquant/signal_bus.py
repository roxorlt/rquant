"""Durable single-writer signal routing and notification outbox state."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

from pydantic import Field, StringConstraints

from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    OutboxAttempt,
    OutboxRecord,
    OutboxStatus,
    RouterDisposition,
    RouterReceipt,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.signal_contracts import SignalEnvelope

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class SignalBusLeaseError(RuntimeError):
    """A stale or unrelated worker attempted to finish a delivery lease."""


class QuarantinedSignal(RuntimeContractModel):
    signal_id: Sha256
    payload_hash: Sha256
    payload_json: str = Field(min_length=1)
    received_at: AwareUtcDatetime
    reason: str = Field(min_length=1)


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


def _retry_policy_fingerprint(
    retry_base_delay: timedelta,
    retry_max_delay: timedelta,
    max_attempts: int,
) -> str:
    payload = {
        "max_attempts": max_attempts,
        "retry_base_delay_microseconds": int(
            retry_base_delay / timedelta(microseconds=1)
        ),
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

                CREATE INDEX IF NOT EXISTS idx_delivery_outbox_due
                ON delivery_outbox(status, next_attempt_at, global_sequence, created_at);

                CREATE INDEX IF NOT EXISTS idx_delivery_outbox_signal
                ON delivery_outbox(signal_id, recipient_id, channel);

                CREATE TABLE IF NOT EXISTS signal_bus_metadata (
                    metadata_key TEXT PRIMARY KEY,
                    metadata_value TEXT NOT NULL
                );
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO signal_bus_metadata(metadata_key, metadata_value)
                VALUES ('retry_policy_fingerprint', ?)
                """,
                (self.retry_policy_fingerprint,),
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
        received = _normalize_time(received_at or datetime.now(UTC))
        signal_id = signal.signal_id
        if signal_id is None:
            raise ValueError("signal_id must be materialized before ingest")
        payload = _signal_payload(signal)
        content_hash = _payload_hash(payload)

        with self._write_transaction() as connection:
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
                    return RouterReceipt(
                        signal_id=signal_id,
                        disposition=RouterDisposition.DUPLICATE,
                        global_sequence=existing["global_sequence"],
                        received_at=received,
                    )
                reason = "signal_id already exists with different canonical payload"
                connection.execute(
                    """
                    INSERT OR IGNORE INTO signal_quarantine(
                        signal_id, payload_hash, payload_json, received_at, reason
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (signal_id, content_hash, payload, _encode_time(received), reason),
                )
                self._before_commit(connection)
                return RouterReceipt(
                    signal_id=signal_id,
                    disposition=RouterDisposition.QUARANTINED,
                    reason=reason,
                    received_at=received,
                )

            # Reject a forged new identity. Existing ids are handled above so their
            # conflicting evidence can still be retained in quarantine.
            SignalEnvelope.model_validate(signal.model_dump(mode="python"))
            cursor = connection.execute(
                """
                INSERT INTO signal_envelope(
                    signal_id, payload_hash, payload_json, received_at
                ) VALUES (?, ?, ?, ?)
                """,
                (signal_id, content_hash, payload, _encode_time(received)),
            )
            sequence = int(cursor.lastrowid)
            self._before_commit(connection)
            return RouterReceipt(
                signal_id=signal_id,
                disposition=RouterDisposition.ACCEPTED,
                global_sequence=sequence,
                received_at=received,
            )

    def signal(self, identifier: int | str) -> SignalEnvelope | None:
        payload = self.signal_payload(identifier)
        if payload is None:
            return None
        return SignalEnvelope.model_validate_json(payload)

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
        unique_targets = sorted(
            {(target.recipient_id, target.channel): target for target in targets}.values(),
            key=lambda target: (target.recipient_id, target.channel.value),
        )
        if not unique_targets:
            return ()

        with self._write_transaction() as connection:
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
            signal = SignalEnvelope.model_validate_json(signal_row["payload_json"])
            if routed_at < signal.available_at:
                raise ValueError("signal cannot be routed before available_at")
            expired = routed_at >= signal.expires_at
            changed = False
            for target in unique_targets:
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
            if changed:
                self._before_commit(connection)
            rows = self._select_outbox_rows(
                connection,
                signal_id=signal_id,
                target_keys={
                    (target.recipient_id, target.channel.value) for target in unique_targets
                },
            )
            return tuple(self._outbox_from_row(row) for row in rows)

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
        if completed_at > lease_until:
            raise SignalBusLeaseError("delivery lease has expired")

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


def _require_time(value: str | None) -> datetime:
    decoded = _decode_time(value)
    if decoded is None:
        raise ValueError("stored datetime is unexpectedly NULL")
    return decoded
