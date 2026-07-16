"""Persistent cross-process notification suppression."""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import IO, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class NotificationLease(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_key: str
    claim_token: str
    backend: Literal["file", "sqlite"]


class _GateRecord(BaseModel):
    event_key: str
    claim_token: str
    state: Literal["pending", "sent"]
    claimed_at: datetime
    blocked_until: datetime
    suppressed_count: int = Field(default=0, ge=0)
    last_suppressed_at: datetime | None = None


def _normalize_time(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("notification gate timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _encode_time(value: datetime) -> str:
    return _normalize_time(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decode_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def error_event_key(component: str, exc: BaseException) -> str:
    """Build a stable key while removing request IDs and changing timestamps."""
    message = str(exc).strip().lower()
    message = re.sub(r"\b[0-9a-f]{16,}\b", "<id>", message)
    message = re.sub(r"\b\d{4}-\d{2}-\d{2}[t ][0-9:.+z-]+\b", "<time>", message)
    canonical = f"{component.strip().lower()}|{type(exc).__name__}|{message}"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"error:{digest}"


class NotificationGate:
    """Reserve a short pending lease, then start cooldown after delivery.

    Per-event files and ``flock`` are the authoritative cross-process gate. SQLite
    mirrors the same state for inspection and becomes the fallback if the file gate
    is unavailable. If both stores fail, callers must fail closed.
    """

    def __init__(
        self,
        path: Path | str,
        *,
        busy_timeout_ms: int = 5_000,
        pending_lease_seconds: int = 60,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        if pending_lease_seconds < 1:
            raise ValueError("pending_lease_seconds must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.pending_lease_seconds = pending_lease_seconds
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fallback_dir = Path(f"{self.path}.gate")
        try:
            self.fallback_dir.mkdir(parents=True, exist_ok=True)
            self._file_ready = True
        except OSError:
            self._file_ready = False
        try:
            self._initialize()
            self._sqlite_ready = True
        except (OSError, sqlite3.Error):
            self._sqlite_ready = False

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS notification_gate (
                    event_key TEXT PRIMARY KEY,
                    claim_token TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'sent',
                    claimed_at TEXT NOT NULL,
                    blocked_until TEXT NOT NULL,
                    suppressed_count INTEGER NOT NULL DEFAULT 0,
                    last_suppressed_at TEXT
                )
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(notification_gate)").fetchall()
            }
            if "state" not in columns:
                connection.execute(
                    "ALTER TABLE notification_gate ADD COLUMN state TEXT NOT NULL DEFAULT 'sent'"
                )
        finally:
            connection.close()

    def _file_path(self, event_key: str) -> Path:
        digest = hashlib.sha256(event_key.encode("utf-8")).hexdigest()
        return self.fallback_dir / f"{digest}.json"

    @contextmanager
    def _locked_file(self, event_key: str) -> Iterator[IO[str]]:
        path = self._file_path(event_key)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        with os.fdopen(fd, "r+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield handle

    @staticmethod
    def _read_file(handle: IO[str], event_key: str) -> _GateRecord | None:
        handle.seek(0)
        raw = handle.read().strip()
        if not raw or raw == "{}":
            return None
        record = _GateRecord.model_validate_json(raw)
        if record.event_key != event_key:
            raise ValueError("notification gate hash collision or corrupt state")
        return record

    @staticmethod
    def _write_file(handle: IO[str], record: _GateRecord | None) -> None:
        handle.seek(0)
        handle.truncate()
        handle.write("{}" if record is None else record.model_dump_json())
        handle.flush()
        os.fsync(handle.fileno())

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> _GateRecord:
        return _GateRecord(
            event_key=row["event_key"],
            claim_token=row["claim_token"],
            state=row["state"],
            claimed_at=_decode_time(row["claimed_at"]),
            blocked_until=_decode_time(row["blocked_until"]),
            suppressed_count=row["suppressed_count"],
            last_suppressed_at=(
                _decode_time(row["last_suppressed_at"]) if row["last_suppressed_at"] else None
            ),
        )

    @staticmethod
    def _upsert_sqlite(connection: sqlite3.Connection, record: _GateRecord) -> None:
        connection.execute(
            """
            INSERT INTO notification_gate (
                event_key, claim_token, state, claimed_at, blocked_until,
                suppressed_count, last_suppressed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_key) DO UPDATE SET
                claim_token = excluded.claim_token,
                state = excluded.state,
                claimed_at = excluded.claimed_at,
                blocked_until = excluded.blocked_until,
                suppressed_count = excluded.suppressed_count,
                last_suppressed_at = excluded.last_suppressed_at
            """,
            [
                record.event_key,
                record.claim_token,
                record.state,
                _encode_time(record.claimed_at),
                _encode_time(record.blocked_until),
                record.suppressed_count,
                _encode_time(record.last_suppressed_at) if record.last_suppressed_at else None,
            ],
        )

    def _mirror_sqlite(self, record: _GateRecord) -> None:
        if not self._sqlite_ready:
            return
        try:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._upsert_sqlite(connection, record)
                connection.execute("COMMIT")
            finally:
                connection.close()
        except (OSError, sqlite3.Error):
            self._sqlite_ready = False

    def _claim_file(
        self,
        event_key: str,
        now: datetime,
    ) -> tuple[NotificationLease | None, _GateRecord]:
        with self._locked_file(event_key) as handle:
            existing = self._read_file(handle, event_key)
            if existing is not None and existing.blocked_until > now:
                record = existing.model_copy(
                    update={
                        "suppressed_count": existing.suppressed_count + 1,
                        "last_suppressed_at": now,
                    }
                )
                self._write_file(handle, record)
                return None, record
            token = uuid4().hex
            record = _GateRecord(
                event_key=event_key,
                claim_token=token,
                state="pending",
                claimed_at=now,
                blocked_until=now + timedelta(seconds=self.pending_lease_seconds),
            )
            self._write_file(handle, record)
            return NotificationLease(
                event_key=event_key,
                claim_token=token,
                backend="file",
            ), record

    def _claim_sqlite(
        self,
        event_key: str,
        now: datetime,
    ) -> NotificationLease | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM notification_gate WHERE event_key = ?",
                [event_key],
            ).fetchone()
            if row is not None:
                existing = self._record_from_row(row)
                if existing.blocked_until > now:
                    record = existing.model_copy(
                        update={
                            "suppressed_count": existing.suppressed_count + 1,
                            "last_suppressed_at": now,
                        }
                    )
                    self._upsert_sqlite(connection, record)
                    connection.execute("COMMIT")
                    return None
            token = uuid4().hex
            record = _GateRecord(
                event_key=event_key,
                claim_token=token,
                state="pending",
                claimed_at=now,
                blocked_until=now + timedelta(seconds=self.pending_lease_seconds),
            )
            self._upsert_sqlite(connection, record)
            connection.execute("COMMIT")
            return NotificationLease(
                event_key=event_key,
                claim_token=token,
                backend="sqlite",
            )
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def claim(
        self,
        event_key: str,
        cooldown_seconds: int,
        *,
        now: datetime | None = None,
    ) -> NotificationLease | None:
        if not event_key.strip():
            raise ValueError("event_key must not be empty")
        if cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must not be negative")
        claimed_at = _normalize_time(now or datetime.now(UTC))
        if self._file_ready:
            try:
                lease, record = self._claim_file(event_key, claimed_at)
                self._mirror_sqlite(record)
                return lease
            except (OSError, ValueError):
                self._file_ready = False
        if self._sqlite_ready:
            try:
                return self._claim_sqlite(event_key, claimed_at)
            except (OSError, sqlite3.Error):
                self._sqlite_ready = False
        raise OSError("notification file gate and SQLite gate are both unavailable")

    def complete(
        self,
        lease: NotificationLease,
        cooldown_seconds: int,
        *,
        now: datetime | None = None,
    ) -> None:
        if cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must not be negative")
        completed_at = _normalize_time(now or datetime.now(UTC))
        if lease.backend == "file" and self._file_ready:
            try:
                with self._locked_file(lease.event_key) as handle:
                    record = self._read_file(handle, lease.event_key)
                    if record is None or record.claim_token != lease.claim_token:
                        return
                    record = record.model_copy(
                        update={
                            "state": "sent",
                            "blocked_until": completed_at + timedelta(seconds=cooldown_seconds),
                        }
                    )
                    self._write_file(handle, record)
                self._mirror_sqlite(record)
                return
            except (OSError, ValueError):
                self._file_ready = False
        if self._sqlite_ready:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    """
                    UPDATE notification_gate
                    SET state = 'sent', blocked_until = ?
                    WHERE event_key = ? AND claim_token = ?
                    """,
                    [
                        _encode_time(completed_at + timedelta(seconds=cooldown_seconds)),
                        lease.event_key,
                        lease.claim_token,
                    ],
                )
                connection.execute("COMMIT")
                return
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()
        raise OSError("notification lease could not be completed")

    def release(self, lease: NotificationLease) -> None:
        """Release only the caller's current claim after total delivery failure."""
        released = False
        if lease.backend == "file" and self._file_ready:
            try:
                with self._locked_file(lease.event_key) as handle:
                    record = self._read_file(handle, lease.event_key)
                    if record is not None and record.claim_token == lease.claim_token:
                        self._write_file(handle, None)
                        released = True
            except (OSError, ValueError):
                self._file_ready = False
        if self._sqlite_ready:
            try:
                connection = self._connect()
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "DELETE FROM notification_gate WHERE event_key = ? AND claim_token = ?",
                        [lease.event_key, lease.claim_token],
                    )
                    connection.execute("COMMIT")
                    released = True
                finally:
                    connection.close()
            except (OSError, sqlite3.Error):
                self._sqlite_ready = False
        if not released and not self._file_ready and not self._sqlite_ready:
            raise OSError("notification lease could not be released")

    def clear(self, event_key: str) -> None:
        """Close an incident after recovery so a later failure can alert again."""
        if not event_key.strip():
            raise ValueError("event_key must not be empty")
        cleared = False
        if self._file_ready:
            try:
                with self._locked_file(event_key) as handle:
                    self._write_file(handle, None)
                cleared = True
            except OSError:
                self._file_ready = False
        if self._sqlite_ready:
            try:
                connection = self._connect()
                try:
                    connection.execute(
                        "DELETE FROM notification_gate WHERE event_key = ?",
                        [event_key],
                    )
                    cleared = True
                finally:
                    connection.close()
            except (OSError, sqlite3.Error):
                self._sqlite_ready = False
        if not cleared:
            raise OSError("notification incident could not be cleared")
