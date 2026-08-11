"""Transactional source-quota reservations shared by live and research workers."""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import socket
import sqlite3
import subprocess
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from rquant.resource_admission import SourceQuotaLease
from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc


class SourceQuotaConflictError(RuntimeError):
    pass


class SourceQuotaExhaustedError(RuntimeError):
    pass


class SourceQuotaAttemptOutcome(StrEnum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILURE = "failure"
    UNKNOWN = "unknown"


class SourceQuotaAttempt(RuntimeContractModel):
    attempt_id: str = Field(min_length=1)
    source: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    lease_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    units: int = Field(strict=True, gt=0)
    prepared_at: datetime
    dispatched_at: datetime | None = None
    outcome: SourceQuotaAttemptOutcome = SourceQuotaAttemptOutcome.PENDING
    committed_at: datetime | None = None
    boot_id: str = Field(min_length=1)
    last_monotonic_ns: int = Field(strict=True, ge=0)
    lifecycle_sequence: int = Field(strict=True, ge=1)
    clock_rollback_count: int = Field(strict=True, ge=0)

    @model_validator(mode="after")
    def validate_lifecycle(self) -> SourceQuotaAttempt:
        if self.dispatched_at is not None and self.dispatched_at < self.prepared_at:
            raise ValueError("attempt dispatch precedes preparation")
        if self.outcome is SourceQuotaAttemptOutcome.PENDING:
            if self.committed_at is not None:
                raise ValueError("pending attempt cannot be committed")
        elif self.committed_at is None:
            raise ValueError("completed attempt requires committed_at")
        elif self.committed_at < self.prepared_at:
            raise ValueError("attempt completion precedes preparation")
        elif self.dispatched_at is not None and self.committed_at < self.dispatched_at:
            raise ValueError("attempt completion precedes dispatch")
        return self


def _iso(value: datetime) -> str:
    return normalize_aware_utc(value).isoformat(timespec="microseconds")


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SourceQuotaConflictError("stored quota timestamp is naive")
    return parsed.astimezone(UTC)


@lru_cache(maxsize=1)
def _current_boot_id() -> str:
    linux_boot_id = Path("/proc/sys/kernel/random/boot_id")
    try:
        value = linux_boot_id.read_text(encoding="ascii").strip()
    except OSError:
        value = ""
    if value:
        return value
    try:
        result = subprocess.run(
            ("/usr/sbin/sysctl", "-n", "kern.boottime"),
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        match = re.search(r"sec\s*=\s*(\d+)", result.stdout)
        if match is not None:
            return f"darwin:{match.group(1)}"
    except (OSError, subprocess.SubprocessError):
        pass
    boot_epoch = round(time.time() - time.monotonic())
    fallback = f"{socket.gethostname()}:{boot_epoch}".encode()
    return f"fallback:{hashlib.sha256(fallback).hexdigest()}"


class SourceQuotaStore:
    """SQLite single-writer ledger for bounded, expiring source reservations."""

    def __init__(
        self,
        path: Path,
        *,
        busy_timeout_ms: int = 5_000,
        boot_id: str | None = None,
        monotonic_ns: Callable[[], int] | None = None,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.boot_id = (boot_id or _current_boot_id()).strip()
        if not self.boot_id:
            raise ValueError("boot_id must be nonempty")
        self._monotonic_ns = monotonic_ns or time.monotonic_ns
        self.path.parent.mkdir(parents=True, exist_ok=True)
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

    def _initialize(self) -> None:
        try:
            with self._migration_lock(), self._connect() as connection:
                connection.execute("BEGIN EXCLUSIVE")
                try:
                    self._initialize_exclusive(connection)
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                raise SourceQuotaConflictError(
                    f"quota ledger migration lock timed out after {self.busy_timeout_ms}ms"
                ) from exc
            raise

    @contextmanager
    def _migration_lock(self) -> Iterator[None]:
        lock_path = self.path.with_name(f".{self.path.name}.migration.lock")
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        deadline = time.monotonic() + (self.busy_timeout_ms / 1_000)
        try:
            os.fchmod(descriptor, 0o600)
            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise SourceQuotaConflictError(
                            "quota ledger migration process lock timed out"
                        ) from exc
                    time.sleep(0.01)
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    @staticmethod
    def _initialize_exclusive(connection: sqlite3.Connection) -> None:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version > 3:
            raise SourceQuotaConflictError("quota ledger schema is newer than this runtime")
        statements = (
            """
                CREATE TABLE IF NOT EXISTS quota_window (
                    source TEXT NOT NULL,
                    window_id TEXT NOT NULL,
                    starts_at TEXT NOT NULL,
                    resets_at TEXT NOT NULL,
                    total_units INTEGER NOT NULL CHECK(total_units > 0),
                    PRIMARY KEY(source, window_id)
                )
            """,
            """
                CREATE TABLE IF NOT EXISTS quota_lease (
                    lease_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    window_id TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    units INTEGER NOT NULL CHECK(units > 0),
                    used_units INTEGER NOT NULL DEFAULT 0 CHECK(used_units >= 0),
                    granted_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    quota_reset_at TEXT NOT NULL,
                    released_at TEXT,
                    UNIQUE(source, window_id, owner),
                    FOREIGN KEY(source, window_id)
                        REFERENCES quota_window(source, window_id)
                )
            """,
            """
                CREATE TABLE IF NOT EXISTS quota_usage (
                    usage_id TEXT PRIMARY KEY,
                    lease_id TEXT NOT NULL REFERENCES quota_lease(lease_id),
                    units INTEGER NOT NULL CHECK(units > 0),
                    consumed_at TEXT NOT NULL
                )
            """,
            """
                CREATE TABLE IF NOT EXISTS quota_attempt (
                    attempt_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    lease_id TEXT NOT NULL UNIQUE REFERENCES quota_lease(lease_id),
                    units INTEGER NOT NULL CHECK(units > 0),
                    prepared_at TEXT NOT NULL,
                    dispatched_at TEXT,
                    outcome TEXT NOT NULL CHECK(
                        outcome IN ('pending', 'success', 'failure', 'unknown')
                    ),
                    committed_at TEXT,
                    boot_id TEXT NOT NULL DEFAULT 'legacy-unknown',
                    last_monotonic_ns INTEGER NOT NULL DEFAULT 0,
                    lifecycle_sequence INTEGER NOT NULL DEFAULT 1,
                    clock_rollback_count INTEGER NOT NULL DEFAULT 0
                )
            """,
            """
                CREATE INDEX IF NOT EXISTS quota_attempt_pending_lease_idx
                ON quota_attempt(lease_id) WHERE outcome = 'pending'
            """,
            """
                CREATE TABLE IF NOT EXISTS quota_transport_attempt (
                    attempt_id TEXT PRIMARY KEY
                        REFERENCES quota_attempt(attempt_id),
                    source TEXT NOT NULL,
                    logical_request_id TEXT NOT NULL,
                    api_name TEXT NOT NULL,
                    call_ordinal INTEGER NOT NULL CHECK(call_ordinal > 0),
                    UNIQUE(source, logical_request_id, call_ordinal)
                )
            """,
            """
                CREATE INDEX IF NOT EXISTS quota_transport_request_idx
                ON quota_transport_attempt(source, logical_request_id, call_ordinal)
            """,
            """
                CREATE TABLE IF NOT EXISTS quota_source_clock (
                    source TEXT PRIMARY KEY,
                    window_kind TEXT NOT NULL CHECK(window_kind IN ('day', 'minute')),
                    trusted_at TEXT NOT NULL,
                    rollback_count INTEGER NOT NULL DEFAULT 0 CHECK(rollback_count >= 0)
                )
            """,
        )
        for statement in statements:
            connection.execute(statement)

        # Recheck immediately before every additive ALTER. This keeps retries safe even
        # when another runtime completed part of a legacy migration before terminating.
        migrations = {
            "boot_id": "TEXT NOT NULL DEFAULT 'legacy-unknown'",
            "last_monotonic_ns": "INTEGER NOT NULL DEFAULT 0",
            "lifecycle_sequence": "INTEGER NOT NULL DEFAULT 1",
            "clock_rollback_count": "INTEGER NOT NULL DEFAULT 0",
        }
        for column, declaration in migrations.items():
            current_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(quota_attempt)")
            }
            if column not in current_columns:
                connection.execute(f"ALTER TABLE quota_attempt ADD COLUMN {column} {declaration}")

        required = {
            "quota_window": {"source", "window_id", "starts_at", "resets_at", "total_units"},
            "quota_lease": {
                "lease_id",
                "source",
                "window_id",
                "owner",
                "units",
                "used_units",
                "granted_at",
                "expires_at",
                "quota_reset_at",
                "released_at",
            },
            "quota_usage": {"usage_id", "lease_id", "units", "consumed_at"},
            "quota_attempt": {
                "attempt_id",
                "source",
                "owner",
                "lease_id",
                "units",
                "prepared_at",
                "dispatched_at",
                "outcome",
                "committed_at",
                "boot_id",
                "last_monotonic_ns",
                "lifecycle_sequence",
                "clock_rollback_count",
            },
            "quota_transport_attempt": {
                "attempt_id",
                "source",
                "logical_request_id",
                "api_name",
                "call_ordinal",
            },
            "quota_source_clock": {
                "source",
                "window_kind",
                "trusted_at",
                "rollback_count",
            },
        }
        for table, columns in required.items():
            actual = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
            if not columns <= actual:
                raise SourceQuotaConflictError("quota ledger schema migration is incomplete")
        connection.execute("PRAGMA user_version = 3")

    def declare_window(
        self,
        *,
        source: str,
        window_id: str,
        starts_at: datetime,
        resets_at: datetime,
        total_units: int,
    ) -> None:
        source = source.strip()
        window_id = window_id.strip()
        starts = normalize_aware_utc(starts_at)
        resets = normalize_aware_utc(resets_at)
        if not source or not window_id:
            raise ValueError("source and window_id must be nonempty")
        if resets <= starts:
            raise ValueError("resets_at must follow starts_at")
        if total_units < 1:
            raise ValueError("total_units must be positive")
        payload = (source, window_id, _iso(starts), _iso(resets), total_units)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    """
                    SELECT source, window_id, starts_at, resets_at, total_units
                    FROM quota_window WHERE source = ? AND window_id = ?
                    """,
                    (source, window_id),
                ).fetchone()
                if existing is not None:
                    if tuple(existing) != payload:
                        raise SourceQuotaConflictError("quota window contract conflicts")
                    connection.rollback()
                    return
                overlap = connection.execute(
                    """
                    SELECT window_id FROM quota_window
                    WHERE source = ? AND starts_at < ? AND resets_at > ?
                    LIMIT 1
                    """,
                    (source, _iso(resets), _iso(starts)),
                ).fetchone()
                if overlap is not None:
                    raise SourceQuotaConflictError(f"quota window overlaps {overlap['window_id']}")
                connection.execute(
                    """
                    INSERT INTO quota_window(
                        source, window_id, starts_at, resets_at, total_units
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _active_window(
        connection: sqlite3.Connection,
        source: str,
        now: datetime,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT * FROM quota_window
            WHERE source = ? AND starts_at <= ? AND resets_at > ?
            ORDER BY starts_at DESC LIMIT 1
            """,
            (source, _iso(now), _iso(now)),
        ).fetchone()
        if row is None:
            raise SourceQuotaExhaustedError(f"no active window for source {source}")
        return row

    @staticmethod
    def _remaining_in_window(
        connection: sqlite3.Connection,
        window: sqlite3.Row,
        now: datetime,
    ) -> int:
        row = connection.execute(
            """
            SELECT
                COALESCE(SUM(used_units), 0) AS consumed,
                COALESCE(SUM(
                    CASE
                        WHEN released_at IS NULL AND (
                            expires_at > ? OR EXISTS(
                                SELECT 1 FROM quota_attempt
                                WHERE quota_attempt.lease_id = quota_lease.lease_id
                                AND quota_attempt.outcome = 'pending'
                            )
                        )
                        THEN units - used_units
                        ELSE 0
                    END
                ), 0) AS reserved
            FROM quota_lease
            WHERE source = ? AND window_id = ?
            """,
            (_iso(now), window["source"], window["window_id"]),
        ).fetchone()
        return int(window["total_units"]) - int(row["consumed"]) - int(row["reserved"])

    def remaining(self, source: str, *, now: datetime) -> int:
        observed = normalize_aware_utc(now)
        with self._connect() as connection:
            window = self._active_window(connection, source, observed)
            return self._remaining_in_window(connection, window, observed)

    def acquire(
        self,
        *,
        source: str,
        owner: str,
        units: int,
        now: datetime,
        expires_at: datetime,
    ) -> SourceQuotaLease:
        observed = normalize_aware_utc(now)
        expires = normalize_aware_utc(expires_at)
        source = source.strip()
        owner = owner.strip()
        if not source or not owner:
            raise ValueError("source and owner must be nonempty")
        if units < 1:
            raise ValueError("units must be positive")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                window = self._active_window(connection, source, observed)
                reset = _parse(window["resets_at"])
                if expires <= observed or expires > reset:
                    raise ValueError("expires_at must be after now and no later than reset")
                existing = connection.execute(
                    """
                    SELECT * FROM quota_lease
                    WHERE source = ? AND window_id = ? AND owner = ?
                    """,
                    (source, window["window_id"], owner),
                ).fetchone()
                if existing is not None:
                    if int(existing["units"]) != units or _parse(existing["expires_at"]) != expires:
                        raise SourceQuotaConflictError("quota owner retry conflicts")
                    connection.rollback()
                    return self._lease_from_row(existing)
                remaining = self._remaining_in_window(connection, window, observed)
                if remaining < units:
                    raise SourceQuotaExhaustedError(
                        f"quota exhausted: requested={units}, remaining={remaining}"
                    )
                lease = SourceQuotaLease(
                    source=source,
                    owner=owner,
                    units=units,
                    granted_at=observed,
                    expires_at=expires,
                    quota_reset_at=reset,
                )
                connection.execute(
                    """
                    INSERT INTO quota_lease(
                        lease_id, source, window_id, owner, units, used_units,
                        granted_at, expires_at, quota_reset_at, released_at
                    ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, NULL)
                    """,
                    (
                        lease.lease_id,
                        lease.source,
                        window["window_id"],
                        lease.owner,
                        lease.units,
                        _iso(lease.granted_at),
                        _iso(lease.expires_at),
                        _iso(lease.quota_reset_at),
                    ),
                )
                connection.commit()
                return lease
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _attempt_from_row(row: sqlite3.Row) -> SourceQuotaAttempt:
        return SourceQuotaAttempt(
            attempt_id=row["attempt_id"],
            source=row["source"],
            owner=row["owner"],
            lease_id=row["lease_id"],
            units=row["units"],
            prepared_at=row["prepared_at"],
            dispatched_at=row["dispatched_at"],
            outcome=row["outcome"],
            committed_at=row["committed_at"],
            boot_id=row["boot_id"],
            last_monotonic_ns=row["last_monotonic_ns"],
            lifecycle_sequence=row["lifecycle_sequence"],
            clock_rollback_count=row["clock_rollback_count"],
        )

    def get_attempt(self, attempt_id: str) -> SourceQuotaAttempt | None:
        identifier = attempt_id.strip()
        if not identifier:
            raise ValueError("attempt_id must be nonempty")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM quota_attempt WHERE attempt_id = ?", (identifier,)
            ).fetchone()
            return None if row is None else self._attempt_from_row(row)

    def list_attempts(self, *, source: str | None = None) -> tuple[SourceQuotaAttempt, ...]:
        with self._connect() as connection:
            if source is None:
                rows = connection.execute(
                    "SELECT * FROM quota_attempt ORDER BY prepared_at, attempt_id"
                ).fetchall()
            else:
                normalized = source.strip()
                if not normalized:
                    raise ValueError("source must be nonempty")
                rows = connection.execute(
                    "SELECT * FROM quota_attempt WHERE source = ? ORDER BY prepared_at, attempt_id",
                    (normalized,),
                ).fetchall()
            return tuple(self._attempt_from_row(row) for row in rows)

    def bind_transport_attempt(
        self,
        *,
        attempt_id: str,
        source: str,
        logical_request_id: str,
        api_name: str,
        call_ordinal: int,
    ) -> None:
        identifier = attempt_id.strip()
        normalized_source = source.strip()
        request_id = logical_request_id.strip()
        normalized_api = api_name.strip()
        if not identifier or not normalized_source or not request_id or not normalized_api:
            raise ValueError(
                "attempt_id, source, logical_request_id, and api_name must be nonempty"
            )
        if type(call_ordinal) is not int or call_ordinal < 1:
            raise ValueError("call_ordinal must be a positive int")
        expected = (
            identifier,
            normalized_source,
            request_id,
            normalized_api,
            call_ordinal,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                attempt = connection.execute(
                    "SELECT source FROM quota_attempt WHERE attempt_id = ?",
                    (identifier,),
                ).fetchone()
                if attempt is None:
                    raise SourceQuotaConflictError("quota transport attempt does not exist")
                if attempt["source"] != normalized_source:
                    raise SourceQuotaConflictError("quota transport attempt source conflicts")
                existing = connection.execute(
                    "SELECT * FROM quota_transport_attempt WHERE attempt_id = ?",
                    (identifier,),
                ).fetchone()
                if existing is not None:
                    if tuple(existing) != expected:
                        raise SourceQuotaConflictError("quota transport attempt binding conflicts")
                    connection.rollback()
                    return
                ordinal = connection.execute(
                    """
                    SELECT * FROM quota_transport_attempt
                    WHERE source = ? AND logical_request_id = ? AND call_ordinal = ?
                    """,
                    (normalized_source, request_id, call_ordinal),
                ).fetchone()
                if ordinal is not None:
                    raise SourceQuotaConflictError("quota transport call ordinal conflicts")
                connection.execute(
                    """
                    INSERT INTO quota_transport_attempt(
                        attempt_id, source, logical_request_id, api_name, call_ordinal
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    expected,
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def list_transport_attempts(
        self,
        *,
        source: str,
        logical_request_id: str,
    ) -> tuple[SourceQuotaAttempt, ...]:
        normalized_source = source.strip()
        request_id = logical_request_id.strip()
        if not normalized_source or not request_id:
            raise ValueError("source and logical_request_id must be nonempty")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT attempt.*
                FROM quota_transport_attempt AS transport
                JOIN quota_attempt AS attempt
                    ON attempt.attempt_id = transport.attempt_id
                WHERE transport.source = ? AND transport.logical_request_id = ?
                ORDER BY transport.call_ordinal
                """,
                (normalized_source, request_id),
            ).fetchall()
            return tuple(self._attempt_from_row(row) for row in rows)

    @staticmethod
    def _quota_window(
        observed_at: datetime,
        *,
        window_kind: Literal["day", "minute"],
    ) -> tuple[str, datetime, datetime]:
        if window_kind == "day":
            starts_at = observed_at.replace(hour=0, minute=0, second=0, microsecond=0)
            resets_at = starts_at + timedelta(days=1)
            return starts_at.strftime("%Y%m%d"), starts_at, resets_at
        starts_at = observed_at.replace(second=0, microsecond=0)
        resets_at = starts_at + timedelta(minutes=1)
        return starts_at.strftime("%Y%m%dT%H%MZ"), starts_at, resets_at

    def begin_transport_dispatch(
        self,
        *,
        source: str,
        owner: str,
        attempt_id: str,
        logical_request_id: str,
        api_name: str,
        call_ordinal: int,
        units: int,
        total_units: int,
        window_kind: Literal["day", "minute"],
        clock: Callable[[], datetime],
    ) -> tuple[SourceQuotaAttempt, bool]:
        normalized_source = source.strip()
        normalized_owner = owner.strip()
        identifier = attempt_id.strip()
        request_id = logical_request_id.strip()
        normalized_api = api_name.strip()
        if not callable(clock):
            raise ValueError("transport dispatch clock must be callable")
        if not all((normalized_source, normalized_owner, identifier, request_id, normalized_api)):
            raise ValueError("transport attempt identifiers must be nonempty")
        if type(call_ordinal) is not int or call_ordinal < 1:
            raise ValueError("call_ordinal must be a positive int")
        if units < 1 or total_units < 1:
            raise ValueError("transport quota units must be positive")
        if window_kind not in {"day", "minute"}:
            raise ValueError("window_kind must be day or minute")
        binding = (
            identifier,
            normalized_source,
            request_id,
            normalized_api,
            call_ordinal,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                observed = normalize_aware_utc(clock())
                monotonic_ns = self._monotonic_ns()
                if monotonic_ns < 0:
                    raise ValueError("monotonic_ns must be nonnegative")
                existing_row = connection.execute(
                    "SELECT * FROM quota_attempt WHERE attempt_id = ?",
                    (identifier,),
                ).fetchone()
                if existing_row is not None:
                    existing = self._attempt_from_row(existing_row)
                    if (existing.source, existing.owner, existing.units) != (
                        normalized_source,
                        normalized_owner,
                        units,
                    ):
                        raise SourceQuotaConflictError("quota attempt retry conflicts")
                    existing_binding = connection.execute(
                        "SELECT * FROM quota_transport_attempt WHERE attempt_id = ?",
                        (identifier,),
                    ).fetchone()
                    if existing_binding is None:
                        connection.execute(
                            """
                            INSERT INTO quota_transport_attempt(
                                attempt_id, source, logical_request_id, api_name, call_ordinal
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            binding,
                        )
                        connection.commit()
                    elif tuple(existing_binding) != binding:
                        raise SourceQuotaConflictError("quota transport attempt binding conflicts")
                    else:
                        connection.rollback()
                    return existing, False

                clock_row = connection.execute(
                    "SELECT * FROM quota_source_clock WHERE source = ?",
                    (normalized_source,),
                ).fetchone()
                rollback = 0
                effective = observed
                if clock_row is None:
                    latest_attempt = connection.execute(
                        """
                        SELECT MAX(COALESCE(dispatched_at, prepared_at)) AS trusted_at
                        FROM quota_attempt WHERE source = ?
                        """,
                        (normalized_source,),
                    ).fetchone()
                    if latest_attempt["trusted_at"] is not None:
                        previous_trusted = _parse(latest_attempt["trusted_at"])
                        if observed < previous_trusted:
                            effective = previous_trusted
                            rollback = 1
                    connection.execute(
                        """
                        INSERT INTO quota_source_clock(
                            source, window_kind, trusted_at, rollback_count
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (normalized_source, window_kind, _iso(effective), rollback),
                    )
                else:
                    if clock_row["window_kind"] != window_kind:
                        raise SourceQuotaConflictError("source quota window kind conflicts")
                    trusted = _parse(clock_row["trusted_at"])
                    if observed < trusted:
                        effective = trusted
                        rollback = 1
                    connection.execute(
                        """
                        UPDATE quota_source_clock
                        SET trusted_at = ?, rollback_count = rollback_count + ?
                        WHERE source = ?
                        """,
                        (_iso(effective), rollback, normalized_source),
                    )

                window_id, starts_at, resets_at = self._quota_window(
                    effective,
                    window_kind=window_kind,
                )
                window_payload = (
                    normalized_source,
                    window_id,
                    _iso(starts_at),
                    _iso(resets_at),
                    total_units,
                )
                window = connection.execute(
                    "SELECT * FROM quota_window WHERE source = ? AND window_id = ?",
                    (normalized_source, window_id),
                ).fetchone()
                if window is None:
                    overlap = connection.execute(
                        """
                        SELECT window_id FROM quota_window
                        WHERE source = ? AND starts_at < ? AND resets_at > ?
                        LIMIT 1
                        """,
                        (normalized_source, _iso(resets_at), _iso(starts_at)),
                    ).fetchone()
                    if overlap is not None:
                        raise SourceQuotaConflictError(
                            f"quota window overlaps {overlap['window_id']}"
                        )
                    connection.execute(
                        """
                        INSERT INTO quota_window(
                            source, window_id, starts_at, resets_at, total_units
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        window_payload,
                    )
                    window = connection.execute(
                        "SELECT * FROM quota_window WHERE source = ? AND window_id = ?",
                        (normalized_source, window_id),
                    ).fetchone()
                elif tuple(window) != window_payload:
                    raise SourceQuotaConflictError("quota window contract conflicts")

                existing_lease = connection.execute(
                    """
                    SELECT lease_id FROM quota_lease
                    WHERE source = ? AND window_id = ? AND owner = ?
                    """,
                    (normalized_source, window_id, normalized_owner),
                ).fetchone()
                if existing_lease is not None:
                    raise SourceQuotaConflictError("quota owner already has an attempt")
                remaining = self._remaining_in_window(connection, window, effective)
                if remaining < units:
                    raise SourceQuotaExhaustedError(
                        f"quota exhausted: requested={units}, remaining={remaining}"
                    )
                lease = SourceQuotaLease(
                    source=normalized_source,
                    owner=normalized_owner,
                    units=units,
                    granted_at=effective,
                    expires_at=resets_at,
                    quota_reset_at=resets_at,
                )
                connection.execute(
                    """
                    INSERT INTO quota_lease(
                        lease_id, source, window_id, owner, units, used_units,
                        granted_at, expires_at, quota_reset_at, released_at
                    ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, NULL)
                    """,
                    (
                        lease.lease_id,
                        normalized_source,
                        window_id,
                        normalized_owner,
                        units,
                        _iso(effective),
                        _iso(resets_at),
                        _iso(resets_at),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO quota_attempt(
                        attempt_id, source, owner, lease_id, units, prepared_at,
                        dispatched_at, outcome, committed_at, boot_id,
                        last_monotonic_ns, lifecycle_sequence, clock_rollback_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?, 2, ?)
                    """,
                    (
                        identifier,
                        normalized_source,
                        normalized_owner,
                        lease.lease_id,
                        units,
                        _iso(effective),
                        _iso(effective),
                        self.boot_id,
                        monotonic_ns,
                        rollback,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO quota_transport_attempt(
                        attempt_id, source, logical_request_id, api_name, call_ordinal
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    binding,
                )
                row = connection.execute(
                    "SELECT * FROM quota_attempt WHERE attempt_id = ?",
                    (identifier,),
                ).fetchone()
                connection.commit()
                return self._attempt_from_row(row), True
            except BaseException:
                connection.rollback()
                raise

    def begin_attempt(
        self,
        *,
        source: str,
        owner: str,
        attempt_id: str,
        units: int,
        now: datetime,
        expires_at: datetime,
    ) -> SourceQuotaAttempt:
        observed = normalize_aware_utc(now)
        monotonic_ns = self._monotonic_ns()
        if monotonic_ns < 0:
            raise ValueError("monotonic_ns must be nonnegative")
        expires = normalize_aware_utc(expires_at)
        source = source.strip()
        owner = owner.strip()
        attempt_id = attempt_id.strip()
        if not source or not owner or not attempt_id:
            raise ValueError("source, owner, and attempt_id must be nonempty")
        if units < 1:
            raise ValueError("units must be positive")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing_attempt = connection.execute(
                    "SELECT * FROM quota_attempt WHERE attempt_id = ?", (attempt_id,)
                ).fetchone()
                if existing_attempt is not None:
                    attempt = self._attempt_from_row(existing_attempt)
                    if (attempt.source, attempt.owner, attempt.units) != (source, owner, units):
                        raise SourceQuotaConflictError("quota attempt retry conflicts")
                    connection.rollback()
                    return attempt
                window = self._active_window(connection, source, observed)
                reset = _parse(window["resets_at"])
                if expires <= observed or expires > reset:
                    raise ValueError("expires_at must be after now and no later than reset")
                existing_lease = connection.execute(
                    """
                    SELECT * FROM quota_lease
                    WHERE source = ? AND window_id = ? AND owner = ?
                    """,
                    (source, window["window_id"], owner),
                ).fetchone()
                if existing_lease is not None:
                    raise SourceQuotaConflictError("quota owner already has an attempt")
                remaining = self._remaining_in_window(connection, window, observed)
                if remaining < units:
                    raise SourceQuotaExhaustedError(
                        f"quota exhausted: requested={units}, remaining={remaining}"
                    )
                lease = SourceQuotaLease(
                    source=source,
                    owner=owner,
                    units=units,
                    granted_at=observed,
                    expires_at=expires,
                    quota_reset_at=reset,
                )
                connection.execute(
                    """
                    INSERT INTO quota_lease(
                        lease_id, source, window_id, owner, units, used_units,
                        granted_at, expires_at, quota_reset_at, released_at
                    ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, NULL)
                    """,
                    (
                        lease.lease_id,
                        source,
                        window["window_id"],
                        owner,
                        units,
                        _iso(observed),
                        _iso(expires),
                        _iso(reset),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO quota_attempt(
                        attempt_id, source, owner, lease_id, units, prepared_at,
                        dispatched_at, outcome, committed_at, boot_id,
                        last_monotonic_ns, lifecycle_sequence, clock_rollback_count
                    ) VALUES (?, ?, ?, ?, ?, ?, NULL, 'pending', NULL, ?, ?, 1, 0)
                    """,
                    (
                        attempt_id,
                        source,
                        owner,
                        lease.lease_id,
                        units,
                        _iso(observed),
                        self.boot_id,
                        monotonic_ns,
                    ),
                )
                connection.commit()
                return SourceQuotaAttempt(
                    attempt_id=attempt_id,
                    source=source,
                    owner=owner,
                    lease_id=lease.lease_id,
                    units=units,
                    prepared_at=observed,
                    boot_id=self.boot_id,
                    last_monotonic_ns=monotonic_ns,
                    lifecycle_sequence=1,
                    clock_rollback_count=0,
                )
            except BaseException:
                connection.rollback()
                raise

    def mark_dispatched(self, attempt_id: str, *, now: datetime) -> SourceQuotaAttempt:
        observed = normalize_aware_utc(now)
        monotonic_ns = self._monotonic_ns()
        if monotonic_ns < 0:
            raise ValueError("monotonic_ns must be nonnegative")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM quota_attempt WHERE attempt_id = ?", (attempt_id,)
                ).fetchone()
                if row is None:
                    raise SourceQuotaConflictError("quota attempt does not exist")
                attempt = self._attempt_from_row(row)
                if attempt.outcome is not SourceQuotaAttemptOutcome.PENDING:
                    raise SourceQuotaConflictError("quota attempt is already completed")
                if attempt.dispatched_at is None:
                    effective = max(observed, attempt.prepared_at)
                    rollback = int(observed < attempt.prepared_at)
                    connection.execute(
                        """
                        UPDATE quota_attempt
                        SET dispatched_at = ?, last_monotonic_ns = ?,
                            lifecycle_sequence = lifecycle_sequence + 1,
                            clock_rollback_count = clock_rollback_count + ?
                        WHERE attempt_id = ?
                        """,
                        (_iso(effective), monotonic_ns, rollback, attempt_id),
                    )
                    row = connection.execute(
                        "SELECT * FROM quota_attempt WHERE attempt_id = ?", (attempt_id,)
                    ).fetchone()
                connection.commit()
                return self._attempt_from_row(row)
            except BaseException:
                connection.rollback()
                raise

    def _complete_attempt(
        self,
        attempt_id: str,
        *,
        outcome: SourceQuotaAttemptOutcome,
        now: datetime,
    ) -> SourceQuotaAttempt:
        if outcome is SourceQuotaAttemptOutcome.PENDING:
            raise ValueError("attempt outcome must be final")
        observed = normalize_aware_utc(now)
        monotonic_ns = self._monotonic_ns()
        if monotonic_ns < 0:
            raise ValueError("monotonic_ns must be nonnegative")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM quota_attempt WHERE attempt_id = ?", (attempt_id,)
                ).fetchone()
                if row is None:
                    raise SourceQuotaConflictError("quota attempt does not exist")
                attempt = self._attempt_from_row(row)
                if attempt.outcome is not SourceQuotaAttemptOutcome.PENDING:
                    if attempt.outcome is not outcome:
                        raise SourceQuotaConflictError("quota attempt completion conflicts")
                    connection.rollback()
                    return attempt
                floor = attempt.dispatched_at or attempt.prepared_at
                effective = max(observed, floor)
                rollback = int(observed < floor)
                lease = connection.execute(
                    "SELECT * FROM quota_lease WHERE lease_id = ?", (attempt.lease_id,)
                ).fetchone()
                if lease is None or lease["released_at"] is not None:
                    raise SourceQuotaConflictError("quota attempt lease is not active")
                if int(lease["used_units"]) + attempt.units > int(lease["units"]):
                    raise SourceQuotaConflictError("attempt consumption exceeds reserved units")
                usage_id = f"attempt:{attempt.attempt_id}"
                connection.execute(
                    "UPDATE quota_lease SET used_units = used_units + ? WHERE lease_id = ?",
                    (attempt.units, attempt.lease_id),
                )
                connection.execute(
                    """
                    INSERT INTO quota_usage(usage_id, lease_id, units, consumed_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (usage_id, attempt.lease_id, attempt.units, _iso(effective)),
                )
                connection.execute(
                    "UPDATE quota_lease SET released_at = ? WHERE lease_id = ?",
                    (_iso(effective), attempt.lease_id),
                )
                connection.execute(
                    """
                    UPDATE quota_attempt
                    SET outcome = ?, committed_at = ?, last_monotonic_ns = ?,
                        lifecycle_sequence = lifecycle_sequence + 1,
                        clock_rollback_count = clock_rollback_count + ?
                    WHERE attempt_id = ?
                    """,
                    (
                        outcome.value,
                        _iso(effective),
                        monotonic_ns,
                        rollback,
                        attempt.attempt_id,
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM quota_attempt WHERE attempt_id = ?", (attempt_id,)
                ).fetchone()
                connection.commit()
                return self._attempt_from_row(row)
            except BaseException:
                connection.rollback()
                raise

    def commit_attempt(
        self,
        attempt_id: str,
        *,
        outcome: SourceQuotaAttemptOutcome,
        now: datetime,
    ) -> SourceQuotaAttempt:
        return self._complete_attempt(attempt_id, outcome=outcome, now=now)

    def recover_attempt(self, attempt_id: str, *, now: datetime) -> SourceQuotaAttempt:
        return self._complete_attempt(
            attempt_id,
            outcome=SourceQuotaAttemptOutcome.UNKNOWN,
            now=now,
        )

    def recover_stale_attempts(
        self,
        *,
        source: str,
        now: datetime,
        min_age: timedelta,
        max_count: int = 100,
    ) -> tuple[SourceQuotaAttempt, ...]:
        normalized_source = source.strip()
        if not normalized_source:
            raise ValueError("source must be nonempty")
        if min_age < timedelta(0):
            raise ValueError("min_age must be nonnegative")
        if max_count < 1:
            raise ValueError("max_count must be positive")
        current_monotonic_ns = self._monotonic_ns()
        if current_monotonic_ns < 0:
            raise ValueError("monotonic_ns must be nonnegative")
        minimum_age_ns = int(min_age.total_seconds() * 1_000_000_000)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM quota_attempt
                WHERE source = ? AND outcome = 'pending'
                ORDER BY prepared_at, attempt_id
                """,
                (normalized_source,),
            ).fetchall()
        eligible: list[str] = []
        for row in rows:
            if row["boot_id"] != self.boot_id:
                eligible.append(row["attempt_id"])
            else:
                last_monotonic_ns = int(row["last_monotonic_ns"])
                if (
                    current_monotonic_ns >= last_monotonic_ns
                    and current_monotonic_ns - last_monotonic_ns >= minimum_age_ns
                ):
                    eligible.append(row["attempt_id"])
            if len(eligible) == max_count:
                break
        recovered: list[SourceQuotaAttempt] = []
        for attempt_id in eligible:
            current = self.get_attempt(attempt_id)
            if current is None or current.outcome is not SourceQuotaAttemptOutcome.PENDING:
                continue
            recovered.append(self.recover_attempt(attempt_id, now=now))
        return tuple(recovered)

    def consume(
        self,
        lease_id: str,
        *,
        usage_id: str,
        units: int,
        now: datetime,
    ) -> None:
        usage_id = usage_id.strip()
        if not usage_id:
            raise ValueError("usage_id must be nonempty")
        if units < 1:
            raise ValueError("units must be positive")
        observed = normalize_aware_utc(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM quota_lease WHERE lease_id = ?",
                    (lease_id,),
                ).fetchone()
                if row is None:
                    raise SourceQuotaConflictError("quota lease does not exist")
                linked_attempt = connection.execute(
                    "SELECT attempt_id FROM quota_attempt WHERE lease_id = ? LIMIT 1",
                    (lease_id,),
                ).fetchone()
                if linked_attempt is not None:
                    raise SourceQuotaConflictError(
                        f"quota attempt lease requires explicit completion "
                        f"{linked_attempt['attempt_id']}"
                    )
                usage = connection.execute(
                    "SELECT lease_id, units FROM quota_usage WHERE usage_id = ?",
                    (usage_id,),
                ).fetchone()
                if usage is not None:
                    if usage["lease_id"] != lease_id or int(usage["units"]) != units:
                        raise SourceQuotaConflictError("usage_id retry conflicts")
                    connection.rollback()
                    return
                if row["released_at"] is not None or _parse(row["expires_at"]) <= observed:
                    raise SourceQuotaConflictError("quota lease is not active")
                if int(row["used_units"]) + units > int(row["units"]):
                    raise SourceQuotaConflictError("consumption exceeds reserved units")
                connection.execute(
                    "UPDATE quota_lease SET used_units = used_units + ? WHERE lease_id = ?",
                    (units, lease_id),
                )
                connection.execute(
                    """
                    INSERT INTO quota_usage(usage_id, lease_id, units, consumed_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (usage_id, lease_id, units, _iso(observed)),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def release(self, lease_id: str, *, now: datetime) -> SourceQuotaLease:
        observed = normalize_aware_utc(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM quota_lease WHERE lease_id = ?",
                    (lease_id,),
                ).fetchone()
                if row is None:
                    raise SourceQuotaConflictError("quota lease does not exist")
                pending = connection.execute(
                    """
                    SELECT attempt_id FROM quota_attempt
                    WHERE lease_id = ? AND outcome = 'pending'
                    LIMIT 1
                    """,
                    (lease_id,),
                ).fetchone()
                if pending is not None:
                    raise SourceQuotaConflictError(
                        f"quota lease has pending attempt {pending['attempt_id']}"
                    )
                if row["released_at"] is None:
                    if observed < _parse(row["granted_at"]):
                        raise SourceQuotaConflictError("release precedes grant")
                    connection.execute(
                        "UPDATE quota_lease SET released_at = ? WHERE lease_id = ?",
                        (_iso(observed), lease_id),
                    )
                    row = connection.execute(
                        "SELECT * FROM quota_lease WHERE lease_id = ?",
                        (lease_id,),
                    ).fetchone()
                connection.commit()
                return self._lease_from_row(row)
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _lease_from_row(row: sqlite3.Row) -> SourceQuotaLease:
        return SourceQuotaLease(
            lease_id=row["lease_id"],
            source=row["source"],
            owner=row["owner"],
            units=row["units"],
            granted_at=row["granted_at"],
            expires_at=row["expires_at"],
            quota_reset_at=row["quota_reset_at"],
            released_at=row["released_at"],
        )
