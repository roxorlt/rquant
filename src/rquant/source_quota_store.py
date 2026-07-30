"""Transactional source-quota reservations shared by live and research workers."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from rquant.resource_admission import SourceQuotaLease
from rquant.runtime_contracts import normalize_aware_utc


class SourceQuotaConflictError(RuntimeError):
    pass


class SourceQuotaExhaustedError(RuntimeError):
    pass


def _iso(value: datetime) -> str:
    return normalize_aware_utc(value).isoformat(timespec="microseconds")


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SourceQuotaConflictError("stored quota timestamp is naive")
    return parsed.astimezone(UTC)


class SourceQuotaStore:
    """SQLite single-writer ledger for bounded, expiring source reservations."""

    def __init__(self, path: Path, *, busy_timeout_ms: int = 5_000) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
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
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS quota_window (
                    source TEXT NOT NULL,
                    window_id TEXT NOT NULL,
                    starts_at TEXT NOT NULL,
                    resets_at TEXT NOT NULL,
                    total_units INTEGER NOT NULL CHECK(total_units > 0),
                    PRIMARY KEY(source, window_id)
                );
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
                );
                CREATE TABLE IF NOT EXISTS quota_usage (
                    usage_id TEXT PRIMARY KEY,
                    lease_id TEXT NOT NULL REFERENCES quota_lease(lease_id),
                    units INTEGER NOT NULL CHECK(units > 0),
                    consumed_at TEXT NOT NULL
                );
                """
            )

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
                    raise SourceQuotaConflictError(
                        f"quota window overlaps {overlap['window_id']}"
                    )
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
                        WHEN released_at IS NULL AND expires_at > ?
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
