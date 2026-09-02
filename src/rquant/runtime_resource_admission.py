"""Production resource admission bindings for the isolated Strategy Lab worker."""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time as system_time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.research_run_spec import ResearchRunSpec
from rquant.resource_admission import (
    MAX_RESOURCE_CAPACITY_BYTES,
    MAX_RESOURCE_COUNT,
    MICROSECONDS_PER_SECOND,
    AdmissionDecision,
    AdmissionOutcome,
    AdmissionPolicy,
    AdmissionRequest,
    ResourceReservationIdentity,
    ResourceReservationLease,
    ResourceSnapshot,
    SourceQuotaLease,
    TradingSession,
    evaluate_admission,
    seconds_to_microseconds,
    timedelta_microseconds,
)
from rquant.resource_journal_high_water import (
    RESOURCE_JOURNAL_HEAD_NAMESPACE,
    RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
    RESOURCE_JOURNAL_SIGNING_PURPOSE,
    TRUSTED_RESOURCE_ROLE_PURPOSES,
    ResourceJournalAntiRollbackReceipt,
    ResourceJournalHighWaterAuthority,
    ResourceJournalHighWaterCheckpoint,
    ResourceJournalHighWaterError,
    TrustedRoleInventory,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_market_session import (
    MarketCalendarAuthority,
    decide_market_session,
)
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeServiceStatus
from rquant.runtime_serving_authority import ServingSourceAuthorityReader
from rquant.runtime_serving_snapshot import RUNTIME_HEALTH_DATASET_ID, RuntimeHealthPayload
from rquant.serving_contracts import FreshnessStatus

LAB_RESOURCE_POLICY_ENV = "RQUANT_LAB_RESOURCE_POLICY_VERSION"
LAB_LIVE_SLO_AUTHORITY_ROOT_ENV = "RQUANT_LAB_LIVE_SLO_AUTHORITY_ROOT"
LAB_RESOURCE_POLICY_V1 = "lab-resource-v1"

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_GIB = 1024**3
_MIB = 1024**2
_POLICY_V1 = {
    "allow_live_session": False,
    "max_live_shard_duration_ms": 5_000,
    "max_snapshot_age_microseconds": 5 * MICROSECONDS_PER_SECOND,
    "max_live_backlog_age_microseconds": 30 * MICROSECONDS_PER_SECOND,
    "max_live_p95_latency_microseconds": 5 * MICROSECONDS_PER_SECOND,
    "min_available_memory_bytes": 2 * _GIB,
    "min_available_disk_bytes": 10 * _GIB,
    "max_io_pressure_pct": 60.0,
    "max_cpu_load_pct": 75.0,
    "max_expected_memory_bytes": 768 * _MIB,
    "max_expected_disk_bytes": 64 * _GIB,
    "max_expected_quota_units": 0,
    "retry_delay_seconds": 60,
}


class RuntimeResourceAdmissionError(RuntimeError):
    """The worker cannot establish a trustworthy admission decision."""


class RuntimeResourceAdmissionTransientError(RuntimeResourceAdmissionError):
    """Another writer held the reservation database; nothing is misconfigured.

    Contention is operational state with a next attempt, so callers must route
    it somewhere retryable.  Issue #159 came from the opposite: `lab_worker`
    folded a lost race into a permanent configuration fault and took the whole
    worker down with it.
    """


class RuntimeResourceAdmissionLockWaitTimeoutError(RuntimeResourceAdmissionTransientError):
    """The bounded lock wait elapsed with a competing writer still committing."""


class RuntimeResourceAdmissionCancelledError(RuntimeResourceAdmissionError):
    """A caller-supplied stop authority abandoned the wait.

    Deliberately not transient: the caller asked to stop, so retrying is the
    wrong answer even though nothing is broken.
    """


_SQLITE_CONTENTION_PRIMARY_ERROR_CODES = frozenset({sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})


def _is_sqlite_contention(exc: BaseException) -> bool:
    """Did SQLite itself report BUSY/LOCKED?  Decided by error code, never text.

    `str(exc)` is not a classification interface.  SQLite's English wording is
    free to change between builds, and the same words appear on failures that
    are not contention at all - `SQLITE_ERROR` and `SQLITE_READONLY` paths both
    produce messages containing "locked" - so a text match simultaneously
    misses real contention worded differently and retries permanent faults
    until the caller's whole budget is gone.  `sqlite_errorcode` (Python 3.11+)
    is the stable answer.  Its low byte is the primary code, so every extended
    form - SQLITE_BUSY_SNAPSHOT, SQLITE_BUSY_RECOVERY, SQLITE_LOCKED_SHAREDCACHE
    - classifies with its parent without being enumerated here.

    Anything carrying no such code is *not* contention: a plain `OSError`, an
    exception built by hand rather than raised by the driver, or a runtime that
    predates the attribute.  That direction is the safe one - an unclassifiable
    failure falls through to "do not retry", so a permanent fault is reported
    once instead of being spun on.
    """

    if not isinstance(exc, sqlite3.OperationalError):
        return False
    code = getattr(exc, "sqlite_errorcode", None)
    if not isinstance(code, int) or isinstance(code, bool):
        return False
    return (code & 0xFF) in _SQLITE_CONTENTION_PRIMARY_ERROR_CODES


def _reservation_failure(message: str, exc: BaseException) -> RuntimeResourceAdmissionError:
    """Classify a raw SQLite failure: contended is retryable, the rest is not."""

    if _is_sqlite_contention(exc):
        return RuntimeResourceAdmissionTransientError(message)
    return RuntimeResourceAdmissionError(message)


_MAX_ACTIVE_RESOURCE_RESERVATIONS = 4_096
_MAX_RESOURCE_LEASE_SECONDS = 3_600
_MAX_RESOURCE_LOCK_WAIT_SECONDS = 1.0
# One second is the ceiling `_lock_wait_seconds` already refuses to exceed and
# the one `_initialize` already waits under - 4ce74b5 measured that on a CI
# runner and adopted it.  Sharing the cap is the point; it is *not* derived from
# how long `reserve()` holds the lock, and no such derivation exists: the
# critical section runs pydantic validation and calls back into the caller's
# `snapshot_provider()`, so its cost is Python work of unbounded size rather
# than a countable number of fsyncs.  What the flat 0.05 got wrong was refusing
# a legitimate loser for a window nobody had measured at all (issue #159).
# Raising it to the shared cap costs no responsiveness: the wait polls
# `stop_requested` every 5ms and `_request_lock_wait_seconds` narrows the budget
# again to whatever is left of the caller's own deadline.
_DEFAULT_RESOURCE_LOCK_WAIT_SECONDS = _MAX_RESOURCE_LOCK_WAIT_SECONDS
_RESOURCE_LOCK_POLL_MILLISECONDS = 5
_RESOURCE_RESERVATION_APPLICATION_ID = 1_381_065_281
_RESOURCE_RESERVATION_SCHEMA_VERSION = 2
_RESOURCE_RESERVATION_TABLE = "resource_reservation"
_RESOURCE_RESERVATION_AUTHORITY_TABLE = "resource_reservation_authority"
_RESOURCE_RESERVATION_EXPIRY_INDEX = "resource_reservation_expiry_v2_idx"
_RESOURCE_RESERVATION_TABLE_SQL = f"""
CREATE TABLE {_RESOURCE_RESERVATION_TABLE} (
    lease_id TEXT NOT NULL CHECK (
        length(lease_id) = 64 AND lease_id NOT GLOB '*[^0-9a-f]*'
    ),
    job_id TEXT NOT NULL CHECK (
        length(job_id) = 36 AND job_id NOT GLOB '*[^0-9a-f-]*'
    ),
    run_id TEXT NOT NULL CHECK (
        length(run_id) = 64 AND run_id NOT GLOB '*[^0-9a-f]*'
    ),
    shard_id TEXT NOT NULL CHECK (
        length(shard_id) = 36 AND shard_id NOT GLOB '*[^0-9a-f-]*'
    ),
    attempt_id TEXT NOT NULL CHECK (
        length(attempt_id) = 36 AND attempt_id NOT GLOB '*[^0-9a-f-]*'
    ),
    claim_generation INTEGER NOT NULL CHECK (
        claim_generation BETWEEN 1 AND {MAX_RESOURCE_COUNT}
    ),
    scheduler_fencing_token INTEGER NOT NULL CHECK (
        scheduler_fencing_token BETWEEN 1 AND {MAX_RESOURCE_COUNT}
    ),
    worker_id TEXT NOT NULL CHECK (
        length(worker_id) BETWEEN 1 AND 200 AND worker_id = trim(worker_id)
    ),
    request_hash TEXT NOT NULL CHECK (
        length(request_hash) = 64 AND request_hash NOT GLOB '*[^0-9a-f]*'
    ),
    expected_memory_bytes INTEGER NOT NULL CHECK (
        expected_memory_bytes BETWEEN 0 AND {MAX_RESOURCE_CAPACITY_BYTES}
    ),
    expected_disk_bytes INTEGER NOT NULL CHECK (
        expected_disk_bytes BETWEEN 0 AND {MAX_RESOURCE_CAPACITY_BYTES}
    ),
    expected_quota_units INTEGER NOT NULL CHECK (
        expected_quota_units BETWEEN 0 AND {MAX_RESOURCE_COUNT}
    ),
    granted_at TEXT NOT NULL CHECK (
        length(granted_at) = 32 AND substr(granted_at, 27, 6) = '+00:00'
    ),
    expires_at TEXT NOT NULL CHECK (
        length(expires_at) = 32 AND substr(expires_at, 27, 6) = '+00:00'
    ),
    last_renewal_operation_id TEXT NOT NULL CHECK (
        length(last_renewal_operation_id) = 64
        AND last_renewal_operation_id NOT GLOB '*[^0-9a-f]*'
    ),
    PRIMARY KEY (lease_id),
    CHECK (expires_at > granted_at)
) STRICT, WITHOUT ROWID
""".strip()
_RESOURCE_RESERVATION_AUTHORITY_TABLE_SQL = f"""
CREATE TABLE {_RESOURCE_RESERVATION_AUTHORITY_TABLE} (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    last_clock_at TEXT NOT NULL CHECK (
        length(last_clock_at) = 32 AND substr(last_clock_at, 27, 6) = '+00:00'
    ),
    last_snapshot_observed_at TEXT CHECK (
        last_snapshot_observed_at IS NULL OR (
            length(last_snapshot_observed_at) = 32
            AND substr(last_snapshot_observed_at, 27, 6) = '+00:00'
        )
    )
) STRICT, WITHOUT ROWID
""".strip()
_RESOURCE_RESERVATION_INDEX_SQL = f"""
CREATE INDEX {_RESOURCE_RESERVATION_EXPIRY_INDEX}
ON {_RESOURCE_RESERVATION_TABLE}(expires_at, lease_id)
""".strip()
_RESOURCE_RESERVATION_COLUMNS = (
    "lease_id",
    "job_id",
    "run_id",
    "shard_id",
    "attempt_id",
    "claim_generation",
    "scheduler_fencing_token",
    "worker_id",
    "request_hash",
    "expected_memory_bytes",
    "expected_disk_bytes",
    "expected_quota_units",
    "granted_at",
    "expires_at",
    "last_renewal_operation_id",
)


@dataclass(frozen=True)
class ResourceReservationAdmission:
    decision: AdmissionDecision
    request: AdmissionRequest
    snapshot: ResourceSnapshot
    policy: AdmissionPolicy
    lease: ResourceReservationLease | None = None


class SQLiteResourceReservationStore:
    """Cross-process resource leases serialized by one SQLite write transaction."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock or _system_clock
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=_RESOURCE_LOCK_POLL_MILLISECONDS / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {_RESOURCE_LOCK_POLL_MILLISECONDS}")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA trusted_schema = OFF")
        return connection

    @staticmethod
    def _stop_requested(stop_requested: Callable[[], bool] | None) -> bool:
        if stop_requested is None:
            return False
        try:
            stopped = stop_requested()
        except Exception as exc:
            raise RuntimeResourceAdmissionError(
                "resource reservation cancellation authority failed"
            ) from exc
        if not isinstance(stopped, bool):
            raise RuntimeResourceAdmissionError(
                "resource reservation cancellation authority returned an invalid contract"
            )
        return stopped

    @staticmethod
    def _lock_wait_seconds(value: object | None) -> float:
        if value is None:
            return _DEFAULT_RESOURCE_LOCK_WAIT_SECONDS
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RuntimeResourceAdmissionError(
                "resource lock_wait_timeout_seconds must be numeric"
            )
        normalized = float(value)
        if (
            not math.isfinite(normalized)
            or normalized <= 0
            or normalized > _MAX_RESOURCE_LOCK_WAIT_SECONDS
        ):
            raise RuntimeResourceAdmissionError(
                "resource lock_wait_timeout_seconds is outside 0..1"
            )
        return normalized

    def _begin_immediate(
        self,
        connection: sqlite3.Connection,
        *,
        lock_wait_timeout_seconds: object | None,
        stop_requested: Callable[[], bool] | None,
    ) -> None:
        wait_seconds = self._lock_wait_seconds(lock_wait_timeout_seconds)
        deadline = system_time.monotonic() + wait_seconds
        while True:
            if self._stop_requested(stop_requested):
                raise RuntimeResourceAdmissionCancelledError(
                    "resource reservation lock wait cancelled"
                )
            try:
                connection.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                if not _is_sqlite_contention(exc):
                    raise
                if self._stop_requested(stop_requested):
                    raise RuntimeResourceAdmissionCancelledError(
                        "resource reservation lock wait cancelled"
                    ) from exc
                if system_time.monotonic() >= deadline:
                    raise RuntimeResourceAdmissionLockWaitTimeoutError(
                        "resource reservation lock wait timeout"
                    ) from exc
                system_time.sleep(
                    min(
                        _RESOURCE_LOCK_POLL_MILLISECONDS / 1_000,
                        max(0, deadline - system_time.monotonic()),
                    )
                )
                continue
            if self._stop_requested(stop_requested):
                connection.rollback()
                raise RuntimeResourceAdmissionCancelledError(
                    "resource reservation lock wait cancelled"
                )
            return

    def _request_lock_wait_seconds(
        self,
        request: AdmissionRequest,
        *,
        configured_timeout_seconds: object | None,
    ) -> float:
        configured = self._lock_wait_seconds(configured_timeout_seconds)
        now = self._now()
        remaining_microseconds = timedelta_microseconds(request.deadline - now)
        if remaining_microseconds <= 0:
            raise RuntimeResourceAdmissionError(
                "resource reservation request deadline expired before lock acquisition"
            )
        return min(configured, remaining_microseconds / MICROSECONDS_PER_SECOND)

    @staticmethod
    def _normalized_schema_sql(value: str | None) -> str:
        return "" if value is None else " ".join(value.split())

    @staticmethod
    def _schema_pragma(connection: sqlite3.Connection, name: str) -> int:
        row = connection.execute(f"PRAGMA {name}").fetchone()
        if row is None or isinstance(row[0], bool) or not isinstance(row[0], int):
            raise RuntimeResourceAdmissionError("resource reservation schema pragma is invalid")
        return row[0]

    def _attest_schema(self, connection: sqlite3.Connection) -> None:
        if (
            self._schema_pragma(connection, "application_id")
            != _RESOURCE_RESERVATION_APPLICATION_ID
            or self._schema_pragma(connection, "user_version")
            != _RESOURCE_RESERVATION_SCHEMA_VERSION
        ):
            raise RuntimeResourceAdmissionError("resource reservation schema identity mismatch")
        objects = tuple(
            tuple(row)
            for row in connection.execute(
                """
                SELECT type, name, tbl_name, sql
                FROM sqlite_schema
                WHERE name NOT LIKE 'sqlite_%'
                ORDER BY type, name
                """
            ).fetchall()
        )
        expected_objects = (
            (
                "index",
                _RESOURCE_RESERVATION_EXPIRY_INDEX,
                _RESOURCE_RESERVATION_TABLE,
                _RESOURCE_RESERVATION_INDEX_SQL,
            ),
            (
                "table",
                _RESOURCE_RESERVATION_TABLE,
                _RESOURCE_RESERVATION_TABLE,
                _RESOURCE_RESERVATION_TABLE_SQL,
            ),
            (
                "table",
                _RESOURCE_RESERVATION_AUTHORITY_TABLE,
                _RESOURCE_RESERVATION_AUTHORITY_TABLE,
                _RESOURCE_RESERVATION_AUTHORITY_TABLE_SQL,
            ),
        )
        normalized_objects = tuple(
            (object_type, name, table_name, self._normalized_schema_sql(sql))
            for object_type, name, table_name, sql in objects
        )
        normalized_expected = tuple(
            (object_type, name, table_name, self._normalized_schema_sql(sql))
            for object_type, name, table_name, sql in expected_objects
        )
        if normalized_objects != normalized_expected:
            raise RuntimeResourceAdmissionError(
                "resource reservation schema object inventory mismatch"
            )
        table_rows = tuple(
            tuple(row)
            for row in connection.execute(
                f"PRAGMA table_xinfo('{_RESOURCE_RESERVATION_TABLE}')"
            ).fetchall()
        )
        expected_table_rows = tuple(
            (
                index,
                name,
                "INTEGER"
                if name
                in {
                    "claim_generation",
                    "scheduler_fencing_token",
                    "expected_memory_bytes",
                    "expected_disk_bytes",
                    "expected_quota_units",
                }
                else "TEXT",
                1,
                None,
                1 if name == "lease_id" else 0,
                0,
            )
            for index, name in enumerate(_RESOURCE_RESERVATION_COLUMNS)
        )
        if table_rows != expected_table_rows:
            raise RuntimeResourceAdmissionError(
                "resource reservation schema column contract mismatch"
            )
        authority_rows = tuple(
            tuple(row)
            for row in connection.execute(
                f"PRAGMA table_xinfo('{_RESOURCE_RESERVATION_AUTHORITY_TABLE}')"
            ).fetchall()
        )
        if authority_rows != (
            (0, "singleton", "INTEGER", 1, None, 1, 0),
            (1, "last_clock_at", "TEXT", 1, None, 0, 0),
            (2, "last_snapshot_observed_at", "TEXT", 0, None, 0, 0),
        ):
            raise RuntimeResourceAdmissionError(
                "resource reservation authority column contract mismatch"
            )
        table_list_rows = tuple(
            tuple(row)
            for row in connection.execute("PRAGMA table_list").fetchall()
            if row[1] == _RESOURCE_RESERVATION_TABLE
        )
        if table_list_rows != (
            (
                "main",
                _RESOURCE_RESERVATION_TABLE,
                "table",
                len(_RESOURCE_RESERVATION_COLUMNS),
                1,
                1,
            ),
        ) or tuple(
            tuple(row)
            for row in connection.execute("PRAGMA table_list").fetchall()
            if row[1] == _RESOURCE_RESERVATION_AUTHORITY_TABLE
        ) != (
            (
                "main",
                _RESOURCE_RESERVATION_AUTHORITY_TABLE,
                "table",
                3,
                1,
                1,
            ),
        ):
            raise RuntimeResourceAdmissionError(
                "resource reservation schema STRICT contract mismatch"
            )
        index_rows = {
            (row[1], row[2], row[3], row[4])
            for row in connection.execute(
                f"PRAGMA index_list('{_RESOURCE_RESERVATION_TABLE}')"
            ).fetchall()
        }
        if index_rows != {
            (_RESOURCE_RESERVATION_EXPIRY_INDEX, 0, "c", 0),
            (f"sqlite_autoindex_{_RESOURCE_RESERVATION_TABLE}_1", 1, "pk", 0),
        }:
            raise RuntimeResourceAdmissionError(
                "resource reservation schema index inventory mismatch"
            )
        expiry_index_rows = tuple(
            tuple(row)
            for row in connection.execute(
                f"PRAGMA index_xinfo('{_RESOURCE_RESERVATION_EXPIRY_INDEX}')"
            ).fetchall()
        )
        if expiry_index_rows != (
            (0, 13, "expires_at", 0, "BINARY", 1),
            (1, 0, "lease_id", 0, "BINARY", 1),
        ):
            raise RuntimeResourceAdmissionError(
                "resource reservation schema index contract mismatch"
            )
        if (
            connection.execute(
                f"PRAGMA foreign_key_list('{_RESOURCE_RESERVATION_TABLE}')"
            ).fetchall()
            or connection.execute(
                f"PRAGMA foreign_key_list('{_RESOURCE_RESERVATION_AUTHORITY_TABLE}')"
            ).fetchall()
        ):
            raise RuntimeResourceAdmissionError(
                "resource reservation schema foreign keys are not allowed"
            )
        authority_data = connection.execute(
            f"""
            SELECT singleton, last_clock_at, last_snapshot_observed_at
            FROM {_RESOURCE_RESERVATION_AUTHORITY_TABLE}
            """
        ).fetchall()
        if len(authority_data) != 1 or authority_data[0]["singleton"] != 1:
            raise RuntimeResourceAdmissionError(
                "resource reservation authority singleton is invalid"
            )
        try:
            normalize_aware_utc(datetime.fromisoformat(authority_data[0]["last_clock_at"]))
            snapshot_value = authority_data[0]["last_snapshot_observed_at"]
            if snapshot_value is not None:
                normalize_aware_utc(datetime.fromisoformat(snapshot_value))
        except Exception as exc:
            raise RuntimeResourceAdmissionError(
                "resource reservation authority watermark is invalid"
            ) from exc

    def _initialize(self) -> None:
        # Opening the store is the one operation that runs before any lock
        # discipline is in force.  `_connect` and the `synchronous` pragma both
        # have to read the schema, which needs a SHARED lock, and the only
        # thing protecting them is the 5ms `busy_timeout` poll value chosen so
        # `_begin_immediate` can do its own cancellable waiting.  A second
        # process opening the same database while the first one is committing
        # the schema - EXCLUSIVE, and with `synchronous = FULL` that means real
        # fsyncs - therefore fails outright instead of waiting, which is how
        # two contenders both died with "database is locked" on a CI runner
        # where the commit costs more than 5ms.  Initialisation gets the same
        # bounded wait the rest of the store already uses.
        deadline = system_time.monotonic() + _MAX_RESOURCE_LOCK_WAIT_SECONDS
        while True:
            try:
                self._initialize_once()
                return
            except sqlite3.OperationalError as exc:
                if not _is_sqlite_contention(exc):
                    raise RuntimeResourceAdmissionError(
                        "resource reservation store initialization failed"
                    ) from exc
                remaining = deadline - system_time.monotonic()
                if remaining <= 0:
                    raise RuntimeResourceAdmissionTransientError(
                        "resource reservation store initialization failed"
                    ) from exc
                system_time.sleep(
                    min(_RESOURCE_LOCK_POLL_MILLISECONDS / 1_000, max(0.0, remaining))
                )

    def _initialize_once(self) -> None:
        try:
            with self._connect() as connection:
                connection.execute("PRAGMA synchronous = FULL")
                self._begin_immediate(
                    connection,
                    lock_wait_timeout_seconds=_MAX_RESOURCE_LOCK_WAIT_SECONDS,
                    stop_requested=None,
                )
                objects = connection.execute(
                    "SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1"
                ).fetchone()
                application_id = self._schema_pragma(connection, "application_id")
                user_version = self._schema_pragma(connection, "user_version")
                if objects is None and application_id == 0 and user_version == 0:
                    connection.execute(_RESOURCE_RESERVATION_TABLE_SQL)
                    connection.execute(_RESOURCE_RESERVATION_AUTHORITY_TABLE_SQL)
                    connection.execute(_RESOURCE_RESERVATION_INDEX_SQL)
                    connection.execute(
                        f"""
                        INSERT INTO {_RESOURCE_RESERVATION_AUTHORITY_TABLE} (
                            singleton, last_clock_at, last_snapshot_observed_at
                        ) VALUES (1, ?, NULL)
                        """,
                        (self._timestamp(self._now()),),
                    )
                    connection.execute(
                        f"PRAGMA application_id = {_RESOURCE_RESERVATION_APPLICATION_ID}"
                    )
                    connection.execute(
                        f"PRAGMA user_version = {_RESOURCE_RESERVATION_SCHEMA_VERSION}"
                    )
                self._attest_schema(connection)
                connection.commit()
        except RuntimeResourceAdmissionError:
            raise
        except sqlite3.OperationalError:
            # `_initialize` decides whether a busy database is worth another
            # attempt; every other operational error is terminal there too.
            raise
        except (OSError, sqlite3.Error) as exc:
            raise RuntimeResourceAdmissionError(
                "resource reservation store initialization failed"
            ) from exc

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return normalize_aware_utc(value).isoformat(timespec="microseconds")

    def _now(self) -> datetime:
        try:
            return normalize_aware_utc(self.clock())
        except Exception as exc:
            raise RuntimeResourceAdmissionError("resource reservation clock failed") from exc

    def _authority_now(self, connection: sqlite3.Connection) -> datetime:
        sampled_at = self._now()
        row = connection.execute(
            f"""
            SELECT last_clock_at
            FROM {_RESOURCE_RESERVATION_AUTHORITY_TABLE}
            WHERE singleton = 1
            """
        ).fetchone()
        if row is None:
            raise RuntimeResourceAdmissionError(
                "resource reservation authority singleton is missing"
            )
        try:
            previous = normalize_aware_utc(datetime.fromisoformat(row["last_clock_at"]))
        except Exception as exc:
            raise RuntimeResourceAdmissionError(
                "resource reservation authority watermark is invalid"
            ) from exc
        if sampled_at < previous:
            raise RuntimeResourceAdmissionError(
                "resource reservation authority clock rollback detected"
            )
        updated = connection.execute(
            f"""
            UPDATE {_RESOURCE_RESERVATION_AUTHORITY_TABLE}
            SET last_clock_at = ?
            WHERE singleton = 1 AND last_clock_at = ?
            """,
            (self._timestamp(sampled_at), self._timestamp(previous)),
        )
        if updated.rowcount != 1:
            raise RuntimeResourceAdmissionError(
                "resource reservation authority watermark update failed"
            )
        return sampled_at

    def _accept_snapshot_watermark(
        self,
        connection: sqlite3.Connection,
        *,
        snapshot: ResourceSnapshot,
    ) -> None:
        row = connection.execute(
            f"""
            SELECT last_snapshot_observed_at
            FROM {_RESOURCE_RESERVATION_AUTHORITY_TABLE}
            WHERE singleton = 1
            """
        ).fetchone()
        if row is None:
            raise RuntimeResourceAdmissionError(
                "resource reservation authority singleton is missing"
            )
        previous_value = row["last_snapshot_observed_at"]
        if previous_value is not None:
            try:
                previous = normalize_aware_utc(datetime.fromisoformat(previous_value))
            except Exception as exc:
                raise RuntimeResourceAdmissionError(
                    "resource reservation snapshot watermark is invalid"
                ) from exc
            if snapshot.observed_at < previous:
                raise RuntimeResourceAdmissionError(
                    "resource reservation snapshot clock rollback detected"
                )
        updated = connection.execute(
            f"""
            UPDATE {_RESOURCE_RESERVATION_AUTHORITY_TABLE}
            SET last_snapshot_observed_at = ?
            WHERE singleton = 1
            """,
            (self._timestamp(snapshot.observed_at),),
        )
        if updated.rowcount != 1:
            raise RuntimeResourceAdmissionError(
                "resource reservation snapshot watermark update failed"
            )

    @staticmethod
    def _lease_expiry(granted_at: datetime, *, lease_seconds: int) -> datetime:
        try:
            return granted_at + timedelta(seconds=lease_seconds)
        except OverflowError as exc:
            raise RuntimeResourceAdmissionError(
                "resource reservation lease expiry is outside datetime range"
            ) from exc

    @staticmethod
    def _validate_lease_seconds(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise RuntimeResourceAdmissionError("resource lease_seconds must be an integer")
        if value < 1 or value > _MAX_RESOURCE_LEASE_SECONDS:
            raise RuntimeResourceAdmissionError("resource lease_seconds is outside 1..3600")
        return value

    @staticmethod
    def _reserve_operation_id(
        *,
        identity: ResourceReservationIdentity,
        request_hash: str,
    ) -> str:
        return canonical_sha256(
            {
                "operation": "resource_reservation_reserve_v1",
                "identity": identity.model_dump(mode="json"),
                "request_hash": request_hash,
            }
        )

    @staticmethod
    def _renewal_operation_id(lease: ResourceReservationLease) -> str:
        return canonical_sha256(
            {
                "operation": "resource_reservation_renew_v1",
                "lease": lease.model_dump(mode="json"),
            }
        )

    @staticmethod
    def _lease_matches_request(
        lease: ResourceReservationLease,
        *,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        request_hash: str,
    ) -> bool:
        return (
            lease.identity == identity
            and lease.request_hash == request_hash
            and lease.expected_memory_bytes == request.expected_memory_bytes
            and lease.expected_disk_bytes == request.expected_disk_bytes
            and lease.expected_quota_units == request.expected_quota_units
        )

    @staticmethod
    def _admitted_decision(snapshot: ResourceSnapshot) -> AdmissionDecision:
        return AdmissionDecision(
            outcome=AdmissionOutcome.ADMITTED,
            observed_at=snapshot.observed_at,
        )

    @staticmethod
    def _lease_from_row(row: sqlite3.Row) -> ResourceReservationLease:
        try:
            return ResourceReservationLease(
                lease_id=row["lease_id"],
                identity=ResourceReservationIdentity(
                    job_id=row["job_id"],
                    run_id=row["run_id"],
                    shard_id=row["shard_id"],
                    attempt_id=row["attempt_id"],
                    claim_generation=row["claim_generation"],
                    scheduler_fencing_token=row["scheduler_fencing_token"],
                    worker_id=row["worker_id"],
                ),
                request_hash=row["request_hash"],
                expected_memory_bytes=row["expected_memory_bytes"],
                expected_disk_bytes=row["expected_disk_bytes"],
                expected_quota_units=row["expected_quota_units"],
                granted_at=datetime.fromisoformat(row["granted_at"]),
                expires_at=datetime.fromisoformat(row["expires_at"]),
            )
        except Exception as exc:
            raise RuntimeResourceAdmissionError(
                "persisted resource reservation lease is invalid"
            ) from exc

    def _verify_written_lease(
        self,
        connection: sqlite3.Connection,
        *,
        expected: ResourceReservationLease,
        expected_operation_id: str,
        operation: str,
    ) -> None:
        row = connection.execute(
            "SELECT * FROM resource_reservation WHERE lease_id = ?",
            (expected.lease_id,),
        ).fetchone()
        try:
            persisted = None if row is None else self._lease_from_row(row)
        except RuntimeResourceAdmissionError as exc:
            raise RuntimeResourceAdmissionError(
                f"resource reservation {operation} verification failed"
            ) from exc
        if (
            persisted != expected
            or row is None
            or row["last_renewal_operation_id"] != expected_operation_id
        ):
            raise RuntimeResourceAdmissionError(
                f"resource reservation {operation} verification failed"
            )

    @staticmethod
    def _delete_expired(connection: sqlite3.Connection, *, now: datetime) -> None:
        connection.execute(
            "DELETE FROM resource_reservation WHERE expires_at <= ?",
            (SQLiteResourceReservationStore._timestamp(now),),
        )

    @staticmethod
    def _active_rows(
        connection: sqlite3.Connection,
        *,
        excluded_lease_id: str | None = None,
    ) -> tuple[sqlite3.Row, ...]:
        if excluded_lease_id is None:
            rows = connection.execute(
                "SELECT * FROM resource_reservation ORDER BY lease_id LIMIT ?",
                (_MAX_ACTIVE_RESOURCE_RESERVATIONS + 1,),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT * FROM resource_reservation
                WHERE lease_id <> ?
                ORDER BY lease_id
                LIMIT ?
                """,
                (excluded_lease_id, _MAX_ACTIVE_RESOURCE_RESERVATIONS + 1),
            ).fetchall()
        if len(rows) > _MAX_ACTIVE_RESOURCE_RESERVATIONS:
            raise RuntimeResourceAdmissionError("active resource reservation budget exceeded")
        return tuple(rows)

    @staticmethod
    def _checked_sum(rows: tuple[sqlite3.Row, ...], field: str) -> int:
        total = 0
        maximum = (
            MAX_RESOURCE_CAPACITY_BYTES
            if field in {"expected_memory_bytes", "expected_disk_bytes"}
            else MAX_RESOURCE_COUNT
        )
        for row in rows:
            value = row[field]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RuntimeResourceAdmissionError(
                    "resource reservation contains an invalid numeric value"
                )
            total += value
            if total > maximum:
                raise RuntimeResourceAdmissionError(
                    "resource reservation aggregate exceeds the supported range"
                )
        return total

    @staticmethod
    def _adjust_snapshot(
        snapshot: ResourceSnapshot,
        rows: tuple[sqlite3.Row, ...],
    ) -> ResourceSnapshot:
        reserved_memory = SQLiteResourceReservationStore._checked_sum(rows, "expected_memory_bytes")
        reserved_disk = SQLiteResourceReservationStore._checked_sum(rows, "expected_disk_bytes")
        reserved_quota = SQLiteResourceReservationStore._checked_sum(rows, "expected_quota_units")
        values = snapshot.model_dump(mode="python")
        values.update(
            available_memory_bytes=max(0, snapshot.available_memory_bytes - reserved_memory),
            available_disk_bytes=max(0, snapshot.available_disk_bytes - reserved_disk),
            source_quota_remaining=max(0, snapshot.source_quota_remaining - reserved_quota),
        )
        return ResourceSnapshot.model_validate(values)

    @staticmethod
    def _validate_snapshot(
        snapshot: object,
        *,
        now: datetime,
        policy: AdmissionPolicy,
    ) -> ResourceSnapshot:
        if not isinstance(snapshot, ResourceSnapshot):
            raise RuntimeResourceAdmissionError(
                "resource snapshot provider returned an invalid contract"
            )
        if snapshot.observed_at > now:
            raise RuntimeResourceAdmissionError("resource snapshot is from the future")
        if (
            timedelta_microseconds(now - snapshot.observed_at)
            > policy.max_snapshot_age_microseconds
        ):
            raise RuntimeResourceAdmissionError("resource snapshot is stale")
        return snapshot

    def reserve(
        self,
        *,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        policy: AdmissionPolicy,
        snapshot_provider: Callable[[], ResourceSnapshot],
        lease_seconds: int,
        quota_lease_provider: (
            Callable[[AdmissionRequest, ResourceSnapshot], SourceQuotaLease | None] | None
        ) = None,
        lock_wait_timeout_seconds: float | None = None,
        stop_requested: Callable[[], bool] | None = None,
    ) -> ResourceReservationAdmission:
        validated_identity = ResourceReservationIdentity.model_validate(identity)
        validated_request = AdmissionRequest.model_validate(request)
        validated_policy = AdmissionPolicy.model_validate(policy)
        lease_ttl = self._validate_lease_seconds(lease_seconds)
        if str(validated_identity.job_id) != validated_request.job_id:
            raise RuntimeResourceAdmissionError(
                "resource reservation identity does not match admission request"
            )
        request_hash = canonical_sha256(validated_request)
        lease_id = canonical_sha256(validated_identity)
        lock_wait_seconds = self._request_lock_wait_seconds(
            validated_request,
            configured_timeout_seconds=lock_wait_timeout_seconds,
        )
        try:
            with self._connect() as connection:
                self._begin_immediate(
                    connection,
                    lock_wait_timeout_seconds=lock_wait_seconds,
                    stop_requested=stop_requested,
                )
                self._attest_schema(connection)
                now = self._authority_now(connection)
                if validated_request.deadline <= now:
                    raise RuntimeResourceAdmissionError(
                        "resource reservation request deadline expired after lock acquisition"
                    )
                self._delete_expired(connection, now=now)
                existing_row = connection.execute(
                    "SELECT * FROM resource_reservation WHERE lease_id = ?",
                    (lease_id,),
                ).fetchone()
                existing = None if existing_row is None else self._lease_from_row(existing_row)
                if existing is not None and not self._lease_matches_request(
                    existing,
                    identity=validated_identity,
                    request=validated_request,
                    request_hash=request_hash,
                ):
                    raise RuntimeResourceAdmissionError(
                        "resource reservation retry conflicts with persisted lease"
                    )
                raw_snapshot = snapshot_provider()
                if self._stop_requested(stop_requested):
                    raise RuntimeResourceAdmissionCancelledError(
                        "resource reservation admission cancelled after resource probe"
                    )
                sampled_at = self._authority_now(connection)
                snapshot = self._validate_snapshot(
                    raw_snapshot,
                    now=sampled_at,
                    policy=validated_policy,
                )
                self._accept_snapshot_watermark(connection, snapshot=snapshot)
                rows = self._active_rows(connection, excluded_lease_id=lease_id)
                adjusted_snapshot = self._adjust_snapshot(snapshot, rows)
                if existing is not None:
                    if existing.expires_at <= sampled_at:
                        raise RuntimeResourceAdmissionError(
                            "resource reservation retry found an expired lease"
                        )
                    decision = self._admitted_decision(adjusted_snapshot)
                    lease: ResourceReservationLease | None = existing
                else:
                    if len(rows) >= _MAX_ACTIVE_RESOURCE_RESERVATIONS:
                        raise RuntimeResourceAdmissionError(
                            "active resource reservation capacity exhausted"
                        )
                    quota_lease = None
                    if (
                        validated_request.expected_quota_units > 0
                        and quota_lease_provider is not None
                    ):
                        quota_lease = quota_lease_provider(
                            validated_request,
                            adjusted_snapshot,
                        )
                        if quota_lease is not None and not isinstance(
                            quota_lease,
                            SourceQuotaLease,
                        ):
                            raise RuntimeResourceAdmissionError(
                                "source quota lease provider returned an invalid contract"
                            )
                    decision = evaluate_admission(
                        validated_request,
                        adjusted_snapshot,
                        validated_policy,
                        quota_lease=quota_lease,
                    )
                    lease = None
                if existing is None and decision.outcome is AdmissionOutcome.ADMITTED:
                    expires_at = self._lease_expiry(
                        sampled_at,
                        lease_seconds=lease_ttl,
                    )
                    lease = ResourceReservationLease(
                        identity=validated_identity,
                        request_hash=request_hash,
                        expected_memory_bytes=validated_request.expected_memory_bytes,
                        expected_disk_bytes=validated_request.expected_disk_bytes,
                        expected_quota_units=validated_request.expected_quota_units,
                        granted_at=sampled_at,
                        expires_at=expires_at,
                    )
                    operation_id = self._reserve_operation_id(
                        identity=validated_identity,
                        request_hash=request_hash,
                    )
                    connection.execute(
                        """
                        INSERT INTO resource_reservation (
                            lease_id, job_id, run_id, shard_id, attempt_id,
                            claim_generation, scheduler_fencing_token, worker_id,
                            request_hash, expected_memory_bytes, expected_disk_bytes,
                            expected_quota_units, granted_at, expires_at,
                            last_renewal_operation_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            lease.lease_id,
                            str(lease.identity.job_id),
                            lease.identity.run_id,
                            str(lease.identity.shard_id),
                            str(lease.identity.attempt_id),
                            lease.identity.claim_generation,
                            lease.identity.scheduler_fencing_token,
                            lease.identity.worker_id,
                            lease.request_hash,
                            lease.expected_memory_bytes,
                            lease.expected_disk_bytes,
                            lease.expected_quota_units,
                            self._timestamp(lease.granted_at),
                            self._timestamp(lease.expires_at),
                            operation_id,
                        ),
                    )
                    self._verify_written_lease(
                        connection,
                        expected=lease,
                        expected_operation_id=operation_id,
                        operation="insert",
                    )
                if self._stop_requested(stop_requested):
                    raise RuntimeResourceAdmissionCancelledError(
                        "resource reservation admission cancelled before commit"
                    )
                connection.commit()
                return ResourceReservationAdmission(
                    decision=decision,
                    request=validated_request,
                    snapshot=adjusted_snapshot,
                    policy=validated_policy,
                    lease=lease,
                )
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise _reservation_failure("resource reservation transaction failed", exc) from exc

    def active_leases(self) -> tuple[ResourceReservationLease, ...]:
        try:
            with self._connect() as connection:
                self._begin_immediate(
                    connection,
                    lock_wait_timeout_seconds=None,
                    stop_requested=None,
                )
                self._attest_schema(connection)
                self._delete_expired(connection, now=self._authority_now(connection))
                rows = self._active_rows(connection)
                leases = tuple(self._lease_from_row(row) for row in rows)
                connection.commit()
                return leases
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise _reservation_failure("resource reservation read failed", exc) from exc

    def recheck(
        self,
        *,
        lease: ResourceReservationLease,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        policy: AdmissionPolicy,
        snapshot_provider: Callable[[], ResourceSnapshot],
        lease_seconds: int,
        quota_lease_provider: (
            Callable[[AdmissionRequest, ResourceSnapshot], SourceQuotaLease | None] | None
        ) = None,
        lock_wait_timeout_seconds: float | None = None,
        stop_requested: Callable[[], bool] | None = None,
    ) -> ResourceReservationAdmission:
        validated_lease = ResourceReservationLease.model_validate(lease)
        validated_identity = ResourceReservationIdentity.model_validate(identity)
        validated_request = AdmissionRequest.model_validate(request)
        validated_policy = AdmissionPolicy.model_validate(policy)
        lease_ttl = self._validate_lease_seconds(lease_seconds)
        if validated_lease.identity != validated_identity:
            raise RuntimeResourceAdmissionError("resource reservation recheck identity mismatch")
        if str(validated_identity.job_id) != validated_request.job_id:
            raise RuntimeResourceAdmissionError(
                "resource reservation identity does not match admission request"
            )
        request_hash = canonical_sha256(validated_request)
        if validated_lease.request_hash != request_hash:
            raise RuntimeResourceAdmissionError(
                "resource reservation request changed during execution"
            )
        renewal_operation_id = self._renewal_operation_id(validated_lease)
        lock_wait_seconds = self._request_lock_wait_seconds(
            validated_request,
            configured_timeout_seconds=lock_wait_timeout_seconds,
        )
        try:
            with self._connect() as connection:
                self._begin_immediate(
                    connection,
                    lock_wait_timeout_seconds=lock_wait_seconds,
                    stop_requested=stop_requested,
                )
                self._attest_schema(connection)
                now = self._authority_now(connection)
                row = connection.execute(
                    "SELECT * FROM resource_reservation WHERE lease_id = ?",
                    (validated_lease.lease_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeResourceAdmissionError(
                        "resource reservation expired or is missing"
                    )
                persisted = self._lease_from_row(row)
                if not self._lease_matches_request(
                    persisted,
                    identity=validated_identity,
                    request=validated_request,
                    request_hash=request_hash,
                ):
                    raise RuntimeResourceAdmissionError(
                        "resource reservation recheck conflicts with persisted lease"
                    )
                if persisted.expires_at <= now:
                    raise RuntimeResourceAdmissionError(
                        "resource reservation expired before recheck"
                    )
                is_idempotent_retry = persisted != validated_lease
                if is_idempotent_retry and (
                    row["last_renewal_operation_id"] != renewal_operation_id
                    or persisted.expires_at <= validated_lease.expires_at
                ):
                    raise RuntimeResourceAdmissionError(
                        "resource reservation changed before recheck"
                    )
                self._delete_expired(connection, now=now)
                raw_snapshot = snapshot_provider()
                if self._stop_requested(stop_requested):
                    raise RuntimeResourceAdmissionCancelledError(
                        "resource reservation recheck cancelled after resource probe"
                    )
                sampled_at = self._authority_now(connection)
                snapshot = self._validate_snapshot(
                    raw_snapshot,
                    now=sampled_at,
                    policy=validated_policy,
                )
                self._accept_snapshot_watermark(connection, snapshot=snapshot)
                if persisted.expires_at <= sampled_at:
                    raise RuntimeResourceAdmissionError(
                        "resource reservation expired during resource probe"
                    )
                rows = self._active_rows(
                    connection,
                    excluded_lease_id=validated_lease.lease_id,
                )
                adjusted_snapshot = self._adjust_snapshot(snapshot, rows)
                if is_idempotent_retry:
                    decision = self._admitted_decision(adjusted_snapshot)
                    if self._stop_requested(stop_requested):
                        raise RuntimeResourceAdmissionCancelledError(
                            "resource reservation recheck cancelled before commit"
                        )
                    connection.commit()
                    return ResourceReservationAdmission(
                        decision=decision,
                        request=validated_request,
                        snapshot=adjusted_snapshot,
                        policy=validated_policy,
                        lease=persisted,
                    )
                quota_lease = None
                if validated_request.expected_quota_units > 0 and quota_lease_provider is not None:
                    quota_lease = quota_lease_provider(validated_request, adjusted_snapshot)
                    if quota_lease is not None and not isinstance(quota_lease, SourceQuotaLease):
                        raise RuntimeResourceAdmissionError(
                            "source quota lease provider returned an invalid contract"
                        )
                decision = evaluate_admission(
                    validated_request,
                    adjusted_snapshot,
                    validated_policy,
                    quota_lease=quota_lease,
                )
                renewed: ResourceReservationLease | None = persisted
                if decision.outcome is AdmissionOutcome.ADMITTED:
                    renewal_now = self._authority_now(connection)
                    self._validate_snapshot(
                        snapshot,
                        now=renewal_now,
                        policy=validated_policy,
                    )
                    current_row = connection.execute(
                        "SELECT * FROM resource_reservation WHERE lease_id = ?",
                        (validated_lease.lease_id,),
                    ).fetchone()
                    if current_row is None or self._lease_from_row(current_row) != persisted:
                        raise RuntimeResourceAdmissionError(
                            "resource reservation changed during recheck"
                        )
                    if persisted.expires_at <= renewal_now:
                        raise RuntimeResourceAdmissionError(
                            "resource reservation expired before renewal commit"
                        )
                    if self._stop_requested(stop_requested):
                        raise RuntimeResourceAdmissionCancelledError(
                            "resource reservation recheck cancelled before renewal"
                        )
                    renewed = ResourceReservationLease(
                        lease_id=persisted.lease_id,
                        identity=persisted.identity,
                        request_hash=persisted.request_hash,
                        expected_memory_bytes=persisted.expected_memory_bytes,
                        expected_disk_bytes=persisted.expected_disk_bytes,
                        expected_quota_units=persisted.expected_quota_units,
                        granted_at=persisted.granted_at,
                        expires_at=self._lease_expiry(
                            renewal_now,
                            lease_seconds=lease_ttl,
                        ),
                    )
                    updated = connection.execute(
                        """
                        UPDATE resource_reservation
                        SET expires_at = ?, last_renewal_operation_id = ?
                        WHERE lease_id = ?
                          AND expires_at = ?
                          AND last_renewal_operation_id = ?
                        """,
                        (
                            self._timestamp(renewed.expires_at),
                            renewal_operation_id,
                            renewed.lease_id,
                            self._timestamp(persisted.expires_at),
                            current_row["last_renewal_operation_id"],
                        ),
                    )
                    if updated.rowcount != 1:
                        raise RuntimeResourceAdmissionError(
                            "resource reservation renewal verification failed"
                        )
                    self._verify_written_lease(
                        connection,
                        expected=renewed,
                        expected_operation_id=renewal_operation_id,
                        operation="renewal",
                    )
                if self._stop_requested(stop_requested):
                    raise RuntimeResourceAdmissionCancelledError(
                        "resource reservation recheck cancelled before commit"
                    )
                connection.commit()
                return ResourceReservationAdmission(
                    decision=decision,
                    request=validated_request,
                    snapshot=adjusted_snapshot,
                    policy=validated_policy,
                    lease=renewed,
                )
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise _reservation_failure("resource reservation recheck failed", exc) from exc

    def release(
        self,
        lease: ResourceReservationLease,
        *,
        identity: ResourceReservationIdentity,
        lock_wait_timeout_seconds: float | None = None,
    ) -> bool:
        validated_lease = ResourceReservationLease.model_validate(lease)
        validated_identity = ResourceReservationIdentity.model_validate(identity)
        if validated_lease.identity != validated_identity:
            raise RuntimeResourceAdmissionError("resource reservation release identity mismatch")
        try:
            with self._connect() as connection:
                self._begin_immediate(
                    connection,
                    lock_wait_timeout_seconds=lock_wait_timeout_seconds,
                    stop_requested=None,
                )
                self._attest_schema(connection)
                self._authority_now(connection)
                row = connection.execute(
                    "SELECT * FROM resource_reservation WHERE lease_id = ?",
                    (validated_lease.lease_id,),
                ).fetchone()
                if row is None:
                    connection.commit()
                    return False
                persisted = self._lease_from_row(row)
                if persisted != validated_lease:
                    raise RuntimeResourceAdmissionError(
                        "resource reservation release lease mismatch"
                    )
                deleted = connection.execute(
                    "DELETE FROM resource_reservation WHERE lease_id = ?",
                    (validated_lease.lease_id,),
                )
                if (
                    deleted.rowcount != 1
                    or connection.execute(
                        "SELECT 1 FROM resource_reservation WHERE lease_id = ?",
                        (validated_lease.lease_id,),
                    ).fetchone()
                    is not None
                ):
                    raise RuntimeResourceAdmissionError(
                        "resource reservation release verification failed"
                    )
                connection.commit()
                return True
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise _reservation_failure("resource reservation release failed", exc) from exc


PersistentResourceReservationStore = SQLiteResourceReservationStore


class ResourceOperationConflictError(RuntimeResourceAdmissionError):
    """A caller reused an operation or effect identity for another request."""


class ResourceOperationKind(StrEnum):
    RESERVE = "reserve"
    RECHECK = "recheck"
    RENEW = "renew"
    RELEASE = "release"


RESOURCE_OPERATION_KEY_PURPOSE = RESOURCE_JOURNAL_SIGNING_PURPOSE
RESOURCE_OPERATION_RECEIPT_NAMESPACE = "rquant-resource-admission-receipt/v1"
_RESOURCE_OPERATION_GENESIS_NAMESPACE = "rquant-resource-admission-genesis/v1"
_RESOURCE_OPERATION_HEAD_NAMESPACE = RESOURCE_JOURNAL_HEAD_NAMESPACE
_RESOURCE_OPERATION_SIGNATURE_ALGORITHM = "ed25519"
_RESOURCE_OPERATION_ZERO_HASH = "0" * 64


class ResourceOperationReceiptSigner(Protocol):
    """Signer identity for resource receipts, genesis, and journal heads."""

    key_id: str
    issuer: str
    key_purpose: str
    signature_algorithm: str
    public_key_fingerprint: str

    def sign(self, *, namespace: str, payload: bytes) -> str: ...


class ResourceOperationReceiptVerifier(Protocol):
    key_id: str
    issuer: str
    key_purpose: str
    signature_algorithm: str
    public_key_fingerprint: str

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool: ...


@dataclass(frozen=True)
class _ResourceOperationVerifierRecord:
    verifier: ResourceOperationReceiptVerifier
    key_id: str
    issuer: str
    key_purpose: str
    signature_algorithm: str
    public_key_fingerprint: str


class ClosedResourceOperationKeyring:
    """Immutable, genesis-bound verifier set for one resource authority role."""

    def __init__(
        self,
        *,
        verifiers: tuple[ResourceOperationReceiptVerifier, ...],
        trusted_issuer: str,
        trusted_role_inventory: TrustedRoleInventory,
    ) -> None:
        issuer = trusted_issuer.strip()
        if not issuer or not verifiers:
            raise ValueError("resource operation trusted issuer and verifiers are required")
        records: dict[str, _ResourceOperationVerifierRecord] = {}
        fingerprints: set[str] = set()
        for verifier in verifiers:
            key_id = verifier.key_id.strip()
            fingerprint = verifier.public_key_fingerprint.strip().lower()
            if key_id in records:
                raise ValueError("resource operation key_id must identify exactly one public key")
            if (
                not key_id
                or verifier.issuer.strip() != issuer
                or verifier.key_purpose.strip() != RESOURCE_OPERATION_KEY_PURPOSE
                or verifier.signature_algorithm.strip() != _RESOURCE_OPERATION_SIGNATURE_ALGORITHM
                or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
                or not callable(verifier.verify)
            ):
                raise ValueError("resource operation verifier identity is invalid")
            if fingerprint in fingerprints:
                raise ValueError("resource operation public key fingerprint must have one key_id")
            records[key_id] = _ResourceOperationVerifierRecord(
                verifier=verifier,
                key_id=key_id,
                issuer=issuer,
                key_purpose=RESOURCE_OPERATION_KEY_PURPOSE,
                signature_algorithm=_RESOURCE_OPERATION_SIGNATURE_ALGORITHM,
                public_key_fingerprint=fingerprint,
            )
            fingerprints.add(fingerprint)

        if not isinstance(trusted_role_inventory, TrustedRoleInventory):
            raise ValueError("resource operation trusted role inventory is required")
        if trusted_role_inventory.fingerprints_for_purpose(
            RESOURCE_OPERATION_KEY_PURPOSE
        ) != frozenset(fingerprints):
            raise ValueError("resource operation verifiers do not match the trusted role inventory")

        self.trusted_issuer = issuer
        self.trusted_role_inventory = trusted_role_inventory
        self._records = records
        self.allowed_public_key_fingerprints = tuple(sorted(fingerprints))
        self.policy_hash = canonical_sha256(
            {
                "algorithm": _RESOURCE_OPERATION_SIGNATURE_ALGORITHM,
                "allowed_namespaces": sorted(
                    (
                        RESOURCE_OPERATION_RECEIPT_NAMESPACE,
                        _RESOURCE_OPERATION_GENESIS_NAMESPACE,
                        _RESOURCE_OPERATION_HEAD_NAMESPACE,
                    )
                ),
                "contract": "rquant-resource-operation-closed-keyring/v1",
                "issuer": issuer,
                "key_purpose": RESOURCE_OPERATION_KEY_PURPOSE,
                "records": [
                    {
                        "key_id": key_id,
                        "public_key_fingerprint": record.public_key_fingerprint,
                    }
                    for key_id, record in sorted(records.items())
                ],
                "trusted_role_inventory_hash": trusted_role_inventory.policy_hash,
            }
        )

    def allows_signer(self, signer: ResourceOperationReceiptSigner) -> bool:
        record = self._records.get(signer.key_id.strip())
        return bool(
            record is not None
            and signer.issuer.strip() == self.trusted_issuer
            and signer.key_purpose.strip() == RESOURCE_OPERATION_KEY_PURPOSE
            and signer.signature_algorithm.strip() == _RESOURCE_OPERATION_SIGNATURE_ALGORITHM
            and signer.public_key_fingerprint.strip().lower() == record.public_key_fingerprint
        )

    def verify(
        self,
        *,
        issuer: str,
        key_id: str,
        key_purpose: str,
        namespace: str,
        signature_algorithm: str,
        public_key_fingerprint: str,
        payload: bytes,
        signature: str,
    ) -> bool:
        record = self._records.get(key_id)
        if (
            record is None
            or namespace
            not in {
                RESOURCE_OPERATION_RECEIPT_NAMESPACE,
                _RESOURCE_OPERATION_GENESIS_NAMESPACE,
                _RESOURCE_OPERATION_HEAD_NAMESPACE,
            }
            or issuer != self.trusted_issuer
            or key_purpose != RESOURCE_OPERATION_KEY_PURPOSE
            or signature_algorithm != _RESOURCE_OPERATION_SIGNATURE_ALGORITHM
            or public_key_fingerprint.lower() != record.public_key_fingerprint
            or record.verifier.key_id.strip() != record.key_id
            or record.verifier.issuer.strip() != record.issuer
            or record.verifier.key_purpose.strip() != record.key_purpose
            or record.verifier.signature_algorithm.strip() != record.signature_algorithm
            or record.verifier.public_key_fingerprint.strip().lower()
            != record.public_key_fingerprint
        ):
            return False
        try:
            return bool(
                record.verifier.verify(
                    namespace=namespace,
                    payload=payload,
                    signature=signature,
                )
            )
        except Exception:
            return False


class ResourceOperationReceipt(RuntimeContractModel):
    """Signed, immutable proof that one resource operation is durably closed."""

    authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    sequence: int = Field(strict=True, ge=1)
    previous_entry_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    operation_id: str = Field(min_length=1, max_length=500)
    effect_key: str = Field(min_length=1, max_length=500)
    kind: ResourceOperationKind
    attempt_identity_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    prior_receipt_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    applied_at: AwareUtcDatetime
    closed: Literal[True] = True
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: str = Field(min_length=1, max_length=200)
    namespace: str = Field(min_length=1, max_length=200)
    signature_algorithm: str = Field(min_length=1, max_length=100)
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        return _resource_operation_json_bytes(
            {
                "applied_at": self.applied_at.isoformat(timespec="microseconds"),
                "attempt_identity_hash": self.attempt_identity_hash,
                "authority_id": self.authority_id,
                "closed": self.closed,
                "contract": "rquant-resource-admission-operation-receipt/v1",
                "effect_key": self.effect_key,
                "issuer": self.issuer,
                "key_id": self.key_id,
                "key_purpose": self.key_purpose,
                "kind": self.kind.value,
                "lineage_id": self.lineage_id,
                "namespace": self.namespace,
                "operation_id": self.operation_id,
                "payload_hash": self.payload_hash,
                "prior_receipt_hash": self.prior_receipt_hash,
                "previous_entry_hash": self.previous_entry_hash,
                "public_key_fingerprint": self.public_key_fingerprint,
                "result_hash": self.result_hash,
                "sequence": self.sequence,
                "signature_algorithm": self.signature_algorithm,
            }
        )

    @property
    def receipt_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class _ResourceOperationState(RuntimeContractModel):
    decision: AdmissionDecision | None = None
    request: AdmissionRequest | None = None
    snapshot: ResourceSnapshot | None = None
    policy: AdmissionPolicy | None = None
    lease: ResourceReservationLease | None = None
    released: bool = False

    @model_validator(mode="after")
    def validate_state(self) -> _ResourceOperationState:
        if self.released:
            if any(
                value is not None
                for value in (self.decision, self.request, self.snapshot, self.policy, self.lease)
            ):
                raise ValueError("released resource operation cannot expose an admission state")
        elif any(
            value is None for value in (self.decision, self.request, self.snapshot, self.policy)
        ):
            raise ValueError("resource admission operation state is incomplete")
        return self


class ResourceOperationResult(_ResourceOperationState):
    receipt: ResourceOperationReceipt


class _ResourceAuthorityGenesis(RuntimeContractModel):
    authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    trusted_issuer: str = Field(min_length=1, max_length=200)
    key_purpose: str = Field(min_length=1, max_length=200)
    receipt_namespace: str = Field(min_length=1, max_length=200)
    genesis_namespace: str = Field(min_length=1, max_length=200)
    head_namespace: str = Field(min_length=1, max_length=200)
    signature_algorithm: str = Field(min_length=1, max_length=100)
    keyring_policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    trusted_role_inventory_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    allowed_public_key_fingerprints: tuple[str, ...]
    mode: Literal["production", "test-standalone"]
    high_water_authority_id: str | None = Field(default=None, max_length=200)
    anti_rollback_root_authority_id: str | None = Field(default=None, max_length=200)
    high_water_verifier_fingerprints: tuple[str, ...] = ()
    initial_key_id: str = Field(min_length=1, max_length=200)
    initial_public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        value = self.model_dump(mode="json", exclude={"signature"})
        value["contract"] = "rquant-resource-admission-authority-genesis/v1"
        return _resource_operation_json_bytes(value)

    @property
    def genesis_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class _ResourceJournalHead(RuntimeContractModel):
    authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    genesis_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    keyring_policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sequence: int = Field(strict=True, ge=0)
    entry_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    materialized_state_root: str = Field(pattern=r"^[0-9a-f]{64}$")
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: str = Field(min_length=1, max_length=200)
    namespace: str = Field(min_length=1, max_length=200)
    signature_algorithm: str = Field(min_length=1, max_length=100)
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        value = self.model_dump(mode="json", exclude={"signature"})
        value["contract"] = "rquant-resource-admission-journal-head/v1"
        return _resource_operation_json_bytes(value)

    @property
    def head_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class _ResourceMaterializedReservation(RuntimeContractModel):
    lease: ResourceReservationLease
    last_renewal_operation_id: str = Field(min_length=1, max_length=500)
    last_effect_operation_id: str = Field(min_length=1, max_length=500)


@dataclass(frozen=True)
class _ResourceJournalAudit:
    genesis: _ResourceAuthorityGenesis
    head: _ResourceJournalHead
    head_json: str
    materialized_reservations: tuple[_ResourceMaterializedReservation, ...]
    receipt_lease_ids: Mapping[str, str]
    receipt_operation_ids: Mapping[str, str]


def _resource_operation_json_bytes(value: Mapping[str, object]) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )


_RESOURCE_OPERATION_SCHEMA_VERSION = 3
_RESOURCE_OPERATION_TABLE = "resource_effect_operation"
_RESOURCE_OPERATION_META_TABLE = "resource_authority_meta"
_RESOURCE_OPERATION_EXPIRY_INDEX = "resource_reservation_expiry_v3_idx"
_RESOURCE_OPERATION_RESERVATION_COLUMNS = (
    *_RESOURCE_RESERVATION_COLUMNS,
    "last_effect_operation_id",
)
_RESOURCE_OPERATION_RESERVATION_TABLE_SQL = f"""
CREATE TABLE {_RESOURCE_RESERVATION_TABLE} (
    lease_id TEXT NOT NULL CHECK (
        length(lease_id) = 64 AND lease_id NOT GLOB '*[^0-9a-f]*'
    ),
    job_id TEXT NOT NULL CHECK (
        length(job_id) = 36 AND job_id NOT GLOB '*[^0-9a-f-]*'
    ),
    run_id TEXT NOT NULL CHECK (
        length(run_id) = 64 AND run_id NOT GLOB '*[^0-9a-f]*'
    ),
    shard_id TEXT NOT NULL CHECK (
        length(shard_id) = 36 AND shard_id NOT GLOB '*[^0-9a-f-]*'
    ),
    attempt_id TEXT NOT NULL CHECK (
        length(attempt_id) = 36 AND attempt_id NOT GLOB '*[^0-9a-f-]*'
    ),
    claim_generation INTEGER NOT NULL CHECK (
        claim_generation BETWEEN 1 AND {MAX_RESOURCE_COUNT}
    ),
    scheduler_fencing_token INTEGER NOT NULL CHECK (
        scheduler_fencing_token BETWEEN 1 AND {MAX_RESOURCE_COUNT}
    ),
    worker_id TEXT NOT NULL CHECK (
        length(worker_id) BETWEEN 1 AND 200 AND worker_id = trim(worker_id)
    ),
    request_hash TEXT NOT NULL CHECK (
        length(request_hash) = 64 AND request_hash NOT GLOB '*[^0-9a-f]*'
    ),
    expected_memory_bytes INTEGER NOT NULL CHECK (
        expected_memory_bytes BETWEEN 0 AND {MAX_RESOURCE_CAPACITY_BYTES}
    ),
    expected_disk_bytes INTEGER NOT NULL CHECK (
        expected_disk_bytes BETWEEN 0 AND {MAX_RESOURCE_CAPACITY_BYTES}
    ),
    expected_quota_units INTEGER NOT NULL CHECK (expected_quota_units = 0),
    granted_at TEXT NOT NULL CHECK (
        length(granted_at) = 32 AND substr(granted_at, 27, 6) = '+00:00'
    ),
    expires_at TEXT NOT NULL CHECK (
        length(expires_at) = 32 AND substr(expires_at, 27, 6) = '+00:00'
    ),
    last_renewal_operation_id TEXT NOT NULL CHECK (
        length(last_renewal_operation_id) BETWEEN 1 AND 500
    ),
    last_effect_operation_id TEXT NOT NULL UNIQUE CHECK (
        length(last_effect_operation_id) BETWEEN 1 AND 500
    ),
    PRIMARY KEY (lease_id),
    CHECK (expires_at > granted_at)
) STRICT, WITHOUT ROWID
""".strip()
_RESOURCE_OPERATION_AUTHORITY_TABLE_SQL = _RESOURCE_RESERVATION_AUTHORITY_TABLE_SQL
_RESOURCE_OPERATION_META_TABLE_SQL = f"""
CREATE TABLE {_RESOURCE_OPERATION_META_TABLE} (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    authority_id TEXT NOT NULL CHECK (
        length(authority_id) BETWEEN 1 AND 200 AND authority_id = trim(authority_id)
    ),
    lineage_id TEXT NOT NULL CHECK (
        length(lineage_id) = 64 AND lineage_id NOT GLOB '*[^0-9a-f]*'
    ),
    trusted_issuer TEXT NOT NULL CHECK (
        length(trusted_issuer) BETWEEN 1 AND 200 AND trusted_issuer = trim(trusted_issuer)
    ),
    key_purpose TEXT NOT NULL CHECK (key_purpose = '{RESOURCE_OPERATION_KEY_PURPOSE}'),
    receipt_namespace TEXT NOT NULL CHECK (
        receipt_namespace = '{RESOURCE_OPERATION_RECEIPT_NAMESPACE}'
    ),
    genesis_namespace TEXT NOT NULL CHECK (
        genesis_namespace = '{_RESOURCE_OPERATION_GENESIS_NAMESPACE}'
    ),
    head_namespace TEXT NOT NULL CHECK (
        head_namespace = '{_RESOURCE_OPERATION_HEAD_NAMESPACE}'
    ),
    signature_algorithm TEXT NOT NULL CHECK (
        signature_algorithm = '{_RESOURCE_OPERATION_SIGNATURE_ALGORITHM}'
    ),
    keyring_policy_hash TEXT NOT NULL CHECK (
        length(keyring_policy_hash) = 64
        AND keyring_policy_hash NOT GLOB '*[^0-9a-f]*'
    ),
    trusted_role_inventory_hash TEXT NOT NULL CHECK (
        length(trusted_role_inventory_hash) = 64
        AND trusted_role_inventory_hash NOT GLOB '*[^0-9a-f]*'
    ),
    allowed_fingerprints_json TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('production', 'test-standalone')),
    high_water_authority_id TEXT,
    anti_rollback_root_authority_id TEXT,
    high_water_verifier_fingerprints_json TEXT NOT NULL,
    genesis_json TEXT NOT NULL,
    genesis_head_json TEXT NOT NULL,
    head_json TEXT NOT NULL,
    active_key_id TEXT NOT NULL CHECK (
        length(active_key_id) BETWEEN 1 AND 200 AND active_key_id = trim(active_key_id)
    ),
    active_public_key_fingerprint TEXT NOT NULL CHECK (
        length(active_public_key_fingerprint) = 64
        AND active_public_key_fingerprint NOT GLOB '*[^0-9a-f]*'
    ),
    prepared_operation_id TEXT,
    prepared_previous_checkpoint_hash TEXT,
    prepared_checkpoint_json TEXT,
    checkpoint_root_json TEXT,
    CHECK (
        (mode = 'production' AND high_water_authority_id IS NOT NULL
            AND anti_rollback_root_authority_id IS NOT NULL)
        OR (mode = 'test-standalone' AND high_water_authority_id IS NULL
            AND anti_rollback_root_authority_id IS NULL)
    ),
    CHECK (
        (prepared_operation_id IS NULL
            AND prepared_previous_checkpoint_hash IS NULL
            AND prepared_checkpoint_json IS NULL)
        OR (prepared_operation_id IS NOT NULL
            AND prepared_previous_checkpoint_hash IS NOT NULL
            AND prepared_checkpoint_json IS NOT NULL)
    )
) STRICT, WITHOUT ROWID
""".strip()
_RESOURCE_OPERATION_TABLE_SQL = f"""
CREATE TABLE {_RESOURCE_OPERATION_TABLE} (
    operation_id TEXT NOT NULL PRIMARY KEY CHECK (
        length(operation_id) BETWEEN 1 AND 500 AND operation_id = trim(operation_id)
    ),
    effect_key TEXT NOT NULL UNIQUE CHECK (
        length(effect_key) BETWEEN 1 AND 500 AND effect_key = trim(effect_key)
    ),
    kind TEXT NOT NULL CHECK (kind IN ('reserve', 'recheck', 'renew', 'release')),
    sequence INTEGER NOT NULL UNIQUE CHECK (sequence >= 1),
    previous_entry_hash TEXT NOT NULL CHECK (
        length(previous_entry_hash) = 64
        AND previous_entry_hash NOT GLOB '*[^0-9a-f]*'
    ),
    attempt_identity_hash TEXT NOT NULL CHECK (
        length(attempt_identity_hash) = 64
        AND attempt_identity_hash NOT GLOB '*[^0-9a-f]*'
    ),
    payload_hash TEXT NOT NULL CHECK (
        length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'
    ),
    result_json TEXT NOT NULL,
    result_hash TEXT NOT NULL CHECK (
        length(result_hash) = 64 AND result_hash NOT GLOB '*[^0-9a-f]*'
    ),
    receipt_json TEXT NOT NULL,
    entry_hash TEXT NOT NULL UNIQUE CHECK (
        length(entry_hash) = 64 AND entry_hash NOT GLOB '*[^0-9a-f]*'
    ),
    head_json TEXT NOT NULL,
    applied_at TEXT NOT NULL CHECK (
        length(applied_at) = 32 AND substr(applied_at, 27, 6) = '+00:00'
    ),
    UNIQUE (operation_id, effect_key)
) STRICT, WITHOUT ROWID
""".strip()
_RESOURCE_OPERATION_INDEX_SQL = f"""
CREATE INDEX {_RESOURCE_OPERATION_EXPIRY_INDEX}
ON {_RESOURCE_RESERVATION_TABLE}(expires_at, lease_id)
""".strip()


class SQLiteResourceAdmissionAuthority:
    """V3 resource-only admission authority with signed, replayable effect receipts.

    It deliberately has no source-quota provider hook.  Quota reservation is a
    separate authority transaction, so resource admission can never create a
    cross-database double write.
    """

    _lock_wait_seconds = staticmethod(SQLiteResourceReservationStore._lock_wait_seconds)
    _stop_requested = staticmethod(SQLiteResourceReservationStore._stop_requested)
    _timestamp = staticmethod(SQLiteResourceReservationStore._timestamp)
    _lease_expiry = staticmethod(SQLiteResourceReservationStore._lease_expiry)
    _validate_lease_seconds = staticmethod(SQLiteResourceReservationStore._validate_lease_seconds)
    _lease_matches_request = staticmethod(SQLiteResourceReservationStore._lease_matches_request)
    _lease_from_row = staticmethod(SQLiteResourceReservationStore._lease_from_row)
    _active_rows = staticmethod(SQLiteResourceReservationStore._active_rows)
    _adjust_snapshot = staticmethod(SQLiteResourceReservationStore._adjust_snapshot)
    _validate_snapshot = staticmethod(SQLiteResourceReservationStore._validate_snapshot)

    def __init__(
        self,
        path: Path,
        *,
        authority_id: str,
        signer: ResourceOperationReceiptSigner,
        keyring: ClosedResourceOperationKeyring,
        high_water_authority: ResourceJournalHighWaterAuthority | None = None,
        mode: Literal["production", "test-standalone"] = "production",
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if mode not in {"production", "test-standalone"}:
            raise RuntimeResourceAdmissionError("resource journal mode is invalid")
        if mode == "production" and high_water_authority is None:
            raise RuntimeResourceAdmissionError(
                "production resource journal requires an external monotonic high-water root"
            )
        if mode == "test-standalone" and high_water_authority is not None:
            raise RuntimeResourceAdmissionError(
                "test-standalone resource journal cannot configure a production root"
            )
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.authority_id = authority_id.strip()
        if not self.authority_id:
            raise ValueError("resource authority_id must be nonempty")
        key_id = signer.key_id.strip()
        if not key_id or not callable(signer.sign) or not keyring.allows_signer(signer):
            raise ValueError(
                "resource operation signer identity is not trusted for resource purpose"
            )
        self._signer = signer
        self._keyring = keyring
        self._key_id = key_id
        self._issuer = signer.issuer.strip()
        self._public_key_fingerprint = signer.public_key_fingerprint.strip().lower()
        self._mode = mode
        self._high_water_authority = high_water_authority
        if high_water_authority is not None:
            cache_authority_id = high_water_authority.authority_id.strip()
            root_authority_id = (high_water_authority.anti_rollback_root_authority_id or "").strip()
            if (
                high_water_authority.mode != "production"
                or not cache_authority_id
                or not root_authority_id
                or len(
                    {
                        self.authority_id,
                        cache_authority_id,
                        root_authority_id,
                    }
                )
                != 3
            ):
                raise RuntimeResourceAdmissionError(
                    "resource journal requires independent production cache "
                    "and anti-rollback root authorities"
                )
            expected_root_fingerprints = keyring.trusted_role_inventory.fingerprints_for_purpose(
                RESOURCE_JOURNAL_HIGH_WATER_PURPOSE
            )
            if high_water_authority.verifier_fingerprints != expected_root_fingerprints:
                raise RuntimeResourceAdmissionError(
                    "resource journal high-water verifier inventory conflicts"
                )
            expected_journal_fingerprints = keyring.trusted_role_inventory.fingerprints_for_purpose(
                RESOURCE_OPERATION_KEY_PURPOSE
            )
            if high_water_authority.journal_verifier_fingerprints != expected_journal_fingerprints:
                raise RuntimeResourceAdmissionError(
                    "resource journal cache verifier inventory conflicts"
                )
            root_path = Path(high_water_authority.storage_path).resolve()
            if root_path == self.path or (
                root_path.exists() and self.path.exists() and os.path.samefile(root_path, self.path)
            ):
                raise RuntimeResourceAdmissionError(
                    "resource journal and high-water authority require independent storage"
                )
        self.clock = clock or _system_clock
        self._initialize()

    @property
    def mode(self) -> Literal["production", "test-standalone"]:
        return self._mode

    @property
    def trusted_role_inventory_hash(self) -> str:
        return self._keyring.trusted_role_inventory.policy_hash

    @property
    def high_water_authority(self) -> ResourceJournalHighWaterAuthority | None:
        return self._high_water_authority

    @classmethod
    def copy_migrate_v2(
        cls,
        source_path: Path,
        destination_path: Path,
        *,
        authority_id: str,
        signer: ResourceOperationReceiptSigner,
        keyring: ClosedResourceOperationKeyring,
        high_water_authority: ResourceJournalHighWaterAuthority | None = None,
        mode: Literal["production", "test-standalone"] = "production",
        clock: Callable[[], datetime] | None = None,
    ) -> SQLiteResourceAdmissionAuthority:
        """Copy an empty v2 authority into a new v3 file without touching the source.

        Active v2 leases lack a signed predecessor receipt.  Refusing those
        migrations prevents a lease from being released or renewed without the
        fence evidence required by v3.
        """

        source = Path(source_path).resolve()
        destination = Path(destination_path).resolve()
        if source == destination:
            raise RuntimeResourceAdmissionError("v2 migration requires a distinct v3 file")
        if destination.exists():
            raise RuntimeResourceAdmissionError("v3 migration destination already exists")
        try:
            with sqlite3.connect(f"file:{source}?mode=ro", uri=True) as connection:
                connection.row_factory = sqlite3.Row
                if (
                    SQLiteResourceReservationStore._schema_pragma(connection, "application_id")
                    != _RESOURCE_RESERVATION_APPLICATION_ID
                    or SQLiteResourceReservationStore._schema_pragma(connection, "user_version")
                    != _RESOURCE_RESERVATION_SCHEMA_VERSION
                ):
                    raise RuntimeResourceAdmissionError(
                        "source is not a recognized resource v2 file"
                    )
                active = connection.execute(
                    f"SELECT COUNT(*) FROM {_RESOURCE_RESERVATION_TABLE}"
                ).fetchone()[0]
                if active != 0:
                    raise RuntimeResourceAdmissionError(
                        "v2 migration requires no active leases because they lack signed receipts"
                    )
                authority = connection.execute(
                    f"""
                    SELECT last_clock_at, last_snapshot_observed_at
                    FROM {_RESOURCE_RESERVATION_AUTHORITY_TABLE}
                    WHERE singleton = 1
                    """
                ).fetchone()
                if authority is None:
                    raise RuntimeResourceAdmissionError(
                        "source resource v2 authority is incomplete"
                    )
                source_clock = authority["last_clock_at"]
                source_snapshot = authority["last_snapshot_observed_at"]
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise RuntimeResourceAdmissionError("resource v2 copy migration failed") from exc
        migrated = cls(
            destination,
            authority_id=authority_id,
            signer=signer,
            keyring=keyring,
            high_water_authority=high_water_authority,
            mode=mode,
            clock=clock,
        )
        try:
            with migrated._connect() as connection:
                migrated._begin_immediate(connection)
                migrated._attest_schema(connection)
                migrated._audit_journal(connection)
                updated = connection.execute(
                    f"""
                    UPDATE {_RESOURCE_RESERVATION_AUTHORITY_TABLE}
                    SET last_clock_at = ?, last_snapshot_observed_at = ?
                    WHERE singleton = 1
                    """,
                    (source_clock, source_snapshot),
                )
                if updated.rowcount != 1:
                    raise RuntimeResourceAdmissionError(
                        "resource v2 copy migration watermark update failed"
                    )
                connection.commit()
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise RuntimeResourceAdmissionError("resource v2 copy migration failed") from exc
        return migrated

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=_RESOURCE_LOCK_POLL_MILLISECONDS / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {_RESOURCE_LOCK_POLL_MILLISECONDS}")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA trusted_schema = OFF")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _begin_immediate(
        self,
        connection: sqlite3.Connection,
        *,
        lock_wait_timeout_seconds: object | None = None,
    ) -> None:
        SQLiteResourceReservationStore._begin_immediate(
            self,
            connection,
            lock_wait_timeout_seconds=lock_wait_timeout_seconds,
            stop_requested=None,
        )

    def _now(self) -> datetime:
        try:
            return normalize_aware_utc(self.clock())
        except Exception as exc:
            raise RuntimeResourceAdmissionError("resource authority clock failed") from exc

    def _authority_now(self, connection: sqlite3.Connection) -> datetime:
        return SQLiteResourceReservationStore._authority_now(self, connection)

    @staticmethod
    def _normalized_schema_sql(value: str | None) -> str:
        return SQLiteResourceReservationStore._normalized_schema_sql(value)

    def _attest_schema(self, connection: sqlite3.Connection) -> None:
        if (
            SQLiteResourceReservationStore._schema_pragma(connection, "application_id")
            != _RESOURCE_RESERVATION_APPLICATION_ID
            or SQLiteResourceReservationStore._schema_pragma(connection, "user_version")
            != _RESOURCE_OPERATION_SCHEMA_VERSION
        ):
            raise RuntimeResourceAdmissionError("resource operation schema identity mismatch")
        expected = {
            ("table", _RESOURCE_RESERVATION_TABLE): _RESOURCE_OPERATION_RESERVATION_TABLE_SQL,
            (
                "table",
                _RESOURCE_RESERVATION_AUTHORITY_TABLE,
            ): _RESOURCE_OPERATION_AUTHORITY_TABLE_SQL,
            ("table", _RESOURCE_OPERATION_META_TABLE): _RESOURCE_OPERATION_META_TABLE_SQL,
            ("table", _RESOURCE_OPERATION_TABLE): _RESOURCE_OPERATION_TABLE_SQL,
            ("index", _RESOURCE_OPERATION_EXPIRY_INDEX): _RESOURCE_OPERATION_INDEX_SQL,
        }
        objects = {
            (row["type"], row["name"]): self._normalized_schema_sql(row["sql"])
            for row in connection.execute(
                """
                SELECT type, name, sql FROM sqlite_schema
                WHERE name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
        }
        if objects != {key: self._normalized_schema_sql(value) for key, value in expected.items()}:
            raise RuntimeResourceAdmissionError(
                "resource operation schema object inventory mismatch"
            )
        expected_columns = {
            _RESOURCE_RESERVATION_TABLE: _RESOURCE_OPERATION_RESERVATION_COLUMNS,
            _RESOURCE_RESERVATION_AUTHORITY_TABLE: (
                "singleton",
                "last_clock_at",
                "last_snapshot_observed_at",
            ),
            _RESOURCE_OPERATION_META_TABLE: (
                "singleton",
                "authority_id",
                "lineage_id",
                "trusted_issuer",
                "key_purpose",
                "receipt_namespace",
                "genesis_namespace",
                "head_namespace",
                "signature_algorithm",
                "keyring_policy_hash",
                "trusted_role_inventory_hash",
                "allowed_fingerprints_json",
                "mode",
                "high_water_authority_id",
                "anti_rollback_root_authority_id",
                "high_water_verifier_fingerprints_json",
                "genesis_json",
                "genesis_head_json",
                "head_json",
                "active_key_id",
                "active_public_key_fingerprint",
                "prepared_operation_id",
                "prepared_previous_checkpoint_hash",
                "prepared_checkpoint_json",
                "checkpoint_root_json",
            ),
            _RESOURCE_OPERATION_TABLE: (
                "operation_id",
                "effect_key",
                "kind",
                "sequence",
                "previous_entry_hash",
                "attempt_identity_hash",
                "payload_hash",
                "result_json",
                "result_hash",
                "receipt_json",
                "entry_hash",
                "head_json",
                "applied_at",
            ),
        }
        for table, columns in expected_columns.items():
            actual = tuple(
                row["name"]
                for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()
            )
            if actual != columns:
                raise RuntimeResourceAdmissionError(
                    "resource operation schema column contract mismatch"
                )
        authority = connection.execute(
            f"""
            SELECT singleton, last_clock_at, last_snapshot_observed_at
            FROM {_RESOURCE_RESERVATION_AUTHORITY_TABLE}
            """
        ).fetchall()
        if len(authority) != 1 or authority[0]["singleton"] != 1:
            raise RuntimeResourceAdmissionError("resource operation authority singleton is invalid")
        try:
            normalize_aware_utc(datetime.fromisoformat(authority[0]["last_clock_at"]))
            snapshot = authority[0]["last_snapshot_observed_at"]
            if snapshot is not None:
                normalize_aware_utc(datetime.fromisoformat(snapshot))
        except Exception as exc:
            raise RuntimeResourceAdmissionError(
                "resource operation authority watermark is invalid"
            ) from exc

    def _initialize(self) -> None:
        try:
            with self._connect() as connection:
                self._begin_immediate(
                    connection, lock_wait_timeout_seconds=_MAX_RESOURCE_LOCK_WAIT_SECONDS
                )
                objects = connection.execute(
                    "SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1"
                ).fetchone()
                application_id = SQLiteResourceReservationStore._schema_pragma(
                    connection, "application_id"
                )
                user_version = SQLiteResourceReservationStore._schema_pragma(
                    connection, "user_version"
                )
                if objects is None and application_id == 0 and user_version == 0:
                    connection.execute(_RESOURCE_OPERATION_RESERVATION_TABLE_SQL)
                    connection.execute(_RESOURCE_OPERATION_AUTHORITY_TABLE_SQL)
                    connection.execute(_RESOURCE_OPERATION_META_TABLE_SQL)
                    connection.execute(_RESOURCE_OPERATION_TABLE_SQL)
                    connection.execute(_RESOURCE_OPERATION_INDEX_SQL)
                    connection.execute(
                        f"""
                        INSERT INTO {_RESOURCE_RESERVATION_AUTHORITY_TABLE}(
                            singleton, last_clock_at, last_snapshot_observed_at
                        ) VALUES (1, ?, NULL)
                        """,
                        (self._timestamp(self._now()),),
                    )
                    connection.execute(
                        f"PRAGMA application_id = {_RESOURCE_RESERVATION_APPLICATION_ID}"
                    )
                    connection.execute(
                        f"PRAGMA user_version = {_RESOURCE_OPERATION_SCHEMA_VERSION}"
                    )
                    self._create_genesis(connection)
                elif (
                    application_id == _RESOURCE_RESERVATION_APPLICATION_ID
                    and user_version == _RESOURCE_RESERVATION_SCHEMA_VERSION
                ):
                    raise RuntimeResourceAdmissionError(
                        "resource v2 file is read-only; use copy_migrate_v2 or a new v3 file"
                    )
                self._attest_schema(connection)
                audit = self._audit_journal(connection)
                self._lineage_id = audit.genesis.lineage_id
                connection.commit()
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise RuntimeResourceAdmissionError(
                "resource operation authority initialization failed"
            ) from exc
        self._reconcile_high_water()

    @staticmethod
    def _contract_json(value: RuntimeContractModel | list[str]) -> str:
        payload: object
        if isinstance(value, RuntimeContractModel):
            payload = value.model_dump(mode="json")
        else:
            payload = value
        return json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )

    def _sign(self, *, namespace: str, payload: bytes) -> str:
        try:
            signature = self._signer.sign(namespace=namespace, payload=payload)
        except Exception as exc:
            raise RuntimeResourceAdmissionError("resource operation signing failed") from exc
        if (
            not isinstance(signature, str)
            or not signature
            or not self._keyring.verify(
                issuer=self._issuer,
                key_id=self._key_id,
                key_purpose=RESOURCE_OPERATION_KEY_PURPOSE,
                namespace=namespace,
                signature_algorithm=_RESOURCE_OPERATION_SIGNATURE_ALGORITHM,
                public_key_fingerprint=self._public_key_fingerprint,
                payload=payload,
                signature=signature,
            )
        ):
            raise RuntimeResourceAdmissionError(
                "resource operation signer returned an unverifiable signature"
            )
        return signature

    def _verify_signature(
        self,
        *,
        issuer: str,
        key_id: str,
        key_purpose: str,
        namespace: str,
        signature_algorithm: str,
        public_key_fingerprint: str,
        payload: bytes,
        signature: str,
    ) -> bool:
        return self._keyring.verify(
            issuer=issuer,
            key_id=key_id,
            key_purpose=key_purpose,
            namespace=namespace,
            signature_algorithm=signature_algorithm,
            public_key_fingerprint=public_key_fingerprint,
            payload=payload,
            signature=signature,
        )

    def _sign_head(
        self,
        *,
        genesis: _ResourceAuthorityGenesis,
        sequence: int,
        entry_hash: str,
        previous_head_hash: str,
        materialized_state_root: str,
    ) -> _ResourceJournalHead:
        unsigned = _ResourceJournalHead(
            authority_id=self.authority_id,
            lineage_id=genesis.lineage_id,
            genesis_hash=genesis.genesis_hash,
            keyring_policy_hash=self._keyring.policy_hash,
            sequence=sequence,
            entry_hash=entry_hash,
            previous_head_hash=previous_head_hash,
            materialized_state_root=materialized_state_root,
            issuer=self._issuer,
            key_id=self._key_id,
            key_purpose=RESOURCE_OPERATION_KEY_PURPOSE,
            namespace=_RESOURCE_OPERATION_HEAD_NAMESPACE,
            signature_algorithm=_RESOURCE_OPERATION_SIGNATURE_ALGORITHM,
            public_key_fingerprint=self._public_key_fingerprint,
            signature="pending",
        )
        return unsigned.model_copy(
            update={
                "signature": self._sign(
                    namespace=_RESOURCE_OPERATION_HEAD_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )

    def _high_water_checkpoint(
        self,
        head: _ResourceJournalHead,
        *,
        head_json: str,
    ) -> ResourceJournalHighWaterCheckpoint:
        return ResourceJournalHighWaterCheckpoint(
            schema_version=1,
            contract="rquant-resource-journal-high-water-checkpoint/v1",
            journal_authority_id=self.authority_id,
            lineage_id=head.lineage_id,
            sequence=head.sequence,
            previous_head_hash=head.previous_head_hash,
            head_hash=head.head_hash,
            materialized_state_root=head.materialized_state_root,
            signed_head_json=head_json,
        )

    @staticmethod
    def _high_water_operation_id(checkpoint: ResourceJournalHighWaterCheckpoint) -> str:
        return canonical_sha256(
            {
                "checkpoint_hash": checkpoint.checkpoint_hash,
                "contract": "rquant-resource-journal-high-water-operation/v1",
                "journal_authority_id": checkpoint.journal_authority_id,
                "lineage_id": checkpoint.lineage_id,
                "sequence": checkpoint.sequence,
            }
        )

    @staticmethod
    def _materialized_state_root(
        reservations: tuple[_ResourceMaterializedReservation, ...],
    ) -> str:
        return canonical_sha256(
            [reservation.model_dump(mode="json") for reservation in reservations]
        )

    def _read_materialized_reservations(
        self,
        connection: sqlite3.Connection,
    ) -> tuple[_ResourceMaterializedReservation, ...]:
        records: list[_ResourceMaterializedReservation] = []
        for row in connection.execute(
            f"SELECT * FROM {_RESOURCE_RESERVATION_TABLE} ORDER BY lease_id"
        ).fetchall():
            lease = self._lease_from_row(row)
            if lease.lease_id != row["lease_id"]:
                raise RuntimeResourceAdmissionError(
                    "resource materialized lease identity is invalid"
                )
            try:
                records.append(
                    _ResourceMaterializedReservation(
                        lease=lease,
                        last_renewal_operation_id=row["last_renewal_operation_id"],
                        last_effect_operation_id=row["last_effect_operation_id"],
                    )
                )
            except Exception as exc:
                raise RuntimeResourceAdmissionError(
                    "resource materialized lease row is malformed"
                ) from exc
        return tuple(records)

    @staticmethod
    def _reduce_materialized_reservation(
        *,
        materialized: dict[str, _ResourceMaterializedReservation],
        receipt_lease_ids: dict[str, str],
        receipt_operation_ids: dict[str, str],
        result: ResourceOperationResult,
    ) -> None:
        receipt = result.receipt
        prior_hash = receipt.prior_receipt_hash
        if result.released:
            lease_id = None if prior_hash is None else receipt_lease_ids.get(prior_hash)
            prior_operation_id = (
                None if prior_hash is None else receipt_operation_ids.get(prior_hash)
            )
            current = None if lease_id is None else materialized.get(lease_id)
            if (
                receipt.kind is not ResourceOperationKind.RELEASE
                or current is None
                or current.last_effect_operation_id != prior_operation_id
            ):
                raise RuntimeResourceAdmissionError(
                    "resource journal release reducer tombstone is invalid"
                )
            del materialized[lease_id]
            return
        lease = result.lease
        if lease is None:
            if receipt.kind is not ResourceOperationKind.RESERVE:
                raise RuntimeResourceAdmissionError(
                    "resource journal reducer operation lost its bound lease"
                )
            return
        current = materialized.get(lease.lease_id)
        if receipt.kind is ResourceOperationKind.RESERVE:
            if current is not None or prior_hash is not None:
                raise RuntimeResourceAdmissionError(
                    "resource journal reserve reducer conflicts with materialized state"
                )
            last_renewal_operation_id = receipt.operation_id
        else:
            prior_lease_id = None if prior_hash is None else receipt_lease_ids.get(prior_hash)
            prior_operation_id = (
                None if prior_hash is None else receipt_operation_ids.get(prior_hash)
            )
            if (
                current is None
                or prior_lease_id != lease.lease_id
                or current.last_effect_operation_id != prior_operation_id
            ):
                raise RuntimeResourceAdmissionError(
                    "resource journal reducer prior receipt fence is invalid"
                )
            last_renewal_operation_id = current.last_renewal_operation_id
            if result.decision is not None and result.decision.outcome is AdmissionOutcome.ADMITTED:
                last_renewal_operation_id = receipt.operation_id
        materialized[lease.lease_id] = _ResourceMaterializedReservation(
            lease=lease,
            last_renewal_operation_id=last_renewal_operation_id,
            last_effect_operation_id=receipt.operation_id,
        )
        receipt_lease_ids[receipt.receipt_hash] = lease.lease_id
        receipt_operation_ids[receipt.receipt_hash] = receipt.operation_id

    @staticmethod
    def _ordered_materialized_reservations(
        materialized: Mapping[str, _ResourceMaterializedReservation],
    ) -> tuple[_ResourceMaterializedReservation, ...]:
        return tuple(materialized[lease_id] for lease_id in sorted(materialized))

    def _create_genesis(self, connection: sqlite3.Connection) -> None:
        lineage_id = os.urandom(32).hex()
        high_water = self._high_water_authority
        high_water_authority_id = None if high_water is None else high_water.authority_id
        anti_rollback_root_authority_id = (
            None if high_water is None else high_water.anti_rollback_root_authority_id
        )
        high_water_fingerprints = (
            () if high_water is None else tuple(sorted(high_water.verifier_fingerprints))
        )
        unsigned = _ResourceAuthorityGenesis(
            authority_id=self.authority_id,
            lineage_id=lineage_id,
            trusted_issuer=self._keyring.trusted_issuer,
            key_purpose=RESOURCE_OPERATION_KEY_PURPOSE,
            receipt_namespace=RESOURCE_OPERATION_RECEIPT_NAMESPACE,
            genesis_namespace=_RESOURCE_OPERATION_GENESIS_NAMESPACE,
            head_namespace=_RESOURCE_OPERATION_HEAD_NAMESPACE,
            signature_algorithm=_RESOURCE_OPERATION_SIGNATURE_ALGORITHM,
            keyring_policy_hash=self._keyring.policy_hash,
            trusted_role_inventory_hash=self._keyring.trusted_role_inventory.policy_hash,
            allowed_public_key_fingerprints=self._keyring.allowed_public_key_fingerprints,
            mode=self._mode,
            high_water_authority_id=high_water_authority_id,
            anti_rollback_root_authority_id=anti_rollback_root_authority_id,
            high_water_verifier_fingerprints=high_water_fingerprints,
            initial_key_id=self._key_id,
            initial_public_key_fingerprint=self._public_key_fingerprint,
            issuer=self._issuer,
            key_id=self._key_id,
            public_key_fingerprint=self._public_key_fingerprint,
            signature="pending",
        )
        genesis = unsigned.model_copy(
            update={
                "signature": self._sign(
                    namespace=_RESOURCE_OPERATION_GENESIS_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )
        initial_head = self._sign_head(
            genesis=genesis,
            sequence=0,
            entry_hash=_RESOURCE_OPERATION_ZERO_HASH,
            previous_head_hash=_RESOURCE_OPERATION_ZERO_HASH,
            materialized_state_root=canonical_sha256([]),
        )
        allowed_json = self._contract_json(list(self._keyring.allowed_public_key_fingerprints))
        genesis_json = self._contract_json(genesis)
        initial_head_json = self._contract_json(initial_head)
        high_water_fingerprints_json = self._contract_json(list(high_water_fingerprints))
        checkpoint = self._high_water_checkpoint(initial_head, head_json=initial_head_json)
        prepared_operation_id = (
            None if self._mode == "test-standalone" else self._high_water_operation_id(checkpoint)
        )
        prepared_previous_checkpoint_hash = (
            None if prepared_operation_id is None else _RESOURCE_OPERATION_ZERO_HASH
        )
        prepared_checkpoint_json = (
            None if prepared_operation_id is None else self._contract_json(checkpoint)
        )
        connection.execute(
            f"""
            INSERT INTO {_RESOURCE_OPERATION_META_TABLE}(
                singleton, authority_id, lineage_id, trusted_issuer, key_purpose,
                receipt_namespace, genesis_namespace, head_namespace,
                signature_algorithm, keyring_policy_hash, trusted_role_inventory_hash,
                allowed_fingerprints_json, mode, high_water_authority_id,
                anti_rollback_root_authority_id,
                high_water_verifier_fingerprints_json, genesis_json,
                genesis_head_json, head_json, active_key_id,
                active_public_key_fingerprint, prepared_operation_id,
                prepared_previous_checkpoint_hash, prepared_checkpoint_json,
                checkpoint_root_json
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.authority_id,
                lineage_id,
                self._keyring.trusted_issuer,
                RESOURCE_OPERATION_KEY_PURPOSE,
                RESOURCE_OPERATION_RECEIPT_NAMESPACE,
                _RESOURCE_OPERATION_GENESIS_NAMESPACE,
                _RESOURCE_OPERATION_HEAD_NAMESPACE,
                _RESOURCE_OPERATION_SIGNATURE_ALGORITHM,
                self._keyring.policy_hash,
                self._keyring.trusted_role_inventory.policy_hash,
                allowed_json,
                self._mode,
                high_water_authority_id,
                anti_rollback_root_authority_id,
                high_water_fingerprints_json,
                genesis_json,
                initial_head_json,
                initial_head_json,
                self._key_id,
                self._public_key_fingerprint,
                prepared_operation_id,
                prepared_previous_checkpoint_hash,
                prepared_checkpoint_json,
                None,
            ),
        )

    def _validate_genesis(
        self,
        row: sqlite3.Row,
    ) -> _ResourceAuthorityGenesis:
        allowed_json = self._contract_json(list(self._keyring.allowed_public_key_fingerprints))
        high_water = self._high_water_authority
        high_water_authority_id = None if high_water is None else high_water.authority_id
        anti_rollback_root_authority_id = (
            None if high_water is None else high_water.anti_rollback_root_authority_id
        )
        high_water_fingerprints = (
            () if high_water is None else tuple(sorted(high_water.verifier_fingerprints))
        )
        high_water_fingerprints_json = self._contract_json(list(high_water_fingerprints))
        static_values = (
            row["authority_id"],
            row["trusted_issuer"],
            row["key_purpose"],
            row["receipt_namespace"],
            row["genesis_namespace"],
            row["head_namespace"],
            row["signature_algorithm"],
            row["keyring_policy_hash"],
            row["trusted_role_inventory_hash"],
            row["allowed_fingerprints_json"],
            row["mode"],
            row["high_water_authority_id"],
            row["anti_rollback_root_authority_id"],
            row["high_water_verifier_fingerprints_json"],
        )
        if static_values != (
            self.authority_id,
            self._keyring.trusted_issuer,
            RESOURCE_OPERATION_KEY_PURPOSE,
            RESOURCE_OPERATION_RECEIPT_NAMESPACE,
            _RESOURCE_OPERATION_GENESIS_NAMESPACE,
            _RESOURCE_OPERATION_HEAD_NAMESPACE,
            _RESOURCE_OPERATION_SIGNATURE_ALGORITHM,
            self._keyring.policy_hash,
            self._keyring.trusted_role_inventory.policy_hash,
            allowed_json,
            self._mode,
            high_water_authority_id,
            anti_rollback_root_authority_id,
            high_water_fingerprints_json,
        ):
            raise RuntimeResourceAdmissionError(
                "resource authority trusted genesis policy conflicts"
            )
        try:
            genesis = _ResourceAuthorityGenesis.model_validate_json(row["genesis_json"])
        except Exception as exc:
            raise RuntimeResourceAdmissionError("resource authority genesis is malformed") from exc
        if (
            genesis.authority_id != self.authority_id
            or genesis.lineage_id != row["lineage_id"]
            or genesis.trusted_issuer != self._keyring.trusted_issuer
            or genesis.key_purpose != RESOURCE_OPERATION_KEY_PURPOSE
            or genesis.receipt_namespace != RESOURCE_OPERATION_RECEIPT_NAMESPACE
            or genesis.genesis_namespace != _RESOURCE_OPERATION_GENESIS_NAMESPACE
            or genesis.head_namespace != _RESOURCE_OPERATION_HEAD_NAMESPACE
            or genesis.signature_algorithm != _RESOURCE_OPERATION_SIGNATURE_ALGORITHM
            or genesis.keyring_policy_hash != self._keyring.policy_hash
            or genesis.trusted_role_inventory_hash
            != self._keyring.trusted_role_inventory.policy_hash
            or genesis.allowed_public_key_fingerprints
            != self._keyring.allowed_public_key_fingerprints
            or genesis.mode != self._mode
            or genesis.high_water_authority_id != high_water_authority_id
            or genesis.anti_rollback_root_authority_id != anti_rollback_root_authority_id
            or genesis.high_water_verifier_fingerprints != high_water_fingerprints
            or genesis.issuer != self._keyring.trusted_issuer
            or genesis.key_id != genesis.initial_key_id
            or genesis.public_key_fingerprint != genesis.initial_public_key_fingerprint
            or not self._verify_signature(
                issuer=genesis.issuer,
                key_id=genesis.key_id,
                key_purpose=genesis.key_purpose,
                namespace=genesis.genesis_namespace,
                signature_algorithm=genesis.signature_algorithm,
                public_key_fingerprint=genesis.public_key_fingerprint,
                payload=genesis.signing_bytes(),
                signature=genesis.signature,
            )
        ):
            raise RuntimeResourceAdmissionError(
                "resource authority genesis signature or identity is invalid"
            )
        return genesis

    def _validate_head(
        self,
        head_json: str,
        *,
        genesis: _ResourceAuthorityGenesis,
        sequence: int,
        entry_hash: str,
        previous_head_hash: str,
        materialized_state_root: str | None = None,
    ) -> _ResourceJournalHead:
        try:
            head = _ResourceJournalHead.model_validate_json(head_json)
        except Exception as exc:
            raise RuntimeResourceAdmissionError("resource journal head is malformed") from exc
        if (
            head.authority_id != self.authority_id
            or head.lineage_id != genesis.lineage_id
            or head.genesis_hash != genesis.genesis_hash
            or head.keyring_policy_hash != self._keyring.policy_hash
            or head.sequence != sequence
            or head.entry_hash != entry_hash
            or head.previous_head_hash != previous_head_hash
            or (
                materialized_state_root is not None
                and head.materialized_state_root != materialized_state_root
            )
            or head.issuer != self._keyring.trusted_issuer
            or head.key_purpose != RESOURCE_OPERATION_KEY_PURPOSE
            or head.namespace != _RESOURCE_OPERATION_HEAD_NAMESPACE
            or head.signature_algorithm != _RESOURCE_OPERATION_SIGNATURE_ALGORITHM
            or not self._verify_signature(
                issuer=head.issuer,
                key_id=head.key_id,
                key_purpose=head.key_purpose,
                namespace=head.namespace,
                signature_algorithm=head.signature_algorithm,
                public_key_fingerprint=head.public_key_fingerprint,
                payload=head.signing_bytes(),
                signature=head.signature,
            )
        ):
            raise RuntimeResourceAdmissionError("resource journal signed head integrity failed")
        return head

    @staticmethod
    def _entry_hash(
        row: sqlite3.Row | Mapping[str, object],
        *,
        lineage_id: str,
    ) -> str:
        return canonical_sha256(
            {
                "applied_at": row["applied_at"],
                "attempt_identity_hash": row["attempt_identity_hash"],
                "contract": "rquant-resource-admission-journal-entry/v1",
                "effect_key": row["effect_key"],
                "kind": row["kind"],
                "lineage_id": lineage_id,
                "operation_id": row["operation_id"],
                "payload_hash": row["payload_hash"],
                "previous_entry_hash": row["previous_entry_hash"],
                "receipt_json": row["receipt_json"],
                "result_hash": row["result_hash"],
                "result_json": row["result_json"],
                "sequence": row["sequence"],
            }
        )

    def _audit_journal(self, connection: sqlite3.Connection) -> _ResourceJournalAudit:
        meta_rows = connection.execute(f"SELECT * FROM {_RESOURCE_OPERATION_META_TABLE}").fetchall()
        if len(meta_rows) != 1 or meta_rows[0]["singleton"] != 1:
            raise RuntimeResourceAdmissionError("resource authority genesis meta is invalid")
        meta = meta_rows[0]
        genesis = self._validate_genesis(meta)
        genesis_head = self._validate_head(
            meta["genesis_head_json"],
            genesis=genesis,
            sequence=0,
            entry_hash=_RESOURCE_OPERATION_ZERO_HASH,
            previous_head_hash=_RESOURCE_OPERATION_ZERO_HASH,
            materialized_state_root=canonical_sha256([]),
        )
        previous_entry_hash = _RESOURCE_OPERATION_ZERO_HASH
        previous_head = genesis_head
        previous_head_json = meta["genesis_head_json"]
        materialized: dict[str, _ResourceMaterializedReservation] = {}
        receipt_lease_ids: dict[str, str] = {}
        receipt_operation_ids: dict[str, str] = {}
        rows = connection.execute(
            f"SELECT * FROM {_RESOURCE_OPERATION_TABLE} ORDER BY sequence"
        ).fetchall()
        for expected_sequence, row in enumerate(rows, start=1):
            if (
                row["sequence"] != expected_sequence
                or row["previous_entry_hash"] != previous_entry_hash
            ):
                raise RuntimeResourceAdmissionError(
                    "resource operation journal chain sequence is invalid"
                )
            result = self._journal_result(row, lineage_id=genesis.lineage_id)
            self._reduce_materialized_reservation(
                materialized=materialized,
                receipt_lease_ids=receipt_lease_ids,
                receipt_operation_ids=receipt_operation_ids,
                result=result,
            )
            expected_materialized = self._ordered_materialized_reservations(materialized)
            expected_materialized_root = self._materialized_state_root(expected_materialized)
            entry_hash = self._entry_hash(row, lineage_id=genesis.lineage_id)
            if row["entry_hash"] != entry_hash:
                raise RuntimeResourceAdmissionError(
                    "resource operation journal entry integrity failed"
                )
            head = self._validate_head(
                row["head_json"],
                genesis=genesis,
                sequence=expected_sequence,
                entry_hash=entry_hash,
                previous_head_hash=previous_head.head_hash,
                materialized_state_root=expected_materialized_root,
            )
            previous_entry_hash = entry_hash
            previous_head = head
            previous_head_json = row["head_json"]
        if (
            meta["head_json"] != previous_head_json
            or meta["active_key_id"] != previous_head.key_id
            or meta["active_public_key_fingerprint"] != previous_head.public_key_fingerprint
        ):
            raise RuntimeResourceAdmissionError(
                "resource operation journal head does not attest the full chain"
            )
        expected_materialized = self._ordered_materialized_reservations(materialized)
        actual_materialized = self._read_materialized_reservations(connection)
        if actual_materialized != expected_materialized:
            raise RuntimeResourceAdmissionError(
                "resource operation materialized state diverges from journal reducer"
            )
        self._validate_local_high_water_meta(
            meta,
            head=previous_head,
            head_json=previous_head_json,
        )
        self._lineage_id = genesis.lineage_id
        return _ResourceJournalAudit(
            genesis=genesis,
            head=previous_head,
            head_json=previous_head_json,
            materialized_reservations=expected_materialized,
            receipt_lease_ids=dict(receipt_lease_ids),
            receipt_operation_ids=dict(receipt_operation_ids),
        )

    def _validate_local_high_water_meta(
        self,
        meta: sqlite3.Row,
        *,
        head: _ResourceJournalHead,
        head_json: str,
    ) -> None:
        prepared_values = (
            meta["prepared_operation_id"],
            meta["prepared_previous_checkpoint_hash"],
            meta["prepared_checkpoint_json"],
        )
        if self._mode == "test-standalone":
            if prepared_values != (None, None, None) or meta["checkpoint_root_json"] is not None:
                raise RuntimeResourceAdmissionError(
                    "test-standalone resource journal contains production high-water state"
                )
            return
        high_water = self._high_water_authority
        if high_water is None or high_water.anti_rollback_root_authority_id is None:
            raise RuntimeResourceAdmissionError(
                "production resource journal anti-rollback root is missing"
            )
        checkpoint = self._high_water_checkpoint(head, head_json=head_json)
        if prepared_values[0] is not None:
            try:
                prepared = ResourceJournalHighWaterCheckpoint.model_validate_json(
                    prepared_values[2]
                )
            except Exception as exc:
                raise RuntimeResourceAdmissionError(
                    "resource journal prepared high-water checkpoint is malformed"
                ) from exc
            if (
                prepared != checkpoint
                or prepared_values[0] != self._high_water_operation_id(prepared)
                or not isinstance(prepared_values[1], str)
                or re.fullmatch(r"[0-9a-f]{64}", prepared_values[1]) is None
            ):
                raise RuntimeResourceAdmissionError(
                    "resource journal prepared high-water checkpoint integrity failed"
                )
            if head.sequence == 0:
                if (
                    prepared_values[1] != _RESOURCE_OPERATION_ZERO_HASH
                    or meta["checkpoint_root_json"] is not None
                ):
                    raise RuntimeResourceAdmissionError(
                        "resource journal genesis high-water prepare is invalid"
                    )
                return
            try:
                previous_root = ResourceJournalAntiRollbackReceipt.model_validate_json(
                    meta["checkpoint_root_json"]
                )
            except Exception as exc:
                raise RuntimeResourceAdmissionError(
                    "resource journal previous high-water root is malformed"
                ) from exc
            if (
                self._contract_json(previous_root) != meta["checkpoint_root_json"]
                or previous_root.root_authority_id != high_water.anti_rollback_root_authority_id
                or previous_root.high_water_authority_id != high_water.authority_id
                or previous_root.journal_authority_id != self.authority_id
                or previous_root.checkpoint.lineage_id != head.lineage_id
                or previous_root.checkpoint.sequence != head.sequence - 1
                or previous_root.checkpoint.head_hash != head.previous_head_hash
                or previous_root.checkpoint.checkpoint_hash != prepared_values[1]
            ):
                raise RuntimeResourceAdmissionError(
                    "resource journal previous high-water root conflicts"
                )
            return
        try:
            root = ResourceJournalAntiRollbackReceipt.model_validate_json(
                meta["checkpoint_root_json"]
            )
        except Exception as exc:
            raise RuntimeResourceAdmissionError(
                "resource journal checkpointed high-water root is missing or malformed"
            ) from exc
        if (
            self._contract_json(root) != meta["checkpoint_root_json"]
            or root.root_authority_id != high_water.anti_rollback_root_authority_id
            or root.high_water_authority_id != high_water.authority_id
            or root.journal_authority_id != self.authority_id
            or root.checkpoint != checkpoint
        ):
            raise RuntimeResourceAdmissionError(
                "resource journal checkpointed high-water root conflicts"
            )

    def _reconcile_high_water(self) -> None:
        if self._mode == "test-standalone":
            return
        high_water = self._high_water_authority
        if high_water is None:
            raise RuntimeResourceAdmissionError(
                "production resource journal high-water authority is missing"
            )
        with self._connect() as connection:
            self._begin_immediate(connection)
            self._attest_schema(connection)
            audit = self._audit_journal(connection)
            meta = connection.execute(
                f"SELECT * FROM {_RESOURCE_OPERATION_META_TABLE} WHERE singleton = 1"
            ).fetchone()
            if meta is None:
                raise RuntimeResourceAdmissionError("resource journal high-water meta is missing")
            prepared_operation_id = meta["prepared_operation_id"]
            checkpoint_root_json = meta["checkpoint_root_json"]
            if prepared_operation_id is None:
                local_root = ResourceJournalAntiRollbackReceipt.model_validate_json(
                    checkpoint_root_json
                )
                connection.rollback()
                try:
                    external_root = high_water.current(journal_authority_id=self.authority_id)
                except ResourceJournalHighWaterError as exc:
                    raise RuntimeResourceAdmissionError(
                        "resource journal external high-water read failed"
                    ) from exc
                if external_root != local_root:
                    raise RuntimeResourceAdmissionError(
                        "resource journal external high-water rejected rollback or donor state"
                    )
                return
            prepared_previous = str(meta["prepared_previous_checkpoint_hash"])
            prepared_checkpoint_json = str(meta["prepared_checkpoint_json"])
            checkpoint = ResourceJournalHighWaterCheckpoint.model_validate_json(
                prepared_checkpoint_json
            )
            expected_head_json = audit.head_json
            connection.rollback()

        try:
            if checkpoint.sequence == 0:
                root = high_water.pin(
                    operation_id=str(prepared_operation_id),
                    journal_authority_id=self.authority_id,
                    checkpoint=checkpoint,
                )
            else:
                root = high_water.compare_and_advance(
                    operation_id=str(prepared_operation_id),
                    journal_authority_id=self.authority_id,
                    previous_checkpoint_hash=prepared_previous,
                    checkpoint=checkpoint,
                )
        except ResourceJournalHighWaterError as exc:
            raise RuntimeResourceAdmissionError(
                "resource journal external high-water compare-and-advance failed"
            ) from exc
        if (
            root.root_authority_id != high_water.anti_rollback_root_authority_id
            or root.high_water_authority_id != high_water.authority_id
            or root.journal_authority_id != self.authority_id
            or root.operation_id != prepared_operation_id
            or root.previous_checkpoint_hash != prepared_previous
            or root.checkpoint != checkpoint
        ):
            raise RuntimeResourceAdmissionError(
                "resource journal external high-water returned a conflicting root"
            )

        root_json = self._contract_json(root)
        with self._connect() as connection:
            self._begin_immediate(connection)
            self._attest_schema(connection)
            self._audit_journal(connection)
            updated = connection.execute(
                f"""
                UPDATE {_RESOURCE_OPERATION_META_TABLE}
                SET prepared_operation_id = NULL,
                    prepared_previous_checkpoint_hash = NULL,
                    prepared_checkpoint_json = NULL,
                    checkpoint_root_json = ?
                WHERE singleton = 1
                    AND prepared_operation_id = ?
                    AND prepared_previous_checkpoint_hash = ?
                    AND prepared_checkpoint_json = ?
                    AND head_json = ?
                """,
                (
                    root_json,
                    prepared_operation_id,
                    prepared_previous,
                    prepared_checkpoint_json,
                    expected_head_json,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeResourceAdmissionError(
                    "resource journal local high-water checkpoint compare-and-swap failed"
                )
            self._audit_journal(connection)
            connection.commit()

    @staticmethod
    def _require_operation_id(value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 500:
            raise RuntimeResourceAdmissionError("resource operation_id is invalid")
        return normalized

    @staticmethod
    def _no_quota_side_effects(request: AdmissionRequest) -> None:
        if request.expected_quota_units != 0:
            raise RuntimeResourceAdmissionError(
                "resource operation v3 prohibits source quota side effects"
            )

    def _active_materialized_rows(
        self,
        connection: sqlite3.Connection,
        *,
        now: datetime,
        excluded_lease_id: str | None = None,
    ) -> tuple[sqlite3.Row, ...]:
        parameters: tuple[object, ...]
        if excluded_lease_id is None:
            where = "expires_at > ?"
            parameters = (
                self._timestamp(now),
                _MAX_ACTIVE_RESOURCE_RESERVATIONS + 1,
            )
        else:
            where = "expires_at > ? AND lease_id <> ?"
            parameters = (
                self._timestamp(now),
                excluded_lease_id,
                _MAX_ACTIVE_RESOURCE_RESERVATIONS + 1,
            )
        rows = connection.execute(
            f"""
            SELECT * FROM {_RESOURCE_RESERVATION_TABLE}
            WHERE {where}
            ORDER BY lease_id
            LIMIT ?
            """,
            parameters,
        ).fetchall()
        if len(rows) > _MAX_ACTIVE_RESOURCE_RESERVATIONS:
            raise RuntimeResourceAdmissionError("active resource reservation budget exceeded")
        return tuple(rows)

    def _sign_receipt(
        self,
        *,
        sequence: int,
        previous_entry_hash: str,
        operation_id: str,
        effect_key: str,
        kind: ResourceOperationKind,
        attempt_identity_hash: str,
        payload_hash: str,
        result_hash: str,
        prior_receipt_hash: str | None,
        applied_at: datetime,
    ) -> ResourceOperationReceipt:
        unsigned = ResourceOperationReceipt(
            authority_id=self.authority_id,
            lineage_id=self._lineage_id,
            sequence=sequence,
            previous_entry_hash=previous_entry_hash,
            operation_id=operation_id,
            effect_key=effect_key,
            kind=kind,
            attempt_identity_hash=attempt_identity_hash,
            payload_hash=payload_hash,
            result_hash=result_hash,
            prior_receipt_hash=prior_receipt_hash,
            applied_at=applied_at,
            issuer=self._issuer,
            key_id=self._key_id,
            key_purpose=RESOURCE_OPERATION_KEY_PURPOSE,
            namespace=RESOURCE_OPERATION_RECEIPT_NAMESPACE,
            signature_algorithm=_RESOURCE_OPERATION_SIGNATURE_ALGORITHM,
            public_key_fingerprint=self._public_key_fingerprint,
            signature="pending",
        )
        return unsigned.model_copy(
            update={
                "signature": self._sign(
                    namespace=RESOURCE_OPERATION_RECEIPT_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )

    def _journal_result(
        self,
        row: sqlite3.Row,
        *,
        lineage_id: str | None = None,
    ) -> ResourceOperationResult:
        try:
            receipt = ResourceOperationReceipt.model_validate_json(row["receipt_json"])
            state = _ResourceOperationState.model_validate_json(row["result_json"])
        except Exception as exc:
            raise RuntimeResourceAdmissionError("resource operation journal is malformed") from exc
        if (
            receipt.authority_id != self.authority_id
            or receipt.lineage_id != (lineage_id or self._lineage_id)
            or receipt.sequence != row["sequence"]
            or receipt.previous_entry_hash != row["previous_entry_hash"]
            or receipt.operation_id != row["operation_id"]
            or receipt.effect_key != row["effect_key"]
            or receipt.kind.value != row["kind"]
            or receipt.attempt_identity_hash != row["attempt_identity_hash"]
            or receipt.payload_hash != row["payload_hash"]
            or receipt.result_hash != row["result_hash"]
            or self._timestamp(receipt.applied_at) != row["applied_at"]
            or receipt.issuer != self._keyring.trusted_issuer
            or receipt.key_purpose != RESOURCE_OPERATION_KEY_PURPOSE
            or receipt.namespace != RESOURCE_OPERATION_RECEIPT_NAMESPACE
            or receipt.signature_algorithm != _RESOURCE_OPERATION_SIGNATURE_ALGORITHM
            or not self._verify_signature(
                issuer=receipt.issuer,
                key_id=receipt.key_id,
                key_purpose=receipt.key_purpose,
                namespace=receipt.namespace,
                signature_algorithm=receipt.signature_algorithm,
                public_key_fingerprint=receipt.public_key_fingerprint,
                payload=receipt.signing_bytes(),
                signature=receipt.signature,
            )
            or canonical_sha256(state) != row["result_hash"]
        ):
            raise RuntimeResourceAdmissionError(
                "resource operation journal receipt integrity failed"
            )
        return ResourceOperationResult(receipt=receipt, **state.model_dump(mode="python"))

    def _prior_receipt(
        self,
        connection: sqlite3.Connection,
        *,
        receipt: ResourceOperationReceipt,
        lease: ResourceReservationLease,
        identity: ResourceReservationIdentity,
        now: datetime,
    ) -> str:
        if lease.identity != identity:
            raise RuntimeResourceAdmissionError("resource operation identity or fence conflicts")
        if (
            receipt.authority_id != self.authority_id
            or receipt.lineage_id != self._lineage_id
            or receipt.issuer != self._keyring.trusted_issuer
            or receipt.key_purpose != RESOURCE_OPERATION_KEY_PURPOSE
            or receipt.namespace != RESOURCE_OPERATION_RECEIPT_NAMESPACE
            or receipt.signature_algorithm != _RESOURCE_OPERATION_SIGNATURE_ALGORITHM
        ):
            raise RuntimeResourceAdmissionError(
                "resource operation prior receipt authority conflicts"
            )
        if not self._verify_signature(
            issuer=receipt.issuer,
            key_id=receipt.key_id,
            key_purpose=receipt.key_purpose,
            namespace=receipt.namespace,
            signature_algorithm=receipt.signature_algorithm,
            public_key_fingerprint=receipt.public_key_fingerprint,
            payload=receipt.signing_bytes(),
            signature=receipt.signature,
        ):
            raise RuntimeResourceAdmissionError(
                "resource operation prior receipt signature is invalid"
            )
        row = connection.execute(
            f"SELECT * FROM {_RESOURCE_OPERATION_TABLE} WHERE operation_id = ?",
            (receipt.operation_id,),
        ).fetchone()
        if row is None:
            raise RuntimeResourceAdmissionError("resource operation prior receipt is not durable")
        prior = self._journal_result(row)
        if prior.receipt != receipt:
            raise RuntimeResourceAdmissionError("resource operation prior receipt is not durable")
        if prior.lease != lease or receipt.kind is ResourceOperationKind.RELEASE:
            raise RuntimeResourceAdmissionError(
                "resource operation prior receipt does not bind the lease"
            )
        lease_row = connection.execute(
            f"SELECT * FROM {_RESOURCE_RESERVATION_TABLE} WHERE lease_id = ?",
            (lease.lease_id,),
        ).fetchone()
        if lease_row is None:
            raise RuntimeResourceAdmissionError("resource operation lease is expired or missing")
        persisted = self._lease_from_row(lease_row)
        if persisted != lease or lease_row["last_effect_operation_id"] != receipt.operation_id:
            raise RuntimeResourceAdmissionError("resource operation lease fence is stale")
        if persisted.expires_at <= now:
            raise RuntimeResourceAdmissionError("resource operation lease expired")
        return receipt.receipt_hash

    def _operate(
        self,
        *,
        operation_id: str,
        effect_key: str,
        kind: ResourceOperationKind,
        identity: ResourceReservationIdentity,
        payload: Mapping[str, object],
        prior_receipt_hash: str | None,
        apply: Callable[[sqlite3.Connection, datetime], _ResourceOperationState],
        lock_wait_timeout_seconds: float | None = None,
    ) -> ResourceOperationResult:
        identifier = self._require_operation_id(operation_id)
        payload_hash = canonical_sha256(payload)
        attempt_hash = canonical_sha256(identity)
        self._reconcile_high_water()
        prepared_result: ResourceOperationResult | None = None
        try:
            with self._connect() as connection:
                self._begin_immediate(
                    connection, lock_wait_timeout_seconds=lock_wait_timeout_seconds
                )
                self._attest_schema(connection)
                audit = self._audit_journal(connection)
                existing = connection.execute(
                    f"SELECT * FROM {_RESOURCE_OPERATION_TABLE} WHERE operation_id = ?",
                    (identifier,),
                ).fetchone()
                if existing is not None:
                    replayed = self._journal_result(existing)
                    if (
                        existing["payload_hash"] != payload_hash
                        or existing["effect_key"] != effect_key
                        or existing["kind"] != kind.value
                        or existing["attempt_identity_hash"] != attempt_hash
                    ):
                        raise ResourceOperationConflictError(
                            "resource operation_id payload conflicts"
                        )
                    connection.rollback()
                    return replayed
                effect = connection.execute(
                    f"SELECT operation_id FROM {_RESOURCE_OPERATION_TABLE} WHERE effect_key = ?",
                    (effect_key,),
                ).fetchone()
                if effect is not None:
                    raise ResourceOperationConflictError(
                        "resource effect key is already bound to another operation"
                    )
                now = self._authority_now(connection)
                state = apply(connection, now)
                applied_at = self._authority_now(connection)
                result_hash = canonical_sha256(state)
                sequence = audit.head.sequence + 1
                receipt = self._sign_receipt(
                    sequence=sequence,
                    previous_entry_hash=audit.head.entry_hash,
                    operation_id=identifier,
                    effect_key=effect_key,
                    kind=kind,
                    attempt_identity_hash=attempt_hash,
                    payload_hash=payload_hash,
                    result_hash=result_hash,
                    prior_receipt_hash=prior_receipt_hash,
                    applied_at=applied_at,
                )
                operation_result = ResourceOperationResult(
                    receipt=receipt,
                    **state.model_dump(mode="python"),
                )
                materialized = {
                    reservation.lease.lease_id: reservation
                    for reservation in audit.materialized_reservations
                }
                receipt_lease_ids = dict(audit.receipt_lease_ids)
                receipt_operation_ids = dict(audit.receipt_operation_ids)
                self._reduce_materialized_reservation(
                    materialized=materialized,
                    receipt_lease_ids=receipt_lease_ids,
                    receipt_operation_ids=receipt_operation_ids,
                    result=operation_result,
                )
                expected_materialized = self._ordered_materialized_reservations(materialized)
                actual_materialized = self._read_materialized_reservations(connection)
                if actual_materialized != expected_materialized:
                    raise RuntimeResourceAdmissionError(
                        "resource operation materialized state update diverged from reducer"
                    )
                result_json = self._contract_json(state)
                receipt_json = self._contract_json(receipt)
                applied_at_text = self._timestamp(applied_at)
                entry_values: dict[str, object] = {
                    "operation_id": identifier,
                    "effect_key": effect_key,
                    "kind": kind.value,
                    "sequence": sequence,
                    "previous_entry_hash": audit.head.entry_hash,
                    "attempt_identity_hash": attempt_hash,
                    "payload_hash": payload_hash,
                    "result_json": result_json,
                    "result_hash": result_hash,
                    "receipt_json": receipt_json,
                    "applied_at": applied_at_text,
                }
                entry_hash = self._entry_hash(
                    entry_values,
                    lineage_id=audit.genesis.lineage_id,
                )
                materialized_state_root = self._materialized_state_root(expected_materialized)
                head = self._sign_head(
                    genesis=audit.genesis,
                    sequence=sequence,
                    entry_hash=entry_hash,
                    previous_head_hash=audit.head.head_hash,
                    materialized_state_root=materialized_state_root,
                )
                head_json = self._contract_json(head)
                prepared_operation_id: str | None = None
                prepared_previous_checkpoint_hash: str | None = None
                prepared_checkpoint_json: str | None = None
                if self._mode == "production":
                    meta = connection.execute(
                        f"SELECT checkpoint_root_json FROM {_RESOURCE_OPERATION_META_TABLE} "
                        "WHERE singleton = 1"
                    ).fetchone()
                    if meta is None:
                        raise RuntimeResourceAdmissionError(
                            "resource journal high-water checkpoint is missing"
                        )
                    previous_root = ResourceJournalAntiRollbackReceipt.model_validate_json(
                        meta["checkpoint_root_json"]
                    )
                    checkpoint = self._high_water_checkpoint(head, head_json=head_json)
                    prepared_operation_id = self._high_water_operation_id(checkpoint)
                    prepared_previous_checkpoint_hash = previous_root.checkpoint.checkpoint_hash
                    prepared_checkpoint_json = self._contract_json(checkpoint)
                connection.execute(
                    f"""
                    INSERT INTO {_RESOURCE_OPERATION_TABLE}(
                        operation_id, effect_key, kind, sequence, previous_entry_hash,
                        attempt_identity_hash, payload_hash, result_json, result_hash,
                        receipt_json, entry_hash, head_json, applied_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        identifier,
                        effect_key,
                        kind.value,
                        sequence,
                        audit.head.entry_hash,
                        attempt_hash,
                        payload_hash,
                        result_json,
                        result_hash,
                        receipt_json,
                        entry_hash,
                        head_json,
                        applied_at_text,
                    ),
                )
                updated = connection.execute(
                    f"""
                    UPDATE {_RESOURCE_OPERATION_META_TABLE}
                    SET head_json = ?, active_key_id = ?,
                        active_public_key_fingerprint = ?,
                        prepared_operation_id = ?,
                        prepared_previous_checkpoint_hash = ?,
                        prepared_checkpoint_json = ?
                    WHERE singleton = 1 AND head_json = ?
                    """,
                    (
                        head_json,
                        self._key_id,
                        self._public_key_fingerprint,
                        prepared_operation_id,
                        prepared_previous_checkpoint_hash,
                        prepared_checkpoint_json,
                        audit.head_json,
                    ),
                )
                if updated.rowcount != 1:
                    raise RuntimeResourceAdmissionError(
                        "resource operation journal head compare-and-swap failed"
                    )
                self._audit_journal(connection)
                connection.commit()
                prepared_result = operation_result
        except ResourceOperationConflictError:
            raise
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise RuntimeResourceAdmissionError("resource operation transaction failed") from exc
        if prepared_result is None:
            raise RuntimeResourceAdmissionError("resource operation result was not prepared")
        self._reconcile_high_water()
        return prepared_result

    def reserve(
        self,
        *,
        operation_id: str,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        policy: AdmissionPolicy,
        snapshot_provider: Callable[[], ResourceSnapshot],
        lease_seconds: int,
        lock_wait_timeout_seconds: float | None = None,
    ) -> ResourceOperationResult:
        validated_identity = ResourceReservationIdentity.model_validate(identity)
        validated_request = AdmissionRequest.model_validate(request)
        validated_policy = AdmissionPolicy.model_validate(policy)
        self._no_quota_side_effects(validated_request)
        lease_ttl = self._validate_lease_seconds(lease_seconds)
        if str(validated_identity.job_id) != validated_request.job_id:
            raise RuntimeResourceAdmissionError(
                "resource reservation identity does not match admission request"
            )
        request_hash = canonical_sha256(validated_request)
        lease_id = canonical_sha256(validated_identity)
        payload = {
            "identity": validated_identity.model_dump(mode="json"),
            "lease_seconds": lease_ttl,
            "policy": validated_policy.model_dump(mode="json"),
            "request": validated_request.model_dump(mode="json"),
        }

        def apply(connection: sqlite3.Connection, now: datetime) -> _ResourceOperationState:
            if validated_request.deadline <= now:
                raise RuntimeResourceAdmissionError("resource reservation deadline expired")
            raw_snapshot = snapshot_provider()
            sampled_at = self._authority_now(connection)
            snapshot = self._validate_snapshot(
                raw_snapshot, now=sampled_at, policy=validated_policy
            )
            SQLiteResourceReservationStore._accept_snapshot_watermark(
                self, connection, snapshot=snapshot
            )
            rows = self._active_materialized_rows(connection, now=sampled_at)
            if len(rows) >= _MAX_ACTIVE_RESOURCE_RESERVATIONS:
                raise RuntimeResourceAdmissionError(
                    "active resource reservation capacity exhausted"
                )
            adjusted = self._adjust_snapshot(snapshot, rows)
            decision = evaluate_admission(validated_request, adjusted, validated_policy)
            lease: ResourceReservationLease | None = None
            if decision.outcome is AdmissionOutcome.ADMITTED:
                lease = ResourceReservationLease(
                    identity=validated_identity,
                    request_hash=request_hash,
                    expected_memory_bytes=validated_request.expected_memory_bytes,
                    expected_disk_bytes=validated_request.expected_disk_bytes,
                    expected_quota_units=0,
                    granted_at=sampled_at,
                    expires_at=self._lease_expiry(sampled_at, lease_seconds=lease_ttl),
                )
                connection.execute(
                    f"""
                    INSERT INTO {_RESOURCE_RESERVATION_TABLE}(
                        lease_id, job_id, run_id, shard_id, attempt_id, claim_generation,
                        scheduler_fencing_token, worker_id, request_hash,
                        expected_memory_bytes, expected_disk_bytes, expected_quota_units,
                        granted_at, expires_at, last_renewal_operation_id, last_effect_operation_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                    """,
                    (
                        lease.lease_id,
                        str(lease.identity.job_id),
                        lease.identity.run_id,
                        str(lease.identity.shard_id),
                        str(lease.identity.attempt_id),
                        lease.identity.claim_generation,
                        lease.identity.scheduler_fencing_token,
                        lease.identity.worker_id,
                        lease.request_hash,
                        lease.expected_memory_bytes,
                        lease.expected_disk_bytes,
                        self._timestamp(lease.granted_at),
                        self._timestamp(lease.expires_at),
                        self._require_operation_id(operation_id),
                        self._require_operation_id(operation_id),
                    ),
                )
            return _ResourceOperationState(
                decision=decision,
                request=validated_request,
                snapshot=adjusted,
                policy=validated_policy,
                lease=lease,
            )

        return self._operate(
            operation_id=operation_id,
            effect_key=f"reserve:{lease_id}",
            kind=ResourceOperationKind.RESERVE,
            identity=validated_identity,
            payload=payload,
            prior_receipt_hash=None,
            apply=apply,
            lock_wait_timeout_seconds=lock_wait_timeout_seconds,
        )

    def recheck(
        self,
        *,
        operation_id: str,
        lease: ResourceReservationLease,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        policy: AdmissionPolicy,
        snapshot_provider: Callable[[], ResourceSnapshot],
        lease_seconds: int,
        prior_receipt: ResourceOperationReceipt,
        lock_wait_timeout_seconds: float | None = None,
    ) -> ResourceOperationResult:
        return self._renew_or_recheck(
            kind=ResourceOperationKind.RECHECK,
            operation_id=operation_id,
            lease=lease,
            identity=identity,
            request=request,
            policy=policy,
            snapshot_provider=snapshot_provider,
            lease_seconds=lease_seconds,
            prior_receipt=prior_receipt,
            lock_wait_timeout_seconds=lock_wait_timeout_seconds,
        )

    def renew(
        self,
        *,
        operation_id: str,
        lease: ResourceReservationLease,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        policy: AdmissionPolicy,
        snapshot_provider: Callable[[], ResourceSnapshot],
        lease_seconds: int,
        prior_receipt: ResourceOperationReceipt,
        lock_wait_timeout_seconds: float | None = None,
    ) -> ResourceOperationResult:
        return self._renew_or_recheck(
            kind=ResourceOperationKind.RENEW,
            operation_id=operation_id,
            lease=lease,
            identity=identity,
            request=request,
            policy=policy,
            snapshot_provider=snapshot_provider,
            lease_seconds=lease_seconds,
            prior_receipt=prior_receipt,
            lock_wait_timeout_seconds=lock_wait_timeout_seconds,
        )

    def _renew_or_recheck(
        self,
        *,
        kind: ResourceOperationKind,
        operation_id: str,
        lease: ResourceReservationLease,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        policy: AdmissionPolicy,
        snapshot_provider: Callable[[], ResourceSnapshot],
        lease_seconds: int,
        prior_receipt: ResourceOperationReceipt,
        lock_wait_timeout_seconds: float | None,
    ) -> ResourceOperationResult:
        validated_lease = ResourceReservationLease.model_validate(lease)
        validated_identity = ResourceReservationIdentity.model_validate(identity)
        validated_request = AdmissionRequest.model_validate(request)
        validated_policy = AdmissionPolicy.model_validate(policy)
        validated_receipt = ResourceOperationReceipt.model_validate(prior_receipt)
        self._no_quota_side_effects(validated_request)
        lease_ttl = self._validate_lease_seconds(lease_seconds)
        if (
            validated_lease.identity != validated_identity
            or str(validated_identity.job_id) != validated_request.job_id
        ):
            raise RuntimeResourceAdmissionError("resource operation identity or fence conflicts")
        request_hash = canonical_sha256(validated_request)
        if validated_lease.request_hash != request_hash:
            raise RuntimeResourceAdmissionError(
                "resource operation request changed during execution"
            )
        prior_hash = validated_receipt.receipt_hash
        payload = {
            "lease": validated_lease.model_dump(mode="json"),
            "lease_seconds": lease_ttl,
            "policy": validated_policy.model_dump(mode="json"),
            "prior_receipt": validated_receipt.model_dump(mode="json"),
            "request": validated_request.model_dump(mode="json"),
        }

        def apply(connection: sqlite3.Connection, now: datetime) -> _ResourceOperationState:
            self._prior_receipt(
                connection,
                receipt=validated_receipt,
                lease=validated_lease,
                identity=validated_identity,
                now=now,
            )
            raw_snapshot = snapshot_provider()
            sampled_at = self._authority_now(connection)
            snapshot = self._validate_snapshot(
                raw_snapshot, now=sampled_at, policy=validated_policy
            )
            SQLiteResourceReservationStore._accept_snapshot_watermark(
                self, connection, snapshot=snapshot
            )
            if validated_lease.expires_at <= sampled_at:
                raise RuntimeResourceAdmissionError(
                    "resource operation lease expired during resource probe"
                )
            rows = self._active_materialized_rows(
                connection,
                now=sampled_at,
                excluded_lease_id=validated_lease.lease_id,
            )
            adjusted = self._adjust_snapshot(snapshot, rows)
            decision = evaluate_admission(validated_request, adjusted, validated_policy)
            renewed = validated_lease
            if decision.outcome is AdmissionOutcome.ADMITTED:
                renewal_at = self._authority_now(connection)
                if validated_lease.expires_at <= renewal_at:
                    raise RuntimeResourceAdmissionError(
                        "resource operation lease expired before renewal"
                    )
                renewed = validated_lease.model_copy(
                    update={"expires_at": self._lease_expiry(renewal_at, lease_seconds=lease_ttl)}
                )
                updated = connection.execute(
                    f"""
                    UPDATE {_RESOURCE_RESERVATION_TABLE}
                    SET expires_at = ?, last_renewal_operation_id = ?, last_effect_operation_id = ?
                    WHERE lease_id = ? AND expires_at = ? AND last_effect_operation_id = ?
                    """,
                    (
                        self._timestamp(renewed.expires_at),
                        self._require_operation_id(operation_id),
                        self._require_operation_id(operation_id),
                        renewed.lease_id,
                        self._timestamp(validated_lease.expires_at),
                        validated_receipt.operation_id,
                    ),
                )
            else:
                updated = connection.execute(
                    f"""
                    UPDATE {_RESOURCE_RESERVATION_TABLE}
                    SET last_effect_operation_id = ?
                    WHERE lease_id = ? AND expires_at = ? AND last_effect_operation_id = ?
                    """,
                    (
                        self._require_operation_id(operation_id),
                        validated_lease.lease_id,
                        self._timestamp(validated_lease.expires_at),
                        validated_receipt.operation_id,
                    ),
                )
            if updated.rowcount != 1:
                raise RuntimeResourceAdmissionError(
                    "resource operation renewal fence verification failed"
                )
            return _ResourceOperationState(
                decision=decision,
                request=validated_request,
                snapshot=adjusted,
                policy=validated_policy,
                lease=renewed,
            )

        return self._operate(
            operation_id=operation_id,
            effect_key=f"{kind.value}:{validated_lease.lease_id}:{prior_hash}",
            kind=kind,
            identity=validated_identity,
            payload=payload,
            prior_receipt_hash=prior_hash,
            apply=apply,
            lock_wait_timeout_seconds=lock_wait_timeout_seconds,
        )

    def release(
        self,
        *,
        operation_id: str,
        lease: ResourceReservationLease,
        identity: ResourceReservationIdentity,
        prior_receipt: ResourceOperationReceipt,
        lock_wait_timeout_seconds: float | None = None,
    ) -> ResourceOperationResult:
        validated_lease = ResourceReservationLease.model_validate(lease)
        validated_identity = ResourceReservationIdentity.model_validate(identity)
        validated_receipt = ResourceOperationReceipt.model_validate(prior_receipt)
        if validated_lease.identity != validated_identity:
            raise RuntimeResourceAdmissionError("resource operation identity or fence conflicts")
        prior_hash = validated_receipt.receipt_hash
        payload = {
            "lease": validated_lease.model_dump(mode="json"),
            "prior_receipt": validated_receipt.model_dump(mode="json"),
        }

        def apply(connection: sqlite3.Connection, now: datetime) -> _ResourceOperationState:
            self._prior_receipt(
                connection,
                receipt=validated_receipt,
                lease=validated_lease,
                identity=validated_identity,
                now=now,
            )
            deleted = connection.execute(
                f"""
                DELETE FROM {_RESOURCE_RESERVATION_TABLE}
                WHERE lease_id = ? AND last_effect_operation_id = ?
                """,
                (validated_lease.lease_id, validated_receipt.operation_id),
            )
            if deleted.rowcount != 1:
                raise RuntimeResourceAdmissionError(
                    "resource operation release fence verification failed"
                )
            return _ResourceOperationState(released=True)

        return self._operate(
            operation_id=operation_id,
            effect_key=f"release:{validated_lease.lease_id}:{prior_hash}",
            kind=ResourceOperationKind.RELEASE,
            identity=validated_identity,
            payload=payload,
            prior_receipt_hash=prior_hash,
            apply=apply,
            lock_wait_timeout_seconds=lock_wait_timeout_seconds,
        )

    def lookup(self, operation_id: str) -> ResourceOperationResult:
        identifier = self._require_operation_id(operation_id)
        self._reconcile_high_water()
        try:
            with self._connect() as connection:
                self._begin_immediate(connection)
                self._attest_schema(connection)
                self._audit_journal(connection)
                row = connection.execute(
                    f"SELECT * FROM {_RESOURCE_OPERATION_TABLE} WHERE operation_id = ?",
                    (identifier,),
                ).fetchone()
                if row is None:
                    raise RuntimeResourceAdmissionError(
                        "resource operation journal entry is missing"
                    )
                result = self._journal_result(row)
                connection.rollback()
                return result
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise RuntimeResourceAdmissionError("resource operation journal lookup failed") from exc

    def lookup_latest(
        self,
        *,
        identity: ResourceReservationIdentity,
        lease_id: str,
    ) -> ResourceOperationResult:
        """Return the latest signed effect for one exact fenced lease identity."""

        validated_identity = ResourceReservationIdentity.model_validate(identity)
        expected_lease_id = canonical_sha256(validated_identity)
        if lease_id != expected_lease_id:
            raise RuntimeResourceAdmissionError(
                "resource operation recovery lease identity conflicts"
            )
        attempt_identity_hash = canonical_sha256(validated_identity)
        self._reconcile_high_water()
        try:
            with self._connect() as connection:
                self._begin_immediate(connection)
                self._attest_schema(connection)
                audit = self._audit_journal(connection)
                row = connection.execute(
                    f"""
                    SELECT * FROM {_RESOURCE_OPERATION_TABLE}
                    WHERE attempt_identity_hash = ?
                    ORDER BY sequence DESC
                    LIMIT 1
                    """,
                    (attempt_identity_hash,),
                ).fetchone()
                if row is None:
                    raise RuntimeResourceAdmissionError(
                        "resource operation recovery entry is missing"
                    )
                result = self._journal_result(row)
                if result.released:
                    prior_hash = result.receipt.prior_receipt_hash
                    if audit.receipt_lease_ids.get(prior_hash or "") != expected_lease_id:
                        raise RuntimeResourceAdmissionError(
                            "resource operation terminal recovery fence conflicts"
                        )
                elif result.lease is not None and result.lease.lease_id != expected_lease_id:
                    raise RuntimeResourceAdmissionError(
                        "resource operation recovery result belongs to another lease"
                    )
                connection.rollback()
                return result
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise RuntimeResourceAdmissionError(
                "resource operation latest recovery failed"
            ) from exc

    def active_leases(self) -> tuple[ResourceReservationLease, ...]:
        self._reconcile_high_water()
        try:
            with self._connect() as connection:
                self._begin_immediate(connection)
                self._attest_schema(connection)
                self._audit_journal(connection)
                now = self._authority_now(connection)
                leases = tuple(
                    self._lease_from_row(row)
                    for row in self._active_materialized_rows(connection, now=now)
                )
                connection.commit()
                return leases
        except RuntimeResourceAdmissionError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise RuntimeResourceAdmissionError("resource operation lease read failed") from exc


def compose_production_resource_admission_authority(
    path: Path,
    *,
    authority_id: str,
    signer: ResourceOperationReceiptSigner,
    keyring: ClosedResourceOperationKeyring,
    high_water_authority: ResourceJournalHighWaterAuthority | None,
    mode: Literal["production", "test-standalone"] = "production",
    clock: Callable[[], datetime] | None = None,
) -> SQLiteResourceAdmissionAuthority:
    """Compose the production journal while refusing every rootless mode."""

    if mode != "production":
        raise RuntimeResourceAdmissionError(
            "production resource journal composition rejects non-production mode"
        )
    if high_water_authority is None:
        raise RuntimeResourceAdmissionError(
            "production resource journal composition requires a high-water root"
        )
    return SQLiteResourceAdmissionAuthority(
        path,
        authority_id=authority_id,
        signer=signer,
        keyring=keyring,
        high_water_authority=high_water_authority,
        mode=mode,
        clock=clock,
    )


class ResourceProbe(Protocol):
    def available_memory_bytes(self) -> int: ...

    def available_disk_bytes(self, path: Path) -> int: ...

    def cpu_load_pct(self) -> float: ...

    def io_pressure_pct(self) -> float: ...


class LiveSloEvidence(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    live_backlog_age_microseconds: int = Field(strict=True, ge=0, le=MAX_RESOURCE_COUNT)
    live_p95_latency_microseconds: int = Field(strict=True, ge=0, le=MAX_RESOURCE_COUNT)
    live_healthy: bool = Field(strict=True)

    @model_validator(mode="before")
    @classmethod
    def normalize_duration_inputs(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        normalized = dict(value)
        for seconds_name, microseconds_name in (
            ("live_backlog_age_seconds", "live_backlog_age_microseconds"),
            ("live_p95_latency_seconds", "live_p95_latency_microseconds"),
        ):
            if seconds_name not in normalized:
                continue
            if microseconds_name in normalized:
                raise ValueError(f"{seconds_name} and {microseconds_name} cannot both be provided")
            normalized[microseconds_name] = seconds_to_microseconds(
                normalized.pop(seconds_name),
                label=seconds_name,
            )
        return normalized

    @property
    def live_backlog_age_seconds(self) -> float:
        return self.live_backlog_age_microseconds / MICROSECONDS_PER_SECOND

    @property
    def live_p95_latency_seconds(self) -> float:
        return self.live_p95_latency_microseconds / MICROSECONDS_PER_SECOND


class LiveSloProbe(Protocol):
    def __call__(self, observed_at: datetime, /) -> LiveSloEvidence: ...


class TradingSessionResolver(Protocol):
    def __call__(self, observed_at: datetime, /) -> TradingSession: ...


class RuntimeHealthAuthorityWatermark(RuntimeContractModel):
    as_of: AwareUtcDatetime
    pointer_published_at: AwareUtcDatetime
    sequence: int = Field(strict=True, ge=0, le=MAX_RESOURCE_COUNT)
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")


class RuntimeHealthAuthorityLiveSloProbeConfig(RuntimeContractModel):
    authority_root: Path
    expected_producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")

    def build(
        self,
        *,
        watermark: RuntimeHealthAuthorityWatermark | None = None,
    ) -> RuntimeHealthAuthorityLiveSloProbe:
        return RuntimeHealthAuthorityLiveSloProbe(
            authority_root=self.authority_root,
            expected_producer_commit=self.expected_producer_commit,
            watermark=watermark,
        )


def _system_clock() -> datetime:
    return datetime.now(UTC)


class _WatermarkedServingSourceAuthorityReader(ServingSourceAuthorityReader):
    def seed_watermark(self, watermark: RuntimeHealthAuthorityWatermark) -> None:
        self._last_observation = (
            watermark.as_of,
            watermark.pointer_published_at,
            watermark.sequence,
            watermark.generation_id,
        )

    def export_watermark(self) -> RuntimeHealthAuthorityWatermark | None:
        observation = self._last_observation
        if observation is None:
            return None
        return RuntimeHealthAuthorityWatermark(
            as_of=observation[0],
            pointer_published_at=observation[1],
            sequence=observation[2],
            generation_id=observation[3],
        )


class RuntimeHealthAuthorityLiveSloProbe:
    """Read SLO evidence only from the immutable runtime-health authority."""

    def __init__(
        self,
        *,
        authority_root: Path,
        expected_producer_commit: str,
        watermark: RuntimeHealthAuthorityWatermark | None = None,
    ) -> None:
        self.config = RuntimeHealthAuthorityLiveSloProbeConfig(
            authority_root=Path(authority_root).resolve(),
            expected_producer_commit=expected_producer_commit,
        )
        self.reader = _WatermarkedServingSourceAuthorityReader(
            root=self.config.authority_root,
            expected_producer_commit=expected_producer_commit,
            expected_dataset_id=RUNTIME_HEALTH_DATASET_ID,
            expected_payload_kind="runtime_health",
        )
        self._watermark = watermark
        if watermark is not None:
            self.reader.seed_watermark(watermark)

    @property
    def watermark(self) -> RuntimeHealthAuthorityWatermark | None:
        return self._watermark

    def __call__(self, observed_at: datetime, /) -> LiveSloEvidence:
        observed = normalize_aware_utc(observed_at)
        if self._watermark is not None and observed < self._watermark.as_of:
            raise RuntimeResourceAdmissionError(
                "runtime health observation is older than the accepted watermark"
            )
        try:
            result = self.reader(observed)
        except Exception as exc:
            raise RuntimeResourceAdmissionError("runtime health authority read failed") from exc
        reader_watermark = self.reader.export_watermark()
        if reader_watermark is None:  # pragma: no cover - reader accepts every successful read
            raise RuntimeResourceAdmissionError("runtime health authority watermark is missing")
        self._watermark = reader_watermark
        if not isinstance(result.payload, RuntimeHealthPayload):
            raise RuntimeResourceAdmissionError(
                "runtime health authority returned the wrong payload"
            )
        payload = result.payload
        live_services = tuple(
            service
            for service in payload.runtime_services
            if service.plane is RuntimeServicePlane.LIVE
        )
        if not live_services:
            raise RuntimeResourceAdmissionError(
                "runtime health authority has no live service SLO evidence"
            )
        backlog_ages_us: list[int] = []
        p95_latencies_us: list[int] = []
        for service in live_services:
            heartbeat = service.heartbeat
            if heartbeat is None:
                raise RuntimeResourceAdmissionError(
                    f"runtime health authority lacks heartbeat evidence: {service.service_id}"
                )
            if heartbeat.p95_step_duration_seconds is None:
                raise RuntimeResourceAdmissionError(
                    f"runtime health authority lacks p95 SLO evidence: {service.service_id}"
                )
            if heartbeat.last_success_at is None:
                raise RuntimeResourceAdmissionError(
                    f"runtime health authority lacks last_success evidence: {service.service_id}"
                )
            if heartbeat.last_success_at > observed:
                raise RuntimeResourceAdmissionError(
                    "runtime health authority has future last_success evidence: "
                    f"{service.service_id}"
                )
            backlog_ages_us.append(
                _nonnegative_timedelta_microseconds(observed - heartbeat.last_success_at)
            )
            p95_latencies_us.append(_seconds_to_microseconds(heartbeat.p95_step_duration_seconds))
        authority_age_us = _nonnegative_timedelta_microseconds(observed - result.published_at)
        live_services_healthy = bool(live_services) and all(
            service.status is RuntimeServiceStatus.RUNNING
            and not service.stale
            and service.heartbeat is not None
            and service.heartbeat.status is RuntimeServiceStatus.RUNNING
            for service in live_services
        )
        calculated_backlog_us = max(
            *backlog_ages_us,
            authority_age_us,
        )
        calculated_p95_us = max(p95_latencies_us)
        if payload.live_backlog_age_seconds is None:
            raise RuntimeResourceAdmissionError(
                "runtime health authority lacks aggregate backlog SLO evidence"
            )
        if payload.live_p95_latency_seconds is None:
            raise RuntimeResourceAdmissionError(
                "runtime health authority lacks aggregate p95 SLO evidence"
            )
        payload_backlog_us = (
            _seconds_to_microseconds(payload.live_backlog_age_seconds) + authority_age_us
        )
        payload_p95_us = _seconds_to_microseconds(payload.live_p95_latency_seconds)
        if payload_backlog_us != calculated_backlog_us or payload_p95_us != calculated_p95_us:
            raise RuntimeResourceAdmissionError(
                "runtime health authority aggregate SLO conflicts with service evidence"
            )
        if payload.live_healthy != live_services_healthy:
            raise RuntimeResourceAdmissionError(
                "runtime health authority aggregate health conflicts with service evidence"
            )
        return LiveSloEvidence(
            observed_at=result.published_at,
            live_backlog_age_microseconds=payload_backlog_us,
            live_p95_latency_microseconds=payload_p95_us,
            live_healthy=(result.status is FreshnessStatus.FRESH and payload.live_healthy),
        )


class SystemResourceProbe:
    """Small, local-only resource probe with no optional Python dependencies."""

    def available_memory_bytes(self) -> int:
        if sys.platform.startswith("linux"):
            return self._linux_available_memory_bytes()
        if sys.platform == "darwin":
            return self._darwin_available_memory_bytes()
        try:
            return int(os.sysconf("SC_AVPHYS_PAGES")) * int(os.sysconf("SC_PAGE_SIZE"))
        except (OSError, ValueError) as exc:
            raise RuntimeResourceAdmissionError(
                "available memory is unsupported on this platform"
            ) from exc

    def available_disk_bytes(self, path: Path) -> int:
        return int(shutil.disk_usage(path).free)

    def cpu_load_pct(self) -> float:
        cpu_count = os.cpu_count()
        if cpu_count is None or cpu_count < 1:
            raise RuntimeResourceAdmissionError("CPU count is unavailable")
        load_one, _, _ = os.getloadavg()
        return _bounded_pct(load_one * 100.0 / cpu_count)

    def io_pressure_pct(self) -> float:
        if sys.platform.startswith("linux"):
            return self._linux_io_pressure_pct()
        if sys.platform == "darwin":
            # Darwin has no PSI interface. Load average includes runnable and blocked
            # tasks, so this is a conservative local scheduling-pressure proxy.
            return self.cpu_load_pct()
        raise RuntimeResourceAdmissionError("I/O pressure is unsupported on this platform")

    @staticmethod
    def _linux_available_memory_bytes() -> int:
        try:
            payload = Path("/proc/meminfo").read_text(encoding="ascii")
        except OSError as exc:
            raise RuntimeResourceAdmissionError("/proc/meminfo is unavailable") from exc
        return _parse_linux_meminfo_available_bytes(payload)

    @staticmethod
    def _darwin_available_memory_bytes() -> int:
        try:
            completed = subprocess.run(
                ["/usr/bin/vm_stat"],
                check=True,
                capture_output=True,
                text=True,
                timeout=2,
                env={"PATH": "/usr/bin:/bin"},
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise RuntimeResourceAdmissionError("vm_stat failed") from exc
        return _parse_darwin_vm_stat_available_bytes(completed.stdout)

    @staticmethod
    def _linux_io_pressure_pct() -> float:
        try:
            payload = Path("/proc/pressure/io").read_text(encoding="ascii")
        except OSError as exc:
            raise RuntimeResourceAdmissionError("/proc/pressure/io is unavailable") from exc
        return _parse_linux_io_pressure_pct(payload)


def _parse_linux_meminfo_available_bytes(payload: str) -> int:
    for line in payload.splitlines():
        if line.startswith("MemAvailable:"):
            fields = line.split()
            if len(fields) == 3 and fields[2] == "kB":
                try:
                    available_kib = _parse_bounded_unsigned_integer(
                        fields[1],
                        label="MemAvailable",
                        maximum=MAX_RESOURCE_CAPACITY_BYTES // 1024,
                    )
                    if available_kib == 0:
                        raise ValueError("capacity must be positive")
                    return available_kib * 1024
                except ValueError as exc:
                    raise RuntimeResourceAdmissionError(
                        "MemAvailable is invalid in /proc/meminfo"
                    ) from exc
    raise RuntimeResourceAdmissionError("MemAvailable is missing from /proc/meminfo")


def _parse_darwin_vm_stat_available_bytes(payload: str) -> int:
    page_match = re.search(r"page size of (\S+) bytes", payload)
    if page_match is None:
        raise RuntimeResourceAdmissionError("vm_stat page size is missing")
    try:
        page_size = _parse_bounded_unsigned_integer(
            page_match.group(1),
            label="vm_stat page size",
            maximum=1024**2,
        )
    except ValueError as exc:
        raise RuntimeResourceAdmissionError("vm_stat page size is invalid") from exc
    if page_size < 512 or page_size & (page_size - 1):
        raise RuntimeResourceAdmissionError("vm_stat page size is invalid")
    pages: dict[str, int] = {}
    for line in payload.splitlines()[1:]:
        match = re.fullmatch(r"Pages (free|inactive|speculative):\s+(\S+)\.", line)
        if match is not None:
            try:
                pages[match.group(1)] = _parse_bounded_unsigned_integer(
                    match.group(2),
                    label=f"vm_stat {match.group(1)} pages",
                    maximum=MAX_RESOURCE_CAPACITY_BYTES // page_size,
                )
            except ValueError as exc:
                raise RuntimeResourceAdmissionError(
                    "vm_stat available page counter is invalid"
                ) from exc
    if set(pages) != {"free", "inactive", "speculative"}:
        raise RuntimeResourceAdmissionError("vm_stat available page counters are missing")
    available_pages = sum(pages.values())
    if available_pages <= 0 or available_pages > MAX_RESOURCE_CAPACITY_BYTES // page_size:
        raise RuntimeResourceAdmissionError("vm_stat available capacity is invalid")
    return available_pages * page_size


def _parse_linux_io_pressure_pct(payload: str) -> float:
    for line in payload.splitlines():
        if not line.startswith("some "):
            continue
        for field in line.split()[1:]:
            if field.startswith("avg10="):
                raw_value = field.removeprefix("avg10=")
                try:
                    if len(raw_value) > 32:
                        raise ValueError("value has too many digits")
                    value = Decimal(raw_value)
                    if not value.is_finite() or value < 0 or value > 100:
                        raise ValueError("value is outside 0..100")
                    return float(value)
                except (InvalidOperation, ValueError) as exc:
                    raise RuntimeResourceAdmissionError("I/O PSI avg10 is invalid") from exc
    raise RuntimeResourceAdmissionError("I/O PSI avg10 is missing")


def _seconds_to_microseconds(value: float) -> int:
    try:
        return seconds_to_microseconds(value, label="runtime SLO duration")
    except ValueError as exc:
        raise RuntimeResourceAdmissionError("runtime SLO duration is invalid") from exc


def _nonnegative_timedelta_microseconds(value: timedelta) -> int:
    return max(0, timedelta_microseconds(value))


def _parse_bounded_unsigned_integer(
    value: str,
    *,
    label: str,
    maximum: int,
) -> int:
    if (
        not value
        or (len(value) > 1 and value.startswith("0"))
        or len(value) > len(str(maximum))
        or re.fullmatch(r"[0-9]+", value) is None
    ):
        raise ValueError(f"{label} is not an unsigned integer")
    parsed = int(value)
    if parsed > maximum:
        raise ValueError(f"{label} exceeds the supported range")
    return parsed


def _validate_authority_watermark_advance(
    previous: RuntimeHealthAuthorityWatermark | None,
    candidate: RuntimeHealthAuthorityWatermark,
) -> bool:
    if previous is None:
        return True
    if candidate.as_of < previous.as_of:
        return False
    if candidate.pointer_published_at < previous.pointer_published_at:
        raise RuntimeResourceAdmissionError("runtime health authority pointer rollback detected")
    if candidate.sequence < previous.sequence:
        raise RuntimeResourceAdmissionError("runtime health authority sequence rollback detected")
    if (
        candidate.sequence == previous.sequence
        and candidate.generation_id != previous.generation_id
    ):
        raise RuntimeResourceAdmissionError("runtime health authority generation rollback detected")
    return True


def _bounded_pct(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeResourceAdmissionError("resource percentage is not canonical")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0 or normalized > 100:
        raise RuntimeResourceAdmissionError("resource percentage is invalid")
    return normalized


def trading_session_at(observed_at: datetime) -> TradingSession:
    local = normalize_aware_utc(observed_at).astimezone(_SHANGHAI)
    if local.weekday() >= 5:
        return TradingSession.CLOSED
    wall = local.timetz().replace(tzinfo=None)
    if time(9, 15) <= wall < time(9, 30):
        return TradingSession.PRE_MARKET
    if time(9, 30) <= wall < time(11, 30):
        return TradingSession.MORNING
    if time(11, 30) <= wall < time(13, 0):
        return TradingSession.LUNCH
    if time(13, 0) <= wall < time(15, 10):
        return TradingSession.AFTERNOON
    if time(15, 10) <= wall < time(18, 0):
        return TradingSession.POST_MARKET
    return TradingSession.CLOSED


class RuntimeTradeCalendarSessionResolver:
    """Resolve research scheduling sessions from one immutable SSE calendar."""

    def __init__(self, authority: MarketCalendarAuthority) -> None:
        self.authority = MarketCalendarAuthority.model_validate(authority)

    def __call__(self, observed_at: datetime, /) -> TradingSession:
        decision = decide_market_session(self.authority, observed_at)
        if not decision.is_open_date:
            return TradingSession.CLOSED
        return trading_session_at(observed_at)


@dataclass
class _SpawnedLocalResourceSnapshotProvider:
    disk_path: Path
    clock: Callable[[], datetime]
    probe: ResourceProbe
    live_slo_config: RuntimeHealthAuthorityLiveSloProbeConfig
    session_resolver: TradingSessionResolver
    authority_watermark: RuntimeHealthAuthorityWatermark | None

    def __call__(self) -> ResourceSnapshot:
        live_slo_probe = self.live_slo_config.build(watermark=self.authority_watermark)
        provider = LocalResourceSnapshotProvider(
            disk_path=self.disk_path,
            clock=self.clock,
            probe=self.probe,
            live_slo_probe=live_slo_probe,
            session_resolver=self.session_resolver,
        )
        snapshot = provider()
        self.authority_watermark = live_slo_probe.watermark
        return snapshot

    def export_probe_state(self) -> RuntimeHealthAuthorityWatermark | None:
        return self.authority_watermark


class LocalResourceSnapshotProvider:
    def __init__(
        self,
        *,
        disk_path: Path,
        clock: Callable[[], datetime] | None = None,
        probe: ResourceProbe | None = None,
        live_slo_probe: LiveSloProbe | None = None,
        live_slo_config: RuntimeHealthAuthorityLiveSloProbeConfig | None = None,
        session_resolver: TradingSessionResolver,
    ) -> None:
        if (live_slo_probe is None) == (live_slo_config is None):
            raise RuntimeResourceAdmissionError(
                "exactly one live SLO probe or spawn-safe configuration is required"
            )
        self.disk_path = Path(disk_path).resolve()
        self.clock = clock or _system_clock
        self.probe = probe or SystemResourceProbe()
        self.live_slo_probe = live_slo_probe
        self.live_slo_config = (
            live_slo_probe.config
            if isinstance(live_slo_probe, RuntimeHealthAuthorityLiveSloProbe)
            else live_slo_config
        )
        self.session_resolver = session_resolver
        self._authority_state_lock = threading.Lock()
        self._authority_watermark = (
            live_slo_probe.watermark
            if isinstance(live_slo_probe, RuntimeHealthAuthorityLiveSloProbe)
            else None
        )

    def spawn_probe_provider(self) -> _SpawnedLocalResourceSnapshotProvider:
        if self.live_slo_config is None:
            raise RuntimeResourceAdmissionError(
                "custom live SLO probes do not expose a spawn-safe authority configuration"
            )
        with self._authority_state_lock:
            watermark = self._authority_watermark
        return _SpawnedLocalResourceSnapshotProvider(
            disk_path=self.disk_path,
            clock=self.clock,
            probe=self.probe,
            live_slo_config=self.live_slo_config,
            session_resolver=self.session_resolver,
            authority_watermark=watermark,
        )

    def accept_probe_state(self, state: object) -> bool:
        if state is None:
            return True
        if not isinstance(state, RuntimeHealthAuthorityWatermark):
            raise RuntimeResourceAdmissionError(
                "resource probe returned an invalid authority watermark"
            )
        with self._authority_state_lock:
            if not _validate_authority_watermark_advance(self._authority_watermark, state):
                return False
            self._authority_watermark = state
            return True

    @staticmethod
    def _read(label: str, reader: Callable[[], int | float]) -> int | float:
        try:
            return reader()
        except Exception as exc:
            raise RuntimeResourceAdmissionError(f"{label} probe failed") from exc

    @staticmethod
    def _canonical_probe_bytes(value: object, *, label: str) -> int:
        if type(value) is not int or not 0 <= value <= MAX_RESOURCE_CAPACITY_BYTES:
            raise ValueError(f"{label} must be a canonical bounded integer")
        return value

    @staticmethod
    def _canonical_probe_percentage(value: object, *, label: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{label} must be a canonical finite number")
        normalized = float(value)
        if not math.isfinite(normalized) or not 0 <= normalized <= 100:
            raise ValueError(f"{label} must be between 0 and 100")
        return normalized

    def __call__(self) -> ResourceSnapshot:
        try:
            observed_at = normalize_aware_utc(self.clock())
        except Exception as exc:
            raise RuntimeResourceAdmissionError("resource snapshot clock failed") from exc
        try:
            session = self.session_resolver(observed_at)
        except Exception as exc:
            raise RuntimeResourceAdmissionError("trade calendar session resolution failed") from exc
        live_slo_applicable = session in {
            TradingSession.PRE_MARKET,
            TradingSession.MORNING,
            TradingSession.LUNCH,
            TradingSession.AFTERNOON,
        }
        if live_slo_applicable:
            live_slo_probe = self.live_slo_probe
            if live_slo_probe is None:
                assert self.live_slo_config is not None
                with self._authority_state_lock:
                    watermark = self._authority_watermark
                live_slo_probe = self.live_slo_config.build(watermark=watermark)
            try:
                live_slo = live_slo_probe(observed_at)
            except Exception as exc:
                raise RuntimeResourceAdmissionError("live SLO probe failed") from exc
            if isinstance(live_slo_probe, RuntimeHealthAuthorityLiveSloProbe):
                self.accept_probe_state(live_slo_probe.watermark)
            if not isinstance(live_slo, LiveSloEvidence):
                raise RuntimeResourceAdmissionError("live SLO probe returned an invalid contract")
            if live_slo.observed_at > observed_at:
                raise RuntimeResourceAdmissionError("live SLO evidence is from the future")
        else:
            live_slo = LiveSloEvidence(
                observed_at=observed_at,
                live_backlog_age_microseconds=0,
                live_p95_latency_microseconds=0,
                live_healthy=True,
            )
        memory = self._read("available memory", self.probe.available_memory_bytes)
        disk = self._read(
            "available disk",
            lambda: self.probe.available_disk_bytes(self.disk_path),
        )
        cpu = self._read("CPU load", self.probe.cpu_load_pct)
        io_pressure = self._read("I/O pressure", self.probe.io_pressure_pct)
        try:
            return ResourceSnapshot(
                observed_at=observed_at,
                session=session,
                live_slo_applicable=live_slo_applicable,
                live_backlog_age_microseconds=live_slo.live_backlog_age_microseconds,
                live_p95_latency_microseconds=live_slo.live_p95_latency_microseconds,
                available_memory_bytes=self._canonical_probe_bytes(
                    memory,
                    label="available memory",
                ),
                available_disk_bytes=self._canonical_probe_bytes(
                    disk,
                    label="available disk",
                ),
                io_pressure_pct=self._canonical_probe_percentage(
                    io_pressure,
                    label="I/O pressure",
                ),
                cpu_load_pct=self._canonical_probe_percentage(
                    cpu,
                    label="CPU load",
                ),
                source_quota_remaining=0,
                live_healthy=live_slo.live_healthy,
            )
        except Exception as exc:
            raise RuntimeResourceAdmissionError("resource snapshot validation failed") from exc


def admission_policy_for_version(version: str) -> AdmissionPolicy:
    if version != LAB_RESOURCE_POLICY_V1:
        raise RuntimeResourceAdmissionError(
            f"unknown Strategy Lab resource policy version: {version!r}"
        )
    return AdmissionPolicy.model_validate(_POLICY_V1)


@dataclass(frozen=True)
class StaticAdmissionPolicyProvider:
    policy: AdmissionPolicy

    def __call__(self, _spec: ResearchRunSpec) -> AdmissionPolicy:
        return self.policy


@dataclass(frozen=True)
class RuntimeResourceAdmissionBindings:
    require_resource_admission: bool
    resource_snapshot_provider: LocalResourceSnapshotProvider | None
    admission_policy_provider: Callable[[ResearchRunSpec], AdmissionPolicy] | None


def build_runtime_resource_admission(
    *,
    app_env: Literal["dev", "prod"],
    disk_path: Path,
    configured_policy_version: str | None,
    legacy_opt_out: bool,
    clock: Callable[[], datetime] | None = None,
    probe: ResourceProbe | None = None,
    live_slo_probe: LiveSloProbe | None = None,
    live_slo_probe_config: RuntimeHealthAuthorityLiveSloProbeConfig | None = None,
    session_resolver: TradingSessionResolver | None = None,
) -> RuntimeResourceAdmissionBindings:
    if not isinstance(legacy_opt_out, bool):
        raise RuntimeResourceAdmissionError("legacy_opt_out must be a boolean")
    if legacy_opt_out:
        if app_env == "prod":
            raise RuntimeResourceAdmissionError(
                "production Strategy Lab worker cannot disable resource admission"
            )
        return RuntimeResourceAdmissionBindings(
            require_resource_admission=False,
            resource_snapshot_provider=None,
            admission_policy_provider=None,
        )
    version = (configured_policy_version or "").strip()
    if not version:
        raise RuntimeResourceAdmissionError(
            f"Strategy Lab resource policy version is required in {LAB_RESOURCE_POLICY_ENV}"
        )
    policy = admission_policy_for_version(version)
    if live_slo_probe is None and live_slo_probe_config is None:
        raise RuntimeResourceAdmissionError("Strategy Lab live SLO probe must be configured")
    if live_slo_probe is not None and live_slo_probe_config is not None:
        raise RuntimeResourceAdmissionError(
            "Strategy Lab live SLO authority has conflicting configurations"
        )
    if session_resolver is None:
        raise RuntimeResourceAdmissionError(
            "Strategy Lab authoritative trade calendar resolver must be configured"
        )
    provider = LocalResourceSnapshotProvider(
        disk_path=disk_path,
        clock=clock,
        probe=probe,
        live_slo_probe=live_slo_probe,
        live_slo_config=live_slo_probe_config,
        session_resolver=session_resolver,
    )
    return RuntimeResourceAdmissionBindings(
        require_resource_admission=True,
        resource_snapshot_provider=provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(policy),
    )


__all__ = [
    "ClosedResourceOperationKeyring",
    "LAB_RESOURCE_POLICY_ENV",
    "LAB_RESOURCE_POLICY_V1",
    "LAB_LIVE_SLO_AUTHORITY_ROOT_ENV",
    "LiveSloEvidence",
    "LiveSloProbe",
    "LocalResourceSnapshotProvider",
    "PersistentResourceReservationStore",
    "RESOURCE_OPERATION_KEY_PURPOSE",
    "RESOURCE_OPERATION_RECEIPT_NAMESPACE",
    "TRUSTED_RESOURCE_ROLE_PURPOSES",
    "ResourceProbe",
    "ResourceOperationConflictError",
    "ResourceOperationKind",
    "ResourceOperationReceipt",
    "ResourceOperationReceiptSigner",
    "ResourceOperationReceiptVerifier",
    "ResourceOperationResult",
    "ResourceReservationAdmission",
    "RuntimeResourceAdmissionBindings",
    "RuntimeResourceAdmissionError",
    "RuntimeHealthAuthorityLiveSloProbe",
    "RuntimeHealthAuthorityLiveSloProbeConfig",
    "RuntimeHealthAuthorityWatermark",
    "StaticAdmissionPolicyProvider",
    "RuntimeTradeCalendarSessionResolver",
    "SQLiteResourceAdmissionAuthority",
    "SQLiteResourceReservationStore",
    "SystemResourceProbe",
    "TradingSessionResolver",
    "TrustedRoleInventory",
    "admission_policy_for_version",
    "build_runtime_resource_admission",
    "compose_production_resource_admission_authority",
    "trading_session_at",
]
