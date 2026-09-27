"""Durable, bounded read-only daily-bar audit report commands."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
import threading
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal
from uuid import uuid4

import duckdb
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.data_audit_evidence import MAX_AUDIT_DAYS, DailyBarNullFieldSpec
from rquant.data_audit_report import (
    AuditReplicaFileIdentity,
    DataAuditReplicaChangedError,
    capture_data_audit_replica_identity,
    create_and_publish_data_audit_report,
    load_data_audit_report,
)

_MAX_JOBS = 4096
_MAX_EVENTS = 32
_COOLDOWN = timedelta(minutes=10)
_TASK_ID = re.compile(r"[0-9a-f]{32}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_Status = Literal["queued", "running", "succeeded", "failed"]
_EventType = Literal["queued", "started", "resumed", "source_check", "succeeded", "failed"]
_ErrorCode = Literal["replica_changed", "invalid_evidence", "artifact_invalid", "internal_error"]


class _JobModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class DataAuditReportJobRequest(_JobModel):
    """Trusted caller input; the browser must never choose paths or file identity."""

    idempotency_key: str = Field(pattern=r"^[A-Za-z0-9_-]{16,64}$")
    primary_path: Path
    replica_path: Path
    replica_file_identity: AuditReplicaFileIdentity
    audit_start: date
    observed_through: date
    null_fields: tuple[DailyBarNullFieldSpec, ...] = Field(min_length=1, max_length=9)

    @field_validator("primary_path", "replica_path")
    @classmethod
    def require_absolute_path(cls, path: Path) -> Path:
        if not path.is_absolute() or path != Path(str(path.resolve(strict=False))):
            raise ValueError("audit source paths must be absolute and canonical")
        return path

    @model_validator(mode="after")
    def validate_range(self) -> DataAuditReportJobRequest:
        days = (self.observed_through - self.audit_start).days + 1
        if days < 1 or days > MAX_AUDIT_DAYS:
            raise ValueError(f"audit range must contain 1 to {MAX_AUDIT_DAYS} days")
        names = [field.field_name for field in self.null_fields]
        if len(set(names)) != len(names):
            raise ValueError("NULL audit fields must be unique")
        return self


class DataAuditReportJobReceipt(_JobModel):
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    status: _Status
    attempts: int = Field(ge=0, strict=True)
    created_at: datetime
    updated_at: datetime
    report_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    error_code: _ErrorCode | None = None

    @model_validator(mode="after")
    def validate_state(self) -> DataAuditReportJobReceipt:
        if (self.status == "succeeded") != (self.report_hash is not None):
            raise ValueError("only succeeded audit tasks may carry a report hash")
        if (self.status == "failed") != (self.error_code is not None):
            raise ValueError("only failed audit tasks may carry an error code")
        return self


class DataAuditReportJobEvent(_JobModel):
    event_id: int = Field(gt=0, strict=True)
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    event_type: _EventType
    attempts: int = Field(ge=0, strict=True)
    occurred_at: datetime
    error_code: _ErrorCode | None = None

    @model_validator(mode="after")
    def validate_error(self) -> DataAuditReportJobEvent:
        if (self.event_type == "failed") != (self.error_code is not None):
            raise ValueError("only failed events may carry an error code")
        return self


class DataAuditReportArtifactUnavailableError(RuntimeError):
    """A stored success cannot be verified against its immutable artifact."""


class _ReplicaChangedError(ValueError):
    pass


@dataclass(frozen=True)
class _Claim:
    task_id: str
    token: str
    attempts: int
    request: DataAuditReportJobRequest
    replica_sha256: str | None


def _canonical_request(request: DataAuditReportJobRequest) -> str:
    return json.dumps(
        request.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def _utc(clock: Callable[[], datetime]) -> datetime:
    now = clock()
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("audit task clock must be timezone-aware")
    return now.astimezone(UTC)


def _same_pre_pin_identity(
    actual: AuditReplicaFileIdentity,
    expected: AuditReplicaFileIdentity,
    *,
    digest_recorded: bool,
) -> bool:
    if not digest_recorded:
        return actual == expected
    return (
        actual.device,
        actual.inode,
        actual.size,
        actual.mtime_ns,
    ) == (
        expected.device,
        expected.inode,
        expected.size,
        expected.mtime_ns,
    )


class DataAuditReportJobStore:
    """SQLite task state; submission performs only constant-time filesystem checks."""

    def __init__(
        self,
        *,
        state_path: Path,
        report_directory: Path,
        clock: Callable[[], datetime] | None = None,
        lease_seconds: int = 120,
    ) -> None:
        if not state_path.is_absolute() or not report_directory.is_absolute():
            raise ValueError("audit job paths must be absolute")
        if lease_seconds < 3 or lease_seconds > 3600:
            raise ValueError("audit job lease must be between 3 and 3600 seconds")
        self.state_path = state_path
        self.report_directory = report_directory
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lease_seconds = lease_seconds
        state_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            with self._transaction_on(connection):
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS data_audit_report_job (
                        task_id TEXT PRIMARY KEY,
                        idempotency_key TEXT NOT NULL UNIQUE,
                        request_json TEXT NOT NULL,
                        request_sha256 TEXT NOT NULL,
                        status TEXT NOT NULL CHECK(status IN (
                            'queued','running','succeeded','failed'
                        )),
                        attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                        lease_token TEXT,
                        lease_until TEXT,
                        replica_sha256 TEXT,
                        report_hash TEXT,
                        error_code TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS data_audit_report_one_active
                    ON data_audit_report_job((1)) WHERE status IN ('queued','running')
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS data_audit_report_job_event (
                        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        task_id TEXT NOT NULL,
                        event_type TEXT NOT NULL CHECK(event_type IN (
                            'queued','started','resumed','source_check','succeeded','failed'
                        )),
                        attempts INTEGER NOT NULL CHECK(attempts >= 0),
                        occurred_at TEXT NOT NULL,
                        error_code TEXT CHECK(error_code IN (
                            'replica_changed','invalid_evidence','artifact_invalid','internal_error'
                        )),
                        CHECK((event_type = 'failed') = (error_code IS NOT NULL))
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS data_audit_report_job_event_order
                    ON data_audit_report_job_event(task_id, event_id)
                    """
                )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.state_path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    @contextmanager
    def _transaction_on(connection: sqlite3.Connection) -> Iterator[None]:
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with closing(self._connect()) as connection, self._transaction_on(connection):
            yield connection

    @staticmethod
    def _record_event(
        connection: sqlite3.Connection,
        *,
        task_id: str,
        event_type: _EventType,
        attempts: int,
        occurred_at: str,
        error_code: _ErrorCode | None = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO data_audit_report_job_event
                (task_id, event_type, attempts, occurred_at, error_code)
            VALUES (?, ?, ?, ?, ?)
            """,
            (task_id, event_type, attempts, occurred_at, error_code),
        )
        connection.execute(
            """
            DELETE FROM data_audit_report_job_event
            WHERE task_id = ? AND event_id NOT IN (
                SELECT event_id FROM data_audit_report_job_event
                WHERE task_id = ? ORDER BY event_id DESC LIMIT ?
            )
            """,
            (task_id, task_id, _MAX_EVENTS),
        )

    def submit(self, request: DataAuditReportJobRequest) -> DataAuditReportJobReceipt:
        request = DataAuditReportJobRequest.model_validate(request)
        payload = _canonical_request(request)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        now = _utc(self.clock)
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM data_audit_report_job WHERE idempotency_key = ?",
                (request.idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["request_json"] != payload or existing["request_sha256"] != digest:
                    raise ValueError("idempotency key already binds a different audit request")
                task_id = existing["task_id"]
            else:
                current = capture_data_audit_replica_identity(
                    request.primary_path, request.replica_path
                )
                if current != request.replica_file_identity:
                    raise _ReplicaChangedError("submitted replica file identity changed")
                active = connection.execute(
                    "SELECT 1 FROM data_audit_report_job "
                    "WHERE status IN ('queued','running') LIMIT 1"
                ).fetchone()
                if active is not None:
                    raise ValueError("another audit report task is active")
                latest = connection.execute(
                    "SELECT created_at FROM data_audit_report_job ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
                if latest is not None and now - datetime.fromisoformat(latest[0]) < _COOLDOWN:
                    raise ValueError("audit report task cooldown has not elapsed")
                count = connection.execute("SELECT COUNT(*) FROM data_audit_report_job").fetchone()[
                    0
                ]
                if count >= _MAX_JOBS:
                    raise ValueError("audit report task capacity reached")
                task_id = uuid4().hex
                connection.execute(
                    """
                    INSERT INTO data_audit_report_job (
                        task_id, idempotency_key, request_json, request_sha256,
                        status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'queued', ?, ?)
                    """,
                    (
                        task_id,
                        request.idempotency_key,
                        payload,
                        digest,
                        now.isoformat(),
                        now.isoformat(),
                    ),
                )
                self._record_event(
                    connection,
                    task_id=task_id,
                    event_type="queued",
                    attempts=0,
                    occurred_at=now.isoformat(),
                )
        return self.status(task_id)

    @staticmethod
    def _receipt(row: sqlite3.Row) -> DataAuditReportJobReceipt:
        return DataAuditReportJobReceipt(
            task_id=row["task_id"],
            status=row["status"],
            attempts=row["attempts"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            report_hash=row["report_hash"],
            error_code=row["error_code"],
        )

    def _verify_artifact(
        self,
        *,
        report_hash: str | None,
        source_sha256: str | None,
        request: DataAuditReportJobRequest,
    ) -> None:
        if (
            report_hash is None
            or _SHA256.fullmatch(report_hash) is None
            or source_sha256 is None
            or _SHA256.fullmatch(source_sha256) is None
        ):
            raise DataAuditReportArtifactUnavailableError("stored audit report identity is invalid")
        path = self.report_directory / f"data-audit-v1-{report_hash}.json"
        try:
            report = load_data_audit_report(path)
            if (
                report.content_hash != report_hash
                or report.source.mode != "production_unverified"
                or report.source.namespace != "production"
                or report.source.snapshot_label != f"sha256:{source_sha256}"
                or report.audit_start != request.audit_start
                or report.observed_through != request.observed_through
                or report.null_fields
                != tuple(sorted(request.null_fields, key=lambda field: field.field_name))
                or report.collection_status != "collection_unconfirmed"
            ):
                raise ValueError("audit report and stored task disagree")
        except (OSError, ValueError) as exc:
            raise DataAuditReportArtifactUnavailableError(
                "sealed audit report cannot be verified"
            ) from exc

    def _status_row(self, row: sqlite3.Row) -> DataAuditReportJobReceipt:
        receipt = self._receipt(row)
        if receipt.status == "succeeded":
            self._verify_artifact(
                report_hash=row["report_hash"],
                source_sha256=row["replica_sha256"],
                request=DataAuditReportJobRequest.model_validate_json(row["request_json"]),
            )
        return receipt

    def status(self, task_id: str) -> DataAuditReportJobReceipt:
        if _TASK_ID.fullmatch(task_id) is None:
            raise ValueError("invalid audit report task id")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM data_audit_report_job WHERE task_id = ?", (task_id,)
            ).fetchone()
        if row is None:
            raise KeyError(task_id)
        return self._status_row(row)

    def lookup_by_key(
        self, idempotency_key: str
    ) -> tuple[DataAuditReportJobRequest, DataAuditReportJobReceipt] | None:
        if re.fullmatch(r"[A-Za-z0-9_-]{16,64}", idempotency_key) is None:
            raise ValueError("invalid audit report idempotency key")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM data_audit_report_job WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        if row is None:
            return None
        request = DataAuditReportJobRequest.model_validate_json(row["request_json"])
        return request, self._status_row(row)

    def latest(self) -> DataAuditReportJobReceipt | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM data_audit_report_job ORDER BY created_at DESC, task_id DESC LIMIT 1"
            ).fetchone()
        return None if row is None else self._status_row(row)

    def latest_success(self) -> DataAuditReportJobReceipt | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT * FROM data_audit_report_job WHERE status = 'succeeded'
                ORDER BY created_at DESC, task_id DESC LIMIT 1"""
            ).fetchone()
        return None if row is None else self._status_row(row)

    def list_events(self, task_id: str, *, limit: int = 20) -> tuple[DataAuditReportJobEvent, ...]:
        if type(limit) is not int or not 1 <= limit <= _MAX_EVENTS:
            raise ValueError("audit report event limit is out of bounds")
        self.status(task_id)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT event_id, task_id, event_type, attempts, occurred_at, error_code
                FROM data_audit_report_job_event WHERE task_id = ?
                ORDER BY event_id DESC LIMIT ?""",
                (task_id, limit),
            ).fetchall()
        return tuple(
            DataAuditReportJobEvent(
                event_id=row["event_id"],
                task_id=row["task_id"],
                event_type=row["event_type"],
                attempts=row["attempts"],
                occurred_at=datetime.fromisoformat(row["occurred_at"]),
                error_code=row["error_code"],
            )
            for row in reversed(rows)
        )

    def _claim(self) -> _Claim | None:
        now = _utc(self.clock)
        with self._transaction() as connection:
            row = connection.execute(
                """SELECT * FROM data_audit_report_job
                WHERE status = 'queued' OR (status = 'running' AND lease_until <= ?)
                ORDER BY created_at, task_id LIMIT 1""",
                (now.isoformat(),),
            ).fetchone()
            if row is None:
                return None
            token = secrets.token_hex(16)
            connection.execute(
                """UPDATE data_audit_report_job SET status = 'running',
                attempts = attempts + 1, lease_token = ?, lease_until = ?, updated_at = ?
                WHERE task_id = ?""",
                (
                    token,
                    (now + timedelta(seconds=self.lease_seconds)).isoformat(),
                    now.isoformat(),
                    row["task_id"],
                ),
            )
            self._record_event(
                connection,
                task_id=row["task_id"],
                event_type="resumed" if row["status"] == "running" else "started",
                attempts=row["attempts"] + 1,
                occurred_at=now.isoformat(),
            )
            return _Claim(
                task_id=row["task_id"],
                token=token,
                attempts=row["attempts"] + 1,
                request=DataAuditReportJobRequest.model_validate_json(row["request_json"]),
                replica_sha256=row["replica_sha256"],
            )

    def _record_replica_sha256(self, claim: _Claim, digest: str) -> None:
        if _SHA256.fullmatch(digest) is None:
            raise ValueError("replica SHA256 is invalid")
        if claim.replica_sha256 is not None and digest != claim.replica_sha256:
            raise _ReplicaChangedError("replica content changed during recovery")
        now = _utc(self.clock).isoformat()
        with self._transaction() as connection:
            changed = connection.execute(
                """UPDATE data_audit_report_job SET replica_sha256 = ?, updated_at = ?
                WHERE task_id = ? AND status = 'running' AND lease_token = ?
                  AND lease_until > ? AND (replica_sha256 IS NULL OR replica_sha256 = ?)""",
                (digest, now, claim.task_id, claim.token, now, digest),
            ).rowcount
            if changed != 1:
                raise RuntimeError("audit report task lease or source identity was lost")
            self._record_event(
                connection,
                task_id=claim.task_id,
                event_type="source_check",
                attempts=claim.attempts,
                occurred_at=now,
            )

    def _renew(self, claim: _Claim) -> bool:
        now = _utc(self.clock)
        with self._transaction() as connection:
            return (
                connection.execute(
                    """UPDATE data_audit_report_job SET lease_until = ?, updated_at = ?
                WHERE task_id = ? AND status = 'running' AND lease_token = ? AND lease_until > ?""",
                    (
                        (now + timedelta(seconds=self.lease_seconds)).isoformat(),
                        now.isoformat(),
                        claim.task_id,
                        claim.token,
                        now.isoformat(),
                    ),
                ).rowcount
                == 1
            )

    def _finish_success(self, claim: _Claim, report_hash: str) -> DataAuditReportJobReceipt:
        now = _utc(self.clock).isoformat()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM data_audit_report_job WHERE task_id = ?", (claim.task_id,)
            ).fetchone()
            if row is None or row["replica_sha256"] is None:
                raise RuntimeError("audit report task source identity is absent")
            self._verify_artifact(
                report_hash=report_hash,
                source_sha256=row["replica_sha256"],
                request=claim.request,
            )
            changed = connection.execute(
                """UPDATE data_audit_report_job SET status = 'succeeded', report_hash = ?,
                lease_token = NULL, lease_until = NULL, updated_at = ?
                WHERE task_id = ? AND status = 'running' AND lease_token = ? AND lease_until > ?""",
                (report_hash, now, claim.task_id, claim.token, now),
            ).rowcount
            if changed != 1:
                raise RuntimeError("audit report task lease was lost")
            self._record_event(
                connection,
                task_id=claim.task_id,
                event_type="succeeded",
                attempts=claim.attempts,
                occurred_at=now,
            )
        return self.status(claim.task_id)

    def _finish_failure(self, claim: _Claim, error_code: _ErrorCode) -> DataAuditReportJobReceipt:
        now = _utc(self.clock).isoformat()
        with self._transaction() as connection:
            changed = connection.execute(
                """UPDATE data_audit_report_job SET status = 'failed', error_code = ?,
                lease_token = NULL, lease_until = NULL, updated_at = ?
                WHERE task_id = ? AND status = 'running' AND lease_token = ? AND lease_until > ?""",
                (error_code, now, claim.task_id, claim.token, now),
            ).rowcount
            if changed != 1:
                raise RuntimeError("audit report task lease was lost")
            self._record_event(
                connection,
                task_id=claim.task_id,
                event_type="failed",
                attempts=claim.attempts,
                occurred_at=now,
                error_code=error_code,
            )
        return self.status(claim.task_id)


def _error_code(error: Exception) -> _ErrorCode:
    if isinstance(error, (_ReplicaChangedError, DataAuditReplicaChangedError, OSError)):
        return "replica_changed"
    if isinstance(error, DataAuditReportArtifactUnavailableError):
        return "artifact_invalid"
    if isinstance(error, (ValueError, duckdb.Error)):
        return "invalid_evidence"
    return "internal_error"


class DataAuditReportJobWorker:
    """Run one claim with a renewable lease; stale worker results are fenced."""

    def __init__(self, store: DataAuditReportJobStore) -> None:
        self.store = store

    def run_one(self) -> DataAuditReportJobReceipt | None:
        claim = self.store._claim()
        if claim is None:
            return None
        stopped = threading.Event()
        lost = threading.Event()

        def heartbeat() -> None:
            while not stopped.wait(self.store.lease_seconds / 3):
                try:
                    if not self.store._renew(claim):
                        lost.set()
                        return
                except Exception:
                    lost.set()
                    return

        renewer = threading.Thread(target=heartbeat, name="data-audit-report-lease", daemon=True)
        renewer.start()
        try:
            request = claim.request
            try:
                current = capture_data_audit_replica_identity(
                    request.primary_path, request.replica_path
                )
            except (OSError, ValueError) as exc:
                raise _ReplicaChangedError("replica identity changed before audit") from exc
            if not _same_pre_pin_identity(
                current,
                request.replica_file_identity,
                digest_recorded=claim.replica_sha256 is not None,
            ):
                raise _ReplicaChangedError("replica identity changed before audit")
            report_path = create_and_publish_data_audit_report(
                primary_path=request.primary_path,
                replica_path=request.replica_path,
                audit_start=request.audit_start,
                observed_through=request.observed_through,
                null_fields=request.null_fields,
                directory=self.store.report_directory,
                expected_file_identity=request.replica_file_identity,
                expected_file_sha256=claim.replica_sha256,
                on_replica_sha256=lambda digest: self.store._record_replica_sha256(claim, digest),
            )
            report = load_data_audit_report(report_path)
            if lost.is_set():
                raise RuntimeError("audit report task lease was lost")
            return self.store._finish_success(claim, report.content_hash)
        except Exception as exc:
            if lost.is_set():
                raise RuntimeError("audit report task lease was lost") from exc
            return self.store._finish_failure(claim, _error_code(exc))
        finally:
            stopped.set()
            renewer.join(timeout=self.store.lease_seconds / 3 + 1)
