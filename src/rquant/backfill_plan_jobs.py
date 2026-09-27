"""Durable, read-only Lab maintenance jobs for daily-bar backfill proposals."""

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
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

import duckdb
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.backfill_plan_artifact import (
    BackfillSnapshotFileIdentity,
    capture_backfill_snapshot_identity,
    create_and_publish_daily_bar_backfill_plan,
    load_daily_bar_backfill_plan,
)
from rquant.backfill_plan_core import BackfillEstimateAssumptions, DailyBarBackfillPlan
from rquant.data_audit_contracts import MAX_AUDIT_DAYS

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_JOBS = 4096
_PLAN_PREFIX = "daily-bar-backfill-plan-v1-"
_JobStatus = Literal["queued", "running", "succeeded", "failed"]
_ErrorCode = Literal[
    "snapshot_changed", "invalid_evidence", "artifact_invalid", "internal_error"
]


class _JobModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class BackfillPlanJobRequest(_JobModel):
    """Trusted Lab input; Web must never supply paths or claimed snapshot identity."""

    idempotency_key: str = Field(pattern=r"^[A-Za-z0-9_-]{16,64}$")
    snapshot_path: Path
    snapshot_file_identity: BackfillSnapshotFileIdentity
    snapshot_label: str = Field(min_length=1, max_length=128)
    evidence_code_revision: str = Field(min_length=1, max_length=80)
    audit_start: date
    completed_through: date
    observed_at: datetime
    assumptions: BackfillEstimateAssumptions

    @field_validator("snapshot_path")
    @classmethod
    def require_absolute_path(cls, path: Path) -> Path:
        if not path.is_absolute():
            raise ValueError("snapshot path must be absolute")
        return path

    @model_validator(mode="after")
    def validate_bounds(self) -> BackfillPlanJobRequest:
        if not self.snapshot_label.strip() or not self.evidence_code_revision.strip():
            raise ValueError("source labels must be non-empty")
        days = (self.completed_through - self.audit_start).days + 1
        if days < 1 or days > MAX_AUDIT_DAYS:
            raise ValueError(f"audit range must contain 1 to {MAX_AUDIT_DAYS} days")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        local = self.observed_at.astimezone(_SHANGHAI)
        if self.completed_through > local.date() or (
            self.completed_through == local.date() and local.time() < time(15)
        ):
            raise ValueError("completed_through must be after Shanghai market close")
        if not self.completed_through <= self.assumptions.status_source_as_of <= local.date():
            raise ValueError("status source date must include the cutoff without being future")
        return self


class BackfillPlanJobReceipt(_JobModel):
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    status: _JobStatus
    attempts: int = Field(ge=0, strict=True)
    created_at: datetime
    updated_at: datetime
    plan_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    error_code: _ErrorCode | None = None

    @model_validator(mode="after")
    def validate_state(self) -> BackfillPlanJobReceipt:
        if (self.status == "succeeded") != (self.plan_hash is not None):
            raise ValueError("only succeeded jobs may carry a plan hash")
        if (self.status == "failed") != (self.error_code is not None):
            raise ValueError("only failed jobs may carry an error code")
        return self


class BackfillPlanArtifactUnavailableError(RuntimeError):
    """A persisted success cannot currently be verified against its sealed artifact."""


@dataclass(frozen=True)
class _Claim:
    task_id: str
    token: str
    request: BackfillPlanJobRequest
    snapshot_sha256: str | None


def _canonical_request(request: BackfillPlanJobRequest) -> str:
    return json.dumps(
        request.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def _utc(clock: Callable[[], datetime]) -> datetime:
    now = clock()
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("job clock must be timezone-aware")
    return now.astimezone(UTC)


def _plan_matches_request(
    plan: DailyBarBackfillPlan,
    request: BackfillPlanJobRequest,
    snapshot_sha256: str,
) -> bool:
    return (
        plan.source.claimed_file_sha256 == snapshot_sha256
        and plan.source.snapshot_label == request.snapshot_label
        and plan.evidence.code_revision == request.evidence_code_revision
        and plan.audit_start == request.audit_start
        and plan.completed_through == request.completed_through
        and plan.cutoff_observed_at_utc == request.observed_at.astimezone(UTC)
        and plan.estimate.assumptions == request.assumptions
        and plan.executable is False
    )


class BackfillPlanJobStore:
    """A bounded SQLite task outbox; submission never opens the DuckDB snapshot."""

    def __init__(
        self,
        *,
        state_path: Path,
        plan_directory: Path,
        clock: Callable[[], datetime] | None = None,
        lease_seconds: int = 120,
    ) -> None:
        if not state_path.is_absolute() or not plan_directory.is_absolute():
            raise ValueError("job state and plan directory must use absolute paths")
        if lease_seconds < 3 or lease_seconds > 3600:
            raise ValueError("job lease must be between 3 and 3600 seconds")
        self.state_path = state_path
        self.plan_directory = plan_directory
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lease_seconds = lease_seconds
        state_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_plan_job (
                    task_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    request_json TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('queued','running','succeeded','failed')),
                    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                    lease_token TEXT,
                    lease_until TEXT,
                    plan_hash TEXT,
                    snapshot_sha256 TEXT,
                    error_code TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.state_path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def submit(self, request: BackfillPlanJobRequest) -> BackfillPlanJobReceipt:
        request = BackfillPlanJobRequest.model_validate(request)
        payload = _canonical_request(request)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        now = _utc(self.clock).isoformat()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM backfill_plan_job WHERE idempotency_key = ?",
                (request.idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["request_sha256"] != digest or existing["request_json"] != payload:
                    raise ValueError("idempotency key already binds a different request")
                task_id = existing["task_id"]
            else:
                if (
                    capture_backfill_snapshot_identity(request.snapshot_path)
                    != request.snapshot_file_identity
                ):
                    raise ValueError("snapshot file identity changed before task submission")
                count = connection.execute("SELECT COUNT(*) FROM backfill_plan_job").fetchone()[0]
                if count >= _MAX_JOBS:
                    raise ValueError("backfill plan job capacity reached")
                task_id = uuid4().hex
                connection.execute(
                    """
                    INSERT INTO backfill_plan_job (
                        task_id, idempotency_key, request_json, request_sha256, status,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'queued', ?, ?)
                    """,
                    (task_id, request.idempotency_key, payload, digest, now, now),
                )
        return self.status(task_id)

    def status(self, task_id: str) -> BackfillPlanJobReceipt:
        if re.fullmatch(r"[0-9a-f]{32}", task_id) is None:
            raise ValueError("invalid backfill plan task id")
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM backfill_plan_job WHERE task_id = ?", (task_id,)
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise KeyError(task_id)
        receipt = self._receipt(row)
        if receipt.status == "succeeded":
            request = BackfillPlanJobRequest.model_validate_json(row["request_json"])
            assert receipt.plan_hash is not None
            self._verify_artifact(receipt.plan_hash, request, row["snapshot_sha256"])
        return receipt

    def lookup_by_key(
        self, idempotency_key: str
    ) -> tuple[BackfillPlanJobRequest, BackfillPlanJobReceipt] | None:
        """Look up the original request and its integrity-checked current status."""
        admission = self.admission_by_key(idempotency_key)
        if admission is None:
            return None
        request, task_id = admission
        return request, self.status(task_id)

    def admission_by_key(
        self, idempotency_key: str
    ) -> tuple[BackfillPlanJobRequest, str] | None:
        """Read the durable task binding without treating plan generation as admission."""
        if re.fullmatch(r"[A-Za-z0-9_-]{16,64}", idempotency_key) is None:
            raise ValueError("invalid backfill plan idempotency key")
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT task_id, request_json FROM backfill_plan_job WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            return None
        request = BackfillPlanJobRequest.model_validate_json(row["request_json"])
        return request, row["task_id"]

    def retry_failed(self, task_id: str) -> BackfillPlanJobReceipt:
        with self._transaction() as connection:
            changed = connection.execute(
                """
                UPDATE backfill_plan_job
                SET status = 'queued', error_code = NULL, updated_at = ?
                WHERE task_id = ? AND status = 'failed'
                """,
                (_utc(self.clock).isoformat(), task_id),
            ).rowcount
            if changed != 1:
                raise ValueError("only a failed backfill plan task can be retried")
        return self.status(task_id)

    @staticmethod
    def _receipt(row: sqlite3.Row) -> BackfillPlanJobReceipt:
        return BackfillPlanJobReceipt(
            task_id=row["task_id"],
            status=row["status"],
            attempts=row["attempts"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            plan_hash=row["plan_hash"],
            error_code=row["error_code"],
        )

    def _verify_artifact(
        self, plan_hash: str, request: BackfillPlanJobRequest, snapshot_sha256: str | None
    ) -> None:
        if _SHA256.fullmatch(plan_hash) is None or snapshot_sha256 is None or _SHA256.fullmatch(
            snapshot_sha256
        ) is None:
            raise BackfillPlanArtifactUnavailableError("stored plan identity is invalid")
        path = self.plan_directory / f"{_PLAN_PREFIX}{plan_hash}.json"
        try:
            plan = load_daily_bar_backfill_plan(path)
            if plan.content_sha256 != plan_hash or not _plan_matches_request(
                plan, request, snapshot_sha256
            ):
                raise ValueError("plan and task identities disagree")
        except (OSError, ValueError) as exc:
            raise BackfillPlanArtifactUnavailableError("sealed plan cannot be verified") from exc

    def _claim(self) -> _Claim | None:
        now = _utc(self.clock)
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT * FROM backfill_plan_job
                WHERE status = 'queued' OR (status = 'running' AND lease_until <= ?)
                ORDER BY created_at, task_id LIMIT 1
                """,
                (now.isoformat(),),
            ).fetchone()
            if row is None:
                return None
            token = secrets.token_hex(16)
            connection.execute(
                """
                UPDATE backfill_plan_job
                SET status = 'running', attempts = attempts + 1, lease_token = ?,
                    lease_until = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (
                    token,
                    (now + timedelta(seconds=self.lease_seconds)).isoformat(),
                    now.isoformat(),
                    row["task_id"],
                ),
            )
            return _Claim(
                task_id=row["task_id"],
                token=token,
                request=BackfillPlanJobRequest.model_validate_json(row["request_json"]),
                snapshot_sha256=row["snapshot_sha256"],
            )

    def _record_snapshot_sha256(self, claim: _Claim, digest: str) -> None:
        if _SHA256.fullmatch(digest) is None:
            raise ValueError("snapshot SHA256 digest is invalid")
        if claim.snapshot_sha256 is not None and claim.snapshot_sha256 != digest:
            raise ValueError("snapshot SHA256 digest changed during task recovery")
        now = _utc(self.clock).isoformat()
        with self._transaction() as connection:
            changed = connection.execute(
                """
                UPDATE backfill_plan_job
                SET snapshot_sha256 = ?, updated_at = ?
                WHERE task_id = ? AND status = 'running' AND lease_token = ?
                  AND lease_until > ?
                  AND (snapshot_sha256 IS NULL OR snapshot_sha256 = ?)
                """,
                (digest, now, claim.task_id, claim.token, now, digest),
            ).rowcount
            if changed != 1:
                raise RuntimeError("backfill plan task lease or source identity was lost")

    def _renew(self, claim: _Claim) -> bool:
        now = _utc(self.clock)
        with self._transaction() as connection:
            return (
                connection.execute(
                    """
                    UPDATE backfill_plan_job SET lease_until = ?, updated_at = ?
                    WHERE task_id = ? AND status = 'running' AND lease_token = ?
                      AND lease_until > ?
                    """,
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

    def _finish_success(
        self, claim: _Claim, plan_hash: str, snapshot_sha256: str
    ) -> BackfillPlanJobReceipt:
        self._verify_artifact(plan_hash, claim.request, snapshot_sha256)
        now = _utc(self.clock).isoformat()
        with self._transaction() as connection:
            changed = connection.execute(
                """
                UPDATE backfill_plan_job
                SET status = 'succeeded', plan_hash = ?, lease_token = NULL,
                    lease_until = NULL, updated_at = ?
                WHERE task_id = ? AND status = 'running' AND lease_token = ?
                  AND lease_until > ? AND snapshot_sha256 = ?
                """,
                (plan_hash, now, claim.task_id, claim.token, now, snapshot_sha256),
            ).rowcount
            if changed != 1:
                raise RuntimeError("backfill plan task lease was lost")
        return self.status(claim.task_id)

    def _finish_failure(self, claim: _Claim, error_code: _ErrorCode) -> BackfillPlanJobReceipt:
        now = _utc(self.clock).isoformat()
        with self._transaction() as connection:
            changed = connection.execute(
                """
                UPDATE backfill_plan_job
                SET status = 'failed', error_code = ?, lease_token = NULL,
                    lease_until = NULL, updated_at = ?
                WHERE task_id = ? AND status = 'running' AND lease_token = ?
                  AND lease_until > ?
                """,
                (error_code, now, claim.task_id, claim.token, now),
            ).rowcount
            if changed != 1:
                raise RuntimeError("backfill plan task lease was lost")
        return self.status(claim.task_id)


def _error_code(error: Exception) -> _ErrorCode:
    message = str(error).lower()
    if isinstance(error, OSError) or any(
        word in message for word in ("snapshot", "sha256 digest", "source file")
    ):
        return "snapshot_changed"
    if isinstance(error, BackfillPlanArtifactUnavailableError) or "backfill plan" in message:
        return "artifact_invalid"
    if isinstance(error, (ValueError, duckdb.Error)):
        return "invalid_evidence"
    return "internal_error"


class BackfillPlanJobWorker:
    """Run one claimed read-only task; expired claims are fenced and safely replayed."""

    def __init__(self, store: BackfillPlanJobStore) -> None:
        self.store = store

    def run_one(self) -> BackfillPlanJobReceipt | None:
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

        renewer = threading.Thread(target=heartbeat, name="backfill-plan-lease", daemon=True)
        renewer.start()
        try:
            request = claim.request
            current_identity = capture_backfill_snapshot_identity(request.snapshot_path)
            if claim.snapshot_sha256 is None:
                if current_identity != request.snapshot_file_identity:
                    raise ValueError("snapshot file identity changed before task execution")
            elif (
                current_identity.device,
                current_identity.inode,
                current_identity.size,
                current_identity.mtime_ns,
            ) != (
                request.snapshot_file_identity.device,
                request.snapshot_file_identity.inode,
                request.snapshot_file_identity.size,
                request.snapshot_file_identity.mtime_ns,
            ):
                raise ValueError("snapshot file identity changed before task recovery")
            path = create_and_publish_daily_bar_backfill_plan(
                snapshot_path=request.snapshot_path,
                expected_file_sha256=claim.snapshot_sha256,
                expected_file_identity=current_identity,
                on_source_sha256=lambda digest: self.store._record_snapshot_sha256(
                    claim, digest
                ),
                snapshot_label=request.snapshot_label,
                evidence_code_revision=request.evidence_code_revision,
                audit_start=request.audit_start,
                completed_through=request.completed_through,
                observed_at=request.observed_at,
                assumptions=request.assumptions,
                directory=self.store.plan_directory,
            )
            plan = load_daily_bar_backfill_plan(path)
            snapshot_sha256 = plan.source.claimed_file_sha256
            if lost.is_set():
                raise RuntimeError("backfill plan task lease was lost")
            return self.store._finish_success(claim, plan.content_sha256, snapshot_sha256)
        except Exception as exc:
            if lost.is_set():
                raise RuntimeError("backfill plan task lease was lost") from exc
            return self.store._finish_failure(claim, _error_code(exc))
        finally:
            stopped.set()
            renewer.join(timeout=self.store.lease_seconds / 3 + 1)
