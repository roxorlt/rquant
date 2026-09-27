"""Bounded read-only Serving snapshot of daily-bar audit report task state."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from rquant.data_audit_report_jobs import (
    DataAuditReportJobEvent,
    DataAuditReportJobReceipt,
    DataAuditReportJobRequest,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc
from rquant.serving_read_models import ServingProjectionPayload

MAX_AUDIT_REPORT_PROGRESS_EVENTS = 20
_MAX_STORED_JOBS = 4096
_MAX_STORED_EVENTS = 32
_MAX_REQUEST_BYTES = 16 * 1024
_MAX_STATE_BYTES = 64 * 1024 * 1024
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_EXPECTED_LAST_EVENTS = {
    "queued": frozenset({"queued"}),
    "running": frozenset({"started", "resumed", "source_check"}),
    "succeeded": frozenset({"succeeded"}),
    "failed": frozenset({"failed"}),
}


class DataAuditReportJobProgress(RuntimeContractModel):
    status_key: Literal["current"] = "current"
    availability: Literal["unavailable", "empty", "ready"]
    latest_task_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    latest_status: Literal["queued", "running", "succeeded", "failed"] | None = None
    latest_attempts: int | None = Field(default=None, ge=0, strict=True)
    latest_created_at: AwareUtcDatetime | None = None
    latest_updated_at: AwareUtcDatetime | None = None
    latest_error_code: str | None = None
    successful_task_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    successful_report_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    successful_created_at: AwareUtcDatetime | None = None
    successful_updated_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def validate_state(self) -> DataAuditReportJobProgress:
        latest = (
            self.latest_task_id,
            self.latest_status,
            self.latest_attempts,
            self.latest_created_at,
            self.latest_updated_at,
        )
        success = (
            self.successful_task_id,
            self.successful_report_hash,
            self.successful_created_at,
            self.successful_updated_at,
        )
        if self.availability != "ready":
            if any(value is not None for value in (*latest, *success, self.latest_error_code)):
                raise ValueError("empty audit task state cannot carry tasks")
            return self
        if any(value is None for value in latest):
            raise ValueError("ready audit task state lacks its latest attempt")
        assert self.latest_created_at is not None and self.latest_updated_at is not None
        if self.latest_created_at > self.latest_updated_at:
            raise ValueError("audit task timestamps disagree")
        if (self.latest_status == "failed") != (self.latest_error_code is not None):
            raise ValueError("audit task error code disagrees with status")
        if any(value is not None for value in success) and any(value is None for value in success):
            raise ValueError("last successful audit task is incomplete")
        if self.successful_created_at is not None:
            assert self.successful_updated_at is not None
            if (
                self.successful_created_at > self.successful_updated_at
                or self.successful_created_at > self.latest_created_at
                or self.successful_updated_at > self.latest_updated_at
            ):
                raise ValueError("audit success occurred after the latest attempt")
        if self.latest_status == "succeeded" and (self.latest_task_id != self.successful_task_id):
            raise ValueError("latest successful task differs from success pointer")
        return self


@dataclass(frozen=True)
class DataAuditReportSuccessfulTask:
    receipt: DataAuditReportJobReceipt
    request: DataAuditReportJobRequest
    replica_sha256: str


@dataclass(frozen=True)
class DataAuditReportJobSnapshot:
    availability: Literal["unavailable", "empty", "ready"]
    latest: DataAuditReportJobReceipt | None
    successful: DataAuditReportSuccessfulTask | None
    events: tuple[DataAuditReportJobEvent, ...]
    available_at: datetime

    def progress(self) -> DataAuditReportJobProgress:
        latest = self.latest
        successful = self.successful
        return DataAuditReportJobProgress(
            availability=self.availability,
            latest_task_id=None if latest is None else latest.task_id,
            latest_status=None if latest is None else latest.status,
            latest_attempts=None if latest is None else latest.attempts,
            latest_created_at=None if latest is None else latest.created_at,
            latest_updated_at=None if latest is None else latest.updated_at,
            latest_error_code=None if latest is None else latest.error_code,
            successful_task_id=None if successful is None else successful.receipt.task_id,
            successful_report_hash=None if successful is None else successful.receipt.report_hash,
            successful_created_at=None if successful is None else successful.receipt.created_at,
            successful_updated_at=None if successful is None else successful.receipt.updated_at,
        )


def validate_data_audit_report_job_progress(
    progress: DataAuditReportJobProgress,
    events: tuple[DataAuditReportJobEvent, ...],
    *,
    available_at: datetime,
    report_hash: str | None,
) -> None:
    available = normalize_aware_utc(available_at)
    if len(events) > MAX_AUDIT_REPORT_PROGRESS_EVENTS:
        raise ValueError("audit task events exceed projection bound")
    if progress.availability != "ready":
        if events or report_hash is not None:
            raise ValueError("empty audit task state has events or report")
        return
    assert progress.latest_created_at is not None and progress.latest_updated_at is not None
    if progress.latest_updated_at > available:
        raise ValueError("audit task is newer than projection")
    if progress.successful_updated_at is not None and progress.successful_updated_at > available:
        raise ValueError("audit success is newer than projection")
    if (report_hash is None) != (progress.successful_report_hash is None):
        raise ValueError("audit report and success pointer disagree")
    if report_hash is not None and report_hash != progress.successful_report_hash:
        raise ValueError("audit report differs from successful task")
    if not events:
        raise ValueError("audit task has no latest event")
    previous_id = 0
    previous_time = progress.latest_created_at
    previous_attempts = 0
    for event in events:
        if (
            event.task_id != progress.latest_task_id
            or event.event_id <= previous_id
            or event.occurred_at < previous_time
            or event.occurred_at > progress.latest_updated_at
            or event.attempts < previous_attempts
            or event.attempts > progress.latest_attempts
        ):
            raise ValueError("audit task events disagree with latest attempt")
        previous_id = event.event_id
        previous_time = event.occurred_at
        previous_attempts = event.attempts
    last = events[-1]
    if (
        progress.latest_status is None
        or last.event_type not in _EXPECTED_LAST_EVENTS[progress.latest_status]
        or last.attempts != progress.latest_attempts
        or last.error_code != progress.latest_error_code
    ):
        raise ValueError("audit task terminal event disagrees with status")


def project_data_audit_report_job(
    snapshot: DataAuditReportJobSnapshot, *, available_at: datetime
) -> tuple[ServingProjectionPayload, ...]:
    progress = snapshot.progress()
    validate_data_audit_report_job_progress(
        progress,
        snapshot.events,
        available_at=available_at,
        report_hash=None
        if snapshot.successful is None
        else snapshot.successful.receipt.report_hash,
    )
    return (
        ServingProjectionPayload(
            table_name="audit_report_job",
            available_at=available_at,
            rows=(progress.model_dump(mode="json"),),
        ),
        ServingProjectionPayload(
            table_name="audit_report_job_event",
            available_at=available_at,
            rows=tuple(event.model_dump(mode="json") for event in snapshot.events),
        ),
    )


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("audit task timestamp is invalid")
    return normalize_aware_utc(datetime.fromisoformat(value))


def _file_identity(found: os.stat_result) -> tuple[int, int]:
    return found.st_dev, found.st_ino


def _sidecar_state(
    state_path: Path,
) -> tuple[tuple[tuple[int, int] | None, tuple[int, int] | None], int]:
    identities = []
    wal_ctime_ns = 0
    for suffix in ("-wal", "-shm"):
        try:
            found = os.lstat(Path(f"{state_path}{suffix}"))
        except FileNotFoundError:
            identities.append(None)
            continue
        if not stat.S_ISREG(found.st_mode) or stat.S_ISLNK(found.st_mode):
            raise ValueError("audit task SQLite sidecar is not a regular file")
        identities.append(_file_identity(found))
        if suffix == "-wal":
            wal_ctime_ns = found.st_ctime_ns
    if (identities[0] is None) != (identities[1] is None):
        raise ValueError("audit task SQLite sidecars must be paired")
    return (identities[0], identities[1]), wal_ctime_ns


def _request(row: sqlite3.Row) -> DataAuditReportJobRequest:
    raw = row["request_json"]
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > _MAX_REQUEST_BYTES:
        raise ValueError("audit task request exceeds bound")
    request = DataAuditReportJobRequest.model_validate_json(raw)
    canonical = json.dumps(
        request.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    if canonical != raw or row["request_sha256"] != hashlib.sha256(raw.encode("utf-8")).hexdigest():
        raise ValueError("audit task request hash disagrees")
    return request


def _receipt(row: sqlite3.Row) -> DataAuditReportJobReceipt:
    return DataAuditReportJobReceipt(
        task_id=row["task_id"],
        status=row["status"],
        attempts=row["attempts"],
        created_at=_timestamp(row["created_at"]),
        updated_at=_timestamp(row["updated_at"]),
        report_hash=row["report_hash"],
        error_code=row["error_code"],
    )


def _read_latest_success_row(connection: sqlite3.Connection) -> sqlite3.Row | None:
    return connection.execute(
        """SELECT * FROM data_audit_report_job WHERE status = 'succeeded'
        ORDER BY created_at DESC, task_id DESC LIMIT 1"""
    ).fetchone()


def _read_snapshot(
    connection: sqlite3.Connection, *, observed: datetime, state: os.stat_result
) -> DataAuditReportJobSnapshot:
    if connection.execute(
        "SELECT 1 FROM data_audit_report_job LIMIT 1 OFFSET ?", (_MAX_STORED_JOBS,)
    ).fetchone():
        raise ValueError("audit task store exceeds its bound")
    latest_row = connection.execute(
        "SELECT * FROM data_audit_report_job ORDER BY created_at DESC, task_id DESC LIMIT 1"
    ).fetchone()
    successful_row = _read_latest_success_row(connection)
    latest = None if latest_row is None else _receipt(latest_row)
    successful = None
    if successful_row is not None:
        receipt = _receipt(successful_row)
        digest = successful_row["replica_sha256"]
        if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
            raise ValueError("successful audit task lacks replica content identity")
        successful = DataAuditReportSuccessfulTask(
            receipt=receipt, request=_request(successful_row), replica_sha256=digest
        )
    if latest is None and successful is not None:
        raise ValueError("audit task success lacks latest attempt")
    events: tuple[DataAuditReportJobEvent, ...] = ()
    if latest is not None:
        rows = connection.execute(
            """SELECT event_id, task_id, event_type, attempts, occurred_at, error_code
            FROM data_audit_report_job_event WHERE task_id = ?
            ORDER BY event_id DESC LIMIT ?""",
            (latest.task_id, _MAX_STORED_EVENTS + 1),
        ).fetchall()
        if len(rows) > _MAX_STORED_EVENTS:
            raise ValueError("audit task event store exceeds its bound")
        events = tuple(
            DataAuditReportJobEvent(
                event_id=row["event_id"],
                task_id=row["task_id"],
                event_type=row["event_type"],
                attempts=row["attempts"],
                occurred_at=_timestamp(row["occurred_at"]),
                error_code=row["error_code"],
            )
            for row in reversed(rows[:MAX_AUDIT_REPORT_PROGRESS_EVENTS])
        )
    available = max(
        datetime.fromtimestamp(state.st_ctime_ns / 1_000_000_000, tz=UTC),
        _EPOCH if latest is None else normalize_aware_utc(latest.updated_at),
        _EPOCH if successful is None else normalize_aware_utc(successful.receipt.updated_at),
    )
    snapshot = DataAuditReportJobSnapshot(
        availability="empty" if latest is None else "ready",
        latest=latest,
        successful=successful,
        events=events,
        available_at=available,
    )
    validate_data_audit_report_job_progress(
        snapshot.progress(),
        events,
        available_at=available,
        report_hash=None if successful is None else successful.receipt.report_hash,
    )
    if available > observed:
        raise ValueError("audit task state is newer than observation")
    return snapshot


def read_data_audit_report_job_snapshot(
    state_path: Path | None, *, observed_at: datetime
) -> DataAuditReportJobSnapshot:
    """Read a live WAL task state and its safe events from one read-only transaction."""
    observed = normalize_aware_utc(observed_at)
    unavailable = DataAuditReportJobSnapshot(
        availability="unavailable", latest=None, successful=None, events=(), available_at=_EPOCH
    )
    if state_path is None:
        return unavailable
    if not state_path.is_absolute() or state_path != state_path.resolve(strict=False):
        raise ValueError("audit task state path must be absolute and canonical")
    try:
        before = os.lstat(state_path)
    except FileNotFoundError:
        if _sidecar_state(state_path)[0] != (None, None):
            raise ValueError("audit task state is missing while SQLite sidecars exist") from None
        return unavailable
    if not stat.S_ISREG(before.st_mode) or stat.S_ISLNK(before.st_mode):
        raise ValueError("audit task state must be a regular non-symlink file")
    if before.st_size > _MAX_STATE_BYTES:
        raise ValueError("audit task state exceeds size bound")
    sidecars, wal_ctime_before = _sidecar_state(state_path)
    connection: sqlite3.Connection | None = None
    try:
        # A plain read-only open creates WAL sidecars when none exist. In that
        # checkpointed case immutable mode leaves the task directory untouched;
        # live WAL mode is used only with already present sidecars.
        uri = f"{state_path.as_uri()}?mode=ro"
        if sidecars == (None, None):
            uri += "&immutable=1"
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        snapshot = _read_snapshot(connection, observed=observed, state=before)
        connection.rollback()
    finally:
        if connection is not None:
            connection.close()
    try:
        after = os.lstat(state_path)
    except FileNotFoundError as exc:
        raise ValueError("audit task state rotated while read") from exc
    sidecars_after, wal_ctime_after = _sidecar_state(state_path)
    if (
        not stat.S_ISREG(after.st_mode)
        or stat.S_ISLNK(after.st_mode)
        or _file_identity(before) != _file_identity(after)
        or (sidecars == (None, None) and before.st_ctime_ns != after.st_ctime_ns)
        or sidecars_after != sidecars
    ):
        raise ValueError("audit task state rotated while read")
    available_ns = max(before.st_ctime_ns, after.st_ctime_ns, wal_ctime_before, wal_ctime_after)
    available_at = max(
        snapshot.available_at, datetime.fromtimestamp(available_ns / 1_000_000_000, tz=UTC)
    )
    if available_at > observed:
        raise ValueError("audit task state is newer than observation")
    return replace(snapshot, available_at=available_at)
