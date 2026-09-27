"""Read one bounded maintenance-job snapshot without opening its writer store."""

from __future__ import annotations

import os
import sqlite3
import stat
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc

MAX_BACKFILL_PROGRESS_EVENTS = 20
_MAX_STORED_EVENTS = 64
_MAX_STORED_JOBS = 4096
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_Status = Literal["queued", "running", "succeeded", "failed"]
_EventType = Literal[
    "queued", "started", "resumed", "source_check", "succeeded", "failed", "retried"
]
_ErrorCode = Literal["snapshot_changed", "invalid_evidence", "artifact_invalid", "internal_error"]


class BackfillPlanProgressState(RuntimeContractModel):
    status_key: Literal["current"] = "current"
    availability: Literal["unavailable", "empty", "ready"]
    event_history: Literal["available", "unavailable"] = "unavailable"
    task_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    status: _Status | None = None
    attempts: int | None = Field(default=None, ge=0, strict=True)
    created_at: AwareUtcDatetime | None = None
    updated_at: AwareUtcDatetime | None = None
    plan_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    error_code: _ErrorCode | None = None

    @model_validator(mode="after")
    def validate_state(self) -> BackfillPlanProgressState:
        if self.availability == "unavailable" and self.event_history != "unavailable":
            raise ValueError("unavailable task cannot claim event history")
        if self.availability != "ready":
            if any(
                value is not None
                for value in (
                    self.task_id,
                    self.status,
                    self.attempts,
                    self.created_at,
                    self.updated_at,
                    self.plan_hash,
                    self.error_code,
                )
            ):
                raise ValueError("unavailable or empty progress cannot carry a task")
            return self
        if None in (
            self.task_id,
            self.status,
            self.attempts,
            self.created_at,
            self.updated_at,
        ):
            raise ValueError("ready progress needs a complete task")
        assert self.created_at is not None and self.updated_at is not None
        if self.created_at > self.updated_at:
            raise ValueError("backfill plan task timestamps disagree")
        if (self.status == "succeeded") != (self.plan_hash is not None):
            raise ValueError("only succeeded backfill plan tasks may carry a plan hash")
        if (self.status == "failed") != (self.error_code is not None):
            raise ValueError("only failed backfill plan tasks may carry an error code")
        return self


class BackfillPlanProgressEvent(RuntimeContractModel):
    event_id: int = Field(gt=0, strict=True)
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    event_type: _EventType
    attempts: int = Field(ge=0, strict=True)
    occurred_at: AwareUtcDatetime
    error_code: _ErrorCode | None = None

    @model_validator(mode="after")
    def validate_error(self) -> BackfillPlanProgressEvent:
        if (self.event_type == "failed") != (self.error_code is not None):
            raise ValueError("only failed backfill plan events may carry an error code")
        return self


@dataclass(frozen=True)
class BackfillPlanJobSnapshot:
    progress: BackfillPlanProgressState
    events: tuple[BackfillPlanProgressEvent, ...]
    available_at: datetime

    def validate(self, *, plan_hashes: frozenset[str] = frozenset()) -> None:
        validate_backfill_plan_progress(
            self.progress, self.events, available_at=self.available_at, plan_hashes=plan_hashes
        )


def validate_backfill_plan_progress(
    progress: BackfillPlanProgressState,
    events: tuple[BackfillPlanProgressEvent, ...],
    *,
    available_at: datetime,
    plan_hashes: frozenset[str],
) -> None:
    """Keep status, safe events and the complete same-generation plan index coherent."""
    available = normalize_aware_utc(available_at)
    if len(events) > MAX_BACKFILL_PROGRESS_EVENTS:
        raise ValueError("backfill plan progress events exceed their bound")
    if progress.availability != "ready":
        if events:
            raise ValueError("backfill plan progress has events without a task")
        return
    if progress.created_at is None or progress.updated_at is None:
        raise ValueError("backfill plan progress lacks task timestamps")
    if progress.updated_at > available:
        raise ValueError("backfill plan task is newer than its projection")
    if progress.event_history == "unavailable":
        if events:
            raise ValueError("backfill plan job claims unavailable event history")
        if progress.status == "succeeded" and progress.plan_hash not in plan_hashes:
            raise ValueError("succeeded backfill plan is missing from the same-generation catalog")
        return
    if not events:
        raise ValueError("backfill plan progress lacks its latest event")
    previous_id = 0
    previous_time = progress.created_at
    previous_attempts = 0
    for event in events:
        if (
            event.task_id != progress.task_id
            or event.event_id <= previous_id
            or event.occurred_at < previous_time
            or event.occurred_at > progress.updated_at
            or event.attempts < previous_attempts
            or event.attempts > progress.attempts
        ):
            raise ValueError("backfill plan progress events disagree with the task")
        previous_id = event.event_id
        previous_time = event.occurred_at
        previous_attempts = event.attempts
    latest = events[-1]
    expected_types: dict[str, frozenset[str]] = {
        "queued": frozenset({"queued", "retried"}),
        "running": frozenset({"started", "resumed", "source_check"}),
        "succeeded": frozenset({"succeeded"}),
        "failed": frozenset({"failed"}),
    }
    if (
        progress.status is None
        or latest.event_type not in expected_types[progress.status]
        or latest.attempts != progress.attempts
        or latest.error_code != progress.error_code
    ):
        raise ValueError("backfill plan progress terminal event disagrees")
    if progress.status == "succeeded" and progress.plan_hash not in plan_hashes:
        raise ValueError("succeeded backfill plan is missing from the same-generation catalog")


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("backfill plan job timestamp is invalid")
    return normalize_aware_utc(datetime.fromisoformat(value))


def _file_identity(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _require_existing_wal_sidecars(state_path: Path) -> None:
    wal_path = Path(f"{state_path}-wal")
    shm_path = Path(f"{state_path}-shm")

    def regular_or_missing(path: Path) -> bool:
        try:
            found = os.lstat(path)
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(found.st_mode):
            raise ValueError("backfill plan job SQLite sidecar is not a regular file")
        return True

    wal_exists = regular_or_missing(wal_path)
    shm_exists = regular_or_missing(shm_path)
    if wal_exists and not shm_exists:
        raise ValueError("backfill plan job WAL shared memory sidecar is missing")


def read_backfill_plan_job_snapshot(
    state_path: Path | None, *, observed_at: datetime
) -> BackfillPlanJobSnapshot:
    """Read live WAL state and events inside one SQLite transaction, without writes."""
    observed = normalize_aware_utc(observed_at)
    unavailable = BackfillPlanJobSnapshot(
        progress=BackfillPlanProgressState(availability="unavailable"),
        events=(),
        available_at=_EPOCH,
    )
    if state_path is None:
        return unavailable
    if not state_path.is_absolute():
        raise ValueError("backfill plan job state path must be absolute")
    try:
        before = os.lstat(state_path)
    except FileNotFoundError:
        return unavailable
    if not stat.S_ISREG(before.st_mode) or stat.S_ISLNK(before.st_mode):
        raise ValueError("backfill plan job state must be a regular non-symlink file")
    _require_existing_wal_sidecars(state_path)
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"{state_path.as_uri()}?mode=ro", uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        event_table_available = (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='backfill_plan_job_event'"
            ).fetchone()
            is not None
        )
        job_columns = {row[1] for row in connection.execute("PRAGMA table_info(backfill_plan_job)")}
        has_history_marker = "event_history_complete" in job_columns
        if has_history_marker and not event_table_available:
            raise ValueError("backfill plan event history table vanished")
        if connection.execute(
            "SELECT 1 FROM backfill_plan_job LIMIT 1 OFFSET ?", (_MAX_STORED_JOBS,)
        ).fetchone():
            raise ValueError("backfill plan job store exceeds its bound")
        row = connection.execute(
            f"""
            SELECT task_id, status, attempts, created_at, updated_at, plan_hash, error_code
                   {", event_history_complete" if has_history_marker else ""}
            FROM backfill_plan_job ORDER BY created_at DESC, rowid DESC LIMIT 1
            """
        ).fetchone()
        if row is None:
            available_at = datetime.fromtimestamp(before.st_ctime_ns / 1_000_000_000, tz=UTC)
            snapshot = BackfillPlanJobSnapshot(
                progress=BackfillPlanProgressState(
                    availability="empty",
                    event_history="available" if event_table_available else "unavailable",
                ),
                events=(),
                available_at=available_at,
            )
        else:
            if has_history_marker:
                complete = row["event_history_complete"]
                if type(complete) is not int or complete not in (0, 1):
                    raise ValueError("backfill plan event history marker is invalid")
                event_history = "available" if complete == 1 else "unavailable"
            else:
                event_history = "available" if event_table_available else "unavailable"
            progress = BackfillPlanProgressState(
                availability="ready",
                event_history=event_history,
                task_id=row["task_id"],
                status=row["status"],
                attempts=row["attempts"],
                created_at=_timestamp(row["created_at"]),
                updated_at=_timestamp(row["updated_at"]),
                plan_hash=row["plan_hash"],
                error_code=row["error_code"],
            )
            rows = (
                connection.execute(
                    """
                    SELECT event_id, task_id, event_type, attempts, occurred_at, error_code
                    FROM backfill_plan_job_event WHERE task_id = ?
                    ORDER BY event_id DESC LIMIT ?
                    """,
                    (progress.task_id, _MAX_STORED_EVENTS + 1),
                ).fetchall()
                if event_history == "available"
                else []
            )
            if len(rows) > _MAX_STORED_EVENTS:
                raise ValueError("backfill plan job events exceed their stored bound")
            events = tuple(
                BackfillPlanProgressEvent(
                    event_id=item["event_id"],
                    task_id=item["task_id"],
                    event_type=item["event_type"],
                    attempts=item["attempts"],
                    occurred_at=_timestamp(item["occurred_at"]),
                    error_code=item["error_code"],
                )
                for item in reversed(rows[:MAX_BACKFILL_PROGRESS_EVENTS])
            )
            snapshot = BackfillPlanJobSnapshot(
                progress=progress,
                events=events,
                available_at=progress.updated_at or _EPOCH,
            )
        connection.rollback()
    finally:
        if connection is not None:
            connection.close()
    try:
        after = os.lstat(state_path)
    except FileNotFoundError as exc:
        raise ValueError("backfill plan job state rotated while read") from exc
    if (
        not stat.S_ISREG(after.st_mode)
        or stat.S_ISLNK(after.st_mode)
        or _file_identity(before) != _file_identity(after)
    ):
        raise ValueError("backfill plan job state rotated while read")
    if snapshot.available_at > observed:
        raise ValueError("backfill plan job state is newer than observation")
    return snapshot
