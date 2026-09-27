"""Seal one read-only daily-bar backfill proposal as an immutable Lab artifact."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import BinaryIO

import duckdb

from rquant.backfill_plan_core import (
    BackfillEstimateAssumptions,
    DailyBarBackfillPlan,
    build_daily_bar_backfill_plan,
)

MAX_BACKFILL_PLAN_BYTES = 8_000_000
_PLAN_PREFIX = "daily-bar-backfill-plan-v1-"


def _canonical_bytes(plan: DailyBarBackfillPlan) -> bytes:
    return json.dumps(
        plan.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _file_sha256(handle: BinaryIO) -> str:
    handle.seek(0)
    digest = hashlib.sha256()
    while chunk := handle.read(1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def _file_identity(observed: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _require_same_snapshot(path: Path, opened: os.stat_result) -> None:
    try:
        current = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise ValueError("snapshot path changed during read") from exc
    if not stat.S_ISREG(current.st_mode) or _file_identity(current) != _file_identity(opened):
        raise ValueError("snapshot file identity changed during read")
    if Path(f"{path}.wal").exists():
        raise ValueError("snapshot has an unsealed DuckDB WAL")


def parse_daily_bar_backfill_plan_bytes(data: bytes, *, filename: str) -> DailyBarBackfillPlan:
    """Validate bounded canonical content and its content-addressed filename."""
    if not data or len(data) > MAX_BACKFILL_PLAN_BYTES:
        raise ValueError("backfill plan exceeds byte limit or is empty")
    try:
        plan = DailyBarBackfillPlan.model_validate_json(data)
    except ValueError as exc:
        raise ValueError("backfill plan is invalid or its digest mismatches") from exc
    if data != _canonical_bytes(plan):
        raise ValueError("backfill plan is not canonical")
    if filename != f"{_PLAN_PREFIX}{plan.content_sha256}.json":
        raise ValueError("backfill plan filename disagrees with content hash")
    return plan


def load_daily_bar_backfill_plan(path: Path) -> DailyBarBackfillPlan:
    """Read one immutable plan; reject aliases and damage before exposing it."""
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_BACKFILL_PLAN_BYTES:
            raise ValueError("backfill plan must be a bounded regular file")
        data = handle.read(MAX_BACKFILL_PLAN_BYTES + 1)
        if _file_identity(os.fstat(handle.fileno())) != _file_identity(before) or len(data) != (
            before.st_size
        ):
            raise ValueError("backfill plan changed during read")
    return parse_daily_bar_backfill_plan_bytes(data, filename=path.name)


def _publish_plan(plan: DailyBarBackfillPlan, directory: Path) -> Path:
    plan = DailyBarBackfillPlan.model_validate(plan)
    data = _canonical_bytes(plan)
    destination_name = f"{_PLAN_PREFIX}{plan.content_sha256}.json"
    parse_daily_bar_backfill_plan_bytes(data, filename=destination_name)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / destination_name
    descriptor, temporary_name = tempfile.mkstemp(prefix=".backfill-plan-", dir=directory)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fchmod(handle.fileno(), 0o444)
            os.fsync(handle.fileno())
        parse_daily_bar_backfill_plan_bytes(temporary.read_bytes(), filename=destination_name)
        try:
            os.link(str(temporary), str(destination))
        except FileExistsError:
            if load_daily_bar_backfill_plan(destination) != plan:
                raise ValueError("existing backfill plan differs from content identity") from None
        else:
            directory_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        return destination
    finally:
        temporary.unlink(missing_ok=True)


def create_and_publish_daily_bar_backfill_plan(
    *,
    snapshot_path: Path,
    expected_file_sha256: str,
    snapshot_label: str,
    evidence_code_revision: str,
    audit_start: date,
    completed_through: date,
    observed_at: datetime,
    assumptions: BackfillEstimateAssumptions,
    directory: Path,
) -> Path:
    """Measure a fixed file, take one read-only view, then atomically publish.

    The measured file SHA256 binds the proposal to bytes seen by this Lab run. It
    does not establish a production ingestion or completion authority chain.
    """
    if not snapshot_path.is_absolute() or not directory.is_absolute():
        raise ValueError("snapshot and plan directory must be explicit absolute paths")
    if re.fullmatch(r"[0-9a-f]{64}", expected_file_sha256) is None:
        raise ValueError("expected snapshot SHA256 must be lowercase hexadecimal")

    descriptor = os.open(snapshot_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as handle:
        opened = os.fstat(handle.fileno())
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError("snapshot must be a regular file")
        _require_same_snapshot(snapshot_path, opened)
        observed_sha256 = _file_sha256(handle)
        if observed_sha256 != expected_file_sha256:
            raise ValueError("snapshot SHA256 digest disagrees with supplied identity")
        _require_same_snapshot(snapshot_path, opened)
        with duckdb.connect(str(snapshot_path), read_only=True) as connection:
            _require_same_snapshot(snapshot_path, opened)
            plan = build_daily_bar_backfill_plan(
                connection,
                snapshot_label=snapshot_label,
                snapshot_file_sha256=observed_sha256,
                evidence_code_revision=evidence_code_revision,
                audit_start=audit_start,
                completed_through=completed_through,
                observed_at=observed_at,
                assumptions=assumptions,
            )
            _require_same_snapshot(snapshot_path, opened)
        if _file_sha256(handle) != observed_sha256:
            raise ValueError("snapshot contents changed during evidence read")
        _require_same_snapshot(snapshot_path, opened)
    return _publish_plan(plan, directory)
