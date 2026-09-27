"""Seal one read-only daily-bar backfill proposal as an immutable Lab artifact."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
import tempfile
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import BinaryIO

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from rquant.backfill_plan_core import (
    BackfillEstimateAssumptions,
    DailyBarBackfillPlan,
    build_daily_bar_backfill_plan,
)

MAX_BACKFILL_PLAN_BYTES = 8_000_000
_PLAN_PREFIX = "daily-bar-backfill-plan-v1-"


class BackfillSnapshotFileIdentity(BaseModel):
    """Fast file identity, not proof of production generation or complete collection."""

    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")

    device: int = Field(ge=0, strict=True)
    inode: int = Field(ge=0, strict=True)
    size: int = Field(gt=0, strict=True)
    mtime_ns: int = Field(gt=0, strict=True)
    ctime_ns: int = Field(gt=0, strict=True)


def _snapshot_identity(observed: os.stat_result) -> BackfillSnapshotFileIdentity:
    return BackfillSnapshotFileIdentity(
        device=observed.st_dev,
        inode=observed.st_ino,
        size=observed.st_size,
        mtime_ns=observed.st_mtime_ns,
        ctime_ns=observed.st_ctime_ns,
    )


def capture_backfill_snapshot_identity(path: Path) -> BackfillSnapshotFileIdentity:
    """Bind an explicit fixed file in O(1), without opening DuckDB or reading bytes."""
    if not path.is_absolute():
        raise ValueError("snapshot path must be absolute")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as handle:
        opened = os.fstat(handle.fileno())
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError("snapshot must be a regular file")
        _require_same_snapshot(path, opened)
        return _snapshot_identity(opened)


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


def _inode_identity(observed: os.stat_result) -> tuple[int, int]:
    return observed.st_dev, observed.st_ino


def _require_same_snapshot(path: Path, opened: os.stat_result) -> None:
    try:
        current = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise ValueError("snapshot path changed during read") from exc
    if not stat.S_ISREG(current.st_mode) or _file_identity(current) != _file_identity(opened):
        raise ValueError("snapshot file identity changed during read")
    wal_path = Path(f"{path}.wal")
    if wal_path.exists() or wal_path.is_symlink():
        raise ValueError("snapshot has an unsealed DuckDB WAL")


def _require_same_directory(path: Path, descriptor: int) -> None:
    try:
        current = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise ValueError("plan output directory changed during publication") from exc
    if not stat.S_ISDIR(current.st_mode) or _inode_identity(current) != _inode_identity(
        os.fstat(descriptor)
    ):
        raise ValueError("plan output directory identity changed during publication")


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


def _load_plan_descriptor(
    descriptor: int, *, filename: str
) -> tuple[DailyBarBackfillPlan, os.stat_result]:
    with os.fdopen(descriptor, "rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_BACKFILL_PLAN_BYTES:
            raise ValueError("backfill plan must be a bounded regular file")
        data = handle.read(MAX_BACKFILL_PLAN_BYTES + 1)
        if _file_identity(os.fstat(handle.fileno())) != _file_identity(before) or len(data) != (
            before.st_size
        ):
            raise ValueError("backfill plan changed during read")
    return parse_daily_bar_backfill_plan_bytes(data, filename=filename), before


def load_daily_bar_backfill_plan(path: Path) -> DailyBarBackfillPlan:
    """Read one immutable plan; reject aliases and damage before exposing it."""
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    return _load_plan_descriptor(descriptor, filename=path.name)[0]


def _publish_plan(plan: DailyBarBackfillPlan, directory: Path) -> Path:
    plan = DailyBarBackfillPlan.model_validate(plan)
    data = _canonical_bytes(plan)
    destination_name = f"{_PLAN_PREFIX}{plan.content_sha256}.json"
    parse_daily_bar_backfill_plan_bytes(data, filename=destination_name)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / destination_name
    directory_fd = os.open(
        directory,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        _require_same_directory(directory, directory_fd)
        temporary_name = f".backfill-plan-{secrets.token_hex(16)}"
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_fd,
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fchmod(handle.fileno(), 0o444)
                os.fsync(handle.fileno())
            temporary_descriptor = os.open(
                temporary_name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_fd,
            )
            _, temporary_stat = _load_plan_descriptor(
                temporary_descriptor, filename=destination_name
            )
            _require_same_directory(directory, directory_fd)
            try:
                os.link(
                    temporary_name,
                    destination_name,
                    src_dir_fd=directory_fd,
                    dst_dir_fd=directory_fd,
                )
            except FileExistsError:
                existing_descriptor = os.open(
                    destination_name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=directory_fd,
                )
                existing, destination_stat = _load_plan_descriptor(
                    existing_descriptor, filename=destination_name
                )
                if existing != plan:
                    raise ValueError(
                        "existing backfill plan differs from content identity"
                    ) from None
            else:
                destination_stat = temporary_stat
            _require_same_directory(directory, directory_fd)
            if _inode_identity(
                os.stat(destination_name, dir_fd=directory_fd, follow_symlinks=False)
            ) != _inode_identity(destination_stat):
                raise ValueError("published backfill plan identity changed")
        finally:
            os.unlink(temporary_name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        _require_same_directory(directory, directory_fd)
        if _inode_identity(destination.stat(follow_symlinks=False)) != _inode_identity(
            destination_stat
        ):
            raise ValueError("published backfill plan path changed")
        return destination
    finally:
        os.close(directory_fd)


def create_and_publish_daily_bar_backfill_plan(
    *,
    snapshot_path: Path,
    expected_file_sha256: str | None = None,
    snapshot_label: str,
    evidence_code_revision: str,
    audit_start: date,
    completed_through: date,
    observed_at: datetime,
    assumptions: BackfillEstimateAssumptions,
    directory: Path,
    expected_file_identity: BackfillSnapshotFileIdentity | None = None,
    on_source_sha256: Callable[[str], None] | None = None,
) -> Path:
    """Measure a fixed file, take one read-only view, then atomically publish.

    The measured file SHA256 binds the proposal to bytes seen by this Lab run. It
    does not establish a production ingestion or completion authority chain.
    """
    if not snapshot_path.is_absolute() or not directory.is_absolute():
        raise ValueError("snapshot and plan directory must be explicit absolute paths")
    if expected_file_sha256 is not None and re.fullmatch(
        r"[0-9a-f]{64}", expected_file_sha256
    ) is None:
        raise ValueError("expected snapshot SHA256 must be lowercase hexadecimal")

    descriptor = os.open(snapshot_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as handle:
        opened = os.fstat(handle.fileno())
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError("snapshot must be a regular file")
        if expected_file_identity is not None and _snapshot_identity(
            opened
        ) != BackfillSnapshotFileIdentity.model_validate(expected_file_identity):
            raise ValueError("snapshot file identity changed before evidence read")
        _require_same_snapshot(snapshot_path, opened)
        observed_sha256 = _file_sha256(handle)
        if expected_file_sha256 is not None and observed_sha256 != expected_file_sha256:
            raise ValueError("snapshot SHA256 digest disagrees with supplied identity")
        _require_same_snapshot(snapshot_path, opened)
        if on_source_sha256 is not None:
            on_source_sha256(observed_sha256)
        directory.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=".backfill-source-", dir=directory.parent
        ) as private_directory:
            private = Path(private_directory)
            private_stat = private.stat(follow_symlinks=False)
            if private_stat.st_uid != os.geteuid() or stat.S_IMODE(private_stat.st_mode) != 0o700:
                raise ValueError("snapshot pin directory is not private")
            pinned_snapshot = private / "snapshot.duckdb"
            try:
                os.link(str(snapshot_path), str(pinned_snapshot), follow_symlinks=False)
            except OSError as exc:
                raise ValueError("cannot pin snapshot inode without copying") from exc
            pinned_stat = pinned_snapshot.stat(follow_symlinks=False)
            pinned_source = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(pinned_stat.st_mode)
                or _inode_identity(pinned_stat) != _inode_identity(opened)
                or _inode_identity(pinned_source) != _inode_identity(opened)
                or pinned_source.st_size != opened.st_size
                or pinned_source.st_mtime_ns != opened.st_mtime_ns
            ):
                raise ValueError("snapshot changed before pinning")
            _require_same_snapshot(snapshot_path, pinned_source)
            with duckdb.connect(str(pinned_snapshot), read_only=True) as connection:
                _require_same_snapshot(snapshot_path, pinned_source)
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
                _require_same_snapshot(snapshot_path, pinned_source)
            if _file_sha256(handle) != observed_sha256:
                raise ValueError("snapshot contents changed during evidence read")
            _require_same_snapshot(snapshot_path, pinned_source)
    return _publish_plan(plan, directory)
