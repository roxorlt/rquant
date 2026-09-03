"""Persistent fenced runtime for recovery jobs and periodic rehearsals."""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import sqlite3
import stat
import sys
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal, Self, TypeVar
from urllib.parse import quote

from pydantic import (
    Field,
    JsonValue,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.runtime_recovery_artifacts import (
    FixedReplayVerifier,
    RealRecoveryIntegrityError,
    RealRecoveryReceipt,
    RealRecoveryRestorer,
    RealRecoveryTargetManifest,
    RecoveryPayloadVerifier,
    RecoveryToolVerifierBundle,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_APPLICATION_ID = 0x52515243
_SCHEMA_VERSION = 4
_MAX_CHECKPOINT_BYTES = 1024 * 1024
_MAX_CONTRACT_BYTES = 16 * 1024 * 1024
_MAX_AUDIT_EVENTS = 1_000_000
_MAX_AUDIT_EVENT_BYTES = 1024 * 1024

ContractT = TypeVar("ContractT", bound=RuntimeContractModel)
_MigrationArchiveProof = tuple[tuple[int, ...], tuple[int, ...]]


class RecoveryServiceIntegrityError(RuntimeError):
    """Recovery service state, lease, or immutable receipt is untrustworthy."""


class RecoveryServiceLeaseLostError(RecoveryServiceIntegrityError):
    """A transient fenced capability expired or was superseded."""


def _recovery_error_class(error: Exception) -> str:
    if isinstance(error, RecoveryServiceLeaseLostError):
        return "transient_lease"
    if isinstance(error, OSError):
        return "transient_io"
    if isinstance(error, (RealRecoveryIntegrityError, RecoveryServiceIntegrityError, ValueError)):
        return "permanent_integrity"
    return "transient_unknown"


def _encode_time(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decode_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RecoveryServiceIntegrityError("stored timestamp is naive")
    return parsed.astimezone(UTC)


def _canonical_absolute(value: Path | str, *, label: str) -> str:
    path = Path(value)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise ValueError(f"{label} must be an absolute canonical path")
    return str(path)


def _regular_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _directory_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_canonical_contract(path: Path, model: type[ContractT]) -> ContractT:
    parent = path.parent
    parent_before = os.lstat(parent)
    if stat.S_ISLNK(parent_before.st_mode) or not stat.S_ISDIR(parent_before.st_mode):
        raise RecoveryServiceIntegrityError("recovery contract parent is unsafe")
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        named = os.lstat(path)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or opened.st_size > _MAX_CONTRACT_BYTES
        ):
            raise RecoveryServiceIntegrityError("recovery contract file is unsafe")
        chunks: list[bytes] = []
        size = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            size += len(chunk)
            if size > _MAX_CONTRACT_BYTES:
                raise RecoveryServiceIntegrityError("recovery contract exceeds byte budget")
            chunks.append(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        named_after = os.lstat(path)
        parent_after = os.lstat(parent)
        if (
            len(raw) != opened.st_size
            or _regular_stat_identity(opened) != _regular_stat_identity(after)
            or (after.st_dev, after.st_ino) != (named_after.st_dev, named_after.st_ino)
            or _directory_stat_identity(parent_before) != _directory_stat_identity(parent_after)
        ):
            raise RecoveryServiceIntegrityError("recovery contract changed while reading")
    except OSError as exc:
        raise RecoveryServiceIntegrityError(
            "recovery contract path is unavailable or unsafe"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        decoded = strict_canonical_json_loads(raw)
        contract = model.model_validate(decoded)
    except Exception as exc:
        raise RecoveryServiceIntegrityError("recovery contract is invalid") from exc
    if canonical_json_bytes(contract.model_dump(mode="json")) != raw:
        raise RecoveryServiceIntegrityError("recovery contract is not canonical")
    return contract


def _validate_directory_fd(
    descriptor: int,
    path: Path,
    *,
    label: str,
    required_mode: int | None = None,
) -> os.stat_result:
    try:
        opened = os.fstat(descriptor)
        named = os.lstat(path)
    except OSError as exc:
        raise RecoveryServiceIntegrityError(f"{label} directory is unavailable") from exc
    mode = stat.S_IMODE(opened.st_mode)
    if (
        stat.S_ISLNK(named.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or not stat.S_ISDIR(named.st_mode)
        or opened.st_uid != os.geteuid()
        or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
        or (required_mode is not None and mode != required_mode)
    ):
        raise RecoveryServiceIntegrityError(f"{label} directory is unsafe")
    return opened


def _directory_fd_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
    )


def _path_exists_at(directory_fd: int, filename: str) -> bool:
    try:
        os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def _archive_proof_at(archive_fd: int, filename: str) -> _MigrationArchiveProof:
    try:
        archive = os.fstat(archive_fd)
        archived = os.stat(filename, dir_fd=archive_fd, follow_symlinks=False)
    except OSError as exc:
        raise RecoveryServiceIntegrityError(
            "legacy recovery receipt archive proof is unavailable"
        ) from exc
    if not stat.S_ISDIR(archive.st_mode) or not stat.S_ISREG(archived.st_mode):
        raise RecoveryServiceIntegrityError("legacy recovery receipt archive proof is unsafe")
    return _directory_fd_identity(archive), _regular_stat_identity(archived)


def _read_canonical_contract_at(
    directory_fd: int,
    directory_path: Path,
    filename: str,
    model: type[ContractT],
) -> ContractT:
    if Path(filename).name != filename or not filename.endswith(".json"):
        raise RecoveryServiceIntegrityError("recovery contract relative path is unsafe")
    parent_before = _validate_directory_fd(
        directory_fd,
        directory_path,
        label="recovery contract parent",
    )
    descriptor = -1
    try:
        descriptor = os.open(
            filename,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_fd,
        )
        opened = os.fstat(descriptor)
        named = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or opened.st_size > _MAX_CONTRACT_BYTES
        ):
            raise RecoveryServiceIntegrityError("recovery contract file is unsafe")
        chunks: list[bytes] = []
        size = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            size += len(chunk)
            if size > _MAX_CONTRACT_BYTES:
                raise RecoveryServiceIntegrityError("recovery contract exceeds byte budget")
            chunks.append(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        named_after = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
        parent_after = _validate_directory_fd(
            directory_fd,
            directory_path,
            label="recovery contract parent",
        )
        if (
            len(raw) != opened.st_size
            or _regular_stat_identity(opened) != _regular_stat_identity(after)
            or (after.st_dev, after.st_ino) != (named_after.st_dev, named_after.st_ino)
            or _directory_fd_identity(parent_before) != _directory_fd_identity(parent_after)
        ):
            raise RecoveryServiceIntegrityError("recovery contract changed while reading")
    except OSError as exc:
        raise RecoveryServiceIntegrityError(
            "recovery contract path is unavailable or unsafe"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        decoded = strict_canonical_json_loads(raw)
        contract = model.model_validate(decoded)
    except Exception as exc:
        raise RecoveryServiceIntegrityError("recovery contract is invalid") from exc
    if canonical_json_bytes(contract.model_dump(mode="json")) != raw:
        raise RecoveryServiceIntegrityError("recovery contract is not canonical")
    return contract


class RecoveryServiceJob(RuntimeContractModel):
    job_id: Sha256
    request_id: str = Field(min_length=1, max_length=256)
    backup_root: str
    manifest_path: str
    tool_bundle_path: str
    restore_root: str
    status: Literal["pending", "running", "scheduled", "succeeded", "failed"]
    deadline_at: AwareUtcDatetime
    attempt_timeout_seconds: int = Field(ge=1, le=7 * 24 * 60 * 60)
    next_attempt_at: AwareUtcDatetime
    rehearsal_interval_seconds: int | None = Field(default=None, ge=60, le=365 * 24 * 60 * 60)
    attempt_count: int = Field(ge=0)
    fence: int = Field(ge=0)
    lease_owner: str | None = None
    lease_until: AwareUtcDatetime | None = None
    checkpoint_stage: str | None = None
    checkpoint: Mapping[str, JsonValue] | None = None
    recovery_receipt_id: Sha256 | None = None
    last_error_type: str | None = None
    last_error_message: str | None = None
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime

    @field_validator("backup_root", "manifest_path", "tool_bundle_path", "restore_root")
    @classmethod
    def validate_paths(cls, value: str) -> str:
        return _canonical_absolute(value, label="recovery job path")

    @field_validator("checkpoint")
    @classmethod
    def canonicalize_checkpoint(
        cls,
        value: Mapping[str, JsonValue] | None,
    ) -> Mapping[str, JsonValue] | None:
        if value is None:
            return None
        decoded = strict_canonical_json_loads(canonical_json_bytes(dict(value)))
        if not isinstance(decoded, dict):
            raise ValueError("checkpoint must be an object")
        return MappingProxyType(dict(sorted(decoded.items())))

    @field_serializer("checkpoint")
    def serialize_checkpoint(
        self,
        value: Mapping[str, JsonValue] | None,
    ) -> dict[str, JsonValue] | None:
        return None if value is None else dict(value)

    @model_validator(mode="after")
    def validate_job(self) -> Self:
        lease = (self.lease_owner, self.lease_until)
        if self.status == "running" and (lease[0] is None or lease[1] is None):
            raise ValueError("running recovery job requires a lease")
        if self.status != "running" and lease != (None, None):
            raise ValueError("non-running recovery job cannot retain a lease")
        if (self.checkpoint_stage is None) != (self.checkpoint is None):
            raise ValueError("checkpoint stage and payload must appear together")
        if self.status == "succeeded" and self.recovery_receipt_id is None:
            raise ValueError("successful recovery job requires a receipt")
        if self.status == "scheduled" and self.rehearsal_interval_seconds is None:
            raise ValueError("scheduled recovery job requires a rehearsal interval")
        return self


class RecoveryServiceResult(RuntimeContractModel):
    job_id: Sha256
    fence: int = Field(ge=1)
    status: Literal["succeeded", "failed", "retry_scheduled", "rehearsal_scheduled"]
    recovery_receipt_id: Sha256 | None = None
    service_receipt_id: Sha256 | None = None


class _RecoveryServiceReceiptBase(RuntimeContractModel):
    receipt_id: Sha256 | None = None
    job_id: Sha256
    fence: int = Field(ge=1)
    status: Literal["succeeded", "failed", "retry_scheduled", "rehearsal_scheduled"]
    verification_level: Literal["full"] | None = None
    recovery_receipt_id: Sha256 | None = None
    error_type: str | None = None
    error_message: str | None = None
    completed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_receipt(self) -> Self:
        carries_error = self.status in {"failed", "retry_scheduled"}
        if carries_error != (self.error_type is not None and self.error_message is not None):
            raise ValueError("service failure/retry receipt must carry an exact error")
        if (self.status == "succeeded") != (self.recovery_receipt_id is not None):
            raise ValueError("service success and failure evidence conflict")
        if (self.status == "succeeded") != (self.verification_level == "full"):
            raise ValueError("only successful full rehearsals carry verification evidence")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"receipt_id"}))
        if self.receipt_id is not None and self.receipt_id != expected:
            raise ValueError("service receipt id does not bind content")
        object.__setattr__(self, "receipt_id", expected)
        return self


class LegacyRecoveryServiceReceipt(_RecoveryServiceReceiptBase):
    """The exact immutable receipt contract emitted by recovery state schema v3."""

    schema_version: Literal[1] = 1


class RecoveryServiceReceipt(_RecoveryServiceReceiptBase):
    schema_version: Literal[2] = 2


class RecoveryServiceLease:
    """A fenced attempt capability passed to one recovery executor."""

    def __init__(self, service: RuntimeRecoveryService, job: RecoveryServiceJob) -> None:
        self._service = service
        self.job = job
        self.fence = job.fence
        self._lock = threading.RLock()

    def checkpoint(self, stage: str, payload: Mapping[str, JsonValue]) -> RecoveryServiceJob:
        with self._lock:
            self.job = self._service._checkpoint(
                job_id=self.job.job_id,
                fence=self.fence,
                stage=stage,
                payload=payload,
            )
            return self.job

    def renew(self) -> RecoveryServiceJob:
        with self._lock:
            self.job = self._service._renew(job_id=self.job.job_id, fence=self.fence)
            return self.job

    def assert_active(self, stage: str) -> RecoveryServiceJob:
        with self._lock:
            self.job = self._service._assert_active_fence(
                job_id=self.job.job_id,
                fence=self.fence,
                stage=stage,
            )
            return self.job


_SCHEMA_SQL = f"""
PRAGMA application_id = {_APPLICATION_ID};
PRAGMA user_version = {_SCHEMA_VERSION};
CREATE TABLE IF NOT EXISTS recovery_metadata(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS recovery_job(
    job_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    backup_root TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    tool_bundle_path TEXT NOT NULL,
    restore_root TEXT NOT NULL,
    status TEXT NOT NULL,
    deadline_at TEXT NOT NULL,
    attempt_timeout_seconds INTEGER NOT NULL,
    next_attempt_at TEXT NOT NULL,
    rehearsal_interval_seconds INTEGER,
    attempt_count INTEGER NOT NULL,
    fence INTEGER NOT NULL,
    lease_owner TEXT,
    lease_until TEXT,
    checkpoint_stage TEXT,
    checkpoint_json TEXT,
    recovery_receipt_id TEXT,
    last_error_type TEXT,
    last_error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS recovery_job_ready_idx
ON recovery_job(status, next_attempt_at, deadline_at, job_id);
CREATE TABLE IF NOT EXISTS recovery_receipt(
    receipt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    fence INTEGER NOT NULL,
    status TEXT NOT NULL,
    verification_level TEXT,
    recovery_receipt_id TEXT,
    content_sha256 TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(job_id, fence)
) STRICT;
CREATE TABLE IF NOT EXISTS recovery_receipt_outbox(
    receipt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    fence INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(job_id, fence)
) STRICT;
CREATE INDEX IF NOT EXISTS recovery_receipt_outbox_created_idx
ON recovery_receipt_outbox(created_at, receipt_id);
CREATE TABLE IF NOT EXISTS recovery_receipt_migration(
    legacy_receipt_id TEXT PRIMARY KEY,
    upgraded_receipt_id TEXT NOT NULL UNIQUE,
    job_id TEXT NOT NULL,
    fence INTEGER NOT NULL,
    legacy_payload_json TEXT NOT NULL,
    upgraded_payload_json TEXT NOT NULL,
    legacy_content_sha256 TEXT NOT NULL,
    upgraded_content_sha256 TEXT NOT NULL,
    legacy_relative_path TEXT NOT NULL,
    upgraded_relative_path TEXT NOT NULL,
    status TEXT NOT NULL,
    v2_published_at TEXT,
    index_updated_at TEXT,
    archived_at TEXT,
    audited_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(job_id, fence)
) STRICT;
CREATE INDEX IF NOT EXISTS recovery_receipt_migration_status_idx
ON recovery_receipt_migration(status, created_at, legacy_receipt_id);
CREATE TABLE IF NOT EXISTS recovery_audit(
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    previous_sha256 TEXT,
    event_sha256 TEXT NOT NULL UNIQUE,
    event_json TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;
"""

_EXPECTED_COLUMNS = {
    "recovery_metadata": ("key", "value"),
    "recovery_job": (
        "job_id",
        "request_id",
        "backup_root",
        "manifest_path",
        "tool_bundle_path",
        "restore_root",
        "status",
        "deadline_at",
        "attempt_timeout_seconds",
        "next_attempt_at",
        "rehearsal_interval_seconds",
        "attempt_count",
        "fence",
        "lease_owner",
        "lease_until",
        "checkpoint_stage",
        "checkpoint_json",
        "recovery_receipt_id",
        "last_error_type",
        "last_error_message",
        "created_at",
        "updated_at",
    ),
    "recovery_receipt": (
        "receipt_id",
        "job_id",
        "fence",
        "status",
        "verification_level",
        "recovery_receipt_id",
        "content_sha256",
        "relative_path",
        "created_at",
    ),
    "recovery_receipt_outbox": (
        "receipt_id",
        "job_id",
        "fence",
        "payload_json",
        "content_sha256",
        "relative_path",
        "created_at",
    ),
    "recovery_receipt_migration": (
        "legacy_receipt_id",
        "upgraded_receipt_id",
        "job_id",
        "fence",
        "legacy_payload_json",
        "upgraded_payload_json",
        "legacy_content_sha256",
        "upgraded_content_sha256",
        "legacy_relative_path",
        "upgraded_relative_path",
        "status",
        "v2_published_at",
        "index_updated_at",
        "archived_at",
        "audited_at",
        "completed_at",
        "created_at",
        "updated_at",
    ),
    "recovery_audit": (
        "sequence",
        "previous_sha256",
        "event_sha256",
        "event_json",
        "created_at",
    ),
}

_MIGRATION_STATUS_ORDER = {
    "intent": 0,
    "v2_published": 1,
    "indexed": 2,
    "archived": 3,
    "audited": 4,
    "completed": 5,
}

_MIGRATION_STATUS_TIMESTAMPS = {
    "v2_published": "v2_published_at",
    "indexed": "index_updated_at",
    "archived": "archived_at",
    "audited": "audited_at",
    "completed": "completed_at",
}


class RuntimeRecoveryService:
    def __init__(
        self,
        *,
        state_path: Path,
        receipt_root: Path,
        worker_id: str,
        clock: Callable[[], datetime] | None = None,
        lease_seconds: int = 300,
        max_attempts: int = 3,
        retry_delay_seconds: int = 60,
    ) -> None:
        if not worker_id or len(worker_id) > 256:
            raise ValueError("worker_id is invalid")
        for label, value in (
            ("lease_seconds", lease_seconds),
            ("max_attempts", max_attempts),
            ("retry_delay_seconds", retry_delay_seconds),
        ):
            if type(value) is not int or value < 1:
                raise ValueError(f"{label} must be a positive integer")
        self.state_path = Path(_canonical_absolute(state_path, label="state_path"))
        self.receipt_root = Path(_canonical_absolute(receipt_root, label="receipt_root"))
        self.worker_id = worker_id
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds
        self._prepare_parent(self.state_path.parent)
        self._prepare_parent(self.receipt_root)
        self._prepare_state_file()
        self._initialize()
        self.reconcile_receipt_publications()

    @staticmethod
    def _prepare_parent(path: Path) -> None:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        observed = os.lstat(path)
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise RecoveryServiceIntegrityError("recovery service parent is unsafe")

    def _prepare_state_file(self) -> None:
        try:
            descriptor = os.open(
                self.state_path,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        except FileExistsError:
            descriptor = os.open(
                self.state_path,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            )
        try:
            observed = os.fstat(descriptor)
            named = os.lstat(self.state_path)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or observed.st_nlink != 1
                or stat.S_IMODE(observed.st_mode) != 0o600
                or (observed.st_dev, observed.st_ino) != (named.st_dev, named.st_ino)
            ):
                raise RecoveryServiceIntegrityError("recovery service state file is unsafe")
            self._state_identity = (observed.st_dev, observed.st_ino)
            os.fsync(descriptor)
        except OSError as exc:
            raise RecoveryServiceIntegrityError("recovery service state path is unsafe") from exc
        finally:
            os.close(descriptor)
        directory = os.open(
            self.state_path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def _connect(self) -> sqlite3.Connection:
        descriptor = -1
        try:
            descriptor = os.open(
                self.state_path,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            )
            opened = os.fstat(descriptor)
            named = os.lstat(self.state_path)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.geteuid()
                or opened.st_nlink != 1
                or stat.S_IMODE(opened.st_mode) != 0o600
                or (opened.st_dev, opened.st_ino) != self._state_identity
                or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            ):
                raise RecoveryServiceIntegrityError("recovery service state identity changed")
            connection = sqlite3.connect(self.state_path, timeout=5, isolation_level=None)
            after = os.fstat(descriptor)
            named_after = os.lstat(self.state_path)
            if (after.st_dev, after.st_ino) != self._state_identity or (
                after.st_dev,
                after.st_ino,
            ) != (named_after.st_dev, named_after.st_ino):
                connection.close()
                raise RecoveryServiceIntegrityError("recovery service state path changed")
        except OSError as exc:
            raise RecoveryServiceIntegrityError("recovery service state path is unsafe") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            previous_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if previous_version not in {0, 3, _SCHEMA_VERSION}:
                raise RecoveryServiceIntegrityError(
                    "recovery service schema migration source is unsupported"
                )
            connection.executescript(_SCHEMA_SQL)
            previous_metadata = connection.execute(
                "SELECT value FROM recovery_metadata WHERE key = 'schema_version'"
            ).fetchone()
            previous_metadata_version = (
                None if previous_metadata is None else str(previous_metadata[0])
            )
            if previous_metadata_version not in {None, "3", str(_SCHEMA_VERSION)}:
                raise RecoveryServiceIntegrityError(
                    "recovery service schema metadata source is unsupported"
                )
            connection.execute(
                "INSERT OR IGNORE INTO recovery_metadata(key, value) VALUES ('schema_version', ?)",
                (str(_SCHEMA_VERSION),),
            )
            connection.execute(
                "INSERT OR IGNORE INTO recovery_metadata(key, value) VALUES ('audit_sequence', '0')"
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO recovery_metadata(key, value)
                VALUES ('audit_head_sha256', '')
                """
            )
            if previous_version == 3 or previous_metadata_version == "3":
                self._migrate_v3_receipts(connection)
            connection.execute(
                "UPDATE recovery_metadata SET value = ? WHERE key = 'schema_version'",
                (str(_SCHEMA_VERSION),),
            )
            connection.commit()
            os.chmod(self.state_path, 0o600)
            self._attest_state(connection, full_audit=True)
        finally:
            connection.close()

    @staticmethod
    def _attest_schema(
        connection: sqlite3.Connection,
        *,
        full_audit: bool = False,
    ) -> None:
        application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if (application_id, user_version) != (_APPLICATION_ID, _SCHEMA_VERSION):
            raise RecoveryServiceIntegrityError("recovery service schema identity differs")
        objects = {
            (str(row[0]), str(row[1]))
            for row in connection.execute(
                "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        expected_objects = {
            ("table", "recovery_metadata"),
            ("table", "recovery_job"),
            ("table", "recovery_receipt"),
            ("table", "recovery_receipt_outbox"),
            ("table", "recovery_receipt_migration"),
            ("table", "recovery_audit"),
            ("index", "recovery_job_ready_idx"),
            ("index", "recovery_receipt_outbox_created_idx"),
            ("index", "recovery_receipt_migration_status_idx"),
        }
        if objects != expected_objects:
            raise RecoveryServiceIntegrityError("recovery service schema objects differ")
        for table, expected in _EXPECTED_COLUMNS.items():
            observed = tuple(
                str(row[1]) for row in connection.execute(f"PRAGMA table_info('{table}')")
            )
            if observed != expected:
                raise RecoveryServiceIntegrityError("recovery service schema columns differ")
        metadata = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                """
                SELECT key, value FROM recovery_metadata
                WHERE key IN ('schema_version', 'audit_sequence', 'audit_head_sha256')
                """
            )
        }
        if metadata.get("schema_version") != str(_SCHEMA_VERSION):
            raise RecoveryServiceIntegrityError("recovery service schema metadata differs")
        RuntimeRecoveryService._attest_migration_intents(connection, require_completed=True)
        RuntimeRecoveryService._attest_audit_head(connection, metadata=metadata)
        if full_audit:
            sequence, head = RuntimeRecoveryService._attest_audit_chain(connection)
            if sequence != int(metadata.get("audit_sequence", "-1")) or head != metadata.get(
                "audit_head_sha256"
            ):
                raise RecoveryServiceIntegrityError(
                    "recovery audit checkpoint differs from full chain"
                )

    def _attest_state(
        self,
        connection: sqlite3.Connection,
        *,
        full_audit: bool = False,
    ) -> None:
        self._attest_schema(connection, full_audit=full_audit)
        self._attest_migration_files_at(
            connection,
            receipt_root=self.receipt_root,
            require_completed=True,
        )

    @staticmethod
    def _migration_event_payload(
        *,
        legacy_receipt_id: str,
        upgraded_receipt_id: str,
    ) -> dict[str, str]:
        return {
            "event": "legacy_receipt_upgraded",
            "legacy_receipt_id": legacy_receipt_id,
            "receipt_id": upgraded_receipt_id,
        }

    @staticmethod
    def _migration_audit_event_exists(
        connection: sqlite3.Connection,
        *,
        legacy_receipt_id: str,
        upgraded_receipt_id: str,
    ) -> bool:
        expected = RuntimeRecoveryService._migration_event_payload(
            legacy_receipt_id=legacy_receipt_id,
            upgraded_receipt_id=upgraded_receipt_id,
        )
        observed = 0
        cursor = connection.execute("SELECT event_json FROM recovery_audit ORDER BY sequence")
        while (row := cursor.fetchone()) is not None:
            observed += 1
            if observed > _MAX_AUDIT_EVENTS:
                raise RecoveryServiceIntegrityError("recovery audit chain exceeds event budget")
            raw = str(row["event_json"]).encode("utf-8")
            if len(raw) > _MAX_AUDIT_EVENT_BYTES:
                raise RecoveryServiceIntegrityError("recovery audit event exceeds byte budget")
            try:
                payload = strict_canonical_json_loads(raw)
            except Exception as exc:
                raise RecoveryServiceIntegrityError("recovery audit JSON is invalid") from exc
            if isinstance(payload, dict) and payload.get("event") == expected:
                return True
        return False

    @staticmethod
    def _migration_receipts_from_row(
        row: sqlite3.Row,
        *,
        require_completed: bool,
    ) -> tuple[LegacyRecoveryServiceReceipt, RecoveryServiceReceipt]:
        status = str(row["status"])
        if status not in _MIGRATION_STATUS_ORDER:
            raise RecoveryServiceIntegrityError("recovery receipt migration status is invalid")
        if require_completed and status != "completed":
            raise RecoveryServiceIntegrityError("recovery receipt migration is incomplete")
        status_rank = _MIGRATION_STATUS_ORDER[status]
        for stage, column in _MIGRATION_STATUS_TIMESTAMPS.items():
            value = row[column]
            if _MIGRATION_STATUS_ORDER[stage] <= status_rank:
                if _decode_time(value) is None:
                    raise RecoveryServiceIntegrityError(
                        "recovery receipt migration timestamp is missing"
                    )
            elif value is not None:
                raise RecoveryServiceIntegrityError(
                    "recovery receipt migration timestamp is ahead of status"
                )
        if _decode_time(row["created_at"]) is None or _decode_time(row["updated_at"]) is None:
            raise RecoveryServiceIntegrityError("recovery receipt migration timestamp is invalid")
        legacy_raw = str(row["legacy_payload_json"]).encode("utf-8")
        upgraded_raw = str(row["upgraded_payload_json"]).encode("utf-8")
        if len(legacy_raw) > _MAX_CONTRACT_BYTES or len(upgraded_raw) > _MAX_CONTRACT_BYTES:
            raise RecoveryServiceIntegrityError("recovery receipt migration payload is oversized")
        try:
            legacy = LegacyRecoveryServiceReceipt.model_validate(
                strict_canonical_json_loads(legacy_raw)
            )
            upgraded = RecoveryServiceReceipt.model_validate(
                strict_canonical_json_loads(upgraded_raw)
            )
        except Exception as exc:
            raise RecoveryServiceIntegrityError(
                "recovery receipt migration payload is invalid"
            ) from exc
        expected_upgraded = RuntimeRecoveryService._upgrade_v3_receipt(legacy)
        if (
            canonical_json_bytes(legacy.model_dump(mode="json")) != legacy_raw
            or canonical_json_bytes(upgraded.model_dump(mode="json")) != upgraded_raw
            or upgraded != expected_upgraded
            or row["legacy_receipt_id"] != legacy.receipt_id
            or row["upgraded_receipt_id"] != upgraded.receipt_id
            or row["job_id"] != legacy.job_id
            or row["job_id"] != upgraded.job_id
            or int(row["fence"]) != legacy.fence
            or int(row["fence"]) != upgraded.fence
            or row["legacy_content_sha256"] != hashlib.sha256(legacy_raw).hexdigest()
            or row["upgraded_content_sha256"] != hashlib.sha256(upgraded_raw).hexdigest()
            or row["legacy_relative_path"] != f"{legacy.receipt_id}.json"
            or row["upgraded_relative_path"] != f"{upgraded.receipt_id}.json"
        ):
            raise RecoveryServiceIntegrityError("recovery receipt migration intent differs")
        return legacy, upgraded

    @staticmethod
    def _attest_migration_intents(
        connection: sqlite3.Connection,
        *,
        require_completed: bool,
    ) -> None:
        rows = connection.execute(
            """
            SELECT * FROM recovery_receipt_migration
            ORDER BY created_at, legacy_receipt_id LIMIT 10001
            """
        ).fetchall()
        if len(rows) > 10_000:
            raise RecoveryServiceIntegrityError(
                "recovery receipt migration inventory exceeds budget"
            )
        for row in rows:
            legacy, upgraded = RuntimeRecoveryService._migration_receipts_from_row(
                row,
                require_completed=require_completed,
            )
            if require_completed and not RuntimeRecoveryService._migration_audit_event_exists(
                connection,
                legacy_receipt_id=str(legacy.receipt_id),
                upgraded_receipt_id=str(upgraded.receipt_id),
            ):
                raise RecoveryServiceIntegrityError("recovery receipt migration audit is missing")

    @staticmethod
    def _verify_upgraded_migration_receipt_at(
        receipt_root: Path,
        receipt: RecoveryServiceReceipt,
    ) -> None:
        assert receipt.receipt_id is not None
        filename = f"{receipt.receipt_id}.json"
        expected = canonical_json_bytes(receipt.model_dump(mode="json"))
        root_fd = -1
        try:
            root_fd = os.open(
                receipt_root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            root_before = _validate_directory_fd(
                root_fd,
                receipt_root,
                label="recovery receipt root",
                required_mode=0o700,
            )
            try:
                observed = _read_canonical_contract_at(
                    root_fd,
                    receipt_root,
                    filename,
                    RecoveryServiceReceipt,
                )
            except RecoveryServiceIntegrityError as exc:
                raise RecoveryServiceIntegrityError(
                    "upgraded recovery receipt is missing or invalid"
                ) from exc
            payload = canonical_json_bytes(observed.model_dump(mode="json"))
            root_after = _validate_directory_fd(
                root_fd,
                receipt_root,
                label="recovery receipt root",
                required_mode=0o700,
            )
            if (
                observed != receipt
                or payload != expected
                or hashlib.sha256(payload).hexdigest() != hashlib.sha256(expected).hexdigest()
                or _directory_fd_identity(root_before) != _directory_fd_identity(root_after)
            ):
                raise RecoveryServiceIntegrityError("upgraded recovery receipt differs")
        except OSError as exc:
            raise RecoveryServiceIntegrityError(
                "upgraded recovery receipt path is unavailable or unsafe"
            ) from exc
        finally:
            if root_fd >= 0:
                os.close(root_fd)

    @staticmethod
    def _verify_archived_migration_receipt_at(
        receipt_root: Path,
        receipt: LegacyRecoveryServiceReceipt,
        *,
        expected_identity: _MigrationArchiveProof | None = None,
    ) -> None:
        assert receipt.receipt_id is not None
        filename = f"{receipt.receipt_id}.json"
        expected = canonical_json_bytes(receipt.model_dump(mode="json"))
        archive = receipt_root / ".legacy-v1"
        root_fd = -1
        archive_fd = -1
        try:
            root_fd = os.open(
                receipt_root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            root_before = _validate_directory_fd(
                root_fd,
                receipt_root,
                label="recovery receipt root",
                required_mode=0o700,
            )
            archive_fd = os.open(
                ".legacy-v1",
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
            archive_before = _validate_directory_fd(
                archive_fd,
                archive,
                label="legacy recovery receipt archive",
                required_mode=0o700,
            )
            try:
                observed = _read_canonical_contract_at(
                    archive_fd,
                    archive,
                    filename,
                    LegacyRecoveryServiceReceipt,
                )
            except RecoveryServiceIntegrityError as exc:
                raise RecoveryServiceIntegrityError(
                    "archived legacy recovery receipt is missing or invalid"
                ) from exc
            observed_identity = _archive_proof_at(archive_fd, filename)
            payload = canonical_json_bytes(observed.model_dump(mode="json"))
            if _path_exists_at(root_fd, filename):
                raise RecoveryServiceIntegrityError(
                    "legacy recovery receipt was not durably archived"
                )
            root_after = _validate_directory_fd(
                root_fd,
                receipt_root,
                label="recovery receipt root",
                required_mode=0o700,
            )
            archive_after = _validate_directory_fd(
                archive_fd,
                archive,
                label="legacy recovery receipt archive",
                required_mode=0o700,
            )
            if (
                observed != receipt
                or payload != expected
                or hashlib.sha256(payload).hexdigest() != hashlib.sha256(expected).hexdigest()
                or _directory_fd_identity(root_before) != _directory_fd_identity(root_after)
                or _directory_fd_identity(archive_before) != _directory_fd_identity(archive_after)
            ):
                raise RecoveryServiceIntegrityError("archived legacy recovery receipt differs")
            if expected_identity is not None and observed_identity != expected_identity:
                raise RecoveryServiceIntegrityError(
                    "legacy recovery receipt archive changed after write"
                )
        except OSError as exc:
            raise RecoveryServiceIntegrityError(
                "legacy recovery receipt archive is unavailable or unsafe"
            ) from exc
        finally:
            if archive_fd >= 0:
                os.close(archive_fd)
            if root_fd >= 0:
                os.close(root_fd)

    @staticmethod
    def _attest_migration_files_at(
        connection: sqlite3.Connection,
        *,
        receipt_root: Path,
        require_completed: bool,
    ) -> None:
        rows = connection.execute(
            """
            SELECT * FROM recovery_receipt_migration
            ORDER BY created_at, legacy_receipt_id LIMIT 10001
            """
        ).fetchall()
        if len(rows) > 10_000:
            raise RecoveryServiceIntegrityError(
                "recovery receipt migration inventory exceeds budget"
            )
        for row in rows:
            legacy, upgraded = RuntimeRecoveryService._migration_receipts_from_row(
                row,
                require_completed=require_completed,
            )
            status_rank = _MIGRATION_STATUS_ORDER[str(row["status"])]
            if status_rank >= _MIGRATION_STATUS_ORDER["v2_published"]:
                RuntimeRecoveryService._verify_upgraded_migration_receipt_at(
                    receipt_root,
                    upgraded,
                )
            if status_rank >= _MIGRATION_STATUS_ORDER["archived"]:
                RuntimeRecoveryService._verify_archived_migration_receipt_at(
                    receipt_root,
                    legacy,
                )

    @staticmethod
    def _attest_audit_head(
        connection: sqlite3.Connection,
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> None:
        if metadata is None:
            metadata = {
                str(row[0]): str(row[1])
                for row in connection.execute(
                    """
                    SELECT key, value FROM recovery_metadata
                    WHERE key IN ('audit_sequence', 'audit_head_sha256')
                    """
                )
            }
        try:
            expected_sequence = int(metadata["audit_sequence"])
            expected_head = metadata["audit_head_sha256"]
        except (KeyError, ValueError) as exc:
            raise RecoveryServiceIntegrityError("recovery audit checkpoint is missing") from exc
        latest = connection.execute(
            """
            SELECT sequence, event_sha256, event_json, created_at
            FROM recovery_audit ORDER BY sequence DESC LIMIT 1
            """
        ).fetchone()
        if latest is None:
            if expected_sequence != 0 or expected_head != "":
                raise RecoveryServiceIntegrityError("recovery audit head differs")
            return
        raw = str(latest["event_json"]).encode("utf-8")
        if len(raw) > _MAX_AUDIT_EVENT_BYTES:
            raise RecoveryServiceIntegrityError("recovery audit event exceeds byte budget")
        try:
            payload = strict_canonical_json_loads(raw)
        except Exception as exc:
            raise RecoveryServiceIntegrityError("recovery audit head JSON is invalid") from exc
        if (
            int(latest["sequence"]) != expected_sequence
            or str(latest["event_sha256"]) != expected_head
            or hashlib.sha256(raw).hexdigest() != expected_head
            or canonical_json_bytes(payload) != raw
            or not isinstance(payload, dict)
            or payload.get("contract") != "runtime-recovery-service-audit/v1"
            or payload.get("created_at") != latest["created_at"]
        ):
            raise RecoveryServiceIntegrityError("recovery audit head differs")

    @staticmethod
    def _attest_audit_chain(
        connection: sqlite3.Connection,
        *,
        max_events: int = _MAX_AUDIT_EVENTS,
    ) -> tuple[int, str]:
        if type(max_events) is not int or max_events < 1:
            raise ValueError("max_events must be a positive integer")
        previous_sha: str | None = None
        expected_sequence = 1
        cursor = connection.execute(
            """
            SELECT sequence, previous_sha256, event_sha256, event_json, created_at
            FROM recovery_audit ORDER BY sequence
            """
        )
        while (row := cursor.fetchone()) is not None:
            if expected_sequence > max_events:
                raise RecoveryServiceIntegrityError("recovery audit chain exceeds event budget")
            sequence = int(row["sequence"])
            raw = str(row["event_json"]).encode("utf-8")
            if len(raw) > _MAX_AUDIT_EVENT_BYTES:
                raise RecoveryServiceIntegrityError("recovery audit event exceeds byte budget")
            try:
                payload = strict_canonical_json_loads(raw)
            except Exception as exc:
                raise RecoveryServiceIntegrityError("recovery audit JSON is invalid") from exc
            if (
                sequence != expected_sequence
                or row["previous_sha256"] != previous_sha
                or hashlib.sha256(raw).hexdigest() != str(row["event_sha256"])
                or canonical_json_bytes(payload) != raw
                or not isinstance(payload, dict)
                or payload.get("contract") != "runtime-recovery-service-audit/v1"
                or payload.get("previous_sha256") != previous_sha
                or payload.get("created_at") != row["created_at"]
            ):
                raise RecoveryServiceIntegrityError("recovery append-only audit chain differs")
            previous_sha = str(row["event_sha256"])
            expected_sequence += 1
        return expected_sequence - 1, previous_sha or ""

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._attest_state(connection, full_audit=False)
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _identity_payload(
        *,
        request_id: str,
        backup_root: str,
        manifest_path: str,
        tool_bundle_path: str,
        restore_root: str,
    ) -> dict[str, object]:
        return {
            "contract": "runtime-recovery-service-job/v3",
            "request_id": request_id,
            "backup_root": backup_root,
            "manifest_path": manifest_path,
            "tool_bundle_path": tool_bundle_path,
            "restore_root": restore_root,
        }

    def submit(
        self,
        *,
        request_id: str,
        backup_root: Path,
        manifest_path: Path,
        tool_bundle_path: Path,
        restore_root: Path,
        deadline_at: datetime,
        rehearsal_interval_seconds: int | None = None,
    ) -> RecoveryServiceJob:
        if not request_id or len(request_id) > 256:
            raise ValueError("request_id is invalid")
        if rehearsal_interval_seconds is not None:
            raise ValueError(
                "periodic recovery rehearsal must be scheduled by the external systemd timer"
            )
        backup = _canonical_absolute(backup_root, label="backup_root")
        manifest = _canonical_absolute(manifest_path, label="manifest_path")
        tool = _canonical_absolute(tool_bundle_path, label="tool_bundle_path")
        restore = _canonical_absolute(restore_root, label="restore_root")
        backup_path = Path(backup)
        if (
            not Path(manifest).is_relative_to(backup_path)
            or not Path(tool).is_relative_to(backup_path)
            or Path(restore) == backup_path
            or Path(restore).is_relative_to(backup_path)
            or backup_path.is_relative_to(Path(restore))
        ):
            raise ValueError("recovery job backup, contracts, and restore roots are not isolated")
        now = self.clock().astimezone(UTC)
        if deadline_at.tzinfo is None or deadline_at.utcoffset() is None or deadline_at <= now:
            raise ValueError("recovery deadline must be future and timezone-aware")
        payload = self._identity_payload(
            request_id=request_id,
            backup_root=backup,
            manifest_path=manifest,
            tool_bundle_path=tool,
            restore_root=restore,
        )
        job_id = canonical_sha256(payload)
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM recovery_job WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                job = self._job_from_row(existing)
                if job.job_id != job_id:
                    raise RecoveryServiceIntegrityError(
                        "request_id was reused with different content"
                    )
                return job
            connection.execute(
                """
                INSERT INTO recovery_job(
                    job_id, request_id, backup_root, manifest_path, tool_bundle_path, restore_root,
                    status, deadline_at, attempt_timeout_seconds, next_attempt_at,
                    rehearsal_interval_seconds,
                    attempt_count, fence, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, 0, 0, ?, ?)
                """,
                (
                    job_id,
                    request_id,
                    backup,
                    manifest,
                    tool,
                    restore,
                    _encode_time(deadline_at),
                    max(1, math.ceil((deadline_at.astimezone(UTC) - now).total_seconds())),
                    _encode_time(now),
                    rehearsal_interval_seconds,
                    _encode_time(now),
                    _encode_time(now),
                ),
            )
            self._append_audit(connection, {"event": "submitted", "job_id": job_id}, now=now)
            row = connection.execute(
                "SELECT * FROM recovery_job WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            assert row is not None
            return self._job_from_row(row)

    def submit_from_record(self, job: RecoveryServiceJob) -> RecoveryServiceJob:
        return self.submit(
            request_id=job.request_id,
            backup_root=Path(job.backup_root),
            manifest_path=Path(job.manifest_path),
            tool_bundle_path=Path(job.tool_bundle_path),
            restore_root=Path(job.restore_root),
            deadline_at=job.deadline_at,
            rehearsal_interval_seconds=job.rehearsal_interval_seconds,
        )

    @staticmethod
    def _job_from_row(row: sqlite3.Row) -> RecoveryServiceJob:
        checkpoint_raw = row["checkpoint_json"]
        checkpoint = (
            None
            if checkpoint_raw is None
            else strict_canonical_json_loads(str(checkpoint_raw).encode("utf-8"))
        )
        return RecoveryServiceJob(
            job_id=row["job_id"],
            request_id=row["request_id"],
            backup_root=row["backup_root"],
            manifest_path=row["manifest_path"],
            tool_bundle_path=row["tool_bundle_path"],
            restore_root=row["restore_root"],
            status=row["status"],
            deadline_at=_decode_time(row["deadline_at"]),
            attempt_timeout_seconds=int(row["attempt_timeout_seconds"]),
            next_attempt_at=_decode_time(row["next_attempt_at"]),
            rehearsal_interval_seconds=row["rehearsal_interval_seconds"],
            attempt_count=int(row["attempt_count"]),
            fence=int(row["fence"]),
            lease_owner=row["lease_owner"],
            lease_until=_decode_time(row["lease_until"]),
            checkpoint_stage=row["checkpoint_stage"],
            checkpoint=checkpoint,
            recovery_receipt_id=row["recovery_receipt_id"],
            last_error_type=row["last_error_type"],
            last_error_message=row["last_error_message"],
            created_at=_decode_time(row["created_at"]),
            updated_at=_decode_time(row["updated_at"]),
        )

    def job(self, job_id: str) -> RecoveryServiceJob:
        connection = self._connect()
        try:
            self._attest_state(connection, full_audit=False)
            row = connection.execute(
                "SELECT * FROM recovery_job WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            return self._job_from_row(row)
        finally:
            connection.close()

    def verified_receipts(
        self,
        *,
        job_id: str | None = None,
    ) -> tuple[RecoveryServiceReceipt, ...]:
        """Return immutable receipts after checking catalog, file, and content identity."""

        connection = self._connect()
        try:
            self._attest_state(connection, full_audit=True)
            if job_id is None:
                rows = connection.execute(
                    "SELECT * FROM recovery_receipt ORDER BY created_at, receipt_id LIMIT 10001"
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT * FROM recovery_receipt WHERE job_id = ?
                    ORDER BY created_at, receipt_id LIMIT 10001
                    """,
                    (job_id,),
                ).fetchall()
        finally:
            connection.close()
        if len(rows) > 10_000:
            raise RecoveryServiceIntegrityError("recovery receipt inventory exceeds budget")
        receipts: list[RecoveryServiceReceipt] = []
        for row in rows:
            relative = str(row["relative_path"])
            if Path(relative).name != relative or not relative.endswith(".json"):
                raise RecoveryServiceIntegrityError("recovery receipt path is unsafe")
            try:
                receipt = _read_canonical_contract(
                    self.receipt_root / relative,
                    RecoveryServiceReceipt,
                )
            except RecoveryServiceIntegrityError as exc:
                raise RecoveryServiceIntegrityError(
                    "immutable recovery receipt is invalid"
                ) from exc
            payload = canonical_json_bytes(receipt.model_dump(mode="json"))
            if (
                receipt.receipt_id != row["receipt_id"]
                or receipt.job_id != row["job_id"]
                or receipt.fence != int(row["fence"])
                or receipt.status != row["status"]
                or receipt.verification_level != row["verification_level"]
                or receipt.recovery_receipt_id != row["recovery_receipt_id"]
                or hashlib.sha256(payload).hexdigest() != row["content_sha256"]
                or _encode_time(receipt.completed_at) != row["created_at"]
            ):
                raise RecoveryServiceIntegrityError("immutable recovery receipt differs")
            receipts.append(receipt)
        return tuple(receipts)

    def _append_audit(
        self,
        connection: sqlite3.Connection,
        event: Mapping[str, JsonValue],
        *,
        now: datetime,
    ) -> None:
        metadata = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                """
                SELECT key, value FROM recovery_metadata
                WHERE key IN ('audit_sequence', 'audit_head_sha256')
                """
            )
        }
        try:
            previous_sequence = int(metadata["audit_sequence"])
            previous_head = metadata["audit_head_sha256"]
        except (KeyError, ValueError) as exc:
            raise RecoveryServiceIntegrityError("recovery audit checkpoint is missing") from exc
        previous_sha = previous_head or None
        payload = {
            "contract": "runtime-recovery-service-audit/v1",
            "previous_sha256": previous_sha,
            "event": dict(event),
            "created_at": _encode_time(now),
        }
        event_json = canonical_json_bytes(payload).decode("utf-8")
        event_sha = hashlib.sha256(event_json.encode("utf-8")).hexdigest()
        cursor = connection.execute(
            """
            INSERT INTO recovery_audit(previous_sha256, event_sha256, event_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (previous_sha, event_sha, event_json, _encode_time(now)),
        )
        sequence = int(cursor.lastrowid or 0)
        if sequence != previous_sequence + 1:
            raise RecoveryServiceIntegrityError("recovery audit sequence is not monotonic")
        connection.executemany(
            "UPDATE recovery_metadata SET value = ? WHERE key = ?",
            (
                (str(sequence), "audit_sequence"),
                (event_sha, "audit_head_sha256"),
            ),
        )

    def _claim(self) -> RecoveryServiceJob | None:
        now = self.clock().astimezone(UTC)
        with self._transaction() as connection:
            expired_scheduled = connection.execute(
                """
                SELECT job_id, fence FROM recovery_job
                WHERE status = 'scheduled' AND deadline_at <= ?
                ORDER BY job_id LIMIT 1001
                """,
                (_encode_time(now),),
            ).fetchall()
            if len(expired_scheduled) > 1000:
                raise RecoveryServiceIntegrityError(
                    "expired recovery rehearsal inventory exceeds budget"
                )
            for row in expired_scheduled:
                terminal_fence = int(row["fence"]) + 1
                changed = connection.execute(
                    """
                    UPDATE recovery_job
                    SET status = 'failed', fence = ?, lease_owner = NULL, lease_until = NULL,
                        last_error_type = 'ExpiredRecoveryDeadline',
                        last_error_message = 'scheduled recovery rehearsal deadline expired',
                        updated_at = ?
                    WHERE job_id = ? AND status = 'scheduled' AND fence = ?
                      AND deadline_at <= ?
                    """,
                    (
                        terminal_fence,
                        _encode_time(now),
                        row["job_id"],
                        row["fence"],
                        _encode_time(now),
                    ),
                ).rowcount
                if changed != 1:
                    raise RecoveryServiceIntegrityError("expired recovery rehearsal fence was lost")
                receipt = RecoveryServiceReceipt(
                    job_id=row["job_id"],
                    fence=terminal_fence,
                    status="failed",
                    error_type="ExpiredRecoveryDeadline",
                    error_message="scheduled recovery rehearsal deadline expired",
                    completed_at=now,
                )
                self._stage_service_receipt(connection, receipt)
                self._append_audit(
                    connection,
                    {
                        "event": "failed",
                        "job_id": row["job_id"],
                        "fence": terminal_fence,
                        "error_type": "ExpiredRecoveryDeadline",
                    },
                    now=now,
                )
            expired = connection.execute(
                """
                SELECT job_id, attempt_count, deadline_at, fence FROM recovery_job
                WHERE status = 'running' AND lease_until < ? ORDER BY job_id
                """,
                (_encode_time(now),),
            ).fetchall()
            for row in expired:
                deadline = _decode_time(row["deadline_at"])
                exhausted = int(row["attempt_count"]) >= self.max_attempts or deadline <= now
                changed = connection.execute(
                    """
                    UPDATE recovery_job
                    SET status = ?, lease_owner = NULL, lease_until = NULL,
                        next_attempt_at = ?, updated_at = ?,
                        last_error_type = 'ExpiredRecoveryLease',
                        last_error_message = 'recovery worker lease expired'
                    WHERE job_id = ? AND status = 'running' AND lease_until < ?
                      AND fence = ?
                    """,
                    (
                        "failed" if exhausted else "pending",
                        _encode_time(now),
                        _encode_time(now),
                        row["job_id"],
                        _encode_time(now),
                        row["fence"],
                    ),
                ).rowcount
                if changed != 1:
                    raise RecoveryServiceLeaseLostError(
                        "expired recovery lease fence was superseded"
                    )
                self._append_audit(
                    connection,
                    {
                        "event": "lease_expired",
                        "job_id": row["job_id"],
                        "fence": int(row["fence"]),
                        "error_class": "transient_lease",
                        "terminal": exhausted,
                    },
                    now=now,
                )
                receipt = RecoveryServiceReceipt(
                    job_id=row["job_id"],
                    fence=int(row["fence"]),
                    status="failed" if exhausted else "retry_scheduled",
                    error_type="ExpiredRecoveryLease",
                    error_message="recovery worker lease expired",
                    completed_at=now,
                )
                self._stage_service_receipt(connection, receipt)
                if exhausted:
                    self._append_audit(
                        connection,
                        {
                            "event": "failed",
                            "job_id": row["job_id"],
                            "fence": int(row["fence"]),
                            "error_type": "ExpiredRecoveryLease",
                            "error_class": "transient_lease",
                        },
                        now=now,
                    )
            row = connection.execute(
                """
                SELECT * FROM recovery_job
                WHERE status = 'pending'
                  AND next_attempt_at <= ? AND deadline_at > ?
                  AND attempt_count < ?
                ORDER BY next_attempt_at, job_id LIMIT 1
                """,
                (_encode_time(now), _encode_time(now), self.max_attempts),
            ).fetchone()
            if row is None:
                return None
            fence = int(row["fence"]) + 1
            attempts = int(row["attempt_count"]) + 1
            lease_until = now + timedelta(seconds=self.lease_seconds)
            changed = connection.execute(
                """
                UPDATE recovery_job
                SET status = 'running', attempt_count = ?, fence = ?,
                    lease_owner = ?, lease_until = ?, updated_at = ?,
                    last_error_type = NULL, last_error_message = NULL
                WHERE job_id = ? AND status = 'pending' AND fence = ?
                """,
                (
                    attempts,
                    fence,
                    self.worker_id,
                    _encode_time(lease_until),
                    _encode_time(now),
                    row["job_id"],
                    row["fence"],
                ),
            ).rowcount
            if changed != 1:
                raise RecoveryServiceIntegrityError("recovery claim fence was lost")
            self._append_audit(
                connection,
                {"event": "claimed", "job_id": row["job_id"], "fence": fence},
                now=now,
            )
            claimed = connection.execute(
                "SELECT * FROM recovery_job WHERE job_id = ?",
                (row["job_id"],),
            ).fetchone()
            assert claimed is not None
            return self._job_from_row(claimed)

    def _checkpoint(
        self,
        *,
        job_id: str,
        fence: int,
        stage: str,
        payload: Mapping[str, JsonValue],
    ) -> RecoveryServiceJob:
        if not stage or len(stage) > 256:
            raise ValueError("checkpoint stage is invalid")
        content = canonical_json_bytes(dict(payload))
        if len(content) > _MAX_CHECKPOINT_BYTES:
            raise ValueError("checkpoint exceeds its byte budget")
        now = self.clock().astimezone(UTC)
        with self._transaction() as connection:
            changed = connection.execute(
                """
                UPDATE recovery_job
                SET checkpoint_stage = ?, checkpoint_json = ?, updated_at = ?
                WHERE job_id = ? AND status = 'running' AND fence = ?
                  AND lease_owner = ? AND lease_until >= ? AND deadline_at > ?
                """,
                (
                    stage,
                    content.decode("utf-8"),
                    _encode_time(now),
                    job_id,
                    fence,
                    self.worker_id,
                    _encode_time(now),
                    _encode_time(now),
                ),
            ).rowcount
            if changed != 1:
                raise RecoveryServiceLeaseLostError("recovery checkpoint lease or fence was lost")
            row = connection.execute(
                "SELECT * FROM recovery_job WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            assert row is not None
            return self._job_from_row(row)

    def _renew(self, *, job_id: str, fence: int) -> RecoveryServiceJob:
        now = self.clock().astimezone(UTC)
        lease_until = now + timedelta(seconds=self.lease_seconds)
        with self._transaction() as connection:
            changed = connection.execute(
                """
                UPDATE recovery_job SET lease_until = ?, updated_at = ?
                WHERE job_id = ? AND status = 'running' AND fence = ?
                  AND lease_owner = ? AND lease_until >= ? AND deadline_at > ?
                """,
                (
                    _encode_time(lease_until),
                    _encode_time(now),
                    job_id,
                    fence,
                    self.worker_id,
                    _encode_time(now),
                    _encode_time(now),
                ),
            ).rowcount
            if changed != 1:
                raise RecoveryServiceLeaseLostError("recovery renewal lease or fence was lost")
            row = connection.execute(
                "SELECT * FROM recovery_job WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            assert row is not None
            return self._job_from_row(row)

    def _assert_active_fence(
        self,
        *,
        job_id: str,
        fence: int,
        stage: str,
    ) -> RecoveryServiceJob:
        now = self.clock().astimezone(UTC)
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT * FROM recovery_job
                WHERE job_id = ? AND status = 'running' AND fence = ?
                  AND lease_owner = ? AND lease_until >= ? AND deadline_at > ?
                """,
                (
                    job_id,
                    fence,
                    self.worker_id,
                    _encode_time(now),
                    _encode_time(now),
                ),
            ).fetchone()
            if row is None:
                raise RecoveryServiceLeaseLostError(f"recovery {stage} lease or fence was lost")
            return self._job_from_row(row)

    def _write_service_receipt(self, receipt: RecoveryServiceReceipt) -> Path:
        assert receipt.receipt_id is not None
        payload = canonical_json_bytes(receipt.model_dump(mode="json"))
        path = self.receipt_root / f"{receipt.receipt_id}.json"
        if path.exists():
            try:
                existing = _read_canonical_contract(path, RecoveryServiceReceipt)
            except RecoveryServiceIntegrityError:
                self._quarantine_receipt_path(path)
            else:
                if canonical_json_bytes(existing.model_dump(mode="json")) == payload:
                    return path
                self._quarantine_receipt_path(path)
        temporary = self.receipt_root / f".{receipt.receipt_id}.{os.urandom(16).hex()}.tmp"
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            os.chmod(temporary, 0o400)
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()
        directory = os.open(self.receipt_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return path

    @staticmethod
    def _receipt_matches_index(
        row: sqlite3.Row,
        receipt: _RecoveryServiceReceiptBase,
        payload: bytes,
        *,
        relative_path: str,
    ) -> bool:
        return all(
            (
                receipt.receipt_id == row["receipt_id"],
                receipt.job_id == row["job_id"],
                receipt.fence == int(row["fence"]),
                receipt.status == row["status"],
                receipt.verification_level == row["verification_level"],
                receipt.recovery_receipt_id == row["recovery_receipt_id"],
                hashlib.sha256(payload).hexdigest() == row["content_sha256"],
                relative_path == row["relative_path"],
                _encode_time(receipt.completed_at) == row["created_at"],
            )
        )

    def _legacy_receipt_archive(self) -> Path:
        archive = self.receipt_root / ".legacy-v1"
        root_fd = -1
        archive_fd = -1
        try:
            root_fd = os.open(
                self.receipt_root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            _validate_directory_fd(
                root_fd,
                self.receipt_root,
                label="recovery receipt root",
                required_mode=0o700,
            )
            with suppress(FileExistsError):
                os.mkdir(".legacy-v1", mode=0o700, dir_fd=root_fd)
            archive_fd = os.open(
                ".legacy-v1",
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
            _validate_directory_fd(
                archive_fd,
                archive,
                label="legacy recovery receipt archive",
                required_mode=0o700,
            )
            _validate_directory_fd(
                root_fd,
                self.receipt_root,
                label="recovery receipt root",
                required_mode=0o700,
            )
        except OSError as exc:
            raise RecoveryServiceIntegrityError(
                "legacy recovery receipt archive is unsafe"
            ) from exc
        finally:
            if archive_fd >= 0:
                os.close(archive_fd)
            if root_fd >= 0:
                os.close(root_fd)
        return archive

    @contextmanager
    def _legacy_archive_handles(self) -> Iterator[tuple[int, int, Path]]:
        archive = self._legacy_receipt_archive()
        root_fd = -1
        archive_fd = -1
        try:
            root_fd = os.open(
                self.receipt_root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            root_before = _validate_directory_fd(
                root_fd,
                self.receipt_root,
                label="recovery receipt root",
                required_mode=0o700,
            )
            archive_fd = os.open(
                ".legacy-v1",
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
            archive_before = _validate_directory_fd(
                archive_fd,
                archive,
                label="legacy recovery receipt archive",
                required_mode=0o700,
            )
            yield root_fd, archive_fd, archive
            root_after = _validate_directory_fd(
                root_fd,
                self.receipt_root,
                label="recovery receipt root",
                required_mode=0o700,
            )
            archive_after = _validate_directory_fd(
                archive_fd,
                archive,
                label="legacy recovery receipt archive",
                required_mode=0o700,
            )
            if _directory_fd_identity(root_before) != _directory_fd_identity(
                root_after
            ) or _directory_fd_identity(archive_before) != _directory_fd_identity(archive_after):
                raise RecoveryServiceIntegrityError(
                    "legacy recovery receipt archive directory changed"
                )
        except OSError as exc:
            raise RecoveryServiceIntegrityError(
                "legacy recovery receipt archive is unsafe"
            ) from exc
        finally:
            if archive_fd >= 0:
                os.close(archive_fd)
            if root_fd >= 0:
                os.close(root_fd)

    def _load_v3_receipt(
        self,
        *,
        relative_path: str,
    ) -> tuple[LegacyRecoveryServiceReceipt, Path]:
        if Path(relative_path).name != relative_path or not relative_path.endswith(".json"):
            raise RecoveryServiceIntegrityError("legacy recovery receipt path is unsafe")
        source = self.receipt_root / relative_path
        archive = self._legacy_receipt_archive() / relative_path
        if source.exists():
            selected = source
        elif archive.exists():
            selected = archive
        else:
            raise RecoveryServiceIntegrityError("legacy recovery receipt is missing")
        try:
            return _read_canonical_contract(selected, LegacyRecoveryServiceReceipt), selected
        except RecoveryServiceIntegrityError as exc:
            raise RecoveryServiceIntegrityError("legacy recovery receipt is invalid") from exc

    def _archive_v3_receipt(
        self,
        *,
        source: Path,
        receipt: LegacyRecoveryServiceReceipt,
    ) -> tuple[Path, _MigrationArchiveProof]:
        assert receipt.receipt_id is not None
        filename = f"{receipt.receipt_id}.json"
        if source.name != filename:
            raise RecoveryServiceIntegrityError("legacy recovery receipt archive path is unsafe")
        destination = self._legacy_receipt_archive() / filename
        expected = canonical_json_bytes(receipt.model_dump(mode="json"))
        with self._legacy_archive_handles() as (root_fd, archive_fd, archive):
            if _path_exists_at(archive_fd, filename):
                try:
                    archived = _read_canonical_contract_at(
                        archive_fd,
                        archive,
                        filename,
                        LegacyRecoveryServiceReceipt,
                    )
                except RecoveryServiceIntegrityError as exc:
                    raise RecoveryServiceIntegrityError(
                        "archived legacy recovery receipt is invalid"
                    ) from exc
                if canonical_json_bytes(archived.model_dump(mode="json")) != expected:
                    raise RecoveryServiceIntegrityError("legacy recovery receipt archive conflicts")
                if source.parent == self.receipt_root and _path_exists_at(root_fd, filename):
                    source_receipt = _read_canonical_contract_at(
                        root_fd,
                        self.receipt_root,
                        filename,
                        LegacyRecoveryServiceReceipt,
                    )
                    if canonical_json_bytes(source_receipt.model_dump(mode="json")) != expected:
                        raise RecoveryServiceIntegrityError(
                            "legacy recovery receipt archive conflicts"
                        )
                    os.unlink(filename, dir_fd=root_fd)
                    os.fsync(root_fd)
                os.fsync(archive_fd)
                return destination, _archive_proof_at(archive_fd, filename)
            if source.parent != self.receipt_root:
                raise RecoveryServiceIntegrityError("legacy recovery receipt archive is missing")
            source_receipt = _read_canonical_contract_at(
                root_fd,
                self.receipt_root,
                filename,
                LegacyRecoveryServiceReceipt,
            )
            if canonical_json_bytes(source_receipt.model_dump(mode="json")) != expected:
                raise RecoveryServiceIntegrityError("legacy recovery receipt archive conflicts")
            os.rename(filename, filename, src_dir_fd=root_fd, dst_dir_fd=archive_fd)
            os.fsync(root_fd)
            os.fsync(archive_fd)
            archive_proof = _archive_proof_at(archive_fd, filename)
        return destination, archive_proof

    @staticmethod
    def _upgrade_v3_receipt(receipt: LegacyRecoveryServiceReceipt) -> RecoveryServiceReceipt:
        return RecoveryServiceReceipt(
            job_id=receipt.job_id,
            fence=receipt.fence,
            status=receipt.status,
            verification_level=receipt.verification_level,
            recovery_receipt_id=receipt.recovery_receipt_id,
            error_type=receipt.error_type,
            error_message=receipt.error_message,
            completed_at=receipt.completed_at,
        )

    @contextmanager
    def _migration_transaction(self, connection: sqlite3.Connection) -> Iterator[None]:
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    @staticmethod
    def _migration_intent_by_legacy_id(
        connection: sqlite3.Connection,
        legacy_receipt_id: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM recovery_receipt_migration WHERE legacy_receipt_id = ?",
            (legacy_receipt_id,),
        ).fetchone()

    @staticmethod
    def _migration_intent_by_upgraded_id(
        connection: sqlite3.Connection,
        upgraded_receipt_id: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM recovery_receipt_migration WHERE upgraded_receipt_id = ?",
            (upgraded_receipt_id,),
        ).fetchone()

    def _stage_v3_migration_intent(
        self,
        connection: sqlite3.Connection,
        *,
        legacy: LegacyRecoveryServiceReceipt,
        upgraded: RecoveryServiceReceipt,
        now: datetime,
    ) -> sqlite3.Row:
        assert legacy.receipt_id is not None
        assert upgraded.receipt_id is not None
        legacy_payload = canonical_json_bytes(legacy.model_dump(mode="json"))
        upgraded_payload = canonical_json_bytes(upgraded.model_dump(mode="json"))
        if len(legacy_payload) > _MAX_CONTRACT_BYTES or len(upgraded_payload) > _MAX_CONTRACT_BYTES:
            raise RecoveryServiceIntegrityError("recovery receipt migration payload is oversized")
        with self._migration_transaction(connection):
            with suppress(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO recovery_receipt_migration(
                        legacy_receipt_id, upgraded_receipt_id, job_id, fence,
                        legacy_payload_json, upgraded_payload_json,
                        legacy_content_sha256, upgraded_content_sha256,
                        legacy_relative_path, upgraded_relative_path, status,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'intent', ?, ?)
                    """,
                    (
                        legacy.receipt_id,
                        upgraded.receipt_id,
                        legacy.job_id,
                        legacy.fence,
                        legacy_payload.decode("utf-8"),
                        upgraded_payload.decode("utf-8"),
                        hashlib.sha256(legacy_payload).hexdigest(),
                        hashlib.sha256(upgraded_payload).hexdigest(),
                        f"{legacy.receipt_id}.json",
                        f"{upgraded.receipt_id}.json",
                        _encode_time(now),
                        _encode_time(now),
                    ),
                )
            row = self._migration_intent_by_legacy_id(connection, str(legacy.receipt_id))
            if row is None:
                row = connection.execute(
                    "SELECT * FROM recovery_receipt_migration WHERE job_id = ? AND fence = ?",
                    (legacy.job_id, legacy.fence),
                ).fetchone()
            if row is None:
                raise RecoveryServiceIntegrityError(
                    "recovery receipt migration intent was not persisted"
                )
            stored_legacy, stored_upgraded = self._migration_receipts_from_row(
                row,
                require_completed=False,
            )
            if stored_legacy != legacy or stored_upgraded != upgraded:
                raise RecoveryServiceIntegrityError("recovery receipt migration intent conflicts")
        refreshed = self._migration_intent_by_legacy_id(connection, str(legacy.receipt_id))
        if refreshed is None:
            raise RecoveryServiceIntegrityError("recovery receipt migration intent is missing")
        return refreshed

    @staticmethod
    def _advance_v3_migration_status(
        connection: sqlite3.Connection,
        *,
        legacy_receipt_id: str,
        status: str,
        now: datetime,
    ) -> None:
        if status not in _MIGRATION_STATUS_ORDER or status == "intent":
            raise RecoveryServiceIntegrityError("recovery receipt migration status is invalid")
        row = RuntimeRecoveryService._migration_intent_by_legacy_id(
            connection,
            legacy_receipt_id,
        )
        if row is None:
            raise RecoveryServiceIntegrityError("recovery receipt migration intent is missing")
        current_status = str(row["status"])
        if current_status not in _MIGRATION_STATUS_ORDER:
            raise RecoveryServiceIntegrityError("recovery receipt migration status is invalid")
        current_rank = _MIGRATION_STATUS_ORDER[current_status]
        target_rank = _MIGRATION_STATUS_ORDER[status]
        if current_rank > target_rank:
            return
        if current_rank == target_rank:
            return
        if target_rank != current_rank + 1:
            raise RecoveryServiceIntegrityError("recovery receipt migration skipped a state")
        column = _MIGRATION_STATUS_TIMESTAMPS[status]
        changed = connection.execute(
            f"""
            UPDATE recovery_receipt_migration
            SET status = ?, {column} = ?, updated_at = ?
            WHERE legacy_receipt_id = ? AND status = ?
            """,
            (
                status,
                _encode_time(now),
                _encode_time(now),
                legacy_receipt_id,
                current_status,
            ),
        ).rowcount
        if changed != 1:
            raise RecoveryServiceIntegrityError("recovery receipt migration state was lost")

    def _ensure_v3_migration_indexed(
        self,
        connection: sqlite3.Connection,
        *,
        legacy: LegacyRecoveryServiceReceipt,
        upgraded: RecoveryServiceReceipt,
    ) -> None:
        assert legacy.receipt_id is not None
        assert upgraded.receipt_id is not None
        self._verify_upgraded_migration_receipt_at(self.receipt_root, upgraded)
        legacy_payload = canonical_json_bytes(legacy.model_dump(mode="json"))
        upgraded_payload = canonical_json_bytes(upgraded.model_dump(mode="json"))
        row = connection.execute(
            "SELECT * FROM recovery_receipt WHERE job_id = ? AND fence = ?",
            (legacy.job_id, legacy.fence),
        ).fetchone()
        if row is None:
            raise RecoveryServiceIntegrityError("legacy recovery receipt index is missing")
        if row["receipt_id"] == upgraded.receipt_id:
            if not self._receipt_matches_index(
                row,
                upgraded,
                upgraded_payload,
                relative_path=f"{upgraded.receipt_id}.json",
            ):
                raise RecoveryServiceIntegrityError("recovery receipt index conflicts")
            return
        if row["receipt_id"] != legacy.receipt_id or not self._receipt_matches_index(
            row,
            legacy,
            legacy_payload,
            relative_path=f"{legacy.receipt_id}.json",
        ):
            raise RecoveryServiceIntegrityError("legacy recovery receipt index conflicts")
        changed = connection.execute(
            """
            UPDATE recovery_receipt
            SET receipt_id = ?, content_sha256 = ?, relative_path = ?, created_at = ?
            WHERE receipt_id = ? AND job_id = ? AND fence = ? AND status = ?
              AND verification_level IS ? AND recovery_receipt_id IS ?
              AND content_sha256 = ? AND relative_path = ? AND created_at = ?
            """,
            (
                upgraded.receipt_id,
                hashlib.sha256(upgraded_payload).hexdigest(),
                f"{upgraded.receipt_id}.json",
                _encode_time(upgraded.completed_at),
                legacy.receipt_id,
                legacy.job_id,
                legacy.fence,
                legacy.status,
                legacy.verification_level,
                legacy.recovery_receipt_id,
                hashlib.sha256(legacy_payload).hexdigest(),
                f"{legacy.receipt_id}.json",
                _encode_time(legacy.completed_at),
            ),
        ).rowcount
        if changed != 1:
            raise RecoveryServiceIntegrityError("legacy recovery receipt migration lost its fence")

    def _find_legacy_receipt_for_upgraded(
        self,
        upgraded: RecoveryServiceReceipt,
    ) -> tuple[LegacyRecoveryServiceReceipt, Path] | None:
        assert upgraded.receipt_id is not None
        archive = self._legacy_receipt_archive()
        matches: list[tuple[LegacyRecoveryServiceReceipt, Path]] = []
        observed = 0
        for directory in (archive, self.receipt_root):
            with os.scandir(directory) as entries:
                for entry in entries:
                    if entry.name in {".legacy-v1", ".quarantine"} or not entry.name.endswith(
                        ".json"
                    ):
                        continue
                    if entry.name == f"{upgraded.receipt_id}.json":
                        continue
                    observed += 1
                    if observed > 10_000:
                        raise RecoveryServiceIntegrityError(
                            "legacy recovery receipt inventory exceeds budget"
                        )
                    path = directory / entry.name
                    try:
                        legacy = _read_canonical_contract(path, LegacyRecoveryServiceReceipt)
                    except RecoveryServiceIntegrityError:
                        if directory == archive:
                            raise
                        continue
                    if self._upgrade_v3_receipt(legacy) == upgraded:
                        matches.append((legacy, path))
        if len(matches) > 1:
            raise RecoveryServiceIntegrityError("legacy recovery receipt migration is ambiguous")
        return None if not matches else matches[0]

    def _resume_v3_receipt_migration(
        self,
        connection: sqlite3.Connection,
        *,
        legacy_receipt_id: str,
    ) -> None:
        while True:
            row = self._migration_intent_by_legacy_id(connection, legacy_receipt_id)
            if row is None:
                raise RecoveryServiceIntegrityError("recovery receipt migration intent is missing")
            legacy, upgraded = self._migration_receipts_from_row(
                row,
                require_completed=False,
            )
            assert legacy.receipt_id is not None
            assert upgraded.receipt_id is not None
            status = str(row["status"])
            status_rank = _MIGRATION_STATUS_ORDER[status]
            if status_rank >= _MIGRATION_STATUS_ORDER["v2_published"]:
                self._verify_upgraded_migration_receipt_at(self.receipt_root, upgraded)
            if status_rank >= _MIGRATION_STATUS_ORDER["archived"]:
                self._verify_archived_migration_receipt_at(self.receipt_root, legacy)
            now = self.clock().astimezone(UTC)
            if status == "completed":
                return
            if status_rank < _MIGRATION_STATUS_ORDER["v2_published"]:
                path = self._write_service_receipt(upgraded)
                if path.name != f"{upgraded.receipt_id}.json":
                    raise RecoveryServiceIntegrityError("upgraded recovery receipt path differs")
                self._verify_upgraded_migration_receipt_at(self.receipt_root, upgraded)
                with self._migration_transaction(connection):
                    self._advance_v3_migration_status(
                        connection,
                        legacy_receipt_id=str(legacy.receipt_id),
                        status="v2_published",
                        now=now,
                    )
                continue
            if status_rank < _MIGRATION_STATUS_ORDER["indexed"]:
                with self._migration_transaction(connection):
                    self._ensure_v3_migration_indexed(
                        connection,
                        legacy=legacy,
                        upgraded=upgraded,
                    )
                    self._advance_v3_migration_status(
                        connection,
                        legacy_receipt_id=str(legacy.receipt_id),
                        status="indexed",
                        now=now,
                    )
                continue
            if status_rank < _MIGRATION_STATUS_ORDER["archived"]:
                archived_legacy, source = self._load_v3_receipt(
                    relative_path=f"{legacy.receipt_id}.json",
                )
                if archived_legacy != legacy:
                    raise RecoveryServiceIntegrityError("legacy recovery receipt intent conflicts")
                _archive_path, archive_proof = self._archive_v3_receipt(
                    source=source,
                    receipt=legacy,
                )
                self._verify_archived_migration_receipt_at(
                    self.receipt_root,
                    legacy,
                    expected_identity=archive_proof,
                )
                with self._migration_transaction(connection):
                    self._advance_v3_migration_status(
                        connection,
                        legacy_receipt_id=str(legacy.receipt_id),
                        status="archived",
                        now=now,
                    )
                continue
            if status_rank < _MIGRATION_STATUS_ORDER["audited"]:
                with self._migration_transaction(connection):
                    if not self._migration_audit_event_exists(
                        connection,
                        legacy_receipt_id=str(legacy.receipt_id),
                        upgraded_receipt_id=str(upgraded.receipt_id),
                    ):
                        self._append_audit(
                            connection,
                            self._migration_event_payload(
                                legacy_receipt_id=str(legacy.receipt_id),
                                upgraded_receipt_id=str(upgraded.receipt_id),
                            ),
                            now=now,
                        )
                    self._advance_v3_migration_status(
                        connection,
                        legacy_receipt_id=str(legacy.receipt_id),
                        status="audited",
                        now=now,
                    )
                continue
            if status_rank < _MIGRATION_STATUS_ORDER["completed"]:
                with self._migration_transaction(connection):
                    if not self._migration_audit_event_exists(
                        connection,
                        legacy_receipt_id=str(legacy.receipt_id),
                        upgraded_receipt_id=str(upgraded.receipt_id),
                    ):
                        raise RecoveryServiceIntegrityError(
                            "recovery receipt migration audit is missing"
                        )
                    self._advance_v3_migration_status(
                        connection,
                        legacy_receipt_id=str(legacy.receipt_id),
                        status="completed",
                        now=now,
                    )
                continue
            raise RecoveryServiceIntegrityError("recovery receipt migration status is invalid")

    def _migrate_v3_receipts(self, connection: sqlite3.Connection) -> None:
        """Resume durable v1→v2 receipt migrations until every intent is completed."""

        self._attest_migration_intents(connection, require_completed=False)
        rows = connection.execute(
            "SELECT * FROM recovery_receipt ORDER BY created_at, receipt_id LIMIT 10001"
        ).fetchall()
        if len(rows) > 10_000:
            raise RecoveryServiceIntegrityError("legacy recovery receipt inventory exceeds budget")
        for row in rows:
            relative_path = str(row["relative_path"])
            current_path = self.receipt_root / relative_path
            if current_path.exists():
                try:
                    current = _read_canonical_contract(current_path, RecoveryServiceReceipt)
                except RecoveryServiceIntegrityError:
                    current = None
                if current is not None:
                    payload = canonical_json_bytes(current.model_dump(mode="json"))
                    if not self._receipt_matches_index(
                        row,
                        current,
                        payload,
                        relative_path=relative_path,
                    ):
                        raise RecoveryServiceIntegrityError(
                            "recovery receipt index conflicts during migration"
                        )
                    intent = self._migration_intent_by_upgraded_id(
                        connection,
                        str(current.receipt_id),
                    )
                    if intent is not None:
                        self._resume_v3_receipt_migration(
                            connection,
                            legacy_receipt_id=str(intent["legacy_receipt_id"]),
                        )
                        continue
                    legacy_match = self._find_legacy_receipt_for_upgraded(current)
                    if legacy_match is not None:
                        legacy, _source = legacy_match
                        intent = self._stage_v3_migration_intent(
                            connection,
                            legacy=legacy,
                            upgraded=current,
                            now=self.clock().astimezone(UTC),
                        )
                        self._resume_v3_receipt_migration(
                            connection,
                            legacy_receipt_id=str(intent["legacy_receipt_id"]),
                        )
                    continue

            legacy, source = self._load_v3_receipt(relative_path=relative_path)
            legacy_payload = canonical_json_bytes(legacy.model_dump(mode="json"))
            if not self._receipt_matches_index(
                row,
                legacy,
                legacy_payload,
                relative_path=relative_path,
            ):
                raise RecoveryServiceIntegrityError("legacy recovery receipt index conflicts")
            upgraded = self._upgrade_v3_receipt(legacy)
            assert upgraded.receipt_id is not None
            if source.name != f"{legacy.receipt_id}.json":
                raise RecoveryServiceIntegrityError("legacy recovery receipt path is unsafe")
            intent = self._stage_v3_migration_intent(
                connection,
                legacy=legacy,
                upgraded=upgraded,
                now=self.clock().astimezone(UTC),
            )
            self._resume_v3_receipt_migration(
                connection,
                legacy_receipt_id=str(intent["legacy_receipt_id"]),
            )
        pending = connection.execute(
            """
            SELECT legacy_receipt_id FROM recovery_receipt_migration
            WHERE status != 'completed'
            ORDER BY created_at, legacy_receipt_id LIMIT 10001
            """
        ).fetchall()
        if len(pending) > 10_000:
            raise RecoveryServiceIntegrityError(
                "recovery receipt migration inventory exceeds budget"
            )
        for row in pending:
            self._resume_v3_receipt_migration(
                connection,
                legacy_receipt_id=str(row["legacy_receipt_id"]),
            )

    def _quarantine_receipt_path(self, path: Path) -> Path:
        if path.parent != self.receipt_root or path.name != Path(path.name).name:
            raise RecoveryServiceIntegrityError("receipt quarantine path is unsafe")
        quarantine = self.receipt_root / ".quarantine"
        quarantine.mkdir(mode=0o700, exist_ok=True)
        observed = os.lstat(quarantine)
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise RecoveryServiceIntegrityError("receipt quarantine directory is unsafe")
        destination = quarantine / f"{path.name}.{os.urandom(16).hex()}.orphan"
        os.replace(path, destination)
        for directory_path in (quarantine, self.receipt_root):
            descriptor = os.open(
                directory_path,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return destination

    @staticmethod
    def _stage_service_receipt(
        connection: sqlite3.Connection,
        receipt: RecoveryServiceReceipt,
    ) -> None:
        assert receipt.receipt_id is not None
        payload = canonical_json_bytes(receipt.model_dump(mode="json"))
        try:
            connection.execute(
                """
                INSERT INTO recovery_receipt_outbox(
                    receipt_id, job_id, fence, payload_json, content_sha256,
                    relative_path, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt.receipt_id,
                    receipt.job_id,
                    receipt.fence,
                    payload.decode("utf-8"),
                    hashlib.sha256(payload).hexdigest(),
                    f"{receipt.receipt_id}.json",
                    _encode_time(receipt.completed_at),
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise RecoveryServiceIntegrityError(
                "recovery receipt publication intent conflicts"
            ) from exc

    @staticmethod
    def _receipt_from_outbox_row(row: sqlite3.Row) -> RecoveryServiceReceipt:
        raw = str(row["payload_json"]).encode("utf-8")
        if len(raw) > _MAX_CONTRACT_BYTES:
            raise RecoveryServiceIntegrityError("recovery receipt outbox payload is oversized")
        try:
            decoded = strict_canonical_json_loads(raw)
            receipt = RecoveryServiceReceipt.model_validate(decoded)
        except Exception as exc:
            raise RecoveryServiceIntegrityError("recovery receipt outbox is invalid") from exc
        if (
            canonical_json_bytes(receipt.model_dump(mode="json")) != raw
            or receipt.receipt_id != row["receipt_id"]
            or receipt.job_id != row["job_id"]
            or receipt.fence != int(row["fence"])
            or hashlib.sha256(raw).hexdigest() != row["content_sha256"]
            or str(row["relative_path"]) != f"{receipt.receipt_id}.json"
            or _encode_time(receipt.completed_at) != row["created_at"]
        ):
            raise RecoveryServiceIntegrityError("recovery receipt outbox identity differs")
        return receipt

    def _quarantine_unindexed_receipts(self, known_paths: set[str]) -> None:
        observed_count = 0
        with os.scandir(self.receipt_root) as entries:
            for entry in entries:
                if entry.name in {".quarantine", ".legacy-v1"}:
                    continue
                observed_count += 1
                if observed_count > 10_000:
                    raise RecoveryServiceIntegrityError(
                        "physical recovery receipt inventory exceeds budget"
                    )
                if entry.name not in known_paths:
                    self._quarantine_receipt_path(self.receipt_root / entry.name)

    def reconcile_receipt_publications(self) -> int:
        """Publish committed receipt intents, then idempotently index and acknowledge them."""

        connection = self._connect()
        try:
            self._attest_state(connection, full_audit=False)
            indexed = connection.execute(
                "SELECT relative_path FROM recovery_receipt LIMIT 10001"
            ).fetchall()
            pending = connection.execute(
                """
                SELECT * FROM recovery_receipt_outbox
                ORDER BY created_at, receipt_id LIMIT 10001
                """
            ).fetchall()
        finally:
            connection.close()
        if len(indexed) > 10_000 or len(pending) > 10_000:
            raise RecoveryServiceIntegrityError(
                "recovery receipt publication inventory exceeds budget"
            )
        known_paths = {str(row["relative_path"]) for row in (*indexed, *pending)}
        self._quarantine_unindexed_receipts(known_paths)
        published = 0
        for pending_row in pending:
            receipt = self._receipt_from_outbox_row(pending_row)
            path = self._write_service_receipt(receipt)
            payload = canonical_json_bytes(receipt.model_dump(mode="json"))
            content_sha256 = hashlib.sha256(payload).hexdigest()
            relative_path = path.relative_to(self.receipt_root).as_posix()
            with self._transaction() as connection:
                current = connection.execute(
                    "SELECT * FROM recovery_receipt_outbox WHERE receipt_id = ?",
                    (receipt.receipt_id,),
                ).fetchone()
                if current is None:
                    continue
                if self._receipt_from_outbox_row(current) != receipt:
                    raise RecoveryServiceIntegrityError(
                        "recovery receipt publication intent changed"
                    )
                if _read_canonical_contract(path, RecoveryServiceReceipt) != receipt:
                    raise RecoveryServiceIntegrityError("published recovery receipt differs")
                connection.execute(
                    """
                    INSERT OR IGNORE INTO recovery_receipt(
                        receipt_id, job_id, fence, status, verification_level,
                        recovery_receipt_id, content_sha256, relative_path, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        receipt.receipt_id,
                        receipt.job_id,
                        receipt.fence,
                        receipt.status,
                        receipt.verification_level,
                        receipt.recovery_receipt_id,
                        content_sha256,
                        relative_path,
                        _encode_time(receipt.completed_at),
                    ),
                )
                indexed_row = connection.execute(
                    "SELECT * FROM recovery_receipt WHERE receipt_id = ?",
                    (receipt.receipt_id,),
                ).fetchone()
                if indexed_row is None or any(
                    (
                        indexed_row["job_id"] != receipt.job_id,
                        int(indexed_row["fence"]) != receipt.fence,
                        indexed_row["status"] != receipt.status,
                        indexed_row["verification_level"] != receipt.verification_level,
                        indexed_row["recovery_receipt_id"] != receipt.recovery_receipt_id,
                        indexed_row["content_sha256"] != content_sha256,
                        indexed_row["relative_path"] != relative_path,
                        indexed_row["created_at"] != _encode_time(receipt.completed_at),
                    )
                ):
                    raise RecoveryServiceIntegrityError("recovery receipt index conflicts")
                deleted = connection.execute(
                    """
                    DELETE FROM recovery_receipt_outbox
                    WHERE receipt_id = ? AND content_sha256 = ?
                    """,
                    (receipt.receipt_id, content_sha256),
                ).rowcount
                if deleted != 1:
                    raise RecoveryServiceIntegrityError(
                        "recovery receipt publication acknowledgement was lost"
                    )
            published += 1
        return published

    def _complete_success(
        self,
        job: RecoveryServiceJob,
        recovery: RealRecoveryReceipt,
    ) -> RecoveryServiceResult:
        if recovery.status != "succeeded":
            raise RecoveryServiceIntegrityError("executor returned a failed recovery receipt")
        now = self.clock().astimezone(UTC)
        receipt = RecoveryServiceReceipt(
            job_id=job.job_id,
            fence=job.fence,
            status="succeeded",
            verification_level="full",
            recovery_receipt_id=str(recovery.receipt_id),
            completed_at=now,
        )
        with self._transaction() as connection:
            active = connection.execute(
                """
                SELECT 1 FROM recovery_job
                WHERE job_id = ? AND status = 'running' AND fence = ?
                  AND lease_owner = ? AND lease_until >= ? AND deadline_at > ?
                """,
                (
                    job.job_id,
                    job.fence,
                    self.worker_id,
                    _encode_time(now),
                    _encode_time(now),
                ),
            ).fetchone()
            if active is None:
                raise RecoveryServiceLeaseLostError(
                    "recovery completion receipt lease or fence was lost"
                )
            changed = connection.execute(
                """
                UPDATE recovery_job
                SET status = 'succeeded', next_attempt_at = ?,
                    lease_owner = NULL, lease_until = NULL,
                    recovery_receipt_id = ?, updated_at = ?
                WHERE job_id = ? AND status = 'running' AND fence = ? AND lease_owner = ?
                  AND lease_until >= ? AND deadline_at > ?
                """,
                (
                    _encode_time(now),
                    recovery.receipt_id,
                    _encode_time(now),
                    job.job_id,
                    job.fence,
                    self.worker_id,
                    _encode_time(now),
                    _encode_time(now),
                ),
            ).rowcount
            if changed != 1:
                raise RecoveryServiceLeaseLostError("recovery completion fence was lost")
            self._stage_service_receipt(connection, receipt)
            self._append_audit(
                connection,
                {"event": "succeeded", "job_id": job.job_id, "fence": job.fence},
                now=now,
            )
        self.reconcile_receipt_publications()
        return RecoveryServiceResult(
            job_id=job.job_id,
            fence=job.fence,
            status="succeeded",
            recovery_receipt_id=str(recovery.receipt_id),
            service_receipt_id=str(receipt.receipt_id),
        )

    def _complete_failure(
        self,
        job: RecoveryServiceJob,
        error: Exception,
    ) -> RecoveryServiceResult:
        now = self.clock().astimezone(UTC)
        error_class = _recovery_error_class(error)
        retry = (
            error_class.startswith("transient_")
            and job.attempt_count < self.max_attempts
            and (now + timedelta(seconds=self.retry_delay_seconds) < job.deadline_at)
        )
        if retry:
            receipt = RecoveryServiceReceipt(
                job_id=job.job_id,
                fence=job.fence,
                status="retry_scheduled",
                error_type=type(error).__name__,
                error_message=str(error) or type(error).__name__,
                completed_at=now,
            )
            with self._transaction() as connection:
                changed = connection.execute(
                    """
                    UPDATE recovery_job
                    SET status = 'pending', next_attempt_at = ?,
                        lease_owner = NULL, lease_until = NULL,
                        last_error_type = ?, last_error_message = ?, updated_at = ?
                    WHERE job_id = ? AND status = 'running' AND fence = ? AND lease_owner = ?
                      AND lease_until >= ? AND deadline_at > ?
                    """,
                    (
                        _encode_time(now + timedelta(seconds=self.retry_delay_seconds)),
                        type(error).__name__,
                        str(error) or type(error).__name__,
                        _encode_time(now),
                        job.job_id,
                        job.fence,
                        self.worker_id,
                        _encode_time(now),
                        _encode_time(now),
                    ),
                ).rowcount
                if changed != 1:
                    raise RecoveryServiceLeaseLostError("recovery failure fence or lease was lost")
                self._stage_service_receipt(connection, receipt)
                self._append_audit(
                    connection,
                    {
                        "event": "retry_scheduled",
                        "job_id": job.job_id,
                        "fence": job.fence,
                        "error_class": error_class,
                        "error_type": type(error).__name__,
                    },
                    now=now,
                )
            self.reconcile_receipt_publications()
            return RecoveryServiceResult(
                job_id=job.job_id,
                fence=job.fence,
                status="retry_scheduled",
                service_receipt_id=str(receipt.receipt_id),
            )

        receipt = RecoveryServiceReceipt(
            job_id=job.job_id,
            fence=job.fence,
            status="failed",
            error_type=type(error).__name__,
            error_message=str(error) or type(error).__name__,
            completed_at=now,
        )
        with self._transaction() as connection:
            changed = connection.execute(
                """
                UPDATE recovery_job
                SET status = 'failed', lease_owner = NULL, lease_until = NULL,
                    last_error_type = ?, last_error_message = ?, updated_at = ?
                WHERE job_id = ? AND status = 'running' AND fence = ? AND lease_owner = ?
                  AND lease_until >= ? AND deadline_at > ?
                """,
                (
                    type(error).__name__,
                    str(error) or type(error).__name__,
                    _encode_time(now),
                    job.job_id,
                    job.fence,
                    self.worker_id,
                    _encode_time(now),
                    _encode_time(now),
                ),
            ).rowcount
            if changed != 1:
                raise RecoveryServiceLeaseLostError(
                    "recovery terminal failure fence or lease was lost"
                )
            self._stage_service_receipt(connection, receipt)
            self._append_audit(
                connection,
                {
                    "event": "failed",
                    "job_id": job.job_id,
                    "fence": job.fence,
                    "error_class": error_class,
                    "error_type": type(error).__name__,
                },
                now=now,
            )
        self.reconcile_receipt_publications()
        return RecoveryServiceResult(
            job_id=job.job_id,
            fence=job.fence,
            status="failed",
            service_receipt_id=str(receipt.receipt_id),
        )

    def run_once(
        self,
        execute: Callable[[RecoveryServiceLease], RealRecoveryReceipt],
    ) -> RecoveryServiceResult | None:
        job = self._claim()
        self.reconcile_receipt_publications()
        if job is None:
            return None
        lease = RecoveryServiceLease(self, job)
        try:
            recovery = execute(lease)
        except Exception as exc:
            return self._complete_failure(lease.job, exc)
        lease.assert_active("completion receipt")
        return self._complete_success(lease.job, recovery)

    def run_real_once(
        self,
        *,
        signature_verifier: RecoveryPayloadVerifier,
        fixed_replay_verifier: FixedReplayVerifier,
        max_artifacts: int = 4096,
        max_total_bytes: int = 256 * 1024**3,
    ) -> RecoveryServiceResult | None:
        """Claim one job and execute its exact signed real-artifact recovery."""

        def execute(lease: RecoveryServiceLease) -> RealRecoveryReceipt:
            stopped = threading.Event()
            heartbeat_errors: list[Exception] = []

            def heartbeat() -> None:
                interval = max(0.1, min(30.0, self.lease_seconds / 3))
                while not stopped.wait(interval):
                    try:
                        lease.renew()
                    except Exception as exc:
                        heartbeat_errors.append(exc)
                        stopped.set()

            thread = threading.Thread(
                target=heartbeat,
                name=f"rquant-recovery-heartbeat-{lease.job.job_id[:12]}",
                daemon=True,
            )
            thread.start()
            try:
                target = _read_canonical_contract(
                    Path(lease.job.manifest_path),
                    RealRecoveryTargetManifest,
                )
                tool = _read_canonical_contract(
                    Path(lease.job.tool_bundle_path),
                    RecoveryToolVerifierBundle,
                )
                lease.checkpoint(
                    "contracts-verified",
                    {
                        "manifest_id": str(target.manifest_id),
                        "tool_bundle_id": str(tool.bundle_id),
                    },
                )
                remaining = (
                    lease.job.deadline_at.astimezone(UTC) - self.clock().astimezone(UTC)
                ).total_seconds()
                if remaining <= 0:
                    raise RecoveryServiceIntegrityError("recovery job deadline expired")
                restorer = RealRecoveryRestorer(
                    backup_root=Path(lease.job.backup_root),
                    restore_root=Path(lease.job.restore_root),
                    signature_verifier=signature_verifier,
                    fixed_replay_verifier=fixed_replay_verifier,
                    max_artifacts=max_artifacts,
                    max_total_bytes=max_total_bytes,
                    deadline_seconds=remaining,
                    cancelled=lambda: bool(heartbeat_errors),
                )

                def publication_fence(stage: str) -> None:
                    if heartbeat_errors:
                        raise RecoveryServiceLeaseLostError(
                            "recovery heartbeat lost its fence"
                        ) from heartbeat_errors[0]
                    lease.checkpoint(
                        f"restore-{stage}",
                        {
                            "manifest_id": str(target.manifest_id),
                            "publication_stage": stage,
                        },
                    )

                receipt = restorer.restore(
                    target=target,
                    tool_bundle=tool,
                    publication_fence=publication_fence,
                )
                if heartbeat_errors:
                    raise RecoveryServiceIntegrityError("recovery heartbeat lost its fence") from (
                        heartbeat_errors[0]
                    )
                if (
                    receipt.manifest_id != target.manifest_id
                    or receipt.tool_bundle_id != tool.bundle_id
                    or receipt.target_profile_generation != target.target_profile_generation
                    or receipt.target_commit != target.target_commit
                ):
                    raise RecoveryServiceIntegrityError(
                        "real recovery receipt does not bind the claimed target"
                    )
                lease.checkpoint(
                    "recovery-published",
                    {"recovery_receipt_id": str(receipt.receipt_id)},
                )
                if heartbeat_errors:
                    raise RecoveryServiceIntegrityError("recovery heartbeat lost its fence") from (
                        heartbeat_errors[0]
                    )
                return receipt
            finally:
                stopped.set()
                thread.join(timeout=max(1.0, self.lease_seconds / 3 + 1))

        return self.run_once(execute)


def load_verified_recovery_service_receipts(
    *,
    state_path: Path,
    receipt_root: Path,
    job_id: str | None = None,
) -> tuple[RecoveryServiceReceipt, ...]:
    """Read service receipts without creating, migrating, or writing runtime state."""

    state = Path(_canonical_absolute(state_path, label="state_path"))
    root = Path(_canonical_absolute(receipt_root, label="receipt_root"))
    descriptor = -1
    connection: sqlite3.Connection | None = None
    try:
        descriptor = os.open(state, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        named = os.lstat(state)
        root_stat = os.lstat(root)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or stat.S_ISLNK(root_stat.st_mode)
            or not stat.S_ISDIR(root_stat.st_mode)
            or root_stat.st_uid != os.geteuid()
        ):
            raise RecoveryServiceIntegrityError("recovery service readonly paths are unsafe")
        uri = f"file:{quote(str(state), safe='/')}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        RuntimeRecoveryService._attest_schema(connection, full_audit=True)
        RuntimeRecoveryService._attest_migration_files_at(
            connection,
            receipt_root=root,
            require_completed=True,
        )
        if job_id is None:
            rows = connection.execute(
                "SELECT * FROM recovery_receipt ORDER BY created_at, receipt_id LIMIT 10001"
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT * FROM recovery_receipt WHERE job_id = ?
                ORDER BY created_at, receipt_id LIMIT 10001
                """,
                (job_id,),
            ).fetchall()
        after = os.fstat(descriptor)
        named_after = os.lstat(state)
        if _regular_stat_identity(opened) != _regular_stat_identity(after) or (
            after.st_dev,
            after.st_ino,
        ) != (named_after.st_dev, named_after.st_ino):
            raise RecoveryServiceIntegrityError("recovery service state changed while reading")
    except (OSError, sqlite3.Error) as exc:
        raise RecoveryServiceIntegrityError(
            "recovery service readonly state is unavailable"
        ) from exc
    finally:
        if connection is not None:
            connection.close()
        if descriptor >= 0:
            os.close(descriptor)
    if len(rows) > 10_000:
        raise RecoveryServiceIntegrityError("recovery receipt inventory exceeds budget")
    receipts: list[RecoveryServiceReceipt] = []
    for row in rows:
        relative = str(row["relative_path"])
        if Path(relative).name != relative or not relative.endswith(".json"):
            raise RecoveryServiceIntegrityError("recovery receipt path is unsafe")
        receipt = _read_canonical_contract(root / relative, RecoveryServiceReceipt)
        payload = canonical_json_bytes(receipt.model_dump(mode="json"))
        if (
            receipt.receipt_id != row["receipt_id"]
            or receipt.job_id != row["job_id"]
            or receipt.fence != int(row["fence"])
            or receipt.status != row["status"]
            or receipt.verification_level != row["verification_level"]
            or receipt.recovery_receipt_id != row["recovery_receipt_id"]
            or hashlib.sha256(payload).hexdigest() != row["content_sha256"]
            or _encode_time(receipt.completed_at) != row["created_at"]
        ):
            raise RecoveryServiceIntegrityError("immutable recovery receipt differs")
        receipts.append(receipt)
    return tuple(receipts)


# ---------------------------------------------------------------------------------------
# Runtime wrapper entry point
# ---------------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """The arguments the fixed root-owned runtime wrapper derives for the recovery roles.

    Every value comes from the two root-owned documents. `--mode` is the role's own frozen
    literal in `PRODUCTION_ROLE_POLICY`, which is what distinguishes `runtime_recovery` from
    `runtime_recovery_rehearsal`: both map to this module, and neither takes the choice from
    the caller.
    """

    parser = argparse.ArgumentParser(description="Run one rQuant runtime recovery pass")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--control-root", required=True, type=Path)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-generation", required=True)
    parser.add_argument("--mode", required=True, choices=("execute", "rehearse"))
    return parser


def runtime_root_for(control_root: Path) -> Path:
    """`<runtime root>/control/recovery/<instance>` -> `<runtime root>`.

    The control root is the profile's own root-owned prefix plus the authorised instance
    label, so this is a fixed arithmetic on a validated value, not a guess about a caller
    supplied path.
    """

    parents = control_root.parents
    if len(parents) < 3 or control_root.parent.name != "recovery":
        raise ValueError("recovery control root does not sit under a runtime control root")
    return parents[2]


def main(argv: Sequence[str] | None = None) -> int:
    """Consume the derived arguments, or exit non-zero.

    Before this existed the module had no entry point at all: `runpy.run_module` imported it
    and returned, so a oneshot recovery unit would have reported success without performing
    a single recovery step. Silence is the dangerous failure mode here, which is why an
    unusable argument set has to be a non-zero exit rather than a no-op.
    """

    # Not `rquant.cli`: importing it drags in the settings-reading modules, which a role
    # child started from a three-name environment cannot construct. The recovery body now
    # lives in a module that reads nothing at import time.
    from rquant.runtime_recovery_production import cmd_runtime_recovery_production

    arguments = build_parser().parse_args(argv)
    return int(
        cmd_runtime_recovery_production(
            argparse.Namespace(
                runtime_root=runtime_root_for(arguments.control_root),
                expected_profile_generation=arguments.expected_generation,
                production_recovery_action=arguments.mode,
            )
        )
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
