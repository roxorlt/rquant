"""Immutable, deterministic job-level artifact bundles for Strategy Lab."""

from __future__ import annotations

import base64
import ctypes
import errno
import fcntl
import hashlib
import io
import json
import math
import os
import re
import sqlite3
import stat
import sys
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Literal, Self
from uuid import UUID, uuid4
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.research_run_spec import DatasetSnapshotIdentity, ResearchRunSpec

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_CODE_SHA_PATTERN = r"^[0-9a-f]{40}$"
_TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)
_LEGACY_PROCESS_LOCKS_GUARD = threading.Lock()


@dataclass
class _LegacyProcessLockEntry:
    lock: threading.RLock
    references: int


_LEGACY_PROCESS_LOCKS: dict[str, _LegacyProcessLockEntry] = {}


class LabArtifactError(RuntimeError):
    """Base error for job artifact operations."""


class LabArtifactPathError(LabArtifactError):
    """An artifact path escaped its managed root or violated the path contract."""


class LabArtifactIntegrityError(LabArtifactError):
    """Artifact bytes, structure, identity, or permissions failed verification."""


class LabArtifactConflictError(LabArtifactError):
    """A deterministic artifact identity already contains different content."""


class LabArtifactAuthorizationError(LabArtifactError):
    """Export evidence does not authorize the selected sealed artifact."""


class LabArtifactPlatformError(LabArtifactError):
    """The host cannot provide a required fail-closed filesystem primitive."""


class LabLegacyArtifactConflictError(LabArtifactError):
    """A legacy logical run is already indexed with different source bytes."""


class LabArtifactModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
        strict=True,
    )

    def model_copy(
        self,
        *,
        update: Mapping[str, object] | None = None,
        deep: bool = False,
    ) -> Self:
        if not update:
            return super().model_copy(deep=deep)
        payload = self.model_dump(mode="python", round_trip=True)
        payload.update(update)
        return type(self).model_validate(payload)


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("canonical numeric values must be finite")
    sign, digits, exponent = value.as_tuple()
    if not any(digits):
        return "0"
    trimmed = list(digits)
    while trimmed and trimmed[-1] == 0:
        trimmed.pop()
        exponent += 1
    coefficient = "".join(str(digit) for digit in trimmed)
    if exponent >= 0:
        magnitude = coefficient + ("0" * exponent)
    else:
        point = len(coefficient) + exponent
        magnitude = (
            f"{coefficient[:point]}.{coefficient[point:]}"
            if point > 0
            else f"0.{'0' * -point}{coefficient}"
        )
    return f"{'-' if sign else ''}{magnitude}"


def _canonical_value(value: object) -> object:
    if isinstance(value, Enum):
        return _canonical_value(value.value)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical numeric values must be finite")
        return {"$float": value.hex()}
    if isinstance(value, Decimal):
        return {"$decimal": _canonical_decimal(value)}
    if isinstance(value, datetime):
        try:
            offset = value.utcoffset()
        except (OverflowError, ValueError) as exc:
            raise ValueError("canonical datetime is outside the UTC datetime range") from exc
        if value.tzinfo is None or offset is None:
            raise ValueError("canonical datetime values must be timezone-aware")
        try:
            normalized = value.astimezone(UTC)
        except (OverflowError, ValueError) as exc:
            raise ValueError("canonical datetime is outside the UTC datetime range") from exc
        return {"$datetime": normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, UUID):
        return {"$uuid": str(value)}
    if isinstance(value, Path):
        return {"$path": value.as_posix()}
    if isinstance(value, BaseModel):
        return _canonical_value(value.model_dump(mode="python", round_trip=True))
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical mappings require string keys")
        return {key: _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_json_bytes(value: object) -> bytes:
    """Encode supported values to stable, lossless canonical JSON bytes."""

    return json.dumps(
        _canonical_value(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _safe_relative_path(value: str) -> str:
    if not value or "\\" in value:
        raise ValueError("artifact relative path is invalid")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("artifact relative path is unsafe")
    if path.as_posix() != value:
        raise ValueError("artifact relative path is not canonical")
    return value


class LabParquetIdentity(LabArtifactModel):
    table_name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    row_count: int = Field(ge=0)
    columns: tuple[str, ...]
    dtypes: tuple[str, ...]
    content_sha256: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_shape(self) -> LabParquetIdentity:
        if len(self.columns) != len(self.dtypes):
            raise ValueError("Parquet columns and dtypes must have equal length")
        if len(self.columns) != len(set(self.columns)):
            raise ValueError("Parquet columns must be unique")
        return self


class LabJobArtifactFile(LabArtifactModel):
    relative_path: str
    media_type: str = Field(min_length=1)
    size: int = Field(ge=0)
    sha256: str = Field(pattern=_HASH_PATTERN)
    parquet: LabParquetIdentity | None = None

    @model_validator(mode="after")
    def validate_file_contract(self) -> LabJobArtifactFile:
        try:
            _safe_relative_path(self.relative_path)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        is_parquet = self.media_type == "application/vnd.apache.parquet"
        if is_parquet != (self.parquet is not None):
            raise ValueError("Parquet media type and metadata must appear together")
        if self.parquet is not None:
            expected = f"tables/{self.parquet.table_name}.parquet"
            if self.relative_path != expected:
                raise ValueError("Parquet path must match its table name")
        return self


def _complete_result_hash_payload(
    *,
    job_id: UUID,
    spec_hash: str,
    plan_hash: str,
    adapter_id: str,
    adapter_version: str,
    result_contract_version: str,
    code_sha: str,
    dataset_snapshot: DatasetSnapshotIdentity | None,
    files: tuple[LabJobArtifactFile, ...],
) -> dict[str, object]:
    return {
        "job_id": job_id,
        "spec_hash": spec_hash,
        "plan_hash": plan_hash,
        "adapter_id": adapter_id,
        "adapter_version": adapter_version,
        "result_contract_version": result_contract_version,
        "code_sha": code_sha,
        "dataset_snapshot": dataset_snapshot,
        "files": files,
    }


class LabJobArtifactManifest(LabArtifactModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    result_contract_version: str = Field(min_length=1)
    code_sha: str = Field(pattern=_CODE_SHA_PATTERN)
    dataset_snapshot: DatasetSnapshotIdentity | None
    files: tuple[LabJobArtifactFile, ...]
    complete_result_hash: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_manifest(self) -> LabJobArtifactManifest:
        paths = tuple(item.relative_path for item in self.files)
        if paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise ValueError("manifest file inventory must be sorted and unique")
        fixed_media_types = {
            "spec.json": "application/json",
            "metrics.json": "application/json",
            "report.md": "text/markdown; charset=utf-8",
        }
        fixed_entries = {
            item.relative_path: item
            for item in self.files
            if not item.relative_path.startswith("tables/")
        }
        if set(fixed_entries) != set(fixed_media_types):
            raise ValueError("manifest exact result inventory contains extra or missing files")
        for relative_path, media_type in fixed_media_types.items():
            entry = fixed_entries[relative_path]
            if entry.media_type != media_type or entry.parquet is not None:
                raise ValueError(f"manifest fixed file media type conflicts: {relative_path}")
        table_entries = tuple(
            item for item in self.files if item.relative_path.startswith("tables/")
        )
        if not table_entries:
            raise ValueError("manifest requires at least one complete Parquet table")
        if any(
            item.parquet is None
            or item.media_type != "application/vnd.apache.parquet"
            or item.relative_path != f"tables/{item.parquet.table_name}.parquet"
            for item in table_entries
        ):
            raise ValueError("manifest exact table inventory or media type conflicts")
        expected_hash = _sha256(
            canonical_json_bytes(
                _complete_result_hash_payload(
                    job_id=self.job_id,
                    spec_hash=self.spec_hash,
                    plan_hash=self.plan_hash,
                    adapter_id=self.adapter_id,
                    adapter_version=self.adapter_version,
                    result_contract_version=self.result_contract_version,
                    code_sha=self.code_sha,
                    dataset_snapshot=self.dataset_snapshot,
                    files=self.files,
                )
            )
        )
        if self.complete_result_hash != expected_hash:
            raise ValueError("complete_result_hash does not match manifest content")
        return self

    def canonical_json_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")

    @property
    def manifest_hash(self) -> str:
        return _sha256(self.canonical_json_bytes())


class LabArtifactFileIdentity(LabArtifactModel):
    relative_path: str
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    ctime_ns: int = Field(ge=0)


class LabJobArtifactCandidate(LabArtifactModel):
    path: Path
    job_id: UUID
    manifest: LabJobArtifactManifest
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    ctime_ns: int = Field(ge=0)
    file_identities: tuple[LabArtifactFileIdentity, ...]


class LabArtifactSealIntent(LabArtifactModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    candidate_name: str = Field(pattern=r"^[0-9a-f]{32}-[0-9a-f]{32}$")
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    complete_result_hash: str = Field(pattern=_HASH_PATTERN)
    bundle_device: int = Field(ge=0)
    bundle_inode: int = Field(ge=1)
    bundle_size: int = Field(ge=0)
    bundle_mtime_ns: int = Field(ge=0)
    bundle_ctime_ns: int = Field(ge=0)
    file_identities: tuple[LabArtifactFileIdentity, ...]

    @model_validator(mode="after")
    def validate_file_identities(self) -> LabArtifactSealIntent:
        paths = tuple(item.relative_path for item in self.file_identities)
        if paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise ValueError("seal intent file identities must be sorted and unique")
        return self

    def canonical_json_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")


class LabSealedJobArtifact(LabArtifactModel):
    path: Path
    manifest: LabJobArtifactManifest
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    file_identities: tuple[LabArtifactFileIdentity, ...]
    reused_existing: bool = False


class LabArtifactIndexEvidence(LabArtifactModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    sealed_path: Path
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    complete_result_hash: str = Field(pattern=_HASH_PATTERN)
    bundle_device: int = Field(ge=0)
    bundle_inode: int = Field(ge=1)
    file_identities: tuple[LabArtifactFileIdentity, ...]
    indexed_at: datetime

    @model_validator(mode="after")
    def validate_indexed_at(self) -> LabArtifactIndexEvidence:
        if self.indexed_at.tzinfo is None or self.indexed_at.utcoffset() is None:
            raise ValueError("indexed_at must be timezone-aware")
        return self


class LabVerifiedSealedBinding(LabArtifactModel):
    sealed: LabSealedJobArtifact
    evidence: LabArtifactIndexEvidence


class LabArtifactRecoveryAuthority(LabArtifactModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    result_contract_version: str = Field(min_length=1)
    code_sha: str = Field(pattern=_CODE_SHA_PATTERN)
    dataset_snapshot: DatasetSnapshotIdentity | None
    expected_manifest_hash: str = Field(pattern=_HASH_PATTERN)


class LabArtifactRecoveryRecord(LabArtifactModel):
    path: Path
    status: Literal["recoverable", "needs_authority", "invalid", "quarantined"]
    job_id: UUID | None = None
    manifest_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    device: int | None = Field(default=None, ge=0)
    inode: int | None = Field(default=None, ge=1)
    reason: str | None = None

    @model_validator(mode="after")
    def validate_path_identity(self) -> LabArtifactRecoveryRecord:
        if (self.device is None) != (self.inode is None):
            raise ValueError("recovery device and inode must appear together")
        return self


class LabLegacyArtifactRecord(LabArtifactModel):
    schema_version: Literal[1] = 1
    logical_run_id: str = Field(min_length=1)
    source_path: Path
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    sha256: str = Field(pattern=_HASH_PATTERN)
    media_type: Literal["application/json", "text/markdown; charset=utf-8"]
    imported_at: datetime

    @model_validator(mode="after")
    def validate_imported_at(self) -> LabLegacyArtifactRecord:
        if self.imported_at.tzinfo is None or self.imported_at.utcoffset() is None:
            raise ValueError("imported_at must be timezone-aware")
        return self


class LabLegacyIndexResult(LabArtifactModel):
    status: Literal["imported", "reused"]
    record: LabLegacyArtifactRecord


class LabLegacyAuthorityEvent(LabArtifactModel):
    schema_version: Literal[1] = 1
    event_type: Literal["staged", "published", "abandoned"]
    logical_run_id: str = Field(min_length=1)
    operation_id: UUID
    generation: int = Field(ge=1)
    record: LabLegacyArtifactRecord
    occurred_at: datetime

    @model_validator(mode="after")
    def validate_event(self) -> LabLegacyAuthorityEvent:
        if self.logical_run_id != self.record.logical_run_id:
            raise ValueError("legacy authority event logical run does not match record")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("legacy authority event time must be timezone-aware")
        return self

    def canonical_json_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")


class _FileObservation(LabArtifactModel):
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    mode: int = Field(ge=0)
    nlink: int = Field(ge=0)
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    ctime_ns: int = Field(ge=0)

    @classmethod
    def from_stat(cls, observed: os.stat_result) -> _FileObservation:
        return cls(
            device=observed.st_dev,
            inode=observed.st_ino,
            mode=stat.S_IFMT(observed.st_mode),
            nlink=observed.st_nlink,
            size=observed.st_size,
            mtime_ns=observed.st_mtime_ns,
            ctime_ns=observed.st_ctime_ns,
        )


@dataclass
class _BoundArtifactFile:
    relative_path: str
    descriptor: int
    parent_descriptor: int
    name: str
    original: _FileObservation
    current: _FileObservation


@dataclass
class _BoundArtifactBundle:
    parent_descriptor: int
    bundle_descriptor: int
    tables_descriptor: int
    bundle_name: str
    original: _FileObservation
    current: _FileObservation
    tables_original: _FileObservation
    tables_current: _FileObservation
    files: dict[str, _BoundArtifactFile]

    def close(self) -> None:
        for item in self.files.values():
            with suppress(OSError):
                os.close(item.descriptor)
        with suppress(OSError):
            os.close(self.tables_descriptor)
        with suppress(OSError):
            os.close(self.bundle_descriptor)
        with suppress(OSError):
            os.close(self.parent_descriptor)


@dataclass
class _BoundReadonlyFile:
    path: Path
    parent_descriptor: int
    descriptor: int
    parent_identity: _FileObservation
    file_identity: _FileObservation

    def close(self) -> None:
        with suppress(OSError):
            os.close(self.descriptor)
        with suppress(OSError):
            os.close(self.parent_descriptor)


@dataclass
class _BoundSealIntent:
    parent_descriptor: int
    descriptor: int
    name: str
    identity: _FileObservation
    intent: LabArtifactSealIntent

    def close(self) -> None:
        with suppress(OSError):
            os.close(self.descriptor)
        with suppress(OSError):
            os.close(self.parent_descriptor)


def _read_descriptor(descriptor: int) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    while chunk := os.read(descriptor, 1024 * 1024):
        chunks.append(chunk)
    return b"".join(chunks)


def _matches_file_identity(
    observed: _FileObservation,
    expected: LabArtifactFileIdentity,
    *,
    exact_ctime: bool,
) -> bool:
    stable = (
        observed.device,
        observed.inode,
        observed.size,
        observed.mtime_ns,
    ) == (
        expected.device,
        expected.inode,
        expected.size,
        expected.mtime_ns,
    )
    ctime_matches = (
        observed.ctime_ns == expected.ctime_ns
        if exact_ctime
        else observed.ctime_ns >= expected.ctime_ns
    )
    return stable and ctime_matches and observed.mode == stat.S_IFREG and observed.nlink == 1


def _secure_absolute_path(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _secure_open_directory(
    path: Path,
    *,
    create: bool,
    create_mode: int = 0o700,
) -> int:
    """Open an absolute directory without following any ancestor symlink."""

    absolute = _secure_absolute_path(path)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open("/", flags)
    try:
        for component in absolute.parts[1:]:
            if component in {"", ".", ".."}:
                raise LabArtifactPathError("managed path contains an unsafe component")
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                with suppress(FileExistsError):
                    os.mkdir(component, mode=create_mode, dir_fd=descriptor)
                child = os.open(component, flags, dir_fd=descriptor)
            observed = _FileObservation.from_stat(os.fstat(child))
            if observed.mode != stat.S_IFDIR:
                os.close(child)
                raise LabArtifactPathError("managed path component is not a directory")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except LabArtifactError:
        os.close(descriptor)
        raise
    except OSError as exc:
        os.close(descriptor)
        raise LabArtifactPathError(
            f"managed path ancestor is missing or unsafe: {absolute}"
        ) from exc


def _write_private_bytes_at(parent_descriptor: int, name: str, payload: bytes) -> None:
    if PurePosixPath(name).name != name or name in {"", ".", ".."}:
        raise LabArtifactPathError("artifact file name is unsafe")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, 0o600, dir_fd=parent_descriptor)
    try:
        opened = _FileObservation.from_stat(os.fstat(descriptor))
        if opened.mode != stat.S_IFREG or opened.nlink != 1:
            raise LabArtifactIntegrityError("artifact output is not a private regular file")
        os.fchmod(descriptor, 0o600)
        if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600:
            raise LabArtifactIntegrityError("artifact output permissions did not become 0600")
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise LabArtifactIntegrityError("artifact output write made no progress")
            offset += written
        os.fsync(descriptor)
        after = _FileObservation.from_stat(os.fstat(descriptor))
        at_path = _FileObservation.from_stat(
            os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        )
        if after != at_path or after.mode != stat.S_IFREG or after.nlink != 1:
            raise LabArtifactIntegrityError("artifact output identity changed while writing")
    except LabArtifactError:
        raise
    except OSError as exc:
        raise LabArtifactIntegrityError("artifact output could not be written safely") from exc
    finally:
        os.close(descriptor)


def _open_or_create_private_regular_at(
    parent_descriptor: int,
    name: str,
    *,
    access_flags: int,
) -> tuple[int, bool]:
    if PurePosixPath(name).name != name or name in {"", ".", ".."}:
        raise LabArtifactPathError("managed file name is unsafe")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    created = False
    try:
        descriptor = os.open(
            name,
            access_flags | os.O_CREAT | os.O_EXCL | nofollow,
            0o600,
            dir_fd=parent_descriptor,
        )
        created = True
    except FileExistsError:
        descriptor = os.open(name, access_flags | nofollow, dir_fd=parent_descriptor)
    try:
        observed = _FileObservation.from_stat(os.fstat(descriptor))
        at_path = _FileObservation.from_stat(
            os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        )
        if observed != at_path or observed.mode != stat.S_IFREG or observed.nlink != 1:
            raise LabArtifactIntegrityError("managed file is not a private regular file")
        os.fchmod(descriptor, 0o600)
        if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600:
            raise LabArtifactIntegrityError("managed file permissions did not become 0600")
        if created:
            os.fsync(descriptor)
            os.fsync(parent_descriptor)
        return descriptor, created
    except Exception:
        os.close(descriptor)
        raise


def _ensure_private_directory(path: Path, *, manage_existing: bool = True) -> None:
    existed = True
    try:
        descriptor = _secure_open_directory(path, create=False)
    except LabArtifactPathError as exc:
        if not isinstance(exc.__cause__, FileNotFoundError):
            raise
        existed = False
        descriptor = _secure_open_directory(path, create=True)
    try:
        if not existed or manage_existing:
            os.fchmod(descriptor, 0o700)
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _assert_bound_readonly_file(bound: _BoundReadonlyFile, *, label: str) -> None:
    current_parent_descriptor = -1
    try:
        parent_fd = _FileObservation.from_stat(os.fstat(bound.parent_descriptor))
        current_parent_descriptor = _secure_open_directory(bound.path.parent, create=False)
        parent_path = _FileObservation.from_stat(os.fstat(current_parent_descriptor))
        file_fd = _FileObservation.from_stat(os.fstat(bound.descriptor))
        file_path = _FileObservation.from_stat(
            os.stat(
                bound.path.name,
                dir_fd=bound.parent_descriptor,
                follow_symlinks=False,
            )
        )
    except OSError as exc:
        raise LabArtifactIntegrityError(f"{label} changed while bound") from exc
    finally:
        if current_parent_descriptor >= 0:
            os.close(current_parent_descriptor)
    if parent_fd != parent_path or (
        parent_fd.device,
        parent_fd.inode,
        parent_fd.mode,
    ) != (
        bound.parent_identity.device,
        bound.parent_identity.inode,
        stat.S_IFDIR,
    ):
        raise LabArtifactIntegrityError(f"{label} parent changed while bound")
    if file_fd != bound.file_identity or file_path != bound.file_identity:
        raise LabArtifactIntegrityError(f"{label} changed while bound")
    if file_fd.mode != stat.S_IFREG or file_fd.nlink != 1:
        raise LabArtifactIntegrityError(f"{label} is not a private regular file")


@contextmanager
def _open_bound_readonly_file(path: Path, *, label: str) -> Iterator[_BoundReadonlyFile]:
    parent_descriptor = -1
    descriptor = -1
    try:
        parent_descriptor = _secure_open_directory(path.parent, create=False)
        parent_identity = _FileObservation.from_stat(os.fstat(parent_descriptor))
        before = _FileObservation.from_stat(
            os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        )
        if before.mode != stat.S_IFREG or before.nlink != 1:
            raise LabArtifactIntegrityError(f"{label} is not a private regular file")
        descriptor = os.open(
            path.name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_descriptor,
        )
        opened = _FileObservation.from_stat(os.fstat(descriptor))
        if opened != before:
            raise LabArtifactIntegrityError(f"{label} changed while opening")
        bound = _BoundReadonlyFile(
            path=path,
            parent_descriptor=parent_descriptor,
            descriptor=descriptor,
            parent_identity=parent_identity,
            file_identity=opened,
        )
        _assert_bound_readonly_file(bound, label=label)
        yield bound
        _assert_bound_readonly_file(bound, label=label)
    except LabArtifactError:
        raise
    except OSError as exc:
        raise LabArtifactIntegrityError(f"{label} cannot be opened safely") from exc
    finally:
        if "bound" in locals():
            bound.close()
        else:
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
            if parent_descriptor >= 0:
                with suppress(OSError):
                    os.close(parent_descriptor)


def _rename_noreplace(
    source_parent: int,
    source_name: str,
    destination_parent: int,
    destination_name: str,
) -> None:
    """Atomically rename without replacement, or fail closed when unavailable."""

    library = ctypes.CDLL(None, use_errno=True)
    encoded_source = os.fsencode(source_name)
    encoded_destination = os.fsencode(destination_name)
    if sys.platform == "darwin":
        function = getattr(library, "renameatx_np", None)
        if function is None:
            raise LabArtifactPlatformError("renameatx_np(RENAME_EXCL) is unavailable")
        function.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        function.restype = ctypes.c_int
        result = function(
            source_parent,
            encoded_source,
            destination_parent,
            encoded_destination,
            0x00000004,
        )
    elif sys.platform.startswith("linux"):
        function = getattr(library, "renameat2", None)
        if function is None:
            raise LabArtifactPlatformError("renameat2(RENAME_NOREPLACE) is unavailable")
        function.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        function.restype = ctypes.c_int
        result = function(
            source_parent,
            encoded_source,
            destination_parent,
            encoded_destination,
            0x00000001,
        )
    else:
        raise LabArtifactPlatformError(
            f"atomic no-replace publication is unsupported on {sys.platform}"
        )
    if result != 0:
        error_number = ctypes.get_errno()
        if error_number in {errno.ENOSYS, errno.ENOTSUP, errno.EOPNOTSUPP, errno.EINVAL}:
            raise LabArtifactPlatformError(
                "the filesystem does not support atomic no-replace publication"
            )
        raise OSError(error_number, os.strerror(error_number))


def _canonical_table_value(value: object) -> object:
    if value is pd.NA or value is pd.NaT or value is None:
        return {"$null": True}
    if hasattr(value, "item") and not isinstance(value, (str, bytes, Decimal)):
        with suppress(ValueError, TypeError, AttributeError):
            value = value.item()  # type: ignore[union-attr]
    if isinstance(value, float):
        if math.isnan(value):
            return {"$null": True}
        if not math.isfinite(value):
            return {"$float": value.hex()}
        return {"$float": value.hex()}
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return {"$timestamp_ns": str(value.value)}
        return {"$timestamp_utc_ns": str(value.tz_convert(UTC).value)}
    if isinstance(value, pd.Timedelta):
        return {"$timedelta_ns": str(value.value)}
    if isinstance(value, bytes):
        return {"$bytes": base64.b64encode(value).decode("ascii")}
    return _canonical_value(value)


def _table_content_hash(frame: pd.DataFrame) -> str:
    payload = {
        "columns": list(frame.columns),
        "dtypes": [str(dtype) for dtype in frame.dtypes],
        "rows": [
            [_canonical_table_value(value) for value in row]
            for row in frame.itertuples(index=False, name=None)
        ],
    }
    return _sha256(canonical_json_bytes(payload))


def _parse_canonical_json(payload: bytes, *, label: str) -> object:
    try:
        text = payload.decode("utf-8")
        parsed = json.loads(
            text,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"invalid numeric constant: {value}")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise LabArtifactIntegrityError(f"{label} is not valid canonical JSON") from exc
    try:
        expected = canonical_json_bytes(parsed)
    except (TypeError, ValueError) as exc:
        raise LabArtifactIntegrityError(f"{label} is not valid canonical JSON") from exc
    if payload != expected:
        raise LabArtifactIntegrityError(f"{label} is not canonical JSON")
    return parsed


def _rebuild_canonical_value(value: object) -> object:
    if isinstance(value, list):
        return [_rebuild_canonical_value(item) for item in value]
    if not isinstance(value, dict):
        return value
    if set(value) == {"$decimal"} and isinstance(value["$decimal"], str):
        return Decimal(value["$decimal"])
    if set(value) == {"$datetime"} and isinstance(value["$datetime"], str):
        raw = value["$datetime"].replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(raw)
        except (OverflowError, ValueError) as exc:
            raise ValueError("canonical datetime cannot be rebuilt") from exc
    if set(value) == {"$date"} and isinstance(value["$date"], str):
        return date.fromisoformat(value["$date"])
    if set(value) == {"$uuid"} and isinstance(value["$uuid"], str):
        return UUID(value["$uuid"])
    if set(value) == {"$path"} and isinstance(value["$path"], str):
        return Path(value["$path"])
    if set(value) == {"$float"} and isinstance(value["$float"], str):
        rebuilt = float.fromhex(value["$float"])
        if not math.isfinite(rebuilt):
            raise ValueError("canonical numeric values must be finite")
        return rebuilt
    return {key: _rebuild_canonical_value(item) for key, item in value.items()}


_RESERVED_CANONICAL_TAGS = {
    "$date",
    "$datetime",
    "$decimal",
    "$float",
    "$path",
    "$uuid",
}


def _rebuild_metrics_value(value: object) -> object:
    if isinstance(value, list):
        return [_rebuild_metrics_value(item) for item in value]
    if not isinstance(value, dict):
        return value
    reserved = set(value) & _RESERVED_CANONICAL_TAGS
    if reserved:
        if len(value) != 1 or len(reserved) != 1:
            raise ValueError("metrics canonical tag must be the only mapping key")
        tag = next(iter(reserved))
        raw = value[tag]
        try:
            if tag == "$datetime" and isinstance(raw, str):
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if tag == "$date" and isinstance(raw, str):
                return date.fromisoformat(raw)
            if tag == "$decimal" and isinstance(raw, str):
                rebuilt_decimal = Decimal(raw)
                if not rebuilt_decimal.is_finite():
                    raise ValueError("metrics decimal must be finite")
                return rebuilt_decimal
            if tag == "$float" and isinstance(raw, str):
                rebuilt_float = float.fromhex(raw)
                if not math.isfinite(rebuilt_float):
                    raise ValueError("metrics float must be finite")
                return rebuilt_float
            if tag == "$uuid" and isinstance(raw, str):
                return UUID(raw)
            if tag == "$path" and isinstance(raw, str):
                return Path(raw)
        except (ArithmeticError, OverflowError, ValueError) as exc:
            raise ValueError(f"metrics canonical tag is invalid: {tag}") from exc
        raise ValueError(f"metrics canonical tag is invalid: {tag}")
    return {key: _rebuild_metrics_value(item) for key, item in value.items()}


def _validate_metrics_payload(payload: bytes) -> None:
    parsed = _parse_canonical_json(payload, label="metrics.json")
    try:
        rebuilt = _rebuild_metrics_value(parsed)
        rebuilt_bytes = canonical_json_bytes(rebuilt)
    except (TypeError, ValueError) as exc:
        raise LabArtifactIntegrityError("metrics.json contains an invalid canonical tag") from exc
    if rebuilt_bytes != payload:
        raise LabArtifactIntegrityError("metrics.json canonical tag round-trip conflicts")


def _rebuild_research_run_spec(payload: bytes) -> ResearchRunSpec:
    parsed = _parse_canonical_json(payload, label="spec.json")
    if not isinstance(parsed, dict):
        raise LabArtifactIntegrityError("spec.json must contain an object")
    try:
        spec = ResearchRunSpec.model_validate(_rebuild_canonical_value(parsed))
    except Exception as exc:
        raise LabArtifactIntegrityError(f"spec.json is not a valid ResearchRunSpec: {exc}") from exc
    if spec.canonical_json().encode("utf-8") != payload:
        raise LabArtifactIntegrityError(
            "spec.json bytes do not match rebuilt ResearchRunSpec canonical JSON"
        )
    return spec


class LabJobArtifactStore:
    """Create and verify complete job artifacts without touching scheduler state."""

    def __init__(self, root: Path) -> None:
        self.root = root.absolute()
        self.candidates_root = self.root / "candidates"
        self.sealed_root = self.root / "sealed"
        self.quarantine_root = self.root / "quarantine"
        self.seal_intents_root = self.root / "seal-intents"
        self.seal_intents_quarantine_root = self.root / "seal-intents-quarantine"
        for path in (
            self.root,
            self.candidates_root,
            self.sealed_root,
            self.quarantine_root,
            self.seal_intents_root,
            self.seal_intents_quarantine_root,
        ):
            _ensure_private_directory(path)
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        self._root_parent_descriptor = -1
        self._root_descriptor = -1
        self._managed_descriptors: dict[Path, int] = {}
        try:
            self._root_parent_descriptor = _secure_open_directory(
                self.root.parent,
                create=False,
            )
            self._root_descriptor = os.open(
                self.root.name,
                directory_flags,
                dir_fd=self._root_parent_descriptor,
            )
            for child in (
                self.candidates_root,
                self.sealed_root,
                self.quarantine_root,
                self.seal_intents_root,
                self.seal_intents_quarantine_root,
            ):
                self._managed_descriptors[child] = os.open(
                    child.name,
                    directory_flags,
                    dir_fd=self._root_descriptor,
                )
            self._root_parent_identity = _FileObservation.from_stat(
                os.fstat(self._root_parent_descriptor)
            )
            self._root_identity = _FileObservation.from_stat(os.fstat(self._root_descriptor))
            self._managed_identities = {
                path: _FileObservation.from_stat(os.fstat(descriptor))
                for path, descriptor in self._managed_descriptors.items()
            }
            self._assert_managed_roots()
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        for descriptor in getattr(self, "_managed_descriptors", {}).values():
            with suppress(OSError):
                os.close(descriptor)
        self._managed_descriptors = {}
        for attribute in ("_root_descriptor", "_root_parent_descriptor"):
            descriptor = getattr(self, attribute, -1)
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
                setattr(self, attribute, -1)

    def __del__(self) -> None:
        self.close()

    @staticmethod
    def _same_directory_identity(
        observed: _FileObservation,
        expected: _FileObservation,
    ) -> bool:
        return (
            observed.device,
            observed.inode,
            observed.mode,
        ) == (
            expected.device,
            expected.inode,
            stat.S_IFDIR,
        )

    def _assert_managed_roots(self) -> None:
        current_parent_descriptor = -1
        try:
            parent_fd = _FileObservation.from_stat(os.fstat(self._root_parent_descriptor))
            current_parent_descriptor = _secure_open_directory(self.root.parent, create=False)
            parent_path = _FileObservation.from_stat(os.fstat(current_parent_descriptor))
            root_fd = _FileObservation.from_stat(os.fstat(self._root_descriptor))
            root_entry = _FileObservation.from_stat(
                os.stat(
                    self.root.name,
                    dir_fd=self._root_parent_descriptor,
                    follow_symlinks=False,
                )
            )
            if not self._same_directory_identity(parent_fd, self._root_parent_identity):
                raise LabArtifactIntegrityError("managed root parent identity changed")
            if not self._same_directory_identity(parent_path, self._root_parent_identity):
                raise LabArtifactIntegrityError("managed root parent identity changed")
            if not self._same_directory_identity(root_fd, self._root_identity):
                raise LabArtifactIntegrityError("managed root identity changed")
            if not self._same_directory_identity(root_entry, self._root_identity):
                raise LabArtifactIntegrityError("managed root identity changed")
            for path, descriptor in self._managed_descriptors.items():
                expected = self._managed_identities[path]
                opened = _FileObservation.from_stat(os.fstat(descriptor))
                at_root = _FileObservation.from_stat(
                    os.stat(path.name, dir_fd=self._root_descriptor, follow_symlinks=False)
                )
                if not self._same_directory_identity(opened, expected) or not (
                    self._same_directory_identity(at_root, expected)
                ):
                    label = "candidate" if path == self.candidates_root else "managed artifact"
                    raise LabArtifactIntegrityError(
                        f"{label} directory identity changed: {path.name}"
                    )
        except LabArtifactError:
            raise
        except (AttributeError, OSError) as exc:
            raise LabArtifactIntegrityError("managed artifact root identity changed") from exc
        finally:
            if current_parent_descriptor >= 0:
                os.close(current_parent_descriptor)

    def _managed_parent_descriptor(self, parent_root: Path) -> int:
        self._assert_managed_roots()
        descriptor = self._managed_descriptors.get(parent_root.absolute())
        if descriptor is None:
            raise LabArtifactPathError("artifact parent is outside the managed root")
        return os.dup(descriptor)

    @staticmethod
    def _validate_table(table_name: str, frame: pd.DataFrame) -> None:
        if _TABLE_NAME.fullmatch(table_name) is None:
            raise LabArtifactPathError(f"unsafe table name: {table_name}")
        if any(not isinstance(column, str) for column in frame.columns):
            raise LabArtifactIntegrityError("artifact DataFrame columns must be strings")
        if len(frame.columns) != len(set(frame.columns)):
            raise LabArtifactIntegrityError("artifact DataFrame columns must be unique")
        if not frame.index.equals(pd.RangeIndex(start=0, stop=len(frame), step=1)):
            raise LabArtifactIntegrityError(
                "artifact DataFrame must use a default RangeIndex; persist index as a column"
            )

    @staticmethod
    def _serialize_parquet(
        table_name: str,
        frame: pd.DataFrame,
    ) -> tuple[bytes, LabJobArtifactFile]:
        output = io.BytesIO()
        frame.to_parquet(output, index=False)
        payload = output.getvalue()
        try:
            persisted = pd.read_parquet(io.BytesIO(payload))
        except Exception as exc:
            raise LabArtifactIntegrityError(
                f"candidate table cannot be read: {table_name}"
            ) from exc
        original_shape = (
            len(frame),
            tuple(frame.columns),
            tuple(str(item) for item in frame.dtypes),
        )
        persisted_shape = (
            len(persisted),
            tuple(persisted.columns),
            tuple(str(item) for item in persisted.dtypes),
        )
        if persisted_shape != original_shape:
            raise LabArtifactIntegrityError(
                f"Parquet round-trip changed rows, columns, or dtypes: {table_name}"
            )
        return (
            payload,
            LabJobArtifactFile(
                relative_path=f"tables/{table_name}.parquet",
                media_type="application/vnd.apache.parquet",
                size=len(payload),
                sha256=_sha256(payload),
                parquet=LabParquetIdentity(
                    table_name=table_name,
                    row_count=len(persisted),
                    columns=tuple(persisted.columns),
                    dtypes=tuple(str(item) for item in persisted.dtypes),
                    content_sha256=_table_content_hash(persisted),
                ),
            ),
        )

    @staticmethod
    def _after_candidate_directory_bound(_candidate_name: str, _descriptor: int) -> None:
        """Fault-injection boundary after the candidate directory is fd-bound."""

    def _assert_candidate_creation_binding(
        self,
        *,
        candidates_descriptor: int,
        candidate_name: str,
        candidate_descriptor: int,
        candidate_identity: _FileObservation,
        tables_descriptor: int | None = None,
        tables_identity: _FileObservation | None = None,
    ) -> None:
        try:
            self._assert_managed_roots()
            candidates_fd = _FileObservation.from_stat(os.fstat(candidates_descriptor))
            expected_candidates = self._managed_identities[self.candidates_root]
            if not self._same_directory_identity(candidates_fd, expected_candidates):
                raise LabArtifactIntegrityError("candidate parent identity changed")
            candidate_fd = _FileObservation.from_stat(os.fstat(candidate_descriptor))
            candidate_path = _FileObservation.from_stat(
                os.stat(
                    candidate_name,
                    dir_fd=candidates_descriptor,
                    follow_symlinks=False,
                )
            )
            if candidate_fd != candidate_path or (
                candidate_fd.device,
                candidate_fd.inode,
                candidate_fd.mode,
            ) != (
                candidate_identity.device,
                candidate_identity.inode,
                stat.S_IFDIR,
            ):
                raise LabArtifactIntegrityError("candidate directory identity changed")
            if tables_descriptor is not None:
                if tables_identity is None:
                    raise LabArtifactIntegrityError("candidate tables identity is unavailable")
                tables_fd = _FileObservation.from_stat(os.fstat(tables_descriptor))
                tables_path = _FileObservation.from_stat(
                    os.stat(
                        "tables",
                        dir_fd=candidate_descriptor,
                        follow_symlinks=False,
                    )
                )
                if tables_fd != tables_path or (
                    tables_fd.device,
                    tables_fd.inode,
                    tables_fd.mode,
                ) != (
                    tables_identity.device,
                    tables_identity.inode,
                    stat.S_IFDIR,
                ):
                    raise LabArtifactIntegrityError("candidate tables identity changed")
        except LabArtifactError:
            raise
        except OSError as exc:
            raise LabArtifactIntegrityError("candidate directory identity changed") from exc

    def prepare_candidate(
        self,
        *,
        job_id: UUID,
        spec: ResearchRunSpec,
        plan_hash: str,
        adapter_id: str,
        adapter_version: str,
        result_contract_version: str,
        metrics: Mapping[str, object],
        report_markdown: str,
        tables: Mapping[str, pd.DataFrame],
    ) -> LabJobArtifactCandidate:
        self._assert_managed_roots()
        if re.fullmatch(_HASH_PATTERN, plan_hash) is None:
            raise ValueError("plan_hash must be a lowercase SHA-256 hash")
        if not tables:
            raise ValueError("job artifact requires at least one complete table")
        if any(not isinstance(name, str) for name in tables):
            raise LabArtifactPathError("table names must be strings")
        for table_name, frame in tables.items():
            if not isinstance(frame, pd.DataFrame):
                raise TypeError("artifact tables must be pandas DataFrames")
            self._validate_table(table_name, frame)
        metrics_bytes = canonical_json_bytes(metrics)
        try:
            report_bytes = report_markdown.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise ValueError("report_markdown must be valid UTF-8 text") from exc
        parquet_payloads = {
            table_name: self._serialize_parquet(table_name, tables[table_name])
            for table_name in sorted(tables)
        }
        candidate_name = f"{job_id.hex}-{uuid4().hex}"
        candidate_path = self.candidates_root / candidate_name
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        candidates_descriptor = self._managed_parent_descriptor(self.candidates_root)
        candidate_descriptor = -1
        tables_descriptor = -1
        try:
            os.mkdir(candidate_name, mode=0o700, dir_fd=candidates_descriptor)
            candidate_descriptor = os.open(
                candidate_name,
                directory_flags,
                dir_fd=candidates_descriptor,
            )
            candidate_identity = _FileObservation.from_stat(os.fstat(candidate_descriptor))
            if candidate_identity.mode != stat.S_IFDIR:
                raise LabArtifactIntegrityError("candidate output is not a directory")
            os.fchmod(candidate_descriptor, 0o700)
            if stat.S_IMODE(os.fstat(candidate_descriptor).st_mode) != 0o700:
                raise LabArtifactIntegrityError("candidate permissions did not become 0700")
            self._after_candidate_directory_bound(candidate_name, candidate_descriptor)
            self._assert_candidate_creation_binding(
                candidates_descriptor=candidates_descriptor,
                candidate_name=candidate_name,
                candidate_descriptor=candidate_descriptor,
                candidate_identity=candidate_identity,
            )
            os.mkdir("tables", mode=0o700, dir_fd=candidate_descriptor)
            tables_descriptor = os.open("tables", directory_flags, dir_fd=candidate_descriptor)
            tables_identity = _FileObservation.from_stat(os.fstat(tables_descriptor))
            if tables_identity.mode != stat.S_IFDIR:
                raise LabArtifactIntegrityError("candidate tables output is not a directory")
            os.fchmod(tables_descriptor, 0o700)
            if stat.S_IMODE(os.fstat(tables_descriptor).st_mode) != 0o700:
                raise LabArtifactIntegrityError("candidate tables permissions did not become 0700")
            spec_bytes = spec.canonical_json().encode("utf-8")
            payloads: dict[str, tuple[str, bytes]] = {
                "spec.json": ("application/json", spec_bytes),
                "metrics.json": ("application/json", metrics_bytes),
                "report.md": ("text/markdown; charset=utf-8", report_bytes),
            }
            files: list[LabJobArtifactFile] = []
            for relative_path, (media_type, payload) in sorted(payloads.items()):
                self._assert_candidate_creation_binding(
                    candidates_descriptor=candidates_descriptor,
                    candidate_name=candidate_name,
                    candidate_descriptor=candidate_descriptor,
                    candidate_identity=candidate_identity,
                    tables_descriptor=tables_descriptor,
                    tables_identity=tables_identity,
                )
                _write_private_bytes_at(candidate_descriptor, relative_path, payload)
                files.append(
                    LabJobArtifactFile(
                        relative_path=relative_path,
                        media_type=media_type,
                        size=len(payload),
                        sha256=_sha256(payload),
                    )
                )
            for table_name, (payload, inventory) in parquet_payloads.items():
                self._assert_candidate_creation_binding(
                    candidates_descriptor=candidates_descriptor,
                    candidate_name=candidate_name,
                    candidate_descriptor=candidate_descriptor,
                    candidate_identity=candidate_identity,
                    tables_descriptor=tables_descriptor,
                    tables_identity=tables_identity,
                )
                _write_private_bytes_at(tables_descriptor, f"{table_name}.parquet", payload)
                files.append(inventory)
            ordered_files = tuple(sorted(files, key=lambda item: item.relative_path))
            identity = _complete_result_hash_payload(
                job_id=job_id,
                spec_hash=spec.spec_hash,
                plan_hash=plan_hash,
                adapter_id=adapter_id,
                adapter_version=adapter_version,
                result_contract_version=result_contract_version,
                code_sha=spec.code_sha,
                dataset_snapshot=spec.dataset_snapshot,
                files=ordered_files,
            )
            manifest = LabJobArtifactManifest(
                job_id=job_id,
                spec_hash=spec.spec_hash,
                plan_hash=plan_hash,
                adapter_id=adapter_id,
                adapter_version=adapter_version,
                result_contract_version=result_contract_version,
                code_sha=spec.code_sha,
                dataset_snapshot=spec.dataset_snapshot,
                files=ordered_files,
                complete_result_hash=_sha256(canonical_json_bytes(identity)),
            )
            manifest_bytes = manifest.canonical_json_bytes()
            self._assert_candidate_creation_binding(
                candidates_descriptor=candidates_descriptor,
                candidate_name=candidate_name,
                candidate_descriptor=candidate_descriptor,
                candidate_identity=candidate_identity,
                tables_descriptor=tables_descriptor,
                tables_identity=tables_identity,
            )
            _write_private_bytes_at(candidate_descriptor, "manifest.json", manifest_bytes)
            sums = {item.relative_path: item.sha256 for item in manifest.files}
            sums["manifest.json"] = _sha256(manifest_bytes)
            sums_bytes = "".join(
                f"{digest}  {relative_path}\n" for relative_path, digest in sorted(sums.items())
            ).encode("ascii")
            self._assert_candidate_creation_binding(
                candidates_descriptor=candidates_descriptor,
                candidate_name=candidate_name,
                candidate_descriptor=candidate_descriptor,
                candidate_identity=candidate_identity,
                tables_descriptor=tables_descriptor,
                tables_identity=tables_identity,
            )
            _write_private_bytes_at(candidate_descriptor, "SHA256SUMS", sums_bytes)
            self._assert_candidate_creation_binding(
                candidates_descriptor=candidates_descriptor,
                candidate_name=candidate_name,
                candidate_descriptor=candidate_descriptor,
                candidate_identity=candidate_identity,
                tables_descriptor=tables_descriptor,
                tables_identity=tables_identity,
            )
            os.fsync(tables_descriptor)
            os.fsync(candidate_descriptor)
            os.fsync(candidates_descriptor)
            os.close(tables_descriptor)
            tables_descriptor = -1
            os.close(candidate_descriptor)
            candidate_descriptor = -1
            os.close(candidates_descriptor)
            candidates_descriptor = -1
            candidate = self._candidate_from_path(candidate_path)
            self.verify_candidate(candidate)
            return candidate
        except Exception:
            # A failed candidate remains isolated for explicit operator recovery.
            for descriptor in (
                tables_descriptor,
                candidate_descriptor,
                candidates_descriptor,
            ):
                if descriptor >= 0:
                    with suppress(OSError):
                        os.fsync(descriptor)
            raise
        finally:
            for descriptor in (
                tables_descriptor,
                candidate_descriptor,
                candidates_descriptor,
            ):
                if descriptor >= 0:
                    with suppress(OSError):
                        os.close(descriptor)

    def _assert_managed_child(self, path: Path, parent: Path, *, label: str) -> Path:
        self._assert_managed_roots()
        absolute = path.absolute()
        if absolute.parent != parent or absolute.name in {"", ".", ".."}:
            raise LabArtifactPathError(f"{label} is outside its managed root")
        return absolute

    @staticmethod
    def _expected_paths(manifest: LabJobArtifactManifest) -> set[str]:
        return {
            "manifest.json",
            "SHA256SUMS",
            *(item.relative_path for item in manifest.files),
        }

    @staticmethod
    def _artifact_identity(
        relative_path: str,
        observed: _FileObservation,
    ) -> LabArtifactFileIdentity:
        return LabArtifactFileIdentity(
            relative_path=relative_path,
            device=observed.device,
            inode=observed.inode,
            size=observed.size,
            mtime_ns=observed.mtime_ns,
            ctime_ns=observed.ctime_ns,
        )

    def _probe_bundle(
        self,
        bundle: Path,
        *,
        parent_root: Path,
    ) -> tuple[
        _FileObservation,
        LabJobArtifactManifest,
        tuple[LabArtifactFileIdentity, ...],
    ]:
        parent_descriptor = self._managed_parent_descriptor(parent_root)
        bundle_descriptor = -1
        tables_descriptor = -1
        try:
            managed = self._assert_managed_child(bundle, parent_root, label="artifact bundle")
            directory_flags = (
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            )
            before = _FileObservation.from_stat(
                os.stat(managed.name, dir_fd=parent_descriptor, follow_symlinks=False)
            )
            bundle_descriptor = os.open(
                managed.name,
                directory_flags,
                dir_fd=parent_descriptor,
            )
            opened = _FileObservation.from_stat(os.fstat(bundle_descriptor))
            if opened != before or opened.mode != stat.S_IFDIR:
                raise LabArtifactIntegrityError("artifact bundle changed while probing")
            manifest_descriptor = os.open(
                "manifest.json",
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=bundle_descriptor,
            )
            try:
                manifest_observed = _FileObservation.from_stat(os.fstat(manifest_descriptor))
                if manifest_observed.mode != stat.S_IFREG or manifest_observed.nlink != 1:
                    raise LabArtifactIntegrityError("job artifact manifest is unsafe")
                manifest_bytes = _read_descriptor(manifest_descriptor)
            finally:
                os.close(manifest_descriptor)
            try:
                manifest = LabJobArtifactManifest.model_validate_json(manifest_bytes)
            except Exception as exc:
                raise LabArtifactIntegrityError(f"invalid job artifact manifest: {exc}") from exc
            if manifest_bytes != manifest.canonical_json_bytes():
                raise LabArtifactIntegrityError("job artifact manifest is not canonical JSON")
            expected = self._expected_paths(manifest)
            root_files = {
                path for path in expected if PurePosixPath(path).parent == PurePosixPath(".")
            }
            table_files = {
                PurePosixPath(path).name
                for path in expected
                if PurePosixPath(path).parent.as_posix() == "tables"
            }
            root_entries = set(os.listdir(bundle_descriptor))
            if root_entries != root_files | {"tables"}:
                raise LabArtifactIntegrityError("job artifact inventory mismatch")
            tables_descriptor = os.open("tables", directory_flags, dir_fd=bundle_descriptor)
            tables_observed = _FileObservation.from_stat(os.fstat(tables_descriptor))
            if tables_observed.mode != stat.S_IFDIR:
                raise LabArtifactIntegrityError("artifact tables entry is unsafe")
            if set(os.listdir(tables_descriptor)) != table_files:
                raise LabArtifactIntegrityError("job artifact table inventory mismatch")
            identities: list[LabArtifactFileIdentity] = []
            for relative_path in sorted(expected):
                pure = PurePosixPath(relative_path)
                parent_fd = (
                    tables_descriptor if pure.parent.as_posix() == "tables" else bundle_descriptor
                )
                observed = _FileObservation.from_stat(
                    os.stat(pure.name, dir_fd=parent_fd, follow_symlinks=False)
                )
                if observed.mode != stat.S_IFREG or observed.nlink != 1:
                    raise LabArtifactIntegrityError(f"artifact file is unsafe: {relative_path}")
                identities.append(self._artifact_identity(relative_path, observed))
            self._assert_managed_roots()
            return opened, manifest, tuple(identities)
        except LabArtifactError:
            raise
        except OSError as exc:
            raise LabArtifactIntegrityError("artifact bundle changed while probing") from exc
        finally:
            for descriptor in (tables_descriptor, bundle_descriptor, parent_descriptor):
                if descriptor >= 0:
                    with suppress(OSError):
                        os.close(descriptor)

    @staticmethod
    def _after_bound_file_read(
        _relative_path: str,
        _bound: _BoundArtifactBundle,
    ) -> None:
        """Fault-injection boundary while every bundle descriptor remains open."""

    def _validate_bound_bundle(
        self,
        bound: _BoundArtifactBundle,
        manifest: LabJobArtifactManifest,
        *,
        permission_profile: Literal["candidate", "interrupted", "sealed"],
    ) -> tuple[LabArtifactFileIdentity, ...]:
        self._assert_bound_paths(bound)
        payloads: dict[str, bytes] = {}
        identities: list[LabArtifactFileIdentity] = []
        expected_hashes = self._expected_bound_hashes(manifest)
        for relative_path in sorted(bound.files):
            item = bound.files[relative_path]
            observed = _FileObservation.from_stat(os.fstat(item.descriptor))
            payload = _read_descriptor(item.descriptor)
            self._after_bound_file_read(relative_path, bound)
            if len(payload) != observed.size or _sha256(payload) != expected_hashes[relative_path]:
                raise LabArtifactIntegrityError(f"job artifact bytes conflict: {relative_path}")
            payloads[relative_path] = payload
            identities.append(self._artifact_identity(relative_path, observed))
            mode = stat.S_IMODE(os.fstat(item.descriptor).st_mode)
            allowed = {0o400} if permission_profile == "sealed" else {0o600}
            if permission_profile == "interrupted":
                allowed = {0o400, 0o600}
            if mode not in allowed:
                raise LabArtifactIntegrityError(
                    f"{permission_profile} artifact file permissions conflict: {relative_path}"
                )
        directory_modes = (
            {0o500}
            if permission_profile == "sealed"
            else ({0o500, 0o700} if permission_profile == "interrupted" else {0o700})
        )
        if stat.S_IMODE(os.fstat(bound.bundle_descriptor).st_mode) not in directory_modes:
            raise LabArtifactIntegrityError(
                f"{permission_profile} artifact bundle permissions conflict"
            )
        if stat.S_IMODE(os.fstat(bound.tables_descriptor).st_mode) not in directory_modes:
            raise LabArtifactIntegrityError(
                f"{permission_profile} artifact tables permissions conflict"
            )
        if payloads["manifest.json"] != manifest.canonical_json_bytes():
            raise LabArtifactIntegrityError("job artifact manifest bytes conflict")
        rebuilt_spec = _rebuild_research_run_spec(payloads["spec.json"])
        if rebuilt_spec.spec_hash != manifest.spec_hash:
            raise LabArtifactIntegrityError("spec.json does not match spec_hash")
        if (
            rebuilt_spec.code_sha != manifest.code_sha
            or rebuilt_spec.dataset_snapshot != manifest.dataset_snapshot
        ):
            raise LabArtifactIntegrityError("manifest spec identity conflicts with spec.json")
        _validate_metrics_payload(payloads["metrics.json"])
        try:
            payloads["report.md"].decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise LabArtifactIntegrityError("report.md is not valid UTF-8") from exc
        for relative_path, entry in sorted(
            ((item.relative_path, item) for item in manifest.files),
        ):
            if entry.parquet is None:
                continue
            try:
                frame = pd.read_parquet(io.BytesIO(payloads[relative_path]))
            except Exception as exc:
                raise LabArtifactIntegrityError(
                    f"Parquet artifact cannot be read: {relative_path}"
                ) from exc
            actual = (
                len(frame),
                tuple(frame.columns),
                tuple(str(item) for item in frame.dtypes),
                _table_content_hash(frame),
            )
            expected = (
                entry.parquet.row_count,
                entry.parquet.columns,
                entry.parquet.dtypes,
                entry.parquet.content_sha256,
            )
            if actual != expected:
                raise LabArtifactIntegrityError(
                    f"Parquet artifact content conflicts: {relative_path}"
                )
        expected_sums = {item.relative_path: item.sha256 for item in manifest.files}
        expected_sums["manifest.json"] = manifest.manifest_hash
        sums = "".join(
            f"{digest}  {relative_path}\n"
            for relative_path, digest in sorted(expected_sums.items())
        ).encode("ascii")
        if payloads["SHA256SUMS"] != sums:
            raise LabArtifactIntegrityError("SHA256SUMS is not canonical or does not match")
        if manifest.complete_result_hash != _sha256(
            canonical_json_bytes(
                _complete_result_hash_payload(
                    job_id=manifest.job_id,
                    spec_hash=manifest.spec_hash,
                    plan_hash=manifest.plan_hash,
                    adapter_id=manifest.adapter_id,
                    adapter_version=manifest.adapter_version,
                    result_contract_version=manifest.result_contract_version,
                    code_sha=manifest.code_sha,
                    dataset_snapshot=manifest.dataset_snapshot,
                    files=manifest.files,
                )
            )
        ):
            raise LabArtifactIntegrityError("complete result hash conflicts")
        self._assert_bound_paths(bound)
        self._assert_managed_roots()
        return tuple(sorted(identities, key=lambda item: item.relative_path))

    def _validate_bundle(
        self,
        bundle: Path,
        *,
        parent_root: Path,
        permission_profile: Literal["candidate", "interrupted", "sealed"],
    ) -> tuple[
        LabJobArtifactManifest,
        tuple[LabArtifactFileIdentity, ...],
        _FileObservation,
    ]:
        observed, manifest, identities = self._probe_bundle(bundle, parent_root=parent_root)
        with self._bind_bundle(
            parent_root=parent_root,
            bundle_path=bundle,
            manifest=manifest,
            expected_bundle=observed,
            expected_files=identities,
        ) as bound:
            verified_identities = self._validate_bound_bundle(
                bound,
                manifest,
                permission_profile=permission_profile,
            )
            return manifest, verified_identities, bound.current

    @staticmethod
    def _same_bundle_identity(
        observed: _FileObservation,
        candidate: LabJobArtifactCandidate,
    ) -> bool:
        return (
            observed.device,
            observed.inode,
            observed.size,
            observed.mtime_ns,
            observed.ctime_ns,
            observed.mode,
        ) == (
            candidate.device,
            candidate.inode,
            candidate.size,
            candidate.mtime_ns,
            candidate.ctime_ns,
            stat.S_IFDIR,
        )

    @contextmanager
    def _bind_bundle(
        self,
        *,
        parent_root: Path,
        bundle_path: Path,
        manifest: LabJobArtifactManifest,
        expected_bundle: _FileObservation,
        expected_files: tuple[LabArtifactFileIdentity, ...],
    ) -> Iterator[_BoundArtifactBundle]:
        parent_descriptor = -1
        bundle_descriptor = -1
        tables_descriptor = -1
        opened_files: dict[str, _BoundArtifactFile] = {}
        try:
            managed = self._assert_managed_child(
                bundle_path,
                parent_root,
                label="bound artifact bundle",
            )
            directory_flags = (
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            )
            parent_descriptor = self._managed_parent_descriptor(parent_root)
            before = _FileObservation.from_stat(
                os.stat(managed.name, dir_fd=parent_descriptor, follow_symlinks=False)
            )
            if before != expected_bundle:
                raise LabArtifactIntegrityError("bound artifact bundle identity changed")
            bundle_descriptor = os.open(
                managed.name,
                directory_flags,
                dir_fd=parent_descriptor,
            )
            opened = _FileObservation.from_stat(os.fstat(bundle_descriptor))
            if opened != before or opened.mode != stat.S_IFDIR:
                raise LabArtifactIntegrityError("bound artifact bundle changed while opening")
            tables_before = _FileObservation.from_stat(
                os.stat("tables", dir_fd=bundle_descriptor, follow_symlinks=False)
            )
            tables_descriptor = os.open(
                "tables",
                directory_flags,
                dir_fd=bundle_descriptor,
            )
            tables_opened = _FileObservation.from_stat(os.fstat(tables_descriptor))
            if tables_opened != tables_before or tables_opened.mode != stat.S_IFDIR:
                raise LabArtifactIntegrityError("bound tables directory changed while opening")
            expected_by_path = {item.relative_path: item for item in expected_files}
            if set(expected_by_path) != self._expected_paths(manifest):
                raise LabArtifactIntegrityError("bound artifact file inventory changed")
            for relative_path in sorted(expected_by_path):
                pure = PurePosixPath(relative_path)
                parent_fd = (
                    tables_descriptor if pure.parent.as_posix() == "tables" else bundle_descriptor
                )
                name = pure.name
                file_before = _FileObservation.from_stat(
                    os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                )
                if not _matches_file_identity(
                    file_before,
                    expected_by_path[relative_path],
                    exact_ctime=True,
                ):
                    raise LabArtifactIntegrityError(
                        f"bound artifact file identity changed: {relative_path}"
                    )
                descriptor = os.open(
                    name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=parent_fd,
                )
                file_opened = _FileObservation.from_stat(os.fstat(descriptor))
                if file_opened != file_before:
                    os.close(descriptor)
                    raise LabArtifactIntegrityError(
                        f"bound artifact file changed while opening: {relative_path}"
                    )
                opened_files[relative_path] = _BoundArtifactFile(
                    relative_path=relative_path,
                    descriptor=descriptor,
                    parent_descriptor=parent_fd,
                    name=name,
                    original=file_opened,
                    current=file_opened,
                )
            bound = _BoundArtifactBundle(
                parent_descriptor=parent_descriptor,
                bundle_descriptor=bundle_descriptor,
                tables_descriptor=tables_descriptor,
                bundle_name=managed.name,
                original=opened,
                current=opened,
                tables_original=tables_opened,
                tables_current=tables_opened,
                files=opened_files,
            )
            self._assert_bound_paths(bound)
            yield bound
        except LabArtifactIntegrityError:
            raise
        except OSError as exc:
            if "bound" in locals():
                raise
            raise LabArtifactIntegrityError(
                "artifact changed while binding file descriptors"
            ) from exc
        finally:
            if "bound" in locals():
                bound.close()
            else:
                for item in opened_files.values():
                    with suppress(OSError):
                        os.close(item.descriptor)
                for descriptor in (tables_descriptor, bundle_descriptor, parent_descriptor):
                    if descriptor >= 0:
                        with suppress(OSError):
                            os.close(descriptor)

    @staticmethod
    def _assert_bound_paths(bound: _BoundArtifactBundle) -> None:
        try:
            bundle_fd = _FileObservation.from_stat(os.fstat(bound.bundle_descriptor))
            bundle_path = _FileObservation.from_stat(
                os.stat(
                    bound.bundle_name,
                    dir_fd=bound.parent_descriptor,
                    follow_symlinks=False,
                )
            )
            if bundle_fd != bound.current or bundle_path != bound.current:
                raise LabArtifactIntegrityError("bound artifact bundle identity changed")
            tables_fd = _FileObservation.from_stat(os.fstat(bound.tables_descriptor))
            tables_path = _FileObservation.from_stat(
                os.stat("tables", dir_fd=bound.bundle_descriptor, follow_symlinks=False)
            )
            if tables_fd != bound.tables_current or tables_path != bound.tables_current:
                raise LabArtifactIntegrityError("bound tables directory identity changed")
            root_files = {
                item.name
                for item in bound.files.values()
                if item.parent_descriptor == bound.bundle_descriptor
            }
            table_files = {
                item.name
                for item in bound.files.values()
                if item.parent_descriptor == bound.tables_descriptor
            }
            if set(os.listdir(bound.bundle_descriptor)) != root_files | {"tables"}:
                raise LabArtifactIntegrityError("bound artifact inventory changed")
            if set(os.listdir(bound.tables_descriptor)) != table_files:
                raise LabArtifactIntegrityError("bound table inventory changed")
            for item in bound.files.values():
                opened = _FileObservation.from_stat(os.fstat(item.descriptor))
                at_path = _FileObservation.from_stat(
                    os.stat(
                        item.name,
                        dir_fd=item.parent_descriptor,
                        follow_symlinks=False,
                    )
                )
                if opened != item.current or at_path != item.current:
                    raise LabArtifactIntegrityError(
                        f"bound artifact file identity changed: {item.relative_path}"
                    )
        except LabArtifactIntegrityError:
            raise
        except OSError as exc:
            raise LabArtifactIntegrityError("bound artifact path identity changed") from exc

    @staticmethod
    def _expected_bound_hashes(manifest: LabJobArtifactManifest) -> dict[str, str]:
        expected = {item.relative_path: item.sha256 for item in manifest.files}
        expected["manifest.json"] = manifest.manifest_hash
        sums = "".join(
            f"{digest}  {relative_path}\n" for relative_path, digest in sorted(expected.items())
        ).encode("ascii")
        expected["SHA256SUMS"] = _sha256(sums)
        return expected

    def _verify_bound_bytes(
        self,
        bound: _BoundArtifactBundle,
        manifest: LabJobArtifactManifest,
    ) -> None:
        expected = self._expected_bound_hashes(manifest)
        for relative_path, item in bound.files.items():
            payload = _read_descriptor(item.descriptor)
            if len(payload) != item.current.size or _sha256(payload) != expected[relative_path]:
                raise LabArtifactIntegrityError(f"bound artifact bytes changed: {relative_path}")
        if _read_descriptor(bound.files["manifest.json"].descriptor) != (
            manifest.canonical_json_bytes()
        ):
            raise LabArtifactIntegrityError("bound candidate manifest changed")
        self._assert_bound_paths(bound)

    def _seal_intent_path(self, job_id: UUID) -> Path:
        return self.seal_intents_root / f"{job_id.hex}.json"

    def _seal_intent_state(self, job_id: UUID) -> Literal["missing", "valid", "torn"]:
        descriptor = self._managed_parent_descriptor(self.seal_intents_root)
        file_descriptor = -1
        try:
            try:
                observed = _FileObservation.from_stat(
                    os.stat(
                        f"{job_id.hex}.json",
                        dir_fd=descriptor,
                        follow_symlinks=False,
                    )
                )
            except FileNotFoundError:
                return "missing"
            if (
                observed.mode != stat.S_IFREG
                or observed.nlink != 1
                or stat.S_IMODE(
                    os.stat(
                        f"{job_id.hex}.json",
                        dir_fd=descriptor,
                        follow_symlinks=False,
                    ).st_mode
                )
                != 0o600
            ):
                raise LabArtifactIntegrityError("job artifact seal intent is unsafe")
            file_descriptor = os.open(
                f"{job_id.hex}.json",
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptor,
            )
            opened = _FileObservation.from_stat(os.fstat(file_descriptor))
            if opened != observed:
                raise LabArtifactIntegrityError("job artifact seal intent identity changed")
            payload = _read_descriptor(file_descriptor)
            try:
                intent = LabArtifactSealIntent.model_validate_json(payload)
            except Exception:
                return "torn"
            if payload != intent.canonical_json_bytes() or intent.job_id != job_id:
                return "torn"
            return "valid"
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)
            os.close(descriptor)

    def _seal_intent_exists(self, job_id: UUID) -> bool:
        return self._seal_intent_state(job_id) == "valid"

    def _quarantine_seal_intent_entry(self, name: str) -> None:
        source_parent = self._managed_parent_descriptor(self.seal_intents_root)
        target_parent = self._managed_parent_descriptor(self.seal_intents_quarantine_root)
        descriptor = -1
        try:
            before = _FileObservation.from_stat(
                os.stat(name, dir_fd=source_parent, follow_symlinks=False)
            )
            if before.mode != stat.S_IFREG or before.nlink != 1:
                raise LabArtifactIntegrityError("seal intent recovery entry is unsafe")
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=source_parent,
            )
            opened = _FileObservation.from_stat(os.fstat(descriptor))
            if opened != before:
                raise LabArtifactIntegrityError("seal intent recovery entry identity changed")
            target_name = f"{name.lstrip('.')}.{uuid4().hex}.quarantined"
            _rename_noreplace(source_parent, name, target_parent, target_name)
            target = _FileObservation.from_stat(
                os.stat(target_name, dir_fd=target_parent, follow_symlinks=False)
            )
            still_open = _FileObservation.from_stat(os.fstat(descriptor))
            stable_before = (
                opened.device,
                opened.inode,
                opened.mode,
                opened.nlink,
                opened.size,
                opened.mtime_ns,
            )
            stable_after = (
                still_open.device,
                still_open.inode,
                still_open.mode,
                still_open.nlink,
                still_open.size,
                still_open.mtime_ns,
            )
            if target != still_open or stable_after != stable_before:
                raise LabArtifactIntegrityError("seal intent quarantine identity changed")
            os.fsync(source_parent)
            os.fsync(target_parent)
            self._assert_managed_roots()
        except LabArtifactError:
            raise
        except OSError as exc:
            raise LabArtifactIntegrityError("seal intent could not be quarantined") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(source_parent)
            os.close(target_parent)

    def _quarantine_orphaned_seal_intent_temps(self, job_id: UUID) -> None:
        parent = self._managed_parent_descriptor(self.seal_intents_root)
        try:
            prefix = f".{job_id.hex}."
            names = sorted(
                name
                for name in os.listdir(parent)
                if name.startswith(prefix) and name.endswith(".intent.tmp")
            )
        finally:
            os.close(parent)
        for name in names:
            self._quarantine_seal_intent_entry(name)

    @staticmethod
    def _candidate_seal_intent(
        candidate: LabJobArtifactCandidate,
    ) -> LabArtifactSealIntent:
        return LabArtifactSealIntent(
            job_id=candidate.job_id,
            candidate_name=candidate.path.name,
            manifest_hash=candidate.manifest_hash,
            complete_result_hash=candidate.manifest.complete_result_hash,
            bundle_device=candidate.device,
            bundle_inode=candidate.inode,
            bundle_size=candidate.size,
            bundle_mtime_ns=candidate.mtime_ns,
            bundle_ctime_ns=candidate.ctime_ns,
            file_identities=candidate.file_identities,
        )

    @staticmethod
    def _after_seal_intent_bound(_bound: _BoundSealIntent) -> None:
        """Fault-injection boundary while the seal intent fd remains open."""

    @staticmethod
    def _after_seal_intent_temp_fsync(_descriptor: int, _name: str) -> None:
        """Fault-injection boundary after a complete intent temp is durable."""

    @staticmethod
    def _after_seal_intent_publish(_bound: _BoundSealIntent) -> None:
        """Fault-injection boundary after no-replace intent publication."""

    def _assert_bound_seal_intent(self, bound: _BoundSealIntent) -> None:
        try:
            opened = _FileObservation.from_stat(os.fstat(bound.descriptor))
            at_path = _FileObservation.from_stat(
                os.stat(
                    bound.name,
                    dir_fd=bound.parent_descriptor,
                    follow_symlinks=False,
                )
            )
            payload = _read_descriptor(bound.descriptor)
            if opened != bound.identity or at_path != bound.identity:
                raise LabArtifactIntegrityError("job artifact seal intent identity changed")
            if opened.mode != stat.S_IFREG or opened.nlink != 1:
                raise LabArtifactIntegrityError("job artifact seal intent is unsafe")
            if stat.S_IMODE(os.fstat(bound.descriptor).st_mode) != 0o600:
                raise LabArtifactIntegrityError("job artifact seal intent permissions conflict")
            if payload != bound.intent.canonical_json_bytes():
                raise LabArtifactIntegrityError("job artifact seal intent bytes changed")
            self._assert_managed_roots()
        except LabArtifactError:
            raise
        except OSError as exc:
            raise LabArtifactIntegrityError("job artifact seal intent identity changed") from exc

    @contextmanager
    def _bind_seal_intent(
        self,
        job_id: UUID,
        *,
        candidate: LabJobArtifactCandidate | None,
        create: bool,
    ) -> Iterator[_BoundSealIntent]:
        parent_descriptor = self._managed_parent_descriptor(self.seal_intents_root)
        descriptor = -1
        name = f"{job_id.hex}.json"
        expected = self._candidate_seal_intent(candidate) if candidate is not None else None
        fault_boundary_reached = False
        published_here = False
        try:
            if create and expected is not None:
                self._quarantine_orphaned_seal_intent_temps(job_id)
                state = self._seal_intent_state(job_id)
                if state == "torn":
                    raise LabArtifactIntegrityError(
                        "torn job artifact seal intent requires recovery authority"
                    )
                if state == "missing":
                    temporary_name = f".{job_id.hex}.{uuid4().hex}.intent.tmp"
                    descriptor = os.open(
                        temporary_name,
                        os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                        0o600,
                        dir_fd=parent_descriptor,
                    )
                    payload = expected.canonical_json_bytes()
                    offset = 0
                    while offset < len(payload):
                        written = os.write(descriptor, payload[offset:])
                        if written <= 0:
                            raise LabArtifactIntegrityError(
                                "job artifact seal intent write made no progress"
                            )
                        offset += written
                    os.fchmod(descriptor, 0o600)
                    os.fsync(descriptor)
                    temporary_identity = _FileObservation.from_stat(os.fstat(descriptor))
                    temporary_at_path = _FileObservation.from_stat(
                        os.stat(
                            temporary_name,
                            dir_fd=parent_descriptor,
                            follow_symlinks=False,
                        )
                    )
                    temporary_payload = _read_descriptor(descriptor)
                    try:
                        temporary_intent = LabArtifactSealIntent.model_validate_json(
                            temporary_payload
                        )
                    except Exception as exc:
                        raise LabArtifactIntegrityError(
                            f"invalid job artifact seal intent temp: {exc}"
                        ) from exc
                    if (
                        temporary_identity != temporary_at_path
                        or temporary_identity.mode != stat.S_IFREG
                        or temporary_identity.nlink != 1
                        or temporary_payload != temporary_intent.canonical_json_bytes()
                        or temporary_intent != expected
                    ):
                        raise LabArtifactIntegrityError(
                            "job artifact seal intent temp validation failed"
                        )
                    fault_boundary_reached = True
                    self._after_seal_intent_temp_fsync(descriptor, temporary_name)
                    try:
                        _rename_noreplace(
                            parent_descriptor,
                            temporary_name,
                            parent_descriptor,
                            name,
                        )
                    except OSError as exc:
                        if exc.errno != errno.EEXIST:
                            raise
                        os.close(descriptor)
                        descriptor = -1
                        self._quarantine_seal_intent_entry(temporary_name)
                    else:
                        published_here = True
                        os.fsync(parent_descriptor)
                if descriptor < 0:
                    descriptor = os.open(
                        name,
                        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=parent_descriptor,
                    )
            else:
                descriptor = os.open(
                    name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=parent_descriptor,
                )
            identity = _FileObservation.from_stat(os.fstat(descriptor))
            payload = _read_descriptor(descriptor)
            try:
                intent = LabArtifactSealIntent.model_validate_json(payload)
            except Exception as exc:
                raise LabArtifactIntegrityError(f"invalid job artifact seal intent: {exc}") from exc
            if payload != intent.canonical_json_bytes() or intent.job_id != job_id:
                raise LabArtifactIntegrityError("job artifact seal intent identity conflicts")
            if candidate is not None and not self._intent_matches_candidate(intent, candidate):
                raise LabArtifactConflictError("job seal intent already binds different bytes")
            bound = _BoundSealIntent(
                parent_descriptor=parent_descriptor,
                descriptor=descriptor,
                name=name,
                identity=identity,
                intent=intent,
            )
            self._assert_bound_seal_intent(bound)
            if published_here:
                self._after_seal_intent_publish(bound)
                self._assert_bound_seal_intent(bound)
            self._after_seal_intent_bound(bound)
            self._assert_bound_seal_intent(bound)
            try:
                yield bound
            except BaseException:
                self._assert_bound_seal_intent(bound)
                raise
            else:
                self._assert_bound_seal_intent(bound)
        except LabArtifactError:
            raise
        except OSError as exc:
            if "bound" in locals() or fault_boundary_reached:
                raise
            raise LabArtifactIntegrityError("job artifact seal intent cannot be bound") from exc
        finally:
            if "bound" in locals():
                bound.close()
            else:
                if descriptor >= 0:
                    os.close(descriptor)
                os.close(parent_descriptor)

    @staticmethod
    def _intent_matches_candidate(
        intent: LabArtifactSealIntent,
        candidate: LabJobArtifactCandidate,
    ) -> bool:
        if (
            intent.job_id != candidate.job_id
            or intent.candidate_name != candidate.path.name
            or intent.manifest_hash != candidate.manifest_hash
            or intent.complete_result_hash != candidate.manifest.complete_result_hash
            or (
                intent.bundle_device,
                intent.bundle_inode,
                intent.bundle_size,
                intent.bundle_mtime_ns,
            )
            != (
                candidate.device,
                candidate.inode,
                candidate.size,
                candidate.mtime_ns,
            )
            or candidate.ctime_ns < intent.bundle_ctime_ns
        ):
            return False
        intended = {item.relative_path: item for item in intent.file_identities}
        current = {item.relative_path: item for item in candidate.file_identities}
        return set(intended) == set(current) and all(
            _matches_file_identity(
                _FileObservation(
                    device=item.device,
                    inode=item.inode,
                    mode=stat.S_IFREG,
                    nlink=1,
                    size=item.size,
                    mtime_ns=item.mtime_ns,
                    ctime_ns=item.ctime_ns,
                ),
                intended[relative_path],
                exact_ctime=False,
            )
            for relative_path, item in current.items()
        )

    def _load_seal_intent(self, job_id: UUID) -> LabArtifactSealIntent:
        with self._bind_seal_intent(job_id, candidate=None, create=False) as bound:
            return bound.intent

    @staticmethod
    def _validate_metadata_transition(
        before: _FileObservation,
        after: _FileObservation,
        *,
        expected_mode: int,
        label: str,
    ) -> None:
        if (
            after.device,
            after.inode,
            after.nlink,
            after.size,
            after.mtime_ns,
            after.mode,
        ) != (
            before.device,
            before.inode,
            before.nlink,
            before.size,
            before.mtime_ns,
            expected_mode,
        ) or after.ctime_ns < before.ctime_ns:
            raise LabArtifactIntegrityError(f"{label} identity changed during metadata update")

    def _seal_bound_files(self, bound: _BoundArtifactBundle) -> None:
        self._assert_bound_paths(bound)
        for relative_path in sorted(bound.files):
            item = bound.files[relative_path]
            before = _FileObservation.from_stat(os.fstat(item.descriptor))
            if before != item.current:
                raise LabArtifactIntegrityError(
                    f"bound artifact file identity changed before chmod: {relative_path}"
                )
            try:
                current_permissions = stat.S_IMODE(os.fstat(item.descriptor).st_mode)
                if current_permissions == 0o600:
                    os.fchmod(item.descriptor, 0o400)
                    after_chmod = _FileObservation.from_stat(os.fstat(item.descriptor))
                    self._validate_metadata_transition(
                        before,
                        after_chmod,
                        expected_mode=stat.S_IFREG,
                        label=f"artifact file {relative_path}",
                    )
                    item.current = after_chmod
                elif current_permissions == 0o400:
                    after_chmod = before
                else:
                    raise LabArtifactIntegrityError(
                        f"artifact file has unexpected freeze permissions: {relative_path}"
                    )
                if stat.S_IMODE(os.fstat(item.descriptor).st_mode) != 0o400:
                    raise LabArtifactIntegrityError(
                        f"artifact file permissions did not become 0400: {relative_path}"
                    )
                os.fsync(item.descriptor)
                after_fsync = _FileObservation.from_stat(os.fstat(item.descriptor))
            except LabArtifactIntegrityError:
                raise
            except OSError as exc:
                raise LabArtifactIntegrityError(
                    f"artifact file metadata could not be sealed: {relative_path}"
                ) from exc
            if after_fsync != item.current:
                raise LabArtifactIntegrityError(
                    f"artifact file identity changed during fsync: {relative_path}"
                )
            self._assert_bound_paths(bound)
        self._verify_bound_bytes(bound, self._bound_manifest(bound))

    @staticmethod
    def _bound_manifest(bound: _BoundArtifactBundle) -> LabJobArtifactManifest:
        payload = _read_descriptor(bound.files["manifest.json"].descriptor)
        try:
            manifest = LabJobArtifactManifest.model_validate_json(payload)
        except Exception as exc:
            raise LabArtifactIntegrityError("bound candidate manifest is invalid") from exc
        if payload != manifest.canonical_json_bytes():
            raise LabArtifactIntegrityError("bound candidate manifest is not canonical")
        return manifest

    def _finalize_bound_directories(self, bound: _BoundArtifactBundle) -> None:
        self._assert_bound_paths(bound)
        before_tables = _FileObservation.from_stat(os.fstat(bound.tables_descriptor))
        os.fchmod(bound.tables_descriptor, 0o500)
        after_tables = _FileObservation.from_stat(os.fstat(bound.tables_descriptor))
        self._validate_metadata_transition(
            before_tables,
            after_tables,
            expected_mode=stat.S_IFDIR,
            label="artifact tables directory",
        )
        if stat.S_IMODE(os.fstat(bound.tables_descriptor).st_mode) != 0o500:
            raise LabArtifactIntegrityError("artifact tables permissions did not become 0500")
        bound.tables_current = after_tables
        os.fsync(bound.tables_descriptor)
        if _FileObservation.from_stat(os.fstat(bound.tables_descriptor)) != after_tables:
            raise LabArtifactIntegrityError("artifact tables identity changed during fsync")
        self._assert_bound_paths(bound)

        before_bundle = _FileObservation.from_stat(os.fstat(bound.bundle_descriptor))
        os.fchmod(bound.bundle_descriptor, 0o500)
        after_bundle = _FileObservation.from_stat(os.fstat(bound.bundle_descriptor))
        self._validate_metadata_transition(
            before_bundle,
            after_bundle,
            expected_mode=stat.S_IFDIR,
            label="artifact bundle directory",
        )
        if stat.S_IMODE(os.fstat(bound.bundle_descriptor).st_mode) != 0o500:
            raise LabArtifactIntegrityError("artifact bundle permissions did not become 0500")
        bound.current = after_bundle
        os.fsync(bound.bundle_descriptor)
        if _FileObservation.from_stat(os.fstat(bound.bundle_descriptor)) != after_bundle:
            raise LabArtifactIntegrityError("artifact bundle identity changed during fsync")
        self._assert_bound_paths(bound)
        os.fsync(bound.parent_descriptor)

    def _candidate_from_path(
        self,
        path: Path,
        *,
        allow_interrupted_seal: bool = False,
    ) -> LabJobArtifactCandidate:
        managed = self._assert_managed_child(path, self.candidates_root, label="candidate")
        _, preliminary_manifest, _ = self._probe_bundle(
            managed,
            parent_root=self.candidates_root,
        )
        has_intent = self._seal_intent_exists(preliminary_manifest.job_id)
        profile: Literal["candidate", "interrupted", "sealed"] = (
            "interrupted" if allow_interrupted_seal and has_intent else "candidate"
        )
        manifest, identities, observed = self._validate_bundle(
            managed,
            parent_root=self.candidates_root,
            permission_profile=profile,
        )
        candidate = LabJobArtifactCandidate(
            path=managed,
            job_id=manifest.job_id,
            manifest=manifest,
            manifest_hash=manifest.manifest_hash,
            device=observed.device,
            inode=observed.inode,
            size=observed.size,
            mtime_ns=observed.mtime_ns,
            ctime_ns=observed.ctime_ns,
            file_identities=identities,
        )
        if allow_interrupted_seal and has_intent:
            intent = self._load_seal_intent(manifest.job_id)
            if not self._intent_matches_candidate(intent, candidate):
                raise LabArtifactIntegrityError(
                    "candidate seal intent does not bind the interrupted identity"
                )
        return candidate

    def verify_candidate(
        self,
        candidate: LabJobArtifactCandidate,
        *,
        allow_interrupted_seal: bool = False,
    ) -> LabJobArtifactManifest:
        path = self._assert_managed_child(candidate.path, self.candidates_root, label="candidate")
        has_intent = self._seal_intent_exists(candidate.job_id)
        profile: Literal["candidate", "interrupted", "sealed"] = (
            "interrupted" if allow_interrupted_seal and has_intent else "candidate"
        )
        manifest, identities, observed = self._validate_bundle(
            path,
            parent_root=self.candidates_root,
            permission_profile=profile,
        )
        if not self._same_bundle_identity(observed, candidate):
            raise LabArtifactIntegrityError("candidate bundle identity changed")
        if allow_interrupted_seal and has_intent:
            intent = self._load_seal_intent(manifest.job_id)
            current = candidate.model_copy(update={"file_identities": identities})
            if not self._intent_matches_candidate(intent, current):
                raise LabArtifactIntegrityError(
                    "candidate seal intent does not bind the interrupted identity"
                )
        if (
            manifest != candidate.manifest
            or manifest.manifest_hash != candidate.manifest_hash
            or identities != candidate.file_identities
        ):
            raise LabArtifactIntegrityError("candidate bundle identity or files changed")
        return manifest

    def verify_sealed(self, path: Path) -> LabSealedJobArtifact:
        managed = self._assert_managed_child(path, self.sealed_root, label="sealed bundle")
        manifest, identities, observed = self._validate_bundle(
            managed,
            parent_root=self.sealed_root,
            permission_profile="sealed",
        )
        if managed.name != manifest.job_id.hex:
            raise LabArtifactIntegrityError("sealed path does not match job identity")
        return LabSealedJobArtifact(
            path=managed,
            manifest=manifest,
            manifest_hash=manifest.manifest_hash,
            device=observed.device,
            inode=observed.inode,
            file_identities=identities,
        )

    @staticmethod
    def _after_existing_sealed_bound(
        _bound: _BoundArtifactBundle,
        _sealed: LabSealedJobArtifact,
    ) -> None:
        """Fault-injection boundary while an existing sealed bundle remains bound."""

    @contextmanager
    def _bind_verified_sealed(
        self,
        path: Path,
    ) -> Iterator[LabSealedJobArtifact]:
        managed = self._assert_managed_child(path, self.sealed_root, label="sealed bundle")
        observed, manifest, identities = self._probe_bundle(
            managed,
            parent_root=self.sealed_root,
        )
        with self._bind_bundle(
            parent_root=self.sealed_root,
            bundle_path=managed,
            manifest=manifest,
            expected_bundle=observed,
            expected_files=identities,
        ) as bound:
            verified_identities = self._validate_bound_bundle(
                bound,
                manifest,
                permission_profile="sealed",
            )
            if managed.name != manifest.job_id.hex:
                raise LabArtifactIntegrityError("sealed path does not match job identity")
            sealed = LabSealedJobArtifact(
                path=managed,
                manifest=manifest,
                manifest_hash=manifest.manifest_hash,
                device=bound.current.device,
                inode=bound.current.inode,
                file_identities=verified_identities,
            )
            self._after_existing_sealed_bound(bound, sealed)
            self._assert_bound_paths(bound)
            self._verify_bound_bytes(bound, manifest)
            yield sealed
            self._assert_bound_paths(bound)
            self._verify_bound_bytes(bound, manifest)

    @contextmanager
    def bind_verified_sealed(
        self,
        path: Path,
        *,
        indexed_at: datetime,
    ) -> Iterator[LabVerifiedSealedBinding]:
        """Hold every sealed bundle fd open across a caller-owned transaction."""

        with self._bind_verified_sealed(path) as sealed:
            evidence = LabArtifactIndexEvidence(
                job_id=sealed.manifest.job_id,
                sealed_path=sealed.path,
                manifest_hash=sealed.manifest_hash,
                complete_result_hash=sealed.manifest.complete_result_hash,
                bundle_device=sealed.device,
                bundle_inode=sealed.inode,
                file_identities=sealed.file_identities,
                indexed_at=indexed_at,
            )
            yield LabVerifiedSealedBinding(sealed=sealed, evidence=evidence)

    @staticmethod
    def _atomic_publish_noreplace(
        source_parent: int,
        source_name: str,
        destination_parent: int,
        destination_name: str,
    ) -> None:
        _rename_noreplace(
            source_parent,
            source_name,
            destination_parent,
            destination_name,
        )

    def seal_candidate(self, candidate: LabJobArtifactCandidate) -> LabSealedJobArtifact:
        self._assert_managed_roots()
        target = self.sealed_root / candidate.job_id.hex
        sealed_parent_probe = self._managed_parent_descriptor(self.sealed_root)
        try:
            try:
                os.stat(target.name, dir_fd=sealed_parent_probe, follow_symlinks=False)
            except FileNotFoundError:
                target_exists = False
            else:
                target_exists = True
        finally:
            os.close(sealed_parent_probe)
        if target_exists:
            with self._bind_verified_sealed(target) as existing:
                if (
                    existing.manifest_hash != candidate.manifest_hash
                    or existing.manifest.complete_result_hash
                    != candidate.manifest.complete_result_hash
                ):
                    raise LabArtifactConflictError("job already has a different sealed result")
                self.quarantine_candidate(candidate, reason="idempotent sealed bundle reuse")
                return existing.model_copy(update={"reused_existing": True})
        candidate_observed, preliminary_manifest, _ = self._probe_bundle(
            candidate.path,
            parent_root=self.candidates_root,
        )
        if preliminary_manifest != candidate.manifest or not self._same_bundle_identity(
            candidate_observed, candidate
        ):
            raise LabArtifactIntegrityError("candidate bundle identity changed before seal")
        if self.verify_candidate(candidate, allow_interrupted_seal=True) != candidate.manifest:
            raise LabArtifactIntegrityError("candidate manifest changed before seal")
        with self._bind_bundle(
            parent_root=self.candidates_root,
            bundle_path=candidate.path,
            manifest=candidate.manifest,
            expected_bundle=candidate_observed,
            expected_files=candidate.file_identities,
        ) as bound:
            has_intent = self._seal_intent_exists(candidate.job_id)
            identities = self._validate_bound_bundle(
                bound,
                candidate.manifest,
                permission_profile="interrupted" if has_intent else "candidate",
            )
            current_candidate = candidate.model_copy(update={"file_identities": identities})
            manifest = candidate.manifest
            if self._bound_manifest(bound) != manifest:
                raise LabArtifactIntegrityError("bound candidate manifest identity changed")
            self._assert_bound_paths(bound)
            self._verify_bound_bytes(bound, manifest)
            with self._bind_seal_intent(
                candidate.job_id,
                candidate=current_candidate,
                create=True,
            ) as bound_intent:
                self._assert_bound_seal_intent(bound_intent)
                self._seal_bound_files(bound)
                self._assert_bound_seal_intent(bound_intent)
                self._verify_bound_bytes(bound, manifest)
                self._assert_bound_paths(bound)
                sealed_parent = self._managed_parent_descriptor(self.sealed_root)
                source_parent = bound.parent_descriptor
                try:
                    self._atomic_publish_noreplace(
                        source_parent,
                        bound.bundle_name,
                        sealed_parent,
                        target.name,
                    )
                    self._assert_bound_seal_intent(bound_intent)
                    os.fsync(source_parent)
                    os.fsync(sealed_parent)
                    try:
                        os.stat(
                            bound.bundle_name,
                            dir_fd=source_parent,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        pass
                    else:
                        raise LabArtifactIntegrityError(
                            "candidate source path still exists after atomic rename"
                        )
                except (LabArtifactIntegrityError, LabArtifactPlatformError):
                    os.close(sealed_parent)
                    raise
                except OSError as exc:
                    os.close(sealed_parent)
                    if exc.errno == errno.EEXIST:
                        try:
                            with self._bind_verified_sealed(target) as existing:
                                if (
                                    existing.manifest_hash == candidate.manifest_hash
                                    and existing.manifest.complete_result_hash
                                    == candidate.manifest.complete_result_hash
                                ):
                                    self.quarantine_candidate(
                                        candidate,
                                        reason="idempotent racing sealed bundle reuse",
                                    )
                                    return existing.model_copy(update={"reused_existing": True})
                        except LabArtifactError:
                            pass
                    raise LabArtifactConflictError(
                        "candidate could not be atomically sealed"
                    ) from exc
                with suppress(OSError):
                    os.close(source_parent)
                bound.parent_descriptor = sealed_parent
                bound.bundle_name = target.name
                after_rename = _FileObservation.from_stat(os.fstat(bound.bundle_descriptor))
                self._validate_metadata_transition(
                    bound.current,
                    after_rename,
                    expected_mode=stat.S_IFDIR,
                    label="artifact bundle rename",
                )
                bound.current = after_rename
                self._assert_bound_paths(bound)
                self._verify_bound_bytes(bound, manifest)
                if self._bound_manifest(bound) != candidate.manifest:
                    raise LabArtifactIntegrityError("published candidate manifest identity changed")
                self._finalize_bound_directories(bound)
                self._assert_bound_seal_intent(bound_intent)
                self._verify_bound_bytes(bound, manifest)
                sealed = self.verify_sealed(target)
                self._assert_bound_seal_intent(bound_intent)
            return sealed

    def list_candidate_recovery(self) -> tuple[LabArtifactRecoveryRecord, ...]:
        records: list[LabArtifactRecoveryRecord] = []
        candidates_descriptor = self._managed_parent_descriptor(self.candidates_root)
        quarantine_descriptor = self._managed_parent_descriptor(self.quarantine_root)
        try:
            candidate_names = sorted(os.listdir(candidates_descriptor))
            quarantine_names = sorted(os.listdir(quarantine_descriptor))
        finally:
            os.close(candidates_descriptor)
            os.close(quarantine_descriptor)
        for name in candidate_names:
            path = self.candidates_root / name
            descriptor = self._managed_parent_descriptor(self.candidates_root)
            try:
                observed = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            finally:
                os.close(descriptor)
            if not stat.S_ISDIR(observed.st_mode):
                records.append(
                    LabArtifactRecoveryRecord(
                        path=path,
                        status="invalid",
                        reason="candidate entry is not a regular directory",
                    )
                )
                continue
            try:
                candidate = self._candidate_from_path(path, allow_interrupted_seal=True)
            except LabArtifactError as exc:
                records.append(
                    LabArtifactRecoveryRecord(
                        path=path,
                        status="invalid",
                        device=observed.st_dev,
                        inode=observed.st_ino,
                        reason=str(exc),
                    )
                )
            else:
                has_intent = self._seal_intent_exists(candidate.job_id)
                records.append(
                    LabArtifactRecoveryRecord(
                        path=path,
                        status="recoverable" if has_intent else "needs_authority",
                        job_id=candidate.job_id,
                        manifest_hash=candidate.manifest_hash,
                        device=candidate.device,
                        inode=candidate.inode,
                    )
                )
        for name in quarantine_names:
            path = self.quarantine_root / name
            descriptor = self._managed_parent_descriptor(self.quarantine_root)
            try:
                observed = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            finally:
                os.close(descriptor)
            if stat.S_ISDIR(observed.st_mode):
                records.append(
                    LabArtifactRecoveryRecord(
                        path=path,
                        status="quarantined",
                        device=observed.st_dev,
                        inode=observed.st_ino,
                    )
                )
        self._assert_managed_roots()
        return tuple(records)

    @staticmethod
    def _authorize_recovery(
        candidate: LabJobArtifactCandidate,
        authority: LabArtifactRecoveryAuthority | None,
    ) -> None:
        if authority is None:
            raise LabArtifactAuthorizationError(
                "external recovery authority is required for an unbound candidate"
            )
        manifest = candidate.manifest
        expected = (
            candidate.job_id,
            manifest.spec_hash,
            manifest.plan_hash,
            manifest.adapter_id,
            manifest.adapter_version,
            manifest.result_contract_version,
            manifest.code_sha,
            manifest.dataset_snapshot,
            candidate.manifest_hash,
        )
        supplied = (
            authority.job_id,
            authority.spec_hash,
            authority.plan_hash,
            authority.adapter_id,
            authority.adapter_version,
            authority.result_contract_version,
            authority.code_sha,
            authority.dataset_snapshot,
            authority.expected_manifest_hash,
        )
        if supplied != expected:
            raise LabArtifactAuthorizationError(
                "external recovery authority conflicts with candidate identity"
            )

    def recover_candidate(
        self,
        record: LabArtifactRecoveryRecord,
        *,
        authority: LabArtifactRecoveryAuthority | None = None,
    ) -> LabSealedJobArtifact:
        if record.status not in {"recoverable", "needs_authority"}:
            raise LabArtifactIntegrityError("only candidate recovery records can be sealed")
        if record.device is None or record.inode is None:
            raise LabArtifactIntegrityError("candidate recovery identity is unavailable")
        candidate = self._candidate_from_path(
            record.path,
            allow_interrupted_seal=True,
        )
        if (candidate.device, candidate.inode) != (record.device, record.inode):
            raise LabArtifactIntegrityError("candidate recovery record identity changed")
        if (
            record.job_id != candidate.job_id
            or record.manifest_hash != candidate.manifest_hash
            or (candidate.device, candidate.inode) != (record.device, record.inode)
        ):
            raise LabArtifactIntegrityError("candidate recovery evidence changed")
        intent_state = self._seal_intent_state(candidate.job_id)
        if intent_state != "valid" or record.status == "needs_authority" or authority is not None:
            self._authorize_recovery(candidate, authority)
        if intent_state == "torn":
            self._quarantine_seal_intent_entry(f"{candidate.job_id.hex}.json")
        return self.seal_candidate(candidate)

    def recover_interrupted_seal(self, path: Path) -> LabSealedJobArtifact:
        """Finish a durable seal intent after rename without trusting path identity."""

        managed = self._assert_managed_child(
            path, self.sealed_root, label="interrupted sealed bundle"
        )
        try:
            job_id = UUID(hex=managed.name)
        except ValueError as exc:
            raise LabArtifactIntegrityError(
                "interrupted sealed path does not contain a job identity"
            ) from exc
        if job_id.hex != managed.name:
            raise LabArtifactIntegrityError("interrupted sealed path is not canonical")
        with self._bind_seal_intent(job_id, candidate=None, create=False) as bound_intent:
            try:
                return self.verify_sealed(managed)
            except LabArtifactIntegrityError:
                pass
            manifest, identities, observed = self._validate_bundle(
                managed,
                parent_root=self.sealed_root,
                permission_profile="interrupted",
            )
            if managed.name != manifest.job_id.hex:
                raise LabArtifactIntegrityError(
                    "interrupted sealed path does not match job identity"
                )
            intent = bound_intent.intent
            if (
                intent.manifest_hash != manifest.manifest_hash
                or intent.complete_result_hash != manifest.complete_result_hash
            ):
                raise LabArtifactIntegrityError("seal intent manifest identity conflicts")
            if (
                observed.device,
                observed.inode,
                observed.size,
                observed.mtime_ns,
                observed.mode,
            ) != (
                intent.bundle_device,
                intent.bundle_inode,
                intent.bundle_size,
                intent.bundle_mtime_ns,
                stat.S_IFDIR,
            ) or observed.ctime_ns < intent.bundle_ctime_ns:
                raise LabArtifactIntegrityError("seal intent bundle identity changed")
            intended_files = {item.relative_path: item for item in intent.file_identities}
            current_files = {item.relative_path: item for item in identities}
            if set(intended_files) != set(current_files) or any(
                not _matches_file_identity(
                    _FileObservation(
                        device=current.device,
                        inode=current.inode,
                        mode=stat.S_IFREG,
                        nlink=1,
                        size=current.size,
                        mtime_ns=current.mtime_ns,
                        ctime_ns=current.ctime_ns,
                    ),
                    intended_files[relative_path],
                    exact_ctime=False,
                )
                for relative_path, current in current_files.items()
            ):
                raise LabArtifactIntegrityError("seal intent file identity changed")
            self._assert_bound_seal_intent(bound_intent)
            with self._bind_bundle(
                parent_root=self.sealed_root,
                bundle_path=managed,
                manifest=manifest,
                expected_bundle=observed,
                expected_files=identities,
            ) as bound:
                allowed_file_modes = {0o400, 0o600}
                if any(
                    stat.S_IMODE(os.fstat(item.descriptor).st_mode) not in allowed_file_modes
                    for item in bound.files.values()
                ):
                    raise LabArtifactIntegrityError(
                        "interrupted seal contains unexpected file permissions"
                    )
                if stat.S_IMODE(os.fstat(bound.bundle_descriptor).st_mode) not in {0o500, 0o700}:
                    raise LabArtifactIntegrityError(
                        "interrupted seal contains unexpected bundle permissions"
                    )
                if stat.S_IMODE(os.fstat(bound.tables_descriptor).st_mode) not in {0o500, 0o700}:
                    raise LabArtifactIntegrityError(
                        "interrupted seal contains unexpected tables permissions"
                    )
                self._verify_bound_bytes(bound, manifest)
                self._assert_bound_seal_intent(bound_intent)
                self._seal_bound_files(bound)
                self._assert_bound_seal_intent(bound_intent)
                self._finalize_bound_directories(bound)
                self._assert_bound_seal_intent(bound_intent)
                self._verify_bound_bytes(bound, manifest)
            sealed = self.verify_sealed(managed)
            self._assert_bound_seal_intent(bound_intent)
            return sealed

    def quarantine_candidate(
        self,
        candidate: LabJobArtifactCandidate,
        *,
        reason: str,
    ) -> LabArtifactRecoveryRecord:
        if not reason.strip():
            raise ValueError("quarantine reason must not be empty")
        path = self._assert_managed_child(candidate.path, self.candidates_root, label="candidate")
        target = self.quarantine_root / (
            f"{path.name}-{candidate.manifest_hash[:16]}-{uuid4().hex}"
        )
        observed = self._quarantine_bound_entry(
            source_name=path.name,
            target_name=target.name,
            expected_device=candidate.device,
            expected_inode=candidate.inode,
        )
        return LabArtifactRecoveryRecord(
            path=target,
            status="quarantined",
            job_id=candidate.job_id,
            manifest_hash=candidate.manifest_hash,
            device=observed.device,
            inode=observed.inode,
            reason=" ".join(reason.split()),
        )

    def quarantine_recovery_record(
        self,
        record: LabArtifactRecoveryRecord,
        *,
        reason: str,
    ) -> LabArtifactRecoveryRecord:
        """Logically isolate an invalid or recoverable candidate without deleting it."""

        if record.status == "quarantined":
            raise LabArtifactIntegrityError("candidate is already quarantined")
        if record.device is None or record.inode is None:
            raise LabArtifactIntegrityError("candidate recovery identity is unavailable")
        if not reason.strip():
            raise ValueError("quarantine reason must not be empty")
        path = self._assert_managed_child(
            record.path,
            self.candidates_root,
            label="candidate recovery entry",
        )
        target = self.quarantine_root / f"{path.name}-recovery-{uuid4().hex}"
        observed = self._quarantine_bound_entry(
            source_name=path.name,
            target_name=target.name,
            expected_device=record.device,
            expected_inode=record.inode,
        )
        return LabArtifactRecoveryRecord(
            path=target,
            status="quarantined",
            job_id=record.job_id,
            manifest_hash=record.manifest_hash,
            device=observed.device,
            inode=observed.inode,
            reason=" ".join(reason.split()),
        )

    @staticmethod
    def _atomic_quarantine_noreplace(
        source_parent: int,
        source_name: str,
        destination_parent: int,
        destination_name: str,
    ) -> None:
        _rename_noreplace(
            source_parent,
            source_name,
            destination_parent,
            destination_name,
        )

    def _quarantine_bound_entry(
        self,
        *,
        source_name: str,
        target_name: str,
        expected_device: int,
        expected_inode: int,
    ) -> _FileObservation:
        source_parent = self._managed_parent_descriptor(self.candidates_root)
        target_parent = self._managed_parent_descriptor(self.quarantine_root)
        source_descriptor = -1
        try:
            directory_flags = (
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            )
            before = _FileObservation.from_stat(
                os.stat(source_name, dir_fd=source_parent, follow_symlinks=False)
            )
            if (
                before.device,
                before.inode,
                before.mode,
            ) != (expected_device, expected_inode, stat.S_IFDIR):
                raise LabArtifactIntegrityError("candidate quarantine identity changed")
            source_descriptor = os.open(
                source_name,
                directory_flags,
                dir_fd=source_parent,
            )
            opened = _FileObservation.from_stat(os.fstat(source_descriptor))
            if opened != before:
                raise LabArtifactIntegrityError("candidate quarantine identity changed")
            try:
                self._atomic_quarantine_noreplace(
                    source_parent,
                    source_name,
                    target_parent,
                    target_name,
                )
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    raise LabArtifactConflictError("quarantine destination already exists") from exc
                raise
            target = _FileObservation.from_stat(
                os.stat(target_name, dir_fd=target_parent, follow_symlinks=False)
            )
            still_open = _FileObservation.from_stat(os.fstat(source_descriptor))
            if target != still_open or (
                target.device,
                target.inode,
            ) != (expected_device, expected_inode):
                raise LabArtifactIntegrityError("candidate quarantine target identity changed")
            try:
                os.stat(source_name, dir_fd=source_parent, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise LabArtifactIntegrityError("candidate quarantine source identity changed")
            os.fsync(source_parent)
            os.fsync(target_parent)
            self._assert_managed_roots()
            return target
        except LabArtifactError:
            raise
        except OSError as exc:
            raise LabArtifactIntegrityError("candidate quarantine identity changed") from exc
        finally:
            if source_descriptor >= 0:
                os.close(source_descriptor)
            os.close(source_parent)
            os.close(target_parent)

    @staticmethod
    def _authorize_export(
        sealed: LabSealedJobArtifact,
        evidence: LabArtifactIndexEvidence,
    ) -> None:
        expected = (
            sealed.manifest.job_id,
            sealed.path,
            sealed.manifest_hash,
            sealed.manifest.complete_result_hash,
            sealed.device,
            sealed.inode,
            sealed.file_identities,
        )
        actual = (
            evidence.job_id,
            evidence.sealed_path.absolute(),
            evidence.manifest_hash,
            evidence.complete_result_hash,
            evidence.bundle_device,
            evidence.bundle_inode,
            evidence.file_identities,
        )
        if actual != expected:
            raise LabArtifactAuthorizationError(
                "indexed evidence does not authorize this sealed bundle"
            )

    @staticmethod
    def _atomic_zip_publish_noreplace(
        source_parent: int,
        source_name: str,
        destination_parent: int,
        destination_name: str,
    ) -> None:
        _rename_noreplace(
            source_parent,
            source_name,
            destination_parent,
            destination_name,
        )

    def export_deterministic_zip(
        self,
        sealed_path: Path,
        evidence: LabArtifactIndexEvidence,
        destination: Path,
    ) -> Path:
        """Export stable bytes for this Python/ZIP runtime, not a cross-platform guarantee."""

        try:
            managed = self._assert_managed_child(
                sealed_path,
                self.sealed_root,
                label="sealed bundle",
            )
        except LabArtifactPathError as exc:
            raise LabArtifactAuthorizationError(
                "only a managed sealed bundle can be exported"
            ) from exc
        destination = destination.absolute()
        _ensure_private_directory(destination.parent, manage_existing=False)
        if destination.name in {"", ".", ".."}:
            raise LabArtifactPathError("ZIP destination name is unsafe")
        destination_parent = _secure_open_directory(destination.parent, create=True)
        destination_parent_identity = _FileObservation.from_stat(os.fstat(destination_parent))
        temporary_name = f".{destination.name}.{uuid4().hex}.tmp"
        temporary_descriptor = -1
        try:
            observed, manifest, identities = self._probe_bundle(
                managed,
                parent_root=self.sealed_root,
            )
            with self._bind_bundle(
                parent_root=self.sealed_root,
                bundle_path=managed,
                manifest=manifest,
                expected_bundle=observed,
                expected_files=identities,
            ) as bound:
                verified_identities = self._validate_bound_bundle(
                    bound,
                    manifest,
                    permission_profile="sealed",
                )
                sealed = LabSealedJobArtifact(
                    path=managed,
                    manifest=manifest,
                    manifest_hash=manifest.manifest_hash,
                    device=bound.current.device,
                    inode=bound.current.inode,
                    file_identities=verified_identities,
                )
                if managed.name != manifest.job_id.hex:
                    raise LabArtifactIntegrityError("sealed path does not match job identity")
                self._authorize_export(sealed, evidence)
                expected_hashes = self._expected_bound_hashes(manifest)
                export_payloads: dict[str, bytes] = {}
                for relative_path in sorted(bound.files):
                    payload = _read_descriptor(bound.files[relative_path].descriptor)
                    if _sha256(payload) != expected_hashes[relative_path]:
                        raise LabArtifactIntegrityError(f"export bytes conflict: {relative_path}")
                    export_payloads[relative_path] = payload
                self._assert_bound_paths(bound)
                self._assert_managed_roots()
                temporary_descriptor = os.open(
                    temporary_name,
                    os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=destination_parent,
                )
                temporary_identity = _FileObservation.from_stat(os.fstat(temporary_descriptor))
                if temporary_identity.mode != stat.S_IFREG or temporary_identity.nlink != 1:
                    raise LabArtifactIntegrityError("ZIP temporary is unsafe")
                with (
                    os.fdopen(os.dup(temporary_descriptor), "w+b") as stream,
                    ZipFile(
                        stream,
                        mode="w",
                        compression=ZIP_DEFLATED,
                        compresslevel=9,
                        strict_timestamps=True,
                    ) as archive,
                ):
                    for relative_path in sorted(export_payloads):
                        info = ZipInfo(relative_path, date_time=_ZIP_TIMESTAMP)
                        info.compress_type = ZIP_DEFLATED
                        info.create_system = 3
                        info.external_attr = (stat.S_IFREG | 0o400) << 16
                        info.flag_bits = 0
                        archive.writestr(
                            info,
                            export_payloads[relative_path],
                            compress_type=ZIP_DEFLATED,
                            compresslevel=9,
                        )
                os.fchmod(temporary_descriptor, 0o600)
                os.fsync(temporary_descriptor)
                final_temporary = _FileObservation.from_stat(os.fstat(temporary_descriptor))
                try:
                    self._atomic_zip_publish_noreplace(
                        destination_parent,
                        temporary_name,
                        destination_parent,
                        destination.name,
                    )
                except OSError as exc:
                    if exc.errno == errno.EEXIST:
                        raise LabArtifactConflictError("ZIP destination already exists") from exc
                    raise
                published = _FileObservation.from_stat(
                    os.stat(
                        destination.name,
                        dir_fd=destination_parent,
                        follow_symlinks=False,
                    )
                )
                self._validate_metadata_transition(
                    final_temporary,
                    published,
                    expected_mode=stat.S_IFREG,
                    label="ZIP destination",
                )
                os.fsync(destination_parent)
                current_destination_parent = _secure_open_directory(
                    destination.parent,
                    create=False,
                )
                try:
                    parent_at_path = _FileObservation.from_stat(
                        os.fstat(current_destination_parent)
                    )
                finally:
                    os.close(current_destination_parent)
                if not self._same_directory_identity(
                    parent_at_path,
                    destination_parent_identity,
                ):
                    raise LabArtifactIntegrityError("ZIP destination parent identity changed")
                self._assert_bound_paths(bound)
                self._assert_managed_roots()
            return destination
        finally:
            if temporary_descriptor >= 0:
                os.close(temporary_descriptor)
            os.close(destination_parent)


@dataclass(frozen=True)
class _LegacyAuthorityState:
    latest: dict[str, LabLegacyAuthorityEvent]
    generations: dict[str, int]


class LegacyArtifactIndex:
    """Index legacy sources with an fd-bound append-only authority ledger."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = _secure_absolute_path(path)
        self.clock = clock or (lambda: datetime.now(UTC))
        lock_key = os.fspath(self.path)
        self._process_lock_key = lock_key
        self._process_lock_registered = False
        with _LEGACY_PROCESS_LOCKS_GUARD:
            entry = _LEGACY_PROCESS_LOCKS.get(lock_key)
            if entry is None:
                entry = _LegacyProcessLockEntry(lock=threading.RLock(), references=0)
                _LEGACY_PROCESS_LOCKS[lock_key] = entry
            entry.references += 1
            self._process_lock = entry.lock
            self._process_lock_registered = True
        self._parent_descriptor = -1
        self._lock_descriptor = -1
        self._authority_descriptor = -1
        self._database_descriptor = -1
        self._journal_descriptor = -1
        self._cache_quarantine_descriptor = -1
        self._authority_lock_depth = 0
        self._cache_quarantine_path = self.path.parent / ".legacy-cache-quarantine"
        _ensure_private_directory(self.path.parent, manage_existing=False)
        _ensure_private_directory(self._cache_quarantine_path)
        self._parent_descriptor = _secure_open_directory(self.path.parent, create=False)
        try:
            self._parent_identity = _FileObservation.from_stat(os.fstat(self._parent_descriptor))
            with self._process_lock:
                self._lock_descriptor, _ = _open_or_create_private_regular_at(
                    self._parent_descriptor,
                    f"{self.path.name}.lock",
                    access_flags=os.O_RDWR,
                )
                fcntl.flock(self._lock_descriptor, fcntl.LOCK_EX)
                self._authority_lock_depth += 1
                try:
                    self._authority_descriptor, _ = _open_or_create_private_regular_at(
                        self._parent_descriptor,
                        f"{self.path.name}.authority.jsonl",
                        access_flags=os.O_RDWR,
                    )
                    self._database_descriptor, _ = _open_or_create_private_regular_at(
                        self._parent_descriptor,
                        self.path.name,
                        access_flags=os.O_RDONLY,
                    )
                    self._journal_descriptor, _ = _open_or_create_private_regular_at(
                        self._parent_descriptor,
                        f"{self.path.name}-journal",
                        access_flags=os.O_RDONLY,
                    )
                    self._cache_quarantine_descriptor = os.open(
                        self._cache_quarantine_path.name,
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=self._parent_descriptor,
                    )
                    self._lock_identity = _FileObservation.from_stat(
                        os.fstat(self._lock_descriptor)
                    )
                    self._authority_identity = _FileObservation.from_stat(
                        os.fstat(self._authority_descriptor)
                    )
                    self._database_identity = _FileObservation.from_stat(
                        os.fstat(self._database_descriptor)
                    )
                    self._journal_identity = _FileObservation.from_stat(
                        os.fstat(self._journal_descriptor)
                    )
                    self._cache_quarantine_identity = _FileObservation.from_stat(
                        os.fstat(self._cache_quarantine_descriptor)
                    )
                    self._assert_index_identity()
                    authority = self._read_authority_state()
                    self._ensure_cache_ready(authority)
                    self._assert_index_identity()
                finally:
                    self._authority_lock_depth -= 1
                    fcntl.flock(self._lock_descriptor, fcntl.LOCK_UN)
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        for attribute in (
            "_cache_quarantine_descriptor",
            "_journal_descriptor",
            "_database_descriptor",
            "_authority_descriptor",
            "_lock_descriptor",
            "_parent_descriptor",
        ):
            descriptor = getattr(self, attribute, -1)
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
                setattr(self, attribute, -1)
        if getattr(self, "_process_lock_registered", False):
            with _LEGACY_PROCESS_LOCKS_GUARD:
                entry = _LEGACY_PROCESS_LOCKS.get(self._process_lock_key)
                if entry is not None and entry.lock is self._process_lock:
                    entry.references -= 1
                    if entry.references == 0:
                        del _LEGACY_PROCESS_LOCKS[self._process_lock_key]
            self._process_lock_registered = False

    def __del__(self) -> None:
        self.close()

    @staticmethod
    def _same_index_entry(
        observed: _FileObservation,
        expected: _FileObservation,
        *,
        mode: int,
    ) -> bool:
        return (
            observed.device,
            observed.inode,
            observed.mode,
        ) == (expected.device, expected.inode, mode)

    def _assert_bound_index_file(
        self,
        *,
        descriptor: int,
        expected: _FileObservation,
        name: str,
        label: str,
    ) -> None:
        opened = _FileObservation.from_stat(os.fstat(descriptor))
        at_path = _FileObservation.from_stat(
            os.stat(name, dir_fd=self._parent_descriptor, follow_symlinks=False)
        )
        if (
            not self._same_index_entry(opened, expected, mode=stat.S_IFREG)
            or not self._same_index_entry(at_path, expected, mode=stat.S_IFREG)
            or opened.nlink != 1
            or at_path.nlink != 1
        ):
            raise LabArtifactIntegrityError(f"legacy index {label} identity changed")

    def _assert_index_sidecars_safe(self) -> None:
        for suffix in ("-wal", "-shm"):
            try:
                observed = _FileObservation.from_stat(
                    os.stat(
                        f"{self.path.name}{suffix}",
                        dir_fd=self._parent_descriptor,
                        follow_symlinks=False,
                    )
                )
            except FileNotFoundError:
                continue
            if observed.mode != stat.S_IFREG or observed.nlink != 1:
                raise LabArtifactIntegrityError("legacy index sidecar identity is unsafe")

    def _assert_authority_identity(self) -> None:
        current_parent_descriptor = -1
        try:
            parent_fd = _FileObservation.from_stat(os.fstat(self._parent_descriptor))
            current_parent_descriptor = _secure_open_directory(self.path.parent, create=False)
            parent_path = _FileObservation.from_stat(os.fstat(current_parent_descriptor))
            if not self._same_index_entry(
                parent_fd,
                self._parent_identity,
                mode=stat.S_IFDIR,
            ) or not self._same_index_entry(
                parent_path,
                self._parent_identity,
                mode=stat.S_IFDIR,
            ):
                raise LabArtifactIntegrityError("legacy index parent identity changed")
            self._assert_bound_index_file(
                descriptor=self._lock_descriptor,
                expected=self._lock_identity,
                name=f"{self.path.name}.lock",
                label="lock",
            )
            self._assert_bound_index_file(
                descriptor=self._authority_descriptor,
                expected=self._authority_identity,
                name=f"{self.path.name}.authority.jsonl",
                label="authority",
            )
            quarantine_fd = _FileObservation.from_stat(os.fstat(self._cache_quarantine_descriptor))
            quarantine_path = _FileObservation.from_stat(
                os.stat(
                    self._cache_quarantine_path.name,
                    dir_fd=self._parent_descriptor,
                    follow_symlinks=False,
                )
            )
            if not self._same_index_entry(
                quarantine_fd,
                self._cache_quarantine_identity,
                mode=stat.S_IFDIR,
            ) or not self._same_index_entry(
                quarantine_path,
                self._cache_quarantine_identity,
                mode=stat.S_IFDIR,
            ):
                raise LabArtifactIntegrityError("legacy index cache quarantine identity changed")
        except LabArtifactError:
            raise
        except (AttributeError, OSError) as exc:
            raise LabArtifactIntegrityError("legacy index authority identity changed") from exc
        finally:
            if current_parent_descriptor >= 0:
                os.close(current_parent_descriptor)

    def _assert_index_identity(self) -> None:
        self._assert_authority_identity()
        try:
            self._assert_bound_index_file(
                descriptor=self._database_descriptor,
                expected=self._database_identity,
                name=self.path.name,
                label="database",
            )
            self._assert_bound_index_file(
                descriptor=self._journal_descriptor,
                expected=self._journal_identity,
                name=f"{self.path.name}-journal",
                label="journal",
            )
            self._assert_index_sidecars_safe()
        except LabArtifactError:
            raise
        except (AttributeError, OSError) as exc:
            raise LabArtifactIntegrityError("legacy index database identity changed") from exc

    @contextmanager
    def _exclusive_index_lock(self) -> Iterator[None]:
        with self._process_lock:
            fcntl.flock(self._lock_descriptor, fcntl.LOCK_EX)
            self._authority_lock_depth += 1
            try:
                self._assert_index_identity()
                self._ensure_cache_ready(self._read_authority_state())
                yield
                self._assert_index_identity()
            finally:
                self._authority_lock_depth -= 1
                fcntl.flock(self._lock_descriptor, fcntl.LOCK_UN)

    @staticmethod
    def _before_sqlite_connect() -> None:
        """Fault-injection boundary before SQLite opens its path."""

    @staticmethod
    def _after_sqlite_connect(_connection: sqlite3.Connection) -> None:
        """Fault-injection boundary after SQLite opens its path."""

    def _connect(self, *, read_only: bool = False) -> sqlite3.Connection:
        self._assert_index_identity()
        self._before_sqlite_connect()
        self._assert_index_identity()
        try:
            database: str | Path = f"{self.path.as_uri()}?mode=ro" if read_only else self.path
            connection = sqlite3.connect(
                database,
                timeout=30,
                isolation_level=None,
                uri=read_only,
            )
            self._after_sqlite_connect(connection)
            if not read_only:
                connection.execute("PRAGMA journal_mode=TRUNCATE")
                connection.execute("PRAGMA synchronous=FULL")
            database_rows = connection.execute("PRAGMA database_list").fetchall()
            main_paths = [
                _secure_absolute_path(Path(str(row[2])))
                for row in database_rows
                if row[1] == "main"
            ]
            if main_paths != [self.path]:
                raise LabArtifactIntegrityError("legacy index connection identity changed")
            self._assert_index_identity()
            return connection
        except Exception:
            if "connection" in locals():
                connection.close()
            raise

    @contextmanager
    def _cache_connection(
        self,
        *,
        read_only: bool = False,
    ) -> Iterator[sqlite3.Connection]:
        connection = self._connect(read_only=read_only)
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _initialize_cache_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS legacy_artifact (
                logical_run_id TEXT PRIMARY KEY,
                source_path TEXT NOT NULL,
                device INTEGER NOT NULL,
                inode INTEGER NOT NULL,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                media_type TEXT NOT NULL,
                imported_at TEXT NOT NULL,
                publication_state TEXT NOT NULL DEFAULT 'cached',
                operation_id TEXT,
                generation INTEGER
            )
            """
        )
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(legacy_artifact)").fetchall()
        }
        additions = {
            "publication_state": "TEXT NOT NULL DEFAULT 'cached'",
            "operation_id": "TEXT",
            "generation": "INTEGER",
        }
        for name, definition in additions.items():
            if name not in columns:
                connection.execute(f"ALTER TABLE legacy_artifact ADD COLUMN {name} {definition}")

    @staticmethod
    def _published_authority_events(
        authority: _LegacyAuthorityState,
    ) -> tuple[LabLegacyAuthorityEvent, ...]:
        return tuple(
            sorted(
                (event for event in authority.latest.values() if event.event_type == "published"),
                key=lambda event: event.logical_run_id,
            )
        )

    @staticmethod
    def _cache_row(event: LabLegacyAuthorityEvent) -> tuple[object, ...]:
        record = event.record
        return (
            record.logical_run_id,
            str(record.source_path),
            record.device,
            record.inode,
            record.size,
            record.mtime_ns,
            record.sha256,
            record.media_type,
            record.imported_at.isoformat(timespec="microseconds"),
            "cached",
            str(event.operation_id),
            event.generation,
        )

    def _cache_matches_authority(
        self,
        connection: sqlite3.Connection,
        authority: _LegacyAuthorityState,
    ) -> bool:
        columns = tuple(
            str(row[1])
            for row in connection.execute("PRAGMA table_info(legacy_artifact)").fetchall()
        )
        expected_columns = (
            "logical_run_id",
            "source_path",
            "device",
            "inode",
            "size",
            "mtime_ns",
            "sha256",
            "media_type",
            "imported_at",
            "publication_state",
            "operation_id",
            "generation",
        )
        if columns != expected_columns:
            return False
        rows = tuple(
            connection.execute(
                """
                SELECT logical_run_id, source_path, device, inode, size, mtime_ns,
                       sha256, media_type, imported_at, publication_state,
                       operation_id, generation
                FROM legacy_artifact ORDER BY logical_run_id
                """
            ).fetchall()
        )
        expected = tuple(
            self._cache_row(event) for event in self._published_authority_events(authority)
        )
        return rows == expected

    def _populate_cache(
        self,
        connection: sqlite3.Connection,
        authority: _LegacyAuthorityState,
    ) -> None:
        self._initialize_cache_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        for event in self._published_authority_events(authority):
            connection.execute(
                """
                INSERT INTO legacy_artifact (
                    logical_run_id, source_path, device, inode, size, mtime_ns,
                    sha256, media_type, imported_at, publication_state,
                    operation_id, generation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._cache_row(event),
            )
        connection.commit()

    def _quarantine_cache_entry(
        self,
        *,
        name: str,
        descriptor: int,
        expected: _FileObservation,
    ) -> None:
        before = _FileObservation.from_stat(
            os.stat(name, dir_fd=self._parent_descriptor, follow_symlinks=False)
        )
        opened = _FileObservation.from_stat(os.fstat(descriptor))
        if (
            not self._same_index_entry(before, expected, mode=stat.S_IFREG)
            or not self._same_index_entry(opened, expected, mode=stat.S_IFREG)
            or before.nlink != 1
            or opened.nlink != 1
        ):
            raise LabArtifactIntegrityError("legacy cache identity changed before quarantine")
        target_name = f"{name}.{uuid4().hex}.quarantined"
        _rename_noreplace(
            self._parent_descriptor,
            name,
            self._cache_quarantine_descriptor,
            target_name,
        )
        target = _FileObservation.from_stat(
            os.stat(
                target_name,
                dir_fd=self._cache_quarantine_descriptor,
                follow_symlinks=False,
            )
        )
        still_open = _FileObservation.from_stat(os.fstat(descriptor))
        if target != still_open or (
            target.device,
            target.inode,
            target.mode,
            target.nlink,
            target.size,
            target.mtime_ns,
        ) != (
            opened.device,
            opened.inode,
            opened.mode,
            opened.nlink,
            opened.size,
            opened.mtime_ns,
        ):
            raise LabArtifactIntegrityError("legacy cache quarantine identity changed")

    def _rebuild_cache(self, authority: _LegacyAuthorityState) -> None:
        self._assert_authority_identity()
        self._quarantine_cache_entry(
            name=self.path.name,
            descriptor=self._database_descriptor,
            expected=self._database_identity,
        )
        self._quarantine_cache_entry(
            name=f"{self.path.name}-journal",
            descriptor=self._journal_descriptor,
            expected=self._journal_identity,
        )
        os.fsync(self._parent_descriptor)
        os.fsync(self._cache_quarantine_descriptor)
        os.close(self._database_descriptor)
        os.close(self._journal_descriptor)
        self._database_descriptor = -1
        self._journal_descriptor = -1

        temporary_name = f".{self.path.name}.{uuid4().hex}.cache.tmp"
        temporary_descriptor, _ = _open_or_create_private_regular_at(
            self._parent_descriptor,
            temporary_name,
            access_flags=os.O_RDWR,
        )
        temporary_identity = _FileObservation.from_stat(os.fstat(temporary_descriptor))
        os.close(temporary_descriptor)
        temporary_path = self.path.parent / temporary_name
        connection = sqlite3.connect(temporary_path, timeout=30, isolation_level=None)
        try:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=FULL")
            self._populate_cache(connection, authority)
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            if integrity != ("ok",):
                raise LabArtifactIntegrityError("rebuilt legacy cache failed integrity check")
        finally:
            connection.close()
        self._assert_authority_identity()
        temporary_descriptor = os.open(
            temporary_name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=self._parent_descriptor,
        )
        rebuilt_identity = _FileObservation.from_stat(os.fstat(temporary_descriptor))
        at_path = _FileObservation.from_stat(
            os.stat(
                temporary_name,
                dir_fd=self._parent_descriptor,
                follow_symlinks=False,
            )
        )
        if (
            rebuilt_identity != at_path
            or (rebuilt_identity.device, rebuilt_identity.inode)
            != (temporary_identity.device, temporary_identity.inode)
            or rebuilt_identity.mode != stat.S_IFREG
            or rebuilt_identity.nlink != 1
        ):
            os.close(temporary_descriptor)
            raise LabArtifactIntegrityError("rebuilt legacy cache candidate identity changed")
        os.fchmod(temporary_descriptor, 0o600)
        os.fsync(temporary_descriptor)
        _rename_noreplace(
            self._parent_descriptor,
            temporary_name,
            self._parent_descriptor,
            self.path.name,
        )
        os.fsync(self._parent_descriptor)
        self._database_descriptor = temporary_descriptor
        self._database_identity = _FileObservation.from_stat(os.fstat(self._database_descriptor))
        self._journal_descriptor, _ = _open_or_create_private_regular_at(
            self._parent_descriptor,
            f"{self.path.name}-journal",
            access_flags=os.O_RDONLY,
        )
        self._journal_identity = _FileObservation.from_stat(os.fstat(self._journal_descriptor))
        self._assert_index_identity()

    def _ensure_cache_ready(self, authority: _LegacyAuthorityState) -> None:
        try:
            with self._cache_connection(read_only=True) as connection:
                if self._cache_matches_authority(connection, authority):
                    return
        except sqlite3.Error:
            pass
        self._rebuild_cache(authority)
        with self._cache_connection() as connection:
            if not self._cache_matches_authority(connection, authority):
                raise LabArtifactIntegrityError("rebuilt legacy cache differs from authority")

    @staticmethod
    def _parse_authority_payload(payload: bytes) -> _LegacyAuthorityState:
        latest: dict[str, LabLegacyAuthorityEvent] = {}
        generations: dict[str, int] = {}
        for raw_line in payload.splitlines():
            try:
                event = LabLegacyAuthorityEvent.model_validate_json(raw_line)
            except Exception as exc:
                raise LabArtifactIntegrityError("legacy authority ledger is invalid") from exc
            if raw_line != event.canonical_json_bytes():
                raise LabArtifactIntegrityError("legacy authority ledger is not canonical")
            previous = latest.get(event.logical_run_id)
            prior_generation = generations.get(event.logical_run_id, 0)
            if event.event_type == "staged":
                if previous is not None and previous.event_type in {"staged", "published"}:
                    raise LabArtifactIntegrityError("legacy authority transition is invalid")
                if event.generation != prior_generation + 1:
                    raise LabArtifactIntegrityError("legacy authority generation is invalid")
                generations[event.logical_run_id] = event.generation
            else:
                if (
                    previous is None
                    or previous.event_type != "staged"
                    or previous.operation_id != event.operation_id
                    or previous.generation != event.generation
                    or previous.record != event.record
                ):
                    raise LabArtifactIntegrityError("legacy authority transition is invalid")
            latest[event.logical_run_id] = event
        return _LegacyAuthorityState(latest=latest, generations=generations)

    def _read_authority_state(self) -> _LegacyAuthorityState:
        if self._authority_lock_depth <= 0:
            raise LabArtifactIntegrityError("legacy authority ledger requires the exclusive lock")
        self._assert_index_identity()
        payload = _read_descriptor(self._authority_descriptor)
        self._assert_index_identity()
        if payload and not payload.endswith(b"\n"):
            final_newline = payload.rfind(b"\n")
            complete = payload[: final_newline + 1] if final_newline >= 0 else b""
            self._parse_authority_payload(complete)
            os.ftruncate(self._authority_descriptor, len(complete))
            os.fsync(self._authority_descriptor)
            self._assert_index_identity()
            payload = _read_descriptor(self._authority_descriptor)
            if payload != complete:
                raise LabArtifactIntegrityError("legacy authority tail repair was not durable")
        return self._parse_authority_payload(payload)

    def _append_authority_event(self, event: LabLegacyAuthorityEvent) -> None:
        self._read_authority_state()
        payload = event.canonical_json_bytes() + b"\n"
        os.lseek(self._authority_descriptor, 0, os.SEEK_END)
        offset = 0
        while offset < len(payload):
            written = os.write(self._authority_descriptor, payload[offset:])
            if written <= 0:
                raise LabArtifactIntegrityError("legacy authority append made no progress")
            offset += written
        os.fsync(self._authority_descriptor)
        self._assert_index_identity()
        self._read_authority_state()

    def _clock_utc(self) -> datetime:
        observed = self.clock()
        try:
            offset = observed.utcoffset()
        except (OverflowError, ValueError) as exc:
            raise ValueError("legacy index clock is outside the UTC datetime range") from exc
        if observed.tzinfo is None or offset is None:
            raise ValueError("legacy index clock must return a timezone-aware datetime")
        try:
            return observed.astimezone(UTC)
        except (OverflowError, ValueError) as exc:
            raise ValueError("legacy index clock is outside the UTC datetime range") from exc

    @staticmethod
    def _media_type(path: Path) -> Literal["application/json", "text/markdown; charset=utf-8"]:
        suffix = path.suffix.lower()
        if suffix == ".json":
            return "application/json"
        if suffix in {".md", ".markdown"}:
            return "text/markdown; charset=utf-8"
        raise LabArtifactPathError("legacy source must be JSON or Markdown")

    def get(self, logical_run_id: str) -> LabLegacyArtifactRecord | None:
        with self._exclusive_index_lock():
            event = self._read_authority_state().latest.get(logical_run_id)
            if event is None or event.event_type != "published":
                return None
            record = event.record
            try:
                with _open_bound_readonly_file(
                    record.source_path,
                    label="published legacy artifact source",
                ) as bound:
                    payload = _read_descriptor(bound.descriptor)
                    _assert_bound_readonly_file(bound, label="published legacy artifact source")
                    if not self._legacy_record_matches(record, bound.file_identity, payload):
                        return None
            except LabArtifactError:
                return None
            return record

    @staticmethod
    def _legacy_record_matches(
        record: LabLegacyArtifactRecord,
        observation: _FileObservation,
        payload: bytes,
    ) -> bool:
        return (
            record.device,
            record.inode,
            record.size,
            record.mtime_ns,
            record.sha256,
        ) == (
            observation.device,
            observation.inode,
            observation.size,
            observation.mtime_ns,
            _sha256(payload),
        )

    @staticmethod
    def _before_commit_source_check(path: Path, expected: _FileObservation) -> None:
        try:
            observed = _FileObservation.from_stat(path.lstat())
        except OSError as exc:
            raise LabArtifactIntegrityError("legacy source changed before index commit") from exc
        if observed != expected:
            raise LabArtifactIntegrityError("legacy source changed before index commit")

    @staticmethod
    def _after_stage_commit(_record: LabLegacyArtifactRecord) -> None:
        """Fault-injection boundary after an authority stage becomes durable."""

    def _cache_stage(
        self,
        *,
        event: LabLegacyAuthorityEvent,
        previous: LabLegacyAuthorityEvent | None,
    ) -> None:
        with self._cache_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if previous is not None and previous.event_type == "staged":
                connection.execute(
                    """
                    DELETE FROM legacy_artifact
                    WHERE logical_run_id = ? AND operation_id = ? AND generation = ?
                      AND publication_state = 'staged'
                    """,
                    (
                        previous.logical_run_id,
                        str(previous.operation_id),
                        previous.generation,
                    ),
                )
            record = event.record
            connection.execute(
                """
                INSERT INTO legacy_artifact (
                    logical_run_id, source_path, device, inode, size, mtime_ns,
                    sha256, media_type, imported_at, publication_state,
                    operation_id, generation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'staged', ?, ?)
                ON CONFLICT(logical_run_id) DO UPDATE SET
                    source_path=excluded.source_path,
                    device=excluded.device,
                    inode=excluded.inode,
                    size=excluded.size,
                    mtime_ns=excluded.mtime_ns,
                    sha256=excluded.sha256,
                    media_type=excluded.media_type,
                    imported_at=excluded.imported_at,
                    publication_state='staged',
                    operation_id=excluded.operation_id,
                    generation=excluded.generation
                """,
                (
                    record.logical_run_id,
                    str(record.source_path),
                    record.device,
                    record.inode,
                    record.size,
                    record.mtime_ns,
                    record.sha256,
                    record.media_type,
                    record.imported_at.isoformat(timespec="microseconds"),
                    str(event.operation_id),
                    event.generation,
                ),
            )
            self._assert_index_identity()
            connection.commit()
            self._assert_index_identity()

    def _cache_complete(self, event: LabLegacyAuthorityEvent) -> None:
        with self._cache_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE legacy_artifact SET publication_state = 'cached'
                WHERE logical_run_id = ? AND operation_id = ? AND generation = ?
                  AND publication_state = 'staged'
                """,
                (event.logical_run_id, str(event.operation_id), event.generation),
            )
            if cursor.rowcount != 1:
                raise LabArtifactIntegrityError("legacy cache operation ownership changed")
            self._assert_index_identity()
            connection.commit()
            self._assert_index_identity()

    def _delete_cache_operation(self, event: LabLegacyAuthorityEvent) -> None:
        with self._cache_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                DELETE FROM legacy_artifact
                WHERE logical_run_id = ? AND operation_id = ? AND generation = ?
                  AND publication_state IN ('staged', 'cached')
                """,
                (event.logical_run_id, str(event.operation_id), event.generation),
            )
            connection.commit()

    def import_file(
        self,
        *,
        logical_run_id: str,
        source_path: Path,
    ) -> LabLegacyIndexResult:
        with self._exclusive_index_lock():
            return self._import_file_locked(
                logical_run_id=logical_run_id,
                source_path=source_path,
            )

    def _import_file_locked(
        self,
        *,
        logical_run_id: str,
        source_path: Path,
    ) -> LabLegacyIndexResult:
        logical_run_id = " ".join(logical_run_id.split())
        if not logical_run_id:
            raise ValueError("logical_run_id must not be empty")
        source = _secure_absolute_path(source_path)
        media_type = self._media_type(source)
        with _open_bound_readonly_file(source, label="legacy artifact source") as bound:
            payload = _read_descriptor(bound.descriptor)
            _assert_bound_readonly_file(bound, label="legacy artifact source")
            observation = bound.file_identity
            occurred_at = self._clock_utc()
            record = LabLegacyArtifactRecord(
                logical_run_id=logical_run_id,
                source_path=source,
                device=observation.device,
                inode=observation.inode,
                size=observation.size,
                mtime_ns=observation.mtime_ns,
                sha256=_sha256(payload),
                media_type=media_type,
                imported_at=occurred_at,
            )
            authority = self._read_authority_state()
            previous = authority.latest.get(logical_run_id)
            if previous is not None and previous.event_type == "published":
                existing = previous.record
                if existing.source_path != source or not self._legacy_record_matches(
                    existing,
                    observation,
                    payload,
                ):
                    raise LabLegacyArtifactConflictError(
                        "legacy logical run already references different source bytes"
                    )
                self._before_commit_source_check(source, observation)
                _assert_bound_readonly_file(bound, label="legacy artifact source")
                return LabLegacyIndexResult(status="reused", record=existing)
            if previous is not None and previous.event_type == "staged":
                abandoned = previous.model_copy(
                    update={"event_type": "abandoned", "occurred_at": occurred_at}
                )
                self._append_authority_event(abandoned)
            generation = authority.generations.get(logical_run_id, 0) + 1
            operation_id = uuid4()
            staged = LabLegacyAuthorityEvent(
                event_type="staged",
                logical_run_id=logical_run_id,
                operation_id=operation_id,
                generation=generation,
                record=record,
                occurred_at=occurred_at,
            )
            self._append_authority_event(staged)
            published = False
            try:
                self._cache_stage(event=staged, previous=previous)
                self._before_commit_source_check(source, observation)
                _assert_bound_readonly_file(bound, label="legacy artifact source")
                self._after_stage_commit(record)
                self._before_commit_source_check(source, observation)
                _assert_bound_readonly_file(bound, label="legacy artifact source")
                self._cache_complete(staged)
                self._before_commit_source_check(source, observation)
                _assert_bound_readonly_file(bound, label="legacy artifact source")
                self._append_authority_event(
                    staged.model_copy(
                        update={"event_type": "published", "occurred_at": self._clock_utc()}
                    )
                )
                published = True
            except Exception:
                if not published:
                    with suppress(LabArtifactError, OSError, ValueError):
                        current = self._read_authority_state().latest.get(logical_run_id)
                        if (
                            current is not None
                            and current.event_type == "staged"
                            and current.operation_id == operation_id
                            and current.generation == generation
                        ):
                            self._append_authority_event(
                                staged.model_copy(
                                    update={
                                        "event_type": "abandoned",
                                        "occurred_at": occurred_at,
                                    }
                                )
                            )
                    with suppress(LabArtifactError, sqlite3.Error, OSError):
                        self._delete_cache_operation(staged)
                raise
            return LabLegacyIndexResult(status="imported", record=record)
