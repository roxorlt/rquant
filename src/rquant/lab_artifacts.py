"""Immutable, deterministic job-level artifact bundles for Strategy Lab."""

from __future__ import annotations

import base64
import ctypes
import errno
import fcntl
import hashlib
import io
import math
import os
import re
import sqlite3
import stat
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from functools import wraps
from pathlib import Path, PurePosixPath
from typing import Concatenate, Literal, ParamSpec, Self, TypeVar
from uuid import UUID, uuid4
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.canonical_json_stream import (
    CanonicalJsonStreamWriter,
    PandasJsonColumnAccessor,
)
from rquant.research_run_spec import DatasetSnapshotIdentity, ResearchRunSpec
from rquant.strict_json import (
    StrictJsonError,
    strict_json_loads,
    strict_model_validate_canonical_json,
)
from rquant.strict_json import (
    canonical_json_bytes as encode_canonical_json_bytes,
)

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_CODE_SHA_PATTERN = r"^[0-9a-f]{40}$"
_TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)
_ZIP_STREAM_CHUNK_SIZE = 1024 * 1024
_LEGACY_GENESIS_HASH = "0" * 64
# Reentrant on purpose. These guard the per-inode lock registries, and an index's
# __del__ closes it - so a garbage collection triggered by an allocation *inside*
# one of these critical sections re-enters the same guard on the same thread. A
# plain Lock deadlocks the interpreter there with no way out; CPython 3.12's
# collector made that ordering common enough to hang a whole CI shard.
_LEGACY_PROCESS_LOCKS_GUARD = threading.RLock()
_ARTIFACT_PROCESS_LOCKS_GUARD = threading.RLock()
_FINALIZATION_PROCESS_LOCKS_GUARD = threading.RLock()


@dataclass
class _LegacyProcessLockEntry:
    lock: threading.RLock
    references: int
    owner_thread_id: int | None = None


_LEGACY_PROCESS_LOCKS: dict[tuple[int, int], _LegacyProcessLockEntry] = {}


@dataclass
class _ArtifactProcessLockEntry:
    lock: threading.RLock
    references: int
    owner_thread_id: int | None = None
    prepare_owner_thread_id: int | None = None
    lifecycle_owner_thread_id: int | None = None
    lifecycle_depth: int = 0
    poisoned: bool = False


_ARTIFACT_PROCESS_LOCKS: dict[tuple[int, int], _ArtifactProcessLockEntry] = {}


@dataclass
class _FinalizationProcessLockEntry:
    lock: threading.Lock
    references: int


_FINALIZATION_PROCESS_LOCKS: dict[
    tuple[int, int, str, str],
    _FinalizationProcessLockEntry,
] = {}
_ArtifactOperationParams = ParamSpec("_ArtifactOperationParams")
_ArtifactOperationResult = TypeVar("_ArtifactOperationResult")


def _artifact_public_operation(
    *,
    prepare: bool = False,
) -> Callable[
    [
        Callable[
            Concatenate[LabJobArtifactStore, _ArtifactOperationParams],
            _ArtifactOperationResult,
        ]
    ],
    Callable[
        Concatenate[LabJobArtifactStore, _ArtifactOperationParams],
        _ArtifactOperationResult,
    ],
]:
    def decorate(
        operation: Callable[
            Concatenate[LabJobArtifactStore, _ArtifactOperationParams],
            _ArtifactOperationResult,
        ],
    ) -> Callable[
        Concatenate[LabJobArtifactStore, _ArtifactOperationParams],
        _ArtifactOperationResult,
    ]:
        @wraps(operation)
        def guarded(
            store: LabJobArtifactStore,
            *args: _ArtifactOperationParams.args,
            **kwargs: _ArtifactOperationParams.kwargs,
        ) -> _ArtifactOperationResult:
            with store._artifact_operation_lifecycle(prepare=prepare):
                return operation(store, *args, **kwargs)

        return guarded

    return decorate


class LabArtifactError(RuntimeError):
    """Base error for job artifact operations."""


class LabArtifactPathError(LabArtifactError):
    """An artifact path escaped its managed root or violated the path contract."""


class LabArtifactIntegrityError(LabArtifactError):
    """Artifact bytes, structure, identity, or permissions failed verification."""


class LabArtifactLifecycleError(LabArtifactIntegrityError):
    """An artifact store lifecycle transition conflicts with active ownership."""


class LabArtifactPayloadLimitError(LabArtifactIntegrityError):
    """A pure artifact plan exceeded its caller-supplied in-memory budget."""


class LabArtifactConflictError(LabArtifactError):
    """A deterministic artifact identity already contains different content."""


class LabArtifactAuthorizationError(LabArtifactError):
    """Export evidence does not authorize the selected sealed artifact."""


class LabArtifactPlatformError(LabArtifactError):
    """The host cannot provide a required fail-closed filesystem primitive."""


class LabArtifactFinalizationLockError(LabArtifactError):
    """A per-result finalization lock could not be acquired or verified."""


class LabArtifactFinalizationLockTimeoutError(LabArtifactFinalizationLockError):
    """A per-result finalization lock remained unavailable until its deadline."""


class _BoundedBytesIO(io.BytesIO):
    def __init__(self, *, max_payload_bytes: int) -> None:
        super().__init__()
        self._max_payload_bytes = max_payload_bytes

    def write(self, payload: bytes | bytearray | memoryview, /) -> int:
        current_size = self.getbuffer().nbytes
        next_size = max(current_size, self.tell() + len(payload))
        if next_size > self._max_payload_bytes:
            raise LabArtifactPayloadLimitError("final artifact payload byte budget exceeded")
        return super().write(payload)


class _LabArtifactActiveGuardError(LabArtifactIntegrityError):
    """Durable guard authority requires explicit startup recovery."""


class LabLegacyArtifactConflictError(LabArtifactError):
    """A legacy logical run is already indexed with different source bytes."""


def _raise_collected_errors(
    message: str,
    errors: list[BaseException],
) -> None:
    if not errors:
        return
    if len(errors) == 1:
        raise errors[0]
    if all(isinstance(error, Exception) for error in errors):
        raise ExceptionGroup(
            message,
            [error for error in errors if isinstance(error, Exception)],
        )
    raise BaseExceptionGroup(message, errors)


def _acquire_exclusive_flock(descriptor: int, *, label: str) -> None:
    """Acquire flock while treating an acquisition exception as unknown state."""

    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
    except BaseException as acquire_error:
        errors = [acquire_error]
        for _attempt in range(2):
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                break
            except BaseException as unlock_error:
                errors.append(unlock_error)
        _raise_collected_errors(
            f"{label} acquisition and unknown-state rollback both failed",
            errors,
        )


def _close_descriptor_fail_closed(descriptor: int, *, label: str) -> None:
    """Close an fd, retrying when a wrapper may have raised before close(2)."""

    try:
        os.close(descriptor)
        return
    except BaseException as close_error:
        errors = [close_error]
    try:
        os.close(descriptor)
    except OSError as retry_error:
        if retry_error.errno != errno.EBADF:
            errors.append(retry_error)
    except BaseException as retry_error:
        errors.append(retry_error)
    _raise_collected_errors(f"{label} close failed", errors)


def _entry_file_type(mode: int) -> Literal["directory", "regular", "symlink", "other"]:
    if stat.S_ISDIR(mode):
        return "directory"
    if stat.S_ISREG(mode):
        return "regular"
    if stat.S_ISLNK(mode):
        return "symlink"
    return "other"


def _normalize_logical_run_id(logical_run_id: str) -> str:
    if not isinstance(logical_run_id, str):
        raise TypeError("logical_run_id must be a string")
    normalized = " ".join(logical_run_id.split())
    if not normalized:
        raise ValueError("logical_run_id must not be empty")
    return normalized


def _matches_rename_identity(
    before: _FileObservation,
    after: _FileObservation,
) -> bool:
    return (
        after.device,
        after.inode,
        after.mode,
        after.nlink,
        after.size,
        after.mtime_ns,
    ) == (
        before.device,
        before.inode,
        before.mode,
        before.nlink,
        before.size,
        before.mtime_ns,
    ) and after.ctime_ns >= before.ctime_ns


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


class LabBoundZipDestination(LabArtifactModel):
    directory_path: Path
    directory_descriptor: int = Field(ge=0)
    directory_device: int = Field(ge=0)
    directory_inode: int = Field(ge=1)
    file_name: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_destination(self) -> LabBoundZipDestination:
        normalized = Path(os.path.abspath(os.fspath(self.directory_path)))
        if self.directory_path != normalized:
            raise ValueError("bound ZIP directory path must be absolute and normalized")
        if (
            PurePosixPath(self.file_name).name != self.file_name
            or "\\" in self.file_name
            or self.file_name in {"", ".", ".."}
        ):
            raise ValueError("bound ZIP file name is unsafe")
        return self

    @property
    def path(self) -> Path:
        return self.directory_path / self.file_name


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

    return encode_canonical_json_bytes(_canonical_value(value))


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


class LabPandasDtypeIdentity(LabArtifactModel):
    family: Literal[
        "numpy",
        "extension",
        "categorical",
        "datetime_tz",
        "period",
        "interval",
    ]
    pandas_dtype: str = Field(min_length=1)
    dtype_repr: str = Field(min_length=1)
    dtype_class: str = Field(min_length=1)
    numpy_kind: str | None = None
    categories: tuple[str, ...] | None = None
    categories_dtype: str | None = None
    categories_dtype_class: str | None = None
    categories_dtype_identity: LabPandasDtypeIdentity | None = None
    ordered: bool | None = None
    timezone: str | None = None
    unit: str | None = None
    storage: str | None = None
    na_value: str | None = None
    na_value_kind: Literal["pd.NA", "NaT", "nan", "none", "canonical"] | None = None
    period_frequency: str | None = None
    interval_subtype_identity: LabPandasDtypeIdentity | None = None
    interval_closed: Literal["left", "right", "both", "neither"] | None = None

    @model_validator(mode="after")
    def validate_family_metadata(self) -> LabPandasDtypeIdentity:
        if self.family == "categorical":
            if (
                self.categories is None
                or self.categories_dtype is None
                or self.categories_dtype_class is None
                or self.categories_dtype_identity is None
                or self.ordered is None
            ):
                raise ValueError("categorical dtype identity is incomplete")
        elif any(
            value is not None
            for value in (
                self.categories,
                self.categories_dtype,
                self.categories_dtype_class,
                self.categories_dtype_identity,
                self.ordered,
            )
        ):
            raise ValueError("category metadata is only valid for categorical dtypes")
        if self.family == "datetime_tz":
            if self.timezone is None or self.unit is None:
                raise ValueError("timezone dtype identity is incomplete")
        elif self.timezone is not None:
            raise ValueError("timezone metadata is only valid for timezone dtypes")
        if self.family == "period":
            if self.period_frequency is None:
                raise ValueError("period dtype identity is incomplete")
        elif self.period_frequency is not None:
            raise ValueError("period metadata is only valid for period dtypes")
        if self.family == "interval":
            if self.interval_subtype_identity is None or self.interval_closed is None:
                raise ValueError("interval dtype identity is incomplete")
        elif self.interval_subtype_identity is not None or self.interval_closed is not None:
            raise ValueError("interval metadata is only valid for interval dtypes")
        if self.family != "extension" and any(
            value is not None for value in (self.storage, self.na_value, self.na_value_kind)
        ):
            raise ValueError("extension metadata is only valid for extension dtypes")
        return self


class LabParquetIdentity(LabArtifactModel):
    table_name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    row_count: int = Field(ge=0)
    columns: tuple[str, ...]
    dtypes: tuple[str, ...]
    dtype_identities: tuple[LabPandasDtypeIdentity, ...]
    content_sha256: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_shape(self) -> LabParquetIdentity:
        if len(self.columns) != len(self.dtypes) or len(self.columns) != len(self.dtype_identities):
            raise ValueError("Parquet columns and dtype identities must have equal length")
        if self.dtypes != tuple(item.pandas_dtype for item in self.dtype_identities):
            raise ValueError("Parquet dtype strings conflict with typed identities")
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
        return canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def manifest_hash(self) -> str:
        return _sha256(self.canonical_json_bytes())


class LabArtifactPlannedPayload(LabArtifactModel):
    relative_path: str
    payload: bytes

    @model_validator(mode="after")
    def validate_relative_path(self) -> LabArtifactPlannedPayload:
        try:
            _safe_relative_path(self.relative_path)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return self


class LabArtifactPayloadBudget(LabArtifactModel):
    """In-memory planning limits supplied by the finalizer service boundary."""

    max_single_payload_bytes: int = Field(ge=1)
    max_total_payload_bytes: int = Field(ge=1)
    max_table_count: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_payload_budget(self) -> LabArtifactPayloadBudget:
        if self.max_single_payload_bytes > self.max_total_payload_bytes:
            raise ValueError("single payload budget cannot exceed total payload budget")
        return self


class LabJobArtifactPlan(LabArtifactModel):
    job_id: UUID
    manifest: LabJobArtifactManifest
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    payloads: tuple[LabArtifactPlannedPayload, ...]

    @model_validator(mode="after")
    def validate_exact_payloads(self) -> LabJobArtifactPlan:
        if self.job_id != self.manifest.job_id:
            raise ValueError("artifact plan job_id conflicts with manifest")
        if self.manifest_hash != self.manifest.manifest_hash:
            raise ValueError("artifact plan manifest_hash conflicts with manifest")
        paths = tuple(item.relative_path for item in self.payloads)
        if paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise ValueError("artifact plan payloads must be sorted and unique")
        manifest_bytes = self.manifest.canonical_json_bytes()
        sums = {item.relative_path: item.sha256 for item in self.manifest.files}
        sums["manifest.json"] = self.manifest_hash
        sums_bytes = "".join(
            f"{digest}  {relative_path}\n" for relative_path, digest in sorted(sums.items())
        ).encode("ascii")
        expected = {item.relative_path: (item.size, item.sha256) for item in self.manifest.files}
        expected["manifest.json"] = (len(manifest_bytes), self.manifest_hash)
        expected["SHA256SUMS"] = (len(sums_bytes), _sha256(sums_bytes))
        actual = {item.relative_path: item.payload for item in self.payloads}
        if set(actual) != set(expected):
            raise ValueError("artifact plan payload inventory conflicts with manifest")
        if actual["manifest.json"] != manifest_bytes or actual["SHA256SUMS"] != sums_bytes:
            raise ValueError("artifact plan authority payloads conflict with manifest")
        if any(
            (len(actual[path]), _sha256(actual[path])) != identity
            for path, identity in expected.items()
        ):
            raise ValueError("artifact plan payload bytes conflict with manifest")
        return self


class LabArtifactFileIdentity(LabArtifactModel):
    relative_path: str
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    ctime_ns: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_relative_path(self) -> LabArtifactFileIdentity:
        try:
            _safe_relative_path(self.relative_path)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return self


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

    @model_validator(mode="after")
    def validate_candidate_identity(self) -> LabJobArtifactCandidate:
        if self.job_id != self.manifest.job_id:
            raise ValueError("candidate job_id conflicts with manifest")
        if self.manifest_hash != self.manifest.manifest_hash:
            raise ValueError("candidate manifest_hash conflicts with manifest")
        paths = tuple(item.relative_path for item in self.file_identities)
        expected_paths = {
            "manifest.json",
            "SHA256SUMS",
            *(item.relative_path for item in self.manifest.files),
        }
        if paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise ValueError("candidate file identities must be sorted and unique")
        if set(paths) != expected_paths:
            raise ValueError("candidate file identities conflict with manifest inventory")
        identities = {item.relative_path: item for item in self.file_identities}
        expected_sizes = {item.relative_path: item.size for item in self.manifest.files}
        manifest_bytes = self.manifest.canonical_json_bytes()
        expected_sizes["manifest.json"] = len(manifest_bytes)
        sums = {item.relative_path: item.sha256 for item in self.manifest.files}
        sums["manifest.json"] = self.manifest.manifest_hash
        expected_sizes["SHA256SUMS"] = len(
            "".join(
                f"{digest}  {relative_path}\n" for relative_path, digest in sorted(sums.items())
            ).encode("ascii")
        )
        if any(
            identities[relative_path].size != expected_size
            for relative_path, expected_size in expected_sizes.items()
        ):
            raise ValueError("candidate file sizes conflict with manifest inventory")
        file_nodes = tuple((item.device, item.inode) for item in self.file_identities)
        if any(item.device != self.device for item in self.file_identities):
            raise ValueError("candidate files must share the bundle filesystem")
        if len(file_nodes) != len(set(file_nodes)) or (self.device, self.inode) in file_nodes:
            raise ValueError("candidate file identities are not unique")
        return self


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
        if not self.candidate_name.startswith(f"{self.job_id.hex}-"):
            raise ValueError("seal intent candidate name conflicts with job identity")
        paths = tuple(item.relative_path for item in self.file_identities)
        if paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise ValueError("seal intent file identities must be sorted and unique")
        return self

    def canonical_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


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


class LabPrepareCandidateRequest(LabArtifactModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=False,
        strict=True,
        arbitrary_types_allowed=True,
    )

    job_id: UUID
    spec: ResearchRunSpec
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    result_contract_version: str = Field(min_length=1)
    metrics: Mapping[str, object]
    report_markdown: str
    tables: Mapping[str, pd.DataFrame]

    @model_validator(mode="after")
    def validate_request_shape(self) -> LabPrepareCandidateRequest:
        if not self.tables:
            raise ValueError("job artifact requires at least one complete table")
        if any(
            not value.strip()
            for value in (
                self.adapter_id,
                self.adapter_version,
                self.result_contract_version,
            )
        ):
            raise ValueError("adapter and result contract identities must not be empty")
        return self


class LabCandidateNamespaceIdentity(LabArtifactModel):
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    ctime_ns: int = Field(ge=0)


class LabCandidateNamespaceGuardIntent(LabArtifactModel):
    schema_version: Literal[1] = 1
    operation_id: UUID
    candidate_name: str = Field(pattern=r"^[0-9a-f]{32}-[0-9a-f]{32}$")
    platform: Literal["darwin", "linux"]
    phase: Literal["armed"] = "armed"
    candidates_identity: LabCandidateNamespaceIdentity
    candidate_identity: LabCandidateNamespaceIdentity
    tables_identity: LabCandidateNamespaceIdentity
    candidates_original_mode: int = Field(ge=0, le=0o777)
    candidate_original_mode: int = Field(ge=0, le=0o777)
    tables_original_mode: int = Field(ge=0, le=0o777)
    candidates_original_flags: int | None = Field(default=None, ge=0)
    candidate_original_flags: int | None = Field(default=None, ge=0)
    tables_original_flags: int | None = Field(default=None, ge=0)
    created_at: datetime

    @model_validator(mode="after")
    def validate_guard_contract(self) -> LabCandidateNamespaceGuardIntent:
        if (
            self.candidates_original_mode,
            self.candidate_original_mode,
            self.tables_original_mode,
        ) != (0o700, 0o700, 0o700):
            raise ValueError("namespace guard requires exact original 0700 modes")
        flags = (
            self.candidates_original_flags,
            self.candidate_original_flags,
            self.tables_original_flags,
        )
        if self.platform == "darwin" and any(value is None for value in flags):
            raise ValueError("Darwin namespace guard requires original inode flags")
        if self.platform == "linux" and any(value is not None for value in flags):
            raise ValueError("Linux namespace guard must not claim Darwin inode flags")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("namespace guard creation time must be timezone-aware")
        return self

    def canonical_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


class LabArtifactRecoveryRecord(LabArtifactModel):
    path: Path
    status: Literal[
        "recoverable",
        "needs_authority",
        "recoverable_torn",
        "invalid",
        "quarantined",
    ]
    job_id: UUID | None = None
    manifest_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    device: int | None = Field(default=None, ge=0)
    inode: int | None = Field(default=None, ge=1)
    file_type: Literal["directory", "regular", "symlink", "other"] | None = None
    reason: str | None = None

    @model_validator(mode="after")
    def validate_path_identity(self) -> LabArtifactRecoveryRecord:
        if (self.device is None) != (self.inode is None):
            raise ValueError("recovery device and inode must appear together")
        if (self.device is None) != (self.file_type is None):
            raise ValueError("recovery identity and file type must appear together")
        if self.status == "invalid":
            if self.job_id is not None or self.manifest_hash is not None:
                raise ValueError("invalid recovery evidence cannot claim logical identity")
            if not self.reason:
                raise ValueError("invalid recovery evidence requires a reason")
            if self.device is None or self.inode is None or self.file_type is None:
                raise ValueError("invalid recovery evidence requires lstat identity")
        if self.status in {"recoverable", "needs_authority", "recoverable_torn"}:
            if (
                self.job_id is None
                or self.manifest_hash is None
                or self.device is None
                or self.inode is None
            ):
                raise ValueError("candidate recovery evidence is incomplete")
            if self.file_type != "directory":
                raise ValueError("candidate recovery evidence must bind a directory")
            if (
                re.fullmatch(
                    rf"{self.job_id.hex}-[0-9a-f]{{32}}",
                    self.path.name,
                )
                is None
            ):
                raise ValueError("candidate recovery path conflicts with job identity")
            if self.status == "recoverable_torn":
                if not self.reason:
                    raise ValueError("torn candidate recovery requires a reason")
            elif self.reason is not None:
                raise ValueError("non-torn candidate recovery must not carry a reason")
        if self.status == "quarantined" and (self.device is None or self.inode is None):
            raise ValueError("quarantined recovery identity is incomplete")
        if self.status == "quarantined" and self.file_type is None:
            raise ValueError("quarantined recovery file type is incomplete")
        if self.status == "quarantined" and (self.job_id is None) != (self.manifest_hash is None):
            raise ValueError("quarantined logical identity must be complete or absent")
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


class _LabLegacyAuthorityEventPayload(LabArtifactModel):
    schema_version: Literal[2] = 2
    event_type: Literal["staged", "published", "abandoned", "invalidated"]
    sequence: int = Field(ge=1)
    previous_hash: str = Field(pattern=_HASH_PATTERN)
    logical_run_id: str = Field(min_length=1)
    operation_id: UUID
    generation: int = Field(ge=1)
    record: LabLegacyArtifactRecord
    occurred_at: datetime

    @model_validator(mode="after")
    def validate_event(self) -> _LabLegacyAuthorityEventPayload:
        if self.logical_run_id != self.record.logical_run_id:
            raise ValueError("legacy authority event logical run does not match record")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("legacy authority event time must be timezone-aware")
        return self

    def canonical_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


class LabLegacyAuthorityEvent(_LabLegacyAuthorityEventPayload):
    event_hash: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def create(
        cls,
        *,
        event_type: Literal["staged", "published", "abandoned", "invalidated"],
        sequence: int,
        previous_hash: str,
        logical_run_id: str,
        operation_id: UUID,
        generation: int,
        record: LabLegacyArtifactRecord,
        occurred_at: datetime,
    ) -> LabLegacyAuthorityEvent:
        payload = _LabLegacyAuthorityEventPayload(
            event_type=event_type,
            sequence=sequence,
            previous_hash=previous_hash,
            logical_run_id=logical_run_id,
            operation_id=operation_id,
            generation=generation,
            record=record,
            occurred_at=occurred_at,
        )
        return cls(
            **payload.model_dump(mode="python"),
            event_hash=_sha256(payload.canonical_json_bytes()),
        )

    @model_validator(mode="after")
    def validate_event_hash(self) -> LabLegacyAuthorityEvent:
        payload = _LabLegacyAuthorityEventPayload.model_validate(
            self.model_dump(mode="python", exclude={"event_hash"})
        )
        if self.event_hash != _sha256(payload.canonical_json_bytes()):
            raise ValueError("legacy authority event hash conflicts with payload")
        return self


class LabLegacyAuthorityHead(LabArtifactModel):
    schema_version: Literal[1] = 1
    sequence: int = Field(ge=0)
    final_hash: str = Field(pattern=_HASH_PATTERN)
    ledger_size: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_genesis(self) -> LabLegacyAuthorityHead:
        if self.sequence == 0 and (
            self.final_hash != _LEGACY_GENESIS_HASH or self.ledger_size != 0
        ):
            raise ValueError("legacy authority genesis head is invalid")
        if self.sequence > 0 and (self.final_hash == _LEGACY_GENESIS_HASH or self.ledger_size == 0):
            raise ValueError("legacy authority non-genesis head is invalid")
        return self

    def canonical_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


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


def _sha256_descriptor(descriptor: int) -> str:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, 1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


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
    mutation_guard: Callable[[], object] | None = None,
) -> int:
    """Open an absolute directory without following any ancestor symlink."""

    absolute = _secure_absolute_path(path)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    child_descriptor = -1
    result = -1
    main_error: BaseException | None = None
    try:
        descriptor = os.open("/", flags)
        for component in absolute.parts[1:]:
            if component in {"", ".", ".."}:
                raise LabArtifactPathError("managed path contains an unsafe component")
            created_or_observed_missing = False
            try:
                child_descriptor = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                created_or_observed_missing = True
                with suppress(FileExistsError):
                    if mutation_guard is not None:
                        mutation_guard()
                    os.mkdir(component, mode=create_mode, dir_fd=descriptor)
                os.fsync(descriptor)
                child_descriptor = os.open(component, flags, dir_fd=descriptor)
            observed = _FileObservation.from_stat(os.fstat(child_descriptor))
            if observed.mode != stat.S_IFDIR:
                raise LabArtifactPathError("managed path component is not a directory")
            if created_or_observed_missing:
                os.fsync(child_descriptor)
            previous_descriptor = descriptor
            descriptor = child_descriptor
            child_descriptor = -1
            _close_descriptor_fail_closed(
                previous_descriptor,
                label="secure directory ancestor descriptor",
            )
        result = descriptor
        descriptor = -1
    except LabArtifactError as exc:
        main_error = exc
    except OSError as exc:
        main_error = LabArtifactPathError(f"managed path ancestor is missing or unsafe: {absolute}")
        main_error.__cause__ = exc
    except BaseException as exc:
        main_error = exc
    finally:
        cleanup_errors: list[BaseException] = []
        for opened_descriptor in (child_descriptor, descriptor):
            if opened_descriptor >= 0:
                try:
                    _close_descriptor_fail_closed(
                        opened_descriptor,
                        label="secure directory descriptor",
                    )
                except BaseException as close_error:
                    cleanup_errors.append(close_error)
        errors = [main_error, *cleanup_errors] if main_error is not None else cleanup_errors
        _raise_collected_errors(
            "secure directory operation and descriptor cleanup failed",
            errors,
        )
    if result < 0:
        raise LabArtifactIntegrityError("secure directory open completed without a descriptor")
    return result


def _write_private_bytes_at(parent_descriptor: int, name: str, payload: bytes) -> None:
    if PurePosixPath(name).name != name or name in {"", ".", ".."}:
        raise LabArtifactPathError("artifact file name is unsafe")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    main_error: BaseException | None = None
    try:
        descriptor = os.open(name, flags, 0o600, dir_fd=parent_descriptor)
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
    except LabArtifactError as exc:
        main_error = exc
    except OSError as exc:
        main_error = LabArtifactIntegrityError("artifact output could not be written safely")
        main_error.__cause__ = exc
    except BaseException as exc:
        main_error = exc
    finally:
        cleanup_errors: list[BaseException] = []
        if descriptor >= 0:
            try:
                _close_descriptor_fail_closed(
                    descriptor,
                    label="private artifact output descriptor",
                )
            except BaseException as close_error:
                cleanup_errors.append(close_error)
        errors = [main_error, *cleanup_errors] if main_error is not None else cleanup_errors
        _raise_collected_errors(
            "private artifact write and descriptor cleanup failed",
            errors,
        )


def _candidate_namespace_flag(descriptor: int) -> tuple[int, int]:
    if sys.platform == "darwin":
        immutable = getattr(stat, "UF_IMMUTABLE", 0)
        if not immutable or not hasattr(os.fstat(descriptor), "st_flags"):
            raise LabArtifactPlatformError("Darwin immutable inode flags are unavailable")
        return int(os.fstat(descriptor).st_flags), immutable
    raise LabArtifactPlatformError(
        "candidate inode flags are only available through Darwin fchflags(2)"
    )


def _set_candidate_namespace_flags(descriptor: int, flags: int) -> None:
    if sys.platform == "darwin":
        libc = ctypes.CDLL(None, use_errno=True)
        fchflags = getattr(libc, "fchflags", None)
        if fchflags is None:
            raise LabArtifactPlatformError("Darwin fchflags(2) is unavailable")
        fchflags.argtypes = [ctypes.c_int, ctypes.c_uint]
        fchflags.restype = ctypes.c_int
        ctypes.set_errno(0)
        if fchflags(descriptor, flags) != 0:
            error = ctypes.get_errno()
            raise LabArtifactPlatformError(
                f"Darwin immutable inode flag update failed: errno={error}"
            )
    else:
        raise LabArtifactPlatformError("candidate inode flag update requires Darwin")
    os.fsync(descriptor)


def _open_empty_private_file_at(parent_descriptor: int, name: str) -> int:
    if PurePosixPath(name).name != name or name in {"", ".", ".."}:
        raise LabArtifactPathError("artifact file name is unsafe")
    descriptor = os.open(
        name,
        os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
        dir_fd=parent_descriptor,
    )
    try:
        opened = _FileObservation.from_stat(os.fstat(descriptor))
        if (
            opened.mode != stat.S_IFREG
            or opened.nlink != 1
            or opened.size != 0
            or stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600
        ):
            raise LabArtifactIntegrityError("artifact placeholder is not a private empty file")
        return descriptor
    except BaseException as error:
        try:
            os.close(descriptor)
        except BaseException as cleanup_error:
            if isinstance(error, Exception) and isinstance(cleanup_error, Exception):
                raise ExceptionGroup(
                    "artifact placeholder validation and close both failed",
                    [error, cleanup_error],
                ) from None
            raise BaseExceptionGroup(
                "artifact placeholder validation and close both failed",
                [error, cleanup_error],
            ) from None
        raise


def _write_bound_payload(
    descriptor: int,
    parent_descriptor: int,
    name: str,
    payload: bytes,
) -> None:
    before = _FileObservation.from_stat(os.fstat(descriptor))
    if before.mode != stat.S_IFREG or before.nlink != 1 or before.size != 0:
        raise LabArtifactIntegrityError("artifact placeholder identity changed before write")
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
    if (
        after != at_path
        or after.mode != stat.S_IFREG
        or after.nlink != 1
        or after.size != len(payload)
        or _sha256(_read_descriptor(descriptor)) != _sha256(payload)
    ):
        raise LabArtifactIntegrityError("artifact output identity changed while writing")


def _open_or_create_private_regular_at(
    parent_descriptor: int,
    name: str,
    *,
    access_flags: int,
    require_private_existing: bool = False,
    mutation_guard: Callable[[], object] | None = None,
) -> tuple[int, bool]:
    if PurePosixPath(name).name != name or name in {"", ".", ".."}:
        raise LabArtifactPathError("managed file name is unsafe")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    created = False
    try:
        if mutation_guard is not None:
            mutation_guard()
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
        permissions = stat.S_IMODE(os.fstat(descriptor).st_mode)
        if not created and not require_private_existing:
            if mutation_guard is not None:
                mutation_guard()
            os.fchmod(descriptor, 0o600)
            permissions = stat.S_IMODE(os.fstat(descriptor).st_mode)
        if permissions != 0o600:
            raise LabArtifactIntegrityError("managed file permissions must be exactly 0600")
        if created:
            os.fsync(descriptor)
            os.fsync(parent_descriptor)
        return descriptor, created
    except BaseException as error:
        try:
            os.close(descriptor)
        except BaseException as cleanup_error:
            if isinstance(error, Exception) and isinstance(cleanup_error, Exception):
                raise ExceptionGroup(
                    "managed file validation and close both failed",
                    [error, cleanup_error],
                ) from None
            raise BaseExceptionGroup(
                "managed file validation and close both failed",
                [error, cleanup_error],
            ) from None
        raise


def _ensure_private_directory(
    path: Path,
    *,
    manage_existing: bool = True,
    require_private_existing: bool = False,
    mutation_guard: Callable[[], object] | None = None,
) -> None:
    existed = True
    descriptor = -1
    main_error: BaseException | None = None
    try:
        try:
            descriptor = _secure_open_directory(path, create=False)
        except LabArtifactPathError as exc:
            if not isinstance(exc.__cause__, FileNotFoundError):
                raise
            existed = False
            descriptor = _secure_open_directory(
                path,
                create=True,
                mutation_guard=mutation_guard,
            )
        if existed and require_private_existing:
            if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o700:
                raise LabArtifactIntegrityError(
                    f"managed directory permissions must be exactly 0700: {path}"
                )
        elif existed and manage_existing:
            if mutation_guard is not None:
                mutation_guard()
            os.fchmod(descriptor, 0o700)
            os.fsync(descriptor)
    except BaseException as exc:
        main_error = exc
    finally:
        cleanup_errors: list[BaseException] = []
        if descriptor >= 0:
            try:
                _close_descriptor_fail_closed(
                    descriptor,
                    label="private directory descriptor",
                )
            except BaseException as close_error:
                cleanup_errors.append(close_error)
        errors = [main_error, *cleanup_errors] if main_error is not None else cleanup_errors
        _raise_collected_errors(
            "private directory operation and descriptor cleanup failed",
            errors,
        )


def _assert_bound_readonly_file(bound: _BoundReadonlyFile, *, label: str) -> None:
    current_parent_descriptor = -1
    main_error: BaseException | None = None
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
    except LabArtifactError as exc:
        main_error = exc
    except OSError as exc:
        main_error = LabArtifactIntegrityError(f"{label} changed while bound")
        main_error.__cause__ = exc
    except BaseException as exc:
        main_error = exc
    finally:
        cleanup_errors: list[BaseException] = []
        if current_parent_descriptor >= 0:
            try:
                _close_descriptor_fail_closed(
                    current_parent_descriptor,
                    label=f"{label} current parent descriptor",
                )
            except BaseException as close_error:
                cleanup_errors.append(close_error)
        errors = [main_error, *cleanup_errors] if main_error is not None else cleanup_errors
        _raise_collected_errors(
            f"{label} identity check and parent cleanup failed",
            errors,
        )


@contextmanager
def _open_bound_readonly_file(path: Path, *, label: str) -> Iterator[_BoundReadonlyFile]:
    parent_descriptor = -1
    descriptor = -1
    bound: _BoundReadonlyFile | None = None
    main_error: BaseException | None = None
    try:
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
        except LabArtifactError:
            raise
        except OSError as exc:
            raise LabArtifactIntegrityError(f"{label} cannot be opened safely") from exc
        caller_error: BaseException | None = None
        try:
            yield bound
        except BaseException as exc:
            caller_error = exc
        try:
            _assert_bound_readonly_file(bound, label=label)
        except BaseException as integrity_error:
            if caller_error is not None:
                raise BaseExceptionGroup(
                    f"{label} caller and identity checks both failed",
                    [caller_error, integrity_error],
                ) from None
            raise
        if caller_error is not None:
            raise caller_error
    except BaseException as exc:
        main_error = exc
    finally:
        cleanup_errors: list[BaseException] = []
        if bound is not None:
            descriptors = (bound.descriptor, bound.parent_descriptor)
        else:
            descriptors = (descriptor, parent_descriptor)
        for opened_descriptor in descriptors:
            if opened_descriptor >= 0:
                try:
                    os.close(opened_descriptor)
                except BaseException as close_error:
                    cleanup_errors.append(close_error)
        errors = [main_error, *cleanup_errors] if main_error is not None else cleanup_errors
        _raise_collected_errors(
            f"{label} operation and descriptor cleanup failed",
            errors,
        )


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
    if value is None:
        return {"$none": True}
    if value is pd.NA:
        return {"$pd_na": True}
    if value is pd.NaT:
        return {"$pd_nat": True}
    if isinstance(value, np.datetime64) and np.isnat(value):
        return {"$datetime_nat": str(value.dtype)}
    if isinstance(value, np.timedelta64) and np.isnat(value):
        return {"$timedelta_nat": str(value.dtype)}
    if isinstance(value, pd.Period):
        return {
            "$period": {
                "frequency": value.freqstr,
                "ordinal": str(value.ordinal),
            }
        }
    if isinstance(value, pd.Interval):
        return {
            "$interval": {
                "closed": value.closed,
                "left": _canonical_table_value(value.left),
                "right": _canonical_table_value(value.right),
            }
        }
    if hasattr(value, "item") and not isinstance(value, (str, bytes, Decimal)):
        with suppress(ValueError, TypeError, AttributeError):
            value = value.item()  # type: ignore[union-attr]
    if isinstance(value, float):
        if math.isnan(value):
            return {"$nan": True}
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


def _dtype_class_name(value: object) -> str:
    dtype_type = type(value)
    return f"{dtype_type.__module__}.{dtype_type.__qualname__}"


def _canonical_dtype_token(value: object) -> str:
    return canonical_json_bytes(_canonical_table_value(value)).decode("ascii")


def _pandas_dtype_identity(dtype: object) -> LabPandasDtypeIdentity:
    common = {
        "pandas_dtype": str(dtype),
        "dtype_repr": repr(dtype),
        "dtype_class": _dtype_class_name(dtype),
    }
    if isinstance(dtype, pd.CategoricalDtype):
        categories = dtype.categories
        return LabPandasDtypeIdentity(
            family="categorical",
            **common,
            categories=tuple(_canonical_dtype_token(value) for value in categories),
            categories_dtype=str(categories.dtype),
            categories_dtype_class=_dtype_class_name(categories.dtype),
            categories_dtype_identity=_pandas_dtype_identity(categories.dtype),
            ordered=dtype.ordered,
        )
    if isinstance(dtype, pd.DatetimeTZDtype):
        return LabPandasDtypeIdentity(
            family="datetime_tz",
            **common,
            timezone=str(dtype.tz),
            unit=dtype.unit,
        )
    if isinstance(dtype, pd.PeriodDtype):
        return LabPandasDtypeIdentity(
            family="period",
            **common,
            period_frequency=dtype._freqstr,
        )
    if isinstance(dtype, pd.IntervalDtype):
        return LabPandasDtypeIdentity(
            family="interval",
            **common,
            interval_subtype_identity=_pandas_dtype_identity(dtype.subtype),
            interval_closed=dtype.closed,
        )
    if isinstance(dtype, pd.StringDtype):
        na_value = dtype.na_value
        if na_value is pd.NA:
            na_value_kind = "pd.NA"
        elif na_value is pd.NaT:
            na_value_kind = "NaT"
        elif isinstance(na_value, float) and math.isnan(na_value):
            na_value_kind = "nan"
        elif na_value is None:
            na_value_kind = "none"
        else:
            na_value_kind = "canonical"
        return LabPandasDtypeIdentity(
            family="extension",
            **common,
            storage=dtype.storage,
            na_value=_canonical_dtype_token(na_value),
            na_value_kind=na_value_kind,
        )
    if isinstance(dtype, pd.api.extensions.ExtensionDtype):
        if (
            re.fullmatch(
                r"(?:U?Int(?:8|16|32|64)|Float(?:32|64)|boolean)",
                str(dtype),
            )
            is None
        ):
            raise TypeError(f"unsupported pandas extension dtype: {dtype!r}")
        storage = getattr(dtype, "storage", None)
        na_value = getattr(dtype, "na_value", None)
        return LabPandasDtypeIdentity(
            family="extension",
            **common,
            storage=str(storage) if storage is not None else None,
            na_value=_canonical_dtype_token(na_value) if na_value is not None else None,
            na_value_kind="pd.NA" if na_value is pd.NA else None,
        )
    numpy_kind = getattr(dtype, "kind", None)
    return LabPandasDtypeIdentity(
        family="numpy",
        **common,
        numpy_kind=str(numpy_kind) if numpy_kind is not None else None,
    )


def _frame_dtype_identities(frame: pd.DataFrame) -> tuple[LabPandasDtypeIdentity, ...]:
    return tuple(_pandas_dtype_identity(dtype) for dtype in frame.dtypes)


def _rebuild_canonical_dtype_token(token: str) -> object:
    try:
        value = strict_json_loads(token)
    except StrictJsonError as exc:
        raise LabArtifactIntegrityError("pandas dtype metadata is not canonical JSON") from exc
    if canonical_json_bytes(value).decode("ascii") != token:
        raise LabArtifactIntegrityError("pandas dtype metadata is not canonical JSON")
    if isinstance(value, dict) and set(value) == {"$timestamp_ns"}:
        return pd.Timestamp(int(value["$timestamp_ns"]))
    if isinstance(value, dict) and set(value) == {"$timestamp_utc_ns"}:
        return pd.Timestamp(int(value["$timestamp_utc_ns"]), tz=UTC)
    if isinstance(value, dict) and set(value) == {"$timedelta_ns"}:
        return pd.Timedelta(int(value["$timedelta_ns"]), unit="ns")
    if isinstance(value, dict) and set(value) == {"$bytes"}:
        return base64.b64decode(value["$bytes"], validate=True)
    if isinstance(value, dict) and set(value) == {"$none"}:
        return None
    if isinstance(value, dict) and set(value) == {"$pd_na"}:
        return pd.NA
    if isinstance(value, dict) and set(value) == {"$pd_nat"}:
        return pd.NaT
    if isinstance(value, dict) and set(value) == {"$datetime_nat"}:
        dtype = value["$datetime_nat"]
        if (
            not isinstance(dtype, str)
            or re.fullmatch(r"datetime64(?:\[[A-Za-z]+\])?", dtype) is None
        ):
            raise LabArtifactIntegrityError("numpy datetime NaT metadata is invalid")
        unit = dtype.removeprefix("datetime64[").removesuffix("]")
        return np.datetime64("NaT", unit) if dtype != "datetime64" else np.datetime64("NaT")
    if isinstance(value, dict) and set(value) == {"$timedelta_nat"}:
        dtype = value["$timedelta_nat"]
        if (
            not isinstance(dtype, str)
            or re.fullmatch(r"timedelta64(?:\[[A-Za-z]+\])?", dtype) is None
        ):
            raise LabArtifactIntegrityError("numpy timedelta NaT metadata is invalid")
        unit = dtype.removeprefix("timedelta64[").removesuffix("]")
        return np.timedelta64("NaT", unit) if dtype != "timedelta64" else np.timedelta64("NaT")
    if isinstance(value, dict) and set(value) == {"$nan"}:
        return float("nan")
    if isinstance(value, dict) and set(value) == {"$period"}:
        period = value["$period"]
        if not isinstance(period, dict) or set(period) != {"frequency", "ordinal"}:
            raise LabArtifactIntegrityError("pandas period dtype metadata is invalid")
        try:
            return pd.Period(ordinal=int(period["ordinal"]), freq=str(period["frequency"]))
        except (TypeError, ValueError) as exc:
            raise LabArtifactIntegrityError("pandas period dtype metadata is invalid") from exc
    if isinstance(value, dict) and set(value) == {"$interval"}:
        interval = value["$interval"]
        if not isinstance(interval, dict) or set(interval) != {"closed", "left", "right"}:
            raise LabArtifactIntegrityError("pandas interval dtype metadata is invalid")
        closed = interval["closed"]
        if closed not in {"left", "right", "both", "neither"}:
            raise LabArtifactIntegrityError("pandas interval closure is invalid")
        try:
            return pd.Interval(
                _rebuild_canonical_dtype_token(
                    canonical_json_bytes(interval["left"]).decode("ascii")
                ),
                _rebuild_canonical_dtype_token(
                    canonical_json_bytes(interval["right"]).decode("ascii")
                ),
                closed=closed,
            )
        except (TypeError, ValueError) as exc:
            raise LabArtifactIntegrityError("pandas interval dtype metadata is invalid") from exc
    if isinstance(value, dict) and set(value) == {"$null"}:
        return None
    return _rebuild_canonical_value(value)


def _pandas_dtype_from_identity(identity: LabPandasDtypeIdentity) -> object:
    try:
        if identity.family == "numpy":
            rebuilt: object = pd.api.types.pandas_dtype(identity.pandas_dtype)
        elif identity.family == "datetime_tz":
            if identity.timezone is None or identity.unit is None:
                raise LabArtifactIntegrityError("timezone dtype identity is incomplete")
            rebuilt = pd.DatetimeTZDtype(unit=identity.unit, tz=identity.timezone)
        elif identity.family == "period":
            if identity.period_frequency is None:
                raise LabArtifactIntegrityError("period dtype identity is incomplete")
            rebuilt = pd.PeriodDtype(identity.period_frequency)
        elif identity.family == "interval":
            if identity.interval_subtype_identity is None or identity.interval_closed is None:
                raise LabArtifactIntegrityError("interval dtype identity is incomplete")
            rebuilt = pd.IntervalDtype(
                subtype=_pandas_dtype_from_identity(identity.interval_subtype_identity),
                closed=identity.interval_closed,
            )
        elif identity.family == "categorical":
            if (
                identity.categories is None
                or identity.categories_dtype_identity is None
                or identity.ordered is None
            ):
                raise LabArtifactIntegrityError("categorical dtype identity is incomplete")
            category_dtype = _pandas_dtype_from_identity(identity.categories_dtype_identity)
            category_values = [_rebuild_canonical_dtype_token(item) for item in identity.categories]
            categories = pd.Index(pd.array(category_values, dtype=category_dtype))
            rebuilt = pd.CategoricalDtype(categories=categories, ordered=identity.ordered)
        elif identity.dtype_class == _dtype_class_name(pd.StringDtype()):
            if identity.storage is None or identity.na_value_kind is None:
                raise LabArtifactIntegrityError("string dtype identity is incomplete")
            if identity.na_value_kind == "pd.NA":
                na_value: object = pd.NA
            elif identity.na_value_kind == "NaT":
                na_value = pd.NaT
            elif identity.na_value_kind == "nan":
                na_value = float("nan")
            elif identity.na_value_kind == "none":
                na_value = None
            else:
                if identity.na_value is None:
                    raise LabArtifactIntegrityError("string NA metadata is incomplete")
                na_value = _rebuild_canonical_dtype_token(identity.na_value)
            rebuilt = pd.StringDtype(storage=identity.storage, na_value=na_value)
        elif identity.family == "extension":
            rebuilt = pd.api.types.pandas_dtype(identity.pandas_dtype)
        else:
            raise LabArtifactIntegrityError("unsupported pandas dtype identity")
    except LabArtifactError:
        raise
    except (TypeError, ValueError) as exc:
        raise LabArtifactIntegrityError(
            f"pandas dtype cannot be reconstructed: {identity.pandas_dtype}"
        ) from exc
    if _pandas_dtype_identity(rebuilt) != identity:
        raise LabArtifactIntegrityError(
            f"pandas dtype identity cannot be reconstructed exactly: {identity.pandas_dtype}"
        )
    return rebuilt


def _restore_manifest_dtypes(
    frame: pd.DataFrame,
    identities: tuple[LabPandasDtypeIdentity, ...],
) -> pd.DataFrame:
    if len(frame.columns) != len(identities):
        raise LabArtifactIntegrityError("Parquet dtype identity shape changed")
    restored = frame.copy(deep=False)
    for position, identity in enumerate(identities):
        column = restored.columns[position]
        try:
            target_dtype = _pandas_dtype_from_identity(identity)
            if identity.family == "categorical":
                values: object = pd.Categorical(
                    restored.iloc[:, position],
                    dtype=target_dtype,
                )
            else:
                values = pd.array(restored.iloc[:, position], dtype=target_dtype)
            restored[column] = pd.Series(values, index=restored.index)
        except (TypeError, ValueError) as exc:
            raise LabArtifactIntegrityError(
                f"Parquet pandas dtype cannot be reconstructed: {column}"
            ) from exc
    if _frame_dtype_identities(restored) != identities:
        raise LabArtifactIntegrityError("Parquet pandas dtype metadata changed")
    return restored


def _table_content_hash(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    writer = CanonicalJsonStreamWriter(digest.update)

    digest.update(b'{"columns":')
    writer.write_value(_canonical_value(list(frame.columns)))
    digest.update(b',"dtype_identities":')
    writer.write_value(_canonical_value(_frame_dtype_identities(frame)))
    digest.update(b',"dtypes":')
    writer.write_value([str(dtype) for dtype in frame.dtypes])
    digest.update(b',"rows":[')
    accessors = tuple(
        PandasJsonColumnAccessor(frame.iloc[:, position]) for position in range(len(frame.columns))
    )
    for row_index in range(len(frame)):
        if row_index:
            digest.update(b",")
        digest.update(b"[")
        for value_index, accessor in enumerate(accessors):
            if value_index:
                digest.update(b",")
            if not accessor.write_canonical_table_value(writer, row_index):
                value = accessor.value(row_index)
                if isinstance(value, bytes):
                    writer.write_ascii(b'{"$bytes":')
                    writer.write_base64_bytes(value)
                    writer.write_ascii(b"}")
                else:
                    writer.write_value(_canonical_table_value(value))
        digest.update(b"]")
    digest.update(b"]}")
    return digest.hexdigest()


def _parse_canonical_json(payload: bytes, *, label: str) -> object:
    try:
        parsed = strict_json_loads(payload)
    except StrictJsonError as exc:
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

    def __init__(
        self,
        root: Path,
        *,
        mutation_guard: Callable[[], object] | None = None,
    ) -> None:
        self.root = _secure_absolute_path(root)
        self.mutation_guard = mutation_guard
        self.candidates_root = self.root / "candidates"
        self.sealed_root = self.root / "sealed"
        self.quarantine_root = self.root / "quarantine"
        self.seal_intents_root = self.root / "seal-intents"
        self.seal_intents_quarantine_root = self.root / "seal-intents-quarantine"
        self.namespace_guard_active_root = self.root / "namespace-guard-active"
        self.namespace_guard_history_root = self.root / "namespace-guard-history"
        self.namespace_guard_quarantine_root = self.root / "namespace-guard-quarantine"
        self.finalization_locks_root = self.root / "finalization-locks"
        self._closed = False
        self._closing = False
        self._preview_activity_count = 0
        self._preview_activity_owners: dict[int, int] = {}
        self._preview_condition = threading.Condition()
        self._poisoned = False
        self._operation_depth = 0
        self._guard_lock_depth = 0
        self._guard_lock_descriptor = -1
        self._guard_lock_identity: _FileObservation | None = None
        self._root_parent_descriptor = -1
        self._root_descriptor = -1
        self._managed_descriptors: dict[Path, int] = {}
        self._process_lock_key: tuple[int, int] | None = None
        self._process_lock_registered = False
        self._process_lock: threading.RLock | None = None
        self._process_lock_entry: _ArtifactProcessLockEntry | None = None
        try:
            _ensure_private_directory(
                self.root,
                manage_existing=False,
                require_private_existing=True,
                mutation_guard=self.mutation_guard,
            )
            for path in (
                self.candidates_root,
                self.sealed_root,
                self.quarantine_root,
                self.seal_intents_root,
                self.seal_intents_quarantine_root,
                self.namespace_guard_active_root,
                self.namespace_guard_history_root,
                self.namespace_guard_quarantine_root,
                self.finalization_locks_root,
            ):
                _ensure_private_directory(
                    path,
                    manage_existing=False,
                    require_private_existing=(path != self.candidates_root),
                    mutation_guard=self.mutation_guard,
                )
            directory_flags = (
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            )
            self._root_parent_descriptor = _secure_open_directory(
                self.root.parent,
                create=False,
            )
            self._root_descriptor = os.open(
                self.root.name,
                directory_flags,
                dir_fd=self._root_parent_descriptor,
            )
            self._root_parent_identity = _FileObservation.from_stat(
                os.fstat(self._root_parent_descriptor)
            )
            self._root_identity = _FileObservation.from_stat(os.fstat(self._root_descriptor))
            lock_key = (self._root_identity.device, self._root_identity.inode)
            with _ARTIFACT_PROCESS_LOCKS_GUARD:
                entry = _ARTIFACT_PROCESS_LOCKS.get(lock_key)
                if entry is None:
                    entry = _ArtifactProcessLockEntry(lock=threading.RLock(), references=0)
                    _ARTIFACT_PROCESS_LOCKS[lock_key] = entry
                entry.references += 1
                self._process_lock = entry.lock
                self._process_lock_entry = entry
                self._process_lock_key = lock_key
                self._process_lock_registered = True
            for child in (
                self.candidates_root,
                self.sealed_root,
                self.quarantine_root,
                self.seal_intents_root,
                self.seal_intents_quarantine_root,
                self.namespace_guard_active_root,
                self.namespace_guard_history_root,
                self.namespace_guard_quarantine_root,
                self.finalization_locks_root,
            ):
                self._managed_descriptors[child] = os.open(
                    child.name,
                    directory_flags,
                    dir_fd=self._root_descriptor,
                )
            self._managed_identities = {
                path: _FileObservation.from_stat(os.fstat(descriptor))
                for path, descriptor in self._managed_descriptors.items()
            }
            self._guard_lock_descriptor, _ = _open_or_create_private_regular_at(
                self._root_descriptor,
                "namespace-guard.lock",
                access_flags=os.O_RDWR,
                require_private_existing=True,
                mutation_guard=self.mutation_guard,
            )
            self._guard_lock_identity = _FileObservation.from_stat(
                os.fstat(self._guard_lock_descriptor)
            )
            with self._exclusive_namespace_guard(allow_poisoned=True):
                self._guard_mutation()
                self._recover_active_namespace_guards()
            self._assert_managed_roots()
            with self._process_lock:
                self._process_lock_entry.poisoned = False
        except BaseException as error:
            cleanup_errors: list[BaseException] = []
            try:
                self.close()
            except BaseException as cleanup_error:
                cleanup_errors.append(cleanup_error)
            if cleanup_errors:
                _raise_collected_errors(
                    "artifact store initialization and cleanup both failed",
                    [error, *cleanup_errors],
                )
            raise

    def _guard_mutation(self) -> None:
        if self.mutation_guard is not None:
            self.mutation_guard()

    def close(self) -> None:
        condition = getattr(self, "_preview_condition", None)
        if condition is None:
            self._close_resources()
            return
        with condition:
            current_thread_id = threading.get_ident()
            owned_activity_depth = getattr(self, "_preview_activity_owners", {}).get(
                current_thread_id,
                0,
            )
            if owned_activity_depth:
                raise LabArtifactLifecycleError(
                    "artifact store cannot close from a thread that owns preview activity"
                )
            while self._closing and not self._closed:
                condition.wait()
            if self._closed:
                return
            self._closing = True
            while self._preview_activity_count:
                condition.wait()
        try:
            self._close_resources()
        except BaseException:
            with condition:
                self._closing = False
                condition.notify_all()
            raise
        with condition:
            condition.notify_all()

    def _close_resources(self) -> None:
        if getattr(self, "_closed", False):
            return
        process_lock = getattr(self, "_process_lock", None)
        if process_lock is None:
            cleanup_errors: list[BaseException] = []
            for descriptor in getattr(self, "_managed_descriptors", {}).values():
                try:
                    os.close(descriptor)
                except BaseException as exc:
                    cleanup_errors.append(exc)
            self._managed_descriptors = {}
            for attribute in (
                "_guard_lock_descriptor",
                "_root_descriptor",
                "_root_parent_descriptor",
            ):
                descriptor = getattr(self, attribute, -1)
                if descriptor >= 0:
                    try:
                        os.close(descriptor)
                    except BaseException as exc:
                        cleanup_errors.append(exc)
                    finally:
                        setattr(self, attribute, -1)
            self._process_lock_entry = None
            self._process_lock_key = None
            self._process_lock_registered = False
            self._closed = True
            _raise_collected_errors("artifact store close failed", cleanup_errors)
            return
        cleanup_errors = []
        with process_lock:
            if self._closed:
                return
            if (
                getattr(self, "_operation_depth", 0) > 0
                or getattr(self, "_guard_lock_depth", 0) > 0
            ):
                raise LabArtifactIntegrityError(
                    "artifact store cannot close during an artifact transaction"
                )
            for descriptor in getattr(self, "_managed_descriptors", {}).values():
                try:
                    os.close(descriptor)
                except BaseException as exc:
                    cleanup_errors.append(exc)
            self._managed_descriptors = {}
            for attribute in (
                "_guard_lock_descriptor",
                "_root_descriptor",
                "_root_parent_descriptor",
            ):
                descriptor = getattr(self, attribute, -1)
                if descriptor >= 0:
                    try:
                        os.close(descriptor)
                    except BaseException as exc:
                        cleanup_errors.append(exc)
                    finally:
                        setattr(self, attribute, -1)
            if getattr(self, "_process_lock_registered", False):
                with _ARTIFACT_PROCESS_LOCKS_GUARD:
                    key = self._process_lock_key
                    entry = _ARTIFACT_PROCESS_LOCKS.get(key) if key is not None else None
                    if entry is self._process_lock_entry and entry is not None:
                        entry.references -= 1
                        if entry.references == 0 and key is not None:
                            del _ARTIFACT_PROCESS_LOCKS[key]
                    else:
                        cleanup_errors.append(
                            LabArtifactIntegrityError(
                                "artifact store process lock registry changed before close"
                            )
                        )
                self._process_lock_registered = False
            self._process_lock_entry = None
            self._process_lock_key = None
            self._process_lock = None
            self._closed = True
        _raise_collected_errors("artifact store close failed", cleanup_errors)

    def __del__(self) -> None:
        with suppress(BaseException):
            self.close()

    @property
    def poisoned(self) -> bool:
        entry = self._process_lock_entry
        return self._poisoned or (entry is not None and entry.poisoned)

    def _assert_store_operational(self) -> None:
        if self._closed:
            raise LabArtifactIntegrityError("artifact store is closed")
        entry = self._process_lock_entry
        if self._poisoned or entry is None or entry.poisoned:
            raise LabArtifactIntegrityError("artifact store is poisoned")

    @contextmanager
    def _preview_activity(self) -> Iterator[None]:
        current_thread_id = threading.get_ident()
        with self._preview_condition:
            if self._closing or self._closed:
                raise LabArtifactIntegrityError("artifact store is closing or closed")
            self._assert_store_operational()
            self._preview_activity_count += 1
            self._preview_activity_owners[current_thread_id] = (
                self._preview_activity_owners.get(current_thread_id, 0) + 1
            )
        try:
            yield
        finally:
            with self._preview_condition:
                owned_activity_depth = self._preview_activity_owners.get(current_thread_id, 0)
                if owned_activity_depth <= 0 or self._preview_activity_count <= 0:
                    raise LabArtifactLifecycleError(
                        "preview activity ownership changed before release"
                    )
                if owned_activity_depth == 1:
                    del self._preview_activity_owners[current_thread_id]
                else:
                    self._preview_activity_owners[current_thread_id] = owned_activity_depth - 1
                self._preview_activity_count -= 1
                if self._preview_activity_count == 0:
                    self._preview_condition.notify_all()

    def _assert_finalization_lock_identity(
        self,
        *,
        descriptor: int,
        name: str,
        expected: _FileObservation | None = None,
    ) -> _FileObservation:
        self._assert_managed_roots()
        parent_descriptor = self._managed_descriptors[self.finalization_locks_root]
        try:
            opened_stat = os.fstat(descriptor)
            path_stat = os.stat(
                name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise LabArtifactFinalizationLockError(
                "finalization lock identity is unavailable"
            ) from exc
        opened = _FileObservation.from_stat(opened_stat)
        at_path = _FileObservation.from_stat(path_stat)
        if (
            not self._same_finalization_lock_identity(opened, at_path)
            or (
                expected is not None and not self._same_finalization_lock_identity(opened, expected)
            )
            or opened.mode != stat.S_IFREG
            or opened.nlink != 1
            or opened.size != 0
            or stat.S_IMODE(opened_stat.st_mode) != 0o600
            or stat.S_IMODE(path_stat.st_mode) != 0o600
        ):
            raise LabArtifactFinalizationLockError(
                "finalization lock is not a secure regular file or changed identity"
            )
        return opened

    @staticmethod
    def _same_finalization_lock_identity(
        left: _FileObservation,
        right: _FileObservation,
    ) -> bool:
        # Lock acquisition can overlap first creation; ctime/mtime are not
        # authority, while inode, link count, type and zero length are.
        return (
            left.device,
            left.inode,
            left.mode,
            left.nlink,
            left.size,
        ) == (
            right.device,
            right.inode,
            right.mode,
            right.nlink,
            right.size,
        )

    def _open_finalization_lock_descriptor(self, name: str) -> int:
        nofollow = getattr(os, "O_NOFOLLOW", None)
        if nofollow is None:
            raise LabArtifactFinalizationLockError(
                "finalization lock requires secure no-follow file opens"
            )
        parent_descriptor = self._managed_descriptors[self.finalization_locks_root]
        flags = os.O_RDWR | nofollow
        created = False
        try:
            try:
                descriptor = os.open(
                    name,
                    flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=parent_descriptor,
                )
                created = True
            except FileExistsError:
                descriptor = os.open(name, flags, dir_fd=parent_descriptor)
        except OSError as exc:
            raise LabArtifactFinalizationLockError(
                "finalization lock is not a secure regular file"
            ) from exc
        try:
            self._assert_finalization_lock_identity(
                descriptor=descriptor,
                name=name,
            )
            if created:
                os.fsync(descriptor)
                os.fsync(parent_descriptor)
            return descriptor
        except BaseException as error:
            try:
                _close_descriptor_fail_closed(
                    descriptor,
                    label="invalid finalization lock descriptor",
                )
            except BaseException as cleanup_error:
                _raise_collected_errors(
                    "finalization lock validation and close both failed",
                    [error, cleanup_error],
                )
            raise

    @staticmethod
    def _acquire_finalization_flock(
        descriptor: int,
        *,
        deadline: float,
        poll_interval_seconds: float,
    ) -> None:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except InterruptedError:
                pass
            except BlockingIOError:
                pass
            except OSError as exc:
                if exc.errno not in {errno.EAGAIN, errno.EWOULDBLOCK, errno.EINTR}:
                    raise LabArtifactFinalizationLockError(
                        "finalization lock acquisition failed"
                    ) from exc
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LabArtifactFinalizationLockTimeoutError(
                    "finalization lock acquisition timed out"
                )
            time.sleep(min(poll_interval_seconds, remaining))

    @contextmanager
    def finalization_identity_lock(
        self,
        *,
        job_id: UUID,
        manifest_hash: str,
        timeout_seconds: float,
        poll_interval_seconds: float = 0.01,
    ) -> Iterator[None]:
        """Serialize one result's recover/prepare/seal decision across processes."""

        if not isinstance(job_id, UUID):
            raise TypeError("job_id must be a UUID")
        if re.fullmatch(_HASH_PATTERN, manifest_hash) is None:
            raise ValueError("manifest_hash must be a lowercase SHA256")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be a positive finite number")
        if (
            isinstance(poll_interval_seconds, bool)
            or not isinstance(poll_interval_seconds, (int, float))
            or not math.isfinite(poll_interval_seconds)
            or poll_interval_seconds <= 0
        ):
            raise ValueError("poll_interval_seconds must be a positive finite number")
        deadline = time.monotonic() + timeout_seconds
        lock_name = f"{job_id.hex}-{manifest_hash}.lock"
        process_key = (
            self._root_identity.device,
            self._root_identity.inode,
            job_id.hex,
            manifest_hash,
        )
        with self._preview_activity():
            with _FINALIZATION_PROCESS_LOCKS_GUARD:
                entry = _FINALIZATION_PROCESS_LOCKS.get(process_key)
                if entry is None:
                    entry = _FinalizationProcessLockEntry(
                        lock=threading.Lock(),
                        references=0,
                    )
                    _FINALIZATION_PROCESS_LOCKS[process_key] = entry
                entry.references += 1
            process_lock_acquired = False
            descriptor = -1
            flock_acquired = False
            operation_error: BaseException | None = None
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not entry.lock.acquire(timeout=remaining):
                    raise LabArtifactFinalizationLockTimeoutError(
                        "finalization lock acquisition timed out"
                    )
                process_lock_acquired = True
                descriptor = self._open_finalization_lock_descriptor(lock_name)
                expected = self._assert_finalization_lock_identity(
                    descriptor=descriptor,
                    name=lock_name,
                )
                self._acquire_finalization_flock(
                    descriptor,
                    deadline=deadline,
                    poll_interval_seconds=poll_interval_seconds,
                )
                flock_acquired = True
                self._assert_finalization_lock_identity(
                    descriptor=descriptor,
                    name=lock_name,
                    expected=expected,
                )
                yield
            except BaseException as exc:
                operation_error = exc
            cleanup_errors: list[BaseException] = []
            if flock_acquired:
                try:
                    self._assert_finalization_lock_identity(
                        descriptor=descriptor,
                        name=lock_name,
                    )
                except BaseException as exc:
                    cleanup_errors.append(exc)
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                except BaseException as exc:
                    cleanup_errors.append(exc)
            if descriptor >= 0:
                try:
                    _close_descriptor_fail_closed(
                        descriptor,
                        label="finalization lock descriptor",
                    )
                except BaseException as exc:
                    cleanup_errors.append(exc)
            if process_lock_acquired:
                try:
                    entry.lock.release()
                except BaseException as exc:
                    cleanup_errors.append(exc)
            with _FINALIZATION_PROCESS_LOCKS_GUARD:
                registered = _FINALIZATION_PROCESS_LOCKS.get(process_key)
                if registered is entry:
                    entry.references -= 1
                    if entry.references == 0:
                        del _FINALIZATION_PROCESS_LOCKS[process_key]
                else:
                    cleanup_errors.append(
                        LabArtifactFinalizationLockError(
                            "finalization process lock registry changed"
                        )
                    )
            errors = (
                [operation_error, *cleanup_errors]
                if operation_error is not None
                else cleanup_errors
            )
            _raise_collected_errors(
                "finalization operation and lock cleanup failed",
                errors,
            )

    def _mark_store_poisoned(self) -> None:
        self._poisoned = True
        if self._process_lock_entry is not None:
            self._process_lock_entry.poisoned = True

    def _assert_no_active_namespace_guard_authority(self) -> None:
        active_descriptor = self._managed_descriptors[self.namespace_guard_active_root]
        try:
            entries = os.listdir(active_descriptor)
        except OSError as exc:
            raise LabArtifactIntegrityError("namespace guard authority is unavailable") from exc
        if entries:
            raise _LabArtifactActiveGuardError(
                "artifact store has active durable namespace guard authority"
            )

    @contextmanager
    def _artifact_operation_lifecycle(
        self,
        *,
        prepare: bool,
    ) -> Iterator[None]:
        if self._closed:
            raise LabArtifactIntegrityError("artifact store is closed")
        process_lock = self._process_lock
        entry = self._process_lock_entry
        if process_lock is None or entry is None:
            raise LabArtifactIntegrityError("artifact store is closed")
        with process_lock:
            self._assert_store_operational()
            current_thread_id = threading.get_ident()
            if prepare and entry.lifecycle_owner_thread_id == current_thread_id:
                raise LabArtifactIntegrityError("reentrant prepare operation is forbidden")
            if prepare and entry.prepare_owner_thread_id is not None:
                raise LabArtifactIntegrityError("prepare lifecycle ownership is inconsistent")
            outermost = entry.lifecycle_depth == 0
            lifecycle_lock_acquired = False
            if outermost:
                try:
                    self._assert_namespace_guard_lock_identity()
                    _acquire_exclusive_flock(
                        self._guard_lock_descriptor,
                        label="artifact lifecycle lock",
                    )
                    lifecycle_lock_acquired = True
                    self._assert_namespace_guard_lock_identity()
                    self._assert_store_operational()
                    self._assert_no_active_namespace_guard_authority()
                except BaseException as acquire_error:
                    acquire_errors = [acquire_error]
                    if lifecycle_lock_acquired:
                        try:
                            fcntl.flock(self._guard_lock_descriptor, fcntl.LOCK_UN)
                        except BaseException as unlock_error:
                            acquire_errors.append(unlock_error)
                    entry.lifecycle_owner_thread_id = None
                    entry.prepare_owner_thread_id = None
                    entry.lifecycle_depth = 0
                    self._operation_depth = 0
                    _raise_collected_errors(
                        "artifact lifecycle acquisition and cleanup both failed",
                        acquire_errors,
                    )
                entry.lifecycle_owner_thread_id = current_thread_id
            elif entry.lifecycle_owner_thread_id != current_thread_id:
                raise LabArtifactIntegrityError("artifact lifecycle ownership is inconsistent")
            if prepare:
                entry.prepare_owner_thread_id = current_thread_id
            entry.lifecycle_depth += 1
            self._operation_depth += 1
            operation_error: BaseException | None = None
            try:
                yield
            except BaseException as exc:
                operation_error = exc
            self._operation_depth -= 1
            entry.lifecycle_depth -= 1
            if prepare:
                entry.prepare_owner_thread_id = None
            errors: list[BaseException] = []
            if operation_error is not None:
                errors.append(operation_error)
            if outermost:
                try:
                    self._assert_namespace_guard_lock_identity()
                    self._assert_no_active_namespace_guard_authority()
                except BaseException as exc:
                    self._mark_store_poisoned()
                    if operation_error is None or not isinstance(
                        exc,
                        _LabArtifactActiveGuardError,
                    ):
                        errors.append(exc)
                entry.lifecycle_owner_thread_id = None
                try:
                    fcntl.flock(self._guard_lock_descriptor, fcntl.LOCK_UN)
                except BaseException as unlock_error:
                    errors.append(unlock_error)
            _raise_collected_errors(
                "artifact operation and lifecycle cleanup failed",
                errors,
            )

    def _assert_namespace_guard_lock_identity(self) -> None:
        if self._guard_lock_descriptor < 0 or self._guard_lock_identity is None:
            raise LabArtifactIntegrityError("namespace guard lock is unavailable")
        try:
            opened_stat = os.fstat(self._guard_lock_descriptor)
            path_stat = os.stat(
                "namespace-guard.lock",
                dir_fd=self._root_descriptor,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise LabArtifactIntegrityError("namespace guard lock identity is unavailable") from exc
        opened = _FileObservation.from_stat(opened_stat)
        at_path = _FileObservation.from_stat(path_stat)
        if (
            opened != self._guard_lock_identity
            or at_path != opened
            or opened.mode != stat.S_IFREG
            or opened.nlink != 1
            or stat.S_IMODE(opened_stat.st_mode) != 0o600
            or stat.S_IMODE(path_stat.st_mode) != 0o600
        ):
            raise LabArtifactIntegrityError("namespace guard lock identity changed")

    @contextmanager
    def _exclusive_namespace_guard(
        self,
        *,
        allow_poisoned: bool = False,
    ) -> Iterator[None]:
        if self._closed:
            raise LabArtifactIntegrityError("artifact store is closed")
        process_lock = self._process_lock
        entry = self._process_lock_entry
        if process_lock is None or entry is None:
            raise LabArtifactIntegrityError("artifact store is closed")
        with process_lock:
            if (self._poisoned or entry.poisoned) and not allow_poisoned:
                raise LabArtifactIntegrityError("artifact store is poisoned")
            current_thread_id = threading.get_ident()
            if entry.owner_thread_id == current_thread_id:
                raise LabArtifactIntegrityError("reentrant namespace guard operation is forbidden")
            if self._guard_lock_descriptor < 0:
                raise LabArtifactIntegrityError("namespace guard lock is unavailable")
            outermost = self._guard_lock_depth == 0
            if not outermost:
                raise LabArtifactIntegrityError("namespace guard depth is inconsistent")
            lifecycle_owned = (
                entry.lifecycle_depth > 0 and entry.lifecycle_owner_thread_id == current_thread_id
            )
            guard_lock_acquired = False
            if outermost:
                try:
                    self._assert_namespace_guard_lock_identity()
                    if not lifecycle_owned:
                        _acquire_exclusive_flock(
                            self._guard_lock_descriptor,
                            label="namespace guard lock",
                        )
                        guard_lock_acquired = True
                        self._assert_namespace_guard_lock_identity()
                except BaseException as acquire_error:
                    acquire_errors = [acquire_error]
                    if guard_lock_acquired:
                        try:
                            fcntl.flock(self._guard_lock_descriptor, fcntl.LOCK_UN)
                        except BaseException as unlock_error:
                            acquire_errors.append(unlock_error)
                    entry.owner_thread_id = None
                    self._guard_lock_depth = 0
                    _raise_collected_errors(
                        "namespace guard acquisition and cleanup both failed",
                        acquire_errors,
                    )
                entry.owner_thread_id = current_thread_id
            self._guard_lock_depth += 1
            operation_error: BaseException | None = None
            try:
                yield
            except BaseException as exc:
                operation_error = exc
            self._guard_lock_depth -= 1
            errors: list[BaseException] = []
            if operation_error is not None:
                errors.append(operation_error)
            if outermost:
                try:
                    self._assert_namespace_guard_lock_identity()
                except BaseException as exc:
                    self._mark_store_poisoned()
                    errors.append(exc)
                entry.owner_thread_id = None
                if not lifecycle_owned:
                    try:
                        fcntl.flock(self._guard_lock_descriptor, fcntl.LOCK_UN)
                    except BaseException as unlock_error:
                        errors.append(unlock_error)
            _raise_collected_errors(
                "namespace guard operation and cleanup failed",
                errors,
            )

    @staticmethod
    def _namespace_identity(observed: _FileObservation) -> LabCandidateNamespaceIdentity:
        if observed.mode != stat.S_IFDIR:
            raise LabArtifactIntegrityError("namespace guard identity is not a directory")
        return LabCandidateNamespaceIdentity(
            device=observed.device,
            inode=observed.inode,
            size=observed.size,
            mtime_ns=observed.mtime_ns,
            ctime_ns=observed.ctime_ns,
        )

    @staticmethod
    def _matches_namespace_identity(
        observed: _FileObservation,
        expected: LabCandidateNamespaceIdentity,
    ) -> bool:
        return (
            observed.device,
            observed.inode,
            observed.mode,
            observed.size,
            observed.mtime_ns,
        ) == (
            expected.device,
            expected.inode,
            stat.S_IFDIR,
            expected.size,
            expected.mtime_ns,
        ) and observed.ctime_ns >= expected.ctime_ns

    def _create_namespace_guard_intent(
        self,
        *,
        candidate_name: str,
        candidates_descriptor: int,
        candidate_descriptor: int,
        tables_descriptor: int,
    ) -> LabCandidateNamespaceGuardIntent:
        candidates = _FileObservation.from_stat(os.fstat(candidates_descriptor))
        candidate = _FileObservation.from_stat(os.fstat(candidate_descriptor))
        tables = _FileObservation.from_stat(os.fstat(tables_descriptor))
        modes = tuple(
            stat.S_IMODE(os.fstat(descriptor).st_mode)
            for descriptor in (
                candidates_descriptor,
                candidate_descriptor,
                tables_descriptor,
            )
        )
        if modes != (0o700, 0o700, 0o700):
            raise LabArtifactIntegrityError(
                "namespace guard can only arm from exact 0700 directory modes"
            )
        platform = self._namespace_guard_platform()
        if platform == "darwin":
            flags: tuple[int | None, int | None, int | None] = (
                _candidate_namespace_flag(candidates_descriptor)[0],
                _candidate_namespace_flag(candidate_descriptor)[0],
                _candidate_namespace_flag(tables_descriptor)[0],
            )
        else:
            flags = (None, None, None)
        return LabCandidateNamespaceGuardIntent(
            operation_id=uuid4(),
            candidate_name=candidate_name,
            platform=platform,
            candidates_identity=self._namespace_identity(candidates),
            candidate_identity=self._namespace_identity(candidate),
            tables_identity=self._namespace_identity(tables),
            candidates_original_mode=modes[0],
            candidate_original_mode=modes[1],
            tables_original_mode=modes[2],
            candidates_original_flags=flags[0],
            candidate_original_flags=flags[1],
            tables_original_flags=flags[2],
            created_at=datetime.now(UTC),
        )

    @staticmethod
    def _namespace_guard_platform() -> Literal["darwin", "linux"]:
        if sys.platform == "darwin":
            return "darwin"
        if sys.platform.startswith("linux"):
            return "linux"
        raise LabArtifactPlatformError("candidate creation requires a durable namespace guard")

    @staticmethod
    def _guard_intent_name(intent: LabCandidateNamespaceGuardIntent) -> str:
        return f"{intent.candidate_name}.json"

    def _publish_namespace_guard_intent(
        self,
        intent: LabCandidateNamespaceGuardIntent,
    ) -> None:
        active_descriptor = self._managed_descriptors[self.namespace_guard_active_root]
        temporary_name = f".{intent.candidate_name}.{intent.operation_id.hex}.tmp"
        final_name = self._guard_intent_name(intent)
        payload = intent.canonical_json_bytes()
        self._guard_mutation()
        _write_private_bytes_at(active_descriptor, temporary_name, payload)
        temporary_descriptor = os.open(
            temporary_name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=active_descriptor,
        )
        try:
            observed = _FileObservation.from_stat(os.fstat(temporary_descriptor))
            if observed.mode != stat.S_IFREG or observed.nlink != 1:
                raise LabArtifactIntegrityError("namespace guard temp is unsafe")
            rebuilt = strict_model_validate_canonical_json(
                LabCandidateNamespaceGuardIntent,
                _read_descriptor(temporary_descriptor),
            )
            if rebuilt != intent or _read_descriptor(temporary_descriptor) != payload:
                raise LabArtifactIntegrityError("namespace guard temp is not canonical")
            try:
                self._guard_mutation()
                _rename_noreplace(
                    active_descriptor,
                    temporary_name,
                    active_descriptor,
                    final_name,
                )
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    raise LabArtifactConflictError("namespace guard intent already exists") from exc
                raise
            at_final = _FileObservation.from_stat(
                os.stat(final_name, dir_fd=active_descriptor, follow_symlinks=False)
            )
            if at_final != _FileObservation.from_stat(os.fstat(temporary_descriptor)):
                raise LabArtifactIntegrityError(
                    "namespace guard intent identity changed during publication"
                )
            os.fsync(active_descriptor)
        finally:
            os.close(temporary_descriptor)

    def _load_namespace_guard_intent(
        self,
        name: str,
    ) -> LabCandidateNamespaceGuardIntent:
        active_descriptor = self._managed_descriptors[self.namespace_guard_active_root]
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=active_descriptor,
        )
        try:
            observed = _FileObservation.from_stat(os.fstat(descriptor))
            if (
                observed.mode != stat.S_IFREG
                or observed.nlink != 1
                or stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600
            ):
                raise LabArtifactIntegrityError("namespace guard intent is unsafe")
            payload = _read_descriptor(descriptor)
            try:
                intent = strict_model_validate_canonical_json(
                    LabCandidateNamespaceGuardIntent, payload
                )
            except Exception as exc:
                raise LabArtifactIntegrityError("namespace guard intent is invalid") from exc
            if payload != intent.canonical_json_bytes() or name != self._guard_intent_name(intent):
                raise LabArtifactIntegrityError("namespace guard intent is not canonical")
            return intent
        finally:
            os.close(descriptor)

    def _assert_namespace_guard_identities(
        self,
        intent: LabCandidateNamespaceGuardIntent,
        *,
        candidates_descriptor: int,
        candidate_descriptor: int,
        tables_descriptor: int,
    ) -> None:
        try:
            candidates_fd = _FileObservation.from_stat(os.fstat(candidates_descriptor))
            candidates_path = _FileObservation.from_stat(
                os.stat(
                    self.candidates_root.name,
                    dir_fd=self._root_descriptor,
                    follow_symlinks=False,
                )
            )
            candidate_fd = _FileObservation.from_stat(os.fstat(candidate_descriptor))
            candidate_path = _FileObservation.from_stat(
                os.stat(
                    intent.candidate_name,
                    dir_fd=candidates_descriptor,
                    follow_symlinks=False,
                )
            )
            tables_fd = _FileObservation.from_stat(os.fstat(tables_descriptor))
            tables_path = _FileObservation.from_stat(
                os.stat("tables", dir_fd=candidate_descriptor, follow_symlinks=False)
            )
        except OSError as exc:
            raise LabArtifactIntegrityError(
                "namespace guard directory identity is unavailable"
            ) from exc
        checks = (
            (candidates_fd, candidates_path, intent.candidates_identity),
            (candidate_fd, candidate_path, intent.candidate_identity),
            (tables_fd, tables_path, intent.tables_identity),
        )
        if any(
            opened != at_path or not self._matches_namespace_identity(opened, expected)
            for opened, at_path, expected in checks
        ):
            raise LabArtifactIntegrityError("namespace guard directory identity changed")

    def _activate_candidate_namespace_guard(
        self,
        intent: LabCandidateNamespaceGuardIntent,
        *,
        candidates_descriptor: int,
        candidate_descriptor: int,
        tables_descriptor: int,
    ) -> None:
        self._guard_mutation()
        self._assert_namespace_guard_identities(
            intent,
            candidates_descriptor=candidates_descriptor,
            candidate_descriptor=candidate_descriptor,
            tables_descriptor=tables_descriptor,
        )
        if intent.platform == "darwin":
            assert intent.tables_original_flags is not None
            assert intent.candidate_original_flags is not None
            _set_candidate_namespace_flags(
                tables_descriptor,
                intent.tables_original_flags | stat.UF_IMMUTABLE,
            )
            _set_candidate_namespace_flags(
                candidate_descriptor,
                intent.candidate_original_flags | stat.UF_IMMUTABLE,
            )
        else:
            os.fchmod(tables_descriptor, 0o500)
            os.fsync(tables_descriptor)
            os.fchmod(candidate_descriptor, 0o500)
            os.fsync(candidate_descriptor)
            os.fchmod(candidates_descriptor, 0o500)
            os.fsync(candidates_descriptor)

    def _restore_candidate_namespace_guard(
        self,
        intent: LabCandidateNamespaceGuardIntent,
        *,
        candidates_descriptor: int,
        candidate_descriptor: int,
        tables_descriptor: int,
    ) -> None:
        self._guard_mutation()
        self._assert_namespace_guard_identities(
            intent,
            candidates_descriptor=candidates_descriptor,
            candidate_descriptor=candidate_descriptor,
            tables_descriptor=tables_descriptor,
        )
        if intent.platform == "darwin":
            assert intent.tables_original_flags is not None
            assert intent.candidate_original_flags is not None
            assert intent.candidates_original_flags is not None
            _set_candidate_namespace_flags(
                tables_descriptor,
                intent.tables_original_flags,
            )
            _set_candidate_namespace_flags(
                candidate_descriptor,
                intent.candidate_original_flags,
            )
            _set_candidate_namespace_flags(
                candidates_descriptor,
                intent.candidates_original_flags,
            )
        os.fchmod(tables_descriptor, intent.tables_original_mode)
        os.fsync(tables_descriptor)
        os.fchmod(candidate_descriptor, intent.candidate_original_mode)
        os.fsync(candidate_descriptor)
        os.fchmod(candidates_descriptor, intent.candidates_original_mode)
        os.fsync(candidates_descriptor)
        if any(
            stat.S_IMODE(os.fstat(descriptor).st_mode) != expected
            for descriptor, expected in (
                (candidates_descriptor, intent.candidates_original_mode),
                (candidate_descriptor, intent.candidate_original_mode),
                (tables_descriptor, intent.tables_original_mode),
            )
        ):
            raise LabArtifactIntegrityError("namespace guard modes were not restored")
        self._assert_namespace_guard_identities(
            intent,
            candidates_descriptor=candidates_descriptor,
            candidate_descriptor=candidate_descriptor,
            tables_descriptor=tables_descriptor,
        )

    def _archive_namespace_guard_intent(
        self,
        intent: LabCandidateNamespaceGuardIntent,
        *,
        outcome: Literal["published", "aborted"] = "published",
    ) -> None:
        active_descriptor = self._managed_descriptors[self.namespace_guard_active_root]
        history_descriptor = self._managed_descriptors[self.namespace_guard_history_root]
        source_name = self._guard_intent_name(intent)
        suffix = ".json" if outcome == "published" else ".aborted.json"
        target_name = f"{intent.candidate_name}.{intent.operation_id.hex}{suffix}"
        source_descriptor = os.open(
            source_name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=active_descriptor,
        )
        try:
            source = _FileObservation.from_stat(os.fstat(source_descriptor))
            payload = _read_descriptor(source_descriptor)
            if payload != intent.canonical_json_bytes():
                raise LabArtifactIntegrityError("namespace guard intent changed before archive")
            self._guard_mutation()
            _rename_noreplace(
                active_descriptor,
                source_name,
                history_descriptor,
                target_name,
            )
            target = _FileObservation.from_stat(
                os.stat(target_name, dir_fd=history_descriptor, follow_symlinks=False)
            )
            source_after = _FileObservation.from_stat(os.fstat(source_descriptor))
            if target != source_after or not _matches_rename_identity(source, source_after):
                raise LabArtifactIntegrityError("namespace guard history identity changed")
            os.fsync(active_descriptor)
            os.fsync(history_descriptor)
        finally:
            os.close(source_descriptor)

    def _quarantine_namespace_guard_temp(self, name: str) -> None:
        active_descriptor = self._managed_descriptors[self.namespace_guard_active_root]
        quarantine_descriptor = self._managed_descriptors[self.namespace_guard_quarantine_root]
        observed_stat = os.stat(name, dir_fd=active_descriptor, follow_symlinks=False)
        observed = _FileObservation.from_stat(observed_stat)
        if (
            observed.mode != stat.S_IFREG
            or observed.nlink != 1
            or stat.S_IMODE(observed_stat.st_mode) != 0o600
        ):
            raise LabArtifactIntegrityError("namespace guard temp is unsafe")
        target_name = f"{name}.{uuid4().hex}.quarantined"
        self._guard_mutation()
        _rename_noreplace(
            active_descriptor,
            name,
            quarantine_descriptor,
            target_name,
        )
        target = _FileObservation.from_stat(
            os.stat(target_name, dir_fd=quarantine_descriptor, follow_symlinks=False)
        )
        if not _matches_rename_identity(observed, target):
            raise LabArtifactIntegrityError("namespace guard temp quarantine changed identity")
        os.fsync(active_descriptor)
        os.fsync(quarantine_descriptor)

    def _recover_active_namespace_guards(self) -> None:
        active_descriptor = self._managed_descriptors[self.namespace_guard_active_root]
        names = sorted(os.listdir(active_descriptor))
        final_names = [name for name in names if name.endswith(".json")]
        if len(final_names) > 1:
            raise LabArtifactIntegrityError("multiple active namespace guard intents exist")
        for name in names:
            if name.endswith(".json"):
                continue
            if name.startswith(".") and name.endswith(".tmp"):
                self._quarantine_namespace_guard_temp(name)
                continue
            raise LabArtifactIntegrityError("unknown namespace guard ledger entry")
        for name in final_names:
            intent = self._load_namespace_guard_intent(name)
            candidates_descriptor = os.dup(self._managed_descriptors[self.candidates_root])
            candidate_descriptor = -1
            tables_descriptor = -1
            try:
                directory_flags = (
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
                )
                candidate_descriptor = os.open(
                    intent.candidate_name,
                    directory_flags,
                    dir_fd=candidates_descriptor,
                )
                tables_descriptor = os.open(
                    "tables",
                    directory_flags,
                    dir_fd=candidate_descriptor,
                )
                self._restore_candidate_namespace_guard(
                    intent,
                    candidates_descriptor=candidates_descriptor,
                    candidate_descriptor=candidate_descriptor,
                    tables_descriptor=tables_descriptor,
                )
                self._archive_namespace_guard_intent(intent, outcome="aborted")
            except Exception:
                self._mark_store_poisoned()
                raise
            finally:
                for descriptor in (
                    tables_descriptor,
                    candidate_descriptor,
                    candidates_descriptor,
                ):
                    if descriptor >= 0:
                        os.close(descriptor)

    @staticmethod
    def _after_candidate_namespace_guarded(
        _intent: LabCandidateNamespaceGuardIntent,
    ) -> None:
        """Fault-injection boundary after durable namespace protection is active."""

    @staticmethod
    def _before_complete_candidate_return(_candidate: LabJobArtifactCandidate) -> None:
        """Fault-injection boundary immediately before candidate publication."""

    def _assert_candidate_publication_not_aborted(self, candidate_name: str) -> None:
        history_descriptor = self._managed_descriptors[self.namespace_guard_history_root]
        prefix = f"{candidate_name}."
        if any(
            name.startswith(prefix) and name.endswith(".aborted.json")
            for name in os.listdir(history_descriptor)
        ):
            raise LabArtifactIntegrityError("candidate publication was aborted")

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

    def _assert_managed_roots(self, *, candidates_permissions: int = 0o700) -> None:
        current_parent_descriptor = -1
        main_error: BaseException | None = None
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
            if (
                stat.S_IMODE(os.fstat(self._root_descriptor).st_mode) != 0o700
                or stat.S_IMODE(
                    os.stat(
                        self.root.name,
                        dir_fd=self._root_parent_descriptor,
                        follow_symlinks=False,
                    ).st_mode
                )
                != 0o700
            ):
                raise LabArtifactIntegrityError(
                    "managed artifact root permissions must be exactly 0700"
                )
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
                expected_permissions = (
                    candidates_permissions if path == self.candidates_root else 0o700
                )
                if (
                    stat.S_IMODE(os.fstat(descriptor).st_mode) != expected_permissions
                    or stat.S_IMODE(
                        os.stat(
                            path.name,
                            dir_fd=self._root_descriptor,
                            follow_symlinks=False,
                        ).st_mode
                    )
                    != expected_permissions
                ):
                    raise LabArtifactIntegrityError(
                        f"managed artifact directory permissions must be exactly 0700: {path.name}"
                    )
        except LabArtifactError as exc:
            main_error = exc
        except (AttributeError, OSError) as exc:
            main_error = LabArtifactIntegrityError("managed artifact root identity changed")
            main_error.__cause__ = exc
        except BaseException as exc:
            main_error = exc
        finally:
            cleanup_errors: list[BaseException] = []
            if current_parent_descriptor >= 0:
                try:
                    _close_descriptor_fail_closed(
                        current_parent_descriptor,
                        label="managed root parent descriptor",
                    )
                except BaseException as close_error:
                    cleanup_errors.append(close_error)
            errors = [main_error, *cleanup_errors] if main_error is not None else cleanup_errors
            _raise_collected_errors(
                "managed root identity check and descriptor cleanup failed",
                errors,
            )

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
        *,
        max_payload_bytes: int | None = None,
    ) -> tuple[bytes, LabJobArtifactFile]:
        try:
            original_dtype_identities = _frame_dtype_identities(frame)
            original_content_hash = _table_content_hash(frame)
        except (TypeError, ValueError) as exc:
            raise LabArtifactIntegrityError(
                f"candidate table has unsupported semantic values: {table_name}"
            ) from exc
        output: io.BytesIO
        if max_payload_bytes is None:
            output = io.BytesIO()
        else:
            output = _BoundedBytesIO(max_payload_bytes=max_payload_bytes)
        frame.to_parquet(output, index=False)
        payload = output.getvalue()
        try:
            persisted = pd.read_parquet(io.BytesIO(payload))
            persisted = _restore_manifest_dtypes(persisted, original_dtype_identities)
        except Exception as exc:
            raise LabArtifactIntegrityError(
                f"candidate table cannot be read: {table_name}"
            ) from exc
        original_shape = (
            len(frame),
            tuple(frame.columns),
            original_dtype_identities,
        )
        persisted_shape = (
            len(persisted),
            tuple(persisted.columns),
            _frame_dtype_identities(persisted),
        )
        if persisted_shape != original_shape:
            raise LabArtifactIntegrityError(
                f"Parquet round-trip changed rows, columns, or dtypes: {table_name}"
            )
        try:
            persisted_content_hash = _table_content_hash(persisted)
        except (TypeError, ValueError) as exc:
            raise LabArtifactIntegrityError(
                f"Parquet round-trip produced unsupported semantic values: {table_name}"
            ) from exc
        if persisted_content_hash != original_content_hash:
            raise LabArtifactIntegrityError(
                f"Parquet round-trip changed canonical content semantics: {table_name}"
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
                    dtypes=tuple(item.pandas_dtype for item in original_dtype_identities),
                    dtype_identities=original_dtype_identities,
                    content_sha256=original_content_hash,
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
        candidates_permissions: int = 0o700,
    ) -> None:
        try:
            self._assert_managed_roots(candidates_permissions=candidates_permissions)
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

    def _plan_candidate(
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
        payload_budget: LabArtifactPayloadBudget | None = None,
    ) -> LabJobArtifactPlan:
        request = LabPrepareCandidateRequest.model_validate(
            {
                "job_id": job_id,
                "spec": spec,
                "plan_hash": plan_hash,
                "adapter_id": adapter_id,
                "adapter_version": adapter_version,
                "result_contract_version": result_contract_version,
                "metrics": metrics,
                "report_markdown": report_markdown,
                "tables": tables,
            }
        )
        spec_bytes = request.spec.canonical_json().encode("utf-8")
        rebuilt_spec = _rebuild_research_run_spec(spec_bytes)
        if rebuilt_spec != request.spec:
            raise LabArtifactIntegrityError("prepare request spec canonical identity changed")
        validated_tables = dict(request.tables)
        if payload_budget is not None and len(validated_tables) > payload_budget.max_table_count:
            raise LabArtifactPayloadLimitError("final artifact table count exceeds payload budget")
        for table_name, frame in validated_tables.items():
            if not isinstance(frame, pd.DataFrame):
                raise TypeError("artifact tables must be pandas DataFrames")
            self._validate_table(table_name, frame)
        metrics_bytes = canonical_json_bytes(dict(request.metrics))
        _validate_metrics_payload(metrics_bytes)
        try:
            report_bytes = request.report_markdown.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise ValueError("report_markdown must be valid UTF-8 text") from exc
        payloads: dict[str, tuple[str, bytes]] = {
            "spec.json": ("application/json", spec_bytes),
            "metrics.json": ("application/json", metrics_bytes),
            "report.md": ("text/markdown; charset=utf-8", report_bytes),
        }
        planned_bytes = sum(len(payload) for _media_type, payload in payloads.values())
        if payload_budget is not None:
            if any(
                len(payload) > payload_budget.max_single_payload_bytes
                for _media_type, payload in payloads.values()
            ):
                raise LabArtifactPayloadLimitError(
                    "final artifact payload exceeds single payload byte budget"
                )
            if planned_bytes > payload_budget.max_total_payload_bytes:
                raise LabArtifactPayloadLimitError(
                    "final artifact payload exceeds total byte budget"
                )
        parquet_payloads: dict[str, tuple[bytes, LabJobArtifactFile]] = {}
        for table_name in sorted(validated_tables):
            max_payload_bytes = None
            if payload_budget is not None:
                remaining = payload_budget.max_total_payload_bytes - planned_bytes
                if remaining < 1:
                    raise LabArtifactPayloadLimitError(
                        "final artifact payload exceeds total byte budget"
                    )
                max_payload_bytes = min(
                    payload_budget.max_single_payload_bytes,
                    remaining,
                )
            if max_payload_bytes is None:
                serialized = self._serialize_parquet(
                    table_name,
                    validated_tables[table_name],
                )
            else:
                serialized = self._serialize_parquet(
                    table_name,
                    validated_tables[table_name],
                    max_payload_bytes=max_payload_bytes,
                )
            parquet_payloads[table_name] = serialized
            planned_bytes += len(serialized[0])
        files = [
            LabJobArtifactFile(
                relative_path=relative_path,
                media_type=media_type,
                size=len(payload),
                sha256=_sha256(payload),
            )
            for relative_path, (media_type, payload) in sorted(payloads.items())
        ]
        files.extend(inventory for _, inventory in parquet_payloads.values())
        ordered_files = tuple(sorted(files, key=lambda item: item.relative_path))
        identity = _complete_result_hash_payload(
            job_id=request.job_id,
            spec_hash=rebuilt_spec.spec_hash,
            plan_hash=request.plan_hash,
            adapter_id=request.adapter_id,
            adapter_version=request.adapter_version,
            result_contract_version=request.result_contract_version,
            code_sha=rebuilt_spec.code_sha,
            dataset_snapshot=rebuilt_spec.dataset_snapshot,
            files=ordered_files,
        )
        manifest = LabJobArtifactManifest(
            job_id=request.job_id,
            spec_hash=rebuilt_spec.spec_hash,
            plan_hash=request.plan_hash,
            adapter_id=request.adapter_id,
            adapter_version=request.adapter_version,
            result_contract_version=request.result_contract_version,
            code_sha=rebuilt_spec.code_sha,
            dataset_snapshot=rebuilt_spec.dataset_snapshot,
            files=ordered_files,
            complete_result_hash=_sha256(canonical_json_bytes(identity)),
        )
        manifest_bytes = manifest.canonical_json_bytes()
        sums = {item.relative_path: item.sha256 for item in manifest.files}
        sums["manifest.json"] = _sha256(manifest_bytes)
        sums_bytes = "".join(
            f"{digest}  {relative_path}\n" for relative_path, digest in sorted(sums.items())
        ).encode("ascii")
        bundle_payloads = {
            relative_path: payload for relative_path, (_media_type, payload) in payloads.items()
        }
        bundle_payloads.update(
            {
                f"tables/{table_name}.parquet": payload
                for table_name, (payload, _inventory) in parquet_payloads.items()
            }
        )
        bundle_payloads["manifest.json"] = manifest_bytes
        bundle_payloads["SHA256SUMS"] = sums_bytes
        if payload_budget is not None:
            if any(
                len(payload) > payload_budget.max_single_payload_bytes
                for payload in bundle_payloads.values()
            ):
                raise LabArtifactPayloadLimitError(
                    "final artifact payload exceeds single payload byte budget"
                )
            if sum(map(len, bundle_payloads.values())) > payload_budget.max_total_payload_bytes:
                raise LabArtifactPayloadLimitError(
                    "final artifact payload exceeds total byte budget"
                )
        return LabJobArtifactPlan(
            job_id=request.job_id,
            manifest=manifest,
            manifest_hash=manifest.manifest_hash,
            payloads=tuple(
                LabArtifactPlannedPayload(relative_path=relative_path, payload=payload)
                for relative_path, payload in sorted(bundle_payloads.items())
            ),
        )

    def preview_candidate(
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
        payload_budget: LabArtifactPayloadBudget | None = None,
    ) -> LabJobArtifactPlan:
        """Compute the exact candidate bytes and identity without filesystem writes."""

        with self._preview_activity():
            return self._plan_candidate(
                job_id=job_id,
                spec=spec,
                plan_hash=plan_hash,
                adapter_id=adapter_id,
                adapter_version=adapter_version,
                result_contract_version=result_contract_version,
                metrics=metrics,
                report_markdown=report_markdown,
                tables=tables,
                payload_budget=payload_budget,
            )

    @_artifact_public_operation(prepare=True)
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
        payload_budget: LabArtifactPayloadBudget | None = None,
    ) -> LabJobArtifactCandidate:
        self._assert_store_operational()
        plan = self._plan_candidate(
            job_id=job_id,
            spec=spec,
            plan_hash=plan_hash,
            adapter_id=adapter_id,
            adapter_version=adapter_version,
            result_contract_version=result_contract_version,
            metrics=metrics,
            report_markdown=report_markdown,
            tables=tables,
            payload_budget=payload_budget,
        )
        return self._prepare_candidate_from_plan(plan)

    @_artifact_public_operation(prepare=True)
    def prepare_candidate_from_plan(
        self,
        plan: LabJobArtifactPlan,
    ) -> LabJobArtifactCandidate:
        self._assert_store_operational()
        validated = LabJobArtifactPlan.model_validate(plan)
        return self._prepare_candidate_from_plan(validated)

    def _prepare_candidate_from_plan(
        self,
        plan: LabJobArtifactPlan,
    ) -> LabJobArtifactCandidate:
        bundle_payloads = {item.relative_path: item.payload for item in plan.payloads}
        candidate_name = f"{plan.job_id.hex}-{uuid4().hex}"
        if re.fullmatch(r"[0-9a-f]{32}-[0-9a-f]{32}", candidate_name) is None:
            raise LabArtifactIntegrityError("validated candidate name is not a safe segment")
        candidate_path = self.candidates_root / candidate_name
        with self._exclusive_namespace_guard():
            self._recover_active_namespace_guards()
            self._assert_managed_roots()
            directory_flags = (
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            )
            candidates_descriptor = self._managed_parent_descriptor(self.candidates_root)
            candidate_descriptor = -1
            tables_descriptor = -1
            payload_descriptors: dict[str, int] = {}
            guard_intent: LabCandidateNamespaceGuardIntent | None = None
            guard_archived = False
            guard_restored = False
            try:
                self._guard_mutation()
                os.mkdir(candidate_name, mode=0o700, dir_fd=candidates_descriptor)
                candidate_descriptor = os.open(
                    candidate_name,
                    directory_flags,
                    dir_fd=candidates_descriptor,
                )
                candidate_identity = _FileObservation.from_stat(os.fstat(candidate_descriptor))
                if candidate_identity.mode != stat.S_IFDIR:
                    raise LabArtifactIntegrityError("candidate output is not a directory")
                if stat.S_IMODE(os.fstat(candidate_descriptor).st_mode) != 0o700:
                    raise LabArtifactIntegrityError("candidate permissions did not become 0700")
                self._after_candidate_directory_bound(
                    candidate_name,
                    candidate_descriptor,
                )
                self._assert_candidate_creation_binding(
                    candidates_descriptor=candidates_descriptor,
                    candidate_name=candidate_name,
                    candidate_descriptor=candidate_descriptor,
                    candidate_identity=candidate_identity,
                )
                self._guard_mutation()
                os.mkdir("tables", mode=0o700, dir_fd=candidate_descriptor)
                tables_descriptor = os.open(
                    "tables",
                    directory_flags,
                    dir_fd=candidate_descriptor,
                )
                tables_identity = _FileObservation.from_stat(os.fstat(tables_descriptor))
                if tables_identity.mode != stat.S_IFDIR:
                    raise LabArtifactIntegrityError("candidate tables output is not a directory")
                if stat.S_IMODE(os.fstat(tables_descriptor).st_mode) != 0o700:
                    raise LabArtifactIntegrityError(
                        "candidate tables permissions did not become 0700"
                    )
                for relative_path in sorted(bundle_payloads):
                    pure = PurePosixPath(relative_path)
                    parent_descriptor = (
                        tables_descriptor
                        if pure.parent.as_posix() == "tables"
                        else candidate_descriptor
                    )
                    self._guard_mutation()
                    payload_descriptors[relative_path] = _open_empty_private_file_at(
                        parent_descriptor,
                        pure.name,
                    )
                os.fsync(tables_descriptor)
                os.fsync(candidate_descriptor)
                os.fsync(candidates_descriptor)
                guard_intent = self._create_namespace_guard_intent(
                    candidate_name=candidate_name,
                    candidates_descriptor=candidates_descriptor,
                    candidate_descriptor=candidate_descriptor,
                    tables_descriptor=tables_descriptor,
                )
                self._publish_namespace_guard_intent(guard_intent)
                try:
                    self._activate_candidate_namespace_guard(
                        guard_intent,
                        candidates_descriptor=candidates_descriptor,
                        candidate_descriptor=candidate_descriptor,
                        tables_descriptor=tables_descriptor,
                    )
                    self._after_candidate_namespace_guarded(guard_intent)
                    guarded_candidates_permissions = (
                        0o700 if guard_intent.platform == "darwin" else 0o500
                    )
                    self._assert_candidate_creation_binding(
                        candidates_descriptor=candidates_descriptor,
                        candidate_name=candidate_name,
                        candidate_descriptor=candidate_descriptor,
                        candidate_identity=candidate_identity,
                        tables_descriptor=tables_descriptor,
                        tables_identity=tables_identity,
                        candidates_permissions=guarded_candidates_permissions,
                    )
                    for relative_path in sorted(bundle_payloads):
                        pure = PurePosixPath(relative_path)
                        parent_descriptor = (
                            tables_descriptor
                            if pure.parent.as_posix() == "tables"
                            else candidate_descriptor
                        )
                        self._guard_mutation()
                        _write_bound_payload(
                            payload_descriptors[relative_path],
                            parent_descriptor,
                            pure.name,
                            bundle_payloads[relative_path],
                        )
                    self._guard_mutation()
                    os.fsync(tables_descriptor)
                    os.fsync(candidate_descriptor)
                    os.fsync(candidates_descriptor)
                    self._restore_candidate_namespace_guard(
                        guard_intent,
                        candidates_descriptor=candidates_descriptor,
                        candidate_descriptor=candidate_descriptor,
                        tables_descriptor=tables_descriptor,
                    )
                    guard_restored = True
                    self._assert_candidate_creation_binding(
                        candidates_descriptor=candidates_descriptor,
                        candidate_name=candidate_name,
                        candidate_descriptor=candidate_descriptor,
                        candidate_identity=candidate_identity,
                        tables_descriptor=tables_descriptor,
                        tables_identity=tables_identity,
                    )
                    for descriptor in payload_descriptors.values():
                        os.close(descriptor)
                    payload_descriptors.clear()
                    os.close(tables_descriptor)
                    tables_descriptor = -1
                    os.close(candidate_descriptor)
                    candidate_descriptor = -1
                    os.close(candidates_descriptor)
                    candidates_descriptor = -1
                    candidate = self._finalize_public_candidate(
                        self._candidate_from_path(candidate_path)
                    )
                    self._before_complete_candidate_return(candidate)
                    self._guard_mutation()
                    self._archive_namespace_guard_intent(
                        guard_intent,
                        outcome="published",
                    )
                    guard_archived = True
                    return candidate
                except BaseException as operation_error:
                    if not guard_archived:
                        try:
                            if not guard_restored:
                                self._restore_candidate_namespace_guard(
                                    guard_intent,
                                    candidates_descriptor=candidates_descriptor,
                                    candidate_descriptor=candidate_descriptor,
                                    tables_descriptor=tables_descriptor,
                                )
                            self._archive_namespace_guard_intent(
                                guard_intent,
                                outcome="aborted",
                            )
                            guard_archived = True
                        except BaseException as cleanup_error:
                            self._mark_store_poisoned()
                            raise BaseExceptionGroup(
                                "candidate operation and namespace guard cleanup both failed",
                                [operation_error, cleanup_error],
                            ) from None
                    raise
            finally:
                for descriptor in payload_descriptors.values():
                    with suppress(OSError):
                        os.close(descriptor)
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
                manifest = strict_model_validate_canonical_json(
                    LabJobArtifactManifest, manifest_bytes
                )
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
        identities: list[LabArtifactFileIdentity] = []
        expected_hashes = self._expected_bound_hashes(manifest)
        manifest_files = {item.relative_path: item for item in manifest.files}
        expected_sums = {item.relative_path: item.sha256 for item in manifest.files}
        expected_sums["manifest.json"] = manifest.manifest_hash
        canonical_sums = "".join(
            f"{digest}  {relative_path}\n"
            for relative_path, digest in sorted(expected_sums.items())
        ).encode("ascii")
        for relative_path in sorted(bound.files):
            item = bound.files[relative_path]
            observed = _FileObservation.from_stat(os.fstat(item.descriptor))
            manifest_file = manifest_files.get(relative_path)
            parquet = manifest_file.parquet if manifest_file is not None else None
            if parquet is None:
                payload = _read_descriptor(item.descriptor)
                digest = _sha256(payload)
            else:
                payload = None
                digest = _sha256_descriptor(item.descriptor)
            self._after_bound_file_read(relative_path, bound)
            if observed.size != item.current.size or digest != expected_hashes[relative_path]:
                raise LabArtifactIntegrityError(f"job artifact bytes conflict: {relative_path}")
            identities.append(self._artifact_identity(relative_path, observed))
            mode = stat.S_IMODE(os.fstat(item.descriptor).st_mode)
            allowed = {0o400} if permission_profile == "sealed" else {0o600}
            if permission_profile == "interrupted":
                allowed = {0o400, 0o600}
            if mode not in allowed:
                raise LabArtifactIntegrityError(
                    f"{permission_profile} artifact file permissions conflict: {relative_path}"
                )
            if relative_path == "manifest.json":
                if payload is None:
                    raise LabArtifactIntegrityError("manifest.json payload was not loaded")
                if payload != manifest.canonical_json_bytes():
                    raise LabArtifactIntegrityError("job artifact manifest bytes conflict")
            elif relative_path == "spec.json":
                if payload is None:
                    raise LabArtifactIntegrityError("spec.json payload was not loaded")
                rebuilt_spec = _rebuild_research_run_spec(payload)
                if rebuilt_spec.spec_hash != manifest.spec_hash:
                    raise LabArtifactIntegrityError("spec.json does not match spec_hash")
                if (
                    rebuilt_spec.code_sha != manifest.code_sha
                    or rebuilt_spec.dataset_snapshot != manifest.dataset_snapshot
                ):
                    raise LabArtifactIntegrityError(
                        "manifest spec identity conflicts with spec.json"
                    )
            elif relative_path == "metrics.json":
                if payload is None:
                    raise LabArtifactIntegrityError("metrics.json payload was not loaded")
                _validate_metrics_payload(payload)
            elif relative_path == "report.md":
                if payload is None:
                    raise LabArtifactIntegrityError("report.md payload was not loaded")
                try:
                    payload.decode("utf-8", errors="strict")
                except UnicodeDecodeError as exc:
                    raise LabArtifactIntegrityError("report.md is not valid UTF-8") from exc
            elif relative_path == "SHA256SUMS":
                if payload is None:
                    raise LabArtifactIntegrityError("SHA256SUMS payload was not loaded")
                if payload != canonical_sums:
                    raise LabArtifactIntegrityError("SHA256SUMS is not canonical or does not match")
            elif parquet is not None:
                try:
                    os.lseek(item.descriptor, 0, os.SEEK_SET)
                    with os.fdopen(os.dup(item.descriptor), "rb") as stream:
                        frame = pd.read_parquet(stream)
                    frame = _restore_manifest_dtypes(frame, parquet.dtype_identities)
                except Exception as exc:
                    raise LabArtifactIntegrityError(
                        f"Parquet artifact cannot be read: {relative_path}"
                    ) from exc
                actual = (
                    len(frame),
                    tuple(frame.columns),
                    tuple(str(dtype) for dtype in frame.dtypes),
                    _frame_dtype_identities(frame),
                    _table_content_hash(frame),
                )
                expected = (
                    parquet.row_count,
                    parquet.columns,
                    parquet.dtypes,
                    parquet.dtype_identities,
                    parquet.content_sha256,
                )
                del frame
                if actual != expected:
                    raise LabArtifactIntegrityError(
                        f"Parquet artifact content conflicts: {relative_path}"
                    )
            else:
                raise LabArtifactIntegrityError(
                    f"job artifact file has no validation contract: {relative_path}"
                )
            if payload is not None:
                del payload
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
        main_error: BaseException | None = None
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
        except LabArtifactIntegrityError as exc:
            main_error = exc
        except OSError as exc:
            if "bound" in locals():
                main_error = exc
            else:
                main_error = LabArtifactIntegrityError(
                    "artifact changed while binding file descriptors"
                )
                main_error.__cause__ = exc
        except BaseException as exc:
            main_error = exc
        finally:
            cleanup_errors: list[BaseException] = []
            if "bound" in locals():
                descriptors = (
                    *(item.descriptor for item in bound.files.values()),
                    bound.tables_descriptor,
                    bound.bundle_descriptor,
                    bound.parent_descriptor,
                )
            else:
                descriptors = (
                    *(item.descriptor for item in opened_files.values()),
                    tables_descriptor,
                    bundle_descriptor,
                    parent_descriptor,
                )
            for opened_descriptor in descriptors:
                if opened_descriptor >= 0:
                    try:
                        os.close(opened_descriptor)
                    except BaseException as close_error:
                        cleanup_errors.append(close_error)
            errors = [main_error, *cleanup_errors] if main_error is not None else cleanup_errors
            _raise_collected_errors(
                "artifact bundle operation and descriptor cleanup failed",
                errors,
            )

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
            observed = _FileObservation.from_stat(os.fstat(item.descriptor))
            if relative_path == "manifest.json":
                payload = _read_descriptor(item.descriptor)
                digest = _sha256(payload)
                if payload != manifest.canonical_json_bytes():
                    raise LabArtifactIntegrityError("bound candidate manifest changed")
            else:
                digest = _sha256_descriptor(item.descriptor)
            if observed.size != item.current.size or digest != expected[relative_path]:
                raise LabArtifactIntegrityError(f"bound artifact bytes changed: {relative_path}")
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
                intent = strict_model_validate_canonical_json(LabArtifactSealIntent, payload)
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
            before_mode = stat.S_IMODE(
                os.stat(name, dir_fd=source_parent, follow_symlinks=False).st_mode
            )
            if before.mode != stat.S_IFREG or before.nlink != 1 or before_mode != 0o600:
                raise LabArtifactIntegrityError("seal intent recovery entry is unsafe")
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=source_parent,
            )
            opened = _FileObservation.from_stat(os.fstat(descriptor))
            if opened != before or stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600:
                raise LabArtifactIntegrityError("seal intent recovery entry identity changed")
            target_name = f"{name.lstrip('.')}.{uuid4().hex}.quarantined"
            self._guard_mutation()
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
        parent_descriptor = -1
        descriptor = -1
        bound: _BoundSealIntent | None = None
        fault_boundary_reached = False
        published_here = False
        main_error: BaseException | None = None
        try:
            parent_descriptor = self._managed_parent_descriptor(self.seal_intents_root)
            name = f"{job_id.hex}.json"
            expected = self._candidate_seal_intent(candidate) if candidate is not None else None
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
                        temporary_intent = strict_model_validate_canonical_json(
                            LabArtifactSealIntent, temporary_payload
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
                        self._guard_mutation()
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
                intent = strict_model_validate_canonical_json(LabArtifactSealIntent, payload)
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
            caller_error: BaseException | None = None
            try:
                yield bound
            except BaseException as exc:
                caller_error = exc
            integrity_error: BaseException | None = None
            try:
                self._assert_bound_seal_intent(bound)
            except BaseException as exc:
                integrity_error = exc
            if caller_error is not None and integrity_error is not None:
                if isinstance(caller_error, Exception) and isinstance(
                    integrity_error,
                    Exception,
                ):
                    raise ExceptionGroup(
                        "seal intent caller and final identity checks both failed",
                        [caller_error, integrity_error],
                    ) from None
                raise BaseExceptionGroup(
                    "seal intent caller and final identity checks both failed",
                    [caller_error, integrity_error],
                ) from None
            if integrity_error is not None:
                raise integrity_error
            if caller_error is not None:
                raise caller_error
        except LabArtifactError as exc:
            main_error = exc
        except OSError as exc:
            if bound is not None or fault_boundary_reached:
                main_error = exc
            else:
                main_error = LabArtifactIntegrityError("job artifact seal intent cannot be bound")
                main_error.__cause__ = exc
        except BaseException as exc:
            main_error = exc
        finally:
            cleanup_errors: list[BaseException] = []
            if bound is not None:
                descriptors = (bound.descriptor, bound.parent_descriptor)
            else:
                descriptors = (descriptor, parent_descriptor)
            for opened_descriptor in descriptors:
                if opened_descriptor >= 0:
                    try:
                        os.close(opened_descriptor)
                    except BaseException as close_error:
                        cleanup_errors.append(close_error)
            errors = [main_error, *cleanup_errors] if main_error is not None else cleanup_errors
            _raise_collected_errors(
                "seal intent operation and descriptor cleanup failed",
                errors,
            )

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
                    self._guard_mutation()
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
            manifest = strict_model_validate_canonical_json(LabJobArtifactManifest, payload)
        except Exception as exc:
            raise LabArtifactIntegrityError("bound candidate manifest is invalid") from exc
        if payload != manifest.canonical_json_bytes():
            raise LabArtifactIntegrityError("bound candidate manifest is not canonical")
        return manifest

    def _finalize_bound_directories(self, bound: _BoundArtifactBundle) -> None:
        self._assert_bound_paths(bound)
        before_tables = _FileObservation.from_stat(os.fstat(bound.tables_descriptor))
        self._guard_mutation()
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
        self._guard_mutation()
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
        self._assert_candidate_publication_not_aborted(managed.name)
        _, preliminary_manifest, _ = self._probe_bundle(
            managed,
            parent_root=self.candidates_root,
        )
        intent_state = self._seal_intent_state(preliminary_manifest.job_id)
        profile: Literal["candidate", "interrupted", "sealed"] = (
            "interrupted"
            if allow_interrupted_seal and intent_state in {"valid", "torn"}
            else "candidate"
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
        candidate = self._defensively_validate_candidate(candidate)
        if allow_interrupted_seal and intent_state == "valid":
            intent = self._load_seal_intent(manifest.job_id)
            if not self._intent_matches_candidate(intent, candidate):
                raise LabArtifactIntegrityError(
                    "candidate seal intent does not bind the interrupted identity"
                )
        return candidate

    def _defensively_validate_candidate(
        self,
        candidate: LabJobArtifactCandidate,
    ) -> LabJobArtifactCandidate:
        try:
            payload = {
                field_name: getattr(candidate, field_name)
                for field_name in LabJobArtifactCandidate.model_fields
            }
            rebuilt = LabJobArtifactCandidate.model_validate(payload)
        except Exception as exc:
            raise LabArtifactIntegrityError("candidate typed identity is invalid") from exc
        managed = self._assert_managed_child(
            rebuilt.path,
            self.candidates_root,
            label="candidate",
        )
        if (
            re.fullmatch(
                rf"{rebuilt.job_id.hex}-[0-9a-f]{{32}}",
                managed.name,
            )
            is None
        ):
            raise LabArtifactIntegrityError("candidate path conflicts with job identity")
        if managed != rebuilt.path:
            rebuilt = rebuilt.model_copy(update={"path": managed})
        return rebuilt

    @staticmethod
    def _defensively_validate_recovery_record(
        record: LabArtifactRecoveryRecord,
    ) -> LabArtifactRecoveryRecord:
        try:
            return LabArtifactRecoveryRecord.model_validate(
                {
                    field_name: getattr(record, field_name)
                    for field_name in LabArtifactRecoveryRecord.model_fields
                }
            )
        except Exception as exc:
            raise LabArtifactIntegrityError("candidate recovery evidence is invalid") from exc

    @staticmethod
    def _defensively_validate_recovery_authority(
        authority: LabArtifactRecoveryAuthority,
    ) -> LabArtifactRecoveryAuthority:
        try:
            return LabArtifactRecoveryAuthority.model_validate(
                {
                    field_name: getattr(authority, field_name)
                    for field_name in LabArtifactRecoveryAuthority.model_fields
                }
            )
        except Exception as exc:
            raise LabArtifactAuthorizationError("external recovery authority is invalid") from exc

    @_artifact_public_operation()
    def verify_candidate(
        self,
        candidate: LabJobArtifactCandidate,
        *,
        allow_interrupted_seal: bool = False,
    ) -> LabJobArtifactManifest:
        self._assert_store_operational()
        finalized = self._finalize_public_candidate(
            candidate,
            allow_interrupted_seal=allow_interrupted_seal,
        )
        return finalized.manifest

    @staticmethod
    def _after_public_candidate_finalized(_candidate: LabJobArtifactCandidate) -> None:
        """Fault-injection boundary while a public candidate remains fd-bound."""

    def _finalize_public_candidate(
        self,
        candidate: LabJobArtifactCandidate,
        *,
        allow_interrupted_seal: bool = False,
    ) -> LabJobArtifactCandidate:
        candidate = self._defensively_validate_candidate(candidate)
        with self._bind_verified_candidate(
            candidate,
            allow_interrupted_seal=allow_interrupted_seal,
        ):
            self._after_public_candidate_finalized(candidate)
            return candidate

    @contextmanager
    def _bind_verified_candidate(
        self,
        candidate: LabJobArtifactCandidate,
        *,
        allow_interrupted_seal: bool,
    ) -> Iterator[LabJobArtifactManifest]:
        candidate = self._defensively_validate_candidate(candidate)
        path = self._assert_managed_child(candidate.path, self.candidates_root, label="candidate")
        intent_state = self._seal_intent_state(candidate.job_id)
        intent = self._load_seal_intent(candidate.job_id) if intent_state == "valid" else None
        intent_matches_candidate = intent is not None and self._intent_matches_candidate(
            intent, candidate
        )
        profile: Literal["candidate", "interrupted", "sealed"] = (
            "interrupted"
            if allow_interrupted_seal and (intent_state == "torn" or intent_matches_candidate)
            else "candidate"
        )
        observed, manifest, identities = self._probe_bundle(
            path,
            parent_root=self.candidates_root,
        )
        with self._bind_bundle(
            parent_root=self.candidates_root,
            bundle_path=path,
            manifest=manifest,
            expected_bundle=observed,
            expected_files=identities,
        ) as bound:
            verified_identities = self._validate_bound_bundle(
                bound,
                manifest,
                permission_profile=profile,
            )
            if not self._same_bundle_identity(bound.current, candidate):
                raise LabArtifactIntegrityError("candidate bundle identity changed")
            if allow_interrupted_seal and intent_matches_candidate and intent is not None:
                current = candidate.model_copy(update={"file_identities": verified_identities})
                if not self._intent_matches_candidate(intent, current):
                    raise LabArtifactIntegrityError(
                        "candidate seal intent does not bind the interrupted identity"
                    )
            if (
                manifest != candidate.manifest
                or manifest.manifest_hash != candidate.manifest_hash
                or verified_identities != candidate.file_identities
            ):
                raise LabArtifactIntegrityError("candidate bundle identity or files changed")
            caller_error: BaseException | None = None
            try:
                try:
                    yield manifest
                except BaseException as exc:
                    caller_error = exc
            finally:
                integrity_error: BaseException | None = None
                try:
                    final_identities = self._validate_bound_bundle(
                        bound,
                        manifest,
                        permission_profile=profile,
                    )
                    if (
                        not self._same_bundle_identity(bound.current, candidate)
                        or final_identities != candidate.file_identities
                    ):
                        raise LabArtifactIntegrityError(
                            "candidate bundle identity changed before return"
                        )
                except BaseException as exc:
                    integrity_error = exc
                if caller_error is not None and integrity_error is not None:
                    if isinstance(caller_error, Exception) and isinstance(
                        integrity_error, Exception
                    ):
                        raise ExceptionGroup(
                            "candidate operation and final verification both failed",
                            [caller_error, integrity_error],
                        )
                    raise BaseExceptionGroup(
                        "candidate operation and final verification both failed",
                        [caller_error, integrity_error],
                    )
                if integrity_error is not None:
                    raise integrity_error
            if caller_error is not None:
                raise caller_error

    @_artifact_public_operation()
    def verify_sealed(self, path: Path) -> LabSealedJobArtifact:
        self._assert_store_operational()
        return self._finalize_public_sealed(path)

    @staticmethod
    def _after_existing_sealed_bound(
        _bound: _BoundArtifactBundle,
        _sealed: LabSealedJobArtifact,
    ) -> None:
        """Fault-injection boundary while an existing sealed bundle remains bound."""

    @staticmethod
    def _after_public_sealed_finalized(_sealed: LabSealedJobArtifact) -> None:
        """Fault-injection boundary before a public sealed identity returns."""

    @staticmethod
    def _before_public_sealed_bind(_path: Path) -> None:
        """Fault-injection boundary before the final public bundle binding."""

    def _finalize_public_sealed(
        self,
        path: Path,
        *,
        expected_manifest: LabJobArtifactManifest | None = None,
        expected_bundle_identity: tuple[int, int] | None = None,
        expected_file_identities: tuple[LabArtifactFileIdentity, ...] | None = None,
        reused_existing: bool = False,
    ) -> LabSealedJobArtifact:
        self._before_public_sealed_bind(path)
        with self._bind_verified_sealed(path) as sealed:
            if expected_manifest is not None and sealed.manifest != expected_manifest:
                raise LabArtifactIntegrityError(
                    "public sealed result conflicts with expected manifest identity"
                )
            if (
                expected_bundle_identity is not None
                and (
                    sealed.device,
                    sealed.inode,
                )
                != expected_bundle_identity
            ):
                raise LabArtifactIntegrityError(
                    "public sealed result conflicts with expected bundle identity"
                )
            if (
                expected_file_identities is not None
                and sealed.file_identities != expected_file_identities
            ):
                raise LabArtifactIntegrityError(
                    "public sealed result conflicts with expected file identities"
                )
            result = sealed.model_copy(update={"reused_existing": reused_existing})
            self._after_public_sealed_finalized(result)
            return result

    def _bound_file_identities(
        self,
        bound: _BoundArtifactBundle,
    ) -> tuple[LabArtifactFileIdentity, ...]:
        return tuple(
            self._artifact_identity(relative_path, bound.files[relative_path].current)
            for relative_path in sorted(bound.files)
        )

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
            sealed = LabSealedJobArtifact(
                path=managed,
                manifest=manifest,
                manifest_hash=manifest.manifest_hash,
                device=bound.current.device,
                inode=bound.current.inode,
                file_identities=identities,
            )
            self._after_existing_sealed_bound(bound, sealed)
            verified_identities = self._validate_bound_bundle(
                bound,
                manifest,
                permission_profile="sealed",
            )
            if managed.name != manifest.job_id.hex:
                raise LabArtifactIntegrityError("sealed path does not match job identity")
            sealed = sealed.model_copy(
                update={"file_identities": verified_identities},
            )
            self._assert_bound_paths(bound)
            caller_error: BaseException | None = None
            try:
                try:
                    yield sealed
                except BaseException as exc:
                    caller_error = exc
            finally:
                integrity_error: BaseException | None = None
                try:
                    self._assert_bound_paths(bound)
                    self._verify_bound_bytes(bound, manifest)
                except BaseException as exc:
                    integrity_error = exc
                if caller_error is not None and integrity_error is not None:
                    if isinstance(caller_error, Exception) and isinstance(
                        integrity_error, Exception
                    ):
                        raise ExceptionGroup(
                            "caller transaction and sealed artifact verification both failed",
                            [caller_error, integrity_error],
                        )
                    raise BaseExceptionGroup(
                        "caller transaction and sealed artifact verification both failed",
                        [caller_error, integrity_error],
                    )
                if integrity_error is not None:
                    raise integrity_error
            if caller_error is not None:
                raise caller_error

    @contextmanager
    def bind_verified_sealed(
        self,
        path: Path,
        *,
        indexed_at: datetime,
    ) -> Iterator[LabVerifiedSealedBinding]:
        """Hold every sealed bundle fd open across a caller-owned transaction."""

        with self._artifact_operation_lifecycle(prepare=False):
            self._assert_store_operational()
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

    @contextmanager
    def artifact_commit_lifecycle(self) -> Iterator[None]:
        """Hold the root lifecycle beyond binding exit through a durable commit."""

        with self._artifact_operation_lifecycle(prepare=False):
            self._assert_store_operational()
            yield

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

    @_artifact_public_operation()
    def seal_candidate(self, candidate: LabJobArtifactCandidate) -> LabSealedJobArtifact:
        self._assert_store_operational()
        self._guard_mutation()
        candidate = self._defensively_validate_candidate(candidate)
        if self.verify_candidate(candidate, allow_interrupted_seal=True) != candidate.manifest:
            raise LabArtifactIntegrityError("candidate manifest changed before seal")
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
                result = existing.model_copy(update={"reused_existing": True})
                self._after_public_sealed_finalized(result)
                return result
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
                    self._guard_mutation()
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
                                    current_candidate = candidate.model_copy(
                                        update={
                                            "file_identities": self._bound_file_identities(bound)
                                        }
                                    )
                                    self.quarantine_candidate(
                                        current_candidate,
                                        reason="idempotent racing sealed bundle reuse",
                                    )
                                    result = existing.model_copy(update={"reused_existing": True})
                                    self._after_public_sealed_finalized(result)
                                    return result
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
                self._assert_bound_seal_intent(bound_intent)
                return self._finalize_public_sealed(
                    target,
                    expected_manifest=candidate.manifest,
                    expected_bundle_identity=(bound.current.device, bound.current.inode),
                    expected_file_identities=self._bound_file_identities(bound),
                )

    @_artifact_public_operation()
    def list_candidate_recovery(self) -> tuple[LabArtifactRecoveryRecord, ...]:
        self._assert_store_operational()
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
                        device=observed.st_dev,
                        inode=observed.st_ino,
                        file_type=_entry_file_type(observed.st_mode),
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
                        file_type="directory",
                        reason=str(exc),
                    )
                )
            else:
                intent_state = self._seal_intent_state(candidate.job_id)
                recovery_status: Literal[
                    "recoverable",
                    "needs_authority",
                    "recoverable_torn",
                ] = {
                    "valid": "recoverable",
                    "missing": "needs_authority",
                    "torn": "recoverable_torn",
                }[intent_state]
                records.append(
                    LabArtifactRecoveryRecord(
                        path=path,
                        status=recovery_status,
                        job_id=candidate.job_id,
                        manifest_hash=candidate.manifest_hash,
                        device=candidate.device,
                        inode=candidate.inode,
                        file_type="directory",
                        reason=(
                            "recoverable_torn seal intent requires external authority"
                            if intent_state == "torn"
                            else None
                        ),
                    )
                )
        for name in quarantine_names:
            path = self.quarantine_root / name
            descriptor = self._managed_parent_descriptor(self.quarantine_root)
            try:
                observed = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            finally:
                os.close(descriptor)
            records.append(
                LabArtifactRecoveryRecord(
                    path=path,
                    status="quarantined",
                    device=observed.st_dev,
                    inode=observed.st_ino,
                    file_type=_entry_file_type(observed.st_mode),
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
        try:
            candidate = LabJobArtifactCandidate.model_validate(
                {
                    field_name: getattr(candidate, field_name)
                    for field_name in LabJobArtifactCandidate.model_fields
                }
            )
            authority = LabJobArtifactStore._defensively_validate_recovery_authority(authority)
        except LabArtifactAuthorizationError:
            raise
        except Exception as exc:
            raise LabArtifactAuthorizationError("candidate recovery identity is invalid") from exc
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

    @_artifact_public_operation()
    def recover_candidate(
        self,
        record: LabArtifactRecoveryRecord,
        *,
        authority: LabArtifactRecoveryAuthority | None = None,
    ) -> LabSealedJobArtifact:
        self._assert_store_operational()
        record = self._defensively_validate_recovery_record(record)
        if record.status not in {"recoverable", "needs_authority", "recoverable_torn"}:
            raise LabArtifactIntegrityError("only candidate recovery records can be sealed")
        if record.device is None or record.inode is None:
            raise LabArtifactIntegrityError("candidate recovery identity is unavailable")
        candidate = self._candidate_from_path(
            record.path,
            allow_interrupted_seal=True,
        )
        if self.verify_candidate(candidate, allow_interrupted_seal=True) != candidate.manifest:
            raise LabArtifactIntegrityError("candidate recovery manifest changed")
        if (candidate.device, candidate.inode) != (record.device, record.inode):
            raise LabArtifactIntegrityError("candidate recovery record identity changed")
        if (
            record.job_id != candidate.job_id
            or record.manifest_hash != candidate.manifest_hash
            or (candidate.device, candidate.inode) != (record.device, record.inode)
        ):
            raise LabArtifactIntegrityError("candidate recovery evidence changed")
        intent_state = self._seal_intent_state(candidate.job_id)
        if (
            intent_state != "valid"
            or record.status in {"needs_authority", "recoverable_torn"}
            or authority is not None
        ):
            self._authorize_recovery(candidate, authority)
        if intent_state == "torn":
            self._quarantine_seal_intent_entry(f"{candidate.job_id.hex}.json")
            with self._bind_seal_intent(
                candidate.job_id,
                candidate=candidate,
                create=True,
            ):
                pass
        return self.seal_candidate(candidate)

    @_artifact_public_operation()
    def recover_interrupted_seal(self, path: Path) -> LabSealedJobArtifact:
        """Finish a durable seal intent after rename without trusting path identity."""

        self._assert_store_operational()
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
                manifest, identities, observed = self._validate_bundle(
                    managed,
                    parent_root=self.sealed_root,
                    permission_profile="sealed",
                )
            except LabArtifactIntegrityError:
                manifest, identities, observed = self._validate_bundle(
                    managed,
                    parent_root=self.sealed_root,
                    permission_profile="interrupted",
                )
                already_sealed = False
            else:
                already_sealed = True
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
            if already_sealed:
                return self._finalize_public_sealed(
                    managed,
                    expected_manifest=manifest,
                    expected_bundle_identity=(observed.device, observed.inode),
                    expected_file_identities=identities,
                )
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
                return self._finalize_public_sealed(
                    managed,
                    expected_manifest=manifest,
                    expected_bundle_identity=(bound.current.device, bound.current.inode),
                    expected_file_identities=self._bound_file_identities(bound),
                )

    @_artifact_public_operation()
    def quarantine_candidate(
        self,
        candidate: LabJobArtifactCandidate,
        *,
        reason: str,
    ) -> LabArtifactRecoveryRecord:
        self._assert_store_operational()
        if not reason.strip():
            raise ValueError("quarantine reason must not be empty")
        candidate = self._defensively_validate_candidate(candidate)
        if self.verify_candidate(candidate, allow_interrupted_seal=True) != candidate.manifest:
            raise LabArtifactIntegrityError("candidate manifest changed before quarantine")
        path = self._assert_managed_child(candidate.path, self.candidates_root, label="candidate")
        target = self.quarantine_root / (
            f"{path.name}-{candidate.manifest_hash[:16]}-{uuid4().hex}"
        )
        with self._bind_quarantined_entry(
            source_name=path.name,
            target_name=target.name,
            expected_device=candidate.device,
            expected_inode=candidate.inode,
        ) as observed:
            result = LabArtifactRecoveryRecord(
                path=target,
                status="quarantined",
                job_id=candidate.job_id,
                manifest_hash=candidate.manifest_hash,
                device=observed.device,
                inode=observed.inode,
                file_type="directory",
                reason=" ".join(reason.split()),
            )
            self._after_quarantine_record_finalized(result)
            return result

    @_artifact_public_operation()
    def quarantine_recovery_record(
        self,
        record: LabArtifactRecoveryRecord,
        *,
        reason: str,
    ) -> LabArtifactRecoveryRecord:
        """Logically isolate an invalid or recoverable candidate without deleting it."""

        self._assert_store_operational()
        record = self._defensively_validate_recovery_record(record)
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
        if record.file_type == "directory":
            try:
                derived_candidate = self._candidate_from_path(
                    path,
                    allow_interrupted_seal=True,
                )
            except LabArtifactError:
                derived_candidate = None
        else:
            derived_candidate = None
        if derived_candidate is not None:
            if (derived_candidate.device, derived_candidate.inode) != (
                record.device,
                record.inode,
            ):
                raise LabArtifactIntegrityError("candidate recovery identity changed")
            if record.status != "invalid" and (
                record.job_id != derived_candidate.job_id
                or record.manifest_hash != derived_candidate.manifest_hash
            ):
                raise LabArtifactIntegrityError(
                    "candidate recovery logical identity changed before quarantine"
                )
            derived_job_id = derived_candidate.job_id
            derived_manifest_hash = derived_candidate.manifest_hash
        else:
            derived_job_id = None
            derived_manifest_hash = None
        target = self.quarantine_root / f"{path.name}-recovery-{uuid4().hex}"
        with self._bind_quarantined_entry(
            source_name=path.name,
            target_name=target.name,
            expected_device=record.device,
            expected_inode=record.inode,
            expected_file_type=record.file_type,
        ) as observed:
            result = LabArtifactRecoveryRecord(
                path=target,
                status="quarantined",
                job_id=derived_job_id,
                manifest_hash=derived_manifest_hash,
                device=observed.device,
                inode=observed.inode,
                file_type=_entry_file_type(observed.mode),
                reason=" ".join(reason.split()),
            )
            self._after_quarantine_record_finalized(result)
            return result

    @staticmethod
    def _after_quarantine_record_finalized(_record: LabArtifactRecoveryRecord) -> None:
        """Fault-injection boundary before a quarantine record returns."""

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

    @staticmethod
    def _open_quarantine_entry_descriptor(
        parent_descriptor: int,
        name: str,
        file_type: Literal["directory", "regular", "symlink", "other"],
    ) -> int:
        if file_type == "directory":
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        elif file_type == "regular":
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        elif file_type == "symlink" and sys.platform == "darwin" and hasattr(os, "O_SYMLINK"):
            flags = os.O_RDONLY | os.O_SYMLINK
        elif file_type == "symlink" and hasattr(os, "O_PATH"):
            flags = os.O_PATH | getattr(os, "O_NOFOLLOW", 0)
        else:
            raise LabArtifactPlatformError(
                f"cannot fd-bind quarantine entry type on this platform: {file_type}"
            )
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
        observed = _FileObservation.from_stat(os.fstat(descriptor))
        if _entry_file_type(observed.mode) != file_type:
            os.close(descriptor)
            raise LabArtifactIntegrityError("candidate quarantine entry type changed")
        return descriptor

    @contextmanager
    def _bind_quarantined_entry(
        self,
        *,
        source_name: str,
        target_name: str,
        expected_device: int,
        expected_inode: int,
        expected_file_type: Literal["directory", "regular", "symlink", "other"] = "directory",
    ) -> Iterator[_FileObservation]:
        source_parent = -1
        target_parent = -1
        source_descriptor = -1
        target_descriptor = -1
        main_error: BaseException | None = None
        try:
            source_parent = self._managed_parent_descriptor(self.candidates_root)
            target_parent = self._managed_parent_descriptor(self.quarantine_root)
            before = _FileObservation.from_stat(
                os.stat(source_name, dir_fd=source_parent, follow_symlinks=False)
            )
            if (
                before.device,
                before.inode,
                before.mode,
            ) != (
                expected_device,
                expected_inode,
                {
                    "directory": stat.S_IFDIR,
                    "regular": stat.S_IFREG,
                    "symlink": stat.S_IFLNK,
                }.get(expected_file_type, before.mode),
            ) or _entry_file_type(before.mode) != expected_file_type:
                raise LabArtifactIntegrityError("candidate quarantine identity changed")
            if expected_file_type != "other":
                source_descriptor = self._open_quarantine_entry_descriptor(
                    source_parent,
                    source_name,
                    expected_file_type,
                )
                opened = _FileObservation.from_stat(os.fstat(source_descriptor))
                if opened != before:
                    raise LabArtifactIntegrityError("candidate quarantine identity changed")
            try:
                self._guard_mutation()
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
            still_open = (
                _FileObservation.from_stat(os.fstat(source_descriptor))
                if source_descriptor >= 0
                else before
            )
            if not _matches_rename_identity(before, target) or (
                target.device,
                target.inode,
            ) != (
                expected_device,
                expected_inode,
            ):
                raise LabArtifactIntegrityError("candidate quarantine target identity changed")
            if source_descriptor >= 0 and target != still_open:
                raise LabArtifactIntegrityError("candidate quarantine target identity changed")
            try:
                os.stat(source_name, dir_fd=source_parent, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise LabArtifactIntegrityError("candidate quarantine source identity changed")
            if expected_file_type != "other":
                target_descriptor = self._open_quarantine_entry_descriptor(
                    target_parent,
                    target_name,
                    expected_file_type,
                )
                target_opened = _FileObservation.from_stat(os.fstat(target_descriptor))
                if target_opened != target or target_opened != still_open:
                    raise LabArtifactIntegrityError(
                        "candidate quarantine target identity changed while binding"
                    )
            os.fsync(source_parent)
            os.fsync(target_parent)
            self._assert_managed_roots()
            caller_error: BaseException | None = None
            try:
                yield target
            except BaseException as exc:
                caller_error = exc
            integrity_error: BaseException | None = None
            try:
                target_at_return = _FileObservation.from_stat(
                    os.stat(target_name, dir_fd=target_parent, follow_symlinks=False)
                )
                target_fd_at_return = (
                    _FileObservation.from_stat(os.fstat(target_descriptor))
                    if target_descriptor >= 0
                    else target
                )
                source_fd_at_return = (
                    _FileObservation.from_stat(os.fstat(source_descriptor))
                    if source_descriptor >= 0
                    else target
                )
                if not (target_at_return == target_fd_at_return == source_fd_at_return == target):
                    raise LabArtifactIntegrityError(
                        "candidate quarantine target identity changed before return"
                    )
                try:
                    os.stat(source_name, dir_fd=source_parent, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise LabArtifactIntegrityError(
                        "candidate quarantine source identity changed before return"
                    )
                self._assert_managed_roots()
            except BaseException as exc:
                integrity_error = exc
            if caller_error is not None and integrity_error is not None:
                raise BaseExceptionGroup(
                    "quarantine operation and final identity checks both failed",
                    [caller_error, integrity_error],
                ) from None
            if integrity_error is not None:
                raise integrity_error
            if caller_error is not None:
                raise caller_error
        except LabArtifactError as exc:
            main_error = exc
        except OSError as exc:
            main_error = LabArtifactIntegrityError("candidate quarantine identity changed")
            main_error.__cause__ = exc
        except BaseException as exc:
            main_error = exc
        finally:
            cleanup_errors: list[BaseException] = []
            for descriptor in (
                target_descriptor,
                source_descriptor,
                source_parent,
                target_parent,
            ):
                if descriptor >= 0:
                    try:
                        os.close(descriptor)
                    except BaseException as close_error:
                        cleanup_errors.append(close_error)
            errors = [main_error, *cleanup_errors] if main_error is not None else cleanup_errors
            _raise_collected_errors(
                "candidate quarantine operation and descriptor cleanup failed",
                errors,
            )

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
    def _defensively_validate_index_evidence(
        evidence: LabArtifactIndexEvidence,
    ) -> LabArtifactIndexEvidence:
        try:
            return LabArtifactIndexEvidence.model_validate(
                {
                    field_name: getattr(evidence, field_name)
                    for field_name in LabArtifactIndexEvidence.model_fields
                }
            )
        except Exception as exc:
            raise LabArtifactAuthorizationError("indexed evidence is invalid") from exc

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

    def _quarantine_failed_zip_temporary(
        self,
        parent_descriptor: int,
        temporary_name: str,
        temporary_descriptor: int,
        destination_name: str,
    ) -> None:
        try:
            opened_before = _FileObservation.from_stat(os.fstat(temporary_descriptor))
            at_path_before = _FileObservation.from_stat(
                os.stat(
                    temporary_name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
            )
        except OSError as exc:
            raise LabArtifactIntegrityError(
                "ZIP temporary identity changed before cleanup"
            ) from exc
        if (
            opened_before != at_path_before
            or opened_before.mode != stat.S_IFREG
            or opened_before.nlink != 1
            or stat.S_IMODE(os.fstat(temporary_descriptor).st_mode) != 0o600
        ):
            raise LabArtifactIntegrityError("ZIP temporary identity changed before cleanup")
        os.ftruncate(temporary_descriptor, 0)
        os.fchmod(temporary_descriptor, 0o600)
        os.fsync(temporary_descriptor)
        opened_truncated = _FileObservation.from_stat(os.fstat(temporary_descriptor))
        try:
            at_path_truncated = _FileObservation.from_stat(
                os.stat(
                    temporary_name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
            )
        except OSError as exc:
            raise LabArtifactIntegrityError(
                "ZIP temporary identity changed during cleanup"
            ) from exc
        if opened_truncated != at_path_truncated:
            raise LabArtifactIntegrityError("ZIP temporary identity changed during cleanup")
        quarantine_name = f".{destination_name}.{uuid4().hex}.discarded"
        try:
            _rename_noreplace(
                parent_descriptor,
                temporary_name,
                parent_descriptor,
                quarantine_name,
            )
        except OSError as exc:
            raise LabArtifactIntegrityError("ZIP temporary cleanup could not be isolated") from exc
        target = _FileObservation.from_stat(
            os.stat(
                quarantine_name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        )
        opened_after = _FileObservation.from_stat(os.fstat(temporary_descriptor))
        if target != opened_after or not _matches_rename_identity(
            opened_truncated,
            opened_after,
        ):
            raise LabArtifactIntegrityError("ZIP temporary cleanup changed bound identity")
        self._before_zip_temporary_unlink(
            parent_descriptor,
            quarantine_name,
        )
        try:
            before_unlink = _FileObservation.from_stat(
                os.stat(
                    quarantine_name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
            )
        except OSError as exc:
            raise LabArtifactIntegrityError(
                "ZIP temporary cleanup identity changed before unlink"
            ) from exc
        opened_before_unlink = _FileObservation.from_stat(os.fstat(temporary_descriptor))
        if before_unlink != opened_before_unlink or before_unlink != target:
            raise LabArtifactIntegrityError("ZIP temporary cleanup identity changed before unlink")
        os.unlink(quarantine_name, dir_fd=parent_descriptor)
        opened_unlinked = _FileObservation.from_stat(os.fstat(temporary_descriptor))
        if (
            opened_unlinked.nlink != 0
            or (
                opened_unlinked.device,
                opened_unlinked.inode,
                opened_unlinked.mode,
                opened_unlinked.size,
                opened_unlinked.mtime_ns,
            )
            != (
                opened_before_unlink.device,
                opened_before_unlink.inode,
                opened_before_unlink.mode,
                opened_before_unlink.size,
                opened_before_unlink.mtime_ns,
            )
            or opened_unlinked.ctime_ns < opened_before_unlink.ctime_ns
        ):
            raise LabArtifactIntegrityError("ZIP temporary cleanup unlink was not bound")
        try:
            os.stat(
                quarantine_name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            pass
        else:
            raise LabArtifactIntegrityError("ZIP temporary cleanup path still exists")
        os.fsync(parent_descriptor)

    @staticmethod
    def _before_zip_temporary_unlink(
        _parent_descriptor: int,
        _name: str,
    ) -> None:
        """Fault-injection boundary before an identity-bound ZIP cleanup unlink."""

    @staticmethod
    def _after_zip_final_checks(_destination: Path) -> None:
        """Fault-injection boundary before a published ZIP path returns."""

    def _open_bound_zip_destination(
        self,
        destination: LabBoundZipDestination,
    ) -> tuple[int, _FileObservation]:
        descriptor = -1
        path_descriptor = -1
        try:
            descriptor = os.dup(destination.directory_descriptor)
            raw = os.fstat(descriptor)
            observed = _FileObservation.from_stat(raw)
            if (
                (observed.device, observed.inode)
                != (destination.directory_device, destination.directory_inode)
                or observed.mode != stat.S_IFDIR
                or stat.S_IMODE(raw.st_mode) != 0o700
                or raw.st_uid != os.getuid()
            ):
                raise LabArtifactIntegrityError(
                    "bound ZIP destination directory identity is unsafe or changed"
                )
            try:
                path_descriptor = _secure_open_directory(
                    destination.directory_path,
                    create=False,
                )
            except LabArtifactPathError as exc:
                raise LabArtifactIntegrityError(
                    "bound ZIP destination path identity changed"
                ) from exc
            at_path = _FileObservation.from_stat(os.fstat(path_descriptor))
            if not self._same_directory_identity(at_path, observed):
                raise LabArtifactIntegrityError("bound ZIP destination path identity changed")
            result = descriptor
            descriptor = -1
            return result, observed
        finally:
            for opened_descriptor in (path_descriptor, descriptor):
                if opened_descriptor >= 0:
                    with suppress(OSError):
                        os.close(opened_descriptor)

    @_artifact_public_operation()
    def export_deterministic_zip(
        self,
        sealed_path: Path,
        evidence: LabArtifactIndexEvidence,
        destination: Path,
    ) -> Path:
        return self._export_deterministic_zip(
            sealed_path,
            evidence,
            destination,
            bound_destination=None,
        )

    @_artifact_public_operation()
    def export_deterministic_zip_bound(
        self,
        sealed_path: Path,
        evidence: LabArtifactIndexEvidence,
        destination: LabBoundZipDestination,
    ) -> Path:
        validated = LabBoundZipDestination.model_validate(destination)
        return self._export_deterministic_zip(
            sealed_path,
            evidence,
            validated.path,
            bound_destination=validated,
        )

    def _export_deterministic_zip(
        self,
        sealed_path: Path,
        evidence: LabArtifactIndexEvidence,
        destination: Path,
        *,
        bound_destination: LabBoundZipDestination | None,
    ) -> Path:
        """Export stable bytes for this Python/ZIP runtime, not a cross-platform guarantee."""

        self._assert_store_operational()
        evidence = self._defensively_validate_index_evidence(evidence)
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
        if destination.name in {"", ".", ".."}:
            raise LabArtifactPathError("ZIP destination name is unsafe")
        destination_parent = -1
        destination_parent_identity: _FileObservation | None = None
        temporary_name = f".{destination.name}.{uuid4().hex}.tmp"
        temporary_descriptor = -1
        temporary_published = False
        destination_descriptor = -1
        result: Path | None = None
        main_error: BaseException | None = None
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
                self._assert_bound_paths(bound)
                self._assert_managed_roots()
                if bound_destination is None:
                    _ensure_private_directory(destination.parent, manage_existing=False)
                    destination_parent = _secure_open_directory(destination.parent, create=True)
                    destination_parent_identity = _FileObservation.from_stat(
                        os.fstat(destination_parent)
                    )
                else:
                    destination_parent, destination_parent_identity = (
                        self._open_bound_zip_destination(bound_destination)
                    )
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
                    for relative_path in sorted(expected_hashes):
                        source = bound.files[relative_path]
                        before_stream = _FileObservation.from_stat(os.fstat(source.descriptor))
                        if before_stream != source.current:
                            raise LabArtifactIntegrityError(
                                f"export source identity changed: {relative_path}"
                            )
                        info = ZipInfo(relative_path, date_time=_ZIP_TIMESTAMP)
                        info.compress_type = ZIP_DEFLATED
                        info.create_system = 3
                        info.external_attr = (stat.S_IFREG | 0o400) << 16
                        info.flag_bits = 0
                        info._compresslevel = 9
                        digest = hashlib.sha256()
                        streamed_size = 0
                        os.lseek(source.descriptor, 0, os.SEEK_SET)
                        with archive.open(info, mode="w", force_zip64=True) as entry:
                            while chunk := os.read(
                                source.descriptor,
                                _ZIP_STREAM_CHUNK_SIZE,
                            ):
                                digest.update(chunk)
                                streamed_size += len(chunk)
                                written = entry.write(chunk)
                                if written != len(chunk):
                                    raise LabArtifactIntegrityError(
                                        f"ZIP entry write made partial progress: {relative_path}"
                                    )
                        after_stream = _FileObservation.from_stat(os.fstat(source.descriptor))
                        if before_stream != after_stream or streamed_size != before_stream.size:
                            raise LabArtifactIntegrityError(
                                f"export source changed while streaming: {relative_path}"
                            )
                        if digest.hexdigest() != expected_hashes[relative_path]:
                            raise LabArtifactIntegrityError(
                                f"export bytes conflict: {relative_path}"
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
                    temporary_published = True
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
                destination_descriptor = os.open(
                    destination.name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=destination_parent,
                )
                destination_opened = _FileObservation.from_stat(os.fstat(destination_descriptor))
                if destination_opened != published or (
                    destination_opened.device,
                    destination_opened.inode,
                    destination_opened.mode,
                    destination_opened.nlink,
                    destination_opened.size,
                    destination_opened.mtime_ns,
                ) != (
                    final_temporary.device,
                    final_temporary.inode,
                    final_temporary.mode,
                    final_temporary.nlink,
                    final_temporary.size,
                    final_temporary.mtime_ns,
                ):
                    raise LabArtifactIntegrityError(
                        "ZIP destination identity changed after publication"
                    )
                destination_bound = _BoundReadonlyFile(
                    path=destination,
                    parent_descriptor=destination_parent,
                    descriptor=destination_descriptor,
                    parent_identity=destination_parent_identity,
                    file_identity=destination_opened,
                )
                expected_zip_hash = _sha256_descriptor(temporary_descriptor)
                if _sha256_descriptor(destination_descriptor) != expected_zip_hash:
                    raise LabArtifactIntegrityError(
                        "ZIP destination bytes changed after publication"
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
                _assert_bound_readonly_file(destination_bound, label="ZIP destination")
                self._after_zip_final_checks(destination)
                _assert_bound_readonly_file(destination_bound, label="ZIP destination")
                if _sha256_descriptor(destination_descriptor) != expected_zip_hash:
                    raise LabArtifactIntegrityError("ZIP destination bytes changed before return")
                self._assert_bound_paths(bound)
                self._assert_managed_roots()
            result = destination
        except BaseException as exc:
            main_error = exc
        finally:
            cleanup_error: BaseException | None = None
            if temporary_descriptor >= 0 and not temporary_published:
                try:
                    self._quarantine_failed_zip_temporary(
                        destination_parent,
                        temporary_name,
                        temporary_descriptor,
                        destination.name,
                    )
                except BaseException as exc:
                    cleanup_error = exc
            for descriptor in (
                destination_descriptor,
                temporary_descriptor,
                destination_parent,
            ):
                if descriptor >= 0:
                    with suppress(OSError):
                        os.close(descriptor)
            if main_error is not None and cleanup_error is not None:
                if isinstance(main_error, Exception) and isinstance(cleanup_error, Exception):
                    raise ExceptionGroup(
                        "ZIP publication and cleanup both failed",
                        [main_error, cleanup_error],
                    ) from None
                raise BaseExceptionGroup(
                    "ZIP publication and cleanup both failed",
                    [main_error, cleanup_error],
                ) from None
            if cleanup_error is not None:
                raise cleanup_error
        if main_error is not None:
            raise main_error
        if result is None:
            raise LabArtifactIntegrityError("ZIP export completed without a result")
        return result


@dataclass(frozen=True)
class _LegacyAuthorityState:
    latest: dict[str, LabLegacyAuthorityEvent]
    generations: dict[str, int]
    events: tuple[LabLegacyAuthorityEvent, ...]
    sequence: int
    final_hash: str
    ledger_size: int


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
        self._lifecycle_condition = threading.Condition(threading.Lock())
        self._active_operations = 0
        self._operation_threads: dict[int, int] = {}
        self._closing = False
        self._closed = False
        self._process_lock_key: tuple[int, int] | None = None
        self._process_lock_registered = False
        self._process_lock = threading.RLock()
        self._process_lock_entry: _LegacyProcessLockEntry | None = None
        self._parent_descriptor = -1
        self._lock_descriptor = -1
        self._authority_descriptor = -1
        self._heads_descriptor = -1
        self._head_descriptor = -1
        self._head_name = ""
        self._database_descriptor = -1
        self._journal_descriptor = -1
        self._cache_quarantine_descriptor = -1
        self._authority_quarantine_descriptor = -1
        self._authority_lock_depth = 0
        self._cache_quarantine_path = self.path.parent / ".legacy-cache-quarantine"
        self._authority_quarantine_path = self.path.parent / ".legacy-authority-quarantine"
        self._authority_heads_path = self.path.parent / self._authority_heads_name
        try:
            _ensure_private_directory(
                self.path.parent,
                manage_existing=False,
                require_private_existing=True,
            )
            _ensure_private_directory(
                self._cache_quarantine_path,
                manage_existing=False,
                require_private_existing=True,
            )
            _ensure_private_directory(
                self._authority_quarantine_path,
                manage_existing=False,
                require_private_existing=True,
            )
            _ensure_private_directory(
                self._authority_heads_path,
                manage_existing=False,
                require_private_existing=True,
            )
            self._parent_descriptor = _secure_open_directory(self.path.parent, create=False)
            self._parent_identity = _FileObservation.from_stat(os.fstat(self._parent_descriptor))
            self._lock_descriptor, _ = _open_or_create_private_regular_at(
                self._parent_descriptor,
                f"{self.path.name}.lock",
                access_flags=os.O_RDWR,
                require_private_existing=True,
            )
            self._lock_identity = _FileObservation.from_stat(os.fstat(self._lock_descriptor))
            lock_key = (self._lock_identity.device, self._lock_identity.inode)
            with _LEGACY_PROCESS_LOCKS_GUARD:
                entry = _LEGACY_PROCESS_LOCKS.get(lock_key)
                if entry is None:
                    entry = _LegacyProcessLockEntry(lock=threading.RLock(), references=0)
                    _LEGACY_PROCESS_LOCKS[lock_key] = entry
                entry.references += 1
                try:
                    self._process_lock = entry.lock
                    self._process_lock_entry = entry
                    self._process_lock_key = lock_key
                    self._process_lock_registered = True
                except BaseException:
                    entry.references -= 1
                    if entry.references == 0:
                        del _LEGACY_PROCESS_LOCKS[lock_key]
                    raise
        except BaseException as error:
            try:
                self.close()
            except BaseException as cleanup_error:
                if isinstance(error, Exception) and isinstance(cleanup_error, Exception):
                    raise ExceptionGroup(
                        "legacy index initialization and cleanup both failed",
                        [error, cleanup_error],
                    ) from None
                raise BaseExceptionGroup(
                    "legacy index initialization and cleanup both failed",
                    [error, cleanup_error],
                ) from None
            raise
        try:
            with self._legacy_process_operation_lock():
                _acquire_exclusive_flock(
                    self._lock_descriptor,
                    label="legacy index initialization lock",
                )
                self._authority_lock_depth += 1
                operation_error: BaseException | None = None
                try:
                    self._authority_descriptor, _ = _open_or_create_private_regular_at(
                        self._parent_descriptor,
                        f"{self.path.name}.authority.jsonl",
                        access_flags=os.O_RDWR,
                        require_private_existing=True,
                    )
                    self._cache_quarantine_descriptor = os.open(
                        self._cache_quarantine_path.name,
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=self._parent_descriptor,
                    )
                    self._authority_quarantine_descriptor = os.open(
                        self._authority_quarantine_path.name,
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=self._parent_descriptor,
                    )
                    self._heads_descriptor = os.open(
                        self._authority_heads_path.name,
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=self._parent_descriptor,
                    )
                    self._cache_quarantine_identity = _FileObservation.from_stat(
                        os.fstat(self._cache_quarantine_descriptor)
                    )
                    self._authority_quarantine_identity = _FileObservation.from_stat(
                        os.fstat(self._authority_quarantine_descriptor)
                    )
                    self._heads_identity = _FileObservation.from_stat(
                        os.fstat(self._heads_descriptor)
                    )
                    self._bind_or_create_authority_head()
                    self._database_descriptor, _ = _open_or_create_private_regular_at(
                        self._parent_descriptor,
                        self.path.name,
                        access_flags=os.O_RDONLY,
                        require_private_existing=True,
                    )
                    self._journal_descriptor, _ = _open_or_create_private_regular_at(
                        self._parent_descriptor,
                        f"{self.path.name}-journal",
                        access_flags=os.O_RDONLY,
                        require_private_existing=True,
                    )
                    self._lock_identity = _FileObservation.from_stat(
                        os.fstat(self._lock_descriptor)
                    )
                    self._authority_identity = _FileObservation.from_stat(
                        os.fstat(self._authority_descriptor)
                    )
                    self._head_identity = _FileObservation.from_stat(
                        os.fstat(self._head_descriptor)
                    )
                    self._database_identity = _FileObservation.from_stat(
                        os.fstat(self._database_descriptor)
                    )
                    self._journal_identity = _FileObservation.from_stat(
                        os.fstat(self._journal_descriptor)
                    )
                    self._assert_index_identity()
                    authority = self._read_authority_state()
                    authority = self._reconcile_published_sources(authority)
                    self._ensure_cache_ready(authority)
                    self._assert_index_identity()
                except BaseException as exc:
                    operation_error = exc
                unlock_error: BaseException | None = None
                try:
                    fcntl.flock(self._lock_descriptor, fcntl.LOCK_UN)
                except BaseException as exc:
                    unlock_error = exc
                finally:
                    self._authority_lock_depth -= 1
                if operation_error is not None and unlock_error is not None:
                    if isinstance(operation_error, Exception) and isinstance(
                        unlock_error,
                        Exception,
                    ):
                        raise ExceptionGroup(
                            "legacy index initialization and unlock both failed",
                            [operation_error, unlock_error],
                        ) from None
                    raise BaseExceptionGroup(
                        "legacy index initialization and unlock both failed",
                        [operation_error, unlock_error],
                    ) from None
                if unlock_error is not None:
                    raise unlock_error
                if operation_error is not None:
                    raise operation_error
        except BaseException as error:
            try:
                self.close()
            except BaseException as cleanup_error:
                if isinstance(error, Exception) and isinstance(cleanup_error, Exception):
                    raise ExceptionGroup(
                        "legacy index initialization and cleanup both failed",
                        [error, cleanup_error],
                    ) from None
                raise BaseExceptionGroup(
                    "legacy index initialization and cleanup both failed",
                    [error, cleanup_error],
                ) from None
            raise

    def close(self) -> None:
        condition = getattr(self, "_lifecycle_condition", None)
        if condition is None:
            return
        current_thread_id = threading.get_ident()
        with condition:
            if self._closed:
                return
            if self._operation_threads.get(current_thread_id, 0) > 0:
                raise LabArtifactIntegrityError(
                    "legacy index cannot close from an active operation"
                )
            if self._closing:
                while not self._closed:
                    condition.wait()
                return
            self._closing = True
            while self._active_operations > 0:
                condition.wait()
        cleanup_errors: list[BaseException] = []
        try:
            with self._process_lock:
                for attribute in (
                    "_authority_quarantine_descriptor",
                    "_cache_quarantine_descriptor",
                    "_journal_descriptor",
                    "_database_descriptor",
                    "_head_descriptor",
                    "_heads_descriptor",
                    "_authority_descriptor",
                    "_lock_descriptor",
                    "_parent_descriptor",
                ):
                    descriptor = getattr(self, attribute, -1)
                    if descriptor >= 0:
                        try:
                            os.close(descriptor)
                        except BaseException as exc:
                            cleanup_errors.append(exc)
                        finally:
                            setattr(self, attribute, -1)
                if getattr(self, "_process_lock_registered", False):
                    with _LEGACY_PROCESS_LOCKS_GUARD:
                        key = self._process_lock_key
                        entry = _LEGACY_PROCESS_LOCKS.get(key) if key is not None else None
                        if (
                            entry is not None
                            and entry is self._process_lock_entry
                            and entry.lock is self._process_lock
                        ):
                            entry.references -= 1
                            if entry.references == 0 and key is not None:
                                del _LEGACY_PROCESS_LOCKS[key]
                        else:
                            cleanup_errors.append(
                                LabArtifactIntegrityError(
                                    "legacy index process lock registry changed before close"
                                )
                            )
                    self._process_lock_registered = False
                    self._process_lock_entry = None
                    self._process_lock_key = None
        except BaseException as exc:
            cleanup_errors.append(exc)
        finally:
            with condition:
                self._closed = True
                self._closing = False
                condition.notify_all()
        if len(cleanup_errors) == 1:
            raise cleanup_errors[0]
        if cleanup_errors:
            if all(isinstance(error, Exception) for error in cleanup_errors):
                raise ExceptionGroup(
                    "legacy index close encountered multiple failures",
                    [error for error in cleanup_errors if isinstance(error, Exception)],
                )
            raise BaseExceptionGroup(
                "legacy index close encountered multiple failures",
                cleanup_errors,
            )

    def __del__(self) -> None:
        with suppress(BaseException):
            self.close()

    @contextmanager
    def _legacy_process_operation_lock(self) -> Iterator[None]:
        entry = self._process_lock_entry
        key = self._process_lock_key
        if entry is None or key is None:
            raise LabArtifactIntegrityError("legacy index process lock is unavailable")
        current_thread_id = threading.get_ident()
        with _LEGACY_PROCESS_LOCKS_GUARD:
            registered = _LEGACY_PROCESS_LOCKS.get(key)
            if registered is not entry or registered.lock is not self._process_lock:
                raise LabArtifactIntegrityError("legacy index process lock identity changed")
            if entry.owner_thread_id == current_thread_id:
                raise LabArtifactIntegrityError("reentrant legacy index operation is not allowed")
        with self._process_lock:
            with _LEGACY_PROCESS_LOCKS_GUARD:
                registered = _LEGACY_PROCESS_LOCKS.get(key)
                if registered is not entry or registered.lock is not self._process_lock:
                    raise LabArtifactIntegrityError("legacy index process lock identity changed")
                if entry.owner_thread_id is not None:
                    raise LabArtifactIntegrityError("legacy index process lock owner changed")
                entry.owner_thread_id = current_thread_id
            caller_error: BaseException | None = None
            try:
                yield
            except BaseException as exc:
                caller_error = exc
            integrity_error: BaseException | None = None
            with _LEGACY_PROCESS_LOCKS_GUARD:
                if entry.owner_thread_id != current_thread_id:
                    integrity_error = LabArtifactIntegrityError(
                        "legacy index process lock owner changed"
                    )
                else:
                    entry.owner_thread_id = None
            if caller_error is not None and integrity_error is not None:
                if isinstance(caller_error, Exception):
                    raise ExceptionGroup(
                        "legacy operation and process lock cleanup both failed",
                        [caller_error, integrity_error],
                    ) from None
                raise BaseExceptionGroup(
                    "legacy operation and process lock cleanup both failed",
                    [caller_error, integrity_error],
                ) from None
            if integrity_error is not None:
                raise integrity_error
            if caller_error is not None:
                raise caller_error

    @property
    def _authority_heads_name(self) -> str:
        return f"{self.path.name}.authority.heads"

    @property
    def _legacy_authority_head_name(self) -> str:
        return f"{self.path.name}.authority.head.json"

    @staticmethod
    def _authority_head_file_name(head: LabLegacyAuthorityHead) -> str:
        return f"{head.sequence:020d}-{head.final_hash}-{head.ledger_size:020d}.json"

    @staticmethod
    def _complete_authority_payload(payload: bytes) -> bytes:
        if not payload or payload.endswith(b"\n"):
            return payload
        final_newline = payload.rfind(b"\n")
        return payload[: final_newline + 1] if final_newline >= 0 else b""

    @classmethod
    def _authority_heads_for_state(
        cls,
        state: _LegacyAuthorityState,
    ) -> tuple[LabLegacyAuthorityHead, ...]:
        heads = [
            LabLegacyAuthorityHead(
                sequence=0,
                final_hash=_LEGACY_GENESIS_HASH,
                ledger_size=0,
            )
        ]
        ledger_size = 0
        for event in state.events:
            ledger_size += len(event.canonical_json_bytes()) + 1
            heads.append(
                LabLegacyAuthorityHead(
                    sequence=event.sequence,
                    final_hash=event.event_hash,
                    ledger_size=ledger_size,
                )
            )
        if heads[-1] != cls._authority_head_for_state(state):
            raise LabArtifactIntegrityError(
                "legacy authority generation heads conflict with ledger state"
            )
        return tuple(heads)

    def _quarantine_authority_entry(self, name: str, *, source_parent: int) -> None:
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=source_parent,
        )
        try:
            opened = _FileObservation.from_stat(os.fstat(descriptor))
            at_path = _FileObservation.from_stat(
                os.stat(name, dir_fd=source_parent, follow_symlinks=False)
            )
            if (
                opened != at_path
                or opened.mode != stat.S_IFREG
                or opened.nlink != 1
                or stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600
            ):
                raise LabArtifactIntegrityError("legacy authority recovery entry is unsafe")
            target = f"{name.lstrip('.')}.{uuid4().hex}.quarantined"
            _rename_noreplace(
                source_parent,
                name,
                self._authority_quarantine_descriptor,
                target,
            )
            moved = _FileObservation.from_stat(
                os.stat(
                    target,
                    dir_fd=self._authority_quarantine_descriptor,
                    follow_symlinks=False,
                )
            )
            if moved != _FileObservation.from_stat(os.fstat(descriptor)):
                raise LabArtifactIntegrityError("legacy authority quarantine identity changed")
            os.fsync(source_parent)
            os.fsync(self._authority_quarantine_descriptor)
            if source_parent == self._heads_descriptor:
                self._heads_identity = _FileObservation.from_stat(os.fstat(self._heads_descriptor))
        finally:
            os.close(descriptor)

    def _quarantine_orphaned_authority_head_temps(self) -> None:
        names = sorted(
            name
            for name in os.listdir(self._heads_descriptor)
            if name.startswith(".head.") and name.endswith(".tmp")
        )
        for name in names:
            self._quarantine_authority_entry(
                name,
                source_parent=self._heads_descriptor,
            )

    @staticmethod
    def _read_authority_head_descriptor(
        descriptor: int,
        *,
        expected_name: str,
    ) -> LabLegacyAuthorityHead:
        opened = _FileObservation.from_stat(os.fstat(descriptor))
        if (
            opened.mode != stat.S_IFREG
            or opened.nlink != 1
            or stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600
        ):
            raise LabArtifactIntegrityError(
                "legacy authority generation head is not a private regular file"
            )
        payload = _read_descriptor(descriptor)
        try:
            head = strict_model_validate_canonical_json(LabLegacyAuthorityHead, payload)
        except Exception as exc:
            raise LabArtifactIntegrityError("legacy authority generation head is invalid") from exc
        if payload != head.canonical_json_bytes():
            raise LabArtifactIntegrityError("legacy authority generation head is not canonical")
        if expected_name != LegacyArtifactIndex._authority_head_file_name(head):
            raise LabArtifactIntegrityError(
                "legacy authority generation head filename conflicts with content"
            )
        return head

    def _read_bound_authority_head(self) -> LabLegacyAuthorityHead:
        opened = _FileObservation.from_stat(os.fstat(self._head_descriptor))
        at_path = _FileObservation.from_stat(
            os.stat(
                self._head_name,
                dir_fd=self._heads_descriptor,
                follow_symlinks=False,
            )
        )
        if (
            opened != at_path
            or opened != self._head_identity
            or opened.mode != stat.S_IFREG
            or opened.nlink != 1
            or stat.S_IMODE(os.fstat(self._head_descriptor).st_mode) != 0o600
        ):
            raise LabArtifactIntegrityError("legacy authority head identity or permissions changed")
        return self._read_authority_head_descriptor(
            self._head_descriptor,
            expected_name=self._head_name,
        )

    def _scan_authority_heads(
        self,
        state: _LegacyAuthorityState,
    ) -> tuple[str, int, _FileObservation, LabLegacyAuthorityHead] | None:
        self._quarantine_orphaned_authority_head_temps()
        expected = self._authority_heads_for_state(state)
        observed_by_sequence: dict[int, tuple[str, _FileObservation, LabLegacyAuthorityHead]] = {}
        for name in sorted(os.listdir(self._heads_descriptor)):
            if re.fullmatch(r"[0-9]{20}-[0-9a-f]{64}-[0-9]{20}\.json", name) is None:
                raise LabArtifactIntegrityError(
                    "legacy authority heads directory contains an unknown entry"
                )
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=self._heads_descriptor,
            )
            try:
                before = _FileObservation.from_stat(
                    os.stat(name, dir_fd=self._heads_descriptor, follow_symlinks=False)
                )
                opened = _FileObservation.from_stat(os.fstat(descriptor))
                if opened != before:
                    raise LabArtifactIntegrityError(
                        "legacy authority generation head changed while scanning"
                    )
                head = self._read_authority_head_descriptor(
                    descriptor,
                    expected_name=name,
                )
                if head.sequence >= len(expected) or head != expected[head.sequence]:
                    raise LabArtifactIntegrityError(
                        "legacy authority generation head conflicts with ledger"
                    )
                if head.sequence in observed_by_sequence:
                    raise LabArtifactConflictError(
                        "legacy authority generation has conflicting heads"
                    )
                observed_by_sequence[head.sequence] = (name, opened, head)
            finally:
                os.close(descriptor)
        if not observed_by_sequence:
            self._heads_identity = _FileObservation.from_stat(os.fstat(self._heads_descriptor))
            return None
        highest = max(observed_by_sequence)
        if set(observed_by_sequence) != set(range(highest + 1)):
            raise LabArtifactIntegrityError("legacy authority generation heads are not continuous")
        selected_name, selected_identity, selected_head = observed_by_sequence[highest]
        selected_descriptor = os.open(
            selected_name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=self._heads_descriptor,
        )
        try:
            selected_opened = _FileObservation.from_stat(os.fstat(selected_descriptor))
            selected_at_path = _FileObservation.from_stat(
                os.stat(
                    selected_name,
                    dir_fd=self._heads_descriptor,
                    follow_symlinks=False,
                )
            )
            if selected_opened != selected_identity or selected_at_path != selected_identity:
                raise LabArtifactIntegrityError(
                    "legacy authority selected head changed while binding"
                )
            self._heads_identity = _FileObservation.from_stat(os.fstat(self._heads_descriptor))
            result = selected_name, selected_descriptor, selected_opened, selected_head
            selected_descriptor = -1
            return result
        finally:
            if selected_descriptor >= 0:
                os.close(selected_descriptor)

    def _set_head_binding(
        self,
        selection: tuple[str, int, _FileObservation, LabLegacyAuthorityHead],
    ) -> None:
        name, descriptor, identity, _ = selection
        previous = self._head_descriptor
        self._head_name = name
        self._head_descriptor = descriptor
        self._head_identity = identity
        if previous >= 0:
            os.close(previous)

    @staticmethod
    def _atomic_authority_head_publish_noreplace(
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

    def _publish_authority_head(
        self,
        head: LabLegacyAuthorityHead,
        *,
        expected_current: LabLegacyAuthorityHead | None,
    ) -> None:
        self._quarantine_orphaned_authority_head_temps()
        destination_name = self._authority_head_file_name(head)
        temporary_name = f".head.{uuid4().hex}.tmp"
        descriptor = os.open(
            temporary_name,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=self._heads_descriptor,
        )
        published = False
        try:
            payload = head.canonical_json_bytes()
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise LabArtifactIntegrityError("legacy authority head write made no progress")
                offset += written
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            temporary = _FileObservation.from_stat(os.fstat(descriptor))
            temporary_at_path = _FileObservation.from_stat(
                os.stat(
                    temporary_name,
                    dir_fd=self._heads_descriptor,
                    follow_symlinks=False,
                )
            )
            try:
                rebuilt = strict_model_validate_canonical_json(
                    LabLegacyAuthorityHead, _read_descriptor(descriptor)
                )
            except Exception as exc:
                raise LabArtifactIntegrityError(
                    "legacy authority head candidate is invalid"
                ) from exc
            if (
                temporary != temporary_at_path
                or temporary.mode != stat.S_IFREG
                or temporary.nlink != 1
                or stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600
                or rebuilt != head
                or _read_descriptor(descriptor) != rebuilt.canonical_json_bytes()
            ):
                raise LabArtifactIntegrityError("legacy authority head candidate identity changed")
            if expected_current is None:
                if head.sequence != 0:
                    raise LabArtifactIntegrityError(
                        "legacy authority first generation head must be genesis"
                    )
            else:
                if self._read_bound_authority_head() != expected_current:
                    raise LabArtifactIntegrityError(
                        "legacy authority head changed before publication"
                    )
                if head.sequence != expected_current.sequence + 1:
                    raise LabArtifactIntegrityError(
                        "legacy authority head generations must be sequential"
                    )
            try:
                self._atomic_authority_head_publish_noreplace(
                    self._heads_descriptor,
                    temporary_name,
                    self._heads_descriptor,
                    destination_name,
                )
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    raise LabArtifactConflictError(
                        "legacy authority generation head already exists"
                    ) from exc
                raise
            published = True
            published_at_path = _FileObservation.from_stat(
                os.stat(
                    destination_name,
                    dir_fd=self._heads_descriptor,
                    follow_symlinks=False,
                )
            )
            published_fd = _FileObservation.from_stat(os.fstat(descriptor))
            if published_at_path != published_fd:
                raise LabArtifactIntegrityError(
                    "legacy authority head publication identity changed"
                )
            os.fsync(self._heads_descriptor)
            self._heads_identity = _FileObservation.from_stat(os.fstat(self._heads_descriptor))
            previous_descriptor = self._head_descriptor
            self._head_name = destination_name
            self._head_descriptor = descriptor
            self._head_identity = published_fd
            descriptor = -1
            if previous_descriptor >= 0:
                os.close(previous_descriptor)
        except BaseException:
            if published:
                os.fsync(self._heads_descriptor)
            raise
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _read_legacy_authority_head(self) -> LabLegacyAuthorityHead | None:
        try:
            descriptor = os.open(
                self._legacy_authority_head_name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=self._parent_descriptor,
            )
        except FileNotFoundError:
            return None
        try:
            before = _FileObservation.from_stat(
                os.stat(
                    self._legacy_authority_head_name,
                    dir_fd=self._parent_descriptor,
                    follow_symlinks=False,
                )
            )
            opened = _FileObservation.from_stat(os.fstat(descriptor))
            if opened != before:
                raise LabArtifactIntegrityError(
                    "legacy single authority head changed while opening"
                )
            payload = _read_descriptor(descriptor)
            try:
                head = strict_model_validate_canonical_json(LabLegacyAuthorityHead, payload)
            except Exception as exc:
                raise LabArtifactIntegrityError("legacy single authority head is invalid") from exc
            if (
                payload != head.canonical_json_bytes()
                or opened.mode != stat.S_IFREG
                or opened.nlink != 1
                or stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600
            ):
                raise LabArtifactIntegrityError("legacy single authority head is unsafe")
            return head
        finally:
            os.close(descriptor)

    def _bind_or_create_authority_head(self) -> None:
        payload = _read_descriptor(self._authority_descriptor)
        complete = self._complete_authority_payload(payload)
        state = self._parse_authority_payload(complete)
        selection = self._scan_authority_heads(state)
        if selection is not None:
            self._set_head_binding(selection)
        legacy_head = self._read_legacy_authority_head()
        expected_heads = self._authority_heads_for_state(state)
        if legacy_head is not None:
            if (
                legacy_head.sequence >= len(expected_heads)
                or legacy_head != expected_heads[legacy_head.sequence]
            ):
                raise LabArtifactIntegrityError(
                    "legacy single authority head conflicts with ledger"
                )
            current_sequence = (
                self._read_bound_authority_head().sequence if self._head_descriptor >= 0 else -1
            )
            for sequence in range(current_sequence + 1, legacy_head.sequence + 1):
                expected_current = expected_heads[sequence - 1] if sequence > 0 else None
                self._publish_authority_head(
                    expected_heads[sequence],
                    expected_current=expected_current,
                )
            self._quarantine_authority_entry(
                self._legacy_authority_head_name,
                source_parent=self._parent_descriptor,
            )
        elif self._head_descriptor < 0:
            if state.sequence != 0:
                raise LabArtifactIntegrityError(
                    "legacy authority generation heads are missing for a non-empty ledger"
                )
            self._publish_authority_head(expected_heads[0], expected_current=None)

    def _refresh_authority_head_binding(self) -> bool:
        payload = _read_descriptor(self._authority_descriptor)
        state = self._parse_authority_payload(self._complete_authority_payload(payload))
        selection = self._scan_authority_heads(state)
        if selection is None:
            raise LabArtifactIntegrityError("legacy authority generation heads are missing")
        name, descriptor, identity, head = selection
        current = self._read_bound_authority_head()
        if head.sequence < current.sequence:
            os.close(descriptor)
            raise LabArtifactIntegrityError("legacy authority head rollback detected")
        if name == self._head_name and identity == self._head_identity:
            os.close(descriptor)
            return False
        self._set_head_binding((name, descriptor, identity, head))
        return True

    def _refresh_cache_bindings_if_authoritative(
        self,
        authority: _LegacyAuthorityState,
    ) -> bool:
        try:
            database_at_path = _FileObservation.from_stat(
                os.stat(
                    self.path.name,
                    dir_fd=self._parent_descriptor,
                    follow_symlinks=False,
                )
            )
            journal_at_path = _FileObservation.from_stat(
                os.stat(
                    f"{self.path.name}-journal",
                    dir_fd=self._parent_descriptor,
                    follow_symlinks=False,
                )
            )
            database_opened = _FileObservation.from_stat(os.fstat(self._database_descriptor))
            journal_opened = _FileObservation.from_stat(os.fstat(self._journal_descriptor))
        except OSError as exc:
            raise LabArtifactIntegrityError("legacy cache path identity changed") from exc
        if database_at_path == database_opened and journal_at_path == journal_opened:
            return False

        replacements: list[tuple[str, int, _FileObservation]] = []
        try:
            for name in (self.path.name, f"{self.path.name}-journal"):
                label = "database" if name == self.path.name else "journal"
                try:
                    at_path = _FileObservation.from_stat(
                        os.stat(name, dir_fd=self._parent_descriptor, follow_symlinks=False)
                    )
                    descriptor = os.open(
                        name,
                        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=self._parent_descriptor,
                    )
                except OSError as exc:
                    raise LabArtifactIntegrityError(
                        f"legacy index {label} identity changed"
                    ) from exc
                try:
                    opened = _FileObservation.from_stat(os.fstat(descriptor))
                    if (
                        opened != at_path
                        or opened.mode != stat.S_IFREG
                        or opened.nlink != 1
                        or stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600
                    ):
                        raise LabArtifactIntegrityError(f"legacy index {label} identity changed")
                    replacements.append((name, descriptor, opened))
                    descriptor = -1
                finally:
                    if descriptor >= 0:
                        os.close(descriptor)
            database_payload = _read_descriptor(replacements[0][1])
            journal_payload = _read_descriptor(replacements[1][1])
            if journal_payload:
                raise LabArtifactIntegrityError("legacy cache replacement journal is not empty")
            self._validate_serialized_cache(database_payload, authority)
            canonical_payload = self._build_serialized_cache(authority)
            if database_payload != canonical_payload:
                raise LabArtifactIntegrityError(
                    "legacy cache replacement was not a canonical authority rebuild"
                )
            for name, descriptor, opened in replacements:
                final_path = _FileObservation.from_stat(
                    os.stat(name, dir_fd=self._parent_descriptor, follow_symlinks=False)
                )
                final_opened = _FileObservation.from_stat(os.fstat(descriptor))
                if final_path != opened or final_opened != opened:
                    raise LabArtifactIntegrityError(
                        "legacy cache replacement changed while validating"
                    )
            self._assert_authority_identity()
        except BaseException:
            for _, descriptor, _ in replacements:
                os.close(descriptor)
            raise
        previous_database = self._database_descriptor
        previous_journal = self._journal_descriptor
        self._database_descriptor = replacements[0][1]
        self._database_identity = replacements[0][2]
        self._journal_descriptor = replacements[1][1]
        self._journal_identity = replacements[1][2]
        os.close(previous_database)
        os.close(previous_journal)
        self._assert_index_identity()
        return True

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
            or stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600
            or stat.S_IMODE(
                os.stat(
                    name,
                    dir_fd=self._parent_descriptor,
                    follow_symlinks=False,
                ).st_mode
            )
            != 0o600
        ):
            raise LabArtifactIntegrityError(f"legacy index {label} identity or permissions changed")

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
            permissions = stat.S_IMODE(
                os.stat(
                    f"{self.path.name}{suffix}",
                    dir_fd=self._parent_descriptor,
                    follow_symlinks=False,
                ).st_mode
            )
            if observed.mode != stat.S_IFREG or observed.nlink != 1 or permissions != 0o600:
                raise LabArtifactIntegrityError("legacy index sidecar identity is unsafe")

    def _assert_authority_identity(self) -> None:
        current_parent_descriptor = -1
        main_error: BaseException | None = None
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
            if (
                stat.S_IMODE(os.fstat(self._parent_descriptor).st_mode) != 0o700
                or stat.S_IMODE(os.fstat(current_parent_descriptor).st_mode) != 0o700
            ):
                raise LabArtifactIntegrityError(
                    "legacy index parent permissions must be exactly 0700"
                )
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
            heads_fd = _FileObservation.from_stat(os.fstat(self._heads_descriptor))
            heads_path = _FileObservation.from_stat(
                os.stat(
                    self._authority_heads_path.name,
                    dir_fd=self._parent_descriptor,
                    follow_symlinks=False,
                )
            )
            if (
                heads_fd != self._heads_identity
                or heads_path != self._heads_identity
                or heads_fd.mode != stat.S_IFDIR
                or stat.S_IMODE(os.fstat(self._heads_descriptor).st_mode) != 0o700
            ):
                raise LabArtifactIntegrityError("legacy authority heads directory identity changed")
            self._read_bound_authority_head()
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
            authority_quarantine_fd = _FileObservation.from_stat(
                os.fstat(self._authority_quarantine_descriptor)
            )
            authority_quarantine_path = _FileObservation.from_stat(
                os.stat(
                    self._authority_quarantine_path.name,
                    dir_fd=self._parent_descriptor,
                    follow_symlinks=False,
                )
            )
            if not self._same_index_entry(
                authority_quarantine_fd,
                self._authority_quarantine_identity,
                mode=stat.S_IFDIR,
            ) or not self._same_index_entry(
                authority_quarantine_path,
                self._authority_quarantine_identity,
                mode=stat.S_IFDIR,
            ):
                raise LabArtifactIntegrityError(
                    "legacy index authority quarantine identity changed"
                )
            if (
                stat.S_IMODE(os.fstat(self._cache_quarantine_descriptor).st_mode) != 0o700
                or stat.S_IMODE(
                    os.stat(
                        self._cache_quarantine_path.name,
                        dir_fd=self._parent_descriptor,
                        follow_symlinks=False,
                    ).st_mode
                )
                != 0o700
                or stat.S_IMODE(os.fstat(self._authority_quarantine_descriptor).st_mode) != 0o700
                or stat.S_IMODE(
                    os.stat(
                        self._authority_quarantine_path.name,
                        dir_fd=self._parent_descriptor,
                        follow_symlinks=False,
                    ).st_mode
                )
                != 0o700
            ):
                raise LabArtifactIntegrityError(
                    "legacy index quarantine permissions must be exactly 0700"
                )
        except LabArtifactError as exc:
            main_error = exc
        except (AttributeError, OSError) as exc:
            main_error = LabArtifactIntegrityError("legacy index authority identity changed")
            main_error.__cause__ = exc
        except BaseException as exc:
            main_error = exc
        finally:
            cleanup_errors: list[BaseException] = []
            if current_parent_descriptor >= 0:
                try:
                    _close_descriptor_fail_closed(
                        current_parent_descriptor,
                        label="legacy index current parent descriptor",
                    )
                except BaseException as close_error:
                    cleanup_errors.append(close_error)
            errors = [main_error, *cleanup_errors] if main_error is not None else cleanup_errors
            _raise_collected_errors(
                "legacy authority identity check and descriptor cleanup failed",
                errors,
            )

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
    def _legacy_operation_lifecycle(self) -> Iterator[None]:
        current_thread_id = threading.get_ident()
        with self._lifecycle_condition:
            if self._closing or self._closed:
                raise LabArtifactIntegrityError("legacy index is closing or closed")
            self._active_operations += 1
            self._operation_threads[current_thread_id] = (
                self._operation_threads.get(current_thread_id, 0) + 1
            )
        try:
            with self._legacy_process_operation_lock():
                if self._closed:
                    raise LabArtifactIntegrityError("legacy index is closed")
                yield
        finally:
            with self._lifecycle_condition:
                self._active_operations -= 1
                remaining = self._operation_threads[current_thread_id] - 1
                if remaining == 0:
                    del self._operation_threads[current_thread_id]
                else:
                    self._operation_threads[current_thread_id] = remaining
                self._lifecycle_condition.notify_all()

    @contextmanager
    def _exclusive_index_lock(self) -> Iterator[None]:
        with self._legacy_operation_lifecycle():
            lock_acquired = False
            depth_incremented = False
            operation_error: BaseException | None = None
            try:
                _acquire_exclusive_flock(
                    self._lock_descriptor,
                    label="legacy index operation lock",
                )
                lock_acquired = True
                self._authority_lock_depth += 1
                depth_incremented = True
                self._refresh_authority_head_binding()
                self._assert_authority_identity()
                authority = self._read_authority_state()
                self._refresh_cache_bindings_if_authoritative(authority)
                self._assert_index_identity()
                self._ensure_cache_ready(authority)
                caller_error: BaseException | None = None
                try:
                    yield
                except BaseException as exc:
                    caller_error = exc
                integrity_error: BaseException | None = None
                try:
                    self._assert_index_identity()
                except BaseException as exc:
                    integrity_error = exc
                if caller_error is not None and integrity_error is not None:
                    if isinstance(caller_error, Exception) and isinstance(
                        integrity_error,
                        Exception,
                    ):
                        raise ExceptionGroup(
                            "legacy index operation and final identity check both failed",
                            [caller_error, integrity_error],
                        ) from None
                    raise BaseExceptionGroup(
                        "legacy index operation and final identity check both failed",
                        [caller_error, integrity_error],
                    ) from None
                if integrity_error is not None:
                    raise integrity_error
                if caller_error is not None:
                    raise caller_error
            except BaseException as exc:
                operation_error = exc
            finally:
                cleanup_errors: list[BaseException] = []
                if depth_incremented:
                    self._authority_lock_depth -= 1
                if lock_acquired:
                    try:
                        fcntl.flock(self._lock_descriptor, fcntl.LOCK_UN)
                    except BaseException as unlock_error:
                        cleanup_errors.append(unlock_error)
                errors = (
                    [operation_error, *cleanup_errors]
                    if operation_error is not None
                    else cleanup_errors
                )
                _raise_collected_errors(
                    "legacy index operation and lock cleanup failed",
                    errors,
                )

    @staticmethod
    def _before_sqlite_connect() -> None:
        """Fault-injection boundary before SQLite opens its path."""

    @staticmethod
    def _after_sqlite_connect(_connection: sqlite3.Connection) -> None:
        """Fault-injection boundary after SQLite opens its path."""

    def _connect(self) -> sqlite3.Connection:
        self._assert_index_identity()
        self._before_sqlite_connect()
        self._assert_index_identity()
        try:
            database = f"{self.path.as_uri()}?mode=ro"
            connection = sqlite3.connect(
                database,
                timeout=30,
                isolation_level=None,
                uri=True,
            )
            self._after_sqlite_connect(connection)
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
        except BaseException as error:
            if "connection" in locals():
                try:
                    connection.close()
                except BaseException as cleanup_error:
                    if isinstance(error, Exception) and isinstance(
                        cleanup_error,
                        Exception,
                    ):
                        raise ExceptionGroup(
                            "legacy SQLite connection and close both failed",
                            [error, cleanup_error],
                        ) from None
                    raise BaseExceptionGroup(
                        "legacy SQLite connection and close both failed",
                        [error, cleanup_error],
                    ) from None
            raise

    @contextmanager
    def _cache_connection(self) -> Iterator[sqlite3.Connection]:
        connection: sqlite3.Connection | None = None
        operation_error: BaseException | None = None
        try:
            connection = self._connect()
            yield connection
        except BaseException as exc:
            operation_error = exc
        finally:
            cleanup_errors: list[BaseException] = []
            if connection is not None:
                try:
                    connection.close()
                except BaseException as close_error:
                    cleanup_errors.append(close_error)
            errors = (
                [operation_error, *cleanup_errors]
                if operation_error is not None
                else cleanup_errors
            )
            _raise_collected_errors(
                "legacy cache operation and connection cleanup failed",
                errors,
            )

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

    def _validate_serialized_cache(
        self,
        payload: bytes,
        authority: _LegacyAuthorityState,
    ) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        try:
            connection.deserialize(payload)
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            if integrity != ("ok",) or not self._cache_matches_authority(connection, authority):
                raise LabArtifactIntegrityError("serialized legacy cache differs from authority")
        except sqlite3.Error as exc:
            raise LabArtifactIntegrityError(
                "serialized legacy cache cannot be deserialized"
            ) from exc
        finally:
            connection.close()

    def _build_serialized_cache(self, authority: _LegacyAuthorityState) -> bytes:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        try:
            self._populate_cache(connection, authority)
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            if integrity != ("ok",):
                raise LabArtifactIntegrityError("in-memory legacy cache failed integrity check")
            payload = connection.serialize()
        finally:
            connection.close()
        self._validate_serialized_cache(payload, authority)
        return payload

    @staticmethod
    def _after_cache_temp_bound(_name: str, _descriptor: int) -> None:
        """Fault-injection boundary after a cache temp inode is fd-bound."""

    def _rebuild_cache(self, authority: _LegacyAuthorityState) -> None:
        self._assert_authority_identity()
        serialized = self._build_serialized_cache(authority)
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
        temporary_descriptor = os.open(
            temporary_name,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=self._parent_descriptor,
        )
        operation_error: BaseException | None = None
        try:
            initial_identity = _FileObservation.from_stat(os.fstat(temporary_descriptor))
            if initial_identity.mode != stat.S_IFREG or initial_identity.nlink != 1:
                raise LabArtifactIntegrityError(
                    "rebuilt legacy cache candidate is not a regular file"
                )
            os.fchmod(temporary_descriptor, 0o600)
            self._after_cache_temp_bound(temporary_name, temporary_descriptor)
            offset = 0
            while offset < len(serialized):
                written = os.write(temporary_descriptor, serialized[offset:])
                if written <= 0:
                    raise LabArtifactIntegrityError("rebuilt legacy cache write made no progress")
                offset += written
            os.fsync(temporary_descriptor)
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
                != (initial_identity.device, initial_identity.inode)
                or rebuilt_identity.mode != stat.S_IFREG
                or rebuilt_identity.nlink != 1
                or stat.S_IMODE(os.fstat(temporary_descriptor).st_mode) != 0o600
            ):
                raise LabArtifactIntegrityError("rebuilt legacy cache candidate identity changed")
            persisted = _read_descriptor(temporary_descriptor)
            if persisted != serialized:
                raise LabArtifactIntegrityError("rebuilt legacy cache candidate bytes changed")
            self._validate_serialized_cache(persisted, authority)
            self._assert_authority_identity()
            _rename_noreplace(
                self._parent_descriptor,
                temporary_name,
                self._parent_descriptor,
                self.path.name,
            )
            os.fsync(self._parent_descriptor)
            self._database_descriptor = temporary_descriptor
            temporary_descriptor = -1
        except BaseException as exc:
            operation_error = exc
        finally:
            cleanup_error: BaseException | None = None
            if temporary_descriptor >= 0:
                try:
                    os.close(temporary_descriptor)
                except BaseException as exc:
                    cleanup_error = exc
            if operation_error is not None and cleanup_error is not None:
                if isinstance(operation_error, Exception) and isinstance(
                    cleanup_error,
                    Exception,
                ):
                    raise ExceptionGroup(
                        "legacy cache rebuild and descriptor close both failed",
                        [operation_error, cleanup_error],
                    ) from None
                raise BaseExceptionGroup(
                    "legacy cache rebuild and descriptor close both failed",
                    [operation_error, cleanup_error],
                ) from None
            if cleanup_error is not None:
                raise cleanup_error
        if operation_error is not None:
            raise operation_error
        self._database_identity = _FileObservation.from_stat(os.fstat(self._database_descriptor))
        self._journal_descriptor, _ = _open_or_create_private_regular_at(
            self._parent_descriptor,
            f"{self.path.name}-journal",
            access_flags=os.O_RDONLY,
        )
        self._journal_identity = _FileObservation.from_stat(os.fstat(self._journal_descriptor))
        self._assert_index_identity()

    def _ensure_cache_ready(self, authority: _LegacyAuthorityState) -> None:
        self._refresh_cache_bindings_if_authoritative(authority)
        try:
            with self._cache_connection() as connection:
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
        if payload and not payload.endswith(b"\n"):
            raise LabArtifactIntegrityError(
                "legacy authority parser requires a complete ledger prefix"
            )
        latest: dict[str, LabLegacyAuthorityEvent] = {}
        generations: dict[str, int] = {}
        events: list[LabLegacyAuthorityEvent] = []
        previous_hash = _LEGACY_GENESIS_HASH
        for raw_line in payload.splitlines():
            try:
                event = strict_model_validate_canonical_json(LabLegacyAuthorityEvent, raw_line)
            except Exception as exc:
                raise LabArtifactIntegrityError("legacy authority ledger is invalid") from exc
            if raw_line != event.canonical_json_bytes():
                raise LabArtifactIntegrityError("legacy authority ledger is not canonical")
            expected_sequence = len(events) + 1
            if event.sequence != expected_sequence or event.previous_hash != previous_hash:
                raise LabArtifactIntegrityError(
                    "legacy authority hash chain or sequence is invalid"
                )
            previous = latest.get(event.logical_run_id)
            prior_generation = generations.get(event.logical_run_id, 0)
            if event.event_type == "staged":
                if previous is not None and previous.event_type in {"staged", "published"}:
                    raise LabArtifactIntegrityError("legacy authority transition is invalid")
                if event.generation != prior_generation + 1:
                    raise LabArtifactIntegrityError("legacy authority generation is invalid")
                generations[event.logical_run_id] = event.generation
            elif event.event_type in {"published", "abandoned"}:
                if (
                    previous is None
                    or previous.event_type != "staged"
                    or previous.operation_id != event.operation_id
                    or previous.generation != event.generation
                    or previous.record != event.record
                ):
                    raise LabArtifactIntegrityError("legacy authority transition is invalid")
            else:
                if (
                    previous is None
                    or previous.event_type != "published"
                    or previous.operation_id != event.operation_id
                    or previous.generation != event.generation
                    or previous.record != event.record
                ):
                    raise LabArtifactIntegrityError("legacy authority transition is invalid")
            latest[event.logical_run_id] = event
            events.append(event)
            previous_hash = event.event_hash
        return _LegacyAuthorityState(
            latest=latest,
            generations=generations,
            events=tuple(events),
            sequence=len(events),
            final_hash=previous_hash,
            ledger_size=len(payload),
        )

    @staticmethod
    def _authority_head_for_state(
        state: _LegacyAuthorityState,
    ) -> LabLegacyAuthorityHead:
        return LabLegacyAuthorityHead(
            sequence=state.sequence,
            final_hash=state.final_hash,
            ledger_size=state.ledger_size,
        )

    @staticmethod
    def _head_matches_state(
        head: LabLegacyAuthorityHead,
        state: _LegacyAuthorityState,
    ) -> bool:
        return (
            head.sequence,
            head.final_hash,
            head.ledger_size,
        ) == (
            state.sequence,
            state.final_hash,
            state.ledger_size,
        )

    def _read_authority_state(self) -> _LegacyAuthorityState:
        if self._authority_lock_depth <= 0:
            raise LabArtifactIntegrityError("legacy authority ledger requires the exclusive lock")
        self._assert_authority_identity()
        payload = _read_descriptor(self._authority_descriptor)
        head = self._read_bound_authority_head()
        self._assert_authority_identity()
        if head.ledger_size > len(payload):
            raise LabArtifactIntegrityError(
                "legacy authority ledger rollback detected by durable head"
            )
        if head.ledger_size > 0 and payload[head.ledger_size - 1 : head.ledger_size] != b"\n":
            raise LabArtifactIntegrityError(
                "legacy authority head does not end on an event boundary"
            )
        prefix = payload[: head.ledger_size]
        prefix_state = self._parse_authority_payload(prefix)
        if not self._head_matches_state(head, prefix_state):
            raise LabArtifactIntegrityError(
                "legacy authority ledger prefix conflicts with durable head"
            )
        has_partial_tail = bool(payload) and not payload.endswith(b"\n")
        if has_partial_tail:
            final_newline = payload.rfind(b"\n")
            complete = payload[: final_newline + 1] if final_newline >= 0 else b""
        else:
            complete = payload
        complete_state = self._parse_authority_payload(complete)
        if complete_state.sequence < head.sequence or complete_state.ledger_size < head.ledger_size:
            raise LabArtifactIntegrityError(
                "legacy authority ledger rollback detected by durable head"
            )
        if complete_state.sequence == head.sequence and not self._head_matches_state(
            head, complete_state
        ):
            raise LabArtifactIntegrityError("legacy authority ledger conflicts with durable head")
        if complete_state.sequence > head.sequence:
            if complete_state.sequence != head.sequence + 1:
                raise LabArtifactIntegrityError(
                    "legacy authority audit heads are missing beyond one crash-recovery event"
                )
            recovered_heads = self._authority_heads_for_state(complete_state)
            for sequence in range(head.sequence + 1, complete_state.sequence + 1):
                recovered_head = recovered_heads[sequence]
                self._publish_authority_head(
                    recovered_head,
                    expected_current=head,
                )
                head = recovered_head
        if has_partial_tail:
            if not self._head_matches_state(head, complete_state):
                raise LabArtifactIntegrityError(
                    "legacy authority partial tail is not anchored by durable head"
                )
            os.ftruncate(self._authority_descriptor, len(complete))
            os.fsync(self._authority_descriptor)
            self._assert_authority_identity()
            payload = _read_descriptor(self._authority_descriptor)
            if payload != complete:
                raise LabArtifactIntegrityError("legacy authority tail repair was not durable")
        return complete_state

    @staticmethod
    def _after_ledger_fsync_before_head_publish() -> None:
        """Fault-injection boundary after an event is durable but head is older."""

    def _append_authority_event(
        self,
        *,
        event_type: Literal["staged", "published", "abandoned", "invalidated"],
        logical_run_id: str,
        operation_id: UUID,
        generation: int,
        record: LabLegacyArtifactRecord,
        occurred_at: datetime,
    ) -> LabLegacyAuthorityEvent:
        state = self._read_authority_state()
        event = LabLegacyAuthorityEvent.create(
            event_type=event_type,
            sequence=state.sequence + 1,
            previous_hash=state.final_hash,
            logical_run_id=logical_run_id,
            operation_id=operation_id,
            generation=generation,
            record=record,
            occurred_at=occurred_at,
        )
        payload = event.canonical_json_bytes() + b"\n"
        if os.lseek(self._authority_descriptor, 0, os.SEEK_END) != state.ledger_size:
            raise LabArtifactIntegrityError("legacy authority ledger size changed before append")
        offset = 0
        while offset < len(payload):
            written = os.write(self._authority_descriptor, payload[offset:])
            if written <= 0:
                raise LabArtifactIntegrityError("legacy authority append made no progress")
            offset += written
        os.fsync(self._authority_descriptor)
        self._after_ledger_fsync_before_head_publish()
        self._assert_index_identity()
        appended_payload = _read_descriptor(self._authority_descriptor)
        appended_state = self._parse_authority_payload(appended_payload)
        if (
            appended_state.sequence != event.sequence
            or appended_state.final_hash != event.event_hash
        ):
            raise LabArtifactIntegrityError(
                "legacy authority event append did not produce the expected chain"
            )
        self._publish_authority_head(
            self._authority_head_for_state(appended_state),
            expected_current=self._authority_head_for_state(state),
        )
        self._assert_index_identity()
        return event

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

    def _published_source_matches(self, event: LabLegacyAuthorityEvent) -> bool:
        record = event.record
        try:
            with _open_bound_readonly_file(
                record.source_path,
                label="published legacy artifact source",
            ) as bound:
                payload = _read_descriptor(bound.descriptor)
                _assert_bound_readonly_file(bound, label="published legacy artifact source")
                return self._legacy_record_matches(record, bound.file_identity, payload)
        except (LabArtifactError, OSError):
            return False

    def _invalidate_published_event(
        self,
        event: LabLegacyAuthorityEvent,
    ) -> LabLegacyAuthorityEvent:
        current = self._read_authority_state().latest.get(event.logical_run_id)
        if (
            current is not None
            and current.event_type == "invalidated"
            and current.operation_id == event.operation_id
            and current.generation == event.generation
            and current.record == event.record
        ):
            return current
        if current != event or event.event_type != "published":
            raise LabArtifactIntegrityError(
                "legacy published source authority changed before invalidation"
            )
        return self._append_authority_event(
            event_type="invalidated",
            logical_run_id=event.logical_run_id,
            operation_id=event.operation_id,
            generation=event.generation,
            record=event.record,
            occurred_at=self._clock_utc(),
        )

    def _reconcile_published_sources(
        self,
        authority: _LegacyAuthorityState,
    ) -> _LegacyAuthorityState:
        changed = False
        for event in self._published_authority_events(authority):
            if self._published_source_matches(event):
                continue
            self._invalidate_published_event(event)
            changed = True
        return self._read_authority_state() if changed else authority

    def get(self, logical_run_id: str) -> LabLegacyArtifactRecord | None:
        logical_run_id = _normalize_logical_run_id(logical_run_id)
        with self._exclusive_index_lock():
            authority = self._reconcile_published_sources(self._read_authority_state())
            self._ensure_cache_ready(authority)
            event = authority.latest.get(logical_run_id)
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
                        self._invalidate_published_event(event)
                        self._ensure_cache_ready(self._read_authority_state())
                        return None
            except LabArtifactError:
                self._invalidate_published_event(event)
                self._ensure_cache_ready(self._read_authority_state())
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

    @staticmethod
    def _after_published_authority_commit(_record: LabLegacyArtifactRecord) -> None:
        """Fault-injection boundary after published ledger and head fsync."""

    @staticmethod
    def _after_import_cache_sync(_record: LabLegacyArtifactRecord) -> None:
        """Fault-injection boundary before the bound source can be returned."""

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
        logical_run_id = _normalize_logical_run_id(logical_run_id)
        source = _secure_absolute_path(source_path)
        media_type = self._media_type(source)
        staged: LabLegacyAuthorityEvent | None = None
        published: LabLegacyAuthorityEvent | None = None
        occurred_at: datetime | None = None
        try:
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
                if (
                    previous is not None
                    and previous.event_type == "published"
                    and not self._published_source_matches(previous)
                ):
                    self._invalidate_published_event(previous)
                    authority = self._read_authority_state()
                    self._ensure_cache_ready(authority)
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
                    published = previous
                    result = LabLegacyIndexResult(status="reused", record=existing)
                else:
                    if previous is not None and previous.event_type == "staged":
                        self._append_authority_event(
                            event_type="abandoned",
                            logical_run_id=previous.logical_run_id,
                            operation_id=previous.operation_id,
                            generation=previous.generation,
                            record=previous.record,
                            occurred_at=occurred_at,
                        )
                    generation = authority.generations.get(logical_run_id, 0) + 1
                    operation_id = uuid4()
                    staged = self._append_authority_event(
                        event_type="staged",
                        logical_run_id=logical_run_id,
                        operation_id=operation_id,
                        generation=generation,
                        record=record,
                        occurred_at=occurred_at,
                    )
                    self._before_commit_source_check(source, observation)
                    _assert_bound_readonly_file(bound, label="legacy artifact source")
                    self._after_stage_commit(record)
                    self._before_commit_source_check(source, observation)
                    _assert_bound_readonly_file(bound, label="legacy artifact source")
                    published = self._append_authority_event(
                        event_type="published",
                        logical_run_id=staged.logical_run_id,
                        operation_id=staged.operation_id,
                        generation=staged.generation,
                        record=staged.record,
                        occurred_at=self._clock_utc(),
                    )
                    self._after_published_authority_commit(record)
                    self._before_commit_source_check(source, observation)
                    _assert_bound_readonly_file(bound, label="legacy artifact source")
                    result = LabLegacyIndexResult(status="imported", record=record)
                self._ensure_cache_ready(self._read_authority_state())
                self._after_import_cache_sync(result.record)
                self._before_commit_source_check(source, observation)
                _assert_bound_readonly_file(bound, label="legacy artifact source")
                return result
        except BaseException as error:
            cleanup_error: BaseException | None = None
            try:
                if published is not None and not self._published_source_matches(published):
                    self._invalidate_published_event(published)
                    self._ensure_cache_ready(self._read_authority_state())
                elif staged is not None and occurred_at is not None:
                    current = self._read_authority_state().latest.get(logical_run_id)
                    if (
                        current is not None
                        and current.event_type == "staged"
                        and current.operation_id == staged.operation_id
                        and current.generation == staged.generation
                    ):
                        self._append_authority_event(
                            event_type="abandoned",
                            logical_run_id=staged.logical_run_id,
                            operation_id=staged.operation_id,
                            generation=staged.generation,
                            record=staged.record,
                            occurred_at=occurred_at,
                        )
            except BaseException as exc:
                cleanup_error = exc
            if cleanup_error is not None:
                if isinstance(error, Exception) and isinstance(cleanup_error, Exception):
                    raise ExceptionGroup(
                        "legacy import and authority cleanup both failed",
                        [error, cleanup_error],
                    ) from None
                raise BaseExceptionGroup(
                    "legacy import and authority cleanup both failed",
                    [error, cleanup_error],
                ) from None
            if isinstance(error, BaseExceptionGroup) and all(
                isinstance(item, LabArtifactIntegrityError) for item in error.exceptions
            ):
                raise LabArtifactIntegrityError(
                    "legacy source changed during index publication"
                ) from error
            raise
