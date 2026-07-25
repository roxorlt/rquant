"""Immutable, deterministic job-level artifact bundles for Strategy Lab."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import math
import os
import re
import sqlite3
import stat
from collections.abc import Callable, Mapping
from contextlib import suppress
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
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical datetime values must be timezone-aware")
        normalized = value.astimezone(UTC)
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
        required = {"spec.json", "metrics.json", "report.md"}
        if not required.issubset(paths):
            raise ValueError("manifest is missing required result files")
        if not any(path.startswith("tables/") for path in paths):
            raise ValueError("manifest requires at least one complete Parquet table")
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
    file_identities: tuple[LabArtifactFileIdentity, ...]


class LabSealedJobArtifact(LabArtifactModel):
    path: Path
    manifest: LabJobArtifactManifest
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    reused_existing: bool = False


class LabArtifactIndexEvidence(LabArtifactModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    sealed_path: Path
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    complete_result_hash: str = Field(pattern=_HASH_PATTERN)
    bundle_device: int = Field(ge=0)
    bundle_inode: int = Field(ge=1)
    indexed_at: datetime

    @model_validator(mode="after")
    def validate_indexed_at(self) -> LabArtifactIndexEvidence:
        if self.indexed_at.tzinfo is None or self.indexed_at.utcoffset() is None:
            raise ValueError("indexed_at must be timezone-aware")
        return self


class LabArtifactRecoveryRecord(LabArtifactModel):
    path: Path
    status: Literal["recoverable", "invalid", "quarantined"]
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


class _FileObservation(LabArtifactModel):
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    mode: int = Field(ge=0)
    nlink: int = Field(ge=1)
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


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_private_directory(path: Path, *, manage_existing: bool = True) -> None:
    created = False
    if os.path.lexists(path):
        observed = path.lstat()
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise LabArtifactPathError(f"managed artifact directory is unsafe: {path}")
    else:
        path.mkdir(parents=True, mode=0o700)
        created = True
    if created or manage_existing:
        os.chmod(path, 0o700)


def _write_private_bytes(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(payload):
            offset += os.write(descriptor, payload[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_regular_file(path: Path, *, label: str) -> tuple[bytes, _FileObservation]:
    parent_descriptor = -1
    descriptor = -1
    try:
        parent_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        parent_descriptor = os.open(path.parent, parent_flags)
        parent_before = _FileObservation.from_stat(os.fstat(parent_descriptor))
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
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        after_open = _FileObservation.from_stat(os.fstat(descriptor))
        after_path = _FileObservation.from_stat(path.lstat())
        parent_after = _FileObservation.from_stat(os.fstat(parent_descriptor))
        parent_path_after = _FileObservation.from_stat(path.parent.lstat())
    except LabArtifactIntegrityError:
        raise
    except OSError as exc:
        raise LabArtifactIntegrityError(f"{label} cannot be read safely") from exc
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)
        if parent_descriptor >= 0:
            with suppress(OSError):
                os.close(parent_descriptor)
    if opened != after_open or opened != after_path:
        raise LabArtifactIntegrityError(f"{label} changed while reading")
    if parent_before != parent_after or parent_before != parent_path_after:
        raise LabArtifactIntegrityError(f"{label} parent changed while reading")
    return b"".join(chunks), opened


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


def _file_identity(path: Path, relative_path: str) -> LabArtifactFileIdentity:
    observed = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
        raise LabArtifactIntegrityError(f"unsafe artifact file: {relative_path}")
    return LabArtifactFileIdentity(
        relative_path=relative_path,
        device=observed.st_dev,
        inode=observed.st_ino,
        size=observed.st_size,
        mtime_ns=observed.st_mtime_ns,
        ctime_ns=observed.st_ctime_ns,
    )


class LabJobArtifactStore:
    """Create and verify complete job artifacts without touching scheduler state."""

    def __init__(self, root: Path) -> None:
        self.root = root.absolute()
        self.candidates_root = self.root / "candidates"
        self.sealed_root = self.root / "sealed"
        self.quarantine_root = self.root / "quarantine"
        for path in (
            self.root,
            self.candidates_root,
            self.sealed_root,
            self.quarantine_root,
        ):
            _ensure_private_directory(path)

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
    def _write_parquet(path: Path, table_name: str, frame: pd.DataFrame) -> LabJobArtifactFile:
        frame.to_parquet(path, index=False)
        os.chmod(path, 0o600)
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        payload, _ = _read_regular_file(path, label=f"candidate table {table_name}")
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
        return LabJobArtifactFile(
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
        )

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
        candidate_path = self.candidates_root / f"{job_id.hex}-{uuid4().hex}"
        candidate_path.mkdir(mode=0o700)
        tables_path = candidate_path / "tables"
        tables_path.mkdir(mode=0o700)
        try:
            spec_bytes = spec.canonical_json().encode("utf-8")
            payloads: dict[str, tuple[str, bytes]] = {
                "spec.json": ("application/json", spec_bytes),
                "metrics.json": ("application/json", metrics_bytes),
                "report.md": ("text/markdown; charset=utf-8", report_bytes),
            }
            files: list[LabJobArtifactFile] = []
            for relative_path, (media_type, payload) in sorted(payloads.items()):
                _write_private_bytes(candidate_path / relative_path, payload)
                files.append(
                    LabJobArtifactFile(
                        relative_path=relative_path,
                        media_type=media_type,
                        size=len(payload),
                        sha256=_sha256(payload),
                    )
                )
            for table_name in sorted(tables):
                files.append(
                    self._write_parquet(
                        tables_path / f"{table_name}.parquet",
                        table_name,
                        tables[table_name],
                    )
                )
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
            _write_private_bytes(candidate_path / "manifest.json", manifest_bytes)
            sums = {item.relative_path: item.sha256 for item in manifest.files}
            sums["manifest.json"] = _sha256(manifest_bytes)
            sums_bytes = "".join(
                f"{digest}  {relative_path}\n" for relative_path, digest in sorted(sums.items())
            ).encode("ascii")
            _write_private_bytes(candidate_path / "SHA256SUMS", sums_bytes)
            _fsync_directory(tables_path)
            _fsync_directory(candidate_path)
            _fsync_directory(self.candidates_root)
            candidate = self._candidate_from_path(candidate_path)
            self.verify_candidate(candidate)
            return candidate
        except Exception:
            # A failed candidate remains isolated for explicit operator recovery.
            with suppress(OSError):
                _fsync_directory(candidate_path)
            raise

    def _assert_managed_child(self, path: Path, parent: Path, *, label: str) -> Path:
        absolute = path.absolute()
        if absolute.parent != parent or absolute.name in {"", ".", ".."}:
            raise LabArtifactPathError(f"{label} is outside its managed root")
        return absolute

    @staticmethod
    def _directory_observation(path: Path, *, label: str) -> _FileObservation:
        try:
            observed = path.lstat()
        except OSError as exc:
            raise LabArtifactIntegrityError(f"{label} is missing") from exc
        identity = _FileObservation.from_stat(observed)
        if identity.mode != stat.S_IFDIR or path.is_symlink():
            raise LabArtifactIntegrityError(f"{label} is not a regular directory")
        return identity

    @staticmethod
    def _expected_paths(manifest: LabJobArtifactManifest) -> set[str]:
        return {
            "manifest.json",
            "SHA256SUMS",
            *(item.relative_path for item in manifest.files),
        }

    @staticmethod
    def _actual_paths(bundle: Path) -> tuple[set[str], set[str]]:
        files: set[str] = set()
        directories: set[str] = set()
        for current, dir_names, file_names in os.walk(bundle, followlinks=False):
            dir_names.sort()
            file_names.sort()
            current_path = Path(current)
            for name in dir_names:
                child = current_path / name
                relative = child.relative_to(bundle).as_posix()
                observed = child.lstat()
                if child.is_symlink() or not stat.S_ISDIR(observed.st_mode):
                    raise LabArtifactIntegrityError(f"unsafe artifact directory: {relative}")
                directories.add(relative)
            for name in file_names:
                child = current_path / name
                relative = child.relative_to(bundle).as_posix()
                observed = child.lstat()
                if child.is_symlink() or not stat.S_ISREG(observed.st_mode):
                    raise LabArtifactIntegrityError(f"unsafe artifact file: {relative}")
                if observed.st_nlink != 1:
                    raise LabArtifactIntegrityError(
                        f"artifact file has an external hard link: {relative}"
                    )
                files.add(relative)
        return files, directories

    def _validate_bundle(
        self,
        bundle: Path,
        *,
        require_sealed_permissions: bool,
    ) -> tuple[LabJobArtifactManifest, tuple[LabArtifactFileIdentity, ...]]:
        bundle_before = self._directory_observation(bundle, label="job artifact bundle")
        manifest_bytes, _ = _read_regular_file(
            bundle / "manifest.json", label="job artifact manifest"
        )
        try:
            manifest = LabJobArtifactManifest.model_validate_json(manifest_bytes)
        except Exception as exc:
            raise LabArtifactIntegrityError(f"invalid job artifact manifest: {exc}") from exc
        if manifest_bytes != manifest.canonical_json_bytes():
            raise LabArtifactIntegrityError("job artifact manifest is not canonical JSON")
        actual_files, actual_directories = self._actual_paths(bundle)
        expected_files = self._expected_paths(manifest)
        if actual_files != expected_files or actual_directories != {"tables"}:
            raise LabArtifactIntegrityError(
                "job artifact inventory mismatch: "
                f"unexpected={sorted(actual_files - expected_files)} "
                f"missing={sorted(expected_files - actual_files)} "
                f"directories={sorted(actual_directories)}"
            )
        observed_payloads: dict[str, bytes] = {}
        identities: list[LabArtifactFileIdentity] = []
        by_path = {item.relative_path: item for item in manifest.files}
        for relative_path in sorted(expected_files):
            path = bundle / relative_path
            payload, observation = _read_regular_file(
                path, label=f"job artifact file {relative_path}"
            )
            identities.append(
                LabArtifactFileIdentity(
                    relative_path=relative_path,
                    device=observation.device,
                    inode=observation.inode,
                    size=observation.size,
                    mtime_ns=observation.mtime_ns,
                    ctime_ns=observation.ctime_ns,
                )
            )
            if require_sealed_permissions and path.stat().st_mode & 0o222:
                raise LabArtifactIntegrityError(
                    f"sealed artifact file remains writable: {relative_path}"
                )
            if relative_path in by_path:
                expected = by_path[relative_path]
                if len(payload) != expected.size or _sha256(payload) != expected.sha256:
                    raise LabArtifactIntegrityError(f"job artifact bytes conflict: {relative_path}")
                observed_payloads[relative_path] = payload
        if require_sealed_permissions:
            for directory in (bundle, bundle / "tables"):
                if directory.stat().st_mode & 0o222:
                    raise LabArtifactIntegrityError(
                        f"sealed artifact directory remains writable: {directory.name}"
                    )
        spec_payload = _parse_canonical_json(observed_payloads["spec.json"], label="spec.json")
        if _sha256(observed_payloads["spec.json"]) != manifest.spec_hash:
            raise LabArtifactIntegrityError("spec.json does not match spec_hash")
        if not isinstance(spec_payload, dict):
            raise LabArtifactIntegrityError("spec.json must contain an object")
        manifest_snapshot = (
            manifest.dataset_snapshot.model_dump(mode="json", exclude_none=True)
            if manifest.dataset_snapshot is not None
            else None
        )
        if (
            spec_payload.get("code_sha") != manifest.code_sha
            or spec_payload.get("dataset_snapshot") != manifest_snapshot
        ):
            raise LabArtifactIntegrityError("manifest spec identity conflicts with spec.json")
        _parse_canonical_json(observed_payloads["metrics.json"], label="metrics.json")
        try:
            observed_payloads["report.md"].decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise LabArtifactIntegrityError("report.md is not valid UTF-8") from exc
        for relative_path, entry in sorted(by_path.items()):
            if entry.parquet is None:
                continue
            try:
                frame = pd.read_parquet(io.BytesIO(observed_payloads[relative_path]))
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
        sums_bytes = "".join(
            f"{digest}  {relative_path}\n"
            for relative_path, digest in sorted(expected_sums.items())
        ).encode("ascii")
        actual_sums, _ = _read_regular_file(bundle / "SHA256SUMS", label="job artifact checksums")
        if actual_sums != sums_bytes:
            raise LabArtifactIntegrityError("SHA256SUMS is not canonical or does not match")
        complete_hash = _sha256(
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
        )
        if complete_hash != manifest.complete_result_hash:
            raise LabArtifactIntegrityError("complete result hash conflicts")
        bundle_after = self._directory_observation(bundle, label="job artifact bundle")
        if bundle_after != bundle_before:
            raise LabArtifactIntegrityError("job artifact bundle changed while verifying")
        return manifest, tuple(sorted(identities, key=lambda item: item.relative_path))

    def _candidate_from_path(self, path: Path) -> LabJobArtifactCandidate:
        managed = self._assert_managed_child(path, self.candidates_root, label="candidate")
        observed = self._directory_observation(managed, label="job artifact candidate")
        manifest, identities = self._validate_bundle(managed, require_sealed_permissions=False)
        self._validate_candidate_permissions(managed, manifest)
        return LabJobArtifactCandidate(
            path=managed,
            job_id=manifest.job_id,
            manifest=manifest,
            manifest_hash=manifest.manifest_hash,
            device=observed.device,
            inode=observed.inode,
            file_identities=identities,
        )

    @staticmethod
    def _validate_candidate_permissions(
        bundle: Path,
        manifest: LabJobArtifactManifest,
    ) -> None:
        if any(stat.S_IMODE(path.stat().st_mode) != 0o700 for path in (bundle, bundle / "tables")):
            raise LabArtifactIntegrityError("candidate directory permissions must be 0700")
        for relative_path in LabJobArtifactStore._expected_paths(manifest):
            if stat.S_IMODE((bundle / relative_path).stat().st_mode) != 0o600:
                raise LabArtifactIntegrityError("candidate file permissions must be 0600")

    def verify_candidate(self, candidate: LabJobArtifactCandidate) -> LabJobArtifactManifest:
        path = self._assert_managed_child(candidate.path, self.candidates_root, label="candidate")
        observed = self._directory_observation(path, label="job artifact candidate")
        if (observed.device, observed.inode) != (candidate.device, candidate.inode):
            raise LabArtifactIntegrityError("candidate bundle identity changed")
        manifest, identities = self._validate_bundle(path, require_sealed_permissions=False)
        if (
            manifest != candidate.manifest
            or manifest.manifest_hash != candidate.manifest_hash
            or identities != candidate.file_identities
        ):
            raise LabArtifactIntegrityError("candidate bundle identity or files changed")
        return manifest

    @staticmethod
    def _make_files_read_only(bundle: Path) -> None:
        for child in sorted(bundle.rglob("*")):
            if child.is_symlink():
                raise LabArtifactIntegrityError("candidate contains a symlink")
            if child.is_file():
                os.chmod(child, 0o400)

    @staticmethod
    def _make_directories_read_only(bundle: Path) -> None:
        for child in sorted(
            (path for path in bundle.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            os.chmod(child, 0o500)
        os.chmod(bundle, 0o500)
        _fsync_directory(bundle / "tables")
        _fsync_directory(bundle)

    def verify_sealed(self, path: Path) -> LabSealedJobArtifact:
        managed = self._assert_managed_child(path, self.sealed_root, label="sealed bundle")
        observed = self._directory_observation(managed, label="sealed job artifact")
        manifest, _ = self._validate_bundle(managed, require_sealed_permissions=True)
        if managed.name != manifest.job_id.hex:
            raise LabArtifactIntegrityError("sealed path does not match job identity")
        return LabSealedJobArtifact(
            path=managed,
            manifest=manifest,
            manifest_hash=manifest.manifest_hash,
            device=observed.device,
            inode=observed.inode,
        )

    def seal_candidate(self, candidate: LabJobArtifactCandidate) -> LabSealedJobArtifact:
        manifest = self.verify_candidate(candidate)
        target = self.sealed_root / manifest.job_id.hex
        if os.path.lexists(target):
            existing = self.verify_sealed(target)
            if (
                existing.manifest_hash != manifest.manifest_hash
                or existing.manifest.complete_result_hash != manifest.complete_result_hash
            ):
                raise LabArtifactConflictError("job already has a different sealed result")
            self.quarantine_candidate(candidate, reason="idempotent sealed bundle reuse")
            return existing.model_copy(update={"reused_existing": True})
        # macOS denies rename(2) for a source directory without owner-write.
        # Seal bytes before publication, atomically rename, then lock directories.
        self._make_files_read_only(candidate.path)
        try:
            os.rename(candidate.path, target)
        except OSError as exc:
            if not os.path.lexists(target):
                raise LabArtifactConflictError("candidate could not be atomically sealed") from exc
            existing = self.verify_sealed(target)
            if existing.manifest_hash != manifest.manifest_hash:
                raise LabArtifactConflictError("job already has a different sealed result") from exc
            self.quarantine_candidate(candidate, reason="concurrent sealed bundle reuse")
            return existing.model_copy(update={"reused_existing": True})
        self._make_directories_read_only(target)
        _fsync_directory(self.sealed_root)
        return self.verify_sealed(target)

    def list_candidate_recovery(self) -> tuple[LabArtifactRecoveryRecord, ...]:
        records: list[LabArtifactRecoveryRecord] = []
        for path in sorted(self.candidates_root.iterdir()):
            observed = path.lstat()
            if path.is_symlink() or not stat.S_ISDIR(observed.st_mode):
                records.append(
                    LabArtifactRecoveryRecord(
                        path=path,
                        status="invalid",
                        reason="candidate entry is not a regular directory",
                    )
                )
                continue
            try:
                candidate = self._candidate_from_path(path)
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
                records.append(
                    LabArtifactRecoveryRecord(
                        path=path,
                        status="recoverable",
                        job_id=candidate.job_id,
                        manifest_hash=candidate.manifest_hash,
                        device=candidate.device,
                        inode=candidate.inode,
                    )
                )
        for path in sorted(self.quarantine_root.iterdir()):
            if path.is_dir() and not path.is_symlink():
                observed = path.lstat()
                records.append(
                    LabArtifactRecoveryRecord(
                        path=path,
                        status="quarantined",
                        device=observed.st_dev,
                        inode=observed.st_ino,
                    )
                )
        return tuple(records)

    def recover_candidate(self, record: LabArtifactRecoveryRecord) -> LabSealedJobArtifact:
        if record.status != "recoverable":
            raise LabArtifactIntegrityError("only recoverable candidates can be sealed")
        candidate = self._candidate_from_path(record.path)
        if record.job_id != candidate.job_id or record.manifest_hash != candidate.manifest_hash:
            raise LabArtifactIntegrityError("candidate recovery evidence changed")
        return self.seal_candidate(candidate)

    def recover_interrupted_seal(self, path: Path) -> LabSealedJobArtifact:
        """Finish the chmod/fsync tail after a candidate was already renamed."""

        managed = self._assert_managed_child(
            path, self.sealed_root, label="interrupted sealed bundle"
        )
        try:
            return self.verify_sealed(managed)
        except LabArtifactIntegrityError:
            pass
        manifest, _ = self._validate_bundle(managed, require_sealed_permissions=False)
        if managed.name != manifest.job_id.hex:
            raise LabArtifactIntegrityError("interrupted sealed path does not match job identity")
        for relative_path in self._expected_paths(manifest):
            if (managed / relative_path).stat().st_mode & 0o222:
                raise LabArtifactIntegrityError("interrupted seal contains writable result bytes")
        self._make_directories_read_only(managed)
        _fsync_directory(self.sealed_root)
        return self.verify_sealed(managed)

    def quarantine_candidate(
        self,
        candidate: LabJobArtifactCandidate,
        *,
        reason: str,
    ) -> LabArtifactRecoveryRecord:
        if not reason.strip():
            raise ValueError("quarantine reason must not be empty")
        path = self._assert_managed_child(candidate.path, self.candidates_root, label="candidate")
        observed = self._directory_observation(path, label="job artifact candidate")
        if (observed.device, observed.inode) != (candidate.device, candidate.inode):
            raise LabArtifactIntegrityError("candidate identity changed before quarantine")
        target = self.quarantine_root / (
            f"{path.name}-{candidate.manifest_hash[:16]}-{uuid4().hex}"
        )
        os.rename(path, target)
        _fsync_directory(self.candidates_root)
        _fsync_directory(self.quarantine_root)
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
        observed = self._directory_observation(path, label="candidate recovery entry")
        if (observed.device, observed.inode) != (record.device, record.inode):
            raise LabArtifactIntegrityError("candidate recovery identity changed")
        target = self.quarantine_root / f"{path.name}-recovery-{uuid4().hex}"
        os.rename(path, target)
        _fsync_directory(self.candidates_root)
        _fsync_directory(self.quarantine_root)
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
        )
        actual = (
            evidence.job_id,
            evidence.sealed_path.absolute(),
            evidence.manifest_hash,
            evidence.complete_result_hash,
            evidence.bundle_device,
            evidence.bundle_inode,
        )
        if actual != expected:
            raise LabArtifactAuthorizationError(
                "indexed evidence does not authorize this sealed bundle"
            )

    def export_deterministic_zip(
        self,
        sealed_path: Path,
        evidence: LabArtifactIndexEvidence,
        destination: Path,
    ) -> Path:
        try:
            sealed = self.verify_sealed(sealed_path)
        except (LabArtifactPathError, LabArtifactIntegrityError) as exc:
            raise LabArtifactAuthorizationError(
                "only a verified sealed bundle can be exported"
            ) from exc
        self._authorize_export(sealed, evidence)
        destination = destination.absolute()
        _ensure_private_directory(destination.parent, manage_existing=False)
        temporary = destination.parent / f".{destination.name}.{uuid4().hex}.tmp"
        expected_files = sorted(self._expected_paths(sealed.manifest))
        expected_payload_hashes = {
            item.relative_path: item.sha256 for item in sealed.manifest.files
        }
        expected_payload_hashes["manifest.json"] = sealed.manifest_hash
        expected_sums = "".join(
            f"{digest}  {relative_path}\n"
            for relative_path, digest in sorted(expected_payload_hashes.items())
        ).encode("ascii")
        expected_payload_hashes["SHA256SUMS"] = _sha256(expected_sums)
        try:
            with ZipFile(
                temporary,
                mode="x",
                compression=ZIP_DEFLATED,
                compresslevel=9,
                strict_timestamps=True,
            ) as archive:
                for relative_path in expected_files:
                    payload, _ = _read_regular_file(
                        sealed.path / relative_path,
                        label=f"export artifact {relative_path}",
                    )
                    if _sha256(payload) != expected_payload_hashes[relative_path]:
                        raise LabArtifactIntegrityError(f"export bytes conflict: {relative_path}")
                    info = ZipInfo(relative_path, date_time=_ZIP_TIMESTAMP)
                    info.compress_type = ZIP_DEFLATED
                    info.create_system = 3
                    info.external_attr = (stat.S_IFREG | 0o400) << 16
                    info.flag_bits = 0
                    archive.writestr(info, payload, compress_type=ZIP_DEFLATED, compresslevel=9)
            os.chmod(temporary, 0o600)
            descriptor = os.open(temporary, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            if os.path.lexists(destination):
                raise LabArtifactConflictError("ZIP destination already exists")
            os.rename(temporary, destination)
            _fsync_directory(destination.parent)
            return destination
        except Exception:
            # Export temporaries are retained for explicit operator inspection.
            raise


class LegacyArtifactIndex:
    """Independent read-only source index for pre-Job-Center JSON/Markdown files."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = path.absolute()
        self.clock = clock or (lambda: datetime.now(UTC))
        _ensure_private_directory(self.path.parent, manage_existing=False)
        if os.path.lexists(self.path):
            observed = self.path.lstat()
            if self.path.is_symlink() or not stat.S_ISREG(observed.st_mode):
                raise LabArtifactIntegrityError("legacy index path is unsafe")
            if observed.st_nlink != 1:
                raise LabArtifactIntegrityError("legacy index has an external hard link")
        with self._connect() as connection:
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
                    imported_at TEXT NOT NULL
                )
                """
            )
        os.chmod(self.path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _media_type(path: Path) -> Literal["application/json", "text/markdown; charset=utf-8"]:
        suffix = path.suffix.lower()
        if suffix == ".json":
            return "application/json"
        if suffix in {".md", ".markdown"}:
            return "text/markdown; charset=utf-8"
        raise LabArtifactPathError("legacy source must be JSON or Markdown")

    @staticmethod
    def _row_to_record(row: tuple[object, ...]) -> LabLegacyArtifactRecord:
        return LabLegacyArtifactRecord(
            logical_run_id=str(row[0]),
            source_path=Path(str(row[1])),
            device=int(row[2]),
            inode=int(row[3]),
            size=int(row[4]),
            mtime_ns=int(row[5]),
            sha256=str(row[6]),
            media_type=str(row[7]),  # type: ignore[arg-type]
            imported_at=datetime.fromisoformat(str(row[8])),
        )

    def get(self, logical_run_id: str) -> LabLegacyArtifactRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT logical_run_id, source_path, device, inode, size, mtime_ns,
                       sha256, media_type, imported_at
                FROM legacy_artifact WHERE logical_run_id = ?
                """,
                (logical_run_id,),
            ).fetchone()
        return self._row_to_record(row) if row is not None else None

    @staticmethod
    def _before_commit_source_check(path: Path, expected: _FileObservation) -> None:
        try:
            observed = _FileObservation.from_stat(path.lstat())
        except OSError as exc:
            raise LabArtifactIntegrityError("legacy source changed before index commit") from exc
        if observed != expected:
            raise LabArtifactIntegrityError("legacy source changed before index commit")

    def import_file(
        self,
        *,
        logical_run_id: str,
        source_path: Path,
    ) -> LabLegacyIndexResult:
        logical_run_id = " ".join(logical_run_id.split())
        if not logical_run_id:
            raise ValueError("logical_run_id must not be empty")
        source = source_path.absolute()
        media_type = self._media_type(source)
        payload, observation = _read_regular_file(source, label="legacy artifact source")
        imported_at = self.clock()
        if imported_at.tzinfo is None or imported_at.utcoffset() is None:
            raise ValueError("legacy index clock must return a timezone-aware datetime")
        record = LabLegacyArtifactRecord(
            logical_run_id=logical_run_id,
            source_path=source,
            device=observation.device,
            inode=observation.inode,
            size=observation.size,
            mtime_ns=observation.mtime_ns,
            sha256=_sha256(payload),
            media_type=media_type,
            imported_at=imported_at.astimezone(UTC),
        )
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT logical_run_id, source_path, device, inode, size, mtime_ns,
                       sha256, media_type, imported_at
                FROM legacy_artifact WHERE logical_run_id = ?
                """,
                (logical_run_id,),
            ).fetchone()
            if row is not None:
                existing = self._row_to_record(row)
                if existing.source_path == source and existing.sha256 == record.sha256:
                    self._before_commit_source_check(source, observation)
                    connection.commit()
                    return LabLegacyIndexResult(status="reused", record=existing)
                raise LabLegacyArtifactConflictError(
                    "legacy logical run already references different source bytes"
                )
            connection.execute(
                """
                INSERT INTO legacy_artifact (
                    logical_run_id, source_path, device, inode, size, mtime_ns,
                    sha256, media_type, imported_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )
            self._before_commit_source_check(source, observation)
            connection.commit()
        except Exception:
            with suppress(sqlite3.Error):
                connection.rollback()
            raise
        finally:
            connection.close()
        return LabLegacyIndexResult(status="imported", record=record)
