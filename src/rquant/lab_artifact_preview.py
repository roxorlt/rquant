"""Read-only, bounded previews for scheduler-authoritative sealed artifacts."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
from contextlib import suppress
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import TypeAlias
from uuid import UUID

import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from rquant.lab_artifacts import (
    LabArtifactFileIdentity,
    LabJobArtifactManifest,
)
from rquant.lab_jobs import LabArtifactPreviewAuthority, LabJobReader

ArtifactScalar: TypeAlias = str | int | float | bool | None


class ArtifactPreviewError(RuntimeError):
    """Base error for preview authorization or integrity failures."""


class ArtifactPreviewUnavailableError(ArtifactPreviewError):
    """The ledger does not authorize a result preview."""


class ArtifactPreviewIntegrityError(ArtifactPreviewError):
    """The sealed filesystem evidence is unsafe, changed, or corrupt."""


class ArtifactPreviewModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
    )


class ArtifactTablePreview(ArtifactPreviewModel):
    table_name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    total_rows: int = Field(ge=0)
    total_columns: int = Field(ge=0)
    columns: tuple[str, ...]
    rows: tuple[tuple[ArtifactScalar, ...], ...]
    rows_truncated: bool
    columns_truncated: bool


class ArtifactPreview(ArtifactPreviewModel):
    job_id: UUID
    spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    complete_result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    report_markdown: str
    metrics: JsonValue
    available_tables: tuple[str, ...]
    table: ArtifactTablePreview | None


def _same_file_identity(observed: os.stat_result, expected: LabArtifactFileIdentity) -> bool:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    ) == (
        expected.device,
        expected.inode,
        expected.size,
        expected.mtime_ns,
        expected.ctime_ns,
    )


def _same_opened_file(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        left.st_mode,
        left.st_nlink,
        left.st_size,
        left.st_mtime_ns,
        left.st_ctime_ns,
    ) == (
        right.st_dev,
        right.st_ino,
        right.st_mode,
        right.st_nlink,
        right.st_size,
        right.st_mtime_ns,
        right.st_ctime_ns,
    )


def _read_descriptor(descriptor: int, *, limit: int, label: str) -> bytes:
    observed = os.fstat(descriptor)
    if observed.st_size > limit:
        raise ArtifactPreviewIntegrityError(f"{label} exceeds its size limit")
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    remaining = limit + 1
    while remaining > 0:
        chunk = os.read(descriptor, min(1024 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    payload = b"".join(chunks)
    if len(payload) > limit:
        raise ArtifactPreviewIntegrityError(f"{label} exceeds its size limit")
    return payload


def _hash_descriptor(descriptor: int, *, limit: int, label: str) -> str:
    observed = os.fstat(descriptor)
    if observed.st_size > limit:
        raise ArtifactPreviewIntegrityError(f"{label} exceeds its size limit")
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    remaining = limit + 1
    byte_count = 0
    while remaining > 0:
        chunk = os.read(descriptor, min(1024 * 1024, remaining))
        if not chunk:
            break
        digest.update(chunk)
        byte_count += len(chunk)
        remaining -= len(chunk)
    if byte_count != observed.st_size or byte_count > limit:
        raise ArtifactPreviewIntegrityError(f"{label} changed or exceeds its size limit")
    return digest.hexdigest()


def _preview_scalar(value: object) -> ArtifactScalar:
    if value is None or type(value) in {str, int, bool}:
        return value  # type: ignore[return-value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ArtifactPreviewIntegrityError("Parquet preview contains a non-finite float")
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise ArtifactPreviewIntegrityError(
        f"Parquet preview contains unsupported scalar {type(value).__name__}"
    )


class ArtifactPreviewReader:
    """Verify a sealed result graph and expose only bounded read-only content."""

    def __init__(
        self,
        *,
        reader: LabJobReader,
        artifact_root: Path,
        max_bundle_bytes: int = 256 * 1024 * 1024,
        max_file_bytes: int = 128 * 1024 * 1024,
        max_text_bytes: int = 4 * 1024 * 1024,
        max_manifest_bytes: int = 1024 * 1024,
        max_preview_rows: int = 100,
        max_preview_columns: int = 40,
    ) -> None:
        limits = (
            max_bundle_bytes,
            max_file_bytes,
            max_text_bytes,
            max_manifest_bytes,
            max_preview_rows,
            max_preview_columns,
        )
        if any(type(value) is not int or value < 1 for value in limits):
            raise ValueError("artifact preview limits must be positive integers")
        self.reader = reader
        self.artifact_root = Path(os.path.abspath(artifact_root))
        self.max_bundle_bytes = max_bundle_bytes
        self.max_file_bytes = max_file_bytes
        self.max_text_bytes = max_text_bytes
        self.max_manifest_bytes = max_manifest_bytes
        self.max_preview_rows = max_preview_rows
        self.max_preview_columns = max_preview_columns

    @staticmethod
    def _open_directory(parent: int | Path, name: str | None = None) -> int:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        if isinstance(parent, Path):
            return os.open(parent, flags)
        assert name is not None
        return os.open(name, flags, dir_fd=parent)

    @staticmethod
    def _open_bound_file(
        parent_fd: int,
        name: str,
        expected: LabArtifactFileIdentity,
    ) -> int:
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o400
            or not _same_file_identity(before, expected)
        ):
            raise ArtifactPreviewIntegrityError(
                f"artifact file identity is unsafe or changed: {expected.relative_path}"
            )
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(descriptor)
        if not _same_opened_file(before, opened):
            os.close(descriptor)
            raise ArtifactPreviewIntegrityError(
                f"artifact file changed while opening: {expected.relative_path}"
            )
        return descriptor

    @staticmethod
    def _validate_manifest_authority(
        authority: LabArtifactPreviewAuthority,
        manifest: LabJobArtifactManifest,
    ) -> None:
        job = authority.job
        evidence = authority.evidence
        if (
            manifest.job_id != job.job_id
            or manifest.spec_hash != job.spec_hash
            or manifest.code_sha != job.spec.code_sha
            or manifest.dataset_snapshot != job.spec.dataset_snapshot
            or manifest.manifest_hash != evidence.manifest_hash
            or manifest.complete_result_hash != evidence.complete_result_hash
        ):
            raise ArtifactPreviewIntegrityError(
                "artifact manifest conflicts with scheduler result evidence"
            )

    def preview(
        self,
        job_id: UUID,
        *,
        table_name: str | None = None,
        row_limit: int = 20,
        column_limit: int = 12,
    ) -> ArtifactPreview:
        if not 1 <= row_limit <= self.max_preview_rows:
            raise ValueError(f"row_limit must be between 1 and {self.max_preview_rows}")
        if not 1 <= column_limit <= self.max_preview_columns:
            raise ValueError(f"column_limit must be between 1 and {self.max_preview_columns}")
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None:
            raise ArtifactPreviewUnavailableError(
                "artifact preview requires a succeeded job with sealed result evidence"
            )
        return self._preview_authorized(
            authority,
            table_name=table_name,
            row_limit=row_limit,
            column_limit=column_limit,
        )

    def _preview_authorized(
        self,
        authority: LabArtifactPreviewAuthority,
        *,
        table_name: str | None,
        row_limit: int,
        column_limit: int,
    ) -> ArtifactPreview:
        evidence = authority.evidence
        expected_path = self.artifact_root / "sealed" / authority.job.job_id.hex
        if evidence.sealed_path != expected_path:
            raise ArtifactPreviewIntegrityError("sealed artifact path is outside its bounded root")
        descriptors: list[int] = []
        opened_files: dict[str, int] = {}
        originals: dict[str, os.stat_result] = {}
        try:
            if self.artifact_root.resolve(strict=True) != self.artifact_root:
                raise ArtifactPreviewIntegrityError("artifact root contains a symlink")
            root_fd = self._open_directory(self.artifact_root)
            descriptors.append(root_fd)
            sealed_fd = self._open_directory(root_fd, "sealed")
            descriptors.append(sealed_fd)
            bundle_before = os.stat(
                authority.job.job_id.hex,
                dir_fd=sealed_fd,
                follow_symlinks=False,
            )
            bundle_fd = self._open_directory(sealed_fd, authority.job.job_id.hex)
            descriptors.append(bundle_fd)
            bundle_opened = os.fstat(bundle_fd)
            if (
                not _same_opened_file(bundle_before, bundle_opened)
                or not stat.S_ISDIR(bundle_opened.st_mode)
                or stat.S_IMODE(bundle_opened.st_mode) != 0o500
                or (bundle_opened.st_dev, bundle_opened.st_ino)
                != (evidence.bundle_device, evidence.bundle_inode)
            ):
                raise ArtifactPreviewIntegrityError("sealed bundle identity is unsafe or changed")
            tables_fd = self._open_directory(bundle_fd, "tables")
            descriptors.append(tables_fd)
            tables_opened = os.fstat(tables_fd)
            if (
                not stat.S_ISDIR(tables_opened.st_mode)
                or stat.S_IMODE(tables_opened.st_mode) != 0o500
            ):
                raise ArtifactPreviewIntegrityError("sealed tables directory is unsafe")

            identities = {item.relative_path: item for item in evidence.file_identities}
            manifest_identity = identities.get("manifest.json")
            if manifest_identity is None:
                raise ArtifactPreviewIntegrityError("sealed evidence has no manifest identity")
            manifest_fd = self._open_bound_file(bundle_fd, "manifest.json", manifest_identity)
            opened_files["manifest.json"] = manifest_fd
            originals["manifest.json"] = os.fstat(manifest_fd)
            manifest_bytes = _read_descriptor(
                manifest_fd,
                limit=self.max_manifest_bytes,
                label="manifest.json",
            )
            try:
                manifest = LabJobArtifactManifest.model_validate_json(manifest_bytes)
            except Exception as exc:
                raise ArtifactPreviewIntegrityError("artifact manifest is invalid") from exc
            if manifest_bytes != manifest.canonical_json_bytes():
                raise ArtifactPreviewIntegrityError("artifact manifest is not canonical JSON")
            self._validate_manifest_authority(authority, manifest)
            expected_paths = {
                "manifest.json",
                "SHA256SUMS",
                *(item.relative_path for item in manifest.files),
            }
            if set(identities) != expected_paths:
                raise ArtifactPreviewIntegrityError("artifact evidence inventory conflicts")
            total_bytes = sum(item.size for item in evidence.file_identities)
            if total_bytes > self.max_bundle_bytes:
                raise ArtifactPreviewIntegrityError("artifact bundle exceeds its size limit")

            for relative_path in sorted(expected_paths - {"manifest.json"}):
                pure = PurePosixPath(relative_path)
                parent_fd = tables_fd if pure.parent.as_posix() == "tables" else bundle_fd
                descriptor = self._open_bound_file(
                    parent_fd,
                    pure.name,
                    identities[relative_path],
                )
                opened_files[relative_path] = descriptor
                originals[relative_path] = os.fstat(descriptor)

            expected_hashes = {item.relative_path: item.sha256 for item in manifest.files}
            expected_hashes["manifest.json"] = manifest.manifest_hash
            sums_bytes = "".join(
                f"{digest}  {relative_path}\n"
                for relative_path, digest in sorted(expected_hashes.items())
            ).encode("ascii")
            expected_hashes["SHA256SUMS"] = hashlib.sha256(sums_bytes).hexdigest()
            for relative_path, descriptor in opened_files.items():
                digest = _hash_descriptor(
                    descriptor,
                    limit=(
                        self.max_manifest_bytes
                        if relative_path == "manifest.json"
                        else self.max_file_bytes
                    ),
                    label=relative_path,
                )
                if digest != expected_hashes[relative_path]:
                    raise ArtifactPreviewIntegrityError(
                        f"artifact file hash conflicts: {relative_path}"
                    )
            if (
                _read_descriptor(
                    opened_files["SHA256SUMS"],
                    limit=self.max_manifest_bytes,
                    label="SHA256SUMS",
                )
                != sums_bytes
            ):
                raise ArtifactPreviewIntegrityError("SHA256SUMS is not canonical")

            report_bytes = _read_descriptor(
                opened_files["report.md"],
                limit=self.max_text_bytes,
                label="report.md",
            )
            metrics_bytes = _read_descriptor(
                opened_files["metrics.json"],
                limit=self.max_text_bytes,
                label="metrics.json",
            )
            try:
                report = report_bytes.decode("utf-8", errors="strict")
                metrics = json.loads(metrics_bytes)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ArtifactPreviewIntegrityError("artifact text payload is invalid") from exc
            canonical_metrics = json.dumps(
                metrics,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            if canonical_metrics != metrics_bytes:
                raise ArtifactPreviewIntegrityError("metrics.json is not canonical JSON")

            table_entries = tuple(item for item in manifest.files if item.parquet is not None)
            available_tables = tuple(
                item.parquet.table_name for item in table_entries if item.parquet
            )
            selected_name = table_name or available_tables[0]
            selected = next(
                (
                    item
                    for item in table_entries
                    if item.parquet and item.parquet.table_name == selected_name
                ),
                None,
            )
            if selected is None or selected.parquet is None:
                raise ValueError(f"unknown artifact table: {selected_name}")
            parquet = selected.parquet
            columns = parquet.columns[:column_limit]
            parquet_fd = opened_files[selected.relative_path]
            os.lseek(parquet_fd, 0, os.SEEK_SET)
            with os.fdopen(os.dup(parquet_fd), "rb") as stream:
                parquet_file = pq.ParquetFile(stream)
                if (
                    parquet_file.metadata.num_rows != parquet.row_count
                    or tuple(parquet_file.schema_arrow.names) != parquet.columns
                ):
                    raise ArtifactPreviewIntegrityError(
                        f"Parquet metadata conflicts: {selected.relative_path}"
                    )
                rows: tuple[tuple[ArtifactScalar, ...], ...] = ()
                if columns and parquet.row_count:
                    batches = parquet_file.iter_batches(
                        batch_size=row_limit,
                        columns=list(columns),
                    )
                    batch = next(batches, None)
                    if batch is not None:
                        rows = tuple(
                            tuple(_preview_scalar(value) for value in row.values())
                            for row in batch.to_pylist()
                        )
            table = ArtifactTablePreview(
                table_name=selected_name,
                total_rows=parquet.row_count,
                total_columns=len(parquet.columns),
                columns=columns,
                rows=rows,
                rows_truncated=parquet.row_count > len(rows),
                columns_truncated=len(parquet.columns) > len(columns),
            )

            for relative_path, descriptor in opened_files.items():
                if not _same_opened_file(originals[relative_path], os.fstat(descriptor)):
                    raise ArtifactPreviewIntegrityError(
                        f"artifact file changed during preview: {relative_path}"
                    )
            bundle_at_path = os.stat(
                authority.job.job_id.hex,
                dir_fd=sealed_fd,
                follow_symlinks=False,
            )
            if not _same_opened_file(bundle_opened, bundle_at_path):
                raise ArtifactPreviewIntegrityError("sealed bundle changed during preview")
            return ArtifactPreview(
                job_id=authority.job.job_id,
                spec_hash=authority.job.spec_hash,
                manifest_hash=evidence.manifest_hash,
                complete_result_hash=evidence.complete_result_hash,
                report_markdown=report,
                metrics=metrics,
                available_tables=available_tables,
                table=table,
            )
        except ArtifactPreviewError:
            raise
        except OSError as exc:
            raise ArtifactPreviewIntegrityError(
                "sealed artifact path changed or is unsafe"
            ) from exc
        finally:
            for descriptor in reversed((*opened_files.values(), *descriptors)):
                with suppress(OSError):
                    os.close(descriptor)
