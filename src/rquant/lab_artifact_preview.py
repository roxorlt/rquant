"""Read-only, bounded previews for scheduler-authoritative sealed artifacts."""

from __future__ import annotations

import hashlib
import math
import os
import stat
import struct
from contextlib import suppress
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import TypeAlias
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from rquant.lab_artifacts import (
    LabArtifactFileIdentity,
    LabJobArtifactManifest,
    LabParquetIdentity,
    _rebuild_research_run_spec,
)
from rquant.lab_jobs import LabArtifactPreviewAuthority, LabJobReader
from rquant.research_run_spec import ResearchRunSpec
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

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


class ArtifactCompleteTableBudget(ArtifactPreviewModel):
    max_table_count: int = Field(default=8, ge=1, le=8)
    max_table_bytes: int = Field(default=33_554_432, ge=1, le=33_554_432)
    max_total_bytes: int = Field(default=62_128_104, ge=1, le=62_128_104)


class ArtifactCompleteTable(ArtifactPreviewModel):
    parquet: LabParquetIdentity
    rows: tuple[tuple[ArtifactScalar, ...], ...]


class ArtifactCompleteTables(ArtifactPreviewModel):
    authority: LabArtifactPreviewAuthority
    manifest: LabJobArtifactManifest
    spec: ResearchRunSpec
    report_markdown: str
    metrics: JsonValue
    tables: tuple[ArtifactCompleteTable, ...]


class ArtifactCompleteByteEvidence(ArtifactPreviewModel):
    """Complete sealed byte identity; Parquet data semantics require full materialization."""

    authority: LabArtifactPreviewAuthority
    manifest: LabJobArtifactManifest
    spec: ResearchRunSpec
    metrics: JsonValue
    file_identities: tuple[LabArtifactFileIdentity, ...]
    tables: tuple[LabParquetIdentity, ...]
    encoded_table_bytes: int = Field(ge=0)
    verified_bundle_bytes: int = Field(ge=0)


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
        max_parquet_uncompressed_bytes: int = 32 * 1024 * 1024,
        max_preview_arrow_bytes: int = 8 * 1024 * 1024,
        max_preview_cell_bytes: int = 1024 * 1024,
        max_preview_serialized_bytes: int = 2 * 1024 * 1024,
    ) -> None:
        limits = (
            max_bundle_bytes,
            max_file_bytes,
            max_text_bytes,
            max_manifest_bytes,
            max_preview_rows,
            max_preview_columns,
            max_parquet_uncompressed_bytes,
            max_preview_arrow_bytes,
            max_preview_cell_bytes,
            max_preview_serialized_bytes,
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
        self.max_parquet_uncompressed_bytes = max_parquet_uncompressed_bytes
        self.max_preview_arrow_bytes = max_preview_arrow_bytes
        self.max_preview_cell_bytes = max_preview_cell_bytes
        self.max_preview_serialized_bytes = max_preview_serialized_bytes

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

    @staticmethod
    def _variable_cell_bytes(array: pa.Array, index: int) -> int | None:
        data_type = array.type
        if not (
            pa.types.is_string(data_type)
            or pa.types.is_large_string(data_type)
            or pa.types.is_binary(data_type)
            or pa.types.is_large_binary(data_type)
        ):
            return None
        if not array[index].is_valid:
            return 0
        buffers = array.buffers()
        offsets = buffers[1]
        if offsets is None:
            raise ArtifactPreviewIntegrityError("Parquet variable cell has no offsets")
        width = (
            8 if pa.types.is_large_string(data_type) or pa.types.is_large_binary(data_type) else 4
        )
        format_code = "<q" if width == 8 else "<i"
        offset_index = array.offset + index
        view = memoryview(offsets)
        start = struct.unpack_from(format_code, view, offset_index * width)[0]
        end = struct.unpack_from(format_code, view, (offset_index + 1) * width)[0]
        if start < 0 or end < start:
            raise ArtifactPreviewIntegrityError("Parquet variable cell offsets are invalid")
        return end - start

    @staticmethod
    def _arrow_preview_type_supported(data_type: pa.DataType) -> bool:
        return any(
            predicate(data_type)
            for predicate in (
                pa.types.is_boolean,
                pa.types.is_integer,
                pa.types.is_floating,
                pa.types.is_decimal,
                pa.types.is_date,
                pa.types.is_timestamp,
                pa.types.is_string,
                pa.types.is_large_string,
                pa.types.is_binary,
                pa.types.is_large_binary,
            )
        )

    def _validate_parquet_metadata(
        self,
        parquet_file: pq.ParquetFile,
        *,
        relative_path: str,
        expected_rows: int,
        expected_columns: tuple[str, ...],
        selected_columns: tuple[str, ...],
    ) -> int:
        metadata = parquet_file.metadata
        if metadata.num_rows != expected_rows or tuple(parquet_file.schema_arrow.names) != expected_columns:
            raise ArtifactPreviewIntegrityError(f"Parquet metadata conflicts: {relative_path}")
        uncompressed_bytes = 0
        for index in range(metadata.num_row_groups):
            size = metadata.row_group(index).total_byte_size
            if type(size) is not int or size < 0:
                raise ArtifactPreviewIntegrityError(f"Parquet row-group metadata is invalid: {relative_path}")
            uncompressed_bytes += size
            if uncompressed_bytes > self.max_parquet_uncompressed_bytes:
                raise ArtifactPreviewIntegrityError(f"Parquet uncompressed data exceeds preview budget: {relative_path}")
        for name in selected_columns:
            data_type = parquet_file.schema_arrow.field(name).type
            if not self._arrow_preview_type_supported(data_type):
                raise ArtifactPreviewIntegrityError(f"Parquet preview contains unsupported type {data_type}")
        return uncompressed_bytes

    def _read_parquet_preview_rows(
        self,
        descriptor: int,
        *,
        relative_path: str,
        expected_rows: int,
        expected_columns: tuple[str, ...],
        selected_columns: tuple[str, ...],
        row_limit: int,
    ) -> tuple[tuple[ArtifactScalar, ...], ...]:
        os.lseek(descriptor, 0, os.SEEK_SET)
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            parquet_file = pq.ParquetFile(stream)
            self._validate_parquet_metadata(parquet_file, relative_path=relative_path,
                expected_rows=expected_rows, expected_columns=expected_columns, selected_columns=selected_columns)

            rows: list[tuple[ArtifactScalar, ...]] = []
            arrow_bytes = 0
            serialized_bytes = 2
            if not selected_columns or expected_rows == 0:
                return ()
            for batch in parquet_file.iter_batches(
                batch_size=min(row_limit, self.max_preview_rows),
                columns=list(selected_columns),
            ):
                arrow_bytes += batch.nbytes
                if arrow_bytes > self.max_preview_arrow_bytes:
                    raise ArtifactPreviewIntegrityError(
                        f"Parquet materialized Arrow data exceeds preview budget: {relative_path}"
                    )
                for row_index in range(batch.num_rows):
                    if len(rows) >= row_limit:
                        return tuple(rows)
                    row: list[ArtifactScalar] = []
                    row_serialized_bytes = 2
                    for column_index in range(batch.num_columns):
                        array = batch.column(column_index)
                        cell_bytes = self._variable_cell_bytes(array, row_index)
                        if cell_bytes is not None and cell_bytes > self.max_preview_cell_bytes:
                            raise ArtifactPreviewIntegrityError(
                                f"Parquet preview cell exceeds byte budget: {relative_path}"
                            )
                        value = _preview_scalar(array[row_index].as_py())
                        encoded = canonical_json_bytes(value)
                        row_serialized_bytes += len(encoded) + (1 if row else 0)
                        if (
                            serialized_bytes + row_serialized_bytes + (1 if rows else 0)
                            > self.max_preview_serialized_bytes
                        ):
                            raise ArtifactPreviewIntegrityError(
                                f"Parquet serialized preview exceeds byte budget: {relative_path}"
                            )
                        row.append(value)
                    serialized_bytes += row_serialized_bytes + (1 if rows else 0)
                    rows.append(tuple(row))
            return tuple(rows)

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

    def read_complete_tables(
        self,
        job_id: UUID,
        *,
        table_names: tuple[str, ...],
        budget: ArtifactCompleteTableBudget,
    ) -> ArtifactCompleteTables:
        budget = ArtifactCompleteTableBudget.model_validate(budget.model_dump(mode="python"))
        if not table_names or len(table_names) != len(set(table_names)) or len(table_names) > budget.max_table_count:
            raise ValueError("complete artifact table selection must be unique and bounded")
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None:
            raise ArtifactPreviewUnavailableError(
                "artifact preview requires a succeeded job with sealed result evidence"
            )
        result = self._read_authorized_bundle(authority, table_name=None, row_limit=1,
            column_limit=1, complete_budget=budget, table_names=table_names)
        assert isinstance(result, ArtifactCompleteTables)
        return result

    def read_complete_byte_evidence(
        self,
        job_id: UUID,
        *,
        table_names: tuple[str, ...],
        budget: ArtifactCompleteTableBudget,
    ) -> ArtifactCompleteByteEvidence:
        budget = ArtifactCompleteTableBudget.model_validate(budget.model_dump(mode="python"))
        if not table_names or len(table_names) != len(set(table_names)) or len(table_names) > budget.max_table_count:
            raise ValueError("complete artifact table selection must be unique and bounded")
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None:
            raise ArtifactPreviewUnavailableError(
                "artifact preview requires a succeeded job with sealed result evidence"
            )
        result = self._read_authorized_bundle(authority, table_name=None, row_limit=1,
            column_limit=1, complete_budget=budget, table_names=table_names, byte_evidence=True)
        assert isinstance(result, ArtifactCompleteByteEvidence)
        return result

    def _read_parquet_complete_rows(
        self,
        descriptor: int,
        *,
        relative_path: str,
        expected: LabParquetIdentity,
        byte_limit: int,
    ) -> tuple[tuple[tuple[ArtifactScalar, ...], ...], int]:
        os.lseek(descriptor, 0, os.SEEK_SET)
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            parquet_file = pq.ParquetFile(stream)
            uncompressed_bytes = self._validate_parquet_metadata(parquet_file, relative_path=relative_path,
                expected_rows=expected.row_count, expected_columns=expected.columns, selected_columns=expected.columns)
            if uncompressed_bytes > byte_limit:
                raise ArtifactPreviewIntegrityError(f"Parquet uncompressed data exceeds complete budget: {relative_path}")
            rows: list[tuple[ArtifactScalar, ...]] = []
            arrow_bytes = 0
            serialized_bytes = 2
            for batch in parquet_file.iter_batches(batch_size=100):
                arrow_bytes += batch.nbytes
                if arrow_bytes > byte_limit:
                    raise ArtifactPreviewIntegrityError(f"Parquet materialized data exceeds complete budget: {relative_path}")
                for row_index in range(batch.num_rows):
                    row: list[ArtifactScalar] = []
                    row_bytes = 2
                    for column_index in range(batch.num_columns):
                        array = batch.column(column_index)
                        cell_bytes = self._variable_cell_bytes(array, row_index)
                        if cell_bytes is not None and cell_bytes > byte_limit:
                            raise ArtifactPreviewIntegrityError(f"Parquet complete cell exceeds byte budget: {relative_path}")
                        value = _preview_scalar(array[row_index].as_py())
                        row_bytes += len(canonical_json_bytes(value)) + (1 if row else 0)
                        if serialized_bytes + row_bytes + (1 if rows else 0) > byte_limit:
                            raise ArtifactPreviewIntegrityError(f"Parquet serialized complete data exceeds byte budget: {relative_path}")
                        row.append(value)
                    serialized_bytes += row_bytes + (1 if rows else 0)
                    rows.append(tuple(row))
            if len(rows) != expected.row_count:
                raise ArtifactPreviewIntegrityError(f"Parquet complete row count conflicts: {relative_path}")
            return tuple(rows), max(uncompressed_bytes, arrow_bytes, serialized_bytes)

    def _preview_authorized(
        self,
        authority: LabArtifactPreviewAuthority,
        *,
        table_name: str | None,
        row_limit: int,
        column_limit: int,
    ) -> ArtifactPreview:
        result = self._read_authorized_bundle(authority, table_name=table_name,
            row_limit=row_limit, column_limit=column_limit)
        assert isinstance(result, ArtifactPreview)
        return result

    def _read_authorized_bundle(
        self,
        authority: LabArtifactPreviewAuthority,
        *,
        table_name: str | None,
        row_limit: int,
        column_limit: int,
        complete_budget: ArtifactCompleteTableBudget | None = None,
        table_names: tuple[str, ...] = (),
        byte_evidence: bool = False,
    ) -> ArtifactPreview | ArtifactCompleteTables | ArtifactCompleteByteEvidence:
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
            root_before = os.stat(self.artifact_root, follow_symlinks=False) if byte_evidence else None
            root_fd = self._open_directory(self.artifact_root)
            descriptors.append(root_fd)
            root_opened = os.fstat(root_fd) if byte_evidence else None
            if byte_evidence and (
                root_before is None or root_opened is None
                or not _same_opened_file(root_before, root_opened)
                or not stat.S_ISDIR(root_opened.st_mode)
                or stat.S_IMODE(root_opened.st_mode) != 0o700
            ):
                raise ArtifactPreviewIntegrityError("artifact root identity or permissions are unsafe")
            sealed_before = os.stat("sealed", dir_fd=root_fd, follow_symlinks=False) if byte_evidence else None
            sealed_fd = self._open_directory(root_fd, "sealed")
            descriptors.append(sealed_fd)
            sealed_opened = os.fstat(sealed_fd) if byte_evidence else None
            if byte_evidence and (
                sealed_before is None or sealed_opened is None
                or not _same_opened_file(sealed_before, sealed_opened)
                or not stat.S_ISDIR(sealed_opened.st_mode)
                or stat.S_IMODE(sealed_opened.st_mode) != 0o700
            ):
                raise ArtifactPreviewIntegrityError("sealed directory identity or permissions are unsafe")
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
            tables_before = os.stat("tables", dir_fd=bundle_fd, follow_symlinks=False) if byte_evidence else None
            tables_fd = self._open_directory(bundle_fd, "tables")
            descriptors.append(tables_fd)
            tables_opened = os.fstat(tables_fd)
            if (
                not stat.S_ISDIR(tables_opened.st_mode)
                or stat.S_IMODE(tables_opened.st_mode) != 0o500
            ):
                raise ArtifactPreviewIntegrityError("sealed tables directory is unsafe")
            if byte_evidence and (tables_before is None or not _same_opened_file(tables_before, tables_opened)):
                raise ArtifactPreviewIntegrityError("sealed tables directory changed while opening")

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
                manifest = strict_model_validate_canonical_json(
                    LabJobArtifactManifest,
                    manifest_bytes,
                )
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
            if byte_evidence:
                if len(identities) != len(evidence.file_identities):
                    raise ArtifactPreviewIntegrityError("artifact evidence inventory contains duplicate identities")
                byte_tables = tuple(item for item in manifest.files if item.parquet is not None)
                if {item.parquet.table_name for item in byte_tables if item.parquet} != set(table_names) or len(byte_tables) != len(table_names):
                    raise ArtifactPreviewIntegrityError("complete artifact exact table inventory conflicts")
                if any(item.size != identities[item.relative_path].size for item in manifest.files):
                    raise ArtifactPreviewIntegrityError("artifact manifest file size conflicts with bound evidence")
                for directory_fd, expected_names in (
                    (bundle_fd, {"manifest.json", "SHA256SUMS", "spec.json", "metrics.json", "report.md", "tables"}),
                    (tables_fd, {PurePosixPath(item.relative_path).name for item in byte_tables}),
                ):
                    observed_names: set[str] = set()
                    with os.scandir(directory_fd) as entries:
                        for entry in entries:
                            if entry.name not in expected_names:
                                raise ArtifactPreviewIntegrityError("sealed directory inventory conflicts")
                            observed_names.add(entry.name)
                    if observed_names != expected_names:
                        raise ArtifactPreviewIntegrityError("sealed directory inventory conflicts")
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
                metrics = strict_canonical_json_loads(metrics_bytes)
            except (UnicodeDecodeError, StrictJsonError) as exc:
                raise ArtifactPreviewIntegrityError("artifact text payload is invalid") from exc

            table_entries = tuple(item for item in manifest.files if item.parquet is not None)
            available_tables = tuple(
                item.parquet.table_name for item in table_entries if item.parquet
            )
            complete: ArtifactCompleteTables | ArtifactCompleteByteEvidence | None = None
            table: ArtifactTablePreview | None = None
            if complete_budget is not None:
                if set(available_tables) != set(table_names) or len(table_entries) != len(table_names):
                    raise ArtifactPreviewIntegrityError("complete artifact exact table inventory conflicts")
                encoded_bytes = sum(item.size for item in table_entries)
                if encoded_bytes > complete_budget.max_total_bytes or any(item.size > complete_budget.max_table_bytes for item in table_entries):
                    raise ArtifactPreviewIntegrityError("complete artifact tables exceed byte budget")
                spec_bytes = _read_descriptor(opened_files["spec.json"], limit=self.max_text_bytes, label="spec.json")
                try:
                    spec = _rebuild_research_run_spec(spec_bytes)
                except Exception as exc:
                    raise ArtifactPreviewIntegrityError("complete artifact spec is invalid") from exc
                if spec_bytes != spec.canonical_json().encode("utf-8") or spec != authority.job.spec:
                    raise ArtifactPreviewIntegrityError("complete artifact spec differs from accepted job")
                if byte_evidence:
                    complete = ArtifactCompleteByteEvidence(authority=authority, manifest=manifest,
                        spec=spec, metrics=metrics, file_identities=evidence.file_identities,
                        tables=tuple(entry.parquet for entry in table_entries if entry.parquet),
                        encoded_table_bytes=encoded_bytes, verified_bundle_bytes=total_bytes)
                else:
                    complete_tables: list[ArtifactCompleteTable] = []
                    decoded_bytes = 0
                    for entry in table_entries:
                        assert entry.parquet is not None
                        rows, usage = self._read_parquet_complete_rows(opened_files[entry.relative_path],
                            relative_path=entry.relative_path, expected=entry.parquet,
                            byte_limit=min(complete_budget.max_table_bytes, complete_budget.max_total_bytes - decoded_bytes))
                        decoded_bytes += usage
                        if decoded_bytes > complete_budget.max_total_bytes:
                            raise ArtifactPreviewIntegrityError("complete artifact decoded tables exceed byte budget")
                        complete_tables.append(ArtifactCompleteTable(parquet=entry.parquet, rows=rows))
                    complete = ArtifactCompleteTables(authority=authority, manifest=manifest, spec=spec,
                        report_markdown=report, metrics=metrics, tables=tuple(complete_tables))
            else:
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
                rows = self._read_parquet_preview_rows(
                    parquet_fd,
                    relative_path=selected.relative_path,
                    expected_rows=parquet.row_count,
                    expected_columns=parquet.columns,
                    selected_columns=columns,
                    row_limit=row_limit,
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

            if byte_evidence and self.reader.get_artifact_preview_authority(authority.job.job_id) != authority:
                raise ArtifactPreviewIntegrityError("artifact authority changed during complete byte read")
            for relative_path, descriptor in opened_files.items():
                if not _same_opened_file(originals[relative_path], os.fstat(descriptor)):
                    raise ArtifactPreviewIntegrityError(
                        f"artifact file changed during preview: {relative_path}"
                    )
                pure = PurePosixPath(relative_path)
                parent_fd = tables_fd if pure.parent.as_posix() == "tables" else bundle_fd
                at_path = os.stat(pure.name, dir_fd=parent_fd, follow_symlinks=False)
                if not _same_opened_file(originals[relative_path], at_path):
                    raise ArtifactPreviewIntegrityError(f"artifact file changed during preview: {relative_path}")
            if not _same_opened_file(tables_opened, os.fstat(tables_fd)) or not _same_opened_file(
                tables_opened, os.stat("tables", dir_fd=bundle_fd, follow_symlinks=False)):
                raise ArtifactPreviewIntegrityError("sealed tables directory changed during preview")
            bundle_at_path = os.stat(
                authority.job.job_id.hex,
                dir_fd=sealed_fd,
                follow_symlinks=False,
            )
            if not _same_opened_file(bundle_opened, bundle_at_path):
                raise ArtifactPreviewIntegrityError("sealed bundle changed during preview")
            if byte_evidence:
                assert root_opened is not None and sealed_opened is not None
                if (
                    not _same_opened_file(bundle_opened, os.fstat(bundle_fd))
                    or not _same_opened_file(sealed_opened, os.fstat(sealed_fd))
                    or not _same_opened_file(sealed_opened, os.stat("sealed", dir_fd=root_fd, follow_symlinks=False))
                    or not _same_opened_file(root_opened, os.fstat(root_fd))
                    or not _same_opened_file(root_opened, os.stat(self.artifact_root, follow_symlinks=False))
                    or self.artifact_root.resolve(strict=True) != self.artifact_root
                ):
                    raise ArtifactPreviewIntegrityError("sealed directory graph changed during complete byte read")
            if complete is not None:
                return complete
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
