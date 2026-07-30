"""Job-id-only deterministic ZIP export for sealed Strategy Lab results."""

from __future__ import annotations

import os
import stat
from contextlib import suppress
from pathlib import Path
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from rquant.lab_artifacts import (
    LabArtifactIntegrityError,
    LabBoundZipDestination,
    LabJobArtifactStore,
    _ensure_private_directory,
    _secure_absolute_path,
    _secure_open_directory,
    _sha256_descriptor,
)
from rquant.lab_jobs import LabJobReader


class LabJobZipExportUnavailableError(RuntimeError):
    """The ledger does not authorize export for the requested job."""


class _ExportModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
    )


class _LabJobZipExportRequest(_ExportModel):
    job_id: UUID


class LabJobZipExportReceipt(_ExportModel):
    request_id: UUID
    job_id: UUID
    path: Path
    byte_size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: Path) -> Path:
        normalized = _secure_absolute_path(value)
        if value != normalized:
            raise ValueError("export receipt path must be absolute and normalized")
        return value


class LabJobZipExportFacade:
    """Publish request-scoped ZIPs beneath one constructor-bound private root."""

    def __init__(
        self,
        *,
        reader: LabJobReader,
        artifact_store: LabJobArtifactStore,
        export_root: Path,
    ) -> None:
        self.reader = reader
        self.artifact_store = artifact_store
        self.export_root = _secure_absolute_path(export_root)
        _ensure_private_directory(
            self.export_root,
            manage_existing=False,
            require_private_existing=True,
        )
        descriptor = _secure_open_directory(self.export_root, create=False)
        try:
            observed = self._validate_private_directory(descriptor, label="export root")
            self._export_root_identity = (observed.st_dev, observed.st_ino)
        finally:
            os.close(descriptor)

    @staticmethod
    def _validate_private_directory(descriptor: int, *, label: str) -> os.stat_result:
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != 0o700
            or observed.st_uid != os.geteuid()
        ):
            raise LabArtifactIntegrityError(f"{label} is not a private owned directory")
        return observed

    def _open_bound_export_root(self) -> int:
        descriptor = _secure_open_directory(self.export_root, create=False)
        try:
            observed = self._validate_private_directory(descriptor, label="export root")
            if (observed.st_dev, observed.st_ino) != self._export_root_identity:
                raise LabArtifactIntegrityError("export root identity changed")
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    @classmethod
    def _open_private_child(
        cls,
        parent_descriptor: int,
        name: str,
        *,
        label: str,
        create: bool = False,
    ) -> int:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        if create:
            with suppress(FileExistsError):
                os.mkdir(name, mode=0o700, dir_fd=parent_descriptor)
                os.fsync(parent_descriptor)
        before = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
        try:
            opened = cls._validate_private_directory(descriptor, label=label)
            at_path = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
            if (
                before.st_dev,
                before.st_ino,
                stat.S_IFMT(before.st_mode),
            ) != (
                opened.st_dev,
                opened.st_ino,
                stat.S_IFDIR,
            ) or (
                at_path.st_dev,
                at_path.st_ino,
                stat.S_IFMT(at_path.st_mode),
            ) != (
                opened.st_dev,
                opened.st_ino,
                stat.S_IFDIR,
            ):
                raise LabArtifactIntegrityError(f"{label} path identity changed")
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    def _build_receipt(
        self,
        *,
        request_id: UUID,
        job_id: UUID,
        path: Path,
    ) -> LabJobZipExportReceipt:
        descriptors: list[int] = []
        try:
            root_descriptor = self._open_bound_export_root()
            descriptors.append(root_descriptor)
            job_descriptor = self._open_private_child(
                root_descriptor,
                job_id.hex,
                label="job export directory",
            )
            descriptors.append(job_descriptor)
            request_descriptor = self._open_private_child(
                job_descriptor,
                request_id.hex,
                label="request export directory",
            )
            descriptors.append(request_descriptor)
            file_descriptor = os.open(
                "result.zip",
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=request_descriptor,
            )
            descriptors.append(file_descriptor)
            before = os.fstat(file_descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_uid != os.geteuid()
            ):
                raise LabArtifactIntegrityError("exported ZIP is not a private regular file")
            sha256 = _sha256_descriptor(file_descriptor)
            after = os.fstat(file_descriptor)
            if (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            ) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            ):
                raise LabArtifactIntegrityError("exported ZIP changed while hashing")
            rebound_root = self._open_bound_export_root()
            os.close(rebound_root)
            return LabJobZipExportReceipt(
                request_id=request_id,
                job_id=job_id,
                path=path,
                byte_size=after.st_size,
                sha256=sha256,
            )
        finally:
            for descriptor in reversed(descriptors):
                with suppress(OSError):
                    os.close(descriptor)

    def export(self, job_id: UUID) -> LabJobZipExportReceipt:
        """Create a unique request-scoped ZIP; repeated calls never reuse a path."""
        request = _LabJobZipExportRequest(job_id=job_id)
        authority = self.reader.get_artifact_preview_authority(request.job_id)
        if authority is None:
            raise LabJobZipExportUnavailableError(
                "ZIP export requires a succeeded job with sealed result evidence"
            )
        request_id = uuid4()
        destination = self.export_root / request.job_id.hex / request_id.hex / "result.zip"
        descriptors: list[int] = []
        try:
            root_descriptor = self._open_bound_export_root()
            descriptors.append(root_descriptor)
            job_descriptor = self._open_private_child(
                root_descriptor,
                request.job_id.hex,
                label="job export directory",
                create=True,
            )
            descriptors.append(job_descriptor)
            request_descriptor = self._open_private_child(
                job_descriptor,
                request_id.hex,
                label="request export directory",
                create=True,
            )
            descriptors.append(request_descriptor)
            request_directory = os.fstat(request_descriptor)
            published = self.artifact_store.export_deterministic_zip_bound(
                authority.evidence.sealed_path,
                authority.evidence,
                LabBoundZipDestination(
                    directory_path=destination.parent,
                    directory_descriptor=request_descriptor,
                    directory_device=request_directory.st_dev,
                    directory_inode=request_directory.st_ino,
                    file_name=destination.name,
                ),
            )
            if published != destination:
                raise LabArtifactIntegrityError("artifact store returned an unexpected export path")
            return self._build_receipt(
                request_id=request_id,
                job_id=request.job_id,
                path=published,
            )
        finally:
            for descriptor in reversed(descriptors):
                with suppress(OSError):
                    os.close(descriptor)
