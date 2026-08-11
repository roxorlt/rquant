"""Job-id-only deterministic ZIP export for sealed Strategy Lab results."""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from rquant.lab_artifacts import (
    LabArtifactIntegrityError,
    LabBoundZipDestination,
    LabJobArtifactStore,
    _ensure_private_directory,
    _rename_noreplace,
    _secure_absolute_path,
    _secure_open_directory,
    _sha256_descriptor,
)
from rquant.lab_jobs import LabJobReader


class LabJobZipExportUnavailableError(RuntimeError):
    """The ledger does not authorize export for the requested job."""


class LabJobZipExportCapacityError(RuntimeError):
    """The bounded online export spool needs offline tombstone maintenance."""


_EXPORT_DIRECTORY_NAME = re.compile(r"^[0-9a-f]{32}$")


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
        max_export_records: int = 1024,
    ) -> None:
        if type(max_export_records) is not int or max_export_records < 1:
            raise ValueError("max_export_records must be a positive integer")
        self.reader = reader
        self.artifact_store = artifact_store
        self.export_root = _secure_absolute_path(export_root)
        self.max_export_records = max_export_records
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

    @contextmanager
    def _locked_export_root(self) -> Iterator[int]:
        root_descriptor = self._open_bound_export_root()
        try:
            flock(root_descriptor, LOCK_EX)
            yield root_descriptor
        finally:
            with suppress(OSError):
                flock(root_descriptor, LOCK_UN)
            with suppress(OSError):
                os.close(root_descriptor)

    def _enforce_record_budget(self, root_descriptor: int) -> None:
        records = 0
        for job_name in os.listdir(root_descriptor):
            if _EXPORT_DIRECTORY_NAME.fullmatch(job_name) is None:
                raise LabArtifactIntegrityError("export root contains an unknown entry")
            job_descriptor = self._open_private_child(
                root_descriptor,
                job_name,
                label="job export directory",
            )
            try:
                for request_name in os.listdir(job_descriptor):
                    if _EXPORT_DIRECTORY_NAME.fullmatch(request_name) is None:
                        raise LabArtifactIntegrityError(
                            "job export directory contains an unknown entry"
                        )
                    request_descriptor = self._open_private_child(
                        job_descriptor,
                        request_name,
                        label="request export directory",
                    )
                    os.close(request_descriptor)
                    records += 1
                    if records >= self.max_export_records:
                        raise LabJobZipExportCapacityError(
                            "online ZIP export record budget is exhausted; "
                            "run offline tombstone maintenance"
                        )
            finally:
                os.close(job_descriptor)

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
            with self._locked_export_root() as root_descriptor:
                self._enforce_record_budget(root_descriptor)
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
                    raise LabArtifactIntegrityError(
                        "artifact store returned an unexpected export path"
                    )
                return self._build_receipt(
                    request_id=request_id,
                    job_id=request.job_id,
                    path=published,
                )
        finally:
            for descriptor in reversed(descriptors):
                with suppress(OSError):
                    os.close(descriptor)

    def discard(self, receipt: LabJobZipExportReceipt) -> None:
        """Reclaim verified ZIP bytes without path-based online deletion."""
        if not isinstance(receipt, LabJobZipExportReceipt):
            raise TypeError("receipt must be a LabJobZipExportReceipt")
        validated = LabJobZipExportReceipt.model_validate(receipt)
        expected = self.export_root / validated.job_id.hex / validated.request_id.hex / "result.zip"
        if validated.path != expected:
            raise LabArtifactIntegrityError("export receipt path is outside its request scope")

        descriptors: list[int] = []
        try:
            with self._locked_export_root() as root_descriptor:
                job_descriptor = self._open_private_child(
                    root_descriptor,
                    validated.job_id.hex,
                    label="job export directory",
                )
                descriptors.append(job_descriptor)
                request_descriptor = self._open_private_child(
                    job_descriptor,
                    validated.request_id.hex,
                    label="request export directory",
                )
                descriptors.append(request_descriptor)
                file_descriptor = os.open(
                    "result.zip",
                    os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=request_descriptor,
                )
                descriptors.append(file_descriptor)
                opened = os.fstat(file_descriptor)
                at_path = os.stat(
                    "result.zip",
                    dir_fd=request_descriptor,
                    follow_symlinks=False,
                )
                if (
                    not stat.S_ISREG(opened.st_mode)
                    or opened.st_nlink != 1
                    or stat.S_IMODE(opened.st_mode) != 0o600
                    or opened.st_uid != os.geteuid()
                    or (opened.st_dev, opened.st_ino) != (at_path.st_dev, at_path.st_ino)
                    or opened.st_size != validated.byte_size
                    or _sha256_descriptor(file_descriptor) != validated.sha256
                ):
                    raise LabArtifactIntegrityError(
                        "export receipt does not match the published ZIP"
                    )
                quarantine_name = f".result.zip.{uuid4().hex}.discarded"
                _rename_noreplace(
                    request_descriptor,
                    "result.zip",
                    request_descriptor,
                    quarantine_name,
                )
                renamed_path = os.stat(
                    quarantine_name,
                    dir_fd=request_descriptor,
                    follow_symlinks=False,
                )
                renamed_open = os.fstat(file_descriptor)
                if not self._same_renamed_file(renamed_path, renamed_open, opened):
                    raise LabArtifactIntegrityError(
                        "export receipt identity changed while isolating cleanup"
                    )
                self._before_discard_truncate(request_descriptor, quarantine_name)
                os.ftruncate(file_descriptor, 0)
                os.fsync(file_descriptor)
                retired = os.fstat(file_descriptor)
                if not self._same_retired_file(retired, renamed_open):
                    raise LabArtifactIntegrityError(
                        "export receipt data reclamation was not identity-bound"
                    )
                os.fsync(request_descriptor)
        except LabArtifactIntegrityError:
            raise
        except OSError as exc:
            raise LabArtifactIntegrityError(
                "export receipt path changed or could not be discarded"
            ) from exc
        finally:
            for descriptor in reversed(descriptors):
                with suppress(OSError):
                    os.close(descriptor)

    @staticmethod
    def _same_renamed_file(
        at_path: os.stat_result,
        opened: os.stat_result,
        original: os.stat_result,
    ) -> bool:
        def stable(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
            return (
                value.st_dev,
                value.st_ino,
                value.st_mode,
                value.st_nlink,
                value.st_uid,
                value.st_size,
                value.st_mtime_ns,
            )

        return stable(at_path) == stable(opened) == stable(original)

    @staticmethod
    def _same_retired_file(
        observed: os.stat_result,
        expected: os.stat_result,
    ) -> bool:
        return (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_nlink,
            observed.st_uid,
        ) == (
            expected.st_dev,
            expected.st_ino,
            expected.st_mode,
            expected.st_nlink,
            expected.st_uid,
        ) and observed.st_size == 0

    @staticmethod
    def _before_discard_truncate(_directory_descriptor: int, _name: str) -> None:
        """Fault-injection boundary before descriptor-bound data reclamation."""
