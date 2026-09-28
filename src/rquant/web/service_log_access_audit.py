"""Durable, closed pre-read audit for the protected service-log route."""

from __future__ import annotations

import fcntl
import os
import stat
from pathlib import Path
from typing import Literal, Protocol

from pydantic import Field, StrictStr

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel


class ServiceLogAccessRecord(RuntimeContractModel):
    """An authorized read attempt, recorded before the journal is contacted."""

    operator: StrictStr = Field(pattern=r"^[A-Za-z0-9._@-]{1,64}$")
    unit: Literal["rquant-daily.service", "rquant-backup.service"]
    result_class: Literal["admitted"] = "admitted"
    at: AwareUtcDatetime


class ServiceLogAccessAudit(Protocol):
    """Return only after durably recording the authorized read attempt."""

    def preflight(self) -> bool: ...

    def record(self, event: ServiceLogAccessRecord) -> None: ...


AUDIT_FILE_NAME = "service-log-access.jsonl"
_MAX_RECORD_BYTES = 512
_MAX_AUDIT_BYTES = 16 * 1024 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_RDWR | os.O_APPEND | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
_PREFLIGHT_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK


class JsonlServiceLogAccessAudit:
    """Serialize and sync each access decision in a private, append-only file."""

    def __init__(self, directory: Path) -> None:
        self._directory = Path(directory)
        if not self._directory.is_absolute() or ".." in self._directory.parts:
            raise ValueError("service log audit directory must be absolute and canonical")
        descriptor = self._open_directory()
        os.close(descriptor)

    def _open_directory(self) -> int:
        descriptor = os.open(self._directory, _DIRECTORY_FLAGS)
        try:
            identity = os.fstat(descriptor)
            current = self._directory.lstat()
            if (
                not stat.S_ISDIR(identity.st_mode)
                or identity.st_uid != os.geteuid()
                or stat.S_IMODE(identity.st_mode) != 0o700
                or (identity.st_dev, identity.st_ino) != (current.st_dev, current.st_ino)
            ):
                raise ValueError("service log audit directory has unsafe ownership or mode")
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    @staticmethod
    def _open_file(directory_fd: int) -> int:
        try:
            return os.open(
                AUDIT_FILE_NAME,
                _FILE_FLAGS | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=directory_fd,
            )
        except FileExistsError:
            return os.open(AUDIT_FILE_NAME, _FILE_FLAGS, dir_fd=directory_fd)

    @staticmethod
    def _validate_file(descriptor: int, directory_fd: int) -> os.stat_result:
        identity = os.fstat(descriptor)
        current = os.stat(AUDIT_FILE_NAME, dir_fd=directory_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(identity.st_mode)
            or identity.st_uid != os.geteuid()
            or stat.S_IMODE(identity.st_mode) != 0o600
            or identity.st_nlink != 1
            or (identity.st_dev, identity.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise ValueError("service log audit file has unsafe identity or mode")
        return identity

    def preflight(self) -> bool:
        """Check currently verifiable sink safety without creating or changing it."""
        try:
            directory_fd = self._open_directory()
            try:
                filesystem = os.fstatvfs(directory_fd)
                if (
                    filesystem.f_flag & os.ST_RDONLY
                    or filesystem.f_bavail * filesystem.f_frsize < _MAX_RECORD_BYTES
                ):
                    return False
                try:
                    file_fd = os.open(
                        AUDIT_FILE_NAME, _PREFLIGHT_FILE_FLAGS, dir_fd=directory_fd
                    )
                except FileNotFoundError:
                    return True
                try:
                    identity = self._validate_file(file_fd, directory_fd)
                    return (
                        identity.st_size + _MAX_RECORD_BYTES <= _MAX_AUDIT_BYTES
                        and (
                            identity.st_size == 0
                            or os.pread(file_fd, 1, identity.st_size - 1) == b"\n"
                        )
                    )
                finally:
                    os.close(file_fd)
            finally:
                os.close(directory_fd)
        except (OSError, ValueError):
            return False

    def record(self, event: ServiceLogAccessRecord) -> None:
        validated = ServiceLogAccessRecord.model_validate(
            event.model_dump(include={"operator", "unit", "result_class", "at"})
        )
        payload = validated.model_dump_json().encode("utf-8") + b"\n"
        if len(payload) > _MAX_RECORD_BYTES:
            raise ValueError("service log audit record exceeds size limit")

        directory_fd = self._open_directory()
        try:
            fcntl.flock(directory_fd, fcntl.LOCK_EX)
            file_fd = self._open_file(directory_fd)
            try:
                identity = self._validate_file(file_fd, directory_fd)
                if identity.st_size and os.pread(file_fd, 1, identity.st_size - 1) != b"\n":
                    raise ValueError("service log audit has an incomplete last record")
                if identity.st_size + len(payload) > _MAX_AUDIT_BYTES:
                    raise ValueError("service log audit has reached its capacity")
                if os.write(file_fd, payload) != len(payload):
                    raise OSError("service log audit write was incomplete")
                os.fsync(file_fd)
                # Retry directory durability even if an earlier first-create sync failed.
                os.fsync(directory_fd)
            finally:
                os.close(file_fd)
        finally:
            os.close(directory_fd)


__all__ = [
    "AUDIT_FILE_NAME",
    "JsonlServiceLogAccessAudit",
    "ServiceLogAccessAudit",
    "ServiceLogAccessRecord",
]
