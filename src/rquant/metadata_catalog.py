"""Descriptor-sealed read-only access to small independent DuckDB metadata catalogs."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from pathlib import Path
from types import TracebackType
from typing import Self

import duckdb
from pydantic import Field

from rquant.runtime_contracts import RuntimeContractModel


class MetadataCatalogDescriptor(RuntimeContractModel):
    source_path: Path
    device: int = Field(ge=0)
    inode: int = Field(ge=0)
    size_bytes: int = Field(ge=0)
    modified_ns: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ImmutableDuckDBMetadataCatalog:
    """Query a sealed copy captured from one O_NOFOLLOW source descriptor."""

    def __init__(
        self,
        *,
        descriptor: MetadataCatalogDescriptor,
        snapshot_path: Path,
        connection: duckdb.DuckDBPyConnection,
    ) -> None:
        self.descriptor = descriptor
        self.snapshot_path = snapshot_path
        self.connection = connection
        self._closed = False

    @classmethod
    def open(
        cls,
        path: Path,
        *,
        forbidden_paths: tuple[Path, ...] = (),
        snapshot_root: Path | None = None,
    ) -> Self:
        candidate = Path(path)
        if not candidate.is_absolute():
            raise ValueError("metadata catalog path must be absolute")
        if candidate.name in {"", ".", ".."}:
            raise ValueError("metadata catalog path is invalid")
        parent_fd = os.open(
            candidate.parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        source_fd = -1
        snapshot_fd = -1
        snapshot_path: Path | None = None
        try:
            parent_identity = os.fstat(parent_fd)
            if not stat.S_ISDIR(parent_identity.st_mode):
                raise ValueError("metadata catalog parent is not a physical directory")
            source_fd = os.open(
                candidate.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=parent_fd,
            )
            opened = os.fstat(source_fd)
            if not stat.S_ISREG(opened.st_mode):
                raise ValueError("metadata catalog must be a regular file")
            cls._reject_operational_alias(opened, forbidden_paths)

            root = Path(snapshot_root or tempfile.gettempdir())
            root.mkdir(parents=True, mode=0o700, exist_ok=True)
            snapshot_fd, raw_snapshot_path = tempfile.mkstemp(
                prefix="rquant-metadata-",
                suffix=".duckdb",
                dir=root,
            )
            snapshot_path = Path(raw_snapshot_path)
            os.fchmod(snapshot_fd, 0o600)
            digest = hashlib.sha256()
            while True:
                chunk = os.read(source_fd, 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                view = memoryview(chunk)
                while view:
                    written = os.write(snapshot_fd, view)
                    view = view[written:]
            os.fsync(snapshot_fd)
            completed = os.fstat(source_fd)
            if cls._identity(opened) != cls._identity(completed):
                raise RuntimeError("metadata catalog changed while sealing")
            os.close(snapshot_fd)
            snapshot_fd = -1
            connection = duckdb.connect(str(snapshot_path), read_only=True)
            descriptor = MetadataCatalogDescriptor(
                source_path=candidate,
                device=opened.st_dev,
                inode=opened.st_ino,
                size_bytes=opened.st_size,
                modified_ns=opened.st_mtime_ns,
                sha256=digest.hexdigest(),
            )
            return cls(
                descriptor=descriptor,
                snapshot_path=snapshot_path,
                connection=connection,
            )
        except BaseException:
            if snapshot_fd >= 0:
                os.close(snapshot_fd)
            if snapshot_path is not None:
                snapshot_path.unlink(missing_ok=True)
            raise
        finally:
            if source_fd >= 0:
                os.close(source_fd)
            os.close(parent_fd)

    @staticmethod
    def _identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    @staticmethod
    def _reject_operational_alias(
        opened: os.stat_result,
        forbidden_paths: tuple[Path, ...],
    ) -> None:
        for path in forbidden_paths:
            try:
                forbidden = os.stat(path, follow_symlinks=True)
            except FileNotFoundError:
                continue
            if (opened.st_dev, opened.st_ino) == (forbidden.st_dev, forbidden.st_ino):
                raise ValueError("metadata catalog aliases an operational database")

    def __enter__(self) -> Self:
        if self._closed:
            raise RuntimeError("metadata catalog is closed")
        return self

    def __exit__(
        self,
        _error_type: type[BaseException] | None,
        _error: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.connection.close()
        self.snapshot_path.unlink(missing_ok=True)


__all__ = ["ImmutableDuckDBMetadataCatalog", "MetadataCatalogDescriptor"]
