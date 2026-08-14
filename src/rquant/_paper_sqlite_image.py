"""Private descriptor-bound SQLite image primitives for paper migration."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from rquant.paper_migration_publication import PublicationFileIdentity

_PRODUCTION_MAX_SQLITE_IMAGE_BYTES = 16 * 1024 * 1024
_READ_CHUNK_BYTES = 1024 * 1024
_SERIALIZED_SQLITE_IMAGE_OPERATIONAL_BUDGET_BYTES = 96 * 1024 * 1024


class _StableSQLiteImageError(ValueError):
    """A descriptor image cannot safely be used as a SQLite database."""


@dataclass(frozen=True)
class _StableSQLiteImageBinding:
    identity: PublicationFileIdentity
    sha256: str


@dataclass(frozen=True)
class _StableSQLiteImage:
    data: bytes
    binding: _StableSQLiteImageBinding


class _SQLiteMemoryAdapter(Protocol):
    def open_memory(self) -> sqlite3.Connection: ...


class _DefaultSQLiteMemoryAdapter:
    def open_memory(self) -> sqlite3.Connection:
        return sqlite3.connect(":memory:", isolation_level=None)


def _file_identity(metadata: os.stat_result) -> PublicationFileIdentity:
    from rquant.paper_migration_publication import PublicationFileIdentity

    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_size < 0:
        raise _StableSQLiteImageError("SQLite image must be a singly-linked regular file")
    return PublicationFileIdentity(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        uid=metadata.st_uid,
        gid=metadata.st_gid,
        mode=stat.S_IMODE(metadata.st_mode),
        nlink=1,
        size=metadata.st_size,
        mtime_ns=metadata.st_mtime_ns,
        ctime_ns=metadata.st_ctime_ns,
    )


def _read_exact_hash(descriptor: int, *, expected_size: int) -> str:
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        digest = hashlib.sha256()
        remaining = expected_size
        while remaining:
            chunk = os.read(descriptor, min(_READ_CHUNK_BYTES, remaining))
            if not chunk:
                raise _StableSQLiteImageError("SQLite image ended before its captured size")
            digest.update(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise _StableSQLiteImageError("SQLite image grew beyond its captured size")
        return digest.hexdigest()
    except OSError as exc:
        raise _StableSQLiteImageError("SQLite image descriptor cannot be read") from exc


def _capture_stable_sqlite_image_with_bound(
    descriptor: int,
    *,
    max_bytes: int,
) -> _StableSQLiteImage:
    try:
        before = _file_identity(os.fstat(descriptor))
    except OSError as exc:
        raise _StableSQLiteImageError("SQLite image descriptor cannot be inspected") from exc
    if before.size > max_bytes:
        raise _StableSQLiteImageError("SQLite image exceeds the fixed image capacity")
    try:
        data = bytearray(before.size)
    except MemoryError as exc:
        raise _StableSQLiteImageError("SQLite image capture cannot allocate") from exc
    offset = 0
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        while offset < before.size:
            chunk = os.read(descriptor, min(_READ_CHUNK_BYTES, before.size - offset))
            if not chunk:
                raise _StableSQLiteImageError("SQLite image ended before its captured size")
            data[offset : offset + len(chunk)] = chunk
            offset += len(chunk)
        if os.read(descriptor, 1):
            raise _StableSQLiteImageError("SQLite image grew beyond its captured size")
        after = _file_identity(os.fstat(descriptor))
    except OSError as exc:
        raise _StableSQLiteImageError("SQLite image descriptor cannot be read") from exc
    if before != after:
        raise _StableSQLiteImageError("SQLite image identity changed while captured")
    try:
        immutable = bytes(data)
    except MemoryError as exc:
        raise _StableSQLiteImageError("SQLite image capture cannot finalize") from exc
    return _StableSQLiteImage(
        data=immutable,
        binding=_StableSQLiteImageBinding(
            identity=before,
            sha256=hashlib.sha256(immutable).hexdigest(),
        ),
    )


def _capture_stable_sqlite_image(descriptor: int) -> _StableSQLiteImage:
    return _capture_stable_sqlite_image_with_bound(
        descriptor,
        max_bytes=_PRODUCTION_MAX_SQLITE_IMAGE_BYTES,
    )


def _capture_stable_sqlite_image_for_test(
    descriptor: int,
    *,
    max_bytes: int,
) -> _StableSQLiteImage:
    if max_bytes <= 0 or max_bytes > _PRODUCTION_MAX_SQLITE_IMAGE_BYTES:
        raise ValueError(
            "test SQLite image capacity must be positive and no larger than production"
        )
    return _capture_stable_sqlite_image_with_bound(descriptor, max_bytes=max_bytes)


def _revalidate_stable_sqlite_image(
    descriptor: int,
    binding: _StableSQLiteImageBinding,
) -> None:
    try:
        before = _file_identity(os.fstat(descriptor))
    except OSError as exc:
        raise _StableSQLiteImageError("SQLite image descriptor cannot be inspected") from exc
    if before != binding.identity:
        raise _StableSQLiteImageError("SQLite image identity differs from its binding")
    digest = _read_exact_hash(descriptor, expected_size=binding.identity.size)
    try:
        after = _file_identity(os.fstat(descriptor))
    except OSError as exc:
        raise _StableSQLiteImageError("SQLite image descriptor cannot be inspected") from exc
    if after != binding.identity or digest != binding.sha256:
        raise _StableSQLiteImageError("SQLite image bytes differ from its binding")


def _open_memory_sqlite_image(
    image: _StableSQLiteImage,
    *,
    adapter: _SQLiteMemoryAdapter | None = None,
) -> sqlite3.Connection:
    connection: sqlite3.Connection | None = None
    try:
        connection = (adapter or _DefaultSQLiteMemoryAdapter()).open_memory()
        deserialize = getattr(connection, "deserialize", None)
        if not callable(deserialize):
            raise _StableSQLiteImageError("SQLite deserialize support is unavailable")
        deserialize(image.data)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA temp_store = MEMORY")
        connection.execute("PRAGMA cache_size = -8192")
        return connection
    except BaseException as exc:
        if connection is not None:
            with suppress(BaseException):
                connection.close()
        if isinstance(exc, _StableSQLiteImageError):
            raise
        raise _StableSQLiteImageError("SQLite memory image cannot be opened") from exc
