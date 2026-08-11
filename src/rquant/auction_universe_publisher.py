"""Atomic publisher for the point-in-time opening-auction universe."""

from __future__ import annotations

import errno
import fcntl
import os
import stat
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import date, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from rquant.auction_universe_authority import AuctionUniverseAuthority

_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_FILE_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
_LOCK_FLAGS = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
_MAX_AUTHORITY_BYTES = 2 * 1024 * 1024
_READ_CHUNK_BYTES = 256 * 1024


class AuctionUniversePublicationError(RuntimeError):
    """The auction universe could not be published without losing integrity."""


class AuctionUniversePublicationReceipt(BaseModel):
    model_config = ConfigDict(frozen=True)

    published: bool
    generation_path: Path
    content_sha256: str


def _normalized_absolute_path(path: Path) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.normpath(os.fspath(candidate)))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError("auction universe root must be an absolute normalized path")
    return candidate


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _validate_directory(value: os.stat_result, *, label: str, private: bool) -> None:
    if not stat.S_ISDIR(value.st_mode) or stat.S_ISLNK(value.st_mode):
        raise AuctionUniversePublicationError(f"{label} is a symlink or unsafe directory")
    if private:
        if value.st_uid != os.geteuid():
            raise AuctionUniversePublicationError(f"{label} owner does not match the process")
        if stat.S_IMODE(value.st_mode) != 0o700:
            raise AuctionUniversePublicationError(f"{label} must have mode 0700")


def _walk_to_directory(path: Path, *, create_final: bool) -> int:
    descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
    try:
        for index, component in enumerate(path.parts[1:]):
            is_final = index == len(path.parts[1:]) - 1
            try:
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                if not (create_final and is_final):
                    raise
                os.mkdir(component, mode=0o700, dir_fd=descriptor)
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            _validate_directory(before, label="auction universe path", private=is_final)
            child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            opened = os.fstat(child)
            if _identity(before) != _identity(opened):
                os.close(child)
                raise AuctionUniversePublicationError("auction universe path changed while opening")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except AuctionUniversePublicationError:
        os.close(descriptor)
        raise
    except OSError as exc:
        os.close(descriptor)
        raise AuctionUniversePublicationError(
            "auction universe path is unavailable, a symlink, or unsafe"
        ) from exc


def _open_private_directory_at(parent: int, name: str) -> int:
    try:
        with suppress(FileExistsError):
            os.mkdir(name, mode=0o700, dir_fd=parent)
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        _validate_directory(before, label=name, private=True)
        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent)
        opened = os.fstat(descriptor)
        if _identity(before) != _identity(opened):
            os.close(descriptor)
            raise AuctionUniversePublicationError(f"{name} changed while opening")
        return descriptor
    except AuctionUniversePublicationError:
        raise
    except OSError as exc:
        raise AuctionUniversePublicationError(f"{name} is unavailable or unsafe") from exc


def _validate_private_file(value: os.stat_result, *, label: str) -> None:
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
        raise AuctionUniversePublicationError(f"{label} is a symlink or unsafe file")
    if value.st_uid != os.geteuid():
        raise AuctionUniversePublicationError(f"{label} owner does not match the process")
    if stat.S_IMODE(value.st_mode) != 0o600:
        raise AuctionUniversePublicationError(f"{label} must have mode 0600")
    if value.st_nlink != 1:
        raise AuctionUniversePublicationError(f"{label} must have one hard link")
    if value.st_size <= 0 or value.st_size > _MAX_AUTHORITY_BYTES:
        raise AuctionUniversePublicationError(f"{label} has an unsafe size")


def _read_private_file_at(parent: int, name: str, *, label: str) -> bytes:
    descriptor = -1
    try:
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        _validate_private_file(before, label=label)
        descriptor = os.open(name, _FILE_READ_FLAGS, dir_fd=parent)
        opened = os.fstat(descriptor)
        current = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if _identity(before) != _identity(opened) or _identity(current) != _identity(opened):
            raise AuctionUniversePublicationError(f"{label} changed while opening")
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, _READ_CHUNK_BYTES):
            total += len(chunk)
            if total > _MAX_AUTHORITY_BYTES:
                raise AuctionUniversePublicationError(f"{label} exceeds its size limit")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if _identity(after) != _identity(opened) or _identity(active) != _identity(after):
            raise AuctionUniversePublicationError(f"{label} changed while reading")
        return b"".join(chunks)
    except AuctionUniversePublicationError:
        raise
    except OSError as exc:
        raise AuctionUniversePublicationError(f"{label} is unavailable or unsafe") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _write_new_private_file_at(parent: int, name: str, payload: bytes) -> None:
    descriptor = -1
    try:
        descriptor = os.open(name, _FILE_CREATE_FLAGS, 0o600, dir_fd=parent)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise AuctionUniversePublicationError("short write while publishing authority")
            view = view[written:]
        os.fsync(descriptor)
        _validate_private_file(os.fstat(descriptor), label=name)
    except AuctionUniversePublicationError:
        raise
    except OSError as exc:
        raise AuctionUniversePublicationError(f"cannot create private authority {name}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


@contextmanager
def _exclusive_publication_lock(root: int) -> Iterator[None]:
    descriptor = -1
    try:
        descriptor = os.open(".publish.lock", _LOCK_FLAGS, 0o600, dir_fd=root)
        lock_stat = os.fstat(descriptor)
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_uid != os.geteuid()
            or stat.S_IMODE(lock_stat.st_mode) != 0o600
            or lock_stat.st_nlink != 1
        ):
            raise AuctionUniversePublicationError("publication lock is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        current = os.stat(".publish.lock", dir_fd=root, follow_symlinks=False)
        if _identity(current) != _identity(os.fstat(descriptor)):
            raise AuctionUniversePublicationError("publication lock changed while acquiring")
        yield
    except AuctionUniversePublicationError:
        raise
    except OSError as exc:
        raise AuctionUniversePublicationError("cannot acquire publication lock") from exc
    finally:
        if descriptor >= 0:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


def _ensure_generation(generations: int, name: str, payload: bytes) -> None:
    try:
        _write_new_private_file_at(generations, name, payload)
        os.fsync(generations)
    except AuctionUniversePublicationError as exc:
        cause = exc.__cause__
        if not isinstance(cause, OSError) or cause.errno != errno.EEXIST:
            raise
        if _read_private_file_at(generations, name, label="auction universe generation") != payload:
            raise AuctionUniversePublicationError(
                "content-addressed generation conflicts with canonical payload"
            ) from exc


def _publish_current(root: int, payload: bytes) -> bool:
    try:
        existing = _read_private_file_at(root, "current.json", label="auction universe current")
    except AuctionUniversePublicationError as exc:
        cause = exc.__cause__
        if not isinstance(cause, FileNotFoundError):
            raise
    else:
        if existing == payload:
            return False

    temporary = f".current.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        _write_new_private_file_at(root, temporary, payload)
        os.replace(temporary, "current.json", src_dir_fd=root, dst_dir_fd=root)
        os.fsync(root)
        if _read_private_file_at(root, "current.json", label="auction universe current") != payload:
            raise AuctionUniversePublicationError("published authority does not match payload")
        return True
    finally:
        with suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=root)


def publish_auction_universe_authority(
    root: Path,
    *,
    effective_trade_date: date,
    reference_trade_date: date,
    available_at: datetime,
    producer_commit: str,
    source_snapshot_id: str,
    codes: tuple[str, ...],
) -> AuctionUniversePublicationReceipt:
    """Seal one immutable universe generation and atomically publish its head."""

    authority = AuctionUniverseAuthority.create(
        effective_trade_date=effective_trade_date,
        reference_trade_date=reference_trade_date,
        available_at=available_at,
        producer_commit=producer_commit,
        source_snapshot_id=source_snapshot_id,
        codes=codes,
    )
    payload = authority.canonical_json_bytes()
    candidate = _normalized_absolute_path(root)
    root_descriptor = _walk_to_directory(candidate, create_final=True)
    generations_descriptor = -1
    try:
        with _exclusive_publication_lock(root_descriptor):
            generations_descriptor = _open_private_directory_at(root_descriptor, "generations")
            generation_name = f"{authority.content_sha256}.json"
            _ensure_generation(generations_descriptor, generation_name, payload)
            published = _publish_current(root_descriptor, payload)
    finally:
        if generations_descriptor >= 0:
            os.close(generations_descriptor)
        os.close(root_descriptor)
    return AuctionUniversePublicationReceipt(
        published=published,
        generation_path=candidate / "generations" / generation_name,
        content_sha256=authority.content_sha256,
    )


__all__ = [
    "AuctionUniversePublicationError",
    "AuctionUniversePublicationReceipt",
    "publish_auction_universe_authority",
]
