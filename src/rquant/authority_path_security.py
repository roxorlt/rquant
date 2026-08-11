"""Descriptor-bound path validation for privileged authority runtimes."""

from __future__ import annotations

import os
import stat
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


class AuthorityPathSecurityError(RuntimeError):
    """A protected authority path cannot be bound to trusted filesystem objects."""


@dataclass(frozen=True)
class SecurePathMetadata:
    uid: int
    gid: int
    mode: int
    device: int
    inode: int
    size: int


@dataclass(frozen=True)
class SecureCreatedFile:
    metadata: SecurePathMetadata
    created: bool


def _canonical_relative(path: Path, trusted_root: Path) -> tuple[str, ...]:
    candidate = Path(os.path.abspath(path))
    root = Path(os.path.abspath(trusted_root))
    if candidate != path or root != trusted_root or not candidate.is_absolute():
        raise AuthorityPathSecurityError("protected path is not canonical")
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise AuthorityPathSecurityError("protected path escapes trusted root") from exc
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise AuthorityPathSecurityError("protected path is not a child of trusted root")
    return relative.parts


def _validate_directory(
    metadata: os.stat_result,
    *,
    allowed_owner_uids: frozenset[int],
    label: str,
) -> None:
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid not in allowed_owner_uids
        or metadata.st_mode & 0o022
    ):
        raise AuthorityPathSecurityError(f"{label} ancestor is unsafe")


def secure_path_metadata(
    path: Path,
    *,
    trusted_root: Path = Path("/"),
    allowed_ancestor_uids: frozenset[int] | None = None,
    kind: Literal["directory", "file", "socket"],
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
) -> SecurePathMetadata:
    """Bind every ancestor and the final object through stable descriptors.

    This is the reusable form of the Daily signer preflight walk: each named
    object is observed without following links, opened relative to its already
    bound parent, and matched by device/inode before the walk advances.
    """

    relative_parts = _canonical_relative(path, trusted_root)
    allowed = allowed_ancestor_uids or frozenset({0, expected_uid})
    descriptors: list[int] = []
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        parent = os.open(trusted_root, directory_flags)
        descriptors.append(parent)
        _validate_directory(
            os.fstat(parent),
            allowed_owner_uids=allowed,
            label="trusted root",
        )
        for component in relative_parts[:-1]:
            try:
                named = os.stat(component, dir_fd=parent, follow_symlinks=False)
                child = os.open(component, directory_flags, dir_fd=parent)
            except OSError as exc:
                raise AuthorityPathSecurityError("protected path ancestor is unavailable") from exc
            opened = os.fstat(child)
            try:
                _validate_directory(
                    named,
                    allowed_owner_uids=allowed,
                    label="protected path",
                )
                if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
                    raise AuthorityPathSecurityError("protected path ancestor identity changed")
            except BaseException:
                os.close(child)
                raise
            descriptors.append(child)
            parent = child

        name = relative_parts[-1]
        try:
            named = os.stat(name, dir_fd=parent, follow_symlinks=False)
        except OSError as exc:
            raise AuthorityPathSecurityError("protected path is unavailable") from exc
        if kind == "directory":
            try:
                final_descriptor = os.open(name, directory_flags, dir_fd=parent)
            except OSError as exc:
                raise AuthorityPathSecurityError("protected directory is unavailable") from exc
            descriptors.append(final_descriptor)
            opened = os.fstat(final_descriptor)
            kind_matches = stat.S_ISDIR(opened.st_mode)
        elif kind == "file":
            try:
                final_descriptor = os.open(
                    name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=parent,
                )
            except OSError as exc:
                raise AuthorityPathSecurityError("protected file is unavailable") from exc
            descriptors.append(final_descriptor)
            opened = os.fstat(final_descriptor)
            kind_matches = stat.S_ISREG(opened.st_mode) and opened.st_nlink == 1
        else:
            opened = os.stat(name, dir_fd=parent, follow_symlinks=False)
            kind_matches = stat.S_ISSOCK(opened.st_mode)
        if (
            not kind_matches
            or stat.S_ISLNK(named.st_mode)
            or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
            or opened.st_uid != expected_uid
            or opened.st_gid != expected_gid
            or stat.S_IMODE(opened.st_mode) != expected_mode
        ):
            raise AuthorityPathSecurityError("protected path owner, mode, or identity is unsafe")
        rebound = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (opened.st_dev, opened.st_ino) != (rebound.st_dev, rebound.st_ino):
            raise AuthorityPathSecurityError("protected path identity changed")
        return SecurePathMetadata(
            uid=opened.st_uid,
            gid=opened.st_gid,
            mode=stat.S_IMODE(opened.st_mode),
            device=opened.st_dev,
            inode=opened.st_ino,
            size=opened.st_size,
        )
    finally:
        for descriptor in reversed(descriptors):
            with suppress(OSError):
                os.close(descriptor)


def read_secure_regular_file(
    path: Path,
    *,
    trusted_root: Path = Path("/"),
    allowed_ancestor_uids: frozenset[int] | None = None,
    expected_uid: int,
    expected_gid: int,
    allowed_final_uids: frozenset[int] | None = None,
    allowed_final_gids: frozenset[int] | None = None,
    allowed_modes: frozenset[int],
    max_bytes: int,
    min_bytes: int = 1,
) -> bytes:
    """Read a bounded regular file after a full descriptor-bound path walk."""

    if max_bytes < 1 or min_bytes < 0 or min_bytes > max_bytes or not allowed_modes:
        raise ValueError("secure file policy is invalid")
    relative_parts = _canonical_relative(path, trusted_root)
    allowed = allowed_ancestor_uids or frozenset({0, expected_uid})
    final_uids = allowed_final_uids or frozenset({expected_uid})
    final_gids = allowed_final_gids or frozenset({expected_gid})
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptors: list[int] = []
    try:
        parent = os.open(trusted_root, directory_flags)
        descriptors.append(parent)
        _validate_directory(
            os.fstat(parent),
            allowed_owner_uids=allowed,
            label="trusted root",
        )
        for component in relative_parts[:-1]:
            named = os.stat(component, dir_fd=parent, follow_symlinks=False)
            child = os.open(component, directory_flags, dir_fd=parent)
            opened = os.fstat(child)
            try:
                _validate_directory(
                    named,
                    allowed_owner_uids=allowed,
                    label="protected path",
                )
                if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
                    raise AuthorityPathSecurityError("protected path ancestor identity changed")
            except BaseException:
                os.close(child)
                raise
            descriptors.append(child)
            parent = child
        name = relative_parts[-1]
        named = os.stat(name, dir_fd=parent, follow_symlinks=False)
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent,
        )
        descriptors.append(descriptor)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(named.st_mode)
            or stat.S_ISLNK(named.st_mode)
            or named.st_nlink != 1
            or named.st_uid not in final_uids
            or named.st_gid not in final_gids
            or stat.S_IMODE(named.st_mode) not in allowed_modes
            or not min_bytes <= named.st_size <= max_bytes
            or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise AuthorityPathSecurityError("protected file owner, mode, or identity is unsafe")
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, min(64 * 1024, max_bytes + 1 - total)):
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                raise AuthorityPathSecurityError("protected file is oversized")
        after = os.fstat(descriptor)
        rebound = os.stat(name, dir_fd=parent, follow_symlinks=False)
        before_identity = (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        )
        if before_identity != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) or (opened.st_dev, opened.st_ino) != (rebound.st_dev, rebound.st_ino):
            raise AuthorityPathSecurityError("protected file changed while reading")
        return b"".join(chunks)
    except AuthorityPathSecurityError:
        raise
    except OSError as exc:
        raise AuthorityPathSecurityError("protected path is unavailable") from exc
    finally:
        for descriptor in reversed(descriptors):
            with suppress(OSError):
                os.close(descriptor)


def secure_create_regular_file(
    path: Path,
    *,
    trusted_root: Path = Path("/"),
    allowed_ancestor_uids: frozenset[int] | None = None,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
) -> SecureCreatedFile:
    """Open or atomically create one protected regular file below bound ancestors."""

    relative_parts = _canonical_relative(path, trusted_root)
    allowed = allowed_ancestor_uids or frozenset({0, expected_uid})
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    file_flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptors: list[int] = []
    try:
        parent = os.open(trusted_root, directory_flags)
        descriptors.append(parent)
        _validate_directory(
            os.fstat(parent),
            allowed_owner_uids=allowed,
            label="trusted root",
        )
        for component in relative_parts[:-1]:
            named = os.stat(component, dir_fd=parent, follow_symlinks=False)
            child = os.open(component, directory_flags, dir_fd=parent)
            opened = os.fstat(child)
            try:
                _validate_directory(
                    named,
                    allowed_owner_uids=allowed,
                    label="protected path",
                )
                if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
                    raise AuthorityPathSecurityError("protected path ancestor identity changed")
            except BaseException:
                os.close(child)
                raise
            descriptors.append(child)
            parent = child
        name = relative_parts[-1]
        created = False
        try:
            descriptor = os.open(name, file_flags, dir_fd=parent)
        except FileNotFoundError:
            descriptor = os.open(
                name,
                file_flags | os.O_CREAT | os.O_EXCL,
                expected_mode,
                dir_fd=parent,
            )
            created = True
        descriptors.append(descriptor)
        opened = os.fstat(descriptor)
        named = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_ISLNK(named.st_mode)
            or opened.st_nlink != 1
            or opened.st_uid != expected_uid
            or opened.st_gid != expected_gid
            or stat.S_IMODE(opened.st_mode) != expected_mode
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
        ):
            raise AuthorityPathSecurityError("protected file owner, mode, or identity is unsafe")
        rebound = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (opened.st_dev, opened.st_ino) != (rebound.st_dev, rebound.st_ino):
            raise AuthorityPathSecurityError("protected file identity changed")
        return SecureCreatedFile(
            metadata=SecurePathMetadata(
                uid=opened.st_uid,
                gid=opened.st_gid,
                mode=stat.S_IMODE(opened.st_mode),
                device=opened.st_dev,
                inode=opened.st_ino,
                size=opened.st_size,
            ),
            created=created,
        )
    except AuthorityPathSecurityError:
        raise
    except OSError as exc:
        raise AuthorityPathSecurityError("protected path is unavailable") from exc
    finally:
        for descriptor in reversed(descriptors):
            with suppress(OSError):
                os.close(descriptor)


__all__ = [
    "AuthorityPathSecurityError",
    "SecureCreatedFile",
    "SecurePathMetadata",
    "read_secure_regular_file",
    "secure_create_regular_file",
    "secure_path_metadata",
]
