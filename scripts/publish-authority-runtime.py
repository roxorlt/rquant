#!/usr/bin/env python3
"""Descriptor-bound publication of an unprivileged authority runtime payload."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import sys
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import Any

_RELEASE_SHA = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
_MAX_MANIFEST_BYTES = 16 * 1024 * 1024
_MAX_FILE_BYTES = 512 * 1024 * 1024
_MAX_FILES = 100_000
PUBLISHER_VERSION = "rquant-authority-runtime-publisher/v2"


class AuthorityRuntimeInstallError(RuntimeError):
    """The unprivileged payload cannot be safely published by root."""


def _close(descriptor: int) -> None:
    with suppress(OSError):
        os.close(descriptor)


def _identity(metadata: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def _utf8_sort_key(value: str) -> bytes:
    try:
        return value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise AuthorityRuntimeInstallError(
            "authority runtime inventory path is not valid UTF-8"
        ) from exc


def _require_directory_metadata(
    metadata: os.stat_result,
    *,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
    label: str,
) -> None:
    if stat.S_ISLNK(metadata.st_mode):
        raise AuthorityRuntimeInstallError(f"{label} is a symlink")
    if not stat.S_ISDIR(metadata.st_mode):
        raise AuthorityRuntimeInstallError(f"{label} is not a directory")
    if (
        metadata.st_uid != expected_uid
        or metadata.st_gid != expected_gid
        or stat.S_IMODE(metadata.st_mode) != expected_mode
    ):
        raise AuthorityRuntimeInstallError(f"{label} owner or mode is unsafe")


def _open_directory_path(
    path: Path,
    *,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
    label: str,
) -> int:
    try:
        named = os.stat(path, follow_symlinks=False)
        if stat.S_ISLNK(named.st_mode):
            raise AuthorityRuntimeInstallError(f"{label} is a symlink")
        descriptor = os.open(path, _DIRECTORY_FLAGS)
    except AuthorityRuntimeInstallError:
        raise
    except OSError as exc:
        raise AuthorityRuntimeInstallError(f"{label} is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        _require_directory_metadata(
            opened,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=expected_mode,
            label=label,
        )
        if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
            raise AuthorityRuntimeInstallError(f"{label} identity changed")
        rebound = os.stat(path, follow_symlinks=False)
        if (opened.st_dev, opened.st_ino) != (rebound.st_dev, rebound.st_ino):
            raise AuthorityRuntimeInstallError(f"{label} identity changed")
        return descriptor
    except BaseException:
        _close(descriptor)
        raise


def _open_directory_at(
    parent_fd: int,
    name: str,
    *,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
    label: str,
    expected_identity: tuple[int, int, int, int, int, int, int, int] | None = None,
) -> int:
    try:
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if stat.S_ISLNK(named.st_mode):
            raise AuthorityRuntimeInstallError(f"{label} is a symlink")
        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except AuthorityRuntimeInstallError:
        raise
    except OSError as exc:
        raise AuthorityRuntimeInstallError(f"{label} is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        _require_directory_metadata(
            opened,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=expected_mode,
            label=label,
        )
        if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
            raise AuthorityRuntimeInstallError(f"{label} identity changed")
        if expected_identity is not None and _identity(opened) != expected_identity:
            raise AuthorityRuntimeInstallError(f"{label} identity changed")
        rebound = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (opened.st_dev, opened.st_ino) != (rebound.st_dev, rebound.st_ino):
            raise AuthorityRuntimeInstallError(f"{label} identity changed")
        return descriptor
    except BaseException:
        _close(descriptor)
        raise


def _open_regular_at(
    parent_fd: int,
    name: str,
    *,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
    label: str,
    expected_identity: tuple[int, int, int, int, int, int, int, int] | None = None,
    writable: bool = False,
) -> int:
    try:
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if stat.S_ISLNK(named.st_mode):
            raise AuthorityRuntimeInstallError(f"{label} is a symlink")
        if not stat.S_ISREG(named.st_mode):
            raise AuthorityRuntimeInstallError(f"{label} is a special file")
        if named.st_nlink != 1:
            raise AuthorityRuntimeInstallError(f"{label} has a hardlink")
        flags = _FILE_FLAGS | (os.O_RDWR if writable else os.O_RDONLY)
        descriptor = os.open(name, flags, dir_fd=parent_fd)
    except AuthorityRuntimeInstallError:
        raise
    except OSError as exc:
        raise AuthorityRuntimeInstallError(f"{label} is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_uid != expected_uid
            or opened.st_gid != expected_gid
            or stat.S_IMODE(opened.st_mode) != expected_mode
        ):
            raise AuthorityRuntimeInstallError(f"{label} owner, mode, or type is unsafe")
        if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
            raise AuthorityRuntimeInstallError(f"{label} identity changed")
        if expected_identity is not None and _identity(opened) != expected_identity:
            raise AuthorityRuntimeInstallError(f"{label} identity changed")
        rebound = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (opened.st_dev, opened.st_ino) != (rebound.st_dev, rebound.st_ino):
            raise AuthorityRuntimeInstallError(f"{label} identity changed")
        return descriptor
    except BaseException:
        _close(descriptor)
        raise


def _read_bounded(descriptor: int, *, max_bytes: int, label: str) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - total))
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
        if total > max_bytes:
            raise AuthorityRuntimeInstallError(f"{label} is oversized")
    return b"".join(chunks)


def _require_bound_entry(
    parent_fd: int,
    name: str,
    *,
    expected_identity: tuple[int, int, int, int, int, int, int, int],
    label: str,
) -> None:
    try:
        rebound = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        raise AuthorityRuntimeInstallError(f"{label} is unavailable") from exc
    if stat.S_ISLNK(rebound.st_mode):
        raise AuthorityRuntimeInstallError(f"{label} is a symlink")
    if _identity(rebound) != expected_identity:
        raise AuthorityRuntimeInstallError(f"{label} identity changed")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise AuthorityRuntimeInstallError("authority runtime manifest has duplicate keys")
        value[key] = item
    return value


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _safe_relative_path(value: object) -> tuple[str, tuple[str, ...]]:
    if type(value) is not str or not value or len(value) > 1_024:
        raise AuthorityRuntimeInstallError("authority runtime manifest path is unsafe")
    parsed = PurePosixPath(value)
    if (
        parsed.is_absolute()
        or parsed.as_posix() != value
        or any(part in {"", ".", ".."} for part in parsed.parts)
        or any("\x00" in part or "/" in part for part in parsed.parts)
    ):
        raise AuthorityRuntimeInstallError("authority runtime manifest path is unsafe")
    return value, parsed.parts


def _validated_manifest(
    payload: bytes,
    *,
    release_sha: str,
    expected_publisher_sha256: str,
    expected_publisher_version: str,
) -> tuple[dict[str, Any], ...]:
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_unique_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                AuthorityRuntimeInstallError("authority runtime manifest is non-finite")
            ),
        )
    except AuthorityRuntimeInstallError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise AuthorityRuntimeInstallError("authority runtime manifest is malformed") from exc
    if _canonical_json(value) != payload or type(value) is not dict:
        raise AuthorityRuntimeInstallError("authority runtime manifest is not canonical")
    if set(value) != {
        "contract",
        "executable",
        "files",
        "publisher_sha256",
        "publisher_version",
        "release_sha",
        "schema_version",
    }:
        raise AuthorityRuntimeInstallError("authority runtime manifest schema is invalid")
    if (
        value["schema_version"] != 2
        or value["contract"] != "rquant-authority-runtime-manifest/v2"
        or value["release_sha"] != release_sha
        or type(value["publisher_sha256"]) is not str
        or type(value["publisher_version"]) is not str
        or value["publisher_sha256"] != expected_publisher_sha256
        or value["publisher_version"] != expected_publisher_version
        or _SHA256.fullmatch(expected_publisher_sha256) is None
        or expected_publisher_version != PUBLISHER_VERSION
        or type(value["files"]) is not list
        or not 1 <= len(value["files"]) <= _MAX_FILES
    ):
        raise AuthorityRuntimeInstallError("authority runtime manifest contract is invalid")
    executable, _parts = _safe_relative_path(value["executable"])
    entries: list[dict[str, Any]] = []
    observed: set[str] = set()
    folded: dict[str, str] = {}
    for raw in value["files"]:
        if type(raw) is not dict or set(raw) != {"mode", "path", "sha256", "size"}:
            raise AuthorityRuntimeInstallError("authority runtime manifest file schema is invalid")
        relative, parts = _safe_relative_path(raw["path"])
        if relative in observed:
            raise AuthorityRuntimeInstallError("authority runtime manifest has a duplicate path")
        observed.add(relative)
        for depth in range(1, len(parts) + 1):
            identity = "/".join(parts[:depth])
            previous = folded.setdefault(identity.casefold(), identity)
            if previous != identity:
                raise AuthorityRuntimeInstallError(
                    "authority runtime manifest has a case-conflicting path"
                )
        mode = raw["mode"]
        size = raw["size"]
        digest = raw["sha256"]
        if (
            type(mode) is not int
            or mode not in {0o444, 0o555}
            or type(size) is not int
            or not 0 <= size <= _MAX_FILE_BYTES
            or type(digest) is not str
            or _SHA256.fullmatch(digest) is None
        ):
            raise AuthorityRuntimeInstallError(
                "authority runtime manifest file contract is invalid"
            )
        entries.append(
            {"mode": mode, "path": relative, "parts": parts, "sha256": digest, "size": size}
        )
    paths = tuple(entry["path"] for entry in entries)
    if paths != tuple(sorted(paths, key=_utf8_sort_key)):
        raise AuthorityRuntimeInstallError("authority runtime manifest paths are not sorted")
    executable_entry = next((entry for entry in entries if entry["path"] == executable), None)
    if executable_entry is None or executable_entry["mode"] != 0o555:
        raise AuthorityRuntimeInstallError("authority runtime executable is invalid")
    return tuple(entries)


def _scan_payload(
    payload_fd: int,
    *,
    expected_uid: int,
    expected_gid: int,
    sealed: bool = True,
) -> dict[str, tuple[str, tuple[int, int, int, int, int, int, int, int]]]:
    root_metadata = os.fstat(payload_fd)
    if sealed:
        _require_directory_metadata(
            root_metadata,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=0o555,
            label="authority runtime payload",
        )
    elif (
        not stat.S_ISDIR(root_metadata.st_mode)
        or root_metadata.st_uid != expected_uid
        or root_metadata.st_gid != expected_gid
        or stat.S_IMODE(root_metadata.st_mode) not in {0o700, 0o755}
    ):
        raise AuthorityRuntimeInstallError(
            "authority runtime unsealed payload owner or mode is unsafe"
        )
    inventory: dict[str, tuple[str, tuple[int, int, int, int, int, int, int, int]]] = {}
    folded: dict[str, str] = {}

    def visit(directory_fd: int, prefix: tuple[str, ...]) -> None:
        try:
            names = sorted(os.listdir(directory_fd), key=_utf8_sort_key)
        except OSError as exc:
            raise AuthorityRuntimeInstallError(
                "authority runtime payload cannot be listed"
            ) from exc
        if len(names) != len(set(names)):
            raise AuthorityRuntimeInstallError("authority runtime payload has a duplicate path")
        for name in names:
            if not name or name in {".", ".."} or "/" in name or "\x00" in name:
                raise AuthorityRuntimeInstallError("authority runtime payload path is unsafe")
            relative = "/".join((*prefix, name))
            for depth in range(1, len(prefix) + 2):
                identity = "/".join((*prefix, name)[:depth])
                previous = folded.setdefault(identity.casefold(), identity)
                if previous != identity:
                    raise AuthorityRuntimeInstallError(
                        "authority runtime payload has a case-conflicting path"
                    )
            try:
                named = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            except OSError as exc:
                raise AuthorityRuntimeInstallError(
                    "authority runtime payload entry is unavailable"
                ) from exc
            if stat.S_ISLNK(named.st_mode):
                raise AuthorityRuntimeInstallError("authority runtime payload contains a symlink")
            if stat.S_ISDIR(named.st_mode):
                directory_mode = stat.S_IMODE(named.st_mode)
                if (
                    named.st_uid != expected_uid
                    or named.st_gid != expected_gid
                    or (sealed and directory_mode != 0o555)
                    or (not sealed and directory_mode not in {0o700, 0o755})
                ):
                    raise AuthorityRuntimeInstallError(
                        "authority runtime payload directory owner or mode is unsafe"
                    )
                child = _open_directory_at(
                    directory_fd,
                    name,
                    expected_uid=expected_uid,
                    expected_gid=expected_gid,
                    expected_mode=directory_mode,
                    label=f"authority runtime payload directory {relative}",
                )
                try:
                    metadata = os.fstat(child)
                    inventory[relative] = ("directory", _identity(metadata))
                    visit(child, (*prefix, name))
                finally:
                    _close(child)
                continue
            if not stat.S_ISREG(named.st_mode):
                raise AuthorityRuntimeInstallError(
                    "authority runtime payload contains a special file"
                )
            if named.st_nlink != 1:
                raise AuthorityRuntimeInstallError("authority runtime payload contains a hardlink")
            if (
                named.st_uid != expected_uid
                or named.st_gid != expected_gid
                or (sealed and stat.S_IMODE(named.st_mode) not in {0o444, 0o555})
                or (
                    not sealed
                    and (
                        stat.S_IMODE(named.st_mode) & 0o022 != 0
                        or stat.S_IMODE(named.st_mode) & 0o7000 != 0
                    )
                )
                or named.st_size > _MAX_FILE_BYTES
            ):
                raise AuthorityRuntimeInstallError(
                    "authority runtime payload file owner, mode, or size is unsafe"
                )
            inventory[relative] = ("file", _identity(named))
            if len(inventory) > _MAX_FILES * 4:
                raise AuthorityRuntimeInstallError(
                    "authority runtime payload inventory is oversized"
                )

    visit(payload_fd, ())
    return inventory


def _open_parent_chain(
    root_fd: int,
    parts: tuple[str, ...],
    *,
    inventory: dict[str, tuple[str, tuple[int, int, int, int, int, int, int, int]]],
    expected_uid: int,
    expected_gid: int,
) -> tuple[int, list[int]]:
    parent = root_fd
    opened: list[int] = []
    prefix: list[str] = []
    try:
        for component in parts:
            prefix.append(component)
            relative = "/".join(prefix)
            expected = inventory.get(relative)
            if expected is None or expected[0] != "directory":
                raise AuthorityRuntimeInstallError(
                    "authority runtime payload directory inventory changed"
                )
            child = _open_directory_at(
                parent,
                component,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                expected_mode=expected[1][4],
                expected_identity=expected[1],
                label=f"authority runtime payload directory {relative}",
            )
            opened.append(child)
            parent = child
        return parent, opened
    except BaseException:
        for descriptor in reversed(opened):
            _close(descriptor)
        raise


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise AuthorityRuntimeInstallError("authority runtime destination write failed")
        view = view[written:]


def _create_file_at(
    parent_fd: int,
    name: str,
    payload: bytes,
    *,
    mode: int,
    uid: int,
    gid: int,
) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        descriptor = os.open(name, flags, 0o600, dir_fd=parent_fd)
    except OSError as exc:
        raise AuthorityRuntimeInstallError(
            "authority runtime destination file cannot be created"
        ) from exc
    try:
        _write_all(descriptor, payload)
        os.fsync(descriptor)
        os.fchown(descriptor, uid, gid)
        os.fchmod(descriptor, mode)
    finally:
        _close(descriptor)


def _copy_file(
    source_parent_fd: int,
    destination_parent_fd: int,
    name: str,
    *,
    relative: str,
    expected: dict[str, Any],
    source_identity: tuple[int, int, int, int, int, int, int, int],
    source_uid: int,
    source_gid: int,
    published_uid: int,
    published_gid: int,
) -> None:
    source = _open_regular_at(
        source_parent_fd,
        name,
        expected_uid=source_uid,
        expected_gid=source_gid,
        expected_mode=expected["mode"],
        expected_identity=source_identity,
        label=f"authority runtime payload file {relative}",
    )
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    destination = -1
    digest = hashlib.sha256()
    size = 0
    before = os.fstat(source)
    try:
        destination = os.open(name, flags, 0o600, dir_fd=destination_parent_fd)
        while chunk := os.read(source, 64 * 1024):
            size += len(chunk)
            if size > _MAX_FILE_BYTES:
                raise AuthorityRuntimeInstallError("authority runtime payload file is oversized")
            digest.update(chunk)
            _write_all(destination, chunk)
        after = os.fstat(source)
        rebound = os.stat(name, dir_fd=source_parent_fd, follow_symlinks=False)
        if _identity(before) != _identity(after) or _identity(after) != _identity(rebound):
            raise AuthorityRuntimeInstallError(
                "authority runtime payload file identity changed while copying"
            )
        if size != expected["size"] or digest.hexdigest() != expected["sha256"]:
            raise AuthorityRuntimeInstallError("authority runtime payload file hash mismatch")
        os.fsync(destination)
        os.fchown(destination, published_uid, published_gid)
        os.fchmod(destination, expected["mode"])
    finally:
        _close(source)
        if destination >= 0:
            _close(destination)


def validate_unsealed_tree(
    *,
    root: Path,
    expected_uid: int,
    expected_gid: int,
) -> None:
    """Reject unsafe build input before a build tool can inspect its contents."""

    metadata = os.stat(root, follow_symlinks=False)
    descriptor = _open_directory_path(
        root,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        expected_mode=stat.S_IMODE(metadata.st_mode),
        label="authority runtime build input",
    )
    try:
        _scan_payload(
            descriptor,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            sealed=False,
        )
    finally:
        _close(descriptor)


def _seal_file(
    parent_fd: int,
    name: str,
    *,
    relative: str,
    source_identity: tuple[int, int, int, int, int, int, int, int],
    expected_uid: int,
    expected_gid: int,
    build_root_bytes: bytes,
    runtime_release_bytes: bytes,
) -> dict[str, Any]:
    original_mode = source_identity[4]
    readable = _open_regular_at(
        parent_fd,
        name,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        expected_mode=original_mode,
        expected_identity=source_identity,
        label=f"authority runtime unsealed payload file {relative}",
    )
    try:
        os.fchmod(readable, original_mode | 0o200)
        writable_identity = _identity(os.fstat(readable))
    finally:
        _close(readable)
    descriptor = _open_regular_at(
        parent_fd,
        name,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        expected_mode=original_mode | 0o200,
        expected_identity=writable_identity,
        label=f"authority runtime unsealed payload file {relative}",
        writable=True,
    )
    try:
        body = _read_bounded(
            descriptor,
            max_bytes=_MAX_FILE_BYTES,
            label=f"authority runtime unsealed payload file {relative}",
        )
        rewritten = body.replace(build_root_bytes, runtime_release_bytes)
        if rewritten != body:
            os.lseek(descriptor, 0, os.SEEK_SET)
            os.ftruncate(descriptor, 0)
            _write_all(descriptor, rewritten)
        mode = 0o555 if original_mode & 0o111 else 0o444
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
        sealed = os.fstat(descriptor)
        rebound = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            (sealed.st_dev, sealed.st_ino) != (source_identity[0], source_identity[1])
            or _identity(sealed) != _identity(rebound)
            or sealed.st_nlink != 1
            or sealed.st_uid != expected_uid
            or sealed.st_gid != expected_gid
            or stat.S_IMODE(sealed.st_mode) != mode
        ):
            raise AuthorityRuntimeInstallError(
                "authority runtime payload file changed while sealing"
            )
        return {
            "mode": mode,
            "path": relative,
            "sha256": hashlib.sha256(rewritten).hexdigest(),
            "size": len(rewritten),
        }
    finally:
        _close(descriptor)


def _sealed_manifest_entries(
    payload_fd: int,
    *,
    inventory: dict[str, tuple[str, tuple[int, int, int, int, int, int, int, int]]],
    expected_uid: int,
    expected_gid: int,
) -> tuple[dict[str, Any], ...]:
    entries: list[dict[str, Any]] = []
    for relative in sorted(inventory, key=_utf8_sort_key):
        kind, identity = inventory[relative]
        if kind != "file":
            continue
        parts = tuple(relative.split("/"))
        parent, opened = _open_parent_chain(
            payload_fd,
            parts[:-1],
            inventory=inventory,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
        )
        descriptor = -1
        try:
            descriptor = _open_regular_at(
                parent,
                parts[-1],
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                expected_mode=identity[4],
                expected_identity=identity,
                label=f"authority runtime sealed payload file {relative}",
            )
            body = _read_bounded(
                descriptor,
                max_bytes=_MAX_FILE_BYTES,
                label=f"authority runtime sealed payload file {relative}",
            )
            if _identity(os.fstat(descriptor)) != identity:
                raise AuthorityRuntimeInstallError(
                    "authority runtime sealed payload changed while inventorying"
                )
            entries.append(
                {
                    "mode": identity[4],
                    "path": relative,
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "size": len(body),
                }
            )
        finally:
            _close(descriptor)
            for opened_descriptor in reversed(opened):
                _close(opened_descriptor)
    return tuple(entries)


def seal_authority_runtime_candidate(
    *,
    candidate_root: Path,
    release_sha: str,
    build_root_bytes: bytes,
    runtime_release_bytes: bytes,
    expected_source_uid: int,
    expected_source_gid: int,
    publisher_sha256: str,
    publisher_version: str,
) -> Path:
    """Seal a closed payload as its ordinary, nonprivileged build owner."""

    if os.geteuid() == 0:
        raise AuthorityRuntimeInstallError(
            "authority runtime payload preparation must not run as root"
        )
    if (
        _RELEASE_SHA.fullmatch(release_sha) is None
        or _SHA256.fullmatch(publisher_sha256) is None
        or publisher_version != PUBLISHER_VERSION
        or not build_root_bytes
        or not runtime_release_bytes
    ):
        raise AuthorityRuntimeInstallError("authority runtime preparation contract is invalid")
    candidate_fd = _open_directory_path(
        candidate_root,
        expected_uid=expected_source_uid,
        expected_gid=expected_source_gid,
        expected_mode=0o700,
        label="authority runtime candidate",
    )
    payload_fd = -1
    try:
        if tuple(sorted(os.listdir(candidate_fd), key=_utf8_sort_key)) != ("payload",):
            raise AuthorityRuntimeInstallError(
                "authority runtime unsealed candidate inventory is not closed"
            )
        payload_metadata = os.stat("payload", dir_fd=candidate_fd, follow_symlinks=False)
        if stat.S_ISLNK(payload_metadata.st_mode):
            raise AuthorityRuntimeInstallError("authority runtime candidate payload is a symlink")
        payload_fd = _open_directory_at(
            candidate_fd,
            "payload",
            expected_uid=expected_source_uid,
            expected_gid=expected_source_gid,
            expected_mode=stat.S_IMODE(payload_metadata.st_mode),
            label="authority runtime unsealed payload",
        )
        inventory = _scan_payload(
            payload_fd,
            expected_uid=expected_source_uid,
            expected_gid=expected_source_gid,
            sealed=False,
        )
        for relative in sorted(inventory, key=_utf8_sort_key):
            kind, identity = inventory[relative]
            if kind != "file":
                continue
            parts = tuple(relative.split("/"))
            parent, opened = _open_parent_chain(
                payload_fd,
                parts[:-1],
                inventory=inventory,
                expected_uid=expected_source_uid,
                expected_gid=expected_source_gid,
            )
            try:
                _seal_file(
                    parent,
                    parts[-1],
                    relative=relative,
                    source_identity=identity,
                    expected_uid=expected_source_uid,
                    expected_gid=expected_source_gid,
                    build_root_bytes=build_root_bytes,
                    runtime_release_bytes=runtime_release_bytes,
                )
            finally:
                for descriptor in reversed(opened):
                    _close(descriptor)
        directories = [
            relative
            for relative, (kind, _identity_value) in inventory.items()
            if kind == "directory"
        ]
        for relative in sorted(
            directories,
            key=lambda value: (-value.count("/"), _utf8_sort_key(value)),
        ):
            parts = tuple(relative.split("/"))
            directory, opened = _open_parent_chain(
                payload_fd,
                parts,
                inventory=inventory,
                expected_uid=expected_source_uid,
                expected_gid=expected_source_gid,
            )
            try:
                os.fchmod(directory, 0o555)
            finally:
                for descriptor in reversed(opened):
                    _close(descriptor)
        os.fchmod(payload_fd, 0o555)
        sealed_inventory = _scan_payload(
            payload_fd,
            expected_uid=expected_source_uid,
            expected_gid=expected_source_gid,
        )
        entries = _sealed_manifest_entries(
            payload_fd,
            inventory=sealed_inventory,
            expected_uid=expected_source_uid,
            expected_gid=expected_source_gid,
        )
        executable = "venv/bin/rquant"
        executable_entry = next(
            (entry for entry in entries if entry["path"] == executable),
            None,
        )
        if executable_entry is None or executable_entry["mode"] != 0o555:
            raise AuthorityRuntimeInstallError("authority runtime executable is invalid")
        manifest = _canonical_json(
            {
                "contract": "rquant-authority-runtime-manifest/v2",
                "executable": executable,
                "files": entries,
                "publisher_sha256": publisher_sha256,
                "publisher_version": publisher_version,
                "release_sha": release_sha,
                "schema_version": 2,
            }
        )
        _create_file_at(
            candidate_fd,
            "manifest.json",
            manifest,
            mode=0o444,
            uid=expected_source_uid,
            gid=expected_source_gid,
        )
        os.fchmod(candidate_fd, 0o500)
        os.fsync(candidate_fd)
        return candidate_root / "manifest.json"
    except AuthorityRuntimeInstallError:
        raise
    except (OSError, UnicodeError, ValueError) as exc:
        raise AuthorityRuntimeInstallError(
            "authority runtime payload preparation failed closed"
        ) from exc
    finally:
        _close(payload_fd)
        _close(candidate_fd)


def _open_private_key(
    path: Path,
    *,
    expected_uid: int,
    expected_gid: int,
) -> int:
    try:
        named = os.stat(path, follow_symlinks=False)
        if stat.S_ISLNK(named.st_mode):
            raise AuthorityRuntimeInstallError("authority runtime signing key is a symlink")
        descriptor = os.open(path, _FILE_FLAGS)
    except AuthorityRuntimeInstallError:
        raise
    except OSError as exc:
        raise AuthorityRuntimeInstallError("authority runtime signing key is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_uid != expected_uid
            or opened.st_gid != expected_gid
            or stat.S_IMODE(opened.st_mode) != 0o400
            or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise AuthorityRuntimeInstallError("authority runtime signing key metadata is unsafe")
        return descriptor
    except BaseException:
        _close(descriptor)
        raise


def _sign_manifest(
    *,
    stage_path: Path,
    key_fd: int,
    published_uid: int,
    published_gid: int,
) -> None:
    openssl = shutil.which("openssl")
    if openssl is None:
        raise AuthorityRuntimeInstallError("openssl is required to sign authority runtime")
    descriptor_root = Path("/proc/self/fd")
    if not descriptor_root.is_dir():
        descriptor_root = Path("/dev/fd")
    signature = stage_path / "manifest.sig"
    completed = subprocess.run(
        (
            openssl,
            "pkeyutl",
            "-sign",
            "-rawin",
            "-inkey",
            str(descriptor_root / str(key_fd)),
            "-in",
            str(stage_path / "manifest.json"),
            "-out",
            str(signature),
        ),
        check=False,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        pass_fds=(key_fd,),
        timeout=10,
    )
    if completed.returncode != 0:
        raise AuthorityRuntimeInstallError("authority runtime manifest signing failed")
    metadata = os.stat(signature, follow_symlinks=False)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise AuthorityRuntimeInstallError("authority runtime signature output is unsafe")
    os.chown(signature, published_uid, published_gid, follow_symlinks=False)
    os.chmod(signature, 0o444, follow_symlinks=False)


def publish_authority_runtime(
    *,
    candidate_root: Path,
    generations_root: Path,
    release_sha: str,
    signing_private_key_path: Path,
    expected_source_uid: int,
    expected_source_gid: int,
    expected_publisher_sha256: str,
    expected_publisher_version: str,
    published_uid: int = 0,
    published_gid: int = 0,
) -> Path:
    """Verify, copy, sign, and atomically publish one closed runtime generation."""

    if _RELEASE_SHA.fullmatch(release_sha) is None:
        raise AuthorityRuntimeInstallError("authority runtime release SHA is invalid")
    candidate_fd = _open_directory_path(
        candidate_root,
        expected_uid=expected_source_uid,
        expected_gid=expected_source_gid,
        expected_mode=0o500,
        label="authority runtime candidate",
    )
    generations_fd = _open_directory_path(
        generations_root,
        expected_uid=published_uid,
        expected_gid=published_gid,
        expected_mode=0o755,
        label="authority runtime generations root",
    )
    payload_fd = -1
    manifest_fd = -1
    key_fd = -1
    lock_fd = -1
    stage_fd = -1
    stage_name = f".stage-{release_sha}-{secrets.token_hex(8)}"
    stage_path = generations_root / stage_name
    try:
        candidate_names = tuple(sorted(os.listdir(candidate_fd)))
        if candidate_names != ("manifest.json", "payload"):
            raise AuthorityRuntimeInstallError(
                "authority runtime candidate inventory is not closed"
            )
        manifest_fd = _open_regular_at(
            candidate_fd,
            "manifest.json",
            expected_uid=expected_source_uid,
            expected_gid=expected_source_gid,
            expected_mode=0o444,
            label="authority runtime candidate manifest",
        )
        manifest_before = os.fstat(manifest_fd)
        manifest_bytes = _read_bounded(
            manifest_fd,
            max_bytes=_MAX_MANIFEST_BYTES,
            label="authority runtime candidate manifest",
        )
        manifest_after = os.fstat(manifest_fd)
        if _identity(manifest_before) != _identity(manifest_after):
            raise AuthorityRuntimeInstallError("authority runtime candidate manifest changed")
        entries = _validated_manifest(
            manifest_bytes,
            release_sha=release_sha,
            expected_publisher_sha256=expected_publisher_sha256,
            expected_publisher_version=expected_publisher_version,
        )
        payload_fd = _open_directory_at(
            candidate_fd,
            "payload",
            expected_uid=expected_source_uid,
            expected_gid=expected_source_gid,
            expected_mode=0o555,
            label="authority runtime candidate payload",
        )
        payload_before = _identity(os.fstat(payload_fd))
        inventory = _scan_payload(
            payload_fd,
            expected_uid=expected_source_uid,
            expected_gid=expected_source_gid,
        )
        expected_files = {entry["path"] for entry in entries}
        expected_directories = {
            "/".join(entry["parts"][:depth])
            for entry in entries
            for depth in range(1, len(entry["parts"]))
        }
        observed_files = {path for path, item in inventory.items() if item[0] == "file"}
        observed_directories = {path for path, item in inventory.items() if item[0] == "directory"}
        if observed_files != expected_files or observed_directories != expected_directories:
            raise AuthorityRuntimeInstallError("authority runtime payload inventory mismatch")

        lock_fd = os.open(
            ".publish.lock",
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=generations_fd,
        )
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            os.stat(release_sha, dir_fd=generations_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise AuthorityRuntimeInstallError(
                "authority runtime generation already exists and is immutable"
            )
        os.mkdir(stage_name, 0o700, dir_fd=generations_fd)
        stage_fd = _open_directory_at(
            generations_fd,
            stage_name,
            expected_uid=published_uid,
            expected_gid=published_gid,
            expected_mode=0o700,
            label="authority runtime publication stage",
        )
        os.mkdir("payload", 0o700, dir_fd=stage_fd)
        destination_payload_fd = _open_directory_at(
            stage_fd,
            "payload",
            expected_uid=published_uid,
            expected_gid=published_gid,
            expected_mode=0o700,
            label="authority runtime destination payload",
        )
        try:
            for relative in sorted(
                expected_directories, key=lambda value: (value.count("/"), value)
            ):
                parts = tuple(relative.split("/"))
                parent, opened = _open_parent_chain_destination(
                    destination_payload_fd,
                    parts[:-1],
                    expected_uid=published_uid,
                    expected_gid=published_gid,
                )
                try:
                    os.mkdir(parts[-1], 0o700, dir_fd=parent)
                finally:
                    for descriptor in reversed(opened):
                        _close(descriptor)
            for entry in entries:
                source_parent, source_opened = _open_parent_chain(
                    payload_fd,
                    entry["parts"][:-1],
                    inventory=inventory,
                    expected_uid=expected_source_uid,
                    expected_gid=expected_source_gid,
                )
                destination_parent, destination_opened = _open_parent_chain_destination(
                    destination_payload_fd,
                    entry["parts"][:-1],
                    expected_uid=published_uid,
                    expected_gid=published_gid,
                )
                try:
                    source_entry = inventory[entry["path"]]
                    _copy_file(
                        source_parent,
                        destination_parent,
                        entry["parts"][-1],
                        relative=entry["path"],
                        expected=entry,
                        source_identity=source_entry[1],
                        source_uid=expected_source_uid,
                        source_gid=expected_source_gid,
                        published_uid=published_uid,
                        published_gid=published_gid,
                    )
                finally:
                    for descriptor in reversed(source_opened):
                        _close(descriptor)
                    for descriptor in reversed(destination_opened):
                        _close(descriptor)
            for relative in sorted(
                expected_directories, key=lambda value: value.count("/"), reverse=True
            ):
                parts = tuple(relative.split("/"))
                parent, opened = _open_parent_chain_destination(
                    destination_payload_fd,
                    parts,
                    expected_uid=published_uid,
                    expected_gid=published_gid,
                )
                try:
                    os.fchmod(parent, 0o555)
                    os.fchown(parent, published_uid, published_gid)
                finally:
                    for descriptor in reversed(opened):
                        _close(descriptor)
            os.fchmod(destination_payload_fd, 0o555)
            os.fchown(destination_payload_fd, published_uid, published_gid)
            os.fsync(destination_payload_fd)
        finally:
            _close(destination_payload_fd)

        rebound_inventory = _scan_payload(
            payload_fd,
            expected_uid=expected_source_uid,
            expected_gid=expected_source_gid,
        )
        if rebound_inventory != inventory:
            raise AuthorityRuntimeInstallError("authority runtime payload changed while publishing")
        rebound_candidate_names = tuple(sorted(os.listdir(candidate_fd)))
        if rebound_candidate_names != candidate_names:
            raise AuthorityRuntimeInstallError(
                "authority runtime candidate inventory changed while publishing"
            )
        _require_bound_entry(
            candidate_fd,
            "payload",
            expected_identity=payload_before,
            label="authority runtime candidate payload",
        )
        os.lseek(manifest_fd, 0, os.SEEK_SET)
        rebound_manifest = _read_bounded(
            manifest_fd,
            max_bytes=_MAX_MANIFEST_BYTES,
            label="authority runtime candidate manifest",
        )
        if rebound_manifest != manifest_bytes or _identity(os.fstat(manifest_fd)) != _identity(
            manifest_before
        ):
            raise AuthorityRuntimeInstallError("authority runtime candidate manifest changed")
        _require_bound_entry(
            candidate_fd,
            "manifest.json",
            expected_identity=_identity(manifest_before),
            label="authority runtime candidate manifest",
        )

        _create_file_at(
            stage_fd,
            "manifest.json",
            manifest_bytes,
            mode=0o444,
            uid=published_uid,
            gid=published_gid,
        )
        _create_file_at(
            stage_fd,
            "manifest.sha256",
            f"{hashlib.sha256(manifest_bytes).hexdigest()}\n".encode("ascii"),
            mode=0o444,
            uid=published_uid,
            gid=published_gid,
        )
        key_fd = _open_private_key(
            signing_private_key_path,
            expected_uid=published_uid,
            expected_gid=published_gid,
        )
        _sign_manifest(
            stage_path=stage_path,
            key_fd=key_fd,
            published_uid=published_uid,
            published_gid=published_gid,
        )
        # The generation is complete but not selected by ``current``.  macOS
        # refuses to rename a 0555 directory, so keep the root-owned stage
        # owner-writable through the atomic rename and tighten the still-bound
        # inode immediately afterwards.
        os.fchmod(stage_fd, 0o755)
        os.fchown(stage_fd, published_uid, published_gid)
        os.fsync(stage_fd)
        os.rename(
            stage_name,
            release_sha,
            src_dir_fd=generations_fd,
            dst_dir_fd=generations_fd,
        )
        os.fchmod(stage_fd, 0o555)
        os.fsync(stage_fd)
        os.fsync(generations_fd)
        return generations_root / release_sha
    except AuthorityRuntimeInstallError:
        raise
    except (OSError, subprocess.SubprocessError, UnicodeError, ValueError) as exc:
        raise AuthorityRuntimeInstallError("authority runtime publication failed closed") from exc
    finally:
        for descriptor in (
            stage_fd,
            lock_fd,
            key_fd,
            manifest_fd,
            payload_fd,
            generations_fd,
            candidate_fd,
        ):
            if descriptor >= 0:
                _close(descriptor)
        if stage_path.exists():
            shutil.rmtree(stage_path, ignore_errors=True)


def _open_parent_chain_destination(
    root_fd: int,
    parts: tuple[str, ...],
    *,
    expected_uid: int,
    expected_gid: int,
) -> tuple[int, list[int]]:
    parent = root_fd
    opened: list[int] = []
    try:
        for component in parts:
            child = _open_directory_at(
                parent,
                component,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                expected_mode=0o700,
                label="authority runtime destination directory",
            )
            opened.append(child)
            parent = child
        return parent, opened
    except BaseException:
        for descriptor in reversed(opened):
            _close(descriptor)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--operation",
        choices=("validate-build-input", "prepare-candidate", "publish"),
        default="publish",
    )
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--tree-root", type=Path)
    parser.add_argument("--generations-root", type=Path)
    parser.add_argument("--release-sha")
    parser.add_argument("--signing-private-key", type=Path)
    parser.add_argument("--source-uid", type=int)
    parser.add_argument("--source-gid", type=int)
    parser.add_argument("--publisher-sha256")
    parser.add_argument("--publisher-version")
    parser.add_argument("--build-root-bytes")
    parser.add_argument("--runtime-release-bytes")
    parser.add_argument("--published-uid", type=int, default=0)
    parser.add_argument("--published-gid", type=int, default=0)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.source_uid is None or arguments.source_gid is None:
            raise AuthorityRuntimeInstallError("authority runtime source identity is required")
        if arguments.operation == "validate-build-input":
            if arguments.tree_root is None:
                raise AuthorityRuntimeInstallError("authority runtime build input is required")
            validate_unsealed_tree(
                root=arguments.tree_root,
                expected_uid=arguments.source_uid,
                expected_gid=arguments.source_gid,
            )
            print(arguments.tree_root)
            return 0
        if (
            arguments.candidate_root is None
            or arguments.release_sha is None
            or arguments.publisher_sha256 is None
            or arguments.publisher_version is None
        ):
            raise AuthorityRuntimeInstallError("authority runtime operation contract is incomplete")
        if arguments.operation == "prepare-candidate":
            if arguments.build_root_bytes is None or arguments.runtime_release_bytes is None:
                raise AuthorityRuntimeInstallError(
                    "authority runtime preparation paths are required"
                )
            manifest = seal_authority_runtime_candidate(
                candidate_root=arguments.candidate_root,
                release_sha=arguments.release_sha,
                build_root_bytes=os.fsencode(arguments.build_root_bytes),
                runtime_release_bytes=os.fsencode(arguments.runtime_release_bytes),
                expected_source_uid=arguments.source_uid,
                expected_source_gid=arguments.source_gid,
                publisher_sha256=arguments.publisher_sha256,
                publisher_version=arguments.publisher_version,
            )
            print(manifest)
            return 0
        if arguments.generations_root is None or arguments.signing_private_key is None:
            raise AuthorityRuntimeInstallError("authority runtime publication paths are required")
        release = publish_authority_runtime(
            candidate_root=arguments.candidate_root,
            generations_root=arguments.generations_root,
            release_sha=arguments.release_sha,
            signing_private_key_path=arguments.signing_private_key,
            expected_source_uid=arguments.source_uid,
            expected_source_gid=arguments.source_gid,
            expected_publisher_sha256=arguments.publisher_sha256,
            expected_publisher_version=arguments.publisher_version,
            published_uid=arguments.published_uid,
            published_gid=arguments.published_gid,
        )
    except AuthorityRuntimeInstallError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(release)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
