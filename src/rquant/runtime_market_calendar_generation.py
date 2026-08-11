"""Content-addressed installation for immutable runtime market calendars."""

from __future__ import annotations

import os
import re
import stat
from contextlib import suppress
from pathlib import Path

from rquant.runtime_market_session import load_market_calendar_authority
from rquant.strict_json import canonical_json_bytes

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_MAX_BYTES = 4 * 1024 * 1024


def _normalized_absolute(path: Path, *, label: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError(f"{label} must be absolute and normalized")
    return candidate


def market_calendar_generation_path(runtime_root: Path, content_sha256: str) -> Path:
    root = _normalized_absolute(runtime_root, label="runtime root")
    if _SHA256.fullmatch(content_sha256) is None:
        raise ValueError("market calendar content sha256 is invalid")
    return root / "authorities" / "market-calendar" / "generations" / f"{content_sha256}.json"


def _open_existing_directory(path: Path) -> int:
    descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
    try:
        for component in path.parts[1:]:
            observed = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                raise ValueError("runtime calendar path contains an unsafe symlink or component")
            child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            opened = os.fstat(child)
            if (observed.st_dev, observed.st_ino, observed.st_mode) != (
                opened.st_dev,
                opened.st_ino,
                opened.st_mode,
            ):
                os.close(child)
                raise ValueError("runtime calendar directory changed while opening")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _prepare_generation_directory(runtime_root: Path) -> int:
    root = _normalized_absolute(runtime_root, label="runtime root")
    parent = _open_existing_directory(root.parent)
    descriptor = -1
    try:
        with suppress(FileExistsError):
            os.mkdir(root.name, mode=0o700, dir_fd=parent)
        observed = os.stat(root.name, dir_fd=parent, follow_symlinks=False)
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise ValueError("runtime root is unsafe or contains a symlink")
        descriptor = os.open(root.name, _DIRECTORY_FLAGS, dir_fd=parent)
        if os.fstat(descriptor).st_uid != os.geteuid():
            raise ValueError("runtime root owner is unsafe")
        for component in ("authorities", "market-calendar", "generations"):
            with suppress(FileExistsError):
                os.mkdir(component, mode=0o700, dir_fd=descriptor)
            before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if (
                stat.S_ISLNK(before.st_mode)
                or not stat.S_ISDIR(before.st_mode)
                or before.st_uid != os.geteuid()
            ):
                raise ValueError("runtime calendar directory is unsafe")
            child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            after = os.fstat(child)
            if (before.st_dev, before.st_ino, before.st_mode) != (
                after.st_dev,
                after.st_ino,
                after.st_mode,
            ):
                os.close(child)
                raise ValueError("runtime calendar directory changed while opening")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        raise
    finally:
        os.close(parent)


def _read_generation(directory: int, name: str) -> tuple[bytes, os.stat_result]:
    before = os.stat(name, dir_fd=directory, follow_symlinks=False)
    if (
        stat.S_ISLNK(before.st_mode)
        or not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_size <= 0
        or before.st_size > _MAX_BYTES
    ):
        raise ValueError("immutable market calendar generation is unsafe")
    descriptor = os.open(name, _FILE_FLAGS, dir_fd=directory)
    try:
        opened = os.fstat(descriptor)
        if (before.st_dev, before.st_ino, before.st_size) != (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
        ):
            raise ValueError("immutable market calendar generation changed while opening")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        if remaining or (opened.st_dev, opened.st_ino, opened.st_size) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
        ):
            raise ValueError("immutable market calendar generation changed while reading")
        return b"".join(chunks), after
    finally:
        os.close(descriptor)


def _recover_or_reject_existing(
    directory: int,
    *,
    target_name: str,
    stage_name: str,
    payload: bytes,
) -> bool:
    try:
        target_payload, target_stat = _read_generation(directory, target_name)
    except FileNotFoundError:
        return False
    if target_payload != payload:
        raise ValueError("immutable market calendar content conflicts with its identity")
    if target_stat.st_nlink == 1:
        return True
    if target_stat.st_nlink != 2:
        raise ValueError("immutable market calendar generation has unsafe link count")
    try:
        stage_payload, stage_stat = _read_generation(directory, stage_name)
    except FileNotFoundError as exc:
        raise ValueError("immutable market calendar generation has an unknown extra link") from exc
    if stage_payload != payload or (stage_stat.st_dev, stage_stat.st_ino) != (
        target_stat.st_dev,
        target_stat.st_ino,
    ):
        raise ValueError("immutable market calendar recovery stage is unsafe")
    os.unlink(stage_name, dir_fd=directory)
    os.fsync(directory)
    _, recovered = _read_generation(directory, target_name)
    if recovered.st_nlink != 1:
        raise ValueError("immutable market calendar recovery did not converge")
    return True


def install_market_calendar_generation(
    source_path: Path,
    *,
    runtime_root: Path,
    expected_commit: str,
    expected_content_sha256: str,
) -> Path:
    target = market_calendar_generation_path(runtime_root, expected_content_sha256)
    authority = load_market_calendar_authority(
        _normalized_absolute(source_path, label="market calendar source"),
        expected_commit=expected_commit,
    )
    if authority.content_sha256 != expected_content_sha256:
        raise ValueError("market calendar content identity does not match expected content sha256")
    payload = canonical_json_bytes(authority.model_dump(mode="json"))
    directory = _prepare_generation_directory(Path(runtime_root))
    target_name = target.name
    stage_name = f".{expected_content_sha256}.stage"
    try:
        if _recover_or_reject_existing(
            directory,
            target_name=target_name,
            stage_name=stage_name,
            payload=payload,
        ):
            return target
        try:
            stage_payload, stage_stat = _read_generation(directory, stage_name)
        except FileNotFoundError:
            stage_stat = None
        else:
            if stage_stat.st_nlink != 1:
                raise ValueError("immutable market calendar recovery stage conflicts")
            if stage_payload != payload:
                os.unlink(stage_name, dir_fd=directory)
                os.fsync(directory)
                stage_stat = None
        if stage_stat is None:
            stage_descriptor = os.open(
                stage_name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory,
            )
            try:
                os.fchmod(stage_descriptor, 0o600)
                view = memoryview(payload)
                while view:
                    written = os.write(stage_descriptor, view)
                    if written <= 0:
                        raise OSError("market calendar stage write made no progress")
                    view = view[written:]
                os.fsync(stage_descriptor)
            finally:
                os.close(stage_descriptor)
        try:
            os.link(
                stage_name,
                target_name,
                src_dir_fd=directory,
                dst_dir_fd=directory,
                follow_symlinks=False,
            )
        except FileExistsError:
            if not _recover_or_reject_existing(
                directory,
                target_name=target_name,
                stage_name=stage_name,
                payload=payload,
            ):
                raise
            return target
        os.fsync(directory)
        os.unlink(stage_name, dir_fd=directory)
        os.fsync(directory)
        installed_payload, installed = _read_generation(directory, target_name)
        if installed_payload != payload or installed.st_nlink != 1:
            raise ValueError("immutable market calendar installation did not verify")
        return target
    finally:
        os.close(directory)


__all__ = [
    "install_market_calendar_generation",
    "market_calendar_generation_path",
]
