"""Crash-persistent authority for one deployable checkout generation.

This module intentionally uses only the Python standard library so startup wrappers can
load it by physical file path before importing the :mod:`rquant` package.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
import subprocess
import tomllib
from collections.abc import Callable
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MARKER_SCHEMA_VERSION = 1
MAX_MARKER_BYTES = 32 * 1024


class ReleaseGenerationError(RuntimeError):
    """The release generation cannot be trusted."""


@dataclass(frozen=True)
class PathIdentity:
    device: int
    inode: int
    mode: int
    owner: int
    links: int

    @classmethod
    def capture(cls, value: os.stat_result) -> PathIdentity:
        return cls(value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_nlink)


@dataclass(frozen=True)
class ReleaseGenerationMarker:
    schema_version: int
    commit: str
    uv_lock_sha256: str
    pyproject_sha256: str
    package_version: str
    python_version: str
    python_abi: str
    venv_path: str
    venv_identity: PathIdentity
    pyvenv_cfg_sha256: str
    python_path: str
    python_identity: PathIdentity
    site_packages_path: str
    site_packages_identity: PathIdentity
    published_at: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> ReleaseGenerationMarker:
        try:
            return cls(
                schema_version=int(payload["schema_version"]),
                commit=str(payload["commit"]),
                uv_lock_sha256=str(payload["uv_lock_sha256"]),
                pyproject_sha256=str(payload["pyproject_sha256"]),
                package_version=str(payload["package_version"]),
                python_version=str(payload["python_version"]),
                python_abi=str(payload["python_abi"]),
                venv_path=str(payload["venv_path"]),
                venv_identity=PathIdentity(**payload["venv_identity"]),
                pyvenv_cfg_sha256=str(payload["pyvenv_cfg_sha256"]),
                python_path=str(payload["python_path"]),
                python_identity=PathIdentity(**payload["python_identity"]),
                site_packages_path=str(payload["site_packages_path"]),
                site_packages_identity=PathIdentity(**payload["site_packages_identity"]),
                published_at=str(payload["published_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ReleaseGenerationError("release generation marker is malformed") from exc


def marker_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.complete.json")


def _canonical(path: Path, *, label: str) -> Path:
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise ReleaseGenerationError(f"{label} must be an absolute canonical path")
    return path


def _identity(path: Path, *, label: str, directory: bool) -> PathIdentity:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise ReleaseGenerationError(f"{label} is unavailable") from exc
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    if (
        not expected_type(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or (not directory and observed.st_nlink != 1)
        or observed.st_mode & 0o022
        or path.resolve(strict=True) != path
    ):
        raise ReleaseGenerationError(f"{label} has unsafe identity")
    return PathIdentity.capture(observed)


def _private_lock_root(path: Path) -> tuple[int, PathIdentity]:
    identity = _identity(path, label="deployment authority root", directory=True)
    if stat.S_IMODE(identity.mode) != 0o700:
        raise ReleaseGenerationError("deployment authority root must have mode 0700")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    opened = PathIdentity.capture(os.fstat(descriptor))
    if _object_key(opened) != _object_key(identity):
        os.close(descriptor)
        raise ReleaseGenerationError("deployment authority root identity changed")
    return descriptor, identity


def _object_key(identity: PathIdentity) -> tuple[int, int, int, int]:
    return identity.device, identity.inode, identity.mode, identity.owner


def _hash_file(path: Path, *, label: str) -> str:
    _identity(path, label=label, directory=False)
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise ReleaseGenerationError(f"{label} cannot be read") from exc
    return digest.hexdigest()


def _git_output(repo: Path, git_path: Path, *arguments: str) -> str:
    try:
        result = subprocess.run(
            [str(git_path), *arguments],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseGenerationError("release generation Git verification failed") from exc
    return result.stdout.strip()


def _assert_tracked_clean(repo: Path, git_path: Path) -> None:
    try:
        status = subprocess.run(
            [str(git_path), "status", "--porcelain=v1", "--untracked-files=no"],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        diff = subprocess.run(
            [str(git_path), "diff-index", "--quiet", "HEAD", "--"],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseGenerationError("tracked checkout verification failed") from exc
    if status.stdout or diff.returncode != 0:
        raise ReleaseGenerationError("tracked checkout is dirty")


def _python_facts(python_path: Path) -> tuple[str, str]:
    program = (
        "import json,sys,sysconfig;"
        "print(json.dumps({'version': '.'.join(map(str, sys.version_info[:3])),"
        "'cache_tag': sys.implementation.cache_tag or '',"
        "'soabi': sysconfig.get_config_var('SOABI') or ''}, sort_keys=True))"
    )
    try:
        result = subprocess.run(
            [str(python_path), "-I", "-S", "-c", program],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        payload = json.loads(result.stdout)
        version = str(payload["version"])
        abi = f"{payload['cache_tag']}:{payload['soabi']}"
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, KeyError) as exc:
        raise ReleaseGenerationError("release Python ABI cannot be verified") from exc
    if not version or abi == ":":
        raise ReleaseGenerationError("release Python ABI is incomplete")
    return version, abi


class ReleaseGenerationAuthority:
    def __init__(
        self,
        *,
        repo: Path,
        lock_path: Path,
        lock_fd: int,
        python_path: Path,
        git_path: Path,
        writable: bool = False,
        mutation_hook: Callable[[str], None] | None = None,
    ) -> None:
        self.repo = _canonical(repo, label="release checkout")
        self.lock_path = _canonical(lock_path, label="deployment lock")
        self.marker_path = marker_path_for_lock(self.lock_path)
        self.lock_fd = lock_fd
        self.python_path = _canonical(python_path, label="release Python")
        self.git_path = _canonical(git_path, label="trusted Git")
        self.writable = writable
        self._mutation_hook = mutation_hook or (lambda _stage: None)
        self._assert_lock()

    def _assert_lock(self) -> None:
        try:
            opened = os.fstat(self.lock_fd)
            active = self.lock_path.lstat()
        except OSError as exc:
            raise ReleaseGenerationError("deployment generation lock is unavailable") from exc
        if (
            PathIdentity.capture(opened) != PathIdentity.capture(active)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise ReleaseGenerationError("deployment generation lock identity changed")

    def _facts(self, *, expected_commit: str) -> ReleaseGenerationMarker:
        if len(expected_commit) != 40 or any(c not in "0123456789abcdef" for c in expected_commit):
            raise ReleaseGenerationError("release commit must be a lowercase full SHA")
        commit = _git_output(self.repo, self.git_path, "rev-parse", "--verify", "HEAD^{commit}")
        if commit != expected_commit:
            raise ReleaseGenerationError("release checkout commit does not match marker")
        uv_lock = self.repo / "uv.lock"
        pyproject = self.repo / "pyproject.toml"
        uv_hash = _hash_file(uv_lock, label="uv.lock")
        pyproject_hash = _hash_file(pyproject, label="pyproject.toml")
        try:
            package_version = str(
                tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]["version"]
            )
        except (OSError, KeyError, tomllib.TOMLDecodeError) as exc:
            raise ReleaseGenerationError("package version cannot be verified") from exc
        venv = self.repo / ".venv"
        venv_identity = _identity(venv, label="release venv", directory=True)
        python_identity = _identity(
            self.python_path,
            label="release venv Python",
            directory=False,
        )
        if not self.python_path.is_relative_to(venv):
            raise ReleaseGenerationError("release Python is outside the venv")
        version, abi = _python_facts(self.python_path)
        major_minor = ".".join(version.split(".")[:2])
        site_packages = venv / "lib" / f"python{major_minor}" / "site-packages"
        site_identity = _identity(
            site_packages,
            label="release site-packages",
            directory=True,
        )
        return ReleaseGenerationMarker(
            schema_version=MARKER_SCHEMA_VERSION,
            commit=commit,
            uv_lock_sha256=uv_hash,
            pyproject_sha256=pyproject_hash,
            package_version=package_version,
            python_version=version,
            python_abi=abi,
            venv_path=str(venv),
            venv_identity=venv_identity,
            pyvenv_cfg_sha256=_hash_file(venv / "pyvenv.cfg", label="pyvenv.cfg"),
            python_path=str(self.python_path),
            python_identity=python_identity,
            site_packages_path=str(site_packages),
            site_packages_identity=site_identity,
            published_at=datetime.now(UTC).isoformat(),
        )

    def _assert_root(self, descriptor: int, expected: PathIdentity) -> None:
        active = _identity(
            self.lock_path.parent,
            label="deployment authority root",
            directory=True,
        )
        if _object_key(active) != _object_key(expected) or _object_key(
            PathIdentity.capture(os.fstat(descriptor))
        ) != _object_key(expected):
            raise ReleaseGenerationError("deployment authority root identity changed")
        self._assert_lock()

    def _read_marker(self) -> ReleaseGenerationMarker:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        descriptor = -1
        try:
            descriptor = os.open(
                self.marker_path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or opened.st_nlink != 1
                or stat.S_IMODE(opened.st_mode) != 0o600
            ):
                raise ReleaseGenerationError("release generation marker is unsafe")
            payload = os.read(descriptor, MAX_MARKER_BYTES + 1)
            if len(payload) > MAX_MARKER_BYTES:
                raise ReleaseGenerationError("release generation marker is too large")
            active = self.marker_path.lstat()
            if PathIdentity.capture(opened) != PathIdentity.capture(active):
                raise ReleaseGenerationError("release generation marker identity changed")
            self._assert_root(root_fd, root_identity)
            return ReleaseGenerationMarker.from_payload(json.loads(payload))
        except FileNotFoundError as exc:
            raise ReleaseGenerationError("release generation marker is missing") from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise ReleaseGenerationError("release generation marker cannot be read") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(root_fd)

    @staticmethod
    def _comparable(marker: ReleaseGenerationMarker) -> dict[str, Any]:
        payload = asdict(marker)
        payload.pop("published_at", None)
        return payload

    def verify(self, *, expected_commit: str) -> ReleaseGenerationMarker:
        self._assert_lock()
        published = self._read_marker()
        if published.schema_version != MARKER_SCHEMA_VERSION:
            raise ReleaseGenerationError("release generation marker schema is unsupported")
        if published.commit != expected_commit:
            raise ReleaseGenerationError("release generation marker commit is stale")
        current = self._facts(expected_commit=expected_commit)
        if self._comparable(published) != self._comparable(current):
            if published.uv_lock_sha256 != current.uv_lock_sha256:
                raise ReleaseGenerationError("uv.lock no longer matches release marker")
            if published.venv_identity != current.venv_identity:
                raise ReleaseGenerationError("release venv identity no longer matches marker")
            raise ReleaseGenerationError("release generation marker is stale")
        _assert_tracked_clean(self.repo, self.git_path)
        self._assert_lock()
        return published

    def invalidate(self) -> None:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot invalidate")
        self._assert_lock()
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            self._mutation_hook("before_marker_invalidate")
            self._assert_root(root_fd, root_identity)
            with suppress(FileNotFoundError):
                os.unlink(self.marker_path.name, dir_fd=root_fd)
            os.fsync(root_fd)
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)

    def publish(self, *, expected_commit: str) -> ReleaseGenerationMarker:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot publish")
        self._assert_lock()
        _assert_tracked_clean(self.repo, self.git_path)
        marker = self._facts(expected_commit=expected_commit)
        payload = (
            json.dumps(asdict(marker), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        temporary_name = f".{self.marker_path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
        descriptor = -1
        published = False
        try:
            self._assert_root(root_fd, root_identity)
            descriptor = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_fd,
            )
            os.write(descriptor, payload)
            os.fsync(descriptor)
            self._mutation_hook("marker_temp_fsynced")
            self._assert_root(root_fd, root_identity)
            os.replace(
                temporary_name,
                self.marker_path.name,
                src_dir_fd=root_fd,
                dst_dir_fd=root_fd,
            )
            published = True
            os.fsync(root_fd)
            self._assert_root(root_fd, root_identity)
            active = self.marker_path.lstat()
            if PathIdentity.capture(os.fstat(descriptor)) != PathIdentity.capture(active):
                raise ReleaseGenerationError("published generation marker identity changed")
            self._mutation_hook("marker_published")
            return marker
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if not published:
                with suppress(FileNotFoundError):
                    os.unlink(temporary_name, dir_fd=root_fd)
            os.close(root_fd)


if __name__ == "__main__":
    raise SystemExit("release_generation is a library, not a command")
