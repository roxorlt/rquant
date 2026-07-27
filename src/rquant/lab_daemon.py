"""Fail-closed runtime primitives for Strategy Lab background daemons."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from types import MappingProxyType
from typing import Protocol, TypeVar
from uuid import UUID, uuid4

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_validator

from rquant.lab_artifact_protocol import LabFinalizerAuthorityKey

_CODE_SHA = re.compile(r"^[0-9a-f]{40}$")
_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class LabDaemonConfigurationError(RuntimeError):
    """A daemon cannot start without weakening its trust boundary."""


def _canonical_absolute_path(path: Path, *, label: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise LabDaemonConfigurationError(f"{label} path must be absolute and normalized")
    return candidate


def _validate_private_regular_identity(observed: os.stat_result, *, label: str) -> None:
    if stat.S_ISLNK(observed.st_mode):
        raise LabDaemonConfigurationError(f"{label} must not be a symlink")
    if not stat.S_ISREG(observed.st_mode):
        raise LabDaemonConfigurationError(f"{label} must be a regular file")
    if observed.st_uid != os.getuid():
        raise LabDaemonConfigurationError(f"{label} must be owned by this user")
    if observed.st_mode & 0o777 != 0o600:
        raise LabDaemonConfigurationError(f"{label} must have private mode 0600")
    if observed.st_nlink != 1:
        raise LabDaemonConfigurationError(f"{label} must not be a hardlink")


def _validate_private_directory_identity(observed: os.stat_result, *, label: str) -> None:
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
        raise LabDaemonConfigurationError(f"{label} must be a real directory")
    if observed.st_uid != os.getuid():
        raise LabDaemonConfigurationError(f"{label} must be owned by this user")
    if stat.S_IMODE(observed.st_mode) != 0o700:
        raise LabDaemonConfigurationError(f"{label} must have private mode 0700")


def require_private_directory(path: Path, *, label: str) -> Path:
    candidate = _canonical_absolute_path(path, label=label)
    try:
        observed = candidate.lstat()
    except FileNotFoundError as exc:
        raise LabDaemonConfigurationError(f"{label} directory does not exist") from exc
    if not stat.S_ISDIR(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
        raise LabDaemonConfigurationError(f"{label} must be a real directory")
    if observed.st_uid != os.getuid():
        raise LabDaemonConfigurationError(f"{label} must be owned by this user")
    if observed.st_mode & 0o077:
        raise LabDaemonConfigurationError(f"{label} must have private permissions")
    return candidate


def ensure_private_directory(path: Path, *, label: str) -> Path:
    """Create one validated runtime root after Settings completed pure validation."""
    candidate = _canonical_absolute_path(path, label=label)
    if candidate.resolve(strict=False) != candidate:
        raise LabDaemonConfigurationError(f"{label} path must not use symlink aliases")
    if candidate.exists() or candidate.is_symlink():
        return require_private_directory(candidate, label=label)
    try:
        candidate.mkdir(parents=True, mode=0o700, exist_ok=False)
        candidate.chmod(0o700)
    except OSError as exc:
        raise LabDaemonConfigurationError(f"{label} could not be created safely") from exc
    return require_private_directory(candidate, label=label)


def require_clean_code_sha(provider: Callable[[], str | None]) -> str:
    try:
        value = provider()
    except Exception as exc:
        raise LabDaemonConfigurationError("clean code SHA provider failed") from exc
    if not isinstance(value, str) or _CODE_SHA.fullmatch(value) is None:
        raise LabDaemonConfigurationError("daemon requires a clean 40-character lowercase Git SHA")
    return value


def _require_physical_checkout_virtualenv(path: Path) -> tuple[Path, Path]:
    expected = _canonical_absolute_path(path, label="expected checkout root")
    try:
        resolved_expected = expected.resolve(strict=True)
        expected_venv = expected / ".venv"
        expected_venv_stat = expected_venv.lstat()
        resolved_venv = expected_venv.resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise LabDaemonConfigurationError(
            "lab runtime binding contains a missing or unsafe path"
        ) from exc
    if resolved_expected != expected:
        raise LabDaemonConfigurationError(
            "lab runtime binding expected checkout must be a physical directory"
        )
    if (
        not stat.S_ISDIR(expected_venv_stat.st_mode)
        or stat.S_ISLNK(expected_venv_stat.st_mode)
        or expected_venv_stat.st_uid != os.getuid()
        or expected_venv_stat.st_mode & 0o022
        or resolved_venv != expected_venv
    ):
        raise LabDaemonConfigurationError(
            "lab runtime binding requires an owned physical virtualenv"
        )
    return expected, expected_venv


def verify_lab_runtime_binding(
    *,
    expected_checkout_root: Path,
    executable: Path,
    launcher: Path,
    virtualenv_prefix: Path,
    console_interpreter: Path,
    package_file: Path,
    working_directory: Path,
    verified_code_sha: str,
    git_top_level: Path,
    git_head: str,
) -> str:
    """Bind one daemon process to the checkout named by its launch contract."""
    expected, expected_venv = _require_physical_checkout_virtualenv(expected_checkout_root)
    try:
        runtime_cwd = Path(working_directory).resolve(strict=True)
        runtime_package_root = Path(package_file).resolve(strict=True).parent
        runtime_executable = _canonical_absolute_path(
            Path(executable),
            label="runtime executable",
        )
        runtime_launcher = _canonical_absolute_path(Path(launcher), label="runtime launcher")
        runtime_prefix = _canonical_absolute_path(
            Path(virtualenv_prefix),
            label="runtime virtualenv prefix",
        )
        runtime_console_interpreter = _canonical_absolute_path(
            Path(console_interpreter),
            label="runtime console interpreter",
        )
        expected_launcher = expected / ".venv" / "bin" / "rquant"
        expected_package_root = (expected / "src" / "rquant").resolve(strict=True)
        runtime_git_root = Path(git_top_level).resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise LabDaemonConfigurationError(
            "lab runtime binding contains a missing or unsafe path"
        ) from exc
    if runtime_cwd != expected:
        raise LabDaemonConfigurationError("lab runtime binding working directory mismatch")
    if (
        not runtime_executable.is_relative_to(expected_venv)
        or runtime_executable.parent.name != "bin"
        or not runtime_executable.name.startswith("python")
    ):
        raise LabDaemonConfigurationError("lab runtime binding executable mismatch")
    if runtime_launcher != expected_launcher:
        raise LabDaemonConfigurationError("lab runtime binding launcher mismatch")
    if runtime_package_root != expected_package_root:
        raise LabDaemonConfigurationError("lab runtime binding package root mismatch")
    if runtime_prefix != expected_venv:
        raise LabDaemonConfigurationError("lab runtime binding virtualenv prefix mismatch")
    if runtime_console_interpreter != runtime_executable:
        raise LabDaemonConfigurationError("lab runtime binding console shebang mismatch")
    if runtime_git_root != expected:
        raise LabDaemonConfigurationError("lab runtime binding Git top-level mismatch")
    if _CODE_SHA.fullmatch(verified_code_sha) is None or git_head != verified_code_sha:
        raise LabDaemonConfigurationError("lab runtime binding verified SHA mismatch")
    return verified_code_sha


def require_lab_runtime_binding(expected_checkout_root: Path) -> str:
    """Read and verify all live process identities before daemon I/O starts."""
    expected, _expected_venv = _require_physical_checkout_virtualenv(
        expected_checkout_root,
    )
    import rquant

    try:
        top_level_result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=expected,
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        head_result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=expected,
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise LabDaemonConfigurationError("lab runtime binding Git probe failed") from exc
    git_head = head_result.stdout.strip()
    if top_level_result.returncode != 0 or head_result.returncode != 0:
        raise LabDaemonConfigurationError("lab runtime binding Git probe failed")
    package_file = getattr(rquant, "__file__", None)
    if not isinstance(package_file, str) or not package_file:
        raise LabDaemonConfigurationError("lab runtime binding package file is unavailable")
    launcher = _canonical_absolute_path(Path(sys.argv[0]), label="runtime launcher")
    try:
        launcher_stat = launcher.lstat()
        if (
            not stat.S_ISREG(launcher_stat.st_mode)
            or stat.S_ISLNK(launcher_stat.st_mode)
            or launcher_stat.st_uid != os.getuid()
            or launcher_stat.st_nlink != 1
            or launcher_stat.st_mode & 0o022
        ):
            raise LabDaemonConfigurationError("lab runtime binding launcher is unsafe")
        launcher_descriptor = os.open(
            launcher,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            current_launcher = os.fstat(launcher_descriptor)
            if (current_launcher.st_dev, current_launcher.st_ino) != (
                launcher_stat.st_dev,
                launcher_stat.st_ino,
            ):
                raise LabDaemonConfigurationError("lab runtime binding launcher identity changed")
            first_line = os.read(launcher_descriptor, 4_096).splitlines()[0]
        finally:
            os.close(launcher_descriptor)
        if not first_line.startswith(b"#!"):
            raise LabDaemonConfigurationError("lab runtime binding launcher has no shebang")
        console_interpreter = Path(first_line[2:].decode("utf-8").strip())
    except (IndexError, UnicodeDecodeError, OSError) as exc:
        raise LabDaemonConfigurationError(
            "lab runtime binding launcher could not be verified"
        ) from exc
    verify_lab_runtime_binding(
        expected_checkout_root=expected,
        executable=Path(sys.executable),
        launcher=launcher,
        virtualenv_prefix=Path(sys.prefix),
        console_interpreter=console_interpreter,
        package_file=Path(package_file),
        working_directory=Path.cwd(),
        verified_code_sha=git_head,
        git_top_level=Path(top_level_result.stdout.strip()),
        git_head=git_head,
    )
    from rquant.research_manifest import detect_verified_code_commit

    injected_sha = os.getenv("RQUANT_CODE_COMMIT", "").strip()
    if injected_sha and injected_sha != git_head:
        raise LabDaemonConfigurationError("lab runtime binding injected SHA mismatch")
    verified = require_clean_code_sha(lambda: detect_verified_code_commit(expected))
    if verified != git_head:
        raise LabDaemonConfigurationError("lab runtime binding verified SHA mismatch")
    return verified


def _read_private_file(path: Path, *, label: str, max_bytes: int = 16_384) -> bytes:
    candidate = _canonical_absolute_path(path, label=label)
    try:
        observed = candidate.lstat()
    except FileNotFoundError as exc:
        raise LabDaemonConfigurationError(f"{label} key file does not exist") from exc
    try:
        _validate_private_regular_identity(observed, label=f"{label} key file")
    except LabDaemonConfigurationError as exc:
        if "mode 0600" in str(exc):
            raise LabDaemonConfigurationError(
                f"{label} key file must have private permissions (mode 0600)"
            ) from exc
        raise
    if observed.st_size > max_bytes:
        raise LabDaemonConfigurationError(f"{label} key file exceeds size limit")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise LabDaemonConfigurationError(f"{label} key file could not be opened safely") from exc
    try:
        current = os.fstat(descriptor)
        if (current.st_dev, current.st_ino) != (observed.st_dev, observed.st_ino):
            raise LabDaemonConfigurationError(f"{label} key file changed during validation")
        _validate_private_regular_identity(current, label=f"{label} key file")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        final = os.fstat(descriptor)
        if (final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns) != (
            current.st_dev,
            current.st_ino,
            current.st_size,
            current.st_mtime_ns,
        ) or len(payload) != final.st_size:
            raise LabDaemonConfigurationError(f"{label} key file changed during read")
        _validate_private_regular_identity(final, label=f"{label} key file")
        try:
            active = candidate.lstat()
            _validate_private_regular_identity(active, label=f"{label} key file")
        except (OSError, LabDaemonConfigurationError) as exc:
            raise LabDaemonConfigurationError(f"{label} key file changed during read") from exc
        if (
            active.st_dev,
            active.st_ino,
            active.st_size,
            active.st_mtime_ns,
        ) != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
        ):
            raise LabDaemonConfigurationError(f"{label} key file changed during read")
    finally:
        os.close(descriptor)
    if len(payload) > max_bytes:
        raise LabDaemonConfigurationError(f"{label} key file exceeds size limit")
    return payload


_ConnectionT = TypeVar("_ConnectionT")


class LabSqliteAuthority:
    """Retain the filesystem identity that authorizes one Lab SQLite file.

    SQLite must open its real pathname so WAL and sidecar discovery keep working.
    The retained descriptors and pre/post-connect fences reject pathname or parent
    replacement before the first SQL statement. They cannot prevent a malicious
    same-UID process from performing a complete ABA swap inside that narrow gap.
    """

    def __init__(
        self,
        *,
        path: Path,
        label: str,
        parent_descriptor: int,
        database_descriptor: int,
        parent_identity: os.stat_result,
        database_identity: os.stat_result,
    ) -> None:
        self.path = path
        self.label = label
        self._parent_descriptor = parent_descriptor
        self._database_descriptor = database_descriptor
        self._parent_identity = parent_identity
        self._database_identity = database_identity

    @staticmethod
    def _identity(observed: os.stat_result) -> tuple[int, int]:
        return observed.st_dev, observed.st_ino

    def assert_current(self) -> None:
        if self._parent_descriptor < 0 or self._database_descriptor < 0:
            raise LabDaemonConfigurationError(f"{self.label} authority is closed")
        try:
            parent_fd_stat = os.fstat(self._parent_descriptor)
            parent_path_stat = self.path.parent.lstat()
            database_fd_stat = os.fstat(self._database_descriptor)
            database_path_stat = os.stat(
                self.path.name,
                dir_fd=self._parent_descriptor,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise LabDaemonConfigurationError(
                f"{self.label} identity changed after validation"
            ) from exc
        try:
            _validate_private_directory_identity(
                parent_fd_stat,
                label=f"{self.label} parent",
            )
            _validate_private_directory_identity(
                parent_path_stat,
                label=f"{self.label} parent",
            )
        except LabDaemonConfigurationError as exc:
            raise LabDaemonConfigurationError(
                f"{self.label} parent identity changed after validation"
            ) from exc
        if self._identity(parent_fd_stat) != self._identity(
            self._parent_identity
        ) or self._identity(parent_path_stat) != self._identity(self._parent_identity):
            raise LabDaemonConfigurationError(
                f"{self.label} parent identity changed after validation"
            )
        try:
            _validate_private_regular_identity(database_fd_stat, label=self.label)
            _validate_private_regular_identity(database_path_stat, label=self.label)
        except LabDaemonConfigurationError as exc:
            raise LabDaemonConfigurationError(
                f"{self.label} identity changed after validation"
            ) from exc
        if self._identity(database_fd_stat) != self._identity(
            self._database_identity
        ) or self._identity(database_path_stat) != self._identity(self._database_identity):
            raise LabDaemonConfigurationError(f"{self.label} identity changed after validation")

    def open_verified_connection(
        self,
        opener: Callable[[Path], _ConnectionT],
    ) -> _ConnectionT:
        self.assert_current()
        connection = opener(self.path)
        try:
            self.assert_current()
        except BaseException:
            close = getattr(connection, "close", None)
            if callable(close):
                close()
            raise
        return connection

    def close(self) -> None:
        database_descriptor, self._database_descriptor = self._database_descriptor, -1
        parent_descriptor, self._parent_descriptor = self._parent_descriptor, -1
        if database_descriptor >= 0:
            os.close(database_descriptor)
        if parent_descriptor >= 0:
            try:
                fcntl.flock(parent_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(parent_descriptor)

    def __enter__(self) -> LabSqliteAuthority:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def prepare_private_sqlite_path(
    path: Path,
    *,
    label: str,
    create: bool,
) -> LabSqliteAuthority:
    """Create or verify the daemon SQLite authority without following links."""
    candidate = _canonical_absolute_path(path, label=label)
    parent = candidate.parent
    try:
        if parent.resolve(strict=True) != parent:
            raise LabDaemonConfigurationError(f"{label} parent must be canonical")
        parent_stat = parent.lstat()
    except FileNotFoundError as exc:
        raise LabDaemonConfigurationError(f"{label} parent directory does not exist") from exc
    if not stat.S_ISDIR(parent_stat.st_mode) or stat.S_ISLNK(parent_stat.st_mode):
        raise LabDaemonConfigurationError(f"{label} parent must be a real directory")
    if parent_stat.st_uid != os.getuid() or parent_stat.st_mode & 0o077:
        raise LabDaemonConfigurationError(
            f"{label} parent must be owned by this user with private mode 0700"
        )
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        parent_descriptor = os.open(parent, directory_flags)
    except OSError as exc:
        raise LabDaemonConfigurationError(f"{label} parent could not be opened safely") from exc
    try:
        opened_parent = os.fstat(parent_descriptor)
        active_parent = parent.lstat()
        _validate_private_directory_identity(opened_parent, label=f"{label} parent")
        _validate_private_directory_identity(active_parent, label=f"{label} parent")
        expected_identity = (parent_stat.st_dev, parent_stat.st_ino)
        if (opened_parent.st_dev, opened_parent.st_ino) != expected_identity or (
            active_parent.st_dev,
            active_parent.st_ino,
        ) != expected_identity:
            raise LabDaemonConfigurationError(f"{label} parent identity changed")
    except BaseException:
        os.close(parent_descriptor)
        raise
    descriptor = -1
    try:
        try:
            observed = os.stat(candidate.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            if not create:
                raise LabDaemonConfigurationError(f"{label} does not exist") from None
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(candidate.name, flags, 0o600, dir_fd=parent_descriptor)
            except OSError as exc:
                raise LabDaemonConfigurationError(
                    f"{label} could not be created atomically"
                ) from exc
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            observed = os.fstat(descriptor)
        else:
            _validate_private_regular_identity(observed, label=label)
            flags = (os.O_RDWR if create else os.O_RDONLY) | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(candidate.name, flags, dir_fd=parent_descriptor)
            except OSError as exc:
                raise LabDaemonConfigurationError(f"{label} could not be opened safely") from exc
        current = os.fstat(descriptor)
        if (current.st_dev, current.st_ino) != (observed.st_dev, observed.st_ino):
            raise LabDaemonConfigurationError(f"{label} changed during validation")
        _validate_private_regular_identity(current, label=label)
        try:
            fcntl.flock(parent_descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as exc:
            raise LabDaemonConfigurationError(
                f"{label} maintenance lock could not be acquired"
            ) from exc
        authority = LabSqliteAuthority(
            path=candidate,
            label=label,
            parent_descriptor=parent_descriptor,
            database_descriptor=descriptor,
            parent_identity=opened_parent,
            database_identity=current,
        )
        authority.assert_current()
        parent_descriptor = -1
        descriptor = -1
        return authority
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)
        raise


def _decode_secret(value: object, *, label: str) -> bytes:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64,}", value) is None:
        raise LabDaemonConfigurationError(f"{label} must contain at least 32 bytes as hex")
    try:
        secret = bytes.fromhex(value)
    except ValueError as exc:  # pragma: no cover - guarded by regex
        raise LabDaemonConfigurationError(f"{label} is not valid hex") from exc
    if len(secret) < 32:
        raise LabDaemonConfigurationError(f"{label} must contain at least 32 bytes")
    return secret


@dataclass(frozen=True)
class LabAuthorityKeyring:
    active_key_id: str
    _active_secret: bytes
    _verification_secrets: Mapping[str, bytes]

    @classmethod
    def load(
        cls,
        *,
        active_key_id: str,
        active_key_path: Path,
        verification_keyring_path: Path,
    ) -> LabAuthorityKeyring:
        if _KEY_ID.fullmatch(active_key_id) is None:
            raise LabDaemonConfigurationError("authority active key id is invalid")
        active_payload = _read_private_file(active_key_path, label="authority active")
        try:
            active_text = active_payload.decode("ascii").strip()
        except UnicodeDecodeError as exc:
            raise LabDaemonConfigurationError("authority active key must be ASCII hex") from exc
        active_secret = _decode_secret(active_text, label="authority active key")
        ring_payload = _read_private_file(
            verification_keyring_path,
            label="authority verification keyring",
        )
        try:
            document = json.loads(ring_payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LabDaemonConfigurationError("authority keyring is not valid JSON") from exc
        if not isinstance(document, dict) or document.get("schema_version") != 1:
            raise LabDaemonConfigurationError("authority keyring schema_version must be 1")
        raw_keys = document.get("keys")
        if not isinstance(raw_keys, dict) or not raw_keys:
            raise LabDaemonConfigurationError("authority keyring must contain keys")
        secrets: dict[str, bytes] = {}
        for key_id, raw_secret in raw_keys.items():
            if not isinstance(key_id, str) or _KEY_ID.fullmatch(key_id) is None:
                raise LabDaemonConfigurationError("authority keyring contains an invalid key id")
            secrets[key_id] = _decode_secret(
                raw_secret,
                label=f"authority keyring key {key_id}",
            )
        if secrets.get(active_key_id) != active_secret:
            raise LabDaemonConfigurationError(
                "authority active key does not match verification keyring"
            )
        return cls(
            active_key_id=active_key_id,
            _active_secret=active_secret,
            _verification_secrets=MappingProxyType(secrets),
        )

    def signing_key(self) -> LabFinalizerAuthorityKey:
        return LabFinalizerAuthorityKey(
            key_id=self.active_key_id,
            secret=self._active_secret,
        )

    def verification_key(self, key_id: str) -> LabFinalizerAuthorityKey | None:
        secret = self._verification_secrets.get(key_id)
        if secret is None:
            return None
        return LabFinalizerAuthorityKey(key_id=key_id, secret=secret)


class LabDaemonLock:
    """Advisory daemon lock anchored beside, rather than inside, its runtime root.

    The stable lock name binds the configured canonical root path and daemon name.
    Replacing the lock root therefore cannot create a second lock namespace. A
    malicious same-UID replacement of a higher-level parent remains outside this
    local filesystem boundary.
    """

    def __init__(self, root: Path, name: str) -> None:
        if re.fullmatch(r"[a-z][a-z0-9-]{0,63}", name) is None:
            raise ValueError("daemon lock name is invalid")
        self.root = Path(root)
        self.name = name
        self.path = self.root / f"{name}.lock"
        self.authority_path: Path | None = None
        self._descriptor = -1
        self._root_descriptor = -1
        self._parent_descriptor = -1

    @staticmethod
    def _open_private_file(
        directory_descriptor: int,
        name: str,
        *,
        label: str,
    ) -> int:
        try:
            observed = os.stat(
                name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            observed = None
        if observed is None:
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(name, flags, 0o600, dir_fd=directory_descriptor)
            except OSError as exc:
                raise LabDaemonConfigurationError(
                    f"{label} could not be created atomically"
                ) from exc
            created = True
        else:
            _validate_private_regular_identity(observed, label=label)
            flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(name, flags, dir_fd=directory_descriptor)
            except OSError as exc:
                raise LabDaemonConfigurationError(f"{label} could not be opened safely") from exc
            created = False
        try:
            if created:
                os.fchmod(descriptor, 0o600)
            current = os.fstat(descriptor)
            if observed is not None and (current.st_dev, current.st_ino) != (
                observed.st_dev,
                observed.st_ino,
            ):
                raise LabDaemonConfigurationError(f"{label} changed during validation")
            _validate_private_regular_identity(current, label=label)
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    def _assert_parent_current(
        self,
        descriptor: int,
        expected: os.stat_result,
    ) -> None:
        try:
            current = os.fstat(descriptor)
            path_current = self.root.parent.lstat()
        except OSError as exc:
            raise LabDaemonConfigurationError("daemon lock parent identity changed") from exc
        try:
            _validate_private_directory_identity(current, label="daemon lock parent")
            _validate_private_directory_identity(path_current, label="daemon lock parent")
        except LabDaemonConfigurationError as exc:
            raise LabDaemonConfigurationError("daemon lock parent identity changed") from exc
        if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino) or (
            path_current.st_dev,
            path_current.st_ino,
        ) != (expected.st_dev, expected.st_ino):
            raise LabDaemonConfigurationError("daemon lock parent identity changed")

    def _assert_root_current(
        self,
        descriptor: int,
        expected: os.stat_result,
    ) -> None:
        try:
            current = os.fstat(descriptor)
            path_current = self.root.lstat()
        except OSError as exc:
            raise LabDaemonConfigurationError("daemon lock root identity changed") from exc
        try:
            _validate_private_directory_identity(current, label="daemon lock root")
            _validate_private_directory_identity(path_current, label="daemon lock root")
        except LabDaemonConfigurationError as exc:
            raise LabDaemonConfigurationError("daemon lock root identity changed") from exc
        if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino) or (
            path_current.st_dev,
            path_current.st_ino,
        ) != (expected.st_dev, expected.st_ino):
            raise LabDaemonConfigurationError("daemon lock root identity changed")

    def acquire(self) -> None:
        if self._descriptor >= 0 or self._root_descriptor >= 0 or self._parent_descriptor >= 0:
            raise RuntimeError("daemon lock is already acquired")
        self.root = _canonical_absolute_path(self.root, label="daemon lock root")
        self.path = self.root / f"{self.name}.lock"
        parent = self.root.parent
        try:
            parent_stat = parent.lstat()
            if parent.resolve(strict=True) != parent:
                raise LabDaemonConfigurationError("daemon lock parent must be canonical")
            _validate_private_directory_identity(parent_stat, label="daemon lock parent")
        except FileNotFoundError as exc:
            raise LabDaemonConfigurationError("daemon lock parent does not exist") from exc
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            parent_descriptor = os.open(parent, directory_flags)
        except OSError as exc:
            raise LabDaemonConfigurationError(
                "daemon lock parent could not be opened safely"
            ) from exc
        root_descriptor = -1
        descriptor = -1
        metadata_descriptor = -1
        try:
            self._assert_parent_current(parent_descriptor, parent_stat)
            try:
                root_stat = os.stat(
                    self.root.name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                try:
                    os.mkdir(self.root.name, mode=0o700, dir_fd=parent_descriptor)
                except OSError as exc:
                    raise LabDaemonConfigurationError(
                        "daemon lock root could not be created safely"
                    ) from exc
                root_stat = os.stat(
                    self.root.name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
            _validate_private_directory_identity(root_stat, label="daemon lock root")
            root_descriptor = os.open(
                self.root.name,
                directory_flags,
                dir_fd=parent_descriptor,
            )
            self._assert_parent_current(parent_descriptor, parent_stat)
            self._assert_root_current(root_descriptor, root_stat)

            root_identity = hashlib.sha256(os.fsencode(str(self.root))).hexdigest()[:24]
            authority_name = f".rquant-lab-lock-{root_identity}-{self.name}.lock"
            self.authority_path = parent / authority_name
            descriptor = self._open_private_file(
                parent_descriptor,
                authority_name,
                label="daemon authority lock file",
            )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise LabDaemonConfigurationError(
                    f"lab {self.name} daemon is already running"
                ) from exc
            self._assert_parent_current(parent_descriptor, parent_stat)
            self._assert_root_current(root_descriptor, root_stat)

            metadata_descriptor = self._open_private_file(
                root_descriptor,
                self.path.name,
                label="daemon lock file",
            )
            os.ftruncate(metadata_descriptor, 0)
            os.write(metadata_descriptor, f"{os.getpid()}\n".encode("ascii"))
            os.fsync(metadata_descriptor)
            os.close(metadata_descriptor)
            metadata_descriptor = -1
            self._assert_parent_current(parent_descriptor, parent_stat)
            self._assert_root_current(root_descriptor, root_stat)
        except BaseException:
            if metadata_descriptor >= 0:
                os.close(metadata_descriptor)
            if descriptor >= 0:
                os.close(descriptor)
            if root_descriptor >= 0:
                os.close(root_descriptor)
            os.close(parent_descriptor)
            raise
        self._descriptor = descriptor
        self._root_descriptor = root_descriptor
        self._parent_descriptor = parent_descriptor

    def release(self) -> None:
        if self._descriptor < 0 and self._root_descriptor < 0 and self._parent_descriptor < 0:
            return
        descriptor, self._descriptor = self._descriptor, -1
        root_descriptor, self._root_descriptor = self._root_descriptor, -1
        parent_descriptor, self._parent_descriptor = self._parent_descriptor, -1
        try:
            if descriptor >= 0:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if root_descriptor >= 0:
                os.close(root_descriptor)
            if parent_descriptor >= 0:
                os.close(parent_descriptor)

    def __enter__(self) -> LabDaemonLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


class _FinalizationCandidate(Protocol):
    job_id: UUID
    job_version: int
    spec_hash: str
    updated_at: datetime


class _FinalizationPage(Protocol):
    items: tuple[_FinalizationCandidate, ...]
    has_more: bool
    next_cursor: str | None


class _FinalizationReader(Protocol):
    def list_finalization_candidates(
        self,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> _FinalizationPage: ...


class _FinalizationResult(Protocol):
    status: str


class _Finalizer(Protocol):
    def finalize(self, job_id: UUID) -> _FinalizationResult: ...


class LabFinalizerFailureState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    attempts: int = Field(ge=1, le=1_000_000)
    cooldown_until: datetime
    last_seen_cycle: int = Field(default=0, ge=0)

    @field_validator("cooldown_until")
    @classmethod
    def require_aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("cooldown_until must be timezone-aware")
        return value.astimezone(UTC)


class LabFinalizerDaemonState(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    schema_version: int = Field(default=1, ge=1, le=1)
    cursor: str | None = Field(default=None, max_length=8_192)
    cycle: int = Field(default=0, ge=0)
    failures: dict[str, LabFinalizerFailureState] = Field(default_factory=dict)


class LabFinalizerStateStore:
    """Private crash-safe state outside the scheduler-owned Lab SQLite ledger."""

    _MAX_BYTES = 1_048_576
    _MAX_FAILURES = 4_096

    def __init__(self, root: Path) -> None:
        self.root = _canonical_absolute_path(root, label="lab finalizer state")
        self.path = self.root / "state.json"

    def _open_root(self) -> tuple[int, os.stat_result]:
        require_private_directory(self.root, label="lab finalizer state")
        initial = self.root.lstat()
        _validate_private_directory_identity(initial, label="lab finalizer state")
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.root, flags)
        except OSError as exc:
            raise LabDaemonConfigurationError(
                "lab finalizer state directory could not be opened safely"
            ) from exc
        try:
            self._assert_root_current(descriptor, initial)
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor, initial

    def _assert_root_current(
        self,
        descriptor: int,
        expected: os.stat_result,
    ) -> None:
        try:
            observed = os.fstat(descriptor)
            path_observed = self.root.lstat()
        except OSError as exc:
            raise LabDaemonConfigurationError(
                "lab finalizer state directory identity changed"
            ) from exc
        try:
            _validate_private_directory_identity(observed, label="lab finalizer state")
            _validate_private_directory_identity(path_observed, label="lab finalizer state")
        except LabDaemonConfigurationError as exc:
            raise LabDaemonConfigurationError(
                "lab finalizer state directory identity changed"
            ) from exc
        if (observed.st_dev, observed.st_ino) != (expected.st_dev, expected.st_ino) or (
            path_observed.st_dev,
            path_observed.st_ino,
        ) != (expected.st_dev, expected.st_ino):
            raise LabDaemonConfigurationError("lab finalizer state directory identity changed")

    def load(self) -> LabFinalizerDaemonState:
        root_descriptor, root_identity = self._open_root()
        descriptor = -1
        try:
            try:
                observed = os.stat(
                    self.path.name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                self._assert_root_current(root_descriptor, root_identity)
                return LabFinalizerDaemonState()
            _validate_private_regular_identity(observed, label="lab finalizer state file")
            if observed.st_size > self._MAX_BYTES:
                raise LabDaemonConfigurationError("lab finalizer state file is too large")
            descriptor = os.open(
                self.path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_descriptor,
            )
            current = os.fstat(descriptor)
            if (current.st_dev, current.st_ino) != (observed.st_dev, observed.st_ino):
                raise LabDaemonConfigurationError("lab finalizer state identity changed")
            _validate_private_regular_identity(current, label="lab finalizer state file")
            payload = b""
            while len(payload) <= self._MAX_BYTES:
                chunk = os.read(descriptor, min(65_536, self._MAX_BYTES + 1 - len(payload)))
                if not chunk:
                    break
                payload += chunk
            final = os.fstat(descriptor)
            _validate_private_regular_identity(final, label="lab finalizer state file")
            if (final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns) != (
                current.st_dev,
                current.st_ino,
                current.st_size,
                current.st_mtime_ns,
            ) or len(payload) != final.st_size:
                raise LabDaemonConfigurationError("lab finalizer state changed during read")
            try:
                state = LabFinalizerDaemonState.model_validate_json(payload)
            except (ValueError, TypeError) as exc:
                raise LabDaemonConfigurationError("lab finalizer state is corrupt") from exc
            if len(state.failures) > self._MAX_FAILURES:
                raise LabDaemonConfigurationError("lab finalizer state has too many failures")
            self._assert_root_current(root_descriptor, root_identity)
            return state
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(root_descriptor)

    def save(self, state: LabFinalizerDaemonState) -> None:
        state = LabFinalizerDaemonState.model_validate(state.model_dump())
        if len(state.failures) > self._MAX_FAILURES:
            raise LabDaemonConfigurationError("lab finalizer state has too many failures")
        payload = state.model_dump_json().encode("utf-8")
        if len(payload) > self._MAX_BYTES:
            raise LabDaemonConfigurationError("lab finalizer state file is too large")
        root_descriptor, root_identity = self._open_root()
        temporary_name = f".state.{os.getpid()}.{uuid4().hex}.tmp"
        descriptor = -1
        try:
            try:
                existing = os.stat(
                    self.path.name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                existing = None
            if existing is not None:
                _validate_private_regular_identity(existing, label="lab finalizer state file")
            descriptor = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_descriptor,
            )
            os.fchmod(descriptor, 0o600)
            offset = 0
            while offset < len(payload):
                offset += os.write(descriptor, payload[offset:])
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            self._assert_root_current(root_descriptor, root_identity)
            if existing is not None:
                active = os.stat(
                    self.path.name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
                _validate_private_regular_identity(
                    active,
                    label="lab finalizer state file",
                )
                if (active.st_dev, active.st_ino) != (existing.st_dev, existing.st_ino):
                    raise LabDaemonConfigurationError(
                        "lab finalizer state identity changed before commit"
                    )
            os.replace(
                temporary_name,
                self.path.name,
                src_dir_fd=root_descriptor,
                dst_dir_fd=root_descriptor,
            )
            self._assert_root_current(root_descriptor, root_identity)
            committed = os.stat(
                self.path.name,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
            active_path = self.path.lstat()
            _validate_private_regular_identity(
                committed,
                label="lab finalizer state file",
            )
            _validate_private_regular_identity(
                active_path,
                label="lab finalizer state file",
            )
            if (committed.st_dev, committed.st_ino) != (
                active_path.st_dev,
                active_path.st_ino,
            ) or committed.st_size != len(payload):
                raise LabDaemonConfigurationError(
                    "lab finalizer state identity changed after commit"
                )
            os.fsync(root_descriptor)
        except OSError as exc:
            raise LabDaemonConfigurationError(
                "lab finalizer state could not be committed atomically"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=root_descriptor)
            os.close(root_descriptor)


def _finalization_fingerprint(candidate: _FinalizationCandidate) -> str:
    updated_at = candidate.updated_at
    if updated_at.tzinfo is None or updated_at.utcoffset() is None:
        raise LabDaemonConfigurationError(
            "finalization candidate updated_at must be timezone-aware"
        )
    payload = "\0".join(
        (
            str(candidate.job_id),
            str(candidate.job_version),
            candidate.spec_hash,
            updated_at.astimezone(UTC).isoformat(),
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class LabFinalizerTickResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidates: int = Field(ge=0)
    published: int = Field(default=0, ge=0)
    acknowledged: int = Field(default=0, ge=0)
    rejected: int = Field(default=0, ge=0)
    not_ready: int = Field(default=0, ge=0)
    cooled_down: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    first_error_type: str | None = None
    first_error_message: str | None = None


class LabFinalizerDaemon:
    """Bounded polling loop around the read-only finalizer core."""

    def __init__(
        self,
        *,
        reader: _FinalizationReader,
        finalizer: _Finalizer,
        state_store: LabFinalizerStateStore,
        max_jobs_per_tick: int,
        poll_interval_ms: int,
        failure_cooldown_seconds: int,
        failure_cooldown_max_seconds: int,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        if not 1 <= max_jobs_per_tick <= 128:
            raise ValueError("max_jobs_per_tick must be between 1 and 128")
        if poll_interval_ms < 1:
            raise ValueError("poll_interval_ms must be positive")
        if failure_cooldown_seconds < 1:
            raise ValueError("failure_cooldown_seconds must be positive")
        if failure_cooldown_max_seconds < failure_cooldown_seconds:
            raise ValueError("failure cooldown maximum must not be below its base")
        self.reader = reader
        self.finalizer = finalizer
        self.state_store = state_store
        self.max_jobs_per_tick = max_jobs_per_tick
        self.poll_interval_ms = poll_interval_ms
        self.failure_cooldown_seconds = failure_cooldown_seconds
        self.failure_cooldown_max_seconds = failure_cooldown_max_seconds
        self.now_provider = now_provider or (lambda: datetime.now(UTC))
        self._stop = Event()

    def request_stop(self) -> None:
        self._stop.set()

    @staticmethod
    def _make_failure_room(
        state: LabFinalizerDaemonState,
        incoming_key: str,
    ) -> None:
        if (
            incoming_key in state.failures
            or len(state.failures) < LabFinalizerStateStore._MAX_FAILURES
        ):
            return
        victim = min(
            state.failures.items(),
            key=lambda item: (
                item[1].last_seen_cycle,
                item[1].cooldown_until,
                item[0],
            ),
        )[0]
        state.failures.pop(victim)

    def run_once(self) -> LabFinalizerTickResult:
        state = self.state_store.load()
        page = self.reader.list_finalization_candidates(
            limit=self.max_jobs_per_tick,
            cursor=state.cursor,
        )
        counts = {
            "published": 0,
            "acknowledged": 0,
            "rejected": 0,
            "not_ready": 0,
        }
        failed = 0
        cooled_down = 0
        first_error_type: str | None = None
        first_error_message: str | None = None
        for candidate in page.items:
            if self._stop.is_set():
                break
            now = self.now_provider()
            if now.tzinfo is None or now.utcoffset() is None:
                raise LabDaemonConfigurationError("finalizer clock must be timezone-aware")
            fingerprint = _finalization_fingerprint(candidate)
            failure_key = str(candidate.job_id)
            prior_failure = state.failures.get(failure_key)
            if prior_failure is not None and prior_failure.fingerprint != fingerprint:
                state.failures.pop(failure_key, None)
                prior_failure = None
            elif prior_failure is not None:
                prior_failure = prior_failure.model_copy(update={"last_seen_cycle": state.cycle})
                state.failures[failure_key] = prior_failure
            if prior_failure is not None and now < prior_failure.cooldown_until:
                cooled_down += 1
                continue
            try:
                result = self.finalizer.finalize(candidate.job_id)
                if result.status not in counts:
                    raise RuntimeError(f"unknown finalizer status: {result.status}")
                counts[result.status] += 1
                state.failures.pop(failure_key, None)
            except Exception as exc:
                failed += 1
                attempts = 1 if prior_failure is None else prior_failure.attempts + 1
                exponent = min(attempts - 1, 30)
                cooldown_seconds = min(
                    self.failure_cooldown_max_seconds,
                    self.failure_cooldown_seconds * (2**exponent),
                )
                self._make_failure_room(state, failure_key)
                state.failures[failure_key] = LabFinalizerFailureState(
                    fingerprint=fingerprint,
                    attempts=attempts,
                    cooldown_until=now + timedelta(seconds=cooldown_seconds),
                    last_seen_cycle=state.cycle,
                )
                self.state_store.save(state)
                if first_error_type is None:
                    first_error_type = type(exc).__name__
                    first_error_message = " ".join((str(exc) or type(exc).__name__).split())[:400]
                logger.exception(
                    "lab-finalizer candidate failed: job_id={} error_type={}",
                    candidate.job_id,
                    type(exc).__name__,
                )
        if not self._stop.is_set():
            if page.has_more:
                if not page.next_cursor:
                    raise LabDaemonConfigurationError(
                        "finalization page with has_more requires next_cursor"
                    )
                state.cursor = page.next_cursor
            else:
                state.cursor = None
                state.failures = {
                    key: failure
                    for key, failure in state.failures.items()
                    if failure.last_seen_cycle >= state.cycle
                }
                state.cycle += 1
        self.state_store.save(state)
        return LabFinalizerTickResult(
            candidates=len(page.items),
            failed=failed,
            cooled_down=cooled_down,
            first_error_type=first_error_type,
            first_error_message=first_error_message,
            **counts,
        )

    def run_forever(self) -> None:
        while not self._stop.is_set():
            result = self.run_once()
            logger.info("lab-finalizer tick: {}", result.model_dump_json())
            self._stop.wait(self.poll_interval_ms / 1_000)
