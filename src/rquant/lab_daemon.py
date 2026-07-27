"""Fail-closed runtime primitives for Strategy Lab background daemons."""

from __future__ import annotations

import fcntl
import json
import os
import re
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from types import MappingProxyType
from typing import Protocol
from uuid import UUID

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

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
    finally:
        os.close(descriptor)
    if len(payload) > max_bytes:
        raise LabDaemonConfigurationError(f"{label} key file exceeds size limit")
    return payload


def prepare_private_sqlite_path(
    path: Path,
    *,
    label: str,
    create: bool,
) -> Path:
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
    if parent_stat.st_uid != os.getuid() or parent_stat.st_mode & 0o022:
        raise LabDaemonConfigurationError(
            f"{label} parent must be owned by this user and not group/world writable"
        )
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        parent_descriptor = os.open(parent, directory_flags)
    except OSError as exc:
        raise LabDaemonConfigurationError(f"{label} parent could not be opened safely") from exc
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
            current = os.fstat(descriptor)
            _validate_private_regular_identity(current, label=label)
            return candidate
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
        return candidate
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_descriptor)


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
    """Advisory per-daemon process lock retained for the process lifetime."""

    def __init__(self, root: Path, name: str) -> None:
        if re.fullmatch(r"[a-z][a-z0-9-]{0,63}", name) is None:
            raise ValueError("daemon lock name is invalid")
        self.root = Path(root)
        self.name = name
        self.path = self.root / f"{name}.lock"
        self._descriptor = -1

    def acquire(self) -> None:
        if self._descriptor >= 0:
            raise RuntimeError("daemon lock is already acquired")
        existed = self.root.exists()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not existed:
            self.root.chmod(0o700)
        root_stat = self.root.lstat()
        if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
            raise LabDaemonConfigurationError("daemon lock root must be a real directory")
        if root_stat.st_uid != os.getuid() or root_stat.st_mode & 0o077:
            raise LabDaemonConfigurationError("daemon lock root must have private permissions")
        try:
            observed = self.path.lstat()
        except FileNotFoundError:
            observed = None
        if observed is None:
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(self.path, flags, 0o600)
            except OSError as exc:
                raise LabDaemonConfigurationError(
                    "daemon lock file could not be created atomically"
                ) from exc
            created = True
        else:
            _validate_private_regular_identity(observed, label="daemon lock file")
            flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(self.path, flags)
            except OSError as exc:
                raise LabDaemonConfigurationError(
                    "daemon lock file could not be opened safely"
                ) from exc
            created = False
        try:
            if created:
                os.fchmod(descriptor, 0o600)
            current = os.fstat(descriptor)
            if observed is not None and (current.st_dev, current.st_ino) != (
                observed.st_dev,
                observed.st_ino,
            ):
                raise LabDaemonConfigurationError("daemon lock file changed during validation")
            _validate_private_regular_identity(current, label="daemon lock file")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise LabDaemonConfigurationError(
                    f"lab {self.name} daemon is already running"
                ) from exc
            os.ftruncate(descriptor, 0)
            os.write(descriptor, f"{os.getpid()}\n".encode("ascii"))
            os.fsync(descriptor)
        except BaseException:
            os.close(descriptor)
            raise
        self._descriptor = descriptor

    def release(self) -> None:
        if self._descriptor < 0:
            return
        descriptor, self._descriptor = self._descriptor, -1
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def __enter__(self) -> LabDaemonLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


class _FinalizationCandidate(Protocol):
    job_id: UUID


class _FinalizationPage(Protocol):
    items: tuple[_FinalizationCandidate, ...]


class _FinalizationReader(Protocol):
    def list_finalization_candidates(self, *, limit: int) -> _FinalizationPage: ...


class _FinalizationResult(Protocol):
    status: str


class _Finalizer(Protocol):
    def finalize(self, job_id: UUID) -> _FinalizationResult: ...


class LabFinalizerTickResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidates: int = Field(ge=0)
    published: int = Field(default=0, ge=0)
    acknowledged: int = Field(default=0, ge=0)
    rejected: int = Field(default=0, ge=0)
    not_ready: int = Field(default=0, ge=0)
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
        max_jobs_per_tick: int,
        poll_interval_ms: int,
    ) -> None:
        if not 1 <= max_jobs_per_tick <= 128:
            raise ValueError("max_jobs_per_tick must be between 1 and 128")
        if poll_interval_ms < 1:
            raise ValueError("poll_interval_ms must be positive")
        self.reader = reader
        self.finalizer = finalizer
        self.max_jobs_per_tick = max_jobs_per_tick
        self.poll_interval_ms = poll_interval_ms
        self._stop = Event()

    def request_stop(self) -> None:
        self._stop.set()

    def run_once(self) -> LabFinalizerTickResult:
        page = self.reader.list_finalization_candidates(limit=self.max_jobs_per_tick)
        counts = {
            "published": 0,
            "acknowledged": 0,
            "rejected": 0,
            "not_ready": 0,
        }
        failed = 0
        first_error_type: str | None = None
        first_error_message: str | None = None
        for candidate in page.items:
            if self._stop.is_set():
                break
            try:
                result = self.finalizer.finalize(candidate.job_id)
                if result.status not in counts:
                    raise RuntimeError(f"unknown finalizer status: {result.status}")
                counts[result.status] += 1
            except Exception as exc:
                failed += 1
                if first_error_type is None:
                    first_error_type = type(exc).__name__
                    first_error_message = " ".join((str(exc) or type(exc).__name__).split())[:400]
                logger.exception(
                    "lab-finalizer candidate failed: job_id={} error_type={}",
                    candidate.job_id,
                    type(exc).__name__,
                )
        return LabFinalizerTickResult(
            candidates=len(page.items),
            failed=failed,
            first_error_type=first_error_type,
            first_error_message=first_error_message,
            **counts,
        )

    def run_forever(self) -> None:
        while not self._stop.is_set():
            result = self.run_once()
            logger.info("lab-finalizer tick: {}", result.model_dump_json())
            self._stop.wait(self.poll_interval_ms / 1_000)
