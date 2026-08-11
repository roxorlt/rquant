"""Fail-closed runtime primitives for Strategy Lab background daemons."""

from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import math
import os
import re
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol, TypeVar
from uuid import UUID, uuid4

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_validator

from rquant.job_center_authority import (
    JobCenterAuthorityIntegrityError,
    load_job_center_authority,
)
from rquant.job_center_authority import (
    JobCenterAuthorityManifest as LabJobCenterAuthorityManifest,
)
from rquant.lab_artifact_protocol import LabFinalizerAuthorityKey
from rquant.lab_jobs import LabIntegrityDegradedError
from rquant.runtime_code_attestation import CodeTrustEvidence

if TYPE_CHECKING:
    from rquant.lab_job_center import ExperimentLifecycleCoordinator
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader


def _load_strict_json() -> tuple[
    type[ValueError],
    Callable[[str | bytes | bytearray], object],
    Callable[..., object],
    Callable[..., object],
    Callable[..., bytes],
    Callable[..., bytes],
]:
    path = Path(__file__).resolve().parents[2] / "scripts" / "strict_json.py"
    spec = importlib.util.spec_from_file_location("_rquant_lab_strict_json", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("strict JSON authority cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return (
        module.StrictJsonError,
        module.strict_json_loads,
        module.strict_canonical_json_loads,
        module.strict_model_validate_canonical_json,
        module.canonical_model_json_bytes,
        module.canonical_json_bytes,
    )


(
    StrictJsonError,
    strict_json_loads,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
    canonical_model_json_bytes,
    canonical_json_bytes,
) = _load_strict_json()

_CODE_SHA = re.compile(r"^[0-9a-f]{40}$")
_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_LAB_RUNTIME_PREPARED_SCHEMA_VERSION = 2
_LAB_RUNTIME_PREPARED_FILENAME = ".prepared.json"
_LAB_RUNTIME_PREPARED_LOCK_FILENAME = ".prepared.lock"
_LAB_RUNTIME_PREPARED_MAX_BYTES = 1024 * 1024


class LabDaemonConfigurationError(RuntimeError):
    """A daemon cannot start without weakening its trust boundary."""


def load_lab_job_center_authority_manifest(
    path: Path,
    *,
    expected_code_sha: str,
    expected_research_root: Path,
    expected_lab_jobs_path: Path,
    expected_command_spool_path: Path,
    expected_final_artifact_root: Path,
    expected_runtime_deployment_root: Path | None = None,
    expected_deployment_profile_id: str | None = None,
    expected_deployment_generation_hash: str | None = None,
) -> LabJobCenterAuthorityManifest:
    """Compatibility wrapper over the content-bound authority loader."""

    try:
        return load_job_center_authority(
            path,
            expected_code_sha=expected_code_sha,
            runtime_root=expected_research_root,
            lab_jobs_path=expected_lab_jobs_path,
            command_spool_path=expected_command_spool_path,
            final_artifact_root=expected_final_artifact_root,
            runtime_deployment_root=expected_runtime_deployment_root,
            deployment_profile_id=expected_deployment_profile_id,
            deployment_generation_hash=expected_deployment_generation_hash,
        )
    except (JobCenterAuthorityIntegrityError, OSError, ValueError) as exc:
        raise LabDaemonConfigurationError("Job Center authority manifest is invalid") from exc


def build_experiment_lifecycle_coordinator(
    manifest: LabJobCenterAuthorityManifest,
    *,
    reader: LabJobReader | None = None,
    spool: LabCommandSpool | None = None,
    clock: Callable[[], datetime] | None = None,
) -> ExperimentLifecycleCoordinator:
    """Compose the writer-side Experiment lifecycle from the verified manifest."""

    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.lab_job_center import (
        ExperimentLifecycleCoordinator,
        LabCommandSubmissionFacade,
    )
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader
    from rquant.runtime_artifact_terminal_lifecycle import (
        build_production_artifact_terminal_lifecycle,
    )
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

    authority = LabJobCenterAuthorityManifest.model_validate(manifest)
    selected_reader = reader or LabJobReader(authority.lab_jobs_path)
    selected_spool = spool or LabCommandSpool(authority.command_spool_path)
    terminal_lifecycle = build_production_artifact_terminal_lifecycle(
        runtime_root=authority.runtime_deployment_root,
        experiment_registry_path=authority.experiment_registry_path,
    )
    registry = terminal_lifecycle.experiment_registry
    definitions = ImmutableDefinitionRegistry(
        authority.definition_registry_root,
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=authority.code_sha
        ).trusted_executable_registry(),
    )
    facade = LabCommandSubmissionFacade(
        reader=selected_reader,
        spool=selected_spool,
        experiment_registry=registry,
        definition_registry=definitions,
        clock=clock,
    )
    return ExperimentLifecycleCoordinator(facade)


class LabDaemonReadiness(BaseModel):
    """One generation-bound heartbeat published by a formal Lab daemon."""

    model_config = ConfigDict(frozen=True, allow_inf_nan=False)

    label: str
    pid: int = Field(gt=0)
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    environment_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    started_at: datetime
    heartbeat_at: datetime
    heartbeat_monotonic: float = Field(ge=0)
    generation_lock_device: int = Field(ge=0)
    generation_lock_inode: int = Field(gt=0)

    @field_validator("started_at", "heartbeat_at")
    @classmethod
    def _aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("readiness timestamps must be timezone-aware")
        return value.astimezone(UTC)


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
    _validate_private_directory_identity(observed, label=label)
    return candidate


def ensure_private_directory(
    path: Path,
    *,
    label: str,
    mutation_guard: Callable[[], object] | None = None,
) -> Path:
    """Create one validated runtime root after Settings completed pure validation."""
    candidate = _canonical_absolute_path(path, label=label)
    if candidate.resolve(strict=False) != candidate:
        raise LabDaemonConfigurationError(f"{label} path must not use symlink aliases")
    if candidate.exists() or candidate.is_symlink():
        return require_private_directory(candidate, label=label)
    try:
        if mutation_guard is not None:
            mutation_guard()
        candidate.mkdir(parents=True, mode=0o700, exist_ok=False)
    except OSError as exc:
        raise LabDaemonConfigurationError(f"{label} could not be created safely") from exc
    return require_private_directory(candidate, label=label)


def lab_runtime_prepared_path(runtime_root: Path) -> Path:
    root = _canonical_absolute_path(runtime_root, label="lab runtime root")
    return root / _LAB_RUNTIME_PREPARED_FILENAME


def _runtime_identity_payload(path: Path, observed: os.stat_result) -> dict[str, object]:
    return {
        "path": str(path),
        "device": observed.st_dev,
        "inode": observed.st_ino,
        "mode": stat.S_IMODE(observed.st_mode),
    }


def _filesystem_identity(observed: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
        observed.st_nlink,
    )


def _directory_filesystem_identity(observed: os.stat_result) -> tuple[int, int, int, int]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
    )


class _TrustedRuntimeRoot:
    """Descriptor-bound, no-symlink authority for one declared runtime root."""

    def __init__(
        self,
        *,
        path: Path,
        descriptors: list[int],
        names: list[str],
        identities: list[tuple[int, int, int, int]],
    ) -> None:
        self.path = path
        self._descriptors = descriptors
        self._names = names
        self._identities = identities

    @classmethod
    def open(cls, path: Path) -> _TrustedRuntimeRoot:
        candidate = _canonical_absolute_path(path, label="lab runtime root")
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptors: list[int] = []
        names: list[str] = []
        identities: list[tuple[int, int, int, int]] = []
        try:
            root_fd = os.open(os.sep, flags)
            descriptors.append(root_fd)
            identities.append(_directory_filesystem_identity(os.fstat(root_fd)))
            parent_fd = root_fd
            for name in candidate.parts[1:]:
                active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                if stat.S_ISLNK(active.st_mode) or not stat.S_ISDIR(active.st_mode):
                    raise LabDaemonConfigurationError(
                        "lab runtime root ancestor chain contains a symlink or non-directory"
                    )
                descriptor = os.open(name, flags, dir_fd=parent_fd)
                opened = os.fstat(descriptor)
                if _directory_filesystem_identity(active) != _directory_filesystem_identity(opened):
                    os.close(descriptor)
                    raise LabDaemonConfigurationError("lab runtime root ancestor identity changed")
                descriptors.append(descriptor)
                names.append(name)
                identities.append(_directory_filesystem_identity(opened))
                parent_fd = descriptor
            authority = cls(
                path=candidate,
                descriptors=descriptors,
                names=names,
                identities=identities,
            )
            _validate_private_directory_identity(
                os.fstat(authority.root_fd),
                label="lab runtime root",
            )
            authority.assert_current()
            return authority
        except BaseException as exc:
            for descriptor in reversed(descriptors):
                with suppress(OSError):
                    os.close(descriptor)
            if isinstance(exc, LabDaemonConfigurationError):
                raise
            if isinstance(exc, OSError):
                raise LabDaemonConfigurationError(
                    "lab runtime root ancestor chain could not be opened safely"
                ) from exc
            raise

    @property
    def root_fd(self) -> int:
        if not self._descriptors:
            raise LabDaemonConfigurationError("lab runtime root authority is closed")
        return self._descriptors[-1]

    @property
    def identity(self) -> tuple[int, int]:
        observed = os.fstat(self.root_fd)
        return observed.st_dev, observed.st_ino

    def assert_current(self) -> None:
        if not self._descriptors:
            raise LabDaemonConfigurationError("lab runtime root authority is closed")
        try:
            for index, descriptor in enumerate(self._descriptors):
                opened = os.fstat(descriptor)
                if _directory_filesystem_identity(opened) != self._identities[index]:
                    raise LabDaemonConfigurationError("lab runtime root ancestor identity changed")
                if index == 0:
                    continue
                active = os.stat(
                    self._names[index - 1],
                    dir_fd=self._descriptors[index - 1],
                    follow_symlinks=False,
                )
                if _directory_filesystem_identity(active) != self._identities[index]:
                    raise LabDaemonConfigurationError("lab runtime root ancestor identity changed")
            _validate_private_directory_identity(
                os.fstat(self.root_fd),
                label="lab runtime root",
            )
        except OSError as exc:
            raise LabDaemonConfigurationError("lab runtime root ancestor identity changed") from exc

    def close(self) -> None:
        descriptors, self._descriptors = self._descriptors, []
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _reject_sqlite_sidecars(path: Path, *, label: str) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = path.with_name(f"{path.name}{suffix}")
        if os.path.lexists(sidecar):
            raise LabDaemonConfigurationError(
                f"checkpoint and remove {label} SQLite sidecars before preparing Lab runtime"
            )


def _reject_sqlite_sidecars_at(root_fd: int, name: str, *, label: str) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        try:
            os.stat(f"{name}{suffix}", dir_fd=root_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        raise LabDaemonConfigurationError(
            f"checkpoint and remove {label} SQLite sidecars before preparing Lab runtime"
        )


def _write_runtime_prepared_sentinel(
    root: Path,
    payload: dict[str, object],
    *,
    mutation_guard: Callable[[], object],
    lock_descriptor: int | None = None,
    expected_identity: tuple[int, int] | None = None,
    expected_root_identity: tuple[int, int] | None = None,
    root_authority: _TrustedRuntimeRoot | None = None,
) -> None:
    owned_authority = root_authority is None
    authority = root_authority or _TrustedRuntimeRoot.open(root)
    owned_lock = lock_descriptor is None
    root_fd = -1
    temporary = ""
    descriptor = -1
    try:
        if lock_descriptor is None:
            lock_descriptor = _open_runtime_prepared_lock(
                root,
                create=True,
                root_authority=authority,
            )
        authority.assert_current()
        root_fd = authority.root_fd
        root_observed = os.fstat(root_fd)
        temporary = f".{_LAB_RUNTIME_PREPARED_FILENAME}.{uuid4().hex}.tmp"
        opened_root = os.fstat(root_fd)
        if (
            expected_root_identity is not None
            and (
                root_observed.st_dev,
                root_observed.st_ino,
            )
            != expected_root_identity
        ):
            raise LabDaemonConfigurationError("lab runtime root identity changed")
        if (opened_root.st_dev, opened_root.st_ino) != (
            root_observed.st_dev,
            root_observed.st_ino,
        ):
            raise LabDaemonConfigurationError("lab runtime root identity changed")
        mutation_guard()
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=root_fd,
        )
        encoded = canonical_json_bytes(payload, trailing_newline=True)
        offset = 0
        while offset < len(encoded):
            written = os.write(descriptor, encoded[offset:])
            if written <= 0:
                raise LabDaemonConfigurationError("lab runtime prepared sentinel write failed")
            offset += written
        os.fsync(descriptor)
        _validate_private_regular_identity(
            os.fstat(descriptor),
            label="lab runtime prepared sentinel",
        )
        mutation_guard()
        authority.assert_current()
        if expected_identity is not None:
            try:
                active_sentinel = os.stat(
                    _LAB_RUNTIME_PREPARED_FILENAME,
                    dir_fd=root_fd,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise LabDaemonConfigurationError(
                    "lab runtime prepared sentinel changed during update"
                ) from exc
            if (active_sentinel.st_dev, active_sentinel.st_ino) != expected_identity:
                raise LabDaemonConfigurationError(
                    "lab runtime prepared sentinel changed during update"
                )
        os.replace(
            temporary,
            _LAB_RUNTIME_PREPARED_FILENAME,
            src_dir_fd=root_fd,
            dst_dir_fd=root_fd,
        )
        os.fsync(root_fd)
        authority.assert_current()
    except BaseException:
        if root_fd >= 0 and temporary:
            with suppress(OSError):
                os.unlink(temporary, dir_fd=root_fd)
        raise
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if owned_lock and lock_descriptor is not None:
            os.close(lock_descriptor)
        if owned_authority:
            authority.close()


def _open_runtime_prepared_lock(
    root: Path,
    *,
    create: bool,
    root_authority: _TrustedRuntimeRoot | None = None,
) -> int:
    owned_authority = root_authority is None
    authority = root_authority or _TrustedRuntimeRoot.open(root)
    root_fd = authority.root_fd
    descriptor = -1
    try:
        authority.assert_current()
        flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        if create:
            flags |= os.O_CREAT
        descriptor = os.open(
            _LAB_RUNTIME_PREPARED_LOCK_FILENAME,
            flags,
            0o600,
            dir_fd=root_fd,
        )
        opened = os.fstat(descriptor)
        active = os.stat(
            _LAB_RUNTIME_PREPARED_LOCK_FILENAME,
            dir_fd=root_fd,
            follow_symlinks=False,
        )
        _validate_private_regular_identity(opened, label="lab runtime prepared lock")
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_uid,
            opened.st_nlink,
        ) != (
            active.st_dev,
            active.st_ino,
            active.st_mode,
            active.st_uid,
            active.st_nlink,
        ):
            raise LabDaemonConfigurationError("lab runtime prepared lock identity changed")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        authority.assert_current()
        return descriptor
    except BaseException as exc:
        if descriptor >= 0:
            os.close(descriptor)
        if isinstance(exc, LabDaemonConfigurationError):
            raise
        if not isinstance(exc, OSError):
            raise
        raise LabDaemonConfigurationError("lab runtime prepared lock is unavailable") from exc
    finally:
        if owned_authority:
            authority.close()


def _read_runtime_prepared_sentinel_record(
    root: Path,
    *,
    root_authority: _TrustedRuntimeRoot | None = None,
) -> tuple[dict[str, object], tuple[int, int], tuple[int, int]]:
    candidate = _canonical_absolute_path(root, label="lab runtime root")
    owned_authority = root_authority is None
    authority: _TrustedRuntimeRoot | None = None
    descriptor = -1
    try:
        authority = root_authority or _TrustedRuntimeRoot.open(candidate)
        if authority.path != candidate:
            raise LabDaemonConfigurationError("lab runtime root physical path changed")
        authority.assert_current()
        root_fd = authority.root_fd
        opened_root = os.fstat(root_fd)
        root_identity = (opened_root.st_dev, opened_root.st_ino)
        descriptor = os.open(
            _LAB_RUNTIME_PREPARED_FILENAME,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_fd,
        )
        opened = os.fstat(descriptor)
        _validate_private_regular_identity(opened, label="lab runtime prepared sentinel")
        active_sentinel = os.stat(
            _LAB_RUNTIME_PREPARED_FILENAME,
            dir_fd=root_fd,
            follow_symlinks=False,
        )
        if (opened.st_dev, opened.st_ino) != (
            active_sentinel.st_dev,
            active_sentinel.st_ino,
        ):
            raise LabDaemonConfigurationError("lab runtime prepared sentinel identity changed")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(
                descriptor,
                min(64 * 1024, _LAB_RUNTIME_PREPARED_MAX_BYTES + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > _LAB_RUNTIME_PREPARED_MAX_BYTES:
                raise LabDaemonConfigurationError("lab runtime prepared sentinel is too large")
        authority.assert_current()
        after = os.stat(
            _LAB_RUNTIME_PREPARED_FILENAME,
            dir_fd=root_fd,
            follow_symlinks=False,
        )
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_uid,
            opened.st_nlink,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_uid,
            after.st_nlink,
        ):
            raise LabDaemonConfigurationError("lab runtime prepared sentinel identity changed")
        payload = strict_canonical_json_loads(b"".join(chunks), trailing_newline=True)
        if not isinstance(payload, dict):
            raise LabDaemonConfigurationError("lab runtime prepared sentinel is malformed")
        return payload, (opened.st_dev, opened.st_ino), root_identity
    except LabDaemonConfigurationError:
        raise
    except (FileNotFoundError, OSError, UnicodeError, StrictJsonError) as exc:
        raise LabDaemonConfigurationError("lab runtime prepared sentinel is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if owned_authority and authority is not None:
            authority.close()


def _read_runtime_prepared_sentinel(root: Path) -> dict[str, object]:
    payload, _identity, _root_identity = _read_runtime_prepared_sentinel_record(root)
    return payload


def verify_lab_runtime_prepared(
    runtime_root: Path,
    *,
    checkout_root: Path,
    expected_commit: str,
    managed_directories: Mapping[str, Path],
    managed_files: Mapping[str, Path],
    legacy_paths: Mapping[Path, Path],
    allow_missing_files: frozenset[str] = frozenset(),
) -> dict[str, object]:
    root = require_private_directory(runtime_root, label="lab runtime root")
    checkout = _canonical_absolute_path(checkout_root, label="checkout root")
    root_observed = root.lstat()
    payload = _read_runtime_prepared_sentinel(root)
    recorded_files = payload.get("managed_files")
    expected_directories: dict[str, dict[str, object]] = {}
    for label, raw_path in managed_directories.items():
        path = require_private_directory(raw_path, label=label)
        if path.parent != root:
            raise LabDaemonConfigurationError(f"{label} must be inside lab runtime root")
        expected_directories[label] = _runtime_identity_payload(path, path.lstat())
    expected_files: dict[str, dict[str, object]] = {}
    for label, raw_path in managed_files.items():
        path = _canonical_absolute_path(raw_path, label=label)
        if path.parent != root:
            raise LabDaemonConfigurationError(f"{label} must be inside lab runtime root")
        if "sqlite" in label.casefold() or path.suffix.casefold() in {".db", ".sqlite3"}:
            _reject_sqlite_sidecars(path, label=label)
        recorded = recorded_files.get(label) if isinstance(recorded_files, dict) else None
        if not isinstance(recorded, dict):
            raise LabDaemonConfigurationError("lab runtime prepared sentinel file binding changed")
        if recorded.get("exists") is False:
            if os.path.lexists(path):
                raise LabDaemonConfigurationError(
                    f"{label} exists but is not registered in the prepared sentinel"
                )
            if label not in allow_missing_files:
                raise LabDaemonConfigurationError(
                    f"{label} is not initialized in the prepared sentinel"
                )
            expected_files[label] = {"path": str(path), "exists": False}
        elif os.path.lexists(path):
            observed = path.lstat()
            _validate_private_regular_identity(observed, label=label)
            expected_files[label] = {
                **_runtime_identity_payload(path, observed),
                "exists": True,
            }
        else:
            raise LabDaemonConfigurationError(f"{label} registered file is unavailable")
    migration_sources: dict[str, dict[str, object]] = {}
    for target, raw_source in legacy_paths.items():
        source = _canonical_absolute_path(raw_source, label=f"legacy {target.name}")
        if os.path.lexists(source):
            raise LabDaemonConfigurationError(f"legacy {source.name} still exists")
        migration_sources[str(target)] = {
            "source": str(source),
        }
    expected = {
        "schema_version": _LAB_RUNTIME_PREPARED_SCHEMA_VERSION,
        "checkout_root": (
            payload.get("checkout_root")
            if checkout.name == "release" and checkout.parent.name != ""
            else str(checkout)
        ),
        "runtime_root": str(root),
        "runtime_device": root_observed.st_dev,
        "runtime_inode": root_observed.st_ino,
        "managed_directories": expected_directories,
        "managed_files": expected_files,
    }
    if _CODE_SHA.fullmatch(expected_commit) is None:
        raise LabDaemonConfigurationError("lab runtime release commit must be a full SHA")
    authority_id = payload.get("runtime_authority_id")
    if not isinstance(authority_id, str) or re.fullmatch(r"[0-9a-f]{32}", authority_id) is None:
        raise LabDaemonConfigurationError("lab runtime prepared authority is invalid")
    if any(payload.get(key) != value for key, value in expected.items()):
        raise LabDaemonConfigurationError("lab runtime prepared sentinel binding changed")
    recorded_sources = payload.get("migration_sources")
    if not isinstance(recorded_sources, dict) or set(recorded_sources) != set(migration_sources):
        raise LabDaemonConfigurationError("lab runtime prepared sentinel migration binding changed")
    for target, expected_source in migration_sources.items():
        recorded = recorded_sources.get(target)
        if (
            not isinstance(recorded, dict)
            or set(recorded) != {"source", "migrated"}
            or recorded.get("source") != expected_source["source"]
            or not isinstance(recorded.get("migrated"), bool)
        ):
            raise LabDaemonConfigurationError(
                "lab runtime prepared sentinel migration binding changed"
            )
    return payload


def register_lab_runtime_managed_file(
    runtime_root: Path,
    *,
    label: str,
    path: Path,
    mutation_guard: Callable[[], object],
    owner: str = "scheduler",
) -> dict[str, object]:
    """Atomically bind the first scheduler-created SQLite inode to runtime authority."""
    if owner != "scheduler":
        raise LabDaemonConfigurationError("only the scheduler owner may register Lab files")
    root = _canonical_absolute_path(runtime_root, label="lab runtime root")
    candidate = _canonical_absolute_path(path, label=label)
    if candidate.parent != root:
        raise LabDaemonConfigurationError(f"{label} must be inside lab runtime root")
    authority = _TrustedRuntimeRoot.open(root)
    lock_descriptor = -1
    database_descriptor = -1
    try:
        authority.assert_current()
        _reject_sqlite_sidecars_at(authority.root_fd, candidate.name, label=label)
        lock_descriptor = _open_runtime_prepared_lock(
            root,
            create=False,
            root_authority=authority,
        )
        payload, sentinel_identity, root_identity = _read_runtime_prepared_sentinel_record(
            root,
            root_authority=authority,
        )
        if authority.identity != (
            payload.get("runtime_device"),
            payload.get("runtime_inode"),
        ):
            raise LabDaemonConfigurationError("lab runtime prepared root identity changed")
        files = payload.get("managed_files")
        recorded = files.get(label) if isinstance(files, dict) else None
        if recorded != {"path": str(candidate), "exists": False}:
            raise LabDaemonConfigurationError(
                f"{label} cannot be registered from its current prepared state"
            )
        try:
            database_descriptor = os.open(
                candidate.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=authority.root_fd,
            )
            observed = os.fstat(database_descriptor)
            active = os.stat(
                candidate.name,
                dir_fd=authority.root_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise LabDaemonConfigurationError(f"{label} could not be opened safely") from exc
        _validate_private_regular_identity(observed, label=label)
        _validate_private_regular_identity(active, label=label)
        if _filesystem_identity(observed) != _filesystem_identity(active):
            raise LabDaemonConfigurationError(f"{label} identity changed before registration")
        mutation_guard()
        authority.assert_current()
        current = os.fstat(database_descriptor)
        active = os.stat(
            candidate.name,
            dir_fd=authority.root_fd,
            follow_symlinks=False,
        )
        _validate_private_regular_identity(current, label=label)
        _validate_private_regular_identity(active, label=label)
        if _filesystem_identity(current) != _filesystem_identity(observed) or _filesystem_identity(
            active
        ) != _filesystem_identity(observed):
            raise LabDaemonConfigurationError(f"{label} identity changed before registration")
        updated_files = dict(files)
        updated_files[label] = {
            **_runtime_identity_payload(candidate, current),
            "exists": True,
        }
        updated = {**payload, "managed_files": updated_files}
        _write_runtime_prepared_sentinel(
            root,
            updated,
            mutation_guard=mutation_guard,
            lock_descriptor=lock_descriptor,
            expected_identity=sentinel_identity,
            expected_root_identity=root_identity,
            root_authority=authority,
        )
        return updated
    finally:
        if database_descriptor >= 0:
            os.close(database_descriptor)
        if lock_descriptor >= 0:
            os.close(lock_descriptor)
        authority.close()


def prepare_lab_runtime_sqlite_authority(
    runtime_root: Path,
    *,
    label: str,
    path: Path,
    mutation_guard: Callable[[], object],
    owner: str = "scheduler",
) -> LabSqliteAuthority:
    """Open or first-create/register SQLite under one retained runtime-root authority."""
    if owner != "scheduler":
        raise LabDaemonConfigurationError("only the scheduler owner may prepare Lab SQLite")
    root = _canonical_absolute_path(runtime_root, label="lab runtime root")
    candidate = _canonical_absolute_path(path, label=label)
    if candidate.parent != root:
        raise LabDaemonConfigurationError(f"{label} must be inside lab runtime root")
    authority = _TrustedRuntimeRoot.open(root)
    lock_descriptor = -1
    sqlite_authority: LabSqliteAuthority | None = None
    try:
        authority.assert_current()
        lock_descriptor = _open_runtime_prepared_lock(
            root,
            create=False,
            root_authority=authority,
        )
        payload, sentinel_identity, root_identity = _read_runtime_prepared_sentinel_record(
            root,
            root_authority=authority,
        )
        if authority.identity != (
            payload.get("runtime_device"),
            payload.get("runtime_inode"),
        ):
            raise LabDaemonConfigurationError("lab runtime prepared root identity changed")
        files = payload.get("managed_files")
        recorded = files.get(label) if isinstance(files, dict) else None
        if not isinstance(recorded, dict) or recorded.get("path") != str(candidate):
            raise LabDaemonConfigurationError(f"{label} prepared sentinel registration is invalid")
        needs_registration = recorded == {"path": str(candidate), "exists": False}
        if not needs_registration and recorded.get("exists") is not True:
            raise LabDaemonConfigurationError(f"{label} prepared sentinel registration is invalid")
        _reject_sqlite_sidecars_at(authority.root_fd, candidate.name, label=label)

        def guarded_mutation() -> object:
            authority.assert_current()
            result = mutation_guard()
            authority.assert_current()
            return result

        guarded_mutation()
        parent_descriptor = os.dup(authority.root_fd)
        sqlite_authority = _prepare_private_sqlite_from_parent(
            candidate,
            label=label,
            create=needs_registration,
            mutation_guard=guarded_mutation if needs_registration else None,
            parent_descriptor=parent_descriptor,
            parent_identity=os.fstat(parent_descriptor),
        )
        authority.assert_current()
        observed = os.fstat(sqlite_authority._database_descriptor)
        if needs_registration:
            updated_files = dict(files)
            updated_files[label] = {
                **_runtime_identity_payload(candidate, observed),
                "exists": True,
            }
            _write_runtime_prepared_sentinel(
                root,
                {**payload, "managed_files": updated_files},
                mutation_guard=guarded_mutation,
                lock_descriptor=lock_descriptor,
                expected_identity=sentinel_identity,
                expected_root_identity=root_identity,
                root_authority=authority,
            )
        elif recorded != {
            **_runtime_identity_payload(candidate, observed),
            "exists": True,
        }:
            raise LabDaemonConfigurationError(f"{label} prepared identity changed")
        result, sqlite_authority = sqlite_authority, None
        return result
    except BaseException:
        if sqlite_authority is not None:
            try:
                sqlite_authority.discard_created()
            finally:
                sqlite_authority.close()
                sqlite_authority = None
        raise
    finally:
        if sqlite_authority is not None:
            sqlite_authority.close()
        if lock_descriptor >= 0:
            os.close(lock_descriptor)
        authority.close()


def prepare_lab_runtime_layout(
    runtime_root: Path,
    *,
    checkout_root: Path,
    managed_directories: Mapping[str, Path],
    managed_files: Mapping[str, Path],
    legacy_paths: Mapping[Path, Path],
    mutation_guard: Callable[[], object],
) -> Path:
    root = ensure_private_directory(
        runtime_root,
        label="lab runtime root",
        mutation_guard=mutation_guard,
    )
    targets = {**managed_directories, **managed_files}
    migration_payload: dict[str, dict[str, object]] = {}
    for label, target_value in targets.items():
        target = _canonical_absolute_path(target_value, label=label)
        if target.parent != root:
            raise LabDaemonConfigurationError(f"{label} must be a direct child of lab runtime root")
        legacy = legacy_paths.get(target)
        if legacy is None:
            continue
        legacy = _canonical_absolute_path(legacy, label=f"legacy {label}")
        migration_payload[str(target)] = {"source": str(legacy), "migrated": False}
        target_exists = os.path.lexists(target)
        legacy_exists = os.path.lexists(legacy)
        if target_exists and legacy_exists:
            raise LabDaemonConfigurationError(f"legacy and target {label} both exist")
        if target_exists or not legacy_exists:
            continue
        parent_observed = legacy.parent.lstat()
        if (
            not stat.S_ISDIR(parent_observed.st_mode)
            or stat.S_ISLNK(parent_observed.st_mode)
            or parent_observed.st_uid != os.getuid()
            or parent_observed.st_mode & 0o022
        ):
            raise LabDaemonConfigurationError(f"legacy {label} parent is unsafe")
        legacy_observed = legacy.lstat()
        if label in managed_directories:
            _validate_private_directory_identity(legacy_observed, label=f"legacy {label}")
        else:
            _validate_private_regular_identity(legacy_observed, label=f"legacy {label}")
        source_fd = os.open(
            legacy.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        target_fd = os.open(
            root,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            mutation_guard()
            active = os.stat(legacy.name, dir_fd=source_fd, follow_symlinks=False)
            if (
                active.st_dev,
                active.st_ino,
                active.st_mode,
                active.st_uid,
                active.st_nlink,
            ) != (
                legacy_observed.st_dev,
                legacy_observed.st_ino,
                legacy_observed.st_mode,
                legacy_observed.st_uid,
                legacy_observed.st_nlink,
            ):
                raise LabDaemonConfigurationError(f"legacy {label} identity changed")
            if label in managed_files and (
                "sqlite" in label.casefold() or legacy.suffix.casefold() in {".db", ".sqlite3"}
            ):
                for suffix in ("-wal", "-shm", "-journal"):
                    try:
                        os.stat(
                            f"{legacy.name}{suffix}",
                            dir_fd=source_fd,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        continue
                    raise LabDaemonConfigurationError(
                        "checkpoint and remove legacy SQLite sidecars before migration"
                    )
            os.rename(
                legacy.name,
                target.name,
                src_dir_fd=source_fd,
                dst_dir_fd=target_fd,
            )
            os.fsync(source_fd)
            os.fsync(target_fd)
            migration_payload[str(target)]["migrated"] = True
        except OSError as exc:
            raise LabDaemonConfigurationError(f"legacy {label} could not be migrated") from exc
        finally:
            os.close(target_fd)
            os.close(source_fd)
    for label, path in managed_directories.items():
        ensure_private_directory(path, label=label, mutation_guard=mutation_guard)
    for label, path in managed_files.items():
        candidate = _canonical_absolute_path(path, label=label)
        if "sqlite" in label.casefold() or candidate.suffix.casefold() in {".db", ".sqlite3"}:
            _reject_sqlite_sidecars(candidate, label=label)
        if os.path.lexists(path):
            _validate_private_regular_identity(path.lstat(), label=label)
    prepared_commit = str(mutation_guard())
    if _CODE_SHA.fullmatch(prepared_commit) is None:
        raise LabDaemonConfigurationError("lab runtime prepared commit must be a full SHA")
    checkout = _canonical_absolute_path(checkout_root, label="checkout root")
    root_observed = root.lstat()
    directory_payload = {
        label: _runtime_identity_payload(Path(path), Path(path).lstat())
        for label, path in managed_directories.items()
    }
    file_payload: dict[str, dict[str, object]] = {}
    for label, raw_path in managed_files.items():
        path = Path(raw_path)
        if os.path.lexists(path):
            file_payload[label] = {
                **_runtime_identity_payload(path, path.lstat()),
                "exists": True,
            }
        else:
            file_payload[label] = {"path": str(path), "exists": False}
    _write_runtime_prepared_sentinel(
        root,
        {
            "schema_version": _LAB_RUNTIME_PREPARED_SCHEMA_VERSION,
            "checkout_root": str(checkout),
            "runtime_root": str(root),
            "runtime_device": root_observed.st_dev,
            "runtime_inode": root_observed.st_ino,
            "runtime_authority_id": uuid4().hex,
            "prepared_by_commit": prepared_commit,
            "managed_directories": directory_payload,
            "managed_files": file_payload,
            "migration_sources": migration_payload,
            "prepared_at": datetime.now(UTC).isoformat(),
        },
        mutation_guard=mutation_guard,
    )
    return root


def require_unique_runtime_paths(paths: Mapping[str, Path]) -> None:
    """Reject distinct configured paths that resolve to one live filesystem object."""
    identities: dict[tuple[int, int], tuple[str, Path]] = {}
    for label, raw_path in paths.items():
        candidate = _canonical_absolute_path(raw_path, label=label)
        try:
            observed = candidate.lstat()
        except OSError as exc:
            raise LabDaemonConfigurationError(f"{label} path is unavailable") from exc
        if stat.S_ISLNK(observed.st_mode):
            raise LabDaemonConfigurationError(f"{label} path must not be a symlink")
        if observed.st_uid != os.getuid():
            raise LabDaemonConfigurationError(f"{label} path must be owned by this user")
        identity = observed.st_dev, observed.st_ino
        prior = identities.get(identity)
        if prior is not None:
            prior_label, prior_path = prior
            raise LabDaemonConfigurationError(
                "lab runtime paths share the same filesystem identity: "
                f"{prior_label}={prior_path} <> {label}={candidate}"
            )
        identities[identity] = (label, candidate)


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
    expected_runtime_prefix: Path | None = None,
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
        runtime_generation = (
            expected_venv
            if expected_runtime_prefix is None
            else _canonical_absolute_path(
                expected_runtime_prefix,
                label="expected runtime generation",
            )
        )
        runtime_generation_stat = runtime_generation.lstat()
        if (
            not stat.S_ISDIR(runtime_generation_stat.st_mode)
            or stat.S_ISLNK(runtime_generation_stat.st_mode)
            or runtime_generation_stat.st_uid != os.getuid()
            or (expected_runtime_prefix is not None and runtime_generation_stat.st_mode & 0o077)
            or runtime_generation.resolve(strict=True) != runtime_generation
        ):
            raise LabDaemonConfigurationError("lab runtime generation is unsafe")
        expected_launcher = runtime_generation / "bin" / "rquant"
        expected_package_root = (expected / "src" / "rquant").resolve(strict=True)
        runtime_git_root = Path(git_top_level).resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise LabDaemonConfigurationError(
            "lab runtime binding contains a missing or unsafe path"
        ) from exc
    if runtime_cwd != expected:
        raise LabDaemonConfigurationError("lab runtime binding working directory mismatch")
    if (
        not runtime_executable.is_relative_to(runtime_generation)
        or runtime_executable.parent.name != "bin"
        or not runtime_executable.name.startswith("python")
    ):
        raise LabDaemonConfigurationError("lab runtime binding executable mismatch")
    if runtime_launcher != expected_launcher:
        raise LabDaemonConfigurationError("lab runtime binding launcher mismatch")
    if runtime_package_root != expected_package_root:
        raise LabDaemonConfigurationError("lab runtime binding package root mismatch")
    if runtime_prefix != runtime_generation:
        raise LabDaemonConfigurationError("lab runtime binding virtualenv prefix mismatch")
    if runtime_console_interpreter != runtime_executable:
        raise LabDaemonConfigurationError("lab runtime binding console shebang mismatch")
    if runtime_git_root != expected:
        raise LabDaemonConfigurationError("lab runtime binding Git top-level mismatch")
    if _CODE_SHA.fullmatch(verified_code_sha) is None or git_head != verified_code_sha:
        raise LabDaemonConfigurationError("lab runtime binding verified SHA mismatch")
    return verified_code_sha


def _verify_deployment_generation(
    *,
    expected_checkout_root: Path,
    expected_generation: str,
    lock_path: Path,
    lock_fd: int,
) -> None:
    candidate = _canonical_absolute_path(lock_path, label="deployment generation lock")
    environment_root = candidate.with_name(f"{candidate.stem}.venvs")
    immutable_code_root = (
        expected_checkout_root.name == "release"
        and expected_checkout_root.parent.parent == environment_root
    )
    if not immutable_code_root:
        expected_lock = (
            expected_checkout_root.parent / ".rquant-deploy" / f"{expected_checkout_root.name}.lock"
        )
        if candidate != expected_lock:
            raise LabDaemonConfigurationError("deployment generation lock path mismatch")
    if _CODE_SHA.fullmatch(expected_generation) is None or lock_fd < 0:
        raise LabDaemonConfigurationError("deployment generation binding is invalid")
    try:
        opened = os.fstat(lock_fd)
        active = candidate.lstat()
        fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except (OSError, BlockingIOError) as exc:
        raise LabDaemonConfigurationError("deployment generation lock is unavailable") from exc
    if (
        (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid, opened.st_nlink)
        != (active.st_dev, active.st_ino, active.st_mode, active.st_uid, active.st_nlink)
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.getuid()
        or opened.st_nlink != 1
        or stat.S_IMODE(opened.st_mode) != 0o600
    ):
        raise LabDaemonConfigurationError("deployment generation lock identity changed")


def require_legacy_checkout_runtime_binding(
    expected_checkout_root: Path,
    trusted_git_path: Path = Path("/usr/bin/git"),
    *,
    deployment_generation: str | None = None,
    deployment_lock_path: Path | None = None,
    deployment_generation_fd: int | None = None,
    startup_deadline_monotonic: float | None = None,
) -> str:
    """Developer-only checkout binding retained for exploratory migration."""
    binding_deadline = (
        startup_deadline_monotonic
        if startup_deadline_monotonic is not None
        else time.monotonic() + 3
    )
    if not math.isfinite(binding_deadline) or time.monotonic() >= binding_deadline:
        raise LabDaemonConfigurationError("Lab startup deadline is invalid or expired")
    expected_candidate = _canonical_absolute_path(
        expected_checkout_root,
        label="expected checkout root",
    )
    if (
        deployment_generation is not None
        and deployment_lock_path is not None
        and deployment_generation_fd is not None
        and expected_candidate.name == "release"
        and expected_candidate.parent.parent
        == Path(deployment_lock_path).with_name(f"{Path(deployment_lock_path).stem}.venvs")
    ):
        return _require_immutable_lab_runtime_binding(
            expected_candidate,
            trusted_git_path=trusted_git_path,
            deployment_generation=deployment_generation,
            deployment_lock_path=Path(deployment_lock_path),
            deployment_generation_fd=int(deployment_generation_fd),
            startup_deadline_monotonic=binding_deadline,
        )
    expected, _expected_venv = _require_physical_checkout_virtualenv(
        expected_checkout_root,
    )
    generation_values = (
        deployment_generation,
        deployment_lock_path,
        deployment_generation_fd,
    )
    if any(value is not None for value in generation_values):
        if any(value is None for value in generation_values):
            raise LabDaemonConfigurationError("deployment generation binding is incomplete")
        _verify_deployment_generation(
            expected_checkout_root=expected,
            expected_generation=str(deployment_generation),
            lock_path=Path(deployment_lock_path),
            lock_fd=int(deployment_generation_fd),
        )
    import rquant
    from rquant.research_manifest import (
        _run_trusted_git,
        bind_trusted_git_executable,
        detect_verified_code_commit,
    )

    try:
        trusted_git = bind_trusted_git_executable(trusted_git_path)
        top_level_result = _run_trusted_git(
            trusted_git,
            ["rev-parse", "--show-toplevel"],
            cwd=expected,
            deadline_monotonic=binding_deadline,
        )
        head_result = _run_trusted_git(
            trusted_git,
            ["rev-parse", "HEAD"],
            cwd=expected,
            deadline_monotonic=binding_deadline,
        )
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
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
        expected_runtime_prefix=(Path(sys.prefix) if deployment_generation is not None else None),
    )
    injected_sha = os.getenv("RQUANT_CODE_COMMIT", "").strip()
    if injected_sha and injected_sha != git_head:
        raise LabDaemonConfigurationError("lab runtime binding injected SHA mismatch")
    verified = require_clean_code_sha(
        lambda: detect_verified_code_commit(
            expected,
            trusted_git_path=trusted_git.path,
            deadline_monotonic=binding_deadline,
        )
    )
    if verified != git_head:
        raise LabDaemonConfigurationError("lab runtime binding verified SHA mismatch")
    if deployment_generation is not None:
        expected_environment_root = Path(deployment_lock_path).with_name(
            f"{Path(deployment_lock_path).stem}.venvs"
        )
        if Path(sys.prefix).parent != expected_environment_root:
            raise LabDaemonConfigurationError("deployment environment selector mismatch")
        if verified != deployment_generation:
            raise LabDaemonConfigurationError("deployment generation SHA mismatch")
        _verify_deployment_generation(
            expected_checkout_root=expected,
            expected_generation=deployment_generation,
            lock_path=Path(deployment_lock_path),
            lock_fd=int(deployment_generation_fd),
        )
    return verified


def require_lab_runtime_binding(
    capability: object,
) -> CodeTrustEvidence:
    """Accept only a verified immutable-generation capability for formal Lab."""

    from rquant.runtime_code_generation import RuntimeCodeGenerationCapability

    if not isinstance(capability, RuntimeCodeGenerationCapability):
        raise LabDaemonConfigurationError(
            "formal Lab runtime requires an attested generation capability"
        )
    try:
        capability.require_live()
    except Exception as exc:
        raise LabDaemonConfigurationError("formal Lab runtime capability is invalid") from exc
    return capability.evidence


def _require_immutable_lab_runtime_binding(
    code_root: Path,
    *,
    trusted_git_path: Path,
    deployment_generation: str,
    deployment_lock_path: Path,
    deployment_generation_fd: int,
    startup_deadline_monotonic: float | None,
) -> str:
    import rquant
    from rquant.release_generation import ReleaseGenerationAuthority
    from rquant.research_manifest import bind_trusted_git_executable

    if startup_deadline_monotonic is None:
        raise LabDaemonConfigurationError("Lab authority deadline binding is missing")
    authority_deadline = startup_deadline_monotonic
    if not math.isfinite(authority_deadline) or time.monotonic() >= authority_deadline:
        raise LabDaemonConfigurationError("Lab startup deadline is invalid or expired")
    if _CODE_SHA.fullmatch(deployment_generation) is None:
        raise LabDaemonConfigurationError("deployment generation SHA mismatch")
    try:
        observed_root = code_root.lstat()
        if (
            not stat.S_ISDIR(observed_root.st_mode)
            or stat.S_ISLNK(observed_root.st_mode)
            or observed_root.st_uid != os.getuid()
            or observed_root.st_mode & 0o077
            or code_root.resolve(strict=True) != code_root
        ):
            raise LabDaemonConfigurationError("immutable release code root is unsafe")
        generation = code_root.parent
        package_file = Path(str(rquant.__file__)).resolve(strict=True)
        if not package_file.is_relative_to((code_root / "src" / "rquant").resolve(strict=True)):
            raise LabDaemonConfigurationError("rquant imported outside immutable release code")
        if Path.cwd().resolve(strict=True) != code_root:
            raise LabDaemonConfigurationError("immutable release working directory mismatch")
        if Path(sys.prefix) != generation or Path(sys.executable) != generation / "bin" / "python":
            raise LabDaemonConfigurationError("immutable release interpreter mismatch")
        launcher = _canonical_absolute_path(Path(sys.argv[0]), label="runtime launcher")
        if launcher != generation / "bin" / "rquant":
            raise LabDaemonConfigurationError("immutable release launcher mismatch")
        _verify_deployment_generation(
            expected_checkout_root=code_root,
            expected_generation=deployment_generation,
            lock_path=deployment_lock_path,
            lock_fd=deployment_generation_fd,
        )
        trusted_git = bind_trusted_git_executable(trusted_git_path)
        command = sys.argv[1] if len(sys.argv) > 1 else ""
        provisional = {
            "lab-scheduler": "com.roxor.rquant-lab-scheduler",
            "lab-worker": "com.roxor.rquant-lab-worker",
            "lab-finalizer": "com.roxor.rquant-lab-finalizer",
        }.get(command)
        marker = ReleaseGenerationAuthority(
            repo=code_root,
            immutable_code_root=code_root,
            lock_path=deployment_lock_path,
            lock_fd=deployment_generation_fd,
            python_path=Path(sys.executable),
            git_path=trusted_git.path,
            overall_deadline_monotonic=authority_deadline,
        ).verify(
            expected_commit=deployment_generation,
            provisional_handoff_label=provisional,
        )
    except LabDaemonConfigurationError:
        raise
    except Exception as exc:
        raise LabDaemonConfigurationError("immutable Lab runtime binding failed") from exc
    if marker.commit != deployment_generation or Path(marker.venv_path) != generation:
        raise LabDaemonConfigurationError("immutable release generation authority is stale")
    return deployment_generation


@dataclass(frozen=True)
class RuntimeAuthorityIdentity:
    """Immutable filesystem identity retained from one complete runtime proof."""

    path: Path
    device: int
    inode: int
    mode: int
    owner: int
    links: int
    size: int
    mtime_ns: int
    ctime_ns: int
    stable_metadata: bool = True

    @classmethod
    def capture(
        cls,
        path: Path,
        *,
        label: str,
        stable_metadata: bool = True,
    ) -> RuntimeAuthorityIdentity:
        candidate = _canonical_absolute_path(path, label=label)
        try:
            observed = candidate.lstat()
        except OSError as exc:
            raise LabDaemonConfigurationError(f"{label} identity is unavailable") from exc
        return cls(
            path=candidate,
            device=observed.st_dev,
            inode=observed.st_ino,
            mode=observed.st_mode,
            owner=observed.st_uid,
            links=observed.st_nlink,
            size=observed.st_size,
            mtime_ns=observed.st_mtime_ns,
            ctime_ns=observed.st_ctime_ns,
            stable_metadata=stable_metadata,
        )

    def verify(self) -> None:
        try:
            observed = self.path.lstat()
        except OSError as exc:
            raise LabDaemonConfigurationError(
                "verified runtime authority identity changed"
            ) from exc
        current_object = (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_uid,
        )
        expected_object = (
            self.device,
            self.inode,
            self.mode,
            self.owner,
        )
        current_metadata = (
            observed.st_nlink,
            observed.st_size,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )
        expected_metadata = (
            self.links,
            self.size,
            self.mtime_ns,
            self.ctime_ns,
        )
        if current_object != expected_object or (
            self.stable_metadata and current_metadata != expected_metadata
        ):
            raise LabDaemonConfigurationError("verified runtime authority identity changed")


@dataclass(frozen=True)
class VerifiedLabRuntimeIdentity:
    """Typed startup proof used by constant-work daemon mutation fences."""

    code_sha: str
    checkout_path: Path
    checkout_root: RuntimeAuthorityIdentity | None
    generation_root: RuntimeAuthorityIdentity | None
    authority_root: RuntimeAuthorityIdentity | None
    environment_root: RuntimeAuthorityIdentity | None
    deployment_lock: RuntimeAuthorityIdentity | None
    selector: RuntimeAuthorityIdentity | None
    manifest: RuntimeAuthorityIdentity | None
    marker: RuntimeAuthorityIdentity | None
    venv: RuntimeAuthorityIdentity | None
    python: RuntimeAuthorityIdentity | None
    python_target: RuntimeAuthorityIdentity | None
    package: RuntimeAuthorityIdentity | None
    package_file: RuntimeAuthorityIdentity | None
    site_packages: RuntimeAuthorityIdentity | None
    uv_lock: RuntimeAuthorityIdentity | None
    pyproject: RuntimeAuthorityIdentity | None
    pyvenv_cfg: RuntimeAuthorityIdentity | None
    environment_generation_id: str | None
    venv_path: Path
    python_path: Path
    package_root: Path
    process_bound: bool
    process_prefix: Path
    process_executable: Path
    process_package_file: Path | None
    working_directory: Path

    def authorities(self) -> tuple[RuntimeAuthorityIdentity, ...]:
        return tuple(
            identity
            for identity in (
                self.checkout_root,
                self.generation_root,
                self.authority_root,
                self.environment_root,
                self.deployment_lock,
                self.selector,
                self.manifest,
                self.marker,
                self.venv,
                self.python,
                self.python_target,
                self.package,
                self.package_file,
                self.site_packages,
                self.uv_lock,
                self.pyproject,
                self.pyvenv_cfg,
            )
            if identity is not None
        )


def _capture_optional_runtime_identity(
    path: Path,
    *,
    label: str,
) -> RuntimeAuthorityIdentity | None:
    if not os.path.lexists(path):
        return None
    return RuntimeAuthorityIdentity.capture(path, label=label)


def _require_runtime_mode(
    identity: RuntimeAuthorityIdentity,
    *,
    label: str,
    mode: int,
    directory: bool,
) -> None:
    type_matches = stat.S_ISDIR(identity.mode) if directory else stat.S_ISREG(identity.mode)
    if (
        not type_matches
        or stat.S_ISLNK(identity.mode)
        or identity.owner != os.getuid()
        or (not directory and identity.links != 1)
        or stat.S_IMODE(identity.mode) != mode
    ):
        raise LabDaemonConfigurationError(f"{label} seal is invalid")


@dataclass(frozen=True)
class AttestedLabRuntimeGuard:
    """Recheck one formal generation and expose signed descriptive provenance."""

    capability: object
    startup_evidence: CodeTrustEvidence

    def verify_evidence(self) -> CodeTrustEvidence:
        observed = require_lab_runtime_binding(self.capability)
        if observed != self.startup_evidence:
            raise LabDaemonConfigurationError("formal Lab runtime evidence drifted")
        return observed

    def verify(self, *, startup_deadline_monotonic: float | None = None) -> str:
        if startup_deadline_monotonic is not None and (
            not math.isfinite(startup_deadline_monotonic)
            or time.monotonic() >= startup_deadline_monotonic
        ):
            raise LabDaemonConfigurationError("formal Lab startup deadline expired")
        return self.verify_evidence().provenance_commit

    def __call__(self) -> str:
        return self.verify_evidence().provenance_commit


@dataclass(frozen=True)
class LabRuntimeGuard:
    """Developer-only checkout guard retained for exploratory migration."""

    expected_checkout_root: Path
    startup_sha: str
    trusted_git_path: Path = Path("/usr/bin/git")
    verifier: Callable[[Path], str] | None = None
    deployment_generation: str | None = None
    deployment_lock_path: Path | None = None
    deployment_generation_fd: int | None = None

    def __post_init__(self) -> None:
        expected = _canonical_absolute_path(
            self.expected_checkout_root,
            label="expected checkout root",
        )
        startup_sha = require_clean_code_sha(lambda: self.startup_sha)
        trusted_git_path = _canonical_absolute_path(
            self.trusted_git_path,
            label="trusted Git path",
        )
        object.__setattr__(self, "expected_checkout_root", expected)
        object.__setattr__(self, "startup_sha", startup_sha)
        object.__setattr__(self, "trusted_git_path", trusted_git_path)
        generation_values = (
            self.deployment_generation,
            self.deployment_lock_path,
            self.deployment_generation_fd,
        )
        if any(value is not None for value in generation_values) and any(
            value is None for value in generation_values
        ):
            raise LabDaemonConfigurationError("deployment generation guard is incomplete")

    def verify(self, *, startup_deadline_monotonic: float | None = None) -> str:
        try:
            if self.deployment_generation is not None:
                _verify_deployment_generation(
                    expected_checkout_root=self.expected_checkout_root,
                    expected_generation=self.deployment_generation,
                    lock_path=Path(self.deployment_lock_path),
                    lock_fd=int(self.deployment_generation_fd),
                )
            if self.verifier is not None:
                observed = self.verifier(self.expected_checkout_root)
            else:
                binding: dict[str, object] = {}
                if self.deployment_generation is not None:
                    binding = {
                        "deployment_generation": self.deployment_generation,
                        "deployment_lock_path": self.deployment_lock_path,
                        "deployment_generation_fd": self.deployment_generation_fd,
                    }
                observed = require_legacy_checkout_runtime_binding(
                    self.expected_checkout_root,
                    self.trusted_git_path,
                    startup_deadline_monotonic=(
                        startup_deadline_monotonic
                        if startup_deadline_monotonic is not None
                        else time.monotonic() + 3
                    ),
                    **binding,
                )
        except LabDaemonConfigurationError:
            raise
        except Exception as exc:
            raise LabDaemonConfigurationError("lab runtime guard verification failed") from exc
        current = require_clean_code_sha(lambda: observed)
        if current != self.startup_sha:
            raise LabDaemonConfigurationError("lab runtime guard detected startup SHA drift")
        if self.deployment_generation is not None:
            if current != self.deployment_generation:
                raise LabDaemonConfigurationError("lab runtime guard generation drift")
            _verify_deployment_generation(
                expected_checkout_root=self.expected_checkout_root,
                expected_generation=self.deployment_generation,
                lock_path=Path(self.deployment_lock_path),
                lock_fd=int(self.deployment_generation_fd),
            )
        return current

    def verify_runtime_identity(
        self,
        *,
        startup_deadline_monotonic: float | None = None,
    ) -> VerifiedLabRuntimeIdentity:
        verified = self.verify(startup_deadline_monotonic=startup_deadline_monotonic)
        return self.capture_verified_identity(verified)

    def capture_verified_identity(self, verified_code_sha: str) -> VerifiedLabRuntimeIdentity:
        current = require_clean_code_sha(lambda: verified_code_sha)
        if current != self.startup_sha:
            raise LabDaemonConfigurationError("verified runtime identity SHA drift")
        checkout = self.expected_checkout_root
        checkout_identity = _capture_optional_runtime_identity(
            checkout,
            label="verified runtime checkout",
        )
        uv_lock = _capture_optional_runtime_identity(
            checkout / "uv.lock",
            label="verified runtime uv.lock",
        )
        pyproject = _capture_optional_runtime_identity(
            checkout / "pyproject.toml",
            label="verified runtime pyproject.toml",
        )
        expected_package_root = checkout / "src" / "rquant"
        package = _capture_optional_runtime_identity(
            expected_package_root,
            label="verified runtime package root",
        )
        package_file_path = expected_package_root / "__init__.py"
        package_file = _capture_optional_runtime_identity(
            package_file_path,
            label="verified runtime package file",
        )
        generation_root = None
        authority_root = None
        environment_root = None
        deployment_lock = None
        selector_identity = None
        manifest_identity = None
        marker_identity = None
        venv_identity = None
        python_identity = None
        python_target_identity = None
        site_packages_identity = None
        pyvenv_cfg = None
        environment_generation_id = None
        venv_path = Path(sys.prefix)
        python_path = Path(sys.executable)
        package_root = expected_package_root

        if self.deployment_generation is not None:
            from rquant.release_generation import (
                EnvironmentSelector,
                ReleaseGenerationError,
                ReleaseGenerationMarker,
                environment_manifest_path_for_lock,
                environment_root_for_lock,
                environment_selector_path_for_lock,
                generation_code_root,
                marker_path_for_lock,
            )

            lock_path = Path(self.deployment_lock_path)
            _verify_deployment_generation(
                expected_checkout_root=checkout,
                expected_generation=self.deployment_generation,
                lock_path=lock_path,
                lock_fd=int(self.deployment_generation_fd),
            )
            try:
                selector_path = environment_selector_path_for_lock(lock_path)
                selector_payload = strict_canonical_json_loads(
                    _read_private_file(
                        selector_path,
                        label="release environment selector",
                        max_bytes=32 * 1024,
                    ),
                    trailing_newline=True,
                )
                selector = EnvironmentSelector.from_payload(selector_payload)
                marker_path = marker_path_for_lock(lock_path)
                marker_payload = strict_canonical_json_loads(
                    _read_private_file(
                        marker_path,
                        label="release generation marker",
                        max_bytes=32 * 1024,
                    ),
                    trailing_newline=True,
                )
                marker = ReleaseGenerationMarker.from_payload(marker_payload)
            except (LabDaemonConfigurationError, ReleaseGenerationError, StrictJsonError) as exc:
                raise LabDaemonConfigurationError(
                    "verified runtime authority records are invalid"
                ) from exc
            venv_path = _canonical_absolute_path(
                Path(selector.environment_path),
                label="verified runtime generation",
            )
            environment_root_path = environment_root_for_lock(lock_path)
            if (
                selector.commit != current
                or marker.commit != current
                or marker.environment_generation_id != selector.generation_id
                or marker.environment_manifest_sha256 != selector.manifest_sha256
                or Path(marker.venv_path) != venv_path
                or venv_path.parent != environment_root_path
                or venv_path.name != selector.generation_id
                or generation_code_root(venv_path) != checkout
            ):
                raise LabDaemonConfigurationError("verified runtime generation binding changed")
            manifest_path = environment_manifest_path_for_lock(
                lock_path,
                selector.generation_id,
            )
            if selector.manifest_name != manifest_path.name:
                raise LabDaemonConfigurationError("verified runtime manifest binding changed")
            python_path = _canonical_absolute_path(
                Path(marker.python_path),
                label="verified runtime Python",
            )
            site_packages_path = _canonical_absolute_path(
                Path(marker.site_packages_path),
                label="verified runtime site-packages",
            )
            package_root = checkout / "src" / "rquant"
            deployment_lock = RuntimeAuthorityIdentity.capture(
                lock_path,
                label="verified deployment lock",
            )
            authority_root = RuntimeAuthorityIdentity.capture(
                lock_path.parent,
                label="verified deployment authority root",
                stable_metadata=False,
            )
            environment_root = RuntimeAuthorityIdentity.capture(
                environment_root_path,
                label="verified environment authority root",
                stable_metadata=False,
            )
            selector_identity = RuntimeAuthorityIdentity.capture(
                selector_path,
                label="verified environment selector",
            )
            manifest_identity = RuntimeAuthorityIdentity.capture(
                manifest_path,
                label="verified environment manifest",
            )
            marker_identity = RuntimeAuthorityIdentity.capture(
                marker_path,
                label="verified release marker",
            )
            generation_root = RuntimeAuthorityIdentity.capture(
                venv_path,
                label="verified runtime generation",
            )
            venv_identity = generation_root
            python_identity = RuntimeAuthorityIdentity.capture(
                python_path,
                label="verified runtime Python",
            )
            python_target_identity = RuntimeAuthorityIdentity.capture(
                python_path.resolve(strict=True),
                label="verified runtime Python target",
            )
            site_packages_identity = RuntimeAuthorityIdentity.capture(
                site_packages_path,
                label="verified runtime site-packages",
            )
            pyvenv_cfg = RuntimeAuthorityIdentity.capture(
                venv_path / "pyvenv.cfg",
                label="verified runtime pyvenv.cfg",
            )
            checkout_identity = RuntimeAuthorityIdentity.capture(
                checkout,
                label="verified runtime checkout",
            )
            package = RuntimeAuthorityIdentity.capture(
                package_root,
                label="verified runtime package root",
            )
            package_file = RuntimeAuthorityIdentity.capture(
                package_root / "__init__.py",
                label="verified runtime package file",
            )
            uv_lock = RuntimeAuthorityIdentity.capture(
                checkout / "uv.lock",
                label="verified runtime uv.lock",
            )
            pyproject = RuntimeAuthorityIdentity.capture(
                checkout / "pyproject.toml",
                label="verified runtime pyproject.toml",
            )
            for identity, label, mode in (
                (deployment_lock, "deployment lock", 0o600),
                (selector_identity, "environment selector", 0o600),
                (manifest_identity, "environment manifest", 0o600),
                (marker_identity, "release marker", 0o600),
            ):
                _require_runtime_mode(identity, label=label, mode=mode, directory=False)
            for identity, label, mode in (
                (authority_root, "deployment authority root", 0o700),
                (environment_root, "environment authority root", 0o700),
                (generation_root, "runtime generation", 0o500),
                (checkout_identity, "runtime checkout", 0o500),
                (package, "runtime package root", 0o500),
                (site_packages_identity, "runtime site-packages", 0o500),
            ):
                _require_runtime_mode(identity, label=label, mode=mode, directory=True)
            environment_generation_id = selector.generation_id
            _verify_deployment_generation(
                expected_checkout_root=checkout,
                expected_generation=self.deployment_generation,
                lock_path=lock_path,
                lock_fd=int(self.deployment_generation_fd),
            )
        else:
            venv_identity = _capture_optional_runtime_identity(
                venv_path,
                label="verified runtime virtualenv",
            )
            python_identity = _capture_optional_runtime_identity(
                python_path,
                label="verified runtime Python",
            )
            if python_identity is not None:
                python_target_identity = _capture_optional_runtime_identity(
                    python_path.resolve(strict=True),
                    label="verified runtime Python target",
                )

        process_bound = self.verifier is None
        process_package_file = None
        working_directory = Path.cwd()
        if process_bound:
            import rquant

            module_file = getattr(rquant, "__file__", None)
            if not isinstance(module_file, str) or not module_file:
                raise LabDaemonConfigurationError("verified runtime package file is unavailable")
            process_package_file = Path(module_file).resolve(strict=True)
            working_directory = Path.cwd().resolve(strict=True)
        return VerifiedLabRuntimeIdentity(
            code_sha=current,
            checkout_path=checkout,
            checkout_root=checkout_identity,
            generation_root=generation_root,
            authority_root=authority_root,
            environment_root=environment_root,
            deployment_lock=deployment_lock,
            selector=selector_identity,
            manifest=manifest_identity,
            marker=marker_identity,
            venv=venv_identity,
            python=python_identity,
            python_target=python_target_identity,
            package=package,
            package_file=package_file,
            site_packages=site_packages_identity,
            uv_lock=uv_lock,
            pyproject=pyproject,
            pyvenv_cfg=pyvenv_cfg,
            environment_generation_id=environment_generation_id,
            venv_path=venv_path,
            python_path=python_path,
            package_root=package_root,
            process_bound=process_bound,
            process_prefix=Path(sys.prefix),
            process_executable=Path(sys.executable),
            process_package_file=process_package_file,
            working_directory=working_directory,
        )

    def verify_identity(self, identity: VerifiedLabRuntimeIdentity) -> str:
        if (
            type(identity) is not VerifiedLabRuntimeIdentity
            or identity.code_sha != self.startup_sha
            or identity.checkout_path != self.expected_checkout_root
            or identity.process_prefix != Path(sys.prefix)
            or identity.process_executable != Path(sys.executable)
        ):
            raise LabDaemonConfigurationError("verified runtime identity binding changed")
        if self.deployment_generation is not None:
            if (
                identity.environment_generation_id is None
                or identity.code_sha != self.deployment_generation
                or identity.deployment_lock is None
            ):
                raise LabDaemonConfigurationError("verified runtime generation binding changed")
            _verify_deployment_generation(
                expected_checkout_root=self.expected_checkout_root,
                expected_generation=self.deployment_generation,
                lock_path=Path(self.deployment_lock_path),
                lock_fd=int(self.deployment_generation_fd),
            )
        for authority in identity.authorities():
            authority.verify()
        if identity.process_bound:
            import rquant

            module_file = getattr(rquant, "__file__", None)
            if not isinstance(module_file, str) or not module_file:
                raise LabDaemonConfigurationError("verified runtime package identity changed")
            try:
                current_package_file = Path(module_file).resolve(strict=True)
                current_working_directory = Path.cwd().resolve(strict=True)
            except OSError as exc:
                raise LabDaemonConfigurationError(
                    "verified runtime process identity changed"
                ) from exc
            if (
                current_package_file != identity.process_package_file
                or current_working_directory != identity.working_directory
            ):
                raise LabDaemonConfigurationError("verified runtime process identity changed")
        return identity.code_sha


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
        created: bool,
    ) -> None:
        self.path = path
        self.label = label
        self._parent_descriptor = parent_descriptor
        self._database_descriptor = database_descriptor
        self._parent_identity = parent_identity
        self._database_identity = database_identity
        self.created = created

    @staticmethod
    def _identity(observed: os.stat_result) -> tuple[int, int]:
        return observed.st_dev, observed.st_ino

    @property
    def database_generation(self) -> tuple[int, int]:
        return self._identity(self._database_identity)

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

    def discard_created(self) -> None:
        """Remove only the inode this authority created, through its retained parent."""
        if not self.created:
            return
        if self._parent_descriptor < 0 or self._database_descriptor < 0:
            raise LabDaemonConfigurationError(f"{self.label} authority is closed")
        opened = os.fstat(self._database_descriptor)
        try:
            active = os.stat(
                self.path.name,
                dir_fd=self._parent_descriptor,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise LabDaemonConfigurationError(
                f"{self.label} created database cannot be safely removed"
            ) from exc
        if self._identity(opened) != self._identity(self._database_identity) or self._identity(
            active
        ) != self._identity(self._database_identity):
            raise LabDaemonConfigurationError(
                f"{self.label} created database identity changed before cleanup"
            )
        os.unlink(self.path.name, dir_fd=self._parent_descriptor)
        os.fsync(self._parent_descriptor)
        self.created = False

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


def _prepare_private_sqlite_from_parent(
    candidate: Path,
    *,
    label: str,
    create: bool,
    mutation_guard: Callable[[], object] | None,
    parent_descriptor: int,
    parent_identity: os.stat_result,
) -> LabSqliteAuthority:
    descriptor = -1
    created = False
    try:
        try:
            observed = os.stat(candidate.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            if not create:
                raise LabDaemonConfigurationError(f"{label} does not exist") from None
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            try:
                if mutation_guard is not None:
                    mutation_guard()
                descriptor = os.open(candidate.name, flags, 0o600, dir_fd=parent_descriptor)
                created = True
            except OSError as exc:
                raise LabDaemonConfigurationError(
                    f"{label} could not be created atomically"
                ) from exc
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
            parent_identity=parent_identity,
            database_identity=current,
            created=created,
        )
        authority.assert_current()
        parent_descriptor = -1
        descriptor = -1
        return authority
    except BaseException:
        if created and descriptor >= 0 and parent_descriptor >= 0:
            opened = os.fstat(descriptor)
            try:
                active = os.stat(
                    candidate.name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                active = None
            if active is not None and (active.st_dev, active.st_ino) == (
                opened.st_dev,
                opened.st_ino,
            ):
                os.unlink(candidate.name, dir_fd=parent_descriptor)
                os.fsync(parent_descriptor)
        if descriptor >= 0:
            os.close(descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)
        raise


def prepare_private_sqlite_path(
    path: Path,
    *,
    label: str,
    create: bool,
    mutation_guard: Callable[[], object] | None = None,
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
    return _prepare_private_sqlite_from_parent(
        candidate,
        label=label,
        create=create,
        mutation_guard=mutation_guard,
        parent_descriptor=parent_descriptor,
        parent_identity=opened_parent,
    )


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
            document = strict_canonical_json_loads(
                ring_payload,
                trailing_newline=True,
            )
        except (UnicodeDecodeError, StrictJsonError) as exc:
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


class LabDaemonReadinessPublisher:
    """Atomically publish one daemon's liveness under the deployment authority root."""

    _LABEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,127}$")

    def __init__(
        self,
        *,
        deployment_lock_path: Path,
        deployment_lock_fd: int,
        daemon_authority_lease_fd: int | None = None,
        label: str,
        operation_id: str,
        environment_generation_id: str,
        code_sha: str,
        heartbeat_interval_seconds: float,
        readiness_root: Path | None = None,
        mutation_guard: Callable[[], object] | None = None,
        monotonic_provider: Callable[[], float] = time.monotonic,
        now_provider: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if self._LABEL.fullmatch(label) is None:
            raise LabDaemonConfigurationError("daemon readiness label is invalid")
        if re.fullmatch(r"[0-9a-f]{32}", operation_id) is None:
            raise LabDaemonConfigurationError("daemon readiness operation id is invalid")
        if re.fullmatch(r"[0-9a-f]{64}", environment_generation_id) is None:
            raise LabDaemonConfigurationError("daemon readiness environment generation is invalid")
        if _CODE_SHA.fullmatch(code_sha) is None:
            raise LabDaemonConfigurationError("daemon readiness code SHA is invalid")
        if not 0.1 <= heartbeat_interval_seconds <= 60:
            raise LabDaemonConfigurationError("daemon readiness interval is invalid")
        self.lock_path = _canonical_absolute_path(
            deployment_lock_path,
            label="deployment generation lock",
        )
        self.lock_fd = deployment_lock_fd
        self.label = label
        self.operation_id = operation_id
        self.environment_generation_id = environment_generation_id
        self.code_sha = code_sha
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.mutation_guard = mutation_guard
        self.monotonic_provider = monotonic_provider
        self.now_provider = now_provider
        self.root = _canonical_absolute_path(
            readiness_root or self.lock_path.with_name(f"{self.lock_path.stem}.lab-readiness"),
            label="lab daemon readiness root",
        )
        self.path = self.root / f"{label}.json"
        self.started_at = self.now_provider()
        self._stop = Event()
        self._thread_entered = Event()
        self._thread: Thread | None = None
        self._thread_state = "created"
        self._daemon_authority_lease_fd = -1
        self._verify_lock()
        if daemon_authority_lease_fd is not None:
            lease = os.fstat(daemon_authority_lease_fd)
            _validate_private_regular_identity(lease, label="daemon authority lease")
            self._daemon_authority_lease_fd = daemon_authority_lease_fd

    def _verify_lock(self) -> os.stat_result:
        try:
            opened = os.fstat(self.lock_fd)
            active = self.lock_path.lstat()
        except OSError as exc:
            raise LabDaemonConfigurationError("deployment generation lock is unavailable") from exc
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (opened.st_dev, opened.st_ino) != (active.st_dev, active.st_ino)
        ):
            raise LabDaemonConfigurationError("deployment generation lock identity changed")
        return opened

    def _root_fd(self, *, create: bool) -> tuple[int, os.stat_result]:
        parent = require_private_directory(self.root.parent, label="release authority root")
        if create:
            ensure_private_directory(
                self.root,
                label="lab daemon readiness root",
                mutation_guard=self.mutation_guard,
            )
        root = require_private_directory(self.root, label="lab daemon readiness root")
        root_stat = root.lstat()
        try:
            descriptor = os.open(
                root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
        except OSError as exc:
            raise LabDaemonConfigurationError("readiness root cannot be opened safely") from exc
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (root_stat.st_dev, root_stat.st_ino):
            os.close(descriptor)
            raise LabDaemonConfigurationError("readiness root identity changed")
        if parent != self.root.parent:
            os.close(descriptor)
            raise LabDaemonConfigurationError("readiness authority root changed")
        return descriptor, root_stat

    def _assert_root_current(self, descriptor: int, expected: os.stat_result) -> None:
        self._verify_lock()
        try:
            active = self.root.lstat()
            opened = os.fstat(descriptor)
        except OSError as exc:
            raise LabDaemonConfigurationError("readiness root identity changed") from exc
        if (active.st_dev, active.st_ino) != (expected.st_dev, expected.st_ino) or (
            opened.st_dev,
            opened.st_ino,
        ) != (expected.st_dev, expected.st_ino):
            raise LabDaemonConfigurationError("readiness root identity changed")

    @staticmethod
    def _write_all(descriptor: int, payload: bytes) -> None:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise LabDaemonConfigurationError("readiness heartbeat write was incomplete")
            offset += written

    def publish_once(self) -> LabDaemonReadiness:
        if self.mutation_guard is not None:
            self.mutation_guard()
        lock = self._verify_lock()
        heartbeat = LabDaemonReadiness(
            label=self.label,
            pid=os.getpid(),
            operation_id=self.operation_id,
            environment_generation_id=self.environment_generation_id,
            code_sha=self.code_sha,
            started_at=self.started_at,
            heartbeat_at=self.now_provider(),
            heartbeat_monotonic=self.monotonic_provider(),
            generation_lock_device=lock.st_dev,
            generation_lock_inode=lock.st_ino,
        )
        payload = canonical_model_json_bytes(heartbeat) + b"\n"
        root_fd, root_stat = self._root_fd(create=True)
        temporary = f".{self.label}.{os.getpid()}.{uuid4().hex}.tmp"
        descriptor = -1
        try:
            if self.mutation_guard is not None:
                self.mutation_guard()
            self._assert_root_current(root_fd, root_stat)
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_fd,
            )
            self._write_all(descriptor, payload)
            os.fsync(descriptor)
            _validate_private_regular_identity(os.fstat(descriptor), label="readiness heartbeat")
            if self.mutation_guard is not None:
                self.mutation_guard()
            self._assert_root_current(root_fd, root_stat)
            os.replace(temporary, self.path.name, src_dir_fd=root_fd, dst_dir_fd=root_fd)
            os.fsync(root_fd)
            self._assert_root_current(root_fd, root_stat)
        except BaseException:
            with suppress(OSError):
                os.unlink(temporary, dir_fd=root_fd)
            raise
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(root_fd)
        return heartbeat

    @classmethod
    def read(
        cls,
        *,
        deployment_lock_path: Path,
        label: str,
        readiness_root: Path | None = None,
    ) -> LabDaemonReadiness:
        lock_path = _canonical_absolute_path(
            deployment_lock_path,
            label="deployment generation lock",
        )
        root = _canonical_absolute_path(
            readiness_root or lock_path.with_name(f"{lock_path.stem}.lab-readiness"),
            label="lab daemon readiness root",
        )
        require_private_directory(root, label="lab daemon readiness root")
        root_stat = root.lstat()
        root_fd = os.open(
            root,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        descriptor = -1
        try:
            descriptor = os.open(
                f"{label}.json",
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
            opened = os.fstat(descriptor)
            _validate_private_regular_identity(opened, label="readiness heartbeat")
            payload = os.read(descriptor, 16385)
            final = os.fstat(descriptor)
            if len(payload) > 16384 or (
                final.st_dev,
                final.st_ino,
                final.st_mode,
                final.st_uid,
                final.st_nlink,
                final.st_size,
            ) != (
                opened.st_dev,
                opened.st_ino,
                opened.st_mode,
                opened.st_uid,
                opened.st_nlink,
                opened.st_size,
            ):
                raise LabDaemonConfigurationError("readiness heartbeat identity changed")
            active = os.stat(f"{label}.json", dir_fd=root_fd, follow_symlinks=False)
            if (active.st_dev, active.st_ino) != (opened.st_dev, opened.st_ino):
                raise LabDaemonConfigurationError("readiness heartbeat identity changed")
            current_root = root.lstat()
            if (current_root.st_dev, current_root.st_ino) != (
                root_stat.st_dev,
                root_stat.st_ino,
            ):
                raise LabDaemonConfigurationError("readiness root identity changed")
            return strict_model_validate_canonical_json(
                LabDaemonReadiness, payload, trailing_newline=True
            )
        except (OSError, ValueError) as exc:
            raise LabDaemonConfigurationError("readiness heartbeat is invalid") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(root_fd)

    def _run(self) -> None:
        while not self._stop.wait(self.heartbeat_interval_seconds):
            try:
                self.publish_once()
            except Exception:
                logger.exception("lab daemon readiness heartbeat failed")
                self._stop.set()

    def _thread_main(self) -> None:
        self._thread_entered.set()
        self._run()

    def _stop_started_thread(self, thread: Thread) -> None:
        self._stop.set()
        thread.join(timeout=max(1.0, self.heartbeat_interval_seconds * 2))
        if thread.is_alive():
            raise RuntimeError("daemon readiness publisher did not stop within its deadline")
        self._thread = None
        self._thread_state = "stopped"
        self._release_authority_lease()

    @staticmethod
    def _attach_cleanup_error(primary: BaseException, cleanup_error: BaseException) -> None:
        cleanup_group = BaseExceptionGroup(
            "daemon readiness startup cleanup failures",
            [cleanup_error],
        )
        primary.cleanup_error_group = cleanup_group  # type: ignore[attr-defined]
        primary.add_note("daemon readiness startup cleanup also failed")

    def start(self) -> LabDaemonReadiness:
        if self._thread_state != "created":
            raise RuntimeError("daemon readiness publisher is already started")
        heartbeat = self.publish_once()
        thread = Thread(target=self._thread_main, name=f"readiness-{self.label}", daemon=True)
        self._thread = thread
        self._thread_state = "starting"
        try:
            thread.start()
            if not self._thread_entered.wait(timeout=max(1.0, self.heartbeat_interval_seconds * 2)):
                raise RuntimeError("daemon readiness publisher thread did not start")
        except BaseException as primary_exception:
            self._stop.set()
            try:
                if self._thread_entered.is_set() or thread.is_alive():
                    self._stop_started_thread(thread)
                else:
                    self._thread = None
                    self._thread_state = "stopped"
                    self._release_authority_lease()
            except BaseException as cleanup_error:
                self._attach_cleanup_error(primary_exception, cleanup_error)
            raise
        self._thread_state = "started"
        return heartbeat

    def _release_authority_lease(self) -> None:
        if self._daemon_authority_lease_fd < 0:
            return
        descriptor = self._daemon_authority_lease_fd
        os.close(descriptor)
        self._daemon_authority_lease_fd = -1

    def close(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is None:
            self._thread_state = "stopped"
            self._release_authority_lease()
            return
        if self._thread_state == "created" or (
            self._thread_state == "starting"
            and not self._thread_entered.is_set()
            and not thread.is_alive()
        ):
            self._thread = None
            self._thread_state = "stopped"
            self._release_authority_lease()
            return
        self._stop_started_thread(thread)

    def __enter__(self) -> LabDaemonReadinessPublisher:
        try:
            self.start()
        except BaseException as primary_exception:
            try:
                self.close()
            except BaseException as cleanup_error:
                cleanup_group = BaseExceptionGroup(
                    "daemon readiness context cleanup failures",
                    [cleanup_error],
                )
                primary_exception.cleanup_error_group = cleanup_group  # type: ignore[attr-defined]
                primary_exception.add_note("daemon readiness context cleanup also failed")
            raise
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class LabDaemonLock:
    """Advisory daemon lock anchored beside, rather than inside, its runtime root.

    The stable lock name binds the configured canonical root path and daemon name.
    Replacing the lock root therefore cannot create a second lock namespace. A
    malicious same-UID replacement of a higher-level parent remains outside this
    local filesystem boundary.
    """

    def __init__(
        self,
        root: Path,
        name: str,
        *,
        mutation_guard: Callable[[], object] | None = None,
    ) -> None:
        if re.fullmatch(r"[a-z][a-z0-9-]{0,63}", name) is None:
            raise ValueError("daemon lock name is invalid")
        self.root = Path(root)
        self.name = name
        self.mutation_guard = mutation_guard
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
        mutation_guard: Callable[[], object] | None = None,
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
                if mutation_guard is not None:
                    mutation_guard()
                descriptor = os.open(name, flags, 0o600, dir_fd=directory_descriptor)
            except OSError as exc:
                raise LabDaemonConfigurationError(
                    f"{label} could not be created atomically"
                ) from exc
        else:
            _validate_private_regular_identity(observed, label=label)
            flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(name, flags, dir_fd=directory_descriptor)
            except OSError as exc:
                raise LabDaemonConfigurationError(f"{label} could not be opened safely") from exc
        try:
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
                    if self.mutation_guard is not None:
                        self.mutation_guard()
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
                mutation_guard=self.mutation_guard,
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
                mutation_guard=self.mutation_guard,
            )
            if self.mutation_guard is not None:
                self.mutation_guard()
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
                # A readiness lease is a dup of this open-file description; LOCK_UN here
                # would release singleton authority before its heartbeat thread exits.
                os.close(descriptor)
        finally:
            if root_descriptor >= 0:
                os.close(root_descriptor)
            if parent_descriptor >= 0:
                os.close(parent_descriptor)

    def duplicate_authority_lease(self) -> int:
        if self._descriptor < 0 or self.authority_path is None:
            raise RuntimeError("daemon lock is not acquired")
        if self.mutation_guard is not None:
            self.mutation_guard()
        try:
            opened = os.fstat(self._descriptor)
            active = self.authority_path.lstat()
            _validate_private_regular_identity(opened, label="daemon authority lock file")
            if (opened.st_dev, opened.st_ino) != (active.st_dev, active.st_ino):
                raise LabDaemonConfigurationError("daemon authority lock identity changed")
            lease = os.dup(self._descriptor)
            rebound = os.fstat(lease)
            if (rebound.st_dev, rebound.st_ino) != (opened.st_dev, opened.st_ino):
                os.close(lease)
                raise LabDaemonConfigurationError("daemon authority lease identity changed")
            return lease
        except LabDaemonConfigurationError:
            raise
        except OSError as exc:
            raise LabDaemonConfigurationError("daemon authority lease is unavailable") from exc

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


class _IncrementalIntegrityAuditor(Protocol):
    def audit_incremental(self, *, max_chain_entries: int) -> object: ...


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
    _WRITER_LOCK_NAME = ".state.writer.lock"

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
        self._assert_root_binding(descriptor, expected)

    def _assert_root_binding(
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

    @staticmethod
    def _state_identity(observed: os.stat_result) -> tuple[int, ...]:
        return (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_uid,
            observed.st_nlink,
            observed.st_size,
            observed.st_mtime_ns,
        )

    def _open_writer_lock(
        self,
        root_descriptor: int,
        root_identity: os.stat_result,
        *,
        mutation_guard: Callable[[], object] | None = None,
    ) -> int:
        flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        try:
            if mutation_guard is not None:
                mutation_guard()
            descriptor = os.open(
                self._WRITER_LOCK_NAME,
                flags | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=root_descriptor,
            )
        except FileExistsError:
            descriptor = os.open(
                self._WRITER_LOCK_NAME,
                flags,
                dir_fd=root_descriptor,
            )
        try:
            opened = os.fstat(descriptor)
            _validate_private_regular_identity(
                opened,
                label="lab finalizer state writer lock",
            )
            active = os.stat(
                self._WRITER_LOCK_NAME,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
            _validate_private_regular_identity(
                active,
                label="lab finalizer state writer lock",
            )
            if self._state_identity(active) != self._state_identity(opened):
                raise LabDaemonConfigurationError(
                    "lab finalizer state writer lock identity changed"
                )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            self._assert_root_binding(root_descriptor, root_identity)
            active = os.stat(
                self._WRITER_LOCK_NAME,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
            if self._state_identity(active) != self._state_identity(opened):
                raise LabDaemonConfigurationError(
                    "lab finalizer state writer lock identity changed"
                )
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    @staticmethod
    def _before_state_exchange(_root_descriptor: int) -> None:
        """Fault-injection boundary before final state publication."""

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
                try:
                    appeared = os.stat(
                        self.path.name,
                        dir_fd=root_descriptor,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    self._assert_root_current(root_descriptor, root_identity)
                    return LabFinalizerDaemonState()
                _validate_private_regular_identity(
                    appeared,
                    label="lab finalizer state file",
                )
                raise LabDaemonConfigurationError(
                    "lab finalizer state appeared after missing observation"
                ) from None
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
                state = strict_model_validate_canonical_json(LabFinalizerDaemonState, payload)
            except (ValueError, TypeError) as exc:
                raise LabDaemonConfigurationError("lab finalizer state is corrupt") from exc
            if len(state.failures) > self._MAX_FAILURES:
                raise LabDaemonConfigurationError("lab finalizer state has too many failures")
            self._assert_root_current(root_descriptor, root_identity)
            active = os.stat(
                self.path.name,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
            active_path = self.path.lstat()
            for identity in (active, active_path):
                _validate_private_regular_identity(
                    identity,
                    label="lab finalizer state file",
                )
            expected_identity = (
                final.st_dev,
                final.st_ino,
                final.st_mode,
                final.st_uid,
                final.st_nlink,
                final.st_size,
                final.st_mtime_ns,
            )
            if any(
                (
                    identity.st_dev,
                    identity.st_ino,
                    identity.st_mode,
                    identity.st_uid,
                    identity.st_nlink,
                    identity.st_size,
                    identity.st_mtime_ns,
                )
                != expected_identity
                for identity in (active, active_path)
            ):
                raise LabDaemonConfigurationError("lab finalizer state changed during read")
            return state
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(root_descriptor)

    def save(
        self,
        state: LabFinalizerDaemonState,
        *,
        mutation_guard: Callable[[], object] | None = None,
    ) -> None:
        state = LabFinalizerDaemonState.model_validate(state.model_dump())
        if len(state.failures) > self._MAX_FAILURES:
            raise LabDaemonConfigurationError("lab finalizer state has too many failures")
        payload = canonical_model_json_bytes(state)
        if len(payload) > self._MAX_BYTES:
            raise LabDaemonConfigurationError("lab finalizer state file is too large")
        root_descriptor, root_identity = self._open_root()
        temporary_name = f".state.{os.getpid()}.{uuid4().hex}.tmp"
        descriptor = -1
        existing_descriptor = -1
        writer_lock_descriptor = -1
        replaced = False

        def guard_mutation() -> None:
            if mutation_guard is not None:
                mutation_guard()

        def guarded_unlink_if_exists(name: str) -> None:
            try:
                os.stat(name, dir_fd=root_descriptor, follow_symlinks=False)
            except FileNotFoundError:
                return
            guard_mutation()
            os.unlink(name, dir_fd=root_descriptor)

        try:
            writer_lock_descriptor = self._open_writer_lock(
                root_descriptor,
                root_identity,
                mutation_guard=mutation_guard,
            )
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
                existing_descriptor = os.open(
                    self.path.name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=root_descriptor,
                )
                opened_existing = os.fstat(existing_descriptor)
                if (opened_existing.st_dev, opened_existing.st_ino) != (
                    existing.st_dev,
                    existing.st_ino,
                ):
                    raise LabDaemonConfigurationError(
                        "lab finalizer state identity changed before commit"
                    )
            guard_mutation()
            descriptor = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_descriptor,
            )
            offset = 0
            while offset < len(payload):
                offset += os.write(descriptor, payload[offset:])
            os.fsync(descriptor)
            temporary_identity = os.fstat(descriptor)
            _validate_private_regular_identity(
                temporary_identity,
                label="lab finalizer state temporary file",
            )
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
            if existing is None:
                try:
                    guard_mutation()
                    os.link(
                        temporary_name,
                        self.path.name,
                        src_dir_fd=root_descriptor,
                        dst_dir_fd=root_descriptor,
                        follow_symlinks=False,
                    )
                except FileExistsError as exc:
                    raise LabDaemonConfigurationError(
                        "lab finalizer state was created concurrently"
                    ) from exc
                replaced = True
                guarded_unlink_if_exists(temporary_name)
            else:
                self._before_state_exchange(root_descriptor)
                self._assert_root_current(root_descriptor, root_identity)
                active = os.stat(
                    self.path.name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
                _validate_private_regular_identity(
                    active,
                    label="lab finalizer state file",
                )
                if self._state_identity(active) != self._state_identity(existing):
                    raise LabDaemonConfigurationError(
                        "lab finalizer state changed concurrently before commit"
                    )
                guard_mutation()
                os.replace(
                    temporary_name,
                    self.path.name,
                    src_dir_fd=root_descriptor,
                    dst_dir_fd=root_descriptor,
                )
                replaced = True
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
            if any(
                (identity.st_dev, identity.st_ino)
                != (temporary_identity.st_dev, temporary_identity.st_ino)
                for identity in (committed, active_path)
            ) or committed.st_size != len(payload):
                raise LabDaemonConfigurationError(
                    "lab finalizer state identity changed after commit"
                )
            os.fsync(root_descriptor)
            self._assert_root_current(root_descriptor, root_identity)
            final_active = os.stat(
                self.path.name,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
            final_active_path = self.path.lstat()
            for identity in (final_active, final_active_path):
                _validate_private_regular_identity(
                    identity,
                    label="lab finalizer state file",
                )
            expected_identity = (
                temporary_identity.st_dev,
                temporary_identity.st_ino,
                temporary_identity.st_mode,
                temporary_identity.st_uid,
                temporary_identity.st_nlink,
                temporary_identity.st_size,
                temporary_identity.st_mtime_ns,
            )
            if any(
                (
                    identity.st_dev,
                    identity.st_ino,
                    identity.st_mode,
                    identity.st_uid,
                    identity.st_nlink,
                    identity.st_size,
                    identity.st_mtime_ns,
                )
                != expected_identity
                for identity in (final_active, final_active_path)
            ):
                raise LabDaemonConfigurationError(
                    "lab finalizer state identity changed after commit"
                )
        except BaseException as exc:
            if replaced and existing_descriptor >= 0:
                try:
                    active = os.stat(
                        self.path.name,
                        dir_fd=root_descriptor,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    active = None
                if active is None or self._state_identity(active) == self._state_identity(
                    temporary_identity
                ):
                    restore_name = f".state.restore.{os.getpid()}.{uuid4().hex}.tmp"
                    failed_name = f".state.failed.{os.getpid()}.{uuid4().hex}.tmp"
                    restore_descriptor = -1
                    failed_retained = False
                    try:
                        guard_mutation()
                        restore_descriptor = os.open(
                            restore_name,
                            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                            0o600,
                            dir_fd=root_descriptor,
                        )
                        os.lseek(existing_descriptor, 0, os.SEEK_SET)
                        while True:
                            chunk = os.read(existing_descriptor, 65_536)
                            if not chunk:
                                break
                            written = 0
                            while written < len(chunk):
                                written += os.write(restore_descriptor, chunk[written:])
                        os.fsync(restore_descriptor)
                        if active is None:
                            with suppress(FileExistsError):
                                guard_mutation()
                                os.link(
                                    restore_name,
                                    self.path.name,
                                    src_dir_fd=root_descriptor,
                                    dst_dir_fd=root_descriptor,
                                    follow_symlinks=False,
                                )
                        else:
                            guard_mutation()
                            os.rename(
                                self.path.name,
                                failed_name,
                                src_dir_fd=root_descriptor,
                                dst_dir_fd=root_descriptor,
                            )
                            moved = os.stat(
                                failed_name,
                                dir_fd=root_descriptor,
                                follow_symlinks=False,
                            )
                            if self._state_identity(moved) != self._state_identity(
                                temporary_identity
                            ):
                                failed_retained = True
                                with suppress(FileExistsError):
                                    guard_mutation()
                                    os.link(
                                        failed_name,
                                        self.path.name,
                                        src_dir_fd=root_descriptor,
                                        dst_dir_fd=root_descriptor,
                                        follow_symlinks=False,
                                    )
                            else:
                                with suppress(FileExistsError):
                                    guard_mutation()
                                    os.link(
                                        restore_name,
                                        self.path.name,
                                        src_dir_fd=root_descriptor,
                                        dst_dir_fd=root_descriptor,
                                        follow_symlinks=False,
                                    )
                            try:
                                os.stat(
                                    self.path.name,
                                    dir_fd=root_descriptor,
                                    follow_symlinks=False,
                                )
                            except FileNotFoundError:
                                failed_retained = True
                            else:
                                guard_mutation()
                                os.unlink(
                                    failed_name,
                                    dir_fd=root_descriptor,
                                )
                                failed_retained = False
                        os.fsync(root_descriptor)
                    finally:
                        if restore_descriptor >= 0:
                            os.close(restore_descriptor)
                        guarded_unlink_if_exists(restore_name)
                        if not failed_retained:
                            guarded_unlink_if_exists(failed_name)
            elif replaced:
                try:
                    active = os.stat(
                        self.path.name,
                        dir_fd=root_descriptor,
                        follow_symlinks=False,
                    )
                    if (active.st_dev, active.st_ino) == (
                        temporary_identity.st_dev,
                        temporary_identity.st_ino,
                    ):
                        guard_mutation()
                        os.unlink(self.path.name, dir_fd=root_descriptor)
                        os.fsync(root_descriptor)
                except OSError:
                    pass
            if isinstance(exc, OSError):
                raise LabDaemonConfigurationError(
                    "lab finalizer state could not be committed atomically"
                ) from exc
            raise
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if existing_descriptor >= 0:
                os.close(existing_descriptor)
            if writer_lock_descriptor >= 0:
                fcntl.flock(writer_lock_descriptor, fcntl.LOCK_UN)
                os.close(writer_lock_descriptor)
            guarded_unlink_if_exists(temporary_name)
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
        runtime_guard: Callable[[], str] | None = None,
        now_provider: Callable[[], datetime] | None = None,
        integrity_auditor: _IncrementalIntegrityAuditor | None = None,
        max_integrity_chain_entries: int = 16,
    ) -> None:
        if not 1 <= max_jobs_per_tick <= 128:
            raise ValueError("max_jobs_per_tick must be between 1 and 128")
        if poll_interval_ms < 1:
            raise ValueError("poll_interval_ms must be positive")
        if failure_cooldown_seconds < 1:
            raise ValueError("failure_cooldown_seconds must be positive")
        if failure_cooldown_max_seconds < failure_cooldown_seconds:
            raise ValueError("failure cooldown maximum must not be below its base")
        if not 1 <= max_integrity_chain_entries <= 128:
            raise ValueError("max_integrity_chain_entries must be between 1 and 128")
        self.reader = reader
        self.finalizer = finalizer
        self.state_store = state_store
        self.max_jobs_per_tick = max_jobs_per_tick
        self.poll_interval_ms = poll_interval_ms
        self.failure_cooldown_seconds = failure_cooldown_seconds
        self.failure_cooldown_max_seconds = failure_cooldown_max_seconds
        self.runtime_guard = runtime_guard
        self.now_provider = now_provider or (lambda: datetime.now(UTC))
        discovered_auditor = getattr(reader, "audit_incremental", None)
        if integrity_auditor is None and callable(discovered_auditor):
            integrity_auditor = reader  # type: ignore[assignment]
        self.integrity_auditor = integrity_auditor
        self.max_integrity_chain_entries = max_integrity_chain_entries
        self._stop = Event()

    def request_stop(self) -> None:
        self._stop.set()

    def _verify_runtime(self) -> None:
        if self.runtime_guard is not None:
            self.runtime_guard()

    def _audit_integrity(self) -> None:
        if self.integrity_auditor is None:
            return
        try:
            self.integrity_auditor.audit_incremental(
                max_chain_entries=self.max_integrity_chain_entries
            )
        except Exception as exc:
            message = " ".join((str(exc) or type(exc).__name__).split())[:400]
            logger.error(
                "lab-finalizer integrity audit degraded: phase=finalizer_pre_tick "
                "error_type={} message={}",
                type(exc).__name__,
                message,
            )
            raise LabIntegrityDegradedError(
                "finalizer_pre_tick: incremental ledger audit degraded: " + message
            ) from exc

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
        self._verify_runtime()
        self._audit_integrity()
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
            self._verify_runtime()
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
                self._verify_runtime()
                if result.status not in counts:
                    raise RuntimeError(f"unknown finalizer status: {result.status}")
                counts[result.status] += 1
                state.failures.pop(failure_key, None)
            except LabDaemonConfigurationError:
                raise
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
                self._verify_runtime()
                self.state_store.save(state, mutation_guard=self.runtime_guard)
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
        self._verify_runtime()
        self.state_store.save(state, mutation_guard=self.runtime_guard)
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
