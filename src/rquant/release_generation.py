"""Crash-persistent authority for one deployable checkout generation.

This module intentionally uses only the Python standard library so startup wrappers can
load it by physical file path before importing the :mod:`rquant` package.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import secrets
import shutil
import stat
import subprocess
import tomllib
from collections.abc import Callable
from contextlib import suppress
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MARKER_SCHEMA_VERSION = 1
INTENT_SCHEMA_VERSION = 1
COMMIT_SCHEMA_VERSION = 1
ENVIRONMENT_SCHEMA_VERSION = 1
MAX_MARKER_BYTES = 32 * 1024
MAX_INTENT_BYTES = 128 * 1024
MAX_ENVIRONMENT_MANIFEST_BYTES = 64 * 1024 * 1024
DEFAULT_GENERATION_GC_GRACE_SECONDS = 7 * 24 * 60 * 60
DEFAULT_GENERATION_MINIMUM_FREE_BYTES = 2 * 1024 * 1024 * 1024
GENERATION_ID_PATTERN = re.compile(r"[0-9a-f]{64}")
BUILDING_GENERATION_PATTERN = re.compile(r"\.([0-9a-f]{64})\.[0-9a-f]{16}\.building")
_VENV_RELATIVE_SYMLINKS = frozenset({"bin/python3", "lib64"})


class ReleaseGenerationError(RuntimeError):
    """The release generation cannot be trusted."""


class ReleaseGenerationRecordMissingError(ReleaseGenerationError):
    """A private release record is absent from its bound authority directory."""


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
class GenerationGcMetrics:
    scanned_generations: int
    deleted_generations: int
    reclaimed_bytes: int
    free_bytes_before: int
    free_bytes_after: int
    required_free_bytes: int
    retained_generation_ids: tuple[str, ...]


@dataclass(frozen=True)
class ReleaseGenerationMarker:
    schema_version: int
    operation_id: str
    transaction_kind: str
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
    environment_generation_id: str
    previous_generation_id: str
    environment_manifest_sha256: str
    published_at: str

    def content_hash(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> ReleaseGenerationMarker:
        try:
            return cls(
                schema_version=int(payload["schema_version"]),
                operation_id=str(payload["operation_id"]),
                transaction_kind=str(payload["transaction_kind"]),
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
                environment_generation_id=str(payload["environment_generation_id"]),
                previous_generation_id=str(payload["previous_generation_id"]),
                environment_manifest_sha256=str(payload["environment_manifest_sha256"]),
                published_at=str(payload["published_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ReleaseGenerationError("release generation marker is malformed") from exc


@dataclass(frozen=True)
class DeploymentIntent:
    schema_version: int
    operation_id: str
    previous_sha: str
    target_sha: str
    target_ref: str
    stage: str
    changed_files: tuple[str, ...]
    restart_services: tuple[str, ...]
    active_services: tuple[str, ...]
    active_timers: tuple[str, ...]
    restarted_services: tuple[str, ...]
    handoff_operation_id: str
    handoff_labels: tuple[str, ...]
    marker_generation: str
    previous_generation_id: str
    created_at: str
    updated_at: str
    stage_history: tuple[dict[str, str], ...]

    def content_hash(self) -> str:
        values = asdict(self)
        if not self.handoff_operation_id:
            values.pop("handoff_operation_id")
        if not self.handoff_labels:
            values.pop("handoff_labels")
        payload = json.dumps(values, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()

    @classmethod
    def create(
        cls,
        *,
        previous_sha: str,
        target_sha: str,
        target_ref: str,
        changed_files: tuple[str, ...],
        restart_services: tuple[str, ...],
        active_services: tuple[str, ...],
        active_timers: tuple[str, ...],
        marker_generation: str = "",
        previous_generation_id: str = "",
        handoff_operation_id: str = "",
        handoff_labels: tuple[str, ...] = (),
        operation_id: str | None = None,
        stage: str = "planned",
    ) -> DeploymentIntent:
        for label, value in (("previous", previous_sha), ("target", target_sha)):
            if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
                raise ReleaseGenerationError(f"deployment intent {label} SHA is invalid")
        timestamp = datetime.now(UTC).isoformat()
        return cls(
            schema_version=INTENT_SCHEMA_VERSION,
            operation_id=operation_id or secrets.token_hex(16),
            previous_sha=previous_sha,
            target_sha=target_sha,
            target_ref=target_ref,
            stage=stage,
            changed_files=tuple(changed_files),
            restart_services=tuple(restart_services),
            active_services=tuple(active_services),
            active_timers=tuple(active_timers),
            restarted_services=(),
            handoff_operation_id=handoff_operation_id,
            handoff_labels=tuple(handoff_labels),
            marker_generation=marker_generation,
            previous_generation_id=previous_generation_id,
            created_at=timestamp,
            updated_at=timestamp,
            stage_history=({"stage": stage, "timestamp": timestamp},),
        )

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> DeploymentIntent:
        try:
            intent = cls(
                schema_version=int(payload["schema_version"]),
                operation_id=str(payload["operation_id"]),
                previous_sha=str(payload["previous_sha"]),
                target_sha=str(payload["target_sha"]),
                target_ref=str(payload["target_ref"]),
                stage=str(payload["stage"]),
                changed_files=tuple(str(value) for value in payload["changed_files"]),
                restart_services=tuple(str(value) for value in payload["restart_services"]),
                active_services=tuple(str(value) for value in payload["active_services"]),
                active_timers=tuple(str(value) for value in payload["active_timers"]),
                restarted_services=tuple(str(value) for value in payload["restarted_services"]),
                handoff_operation_id=str(payload.get("handoff_operation_id", "")),
                handoff_labels=tuple(str(value) for value in payload.get("handoff_labels", ())),
                marker_generation=str(payload["marker_generation"]),
                previous_generation_id=str(payload.get("previous_generation_id", "")),
                created_at=str(payload["created_at"]),
                updated_at=str(payload["updated_at"]),
                stage_history=tuple(
                    {
                        "stage": str(value["stage"]),
                        "timestamp": str(value["timestamp"]),
                    }
                    for value in payload["stage_history"]
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ReleaseGenerationError("deployment intent is malformed") from exc
        if intent.schema_version != INTENT_SCHEMA_VERSION or len(intent.operation_id) != 32:
            raise ReleaseGenerationError("deployment intent schema or operation id is invalid")
        for label, value in (("previous", intent.previous_sha), ("target", intent.target_sha)):
            if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
                raise ReleaseGenerationError(f"deployment intent {label} SHA is invalid")
        if intent.previous_generation_id and len(intent.previous_generation_id) != 64:
            raise ReleaseGenerationError("deployment intent previous generation is invalid")
        if intent.handoff_operation_id and not re.fullmatch(
            r"[0-9a-f]{32}", intent.handoff_operation_id
        ):
            raise ReleaseGenerationError("deployment handoff operation is invalid")
        if bool(intent.handoff_operation_id) != bool(intent.handoff_labels):
            raise ReleaseGenerationError("deployment handoff binding is incomplete")
        return intent

    def advance(
        self,
        *,
        stage: str,
        restarted_services: tuple[str, ...] | None = None,
    ) -> DeploymentIntent:
        timestamp = datetime.now(UTC).isoformat()
        return replace(
            self,
            stage=stage,
            restarted_services=(
                self.restarted_services if restarted_services is None else tuple(restarted_services)
            ),
            updated_at=timestamp,
            stage_history=(*self.stage_history, {"stage": stage, "timestamp": timestamp}),
        )


@dataclass(frozen=True)
class EnvironmentSelector:
    schema_version: int
    operation_id: str
    transaction_kind: str
    commit: str
    generation_id: str
    previous_generation_id: str
    environment_path: str
    manifest_name: str
    manifest_sha256: str
    published_at: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> EnvironmentSelector:
        try:
            selector = cls(
                schema_version=int(payload["schema_version"]),
                operation_id=str(payload["operation_id"]),
                transaction_kind=str(payload["transaction_kind"]),
                commit=str(payload["commit"]),
                generation_id=str(payload["generation_id"]),
                previous_generation_id=str(payload["previous_generation_id"]),
                environment_path=str(payload["environment_path"]),
                manifest_name=str(payload["manifest_name"]),
                manifest_sha256=str(payload["manifest_sha256"]),
                published_at=str(payload["published_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ReleaseGenerationError("environment selector is malformed") from exc
        if (
            selector.schema_version != ENVIRONMENT_SCHEMA_VERSION
            or len(selector.operation_id) != 32
            or selector.transaction_kind not in {"deployment", "initialization"}
            or len(selector.commit) != 40
            or len(selector.generation_id) != 64
            or (
                selector.previous_generation_id != "" and len(selector.previous_generation_id) != 64
            )
            or len(selector.manifest_sha256) != 64
        ):
            raise ReleaseGenerationError("environment selector is invalid")
        return selector


@dataclass(frozen=True)
class ReleaseGenerationCommit:
    schema_version: int
    operation_id: str
    transaction_kind: str
    commit: str
    marker_sha256: str
    transaction_sha256: str
    environment_generation_id: str
    previous_generation_id: str
    environment_manifest_sha256: str
    committed_at: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> ReleaseGenerationCommit:
        try:
            record = cls(
                schema_version=int(payload["schema_version"]),
                operation_id=str(payload["operation_id"]),
                transaction_kind=str(payload["transaction_kind"]),
                commit=str(payload["commit"]),
                marker_sha256=str(payload["marker_sha256"]),
                transaction_sha256=str(payload["transaction_sha256"]),
                environment_generation_id=str(payload["environment_generation_id"]),
                previous_generation_id=str(payload["previous_generation_id"]),
                environment_manifest_sha256=str(payload["environment_manifest_sha256"]),
                committed_at=str(payload["committed_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ReleaseGenerationError("release generation commit record is malformed") from exc
        if (
            record.schema_version != COMMIT_SCHEMA_VERSION
            or len(record.operation_id) != 32
            or record.transaction_kind not in {"deployment", "initialization"}
            or len(record.commit) != 40
            or any(
                len(value) != 64
                for value in (
                    record.marker_sha256,
                    record.transaction_sha256,
                    record.environment_generation_id,
                    record.environment_manifest_sha256,
                )
            )
            or (record.previous_generation_id != "" and len(record.previous_generation_id) != 64)
        ):
            raise ReleaseGenerationError("release generation commit record is invalid")
        return record


def marker_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.complete.json")


def intent_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.intent.json")


def initialization_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.initialized.json")


def commit_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.commit.json")


def environment_selector_path_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.environment.json")


def environment_root_for_lock(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.venvs")


def environment_manifest_path_for_lock(lock_path: Path, generation_id: str) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.venv-{generation_id}.manifest.json")


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


def _trusted_executable_binding(path: Path, *, label: str) -> dict[str, object]:
    canonical = _canonical(path, label=label)
    try:
        observed = canonical.lstat()
    except OSError as exc:
        raise ReleaseGenerationError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid not in {0, os.getuid()}
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
        or canonical.resolve(strict=True) != canonical
    ):
        raise ReleaseGenerationError(f"{label} has unsafe identity")
    digest = hashlib.sha256()
    try:
        with canonical.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        active = canonical.lstat()
    except OSError as exc:
        raise ReleaseGenerationError(f"{label} cannot be read") from exc
    if PathIdentity.capture(active) != PathIdentity.capture(observed):
        raise ReleaseGenerationError(f"{label} identity changed while reading")
    return {
        "physical_path": str(canonical),
        "identity": asdict(PathIdentity.capture(observed)),
        "sha256": digest.hexdigest(),
    }


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


def _verified_interpreter(path: Path, *, label: str) -> tuple[Path, PathIdentity, str]:
    try:
        resolved = path.resolve(strict=True)
        observed = resolved.lstat()
    except OSError as exc:
        raise ReleaseGenerationError(f"{label} cannot be resolved") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_nlink != 1
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
    ):
        raise ReleaseGenerationError(f"{label} has unsafe identity")
    return resolved, PathIdentity.capture(observed), _hash_file(resolved, label=label)


def _venv_system_interpreter(python_path: Path) -> tuple[Path, PathIdentity, str]:
    try:
        result = subprocess.run(
            [
                str(python_path),
                "-I",
                "-S",
                "-c",
                "import sys; print(sys._base_executable)",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseGenerationError("deployment system Python cannot be discovered") from exc
    raw_path = result.stdout.strip()
    if not raw_path:
        raise ReleaseGenerationError("deployment system Python is empty")
    return _verified_interpreter(Path(raw_path), label="deployment system Python")


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        try:
            written = os.write(descriptor, payload[offset:])
        except OSError as exc:
            raise ReleaseGenerationError("release generation marker cannot be written") from exc
        if written <= 0:
            raise ReleaseGenerationError("release generation marker write made no progress")
        offset += written


def _verify_temporary_payload(
    descriptor: int,
    *,
    expected_payload: bytes,
    expected_marker: ReleaseGenerationMarker,
) -> None:
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, MAX_MARKER_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_MARKER_BYTES:
                raise ReleaseGenerationError("temporary release marker is too large")
        observed_payload = b"".join(chunks)
        observed_marker = ReleaseGenerationMarker.from_payload(json.loads(observed_payload))
    except ReleaseGenerationError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseGenerationError("temporary release marker cannot be verified") from exc
    if (
        len(observed_payload) != len(expected_payload)
        or hashlib.sha256(observed_payload).digest() != hashlib.sha256(expected_payload).digest()
        or observed_marker != expected_marker
    ):
        raise ReleaseGenerationError("temporary release marker content mismatch")


def _read_private_json(
    *,
    root_fd: int,
    root_path: Path,
    name: str,
    maximum_bytes: int,
) -> tuple[dict[str, Any], PathIdentity]:
    descriptor = -1
    try:
        descriptor = os.open(
            name,
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
            raise ReleaseGenerationError(f"private deployment record {name} is unsafe")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise ReleaseGenerationError(f"private deployment record {name} is too large")
        active = (root_path / name).lstat()
        identity = PathIdentity.capture(opened)
        if identity != PathIdentity.capture(active):
            raise ReleaseGenerationError(f"private deployment record {name} identity changed")
        payload = json.loads(b"".join(chunks))
        if not isinstance(payload, dict):
            raise ReleaseGenerationError(f"private deployment record {name} is malformed")
        return payload, identity
    except ReleaseGenerationError:
        raise
    except FileNotFoundError as exc:
        raise ReleaseGenerationRecordMissingError(
            f"private deployment record {name} is missing"
        ) from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseGenerationError(f"private deployment record {name} cannot be read") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _write_private_json(
    *,
    root_fd: int,
    root_path: Path,
    name: str,
    payload: dict[str, Any],
    require_absent: bool,
    expected_identity: PathIdentity | None = None,
    maximum_bytes: int = MAX_INTENT_BYTES,
) -> None:
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if len(encoded) > maximum_bytes:
        raise ReleaseGenerationError(f"private deployment record {name} is too large")
    temporary_name = f".{name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    descriptor = -1
    published = False
    try:
        descriptor = os.open(
            temporary_name,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=root_fd,
        )
        _write_all(descriptor, encoded)
        os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        observed = os.read(descriptor, maximum_bytes + 1)
        if (
            observed != encoded
            or hashlib.sha256(observed).digest() != hashlib.sha256(encoded).digest()
            or not isinstance(json.loads(observed), dict)
        ):
            raise ReleaseGenerationError(f"private deployment record {name} verification failed")
        if require_absent:
            try:
                os.link(
                    temporary_name,
                    name,
                    src_dir_fd=root_fd,
                    dst_dir_fd=root_fd,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise ReleaseGenerationError(
                    f"private deployment record {name} already exists"
                ) from exc
            os.unlink(temporary_name, dir_fd=root_fd)
        else:
            if expected_identity is None:
                raise ReleaseGenerationError("deployment record update lacks an identity fence")
            active = (root_path / name).lstat()
            if PathIdentity.capture(active) != expected_identity:
                raise ReleaseGenerationError(f"private deployment record {name} changed")
            os.replace(temporary_name, name, src_dir_fd=root_fd, dst_dir_fd=root_fd)
        published = True
        os.fsync(root_fd)
        active = (root_path / name).lstat()
        if PathIdentity.capture(os.fstat(descriptor)) != PathIdentity.capture(active):
            raise ReleaseGenerationError(f"private deployment record {name} publish changed")
    except ReleaseGenerationError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseGenerationError(f"private deployment record {name} cannot be written") from exc
    finally:
        if not published:
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=root_fd)
        if descriptor >= 0:
            os.close(descriptor)


def _payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _environment_generation_id(*, operation_id: str, commit: str) -> str:
    return hashlib.sha256(f"{operation_id}:{commit}".encode()).hexdigest()


def _nonnegative_float_setting(value: float | None, *, env_name: str, default: float) -> float:
    raw: object = os.environ.get(env_name, default) if value is None else value
    try:
        parsed = float(raw)
    except (TypeError, ValueError) as exc:
        raise ReleaseGenerationError(f"{env_name} must be a non-negative number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ReleaseGenerationError(f"{env_name} must be a non-negative number")
    return parsed


def _nonnegative_int_setting(value: int | None, *, env_name: str, default: int) -> int:
    raw: object = os.environ.get(env_name, default) if value is None else value
    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise ReleaseGenerationError(f"{env_name} must be a non-negative integer") from exc
    if parsed < 0:
        raise ReleaseGenerationError(f"{env_name} must be a non-negative integer")
    return parsed


def _private_tree_size(
    root: Path,
    *,
    system_python: Path,
    system_python_identity: PathIdentity,
    system_python_sha256: str,
) -> int:
    total = 0
    for current_root, directory_names, file_names in os.walk(root):
        current = Path(current_root)
        observed_root = current.lstat()
        if (
            not stat.S_ISDIR(observed_root.st_mode)
            or stat.S_ISLNK(observed_root.st_mode)
            or observed_root.st_uid != os.getuid()
        ):
            raise ReleaseGenerationError("source release venv contains an unsafe directory")
        for name in (*directory_names, *file_names):
            path = current / name
            observed = path.lstat()
            if observed.st_uid != os.getuid():
                raise ReleaseGenerationError("source release venv contains an unsafe object")
            if stat.S_ISLNK(observed.st_mode):
                _environment_entry(
                    path,
                    root,
                    system_python=system_python,
                    system_python_identity=system_python_identity,
                    system_python_sha256=system_python_sha256,
                )
                continue
            if stat.S_ISREG(observed.st_mode):
                if observed.st_nlink != 1:
                    raise ReleaseGenerationError("source release venv contains a hardlink")
                total += observed.st_size
            elif not stat.S_ISDIR(observed.st_mode):
                raise ReleaseGenerationError("source release venv contains an unsafe object")
    return total


def _remove_private_tree_at(parent_fd: int, name: str) -> int:
    descriptor = os.open(
        name,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=parent_fd,
    )
    reclaimed = 0
    try:
        opened = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            PathIdentity.capture(opened) != PathIdentity.capture(active)
            or not stat.S_ISDIR(opened.st_mode)
            or opened.st_uid != os.getuid()
        ):
            raise ReleaseGenerationError("orphan environment generation is unsafe")
        os.fchmod(descriptor, 0o700)
        for child_name in os.listdir(descriptor):
            child = os.stat(child_name, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISLNK(child.st_mode):
                if child.st_uid != os.getuid():
                    raise ReleaseGenerationError(
                        "orphan environment generation contains unsafe data"
                    )
                os.unlink(child_name, dir_fd=descriptor)
                continue
            if stat.S_ISDIR(child.st_mode) and not stat.S_ISLNK(child.st_mode):
                reclaimed += _remove_private_tree_at(descriptor, child_name)
                continue
            if (
                not stat.S_ISREG(child.st_mode)
                or stat.S_ISLNK(child.st_mode)
                or child.st_uid != os.getuid()
                or child.st_nlink != 1
            ):
                raise ReleaseGenerationError("orphan environment generation contains unsafe data")
            child_fd = os.open(
                child_name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptor,
            )
            try:
                if PathIdentity.capture(os.fstat(child_fd)) != PathIdentity.capture(child):
                    raise ReleaseGenerationError("orphan environment generation changed")
                os.fchmod(child_fd, 0o600)
                reclaimed += child.st_size
            finally:
                os.close(child_fd)
            os.unlink(child_name, dir_fd=descriptor)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.rmdir(name, dir_fd=parent_fd)
    return reclaimed


def _environment_entry(
    path: Path,
    root: Path,
    *,
    system_python: Path,
    system_python_identity: PathIdentity,
    system_python_sha256: str,
) -> dict[str, Any]:
    observed = path.lstat()
    relative = path.relative_to(root).as_posix()
    if stat.S_ISLNK(observed.st_mode):
        version, _abi = _python_facts(system_python)
        major_minor = ".".join(version.split(".")[:2])
        allowed = _VENV_RELATIVE_SYMLINKS | {
            "bin/python",
            f"bin/python{major_minor}",
        }
        if relative not in allowed or observed.st_uid != os.getuid():
            raise ReleaseGenerationError("environment generation contains an unsafe symlink")
        target_text = os.readlink(path)
        target = Path(target_text)
        if relative != "bin/python" and (target.is_absolute() or ".." in target.parts):
            raise ReleaseGenerationError("environment generation symlink target is unsafe")
        try:
            resolved = path.resolve(strict=True)
            resolved_observed = resolved.lstat()
        except OSError as exc:
            raise ReleaseGenerationError("environment generation symlink is broken") from exc
        if relative.startswith("bin/python"):
            if (
                resolved != system_python
                or PathIdentity.capture(resolved_observed) != system_python_identity
                or _hash_file(resolved, label="system Python") != system_python_sha256
            ):
                raise ReleaseGenerationError("environment generation Python symlink target changed")
        elif relative == "lib64" and not resolved.is_relative_to(root):
            raise ReleaseGenerationError("environment generation lib64 escapes its root")
        return {
            "path": relative,
            "kind": "symlink",
            "mode": stat.S_IMODE(observed.st_mode),
            "size": observed.st_size,
            "mtime_ns": observed.st_mtime_ns,
            "ctime_ns": observed.st_ctime_ns,
            "sha256": "",
            "link_target": target_text,
            "resolved_target": str(resolved),
        }
    if stat.S_ISDIR(observed.st_mode):
        kind = "directory"
        digest = ""
        size = 0
    elif stat.S_ISREG(observed.st_mode) and not stat.S_ISLNK(observed.st_mode):
        if observed.st_nlink != 1:
            raise ReleaseGenerationError("environment generation contains a hardlink")
        kind = "file"
        digest = _hash_file(path, label=f"environment file {relative}")
        size = observed.st_size
    else:
        raise ReleaseGenerationError("environment generation contains an unsafe object")
    if observed.st_uid != os.getuid() or observed.st_mode & 0o077:
        raise ReleaseGenerationError("environment generation is not owner-private")
    return {
        "path": relative,
        "kind": kind,
        "mode": stat.S_IMODE(observed.st_mode),
        "size": size,
        "mtime_ns": observed.st_mtime_ns,
        "ctime_ns": observed.st_ctime_ns,
        "sha256": digest,
    }


def _freeze_environment(
    root: Path,
    *,
    system_python: Path,
    system_python_identity: PathIdentity,
    system_python_sha256: str,
) -> None:
    for current_root, directory_names, file_names in os.walk(root, topdown=False):
        current = Path(current_root)
        for name in file_names:
            path = current / name
            observed = path.lstat()
            if stat.S_ISLNK(observed.st_mode):
                _environment_entry(
                    path,
                    root,
                    system_python=system_python,
                    system_python_identity=system_python_identity,
                    system_python_sha256=system_python_sha256,
                )
                continue
            if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
                raise ReleaseGenerationError("environment generation contains a symlink")
            path.chmod(0o500 if observed.st_mode & stat.S_IXUSR else 0o400)
        for name in directory_names:
            path = current / name
            observed = path.lstat()
            if stat.S_ISLNK(observed.st_mode):
                _environment_entry(
                    path,
                    root,
                    system_python=system_python,
                    system_python_identity=system_python_identity,
                    system_python_sha256=system_python_sha256,
                )
                continue
            if not stat.S_ISDIR(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
                raise ReleaseGenerationError("environment generation contains a symlink")
            path.chmod(0o500)
    root.chmod(0o500)


def _rebind_environment_console_scripts(
    staging_path: Path,
    final_path: Path,
    source_venv: Path,
) -> None:
    bin_path = staging_path / "bin"
    for path in bin_path.iterdir():
        observed = path.lstat()
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
            continue
        payload = path.read_bytes()
        lines = payload.splitlines(keepends=True)
        if lines and lines[0].startswith(b"#!"):
            interpreter = lines[0][2:].rstrip(b"\r\n")
            for root in (staging_path, source_venv):
                source_bin = str(root / "bin").encode() + b"/"
                if interpreter.startswith(source_bin):
                    name = interpreter[len(source_bin) :]
                    if name.startswith(b"python") and b"/" not in name:
                        suffix = lines[0][2 + len(interpreter) :]
                        lines[0] = b"#!" + str(final_path / "bin").encode() + b"/" + name + suffix
                        path.write_bytes(b"".join(lines))
                    break
        rebound = path.read_bytes()
        if str(staging_path).encode() in rebound or str(source_venv).encode() in rebound:
            raise ReleaseGenerationError(
                "environment console script retains a staging or source path"
            )


def _environment_manifest(
    root: Path,
    *,
    operation_id: str,
    transaction_kind: str,
    commit: str,
    generation_id: str,
    system_python: Path,
    system_python_identity: PathIdentity,
    system_python_sha256: str,
    uv_binding: dict[str, object] | None,
) -> dict[str, Any]:
    entry_arguments = {
        "system_python": system_python,
        "system_python_identity": system_python_identity,
        "system_python_sha256": system_python_sha256,
    }
    entries = [_environment_entry(root, root, **entry_arguments)]
    entries.extend(
        _environment_entry(path, root, **entry_arguments)
        for path in sorted(root.rglob("*"), key=lambda value: value.as_posix())
    )
    return {
        "schema_version": ENVIRONMENT_SCHEMA_VERSION,
        "operation_id": operation_id,
        "transaction_kind": transaction_kind,
        "commit": commit,
        "generation_id": generation_id,
        "environment_path": str(root),
        "system_python_path": str(system_python),
        "system_python_identity": asdict(system_python_identity),
        "system_python_sha256": system_python_sha256,
        "uv_binding": uv_binding or {},
        "entries": entries,
    }


def _verify_environment_manifest(root: Path, manifest: dict[str, Any]) -> None:
    if int(manifest.get("schema_version", 0)) != ENVIRONMENT_SCHEMA_VERSION or manifest.get(
        "environment_path"
    ) != str(root):
        raise ReleaseGenerationError("environment generation manifest is invalid")
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ReleaseGenerationError("environment generation manifest has no entries")
    try:
        system_python = _canonical(
            Path(str(manifest["system_python_path"])),
            label="system Python",
        )
        expected_system_identity = PathIdentity(**manifest["system_python_identity"])
        expected_system_sha256 = str(manifest["system_python_sha256"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ReleaseGenerationError("environment generation interpreter is malformed") from exc
    resolved_system, system_identity, system_sha256 = _verified_interpreter(
        system_python,
        label="system Python",
    )
    if (
        resolved_system != system_python
        or system_identity != expected_system_identity
        or system_sha256 != expected_system_sha256
    ):
        raise ReleaseGenerationError("environment generation interpreter changed")
    uv_binding = manifest.get("uv_binding", {})
    if not isinstance(uv_binding, dict):
        raise ReleaseGenerationError("environment generation uv binding is malformed")
    if uv_binding:
        try:
            uv_path = Path(str(uv_binding["physical_path"]))
            uv_identity = PathIdentity(**uv_binding["identity"])
            uv_sha256 = str(uv_binding["sha256"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ReleaseGenerationError("environment generation uv binding is malformed") from exc
        observed_uv = _trusted_executable_binding(uv_path, label="release uv")
        if (
            PathIdentity(**observed_uv["identity"]) != uv_identity
            or observed_uv["sha256"] != uv_sha256
        ):
            raise ReleaseGenerationError("environment generation uv binding changed")
    expected_paths: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise ReleaseGenerationError("environment generation manifest is malformed")
        relative = str(entry["path"])
        path = root if relative == "." else root / relative
        if relative in expected_paths or (relative and Path(relative).is_absolute()):
            raise ReleaseGenerationError("environment generation manifest path is invalid")
        expected_paths.add(relative)
        observed = _environment_entry(
            path,
            root,
            system_python=system_python,
            system_python_identity=system_identity,
            system_python_sha256=system_sha256,
        )
        if observed != entry:
            raise ReleaseGenerationError("environment generation content changed")
    actual_paths = {"."}
    actual_paths.update(path.relative_to(root).as_posix() for path in root.rglob("*"))
    if actual_paths != expected_paths:
        raise ReleaseGenerationError("environment generation namespace changed")


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
        gc_grace_seconds: float | None = None,
        minimum_free_bytes: int | None = None,
        uv_path: Path | None = None,
        environment_builder: Callable[[Path], None] | None = None,
    ) -> None:
        self.repo = _canonical(repo, label="release checkout")
        self.lock_path = _canonical(lock_path, label="deployment lock")
        self.marker_path = marker_path_for_lock(self.lock_path)
        self.intent_path = intent_path_for_lock(self.lock_path)
        self.initialization_path = initialization_path_for_lock(self.lock_path)
        self.commit_path = commit_path_for_lock(self.lock_path)
        self.handoff_path = self.lock_path.with_name(f"{self.lock_path.stem}.lab-handoff.json")
        self.environment_selector_path = environment_selector_path_for_lock(self.lock_path)
        self.environment_root = environment_root_for_lock(self.lock_path)
        self.lock_fd = lock_fd
        self.python_path = _canonical(python_path, label="release Python")
        self.git_path = _canonical(git_path, label="trusted Git")
        self.uv_path = None if uv_path is None else _canonical(uv_path, label="release uv")
        self.uv_binding = (
            None
            if self.uv_path is None
            else _trusted_executable_binding(self.uv_path, label="release uv")
        )
        self._environment_builder = environment_builder
        self.writable = writable
        self._mutation_hook = mutation_hook or (lambda _stage: None)
        self.gc_grace_seconds = _nonnegative_float_setting(
            gc_grace_seconds,
            env_name="RQUANT_RELEASE_GENERATION_GC_GRACE_SECONDS",
            default=DEFAULT_GENERATION_GC_GRACE_SECONDS,
        )
        self.minimum_free_bytes = _nonnegative_int_setting(
            minimum_free_bytes,
            env_name="RQUANT_RELEASE_GENERATION_MIN_FREE_BYTES",
            default=DEFAULT_GENERATION_MINIMUM_FREE_BYTES,
        )
        self._assert_lock()
        if self.writable:
            self._assert_exclusive_lock()

    def _build_environment(self, destination: Path, *, system_python: Path) -> None:
        if self._environment_builder is not None:
            self._environment_builder(destination)
            return
        if self.uv_path is None:
            raise ReleaseGenerationError("writable generation authority requires release uv")
        environment = {
            **os.environ,
            "VIRTUAL_ENV": str(destination),
            "UV_PROJECT_ENVIRONMENT": str(destination),
        }
        commands = (
            [
                str(self.uv_path),
                "venv",
                "--allow-existing",
                "--relocatable",
                "--python",
                str(system_python),
                str(destination),
            ],
            [str(self.uv_path), "sync", "--frozen", "--active"],
        )
        for command in commands:
            try:
                result = subprocess.run(
                    command,
                    cwd=self.repo,
                    env=environment,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=900,
                )
            except (OSError, subprocess.SubprocessError) as exc:
                raise ReleaseGenerationError("immutable release environment build failed") from exc
            if result.returncode != 0:
                diagnostic = (result.stderr or result.stdout or "no command output").strip()
                raise ReleaseGenerationError(
                    f"immutable release environment build failed: {diagnostic[:1000]}"
                )

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

    def _assert_exclusive_lock(self) -> None:
        try:
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ReleaseGenerationError(
                "writable generation authority requires the exclusive deployment lock"
            ) from exc

    def _facts(
        self,
        *,
        expected_commit: str,
        selector: EnvironmentSelector,
        manifest: dict[str, Any],
    ) -> ReleaseGenerationMarker:
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
        if selector.commit != commit or str(manifest.get("commit")) != commit:
            raise ReleaseGenerationError("environment generation commit is stale")
        venv = _canonical(Path(selector.environment_path), label="release environment")
        _verify_environment_manifest(venv, manifest)
        venv_identity = _identity(venv, label="release venv", directory=True)
        selected_python = venv / "bin" / "python"
        try:
            system_python = Path(str(manifest["system_python_path"]))
            expected_system_identity = PathIdentity(**manifest["system_python_identity"])
            expected_system_sha256 = str(manifest["system_python_sha256"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ReleaseGenerationError("release interpreter binding is malformed") from exc
        selected_observed = selected_python.lstat()
        if stat.S_ISLNK(selected_observed.st_mode):
            resolved_python, python_identity, python_sha256 = _verified_interpreter(
                selected_python,
                label="release venv Python",
            )
            if (
                resolved_python != system_python
                or python_identity != expected_system_identity
                or python_sha256 != expected_system_sha256
            ):
                raise ReleaseGenerationError("release venv Python binding changed")
        else:
            python_identity = _identity(
                selected_python,
                label="release venv Python",
                directory=False,
            )
        version, abi = _python_facts(selected_python)
        major_minor = ".".join(version.split(".")[:2])
        site_packages = venv / "lib" / f"python{major_minor}" / "site-packages"
        site_identity = _identity(
            site_packages,
            label="release site-packages",
            directory=True,
        )
        return ReleaseGenerationMarker(
            schema_version=MARKER_SCHEMA_VERSION,
            operation_id=selector.operation_id,
            transaction_kind=selector.transaction_kind,
            commit=commit,
            uv_lock_sha256=uv_hash,
            pyproject_sha256=pyproject_hash,
            package_version=package_version,
            python_version=version,
            python_abi=abi,
            venv_path=str(venv),
            venv_identity=venv_identity,
            pyvenv_cfg_sha256=_hash_file(venv / "pyvenv.cfg", label="pyvenv.cfg"),
            python_path=str(selected_python),
            python_identity=python_identity,
            site_packages_path=str(site_packages),
            site_packages_identity=site_identity,
            environment_generation_id=selector.generation_id,
            previous_generation_id=selector.previous_generation_id,
            environment_manifest_sha256=selector.manifest_sha256,
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

    def _optional_private_payload(
        self,
        path: Path,
        *,
        maximum_bytes: int,
    ) -> dict[str, Any] | None:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            try:
                payload, _identity_value = _read_private_json(
                    root_fd=root_fd,
                    root_path=self.lock_path.parent,
                    name=path.name,
                    maximum_bytes=maximum_bytes,
                )
            except ReleaseGenerationRecordMissingError:
                self._assert_root(root_fd, root_identity)
                return None
            self._assert_root(root_fd, root_identity)
            return payload
        finally:
            os.close(root_fd)

    def _retained_environment_ids(self, environment_fd: int) -> set[str]:
        del environment_fd
        retained: set[str] = set()
        selector_payload = self._optional_private_payload(
            self.environment_selector_path,
            maximum_bytes=MAX_MARKER_BYTES,
        )
        if selector_payload is not None:
            selector = EnvironmentSelector.from_payload(selector_payload)
            retained.update(
                value
                for value in (selector.generation_id, selector.previous_generation_id)
                if value
            )
        marker_payload = self._optional_private_payload(
            self.marker_path,
            maximum_bytes=MAX_MARKER_BYTES,
        )
        if marker_payload is not None:
            marker = ReleaseGenerationMarker.from_payload(marker_payload)
            retained.update(
                value
                for value in (
                    marker.environment_generation_id,
                    marker.previous_generation_id,
                )
                if value
            )
        commit_payload = self._optional_private_payload(
            self.commit_path,
            maximum_bytes=MAX_MARKER_BYTES,
        )
        if commit_payload is not None:
            commit_record = ReleaseGenerationCommit.from_payload(commit_payload)
            retained.update(
                value
                for value in (
                    commit_record.environment_generation_id,
                    commit_record.previous_generation_id,
                )
                if value
            )
        for path in (self.intent_path, self.initialization_path):
            payload = self._optional_private_payload(path, maximum_bytes=MAX_INTENT_BYTES)
            if payload is None:
                continue
            intent = DeploymentIntent.from_payload(payload)
            if intent.previous_generation_id:
                retained.add(intent.previous_generation_id)
            retained.update(
                _environment_generation_id(operation_id=intent.operation_id, commit=commit)
                for commit in (intent.previous_sha, intent.target_sha)
            )
        return retained

    def _append_generation_gc_audit(self, payload: dict[str, Any]) -> None:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        name = f"{self.lock_path.stem}.generation-gc.jsonl"
        descriptor = -1
        try:
            self._assert_root(root_fd, root_identity)
            descriptor = os.open(
                name,
                os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_fd,
            )
            opened = os.fstat(descriptor)
            active = (self.lock_path.parent / name).lstat()
            if (
                PathIdentity.capture(opened) != PathIdentity.capture(active)
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or opened.st_nlink != 1
                or stat.S_IMODE(opened.st_mode) != 0o600
            ):
                raise ReleaseGenerationError("generation GC audit is unsafe")
            _write_all(
                descriptor,
                (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(),
            )
            os.fsync(descriptor)
            self._assert_root(root_fd, root_identity)
        except ReleaseGenerationError:
            raise
        except OSError as exc:
            raise ReleaseGenerationError("generation GC audit cannot be written") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(root_fd)

    def _remove_orphan_environment_manifest(self, generation_id: str) -> None:
        manifest_path = environment_manifest_path_for_lock(self.lock_path, generation_id)
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            try:
                payload, identity = _read_private_json(
                    root_fd=root_fd,
                    root_path=self.lock_path.parent,
                    name=manifest_path.name,
                    maximum_bytes=MAX_ENVIRONMENT_MANIFEST_BYTES,
                )
            except ReleaseGenerationRecordMissingError:
                self._assert_root(root_fd, root_identity)
                return
            if payload.get("generation_id") != generation_id or payload.get(
                "environment_path"
            ) != str(self.environment_root / generation_id):
                raise ReleaseGenerationError("orphan environment manifest binding changed")
            self._mutation_hook("before_environment_gc_manifest_delete")
            active = manifest_path.lstat()
            if PathIdentity.capture(active) != identity:
                raise ReleaseGenerationError("orphan environment manifest identity changed")
            self._assert_root(root_fd, root_identity)
            os.unlink(manifest_path.name, dir_fd=root_fd)
            os.fsync(root_fd)
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)

    def garbage_collect_environments(
        self,
        *,
        reason: str,
        required_bytes: int = 0,
    ) -> GenerationGcMetrics:
        if not self.writable:
            raise ReleaseGenerationError(
                "read-only generation authority cannot collect environments"
            )
        if required_bytes < 0:
            raise ReleaseGenerationError("generation disk requirement cannot be negative")
        self._assert_lock()
        self._assert_exclusive_lock()
        environment_fd, environment_identity = self._ensure_environment_root()
        free_before = shutil.disk_usage(self.environment_root).free
        scanned = 0
        deleted = 0
        reclaimed = 0
        try:
            retained = self._retained_environment_ids(environment_fd)
            cutoff = datetime.now(UTC).timestamp() - self.gc_grace_seconds
            for name in sorted(os.listdir(environment_fd)):
                generation_match = GENERATION_ID_PATTERN.fullmatch(name)
                building_match = BUILDING_GENERATION_PATTERN.fullmatch(name)
                if generation_match is None and building_match is None:
                    continue
                generation_id = name if generation_match is not None else building_match.group(1)
                scanned += 1
                observed = os.stat(name, dir_fd=environment_fd, follow_symlinks=False)
                if (
                    not stat.S_ISDIR(observed.st_mode)
                    or stat.S_ISLNK(observed.st_mode)
                    or observed.st_uid != os.getuid()
                ):
                    raise ReleaseGenerationError("release environment generation is unsafe")
                if generation_id in retained or observed.st_mtime > cutoff:
                    continue
                self._mutation_hook("before_environment_gc_delete")
                active_environment = _identity(
                    self.environment_root,
                    label="release environment root",
                    directory=True,
                )
                if _object_key(active_environment) != _object_key(
                    environment_identity
                ) or _object_key(PathIdentity.capture(os.fstat(environment_fd))) != _object_key(
                    environment_identity
                ):
                    raise ReleaseGenerationError("release environment root identity changed")
                if generation_match is not None:
                    self._remove_orphan_environment_manifest(generation_id)
                reclaimed += _remove_private_tree_at(environment_fd, name)
                os.fsync(environment_fd)
                deleted += 1
            free_after = shutil.disk_usage(self.environment_root).free
            required_free = self.minimum_free_bytes + required_bytes
            status = "ok" if free_after >= required_free else "disk_budget_blocked"
            metrics = GenerationGcMetrics(
                scanned_generations=scanned,
                deleted_generations=deleted,
                reclaimed_bytes=reclaimed,
                free_bytes_before=free_before,
                free_bytes_after=free_after,
                required_free_bytes=required_free,
                retained_generation_ids=tuple(sorted(retained)),
            )
            self._append_generation_gc_audit(
                {
                    "deleted_generations": deleted,
                    "free_bytes_after": free_after,
                    "free_bytes_before": free_before,
                    "reason": reason,
                    "reclaimed_bytes": reclaimed,
                    "required_free_bytes": required_free,
                    "retained_generation_ids": list(metrics.retained_generation_ids),
                    "scanned_generations": scanned,
                    "status": status,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            )
            if free_after < required_free:
                raise ReleaseGenerationError(
                    "release environment disk budget is insufficient: "
                    f"free={free_after} required={required_free}"
                )
            return metrics
        finally:
            os.close(environment_fd)

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

    def commit_generation(
        self,
        *,
        operation_id: str,
        transaction_kind: str,
    ) -> ReleaseGenerationCommit:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot commit")
        self._assert_lock()
        marker = self._read_marker()
        if marker.operation_id != operation_id or marker.transaction_kind != transaction_kind:
            raise ReleaseGenerationError("release marker transaction binding changed")
        transaction = self._transaction_record(
            operation_id=operation_id,
            transaction_kind=transaction_kind,
        )
        if transaction.stage != "completed":
            raise ReleaseGenerationError("release transaction is not completed")
        selector, _selector_identity = self._read_selector()
        manifest, _manifest_identity = self._read_environment_manifest(selector)
        current = self._facts(
            expected_commit=marker.commit,
            selector=selector,
            manifest=manifest,
        )
        if self._comparable(current) != self._comparable(marker):
            raise ReleaseGenerationError("release marker changed before commit")
        record = ReleaseGenerationCommit(
            schema_version=COMMIT_SCHEMA_VERSION,
            operation_id=operation_id,
            transaction_kind=transaction_kind,
            commit=marker.commit,
            marker_sha256=marker.content_hash(),
            transaction_sha256=transaction.content_hash(),
            environment_generation_id=marker.environment_generation_id,
            previous_generation_id=marker.previous_generation_id,
            environment_manifest_sha256=marker.environment_manifest_sha256,
            committed_at=datetime.now(UTC).isoformat(),
        )
        try:
            existing = self._read_commit_record()
        except ReleaseGenerationRecordMissingError:
            existing_identity = None
        else:
            if replace(existing, committed_at=record.committed_at) == record:
                return existing
            root_fd, root_identity = _private_lock_root(self.lock_path.parent)
            try:
                _payload, existing_identity = _read_private_json(
                    root_fd=root_fd,
                    root_path=self.lock_path.parent,
                    name=self.commit_path.name,
                    maximum_bytes=MAX_MARKER_BYTES,
                )
                self._assert_root(root_fd, root_identity)
            finally:
                os.close(root_fd)
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            self._mutation_hook("before_generation_commit")
            self._assert_root(root_fd, root_identity)
            _write_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=self.commit_path.name,
                payload=asdict(record),
                require_absent=existing_identity is None,
                expected_identity=existing_identity,
                maximum_bytes=MAX_MARKER_BYTES,
            )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)
        self._mutation_hook("generation_committed")
        return record

    @staticmethod
    def _comparable(marker: ReleaseGenerationMarker) -> dict[str, Any]:
        payload = asdict(marker)
        payload.pop("published_at", None)
        return payload

    def _read_selector(self) -> tuple[EnvironmentSelector, PathIdentity]:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            payload, identity = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=self.environment_selector_path.name,
                maximum_bytes=MAX_MARKER_BYTES,
            )
            self._assert_root(root_fd, root_identity)
            return EnvironmentSelector.from_payload(payload), identity
        finally:
            os.close(root_fd)

    def _read_environment_manifest(
        self,
        selector: EnvironmentSelector,
    ) -> tuple[dict[str, Any], PathIdentity]:
        expected = environment_manifest_path_for_lock(self.lock_path, selector.generation_id)
        if selector.manifest_name != expected.name:
            raise ReleaseGenerationError("environment manifest name is not generation-bound")
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            payload, identity = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=selector.manifest_name,
                maximum_bytes=MAX_ENVIRONMENT_MANIFEST_BYTES,
            )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)
        if _payload_hash(payload) != selector.manifest_sha256:
            raise ReleaseGenerationError("environment generation manifest hash changed")
        if (
            str(payload.get("operation_id")) != selector.operation_id
            or str(payload.get("transaction_kind")) != selector.transaction_kind
            or str(payload.get("generation_id")) != selector.generation_id
        ):
            raise ReleaseGenerationError("environment generation manifest binding changed")
        return payload, identity

    def selected_environment(self) -> EnvironmentSelector:
        self._assert_lock()
        selector, _identity_value = self._read_selector()
        manifest, _manifest_identity = self._read_environment_manifest(selector)
        _verify_environment_manifest(Path(selector.environment_path), manifest)
        self._assert_lock()
        return selector

    def _read_commit_record(self) -> ReleaseGenerationCommit:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            payload, _identity_value = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=self.commit_path.name,
                maximum_bytes=MAX_MARKER_BYTES,
            )
            self._assert_root(root_fd, root_identity)
            return ReleaseGenerationCommit.from_payload(payload)
        finally:
            os.close(root_fd)

    def _transaction_record(
        self,
        *,
        operation_id: str,
        transaction_kind: str,
    ) -> DeploymentIntent:
        if transaction_kind == "initialization":
            try:
                active, _active_identity = self._read_intent_record(self.intent_path)
            except ReleaseGenerationRecordMissingError:
                pass
            else:
                raise ReleaseGenerationError(
                    "active deployment intent blocks initialization generation: "
                    f"stage={active.stage}"
                )
            record, _identity_value = self._read_intent_record(self.initialization_path)
        elif transaction_kind == "deployment":
            try:
                record, _identity_value = self._read_intent_record(self.intent_path)
            except ReleaseGenerationRecordMissingError:
                archive = self.intent_path.with_name(
                    f"{self.intent_path.stem}.{operation_id}.completed.json"
                )
                record, _identity_value = self._read_intent_record(archive)
        else:
            raise ReleaseGenerationError("release transaction kind is invalid")
        if record.operation_id != operation_id:
            raise ReleaseGenerationError("release transaction operation id changed")
        return record

    def _verify_deployment_handoff(
        self,
        transaction: DeploymentIntent,
        *,
        provisional_label: str | None,
    ) -> None:
        if not transaction.handoff_operation_id:
            return
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            payload, _identity_value = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=self.handoff_path.name,
                maximum_bytes=MAX_INTENT_BYTES,
            )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)
        labels = tuple(str(value) for value in payload.get("labels", ()))
        restarted = tuple(str(value) for value in payload.get("restarted_labels", ()))
        if (
            payload.get("schema_version") != 1
            or payload.get("operation_id") != transaction.handoff_operation_id
            or labels != transaction.handoff_labels
            or not set(restarted).issubset(labels)
        ):
            raise ReleaseGenerationError("deployment handoff authority is stale")
        if payload.get("stage") == "completed" and set(restarted) == set(labels):
            return
        if provisional_label is not None and (
            provisional_label in labels and payload.get("stage") == "restarting"
        ):
            return
        raise ReleaseGenerationError("deployment handoff is not completed")

    def verify(
        self,
        *,
        expected_commit: str,
        provisional_handoff_label: str | None = None,
    ) -> ReleaseGenerationMarker:
        self._assert_lock()
        published = self._read_marker()
        if published.schema_version != MARKER_SCHEMA_VERSION:
            raise ReleaseGenerationError("release generation marker schema is unsupported")
        if published.commit != expected_commit:
            raise ReleaseGenerationError("release generation marker commit is stale")
        transaction = self._transaction_record(
            operation_id=published.operation_id,
            transaction_kind=published.transaction_kind,
        )
        if transaction.stage != "completed":
            raise ReleaseGenerationError("release transaction is not completed")
        self._verify_deployment_handoff(
            transaction,
            provisional_label=provisional_handoff_label,
        )
        try:
            committed = self._read_commit_record()
        except ReleaseGenerationError as exc:
            raise ReleaseGenerationError("release generation commit record is missing") from exc
        selector, _selector_identity = self._read_selector()
        manifest, _manifest_identity = self._read_environment_manifest(selector)
        current = self._facts(
            expected_commit=expected_commit,
            selector=selector,
            manifest=manifest,
        )
        if self._comparable(published) != self._comparable(current):
            if published.uv_lock_sha256 != current.uv_lock_sha256:
                raise ReleaseGenerationError("uv.lock no longer matches release marker")
            if published.venv_identity != current.venv_identity:
                raise ReleaseGenerationError("release venv identity no longer matches marker")
            raise ReleaseGenerationError("release generation marker is stale")
        if (
            committed.operation_id != published.operation_id
            or committed.transaction_kind != published.transaction_kind
            or committed.commit != published.commit
            or committed.marker_sha256 != published.content_hash()
            or committed.transaction_sha256 != transaction.content_hash()
            or committed.environment_generation_id != published.environment_generation_id
            or committed.previous_generation_id != published.previous_generation_id
            or committed.environment_manifest_sha256 != published.environment_manifest_sha256
        ):
            raise ReleaseGenerationError("release generation commit record is stale")
        _assert_tracked_clean(self.repo, self.git_path)
        self._assert_lock()
        return published

    def _read_intent_record(self, path: Path) -> tuple[DeploymentIntent, PathIdentity]:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            payload, identity = _read_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=path.name,
                maximum_bytes=MAX_INTENT_BYTES,
            )
            self._assert_root(root_fd, root_identity)
            return DeploymentIntent.from_payload(payload), identity
        finally:
            os.close(root_fd)

    def _create_intent_record(self, path: Path, intent: DeploymentIntent) -> None:
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            self._assert_root(root_fd, root_identity)
            _write_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=path.name,
                payload=asdict(intent),
                require_absent=True,
            )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)

    def _update_intent_record(
        self,
        path: Path,
        *,
        operation_id: str,
        stage: str,
        restarted_services: tuple[str, ...] | None = None,
    ) -> DeploymentIntent:
        current, identity = self._read_intent_record(path)
        if current.operation_id != operation_id:
            raise ReleaseGenerationError("deployment intent operation id changed")
        updated = current.advance(stage=stage, restarted_services=restarted_services)
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        try:
            self._assert_root(root_fd, root_identity)
            _write_private_json(
                root_fd=root_fd,
                root_path=self.lock_path.parent,
                name=path.name,
                payload=asdict(updated),
                require_absent=False,
                expected_identity=identity,
            )
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)
        return updated

    def begin_deployment_intent(
        self,
        *,
        previous_sha: str,
        target_sha: str,
        target_ref: str,
        changed_files: tuple[str, ...],
        restart_services: tuple[str, ...],
        active_services: tuple[str, ...],
        active_timers: tuple[str, ...],
        marker_generation: str = "",
        previous_generation_id: str = "",
        handoff_operation_id: str = "",
        handoff_labels: tuple[str, ...] = (),
    ) -> DeploymentIntent:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot create intent")
        self._assert_lock()
        if not marker_generation:
            marker = self.verify(expected_commit=previous_sha)
            marker_generation = marker.content_hash()
            previous_generation_id = marker.environment_generation_id
        if len(previous_generation_id) != 64:
            raise ReleaseGenerationError("deployment previous generation is unavailable")
        try:
            current, completed_identity = self._read_intent_record(self.intent_path)
        except ReleaseGenerationRecordMissingError:
            pass
        else:
            if current.stage != "completed":
                raise ReleaseGenerationError("an incomplete deployment intent already exists")
            archive = self.intent_path.with_name(
                f"{self.intent_path.stem}.{current.operation_id}.completed.json"
            )
            root_fd, root_identity = _private_lock_root(self.lock_path.parent)
            try:
                self._assert_root(root_fd, root_identity)
                if PathIdentity.capture(self.intent_path.lstat()) != completed_identity:
                    raise ReleaseGenerationError("completed deployment intent changed")
                os.replace(
                    self.intent_path.name,
                    archive.name,
                    src_dir_fd=root_fd,
                    dst_dir_fd=root_fd,
                )
                os.fsync(root_fd)
                self._assert_root(root_fd, root_identity)
            finally:
                os.close(root_fd)
        intent = DeploymentIntent.create(
            previous_sha=previous_sha,
            target_sha=target_sha,
            target_ref=target_ref,
            changed_files=changed_files,
            restart_services=restart_services,
            active_services=active_services,
            active_timers=active_timers,
            marker_generation=marker_generation,
            previous_generation_id=previous_generation_id,
            handoff_operation_id=handoff_operation_id,
            handoff_labels=handoff_labels,
        )
        self._create_intent_record(self.intent_path, intent)
        return intent

    def read_deployment_intent(self) -> DeploymentIntent:
        self._assert_lock()
        intent, _identity_value = self._read_intent_record(self.intent_path)
        return intent

    def update_deployment_intent(
        self,
        *,
        operation_id: str,
        stage: str,
        restarted_services: tuple[str, ...] | None = None,
    ) -> DeploymentIntent:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot update intent")
        self._assert_lock()
        return self._update_intent_record(
            self.intent_path,
            operation_id=operation_id,
            stage=stage,
            restarted_services=restarted_services,
        )

    def begin_initialization(self, *, target_sha: str) -> DeploymentIntent:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot initialize")
        self._assert_lock()
        try:
            current, _identity_value = self._read_intent_record(self.initialization_path)
        except ReleaseGenerationRecordMissingError:
            pass
        else:
            if current.stage == "completed":
                raise ReleaseGenerationError("release generation initialization already completed")
            if current.target_sha != target_sha:
                raise ReleaseGenerationError("initialization target is already pinned")
            return current
        intent = DeploymentIntent.create(
            previous_sha=target_sha,
            target_sha=target_sha,
            target_ref=target_sha,
            changed_files=(),
            restart_services=(),
            active_services=(),
            active_timers=(),
            previous_generation_id="",
            stage="initializing",
        )
        self._create_intent_record(self.initialization_path, intent)
        return intent

    def read_initialization(self) -> DeploymentIntent:
        self._assert_lock()
        initialization, _identity_value = self._read_intent_record(self.initialization_path)
        return initialization

    def complete_initialization(self, *, operation_id: str) -> DeploymentIntent:
        return self._update_intent_record(
            self.initialization_path,
            operation_id=operation_id,
            stage="completed",
        )

    def _ensure_environment_root(self) -> tuple[int, PathIdentity]:
        if self.environment_root.exists() or self.environment_root.is_symlink():
            identity = _identity(
                self.environment_root,
                label="release environment root",
                directory=True,
            )
            if stat.S_IMODE(identity.mode) != 0o700:
                raise ReleaseGenerationError("release environment root must have mode 0700")
        else:
            root_fd, root_identity = _private_lock_root(self.lock_path.parent)
            try:
                self._assert_root(root_fd, root_identity)
                os.mkdir(self.environment_root.name, 0o700, dir_fd=root_fd)
                os.fsync(root_fd)
                self._assert_root(root_fd, root_identity)
            except OSError as exc:
                raise ReleaseGenerationError("release environment root cannot be created") from exc
            finally:
                os.close(root_fd)
        descriptor = os.open(
            self.environment_root,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        identity = _identity(
            self.environment_root,
            label="release environment root",
            directory=True,
        )
        if PathIdentity.capture(os.fstat(descriptor)) != identity:
            os.close(descriptor)
            raise ReleaseGenerationError("release environment root identity changed")
        return descriptor, identity

    def _publish_environment(
        self,
        *,
        expected_commit: str,
        operation_id: str,
        transaction_kind: str,
        previous_generation_id: str,
    ) -> tuple[EnvironmentSelector, dict[str, Any]]:
        source_venv = self.repo / ".venv"
        _identity(source_venv, label="source release venv", directory=True)
        if not self.python_path.is_relative_to(source_venv):
            raise ReleaseGenerationError("deployment Python is outside source release venv")
        system_python, system_python_identity, system_python_sha256 = _venv_system_interpreter(
            self.python_path
        )
        source_bytes = _private_tree_size(
            source_venv,
            system_python=system_python,
            system_python_identity=system_python_identity,
            system_python_sha256=system_python_sha256,
        )
        self.garbage_collect_environments(
            reason=f"pre-publish:{transaction_kind}",
            required_bytes=source_bytes,
        )
        generation_id = _environment_generation_id(
            operation_id=operation_id,
            commit=expected_commit,
        )
        final_path = self.environment_root / generation_id
        environment_fd, environment_identity = self._ensure_environment_root()
        staging_name = f".{generation_id}.{secrets.token_hex(8)}.building"
        staging_path = self.environment_root / staging_name
        manifest_path = environment_manifest_path_for_lock(self.lock_path, generation_id)
        try:
            manifest: dict[str, Any] | None = None
            if final_path.exists() or final_path.is_symlink():
                _identity(final_path, label="release environment generation", directory=True)
                try:
                    root_fd, root_identity = _private_lock_root(self.lock_path.parent)
                    try:
                        manifest, _manifest_identity = _read_private_json(
                            root_fd=root_fd,
                            root_path=self.lock_path.parent,
                            name=manifest_path.name,
                            maximum_bytes=MAX_ENVIRONMENT_MANIFEST_BYTES,
                        )
                        self._assert_root(root_fd, root_identity)
                    finally:
                        os.close(root_fd)
                except ReleaseGenerationRecordMissingError:
                    pass
            else:
                os.mkdir(staging_name, 0o700, dir_fd=environment_fd)
                try:
                    self._build_environment(staging_path, system_python=system_python)
                    _rebind_environment_console_scripts(staging_path, final_path, source_venv)
                    self._mutation_hook("environment_staged")
                    active_environment = _identity(
                        self.environment_root,
                        label="release environment root",
                        directory=True,
                    )
                    if _object_key(active_environment) != _object_key(
                        environment_identity
                    ) or _object_key(PathIdentity.capture(os.fstat(environment_fd))) != _object_key(
                        environment_identity
                    ):
                        raise ReleaseGenerationError("release environment root identity changed")
                    os.rename(
                        staging_name,
                        generation_id,
                        src_dir_fd=environment_fd,
                        dst_dir_fd=environment_fd,
                    )
                    os.fsync(environment_fd)
                    _freeze_environment(
                        final_path,
                        system_python=system_python,
                        system_python_identity=system_python_identity,
                        system_python_sha256=system_python_sha256,
                    )
                except BaseException:
                    if staging_path.exists() and not staging_path.is_symlink():
                        _remove_private_tree_at(environment_fd, staging_name)
                    if manifest is None and final_path.exists() and not final_path.is_symlink():
                        _remove_private_tree_at(environment_fd, generation_id)
                    raise
            if manifest is None:
                _freeze_environment(
                    final_path,
                    system_python=system_python,
                    system_python_identity=system_python_identity,
                    system_python_sha256=system_python_sha256,
                )
                active_environment = _identity(
                    self.environment_root,
                    label="release environment root",
                    directory=True,
                )
                if _object_key(active_environment) != _object_key(
                    environment_identity
                ) or _object_key(PathIdentity.capture(os.fstat(environment_fd))) != _object_key(
                    environment_identity
                ):
                    raise ReleaseGenerationError("release environment root identity changed")
                self._mutation_hook("environment_generation_ready")
                manifest = _environment_manifest(
                    final_path,
                    operation_id=operation_id,
                    transaction_kind=transaction_kind,
                    commit=expected_commit,
                    generation_id=generation_id,
                    system_python=system_python,
                    system_python_identity=system_python_identity,
                    system_python_sha256=system_python_sha256,
                    uv_binding=self.uv_binding,
                )
                manifest_hash = _payload_hash(manifest)
                root_fd, root_identity = _private_lock_root(self.lock_path.parent)
                try:
                    self._assert_root(root_fd, root_identity)
                    _write_private_json(
                        root_fd=root_fd,
                        root_path=self.lock_path.parent,
                        name=manifest_path.name,
                        payload=manifest,
                        require_absent=True,
                        maximum_bytes=MAX_ENVIRONMENT_MANIFEST_BYTES,
                    )
                    self._assert_root(root_fd, root_identity)
                finally:
                    os.close(root_fd)
                self._mutation_hook("environment_sealed")
            else:
                if (
                    str(manifest.get("operation_id")) != operation_id
                    or str(manifest.get("transaction_kind")) != transaction_kind
                    or str(manifest.get("commit")) != expected_commit
                    or str(manifest.get("generation_id")) != generation_id
                ):
                    raise ReleaseGenerationError("existing environment generation is stale")
                _verify_environment_manifest(final_path, manifest)
                manifest_hash = _payload_hash(manifest)
            selector = EnvironmentSelector(
                schema_version=ENVIRONMENT_SCHEMA_VERSION,
                operation_id=operation_id,
                transaction_kind=transaction_kind,
                commit=expected_commit,
                generation_id=generation_id,
                previous_generation_id=previous_generation_id,
                environment_path=str(final_path),
                manifest_name=manifest_path.name,
                manifest_sha256=manifest_hash,
                published_at=datetime.now(UTC).isoformat(),
            )
            try:
                _prior, selector_identity = self._read_selector()
            except ReleaseGenerationRecordMissingError:
                pass
                selector_identity = None
            root_fd, root_identity = _private_lock_root(self.lock_path.parent)
            try:
                self._assert_root(root_fd, root_identity)
                _write_private_json(
                    root_fd=root_fd,
                    root_path=self.lock_path.parent,
                    name=self.environment_selector_path.name,
                    payload=asdict(selector),
                    require_absent=selector_identity is None,
                    expected_identity=selector_identity,
                    maximum_bytes=MAX_MARKER_BYTES,
                )
                self._assert_root(root_fd, root_identity)
            finally:
                os.close(root_fd)
            self._mutation_hook("environment_selector_published")
            return selector, manifest
        finally:
            os.close(environment_fd)

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
            with suppress(FileNotFoundError):
                os.unlink(self.commit_path.name, dir_fd=root_fd)
            os.fsync(root_fd)
            self._assert_root(root_fd, root_identity)
        finally:
            os.close(root_fd)

    def publish(
        self,
        *,
        expected_commit: str,
        operation_id: str,
        transaction_kind: str,
    ) -> ReleaseGenerationMarker:
        if not self.writable:
            raise ReleaseGenerationError("read-only generation authority cannot publish")
        self._assert_lock()
        _assert_tracked_clean(self.repo, self.git_path)
        transaction = self._transaction_record(
            operation_id=operation_id,
            transaction_kind=transaction_kind,
        )
        expected_stage = (
            "initializing" if transaction_kind == "initialization" else "timers_restored"
        )
        if transaction.stage != expected_stage:
            raise ReleaseGenerationError("release transaction is not ready for marker publication")
        target_commit = (
            transaction.previous_sha
            if transaction_kind == "deployment" and expected_commit == transaction.previous_sha
            else transaction.target_sha
        )
        if target_commit != expected_commit:
            raise ReleaseGenerationError("release transaction target does not match marker")
        selector, manifest = self._publish_environment(
            expected_commit=expected_commit,
            operation_id=operation_id,
            transaction_kind=transaction_kind,
            previous_generation_id=transaction.previous_generation_id,
        )
        marker = self._facts(
            expected_commit=expected_commit,
            selector=selector,
            manifest=manifest,
        )
        payload = (
            json.dumps(asdict(marker), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        root_fd, root_identity = _private_lock_root(self.lock_path.parent)
        temporary_name = f".{self.marker_path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
        descriptor = -1
        renamed = False
        completed = False
        try:
            self._assert_root(root_fd, root_identity)
            descriptor = os.open(
                temporary_name,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_fd,
            )
            _write_all(descriptor, payload)
            os.fsync(descriptor)
            self._mutation_hook("marker_temp_fsynced")
            self._assert_root(root_fd, root_identity)
            _verify_temporary_payload(
                descriptor,
                expected_payload=payload,
                expected_marker=marker,
            )
            self._assert_root(root_fd, root_identity)
            os.replace(
                temporary_name,
                self.marker_path.name,
                src_dir_fd=root_fd,
                dst_dir_fd=root_fd,
            )
            renamed = True
            os.fsync(root_fd)
            self._assert_root(root_fd, root_identity)
            active = self.marker_path.lstat()
            if PathIdentity.capture(os.fstat(descriptor)) != PathIdentity.capture(active):
                raise ReleaseGenerationError("published generation marker identity changed")
            completed = True
            self._mutation_hook("marker_published")
            return marker
        finally:
            if not completed and renamed:
                try:
                    active = self.marker_path.lstat()
                    if descriptor >= 0 and PathIdentity.capture(
                        os.fstat(descriptor)
                    ) == PathIdentity.capture(active):
                        os.unlink(self.marker_path.name, dir_fd=root_fd)
                        os.fsync(root_fd)
                except FileNotFoundError:
                    pass
            elif not renamed:
                with suppress(FileNotFoundError):
                    os.unlink(temporary_name, dir_fd=root_fd)
            if descriptor >= 0:
                os.close(descriptor)
            os.close(root_fd)


if __name__ == "__main__":
    raise SystemExit("release_generation is a library, not a command")
