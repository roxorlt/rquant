#!/usr/bin/env python3
"""Acquire the release-generation lock before importing the project deployer."""

from __future__ import annotations

import argparse
import ast
import fcntl
import hashlib
import importlib.util
import json
import math
import os
import re
import secrets
import signal
import stat
import subprocess
import sys
import time
import tomllib
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from types import ModuleType
from zoneinfo import ZoneInfo

sys.dont_write_bytecode = True


class DeployBootstrapError(RuntimeError):
    pass


class DeployDeferredError(DeployBootstrapError):
    """A write-capable deployment must wait for the protected window to end."""

    exit_code = 75


TARGET_PATTERN = re.compile(r"(?:v\d+\.\d+\.\d+|[0-9a-f]{40})")
LAB_LAUNCHD_LABELS = (
    "com.roxor.rquant-lab-scheduler",
    "com.roxor.rquant-lab-worker",
    "com.roxor.rquant-lab-finalizer",
)
LAUNCHD_HANDOFF_TIMEOUT_SECONDS = 30.0
LAUNCHD_READINESS_STABILITY_SECONDS = 5.0
UV_CANDIDATES = (
    Path("/opt/homebrew/bin/uv"),
    Path("/usr/local/bin/uv"),
    Path.home() / ".local" / "bin" / "uv",
)
LAB_INSTALL_SCHEMA_VERSION = 2
LAB_HANDOFF_SCHEMA_VERSION = 1
LAB_RUNTIME_PREPARED_SCHEMA_VERSION = 2
LAB_RUNTIME_PREPARED_FILENAME = ".prepared.json"
LAB_RUNTIME_DIRECTORY_LABELS = frozenset(
    {
        "lab command spool",
        "lab claim spool",
        "lab report spool",
        "lab worker artifact root",
        "lab final artifact root",
        "lab artifact commit spool",
        "lab daemon lock root",
        "lab finalizer state root",
        "lab readiness root",
    }
)
LAB_RUNTIME_FILE_LABELS = frozenset({"lab jobs SQLite"})
DEPLOY_CONTROL_KEYS = frozenset(
    {
        "LAB_TRUSTED_GIT_PATH",
        "RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS",
        "RQUANT_DEPLOY_OVERALL_TIMEOUT_SECONDS",
        "RQUANT_DEPLOY_UV",
        "RQUANT_LAB_LIFECYCLE_MODE",
        "RQUANT_RELEASE_GENERATION_GC_GRACE_SECONDS",
        "RQUANT_RELEASE_GENERATION_MIN_FREE_BYTES",
        "RQUANT_RELEASE_PROFILE",
    }
)
DEPLOY_CONTROL_PREFIXES = (
    "RQUANT_DEPLOY_",
    "RQUANT_LAB_LIFECYCLE_",
    "RQUANT_RELEASE_",
    "LAB_TRUSTED_GIT_",
)


def _canonical(raw: str, *, label: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise DeployBootstrapError(f"{label} must be an absolute canonical path")
    return path


def _physical_directory(path: Path, *, label: str, private: bool = False) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise DeployBootstrapError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_mode & 0o022
        or (private and stat.S_IMODE(observed.st_mode) != 0o700)
        or path.resolve(strict=True) != path
    ):
        raise DeployBootstrapError(f"{label} has unsafe identity")
    return observed


def _identity(observed: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
        observed.st_nlink,
    )


def _read_bound_private_file(
    path: Path,
    *,
    label: str,
    maximum_bytes: int,
    private_parent: bool,
    missing_ok: bool = False,
) -> bytes | None:
    parent = path.parent
    before_parent = _physical_directory(
        parent,
        label=f"{label} parent",
        private=private_parent,
    )
    root_fd = -1
    descriptor = -1
    try:
        root_fd = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        opened_parent = os.fstat(root_fd)
        if _identity(opened_parent) != _identity(before_parent):
            raise DeployBootstrapError(f"{label} parent identity changed")
        try:
            descriptor = os.open(
                path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
        except FileNotFoundError:
            if missing_ok:
                return None
            raise
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_size > maximum_bytes
        ):
            raise DeployBootstrapError(f"{label} has unsafe identity")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                raise DeployBootstrapError(f"{label} is too large")
            chunks.append(chunk)
        active = os.stat(path.name, dir_fd=root_fd, follow_symlinks=False)
        rebound_parent = parent.lstat()
        if _identity(active) != _identity(opened) or _identity(rebound_parent) != _identity(
            opened_parent
        ):
            raise DeployBootstrapError(f"{label} identity changed")
        return b"".join(chunks)
    except DeployBootstrapError:
        raise
    except OSError as exc:
        raise DeployBootstrapError(f"{label} cannot be read") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if root_fd >= 0:
            os.close(root_fd)


def _read_deploy_controls(path: Path) -> dict[str, str]:
    encoded = _read_bound_private_file(
        path,
        label="deployment dotenv",
        maximum_bytes=1024 * 1024,
        private_parent=False,
        missing_ok=True,
    )
    if encoded is None:
        return {}
    try:
        payload = encoded.decode("utf-8")
    except UnicodeError as exc:
        raise DeployBootstrapError("deployment dotenv cannot be read") from exc
    lines = payload.splitlines()
    controls: dict[str, str] = {}
    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            if line.startswith(DEPLOY_CONTROL_PREFIXES):
                raise DeployBootstrapError(
                    f"deployment dotenv control requires '=' on line {line_number}"
                )
            continue
        key, raw_value = line.split("=", 1)
        if key != key.strip() or re.fullmatch(r"[A-Z][A-Z0-9_]*", key) is None:
            if key.strip().startswith(DEPLOY_CONTROL_PREFIXES):
                raise DeployBootstrapError(
                    f"deployment dotenv key is malformed on line {line_number}"
                )
            continue
        if key not in DEPLOY_CONTROL_KEYS:
            if key.startswith(DEPLOY_CONTROL_PREFIXES):
                raise DeployBootstrapError(f"unknown deployment dotenv key: {key}")
            continue
        if key in controls:
            raise DeployBootstrapError(f"duplicate deployment dotenv key: {key}")
        raw_value = raw_value.strip()
        if raw_value.startswith(("'", '"')):
            try:
                value = ast.literal_eval(raw_value)
            except (SyntaxError, ValueError) as exc:
                raise DeployBootstrapError(
                    f"deployment dotenv value is invalid on line {line_number}"
                ) from exc
            if not isinstance(value, str):
                raise DeployBootstrapError(
                    f"deployment dotenv value is invalid on line {line_number}"
                )
        else:
            if re.fullmatch(r"[A-Za-z0-9_./:+-]*", raw_value) is None:
                raise DeployBootstrapError(
                    f"deployment dotenv value is unsafe on line {line_number}"
                )
            value = raw_value
        if "\x00" in value or "\n" in value or "\r" in value:
            raise DeployBootstrapError(f"deployment dotenv value is unsafe on line {line_number}")
        controls[key] = value
    return controls


def _validate_profile_controls(
    controls: dict[str, str],
    *,
    release_profile: str,
    host_platform: str,
) -> None:
    expected_platform = {
        "linux-production": "linux",
        "macos-lab": "darwin",
    }.get(release_profile)
    if expected_platform != host_platform:
        raise DeployBootstrapError("release profile does not match host platform")
    configured_profile = controls.get("RQUANT_RELEASE_PROFILE", "")
    if configured_profile and configured_profile != release_profile:
        raise DeployBootstrapError("release profile does not match repo dotenv")
    lifecycle = controls.get("RQUANT_LAB_LIFECYCLE_MODE", "")
    if lifecycle and lifecycle not in {"uninstalled", "installed"}:
        raise DeployBootstrapError("Lab lifecycle mode is invalid")
    if host_platform == "linux" and lifecycle not in {"", "uninstalled"}:
        raise DeployBootstrapError("Linux deployment cannot enable Lab lifecycle")


def _deploy_timeout(raw: str, *, default: float, label: str) -> float:
    try:
        value = float(raw) if raw else default
    except ValueError as exc:
        raise DeployBootstrapError(f"{label} is invalid") from exc
    if not math.isfinite(value):
        raise DeployBootstrapError(f"{label} is invalid")
    return value


def _physical_file(path: Path, *, label: str, executable: bool = False) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise DeployBootstrapError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_nlink != 1
        or observed.st_mode & 0o022
        or (executable and not observed.st_mode & stat.S_IXUSR)
        or path.resolve(strict=True) != path
    ):
        raise DeployBootstrapError(f"{label} has unsafe identity")
    return observed


def _verified_venv_python(root: Path, path: Path) -> os.stat_result:
    expected_bin = root / ".venv" / "bin"
    if path.parent != expected_bin or not path.name.startswith("python"):
        raise DeployBootstrapError("deployment Python is outside the release venv bin")
    try:
        resolved = path.resolve(strict=True)
        observed = resolved.lstat()
    except OSError as exc:
        raise DeployBootstrapError("deployment Python is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_nlink != 1
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
    ):
        raise DeployBootstrapError("deployment Python has unsafe identity")
    return observed


def _trusted_git(path: Path) -> None:
    if path.resolve(strict=True) != path:
        raise DeployBootstrapError("trusted Git must be physical")
    observed = path.lstat()
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != 0
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
    ):
        raise DeployBootstrapError("trusted Git has unsafe identity")


def _resolve_uv_path(configured: str) -> tuple[Path, dict[str, object]]:
    candidates = (_canonical(configured, label="deployment uv"),) if configured else UV_CANDIDATES
    candidate = next(
        (path for path in candidates if path.exists() or path.is_symlink()),
        None,
    )
    if candidate is None:
        raise DeployBootstrapError(
            "an absolute uv path is required; checked /opt/homebrew/bin/uv and /usr/local/bin/uv"
        )
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise DeployBootstrapError("deployment uv must be an absolute canonical path")
    current = candidate
    seen: set[tuple[int, int]] = set()
    for _index in range(16):
        try:
            observed = current.lstat()
        except OSError as exc:
            raise DeployBootstrapError("deployment uv symlink chain is unavailable") from exc
        identity = (observed.st_dev, observed.st_ino)
        if identity in seen:
            raise DeployBootstrapError("deployment uv symlink chain contains a cycle")
        seen.add(identity)
        if not stat.S_ISLNK(observed.st_mode):
            break
        if observed.st_uid not in {0, os.getuid()}:
            raise DeployBootstrapError("deployment uv symlink has unsafe ownership")
        target = Path(os.readlink(current))
        current = target if target.is_absolute() else current.parent / target
        current = Path(os.path.normpath(current))
    else:
        raise DeployBootstrapError("deployment uv symlink chain is too deep")
    physical = current.resolve(strict=True)
    if physical != current:
        raise DeployBootstrapError("deployment uv physical target changed during resolution")
    observed = physical.lstat()
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid not in {0, os.getuid()}
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
    ):
        raise DeployBootstrapError("deployment uv has unsafe identity")
    payload = physical.read_bytes()
    active = physical.lstat()
    if (active.st_dev, active.st_ino, active.st_mode, active.st_uid) != (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
    ):
        raise DeployBootstrapError("deployment uv identity changed while reading")
    return physical, {
        "configured_path": str(candidate),
        "physical_path": str(physical),
        "device": observed.st_dev,
        "inode": observed.st_ino,
        "mode": observed.st_mode,
        "owner": observed.st_uid,
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _acquire_lock(
    root: Path,
    lock_path: Path,
    *,
    shared: bool = False,
    timeout_seconds: float = 0,
    create: bool = True,
) -> int:
    expected = root.parent / ".rquant-deploy" / f"{root.name}.lock"
    if lock_path != expected:
        raise DeployBootstrapError("deployment lock does not match checkout binding")
    try:
        if create:
            lock_path.parent.mkdir(mode=0o700, exist_ok=True)
        _physical_directory(lock_path.parent, label="deployment authority root", private=True)
        flags = (os.O_RDONLY if not create and shared else os.O_RDWR) | getattr(os, "O_NOFOLLOW", 0)
        if create:
            flags |= os.O_CREAT
        descriptor = os.open(
            lock_path,
            flags,
            0o600,
        )
        opened = os.fstat(descriptor)
        active = lock_path.lstat()
        if (
            (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid, opened.st_nlink)
            != (active.st_dev, active.st_ino, active.st_mode, active.st_uid, active.st_nlink)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise DeployBootstrapError("deployment generation lock is unsafe")
        operation = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(descriptor, operation | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        os.set_inheritable(descriptor, True)
        return descriptor
    except BlockingIOError as exc:
        raise DeployBootstrapError("another release generation is active") from exc
    except OSError as exc:
        raise DeployBootstrapError("deployment generation lock is unavailable") from exc


def _acquire_handoff_lock(root: Path, lock_path: Path) -> tuple[int, int]:
    handoff_path = lock_path.with_name(f"{lock_path.stem}.handoff.lock")
    descriptor = -1
    root_fd = -1
    try:
        expected = root.parent / ".rquant-deploy" / f"{root.name}.lock"
        if lock_path != expected:
            raise DeployBootstrapError("deployment lock does not match checkout binding")
        lock_path.parent.mkdir(mode=0o700, exist_ok=True)
        _physical_directory(
            lock_path.parent,
            label="deployment authority root",
            private=True,
        )
        before = lock_path.parent.lstat()
        root_fd = os.open(
            lock_path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        opened_root = os.fstat(root_fd)
        if (before.st_dev, before.st_ino, before.st_mode, before.st_uid) != (
            opened_root.st_dev,
            opened_root.st_ino,
            opened_root.st_mode,
            opened_root.st_uid,
        ):
            raise DeployBootstrapError("deployment handoff root identity changed")
        descriptor = os.open(
            handoff_path.name,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=root_fd,
        )
        opened = os.fstat(descriptor)
        active = os.stat(handoff_path.name, dir_fd=root_fd, follow_symlinks=False)
        rebound_root = lock_path.parent.lstat()
        if (
            (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid, opened.st_nlink)
            != (active.st_dev, active.st_ino, active.st_mode, active.st_uid, active.st_nlink)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (rebound_root.st_dev, rebound_root.st_ino, rebound_root.st_mode, rebound_root.st_uid)
            != (before.st_dev, before.st_ino, before.st_mode, before.st_uid)
        ):
            raise DeployBootstrapError("deployment handoff lock is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return root_fd, descriptor
    except BlockingIOError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        if root_fd >= 0:
            os.close(root_fd)
        raise DeployBootstrapError("another deployment handoff/generation is active") from exc
    except DeployBootstrapError:
        if descriptor >= 0:
            os.close(descriptor)
        if root_fd >= 0:
            os.close(root_fd)
        raise
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        if root_fd >= 0:
            os.close(root_fd)
        raise DeployBootstrapError("deployment handoff lock is unavailable") from exc


def _is_protected_handoff_window(now: datetime | None = None) -> bool:
    local = now or datetime.now(ZoneInfo("Asia/Shanghai"))
    if local.tzinfo is None:
        local = local.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    else:
        local = local.astimezone(ZoneInfo("Asia/Shanghai"))
    if local.weekday() >= 5:
        return False
    current = local.hour * 60 + local.minute
    return 9 * 60 + 15 <= current <= 15 * 60 + 10


def _launchctl(
    arguments: list[str],
    *,
    check: bool,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["/bin/launchctl", *arguments],
            check=check,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DeployBootstrapError("Lab launchd handoff command failed") from exc


def _run_process_group(
    arguments: list[str],
    *,
    cwd: Path,
    timeout_seconds: float,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        arguments,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        env=env,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise
    return subprocess.CompletedProcess(arguments, process.returncode, stdout, stderr)


def _generation_lock_is_held(root: Path, lock_path: Path) -> bool:
    try:
        descriptor = _acquire_lock(root, lock_path)
    except DeployBootstrapError as exc:
        if "another release generation is active" in str(exc):
            return True
        raise
    os.close(descriptor)
    return False


def _private_json(
    path: Path,
    *,
    label: str,
    missing_ok: bool = False,
) -> dict[str, object] | None:
    try:
        payload = _read_bound_private_file(
            path,
            label=label,
            maximum_bytes=1024 * 1024,
            private_parent=True,
            missing_ok=missing_ok,
        )
        if payload is None:
            return None
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise DeployBootstrapError(f"{label} is invalid") from exc
    if not isinstance(parsed, dict):
        raise DeployBootstrapError(f"{label} is invalid")
    return parsed


def _stable_record_path(lock_path: Path, suffix: str) -> Path:
    return lock_path.with_name(f"{lock_path.stem}.{suffix}.json")


def _atomic_private_json(path: Path, payload: dict[str, object], *, absent: bool = False) -> None:
    parent = path.parent
    _physical_directory(parent, label="deployment authority root", private=True)
    root_fd = os.open(
        parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    temporary = f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    descriptor = -1
    try:
        if absent and os.path.lexists(path):
            raise DeployBootstrapError(f"{path.name} already exists")
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=root_fd,
        )
        encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        offset = 0
        while offset < len(encoded):
            written = os.write(descriptor, encoded[offset:])
            if written <= 0:
                raise DeployBootstrapError("private deployment record write was incomplete")
            offset += written
        os.fsync(descriptor)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise DeployBootstrapError("private deployment record is unsafe")
        if absent:
            try:
                os.link(
                    temporary,
                    path.name,
                    src_dir_fd=root_fd,
                    dst_dir_fd=root_fd,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise DeployBootstrapError(f"{path.name} appeared concurrently") from exc
            os.unlink(temporary, dir_fd=root_fd)
        else:
            os.replace(temporary, path.name, src_dir_fd=root_fd, dst_dir_fd=root_fd)
        os.fsync(root_fd)
        _private_json(path, label=path.name)
    except BaseException:
        with suppress(OSError):
            os.unlink(temporary, dir_fd=root_fd)
        raise
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(root_fd)


def _verify_lab_runtime_prepared(
    *,
    root: Path,
    runtime_root: Path,
    readiness_root: Path,
    expected_commit: str,
    allow_uninitialized_database: bool = False,
) -> dict[str, object]:
    if re.fullmatch(r"[0-9a-f]{40}", expected_commit) is None:
        raise DeployBootstrapError("Lab runtime prepared sentinel commit is invalid")
    runtime_identity = _physical_directory(
        runtime_root,
        label="Lab runtime root",
        private=True,
    )
    sentinel = runtime_root / LAB_RUNTIME_PREPARED_FILENAME
    sentinel_identity = _physical_file(sentinel, label="Lab runtime prepared sentinel")
    if stat.S_IMODE(sentinel_identity.st_mode) != 0o600:
        raise DeployBootstrapError("Lab runtime prepared sentinel must have mode 0600")
    payload = _private_json(sentinel, label="Lab runtime prepared sentinel")
    if (
        payload.get("schema_version") != LAB_RUNTIME_PREPARED_SCHEMA_VERSION
        or payload.get("checkout_root") != str(root)
        or payload.get("runtime_root") != str(runtime_root)
        or payload.get("runtime_device") != runtime_identity.st_dev
        or payload.get("runtime_inode") != runtime_identity.st_ino
    ):
        raise DeployBootstrapError("Lab runtime prepared sentinel binding changed")
    authority_id = payload.get("runtime_authority_id")
    if not isinstance(authority_id, str) or re.fullmatch(r"[0-9a-f]{32}", authority_id) is None:
        raise DeployBootstrapError("Lab runtime prepared authority is invalid")
    directories = payload.get("managed_directories")
    files = payload.get("managed_files")
    migrations = payload.get("migration_sources")
    if (
        not isinstance(directories, dict)
        or set(directories) != LAB_RUNTIME_DIRECTORY_LABELS
        or not isinstance(files, dict)
        or set(files) != LAB_RUNTIME_FILE_LABELS
        or not isinstance(migrations, dict)
    ):
        raise DeployBootstrapError("Lab runtime prepared sentinel layout is incomplete")
    for label, binding in directories.items():
        if not isinstance(binding, dict):
            raise DeployBootstrapError("Lab runtime prepared sentinel layout is invalid")
        path = _canonical(str(binding.get("path", "")), label=label)
        if path.parent != runtime_root:
            raise DeployBootstrapError("Lab runtime prepared sentinel path escaped runtime root")
        observed = _physical_directory(path, label=label, private=True)
        if binding != {
            "path": str(path),
            "device": observed.st_dev,
            "inode": observed.st_ino,
            "mode": 0o700,
        }:
            raise DeployBootstrapError("Lab runtime prepared sentinel directory changed")
    readiness = directories.get("lab readiness root")
    if not isinstance(readiness, dict) or readiness.get("path") != str(readiness_root):
        raise DeployBootstrapError("Lab runtime prepared sentinel readiness binding changed")
    for label, binding in files.items():
        if not isinstance(binding, dict):
            raise DeployBootstrapError("Lab runtime prepared sentinel file binding is invalid")
        path = _canonical(str(binding.get("path", "")), label=label)
        if path.parent != runtime_root:
            raise DeployBootstrapError("Lab runtime prepared sentinel path escaped runtime root")
        for suffix in ("-wal", "-shm", "-journal"):
            if os.path.lexists(path.with_name(f"{path.name}{suffix}")):
                raise DeployBootstrapError(
                    "checkpoint and remove Lab SQLite sidecars before registration"
                )
        exists = bool(binding.get("exists"))
        if exists:
            observed = _physical_file(path, label=label)
            if stat.S_IMODE(observed.st_mode) != 0o600 or binding != {
                "path": str(path),
                "device": observed.st_dev,
                "inode": observed.st_ino,
                "mode": 0o600,
                "exists": True,
            }:
                raise DeployBootstrapError("Lab runtime prepared sentinel file changed")
        elif binding != {"path": str(path), "exists": False}:
            raise DeployBootstrapError("Lab runtime prepared sentinel file changed")
        elif os.path.lexists(path):
            raise DeployBootstrapError(
                "Lab runtime database exists but is not registered in the prepared sentinel"
            )
        elif not allow_uninitialized_database:
            raise DeployBootstrapError(
                "Lab runtime database is not initialized in the prepared sentinel"
            )
    for target, binding in migrations.items():
        if not isinstance(binding, dict) or set(binding) != {"source", "migrated"}:
            raise DeployBootstrapError("Lab runtime prepared sentinel migration is invalid")
        target_path = _canonical(str(target), label="Lab runtime migration target")
        source = _canonical(str(binding.get("source", "")), label="Lab legacy runtime source")
        if target_path.parent != runtime_root or os.path.lexists(source):
            raise DeployBootstrapError("Lab legacy runtime source still exists")
    return {
        "runtime_authority_id": authority_id,
        "runtime_root": str(runtime_root),
        "runtime_device": runtime_identity.st_dev,
        "runtime_inode": runtime_identity.st_ino,
    }


def _write_lab_installation_state(
    *,
    root: Path,
    lock_path: Path,
    runtime_root: Path,
    readiness_root: Path,
    expected_commit: str,
    publish: bool = True,
) -> dict[str, object]:
    runtime = runtime_root.resolve(strict=True)
    if runtime != runtime_root or runtime in {root, root / "data"}:
        raise DeployBootstrapError("Lab runtime root is not an isolated private namespace")
    _physical_directory(runtime, label="Lab runtime root", private=True)
    if readiness_root.parent != runtime:
        raise DeployBootstrapError("Lab readiness root must be inside the Lab runtime root")
    _physical_directory(readiness_root, label="Lab readiness root", private=True)
    prepared = _verify_lab_runtime_prepared(
        root=root,
        runtime_root=runtime,
        readiness_root=readiness_root,
        expected_commit=expected_commit,
        allow_uninitialized_database=True,
    )
    plists: dict[str, dict[str, object]] = {}
    for label in LAB_LAUNCHD_LABELS:
        path = root / "deploy" / "launchd" / f"{label}.plist"
        identity = _physical_file(path, label=f"Lab launchd plist {label}")
        plists[label] = {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "device": identity.st_dev,
            "inode": identity.st_ino,
        }
    payload: dict[str, object] = {
        "schema_version": LAB_INSTALL_SCHEMA_VERSION,
        "checkout_root": str(root),
        "labels": list(LAB_LAUNCHD_LABELS),
        "plists": plists,
        "runtime_root": str(runtime),
        "readiness_root": str(readiness_root),
        "registered_by_commit": expected_commit,
        "prepared_authority": prepared,
        "installed_at": datetime.now(ZoneInfo("UTC")).isoformat(),
    }
    if publish:
        _atomic_private_json(_stable_record_path(lock_path, "lab-install"), payload)
    return payload


def _read_lab_installation_state(*, root: Path, lock_path: Path) -> dict[str, object]:
    path = _stable_record_path(lock_path, "lab-install")
    payload = _private_json(
        path,
        label="Lab launchd installation state",
        missing_ok=True,
    )
    if payload is None:
        raise DeployBootstrapError("Lab launchd installation state is missing")
    if (
        payload.get("schema_version") != LAB_INSTALL_SCHEMA_VERSION
        or payload.get("checkout_root") != str(root)
        or payload.get("labels") != list(LAB_LAUNCHD_LABELS)
    ):
        raise DeployBootstrapError("Lab launchd installation state is invalid")
    plists = payload.get("plists")
    if not isinstance(plists, dict):
        raise DeployBootstrapError("Lab launchd installation state is invalid")
    for label in LAB_LAUNCHD_LABELS:
        expected = plists.get(label)
        path = root / "deploy" / "launchd" / f"{label}.plist"
        observed = _physical_file(path, label=f"Lab launchd plist {label}")
        if not isinstance(expected, dict) or expected != {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "device": observed.st_dev,
            "inode": observed.st_ino,
        }:
            raise DeployBootstrapError("Lab launchd installation binding changed")
    runtime_root = Path(str(payload.get("runtime_root", "")))
    readiness_root = Path(str(payload.get("readiness_root", "")))
    _physical_directory(runtime_root, label="Lab runtime root", private=True)
    _physical_directory(readiness_root, label="Lab readiness root", private=True)
    if readiness_root.parent != runtime_root:
        raise DeployBootstrapError("Lab readiness installation binding changed")
    prepared = _verify_lab_runtime_prepared(
        root=root,
        runtime_root=runtime_root,
        readiness_root=readiness_root,
        expected_commit=str(payload.get("registered_by_commit", "")),
    )
    if payload.get("prepared_authority") != prepared:
        raise DeployBootstrapError("Lab runtime prepared sentinel binding changed")
    return payload


def _release_readiness_expectation(lock_path: Path) -> tuple[str, str, str]:
    marker = _private_json(
        lock_path.with_name(f"{lock_path.stem}.complete.json"),
        label="release generation marker",
    )
    committed = _private_json(
        lock_path.with_name(f"{lock_path.stem}.commit.json"),
        label="release generation commit",
    )
    operation_id = str(marker.get("operation_id", ""))
    generation_id = str(marker.get("environment_generation_id", ""))
    code_sha = str(marker.get("commit", ""))
    if (
        len(operation_id) != 32
        or len(generation_id) != 64
        or TARGET_PATTERN.fullmatch(code_sha) is None
        or code_sha.startswith("v")
        or committed.get("operation_id") != operation_id
        or committed.get("environment_generation_id") != generation_id
        or committed.get("commit") != code_sha
    ):
        raise DeployBootstrapError("release readiness generation is inconsistent")
    transaction_kind = str(marker.get("transaction_kind", ""))
    if transaction_kind not in {"deployment", "initialization"}:
        raise DeployBootstrapError("release readiness transaction kind is invalid")
    record_name = (
        f"{lock_path.stem}.intent.json"
        if transaction_kind == "deployment"
        else f"{lock_path.stem}.initialized.json"
    )
    transaction = _private_json(
        lock_path.with_name(record_name),
        label="release generation transaction",
    )
    if transaction.get("operation_id") != operation_id or transaction.get("stage") != "completed":
        raise DeployBootstrapError("release readiness transaction is incomplete")
    return operation_id, generation_id, code_sha


def _lab_readiness_payload(
    lock_path: Path,
    label: str,
    *,
    readiness_root: Path | None = None,
) -> dict[str, object]:
    root = readiness_root or lock_path.with_name(f"{lock_path.stem}.lab-readiness")
    _physical_directory(root, label="Lab readiness root", private=True)
    return _private_json(root / f"{label}.json", label=f"Lab readiness {label}")


def _launchctl_pid(output: str, *, label: str) -> int:
    match = re.search(r"(?m)^\s*pid\s*=\s*(\d+)\s*$", output)
    if match is None:
        raise DeployBootstrapError(f"Lab daemon has no launchd PID: {label}")
    return int(match.group(1))


def _validate_readiness_payload(
    payload: dict[str, object],
    *,
    label: str,
    pid: int,
    expected: tuple[str, str, str],
    lock_identity: os.stat_result,
) -> tuple[float, str]:
    operation_id, generation_id, code_sha = expected
    try:
        heartbeat = float(payload["heartbeat_monotonic"])
        started_at = str(payload["started_at"])
        heartbeat_at = datetime.fromisoformat(str(payload["heartbeat_at"]))
        started = datetime.fromisoformat(started_at)
    except (KeyError, TypeError, ValueError) as exc:
        raise DeployBootstrapError(f"Lab daemon heartbeat is invalid: {label}") from exc
    if (
        payload.get("label") != label
        or payload.get("pid") != pid
        or payload.get("operation_id") != operation_id
        or payload.get("environment_generation_id") != generation_id
        or payload.get("code_sha") != code_sha
        or payload.get("generation_lock_device") != lock_identity.st_dev
        or payload.get("generation_lock_inode") != lock_identity.st_ino
        or not math.isfinite(heartbeat)
        or heartbeat < 0
        or started.tzinfo is None
        or started.utcoffset() is None
        or heartbeat_at.tzinfo is None
        or heartbeat_at.utcoffset() is None
    ):
        raise DeployBootstrapError(f"Lab daemon readiness generation mismatch: {label}")
    try:
        os.kill(pid, 0)
    except OSError as exc:
        raise DeployBootstrapError(f"Lab daemon PID is not alive: {label}") from exc
    return heartbeat, started_at


def _wait_for_lab_readiness(
    *,
    root: Path,
    domain: str,
    labels: list[str],
    lock_path: Path,
    timeout_seconds: float,
    stability_seconds: float = LAUNCHD_READINESS_STABILITY_SECONDS,
) -> tuple[str, str, str]:
    deadline = time.monotonic() + timeout_seconds
    expected = _release_readiness_expectation(lock_path)
    installation = _read_lab_installation_state(root=root, lock_path=lock_path)
    readiness_root = Path(str(installation["readiness_root"]))
    lock_identity = _physical_file(lock_path, label="deployment generation lock")
    if stat.S_IMODE(lock_identity.st_mode) != 0o600:
        raise DeployBootstrapError("deployment generation lock must have mode 0600")
    first: dict[str, tuple[int, float, str, float]] = {}
    while time.monotonic() < deadline:
        healthy = True
        for label in labels:
            state = _launchctl(
                ["print", f"{domain}/{label}"],
                check=False,
                timeout_seconds=timeout_seconds,
            )
            if state.returncode != 0 or "state = running" not in state.stdout:
                healthy = False
                break
            try:
                pid = _launchctl_pid(state.stdout, label=label)
                heartbeat, started_at = _validate_readiness_payload(
                    _lab_readiness_payload(
                        lock_path,
                        label,
                        readiness_root=readiness_root,
                    ),
                    label=label,
                    pid=pid,
                    expected=expected,
                    lock_identity=lock_identity,
                )
            except DeployBootstrapError:
                healthy = False
                break
            prior = first.get(label)
            now = time.monotonic()
            if prior is None or prior[0] != pid:
                first[label] = (pid, heartbeat, started_at, now)
                healthy = False
                continue
            if started_at != prior[2] or heartbeat < prior[1]:
                raise DeployBootstrapError(f"Lab daemon heartbeat regressed: {label}")
            if heartbeat == prior[1]:
                healthy = False
                continue
            first[label] = (pid, heartbeat, started_at, prior[3])
            if now - prior[3] < stability_seconds:
                healthy = False
        if (
            healthy
            and len(first) == len(labels)
            and _generation_lock_is_held(
                root,
                lock_path,
            )
        ):
            return expected
        time.sleep(min(0.1, max(0.01, stability_seconds / 4)))
    raise DeployBootstrapError("Lab daemons did not reach generation-bound stable readiness")


def _completed_handoff_path(lock_path: Path, operation_id: str) -> Path:
    if re.fullmatch(r"[0-9a-f]{32}", operation_id) is None:
        raise DeployBootstrapError("Lab launchd handoff operation is invalid")
    return lock_path.with_name(f"{lock_path.stem}.lab-handoff.{operation_id}.completed.json")


def _operation_handoff_path(lock_path: Path, operation_id: str) -> Path:
    if re.fullmatch(r"[0-9a-f]{32}", operation_id) is None:
        raise DeployBootstrapError("Lab launchd handoff operation is invalid")
    return lock_path.with_name(f"{lock_path.stem}.lab-handoff.{operation_id}.json")


def _incomplete_handoff_exists(*, root: Path, lock_path: Path) -> bool:
    path = _stable_record_path(lock_path, "lab-handoff")
    payload = _private_json(
        path,
        label="Lab launchd handoff state",
        missing_ok=True,
    )
    if payload is None:
        return False
    operation_id = str(payload.get("operation_id", ""))
    stage = str(payload.get("stage", ""))
    if (
        payload.get("schema_version") != LAB_HANDOFF_SCHEMA_VERSION
        or payload.get("checkout_root") != str(root)
        or re.fullmatch(r"[0-9a-f]{32}", operation_id) is None
        or not stage
    ):
        raise DeployBootstrapError("Lab launchd handoff state is invalid")
    completed_path = _completed_handoff_path(lock_path, operation_id)
    if stage != "completed":
        if completed_path.exists():
            raise DeployBootstrapError("incomplete Lab handoff conflicts with completed proof")
        return True
    if not completed_path.exists():
        raise DeployBootstrapError("completed Lab launchd handoff proof is missing")
    completed = _private_json(completed_path, label="completed Lab launchd handoff proof")
    if completed.get("operation_id") != operation_id or completed.get("stage") != "completed":
        raise DeployBootstrapError("completed Lab launchd handoff proof is invalid")
    return False


def _lab_installation_identity(lock_path: Path, payload: dict[str, object]) -> dict[str, object]:
    path = _stable_record_path(lock_path, "lab-install")
    observed = _physical_file(path, label="Lab launchd installation state")
    if stat.S_IMODE(observed.st_mode) != 0o600:
        raise DeployBootstrapError("Lab launchd installation state must have mode 0600")
    return {
        "path": str(path),
        "sha256": hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "device": observed.st_dev,
        "inode": observed.st_ino,
    }


class _LabLaunchdHandoff:
    def __init__(
        self,
        *,
        root: Path,
        lock_path: Path,
        timeout_seconds: float,
        overall_timeout_seconds: float = 1800,
        overall_deadline_monotonic: float | None = None,
        release_profile: str = "macos-lab",
        lifecycle_mode: str = "installed",
        supersedes_operation_id: str = "",
    ) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0 or timeout_seconds > 300:
            raise DeployBootstrapError("Lab launchd handoff timeout is invalid")
        if (
            not math.isfinite(overall_timeout_seconds)
            or overall_timeout_seconds < timeout_seconds
            or overall_timeout_seconds > 7200
        ):
            raise DeployBootstrapError("Lab launchd overall timeout is invalid")
        self.root = root
        self.lock_path = lock_path
        self.timeout_seconds = timeout_seconds
        self.overall_timeout_seconds = overall_timeout_seconds
        computed_deadline = time.monotonic() + overall_timeout_seconds
        self.deadline = (
            computed_deadline
            if overall_deadline_monotonic is None
            else min(computed_deadline, overall_deadline_monotonic)
        )
        if not math.isfinite(self.deadline):
            raise DeployBootstrapError("Lab launchd handoff deadline is invalid")
        self.domain = f"gui/{os.getuid()}"
        self.plists = {
            label: root / "deploy" / "launchd" / f"{label}.plist" for label in LAB_LAUNCHD_LABELS
        }
        if release_profile not in {"linux-production", "macos-lab"}:
            raise DeployBootstrapError("release profile is unsupported")
        if (release_profile == "macos-lab") != (sys.platform == "darwin"):
            raise DeployBootstrapError("release profile does not match host platform")
        if lifecycle_mode not in {"uninstalled", "installed"}:
            raise DeployBootstrapError("Lab lifecycle mode is invalid")
        if release_profile != "macos-lab" and lifecycle_mode != "uninstalled":
            raise DeployBootstrapError("Lab lifecycle is only available on the macOS profile")
        self.lifecycle_mode = lifecycle_mode
        self.release_profile = release_profile
        if (
            supersedes_operation_id
            and re.fullmatch(r"[0-9a-f]{32}", supersedes_operation_id) is None
        ):
            raise DeployBootstrapError("superseded Lab handoff operation is invalid")
        self.supersedes_operation_id = supersedes_operation_id
        self.enabled = release_profile == "macos-lab" and lifecycle_mode == "installed"
        self.loaded: list[str] = []
        self.stopped: list[str] = []
        self.restarted: list[str] = []
        self.operation_id = ""
        self.installation: dict[str, object] | None = None
        self.installation_identity: dict[str, object] | None = None
        self.target_ref = ""
        self.target_sha = ""
        self.action = ""
        self.superseding_partial = False
        self.record_path = _stable_record_path(lock_path, "lab-handoff")
        self.lock_fd = -1
        self.root_fd = -1

    def _remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise DeployBootstrapError("Lab launchd handoff overall timeout expired")
        return min(self.timeout_seconds, remaining)

    def _record(
        self,
        stage: str,
        *,
        generation: tuple[str, str, str] | None = None,
    ) -> None:
        if not self.enabled:
            return
        payload: dict[str, object] = {
            "schema_version": LAB_HANDOFF_SCHEMA_VERSION,
            "operation_id": self.operation_id,
            "checkout_root": str(self.root),
            "stage": stage,
            "labels": list(self.loaded),
            "loaded_labels": list(self.loaded),
            "stopped_labels": list(self.stopped),
            "restarted_labels": list(self.restarted),
            "updated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
            "target_ref": self.target_ref,
            "target_sha": self.target_sha,
            "action": self.action,
            "release_profile": self.release_profile,
            "lifecycle_mode": self.lifecycle_mode,
            "installation_identity": self.installation_identity,
            "supersedes_operation_id": self.supersedes_operation_id,
        }
        if stage == "completed":
            if generation is None:
                raise DeployBootstrapError("completed Lab handoff lacks generation binding")
            operation_id, generation_id, code_sha = generation
            payload.update(
                {
                    "generation_operation_id": operation_id,
                    "environment_generation_id": generation_id,
                    "code_sha": code_sha,
                }
            )
        if stage == "completed":
            completed_path = _completed_handoff_path(self.lock_path, self.operation_id)
            if completed_path.exists():
                if (
                    _private_json(
                        completed_path,
                        label="completed Lab launchd handoff proof",
                    )
                    != payload
                ):
                    raise DeployBootstrapError("completed Lab launchd handoff proof changed")
            else:
                _atomic_private_json(completed_path, payload)
        _atomic_private_json(
            _operation_handoff_path(self.lock_path, self.operation_id),
            payload,
        )
        _atomic_private_json(self.record_path, payload)

    def _load_incomplete_record(self) -> bool:
        if not self.record_path.exists():
            return False
        payload = _private_json(self.record_path, label="Lab launchd handoff state")
        structurally_invalid = (
            payload.get("schema_version") != LAB_HANDOFF_SCHEMA_VERSION
            or payload.get("checkout_root") != str(self.root)
            or not isinstance(payload.get("labels"), list)
            or not isinstance(payload.get("loaded_labels"), list)
            or not isinstance(payload.get("stopped_labels"), list)
            or not isinstance(payload.get("restarted_labels"), list)
        )
        if structurally_invalid:
            raise DeployBootstrapError("Lab launchd handoff state is invalid")
        operation_id = str(payload.get("operation_id", ""))
        if re.fullmatch(r"[0-9a-f]{32}", operation_id) is None:
            raise DeployBootstrapError("Lab launchd handoff operation is invalid")
        completed_path = _completed_handoff_path(self.lock_path, operation_id)
        if completed_path.exists():
            completed = _private_json(
                completed_path,
                label="completed Lab launchd handoff proof",
            )
            if (
                completed.get("operation_id") != operation_id
                or completed.get("stage") != "completed"
                or set(completed.get("restarted_labels", ())) != set(LAB_LAUNCHD_LABELS)
            ):
                raise DeployBootstrapError("completed Lab launchd handoff proof is invalid")
            return False
        if payload.get("stage") == "completed":
            raise DeployBootstrapError("completed Lab launchd handoff proof is missing")
        labels = [str(value) for value in payload["labels"]]
        loaded_labels = [str(value) for value in payload["loaded_labels"]]
        stopped = [str(value) for value in payload["stopped_labels"]]
        restarted = [str(value) for value in payload["restarted_labels"]]
        if (
            set(labels) != set(LAB_LAUNCHD_LABELS)
            or loaded_labels != labels
            or not set(stopped).issubset(labels)
            or not set(restarted).issubset(labels)
        ):
            raise DeployBootstrapError("Lab launchd handoff state is invalid")
        binding_changed = (
            payload.get("target_ref") != self.target_ref
            or payload.get("target_sha") != self.target_sha
            or payload.get("action") != self.action
            or payload.get("release_profile") != self.release_profile
            or payload.get("lifecycle_mode") != self.lifecycle_mode
            or payload.get("installation_identity") != self.installation_identity
            or payload.get("supersedes_operation_id", "") != self.supersedes_operation_id
        )
        if binding_changed and payload.get("operation_id") == self.supersedes_operation_id:
            self.superseding_partial = True
            self.loaded = labels
            return False
        if binding_changed:
            raise DeployBootstrapError("Lab launchd handoff binding changed")
        self.operation_id = operation_id
        self.loaded = labels
        self.stopped = stopped
        self.restarted = restarted
        return True

    def _is_loaded(self, label: str) -> bool:
        result = _launchctl(
            ["print", f"{self.domain}/{label}"],
            check=False,
            timeout_seconds=self._remaining(),
        )
        if result.returncode == 0:
            return True
        if result.returncode in {3, 113}:
            return False
        raise DeployBootstrapError(f"Lab launchd state is unavailable for {label}")

    def prepare(
        self,
        *,
        dry_run: bool,
        target_ref: str,
        target_sha: str,
        action: str,
        now: datetime | None = None,
    ) -> None:
        if (
            TARGET_PATTERN.fullmatch(target_ref) is None
            or re.fullmatch(r"[0-9a-f]{40}", target_sha) is None
            or action not in {"deploy", "resume", "rollback"}
        ):
            raise DeployBootstrapError("Lab launchd handoff requires an exact target binding")
        self.target_ref = target_ref
        self.target_sha = target_sha
        self.action = action
        if self.enabled and not dry_run and _is_protected_handoff_window(now):
            raise DeployDeferredError(
                "Lab daemon handoff is deferred during the protected 09:15-15:10 window"
            )
        self.root_fd, self.lock_fd = _acquire_handoff_lock(self.root, self.lock_path)
        if self.enabled:
            self.installation = _read_lab_installation_state(
                root=self.root,
                lock_path=self.lock_path,
            )
            self.installation_identity = _lab_installation_identity(
                self.lock_path,
                self.installation,
            )
        if dry_run:
            if self.enabled:
                loaded = [label for label in LAB_LAUNCHD_LABELS if self._is_loaded(label)]
                if set(loaded) != set(LAB_LAUNCHD_LABELS):
                    raise DeployBootstrapError(
                        "all installed Lab launchd daemons must be loaded before deployment"
                    )
            print(
                json.dumps(
                    {
                        "lab_daemon_handoff": "planned",
                        "labels": list(LAB_LAUNCHD_LABELS),
                        "stopped": False,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return
        if not self.enabled:
            return
        resumed = self._load_incomplete_record()
        if not resumed:
            self.operation_id = secrets.token_hex(16)
            loaded = [label for label in LAB_LAUNCHD_LABELS if self._is_loaded(label)]
            if not self.superseding_partial and set(loaded) != set(LAB_LAUNCHD_LABELS):
                raise DeployBootstrapError(
                    "all installed Lab launchd daemons must be loaded before deployment"
                )
            self.loaded = list(LAB_LAUNCHD_LABELS) if self.superseding_partial else loaded
            self.stopped = (
                [label for label in self.loaded if label not in loaded]
                if self.superseding_partial
                else []
            )
            self.restarted = []
            self._record("planned")
        for label, plist in self.plists.items():
            _physical_file(plist, label=f"Lab launchd plist {label}")
            if not self._is_loaded(label):
                continue
            self._record("stopping")
            _launchctl(
                ["bootout", f"{self.domain}/{label}"],
                check=True,
                timeout_seconds=self._remaining(),
            )
            if label not in self.stopped:
                self.stopped.append(label)
            if label in self.restarted:
                self.restarted.remove(label)
            self._record("stopping")
        self._record("stopped")

    def restore(self) -> None:
        errors: list[str] = []
        if self.enabled:
            for label in self.loaded:
                try:
                    if self._is_loaded(label):
                        if label not in self.restarted:
                            self.restarted.append(label)
                            self._record("restarting")
                        continue
                    self._record("restarting")
                    _launchctl(
                        ["bootstrap", self.domain, str(self.plists[label])],
                        check=True,
                        timeout_seconds=self._remaining(),
                    )
                    if label not in self.restarted:
                        self.restarted.append(label)
                    self._record("restarting")
                except DeployBootstrapError as exc:
                    errors.append(str(exc))
            if self.loaded and not errors:
                try:
                    generation = _wait_for_lab_readiness(
                        root=self.root,
                        domain=self.domain,
                        labels=list(self.loaded),
                        lock_path=self.lock_path,
                        timeout_seconds=self._remaining(),
                    )
                    self._record("completed", generation=generation)
                except DeployBootstrapError as exc:
                    errors.append(str(exc))
        if self.lock_fd >= 0:
            os.close(self.lock_fd)
            self.lock_fd = -1
        if self.root_fd >= 0:
            os.close(self.root_fd)
            self.root_fd = -1
        if errors:
            raise DeployBootstrapError("; ".join(errors))

    def close(self) -> None:
        if self.lock_fd >= 0:
            os.close(self.lock_fd)
            self.lock_fd = -1
        if self.root_fd >= 0:
            os.close(self.root_fd)
            self.root_fd = -1


def _complete_installed_rollout(
    *,
    target_handoff: object,
    deploy_code: int,
    recovery_handoff_factory: Callable[[], object],
    rollback: Callable[[object], int],
    recovery_target_sha: str,
    now: datetime | None = None,
) -> int:
    try:
        target_handoff.restore()
    except DeployBootstrapError as readiness_error:
        recovery_handoff = recovery_handoff_factory()
        try:
            recovery_handoff.prepare(
                dry_run=False,
                target_ref=recovery_target_sha,
                target_sha=recovery_target_sha,
                action="rollback",
                now=now,
            )
            rollback_code = rollback(recovery_handoff)
            if rollback_code != 0:
                raise DeployBootstrapError(
                    "Lab readiness failed and previous generation rollback failed"
                )
            recovery_handoff.restore()
        except Exception:
            recovery_handoff.close()
            raise
        print(
            f"FAILED: target Lab readiness failed and rolled back: {readiness_error}",
            file=sys.stderr,
        )
        return 1
    return deploy_code


def _git_run(
    repo: Path,
    git_path: Path,
    *arguments: str,
    check: bool = True,
    text: bool = True,
    overall_deadline_monotonic: float | None = None,
) -> subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]:
    timeout_seconds = 10.0
    if overall_deadline_monotonic is not None:
        remaining = overall_deadline_monotonic - time.monotonic()
        if remaining <= 0:
            raise DeployBootstrapError("deployment overall timeout expired")
        timeout_seconds = min(timeout_seconds, remaining)
    try:
        return subprocess.run(
            [str(git_path), *arguments],
            cwd=repo,
            check=check,
            capture_output=True,
            text=text,
            timeout=timeout_seconds,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DeployBootstrapError("deployment checkout cannot be verified") from exc


def _run_git_mutation(
    repo: Path,
    git_path: Path,
    *arguments: str,
    overall_deadline_monotonic: float | None = None,
) -> subprocess.CompletedProcess[str]:
    timeout_seconds = 10.0
    if overall_deadline_monotonic is not None:
        remaining = overall_deadline_monotonic - time.monotonic()
        if remaining <= 0:
            raise DeployBootstrapError("deployment overall timeout expired")
        timeout_seconds = min(timeout_seconds, remaining)
    try:
        result = _run_process_group(
            [str(git_path), *arguments],
            cwd=repo,
            timeout_seconds=timeout_seconds,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "1", "GIT_TERMINAL_PROMPT": "0"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DeployBootstrapError("deployment checkout mutation failed") from exc
    if result.returncode != 0:
        diagnostic = (result.stderr or result.stdout or "no command output").strip()
        raise DeployBootstrapError(f"deployment checkout mutation failed: {diagnostic[:1000]}")
    return result


def _git_output(
    repo: Path,
    git_path: Path,
    *arguments: str,
    overall_deadline_monotonic: float | None = None,
) -> str:
    result = _git_run(
        repo,
        git_path,
        *arguments,
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    assert isinstance(result.stdout, str)
    return result.stdout.strip()


def _git_head(
    repo: Path,
    git_path: Path,
    *,
    overall_deadline_monotonic: float | None = None,
) -> str:
    return _git_output(
        repo,
        git_path,
        "rev-parse",
        "--verify",
        "HEAD^{commit}",
        overall_deadline_monotonic=overall_deadline_monotonic,
    )


def _tracked_checkout_is_clean(
    repo: Path,
    git_path: Path,
    *,
    overall_deadline_monotonic: float | None = None,
) -> None:
    status = _git_output(
        repo,
        git_path,
        "status",
        "--porcelain=v1",
        "--untracked-files=no",
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    diff = _git_run(
        repo,
        git_path,
        "diff-index",
        "--quiet",
        "HEAD",
        "--",
        check=False,
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    if status or diff.returncode != 0:
        raise DeployBootstrapError("tracked deployment checkout is dirty")


def _tracked_file_bytes(
    repo: Path,
    git_path: Path,
    commit: str,
    relative: str,
    *,
    overall_deadline_monotonic: float | None = None,
) -> bytes:
    result = _git_run(
        repo,
        git_path,
        "show",
        f"{commit}:{relative}",
        text=False,
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    assert isinstance(result.stdout, bytes)
    return result.stdout


def _verify_generation_target(
    repo: Path,
    git_path: Path,
    target: str,
    *,
    overall_deadline_monotonic: float | None = None,
) -> str:
    if TARGET_PATTERN.fullmatch(target) is None:
        raise DeployBootstrapError("generation target must be a SemVer tag or full SHA")
    if (
        _git_output(
            repo,
            git_path,
            "rev-parse",
            "--abbrev-ref",
            "HEAD",
            overall_deadline_monotonic=overall_deadline_monotonic,
        )
        != "main"
    ):
        raise DeployBootstrapError("generation checkout must be on main")
    _tracked_checkout_is_clean(
        repo,
        git_path,
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    commit = _git_output(
        repo,
        git_path,
        "rev-parse",
        "--verify",
        f"{target}^{{commit}}",
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    allowed = _git_run(
        repo,
        git_path,
        "merge-base",
        "--is-ancestor",
        commit,
        "origin/main",
        check=False,
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    if allowed.returncode != 0:
        raise DeployBootstrapError("generation target is not contained in origin/main")
    if (
        target.startswith("v")
        and _git_output(
            repo,
            git_path,
            "cat-file",
            "-t",
            target,
            overall_deadline_monotonic=overall_deadline_monotonic,
        )
        != "tag"
    ):
        raise DeployBootstrapError("generation SemVer target must be an annotated tag")

    pyproject_payload = _tracked_file_bytes(
        repo,
        git_path,
        commit,
        "pyproject.toml",
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    try:
        package_version = str(tomllib.loads(pyproject_payload.decode())["project"]["version"])
    except (UnicodeDecodeError, KeyError, tomllib.TOMLDecodeError) as exc:
        raise DeployBootstrapError("generation package version cannot be verified") from exc
    if target.startswith("v") and package_version != target[1:]:
        raise DeployBootstrapError("generation tag and package version disagree")
    return commit


def _verify_recovery_target_binding(
    *,
    lock_path: Path,
    target_ref: str,
    action: str,
    target_sha: str | None = None,
) -> str:
    intent = _private_json(
        lock_path.with_name(f"{lock_path.stem}.intent.json"),
        label="deployment intent",
    )
    try:
        schema_version = int(intent["schema_version"])
        operation_id = str(intent["operation_id"])
        previous_sha = str(intent["previous_sha"])
        recorded_target = str(intent["target_sha"])
        recorded_ref = str(intent["target_ref"])
        stage = str(intent["stage"])
    except (KeyError, TypeError, ValueError) as exc:
        raise DeployBootstrapError("deployment intent is malformed") from exc
    if (
        schema_version != 1
        or re.fullmatch(r"[0-9a-f]{32}", operation_id) is None
        or TARGET_PATTERN.fullmatch(previous_sha) is None
        or TARGET_PATTERN.fullmatch(recorded_target) is None
        or previous_sha.startswith("v")
        or recorded_target.startswith("v")
        or not recorded_ref
        or not stage
    ):
        raise DeployBootstrapError("deployment intent is malformed")
    expected_sha = previous_sha if action == "rollback" else recorded_target
    allowed_refs = {expected_sha} if action == "rollback" else {expected_sha, recorded_ref}
    if (
        action not in {"resume", "rollback"}
        or (target_sha is not None and target_sha != expected_sha)
        or target_ref not in allowed_refs
    ):
        raise DeployBootstrapError("recovery target does not match recorded deployment intent")
    return expected_sha


def _fetch_generation_target(
    repo: Path,
    git_path: Path,
    *,
    command_timeout_seconds: float,
    overall_deadline_monotonic: float,
) -> None:
    remaining = overall_deadline_monotonic - time.monotonic()
    if remaining <= 0:
        raise DeployBootstrapError("deployment overall timeout expired before Git fetch")
    try:
        result = _run_process_group(
            [str(git_path), "fetch", "--tags", "origin", "main"],
            cwd=repo,
            timeout_seconds=min(command_timeout_seconds, remaining),
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "1", "GIT_TERMINAL_PROMPT": "0"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DeployBootstrapError("deployment target fetch failed") from exc
    if result.returncode != 0:
        diagnostic = (result.stderr or result.stdout or "no command output").strip()
        raise DeployBootstrapError(f"deployment target fetch failed: {diagnostic[:1000]}")


def _verify_recorded_recovery_commit(
    repo: Path,
    git_path: Path,
    commit: str,
    *,
    overall_deadline_monotonic: float,
) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise DeployBootstrapError("recorded recovery commit is invalid")
    if (
        _git_output(
            repo,
            git_path,
            "rev-parse",
            "--abbrev-ref",
            "HEAD",
            overall_deadline_monotonic=overall_deadline_monotonic,
        )
        != "main"
    ):
        raise DeployBootstrapError("generation checkout must be on main")
    _tracked_checkout_is_clean(
        repo,
        git_path,
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    resolved = _git_output(
        repo,
        git_path,
        "rev-parse",
        "--verify",
        f"{commit}^{{commit}}",
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    if resolved != commit:
        raise DeployBootstrapError("recorded recovery commit identity changed")


def _verify_current_generation_checkout(
    repo: Path,
    git_path: Path,
    commit: str,
    *,
    overall_deadline_monotonic: float | None = None,
) -> None:
    if (
        _git_head(
            repo,
            git_path,
            overall_deadline_monotonic=overall_deadline_monotonic,
        )
        != commit
    ):
        raise DeployBootstrapError("generation target does not match current HEAD")
    _tracked_checkout_is_clean(
        repo,
        git_path,
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    for relative in ("uv.lock", "pyproject.toml"):
        path = repo / relative
        _physical_file(path, label=relative)
        try:
            working = path.read_bytes()
        except OSError as exc:
            raise DeployBootstrapError(f"{relative} cannot be read") from exc
        tracked = _tracked_file_bytes(
            repo,
            git_path,
            commit,
            relative,
            overall_deadline_monotonic=overall_deadline_monotonic,
        )
        if hashlib.sha256(working).digest() != hashlib.sha256(tracked).digest():
            raise DeployBootstrapError(f"{relative} does not match generation target")


def _verify_generation_runtime(
    root: Path,
    python_path: Path,
    *,
    overall_deadline_monotonic: float | None = None,
) -> None:
    venv = root / ".venv"
    _physical_directory(venv, label="release venv")
    if not python_path.is_relative_to(venv):
        raise DeployBootstrapError("deployment Python is outside release venv")
    _physical_file(venv / "pyvenv.cfg", label="pyvenv.cfg")
    timeout_seconds = 10.0
    if overall_deadline_monotonic is not None:
        remaining = overall_deadline_monotonic - time.monotonic()
        if remaining <= 0:
            raise DeployBootstrapError("deployment overall timeout expired")
        timeout_seconds = min(timeout_seconds, remaining)
    try:
        result = subprocess.run(
            [
                str(python_path),
                "-I",
                "-S",
                "-c",
                (
                    "import json,sys,sysconfig;"
                    "print(json.dumps({'version': '.'.join(map(str, sys.version_info[:3])),"
                    "'abi': (sys.implementation.cache_tag or '') + ':' + "
                    "(sysconfig.get_config_var('SOABI') or '')}, sort_keys=True))"
                ),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        facts = json.loads(result.stdout)
        version = str(facts["version"])
        abi = str(facts["abi"])
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, KeyError) as exc:
        raise DeployBootstrapError("release Python ABI cannot be verified") from exc
    if not version or abi == ":":
        raise DeployBootstrapError("release Python ABI is incomplete")
    major_minor = ".".join(version.split(".")[:2])
    _physical_directory(
        venv / "lib" / f"python{major_minor}" / "site-packages",
        label="release site-packages",
    )


def _generation_target(deploy_argv: list[str]) -> str:
    values = list(deploy_argv)
    if values and values[0] == "--":
        values.pop(0)
    parser = argparse.ArgumentParser(prog="generation-control")
    parser.add_argument("--target", required=True)
    parsed, _unknown = parser.parse_known_args(values)
    return str(parsed.target)


def _run_generation_preflight(
    root: Path,
    *,
    timeout_seconds: float = 300,
    overall_deadline_monotonic: float | None = None,
) -> None:
    launcher = root / ".venv" / "bin" / "rquant"
    _physical_file(launcher, label="rquant preflight launcher", executable=True)
    if overall_deadline_monotonic is not None:
        remaining = overall_deadline_monotonic - time.monotonic()
        if remaining <= 0:
            raise DeployBootstrapError("generation preflight overall timeout expired")
        timeout_seconds = min(timeout_seconds, remaining)
    try:
        result = _run_process_group(
            [str(launcher), "preflight"],
            cwd=root,
            timeout_seconds=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise DeployBootstrapError("generation preflight overall timeout expired") from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise DeployBootstrapError("generation preflight could not run") from exc
    if result.returncode != 0:
        diagnostic = (result.stderr or result.stdout or "no command output").strip()
        raise DeployBootstrapError(f"generation preflight failed: {diagnostic[:1000]}")


def _run_frozen_sync(
    root: Path,
    uv_path: Path,
    *,
    timeout_seconds: float = 900,
    overall_deadline_monotonic: float | None = None,
) -> None:
    if overall_deadline_monotonic is not None:
        remaining = overall_deadline_monotonic - time.monotonic()
        if remaining <= 0:
            raise DeployBootstrapError("frozen dependency sync overall timeout expired")
        timeout_seconds = min(timeout_seconds, remaining)
    try:
        result = _run_process_group(
            [str(uv_path), "sync", "--frozen"],
            cwd=root,
            timeout_seconds=timeout_seconds,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DeployBootstrapError("frozen dependency sync could not run") from exc
    if result.returncode != 0:
        diagnostic = (result.stderr or result.stdout or "no command output").strip()
        raise DeployBootstrapError(f"frozen dependency sync failed: {diagnostic[:1000]}")


def _prepare_generation_checkout(
    *,
    root: Path,
    git_path: Path,
    target_commit: str,
    mode: str,
    overall_deadline_monotonic: float | None = None,
) -> None:
    current = _git_head(
        root,
        git_path,
        overall_deadline_monotonic=overall_deadline_monotonic,
    )
    if mode == "initialize":
        if current != target_commit:
            raise DeployBootstrapError("initial generation target does not match current HEAD")
        return
    if mode == "resume":
        allowed = _git_run(
            root,
            git_path,
            "merge-base",
            "--is-ancestor",
            current,
            target_commit,
            check=False,
            overall_deadline_monotonic=overall_deadline_monotonic,
        )
        if allowed.returncode != 0:
            raise DeployBootstrapError("resume target is not a fast-forward from current HEAD")
        if current != target_commit:
            _run_git_mutation(
                root,
                git_path,
                "merge",
                "--ff-only",
                target_commit,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
        return
    if mode == "rollback":
        allowed = _git_run(
            root,
            git_path,
            "merge-base",
            "--is-ancestor",
            target_commit,
            current,
            check=False,
            overall_deadline_monotonic=overall_deadline_monotonic,
        )
        if allowed.returncode != 0:
            raise DeployBootstrapError("rollback target is not an ancestor of current HEAD")
        if current != target_commit:
            _run_git_mutation(
                root,
                git_path,
                "reset",
                "--hard",
                target_commit,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
        return
    raise DeployBootstrapError("unknown generation control mode")


def _load_release_authority(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("_rquant_release_generation", path)
    if spec is None or spec.loader is None:
        raise DeployBootstrapError("release generation authority cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _assert_inherited_lock(root: Path, lock_path: Path, descriptor: int) -> int:
    expected = root.parent / ".rquant-deploy" / f"{root.name}.lock"
    if lock_path != expected or descriptor < 0:
        raise DeployBootstrapError("inherited generation lock binding is invalid")
    try:
        opened = os.fstat(descriptor)
        active = lock_path.lstat()
    except OSError as exc:
        raise DeployBootstrapError("inherited generation lock is unavailable") from exc
    if (
        (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid, opened.st_nlink)
        != (active.st_dev, active.st_ino, active.st_mode, active.st_uid, active.st_nlink)
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.getuid()
        or opened.st_nlink != 1
        or stat.S_IMODE(opened.st_mode) != 0o600
    ):
        raise DeployBootstrapError("inherited generation lock identity changed")
    return descriptor


def _assert_inherited_handoff_lock(root: Path, lock_path: Path, descriptor: int) -> int:
    expected = root.parent / ".rquant-deploy" / f"{root.name}.lock"
    handoff_path = lock_path.with_name(f"{lock_path.stem}.handoff.lock")
    if lock_path != expected or descriptor < 0:
        raise DeployBootstrapError("inherited Lab handoff lock binding is invalid")
    try:
        opened = os.fstat(descriptor)
        active = handoff_path.lstat()
    except OSError as exc:
        raise DeployBootstrapError("inherited Lab handoff lock is unavailable") from exc
    if (
        (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid, opened.st_nlink)
        != (active.st_dev, active.st_ino, active.st_mode, active.st_uid, active.st_nlink)
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.getuid()
        or opened.st_nlink != 1
        or stat.S_IMODE(opened.st_mode) != 0o600
    ):
        raise DeployBootstrapError("inherited Lab handoff lock identity changed")
    return descriptor


def _normalized_deploy_argv(values: list[str]) -> list[str]:
    normalized = list(values)
    if normalized and normalized[0] == "--":
        normalized.pop(0)
    return normalized


def _replace_deployment_target(values: list[str], target: str) -> list[str]:
    replaced = list(values)
    try:
        index = replaced.index("--target")
        replaced[index + 1] = target
    except (ValueError, IndexError) as exc:
        raise DeployBootstrapError("deployment target argument is missing") from exc
    return replaced


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-checkout-root", required=True)
    parser.add_argument("--trusted-git-path", default="")
    parser.add_argument("--deployment-lock-path", required=True)
    parser.add_argument("--python-path", required=True)
    parser.add_argument("--uv-path", default="")
    parser.add_argument(
        "--release-profile",
        choices=("linux-production", "macos-lab"),
        required=True,
    )
    parser.add_argument("--host-platform", choices=("linux", "darwin"), required=True)
    parser.add_argument(
        "--lab-lifecycle-mode",
        default="",
    )
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--initialize-generation", action="store_true")
    modes.add_argument("--register-lab-installation", action="store_true")
    modes.add_argument("--recover-generation", action="store_true")
    modes.add_argument("--finalize-generation", action="store_true")
    parser.add_argument("--recovery-action", choices=("resume", "rollback"))
    parser.add_argument("--finalize-action", choices=("deploy", "resume", "rollback"))
    parser.add_argument("--finalize-phase", choices=("publish", "commit"))
    parser.add_argument("--operation-id")
    parser.add_argument("--inherited-lock-fd", type=int)
    parser.add_argument("--inherited-handoff-lock-fd", type=int)
    parser.add_argument("--lab-runtime-root")
    parser.add_argument("--lab-readiness-root")
    parser.add_argument("--command-timeout-seconds", default="")
    parser.add_argument("--overall-timeout-seconds", default="")
    parser.add_argument("--overall-deadline-monotonic", type=float)
    args, deploy_argv = parser.parse_known_args(argv)
    lock_fd = -1
    handoff_lock_fd = -1
    generation_error_type: type[BaseException] | None = None
    missing_record_type: type[BaseException] | None = None
    handoff: _LabLaunchdHandoff | None = None

    def finish(return_code: int) -> int:
        nonlocal handoff, handoff_lock_fd, lock_fd
        if lock_fd >= 0:
            os.close(lock_fd)
            lock_fd = -1
        if handoff_lock_fd >= 0:
            os.close(handoff_lock_fd)
            handoff_lock_fd = -1
        if handoff is not None:
            try:
                handoff.restore()
            except DeployBootstrapError as exc:
                print(f"Production deploy bootstrap failed: {exc}", file=sys.stderr)
                return_code = 2
            handoff = None
        return return_code

    try:
        root = _canonical(args.expected_checkout_root, label="deployment checkout")
        _physical_directory(root, label="deployment checkout")
        if Path.cwd().resolve(strict=True) != root:
            raise DeployBootstrapError("working directory does not match deployment checkout")
        controls = _read_deploy_controls(root / ".env")
        for key in (
            "RQUANT_RELEASE_GENERATION_GC_GRACE_SECONDS",
            "RQUANT_RELEASE_GENERATION_MIN_FREE_BYTES",
        ):
            if key in controls and key not in os.environ:
                os.environ[key] = controls[key]
        args.trusted_git_path = (
            args.trusted_git_path or controls.get("LAB_TRUSTED_GIT_PATH") or "/usr/bin/git"
        )
        args.uv_path = args.uv_path or controls.get("RQUANT_DEPLOY_UV", "")
        args.command_timeout_seconds = _deploy_timeout(
            str(args.command_timeout_seconds)
            or controls.get("RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS", ""),
            default=300,
            label="deployment command timeout",
        )
        args.overall_timeout_seconds = _deploy_timeout(
            str(args.overall_timeout_seconds)
            or controls.get("RQUANT_DEPLOY_OVERALL_TIMEOUT_SECONDS", ""),
            default=1800,
            label="deployment overall timeout",
        )
        if args.host_platform == "linux":
            if args.lab_lifecycle_mode not in {"", "uninstalled"}:
                raise DeployBootstrapError("Linux deployment cannot enable Lab lifecycle")
            args.lab_lifecycle_mode = "uninstalled"
        else:
            args.lab_lifecycle_mode = (
                args.lab_lifecycle_mode or controls.get("RQUANT_LAB_LIFECYCLE_MODE") or "installed"
            )
        if args.lab_lifecycle_mode not in {"uninstalled", "installed"}:
            raise DeployBootstrapError("Lab lifecycle mode is invalid")
        _validate_profile_controls(
            controls,
            release_profile=args.release_profile,
            host_platform=args.host_platform,
        )
        if args.recover_generation != (args.recovery_action is not None):
            raise DeployBootstrapError(
                "--recovery-action is required only with --recover-generation"
            )
        lock_path = _canonical(args.deployment_lock_path, label="deployment lock")
        git_path = _canonical(args.trusted_git_path, label="trusted Git")
        _trusted_git(git_path)
        python_path = _canonical(args.python_path, label="deployment Python")
        _verified_venv_python(root, python_path)
        uv_path, _uv_binding = _resolve_uv_path(args.uv_path)
        if not 0 < args.command_timeout_seconds <= args.overall_timeout_seconds <= 7200:
            raise DeployBootstrapError("deployment timeout configuration is invalid")
        computed_deadline = time.monotonic() + args.overall_timeout_seconds
        overall_deadline_monotonic = (
            computed_deadline
            if args.overall_deadline_monotonic is None
            else min(computed_deadline, args.overall_deadline_monotonic)
        )
        if not math.isfinite(overall_deadline_monotonic):
            raise DeployBootstrapError("deployment overall deadline is invalid")
        dry_run = "--dry-run" in _normalized_deploy_argv(deploy_argv)
        if sys.platform == "darwin":
            actual_platform = "darwin"
        elif sys.platform.startswith("linux"):
            actual_platform = "linux"
        else:
            actual_platform = ""
        if args.host_platform != actual_platform or (
            (args.release_profile == "macos-lab") != (args.host_platform == "darwin")
        ):
            raise DeployBootstrapError("release profile does not match host platform")
        deploy_values = _normalized_deploy_argv(deploy_argv)
        installed_handoff = (
            args.release_profile == "macos-lab"
            and args.lab_lifecycle_mode == "installed"
            and not (args.initialize_generation or args.register_lab_installation)
            and not args.finalize_generation
        )
        if installed_handoff:
            _read_lab_installation_state(root=root, lock_path=lock_path)
        incomplete_handoff = (
            _incomplete_handoff_exists(root=root, lock_path=lock_path)
            if installed_handoff
            else False
        )
        if (
            installed_handoff
            and _is_protected_handoff_window()
            and (incomplete_handoff or not dry_run)
        ):
            detail = (
                "incomplete Lab daemon handoff recovery"
                if incomplete_handoff
                else "Lab daemon handoff"
            )
            raise DeployDeferredError(
                f"{detail} is deferred during the protected 09:15-15:10 window"
            )
        target_ref = _generation_target(deploy_values)
        handoff_action = str(args.recovery_action or "deploy")
        if args.recover_generation:
            target_sha = _verify_recovery_target_binding(
                lock_path=lock_path,
                target_ref=target_ref,
                action=handoff_action,
            )
            _verify_recorded_recovery_commit(
                root,
                git_path,
                target_sha,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
        elif args.finalize_generation:
            if re.fullmatch(r"[0-9a-f]{40}", target_ref) is None:
                raise DeployBootstrapError("finalizer target must be a full commit SHA")
            target_sha = target_ref
        else:
            if not (args.initialize_generation or args.register_lab_installation):
                _fetch_generation_target(
                    root,
                    git_path,
                    command_timeout_seconds=args.command_timeout_seconds,
                    overall_deadline_monotonic=overall_deadline_monotonic,
                )
            target_sha = _verify_generation_target(
                root,
                git_path,
                target_ref,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
        if args.finalize_generation:
            if args.inherited_lock_fd is None:
                raise DeployBootstrapError("finalizer requires inherited generation lock")
            lock_fd = _assert_inherited_lock(root, lock_path, args.inherited_lock_fd)
            if args.lab_lifecycle_mode == "installed":
                if args.inherited_handoff_lock_fd is None:
                    raise DeployBootstrapError("finalizer requires inherited Lab handoff lock")
                handoff_lock_fd = _assert_inherited_handoff_lock(
                    root,
                    lock_path,
                    args.inherited_handoff_lock_fd,
                )
            elif args.inherited_handoff_lock_fd is not None:
                raise DeployBootstrapError(
                    "inherited Lab handoff lock requires installed lifecycle"
                )
        else:
            if dry_run and (args.initialize_generation or args.recover_generation):
                raise DeployBootstrapError("generation initialization/recovery cannot be a dry-run")
            if not (args.initialize_generation or args.register_lab_installation):
                handoff = _LabLaunchdHandoff(
                    root=root,
                    lock_path=lock_path,
                    timeout_seconds=LAUNCHD_HANDOFF_TIMEOUT_SECONDS,
                    overall_timeout_seconds=args.overall_timeout_seconds,
                    overall_deadline_monotonic=overall_deadline_monotonic,
                    release_profile=args.release_profile,
                    lifecycle_mode=args.lab_lifecycle_mode,
                )
                handoff.prepare(
                    dry_run=dry_run,
                    target_ref=target_ref,
                    target_sha=target_sha,
                    action=handoff_action,
                )
            lock_fd = _acquire_lock(
                root,
                lock_path,
                shared=dry_run,
                create=not (dry_run and args.register_lab_installation),
                timeout_seconds=(
                    LAUNCHD_HANDOFF_TIMEOUT_SECONDS
                    if handoff is not None and handoff.stopped
                    else 0
                ),
            )
        authority_path = root / "src" / "rquant" / "release_generation.py"
        generation_mode = (
            args.initialize_generation or args.register_lab_installation or args.recover_generation
        )
        finalize_arguments_present = any(
            value is not None
            for value in (
                args.finalize_action,
                args.finalize_phase,
                args.operation_id,
                args.inherited_lock_fd,
                args.inherited_handoff_lock_fd,
            )
        )
        if args.finalize_generation and (
            args.finalize_action is None
            or args.finalize_phase is None
            or args.operation_id is None
            or args.inherited_lock_fd is None
            or (args.lab_lifecycle_mode == "installed" and args.inherited_handoff_lock_fd is None)
        ):
            raise DeployBootstrapError(
                "finalize action and operation id are required only with finalizer mode"
            )
        if not args.finalize_generation and finalize_arguments_present:
            raise DeployBootstrapError("finalizer arguments require finalizer mode")
        target = (
            _generation_target(deploy_argv) if generation_mode or args.finalize_generation else ""
        )
        if args.initialize_generation:
            commit = target_sha
            _prepare_generation_checkout(
                root=root,
                git_path=git_path,
                target_commit=commit,
                mode="initialize",
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
            _physical_file(authority_path, label="release generation authority")
            authority_module = _load_release_authority(authority_path)
            generation_error_type = authority_module.ReleaseGenerationError
            missing_record_type = authority_module.ReleaseGenerationRecordMissingError
            authority = authority_module.ReleaseGenerationAuthority(
                repo=root,
                lock_path=lock_path,
                lock_fd=lock_fd,
                python_path=python_path,
                git_path=git_path,
                writable=True,
                uv_path=uv_path,
                command_timeout_seconds=args.command_timeout_seconds,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
            try:
                initialization = authority.read_initialization()
            except missing_record_type:
                initialization = authority.begin_initialization(target_sha=commit)
            else:
                if initialization.target_sha != commit:
                    raise DeployBootstrapError("initialization target is already pinned")
                if initialization.stage == "completed":
                    try:
                        authority.verify(expected_commit=commit)
                    except generation_error_type as exc:
                        if "commit record is missing" not in str(exc):
                            raise generation_error_type(
                                "release generation initialization already completed"
                            ) from exc
                        _run_frozen_sync(
                            root,
                            uv_path,
                            timeout_seconds=args.command_timeout_seconds,
                            overall_deadline_monotonic=overall_deadline_monotonic,
                        )
                        _verify_current_generation_checkout(
                            root,
                            git_path,
                            commit,
                            overall_deadline_monotonic=overall_deadline_monotonic,
                        )
                        _verify_generation_runtime(
                            root,
                            python_path,
                            overall_deadline_monotonic=overall_deadline_monotonic,
                        )
                        _run_generation_preflight(
                            root,
                            timeout_seconds=args.command_timeout_seconds,
                            overall_deadline_monotonic=overall_deadline_monotonic,
                        )
                        authority.commit_generation(
                            operation_id=initialization.operation_id,
                            transaction_kind="initialization",
                        )
                        print(
                            json.dumps(
                                {
                                    "commit": commit,
                                    "status": "generation_initialization_recovered",
                                },
                                sort_keys=True,
                            )
                        )
                        return finish(0)
                    raise generation_error_type(
                        "release generation initialization already completed"
                    )
            _run_frozen_sync(
                root,
                uv_path,
                timeout_seconds=args.command_timeout_seconds,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
            _verify_current_generation_checkout(
                root,
                git_path,
                commit,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
            _verify_generation_runtime(
                root,
                python_path,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
            _run_generation_preflight(
                root,
                timeout_seconds=args.command_timeout_seconds,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
            authority.publish(
                expected_commit=commit,
                operation_id=initialization.operation_id,
                transaction_kind="initialization",
            )
            authority.complete_initialization(operation_id=initialization.operation_id)
            authority.commit_generation(
                operation_id=initialization.operation_id,
                transaction_kind="initialization",
            )
            print(
                json.dumps(
                    {"commit": commit, "status": "generation_initialized"},
                    sort_keys=True,
                )
            )
            return finish(0)

        if args.register_lab_installation:
            commit = target_sha
            if commit != _git_head(
                root,
                git_path,
                overall_deadline_monotonic=overall_deadline_monotonic,
            ):
                raise DeployBootstrapError(
                    "Lab installation registration target must be the current checkout"
                )
            _tracked_checkout_is_clean(
                root,
                git_path,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
            _verify_generation_runtime(
                root,
                python_path,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
            _physical_file(authority_path, label="release generation authority")
            authority_module = _load_release_authority(authority_path)
            generation_error_type = authority_module.ReleaseGenerationError
            authority_module.ReleaseGenerationAuthority(
                repo=root,
                lock_path=lock_path,
                lock_fd=lock_fd,
                python_path=python_path,
                git_path=git_path,
                uv_path=uv_path,
                command_timeout_seconds=args.command_timeout_seconds,
                overall_deadline_monotonic=overall_deadline_monotonic,
            ).verify(expected_commit=commit)
            runtime_root = _canonical(
                args.lab_runtime_root or str(root / "data" / "lab-runtime"),
                label="Lab runtime root",
            )
            readiness_root = _canonical(
                args.lab_readiness_root or str(runtime_root / "readiness"),
                label="Lab readiness root",
            )
            _write_lab_installation_state(
                root=root,
                lock_path=lock_path,
                runtime_root=runtime_root,
                readiness_root=readiness_root,
                expected_commit=commit,
                publish=not dry_run,
            )
            print(
                json.dumps(
                    {
                        "commit": commit,
                        "status": (
                            "lab_installation_registration_planned"
                            if dry_run
                            else "lab_installation_registered"
                        ),
                    },
                    sort_keys=True,
                )
            )
            return finish(0)

        commit = _git_head(
            root,
            git_path,
            overall_deadline_monotonic=overall_deadline_monotonic,
        )
        _tracked_checkout_is_clean(
            root,
            git_path,
            overall_deadline_monotonic=overall_deadline_monotonic,
        )
        _verify_generation_runtime(
            root,
            python_path,
            overall_deadline_monotonic=overall_deadline_monotonic,
        )
        _physical_file(authority_path, label="release generation authority")
        authority_module = _load_release_authority(authority_path)
        generation_error_type = authority_module.ReleaseGenerationError
        missing_record_type = authority_module.ReleaseGenerationRecordMissingError
        authority = authority_module.ReleaseGenerationAuthority(
            repo=root,
            lock_path=lock_path,
            lock_fd=lock_fd,
            python_path=python_path,
            git_path=git_path,
            writable=args.recover_generation or args.finalize_generation,
            uv_path=uv_path,
            command_timeout_seconds=args.command_timeout_seconds,
            overall_deadline_monotonic=overall_deadline_monotonic,
        )

        if args.finalize_generation:
            if TARGET_PATTERN.fullmatch(target) is None or target.startswith("v"):
                raise DeployBootstrapError("finalizer target must be a full commit SHA")
            intent = authority.read_deployment_intent()
            action = str(args.finalize_action)
            expected_commit = intent.previous_sha if action == "rollback" else intent.target_sha
            expected_stage = "timers_restored" if args.finalize_phase == "publish" else "completed"
            if (
                intent.operation_id != args.operation_id
                or target != expected_commit
                or intent.stage != expected_stage
            ):
                raise DeployBootstrapError("finalizer does not match ready deployment intent")
            _verify_current_generation_checkout(
                root,
                git_path,
                expected_commit,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
            _run_generation_preflight(
                root,
                timeout_seconds=args.command_timeout_seconds,
                overall_deadline_monotonic=overall_deadline_monotonic,
            )
            if args.finalize_phase == "publish":
                result = authority.publish(
                    expected_commit=expected_commit,
                    operation_id=intent.operation_id,
                    transaction_kind="deployment",
                )
                schema_version = result.schema_version
            else:
                result = authority.commit_generation(
                    operation_id=intent.operation_id,
                    transaction_kind="deployment",
                )
                schema_version = result.schema_version
            print(
                json.dumps(
                    {
                        "commit": expected_commit,
                        "operation_id": intent.operation_id,
                        "schema_version": schema_version,
                        "status": f"generation_{args.finalize_phase}",
                    },
                    sort_keys=True,
                )
            )
            return finish(0)

        if args.recover_generation:
            intent = authority.read_deployment_intent()
            action = str(args.recovery_action)
            expected_target = intent.previous_sha if action == "rollback" else intent.target_sha
            allowed_refs = {expected_target}
            if action == "resume":
                allowed_refs.add(intent.target_ref)
            if target not in allowed_refs:
                raise DeployBootstrapError(
                    "recovery target does not match recorded deployment intent"
                )
            if commit not in {intent.previous_sha, intent.target_sha}:
                raise DeployBootstrapError(
                    "recovery checkout is outside recorded deployment intent"
                )
        else:
            authority.verify(expected_commit=commit)

        src = root / "src"
        _physical_directory(src, label="deployment source root")
        sys.path.insert(0, str(src))
        from rquant.ops.production_deploy import main as deploy_main

        module = sys.modules.get("rquant.ops.production_deploy")
        module_path = Path(str(getattr(module, "__file__", ""))).resolve(strict=True)
        if module_path != (src / "rquant" / "ops" / "production_deploy.py"):
            raise DeployBootstrapError("production deployer imported outside locked generation")
        deploy_argv = _normalized_deploy_argv(deploy_argv)
        if args.recover_generation:
            deploy_argv.extend(["--recovery-action", str(args.recovery_action)])

        def invoke_deployer(
            values: list[str],
            *,
            startup_generation: str,
            active_handoff: _LabLaunchdHandoff | None,
            overall_deadline: float,
        ) -> int:
            arguments = list(values)
            if active_handoff is not None and active_handoff.enabled and not dry_run:
                arguments.extend(["--lab-handoff-operation-id", active_handoff.operation_id])
                arguments.extend(["--lab-handoff-lock-fd", str(active_handoff.lock_fd)])
                for label in active_handoff.loaded:
                    arguments.extend(["--lab-handoff-label", label])
            return int(
                deploy_main(
                    [
                        *arguments,
                        "--repo",
                        str(root),
                        "--deployment-lock-path",
                        str(lock_path),
                        "--deployment-lock-fd",
                        str(lock_fd),
                        "--startup-generation",
                        startup_generation,
                        "--trusted-git-path",
                        str(git_path),
                        "--python-path",
                        str(python_path),
                        "--uv-path",
                        str(uv_path),
                        "--release-profile",
                        args.release_profile,
                        "--platform-name",
                        args.host_platform,
                        "--lab-lifecycle-mode",
                        args.lab_lifecycle_mode,
                        "--command-timeout-seconds",
                        str(args.command_timeout_seconds),
                        "--overall-timeout-seconds",
                        str(args.overall_timeout_seconds),
                        "--overall-deadline-monotonic",
                        str(overall_deadline),
                    ]
                )
            )

        deploy_code = invoke_deployer(
            deploy_argv,
            startup_generation=commit,
            active_handoff=handoff,
            overall_deadline=overall_deadline_monotonic,
        )
        if handoff is not None and handoff.enabled and not dry_run:
            target_handoff = handoff
            handoff = None
            rollback_target = commit
            if args.recover_generation:
                rollback_target = authority.read_deployment_intent().previous_sha
            if lock_fd >= 0:
                os.close(lock_fd)
                lock_fd = -1

            def recovery_handoff_factory() -> _LabLaunchdHandoff:
                return _LabLaunchdHandoff(
                    root=root,
                    lock_path=lock_path,
                    timeout_seconds=LAUNCHD_HANDOFF_TIMEOUT_SECONDS,
                    overall_timeout_seconds=args.overall_timeout_seconds,
                    release_profile=args.release_profile,
                    lifecycle_mode=args.lab_lifecycle_mode,
                    supersedes_operation_id=target_handoff.operation_id,
                )

            def rollback_after_readiness(
                recovery_handoff: object,
            ) -> int:
                nonlocal lock_fd
                if not rollback_target or not isinstance(recovery_handoff, _LabLaunchdHandoff):
                    raise DeployBootstrapError("Lab readiness rollback is not bound")
                lock_fd = _acquire_lock(
                    root,
                    lock_path,
                    timeout_seconds=LAUNCHD_HANDOFF_TIMEOUT_SECONDS,
                )
                recovery_values = _replace_deployment_target(
                    deploy_argv,
                    rollback_target,
                )
                recovery_values.extend(["--recovery-action", "rollback"])
                try:
                    return invoke_deployer(
                        recovery_values,
                        startup_generation=_git_head(
                            root,
                            git_path,
                            overall_deadline_monotonic=recovery_handoff.deadline,
                        ),
                        active_handoff=recovery_handoff,
                        overall_deadline=recovery_handoff.deadline,
                    )
                finally:
                    if lock_fd >= 0:
                        os.close(lock_fd)
                        lock_fd = -1

            return _complete_installed_rollout(
                target_handoff=target_handoff,
                deploy_code=deploy_code,
                recovery_handoff_factory=recovery_handoff_factory,
                rollback=rollback_after_readiness,
                recovery_target_sha=rollback_target,
            )
        return finish(deploy_code)
    except DeployDeferredError as exc:
        print(f"Production deploy bootstrap deferred: {exc}", file=sys.stderr)
        return finish(exc.exit_code)
    except Exception as exc:
        expected = isinstance(exc, (DeployBootstrapError, OSError, subprocess.SubprocessError))
        if generation_error_type is not None and isinstance(exc, generation_error_type):
            expected = True
        if not expected:
            raise
        print(f"Production deploy bootstrap failed: {exc}", file=sys.stderr)
        return finish(2)
    finally:
        if lock_fd >= 0:
            os.close(lock_fd)
            lock_fd = -1
        if handoff_lock_fd >= 0:
            os.close(handoff_lock_fd)
            handoff_lock_fd = -1
        if handoff is not None:
            try:
                handoff.restore()
            except DeployBootstrapError as exc:
                print(f"Production deploy bootstrap cleanup failed: {exc}", file=sys.stderr)
            handoff = None


if __name__ == "__main__":
    raise SystemExit(main())
