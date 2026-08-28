#!/usr/bin/env python3
"""Detect unsafe runtime artifacts before Lab daemon rollout."""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import stat
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

sys.dont_write_bytecode = True

EXECUTABLE_SUFFIXES = frozenset({".pyc", ".pyo", ".so", ".dylib", ".pyd"})
GIT_TIMEOUT_SECONDS = 5
LAB_RUNTIME_PREPARED_SCHEMA_VERSION = 2
LAB_RUNTIME_DIRECTORY_DEFAULTS = {
    "lab command spool": ("LAB_JOB_COMMAND_DIR", "commands"),
    "lab claim spool": ("LAB_JOB_CLAIM_DIR", "claims"),
    "lab report spool": ("LAB_JOB_REPORT_DIR", "reports"),
    "lab worker artifact root": ("LAB_WORKER_ARTIFACT_DIR", "worker-artifacts"),
    "lab final artifact root": ("LAB_FINAL_ARTIFACT_DIR", "final-artifacts"),
    "lab artifact commit spool": ("LAB_ARTIFACT_COMMIT_DIR", "artifact-commits"),
    "lab daemon lock root": ("LAB_DAEMON_LOCK_DIR", "locks"),
    "lab finalizer state root": ("LAB_FINALIZER_STATE_DIR", "finalizer-state"),
    "lab readiness root": ("LAB_READINESS_DIR", "readiness"),
}
LAB_RUNTIME_PATH_KEYS = frozenset(
    {
        "DATA_DIR",
        "LAB_RUNTIME_DIR",
        "LAB_JOBS_PATH",
        *(key for key, _default in LAB_RUNTIME_DIRECTORY_DEFAULTS.values()),
    }
)
_DOTENV_ASSIGNMENT = re.compile(
    r"^(?:export[ \t]+)?(?P<key>[A-Za-z_][A-Za-z0-9_]*)[ \t]*=[ \t]*(?P<value>.*)$",
)


class PreflightError(RuntimeError):
    pass


def _load_strict_json() -> tuple[
    type[ValueError],
    Callable[[str | bytes | bytearray], object],
    Callable[..., object],
]:
    path = Path(__file__).resolve().with_name("strict_json.py")
    spec = importlib.util.spec_from_file_location("_rquant_preflight_strict_json", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("strict JSON authority cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return (
        module.StrictJsonError,
        module.strict_json_loads,
        module.strict_canonical_json_loads,
    )


StrictJsonError, strict_json_loads, strict_canonical_json_loads = _load_strict_json()


def _load_contained_runner() -> Callable[..., subprocess.CompletedProcess[object]]:
    path = Path(__file__).resolve().parents[1] / "src" / "rquant" / "contained_subprocess.py"
    spec = importlib.util.spec_from_file_location("_rquant_preflight_contained_process", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("contained subprocess authority cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.run_contained


run_contained = _load_contained_runner()


@dataclass(frozen=True)
class _ExecutableIdentity:
    device: int
    inode: int
    mode: int
    owner: int
    links: int


def _trusted_git(raw: str) -> tuple[Path, _ExecutableIdentity]:
    path = Path(raw)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise PreflightError("trusted Git path must be absolute and canonical")
    try:
        if path.resolve(strict=True) != path:
            raise PreflightError("trusted Git path must be physical")
        for parent in path.parents:
            parent_stat = parent.lstat()
            if (
                not stat.S_ISDIR(parent_stat.st_mode)
                or stat.S_ISLNK(parent_stat.st_mode)
                or parent_stat.st_uid != 0
                or parent_stat.st_mode & 0o022
            ):
                raise PreflightError("trusted Git parent path is unsafe")
        observed = path.lstat()
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            opened = os.fstat(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise PreflightError("trusted Git executable is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != 0
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
        or (observed.st_dev, observed.st_ino, observed.st_mode, observed.st_uid)
        != (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid)
    ):
        raise PreflightError("trusted Git executable is unsafe")
    return path, _ExecutableIdentity(
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
        observed.st_nlink,
    )


def _assert_trusted_git(path: Path, expected: _ExecutableIdentity) -> None:
    rebound_path, rebound = _trusted_git(str(path))
    if rebound_path != path or rebound != expected:
        raise PreflightError("trusted Git executable identity changed")


def _git_command(
    checkout: Path,
    arguments: list[str],
    *,
    git_path: Path,
    git_identity: _ExecutableIdentity,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    _assert_trusted_git(git_path, git_identity)
    try:
        result = run_contained(
            [str(git_path), *arguments],
            cwd=checkout,
            deadline_monotonic=time.monotonic() + GIT_TIMEOUT_SECONDS,
            check=False,
            text=True,
            env={
                **os.environ,
                "GIT_OPTIONAL_LOCKS": "0",
                "GIT_TERMINAL_PROMPT": "0",
            },
            may_spawn_background_descendants=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PreflightError("Git checkout verification failed closed") from exc
    _assert_trusted_git(git_path, git_identity)
    if check and result.returncode != 0:
        raise PreflightError("Git checkout verification failed closed")
    return result


def _physical_checkout_root(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise PreflightError("checkout root must be an absolute canonical path")
    try:
        if path.resolve(strict=True) != path:
            raise PreflightError("checkout root must be a physical directory")
        observed = path.lstat()
    except OSError as exc:
        raise PreflightError("checkout root is unavailable") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
    ):
        raise PreflightError("checkout root must be an owned physical directory")
    package_root = path / "src" / "rquant"
    try:
        package_root.lstat()
    except OSError as exc:
        raise PreflightError("src/rquant is unavailable") from exc
    if not package_root.is_dir() or package_root.is_symlink():
        raise PreflightError("src/rquant must be a physical directory")
    return path


def _checkout_root(
    raw: str,
    *,
    git_path: Path,
    git_identity: _ExecutableIdentity,
) -> Path:
    path = _physical_checkout_root(raw)
    top_level = _git_command(
        path,
        ["rev-parse", "--show-toplevel"],
        git_path=git_path,
        git_identity=git_identity,
    ).stdout.strip()
    if Path(top_level) != path:
        raise PreflightError("checkout root does not match Git top-level")
    return path


def _verify_tracked_checkout(
    checkout: Path,
    *,
    expected_commit: str,
    git_path: Path,
    git_identity: _ExecutableIdentity,
) -> None:
    if len(expected_commit) != 40 or any(
        character not in "0123456789abcdef" for character in expected_commit
    ):
        raise PreflightError("expected commit must be a lowercase full Git SHA")
    observed_commit = _git_command(
        checkout,
        ["rev-parse", "--verify", "HEAD^{commit}"],
        git_path=git_path,
        git_identity=git_identity,
    ).stdout.strip()
    if observed_commit != expected_commit:
        raise PreflightError("checkout HEAD does not match expected commit")
    status = _git_command(
        checkout,
        ["status", "--porcelain=v1", "--untracked-files=no"],
        git_path=git_path,
        git_identity=git_identity,
    )
    diff_index = _git_command(
        checkout,
        ["diff-index", "--quiet", "HEAD", "--"],
        git_path=git_path,
        git_identity=git_identity,
        check=False,
    )
    if status.stdout or diff_index.returncode != 0:
        raise PreflightError("tracked checkout is dirty")


def _runtime_artifacts(checkout: Path) -> tuple[Path, ...]:
    package_root = checkout / "src" / "rquant"
    found: list[Path] = []
    try:
        for current_root, directory_names, file_names in os.walk(
            package_root,
            topdown=True,
            followlinks=False,
        ):
            current = Path(current_root)
            physical_directories: list[str] = []
            for name in directory_names:
                child = current / name
                if child.is_symlink():
                    found.append(child)
                else:
                    physical_directories.append(name)
            directory_names[:] = physical_directories
            for name in file_names:
                child = current / name
                if child.is_symlink() or child.suffix.lower() in EXECUTABLE_SUFFIXES:
                    found.append(child)
    except OSError as exc:
        raise PreflightError("runtime artifact scan failed closed") from exc
    return tuple(sorted(set(found)))


def _related_lab_key(raw_line: str) -> str | None:
    candidate = raw_line.strip()
    export_match = re.match(r"export[ \t]+", candidate)
    ambiguous_export = re.match(r"export[ \t]+", candidate, flags=re.IGNORECASE)
    if export_match is not None:
        candidate = candidate[export_match.end() :]
    elif ambiguous_export is not None:
        candidate = candidate[ambiguous_export.end() :]
    match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", candidate)
    if match is None:
        return None
    key = match.group(1).upper()
    return key if key in LAB_RUNTIME_PATH_KEYS else None


def _parse_dotenv_value(raw: str, *, key: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if value[0] in {"'", '"'}:
        quote = value[0]
        closing = value.find(quote, 1)
        if closing < 0:
            raise PreflightError(f"checkout .env has unsupported {key} quoting")
        parsed = value[1:closing]
        trailing = value[closing + 1 :].strip()
        if trailing and not trailing.startswith("#"):
            raise PreflightError(f"checkout .env has unsupported {key} syntax")
    else:
        parsed = re.split(r"[ \t]+#", value, maxsplit=1)[0].rstrip()
    if any(character in parsed for character in ("\\", "$", "\n", "\r")):
        raise PreflightError(f"checkout .env has unsupported {key} expansion or escape")
    return parsed


def _path_identity(observed: os.stat_result) -> tuple[int, int, int, int, int]:
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
    maximum_bytes: int | None,
    private_parent: bool,
    missing_ok: bool = False,
    allowed_modes: frozenset[int] = frozenset({0o600}),
) -> tuple[bytes | None, os.stat_result | None]:
    parent = path.parent
    try:
        before_parent = parent.lstat()
        if (
            not stat.S_ISDIR(before_parent.st_mode)
            or stat.S_ISLNK(before_parent.st_mode)
            or before_parent.st_uid != os.getuid()
            or before_parent.st_mode & 0o022
            or (private_parent and stat.S_IMODE(before_parent.st_mode) != 0o700)
            or parent.resolve(strict=True) != parent
        ):
            raise PreflightError(f"{label} parent is unsafe")
        root_fd = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except PreflightError:
        raise
    except OSError as exc:
        raise PreflightError(f"{label} parent is unavailable") from exc
    descriptor = -1
    try:
        opened_parent = os.fstat(root_fd)
        if _path_identity(opened_parent) != _path_identity(before_parent):
            raise PreflightError(f"{label} parent identity changed")
        try:
            descriptor = os.open(
                path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
        except FileNotFoundError:
            if missing_ok:
                return None, None
            raise
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) not in allowed_modes
            or (maximum_bytes is not None and opened.st_size > maximum_bytes)
        ):
            raise PreflightError(f"{label} must be an owned physical private file")
        payload: bytes | None = None
        if maximum_bytes is not None:
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - total))
                if not chunk:
                    break
                total += len(chunk)
                if total > maximum_bytes:
                    raise PreflightError(f"{label} is too large")
                chunks.append(chunk)
            payload = b"".join(chunks)
        active = os.stat(path.name, dir_fd=root_fd, follow_symlinks=False)
        rebound_parent = parent.lstat()
        if _path_identity(active) != _path_identity(opened) or _path_identity(
            rebound_parent
        ) != _path_identity(opened_parent):
            raise PreflightError(f"{label} identity changed")
        return payload, opened
    except PreflightError:
        raise
    except OSError as exc:
        raise PreflightError(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(root_fd)


def _dotenv_values(path: Path, *, immutable: bool = False) -> dict[str, str]:
    encoded, _identity_value = _read_bound_private_file(
        path,
        label="checkout .env",
        maximum_bytes=1024 * 1024,
        private_parent=False,
        missing_ok=True,
        allowed_modes=frozenset({0o400}) if immutable else frozenset({0o600}),
    )
    if encoded is None:
        return {}
    try:
        lines = encoded.decode("utf-8").splitlines()
    except UnicodeError as exc:
        raise PreflightError("checkout .env is unreadable") from exc
    exact_values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        assignment = _DOTENV_ASSIGNMENT.fullmatch(stripped)
        if assignment is None:
            related = _related_lab_key(stripped)
            if related is not None:
                raise PreflightError(f"checkout .env has unsupported {related} syntax")
            continue
        source_key = assignment.group("key")
        key = source_key.upper()
        if key in LAB_RUNTIME_PATH_KEYS:
            exact_values[source_key] = _parse_dotenv_value(assignment.group("value"), key=key)
    values: dict[str, str] = {}
    for source_key, value in exact_values.items():
        values[source_key.upper()] = value
    return values


def _configured_path(
    values: dict[str, str],
    key: str,
    default: Path | None,
    *,
    label: str,
) -> Path:
    raw = values.get(key.upper(), "")
    for environment_key, environment_value in os.environ.items():
        if environment_key.casefold() == key.casefold():
            raw = environment_value
    if not raw and default is None:
        raise PreflightError(f"{label} is required")
    path = Path(raw) if raw else default
    assert path is not None
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise PreflightError(f"{label} must be an absolute canonical path")
    return path


def _private_runtime_directory(path: Path, *, label: str) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise PreflightError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
        or path.resolve(strict=True) != path
    ):
        raise PreflightError(f"{label} must be an owned physical 0700 directory")
    return observed


def _private_runtime_file(path: Path, *, label: str) -> os.stat_result:
    _payload, opened = _read_bound_private_file(
        path,
        label=label,
        maximum_bytes=None,
        private_parent=True,
    )
    assert opened is not None
    return opened


def _verify_prepared_lab_runtime(
    checkout: Path,
    *,
    daemon_command: str,
    bind_checkout: bool = True,
    immutable_config: bool = False,
) -> None:
    if daemon_command == "lab-runtime-prepare":
        return
    values = _dotenv_values(checkout / ".env", immutable=immutable_config)
    data_dir = _configured_path(values, "DATA_DIR", None, label="DATA_DIR")
    runtime_root = _configured_path(
        values,
        "LAB_RUNTIME_DIR",
        data_dir / "lab-runtime",
        label="Lab runtime root",
    )
    if not os.path.lexists(runtime_root):
        raise PreflightError("Lab runtime prepared sentinel root is unavailable")
    root_identity = _private_runtime_directory(runtime_root, label="Lab runtime root")
    sentinel = runtime_root / ".prepared.json"
    encoded, _sentinel_identity = _read_bound_private_file(
        sentinel,
        label="Lab runtime prepared sentinel",
        maximum_bytes=1024 * 1024,
        private_parent=True,
    )
    assert encoded is not None
    try:
        payload = strict_canonical_json_loads(encoded.decode("utf-8"), trailing_newline=True)
    except (UnicodeError, StrictJsonError) as exc:
        raise PreflightError("Lab runtime prepared sentinel is malformed") from exc
    authority_id = payload.get("runtime_authority_id") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != LAB_RUNTIME_PREPARED_SCHEMA_VERSION
        or (bind_checkout and payload.get("checkout_root") != str(checkout))
        or payload.get("runtime_root") != str(runtime_root)
        or payload.get("runtime_device") != root_identity.st_dev
        or payload.get("runtime_inode") != root_identity.st_ino
        or not isinstance(authority_id, str)
        or len(authority_id) != 32
        or any(character not in "0123456789abcdef" for character in authority_id)
    ):
        raise PreflightError("Lab runtime prepared sentinel binding changed")
    directories = payload.get("managed_directories")
    files = payload.get("managed_files")
    migrations = payload.get("migration_sources")
    if (
        not isinstance(directories, dict)
        or set(directories) != set(LAB_RUNTIME_DIRECTORY_DEFAULTS)
        or not isinstance(files, dict)
        or set(files) != {"lab jobs SQLite"}
        or not isinstance(migrations, dict)
    ):
        raise PreflightError("Lab runtime prepared sentinel layout is incomplete")
    for label, (key, default_name) in LAB_RUNTIME_DIRECTORY_DEFAULTS.items():
        path = _configured_path(values, key, runtime_root / default_name, label=label)
        observed = _private_runtime_directory(path, label=label)
        if path.parent != runtime_root or directories.get(label) != {
            "path": str(path),
            "device": observed.st_dev,
            "inode": observed.st_ino,
            "mode": 0o700,
        }:
            raise PreflightError("Lab runtime prepared sentinel directory changed")
    database = _configured_path(
        values,
        "LAB_JOBS_PATH",
        runtime_root / "lab_jobs.sqlite3",
        label="Lab jobs SQLite",
    )
    binding = files.get("lab jobs SQLite")
    if database.parent != runtime_root or not isinstance(binding, dict):
        raise PreflightError("Lab runtime prepared sentinel database binding changed")
    for suffix in ("-wal", "-shm", "-journal"):
        if os.path.lexists(database.with_name(f"{database.name}{suffix}")):
            raise PreflightError("Lab runtime SQLite sidecar blocks daemon startup")
    if binding.get("exists") is True:
        observed = _private_runtime_file(database, label="Lab jobs SQLite")
        expected_binding = {
            "path": str(database),
            "device": observed.st_dev,
            "inode": observed.st_ino,
            "mode": 0o600,
            "exists": True,
        }
        if binding != expected_binding:
            raise PreflightError("Lab runtime prepared sentinel database changed")
    elif binding == {"path": str(database), "exists": False}:
        if os.path.lexists(database):
            raise PreflightError("Lab runtime database exists but is not registered")
        if daemon_command != "lab-scheduler":
            raise PreflightError("Lab runtime database is not initialized")
    else:
        raise PreflightError("Lab runtime prepared sentinel database binding changed")
    for target, migration in migrations.items():
        if (
            not isinstance(migration, dict)
            or set(migration) != {"source", "migrated"}
            or Path(target).parent != runtime_root
            or os.path.lexists(Path(str(migration.get("source", ""))))
        ):
            raise PreflightError("Lab runtime legacy migration binding changed")


def _assert_generation_lock(path: Path, descriptor: int) -> None:
    try:
        opened = os.fstat(descriptor)
        active = path.lstat()
    except OSError as exc:
        raise PreflightError("deployment generation lock is unavailable") from exc
    if (
        (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid, opened.st_nlink)
        != (active.st_dev, active.st_ino, active.st_mode, active.st_uid, active.st_nlink)
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.getuid()
        or opened.st_nlink != 1
        or stat.S_IMODE(opened.st_mode) != 0o600
    ):
        raise PreflightError("deployment generation lock identity changed")


def _load_release_authority(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("_rquant_release_generation", path)
    if spec is None or spec.loader is None:
        raise PreflightError("release generation authority cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkout-root", required=True)
    parser.add_argument("--expected-commit")
    parser.add_argument("--trusted-git-path")
    parser.add_argument("--deployment-lock-path")
    parser.add_argument("--deployment-lock-fd", type=int)
    parser.add_argument("--python-path")
    parser.add_argument("--provisional-handoff-label")
    parser.add_argument("--prepared-sentinel-only", action="store_true")
    parser.add_argument("--immutable-generation", action="store_true")
    parser.add_argument("--release-managed-checkout", action="store_true")
    parser.add_argument(
        "--lab-daemon-command",
        choices=(
            "lab-scheduler",
            "lab-worker",
            "lab-claim-finalizer",
            "lab-finalizer",
            "lab-runtime-prepare",
        ),
        required=True,
    )
    args = parser.parse_args(argv)
    try:
        if args.release_managed_checkout and args.immutable_generation:
            raise PreflightError(
                "release-managed checkout and immutable generation are mutually exclusive"
            )
        if args.prepared_sentinel_only:
            checkout = _physical_checkout_root(args.checkout_root)
            _verify_prepared_lab_runtime(
                checkout,
                daemon_command=args.lab_daemon_command,
                bind_checkout=not (args.immutable_generation or args.release_managed_checkout),
                immutable_config=args.immutable_generation,
            )
            print("Lab runtime preflight: verified prepared sentinel")
            return 0
        if any(
            value is None
            for value in (
                args.expected_commit,
                args.trusted_git_path,
                args.deployment_lock_path,
                args.deployment_lock_fd,
                args.python_path,
            )
        ):
            raise PreflightError("complete runtime verification arguments are required")
        git_path, git_identity = _trusted_git(args.trusted_git_path)
        checkout = _physical_checkout_root(args.checkout_root)
        _verify_prepared_lab_runtime(
            checkout,
            daemon_command=args.lab_daemon_command,
            bind_checkout=not (args.immutable_generation or args.release_managed_checkout),
            immutable_config=args.immutable_generation,
        )
        if args.immutable_generation:
            lock_path = Path(args.deployment_lock_path)
            _assert_generation_lock(lock_path, args.deployment_lock_fd)
            try:
                _load_release_authority(
                    checkout / "src" / "rquant" / "release_generation.py"
                ).ReleaseGenerationAuthority(
                    repo=checkout,
                    immutable_code_root=checkout,
                    lock_path=lock_path,
                    lock_fd=args.deployment_lock_fd,
                    python_path=Path(args.python_path),
                    git_path=git_path,
                ).verify(
                    expected_commit=args.expected_commit,
                    provisional_handoff_label=args.provisional_handoff_label,
                )
            except Exception as exc:
                raise PreflightError(f"release generation marker is invalid: {exc}") from exc
            print("Lab runtime preflight: verified immutable release generation")
            return 0
        checkout = _checkout_root(
            args.checkout_root,
            git_path=git_path,
            git_identity=git_identity,
        )
        _verify_tracked_checkout(
            checkout,
            expected_commit=args.expected_commit,
            git_path=git_path,
            git_identity=git_identity,
        )
        artifacts = _runtime_artifacts(checkout)
        if artifacts:
            preview = ", ".join(str(path.relative_to(checkout)) for path in artifacts[:20])
            if len(artifacts) > 20:
                preview = f"{preview}, ..."
            raise PreflightError(
                "ignored executable artifacts or package symlinks block formal runtime: "
                f"{len(artifacts)} artifact(s); manually verify and remove only these "
                f"repository entries, then rerun: {preview}"
            )
        lock_path = Path(args.deployment_lock_path)
        _assert_generation_lock(lock_path, args.deployment_lock_fd)
        try:
            _load_release_authority(
                checkout / "src" / "rquant" / "release_generation.py"
            ).ReleaseGenerationAuthority(
                repo=checkout,
                lock_path=lock_path,
                lock_fd=args.deployment_lock_fd,
                python_path=Path(args.python_path),
                git_path=git_path,
            ).verify(
                expected_commit=args.expected_commit,
                provisional_handoff_label=args.provisional_handoff_label,
            )
        except Exception as exc:
            raise PreflightError(f"release generation marker is invalid: {exc}") from exc
        print("Lab runtime preflight: verified complete release generation")
        return 0
    except PreflightError as exc:
        print(f"Lab runtime preflight failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
