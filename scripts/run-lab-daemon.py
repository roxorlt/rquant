#!/usr/bin/env python3
"""Run the Lab runtime preflight before executing a daemon console script."""

from __future__ import annotations

import argparse
import fcntl
import importlib.util
import os
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

sys.dont_write_bytecode = True

FORMAL_RUNTIME_WRAPPER_CONTRACT = "rquant-formal-runtime-wrapper/v1"

_ALLOWED_DAEMONS = frozenset(
    {
        "lab-scheduler",
        "lab-worker",
        "lab-finalizer",
        "lab-claim-finalizer",
        "lab-runtime-prepare",
    }
)
_HANDOFF_LABELS = {
    "lab-scheduler": "com.roxor.rquant-lab-scheduler",
    "lab-worker": "com.roxor.rquant-lab-worker",
    "lab-finalizer": "com.roxor.rquant-lab-finalizer",
    "lab-claim-finalizer": "com.roxor.rquant-lab-claim-finalizer",
}
_PYTHON_INJECTION_VARIABLES = (
    "PYTHONHOME",
    "PYTHONINSPECT",
    "PYTHONPATH",
    "PYTHONSTARTUP",
)
_LAB_HIGHWATER_ENVIRONMENT_PREFIX = "LAB_HIGHWATER_"


class WrapperError(RuntimeError):
    pass


def _production_daemon_environment(
    daemon_command: str,
    *,
    environ: Mapping[str, str],
) -> dict[str, str]:
    """Return the environment allowed across the production scheduler boundary."""

    controlled = dict(environ)
    if daemon_command != "lab-scheduler":
        return controlled
    poisoned_names = tuple(
        sorted(name for name in controlled if name.startswith(_LAB_HIGHWATER_ENVIRONMENT_PREFIX))
    )
    if poisoned_names:
        raise WrapperError(
            "Lab scheduler environment contains forbidden high-water overrides: "
            + ", ".join(poisoned_names)
        )
    app_env = controlled.get("APP_ENV")
    if app_env not in (None, "prod"):
        raise WrapperError("Lab scheduler APP_ENV downgrade is not allowed")
    disable_dotenv = controlled.get("RQUANT_DISABLE_DOTENV")
    if disable_dotenv not in (None, "1"):
        raise WrapperError("Lab scheduler dotenv policy downgrade is not allowed")
    controlled["APP_ENV"] = "prod"
    controlled["RQUANT_DISABLE_DOTENV"] = "1"
    return controlled


def _load_contained_runner() -> Callable[..., subprocess.CompletedProcess[object]]:
    path = Path(__file__).resolve().parents[1] / "src" / "rquant" / "contained_subprocess.py"
    spec = importlib.util.spec_from_file_location("_rquant_wrapper_contained_process", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("contained subprocess authority cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.run_contained


run_contained = _load_contained_runner()


@dataclass(frozen=True)
class _PathIdentity:
    device: int
    inode: int
    mode: int
    owner: int
    links: int

    @classmethod
    def capture(cls, observed: os.stat_result) -> _PathIdentity:
        return cls(
            device=observed.st_dev,
            inode=observed.st_ino,
            mode=observed.st_mode,
            owner=observed.st_uid,
            links=observed.st_nlink,
        )


def _canonical_absolute(raw: str | Path, *, label: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise WrapperError(f"{label} must be an absolute canonical path")
    return path


def _require_owned_regular(
    path: Path,
    *,
    label: str,
    executable: bool = False,
) -> _PathIdentity:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise WrapperError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_nlink != 1
        or observed.st_mode & 0o022
    ):
        raise WrapperError(f"{label} must be an owned physical regular file")
    if executable and not observed.st_mode & stat.S_IXUSR:
        raise WrapperError(f"{label} must be owner-executable")
    return _PathIdentity.capture(observed)


def _require_verified_python_link(
    path: Path,
    *,
    expected_identity: object,
) -> _PathIdentity:
    try:
        resolved = path.resolve(strict=True)
        observed = resolved.lstat()
    except OSError as exc:
        raise WrapperError("selected release Python is unavailable") from exc
    identity = _PathIdentity.capture(observed)
    marker_identity = _PathIdentity(
        device=int(expected_identity.device),
        inode=int(expected_identity.inode),
        mode=int(expected_identity.mode),
        owner=int(expected_identity.owner),
        links=int(expected_identity.links),
    )
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_nlink != 1
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
        or identity != marker_identity
    ):
        raise WrapperError("selected release Python target is unsafe")
    return identity


def _require_owned_directory(path: Path, *, label: str) -> _PathIdentity:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise WrapperError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_mode & 0o022
        or path.resolve(strict=True) != path
    ):
        raise WrapperError(f"{label} must be an owned physical directory")
    return _PathIdentity.capture(observed)


def _require_trusted_git(raw_path: str | Path) -> tuple[Path, _PathIdentity]:
    path = _canonical_absolute(raw_path, label="trusted Git executable")
    try:
        if path.resolve(strict=True) != path:
            raise WrapperError("trusted Git executable must be physical")
        for parent in path.parents:
            observed_parent = parent.lstat()
            if (
                not stat.S_ISDIR(observed_parent.st_mode)
                or stat.S_ISLNK(observed_parent.st_mode)
                or observed_parent.st_uid != 0
                or observed_parent.st_mode & 0o022
            ):
                raise WrapperError("trusted Git parent path is unsafe")
        observed = path.lstat()
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            opened = os.fstat(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise WrapperError("trusted Git executable is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != 0
        or observed.st_mode & 0o022
        or not observed.st_mode & stat.S_IXUSR
        or _PathIdentity.capture(observed) != _PathIdentity.capture(opened)
    ):
        raise WrapperError("trusted Git executable is unsafe")
    return path, _PathIdentity.capture(observed)


def _assert_trusted_git(path: Path, expected: _PathIdentity) -> None:
    rebound_path, rebound = _require_trusted_git(path)
    if rebound_path != path or rebound != expected:
        raise WrapperError("trusted Git executable identity changed")


def _require_runtime_root(
    raw_root: str,
) -> tuple[Path, Path, Path, tuple[_PathIdentity, ...]]:
    root = _canonical_absolute(raw_root, label="expected checkout root")
    try:
        root_resolved = root.resolve(strict=True)
        cwd = Path.cwd().resolve(strict=True)
        venv = root / ".venv"
    except OSError as exc:
        raise WrapperError("expected checkout runtime is unavailable") from exc
    if root_resolved != root:
        raise WrapperError("expected checkout root must be a physical directory")
    root_identity = _require_owned_directory(root, label="expected checkout root")
    if cwd != root:
        raise WrapperError("working directory does not match expected checkout root")
    venv_identity = _require_owned_directory(venv, label="expected checkout virtualenv")
    bin_directory = venv / "bin"
    bin_identity = _require_owned_directory(
        bin_directory,
        label="expected checkout virtualenv bin",
    )
    python = venv / "bin" / "python"
    if Path(sys.executable) != python:
        raise WrapperError("wrapper executable is not the expected virtualenv Python")
    config_identity = _require_owned_regular(
        venv / "pyvenv.cfg",
        label="virtualenv configuration",
    )
    return (
        root,
        venv,
        python,
        (
            root_identity,
            venv_identity,
            bin_identity,
            config_identity,
        ),
    )


def _validate_daemon_argv(
    root: Path,
    venv: Path,
    trusted_git: Path,
    daemon_argv: list[str],
) -> tuple[Path, _PathIdentity]:
    if len(daemon_argv) < 4:
        raise WrapperError("daemon command is incomplete")
    executable = _canonical_absolute(daemon_argv[0], label="daemon executable")
    expected_executable = venv / "bin" / "rquant"
    if executable != expected_executable:
        raise WrapperError("daemon executable does not match expected checkout runtime")
    executable_identity = _require_owned_regular(
        executable,
        label="daemon executable",
        executable=True,
    )
    if daemon_argv[1] not in _ALLOWED_DAEMONS:
        raise WrapperError("unsupported Lab daemon command")
    root_values = [
        daemon_argv[index + 1]
        for index, value in enumerate(daemon_argv[:-1])
        if value == "--expected-checkout-root"
    ]
    if root_values != [str(root)]:
        raise WrapperError("daemon expected checkout root does not match wrapper binding")
    git_values = [
        daemon_argv[index + 1]
        for index, value in enumerate(daemon_argv[:-1])
        if value == "--trusted-git-path"
    ]
    if git_values != [str(trusted_git)]:
        raise WrapperError("daemon trusted Git path does not match wrapper binding")
    for forbidden in (
        "--deployment-generation",
        "--deployment-generation-fd",
        "--deployment-lock-path",
        "--startup-deadline-monotonic",
    ):
        if forbidden in daemon_argv:
            raise WrapperError("deployment generation arguments are wrapper-controlled")
    return executable, executable_identity


def _bind_exact_option(
    argv: list[str],
    *,
    option: str,
    expected: str,
    label: str,
) -> None:
    values = [argv[index + 1] for index, value in enumerate(argv[:-1]) if value == option]
    if not values:
        argv.extend([option, expected])
        return
    if values != [expected]:
        raise WrapperError(f"{label} does not match wrapper-controlled context")


def _replace_bound_option(argv: list[str], *, option: str, value: str) -> list[str]:
    """Rebind one already-validated option while preserving daemon argument order."""

    rebound = list(argv)
    indexes = [index for index, candidate in enumerate(rebound[:-1]) if candidate == option]
    if len(indexes) != 1:
        raise WrapperError(f"{option} is not uniquely bound")
    rebound[indexes[0] + 1] = value
    return rebound


def _bind_controlled_daemon_arguments(
    root: Path,
    trusted_git: Path,
    daemon_argv: list[str],
    *,
    environ: Mapping[str, str],
) -> list[str]:
    """Bind daemon arguments only from the verified checkout and controlled environment."""

    if len(daemon_argv) < 2 or daemon_argv[1] not in _ALLOWED_DAEMONS:
        raise WrapperError("formal Lab daemon command is missing or invalid")
    bound = list(daemon_argv)
    _bind_exact_option(
        bound,
        option="--expected-checkout-root",
        expected=str(root),
        label="daemon checkout root",
    )
    _bind_exact_option(
        bound,
        option="--trusted-git-path",
        expected=str(trusted_git),
        label="daemon trusted Git path",
    )
    if daemon_argv[1] in {"lab-runtime-prepare", "lab-scheduler"}:
        raw_runtime_root = environ.get("RQUANT_RUNTIME_ROOT", "")
        if (
            not raw_runtime_root
            or raw_runtime_root.strip() != raw_runtime_root
            or "\x00" in raw_runtime_root
            or "\n" in raw_runtime_root
            or "\r" in raw_runtime_root
        ):
            raise WrapperError(
                "RQUANT_RUNTIME_ROOT must name the controlled runtime deployment profile root"
            )
        runtime_root = _canonical_absolute(
            raw_runtime_root,
            label="controlled runtime deployment root",
        )
        _require_owned_directory(runtime_root, label="controlled runtime deployment root")
        _bind_exact_option(
            bound,
            option="--runtime-deployment-root",
            expected=str(runtime_root),
            label="runtime deployment root",
        )
    return bound


def _expected_deployment_lock(root: Path) -> Path:
    return root.parent / ".rquant-deploy" / f"{root.name}.lock"


def _acquire_deployment_generation(root: Path, raw_path: str) -> tuple[Path, int]:
    path = _canonical_absolute(raw_path, label="deployment generation lock")
    if path != _expected_deployment_lock(root):
        raise WrapperError("deployment generation lock does not match checkout binding")
    parent = path.parent
    try:
        with suppress(FileExistsError):
            os.mkdir(parent, 0o700)
        parent_stat = parent.lstat()
        if (
            not stat.S_ISDIR(parent_stat.st_mode)
            or stat.S_ISLNK(parent_stat.st_mode)
            or parent_stat.st_uid != os.getuid()
            or stat.S_IMODE(parent_stat.st_mode) != 0o700
            or parent.resolve(strict=True) != parent
        ):
            raise WrapperError("deployment generation lock root is unsafe")
        parent_fd = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(path.name, flags, 0o600, dir_fd=parent_fd)
            opened = os.fstat(descriptor)
            active = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        finally:
            os.close(parent_fd)
        if (
            _PathIdentity.capture(opened) != _PathIdentity.capture(active)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            os.close(descriptor)
            raise WrapperError("deployment generation lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise WrapperError("deployment generation is currently being updated") from exc
        os.set_inheritable(descriptor, True)
        return path, descriptor
    except OSError as exc:
        raise WrapperError("deployment generation lock is unavailable") from exc


def _acquire_immutable_generation(code_root: Path, raw_path: str) -> tuple[Path, int]:
    path = _canonical_absolute(raw_path, label="deployment generation lock")
    expected_environment_root = path.with_name(f"{path.stem}.venvs")
    if code_root.name != "release" or code_root.parent.parent != expected_environment_root:
        raise WrapperError("immutable code root does not match deployment generation storage")
    try:
        parent = path.parent
        _require_owned_directory(parent, label="deployment authority root")
        parent_fd = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            descriptor = os.open(
                path.name,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            opened = os.fstat(descriptor)
            active = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        finally:
            os.close(parent_fd)
        if (
            _PathIdentity.capture(opened) != _PathIdentity.capture(active)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            os.close(descriptor)
            raise WrapperError("deployment generation lock is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        os.set_inheritable(descriptor, True)
        return path, descriptor
    except BlockingIOError as exc:
        raise WrapperError("deployment generation is currently being updated") from exc
    except OSError as exc:
        raise WrapperError("deployment generation lock is unavailable") from exc


def _git_commit(
    root: Path,
    *,
    git_path: Path,
    git_identity: _PathIdentity,
    deadline_monotonic: float,
) -> str:
    _assert_trusted_git(git_path, git_identity)
    if time.monotonic() >= deadline_monotonic:
        raise subprocess.TimeoutExpired([str(git_path), "rev-parse", "HEAD^{commit}"], 0)
    try:
        result = run_contained(
            [str(git_path), "rev-parse", "--verify", "HEAD^{commit}"],
            cwd=root,
            deadline_monotonic=deadline_monotonic,
            check=True,
            text=True,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
            may_spawn_background_descendants=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise WrapperError("checkout commit cannot be verified") from exc
    _assert_trusted_git(git_path, git_identity)
    commit = result.stdout.strip()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise WrapperError("checkout commit is not a full lowercase SHA")
    return commit


def _run_preflight(
    *,
    python: Path,
    preflight: Path,
    root: Path,
    expected_commit: str,
    git_path: Path,
    git_identity: _PathIdentity,
    deployment_lock_path: Path,
    deployment_lock_fd: int,
    handoff_label: str | None = None,
    daemon_command: str,
    immutable_generation: bool = False,
    release_managed_checkout: bool = False,
    deadline_monotonic: float,
) -> None:
    _assert_trusted_git(git_path, git_identity)
    command = [
        str(python),
        "-I",
        "-S",
        str(preflight),
        "--checkout-root",
        str(root),
        "--expected-commit",
        expected_commit,
        "--trusted-git-path",
        str(git_path),
        "--deployment-lock-path",
        str(deployment_lock_path),
        "--deployment-lock-fd",
        str(deployment_lock_fd),
        "--python-path",
        str(python),
        "--lab-daemon-command",
        daemon_command,
    ]
    if handoff_label is not None:
        command.extend(["--provisional-handoff-label", handoff_label])
    if immutable_generation:
        command.append("--immutable-generation")
    if release_managed_checkout:
        command.append("--release-managed-checkout")
    result = run_contained(
        command,
        cwd=root,
        deadline_monotonic=deadline_monotonic,
        check=False,
        pass_fds=(deployment_lock_fd,),
        may_spawn_background_descendants=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[:2048]
        raise WrapperError(detail or "Lab runtime preflight failed")
    if result.stdout:
        sys.stdout.write(result.stdout)
        sys.stdout.flush()
    if result.stderr:
        sys.stderr.write(result.stderr)
        sys.stderr.flush()
    _assert_trusted_git(git_path, git_identity)


def _run_prepared_sentinel_preflight(
    *,
    python: Path,
    preflight: Path,
    root: Path,
    daemon_command: str,
    immutable_generation: bool = False,
    release_managed_checkout: bool = False,
    deadline_monotonic: float,
) -> None:
    command = [
        str(python),
        "-I",
        "-S",
        str(preflight),
        "--checkout-root",
        str(root),
        "--lab-daemon-command",
        daemon_command,
        "--prepared-sentinel-only",
    ]
    if immutable_generation:
        command.append("--immutable-generation")
    if release_managed_checkout:
        command.append("--release-managed-checkout")
    result = run_contained(
        command,
        cwd=root,
        deadline_monotonic=deadline_monotonic,
        check=False,
        may_spawn_background_descendants=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[:2048]
        raise WrapperError(detail or "Lab runtime prepared sentinel preflight failed")


def _immutable_generation_main(
    args: argparse.Namespace,
    daemon_argv: list[str],
    *,
    startup_deadline: float,
) -> int:
    code_root = _canonical_absolute(args.expected_code_root, label="immutable code root")
    _require_owned_directory(code_root, label="immutable code root")
    generation = code_root.parent
    _require_owned_directory(generation, label="selected release environment")
    python = generation / "bin" / "python"
    launcher = generation / "bin" / "rquant"
    if Path(sys.executable) != python:
        raise WrapperError("wrapper is not running with the selected release Python")
    if Path(__file__) != code_root / "scripts" / "run-lab-daemon.py":
        raise WrapperError("wrapper is outside the immutable release generation")
    if len(daemon_argv) < 2 or Path(daemon_argv[0]) != launcher:
        raise WrapperError("daemon launcher is outside the immutable release generation")
    if daemon_argv[1] not in _ALLOWED_DAEMONS:
        raise WrapperError("formal Lab daemon command is missing or invalid")
    _run_prepared_sentinel_preflight(
        python=python,
        preflight=code_root / "scripts" / "preflight-lab-runtime.py",
        root=code_root,
        daemon_command=daemon_argv[1],
        immutable_generation=True,
        deadline_monotonic=startup_deadline,
    )
    lock_path, lock_fd = _acquire_immutable_generation(code_root, args.deployment_lock_path)
    trusted_git, trusted_git_identity = _require_trusted_git(args.trusted_git_path)
    daemon_argv = _bind_controlled_daemon_arguments(
        code_root,
        trusted_git,
        daemon_argv,
        environ=os.environ,
    )
    expected_commit = args.expected_commit
    if len(expected_commit) != 40 or any(c not in "0123456789abcdef" for c in expected_commit):
        raise WrapperError("immutable generation commit is invalid")
    handoff_label = _HANDOFF_LABELS.get(daemon_argv[1])
    release_module = _load_release_authority(code_root / "src" / "rquant" / "release_generation.py")
    marker = release_module.ReleaseGenerationAuthority(
        repo=code_root,
        immutable_code_root=code_root,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=trusted_git,
        overall_deadline_monotonic=startup_deadline,
    ).verify(expected_commit=expected_commit, provisional_handoff_label=handoff_label)
    if Path(marker.venv_path) != generation or Path(marker.python_path) != python:
        raise WrapperError("selected immutable release generation changed")
    os.set_inheritable(lock_fd, True)
    os.execv(
        python,
        [
            str(python),
            "-I",
            "-S",
            str(code_root / "scripts" / "bootstrap-lab-daemon.py"),
            "--expected-code-root",
            str(code_root),
            "--expected-commit",
            expected_commit,
            "--expected-runtime-root",
            str(generation),
            "--trusted-git-path",
            str(trusted_git),
            "--deployment-lock-path",
            str(lock_path),
            "--deployment-lock-fd",
            str(lock_fd),
            "--expected-launcher",
            str(launcher),
            "--startup-deadline-monotonic",
            str(startup_deadline),
            *(["--provisional-handoff-label", handoff_label] if handoff_label is not None else []),
            "--",
            *daemon_argv[1:],
            "--deployment-generation",
            expected_commit,
            "--deployment-lock-path",
            str(lock_path),
            "--deployment-generation-fd",
            str(lock_fd),
            "--deployment-operation-id",
            marker.operation_id,
            "--deployment-environment-generation",
            marker.environment_generation_id,
        ],
    )
    return 1


def _load_release_authority(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("_rquant_release_generation", path)
    if spec is None or spec.loader is None:
        raise WrapperError("release generation authority cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _selected_release_runtime(
    marker: object,
    *,
    deployment_lock_path: Path,
) -> tuple[Path, Path, Path, tuple[_PathIdentity, ...]]:
    venv = _canonical_absolute(marker.venv_path, label="selected release environment")
    expected_root = deployment_lock_path.with_name(f"{deployment_lock_path.stem}.venvs")
    if venv.parent != expected_root:
        raise WrapperError("selected release environment is outside generation storage")
    python = _canonical_absolute(marker.python_path, label="selected release Python")
    launcher = venv / "bin" / "rquant"
    identities = (
        _require_owned_directory(venv, label="selected release environment"),
        _require_owned_directory(venv / "bin", label="selected release bin"),
        _require_verified_python_link(
            python,
            expected_identity=marker.python_identity,
        ),
        _require_owned_regular(launcher, label="selected release launcher", executable=True),
    )
    if python != venv / "bin" / "python":
        raise WrapperError("selected release Python does not match generation binding")
    return venv, python, launcher, identities


def _acquire_formal_deployment_lock(path: Path) -> int:
    path = _canonical_absolute(path, label="formal deployment lock")
    parent = path.parent
    try:
        parent_identity = _require_owned_directory(parent, label="formal deployment lock root")
        parent_fd = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            descriptor = os.open(
                path.name,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=parent_fd,
            )
            opened = os.fstat(descriptor)
            active = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        finally:
            os.close(parent_fd)
        if (
            _require_owned_directory(parent, label="formal deployment lock root") != parent_identity
            or _PathIdentity.capture(opened) != _PathIdentity.capture(active)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            os.close(descriptor)
            raise WrapperError("formal deployment lock is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        os.set_inheritable(descriptor, True)
        return descriptor
    except BlockingIOError as exc:
        raise WrapperError("formal deployment generation is being updated") from exc
    except OSError as exc:
        raise WrapperError("formal deployment lock is unavailable") from exc


def _formal_main(argv: list[str]) -> int:
    formal_runtime = importlib.import_module("rquant.formal_runtime")
    formal_command = importlib.import_module("rquant.formal_runtime_command")
    formal_composition = importlib.import_module("rquant.formal_runtime_composition")
    formal_runtime_error = formal_runtime.FormalRuntimeError
    bind_formal_runtime = formal_runtime.bind_formal_runtime
    exec_formal_runtime = formal_runtime.exec_formal_runtime
    formal_runtime_command_error = formal_command.FormalRuntimeCommandError
    compose_formal_daemon_argv = formal_command.compose_formal_daemon_argv
    parse_formal_wrapper_argv = formal_command.parse_formal_wrapper_argv
    formal_runtime_composition_error = formal_composition.FormalRuntimeCompositionError
    open_formal_runtime_capability = formal_composition.open_formal_runtime_capability

    startup_deadline = time.monotonic() + 60.0
    lock_fd = -1
    capability = None
    try:
        binding = parse_formal_wrapper_argv(argv)
        controlled_environment = _production_daemon_environment(
            binding.command,
            environ=os.environ,
        )
        os.environ.clear()
        os.environ.update(controlled_environment)
        lock_fd = _acquire_formal_deployment_lock(binding.deployment_lock_path)
        capability = open_formal_runtime_capability(
            configuration_path=binding.bootstrap.configuration_path,
            trusted_base=binding.bootstrap.trusted_base,
            expected_authority_uid=binding.bootstrap.authority_uid,
            expected_authority_gid=binding.bootstrap.authority_gid,
            startup_deadline_monotonic=startup_deadline,
        )
        daemon_argv = compose_formal_daemon_argv(
            binding,
            deployment_generation=capability.evidence.provenance_commit,
            deployment_generation_fd=lock_fd,
            startup_deadline_monotonic=startup_deadline,
        )
        session = bind_formal_runtime(
            capability,
            daemon_argv=daemon_argv,
            environment_source=os.environ,
            expected_python_abi=capability.loaded.attestation.execution_spec.python_abi,
        )
        capability = None
        sys.stdout.flush()
        sys.stderr.flush()
        exec_formal_runtime(session)
    except (
        formal_runtime_command_error,
        formal_runtime_composition_error,
        formal_runtime_error,
        OSError,
        WrapperError,
    ) as exc:
        print(f"Lab formal daemon wrapper failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if capability is not None:
            capability.close()
        if lock_fd >= 0:
            os.close(lock_fd)
    return 1


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    if raw_argv[:1] == ["formal"]:
        return _formal_main(raw_argv[1:])
    startup_deadline = time.monotonic() + 60
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-checkout-root", required=True)
    parser.add_argument("--expected-code-root")
    parser.add_argument("--expected-commit")
    parser.add_argument("--trusted-git-path", required=True)
    parser.add_argument("--deployment-lock-path", required=True)
    parser.add_argument("daemon_argv", nargs=argparse.REMAINDER)
    args = parser.parse_args(raw_argv)
    generation_lock_fd = -1
    try:
        daemon_argv = list(args.daemon_argv)
        if daemon_argv and daemon_argv[0] == "--":
            daemon_argv.pop(0)
        daemon_command = daemon_argv[1] if len(daemon_argv) >= 2 else ""
        controlled_environment = _production_daemon_environment(
            daemon_command,
            environ=os.environ,
        )
        os.environ.clear()
        os.environ.update(controlled_environment)
        if args.expected_code_root is not None:
            if args.expected_commit is None:
                raise WrapperError("immutable generation requires an exact commit")
            return _immutable_generation_main(
                args,
                daemon_argv,
                startup_deadline=startup_deadline,
            )
        root, venv, python, runtime_identities = _require_runtime_root(args.expected_checkout_root)
        trusted_git, trusted_git_identity = _require_trusted_git(args.trusted_git_path)
        daemon_argv = _bind_controlled_daemon_arguments(
            root,
            trusted_git,
            daemon_argv,
            environ=os.environ,
        )
        for variable in _PYTHON_INJECTION_VARIABLES:
            if os.environ.get(variable):
                raise WrapperError(f"Python environment injection is not allowed: {variable}")
        wrapper = root / "scripts" / "run-lab-daemon.py"
        preflight = root / "scripts" / "preflight-lab-runtime.py"
        bootstrap = root / "scripts" / "bootstrap-lab-daemon.py"
        release_authority_path = root / "src" / "rquant" / "release_generation.py"
        if Path(__file__) != wrapper:
            raise WrapperError("wrapper path does not match expected checkout")
        wrapper_identity = _require_owned_regular(wrapper, label="Lab runtime wrapper")
        preflight_identity = _require_owned_regular(preflight, label="Lab runtime preflight")
        bootstrap_identity = _require_owned_regular(bootstrap, label="Lab daemon bootstrap")
        release_authority_identity = _require_owned_regular(
            release_authority_path,
            label="release generation authority",
        )
        executable, executable_identity = _validate_daemon_argv(
            root,
            venv,
            trusted_git,
            daemon_argv,
        )
        _run_prepared_sentinel_preflight(
            python=python,
            preflight=preflight,
            root=root,
            daemon_command=daemon_argv[1],
            release_managed_checkout=True,
            deadline_monotonic=startup_deadline,
        )
        deployment_lock_path, generation_lock_fd = _acquire_deployment_generation(
            root,
            args.deployment_lock_path,
        )
        handoff_label = _HANDOFF_LABELS.get(daemon_argv[1])
        expected_commit = _git_commit(
            root,
            git_path=trusted_git,
            git_identity=trusted_git_identity,
            deadline_monotonic=startup_deadline,
        )
        _run_preflight(
            python=python,
            preflight=preflight,
            root=root,
            expected_commit=expected_commit,
            git_path=trusted_git,
            git_identity=trusted_git_identity,
            deployment_lock_path=deployment_lock_path,
            deployment_lock_fd=generation_lock_fd,
            handoff_label=handoff_label,
            daemon_command=daemon_argv[1],
            release_managed_checkout=True,
            deadline_monotonic=startup_deadline,
        )
        try:
            release_module = _load_release_authority(release_authority_path)
            release_marker = release_module.ReleaseGenerationAuthority(
                repo=root,
                lock_path=deployment_lock_path,
                lock_fd=generation_lock_fd,
                python_path=python,
                git_path=trusted_git,
                overall_deadline_monotonic=startup_deadline,
            ).verify(
                expected_commit=expected_commit,
                provisional_handoff_label=handoff_label,
            )
        except Exception as exc:
            raise WrapperError(f"release generation marker is invalid: {exc}") from exc
        selected_venv, selected_python, selected_launcher, selected_identities = (
            _selected_release_runtime(
                release_marker,
                deployment_lock_path=deployment_lock_path,
            )
        )
        rebound_root, rebound_venv, rebound_python, rebound_runtime_identities = (
            _require_runtime_root(args.expected_checkout_root)
        )
        rebound_executable, rebound_executable_identity = _validate_daemon_argv(
            rebound_root,
            rebound_venv,
            trusted_git,
            daemon_argv,
        )
        if (
            (rebound_root, rebound_venv, rebound_python) != (root, venv, python)
            or rebound_runtime_identities != runtime_identities
            or rebound_executable != executable
            or rebound_executable_identity != executable_identity
            or _require_owned_regular(wrapper, label="Lab runtime wrapper") != wrapper_identity
            or _require_owned_regular(preflight, label="Lab runtime preflight")
            != preflight_identity
            or _require_owned_regular(bootstrap, label="Lab daemon bootstrap") != bootstrap_identity
            or _require_owned_regular(
                release_authority_path,
                label="release generation authority",
            )
            != release_authority_identity
            or _git_commit(
                root,
                git_path=trusted_git,
                git_identity=trusted_git_identity,
                deadline_monotonic=startup_deadline,
            )
            != expected_commit
        ):
            raise WrapperError("Lab runtime identity changed during preflight")
        final_root, final_venv, final_python, final_runtime_identities = _require_runtime_root(
            args.expected_checkout_root
        )
        final_executable, final_executable_identity = _validate_daemon_argv(
            final_root,
            final_venv,
            trusted_git,
            daemon_argv,
        )
        if (
            (final_root, final_venv, final_python) != (root, venv, python)
            or final_runtime_identities != runtime_identities
            or final_executable != executable
            or final_executable_identity != executable_identity
            or _require_owned_regular(wrapper, label="Lab runtime wrapper") != wrapper_identity
            or _require_owned_regular(preflight, label="Lab runtime preflight")
            != preflight_identity
            or _require_owned_regular(bootstrap, label="Lab daemon bootstrap") != bootstrap_identity
            or _require_owned_regular(
                release_authority_path,
                label="release generation authority",
            )
            != release_authority_identity
            or _git_commit(
                root,
                git_path=trusted_git,
                git_identity=trusted_git_identity,
                deadline_monotonic=startup_deadline,
            )
            != expected_commit
        ):
            raise WrapperError("Lab runtime identity changed before daemon exec")
        rebound_executable, rebound_executable_identity = _validate_daemon_argv(
            root,
            venv,
            trusted_git,
            daemon_argv,
        )
        if (
            rebound_executable != executable
            or rebound_executable_identity != executable_identity
            or _require_runtime_root(args.expected_checkout_root)[3] != runtime_identities
            or _require_owned_regular(wrapper, label="Lab runtime wrapper") != wrapper_identity
            or _require_owned_regular(preflight, label="Lab runtime preflight")
            != preflight_identity
            or _require_owned_regular(bootstrap, label="Lab daemon bootstrap") != bootstrap_identity
            or _require_owned_regular(
                release_authority_path,
                label="release generation authority",
            )
            != release_authority_identity
            or _git_commit(
                root,
                git_path=trusted_git,
                git_identity=trusted_git_identity,
                deadline_monotonic=startup_deadline,
            )
            != expected_commit
        ):
            raise WrapperError("Lab runtime identity changed at daemon exec boundary")
        final_marker = release_module.ReleaseGenerationAuthority(
            repo=root,
            lock_path=deployment_lock_path,
            lock_fd=generation_lock_fd,
            python_path=selected_python,
            git_path=trusted_git,
            overall_deadline_monotonic=startup_deadline,
        ).verify(
            expected_commit=expected_commit,
            provisional_handoff_label=handoff_label,
        )
        (
            final_selected_venv,
            final_selected_python,
            final_selected_launcher,
            final_selected_identities,
        ) = _selected_release_runtime(
            final_marker,
            deployment_lock_path=deployment_lock_path,
        )
        if (
            final_marker != release_marker
            or (final_selected_venv, final_selected_python, final_selected_launcher)
            != (selected_venv, selected_python, selected_launcher)
            or final_selected_identities != selected_identities
        ):
            raise WrapperError("selected release environment changed before daemon exec")
        os.environ.pop("__PYVENV_LAUNCHER__", None)
        sys.stdout.flush()
        sys.stderr.flush()
        immutable_code_root = release_module.generation_code_root(selected_venv)
        immutable_wrapper = immutable_code_root / "scripts" / "run-lab-daemon.py"
        _require_owned_directory(immutable_code_root, label="immutable code root")
        _require_owned_regular(immutable_wrapper, label="immutable Lab runtime wrapper")
        immutable_daemon_argv = _replace_bound_option(
            daemon_argv,
            option="--expected-checkout-root",
            value=str(immutable_code_root),
        )
        immutable_daemon_argv = _replace_bound_option(
            immutable_daemon_argv,
            option="--trusted-git-path",
            value=str(trusted_git),
        )
        immutable_daemon_argv[0] = str(selected_launcher)
        os.chdir(immutable_code_root)
        os.execv(
            selected_python,
            [
                str(selected_python),
                "-I",
                "-S",
                str(immutable_wrapper),
                "--expected-checkout-root",
                str(root),
                "--expected-code-root",
                str(immutable_code_root),
                "--expected-commit",
                expected_commit,
                "--trusted-git-path",
                str(trusted_git),
                "--deployment-lock-path",
                str(deployment_lock_path),
                "--",
                *immutable_daemon_argv,
            ],
        )
    except (OSError, subprocess.SubprocessError, WrapperError) as exc:
        print(f"Lab daemon wrapper failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if generation_lock_fd >= 0:
            os.close(generation_lock_fd)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
