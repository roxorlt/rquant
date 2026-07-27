#!/usr/bin/env python3
"""Run the Lab runtime preflight before executing a daemon console script."""

from __future__ import annotations

import argparse
import os
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

_ALLOWED_DAEMONS = frozenset({"lab-scheduler", "lab-worker", "lab-finalizer"})
_PYTHON_INJECTION_VARIABLES = (
    "PYTHONHOME",
    "PYTHONINSPECT",
    "PYTHONPATH",
    "PYTHONSTARTUP",
)


class WrapperError(RuntimeError):
    pass


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
    return executable, executable_identity


def _git_commit(root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD^{commit}"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise WrapperError("checkout commit cannot be verified") from exc
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
) -> None:
    result = subprocess.run(
        [
            str(python),
            "-I",
            "-S",
            str(preflight),
            "--checkout-root",
            str(root),
            "--expected-commit",
            expected_commit,
        ],
        cwd=root,
        check=False,
        timeout=15,
    )
    if result.returncode != 0:
        raise WrapperError("Lab runtime preflight failed")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-checkout-root", required=True)
    parser.add_argument("daemon_argv", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        root, venv, python, runtime_identities = _require_runtime_root(args.expected_checkout_root)
        for variable in _PYTHON_INJECTION_VARIABLES:
            if os.environ.get(variable):
                raise WrapperError(f"Python environment injection is not allowed: {variable}")
        wrapper = root / "scripts" / "run-lab-daemon.py"
        preflight = root / "scripts" / "preflight-lab-runtime.py"
        if Path(__file__) != wrapper:
            raise WrapperError("wrapper path does not match expected checkout")
        wrapper_identity = _require_owned_regular(wrapper, label="Lab runtime wrapper")
        preflight_identity = _require_owned_regular(preflight, label="Lab runtime preflight")
        daemon_argv = list(args.daemon_argv)
        if daemon_argv and daemon_argv[0] == "--":
            daemon_argv.pop(0)
        executable, executable_identity = _validate_daemon_argv(root, venv, daemon_argv)
        expected_commit = _git_commit(root)
        _run_preflight(
            python=python,
            preflight=preflight,
            root=root,
            expected_commit=expected_commit,
        )
        rebound_root, rebound_venv, rebound_python, rebound_runtime_identities = (
            _require_runtime_root(args.expected_checkout_root)
        )
        rebound_executable, rebound_executable_identity = _validate_daemon_argv(
            rebound_root,
            rebound_venv,
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
            or _git_commit(root) != expected_commit
        ):
            raise WrapperError("Lab runtime identity changed during preflight")
        _run_preflight(
            python=python,
            preflight=preflight,
            root=root,
            expected_commit=expected_commit,
        )
        final_root, final_venv, final_python, final_runtime_identities = _require_runtime_root(
            args.expected_checkout_root
        )
        final_executable, final_executable_identity = _validate_daemon_argv(
            final_root,
            final_venv,
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
            or _git_commit(root) != expected_commit
        ):
            raise WrapperError("Lab runtime identity changed before daemon exec")
        os.environ.pop("__PYVENV_LAUNCHER__", None)
        sys.stdout.flush()
        sys.stderr.flush()
        os.execv(executable, daemon_argv)
    except (OSError, subprocess.SubprocessError, WrapperError) as exc:
        print(f"Lab daemon wrapper failed: {exc}", file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
