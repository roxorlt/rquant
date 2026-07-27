#!/usr/bin/env python3
"""Run the Lab runtime preflight before executing a daemon console script."""

from __future__ import annotations

import argparse
import os
import stat
import subprocess
import sys
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


def _canonical_absolute(raw: str | Path, *, label: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise WrapperError(f"{label} must be an absolute canonical path")
    return path


def _require_owned_regular(path: Path, *, label: str, executable: bool = False) -> None:
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


def _require_runtime_root(raw_root: str) -> tuple[Path, Path, Path]:
    root = _canonical_absolute(raw_root, label="expected checkout root")
    try:
        root_resolved = root.resolve(strict=True)
        cwd = Path.cwd().resolve(strict=True)
        root_stat = root.lstat()
        venv = root / ".venv"
        venv_stat = venv.lstat()
    except OSError as exc:
        raise WrapperError("expected checkout runtime is unavailable") from exc
    if (
        root_resolved != root
        or not stat.S_ISDIR(root_stat.st_mode)
        or root_stat.st_uid != os.getuid()
        or root_stat.st_mode & 0o022
    ):
        raise WrapperError("expected checkout root must be a physical directory")
    if cwd != root:
        raise WrapperError("working directory does not match expected checkout root")
    if (
        not stat.S_ISDIR(venv_stat.st_mode)
        or stat.S_ISLNK(venv_stat.st_mode)
        or venv_stat.st_uid != os.getuid()
        or venv_stat.st_mode & 0o022
        or venv.resolve(strict=True) != venv
    ):
        raise WrapperError("expected checkout requires an owned physical virtualenv")
    python = venv / "bin" / "python"
    if Path(sys.executable) != python:
        raise WrapperError("wrapper executable is not the expected virtualenv Python")
    _require_owned_regular(venv / "pyvenv.cfg", label="virtualenv configuration")
    return root, venv, python


def _validate_daemon_argv(root: Path, venv: Path, daemon_argv: list[str]) -> Path:
    if len(daemon_argv) < 4:
        raise WrapperError("daemon command is incomplete")
    executable = _canonical_absolute(daemon_argv[0], label="daemon executable")
    expected_executable = venv / "bin" / "rquant"
    if executable != expected_executable:
        raise WrapperError("daemon executable does not match expected checkout runtime")
    _require_owned_regular(executable, label="daemon executable", executable=True)
    if daemon_argv[1] not in _ALLOWED_DAEMONS:
        raise WrapperError("unsupported Lab daemon command")
    root_values = [
        daemon_argv[index + 1]
        for index, value in enumerate(daemon_argv[:-1])
        if value == "--expected-checkout-root"
    ]
    if root_values != [str(root)]:
        raise WrapperError("daemon expected checkout root does not match wrapper binding")
    return executable


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-checkout-root", required=True)
    parser.add_argument("daemon_argv", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        root, venv, python = _require_runtime_root(args.expected_checkout_root)
        for variable in _PYTHON_INJECTION_VARIABLES:
            if os.environ.get(variable):
                raise WrapperError(f"Python environment injection is not allowed: {variable}")
        wrapper = root / "scripts" / "run-lab-daemon.py"
        preflight = root / "scripts" / "preflight-lab-runtime.py"
        if Path(__file__) != wrapper:
            raise WrapperError("wrapper path does not match expected checkout")
        _require_owned_regular(wrapper, label="Lab runtime wrapper")
        _require_owned_regular(preflight, label="Lab runtime preflight")
        daemon_argv = list(args.daemon_argv)
        if daemon_argv and daemon_argv[0] == "--":
            daemon_argv.pop(0)
        executable = _validate_daemon_argv(root, venv, daemon_argv)
        result = subprocess.run(
            [
                str(python),
                "-I",
                "-S",
                str(preflight),
                "--checkout-root",
                str(root),
            ],
            cwd=root,
            check=False,
            timeout=15,
        )
        if result.returncode != 0:
            raise WrapperError("Lab runtime preflight failed")
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
