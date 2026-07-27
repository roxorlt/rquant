#!/usr/bin/env python3
"""Import and dispatch a Lab daemon without processing Python site hooks."""

from __future__ import annotations

import argparse
import fcntl
import os
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


class BootstrapError(RuntimeError):
    pass


@dataclass(frozen=True)
class _Identity:
    device: int
    inode: int
    mode: int
    owner: int
    links: int

    @classmethod
    def capture(cls, value: os.stat_result) -> _Identity:
        return cls(value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_nlink)


def _canonical(raw: str, *, label: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise BootstrapError(f"{label} must be an absolute canonical path")
    return path


def _physical_directory(path: Path, *, label: str) -> _Identity:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise BootstrapError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_mode & 0o022
        or path.resolve(strict=True) != path
    ):
        raise BootstrapError(f"{label} must be an owned physical directory")
    return _Identity.capture(observed)


def _physical_file(path: Path, *, label: str, executable: bool = False) -> _Identity:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise BootstrapError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_nlink != 1
        or observed.st_mode & 0o022
        or (executable and not observed.st_mode & stat.S_IXUSR)
    ):
        raise BootstrapError(f"{label} must be an owned physical regular file")
    return _Identity.capture(observed)


def _assert_generation_lock(path: Path, descriptor: int) -> None:
    try:
        opened = os.fstat(descriptor)
        active = path.lstat()
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except (OSError, BlockingIOError) as exc:
        raise BootstrapError("deployment generation lock is unavailable") from exc
    if (
        _Identity.capture(opened) != _Identity.capture(active)
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.getuid()
        or opened.st_nlink != 1
        or stat.S_IMODE(opened.st_mode) != 0o600
    ):
        raise BootstrapError("deployment generation lock identity changed")


def _run_preflight(*, root: Path, commit: str, git_path: Path, preflight: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            str(preflight),
            "--checkout-root",
            str(root),
            "--expected-commit",
            commit,
            "--trusted-git-path",
            str(git_path),
        ],
        cwd=root,
        check=False,
        timeout=15,
    )
    if result.returncode != 0:
        raise BootstrapError("Lab runtime preflight failed before import")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-checkout-root", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--trusted-git-path", required=True)
    parser.add_argument("--deployment-lock-path", required=True)
    parser.add_argument("--deployment-lock-fd", required=True, type=int)
    parser.add_argument("--expected-launcher", required=True)
    parser.add_argument("daemon_argv", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        root = _canonical(args.expected_checkout_root, label="expected checkout root")
        _physical_directory(root, label="expected checkout root")
        if Path.cwd().resolve(strict=True) != root:
            raise BootstrapError("working directory does not match expected checkout root")
        venv = root / ".venv"
        _physical_directory(venv, label="expected virtualenv")
        _physical_directory(venv / "bin", label="expected virtualenv bin")
        src = root / "src"
        _physical_directory(src, label="project source root")
        launcher = _canonical(args.expected_launcher, label="expected daemon launcher")
        _physical_file(launcher, label="expected daemon launcher", executable=True)
        bootstrap = root / "scripts" / "bootstrap-lab-daemon.py"
        _physical_file(bootstrap, label="Lab daemon bootstrap")
        preflight = root / "scripts" / "preflight-lab-runtime.py"
        _physical_file(preflight, label="Lab runtime preflight")
        lock_path = _canonical(args.deployment_lock_path, label="deployment lock")
        _assert_generation_lock(lock_path, args.deployment_lock_fd)
        if len(args.expected_commit) != 40 or any(
            character not in "0123456789abcdef" for character in args.expected_commit
        ):
            raise BootstrapError("deployment generation is not a full lowercase SHA")
        _run_preflight(
            root=root,
            commit=args.expected_commit,
            git_path=_canonical(args.trusted_git_path, label="trusted Git path"),
            preflight=preflight,
        )
        _assert_generation_lock(lock_path, args.deployment_lock_fd)

        site_packages = (
            venv
            / "lib"
            / (f"python{sys.version_info.major}.{sys.version_info.minor}")
            / "site-packages"
        )
        _physical_directory(site_packages, label="verified site-packages generation")
        stdlib_paths = [
            entry
            for entry in sys.path
            if entry and Path(entry) not in {root, root / "scripts", src, site_packages}
        ]
        sys.path[:] = [str(src), str(site_packages), *stdlib_paths]
        sys.prefix = str(venv)
        sys.exec_prefix = str(venv)
        daemon_argv = list(args.daemon_argv)
        if daemon_argv and daemon_argv[0] == "--":
            daemon_argv.pop(0)
        if not daemon_argv:
            raise BootstrapError("daemon command is missing")
        sys.argv = [str(launcher), *daemon_argv]

        from rquant.cli import main as rquant_main

        module = sys.modules.get("rquant")
        package_file = Path(str(getattr(module, "__file__", ""))).resolve(strict=True)
        if not package_file.is_relative_to((src / "rquant").resolve(strict=True)):
            raise BootstrapError("rquant imported outside the verified source generation")
        _assert_generation_lock(lock_path, args.deployment_lock_fd)
        result = rquant_main()
        return 0 if result is None else int(result)
    except (BootstrapError, OSError, subprocess.SubprocessError) as exc:
        print(f"Lab daemon bootstrap failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
