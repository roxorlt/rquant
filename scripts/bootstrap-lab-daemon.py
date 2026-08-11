#!/usr/bin/env python3
"""Import and dispatch a Lab daemon without processing Python site hooks."""

from __future__ import annotations

import argparse
import fcntl
import importlib.util
import math
import os
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

sys.dont_write_bytecode = True


def _load_contained_runner() -> object:
    path = Path(__file__).resolve().parents[1] / "src" / "rquant" / "contained_subprocess.py"
    spec = importlib.util.spec_from_file_location("_rquant_lab_bootstrap_containment", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("contained subprocess implementation cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.run_contained


run_contained = _load_contained_runner()
DAILY_RECEIPT_AUTHORITY_ENV_PREFIXES = (
    "RQUANT_DAILY_RECEIPT_",
    "RQ_DAILY_SHADOW_RECEIPT_",
)


class BootstrapError(RuntimeError):
    pass


def _reject_daily_receipt_environment() -> None:
    for key in os.environ:
        if key.startswith(DAILY_RECEIPT_AUTHORITY_ENV_PREFIXES):
            raise BootstrapError("Daily receipt authority environment is not allowed")


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


def _run_preflight(
    *,
    root: Path,
    commit: str,
    git_path: Path,
    preflight: Path,
    lock_path: Path,
    lock_fd: int,
    python_path: Path,
    provisional_handoff_label: str | None,
    daemon_command: str,
    immutable_generation: bool = False,
    deadline_monotonic: float,
) -> None:
    command = [
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
        "--deployment-lock-path",
        str(lock_path),
        "--deployment-lock-fd",
        str(lock_fd),
        "--python-path",
        str(python_path),
        "--lab-daemon-command",
        daemon_command,
    ]
    if provisional_handoff_label is not None:
        command.extend(["--provisional-handoff-label", provisional_handoff_label])
    if immutable_generation:
        command.append("--immutable-generation")
    result = run_contained(
        command,
        cwd=root,
        check=False,
        deadline_monotonic=deadline_monotonic,
        pass_fds=(lock_fd,),
        may_spawn_background_descendants=False,
    )
    if result.returncode != 0:
        raise BootstrapError("Lab runtime preflight failed before import")


def _load_release_authority(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("_rquant_release_generation", path)
    if spec is None or spec.loader is None:
        raise BootstrapError("release generation authority cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-checkout-root")
    parser.add_argument("--expected-code-root")
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-runtime-root", required=True)
    parser.add_argument("--trusted-git-path", required=True)
    parser.add_argument("--deployment-lock-path", required=True)
    parser.add_argument("--deployment-lock-fd", required=True, type=int)
    parser.add_argument("--expected-launcher", required=True)
    parser.add_argument("--startup-deadline-monotonic", required=True, type=float)
    parser.add_argument("--provisional-handoff-label")
    parser.add_argument("daemon_argv", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    startup_deadline = args.startup_deadline_monotonic
    if not math.isfinite(startup_deadline) or time.monotonic() >= startup_deadline:
        parser.error("startup deadline is invalid or expired")
    try:
        _reject_daily_receipt_environment()
        daemon_argv = list(args.daemon_argv)
        if daemon_argv and daemon_argv[0] == "--":
            daemon_argv.pop(0)
        if not daemon_argv or daemon_argv[0] not in {
            "lab-runtime-prepare",
            "lab-scheduler",
            "lab-worker",
            "lab-finalizer",
        }:
            raise BootstrapError("formal Lab daemon command is missing or invalid")
        immutable_generation = args.expected_code_root is not None
        raw_root = args.expected_code_root if immutable_generation else args.expected_checkout_root
        if raw_root is None:
            raise BootstrapError("expected source root is required")
        root = _canonical(raw_root, label="expected source root")
        _physical_directory(root, label="expected source root")
        if Path.cwd().resolve(strict=True) != root:
            raise BootstrapError("working directory does not match expected source root")
        venv = _canonical(args.expected_runtime_root, label="expected runtime generation")
        expected_environment_root = Path(args.deployment_lock_path).with_name(
            f"{Path(args.deployment_lock_path).stem}.venvs"
        )
        if (
            venv.parent != expected_environment_root
            or Path(sys.executable) != venv / "bin" / "python"
        ):
            raise BootstrapError(
                "runtime generation does not match selected environment: "
                f"executable={sys.executable} selected={venv} root={expected_environment_root}"
            )
        _physical_directory(venv, label="expected runtime generation")
        _physical_directory(venv / "bin", label="expected runtime generation bin")
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
            lock_path=lock_path,
            lock_fd=args.deployment_lock_fd,
            python_path=Path(sys.executable),
            provisional_handoff_label=args.provisional_handoff_label,
            daemon_command=daemon_argv[0],
            immutable_generation=immutable_generation,
            deadline_monotonic=startup_deadline,
        )
        try:
            _load_release_authority(
                root / "src" / "rquant" / "release_generation.py"
            ).ReleaseGenerationAuthority(
                repo=root,
                immutable_code_root=(root if immutable_generation else None),
                lock_path=lock_path,
                lock_fd=args.deployment_lock_fd,
                python_path=Path(sys.executable),
                git_path=_canonical(args.trusted_git_path, label="trusted Git path"),
                overall_deadline_monotonic=startup_deadline,
            ).verify(
                expected_commit=args.expected_commit,
                provisional_handoff_label=args.provisional_handoff_label,
            )
        except Exception as exc:
            raise BootstrapError(f"release generation marker is invalid: {exc}") from exc
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
        sys.argv = [
            str(launcher),
            *daemon_argv,
            "--startup-deadline-monotonic",
            str(startup_deadline),
        ]

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
