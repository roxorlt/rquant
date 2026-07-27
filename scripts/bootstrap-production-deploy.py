#!/usr/bin/env python3
"""Acquire the release-generation lock before importing the project deployer."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import stat
import subprocess
import sys
import tomllib
from pathlib import Path
from types import ModuleType

sys.dont_write_bytecode = True


class DeployBootstrapError(RuntimeError):
    pass


TARGET_PATTERN = re.compile(r"(?:v\d+\.\d+\.\d+|[0-9a-f]{40})")


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


def _acquire_lock(root: Path, lock_path: Path) -> int:
    expected = root.parent / ".rquant-deploy" / f"{root.name}.lock"
    if lock_path != expected:
        raise DeployBootstrapError("deployment lock does not match checkout binding")
    try:
        lock_path.parent.mkdir(mode=0o700, exist_ok=True)
        _physical_directory(lock_path.parent, label="deployment authority root", private=True)
        descriptor = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
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
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.set_inheritable(descriptor, True)
        return descriptor
    except BlockingIOError as exc:
        raise DeployBootstrapError("another release generation is active") from exc
    except OSError as exc:
        raise DeployBootstrapError("deployment generation lock is unavailable") from exc


def _git_run(
    repo: Path,
    git_path: Path,
    *arguments: str,
    check: bool = True,
    text: bool = True,
) -> subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            [str(git_path), *arguments],
            cwd=repo,
            check=check,
            capture_output=True,
            text=text,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DeployBootstrapError("deployment checkout cannot be verified") from exc


def _git_output(repo: Path, git_path: Path, *arguments: str) -> str:
    result = _git_run(repo, git_path, *arguments)
    assert isinstance(result.stdout, str)
    return result.stdout.strip()


def _git_head(repo: Path, git_path: Path) -> str:
    return _git_output(repo, git_path, "rev-parse", "--verify", "HEAD^{commit}")


def _tracked_checkout_is_clean(repo: Path, git_path: Path) -> None:
    status = _git_output(repo, git_path, "status", "--porcelain=v1", "--untracked-files=no")
    diff = _git_run(
        repo,
        git_path,
        "diff-index",
        "--quiet",
        "HEAD",
        "--",
        check=False,
    )
    if status or diff.returncode != 0:
        raise DeployBootstrapError("tracked deployment checkout is dirty")


def _tracked_file_bytes(repo: Path, git_path: Path, commit: str, relative: str) -> bytes:
    result = _git_run(repo, git_path, "show", f"{commit}:{relative}", text=False)
    assert isinstance(result.stdout, bytes)
    return result.stdout


def _verify_generation_target(repo: Path, git_path: Path, target: str) -> str:
    if TARGET_PATTERN.fullmatch(target) is None:
        raise DeployBootstrapError("generation target must be a SemVer tag or full SHA")
    if _git_output(repo, git_path, "rev-parse", "--abbrev-ref", "HEAD") != "main":
        raise DeployBootstrapError("generation checkout must be on main")
    _tracked_checkout_is_clean(repo, git_path)
    commit = _git_output(repo, git_path, "rev-parse", "--verify", f"{target}^{{commit}}")
    allowed = _git_run(
        repo,
        git_path,
        "merge-base",
        "--is-ancestor",
        commit,
        "origin/main",
        check=False,
    )
    if allowed.returncode != 0:
        raise DeployBootstrapError("generation target is not contained in origin/main")
    if target.startswith("v") and _git_output(repo, git_path, "cat-file", "-t", target) != "tag":
        raise DeployBootstrapError("generation SemVer target must be an annotated tag")

    pyproject_payload = _tracked_file_bytes(repo, git_path, commit, "pyproject.toml")
    try:
        package_version = str(tomllib.loads(pyproject_payload.decode())["project"]["version"])
    except (UnicodeDecodeError, KeyError, tomllib.TOMLDecodeError) as exc:
        raise DeployBootstrapError("generation package version cannot be verified") from exc
    if target.startswith("v") and package_version != target[1:]:
        raise DeployBootstrapError("generation tag and package version disagree")
    return commit


def _verify_current_generation_checkout(repo: Path, git_path: Path, commit: str) -> None:
    if _git_head(repo, git_path) != commit:
        raise DeployBootstrapError("generation target does not match current HEAD")
    _tracked_checkout_is_clean(repo, git_path)
    for relative in ("uv.lock", "pyproject.toml"):
        path = repo / relative
        _physical_file(path, label=relative)
        try:
            working = path.read_bytes()
        except OSError as exc:
            raise DeployBootstrapError(f"{relative} cannot be read") from exc
        tracked = _tracked_file_bytes(repo, git_path, commit, relative)
        if hashlib.sha256(working).digest() != hashlib.sha256(tracked).digest():
            raise DeployBootstrapError(f"{relative} does not match generation target")


def _verify_generation_runtime(root: Path, python_path: Path) -> None:
    venv = root / ".venv"
    _physical_directory(venv, label="release venv")
    if not python_path.is_relative_to(venv):
        raise DeployBootstrapError("deployment Python is outside release venv")
    _physical_file(venv / "pyvenv.cfg", label="pyvenv.cfg")
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
            timeout=10,
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
    return str(parser.parse_args(values).target)


def _run_generation_preflight(root: Path) -> None:
    launcher = root / ".venv" / "bin" / "rquant"
    _physical_file(launcher, label="rquant preflight launcher", executable=True)
    try:
        result = subprocess.run(
            [str(launcher), "preflight"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DeployBootstrapError("generation preflight could not run") from exc
    if result.returncode != 0:
        diagnostic = (result.stderr or result.stdout or "no command output").strip()
        raise DeployBootstrapError(f"generation preflight failed: {diagnostic[:1000]}")


def _run_frozen_sync(root: Path, uv_path: Path) -> None:
    try:
        result = subprocess.run(
            [str(uv_path), "sync", "--frozen"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=900,
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
) -> None:
    current = _git_head(root, git_path)
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
        )
        if allowed.returncode != 0:
            raise DeployBootstrapError("resume target is not a fast-forward from current HEAD")
        if current != target_commit:
            _git_run(root, git_path, "merge", "--ff-only", target_commit)
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
        )
        if allowed.returncode != 0:
            raise DeployBootstrapError("rollback target is not an ancestor of current HEAD")
        if current != target_commit:
            _git_run(root, git_path, "reset", "--hard", target_commit)
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


def _normalized_deploy_argv(values: list[str]) -> list[str]:
    normalized = list(values)
    if normalized and normalized[0] == "--":
        normalized.pop(0)
    return normalized


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-checkout-root", required=True)
    parser.add_argument("--trusted-git-path", required=True)
    parser.add_argument("--deployment-lock-path", required=True)
    parser.add_argument("--python-path", required=True)
    parser.add_argument("--uv-path", required=True)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--initialize-generation", action="store_true")
    modes.add_argument("--recover-generation", action="store_true")
    modes.add_argument("--finalize-generation", action="store_true")
    parser.add_argument("--recovery-action", choices=("resume", "rollback"))
    parser.add_argument("--finalize-action", choices=("deploy", "resume", "rollback"))
    parser.add_argument("--finalize-phase", choices=("publish", "commit"))
    parser.add_argument("--operation-id")
    parser.add_argument("--inherited-lock-fd", type=int)
    args, deploy_argv = parser.parse_known_args(argv)
    lock_fd = -1
    generation_error_type: type[BaseException] | None = None
    try:
        root = _canonical(args.expected_checkout_root, label="deployment checkout")
        _physical_directory(root, label="deployment checkout")
        if Path.cwd().resolve(strict=True) != root:
            raise DeployBootstrapError("working directory does not match deployment checkout")
        lock_path = _canonical(args.deployment_lock_path, label="deployment lock")
        if args.finalize_generation:
            if args.inherited_lock_fd is None:
                raise DeployBootstrapError("finalizer requires inherited generation lock")
            lock_fd = _assert_inherited_lock(root, lock_path, args.inherited_lock_fd)
        else:
            lock_fd = _acquire_lock(root, lock_path)
        git_path = _canonical(args.trusted_git_path, label="trusted Git")
        _trusted_git(git_path)
        python_path = _canonical(args.python_path, label="deployment Python")
        _physical_file(python_path, label="deployment Python", executable=True)
        uv_path = _canonical(args.uv_path, label="deployment uv")
        _physical_file(uv_path, label="deployment uv", executable=True)
        authority_path = root / "src" / "rquant" / "release_generation.py"
        generation_mode = args.initialize_generation or args.recover_generation
        if args.recover_generation != (args.recovery_action is not None):
            raise DeployBootstrapError(
                "--recovery-action is required only with --recover-generation"
            )
        finalize_arguments_present = any(
            value is not None
            for value in (
                args.finalize_action,
                args.finalize_phase,
                args.operation_id,
                args.inherited_lock_fd,
            )
        )
        if args.finalize_generation and (
            args.finalize_action is None
            or args.finalize_phase is None
            or args.operation_id is None
            or args.inherited_lock_fd is None
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
            commit = _verify_generation_target(root, git_path, target)
            _prepare_generation_checkout(
                root=root,
                git_path=git_path,
                target_commit=commit,
                mode="initialize",
            )
            _physical_file(authority_path, label="release generation authority")
            authority_module = _load_release_authority(authority_path)
            generation_error_type = authority_module.ReleaseGenerationError
            authority = authority_module.ReleaseGenerationAuthority(
                repo=root,
                lock_path=lock_path,
                lock_fd=lock_fd,
                python_path=python_path,
                git_path=git_path,
                writable=True,
            )
            try:
                initialization = authority.read_initialization()
            except generation_error_type as exc:
                if "cannot be read" not in str(exc):
                    raise
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
                        _run_frozen_sync(root, uv_path)
                        _verify_current_generation_checkout(root, git_path, commit)
                        _verify_generation_runtime(root, python_path)
                        _run_generation_preflight(root)
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
                        return 0
                    raise generation_error_type(
                        "release generation initialization already completed"
                    )
            _run_frozen_sync(root, uv_path)
            _verify_current_generation_checkout(root, git_path, commit)
            _verify_generation_runtime(root, python_path)
            _run_generation_preflight(root)
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
            return 0

        commit = _git_head(root, git_path)
        _tracked_checkout_is_clean(root, git_path)
        _verify_generation_runtime(root, python_path)
        _physical_file(authority_path, label="release generation authority")
        authority_module = _load_release_authority(authority_path)
        generation_error_type = authority_module.ReleaseGenerationError
        authority = authority_module.ReleaseGenerationAuthority(
            repo=root,
            lock_path=lock_path,
            lock_fd=lock_fd,
            python_path=python_path,
            git_path=git_path,
            writable=args.recover_generation or args.finalize_generation,
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
            _verify_current_generation_checkout(root, git_path, expected_commit)
            _run_generation_preflight(root)
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
            return 0

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
        return int(
            deploy_main(
                [
                    *deploy_argv,
                    "--repo",
                    str(root),
                    "--deployment-lock-path",
                    str(lock_path),
                    "--deployment-lock-fd",
                    str(lock_fd),
                    "--startup-generation",
                    commit,
                    "--trusted-git-path",
                    str(git_path),
                    "--python-path",
                    str(python_path),
                    "--uv-path",
                    str(uv_path),
                ]
            )
        )
    except Exception as exc:
        expected = isinstance(exc, (DeployBootstrapError, OSError, subprocess.SubprocessError))
        if generation_error_type is not None and isinstance(exc, generation_error_type):
            expected = True
        if not expected:
            raise
        print(f"Production deploy bootstrap failed: {exc}", file=sys.stderr)
        return 2
    finally:
        if lock_fd >= 0:
            os.close(lock_fd)


if __name__ == "__main__":
    raise SystemExit(main())
