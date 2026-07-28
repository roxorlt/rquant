"""Controlled, exact-ref production deployment for rQuant.

The deployer intentionally refuses privileged infrastructure changes. It can update a
clean production checkout, sync locked dependencies, restart an allowlisted set of active
services outside the protected market window, run preflight checks, and roll back on failure.
"""

from __future__ import annotations

import argparse
import fcntl
import fnmatch
import json
import os
import re
import shlex
import stat
import subprocess
import sys
import time as monotonic_time
import tomllib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, time
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from rquant.release_generation import (
    DeploymentIntent,
    ReleaseGenerationAuthority,
    ReleaseGenerationError,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
TARGET_PATTERN = re.compile(r"(?:v\d+\.\d+\.\d+|[0-9a-f]{40})")

ALL_LONG_RUNNING_SERVICES = (
    "rquant-canvas.service",
    "rquant-dashboard.service",
    "rquant-monitor.service",
    "rquant-nl-screen.service",
    "rquant-panorama-auth.service",
    "rquant-panorama.service",
    "rquant-surge-watch.service",
)
LAB_LAUNCHD_HANDOFF_LABELS = (
    "com.roxor.rquant-lab-scheduler",
    "com.roxor.rquant-lab-worker",
    "com.roxor.rquant-lab-finalizer",
)
LINUX_RELEASE_PROFILE = "linux-production"
MACOS_LAB_RELEASE_PROFILE = "macos-lab"
RELEASE_PROFILES = (LINUX_RELEASE_PROFILE, MACOS_LAB_RELEASE_PROFILE)

SERVICE_TIMERS: dict[str, tuple[str, ...]] = {
    "rquant-monitor.service": (
        "rquant-monitor.timer",
        "rquant-monitor-watchdog.timer",
    ),
    "rquant-surge-watch.service": ("rquant-surge-watch.timer",),
}

PRIVILEGED_PREFIXES = (
    "deploy/systemd/",
    "deploy/nginx/",
    "deploy/frp/",
    "deploy/sudoers/",
)

NO_RESTART_SOURCE_PATTERNS = (
    "src/rquant/__init__.py",
    "src/rquant/cli.py",
    "src/rquant/preflight.py",
    "src/rquant/ops/*",
)

SHARED_RUNTIME_PATTERNS = (
    "src/rquant/config.py",
    "src/rquant/storage/*",
)

SERVICE_PATTERNS: dict[str, tuple[str, ...]] = {
    "rquant-canvas.service": (
        "src/rquant/dashboard/nl_canvas.py",
        "src/rquant/llm/*",
        "src/rquant/screen/*",
        "src/rquant/presets.py",
    ),
    "rquant-dashboard.service": (
        "src/rquant/dashboard/app.py",
        "src/rquant/health.py",
        "src/rquant/risk/*",
        "src/rquant/state.py",
    ),
    "rquant-monitor.service": (
        "src/rquant/monitor.py",
        "src/rquant/notify/*",
        "src/rquant/risk/*",
        "src/rquant/state.py",
        "src/rquant/presets.py",
        "src/rquant/screen/*",
        "src/rquant/indicator.py",
    ),
    "rquant-nl-screen.service": (
        "src/rquant/dashboard/nl_screen.py",
        "src/rquant/llm/*",
        "src/rquant/screen/*",
        "src/rquant/presets.py",
        "src/rquant/state.py",
    ),
    "rquant-panorama-auth.service": ("src/rquant/panorama_auth.py",),
    "rquant-panorama.service": (
        "src/rquant/dashboard/market_panorama.py",
        "src/rquant/panorama_*",
    ),
    "rquant-surge-watch.service": (
        "src/rquant/surge_watch.py",
        "src/rquant/intraday_*",
        "src/rquant/notify/*",
    ),
}


class PolicyError(RuntimeError):
    """The requested rollout violates a production safety policy."""


class ProtectedWindowError(PolicyError):
    """The rollout would restart services during the protected market window."""


class DeployError(RuntimeError):
    """The rollout failed after repository mutation began."""


class Runner(Protocol):
    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]: ...


class GenerationAuthority(Protocol):
    def invalidate(self) -> None: ...

    def begin_deployment_intent(self, **values: object) -> DeploymentIntent: ...

    def read_deployment_intent(self) -> DeploymentIntent: ...

    def update_deployment_intent(
        self,
        *,
        operation_id: str,
        stage: str,
        restarted_services: tuple[str, ...] | None = None,
    ) -> DeploymentIntent: ...


class GenerationFinalizer(Protocol):
    def finalize(
        self,
        *,
        expected_commit: str,
        operation_id: str,
        action: str,
        phase: str,
    ) -> object: ...


class SubprocessRunner:
    def __init__(
        self,
        cwd: Path,
        *,
        command_timeout_seconds: float = 300,
        overall_timeout_seconds: float = 1800,
    ) -> None:
        if not 0 < command_timeout_seconds <= overall_timeout_seconds <= 7200:
            raise PolicyError("deployment timeout configuration is invalid")
        self._cwd = cwd
        self._command_timeout_seconds = command_timeout_seconds
        self._deadline = monotonic_time.monotonic() + overall_timeout_seconds

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        remaining = self._deadline - monotonic_time.monotonic()
        if remaining <= 0:
            raise DeployError("deployment overall timeout expired")
        try:
            return subprocess.run(
                args,
                cwd=self._cwd,
                check=check,
                capture_output=True,
                text=True,
                timeout=min(self._command_timeout_seconds, remaining),
            )
        except subprocess.TimeoutExpired as exc:
            raise DeployError(f"command timed out: {shlex.join(args)}") from exc
        except subprocess.CalledProcessError as exc:
            diagnostic = (exc.stderr or exc.stdout or "no command output").strip()
            raise DeployError(
                f"command failed ({exc.returncode}): {shlex.join(args)}: {diagnostic[:1000]}"
            ) from exc


class IsolatedGenerationFinalizer:
    def __init__(self, config: DeployConfig) -> None:
        if config.lock_fd is None or config.lock_path is None or config.python_path is None:
            raise PolicyError("isolated generation finalizer binding is incomplete")
        self._config = config

    def finalize(
        self,
        *,
        expected_commit: str,
        operation_id: str,
        action: str,
        phase: str,
    ) -> object:
        config = self._config
        assert config.lock_fd is not None
        assert config.lock_path is not None
        assert config.python_path is not None
        command = [
            str(config.python_path),
            "-I",
            "-S",
            str(config.repo / "scripts" / "bootstrap-production-deploy.py"),
            "--expected-checkout-root",
            str(config.repo),
            "--trusted-git-path",
            str(config.git_path),
            "--deployment-lock-path",
            str(config.lock_path),
            "--python-path",
            str(config.python_path),
            "--uv-path",
            config.uv_bin,
            "--release-profile",
            config.release_profile,
            "--host-platform",
            config.platform_name,
            "--finalize-generation",
            "--inherited-lock-fd",
            str(config.lock_fd),
            "--operation-id",
            operation_id,
            "--finalize-action",
            action,
            "--finalize-phase",
            phase,
            "--",
            "--target",
            expected_commit,
        ]
        try:
            completed = subprocess.run(
                command,
                cwd=config.repo,
                check=True,
                capture_output=True,
                text=True,
                timeout=config.command_timeout_seconds,
                pass_fds=(config.lock_fd,),
            )
            payload = json.loads(completed.stdout)
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
            raise DeployError("target generation authority failed to publish marker") from exc
        if payload.get("commit") != expected_commit or payload.get("operation_id") != operation_id:
            raise DeployError("target generation authority returned a mismatched result")
        return payload


@dataclass(frozen=True)
class ChangePlan:
    changed_files: tuple[str, ...]
    blocked_files: tuple[str, ...]
    restart_services: tuple[str, ...]
    handoff_daemons: tuple[str, ...] = LAB_LAUNCHD_HANDOFF_LABELS


@dataclass(frozen=True)
class DeployConfig:
    repo: Path
    target: str
    dry_run: bool = False
    now: datetime = field(default_factory=lambda: datetime.now(SHANGHAI))
    uv_bin: str = "uv"
    rquant_bin: str = ".venv/bin/rquant"
    audit_path: Path | None = None
    lock_path: Path | None = None
    lock_fd: int | None = None
    startup_generation: str | None = None
    python_path: Path | None = None
    git_path: Path = Path("/usr/bin/git")
    recovery_action: str | None = None
    release_profile: str = LINUX_RELEASE_PROFILE
    platform_name: str = "linux"
    command_timeout_seconds: float = 300
    overall_timeout_seconds: float = 1800
    handoff_operation_id: str = ""
    handoff_labels: tuple[str, ...] = ()
    lab_lifecycle_mode: str = "uninstalled"


@dataclass(frozen=True)
class DeployResult:
    status: str
    previous_sha: str
    target_sha: str
    target: str
    changed_files: tuple[str, ...]
    restart_services: tuple[str, ...]
    handoff_daemons: tuple[str, ...] = ()


def validate_target(target: str) -> str:
    if TARGET_PATTERN.fullmatch(target) is None:
        raise PolicyError("target must be a SemVer tag or a full 40-character SHA")
    return target


def is_protected_market_window(now: datetime) -> bool:
    local = now.astimezone(SHANGHAI) if now.tzinfo else now.replace(tzinfo=SHANGHAI)
    if local.weekday() >= 5:
        return False
    return time(9, 15) <= local.time().replace(tzinfo=None) <= time(15, 10)


def _matches(path: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def validate_release_profile(release_profile: str, platform_name: str) -> str:
    expected_platform = {
        LINUX_RELEASE_PROFILE: "linux",
        MACOS_LAB_RELEASE_PROFILE: "darwin",
    }.get(release_profile)
    if expected_platform is None or platform_name != expected_platform:
        raise PolicyError(
            f"release profile {release_profile!r} is invalid for platform {platform_name!r}"
        )
    return release_profile


def build_change_plan(
    changed_files: list[str] | tuple[str, ...],
    *,
    release_profile: str = LINUX_RELEASE_PROFILE,
) -> ChangePlan:
    files = tuple(sorted({path.strip() for path in changed_files if path.strip()}))
    blocked = tuple(
        path for path in files if any(path.startswith(prefix) for prefix in PRIVILEGED_PREFIXES)
    )
    if release_profile == MACOS_LAB_RELEASE_PROFILE:
        # Runtime guards bind to the exact checkout SHA, so every local checkout
        # transition requires an orderly handoff even for non-Python changes.
        handoff = LAB_LAUNCHD_HANDOFF_LABELS if files else ()
        return ChangePlan(files, blocked, (), handoff)
    if release_profile != LINUX_RELEASE_PROFILE:
        raise PolicyError(f"unknown release profile: {release_profile!r}")

    services: set[str] = set()

    for path in files:
        if path in {"pyproject.toml", "uv.lock"} or _matches(path, SHARED_RUNTIME_PATTERNS):
            services.update(ALL_LONG_RUNNING_SERVICES)
            continue
        if _matches(path, NO_RESTART_SOURCE_PATTERNS):
            continue
        matched = False
        for service, patterns in SERVICE_PATTERNS.items():
            if _matches(path, patterns):
                services.add(service)
                matched = True
        if path.startswith("src/rquant/") and not matched:
            services.update(ALL_LONG_RUNNING_SERVICES)

    ordered_services = tuple(
        service for service in ALL_LONG_RUNNING_SERVICES if service in services
    )
    return ChangePlan(files, blocked, ordered_services, ())


def _stdout(runner: Runner, args: list[str]) -> str:
    return runner.run(args).stdout.strip()


def _check_ancestor(
    runner: Runner,
    git_path: Path,
    ancestor: str,
    descendant: str,
    message: str,
) -> None:
    result = runner.run(
        [str(git_path), "merge-base", "--is-ancestor", ancestor, descendant],
        check=False,
    )
    if result.returncode != 0:
        raise PolicyError(message)


def _audit_path(config: DeployConfig) -> Path:
    return config.audit_path or config.repo / "logs" / "production-deploy.jsonl"


def _append_audit(config: DeployConfig, result: DeployResult, *, error: str = "") -> None:
    path = _audit_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now(SHANGHAI).isoformat(),
        **asdict(result),
        "changed_files": list(result.changed_files),
        "restart_services": list(result.restart_services),
        "error": error,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _append_intent_audit(
    config: DeployConfig,
    intent: DeploymentIntent,
    *,
    event: str,
) -> None:
    path = _audit_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now(SHANGHAI).isoformat(),
        "event": "deployment_intent",
        "operation_id": intent.operation_id,
        "intent_stage": intent.stage,
        "transition": event,
        "previous_sha": intent.previous_sha,
        "target_sha": intent.target_sha,
        "changed_files": list(intent.changed_files),
        "restart_services": list(intent.restart_services),
        "active_services": list(intent.active_services),
        "active_timers": list(intent.active_timers),
        "restarted_services": list(intent.restarted_services),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


@contextmanager
def _deployment_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    parent_stat = path.parent.lstat()
    if (
        not stat.S_ISDIR(parent_stat.st_mode)
        or stat.S_ISLNK(parent_stat.st_mode)
        or parent_stat.st_uid != os.getuid()
        or stat.S_IMODE(parent_stat.st_mode) != 0o700
    ):
        raise PolicyError("production deployment lock root is unsafe")
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        opened = os.fstat(descriptor)
        active = path.lstat()
        if (
            (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid, opened.st_nlink)
            != (active.st_dev, active.st_ino, active.st_mode, active.st_uid, active.st_nlink)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise PolicyError("production deployment lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PolicyError("another production deployment is already running") from exc
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _active_units(
    runner: Runner,
    units: tuple[str, ...],
    *,
    label: str,
) -> tuple[str, ...]:
    active: list[str] = []
    for unit in units:
        state = runner.run(["systemctl", "is-active", unit], check=False)
        state_name = state.stdout.strip()
        if state_name == "inactive":
            continue
        if state.returncode != 0 or state_name != "active":
            raise DeployError(
                f"{label} was not healthy before deployment transition: {unit} ({state_name})"
            )
        active.append(unit)
    return tuple(active)


def _timers_for_services(services: tuple[str, ...]) -> tuple[str, ...]:
    selected = {timer for service in services for timer in SERVICE_TIMERS.get(service, ())}
    return tuple(sorted(selected))


def _stop_timers(runner: Runner, timers: tuple[str, ...]) -> None:
    for timer in timers:
        runner.run(["sudo", "-n", "systemctl", "stop", timer])


def _restore_timers(runner: Runner, timers: tuple[str, ...]) -> None:
    for timer in timers:
        runner.run(["sudo", "-n", "systemctl", "start", timer])
        state = runner.run(["systemctl", "is-active", timer], check=False)
        if state.returncode != 0 or state.stdout.strip() != "active":
            raise DeployError(f"timer failed health check after restoration: {timer}")


def _restart_services(
    runner: Runner,
    services: tuple[str, ...],
    *,
    after_restart: Callable[[tuple[str, ...]], None] | None = None,
) -> tuple[str, ...]:
    restarted: list[str] = []
    for service in services:
        runner.run(["sudo", "-n", "systemctl", "restart", service])
        restarted.append(service)
        if after_restart is not None:
            after_restart(tuple(restarted))
        healthy = runner.run(["systemctl", "is-active", service], check=False)
        if healthy.returncode != 0 or healthy.stdout.strip() != "active":
            raise DeployError(f"service failed health check after restart: {service}")
    return tuple(restarted)


def _advance_intent(
    config: DeployConfig,
    authority: GenerationAuthority,
    intent: DeploymentIntent,
    stage: str,
    *,
    restarted_services: tuple[str, ...] | None = None,
) -> DeploymentIntent:
    updated = authority.update_deployment_intent(
        operation_id=intent.operation_id,
        stage=stage,
        restarted_services=restarted_services,
    )
    _append_intent_audit(config, updated, event=stage)
    return updated


def _execute_transaction(
    config: DeployConfig,
    runner: Runner,
    authority: GenerationAuthority,
    finalizer: GenerationFinalizer,
    intent: DeploymentIntent,
    *,
    action: str,
) -> DeploymentIntent:
    target_sha = intent.previous_sha if action == "rollback" else intent.target_sha
    git = str(config.git_path)
    _stop_timers(runner, intent.active_timers)
    intent = _advance_intent(config, authority, intent, "timers_stopped")

    if action == "rollback":
        runner.run([git, "reset", "--hard", target_sha])
    elif action == "deploy":
        runner.run([git, "merge", "--ff-only", target_sha])
    elif action == "resume":
        current = _stdout(runner, [git, "rev-parse", "HEAD"])
        if current not in {intent.previous_sha, intent.target_sha}:
            raise DeployError("recovery checkout is outside the recorded deployment intent")
        if current != target_sha:
            runner.run([git, "merge", "--ff-only", target_sha])
    else:
        raise PolicyError("unknown deployment recovery action")
    intent = _advance_intent(config, authority, intent, f"{action}_checkout_ready")

    runner.run([config.uv_bin, "sync", "--frozen"])
    intent = _advance_intent(config, authority, intent, f"{action}_dependencies_ready")
    runner.run([config.rquant_bin, "preflight"])
    intent = _advance_intent(config, authority, intent, f"{action}_preflight_ready")
    intent = _advance_intent(config, authority, intent, "services_transitioning")

    def service_restarted(restarted: tuple[str, ...]) -> None:
        nonlocal intent
        intent = _advance_intent(
            config,
            authority,
            intent,
            "services_transitioning",
            restarted_services=restarted,
        )

    restarted = _restart_services(
        runner,
        intent.active_services,
        after_restart=service_restarted,
    )
    intent = _advance_intent(
        config,
        authority,
        intent,
        "services_ready",
        restarted_services=restarted,
    )
    runner.run([config.rquant_bin, "preflight"])
    intent = _advance_intent(config, authority, intent, "post_restart_preflight_ready")
    _restore_timers(runner, intent.active_timers)
    intent = _advance_intent(config, authority, intent, "timers_restored")
    finalizer.finalize(
        expected_commit=target_sha,
        operation_id=intent.operation_id,
        action=action,
        phase="publish",
    )
    intent = _advance_intent(config, authority, intent, "marker_published")
    intent = _advance_intent(config, authority, intent, "completed")
    finalizer.finalize(
        expected_commit=target_sha,
        operation_id=intent.operation_id,
        action=action,
        phase="commit",
    )
    return intent


def _rollback_unmanaged(
    config: DeployConfig,
    runner: Runner,
    previous_sha: str,
    restarted_services: tuple[str, ...],
) -> None:
    runner.run([str(config.git_path), "reset", "--hard", previous_sha])
    runner.run([config.uv_bin, "sync", "--frozen"])
    runner.run([config.rquant_bin, "preflight"])
    _restart_services(runner, restarted_services)
    runner.run([config.rquant_bin, "preflight"])


def _recover_locked(
    config: DeployConfig,
    runner: Runner,
    authority: GenerationAuthority,
    finalizer: GenerationFinalizer,
) -> DeployResult:
    action = config.recovery_action
    if action not in {"resume", "rollback"}:
        raise PolicyError("recovery action must be resume or rollback")
    intent = authority.read_deployment_intent()
    if config.dry_run:
        raise PolicyError("recovery does not support dry-run")
    expected_target = intent.target_sha if action == "resume" else intent.previous_sha
    allowed_refs = {expected_target}
    if action == "resume":
        allowed_refs.add(intent.target_ref)
    if config.target not in allowed_refs:
        raise PolicyError("recovery target does not match the recorded deployment intent")
    plan = build_change_plan(intent.changed_files, release_profile=config.release_profile)
    if plan.blocked_files or plan.restart_services != intent.restart_services:
        raise PolicyError("recorded deployment intent no longer matches change classification")
    if (intent.restart_services or plan.handoff_daemons) and is_protected_market_window(config.now):
        raise ProtectedWindowError(
            "deployment recovery requires service restarts during the protected 09:15-15:10 window"
        )
    if not set(intent.active_services).issubset(intent.restart_services):
        raise PolicyError("recorded active service plan is invalid")
    if not set(intent.restarted_services).issubset(intent.active_services):
        raise PolicyError("recorded restarted service state is invalid")
    if not set(intent.active_timers).issubset(_timers_for_services(intent.restart_services)):
        raise PolicyError("recorded active timer plan is invalid")
    git = str(config.git_path)
    branch = _stdout(runner, [git, "rev-parse", "--abbrev-ref", "HEAD"])
    if branch != "main":
        raise PolicyError(f"production checkout must be on main, found {branch!r}")
    dirty = _stdout(runner, [git, "status", "--porcelain", "--untracked-files=no"])
    if dirty:
        raise PolicyError("tracked production worktree changes must be resolved before recovery")
    intent = _advance_intent(config, authority, intent, "recovery_started")
    authority.invalidate()
    completed = _execute_transaction(
        config,
        runner,
        authority,
        finalizer,
        intent,
        action=action,
    )
    result = DeployResult(
        "recovered",
        intent.previous_sha,
        expected_target,
        config.target,
        intent.changed_files,
        completed.restarted_services,
        plan.handoff_daemons,
    )
    _append_audit(config, result)
    return result


def _deploy_locked(
    config: DeployConfig,
    runner: Runner,
    generation_authority: GenerationAuthority | None,
    generation_finalizer: GenerationFinalizer | None,
) -> DeployResult:
    if config.recovery_action is not None:
        if generation_authority is None or generation_finalizer is None:
            raise PolicyError("deployment recovery requires persistent generation authority")
        return _recover_locked(config, runner, generation_authority, generation_finalizer)
    target = validate_target(config.target)
    git_path = config.git_path
    if not git_path.is_absolute():
        raise PolicyError("trusted Git path must be absolute")
    git = str(git_path)
    branch = _stdout(runner, [git, "rev-parse", "--abbrev-ref", "HEAD"])
    if branch != "main":
        raise PolicyError(f"production checkout must be on main, found {branch!r}")

    dirty = _stdout(runner, [git, "status", "--porcelain", "--untracked-files=no"])
    if dirty:
        raise PolicyError("tracked production worktree changes must be resolved before deploy")

    runner.run([git, "fetch", "--tags", "origin", "main"])
    if target.startswith("v"):
        tag_type = _stdout(runner, [git, "cat-file", "-t", target])
        if tag_type != "tag":
            raise PolicyError("SemVer target must be an annotated tag")
    target_sha = _stdout(runner, [git, "rev-parse", "--verify", f"{target}^{{commit}}"])
    if target.startswith("v"):
        pyproject = _stdout(runner, [git, "show", f"{target_sha}:pyproject.toml"])
        try:
            package_version = str(tomllib.loads(pyproject)["project"]["version"])
        except (KeyError, tomllib.TOMLDecodeError) as exc:
            raise PolicyError("target pyproject.toml has no readable project version") from exc
        if package_version != target[1:]:
            raise PolicyError(f"tag {target} disagrees with package version {package_version}")
    _check_ancestor(
        runner,
        git_path,
        target_sha,
        "origin/main",
        "target is not contained in origin/main",
    )
    previous_sha = _stdout(runner, [git, "rev-parse", "HEAD"])

    if previous_sha == target_sha:
        result = DeployResult("already_current", previous_sha, target_sha, target, (), (), ())
        _append_audit(config, result)
        return result

    _check_ancestor(
        runner,
        git_path,
        previous_sha,
        target_sha,
        "target is not a fast-forward from the deployed commit",
    )
    changed_output = _stdout(runner, [git, "diff", "--name-only", f"{previous_sha}..{target_sha}"])
    change_plan = build_change_plan(
        changed_output.splitlines(),
        release_profile=config.release_profile,
    )
    if change_plan.blocked_files:
        joined = ", ".join(change_plan.blocked_files)
        raise PolicyError(f"privileged infrastructure changes require a separate rollout: {joined}")
    if (change_plan.restart_services or change_plan.handoff_daemons) and is_protected_market_window(
        config.now
    ):
        raise ProtectedWindowError(
            "release requires service restarts during the protected 09:15-15:10 window"
        )

    if config.dry_run:
        result = DeployResult(
            "dry_run",
            previous_sha,
            target_sha,
            target,
            change_plan.changed_files,
            change_plan.restart_services,
            change_plan.handoff_daemons,
        )
        _append_audit(config, result)
        return result

    if generation_authority is not None:
        if generation_finalizer is None:
            raise PolicyError("formal deployment requires isolated target generation authority")
        active_services = _active_units(
            runner,
            change_plan.restart_services,
            label="service",
        )
        active_timers = _active_units(
            runner,
            _timers_for_services(change_plan.restart_services),
            label="timer",
        )
        intent = generation_authority.begin_deployment_intent(
            previous_sha=previous_sha,
            target_sha=target_sha,
            target_ref=target,
            changed_files=change_plan.changed_files,
            restart_services=change_plan.restart_services,
            active_services=active_services,
            active_timers=active_timers,
            handoff_operation_id=config.handoff_operation_id,
            handoff_labels=config.handoff_labels,
        )
        _append_intent_audit(config, intent, event="planned")
        generation_authority.invalidate()
        try:
            completed = _execute_transaction(
                config,
                runner,
                generation_authority,
                generation_finalizer,
                intent,
                action="deploy",
            )
        except Exception as exc:
            try:
                recovery = _advance_intent(
                    config,
                    generation_authority,
                    generation_authority.read_deployment_intent(),
                    "recovery_started",
                )
                generation_authority.invalidate()
                completed = _execute_transaction(
                    config,
                    runner,
                    generation_authority,
                    generation_finalizer,
                    recovery,
                    action="rollback",
                )
            except Exception as rollback_exc:
                result = DeployResult(
                    "rollback_failed",
                    previous_sha,
                    target_sha,
                    target,
                    change_plan.changed_files,
                    generation_authority.read_deployment_intent().restarted_services,
                    change_plan.handoff_daemons,
                )
                _append_audit(config, result, error=f"{exc}; rollback: {rollback_exc}")
                raise DeployError(
                    f"deployment failed and rollback also failed: {rollback_exc}"
                ) from exc
            result = DeployResult(
                "rolled_back",
                previous_sha,
                target_sha,
                target,
                change_plan.changed_files,
                completed.restarted_services,
                change_plan.handoff_daemons,
            )
            _append_audit(config, result, error=str(exc))
            raise DeployError(
                f"deployment failed and rolled back to {previous_sha}: {exc}"
            ) from exc
        result = DeployResult(
            "deployed",
            previous_sha,
            target_sha,
            target,
            change_plan.changed_files,
            completed.restarted_services,
            change_plan.handoff_daemons,
        )
        _append_audit(config, result)
        return result

    restarted: list[str] = []
    try:
        runner.run([git, "merge", "--ff-only", target_sha])
        runner.run([config.uv_bin, "sync", "--frozen"])
        runner.run([config.rquant_bin, "preflight"])
        active_services = _active_units(runner, change_plan.restart_services, label="service")
        _restart_services(
            runner,
            active_services,
            after_restart=lambda values: restarted.__setitem__(slice(None), values),
        )
        runner.run([config.rquant_bin, "preflight"])
    except Exception as exc:
        try:
            _rollback_unmanaged(
                config,
                runner,
                previous_sha,
                tuple(restarted),
            )
        except Exception as rollback_exc:
            result = DeployResult(
                "rollback_failed",
                previous_sha,
                target_sha,
                target,
                change_plan.changed_files,
                tuple(restarted),
                change_plan.handoff_daemons,
            )
            _append_audit(config, result, error=f"{exc}; rollback: {rollback_exc}")
            raise DeployError(
                f"deployment failed and rollback also failed: {rollback_exc}"
            ) from exc
        result = DeployResult(
            "rolled_back",
            previous_sha,
            target_sha,
            target,
            change_plan.changed_files,
            tuple(restarted),
            change_plan.handoff_daemons,
        )
        _append_audit(config, result, error=str(exc))
        raise DeployError(f"deployment failed and rolled back to {previous_sha}: {exc}") from exc

    result = DeployResult(
        "deployed",
        previous_sha,
        target_sha,
        target,
        change_plan.changed_files,
        tuple(restarted),
        change_plan.handoff_daemons,
    )
    _append_audit(config, result)
    return result


def deploy(
    config: DeployConfig,
    *,
    runner: Runner | None = None,
    generation_authority: GenerationAuthority | None = None,
    generation_finalizer: GenerationFinalizer | None = None,
) -> DeployResult:
    validate_release_profile(config.release_profile, config.platform_name)
    if config.lab_lifecycle_mode not in {"uninstalled", "installed"}:
        raise PolicyError("Lab lifecycle mode is invalid")
    if (
        config.release_profile != MACOS_LAB_RELEASE_PROFILE
        and config.lab_lifecycle_mode != "uninstalled"
    ):
        raise PolicyError("Lab lifecycle is only valid for the macOS release profile")
    if (
        config.release_profile == MACOS_LAB_RELEASE_PROFILE
        and config.lab_lifecycle_mode == "installed"
        and not config.dry_run
    ):
        if (
            re.fullmatch(r"[0-9a-f]{32}", config.handoff_operation_id) is None
            or config.handoff_labels != LAB_LAUNCHD_HANDOFF_LABELS
        ):
            raise PolicyError("macOS Lab deployment requires a persisted launchd handoff")
    elif config.handoff_operation_id or config.handoff_labels:
        raise PolicyError("launchd handoff binding is only valid for macOS Lab deployment")
    repo = config.repo.resolve()
    effective_config = DeployConfig(
        repo=repo,
        target=config.target,
        dry_run=config.dry_run,
        now=config.now,
        uv_bin=config.uv_bin,
        rquant_bin=config.rquant_bin,
        audit_path=config.audit_path,
        lock_path=config.lock_path,
        lock_fd=config.lock_fd,
        startup_generation=config.startup_generation,
        python_path=config.python_path,
        git_path=config.git_path,
        recovery_action=config.recovery_action,
        release_profile=config.release_profile,
        platform_name=config.platform_name,
        command_timeout_seconds=config.command_timeout_seconds,
        overall_timeout_seconds=config.overall_timeout_seconds,
        handoff_operation_id=config.handoff_operation_id,
        handoff_labels=config.handoff_labels,
        lab_lifecycle_mode=config.lab_lifecycle_mode,
    )
    effective_runner = runner or SubprocessRunner(
        repo,
        command_timeout_seconds=effective_config.command_timeout_seconds,
        overall_timeout_seconds=effective_config.overall_timeout_seconds,
    )
    lock_path = effective_config.lock_path or (repo.parent / ".rquant-deploy" / f"{repo.name}.lock")
    if effective_config.lock_fd is not None:
        try:
            opened = os.fstat(effective_config.lock_fd)
            active = lock_path.lstat()
        except OSError as exc:
            raise PolicyError("inherited deployment generation lock is unavailable") from exc
        if (
            (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid, opened.st_nlink)
            != (active.st_dev, active.st_ino, active.st_mode, active.st_uid, active.st_nlink)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise PolicyError("inherited deployment generation lock identity changed")
        if generation_authority is None:
            if effective_config.startup_generation is None or effective_config.python_path is None:
                raise PolicyError("release generation binding is incomplete")
            try:
                generation_authority = ReleaseGenerationAuthority(
                    repo=repo,
                    lock_path=lock_path,
                    lock_fd=effective_config.lock_fd,
                    python_path=effective_config.python_path,
                    git_path=effective_config.git_path,
                    writable=not effective_config.dry_run,
                    uv_path=Path(effective_config.uv_bin),
                )
                if effective_config.recovery_action is None:
                    generation_authority.verify(expected_commit=effective_config.startup_generation)
                else:
                    intent = generation_authority.read_deployment_intent()
                    if effective_config.startup_generation not in {
                        intent.previous_sha,
                        intent.target_sha,
                    }:
                        raise ReleaseGenerationError(
                            "recovery checkout is outside deployment intent"
                        )
            except ReleaseGenerationError as exc:
                raise PolicyError(f"release generation is not ready: {exc}") from exc
        if generation_finalizer is None:
            generation_finalizer = IsolatedGenerationFinalizer(effective_config)
        return _deploy_locked(
            effective_config,
            effective_runner,
            generation_authority,
            generation_finalizer,
        )
    with _deployment_lock(lock_path):
        return _deploy_locked(
            effective_config,
            effective_runner,
            generation_authority,
            generation_finalizer,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deploy an exact rQuant tag or commit")
    parser.add_argument("--target", required=True, help="SemVer tag or full 40-character SHA")
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--deployment-lock-path", type=Path, required=True)
    parser.add_argument("--deployment-lock-fd", type=int, required=True)
    parser.add_argument("--startup-generation", required=True)
    parser.add_argument("--trusted-git-path", type=Path, required=True)
    parser.add_argument("--python-path", type=Path, required=True)
    parser.add_argument("--uv-path", type=Path, required=True)
    parser.add_argument("--recovery-action", choices=("resume", "rollback"))
    parser.add_argument("--release-profile", choices=RELEASE_PROFILES, required=True)
    parser.add_argument("--platform-name", choices=("linux", "darwin"), required=True)
    parser.add_argument("--command-timeout-seconds", type=float, default=300)
    parser.add_argument("--overall-timeout-seconds", type=float, default=1800)
    parser.add_argument("--lab-handoff-operation-id", default="")
    parser.add_argument("--lab-handoff-label", action="append", default=[])
    parser.add_argument(
        "--lab-lifecycle-mode",
        choices=("uninstalled", "installed"),
        default="uninstalled",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = DeployConfig(
        repo=args.repo,
        target=args.target,
        dry_run=args.dry_run,
        uv_bin=str(args.uv_path),
        rquant_bin=".venv/bin/rquant",
        lock_path=args.deployment_lock_path,
        lock_fd=args.deployment_lock_fd,
        startup_generation=args.startup_generation,
        python_path=args.python_path,
        git_path=args.trusted_git_path,
        recovery_action=args.recovery_action,
        release_profile=args.release_profile,
        platform_name=args.platform_name,
        command_timeout_seconds=args.command_timeout_seconds,
        overall_timeout_seconds=args.overall_timeout_seconds,
        handoff_operation_id=args.lab_handoff_operation_id,
        handoff_labels=tuple(args.lab_handoff_label),
        lab_lifecycle_mode=args.lab_lifecycle_mode,
    )
    try:
        result = deploy(config)
    except ProtectedWindowError as exc:
        print(f"DEFERRED: {exc}", file=sys.stderr)
        return 75
    except PolicyError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2
    except DeployError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(asdict(result), ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
