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
import re
import shlex
import subprocess
import sys
import tomllib
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, time
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

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
    "rquant-panorama-auth.service": (
        "src/rquant/panorama_auth.py",
    ),
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


class SubprocessRunner:
    def __init__(self, cwd: Path) -> None:
        self._cwd = cwd

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                args,
                cwd=self._cwd,
                check=check,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            diagnostic = (exc.stderr or exc.stdout or "no command output").strip()
            raise DeployError(
                f"command failed ({exc.returncode}): {shlex.join(args)}: {diagnostic[:1000]}"
            ) from exc


@dataclass(frozen=True)
class ChangePlan:
    changed_files: tuple[str, ...]
    blocked_files: tuple[str, ...]
    restart_services: tuple[str, ...]


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


@dataclass(frozen=True)
class DeployResult:
    status: str
    previous_sha: str
    target_sha: str
    target: str
    changed_files: tuple[str, ...]
    restart_services: tuple[str, ...]


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


def build_change_plan(changed_files: list[str] | tuple[str, ...]) -> ChangePlan:
    files = tuple(sorted({path.strip() for path in changed_files if path.strip()}))
    blocked = tuple(
        path for path in files if any(path.startswith(prefix) for prefix in PRIVILEGED_PREFIXES)
    )
    services: set[str] = set()

    for path in files:
        if path in {"pyproject.toml", "uv.lock"} or _matches(
            path, SHARED_RUNTIME_PATTERNS
        ):
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
    return ChangePlan(files, blocked, ordered_services)


def _stdout(runner: Runner, args: list[str]) -> str:
    return runner.run(args).stdout.strip()


def _check_ancestor(runner: Runner, ancestor: str, descendant: str, message: str) -> None:
    result = runner.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
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


@contextmanager
def _deployment_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PolicyError("another production deployment is already running") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _restart_active_services(
    runner: Runner,
    services: tuple[str, ...],
    restarted: list[str],
) -> tuple[str, ...]:
    for service in services:
        state = runner.run(["systemctl", "is-active", service], check=False)
        state_name = state.stdout.strip()
        if state_name == "inactive":
            continue
        if state.returncode != 0 or state_name != "active":
            raise DeployError(
                f"service was not healthy before deployment restart: {service} ({state_name})"
            )
        runner.run(["sudo", "-n", "systemctl", "restart", service])
        restarted.append(service)
        healthy = runner.run(["systemctl", "is-active", service], check=False)
        if healthy.returncode != 0 or healthy.stdout.strip() != "active":
            raise DeployError(f"service failed health check after restart: {service}")
    return tuple(restarted)


def _rollback(
    config: DeployConfig,
    runner: Runner,
    previous_sha: str,
    restarted_services: tuple[str, ...],
) -> None:
    runner.run(["git", "reset", "--hard", previous_sha])
    runner.run([config.uv_bin, "sync", "--frozen"])
    for service in restarted_services:
        runner.run(["sudo", "-n", "systemctl", "restart", service])
        healthy = runner.run(["systemctl", "is-active", service], check=False)
        if healthy.returncode != 0 or healthy.stdout.strip() != "active":
            raise DeployError(f"service failed health check after rollback: {service}")


def _deploy_locked(config: DeployConfig, runner: Runner) -> DeployResult:
    target = validate_target(config.target)
    branch = _stdout(runner, ["git", "rev-parse", "--abbrev-ref", "HEAD"])
    if branch != "main":
        raise PolicyError(f"production checkout must be on main, found {branch!r}")

    dirty = _stdout(runner, ["git", "status", "--porcelain", "--untracked-files=no"])
    if dirty:
        raise PolicyError("tracked production worktree changes must be resolved before deploy")

    runner.run(["git", "fetch", "--tags", "origin", "main"])
    if target.startswith("v"):
        tag_type = _stdout(runner, ["git", "cat-file", "-t", target])
        if tag_type != "tag":
            raise PolicyError("SemVer target must be an annotated tag")
    target_sha = _stdout(runner, ["git", "rev-parse", "--verify", f"{target}^{{commit}}"])
    if target.startswith("v"):
        pyproject = _stdout(
            runner, ["git", "show", f"{target_sha}:pyproject.toml"]
        )
        try:
            package_version = str(tomllib.loads(pyproject)["project"]["version"])
        except (KeyError, tomllib.TOMLDecodeError) as exc:
            raise PolicyError("target pyproject.toml has no readable project version") from exc
        if package_version != target[1:]:
            raise PolicyError(
                f"tag {target} disagrees with package version {package_version}"
            )
    _check_ancestor(
        runner,
        target_sha,
        "origin/main",
        "target is not contained in origin/main",
    )
    previous_sha = _stdout(runner, ["git", "rev-parse", "HEAD"])

    if previous_sha == target_sha:
        result = DeployResult("already_current", previous_sha, target_sha, target, (), ())
        _append_audit(config, result)
        return result

    _check_ancestor(
        runner,
        previous_sha,
        target_sha,
        "target is not a fast-forward from the deployed commit",
    )
    changed_output = _stdout(
        runner, ["git", "diff", "--name-only", f"{previous_sha}..{target_sha}"]
    )
    change_plan = build_change_plan(changed_output.splitlines())
    if change_plan.blocked_files:
        joined = ", ".join(change_plan.blocked_files)
        raise PolicyError(f"privileged infrastructure changes require a separate rollout: {joined}")
    if change_plan.restart_services and is_protected_market_window(config.now):
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
        )
        _append_audit(config, result)
        return result

    restarted: list[str] = []
    try:
        runner.run(["git", "merge", "--ff-only", target_sha])
        runner.run([config.uv_bin, "sync", "--frozen"])
        runner.run([config.rquant_bin, "preflight"])
        _restart_active_services(runner, change_plan.restart_services, restarted)
        runner.run([config.rquant_bin, "preflight"])
    except Exception as exc:
        try:
            _rollback(config, runner, previous_sha, tuple(restarted))
        except Exception as rollback_exc:
            result = DeployResult(
                "rollback_failed",
                previous_sha,
                target_sha,
                target,
                change_plan.changed_files,
                tuple(restarted),
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
    )
    _append_audit(config, result)
    return result


def deploy(config: DeployConfig, *, runner: Runner | None = None) -> DeployResult:
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
    )
    effective_runner = runner or SubprocessRunner(repo)
    lock_path = effective_config.lock_path or repo / "logs" / "production-deploy.lock"
    with _deployment_lock(lock_path):
        return _deploy_locked(effective_config, effective_runner)


def _default_uv_bin() -> str:
    user_uv = Path.home() / ".local" / "bin" / "uv"
    return str(user_uv) if user_uv.exists() else "uv"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deploy an exact rQuant tag or commit")
    parser.add_argument("--target", required=True, help="SemVer tag or full 40-character SHA")
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = DeployConfig(
        repo=args.repo,
        target=args.target,
        dry_run=args.dry_run,
        uv_bin=_default_uv_bin(),
        rquant_bin=".venv/bin/rquant",
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
