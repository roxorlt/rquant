"""Controlled, exact-ref production deployment for rQuant.

The deployer intentionally refuses privileged infrastructure changes. It can update a
clean production checkout, sync locked dependencies, restart an allowlisted set of active
services outside the protected market window, run preflight checks, and roll back on failure.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
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
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, time
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from rquant.contained_subprocess import run_contained
from rquant.release_generation import (
    ALL_LONG_RUNNING_SERVICES as _ALL_LONG_RUNNING_SERVICES,
)
from rquant.release_generation import (
    LAB_LAUNCHD_HANDOFF_LABELS,
    LINUX_RELEASE_PROFILE,
    MACOS_LAB_RELEASE_PROFILE,
    RELEASE_PROFILES,
    DeploymentChangePlan,
    DeploymentIntent,
    ReleaseGenerationAuthority,
    ReleaseGenerationError,
    build_deployment_change_plan,
    validate_deployment_change_policy,
    validate_deployment_intent_policy,
)
from rquant.release_generation import (
    deployment_timers_for_services as _timers_for_services,
)
from rquant.strict_json import StrictJsonError, strict_canonical_json_loads

ALL_LONG_RUNNING_SERVICES = _ALL_LONG_RUNNING_SERVICES
ChangePlan = DeploymentChangePlan
build_change_plan = build_deployment_change_plan
SHANGHAI = ZoneInfo("Asia/Shanghai")
TARGET_PATTERN = re.compile(r"(?:v\d+\.\d+\.\d+|[0-9a-f]{40})")
LINUX_PRODUCTION_RUNTIME_ROOT = Path("/home/lighthouse/rquant/data/runtime")
LINUX_PRODUCTION_EVIDENCE_CACHE_DIR = Path("/home/lighthouse/rquant/var/r07-dr-evidence")
R07_DEPLOY_GATE_SCRIPT = "scripts/r07_deploy_gate.py"
R07_DECISION_GATES = ("bootstrap_disabled", "enforced", "rejected")
R07_DECISION_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
_R07_DECISION_SHA_FIELDS = (
    "installed_commit_sha",
    "installed_tree_sha",
    "target_commit_sha",
    "target_tree_sha",
)
_R07_DECISION_FIELDS = (
    "allowed",
    "gate",
    "reason",
    "requires_evidence",
    "installed_mode",
    "target_mode",
    "installed_commit_sha",
    "installed_tree_sha",
    "target_commit_sha",
    "target_tree_sha",
)


@dataclass(frozen=True)
class R07GateDecision:
    """One resolved Release A/B decision, as the deployer consumes it."""

    allowed: bool
    gate: str
    reason: str
    requires_evidence: bool
    installed_mode: str
    target_mode: str
    installed_commit_sha: str
    installed_tree_sha: str
    target_commit_sha: str
    target_tree_sha: str

    def audit_fields(self) -> dict[str, str]:
        return {
            "r07_gate": self.gate,
            "r07_reason": self.reason,
            "r07_installed_mode": self.installed_mode,
            "r07_target_mode": self.target_mode,
            "r07_installed_commit_sha": self.installed_commit_sha,
            "r07_installed_tree_sha": self.installed_tree_sha,
            "r07_target_commit_sha": self.target_commit_sha,
            "r07_target_tree_sha": self.target_tree_sha,
        }


def r07_decision_from_child_output(raw: str | bytes) -> R07GateDecision:
    """Parse the isolated gate child's stdout as one untrusted canonical JSON decision.

    The child is treated as an untrusted input boundary: the bytes must be exactly canonical
    JSON with no duplicate key, no reordering, no surrounding whitespace, and no second record,
    and every field must match its exact contract before the deployer acts on the verdict.
    """

    try:
        payload = strict_canonical_json_loads(raw)
    except (StrictJsonError, UnicodeDecodeError) as exc:
        raise PolicyError(
            "R07 deployment gate did not return one canonical JSON decision record"
        ) from exc
    if type(payload) is not dict or set(payload) != set(_R07_DECISION_FIELDS):
        raise PolicyError("R07 deployment gate returned an unexpected decision record")
    for field_name in ("allowed", "requires_evidence"):
        if type(payload[field_name]) is not bool:
            raise PolicyError("R07 deployment gate decision flags must be exact booleans")
    for field_name in _R07_DECISION_FIELDS[1:3] + _R07_DECISION_FIELDS[4:]:
        if type(payload[field_name]) is not str:
            raise PolicyError("R07 deployment gate decision fields must be exact strings")
    if payload["gate"] not in R07_DECISION_GATES:
        raise PolicyError("R07 deployment gate returned an unknown gate verdict")
    for field_name in _R07_DECISION_SHA_FIELDS:
        value = payload[field_name]
        if value and R07_DECISION_SHA_PATTERN.fullmatch(str(value)) is None:
            raise PolicyError("R07 deployment gate decision commit and tree IDs must be 40-hex")
    decision = R07GateDecision(**payload)  # type: ignore[arg-type]
    if decision.allowed == (decision.gate == "rejected"):
        raise PolicyError("R07 deployment gate decision is internally inconsistent")
    if decision.requires_evidence and decision.gate != "enforced":
        raise PolicyError(
            "R07 deployment gate returned a non-enforced verdict that consumes evidence"
        )
    return decision


class R07EvidenceGate(Protocol):
    @property
    def cache_dir(self) -> Path: ...

    def evaluate(
        self,
        *,
        repo: Path,
        runner: Runner,
        git_path: Path,
        installed_commit_sha: str,
        target_commit_sha: str,
    ) -> R07GateDecision: ...


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

    def read_prepared_deployment_intent(self) -> DeploymentIntent: ...

    def adopt_prepared_deployment_intent(self, *, operation_id: str) -> DeploymentIntent: ...

    def update_deployment_intent(
        self,
        *,
        operation_id: str,
        stage: str,
        restarted_services: tuple[str, ...] | None = None,
    ) -> DeploymentIntent: ...

    def rebind_deployment_handoff(
        self,
        *,
        operation_id: str,
        handoff_operation_id: str,
        handoff_labels: tuple[str, ...],
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


def _run_process_group(
    args: list[str],
    *,
    cwd: Path,
    deadline_monotonic: float,
    check: bool,
    pass_fds: tuple[int, ...] = (),
    env: dict[str, str] | None = None,
    may_spawn_background_descendants: bool = False,
) -> subprocess.CompletedProcess[str]:
    if monotonic_time.monotonic() >= deadline_monotonic:
        raise subprocess.TimeoutExpired(args, 0)
    return run_contained(
        args,
        cwd=cwd,
        deadline_monotonic=deadline_monotonic,
        check=check,
        pass_fds=pass_fds,
        env=env,
        may_spawn_background_descendants=may_spawn_background_descendants,
    )


class SubprocessRunner:
    def __init__(
        self,
        cwd: Path,
        *,
        trusted_git_path: Path = Path("/usr/bin/git"),
        command_timeout_seconds: float = 300,
        overall_timeout_seconds: float = 1800,
        overall_deadline_monotonic: float | None = None,
    ) -> None:
        if not 0 < command_timeout_seconds <= overall_timeout_seconds <= 7200:
            raise PolicyError("deployment timeout configuration is invalid")
        trusted_git = Path(trusted_git_path)
        if not trusted_git.is_absolute() or trusted_git != Path(os.path.abspath(trusted_git)):
            raise PolicyError("trusted Git path must be absolute and canonical")
        self._cwd = cwd
        self._trusted_git_path = trusted_git
        self._command_timeout_seconds = command_timeout_seconds
        self._overall_timeout_seconds = overall_timeout_seconds
        self._deadline = (
            monotonic_time.monotonic() + overall_timeout_seconds
            if overall_deadline_monotonic is None
            else overall_deadline_monotonic
        )
        if not math.isfinite(self._deadline):
            raise PolicyError("deployment overall deadline is invalid")

    def for_recovery(self) -> SubprocessRunner:
        return SubprocessRunner(
            self._cwd,
            trusted_git_path=self._trusted_git_path,
            command_timeout_seconds=self._command_timeout_seconds,
            overall_timeout_seconds=self._overall_timeout_seconds,
            overall_deadline_monotonic=self._deadline,
        )

    @property
    def deadline_monotonic(self) -> float:
        return self._deadline

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        remaining = self._deadline - monotonic_time.monotonic()
        if remaining <= 0:
            raise DeployError("deployment overall timeout expired")
        environment = os.environ.copy()
        if args and Path(args[0]) == self._trusted_git_path:
            mutating_commands = {"checkout", "fetch", "merge", "pull", "reset", "switch"}
            subcommand = next((value for value in args[1:] if not value.startswith("-")), "")
            environment["GIT_OPTIONAL_LOCKS"] = "1" if subcommand in mutating_commands else "0"
            environment["GIT_TERMINAL_PROMPT"] = "0"
        try:
            return _run_process_group(
                args,
                cwd=self._cwd,
                deadline_monotonic=self._deadline,
                check=check,
                env=environment,
            )
        except subprocess.TimeoutExpired as exc:
            raise DeployError(f"command timed out: {shlex.join(args)}") from exc
        except subprocess.CalledProcessError as exc:
            diagnostic = (exc.stderr or exc.stdout or "no command output").strip()
            raise DeployError(
                f"command failed ({exc.returncode}): {shlex.join(args)}: {diagnostic[:1000]}"
            ) from exc


def _fresh_recovery_runner(runner: Runner) -> Runner:
    factory = getattr(runner, "for_recovery", None)
    if not callable(factory):
        raise DeployError("deployment recovery requires a fresh bounded runner")
    recovered = factory()
    if not hasattr(recovered, "run"):
        raise DeployError("deployment recovery runner is invalid")
    return recovered


class IsolatedGenerationFinalizer:
    def __init__(self, config: DeployConfig) -> None:
        if config.lock_fd is None or config.lock_path is None or config.python_path is None:
            raise PolicyError("isolated generation finalizer binding is incomplete")
        self._config = config

    def for_recovery(self, overall_deadline_monotonic: float) -> IsolatedGenerationFinalizer:
        current = self._config.overall_deadline_monotonic
        return IsolatedGenerationFinalizer(
            replace(
                self._config,
                overall_deadline_monotonic=(
                    overall_deadline_monotonic
                    if current is None
                    else min(current, overall_deadline_monotonic)
                ),
            )
        )

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
        if config.overall_deadline_monotonic is None:
            raise PolicyError("generation finalizer requires the original deployment deadline")
        if config.lab_lifecycle_mode == "installed" and config.handoff_lock_fd is None:
            raise PolicyError("installed finalizer requires inherited Lab handoff lock")
        remaining = config.overall_deadline_monotonic - monotonic_time.monotonic()
        if remaining <= 0:
            raise DeployError("deployment overall timeout expired before generation finalizer")
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
            "--lab-lifecycle-mode",
            config.lab_lifecycle_mode,
            "--finalize-generation",
            "--inherited-lock-fd",
            str(config.lock_fd),
            "--operation-id",
            operation_id,
            "--finalize-action",
            action,
            "--finalize-phase",
            phase,
        ]
        pass_fds = [config.lock_fd]
        if config.handoff_lock_fd is not None:
            command.extend(["--inherited-handoff-lock-fd", str(config.handoff_lock_fd)])
            pass_fds.append(config.handoff_lock_fd)
        command.extend(
            [
                "--overall-deadline-monotonic",
                str(config.overall_deadline_monotonic),
                "--",
                "--target",
                expected_commit,
            ]
        )
        try:
            completed = _run_process_group(
                command,
                cwd=config.repo,
                deadline_monotonic=config.overall_deadline_monotonic,
                check=True,
                pass_fds=tuple(pass_fds),
            )
            payload = json.loads(completed.stdout)
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
            raise DeployError("target generation authority failed to publish marker") from exc
        if payload.get("commit") != expected_commit or payload.get("operation_id") != operation_id:
            raise DeployError("target generation authority returned a mismatched result")
        return payload


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
    handoff_lock_fd: int | None = None
    startup_generation: str | None = None
    python_path: Path | None = None
    git_path: Path = Path("/usr/bin/git")
    recovery_action: str | None = None
    release_profile: str = LINUX_RELEASE_PROFILE
    platform_name: str = "linux"
    command_timeout_seconds: float = 300
    overall_timeout_seconds: float = 1800
    overall_deadline_monotonic: float | None = None
    handoff_operation_id: str = ""
    handoff_labels: tuple[str, ...] = ()
    lab_lifecycle_mode: str = "uninstalled"
    prepared_intent_operation_id: str = ""
    runtime_production_inputs: Path | None = None
    runtime_profile_output_dir: Path | None = None
    runtime_root: Path | None = None
    runtime_schema_v1_migration_authority: Path | None = None
    r07_evidence_cache_dir: Path | None = None


@dataclass
class RuntimeProfileTransactionState:
    runtime_root: Path | None = None
    profile_applied: bool = False
    preview_profile_id: str | None = None


@dataclass(frozen=True)
class DeployResult:
    status: str
    previous_sha: str
    target_sha: str
    target: str
    changed_files: tuple[str, ...]
    restart_services: tuple[str, ...]
    handoff_daemons: tuple[str, ...] = ()
    r07_gate: str = ""
    r07_target_tree_sha: str = ""


def validate_target(target: str) -> str:
    if TARGET_PATTERN.fullmatch(target) is None:
        raise PolicyError("target must be a SemVer tag or a full 40-character SHA")
    return target


def is_protected_market_window(now: datetime) -> bool:
    local = now.astimezone(SHANGHAI) if now.tzinfo else now.replace(tzinfo=SHANGHAI)
    if local.weekday() >= 5:
        return False
    return time(9, 15) <= local.time().replace(tzinfo=None) <= time(15, 10)


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


def _stdout(runner: Runner, args: list[str]) -> str:
    return runner.run(args).stdout.strip()


def _json_object(runner: Runner, args: list[str], *, label: str) -> dict[str, object]:
    raw = _stdout(runner, args)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DeployError(f"{label} returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise DeployError(f"{label} returned a non-object receipt")
    return payload


def _receipt_sha256(payload: dict[str, object], key: str, *, label: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise DeployError(f"{label} returned an invalid {key}")
    return value


def _receipt_path(payload: dict[str, object], key: str, *, label: str) -> Path:
    value = payload.get(key)
    if not isinstance(value, str):
        raise DeployError(f"{label} returned an invalid {key}")
    path = Path(value)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise DeployError(f"{label} returned a non-canonical {key}")
    return path


def _deploy_runtime_profile(
    config: DeployConfig,
    runner: Runner,
    *,
    target_sha: str,
    action: str,
    transaction: RuntimeProfileTransactionState,
    apply_changes: bool = True,
) -> Path | None:
    inputs = config.runtime_production_inputs
    output_dir = config.runtime_profile_output_dir
    if inputs is None:
        return None
    if output_dir is None:  # pragma: no cover - deploy() validates the pair
        raise PolicyError("runtime profile output directory is missing")
    if action == "rollback":
        if config.runtime_root is None:  # pragma: no cover - production policy validates this
            raise PolicyError("runtime root is required for production rollback")
        return config.runtime_root

    production_command = [
        config.rquant_bin,
        "runtime-production-profile",
        "--inputs",
        str(inputs),
        "--output-dir",
        str(output_dir),
        "--expected-commit",
        target_sha,
    ]
    if config.release_profile == LINUX_RELEASE_PROFILE:
        production_command.extend(["--runtime-mode", "linux-production"])
    production = _json_object(
        runner,
        production_command,
        label="runtime production profile",
    )
    if production.get("producer_commit") != target_sha:
        raise DeployError("runtime production profile commit mismatch")
    if production.get("status") != "dry_run":
        raise DeployError("runtime production profile preview was not pure")
    profile_id = _receipt_sha256(
        production,
        "profile_id",
        label="runtime production profile",
    )
    profile_path = _receipt_path(
        production,
        "profile_path",
        label="runtime production profile",
    )
    runtime_root = _receipt_path(
        production,
        "runtime_root",
        label="runtime production profile",
    )
    if config.runtime_root is None or runtime_root != config.runtime_root:
        raise DeployError("runtime production profile root mismatch")
    expected_profile_path = output_dir / f"{profile_id}.json"
    if profile_path != expected_profile_path:
        raise DeployError("runtime production profile path mismatch")
    if transaction.preview_profile_id is not None and transaction.preview_profile_id != profile_id:
        raise DeployError("runtime production profile changed during deployment preview")
    transaction.preview_profile_id = profile_id

    prerequisite_command = [
        config.rquant_bin,
        "runtime-production-prerequisites",
        "--inputs",
        str(inputs),
        "--expected-commit",
        target_sha,
    ]
    if config.release_profile == LINUX_RELEASE_PROFILE:
        prerequisite_command.extend(["--runtime-mode", "linux-production"])
    prerequisite_preview = _json_object(
        runner,
        prerequisite_command,
        label="runtime prerequisite preview",
    )
    if prerequisite_preview.get("profile_id") != profile_id:
        raise DeployError("runtime prerequisite preview profile mismatch")

    if not apply_changes:
        return runtime_root
    prerequisite_apply = _json_object(
        runner,
        [*prerequisite_command, "--apply", "--profile-id", profile_id],
        label="runtime prerequisite apply",
    )
    if prerequisite_apply.get("profile_id") != profile_id:
        raise DeployError("runtime prerequisite apply profile mismatch")
    production_apply = _json_object(
        runner,
        [*production_command, "--apply", "--profile-id", profile_id],
        label="runtime production profile apply",
    )
    if (
        production_apply.get("producer_commit") != target_sha
        or production_apply.get("profile_id") != profile_id
        or production_apply.get("profile_path") != str(profile_path)
        or production_apply.get("status") != "published"
    ):
        raise DeployError("runtime production profile publication mismatch")

    profile_command = [
        config.rquant_bin,
        "runtime-deployment-profile",
        "--profile",
        str(profile_path),
        "--runtime-root",
        str(runtime_root),
        "--expected-commit",
        target_sha,
    ]
    profile_preview = _json_object(
        runner,
        profile_command,
        label="runtime deployment profile preview",
    )
    if profile_preview.get("profile_id") != profile_id:
        raise DeployError("runtime deployment profile preview mismatch")
    profile_apply_command = [*profile_command, "--apply", "--profile-id", profile_id]
    if config.runtime_schema_v1_migration_authority is not None:
        profile_apply_command.extend(
            [
                "--schema-v1-migration-authority",
                str(config.runtime_schema_v1_migration_authority),
            ]
        )
    installed = _json_object(
        runner,
        profile_apply_command,
        label="runtime deployment profile apply",
    )
    if (
        installed.get("producer_commit") != target_sha
        or installed.get("deployment_profile_id") != profile_id
    ):
        raise DeployError("runtime deployment profile receipt mismatch")
    generation_hash = _receipt_sha256(
        installed,
        "generation_hash",
        label="runtime deployment profile apply",
    )
    transaction.runtime_root = runtime_root
    transaction.profile_applied = True
    previous_generation = installed.get("previous_generation_hash")
    if previous_generation is not None and (
        not isinstance(previous_generation, str)
        or re.fullmatch(r"[0-9a-f]{64}", previous_generation) is None
    ):
        raise DeployError("runtime deployment profile returned an invalid previous generation")

    rollout_command = [
        config.rquant_bin,
        "runtime-deployment-rollout",
        "--runtime-root",
        str(runtime_root),
        "--expected-commit",
        target_sha,
        "--profile-id",
        profile_id,
        "--generation-hash",
        generation_hash,
    ]
    if isinstance(previous_generation, str):
        rollout_command.extend(["--previous-generation-hash", previous_generation])
    rollout = _json_object(runner, rollout_command, label="runtime deployment rollout")
    if rollout.get("status") != "succeeded":
        raise DeployError("runtime deployment rollout did not succeed")
    return runtime_root


def _rollback_runtime_profile(
    config: DeployConfig,
    runner: Runner,
    *,
    failed_commit: str,
    previous_commit: str,
    deployment_operation_id: str,
) -> None:
    if config.runtime_root is None:
        return
    operation_id = hashlib.sha256(
        json.dumps(
            {
                "contract": "production-deploy-runtime-rollback/v1",
                "runtime_root": str(config.runtime_root),
                "failed_commit": failed_commit,
                "previous_commit": previous_commit,
                "deployment_operation_id": deployment_operation_id,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    payload = _json_object(
        runner,
        [
            config.rquant_bin,
            "runtime-deployment-rollback",
            "--runtime-root",
            str(config.runtime_root),
            "--failed-commit",
            failed_commit,
            "--expected-previous-commit",
            previous_commit,
            "--operation-id",
            operation_id,
        ],
        label="runtime deployment rollback",
    )
    if payload.get("status") not in {"rolled_back", "already_rolled_back"}:
        raise DeployError("runtime deployment rollback did not succeed")


def _prepare_job_center_authority(
    config: DeployConfig,
    runner: Runner,
    *,
    target_sha: str,
    runtime_root: Path,
) -> None:
    """Publish the Job Center current manifest before any production daemon can start."""

    command = [
        config.rquant_bin,
        "lab-runtime-prepare",
        "--expected-checkout-root",
        str(config.repo),
        "--trusted-git-path",
        str(config.git_path),
        "--runtime-deployment-root",
        str(runtime_root),
        "--expected-code-sha",
        target_sha,
        "--startup-deadline-monotonic",
        str(monotonic_time.monotonic() + config.command_timeout_seconds),
    ]
    runner.run(command)


def _resolve_r07_evidence_cache_dir(config: DeployConfig) -> Path:
    """The retained evidence cache directory, pinned exactly on Linux production."""

    configured = config.r07_evidence_cache_dir
    if config.release_profile == LINUX_RELEASE_PROFILE:
        if configured is not None and configured != LINUX_PRODUCTION_EVIDENCE_CACHE_DIR:
            raise PolicyError(
                "Linux production R07 evidence cache directory must be exactly "
                f"{LINUX_PRODUCTION_EVIDENCE_CACHE_DIR}"
            )
        return LINUX_PRODUCTION_EVIDENCE_CACHE_DIR
    # The Lab default lives beside the deployment lock root, never inside the worktree the
    # deployer fast-forwards.
    return configured or config.repo.parent / ".rquant-deploy" / "r07-dr-evidence"


class IsolatedR07EvidenceGate:
    """Run the R07 gate with the release interpreter, outside the ``-I -S`` deployer."""

    def __init__(self, config: DeployConfig, cache_dir: Path) -> None:
        self._config = config
        self._cache_dir = cache_dir

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    def evaluate(
        self,
        *,
        repo: Path,
        runner: Runner,
        git_path: Path,
        installed_commit_sha: str,
        target_commit_sha: str,
    ) -> R07GateDecision:
        del runner  # the child resolves Git objects itself with the trusted binary
        config = self._config
        script = Path(__file__).resolve().parents[3] / R07_DEPLOY_GATE_SCRIPT
        command = [
            str(config.python_path or sys.executable),
            "-I",
            str(script),
            "--repo",
            str(repo),
            "--trusted-git-path",
            str(git_path),
            "--evidence-cache-dir",
            str(self._cache_dir),
            "--installed-commit",
            installed_commit_sha,
            "--target-commit",
            target_commit_sha,
        ]
        deadline = config.overall_deadline_monotonic
        if deadline is None:
            deadline = monotonic_time.monotonic() + config.overall_timeout_seconds
        try:
            completed = _run_process_group(
                command,
                cwd=repo,
                deadline_monotonic=deadline,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return _r07_unavailable_decision(
                installed_commit_sha,
                target_commit_sha,
                f"the R07 deployment gate could not run: {type(exc).__name__}",
            )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "no output").strip()
            return _r07_unavailable_decision(
                installed_commit_sha,
                target_commit_sha,
                f"the R07 deployment gate failed ({completed.returncode}): {detail[:500]}",
            )
        try:
            return r07_decision_from_child_output(completed.stdout)
        except PolicyError as exc:
            return _r07_unavailable_decision(
                installed_commit_sha,
                target_commit_sha,
                f"the R07 deployment gate returned an unusable decision: {exc}",
            )


def _r07_unavailable_decision(
    installed_commit_sha: str,
    target_commit_sha: str,
    reason: str,
) -> R07GateDecision:
    return R07GateDecision(
        allowed=False,
        gate="rejected",
        reason=reason,
        requires_evidence=False,
        installed_mode="unresolved",
        target_mode="unresolved",
        installed_commit_sha=_reportable_sha(installed_commit_sha),
        installed_tree_sha="",
        target_commit_sha=_reportable_sha(target_commit_sha),
        target_tree_sha="",
    )


def _reportable_sha(value: str) -> str:
    """Never invent a commit ID for the audit: an unresolvable one is recorded as absent."""

    return value if R07_DECISION_SHA_PATTERN.fullmatch(value) else ""


def _build_r07_evidence_gate(config: DeployConfig) -> IsolatedR07EvidenceGate:
    return IsolatedR07EvidenceGate(config, _resolve_r07_evidence_cache_dir(config))


def _require_r07_evidence(
    config: DeployConfig,
    runner: Runner,
    gate: R07EvidenceGate,
    *,
    target: str,
    previous_sha: str,
    target_sha: str,
) -> R07GateDecision:
    """Resolve the Release A/B gate before any checkout or service mutation."""

    decision = gate.evaluate(
        repo=config.repo,
        runner=runner,
        git_path=config.git_path,
        installed_commit_sha=previous_sha,
        target_commit_sha=target_sha,
    )
    if decision.allowed:
        return decision
    if not config.dry_run:
        _append_audit(
            config,
            DeployResult(
                "r07_gate_failed",
                previous_sha,
                target_sha,
                target,
                (),
                (),
                (),
                decision.gate,
                decision.target_tree_sha,
            ),
            error=decision.reason,
            r07_decision=decision,
        )
    raise PolicyError(f"R07 deployment evidence gate refused the target: {decision.reason}")


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


def _append_audit(
    config: DeployConfig,
    result: DeployResult,
    *,
    error: str = "",
    r07_decision: R07GateDecision | None = None,
) -> None:
    path = _audit_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now(SHANGHAI).isoformat(),
        **asdict(result),
        "changed_files": list(result.changed_files),
        "restart_services": list(result.restart_services),
        "error": error,
        **({} if r07_decision is None else r07_decision.audit_fields()),
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


@contextmanager
def _deployment_preview_coordination(path: Path) -> Iterator[None]:
    """Take an existing lock without creating any filesystem object."""

    try:
        parent = path.parent.lstat()
        active = path.lstat()
    except FileNotFoundError:
        yield
        return
    if (
        not stat.S_ISDIR(parent.st_mode)
        or stat.S_ISLNK(parent.st_mode)
        or parent.st_uid != os.getuid()
        or stat.S_IMODE(parent.st_mode) != 0o700
        or not stat.S_ISREG(active.st_mode)
        or active.st_uid != os.getuid()
        or active.st_nlink != 1
        or stat.S_IMODE(active.st_mode) != 0o600
    ):
        raise PolicyError("production deployment preview coordination is unsafe")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(descriptor)
        named = path.lstat()
        if (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_uid, opened.st_nlink) != (
            named.st_dev,
            named.st_ino,
            named.st_mode,
            named.st_uid,
            named.st_nlink,
        ):
            raise PolicyError("production deployment preview coordination changed")
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
    runtime_profile_transaction: RuntimeProfileTransactionState | None = None,
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
    transaction = runtime_profile_transaction or RuntimeProfileTransactionState()
    runtime_root = _deploy_runtime_profile(
        config,
        runner,
        target_sha=target_sha,
        action=action,
        transaction=transaction,
    )
    if runtime_root is None and action == "rollback":
        runtime_root = config.runtime_root
    if runtime_root is not None:
        _prepare_job_center_authority(
            config,
            runner,
            target_sha=target_sha,
            runtime_root=runtime_root,
        )
    preflight_command = [config.rquant_bin, "preflight"]
    if runtime_root is not None:
        preflight_command.extend(["--runtime-root", str(runtime_root)])
    runner.run(preflight_command)
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
    runner.run(preflight_command)
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
    if config.lab_lifecycle_mode == "installed" and intent.handoff_operation_id:
        return _advance_intent(config, authority, intent, "awaiting_readiness")
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
    """Continue one recorded deployment transaction.

    Recovery never chooses a target: it replays the exact recorded intent, whose pair the
    deployment R07 gate already accepted before any mutation, and whose rollback leg the
    specification assigns to "the existing deployer's rollback to the exact previous commit and
    tree". Re-running the Release A/B table here would make a crashed rollout unrecoverable
    while Release A is current, so the audit records the recorded-intent provenance instead.
    """

    action = config.recovery_action
    if action not in {"resume", "rollback"}:
        raise PolicyError("recovery action must be resume or rollback")
    if config.prepared_intent_operation_id:
        prepared = authority.read_prepared_deployment_intent()
        if prepared.operation_id != config.prepared_intent_operation_id or prepared.stage not in {
            "planned",
            "recovery_started",
        }:
            raise PolicyError("prepared recovery intent binding is invalid")
        intent = authority.adopt_prepared_deployment_intent(
            operation_id=config.prepared_intent_operation_id
        )
    else:
        intent = authority.read_deployment_intent()
    if intent.stage == "completed":
        raise PolicyError("deployment intent is already completed")
    if config.dry_run:
        raise PolicyError("recovery does not support dry-run")
    expected_target = intent.target_sha if action == "resume" else intent.previous_sha
    allowed_refs = {expected_target}
    if action == "resume":
        allowed_refs.add(intent.target_ref)
    if config.target not in allowed_refs:
        raise PolicyError("recovery target does not match the recorded deployment intent")
    try:
        plan = validate_deployment_intent_policy(
            intent,
            release_profile=config.release_profile,
            lifecycle_mode=config.lab_lifecycle_mode,
        )
    except ReleaseGenerationError as exc:
        raise PolicyError("recorded deployment intent no longer matches deployment policy") from exc
    requires_handoff = config.lab_lifecycle_mode == "installed" and bool(plan.handoff_daemons)
    if (intent.restart_services or requires_handoff) and is_protected_market_window(config.now):
        raise ProtectedWindowError(
            "deployment recovery requires service restarts during the protected 09:15-15:10 window"
        )
    git = str(config.git_path)
    branch = _stdout(runner, [git, "rev-parse", "--abbrev-ref", "HEAD"])
    if branch != "main":
        raise PolicyError(f"production checkout must be on main, found {branch!r}")
    dirty = _stdout(runner, [git, "status", "--porcelain", "--untracked-files=no"])
    if dirty:
        raise PolicyError("tracked production worktree changes must be resolved before recovery")
    intent = _advance_intent(config, authority, intent, "recovery_started")
    if requires_handoff and config.handoff_operation_id != intent.handoff_operation_id:
        raise PolicyError("deployment handoff rebound was not durably committed before mutation")
    current_sha = _stdout(runner, [git, "rev-parse", "HEAD"])
    if (
        action == "rollback"
        and config.runtime_root is not None
        and current_sha == intent.target_sha
    ):
        _rollback_runtime_profile(
            config,
            runner,
            failed_commit=intent.target_sha,
            previous_commit=intent.previous_sha,
            deployment_operation_id=intent.operation_id,
        )
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
        "recorded_intent",
    )
    _append_audit(config, result)
    return result


def _verify_dry_run_consistency(
    config: DeployConfig,
    runner: Runner,
    *,
    target: str,
    previous_sha: str,
    target_sha: str,
) -> None:
    """Re-read every mutable generation after a lock-free deployment preview."""

    transaction = RuntimeProfileTransactionState()
    _deploy_runtime_profile(
        config,
        runner,
        target_sha=target_sha,
        action="deploy",
        transaction=transaction,
        apply_changes=False,
    )
    git = str(config.git_path)
    if _stdout(runner, [git, "rev-parse", "--abbrev-ref", "HEAD"]) != "main":
        raise PolicyError("production branch changed during dry-run")
    if _stdout(runner, [git, "status", "--porcelain", "--untracked-files=no"]):
        raise PolicyError("production worktree changed during dry-run")
    if _stdout(runner, [git, "rev-parse", "HEAD"]) != previous_sha:
        raise PolicyError("production generation changed during dry-run")
    if _stdout(runner, [git, "rev-parse", "--verify", f"{target}^{{commit}}"]) != target_sha:
        raise PolicyError("deployment target changed during dry-run")
    if config.startup_generation is not None and config.startup_generation != previous_sha:
        raise PolicyError("startup generation changed before dry-run completed")
    _deploy_runtime_profile(
        config,
        runner,
        target_sha=target_sha,
        action="deploy",
        transaction=transaction,
        apply_changes=False,
    )


def _deploy_locked(
    config: DeployConfig,
    runner: Runner,
    generation_authority: GenerationAuthority | None,
    generation_finalizer: GenerationFinalizer | None,
    gate: R07EvidenceGate,
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

    prepared_intent: DeploymentIntent | None = None
    if config.prepared_intent_operation_id:
        if generation_authority is None or generation_finalizer is None:
            raise PolicyError("prepared deployment intent requires generation authority")
        prepared_intent = generation_authority.read_prepared_deployment_intent()
        try:
            change_plan = validate_deployment_intent_policy(
                prepared_intent,
                release_profile=config.release_profile,
                lifecycle_mode=config.lab_lifecycle_mode,
                expected_handoff_operation_id=config.handoff_operation_id,
            )
        except ReleaseGenerationError as exc:
            raise PolicyError("prepared deployment intent is invalid") from exc
        previous_sha = _stdout(runner, [git, "rev-parse", "HEAD"])
        target_sha = prepared_intent.target_sha
        if (
            config.dry_run
            or config.lab_lifecycle_mode != "installed"
            or prepared_intent.operation_id != config.prepared_intent_operation_id
            or prepared_intent.stage not in {"planned", "recovery_started"}
            or prepared_intent.previous_sha != previous_sha
            or prepared_intent.target_ref != target
            or previous_sha == target_sha
            or not change_plan.changed_files
        ):
            raise PolicyError("prepared deployment intent binding is invalid")
        decision = _require_r07_evidence(
            config,
            runner,
            gate,
            target=target,
            previous_sha=previous_sha,
            target_sha=target_sha,
        )
    else:
        if not config.dry_run:
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
        decision = _require_r07_evidence(
            config,
            runner,
            gate,
            target=target,
            previous_sha=previous_sha,
            target_sha=target_sha,
        )

        if previous_sha == target_sha:
            result = DeployResult(
                "already_current",
                previous_sha,
                target_sha,
                target,
                (),
                (),
                (),
                decision.gate,
                decision.target_tree_sha,
            )
            if config.dry_run:
                _verify_dry_run_consistency(
                    config,
                    runner,
                    target=target,
                    previous_sha=previous_sha,
                    target_sha=target_sha,
                )
            else:
                if config.runtime_root is None:  # pragma: no cover - production policy guards
                    raise PolicyError("runtime root is required for production preflight")
                runner.run(
                    [config.rquant_bin, "preflight", "--runtime-root", str(config.runtime_root)]
                )
                _append_audit(config, result, r07_decision=decision)
            return result

        _check_ancestor(
            runner,
            git_path,
            previous_sha,
            target_sha,
            "target is not a fast-forward from the deployed commit",
        )
        changed_output = _stdout(
            runner,
            [git, "diff", "--name-only", f"{previous_sha}..{target_sha}"],
        )
        try:
            change_plan = validate_deployment_change_policy(
                changed_output.splitlines(),
                release_profile=config.release_profile,
                lifecycle_mode=config.lab_lifecycle_mode,
            )
        except ReleaseGenerationError as exc:
            raise PolicyError(str(exc)) from exc
    requires_handoff = config.lab_lifecycle_mode == "installed" and bool(
        change_plan.handoff_daemons
    )
    if (change_plan.restart_services or requires_handoff) and is_protected_market_window(
        config.now
    ):
        raise ProtectedWindowError(
            "release requires service restarts during the protected 09:15-15:10 window"
        )

    if config.dry_run:
        _verify_dry_run_consistency(
            config,
            runner,
            target=target,
            previous_sha=previous_sha,
            target_sha=target_sha,
        )
        result = DeployResult(
            "dry_run",
            previous_sha,
            target_sha,
            target,
            change_plan.changed_files,
            change_plan.restart_services,
            change_plan.handoff_daemons,
            decision.gate,
            decision.target_tree_sha,
        )
        return result

    if generation_authority is not None:
        if generation_finalizer is None:
            raise PolicyError("formal deployment requires isolated target generation authority")
        if prepared_intent is None:
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
        else:
            intent = generation_authority.adopt_prepared_deployment_intent(
                operation_id=prepared_intent.operation_id,
            )
        generation_authority.invalidate()
        runtime_profile_transaction = RuntimeProfileTransactionState()
        try:
            completed = _execute_transaction(
                config,
                runner,
                generation_authority,
                generation_finalizer,
                intent,
                action="deploy",
                runtime_profile_transaction=runtime_profile_transaction,
            )
        except Exception as exc:
            try:
                recovery_runner = _fresh_recovery_runner(runner)
                recovery_finalizer = generation_finalizer
                recovery_authority = generation_authority
                recovery_deadline = getattr(
                    recovery_runner,
                    "deadline_monotonic",
                    None,
                )
                if isinstance(generation_finalizer, IsolatedGenerationFinalizer):
                    if not isinstance(recovery_deadline, float):
                        raise DeployError("deployment recovery deadline is unavailable")
                    recovery_finalizer = generation_finalizer.for_recovery(recovery_deadline)
                if isinstance(generation_authority, ReleaseGenerationAuthority):
                    if not isinstance(recovery_deadline, float):
                        raise DeployError("deployment recovery deadline is unavailable")
                    recovery_authority = generation_authority.for_recovery(recovery_deadline)
                recovery = _advance_intent(
                    config,
                    recovery_authority,
                    recovery_authority.read_deployment_intent(),
                    "recovery_started",
                )
                if runtime_profile_transaction.profile_applied:
                    _rollback_runtime_profile(
                        config,
                        recovery_runner,
                        failed_commit=intent.target_sha,
                        previous_commit=intent.previous_sha,
                        deployment_operation_id=intent.operation_id,
                    )
                recovery_authority.invalidate()
                completed = _execute_transaction(
                    config,
                    recovery_runner,
                    recovery_authority,
                    recovery_finalizer,
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
            decision.gate,
            decision.target_tree_sha,
        )
        _append_audit(config, result, r07_decision=decision)
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
        decision.gate,
        decision.target_tree_sha,
    )
    _append_audit(config, result, r07_decision=decision)
    return result


def deploy(
    config: DeployConfig,
    *,
    runner: Runner | None = None,
    generation_authority: GenerationAuthority | None = None,
    generation_finalizer: GenerationFinalizer | None = None,
    r07_evidence_gate: R07EvidenceGate | None = None,
) -> DeployResult:
    validate_release_profile(config.release_profile, config.platform_name)
    runtime_profile_values = (
        config.runtime_production_inputs,
        config.runtime_profile_output_dir,
        config.runtime_root,
    )
    if any(value is not None for value in runtime_profile_values) and not all(
        value is not None for value in runtime_profile_values
    ):
        raise PolicyError("runtime production inputs, output directory, and root are one group")
    if config.release_profile == LINUX_RELEASE_PROFILE and not all(
        value is not None for value in runtime_profile_values
    ):
        raise PolicyError("Linux production requires a complete runtime profile")
    if (
        config.release_profile == LINUX_RELEASE_PROFILE
        and config.runtime_root != LINUX_PRODUCTION_RUNTIME_ROOT
    ):
        raise PolicyError(
            f"Linux production runtime root must be exactly {LINUX_PRODUCTION_RUNTIME_ROOT}"
        )
    _resolve_r07_evidence_cache_dir(config)
    for path, label in (
        (config.runtime_production_inputs, "runtime production inputs"),
        (config.runtime_profile_output_dir, "runtime profile output directory"),
        (config.runtime_root, "runtime root"),
        (config.r07_evidence_cache_dir, "R07 evidence cache directory"),
        (
            config.runtime_schema_v1_migration_authority,
            "runtime schema v1 migration authority",
        ),
    ):
        if path is not None and (not path.is_absolute() or path != Path(os.path.abspath(path))):
            raise PolicyError(f"{label} must be an absolute canonical path")
    if (
        config.runtime_schema_v1_migration_authority is not None
        and config.runtime_production_inputs is None
    ):
        raise PolicyError("schema v1 migration authority requires a runtime profile")
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
            or config.handoff_lock_fd is None
        ):
            raise PolicyError("macOS Lab deployment requires a persisted launchd handoff")
    elif config.handoff_operation_id or config.handoff_labels or config.handoff_lock_fd is not None:
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
        handoff_lock_fd=config.handoff_lock_fd,
        startup_generation=config.startup_generation,
        python_path=config.python_path,
        git_path=config.git_path,
        recovery_action=config.recovery_action,
        release_profile=config.release_profile,
        platform_name=config.platform_name,
        command_timeout_seconds=config.command_timeout_seconds,
        overall_timeout_seconds=config.overall_timeout_seconds,
        overall_deadline_monotonic=config.overall_deadline_monotonic,
        handoff_operation_id=config.handoff_operation_id,
        handoff_labels=config.handoff_labels,
        lab_lifecycle_mode=config.lab_lifecycle_mode,
        prepared_intent_operation_id=config.prepared_intent_operation_id,
        runtime_production_inputs=config.runtime_production_inputs,
        runtime_profile_output_dir=config.runtime_profile_output_dir,
        runtime_root=config.runtime_root,
        runtime_schema_v1_migration_authority=(config.runtime_schema_v1_migration_authority),
        r07_evidence_cache_dir=config.r07_evidence_cache_dir,
    )
    effective_gate = r07_evidence_gate or _build_r07_evidence_gate(effective_config)
    effective_runner = runner or SubprocessRunner(
        repo,
        trusted_git_path=effective_config.git_path,
        command_timeout_seconds=effective_config.command_timeout_seconds,
        overall_timeout_seconds=effective_config.overall_timeout_seconds,
        overall_deadline_monotonic=effective_config.overall_deadline_monotonic,
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
        if effective_config.handoff_lock_fd is not None:
            handoff_path = lock_path.with_name(f"{lock_path.stem}.handoff.lock")
            try:
                handoff_opened = os.fstat(effective_config.handoff_lock_fd)
                handoff_active = handoff_path.lstat()
            except OSError as exc:
                raise PolicyError("inherited Lab handoff lock is unavailable") from exc
            if (
                (
                    handoff_opened.st_dev,
                    handoff_opened.st_ino,
                    handoff_opened.st_mode,
                    handoff_opened.st_uid,
                    handoff_opened.st_nlink,
                )
                != (
                    handoff_active.st_dev,
                    handoff_active.st_ino,
                    handoff_active.st_mode,
                    handoff_active.st_uid,
                    handoff_active.st_nlink,
                )
                or not stat.S_ISREG(handoff_opened.st_mode)
                or handoff_opened.st_uid != os.getuid()
                or handoff_opened.st_nlink != 1
                or stat.S_IMODE(handoff_opened.st_mode) != 0o600
            ):
                raise PolicyError("inherited Lab handoff lock identity changed")
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
                    command_timeout_seconds=effective_config.command_timeout_seconds,
                    overall_deadline_monotonic=getattr(
                        effective_runner,
                        "deadline_monotonic",
                        None,
                    ),
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
            effective_gate,
        )
    coordination = (
        _deployment_preview_coordination(lock_path)
        if effective_config.dry_run
        else _deployment_lock(lock_path)
    )
    with coordination:
        return _deploy_locked(
            effective_config,
            effective_runner,
            generation_authority,
            generation_finalizer,
            effective_gate,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deploy an exact rQuant tag or commit")
    parser.add_argument("--target", required=True, help="SemVer tag or full 40-character SHA")
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--deployment-lock-path", type=Path, required=True)
    parser.add_argument("--deployment-lock-fd", type=int)
    parser.add_argument("--lab-handoff-lock-fd", type=int)
    parser.add_argument("--startup-generation", required=True)
    parser.add_argument("--trusted-git-path", type=Path, required=True)
    parser.add_argument("--python-path", type=Path, required=True)
    parser.add_argument("--uv-path", type=Path, required=True)
    parser.add_argument("--recovery-action", choices=("resume", "rollback"))
    parser.add_argument("--release-profile", choices=RELEASE_PROFILES, required=True)
    parser.add_argument("--platform-name", choices=("linux", "darwin"), required=True)
    parser.add_argument("--command-timeout-seconds", type=float, default=300)
    parser.add_argument("--overall-timeout-seconds", type=float, default=1800)
    parser.add_argument("--overall-deadline-monotonic", type=float)
    parser.add_argument("--lab-handoff-operation-id", default="")
    parser.add_argument("--prepared-intent-operation-id", default="")
    parser.add_argument("--lab-handoff-label", action="append", default=[])
    parser.add_argument(
        "--lab-lifecycle-mode",
        choices=("uninstalled", "installed"),
        default="uninstalled",
    )
    parser.add_argument("--runtime-production-inputs", type=Path)
    parser.add_argument("--runtime-profile-output-dir", type=Path)
    parser.add_argument("--runtime-root", type=Path)
    parser.add_argument("--runtime-schema-v1-migration-authority", type=Path)
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
        handoff_lock_fd=args.lab_handoff_lock_fd,
        startup_generation=args.startup_generation,
        python_path=args.python_path,
        git_path=args.trusted_git_path,
        recovery_action=args.recovery_action,
        release_profile=args.release_profile,
        platform_name=args.platform_name,
        command_timeout_seconds=args.command_timeout_seconds,
        overall_timeout_seconds=args.overall_timeout_seconds,
        overall_deadline_monotonic=args.overall_deadline_monotonic,
        handoff_operation_id=args.lab_handoff_operation_id,
        handoff_labels=tuple(args.lab_handoff_label),
        lab_lifecycle_mode=args.lab_lifecycle_mode,
        prepared_intent_operation_id=args.prepared_intent_operation_id,
        runtime_production_inputs=args.runtime_production_inputs,
        runtime_profile_output_dir=args.runtime_profile_output_dir,
        runtime_root=args.runtime_root,
        runtime_schema_v1_migration_authority=(args.runtime_schema_v1_migration_authority),
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
