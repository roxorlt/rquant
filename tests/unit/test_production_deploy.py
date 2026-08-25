"""Controlled production deployment policy and orchestration tests."""

from __future__ import annotations

import fcntl
import json
import os
import re
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import rquant.ops.production_deploy as production_deploy
import rquant.ops.r07_deploy_evidence as r07_deploy_evidence
import rquant.signal_family_differential_gate as differential_gate
from rquant.contained_subprocess import ContainedProcessError
from rquant.ops.production_deploy import (
    ALL_LONG_RUNNING_SERVICES,
    LAB_LAUNCHD_HANDOFF_LABELS,
    LINUX_PRODUCTION_RUNTIME_ROOT,
    DeployConfig,
    DeployError,
    DeployResult,
    PolicyError,
    ProtectedWindowError,
    SubprocessRunner,
    build_change_plan,
    build_parser,
    deploy,
    is_protected_market_window,
    validate_release_profile,
    validate_target,
)
from rquant.release_generation import (
    LINUX_RELEASE_PROFILE,
    MACOS_LAB_RELEASE_PROFILE,
    DeploymentIntent,
)
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_signal_family_differential_evidence import (
    RUN_ID,
    _artifact_payload,
    _artifact_zip,
    _enforced_policy_bytes,
    _FakeTransport,
    _lab_trust,
    _release_repo,
    _run_payload,
    _valid_wire_bytes,
)

ROOT = Path(__file__).resolve().parents[2]


class FakeRunner:
    def __init__(self, responses: dict[tuple[str, ...], tuple[int, str]] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[tuple[str, ...]] = []
        self.executed_calls: list[tuple[str, ...]] = []

    def for_recovery(self) -> FakeRunner:
        return self

    @staticmethod
    def _normalize(args: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        command = tuple(args)
        if command and command[0] == "/usr/bin/git":
            return ("git", *command[1:])
        return command

    def run(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        executed = tuple(args)
        key = self._normalize(args)
        self.executed_calls.append(executed)
        self.calls.append(key)
        response = self.responses.get(executed, self.responses.get(key))
        if response is None:
            response = self._runtime_profile_response(key)
        returncode, stdout = response or (0, "")
        result = subprocess.CompletedProcess(args, returncode, stdout=stdout, stderr="")
        if check and returncode != 0:
            raise subprocess.CalledProcessError(returncode, args, output=stdout, stderr="")
        return result

    @staticmethod
    def _runtime_profile_response(command: tuple[str, ...]) -> tuple[int, str] | None:
        if command[:2] == ("rquant", "runtime-production-profile"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            target_sha = command[command.index("--expected-commit") + 1]
            profile_id = "c" * 64
            return (
                0,
                json.dumps(
                    {
                        "producer_commit": target_sha,
                        "profile_id": profile_id,
                        "profile_path": str(output_dir / f"{profile_id}.json"),
                        "runtime_root": str(output_dir.parent / "runtime"),
                        "status": "published" if "--apply" in command else "dry_run",
                    }
                ),
            )
        if command[:2] == ("rquant", "runtime-production-prerequisites"):
            return (0, json.dumps({"profile_id": "c" * 64}))
        if command[:2] == ("rquant", "runtime-deployment-profile"):
            profile_id = Path(command[command.index("--profile") + 1]).stem
            if "--apply" not in command:
                return (0, json.dumps({"profile_id": profile_id}))
            return (
                0,
                json.dumps(
                    {
                        "producer_commit": command[command.index("--expected-commit") + 1],
                        "generation_hash": "d" * 64,
                        "deployment_profile_id": profile_id,
                        "previous_generation_hash": "e" * 64,
                    }
                ),
            )
        if command[:2] == ("rquant", "runtime-deployment-rollout"):
            return (0, json.dumps({"status": "succeeded"}))
        if command[:2] == ("rquant", "runtime-deployment-rollback"):
            return (0, json.dumps({"status": "rolled_back"}))
        return None


class FailingServiceHealthRunner(FakeRunner):
    def __init__(self, responses: dict[tuple[str, ...], tuple[int, str]]) -> None:
        super().__init__(responses)
        self._health_checks = 0

    def run(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        if tuple(args) == ("systemctl", "is-active", "rquant-monitor.service"):
            self._health_checks += 1
            if self._health_checks == 2:
                self.calls.append(tuple(args))
                return subprocess.CompletedProcess(args, 3, stdout="failed\n", stderr="")
        return super().run(args, check=check)


class FailedRollbackHealthRunner(FailingServiceHealthRunner):
    def run(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        if (
            tuple(args) == ("systemctl", "is-active", "rquant-monitor.service")
            and self._health_checks >= 2
        ):
            self._health_checks += 1
            self.calls.append(tuple(args))
            return subprocess.CompletedProcess(args, 3, stdout="failed\n", stderr="")
        return super().run(args, check=check)


class SimulatedDeploymentCrash(BaseException):
    pass


class CrashAfterRunner(FakeRunner):
    def __init__(
        self,
        responses: dict[tuple[str, ...], tuple[int, str]],
        *,
        command: tuple[str, ...],
        occurrence: int = 1,
    ) -> None:
        super().__init__(responses)
        self._command = command
        self._occurrence = occurrence
        self._seen = 0

    def run(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = super().run(args, check=check)
        if self._normalize(args) == self._command:
            self._seen += 1
            if self._seen == self._occurrence:
                raise SimulatedDeploymentCrash
        return result


class SequenceRunner(FakeRunner):
    def __init__(
        self,
        responses: dict[tuple[str, ...], tuple[int, str]],
        *,
        command: tuple[str, ...],
        sequence: list[tuple[int, str]],
    ) -> None:
        super().__init__(responses)
        self._command = command
        self._sequence = list(sequence)

    def run(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        if self._normalize(args) != self._command or not self._sequence:
            return super().run(args, check=check)
        self.calls.append(tuple(args))
        returncode, stdout = self._sequence.pop(0)
        result = subprocess.CompletedProcess(args, returncode, stdout=stdout, stderr="")
        if check and returncode != 0:
            raise subprocess.CalledProcessError(returncode, args, output=stdout, stderr="")
        return result


class FailingFirstJobAuthorityRunner(FakeRunner):
    def __init__(self, responses: dict[tuple[str, ...], tuple[int, str]]) -> None:
        super().__init__(responses)
        self._prepare_calls = 0

    def run(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        if tuple(args[:2]) != ("rquant", "lab-runtime-prepare"):
            return super().run(args, check=check)
        executed = tuple(args)
        self.executed_calls.append(executed)
        self.calls.append(executed)
        self._prepare_calls += 1
        returncode = 1 if self._prepare_calls == 1 else 0
        result = subprocess.CompletedProcess(args, returncode, stdout="", stderr="")
        if check and returncode != 0:
            raise subprocess.CalledProcessError(returncode, args, output="", stderr="")
        return result


class RuntimeRollbackRunner(SequenceRunner):
    def run(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        if tuple(args[:2]) == ("rquant", "runtime-deployment-rollback"):
            self.calls.append(tuple(args))
            self.executed_calls.append(tuple(args))
            return subprocess.CompletedProcess(
                args,
                0,
                stdout=json.dumps({"status": "rolled_back"}),
                stderr="",
            )
        return super().run(args, check=check)


class FakeGenerationAuthority:
    def __init__(self) -> None:
        self.events: list[tuple[str, str | None]] = []
        self.intent: DeploymentIntent | None = None

    def invalidate(self) -> None:
        self.events.append(("invalidate", None))

    def begin_deployment_intent(self, **values: object) -> DeploymentIntent:
        self.events.append(("intent", str(values["target_sha"])))
        self.intent = DeploymentIntent.create(**values)
        return self.intent

    def read_deployment_intent(self) -> DeploymentIntent:
        assert self.intent is not None
        return self.intent

    def read_prepared_deployment_intent(self) -> DeploymentIntent:
        assert self.intent is not None
        return self.intent

    def adopt_prepared_deployment_intent(self, *, operation_id: str) -> DeploymentIntent:
        assert self.intent is not None and self.intent.operation_id == operation_id
        self.events.append(("intent_adopted", operation_id))
        return self.intent

    def update_deployment_intent(
        self,
        *,
        operation_id: str,
        stage: str,
        restarted_services: tuple[str, ...] | None = None,
    ) -> DeploymentIntent:
        assert self.intent is not None and self.intent.operation_id == operation_id
        self.intent = self.intent.advance(
            stage=stage,
            restarted_services=restarted_services,
        )
        self.events.append(("stage", stage))
        return self.intent

    def rebind_deployment_handoff(
        self,
        *,
        operation_id: str,
        handoff_operation_id: str,
        handoff_labels: tuple[str, ...],
    ) -> DeploymentIntent:
        assert self.intent is not None and self.intent.operation_id == operation_id
        self.intent = self.intent.rebind_handoff(
            handoff_operation_id=handoff_operation_id,
            handoff_labels=handoff_labels,
        )
        self.events.append(("handoff", handoff_operation_id))
        return self.intent


class FakeGenerationFinalizer:
    def __init__(self, *, crash: bool = False, crash_phase: str = "publish") -> None:
        self.calls: list[tuple[str, str, str, str]] = []
        self.crash = crash
        self.crash_phase = crash_phase

    def finalize(
        self,
        *,
        expected_commit: str,
        operation_id: str,
        action: str,
        phase: str,
    ) -> object:
        self.calls.append((expected_commit, operation_id, action, phase))
        if self.crash and phase == self.crash_phase:
            raise SimulatedDeploymentCrash
        return object()


class CrashAfterStageAuthority(FakeGenerationAuthority):
    def __init__(self, stage: str) -> None:
        super().__init__()
        self._crash_stage = stage

    def update_deployment_intent(
        self,
        *,
        operation_id: str,
        stage: str,
        restarted_services: tuple[str, ...] | None = None,
    ) -> DeploymentIntent:
        intent = super().update_deployment_intent(
            operation_id=operation_id,
            stage=stage,
            restarted_services=restarted_services,
        )
        if stage == self._crash_stage:
            raise SimulatedDeploymentCrash
        return intent


def _sha(char: str) -> str:
    return char * 40


def _advance_fake_intent_to(
    authority: FakeGenerationAuthority,
    *,
    target_stage: str,
    action: str = "deploy",
    restarted_services: tuple[str, ...] | None = None,
) -> DeploymentIntent:
    assert authority.intent is not None
    stages = (
        "timers_stopped",
        f"{action}_checkout_ready",
        f"{action}_dependencies_ready",
        f"{action}_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
        "marker_published",
        "completed",
    )
    current_stage = authority.intent.stage
    start = (
        0 if current_stage in {"planned", "recovery_started"} else stages.index(current_stage) + 1
    )
    for stage in stages[start:]:
        authority.update_deployment_intent(
            operation_id=authority.intent.operation_id,
            stage=stage,
            restarted_services=restarted_services if stage == target_stage else None,
        )
        if stage == target_stage:
            assert authority.intent is not None
            return authority.intent
    raise AssertionError("fixture target stage precedes current stage")


def _complete_deployment_intent(authority: FakeGenerationAuthority) -> DeploymentIntent:
    _advance_fake_intent_to(authority, target_stage="completed")
    assert authority.intent is not None
    return authority.intent


def _base_responses(target: str = "v0.13.2") -> dict[tuple[str, ...], tuple[int, str]]:
    old_sha = _sha("a")
    new_sha = _sha("b")
    responses = {
        ("git", "rev-parse", "--abbrev-ref", "HEAD"): (0, "main\n"),
        ("git", "status", "--porcelain", "--untracked-files=no"): (0, ""),
        ("git", "fetch", "--tags", "origin", "main"): (0, ""),
        ("git", "rev-parse", "--verify", f"{target}^{{commit}}"): (0, f"{new_sha}\n"),
        ("git", "merge-base", "--is-ancestor", new_sha, "origin/main"): (0, ""),
        ("git", "rev-parse", "HEAD"): (0, f"{old_sha}\n"),
        ("git", "merge-base", "--is-ancestor", old_sha, new_sha): (0, ""),
        ("git", "diff", "--name-only", f"{old_sha}..{new_sha}"): (
            0,
            "src/rquant/preflight.py\nCHANGELOG.md\n",
        ),
    }
    if target.startswith("v"):
        responses[("git", "cat-file", "-t", target)] = (0, "tag\n")
        responses[("git", "show", f"{new_sha}:pyproject.toml")] = (
            0,
            f'[project]\nname = "rquant"\nversion = "{target[1:]}"\n',
        )
    responses.update(_r07_responses())
    return responses


def _bind_git_responses(
    responses: dict[tuple[str, ...], tuple[int, str]],
    git_path: Path,
) -> dict[tuple[str, ...], tuple[int, str]]:
    return {
        ((str(git_path), *command[1:]) if command[0] == "git" else command): response
        for command, response in responses.items()
    }


def _config(tmp_path: Path, *, target: str = "v0.13.2", dry_run: bool = False) -> DeployConfig:
    return DeployConfig(
        repo=tmp_path,
        target=target,
        dry_run=dry_run,
        now=datetime(2026, 7, 13, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        uv_bin="uv",
        rquant_bin="rquant",
        audit_path=tmp_path / "deployments.jsonl",
        runtime_production_inputs=(LINUX_PRODUCTION_RUNTIME_ROOT.parent / "runtime-inputs.json"),
        runtime_profile_output_dir=(LINUX_PRODUCTION_RUNTIME_ROOT.parent / "runtime-profiles"),
        runtime_root=LINUX_PRODUCTION_RUNTIME_ROOT,
    )


@pytest.mark.parametrize(
    "target",
    ["v0.13.2", _sha("f")],
)
def test_validate_target_accepts_semver_tag_or_full_sha(target: str) -> None:
    assert validate_target(target) == target


@pytest.mark.parametrize(
    "target",
    ["main", "origin/main", "v1", "abc1234", "v0.13.2;touch /tmp/pwned"],
)
def test_validate_target_rejects_moving_or_unsafe_refs(target: str) -> None:
    with pytest.raises(PolicyError):
        validate_target(target)


def test_change_plan_blocks_privileged_infrastructure() -> None:
    plan = build_change_plan(
        [
            "deploy/systemd/rquant-monitor.service",
            "deploy/sudoers/rquant-production-deploy",
            "src/rquant/monitor.py",
        ]
    )

    assert plan.blocked_files == (
        "deploy/sudoers/rquant-production-deploy",
        "deploy/systemd/rquant-monitor.service",
    )
    assert "rquant-monitor.service" in plan.restart_services


def test_change_plan_blocks_launchd_plist_changes_for_separate_install_rollout() -> None:
    plan = build_change_plan(
        [
            "deploy/launchd/com.roxor.rquant-lab-worker.plist",
            "src/rquant/lab_daemon.py",
        ],
        release_profile="macos-lab",
    )

    assert plan.blocked_files == ("deploy/launchd/com.roxor.rquant-lab-worker.plist",)
    assert plan.restart_services == ()
    assert plan.handoff_daemons == LAB_LAUNCHD_HANDOFF_LABELS


def test_change_plan_keeps_preflight_only_release_restart_free() -> None:
    plan = build_change_plan(
        [
            "src/rquant/preflight.py",
            "src/rquant/ops/production_deploy.py",
            "CHANGELOG.md",
            "tests/unit/test_production_deploy.py",
        ]
    )

    assert plan.blocked_files == ()
    assert plan.restart_services == ()
    assert plan.handoff_daemons == ()


def test_change_plan_restarts_all_for_shared_runtime_or_unknown_source() -> None:
    shared = build_change_plan(["src/rquant/config.py"])
    unknown = build_change_plan(["src/rquant/new_runtime.py"])

    assert shared.restart_services == ALL_LONG_RUNNING_SERVICES
    assert unknown.restart_services == ALL_LONG_RUNNING_SERVICES


def test_page_control_is_a_managed_long_running_production_service() -> None:
    assert "rquant-page-control.service" in ALL_LONG_RUNNING_SERVICES


def test_lab_daemon_change_uses_launchd_only_for_macos_release_profile() -> None:
    macos = build_change_plan(
        ["src/rquant/lab_daemon.py"],
        release_profile="macos-lab",
    )
    linux = build_change_plan(
        ["src/rquant/lab_daemon.py"],
        release_profile="linux-production",
    )

    assert macos.restart_services == ()
    assert macos.handoff_daemons == LAB_LAUNCHD_HANDOFF_LABELS
    assert linux.restart_services == ALL_LONG_RUNNING_SERVICES
    assert linux.handoff_daemons == ()


@pytest.mark.parametrize(
    ("release_profile", "platform_name"),
    [
        ("macos-lab", "linux"),
        ("linux-production", "darwin"),
        ("unknown", "darwin"),
    ],
)
def test_release_profile_platform_mismatch_fails_closed(
    release_profile: str,
    platform_name: str,
) -> None:
    with pytest.raises(PolicyError, match="release profile"):
        validate_release_profile(release_profile, platform_name)


def test_linux_production_deploy_requires_runtime_profile_before_any_command(
    tmp_path: Path,
) -> None:
    runner = FakeRunner(_base_responses())
    baseline = _config(tmp_path, dry_run=True)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "release_profile": "linux-production",
            "platform_name": "linux",
            "runtime_production_inputs": None,
            "runtime_profile_output_dir": None,
            "runtime_root": None,
        }
    )

    with pytest.raises(PolicyError, match="production.*runtime profile|required"):
        deploy(config, runner=runner)

    assert runner.calls == []


def test_linux_production_deploy_rejects_relocated_runtime_root_before_any_command(
    tmp_path: Path,
) -> None:
    runner = FakeRunner(_base_responses())
    baseline = _config(tmp_path, dry_run=True)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "release_profile": "linux-production",
            "platform_name": "linux",
            "runtime_root": Path("/srv/rquant/data/runtime"),
        }
    )

    with pytest.raises(PolicyError, match="Linux production runtime root"):
        deploy(config, runner=runner)

    assert runner.calls == []


@pytest.mark.parametrize(
    ("when", "expected"),
    [
        (datetime(2026, 7, 13, 9, 14, tzinfo=ZoneInfo("Asia/Shanghai")), False),
        (datetime(2026, 7, 13, 9, 15, tzinfo=ZoneInfo("Asia/Shanghai")), True),
        (datetime(2026, 7, 13, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai")), True),
        (datetime(2026, 7, 13, 15, 11, tzinfo=ZoneInfo("Asia/Shanghai")), False),
        (datetime(2026, 7, 12, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")), False),
    ],
)
def test_protected_market_window(when: datetime, expected: bool) -> None:
    assert is_protected_market_window(when) is expected


def test_dry_run_builds_exact_plan_without_mutating_repo(tmp_path: Path) -> None:
    runner = FakeRunner(_base_responses())

    result = deploy(_config(tmp_path, dry_run=True), runner=runner)

    assert result.status == "dry_run"
    assert result.target_sha == _sha("b")
    assert result.handoff_daemons == ()
    assert ("git", "merge", "--ff-only", _sha("b")) not in runner.calls
    assert ("uv", "sync", "--frozen") not in runner.calls


def test_dry_run_does_not_create_missing_lock_parent(tmp_path: Path) -> None:
    runner = FakeRunner(_base_responses())
    lock_path = tmp_path / "absent-coordination" / "production.lock"
    baseline = _config(tmp_path, dry_run=True)
    config = DeployConfig(**{**baseline.__dict__, "lock_path": lock_path})

    assert not lock_path.parent.exists()

    result = deploy(config, runner=runner)

    assert result.status == "dry_run"
    assert not lock_path.parent.exists()


def test_lock_free_dry_run_rejects_generation_change_during_preview(tmp_path: Path) -> None:
    class ChangingGenerationRunner(FakeRunner):
        head_reads = 0

        def run(
            self,
            args: list[str],
            *,
            check: bool = True,
        ) -> subprocess.CompletedProcess[str]:
            if self._normalize(args) == ("git", "rev-parse", "HEAD"):
                self.head_reads += 1
                if self.head_reads > 1:
                    self.calls.append(("git", "rev-parse", "HEAD"))
                    return subprocess.CompletedProcess(args, 0, stdout=f"{_sha('c')}\n", stderr="")
            return super().run(args, check=check)

    runner = ChangingGenerationRunner(_base_responses())
    lock_path = tmp_path / "absent-coordination" / "production.lock"
    baseline = _config(tmp_path, dry_run=True)
    config = DeployConfig(**{**baseline.__dict__, "lock_path": lock_path})

    with pytest.raises(PolicyError, match="generation changed"):
        deploy(config, runner=runner)

    assert not lock_path.parent.exists()


def test_all_deploy_git_commands_use_verified_absolute_git_path(tmp_path: Path) -> None:
    trusted_git = Path("/usr/bin/git")
    runner = FakeRunner(_bind_git_responses(_base_responses(), trusted_git))
    baseline = _config(tmp_path, dry_run=True)
    config = DeployConfig(**{**baseline.__dict__, "git_path": trusted_git})

    result = deploy(config, runner=runner)

    assert result.status == "dry_run"
    git_calls = [
        call
        for call in runner.executed_calls
        if call[1:2]
        in {
            ("rev-parse",),
            ("status",),
            ("fetch",),
            ("cat-file",),
            ("show",),
            ("merge-base",),
            ("diff",),
            ("merge",),
            ("reset",),
        }
    ]
    assert git_calls
    assert all(call[0] == str(trusted_git) for call in git_calls)
    assert all(call[0] != "git" for call in runner.executed_calls)


def test_deployment_refuses_to_mutate_generation_held_by_daemon(tmp_path: Path) -> None:
    lock_root = tmp_path.parent / ".rquant-deploy"
    lock_root.mkdir(mode=0o700, exist_ok=True)
    lock_path = lock_root / f"{tmp_path.name}.lock"
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    runner = FakeRunner(_base_responses())
    try:
        with pytest.raises(PolicyError, match="deployment is already running"):
            deploy(_config(tmp_path, dry_run=True), runner=runner)
    finally:
        os.close(descriptor)

    assert runner.calls == []


def test_deploy_rejects_tracked_dirty_worktree(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "status", "--porcelain", "--untracked-files=no")] = (
        0,
        " M src/rquant/monitor.py\n",
    )
    runner = FakeRunner(responses)

    with pytest.raises(PolicyError, match="tracked"):
        deploy(_config(tmp_path), runner=runner)

    assert not any(call[:2] == ("git", "fetch") for call in runner.calls)


def test_fetch_failure_never_mutates_production_checkout(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "fetch", "--tags", "origin", "main")] = (1, "network down")
    runner = FakeRunner(responses)

    with pytest.raises(subprocess.CalledProcessError):
        deploy(_config(tmp_path), runner=runner)

    assert not any(call[:2] == ("git", "merge") for call in runner.calls)
    assert not any(call[:2] == ("git", "reset") for call in runner.calls)


def test_deploy_rejects_lightweight_version_tag(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "cat-file", "-t", "v0.13.2")] = (0, "commit\n")
    runner = FakeRunner(responses)

    with pytest.raises(PolicyError, match="annotated"):
        deploy(_config(tmp_path), runner=runner)

    assert ("git", "merge", "--ff-only", _sha("b")) not in runner.calls


def test_deploy_rejects_tag_that_disagrees_with_package_version(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "show", f"{_sha('b')}:pyproject.toml")] = (
        0,
        '[project]\nname = "rquant"\nversion = "0.13.1"\n',
    )
    runner = FakeRunner(responses)

    with pytest.raises(PolicyError, match="package version"):
        deploy(_config(tmp_path), runner=runner)

    assert ("git", "merge", "--ff-only", _sha("b")) not in runner.calls


def test_deploy_refuses_restart_release_during_market_hours(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "diff", "--name-only", f"{_sha('a')}..{_sha('b')}")] = (
        0,
        "src/rquant/monitor.py\n",
    )
    runner = FakeRunner(responses)
    config = _config(tmp_path)
    config = DeployConfig(
        **{
            **config.__dict__,
            "now": datetime(2026, 7, 13, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        }
    )

    with pytest.raises(ProtectedWindowError):
        deploy(config, runner=runner)

    assert ("git", "merge", "--ff-only", _sha("b")) not in runner.calls


def test_deploy_refuses_privileged_files_before_checkout(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "diff", "--name-only", f"{_sha('a')}..{_sha('b')}")] = (
        0,
        "deploy/systemd/rquant-monitor.service\n",
    )
    runner = FakeRunner(responses)

    with pytest.raises(PolicyError, match="privileged"):
        deploy(_config(tmp_path), runner=runner)

    assert ("git", "merge", "--ff-only", _sha("b")) not in runner.calls


def test_active_affected_service_restarts_with_noninteractive_sudo(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "diff", "--name-only", f"{_sha('a')}..{_sha('b')}")] = (
        0,
        "src/rquant/monitor.py\n",
    )
    responses[("systemctl", "is-active", "rquant-monitor.service")] = (0, "active\n")
    runner = FakeRunner(responses)

    result = deploy(_config(tmp_path), runner=runner)

    assert result.restart_services == ("rquant-monitor.service",)
    assert (
        "sudo",
        "-n",
        "systemctl",
        "restart",
        "rquant-monitor.service",
    ) in runner.calls
    assert runner.calls.count(("systemctl", "is-active", "rquant-monitor.service")) == 2


def test_failed_service_health_rolls_service_back_to_old_code(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "diff", "--name-only", f"{_sha('a')}..{_sha('b')}")] = (
        0,
        "src/rquant/monitor.py\n",
    )
    responses[("systemctl", "is-active", "rquant-monitor.service")] = (0, "active\n")
    runner = FailingServiceHealthRunner(responses)

    with pytest.raises(DeployError, match="rolled back"):
        deploy(_config(tmp_path), runner=runner)

    restart = (
        "sudo",
        "-n",
        "systemctl",
        "restart",
        "rquant-monitor.service",
    )
    assert runner.calls.count(restart) == 2


def test_preexisting_failed_service_is_not_started_by_deployment(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "diff", "--name-only", f"{_sha('a')}..{_sha('b')}")] = (
        0,
        "src/rquant/monitor.py\n",
    )
    responses[("systemctl", "is-active", "rquant-monitor.service")] = (3, "failed\n")
    runner = FakeRunner(responses)

    with pytest.raises(DeployError, match="rolled back"):
        deploy(_config(tmp_path), runner=runner)

    restart = (
        "sudo",
        "-n",
        "systemctl",
        "restart",
        "rquant-monitor.service",
    )
    assert restart not in runner.calls


def test_failed_service_after_rollback_is_reported_as_rollback_failure(
    tmp_path: Path,
) -> None:
    responses = _base_responses()
    responses[("git", "diff", "--name-only", f"{_sha('a')}..{_sha('b')}")] = (
        0,
        "src/rquant/monitor.py\n",
    )
    responses[("systemctl", "is-active", "rquant-monitor.service")] = (0, "active\n")
    runner = FailedRollbackHealthRunner(responses)

    with pytest.raises(DeployError, match="rollback also failed"):
        deploy(_config(tmp_path), runner=runner)

    audit = (_config(tmp_path).audit_path).read_text(encoding="utf-8")
    assert '"status": "rollback_failed"' in audit


def test_successful_deploy_uses_exact_sha_preflight_and_audit(tmp_path: Path) -> None:
    runner = FakeRunner(_base_responses())
    authority = FakeGenerationAuthority()
    finalizer = FakeGenerationFinalizer()

    result = deploy(
        _config(tmp_path),
        runner=runner,
        generation_authority=authority,
        generation_finalizer=finalizer,
    )

    assert result.status == "deployed"
    assert result.handoff_daemons == ()
    production_profile_calls = [
        call
        for call in runner.calls
        if call[:2]
        in {
            ("rquant", "runtime-production-profile"),
            ("rquant", "runtime-production-prerequisites"),
        }
    ]
    assert production_profile_calls
    assert all(
        call[call.index("--runtime-mode") + 1] == "linux-production"
        for call in production_profile_calls
    )
    assert ("git", "merge", "--ff-only", _sha("b")) in runner.calls
    assert ("uv", "sync", "--frozen") in runner.calls
    assert (
        runner.calls.count(
            ("rquant", "preflight", "--runtime-root", str(LINUX_PRODUCTION_RUNTIME_ROOT))
        )
        == 2
    )
    audit = (_config(tmp_path).audit_path).read_text(encoding="utf-8")
    assert '"status": "deployed"' in audit
    assert f'"target_sha": "{_sha("b")}"' in audit
    assert authority.events[0:2] == [("intent", _sha("b")), ("invalidate", None)]
    assert authority.intent is not None and authority.intent.stage == "completed"
    assert finalizer.calls == [
        (_sha("b"), authority.intent.operation_id, "deploy", "publish"),
        (_sha("b"), authority.intent.operation_id, "deploy", "commit"),
    ]


def test_successful_deploy_runs_bound_runtime_profile_preview_apply_and_rollout(
    tmp_path: Path,
) -> None:
    inputs = tmp_path / "production-runtime-inputs.json"
    profiles = tmp_path / "runtime-profiles"
    runtime_root = tmp_path / "runtime"
    profile_id = "c" * 64
    generation = "d" * 64
    profile_path = profiles / f"{profile_id}.json"
    migration_authority = tmp_path / "schema-v1-migration-authority.json"
    target_sha = _sha("b")
    responses = _base_responses()
    production_profile = (
        "rquant",
        "runtime-production-profile",
        "--inputs",
        str(inputs),
        "--output-dir",
        str(profiles),
        "--expected-commit",
        target_sha,
    )
    prerequisite_preview = (
        "rquant",
        "runtime-production-prerequisites",
        "--inputs",
        str(inputs),
        "--expected-commit",
        target_sha,
    )
    prerequisite_apply = (
        *prerequisite_preview,
        "--apply",
        "--profile-id",
        profile_id,
    )
    production_apply = (
        *production_profile,
        "--apply",
        "--profile-id",
        profile_id,
    )
    profile_preview = (
        "rquant",
        "runtime-deployment-profile",
        "--profile",
        str(profile_path),
        "--runtime-root",
        str(runtime_root),
        "--expected-commit",
        target_sha,
    )
    profile_apply = (
        *profile_preview,
        "--apply",
        "--profile-id",
        profile_id,
        "--schema-v1-migration-authority",
        str(migration_authority),
    )
    rollout = (
        "rquant",
        "runtime-deployment-rollout",
        "--runtime-root",
        str(runtime_root),
        "--expected-commit",
        target_sha,
        "--profile-id",
        profile_id,
        "--generation-hash",
        generation,
    )
    responses[production_profile] = (
        0,
        json.dumps(
            {
                "producer_commit": target_sha,
                "profile_id": profile_id,
                "profile_path": str(profile_path),
                "runtime_root": str(runtime_root),
                "status": "dry_run",
            }
        ),
    )
    responses[prerequisite_preview] = (0, json.dumps({"profile_id": profile_id}))
    responses[prerequisite_apply] = (0, json.dumps({"profile_id": profile_id}))
    responses[production_apply] = (
        0,
        json.dumps(
            {
                "producer_commit": target_sha,
                "profile_id": profile_id,
                "profile_path": str(profile_path),
                "status": "published",
            }
        ),
    )
    responses[profile_preview] = (0, json.dumps({"profile_id": profile_id}))
    responses[profile_apply] = (
        0,
        json.dumps(
            {
                "producer_commit": target_sha,
                "generation_hash": generation,
                "deployment_profile_id": profile_id,
                "previous_generation_hash": None,
            }
        ),
    )
    responses[rollout] = (0, json.dumps({"status": "succeeded"}))
    runner = FakeRunner(responses)
    baseline = _config(tmp_path)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "runtime_production_inputs": inputs,
            "runtime_profile_output_dir": profiles,
            "runtime_root": runtime_root,
            "release_profile": "macos-lab",
            "platform_name": "darwin",
            "runtime_schema_v1_migration_authority": migration_authority,
        }
    )

    result = deploy(
        config,
        runner=runner,
        generation_authority=FakeGenerationAuthority(),
        generation_finalizer=FakeGenerationFinalizer(),
    )

    assert result.status == "deployed"
    assert runner.calls.index(prerequisite_preview) < runner.calls.index(prerequisite_apply)
    assert runner.calls.index(prerequisite_apply) < runner.calls.index(production_apply)
    assert runner.calls.index(production_apply) < runner.calls.index(profile_preview)
    assert runner.calls.index(profile_preview) < runner.calls.index(profile_apply)
    assert "--schema-v1-migration-authority" not in profile_preview
    assert runner.calls.index(profile_apply) < runner.calls.index(rollout)
    prepare_calls = [
        call
        for call in runner.calls
        if len(call) >= 2 and call[:2] == ("rquant", "lab-runtime-prepare")
    ]
    assert len(prepare_calls) == 1
    assert "--runtime-deployment-root" in prepare_calls[0]
    assert str(runtime_root) in prepare_calls[0]
    first_preflight = runner.calls.index(
        ("rquant", "preflight", "--runtime-root", str(runtime_root))
    )
    assert runner.calls.index(rollout) < runner.calls.index(prepare_calls[0]) < first_preflight
    assert runner.calls.count(("rquant", "preflight", "--runtime-root", str(runtime_root))) == 2


def test_runtime_profile_dry_run_calculates_without_publish_apply_or_rollout(
    tmp_path: Path,
) -> None:
    inputs = tmp_path / "production-runtime-inputs.json"
    profiles = tmp_path / "runtime-profiles"
    runtime_root = tmp_path / "runtime"
    profile_id = "c" * 64
    profile_path = profiles / f"{profile_id}.json"
    target_sha = _sha("b")
    responses = _base_responses()
    production_profile = (
        "rquant",
        "runtime-production-profile",
        "--inputs",
        str(inputs),
        "--output-dir",
        str(profiles),
        "--expected-commit",
        target_sha,
    )
    prerequisite_preview = (
        "rquant",
        "runtime-production-prerequisites",
        "--inputs",
        str(inputs),
        "--expected-commit",
        target_sha,
    )
    profile_preview = (
        "rquant",
        "runtime-deployment-profile",
        "--profile",
        str(profile_path),
        "--runtime-root",
        str(runtime_root),
        "--expected-commit",
        target_sha,
    )
    responses[production_profile] = (
        0,
        json.dumps(
            {
                "producer_commit": target_sha,
                "profile_id": profile_id,
                "profile_path": str(profile_path),
                "runtime_root": str(runtime_root),
                "status": "dry_run",
            }
        ),
    )
    responses[prerequisite_preview] = (0, json.dumps({"profile_id": profile_id}))
    responses[profile_preview] = (0, json.dumps({"profile_id": profile_id}))
    runner = FakeRunner(responses)
    baseline = _config(tmp_path, dry_run=True)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "runtime_production_inputs": inputs,
            "runtime_profile_output_dir": profiles,
            "runtime_root": runtime_root,
            "release_profile": "macos-lab",
            "platform_name": "darwin",
            "runtime_schema_v1_migration_authority": (
                tmp_path / "schema-v1-migration-authority.json"
            ),
        }
    )

    result = deploy(config, runner=runner)

    assert result.status == "dry_run"
    assert production_profile in runner.calls
    assert prerequisite_preview in runner.calls
    assert profile_preview not in runner.calls
    assert not any("--apply" in call for call in runner.calls)
    assert not any(call[:2] == ("rquant", "runtime-deployment-rollout") for call in runner.calls)
    assert not any("--schema-v1-migration-authority" in call for call in runner.calls)


def test_runtime_profile_dry_run_leaves_filesystem_byte_identical(tmp_path: Path) -> None:
    inputs = tmp_path / "production-runtime-inputs.json"
    profiles = tmp_path / "runtime-profiles"
    runtime_root = tmp_path / "runtime"
    lock_path = tmp_path / "deploy.lock"
    lock_path.write_bytes(b"stable-lock")
    lock_path.chmod(0o600)
    profile_id = "c" * 64
    target_sha = _sha("b")
    profile_path = profiles / f"{profile_id}.json"
    responses = _base_responses()
    responses[
        (
            "rquant",
            "runtime-production-profile",
            "--inputs",
            str(inputs),
            "--output-dir",
            str(profiles),
            "--expected-commit",
            target_sha,
        )
    ] = (
        0,
        json.dumps(
            {
                "producer_commit": target_sha,
                "profile_id": profile_id,
                "profile_path": str(profile_path),
                "runtime_root": str(runtime_root),
                "status": "dry_run",
            }
        ),
    )
    responses[
        (
            "rquant",
            "runtime-production-prerequisites",
            "--inputs",
            str(inputs),
            "--expected-commit",
            target_sha,
        )
    ] = (0, json.dumps({"profile_id": profile_id}))
    responses[
        (
            "rquant",
            "runtime-deployment-profile",
            "--profile",
            str(profile_path),
            "--runtime-root",
            str(runtime_root),
            "--expected-commit",
            target_sha,
        )
    ] = (0, json.dumps({"profile_id": profile_id}))
    baseline = _config(tmp_path, dry_run=True)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "lock_path": lock_path,
            "runtime_production_inputs": inputs,
            "runtime_profile_output_dir": profiles,
            "runtime_root": runtime_root,
            "release_profile": "macos-lab",
            "platform_name": "darwin",
        }
    )

    def snapshot() -> dict[str, tuple[str, bytes]]:
        return {
            str(path.relative_to(tmp_path)): (
                "dir" if path.is_dir() else "file",
                b"" if path.is_dir() else path.read_bytes(),
            )
            for path in sorted(tmp_path.rglob("*"))
        }

    before = snapshot()
    runner = FakeRunner(responses)
    result = deploy(config, runner=runner)

    assert result.status == "dry_run"
    assert not any(call[:2] == ("git", "fetch") for call in runner.calls)
    assert snapshot() == before


def test_already_current_dry_run_profiles_target_without_writing(tmp_path: Path) -> None:
    enforced = _r07_policy_bytes(enforced_predecessor=(_sha("b"), R07_TARGET_TREE))
    responses = _base_responses()
    responses[("git", "rev-parse", "HEAD")] = (0, f"{_sha('b')}\n")
    responses.update(
        _r07_responses(installed_sha=_sha("b"), installed_policy=enforced, target_policy=enforced)
    )
    responses[("git", "rev-parse", "--verify", f"{_sha('b')}^{{tree}}")] = (
        0,
        f"{R07_TARGET_TREE}\n",
    )
    lock_path = tmp_path / "deploy.lock"
    lock_path.write_bytes(b"stable-lock")
    lock_path.chmod(0o600)
    baseline = _config(tmp_path, dry_run=True)
    config = DeployConfig(**{**baseline.__dict__, "lock_path": lock_path})
    runner = FakeRunner(responses)
    _seed_r07_cache(
        tmp_path,
        commit_sha=_sha("b"),
        tree_sha=R07_TARGET_TREE,
        policy=enforced,
    )
    # A retained entry still owes the fixed channel its run identity, so the cache hit is
    # served the workflow-runs query and nothing else.
    gate = _r07_gate(tmp_path, transport=_r07_run_identity_transport(_sha("b")))

    before = {
        str(path.relative_to(tmp_path)): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    result = deploy(config, runner=runner, r07_evidence_gate=gate)
    after = {
        str(path.relative_to(tmp_path)): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert result.status == "already_current"
    assert result.r07_gate == "enforced"
    assert any(call[:2] == ("rquant", "runtime-production-profile") for call in runner.calls)
    assert after == before


def test_failed_profile_bound_preflight_restores_runtime_before_code_rollback(
    tmp_path: Path,
) -> None:
    inputs = tmp_path / "production-runtime-inputs.json"
    profiles = tmp_path / "runtime-profiles"
    runtime_root = tmp_path / "runtime"
    profile_id = "c" * 64
    generation = "d" * 64
    previous_generation = "e" * 64
    profile_path = profiles / f"{profile_id}.json"
    target_sha = _sha("b")
    responses = _base_responses()
    production_profile = (
        "rquant",
        "runtime-production-profile",
        "--inputs",
        str(inputs),
        "--output-dir",
        str(profiles),
        "--expected-commit",
        target_sha,
    )
    prerequisite_preview = (
        "rquant",
        "runtime-production-prerequisites",
        "--inputs",
        str(inputs),
        "--expected-commit",
        target_sha,
    )
    profile_preview = (
        "rquant",
        "runtime-deployment-profile",
        "--profile",
        str(profile_path),
        "--runtime-root",
        str(runtime_root),
        "--expected-commit",
        target_sha,
    )
    rollout = (
        "rquant",
        "runtime-deployment-rollout",
        "--runtime-root",
        str(runtime_root),
        "--expected-commit",
        target_sha,
        "--profile-id",
        profile_id,
        "--generation-hash",
        generation,
        "--previous-generation-hash",
        previous_generation,
    )
    responses.update(
        {
            production_profile: (
                0,
                json.dumps(
                    {
                        "producer_commit": target_sha,
                        "profile_id": profile_id,
                        "profile_path": str(profile_path),
                        "runtime_root": str(runtime_root),
                        "status": "dry_run",
                    }
                ),
            ),
            prerequisite_preview: (0, json.dumps({"profile_id": profile_id})),
            (*prerequisite_preview, "--apply", "--profile-id", profile_id): (
                0,
                json.dumps({"profile_id": profile_id}),
            ),
            profile_preview: (0, json.dumps({"profile_id": profile_id})),
            (*profile_preview, "--apply", "--profile-id", profile_id): (
                0,
                json.dumps(
                    {
                        "producer_commit": target_sha,
                        "generation_hash": generation,
                        "deployment_profile_id": profile_id,
                        "previous_generation_hash": previous_generation,
                    }
                ),
            ),
            rollout: (0, json.dumps({"status": "succeeded"})),
            ("rquant", "preflight", "--runtime-root", str(runtime_root)): (0, ""),
        }
    )
    preflight = ("rquant", "preflight", "--runtime-root", str(runtime_root))
    runner = RuntimeRollbackRunner(
        responses,
        command=preflight,
        sequence=[(1, "profile recovery preflight failed")],
    )
    baseline = _config(tmp_path)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "runtime_production_inputs": inputs,
            "runtime_profile_output_dir": profiles,
            "runtime_root": runtime_root,
            "release_profile": "macos-lab",
            "platform_name": "darwin",
        }
    )

    with pytest.raises(DeployError, match="rolled back"):
        deploy(
            config,
            runner=runner,
            generation_authority=FakeGenerationAuthority(),
            generation_finalizer=FakeGenerationFinalizer(),
        )

    runtime_rollback_index = next(
        index
        for index, command in enumerate(runner.calls)
        if command[:2] == ("rquant", "runtime-deployment-rollback")
    )
    code_rollback_index = runner.calls.index(("git", "reset", "--hard", _sha("a")))
    assert runtime_rollback_index < code_rollback_index


def test_macos_lab_profile_never_invokes_systemctl(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "diff", "--name-only", f"{_sha('a')}..{_sha('b')}")] = (
        0,
        "src/rquant/lab_daemon.py\n",
    )
    baseline = _config(tmp_path)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "release_profile": "macos-lab",
            "platform_name": "darwin",
            "lab_lifecycle_mode": "installed",
            "handoff_operation_id": "d" * 32,
            "handoff_labels": LAB_LAUNCHD_HANDOFF_LABELS,
            "handoff_lock_fd": 9,
        }
    )
    runner = FakeRunner(responses)
    authority = FakeGenerationAuthority()
    finalizer = FakeGenerationFinalizer()

    result = deploy(
        config,
        runner=runner,
        generation_authority=authority,
        generation_finalizer=finalizer,
    )

    assert result.status == "deployed"
    assert result.handoff_daemons == LAB_LAUNCHD_HANDOFF_LABELS
    assert authority.intent is not None
    assert authority.intent.stage == "awaiting_readiness"
    assert authority.intent.restart_services == ()
    assert [call[3] for call in finalizer.calls] == ["publish"]
    assert not any("systemctl" in command for command in runner.calls)


def test_installed_deployer_consumes_precreated_typed_intent_without_refetch(
    tmp_path: Path,
) -> None:
    baseline = _config(tmp_path)
    handoff_operation_id = "d" * 32
    authority = FakeGenerationAuthority()
    authority.intent = DeploymentIntent.create(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref=baseline.target,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="e" * 64,
        handoff_operation_id=handoff_operation_id,
        handoff_labels=LAB_LAUNCHD_HANDOFF_LABELS,
    )
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "release_profile": "macos-lab",
            "platform_name": "darwin",
            "lab_lifecycle_mode": "installed",
            "handoff_operation_id": handoff_operation_id,
            "handoff_labels": LAB_LAUNCHD_HANDOFF_LABELS,
            "handoff_lock_fd": 9,
            "prepared_intent_operation_id": authority.intent.operation_id,
        }
    )
    runner = FakeRunner(_base_responses())
    finalizer = FakeGenerationFinalizer()

    result = deploy(
        config,
        runner=runner,
        generation_authority=authority,
        generation_finalizer=finalizer,
    )

    assert result.status == "deployed"
    assert authority.events[:2] == [
        ("intent_adopted", authority.intent.operation_id),
        ("invalidate", None),
    ]
    assert not [event for event in authority.events if event[0] == "intent"]
    assert not [call for call in runner.calls if call[0:2] == ("git", "fetch")]
    assert not [call for call in runner.calls if call[0:3] == ("git", "diff", "--name-only")]
    assert not [call for call in runner.calls if call[0:3] == ("git", "cat-file", "-t")]


@pytest.mark.parametrize("recovery_action", ("resume", "rollback"))
def test_recovery_atomically_adopts_prepared_only_intent_before_rebinding(
    tmp_path: Path,
    recovery_action: str,
) -> None:
    class PreparedOnlyAuthority(FakeGenerationAuthority):
        def __init__(self, prepared: DeploymentIntent) -> None:
            super().__init__()
            self.prepared = prepared
            self.intent = None

        def read_deployment_intent(self) -> DeploymentIntent:
            assert self.intent is not None, "active intent must not be read before adoption"
            return self.intent

        def read_prepared_deployment_intent(self) -> DeploymentIntent:
            return self.prepared

        def adopt_prepared_deployment_intent(self, *, operation_id: str) -> DeploymentIntent:
            assert operation_id == self.prepared.operation_id
            self.intent = self.prepared
            self.events.append(("intent_adopted", operation_id))
            return self.prepared

    original_handoff = "d" * 32
    recovery_handoff = "e" * 32
    prepared = DeploymentIntent.create(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref="v0.13.2",
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="f" * 64,
        handoff_operation_id=original_handoff,
        handoff_labels=LAB_LAUNCHD_HANDOFF_LABELS,
    )
    authority = PreparedOnlyAuthority(prepared)
    baseline = _config(tmp_path)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "release_profile": "macos-lab",
            "platform_name": "darwin",
            "lab_lifecycle_mode": "installed",
            "target": (
                prepared.target_ref if recovery_action == "resume" else prepared.previous_sha
            ),
            "recovery_action": recovery_action,
            "handoff_operation_id": recovery_handoff,
            "handoff_labels": LAB_LAUNCHD_HANDOFF_LABELS,
            "handoff_lock_fd": 9,
            "prepared_intent_operation_id": prepared.operation_id,
        }
    )
    responses = _base_responses()
    expected_target = prepared.target_sha if recovery_action == "resume" else prepared.previous_sha
    responses[("git", "rev-parse", "HEAD")] = (0, f"{expected_target}\n")

    with pytest.raises(PolicyError, match="durably committed"):
        deploy(
            config,
            runner=FakeRunner(responses),
            generation_authority=authority,
            generation_finalizer=FakeGenerationFinalizer(),
        )

    assert authority.events[0] == ("intent_adopted", prepared.operation_id)
    assert authority.intent is not None
    assert authority.intent.initial_handoff_operation_id == original_handoff
    assert authority.intent.handoff_operation_id == original_handoff
    assert ("invalidate", None) not in authority.events


def test_recovery_rebinds_persisted_successor_after_bootstrap_crash(
    tmp_path: Path,
) -> None:
    original_handoff = "d" * 32
    recovery_handoff = "e" * 32
    authority = FakeGenerationAuthority()
    authority.intent = DeploymentIntent.create(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref="v0.13.2",
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="f" * 64,
        handoff_operation_id=original_handoff,
        handoff_labels=LAB_LAUNCHD_HANDOFF_LABELS,
    )
    baseline = _config(tmp_path)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "release_profile": "macos-lab",
            "platform_name": "darwin",
            "lab_lifecycle_mode": "installed",
            "target": authority.intent.target_ref,
            "recovery_action": "resume",
            "handoff_operation_id": recovery_handoff,
            "handoff_labels": LAB_LAUNCHD_HANDOFF_LABELS,
            "handoff_lock_fd": 9,
        }
    )
    responses = _base_responses()
    responses[("git", "rev-parse", "HEAD")] = (0, f"{authority.intent.target_sha}\n")

    with pytest.raises(PolicyError, match="durably committed"):
        deploy(
            config,
            runner=FakeRunner(responses),
            generation_authority=authority,
            generation_finalizer=FakeGenerationFinalizer(),
        )

    assert authority.intent is not None
    assert authority.intent.initial_handoff_operation_id == original_handoff
    assert authority.intent.handoff_operation_id == original_handoff
    assert not [
        event for event in authority.intent.stage_history if event["stage"] == "handoff_rebound"
    ]
    assert ("invalidate", None) not in authority.events


def test_installed_finalizer_inherits_outer_generation_and_handoff_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_root = tmp_path.parent / ".rquant-deploy"
    lock_root.mkdir(mode=0o700, exist_ok=True)
    generation_lock = lock_root / f"{tmp_path.name}.lock"
    handoff_lock = lock_root / f"{tmp_path.name}.handoff.lock"
    generation_fd = os.open(generation_lock, os.O_RDWR | os.O_CREAT, 0o600)
    handoff_fd = os.open(handoff_lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(generation_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(handoff_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    captured: dict[str, object] = {}
    operation_id = "c" * 32

    def fake_process_group(
        args: list[str],
        *,
        cwd: Path,
        deadline_monotonic: float,
        check: bool,
        pass_fds: tuple[int, ...] = (),
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        captured.update(
            args=args,
            cwd=cwd,
            deadline_monotonic=deadline_monotonic,
            check=check,
            pass_fds=pass_fds,
            env=env,
        )
        return subprocess.CompletedProcess(
            args,
            0,
            stdout=f'{{"commit":"{_sha("b")}","operation_id":"{operation_id}"}}',
            stderr="",
        )

    monkeypatch.setattr(production_deploy, "_run_process_group", fake_process_group)
    baseline = _config(tmp_path)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "lock_path": generation_lock,
            "lock_fd": generation_fd,
            "handoff_lock_fd": handoff_fd,
            "python_path": Path(sys.executable),
            "git_path": Path("/usr/bin/git"),
            "release_profile": "macos-lab",
            "platform_name": "darwin",
            "lab_lifecycle_mode": "installed",
            "handoff_operation_id": "d" * 32,
            "handoff_labels": LAB_LAUNCHD_HANDOFF_LABELS,
            "overall_deadline_monotonic": time.monotonic() + 30,
        }
    )
    try:
        result = production_deploy.IsolatedGenerationFinalizer(config).finalize(
            expected_commit=_sha("b"),
            operation_id=operation_id,
            action="deploy",
            phase="publish",
        )
    finally:
        os.close(handoff_fd)
        os.close(generation_fd)

    arguments = captured["args"]
    assert isinstance(arguments, list)
    assert "--finalize-generation" in arguments
    assert arguments[arguments.index("--inherited-handoff-lock-fd") + 1] == str(handoff_fd)
    assert captured["pass_fds"] == (generation_fd, handoff_fd)
    assert result["commit"] == _sha("b")


@pytest.mark.parametrize(
    ("command", "occurrence"),
    [
        (("git", "merge", "--ff-only", _sha("b")), 1),
        (("uv", "sync", "--frozen"), 1),
        (("rquant", "preflight"), 1),
        (("rquant", "preflight"), 2),
    ],
)
def test_interrupted_deployment_phase_leaves_generation_unpublished(
    tmp_path: Path,
    command: tuple[str, ...],
    occurrence: int,
) -> None:
    resolved_command = (
        (*command, "--runtime-root", str(LINUX_PRODUCTION_RUNTIME_ROOT))
        if command == ("rquant", "preflight")
        else command
    )
    runner = CrashAfterRunner(
        _base_responses(),
        command=resolved_command,
        occurrence=occurrence,
    )
    authority = FakeGenerationAuthority()
    finalizer = FakeGenerationFinalizer()

    with pytest.raises(SimulatedDeploymentCrash):
        deploy(
            _config(tmp_path),
            runner=runner,
            generation_authority=authority,
            generation_finalizer=finalizer,
        )

    assert authority.events[0:2] == [("intent", _sha("b")), ("invalidate", None)]
    assert finalizer.calls == []


@pytest.mark.parametrize(
    "stage",
    [
        "timers_stopped",
        "deploy_checkout_ready",
        "deploy_dependencies_ready",
        "deploy_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
        "marker_published",
        "completed",
    ],
)
def test_every_durable_stage_interruption_is_resumable_without_commit(
    tmp_path: Path,
    stage: str,
) -> None:
    authority = CrashAfterStageAuthority(stage)
    finalizer = FakeGenerationFinalizer()

    with pytest.raises(SimulatedDeploymentCrash):
        deploy(
            _config(tmp_path),
            runner=FakeRunner(_base_responses()),
            generation_authority=authority,
            generation_finalizer=finalizer,
        )

    assert authority.intent is not None
    assert authority.intent.stage == stage
    assert all(call[3] != "commit" for call in finalizer.calls)
    expected_publish = stage in {"marker_published", "completed"}
    assert bool(finalizer.calls) is expected_publish


def test_interrupted_marker_publication_does_not_claim_complete_generation(
    tmp_path: Path,
) -> None:
    runner = FakeRunner(_base_responses())
    authority = FakeGenerationAuthority()
    finalizer = FakeGenerationFinalizer(crash=True)

    with pytest.raises(SimulatedDeploymentCrash):
        deploy(
            _config(tmp_path),
            runner=runner,
            generation_authority=authority,
            generation_finalizer=finalizer,
        )

    assert authority.events == [
        ("intent", _sha("b")),
        ("invalidate", None),
        ("stage", "timers_stopped"),
        ("stage", "deploy_checkout_ready"),
        ("stage", "deploy_dependencies_ready"),
        ("stage", "deploy_preflight_ready"),
        ("stage", "services_transitioning"),
        ("stage", "services_ready"),
        ("stage", "post_restart_preflight_ready"),
        ("stage", "timers_restored"),
    ]


def test_intent_is_durable_before_marker_invalidation(tmp_path: Path) -> None:
    class CrashOnInvalidate(FakeGenerationAuthority):
        def invalidate(self) -> None:
            assert self.intent is not None
            assert self.intent.stage == "planned"
            super().invalidate()
            raise SimulatedDeploymentCrash

    authority = CrashOnInvalidate()

    with pytest.raises(SimulatedDeploymentCrash):
        deploy(
            _config(tmp_path),
            runner=FakeRunner(_base_responses()),
            generation_authority=authority,
            generation_finalizer=FakeGenerationFinalizer(),
        )

    assert authority.events[:2] == [
        ("intent", _sha("b")),
        ("invalidate", None),
    ]


def test_completed_recovery_intent_is_an_explicit_noop_without_external_mutation(
    tmp_path: Path,
) -> None:
    authority = FakeGenerationAuthority()
    authority.begin_deployment_intent(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref="v0.13.2",
        changed_files=("src/rquant/preflight.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="d" * 64,
    )
    completed = _complete_deployment_intent(authority)
    events_before = list(authority.events)
    runner = FakeRunner()
    finalizer = FakeGenerationFinalizer()
    baseline = _config(tmp_path)
    config = DeployConfig(**{**baseline.__dict__, "recovery_action": "resume"})

    with pytest.raises(PolicyError, match="completed|already"):
        deploy(
            config,
            runner=runner,
            generation_authority=authority,
            generation_finalizer=finalizer,
        )

    assert authority.intent == completed
    assert authority.events == events_before
    assert runner.calls == []
    assert finalizer.calls == []


def test_recovery_uses_recorded_plan_after_origin_advances(tmp_path: Path) -> None:
    authority = FakeGenerationAuthority()
    authority.begin_deployment_intent(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref="v0.13.2",
        changed_files=("src/rquant/monitor.py",),
        restart_services=("rquant-monitor.service",),
        active_services=("rquant-monitor.service",),
        active_timers=("rquant-monitor.timer",),
        marker_generation="marker-a",
    )
    _advance_fake_intent_to(
        authority,
        target_stage="services_transitioning",
        restarted_services=("rquant-monitor.service",),
    )
    responses = {
        ("git", "rev-parse", "--abbrev-ref", "HEAD"): (0, "main\n"),
        ("git", "status", "--porcelain", "--untracked-files=no"): (0, ""),
        ("git", "rev-parse", "HEAD"): (0, f"{_sha('b')}\n"),
        ("systemctl", "is-active", "rquant-monitor.service"): (0, "active\n"),
        ("systemctl", "is-active", "rquant-monitor.timer"): (0, "active\n"),
    }
    runner = FakeRunner(responses)
    finalizer = FakeGenerationFinalizer()
    baseline = _config(tmp_path)
    config = DeployConfig(**{**baseline.__dict__, "recovery_action": "resume"})

    result = deploy(
        config,
        runner=runner,
        generation_authority=authority,
        generation_finalizer=finalizer,
    )

    assert result.status == "recovered"
    assert not any(call[:2] == ("git", "fetch") for call in runner.calls)
    assert not any("origin/main" in call for call in runner.calls)
    assert finalizer.calls == [
        (_sha("b"), authority.intent.operation_id, "resume", "publish"),
        (_sha("b"), authority.intent.operation_id, "resume", "commit"),
    ]


def test_recovery_records_start_and_invalidates_before_first_external_mutation(
    tmp_path: Path,
) -> None:
    authority = FakeGenerationAuthority()
    authority.begin_deployment_intent(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref="v0.13.2",
        changed_files=("src/rquant/monitor.py",),
        restart_services=("rquant-monitor.service",),
        active_services=("rquant-monitor.service",),
        active_timers=("rquant-monitor.timer",),
        marker_generation="marker-a",
    )
    _advance_fake_intent_to(
        authority,
        target_stage="services_transitioning",
    )

    class OrderedRecoveryRunner(FakeRunner):
        def run(
            self,
            args: list[str],
            *,
            check: bool = True,
        ) -> subprocess.CompletedProcess[str]:
            command = self._normalize(args)
            mutating = (
                command[:4] == ("sudo", "-n", "systemctl", "stop")
                or command[:2] in {("git", "merge"), ("git", "reset")}
                or command[:2] == ("uv", "sync")
                or command[:2] == ("rquant", "preflight")
                or command[:4] == ("sudo", "-n", "systemctl", "restart")
                or command[:4] == ("sudo", "-n", "systemctl", "start")
            )
            if mutating:
                assert (
                    authority.events[-2:]
                    == [
                        ("stage", "recovery_started"),
                        ("invalidate", None),
                    ]
                    or ("stage", "recovery_started") in authority.events
                )
            return super().run(args, check=check)

    responses = {
        ("git", "rev-parse", "--abbrev-ref", "HEAD"): (0, "main\n"),
        ("git", "status", "--porcelain", "--untracked-files=no"): (0, ""),
        ("git", "rev-parse", "HEAD"): (0, f"{_sha('b')}\n"),
        ("systemctl", "is-active", "rquant-monitor.service"): (0, "active\n"),
        ("systemctl", "is-active", "rquant-monitor.timer"): (0, "active\n"),
    }
    baseline = _config(tmp_path)
    config = DeployConfig(**{**baseline.__dict__, "recovery_action": "resume"})

    deploy(
        config,
        runner=OrderedRecoveryRunner(responses),
        generation_authority=authority,
        generation_finalizer=FakeGenerationFinalizer(),
    )

    recovery_index = authority.events.index(("stage", "recovery_started"))
    assert authority.events[recovery_index + 1] == ("invalidate", None)


def test_recovery_audit_failure_prevents_invalidation_and_external_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = FakeGenerationAuthority()
    authority.begin_deployment_intent(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref="v0.13.2",
        changed_files=("src/rquant/monitor.py",),
        restart_services=("rquant-monitor.service",),
        active_services=("rquant-monitor.service",),
        active_timers=("rquant-monitor.timer",),
        marker_generation="marker-a",
    )
    responses = {
        ("git", "rev-parse", "--abbrev-ref", "HEAD"): (0, "main\n"),
        ("git", "status", "--porcelain", "--untracked-files=no"): (0, ""),
    }

    def fail_recovery_audit(
        _config_value: DeployConfig,
        _intent: DeploymentIntent,
        *,
        event: str,
    ) -> None:
        if event == "recovery_started":
            raise OSError("audit fsync failed")

    monkeypatch.setattr(production_deploy, "_append_intent_audit", fail_recovery_audit)
    baseline = _config(tmp_path)
    config = DeployConfig(**{**baseline.__dict__, "recovery_action": "resume"})
    runner = FakeRunner(responses)

    with pytest.raises(OSError, match="audit fsync"):
        deploy(
            config,
            runner=runner,
            generation_authority=authority,
            generation_finalizer=FakeGenerationFinalizer(),
        )

    assert authority.intent is not None and authority.intent.stage == "recovery_started"
    assert ("invalidate", None) not in authority.events
    assert runner.calls == [
        ("git", "rev-parse", "--abbrev-ref", "HEAD"),
        ("git", "status", "--porcelain", "--untracked-files=no"),
    ]


def test_repeated_recovery_interruption_restarts_from_a_durable_fence(
    tmp_path: Path,
) -> None:
    authority = FakeGenerationAuthority()
    authority.begin_deployment_intent(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref="v0.13.2",
        changed_files=("src/rquant/monitor.py",),
        restart_services=("rquant-monitor.service",),
        active_services=("rquant-monitor.service",),
        active_timers=("rquant-monitor.timer",),
        marker_generation="marker-a",
    )
    responses = {
        ("git", "rev-parse", "--abbrev-ref", "HEAD"): (0, "main\n"),
        ("git", "status", "--porcelain", "--untracked-files=no"): (0, ""),
        ("git", "rev-parse", "HEAD"): (0, f"{_sha('b')}\n"),
        ("systemctl", "is-active", "rquant-monitor.service"): (0, "active\n"),
        ("systemctl", "is-active", "rquant-monitor.timer"): (0, "active\n"),
    }
    baseline = _config(tmp_path)
    config = DeployConfig(**{**baseline.__dict__, "recovery_action": "resume"})
    crashing = CrashAfterRunner(
        responses,
        command=("sudo", "-n", "systemctl", "stop", "rquant-monitor.timer"),
    )

    with pytest.raises(SimulatedDeploymentCrash):
        deploy(
            config,
            runner=crashing,
            generation_authority=authority,
            generation_finalizer=FakeGenerationFinalizer(),
        )

    assert authority.intent is not None
    assert authority.intent.stage == "recovery_started"
    deploy(
        config,
        runner=FakeRunner(responses),
        generation_authority=authority,
        generation_finalizer=FakeGenerationFinalizer(),
    )
    assert [event for event in authority.events if event == ("stage", "recovery_started")] == [
        ("stage", "recovery_started"),
        ("stage", "recovery_started"),
    ]
    assert authority.events.count(("invalidate", None)) == 2


def test_recovery_after_timer_start_interruption_repeats_the_fenced_transition(
    tmp_path: Path,
) -> None:
    authority = FakeGenerationAuthority()
    authority.begin_deployment_intent(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref="v0.13.2",
        changed_files=("src/rquant/monitor.py",),
        restart_services=("rquant-monitor.service",),
        active_services=("rquant-monitor.service",),
        active_timers=("rquant-monitor.timer",),
        marker_generation="marker-a",
    )
    responses = {
        ("git", "rev-parse", "--abbrev-ref", "HEAD"): (0, "main\n"),
        ("git", "status", "--porcelain", "--untracked-files=no"): (0, ""),
        ("git", "rev-parse", "HEAD"): (0, f"{_sha('b')}\n"),
        ("systemctl", "is-active", "rquant-monitor.service"): (0, "active\n"),
        ("systemctl", "is-active", "rquant-monitor.timer"): (0, "active\n"),
    }
    baseline = _config(tmp_path)
    config = DeployConfig(**{**baseline.__dict__, "recovery_action": "resume"})
    start_timer = ("sudo", "-n", "systemctl", "start", "rquant-monitor.timer")

    with pytest.raises(SimulatedDeploymentCrash):
        deploy(
            config,
            runner=CrashAfterRunner(responses, command=start_timer),
            generation_authority=authority,
            generation_finalizer=FakeGenerationFinalizer(),
        )

    assert authority.intent is not None
    assert authority.intent.stage == "post_restart_preflight_ready"
    recovered_runner = FakeRunner(responses)
    result = deploy(
        config,
        runner=recovered_runner,
        generation_authority=authority,
        generation_finalizer=FakeGenerationFinalizer(),
    )
    assert result.status == "recovered"
    assert start_timer in recovered_runner.calls
    assert authority.events.count(("stage", "recovery_started")) == 2
    assert authority.events.count(("invalidate", None)) == 2


def test_rollback_recovery_is_deferred_during_protected_window(tmp_path: Path) -> None:
    authority = FakeGenerationAuthority()
    intent = authority.begin_deployment_intent(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref="v0.13.2",
        changed_files=("src/rquant/monitor.py",),
        restart_services=("rquant-monitor.service",),
        active_services=("rquant-monitor.service",),
        active_timers=("rquant-monitor.timer",),
        marker_generation="marker-a",
    )
    baseline = _config(tmp_path)
    config = DeployConfig(
        **{
            **baseline.__dict__,
            "target": intent.previous_sha,
            "recovery_action": "rollback",
            "now": datetime(2026, 7, 13, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        }
    )
    runner = FakeRunner()

    with pytest.raises(ProtectedWindowError):
        deploy(
            config,
            runner=runner,
            generation_authority=authority,
            generation_finalizer=FakeGenerationFinalizer(),
        )

    assert runner.calls == []


def test_recovery_after_partial_service_restart_completes_services_before_marker(
    tmp_path: Path,
) -> None:
    authority = FakeGenerationAuthority()
    intent = authority.begin_deployment_intent(
        previous_sha=_sha("a"),
        target_sha=_sha("b"),
        target_ref="v0.13.2",
        changed_files=("src/rquant/monitor.py", "src/rquant/surge_watch.py"),
        restart_services=("rquant-monitor.service", "rquant-surge-watch.service"),
        active_services=("rquant-monitor.service", "rquant-surge-watch.service"),
        active_timers=("rquant-monitor.timer", "rquant-surge-watch.timer"),
        marker_generation="marker-a",
    )
    _advance_fake_intent_to(
        authority,
        target_stage="services_transitioning",
        restarted_services=("rquant-monitor.service",),
    )
    responses = {
        ("git", "rev-parse", "--abbrev-ref", "HEAD"): (0, "main\n"),
        ("git", "status", "--porcelain", "--untracked-files=no"): (0, ""),
        ("git", "rev-parse", "HEAD"): (0, f"{_sha('b')}\n"),
        ("systemctl", "is-active", "rquant-monitor.service"): (0, "active\n"),
        ("systemctl", "is-active", "rquant-surge-watch.service"): (0, "active\n"),
        ("systemctl", "is-active", "rquant-monitor.timer"): (0, "active\n"),
        ("systemctl", "is-active", "rquant-surge-watch.timer"): (0, "active\n"),
    }
    runner = FakeRunner(responses)
    finalizer = FakeGenerationFinalizer()
    baseline = _config(tmp_path)
    config = DeployConfig(**{**baseline.__dict__, "recovery_action": "resume"})

    deploy(
        config,
        runner=runner,
        generation_authority=authority,
        generation_finalizer=finalizer,
    )

    second_restart = (
        "sudo",
        "-n",
        "systemctl",
        "restart",
        "rquant-surge-watch.service",
    )
    assert second_restart in runner.calls
    assert (
        runner.calls.count(
            ("rquant", "preflight", "--runtime-root", str(LINUX_PRODUCTION_RUNTIME_ROOT))
        )
        == 2
    )
    for timer in ("rquant-monitor.timer", "rquant-surge-watch.timer"):
        assert ("sudo", "-n", "systemctl", "stop", timer) in runner.calls
        assert ("sudo", "-n", "systemctl", "start", timer) in runner.calls
    assert authority.intent.stage == "completed"
    assert finalizer.calls == [
        (_sha("b"), intent.operation_id, "resume", "publish"),
        (_sha("b"), intent.operation_id, "resume", "commit"),
    ]


def test_hard_crash_after_partial_restart_is_resumable_from_persisted_intent(
    tmp_path: Path,
) -> None:
    responses = _base_responses()
    responses[("git", "diff", "--name-only", f"{_sha('a')}..{_sha('b')}")] = (
        0,
        "src/rquant/monitor.py\nsrc/rquant/surge_watch.py\n",
    )
    for unit in (
        "rquant-monitor.service",
        "rquant-surge-watch.service",
        "rquant-monitor.timer",
        "rquant-monitor-watchdog.timer",
        "rquant-surge-watch.timer",
    ):
        responses[("systemctl", "is-active", unit)] = (0, "active\n")
    authority = FakeGenerationAuthority()
    first_finalizer = FakeGenerationFinalizer()
    crashing = CrashAfterRunner(
        responses,
        command=("sudo", "-n", "systemctl", "restart", "rquant-surge-watch.service"),
    )

    with pytest.raises(SimulatedDeploymentCrash):
        deploy(
            _config(tmp_path),
            runner=crashing,
            generation_authority=authority,
            generation_finalizer=first_finalizer,
        )

    assert authority.intent is not None
    assert authority.intent.stage == "services_transitioning"
    assert authority.intent.restarted_services == ("rquant-monitor.service",)
    assert first_finalizer.calls == []

    recovery_responses = {
        ("git", "rev-parse", "--abbrev-ref", "HEAD"): (0, "main\n"),
        ("git", "status", "--porcelain", "--untracked-files=no"): (0, ""),
        ("git", "rev-parse", "HEAD"): (0, f"{_sha('b')}\n"),
        **{
            ("systemctl", "is-active", unit): (0, "active\n")
            for unit in (
                "rquant-monitor.service",
                "rquant-surge-watch.service",
                "rquant-monitor.timer",
                "rquant-monitor-watchdog.timer",
                "rquant-surge-watch.timer",
            )
        },
    }
    recovery_runner = FakeRunner(recovery_responses)
    recovery_finalizer = FakeGenerationFinalizer()
    baseline = _config(tmp_path)
    recovery = DeployConfig(**{**baseline.__dict__, "recovery_action": "resume"})

    result = deploy(
        recovery,
        runner=recovery_runner,
        generation_authority=authority,
        generation_finalizer=recovery_finalizer,
    )

    assert result.status == "recovered"
    assert authority.intent.stage == "completed"
    assert recovery_finalizer.calls == [
        (_sha("b"), authority.intent.operation_id, "resume", "publish"),
        (_sha("b"), authority.intent.operation_id, "resume", "commit"),
    ]


def test_failed_preflight_rolls_back_code_and_dependencies(tmp_path: Path) -> None:
    responses = _base_responses()
    runner = SequenceRunner(
        responses,
        command=(
            "rquant",
            "preflight",
            "--runtime-root",
            str(LINUX_PRODUCTION_RUNTIME_ROOT),
        ),
        sequence=[(1, "target failed"), (0, "old ready"), (0, "old ready")],
    )
    authority = FakeGenerationAuthority()
    finalizer = FakeGenerationFinalizer()

    with pytest.raises(DeployError, match="rolled back"):
        deploy(
            _config(tmp_path),
            runner=runner,
            generation_authority=authority,
            generation_finalizer=finalizer,
        )

    merge_index = runner.calls.index(("git", "merge", "--ff-only", _sha("b")))
    reset_index = runner.calls.index(("git", "reset", "--hard", _sha("a")))
    assert reset_index > merge_index
    assert runner.calls.count(("uv", "sync", "--frozen")) == 2
    assert (
        runner.calls.count(
            ("rquant", "preflight", "--runtime-root", str(LINUX_PRODUCTION_RUNTIME_ROOT))
        )
        == 3
    )
    assert authority.events[0:2] == [("intent", _sha("b")), ("invalidate", None)]
    assert finalizer.calls == [
        (_sha("a"), authority.intent.operation_id, "rollback", "publish"),
        (_sha("a"), authority.intent.operation_id, "rollback", "commit"),
    ]
    assert all(call[0] != "git" for call in runner.executed_calls)
    audit = (_config(tmp_path).audit_path).read_text(encoding="utf-8")
    assert '"status": "rolled_back"' in audit
    assert sum('"status": "rolled_back"' in line for line in audit.splitlines()) == 1


def test_failed_job_authority_prepare_rolls_back_before_any_service_start(
    tmp_path: Path,
) -> None:
    runner = FailingFirstJobAuthorityRunner(_base_responses())
    authority = FakeGenerationAuthority()

    with pytest.raises(DeployError, match="rolled back"):
        deploy(
            _config(tmp_path),
            runner=runner,
            generation_authority=authority,
            generation_finalizer=FakeGenerationFinalizer(),
        )

    prepare_calls = [
        call
        for call in runner.calls
        if len(call) >= 2 and call[:2] == ("rquant", "lab-runtime-prepare")
    ]
    assert len(prepare_calls) == 2
    assert prepare_calls[0][prepare_calls[0].index("--expected-code-sha") + 1] == _sha("b")
    assert prepare_calls[1][prepare_calls[1].index("--expected-code-sha") + 1] == _sha("a")
    target_prepare_index = runner.calls.index(prepare_calls[0])
    rollback_prepare_index = runner.calls.index(prepare_calls[1])
    service_mutations = [
        index
        for index, call in enumerate(runner.calls)
        if call[:4] == ("sudo", "-n", "systemctl", "restart")
    ]
    assert all(index > rollback_prepare_index for index in service_mutations)
    assert target_prepare_index < runner.calls.index(("git", "reset", "--hard", _sha("a")))


def test_failed_target_recovery_reuses_original_runner_deadline(tmp_path: Path) -> None:
    runner = SequenceRunner(
        _base_responses(),
        command=(
            "rquant",
            "preflight",
            "--runtime-root",
            str(LINUX_PRODUCTION_RUNTIME_ROOT),
        ),
        sequence=[(1, "target failed"), (0, "old ready"), (0, "old ready")],
    )
    authority = FakeGenerationAuthority()

    with pytest.raises(DeployError, match="rolled back"):
        deploy(
            _config(tmp_path),
            runner=runner,
            generation_authority=authority,
            generation_finalizer=FakeGenerationFinalizer(),
        )

    assert ("git", "reset", "--hard", _sha("a")) in runner.calls


def test_failed_merge_attempt_still_restores_previous_head(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "merge", "--ff-only", _sha("b"))] = (1, "merge failed")
    runner = FakeRunner(responses)

    with pytest.raises(DeployError, match="rolled back"):
        deploy(_config(tmp_path), runner=runner)

    assert ("git", "reset", "--hard", _sha("a")) in runner.calls


def test_shell_entrypoint_uses_isolated_stdlib_bootstrap_before_project_import() -> None:
    repo = Path(__file__).resolve().parents[2]
    source = (repo / "scripts" / "deploy-production.sh").read_text(encoding="utf-8")

    assert '"${PYTHON_BIN}" -I -S' in source
    assert "bootstrap-production-deploy.py" in source
    assert '--uv-path "${UV_BIN}"' in source
    assert '-- "$@"' not in source
    assert "-m rquant.ops.production_deploy" not in source
    assert "/../.rquant-deploy" not in source


def test_shell_entrypoint_forwards_complete_runtime_profile_environment(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_uname = fake_bin / "uname"
    fake_uname.write_text("#!/bin/sh\nprintf 'Darwin\\n'\n", encoding="utf-8")
    fake_uname.chmod(0o700)
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$@\"\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o700)
    inputs = tmp_path / "runtime-inputs.json"
    profiles = tmp_path / "profiles"
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "RQUANT_DEPLOY_PYTHON": str(fake_python),
        "RQUANT_RELEASE_PROFILE": "macos-lab",
        "RQUANT_RUNTIME_PRODUCTION_INPUTS": str(inputs),
        "RQUANT_RUNTIME_PROFILE_OUTPUT_DIR": str(profiles),
        "RQUANT_RUNTIME_ROOT": "/home/lighthouse/rquant/data/runtime",
    }

    result = subprocess.run(
        [str(ROOT / "scripts" / "deploy-production.sh"), "--target", "v0.99.0"],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    arguments = result.stdout.splitlines()
    assert arguments[arguments.index("--release-profile") + 1] == "macos-lab"
    assert arguments[arguments.index("--host-platform") + 1] == "darwin"
    assert arguments[-6:] == [
        "--runtime-production-inputs",
        str(inputs),
        "--runtime-profile-output-dir",
        str(profiles),
        "--runtime-root",
        "/home/lighthouse/rquant/data/runtime",
    ]
    assert "--runtime-schema-v1-migration-authority" not in arguments


def test_shell_entrypoint_rejects_partial_runtime_profile_environment(
    tmp_path: Path,
) -> None:
    """The together-or-not-at-all rule, on the branch that can actually reach it.

    Linux production refuses a missing runtime profile earlier and with its own message,
    so the platform is pinned to Darwin the same way the Linux cases pin theirs. Without
    that, the test only passes on a macOS host and fails on every Linux runner.
    """

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_uname = fake_bin / "uname"
    fake_uname.write_text("#!/bin/sh\nprintf 'Darwin\\n'\n", encoding="utf-8")
    fake_uname.chmod(0o700)
    fake_python = tmp_path / "python"
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(0o700)
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "RQUANT_DEPLOY_PYTHON": str(fake_python),
        "RQUANT_RUNTIME_PRODUCTION_INPUTS": str(tmp_path / "runtime-inputs.json"),
    }
    for name in ("RQUANT_RUNTIME_PROFILE_OUTPUT_DIR", "RQUANT_RUNTIME_ROOT"):
        environment.pop(name, None)

    result = subprocess.run(
        [str(ROOT / "scripts" / "deploy-production.sh"), "--target", "v0.99.0"],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "runtime production inputs, profile output directory, and root" in result.stderr


def test_shell_entrypoint_rejects_linux_production_without_runtime_profile(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_uname = fake_bin / "uname"
    fake_uname.write_text("#!/bin/sh\nprintf 'Linux\\n'\n", encoding="utf-8")
    fake_uname.chmod(0o700)
    fake_python = tmp_path / "python"
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(0o700)
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "RQUANT_DEPLOY_PYTHON": str(fake_python),
    }
    for name in (
        "RQUANT_RUNTIME_PRODUCTION_INPUTS",
        "RQUANT_RUNTIME_PROFILE_OUTPUT_DIR",
        "RQUANT_RUNTIME_ROOT",
    ):
        environment.pop(name, None)

    result = subprocess.run(
        [str(ROOT / "scripts" / "deploy-production.sh"), "--target", "v0.99.0"],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert (
        "Linux production requires runtime production inputs, profile output "
        "directory, and runtime root" in result.stderr
    )


def test_shell_entrypoint_rejects_relocated_linux_production_runtime_root(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_uname = fake_bin / "uname"
    fake_uname.write_text("#!/bin/sh\nprintf 'Linux\\n'\n", encoding="utf-8")
    fake_uname.chmod(0o700)
    fake_python = tmp_path / "python"
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(0o700)
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "RQUANT_DEPLOY_PYTHON": str(fake_python),
        "RQUANT_RUNTIME_PRODUCTION_INPUTS": str(tmp_path / "runtime-inputs.json"),
        "RQUANT_RUNTIME_PROFILE_OUTPUT_DIR": str(tmp_path / "profiles"),
        "RQUANT_RUNTIME_ROOT": "/srv/rquant/data/runtime",
    }

    result = subprocess.run(
        [str(ROOT / "scripts" / "deploy-production.sh"), "--target", "v0.99.0"],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "Linux production runtime root" in result.stderr


def test_sudoers_allows_only_exact_managed_timer_transitions() -> None:
    repo = Path(__file__).resolve().parents[2]
    source = (repo / "deploy" / "sudoers" / "rquant-production-deploy").read_text(encoding="utf-8")

    for timer in (
        "rquant-monitor.timer",
        "rquant-monitor-watchdog.timer",
        "rquant-surge-watch.timer",
    ):
        assert f"/usr/bin/systemctl stop {timer}" in source
        assert f"/usr/bin/systemctl start {timer}" in source
    assert "systemctl stop rquant-*" not in source
    assert "systemctl start rquant-*" not in source
    assert "launchctl" not in "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    )
    assert '/usr/local/libexec/rquant-runtime-credential-sealer ""' in source


def test_cli_does_not_allow_overriding_production_executables() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--target", "v0.13.2", "--uv-bin", "/tmp/untrusted"])


def test_subprocess_runner_preserves_failed_command_diagnostics(tmp_path: Path) -> None:
    runner = SubprocessRunner(tmp_path)

    with pytest.raises(DeployError, match="diagnostic-from-command"):
        runner.run(
            [
                sys.executable,
                "-c",
                "import sys; print('diagnostic-from-command', file=sys.stderr); sys.exit(7)",
            ]
        )


def test_subprocess_runner_marks_mutating_git_for_process_group_write_locking(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict[str, str]] = []

    def fake_run_process_group(
        args: list[str],
        *,
        cwd: Path,
        deadline_monotonic: float,
        check: bool,
        pass_fds: tuple[int, ...] = (),
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        del cwd, deadline_monotonic, check, pass_fds
        captured.append(env)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(production_deploy, "_run_process_group", fake_run_process_group)
    runner = SubprocessRunner(tmp_path)

    runner.run(["/usr/bin/git", "reset", "--hard", "a" * 40])

    assert captured[0]["GIT_OPTIONAL_LOCKS"] == "1"


def test_subprocess_runner_uses_explicit_trusted_git_binding_for_lock_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict[str, str]] = []
    trusted_git = tmp_path / "tools" / "git-2.48"

    def fake_run_process_group(
        args: list[str],
        *,
        cwd: Path,
        deadline_monotonic: float,
        check: bool,
        pass_fds: tuple[int, ...] = (),
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        del cwd, deadline_monotonic, check, pass_fds
        captured.append(env)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.delenv("GIT_OPTIONAL_LOCKS", raising=False)
    monkeypatch.setattr(production_deploy, "_run_process_group", fake_run_process_group)
    runner = SubprocessRunner(tmp_path, trusted_git_path=trusted_git)

    runner.run([str(trusted_git), "rev-parse", "HEAD"])
    runner.run([str(trusted_git), "fetch", "origin", "main"])
    runner.run([str(tmp_path / "other" / "git-2.48"), "rev-parse", "HEAD"])

    assert captured[0]["GIT_OPTIONAL_LOCKS"] == "0"
    assert captured[1]["GIT_OPTIONAL_LOCKS"] == "1"
    assert "GIT_OPTIONAL_LOCKS" not in captured[2]


def test_subprocess_runner_bounds_each_command_and_overall_rollout(tmp_path: Path) -> None:
    runner = SubprocessRunner(
        tmp_path,
        command_timeout_seconds=0.05,
        overall_timeout_seconds=0.1,
    )

    with pytest.raises(DeployError, match="timed out"):
        runner.run([sys.executable, "-c", "import time; time.sleep(1)"])


def test_subprocess_runner_timeout_terminates_descendant_process_group(tmp_path: Path) -> None:
    marker = tmp_path / "descendant-survived"
    runner = SubprocessRunner(
        tmp_path,
        command_timeout_seconds=0.1,
        overall_timeout_seconds=0.2,
    )
    program = (
        "import subprocess,sys,time;"
        "subprocess.Popen([sys.executable,'-c',"
        f'"import time,pathlib;time.sleep(0.4);'
        f"pathlib.Path({str(marker)!r}).write_text('alive')\"]);"
        "time.sleep(5)"
    )

    with pytest.raises(DeployError, match="timed out"):
        runner.run([sys.executable, "-c", program])
    time.sleep(0.6)

    assert not marker.exists()


def test_process_runner_timeout_contains_detached_grandchild(tmp_path: Path) -> None:
    marker = tmp_path / "detached-grandchild-survived"
    grandchild = (
        "import sys,time; from pathlib import Path; "
        "time.sleep(.3); Path(sys.argv[1]).write_text('late')"
    )
    child = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable,'-c',{grandchild!r},sys.argv[1]],"
        "start_new_session=True); time.sleep(5)"
    )

    expected = ContainedProcessError if sys.platform == "darwin" else subprocess.TimeoutExpired
    with pytest.raises(expected):
        production_deploy._run_process_group(
            [sys.executable, "-c", child, str(marker)],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 0.2,
            check=True,
            may_spawn_background_descendants=True,
        )
    time.sleep(0.5)

    assert not marker.exists()


def test_subprocess_runner_recovery_inherits_expired_global_deadline(tmp_path: Path) -> None:
    runner = SubprocessRunner(
        tmp_path,
        command_timeout_seconds=0.1,
        overall_timeout_seconds=0.1,
    )
    time.sleep(0.12)

    recovery = runner.for_recovery()
    with pytest.raises(DeployError, match="overall timeout"):
        recovery.run([sys.executable, "-c", "print('recovered')"])


def test_isolated_finalizer_recovery_cannot_extend_original_deadline(tmp_path: Path) -> None:
    baseline = _config(tmp_path)
    original = production_deploy.IsolatedGenerationFinalizer(
        DeployConfig(
            **{
                **baseline.__dict__,
                "lock_path": tmp_path / "deploy.lock",
                "lock_fd": 7,
                "python_path": Path(sys.executable),
                "overall_deadline_monotonic": time.monotonic() - 1,
            }
        )
    )
    recovered = original.for_recovery(time.monotonic() + 30)

    assert recovered._config.overall_deadline_monotonic < time.monotonic()


def test_subprocess_runner_uses_inherited_end_to_end_deadline(tmp_path: Path) -> None:
    runner = SubprocessRunner(
        tmp_path,
        command_timeout_seconds=5,
        overall_timeout_seconds=5,
        overall_deadline_monotonic=time.monotonic() - 0.01,
    )

    with pytest.raises(DeployError, match="overall timeout"):
        runner.run([sys.executable, "-c", "raise SystemExit(0)"])


def test_subprocess_runner_preserves_exact_inherited_absolute_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inherited_deadline = time.monotonic() + 30
    captured: list[float] = []

    def fake_run_process_group(
        args: list[str],
        *,
        cwd: Path,
        deadline_monotonic: float,
        check: bool,
        pass_fds: tuple[int, ...] = (),
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        del cwd, check, pass_fds, env
        captured.append(deadline_monotonic)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(production_deploy, "_run_process_group", fake_run_process_group)
    runner = SubprocessRunner(
        tmp_path,
        command_timeout_seconds=1,
        overall_timeout_seconds=2,
        overall_deadline_monotonic=inherited_deadline,
    )

    runner.run([sys.executable, "-c", "raise SystemExit(0)"])

    assert runner.deadline_monotonic == inherited_deadline
    assert captured == [inherited_deadline]


def test_process_group_helper_rejects_expired_absolute_deadline_before_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = False

    def forbidden_run(*_args: object, **_kwargs: object) -> object:
        nonlocal started
        started = True
        raise AssertionError("expired deadline must prevent process startup")

    monkeypatch.setattr(production_deploy, "run_contained", forbidden_run)

    with pytest.raises(subprocess.TimeoutExpired):
        production_deploy._run_process_group(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() - 1,
            check=True,
        )

    assert not started


def test_real_git_repository_deploys_annotated_fast_forward_tag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "prod"
    origin = tmp_path / "origin.git"
    trusted_git = Path("/usr/bin/git")
    repo.mkdir()

    def git(*args: str) -> str:
        result = subprocess.run(
            [str(trusted_git), *args],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    git("init", "-b", "main")
    git("config", "user.name", "rQuant CI")
    git("config", "user.email", "rquant@example.invalid")
    (repo / "src" / "rquant").mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.13.2"\n',
        encoding="utf-8",
    )
    preflight = repo / "src" / "rquant" / "preflight.py"
    preflight.write_text("BASELINE = True\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "base")
    base_sha = git("rev-parse", "HEAD")

    preflight.write_text("BASELINE = False\n", encoding="utf-8")
    policy_dir = repo / "tests" / "fixtures" / "r07_differential_gate"
    policy_dir.mkdir(parents=True)
    (policy_dir / "policy-v1.json").write_bytes(_r07_policy_bytes())
    git("add", "src/rquant/preflight.py", R07_POLICY_RELATIVE_PATH)
    git("commit", "-m", "target")
    target_sha = git("rev-parse", "HEAD")
    git("tag", "-a", "v0.13.2", "-m", "release")

    subprocess.run(
        [str(trusted_git), "clone", "--bare", str(repo), str(origin)],
        capture_output=True,
        text=True,
        check=True,
    )
    git("reset", "--hard", base_sha)
    git("remote", "add", "origin", str(origin))
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    fake_git_called = tmp_path / "fake-git-called"
    fake_git = fake_bin / "git"
    fake_git.write_text(
        f"#!/bin/sh\nprintf called > {fake_git_called}\nexit 99\n",
        encoding="utf-8",
    )
    fake_git.chmod(0o700)
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ['PATH']}")

    config = DeployConfig(
        repo=repo,
        target="v0.13.2",
        now=datetime(2026, 7, 13, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        uv_bin="/usr/bin/true",
        rquant_bin="/usr/bin/true",
        audit_path=tmp_path / "audit.jsonl",
        lock_path=tmp_path / "deploy.lock",
        git_path=trusted_git,
        release_profile="macos-lab",
        platform_name="darwin",
    )

    result = deploy(config)

    assert result.status == "deployed"
    assert result.r07_gate == "bootstrap_disabled"
    assert result.r07_target_tree_sha == git("rev-parse", "--verify", f"{target_sha}^{{tree}}")
    assert result.target_sha == target_sha
    assert git("rev-parse", "HEAD") == target_sha
    assert not fake_git_called.exists()
    assert '"status": "deployed"' in config.audit_path.read_text(encoding="utf-8")


@pytest.mark.parametrize("signum", (signal.SIGTERM, signal.SIGINT))
@pytest.mark.skipif(sys.platform == "darwin", reason="Darwin rejects background-capable commands")
def test_subprocess_runner_reaps_process_group_before_signal_releases_parent(
    tmp_path: Path,
    signum: signal.Signals,
) -> None:
    ready = tmp_path / "ready"
    late_mutation = tmp_path / "late-mutation"
    child_program = (
        "import subprocess,sys,time; from pathlib import Path; "
        "subprocess.Popen([sys.executable,'-c',"
        '"import sys,time; from pathlib import Path; time.sleep(.25); '
        "Path(sys.argv[1]).write_text('late')\",sys.argv[2]],start_new_session=True); "
        "Path(sys.argv[1]).write_text('ready'); time.sleep(.6)"
    )
    harness = (
        "import sys,time; from pathlib import Path; "
        "from rquant.ops.production_deploy import _run_process_group; "
        f"_run_process_group([sys.executable,'-c',{child_program!r},"
        "sys.argv[1],sys.argv[2]],cwd=Path(sys.argv[3]),"
        "deadline_monotonic=time.monotonic()+10,check=True,"
        "may_spawn_background_descendants=True)"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", harness, str(ready), str(late_mutation), str(tmp_path)],
        cwd=ROOT,
    )
    deadline = time.monotonic() + 5
    while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert ready.exists()

    os.kill(process.pid, signum)
    process.wait(timeout=5)
    time.sleep(0.8)

    assert not late_mutation.exists()


@pytest.mark.skipif(sys.platform == "darwin", reason="Darwin rejects background-capable commands")
def test_process_runner_base_exception_contains_detached_grandchild(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = tmp_path / "base-exception-grandchild-survived"
    grandchild = (
        "import sys,time; from pathlib import Path; "
        "time.sleep(.3); Path(sys.argv[1]).write_text('late')"
    )
    child = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable,'-c',{grandchild!r},sys.argv[1]],"
        "start_new_session=True); time.sleep(5)"
    )
    real_popen = subprocess.Popen

    class SimulatedRunnerCrash(BaseException):
        pass

    class CrashProxy:
        def __init__(self, process: subprocess.Popen[str]) -> None:
            self._process = process
            self.pid = process.pid
            self.returncode: int | None = None
            self.crashed = False

        def communicate(self, *args: object, **kwargs: object) -> tuple[str, str]:
            if not self.crashed:
                self.crashed = True
                time.sleep(0.08)
                raise SimulatedRunnerCrash
            result = self._process.communicate(*args, **kwargs)
            self.returncode = self._process.returncode
            return result

        def poll(self) -> int | None:
            self.returncode = self._process.poll()
            return self.returncode

    monkeypatch.setattr(
        production_deploy.subprocess,
        "Popen",
        lambda *args, **kwargs: CrashProxy(real_popen(*args, **kwargs)),
    )

    with pytest.raises(SimulatedRunnerCrash):
        production_deploy._run_process_group(
            [sys.executable, "-c", child, str(marker)],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 0.5,
            check=True,
            may_spawn_background_descendants=True,
        )
    time.sleep(0.5)

    assert not marker.exists()


R07_POLICY_RELATIVE_PATH = "tests/fixtures/r07_differential_gate/policy-v1.json"
R07_INSTALLED_TREE = _sha("e")
R07_TARGET_TREE = _sha("d")
R07_INSTALLED_BLOB = _sha("1")
R07_TARGET_BLOB = _sha("2")


def _r07_policy_bytes(*, enforced_predecessor: tuple[str, str] | None = None) -> bytes:
    if enforced_predecessor is None:
        return (ROOT / R07_POLICY_RELATIVE_PATH).read_bytes()
    commit_sha, tree_sha = enforced_predecessor
    return _enforced_policy_bytes(commit_sha, tree_sha)


def _r07_responses(
    *,
    installed_sha: str = _sha("a"),
    target_sha: str = _sha("b"),
    installed_policy: bytes | None = None,
    target_policy: bytes | None = b"",
) -> dict[tuple[str, ...], tuple[int, str]]:
    """Git object responses for the installed and target R07 policy reads."""

    resolved_target = _r07_policy_bytes() if target_policy == b"" else target_policy
    responses: dict[tuple[str, ...], tuple[int, str]] = {
        ("git", "rev-parse", "--verify", f"{installed_sha}^{{tree}}"): (
            0,
            f"{R07_INSTALLED_TREE}\n",
        ),
        ("git", "rev-parse", "--verify", f"{target_sha}^{{tree}}"): (0, f"{R07_TARGET_TREE}\n"),
    }
    for sha, payload, blob in (
        (installed_sha, installed_policy, R07_INSTALLED_BLOB),
        (target_sha, resolved_target, R07_TARGET_BLOB),
    ):
        listing = (
            "" if payload is None else f"100644 blob {blob}\t{R07_POLICY_RELATIVE_PATH}\n"
        )
        responses[
            ("git", "ls-tree", "--full-tree", sha, "--", R07_POLICY_RELATIVE_PATH)
        ] = (0, listing)
        if payload is not None:
            responses[("git", "cat-file", "blob", blob)] = (0, payload.decode("utf-8"))
    return responses


class _RecordingVerifier:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, repo: Path, policy: object, wire: object) -> object:
        self.calls.append(wire.candidate_commit_sha)
        return object()


class _EmptyTransport:
    def __init__(self) -> None:
        self.requests: list[str] = []

    def get(self, url: str, *, token: str, accept: str) -> bytes:
        self.requests.append(url)
        raise r07_deploy_evidence.R07EvidenceError("the evidence channel is unavailable")


class _StaticTokenProvider:
    def token(self) -> str:
        return "ghs-test-token"


class _InProcessR07Gate:
    """Drive the real gate in process and hand the deployer its canonical decision."""

    def __init__(self, gate: r07_deploy_evidence.R07DeployEvidenceGate) -> None:
        self._gate = gate

    @property
    def cache_dir(self) -> Path:
        return self._gate.cache_dir

    def evaluate(
        self,
        *,
        repo: Path,
        runner: object,
        git_path: Path,
        installed_commit_sha: str,
        target_commit_sha: str,
    ) -> production_deploy.R07GateDecision:
        decision = self._gate.evaluate(
            repo=repo,
            runner=runner,
            git_path=git_path,
            installed_commit_sha=installed_commit_sha,
            target_commit_sha=target_commit_sha,
        )
        return production_deploy.r07_decision_from_child_output(
            canonical_json_bytes(decision.model_dump(mode="json"))
        )


def _r07_gate(
    tmp_path: Path,
    *,
    verifier: object | None = None,
    transport: object | None = None,
) -> _InProcessR07Gate:
    return _InProcessR07Gate(
        r07_deploy_evidence.R07DeployEvidenceGate(
            cache_dir=tmp_path / "var" / "r07-dr-evidence",
            transport=transport or _EmptyTransport(),
            token_provider=_StaticTokenProvider(),
            clock=lambda: 0.0,
            verifier=verifier or _RecordingVerifier(),
            # `/tmp` is sticky on Linux, so the walk is rooted at the test directory.
            cache_trust=_lab_trust(tmp_path),
        )
    )


def _r07_run_identity_transport(commit_sha: str) -> _FakeTransport:
    """The one query a cache hit still owes the fixed channel: whose run is this target?"""

    return _FakeTransport(
        {
            r07_deploy_evidence.workflow_runs_url(commit_sha): json.dumps(
                {"workflow_runs": [_run_payload(head_sha=commit_sha)]}
            ).encode()
        }
    )


@pytest.fixture(autouse=True)
def _in_process_r07_gate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep the real decision table but skip the isolated child in unit tests."""

    monkeypatch.setattr(
        production_deploy,
        "_build_r07_evidence_gate",
        lambda config: _r07_gate(tmp_path),
    )


def _r07_channel_transport(*, commit_sha: str, tree_sha: str, policy: bytes) -> _FakeTransport:
    """A fake GitHub channel serving exactly one bound artifact for the target commit."""

    parsed = r07_deploy_evidence.parse_policy_blob(policy)
    archive_url = "https://api.github.com/artifact/91/zip"
    return _FakeTransport(
        {
            r07_deploy_evidence.workflow_runs_url(commit_sha): json.dumps(
                {"workflow_runs": [_run_payload(head_sha=commit_sha)]}
            ).encode(),
            r07_deploy_evidence.run_artifacts_url(RUN_ID): json.dumps(
                {"artifacts": [_artifact_payload(name=f"r07-dr-gate-{commit_sha}")]}
            ).encode(),
            archive_url: _artifact_zip(
                {
                    "r07-dr-gate/evidence-v1.json": _valid_wire_bytes(
                        commit_sha,
                        tree_sha,
                        policy=parsed,
                    )
                }
            ),
        }
    )


def _seed_r07_cache(tmp_path: Path, *, commit_sha: str, tree_sha: str, policy: bytes) -> Path:
    parsed = r07_deploy_evidence.parse_policy_blob(policy)
    return r07_deploy_evidence.write_cached_evidence(
        cache_dir=tmp_path / "var" / "r07-dr-evidence",
        commit_sha=commit_sha,
        payload=_valid_wire_bytes(commit_sha, tree_sha, policy=parsed),
    )


def _repository_bytes(root: Path, cache_dir: Path) -> dict[str, bytes]:
    """Every file under the deployment worktree, excluding the retained evidence cache."""

    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file() and cache_dir not in path.parents
    }


def _audit_records(config: DeployConfig) -> list[dict[str, object]]:
    assert config.audit_path is not None
    if not config.audit_path.exists():
        return []
    return [
        json.loads(line)
        for line in config.audit_path.read_text(encoding="utf-8").splitlines()
        if line
    ]


class TestR07Bootstrap:
    """Release A/B evidence gate on the exact-target deployment chain."""

    def test_release_a_installs_once_and_audits_the_bootstrap_pair(self, tmp_path: Path) -> None:
        responses = _base_responses()
        runner = FakeRunner(responses)
        config = _config(tmp_path)

        result = deploy(config, runner=runner, r07_evidence_gate=_r07_gate(tmp_path))

        assert result.status == "deployed"
        assert result.r07_gate == "bootstrap_disabled"
        assert result.r07_target_tree_sha == R07_TARGET_TREE
        record = _audit_records(config)[-1]
        assert record["r07_gate"] == "bootstrap_disabled"
        assert record["r07_target_commit_sha"] == _sha("b")
        assert record["r07_target_tree_sha"] == R07_TARGET_TREE
        assert record["r07_installed_mode"] == "absent"
        assert record["r07_target_mode"] == "disabled_for_bootstrap"

    def test_release_b_deploys_only_with_verified_enforced_evidence(
        self,
        tmp_path: Path,
    ) -> None:
        enforced = _r07_policy_bytes(enforced_predecessor=(_sha("a"), R07_INSTALLED_TREE))
        responses = _base_responses()
        responses.update(
            _r07_responses(installed_policy=_r07_policy_bytes(), target_policy=enforced)
        )
        _seed_r07_cache(
            tmp_path,
            commit_sha=_sha("b"),
            tree_sha=R07_TARGET_TREE,
            policy=enforced,
        )
        verifier = _RecordingVerifier()
        runner = FakeRunner(responses)
        config = _config(tmp_path)
        transport = _r07_run_identity_transport(_sha("b"))

        result = deploy(
            config,
            runner=runner,
            r07_evidence_gate=_r07_gate(tmp_path, verifier=verifier, transport=transport),
        )

        assert result.status == "deployed"
        assert result.r07_gate == "enforced"
        assert verifier.calls == [_sha("b")]
        assert transport.requests == [r07_deploy_evidence.workflow_runs_url(_sha("b"))]
        assert _audit_records(config)[-1]["r07_gate"] == "enforced"

    @pytest.mark.parametrize(
        ("installed_policy", "target_policy", "expected"),
        [
            pytest.param("disabled", "disabled", "bootstrap", id="disabled-to-disabled"),
            pytest.param("disabled", None, "pre-R07", id="disabled-to-absent"),
            pytest.param(None, None, "pre-R07", id="absent-to-absent"),
            pytest.param(None, "enforced", "predecessor", id="absent-to-enforced"),
            pytest.param(
                "disabled",
                "enforced-other",
                "predecessor",
                id="disabled-to-enforced-wrong-predecessor",
            ),
            pytest.param("enforced", "disabled", "bootstrap", id="enforced-to-disabled"),
            pytest.param("enforced", None, "pre-R07", id="enforced-to-absent"),
        ],
    )
    def test_rejected_transitions_never_reach_checkout_or_services(
        self,
        tmp_path: Path,
        installed_policy: str | None,
        target_policy: str | None,
        expected: str,
    ) -> None:
        payloads = {
            None: None,
            "disabled": _r07_policy_bytes(),
            "enforced": _r07_policy_bytes(
                enforced_predecessor=(_sha("a"), R07_INSTALLED_TREE)
            ),
            "enforced-other": _r07_policy_bytes(
                enforced_predecessor=(_sha("9"), _sha("8"))
            ),
        }
        responses = _base_responses()
        responses.update(
            _r07_responses(
                installed_policy=payloads[installed_policy],
                target_policy=payloads[target_policy],
            )
        )
        runner = FakeRunner(responses)
        config = _config(tmp_path)

        with pytest.raises(PolicyError, match="R07"):
            deploy(config, runner=runner, r07_evidence_gate=_r07_gate(tmp_path))

        assert expected in str(_audit_records(config)[-1]["error"])
        assert _audit_records(config)[-1]["status"] == "r07_gate_failed"
        assert not any(call[:2] == ("git", "merge") for call in runner.calls)
        assert not any(call[:2] == ("git", "reset") for call in runner.calls)
        assert not any(call[0] in {"systemctl", "sudo"} for call in runner.calls)
        assert not any("uv" in call[0] for call in runner.calls)

    def test_unavailable_evidence_channel_blocks_before_checkout(self, tmp_path: Path) -> None:
        enforced = _r07_policy_bytes(enforced_predecessor=(_sha("a"), R07_INSTALLED_TREE))
        responses = _base_responses()
        responses.update(
            _r07_responses(installed_policy=_r07_policy_bytes(), target_policy=enforced)
        )
        transport = _EmptyTransport()
        runner = FakeRunner(responses)
        config = _config(tmp_path)

        with pytest.raises(PolicyError, match="R07"):
            deploy(
                config,
                runner=runner,
                r07_evidence_gate=_r07_gate(tmp_path, transport=transport),
            )

        assert transport.requests
        assert not any(call[:2] == ("git", "merge") for call in runner.calls)
        assert _audit_records(config)[-1]["status"] == "r07_gate_failed"

    def test_evidence_for_another_commit_never_satisfies_the_target(
        self,
        tmp_path: Path,
    ) -> None:
        enforced = _r07_policy_bytes(enforced_predecessor=(_sha("a"), R07_INSTALLED_TREE))
        responses = _base_responses()
        responses.update(
            _r07_responses(installed_policy=_r07_policy_bytes(), target_policy=enforced)
        )
        _seed_r07_cache(
            tmp_path,
            commit_sha=_sha("c"),
            tree_sha=R07_TARGET_TREE,
            policy=enforced,
        )
        runner = FakeRunner(responses)
        config = _config(tmp_path)

        with pytest.raises(PolicyError, match="R07"):
            deploy(config, runner=runner, r07_evidence_gate=_r07_gate(tmp_path))

        assert not any(call[:2] == ("git", "merge") for call in runner.calls)

    def test_cached_evidence_for_another_tree_is_rejected(self, tmp_path: Path) -> None:
        enforced = _r07_policy_bytes(enforced_predecessor=(_sha("a"), R07_INSTALLED_TREE))
        responses = _base_responses()
        responses.update(
            _r07_responses(installed_policy=_r07_policy_bytes(), target_policy=enforced)
        )
        _seed_r07_cache(
            tmp_path,
            commit_sha=_sha("b"),
            tree_sha=R07_INSTALLED_TREE,
            policy=enforced,
        )
        runner = FakeRunner(responses)

        with pytest.raises(PolicyError, match="R07"):
            deploy(_config(tmp_path), runner=runner, r07_evidence_gate=_r07_gate(tmp_path))

        assert not any(call[:2] == ("git", "merge") for call in runner.calls)

    def test_a_deploy_user_planted_cache_entry_never_deploys(self, tmp_path: Path) -> None:
        """`lighthouse` owns the cache directory, so a plausible entry is a claim, not evidence.

        The planted bytes are canonical, digest-consistent, bound to the exact target commit and
        tree, and replay cleanly through the private verifier. Only the fixed channel can say
        whether the target ever ran on `push main`, and here it cannot.
        """

        enforced = _r07_policy_bytes(enforced_predecessor=(_sha("a"), R07_INSTALLED_TREE))
        responses = _base_responses()
        responses.update(
            _r07_responses(installed_policy=_r07_policy_bytes(), target_policy=enforced)
        )
        planted = _seed_r07_cache(
            tmp_path,
            commit_sha=_sha("b"),
            tree_sha=R07_TARGET_TREE,
            policy=enforced,
        )
        verifier = _RecordingVerifier()
        runner = FakeRunner(responses)
        transport = _EmptyTransport()
        config = _config(tmp_path)

        with pytest.raises(PolicyError, match="R07"):
            deploy(
                config,
                runner=runner,
                r07_evidence_gate=_r07_gate(tmp_path, verifier=verifier, transport=transport),
            )

        assert planted.is_file()
        assert verifier.calls == []
        assert transport.requests == [r07_deploy_evidence.workflow_runs_url(_sha("b"))]
        assert not any(call[:2] == ("git", "merge") for call in runner.calls)
        assert not any(call[0] in {"systemctl", "sudo"} for call in runner.calls)
        assert _audit_records(config)[-1]["r07_gate"] == "rejected"

    def test_already_current_bootstrap_target_is_rejected(self, tmp_path: Path) -> None:
        responses = _base_responses()
        responses[("git", "rev-parse", "HEAD")] = (0, f"{_sha('b')}\n")
        responses.update(
            _r07_responses(
                installed_sha=_sha("b"),
                installed_policy=_r07_policy_bytes(),
            )
        )
        runner = FakeRunner(responses)

        with pytest.raises(PolicyError, match="R07"):
            deploy(_config(tmp_path), runner=runner, r07_evidence_gate=_r07_gate(tmp_path))

        assert not any(
            call[:2] in {("rquant", "preflight"), ("rquant", "runtime-production-profile")}
            for call in runner.calls
        )

    def test_dry_run_reports_the_decision_without_mutating_the_checkout(
        self,
        tmp_path: Path,
    ) -> None:
        responses = _base_responses()
        runner = FakeRunner(responses)
        config = _config(tmp_path, dry_run=True)
        gate = _r07_gate(tmp_path)
        before = _repository_bytes(tmp_path, gate.cache_dir)

        result = deploy(config, runner=runner, r07_evidence_gate=gate)

        assert result.status == "dry_run"
        assert result.r07_gate == "bootstrap_disabled"
        assert result.r07_target_tree_sha == R07_TARGET_TREE
        assert not any(call[:2] == ("git", "merge") for call in runner.calls)
        assert _repository_bytes(tmp_path, gate.cache_dir) == before

    def test_dry_run_downloads_and_caches_enforced_evidence(self, tmp_path: Path) -> None:
        enforced = _r07_policy_bytes(enforced_predecessor=(_sha("a"), R07_INSTALLED_TREE))
        responses = _base_responses()
        responses.update(
            _r07_responses(installed_policy=_r07_policy_bytes(), target_policy=enforced)
        )
        transport = _r07_channel_transport(
            commit_sha=_sha("b"),
            tree_sha=R07_TARGET_TREE,
            policy=enforced,
        )
        verifier = _RecordingVerifier()
        gate = _r07_gate(tmp_path, verifier=verifier, transport=transport)
        config = _config(tmp_path, dry_run=True)
        runner = FakeRunner(responses)
        before = _repository_bytes(tmp_path, gate.cache_dir)

        result = deploy(config, runner=runner, r07_evidence_gate=gate)

        assert result.status == "dry_run"
        assert result.r07_gate == "enforced"
        assert (gate.cache_dir / f"{_sha('b')}.json").is_file()
        assert verifier.calls == [_sha("b"), _sha("b")]
        assert transport.requests
        assert not any(call[:2] == ("git", "merge") for call in runner.calls)
        assert not any(call[0] in {"systemctl", "sudo"} for call in runner.calls)
        assert _repository_bytes(tmp_path, gate.cache_dir) == before
        assert _audit_records(config) == []

    def test_recovery_replays_only_the_recorded_intent_pair(self, tmp_path: Path) -> None:
        authority = FakeGenerationAuthority()
        authority.begin_deployment_intent(
            previous_sha=_sha("a"),
            target_sha=_sha("b"),
            target_ref="v0.13.2",
            changed_files=("src/rquant/preflight.py",),
            restart_services=(),
            active_services=(),
            active_timers=(),
            marker_generation="marker-a",
        )
        responses = _base_responses()
        responses[("git", "rev-parse", "HEAD")] = (0, f"{_sha('b')}\n")
        baseline = _config(tmp_path)
        wrong_target = DeployConfig(
            **{**baseline.__dict__, "recovery_action": "rollback", "target": _sha("c")}
        )
        runner = FakeRunner(responses)

        with pytest.raises(PolicyError, match="recorded deployment intent"):
            deploy(
                wrong_target,
                runner=runner,
                generation_authority=authority,
                generation_finalizer=FakeGenerationFinalizer(),
                r07_evidence_gate=_r07_gate(tmp_path),
            )

        assert not any(call[:2] == ("git", "reset") for call in runner.calls)
        assert not any(call[0] == "systemctl" for call in runner.calls)
        assert authority.intent is not None and authority.intent.stage == "planned"

        recorded = DeployConfig(
            **{**baseline.__dict__, "recovery_action": "rollback", "target": _sha("a")}
        )
        recovery_runner = FakeRunner(responses)

        result = deploy(
            recorded,
            runner=recovery_runner,
            generation_authority=authority,
            generation_finalizer=FakeGenerationFinalizer(),
            r07_evidence_gate=_r07_gate(tmp_path),
        )

        assert result.status == "recovered"
        assert result.r07_gate == "recorded_intent"
        assert _audit_records(recorded)[-1]["r07_gate"] == "recorded_intent"

    def test_real_checkout_stays_at_head_when_the_target_declares_no_policy(
        self,
        tmp_path: Path,
    ) -> None:
        repo = tmp_path / "prod"
        origin = tmp_path / "origin.git"
        trusted_git = Path("/usr/bin/git")
        repo.mkdir()

        def git(*args: str) -> str:
            return subprocess.run(
                [str(trusted_git), *args],
                cwd=repo,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()

        git("init", "-b", "main")
        git("config", "user.name", "rQuant CI")
        git("config", "user.email", "rquant@example.invalid")
        (repo / "src" / "rquant").mkdir(parents=True)
        (repo / "pyproject.toml").write_text(
            '[project]\nname = "rquant"\nversion = "0.13.2"\n',
            encoding="utf-8",
        )
        preflight = repo / "src" / "rquant" / "preflight.py"
        preflight.write_text("BASELINE = True\n", encoding="utf-8")
        git("add", ".")
        git("commit", "-m", "base")
        base_sha = git("rev-parse", "HEAD")
        preflight.write_text("BASELINE = False\n", encoding="utf-8")
        git("add", "src/rquant/preflight.py")
        git("commit", "-m", "target")
        git("tag", "-a", "v0.13.2", "-m", "release")
        subprocess.run(
            [str(trusted_git), "clone", "--bare", str(repo), str(origin)],
            capture_output=True,
            text=True,
            check=True,
        )
        git("reset", "--hard", base_sha)
        git("remote", "add", "origin", str(origin))
        config = DeployConfig(
            repo=repo,
            target="v0.13.2",
            now=datetime(2026, 7, 13, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            uv_bin="/usr/bin/true",
            rquant_bin="/usr/bin/true",
            audit_path=tmp_path / "audit.jsonl",
            lock_path=tmp_path / "deploy.lock",
            git_path=trusted_git,
            release_profile="macos-lab",
            platform_name="darwin",
        )

        with pytest.raises(PolicyError, match="pre-R07"):
            deploy(config)

        assert git("rev-parse", "HEAD") == base_sha
        assert preflight.read_text(encoding="utf-8") == "BASELINE = True\n"
        record = _audit_records(config)[-1]
        assert record["status"] == "r07_gate_failed"
        assert record["r07_gate"] == "rejected"

    @staticmethod
    def _release_chain_checkout(tmp_path: Path) -> tuple[Path, str, str, str]:
        """A real production-shaped checkout with pre-R07, Release A, and Release B commits."""

        repo = tmp_path / "prod"
        origin = tmp_path / "origin.git"
        trusted_git = Path("/usr/bin/git")
        repo.mkdir()

        def git(*args: str) -> str:
            return subprocess.run(
                [str(trusted_git), *args],
                cwd=repo,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()

        git("init", "-b", "main")
        git("config", "user.name", "rQuant CI")
        git("config", "user.email", "rquant@example.invalid")
        (repo / "src" / "rquant").mkdir(parents=True)
        (repo / "pyproject.toml").write_text(
            '[project]\nname = "rquant"\nversion = "0.13.2"\n',
            encoding="utf-8",
        )
        preflight = repo / "src" / "rquant" / "preflight.py"
        preflight.write_text("BASELINE = True\n", encoding="utf-8")
        git("add", ".")
        git("commit", "-m", "pre-r07")
        pre_r07 = git("rev-parse", "HEAD")
        policy_path = repo / R07_POLICY_RELATIVE_PATH
        policy_path.parent.mkdir(parents=True)
        policy_path.write_bytes(_r07_policy_bytes())
        git("add", R07_POLICY_RELATIVE_PATH)
        git("commit", "-m", "release a")
        release_a = git("rev-parse", "HEAD")
        release_a_tree = git("rev-parse", "--verify", f"{release_a}^{{tree}}")
        policy_path.write_bytes(
            _r07_policy_bytes(enforced_predecessor=(release_a, release_a_tree))
        )
        git("add", R07_POLICY_RELATIVE_PATH)
        git("commit", "-m", "release b")
        release_b = git("rev-parse", "HEAD")
        subprocess.run(
            [str(trusted_git), "clone", "--bare", str(repo), str(origin)],
            capture_output=True,
            text=True,
            check=True,
        )
        git("reset", "--hard", pre_r07)
        git("remote", "add", "origin", str(origin))
        return repo, pre_r07, release_a, release_b

    @staticmethod
    def _lab_config(repo: Path, tmp_path: Path, target: str) -> DeployConfig:
        return DeployConfig(
            repo=repo,
            target=target,
            now=datetime(2026, 7, 13, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            uv_bin="/usr/bin/true",
            rquant_bin="/usr/bin/true",
            audit_path=tmp_path / "audit.jsonl",
            lock_path=tmp_path / "deploy.lock",
            git_path=Path("/usr/bin/git"),
            release_profile="macos-lab",
            platform_name="darwin",
        )

    def test_isolated_gate_installs_release_a_and_blocks_release_b_without_a_token(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.undo()
        monkeypatch.delenv(r07_deploy_evidence.EVIDENCE_TOKEN_VARIABLE, raising=False)
        repo, _pre_r07, release_a, release_b = self._release_chain_checkout(tmp_path)

        installed = deploy(self._lab_config(repo, tmp_path, release_a))

        assert installed.status == "deployed"
        assert installed.r07_gate == "bootstrap_disabled"
        assert (
            subprocess.run(
                ["/usr/bin/git", "rev-parse", "HEAD"],
                cwd=repo,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            == release_a
        )

        with pytest.raises(PolicyError, match="R07"):
            deploy(self._lab_config(repo, tmp_path, release_b))

        assert (
            subprocess.run(
                ["/usr/bin/git", "rev-parse", "HEAD"],
                cwd=repo,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            == release_a
        )
        records = _audit_records(self._lab_config(repo, tmp_path, release_b))
        assert records[0]["r07_gate"] == "bootstrap_disabled"
        assert records[-1]["status"] == "r07_gate_failed"
        assert "RQUANT_GITHUB_EVIDENCE_TOKEN" in str(records[-1]["error"])

    @staticmethod
    def _child_failure_process_group(
        outcome: str,
        calls: list[list[str]],
    ) -> Callable[..., subprocess.CompletedProcess[bytes]]:
        failures: dict[str, Exception] = {
            "timeout": subprocess.TimeoutExpired(["gate"], 1),
            "contained": ContainedProcessError("kernel process tracker cleanup is pending"),
            "oserror": OSError("interpreter is unavailable"),
            "subprocess-error": subprocess.SubprocessError("spawn refused"),
            "unicode": UnicodeDecodeError("ascii", b"\xe7", 0, 1, "ordinal not in range"),
        }

        def fake_process_group(
            args: list[str],
            **kwargs: object,
        ) -> subprocess.CompletedProcess[bytes]:
            calls.append(args)
            failure = failures.get(outcome)
            if failure is not None:
                raise failure
            if outcome == "nonzero":
                return subprocess.CompletedProcess(args, 3, stdout=b"", stderr=b"boom")
            payload = b"not json at all" if outcome == "garbage" else b""
            return subprocess.CompletedProcess(args, 0, stdout=payload, stderr=b"")

        return fake_process_group

    @pytest.mark.parametrize(
        ("outcome", "expected"),
        [
            pytest.param("nonzero", "failed \\(3\\)", id="nonzero-exit"),
            pytest.param("timeout", "could not run", id="timeout"),
            pytest.param("garbage", "unusable decision", id="invalid-json"),
            pytest.param("empty", "unusable decision", id="no-output"),
            pytest.param("contained", "could not run", id="contained-process-error"),
            pytest.param("oserror", "could not run", id="os-error"),
            pytest.param("subprocess-error", "could not run", id="subprocess-error"),
            pytest.param("unicode", "could not run", id="undecodable-output"),
        ],
    )
    def test_isolated_gate_refuses_every_child_failure(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        outcome: str,
        expected: str,
    ) -> None:
        monkeypatch.undo()
        calls: list[list[str]] = []

        monkeypatch.setattr(
            production_deploy,
            "_run_process_group",
            self._child_failure_process_group(outcome, calls),
        )
        gate = production_deploy._build_r07_evidence_gate(_config(tmp_path))

        decision = gate.evaluate(
            repo=tmp_path,
            runner=FakeRunner({}),
            git_path=Path("/usr/bin/git"),
            installed_commit_sha=_sha("a"),
            target_commit_sha=_sha("b"),
        )

        assert decision.allowed is False
        assert decision.gate == "rejected"
        assert re.search(expected, decision.reason)
        assert decision.installed_commit_sha == _sha("a")
        assert decision.target_tree_sha == ""
        assert calls and calls[0][1] == "-I"
        assert calls[0][2].endswith("scripts/r07_deploy_gate.py")

    @pytest.mark.parametrize(
        "outcome",
        [
            pytest.param("contained", id="contained-process-error"),
            pytest.param("oserror", id="os-error"),
            pytest.param("subprocess-error", id="subprocess-error"),
            pytest.param("unicode", id="undecodable-output"),
        ],
    )
    def test_a_child_launch_failure_is_refused_and_audited(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        outcome: str,
    ) -> None:
        monkeypatch.undo()
        monkeypatch.setattr(
            production_deploy,
            "_run_process_group",
            self._child_failure_process_group(outcome, []),
        )
        config = _config(tmp_path)
        runner = FakeRunner(_base_responses())

        with pytest.raises(PolicyError, match="R07 deployment evidence gate refused"):
            deploy(config, runner=runner)

        records = _audit_records(config)
        assert len(records) == 1
        assert records[0]["status"] == "r07_gate_failed"
        assert records[0]["r07_gate"] == "rejected"
        assert records[0]["r07_installed_mode"] == "unresolved"
        assert not any(call[:2] in {("git", "merge"), ("git", "reset")} for call in runner.calls)
        assert not any(call[0] in {"systemctl", "sudo", "uv"} for call in runner.calls)

    def test_a_refused_gate_exits_as_a_policy_refusal(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.undo()

        def refuse(config: DeployConfig, **kwargs: object) -> DeployResult:
            raise PolicyError("R07 deployment evidence gate refused the target: child failed")

        monkeypatch.setattr(production_deploy, "deploy", refuse)

        code = production_deploy.main(
            [
                "--target",
                _sha("b"),
                "--repo",
                str(tmp_path),
                "--deployment-lock-path",
                str(tmp_path / "deploy.lock"),
                "--startup-generation",
                _sha("a"),
                "--trusted-git-path",
                "/usr/bin/git",
                "--python-path",
                sys.executable,
                "--uv-path",
                "/usr/bin/true",
                "--release-profile",
                "linux-production",
                "--platform-name",
                "linux",
            ]
        )

        assert code == 2
        assert "REFUSED: R07 deployment evidence gate refused" in capsys.readouterr().err

    def test_the_child_decision_is_read_as_bytes_and_decoded_as_utf8(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.undo()
        reason = "R07 policy objects could not be resolved: 版本 mismatch"
        payload = canonical_json_bytes(
            self._child_decision_payload(
                allowed=False,
                gate="rejected",
                reason=reason,
                installed_commit_sha="",
                installed_tree_sha="",
                target_commit_sha="",
                target_tree_sha="",
            )
        )
        requested: list[object] = []

        def fake_process_group(
            args: list[str],
            **kwargs: object,
        ) -> subprocess.CompletedProcess[bytes]:
            requested.append(kwargs.get("text", True))
            if kwargs.get("text", True) is not False:
                raise UnicodeDecodeError("ascii", payload, 0, 1, "ordinal not in range(128)")
            return subprocess.CompletedProcess(args, 0, stdout=payload, stderr=b"")

        monkeypatch.setattr(production_deploy, "_run_process_group", fake_process_group)
        gate = production_deploy._build_r07_evidence_gate(_config(tmp_path))

        decision = gate.evaluate(
            repo=tmp_path,
            runner=FakeRunner({}),
            git_path=Path("/usr/bin/git"),
            installed_commit_sha=_sha("a"),
            target_commit_sha=_sha("b"),
        )

        assert requested == [False]
        assert decision.allowed is False
        assert decision.reason == reason

    def test_a_non_ascii_child_reason_survives_a_c_locale_child(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.undo()
        monkeypatch.setenv("LC_ALL", "C")
        monkeypatch.setenv("LANG", "C")
        monkeypatch.delenv("PYTHONUTF8", raising=False)
        repo, _pre_r07, release_a, _release_b = _release_repo(tmp_path)
        policy_path = repo / R07_POLICY_RELATIVE_PATH
        policy_path.write_bytes('{"schema_version":"\u7248\u672c"}'.encode())
        subprocess.run(
            ["/usr/bin/git", "add", R07_POLICY_RELATIVE_PATH],
            cwd=repo,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "/usr/bin/git",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "user.name=test",
                "commit",
                "--quiet",
                "-m",
                "broken policy",
            ],
            cwd=repo,
            check=True,
            capture_output=True,
        )
        broken_commit = subprocess.run(
            ["/usr/bin/git", "rev-parse", "HEAD"],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        gate = production_deploy._build_r07_evidence_gate(_config(tmp_path))

        decision = gate.evaluate(
            repo=repo,
            runner=FakeRunner({}),
            git_path=Path("/usr/bin/git"),
            installed_commit_sha=release_a,
            target_commit_sha=broken_commit,
        )

        assert decision.allowed is False
        assert decision.gate == "rejected"
        assert "\u7248\u672c" in decision.reason

    def test_lab_evidence_cache_follows_the_deployment_lock_root(
        self,
        tmp_path: Path,
    ) -> None:
        repo = tmp_path / "checkouts" / "rquant"
        lock_root = tmp_path / "state" / ".rquant-deploy"
        baseline = _config(tmp_path)
        config = DeployConfig(
            **{
                **baseline.__dict__,
                "repo": repo,
                "lock_path": lock_root / "rquant.lock",
                "release_profile": "macos-lab",
                "platform_name": "darwin",
                "runtime_production_inputs": None,
                "runtime_profile_output_dir": None,
                "runtime_root": None,
            }
        )

        resolved = production_deploy._resolve_r07_evidence_cache_dir(config)

        assert resolved == lock_root / "r07-dr-evidence"
        assert repo not in resolved.parents
        assert repo.parent not in resolved.parents

    def test_lab_evidence_cache_defaults_beside_the_default_lock(self, tmp_path: Path) -> None:
        repo = tmp_path / "checkouts" / "rquant"
        baseline = _config(tmp_path)
        config = DeployConfig(
            **{
                **baseline.__dict__,
                "repo": repo,
                "lock_path": None,
                "release_profile": "macos-lab",
                "platform_name": "darwin",
                "runtime_production_inputs": None,
                "runtime_profile_output_dir": None,
                "runtime_root": None,
            }
        )

        resolved = production_deploy._resolve_r07_evidence_cache_dir(config)

        assert resolved == production_deploy._deployment_lock_path(config).parent / (
            "r07-dr-evidence"
        )
        assert repo not in resolved.parents

    def test_default_gate_pins_the_production_cache_and_private_verifier(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.undo()
        gate = production_deploy._build_r07_evidence_gate(_config(tmp_path))

        assert type(gate) is production_deploy.IsolatedR07EvidenceGate
        assert gate.cache_dir == production_deploy.LINUX_PRODUCTION_EVIDENCE_CACHE_DIR
        assert gate.cache_dir == Path("/home/lighthouse/rquant/var/r07-dr-evidence")
        assert r07_deploy_evidence.DEFAULT_EVIDENCE_VERIFIER is differential_gate.verify_wire

    def test_the_production_cache_literal_is_the_same_in_every_copy(self) -> None:
        """The `-I -S` deployer cannot import the gate module, so it hand-copies the path."""

        assert (
            production_deploy.LINUX_PRODUCTION_EVIDENCE_CACHE_DIR
            == r07_deploy_evidence.LINUX_PRODUCTION_EVIDENCE_CACHE_DIR
        )
        assert Path(
            differential_gate.R07_EVIDENCE_CACHE_DIR
        ) == production_deploy.LINUX_PRODUCTION_EVIDENCE_CACHE_DIR

    def test_the_isolated_gate_pins_the_declared_cache_only_on_linux_production(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        commands: list[list[str]] = []

        class _Completed:
            returncode = 0
            stderr = b""

            def __init__(self, stdout: bytes) -> None:
                self.stdout = stdout

        payload = canonical_json_bytes(self._child_decision_payload())

        def _capture(command: list[str], **_kwargs: object) -> _Completed:
            commands.append(command)
            return _Completed(payload)

        monkeypatch.setattr(production_deploy, "_run_process_group", _capture)

        profiles = ((LINUX_RELEASE_PROFILE, True), (MACOS_LAB_RELEASE_PROFILE, False))
        for profile, expected in profiles:
            config = replace(_config(tmp_path), release_profile=profile)
            gate = production_deploy.IsolatedR07EvidenceGate(config, tmp_path / "cache")
            gate.evaluate(
                repo=tmp_path,
                runner=FakeRunner(_base_responses()),
                git_path=Path("/usr/bin/git"),
                installed_commit_sha=_sha("a"),
                target_commit_sha=_sha("b"),
            )
            assert ("--require-declared-cache-dir" in commands[-1]) is expected

    def test_isolated_gate_child_rejects_an_undeclared_cache_directory(
        self,
        tmp_path: Path,
    ) -> None:
        repo, pre_r07, release_a, _release_b = _release_repo(tmp_path)

        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                str(ROOT / production_deploy.R07_DEPLOY_GATE_SCRIPT),
                "--repo",
                str(repo),
                "--trusted-git-path",
                "/usr/bin/git",
                "--evidence-cache-dir",
                str(tmp_path / "var" / "r07-dr-evidence"),
                "--require-declared-cache-dir",
                "--installed-commit",
                pre_r07,
                "--target-commit",
                release_a,
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr
        decision = production_deploy.r07_decision_from_child_output(completed.stdout)
        assert decision.allowed is False
        assert decision.gate == "rejected"
        assert "cache" in decision.reason

    def test_deployer_still_imports_without_site_packages(self) -> None:
        program = (
            "import sys;"
            f"sys.path.insert(0, {str(ROOT / 'src')!r});"
            "import rquant.ops.production_deploy as module;"
            "print(module.__file__)"
        )
        isolated = subprocess.run(
            [sys.executable, "-I", "-S", "-c", program],
            capture_output=True,
            text=True,
            check=False,
        )
        pydantic = subprocess.run(
            [sys.executable, "-I", "-S", "-c", "import pydantic"],
            capture_output=True,
            text=True,
            check=False,
        )

        assert isolated.returncode == 0, isolated.stderr
        assert pydantic.returncode != 0

    def test_isolated_gate_child_reports_the_release_a_decision(self, tmp_path: Path) -> None:
        repo, pre_r07, release_a, _release_b = _release_repo(tmp_path)

        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                str(ROOT / production_deploy.R07_DEPLOY_GATE_SCRIPT),
                "--repo",
                str(repo),
                "--trusted-git-path",
                "/usr/bin/git",
                "--evidence-cache-dir",
                str(tmp_path / "var" / "r07-dr-evidence"),
                "--installed-commit",
                pre_r07,
                "--target-commit",
                release_a,
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr
        decision = production_deploy.r07_decision_from_child_output(completed.stdout)
        assert decision.allowed is True
        assert decision.gate == "bootstrap_disabled"
        assert decision.target_commit_sha == release_a

    def test_isolated_gate_child_reports_a_rejected_decision(self, tmp_path: Path) -> None:
        repo, pre_r07, release_a, _release_b = _release_repo(tmp_path)

        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                str(ROOT / production_deploy.R07_DEPLOY_GATE_SCRIPT),
                "--repo",
                str(repo),
                "--trusted-git-path",
                "/usr/bin/git",
                "--evidence-cache-dir",
                str(tmp_path / "var" / "r07-dr-evidence"),
                "--installed-commit",
                release_a,
                "--target-commit",
                pre_r07,
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr
        decision = production_deploy.r07_decision_from_child_output(completed.stdout)
        assert decision.allowed is False
        assert decision.gate == "rejected"
        assert "pre-R07" in decision.reason

    @staticmethod
    def _child_decision_payload(**overrides: object) -> dict[str, object]:
        values: dict[str, object] = {
            "allowed": True,
            "gate": "bootstrap_disabled",
            "reason": "Release A bootstrap install from a pre-R07 checkout",
            "requires_evidence": False,
            "installed_mode": "absent",
            "target_mode": "disabled_for_bootstrap",
            "installed_commit_sha": _sha("a"),
            "installed_tree_sha": R07_INSTALLED_TREE,
            "target_commit_sha": _sha("b"),
            "target_tree_sha": R07_TARGET_TREE,
        }
        values.update(overrides)
        return values

    def test_a_canonical_child_decision_is_accepted(self) -> None:
        raw = canonical_json_bytes(self._child_decision_payload())

        decision = production_deploy.r07_decision_from_child_output(raw)

        assert decision.allowed is True
        assert decision.gate == "bootstrap_disabled"
        assert decision.target_tree_sha == R07_TARGET_TREE

    @pytest.mark.parametrize(
        "raw",
        [
            pytest.param(b"", id="empty-output"),
            pytest.param(b"rejected", id="not-json"),
            pytest.param(b'"rejected"', id="not-an-object"),
            pytest.param(b"{}", id="no-keys"),
            pytest.param(b'{"allowed":true}', id="partial-keys"),
        ],
    )
    def test_a_malformed_child_decision_is_refused(self, raw: bytes) -> None:
        with pytest.raises(PolicyError, match="R07 deployment gate"):
            production_deploy.r07_decision_from_child_output(raw)

    def test_an_unresolved_rejection_may_omit_the_commit_and_tree_ids(self) -> None:
        raw = canonical_json_bytes(
            self._child_decision_payload(
                allowed=False,
                gate="rejected",
                reason="R07 policy objects could not be resolved",
                requires_evidence=False,
                installed_mode="unresolved",
                target_mode="unresolved",
                installed_commit_sha="",
                installed_tree_sha="",
                target_commit_sha="",
                target_tree_sha="",
            )
        )

        decision = production_deploy.r07_decision_from_child_output(raw)

        assert decision.allowed is False
        assert decision.target_commit_sha == ""

    def test_a_duplicate_child_decision_key_cannot_flip_the_verdict(self) -> None:
        rejected = canonical_json_bytes(
            self._child_decision_payload(
                allowed=False,
                gate="rejected",
                reason="target declares no R07 policy",
            )
        )
        flipped = rejected.replace(b'{"allowed":false', b'{"allowed":false,"allowed":true', 1)

        with pytest.raises(PolicyError, match="R07 deployment gate"):
            production_deploy.r07_decision_from_child_output(flipped)

    @pytest.mark.parametrize(
        "mutate",
        [
            pytest.param(lambda raw: raw + b"\n", id="trailing-newline"),
            pytest.param(lambda raw: raw + b'{"allowed":true}', id="second-record"),
            pytest.param(lambda raw: b" " + raw, id="leading-space"),
            pytest.param(
                lambda raw: json.dumps(json.loads(raw), indent=2).encode(),
                id="pretty-printed",
            ),
            pytest.param(
                lambda raw: json.dumps(
                    dict(reversed(list(json.loads(raw).items()))),
                    separators=(",", ":"),
                ).encode(),
                id="unsorted-keys",
            ),
        ],
    )
    def test_a_non_canonical_child_decision_is_refused(self, mutate) -> None:
        raw = mutate(canonical_json_bytes(self._child_decision_payload()))

        with pytest.raises(PolicyError, match="R07 deployment gate"):
            production_deploy.r07_decision_from_child_output(raw)

    @pytest.mark.parametrize(
        "overrides",
        [
            pytest.param({"gate": "totally-made-up"}, id="bogus-gate"),
            pytest.param({"gate": "Rejected"}, id="miscased-gate"),
            pytest.param(
                {"requires_evidence": True, "gate": "bootstrap_disabled"},
                id="evidence-without-enforcement",
            ),
            pytest.param({"allowed": False}, id="allowed-contradicts-gate"),
            pytest.param({"target_commit_sha": "not-a-sha"}, id="non-hex-commit"),
            pytest.param({"target_tree_sha": _sha("a").upper()}, id="uppercase-tree"),
            pytest.param({"installed_commit_sha": _sha("a")[:39]}, id="short-commit"),
            pytest.param({"allowed": "true"}, id="string-flag"),
            pytest.param({"reason": 7}, id="numeric-reason"),
            pytest.param({"target_tree_sha": ""}, id="allowed-without-target-tree"),
            pytest.param({"target_commit_sha": ""}, id="allowed-without-target-commit"),
            pytest.param({"installed_tree_sha": ""}, id="allowed-without-installed-tree"),
            pytest.param({"installed_commit_sha": ""}, id="allowed-without-installed-commit"),
        ],
    )
    def test_an_out_of_contract_child_decision_is_refused(
        self,
        overrides: dict[str, object],
    ) -> None:
        raw = canonical_json_bytes(self._child_decision_payload(**overrides))

        with pytest.raises(PolicyError, match="R07 deployment gate"):
            production_deploy.r07_decision_from_child_output(raw)

    def test_both_audit_field_sets_stay_identical(self) -> None:
        stdlib_fields = production_deploy.R07GateDecision(
            **self._child_decision_payload()  # type: ignore[arg-type]
        ).audit_fields()
        pydantic_fields = r07_deploy_evidence.R07DeployDecision(
            **self._child_decision_payload()  # type: ignore[arg-type]
        ).audit_fields()

        assert set(stdlib_fields) == set(pydantic_fields)
        assert stdlib_fields == pydantic_fields

    def test_linux_production_refuses_another_evidence_cache_directory(
        self,
        tmp_path: Path,
    ) -> None:
        baseline = _config(tmp_path)
        config = DeployConfig(
            **{**baseline.__dict__, "r07_evidence_cache_dir": tmp_path / "var"}
        )

        with pytest.raises(PolicyError, match="evidence cache"):
            deploy(config, runner=FakeRunner(_base_responses()))
