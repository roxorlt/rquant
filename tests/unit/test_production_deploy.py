"""Controlled production deployment policy and orchestration tests."""

from __future__ import annotations

import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import rquant.ops.production_deploy as production_deploy
from rquant.contained_subprocess import ContainedProcessError
from rquant.ops.production_deploy import (
    ALL_LONG_RUNNING_SERVICES,
    LAB_LAUNCHD_HANDOFF_LABELS,
    LINUX_PRODUCTION_RUNTIME_ROOT,
    DeployConfig,
    DeployError,
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
from rquant.release_generation import DeploymentIntent

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
    responses = _base_responses()
    responses[("git", "rev-parse", "HEAD")] = (0, f"{_sha('b')}\n")
    lock_path = tmp_path / "deploy.lock"
    lock_path.write_bytes(b"stable-lock")
    lock_path.chmod(0o600)
    baseline = _config(tmp_path, dry_run=True)
    config = DeployConfig(**{**baseline.__dict__, "lock_path": lock_path})
    runner = FakeRunner(responses)

    before = {
        str(path.relative_to(tmp_path)): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    result = deploy(config, runner=runner)
    after = {
        str(path.relative_to(tmp_path)): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert result.status == "already_current"
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
    assert 'PYTHON_BIN="${PROJECT_DIR}/.venv/bin/python"' in source
    assert 'PYTHON_BIN="${RQUANT_DEPLOY_PYTHON:-' not in source
    assert "bootstrap-production-deploy.py" in source
    assert '--uv-path "${UV_BIN}"' in source
    assert '-- "$@"' not in source
    assert "-m rquant.ops.production_deploy" not in source
    assert "/../.rquant-deploy" not in source


def test_shell_entrypoint_rejects_environment_interpreter_override(
    tmp_path: Path,
) -> None:
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\nprintf 'untrusted interpreter executed\\n' >&2\nexit 0\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o700)
    environment = {
        **os.environ,
        "RQUANT_DEPLOY_PYTHON": str(fake_python),
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
    assert "RQUANT_DEPLOY_PYTHON is not supported" in result.stderr
    assert "untrusted interpreter executed" not in result.stderr


def test_shell_entrypoint_rejects_partial_runtime_profile_environment(
    tmp_path: Path,
) -> None:
    environment = {
        **os.environ,
        "RQUANT_RUNTIME_PRODUCTION_INPUTS": str(tmp_path / "runtime-inputs.json"),
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
    assert "runtime production inputs, profile output directory, and root" in result.stderr


def test_shell_entrypoint_rejects_linux_production_without_runtime_profile(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_uname = fake_bin / "uname"
    fake_uname.write_text("#!/bin/sh\nprintf 'Linux\\n'\n", encoding="utf-8")
    fake_uname.chmod(0o700)
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
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
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
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
    git("add", "src/rquant/preflight.py")
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
