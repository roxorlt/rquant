"""Controlled production deployment policy and orchestration tests."""

from __future__ import annotations

import fcntl
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import rquant.ops.production_deploy as production_deploy
from rquant.ops.production_deploy import (
    ALL_LONG_RUNNING_SERVICES,
    LAB_LAUNCHD_HANDOFF_LABELS,
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


class FakeRunner:
    def __init__(self, responses: dict[tuple[str, ...], tuple[int, str]] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[tuple[str, ...]] = []
        self.executed_calls: list[tuple[str, ...]] = []

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
        returncode, stdout = self.responses.get(executed, self.responses.get(key, (0, "")))
        result = subprocess.CompletedProcess(args, returncode, stdout=stdout, stderr="")
        if check and returncode != 0:
            raise subprocess.CalledProcessError(returncode, args, output=stdout, stderr="")
        return result


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
    assert ("git", "merge", "--ff-only", _sha("b")) in runner.calls
    assert ("uv", "sync", "--frozen") in runner.calls
    assert runner.calls.count(("rquant", "preflight")) == 2
    audit = (_config(tmp_path).audit_path).read_text(encoding="utf-8")
    assert '"status": "deployed"' in audit
    assert f'"target_sha": "{_sha("b")}"' in audit
    assert authority.events[0:2] == [("intent", _sha("b")), ("invalidate", None)]
    assert authority.intent is not None and authority.intent.stage == "completed"
    assert finalizer.calls == [
        (_sha("b"), authority.intent.operation_id, "deploy", "publish"),
        (_sha("b"), authority.intent.operation_id, "deploy", "commit"),
    ]


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
        }
    )
    runner = FakeRunner(responses)
    authority = FakeGenerationAuthority()

    result = deploy(
        config,
        runner=runner,
        generation_authority=authority,
        generation_finalizer=FakeGenerationFinalizer(),
    )

    assert result.status == "deployed"
    assert result.handoff_daemons == LAB_LAUNCHD_HANDOFF_LABELS
    assert authority.intent is not None
    assert authority.intent.restart_services == ()
    assert not any("systemctl" in command for command in runner.calls)


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
    runner = CrashAfterRunner(
        _base_responses(),
        command=command,
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
    authority.update_deployment_intent(
        operation_id=authority.intent.operation_id,
        stage="services_transitioning",
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
    authority.update_deployment_intent(
        operation_id=intent.operation_id,
        stage="services_transitioning",
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
    authority.update_deployment_intent(
        operation_id=intent.operation_id,
        stage="services_transitioning",
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
    assert runner.calls.count(("rquant", "preflight")) == 2
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
        command=("rquant", "preflight"),
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
    assert runner.calls.count(("rquant", "preflight")) == 3
    assert authority.events[0:2] == [("intent", _sha("b")), ("invalidate", None)]
    assert finalizer.calls == [
        (_sha("a"), authority.intent.operation_id, "rollback", "publish"),
        (_sha("a"), authority.intent.operation_id, "rollback", "commit"),
    ]
    assert all(call[0] != "git" for call in runner.executed_calls)
    audit = (_config(tmp_path).audit_path).read_text(encoding="utf-8")
    assert '"status": "rolled_back"' in audit


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


def test_subprocess_runner_bounds_each_command_and_overall_rollout(tmp_path: Path) -> None:
    runner = SubprocessRunner(
        tmp_path,
        command_timeout_seconds=0.05,
        overall_timeout_seconds=0.1,
    )

    with pytest.raises(DeployError, match="timed out"):
        runner.run([sys.executable, "-c", "import time; time.sleep(1)"])


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
    )

    result = deploy(config)

    assert result.status == "deployed"
    assert result.target_sha == target_sha
    assert git("rev-parse", "HEAD") == target_sha
    assert not fake_git_called.exists()
    assert '"status": "deployed"' in config.audit_path.read_text(encoding="utf-8")
