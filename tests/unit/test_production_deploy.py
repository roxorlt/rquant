"""Controlled production deployment policy and orchestration tests."""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from rquant.ops.production_deploy import (
    ALL_LONG_RUNNING_SERVICES,
    DeployConfig,
    DeployError,
    PolicyError,
    ProtectedWindowError,
    SubprocessRunner,
    build_change_plan,
    build_parser,
    deploy,
    is_protected_market_window,
    validate_target,
)


class FakeRunner:
    def __init__(self, responses: dict[tuple[str, ...], tuple[int, str]] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[tuple[str, ...]] = []

    def run(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        key = tuple(args)
        self.calls.append(key)
        returncode, stdout = self.responses.get(key, (0, ""))
        result = subprocess.CompletedProcess(args, returncode, stdout=stdout, stderr="")
        if check and returncode != 0:
            raise subprocess.CalledProcessError(
                returncode, args, output=stdout, stderr=""
            )
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


def test_change_plan_restarts_all_for_shared_runtime_or_unknown_source() -> None:
    shared = build_change_plan(["src/rquant/config.py"])
    unknown = build_change_plan(["src/rquant/new_runtime.py"])

    assert shared.restart_services == ALL_LONG_RUNNING_SERVICES
    assert unknown.restart_services == ALL_LONG_RUNNING_SERVICES


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
    assert ("git", "merge", "--ff-only", _sha("b")) not in runner.calls
    assert ("uv", "sync", "--frozen") not in runner.calls


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
            "now": datetime(
                2026, 7, 13, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")
            ),
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
    assert runner.calls.count(
        ("systemctl", "is-active", "rquant-monitor.service")
    ) == 2


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

    result = deploy(_config(tmp_path), runner=runner)

    assert result.status == "deployed"
    assert ("git", "merge", "--ff-only", _sha("b")) in runner.calls
    assert ("uv", "sync", "--frozen") in runner.calls
    assert runner.calls.count(("rquant", "preflight")) == 2
    audit = (_config(tmp_path).audit_path).read_text(encoding="utf-8")
    assert '"status": "deployed"' in audit
    assert f'"target_sha": "{_sha("b")}"' in audit


def test_failed_preflight_rolls_back_code_and_dependencies(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("rquant", "preflight")] = (1, "preflight failed")
    runner = FakeRunner(responses)

    with pytest.raises(DeployError, match="rolled back"):
        deploy(_config(tmp_path), runner=runner)

    merge_index = runner.calls.index(("git", "merge", "--ff-only", _sha("b")))
    reset_index = runner.calls.index(("git", "reset", "--hard", _sha("a")))
    assert reset_index > merge_index
    assert runner.calls.count(("uv", "sync", "--frozen")) == 2
    audit = (_config(tmp_path).audit_path).read_text(encoding="utf-8")
    assert '"status": "rolled_back"' in audit


def test_failed_merge_attempt_still_restores_previous_head(tmp_path: Path) -> None:
    responses = _base_responses()
    responses[("git", "merge", "--ff-only", _sha("b"))] = (1, "merge failed")
    runner = FakeRunner(responses)

    with pytest.raises(DeployError, match="rolled back"):
        deploy(_config(tmp_path), runner=runner)

    assert ("git", "reset", "--hard", _sha("a")) in runner.calls


def test_shell_entrypoint_exposes_controlled_deployer_help() -> None:
    repo = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        ["bash", "scripts/deploy-production.sh", "--help"],
        cwd=repo,
        env={"RQUANT_DEPLOY_PYTHON": sys.executable},
        capture_output=True,
        text=True,
        check=True,
    )

    assert "SemVer tag or full 40-character SHA" in result.stdout


def test_cli_does_not_allow_overriding_production_executables() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["--target", "v0.13.2", "--uv-bin", "/tmp/untrusted"]
        )


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


def test_real_git_repository_deploys_annotated_fast_forward_tag(tmp_path: Path) -> None:
    repo = tmp_path / "prod"
    origin = tmp_path / "origin.git"
    repo.mkdir()

    def git(*args: str) -> str:
        result = subprocess.run(
            ["git", *args],
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
        ["git", "clone", "--bare", str(repo), str(origin)],
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
    )

    result = deploy(config)

    assert result.status == "deployed"
    assert result.target_sha == target_sha
    assert git("rev-parse", "HEAD") == target_sha
    assert '"status": "deployed"' in config.audit_path.read_text(encoding="utf-8")
