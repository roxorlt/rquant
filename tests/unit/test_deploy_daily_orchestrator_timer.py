from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_accept_enables_exact_daily_orchestrator_instance_on_same_head(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env["RQUANT_DAILY_ORCHESTRATOR_INSTANCE"] = "svc-shadow"

    result = subprocess.run(
        ["/bin/bash", str(script), "--accept-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    call_lines = calls.read_text(encoding="utf-8").splitlines()
    assert (
        "sudo systemctl enable rquant-runtime-daily-orchestrator@svc-shadow.timer"
        in call_lines
    )
    assert not _contains_call(call_lines, "systemctl start")
    assert not _contains_call(call_lines, "systemctl restart")
    assert _contains_call(
        call_lines,
        "systemctl show rquant-runtime-daily-orchestrator@svc-shadow.timer "
        "-p UnitFileState --value",
    )
    assert _contains_call(
        call_lines,
        "systemctl show rquant-runtime-daily-orchestrator@svc-shadow.timer -p ActiveState --value",
    )
    assert not _contains_call(call_lines, "NextElapseUSecRealtime")


def test_unaccepted_daily_orchestrator_template_stays_disabled_and_skips_next(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(
        tmp_path,
        changed_files="deploy/systemd/rquant-runtime-daily-orchestrator@.timer",
    )

    result = subprocess.run(
        ["/bin/bash", str(script)],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    call_lines = calls.read_text(encoding="utf-8").splitlines()
    assert not _contains_call(call_lines, "systemctl enable")
    assert not _contains_call(call_lines, "systemctl start")
    assert not _contains_call(call_lines, "systemctl restart")
    assert not _contains_call(call_lines, "NextElapseUSecRealtime")
    assert "daily orchestrator timer remains disabled by default" in result.stdout


def test_accept_changed_daily_orchestrator_template_validates_exact_inactive_instance(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(
        tmp_path,
        changed_files="deploy/systemd/rquant-runtime-daily-orchestrator@.timer",
    )
    env["RQUANT_DAILY_ORCHESTRATOR_INSTANCE"] = "svc-shadow"

    result = subprocess.run(
        ["/bin/bash", str(script), "--accept-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    call_lines = calls.read_text(encoding="utf-8").splitlines()
    assert (
        "sudo systemctl enable rquant-runtime-daily-orchestrator@svc-shadow.timer"
        in call_lines
    )
    assert not _contains_call(call_lines, "systemctl start")
    assert not _contains_call(call_lines, "systemctl restart")
    assert not _contains_call(
        call_lines,
        "systemctl show rquant-runtime-daily-orchestrator@.timer",
    )
    assert not _contains_call(call_lines, "NextElapseUSecRealtime")
    assert _contains_call(
        call_lines,
        "systemctl show rquant-runtime-daily-orchestrator@svc-shadow.timer "
        "-p UnitFileState --value",
    )
    assert _contains_call(
        call_lines,
        "systemctl show rquant-runtime-daily-orchestrator@svc-shadow.timer -p ActiveState --value",
    )


def test_accept_repeated_for_enabled_inactive_daily_orchestrator_does_not_restart(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(
        tmp_path,
        changed_files="deploy/systemd/rquant-runtime-daily-orchestrator@.timer",
    )
    env["RQUANT_DAILY_ORCHESTRATOR_INSTANCE"] = "svc-shadow"
    env["DEPLOY_TEST_ENABLED_UNIT"] = (
        "rquant-runtime-daily-orchestrator@svc-shadow.timer"
    )

    result = subprocess.run(
        ["/bin/bash", str(script), "--accept-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    call_lines = calls.read_text(encoding="utf-8").splitlines()
    assert (
        "sudo systemctl enable rquant-runtime-daily-orchestrator@svc-shadow.timer"
        in call_lines
    )
    assert not _contains_call(call_lines, "systemctl start")
    assert not _contains_call(call_lines, "systemctl restart")
    assert not _contains_call(call_lines, "NextElapseUSecRealtime")


def test_accept_missing_instance_fails_fast(tmp_path: Path) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env.pop("RQUANT_DAILY_ORCHESTRATOR_INSTANCE", None)

    result = subprocess.run(
        ["/bin/bash", str(script), "--accept-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert not calls.exists() or calls.read_text(encoding="utf-8") == ""


def test_accept_rejects_illegal_instance_name(tmp_path: Path) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env["RQUANT_DAILY_ORCHESTRATOR_INSTANCE"] = "bad;rm -rf"

    result = subprocess.run(
        ["/bin/bash", str(script), "--accept-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert not _contains_call(_read_calls(calls), "systemctl enable")


def test_accept_fails_when_unit_file_state_wrong(tmp_path: Path) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env["RQUANT_DAILY_ORCHESTRATOR_INSTANCE"] = "svc-shadow"
    env["DEPLOY_TEST_UNIT_FILE_STATE"] = "disabled"

    result = subprocess.run(
        ["/bin/bash", str(script), "--accept-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0, result.stdout + result.stderr


def test_start_daily_orchestrator_timer_requires_instance(tmp_path: Path) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env.pop("RQUANT_DAILY_ORCHESTRATOR_INSTANCE", None)

    result = subprocess.run(
        ["/bin/bash", str(script), "--start-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert not calls.exists() or calls.read_text(encoding="utf-8") == ""


def test_start_daily_orchestrator_timer_rejects_illegal_instance_name(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env["RQUANT_DAILY_ORCHESTRATOR_INSTANCE"] = "bad/name"

    result = subprocess.run(
        ["/bin/bash", str(script), "--start-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert not _contains_call(_read_calls(calls), "systemctl start")


def test_start_daily_orchestrator_timer_starts_exact_instance_without_enable(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env["RQUANT_DAILY_ORCHESTRATOR_INSTANCE"] = "svc-shadow"
    env["DEPLOY_TEST_ACTIVE_STATE"] = "active"
    env["DEPLOY_TEST_SUBSTATE"] = "waiting"

    result = subprocess.run(
        ["/bin/bash", str(script), "--start-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    call_lines = calls.read_text(encoding="utf-8").splitlines()
    assert (
        "sudo systemctl start rquant-runtime-daily-orchestrator@svc-shadow.timer"
        in call_lines
    )
    assert not _contains_call(call_lines, "systemctl enable")
    assert _contains_call(
        call_lines,
        "systemctl show rquant-runtime-daily-orchestrator@svc-shadow.timer -p ActiveState --value",
    )
    assert _contains_call(
        call_lines,
        "systemctl show rquant-runtime-daily-orchestrator@svc-shadow.timer "
        "-p NextElapseUSecRealtime --value",
    )


def test_start_daily_orchestrator_timer_repeated_is_idempotent(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env["RQUANT_DAILY_ORCHESTRATOR_INSTANCE"] = "svc-shadow"
    env["DEPLOY_TEST_ACTIVE_STATE"] = "active"
    env["DEPLOY_TEST_SUBSTATE"] = "waiting"

    for _ in range(2):
        result = subprocess.run(
            ["/bin/bash", str(script), "--start-daily-orchestrator-timer"],
            cwd=script.parent.parent,
            env=env,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    call_lines = calls.read_text(encoding="utf-8").splitlines()
    assert not _contains_call(call_lines, "systemctl enable")


def test_start_daily_orchestrator_timer_fails_when_active_state_wrong(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env["RQUANT_DAILY_ORCHESTRATOR_INSTANCE"] = "svc-shadow"
    env["DEPLOY_TEST_ACTIVE_STATE"] = "inactive"
    env["DEPLOY_TEST_SUBSTATE"] = "dead"

    result = subprocess.run(
        ["/bin/bash", str(script), "--start-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0


def test_start_daily_orchestrator_timer_fails_when_next_unresolvable(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env["RQUANT_DAILY_ORCHESTRATOR_INSTANCE"] = "svc-shadow"
    env["DEPLOY_TEST_ACTIVE_STATE"] = "active"
    env["DEPLOY_TEST_SUBSTATE"] = "waiting"
    env["DEPLOY_TEST_DAILY_NEXT"] = "n/a"

    result = subprocess.run(
        ["/bin/bash", str(script), "--start-daily-orchestrator-timer"],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0


def test_ordinary_timer_next_trigger_parses_on_macos_style_bsd_date(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(
        tmp_path,
        changed_files="deploy/systemd/rquant-monitor.timer",
    )

    result = subprocess.run(
        ["/bin/bash", str(script)],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "下次 trigger = n/a" not in result.stdout
    assert "无法解析下次 trigger" not in result.stdout
    assert "下次 trigger 在过去" not in result.stdout


def _prepare_deploy_fixture(
    tmp_path: Path,
    *,
    changed_files: str,
) -> tuple[Path, dict[str, str], Path]:
    project = tmp_path / "rquant"
    script = project / "scripts" / "deploy.sh"
    systemd = project / "deploy" / "systemd"
    fake_bin = tmp_path / "bin"
    calls = tmp_path / "calls.log"
    state = tmp_path / "state"
    script.parent.mkdir(parents=True)
    systemd.mkdir(parents=True)
    fake_bin.mkdir()
    state.mkdir()
    shutil.copy2(ROOT / "scripts" / "deploy.sh", script)
    script.chmod(0o755)
    _write_executable(
        project / "scripts" / "install-runtime-credential-infra.sh",
        """#!/usr/bin/env bash
printf 'installer runtime-credential\\n' >> "${DEPLOY_TEST_CALLS}"
exit 0
""",
    )
    _write_executable(
        project / "scripts" / "install-workload-isolation-infra.sh",
        """#!/usr/bin/env bash
printf 'installer workload-isolation\\n' >> "${DEPLOY_TEST_CALLS}"
exit "${DEPLOY_TEST_WORKLOAD_INSTALL_EXIT:-0}"
""",
    )
    for name in (
        "rquant-monitor.service",
        "rquant-runtime-daily-orchestrator@.timer",
        "rquant-serving.slice",
    ):
        (systemd / name).write_text("[Unit]\nDescription=test\n", encoding="utf-8")
    _write_fake_git(fake_bin / "git", calls, state, changed_files)
    _write_fake_sudo(fake_bin / "sudo", calls)
    _write_fake_systemctl(fake_bin / "systemctl", calls)
    _write_executable(
        fake_bin / "sleep",
        """#!/usr/bin/env bash
exit 0
""",
    )
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["DEPLOY_TEST_CALLS"] = str(calls)
    env["DEPLOY_TEST_STATE"] = str(state)
    env["DEPLOY_TEST_PRE_HEAD"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    env["DEPLOY_TEST_POST_HEAD"] = (
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        if changed_files == ""
        else "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    )
    env["DEPLOY_TEST_CHANGED_FILES"] = changed_files
    env["DEPLOY_TEST_UNIT_FILE_STATE"] = "enabled"
    env["DEPLOY_TEST_ACTIVE_STATE"] = "inactive"
    return script, env, calls


def test_deployer_reconciles_workload_after_credentials_and_propagates_failure(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_deploy_fixture(tmp_path, changed_files="")
    env["DEPLOY_TEST_WORKLOAD_INSTALL_EXIT"] = "23"

    result = subprocess.run(
        ["/bin/bash", str(script)],
        cwd=script.parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    observed = _read_calls(calls)
    assert observed.index("installer runtime-credential") < observed.index(
        "installer workload-isolation"
    )
    assert not any("systemctl daemon-reload" in call for call in observed)
    assert "workload isolation candidate infrastructure" in result.stdout


def _write_fake_git(path: Path, calls: Path, state: Path, changed_files: str) -> None:
    _write_executable(
        path,
        f"""#!/usr/bin/env bash
printf 'git %s\\n' "$*" >> {str(calls)!r}
if [[ "$*" == "rev-parse --abbrev-ref HEAD" ]]; then
  printf 'main\\n'
  exit 0
fi
if [[ "$*" == "status --porcelain" ]]; then
  exit 0
fi
if [[ "$*" == "pull --ff-only" ]]; then
  exit 0
fi
if [[ "$*" == "rev-parse HEAD" ]]; then
  count_file={str(state / "rev-parse-head-count")!r}
  count=0
  if [[ -f "${{count_file}}" ]]; then
    count=$(cat "${{count_file}}")
  fi
  count=$((count + 1))
  printf '%s\\n' "${{count}}" > "${{count_file}}"
  if [[ "${{count}}" == "1" ]]; then
    printf '%s\\n' "${{DEPLOY_TEST_PRE_HEAD}}"
  else
    printf '%s\\n' "${{DEPLOY_TEST_POST_HEAD}}"
  fi
  exit 0
fi
if [[ "${{1:-}}" == "diff" && "${{2:-}}" == "--name-only" ]]; then
  printf '%s\\n' {changed_files!r}
  exit 0
fi
exit 1
""",
    )


def _write_fake_sudo(path: Path, calls: Path) -> None:
    _write_executable(
        path,
        f"""#!/usr/bin/env bash
printf 'sudo %s\\n' "$*" >> {str(calls)!r}
if [[ "${{1:-}}" == "systemctl" ]]; then
  shift
  exec systemctl "$@"
fi
exit 0
""",
    )


def _write_fake_systemctl(path: Path, calls: Path) -> None:
    _write_executable(
        path,
        f"""#!/usr/bin/env bash
printf 'systemctl %s\\n' "$*" >> {str(calls)!r}
case "${{1:-}}" in
  daemon-reload)
    exit 0
    ;;
  enable)
    exit 0
    ;;
  restart|start)
    exit 0
    ;;
  is-enabled)
    if [[ "${{2:-}}" == "${{DEPLOY_TEST_ENABLED_UNIT:-}}" ]]; then
      printf 'enabled\\n'
      exit 0
    fi
    printf 'disabled\\n'
    exit 1
    ;;
  is-active)
    printf 'inactive\\n'
    exit 3
    ;;
  list-timers)
    exit 0
    ;;
  show)
    unit="${{2:-}}"
    property="${{4:-}}"
    if [[ "${{property}}" == "UnitFileState" ]]; then
      printf '%s\\n' "${{DEPLOY_TEST_UNIT_FILE_STATE}}"
      exit 0
    fi
    if [[ "${{property}}" == "ActiveState" ]]; then
      printf '%s\\n' "${{DEPLOY_TEST_ACTIVE_STATE}}"
      exit 0
    fi
    if [[ "${{property}}" == "SubState" ]]; then
      printf '%s\\n' "${{DEPLOY_TEST_SUBSTATE:-waiting}}"
      exit 0
    fi
    if [[ "${{property}}" == "NextElapseUSecRealtime" ]]; then
      if [[ "${{unit}}" == "rquant-runtime-daily-orchestrator@.timer" ]]; then
        printf 'n/a\\n'
      elif [[ "${{unit}}" == rquant-runtime-daily-orchestrator@*.timer ]]; then
        printf '%s\\n' "${{DEPLOY_TEST_DAILY_NEXT:-Wed 2026-08-05 17:15:00 CST}}"
      else
        printf 'Wed 2026-08-05 17:15:00 CST\\n'
      fi
      exit 0
    fi
    ;;
esac
exit 1
""",
    )


def _write_executable(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _read_calls(calls: Path) -> list[str]:
    if not calls.exists():
        return []
    return calls.read_text(encoding="utf-8").splitlines()


def _contains_call(call_lines: list[str], needle: str) -> bool:
    return any(needle in line for line in call_lines)
