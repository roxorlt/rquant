"""Behavioral tests for the cloud research-ingest readiness runner."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _write_executable(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _prepare_runner(tmp_path: Path) -> tuple[Path, dict[str, str], Path]:
    project = tmp_path / "rquant"
    script = project / "scripts" / "run-research-ingest-daily.sh"
    script.parent.mkdir(parents=True)
    script.write_text(
        (ROOT / "scripts" / "run-research-ingest-daily.sh").read_text(
            encoding="utf-8"
        ),
        encoding="utf-8",
    )
    script.chmod(0o755)
    calls = project / "calls.log"
    fake_bin = tmp_path / "bin"
    _write_executable(
        fake_bin / "systemctl",
        """#!/usr/bin/env bash
cat <<EOF
ActiveState=${RUNNER_DAILY_ACTIVE_STATE:-inactive}
Result=${RUNNER_DAILY_RESULT:-success}
ExecMainStatus=${RUNNER_DAILY_STATUS:-0}
ExecMainExitTimestamp=${RUNNER_DAILY_EXIT:-Fri 2026-07-17 17:58:00 CST}
EOF
""",
    )
    _write_executable(
        fake_bin / "date",
        """#!/usr/bin/env bash
if [[ "${1:-}" == "+%F" ]]; then
  printf '2026-07-17\\n'
elif [[ "${1:-}" == "-d" ]]; then
  printf '%s\\n' "${RUNNER_EXIT_DATE:-2026-07-17}"
else
  exec /bin/date "$@"
fi
""",
    )
    _write_executable(
        project / ".venv" / "bin" / "rquant",
        """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "${RUNNER_CALLS}"
if [[ "${1:-}" == "research-ingest-readiness" ]]; then
  exit "${RUNNER_READINESS_EXIT:-0}"
fi
attempt=0
if [[ -f "${RUNNER_ATTEMPT_FILE}" ]]; then
  attempt=$(cat "${RUNNER_ATTEMPT_FILE}")
fi
attempt=$((attempt + 1))
printf '%s\\n' "${attempt}" > "${RUNNER_ATTEMPT_FILE}"
if (( attempt <= ${RUNNER_INGEST_FAILURES:-0} )); then
  exit "${RUNNER_INGEST_EXIT:-1}"
fi
exit 0
""",
    )
    _write_executable(
        fake_bin / "sleep",
        """#!/usr/bin/env bash
printf 'sleep %s\\n' "${1:-}" >> "${RUNNER_CALLS}"
""",
    )
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["RUNNER_CALLS"] = str(calls)
    env["RUNNER_ATTEMPT_FILE"] = str(project / "attempt.txt")
    env["RQUANT_RESEARCH_INGEST_MAX_ATTEMPTS"] = "1"
    env["RQUANT_RESEARCH_INGEST_RETRY_SECONDS"] = "0"
    return script, env, calls


def test_runner_only_checks_required_replica_readiness_before_ingest(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_runner(tmp_path)

    result = subprocess.run(
        [str(script)],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert calls.read_text(encoding="utf-8").splitlines() == [
        "research-ingest-readiness --date 2026-07-17",
        "research-ingest --date 2026-07-17 --scheduled",
    ]


def test_runner_has_no_replica_sync_or_arbiter_path_override() -> None:
    content = (ROOT / "scripts/run-research-ingest-daily.sh").read_text(
        encoding="utf-8"
    )

    assert "sync-readonly-replica.sh" not in content
    assert "RQUANT_WORKLOAD_ARBITER" not in content
    assert "--research-phase" not in content


def test_runner_rejects_daily_that_did_not_finish_today(tmp_path: Path) -> None:
    script, env, calls = _prepare_runner(tmp_path)
    env["RUNNER_EXIT_DATE"] = "2026-07-16"

    result = subprocess.run(
        [str(script)],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 1
    assert not calls.exists()
    assert "did not complete successfully today" in result.stderr


def test_runner_normalizes_readiness_failure_to_retryable_one(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_runner(tmp_path)
    env["RUNNER_READINESS_EXIT"] = "1"
    readiness_failure = subprocess.run(
        [str(script)], capture_output=True, text=True, env=env
    )

    assert readiness_failure.returncode == 1
    assert calls.read_text(encoding="utf-8").splitlines() == [
        "research-ingest-readiness --date 2026-07-17",
    ]


def test_runner_retries_exactly_four_times_for_one_fixed_trade_date(
    tmp_path: Path,
) -> None:
    script, env, calls = _prepare_runner(tmp_path)
    env["RQUANT_RESEARCH_INGEST_MAX_ATTEMPTS"] = "4"
    env["RUNNER_INGEST_FAILURES"] = "4"

    result = subprocess.run(
        [str(script)], capture_output=True, text=True, env=env
    )

    assert result.returncode == 1
    lines = calls.read_text(encoding="utf-8").splitlines()
    assert lines.count("research-ingest --date 2026-07-17 --scheduled") == 4
    assert lines.count("sleep 0") == 3
    assert all("2026-07-18" not in line for line in lines)


def test_runner_does_not_retry_degraded_or_disabled_exit(tmp_path: Path) -> None:
    for exit_code in (2, 3):
        script, env, calls = _prepare_runner(tmp_path / str(exit_code))
        env["RQUANT_RESEARCH_INGEST_MAX_ATTEMPTS"] = "4"
        env["RUNNER_INGEST_FAILURES"] = "4"
        env["RUNNER_INGEST_EXIT"] = str(exit_code)

        result = subprocess.run(
            [str(script)], capture_output=True, text=True, env=env
        )

        assert result.returncode == exit_code
        lines = calls.read_text(encoding="utf-8").splitlines()
        assert lines.count("research-ingest --date 2026-07-17 --scheduled") == 1
        assert not any(line.startswith("sleep ") for line in lines)
