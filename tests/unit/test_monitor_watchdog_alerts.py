"""Behavior tests for watchdog incident recovery."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


def _prepare_watchdog(tmp_path: Path) -> tuple[Path, dict[str, str], Path]:
    project = tmp_path / "rquant"
    scripts = project / "scripts"
    fake_bin = tmp_path / "bin"
    calls = tmp_path / "rquant-calls"
    scripts.mkdir(parents=True)
    fake_bin.mkdir()
    (project / ".venv" / "bin").mkdir(parents=True)
    source = Path(__file__).parents[2] / "scripts" / "monitor-watchdog.sh"
    watchdog = scripts / "monitor-watchdog.sh"
    shutil.copy2(source, watchdog)

    systemctl = fake_bin / "systemctl"
    systemctl.write_text(
        """#!/usr/bin/env bash
set -u
if [[ "$1" == "is-active" ]]; then
  unit="${@: -1}"
  [[ "${unit}" == "rquant-monitor.service" || \
     ( "${unit}" == "rquant-surge-watch.service" && "${SURGE_ACTIVE:-0}" == "1" ) ]]
  exit $?
fi
if [[ "$1" == "show" && "${@: -1}" == "--value" ]]; then
  echo "Thu 2026-07-16 09:25:00 CST"
  exit 0
fi
exit 1
""",
        encoding="utf-8",
    )
    systemctl.chmod(0o755)

    date = fake_bin / "date"
    date.write_text(
        """#!/usr/bin/env bash
case "$*" in
  "+%H%M") echo 1000 ;;
  "+%Y-%m-%d") echo 2026-07-16 ;;
  "-Iseconds") echo 2026-07-16T10:00:00+08:00 ;;
  "+%s") echo 1000 ;;
  -d*) echo "${ACTIVE_EPOCH}" ;;
  *) /bin/date "$@" ;;
esac
""",
        encoding="utf-8",
    )
    date.chmod(0o755)

    rquant = project / ".venv" / "bin" / "rquant"
    rquant.write_text(
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "${RQUANT_CALLS}"
""",
        encoding="utf-8",
    )
    rquant.chmod(0o755)

    env = dict(os.environ)
    env["PATH"] = f"{fake_bin}:/usr/bin:/bin"
    env["RQUANT_CALLS"] = str(calls)
    return watchdog, env, calls


def test_watchdog_does_not_resolve_short_lived_process(
    tmp_path: Path,
) -> None:
    watchdog, env, calls = _prepare_watchdog(tmp_path)
    env["ACTIVE_EPOCH"] = "900"

    subprocess.run([str(watchdog)], env=env, check=True)

    assert not calls.exists()
    log = next((watchdog.parent.parent / "logs").glob("watchdog-*.log"))
    assert "active-warming" in log.read_text(encoding="utf-8")


def test_watchdog_resolves_incident_after_five_stable_minutes(
    tmp_path: Path,
) -> None:
    watchdog, env, calls = _prepare_watchdog(tmp_path)
    env["ACTIVE_EPOCH"] = "600"
    env["SURGE_ACTIVE"] = "1"

    subprocess.run([str(watchdog)], env=env, check=True)

    lines = calls.read_text(encoding="utf-8").splitlines()
    assert lines == [
        "alert-resolve --dedup-key service:rquant-surge-watch.service",
        "alert-resolve --dedup-key service-critical:rquant-surge-watch.service",
        "alert-resolve --dedup-key service:rquant-monitor.service",
        "alert-resolve --dedup-key service-critical:rquant-monitor.service",
    ]
