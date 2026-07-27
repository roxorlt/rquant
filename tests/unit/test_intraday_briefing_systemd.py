"""Production systemd contracts for cloud intraday briefing jobs."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"


def _read(name: str) -> str:
    return (SYSTEMD / name).read_text(encoding="utf-8")


def test_morning_pulse_service_uses_cloud_readonly_inputs() -> None:
    content = _read("rquant-morning-pulse.service")

    assert "After=network-online.target rquant-panorama.service" in content
    assert "OnFailure=rquant-alert@%n.service" in content
    assert "User=lighthouse" in content
    assert "Group=lighthouse" in content
    assert "WorkingDirectory=/home/lighthouse/rquant" in content
    assert "EnvironmentFile=/home/lighthouse/rquant/.env" in content
    assert "Environment=TZ=Asia/Shanghai" in content
    assert "Environment=RQUANT_PANORAMA_SOCKS=" in content
    assert "ExecStart=/home/lighthouse/rquant/.venv/bin/rquant morning-pulse" in content
    assert "--force" not in content
    assert "--slot" not in content
    assert "Restart=no" in content
    assert "TimeoutStartSec=180" in content


def test_morning_pulse_timer_runs_each_slot_without_stale_catchup() -> None:
    content = _read("rquant-morning-pulse.timer")

    for hhmm in ("10:00:00", "10:30:00", "11:00:00", "11:30:00"):
        assert f"OnCalendar=Mon..Fri *-*-* {hhmm}" in content
    assert "Unit=rquant-morning-pulse.service" in content
    assert "Persistent=true" not in content
    assert "WantedBy=timers.target" in content


def test_midday_report_service_uses_cloud_readonly_inputs() -> None:
    content = _read("rquant-midday-report.service")

    assert "After=network-online.target rquant-panorama.service" in content
    assert "OnFailure=rquant-alert@%n.service" in content
    assert "User=lighthouse" in content
    assert "Group=lighthouse" in content
    assert "WorkingDirectory=/home/lighthouse/rquant" in content
    assert "EnvironmentFile=/home/lighthouse/rquant/.env" in content
    assert "Environment=TZ=Asia/Shanghai" in content
    assert "Environment=RQUANT_PANORAMA_SOCKS=" in content
    assert "ExecStart=/home/lighthouse/rquant/.venv/bin/rquant midday-report" in content
    assert "--force" not in content
    assert "--date" not in content
    assert "Restart=no" in content
    assert "TimeoutStartSec=180" in content


def test_midday_report_timer_runs_once_without_stale_catchup() -> None:
    content = _read("rquant-midday-report.timer")

    assert "OnCalendar=Mon..Fri *-*-* 12:00:00" in content
    assert "Unit=rquant-midday-report.service" in content
    assert "Persistent=true" not in content
    assert "WantedBy=timers.target" in content
