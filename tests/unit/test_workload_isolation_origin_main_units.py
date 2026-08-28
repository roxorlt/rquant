"""Temporary origin/main integration contracts for intraday briefing units."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
ORIGIN_MAIN_HEAD = "9699827be09ca22479f6741e820722399fe40244"

MORNING_SERVICE = """[Unit]
Description=rQuant Intraday Market Pulse
After=network-online.target rquant-panorama.service rquant-replica-sync.service
Wants=network-online.target
OnFailure=rquant-alert@%n.service

[Service]
Type=oneshot
User=lighthouse
Group=lighthouse
WorkingDirectory=/home/lighthouse/rquant
EnvironmentFile=/home/lighthouse/rquant/.env
Environment=TZ=Asia/Shanghai
Environment=RQUANT_PANORAMA_SOCKS=
ExecStart=/home/lighthouse/rquant/.venv/bin/rquant morning-pulse
Restart=no
TimeoutStartSec=180
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""
MORNING_TIMER = """[Unit]
Description=Trigger rQuant intraday market pulse on each morning slot

[Timer]
OnCalendar=Mon..Fri *-*-* 10:00:00
OnCalendar=Mon..Fri *-*-* 10:30:00
OnCalendar=Mon..Fri *-*-* 11:00:00
OnCalendar=Mon..Fri *-*-* 11:30:00
AccuracySec=1s
Unit=rquant-morning-pulse.service

[Install]
WantedBy=timers.target
"""
MIDDAY_SERVICE = """[Unit]
Description=rQuant Midday Market Report
After=network-online.target rquant-panorama.service rquant-replica-sync.service
Wants=network-online.target
OnFailure=rquant-alert@%n.service

[Service]
Type=oneshot
User=lighthouse
Group=lighthouse
WorkingDirectory=/home/lighthouse/rquant
EnvironmentFile=/home/lighthouse/rquant/.env
Environment=TZ=Asia/Shanghai
Environment=RQUANT_PANORAMA_SOCKS=
ExecStart=/home/lighthouse/rquant/.venv/bin/rquant midday-report
Restart=no
TimeoutStartSec=180
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""
MIDDAY_TIMER = """[Unit]
Description=Trigger rQuant midday market report

[Timer]
OnCalendar=Mon..Fri *-*-* 12:00:00
AccuracySec=1s
Unit=rquant-midday-report.service

[Install]
WantedBy=timers.target
"""


def _without_workload_slice(content: str) -> str:
    return content.replace("Slice=rquant-live.slice\n", "")


def test_origin_main_services_keep_exact_behavior_and_only_add_live_slice() -> None:
    from rquant.workload_isolation import WORKLOAD_UNIT_SLICES

    morning = (SYSTEMD / "rquant-morning-pulse.service").read_text(encoding="utf-8")
    midday = (SYSTEMD / "rquant-midday-report.service").read_text(encoding="utf-8")

    assert _without_workload_slice(morning) == MORNING_SERVICE
    assert _without_workload_slice(midday) == MIDDAY_SERVICE
    assert morning.count("Slice=rquant-live.slice") == 1
    assert midday.count("Slice=rquant-live.slice") == 1
    assert WORKLOAD_UNIT_SLICES["rquant-morning-pulse.service"] == "rquant-live.slice"
    assert WORKLOAD_UNIT_SLICES["rquant-midday-report.service"] == "rquant-live.slice"


def test_origin_main_timer_calendars_are_byte_exact() -> None:
    assert (SYSTEMD / "rquant-morning-pulse.timer").read_text(encoding="utf-8") == MORNING_TIMER
    assert (SYSTEMD / "rquant-midday-report.timer").read_text(encoding="utf-8") == MIDDAY_TIMER


def test_temporary_integration_records_origin_and_future_three_way_audit() -> None:
    content = (SYSTEMD / "README.md").read_text(encoding="utf-8")

    assert ORIGIN_MAIN_HEAD in content
    assert "5bb641ab23efa9595100070ff77282e18c14d170" in content
    assert "three-way" in content
    assert "临时整合" in content
