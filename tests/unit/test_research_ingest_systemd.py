"""Production systemd contract for the cloud research daily ingest."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"


def test_research_ingest_service_uses_daily_default_and_failure_relay() -> None:
    content = (SYSTEMD / "rquant-research-ingest.service").read_text(encoding="utf-8")

    assert "Requires=rquant-replica-sync.service" in content
    assert "After=network-online.target rquant-daily.service rquant-replica-sync.service" in content
    assert "OnFailure=rquant-alert@%n.service" in content
    assert "StartLimitIntervalSec" not in content
    assert "StartLimitBurst" not in content
    assert "User=lighthouse" in content
    assert "EnvironmentFile=/home/lighthouse/rquant/.env" in content
    assert "Environment=TZ=Asia/Shanghai" in content
    assert (
        "ExecStart=/usr/local/libexec/rquant-workload-arbiter research -- "
        "/home/lighthouse/rquant/scripts/run-research-ingest-daily.sh" in content
    )
    assert "--date" not in content
    assert "Restart=no" in content
    assert "RestartSec" not in content
    assert "RestartPreventExitStatus" not in content
    assert "TimeoutStartSec=7200" in content
    assert "/bin/sh" not in content


def test_research_ingest_requires_replica_oneshot_success_in_same_transaction() -> None:
    ingest = (SYSTEMD / "rquant-research-ingest.service").read_text(encoding="utf-8")
    replica = (SYSTEMD / "rquant-replica-sync.service").read_text(encoding="utf-8")

    assert "Requires=rquant-replica-sync.service" in ingest
    assert "After=" in ingest and "rquant-replica-sync.service" in ingest
    assert "Slice=rquant-maintenance.slice" in replica
    assert "rquant-workload-arbiter maintenance --" in replica
    assert "sync-readonly-replica.sh" not in ingest


def test_research_ingest_timer_runs_after_daily_without_cross_day_catchup() -> None:
    content = (SYSTEMD / "rquant-research-ingest.timer").read_text(encoding="utf-8")

    assert "OnCalendar=Mon..Fri *-*-* 18:10:00" in content
    assert "Unit=rquant-research-ingest.service" in content
    assert "Persistent=true" not in content
    assert "WantedBy=timers.target" in content


def test_legacy_infrastructure_deployer_never_auto_enables_research_timer() -> None:
    content = (ROOT / "scripts" / "deploy.sh").read_text(encoding="utf-8")

    assert (
        'if [[ "${t}" == "rquant-research-ingest.timer" ]] '
        '&& ! systemctl is-enabled "${t}"' in content
    )
    assert "研究日增量须按独立上线手册手工启用" in content


def test_legacy_infrastructure_deployer_never_auto_enables_daily_orchestrator_timer() -> None:
    content = (ROOT / "scripts" / "deploy.sh").read_text(encoding="utf-8")

    assert "--accept-daily-orchestrator-timer" in content
    assert "RQUANT_DAILY_ORCHESTRATOR_INSTANCE" in content
    assert "rquant-runtime-daily-orchestrator@.timer" in content
    assert "daily orchestrator timer remains disabled by default" in content
    assert "sudo systemctl enable --now" not in content
