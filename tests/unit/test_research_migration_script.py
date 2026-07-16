"""Safety contract for the resumable research-cloud migration script."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/migrate-research-to-cloud.sh"
SNAPSHOT_ID = "research-20260716T160000Z-a1b2c3d4"


def test_script_uses_staging_resumable_rsync_and_verify_before_publish() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "--archive" in content
    assert "--partial" in content
    assert "--checksum" in content
    assert "research-staging" in content
    assert "research-migration verify" in content
    assert "research-migration publish" in content
    assert content.index("research-migration verify") < content.index(
        "research-migration publish"
    )
    assert "--apply" in content
    assert "SPACE_MULTIPLIER=2" in content
    assert "SPACE_RESERVE_BYTES=1073741824" in content
    assert "git -C \"${PROJECT_DIR}\" diff --quiet" in content
    assert "09:15-15:10" in content
    assert "local monitor is running" in content
    assert "strategy lab worker is running" in content
    assert "Refusing migration publish outside the post-close window" in content
    assert "rquant-monitor.service is active" in content
    assert "Insufficient remote publish space" in content
    assert "timeout --signal=TERM" in content


def test_all_phase_dry_run_prints_four_phases_without_writing(tmp_path: Path) -> None:
    source = tmp_path / "rquant.duckdb"
    source.write_bytes(b"frozen-source")
    recovery = tmp_path / "recovery"
    bundles = tmp_path / "bundles"
    artifacts = tmp_path / "strategy_lab_runs"
    artifacts.mkdir()
    env = {
        **os.environ,
        "RQUANT_MIGRATION_PYTHON": sys.executable,
        "RQUANT_MIGRATION_SOURCE_DB": str(source),
        "RQUANT_MIGRATION_RECOVERY_DIR": str(recovery),
        "RQUANT_MIGRATION_BUNDLE_DIR": str(bundles),
        "RQUANT_MIGRATION_ARTIFACT_DIR": str(artifacts),
        "RQUANT_MIGRATION_REMOTE": "test@example",
        "RQUANT_MIGRATION_REMOTE_REPO": "/srv/rquant",
    }

    result = subprocess.run(
        [
            "bash",
            str(SCRIPT),
            "--phase",
            "all",
            "--snapshot-id",
            SNAPSHOT_ID,
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-07-16",
            "--dry-run",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "research-migration snapshot" in result.stdout
    assert "research-migration prepare" in result.stdout
    assert "research-migration verify" in result.stdout
    assert "rsync" in result.stdout
    assert "test@example:/srv/rquant/data/research-staging/" in result.stdout
    assert "research-migration publish" in result.stdout
    assert "Refusing migration publish outside the post-close window" in result.stdout
    assert "rquant-monitor.service is active" in result.stdout
    assert "Insufficient remote publish space" in result.stdout
    assert "timeout --signal=TERM" in result.stdout
    assert not recovery.exists()
    assert not bundles.exists()


def test_prepare_refuses_to_snapshot_while_local_monitor_is_running(
    tmp_path: Path,
) -> None:
    source = tmp_path / "rquant.duckdb"
    source.write_bytes(b"not-opened-because-guard-fails")
    fake_pgrep = tmp_path / "pgrep"
    fake_pgrep.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_pgrep.chmod(0o755)
    env = {
        **os.environ,
        "RQUANT_MIGRATION_PYTHON": sys.executable,
        "RQUANT_MIGRATION_SOURCE_DB": str(source),
        "RQUANT_MIGRATION_PGREP_BIN": str(fake_pgrep),
    }

    result = subprocess.run(
        [
            "bash",
            str(SCRIPT),
            "--phase",
            "prepare",
            "--snapshot-id",
            SNAPSHOT_ID,
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-07-16",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "local monitor is running" in result.stderr
