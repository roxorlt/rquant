"""Safety contract for the v0.25.2 Stage 1 production rollout."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "rollout-v0.25.2-stage1.sh"


def test_rollout_script_is_valid_strict_bash() -> None:
    result = subprocess.run(
        ["bash", "-n", str(SCRIPT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_rollout_script_guards_exact_release_and_market_window() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "set -Eeuo pipefail" in content
    assert 'TARGET_TAG="v0.25.2"' in content
    assert "RQUANT_STAGE1_EXPECTED_SHA" in content
    assert 'git describe --tags --exact-match' in content
    assert 'git rev-parse HEAD' in content
    assert "hour_minute >= 915" in content
    assert "hour_minute <= 1510" in content
    assert "09:15-15:10" in content


def test_rollout_script_restores_original_timers_and_backs_up_quiescent_main() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "ORIGINALLY_ACTIVE_TIMERS" in content
    assert 'systemctl is-active --quiet "${timer}"' in content
    assert 'restore_original_timers' in content
    assert "trap on_exit EXIT" in content
    assert "rquant-research-ingest.timer" in content
    assert "rquant-research-ingest.service" in content
    assert "refusing rollout while mutating service is active" in content
    stop_index = content.index("stop_mutating_units")
    backup_index = content.index("RQUANT_BACKUP_SOURCE=main", stop_index)
    assert stop_index < backup_index
    assert "verify_backup" in content
    assert "preserve_backup" in content
    assert 'if ! sudo systemctl start "${ORIGINALLY_ACTIVE_TIMERS[@]}"; then' in content
    assert "timeout --signal=TERM --kill-after=30s" in content
    assert "ROLLOUT_HARD_DEADLINE_EPOCH" in content
    assert 'run_guarded sudo systemctl stop "${MUTATING_TIMERS[@]}"' in content
    assert "run_guarded cp --" in content
    assert '"${PROJECT_DIR}/backup/latest.duckdb.gz"' in content
    assert 'run_guarded "${PYTHON_BIN}" - "${replay_file}"' in content
    restored_index = content.index("TIMERS_RESTORED=1")
    verify_index = content.index(
        'systemctl is-active --quiet "${timer}"',
        content.index("restore_original_timers()"),
    )
    assert verify_index < restored_index


def test_rollout_script_previews_then_refreshes_suspensions_atomically() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert 'REFRESH_END="$(' in content
    preview = (
        'suspension-backfill --start-date "${START_DATE}" '
        '--end-date "${REFRESH_END}" --full-refresh --dry-run'
    )
    apply = (
        'suspension-backfill --start-date "${START_DATE}" '
        '--end-date "${REFRESH_END}" --full-refresh'
    )
    assert preview in content
    assert apply in content
    preview_index = content.index(preview)
    apply_index = content.index(apply, preview_index + len(preview))
    assert preview_index < apply_index
    assert "requested_dates" in content
    assert "persisted_date_count" in content


def test_rollout_script_runs_complete_three_strategy_evidence_chain() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert 'STRATEGIES=("n_shape" "growth_board_surge" "auction_gap")' in content
    for command in (
        "backfill-plan",
        "backfill-run",
        "backfill-status",
        "research-repair-minute",
        "dataset-snapshot",
        "data-audit",
        "formal-smoke-replay",
        "research-authority-status",
        "preflight",
    ):
        assert command in content
    for required_value in (
        "planned",
        "unchanged",
        "completed",
        "ready",
        "comparable",
    ):
        assert required_value in content
    assert "p0_count" in content
    assert 'require_json_value "${AUTHORITY_FILE}" status candidate' in content
    assert 'payload["sample_count"] > 0' in content
    assert "sync-readonly-replica.sh" in content


def test_rollout_script_resumes_large_manifest_or_pauses_cleanly() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "RQUANT_STAGE1_RESUME_MANIFEST_ID" in content
    assert "RQUANT_STAGE1_BACKFILL_WORKERS" in content
    assert "RQUANT_STAGE1_BACKFILL_MAX_RUNTIME_MINUTES" in content
    assert '--workers "${BACKFILL_WORKERS}"' in content
    assert '--max-runtime-minutes "${BACKFILL_MAX_RUNTIME_MINUTES}"' in content
    assert "ROLLOUT_RESULT=paused" in content
    pause_index = content.index("ROLLOUT_RESULT=paused")
    assert content.rfind("restore_original_timers", 0, pause_index) >= 0
    assert content.rfind("backfill-status", 0, pause_index) >= 0
    assert "pause_before_full_stage_window" in content
    assert "assert_resume_start_window" in content
    completed_index = content.index("RESUME_BACKFILL_COMPLETED")
    full_stage_index = content.index("REFRESH_END=")
    pause_guard_index = content.index(
        "pause_before_full_stage_window",
        completed_index,
    )
    assert completed_index < pause_guard_index < full_stage_index
    assert content.count("pause_before_full_stage_window") >= 6
