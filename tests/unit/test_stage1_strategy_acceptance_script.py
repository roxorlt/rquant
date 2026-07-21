"""Safety contract for per-strategy Stage 1 acceptance orchestration."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run-stage1-strategy-acceptance.sh"


def test_strategy_acceptance_script_is_valid_strict_bash() -> None:
    result = subprocess.run(
        ["bash", "-n", str(SCRIPT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_strategy_acceptance_script_is_single_strategy_and_fail_closed() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "set -Eeuo pipefail" in content
    assert "stage1-acceptance" in content
    assert '"${STRATEGY}"' in content
    assert "STRATEGIES=(" not in content
    assert "trap on_exit EXIT" in content
    assert "restore_original_timers" in content
    assert "09:15-15:10" in content
    assert "git rev-parse HEAD" in content
    assert "git status --porcelain" in content


def test_strategy_acceptance_script_runs_preview_before_each_apply() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    repair_preview = content.index("research-repair-minute")
    repair_apply = content.index("research-repair-minute", repair_preview + 1)
    snapshot_preview = content.index("dataset-snapshot", repair_apply + 1)
    snapshot_apply = content.index("dataset-snapshot", snapshot_preview + 1)

    assert repair_preview < repair_apply < snapshot_preview < snapshot_apply
    assert "--plan-id" in content[repair_apply:snapshot_preview]
    assert "data-audit" in content
    assert "formal-smoke-replay" in content
    assert "sync-readonly-replica.sh" in content
    assert "preflight" in content


def test_strategy_acceptance_script_reuses_snapshot_as_of_and_saves_evidence() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "snapshot-as-of.txt" in content
    assert "SNAPSHOT_AS_OF" in content
    assert "acceptance-plan.json" in content
    assert "formal-smoke.json" in content
    assert "ROLLOUT_RESULT=retired" in content
    assert "ROLLOUT_RESULT=success" in content


def test_strategy_acceptance_script_bounds_mutations_before_next_market_window() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "ROLLOUT_HARD_DEADLINE_EPOCH" in content
    assert "next weekday 09:10" in content
    assert "timeout --signal=TERM --kill-after=30s" in content
    assert 'run_guarded "${RQUANT_BIN}" research-repair-minute' in content
    assert 'run_guarded "${RQUANT_BIN}" dataset-snapshot' in content
    assert 'run_guarded "${RQUANT_BIN}" data-audit' in content
    assert 'run_guarded "${RQUANT_BIN}" formal-smoke-replay' in content
