"""Per-strategy Stage 1 acceptance planning contracts."""

from __future__ import annotations

from argparse import Namespace
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from rquant import cli
from rquant.backfill_manifest import (
    STRATEGY_BACKFILL_SPECS,
    BackfillCoverage,
    BackfillEstimate,
    BackfillManifest,
    BackfillPhaseCoverage,
    EligibilityRecord,
    EligibilityResolution,
    MergedBackfillWindow,
    MinuteBackfillPlan,
    MinuteBackfillTask,
    backfill_state_input,
)
from rquant.backfill_state import BackfillStateStore

SHANGHAI = ZoneInfo("Asia/Shanghai")
COMMIT = "a" * 40


def _plan(
    strategy: str,
    *,
    with_task: bool = False,
    with_window: bool = False,
) -> MinuteBackfillPlan:
    eligibility = EligibilityRecord(
        strategy_id=strategy,
        strategy_version="v1",
        ts_code="300001.SZ",
        eligibility_date=date(2026, 6, 26),
        entry_date=date(2026, 6, 26),
        decision_at=datetime(2026, 6, 26, 9, 32, tzinfo=SHANGHAI),
        variant=strategy,
    )
    resolution = EligibilityResolution(
        strategy_id=strategy,
        strategy_version="v1",
        requested_dates=(eligibility.eligibility_date,),
        evaluated_dates=(eligibility.eligibility_date,),
        complete_dates=(eligibility.eligibility_date,),
        records=(eligibility,),
    )
    manifest = BackfillManifest.build(
        spec=STRATEGY_BACKFILL_SPECS[strategy],
        start_date=date(2026, 6, 26),
        end_date=date(2026, 6, 26),
        as_of_time=datetime(2026, 6, 27, 2, tzinfo=UTC),
        code_commit=COMMIT,
        eligibilities=(eligibility,),
        eligibility_resolution=resolution,
    )
    tasks = ()
    if with_task:
        tasks = (
            MinuteBackfillTask(
                task_id="d" * 64,
                ts_code="300001.SZ",
                source="tushare",
                freq="1min",
                start_date=date(2026, 6, 26),
                end_date=date(2026, 6, 26),
                open_dates=(date(2026, 6, 26),),
                expected_rows=241,
                response_row_limit=8_000,
                possible_truncation=False,
            ),
        )
    windows = ()
    if with_window:
        windows = (
            MergedBackfillWindow(
                ts_code="300001.SZ",
                start_date=date(2026, 6, 25),
                end_date=date(2026, 6, 26),
                open_dates=(date(2026, 6, 25), date(2026, 6, 26)),
            ),
        )
    return MinuteBackfillPlan(
        manifest=manifest,
        windows=windows,
        tasks=tasks,
        coverage=BackfillCoverage(
            baseline=BackfillPhaseCoverage(
                expected_sessions=90,
                complete_sessions=90,
            ),
            entry=BackfillPhaseCoverage(
                expected_sessions=1,
                complete_sessions=1,
            ),
            exit=BackfillPhaseCoverage(
                expected_sessions=10,
                complete_sessions=10,
            ),
            expected_unique_sessions=101,
            complete_unique_sessions=101,
        ),
        requested_session_count=len(tasks),
        estimate=BackfillEstimate(
            request_count=len(tasks),
            estimated_rows=241 if tasks else 0,
            estimated_disk_bytes=24_100 if tasks else 0,
            rate_limit_seconds=1.0 if tasks else 0.0,
            transfer_seconds=2.0 if tasks else 0.0,
            write_seconds=3.0 if tasks else 0.0,
            total_seconds=6.0 if tasks else 0.0,
            confidence="high",
            confidence_reasons=("test estimate",),
        ),
    )


def _spec(strategy: str, manifest_id: str):
    from rquant.stage1_acceptance import Stage1AcceptanceSpec

    return Stage1AcceptanceSpec(
        strategy=strategy,
        manifest_id=manifest_id,
        start_date=date(2026, 6, 26),
        end_date=date(2026, 6, 26),
        expected_code_commit=COMMIT,
    )


def test_completed_strategy_is_ready_without_reading_other_manifests(
    tmp_path: Path,
) -> None:
    from rquant.stage1_acceptance import build_stage1_acceptance_plan

    plan = _plan("n_shape")
    store = BackfillStateStore(tmp_path / "state.sqlite3")
    store.persist_manifest(backfill_state_input(plan))
    load_manifest = MagicMock(wraps=store.load_manifest)
    get_status = MagicMock(wraps=store.get_manifest_status)
    store.load_manifest = load_manifest
    store.get_manifest_status = get_status

    acceptance = build_stage1_acceptance_plan(
        store,
        _spec("n_shape", plan.manifest.manifest_id),
        observed_code_commit=COMMIT,
        now=datetime(2026, 7, 22, 8, 0, tzinfo=SHANGHAI),
    )

    assert acceptance.disposition == "ready"
    assert acceptance.apply_required is True
    assert acceptance.manifest_status.status == "completed"
    assert acceptance.budget.formal_replay_sample_limit == 1
    assert acceptance.budget.next_protected_window_start == datetime(
        2026, 7, 22, 9, 15, tzinfo=SHANGHAI
    )
    load_manifest.assert_called_once_with(plan.manifest.manifest_id)
    get_status.assert_called_once_with(plan.manifest.manifest_id)


def test_acceptance_budget_counts_existing_window_rows_not_missing_downloads(
    tmp_path: Path,
) -> None:
    from rquant.stage1_acceptance import build_stage1_acceptance_plan

    plan = _plan("n_shape", with_window=True)
    assert plan.estimate.estimated_rows == 0
    store = BackfillStateStore(tmp_path / "state.sqlite3")
    store.persist_manifest(backfill_state_input(plan))

    acceptance = build_stage1_acceptance_plan(
        store,
        _spec("n_shape", plan.manifest.manifest_id),
        observed_code_commit=COMMIT,
        now=datetime(2026, 7, 22, 8, 0, tzinfo=SHANGHAI),
    )

    assert acceptance.budget.estimated_snapshot_scan_rows == 482


def test_abandoned_auction_strategy_is_retired_not_blocking(
    tmp_path: Path,
) -> None:
    from rquant.stage1_acceptance import build_stage1_acceptance_plan

    plan = _plan("auction_gap", with_task=True)
    store = BackfillStateStore(tmp_path / "state.sqlite3")
    store.persist_manifest(backfill_state_input(plan))
    abandonment = store.plan_manifest_abandonment(
        plan.manifest.manifest_id,
        reason="independent auction entry was retired",
        code_commit=COMMIT,
    )
    store.apply_manifest_abandonment(abandonment)

    acceptance = build_stage1_acceptance_plan(
        store,
        _spec("auction_gap", plan.manifest.manifest_id),
        observed_code_commit=COMMIT,
        now=datetime(2026, 7, 22, 16, 0, tzinfo=SHANGHAI),
    )

    assert acceptance.disposition == "retired"
    assert acceptance.apply_required is False
    assert acceptance.blockers == ()
    assert acceptance.manifest_status.pending == 1


def test_retired_auction_accepts_historical_manifest_commit(
    tmp_path: Path,
) -> None:
    from rquant.stage1_acceptance import (
        Stage1AcceptanceSpec,
        build_stage1_acceptance_plan,
    )

    plan = _plan("auction_gap", with_task=True)
    store = BackfillStateStore(tmp_path / "state.sqlite3")
    store.persist_manifest(backfill_state_input(plan))
    abandonment = store.plan_manifest_abandonment(
        plan.manifest.manifest_id,
        reason="independent auction entry was retired",
        code_commit="b" * 40,
    )
    store.apply_manifest_abandonment(abandonment)
    spec = Stage1AcceptanceSpec(
        strategy="auction_gap",
        manifest_id=plan.manifest.manifest_id,
        start_date=date(2026, 6, 26),
        end_date=date(2026, 6, 26),
        expected_code_commit="b" * 40,
    )

    acceptance = build_stage1_acceptance_plan(
        store,
        spec,
        observed_code_commit="b" * 40,
        now=datetime(2026, 7, 22, 16, 0, tzinfo=SHANGHAI),
    )

    assert plan.manifest.code_commit == COMMIT
    assert acceptance.disposition == "retired"


def test_incomplete_selected_strategy_is_blocked_with_its_own_status(
    tmp_path: Path,
) -> None:
    from rquant.stage1_acceptance import build_stage1_acceptance_plan

    plan = _plan("growth_board_surge", with_task=True)
    store = BackfillStateStore(tmp_path / "state.sqlite3")
    store.persist_manifest(backfill_state_input(plan))

    acceptance = build_stage1_acceptance_plan(
        store,
        _spec("growth_board_surge", plan.manifest.manifest_id),
        observed_code_commit=COMMIT,
        now=datetime(2026, 7, 22, 16, 0, tzinfo=SHANGHAI),
    )

    assert acceptance.disposition == "blocked"
    assert acceptance.apply_required is False
    assert acceptance.blockers == ("manifest_not_completed:pending",)


@pytest.mark.parametrize(
    ("strategy", "commit", "message"),
    [
        ("growth_board_surge", COMMIT, "strategy"),
        ("n_shape", "b" * 40, "commit"),
    ],
)
def test_acceptance_rejects_manifest_identity_mismatch(
    tmp_path: Path,
    strategy: str,
    commit: str,
    message: str,
) -> None:
    from rquant.stage1_acceptance import (
        Stage1AcceptanceIdentityError,
        Stage1AcceptanceSpec,
        build_stage1_acceptance_plan,
    )

    plan = _plan("n_shape")
    store = BackfillStateStore(tmp_path / "state.sqlite3")
    store.persist_manifest(backfill_state_input(plan))
    spec = Stage1AcceptanceSpec(
        strategy=strategy,
        manifest_id=plan.manifest.manifest_id,
        start_date=date(2026, 6, 26),
        end_date=date(2026, 6, 26),
        expected_code_commit=commit,
    )

    with pytest.raises(Stage1AcceptanceIdentityError, match=message):
        build_stage1_acceptance_plan(
            store,
            spec,
            observed_code_commit=commit,
            now=datetime(2026, 7, 22, 16, 0, tzinfo=SHANGHAI),
        )


def test_stage1_acceptance_cli_parser_and_ready_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import json

    plan = _plan("n_shape")
    store = BackfillStateStore(tmp_path / "state.sqlite3")
    store.persist_manifest(backfill_state_input(plan))
    state_factory = MagicMock(side_effect=AssertionError("live state opened"))
    monkeypatch.setattr(cli, "BackfillStateStore", state_factory)
    snapshot_context = MagicMock()
    snapshot_context.__enter__.return_value = store
    snapshot_context.__exit__.return_value = False
    snapshot_factory = MagicMock(return_value=snapshot_context)
    monkeypatch.setattr(
        cli,
        "open_backfill_state_snapshot",
        snapshot_factory,
        raising=False,
    )
    monkeypatch.setattr(
        "rquant.research_manifest.detect_verified_code_commit",
        lambda: COMMIT,
    )
    args = cli.build_parser().parse_args(
        [
            "stage1-acceptance",
            "--strategy",
            "n_shape",
            "--manifest-id",
            plan.manifest.manifest_id,
            "--start-date",
            "2026-06-26",
            "--end-date",
            "2026-06-26",
            "--expected-code-commit",
            COMMIT,
        ]
    )

    rc = cli.cmd_stage1_acceptance(args)
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["disposition"] == "ready"
    assert payload["spec"]["strategy"] == "n_shape"
    snapshot_factory.assert_called_once_with()
    state_factory.assert_not_called()


def test_stage1_acceptance_cli_returns_nonzero_when_selected_strategy_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan("growth_board_surge", with_task=True)
    store = BackfillStateStore(tmp_path / "state.sqlite3")
    store.persist_manifest(backfill_state_input(plan))
    snapshot_context = MagicMock()
    snapshot_context.__enter__.return_value = store
    snapshot_context.__exit__.return_value = False
    monkeypatch.setattr(
        cli,
        "open_backfill_state_snapshot",
        MagicMock(return_value=snapshot_context),
    )
    monkeypatch.setattr(
        "rquant.research_manifest.detect_verified_code_commit",
        lambda: COMMIT,
    )

    rc = cli.cmd_stage1_acceptance(
        Namespace(
            strategy="growth_board_surge",
            manifest_id=plan.manifest.manifest_id,
            start_date=date(2026, 6, 26),
            end_date=date(2026, 6, 26),
            expected_code_commit=COMMIT,
        )
    )

    assert rc == 1
