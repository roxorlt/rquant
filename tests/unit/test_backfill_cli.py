"""Manifest backfill CLI contracts."""

from __future__ import annotations

from argparse import Namespace
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
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
    MergedBackfillWindow,
    MinuteBackfillPlan,
    backfill_state_input,
)
from rquant.backfill_state import BackfillStateStore

SHANGHAI = ZoneInfo("Asia/Shanghai")
_COMMIT = "a" * 40


def _plan(*, baseline_complete: int = 90, entry_complete: int = 1) -> MinuteBackfillPlan:
    eligibility = EligibilityRecord(
        strategy_id="growth_board_surge",
        strategy_version="v1",
        ts_code="300001.SZ",
        eligibility_date=date(2026, 6, 26),
        entry_date=date(2026, 6, 26),
        decision_at=datetime(2026, 6, 26, 9, 32, tzinfo=SHANGHAI),
        variant="growth",
    )
    manifest = BackfillManifest.build(
        spec=STRATEGY_BACKFILL_SPECS["growth_board_surge"],
        start_date=date(2026, 6, 26),
        end_date=date(2026, 6, 26),
        as_of_time=datetime(2026, 6, 26, 2, tzinfo=UTC),
        code_commit=_COMMIT,
        eligibilities=(eligibility,),
    )
    coverage = BackfillCoverage(
        baseline=BackfillPhaseCoverage(
            expected_sessions=90,
            complete_sessions=baseline_complete,
        ),
        entry=BackfillPhaseCoverage(
            expected_sessions=1,
            complete_sessions=entry_complete,
        ),
        exit=BackfillPhaseCoverage(expected_sessions=10, complete_sessions=10),
        expected_unique_sessions=101,
        complete_unique_sessions=min(101, baseline_complete + entry_complete + 10),
    )
    return MinuteBackfillPlan(
        manifest=manifest,
        windows=(),
        tasks=(),
        coverage=coverage,
        requested_session_count=0,
        estimate=BackfillEstimate(
            request_count=0,
            estimated_rows=0,
            estimated_disk_bytes=0,
            rate_limit_seconds=0,
            transfer_seconds=0,
            write_seconds=0,
            total_seconds=0,
            confidence="high",
            confidence_reasons=("no download required",),
        ),
    )


def _plan_with_window() -> MinuteBackfillPlan:
    plan = _plan()
    window = MergedBackfillWindow(
        ts_code="300001.SZ",
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 30),
        open_dates=(date(2026, 6, 1), date(2026, 6, 30)),
    )
    return plan.model_copy(update={"windows": (window,)})


def test_backfill_cli_parser_contracts() -> None:
    parser = cli.build_parser()

    plan = parser.parse_args(
        [
            "backfill-plan",
            "--strategy",
            "auction_gap",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-06-30",
        ]
    )
    run = parser.parse_args(["backfill-run", "--manifest-id", "b" * 64])
    status = parser.parse_args(
        ["backfill-status", "--manifest-id", "c" * 64, "--json"]
    )
    snapshot = parser.parse_args(
        [
            "dataset-snapshot",
            "--strategy",
            "auction_gap",
            "--as-of",
            "2026-06-30T15:00:00+08:00",
            "--manifest-id",
            "d" * 64,
        ]
    )

    assert plan.start_date == date(2026, 1, 1)
    assert run.retry_failed is False
    assert status.json is True
    assert snapshot.as_of == datetime(2026, 6, 30, 7, tzinfo=UTC)


@pytest.mark.parametrize(
    ("now", "estimated_seconds", "expected"),
    [
        (datetime(2026, 7, 15, 10, tzinfo=SHANGHAI), 60, False),
        (datetime(2026, 7, 15, 20, tzinfo=SHANGHAI), 3_600, True),
        (datetime(2026, 7, 15, 20, tzinfo=SHANGHAI), 50_000, False),
        (datetime(2026, 7, 18, 10, tzinfo=SHANGHAI), 3_600, True),
    ],
)
def test_backfill_write_window_guard(
    now: datetime,
    estimated_seconds: float,
    expected: bool,
) -> None:
    assert cli._backfill_write_window_safe(now, estimated_seconds) is expected


def test_backfill_status_unknown_manifest_is_nonzero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))

    rc = cli.cmd_backfill_status(
        Namespace(manifest_id="f" * 64, json=True)
    )

    assert rc != 0


def test_backfill_status_retryable_failure_is_nonzero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.backfill_state import (
        BackfillFailure,
        BackfillManifestInput,
        BackfillTaskInput,
    )

    state = BackfillStateStore(tmp_path / "state.sqlite3")
    state.persist_manifest(
        BackfillManifestInput(
            manifest_id="f" * 64,
            payload={"strategy": "test"},
            tasks=(
                BackfillTaskInput(
                    task_id="task-1",
                    payload={"ts_code": "300001.SZ"},
                    max_attempts=3,
                ),
            ),
            eligibility=(),
        )
    )
    claim = state.claim_task(
        "f" * 64,
        worker_id="test-worker",
        lease_seconds=60,
    )
    assert claim is not None
    state.mark_task_failed(
        claim,
        failure=BackfillFailure(
            code="source_timeout",
            message="retry later",
            retryable=True,
        ),
    )
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))

    rc = cli.cmd_backfill_status(Namespace(manifest_id="f" * 64, json=True))

    assert rc == 1


def test_backfill_plan_persists_immutable_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    planned = _plan()
    readonly_store = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = readonly_store
    context.__exit__.return_value = False
    monkeypatch.setattr(cli, "open_readonly_store", MagicMock(return_value=context))
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))
    monkeypatch.setattr(
        "rquant.backfill_manifest.resolve_strategy_eligibility",
        MagicMock(return_value=planned.manifest.eligibilities),
    )
    monkeypatch.setattr(
        "rquant.backfill_manifest.plan_minute_backfill",
        MagicMock(return_value=planned),
    )
    monkeypatch.setattr(
        "rquant.research_manifest.detect_code_commit",
        MagicMock(return_value=_COMMIT),
    )

    rc = cli.cmd_backfill_plan(
        Namespace(
            strategy="growth_board_surge",
            start_date=date(2026, 6, 26),
            end_date=date(2026, 6, 26),
        )
    )

    assert rc == 0
    assert state.load_manifest(planned.manifest.manifest_id) == backfill_state_input(
        planned
    )
    assert planned.manifest.manifest_id in capsys.readouterr().out


def test_dataset_snapshot_recomputes_and_rejects_failed_coverage_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = _plan()
    current = _plan(baseline_complete=80)
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    state.persist_manifest(backfill_state_input(initial))
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))
    writable_store = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = writable_store
    context.__exit__.return_value = False
    monkeypatch.setattr(cli, "DuckDBStore", MagicMock(return_value=context))
    planner = MagicMock(return_value=current)
    monkeypatch.setattr("rquant.backfill_manifest.plan_minute_backfill", planner)

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="growth_board_surge",
            as_of=datetime(2026, 6, 30, 7, tzinfo=UTC),
            manifest_id=initial.manifest.manifest_id,
        )
    )

    assert rc != 0
    planner.assert_called_once()
    writable_store.begin_dataset_snapshot.assert_not_called()


def test_dataset_snapshot_records_metadata_without_refreshing_tables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    plan = _plan()
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    state.persist_manifest(backfill_state_input(plan))
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))
    writable_store = MagicMock()
    writable_store._conn.execute.return_value.fetchone.return_value = (
        datetime(2026, 6, 30, 15),
    )
    context = MagicMock()
    context.__enter__.return_value = writable_store
    context.__exit__.return_value = False
    monkeypatch.setattr(cli, "DuckDBStore", MagicMock(return_value=context))
    monkeypatch.setattr(
        "rquant.backfill_manifest.plan_minute_backfill",
        MagicMock(return_value=plan),
    )
    begun = SimpleNamespace(snapshot_id="snapshot-id", status="building")
    writable_store.begin_dataset_snapshot.return_value = begun
    writable_store.finalize_dataset_snapshot.return_value = SimpleNamespace(
        snapshot_id="snapshot-id",
        status="ready",
    )

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="growth_board_surge",
            as_of=datetime(2026, 6, 30, 7, tzinfo=UTC),
            manifest_id=plan.manifest.manifest_id,
        )
    )

    assert rc == 0
    assert writable_store.upsert_dataset_coverage.call_count == 3
    writable_store.finalize_dataset_snapshot.assert_called_once()
    assert "metadata-only" in capsys.readouterr().out


def test_dataset_snapshot_rejects_as_of_before_required_window_end(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan_with_window()
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    state.persist_manifest(backfill_state_input(plan))
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))
    writable_store = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = writable_store
    context.__exit__.return_value = False
    monkeypatch.setattr(cli, "DuckDBStore", MagicMock(return_value=context))
    monkeypatch.setattr(
        "rquant.backfill_manifest.plan_minute_backfill",
        MagicMock(return_value=plan),
    )

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="growth_board_surge",
            as_of=datetime(2026, 6, 29, 7, tzinfo=UTC),
            manifest_id=plan.manifest.manifest_id,
        )
    )

    assert rc != 0
    writable_store.begin_dataset_snapshot.assert_not_called()


def test_dataset_snapshot_watermarks_are_bounded_by_as_of(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan_with_window()
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    state.persist_manifest(backfill_state_input(plan))
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))
    writable_store = MagicMock()
    writable_store._conn.execute.return_value.fetchone.return_value = (
        datetime(2026, 6, 30, 15),
    )
    context = MagicMock()
    context.__enter__.return_value = writable_store
    context.__exit__.return_value = False
    monkeypatch.setattr(cli, "DuckDBStore", MagicMock(return_value=context))
    monkeypatch.setattr(
        "rquant.backfill_manifest.plan_minute_backfill",
        MagicMock(return_value=plan),
    )
    writable_store.begin_dataset_snapshot.return_value = SimpleNamespace(
        snapshot_id="snapshot-id",
        status="building",
    )
    writable_store.finalize_dataset_snapshot.return_value = SimpleNamespace(
        snapshot_id="snapshot-id",
        status="ready",
    )
    as_of = datetime(2026, 6, 30, 7, tzinfo=UTC)

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="growth_board_surge",
            as_of=as_of,
            manifest_id=plan.manifest.manifest_id,
        )
    )

    assert rc == 0
    calls = writable_store._conn.execute.call_args_list
    assert "trade_time <= ?" in calls[0].args[0]
    assert calls[0].args[1] == [datetime(2026, 6, 30, 15)]
    assert "cal_date <= ?" in calls[1].args[0]
    assert calls[1].args[1] == [date(2026, 6, 30)]


def test_dataset_snapshot_ready_retry_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan()
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    state.persist_manifest(backfill_state_input(plan))
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))
    writable_store = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = writable_store
    context.__exit__.return_value = False
    monkeypatch.setattr(cli, "DuckDBStore", MagicMock(return_value=context))
    monkeypatch.setattr(
        "rquant.backfill_manifest.plan_minute_backfill",
        MagicMock(return_value=plan),
    )
    ready = SimpleNamespace(snapshot_id="snapshot-id", status="ready")
    writable_store.begin_dataset_snapshot.return_value = ready

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="growth_board_surge",
            as_of=datetime(2026, 6, 30, 7, tzinfo=UTC),
            manifest_id=plan.manifest.manifest_id,
        )
    )

    assert rc == 0
    writable_store.upsert_dataset_coverage.assert_not_called()
    writable_store.finalize_dataset_snapshot.assert_not_called()
