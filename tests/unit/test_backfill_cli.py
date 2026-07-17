"""Manifest backfill CLI contracts."""

from __future__ import annotations

import sys
import time
from argparse import Namespace
from datetime import UTC, date, datetime, timedelta
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
    EligibilityResolution,
    MergedBackfillWindow,
    MinuteBackfillPlan,
    backfill_state_input,
)
from rquant.backfill_state import BackfillStateStore
from rquant.data_metadata import DatasetSnapshotArtifact

SHANGHAI = ZoneInfo("Asia/Shanghai")
_COMMIT = "a" * 40
_SNAPSHOT_WRITE_WINDOW_GUARD = cli._dataset_snapshot_write_window_safe


def _lake_artifact(
    *,
    dataset: str,
    trade_date: date,
    marker: str,
) -> DatasetSnapshotArtifact:
    file_hash = marker * 64
    return DatasetSnapshotArtifact(
        artifact_type="lake_partition",
        dataset_id=dataset,
        table_name=dataset,
        artifact_key=f"{dataset}:{trade_date.isoformat()}",
        partition_id=f"{dataset}:{trade_date.isoformat()}",
        relative_path=(
            f"{dataset}/trade_date={trade_date.isoformat()}/versions/"
            f"{file_hash}.parquet"
        ),
        row_count=1,
        schema_hash="a" * 64,
        content_hash="b" * 64,
        file_hash=file_hash,
        file_size=1,
        earliest_time=trade_date.isoformat(),
        latest_time=trade_date.isoformat(),
        event_column="trade_date",
        source="tushare",
        primary_key=("ts_code", "trade_date", "auction_type", "source"),
    )


@pytest.fixture(autouse=True)
def _stable_snapshot_apply_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli,
        "_dataset_snapshot_write_window_safe",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        "rquant.backfill_manifest.resolve_strategy_eligibility",
        lambda *_args, **_kwargs: _plan().manifest.eligibility_resolution,
    )


def _plan(
    *,
    baseline_complete: int = 90,
    entry_complete: int = 1,
    include_resolution: bool = True,
) -> MinuteBackfillPlan:
    eligibility = EligibilityRecord(
        strategy_id="growth_board_surge",
        strategy_version="v1",
        ts_code="300001.SZ",
        eligibility_date=date(2026, 6, 26),
        entry_date=date(2026, 6, 26),
        decision_at=datetime(2026, 6, 26, 9, 32, tzinfo=SHANGHAI),
        variant="growth",
    )
    resolution = EligibilityResolution(
        strategy_id="growth_board_surge",
        strategy_version="v1",
        requested_dates=(eligibility.eligibility_date,),
        evaluated_dates=(eligibility.eligibility_date,),
        complete_dates=(eligibility.eligibility_date,),
        records=(eligibility,),
    )
    manifest = BackfillManifest.build(
        spec=STRATEGY_BACKFILL_SPECS["growth_board_surge"],
        start_date=date(2026, 6, 26),
        end_date=date(2026, 6, 26),
        as_of_time=datetime(2026, 6, 26, 2, tzinfo=UTC),
        code_commit=_COMMIT,
        eligibilities=(eligibility,),
        eligibility_resolution=resolution if include_resolution else None,
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


def _auction_plan(
    input_artifact: DatasetSnapshotArtifact,
) -> MinuteBackfillPlan:
    eligibility = EligibilityRecord(
        strategy_id="auction_gap",
        strategy_version="v1",
        ts_code="600000.SH",
        eligibility_date=date(2026, 6, 26),
        entry_date=date(2026, 6, 26),
        decision_at=datetime(2026, 6, 26, 9, 27, tzinfo=SHANGHAI),
        variant="auction_gap",
    )
    resolution = EligibilityResolution(
        strategy_id="auction_gap",
        strategy_version="v1",
        requested_dates=(eligibility.eligibility_date,),
        evaluated_dates=(eligibility.eligibility_date,),
        complete_dates=(eligibility.eligibility_date,),
        records=(eligibility,),
        input_artifacts=(input_artifact,),
    )
    base = _plan()
    manifest = BackfillManifest.build(
        spec=STRATEGY_BACKFILL_SPECS["auction_gap"],
        start_date=eligibility.eligibility_date,
        end_date=eligibility.eligibility_date,
        as_of_time=datetime(2026, 6, 26, 2, tzinfo=UTC),
        code_commit=_COMMIT,
        eligibilities=(eligibility,),
        eligibility_resolution=resolution,
    )
    return base.model_copy(update={"manifest": manifest})


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
    snapshot_preview = parser.parse_args(
        [
            "dataset-snapshot",
            "--strategy",
            "auction_gap",
            "--as-of",
            "2026-06-30T15:00:00+08:00",
            "--manifest-id",
            "d" * 64,
            "--dry-run",
        ]
    )
    snapshot_apply = parser.parse_args(
        [
            "dataset-snapshot",
            "--strategy",
            "auction_gap",
            "--as-of",
            "2026-06-30T15:00:00+08:00",
            "--manifest-id",
            "d" * 64,
            "--apply",
        ]
    )

    assert plan.start_date == date(2026, 1, 1)
    assert run.retry_failed is False
    assert status.json is True
    assert snapshot.as_of == datetime(2026, 6, 30, 7, tzinfo=UTC)
    assert snapshot.apply is False
    assert snapshot.dry_run is False
    assert snapshot.deadline_worker is False
    assert snapshot_preview.dry_run is True
    assert snapshot_preview.apply is False
    assert snapshot_apply.apply is True


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


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (datetime(2026, 7, 15, 9, 14, tzinfo=SHANGHAI), False),
        (datetime(2026, 7, 15, 9, 15, tzinfo=SHANGHAI), True),
        (datetime(2026, 7, 15, 15, 10, tzinfo=SHANGHAI), True),
        (datetime(2026, 7, 15, 15, 11, tzinfo=SHANGHAI), False),
        (datetime(2026, 7, 18, 10, tzinfo=SHANGHAI), False),
    ],
)
def test_dataset_snapshot_protected_window(
    now: datetime,
    expected: bool,
) -> None:
    assert cli._in_backfill_protected_window(now) is expected


@pytest.mark.parametrize(
    ("now", "estimated_seconds", "expected"),
    [
        (datetime(2026, 7, 15, 7, 30, tzinfo=SHANGHAI), 1_800, True),
        (datetime(2026, 7, 15, 9, 14, tzinfo=SHANGHAI), 1, False),
        (datetime(2026, 7, 15, 15, 11, tzinfo=SHANGHAI), 1_800, True),
        (datetime(2026, 7, 18, 9, 30, tzinfo=SHANGHAI), 50_000, True),
    ],
)
def test_dataset_snapshot_write_window_guard(
    now: datetime,
    estimated_seconds: float,
    expected: bool,
) -> None:
    assert (
        _SNAPSHOT_WRITE_WINDOW_GUARD(now, estimated_seconds)
        is expected
    )


def test_dataset_snapshot_stops_when_execution_reaches_deadline(
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
    clock = [datetime(2026, 7, 15, 8, 0, tzinfo=SHANGHAI)]
    monkeypatch.setattr(cli, "_snapshot_now", lambda: clock[0])

    def cross_deadline(*_args, **_kwargs):
        clock[0] = datetime(2026, 7, 15, 9, 14, tzinfo=SHANGHAI)
        return plan

    monkeypatch.setattr(
        "rquant.backfill_manifest.plan_minute_backfill",
        cross_deadline,
    )
    binding_builder = MagicMock()
    monkeypatch.setattr(
        "rquant.research_snapshot.build_dataset_snapshot_binding",
        binding_builder,
    )

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="growth_board_surge",
            as_of=datetime(2026, 6, 30, 7, tzinfo=UTC),
            manifest_id=plan.manifest.manifest_id,
            apply=True,
        )
    )

    assert rc != 0
    writable_store.begin_dataset_snapshot.assert_not_called()
    binding_builder.assert_not_called()


def test_deadline_supervisor_kills_a_blocked_worker_process() -> None:
    started = time.monotonic()

    rc = cli._run_deadline_supervised_process(
        (sys.executable, "-c", "import time; time.sleep(10)"),
        deadline=datetime.now(UTC) + timedelta(seconds=0.2),
    )

    assert rc != 0
    assert time.monotonic() - started < 3


def test_dataset_snapshot_apply_parent_never_opens_duckdb(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan()
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    state.persist_manifest(backfill_state_input(plan))
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))
    store_factory = MagicMock()
    monkeypatch.setattr(cli, "DuckDBStore", store_factory)
    supervisor = MagicMock(return_value=0)
    monkeypatch.setattr(
        cli,
        "_run_dataset_snapshot_supervised_worker",
        supervisor,
    )
    args = Namespace(
        strategy="growth_board_surge",
        as_of=datetime(2026, 6, 30, 7, tzinfo=UTC),
        manifest_id=plan.manifest.manifest_id,
        apply=True,
        deadline_worker=False,
    )

    rc = cli.cmd_dataset_snapshot(args)

    assert rc == 0
    supervisor.assert_called_once()
    store_factory.assert_not_called()


def test_snapshot_deadline_context_closes_store_when_alarm_fires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 15, 8, 0, tzinfo=SHANGHAI)
    monkeypatch.setattr(cli, "_snapshot_now", lambda: now)
    inner = MagicMock()
    inner.__enter__.return_value = MagicMock()
    inner.__exit__.return_value = False
    guarded = cli._SnapshotDeadlineStoreContext(
        inner,
        deadline=datetime(2026, 7, 15, 9, 14, tzinfo=SHANGHAI),
    )

    with guarded:
        guarded._handle_deadline(0, None)

    assert guarded.expired is True
    assert inner.__exit__.call_count == 1


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
        MagicMock(return_value=planned.manifest.eligibility_resolution),
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
            apply=True,
        )
    )

    assert rc != 0
    planner.assert_called_once()
    writable_store.begin_dataset_snapshot.assert_not_called()


def test_dataset_snapshot_rejects_manifest_without_eligibility_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy_plan = _plan(include_resolution=False)
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    state.persist_manifest(backfill_state_input(legacy_plan))
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))
    writable_store = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = writable_store
    context.__exit__.return_value = False
    monkeypatch.setattr(cli, "DuckDBStore", MagicMock(return_value=context))
    monkeypatch.setattr(
        "rquant.backfill_manifest.plan_minute_backfill",
        MagicMock(return_value=legacy_plan),
    )

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="growth_board_surge",
            as_of=datetime(2026, 6, 30, 7, tzinfo=UTC),
            manifest_id=legacy_plan.manifest.manifest_id,
            apply=True,
        )
    )

    assert rc != 0
    writable_store.begin_dataset_snapshot.assert_not_called()


def test_dataset_snapshot_rejects_stale_eligibility_resolution(
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
    changed = EligibilityRecord(
        strategy_id="growth_board_surge",
        strategy_version="v1",
        ts_code="300002.SZ",
        eligibility_date=date(2026, 6, 26),
        entry_date=date(2026, 6, 26),
        decision_at=datetime(2026, 6, 26, 9, 32, tzinfo=SHANGHAI),
        variant="growth",
    )
    changed_resolution = EligibilityResolution(
        strategy_id="growth_board_surge",
        strategy_version="v1",
        requested_dates=(changed.eligibility_date,),
        evaluated_dates=(changed.eligibility_date,),
        complete_dates=(changed.eligibility_date,),
        records=(changed,),
    )
    monkeypatch.setattr(
        "rquant.backfill_manifest.resolve_strategy_eligibility",
        MagicMock(return_value=changed_resolution),
    )
    planner = MagicMock(return_value=plan)
    monkeypatch.setattr("rquant.backfill_manifest.plan_minute_backfill", planner)

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="growth_board_surge",
            as_of=datetime(2026, 6, 30, 7, tzinfo=UTC),
            manifest_id=plan.manifest.manifest_id,
            apply=True,
        )
    )

    assert rc != 0
    planner.assert_not_called()
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
    binding_builder = MagicMock(
        return_value=SimpleNamespace(binding_hash="binding-hash")
    )
    monkeypatch.setattr(
        "rquant.research_snapshot.build_dataset_snapshot_binding",
        binding_builder,
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
            apply=True,
        )
    )

    assert rc == 0
    assert writable_store.upsert_dataset_coverage.call_count == 4
    writable_store.finalize_dataset_snapshot.assert_called_once()
    binding_builder.assert_called_once()
    assert "binding-hash" in capsys.readouterr().out


def test_dataset_snapshot_defaults_to_readonly_preview_and_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    plan = _plan()
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    state.persist_manifest(backfill_state_input(plan))
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))
    readonly_store = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = readonly_store
    context.__exit__.return_value = False
    monkeypatch.setattr(cli, "open_readonly_store", MagicMock(return_value=context))
    writable_factory = MagicMock()
    monkeypatch.setattr(cli, "DuckDBStore", writable_factory)
    monkeypatch.setattr(
        "rquant.backfill_manifest.plan_minute_backfill",
        MagicMock(return_value=plan),
    )
    monkeypatch.setattr(
        "rquant.research_catalog.ResearchCatalog",
        MagicMock(),
    )
    binding_builder = MagicMock()
    monkeypatch.setattr(
        "rquant.research_snapshot.build_dataset_snapshot_binding",
        binding_builder,
    )

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="growth_board_surge",
            as_of=datetime(2026, 6, 30, 7, tzinfo=UTC),
            manifest_id=plan.manifest.manifest_id,
        )
    )

    assert rc == 0
    writable_factory.assert_not_called()
    binding_builder.assert_not_called()
    assert '"status":"dry_run"' in capsys.readouterr().out


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
            apply=True,
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
    monkeypatch.setattr(
        "rquant.research_snapshot.build_dataset_snapshot_binding",
        MagicMock(return_value=SimpleNamespace(binding_hash="binding-hash")),
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
            apply=True,
        )
    )

    assert rc == 0
    calls = writable_store._conn.execute.call_args_list
    assert "trade_time <= ?" in calls[0].args[0]
    assert calls[0].args[1] == [datetime(2026, 6, 30, 15)]
    assert "cal_date <= ?" in calls[1].args[0]
    assert calls[1].args[1] == [date(2026, 6, 30)]
    finalization = writable_store.finalize_dataset_snapshot.call_args.args[1]
    assert finalization.table_watermarks["manifest_start_date"] == (
        plan.manifest.start_date.isoformat()
    )
    assert finalization.table_watermarks["manifest_end_date"] == (
        plan.manifest.end_date.isoformat()
    )


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
    binding_builder = MagicMock(
        return_value=SimpleNamespace(binding_hash="binding-hash")
    )
    monkeypatch.setattr(
        "rquant.research_snapshot.build_dataset_snapshot_binding",
        binding_builder,
    )
    ready = SimpleNamespace(snapshot_id="snapshot-id", status="ready")
    writable_store.begin_dataset_snapshot.return_value = ready

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="growth_board_surge",
            as_of=datetime(2026, 6, 30, 7, tzinfo=UTC),
            manifest_id=plan.manifest.manifest_id,
            apply=True,
        )
    )

    assert rc == 0
    writable_store.upsert_dataset_coverage.assert_not_called()
    writable_store.finalize_dataset_snapshot.assert_not_called()
    binding_builder.assert_called_once()


def test_dataset_snapshot_keeps_planned_auction_artifact_when_catalog_head_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    planned_auction = _lake_artifact(
        dataset="auction_bar",
        trade_date=date(2026, 6, 26),
        marker="c",
    )
    current_auction = planned_auction.model_copy(
        update={
            "relative_path": planned_auction.relative_path.replace("c" * 64, "d" * 64),
            "file_hash": "d" * 64,
            "content_hash": "e" * 64,
        }
    )
    plan = _auction_plan(planned_auction)
    state = BackfillStateStore(tmp_path / "state.sqlite3")
    state.persist_manifest(backfill_state_input(plan))
    monkeypatch.setattr(cli, "BackfillStateStore", MagicMock(return_value=state))
    writable_store = MagicMock()
    writable_store._conn.execute.return_value.fetchone.return_value = (
        datetime(2026, 6, 26, 15),
    )
    context = MagicMock()
    context.__enter__.return_value = writable_store
    context.__exit__.return_value = False
    monkeypatch.setattr(cli, "DuckDBStore", MagicMock(return_value=context))
    monkeypatch.setattr(
        "rquant.backfill_manifest.plan_minute_backfill",
        MagicMock(return_value=plan),
    )
    monkeypatch.setattr(
        "rquant.research_snapshot.resolve_strategy_eligibility_from_artifacts",
        MagicMock(return_value=plan.manifest.eligibility_resolution),
    )
    resolver = MagicMock()
    resolver.resolve_lake_partitions.return_value = (current_auction,)
    monkeypatch.setattr(
        "rquant.research_snapshot.SnapshotArtifactResolver",
        MagicMock(return_value=resolver),
    )
    binding_builder = MagicMock(
        return_value=SimpleNamespace(binding_hash="binding-hash")
    )
    monkeypatch.setattr(
        "rquant.research_snapshot.build_dataset_snapshot_binding",
        binding_builder,
    )
    writable_store.begin_dataset_snapshot.return_value = SimpleNamespace(
        snapshot_id="snapshot-id",
        status="ready",
    )

    rc = cli.cmd_dataset_snapshot(
        Namespace(
            strategy="auction_gap",
            as_of=datetime(2026, 6, 30, 7, tzinfo=UTC),
            manifest_id=plan.manifest.manifest_id,
            apply=True,
        )
    )

    assert rc == 0
    lake_artifacts = binding_builder.call_args.kwargs["lake_artifacts"]
    selected = [
        artifact
        for artifact in lake_artifacts
        if artifact.dataset_id == "auction_bar"
    ]
    assert selected == [planned_auction]
