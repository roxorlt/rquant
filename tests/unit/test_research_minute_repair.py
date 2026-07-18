"""Governed historical minute research-lake repair tests."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

_COMMIT = "a" * 40
_CST = ZoneInfo("Asia/Shanghai")


def _repair_clock() -> datetime:
    return datetime(2026, 7, 18, 16, 0, tzinfo=_CST)


def _paths(tmp_path: Path):
    from rquant.research_ingest import ResearchIngestPaths

    return ResearchIngestPaths(
        state_dir=tmp_path,
        catalog_path=tmp_path / "research.duckdb",
        readonly_catalog_path=tmp_path / "research_ro.duckdb",
        lake_root=tmp_path / "lake",
        staging_root=tmp_path / "staging",
    )


def _insert_minute_session(
    store,
    *,
    ts_code: str,
    trade_date: date,
    complete: bool = True,
) -> None:
    from rquant.backfill_manifest import minute_session_spec

    times = minute_session_spec().expected_times()
    if not complete:
        times = times[:-1]
    store._conn.executemany(
        """
        INSERT INTO minute_bar (
            ts_code, trade_time, freq, open, high, low, close,
            vol, amount, source, created_at
        ) VALUES (?, ?, '1min', 10, 10.2, 9.9, 10.1,
                  100, 1010, 'tushare', ?)
        """,
        [
            (
                ts_code,
                datetime.combine(trade_date, minute_time),
                datetime(2026, 7, 18, 8, 0),
            )
            for minute_time in times
        ],
    )


def _minute_rows(
    *,
    ts_code: str = "000001.SZ",
    trade_date: date = date(2026, 7, 14),
    created_at: datetime = datetime(2026, 7, 18, 8, 0),
    source: str = "tushare",
):
    import pandas as pd

    from rquant.backfill_manifest import minute_session_spec

    return pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "trade_time": datetime.combine(trade_date, minute_time),
                "freq": "1min",
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 100.0,
                "amount": 1_010.0,
                "source": source,
                "created_at": created_at,
            }
            for minute_time in minute_session_spec().expected_times()
        ]
    )


def _minute_plan():
    from rquant.backfill_manifest import (
        BackfillCoverage,
        BackfillEstimate,
        BackfillManifest,
        BackfillPhaseCoverage,
        EligibilityRecord,
        MergedBackfillWindow,
        MinuteBackfillPlan,
        StrategyBackfillSpec,
        StrategyWindowRequirement,
    )

    entry_date = date(2026, 7, 14)
    eligibility = EligibilityRecord(
        strategy_id="n_shape",
        strategy_version="v1",
        ts_code="000001.SZ",
        eligibility_date=date(2026, 7, 13),
        entry_date=entry_date,
        decision_at=datetime(2026, 7, 13, 9, 30, tzinfo=UTC),
        variant="pool1",
    )
    manifest = BackfillManifest.build(
        spec=StrategyBackfillSpec(
            strategy_id="n_shape",
            strategy_version="v1",
            eligibility_basis="daily",
            eligibility_entry_delay_trading_days=1,
            window=StrategyWindowRequirement(
                baseline_trading_days=1,
                entry_trading_days=1,
                exit_trading_days=1,
            ),
        ),
        start_date=date(2026, 7, 13),
        end_date=date(2026, 7, 13),
        as_of_time=datetime(2026, 7, 18, tzinfo=UTC),
        code_commit=_COMMIT,
        eligibilities=(eligibility,),
    )
    return MinuteBackfillPlan(
        manifest=manifest,
        windows=(
            MergedBackfillWindow(
                ts_code="000001.SZ",
                start_date=date(2026, 7, 13),
                end_date=date(2026, 7, 15),
                open_dates=(
                    date(2026, 7, 13),
                    date(2026, 7, 14),
                    date(2026, 7, 15),
                ),
            ),
        ),
        tasks=(),
        coverage=BackfillCoverage(
            baseline=BackfillPhaseCoverage(
                expected_sessions=1,
                complete_sessions=1,
            ),
            entry=BackfillPhaseCoverage(
                expected_sessions=1,
                complete_sessions=1,
            ),
            exit=BackfillPhaseCoverage(
                expected_sessions=1,
                complete_sessions=1,
            ),
            expected_unique_sessions=3,
            complete_unique_sessions=3,
        ),
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
            confidence_reasons=("fixture",),
        ),
    )


def _seed_minute_repair_case(tmp_path: Path):
    from rquant.backfill_manifest import backfill_state_input
    from rquant.backfill_state import BackfillStateStore
    from rquant.research_ingest import run_daily_research_ingest
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_research_ingest import (
        _Adapter,
        _seed_bootstrap_candidate,
        _seed_source,
        _write_watchlist,
    )

    paths = _paths(tmp_path)
    authority_date = date(2026, 7, 17)
    daily_source = tmp_path / "daily-source.duckdb"
    _seed_source(daily_source, authority_date)
    _seed_bootstrap_candidate(tmp_path, paths=paths)
    _write_watchlist(tmp_path, authority_date, paths=paths)
    run_daily_research_ingest(
        source_database=daily_source,
        paths=paths,
        trade_date=authority_date,
        adapter=_Adapter(authority_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
    )
    operational_path = tmp_path / "operational-ro.duckdb"
    backfill_plan = _minute_plan()
    with DuckDBStore(operational_path) as source:
        for window in backfill_plan.windows:
            for trade_date in window.open_dates:
                _insert_minute_session(
                    source,
                    ts_code=window.ts_code,
                    trade_date=trade_date,
                )
    state = BackfillStateStore(tmp_path / "backfill.sqlite3")
    state.persist_manifest(backfill_state_input(backfill_plan))
    return paths, state, operational_path, backfill_plan


def test_load_completed_backfill_plan_reconstructs_exact_persisted_payload(
    tmp_path: Path,
) -> None:
    from rquant.backfill_manifest import backfill_state_input
    from rquant.backfill_state import BackfillStateStore
    from rquant.research_minute_repair import load_completed_backfill_plan

    state = BackfillStateStore(tmp_path / "backfill.sqlite3")
    plan = _minute_plan()
    state.persist_manifest(backfill_state_input(plan))

    loaded = load_completed_backfill_plan(
        state,
        plan.manifest.manifest_id,
    )

    assert loaded == plan


def test_load_completed_backfill_plan_rejects_nonterminal_manifest(
    tmp_path: Path,
) -> None:
    from rquant.backfill_manifest import MinuteBackfillTask, backfill_state_input
    from rquant.backfill_state import BackfillStateStore
    from rquant.research_minute_repair import load_completed_backfill_plan

    state = BackfillStateStore(tmp_path / "backfill.sqlite3")
    plan = _minute_plan()
    task = MinuteBackfillTask(
        task_id="f" * 64,
        ts_code="000001.SZ",
        source="tushare",
        freq="1min",
        start_date=date(2026, 7, 13),
        end_date=date(2026, 7, 13),
        open_dates=(date(2026, 7, 13),),
        expected_rows=241,
        response_row_limit=8_000,
        possible_truncation=False,
    )
    pending = plan.model_copy(
        update={
            "tasks": (task,),
            "requested_session_count": 1,
        }
    )
    state.persist_manifest(backfill_state_input(pending))

    with pytest.raises(ValueError, match="completed"):
        load_completed_backfill_plan(state, plan.manifest.manifest_id)


def test_load_completed_backfill_plan_rejects_child_eligibility_drift(
    tmp_path: Path,
) -> None:
    from rquant.backfill_manifest import backfill_state_input
    from rquant.backfill_state import BackfillStateStore
    from rquant.research_minute_repair import load_completed_backfill_plan

    state = BackfillStateStore(tmp_path / "backfill.sqlite3")
    plan = _minute_plan()
    persisted = backfill_state_input(plan)
    drifted = persisted.model_copy(
        update={
            "eligibility": (
                persisted.eligibility[0].model_copy(
                    update={
                        "payload": {
                            **persisted.eligibility[0].payload,
                            "variant": "forged",
                        }
                    }
                ),
            )
        }
    )
    state.persist_manifest(drifted)

    with pytest.raises(ValueError, match="eligibility"):
        load_completed_backfill_plan(state, plan.manifest.manifest_id)


def test_required_minute_sessions_use_persisted_windows_and_exclude_unavailable() -> None:
    from rquant.backfill_manifest import (
        BackfillCoverage,
        BackfillPhaseCoverage,
        UnavailableMinuteSession,
    )
    from rquant.research_minute_repair import (
        MinuteRepairSession,
        required_minute_sessions,
    )

    plan = _minute_plan().model_copy(
        update={
            "unavailable_sessions": (
                UnavailableMinuteSession(
                    ts_code="000001.SZ",
                    trade_date=date(2026, 7, 14),
                    reason="known_full_day_suspension",
                ),
            ),
            "coverage": BackfillCoverage(
                baseline=BackfillPhaseCoverage(
                    expected_sessions=1,
                    complete_sessions=1,
                ),
                entry=BackfillPhaseCoverage(
                    expected_sessions=1,
                    complete_sessions=0,
                    accepted_missing_sessions=1,
                ),
                exit=BackfillPhaseCoverage(
                    expected_sessions=1,
                    complete_sessions=1,
                ),
                expected_unique_sessions=3,
                complete_unique_sessions=2,
                accepted_missing_unique_sessions=1,
            ),
        }
    )

    assert required_minute_sessions(plan) == (
        MinuteRepairSession(
            ts_code="000001.SZ",
            trade_date=date(2026, 7, 13),
        ),
        MinuteRepairSession(
            ts_code="000001.SZ",
            trade_date=date(2026, 7, 15),
        ),
    )


def test_minute_repair_returns_unchanged_when_every_window_is_unavailable(
    tmp_path: Path,
) -> None:
    from rquant.backfill_manifest import (
        BackfillCoverage,
        BackfillPhaseCoverage,
        UnavailableMinuteSession,
        backfill_state_input,
    )
    from rquant.backfill_state import BackfillStateStore
    from rquant.research_minute_repair import run_research_minute_repair

    paths, _state, operational_path, base_plan = (
        _seed_minute_repair_case(tmp_path)
    )
    unavailable = tuple(
        UnavailableMinuteSession(
            ts_code=window.ts_code,
            trade_date=trade_date,
            reason="known_full_day_suspension",
        )
        for window in base_plan.windows
        for trade_date in window.open_dates
    )
    empty_plan = base_plan.model_copy(
        update={
            "unavailable_sessions": unavailable,
            "coverage": BackfillCoverage(
                baseline=BackfillPhaseCoverage(
                    expected_sessions=1,
                    complete_sessions=0,
                    accepted_missing_sessions=1,
                ),
                entry=BackfillPhaseCoverage(
                    expected_sessions=1,
                    complete_sessions=0,
                    accepted_missing_sessions=1,
                ),
                exit=BackfillPhaseCoverage(
                    expected_sessions=1,
                    complete_sessions=0,
                    accepted_missing_sessions=1,
                ),
                expected_unique_sessions=3,
                complete_unique_sessions=0,
                accepted_missing_unique_sessions=3,
            ),
        }
    )
    state = BackfillStateStore(tmp_path / "empty-scope.sqlite3")
    state.persist_manifest(backfill_state_input(empty_plan))

    result = run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=empty_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        now=_repair_clock,
    )

    assert result.status == "unchanged"
    assert result.plan.required_session_count == 0
    assert result.plan.unavailable_session_count == 3
    assert result.plan.missing_session_count == 0
    assert result.plan.days == ()


def test_assess_scope_only_repairs_complete_operational_sessions_missing_from_lake(
    tmp_path: Path,
) -> None:
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_lake import export_research_dataset
    from rquant.research_minute_repair import (
        MinuteRepairSession,
        assess_minute_repair_scope,
    )
    from rquant.storage.duckdb import DuckDBStore

    source_path = tmp_path / "operational_ro.duckdb"
    plan = _minute_plan()
    target_dates = tuple(
        day
        for window in plan.windows
        for day in window.open_dates
    )
    with DuckDBStore(source_path) as source:
        source._conn.executemany(
            """
            INSERT INTO trade_calendar (
                exchange, cal_date, is_open, source, updated_at
            ) VALUES ('SSE', ?, TRUE, 'test', ?)
            """,
            [
                (trade_date, datetime(2026, 7, 18, 8, 0))
                for trade_date in target_dates
            ],
        )
        for trade_date in target_dates:
            _insert_minute_session(
                source,
                ts_code="000001.SZ",
                trade_date=trade_date,
            )
        paths = _paths(tmp_path)
        export_research_dataset(
            source._conn,
            catalog=ResearchCatalog(paths.catalog_path),
            lake_root=paths.lake_root,
            dataset="minute_bar",
            start_date=target_dates[0],
            end_date=target_dates[0],
            code_commit=_COMMIT,
            now=lambda: datetime(2026, 7, 18, 8, 30, tzinfo=UTC),
            as_of_date=target_dates[-1],
        )

    scope = assess_minute_repair_scope(
        source_database=source_path,
        paths=paths,
        plan=plan,
        as_of_time=datetime(2026, 7, 18, 9, 0, tzinfo=UTC),
    )

    assert scope.required_session_count == 3
    assert scope.lake_complete_session_count == 1
    assert scope.missing_sessions == (
        MinuteRepairSession(
            ts_code="000001.SZ",
            trade_date=date(2026, 7, 14),
        ),
        MinuteRepairSession(
            ts_code="000001.SZ",
            trade_date=date(2026, 7, 15),
        ),
    )
    assert scope.source_complete_session_count == 2


def test_assess_scope_rejects_partial_operational_source_session(
    tmp_path: Path,
) -> None:
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_minute_repair import assess_minute_repair_scope
    from rquant.storage.duckdb import DuckDBStore

    source_path = tmp_path / "operational_ro.duckdb"
    plan = _minute_plan()
    with DuckDBStore(source_path) as source:
        for trade_date in (
            date(2026, 7, 13),
            date(2026, 7, 14),
        ):
            _insert_minute_session(
                source,
                ts_code="000001.SZ",
                trade_date=trade_date,
            )
        _insert_minute_session(
            source,
            ts_code="000001.SZ",
            trade_date=date(2026, 7, 15),
            complete=False,
        )
    paths = _paths(tmp_path)
    with ResearchCatalog(paths.catalog_path)._connection():
        pass

    with pytest.raises(ValueError, match="operational source is incomplete"):
        assess_minute_repair_scope(
            source_database=source_path,
            paths=paths,
            plan=plan,
            as_of_time=datetime(2026, 7, 18, 9, 0, tzinfo=UTC),
        )


def test_merge_minute_partition_upserts_only_target_sessions_and_preserves_evidence_time() -> None:
    import pandas as pd

    from rquant.research_minute_repair import (
        MinuteRepairSession,
        merge_minute_partition,
    )

    trade_date = date(2026, 7, 14)
    old_time = datetime(2026, 7, 16, 8, 0)
    source_time = datetime(2026, 7, 18, 8, 0)
    existing_target = _minute_rows(
        trade_date=trade_date,
        created_at=old_time,
    ).iloc[:2]
    existing_other = _minute_rows(
        ts_code="000002.SZ",
        trade_date=trade_date,
        created_at=old_time,
        source="tushare_rt_daily",
    ).iloc[:1]
    existing = pd.concat(
        [existing_target, existing_other],
        ignore_index=True,
    )
    operational = _minute_rows(
        trade_date=trade_date,
        created_at=source_time,
    )
    operational.loc[1, "close"] = 10.15

    merged = merge_minute_partition(
        existing,
        operational,
        trade_date=trade_date,
        target_sessions=(
            MinuteRepairSession(
                ts_code="000001.SZ",
                trade_date=trade_date,
            ),
        ),
    )

    target = merged.loc[
        (merged["ts_code"] == "000001.SZ")
        & (merged["source"] == "tushare")
    ].reset_index(drop=True)
    assert len(target) == 241
    assert target.loc[0, "created_at"] == old_time
    assert target.loc[1, "created_at"] == source_time
    assert target.loc[1, "close"] == pytest.approx(10.15)
    assert (
        merged.loc[merged["ts_code"] == "000002.SZ", "source"].tolist()
        == ["tushare_rt_daily"]
    )


def test_minute_row_hash_binds_created_at_and_is_stable_under_input_order() -> None:
    from rquant.research_minute_repair import hash_minute_rows

    frame = _minute_rows().iloc[:3].copy()
    reordered = frame.iloc[::-1].reset_index(drop=True)
    changed_time = frame.copy()
    changed_time.loc[0, "created_at"] = datetime(2026, 7, 18, 9, 0)

    assert hash_minute_rows(frame) == hash_minute_rows(reordered)
    assert hash_minute_rows(frame) != hash_minute_rows(changed_time)


def test_build_minute_repair_day_plan_binds_source_and_merged_content() -> None:
    from rquant.research_minute_repair import (
        MinuteRepairSession,
        build_minute_repair_day_plan,
    )

    trade_date = date(2026, 7, 14)
    target = (
        MinuteRepairSession(
            ts_code="000001.SZ",
            trade_date=trade_date,
        ),
    )
    existing = _minute_rows(
        ts_code="000002.SZ",
        trade_date=trade_date,
        source="tushare_rt_daily",
    ).iloc[:1]
    operational = _minute_rows(trade_date=trade_date)

    day, merged = build_minute_repair_day_plan(
        trade_date=trade_date,
        target_sessions=target,
        existing_manifest_sha256="b" * 64,
        existing=existing,
        operational=operational,
    )

    assert day.target_session_count == 1
    assert day.existing_row_count == 1
    assert day.source_row_count == 241
    assert day.merged_row_count == 242
    assert day.changed is True
    assert len(day.target_sessions_sha256) == 64
    assert len(day.source_rows_sha256) == 64
    assert day.merged_rows_sha256 != day.source_rows_sha256
    assert len(merged) == 242


def test_minute_repair_plan_id_is_stable_and_changes_with_day_content() -> None:
    from rquant.research_minute_repair import (
        ResearchMinuteRepairDayPlan,
        ResearchMinuteRepairPlan,
    )

    day = ResearchMinuteRepairDayPlan(
        trade_date=date(2026, 7, 14),
        target_session_count=1,
        target_sessions_sha256="a" * 64,
        existing_manifest_sha256="b" * 64,
        source_rows_sha256="c" * 64,
        merged_rows_sha256="d" * 64,
        existing_row_count=0,
        source_row_count=241,
        merged_row_count=241,
        changed=True,
    )
    plan = ResearchMinuteRepairPlan(
        code_commit=_COMMIT,
        manifest_id="e" * 64,
        manifest_content_sha256="f" * 64,
        strategy_id="n_shape",
        strategy_version="v1",
        window_scope_sha256="7" * 64,
        unavailable_sessions_sha256="8" * 64,
        authority_current_sha256="1" * 64,
        catalog_sha256="2" * 64,
        readonly_catalog_sha256="3" * 64,
        required_session_count=3,
        unavailable_session_count=0,
        lake_complete_session_count=2,
        missing_session_count=1,
        source_complete_session_count=1,
        required_sessions_sha256="4" * 64,
        missing_sessions_sha256="5" * 64,
        lake_complete_sessions_sha256="6" * 64,
        source_complete_sessions_sha256="5" * 64,
        affected_ts_codes=("000001.SZ",),
        days=(day,),
    )

    restored = ResearchMinuteRepairPlan.model_validate(
        plan.model_dump(mode="json")
    )
    changed = plan.model_copy(
        update={
            "days": (
                day.model_copy(update={"source_rows_sha256": "6" * 64}),
            )
        }
    )

    assert restored.plan_id == plan.plan_id
    assert changed.plan_id != plan.plan_id
    invalid_action = plan.model_dump(mode="json")
    invalid_action["action_id"] = "forged-action"
    with pytest.raises(ValueError, match="action_id"):
        ResearchMinuteRepairPlan.model_validate(invalid_action)


def test_build_minute_repair_plan_is_read_only_clock_independent_and_content_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hashlib

    import rquant.research_minute_repair as repair_module
    from rquant.backfill_manifest import backfill_state_input
    from rquant.backfill_state import BackfillStateStore
    from rquant.research_ingest import run_daily_research_ingest
    from rquant.research_minute_repair import build_research_minute_repair_plan
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_research_ingest import (
        _Adapter,
        _seed_bootstrap_candidate,
        _seed_source,
        _write_watchlist,
    )

    paths = _paths(tmp_path)
    authority_date = date(2026, 7, 17)
    daily_source = tmp_path / "daily-source.duckdb"
    _seed_source(daily_source, authority_date)
    _seed_bootstrap_candidate(tmp_path, paths=paths)
    _write_watchlist(tmp_path, authority_date, paths=paths)
    run_daily_research_ingest(
        source_database=daily_source,
        paths=paths,
        trade_date=authority_date,
        adapter=_Adapter(authority_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
    )

    operational_path = tmp_path / "operational-ro.duckdb"
    plan = _minute_plan()
    with DuckDBStore(operational_path) as source:
        for window in plan.windows:
            for trade_date in window.open_dates:
                _insert_minute_session(
                    source,
                    ts_code=window.ts_code,
                    trade_date=trade_date,
                )
    state = BackfillStateStore(tmp_path / "backfill.sqlite3")
    state.persist_manifest(backfill_state_input(plan))
    current_path = tmp_path / "research-authority-current.json"
    before_current = current_path.read_bytes()
    before_catalog = hashlib.sha256(paths.catalog_path.read_bytes()).hexdigest()
    original_catalog = repair_module.ResearchCatalog
    catalog_modes: list[bool] = []

    class CatalogSpy(original_catalog):
        def __init__(self, path: Path, *, read_only: bool = False) -> None:
            catalog_modes.append(read_only)
            super().__init__(path, read_only=read_only)

    monkeypatch.setattr(repair_module, "ResearchCatalog", CatalogSpy)

    first = build_research_minute_repair_plan(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=plan.manifest.manifest_id,
        code_commit=_COMMIT,
        as_of_time=datetime(2026, 7, 18, 9, 0, tzinfo=UTC),
    )
    second = build_research_minute_repair_plan(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=plan.manifest.manifest_id,
        code_commit=_COMMIT,
        as_of_time=datetime(2026, 7, 18, 10, 0, tzinfo=UTC),
    )

    assert first.plan_id == second.plan_id
    assert first.manifest_id == plan.manifest.manifest_id
    assert first.strategy_id == "n_shape"
    assert first.required_session_count == 3
    assert first.lake_complete_session_count == 0
    assert first.missing_session_count == 3
    assert first.source_complete_session_count == 3
    assert first.affected_ts_codes == ("000001.SZ",)
    assert first.window_scope_sha256 != first.unavailable_sessions_sha256
    assert first.lake_complete_sessions_sha256 != first.missing_sessions_sha256
    assert (
        first.source_complete_sessions_sha256
        == first.missing_sessions_sha256
    )
    assert tuple(day.trade_date for day in first.days) == (
        date(2026, 7, 13),
        date(2026, 7, 14),
        date(2026, 7, 15),
    )
    assert all(day.source_row_count == 241 for day in first.days)
    assert catalog_modes == [True, True]
    assert current_path.read_bytes() == before_current
    assert hashlib.sha256(paths.catalog_path.read_bytes()).hexdigest() == before_catalog


def test_run_minute_repair_atomically_publishes_all_days_and_becomes_unchanged(
    tmp_path: Path,
) -> None:
    import duckdb

    from rquant.backfill_manifest import backfill_state_input
    from rquant.backfill_state import BackfillStateStore
    from rquant.research_ingest import (
        inspect_research_authority,
        run_daily_research_ingest,
    )
    from rquant.research_lake import (
        ResearchPartitionKey,
        ResearchPartitionManifest,
        partition_manifest_relative_path,
    )
    from rquant.research_minute_repair import run_research_minute_repair
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_research_ingest import (
        _Adapter,
        _seed_bootstrap_candidate,
        _seed_source,
        _write_watchlist,
    )

    paths = _paths(tmp_path)
    authority_date = date(2026, 7, 17)
    daily_source = tmp_path / "daily-source.duckdb"
    _seed_source(daily_source, authority_date)
    _seed_bootstrap_candidate(tmp_path, paths=paths)
    _write_watchlist(tmp_path, authority_date, paths=paths)
    run_daily_research_ingest(
        source_database=daily_source,
        paths=paths,
        trade_date=authority_date,
        adapter=_Adapter(authority_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
    )
    operational_path = tmp_path / "operational-ro.duckdb"
    backfill_plan = _minute_plan()
    with DuckDBStore(operational_path) as source:
        for window in backfill_plan.windows:
            for trade_date in window.open_dates:
                _insert_minute_session(
                    source,
                    ts_code=window.ts_code,
                    trade_date=trade_date,
                )
    state = BackfillStateStore(tmp_path / "backfill.sqlite3")
    state.persist_manifest(backfill_state_input(backfill_plan))
    preview = run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=backfill_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        now=_repair_clock,
    )
    applied = run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=backfill_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        apply=True,
        plan_id=preview.plan_id,
        now=_repair_clock,
    )

    assert preview.status == "planned"
    assert applied.status == "candidate"
    assert applied.observation is not None
    assert applied.observation.manifest_id == backfill_plan.manifest.manifest_id
    assert applied.observation.plan_id == preview.plan_id
    assert len(applied.observation.repairs) == 3
    assert inspect_research_authority(paths).stable_trading_days == 0
    assert not tuple(paths.transactions_root.glob("**/*journal.json"))
    for trade_date in (
        date(2026, 7, 13),
        date(2026, 7, 14),
        date(2026, 7, 15),
    ):
        key = ResearchPartitionKey(
            dataset="minute_bar",
            trade_date=trade_date,
            freq="1min",
        )
        manifest_path = paths.lake_root / partition_manifest_relative_path(key)
        manifest = ResearchPartitionManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        )
        with duckdb.connect() as connection:
            row_count = connection.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(paths.lake_root / manifest.relative_path)],
            ).fetchone()[0]
        assert row_count == 241

    unchanged = run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=backfill_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        now=_repair_clock,
    )

    assert unchanged.status == "unchanged"
    assert unchanged.plan.missing_session_count == 0
    assert unchanged.plan.days == ()


def test_plan_baseline_rejects_a_symlinked_manifest_even_when_hash_matches(
    tmp_path: Path,
) -> None:
    import hashlib

    import rquant.research_minute_repair as repair_module
    from rquant.research_lake import partition_manifest_relative_path

    paths, state, operational_path, backfill_plan = (
        _seed_minute_repair_case(tmp_path)
    )
    preview = repair_module.run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=backfill_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        now=_repair_clock,
    )
    day = preview.plan.days[0]
    target = tmp_path / "manifest-target.json"
    target.write_text('{"fixture":true}\n', encoding="utf-8")
    target_hash = hashlib.sha256(target.read_bytes()).hexdigest()
    manifest_path = (
        paths.lake_root
        / partition_manifest_relative_path(
            repair_module._minute_partition_key(day.trade_date)
        )
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.symlink_to(target)
    bound_plan = preview.plan.model_copy(
        update={
            "days": (
                day.model_copy(
                    update={"existing_manifest_sha256": target_hash}
                ),
                *preview.plan.days[1:],
            )
        }
    )

    with pytest.raises(RuntimeError, match="invalid minute manifest path"):
        repair_module._verify_plan_baseline(paths, bound_plan)


def test_minute_repair_preserves_a_dangling_immutable_version_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.research_minute_repair as repair_module

    paths, state, operational_path, backfill_plan = (
        _seed_minute_repair_case(tmp_path)
    )
    preview = repair_module.run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=backfill_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        now=_repair_clock,
    )
    original_prepare_journal = repair_module._prepare_repair_journal
    dangling_version: Path | None = None

    def prepare_journal_with_dangling_version(*args, **kwargs):
        nonlocal dangling_version
        observation = kwargs["observation"]
        dangling_version = (
            paths.lake_root
            / observation.repairs[0].after_manifest.relative_path
        )
        dangling_version.parent.mkdir(parents=True, exist_ok=True)
        dangling_version.symlink_to(tmp_path / "missing-version.parquet")
        return original_prepare_journal(*args, **kwargs)

    def fail_after_versions(step: str) -> None:
        if step == "versions_published":
            raise RuntimeError("injected failure after versions")

    monkeypatch.setattr(
        repair_module,
        "_prepare_repair_journal",
        prepare_journal_with_dangling_version,
    )
    monkeypatch.setattr(
        repair_module,
        "_publish_step_hook",
        fail_after_versions,
    )

    with pytest.raises(RuntimeError):
        repair_module.run_research_minute_repair(
            source_database=operational_path,
            paths=paths,
            state=state,
            manifest_id=backfill_plan.manifest.manifest_id,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=_repair_clock,
        )

    assert dangling_version is not None
    assert dangling_version.is_symlink()
    assert not dangling_version.exists()


@pytest.mark.parametrize(
    "failure_step",
    [
        "versions_published",
        "manifests_published",
        "catalog_published",
        "readonly_published",
        "authority_published",
    ],
)
def test_minute_repair_publish_failure_rolls_back_every_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_step: str,
) -> None:
    import hashlib

    import rquant.research_minute_repair as repair_module
    from rquant.research_lake import (
        ResearchPartitionKey,
        partition_directory,
        partition_manifest_relative_path,
    )

    paths, state, operational_path, backfill_plan = (
        _seed_minute_repair_case(tmp_path)
    )
    preview = repair_module.run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=backfill_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        now=_repair_clock,
    )
    before = {
        "catalog": hashlib.sha256(paths.catalog_path.read_bytes()).hexdigest(),
        "readonly": hashlib.sha256(
            paths.readonly_catalog_path.read_bytes()
        ).hexdigest(),
        "current": (
            paths.state_dir / "research-authority-current.json"
        ).read_bytes(),
        "observations": tuple(
            sorted(
                path.relative_to(paths.state_dir).as_posix()
                for path in (
                    paths.state_dir / "research_observations"
                ).glob("**/*.json")
            )
        ),
    }

    def fail_at(step: str) -> None:
        if step == failure_step:
            raise RuntimeError(f"injected failure at {step}")

    monkeypatch.setattr(repair_module, "_publish_step_hook", fail_at)

    with pytest.raises(RuntimeError, match="injected failure"):
        repair_module.run_research_minute_repair(
            source_database=operational_path,
            paths=paths,
            state=state,
            manifest_id=backfill_plan.manifest.manifest_id,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=_repair_clock,
        )

    assert hashlib.sha256(paths.catalog_path.read_bytes()).hexdigest() == before[
        "catalog"
    ]
    assert hashlib.sha256(
        paths.readonly_catalog_path.read_bytes()
    ).hexdigest() == before["readonly"]
    assert (
        paths.state_dir / "research-authority-current.json"
    ).read_bytes() == before["current"]
    assert tuple(
        sorted(
            path.relative_to(paths.state_dir).as_posix()
            for path in (
                paths.state_dir / "research_observations"
            ).glob("**/*.json")
        )
    ) == before["observations"]
    assert not paths.transactions_root.exists() or not tuple(
        paths.transactions_root.iterdir()
    )
    for trade_date in (
        date(2026, 7, 13),
        date(2026, 7, 14),
        date(2026, 7, 15),
    ):
        key = ResearchPartitionKey(
            dataset="minute_bar",
            trade_date=trade_date,
            freq="1min",
        )
        assert not (
            paths.lake_root / partition_manifest_relative_path(key)
        ).exists()
        versions = (
            paths.lake_root / partition_directory(key) / "versions"
        )
        assert not versions.exists() or not tuple(versions.iterdir())


def test_minute_repair_rechecks_protection_before_each_live_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.research_minute_repair as repair_module

    paths, state, operational_path, backfill_plan = (
        _seed_minute_repair_case(tmp_path)
    )
    preview = repair_module.run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=backfill_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        now=_repair_clock,
    )
    clock_calls = 0

    def moving_clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        minute = 0 if clock_calls <= 5 else 15
        return datetime(2026, 7, 20, 9, minute, tzinfo=_CST)

    original_copy = repair_module._copy_file_atomic
    published_versions: list[Path] = []

    def track_copy(source: Path, target: Path) -> None:
        if paths.lake_root in target.parents and target.suffix == ".parquet":
            published_versions.append(target)
        original_copy(source, target)

    monkeypatch.setattr(repair_module, "_copy_file_atomic", track_copy)

    with pytest.raises(
        ValueError,
        match="forbidden during market protection window",
    ):
        repair_module.run_research_minute_repair(
            source_database=operational_path,
            paths=paths,
            state=state,
            manifest_id=backfill_plan.manifest.manifest_id,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=moving_clock,
        )

    assert len(published_versions) == 1
    assert not paths.transactions_root.exists() or not tuple(
        paths.transactions_root.iterdir()
    )


def test_next_apply_recovers_an_interrupted_minute_repair_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.research_minute_repair as repair_module

    paths, state, operational_path, backfill_plan = (
        _seed_minute_repair_case(tmp_path)
    )
    preview = repair_module.run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=backfill_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        now=_repair_clock,
    )
    original_rollback = repair_module.rollback_research_minute_repair_publish

    def fail_after_catalog(step: str) -> None:
        if step == "catalog_published":
            raise RuntimeError("simulated process interruption")

    def leave_journal(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("simulated unavailable rollback")

    monkeypatch.setattr(repair_module, "_publish_step_hook", fail_after_catalog)
    monkeypatch.setattr(
        repair_module,
        "rollback_research_minute_repair_publish",
        leave_journal,
    )
    with pytest.raises(RuntimeError, match="rollback is pending"):
        repair_module.run_research_minute_repair(
            source_database=operational_path,
            paths=paths,
            state=state,
            manifest_id=backfill_plan.manifest.manifest_id,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=_repair_clock,
        )
    assert tuple(paths.transactions_root.glob("**/minute-repair-journal.json"))

    monkeypatch.setattr(repair_module, "_publish_step_hook", lambda _step: None)
    monkeypatch.setattr(
        repair_module,
        "rollback_research_minute_repair_publish",
        original_rollback,
    )

    recovered = repair_module.run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=backfill_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        apply=True,
        plan_id=preview.plan_id,
        now=_repair_clock,
    )

    assert recovered.status == "candidate"
    assert not tuple(paths.transactions_root.glob("**/*journal.json"))


def test_minute_repair_apply_rejects_source_drift_after_preview(
    tmp_path: Path,
) -> None:
    import duckdb

    from rquant.research_minute_repair import run_research_minute_repair

    paths, state, operational_path, backfill_plan = (
        _seed_minute_repair_case(tmp_path)
    )
    preview = run_research_minute_repair(
        source_database=operational_path,
        paths=paths,
        state=state,
        manifest_id=backfill_plan.manifest.manifest_id,
        code_commit=_COMMIT,
        now=_repair_clock,
    )
    current_path = paths.state_dir / "research-authority-current.json"
    before_current = current_path.read_bytes()
    with duckdb.connect(str(operational_path)) as connection:
        connection.execute(
            """
            UPDATE minute_bar
            SET close = close + 0.01
            WHERE ts_code = '000001.SZ'
              AND trade_time = TIMESTAMP '2026-07-14 09:30:00'
              AND freq = '1min'
              AND source = 'tushare'
            """
        )

    with pytest.raises(ValueError, match="stale minute repair plan"):
        run_research_minute_repair(
            source_database=operational_path,
            paths=paths,
            state=state,
            manifest_id=backfill_plan.manifest.manifest_id,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=_repair_clock,
        )

    assert current_path.read_bytes() == before_current
    assert not tuple(paths.transactions_root.glob("**/*journal.json"))
