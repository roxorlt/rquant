"""Minute backfill planning, coverage, and ETA contracts."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant.data_quality import DEFAULT_MINUTE_SOURCE_SESSION_SPECS
from rquant.storage.duckdb import DuckDBStore
from rquant.trade_calendar import TradeCalendarDay


def _weekday_opens(start: date, count: int) -> list[date]:
    values: list[date] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current)
        current += timedelta(days=1)
    return values


def _seed_calendar(store: DuckDBStore, opens: list[date]) -> None:
    open_set = set(opens)
    rows: list[TradeCalendarDay] = []
    current = opens[0]
    previous_open: date | None = None
    while current <= opens[-1]:
        is_open = current in open_set
        rows.append(
            TradeCalendarDay(
                exchange="SSE",
                cal_date=current,
                is_open=is_open,
                pretrade_date=previous_open,
                source="test",
                updated_at=datetime(2026, 7, 15, tzinfo=UTC),
            )
        )
        if is_open:
            previous_open = current
        current += timedelta(days=1)
    store.upsert_trade_calendar(rows)


def _manifest(
    *,
    entries: list[tuple[str, date]],
    baseline_days: int = 90,
    exit_days: int = 10,
    as_of_time: datetime = datetime(2026, 7, 15, tzinfo=UTC),
):
    from rquant.backfill_manifest import (
        BackfillManifest,
        EligibilityRecord,
        StrategyBackfillSpec,
        StrategyWindowRequirement,
    )

    spec = StrategyBackfillSpec(
        strategy_id="growth_board_surge",
        strategy_version="planner-test-v1",
        eligibility_basis="daily",
        window=StrategyWindowRequirement(
            baseline_trading_days=baseline_days,
            entry_trading_days=1,
            exit_trading_days=exit_days,
        ),
    )
    records = [
        EligibilityRecord(
            strategy_id=spec.strategy_id,
            strategy_version=spec.strategy_version,
            ts_code=ts_code,
            eligibility_date=entry_date,
            entry_date=entry_date,
            decision_at=datetime.combine(entry_date, time(9, 30), tzinfo=UTC),
            variant="gem",
        )
        for ts_code, entry_date in entries
    ]
    return BackfillManifest.build(
        spec=spec,
        start_date=min(day for _, day in entries),
        end_date=max(day for _, day in entries),
        as_of_time=as_of_time,
        code_commit="c" * 40,
        eligibilities=records,
    )


def _insert_session(
    store: DuckDBStore,
    *,
    ts_code: str,
    trade_date: date,
    times: tuple[time, ...],
) -> None:
    store._conn.executemany(
        """
        INSERT INTO minute_bar (
            ts_code, trade_time, freq, open, high, low, close,
            vol, amount, source
        ) VALUES (?, ?, '1min', 10, 10, 10, 10, 100, 1000, 'tushare')
        """,
        [
            (ts_code, datetime.combine(trade_date, minute_time))
            for minute_time in times
        ],
    )


@pytest.fixture()
def store(tmp_path: Path):
    with DuckDBStore(tmp_path / "planner.duckdb") as value:
        yield value


def test_overlapping_windows_merge_but_keep_scope_denominators(
    store: DuckDBStore,
) -> None:
    from rquant.backfill_manifest import plan_minute_backfill

    opens = _weekday_opens(date(2026, 1, 2), 120)
    _seed_calendar(store, opens)
    manifest = _manifest(
        entries=[("300001.SZ", opens[90]), ("300001.SZ", opens[91])]
    )

    plan = plan_minute_backfill(store, manifest)

    assert len(plan.windows) == 1
    assert plan.windows[0].open_dates == tuple(opens[:102])
    assert plan.coverage.baseline.expected_sessions == 180
    assert plan.coverage.entry.expected_sessions == 2
    assert plan.coverage.exit.expected_sessions == 20
    assert plan.coverage.complete_unique_sessions == 0
    assert plan.requested_session_count == 102
    assert [len(task.open_dates) for task in plan.tasks] == [33, 33, 33, 3]
    assert all(task.expected_rows <= 8_000 for task in plan.tasks)
    assert plan.estimate.request_count == 4
    assert plan.estimate.estimated_rows == 102 * 241
    assert plan.estimate.estimated_disk_bytes > 0
    assert plan.estimate.rate_limit_seconds > 0
    assert plan.estimate.total_seconds >= plan.estimate.rate_limit_seconds
    assert plan.estimate.confidence == "low"


def test_only_exact_full_session_counts_as_covered(store: DuckDBStore) -> None:
    from rquant.backfill_manifest import plan_minute_backfill

    opens = _weekday_opens(date(2026, 6, 1), 10)
    _seed_calendar(store, opens)
    manifest = _manifest(
        entries=[("300001.SZ", opens[5])],
        baseline_days=2,
        exit_days=2,
    )
    spec = next(
        value
        for value in DEFAULT_MINUTE_SOURCE_SESSION_SPECS
        if value.source == "tushare" and value.freq == "1min"
    )
    expected_times = spec.expected_times()
    _insert_session(
        store,
        ts_code="300001.SZ",
        trade_date=opens[3],
        times=expected_times,
    )
    _insert_session(
        store,
        ts_code="300001.SZ",
        trade_date=opens[4],
        times=expected_times[:-1],
    )

    plan = plan_minute_backfill(store, manifest)

    assert plan.coverage.baseline.expected_sessions == 2
    assert plan.coverage.baseline.complete_sessions == 1
    assert plan.coverage.baseline.coverage_ratio == pytest.approx(0.5)
    assert plan.coverage.entry.complete_sessions == 0
    assert plan.coverage.exit.complete_sessions == 0
    requested_dates = {
        trade_date for task in plan.tasks for trade_date in task.open_dates
    }
    assert opens[3] not in requested_dates
    assert requested_dates == set(opens[4:8])
    assert plan.requested_session_count == 4
    assert plan.coverage.baseline_gate_passed is False
    assert plan.coverage.entry_exit_gate_passed is False


def test_minute_completion_uses_bounded_exact_target_aggregates(
    store: DuckDBStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.backfill_manifest as manifest_module
    from rquant.backfill_manifest import (
        MergedBackfillWindow,
        _complete_minute_sessions,
    )

    spec = next(
        value
        for value in DEFAULT_MINUTE_SOURCE_SESSION_SPECS
        if value.source == "tushare" and value.freq == "1min"
    )
    first_date = date(2026, 6, 1)
    second_date = date(2026, 6, 2)
    expected_times = spec.expected_times()
    _insert_session(
        store,
        ts_code="300001.SZ",
        trade_date=first_date,
        times=expected_times,
    )
    _insert_session(
        store,
        ts_code="300001.SZ",
        trade_date=second_date,
        times=expected_times[:-1],
    )
    _insert_session(
        store,
        ts_code="300002.SZ",
        trade_date=first_date,
        times=(*expected_times[:-1], time(12, 0)),
    )
    _insert_session(
        store,
        ts_code="300002.SZ",
        trade_date=second_date,
        times=(*expected_times, time(12, 0)),
    )
    store._conn.executemany(
        """
        INSERT INTO minute_bar (
            ts_code, trade_time, freq, open, high, low, close,
            vol, amount, source
        ) VALUES (?, ?, '1min', 10, 10, 10, 10, 100, 1000, 'other')
        """,
        [
            ("300003.SZ", datetime.combine(first_date, minute_time))
            for minute_time in expected_times
        ],
    )
    _insert_session(
        store,
        ts_code="999999.SZ",
        trade_date=first_date,
        times=expected_times,
    )
    windows = (
        MergedBackfillWindow(
            ts_code="300001.SZ",
            start_date=first_date,
            end_date=second_date,
            open_dates=(first_date, second_date),
        ),
        MergedBackfillWindow(
            ts_code="300002.SZ",
            start_date=first_date,
            end_date=second_date,
            open_dates=(first_date, second_date),
        ),
        MergedBackfillWindow(
            ts_code="300003.SZ",
            start_date=first_date,
            end_date=second_date,
            open_dates=(first_date, second_date),
        ),
    )

    class RecordingConnection:
        def __init__(self, connection: object) -> None:
            self.connection = connection
            self.statements: list[str] = []

        def execute(
            self,
            statement: str,
            parameters: list[object] | None = None,
        ) -> object:
            self.statements.append(statement)
            return self.connection.execute(statement, parameters or [])

        def __getattr__(self, name: str) -> object:
            return getattr(self.connection, name)

    recording = RecordingConnection(store._conn)
    monkeypatch.setattr(manifest_module, "_MINUTE_COMPLETION_BATCH_SIZE", 2)
    monkeypatch.setattr(store, "_conn", recording)

    complete = _complete_minute_sessions(store, windows, spec)

    assert complete == {("300001.SZ", first_date)}
    completion_sql = [
        statement.lower()
        for statement in recording.statements
        if "from minute_bar" in statement.lower()
    ]
    assert len(completion_sql) == 3
    assert all("list(" not in statement for statement in completion_sql)
    assert all("values" in statement for statement in completion_sql)


def test_research_lake_coverage_survives_operational_minute_cleanup(
    store: DuckDBStore,
    tmp_path: Path,
) -> None:
    from rquant.backfill_manifest import plan_minute_backfill
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_lake import export_research_dataset

    opens = _weekday_opens(date(2026, 6, 1), 7)
    _seed_calendar(store, opens)
    manifest = _manifest(
        entries=[("300001.SZ", opens[3])],
        baseline_days=1,
        exit_days=1,
    )
    expected_times = next(
        spec
        for spec in DEFAULT_MINUTE_SOURCE_SESSION_SPECS
        if spec.source == "tushare" and spec.freq == "1min"
    ).expected_times()
    _insert_session(
        store,
        ts_code="300001.SZ",
        trade_date=opens[2],
        times=expected_times,
    )
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    lake_root = tmp_path / "lake"
    export_research_dataset(
        store._conn,
        catalog=catalog,
        lake_root=lake_root,
        dataset="minute_bar",
        start_date=opens[2],
        end_date=opens[2],
        code_commit="a" * 40,
    )
    store._conn.execute("DELETE FROM minute_bar")

    plan = plan_minute_backfill(
        store,
        manifest,
        coverage_authority="research_lake",
        research_catalog=catalog,
        research_lake_root=lake_root,
    )

    assert plan.coverage.baseline.complete_sessions == 1
    assert len(plan.minute_coverage_artifacts) == 1
    assert plan.minute_coverage_artifacts[0].dataset_id == "minute_bar"
    assert opens[2] not in {
        trading_date for task in plan.tasks for trading_date in task.open_dates
    }


def test_research_lake_authority_does_not_accept_unpublished_operational_rows(
    store: DuckDBStore,
    tmp_path: Path,
) -> None:
    from rquant.backfill_manifest import plan_minute_backfill
    from rquant.research_catalog import ResearchCatalog

    opens = _weekday_opens(date(2026, 6, 1), 7)
    _seed_calendar(store, opens)
    manifest = _manifest(
        entries=[("300001.SZ", opens[3])],
        baseline_days=1,
        exit_days=1,
    )
    expected_times = next(
        spec
        for spec in DEFAULT_MINUTE_SOURCE_SESSION_SPECS
        if spec.source == "tushare" and spec.freq == "1min"
    ).expected_times()
    _insert_session(
        store,
        ts_code="300001.SZ",
        trade_date=opens[2],
        times=expected_times,
    )

    plan = plan_minute_backfill(
        store,
        manifest,
        coverage_authority="research_lake",
        research_catalog=ResearchCatalog(tmp_path / "research.duckdb"),
        research_lake_root=tmp_path / "lake",
    )

    assert plan.coverage.baseline.complete_sessions == 0
    assert opens[2] in {
        trading_date for task in plan.tasks for trading_date in task.open_dates
    }


def test_known_full_day_suspension_satisfies_coverage_without_task(
    store: DuckDBStore,
) -> None:
    from rquant.backfill_manifest import plan_minute_backfill
    from rquant.suspension import (
        normalize_suspend_d_snapshot,
        persist_suspension_snapshot,
    )

    opens = _weekday_opens(date(2026, 6, 1), 10)
    _seed_calendar(store, opens)
    manifest = _manifest(
        entries=[("300001.SZ", opens[5])],
        baseline_days=2,
        exit_days=2,
    )
    snapshot = normalize_suspend_d_snapshot(
        pd.DataFrame(
            [
                {
                    "ts_code": "300001.SZ",
                    "trade_date": opens[5].strftime("%Y%m%d"),
                    "suspend_timing": "全天",
                    "suspend_type": "S",
                }
            ]
        ),
        trade_date=opens[5],
        queried_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    persist_suspension_snapshot(store, snapshot)

    plan = plan_minute_backfill(store, manifest)

    assert plan.coverage.entry.complete_sessions == 0
    assert plan.coverage.entry.accepted_missing_sessions == 1
    assert plan.coverage.entry.coverage_ratio == 1.0
    assert opens[5] not in {
        trading_date for task in plan.tasks for trading_date in task.open_dates
    }


def test_pre_listing_sessions_are_classified_before_tasks_and_eta(
    store: DuckDBStore,
) -> None:
    from rquant.backfill_manifest import plan_minute_backfill

    opens = _weekday_opens(date(2026, 6, 1), 10)
    _seed_calendar(store, opens)
    store._conn.execute(
        "INSERT INTO stock_basic (ts_code, list_date) VALUES (?, ?)",
        ["300001.SZ", opens[5]],
    )
    manifest = _manifest(
        entries=[("300001.SZ", opens[5])],
        baseline_days=2,
        exit_days=1,
    )

    plan = plan_minute_backfill(store, manifest)

    assert plan.coverage.baseline.accepted_missing_sessions == 2
    assert plan.coverage.baseline.coverage_ratio == 1.0
    assert {
        (row.ts_code, row.trade_date, row.reason)
        for row in plan.unavailable_sessions
    } == {
        ("300001.SZ", opens[3], "not_listed"),
        ("300001.SZ", opens[4], "not_listed"),
    }
    task_dates = {
        trading_date for task in plan.tasks for trading_date in task.open_dates
    }
    assert opens[3] not in task_dates
    assert opens[4] not in task_dates
    assert plan.requested_session_count == 2


def test_calendar_civil_gap_blocks_planning(store: DuckDBStore) -> None:
    from rquant.backfill_manifest import BackfillCalendarError, plan_minute_backfill

    opens = _weekday_opens(date(2026, 6, 1), 10)
    _seed_calendar(store, opens)
    missing_civil_date = opens[3] + timedelta(days=1)
    store._conn.execute(
        "DELETE FROM trade_calendar WHERE exchange = 'SSE' AND cal_date = ?",
        [missing_civil_date],
    )
    manifest = _manifest(
        entries=[("300001.SZ", opens[5])],
        baseline_days=2,
        exit_days=2,
    )

    with pytest.raises(BackfillCalendarError, match="gap"):
        plan_minute_backfill(store, manifest)


def test_calendar_boundary_shortage_is_explicit(store: DuckDBStore) -> None:
    from rquant.backfill_manifest import BackfillCalendarError, plan_minute_backfill

    opens = _weekday_opens(date(2026, 6, 1), 5)
    _seed_calendar(store, opens)
    manifest = _manifest(
        entries=[("300001.SZ", opens[1])],
        baseline_days=2,
        exit_days=2,
    )

    with pytest.raises(BackfillCalendarError, match="prior open sessions"):
        plan_minute_backfill(store, manifest)


def test_empty_eligibility_never_becomes_full_coverage(store: DuckDBStore) -> None:
    from rquant.backfill_manifest import (
        STRATEGY_BACKFILL_SPECS,
        BackfillManifest,
        plan_minute_backfill,
    )

    opens = _weekday_opens(date(2026, 6, 1), 10)
    _seed_calendar(store, opens)
    manifest = BackfillManifest.build(
        spec=STRATEGY_BACKFILL_SPECS["growth_board_surge"],
        start_date=opens[2],
        end_date=opens[3],
        as_of_time=datetime(2026, 7, 15, tzinfo=UTC),
        code_commit="d" * 40,
        eligibilities=(),
    )

    plan = plan_minute_backfill(store, manifest)

    assert plan.coverage.baseline.coverage_ratio == 0.0
    assert plan.coverage.entry_exit_coverage_ratio == 0.0
    assert plan.coverage.baseline_gate_passed is False
    assert plan.coverage.entry_exit_gate_passed is False


def test_manifest_range_outside_authoritative_calendar_fails_even_when_empty(
    store: DuckDBStore,
) -> None:
    from rquant.backfill_manifest import (
        STRATEGY_BACKFILL_SPECS,
        BackfillCalendarError,
        BackfillManifest,
        plan_minute_backfill,
    )

    opens = _weekday_opens(date(2026, 6, 1), 5)
    _seed_calendar(store, opens)
    manifest = BackfillManifest.build(
        spec=STRATEGY_BACKFILL_SPECS["growth_board_surge"],
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 3),
        as_of_time=datetime(2026, 7, 15, tzinfo=UTC),
        code_commit="d" * 40,
        eligibilities=(),
    )

    with pytest.raises(BackfillCalendarError, match="does not cover manifest range"):
        plan_minute_backfill(store, manifest)


def test_plan_rejects_exit_session_after_latest_closed_market_session(
    store: DuckDBStore,
) -> None:
    from rquant.backfill_manifest import (
        BackfillCalendarError,
        BackfillManifest,
        EligibilityRecord,
        StrategyBackfillSpec,
        StrategyWindowRequirement,
        plan_minute_backfill,
    )

    opens = _weekday_opens(date(2026, 6, 1), 36)
    assert opens[-1] == date(2026, 7, 20)
    _seed_calendar(store, opens)
    spec = StrategyBackfillSpec(
        strategy_id="n_shape",
        strategy_version="planner-test-v1",
        eligibility_basis="daily",
        window=StrategyWindowRequirement(
            baseline_trading_days=2,
            entry_trading_days=1,
            exit_trading_days=10,
        ),
    )
    eligibility = EligibilityRecord(
        strategy_id=spec.strategy_id,
        strategy_version=spec.strategy_version,
        ts_code="603416.SH",
        eligibility_date=date(2026, 7, 3),
        entry_date=date(2026, 7, 6),
        decision_at=datetime(2026, 7, 3, 9, tzinfo=UTC),
        variant="pool1",
    )
    manifest = BackfillManifest.build(
        spec=spec,
        start_date=eligibility.eligibility_date,
        end_date=eligibility.eligibility_date,
        as_of_time=datetime(2026, 7, 18, 10, tzinfo=UTC),
        code_commit="e" * 40,
        eligibilities=(eligibility,),
    )

    with pytest.raises(
        BackfillCalendarError,
        match=(
            "required minute session 2026-07-20 is later than "
            "latest closed session 2026-07-17"
        ),
    ):
        plan_minute_backfill(store, manifest)


def test_execution_rejects_persisted_plan_with_unobservable_future_sessions(
    store: DuckDBStore,
) -> None:
    from rquant.backfill_manifest import (
        BackfillCalendarError,
        BackfillManifest,
        plan_minute_backfill,
        validate_executable_backfill_plan,
    )

    opens = _weekday_opens(date(2026, 6, 1), 36)
    _seed_calendar(store, opens)
    safe_manifest = _manifest(
        entries=[("603416.SH", date(2026, 7, 6))],
        baseline_days=2,
        exit_days=10,
        as_of_time=datetime(2026, 7, 21, 10, tzinfo=UTC),
    )
    legacy_plan = plan_minute_backfill(store, safe_manifest)
    unsafe_manifest = BackfillManifest.build(
        spec=safe_manifest.spec,
        start_date=safe_manifest.start_date,
        end_date=safe_manifest.end_date,
        as_of_time=datetime(2026, 7, 18, 10, tzinfo=UTC),
        code_commit=safe_manifest.code_commit,
        eligibilities=safe_manifest.eligibilities,
    )
    legacy_plan = legacy_plan.model_copy(update={"manifest": unsafe_manifest})

    with pytest.raises(
        BackfillCalendarError,
        match=(
            "required minute session 2026-07-20 is later than "
            "latest closed session 2026-07-17"
        ),
    ):
        validate_executable_backfill_plan(store, legacy_plan)


def test_latest_observable_eligibility_date_moves_with_strategy_window(
    store: DuckDBStore,
) -> None:
    from rquant.backfill_manifest import (
        STRATEGY_BACKFILL_SPECS,
        BackfillCalendarError,
        latest_observable_eligibility_date,
        resolve_requested_eligibility_end,
    )

    opens = _weekday_opens(date(2026, 6, 1), 36)
    _seed_calendar(store, opens)
    weekend_as_of = datetime(2026, 7, 18, 10, tzinfo=UTC)

    assert latest_observable_eligibility_date(
        store,
        spec=STRATEGY_BACKFILL_SPECS["n_shape"],
        as_of_time=weekend_as_of,
    ) == date(2026, 7, 2)
    assert latest_observable_eligibility_date(
        store,
        spec=STRATEGY_BACKFILL_SPECS["growth_board_surge"],
        as_of_time=weekend_as_of,
    ) == date(2026, 7, 3)
    assert latest_observable_eligibility_date(
        store,
        spec=STRATEGY_BACKFILL_SPECS["n_shape"],
        as_of_time=datetime(2026, 7, 20, 7, 10, tzinfo=UTC),
    ) == date(2026, 7, 2)
    assert latest_observable_eligibility_date(
        store,
        spec=STRATEGY_BACKFILL_SPECS["n_shape"],
        as_of_time=datetime(2026, 7, 20, 7, 10, 1, tzinfo=UTC),
    ) == date(2026, 7, 3)
    assert resolve_requested_eligibility_end(
        requested_end=None,
        observable_end=date(2026, 7, 2),
        start_date=date(2026, 4, 1),
    ) == date(2026, 7, 2)
    assert resolve_requested_eligibility_end(
        requested_end=date(2026, 7, 1),
        observable_end=date(2026, 7, 2),
        start_date=date(2026, 4, 1),
    ) == date(2026, 7, 1)
    with pytest.raises(
        BackfillCalendarError,
        match="requested eligibility end 2026-07-03 exceeds observable end 2026-07-02",
    ):
        resolve_requested_eligibility_end(
            requested_end=date(2026, 7, 3),
            observable_end=date(2026, 7, 2),
            start_date=date(2026, 4, 1),
        )
