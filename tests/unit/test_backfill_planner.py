"""Minute backfill planning, coverage, and ETA contracts."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

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
        as_of_time=datetime(2026, 7, 15, tzinfo=UTC),
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
