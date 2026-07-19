"""Authoritative suspension facts and successful-empty coverage."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from rquant.adapter.tushare import TushareAdapter
from rquant.storage.duckdb import DuckDBStore
from rquant.suspension import (
    backfill_suspension_facts,
    normalize_suspend_d_snapshot,
    persist_suspension_snapshot,
)
from rquant.trade_calendar import TradeCalendarDay

QUERIED_AT = datetime(2026, 7, 15, 8, tzinfo=UTC)


def test_tushare_adapter_requests_full_market_suspend_snapshot() -> None:
    calls: list[dict[str, str]] = []

    def suspend_d(**kwargs: str) -> pd.DataFrame:
        calls.append(kwargs)
        return pd.DataFrame(
            [
                {
                    "ts_code": "300001.SZ",
                    "trade_date": "20260714",
                    "suspend_timing": "全天",
                    "suspend_type": "S",
                }
            ]
        )

    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = SimpleNamespace(suspend_d=suspend_d)
    adapter._primary_token = "primary"
    adapter._backup_token = ""
    adapter._using_backup = False
    frame = adapter.suspend_d_raw(date(2026, 7, 14))

    assert calls == [
        {
            "trade_date": "20260714",
            "fields": "ts_code,trade_date,suspend_timing,suspend_type",
        }
    ]
    assert frame.to_dict("records")[0]["suspend_type"] == "S"


def test_normalization_treats_missing_suspend_timing_as_full_day() -> None:
    frame = pd.DataFrame(
        [
            {
                "ts_code": "300001.SZ",
                "trade_date": "20260714",
                "suspend_timing": "全天",
                "suspend_type": "S",
            },
            {
                "ts_code": "300002.SZ",
                "trade_date": "20260714",
                "suspend_timing": "09:30-10:30",
                "suspend_type": "S",
            },
            {
                "ts_code": "300003.SZ",
                "trade_date": "20260714",
                "suspend_timing": None,
                "suspend_type": "S",
            },
            {
                "ts_code": "300004.SZ",
                "trade_date": "20260714",
                "suspend_timing": None,
                "suspend_type": "R",
            },
        ]
    )

    snapshot = normalize_suspend_d_snapshot(
        frame,
        trade_date=date(2026, 7, 14),
        queried_at=QUERIED_AT,
    )

    assert [event.session_scope for event in snapshot.events] == [
        "full_day",
        "partial",
        "full_day",
        "unknown",
    ]
    assert snapshot.coverage.coverage_state == "complete"
    assert snapshot.coverage.row_count == 4


def test_successful_empty_snapshot_is_persisted_but_not_a_suspension(
    tmp_path: Path,
) -> None:
    snapshot = normalize_suspend_d_snapshot(
        pd.DataFrame(),
        trade_date=date(2026, 7, 14),
        queried_at=QUERIED_AT,
    )
    with DuckDBStore(tmp_path / "suspend.duckdb") as store:
        persist_suspension_snapshot(store, snapshot)

        coverage = store._conn.execute(  # noqa: SLF001
            "SELECT coverage_state, row_count FROM stock_suspend_coverage"
        ).fetchone()
        known = store.known_full_day_suspensions(
            ("300001.SZ",),
            date(2026, 7, 14),
            date(2026, 7, 14),
        )

    assert coverage == ("complete", 0)
    assert known == set()


def test_resume_or_ambiguous_events_do_not_allow_missing_minutes(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        [
            {
                "ts_code": "300001.SZ",
                "trade_date": "20260714",
                "suspend_timing": "全天",
                "suspend_type": "S",
            },
            {
                "ts_code": "300001.SZ",
                "trade_date": "20260714",
                "suspend_timing": "10:30",
                "suspend_type": "R",
            },
            {
                "ts_code": "300002.SZ",
                "trade_date": "20260714",
                "suspend_timing": "全天",
                "suspend_type": "S",
            },
        ]
    )
    snapshot = normalize_suspend_d_snapshot(
        frame,
        trade_date=date(2026, 7, 14),
        queried_at=QUERIED_AT,
    )
    with DuckDBStore(tmp_path / "suspend.duckdb") as store:
        persist_suspension_snapshot(store, snapshot)
        known = store.known_full_day_suspensions(
            ("300001.SZ", "300002.SZ"),
            date(2026, 7, 14),
            date(2026, 7, 14),
        )

    assert known == {("300002.SZ", date(2026, 7, 14))}


def test_positive_trading_evidence_blocks_full_day_missing_exemption(
    tmp_path: Path,
) -> None:
    trading_date = date(2026, 7, 14)
    frame = pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "trade_date": "20260714",
                "suspend_timing": None,
                "suspend_type": "S",
            }
            for ts_code in (
                "300001.SZ",
                "300002.SZ",
                "300003.SZ",
                "300004.SZ",
                "300005.SZ",
                "300006.SZ",
            )
        ]
        + [
            {
                "ts_code": "300005.SZ",
                "trade_date": "20260714",
                "suspend_timing": None,
                "suspend_type": "R",
            },
            {
                "ts_code": "300006.SZ",
                "trade_date": "20260714",
                "suspend_timing": "09:30-10:30",
                "suspend_type": "S",
            },
        ]
    )
    snapshot = normalize_suspend_d_snapshot(
        frame,
        trade_date=trading_date,
        queried_at=QUERIED_AT,
    )
    with DuckDBStore(tmp_path / "suspend-evidence.duckdb") as store:
        persist_suspension_snapshot(store, snapshot)
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO daily_bar
            (ts_code, trade_date, close, vol, amount)
            VALUES ('300002.SZ', ?, 10.0, 100.0, 1000.0)
            """,
            [trading_date],
        )
        store._conn.executemany(  # noqa: SLF001
            """
            INSERT INTO minute_bar
            (ts_code, trade_time, freq, close, vol, amount, source)
            VALUES (?, ?, '1min', 10.0, ?, ?, 'tushare')
            """,
            [
                (
                    "300003.SZ",
                    datetime(2026, 7, 14, 9, 30),
                    100.0,
                    1000.0,
                ),
                (
                    "300004.SZ",
                    datetime(2026, 7, 14, 9, 30),
                    0.0,
                    0.0,
                ),
            ],
        )

        known = store.known_full_day_suspensions(
            tuple(f"30000{index}.SZ" for index in range(1, 7)),
            trading_date,
            trading_date,
        )

    assert known == {
        ("300001.SZ", trading_date),
        ("300004.SZ", trading_date),
    }


def test_backfill_uses_open_calendar_days_and_skips_complete_coverage(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "backfill.duckdb"
    updated_at = datetime(2026, 7, 15, 7, tzinfo=UTC)
    with DuckDBStore(database_path) as store:
        store.upsert_trade_calendar(
            (
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=date(2026, 7, 13),
                    is_open=False,
                    source="test",
                    updated_at=updated_at,
                ),
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=date(2026, 7, 14),
                    is_open=True,
                    source="test",
                    updated_at=updated_at,
                ),
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=date(2026, 7, 15),
                    is_open=True,
                    pretrade_date=date(2026, 7, 14),
                    source="test",
                    updated_at=updated_at,
                ),
            )
        )
        persist_suspension_snapshot(
            store,
            normalize_suspend_d_snapshot(
                pd.DataFrame(),
                trade_date=date(2026, 7, 14),
                queried_at=updated_at,
            ),
        )

    class Adapter:
        calls: list[date] = []

        def suspend_d_raw(self, trade_date: date) -> pd.DataFrame:
            self.calls.append(trade_date)
            with DuckDBStore(database_path, read_only=True):
                pass
            return pd.DataFrame(
                columns=[
                    "ts_code",
                    "trade_date",
                    "suspend_timing",
                    "suspend_type",
                ]
            )

    adapter = Adapter()
    result = backfill_suspension_facts(
        adapter,
        store_factory=lambda: DuckDBStore(database_path),
        start=date(2026, 7, 13),
        end=date(2026, 7, 15),
        queried_at=QUERIED_AT,
        missing_only=True,
        request_interval_seconds=0,
        sleep=lambda _: None,
    )

    assert adapter.calls == [date(2026, 7, 15)]
    assert result.open_date_count == 2
    assert result.requested_date_count == 1
    assert result.persisted_date_count == 1
    with DuckDBStore(database_path, read_only=True) as store:
        rows = store._conn.execute(  # noqa: SLF001
            """
            SELECT trade_date, coverage_state, row_count
            FROM stock_suspend_coverage
            ORDER BY trade_date
            """
        ).fetchall()
    assert rows == [
        (date(2026, 7, 14), "complete", 0),
        (date(2026, 7, 15), "complete", 0),
    ]


def test_suspension_backfill_plan_is_read_only_and_lists_refresh_dates(
    tmp_path: Path,
) -> None:
    import rquant.suspension as suspension_module

    planner = getattr(
        suspension_module,
        "plan_suspension_backfill",
        None,
    )
    assert planner is not None
    database_path = tmp_path / "suspension-plan.duckdb"
    updated_at = datetime(2026, 7, 15, 7, tzinfo=UTC)
    trading_dates = (date(2026, 7, 14), date(2026, 7, 15))
    with DuckDBStore(database_path) as store:
        store.upsert_trade_calendar(
            tuple(
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=trading_date,
                    is_open=True,
                    pretrade_date=(
                        trading_dates[index - 1] if index > 0 else None
                    ),
                    source="test",
                    updated_at=updated_at,
                )
                for index, trading_date in enumerate(trading_dates)
            )
        )
        persist_suspension_snapshot(
            store,
            normalize_suspend_d_snapshot(
                pd.DataFrame(),
                trade_date=trading_dates[0],
                queried_at=updated_at,
            ),
        )

    missing_plan = planner(
        store_factory=lambda: DuckDBStore(database_path, read_only=True),
        start=trading_dates[0],
        end=trading_dates[-1],
        missing_only=True,
    )
    refresh_plan = planner(
        store_factory=lambda: DuckDBStore(database_path, read_only=True),
        start=trading_dates[0],
        end=trading_dates[-1],
        missing_only=False,
    )

    assert missing_plan.open_dates == trading_dates
    assert missing_plan.requested_dates == (trading_dates[1],)
    assert refresh_plan.requested_dates == trading_dates


def test_backfill_rolls_back_every_snapshot_when_batch_persistence_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import rquant.suspension as suspension_module

    database_path = tmp_path / "atomic-backfill.duckdb"
    trading_dates = (date(2026, 7, 14), date(2026, 7, 15))
    old_queried_at = datetime(2026, 7, 15, 7, tzinfo=UTC)
    with DuckDBStore(database_path) as store:
        store.upsert_trade_calendar(
            tuple(
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=trading_date,
                    is_open=True,
                    pretrade_date=(
                        trading_dates[index - 1] if index > 0 else None
                    ),
                    source="test",
                    updated_at=old_queried_at,
                )
                for index, trading_date in enumerate(trading_dates)
            )
        )
        for trading_date in trading_dates:
            persist_suspension_snapshot(
                store,
                normalize_suspend_d_snapshot(
                    pd.DataFrame(),
                    trade_date=trading_date,
                    queried_at=old_queried_at,
                ),
            )

    class Adapter:
        def suspend_d_raw(self, trade_date: date) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "ts_code": "300001.SZ",
                        "trade_date": trade_date.strftime("%Y%m%d"),
                        "suspend_timing": None,
                        "suspend_type": "S",
                    }
                ]
            )

    original_persist = suspension_module.persist_suspension_snapshot
    persisted = 0

    def fail_before_second_snapshot(
        store: DuckDBStore,
        snapshot,
        *,
        transaction_mode: str = "managed",
    ) -> None:
        nonlocal persisted
        if persisted == 1:
            raise RuntimeError("simulated second snapshot failure")
        original_persist(
            store,
            snapshot,
            transaction_mode=transaction_mode,
        )
        persisted += 1

    monkeypatch.setattr(
        suspension_module,
        "persist_suspension_snapshot",
        fail_before_second_snapshot,
    )

    with pytest.raises(RuntimeError, match="simulated second snapshot failure"):
        backfill_suspension_facts(
            Adapter(),
            store_factory=lambda: DuckDBStore(database_path),
            start=trading_dates[0],
            end=trading_dates[-1],
            queried_at=QUERIED_AT,
            missing_only=False,
            request_interval_seconds=0,
            sleep=lambda _: None,
        )

    with DuckDBStore(database_path, read_only=True) as store:
        coverage = store._conn.execute(  # noqa: SLF001
            """
            SELECT trade_date, row_count, queried_at
            FROM stock_suspend_coverage
            ORDER BY trade_date
            """
        ).fetchall()
        event_count = store._conn.execute(  # noqa: SLF001
            "SELECT count(*) FROM stock_suspend_event"
        ).fetchone()[0]

    assert [(row[0], row[1]) for row in coverage] == [
        (trading_dates[0], 0),
        (trading_dates[1], 0),
    ]
    assert all(row[2].astimezone(UTC) == old_queried_at for row in coverage)
    assert event_count == 0
