"""Authoritative suspension facts and successful-empty coverage."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

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


def test_normalization_is_conservative_about_full_day_suspension() -> None:
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
        "unknown",
    ]
    assert snapshot.coverage.coverage_state == "complete"
    assert snapshot.coverage.row_count == 3


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
