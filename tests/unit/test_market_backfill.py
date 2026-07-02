"""全市场日线历史回补测试。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from rquant.market_backfill import backfill_market_daily, recompute_daily_state
from rquant.storage.duckdb import DuckDBStore


class _FakeMarketAdapter:
    def __init__(self) -> None:
        self.trade_cal_calls: list[tuple[date, date]] = []
        self.daily_calls: list[date] = []
        self.basic_calls: list[date] = []
        self.factor_calls: list[date] = []

    def trade_cal(self, start: date, end: date) -> list[date]:
        self.trade_cal_calls.append((start, end))
        all_dates = [date(2020, 1, 2), date(2020, 1, 3)]
        return [d for d in all_dates if start <= d <= end]

    def daily_by_date(self, trade_date: date) -> pd.DataFrame:
        self.daily_calls.append(trade_date)
        return pd.DataFrame([{
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "open": 10.0,
            "high": 10.5,
            "low": 9.9,
            "close": 10.2,
            "pre_close": 10.0,
            "change": 0.2,
            "pct_chg": 2.0,
            "vol": 1000.0,
            "amount": 10200.0,
        }])

    def daily_basic_by_date(self, trade_date: date) -> pd.DataFrame:
        self.basic_calls.append(trade_date)
        return pd.DataFrame([{
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "turnover_rate": 1.0,
            "volume_ratio": 1.2,
            "total_mv": 100000.0,
            "circ_mv": 80000.0,
        }])

    def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
        self.factor_calls.append(trade_date)
        return pd.DataFrame([{
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "adj_factor": 1.23,
        }])


class _FailingDayAdapter(_FakeMarketAdapter):
    def daily_by_date(self, trade_date: date) -> pd.DataFrame:
        if trade_date == date(2020, 1, 3):
            self.daily_calls.append(trade_date)
            raise RuntimeError("rate limit")
        return super().daily_by_date(trade_date)


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def test_backfill_writes_three_tables(store: DuckDBStore) -> None:
    adapter = _FakeMarketAdapter()

    summary = backfill_market_daily(
        "2020-01-01", "2020-01-05", store, adapter, api_sleep=0.0
    )

    assert summary["trading_dates_count"] == 2
    assert summary["planned_requests"] == 6
    assert summary["executed_dates"] == 2
    assert summary["failed_dates"] == []
    assert summary["daily_rows"] == 2
    assert summary["daily_basic_rows"] == 2
    assert summary["adj_factor_rows"] == 2
    assert adapter.daily_calls == [date(2020, 1, 2), date(2020, 1, 3)]
    assert store.count_daily("600000.SH") == 2
    assert store.count_daily_basic("600000.SH") == 2
    assert store.count_adj_factor("600000.SH") == 2


def test_backfill_dry_run_only_calls_trade_cal(store: DuckDBStore) -> None:
    adapter = _FakeMarketAdapter()

    summary = backfill_market_daily(
        "2020-01-01", "2020-01-05", store, adapter, dry_run=True
    )

    assert summary["dry_run"] is True
    assert summary["trading_dates_count"] == 2
    assert summary["planned_requests"] == 6
    assert summary["executed_dates"] == 0
    assert adapter.trade_cal_calls == [(date(2020, 1, 1), date(2020, 1, 5))]
    assert adapter.daily_calls == []
    assert adapter.basic_calls == []
    assert adapter.factor_calls == []
    assert store.count_daily() == 0


def test_backfill_isolates_single_day_failure(store: DuckDBStore) -> None:
    adapter = _FailingDayAdapter()

    summary = backfill_market_daily(
        "2020-01-01", "2020-01-05", store, adapter, api_sleep=0.0
    )

    assert summary["executed_dates"] == 2
    assert summary["failed_dates"] == ["2020-01-03"]
    assert summary["daily_rows"] == 1
    assert store.count_daily("600000.SH") == 1


def test_backfill_rejects_reversed_range(store: DuckDBStore) -> None:
    with pytest.raises(ValueError, match="start_date"):
        backfill_market_daily(
            "2020-01-05", "2020-01-01", store, _FakeMarketAdapter()
        )


def _seed_daily_history(store: DuckDBStore) -> None:
    """600000.SH 两日（第二日 10% 涨停）+ 000001.SZ 一日（无涨停）。"""
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH", "trade_date": date(2020, 1, 2),
            "open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0,
            "pre_close": 10.0, "change": 0.0, "pct_chg": 0.0,
            "vol": 1.0, "amount": 1.0,
        },
        {
            "ts_code": "600000.SH", "trade_date": date(2020, 1, 3),
            "open": 10.5, "high": 11.0, "low": 10.4, "close": 11.0,
            "pre_close": 10.0, "change": 1.0, "pct_chg": 10.0,
            "vol": 1.0, "amount": 1.0,
        },
        {
            "ts_code": "000001.SZ", "trade_date": date(2020, 1, 2),
            "open": 5.0, "high": 5.1, "low": 4.9, "close": 5.0,
            "pre_close": 5.0, "change": 0.0, "pct_chg": 0.0,
            "vol": 1.0, "amount": 1.0,
        },
    ]))
    store.upsert_stock_basic(pd.DataFrame([{
        "ts_code": "600000.SH", "symbol": "600000", "name": "浦发银行",
        "area": "上海", "industry": "银行", "list_date": "19991110",
        "market": "主板",
    }]))


def test_recompute_daily_state_full_history(store: DuckDBStore) -> None:
    _seed_daily_history(store)

    total = recompute_daily_state(store)

    assert total == 3
    st = store.get_state("600000.SH")
    assert len(st) == 2
    limit_day = st.iloc[1]
    assert bool(limit_day["is_limit_up"])
    assert bool(limit_day["is_first_limit_up"])
    assert int(limit_day["consecutive_limit_ups"]) == 1
    assert store.count_state("000001.SZ") == 1


def test_recompute_daily_state_with_codes_subset(store: DuckDBStore) -> None:
    _seed_daily_history(store)

    total = recompute_daily_state(store, codes=["000001.SZ"])

    assert total == 1
    assert store.count_state("000001.SZ") == 1
    assert store.count_state("600000.SH") == 0
