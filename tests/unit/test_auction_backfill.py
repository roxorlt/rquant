"""集合竞价回补测试。"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


class _FakeAuctionAdapter:
    def __init__(self) -> None:
        self.calls: list[date] = []

    def stk_auction(self, trade_date: date) -> pd.DataFrame:
        self.calls.append(trade_date)
        return pd.DataFrame([{
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "auction_type": "open_realtime",
            "price": 9.81,
            "vol": 28355900.0,
            "amount": 278113479.0,
            "turnover_rate": 0.1,
            "volume_ratio": 3.2,
            "source": "tushare",
        }])


class _PartiallyFailingAuctionAdapter(_FakeAuctionAdapter):
    def stk_auction(self, trade_date: date) -> pd.DataFrame:
        self.calls.append(trade_date)
        if trade_date == date(2025, 2, 19):
            raise RuntimeError("rate limit")
        return pd.DataFrame([{
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "auction_type": "open_realtime",
            "price": 9.81,
            "vol": 28355900.0,
            "amount": 278113479.0,
            "turnover_rate": 0.1,
            "volume_ratio": 3.2,
            "source": "tushare",
        }])


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _seed_trading_dates(store: DuckDBStore) -> None:
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2025, 2, 18),
            "open": 9.8,
            "high": 10.1,
            "low": 9.7,
            "close": 10.0,
            "pre_close": 9.8,
            "change": 0.2,
            "pct_chg": 2.04,
            "vol": 1,
            "amount": 1,
        },
        {
            "ts_code": "600000.SH",
            "trade_date": date(2025, 2, 19),
            "open": 10.0,
            "high": 10.3,
            "low": 9.9,
            "close": 10.2,
            "pre_close": 10.0,
            "change": 0.2,
            "pct_chg": 2.0,
            "vol": 1,
            "amount": 1,
        },
    ]))


def test_backfill_stk_auction_fetches_trading_dates_and_stores(
    store: DuckDBStore,
) -> None:
    from rquant.auction_backfill import backfill_stk_auction

    _seed_trading_dates(store)
    adapter = _FakeAuctionAdapter()

    summary = backfill_stk_auction(
        store,
        adapter,
        start_date="2025-02-18",
        end_date="2025-02-19",
    )

    assert summary.trading_dates_count == 2
    assert summary.executed_requests == 2
    assert summary.failed_requests == 0
    assert summary.rows_written == 2
    assert adapter.calls == [date(2025, 2, 18), date(2025, 2, 19)]
    out = store.query_auction_bars(date(2025, 2, 18))
    assert len(out) == 1
    assert out.iloc[0]["ts_code"] == "600000.SH"


def test_backfill_stk_auction_dry_run_does_not_fetch(store: DuckDBStore) -> None:
    from rquant.auction_backfill import backfill_stk_auction

    _seed_trading_dates(store)
    adapter = _FakeAuctionAdapter()

    summary = backfill_stk_auction(
        store,
        adapter,
        start_date="2025-02-18",
        end_date="2025-02-19",
        dry_run=True,
    )

    assert summary.trading_dates_count == 2
    assert summary.planned_requests == 2
    assert summary.executed_requests == 0
    assert summary.rows_written == 0
    assert adapter.calls == []


def test_backfill_stk_auction_continues_after_single_failure(
    store: DuckDBStore,
) -> None:
    from rquant.auction_backfill import backfill_stk_auction

    _seed_trading_dates(store)
    adapter = _PartiallyFailingAuctionAdapter()

    summary = backfill_stk_auction(
        store,
        adapter,
        start_date="2025-02-18",
        end_date="2025-02-19",
    )

    assert summary.executed_requests == 2
    assert summary.failed_requests == 1
    assert summary.rows_written == 1
    assert summary.failed_dates == ["2025-02-19"]


def test_synthesize_open_auction_from_minute_fills_only_missing_rows(
    store: DuckDBStore,
) -> None:
    from rquant.auction_backfill import synthesize_open_auction_from_minute

    trading_date = date(2026, 6, 26)
    store.upsert_auction_bars(pd.DataFrame([{
        "ts_code": "600000.SH",
        "trade_date": trading_date,
        "auction_type": "open_realtime",
        "price": 8.86,
        "vol": 218919.0,
        "amount": 1939622.0,
        "turnover_rate": 0.1,
        "volume_ratio": 0.2,
        "source": "tushare",
    }]))
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 26, 9, 30),
            "freq": "1min",
            "open": 8.86,
            "high": 8.87,
            "low": 8.85,
            "close": 8.86,
            "vol": 1000.0,
            "amount": 8860.0,
            "source": "tushare",
        },
        {
            "ts_code": "000001.SZ",
            "trade_time": datetime(2026, 6, 26, 9, 30),
            "freq": "1min",
            "open": 10.5,
            "high": 10.6,
            "low": 10.4,
            "close": 10.55,
            "vol": 2000.0,
            "amount": 21000.0,
            "source": "tushare",
        },
    ]))

    summary = synthesize_open_auction_from_minute(store, trading_date)

    assert summary.minute_rows_count == 2
    assert summary.existing_primary_count == 1
    assert summary.rows_written == 1
    out = store.query_auction_bars(trading_date)
    fallback = out[out["source"] == "minute_0930_fallback"].iloc[0]
    assert fallback["ts_code"] == "000001.SZ"
    assert fallback["price"] == 10.5
    assert fallback["vol"] == 2000.0
