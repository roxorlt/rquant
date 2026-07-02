"""Pool1 历史分钟回补测试。"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


class _FakeIntradayAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, datetime, datetime]] = []

    def stk_mins(
        self,
        ts_code: str,
        freq: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        self.calls.append((ts_code, freq, start, end))
        return pd.DataFrame([{
            "ts_code": ts_code,
            "trade_time": start,
            "freq": freq,
            "open": 10.0,
            "high": 10.2,
            "low": 9.9,
            "close": 10.1,
            "vol": 10000.0,
            "amount": 101000.0,
            "source": "tushare",
        }])


class _PartiallyFailingIntradayAdapter(_FakeIntradayAdapter):
    def stk_mins(
        self,
        ts_code: str,
        freq: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        self.calls.append((ts_code, freq, start, end))
        if ts_code == "600000.SH":
            raise RuntimeError("rate limit")
        return pd.DataFrame([{
            "ts_code": ts_code,
            "trade_time": start,
            "freq": freq,
            "open": 10.0,
            "high": 10.2,
            "low": 9.9,
            "close": 10.1,
            "vol": 10000.0,
            "amount": 101000.0,
            "source": "tushare",
        }])


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _seed_pool1(store: DuckDBStore) -> None:
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 22),
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
            "trade_date": date(2026, 6, 23),
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
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 24),
            "open": 10.2,
            "high": 10.8,
            "low": 10.1,
            "close": 10.7,
            "pre_close": 10.2,
            "change": 0.5,
            "pct_chg": 4.9,
            "vol": 1,
            "amount": 1,
        },
    ]))
    store.upsert_screen_result(pd.DataFrame([{
        "trade_date": date(2026, 6, 24),
        "preset_name": "n-shape-pool1",
        "ts_code": "600000.SH",
        "name": "浦发银行",
        "close": 10.7,
        "pct_chg": 4.9,
        "extra": None,
    }]))


def _seed_auction_gap_candidate(store: DuckDBStore) -> None:
    daily_rows = []
    for trade_date in [
        date(2026, 6, 18),
        date(2026, 6, 19),
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
    ]:
        daily_rows.append({
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "open": 9.8,
            "high": 10.2,
            "low": 9.7,
            "close": 10.0,
            "pre_close": 9.8,
            "change": 0.2,
            "pct_chg": 2.04,
            "vol": 1000.0,
            "amount": 10000.0,
        })
    daily_rows.extend([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 25),
            "open": 10.4,
            "high": 11.0,
            "low": 10.2,
            "close": 10.8,
            "pre_close": 10.0,
            "change": 0.8,
            "pct_chg": 8.0,
            "vol": 2000.0,
            "amount": 21600.0,
        },
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 26),
            "open": 10.7,
            "high": 10.9,
            "low": 10.5,
            "close": 10.6,
            "pre_close": 10.8,
            "change": -0.2,
            "pct_chg": -1.85,
            "vol": 2000.0,
            "amount": 21200.0,
        },
    ])
    store.upsert_daily(pd.DataFrame(daily_rows))
    store.upsert_stock_basic(pd.DataFrame([{
        "ts_code": "600000.SH",
        "symbol": "600000",
        "name": "浦发银行",
        "area": "上海",
        "industry": "银行",
        "list_date": "19991110",
        "market": "主板",
    }]))
    store.upsert_state(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 25),
            "is_st": False,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.10,
            "limit_up_price": 11.00,
            "limit_down_price": 9.00,
            "is_limit_up": False,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 10.8,
            "body_lower": 10.4,
        },
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 26),
            "is_st": False,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.10,
            "limit_up_price": 11.88,
            "limit_down_price": 9.72,
            "is_limit_up": False,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 10.6,
            "body_lower": 10.6,
        },
    ]))
    store.upsert_auction_bars(pd.DataFrame([{
        "ts_code": "600000.SH",
        "trade_date": date(2026, 6, 25),
        "auction_type": "open_realtime",
        "price": 10.4,
        "vol": 18000.0,
        "amount": 187200.0,
        "turnover_rate": 0.1,
        "volume_ratio": 1.0,
        "source": "tushare",
    }]))


def test_backfill_pool1_minute_context_fetches_and_stores(store: DuckDBStore) -> None:
    from rquant.intraday_backfill import backfill_pool1_minute_context

    _seed_pool1(store)
    adapter = _FakeIntradayAdapter()

    summary = backfill_pool1_minute_context(
        store,
        adapter,
        screen_date="2026-06-24",
        lookback_days=3,
        freq="1min",
    )

    assert summary.codes_count == 1
    assert summary.planned_requests == 1
    assert summary.executed_requests == 1
    assert summary.rows_written == 1
    assert adapter.calls == [(
        "600000.SH",
        "1min",
        datetime(2026, 6, 22, 9, 30),
        datetime(2026, 6, 24, 15, 0),
    )]
    stored = store.query_minute_bars(
        "600000.SH",
        datetime(2026, 6, 22, 9, 30),
        datetime(2026, 6, 24, 15, 0),
    )
    assert len(stored) == 1


def test_backfill_pool1_dry_run_does_not_fetch(store: DuckDBStore) -> None:
    from rquant.intraday_backfill import backfill_pool1_minute_context

    _seed_pool1(store)
    adapter = _FakeIntradayAdapter()

    summary = backfill_pool1_minute_context(
        store,
        adapter,
        screen_date="2026-06-24",
        lookback_days=3,
        freq="1min",
        dry_run=True,
    )

    assert summary.codes_count == 1
    assert summary.planned_requests == 1
    assert summary.executed_requests == 0
    assert summary.rows_written == 0
    assert adapter.calls == []


def test_backfill_pool1_minute_context_continues_after_single_failure(
    store: DuckDBStore,
) -> None:
    from rquant.intraday_backfill import backfill_pool1_minute_context

    _seed_pool1(store)
    store.upsert_screen_result(pd.DataFrame([{
        "trade_date": date(2026, 6, 24),
        "preset_name": "n-shape-pool1",
        "ts_code": "000001.SZ",
        "name": "平安银行",
        "close": 10.7,
        "pct_chg": 4.9,
        "extra": None,
    }]))
    adapter = _PartiallyFailingIntradayAdapter()

    summary = backfill_pool1_minute_context(
        store,
        adapter,
        screen_date="2026-06-24",
        lookback_days=3,
        freq="1min",
    )

    assert summary.codes_count == 2
    assert summary.planned_requests == 2
    assert summary.executed_requests == 2
    assert summary.failed_requests == 1
    assert summary.rows_written == 1
    assert [call[0] for call in adapter.calls] == ["000001.SZ", "600000.SH"]


def test_backfill_auction_gap_minute_replay_window_fetches_signal_to_exit_window(
    store: DuckDBStore,
) -> None:
    from rquant.intraday_backfill import backfill_auction_gap_minute_replay_window

    _seed_auction_gap_candidate(store)
    adapter = _FakeIntradayAdapter()

    summary = backfill_auction_gap_minute_replay_window(
        store,
        adapter,
        start_date="2026-06-25",
        end_date="2026-06-25",
        max_hold_days=1,
        freq="1min",
    )

    assert summary.candidates_count == 1
    assert summary.planned_requests == 1
    assert summary.executed_requests == 1
    assert summary.rows_written == 1
    assert adapter.calls == [(
        "600000.SH",
        "1min",
        datetime(2026, 6, 25, 9, 30),
        datetime(2026, 6, 26, 15, 0),
    )]


def test_backfill_minute_replay_window_fetches_buy_to_exit_window(
    store: DuckDBStore,
) -> None:
    from rquant.intraday_backfill import backfill_minute_replay_window

    _seed_pool1(store)
    store.upsert_screen_result(pd.DataFrame([{
        "trade_date": date(2026, 6, 22),
        "preset_name": "n-shape-pool1",
        "ts_code": "600000.SH",
        "name": "浦发银行",
        "close": 10.0,
        "pct_chg": 2.04,
        "extra": None,
    }]))
    adapter = _FakeIntradayAdapter()

    summary = backfill_minute_replay_window(
        store,
        adapter,
        start_date="2026-06-22",
        end_date="2026-06-22",
        max_hold_days=1,
        freq="1min",
    )

    assert summary.candidates_count == 1
    assert summary.planned_requests == 1
    assert summary.executed_requests == 1
    assert summary.rows_written == 1
    assert adapter.calls == [(
        "600000.SH",
        "1min",
        datetime(2026, 6, 23, 9, 30),
        datetime(2026, 6, 24, 15, 0),
    )]


def test_backfill_minute_replay_window_dry_run_does_not_fetch(
    store: DuckDBStore,
) -> None:
    from rquant.intraday_backfill import backfill_minute_replay_window

    _seed_pool1(store)
    store.upsert_screen_result(pd.DataFrame([{
        "trade_date": date(2026, 6, 22),
        "preset_name": "n-shape-pool1",
        "ts_code": "600000.SH",
        "name": "浦发银行",
        "close": 10.0,
        "pct_chg": 2.04,
        "extra": None,
    }]))
    adapter = _FakeIntradayAdapter()

    summary = backfill_minute_replay_window(
        store,
        adapter,
        start_date="2026-06-22",
        end_date="2026-06-22",
        max_hold_days=1,
        freq="1min",
        dry_run=True,
    )

    assert summary.candidates_count == 1
    assert summary.planned_requests == 1
    assert summary.executed_requests == 0
    assert summary.rows_written == 0
    assert adapter.calls == []


def test_backfill_minute_replay_window_continues_after_single_failure(
    store: DuckDBStore,
) -> None:
    from rquant.intraday_backfill import backfill_minute_replay_window

    _seed_pool1(store)
    store.upsert_screen_result(pd.DataFrame([
        {
            "trade_date": date(2026, 6, 22),
            "preset_name": "n-shape-pool1",
            "ts_code": "600000.SH",
            "name": "浦发银行",
            "close": 10.0,
            "pct_chg": 2.04,
            "extra": None,
        },
        {
            "trade_date": date(2026, 6, 22),
            "preset_name": "n-shape-pool1",
            "ts_code": "000001.SZ",
            "name": "平安银行",
            "close": 10.0,
            "pct_chg": 2.04,
            "extra": None,
        },
    ]))
    adapter = _PartiallyFailingIntradayAdapter()

    summary = backfill_minute_replay_window(
        store,
        adapter,
        start_date="2026-06-22",
        end_date="2026-06-22",
        max_hold_days=1,
        freq="1min",
    )

    assert summary.candidates_count == 2
    assert summary.planned_requests == 2
    assert summary.executed_requests == 2
    assert summary.failed_requests == 1
    assert summary.rows_written == 1
    assert [call[0] for call in adapter.calls] == ["000001.SZ", "600000.SH"]
