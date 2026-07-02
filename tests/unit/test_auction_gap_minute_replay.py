"""集合竞价候选 + 分钟 B/S replay 测试。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _seed_base(
    store: DuckDBStore,
    *,
    weak_open: bool = False,
    strong_seal: bool = False,
    next_auction_price: float = 10.85,
) -> None:
    dates = [
        date(2026, 6, 18),
        date(2026, 6, 19),
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
        date(2026, 6, 25),
        date(2026, 6, 26),
    ]
    daily_rows = []
    for trade_date in dates[:5]:
        daily_rows.append({
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "open": 9.8,
            "high": 10.2,
            "low": 9.6,
            "close": 10.0,
            "pre_close": 9.8,
            "change": 0.2,
            "pct_chg": 2.0,
            "vol": 1000.0,
            "amount": 10000.0,
        })
    daily_rows.extend([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 25),
            "open": 10.4,
            "high": 11.0,
            "low": 10.36 if not weak_open else 10.0,
            "close": 11.0,
            "pre_close": 10.0,
            "change": 1.0,
            "pct_chg": 10.0,
            "vol": 5000.0,
            "amount": 55000.0,
        },
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 26),
            "open": next_auction_price,
            "high": 11.05,
            "low": 10.7,
            "close": 10.9,
            "pre_close": 11.0,
            "change": -0.1,
            "pct_chg": -0.91,
            "vol": 5000.0,
            "amount": 54500.0,
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
            "is_limit_up": True,
            "is_limit_down": False,
            "is_first_limit_up": True,
            "is_yiziban": False,
            "consecutive_limit_ups": 1,
            "body_upper": 11.0,
            "body_lower": 10.4,
        },
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 26),
            "is_st": False,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.10,
            "limit_up_price": 12.10,
            "limit_down_price": 9.90,
            "is_limit_up": False,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 10.9,
            "body_lower": 10.9,
        },
    ]))
    store.upsert_auction_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 25),
            "auction_type": "open_realtime",
            "price": 10.4,
            "vol": 18000.0,
            "amount": 187200.0,
            "turnover_rate": 0.1,
            "volume_ratio": 1.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 26),
            "auction_type": "open_realtime",
            "price": next_auction_price,
            "vol": 22000.0,
            "amount": next_auction_price * 22000.0,
            "turnover_rate": 0.12,
            "volume_ratio": 1.1,
            "source": "tushare",
        },
    ]))
    low_0930 = 10.36 if not weak_open else 10.0
    minute_rows = [
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 9, 30),
            "freq": "1min",
            "open": 10.40,
            "high": 10.45,
            "low": low_0930,
            "close": 10.42,
            "vol": 1000,
            "amount": 10420,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 9, 31),
            "freq": "1min",
            "open": 10.42,
            "high": 10.55,
            "low": 10.41,
            "close": 10.52,
            "vol": 1000,
            "amount": 10520,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 9, 32),
            "freq": "1min",
            "open": 10.53,
            "high": 10.80,
            "low": 10.53,
            "close": 10.70,
            "vol": 1000,
            "amount": 10700,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 10, 0),
            "freq": "1min",
            "open": 10.95,
            "high": 11.00,
            "low": 10.95,
            "close": 11.00,
            "vol": 1000,
            "amount": 11000,
            "source": "tushare",
        },
    ]
    if strong_seal:
        minute_rows.extend([
            {
                "ts_code": "600000.SH",
                "trade_time": datetime(2026, 6, 25, 10, 1),
                "freq": "1min",
                "open": 11.00,
                "high": 11.00,
                "low": 11.00,
                "close": 11.00,
                "vol": 1000,
                "amount": 11000,
                "source": "tushare",
            },
            {
                "ts_code": "600000.SH",
                "trade_time": datetime(2026, 6, 25, 10, 2),
                "freq": "1min",
                "open": 11.00,
                "high": 11.00,
                "low": 11.00,
                "close": 11.00,
                "vol": 1000,
                "amount": 11000,
                "source": "tushare",
            },
        ])
    minute_rows.append(
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 26, 9, 30),
            "freq": "1min",
            "open": next_auction_price,
            "high": 10.90,
            "low": 10.75,
            "close": 10.80,
            "vol": 1000,
            "amount": 10800,
            "source": "tushare",
        }
    )
    store.upsert_minute_bars(pd.DataFrame(minute_rows))


def test_auction_gap_minute_replay_waits_for_minute_b_and_uses_next_auction_s(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
    )

    _seed_base(store)

    trades = run_auction_gap_minute_replay(
        store,
        AuctionGapMinuteReplayConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            max_hold_days=1,
        ),
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["entry_time"] == pd.Timestamp("2026-06-25 09:32:00")
    assert row["entry_price"] == pytest.approx(10.53)
    assert row["auction_price"] == pytest.approx(10.4)
    assert row["exit_time"] == pd.Timestamp("2026-06-26 09:30:00")
    assert row["exit_price"] == pytest.approx(10.85)
    assert row["exit_reason"] == "next_auction_weak"
    assert row["b_first_limit_up_time"] == pd.Timestamp("2026-06-25 10:00:00")
    assert row["b_close_at_limit_up"]
    assert row["entry_signal_opening_segment"] == 1
    assert row["entry_signal_opening_segment_amount"] == pytest.approx(20940.0)
    assert row["entry_signal_amount_accel_5m"] is None
    assert row["exit_next_auction_price"] == pytest.approx(10.85)
    assert row["exit_next_auction_gap_pct"] == pytest.approx((10.85 / 11.0 - 1) * 100)
    assert row["ret_pct"] == pytest.approx(round((10.85 / 10.53 - 1) * 100, 4))


def test_auction_gap_minute_replay_does_not_buy_when_open_breaks_auction_support(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
    )

    _seed_base(store, weak_open=True)

    trades = run_auction_gap_minute_replay(
        store,
        AuctionGapMinuteReplayConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            max_hold_days=1,
        ),
    )

    assert trades.empty


def test_auction_gap_minute_replay_tolerates_mild_weak_auction_after_strong_seal(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
    )

    _seed_base(store, strong_seal=True, next_auction_price=10.75)

    trades = run_auction_gap_minute_replay(
        store,
        AuctionGapMinuteReplayConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            max_hold_days=1,
        ),
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["b_limit_up_close_minutes"] == 3
    assert row["exit_reason"] == "time_1d"
