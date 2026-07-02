"""集合竞价跳空策略回测测试。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _seed_auction_gap_case(store: DuckDBStore) -> None:
    daily_rows = []
    dates = [
        date(2026, 6, 18),
        date(2026, 6, 19),
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
        date(2026, 6, 25),
        date(2026, 6, 26),
    ]
    for i, trade_date in enumerate(dates):
        daily_rows.append({
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "open": 10.0 + i * 0.1,
            "high": 10.5 + i * 0.1,
            "low": 9.8 + i * 0.1,
            "close": 10.0 + i * 0.1,
            "pre_close": 9.9 + i * 0.1,
            "change": 0.1,
            "pct_chg": 1.0,
            "vol": 1000.0,
            "amount": 10000.0,
        })
        daily_rows.append({
            "ts_code": "000001.SZ",
            "trade_date": trade_date,
            "open": 20.0,
            "high": 20.5,
            "low": 19.8,
            "close": 20.0,
            "pre_close": 19.9,
            "change": 0.1,
            "pct_chg": 0.5,
            "vol": 1000.0,
            "amount": 20000.0,
        })
    store.upsert_daily(pd.DataFrame(daily_rows))
    store.upsert_stock_basic(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "symbol": "600000",
            "name": "浦发银行",
            "area": "上海",
            "industry": "银行",
            "list_date": "19991110",
            "market": "主板",
        },
        {
            "ts_code": "000001.SZ",
            "symbol": "000001",
            "name": "*ST样本",
            "area": "深圳",
            "industry": "测试",
            "list_date": "19910403",
            "market": "主板",
        },
    ]))
    store.upsert_state(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 25),
            "is_st": False,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.10,
            "limit_up_price": 11.55,
            "limit_down_price": 9.45,
            "is_limit_up": True,
            "is_limit_down": False,
            "is_first_limit_up": True,
            "is_yiziban": False,
            "consecutive_limit_ups": 1,
            "body_upper": 10.5,
            "body_lower": 10.5,
        },
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 26),
            "is_st": False,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.10,
            "limit_up_price": 11.66,
            "limit_down_price": 9.54,
            "is_limit_up": False,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 10.6,
            "body_lower": 10.6,
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2026, 6, 25),
            "is_st": True,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.05,
            "limit_up_price": 21.00,
            "limit_down_price": 19.00,
            "is_limit_up": False,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 20.0,
            "body_lower": 20.0,
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2026, 6, 26),
            "is_st": True,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.05,
            "limit_up_price": 21.00,
            "limit_down_price": 19.00,
            "is_limit_up": False,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 20.0,
            "body_lower": 20.0,
        },
    ]))
    store.upsert_auction_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 25),
            "auction_type": "open_realtime",
            "price": 10.7,
            "vol": 18000.0,
            "amount": 192600.0,
            "turnover_rate": 0.1,
            "volume_ratio": 1.0,
            "source": "tushare",
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": date(2026, 6, 25),
            "auction_type": "open_realtime",
            "price": 20.5,
            "vol": 18000.0,
            "amount": 369000.0,
            "turnover_rate": 0.1,
            "volume_ratio": 1.0,
            "source": "tushare",
        },
    ]))


def test_auction_gap_replay_uses_share_volume_and_next_open_exit(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="case_insensitive",
        ),
    )

    assert trades["ts_code"].tolist() == ["600000.SH"]
    row = trades.iloc[0]
    assert row["auction_vol_ratio_5d"] == pytest.approx(0.18)
    assert bool(row["hit_limit_up_today"])
    assert row["next_open_ret_pct"] == pytest.approx((10.6 / 10.7 - 1) * 100)


def test_auction_gap_replay_can_match_literal_lower_st_filter(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="literal_lower",
        ),
    )

    assert trades["ts_code"].tolist() == ["000001.SZ", "600000.SH"]


def test_auction_gap_replay_prefers_tushare_auction_over_minute_fallback(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)
    store.upsert_auction_bars(pd.DataFrame([{
        "ts_code": "600000.SH",
        "trade_date": date(2026, 6, 25),
        "auction_type": "open_realtime",
        "price": 10.8,
        "vol": 20000.0,
        "amount": 216000.0,
        "turnover_rate": None,
        "volume_ratio": None,
        "source": "minute_0930_fallback",
    }]))

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="case_insensitive",
        ),
    )

    assert trades["ts_code"].tolist() == ["600000.SH"]
    assert trades.iloc[0]["entry_price"] == pytest.approx(10.7)


def test_auction_gap_replay_strict_high_gap_mode_requires_gap_above_prior_high(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            gap_mode="strict_high",
            st_filter="case_insensitive",
        ),
    )

    assert trades.empty


def test_auction_gap_replay_can_generate_live_candidate_without_signal_day_daily(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    _seed_auction_gap_case(store)
    store._conn.execute(
        "DELETE FROM daily_bar WHERE trade_date = DATE '2026-06-25'"
    )
    store._conn.execute(
        "DELETE FROM daily_state WHERE trade_date = DATE '2026-06-25'"
    )

    trades = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date="2026-06-25",
            end_date="2026-06-25",
            st_filter="case_insensitive",
            require_next_day=False,
        ),
    )

    assert trades["ts_code"].tolist() == ["600000.SH"]
    row = trades.iloc[0]
    assert row["prev_trade_date"] == pd.Timestamp("2026-06-24")
    assert row["pre_close"] == pytest.approx(10.4)
    assert row["auction_vol_ratio_5d"] == pytest.approx(0.18)
    assert row["limit_up_price"] == pytest.approx(11.44)
    assert pd.isna(row["day_high"])
    assert pd.isna(row["next_open"])
