"""市场情绪特征聚合测试。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def test_build_market_sentiment_from_daily_state_and_bar(
    store: DuckDBStore,
) -> None:
    from rquant.market_context import build_market_sentiment

    trade_date = date(2026, 6, 24)
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "000001.SZ",
            "trade_date": trade_date,
            "open": 10.0,
            "high": 11.0,
            "low": 10.0,
            "close": 11.0,
            "pre_close": 10.0,
            "change": 1.0,
            "pct_chg": 10.0,
            "vol": 100.0,
            "amount": 1100.0,
        },
        {
            "ts_code": "000002.SZ",
            "trade_date": trade_date,
            "open": 10.0,
            "high": 10.2,
            "low": 9.0,
            "close": 9.0,
            "pre_close": 10.0,
            "change": -1.0,
            "pct_chg": -10.0,
            "vol": 200.0,
            "amount": 1800.0,
        },
        {
            "ts_code": "300001.SZ",
            "trade_date": trade_date,
            "open": 10.0,
            "high": 10.1,
            "low": 9.9,
            "close": 10.0,
            "pre_close": 10.0,
            "change": 0.0,
            "pct_chg": 0.0,
            "vol": 50.0,
            "amount": 500.0,
        },
    ]))
    store.upsert_state(pd.DataFrame([
        {
            "ts_code": "000001.SZ",
            "trade_date": trade_date,
            "is_st": False,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.10,
            "limit_up_price": 11.0,
            "limit_down_price": 9.0,
            "is_limit_up": True,
            "is_limit_down": False,
            "is_first_limit_up": True,
            "is_yiziban": False,
            "consecutive_limit_ups": 1,
            "body_upper": 11.0,
            "body_lower": 10.0,
        },
        {
            "ts_code": "000002.SZ",
            "trade_date": trade_date,
            "is_st": False,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.10,
            "limit_up_price": 11.0,
            "limit_down_price": 9.0,
            "is_limit_up": False,
            "is_limit_down": True,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 10.0,
            "body_lower": 9.0,
        },
        {
            "ts_code": "300001.SZ",
            "trade_date": trade_date,
            "is_st": False,
            "is_bj": False,
            "board_type": "gem",
            "limit_pct": 0.20,
            "limit_up_price": 12.0,
            "limit_down_price": 8.0,
            "is_limit_up": True,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": True,
            "consecutive_limit_ups": 3,
            "body_upper": 10.0,
            "body_lower": 10.0,
        },
    ]))

    sentiment = build_market_sentiment(store, trade_date)

    assert sentiment is not None
    assert sentiment.stock_count == 3
    assert sentiment.up_count == 1
    assert sentiment.down_count == 1
    assert sentiment.flat_count == 1
    assert sentiment.limit_up_count == 2
    assert sentiment.first_limit_up_count == 1
    assert sentiment.limit_down_count == 1
    assert sentiment.yiziban_count == 1
    assert sentiment.max_consecutive_limit_ups == 3
    assert sentiment.high_board_count == 1
    assert sentiment.up_ratio_pct == pytest.approx(33.3333)
    assert sentiment.limit_up_ratio_pct == pytest.approx(66.6667)
    assert sentiment.avg_pct_chg == pytest.approx(0.0)
    assert sentiment.median_pct_chg == pytest.approx(0.0)
    assert sentiment.total_amount == pytest.approx(3400.0)


def test_sync_market_sentiment_upserts_one_row(store: DuckDBStore) -> None:
    from rquant.market_context import sync_market_sentiment

    trade_date = date(2026, 6, 24)
    store.upsert_daily(pd.DataFrame([{
        "ts_code": "000001.SZ",
        "trade_date": trade_date,
        "open": 10.0,
        "high": 11.0,
        "low": 10.0,
        "close": 11.0,
        "pre_close": 10.0,
        "change": 1.0,
        "pct_chg": 10.0,
        "vol": 100.0,
        "amount": 1100.0,
    }]))
    store.upsert_state(pd.DataFrame([{
        "ts_code": "000001.SZ",
        "trade_date": trade_date,
        "is_st": False,
        "is_bj": False,
        "board_type": "main",
        "limit_pct": 0.10,
        "limit_up_price": 11.0,
        "limit_down_price": 9.0,
        "is_limit_up": True,
        "is_limit_down": False,
        "is_first_limit_up": True,
        "is_yiziban": False,
        "consecutive_limit_ups": 1,
        "body_upper": 11.0,
        "body_lower": 10.0,
    }]))

    assert sync_market_sentiment(store, trade_date) == 1

    stored = store.query_market_sentiment(trade_date)
    assert stored is not None
    assert stored.iloc[0]["limit_up_count"] == 1
