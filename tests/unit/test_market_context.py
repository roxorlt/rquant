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
    # 只有 1 根历史 K 线，窗口行数不足：温度分子为 0，分母为 3 只有成交标的
    assert sentiment.high_60d_ratio_pct == pytest.approx(0.0)
    assert sentiment.above_ma20_ratio_pct == pytest.approx(0.0)


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


def _daily_row(ts_code: str, trade_date: date, close: float) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "open": close,
        "high": close + 0.1,
        "low": close - 0.1,
        "close": close,
        "pre_close": close,
        "change": 0.0,
        "pct_chg": 0.0,
        "vol": 100.0,
        "amount": close * 100.0,
    }


def _state_row(ts_code: str, trade_date: date) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "is_st": False,
        "is_bj": False,
        "board_type": "main",
        "limit_pct": 0.10,
        "limit_up_price": 99.0,
        "limit_down_price": 1.0,
        "is_limit_up": False,
        "is_limit_down": False,
        "is_first_limit_up": False,
        "is_yiziban": False,
        "consecutive_limit_ups": 0,
        "body_upper": 10.0,
        "body_lower": 10.0,
    }


def _seed_temperature_history(store: DuckDBStore) -> list[date]:
    """UP 一路创新高、DOWN 一路走低，NEW 只上市 30 个交易日。"""
    dates = [ts.date() for ts in pd.bdate_range("2026-03-02", periods=61)]
    rows: list[dict[str, object]] = []
    for idx, trade_date in enumerate(dates):
        rows.append(_daily_row("600001.SH", trade_date, 10.0 + idx * 0.1))
        rows.append(_daily_row("600002.SH", trade_date, 50.0 - idx * 0.1))
    for idx, trade_date in enumerate(dates[-30:]):
        rows.append(_daily_row("600003.SH", trade_date, 5.0 + idx * 0.05))
    store.upsert_daily(pd.DataFrame(rows))
    return dates


def _add_temperature_columns(store: DuckDBStore) -> None:
    # 温度两列的 schema 迁移由另一分支负责，测试里先 ALTER 兜底模拟合并后的表结构
    for column in ("high_60d_ratio_pct", "above_ma20_ratio_pct"):
        store._conn.execute(
            f"ALTER TABLE market_sentiment_daily ADD COLUMN IF NOT EXISTS {column} DOUBLE"
        )


def test_market_temperature_requires_full_window_per_stock(
    store: DuckDBStore,
) -> None:
    from rquant.market_context import build_market_sentiment

    dates = _seed_temperature_history(store)
    store.upsert_state(pd.DataFrame([_state_row("600001.SH", dates[-1])]))

    sentiment = build_market_sentiment(store, dates[-1])

    assert sentiment is not None
    # UP 有 60 日历史且创新高计入分子；NEW 只有 30 日不计入；分母是 3 只有成交标的
    assert sentiment.high_60d_ratio_pct == pytest.approx(100 / 3, abs=1e-3)
    # UP 与 NEW 都站上 MA20，DOWN 在 MA20 下方
    assert sentiment.above_ma20_ratio_pct == pytest.approx(200 / 3, abs=1e-3)


def test_recompute_market_sentiment_range_writes_all_columns(
    store: DuckDBStore,
) -> None:
    from rquant.market_context import recompute_market_sentiment_range

    dates = _seed_temperature_history(store)
    store.upsert_state(pd.DataFrame([
        _state_row("600001.SH", dates[-2]),
        _state_row("600001.SH", dates[-1]),
        _state_row("600002.SH", dates[-1]),
    ]))
    _add_temperature_columns(store)

    written = recompute_market_sentiment_range(
        dates[-2].isoformat(),
        dates[-1].isoformat(),
        store=store,
    )

    assert written == 2
    stored = store._conn.execute(
        """
        SELECT trade_date, stock_count, high_60d_ratio_pct, above_ma20_ratio_pct
        FROM market_sentiment_daily
        ORDER BY trade_date
        """
    ).fetchdf()
    assert len(stored) == 2
    assert stored.iloc[0]["stock_count"] == 1
    assert stored.iloc[1]["stock_count"] == 2
    for _, row in stored.iterrows():
        assert row["high_60d_ratio_pct"] == pytest.approx(100 / 3, abs=1e-3)
        assert row["above_ma20_ratio_pct"] == pytest.approx(200 / 3, abs=1e-3)


def test_sync_market_sentiment_skips_temperature_when_columns_missing(
    store: DuckDBStore,
) -> None:
    from rquant.market_context import sync_market_sentiment

    dates = _seed_temperature_history(store)
    store.upsert_state(pd.DataFrame([_state_row("600001.SH", dates[-1])]))

    # 表结构未迁移（无温度列）时 sync 不应报错，基础列正常落库
    assert sync_market_sentiment(store, dates[-1]) == 1
    stored = store.query_market_sentiment(dates[-1])
    assert stored is not None
    assert stored.iloc[0]["stock_count"] == 1
