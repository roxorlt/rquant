"""股票候选特征层测试。"""

from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def test_build_daily_stock_features_price_position_and_accumulation(
    store: DuckDBStore,
) -> None:
    from rquant.stock_features import build_daily_stock_features

    rows = []
    closes = [10.0, 10.4, 10.3, 10.8, 11.2, 12.0]
    amounts = [1000.0, 1800.0, 900.0, 2200.0, 1300.0, 3000.0]
    for idx, close in enumerate(closes, start=19):
        rows.append({
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, idx),
            "open": close - 0.2,
            "high": close + 0.3,
            "low": close - 0.4,
            "close": close,
            "pre_close": closes[idx - 20] if idx > 19 else 9.8,
            "change": 0.0,
            "pct_chg": (close / (closes[idx - 20] if idx > 19 else 9.8) - 1) * 100,
            "vol": amounts[idx - 19] / close,
            "amount": amounts[idx - 19],
        })
    store.upsert_daily(pd.DataFrame(rows))

    features = build_daily_stock_features(
        store,
        "600000.SH",
        date(2026, 6, 24),
        price_lookbacks=(5,),
        accumulation_lookback=5,
    )

    assert features["price_position_5d_pct"] == pytest.approx(100.0)
    assert features["price_rank_5d_pct"] == pytest.approx(100.0)
    assert features["distance_to_high_5d_pct"] == pytest.approx(2.5)
    assert features["accum_obv_change_5d_pct"] > 0
    assert features["accum_ad_flow_5d_pct"] > 0
    assert features["accum_up_down_amount_ratio_5d"] > 1
    assert features["accum_close_position_avg_5d_pct"] > 50
    # 只有 6 根日线：趋势因子数据不足返回 None，不报错
    assert features["ma_alignment"] is None
    assert features["price_percentile_250d"] is None


def _seed_trend_history(
    store: DuckDBStore,
    ts_code: str,
    closes: list[float],
) -> date:
    dates = [ts.date() for ts in pd.bdate_range("2025-06-02", periods=len(closes))]
    rows = [
        {
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
        for trade_date, close in zip(dates, closes, strict=True)
    ]
    store.upsert_daily(pd.DataFrame(rows))
    return dates[-1]


def test_trend_features_ma_alignment_and_price_percentile(
    store: DuckDBStore,
) -> None:
    from rquant.stock_features import build_daily_stock_features

    up_closes = [10.0 + idx * 0.05 for idx in range(260)]
    down_closes = [30.0 - idx * 0.05 for idx in range(260)]
    reference = _seed_trend_history(store, "600001.SH", up_closes)
    _seed_trend_history(store, "600002.SH", down_closes)

    up = build_daily_stock_features(store, "600001.SH", reference)
    down = build_daily_stock_features(store, "600002.SH", reference)

    # 单调上涨：MA5 > MA10 > MA20 > MA60，收盘价是 250 日最高
    assert up["ma_alignment"] == 1
    assert up["price_percentile_250d"] == pytest.approx(1.0)
    # 单调下跌：均线空头排列，收盘价是 250 日最低（百分位 = 1/250）
    assert down["ma_alignment"] == 0
    assert down["price_percentile_250d"] == pytest.approx(1 / 250, abs=1e-4)


def test_trend_features_partial_history_degrades_gracefully(
    store: DuckDBStore,
) -> None:
    from rquant.stock_features import build_daily_stock_features

    reference = _seed_trend_history(
        store, "600003.SH", [10.0 + idx * 0.05 for idx in range(100)]
    )
    short_reference = _seed_trend_history(
        store, "600004.SH", [10.0 + idx * 0.05 for idx in range(30)]
    )

    features = build_daily_stock_features(store, "600003.SH", reference)
    short = build_daily_stock_features(store, "600004.SH", short_reference)

    # 100 日够算 MA60 但不足 250 日百分位
    assert features["ma_alignment"] == 1
    assert features["price_percentile_250d"] is None
    # 30 日连 MA60 都不够
    assert short["ma_alignment"] is None
    assert short["price_percentile_250d"] is None


def test_build_intraday_relative_volume_uses_same_clock_history_only(
    store: DuckDBStore,
) -> None:
    from rquant.stock_features import build_intraday_relative_volume_features

    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 23, 9, 30),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 100.0,
            "amount": 1000.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 23, 9, 31),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 100.0,
            "amount": 1100.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 23, 9, 32),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 100.0,
            "amount": 1200.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 24, 9, 30),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 100.0,
            "amount": 2000.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 24, 9, 31),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 100.0,
            "amount": 2100.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 24, 9, 32),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 100.0,
            "amount": 2200.0,
            "source": "tushare",
        },
    ]))

    features = build_intraday_relative_volume_features(
        store,
        "600000.SH",
        datetime(2026, 6, 25, 9, 32),
        current_minute_amount=6000.0,
        current_cum_amount=12000.0,
        lookback_days=20,
    )

    assert features["hist_same_minute_amount_median_20d"] == pytest.approx(1700.0)
    assert features["signal_rel_amount_same_minute_20d"] == pytest.approx(3.5294)
    assert features["hist_cum_amount_asof_median_20d"] == pytest.approx(4800.0)
    assert features["signal_rel_cum_amount_asof_20d"] == pytest.approx(2.5)


def test_build_intraday_relative_volume_adds_intraday_acceleration(
    store: DuckDBStore,
) -> None:
    from rquant.stock_features import build_intraday_relative_volume_features

    features = build_intraday_relative_volume_features(
        store,
        "600000.SH",
        datetime(2026, 6, 25, 9, 40),
        current_minute_amount=1400.0,
        current_cum_amount=6000.0,
        current_day_amounts=[
            (time(9, 30), 9000.0),
            (time(9, 31), 8000.0),
            (time(9, 32), 7000.0),
            (time(9, 33), 100.0),
            (time(9, 34), 200.0),
            (time(9, 35), 300.0),
            (time(9, 36), 400.0),
            (time(9, 37), 500.0),
            (time(9, 38), 600.0),
            (time(9, 39), 700.0),
        ],
    )

    assert features["signal_opening_segment"] == 0
    assert features["signal_opening_segment_amount"] is None
    assert features["signal_amount_accel_5m"] == pytest.approx(1400.0 / 500.0)
    assert features["signal_amount_accel_10m"] == pytest.approx(1400.0 / 400.0)


def test_build_intraday_relative_volume_treats_930_to_932_as_opening_segment(
    store: DuckDBStore,
) -> None:
    from rquant.stock_features import build_intraday_relative_volume_features

    features = build_intraday_relative_volume_features(
        store,
        "600000.SH",
        datetime(2026, 6, 25, 9, 31),
        current_minute_amount=8000.0,
        current_cum_amount=17000.0,
        current_day_amounts=[(time(9, 30), 9000.0)],
    )

    assert features["signal_opening_segment"] == 1
    assert features["signal_opening_segment_amount"] == pytest.approx(17000.0)
    assert features["signal_amount_accel_5m"] is None
    assert features["signal_amount_accel_10m"] is None
