"""科创/创业板盘中放量追击策略回放测试。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, time, timedelta

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _daily_row(ts_code: str, trade_date: date, close: float) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "open": close,
        "high": close * 1.02,
        "low": close * 0.98,
        "close": close,
        "pre_close": close / 1.01,
        "change": close - close / 1.01,
        "pct_chg": 1.0,
        "vol": 1000.0,
        "amount": close * 100000.0,
    }


def _state_row(
    ts_code: str,
    trade_date: date,
    *,
    board_type: str,
    limit_up_price: float,
    is_yiziban: bool = False,
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "is_st": False,
        "is_bj": False,
        "board_type": board_type,
        "limit_pct": 0.20,
        "limit_up_price": limit_up_price,
        "limit_down_price": limit_up_price / 1.5,
        "is_limit_up": False,
        "is_limit_down": False,
        "is_first_limit_up": False,
        "is_yiziban": is_yiziban,
        "consecutive_limit_ups": 0,
        "body_upper": limit_up_price / 1.2,
        "body_lower": limit_up_price / 1.25,
    }


def _indicator_row(
    ts_code: str,
    trade_date: date,
    *,
    bull: bool = True,
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "ma5": 11.0 if bull else 9.0,
        "ma10": 10.5,
        "ma20": 10.0,
        "ma60": 9.5,
        "rsi6": None,
        "rsi14": None,
        "macd": None,
        "macd_signal": None,
        "macd_hist": None,
        "kdj_k": None,
        "kdj_d": None,
        "kdj_j": None,
    }


def _minute_row(
    ts_code: str,
    trade_time: datetime,
    price: float,
    *,
    amount: float,
    low: float | None = None,
    high: float | None = None,
    open_price: float | None = None,
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_time": trade_time,
        "freq": "1min",
        "open": open_price if open_price is not None else price,
        "high": high if high is not None else price,
        "low": low if low is not None else price,
        "close": price,
        "vol": amount / price,
        "amount": amount,
        "source": "tushare",
    }


def _seed_base_market(store: DuckDBStore) -> None:
    dates = [
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
        date(2026, 6, 25),
        date(2026, 6, 26),
    ]
    rows: list[dict[str, object]] = []
    for i, trade_date in enumerate(dates):
        rows.append(_daily_row("300001.SZ", trade_date, 10.0 + i * 0.2))
        rows.append(_daily_row("688001.SH", trade_date, 20.0 + i * 0.2))
        rows.append(_daily_row("600001.SH", trade_date, 30.0 + i * 0.2))
    store.upsert_daily(pd.DataFrame(rows))
    store.upsert_stock_basic(pd.DataFrame([
        {
            "ts_code": "300001.SZ",
            "symbol": "300001",
            "name": "创业样本",
            "area": "深圳",
            "industry": "测试",
            "list_date": "20200101",
            "market": "创业板",
        },
        {
            "ts_code": "688001.SH",
            "symbol": "688001",
            "name": "科创样本",
            "area": "上海",
            "industry": "测试",
            "list_date": "20200101",
            "market": "科创板",
        },
        {
            "ts_code": "600001.SH",
            "symbol": "600001",
            "name": "主板样本",
            "area": "上海",
            "industry": "测试",
            "list_date": "20000101",
            "market": "主板",
        },
    ]))
    store.upsert_state(pd.DataFrame([
        _state_row("300001.SZ", date(2026, 6, 25), board_type="gem", limit_up_price=13.0),
        _state_row("300001.SZ", date(2026, 6, 26), board_type="gem", limit_up_price=13.2),
        _state_row("688001.SH", date(2026, 6, 25), board_type="star", limit_up_price=25.0),
        _state_row("688001.SH", date(2026, 6, 26), board_type="star", limit_up_price=25.2),
        _state_row("600001.SH", date(2026, 6, 25), board_type="main", limit_up_price=33.0),
        _state_row("600001.SH", date(2026, 6, 26), board_type="main", limit_up_price=33.2),
    ]))
    store.upsert_indicators(pd.DataFrame([
        _indicator_row("300001.SZ", date(2026, 6, 24), bull=True),
        _indicator_row("688001.SH", date(2026, 6, 24), bull=False),
        _indicator_row("600001.SH", date(2026, 6, 24), bull=True),
    ]))


def _seed_volume_surge_minutes(store: DuckDBStore) -> None:
    rows: list[dict[str, object]] = []
    for hist_day in [date(2026, 6, 22), date(2026, 6, 23)]:
        for minute in [30, 31, 32, 33]:
            rows.append(_minute_row(
                "300001.SZ",
                datetime.combine(hist_day, time(9, minute)),
                10.0,
                amount=1000.0,
            ))
    rows.extend([
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 30), 10.20, amount=1000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 31), 10.30, amount=1000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 32), 10.45, amount=2000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 33), 10.70, amount=3000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 34), 10.80, amount=1200.0),
        _minute_row("300001.SZ", datetime(2026, 6, 26, 9, 30), 11.30, amount=1000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 26, 15, 0), 11.60, amount=1000.0),
    ])
    store.upsert_minute_bars(pd.DataFrame(rows))


def test_growth_board_surge_replay_uses_intraday_volume_signal(store: DuckDBStore) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    _seed_volume_surge_minutes(store)

    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 33),
            lookback_days=2,
            min_hist_days=2,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
        ),
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["ts_code"] == "300001.SZ"
    assert row["entry_time"] == datetime(2026, 6, 25, 9, 34)
    assert row["entry_signal"] == "growth_board_volume_surge"
    assert row["signal_rel_cum_amount_asof"] == pytest.approx(1.75)
    assert row["signal_rel_amount_same_minute"] == pytest.approx(3.0)
    assert row["hist_intraday_days"] == 2
    assert row["intraday_order_flow_available"] is False
    assert "外盘/内盘" in row["unsupported_intraday_conditions"]
    assert row["ret_pct"] > 0


def test_growth_board_surge_filter_supports_ablation_flags() -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        _passes_surge_filter,
    )

    same_minute_only = {
        "hist_intraday_days": 20,
        "signal_rel_cum_amount_asof": 1.5,
        "signal_rel_amount_same_minute": 3.0,
        "signal_amount_accel_5m": 1.0,
    }
    accel_only = {
        "hist_intraday_days": 20,
        "signal_rel_cum_amount_asof": 1.5,
        "signal_rel_amount_same_minute": 1.0,
        "signal_amount_accel_5m": 3.0,
    }

    assert _passes_surge_filter(same_minute_only, GrowthBoardSurgeConfig())
    assert not _passes_surge_filter(
        same_minute_only,
        GrowthBoardSurgeConfig(use_same_minute_surge=False),
    )
    assert _passes_surge_filter(accel_only, GrowthBoardSurgeConfig())
    assert not _passes_surge_filter(
        accel_only,
        GrowthBoardSurgeConfig(use_accel_surge=False),
    )
    assert _passes_surge_filter(
        same_minute_only,
        GrowthBoardSurgeConfig(use_same_minute_surge=False, use_accel_surge=False),
    )


def test_growth_board_surge_replay_calculates_ma_from_daily_bar(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    dates = [date(2026, 3, 1) + timedelta(days=i) for i in range(63)]
    ts_code = "300002.SZ"
    daily_rows = [
        _daily_row(ts_code, trade_date, 10.0 + idx * 0.1)
        for idx, trade_date in enumerate(dates)
    ]
    store.upsert_daily(pd.DataFrame(daily_rows))
    store.upsert_stock_basic(pd.DataFrame([{
        "ts_code": ts_code,
        "symbol": "300002",
        "name": "无指标样本",
        "area": "深圳",
        "industry": "测试",
        "list_date": "20200101",
        "market": "创业板",
    }]))
    signal_date = dates[-2]
    exit_date = dates[-1]
    store.upsert_state(pd.DataFrame([
        _state_row(ts_code, signal_date, board_type="gem", limit_up_price=20.0),
    ]))
    hist_dates = dates[-5:-3]
    minute_rows = []
    for hist_day in hist_dates:
        for minute in [30, 31, 32, 33]:
            minute_rows.append(
                _minute_row(
                    ts_code,
                    datetime.combine(hist_day, time(9, minute)),
                    15.0,
                    amount=1000.0,
                )
            )
    minute_rows.extend([
        _minute_row(ts_code, datetime.combine(signal_date, time(9, 30)), 16.10, amount=1000.0),
        _minute_row(ts_code, datetime.combine(signal_date, time(9, 31)), 16.20, amount=1000.0),
        _minute_row(ts_code, datetime.combine(signal_date, time(9, 32)), 16.35, amount=2000.0),
        _minute_row(ts_code, datetime.combine(signal_date, time(9, 33)), 16.60, amount=3000.0),
        _minute_row(ts_code, datetime.combine(signal_date, time(9, 34)), 16.70, amount=1200.0),
        _minute_row(ts_code, datetime.combine(exit_date, time(15, 0)), 17.20, amount=1000.0),
    ])
    store.upsert_minute_bars(pd.DataFrame(minute_rows))

    trades = run_growth_board_surge_replay(
        store,
        start_date=signal_date,
        end_date=signal_date,
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 33),
            lookback_days=2,
            min_hist_days=2,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
        ),
    )

    assert len(trades) == 1
    assert trades.iloc[0]["ts_code"] == ts_code


def test_growth_board_surge_replay_requires_next_day_exit_minutes(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    rows: list[dict[str, object]] = []
    for hist_day in [date(2026, 6, 22), date(2026, 6, 23)]:
        for minute in [30, 31, 32, 33]:
            rows.append(_minute_row(
                "300001.SZ",
                datetime.combine(hist_day, time(9, minute)),
                10.0,
                amount=1000.0,
            ))
    rows.extend([
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 30), 10.20, amount=1000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 31), 10.30, amount=1000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 32), 10.45, amount=2000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 33), 10.70, amount=3000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 34), 10.80, amount=1200.0),
    ])
    store.upsert_minute_bars(pd.DataFrame(rows))

    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 33),
            lookback_days=2,
            min_hist_days=2,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
        ),
    )

    assert trades.empty


def test_growth_board_surge_replay_filters_intraday_yiziban(store: DuckDBStore) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    rows = []
    for hist_day in [date(2026, 6, 22), date(2026, 6, 23)]:
        rows.append(
            _minute_row(
                "300001.SZ",
                datetime.combine(hist_day, time(9, 30)),
                10.0,
                amount=1000.0,
            )
        )
    rows.extend([
        _minute_row(
            "300001.SZ",
            datetime(2026, 6, 25, 9, 30),
            13.0,
            amount=10000.0,
            low=13.0,
            high=13.0,
            open_price=13.0,
        ),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 31), 13.0, amount=0.0),
        _minute_row("300001.SZ", datetime(2026, 6, 26, 15, 0), 13.2, amount=1000.0),
    ])
    store.upsert_minute_bars(pd.DataFrame(rows))

    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 30),
            lookback_days=2,
            min_hist_days=1,
        ),
    )

    assert trades.empty
