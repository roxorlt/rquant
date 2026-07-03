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
    reopen: bool = False,
    unseal_at_close: bool = False,
    tail_days: bool = False,
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
    if tail_days:
        # 持有窗口第 2 个交易日只有日线（分钟缺失），驱动日线降级退出路径
        daily_rows.append({
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 29),
            "open": 11.0,
            "high": 11.3,
            "low": 10.85,
            "close": 11.2,
            "pre_close": 10.9,
            "change": 0.3,
            "pct_chg": 2.75,
            "vol": 6000.0,
            "amount": 66000.0,
        })
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
    if reopen:
        # 封住 → 开板（close 跌破涨停容差） → 尾盘再封住
        minute_rows.extend([
            {
                "ts_code": "600000.SH",
                "trade_time": datetime(2026, 6, 25, 10, 1),
                "freq": "1min",
                "open": 11.00,
                "high": 11.00,
                "low": 10.85,
                "close": 10.90,
                "vol": 1000,
                "amount": 10900,
                "source": "tushare",
            },
            {
                "ts_code": "600000.SH",
                "trade_time": datetime(2026, 6, 25, 10, 2),
                "freq": "1min",
                "open": 10.95,
                "high": 11.00,
                "low": 10.90,
                "close": 11.00,
                "vol": 1000,
                "amount": 11000,
                "source": "tushare",
            },
        ])
    if unseal_at_close:
        # 盘中封住但尾盘炸板：收盘 close 跌破涨停容差 → b_close_at_limit_up=False
        minute_rows.append({
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 14, 55),
            "freq": "1min",
            "open": 11.00,
            "high": 11.00,
            "low": 10.88,
            "close": 10.90,
            "vol": 1000,
            "amount": 10900,
            "source": "tushare",
        })
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


def _seed_limit_list(
    store: DuckDBStore,
    *,
    open_times: int,
    fd_amount: float = 5e7,
    float_mv: float = 1e9,
) -> None:
    store.upsert_limit_list(pd.DataFrame([{
        "ts_code": "600000.SH",
        "trade_date": date(2026, 6, 25),
        "name": "浦发银行",
        "industry": "银行",
        "close": 11.0,
        "pct_chg": 10.0,
        "amount": 55000.0,
        "limit_amount": None,
        "float_mv": float_mv,
        "total_mv": float_mv,
        "turnover_ratio": 1.0,
        "fd_amount": fd_amount,
        "first_time": "100000",
        "last_time": "100000",
        "open_times": open_times,
        "up_stat": "1/1",
        "limit_times": 1,
        "limit_status": "U",
    }]))


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
    assert row["b_open_times"] == 0
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


def test_auction_gap_minute_replay_factor_threshold_none_keeps_baseline(
    store: DuckDBStore,
) -> None:
    """factor_score_threshold 默认 None = 现状：不评分、不出分数列。"""
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
    assert pd.isna(trades.iloc[0]["auction_factor_score"])


def test_auction_gap_minute_replay_factor_threshold_delays_entry_to_higher_score(
    store: DuckDBStore,
) -> None:
    """9:31 信号分钟得分 ~30 被阈值 40 拦下，9:32 得分 ~47.8 过阈值后下一分钟成交。"""
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
            factor_score_threshold=40.0,
        ),
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    # baseline 信号分钟是 9:31（9:32 开盘成交）；评分闸门把入场推迟到 9:32 信号
    assert row["entry_time"] == pd.Timestamp("2026-06-25 10:00:00")
    assert row["auction_factor_score"] == pytest.approx(47.75, abs=0.1)
    assert row["auction_factor_score_threshold"] == pytest.approx(40.0)


def test_auction_gap_minute_replay_factor_threshold_blocks_all_when_too_high(
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
            factor_score_threshold=99.0,
        ),
    )

    assert trades.empty


def test_b_day_strength_counts_seal_open_reseal_transitions() -> None:
    from rquant.auction_gap_strategy import _b_day_strength

    minutes = pd.DataFrame([
        # 未封 → 封住 → 开板 → 再封 → 尾盘再开板：共 2 次开板，收盘未封住
        {"trade_time": datetime(2026, 6, 25, 9, 31), "high": 10.90, "close": 10.80},
        {"trade_time": datetime(2026, 6, 25, 9, 32), "high": 11.00, "close": 11.00},
        {"trade_time": datetime(2026, 6, 25, 9, 33), "high": 11.00, "close": 10.90},
        {"trade_time": datetime(2026, 6, 25, 9, 34), "high": 11.00, "close": 11.00},
        {"trade_time": datetime(2026, 6, 25, 9, 35), "high": 11.00, "close": 10.95},
    ])

    strength = _b_day_strength(
        minutes,
        trading_date=date(2026, 6, 25),
        limit_up_price=11.00,
        price_tol=0.01,
    )

    assert strength["b_open_times"] == 2
    assert strength["b_limit_up_touch_minutes"] == 4
    assert strength["b_limit_up_close_minutes"] == 2
    assert strength["b_close_at_limit_up"] is False


def test_auction_gap_minute_replay_reports_reopen_count(
    store: DuckDBStore,
) -> None:
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
    )

    _seed_base(store, reopen=True)

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
    assert row["b_open_times"] == 1
    assert row["b_limit_up_close_minutes"] == 2
    assert row["b_close_at_limit_up"]
    assert row["hold_policy"] == "t1"


def _seal_hold_config(**overrides: object) -> object:
    from rquant.auction_gap_strategy import AuctionGapMinuteReplayConfig

    params: dict[str, object] = {
        "start_date": "2026-06-25",
        "end_date": "2026-06-25",
        "max_hold_days": 1,
        "seal_hold_enabled": True,
        "seal_hold_max_days": 2,
        "seal_hold_max_open_times": 0,
    }
    params.update(overrides)
    return AuctionGapMinuteReplayConfig(**params)


def test_seal_hold_extends_hold_and_exits_via_daily_tail(
    store: DuckDBStore,
) -> None:
    """封住 + 官方 0 开板 → seal_hold 延长持有，分钟缺失日走日线降级退出。"""
    from rquant.auction_gap_strategy import run_auction_gap_minute_replay

    _seed_base(store, strong_seal=True, tail_days=True, next_auction_price=10.75)
    _seed_limit_list(store, open_times=0)

    trades = run_auction_gap_minute_replay(store, _seal_hold_config())

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["hold_policy"] == "seal_hold"
    assert row["exit_reason"] == "time_2d"
    assert row["exit_time"] == pd.Timestamp("2026-06-29 15:00:00")
    assert row["exit_price"] == pytest.approx(11.2)
    assert row["holding_trading_days"] == 2
    assert bool(row["exit_daily_fallback"])
    assert row["ret_pct"] == pytest.approx(round((11.2 / 10.53 - 1) * 100, 4))


def test_seal_hold_rejects_when_official_open_times_above_threshold(
    store: DuckDBStore,
) -> None:
    """封住但官方开板 2 次（分钟推算 0 次）→ 官方口径优先，回 T+1。"""
    from rquant.auction_gap_strategy import run_auction_gap_minute_replay

    _seed_base(store, strong_seal=True, tail_days=True, next_auction_price=10.75)
    _seed_limit_list(store, open_times=2)

    trades = run_auction_gap_minute_replay(store, _seal_hold_config())

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["hold_policy"] == "t1"
    assert row["exit_reason"] == "time_1d"
    assert row["exit_time"] == pd.Timestamp("2026-06-26 09:30:00")


def test_seal_hold_requires_close_at_limit_up(
    store: DuckDBStore,
) -> None:
    """尾盘炸板（b_close_at_limit_up=False）→ 即使官方有 U 行也回 T+1。"""
    from rquant.auction_gap_strategy import run_auction_gap_minute_replay

    _seed_base(store, unseal_at_close=True, tail_days=True)
    _seed_limit_list(store, open_times=0)

    trades = run_auction_gap_minute_replay(store, _seal_hold_config())

    assert len(trades) == 1
    row = trades.iloc[0]
    assert not row["b_close_at_limit_up"]
    assert row["hold_policy"] == "t1"
    assert row["exit_reason"] == "next_auction_weak"


def test_seal_hold_falls_back_to_minute_open_times_when_limit_list_missing(
    store: DuckDBStore,
) -> None:
    """limit_list_daily 缺行 → 回退分钟推算 b_open_times=0 → 仍给 seal_hold。"""
    from rquant.auction_gap_strategy import run_auction_gap_minute_replay

    _seed_base(store, strong_seal=True, tail_days=True, next_auction_price=10.75)

    trades = run_auction_gap_minute_replay(store, _seal_hold_config())

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["hold_policy"] == "seal_hold"
    assert row["exit_reason"] == "time_2d"


def test_seal_hold_fallback_rejects_minute_reopen(
    store: DuckDBStore,
) -> None:
    """limit_list_daily 缺行 + 分钟推算开板 1 次 > 阈值 0 → 回 T+1。"""
    from rquant.auction_gap_strategy import run_auction_gap_minute_replay

    _seed_base(store, reopen=True, tail_days=True)

    trades = run_auction_gap_minute_replay(store, _seal_hold_config())

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["b_open_times"] == 1
    assert row["hold_policy"] == "t1"
    assert row["exit_reason"] == "next_auction_weak"


def test_zero_price_next_auction_row_is_ignored(store: DuckDBStore) -> None:
    """次日 auction_bar 出现 price=0 的坏行 → 不按 0 元假成交，落到分钟扫描。"""
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
    )

    _seed_base(store)
    store._conn.execute(
        """
        UPDATE auction_bar SET price = 0
        WHERE ts_code = '600000.SH' AND trade_date = DATE '2026-06-26'
        """
    )

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
    assert row["exit_reason"] != "next_auction_weak"
    assert row["exit_price"] > 0
    assert row["ret_pct"] > -100


def test_resolve_hold_policy_fd_ratio_threshold(store: DuckDBStore) -> None:
    """封单/流通市值占比条件：低于阈值回 T+1，达标给 seal_hold。"""
    from rquant.auction_gap_strategy import _resolve_hold_policy

    candidate = pd.Series({
        "ts_code": "600000.SH",
        "signal_date": date(2026, 6, 25),
    })
    b_strength: dict[str, object] = {"b_close_at_limit_up": True, "b_open_times": 0}

    _seed_limit_list(store, open_times=0, fd_amount=1e6, float_mv=1e9)  # 0.1%
    config = _seal_hold_config(seal_hold_min_fd_to_circ_pct=1.0)
    assert _resolve_hold_policy(store, candidate, b_strength, config) == ("t1", 1)

    _seed_limit_list(store, open_times=0, fd_amount=2e7, float_mv=1e9)  # 2%
    assert _resolve_hold_policy(store, candidate, b_strength, config) == ("seal_hold", 2)
