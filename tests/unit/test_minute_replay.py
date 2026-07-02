"""分钟级强承接 replay 测试。"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _seed_daily_and_screen(store: DuckDBStore) -> None:
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 24),
            "open": 9.8,
            "high": 10.20,
            "low": 9.7,
            "close": 10.00,
            "pre_close": 9.8,
            "change": 0.2,
            "pct_chg": 2.04,
            "vol": 1,
            "amount": 1,
        },
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 25),
            "open": 10.0,
            "high": 10.6,
            "low": 10.0,
            "close": 10.5,
            "pre_close": 10.0,
            "change": 0.5,
            "pct_chg": 5.0,
            "vol": 1,
            "amount": 1,
        },
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, 26),
            "open": 10.5,
            "high": 10.88,
            "low": 10.4,
            "close": 10.6,
            "pre_close": 10.5,
            "change": 0.1,
            "pct_chg": 0.95,
            "vol": 1,
            "amount": 1,
        },
    ]))
    store.upsert_screen_result(pd.DataFrame([{
        "trade_date": date(2026, 6, 24),
        "preset_name": "n-shape-pool1",
        "ts_code": "600000.SH",
        "name": "浦发银行",
        "close": 10.00,
        "pct_chg": 2.04,
        "extra": None,
    }]))


def _seed_minutes(store: DuckDBStore) -> None:
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 9, 30),
            "freq": "1min",
            "open": 10.00,
            "high": 10.10,
            "low": 10.00,
            "close": 10.05,
            "vol": 10000,
            "amount": 100500,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 9, 31),
            "freq": "1min",
            "open": 10.05,
            "high": 10.18,
            "low": 10.03,
            "close": 10.16,
            "vol": 10000,
            "amount": 101600,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 9, 32),
            "freq": "1min",
            "open": 10.16,
            "high": 10.26,
            "low": 10.15,
            "close": 10.24,
            "vol": 10000,
            "amount": 102400,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 9, 33),
            "freq": "1min",
            "open": 10.24,
            "high": 10.30,
            "low": 10.22,
            "close": 10.28,
            "vol": 10000,
            "amount": 102800,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 26, 9, 30),
            "freq": "1min",
            "open": 10.50,
            "high": 10.88,
            "low": 10.50,
            "close": 10.76,
            "vol": 10000,
            "amount": 107600,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 26, 9, 31),
            "freq": "1min",
            "open": 10.76,
            "high": 10.78,
            "low": 10.58,
            "close": 10.62,
            "vol": 10000,
            "amount": 106200,
            "source": "tushare",
        },
    ]))


def test_replay_enters_when_strong_carry_breaks_t_high_and_exits_next_day(
    store: DuckDBStore,
) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["ts_code"] == "600000.SH"
    assert row["entry_time"] == pd.Timestamp("2026-06-25 09:33:00")
    assert row["entry_price"] == 10.24
    assert row["exit_time"] == pd.Timestamp("2026-06-26 09:31:00")
    assert row["exit_reason"] == "take_profit_trailing"
    assert row["ret_pct"] == pytest.approx(3.5156)


def test_entry_snapshots_replay_matches_minute_replay(
    store: DuckDBStore,
) -> None:
    from rquant.minute_replay import (
        build_minute_replay_entry_snapshots,
        run_minute_strong_carry_replay,
    )
    from rquant.paper import PaperTradeConfig
    from rquant.replay_entry_cache import replay_entry_snapshots_to_trades

    _seed_daily_and_screen(store)
    _seed_minutes(store)

    baseline = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
    )
    snapshots = build_minute_replay_entry_snapshots(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
    )
    cached = replay_entry_snapshots_to_trades(snapshots, PaperTradeConfig())

    assert len(snapshots) == 1
    assert len(cached) == 1
    assert cached.iloc[0]["entry_price"] == baseline.iloc[0]["entry_price"]
    assert cached.iloc[0]["exit_reason"] == baseline.iloc[0]["exit_reason"]
    assert cached.iloc[0]["ret_pct"] == pytest.approx(baseline.iloc[0]["ret_pct"])


def test_replay_combined_pool_deduplicates_and_prefers_pool2(
    store: DuckDBStore,
) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_screen_result(pd.DataFrame([{
        "trade_date": date(2026, 6, 24),
        "preset_name": "n-shape-pool2",
        "ts_code": "600000.SH",
        "name": "浦发银行",
        "close": 10.00,
        "pct_chg": 2.04,
        "extra": None,
    }]))

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        preset_name="n-shape-combined",
        max_hold_days=1,
    )

    assert len(trades) == 1
    assert trades.iloc[0]["pool"] == "pool2"


def test_replay_does_not_buy_on_same_minute_as_signal(store: DuckDBStore) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store._conn.execute(
        """
        DELETE FROM minute_bar
        WHERE ts_code = '600000.SH'
          AND trade_time = TIMESTAMP '2026-06-25 09:33:00'
        """
    )

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
    )

    assert trades.empty


def test_replay_can_use_volume_profile_dynamic_risk(
    store: DuckDBStore,
) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay
    from rquant.volume_profile import VolumeProfileRuleConfig

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, day),
            "open": 10.0,
            "high": 10.1,
            "low": 9.9,
            "close": 10.0,
            "pre_close": 10.0,
            "change": 0.0,
            "pct_chg": 0.0,
            "vol": 1,
            "amount": 1,
        }
        for day in [21, 22, 23]
    ]))
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, day, 9, 30),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 10000,
            "amount": 100000,
            "source": "tushare",
        }
        for day in [21, 22, 23]
    ]))

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
        volume_profile_config=VolumeProfileRuleConfig(
            enabled=True,
            lookback_days=(3,),
            min_reclaimed_poc_count=1,
        ),
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["volume_profile_lookbacks"] == "3"
    assert row["volume_profile_rr"] > 1.2
    assert row["stop_loss_basis"] == "t_close"
    assert row["take_profit_basis"] == "profile_fallback_pct"


def test_replay_skips_when_buy_day_minutes_missing(store: DuckDBStore) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
    )

    assert trades.empty


def test_replay_break_retest_enters_after_reclaiming_t_high(
    store: DuckDBStore,
) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 9, 33),
            "freq": "1min",
            "open": 10.24,
            "high": 10.25,
            "low": 10.19,
            "close": 10.23,
            "vol": 10000,
            "amount": 102300,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 9, 34),
            "freq": "1min",
            "open": 10.23,
            "high": 10.32,
            "low": 10.22,
            "close": 10.30,
            "vol": 10000,
            "amount": 103000,
            "source": "tushare",
        },
    ]))

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
        entry_mode="break_retest",
    )

    assert len(trades) == 1
    assert trades.iloc[0]["entry_time"] == pd.Timestamp("2026-06-25 09:34:00")
    assert trades.iloc[0]["entry_signal"] == "minute_break_retest"


def test_replay_late_confirm_waits_until_confirm_time(store: DuckDBStore) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 10, 30),
            "freq": "1min",
            "open": 10.30,
            "high": 10.40,
            "low": 10.25,
            "close": 10.35,
            "vol": 10000,
            "amount": 103500,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 25, 10, 31),
            "freq": "1min",
            "open": 10.36,
            "high": 10.42,
            "low": 10.35,
            "close": 10.40,
            "vol": 10000,
            "amount": 104000,
            "source": "tushare",
        },
    ]))

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
        entry_mode="late_confirm",
    )

    assert len(trades) == 1
    assert trades.iloc[0]["entry_time"] == pd.Timestamp("2026-06-25 10:31:00")
    assert trades.iloc[0]["entry_signal"] == "minute_late_confirm"


def test_replay_vwap_confirm_enters_after_break_holds_above_vwap(
    store: DuckDBStore,
) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
        entry_mode="vwap_confirm",
    )

    assert len(trades) == 1
    assert trades.iloc[0]["entry_time"] == pd.Timestamp("2026-06-25 09:33:00")
    assert trades.iloc[0]["entry_signal"] == "minute_vwap_confirm"


def test_replay_amount_surge_uses_current_and_prior_minutes_only(
    store: DuckDBStore,
) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)

    quiet_trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
        entry_mode="amount_surge",
    )
    assert quiet_trades.empty

    store.upsert_minute_bars(pd.DataFrame([{
        "ts_code": "600000.SH",
        "trade_time": datetime(2026, 6, 25, 9, 32),
        "freq": "1min",
        "open": 10.16,
        "high": 10.26,
        "low": 10.15,
        "close": 10.24,
        "vol": 50000,
        "amount": 512000,
        "source": "tushare",
    }]))

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
        entry_mode="amount_surge",
    )

    assert len(trades) == 1
    assert trades.iloc[0]["entry_time"] == pd.Timestamp("2026-06-25 09:33:00")
    assert trades.iloc[0]["entry_signal"] == "minute_amount_surge"


def test_replay_attaches_signal_day_market_sentiment(
    store: DuckDBStore,
) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_market_sentiment(pd.DataFrame([{
        "trade_date": date(2026, 6, 24),
        "stock_count": 100,
        "up_count": 60,
        "down_count": 30,
        "flat_count": 10,
        "limit_up_count": 25,
        "first_limit_up_count": 12,
        "limit_down_count": 3,
        "yiziban_count": 4,
        "max_consecutive_limit_ups": 5,
        "high_board_count": 7,
        "up_ratio_pct": 60.0,
        "limit_up_ratio_pct": 25.0,
        "avg_pct_chg": 1.2,
        "median_pct_chg": 0.8,
        "total_amount": 123456.0,
    }]))

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["market_limit_up_count"] == 25
    assert row["market_first_limit_up_count"] == 12
    assert row["market_max_consecutive_limit_ups"] == 5
    assert row["market_high_board_count"] == 7
    assert row["market_up_ratio_pct"] == pytest.approx(60.0)


def test_replay_attaches_signal_day_index_context(
    store: DuckDBStore,
) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_index_daily(pd.DataFrame([
        {
            "ts_code": "000001.SH",
            "trade_date": date(2026, 6, 24),
            "open": 3000.0,
            "high": 3020.0,
            "low": 2990.0,
            "close": 3010.0,
            "pre_close": 3000.0,
            "change": 10.0,
            "pct_chg": 0.3333,
            "vol": 1.0,
            "amount": 1.0,
        },
        {
            "ts_code": "399006.SZ",
            "trade_date": date(2026, 6, 24),
            "open": 2000.0,
            "high": 2040.0,
            "low": 1980.0,
            "close": 2040.0,
            "pre_close": 2000.0,
            "change": 40.0,
            "pct_chg": 2.0,
            "vol": 1.0,
            "amount": 1.0,
        },
    ]))

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["index_sse_pct_chg"] == pytest.approx(0.3333)
    assert row["index_chinext_pct_chg"] == pytest.approx(2.0)


def test_replay_attaches_candidate_stock_features(store: DuckDBStore) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, day),
            "open": 9.7 + idx * 0.1,
            "high": 10.0 + idx * 0.1,
            "low": 9.5 + idx * 0.1,
            "close": 9.8 + idx * 0.1,
            "pre_close": 9.7 + idx * 0.1,
            "change": 0.1,
            "pct_chg": 1.0,
            "vol": 10000 + idx * 1000,
            "amount": 100000 + idx * 20000,
        }
        for idx, day in enumerate([19, 20, 21, 22, 23], start=1)
    ]))
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 24, 9, minute),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 100,
            "amount": amount,
            "source": "tushare",
        }
        for minute, amount in [(30, 1000), (31, 1100), (32, 1200)]
    ]))

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["price_position_90d_pct"] is not None
    assert row["price_rank_90d_pct"] is not None
    assert row["accum_obv_change_20d_pct"] > 0
    assert row["signal_rel_amount_same_minute_20d"] > 1
    assert row["signal_rel_cum_amount_asof_20d"] > 1


def test_replay_adjusts_price_discontinuity_window(store: DuckDBStore) -> None:
    from rquant.minute_replay import run_minute_strong_carry_replay

    _seed_daily_and_screen(store)
    _seed_minutes(store)
    store.upsert_daily(pd.DataFrame([{
        "ts_code": "600000.SH",
        "trade_date": date(2026, 6, 26),
        "open": 8.10,
        "high": 8.50,
        "low": 8.00,
        "close": 8.20,
        "pre_close": 8.00,
        "change": 0.2,
        "pct_chg": 2.5,
        "vol": 1,
        "amount": 1,
    }]))
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 26, 9, 30),
            "freq": "1min",
            "open": 8.00,
            "high": 8.20,
            "low": 8.00,
            "close": 8.15,
            "vol": 10000,
            "amount": 81500,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 26, 9, 31),
            "freq": "1min",
            "open": 8.15,
            "high": 8.18,
            "low": 7.95,
            "close": 8.05,
            "vol": 10000,
            "amount": 80500,
            "source": "tushare",
        },
    ]))

    trades = run_minute_strong_carry_replay(
        store,
        start_date="2026-06-24",
        end_date="2026-06-24",
        max_hold_days=1,
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["entry_price_raw"] == pytest.approx(10.24)
    assert row["entry_price"] == pytest.approx(10.24 * 8.00 / 10.50)
    assert row["exit_reason"] == "take_profit_trailing"
    assert row["ret_pct"] > 0
