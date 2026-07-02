"""分钟价量分布特征测试。"""

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


def _seed_profile_data(store: DuckDBStore) -> None:
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, day),
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.0,
            "pre_close": 10.0,
            "change": 0.0,
            "pct_chg": 0.0,
            "vol": 1,
            "amount": 1,
        }
        for day in [1, 2, 3, 4]
    ]))
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 1, 9, 30),
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
            "trade_time": datetime(2026, 6, 2, 9, 30),
            "freq": "1min",
            "open": 11.0,
            "high": 11.0,
            "low": 11.0,
            "close": 11.0,
            "vol": 300.0,
            "amount": 3300.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 3, 9, 30),
            "freq": "1min",
            "open": 9.0,
            "high": 9.0,
            "low": 9.0,
            "close": 9.0,
            "vol": 50.0,
            "amount": 450.0,
            "source": "tushare",
        },
    ]))


def test_calculate_volume_profile_uses_previous_trading_days(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import calculate_volume_profile

    _seed_profile_data(store)

    profile = calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )

    assert profile is not None
    assert profile.rows_count == 3
    assert profile.total_amount == 4750.0
    assert profile.vwap == pytest.approx(4750.0 / 450.0)
    assert profile.poc_price == 11.0
    assert profile.value_area_low <= 11.0 <= profile.value_area_high
    assert profile.above_reference_amount_pct == pytest.approx(3300 / 4750 * 100)


def test_volume_profile_rule_config_defaults_to_90_day_window() -> None:
    from rquant.volume_profile import VolumeProfileRuleConfig

    assert VolumeProfileRuleConfig().lookback_days == (90,)


def test_calculate_volume_profile_returns_none_without_minutes(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import calculate_volume_profile

    store.upsert_daily(pd.DataFrame([{
        "ts_code": "600000.SH",
        "trade_date": date(2026, 6, 4),
        "open": 10.0,
        "high": 10.0,
        "low": 10.0,
        "close": 10.0,
        "pre_close": 10.0,
        "change": 0.0,
        "pct_chg": 0.0,
        "vol": 1,
        "amount": 1,
    }]))

    assert calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=30,
    ) is None


def test_calculate_volume_profile_scales_minutes_to_reference_factor(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import calculate_volume_profile

    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, day),
            "open": 10.0,
            "high": 20.0,
            "low": 9.0,
            "close": 10.0,
            "pre_close": 10.0,
            "change": 0.0,
            "pct_chg": 0.0,
            "vol": 1,
            "amount": 1,
        }
        for day in [1, 2, 3, 4]
    ]))
    store.upsert_adj_factor(pd.DataFrame([
        {"ts_code": "600000.SH", "trade_date": date(2026, 6, 1), "adj_factor": 0.5},
        {"ts_code": "600000.SH", "trade_date": date(2026, 6, 2), "adj_factor": 1.0},
        {"ts_code": "600000.SH", "trade_date": date(2026, 6, 3), "adj_factor": 1.0},
        {"ts_code": "600000.SH", "trade_date": date(2026, 6, 4), "adj_factor": 1.0},
    ]))
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 1, 9, 30),
            "freq": "1min",
            "open": 20.0,
            "high": 20.0,
            "low": 20.0,
            "close": 20.0,
            "vol": 1000.0,
            "amount": 20000.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 2, 9, 30),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 100.0,
            "amount": 1000.0,
            "source": "tushare",
        },
    ]))

    profile = calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )

    assert profile is not None
    assert profile.poc_price == 10.0


def test_build_volume_profile_risk_plan_uses_support_and_fallback_target(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import (
        VolumeProfileRuleConfig,
        build_volume_profile_risk_plan,
        calculate_volume_profile,
    )

    _seed_profile_data(store)
    profile = calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )
    assert profile is not None

    plan = build_volume_profile_risk_plan(
        [profile],
        entry_price=11.20,
        config=VolumeProfileRuleConfig(
            enabled=True,
            lookback_days=(3,),
            min_reclaimed_poc_count=1,
        ),
    )

    assert plan.entry_allowed is True
    assert plan.stop_loss_price == 10.97
    assert plan.stop_loss_basis == "vp3_value_high"
    assert plan.take_profit_basis == "profile_fallback_pct"
    assert plan.payload["reward_risk"] > 1.2


def test_build_volume_profile_risk_plan_rejects_close_overhead_supply() -> None:
    from rquant.volume_profile import (
        VolumeProfile,
        VolumeProfileRuleConfig,
        build_volume_profile_risk_plan,
    )

    profile = VolumeProfile(
        ts_code="600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=30,
        start_date=date(2026, 5, 1),
        end_date=date(2026, 6, 3),
        rows_count=100,
        total_vol=1000,
        total_amount=10000,
        vwap=10.10,
        poc_price=10.00,
        value_area_low=9.80,
        value_area_high=10.40,
        concentration_top5_pct=60,
        below_reference_amount_pct=55,
        above_reference_amount_pct=45,
    )

    plan = build_volume_profile_risk_plan(
        [profile],
        entry_price=10.20,
        config=VolumeProfileRuleConfig(enabled=True, min_reclaimed_poc_count=1),
    )

    assert plan.entry_allowed is False
    assert plan.reject_reason == "overhead_resistance_too_close"
