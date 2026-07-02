"""特征权重搜索测试。"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd


def _weight_search_trades() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "buy_date": date(2026, 6, 25),
            "ts_code": "000001.SZ",
            "entry_time": datetime(2026, 6, 25, 9, 35),
            "ret_pct": -2.0,
            "exit_reason": "stop_loss",
            "signal_rel_amount_same_minute_20d": 5.0,
            "signal_rel_cum_amount_asof_20d": 4.0,
            "accum_obv_change_20d_pct": -20.0,
            "accum_ad_flow_20d_pct": -20.0,
            "accum_up_down_amount_ratio_20d": 0.5,
            "accum_heavy_no_drop_days_20d": 0,
            "price_position_90d_pct": 20.0,
            "distance_to_high_90d_pct": 30.0,
            "market_up_ratio_pct": 30.0,
            "index_csi1000_pct_chg": 0.0,
        },
        {
            "buy_date": date(2026, 6, 25),
            "ts_code": "000002.SZ",
            "entry_time": datetime(2026, 6, 25, 9, 36),
            "ret_pct": 5.0,
            "exit_reason": "take_profit",
            "signal_rel_amount_same_minute_20d": 0.8,
            "signal_rel_cum_amount_asof_20d": 0.8,
            "accum_obv_change_20d_pct": 45.0,
            "accum_ad_flow_20d_pct": 40.0,
            "accum_up_down_amount_ratio_20d": 2.0,
            "accum_heavy_no_drop_days_20d": 4,
            "price_position_90d_pct": 70.0,
            "distance_to_high_90d_pct": 6.0,
            "market_up_ratio_pct": 60.0,
            "index_csi1000_pct_chg": 1.0,
        },
    ])


def test_build_weight_profiles_creates_named_profiles() -> None:
    from rquant.feature_weight_search import build_weight_profiles

    profiles = build_weight_profiles(
        intraday_multipliers=[1.0],
        accumulation_multipliers=[1.5],
        position_multipliers=[1.0],
        market_multipliers=[1.0],
    )

    assert profiles[0].name == "w_i1_a1.5_p1_m1"


def test_run_feature_weight_search_ranks_profiles() -> None:
    from rquant.feature_weight_search import build_weight_profiles, run_feature_weight_search

    profiles = build_weight_profiles(
        intraday_multipliers=[2.0],
        accumulation_multipliers=[2.0],
        position_multipliers=[1.0],
        market_multipliers=[1.0],
    )
    result = run_feature_weight_search(
        _weight_search_trades(),
        score_profiles=profiles,
        top_n_options=[1],
        min_trades=1,
    )

    assert result.iloc[0]["selection"] == "top1"
    assert result.iloc[0]["trades"] == 1
    assert result.iloc[0]["mean_ret_pct"] == 5.0
