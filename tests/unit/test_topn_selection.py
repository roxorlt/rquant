"""触发后特征评分 topN 选择测试。"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest


def _sample_trades() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "entry_mode": "first_break",
            "profile_variant": "baseline",
            "signal_date": date(2026, 6, 24),
            "buy_date": date(2026, 6, 25),
            "ts_code": "000001.SZ",
            "entry_time": datetime(2026, 6, 25, 9, 33),
            "ret_pct": -2.0,
            "exit_reason": "stop_loss",
            "price_position_90d_pct": 20.0,
            "distance_to_high_90d_pct": 30.0,
            "accum_obv_change_20d_pct": -20.0,
            "accum_ad_flow_20d_pct": -15.0,
            "accum_up_down_amount_ratio_20d": 0.5,
            "accum_heavy_no_drop_days_20d": 0,
            "signal_rel_amount_same_minute_20d": 0.8,
            "signal_rel_cum_amount_asof_20d": 0.9,
            "market_up_ratio_pct": 20.0,
            "index_csi1000_pct_chg": -1.5,
        },
        {
            "entry_mode": "first_break",
            "profile_variant": "baseline",
            "signal_date": date(2026, 6, 24),
            "buy_date": date(2026, 6, 25),
            "ts_code": "000002.SZ",
            "entry_time": datetime(2026, 6, 25, 9, 35),
            "ret_pct": 6.0,
            "exit_reason": "take_profit",
            "price_position_90d_pct": 75.0,
            "distance_to_high_90d_pct": 5.0,
            "accum_obv_change_20d_pct": 45.0,
            "accum_ad_flow_20d_pct": 35.0,
            "accum_up_down_amount_ratio_20d": 2.2,
            "accum_heavy_no_drop_days_20d": 4,
            "signal_rel_amount_same_minute_20d": 5.0,
            "signal_rel_cum_amount_asof_20d": 4.0,
            "market_up_ratio_pct": 60.0,
            "index_csi1000_pct_chg": 1.0,
        },
    ])


def test_select_topn_by_feature_score_keeps_best_trade_per_day() -> None:
    from rquant.topn_selection import select_topn_by_feature_score

    selected = select_topn_by_feature_score(_sample_trades(), top_n=1)

    assert len(selected) == 1
    assert selected.iloc[0]["ts_code"] == "000002.SZ"
    assert selected.iloc[0]["feature_rank"] == 1
    assert selected.iloc[0]["feature_score"] > 0


def test_run_topn_comparison_reports_uplift_against_all_triggers() -> None:
    from rquant.topn_selection import run_topn_comparison

    result = run_topn_comparison(_sample_trades(), top_n_options=[1])

    assert set(result.summary["selection"]) == {"all", "top1"}
    all_row = result.summary.set_index("selection").loc["all"]
    top_row = result.summary.set_index("selection").loc["top1"]
    assert all_row["trades"] == 2
    assert top_row["trades"] == 1
    assert top_row["mean_ret_pct"] == pytest.approx(6.0)
    assert top_row["mean_delta_vs_all_pct"] == pytest.approx(4.0)


def test_score_profiles_support_named_ablation() -> None:
    from rquant.topn_selection import resolve_score_profiles, run_topn_comparison

    profiles = resolve_score_profiles(["v1", "no_intraday"])
    no_intraday = profiles[1]

    assert no_intraday.name == "no_intraday"
    assert all(term.group != "intraday" for term in no_intraday.terms)

    result = run_topn_comparison(
        _sample_trades(),
        top_n_options=[1],
        score_profiles=profiles,
    )

    assert set(result.summary["score_profile"]) == {"v1", "no_intraday"}
    selected = result.trades.set_index("score_profile")
    assert selected.loc["v1", "selection"] == "top1"
    assert selected.loc["no_intraday", "selection"] == "top1"
