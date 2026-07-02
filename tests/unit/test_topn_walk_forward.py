"""topN walk-forward 验证测试。"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest


def _walk_forward_trades() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for idx, buy_date in enumerate(
        [
            date(2026, 6, 22),
            date(2026, 6, 23),
            date(2026, 6, 24),
            date(2026, 6, 25),
        ],
        start=1,
    ):
        rows.append({
            "entry_mode": "first_break",
            "profile_variant": "baseline",
            "signal_date": buy_date,
            "buy_date": buy_date,
            "ts_code": f"00000{idx}.SZ",
            "entry_time": datetime.combine(buy_date, datetime.min.time()).replace(
                hour=9,
                minute=35,
            ),
            "ret_pct": 5.0,
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
        })
        rows.append({
            "entry_mode": "first_break",
            "profile_variant": "baseline",
            "signal_date": buy_date,
            "buy_date": buy_date,
            "ts_code": f"30000{idx}.SZ",
            "entry_time": datetime.combine(buy_date, datetime.min.time()).replace(
                hour=9,
                minute=36,
            ),
            "ret_pct": -3.0,
            "exit_reason": "stop_loss",
            "price_position_90d_pct": 10.0,
            "distance_to_high_90d_pct": 40.0,
            "accum_obv_change_20d_pct": -20.0,
            "accum_ad_flow_20d_pct": -20.0,
            "accum_up_down_amount_ratio_20d": 0.5,
            "accum_heavy_no_drop_days_20d": 0,
            "signal_rel_amount_same_minute_20d": 0.5,
            "signal_rel_cum_amount_asof_20d": 0.5,
            "market_up_ratio_pct": 20.0,
            "index_csi1000_pct_chg": -1.0,
        })
    return pd.DataFrame(rows)


def test_build_expanding_folds_uses_only_past_dates() -> None:
    from rquant.topn_walk_forward import build_expanding_folds

    dates = [
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
        date(2026, 6, 25),
    ]

    folds = build_expanding_folds(dates, fold_count=2)

    assert len(folds) == 2
    assert folds[0].train_dates == [date(2026, 6, 22), date(2026, 6, 23)]
    assert folds[0].test_dates == [date(2026, 6, 24)]
    assert folds[1].train_dates == [
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
    ]
    assert folds[1].test_dates == [date(2026, 6, 25)]


def test_run_topn_walk_forward_aggregates_out_of_sample_trades() -> None:
    from rquant.topn_selection import resolve_score_profiles
    from rquant.topn_walk_forward import build_expanding_folds, run_topn_walk_forward

    folds = build_expanding_folds(
        [
            date(2026, 6, 22),
            date(2026, 6, 23),
            date(2026, 6, 24),
            date(2026, 6, 25),
        ],
        fold_count=2,
    )
    result = run_topn_walk_forward(
        _walk_forward_trades(),
        folds=folds,
        top_n_options=[1],
        score_profiles=resolve_score_profiles(["v1"]),
    )

    row = result.summary.iloc[0]
    assert row["folds"] == 2
    assert row["score_profile"] == "v1"
    assert row["selection"] == "top1"
    assert row["test_trades"] == 2
    assert row["test_mean_ret_pct"] == pytest.approx(5.0)
    assert set(result.trades["fold"]) == {1, 2}
