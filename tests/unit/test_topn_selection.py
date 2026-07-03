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


def _base_feature_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "entry_mode": "first_break",
        "profile_variant": "baseline",
        "signal_date": date(2026, 6, 24),
        "buy_date": date(2026, 6, 25),
        "ts_code": "000001.SZ",
        "entry_time": datetime(2026, 6, 25, 9, 33),
        "ret_pct": 1.0,
        "exit_reason": "take_profit",
        "price_position_90d_pct": 50.0,
        "distance_to_high_90d_pct": 10.0,
        "accum_obv_change_20d_pct": 10.0,
        "accum_ad_flow_20d_pct": 10.0,
        "accum_up_down_amount_ratio_20d": 1.5,
        "accum_heavy_no_drop_days_20d": 2,
        "signal_rel_amount_same_minute_20d": 2.0,
        "signal_rel_cum_amount_asof_20d": 2.0,
        "market_up_ratio_pct": 50.0,
        "index_csi1000_pct_chg": 0.5,
    }
    row.update(overrides)
    return row


def test_v1_profile_unchanged_by_v2_additions() -> None:
    from rquant.topn_selection import (
        BASE_SCORE_TERMS,
        available_score_profile_names,
        resolve_score_profiles,
    )

    names = available_score_profile_names()
    assert {"v2_low_position", "v2_momentum", "v2_env_gate"} <= set(names)

    v1 = resolve_score_profiles(["v1"])[0]
    assert v1.terms == BASE_SCORE_TERMS
    assert v1.env_gate is None
    assert all(not term.skip_if_missing for term in v1.terms)


def test_v2_price_percentile_direction_by_profile() -> None:
    from rquant.topn_selection import feature_score, resolve_score_profiles

    low_profile, momentum_profile = resolve_score_profiles(
        ["v2_low_position", "v2_momentum"]
    )
    low_stock = pd.Series(_base_feature_row(price_percentile_250d=0.1))
    high_stock = pd.Series(_base_feature_row(price_percentile_250d=0.9))

    assert feature_score(low_stock, low_profile) > feature_score(high_stock, low_profile)
    assert feature_score(high_stock, momentum_profile) > feature_score(
        low_stock, momentum_profile
    )


def test_v2_ma_alignment_adds_score() -> None:
    from rquant.topn_selection import feature_score, resolve_score_profiles

    for profile in resolve_score_profiles(["v2_low_position", "v2_momentum"]):
        aligned = pd.Series(_base_feature_row(ma_alignment=1, price_percentile_250d=0.5))
        flat = pd.Series(_base_feature_row(ma_alignment=0, price_percentile_250d=0.5))
        assert feature_score(aligned, profile) == pytest.approx(
            feature_score(flat, profile) + 6.0
        )


def test_v2_missing_trend_features_degrade_to_v1_score() -> None:
    """新股无 250 日历史 / 无 60 日均线时，新因子项贡献 0，不炸也不虚高。"""
    from rquant.topn_selection import feature_score, resolve_score_profiles

    v1, low_profile, momentum_profile = resolve_score_profiles(
        ["v1", "v2_low_position", "v2_momentum"]
    )
    new_stock = pd.Series(
        _base_feature_row(ma_alignment=None, price_percentile_250d=None)
    )

    v1_score = feature_score(new_stock, v1)
    assert feature_score(new_stock, low_profile) == pytest.approx(v1_score)
    assert feature_score(new_stock, momentum_profile) == pytest.approx(v1_score)

    absent_columns = pd.Series(_base_feature_row())
    assert feature_score(absent_columns, low_profile) == pytest.approx(v1_score)


def _env_gate_trades() -> pd.DataFrame:
    weak_day = _base_feature_row(
        ts_code="000001.SZ",
        signal_date=date(2026, 6, 23),
        buy_date=date(2026, 6, 24),
        entry_time=datetime(2026, 6, 24, 9, 33),
        ret_pct=-1.0,
        market_above_ma20_ratio_pct=25.0,
    )
    strong_day = _base_feature_row(
        ts_code="000002.SZ",
        signal_date=date(2026, 6, 24),
        buy_date=date(2026, 6, 25),
        entry_time=datetime(2026, 6, 25, 9, 35),
        ret_pct=3.0,
        market_above_ma20_ratio_pct=55.0,
    )
    return pd.DataFrame([weak_day, strong_day])


def test_env_gate_skips_weak_market_days() -> None:
    from rquant.topn_selection import (
        resolve_score_profiles,
        select_topn_by_feature_score,
    )

    trades = _env_gate_trades()
    v1, gated = resolve_score_profiles(["v1", "v2_env_gate"])

    ungated = select_topn_by_feature_score(trades, top_n=1, score_profile=v1)
    assert len(ungated) == 2

    selected = select_topn_by_feature_score(trades, top_n=1, score_profile=gated)
    assert len(selected) == 1
    assert selected.iloc[0]["ts_code"] == "000002.SZ"


def test_env_gate_missing_column_degrades_gracefully() -> None:
    from rquant.topn_selection import (
        resolve_score_profiles,
        select_topn_by_feature_score,
    )

    trades = _env_gate_trades().drop(columns=["market_above_ma20_ratio_pct"])
    gated = resolve_score_profiles(["v2_env_gate"])[0]

    selected = select_topn_by_feature_score(trades, top_n=1, score_profile=gated)
    assert len(selected) == 2


def test_env_gate_multiplier_downweights_without_skipping() -> None:
    from rquant.topn_selection import (
        EnvGateConfig,
        build_score_profile,
        feature_score,
        select_topn_by_feature_score,
    )

    profile = build_score_profile(
        name="env_soft",
        label="温度降权",
        env_gate=EnvGateConfig(
            feature="market_above_ma20_ratio_pct",
            min_value=30.0,
            multiplier=0.5,
        ),
    )
    trades = _env_gate_trades()
    weak_row = trades.iloc[0]
    ungated_score = feature_score(
        weak_row.drop(labels=["market_above_ma20_ratio_pct"]), profile
    )
    assert feature_score(weak_row, profile) == pytest.approx(
        ungated_score * 0.5, abs=1e-3
    )

    selected = select_topn_by_feature_score(trades, top_n=1, score_profile=profile)
    assert len(selected) == 2


def test_run_topn_comparison_with_env_gate_reports_reduced_trades() -> None:
    from rquant.topn_selection import resolve_score_profiles, run_topn_comparison

    result = run_topn_comparison(
        _env_gate_trades(),
        top_n_options=[1],
        score_profiles=resolve_score_profiles(["v2_env_gate"]),
    )

    summary = result.summary.set_index("selection")
    assert summary.loc["all", "trades"] == 2
    assert summary.loc["top1", "trades"] == 1
    assert summary.loc["top1", "mean_ret_pct"] == pytest.approx(3.0)


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
