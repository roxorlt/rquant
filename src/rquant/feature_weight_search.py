"""触发后特征权重搜索。"""

from __future__ import annotations

from collections.abc import Iterable
from itertools import product

import pandas as pd

from rquant.topn_selection import (
    FeatureScoreProfile,
    build_score_profile,
    run_topn_comparison,
)


def _multiplier_label(value: float) -> str:
    return f"{value:g}"


def build_weight_profiles(
    *,
    intraday_multipliers: Iterable[float],
    accumulation_multipliers: Iterable[float],
    position_multipliers: Iterable[float],
    market_multipliers: Iterable[float],
) -> list[FeatureScoreProfile]:
    """构造 group multiplier 评分画像网格。"""
    profiles: list[FeatureScoreProfile] = []
    for intraday, accumulation, position, market in product(
        intraday_multipliers,
        accumulation_multipliers,
        position_multipliers,
        market_multipliers,
    ):
        name = (
            f"w_i{_multiplier_label(intraday)}_"
            f"a{_multiplier_label(accumulation)}_"
            f"p{_multiplier_label(position)}_"
            f"m{_multiplier_label(market)}"
        )
        profiles.append(
            build_score_profile(
                name=name,
                label=name,
                group_multipliers={
                    "intraday": intraday,
                    "accumulation": accumulation,
                    "position": position,
                    "market": market,
                },
            )
        )
    return profiles


def _score_row(row: pd.Series, *, min_trades: int) -> float:
    trades = int(row.get("trades") or 0)
    if trades == 0:
        return -999.0
    mean_ret = float(row.get("mean_ret_pct") or 0.0)
    win_rate = float(row.get("win_rate_pct") or 0.0)
    worst_ret = float(row.get("worst_ret_pct") or 0.0)
    low_sample_penalty = max(min_trades - trades, 0) * 2.0
    return round(
        mean_ret
        + (win_rate - 50.0) * 0.02
        - abs(min(worst_ret, 0.0)) * 0.15
        - low_sample_penalty,
        4,
    )


def run_feature_weight_search(
    trades: pd.DataFrame,
    *,
    score_profiles: list[FeatureScoreProfile],
    top_n_options: list[int] | tuple[int, ...],
    min_trades: int = 5,
) -> pd.DataFrame:
    """在已有交易样本上比较不同特征权重 topN。"""
    result = run_topn_comparison(
        trades,
        top_n_options=top_n_options,
        score_profiles=score_profiles,
    )
    summary = result.summary[result.summary["selection"] != "all"].copy()
    if summary.empty:
        return summary
    summary["robust_score"] = summary.apply(_score_row, axis=1, min_trades=min_trades)
    return summary.sort_values(
        ["robust_score", "mean_ret_pct", "win_rate_pct", "trades"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
