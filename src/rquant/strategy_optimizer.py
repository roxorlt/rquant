"""分钟策略参数自动评估与排序。"""

from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from rquant.minute_replay import MinuteFreq
from rquant.research_run_spec import ExecutionCostSpec
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_compare import EntryMode, ProfileVariant, run_entry_mode_comparison
from rquant.topn_selection import (
    MARKET_TEMPERATURE_FEATURE,
    FeatureScoreProfile,
    resolve_score_profiles,
    run_topn_comparison,
)
from rquant.topn_walk_forward import build_expanding_folds, run_topn_walk_forward


class StrategyOptimizationResult(BaseModel):
    """策略自动优化输出。"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    rankings: pd.DataFrame
    trades: pd.DataFrame
    topn_rankings: pd.DataFrame = Field(default_factory=pd.DataFrame)
    topn_trades: pd.DataFrame = Field(default_factory=pd.DataFrame)
    walk_forward_rankings: pd.DataFrame = Field(default_factory=pd.DataFrame)
    walk_forward_trades: pd.DataFrame = Field(default_factory=pd.DataFrame)


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value[:10])


def _screen_dates(
    store: DuckDBStore,
    start_date: date,
    end_date: date,
    preset_name: str,
) -> list[date]:
    if preset_name == "n-shape-combined":
        df = store._conn.execute(
            """
            SELECT DISTINCT trade_date
            FROM screen_result
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND preset_name IN ('n-shape-pool1', 'n-shape-pool2')
            ORDER BY trade_date
            """,
            [start_date, end_date],
        ).fetchdf()
        return [
            value.date() if hasattr(value, "date") else value
            for value in df["trade_date"].tolist()
        ]
    df = store._conn.execute(
        """
        SELECT DISTINCT trade_date
        FROM screen_result
        WHERE trade_date >= ?
          AND trade_date <= ?
          AND preset_name = ?
        ORDER BY trade_date
        """,
        [start_date, end_date, preset_name],
    ).fetchdf()
    out: list[date] = []
    for value in df["trade_date"].tolist():
        out.append(value.date() if hasattr(value, "date") else value)
    return out


def _attach_market_temperature(store: DuckDBStore, trades: pd.DataFrame) -> pd.DataFrame:
    """给 replay 交易明细按 signal_date（相对入场日是 T-1）补市场温度列。

    replay 明细当前不自带温度列；表缺列（老库）或当日无数据时留 NaN，
    环境门控画像对缺失值自动放行，不影响 V1 等无门控画像。
    """
    if trades.empty or "signal_date" not in trades.columns:
        return trades
    if MARKET_TEMPERATURE_FEATURE in trades.columns:
        return trades
    try:
        df = store._conn.execute(
            """
            SELECT trade_date, above_ma20_ratio_pct
            FROM market_sentiment_daily
            WHERE above_ma20_ratio_pct IS NOT NULL
            """
        ).fetchdf()
    except duckdb.Error:
        return trades
    mapping = {
        _parse_date(str(row["trade_date"])): float(row["above_ma20_ratio_pct"])
        for _, row in df.iterrows()
    }
    out = trades.copy()
    out[MARKET_TEMPERATURE_FEATURE] = out["signal_date"].map(
        lambda value: mapping.get(_parse_date(str(value)))
    )
    return out


def _train_test_ranges(
    dates: list[date],
    *,
    validation_ratio: float,
) -> tuple[tuple[date, date], tuple[date, date]]:
    if not dates:
        msg = "没有可优化的候选日期"
        raise ValueError(msg)
    if validation_ratio <= 0 or len(dates) < 2:
        return (dates[0], dates[-1]), (dates[0], dates[-1])

    split_idx = int(len(dates) * (1 - validation_ratio))
    split_idx = max(1, min(split_idx, len(dates) - 1))
    train_dates = dates[:split_idx]
    test_dates = dates[split_idx:]
    return (train_dates[0], train_dates[-1]), (test_dates[0], test_dates[-1])


def _score_row(row: pd.Series, *, min_trades: int) -> float:
    trades = int(row.get("trades") or 0)
    if trades == 0:
        return -999.0
    mean_ret = float(row.get("mean_ret_pct") or 0.0)
    win_rate = float(row.get("win_rate_pct") or 0.0)
    worst_ret = float(row.get("worst_ret_pct") or 0.0)
    gap_stop_rate = float(row.get("gap_stop_rate_pct") or 0.0)
    low_sample_penalty = max(min_trades - trades, 0) * 2.0
    downside_penalty = abs(min(worst_ret, 0.0)) * 0.15
    gap_penalty = gap_stop_rate * 0.02
    win_bonus = (win_rate - 50.0) * 0.02
    return round(mean_ret + win_bonus - downside_penalty - gap_penalty - low_sample_penalty, 4)


def _prefix_summary(summary: pd.DataFrame, prefix: str, *, min_trades: int) -> pd.DataFrame:
    out = summary.copy()
    out["score"] = out.apply(_score_row, axis=1, min_trades=min_trades)
    rename = {
        col: f"{prefix}_{col}"
        for col in out.columns
        if col not in {
            "entry_mode",
            "profile_variant",
            "score_profile",
            "score_profile_label",
            "selection",
        }
    }
    return out.rename(columns=rename)


def _candidate_id(row: pd.Series) -> str:
    return (
        f"{row['entry_mode']}|{row['profile_variant']}|"
        f"h{int(row['max_hold_days'])}"
    )


def _topn_candidate_id(row: pd.Series) -> str:
    return f"{_candidate_id(row)}|{row['score_profile']}|{row['selection']}"


def _score_walk_forward_row(row: pd.Series, *, min_trades: int) -> float:
    trades = int(row.get("test_trades") or 0)
    if trades == 0:
        return -999.0
    mean_ret = float(row.get("test_mean_ret_pct") or 0.0)
    win_rate = float(row.get("test_win_rate_pct") or 0.0)
    worst_ret = float(row.get("test_worst_ret_pct") or 0.0)
    low_sample_penalty = max(min_trades - trades, 0) * 2.0
    downside_penalty = abs(min(worst_ret, 0.0)) * 0.15
    win_bonus = (win_rate - 50.0) * 0.02
    return round(mean_ret + win_bonus - downside_penalty - low_sample_penalty, 4)


def _topn_summary_by_strategy(
    trades: pd.DataFrame,
    *,
    top_n_options: list[int],
    score_profiles: list[FeatureScoreProfile],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if trades.empty:
        return pd.DataFrame(), pd.DataFrame()

    summary_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    group_cols = ["entry_mode", "profile_variant"]
    for keys, group in trades.groupby(group_cols, dropna=False):
        entry_mode, profile_variant = keys
        result = run_topn_comparison(
            group,
            top_n_options=top_n_options,
            score_profiles=score_profiles,
        )
        if not result.summary.empty:
            summary = result.summary.copy()
            summary.insert(0, "profile_variant", profile_variant)
            summary.insert(0, "entry_mode", entry_mode)
            summary_frames.append(summary)
        if not result.trades.empty:
            selected = result.trades.copy()
            selected["profile_variant"] = profile_variant
            selected["entry_mode"] = entry_mode
            trade_frames.append(selected)

    summaries = (
        pd.concat(summary_frames, ignore_index=True)
        if summary_frames else pd.DataFrame()
    )
    selected_trades = (
        pd.concat(trade_frames, ignore_index=True)
        if trade_frames else pd.DataFrame()
    )
    return summaries, selected_trades


def run_strategy_optimization(
    store: DuckDBStore,
    *,
    start_date: str | date,
    end_date: str | date,
    preset_name: str = "n-shape-pool1",
    entry_modes: list[EntryMode] | None = None,
    profile_variants: list[ProfileVariant] | None = None,
    max_hold_days_options: list[int] | None = None,
    validation_ratio: float = 0.3,
    min_trades: int = 5,
    top_n_options: list[int] | None = None,
    score_profile_names: list[str] | None = None,
    walk_forward_folds: int = 0,
    freq: MinuteFreq = "1min",
    execution_costs: ExecutionCostSpec | None = None,
) -> StrategyOptimizationResult:
    """自动枚举策略组合，并按训练/验证稳健分排序。"""
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    dates = _screen_dates(store, start, end, preset_name)
    train_range, test_range = _train_test_ranges(dates, validation_ratio=validation_ratio)
    modes = entry_modes or [
        "first_break",
        "break_retest",
        "late_confirm",
        "vwap_confirm",
        "amount_surge",
    ]
    variants = profile_variants or ["baseline", "vp_risk_only", "vp_90"]
    hold_options = max_hold_days_options or [1, 3, 5]
    topn_options = top_n_options or [1, 2, 3, 5]
    score_profiles = resolve_score_profiles(score_profile_names)

    ranking_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    topn_ranking_frames: list[pd.DataFrame] = []
    topn_trade_frames: list[pd.DataFrame] = []
    walk_forward_frames: list[pd.DataFrame] = []
    walk_forward_trade_frames: list[pd.DataFrame] = []
    for hold_days in hold_options:
        train = run_entry_mode_comparison(
            store,
            start_date=train_range[0],
            end_date=train_range[1],
            entry_modes=modes,
            profile_variants=variants,
            preset_name=preset_name,
            max_hold_days=hold_days,
            freq=freq,
            execution_costs=execution_costs,
        )
        test = run_entry_mode_comparison(
            store,
            start_date=test_range[0],
            end_date=test_range[1],
            entry_modes=modes,
            profile_variants=variants,
            preset_name=preset_name,
            max_hold_days=hold_days,
            freq=freq,
            execution_costs=execution_costs,
        )
        train_trades = _attach_market_temperature(store, train.trades)
        test_trades = _attach_market_temperature(store, test.trades)
        train_summary = _prefix_summary(train.summary, "train", min_trades=min_trades)
        test_summary = _prefix_summary(test.summary, "test", min_trades=min_trades)
        merged = train_summary.merge(
            test_summary,
            on=["entry_mode", "profile_variant"],
            how="outer",
        )
        merged.insert(0, "max_hold_days", hold_days)
        ranking_frames.append(merged)

        for split_name, trades in (("train", train_trades), ("test", test_trades)):
            if trades.empty:
                continue
            payload = trades.copy()
            payload.insert(0, "split", split_name)
            payload.insert(0, "max_hold_days", hold_days)
            trade_frames.append(payload)

        train_topn, train_topn_trades = _topn_summary_by_strategy(
            train_trades,
            top_n_options=topn_options,
            score_profiles=score_profiles,
        )
        test_topn, test_topn_trades = _topn_summary_by_strategy(
            test_trades,
            top_n_options=topn_options,
            score_profiles=score_profiles,
        )
        if not train_topn.empty and not test_topn.empty:
            train_prefixed = _prefix_summary(train_topn, "train", min_trades=min_trades)
            test_prefixed = _prefix_summary(test_topn, "test", min_trades=min_trades)
            merged_topn = train_prefixed.merge(
                test_prefixed,
                on=[
                    "entry_mode",
                    "profile_variant",
                    "score_profile",
                    "score_profile_label",
                    "selection",
                ],
                how="outer",
            )
            merged_topn = merged_topn[merged_topn["selection"] != "all"].copy()
            if not merged_topn.empty:
                merged_topn.insert(0, "max_hold_days", hold_days)
                topn_ranking_frames.append(merged_topn)

        for split_name, selected_trades in (
            ("train", train_topn_trades),
            ("test", test_topn_trades),
        ):
            if selected_trades.empty:
                continue
            payload = selected_trades.copy()
            payload.insert(0, "split", split_name)
            payload.insert(0, "max_hold_days", hold_days)
            topn_trade_frames.append(payload)

        if walk_forward_folds > 0:
            full = run_entry_mode_comparison(
                store,
                start_date=start,
                end_date=end,
                entry_modes=modes,
                profile_variants=variants,
                preset_name=preset_name,
                max_hold_days=hold_days,
                freq=freq,
                execution_costs=execution_costs,
            )
            min_train_dates = max(2, len(dates) // 3)
            folds = build_expanding_folds(
                dates,
                fold_count=walk_forward_folds,
                min_train_dates=min_train_dates,
            )
            walk = run_topn_walk_forward(
                _attach_market_temperature(store, full.trades),
                folds=folds,
                top_n_options=topn_options,
                score_profiles=score_profiles,
            )
            if not walk.summary.empty:
                summary = walk.summary.copy()
                summary.insert(0, "max_hold_days", hold_days)
                walk_forward_frames.append(summary)
            if not walk.trades.empty:
                payload = walk.trades.copy()
                payload.insert(0, "max_hold_days", hold_days)
                walk_forward_trade_frames.append(payload)

    rankings = (
        pd.concat(ranking_frames, ignore_index=True)
        if ranking_frames else pd.DataFrame()
    )
    if not rankings.empty:
        rankings["candidate_id"] = rankings.apply(_candidate_id, axis=1)
        rankings["robust_score"] = (
            rankings["train_score"].fillna(-999.0) * 0.4
            + rankings["test_score"].fillna(-999.0) * 0.6
            - (rankings["train_mean_ret_pct"].fillna(0.0)
               - rankings["test_mean_ret_pct"].fillna(0.0)).abs() * 0.2
        ).round(4)
        rankings = rankings.sort_values(
            ["robust_score", "test_trades", "train_trades"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
        rankings.insert(0, "rank", range(1, len(rankings) + 1))

    trades = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    topn_rankings = (
        pd.concat(topn_ranking_frames, ignore_index=True)
        if topn_ranking_frames else pd.DataFrame()
    )
    if not topn_rankings.empty:
        topn_rankings["candidate_id"] = topn_rankings.apply(_topn_candidate_id, axis=1)
        topn_rankings["robust_score"] = (
            topn_rankings["train_score"].fillna(-999.0) * 0.4
            + topn_rankings["test_score"].fillna(-999.0) * 0.6
            - (topn_rankings["train_mean_ret_pct"].fillna(0.0)
               - topn_rankings["test_mean_ret_pct"].fillna(0.0)).abs() * 0.2
        ).round(4)
        topn_rankings = topn_rankings.sort_values(
            ["robust_score", "test_trades", "train_trades"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
        topn_rankings.insert(0, "rank", range(1, len(topn_rankings) + 1))

    topn_trades = (
        pd.concat(topn_trade_frames, ignore_index=True)
        if topn_trade_frames else pd.DataFrame()
    )
    walk_forward_rankings = (
        pd.concat(walk_forward_frames, ignore_index=True)
        if walk_forward_frames else pd.DataFrame()
    )
    if not walk_forward_rankings.empty:
        walk_forward_rankings["candidate_id"] = walk_forward_rankings.apply(
            _topn_candidate_id,
            axis=1,
        )
        walk_forward_rankings["robust_score"] = walk_forward_rankings.apply(
            _score_walk_forward_row,
            axis=1,
            min_trades=min_trades,
        )
        walk_forward_rankings = walk_forward_rankings.sort_values(
            ["robust_score", "folds", "test_trades"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
        walk_forward_rankings.insert(0, "rank", range(1, len(walk_forward_rankings) + 1))

    walk_forward_trades = (
        pd.concat(walk_forward_trade_frames, ignore_index=True)
        if walk_forward_trade_frames else pd.DataFrame()
    )
    return StrategyOptimizationResult(
        rankings=rankings,
        trades=trades,
        topn_rankings=topn_rankings,
        topn_trades=topn_trades,
        walk_forward_rankings=walk_forward_rankings,
        walk_forward_trades=walk_forward_trades,
    )
