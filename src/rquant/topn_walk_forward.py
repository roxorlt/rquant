"""触发后 topN 的 walk-forward 验证。"""

from __future__ import annotations

from datetime import date

import pandas as pd
from pydantic import BaseModel, ConfigDict

from rquant.topn_selection import FeatureScoreProfile, run_topn_comparison


class WalkForwardFold(BaseModel):
    """一个 expanding-window walk-forward 时间折。"""

    fold: int
    train_dates: list[date]
    test_dates: list[date]


class TopNWalkForwardResult(BaseModel):
    """walk-forward 聚合结果。"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    summary: pd.DataFrame
    fold_summary: pd.DataFrame
    trades: pd.DataFrame


def _as_date(value: object) -> date:
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def build_expanding_folds(
    dates: list[date],
    *,
    fold_count: int,
    min_train_dates: int = 1,
) -> list[WalkForwardFold]:
    """生成只用过去训练、未来验证的 expanding-window 时间折。"""
    unique_dates = sorted(set(dates))
    if fold_count < 1 or len(unique_dates) <= min_train_dates:
        return []

    available_test_dates = len(unique_dates) - min_train_dates
    usable_folds = min(fold_count, available_test_dates)
    window_size = max(1, available_test_dates // usable_folds)
    first_test_idx = max(min_train_dates, len(unique_dates) - window_size * usable_folds)

    folds: list[WalkForwardFold] = []
    for idx in range(usable_folds):
        test_start = first_test_idx + idx * window_size
        test_end = (
            first_test_idx + (idx + 1) * window_size
            if idx < usable_folds - 1
            else len(unique_dates)
        )
        train_dates = unique_dates[:test_start]
        test_dates = unique_dates[test_start:test_end]
        if len(train_dates) < min_train_dates or not test_dates:
            continue
        folds.append(
            WalkForwardFold(
                fold=len(folds) + 1,
                train_dates=train_dates,
                test_dates=test_dates,
            )
        )
    return folds


def _prefix_summary(summary: pd.DataFrame, prefix: str) -> pd.DataFrame:
    out = summary.copy()
    rename = {
        col: f"{prefix}_{col}"
        for col in out.columns
        if col not in {"score_profile", "score_profile_label", "selection"}
    }
    return out.rename(columns=rename)


def _trade_date_series(trades: pd.DataFrame) -> pd.Series:
    date_col = "buy_date" if "buy_date" in trades.columns else "signal_date"
    return trades[date_col].map(_as_date)


def _group_key_mask(
    trades: pd.DataFrame,
    group_cols: list[str],
    keys: tuple[object, ...],
) -> pd.Series:
    mask = pd.Series(True, index=trades.index)
    for col, value in zip(group_cols, keys, strict=True):
        mask &= trades[col] == value
    return mask


def _iter_groups(
    trades: pd.DataFrame,
    group_cols: list[str],
):
    for keys, group in trades.groupby(group_cols, dropna=False):
        normalized = keys if isinstance(keys, tuple) else (keys,)
        yield normalized, group


def _aggregate_test_trades(
    trades: pd.DataFrame,
    group_cols: list[str],
    fold_summary: pd.DataFrame,
) -> pd.DataFrame:
    key_cols = group_cols + ["score_profile", "score_profile_label", "selection"]
    if trades.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    train_lookup = {}
    if not fold_summary.empty:
        for keys, group in fold_summary.groupby(key_cols, dropna=False):
            normalized = keys if isinstance(keys, tuple) else (keys,)
            train_lookup[normalized] = {
                "train_folds": int(group["fold"].nunique()),
                "train_trades": int(group["train_trades"].sum()),
                "train_mean_ret_pct": round(float(group["train_mean_ret_pct"].mean()), 4),
            }

    for keys, group in trades.groupby(key_cols, dropna=False):
        normalized = keys if isinstance(keys, tuple) else (keys,)
        train_stats = train_lookup.get(
            normalized,
            {"train_folds": 0, "train_trades": 0, "train_mean_ret_pct": None},
        )
        row = dict(zip(key_cols, normalized, strict=True))
        row.update(train_stats)
        row.update({
            "folds": int(group["fold"].nunique()),
            "test_trades": len(group),
            "test_mean_ret_pct": round(float(group["ret_pct"].mean()), 4),
            "test_median_ret_pct": round(float(group["ret_pct"].median()), 4),
            "test_win_rate_pct": round(float((group["ret_pct"] > 0).mean() * 100), 2),
            "test_best_ret_pct": round(float(group["ret_pct"].max()), 4),
            "test_worst_ret_pct": round(float(group["ret_pct"].min()), 4),
            "test_avg_feature_score": round(float(group["feature_score"].mean()), 4),
        })
        rows.append(row)

    return pd.DataFrame(rows)


def run_topn_walk_forward(
    trades: pd.DataFrame,
    *,
    folds: list[WalkForwardFold],
    top_n_options: list[int] | tuple[int, ...],
    score_profiles: list[FeatureScoreProfile] | tuple[FeatureScoreProfile, ...],
    group_cols: list[str] | None = None,
) -> TopNWalkForwardResult:
    """按时间折验证 topN/profile 组合的出样本表现。"""
    if trades.empty or not folds:
        return TopNWalkForwardResult(
            summary=pd.DataFrame(),
            fold_summary=pd.DataFrame(),
            trades=pd.DataFrame(),
        )

    groups = group_cols or ["entry_mode", "profile_variant"]
    dated = trades.copy()
    dated["_trade_date"] = _trade_date_series(dated)

    fold_summary_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    for fold in folds:
        train = dated[dated["_trade_date"].isin(fold.train_dates)].drop(columns=["_trade_date"])
        test = dated[dated["_trade_date"].isin(fold.test_dates)].drop(columns=["_trade_date"])
        if train.empty or test.empty:
            continue

        for keys, train_group in _iter_groups(train, groups):
            test_group = test[_group_key_mask(test, groups, keys)]
            if test_group.empty:
                continue
            train_result = run_topn_comparison(
                train_group,
                top_n_options=top_n_options,
                score_profiles=score_profiles,
            )
            test_result = run_topn_comparison(
                test_group,
                top_n_options=top_n_options,
                score_profiles=score_profiles,
            )
            train_summary = train_result.summary[train_result.summary["selection"] != "all"]
            test_summary = test_result.summary[test_result.summary["selection"] != "all"]
            if train_summary.empty or test_summary.empty:
                continue
            merged = _prefix_summary(train_summary, "train").merge(
                _prefix_summary(test_summary, "test"),
                on=["score_profile", "score_profile_label", "selection"],
                how="inner",
            )
            for col, value in zip(groups, keys, strict=True):
                merged[col] = value
            merged.insert(0, "fold", fold.fold)
            fold_summary_frames.append(merged)

            if not test_result.trades.empty:
                selected = test_result.trades.copy()
                for col, value in zip(groups, keys, strict=True):
                    selected[col] = value
                selected.insert(0, "fold", fold.fold)
                trade_frames.append(selected)

    fold_summary = (
        pd.concat(fold_summary_frames, ignore_index=True)
        if fold_summary_frames else pd.DataFrame()
    )
    selected_trades = (
        pd.concat(trade_frames, ignore_index=True)
        if trade_frames else pd.DataFrame()
    )
    summary = _aggregate_test_trades(selected_trades, groups, fold_summary)
    return TopNWalkForwardResult(
        summary=summary,
        fold_summary=fold_summary,
        trades=selected_trades,
    )
