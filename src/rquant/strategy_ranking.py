"""Deterministic ranking shared by optimizer and shard aggregation."""

from __future__ import annotations

from typing import Literal

import pandas as pd

StrategyRankingTable = Literal[
    "rankings",
    "topn_rankings",
    "walk_forward_rankings",
]
_SORT_COLUMNS: dict[StrategyRankingTable, tuple[str, ...]] = {
    "rankings": ("robust_score", "test_trades", "train_trades"),
    "topn_rankings": ("robust_score", "test_trades", "train_trades"),
    "walk_forward_rankings": ("robust_score", "folds", "test_trades"),
}


def rank_strategy_table(
    frame: pd.DataFrame,
    *,
    table_name: StrategyRankingTable,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    sort_columns = _SORT_COLUMNS[table_name]
    required = {*sort_columns, "candidate_id"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"ranking table is missing required columns: {sorted(missing)}")
    ranked = (
        frame.drop(columns="rank", errors="ignore")
        .sort_values(
            [*sort_columns, "candidate_id"],
            ascending=[False] * len(sort_columns) + [True],
            kind="mergesort",
        )
        .reset_index(drop=True)
    )
    ranked.insert(0, "rank", range(1, len(ranked) + 1))
    return ranked
