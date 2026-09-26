"""Cross-sectional ranking for stocks that already passed screening."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from math import isfinite
from numbers import Integral, Real

import numpy as np
import pandas as pd
from pandas.api.types import is_bool_dtype, is_complex_dtype, is_numeric_dtype


@dataclass(frozen=True, slots=True)
class RankingCondition:
    column: str
    ascending: bool
    weight: float


def rank_screen_results(
    frame: pd.DataFrame,
    conditions: Sequence[RankingCondition],
    *,
    top_n: int,
) -> pd.DataFrame:
    """Weight finite-value percentiles (worst 1/n, best 1); rank fewer missing metrics first."""
    if isinstance(top_n, bool) or not isinstance(top_n, Integral) or top_n < 1:
        raise ValueError("top_n must be a positive integer")
    if not frame.columns.is_unique:
        raise ValueError("duplicate input column names are not supported")
    if "ts_code" not in frame.columns:
        raise ValueError("input must include a ts_code column")
    if "ranking_score" in frame.columns:
        raise ValueError("input column ranking_score is reserved for the ranking result")

    codes = frame["ts_code"]
    if not codes.map(lambda code: isinstance(code, str) and bool(code.strip())).all():
        raise ValueError("ts_code must contain nonempty stock-code strings")
    if codes.duplicated().any():
        raise ValueError("duplicate ts_code values are not supported")
    if not conditions:
        raise ValueError("at least one ranking condition is required")

    seen_columns: set[str] = set()
    for condition in conditions:
        if condition.column in seen_columns:
            raise ValueError(f"duplicate ranking column: {condition.column}")
        seen_columns.add(condition.column)
        if condition.column not in frame.columns:
            raise ValueError(f"ranking column unavailable: {condition.column}")
        if not isinstance(condition.ascending, bool):
            raise ValueError(f"ascending for {condition.column} must be boolean")
        if (
            isinstance(condition.weight, bool)
            or not isinstance(condition.weight, Real)
            or not isfinite(condition.weight)
            or condition.weight < 0
        ):
            raise ValueError(f"weight for {condition.column} must be finite and nonnegative")
        metric = frame[condition.column]
        if not frame.empty and (
            not is_numeric_dtype(metric.dtype)
            or is_bool_dtype(metric.dtype)
            or is_complex_dtype(metric.dtype)
        ):
            raise ValueError(f"numeric ranking column required: {condition.column}")

    total_weight = sum(condition.weight for condition in conditions)
    if not isfinite(total_weight) or total_weight <= 0:
        raise ValueError("ranking conditions need a finite positive total weight")

    scores = np.zeros(len(frame), dtype=np.float64)
    missing_counts = np.zeros(len(frame), dtype=np.int64)
    for condition in conditions:
        if condition.weight == 0:
            continue
        values = frame[condition.column].to_numpy(dtype=np.float64, na_value=np.nan)
        finite = np.isfinite(values)
        missing_counts += ~finite
        percentile = (
            pd.Series(values)
            .where(finite)
            .rank(method="average", pct=True, ascending=not condition.ascending)
            .fillna(0)
            .to_numpy(dtype=np.float64)
        )
        scores += percentile * (condition.weight / total_weight) * 100

    order = sorted(
        range(len(frame)),
        key=lambda position: (missing_counts[position], -scores[position], codes.iloc[position]),
    )[:top_n]
    result = frame.iloc[order].copy().reset_index(drop=True)
    result["ranking_score"] = scores[order]
    return result
