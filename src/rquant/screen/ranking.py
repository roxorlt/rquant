"""Cross-sectional ranking for stocks that already passed screening."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from math import isfinite
from numbers import Integral, Real

import duckdb
import numpy as np
import pandas as pd
from pandas.api.types import is_bool_dtype, is_complex_dtype, is_numeric_dtype

RETURN_20D_COLUMN = "RETURN_20D_PCT[0]"
MAX_RANK_CANDIDATES = 8_000


@dataclass(frozen=True, slots=True)
class RankingCondition:
    column: str
    ascending: bool
    weight: float


def load_twenty_day_adjusted_returns(
    connection: duckdb.DuckDBPyConnection,
    trade_date: date,
    ts_codes: Sequence[str],
) -> pd.DataFrame:
    """Read 21 complete open-session close × adjustment facts on an existing connection."""
    if type(trade_date) is not date:
        raise ValueError("ranking trade_date must be a date")
    codes = tuple(ts_codes)
    if (
        len(codes) > MAX_RANK_CANDIDATES
        or len(codes) != len(set(codes))
        or any(type(code) is not str or not code for code in codes)
    ):
        raise ValueError("ranking candidates must be unique bounded stock codes")
    if not codes:
        return pd.DataFrame(
            {"ts_code": pd.Series(dtype=str), RETURN_20D_COLUMN: pd.Series(dtype=float)}
        )

    rows = connection.execute(
        "SELECT cal_date FROM trade_calendar "
        "WHERE exchange = 'SSE' AND is_open AND cal_date <= ? "
        "ORDER BY cal_date DESC LIMIT 21",
        [trade_date],
    ).fetchall()
    sessions = tuple(row[0] for row in reversed(rows))
    values: dict[str, float] = {}
    calendar_complete = False
    if len(sessions) == 21 and sessions[-1] == trade_date:
        observed_days = connection.execute(
            "SELECT COUNT(*) FROM trade_calendar "
            "WHERE exchange = 'SSE' AND cal_date BETWEEN ? AND ?",
            [sessions[0], trade_date],
        ).fetchone()[0]
        calendar_complete = observed_days == (trade_date - sessions[0]).days + 1
    if calendar_complete:
        facts = connection.execute(
            """
            SELECT history.ts_code,
                   COUNT(*) AS observed_sessions,
                   COUNT(*) FILTER (
                       WHERE history.close > 0 AND isfinite(history.close)
                         AND adjustment.adj_factor > 0
                         AND isfinite(adjustment.adj_factor)
                   ) AS valid_sessions,
                   MAX(CASE WHEN history.trade_date = ? THEN
                       history.close * adjustment.adj_factor END) AS adjusted_start,
                   MAX(CASE WHEN history.trade_date = ? THEN
                       history.close * adjustment.adj_factor END) AS adjusted_end
            FROM daily_bar AS history
            JOIN (SELECT UNNEST(?::DATE[]) AS trade_date) AS open_day
              ON open_day.trade_date = history.trade_date
            LEFT JOIN adj_factor AS adjustment
              ON adjustment.ts_code = history.ts_code
             AND adjustment.trade_date = history.trade_date
            WHERE history.ts_code IN (SELECT UNNEST(?::VARCHAR[]))
            GROUP BY history.ts_code
            """,
            [sessions[0], sessions[-1], list(sessions), list(codes)],
        ).fetchall()
        for code, observed, valid, start, end in facts:
            if observed != 21 or valid != 21 or start is None or end is None:
                continue
            if not (isfinite(start) and isfinite(end) and start > 0):
                continue
            result = (end / start - 1) * 100
            if isfinite(result):
                values[code] = result
    return pd.DataFrame(
        {"ts_code": codes, RETURN_20D_COLUMN: [values.get(code, np.nan) for code in codes]}
    )


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
    any_trusted_value = False
    for condition in conditions:
        if condition.weight == 0:
            continue
        values = frame[condition.column].to_numpy(dtype=np.float64, na_value=np.nan)
        finite = np.isfinite(values)
        any_trusted_value = any_trusted_value or bool(finite.any())
        missing_counts += ~finite
        percentile = (
            pd.Series(values)
            .where(finite)
            .rank(method="average", pct=True, ascending=not condition.ascending)
            .fillna(0)
            .to_numpy(dtype=np.float64)
        )
        scores += percentile * (condition.weight / total_weight) * 100

    if not frame.empty and not any_trusted_value:
        raise ValueError("trusted ranking values unavailable")

    order = sorted(
        range(len(frame)),
        key=lambda position: (missing_counts[position], -scores[position], codes.iloc[position]),
    )[:top_n]
    result = frame.iloc[order].copy().reset_index(drop=True)
    result["ranking_score"] = scores[order]
    return result
