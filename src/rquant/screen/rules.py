"""筛选积木：每块是返回 (df) -> pd.Series[bool] 的工厂函数。"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

Rule = Callable[[pd.DataFrame], pd.Series]


def _tag_lookback(fn: Rule, n: int) -> Rule:
    """给规则函数挂上 min_lookback 属性，方便 screen() 推断总 lookback。"""
    fn.min_lookback = n  # type: ignore[attr-defined]
    return fn


def not_st() -> Rule:
    """排除 ST / *ST / SST。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return ~df["is_st"].astype(bool)
    return _tag_lookback(_rule, 0)


def not_bj() -> Rule:
    """排除北交所（= board_in(['main','gem','star']) 的快捷方式）。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return ~df["is_bj"].astype(bool)
    return _tag_lookback(_rule, 0)


def board_in(boards: list[str]) -> Rule:
    """板块白名单，boards 可选值 main / gem / star / bj。"""
    allowed = set(boards)
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df["board_type"].isin(allowed)
    return _tag_lookback(_rule, 0)
