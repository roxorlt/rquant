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


def _bool_state_rule(col_base: str, offset: int, negate: bool = False) -> Rule:
    col = f"{col_base}[{offset}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        s = df[col].fillna(False).astype(bool)
        return ~s if negate else s
    return _tag_lookback(_rule, offset)


def limit_up(offset: int = 0) -> Rule:
    """某日涨停。"""
    return _bool_state_rule("IS_LIMIT_UP", offset)


def not_limit_up(offset: int = 0) -> Rule:
    """某日未涨停。"""
    return _bool_state_rule("IS_LIMIT_UP", offset, negate=True)


def first_limit_up(offset: int = 0) -> Rule:
    """某日首板（今涨停且昨未涨停）。"""
    return _bool_state_rule("IS_FIRST_LIMIT_UP", offset)


def yiziban(offset: int = 0) -> Rule:
    """某日一字板。"""
    return _bool_state_rule("IS_YIZIBAN", offset)


def limit_down(offset: int = 0) -> Rule:
    """某日跌停。"""
    return _bool_state_rule("IS_LIMIT_DOWN", offset)


def consecutive_ups_gte(n: int, offset: int = 0) -> Rule:
    """某日连板数 ≥ n。"""
    col = f"CONSECUTIVE_LIMIT_UPS[{offset}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[col].fillna(0).astype(int) >= n
    return _tag_lookback(_rule, offset)
