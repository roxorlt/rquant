"""筛选积木：每块是返回 (df) -> pd.Series[bool] 的工厂函数。"""

from __future__ import annotations

import re
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


def not_yiziban(offset: int = 0) -> Rule:
    """某日非一字板。"""
    return _bool_state_rule("IS_YIZIBAN", offset, negate=True)


def circ_mv_lt(threshold_yi: float, offset: int = 0) -> Rule:
    """流通市值 < threshold_yi 亿元。

    Tushare circ_mv 单位是万元，1 亿 = 10000 万，
    所以 threshold_yi * 10000 与 CIRC_MV[offset] 比较。
    """
    threshold_wan = threshold_yi * 10000
    col = f"CIRC_MV[{offset}]"

    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[col].fillna(float("inf")) < threshold_wan

    return _tag_lookback(_rule, offset)


def has_lower_shadow(
    min_ratio: float = 1.5,
    min_amplitude: float = 0.02,
    offset: int = 0,
) -> Rule:
    """下影线达标：下影 / 实体 ≥ min_ratio 且振幅 ≥ min_amplitude。

    - 下影线 = BODY_LOWER[offset] - LOW[offset]
    - 实体 = BODY_UPPER[offset] - BODY_LOWER[offset]
    - 振幅 = (HIGH[offset] - LOW[offset]) / LOW[offset]
    - 实体为 0（一字线/十字星）直接返回 False
    """
    def _rule(df: pd.DataFrame) -> pd.Series:
        body_lower = df[f"BODY_LOWER[{offset}]"]
        body_upper = df[f"BODY_UPPER[{offset}]"]
        low = df[f"LOW[{offset}]"]
        high = df[f"HIGH[{offset}]"]

        lower_shadow = body_lower - low
        body = body_upper - body_lower
        amplitude = (high - low) / low.replace(0, float("nan"))

        has_body = body > 0
        ratio_ok = lower_shadow / body.replace(0, float("nan")) >= min_ratio
        amp_ok = amplitude >= min_amplitude

        return has_body & ratio_ok & amp_ok

    return _tag_lookback(_rule, offset)


def limit_down(offset: int = 0) -> Rule:
    """某日跌停。"""
    return _bool_state_rule("IS_LIMIT_DOWN", offset)


def consecutive_ups_gte(n: int, offset: int = 0) -> Rule:
    """某日连板数 ≥ n。"""
    col = f"CONSECUTIVE_LIMIT_UPS[{offset}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[col].fillna(0).astype(int) >= n
    return _tag_lookback(_rule, offset)


_LOOKBACK_RE = re.compile(r"\[(\d+)\]$")


def _parse_lookback(operand: str | float | int) -> int:
    """从 'CLOSE[3]' 抽出 3；数字常数返回 0。"""
    if isinstance(operand, (int, float)):
        return 0
    match = _LOOKBACK_RE.search(operand)
    return int(match.group(1)) if match else 0


def _resolve(df: pd.DataFrame, operand: str | float | int) -> pd.Series | float:
    if isinstance(operand, (int, float)):
        return operand
    return df[operand]


def gt(left: str | float, right: str | float) -> Rule:
    """left > right，操作数可以是字段名字符串或数字常数。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) > _resolve(df, right)
    return _tag_lookback(_rule, max(_parse_lookback(left), _parse_lookback(right)))


def lt(left: str | float, right: str | float) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) < _resolve(df, right)
    return _tag_lookback(_rule, max(_parse_lookback(left), _parse_lookback(right)))


def gte(left: str | float, right: str | float) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) >= _resolve(df, right)
    return _tag_lookback(_rule, max(_parse_lookback(left), _parse_lookback(right)))


def lte(left: str | float, right: str | float) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) <= _resolve(df, right)
    return _tag_lookback(_rule, max(_parse_lookback(left), _parse_lookback(right)))


def between(field: str, low: float, high: float) -> Rule:
    """字段值在 [low, high] 闭区间。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        s = df[field]
        return (s >= low) & (s <= high)
    return _tag_lookback(_rule, _parse_lookback(field))


def cross_above(fast: str, slow: str, offset: int = 0) -> Rule:
    """fast 均线在 offset 日上穿 slow 均线。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        f0 = df[f"{fast}[{offset}]"]
        s0 = df[f"{slow}[{offset}]"]
        f1 = df[f"{fast}[{offset + 1}]"]
        s1 = df[f"{slow}[{offset + 1}]"]
        return (f0 > s0) & (f1 <= s1)
    return _tag_lookback(_rule, offset + 1)


def cross_below(fast: str, slow: str, offset: int = 0) -> Rule:
    """fast 均线在 offset 日下穿 slow 均线。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        f0 = df[f"{fast}[{offset}]"]
        s0 = df[f"{slow}[{offset}]"]
        f1 = df[f"{fast}[{offset + 1}]"]
        s1 = df[f"{slow}[{offset + 1}]"]
        return (f0 < s0) & (f1 >= s1)
    return _tag_lookback(_rule, offset + 1)


def above_ma(period: int, offset: int = 0) -> Rule:
    """CLOSE 在 offset 日高于 MA{period}。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[f"CLOSE[{offset}]"] > df[f"MA{period}[{offset}]"]
    return _tag_lookback(_rule, offset)


def rsi_oversold(period: int = 14, threshold: float = 30.0, offset: int = 0) -> Rule:
    """RSI 低于阈值（默认 30）。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[f"RSI{period}[{offset}]"] < threshold
    return _tag_lookback(_rule, offset)


def rsi_overbought(period: int = 14, threshold: float = 70.0, offset: int = 0) -> Rule:
    """RSI 高于阈值（默认 70）。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[f"RSI{period}[{offset}]"] > threshold
    return _tag_lookback(_rule, offset)


def volume_ratio_gte(n: float, offset: int = 0, window: int = 5) -> Rule:
    """某日成交量 ≥ n × 前 {window} 日成交量均值。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        today = df[f"VOL[{offset}]"]
        prev_cols = [f"VOL[{offset + i}]" for i in range(1, window + 1)]
        mean_prev = df[prev_cols].mean(axis=1)
        return today >= n * mean_prev
    return _tag_lookback(_rule, offset + window)
