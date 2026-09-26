"""筛选积木：每块是返回 (df) -> pd.Series[bool] 的工厂函数。"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

import pandas as pd

Rule = Callable[[pd.DataFrame], pd.Series]
_DEPENDENCY_TAG = object()


def _tag_lookback(
    fn: Rule, n: int, columns: Iterable[str] | None = None
) -> Rule:
    """给规则函数挂上 min_lookback 属性，方便 screen() 推断总 lookback。"""
    fn.min_lookback = n  # type: ignore[attr-defined]
    if columns is not None:
        fn.required_columns = frozenset(columns)  # type: ignore[attr-defined]
        fn._dependency_tag = _DEPENDENCY_TAG  # type: ignore[attr-defined]
    return fn


def required_rule_columns(rules: Sequence[Rule]) -> frozenset[str]:
    """Collect factory-owned dependencies; untagged rules cannot use selective reads."""
    columns: set[str] = set()
    for rule in rules:
        required = getattr(rule, "required_columns", None)
        if (
            getattr(rule, "_dependency_tag", None) is not _DEPENDENCY_TAG
            or not isinstance(required, frozenset)
            or any(not isinstance(column, str) for column in required)
        ):
            raise ValueError("screen rule dependency metadata is unavailable")
        columns.update(required)
    return frozenset(columns)


@dataclass(frozen=True)
class AggregateRequest:
    """规则声明的长窗口聚合需求，由 load_universe() 执行 SQL 实现。"""

    name: str               # 结果列名，如 "max_consec_ups_8d"
    source_table: str       # "daily_state" | "daily_bar" | "daily_basic"
    source_col: str         # "consecutive_limit_ups" | "is_limit_down" 等
    agg_func: str           # "max" | "sum" | "any" | "count_nonzero"
    window: int             # 交易日窗口大小
    exclude_offset: int | None = None  # 排除某个 offset 的日期（从 T 日起算）


def _tag_aggregates(fn: Rule, requests: list[AggregateRequest]) -> Rule:
    """给规则函数挂上 aggregate_requests 属性。"""
    fn.aggregate_requests = requests  # type: ignore[attr-defined]
    return fn


def not_st() -> Rule:
    """排除 ST / *ST / SST。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df["is_st"].astype("boolean").eq(False).fillna(False)
    return _tag_lookback(_rule, 0, ("is_st",))


def not_bj() -> Rule:
    """排除北交所（= board_in(['main','gem','star']) 的快捷方式）。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df["is_bj"].astype("boolean").eq(False).fillna(False)
    return _tag_lookback(_rule, 0, ("is_bj",))


def board_in(boards: list[str]) -> Rule:
    """板块白名单，boards 可选值 main / gem / star / bj。"""
    allowed = set(boards)
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df["board_type"].isin(allowed)
    return _tag_lookback(_rule, 0, ("board_type",))


def _bool_state_rule(col_base: str, offset: int, negate: bool = False) -> Rule:
    col = f"{col_base}[{offset}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        expected = not negate
        return df[col].astype("boolean").eq(expected).fillna(False)
    # Canvas diagnostic 用：内部工厂闭包的 __qualname__ 没有意义，挂上 friendly name
    _rule.__rquant_name__ = f"{'not_' if negate else ''}{col_base.lower()}({offset})"  # type: ignore[attr-defined]
    return _tag_lookback(_rule, offset, (col,))


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

    return _tag_lookback(_rule, offset, (col,))


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
    body_lower_col = f"BODY_LOWER[{offset}]"
    body_upper_col = f"BODY_UPPER[{offset}]"
    low_col = f"LOW[{offset}]"
    high_col = f"HIGH[{offset}]"

    def _rule(df: pd.DataFrame) -> pd.Series:
        body_lower = df[body_lower_col]
        body_upper = df[body_upper_col]
        low = df[low_col]
        high = df[high_col]

        lower_shadow = body_lower - low
        body = body_upper - body_lower
        amplitude = (high - low) / low.replace(0, float("nan"))

        has_body = body > 0
        ratio_ok = lower_shadow / body.replace(0, float("nan")) >= min_ratio
        amp_ok = amplitude >= min_amplitude

        return has_body & ratio_ok & amp_ok

    return _tag_lookback(
        _rule, offset, (body_lower_col, body_upper_col, low_col, high_col)
    )


def no_consec_ups_in_window(threshold: int = 3, window: int = 8) -> Rule:
    """近 window 日内无 threshold 连板（含）以上。

    声明 AggregateRequest：近 window 日 consecutive_limit_ups 的 max。
    规则：max_value < threshold。
    """
    agg_name = f"max_consec_ups_{window}d"
    req = AggregateRequest(
        name=agg_name,
        source_table="daily_state",
        source_col="consecutive_limit_ups",
        agg_func="max",
        window=window,
    )

    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[agg_name].lt(threshold).fillna(False)

    fn = _tag_lookback(_rule, 0, ())
    fn = _tag_aggregates(fn, [req])
    return fn


def no_limit_down_in_window(window: int = 30) -> Rule:
    """近 window 日无跌停。

    声明 AggregateRequest：近 window 日 is_limit_down 的 any（BOOL_OR）。
    规则：has_limit_down == False。
    """
    agg_name = f"has_limit_down_{window}d"
    req = AggregateRequest(
        name=agg_name,
        source_table="daily_state",
        source_col="is_limit_down",
        agg_func="any",
        window=window,
    )

    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[agg_name].astype("boolean").eq(False).fillna(False)

    fn = _tag_lookback(_rule, 0, ())
    fn = _tag_aggregates(fn, [req])
    return fn


def has_prior_limit_up(window: int = 90, exclude_offset: int = 1) -> Rule:
    """近 window 日内（排除 T-exclude_offset 日）至少有 1 次涨停。

    声明 AggregateRequest：近 window 日 is_limit_up 的 count_nonzero，排除 exclude_offset。
    规则：count >= 1。
    """
    agg_name = f"count_limit_up_{window}d_ex{exclude_offset}"
    req = AggregateRequest(
        name=agg_name,
        source_table="daily_state",
        source_col="is_limit_up",
        agg_func="count_nonzero",
        window=window,
        exclude_offset=exclude_offset,
    )

    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[agg_name].ge(1).fillna(False)

    fn = _tag_lookback(_rule, 0, ())
    fn = _tag_aggregates(fn, [req])
    return fn


def limit_down(offset: int = 0) -> Rule:
    """某日跌停。"""
    return _bool_state_rule("IS_LIMIT_DOWN", offset)


def consecutive_ups_gte(n: int, offset: int = 0) -> Rule:
    """某日连板数 ≥ n。"""
    col = f"CONSECUTIVE_LIMIT_UPS[{offset}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[col].fillna(0).astype(int) >= n
    return _tag_lookback(_rule, offset, (col,))


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


def _operand_columns(*operands: str | float | int) -> tuple[str, ...]:
    return tuple(operand for operand in operands if isinstance(operand, str))


def gt(left: str | float, right: str | float) -> Rule:
    """left > right，操作数可以是字段名字符串或数字常数。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) > _resolve(df, right)
    return _tag_lookback(
        _rule, max(_parse_lookback(left), _parse_lookback(right)),
        _operand_columns(left, right),
    )


def lt(left: str | float, right: str | float) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) < _resolve(df, right)
    return _tag_lookback(
        _rule, max(_parse_lookback(left), _parse_lookback(right)),
        _operand_columns(left, right),
    )


def gte(left: str | float, right: str | float) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) >= _resolve(df, right)
    return _tag_lookback(
        _rule, max(_parse_lookback(left), _parse_lookback(right)),
        _operand_columns(left, right),
    )


def lte(left: str | float, right: str | float) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) <= _resolve(df, right)
    return _tag_lookback(
        _rule, max(_parse_lookback(left), _parse_lookback(right)),
        _operand_columns(left, right),
    )


def between(field: str, low: float, high: float) -> Rule:
    """字段值在 [low, high] 闭区间。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        s = df[field]
        return (s >= low) & (s <= high)
    return _tag_lookback(_rule, _parse_lookback(field), (field,))


def cross_above(fast: str, slow: str, offset: int = 0) -> Rule:
    """fast 均线在 offset 日上穿 slow 均线。"""
    f0_col, s0_col = f"{fast}[{offset}]", f"{slow}[{offset}]"
    f1_col, s1_col = f"{fast}[{offset + 1}]", f"{slow}[{offset + 1}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        f0 = df[f0_col]
        s0 = df[s0_col]
        f1 = df[f1_col]
        s1 = df[s1_col]
        return (f0 > s0) & (f1 <= s1)
    return _tag_lookback(_rule, offset + 1, (f0_col, s0_col, f1_col, s1_col))


def cross_below(fast: str, slow: str, offset: int = 0) -> Rule:
    """fast 均线在 offset 日下穿 slow 均线。"""
    f0_col, s0_col = f"{fast}[{offset}]", f"{slow}[{offset}]"
    f1_col, s1_col = f"{fast}[{offset + 1}]", f"{slow}[{offset + 1}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        f0 = df[f0_col]
        s0 = df[s0_col]
        f1 = df[f1_col]
        s1 = df[s1_col]
        return (f0 < s0) & (f1 >= s1)
    return _tag_lookback(_rule, offset + 1, (f0_col, s0_col, f1_col, s1_col))


def above_ma(period: int, offset: int = 0) -> Rule:
    """CLOSE 在 offset 日高于 MA{period}。"""
    close_col, ma_col = f"CLOSE[{offset}]", f"MA{period}[{offset}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[close_col] > df[ma_col]
    return _tag_lookback(_rule, offset, (close_col, ma_col))


def rsi_oversold(period: int = 14, threshold: float = 30.0, offset: int = 0) -> Rule:
    """RSI 低于阈值（默认 30）。"""
    col = f"RSI{period}[{offset}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[col] < threshold
    return _tag_lookback(_rule, offset, (col,))


def rsi_overbought(period: int = 14, threshold: float = 70.0, offset: int = 0) -> Rule:
    """RSI 高于阈值（默认 70）。"""
    col = f"RSI{period}[{offset}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[col] > threshold
    return _tag_lookback(_rule, offset, (col,))


def volume_ratio_gte(n: float, offset: int = 0, window: int = 5) -> Rule:
    """某日成交量 ≥ n × 前 {window} 日成交量均值。"""
    today_col = f"VOL[{offset}]"
    prev_cols = [f"VOL[{offset + i}]" for i in range(1, window + 1)]
    def _rule(df: pd.DataFrame) -> pd.Series:
        today = df[today_col]
        mean_prev = df[prev_cols].mean(axis=1, skipna=False)
        return today >= n * mean_prev
    return _tag_lookback(_rule, offset + window, (today_col, *prev_cols))
