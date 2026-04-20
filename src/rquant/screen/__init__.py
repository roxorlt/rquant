"""筛选引擎：积木函数 + screen() 入口。"""

from rquant.screen.core import screen
from rquant.screen.loader import load_universe
from rquant.screen.rules import (
    AggregateRequest,
    above_ma,
    between,
    board_in,
    # 市值
    circ_mv_lt,
    # 蜡烛形态
    has_lower_shadow,
    consecutive_ups_gte,
    # 指标
    cross_above,
    cross_below,
    first_limit_up,
    # 比较
    gt,
    gte,
    limit_down,
    # 涨跌停 / 连板
    limit_up,
    lt,
    lte,
    not_bj,
    not_limit_up,
    not_yiziban,
    # 属性类
    not_st,
    rsi_overbought,
    rsi_oversold,
    # 成交量
    volume_ratio_gte,
    yiziban,
)

__all__ = [
    "screen", "load_universe", "AggregateRequest",
    "not_st", "not_bj", "board_in",
    "limit_up", "not_limit_up", "first_limit_up", "yiziban", "not_yiziban", "limit_down",
    "consecutive_ups_gte",
    "gt", "lt", "gte", "lte", "between",
    "cross_above", "cross_below", "above_ma", "rsi_oversold", "rsi_overbought",
    "volume_ratio_gte",
    "circ_mv_lt",
    "has_lower_shadow",
]
