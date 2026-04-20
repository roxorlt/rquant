"""筛选引擎：积木函数 + screen() 入口。"""

from rquant.screen.core import screen
from rquant.screen.loader import load_universe
from rquant.screen.rules import (
    AggregateRequest,
    above_ma,
    between,
    board_in,
    circ_mv_lt,
    consecutive_ups_gte,
    cross_above,
    cross_below,
    first_limit_up,
    gt,
    gte,
    has_lower_shadow,
    has_prior_limit_up,
    limit_down,
    limit_up,
    lt,
    lte,
    no_consec_ups_in_window,
    no_limit_down_in_window,
    not_bj,
    not_limit_up,
    not_st,
    not_yiziban,
    rsi_overbought,
    rsi_oversold,
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
    "no_consec_ups_in_window", "no_limit_down_in_window", "has_prior_limit_up",
]
