"""LLM-facing 规则注册表：每条积木 → RuleSpec（描述、参数 schema、示例、分类）。

向 LLM 暴露的语义层与 screen.rules 实现层解耦。
新加积木时必须在此注册（test_registry_complete.py 会校验对应关系）。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.screen.rules import (  # noqa: F401
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

# ── Args models ───────────────────────────────────────────────────────────────


class _NoArgs(BaseModel):
    """无参积木的占位 args model。"""

    model_config = ConfigDict(extra="forbid")


class OffsetArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    offset: int = Field(0, ge=0, le=30,
                        description="距 T 日的偏移；0=今天, 1=昨天, 2=前天")


class CircMvLtArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    threshold_yi: float = Field(..., gt=0, le=10000,
                                description="流通市值阈值（亿元）")
    offset: int = Field(0, ge=0, le=30)


class BoardInArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    boards: list[Literal["main", "gem", "star", "bj"]] = Field(
        ..., min_length=1,
        description="允许的板块白名单：main=主板, gem=创业板, star=科创板, bj=北交所",
    )


class ConsecutiveUpsGteArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    n: int = Field(..., ge=1, le=20, description="连板数下限（含）")
    offset: int = Field(0, ge=0, le=30)


class HasLowerShadowArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    min_ratio: float = Field(1.5, gt=0, le=20,
                             description="下影线/实体 比下限")
    min_amplitude: float = Field(0.02, ge=0, le=0.30,
                                 description="日振幅下限（小数，0.02=2%）")
    offset: int = Field(0, ge=0, le=30)


class NoConsecUpsInWindowArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    threshold: int = Field(3, ge=1, le=20,
                           description="若窗口内最高连板数 ≥ threshold 则被排除")
    window: int = Field(8, ge=1, le=120, description="回看窗口（交易日）")


class NoLimitDownInWindowArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    window: int = Field(30, ge=1, le=250)


class HasPriorLimitUpArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    window: int = Field(120, ge=1, le=500)
    exclude_offset: int = Field(1, ge=0, le=30,
                                description="排除某偏移日，避免规则与 first_limit_up 冲突")


class CompareArgs(BaseModel):
    """gt / lt / gte / lte 通用 args。
    operand 形如 'CLOSE[0]' / 'MA5[1]' / 'CIRC_MV[0]'，或字面量数字。
    """
    model_config = ConfigDict(extra="forbid")
    left: str | float = Field(..., description="左操作数（字段名或数字）")
    right: str | float = Field(..., description="右操作数（字段名或数字）")


class BetweenArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    field: str = Field(..., description="字段名，如 'CLOSE[0]', 'PCT_CHG[0]'")
    low: float
    high: float


class CrossArgs(BaseModel):
    """cross_above / cross_below 通用 args。"""
    model_config = ConfigDict(extra="forbid")
    fast: str = Field(..., description="快线名，如 'MA5'")
    slow: str = Field(..., description="慢线名，如 'MA20'")
    offset: int = Field(0, ge=0, le=30)


class AboveMaArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    period: int = Field(..., ge=2, le=250, description="均线周期（5/10/20/60 常见）")
    offset: int = Field(0, ge=0, le=30)


class RsiArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    period: int = Field(14, ge=2, le=60)
    threshold: float = Field(..., ge=0, le=100,
                             description="超卖一般 ≤30，超买一般 ≥70")
    offset: int = Field(0, ge=0, le=30)


class VolumeRatioArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    n: float = Field(..., gt=0, le=20,
                     description="量比下限（n=2 表示当日量 ≥ 前 window 日均量×2）")
    offset: int = Field(0, ge=0, le=30)
    window: int = Field(5, ge=1, le=60)


# ── RuleSpec ──────────────────────────────────────────────────────────────────

Category = Literal["filter", "state", "indicator", "shape", "aggregate", "compare"]


@dataclass(frozen=True)
class RuleSpec:
    """LLM-facing 规则描述。"""

    name: str
    description: str
    args_model: type[BaseModel]
    examples: list[str]
    category: Category
    fn: Callable


REGISTRY: list[RuleSpec] = [
    RuleSpec(
        name="not_st",
        description="排除 ST / *ST / SST 标的",
        args_model=_NoArgs,
        examples=["排除 ST", "不要 ST 和 *ST"],
        category="filter",
        fn=not_st,
    ),
    RuleSpec(
        name="not_bj",
        description="排除北交所标的",
        args_model=_NoArgs,
        examples=["不要北交所", "排除北交所"],
        category="filter",
        fn=not_bj,
    ),
    RuleSpec(
        name="circ_mv_lt",
        description="流通市值 < threshold_yi 亿元",
        args_model=CircMvLtArgs,
        examples=["流通市值 < 100 亿", "小盘 50 亿以下", "微盘 30 亿"],
        category="filter",
        fn=circ_mv_lt,
    ),
    RuleSpec(
        name="first_limit_up",
        description="某日首板（今涨停且昨未涨停）",
        args_model=OffsetArgs,
        examples=["昨日首板", "T-1 首板", "前天首板（offset=2）"],
        category="state",
        fn=first_limit_up,
    ),
    RuleSpec(
        name="not_limit_up",
        description="某日未涨停",
        args_model=OffsetArgs,
        examples=["今日未涨停（offset=0）", "昨日未涨停"],
        category="state",
        fn=not_limit_up,
    ),
    # ── filter (board) ──
    RuleSpec(
        name="board_in",
        description="板块白名单（main=主板/gem=创业板/star=科创板/bj=北交所）",
        args_model=BoardInArgs,
        examples=["只看主板", "主板+创业板", "排除科创板和北交所等价于 boards=['main','gem']"],
        category="filter",
        fn=board_in,
    ),
    # ── state (limit-up family) ──
    RuleSpec(
        name="limit_up",
        description="某日涨停",
        args_model=OffsetArgs,
        examples=["今日涨停", "昨日涨停（offset=1）"],
        category="state",
        fn=limit_up,
    ),
    RuleSpec(
        name="limit_down",
        description="某日跌停",
        args_model=OffsetArgs,
        examples=["今日跌停"],
        category="state",
        fn=limit_down,
    ),
    RuleSpec(
        name="yiziban",
        description="某日一字板",
        args_model=OffsetArgs,
        examples=["今日一字板"],
        category="state",
        fn=yiziban,
    ),
    RuleSpec(
        name="not_yiziban",
        description="某日非一字板",
        args_model=OffsetArgs,
        examples=["昨日不是一字板（offset=1）"],
        category="state",
        fn=not_yiziban,
    ),
    RuleSpec(
        name="consecutive_ups_gte",
        description="某日连板数 ≥ n",
        args_model=ConsecutiveUpsGteArgs,
        examples=["昨日 3 连板（n=3, offset=1）", "今日 2 连板"],
        category="state",
        fn=consecutive_ups_gte,
    ),
    # ── shape ──
    RuleSpec(
        name="has_lower_shadow",
        description="下影线达标：下影/实体 ≥ min_ratio 且振幅 ≥ min_amplitude",
        args_model=HasLowerShadowArgs,
        examples=["有明显下影（min_ratio=1.5）", "强势下影（min_ratio=2, min_amplitude=0.03）"],
        category="shape",
        fn=has_lower_shadow,
    ),
    # ── compare（通用比较）──
    RuleSpec(
        name="gt",
        description="left > right；操作数可以是字段名（'CLOSE[0]'）或数字常数",
        args_model=CompareArgs,
        examples=["今最高 > 昨收（left='HIGH[0]', right='CLOSE[1]'）", "PCT_CHG > 5（left='PCT_CHG[0]', right=5）"],
        category="compare",
        fn=gt,
    ),
    RuleSpec(
        name="lt",
        description="left < right",
        args_model=CompareArgs,
        examples=["今实体顶 < 昨实体顶（'BODY_UPPER[0]', 'BODY_UPPER[1]'）"],
        category="compare",
        fn=lt,
    ),
    RuleSpec(
        name="gte",
        description="left >= right",
        args_model=CompareArgs,
        examples=[],
        category="compare",
        fn=gte,
    ),
    RuleSpec(
        name="lte",
        description="left <= right",
        args_model=CompareArgs,
        examples=[],
        category="compare",
        fn=lte,
    ),
    RuleSpec(
        name="between",
        description="字段值在 [low, high] 闭区间",
        args_model=BetweenArgs,
        examples=["PCT_CHG 在 -2~5 之间（field='PCT_CHG[0]', low=-2, high=5）"],
        category="compare",
        fn=between,
    ),
    # ── indicator ──
    RuleSpec(
        name="cross_above",
        description="fast 均线在 offset 日上穿 slow 均线",
        args_model=CrossArgs,
        examples=["MA5 上穿 MA20（fast='MA5', slow='MA20'）", "今日 MA10 上穿 MA60"],
        category="indicator",
        fn=cross_above,
    ),
    RuleSpec(
        name="cross_below",
        description="fast 均线在 offset 日下穿 slow 均线",
        args_model=CrossArgs,
        examples=["MA5 下穿 MA20"],
        category="indicator",
        fn=cross_below,
    ),
    RuleSpec(
        name="above_ma",
        description="CLOSE 在 offset 日高于 MA{period}",
        args_model=AboveMaArgs,
        examples=["今收高于 MA20（period=20）", "昨收高于 MA60"],
        category="indicator",
        fn=above_ma,
    ),
    RuleSpec(
        name="rsi_oversold",
        description="RSI{period} 低于 threshold（默认 30）",
        args_model=RsiArgs,
        examples=["RSI14 超卖（period=14, threshold=30）", "RSI 低于 25"],
        category="indicator",
        fn=rsi_oversold,
    ),
    RuleSpec(
        name="rsi_overbought",
        description="RSI{period} 高于 threshold（默认 70）",
        args_model=RsiArgs,
        examples=["RSI14 超买（period=14, threshold=70）"],
        category="indicator",
        fn=rsi_overbought,
    ),
    RuleSpec(
        name="volume_ratio_gte",
        description="某日成交量 ≥ n × 前 window 日成交量均值",
        args_model=VolumeRatioArgs,
        examples=["量比 ≥ 2（n=2）", "放量超 3 倍 5 日均量（n=3, window=5）"],
        category="indicator",
        fn=volume_ratio_gte,
    ),
    # ── aggregate ──
    RuleSpec(
        name="no_consec_ups_in_window",
        description="近 window 日内最高连板数 < threshold（避开高位连板股）",
        args_model=NoConsecUpsInWindowArgs,
        examples=["近 8 日无 3 连板及以上（threshold=3, window=8）"],
        category="aggregate",
        fn=no_consec_ups_in_window,
    ),
    RuleSpec(
        name="no_limit_down_in_window",
        description="近 window 日无跌停",
        args_model=NoLimitDownInWindowArgs,
        examples=["近 30 日无跌停（window=30）"],
        category="aggregate",
        fn=no_limit_down_in_window,
    ),
    RuleSpec(
        name="has_prior_limit_up",
        description="近 window 日（排除 T-exclude_offset 日）至少 1 次涨停（验证活跃度）",
        args_model=HasPriorLimitUpArgs,
        examples=["近 120 日有过涨停（window=120, exclude_offset=1）"],
        category="aggregate",
        fn=has_prior_limit_up,
    ),
]


REGISTRY_BY_NAME: dict[str, RuleSpec] = {spec.name: spec for spec in REGISTRY}


def get_rule_spec(name: str) -> RuleSpec:
    """按 name 取 RuleSpec；未注册抛 ValueError。"""
    if name not in REGISTRY_BY_NAME:
        raise ValueError(f"Unknown rule: {name!r}")
    return REGISTRY_BY_NAME[name]
