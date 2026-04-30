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
]


REGISTRY_BY_NAME: dict[str, RuleSpec] = {spec.name: spec for spec in REGISTRY}


def get_rule_spec(name: str) -> RuleSpec:
    """按 name 取 RuleSpec；未注册抛 ValueError。"""
    if name not in REGISTRY_BY_NAME:
        raise ValueError(f"Unknown rule: {name!r}")
    return REGISTRY_BY_NAME[name]
