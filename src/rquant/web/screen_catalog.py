"""Chinese labels and form controls for the existing screening registry."""

from __future__ import annotations

from collections.abc import Collection, Sequence
from typing import Any

from pydantic_core import PydanticUndefined

from rquant.llm.registry import REGISTRY, RuleSpec
from rquant.web.models.screen import (
    ScreenBlock,
    ScreenCondition,
    ScreenOption,
    ScreenParameter,
)

_CATEGORIES = {
    "filter": "股票范围",
    "state": "涨跌状态",
    "indicator": "指标",
    "shape": "K 线形态",
    "aggregate": "近期表现",
    "compare": "数值比较",
}

_RULE_COPY = {
    "not_st": ("排除 ST", "剔除名称带 ST 的股票"),
    "not_bj": ("排除北交所", "只看沪深交易所的股票"),
    "circ_mv_lt": ("流通市值低于", "筛出流通市值小于指定金额的股票"),
    "first_limit_up": ("首板", "指定交易日首次涨停"),
    "not_limit_up": ("未涨停", "指定交易日没有涨停"),
    "board_in": ("所属板块", "只保留选中的市场板块"),
    "limit_up": ("涨停", "指定交易日涨停"),
    "limit_down": ("跌停", "指定交易日跌停"),
    "yiziban": ("一字板", "指定交易日开盘即封板"),
    "not_yiziban": ("非一字板", "指定交易日不是一字板"),
    "consecutive_ups_gte": ("连板不少于", "指定交易日达到所填连板数"),
    "has_lower_shadow": ("明显下影线", "下影线和日振幅同时达到门槛"),
    "gt": ("大于", "比较两项数据，左边大于右边"),
    "lt": ("小于", "比较两项数据，左边小于右边"),
    "gte": ("大于或等于", "比较两项数据，左边不小于右边"),
    "lte": ("小于或等于", "比较两项数据，左边不大于右边"),
    "between": ("落在区间", "指定数据位于上下限之间"),
    "cross_above": ("均线上穿", "短期均线由下向上穿过长期均线"),
    "cross_below": ("均线下穿", "短期均线由上向下穿过长期均线"),
    "above_ma": ("收盘价高于均线", "收盘价高于指定周期的均线"),
    "rsi_oversold": ("RSI 超卖", "RSI 低于指定值"),
    "rsi_overbought": ("RSI 超买", "RSI 高于指定值"),
    "volume_ratio_gte": ("成交量放大", "成交量达到近期平均值的指定倍数"),
    "no_consec_ups_in_window": ("近期无高连板", "回看区间内没有达到指定连板数"),
    "no_limit_down_in_window": ("近期无跌停", "回看区间内没有跌停"),
    "has_prior_limit_up": ("近期曾涨停", "回看区间内至少出现过一次涨停"),
}

_FIELDS = (
    ("CLOSE[0]", "收盘价"),
    ("PCT_CHG[0]", "涨跌幅"),
    ("OPEN[0]", "开盘价"),
    ("HIGH[0]", "最高价"),
    ("LOW[0]", "最低价"),
    ("VOL[0]", "成交量"),
    ("CIRC_MV[0]", "流通市值"),
    ("TURNOVER_RATE[0]", "换手率"),
    ("MA5[0]", "5 日均线"),
    ("MA10[0]", "10 日均线"),
    ("MA20[0]", "20 日均线"),
    ("MA60[0]", "60 日均线"),
    ("RSI14[0]", "14 日 RSI"),
)
_BOARD_OPTIONS = (("main", "沪深主板"), ("gem", "创业板"), ("star", "科创板"), ("bj", "北交所"))
_MA_OPTIONS = (
    ("MA5", "5 日均线"),
    ("MA10", "10 日均线"),
    ("MA20", "20 日均线"),
    ("MA60", "60 日均线"),
)
RANKING_METRIC_LABELS = {
    "RETURN_20D_PCT[0]": "20 日涨幅",
    "TURNOVER_RATE[0]": "换手率",
    "CIRC_MV[0]": "流通市值",
    "PCT_CHG[0]": "今日涨跌幅",
}


def available_ranking_metrics(columns: Collection[str]) -> list[ScreenOption]:
    return [
        ScreenOption(value=key, label=label)
        for key, label in RANKING_METRIC_LABELS.items()
        if key in columns
    ]
_INITIALS: dict[str, dict[str, Any]] = {
    "circ_mv_lt": {"threshold_yi": 100},
    "board_in": {"boards": ["main"]},
    "consecutive_ups_gte": {"n": 2},
    "gt": {"left": "CLOSE[0]", "right": "MA5[0]"},
    "lt": {"left": "CLOSE[0]", "right": "MA5[0]"},
    "gte": {"left": "CLOSE[0]", "right": "MA5[0]"},
    "lte": {"left": "CLOSE[0]", "right": "MA5[0]"},
    "between": {"field": "PCT_CHG[0]", "low": 0, "high": 5},
    "cross_above": {"fast": "MA5", "slow": "MA20"},
    "cross_below": {"fast": "MA5", "slow": "MA20"},
    "above_ma": {"period": 20},
    "rsi_oversold": {"threshold": 30},
    "rsi_overbought": {"threshold": 70},
    "volume_ratio_gte": {"n": 2},
}


def _options(rows: tuple[tuple[str, str], ...]) -> list[ScreenOption]:
    return [ScreenOption(value=key, label=label) for key, label in rows]


def _parameter(spec: RuleSpec, key: str) -> ScreenParameter:
    field = spec.args_model.model_fields[key]
    prop = spec.args_model.model_json_schema()["properties"][key]
    options: list[ScreenOption] = []
    scale = 1
    hint: str | None = None
    if key == "boards":
        label, kind, options = "板块", "multi_choice", _options(_BOARD_OPTIONS)
    elif key in {"fast", "slow"}:
        label, kind, options = (
            "快线" if key == "fast" else "慢线",
            "choice",
            _options(_MA_OPTIONS),
        )
    elif key == "field":
        label, kind, options = "比较项", "field", _options(_FIELDS)
    elif key in {"left", "right"}:
        label, kind, options = ("左侧" if key == "left" else "右侧"), "operand", _options(_FIELDS)
        hint = "可选数据项，也可输入固定数字"
    elif key == "offset":
        label, kind = "相对日期", "integer"
        hint = "0 为所选交易日，1 为前一交易日"
    elif key == "threshold_yi":
        label, kind = "市值上限（亿元）", "number"
    elif key == "min_ratio":
        label, kind = "下影线倍数", "number"
    elif key == "min_amplitude":
        label, kind, scale = "最小振幅（%）", "number", 100
    elif key == "threshold":
        label, kind = (
            "连板下限" if spec.name == "no_consec_ups_in_window" else "RSI 门槛",
            "number",
        )
    elif key == "window":
        label, kind = "回看交易日", "integer"
    elif key == "exclude_offset":
        label, kind = "排除前几日", "integer"
    elif key == "period":
        label, kind = "指标周期", "integer"
    elif key == "n":
        label, kind = ("放量倍数" if spec.name == "volume_ratio_gte" else "连板下限"), "number"
    elif key == "low":
        label, kind = "下限", "number"
    elif key == "high":
        label, kind = "上限", "number"
    else:
        raise ValueError(f"unmapped rule parameter: {spec.name}.{key}")
    initial = _INITIALS.get(spec.name, {}).get(key)
    if initial is None and field.default is not PydanticUndefined:
        initial = field.default
    return ScreenParameter(
        key=key,
        label=label,
        input=kind,
        initial=initial,
        required=field.is_required(),
        minimum=prop.get("minimum", prop.get("exclusiveMinimum")),
        maximum=prop.get("maximum", prop.get("exclusiveMaximum")),
        scale=scale,
        options=options,
        hint=hint,
    )


def screen_blocks() -> list[ScreenBlock]:
    if set(_RULE_COPY) != {spec.name for spec in REGISTRY}:
        raise ValueError("screen rule catalog labels are out of sync with registry")
    return [
        ScreenBlock(
            key=spec.name,
            label=_RULE_COPY[spec.name][0],
            hint=_RULE_COPY[spec.name][1],
            category=spec.category,
            category_label=_CATEGORIES[spec.category],
            parameters=[_parameter(spec, name) for name in spec.args_model.model_fields],
        )
        for spec in REGISTRY
    ]


def validate_screen_choices(conditions: Sequence[ScreenCondition]) -> None:
    """Accept only the numeric data items and choices this UI offers."""

    blocks = {block.key: block for block in screen_blocks()}
    for condition in conditions:
        block = blocks.get(condition.key)
        if block is None:
            continue  # The registry compiler reports the unknown rule.
        for parameter in block.parameters:
            if parameter.key not in condition.args:
                continue
            value = condition.args[parameter.key]
            choices = {option.value for option in parameter.options}
            if parameter.input in {"choice", "field"}:
                valid = type(value) is str and value in choices
            elif parameter.input == "operand":
                valid = type(value) in {int, float} or (type(value) is str and value in choices)
            elif parameter.input == "multi_choice":
                valid = type(value) is list and all(
                    type(item) is str and item in choices for item in value
                )
            else:
                continue
            if not valid:
                raise ValueError("screen form choice is not listed in the catalog")
