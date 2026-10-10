"""Chinese labels and form controls for the existing screening registry."""

from __future__ import annotations

import re
from collections.abc import Collection, Sequence
from typing import Any

from pydantic_core import PydanticUndefined

from rquant.llm.registry import REGISTRY, RuleSpec
from rquant.screen.intraday_contracts import INTRADAY_FIELD_LABELS
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
_FUNDAMENTAL_FIELDS = {
    "pe_ttm": ("PE_TTM[0]", "市盈率（倍）"),
    "pb": ("PB[0]", "市净率（倍）"),
    "dv_ttm": ("DV_TTM[0]", "股息率（%）"),
    "roe": ("ROE[0]", "净资产收益率（%）"),
    "or_yoy": ("OR_YOY[0]", "营收同比（%）"),
    "netprofit_yoy": ("NETPROFIT_YOY[0]", "归母净利同比（%）"),
}
_BOARD_OPTIONS = (("main", "沪深主板"), ("gem", "创业板"), ("star", "科创板"), ("bj", "北交所"))
_MA_OPTIONS = (
    ("MA5", "5 日均线"),
    ("MA10", "10 日均线"),
    ("MA20", "20 日均线"),
    ("MA60", "60 日均线"),
)
_MA_PERIOD_OPTIONS = tuple((value[2:], label) for value, label in _MA_OPTIONS)
_RSI_PERIOD_OPTIONS = (("6", "6 日 RSI"), ("14", "14 日 RSI"))
_MA_PERIOD_TEXT = re.compile(r"[1-9][0-9]*\Z")
_MA_NAME_TEXT = re.compile(r"MA([1-9][0-9]*)\Z")
_MA_FIELD_TEXT = re.compile(r"MA([1-9][0-9]{0,2})\[(0|[1-9][0-9]?)\]\Z")
_RSI_FIELD_TEXT = re.compile(r"RSI([1-9][0-9]?)\[(0|[1-9][0-9]?)\]\Z")
_DYNAMIC_MA_RULES = frozenset({"above_ma", "cross_above", "cross_below"})
_COMPARE_RULES = frozenset({"gt", "lt", "gte", "lte"})
RANKING_METRIC_LABELS = {
    "RETURN_20D_PCT[0]": "20 日涨幅",
    "TURNOVER_RATE[0]": "换手率",
    "CIRC_MV[0]": "流通市值",
    "PCT_CHG[0]": "今日涨跌幅",
    **INTRADAY_FIELD_LABELS,
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


def _parameter(
    spec: RuleSpec,
    key: str,
    *,
    dynamic_ma: bool,
    dynamic_rsi: bool,
    fundamental_fields: Collection[str],
    extra_fields: tuple[tuple[str, str], ...],
    daily_anchor: bool,
) -> ScreenParameter:
    field = spec.args_model.model_fields[key]
    prop = spec.args_model.model_json_schema()["properties"][key]
    options: list[ScreenOption] = []
    scale = 1
    hint: str | None = None
    custom_ma = dynamic_ma and (
        (spec.name in _COMPARE_RULES and key in {"left", "right"})
        or (spec.name == "between" and key == "field")
    )
    custom_rsi = dynamic_rsi and (
        (spec.name in _COMPARE_RULES and key in {"left", "right"})
        or (spec.name == "between" and key == "field")
    )
    fields = (
        *((key, "上个交易日" + label if daily_anchor else label) for key, label in _FIELDS),
        *(_FUNDAMENTAL_FIELDS[name] for name in _FUNDAMENTAL_FIELDS if name in fundamental_fields),
        *extra_fields,
    )
    if key == "boards":
        label, kind, options = "板块", "multi_choice", _options(_BOARD_OPTIONS)
    elif key in {"fast", "slow"}:
        if dynamic_ma:
            label, kind = ("快线（日）" if key == "fast" else "慢线（日）"), "integer"
            hint = "可填 2–250 个交易日"
        else:
            label, kind, options = (
                "快线" if key == "fast" else "慢线",
                "choice",
                _options(_MA_OPTIONS),
            )
    elif key == "field":
        label, kind, options = "比较项", "field", _options(fields)
        if custom_rsi:
            hint = "可自定义均线或 RSI；相对日期 0–30 日"
        elif custom_ma:
            hint = "可自定义均线；相对日期 0–30 日"
    elif key in {"left", "right"}:
        label, kind, options = ("左侧" if key == "left" else "右侧"), "operand", _options(fields)
        hint = (
            "可选数据项、固定数字或自定义均线与 RSI；相对日期 0–30 日"
            if custom_rsi
            else "可选数据项、固定数字或自定义均线；相对日期 0–30 日"
            if custom_ma
            else "可选数据项，也可输入固定数字"
        )
    elif key == "offset":
        label, kind = "相对日期", "integer"
        hint = (
            "0 为所选交易日，最多往前 30 个交易日"
            if (dynamic_ma and spec.name in _DYNAMIC_MA_RULES)
            or (dynamic_rsi and spec.name in {"rsi_oversold", "rsi_overbought"})
            else "0 为所选交易日，1 为前一交易日"
        )
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
        if dynamic_ma and spec.name == "above_ma":
            label, kind = "均线周期（日）", "integer"
            hint = "可填 2–250 个交易日"
        elif dynamic_rsi and spec.name in {"rsi_oversold", "rsi_overbought"}:
            label, kind = "RSI 周期（日）", "integer"
            hint = "可填 2–60 个交易日"
        else:
            label, kind, options = (
                "指标周期",
                "choice",
                _options(_MA_PERIOD_OPTIONS if spec.name == "above_ma" else _RSI_PERIOD_OPTIONS),
            )
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
    if dynamic_ma and key in {"fast", "slow"}:
        initial = int(initial[2:])
    if (
        key == "period"
        and initial is not None
        and not (
            (dynamic_ma and spec.name == "above_ma")
            or (dynamic_rsi and spec.name in {"rsi_oversold", "rsi_overbought"})
        )
    ):
        initial = str(initial)
    minimum = prop.get("minimum", prop.get("exclusiveMinimum"))
    maximum = prop.get("maximum", prop.get("exclusiveMaximum"))
    if dynamic_ma and (key in {"fast", "slow"} or (spec.name == "above_ma" and key == "period")):
        minimum, maximum = 2, 250
    if dynamic_rsi and spec.name in {"rsi_oversold", "rsi_overbought"} and key == "period":
        minimum, maximum = 2, 60
    return ScreenParameter(
        key=key,
        label=label,
        input=kind,
        initial=initial,
        required=field.is_required(),
        minimum=minimum,
        maximum=maximum,
        scale=scale,
        options=options,
        hint=hint,
        custom_ma=custom_ma,
    )


def screen_blocks(
    *,
    dynamic_ma: bool = False,
    dynamic_rsi: bool = False,
    fundamental_fields: Collection[str] = (),
    extra_fields: tuple[tuple[str, str], ...] = (),
    daily_anchor: bool = False,
) -> list[ScreenBlock]:
    if set(_RULE_COPY) != {spec.name for spec in REGISTRY}:
        raise ValueError("screen rule catalog labels are out of sync with registry")
    return [
        ScreenBlock(
            key=spec.name,
            label=_RULE_COPY[spec.name][0],
            hint=_RULE_COPY[spec.name][1] + ("；日线条件使用上个已收盘交易日" if daily_anchor else ""),
            category=spec.category,
            category_label=_CATEGORIES[spec.category],
            parameters=[
                _parameter(
                    spec,
                    name,
                    dynamic_ma=dynamic_ma,
                    dynamic_rsi=dynamic_rsi,
                    fundamental_fields=fundamental_fields,
                    extra_fields=extra_fields,
                    daily_anchor=daily_anchor,
                )
                for name in spec.args_model.model_fields
            ],
        )
        for spec in REGISTRY
    ]


def _dynamic_period(value: object, *, named: bool) -> int:
    if type(value) is int:
        period = value
    elif type(value) is str:
        pattern = _MA_NAME_TEXT if named else _MA_PERIOD_TEXT
        match = pattern.fullmatch(value)
        period = int(match.group(1) if named else value) if match else 0
    else:
        period = 0
    if not 2 <= period <= 250:
        raise ValueError("screen indicator period is not yet available")
    return period


def _valid_custom_ma_field(value: object) -> bool:
    if type(value) is not str:
        return False
    match = _MA_FIELD_TEXT.fullmatch(value)
    return match is not None and 2 <= int(match.group(1)) <= 250 and int(match.group(2)) <= 30


def _valid_custom_rsi_field(value: object) -> bool:
    if type(value) is not str:
        return False
    match = _RSI_FIELD_TEXT.fullmatch(value)
    return match is not None and 2 <= int(match.group(1)) <= 60 and int(match.group(2)) <= 30


def validate_screen_choices(
    conditions: Sequence[ScreenCondition],
    *,
    dynamic_ma: bool = False,
    dynamic_rsi: bool = False,
    fundamental_fields: Collection[str] = (),
    extra_fields: tuple[tuple[str, str], ...] = (),
) -> list[dict[str, Any]]:
    """Accept only offered choices and normalize replica MA periods for the registry."""

    blocks = {
        block.key: block
        for block in screen_blocks(
            dynamic_ma=dynamic_ma,
            dynamic_rsi=dynamic_rsi,
            fundamental_fields=fundamental_fields,
            extra_fields=extra_fields,
        )
    }
    normalized: list[dict[str, Any]] = []
    for condition in conditions:
        args = dict(condition.args)
        normalized.append(args)
        block = blocks.get(condition.key)
        if block is None:
            continue  # The registry compiler reports the unknown rule.
        for parameter in block.parameters:
            if parameter.key not in condition.args:
                continue
            value = condition.args[parameter.key]
            if dynamic_ma and condition.key in _DYNAMIC_MA_RULES:
                if parameter.key in {"fast", "slow"}:
                    args[parameter.key] = f"MA{_dynamic_period(value, named=True)}"
                    continue
                if condition.key == "above_ma" and parameter.key == "period":
                    args[parameter.key] = _dynamic_period(value, named=False)
                    continue
            if (
                dynamic_rsi
                and condition.key in {"rsi_oversold", "rsi_overbought"}
                and parameter.key == "period"
            ):
                if type(value) is not int or not 2 <= value <= 60:
                    raise ValueError("screen RSI period is not available")
                continue
            choices = {option.value for option in parameter.options}
            if parameter.input in {"choice", "field"}:
                valid = (
                    type(value) in {str, int} and str(value) in choices
                    if parameter.key == "period"
                    else type(value) is str and value in choices
                )
                if parameter.custom_ma:
                    valid = valid or _valid_custom_ma_field(value)
                if dynamic_rsi and condition.key in _COMPARE_RULES | {"between"}:
                    valid = valid or _valid_custom_rsi_field(value)
            elif parameter.input == "operand":
                valid = type(value) in {int, float} or (type(value) is str and value in choices)
                if parameter.custom_ma:
                    valid = valid or _valid_custom_ma_field(value)
                if dynamic_rsi and condition.key in _COMPARE_RULES:
                    valid = valid or _valid_custom_rsi_field(value)
            elif parameter.input == "multi_choice":
                valid = type(value) is list and all(
                    type(item) is str and item in choices for item in value
                )
            else:
                continue
            if not valid:
                if parameter.key == "period":
                    raise ValueError("screen indicator period is not yet available")
                if parameter.custom_ma or (
                    dynamic_rsi and condition.key in _COMPARE_RULES | {"between"}
                ):
                    raise ValueError("screen custom indicator field is not available")
                raise ValueError("screen form choice is not listed in the catalog")
    return normalized
