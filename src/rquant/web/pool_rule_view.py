"""Turn verified published pool rules into concise, readable API data."""

from __future__ import annotations

import json
import re
from decimal import Decimal
from functools import lru_cache
from typing import Any

from rquant.llm.registry import REGISTRY_BY_NAME
from rquant.screen.pool_ranking import PoolRankingPlan
from rquant.web.models.pools import (
    PoolDefinitionView,
    PoolRankingItem,
    PoolRankingView,
    PoolRuleItem,
    PoolRuleParameter,
)
from rquant.web.models.screen import ScreenBlock, ScreenParameter
from rquant.web.screen_catalog import screen_blocks

MAX_VISIBLE_RULES = 64

_REASONS = {
    "no_audit": "旧规则尚未完成迁移",
    "delete_conflict": "删除记录与规则文件不一致",
    "file_missing": "规则文件缺失",
    "invalid_content": "规则文件内容损坏",
    "command_invalid": "规则发布记录无效",
    "content_mismatch": "规则文件与发布记录不一致",
    "name_mismatch": "规则名称与文件不一致",
    "version_mismatch": "规则文件与发布版本不一致",
    "rules_invalid": "规则内容无效",
    "ranking_invalid": "排名设置无效",
    "dependency_invalid": "父池设置无效",
    "delay_invalid": "父池时段设置无效",
    "parent_missing": "父池不存在",
    "parent_unavailable": "父池规则不可用",
    "dependency_cycle": "池子之间存在循环依赖",
}
_FIELDS = {
    "OPEN": "开盘价",
    "HIGH": "最高价",
    "LOW": "最低价",
    "CLOSE": "收盘价",
    "PRE_CLOSE": "前收盘价",
    "PCT_CHG": "涨跌幅",
    "VOL": "成交量",
    "AMOUNT": "成交额",
    "CIRC_MV": "流通市值",
    "TOTAL_MV": "总市值",
    "TURNOVER_RATE": "换手率",
    "MA5": "5 日均线",
    "MA10": "10 日均线",
    "MA20": "20 日均线",
    "MA60": "60 日均线",
    "RSI6": "6 日 RSI",
    "RSI14": "14 日 RSI",
    "MACD": "MACD 值",
    "MACD_SIGNAL": "MACD 信号线",
    "MACD_HIST": "MACD 柱",
    "KDJ_K": "KDJ K 值",
    "KDJ_D": "KDJ D 值",
    "KDJ_J": "KDJ J 值",
    "BODY_UPPER": "实体上沿",
    "BODY_LOWER": "实体下沿",
    "IS_LIMIT_UP": "涨停状态",
    "IS_LIMIT_DOWN": "跌停状态",
    "IS_FIRST_LIMIT_UP": "首板状态",
    "IS_YIZIBAN": "一字板状态",
    "CONSECUTIVE_LIMIT_UPS": "连板数",
}
_RANKING_LABELS = {
    "RETURN_20D_PCT[0]": "20 日涨幅",
    "TURNOVER_RATE[0]": "换手率",
    "CIRC_MV[0]": "流通市值",
    "PCT_CHG[0]": "今日涨跌幅",
}
_FIELD_PATTERN = re.compile(r"([A-Z][A-Z0-9_]*)(?:\[(0|[1-9][0-9]*)\])?\Z")


@lru_cache(maxsize=1)
def _blocks() -> dict[str, ScreenBlock]:
    return {block.key: block for block in screen_blocks()}


def _number(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = Decimal(str(value))
    if not number.is_finite():
        return None
    rendered = f"{number:,f}"
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered == "-0" else rendered


def _day(offset: int) -> str:
    return "所选交易日" if offset == 0 else f"前 {offset} 个交易日"


def _field(value: str) -> str | None:
    value = value.strip()
    if re.fullmatch(r"-?\d+(?:\.\d+)?", value):
        return _number(float(value))
    matched = _FIELD_PATTERN.fullmatch(value)
    if matched is None:
        return None
    base, offset = matched.groups()
    label = _FIELDS.get(base)
    if label is None:
        return None
    if offset is None:
        return label
    separator = " " if label[0].isascii() else ""
    return f"{_day(int(offset))}{separator}{label}"


def _value(rule_name: str, parameter: ScreenParameter, raw: object) -> str | None:
    key = parameter.key
    choices = {option.value: option.label for option in parameter.options}
    if key == "boards":
        if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
            return None
        labels = [choices.get(item) for item in raw]
        return "、".join(label for label in labels if label is not None) if all(labels) else None
    if key in {"left", "right", "field", "fast", "slow"}:
        return _field(raw) if isinstance(raw, str) else _number(raw)
    if key in {"offset", "exclude_offset"}:
        return _day(raw) if type(raw) is int else None
    if key == "period" and type(raw) is int:
        return f"{raw} 日均线" if rule_name == "above_ma" else f"{raw} 日 RSI"
    if key == "window" and type(raw) is int:
        return f"{raw} 个交易日"
    number = _number(raw)
    if number is None:
        return None
    if key == "min_amplitude":
        scaled = _number(float(raw) * 100)
        return None if scaled is None else f"{scaled}%"
    if key == "threshold_yi":
        return f"{number} 亿元"
    if key == "n" and rule_name == "volume_ratio_gte":
        return f"{number} 倍"
    return number


def _rules(raw: object) -> list[PoolRuleItem]:
    if not isinstance(raw, list):
        raise ValueError("rules must be a list")
    result: list[PoolRuleItem] = []
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"name", "args"}:
            raise ValueError("rule shape is invalid")
        name = item["name"]
        if not isinstance(name, str) or not isinstance(item["args"], dict):
            raise ValueError("rule shape is invalid")
        spec = REGISTRY_BY_NAME.get(name)
        block = _blocks().get(name)
        if spec is None or block is None:
            raise ValueError("rule is unknown")
        values = spec.args_model.model_validate(item["args"]).model_dump()
        parameters: list[PoolRuleParameter] = []
        for parameter in block.parameters:
            value = _value(name, parameter, values[parameter.key])
            if value is None:
                raise ValueError("rule parameter has no readable value")
            label = parameter.label
            if parameter.key == "threshold_yi":
                label = label.removesuffix("（亿元）")
            elif parameter.key == "min_amplitude":
                label = label.removesuffix("（%）")
            parameters.append(PoolRuleParameter(label=label, value=value))
        result.append(PoolRuleItem(label=block.label, parameters=parameters))
    return result


def _ranking(raw: object) -> PoolRankingView | None:
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > 4_096:
        raise ValueError("ranking is unavailable")
    if raw == "null":
        return None
    plan = PoolRankingPlan.model_validate(json.loads(raw))
    return PoolRankingView(
        conditions=[
            PoolRankingItem(
                label=_RANKING_LABELS[condition.metric],
                direction_label="越低越好" if condition.ascending else "越高越好",
                weight=condition.weight,
            )
            for condition in plan.conditions
        ],
        top_n=plan.top_n,
    )


def _unavailable(
    name: str, source_label: str, reason: str, *, state: str = "unavailable"
) -> PoolDefinitionView:
    return PoolDefinitionView(
        name=name,
        state=state,
        status_label={
            "migration_required": "待迁移",
            "deleted": "已删除",
            "limit_exceeded": "规则过多",
        }.get(state, "暂不可查看"),
        reason_label=reason,
        source_label=source_label,
        description=None,
        depends_on=None,
        delay_label=None,
        rules=[],
    )


def pool_definition_view(row: dict[str, Any]) -> PoolDefinitionView:
    """Expose rule facts only for a readable, bounded available definition."""

    key = str(row["pool_name"])
    display_name = row.get("display_name")
    name = (
        display_name.strip()
        if isinstance(display_name, str) and display_name.strip()
        else key.removeprefix("user/")
    )
    source_label = "内置规则" if row.get("source_kind") == "builtin" else "自建规则"
    state = row.get("state")
    if state != "available":
        reason = _REASONS.get(str(row.get("reason")))
        if state == "migration_required":
            return _unavailable(name, source_label, reason or "旧规则尚未完成迁移", state=state)
        if state == "deleted":
            return _unavailable(name, source_label, "这份规则已删除", state=state)
        return _unavailable(name, source_label, reason or "规则源暂不可用")

    try:
        raw_rules = json.loads(row["rules_json"])
    except (TypeError, ValueError):
        return _unavailable(name, source_label, "规则内容损坏")
    if not isinstance(raw_rules, list):
        return _unavailable(name, source_label, "规则内容损坏")
    if len(raw_rules) > MAX_VISIBLE_RULES:
        return _unavailable(
            name, source_label, "规则超过展示上限，暂不可查看", state="limit_exceeded"
        )
    try:
        rendered = _rules(raw_rules)
    except (TypeError, ValueError):
        return _unavailable(name, source_label, "规则内容无法识别")
    ranking_json = row.get("ranking_json")
    try:
        ranking = None if ranking_json is None else _ranking(ranking_json)
    except (TypeError, ValueError, KeyError):
        return _unavailable(name, source_label, "排名设置损坏")

    depends_on = row.get("depends_on")
    delay_mode = row.get("delay_mode")
    delay_days = row.get("delay_days")
    if depends_on is None:
        if delay_mode != "none" or delay_days != 0:
            return _unavailable(name, source_label, "父池时段设置无效")
        delay_label = None
    elif isinstance(depends_on, str) and depends_on and type(delay_days) is int and delay_days > 0:
        if delay_mode == "exact":
            delay_label = f"使用父池恰好前 {delay_days} 个交易日的成员"
        elif delay_mode == "legacy_window":
            delay_label = f"使用父池前 {delay_days} 个交易日内的成员"
        else:
            return _unavailable(name, source_label, "父池时段设置无效")
    else:
        return _unavailable(name, source_label, "父池设置无效")

    description = row.get("description")
    return PoolDefinitionView(
        name=name,
        state="available",
        status_label="已发布",
        reason_label=None,
        source_label=source_label,
        description=description.strip()
        if isinstance(description, str) and description.strip()
        else None,
        depends_on=depends_on,
        delay_label=delay_label,
        rules=rendered,
        ranking=ranking,
    )
