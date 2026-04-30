"""把 REGISTRY 转成 OpenAI Tool Calls schema + LLM 可读的规则目录 markdown。"""

from __future__ import annotations

import json

from rquant.llm.registry import REGISTRY, RuleSpec

# build_screen 是 LLM 唯一可调的工具；rule.name 是 enum 强约束，
# rule.args 是 free-form dict（详细 schema 通过 system prompt 中的目录传给 LLM）。
_BUILD_SCREEN_DESCRIPTION = (
    "构建 A 股选股方案。stages 是有序分层（基础过滤→形态→时机→指标），"
    "每个 stage 含 label（中文）+ rules。所有 rule 跨 stage AND 合取。"
    "rule.name 必须是预定义积木，rule.args 须严格匹配该积木的参数 schema（见 system 提示中的目录）。"
)


def to_openai_tools() -> list[dict]:
    """生成 OpenAI Tool Calls 兼容的 tools list。仅一个 build_screen tool。"""
    rule_names = [spec.name for spec in REGISTRY]

    schema = {
        "type": "object",
        "properties": {
            "trade_date": {
                "type": "string",
                "description": "YYYY-MM-DD；缺失时由调用方填默认值（最新交易日）",
            },
            "stages": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string", "minLength": 1,
                                  "description": "中文分组名（基础过滤 / 形态 / 时机 等）"},
                        "rules": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "enum": rule_names,
                                        "description": "积木名；必须是 enum 中的一个",
                                    },
                                    "args": {
                                        "type": "object",
                                        "description": "积木参数；按系统提示中的 args schema 填",
                                    },
                                },
                                "required": ["name"],
                                "additionalProperties": False,
                            },
                        },
                    },
                    "required": ["label", "rules"],
                    "additionalProperties": False,
                },
            },
            "include_columns": {
                "type": "array",
                "items": {"type": "string"},
                "description": "结果中额外展示的字段（如 'CIRC_MV[0]', 'CONSECUTIVE_LIMIT_UPS[1]'）",
            },
            "rationale": {
                "type": "string",
                "description": "用一两句话说明你为什么这样组合规则",
            },
        },
        "required": ["trade_date", "stages"],
        "additionalProperties": False,
    }

    return [{
        "type": "function",
        "function": {
            "name": "build_screen",
            "description": _BUILD_SCREEN_DESCRIPTION,
            "parameters": schema,
        },
    }]


def build_rule_catalog_md() -> str:
    """生成给 LLM 看的规则目录 markdown，按 category 分组，含每条 args schema。"""
    by_cat: dict[str, list[RuleSpec]] = {}
    for spec in REGISTRY:
        by_cat.setdefault(spec.category, []).append(spec)

    cat_order = ["filter", "state", "shape", "indicator", "aggregate", "compare"]
    cat_titles = {
        "filter": "FILTER（板块/属性过滤）",
        "state": "STATE（涨跌停状态）",
        "shape": "SHAPE（K线形态）",
        "indicator": "INDICATOR（技术指标）",
        "aggregate": "AGGREGATE（历史窗口聚合）",
        "compare": "COMPARE（通用比较，操作数为字段名或数字）",
    }

    out: list[str] = []
    for cat in cat_order:
        if cat not in by_cat:
            continue
        out.append(f"\n## {cat_titles[cat]}\n")
        for spec in by_cat[cat]:
            args_schema = spec.args_model.model_json_schema()
            args_brief = _summarize_args(args_schema)
            out.append(f"- **{spec.name}**({args_brief}) — {spec.description}")
            if spec.examples:
                out.append(f"  · 示例：{' | '.join(spec.examples)}")
            # 完整 args schema（紧凑 JSON，供 LLM 校验参数类型/范围）
            out.append(f"  · args schema: `{json.dumps(args_schema, ensure_ascii=False, separators=(',', ':'))}`")
    return "\n".join(out)


def _summarize_args(schema: dict) -> str:
    """从 JSON schema 抽出参数签名简写，如 'offset: int=0'。"""
    props = schema.get("properties", {})
    if not props:
        return ""
    parts: list[str] = []
    for k, v in props.items():
        t = v.get("type", "any")
        if "default" in v:
            parts.append(f"{k}:{t}={v['default']}")
        else:
            parts.append(f"{k}:{t}")
    return ", ".join(parts)
