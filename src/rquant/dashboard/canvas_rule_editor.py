"""Week 7.5 C.1：基于 RuleSpec.args_model（Pydantic）反射出 inline 编辑 widget。

支持的字段类型（v1）：
  - int / float → st.number_input
  - str → st.text_input
  - bool → st.checkbox
  - 其他 → st.text_input + JSON 序列化（fallback，不推荐编辑复杂参数）

不支持：list / dict / 嵌套 Pydantic 模型（C.1 范围内规则参数都是标量）。
"""

from __future__ import annotations

from typing import Any

import streamlit as st
from pydantic import BaseModel

from rquant.llm.registry import REGISTRY, RuleSpec, get_rule_spec


def _coerce_default(field_info, current: Any) -> Any:
    """current 优先；否则用 args_model 字段默认值；否则按类型给零值。"""
    if current is not None:
        return current
    if field_info.default is not None and field_info.default is not ...:
        return field_info.default
    ann = field_info.annotation
    if ann in (int, float):
        return 0
    if ann is bool:
        return False
    return ""


def render_args_form(
    rule_name: str,
    current_args: dict[str, Any],
    key_prefix: str,
) -> dict[str, Any] | None:
    """渲染 args 编辑表单。返回 new args dict（用户改的）或 None（rule_name 未注册）。

    每个 input 的 streamlit key 用 `{key_prefix}_{field}` 隔离，避免规则间串值。
    """
    try:
        spec: RuleSpec = get_rule_spec(rule_name)
    except ValueError:
        st.error(f"未知 rule: `{rule_name}`")
        return None

    new_args: dict[str, Any] = {}
    model_cls: type[BaseModel] = spec.args_model
    for field_name, field_info in model_cls.model_fields.items():
        widget_key = f"{key_prefix}_{field_name}"
        default_val = _coerce_default(field_info, current_args.get(field_name))
        ann = field_info.annotation
        label = f"{field_name}"

        if ann is int:
            new_args[field_name] = st.number_input(
                label, value=int(default_val), step=1, key=widget_key
            )
        elif ann is float:
            new_args[field_name] = st.number_input(
                label, value=float(default_val), step=0.01, format="%.4f", key=widget_key
            )
        elif ann is bool:
            new_args[field_name] = st.checkbox(label, value=bool(default_val), key=widget_key)
        elif ann is str:
            new_args[field_name] = st.text_input(label, value=str(default_val), key=widget_key)
        else:
            # fallback：当作字符串 + 不带类型校验（C.1 注册表里没有复杂类型）
            new_args[field_name] = st.text_input(
                label, value=str(default_val), key=widget_key,
                help=f"类型 {ann}，C.1 暂用 text input fallback",
            )

    return new_args


def rule_spec_options() -> list[tuple[str, str]]:
    """选规则下拉项：[(name, "name — description"), ...]，按 category 排序。"""
    items = sorted(REGISTRY, key=lambda s: (s.category, s.name))
    return [(s.name, f"{s.name} — {s.description}") for s in items]
