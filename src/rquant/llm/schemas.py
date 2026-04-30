"""LLM 解析产出的结构化数据模型。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RuleCall(BaseModel):
    """单条积木调用。name 必须存在于 REGISTRY，args 由各 RuleSpec.args_model 校验。"""

    name: str = Field(..., min_length=1)
    args: dict[str, Any] = Field(default_factory=dict)


class Stage(BaseModel):
    """筛选 pipeline 一层：用户友好分组（基础过滤 / 形态 / 时机 等）。
    内部规则 AND 合取，跨 stage 也是 AND——分组只为 UI 渲染和认知组织。
    """

    label: str = Field(..., min_length=1)
    rules: list[RuleCall] = Field(default_factory=list)


class ScreenPlan(BaseModel):
    """LLM 解析的完整选股方案。

    Week 7.5 升级到真画布时此结构兼容扩展（stages → DAG nodes）。
    """

    trade_date: str = Field(..., min_length=10, max_length=10,
                            description="YYYY-MM-DD")
    stages: list[Stage] = Field(default_factory=list)
    include_columns: list[str] = Field(default_factory=list)
    rationale: str = Field(default="")

    def flatten_rules(self) -> list[RuleCall]:
        """跨 stage 合并所有规则；语义上等价于 list of rules（AND 合取）。"""
        return [rc for s in self.stages for rc in s.rules]
