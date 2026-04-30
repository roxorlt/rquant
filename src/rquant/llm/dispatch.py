"""ScreenPlan → list[Rule] → screen() 桥。"""

from __future__ import annotations

import pandas as pd

from rquant.llm.registry import get_rule_spec
from rquant.llm.schemas import ScreenPlan
from rquant.screen.core import screen
from rquant.screen.rules import Rule
from rquant.storage.duckdb import DuckDBStore


def build_rules(plan: ScreenPlan) -> list[Rule]:
    """将 plan.stages 平铺并按 name 查 RuleSpec → 用 args_model 校验 → 实例化 Rule。"""
    rules: list[Rule] = []
    for rc in plan.flatten_rules():
        spec = get_rule_spec(rc.name)  # 不存在 → ValueError
        validated = spec.args_model.model_validate(rc.args)  # 不合法 → ValidationError
        kwargs = validated.model_dump(exclude_unset=False)
        rules.append(spec.fn(**kwargs))
    return rules


def screen_with_plan(
    plan: ScreenPlan,
    *,
    store: DuckDBStore | None = None,
) -> pd.DataFrame:
    """端到端：ScreenPlan → 实例化规则 → screen() 出表。"""
    rules = build_rules(plan)
    return screen(
        trade_date=plan.trade_date,
        rules=rules,
        include_columns=plan.include_columns or None,
        store=store,
    )
