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


def screen_with_plan_diagnostic(
    plan: ScreenPlan,
    *,
    store: DuckDBStore | None = None,
) -> tuple[pd.DataFrame, list[tuple[str, int]]]:
    """运行 plan 同时记录每条 rule 累加后的命中数（用于诊断）。

    返回 (final_result_df, [(rule_repr, hit_count_after_this_rule), ...]).
    若 final_result_df 为空，可看每条 rule 的累加命中数判断哪条筛空。
    """
    rules = build_rules(plan)
    if not rules:
        return screen(plan.trade_date, [], store=store), []

    final = screen(plan.trade_date, rules, store=store)

    diagnostics: list[tuple[str, int]] = []
    rule_calls = plan.flatten_rules()
    for n in range(1, len(rules) + 1):
        partial = screen(plan.trade_date, rules[:n], store=store)
        rc = rule_calls[n - 1]
        args_str = ", ".join(f"{k}={v!r}" for k, v in rc.args.items())
        rule_repr = f"{rc.name}({args_str})" if args_str else f"{rc.name}()"
        diagnostics.append((rule_repr, len(partial)))

    return final, diagnostics
