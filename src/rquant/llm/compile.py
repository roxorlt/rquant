"""Pure compilation of a structured screening plan into existing rule functions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rquant.llm.registry import get_rule_spec
from rquant.llm.schemas import ScreenPlan
from rquant.screen.rules import Rule


@dataclass(frozen=True)
class CompiledScreenPlan:
    rules: list[Rule]
    normalized_plan: dict[str, Any]


def compile_screen_plan(plan: ScreenPlan) -> CompiledScreenPlan:
    """Validate registry calls once and keep a canonical cursor-bound plan."""

    rules: list[Rule] = []
    calls: list[dict[str, Any]] = []
    for call in plan.flatten_rules():
        spec = get_rule_spec(call.name)
        args = spec.args_model.model_validate(call.args).model_dump(mode="json")
        rules.append(spec.fn(**args))
        calls.append({"name": call.name, "args": args})
    return CompiledScreenPlan(
        rules=rules,
        normalized_plan={"trade_date": plan.trade_date, "conditions": calls},
    )
