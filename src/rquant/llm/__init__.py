"""LLM 集成层：NL → ScreenPlan → screen()。"""

from typing import Any

from rquant.llm.registry import REGISTRY, REGISTRY_BY_NAME, RuleSpec, get_rule_spec
from rquant.llm.schemas import RuleCall, ScreenPlan, Stage

__all__ = [
    "DeepSeekClient", "LLMClarificationNeeded", "LLMError",
    "RuleCall", "Stage", "ScreenPlan",
    "REGISTRY", "REGISTRY_BY_NAME", "RuleSpec", "get_rule_spec",
    "build_rules", "screen_with_plan",
]


def __getattr__(name: str) -> Any:
    # The read-only web API only needs the pure registry and schemas. Keep the
    # historical package exports without eagerly importing storage or the LLM client.
    if name in {"build_rules", "screen_with_plan"}:
        from rquant.llm import dispatch

        return getattr(dispatch, name)
    if name in {"DeepSeekClient", "LLMClarificationNeeded", "LLMError"}:
        from rquant.llm import client

        return getattr(client, name)
    raise AttributeError(name)
