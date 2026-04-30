"""LLM 集成层：NL → ScreenPlan → screen()。"""

from rquant.llm.client import DeepSeekClient, LLMClarificationNeeded, LLMError
from rquant.llm.dispatch import build_rules, screen_with_plan
from rquant.llm.registry import REGISTRY, REGISTRY_BY_NAME, RuleSpec, get_rule_spec
from rquant.llm.schemas import RuleCall, ScreenPlan, Stage

__all__ = [
    "DeepSeekClient", "LLMClarificationNeeded", "LLMError",
    "RuleCall", "Stage", "ScreenPlan",
    "REGISTRY", "REGISTRY_BY_NAME", "RuleSpec", "get_rule_spec",
    "build_rules", "screen_with_plan",
]
