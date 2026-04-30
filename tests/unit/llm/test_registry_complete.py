"""校验 REGISTRY 与 screen.__all__ 中可调用积木一一对应。
未注册的积木 = LLM 看不到 = 用户用不到，必须被发现并修复。
"""

import rquant.screen as screen_pkg
from rquant.llm.registry import REGISTRY_BY_NAME

# 跳过非积木导出（基础设施 / 类型）
NON_RULE_EXPORTS = {"screen", "load_universe", "AggregateRequest"}


def _all_rule_names_in_screen() -> set[str]:
    """从 screen.__all__ 取所有公开的积木名。"""
    return set(screen_pkg.__all__) - NON_RULE_EXPORTS


def test_every_screen_rule_is_registered() -> None:
    screen_rules = _all_rule_names_in_screen()
    registered = set(REGISTRY_BY_NAME.keys())
    missing = screen_rules - registered
    assert not missing, f"未注册的积木：{sorted(missing)}"


def test_every_registered_rule_exists_in_screen() -> None:
    screen_rules = _all_rule_names_in_screen()
    registered = set(REGISTRY_BY_NAME.keys())
    extra = registered - screen_rules
    assert not extra, f"REGISTRY 中存在 screen 未导出的规则：{sorted(extra)}"
