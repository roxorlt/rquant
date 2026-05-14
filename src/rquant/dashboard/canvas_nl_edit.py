"""Week 7.5 C.2：NL 改 pool 的辅助工具（解析 + diff）。

主要职责：
- nl_edit_pool(query, current): 调 DeepSeek 用编辑 prompt → 拿到完整新 RuleCall list
- diff_rule_calls(old, new): 算 added / removed / 大致一致 三类（用归一化 args 防类型差异）
"""

from __future__ import annotations

from rquant.config import settings
from rquant.llm.client import DeepSeekClient
from rquant.llm.registry import get_rule_spec
from rquant.llm.schemas import RuleCall


def _normalize(rc: RuleCall) -> tuple[str, tuple]:
    """RuleCall → hashable (name, sorted args items) 用于 set 比较。
    args 类型用 args_model 校验+dump 归一化，规避 int/float 差异。"""
    try:
        spec = get_rule_spec(rc.name)
        normalized_args = spec.args_model.model_validate(rc.args).model_dump()
    except Exception:
        normalized_args = rc.args
    # tuple of sorted items；value 是 hashable
    return (rc.name, tuple(sorted(normalized_args.items())))


def diff_rule_calls(
    old: list[RuleCall],
    new: list[RuleCall],
) -> tuple[list[RuleCall], list[RuleCall], list[RuleCall]]:
    """对比新旧 RuleCall 列表，返回 (added, removed, unchanged)。

    - 顺序变化视作 unchanged（不算 added/removed）
    - args 类型差异（int 150 vs float 150.0）走归一化，不算 diff
    - 没做 args 部分修改的"~改"检测，view 上 added+removed 各显示一次
    """
    old_keys = {_normalize(rc) for rc in old}
    new_keys = {_normalize(rc) for rc in new}

    added = [rc for rc in new if _normalize(rc) not in old_keys]
    removed = [rc for rc in old if _normalize(rc) not in new_keys]
    unchanged = [rc for rc in old if _normalize(rc) in new_keys]

    return added, removed, unchanged


def nl_edit_pool(query: str, current: list[RuleCall], *, today: str) -> list[RuleCall]:
    """调 DeepSeek 用编辑 prompt 把 query + current → 完整新 RuleCall 列表。

    Raises:
        LLMClarificationNeeded：query 含糊，LLM 让用户澄清
        LLMError / ValidationError：API 失败 / schema 不通过
    """
    if not settings.deepseek_enabled:
        raise RuntimeError("DEEPSEEK_API_KEY 未配置，无法用 NL 改 pool")
    client = DeepSeekClient(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
        model=settings.deepseek_model,
    )
    plan = client.nl_to_screen_plan(query, today=today, current_rule_calls=current)
    return plan.flatten_rules()
