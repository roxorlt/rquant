"""Week 7.5 C.1：user pool 的 JSON 持久化。

格式跟 v0.12.0 nl_screen.py 写出的 user_presets/*.json 兼容（presets.py:load_user_presets
负责加载），canvas 这边只做 read raw / write back。

不处理 builtin pool（n-shape-pool1 / pool2）—— 它们的 rules 是闭包，反查不出 RuleCall
args；编辑能力推到后续 PR（给 builtin 加 rule_calls metadata，或者 fork to user/）。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from rquant.config import settings
from rquant.llm.schemas import RuleCall


USER_PREFIX = "user/"


def is_user_pool(preset_full_name: str) -> bool:
    return preset_full_name.startswith(USER_PREFIX)


def base_name_of(preset_full_name: str) -> str:
    """user/foo → foo；其他原样返回。"""
    if preset_full_name.startswith(USER_PREFIX):
        return preset_full_name[len(USER_PREFIX) :]
    return preset_full_name


def user_pool_path(base_name: str) -> Path:
    return Path(settings.data_dir) / "user_presets" / f"{base_name}.json"


def load_user_pool_raw(base_name: str) -> dict[str, Any] | None:
    """从 user_presets/<base>.json 读 raw dict。文件不存在返回 None。"""
    path = user_pool_path(base_name)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_user_pool_rule_calls(base_name: str) -> list[RuleCall]:
    """从文件取 rules 列表，转 RuleCall。文件不存在返回 []。"""
    raw = load_user_pool_raw(base_name)
    if not raw:
        return []
    return [RuleCall.model_validate(r) for r in raw.get("rules", [])]


def save_user_pool(
    base_name: str,
    *,
    description: str,
    rule_calls: list[RuleCall],
    include_columns: list[str],
    source: str = "canvas_edit",
) -> Path:
    """覆盖写 user_presets/<base>.json。"""
    path = user_pool_path(base_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": base_name,
        "description": description,
        "rules": [rc.model_dump() for rc in rule_calls],
        "include_columns": include_columns,
        "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def fork_builtin_to_user(builtin_name: str, target_base_name: str | None = None) -> Path:
    """C.4：把 builtin pool fork 成 user pool（写 user_presets/<base>.json）。

    依赖 builtin preset 已经填了 rule_calls（presets.py 维护）。
    target_base_name 默认 = builtin_name。
    若同名 user pool 已存在 → 抛 FileExistsError，避免无意覆盖。
    """
    # 局部 import 避开循环（canvas_persistence ← presets）
    from rquant.presets import PRESET_SCREENS

    if builtin_name not in PRESET_SCREENS:
        raise KeyError(f"未知 preset：{builtin_name}")
    preset = PRESET_SCREENS[builtin_name]
    if not preset.rule_calls:
        raise ValueError(
            f"{builtin_name} 没有 rule_calls 元数据，无法 fork（builtin 需要维护 rule_calls）"
        )

    base = target_base_name or builtin_name
    path = user_pool_path(base)
    if path.exists():
        raise FileExistsError(f"user/{base} 已存在：{path}（先删或换个名字再 fork）")

    return save_user_pool(
        base,
        description=f"Fork from builtin/{builtin_name}：{preset.description}",
        rule_calls=preset.rule_calls,
        include_columns=preset.include_columns,
        source="fork_from_builtin",
    )
