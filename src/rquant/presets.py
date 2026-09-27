"""筛选预设注册表：每个 ScreenPreset 是一套命名的规则组合。"""

from __future__ import annotations

import json as _json
from pathlib import Path as _Path

from loguru import logger as _logger

from rquant.builtin_presets import BUILTIN_PRESET_SCREENS, ScreenPreset
from rquant.config import settings as _settings


def load_user_presets(directory: _Path) -> dict[str, ScreenPreset]:
    """从 directory 下的 *.json 加载用户保存的 preset。

    旧 JSON 结构：
        {
          "name": "<base_name>",
          "description": "<NL query>",
          "rules": [{"name": "not_st", "args": {}}, ...],
          "include_columns": [...],
          "created_at": "...",
          "source": "nl_input"
        }
    v2 增加 display_name、depends_on 与精确 delay_days；旧 offset_days 是回看窗口。

    解析失败的文件跳过（记 warning），不影响其他 preset。

    返回 dict 的 key 是 "user/{base_name}"，前缀强制隔离与代码内置 preset。
    """
    result: dict[str, ScreenPreset] = {}
    if not directory.exists() or not directory.is_dir():
        return result

    # 局部 import 避免循环（registry 依赖 screen.rules）
    from rquant.llm.dispatch import build_rules
    from rquant.llm.schemas import RuleCall, ScreenPlan, Stage

    for path in sorted(directory.glob("*.json")):
        try:
            data = _json.loads(path.read_text(encoding="utf-8"))
            base_name = data["name"]
            if base_name != path.stem:
                raise ValueError("pool name does not match definition filename")
            full_name = f"user/{base_name}"

            depends_on = data.get("depends_on")
            if depends_on is not None and (not isinstance(depends_on, str) or not depends_on):
                raise ValueError("parent pool name must be nonempty")
            delay_days: int | None = None
            offset_days = 0
            if "delay_days" in data:
                delay_days = data["delay_days"]
                if type(delay_days) is not int:
                    raise ValueError("delay_days must be an integer")
                if depends_on is None and delay_days != 0:
                    raise ValueError("delay_days must be 0 without a parent pool")
                if depends_on is not None and not 1 <= delay_days <= 252:
                    raise ValueError("delay_days must be 1..252 with a parent pool")
                if depends_on is None:
                    delay_days = None
            else:
                offset_days = data.get("offset_days", 0)
                if type(offset_days) is not int or offset_days < 0:
                    raise ValueError("offset_days must be a nonnegative integer")
                if depends_on is None and offset_days != 0:
                    raise ValueError("offset_days must be 0 without a parent pool")
                if depends_on is not None and offset_days == 0:
                    raise ValueError("offset_days must be positive with a parent pool")

            display_name = data.get("display_name", base_name)
            if not isinstance(display_name, str) or not display_name.strip():
                raise ValueError("display_name must be nonempty")

            # 把 rules（flat list）包成单 stage，复用 dispatch.build_rules 校验
            rule_calls = [RuleCall(name=r["name"], args=r.get("args", {})) for r in data["rules"]]
            plan = ScreenPlan(
                trade_date="1900-01-01",  # placeholder，preset 落库时与日期无关
                stages=[Stage(label="loaded", rules=rule_calls)],
                include_columns=data.get("include_columns", []),
            )
            rules = build_rules(plan)

            result[full_name] = ScreenPreset(
                name=full_name,
                description=data.get("description", ""),
                rules=rules,
                rule_calls=rule_calls,
                include_columns=data.get("include_columns", []),
                depends_on=depends_on,
                offset_days=offset_days,
                delay_days=delay_days,
                display_name=display_name,
            )
        except Exception as e:
            _logger.warning(f"加载 user preset 失败 {path.name}: {e}")
            continue

    builtins = set(BUILTIN_PRESET_SCREENS)

    def has_valid_ancestry(name: str) -> bool:
        visited: set[str] = set()
        while name not in builtins:
            preset = result.get(name)
            if preset is None or name in visited:
                return False
            visited.add(name)
            if preset.depends_on is None:
                return True
            name = preset.depends_on
        return True

    for name in tuple(result):
        if not has_valid_ancestry(name):
            _logger.warning(f"加载 user preset 失败 {name}: parent missing or dependency cycle")
            del result[name]
    return result


# 启动时自动 merge user_presets 目录下所有 JSON
_user_presets_dir = _Path(_settings.data_dir) / "user_presets"
PRESET_SCREENS: dict[str, ScreenPreset] = {
    **BUILTIN_PRESET_SCREENS,
    **load_user_presets(_user_presets_dir),
}
