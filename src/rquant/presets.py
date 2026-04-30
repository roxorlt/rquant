"""筛选预设注册表：每个 ScreenPreset 是一套命名的规则组合。"""

from __future__ import annotations

from dataclasses import dataclass, field

from rquant.screen.rules import (
    Rule,
    circ_mv_lt,
    first_limit_up,
    gt,
    has_lower_shadow,
    has_prior_limit_up,
    lt,
    no_consec_ups_in_window,
    no_limit_down_in_window,
    not_bj,
    not_limit_up,
    not_st,
    not_yiziban,
)


@dataclass
class ScreenPreset:
    """一套命名的筛选策略。"""

    name: str
    description: str
    rules: list[Rule]
    include_columns: list[str] = field(default_factory=list)
    depends_on: str | None = None
    offset_days: int = 0


import json as _json
from pathlib import Path as _Path

from loguru import logger as _logger


def load_user_presets(directory: _Path) -> dict[str, ScreenPreset]:
    """从 directory 下的 *.json 加载用户保存的 preset。

    JSON 结构：
        {
          "name": "<base_name>",
          "description": "<NL query>",
          "rules": [{"name": "not_st", "args": {}}, ...],
          "include_columns": [...],
          "created_at": "...",
          "source": "nl_input"
        }

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
            full_name = f"user/{base_name}"

            # 把 rules（flat list）包成单 stage，复用 dispatch.build_rules 校验
            plan = ScreenPlan(
                trade_date="1900-01-01",  # placeholder，preset 落库时与日期无关
                stages=[Stage(label="loaded", rules=[
                    RuleCall(name=r["name"], args=r.get("args", {})) for r in data["rules"]
                ])],
                include_columns=data.get("include_columns", []),
            )
            rules = build_rules(plan)

            result[full_name] = ScreenPreset(
                name=full_name,
                description=data.get("description", ""),
                rules=rules,
                include_columns=data.get("include_columns", []),
            )
        except Exception as e:
            _logger.warning(f"加载 user preset 失败 {path.name}: {e}")
            continue

    return result


PRESET_SCREENS: dict[str, ScreenPreset] = {
    "n-shape-pool1": ScreenPreset(
        name="n-shape-pool1",
        description="N形态-Pool1：昨首板+安全过滤+下影线",
        rules=[
            not_st(),
            not_bj(),
            first_limit_up(offset=1),
            not_limit_up(offset=0),
            not_yiziban(offset=1),
            gt("HIGH[0]", "CLOSE[1]"),
            circ_mv_lt(150),
            has_lower_shadow(0.5, 0.02, 0),
            no_consec_ups_in_window(3, 8),
            no_limit_down_in_window(30),
            has_prior_limit_up(120, 1),
        ],
        include_columns=[
            "CIRC_MV[0]",
            "BODY_UPPER[0]",
            "BODY_LOWER[0]",
            "CONSECUTIVE_LIMIT_UPS[1]",
        ],
    ),
    "n-shape-pool2": ScreenPreset(
        name="n-shape-pool2",
        description="N形态-Pool2：Pool1子集T+1实体收缩+下影线",
        depends_on="n-shape-pool1",
        offset_days=2,
        rules=[
            lt("BODY_UPPER[0]", "BODY_UPPER[1]"),
            lt("BODY_LOWER[0]", "BODY_LOWER[1]"),
            has_lower_shadow(0.5, 0.02, 0),
        ],
        include_columns=[
            "BODY_UPPER[0]",
            "BODY_LOWER[0]",
            "BODY_UPPER[1]",
            "BODY_LOWER[1]",
        ],
    ),
}

from rquant.config import settings as _settings

# 启动时自动 merge user_presets 目录下所有 JSON
_user_presets_dir = _Path(_settings.data_dir) / "user_presets"
PRESET_SCREENS.update(load_user_presets(_user_presets_dir))
