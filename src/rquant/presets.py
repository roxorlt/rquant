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
