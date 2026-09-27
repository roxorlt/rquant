"""Pure code-owned pool definitions; importing them never reads user state."""

from __future__ import annotations

from dataclasses import dataclass, field

from rquant.llm.compile import compile_screen_plan
from rquant.llm.schemas import RuleCall, ScreenPlan, Stage
from rquant.runtime_contracts import canonical_sha256
from rquant.screen.rules import Rule


@dataclass
class ScreenPreset:
    """One named rule bundle, with immutable metadata for publishing and copying."""

    name: str
    description: str
    rules: list[Rule]
    include_columns: list[str] = field(default_factory=list)
    depends_on: str | None = None
    offset_days: int = 0
    rule_calls: list[RuleCall] = field(default_factory=list)
    display_name: str | None = None
    delay_days: int | None = None
    ui_description: str | None = None
    definition_version: str | None = None


def builtin_definition_version(preset: ScreenPreset) -> str:
    """The code-owned definition identity shared with read-only publication."""
    identity = {
        "name": preset.name,
        "description": preset.description,
        "depends_on": preset.depends_on,
        "offset_days": preset.offset_days,
        "rules": [item.model_dump(mode="json") for item in preset.rule_calls],
        "include_columns": preset.include_columns,
    }
    return canonical_sha256({"contract": "builtin-pool/v1", **identity})


BUILTIN_PRESET_SCREENS: dict[str, ScreenPreset] = {
    "n-shape-pool1": ScreenPreset(
        name="n-shape-pool1",
        display_name="N 形态一池",
        ui_description="昨首板、安全过滤与下影线",
        description="N形态-Pool1：昨首板+安全过滤+下影线",
        rules=[],
        rule_calls=[
            RuleCall(name="not_st", args={}),
            RuleCall(name="not_bj", args={}),
            RuleCall(name="first_limit_up", args={"offset": 1}),
            RuleCall(name="not_limit_up", args={"offset": 0}),
            RuleCall(name="not_yiziban", args={"offset": 1}),
            RuleCall(name="gt", args={"left": "HIGH[0]", "right": "CLOSE[1]"}),
            RuleCall(name="circ_mv_lt", args={"threshold_yi": 150}),
            RuleCall(
                name="has_lower_shadow",
                args={"min_ratio": 0.5, "min_amplitude": 0.02, "offset": 0},
            ),
            RuleCall(name="no_consec_ups_in_window", args={"threshold": 3, "window": 8}),
            RuleCall(name="no_limit_down_in_window", args={"window": 30}),
            RuleCall(name="has_prior_limit_up", args={"window": 120, "exclude_offset": 1}),
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
        display_name="N 形态二池",
        ui_description="一池候选的实体收缩与下影线",
        description="N形态-Pool2：Pool1子集T+1实体收缩+下影线",
        depends_on="n-shape-pool1",
        offset_days=2,
        rules=[],
        rule_calls=[
            RuleCall(name="lt", args={"left": "BODY_UPPER[0]", "right": "BODY_UPPER[1]"}),
            RuleCall(name="lt", args={"left": "BODY_LOWER[0]", "right": "BODY_LOWER[1]"}),
            RuleCall(
                name="has_lower_shadow",
                args={"min_ratio": 0.5, "min_amplitude": 0.02, "offset": 0},
            ),
        ],
        include_columns=[
            "BODY_UPPER[0]",
            "BODY_LOWER[0]",
            "BODY_UPPER[1]",
            "BODY_LOWER[1]",
        ],
    ),
}

# Execute the same registered rule calls whose metadata is published as the definition.
for _preset in BUILTIN_PRESET_SCREENS.values():
    _preset.rules = compile_screen_plan(
        ScreenPlan(
            trade_date="1900-01-01",
            stages=[Stage(label="builtin", rules=_preset.rule_calls)],
            include_columns=_preset.include_columns,
        )
    ).rules
