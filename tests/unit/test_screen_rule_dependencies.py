"""Rule factories declare exactly the facts their own closures read."""

from __future__ import annotations

import pandas as pd
import pytest

import rquant.screen.rules as screen_rules
from rquant.llm.registry import REGISTRY

_ARGS: dict[str, dict[str, object]] = {
    "circ_mv_lt": {"threshold_yi": 100, "offset": 30},
    "board_in": {"boards": ["main"]},
    "consecutive_ups_gte": {"n": 2, "offset": 30},
    "has_lower_shadow": {"offset": 30},
    "gt": {"left": "HIGH[30]", "right": "CLOSE[0]"},
    "lt": {"left": "MA5[30]", "right": 10},
    "gte": {"left": 10, "right": "RSI14[0]"},
    "lte": {"left": "TURNOVER_RATE[30]", "right": "CIRC_MV[0]"},
    "between": {"field": "PCT_CHG[30]", "low": -5, "high": 5},
    "cross_above": {"fast": "MA5", "slow": "MA20", "offset": 30},
    "cross_below": {"fast": "MA10", "slow": "MA60", "offset": 30},
    "above_ma": {"period": 60, "offset": 30},
    "rsi_oversold": {"period": 6, "threshold": 30, "offset": 30},
    "rsi_overbought": {"period": 14, "threshold": 70, "offset": 30},
    "volume_ratio_gte": {"n": 2, "offset": 30, "window": 60},
    "no_consec_ups_in_window": {"window": 120},
    "no_limit_down_in_window": {"window": 250},
    "has_prior_limit_up": {"window": 500, "exclude_offset": 30},
}


@pytest.mark.parametrize("spec", REGISTRY, ids=lambda spec: spec.name)
def test_registered_factory_dependencies_are_sufficient_to_execute(spec: object) -> None:
    args = spec.args_model.model_validate(_ARGS.get(spec.name, {})).model_dump()
    rule = spec.fn(**args)
    columns = screen_rules.required_rule_columns([rule])
    assert isinstance(columns, frozenset)
    latest_offset = max(
        (int(column.split("[")[1][:-1]) for column in columns if "[" in column),
        default=0,
    )
    assert latest_offset <= rule.min_lookback

    frame = pd.DataFrame({column: [1.0] for column in columns})
    for request in getattr(rule, "aggregate_requests", []):
        frame[request.name] = [1.0]
    assert len(rule(frame)) == 1


def test_variable_window_and_cross_dependencies_are_exact() -> None:
    by_name = {spec.name: spec for spec in REGISTRY}
    volume = by_name["volume_ratio_gte"].fn(n=2, offset=30, window=60)
    cross = by_name["cross_above"].fn(fast="MA5", slow="MA20", offset=30)
    assert screen_rules.required_rule_columns([volume]) == frozenset(
        f"VOL[{offset}]" for offset in range(30, 91)
    )
    assert screen_rules.required_rule_columns([cross]) == frozenset(
        {"MA5[30]", "MA20[30]", "MA5[31]", "MA20[31]"}
    )


def test_unknown_rule_is_rejected_before_selective_loading() -> None:
    with pytest.raises(ValueError, match="metadata"):
        screen_rules.required_rule_columns([lambda frame: frame["CLOSE[0]"] > 0])
