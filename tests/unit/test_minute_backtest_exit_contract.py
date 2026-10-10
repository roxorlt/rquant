import json

import pytest
from pydantic import ValidationError

from rquant.definition_registry import MinuteParameterExitRule
from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet


def _rule_data(parameters: MinuteParameterSet) -> dict[str, object]:
    return {
        "event": "exit", "fill_event": "exit_filled", "action": "s_intent",
        "evaluator_id": "rquant.minute_backtest_parameter_evaluators:parameter_exit_evaluator",
        "eligibility": {"settlement_rule": "a_share_t_plus_one", "minimum_holding_trading_sessions": 1,
                        "same_day_sell_allowed": False, "sellable_position_required": True},
        "price_basis": {"adjustment_basis": "raw", "decision_price": "minute_close", "execution_price": "next_trade"},
        "sell_tranche": {"sequence": 1, "position_fraction": 1.0, "reevaluate_after_fill": False, "terminal_after_fill": True},
        "parameter_json": parameters.model_dump_json(), "parameter_fingerprint": parameters.fingerprint,
    }


def test_exact_exit_descriptor_preserves_original_floating_point_parameters() -> None:
    parameters = MinuteParameterSet(parameters=MinuteNShapeParameters(
        paper={"stop_loss_pct": 0.012345, "take_profit_pct": 0.056789, "trailing_stop_pct": 0.034567},
    ))
    rule = MinuteParameterExitRule.model_validate(_rule_data(parameters))
    assert rule.parameter_set == parameters
    assert rule.parameter_set.parameters.paper.stop_loss_pct == 0.012345
    assert rule.parameter_set.parameters.paper.take_profit_pct == 0.056789
    assert rule.parameter_set.parameters.paper.trailing_stop_pct == 0.034567
    assert MinuteParameterExitRule.model_validate_json(rule.model_dump_json()) == rule


def test_exit_descriptor_binds_the_complete_config_not_a_hash_summary() -> None:
    original = MinuteParameterSet(parameters=MinuteNShapeParameters())
    changed = MinuteParameterSet(parameters=MinuteNShapeParameters(paper={"stop_loss_pct": 0.012345}))
    with pytest.raises(ValidationError, match="fingerprint"):
        MinuteParameterExitRule.model_validate({**_rule_data(original), "parameter_json": changed.model_dump_json()})
    with pytest.raises(ValidationError):
        MinuteParameterExitRule.model_validate({**_rule_data(original), "parameter_json": json.dumps({"fingerprint": original.fingerprint})})
    with pytest.raises(ValidationError, match="canonical"):
        MinuteParameterExitRule.model_validate({**_rule_data(original), "parameter_json": json.dumps(original.model_dump(mode="json"), indent=2)})


@pytest.mark.parametrize(
    "delta",
    [
        {"fill_event": "exit"},
        {"action": "reduce"},
        {"eligibility": {"settlement_rule": "a_share_t_plus_one", "minimum_holding_trading_sessions": 1,
                         "same_day_sell_allowed": True, "sellable_position_required": True}},
        {"sell_tranche": {"sequence": 1, "position_fraction": 0.5, "reevaluate_after_fill": False, "terminal_after_fill": True}},
    ],
)
def test_original_terminal_and_settlement_invariants_are_still_required(delta: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        MinuteParameterExitRule.model_validate({**_rule_data(MinuteParameterSet(parameters=MinuteNShapeParameters())), **delta})
