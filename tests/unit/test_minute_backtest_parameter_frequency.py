from datetime import timedelta

import pytest


@pytest.mark.parametrize("frequency,seconds", [("1min", 120), ("5min", 420), ("15min", 1020), ("30min", 1920), ("60min", 3720)])
def test_parameter_intent_has_bounded_frequency_specific_validity(frequency: str, seconds: int) -> None:
    from rquant.minute_backtest_parameter_definition import build_minute_parameter_definition
    from rquant.minute_backtest_parameter_evaluators import parameter_entry_evaluator
    from rquant.minute_backtest_parameter_features import project_minute_parameter_features
    from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet
    from rquant.strategy_runner import StrategyCandidateState
    from rquant.strategy_spec import StrategyLifecycleState
    from tests.unit.test_minute_backtest_parameter_runtime import AT, COMMIT, candidate, prefix

    parameters = MinuteParameterSet(parameters=MinuteNShapeParameters(freq=frequency))
    definition = build_minute_parameter_definition(parameters, producer_commit=COMMIT)
    minutes = prefix()
    width = int(frequency.removesuffix("min"))
    cutoff = AT + timedelta(hours=1) if frequency == "60min" else AT
    for index in minutes.index:
        stamp = cutoff - timedelta(minutes=(2-index)*width)
        minutes.loc[index, "trade_time"] = stamp
        minutes.loc[index, "available_at"] = stamp
    features = project_minute_parameter_features(parameters, candidate(parameters), minutes, minutes.iloc[:0],
        source_frequency=frequency, decision_cutoff=cutoff)
    state = StrategyCandidateState(strategy_spec_fingerprint=definition.spec.spec_fingerprint,
        candidate_id="600001.SH", state=StrategyLifecycleState.IDLE, last_feature_sequence=-1, updated_at=cutoff)
    decision = parameter_entry_evaluator(definition.spec, state, features)
    assert decision is not None
    assert decision.expires_after == timedelta(seconds=seconds)
    assert definition.spec.parameters["intent_validity_seconds"] == seconds
    assert definition.spec.parameters["intent_validity_basis"] == "frequency_plus_original_120s_margin"
