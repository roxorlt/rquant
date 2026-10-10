from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.minute_backtest_parameter_definition import (
    bootstrap_minute_parameter_definition, build_minute_parameter_definition,
    minute_parameter_executable_registry,
)
from rquant.minute_backtest_parameter_evaluators import parameter_entry_evaluator
from rquant.minute_backtest_parameter_features import (
    MinuteParameterCandidate, project_minute_parameter_features,
)
from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet
from rquant.signal_contracts import SignalAction
from rquant.strategy_runner import StrategyCandidateState
from rquant.strategy_spec import StrategyLifecycleState

COMMIT = "a" * 40
AT = datetime(2025, 1, 2, 2, 30, tzinfo=UTC)


def candidate(recipe: MinuteParameterSet) -> MinuteParameterCandidate:
    return MinuteParameterCandidate(family="n_shape", parameter_hash=recipe.fingerprint,
        ts_code="600001.SH", name="合成样本", pool="pool1", trade_date=date(2025, 1, 2),
        reference_date=date(2025, 1, 1), available_at=AT - timedelta(hours=1),
        t_close=10.0, t_high=10.2, limit_up_price=11.0)


def prefix() -> pd.DataFrame:
    return pd.DataFrame([
        {"ts_code": "600001.SH", "trade_time": AT - timedelta(minutes=2-index),
         "available_at": AT - timedelta(minutes=2-index), "open": 10.3,
         "high": 10.6, "low": 10.15, "close": 10.5, "vol": amount/10.3, "amount": amount}
        for index, amount in enumerate((100.0, 100.0, 250.0))
    ])


@pytest.mark.parametrize("mode", ["first_break", "break_retest", "late_confirm", "vwap_confirm", "amount_surge", "factor_confirm"])
def test_complete_parameters_reach_original_pure_entry_and_fresh_version(mode: str) -> None:
    recipe = MinuteParameterSet(parameters=MinuteNShapeParameters(entry_mode=mode, factor_score_threshold=0.0))
    definition = build_minute_parameter_definition(recipe, producer_commit=COMMIT)
    features = project_minute_parameter_features(recipe, candidate(recipe), prefix(), prefix().iloc[:0],
        source_frequency="1min", decision_cutoff=AT)
    state = StrategyCandidateState(strategy_spec_fingerprint=definition.spec.spec_fingerprint,
        candidate_id="600001.SH", state=StrategyLifecycleState.IDLE, last_feature_sequence=-1, updated_at=AT)
    result = parameter_entry_evaluator(definition.spec, state, features)
    assert result is not None and result.action is SignalAction.B_INTENT
    assert result.evidence["minute_parameter_set_hash"] == recipe.fingerprint
    assert definition.strategy_id == recipe.definition_id
    assert definition.spec.version == 1 and definition.evaluator_semantic_version == "2.0.0"
    assert definition.spec.parameters["minute_parameter_set_json"] == recipe.model_dump_json()


def test_actual_trusted_registration_binds_full_json_hash_and_semantic(tmp_path: Path) -> None:
    recipe = MinuteParameterSet(parameters=MinuteNShapeParameters(paper={"stop_loss_pct": 0.012345}))
    registration = bootstrap_minute_parameter_definition(tmp_path / "definitions", recipe,
        producer_commit=COMMIT, registered_at=AT, available_at=AT)
    definition = build_minute_parameter_definition(recipe, producer_commit=COMMIT)
    registry = ImmutableDefinitionRegistry(tmp_path / "definitions",
        execution_registry=minute_parameter_executable_registry(definition))
    actual = registry.read_strategy_spec(registration.fingerprint, as_of=AT)
    assert actual == registration
    assert actual.execution_binding.exit_rules[0].parameter_set == recipe
    assert actual.execution_binding.entry_evaluator_version == "2.0.0"
    altered = definition.spec.model_copy(update={"parameters": {
        **definition.spec.parameters, "minute_parameter_set_hash": "f" * 64}})
    with pytest.raises(ValueError, match="parameter"):
        registry.register_strategy_spec(altered, feature_contract_fingerprint=registration.feature_contract_fingerprint,
            registered_at=AT, available_at=AT, producer_commit=COMMIT, expected_fingerprint=altered.spec_fingerprint)


def test_projection_excludes_future_publication_and_binds_exact_physical_frequency() -> None:
    recipe = MinuteParameterSet(parameters=MinuteNShapeParameters())
    raw = prefix()
    clean = project_minute_parameter_features(recipe, candidate(recipe), raw, raw.iloc[:0],
        source_frequency="1min", decision_cutoff=AT)
    future = raw.iloc[[-1]].copy()
    future["trade_time"] = AT + timedelta(minutes=1)
    future["available_at"] = AT + timedelta(minutes=1)
    future["close"] = 10.59
    assert project_minute_parameter_features(recipe, candidate(recipe), pd.concat([raw, future]), raw.iloc[:0],
        source_frequency="1min", decision_cutoff=AT) == clean
    delayed = raw.copy()
    delayed.loc[2, "available_at"] = AT + timedelta(seconds=1)
    visible = project_minute_parameter_features(recipe, candidate(recipe), delayed, raw.iloc[:0],
        source_frequency="1min", decision_cutoff=AT)
    assert visible["minute_parameter_bar"] != clean["minute_parameter_bar"]
    with pytest.raises(ValueError, match="frequency"):
        project_minute_parameter_features(recipe, candidate(recipe), raw, raw.iloc[:0],
            source_frequency="5min", decision_cutoff=AT)
    wrong = candidate(recipe).model_copy(update={"parameter_hash": "e" * 64})
    with pytest.raises(ValueError, match="parameter"):
        project_minute_parameter_features(recipe, wrong, raw, raw.iloc[:0], source_frequency="1min", decision_cutoff=AT)
