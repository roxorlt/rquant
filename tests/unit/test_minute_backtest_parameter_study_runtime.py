from __future__ import annotations

from datetime import UTC, date, datetime
from importlib import import_module, util

import pandas as pd
import pytest
from tests.unit.test_minute_backtest_parameter_optimizer import study_candidate
from tests.unit.test_minute_backtest_parameter_runtime import AT, COMMIT, candidate, prefix
from tests.unit.test_minute_backtest_study_protocols import study_protocol

from rquant.minute_backtest_formal import MinuteExperimentProtocol
from rquant.minute_backtest_parameter_definition import build_minute_parameter_definition
from rquant.minute_backtest_parameter_evaluators import parameter_entry_evaluator
from rquant.minute_backtest_parameter_features import (
    PARAMETER_BAR_FEATURE,
    PARAMETER_CANDIDATE_FEATURE,
    MinuteParameterBar,
    MinuteParameterCandidate,
    project_minute_parameter_features,
)
from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding
from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet
from rquant.signal_contracts import SignalAction
from rquant.strategy_runner import StrategyCandidateState
from rquant.strategy_spec import StrategyLifecycleState


def study_runtime_api() -> object:
    assert util.find_spec("rquant.minute_backtest_parameter_study_features") is not None
    return import_module("rquant.minute_backtest_parameter_study_features")


def setup_study() -> tuple[MinuteParameterStudyBinding, tuple[MinuteParameterCandidate, ...], pd.DataFrame]:
    recipe = MinuteParameterSet(parameters=MinuteNShapeParameters())
    definition = build_minute_parameter_definition(recipe, producer_commit=COMMIT)
    body = study_protocol().model_dump(mode="python")
    body["parameters"] = recipe
    body["source"].update(start_date=date(2025, 1, 2), end_date=date(2025, 1, 6))
    body["head"].update(definition_id=recipe.definition_id,
        parameter_fingerprint=recipe.fingerprint, spec_fingerprint=definition.spec.spec_fingerprint,
        executable_fingerprint=definition.executable_fingerprint, producer_commit=COMMIT)
    body["split"].update(train_start=date(2025, 1, 2), train_end=date(2025, 1, 2),
        test_start=date(2025, 1, 6), test_end=date(2025, 1, 6))
    body.update(score_profile="accumulation_heavy", top_n=1)
    protocol = type(study_protocol()).model_validate(body)
    formal = MinuteExperimentProtocol.model_validate({
        "train_range": {"start_date": date(2025, 1, 2), "end_date": date(2025, 1, 2)},
        "validation_range": {"start_date": date(2025, 1, 3), "end_date": date(2025, 1, 3)},
        "frozen_outer_test_range": {"start_date": date(2025, 1, 6), "end_date": date(2025, 1, 6)},
    })
    bound = MinuteParameterStudyBinding.from_formal_protocol(protocol=protocol,
        formal_protocol=formal, request_hash="a" * 64)
    static = {item.name: item.value for item in study_candidate(study_protocol()).features}
    candidates = tuple(MinuteParameterCandidate.model_validate(candidate(recipe).model_dump(mode="python") | {
        "ts_code": code, "study_binding": bound,
        "static_factors": static | {"accum_obv_change_20d_pct": strength},
    }) for code, strength in (("600001.SH", 1.0), ("600002.SH", 100.0)))
    raw = pd.concat([prefix().assign(ts_code=item.ts_code) for item in candidates], ignore_index=True)
    return bound, candidates, raw


def decisions(bound: object, candidates: tuple[object, ...], raw: pd.DataFrame,
              *, new_entry_codes: tuple[str, ...] = ("600001.SH", "600002.SH")) -> object:
    return study_runtime_api().project_minute_parameter_study_decisions(bound,
        candidates=candidates, minutes=raw, historical_minutes=raw.iloc[:0],
        source_frequency="1min", decision_cutoff=AT, new_entry_codes=new_entry_codes)


def entry_features(bound: object, row: object, raw: pd.DataFrame, decision: object | None) -> dict[str, object]:
    values = dict(project_minute_parameter_features(bound.protocol.parameters, row, raw, raw.iloc[:0],
        source_frequency="1min", decision_cutoff=AT))
    bar = MinuteParameterBar.model_validate_json(values[PARAMETER_BAR_FEATURE])
    values[PARAMETER_BAR_FEATURE] = bar.model_copy(update={"study_decision": decision}).model_dump_json()
    return values


def test_actual_entry_topn_uses_only_current_prefix_and_preserves_original_actions() -> None:
    bound, candidates, raw = setup_study()
    observed = decisions(bound, candidates, raw)
    definition = build_minute_parameter_definition(bound.protocol.parameters, producer_commit=COMMIT)
    entries = []
    for row in candidates:
        state = StrategyCandidateState(strategy_spec_fingerprint=definition.spec.spec_fingerprint,
            candidate_id=row.ts_code, state=StrategyLifecycleState.IDLE, last_feature_sequence=-1,
            updated_at=AT)
        result = parameter_entry_evaluator(definition.spec, state,
            entry_features(bound, row, raw, observed[row.ts_code]))
        if result is not None:
            entries.append((row.ts_code, result))
    assert [code for code, _ in entries] == ["600002.SH"]
    assert entries[0][1].action is SignalAction.B_INTENT
    assert entries[0][1].evidence["minute_study_binding_hash"] == bound.binding_hash
    future = raw.iloc[[0]].assign(trade_time=datetime(2025, 1, 2, 3, tzinfo=UTC),
        available_at=datetime(2025, 1, 2, 3, tzinfo=UTC))
    assert decisions(bound, candidates, pd.concat([raw, future], ignore_index=True)) == observed


def test_already_armed_candidate_does_not_consume_a_new_entry_slot() -> None:
    bound, candidates, raw = setup_study()
    observed = decisions(bound, candidates, raw, new_entry_codes=("600001.SH",))
    assert [row.ts_code for row in observed["600001.SH"].selected] == ["600001.SH"]
    definition = build_minute_parameter_definition(bound.protocol.parameters, producer_commit=COMMIT)
    state = StrategyCandidateState(strategy_spec_fingerprint=definition.spec.spec_fingerprint,
        candidate_id="600002.SH", state=StrategyLifecycleState.ARMED, last_feature_sequence=1,
        updated_at=AT)
    completed = parameter_entry_evaluator(definition.spec, state, {"entry_fill_status": "filled"})
    assert completed is not None and completed.expected_to_state is StrategyLifecycleState.HOLDING


def test_study_entry_rejects_missing_dynamic_prefix_proof() -> None:
    bound, candidates, raw = setup_study()
    definition = build_minute_parameter_definition(bound.protocol.parameters, producer_commit=COMMIT)
    state = StrategyCandidateState(strategy_spec_fingerprint=definition.spec.spec_fingerprint,
        candidate_id=candidates[0].ts_code, state=StrategyLifecycleState.IDLE,
        last_feature_sequence=-1, updated_at=AT)
    with pytest.raises(ValueError, match="study"):
        parameter_entry_evaluator(definition.spec, state,
            entry_features(bound, candidates[0], raw, None))


def test_missing_original_score_fact_is_not_filled_with_a_zero() -> None:
    bound, candidates, raw = setup_study()
    body = candidates[0].model_dump(mode="python")
    body["static_factors"].pop("accum_obv_change_20d_pct")
    incomplete = MinuteParameterCandidate.model_validate(body)
    with pytest.raises(ValueError, match="unknown"):
        decisions(bound, (incomplete, candidates[1]), raw)


def test_nonstudy_candidate_and_bar_keep_the_original_serialized_fields() -> None:
    recipe = MinuteParameterSet(parameters=MinuteNShapeParameters())
    row = candidate(recipe)
    values = project_minute_parameter_features(recipe, row, prefix(), prefix().iloc[:0],
        source_frequency="1min", decision_cutoff=AT)
    assert "study_binding" not in row.model_dump(mode="json")
    assert "study_decision" not in MinuteParameterBar.model_validate_json(
        values[PARAMETER_BAR_FEATURE]).model_dump(mode="json")
    assert values[PARAMETER_CANDIDATE_FEATURE] == row.model_dump_json()
