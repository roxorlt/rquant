from __future__ import annotations

import json

import pytest

from rquant import runtime_definition_bootstrap, strategy_evaluators
from rquant.minute_backtest_contracts import FrozenMinuteRuntimeInput
from rquant.minute_backtest_validation import original_builtin_minute_plan
from tests.unit.test_minute_backtest_producer import original_fixture


def test_repeated_full_input_preserves_exact_original_plan_and_frozen_hash() -> None:
    fixture = original_fixture()
    commit = fixture["frozen_input"]["producer_commit"]
    expected = runtime_definition_bootstrap.plan_builtin_definitions(producer_commit=commit)
    value = original_builtin_minute_plan(producer_commit=commit)
    assert value == expected and value.plan_id == expected.plan_id
    assert original_builtin_minute_plan(producer_commit=commit) == value
    raw = json.dumps(fixture["frozen_input"])
    first = FrozenMinuteRuntimeInput.model_validate_json(raw)
    second = FrozenMinuteRuntimeInput.model_validate_json(raw)
    assert first == second and first.input_hash == second.input_hash
    changed = json.loads(raw)
    changed["strategy"]["executable_fingerprint"] = "0" * 64
    with pytest.raises(ValueError):
        FrozenMinuteRuntimeInput.model_validate_json(json.dumps(changed))


def test_caller_cannot_pollute_the_cached_plan_bytes() -> None:
    first = original_builtin_minute_plan(producer_commit="e" * 40)
    expected = first.model_dump(mode="python")
    with pytest.raises(ValueError):
        first.producer_commit = "f" * 40
    object.__setattr__(first, "producer_commit", "f" * 40)
    object.__setattr__(first.strategies[0], "executable_fingerprint", "0" * 64)
    second = original_builtin_minute_plan(producer_commit="e" * 40)
    assert second is not first and second.model_dump(mode="python") == expected


def test_cached_plan_checks_original_referenced_definition_constants(monkeypatch) -> None:
    original_builtin_minute_plan(producer_commit="b" * 40)
    monkeypatch.setattr(runtime_definition_bootstrap, "_FEATURE_CONTRACT_VERSIONS", (1, 2, 3))
    with pytest.raises(ValueError, match="dependency"):
        original_builtin_minute_plan(producer_commit="b" * 40)


def test_cached_plan_checks_native_evaluator_code_on_every_use(monkeypatch) -> None:
    original_builtin_minute_plan(producer_commit="c" * 40)
    implementation = strategy_evaluators.BuiltinStrategyEvaluatorRegistry(producer_commit="c" * 40).load_definition("n_shape", 1).entry_evaluator
    monkeypatch.setattr(implementation, "__code__", test_cached_plan_checks_original_referenced_definition_constants.__code__)
    with pytest.raises(ValueError, match="dependency"):
        original_builtin_minute_plan(producer_commit="c" * 40)


def test_cached_plan_checks_registry_method_binding_on_every_use(monkeypatch) -> None:
    original_builtin_minute_plan(producer_commit="d" * 40)
    monkeypatch.setattr(strategy_evaluators.BuiltinStrategyEvaluatorRegistry, "trusted_executable_registry", test_repeated_full_input_preserves_exact_original_plan_and_frozen_hash)
    with pytest.raises(ValueError, match="dependency"):
        original_builtin_minute_plan(producer_commit="d" * 40)


def test_cached_plan_checks_original_candidate_property_binding(monkeypatch) -> None:
    original_builtin_minute_plan(producer_commit="f" * 40)
    monkeypatch.setattr(strategy_evaluators.BuiltinStrategyDefinition, "candidate_schema_fingerprint", property(lambda self: "0" * 64))
    with pytest.raises(ValueError, match="dependency"):
        original_builtin_minute_plan(producer_commit="f" * 40)
