"""Validation plans share the original bounded request ledger and live checks."""

from __future__ import annotations

from typing import Any

import pytest

from rquant import executable_dependencies as executable
from rquant import minute_backtest_parameter_contracts as contracts
from rquant import minute_backtest_parameter_definition as owner
from rquant.definition_registry import _canonical_feature_contract, _canonical_strategy_spec
from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet


def _parameters() -> MinuteParameterSet:
    return MinuteParameterSet(parameters=MinuteNShapeParameters(freq="60min"))


def _entry() -> Any:
    entry, = owner._PARAMETER_VALIDATION_STATE.get().values()
    return entry


def _ledger() -> Any:
    return contracts._PARAMETER_CONTENT_STATE.get()


def _fee(entry: Any) -> int:
    return sum(guard.code_plan_retained_bytes for guard in (
        entry.builder_guard, entry.executable_guard,
    ))


def _expected(parameters: MinuteParameterSet) -> owner.MinuteParameterValidationPlan:
    definition = owner.build_minute_parameter_definition(parameters, producer_commit="a" * 40)
    contract = _canonical_feature_contract(owner.minute_parameter_feature_contract(definition))
    trusted = owner.minute_parameter_executable_registry(definition)
    wrapper = owner.build_minute_parameter_research_definition("a" * 40)
    return owner.MinuteParameterValidationPlan(
        parameters=parameters, native_spec=_canonical_strategy_spec(definition.spec),
        native_executable_fingerprint=definition.executable_fingerprint,
        candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
        feature_contract=contract, feature_bindings=trusted.feature_bindings(contract),
        wrapper_spec=_canonical_strategy_spec(wrapper.spec),
        wrapper_executable_fingerprint=wrapper.executable_fingerprint,
        wrapper_candidate_schema_fingerprint=wrapper.candidate_schema_fingerprint,
    )


def _replacement_evaluator(*args: Any, **kwargs: Any) -> None:
    return None


def test_actual_two_validation_guards_adopt_equal_output_and_charge_once() -> None:
    parameters = _parameters()
    expected = _expected(parameters)
    with owner.minute_parameter_validation_scope():
        first = owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        entry = _entry()
        assert entry.builder_guard.code_plan is not None
        assert entry.executable_guard.code_plan is not None
        ledger = _ledger()
        assert (
            ledger.validation_plan_bytes == ledger.policy_bytes
            == ledger.retained_bytes == _fee(entry)
        )
        assert 0 < ledger.retained_bytes <= contracts._MAX_PARAMETER_CONTENT_BYTES
        original_fee = ledger.retained_bytes
        second = owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        assert first is not second
        assert first.model_dump_json() == second.model_dump_json() == expected.model_dump_json()
        assert (
            ledger.retained_bytes == ledger.policy_bytes
            == ledger.validation_plan_bytes == original_fee
        )
        entries = owner._PARAMETER_VALIDATION_STATE.get()
    assert ledger.validation_plan_bytes == ledger.policy_bytes == ledger.retained_bytes == 0
    assert entries == {} and ledger.entries == ledger.guards == {}
    assert owner._PARAMETER_VALIDATION_STATE.get() is None
    assert contracts._PARAMETER_CONTENT_STATE.get() is None


@pytest.mark.parametrize("change", ("binding", "code", "default", "keyword", "closure"))
def test_compiled_actual_guards_keep_live_mutation_rejection(
    monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    parameters = _parameters()
    nested = {"values": [1]}
    builder = owner.build_minute_parameter_definition
    if change == "default":
        monkeypatch.setattr(builder, "__defaults__", (nested,))
    elif change == "keyword":
        monkeypatch.setattr(builder, "__kwdefaults__", {"producer_commit": nested})
    elif change == "closure":
        def live_factory() -> int:
            return nested["values"][0]

        monkeypatch.setattr(builder, "__kwdefaults__", {"producer_commit": live_factory})
    with owner.minute_parameter_validation_scope():
        owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        entry = _entry()
        assert entry.builder_guard.code_plan is not None
        assert entry.executable_guard.code_plan is not None
        if change == "binding":
            monkeypatch.setattr(owner, "parameter_entry_evaluator", _replacement_evaluator)
        elif change == "code":
            monkeypatch.setattr(builder, "__code__", builder.__code__.replace())
        else:
            nested["values"][0] = 2
        with pytest.raises(executable.ExecutableDependencyError):
            owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)


@pytest.mark.parametrize("allowance", (0, 1, 100_000))
def test_existing_shared_remaining_allowance_bounds_plans_and_keeps_output(
    allowance: int,
) -> None:
    parameters = _parameters()
    expected = _expected(parameters)
    with owner.minute_parameter_validation_scope():
        ledger = _ledger()
        original_retained = contracts._MAX_PARAMETER_CONTENT_BYTES - allowance
        ledger.retained_bytes = original_retained
        actual = owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        assert actual.model_dump_json() == expected.model_dump_json()
        assert 0 <= ledger.validation_plan_bytes <= allowance
        assert ledger.validation_plan_bytes == ledger.policy_bytes == _fee(_entry())
        assert ledger.retained_bytes == original_retained + ledger.validation_plan_bytes
        assert ledger.retained_bytes <= contracts._MAX_PARAMETER_CONTENT_BYTES
        if allowance <= 1:
            assert _entry().builder_guard.code_plan is _entry().executable_guard.code_plan is None
        before = ledger.retained_bytes
        owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        assert ledger.retained_bytes == before


def test_no_budget_owner_keeps_original_guards() -> None:
    with owner.minute_parameter_validation_scope():
        ledger = _ledger()
        token = contracts._PARAMETER_CONTENT_STATE.set(None)
        try:
            owner.minute_parameter_validation_plan(_parameters(), producer_commit="a" * 40)
            entry = _entry()
            assert entry.builder_guard.code_plan is entry.executable_guard.code_plan is None
        finally:
            contracts._PARAMETER_CONTENT_STATE.reset(token)
        assert ledger.retained_bytes == ledger.policy_bytes == ledger.validation_plan_bytes == 0


def test_eight_original_validation_slots_do_not_charge_overflow() -> None:
    parameters = _parameters()
    with owner.minute_parameter_validation_scope():
        ledger = _ledger()
        for index in range(8):
            owner.minute_parameter_validation_plan(parameters, producer_commit=str(index) * 40)
        entries = owner._PARAMETER_VALIDATION_STATE.get()
        assert len(entries) == 8
        assert ledger.validation_plan_bytes == sum(_fee(entry) for entry in entries.values())
        assert ledger.retained_bytes == ledger.policy_bytes == ledger.validation_plan_bytes
        assert ledger.retained_bytes <= contracts._MAX_PARAMETER_CONTENT_BYTES
        before = ledger.retained_bytes
        first = owner.minute_parameter_validation_plan(parameters, producer_commit="9" * 40)
        second = owner.minute_parameter_validation_plan(parameters, producer_commit="9" * 40)
        assert first == second and first is not second
        assert len(entries) == 8 and ledger.retained_bytes == before
        assert contracts._MAX_PARAMETER_CONTENT_ENTRIES == 8
    assert entries == {} and ledger.retained_bytes == ledger.validation_plan_bytes == 0


def test_constructor_failure_rolls_back_only_unretained_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = owner.MinuteParameterValidationPlan.model_dump_json
    observed = []

    def failed(self: owner.MinuteParameterValidationPlan, *args: Any, **kwargs: Any) -> str:
        observed.append(_ledger().validation_plan_bytes)
        raise RuntimeError("synthetic post-reservation serialization failure")

    with owner.minute_parameter_validation_scope():
        ledger = _ledger()
        ledger.retained_bytes = 100
        with monkeypatch.context() as patched:
            patched.setattr(owner.MinuteParameterValidationPlan, "model_dump_json", failed)
            with pytest.raises(RuntimeError, match="post-reservation"):
                owner.minute_parameter_validation_plan(_parameters(), producer_commit="a" * 40)
        assert observed and observed[0] > 0
        assert ledger.validation_plan_bytes == ledger.policy_bytes == 0
        assert ledger.retained_bytes == 100
        assert owner._PARAMETER_VALIDATION_STATE.get() == {}
        assert owner.MinuteParameterValidationPlan.model_dump_json is original


def test_second_plan_failure_preserves_prior_committed_fee(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = executable.ExecutableDependencyGuard.with_compiled_code_plan
    observed = []

    def failed(
        self: executable.ExecutableDependencyGuard, *, max_retained_bytes: int,
    ) -> executable.ExecutableDependencyGuard:
        if self.contract == "minute-parameter-pure-validation-executables/v1":
            observed.append(_ledger().validation_plan_bytes)
            raise RuntimeError("synthetic second adoption failure")
        return original(self, max_retained_bytes=max_retained_bytes)

    with owner.minute_parameter_validation_scope():
        owner.minute_parameter_validation_plan(_parameters(), producer_commit="a" * 40)
        ledger = _ledger()
        previous = ledger.validation_plan_bytes
        assert previous > 0
        with monkeypatch.context() as patched:
            patched.setattr(executable.ExecutableDependencyGuard, "with_compiled_code_plan", failed)
            with pytest.raises(RuntimeError, match="second adoption"):
                owner.minute_parameter_validation_plan(_parameters(), producer_commit="b" * 40)
        assert observed and observed[0] > previous
        assert (
            ledger.validation_plan_bytes == ledger.retained_bytes == ledger.policy_bytes == previous
        )
        assert len(owner._PARAMETER_VALIDATION_STATE.get()) == 1
