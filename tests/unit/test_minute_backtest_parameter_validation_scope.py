"""A read can reuse pure definitions while retaining every source authority gate."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from types import FrameType

import pytest

from rquant.definition_registry import _canonical_feature_contract, _canonical_strategy_spec
from rquant.executable_dependencies import ExecutableDependencyError
from rquant.minute_backtest_parameters import (
    MinuteAuctionGapParameters, MinuteGrowthParameters, MinuteNShapeParameters, MinuteParameterSet,
)
from rquant.minute_backtest_parameter_contracts import (
    FrozenMinuteParameterResearchInput, MinuteParameterRuntimeContent, MinuteParameterSourceSeed,
)


def recipes() -> tuple[MinuteParameterSet, ...]:
    return tuple(MinuteParameterSet(parameters=value) for value in (
        MinuteNShapeParameters(),
        MinuteAuctionGapParameters(start_date="2026-07-31", end_date="2026-08-04"),
        MinuteGrowthParameters(),
    ))


@pytest.mark.parametrize("parameters", recipes(), ids=("n_shape", "auction", "growth"))
def test_scoped_pure_plan_matches_original_full_definition_fields(parameters: MinuteParameterSet) -> None:
    from rquant import minute_backtest_parameter_definition as owner

    definition = owner.build_minute_parameter_definition(parameters, producer_commit="a" * 40)
    original = owner.minute_parameter_executable_registry(definition)
    contract = _canonical_feature_contract(owner.minute_parameter_feature_contract(definition))
    wrapper = owner.build_minute_parameter_research_definition("a" * 40)
    expected = dict(parameters=parameters, native_spec=_canonical_strategy_spec(definition.spec),
        native_executable_fingerprint=definition.executable_fingerprint,
        candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
        feature_contract=contract, feature_bindings=original.feature_bindings(contract),
        wrapper_spec=_canonical_strategy_spec(wrapper.spec),
        wrapper_executable_fingerprint=wrapper.executable_fingerprint,
        wrapper_candidate_schema_fingerprint=wrapper.candidate_schema_fingerprint)
    with owner.minute_parameter_validation_scope():
        first = owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        second = owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        assert first is not second
        assert first == owner.MinuteParameterValidationPlan.model_validate(expected) == second
        assert first.model_dump_json() == second.model_dump_json()


def test_returned_copy_cannot_pollute_next_pure_validation() -> None:
    from rquant import minute_backtest_parameter_definition as owner

    parameters = recipes()[0]
    with owner.minute_parameter_validation_scope():
        first = owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        changed = first.model_copy(update={"native_executable_fingerprint": "f" * 64})
        assert changed != first
        with pytest.raises((TypeError, ValueError)):
            first.native_spec.parameters["minute_parameter_set_hash"] = "f" * 64
        assert owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40) == first


@pytest.mark.parametrize("change", ["binding", "code", "defaults"])
def test_each_reuse_rejects_actual_executable_dependency_change(
    monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    from rquant import minute_backtest_parameter_definition as owner

    parameters = recipes()[0]
    with owner.minute_parameter_validation_scope():
        owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        if change == "binding":
            monkeypatch.setattr(owner, "parameter_entry_evaluator", lambda *args, **kwargs: None)
        elif change == "code":
            monkeypatch.setattr(owner.parameter_entry_evaluator, "__code__", (lambda *args, **kwargs: None).__code__)
        else:
            monkeypatch.setattr(owner.parameter_entry_evaluator, "__defaults__", ("changed",))
        with pytest.raises(ExecutableDependencyError):
            owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)


def test_cache_payload_pollution_is_rejected_before_reuse() -> None:
    from rquant import minute_backtest_parameter_definition as owner

    parameters = recipes()[0]
    with owner.minute_parameter_validation_scope():
        owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        entry, = owner._PARAMETER_VALIDATION_STATE.get().values()
        object.__setattr__(entry, "payload", entry.payload.replace('"2.0.0"', '"9.0.0"'))
        with pytest.raises(ExecutableDependencyError, match="pure validation"):
            owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)


@pytest.mark.parametrize("change", ["builder_code", "builder_defaults", "owner_output"])
def test_reuse_rejects_changed_builder_or_full_owner_output(
    monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    from rquant import minute_backtest_parameter_definition as owner

    parameters = recipes()[0]
    with owner.minute_parameter_validation_scope():
        owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        if change == "builder_code":
            monkeypatch.setattr(owner.build_minute_parameter_definition, "__code__",
                (lambda *args, **kwargs: None).__code__)
        elif change == "builder_defaults":
            monkeypatch.setattr(owner.build_minute_parameter_definition, "__kwdefaults__",
                {"producer_commit": "changed"})
        else:
            monkeypatch.setattr(owner, "PARAMETER_FEATURE_CONTRACT", "changed.owner.contract")
        with pytest.raises(ExecutableDependencyError):
            owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)


def test_exception_and_new_request_release_the_entire_pure_state() -> None:
    from rquant import minute_backtest_parameter_definition as owner

    parameters = recipes()[0]
    assert owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40) is None
    with pytest.raises(RuntimeError, match="request failed"):
        with owner.minute_parameter_validation_scope():
            owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
            first = owner._PARAMETER_VALIDATION_STATE.get()
            raise RuntimeError("request failed")
    assert first == {} and owner._PARAMETER_VALIDATION_STATE.get() is None
    with owner.minute_parameter_validation_scope():
        assert owner._PARAMETER_VALIDATION_STATE.get() is not first
        assert owner._PARAMETER_VALIDATION_STATE.get() == {}
        owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
    assert owner._PARAMETER_VALIDATION_STATE.get() is None


def test_first_description_is_built_once_and_every_reuse_checks_both_guards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import minute_backtest_parameter_definition as owner

    captures: list[object] = []
    checks: list[int] = []
    original_capture = owner.capture_executable_dependency_guard
    original_check = owner.ExecutableDependencyGuard.assert_unchanged

    def capture(*args: object, **kwargs: object) -> object:
        result = original_capture(*args, **kwargs)
        captures.append(result)
        return result

    def check(guard: object) -> None:
        checks.append(id(guard))
        original_check(guard)

    monkeypatch.setattr(owner, "capture_executable_dependency_guard", capture)
    monkeypatch.setattr(owner.ExecutableDependencyGuard, "assert_unchanged", check)
    parameters = recipes()[0]
    with owner.minute_parameter_validation_scope():
        first = owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40)
        assert len(captures) == 2
        state = owner._PARAMETER_VALIDATION_STATE.get()
        assert state is not None
        entry = state[(parameters.model_dump_json(), "a" * 40)]
        adopted_guards = (entry.builder_guard, entry.executable_guard)
        checks.clear()
        with owner.minute_parameter_validation_scope():
            assert owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40) == first
        assert len(captures) == 2
        assert set(checks) == {id(guard) for guard in adopted_guards}
    with owner.minute_parameter_validation_scope():
        assert owner.minute_parameter_validation_plan(parameters, producer_commit="a" * 40) == first
        assert len(captures) == 4



@pytest.fixture(scope="module")
def complete_parameter_source(
    tmp_path_factory: pytest.TempPathFactory,
) -> FrozenMinuteParameterResearchInput:
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
    from tests.support.minute_parameter_formal_fixture import parameter_source_seed

    parameters = MinuteParameterSet(parameters=MinuteNShapeParameters(freq="60min"))
    with minute_parameter_validation_scope():
        seed = parameter_source_seed(tmp_path_factory.mktemp("pure-content-input"), parameters)
        return seed.freeze(audit_run_id="pure-content-unit-audit", dataset_snapshot_id="d" * 64)


@contextmanager
def derivation_calls() -> Iterator[list[str]]:
    from rquant import minute_backtest_parameter_contracts as contracts

    watched = {
        contracts.MinuteParameterRuntimeContent.complete_parameter_input.__code__: "content",
        contracts._MinuteParameterSourceBody.complete_source.__code__: "seed",
    }
    calls: list[str] = []
    previous = sys.getprofile()

    def observe(frame: FrameType, event: str, arg: object) -> None:
        if event == "call" and frame.f_code in watched:
            calls.append(watched[frame.f_code])

    sys.setprofile(observe)
    try:
        yield calls
    finally:
        sys.setprofile(previous)


@pytest.mark.parametrize("kind", ("content", "seed"))
def test_complete_pure_content_is_built_once_per_actual_input(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
    kind: str,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    def derive(
        value: FrozenMinuteParameterResearchInput,
    ) -> MinuteParameterRuntimeContent | MinuteParameterSourceSeed:
        return value.runtime.content if kind == "content" else value.source_content_seed

    original = derive(complete_parameter_source)
    equivalent = contracts.FrozenMinuteParameterResearchInput.model_validate_json(
        complete_parameter_source.model_dump_json()
    )
    with minute_parameter_validation_scope():
        with derivation_calls() as first_calls:
            first = derive(complete_parameter_source)
        with derivation_calls() as reuse_calls:
            second = derive(equivalent)
        assert kind in first_calls and reuse_calls == []
        assert first is not second
        assert first == second == original
        assert first.model_dump_json() == original.model_dump_json()
        if kind == "seed":
            assert first.seed_hash == original.seed_hash


def test_content_and_seed_share_one_bounded_request_budget(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    assert contracts._MAX_PARAMETER_CONTENT_BYTES == contracts.MAX_INPUT_BYTES
    with minute_parameter_validation_scope():
        for version in range(1, 7):
            source = complete_parameter_source.model_copy(
                update={
                    "runtime": complete_parameter_source.runtime.model_copy(
                        update={"source_version": version}
                    )
                }
            )
            assert source.runtime.content.source_version == version
            assert source.source_content_seed.runtime.source_version == version
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert len(state.entries) == 8
        assert {key[0] for key in state.entries} == {"content", "seed"}
        assert state.policy_bytes == (
            sum(guard.code_paths_bytes for guard in state.guards.values()) + state.validation_plan_bytes
        )
        assert state.policy_bytes > 0
        assert state.retained_bytes == (
            state.policy_bytes + sum(entry.retained_bytes for entry in state.entries.values())
        )
        assert state.retained_bytes <= contracts.MAX_INPUT_BYTES
        with derivation_calls() as calls:
            assert source.source_content_seed.runtime.source_version == 6
        assert "seed" in calls
    assert state.entries == {} and state.retained_bytes == state.policy_bytes == 0
    assert state.guards == {}
    assert contracts._PARAMETER_CONTENT_STATE.get() is None


def test_full_actual_archive_and_recipe_cannot_hit_a_claimed_identity(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
) -> None:
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    source = complete_parameter_source
    changed_material = source.runtime.materials[0].model_copy(update={"content_base64": "AAAA"})
    changed_runtime = source.runtime.model_copy(
        update={"materials": (changed_material, *source.runtime.materials[1:])}
    )
    changed_recipe = MinuteParameterSet(
        parameters=MinuteNShapeParameters(freq="60min", max_hold_days=3)
    )
    with minute_parameter_validation_scope():
        _ = source.source_content_seed
        for invalid in (
            changed_runtime,
            source.runtime.model_copy(update={"parameters": changed_recipe}),
        ):
            assert invalid.source_key == source.runtime.source_key
            with pytest.raises((ValueError, PermissionError)):
                _ = source.model_copy(update={"runtime": invalid}).source_content_seed


def test_returned_content_and_seed_are_independent_deep_copies(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
) -> None:
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    with minute_parameter_validation_scope():
        first = complete_parameter_source.source_content_seed
        content = complete_parameter_source.runtime.content
        expected = first.model_dump_json()
        object.__setattr__(first.runtime.materials[0], "content_base64", "AAAA")
        object.__setattr__(content.parameters.parameters.paper, "stop_loss_pct", 0.5)
        assert complete_parameter_source.source_content_seed.model_dump_json() == expected
        assert (
            complete_parameter_source.runtime.content.parameters.parameters.paper.stop_loss_pct
            != 0.5
        )


@pytest.mark.parametrize(
    "change",
    (
        "code", "defaults", "global", "schema", "field_default", "property", "factory",
        "copier", "model_binding",
    ),
)
def test_pure_content_reuse_rejects_current_dependency_or_schema_change(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    with minute_parameter_validation_scope():
        _ = complete_parameter_source.source_content_seed
        validator = contracts.MinuteParameterRuntimeContent.complete_parameter_input
        if change == "code":
            monkeypatch.setattr(validator, "__code__", (lambda self: self).__code__)
        elif change == "defaults":
            monkeypatch.setattr(validator, "__defaults__", ("changed",))
        elif change == "global":
            monkeypatch.setattr(contracts, "MAX_INPUT_BYTES", contracts.MAX_INPUT_BYTES + 1)
        elif change == "schema":
            monkeypatch.setitem(
                contracts.MinuteParameterRuntimeContent.__pydantic_core_schema__, "changed", True
            )
        elif change == "field_default":
            monkeypatch.setattr(
                contracts.MinuteParameterRuntimeContent.model_fields["source_version"], "default", 2
            )
        elif change == "property":
            monkeypatch.setattr(
                contracts.MinuteParameterWork.work_units.fget, "__code__", (lambda self: 0).__code__
            )
        elif change == "factory":
            from rquant.minute_backtest_parameters import MinuteGrowthParameters

            factory = MinuteGrowthParameters.model_fields["paper"].default_factory
            monkeypatch.setattr(factory, "__code__", (lambda: None).__code__)
        elif change == "copier":
            monkeypatch.setattr(contracts, "deepcopy", lambda value, memo: value)
        else:
            monkeypatch.setattr(contracts, "MinuteParameterRuntimeContent", object)
        with pytest.raises(ExecutableDependencyError):
            _ = complete_parameter_source.source_content_seed


def test_new_request_and_exception_release_all_pure_content(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    with pytest.raises(RuntimeError, match="request failed"), minute_parameter_validation_scope():
        _ = complete_parameter_source.source_content_seed
        state = contracts._PARAMETER_CONTENT_STATE.get()
        with minute_parameter_validation_scope():
            assert contracts._PARAMETER_CONTENT_STATE.get() is state
        raise RuntimeError("request failed")
    assert state.entries == {} and state.retained_bytes == state.policy_bytes == 0
    assert state.guards == {}
    assert contracts._PARAMETER_CONTENT_STATE.get() is None
    with minute_parameter_validation_scope():
        assert contracts._PARAMETER_CONTENT_STATE.get() is not state
        with derivation_calls() as calls:
            _ = complete_parameter_source.source_content_seed
        assert "seed" in calls
    with derivation_calls() as calls:
        _ = complete_parameter_source.source_content_seed
        _ = complete_parameter_source.source_content_seed
    assert calls.count("seed") == 2


def test_combined_byte_capacity_falls_back_without_rejecting_a_legal_source(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    expected = complete_parameter_source.source_content_seed
    content = complete_parameter_source.runtime.content
    with minute_parameter_validation_scope():
        assert complete_parameter_source.runtime.content == content
        content_bytes = contracts._PARAMETER_CONTENT_STATE.get().retained_bytes
    monkeypatch.setattr(contracts, "_MAX_PARAMETER_CONTENT_BYTES", content_bytes)
    with minute_parameter_validation_scope():
        assert complete_parameter_source.runtime.content == content
        state = contracts._PARAMETER_CONTENT_STATE.get()
        # The retained live-policy token includes the current capacity value.
        # Changing that value may change the descriptor's charged byte length.
        assert len(state.entries) == 1 and 0 < state.retained_bytes <= content_bytes
        retained_bytes = state.retained_bytes
        with derivation_calls() as calls:
            assert complete_parameter_source.source_content_seed == expected
            assert complete_parameter_source.source_content_seed == expected
        assert calls.count("seed") == 2
        assert len(state.entries) == 1 and state.retained_bytes == retained_bytes


def test_internal_cached_payload_corruption_is_rejected(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    with minute_parameter_validation_scope():
        _ = complete_parameter_source.runtime.content
        (entry,) = contracts._PARAMETER_CONTENT_STATE.get().entries.values()
        object.__setattr__(entry.value, "source_version", 999)
        with pytest.raises(ExecutableDependencyError, match="payload changed"):
            _ = complete_parameter_source.runtime.content


def test_normal_unrelated_module_initialization_keeps_original_pure_output(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib
    import json

    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    monkeypatch.delitem(sys.modules, "colorsys", raising=False)
    with minute_parameter_validation_scope():
        first = complete_parameter_source.source_content_seed
        guard = contracts._PARAMETER_CONTENT_STATE.get().guards["seed"]
        model_policies = {
            model.__qualname__: contracts._parameter_model_policy((model,), ())
            for model in guard.models
        }
        function_policies = {
            function.__qualname__: contracts._parameter_function_policy(function)
            for function in guard.functions
        }
        assert importlib.import_module("colorsys").__name__ == "colorsys"
        model_changes = [
            model.__qualname__ for model in guard.models
            if contracts._parameter_model_policy((model,), ()) != model_policies[model.__qualname__]
        ]
        function_changes = {
            function.__qualname__: [
                (name, path) for name, path, value
                in contracts._parameter_function_policy(function)[-1]
                if (name, path, value) not in function_policies[function.__qualname__][-1]
            ]
            for function in guard.functions
            if contracts._parameter_function_policy(function)
            != function_policies[function.__qualname__]
        }
        print(json.dumps({
            "changed_models": model_changes, "changed_function_globals": function_changes,
        }))
        assert model_changes == []
        assert function_changes == {}
        second = complete_parameter_source.source_content_seed
        assert second is not first
        assert second.model_dump_json() == first.model_dump_json()
        assert second.seed_hash == first.seed_hash


def test_reuse_reads_live_policy_without_reparsing_immutable_code(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    parser_code = contracts._referenced_global_paths.__code__
    observed = {"calls": 0}
    previous = sys.getprofile()

    def observe(frame: FrameType, event: str, arg: object) -> None:
        if (
            event == "call"
            and frame.f_code is parser_code
            and frame.f_back is not None
            and frame.f_back.f_globals is contracts.__dict__
        ):
            observed["calls"] += 1

    with minute_parameter_validation_scope():
        sys.setprofile(observe)
        try:
            first = complete_parameter_source.source_content_seed
            assert observed["calls"] > 0
            observed["calls"] = 0
            second = complete_parameter_source.source_content_seed
            third = complete_parameter_source.source_content_seed
        finally:
            sys.setprofile(previous)
        assert observed["calls"] == 0
        assert first is not second and second is not third
        assert first.model_dump_json() == second.model_dump_json() == third.model_dump_json()
        assert first.seed_hash == second.seed_hash == third.seed_hash


def test_code_path_descriptors_fall_back_within_the_shared_byte_budget(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    expected = complete_parameter_source.source_content_seed
    monkeypatch.setattr(contracts, "_MAX_PARAMETER_CONTENT_BYTES", 1)
    with minute_parameter_validation_scope():
        first = complete_parameter_source.source_content_seed
        second = complete_parameter_source.source_content_seed
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert set(state.guards) == {"content", "seed"}
        assert all(guard.code_paths is None for guard in state.guards.values())
        assert all(guard.code_paths_bytes == 0 for guard in state.guards.values())
        assert state.entries == {}
        assert state.retained_bytes == state.policy_bytes == 0
        assert first is not second
        assert first.model_dump_json() == second.model_dump_json() == expected.model_dump_json()
        assert first.seed_hash == second.seed_hash == expected.seed_hash
    assert state.guards == {} and state.retained_bytes == state.policy_bytes == 0


def test_unchanged_policy_reuse_keeps_all_guards_without_rebuilding_the_digest(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    policy_code = contracts._parameter_model_policy.__code__
    guard_code = contracts._ParameterContentGuard.assert_unchanged.__code__
    calls = {"policy": 0, "guard": 0}
    previous = sys.getprofile()

    def observe(frame: FrameType, event: str, arg: object) -> None:
        if event == "call" and frame.f_code is policy_code:
            calls["policy"] += 1
        elif event == "call" and frame.f_code is guard_code:
            calls["guard"] += 1

    with minute_parameter_validation_scope():
        first = complete_parameter_source.source_content_seed
        sys.setprofile(observe)
        try:
            second = complete_parameter_source.source_content_seed
        finally:
            sys.setprofile(previous)
        assert calls["guard"] >= 3
        assert calls["policy"] == 0
        assert first is not second
        assert first.model_dump_json() == second.model_dump_json()
        assert first.seed_hash == second.seed_hash


@pytest.mark.parametrize("fallback", (False, True), ids=("structured", "byte_fallback"))
def test_policy_reads_the_current_mutable_factory_closure(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
    monkeypatch: pytest.MonkeyPatch,
    fallback: bool,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    state = {"nested": [1]}

    def factory() -> int:
        return state["nested"][0]

    monkeypatch.setattr(MinuteGrowthParameters.model_fields["paper"], "default_factory", factory)
    if fallback:
        monkeypatch.setattr(contracts, "_MAX_PARAMETER_CONTENT_BYTES", 1)
    with minute_parameter_validation_scope():
        _ = complete_parameter_source.source_content_seed
        guards = contracts._PARAMETER_CONTENT_STATE.get().guards.values()
        assert all((guard.model_policy_probe is None) == fallback for guard in guards)
        state["nested"][0] = 2
        with pytest.raises(ExecutableDependencyError):
            _ = complete_parameter_source.source_content_seed


@pytest.mark.parametrize("change", ("nested_field", "validator", "serializer", "opaque"))
def test_policy_probe_reads_dynamic_fields_compiled_objects_and_opaque_state(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    class Opaque:
        def __init__(self) -> None:
            self.value = 1

        def __repr__(self) -> str:
            return f"Opaque({self.value})"

    opaque = Opaque()
    extra = {"nested": [{"value": 1}], "opaque": opaque}
    monkeypatch.setattr(
        contracts.MinuteParameterRuntimeContent.model_fields["source_version"],
        "json_schema_extra", extra,
    )
    with minute_parameter_validation_scope():
        _ = complete_parameter_source.source_content_seed
        if change == "nested_field":
            extra["nested"][0]["value"] = 2
        elif change == "opaque":
            opaque.value = 2
        else:
            monkeypatch.setattr(
                contracts.MinuteParameterRuntimeContent,
                "__pydantic_validator__" if change == "validator" else "__pydantic_serializer__",
                object(),
            )
        with pytest.raises(ExecutableDependencyError):
            _ = complete_parameter_source.source_content_seed


def test_policy_probe_accepts_equivalent_replaced_mutable_field_data(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    field = contracts.MinuteParameterRuntimeContent.model_fields["source_version"]
    monkeypatch.setattr(field, "json_schema_extra", {"nested": [{"value": 1}]})
    with minute_parameter_validation_scope():
        first = complete_parameter_source.source_content_seed
        field.json_schema_extra = {"nested": [{"value": 1}]}
        second = complete_parameter_source.source_content_seed
        assert first is not second
        assert first.model_dump_json() == second.model_dump_json()
        assert first.seed_hash == second.seed_hash


def test_unsupported_schema_container_keeps_the_full_original_scan(
    complete_parameter_source: FrozenMinuteParameterResearchInput,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    class OpaqueDict(dict):
        def __repr__(self) -> str:
            return "opaque-container"

    nested = OpaqueDict(value=[1])
    monkeypatch.setitem(
        contracts.MinuteParameterRuntimeContent.__pydantic_core_schema__, "probe", nested,
    )
    with minute_parameter_validation_scope():
        _ = complete_parameter_source.source_content_seed
        assert all(
            guard.model_policy_probe is None
            for guard in contracts._PARAMETER_CONTENT_STATE.get().guards.values()
        )
        nested["value"][0] = 2
        with pytest.raises(ExecutableDependencyError):
            _ = complete_parameter_source.source_content_seed
