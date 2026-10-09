"""Fresh content-addressed parameter definitions without replacing native @1."""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from rquant.minute_backtest_parameter_contracts import _ParameterReadUnitContentEntries

from rquant.definition_registry import (
    FeatureExecutionBinding, ImmutableDefinitionRegistry, MinuteParameterExitRule, StrategySpecRegistration,
    TrustedExecutableRegistry, TrustedFeatureImplementation, TrustedStrategyImplementation,
    _canonical_feature_contract, _canonical_strategy_spec,
)
from rquant.executable_dependencies import (
    ExecutableBinding, ExecutableDependencyError, ExecutableDependencyGuard,
    capture_executable_dependency_guard,
)
from rquant.feature_contracts import FeatureContract, FeatureDefinition, FeatureRequirement, RequirementLevel
from rquant.intraday_feature_engine import MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS
from rquant.minute_backtest_contracts import MinuteReplayModel, Sha256
from rquant.minute_backtest_parameter_evaluators import (
    parameter_entry_evaluator, parameter_exit_evaluator, parameter_runtime_evaluator,
    parameter_intent_validity_seconds,
)
from rquant.minute_backtest_parameter_features import (
    PARAMETER_BAR_FEATURE, PARAMETER_CANDIDATE_FEATURE, PARAMETER_FEATURE_CONTRACT,
    PARAMETER_LIFECYCLE_FEATURE, project_minute_parameter_features,
    project_minute_parameter_lifecycle,
)
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.signal_contracts import SignalAction
from rquant.strategy_evaluators import (
    _LIFECYCLE_FEATURES, BuiltinStrategyDefinition, StaticFeatureSemantic,
    _definition, _execution_lifecycle_transitions, project_execution_lifecycle_features,
)
from rquant.strategy_spec import StateTransition, StrategyLifecycleState, StrategyRunMode, StrategySpec


@dataclass(frozen=True)
class MinuteParameterDefinition(BuiltinStrategyDefinition):
    exit_rules: tuple[MinuteParameterExitRule, ...]


def build_minute_parameter_definition(parameters: MinuteParameterSet, *, producer_commit: str) -> MinuteParameterDefinition:
    required = (PARAMETER_CANDIDATE_FEATURE, PARAMETER_BAR_FEATURE, "latest_close", "session_low", "session_high")
    optional = (*_LIFECYCLE_FEATURES, PARAMETER_LIFECYCLE_FEATURE)
    transitions = (
        StateTransition(from_state="idle", event="entry_ready", to_state="armed"),
        StateTransition(from_state="armed", event="entry_filled", to_state="holding"),
        StateTransition(from_state="armed", event="entry_rejected", to_state="terminal"),
        StateTransition(from_state="holding", event="exit", to_state="holding"),
        StateTransition(from_state="holding", event="exit_filled", to_state="terminal"),
    )
    spec = StrategySpec(strategy_id=parameters.definition_id, version=parameters.definition_version,
        feature_contract_id=PARAMETER_FEATURE_CONTRACT, min_feature_contract_version=1,
        required_features=tuple(FeatureRequirement(name=name, level=RequirementLevel.REQUIRED,
            min_contract_version=1) for name in required),
        optional_features=tuple(FeatureRequirement(name=name, level=RequirementLevel.OPTIONAL,
            min_contract_version=1) for name in optional), initial_state=StrategyLifecycleState.IDLE,
        transitions=transitions, parameters={"minute_parameter_set_json": parameters.model_dump_json(),
            "minute_parameter_set_hash": parameters.fingerprint, "semantic_version": parameters.evaluator_semantic_version,
            "intent_validity_seconds": parameter_intent_validity_seconds(parameters),
            "intent_validity_basis": "frequency_plus_original_120s_margin",
            "decision_basis": "visible_bar_close", "execution_basis": "original_pit_paper_broker"},
        allowed_actions=(SignalAction.B_INTENT.value, SignalAction.S_INTENT.value),
        run_mode=StrategyRunMode.PAPER, producer_commit=producer_commit)
    rule = MinuteParameterExitRule(event="exit", fill_event="exit_filled", action=SignalAction.S_INTENT,
        evaluator_id=f"{parameter_exit_evaluator.__module__}:{parameter_exit_evaluator.__qualname__}",
        eligibility={"settlement_rule": "a_share_t_plus_one", "minimum_holding_trading_sessions": 1,
            "same_day_sell_allowed": False, "sellable_position_required": True},
        price_basis={"adjustment_basis": "raw", "decision_price": "minute_close", "execution_price": "next_trade"},
        sell_tranche={"sequence": 1, "position_fraction": 1.0, "reevaluate_after_fill": False, "terminal_after_fill": True},
        parameter_json=parameters.model_dump_json(), parameter_fingerprint=parameters.fingerprint)
    return MinuteParameterDefinition(contract_schema_version=2, strategy_id=spec.strategy_id,
        strategy_version=spec.version, evaluator_semantic_version=parameters.evaluator_semantic_version,
        spec=spec, static_feature_schema={PARAMETER_CANDIDATE_FEATURE: StaticFeatureSemantic("string", "complete_parameter_bound_past_candidate_json")},
        allowed_actions=(SignalAction.B_INTENT, SignalAction.S_INTENT), producer_commit=producer_commit,
        entry_evaluator=parameter_entry_evaluator, exit_evaluator=parameter_exit_evaluator,
        exit_rules=(rule,), evaluator=parameter_runtime_evaluator)


def minute_parameter_feature_contract(definition: MinuteParameterDefinition) -> FeatureContract:
    fields = []
    for requirement in (*definition.spec.required_features, *definition.spec.optional_features):
        name = requirement.name
        lifecycle = name in _LIFECYCLE_FEATURES or name == PARAMETER_LIFECYCLE_FEATURE
        static = name == PARAMETER_CANDIDATE_FEATURE
        fields.append(FeatureDefinition(name=name, dtype="string" if name.startswith("minute_parameter_") else "object",
            source_datasets=("paper_execution",) if lifecycle else (("strategy_candidate",) if static else ("market_minute",)),
            lookback=0 if lifecycle or static else 90, price_basis="raw",
            pit_rule="complete original execution/source publication available_at <= decision_time",
            availability_contract={"source_available_at_basis": "per_candidate_source_available_at",
                "max_delay_seconds": 2 * MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS if lifecycle else MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS,
                "missing_policy": "mark_unavailable" if name == PARAMETER_LIFECYCLE_FEATURE else (
                    "fail_closed" if lifecycle or static else "mark_unavailable"),
                "late_policy": "fail_closed" if lifecycle or static else "mark_stale",
                "decision_visibility_gate": "available_at_lte_decision_time"}))
    return FeatureContract(contract_id=PARAMETER_FEATURE_CONTRACT, version=1,
        features=tuple(fields), producer_commit=definition.producer_commit)


def minute_parameter_executable_registry(definition: MinuteParameterDefinition) -> TrustedExecutableRegistry:
    return TrustedExecutableRegistry(features=tuple(TrustedFeatureImplementation(feature_name=field.name,
        implementation_version=definition.evaluator_semantic_version,
        evaluator=project_execution_lifecycle_features if field.name in _LIFECYCLE_FEATURES else (
            project_minute_parameter_lifecycle if field.name == PARAMETER_LIFECYCLE_FEATURE else project_minute_parameter_features))
        for field in minute_parameter_feature_contract(definition).features),
        strategies=(TrustedStrategyImplementation(strategy_id=definition.strategy_id,
            implementation_version=definition.evaluator_semantic_version,
            candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
            entry_evaluator=definition.entry_evaluator, exit_evaluator=definition.exit_evaluator,
            runtime_evaluator=definition.evaluator, entry_event="entry_filled", exit_rules=definition.exit_rules),))


def bootstrap_minute_parameter_definition(root: Path, parameters: MinuteParameterSet, *, producer_commit: str,
        registered_at: datetime, available_at: datetime) -> StrategySpecRegistration:
    definition = build_minute_parameter_definition(parameters, producer_commit=producer_commit)
    registry = ImmutableDefinitionRegistry(root, execution_registry=minute_parameter_executable_registry(definition))
    contract = minute_parameter_feature_contract(definition)
    feature = registry.register_feature_contract(contract, registered_at=registered_at,
        available_at=available_at, producer_commit=producer_commit, expected_fingerprint=contract.contract_fingerprint)
    record = registry.register_strategy_spec(definition.spec, feature_contract_fingerprint=feature.fingerprint,
        registered_at=registered_at, available_at=available_at, producer_commit=producer_commit,
        expected_fingerprint=definition.spec.spec_fingerprint)
    if record.executable_fingerprint != definition.executable_fingerprint:
        raise PermissionError("parameter registration differs from its complete trusted executable")
    return record


def build_minute_parameter_research_definition(producer_commit: str) -> BuiltinStrategyDefinition:
    from rquant.minute_backtest_definition import _replay_only

    actions = (SignalAction.WATCH, SignalAction.B_INTENT, SignalAction.REDUCE, SignalAction.S_INTENT)
    spec = StrategySpec(strategy_id="minute_parameter_replay", version=1,
        feature_contract_id=PARAMETER_FEATURE_CONTRACT, min_feature_contract_version=1,
        required_features=(), optional_features=tuple(FeatureRequirement(name=name,
            level=RequirementLevel.OPTIONAL, min_contract_version=1) for name in _LIFECYCLE_FEATURES),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(StateTransition(from_state="idle", event="entry_ready", to_state="armed"),
            *_execution_lifecycle_transitions()), parameters={"replay_only": True,
            "source_contract": "minute-parameter-replay-input/v1", "adapter_version": "1",
            "valuation_basis": "pit_asof_15:00", "parameter_contract": "minute-parameter-set/v1"},
        allowed_actions=tuple(action.value for action in actions), run_mode=StrategyRunMode.SHADOW,
        producer_commit=producer_commit)
    return _definition(spec=spec,
        static_feature_schema={"entry_fill_status": StaticFeatureSemantic("string", "execution_ledger_only")},
        allowed_actions=actions, entry_evaluator=_replay_only, evaluator=_replay_only)


def minute_parameter_research_registry(parameters: MinuteParameterSet, *, producer_commit: str) -> TrustedExecutableRegistry:
    definition = build_minute_parameter_definition(parameters, producer_commit=producer_commit)
    wrapper = build_minute_parameter_research_definition(producer_commit)
    native = minute_parameter_executable_registry(definition)
    return TrustedExecutableRegistry(features=tuple(native._features.values()),
        strategies=(*tuple(native._strategies.values()), TrustedStrategyImplementation(
            strategy_id=wrapper.strategy_id, implementation_version=wrapper.evaluator_semantic_version,
            candidate_schema_fingerprint=wrapper.candidate_schema_fingerprint,
            entry_evaluator=wrapper.entry_evaluator, exit_evaluator=wrapper.exit_evaluator,
            runtime_evaluator=wrapper.evaluator, entry_event="entry_filled", exit_rules=wrapper.exit_rules)))


def bootstrap_minute_parameter_research_definition(root: Path, parameters: MinuteParameterSet, *,
    producer_commit: str, now: datetime,
) -> StrategySpecRegistration:
    registry = ImmutableDefinitionRegistry(root,
        execution_registry=minute_parameter_research_registry(parameters, producer_commit=producer_commit))
    native = registry.latest_strategy_spec(parameters.definition_id, as_of=now)
    if native is None:
        raise PermissionError("parameter wrapper requires its complete real native registration")
    definition = build_minute_parameter_research_definition(producer_commit)
    return registry.register_strategy_spec(definition.spec,
        feature_contract_fingerprint=native.feature_contract_fingerprint,
        registered_at=now, available_at=now, producer_commit=producer_commit,
        expected_fingerprint=definition.spec.spec_fingerprint)


class MinuteParameterValidationPlan(MinuteReplayModel):
    """Pure owner outputs only; no receipt, source, role or physical authority."""

    parameters: MinuteParameterSet
    native_spec: StrategySpec
    native_executable_fingerprint: Sha256
    candidate_schema_fingerprint: Sha256
    feature_contract: FeatureContract
    feature_bindings: tuple[FeatureExecutionBinding, ...]
    wrapper_spec: StrategySpec
    wrapper_executable_fingerprint: Sha256
    wrapper_candidate_schema_fingerprint: Sha256


@dataclass(frozen=True, slots=True)
class _ParameterValidationEntry:
    payload: str
    payload_sha256: str
    builder_guard: ExecutableDependencyGuard
    executable_guard: ExecutableDependencyGuard


_PARAMETER_VALIDATION_STATE: ContextVar[dict[tuple[str, str], _ParameterValidationEntry] | None] = ContextVar(
    "minute_parameter_request_pure_definitions", default=None)
_MAX_VALIDATION_PLANS = 8
_P = ParamSpec("_P")
_T = TypeVar("_T")


@contextmanager
def minute_parameter_validation_scope() -> Iterator[None]:
    if _PARAMETER_VALIDATION_STATE.get() is not None:
        yield
        return
    state: dict[tuple[str, str], _ParameterValidationEntry] = {}
    token = _PARAMETER_VALIDATION_STATE.set(state)
    try:
        from rquant.minute_backtest_parameter_contracts import _minute_parameter_content_scope
        from rquant.minute_backtest_parameter_source import minute_parameter_work_scope

        with minute_parameter_work_scope(), _minute_parameter_content_scope():
            yield
    finally:
        state.clear()
        _PARAMETER_VALIDATION_STATE.reset(token)


@contextmanager
def _minute_parameter_read_validation_entries() -> Iterator[_ParameterReadUnitContentEntries | None]:
    from rquant.minute_backtest_parameter_contracts import (
        _PARAMETER_CONTENT_STATE, _parameter_read_unit_content_entries, _release_parameter_validation_guard_bytes,
    )

    state = _PARAMETER_VALIDATION_STATE.get()
    if state is None:
        yield None
        return
    before = dict(state)
    control_bytes = sys.getsizeof(before)
    control_bytes += sum(sys.getsizeof(key) + sum(sys.getsizeof(part) for part in key) for key in before)
    with _parameter_read_unit_content_entries(control_bytes=control_bytes) as lease:
        if lease is None:
            before.clear()
            yield None
            return
        try:
            yield lease
        finally:
            if _PARAMETER_VALIDATION_STATE.get() is state and _PARAMETER_CONTENT_STATE.get() is lease.state:
                for key, entry in tuple(state.items()):
                    if key not in before and state.get(key) is entry:
                        del state[key]
                        _release_parameter_validation_guard_bytes(
                            entry.builder_guard.code_plan_retained_bytes + entry.executable_guard.code_plan_retained_bytes,
                        )
            before.clear()


def minute_parameter_validation_request(function: Callable[_P, _T]) -> Callable[_P, _T]:
    @wraps(function)
    def invoke(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        with minute_parameter_validation_scope():
            return function(*args, **kwargs)
    return invoke


def minute_parameter_validation_plan(parameters: MinuteParameterSet, *,
    producer_commit: str,
) -> MinuteParameterValidationPlan | None:
    state = _PARAMETER_VALIDATION_STATE.get()
    if state is None:
        return None
    key = (parameters.model_dump_json(), producer_commit)
    entry = state.get(key)
    if entry is None:
        builders = (build_minute_parameter_definition, minute_parameter_feature_contract,
            minute_parameter_executable_registry, build_minute_parameter_research_definition)
        # Builders read Enum properties. Rebuild their full data on every use,
        # and separately guard code/defaults and the actual executable graph.
        builder_guard = capture_executable_dependency_guard(
            tuple(ExecutableBinding.from_callable(root) for root in builders),
            contract="minute-parameter-pure-validation-builders/v1", include_global_dependencies=False)
    else:
        entry.builder_guard.assert_unchanged()
        entry.executable_guard.assert_unchanged()
    definition = build_minute_parameter_definition(parameters, producer_commit=producer_commit)
    contract = _canonical_feature_contract(minute_parameter_feature_contract(definition))
    wrapper = build_minute_parameter_research_definition(producer_commit)
    executable_roots = (
        definition.entry_evaluator, definition.exit_evaluator, definition.evaluator,
        project_execution_lifecycle_features, project_minute_parameter_features,
        project_minute_parameter_lifecycle, wrapper.entry_evaluator,
        wrapper.exit_evaluator, wrapper.evaluator,
    )
    if entry is None:
        executable_guard = capture_executable_dependency_guard(
            tuple(ExecutableBinding.from_callable(root) for root in executable_roots),
            contract="minute-parameter-pure-validation-executables/v1")
        from rquant.minute_backtest_parameter_contracts import (
            _adopt_parameter_validation_guards, _release_parameter_validation_guard_bytes,
        )

        (builder_guard, executable_guard), reserved = _adopt_parameter_validation_guards(
            (builder_guard, executable_guard), retain=len(state) < _MAX_VALIDATION_PLANS,
        )
        retained = False
        try:
            trusted = minute_parameter_executable_registry(definition)
            plan = MinuteParameterValidationPlan(parameters=parameters,
                native_spec=_canonical_strategy_spec(definition.spec),
                native_executable_fingerprint=definition.executable_fingerprint,
                candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
                feature_contract=contract, feature_bindings=trusted.feature_bindings(contract),
                wrapper_spec=_canonical_strategy_spec(wrapper.spec),
                wrapper_executable_fingerprint=wrapper.executable_fingerprint,
                wrapper_candidate_schema_fingerprint=wrapper.candidate_schema_fingerprint)
            builder_guard.assert_unchanged()
            executable_guard.assert_unchanged()
            payload = plan.model_dump_json(exclude_computed_fields=True)
            entry = _ParameterValidationEntry(payload=payload,
                payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
                builder_guard=builder_guard, executable_guard=executable_guard)
            if len(state) < _MAX_VALIDATION_PLANS:
                state[key] = entry
                retained = True
        finally:
            if not retained:
                _release_parameter_validation_guard_bytes(reserved)
    if hashlib.sha256(entry.payload.encode()).hexdigest() != entry.payload_sha256:
        raise ExecutableDependencyError("minute parameter pure validation payload changed")
    # Each caller receives a new frozen model, rather than the cached object.
    plan = MinuteParameterValidationPlan.model_validate_json(entry.payload)
    if plan.parameters != parameters or plan.native_spec.producer_commit != producer_commit:
        raise ExecutableDependencyError("minute parameter pure validation recipe changed")
    current_bindings = {(binding.owner_module_name, binding.binding_path): binding.implementation
        for binding in (ExecutableBinding.from_callable(root) for root in executable_roots)}
    if any(current_bindings.get((binding.owner_module_name, binding.binding_path)) is not binding.implementation
            for binding in entry.executable_guard.bindings):
        raise ExecutableDependencyError("minute parameter pure validation executable binding changed")
    if (plan.native_spec, plan.candidate_schema_fingerprint, plan.feature_contract,
            plan.wrapper_spec, plan.wrapper_candidate_schema_fingerprint) != (
            _canonical_strategy_spec(definition.spec), definition.candidate_schema_fingerprint, contract,
            _canonical_strategy_spec(wrapper.spec), wrapper.candidate_schema_fingerprint):
        raise ExecutableDependencyError("minute parameter pure validation owner output changed")
    entry.builder_guard.assert_unchanged()
    entry.executable_guard.assert_unchanged()
    return plan
