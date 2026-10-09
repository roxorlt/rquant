"""Reuse only the original pure builtin plan behind its live dependency guard."""

from __future__ import annotations

import inspect
import sys
import threading
from dataclasses import dataclass

from rquant import runtime_definition_bootstrap, strategy_evaluators
from rquant.definition_registry import TrustedExecutableRegistry
from rquant.executable_dependencies import (
    ExecutableBinding, ExecutableDependencyError, ExecutableDependencyGuard,
    capture_executable_dependency_guard,
)
from rquant.runtime_definition_bootstrap import BuiltinDefinitionBootstrapPlan, plan_builtin_definitions
from rquant.strategy_evaluators import BuiltinStrategyDefinition, BuiltinStrategyEvaluatorRegistry, StrategySpec


@dataclass(frozen=True, slots=True)
class _OriginalPlanCache:
    producer_commit: str
    payload: bytes
    guards: tuple[ExecutableDependencyGuard, ...]


_LOCK = threading.RLock()
_CACHE: _OriginalPlanCache | None = None
_CANDIDATE_SCHEMA_GETTER = BuiltinStrategyDefinition.candidate_schema_fingerprint.fget
_STRATEGY_SPEC_GETTER = StrategySpec.spec_fingerprint.fget
_PROPERTY_BINDINGS = (
    (BuiltinStrategyDefinition, "candidate_schema_fingerprint", inspect.getattr_static(BuiltinStrategyDefinition, "candidate_schema_fingerprint")),
    (StrategySpec, "spec_fingerprint", inspect.getattr_static(StrategySpec, "spec_fingerprint")),
)


def _original_plan_metadata_constants() -> tuple[object, ...]:
    return (
        runtime_definition_bootstrap._FEATURE_CONTRACT_ID,
        runtime_definition_bootstrap._FEATURE_CONTRACT_VERSIONS,
        runtime_definition_bootstrap._LIFECYCLE_FEATURES,
        runtime_definition_bootstrap.EXECUTION_LIFECYCLE_MAX_DELAY_SECONDS,
        runtime_definition_bootstrap.MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS,
        strategy_evaluators._EVALUATOR_SEMANTIC_VERSION,
        strategy_evaluators._LIFECYCLE_FEATURES,
    )


def _plan_dependency_guards(registry: BuiltinStrategyEvaluatorRegistry) -> tuple[ExecutableDependencyGuard, ...]:
    from rquant import definition_registry, executable_dependencies, runtime_definition_bootstrap, strategy_evaluators
    from rquant.minute_backtest_definition import build_minute_definition
    from rquant.portfolio_backtest_definition import build_portfolio_definition

    execution = registry.trusted_executable_registry()
    # The original graph follows module references. Explicit method/builder roots
    # also cover calls made through newly constructed immutable registry objects.
    native_functions = (
        _original_plan_metadata_constants,
        *(item.evaluator for item in execution._features.values()),
        *(implementation for item in execution._strategies.values()
          for implementation in (item.entry_evaluator, item.exit_evaluator, item.runtime_evaluator)),
    )
    module = sys.modules[__name__]
    native_bindings = tuple(ExecutableBinding.from_callable(function) for function in native_functions)
    recipe_functions = (
        plan_builtin_definitions,
        BuiltinStrategyEvaluatorRegistry.__init__,
        BuiltinStrategyEvaluatorRegistry.load_definition,
        BuiltinStrategyEvaluatorRegistry.trusted_executable_registry,
        TrustedExecutableRegistry.__init__,
        TrustedExecutableRegistry.feature_bindings,
        TrustedExecutableRegistry.strategy_binding,
        BuiltinStrategyDefinition.__post_init__,
        runtime_definition_bootstrap._feature_contracts,
        definition_registry._canonical_strategy_spec,
        definition_registry._feature_definition_fingerprint,
        definition_registry._strategy_definition_fingerprint,
        definition_registry._strategy_executable_fingerprint,
        strategy_evaluators._build_n_shape_definition,
        strategy_evaluators._build_growth_board_surge_definition,
        strategy_evaluators._build_auction_gap_definition,
        build_minute_definition, build_portfolio_definition,
        definition_registry._callable_fingerprint,
        executable_dependencies.fingerprint_callable,
        executable_dependencies.fingerprint_executable_bindings,
        executable_dependencies._capture_executable_bindings,
    )
    return (
        capture_executable_dependency_guard(native_bindings, contract="minute-original-plan-native-dependencies/v1"),
        # Fingerprinting itself reads internal opaque sentinels. Its exact code
        # and bindings are guarded separately, without fingerprinting itself's
        # runtime bookkeeping as if it were an input to native strategy math.
        capture_executable_dependency_guard((*tuple(ExecutableBinding.from_callable(function) for function in recipe_functions),
            ExecutableBinding(module, ("_CANDIDATE_SCHEMA_GETTER",), _CANDIDATE_SCHEMA_GETTER),
            ExecutableBinding(module, ("_STRATEGY_SPEC_GETTER",), _STRATEGY_SPEC_GETTER)),
            contract="minute-original-plan-recipe/v1", include_global_dependencies=False),
    )


def original_builtin_minute_plan(*, producer_commit: str) -> BuiltinDefinitionBootstrapPlan:
    """No receipt, source bytes, physical authority, or owner is cached here."""
    global _CACHE

    with _LOCK:
        if any(inspect.getattr_static(owner, name) is not expected for owner, name, expected in _PROPERTY_BINDINGS):
            raise ValueError("minute original builtin plan executable dependency changed")
        if _CACHE is None or _CACHE.producer_commit != producer_commit:
            registry = BuiltinStrategyEvaluatorRegistry(producer_commit=producer_commit)
            guards = _plan_dependency_guards(registry)
            plan = plan_builtin_definitions(producer_commit=producer_commit)
            for guard in guards:
                guard.assert_unchanged()
            _CACHE = _OriginalPlanCache(producer_commit, plan.model_dump_json().encode(), guards)
        try:
            for guard in _CACHE.guards:
                guard.assert_unchanged()
        except ExecutableDependencyError as error:
            _CACHE = None
            raise ValueError("minute original builtin plan executable dependency changed") from error
        # Bytes are the one bounded cache value; every caller receives a fresh
        # frozen typed plan, so even a forced mutation cannot pollute later reads.
        return BuiltinDefinitionBootstrapPlan.model_validate_json(_CACHE.payload)
