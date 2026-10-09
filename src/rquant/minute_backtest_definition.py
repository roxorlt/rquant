"""Trusted research wrapper; native paper evaluators remain the execution authority."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from rquant.definition_registry import ImmutableDefinitionRegistry, StrategySpecRegistration
from rquant.signal_contracts import SignalAction
from rquant.strategy_evaluators import (
    _LIFECYCLE_FEATURES, BuiltinStrategyDefinition, StaticFeatureSemantic,
    _definition, _execution_lifecycle_transitions, _spec,
)
from rquant.strategy_spec import StateTransition, StrategyLifecycleState


def _replay_only(*args: object, **kwargs: object) -> object:
    raise PermissionError("minute runtime definition is research replay-only")


def build_minute_definition(producer_commit: str) -> BuiltinStrategyDefinition:
    actions = (SignalAction.WATCH, SignalAction.B_INTENT, SignalAction.REDUCE, SignalAction.S_INTENT)
    spec = _spec(strategy_id="minute_runtime_replay", producer_commit=producer_commit,
        required=(), optional=_LIFECYCLE_FEATURES,
        transitions=(StateTransition(from_state=StrategyLifecycleState.IDLE, event="entry_ready",
            to_state=StrategyLifecycleState.ARMED), *_execution_lifecycle_transitions()),
        parameters={"replay_only": True, "source_contract": "minute-runtime-replay-input/v2",
            "adapter_version": "2", "valuation_basis": "pit_asof_15:00"}, allowed_actions=actions)
    return _definition(spec=spec,
        static_feature_schema={"entry_fill_status": StaticFeatureSemantic("string", "execution_ledger_only")},
        allowed_actions=actions, entry_evaluator=_replay_only, evaluator=_replay_only)


def bootstrap_minute_definition(root: Path, *, producer_commit: str, now: datetime) -> StrategySpecRegistration:
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
    from rquant.runtime_definition_bootstrap import plan_builtin_definitions

    builtins = BuiltinStrategyEvaluatorRegistry(producer_commit=producer_commit)
    registry = ImmutableDefinitionRegistry(root, execution_registry=builtins.trusted_executable_registry())
    definition = builtins.load_definition("minute_runtime_replay", 1)
    existing = registry.latest_strategy_spec("minute_runtime_replay", as_of=now)
    if existing is not None:
        if (existing.producer_commit, existing.spec, existing.executable_fingerprint,
            existing.candidate_schema_fingerprint) != (producer_commit, definition.spec,
                definition.executable_fingerprint, definition.candidate_schema_fingerprint):
            raise PermissionError("minute wrapper existing registration differs")
        return existing
    plan = plan_builtin_definitions(producer_commit=producer_commit)
    feature = registry.read_feature_contract(plan.feature_contract_fingerprints[2], as_of=now)
    if feature is None or feature.version != 3 or feature.producer_commit != producer_commit:
        raise PermissionError("minute wrapper requires the original trusted v3 feature contract")
    return registry.register_strategy_spec(definition.spec, feature_contract_fingerprint=feature.fingerprint,
        registered_at=now, available_at=now, producer_commit=producer_commit,
        expected_fingerprint=definition.spec.spec_fingerprint)
