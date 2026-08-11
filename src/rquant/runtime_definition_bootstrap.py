"""Plan and publish the immutable built-in feature and strategy definitions."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from rquant.definition_registry import (
    ImmutableDefinitionRegistry,
    _canonical_strategy_spec,
    _feature_definition_fingerprint,
    _strategy_definition_fingerprint,
    _strategy_executable_fingerprint,
)
from rquant.feature_contracts import FeatureContract, FeatureDefinition
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry


StrategyId = Literal["n_shape", "growth_board_surge", "auction_gap"]
_FEATURE_CONTRACT_ID = "intraday-pit"
_FEATURE_CONTRACT_VERSIONS = (1, 2, 3)
_LIFECYCLE_FEATURES = frozenset(
    {
        "entry_fill_status",
        "holding_trading_sessions",
        "position_sellable",
        "entry_price_raw",
        "structure_stop_price_raw",
        "eligible_high_price_raw",
        "remaining_position_fraction",
    }
)


class BuiltinDefinitionStrategyBinding(RuntimeContractModel):
    strategy_id: StrategyId
    strategy_version: Literal[1] = 1
    registration_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_schema_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    strategy_spec_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    executable_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class BuiltinDefinitionBootstrapPlan(RuntimeContractModel):
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    feature_contract_versions: tuple[int, ...]
    feature_contract_fingerprints: tuple[str, ...]
    strategies: tuple[BuiltinDefinitionStrategyBinding, ...]
    plan_id: str = ""

    @field_validator("feature_contract_versions")
    @classmethod
    def require_contiguous_versions(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if value != tuple(range(1, len(value) + 1)):
            raise ValueError("feature contract versions must be contiguous from one")
        return value

    @field_validator("feature_contract_fingerprints")
    @classmethod
    def require_feature_fingerprints(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(
            len(item) != 64 or any(character not in "0123456789abcdef" for character in item)
            for item in value
        ):
            raise ValueError("feature contract fingerprints must be lowercase SHA-256 values")
        return value

    @field_validator("strategies")
    @classmethod
    def canonicalize_strategies(
        cls,
        value: tuple[BuiltinDefinitionStrategyBinding, ...],
    ) -> tuple[BuiltinDefinitionStrategyBinding, ...]:
        return tuple(sorted(value, key=lambda item: item.strategy_id))

    @model_validator(mode="after")
    def validate_plan(self) -> Self:
        if len(self.feature_contract_versions) != len(self.feature_contract_fingerprints):
            raise ValueError("feature contract versions and fingerprints must align")
        strategy_ids = tuple(item.strategy_id for item in self.strategies)
        if strategy_ids != ("auction_gap", "growth_board_surge", "n_shape"):
            raise ValueError("definition bootstrap requires exactly the three built-in strategies")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"plan_id"}))
        if self.plan_id and self.plan_id != expected:
            raise ValueError("definition bootstrap plan id does not match content")
        object.__setattr__(self, "plan_id", expected)
        return self


def _feature_contracts(
    registry: BuiltinStrategyEvaluatorRegistry,
    *,
    producer_commit: str,
) -> tuple[FeatureContract, ...]:
    definitions = tuple(registry.definitions.values())
    feature_names = sorted(
        {
            requirement.name
            for definition in definitions
            for requirement in (
                *definition.spec.required_features,
                *definition.spec.optional_features,
            )
        }
    )
    static_dtypes: dict[str, str] = {}
    for definition in definitions:
        for name, semantic in definition.static_feature_schema.items():
            previous = static_dtypes.setdefault(name, semantic.dtype)
            if previous != semantic.dtype:
                raise ValueError(f"built-in static feature dtype conflict: {name}")

    features: list[FeatureDefinition] = []
    for name in feature_names:
        if name in _LIFECYCLE_FEATURES:
            source_datasets = ("paper_execution",)
            lookback = 0
            pit_rule = "latest execution authority available_at <= decision_time"
            max_delay_seconds = 1
            missing_policy = "fail_closed"
            late_policy = "fail_closed"
        elif name in static_dtypes:
            source_datasets = ("strategy_candidate",)
            lookback = 0
            pit_rule = "candidate snapshot available_at <= decision_time"
            max_delay_seconds = 60
            missing_policy = "fail_closed"
            late_policy = "fail_closed"
        else:
            source_datasets = ("market_minute",)
            lookback = 90
            pit_rule = "all source available_at values <= decision_time"
            max_delay_seconds = 60
            missing_policy = "mark_unavailable"
            late_policy = "mark_stale"
        features.append(
            FeatureDefinition(
                name=name,
                dtype=static_dtypes.get(name, "object"),
                source_datasets=source_datasets,
                lookback=lookback,
                pit_rule=pit_rule,
                price_basis="raw",
                availability_contract={
                    "source_available_at_basis": "per_candidate_source_available_at",
                    "max_delay_seconds": max_delay_seconds,
                    "missing_policy": missing_policy,
                    "late_policy": late_policy,
                    "decision_visibility_gate": "available_at_lte_decision_time",
                },
            )
        )
    return tuple(
        FeatureContract(
            contract_id=_FEATURE_CONTRACT_ID,
            version=version,
            features=tuple(features),
            producer_commit=producer_commit,
        )
        for version in _FEATURE_CONTRACT_VERSIONS
    )


def plan_builtin_definitions(*, producer_commit: str) -> BuiltinDefinitionBootstrapPlan:
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=producer_commit)
    execution_registry = registry.trusted_executable_registry()
    contracts = _feature_contracts(registry, producer_commit=producer_commit)
    feature_fingerprints: list[str] = []
    parent_fingerprint: str | None = None
    for contract in contracts:
        fingerprint = _feature_definition_fingerprint(
            contract,
            execution_registry.feature_bindings(contract),
            parent_fingerprint=parent_fingerprint,
            supersedes=None if parent_fingerprint is None else contract.version - 1,
            replacement_reason=None if parent_fingerprint is None else "contract evolution",
        )
        feature_fingerprints.append(fingerprint)
        parent_fingerprint = fingerprint
    if parent_fingerprint is None:  # pragma: no cover - fixed built-in contract set
        raise ValueError("built-in feature contract plan is empty")

    strategies: list[BuiltinDefinitionStrategyBinding] = []
    for definition in sorted(
        registry.definitions.values(),
        key=lambda item: item.strategy_id,
    ):
        spec = _canonical_strategy_spec(definition.spec)
        execution_binding = execution_registry.strategy_binding(spec)
        strategies.append(
            BuiltinDefinitionStrategyBinding(
                strategy_id=definition.strategy_id,
                strategy_version=definition.strategy_version,
                registration_fingerprint=_strategy_definition_fingerprint(
                    spec,
                    execution_binding,
                    feature_contract_fingerprint=parent_fingerprint,
                    parent_fingerprint=None,
                    supersedes=None,
                    replacement_reason=None,
                ),
                candidate_schema_fingerprint=execution_binding.candidate_schema_fingerprint,
                strategy_spec_fingerprint=spec.spec_fingerprint,
                executable_fingerprint=_strategy_executable_fingerprint(
                    spec,
                    execution_binding,
                ),
            )
        )
    return BuiltinDefinitionBootstrapPlan(
        producer_commit=producer_commit,
        feature_contract_versions=_FEATURE_CONTRACT_VERSIONS,
        feature_contract_fingerprints=tuple(feature_fingerprints),
        strategies=tuple(strategies),
    )


def bootstrap_builtin_definitions(
    root: Path,
    *,
    producer_commit: str,
    registered_at: AwareUtcDatetime,
    available_at: AwareUtcDatetime,
    expected_plan_id: str,
) -> BuiltinDefinitionBootstrapPlan:
    target = Path(root)
    if not target.is_absolute() or target != Path(os.path.abspath(target)):
        raise ValueError("definition registry root must be absolute and normalized")
    plan = plan_builtin_definitions(producer_commit=producer_commit)
    if plan.plan_id != expected_plan_id:
        raise ValueError("definition bootstrap plan id changed")

    evaluator_registry = BuiltinStrategyEvaluatorRegistry(producer_commit=producer_commit)
    definition_registry = ImmutableDefinitionRegistry(
        target,
        execution_registry=evaluator_registry.trusted_executable_registry(),
    )
    contracts = _feature_contracts(evaluator_registry, producer_commit=producer_commit)
    parent = None
    for contract, planned_fingerprint in zip(
        contracts,
        plan.feature_contract_fingerprints,
        strict=True,
    ):
        parent = definition_registry.register_feature_contract(
            contract,
            registered_at=registered_at,
            available_at=available_at,
            producer_commit=producer_commit,
            expected_fingerprint=contract.contract_fingerprint,
            parent_fingerprint=None if parent is None else parent.fingerprint,
            supersedes=None if parent is None else parent.version,
            replacement_reason=None if parent is None else "contract evolution",
        )
        if parent.fingerprint != planned_fingerprint:
            raise ValueError("published feature definition differs from bootstrap plan")
    if parent is None:  # pragma: no cover - fixed built-in contract set
        raise ValueError("built-in feature contract publication is empty")

    planned_by_id = {binding.strategy_id: binding for binding in plan.strategies}
    for definition in sorted(
        evaluator_registry.definitions.values(),
        key=lambda item: item.strategy_id,
    ):
        record = definition_registry.register_strategy_spec(
            definition.spec,
            feature_contract_fingerprint=parent.fingerprint,
            registered_at=registered_at,
            available_at=available_at,
            producer_commit=producer_commit,
            expected_fingerprint=definition.spec.spec_fingerprint,
        )
        planned = planned_by_id[definition.strategy_id]
        if (
            record.fingerprint != planned.registration_fingerprint
            or record.candidate_schema_fingerprint != planned.candidate_schema_fingerprint
            or record.executable_fingerprint != planned.executable_fingerprint
        ):
            raise ValueError(
                "published strategy definition differs from bootstrap plan: "
                f"{definition.strategy_id} "
                f"registration={record.fingerprint == planned.registration_fingerprint} "
                f"candidate={record.candidate_schema_fingerprint == planned.candidate_schema_fingerprint} "
                f"executable={record.executable_fingerprint == planned.executable_fingerprint}"
            )
    return plan


__all__ = [
    "BuiltinDefinitionBootstrapPlan",
    "BuiltinDefinitionStrategyBinding",
    "bootstrap_builtin_definitions",
    "plan_builtin_definitions",
]
