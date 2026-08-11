"""Immutable point-in-time registry for feature and strategy definitions."""

from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import inspect
import os
import re
import stat
import sys
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Literal, TypeVar
from uuid import uuid4

from pydantic import Field, ValidationError, model_validator

from rquant.executable_dependencies import fingerprint_callable
from rquant.feature_contracts import FeatureContract
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)
from rquant.strategy_spec import StrategySpec
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_json_loads,
    strict_model_validate_canonical_json,
)

_FINGERPRINT_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_ID_KEY_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_VERSION_PATTERN = re.compile(r"^v([1-9][0-9]*)$")
_STAGING_PATTERN = re.compile(r"^\.publish-[0-9a-f]{32}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_VERSION_BUILD_MODE = 0o300
_VERSION_FROZEN_MODE = 0o100
_VERSION_PUBLISHED_MODE = 0o500
_PRIVATE_FILE_MODE = 0o400
_MAX_RECORD_BYTES = 4 * 1024 * 1024
_MAX_STAGING_RECOVERY = 16
_MAX_RECOVERY_SCAN_ENTRIES = 4096
_MAX_LOOKUP_BUCKET_ENTRIES = 4096
_MAX_LOGICAL_DEFINITIONS = 16384
_MAX_DEFINITION_VERSIONS = 4096
_SEAL_NAME = ".sealed"
_COMMIT_NAME = ".committed"
_PUBLISH_LOCK_NAME = ".publish.lock"
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_RENAME_NOREPLACE = 1
_RENAME_EXCL = 0x00000004

RootChain = tuple[list[int], list[str]]


def _callable_identity(implementation: Callable[..., object]) -> str:
    unwrapped = inspect.unwrap(implementation)
    module = getattr(unwrapped, "__module__", "")
    qualified_name = getattr(unwrapped, "__qualname__", "")
    if (
        not inspect.isfunction(unwrapped)
        or not module
        or not qualified_name
        or "<locals>" in qualified_name
    ):
        raise TypeError("trusted implementation must be a top-level Python function")
    return f"{module}:{qualified_name}"


def _callable_fingerprint(
    implementation: Callable[..., object],
    implementation_version: str,
) -> str:
    if not implementation_version or implementation_version != implementation_version.strip():
        raise ValueError("trusted implementation version is invalid")
    return fingerprint_callable(
        inspect.unwrap(implementation),
        implementation_version=implementation_version,
        contract="trusted-executable/v3",
        require_source=True,
    )


@dataclass(frozen=True)
class _StagingRecordLease:
    descriptor: int
    identity: tuple[int, ...]
    content_sha256: str


@dataclass
class _VersionPublicationOwnership:
    owned: bool = False
    descriptor: int = -1

    def claim(self, descriptor: int) -> None:
        self.descriptor = descriptor
        self.owned = True


class DefinitionRegistryError(RuntimeError):
    """Base error for immutable definition registry operations."""


class DefinitionIntegrityError(DefinitionRegistryError):
    """Persistent definition state is malformed, unsafe, or tampered."""


class DefinitionExecutableIntegrityError(DefinitionIntegrityError):
    """Stored executable evidence no longer matches the trusted implementation graph."""


class DefinitionConflictError(DefinitionRegistryError):
    """A logical definition version already contains different content."""


class DefinitionReferenceError(DefinitionRegistryError):
    """A strategy references an invalid feature contract."""


class DefinitionSchemaCompatibility(RuntimeContractModel):
    producer_schema_version: Literal[5] = 5
    min_consumer_schema_version: Literal[5] = 5
    max_consumer_schema_version: Literal[5] = 5
    legacy_schema_policy: Literal["explicit_re_registration_required"] = (
        "explicit_re_registration_required"
    )


class FeatureExecutionBinding(RuntimeContractModel):
    feature_name: str = Field(min_length=1)
    implementation_id: str = Field(min_length=1)
    implementation_version: str = Field(min_length=1)
    formula_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    live_replay_shared: Literal[True] = True


class StrategyExitEligibility(RuntimeContractModel):
    settlement_rule: Literal["a_share_t_plus_one"]
    minimum_holding_trading_sessions: int = Field(ge=1)
    same_day_sell_allowed: Literal[False]
    sellable_position_required: Literal[True]


class StrategyExitPriceBasis(RuntimeContractModel):
    adjustment_basis: Literal["raw"]
    decision_price: Literal["minute_close", "last_trade", "vwap"]
    execution_price: Literal["next_minute_open", "next_trade"]


class StrategyStructureStop(RuntimeContractModel):
    reference: Literal["signal_support", "entry_day_low", "t_day_close"]
    buffer_bps: int = Field(ge=0, le=5_000)


class StrategyPercentStop(RuntimeContractModel):
    maximum_loss_bps: int = Field(ge=1, le=5_000)
    acts_as_fallback: Literal[True]


class StrategyTrailingTakeProfit(RuntimeContractModel):
    activation_gain_bps: int = Field(ge=1, le=20_000)
    retracement_bps: int = Field(ge=1, le=10_000)
    high_watermark: Literal["eligible_intraday_high"]


class StrategySellTranche(RuntimeContractModel):
    sequence: int = Field(ge=1)
    position_fraction: float = Field(gt=0, le=1, allow_inf_nan=False)
    reevaluate_after_fill: bool
    terminal_after_fill: bool


class StrategyExitRule(RuntimeContractModel):
    event: str = Field(min_length=1)
    fill_event: str | None = Field(default=None, min_length=1)
    action: str = Field(min_length=1)
    evaluator_id: str = Field(min_length=1)
    eligibility: StrategyExitEligibility
    price_basis: StrategyExitPriceBasis
    structure_stop: StrategyStructureStop
    percent_stop: StrategyPercentStop
    trailing_take_profit: StrategyTrailingTakeProfit
    sell_tranche: StrategySellTranche

    @model_validator(mode="after")
    def validate_sell_semantics(self) -> StrategyExitRule:
        tranche = self.sell_tranche
        if tranche.terminal_after_fill:
            if (
                self.action != "s_intent"
                or tranche.position_fraction != 1.0
                or tranche.reevaluate_after_fill
                or self.fill_event is None
            ):
                raise ValueError(
                    "terminal exit must declare intent and fill events and sell all remaining "
                    "position once"
                )
            if self.fill_event == self.event:
                raise ValueError("terminal exit intent and fill events must be distinct")
        elif (
            self.action != "reduce"
            or tranche.position_fraction >= 1.0
            or not tranche.reevaluate_after_fill
            or self.fill_event is not None
        ):
            raise ValueError("non-terminal exit must partially sell and re-evaluate")
        return self


class StrategyExecutionBinding(RuntimeContractModel):
    candidate_schema_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    entry_event: str = Field(min_length=1)
    entry_evaluator_id: str = Field(min_length=1)
    entry_evaluator_version: str = Field(default="1.0.0", min_length=1)
    entry_evaluator_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    exit_evaluator_id: str = Field(min_length=1)
    exit_evaluator_version: str = Field(default="1.0.0", min_length=1)
    exit_evaluator_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    runtime_evaluator_id: str = Field(min_length=1)
    runtime_evaluator_version: str = Field(default="1.0.0", min_length=1)
    runtime_evaluator_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision_formula_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    exit_rules: tuple[StrategyExitRule, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_exit_rules(self) -> StrategyExecutionBinding:
        events = tuple(rule.event for rule in self.exit_rules)
        if len(events) != len(set(events)):
            raise ValueError("strategy exit rule events must be unique")
        lifecycle_events = events + tuple(
            rule.fill_event for rule in self.exit_rules if rule.fill_event is not None
        )
        if len(lifecycle_events) != len(set(lifecycle_events)):
            raise ValueError("strategy exit intent and fill events must be globally unique")
        if any(rule.evaluator_id != self.exit_evaluator_id for rule in self.exit_rules):
            raise ValueError("strategy exit rules must bind the declared exit evaluator")
        expected = canonical_sha256(
            {
                "contract": "trusted-strategy-decision/v2",
                "entry": self.entry_evaluator_fingerprint,
                "exit": self.exit_evaluator_fingerprint,
                "runtime": self.runtime_evaluator_fingerprint,
            }
        )
        if self.decision_formula_fingerprint != expected:
            raise ValueError("strategy formula fingerprint does not match executable evaluators")
        return self


@dataclass(frozen=True)
class TrustedFeatureImplementation:
    feature_name: str
    implementation_version: str
    evaluator: Callable[..., object]


@dataclass(frozen=True)
class TrustedStrategyImplementation:
    strategy_id: str
    implementation_version: str
    candidate_schema_fingerprint: str
    entry_evaluator: Callable[..., object]
    exit_evaluator: Callable[..., object]
    runtime_evaluator: Callable[..., object]
    entry_event: str
    exit_rules: tuple[StrategyExitRule, ...]


class TrustedExecutableRegistry:
    """Closed allowlist of actual executable objects; no dynamic import surface."""

    def __init__(
        self,
        *,
        features: tuple[TrustedFeatureImplementation, ...],
        strategies: tuple[TrustedStrategyImplementation, ...],
    ) -> None:
        feature_map: dict[str, TrustedFeatureImplementation] = {}
        for implementation in features:
            if not implementation.feature_name or implementation.feature_name in feature_map:
                raise ValueError("trusted feature implementations must be unique")
            _callable_fingerprint(
                implementation.evaluator,
                implementation.implementation_version,
            )
            feature_map[implementation.feature_name] = implementation
        strategy_map: dict[str, TrustedStrategyImplementation] = {}
        for implementation in strategies:
            if not implementation.strategy_id or implementation.strategy_id in strategy_map:
                raise ValueError("trusted strategy implementations must be unique")
            _callable_fingerprint(
                implementation.entry_evaluator,
                implementation.implementation_version,
            )
            _callable_fingerprint(
                implementation.exit_evaluator,
                implementation.implementation_version,
            )
            _callable_fingerprint(
                implementation.runtime_evaluator,
                implementation.implementation_version,
            )
            strategy_map[implementation.strategy_id] = implementation
        self._features = MappingProxyType(feature_map)
        self._strategies = MappingProxyType(strategy_map)

    def feature_bindings(
        self,
        contract: FeatureContract,
    ) -> tuple[FeatureExecutionBinding, ...]:
        bindings: list[FeatureExecutionBinding] = []
        for feature in contract.features:
            implementation = self._features.get(feature.name)
            if implementation is None:
                raise DefinitionReferenceError(
                    f"trusted feature implementation is missing: {feature.name}"
                )
            bindings.append(
                FeatureExecutionBinding(
                    feature_name=feature.name,
                    implementation_id=_callable_identity(implementation.evaluator),
                    implementation_version=implementation.implementation_version,
                    formula_fingerprint=_callable_fingerprint(
                        implementation.evaluator,
                        implementation.implementation_version,
                    ),
                )
            )
        return tuple(sorted(bindings, key=lambda item: item.feature_name))

    def strategy_binding(self, spec: StrategySpec) -> StrategyExecutionBinding:
        implementation = self._strategies.get(spec.strategy_id)
        if implementation is None:
            raise DefinitionReferenceError(
                f"trusted strategy implementation is missing: {spec.strategy_id}"
            )
        entry_fingerprint = _callable_fingerprint(
            implementation.entry_evaluator,
            implementation.implementation_version,
        )
        exit_fingerprint = _callable_fingerprint(
            implementation.exit_evaluator,
            implementation.implementation_version,
        )
        runtime_fingerprint = _callable_fingerprint(
            implementation.runtime_evaluator,
            implementation.implementation_version,
        )
        return StrategyExecutionBinding(
            candidate_schema_fingerprint=implementation.candidate_schema_fingerprint,
            entry_event=implementation.entry_event,
            entry_evaluator_id=_callable_identity(implementation.entry_evaluator),
            entry_evaluator_version=implementation.implementation_version,
            entry_evaluator_fingerprint=entry_fingerprint,
            exit_evaluator_id=_callable_identity(implementation.exit_evaluator),
            exit_evaluator_version=implementation.implementation_version,
            exit_evaluator_fingerprint=exit_fingerprint,
            runtime_evaluator_id=_callable_identity(implementation.runtime_evaluator),
            runtime_evaluator_version=implementation.implementation_version,
            runtime_evaluator_fingerprint=runtime_fingerprint,
            decision_formula_fingerprint=canonical_sha256(
                {
                    "contract": "trusted-strategy-decision/v2",
                    "entry": entry_fingerprint,
                    "exit": exit_fingerprint,
                    "runtime": runtime_fingerprint,
                }
            ),
            exit_rules=implementation.exit_rules,
        )


class FeatureContractRegistration(RuntimeContractModel):
    schema_version: Literal[5] = 5
    schema_compatibility: DefinitionSchemaCompatibility = Field(
        default_factory=DefinitionSchemaCompatibility
    )
    kind: Literal["feature_contract"] = "feature_contract"
    logical_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    registered_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    supersedes: int | None = Field(default=None, ge=1)
    replacement_reason: str | None = Field(default=None, min_length=1)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    contract: FeatureContract
    execution_bindings: tuple[FeatureExecutionBinding, ...] = Field(min_length=1)
    record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_binding(self) -> FeatureContractRegistration:
        if self.logical_id != self.contract.contract_id:
            raise ValueError("logical id does not match feature contract")
        if self.version != self.contract.version:
            raise ValueError("version does not match feature contract")
        if self.producer_commit != self.contract.producer_commit:
            raise ValueError("producer_commit does not match feature contract")
        names = tuple(binding.feature_name for binding in self.execution_bindings)
        if len(names) != len(set(names)) or set(names) != {
            feature.name for feature in self.contract.features
        }:
            raise ValueError("feature execution bindings must exactly cover the contract")
        if self.version == 1:
            if any(
                value is not None
                for value in (
                    self.parent_fingerprint,
                    self.supersedes,
                    self.replacement_reason,
                )
            ):
                raise ValueError("initial feature definition cannot declare lineage")
        elif (
            self.parent_fingerprint is None
            or self.supersedes != self.version - 1
            or self.replacement_reason is None
        ):
            raise ValueError("feature definition lineage must bind its immediate parent")
        if self.fingerprint != _feature_definition_fingerprint(
            self.contract,
            self.execution_bindings,
            parent_fingerprint=self.parent_fingerprint,
            supersedes=self.supersedes,
            replacement_reason=self.replacement_reason,
        ):
            raise ValueError("fingerprint does not bind complete feature semantics")
        if self.record_hash != _record_hash(self):
            raise ValueError("record hash does not match feature registration")
        return self


class StrategySpecRegistration(RuntimeContractModel):
    schema_version: Literal[5] = 5
    schema_compatibility: DefinitionSchemaCompatibility = Field(
        default_factory=DefinitionSchemaCompatibility
    )
    kind: Literal["strategy_spec"] = "strategy_spec"
    logical_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    registered_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    supersedes: int | None = Field(default=None, ge=1)
    replacement_reason: str | None = Field(default=None, min_length=1)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    feature_contract_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    feature_contract_version: int = Field(ge=1)
    feature_contract_producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    spec: StrategySpec
    execution_binding: StrategyExecutionBinding
    candidate_schema_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    executable_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_binding(self) -> StrategySpecRegistration:
        if self.logical_id != self.spec.strategy_id:
            raise ValueError("logical id does not match strategy spec")
        if self.version != self.spec.version:
            raise ValueError("version does not match strategy spec")
        if self.producer_commit != self.spec.producer_commit:
            raise ValueError("producer_commit does not match strategy spec")
        if self.candidate_schema_fingerprint != self.execution_binding.candidate_schema_fingerprint:
            raise ValueError("candidate schema fingerprint does not match execution binding")
        if self.version == 1:
            if any(
                value is not None
                for value in (
                    self.parent_fingerprint,
                    self.supersedes,
                    self.replacement_reason,
                )
            ):
                raise ValueError("initial strategy definition cannot declare lineage")
        elif (
            self.parent_fingerprint is None
            or self.supersedes != self.version - 1
            or self.replacement_reason is None
        ):
            raise ValueError("strategy definition lineage must bind its immediate parent")
        if self.fingerprint != _strategy_definition_fingerprint(
            self.spec,
            self.execution_binding,
            feature_contract_fingerprint=self.feature_contract_fingerprint,
            parent_fingerprint=self.parent_fingerprint,
            supersedes=self.supersedes,
            replacement_reason=self.replacement_reason,
        ):
            raise ValueError("fingerprint does not bind complete strategy semantics")
        if self.executable_fingerprint != _strategy_executable_fingerprint(
            self.spec,
            self.execution_binding,
        ):
            raise ValueError("executable fingerprint does not bind complete execution semantics")
        if self.record_hash != _record_hash(self):
            raise ValueError("record hash does not match strategy registration")
        return self


class DefinitionFingerprintLookup(RuntimeContractModel):
    schema_version: Literal[1] = 1
    kind: Literal["features", "strategies"]
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    logical_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    version: int = Field(ge=1)
    lookup_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_hash(self) -> DefinitionFingerprintLookup:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"lookup_hash"}))
        if self.lookup_hash != expected:
            raise ValueError("lookup hash does not match fingerprint binding")
        return self


RegistrationT = TypeVar(
    "RegistrationT",
    FeatureContractRegistration,
    StrategySpecRegistration,
)


def _record_hash(record: RuntimeContractModel) -> str:
    return canonical_sha256(record.model_dump(mode="python", exclude={"record_hash"}))


def _feature_definition_fingerprint(
    contract: FeatureContract,
    bindings: tuple[FeatureExecutionBinding, ...],
    *,
    parent_fingerprint: str | None,
    supersedes: int | None,
    replacement_reason: str | None,
) -> str:
    return canonical_sha256(
        {
            "kind": "feature_contract",
            "contract": contract,
            "execution_bindings": tuple(sorted(bindings, key=lambda binding: binding.feature_name)),
            "parent_fingerprint": parent_fingerprint,
            "supersedes": supersedes,
            "replacement_reason": replacement_reason,
        }
    )


def _strategy_definition_fingerprint(
    spec: StrategySpec,
    binding: StrategyExecutionBinding,
    *,
    feature_contract_fingerprint: str,
    parent_fingerprint: str | None,
    supersedes: int | None,
    replacement_reason: str | None,
) -> str:
    return canonical_sha256(
        {
            "kind": "strategy_spec",
            "spec": spec,
            "execution_binding": binding,
            "feature_contract_fingerprint": feature_contract_fingerprint,
            "parent_fingerprint": parent_fingerprint,
            "supersedes": supersedes,
            "replacement_reason": replacement_reason,
        }
    )


def _strategy_executable_fingerprint(
    spec: StrategySpec,
    binding: StrategyExecutionBinding,
) -> str:
    return canonical_sha256(
        {
            "contract": "strategy-executable-registration/v1",
            "spec_fingerprint": spec.spec_fingerprint,
            "execution_binding": binding,
        }
    )


def _logical_key(logical_id: str) -> str:
    if not logical_id or logical_id != logical_id.strip() or "\x00" in logical_id:
        raise DefinitionIntegrityError("logical id is invalid")
    return hashlib.sha256(logical_id.encode("utf-8")).hexdigest()


def _version_name(version: int) -> str:
    if version < 1:
        raise DefinitionIntegrityError("definition version is invalid")
    return f"v{version}"


def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _canonical_feature_contract(contract: FeatureContract) -> FeatureContract:
    features = tuple(
        sorted(
            (
                feature.model_copy(
                    update={"source_datasets": tuple(sorted(feature.source_datasets))}
                )
                for feature in contract.features
            ),
            key=lambda feature: feature.name,
        )
    )
    canonical = FeatureContract.model_validate(
        {**contract.model_dump(mode="python"), "features": features}
    )
    if canonical.contract_fingerprint != contract.contract_fingerprint:
        raise DefinitionIntegrityError("feature canonicalization changed its fingerprint")
    return canonical


def _canonical_strategy_spec(spec: StrategySpec) -> StrategySpec:
    try:
        canonical = StrategySpec.model_validate(
            {
                **spec.model_dump(mode="python"),
                "required_features": tuple(
                    sorted(spec.required_features, key=lambda requirement: requirement.name)
                ),
                "optional_features": tuple(
                    sorted(spec.optional_features, key=lambda requirement: requirement.name)
                ),
                "transitions": tuple(
                    sorted(
                        spec.transitions,
                        key=lambda transition: (
                            transition.from_state.value,
                            transition.event,
                            transition.to_state.value,
                        ),
                    )
                ),
                "allowed_actions": tuple(sorted(spec.allowed_actions)),
            }
        )
    except ValidationError as exc:
        raise DefinitionReferenceError(
            "strategy must declare a reachable terminal exit transition"
        ) from exc
    if canonical.spec_fingerprint != spec.spec_fingerprint:
        raise DefinitionIntegrityError("strategy canonicalization changed its fingerprint")
    return canonical


class ImmutableDefinitionRegistry:
    """Append-only, content-addressed feature and strategy definition store."""

    def __init__(
        self,
        root: Path,
        *,
        execution_registry: TrustedExecutableRegistry | None = None,
        feature_binding_resolver: Callable[[FeatureContract], tuple[FeatureExecutionBinding, ...]]
        | None = None,
        strategy_binding_resolver: Callable[[StrategySpec], StrategyExecutionBinding] | None = None,
    ) -> None:
        if feature_binding_resolver is not None or strategy_binding_resolver is not None:
            raise TypeError("arbitrary execution binding resolvers are not trusted")
        if type(execution_registry) is not TrustedExecutableRegistry:
            raise TypeError("a concrete TrustedExecutableRegistry is required")
        self.root = Path(os.path.abspath(os.fspath(root)))
        self._execution_registry = execution_registry

    def register_feature_contract(
        self,
        contract: FeatureContract,
        *,
        registered_at: datetime,
        available_at: datetime,
        producer_commit: str,
        expected_fingerprint: str,
        parent_fingerprint: str | None = None,
        supersedes: int | None = None,
        replacement_reason: str | None = None,
    ) -> FeatureContractRegistration:
        canonical_contract = _canonical_feature_contract(contract)
        bindings = tuple(
            sorted(
                self._execution_registry.feature_bindings(canonical_contract),
                key=lambda binding: binding.feature_name,
            )
        )
        binding_names = tuple(binding.feature_name for binding in bindings)
        contract_names = tuple(feature.name for feature in canonical_contract.features)
        if len(binding_names) != len(set(binding_names)) or set(binding_names) != set(
            contract_names
        ):
            raise DefinitionReferenceError(
                "feature execution bindings must exactly cover the contract"
            )
        fingerprint = _feature_definition_fingerprint(
            canonical_contract,
            bindings,
            parent_fingerprint=parent_fingerprint,
            supersedes=supersedes,
            replacement_reason=replacement_reason,
        )
        self._validate_definition_identity(
            actual_commit=canonical_contract.producer_commit,
            producer_commit=producer_commit,
            actual_fingerprint=canonical_contract.contract_fingerprint,
            expected_fingerprint=expected_fingerprint,
        )
        values = {
            "logical_id": canonical_contract.contract_id,
            "version": canonical_contract.version,
            "registered_at": registered_at,
            "available_at": available_at,
            "fingerprint": fingerprint,
            "parent_fingerprint": parent_fingerprint,
            "supersedes": supersedes,
            "replacement_reason": replacement_reason,
            "producer_commit": producer_commit,
            "contract": canonical_contract,
            "execution_bindings": bindings,
        }
        record = FeatureContractRegistration(
            **values,
            record_hash=canonical_sha256(
                FeatureContractRegistration.model_construct(
                    **values,
                    schema_version=5,
                    kind="feature_contract",
                    record_hash="0" * 64,
                ).model_dump(mode="python", exclude={"record_hash"})
            ),
        )
        return self._publish(
            kind="features",
            logical_id=record.logical_id,
            version=record.version,
            fingerprint=record.fingerprint,
            record=record,
            model=FeatureContractRegistration,
        )

    def register_strategy_spec(
        self,
        spec: StrategySpec,
        *,
        feature_contract_fingerprint: str,
        registered_at: datetime,
        available_at: datetime,
        producer_commit: str,
        expected_fingerprint: str,
        parent_fingerprint: str | None = None,
        supersedes: int | None = None,
        replacement_reason: str | None = None,
    ) -> StrategySpecRegistration:
        canonical_spec = _canonical_strategy_spec(spec)
        self._validate_definition_identity(
            actual_commit=canonical_spec.producer_commit,
            producer_commit=producer_commit,
            actual_fingerprint=canonical_spec.spec_fingerprint,
            expected_fingerprint=expected_fingerprint,
        )
        feature_record = self.read_feature_contract(
            feature_contract_fingerprint,
            as_of=available_at,
        )
        if feature_record is None:
            raise DefinitionReferenceError("feature contract fingerprint is not registered")
        self._validate_strategy_reference(canonical_spec, feature_record)
        execution_binding = self._execution_registry.strategy_binding(canonical_spec)
        self._validate_strategy_execution_binding(canonical_spec, execution_binding)
        fingerprint = _strategy_definition_fingerprint(
            canonical_spec,
            execution_binding,
            feature_contract_fingerprint=feature_record.fingerprint,
            parent_fingerprint=parent_fingerprint,
            supersedes=supersedes,
            replacement_reason=replacement_reason,
        )
        values = {
            "logical_id": canonical_spec.strategy_id,
            "version": canonical_spec.version,
            "registered_at": registered_at,
            "available_at": available_at,
            "fingerprint": fingerprint,
            "parent_fingerprint": parent_fingerprint,
            "supersedes": supersedes,
            "replacement_reason": replacement_reason,
            "producer_commit": producer_commit,
            "feature_contract_fingerprint": feature_record.fingerprint,
            "feature_contract_version": feature_record.version,
            "feature_contract_producer_commit": feature_record.producer_commit,
            "spec": canonical_spec,
            "execution_binding": execution_binding,
            "candidate_schema_fingerprint": execution_binding.candidate_schema_fingerprint,
            "executable_fingerprint": _strategy_executable_fingerprint(
                canonical_spec,
                execution_binding,
            ),
        }
        record = StrategySpecRegistration(
            **values,
            record_hash=canonical_sha256(
                StrategySpecRegistration.model_construct(
                    **values,
                    schema_version=5,
                    kind="strategy_spec",
                    record_hash="0" * 64,
                ).model_dump(mode="python", exclude={"record_hash"})
            ),
        )
        return self._publish(
            kind="strategies",
            logical_id=record.logical_id,
            version=record.version,
            fingerprint=record.fingerprint,
            record=record,
            model=StrategySpecRegistration,
        )

    def read_feature_contract(
        self,
        fingerprint: str,
        *,
        as_of: datetime | None = None,
    ) -> FeatureContractRegistration | None:
        record = self._read_by_fingerprint(
            kind="features",
            fingerprint=fingerprint,
            model=FeatureContractRegistration,
        )
        if record is None or (
            as_of is not None and (record.registered_at > as_of or record.available_at > as_of)
        ):
            return None
        self._validate_stored_feature_execution(record)
        return record

    def read_strategy_spec(
        self,
        fingerprint: str,
        *,
        as_of: datetime | None = None,
    ) -> StrategySpecRegistration | None:
        record = self._read_by_fingerprint(
            kind="strategies",
            fingerprint=fingerprint,
            model=StrategySpecRegistration,
        )
        if record is None or (
            as_of is not None and (record.registered_at > as_of or record.available_at > as_of)
        ):
            return None
        feature = self.read_feature_contract(
            record.feature_contract_fingerprint,
            as_of=record.available_at,
        )
        if feature is None:
            raise DefinitionIntegrityError("strategy feature contract reference is unavailable")
        self._validate_stored_strategy_reference(record, feature)
        self._validate_stored_strategy_execution(record)
        return record

    def latest_feature_contract(
        self,
        logical_id: str,
        *,
        as_of: datetime,
    ) -> FeatureContractRegistration | None:
        records = self._read_logical_records(
            kind="features",
            logical_id=logical_id,
            model=FeatureContractRegistration,
        )
        visible = [
            record
            for record in records
            if record.registered_at <= as_of and record.available_at <= as_of
        ]
        selected = max(
            visible,
            key=lambda record: (record.version, record.available_at, record.fingerprint),
            default=None,
        )
        if selected is None:
            return None
        self._validate_stored_feature_execution(selected)
        return selected

    def latest_strategy_spec(
        self,
        logical_id: str,
        *,
        as_of: datetime,
    ) -> StrategySpecRegistration | None:
        records = self._read_logical_records(
            kind="strategies",
            logical_id=logical_id,
            model=StrategySpecRegistration,
        )
        visible = [
            record
            for record in records
            if record.registered_at <= as_of and record.available_at <= as_of
        ]
        if not visible:
            return None
        selected = max(
            visible,
            key=lambda record: (record.version, record.available_at, record.fingerprint),
        )
        return self.read_strategy_spec(selected.fingerprint, as_of=as_of)

    @staticmethod
    def _validate_definition_identity(
        *,
        actual_commit: str,
        producer_commit: str,
        actual_fingerprint: str,
        expected_fingerprint: str,
    ) -> None:
        if actual_commit != producer_commit:
            raise DefinitionIntegrityError("producer_commit does not match definition")
        if not _FINGERPRINT_PATTERN.fullmatch(expected_fingerprint):
            raise DefinitionIntegrityError("expected fingerprint is invalid")
        if actual_fingerprint != expected_fingerprint:
            raise DefinitionIntegrityError("fingerprint does not match definition")

    @staticmethod
    def _validate_strategy_reference(
        spec: StrategySpec,
        feature_record: FeatureContractRegistration,
    ) -> None:
        contract = feature_record.contract
        if spec.feature_contract_id != contract.contract_id:
            raise DefinitionReferenceError("strategy feature contract id does not match")
        if contract.version < spec.min_feature_contract_version:
            raise DefinitionReferenceError("minimum feature contract version is not registered")
        names = {feature.name for feature in contract.features}
        requirements = (*spec.required_features, *spec.optional_features)
        missing = sorted(
            requirement.name for requirement in requirements if requirement.name not in names
        )
        if missing:
            raise DefinitionReferenceError(f"missing feature: {', '.join(missing)}")
        incompatible = sorted(
            requirement.name
            for requirement in requirements
            if requirement.min_contract_version > contract.version
        )
        if incompatible:
            raise DefinitionReferenceError(
                f"minimum feature contract version is not met for: {', '.join(incompatible)}"
            )

    @staticmethod
    def _validate_strategy_execution_binding(
        spec: StrategySpec,
        binding: StrategyExecutionBinding,
    ) -> None:
        transitions_to_terminal = tuple(
            transition for transition in spec.transitions if transition.to_state.value == "terminal"
        )
        if not transitions_to_terminal:
            raise DefinitionReferenceError(
                "strategy must declare a reachable terminal exit transition"
            )
        if any(transition.from_state.value == "terminal" for transition in spec.transitions):
            raise DefinitionReferenceError(
                "terminal strategy state cannot have outgoing transitions"
            )
        entry_transitions = tuple(
            transition for transition in spec.transitions if transition.event == binding.entry_event
        )
        if len(entry_transitions) != 1 or entry_transitions[0].to_state.value != "holding":
            raise DefinitionReferenceError(
                "strategy entry evaluator must bind one transition into holding"
            )
        terminal_events = {
            transition.event
            for transition in transitions_to_terminal
            if transition.from_state.value == "holding"
        }
        terminal_rule_events = {
            rule.fill_event for rule in binding.exit_rules if rule.sell_tranche.terminal_after_fill
        }
        if terminal_events != terminal_rule_events:
            raise DefinitionReferenceError(
                "strategy exit rules must exactly bind terminal transition events"
            )
        actions = set(spec.allowed_actions)
        if any(rule.action not in actions for rule in binding.exit_rules):
            raise DefinitionReferenceError("strategy exit action is not allowed by the spec")
        if any(rule.action not in {"reduce", "s_intent"} for rule in binding.exit_rules):
            raise DefinitionReferenceError("strategy exit rules must declare sell semantics")
        sequences = sorted(rule.sell_tranche.sequence for rule in binding.exit_rules)
        if sequences != list(range(1, len(sequences) + 1)):
            raise DefinitionReferenceError("strategy sell tranches must be contiguous")
        for rule in binding.exit_rules:
            intent_transitions = tuple(
                transition for transition in spec.transitions if transition.event == rule.event
            )
            if len(intent_transitions) != 1:
                raise DefinitionReferenceError(
                    "strategy exit event must bind exactly one state transition"
                )
            intent_transition = intent_transitions[0]
            if (
                intent_transition.from_state.value != "holding"
                or intent_transition.to_state.value != "holding"
            ):
                raise DefinitionReferenceError(
                    "strategy exit intent must remain holding until verified fill"
                )
            if not rule.sell_tranche.terminal_after_fill:
                if not rule.sell_tranche.reevaluate_after_fill:
                    raise DefinitionReferenceError(
                        "partial strategy exit must remain holding and re-evaluate"
                    )
                continue
            if rule.fill_event is None:
                raise DefinitionReferenceError(
                    "terminal strategy exit is missing its verified fill event"
                )
            fill_transitions = tuple(
                transition for transition in spec.transitions if transition.event == rule.fill_event
            )
            if len(fill_transitions) != 1:
                raise DefinitionReferenceError(
                    "terminal fill event must bind exactly one state transition"
                )
            fill_transition = fill_transitions[0]
            if (
                fill_transition.from_state.value != "holding"
                or fill_transition.to_state.value != "terminal"
            ):
                raise DefinitionReferenceError(
                    "terminal state requires a verified full SELL fill transition"
                )

    @classmethod
    def _validate_stored_strategy_reference(
        cls,
        record: StrategySpecRegistration,
        feature: FeatureContractRegistration,
    ) -> None:
        if (
            record.feature_contract_version != feature.version
            or record.feature_contract_producer_commit != feature.producer_commit
            or record.feature_contract_fingerprint != feature.fingerprint
        ):
            raise DefinitionIntegrityError("strategy feature contract binding is inconsistent")
        try:
            cls._validate_strategy_reference(record.spec, feature)
        except DefinitionReferenceError as exc:
            raise DefinitionIntegrityError("strategy feature contract binding is invalid") from exc

    def _validate_stored_feature_execution(
        self,
        record: FeatureContractRegistration,
    ) -> None:
        expected = self._execution_registry.feature_bindings(record.contract)
        if record.execution_bindings != expected:
            raise DefinitionExecutableIntegrityError(
                "feature definition does not match the trusted executable allowlist"
            )

    def _validate_stored_strategy_execution(
        self,
        record: StrategySpecRegistration,
    ) -> None:
        expected = self._execution_registry.strategy_binding(record.spec)
        if record.execution_binding != expected:
            raise DefinitionExecutableIntegrityError(
                "strategy definition does not match the trusted executable allowlist"
            )
        try:
            self._validate_strategy_execution_binding(record.spec, expected)
        except DefinitionReferenceError as exc:
            raise DefinitionExecutableIntegrityError(
                "stored strategy execution semantics are invalid"
            ) from exc

    @staticmethod
    def _same_definition_content(
        left: FeatureContractRegistration | StrategySpecRegistration,
        right: FeatureContractRegistration | StrategySpecRegistration,
    ) -> bool:
        if isinstance(left, FeatureContractRegistration) and isinstance(
            right, FeatureContractRegistration
        ):
            return (
                left.logical_id == right.logical_id
                and left.version == right.version
                and left.fingerprint == right.fingerprint
                and left.parent_fingerprint == right.parent_fingerprint
                and left.supersedes == right.supersedes
                and left.replacement_reason == right.replacement_reason
                and left.producer_commit == right.producer_commit
                and left.contract == right.contract
                and left.execution_bindings == right.execution_bindings
            )
        if isinstance(left, StrategySpecRegistration) and isinstance(
            right, StrategySpecRegistration
        ):
            return (
                left.logical_id == right.logical_id
                and left.version == right.version
                and left.fingerprint == right.fingerprint
                and left.parent_fingerprint == right.parent_fingerprint
                and left.supersedes == right.supersedes
                and left.replacement_reason == right.replacement_reason
                and left.producer_commit == right.producer_commit
                and left.feature_contract_fingerprint == right.feature_contract_fingerprint
                and left.feature_contract_version == right.feature_contract_version
                and left.feature_contract_producer_commit == right.feature_contract_producer_commit
                and left.spec == right.spec
                and left.execution_binding == right.execution_binding
                and left.executable_fingerprint == right.executable_fingerprint
            )
        return False

    def _publish(
        self,
        *,
        kind: Literal["features", "strategies"],
        logical_id: str,
        version: int,
        fingerprint: str,
        record: RegistrationT,
        model: type[RegistrationT],
    ) -> RegistrationT:
        payload = canonical_model_json_bytes(record)
        if len(payload) > _MAX_RECORD_BYTES:
            raise DefinitionIntegrityError("definition record exceeds size limit")
        root_chain: RootChain | None = None
        root_fd = kind_fd = logical_fd = staging_fd = version_fd = lock_fd = -1
        leases: list[_StagingRecordLease] = []
        publication = _VersionPublicationOwnership()
        version_name = _version_name(version)
        logical_name = _logical_key(logical_id)
        staging_name = f".publish-{uuid4().hex}"
        try:
            root_chain = self._open_anchored_root(create=True)
            if root_chain is None:
                raise DefinitionIntegrityError("definition registry root was not created")
            root_fd = root_chain[0][-1]
            self._validate_root_entries(root_fd)
            kind_fd = self._ensure_child_directory(root_fd, kind)
            logical_fd = self._ensure_child_directory(kind_fd, logical_name)
            lock_fd = self._acquire_publish_lock(logical_fd)
            self._recover_stale_staging(logical_fd)
            self._recover_uncommitted_versions(logical_fd)
            if self._entry_exists(logical_fd, version_name):
                existing = self._read_existing_version_or_quarantine(
                    logical_fd,
                    version_name,
                    logical_id=logical_id,
                    model=model,
                )
                if not self._same_definition_content(existing, record):
                    raise DefinitionConflictError(
                        "logical id and version already contain different content"
                    )
                self._publish_fingerprint_lookup(root_fd, kind=kind, record=existing)
                os.fsync(logical_fd)
                self._revalidate_publication_path(
                    root_chain=root_chain,
                    root_fd=root_fd,
                    kind=kind,
                    kind_fd=kind_fd,
                    logical_name=logical_name,
                    logical_fd=logical_fd,
                )
                return existing
            self._validate_definition_lineage_at(
                logical_fd,
                logical_name=logical_name,
                record=record,
                model=model,
            )
            try:
                os.mkdir(staging_name, _PRIVATE_DIRECTORY_MODE, dir_fd=logical_fd)
            except OSError as exc:
                raise DefinitionIntegrityError(
                    "hidden definition staging directory cannot be created safely"
                ) from exc
            staging_fd = self._open_private_directory_at(logical_fd, staging_name)
            self._write_private_record(staging_fd, f"{fingerprint}.json", payload)
            record_lease = self._capture_staging_record_lease(
                staging_fd,
                f"{fingerprint}.json",
                payload,
            )
            leases.append(record_lease)
            os.fsync(staging_fd)
            seal_payload = self._seal_payload(fingerprint)
            self._write_private_record(
                staging_fd,
                _SEAL_NAME,
                seal_payload,
            )
            seal_lease = self._capture_staging_record_lease(
                staging_fd,
                _SEAL_NAME,
                seal_payload,
            )
            leases.append(seal_lease)
            os.fsync(staging_fd)
            expected_staging = {
                f"{fingerprint}.json": (record_lease, payload),
                _SEAL_NAME: (seal_lease, seal_payload),
            }
            self._validate_staging_for_publish(
                logical_fd=logical_fd,
                staging_name=staging_name,
                staging_fd=staging_fd,
                expected=expected_staging,
            )
            try:
                version_fd = self._publish_version_from_leases(
                    logical_fd=logical_fd,
                    version_name=version_name,
                    staging_name=staging_name,
                    staging_fd=staging_fd,
                    expected=expected_staging,
                    ownership=publication,
                )
            except FileExistsError as exc:
                existing = self._read_existing_version_or_quarantine(
                    logical_fd,
                    version_name,
                    logical_id=logical_id,
                    model=model,
                )
                os.fsync(logical_fd)
                self._revalidate_publication_path(
                    root_chain=root_chain,
                    root_fd=root_fd,
                    kind=kind,
                    kind_fd=kind_fd,
                    logical_name=logical_name,
                    logical_fd=logical_fd,
                )
                if self._same_definition_content(existing, record):
                    self._publish_fingerprint_lookup(
                        root_fd,
                        kind=kind,
                        record=existing,
                    )
                    return existing
                raise DefinitionConflictError(
                    "logical id and version already contain different content"
                ) from exc
            self._revalidate_version_directory(
                logical_fd,
                version_name,
                version_fd,
                expected_mode=_PRIVATE_DIRECTORY_MODE,
            )
            self._revalidate_publication_path(
                root_chain=root_chain,
                root_fd=root_fd,
                kind=kind,
                kind_fd=kind_fd,
                logical_name=logical_name,
                logical_fd=logical_fd,
            )
            self._publish_fingerprint_lookup(root_fd, kind=kind, record=record)
            self._commit_published_version(
                logical_fd=logical_fd,
                version_name=version_name,
                version_fd=version_fd,
                fingerprint=fingerprint,
                expected=expected_staging,
            )
            self._revalidate_publication_path(
                root_chain=root_chain,
                root_fd=root_fd,
                kind=kind,
                kind_fd=kind_fd,
                logical_name=logical_name,
                logical_fd=logical_fd,
            )
            return record
        except BaseException:
            if publication.owned:
                try:
                    self._rollback_owned_publication(
                        root_fd=root_fd,
                        kind=kind,
                        record=record,
                        logical_fd=logical_fd,
                        version_name=version_name,
                        version_fd=(version_fd if version_fd >= 0 else publication.descriptor),
                    )
                except BaseException as rollback_error:
                    raise DefinitionIntegrityError(
                        "failed definition publication could not be rolled back safely"
                    ) from rollback_error
            raise
        finally:
            for lease in leases:
                os.close(lease.descriptor)
            if version_fd >= 0:
                os.close(version_fd)
            if staging_fd >= 0:
                os.close(staging_fd)
            try:
                if logical_fd >= 0:
                    self._remove_hidden_staging(logical_fd, staging_name)
            finally:
                if logical_fd >= 0:
                    os.close(logical_fd)
                if lock_fd >= 0:
                    self._release_publish_lock(lock_fd)
                if kind_fd >= 0:
                    os.close(kind_fd)
                if root_chain is not None:
                    self._close_root_chain(root_chain)

    @classmethod
    def _rollback_owned_publication(
        cls,
        *,
        root_fd: int,
        kind: Literal["features", "strategies"],
        record: RegistrationT,
        logical_fd: int,
        version_name: str,
        version_fd: int,
    ) -> None:
        if min(root_fd, logical_fd, version_fd) < 0:
            raise DefinitionIntegrityError("failed publication lost its rollback leases")
        observed_mode = stat.S_IMODE(os.fstat(version_fd).st_mode)
        if observed_mode not in {
            _PRIVATE_DIRECTORY_MODE,
            _VERSION_PUBLISHED_MODE,
            _VERSION_FROZEN_MODE,
        }:
            raise DefinitionIntegrityError("failed publication version mode is unsafe")
        cls._revalidate_version_directory(
            logical_fd,
            version_name,
            version_fd,
            expected_mode=observed_mode,
        )
        os.fchmod(version_fd, _VERSION_FROZEN_MODE)
        os.fsync(version_fd)
        cls._revalidate_version_directory(
            logical_fd,
            version_name,
            version_fd,
            expected_mode=_VERSION_FROZEN_MODE,
        )
        cls._remove_fingerprint_lookup_if_matches(
            root_fd,
            kind=kind,
            record=record,
        )
        cls._remove_unpublished_version(
            logical_fd,
            version_name,
            version_fd,
        )

    @classmethod
    def _remove_fingerprint_lookup_if_matches(
        cls,
        root_fd: int,
        *,
        kind: Literal["features", "strategies"],
        record: RegistrationT,
    ) -> None:
        lookup = cls._build_fingerprint_lookup(kind=kind, record=record)
        lookups_fd = lookup_kind_fd = bucket_fd = bucket_lock_fd = -1
        target_name = f"{record.fingerprint}.json"
        quarantine_name = f".publish-{uuid4().hex}"
        try:
            if not cls._entry_exists(root_fd, "lookups"):
                return
            lookups_fd = cls._open_private_directory_at(root_fd, "lookups")
            cls._validate_lookup_root_entries(lookups_fd)
            if not cls._entry_exists(lookups_fd, kind):
                return
            lookup_kind_fd = cls._open_private_directory_at(lookups_fd, kind)
            prefix = record.fingerprint[:2]
            if not cls._entry_exists(lookup_kind_fd, prefix):
                return
            bucket_fd = cls._open_private_directory_at(lookup_kind_fd, prefix)
            bucket_lock_fd = cls._acquire_publish_lock(bucket_fd)
            if not cls._entry_exists(bucket_fd, target_name):
                return
            if cls._read_lookup_at(bucket_fd, target_name) != lookup:
                raise DefinitionIntegrityError(
                    "failed publication lookup no longer matches its rollback lease"
                )
            cls._atomic_rename_no_replace(
                bucket_fd,
                target_name,
                quarantine_name,
            )
            os.fsync(bucket_fd)
            cls._remove_hidden_lookup_file(bucket_fd, quarantine_name)
            os.fsync(bucket_fd)
            cls._revalidate_lookup_publication_path(
                root_fd=root_fd,
                lookups_fd=lookups_fd,
                kind=kind,
                lookup_kind_fd=lookup_kind_fd,
                prefix=prefix,
                bucket_fd=bucket_fd,
            )
        finally:
            if bucket_fd >= 0:
                try:
                    if cls._entry_exists(bucket_fd, quarantine_name):
                        cls._remove_hidden_lookup_file(bucket_fd, quarantine_name)
                finally:
                    if bucket_lock_fd >= 0:
                        cls._release_publish_lock(bucket_lock_fd)
                    os.close(bucket_fd)
            if lookup_kind_fd >= 0:
                os.close(lookup_kind_fd)
            if lookups_fd >= 0:
                os.close(lookups_fd)

    def _validate_definition_lineage_at(
        self,
        logical_fd: int,
        *,
        logical_name: str,
        record: RegistrationT,
        model: type[RegistrationT],
    ) -> None:
        existing = sorted(
            self._read_logical_directory_records(
                logical_fd,
                logical_name=logical_name,
                model=model,
            ),
            key=lambda item: item.version,
        )
        if record.version == 1:
            if existing:
                raise DefinitionConflictError(
                    "initial definition cannot be published after an existing lineage"
                )
            return
        expected_versions = list(range(1, record.version))
        if [item.version for item in existing] != expected_versions:
            raise DefinitionConflictError("definition versions must be contiguous without gaps")
        parent = existing[-1]
        if (
            record.supersedes != parent.version
            or record.parent_fingerprint != parent.fingerprint
            or not record.replacement_reason
        ):
            raise DefinitionConflictError(
                "definition lineage does not bind the current immediate parent"
            )

    @staticmethod
    def _validate_loaded_lineage(records: list[RegistrationT]) -> None:
        if not records:
            return
        ordered = sorted(records, key=lambda item: item.version)
        expected_versions = list(range(1, ordered[-1].version + 1))
        if [item.version for item in ordered] != expected_versions:
            raise DefinitionIntegrityError(
                "stored definition lineage must be contiguous from version 1"
            )
        for parent, child in zip(ordered, ordered[1:], strict=False):
            if (
                child.supersedes != parent.version
                or child.parent_fingerprint != parent.fingerprint
                or not child.replacement_reason
            ):
                raise DefinitionIntegrityError(
                    "stored definition lineage does not bind its immediate parent"
                )

    def _read_by_fingerprint(
        self,
        *,
        kind: Literal["features", "strategies"],
        fingerprint: str,
        model: type[RegistrationT],
    ) -> RegistrationT | None:
        if not _FINGERPRINT_PATTERN.fullmatch(fingerprint):
            raise DefinitionIntegrityError("fingerprint path is invalid")
        lookup = self._read_fingerprint_lookup(kind=kind, fingerprint=fingerprint)
        if lookup is None:
            return None
        return self._read_lookup_bound_record(lookup=lookup, model=model)

    @staticmethod
    def _build_fingerprint_lookup(
        *,
        kind: Literal["features", "strategies"],
        record: FeatureContractRegistration | StrategySpecRegistration,
    ) -> DefinitionFingerprintLookup:
        values = {
            "kind": kind,
            "fingerprint": record.fingerprint,
            "logical_key": _logical_key(record.logical_id),
            "version": record.version,
        }
        lookup_hash = canonical_sha256(
            DefinitionFingerprintLookup.model_construct(
                **values,
                schema_version=1,
                lookup_hash="0" * 64,
            ).model_dump(mode="python", exclude={"lookup_hash"})
        )
        return DefinitionFingerprintLookup(**values, lookup_hash=lookup_hash)

    def _publish_fingerprint_lookup(
        self,
        root_fd: int,
        *,
        kind: Literal["features", "strategies"],
        record: RegistrationT,
    ) -> None:
        lookup = self._build_fingerprint_lookup(kind=kind, record=record)
        payload = canonical_model_json_bytes(lookup)
        lookups_fd = lookup_kind_fd = bucket_fd = bucket_lock_fd = -1
        temporary_name = f".publish-{uuid4().hex}"
        target_name = f"{record.fingerprint}.json"
        try:
            lookups_fd = self._ensure_child_directory(root_fd, "lookups")
            self._validate_lookup_root_entries(lookups_fd)
            lookup_kind_fd = self._ensure_child_directory(lookups_fd, kind)
            bucket_fd = self._ensure_child_directory(
                lookup_kind_fd,
                record.fingerprint[:2],
            )
            bucket_lock_fd = self._acquire_publish_lock(bucket_fd)
            self._recover_stale_lookup_files(bucket_fd)
            public_entry_count = self._validate_lookup_bucket_entries(bucket_fd)
            if self._entry_exists(bucket_fd, target_name):
                existing = self._read_lookup_at(bucket_fd, target_name)
                if existing != lookup:
                    raise DefinitionConflictError(
                        "fingerprint lookup already contains different content"
                    )
                os.fsync(bucket_fd)
                self._revalidate_lookup_publication_path(
                    root_fd=root_fd,
                    lookups_fd=lookups_fd,
                    kind=kind,
                    lookup_kind_fd=lookup_kind_fd,
                    prefix=record.fingerprint[:2],
                    bucket_fd=bucket_fd,
                )
                return
            if public_entry_count >= _MAX_LOOKUP_BUCKET_ENTRIES:
                raise DefinitionIntegrityError("fingerprint lookup bucket is unbounded")
            self._write_private_record(bucket_fd, temporary_name, payload)
            try:
                self._atomic_rename_no_replace(
                    bucket_fd,
                    temporary_name,
                    target_name,
                )
            except FileExistsError as exc:
                existing = self._read_lookup_at(bucket_fd, target_name)
                if existing != lookup:
                    raise DefinitionConflictError(
                        "fingerprint lookup already contains different content"
                    ) from exc
            os.fsync(bucket_fd)
            self._revalidate_lookup_publication_path(
                root_fd=root_fd,
                lookups_fd=lookups_fd,
                kind=kind,
                lookup_kind_fd=lookup_kind_fd,
                prefix=record.fingerprint[:2],
                bucket_fd=bucket_fd,
            )
        finally:
            if bucket_fd >= 0:
                try:
                    if self._entry_exists(bucket_fd, temporary_name):
                        self._remove_hidden_lookup_file(bucket_fd, temporary_name)
                finally:
                    if bucket_lock_fd >= 0:
                        self._release_publish_lock(bucket_lock_fd)
                    os.close(bucket_fd)
            if lookup_kind_fd >= 0:
                os.close(lookup_kind_fd)
            if lookups_fd >= 0:
                os.close(lookups_fd)

    @classmethod
    def _revalidate_lookup_publication_path(
        cls,
        *,
        root_fd: int,
        lookups_fd: int,
        kind: Literal["features", "strategies"],
        lookup_kind_fd: int,
        prefix: str,
        bucket_fd: int,
    ) -> None:
        cls._revalidate_child_directory(lookup_kind_fd, prefix, bucket_fd)
        cls._revalidate_child_directory(lookups_fd, kind, lookup_kind_fd)
        cls._revalidate_child_directory(root_fd, "lookups", lookups_fd)
        cls._validate_lookup_root_entries(lookups_fd)

    def _read_fingerprint_lookup(
        self,
        *,
        kind: Literal["features", "strategies"],
        fingerprint: str,
    ) -> DefinitionFingerprintLookup | None:
        root_chain = self._open_anchored_root(create=False)
        if root_chain is None:
            return None
        root_fd = root_chain[0][-1]
        lookups_fd = lookup_kind_fd = bucket_fd = -1
        try:
            self._validate_root_entries(root_fd)
            if not self._entry_exists(root_fd, "lookups"):
                self._revalidate_root_chain(root_chain)
                return None
            lookups_fd = self._open_private_directory_at(root_fd, "lookups")
            self._validate_lookup_root_entries(lookups_fd)
            if not self._entry_exists(lookups_fd, kind):
                self._revalidate_child_directory(root_fd, "lookups", lookups_fd)
                self._revalidate_root_chain(root_chain)
                return None
            lookup_kind_fd = self._open_private_directory_at(lookups_fd, kind)
            self._validate_lookup_kind_entries(lookup_kind_fd)
            prefix = fingerprint[:2]
            if not self._entry_exists(lookup_kind_fd, prefix):
                self._revalidate_child_directory(lookups_fd, kind, lookup_kind_fd)
                self._revalidate_child_directory(root_fd, "lookups", lookups_fd)
                self._revalidate_root_chain(root_chain)
                return None
            bucket_fd = self._open_private_directory_at(lookup_kind_fd, prefix)
            self._validate_lookup_bucket_entries(bucket_fd)
            target_name = f"{fingerprint}.json"
            if not self._entry_exists(bucket_fd, target_name):
                self._revalidate_child_directory(lookup_kind_fd, prefix, bucket_fd)
                self._revalidate_child_directory(lookups_fd, kind, lookup_kind_fd)
                self._revalidate_child_directory(root_fd, "lookups", lookups_fd)
                self._revalidate_root_chain(root_chain)
                return None
            lookup = self._read_lookup_at(bucket_fd, target_name)
            if lookup.kind != kind or lookup.fingerprint != fingerprint:
                raise DefinitionIntegrityError("fingerprint lookup path does not match its binding")
            self._revalidate_child_directory(lookup_kind_fd, prefix, bucket_fd)
            self._revalidate_child_directory(lookups_fd, kind, lookup_kind_fd)
            self._revalidate_child_directory(root_fd, "lookups", lookups_fd)
            self._revalidate_root_chain(root_chain)
            return lookup
        finally:
            if bucket_fd >= 0:
                os.close(bucket_fd)
            if lookup_kind_fd >= 0:
                os.close(lookup_kind_fd)
            if lookups_fd >= 0:
                os.close(lookups_fd)
            self._close_root_chain(root_chain)

    def _read_lookup_bound_record(
        self,
        *,
        lookup: DefinitionFingerprintLookup,
        model: type[RegistrationT],
    ) -> RegistrationT | None:
        root_chain = self._open_anchored_root(create=False)
        if root_chain is None:
            return None
        root_fd = root_chain[0][-1]
        kind_fd = logical_fd = -1
        try:
            self._validate_root_entries(root_fd)
            if not self._entry_exists(root_fd, lookup.kind):
                self._revalidate_root_chain(root_chain)
                return None
            kind_fd = self._open_private_directory_at(root_fd, lookup.kind)
            if not self._entry_exists(kind_fd, lookup.logical_key):
                self._revalidate_child_directory(root_fd, lookup.kind, kind_fd)
                self._revalidate_root_chain(root_chain)
                return None
            logical_fd = self._open_private_directory_at(kind_fd, lookup.logical_key)
            version_name = _version_name(lookup.version)
            if not self._entry_exists(logical_fd, version_name):
                self._revalidate_child_directory(
                    kind_fd,
                    lookup.logical_key,
                    logical_fd,
                )
                self._revalidate_child_directory(root_fd, lookup.kind, kind_fd)
                self._revalidate_root_chain(root_chain)
                return None
            observed = os.stat(
                version_name,
                dir_fd=logical_fd,
                follow_symlinks=False,
            )
            if (
                stat.S_ISDIR(observed.st_mode)
                and observed.st_uid == os.geteuid()
                and stat.S_IMODE(observed.st_mode)
                in {
                    _PRIVATE_DIRECTORY_MODE,
                    _VERSION_BUILD_MODE,
                    _VERSION_FROZEN_MODE,
                }
            ):
                if stat.S_IMODE(
                    observed.st_mode
                ) == _PRIVATE_DIRECTORY_MODE and self._private_version_has_commit_marker(
                    logical_fd, version_name
                ):
                    raise DefinitionIntegrityError(
                        "committed definition version is not published safely"
                    )
                self._revalidate_child_directory(
                    kind_fd,
                    lookup.logical_key,
                    logical_fd,
                )
                self._revalidate_child_directory(root_fd, lookup.kind, kind_fd)
                self._revalidate_root_chain(root_chain)
                return None
            records = self._read_logical_directory_records(
                logical_fd,
                logical_name=lookup.logical_key,
                model=model,
            )
            matching = [record for record in records if record.version == lookup.version]
            if len(matching) != 1:
                raise DefinitionIntegrityError(
                    "fingerprint lookup does not resolve one lineage version"
                )
            record = matching[0]
            if (
                record.fingerprint != lookup.fingerprint
                or _logical_key(record.logical_id) != lookup.logical_key
                or record.version != lookup.version
            ):
                raise DefinitionIntegrityError(
                    "fingerprint lookup does not match its definition record"
                )
            self._revalidate_child_directory(
                kind_fd,
                lookup.logical_key,
                logical_fd,
            )
            self._revalidate_child_directory(root_fd, lookup.kind, kind_fd)
            self._revalidate_root_chain(root_chain)
            return record
        finally:
            if logical_fd >= 0:
                os.close(logical_fd)
            if kind_fd >= 0:
                os.close(kind_fd)
            self._close_root_chain(root_chain)

    @staticmethod
    def _read_lookup_at(bucket_fd: int, name: str) -> DefinitionFingerprintLookup:
        payload = ImmutableDefinitionRegistry._read_private_record(bucket_fd, name)
        try:
            return strict_model_validate_canonical_json(
                DefinitionFingerprintLookup,
                payload,
            )
        except (TypeError, ValueError) as exc:
            raise DefinitionIntegrityError(
                "fingerprint lookup is non-canonical or tampered"
            ) from exc

    def _read_logical_records(
        self,
        *,
        kind: Literal["features", "strategies"],
        logical_id: str,
        model: type[RegistrationT],
    ) -> list[RegistrationT]:
        key = _logical_key(logical_id)
        return [
            record
            for record in self._read_all(kind=kind, model=model)
            if record.logical_id == logical_id and _logical_key(record.logical_id) == key
        ]

    def _read_all(
        self,
        *,
        kind: Literal["features", "strategies"],
        model: type[RegistrationT],
    ) -> list[RegistrationT]:
        root_chain = self._open_anchored_root(create=False)
        if root_chain is None:
            return []
        root_fd = kind_fd = -1
        records: list[RegistrationT] = []
        try:
            root_fd = root_chain[0][-1]
            self._validate_root_entries(root_fd)
            if not self._entry_exists(root_fd, kind):
                self._revalidate_root_chain(root_chain)
                return []
            kind_fd = self._open_private_directory_at(root_fd, kind)
            for logical_name in self._directory_names(
                kind_fd,
                limit=_MAX_LOGICAL_DEFINITIONS,
                overflow_message="logical definition count is unbounded",
            ):
                if not _ID_KEY_PATTERN.fullmatch(logical_name):
                    raise DefinitionIntegrityError("logical id path is invalid")
                logical_fd = self._open_private_directory_at(kind_fd, logical_name)
                try:
                    records.extend(
                        self._read_logical_directory_records(
                            logical_fd,
                            logical_name=logical_name,
                            model=model,
                        )
                    )
                    self._revalidate_child_directory(
                        kind_fd,
                        logical_name,
                        logical_fd,
                    )
                finally:
                    os.close(logical_fd)
            self._revalidate_child_directory(root_fd, kind, kind_fd)
            self._validate_root_entries(root_fd)
            self._revalidate_root_chain(root_chain)
            return records
        finally:
            if kind_fd >= 0:
                os.close(kind_fd)
            self._close_root_chain(root_chain)

    def _read_logical_directory_records(
        self,
        logical_fd: int,
        *,
        logical_name: str,
        model: type[RegistrationT],
    ) -> list[RegistrationT]:
        records: list[RegistrationT] = []
        staging_count = 0
        version_count = 0
        try:
            with os.scandir(logical_fd) as entries:
                for entry in entries:
                    version_name = entry.name
                    if version_name == _PUBLISH_LOCK_NAME:
                        self._validate_publish_lock_entry(logical_fd)
                        continue
                    if _STAGING_PATTERN.fullmatch(version_name):
                        staging_count += 1
                        if staging_count > _MAX_STAGING_RECOVERY:
                            raise DefinitionIntegrityError("hidden staging read limit exceeded")
                        self._validate_hidden_staging(logical_fd, version_name)
                        continue
                    version_count += 1
                    if version_count > _MAX_DEFINITION_VERSIONS:
                        raise DefinitionIntegrityError("definition version count is unbounded")
                    match = _VERSION_PATTERN.fullmatch(version_name)
                    if match is None:
                        raise DefinitionIntegrityError("definition version path is invalid")
                    observed = os.stat(
                        version_name,
                        dir_fd=logical_fd,
                        follow_symlinks=False,
                    )
                    observed_mode = stat.S_IMODE(observed.st_mode)
                    if stat.S_ISDIR(observed.st_mode) and observed.st_uid == os.geteuid():
                        if observed_mode in {_VERSION_BUILD_MODE, _VERSION_FROZEN_MODE}:
                            continue
                        if observed_mode == _PRIVATE_DIRECTORY_MODE:
                            if self._private_version_has_commit_marker(
                                logical_fd,
                                version_name,
                            ):
                                raise DefinitionIntegrityError(
                                    "committed definition version is not published safely"
                                )
                            continue
                    record = self._read_version_at(
                        logical_fd,
                        version_name,
                        logical_id=None,
                        model=model,
                    )
                    if _logical_key(record.logical_id) != logical_name:
                        raise DefinitionIntegrityError("logical id path does not match record")
                    if record.version != int(match.group(1)):
                        raise DefinitionIntegrityError("version path does not match record")
                    records.append(record)
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError(
                "logical definition directory cannot be scanned safely"
            ) from exc
        self._validate_loaded_lineage(records)
        return records

    @classmethod
    def _private_version_has_commit_marker(
        cls,
        logical_fd: int,
        version_name: str,
    ) -> bool:
        version_fd = cls._open_owned_version_directory_at(
            logical_fd,
            version_name,
            allowed_modes={_PRIVATE_DIRECTORY_MODE},
        )
        try:
            return cls._entry_exists(version_fd, _COMMIT_NAME)
        finally:
            os.close(version_fd)

    def _read_version_at(
        self,
        logical_fd: int,
        version_name: str,
        *,
        logical_id: str | None,
        model: type[RegistrationT],
    ) -> RegistrationT:
        version_fd = self._open_published_version_directory_at(logical_fd, version_name)
        try:
            names = self._directory_names(
                version_fd,
                limit=3,
                overflow_message="definition version contains an unsafe linked or extra entry",
            )
            for name in names:
                try:
                    observed = os.stat(name, dir_fd=version_fd, follow_symlinks=False)
                except OSError as exc:
                    raise DefinitionIntegrityError(
                        "definition version contains an unsafe linked record"
                    ) from exc
                if stat.S_ISLNK(observed.st_mode) or observed.st_nlink != 1:
                    raise DefinitionIntegrityError(
                        "definition version contains an unsafe linked record"
                    )
            record_names = [name for name in names if name.endswith(".json")]
            if (
                _SEAL_NAME not in names
                or _COMMIT_NAME not in names
                or len(record_names) != 1
                or len(names) != 3
            ):
                raise DefinitionIntegrityError(
                    "definition version is empty, incomplete, or unsealed"
                )
            record_name = record_names[0]
            if not record_name.endswith(".json"):
                raise DefinitionIntegrityError("definition record file name is invalid")
            path_fingerprint = record_name.removesuffix(".json")
            if not _FINGERPRINT_PATTERN.fullmatch(path_fingerprint):
                raise DefinitionIntegrityError("definition record file name is invalid")
            payload = self._read_private_record(version_fd, record_name)
            seal = self._read_private_record(version_fd, _SEAL_NAME)
            if seal != self._seal_payload(path_fingerprint):
                raise DefinitionIntegrityError(
                    "definition record file name does not match its seal"
                )
            commit = self._read_private_record(version_fd, _COMMIT_NAME)
            if commit != self._commit_payload(path_fingerprint):
                raise DefinitionIntegrityError(
                    "definition version commit marker does not match its record"
                )
            try:
                decoded = strict_json_loads(payload)
                if not isinstance(decoded, dict):
                    raise DefinitionIntegrityError("definition schema version is missing")
                schema_version = decoded.get("schema_version")
                if schema_version in {1, 2}:
                    raise DefinitionIntegrityError(
                        f"definition schema v{schema_version} requires explicit re-registration; "
                        "availability, executable, and exit semantics cannot be inferred"
                    )
                if schema_version == 3:
                    raise DefinitionIntegrityError(
                        "definition schema v3 requires explicit re-registration; "
                        "trusted-executable/v2 evidence cannot be reinterpreted as "
                        "dependency-closed trusted-executable/v3 evidence"
                    )
                if schema_version == 4:
                    raise DefinitionIntegrityError(
                        "definition schema v4 requires explicit re-registration; "
                        "sell intent and verified fill lifecycle events cannot be inferred"
                    )
                if schema_version != 5:
                    raise DefinitionIntegrityError("unsupported definition schema version")
                record = strict_model_validate_canonical_json(model, payload)
            except DefinitionIntegrityError:
                raise
            except (TypeError, ValueError) as exc:
                raise DefinitionIntegrityError(
                    "definition record is non-canonical or its hash indicates tampering"
                ) from exc
            if record.fingerprint != path_fingerprint:
                raise DefinitionIntegrityError(
                    "definition record file name does not match fingerprint"
                )
            if logical_id is not None and record.logical_id != logical_id:
                raise DefinitionIntegrityError("logical id path does not match record")
            self._revalidate_version_directory(
                logical_fd,
                version_name,
                version_fd,
                expected_mode=_VERSION_PUBLISHED_MODE,
            )
            return record
        finally:
            os.close(version_fd)

    @staticmethod
    def _seal_payload(fingerprint: str) -> bytes:
        if not _FINGERPRINT_PATTERN.fullmatch(fingerprint):
            raise DefinitionIntegrityError("definition seal fingerprint is invalid")
        return canonical_json_bytes({"fingerprint": fingerprint})

    @staticmethod
    def _commit_payload(fingerprint: str) -> bytes:
        if not _FINGERPRINT_PATTERN.fullmatch(fingerprint):
            raise DefinitionIntegrityError("definition commit fingerprint is invalid")
        return canonical_json_bytes(
            {"fingerprint": fingerprint, "state": "committed", "version": 1}
        )

    def _read_existing_version_after_race(
        self,
        logical_fd: int,
        version_name: str,
        *,
        logical_id: str,
        model: type[RegistrationT],
    ) -> RegistrationT:
        try:
            return self._read_version_at(
                logical_fd,
                version_name,
                logical_id=logical_id,
                model=model,
            )
        except DefinitionIntegrityError as exc:
            raise DefinitionIntegrityError(
                "existing definition version is empty, unsealed, or unsafe"
            ) from exc

    def _read_existing_version_or_quarantine(
        self,
        logical_fd: int,
        version_name: str,
        *,
        logical_id: str,
        model: type[RegistrationT],
    ) -> RegistrationT:
        try:
            return self._read_existing_version_after_race(
                logical_fd,
                version_name,
                logical_id=logical_id,
                model=model,
            )
        except DefinitionIntegrityError as exc:
            self._quarantine_untrusted_version(logical_fd, version_name)
            raise DefinitionIntegrityError(
                "untrusted occupied definition version was quarantined"
            ) from exc

    def _revalidate_publication_path(
        self,
        *,
        root_chain: RootChain,
        root_fd: int,
        kind: Literal["features", "strategies"],
        kind_fd: int,
        logical_name: str,
        logical_fd: int,
    ) -> None:
        self._revalidate_child_directory(kind_fd, logical_name, logical_fd)
        self._revalidate_child_directory(root_fd, kind, kind_fd)
        self._validate_root_entries(root_fd)
        self._revalidate_root_chain(root_chain)

    @classmethod
    def _revalidate_child_directory(
        cls,
        parent_fd: int,
        name: str,
        child_fd: int,
    ) -> None:
        try:
            opened = os.fstat(child_fd)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            raise DefinitionIntegrityError("definition directory changed while being used") from exc
        if stat.S_ISLNK(active.st_mode):
            raise DefinitionIntegrityError("definition directory changed to a symbolic link")
        cls._validate_private_directory(opened)
        cls._validate_private_directory(active)
        if not _same_inode(opened, active):
            raise DefinitionIntegrityError("definition directory identity changed")

    @classmethod
    def _validate_root_entries(cls, root_fd: int) -> None:
        names = cls._directory_names(
            root_fd,
            limit=3,
            overflow_message="definition registry root path is invalid",
        )
        unexpected = set(names) - {"features", "strategies", "lookups"}
        if unexpected:
            raise DefinitionIntegrityError("definition registry root path is invalid")
        for name in names:
            child_fd = cls._open_private_directory_at(root_fd, name)
            try:
                cls._revalidate_child_directory(root_fd, name, child_fd)
            finally:
                os.close(child_fd)

    @classmethod
    def _validate_lookup_root_entries(cls, lookups_fd: int) -> None:
        names = cls._directory_names(
            lookups_fd,
            limit=2,
            overflow_message="fingerprint lookup root path is invalid",
        )
        if set(names) - {"features", "strategies"}:
            raise DefinitionIntegrityError("fingerprint lookup root path is invalid")
        for name in names:
            child_fd = cls._open_private_directory_at(lookups_fd, name)
            try:
                cls._revalidate_child_directory(lookups_fd, name, child_fd)
            finally:
                os.close(child_fd)

    @classmethod
    def _validate_lookup_kind_entries(cls, lookup_kind_fd: int) -> None:
        names = cls._directory_names(
            lookup_kind_fd,
            limit=256,
            overflow_message="fingerprint lookup prefix count is unbounded",
        )
        for name in names:
            if not re.fullmatch(r"[0-9a-f]{2}", name):
                raise DefinitionIntegrityError("fingerprint lookup prefix path is invalid")
            child_fd = cls._open_private_directory_at(lookup_kind_fd, name)
            try:
                cls._revalidate_child_directory(lookup_kind_fd, name, child_fd)
            finally:
                os.close(child_fd)

    @classmethod
    def _validate_lookup_bucket_entries(cls, bucket_fd: int) -> int:
        public_count = 0
        hidden_count = 0
        try:
            with os.scandir(bucket_fd) as entries:
                for entry in entries:
                    name = entry.name
                    if name == _PUBLISH_LOCK_NAME:
                        cls._validate_publish_lock_entry(bucket_fd)
                        continue
                    hidden = _STAGING_PATTERN.fullmatch(name) is not None
                    public = name.endswith(".json") and _FINGERPRINT_PATTERN.fullmatch(
                        name.removesuffix(".json")
                    )
                    if not hidden and not public:
                        raise DefinitionIntegrityError("fingerprint lookup entry path is invalid")
                    if hidden:
                        hidden_count += 1
                        if hidden_count > _MAX_STAGING_RECOVERY:
                            raise DefinitionIntegrityError(
                                "fingerprint lookup staging limit exceeded"
                            )
                    else:
                        public_count += 1
                        if public_count > _MAX_LOOKUP_BUCKET_ENTRIES:
                            raise DefinitionIntegrityError("fingerprint lookup bucket is unbounded")
                    try:
                        observed = os.stat(
                            name,
                            dir_fd=bucket_fd,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        if hidden:
                            continue
                        raise DefinitionIntegrityError(
                            "fingerprint lookup entry disappeared"
                        ) from None
                    if (
                        stat.S_ISLNK(observed.st_mode)
                        or not stat.S_ISREG(observed.st_mode)
                        or observed.st_uid != os.geteuid()
                        or observed.st_nlink != 1
                        or stat.S_IMODE(observed.st_mode)
                        not in ({0o600, _PRIVATE_FILE_MODE} if hidden else {_PRIVATE_FILE_MODE})
                    ):
                        raise DefinitionIntegrityError("fingerprint lookup entry is unsafe")
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError(
                "fingerprint lookup bucket cannot be scanned safely"
            ) from exc
        return public_count

    @staticmethod
    def _directory_names(
        directory_fd: int,
        *,
        limit: int,
        overflow_message: str,
    ) -> list[str]:
        if limit < 1:
            raise DefinitionIntegrityError("definition directory limit is invalid")
        names: list[str] = []
        try:
            with os.scandir(directory_fd) as entries:
                for entry in entries:
                    if len(names) == limit:
                        raise DefinitionIntegrityError(overflow_message)
                    names.append(entry.name)
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError("definition directory cannot be listed safely") from exc
        names.sort()
        if any(name in {"", ".", ".."} for name in names):
            raise DefinitionIntegrityError("definition path contains an invalid name")
        return names

    def _open_anchored_root(self, *, create: bool) -> RootChain | None:
        absolute = Path(os.path.abspath(self.root))
        names = list(absolute.parts[1:])
        if not names:
            raise DefinitionIntegrityError("filesystem root cannot be a definition registry")
        descriptors = [os.open(os.sep, _DIRECTORY_FLAGS)]
        traversed_names: list[str] = []
        try:
            for index, name in enumerate(names):
                parent_fd = descriptors[-1]
                final = index == len(names) - 1
                try:
                    before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                except FileNotFoundError as exc:
                    if not final:
                        if create:
                            raise DefinitionIntegrityError(
                                "definition registry ancestor is missing"
                            ) from exc
                        self._close_root_chain((descriptors, traversed_names))
                        return None
                    if not create:
                        self._close_root_chain((descriptors, traversed_names))
                        return None
                    with suppress(FileExistsError):
                        os.mkdir(name, _PRIVATE_DIRECTORY_MODE, dir_fd=parent_fd)
                    os.fsync(parent_fd)
                    before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                if stat.S_ISLNK(before.st_mode):
                    raise DefinitionIntegrityError(
                        "definition registry ancestor cannot be a symbolic link"
                    )
                if not stat.S_ISDIR(before.st_mode):
                    raise DefinitionIntegrityError(
                        "definition registry ancestor is not a safe directory"
                    )
                child_fd = -1
                try:
                    child_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
                    opened = os.fstat(child_fd)
                    active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                    if (
                        stat.S_ISLNK(active.st_mode)
                        or not _same_inode(before, opened)
                        or not _same_inode(opened, active)
                    ):
                        raise DefinitionIntegrityError(
                            "definition registry ancestor identity changed"
                        )
                    descriptors.append(child_fd)
                    child_fd = -1
                except DefinitionIntegrityError:
                    raise
                except OSError as exc:
                    raise DefinitionIntegrityError(
                        "definition registry ancestor is unsafe"
                    ) from exc
                finally:
                    if child_fd >= 0:
                        os.close(child_fd)
                traversed_names.append(name)
            self._validate_private_directory(os.fstat(descriptors[-1]))
            chain = (descriptors, traversed_names)
            self._revalidate_root_chain(chain)
            return chain
        except DefinitionIntegrityError:
            self._close_root_chain((descriptors, traversed_names))
            raise
        except OSError as exc:
            self._close_root_chain((descriptors, traversed_names))
            raise DefinitionIntegrityError("definition registry ancestor is unsafe") from exc
        except Exception:
            self._close_root_chain((descriptors, traversed_names))
            raise

    @classmethod
    def _revalidate_root_chain(cls, chain: RootChain) -> None:
        descriptors, names = chain
        if len(descriptors) != len(names) + 1:
            raise DefinitionIntegrityError("definition registry ancestor chain is invalid")
        for index, name in enumerate(names):
            parent_fd = descriptors[index]
            child_fd = descriptors[index + 1]
            try:
                opened = os.fstat(child_fd)
                active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as exc:
                raise DefinitionIntegrityError(
                    "definition registry ancestor changed while being used"
                ) from exc
            if stat.S_ISLNK(active.st_mode) or not stat.S_ISDIR(active.st_mode):
                raise DefinitionIntegrityError("definition registry ancestor became unsafe")
            if not _same_inode(opened, active):
                raise DefinitionIntegrityError("definition registry ancestor identity changed")
        cls._validate_private_directory(os.fstat(descriptors[-1]))

    @staticmethod
    def _close_root_chain(chain: RootChain) -> None:
        descriptors, _ = chain
        while descriptors:
            os.close(descriptors.pop())

    @staticmethod
    def _atomic_rename_no_replace(
        parent_fd: int,
        source_name: str,
        target_name: str,
    ) -> None:
        libc = ctypes.CDLL(None, use_errno=True)
        source = os.fsencode(source_name)
        target = os.fsencode(target_name)
        try:
            if sys.platform == "darwin":
                operation = libc.renameatx_np
                operation.argtypes = (
                    ctypes.c_int,
                    ctypes.c_char_p,
                    ctypes.c_int,
                    ctypes.c_char_p,
                    ctypes.c_uint,
                )
                operation.restype = ctypes.c_int
                arguments = (parent_fd, source, parent_fd, target, _RENAME_EXCL)
            elif sys.platform.startswith("linux"):
                operation = libc.renameat2
                operation.argtypes = (
                    ctypes.c_int,
                    ctypes.c_char_p,
                    ctypes.c_int,
                    ctypes.c_char_p,
                    ctypes.c_uint,
                )
                operation.restype = ctypes.c_int
                arguments = (parent_fd, source, parent_fd, target, _RENAME_NOREPLACE)
            else:
                raise DefinitionIntegrityError(
                    "atomic no-replace publication is unsupported on this platform"
                )
        except AttributeError as exc:
            raise DefinitionIntegrityError(
                "atomic no-replace publication primitive is unavailable"
            ) from exc
        ctypes.set_errno(0)
        if operation(*arguments) == 0:
            return
        error_number = ctypes.get_errno()
        if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
            raise FileExistsError(error_number, os.strerror(error_number), target_name)
        raise DefinitionIntegrityError(
            f"atomic no-replace publication failed: {os.strerror(error_number)}"
        )

    @staticmethod
    def _validate_publish_lock_stat(observed: os.stat_result) -> None:
        if not stat.S_ISREG(observed.st_mode):
            raise DefinitionIntegrityError("definition publish lock is not a regular file")
        if observed.st_uid != os.geteuid():
            raise DefinitionIntegrityError("definition publish lock owner is unsafe")
        if observed.st_nlink != 1:
            raise DefinitionIntegrityError("definition publish lock hard link count is unsafe")
        if stat.S_IMODE(observed.st_mode) != 0o600:
            raise DefinitionIntegrityError("definition publish lock mode is unsafe")

    @classmethod
    def _open_publish_lock(cls, logical_fd: int) -> int:
        for _ in range(3):
            descriptor = -1
            try:
                try:
                    descriptor = os.open(
                        _PUBLISH_LOCK_NAME,
                        os.O_RDWR
                        | os.O_CREAT
                        | os.O_EXCL
                        | getattr(os, "O_NOFOLLOW", 0)
                        | getattr(os, "O_CLOEXEC", 0),
                        0o600,
                        dir_fd=logical_fd,
                    )
                    if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600:
                        os.fchmod(descriptor, 0o600)
                    os.fsync(descriptor)
                except FileExistsError:
                    descriptor = os.open(
                        _PUBLISH_LOCK_NAME,
                        os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                        dir_fd=logical_fd,
                    )
            except FileNotFoundError:
                if descriptor >= 0:
                    os.close(descriptor)
                continue
            except OSError as exc:
                if descriptor >= 0:
                    os.close(descriptor)
                raise DefinitionIntegrityError("definition publish lock is unsafe") from exc
            try:
                opened = os.fstat(descriptor)
                active = os.stat(
                    _PUBLISH_LOCK_NAME,
                    dir_fd=logical_fd,
                    follow_symlinks=False,
                )
                cls._validate_publish_lock_stat(opened)
                cls._validate_publish_lock_stat(active)
                if cls._file_identity(opened) != cls._file_identity(active):
                    os.close(descriptor)
                    descriptor = -1
                    continue
                return descriptor
            except Exception:
                if descriptor >= 0:
                    os.close(descriptor)
                raise
        raise DefinitionIntegrityError("definition publish lock is unsafe")

    @classmethod
    def _acquire_publish_lock(cls, logical_fd: int) -> int:
        descriptor = cls._open_publish_lock(logical_fd)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            opened = os.fstat(descriptor)
            active = os.stat(
                _PUBLISH_LOCK_NAME,
                dir_fd=logical_fd,
                follow_symlinks=False,
            )
            cls._validate_publish_lock_stat(opened)
            cls._validate_publish_lock_stat(active)
            if cls._file_identity(opened) != cls._file_identity(active):
                raise DefinitionIntegrityError(
                    "definition publish lock changed while acquiring ownership"
                )
            return descriptor
        except Exception:
            with suppress(OSError):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
            raise

    @staticmethod
    def _release_publish_lock(descriptor: int) -> None:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    @classmethod
    def _validate_publish_lock_entry(cls, logical_fd: int) -> None:
        descriptor = -1
        try:
            before = os.stat(
                _PUBLISH_LOCK_NAME,
                dir_fd=logical_fd,
                follow_symlinks=False,
            )
            descriptor = os.open(
                _PUBLISH_LOCK_NAME,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=logical_fd,
            )
            opened = os.fstat(descriptor)
            active = os.stat(
                _PUBLISH_LOCK_NAME,
                dir_fd=logical_fd,
                follow_symlinks=False,
            )
            for observed in (before, opened, active):
                cls._validate_publish_lock_stat(observed)
            if not (
                cls._file_identity(before)
                == cls._file_identity(opened)
                == cls._file_identity(active)
            ):
                raise DefinitionIntegrityError("definition publish lock identity changed")
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError("definition publish lock is unsafe") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @classmethod
    def _recover_stale_staging(cls, logical_fd: int) -> None:
        stale, more_staging = cls._bounded_matching_names(
            logical_fd,
            pattern=_STAGING_PATTERN,
            limit=_MAX_STAGING_RECOVERY,
        )
        for name in stale:
            if not cls._remove_hidden_staging(logical_fd, name):
                os.fsync(logical_fd)
                raise DefinitionIntegrityError(
                    "hidden staging recovery limit exceeded; retry to continue bounded cleanup"
                )
        os.fsync(logical_fd)
        if more_staging:
            raise DefinitionIntegrityError(
                "hidden staging recovery limit exceeded; retry to continue bounded cleanup"
            )

    @classmethod
    def _recover_uncommitted_versions(cls, logical_fd: int) -> None:
        recovered = 0
        scanned = 0
        quarantined_untrusted = False
        try:
            with os.scandir(logical_fd) as entries:
                for entry in entries:
                    scanned += 1
                    if scanned > _MAX_RECOVERY_SCAN_ENTRIES:
                        raise DefinitionIntegrityError(
                            "definition version recovery scan limit exceeded"
                        )
                    if _VERSION_PATTERN.fullmatch(entry.name) is None:
                        continue
                    observed = os.stat(
                        entry.name,
                        dir_fd=logical_fd,
                        follow_symlinks=False,
                    )
                    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                        raise DefinitionIntegrityError(
                            "definition version recovery found an unsafe path"
                        )
                    if observed.st_uid != os.geteuid():
                        raise DefinitionIntegrityError(
                            "definition version recovery found an unsafe owner"
                        )
                    mode = stat.S_IMODE(observed.st_mode)
                    if mode == _VERSION_PUBLISHED_MODE:
                        continue
                    if mode not in {
                        _PRIVATE_DIRECTORY_MODE,
                        _VERSION_BUILD_MODE,
                        _VERSION_FROZEN_MODE,
                    }:
                        raise DefinitionIntegrityError(
                            "definition version recovery found an unsafe mode"
                        )
                    recovered += 1
                    if recovered > _MAX_STAGING_RECOVERY:
                        raise DefinitionIntegrityError(
                            "uncommitted version recovery limit exceeded; retry cleanup"
                        )
                    version_fd = cls._open_owned_version_directory_at(
                        logical_fd,
                        entry.name,
                        allowed_modes={
                            _PRIVATE_DIRECTORY_MODE,
                            _VERSION_BUILD_MODE,
                            _VERSION_FROZEN_MODE,
                        },
                    )
                    try:
                        if mode != _PRIVATE_DIRECTORY_MODE:
                            os.fchmod(version_fd, _PRIVATE_DIRECTORY_MODE)
                            os.fsync(version_fd)
                        cls._revalidate_version_directory(
                            logical_fd,
                            entry.name,
                            version_fd,
                            expected_mode=_PRIVATE_DIRECTORY_MODE,
                        )
                        if not cls._is_well_formed_uncommitted_version(version_fd):
                            quarantined_untrusted = True
                    finally:
                        os.close(version_fd)
                    cls._quarantine_untrusted_version(logical_fd, entry.name)
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError(
                "definition version recovery directory cannot be scanned safely"
            ) from exc
        os.fsync(logical_fd)
        if quarantined_untrusted:
            raise DefinitionIntegrityError(
                "untrusted uncommitted definition version was quarantined"
            )

    @classmethod
    def _is_well_formed_uncommitted_version(cls, version_fd: int) -> bool:
        try:
            names = cls._directory_names(
                version_fd,
                limit=3,
                overflow_message="uncommitted definition version is unbounded",
            )
            record_names = [name for name in names if name.endswith(".json")]
            if (
                len(record_names) != 1
                or _SEAL_NAME not in names
                or set(names) - {record_names[0], _SEAL_NAME, _COMMIT_NAME}
            ):
                return False
            fingerprint = record_names[0].removesuffix(".json")
            if not _FINGERPRINT_PATTERN.fullmatch(fingerprint):
                return False
            for name in names:
                observed = os.stat(name, dir_fd=version_fd, follow_symlinks=False)
                cls._validate_private_file(observed)
            if cls._read_private_record(version_fd, _SEAL_NAME) != cls._seal_payload(fingerprint):
                return False
            return not (
                _COMMIT_NAME in names
                and cls._read_private_record(version_fd, _COMMIT_NAME)
                != cls._commit_payload(fingerprint)
            )
        except (DefinitionIntegrityError, OSError):
            return False

    @classmethod
    def _recover_stale_lookup_files(cls, bucket_fd: int) -> None:
        stale, more_staging = cls._bounded_matching_names(
            bucket_fd,
            pattern=_STAGING_PATTERN,
            limit=_MAX_STAGING_RECOVERY,
        )
        for name in stale:
            cls._remove_hidden_lookup_file(bucket_fd, name)
        os.fsync(bucket_fd)
        if more_staging:
            raise DefinitionIntegrityError(
                "lookup staging recovery limit exceeded; retry to continue bounded cleanup"
            )

    @staticmethod
    def _bounded_matching_names(
        directory_fd: int,
        *,
        pattern: re.Pattern[str],
        limit: int,
    ) -> tuple[list[str], bool]:
        if limit < 1:
            raise DefinitionIntegrityError("staging recovery limit is invalid")
        matches: list[str] = []
        scanned = 0
        try:
            with os.scandir(directory_fd) as entries:
                for entry in entries:
                    scanned += 1
                    if scanned > _MAX_RECOVERY_SCAN_ENTRIES:
                        raise DefinitionIntegrityError("staging recovery scan limit exceeded")
                    if not pattern.fullmatch(entry.name):
                        continue
                    if len(matches) == limit:
                        return matches, True
                    matches.append(entry.name)
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError(
                "staging recovery directory cannot be scanned safely"
            ) from exc
        return matches, False

    @staticmethod
    def _bounded_entry_names(
        directory_fd: int,
        *,
        limit: int,
    ) -> tuple[list[str], bool]:
        if limit < 1:
            raise DefinitionIntegrityError("staging entry recovery limit is invalid")
        names: list[str] = []
        try:
            with os.scandir(directory_fd) as entries:
                for entry in entries:
                    if len(names) == limit:
                        return names, True
                    names.append(entry.name)
        except OSError as exc:
            raise DefinitionIntegrityError("staging directory cannot be scanned safely") from exc
        return names, False

    @classmethod
    def _remove_hidden_lookup_file(cls, bucket_fd: int, name: str) -> None:
        if not _STAGING_PATTERN.fullmatch(name):
            raise DefinitionIntegrityError("hidden lookup staging path is invalid")
        if not cls._entry_exists(bucket_fd, name):
            return
        cls._remove_private_file_via_quarantine(
            bucket_fd,
            name,
            allowed_modes={0o600, _PRIVATE_FILE_MODE},
            label="hidden lookup staging file",
        )

    @classmethod
    def _remove_private_file_via_quarantine(
        cls,
        parent_fd: int,
        name: str,
        *,
        allowed_modes: set[int],
        label: str,
        require_single_link: bool = True,
    ) -> None:
        descriptor = -1
        quarantine_name = f".publish-{uuid4().hex}"
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            descriptor = os.open(name, _FILE_FLAGS, dir_fd=parent_fd)
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            for observed in (before, opened, active):
                if (
                    not stat.S_ISREG(observed.st_mode)
                    or observed.st_uid != os.geteuid()
                    or observed.st_nlink < 1
                    or (require_single_link and observed.st_nlink != 1)
                    or stat.S_IMODE(observed.st_mode) not in allowed_modes
                ):
                    raise DefinitionIntegrityError(f"{label} is unsafe")
            if not (
                cls._file_identity(before)
                == cls._file_identity(opened)
                == cls._file_identity(active)
            ):
                raise DefinitionIntegrityError(f"{label} identity changed")
            cls._atomic_rename_no_replace(
                parent_fd,
                name,
                quarantine_name,
            )
            os.fsync(parent_fd)
            quarantined = os.stat(
                quarantine_name,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            if (
                not _same_inode(opened, quarantined)
                or not stat.S_ISREG(quarantined.st_mode)
                or quarantined.st_uid != os.geteuid()
                or quarantined.st_nlink != opened.st_nlink
                or (require_single_link and quarantined.st_nlink != 1)
                or stat.S_IMODE(quarantined.st_mode) not in allowed_modes
            ):
                cls._restore_quarantine_entry(
                    parent_fd,
                    quarantine_name=quarantine_name,
                    original_name=name,
                )
                raise DefinitionIntegrityError(f"{label} identity changed")
            os.unlink(quarantine_name, dir_fd=parent_fd)
            os.fsync(parent_fd)
            if cls._entry_exists(parent_fd, name):
                raise DefinitionIntegrityError(f"{label} replacement appeared")
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError(f"{label} cannot be removed safely") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @classmethod
    def _restore_quarantine_entry(
        cls,
        parent_fd: int,
        *,
        quarantine_name: str,
        original_name: str,
    ) -> None:
        try:
            cls._atomic_rename_no_replace(
                parent_fd,
                quarantine_name,
                original_name,
            )
            os.fsync(parent_fd)
        except (DefinitionIntegrityError, FileExistsError):
            pass

    @classmethod
    def _validate_hidden_staging(cls, logical_fd: int, name: str) -> None:
        if not _STAGING_PATTERN.fullmatch(name):
            raise DefinitionIntegrityError("hidden definition staging path is invalid")
        try:
            before = os.stat(name, dir_fd=logical_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if stat.S_ISLNK(before.st_mode):
            raise DefinitionIntegrityError(
                "hidden definition staging directory cannot be a symbolic link"
            )
        try:
            staging_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=logical_fd)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise DefinitionIntegrityError("hidden definition staging directory is unsafe") from exc
        try:
            opened = os.fstat(staging_fd)
            if (
                not stat.S_ISDIR(opened.st_mode)
                or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode)
                not in {_PRIVATE_DIRECTORY_MODE, _VERSION_PUBLISHED_MODE}
            ):
                raise DefinitionIntegrityError("hidden definition staging directory mode is unsafe")
            if not _same_inode(before, opened):
                raise DefinitionIntegrityError(
                    "hidden definition staging directory identity changed"
                )
            try:
                active = os.stat(name, dir_fd=logical_fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            if stat.S_ISLNK(active.st_mode) or not _same_inode(opened, active):
                raise DefinitionIntegrityError(
                    "hidden definition staging directory identity changed"
                )
        finally:
            os.close(staging_fd)

    @classmethod
    def _remove_hidden_staging(cls, logical_fd: int, name: str) -> bool:
        if not cls._entry_exists(logical_fd, name):
            return True
        if not _STAGING_PATTERN.fullmatch(name):
            raise DefinitionIntegrityError("hidden definition staging path is invalid")
        staging_fd = cls._open_staging_directory_at(logical_fd, name)
        staging_mode = stat.S_IMODE(os.fstat(staging_fd).st_mode)
        quarantine_name = f".publish-{uuid4().hex}"
        try:
            if staging_mode != _PRIVATE_DIRECTORY_MODE:
                os.fchmod(staging_fd, _PRIVATE_DIRECTORY_MODE)
                staging_mode = _PRIVATE_DIRECTORY_MODE
            cls._revalidate_version_directory(
                logical_fd,
                name,
                staging_fd,
                expected_mode=staging_mode,
            )
            cls._atomic_rename_no_replace(
                logical_fd,
                name,
                quarantine_name,
            )
            os.fsync(logical_fd)
            try:
                cls._revalidate_version_directory(
                    logical_fd,
                    quarantine_name,
                    staging_fd,
                    expected_mode=staging_mode,
                )
            except DefinitionIntegrityError:
                cls._restore_quarantine_entry(
                    logical_fd,
                    quarantine_name=quarantine_name,
                    original_name=name,
                )
                raise
            os.fchmod(staging_fd, _PRIVATE_DIRECTORY_MODE)
            staging_mode = _PRIVATE_DIRECTORY_MODE
            entries, truncated = cls._bounded_entry_names(
                staging_fd,
                limit=_MAX_STAGING_RECOVERY,
            )
            for entry in entries:
                cls._revalidate_version_directory(
                    logical_fd,
                    quarantine_name,
                    staging_fd,
                    expected_mode=staging_mode,
                )
                cls._remove_staging_entry(staging_fd, entry)
            os.fsync(staging_fd)
            if truncated:
                return False
            cls._revalidate_version_directory(
                logical_fd,
                quarantine_name,
                staging_fd,
                expected_mode=staging_mode,
            )
            os.rmdir(quarantine_name, dir_fd=logical_fd)
            os.fsync(logical_fd)
            if cls._entry_exists(logical_fd, name):
                raise DefinitionIntegrityError(
                    "hidden definition staging directory replacement appeared"
                )
            return True
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError(
                "hidden definition staging directory cannot be removed safely"
            ) from exc
        finally:
            os.close(staging_fd)

    @classmethod
    def _validate_staging_entry(cls, staging_fd: int, entry: str) -> None:
        if not entry or "/" in entry or "\x00" in entry or entry in {".", ".."}:
            raise DefinitionIntegrityError("hidden definition staging entry is unsafe")
        try:
            observed = os.stat(entry, dir_fd=staging_fd, follow_symlinks=False)
        except OSError as exc:
            raise DefinitionIntegrityError("hidden definition staging entry is unsafe") from exc
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink < 1
            or stat.S_IMODE(observed.st_mode) not in {0o600, _PRIVATE_FILE_MODE}
        ):
            raise DefinitionIntegrityError("hidden definition staging entry is unsafe")

    @classmethod
    def _remove_staging_entry(cls, staging_fd: int, entry: str) -> None:
        cls._validate_staging_entry(staging_fd, entry)
        cls._remove_private_file_via_quarantine(
            staging_fd,
            entry,
            allowed_modes={0o600, _PRIVATE_FILE_MODE},
            label="hidden definition staging entry",
            require_single_link=False,
        )

    @classmethod
    def _ensure_child_directory(cls, parent_fd: int, name: str) -> int:
        try:
            os.mkdir(name, _PRIVATE_DIRECTORY_MODE, dir_fd=parent_fd)
        except FileExistsError:
            pass
        except OSError as exc:
            raise DefinitionIntegrityError("definition directory cannot be created safely") from exc
        child_fd = cls._open_private_directory_at(parent_fd, name)
        try:
            os.fsync(parent_fd)
            cls._revalidate_child_directory(parent_fd, name, child_fd)
            return child_fd
        except Exception:
            os.close(child_fd)
            raise

    @classmethod
    def _open_private_directory_at(cls, parent_fd: int, name: str) -> int:
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise DefinitionIntegrityError("definition directory cannot be a symbolic link")
            descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError("definition directory is missing or unsafe") from exc
        try:
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            cls._validate_private_directory(opened)
            if not _same_inode(before, opened) or not _same_inode(opened, active):
                raise DefinitionIntegrityError("definition directory identity changed")
            return descriptor
        except Exception:
            os.close(descriptor)
            raise

    @classmethod
    def _open_published_version_directory_at(cls, parent_fd: int, name: str) -> int:
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise DefinitionIntegrityError("definition version cannot be a symbolic link")
            descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError("definition version is missing or unsafe") from exc
        try:
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            for observed in (before, opened, active):
                if (
                    not stat.S_ISDIR(observed.st_mode)
                    or observed.st_uid != os.geteuid()
                    or stat.S_IMODE(observed.st_mode) != _VERSION_PUBLISHED_MODE
                ):
                    raise DefinitionIntegrityError(
                        "definition version directory is not published safely"
                    )
            if not _same_inode(before, opened) or not _same_inode(opened, active):
                raise DefinitionIntegrityError("definition version directory identity changed")
            return descriptor
        except Exception:
            os.close(descriptor)
            raise

    @classmethod
    def _open_owned_version_directory_at(
        cls,
        parent_fd: int,
        name: str,
        *,
        allowed_modes: set[int],
    ) -> int:
        descriptor = -1
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise DefinitionIntegrityError("definition version cannot be a symbolic link")
            descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            for observed in (before, opened, active):
                if (
                    not stat.S_ISDIR(observed.st_mode)
                    or observed.st_uid != os.geteuid()
                    or stat.S_IMODE(observed.st_mode) not in allowed_modes
                ):
                    raise DefinitionIntegrityError(
                        "definition version directory is not owned safely"
                    )
            if not _same_inode(before, opened) or not _same_inode(opened, active):
                raise DefinitionIntegrityError("definition version directory identity changed")
            return descriptor
        except DefinitionIntegrityError:
            if descriptor >= 0:
                os.close(descriptor)
            raise
        except OSError as exc:
            if descriptor >= 0:
                os.close(descriptor)
            raise DefinitionIntegrityError("definition version is missing or unsafe") from exc

    @classmethod
    def _open_staging_directory_at(cls, parent_fd: int, name: str) -> int:
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise DefinitionIntegrityError(
                    "hidden definition staging directory cannot be a symbolic link"
                )
            descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError(
                "hidden definition staging directory is missing or unsafe"
            ) from exc
        try:
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            for observed in (before, opened, active):
                if (
                    not stat.S_ISDIR(observed.st_mode)
                    or observed.st_uid != os.geteuid()
                    or stat.S_IMODE(observed.st_mode)
                    not in {_PRIVATE_DIRECTORY_MODE, _VERSION_PUBLISHED_MODE}
                ):
                    raise DefinitionIntegrityError(
                        "hidden definition staging directory mode is unsafe"
                    )
            if not _same_inode(before, opened) or not _same_inode(opened, active):
                raise DefinitionIntegrityError(
                    "hidden definition staging directory identity changed"
                )
            return descriptor
        except Exception:
            os.close(descriptor)
            raise

    @staticmethod
    def _validate_private_directory(observed: os.stat_result) -> None:
        if not stat.S_ISDIR(observed.st_mode):
            raise DefinitionIntegrityError("definition path is not a directory")
        if observed.st_uid != os.geteuid():
            raise DefinitionIntegrityError("definition directory owner is unsafe")
        if stat.S_IMODE(observed.st_mode) != _PRIVATE_DIRECTORY_MODE:
            raise DefinitionIntegrityError("definition directory is not owner-only")

    @staticmethod
    def _validate_private_file(observed: os.stat_result) -> None:
        if not stat.S_ISREG(observed.st_mode):
            raise DefinitionIntegrityError("definition record is not a regular file")
        if observed.st_uid != os.geteuid():
            raise DefinitionIntegrityError("definition record owner is unsafe")
        if observed.st_nlink != 1:
            raise DefinitionIntegrityError("definition record hard link count is unsafe")
        if stat.S_IMODE(observed.st_mode) != _PRIVATE_FILE_MODE:
            raise DefinitionIntegrityError("definition record is not owner-only immutable data")

    @staticmethod
    def _file_identity(observed: os.stat_result) -> tuple[int, ...]:
        return (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_uid,
            observed.st_gid,
            observed.st_nlink,
            observed.st_size,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )

    @classmethod
    def _capture_staging_record_lease(
        cls,
        directory_fd: int,
        name: str,
        expected_payload: bytes,
    ) -> _StagingRecordLease:
        descriptor = -1
        try:
            observed = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            cls._validate_private_file(observed)
            descriptor = os.open(name, _FILE_FLAGS, dir_fd=directory_fd)
            opened = os.fstat(descriptor)
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            for item in (opened, current):
                cls._validate_private_file(item)
            identity = cls._file_identity(observed)
            if identity != cls._file_identity(opened) or identity != cls._file_identity(current):
                raise DefinitionIntegrityError("definition staging record identity changed")
            payload = cls._read_leased_payload(descriptor, identity=identity)
            if payload != expected_payload:
                raise DefinitionIntegrityError("definition staging record hash changed")
            lease = _StagingRecordLease(
                descriptor=descriptor,
                identity=identity,
                content_sha256=hashlib.sha256(payload).hexdigest(),
            )
            descriptor = -1
            return lease
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError("definition staging record cannot be leased") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @classmethod
    def _read_leased_payload(
        cls,
        descriptor: int,
        *,
        identity: tuple[int, ...],
    ) -> bytes:
        try:
            before = os.fstat(descriptor)
            try:
                cls._validate_private_file(before)
            except DefinitionIntegrityError as exc:
                raise DefinitionIntegrityError("definition staging lease identity changed") from exc
            if cls._file_identity(before) != identity:
                raise DefinitionIntegrityError("definition staging lease identity changed")
            os.lseek(descriptor, 0, os.SEEK_SET)
            remaining = before.st_size
            chunks: list[bytes] = []
            while remaining:
                chunk = os.read(descriptor, min(remaining, 64 * 1024))
                if not chunk:
                    raise DefinitionIntegrityError("definition staging lease was truncated")
                chunks.append(chunk)
                remaining -= len(chunk)
            if os.read(descriptor, 1):
                raise DefinitionIntegrityError("definition staging lease grew while reading")
            after = os.fstat(descriptor)
            try:
                cls._validate_private_file(after)
            except DefinitionIntegrityError as exc:
                raise DefinitionIntegrityError("definition staging lease identity changed") from exc
            if cls._file_identity(after) != identity:
                raise DefinitionIntegrityError("definition staging lease identity changed")
            return b"".join(chunks)
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError("definition staging lease cannot be read") from exc

    @classmethod
    def _validate_staging_for_publish(
        cls,
        *,
        logical_fd: int,
        staging_name: str,
        staging_fd: int,
        expected: dict[str, tuple[_StagingRecordLease, bytes]],
    ) -> None:
        cls._revalidate_staging_leases(
            logical_fd=logical_fd,
            staging_name=staging_name,
            staging_fd=staging_fd,
            expected_mode=_PRIVATE_DIRECTORY_MODE,
            expected=expected,
        )
        os.fchmod(staging_fd, _VERSION_PUBLISHED_MODE)
        os.fsync(staging_fd)
        cls._revalidate_staging_leases(
            logical_fd=logical_fd,
            staging_name=staging_name,
            staging_fd=staging_fd,
            expected_mode=_VERSION_PUBLISHED_MODE,
            expected=expected,
        )

    @classmethod
    def _revalidate_staging_leases(
        cls,
        *,
        logical_fd: int,
        staging_name: str,
        staging_fd: int,
        expected_mode: int,
        expected: dict[str, tuple[_StagingRecordLease, bytes]],
    ) -> None:
        try:
            cls._revalidate_version_directory(
                logical_fd,
                staging_name,
                staging_fd,
                expected_mode=expected_mode,
            )
        except DefinitionIntegrityError as exc:
            raise DefinitionIntegrityError(
                "definition staging directory identity or mode changed"
            ) from exc
        names = cls._directory_names(
            staging_fd,
            limit=len(expected),
            overflow_message="definition staging contains linked or extra entries",
        )
        if set(names) != set(expected):
            raise DefinitionIntegrityError("definition staging contains unexpected entries")
        for name, (lease, expected_payload) in expected.items():
            observed = os.stat(name, dir_fd=staging_fd, follow_symlinks=False)
            cls._validate_private_file(observed)
            if cls._file_identity(observed) != lease.identity:
                raise DefinitionIntegrityError("definition staging record identity changed")
            payload = cls._read_private_record(staging_fd, name)
            current = os.stat(name, dir_fd=staging_fd, follow_symlinks=False)
            cls._validate_private_file(current)
            if cls._file_identity(current) != lease.identity:
                raise DefinitionIntegrityError("definition staging record identity changed")
            if (
                hashlib.sha256(payload).hexdigest() != lease.content_sha256
                or payload != expected_payload
            ):
                raise DefinitionIntegrityError("definition staging record hash changed")
            leased_payload = cls._read_leased_payload(
                lease.descriptor,
                identity=lease.identity,
            )
            if (
                hashlib.sha256(leased_payload).hexdigest() != lease.content_sha256
                or leased_payload != expected_payload
            ):
                raise DefinitionIntegrityError("definition staging lease hash changed")
        try:
            cls._revalidate_version_directory(
                logical_fd,
                staging_name,
                staging_fd,
                expected_mode=expected_mode,
            )
        except DefinitionIntegrityError as exc:
            raise DefinitionIntegrityError(
                "definition staging directory identity or mode changed"
            ) from exc

    @classmethod
    def _publish_version_from_leases(
        cls,
        *,
        logical_fd: int,
        version_name: str,
        staging_name: str,
        staging_fd: int,
        expected: dict[str, tuple[_StagingRecordLease, bytes]],
        ownership: _VersionPublicationOwnership,
    ) -> int:
        cls._revalidate_staging_leases(
            logical_fd=logical_fd,
            staging_name=staging_name,
            staging_fd=staging_fd,
            expected_mode=_VERSION_PUBLISHED_MODE,
            expected=expected,
        )
        cls._validate_materialized_version(staging_fd, expected=expected)
        cls._revalidate_staging_leases(
            logical_fd=logical_fd,
            staging_name=staging_name,
            staging_fd=staging_fd,
            expected_mode=_VERSION_PUBLISHED_MODE,
            expected=expected,
        )
        os.fchmod(staging_fd, _PRIVATE_DIRECTORY_MODE)
        cls._revalidate_staging_leases(
            logical_fd=logical_fd,
            staging_name=staging_name,
            staging_fd=staging_fd,
            expected_mode=_PRIVATE_DIRECTORY_MODE,
            expected=expected,
        )
        try:
            cls._atomic_rename_no_replace(logical_fd, staging_name, version_name)
        except BaseException:
            try:
                active = os.stat(
                    version_name,
                    dir_fd=logical_fd,
                    follow_symlinks=False,
                )
                opened = os.fstat(staging_fd)
            except OSError:
                raise
            if _same_inode(opened, active):
                ownership.claim(staging_fd)
            else:
                cls._quarantine_untrusted_version(logical_fd, version_name)
            raise
        active = os.stat(version_name, dir_fd=logical_fd, follow_symlinks=False)
        opened = os.fstat(staging_fd)
        if not _same_inode(opened, active):
            cls._quarantine_untrusted_version(logical_fd, version_name)
            raise DefinitionIntegrityError(
                "definition version identity changed and was quarantined"
            )
        ownership.claim(staging_fd)
        try:
            cls._revalidate_staging_leases(
                logical_fd=logical_fd,
                staging_name=version_name,
                staging_fd=staging_fd,
                expected_mode=_PRIVATE_DIRECTORY_MODE,
                expected=expected,
            )
            os.fsync(staging_fd)
            os.fsync(logical_fd)
            return os.dup(staging_fd)
        except BaseException:
            raise

    @classmethod
    def _commit_published_version(
        cls,
        *,
        logical_fd: int,
        version_name: str,
        version_fd: int,
        fingerprint: str,
        expected: dict[str, tuple[_StagingRecordLease, bytes]],
    ) -> None:
        cls._revalidate_staging_leases(
            logical_fd=logical_fd,
            staging_name=version_name,
            staging_fd=version_fd,
            expected_mode=_PRIVATE_DIRECTORY_MODE,
            expected=expected,
        )
        commit_payload = cls._commit_payload(fingerprint)
        cls._write_private_record(version_fd, _COMMIT_NAME, commit_payload)
        cls._validate_materialized_version(
            version_fd,
            expected=expected,
            commit_payload=commit_payload,
        )
        os.fsync(version_fd)
        os.fchmod(version_fd, _VERSION_PUBLISHED_MODE)
        cls._revalidate_version_directory(
            logical_fd,
            version_name,
            version_fd,
            expected_mode=_VERSION_PUBLISHED_MODE,
        )
        cls._validate_materialized_version(
            version_fd,
            expected=expected,
            commit_payload=commit_payload,
        )
        os.fsync(version_fd)
        os.fsync(logical_fd)

    @classmethod
    def _quarantine_untrusted_version(
        cls,
        logical_fd: int,
        version_name: str,
    ) -> None:
        quarantine_name = f".publish-{uuid4().hex}"
        try:
            cls._atomic_rename_no_replace(
                logical_fd,
                version_name,
                quarantine_name,
            )
        except FileNotFoundError:
            return
        except DefinitionIntegrityError:
            if not cls._entry_exists(logical_fd, version_name):
                return
            raise
        os.fsync(logical_fd)
        try:
            cls._remove_hidden_staging(logical_fd, quarantine_name)
        except DefinitionIntegrityError as exc:
            if cls._entry_exists(logical_fd, version_name):
                raise DefinitionIntegrityError(
                    "untrusted definition version could not be isolated"
                ) from exc
            raise
        if cls._entry_exists(logical_fd, version_name):
            raise DefinitionIntegrityError(
                "untrusted definition version reappeared after quarantine"
            )
        os.fsync(logical_fd)

    @classmethod
    def _validate_materialized_version(
        cls,
        version_fd: int,
        *,
        expected: dict[str, tuple[_StagingRecordLease, bytes]],
        commit_payload: bytes | None = None,
    ) -> None:
        expected_names = set(expected)
        if commit_payload is not None:
            expected_names.add(_COMMIT_NAME)
        names = cls._directory_names(
            version_fd,
            limit=len(expected_names),
            overflow_message="definition version contains linked or extra entries",
        )
        if set(names) != expected_names:
            raise DefinitionIntegrityError("definition version contains unexpected entries")
        for name, (_, expected_payload) in expected.items():
            payload = cls._read_private_record(version_fd, name)
            if payload != expected_payload:
                raise DefinitionIntegrityError("definition version content hash changed")
        if commit_payload is not None:
            payload = cls._read_private_record(version_fd, _COMMIT_NAME)
            if payload != commit_payload:
                raise DefinitionIntegrityError("definition version commit marker changed")

    @classmethod
    def _remove_unpublished_version(
        cls,
        logical_fd: int,
        version_name: str,
        version_fd: int,
    ) -> None:
        try:
            opened = os.fstat(version_fd)
            active = os.stat(version_name, dir_fd=logical_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise DefinitionIntegrityError(
                "unpublished definition version cannot be inspected"
            ) from exc
        if stat.S_ISLNK(active.st_mode) or not _same_inode(opened, active):
            raise DefinitionIntegrityError("unpublished definition version identity changed")
        os.fchmod(version_fd, _PRIVATE_DIRECTORY_MODE)
        entries, truncated = cls._bounded_entry_names(version_fd, limit=3)
        if truncated:
            raise DefinitionIntegrityError("unpublished definition version is unbounded")
        for entry in entries:
            cls._remove_staging_entry(version_fd, entry)
        os.fsync(version_fd)
        os.rmdir(version_name, dir_fd=logical_fd)
        os.fsync(logical_fd)

    @classmethod
    def _revalidate_version_directory(
        cls,
        parent_fd: int,
        name: str,
        child_fd: int,
        *,
        expected_mode: int,
    ) -> None:
        try:
            opened = os.fstat(child_fd)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            raise DefinitionIntegrityError("definition version changed while being used") from exc
        for observed in (opened, active):
            if (
                not stat.S_ISDIR(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or stat.S_IMODE(observed.st_mode) != expected_mode
            ):
                raise DefinitionIntegrityError("definition version directory is unsafe")
        if not _same_inode(opened, active):
            raise DefinitionIntegrityError("definition version directory identity changed")

    @staticmethod
    def _write_private_record(directory_fd: int, name: str, payload: bytes) -> None:
        descriptor = -1
        try:
            descriptor = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_fd,
            )
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise DefinitionIntegrityError("definition record write made no progress")
                offset += written
            os.fsync(descriptor)
            os.fchmod(descriptor, _PRIVATE_FILE_MODE)
            os.fsync(descriptor)
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError("definition record cannot be written safely") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @staticmethod
    def _read_private_record(directory_fd: int, name: str) -> bytes:
        descriptor = -1
        try:
            before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise DefinitionIntegrityError("definition record cannot be a symbolic link")
            descriptor = os.open(name, _FILE_FLAGS, dir_fd=directory_fd)
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            for observed in (before, opened, active):
                ImmutableDefinitionRegistry._validate_private_file(observed)
            if not (
                ImmutableDefinitionRegistry._file_identity(before)
                == ImmutableDefinitionRegistry._file_identity(opened)
                == ImmutableDefinitionRegistry._file_identity(active)
            ):
                raise DefinitionIntegrityError("definition record identity changed")
            if opened.st_size > _MAX_RECORD_BYTES:
                raise DefinitionIntegrityError("definition record exceeds size limit")
            payload = os.read(descriptor, _MAX_RECORD_BYTES + 1)
            after = os.fstat(descriptor)
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if len(payload) > _MAX_RECORD_BYTES:
                raise DefinitionIntegrityError("definition record exceeds size limit")
            for observed in (after, current):
                ImmutableDefinitionRegistry._validate_private_file(observed)
            if not (
                ImmutableDefinitionRegistry._file_identity(opened)
                == ImmutableDefinitionRegistry._file_identity(after)
                == ImmutableDefinitionRegistry._file_identity(current)
            ):
                raise DefinitionIntegrityError("definition record changed while reading")
            return payload
        except DefinitionIntegrityError:
            raise
        except OSError as exc:
            raise DefinitionIntegrityError("definition record is missing or unsafe") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @staticmethod
    def _entry_exists(directory_fd: int, name: str) -> bool:
        try:
            os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        return True
