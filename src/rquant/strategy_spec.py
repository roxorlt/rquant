"""Immutable strategy declarations for independent live and replay runners."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, StrEnum
from types import MappingProxyType

from pydantic import Field, field_serializer, field_validator, model_validator

from rquant.feature_contracts import FeatureRequirement, RequirementLevel
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256


class StrategyRunMode(StrEnum):
    SHADOW = "shadow"
    MONITOR = "monitor"
    PAPER = "paper"


class StrategyLifecycleState(StrEnum):
    IDLE = "idle"
    WATCHING = "watching"
    ARMED = "armed"
    HOLDING = "holding"
    TERMINAL = "terminal"


class StateTransition(RuntimeContractModel):
    from_state: StrategyLifecycleState
    event: str = Field(min_length=1)
    to_state: StrategyLifecycleState


def _freeze_parameter(value: object) -> object:
    if isinstance(value, Mapping):
        if set(value) == {"$decimal"}:
            return Decimal(str(value["$decimal"]))
        if set(value) == {"$datetime"}:
            parsed = datetime.fromisoformat(str(value["$datetime"]))
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError("tagged strategy datetime must be timezone-aware")
            return parsed.astimezone(UTC)
        if set(value) == {"$date"}:
            return date.fromisoformat(str(value["$date"]))
        if any(not isinstance(key, str) for key in value):
            raise TypeError("strategy parameter mappings require string keys")
        return MappingProxyType(
            {
                key: _freeze_parameter(item)
                for key, item in sorted(value.items(), key=lambda pair: pair[0])
            }
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(_freeze_parameter(item) for item in value)
    return value


def _serialize_parameter(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _serialize_parameter(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_serialize_parameter(item) for item in value]
    if isinstance(value, Decimal):
        return {"$decimal": format(value.normalize(), "f")}
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("strategy datetime parameters must be timezone-aware")
        return {"$datetime": value.astimezone(UTC).isoformat(timespec="microseconds")}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, Enum):
        return value.value
    return value


def _requirement_payload(requirement: FeatureRequirement) -> dict[str, object]:
    return requirement.model_dump(mode="python")


def _transition_payload(transition: StateTransition) -> dict[str, object]:
    return transition.model_dump(mode="python")


class StrategySpec(RuntimeContractModel):
    strategy_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    feature_contract_id: str = Field(min_length=1)
    min_feature_contract_version: int = Field(ge=1)
    required_features: tuple[FeatureRequirement, ...]
    optional_features: tuple[FeatureRequirement, ...]
    initial_state: StrategyLifecycleState
    transitions: tuple[StateTransition, ...] = Field(min_length=1)
    parameters: Mapping[str, object]
    allowed_actions: tuple[str, ...]
    run_mode: StrategyRunMode
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")

    @field_validator("parameters")
    @classmethod
    def validate_and_freeze_parameters(
        cls,
        value: Mapping[str, object],
    ) -> Mapping[str, object]:
        frozen = _freeze_parameter(value)
        if not isinstance(frozen, Mapping):
            raise TypeError("parameters must be a mapping")
        canonical_sha256(frozen)
        return frozen

    @field_serializer("parameters")
    def serialize_parameters(self, value: Mapping[str, object]) -> dict[str, object]:
        serialized = _serialize_parameter(value)
        if not isinstance(serialized, dict):
            raise TypeError("parameters must serialize as a mapping")
        return serialized

    @field_validator("allowed_actions")
    @classmethod
    def validate_allowed_actions(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("allowed_actions cannot contain empty values")
        if len(values) != len(set(values)):
            raise ValueError("allowed_actions must be unique")
        return values

    @model_validator(mode="after")
    def validate_feature_requirements(self) -> StrategySpec:
        required_names = tuple(item.name for item in self.required_features)
        optional_names = tuple(item.name for item in self.optional_features)
        if any(item.level is not RequirementLevel.REQUIRED for item in self.required_features):
            raise ValueError("required_features must use the required level")
        if any(item.level is not RequirementLevel.OPTIONAL for item in self.optional_features):
            raise ValueError("optional_features must use the optional level")
        if len(required_names) != len(set(required_names)):
            raise ValueError("required_features names must be unique")
        if len(optional_names) != len(set(optional_names)):
            raise ValueError("optional_features names must be unique")
        if set(required_names) & set(optional_names):
            raise ValueError("required and optional feature names must be disjoint")
        return self

    @model_validator(mode="after")
    def validate_transition_reachability(self) -> StrategySpec:
        transition_keys = tuple(
            (transition.from_state, transition.event) for transition in self.transitions
        )
        if len(transition_keys) != len(set(transition_keys)):
            raise ValueError("each state and event pair must identify one transition")
        reachable = {self.initial_state}
        changed = True
        while changed:
            changed = False
            for transition in self.transitions:
                if transition.from_state in reachable and transition.to_state not in reachable:
                    reachable.add(transition.to_state)
                    changed = True
        referenced = {
            state
            for transition in self.transitions
            for state in (transition.from_state, transition.to_state)
        }
        unreachable = referenced - reachable
        if unreachable:
            names = ", ".join(sorted(state.value for state in unreachable))
            raise ValueError(f"transition states must be reachable from initial_state: {names}")
        return self

    @property
    def parameter_fingerprint(self) -> str:
        return canonical_sha256(self.parameters)

    @property
    def spec_fingerprint(self) -> str:
        payload = {
            "strategy_id": self.strategy_id,
            "version": self.version,
            "feature_contract_id": self.feature_contract_id,
            "min_feature_contract_version": self.min_feature_contract_version,
            "required_features": tuple(
                _requirement_payload(item)
                for item in sorted(self.required_features, key=lambda item: item.name)
            ),
            "optional_features": tuple(
                _requirement_payload(item)
                for item in sorted(self.optional_features, key=lambda item: item.name)
            ),
            "initial_state": self.initial_state,
            "transitions": tuple(
                _transition_payload(item)
                for item in sorted(
                    self.transitions,
                    key=lambda item: (item.from_state.value, item.event, item.to_state.value),
                )
            ),
            "parameters": self.parameters,
            "allowed_actions": tuple(sorted(self.allowed_actions)),
            "run_mode": self.run_mode,
            "producer_commit": self.producer_commit,
        }
        return canonical_sha256(payload)
