"""Pure allow-listed evaluators for the built-in intraday strategies."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from numbers import Integral, Real
from types import MappingProxyType

from rquant.definition_registry import (
    StrategyExitEligibility,
    StrategyExitPriceBasis,
    StrategyExitRule,
    StrategyPercentStop,
    StrategySellTranche,
    StrategyStructureStop,
    StrategyTrailingTakeProfit,
    TrustedExecutableRegistry,
    TrustedFeatureImplementation,
    TrustedStrategyImplementation,
    _callable_identity,
    _strategy_executable_fingerprint,
)
from rquant.feature_contracts import FeatureRequirement, RequirementLevel
from rquant.intraday_feature_engine import live_compute
from rquant.runtime_builder_strategy import StrategyEvaluatorBinding
from rquant.runtime_contracts import canonical_sha256
from rquant.signal_contracts import SignalAction
from rquant.strategy_candidate_snapshot import (
    StrategyCandidateStaticFeatureSemantic,
    strategy_candidate_schema_fingerprint,
)
from rquant.strategy_runner import (
    StrategyCandidateState,
    StrategyDecision,
    StrategyEvaluator,
)
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_FEATURE_CONTRACT_ID = "intraday-pit"
_FEATURE_CONTRACT_VERSION = 3
_CONTRACT_SCHEMA_VERSION = 1
_EVALUATOR_SEMANTIC_VERSION = "1.0.0"
_ENTRY_STATES = frozenset((StrategyLifecycleState.IDLE, StrategyLifecycleState.WATCHING))
_LIFECYCLE_FEATURES = (
    "entry_fill_status",
    "exit_execution_status",
    "position_closed",
    "holding_trading_sessions",
    "position_sellable",
    "entry_price_raw",
    "structure_stop_price_raw",
    "eligible_high_price_raw",
    "remaining_position_fraction",
)
_STOP_LOSS_BPS = 300
_TRAILING_ACTIVATION_BPS = 800
_TRAILING_RETRACEMENT_BPS = 300


def project_execution_lifecycle_features(
    source: Mapping[str, object],
) -> Mapping[str, object]:
    """Project only authoritative execution state into strategy lifecycle features."""
    if not isinstance(source, Mapping):
        raise TypeError("execution lifecycle source must be a mapping")
    unknown = set(source).difference(_LIFECYCLE_FEATURES)
    if unknown:
        raise ValueError("unsupported execution lifecycle field: " + ", ".join(sorted(unknown)))

    projected: dict[str, object] = {}
    if "entry_fill_status" in source:
        status = source["entry_fill_status"]
        if status not in {"pending", "filled", "rejected"}:
            raise ValueError("entry_fill_status must be pending, filled, or rejected")
        projected["entry_fill_status"] = status
    if "exit_execution_status" in source:
        status = source["exit_execution_status"]
        if status not in {"none", "pending", "retryable", "filled"}:
            raise ValueError("exit_execution_status must be none, pending, retryable, or filled")
        projected["exit_execution_status"] = status
    if "position_closed" in source:
        value = source["position_closed"]
        if type(value) is not bool:
            raise TypeError("position_closed must be a bool")
        projected["position_closed"] = value
    if "holding_trading_sessions" in source:
        value = source["holding_trading_sessions"]
        if isinstance(value, bool) or not isinstance(value, Integral) or value < 0:
            raise ValueError("holding_trading_sessions must be a nonnegative integer")
        projected["holding_trading_sessions"] = int(value)
    if "position_sellable" in source:
        value = source["position_sellable"]
        if type(value) is not bool:
            raise TypeError("position_sellable must be a bool")
        projected["position_sellable"] = value

    for name in (
        "entry_price_raw",
        "structure_stop_price_raw",
        "eligible_high_price_raw",
        "remaining_position_fraction",
    ):
        if name not in source:
            continue
        value = source[name]
        if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
            raise TypeError(f"{name} must be a number")
        normalized = float(value)
        if not math.isfinite(normalized) or (
            normalized <= 0 and name != "remaining_position_fraction"
        ):
            raise ValueError(f"{name} must be finite and positive")
        if name == "remaining_position_fraction" and normalized < 0:
            raise ValueError("remaining_position_fraction cannot be negative")
        if name == "remaining_position_fraction" and normalized > 1.0:
            raise ValueError("remaining_position_fraction cannot exceed one")
        projected[name] = normalized
    return MappingProxyType(dict(sorted(projected.items())))


@dataclass(frozen=True)
class StaticFeatureSemantic:
    """One immutable static feature type and point-in-time meaning."""

    dtype: str
    semantic: str

    def __post_init__(self) -> None:
        if not isinstance(self.semantic, str) or not self.semantic.strip():
            raise ValueError("static feature semantic cannot be empty")
        canonical = StrategyCandidateStaticFeatureSemantic(
            dtype=self.dtype,
            semantic=self.semantic,
        )
        object.__setattr__(self, "dtype", canonical.dtype)
        object.__setattr__(self, "semantic", canonical.semantic)

    def contract_payload(self) -> dict[str, str]:
        return {"dtype": self.dtype, "semantic": self.semantic}


@dataclass(frozen=True)
class BuiltinStrategyDefinition:
    """A frozen strategy spec, evaluator, and independently versioned contract."""

    contract_schema_version: int
    strategy_id: str
    strategy_version: int
    evaluator_semantic_version: str
    spec: StrategySpec
    static_feature_schema: Mapping[str, StaticFeatureSemantic]
    allowed_actions: tuple[SignalAction, ...]
    producer_commit: str
    entry_evaluator: StrategyEvaluator
    exit_evaluator: StrategyEvaluator
    exit_rules: tuple[StrategyExitRule, ...]
    evaluator: StrategyEvaluator

    def __post_init__(self) -> None:
        if (
            not isinstance(self.contract_schema_version, int)
            or isinstance(self.contract_schema_version, bool)
            or self.contract_schema_version < 1
        ):
            raise ValueError("contract_schema_version must be positive")
        if not isinstance(self.strategy_id, str) or not self.strategy_id.strip():
            raise ValueError("strategy_id cannot be empty")
        if (
            not isinstance(self.strategy_version, int)
            or isinstance(self.strategy_version, bool)
            or self.strategy_version < 1
        ):
            raise ValueError("strategy_version must be positive")
        if (
            not isinstance(self.evaluator_semantic_version, str)
            or not self.evaluator_semantic_version.strip()
        ):
            raise ValueError("evaluator_semantic_version cannot be empty")
        if _COMMIT_PATTERN.fullmatch(self.producer_commit) is None:
            raise ValueError("producer_commit must be a 40-character lowercase SHA")
        if not isinstance(self.spec, StrategySpec):
            raise TypeError("spec must be a StrategySpec")
        if self.strategy_id != self.spec.strategy_id:
            raise ValueError("strategy_id must exactly match spec.strategy_id")
        if self.strategy_version != self.spec.version:
            raise ValueError("strategy_version must exactly match spec.version")
        if self.producer_commit != self.spec.producer_commit:
            raise ValueError("producer_commit must exactly match spec.producer_commit")
        if not isinstance(self.allowed_actions, tuple) or any(
            not isinstance(action, SignalAction) for action in self.allowed_actions
        ):
            raise TypeError("allowed_actions must be a tuple of SignalAction values")
        if len(self.allowed_actions) != len(set(self.allowed_actions)):
            raise ValueError("allowed_actions must be unique")
        if tuple(action.value for action in self.allowed_actions) != self.spec.allowed_actions:
            raise ValueError("allowed_actions must exactly match spec.allowed_actions")
        if not all(
            callable(evaluator)
            for evaluator in (
                self.entry_evaluator,
                self.exit_evaluator,
                self.evaluator,
            )
        ):
            raise TypeError("strategy evaluators must be callable")
        if not self.exit_rules:
            raise ValueError("exit_rules cannot be empty")
        exit_evaluator_id = _callable_identity(self.exit_evaluator)
        if any(rule.evaluator_id != exit_evaluator_id for rule in self.exit_rules):
            raise ValueError("exit rules must bind the built-in exit evaluator")
        frozen_schema = MappingProxyType(
            {name: semantic for name, semantic in sorted(self.static_feature_schema.items())}
        )
        if not frozen_schema:
            raise ValueError("static feature schema cannot be empty")
        if any(not isinstance(name, str) or not name for name in frozen_schema):
            raise ValueError("static feature names cannot be empty")
        if any(
            not isinstance(semantic, StaticFeatureSemantic) for semantic in frozen_schema.values()
        ):
            raise TypeError("static feature schema values must be StaticFeatureSemantic")
        declared_features = {
            requirement.name
            for requirement in self.spec.required_features + self.spec.optional_features
        }
        if not set(frozen_schema).issubset(declared_features):
            raise ValueError("static feature schema names must be declared by the spec")
        object.__setattr__(self, "static_feature_schema", frozen_schema)

    def contract_payload(self) -> dict[str, object]:
        return {
            "contract_schema_version": self.contract_schema_version,
            "identity": {
                "strategy_id": self.strategy_id,
                "strategy_version": self.strategy_version,
            },
            "evaluator_semantic_version": self.evaluator_semantic_version,
            "spec_fingerprint": self.spec.spec_fingerprint,
            "static_feature_schema": {
                name: semantic.contract_payload()
                for name, semantic in sorted(self.static_feature_schema.items())
            },
            "allowed_actions": tuple(sorted(action.value for action in self.allowed_actions)),
            "producer_commit": self.producer_commit,
            "executable_fingerprint": self.executable_fingerprint,
        }

    @property
    def contract_fingerprint(self) -> str:
        return canonical_sha256(self.contract_payload())

    @property
    def candidate_schema_fingerprint(self) -> str:
        return strategy_candidate_schema_fingerprint(
            strategy_id=self.strategy_id,
            strategy_version=str(self.strategy_version),
            static_feature_schema={
                name: semantic.contract_payload()
                for name, semantic in sorted(self.static_feature_schema.items())
            },
        )

    @property
    def executable_fingerprint(self) -> str:
        registry = TrustedExecutableRegistry(
            features=(),
            strategies=(
                TrustedStrategyImplementation(
                    strategy_id=self.strategy_id,
                    implementation_version=self.evaluator_semantic_version,
                    candidate_schema_fingerprint=self.candidate_schema_fingerprint,
                    entry_evaluator=self.entry_evaluator,
                    exit_evaluator=self.exit_evaluator,
                    runtime_evaluator=self.evaluator,
                    entry_event="entry_filled",
                    exit_rules=self.exit_rules,
                ),
            ),
        )
        return _strategy_executable_fingerprint(
            self.spec,
            registry.strategy_binding(self.spec),
        )


class BuiltinStrategyEvaluatorRegistry:
    """Immutable process-local allow-list for built-in pure evaluators."""

    __slots__ = ("_definitions",)

    def __setattr__(self, name: str, value: object) -> None:
        if name == "_definitions" and hasattr(self, name):
            raise AttributeError("built-in strategy registry is immutable")
        object.__setattr__(self, name, value)

    def __init__(self, *, producer_commit: str) -> None:
        if not isinstance(producer_commit, str):
            raise TypeError("producer_commit must be a string")
        if _COMMIT_PATTERN.fullmatch(producer_commit) is None:
            raise ValueError("producer_commit must be a 40-character lowercase SHA")
        definitions = tuple(
            builder(producer_commit)
            for builder in (
                _build_n_shape_definition,
                _build_growth_board_surge_definition,
                _build_auction_gap_definition,
            )
        )
        by_identity = {
            (definition.strategy_id, definition.strategy_version): definition
            for definition in definitions
        }
        if len(by_identity) != len(definitions):
            raise RuntimeError("built-in strategy identities must be unique")
        self._definitions = MappingProxyType(dict(sorted(by_identity.items())))

    @property
    def identities(self) -> tuple[tuple[str, int], ...]:
        return tuple(self._definitions)

    @property
    def definitions(
        self,
    ) -> Mapping[tuple[str, int], BuiltinStrategyDefinition]:
        return self._definitions

    def load_definition(
        self,
        strategy_id: str,
        strategy_version: int,
    ) -> BuiltinStrategyDefinition:
        try:
            return self._definitions[(strategy_id, strategy_version)]
        except (KeyError, TypeError) as error:
            raise KeyError(
                f"unknown built-in strategy: {strategy_id!r} v{strategy_version!r}"
            ) from error

    def load_spec(self, strategy_id: str, strategy_version: int) -> StrategySpec:
        return self.load_definition(strategy_id, strategy_version).spec

    def load_binding(
        self,
        strategy_id: str,
        strategy_version: int,
    ) -> StrategyEvaluatorBinding:
        definition = self.load_definition(strategy_id, strategy_version)
        return StrategyEvaluatorBinding(
            strategy_id=definition.strategy_id,
            strategy_version=definition.strategy_version,
            contract_fingerprint=definition.executable_fingerprint,
            evaluator=definition.evaluator,
        )

    def trusted_executable_registry(self) -> TrustedExecutableRegistry:
        feature_names = sorted(
            {
                requirement.name
                for definition in self._definitions.values()
                for requirement in (
                    *definition.spec.required_features,
                    *definition.spec.optional_features,
                )
            }
        )
        return TrustedExecutableRegistry(
            features=tuple(
                TrustedFeatureImplementation(
                    feature_name=name,
                    implementation_version=_EVALUATOR_SEMANTIC_VERSION,
                    evaluator=(
                        project_execution_lifecycle_features
                        if name in _LIFECYCLE_FEATURES
                        else live_compute
                    ),
                )
                for name in feature_names
            ),
            strategies=tuple(
                TrustedStrategyImplementation(
                    strategy_id=definition.strategy_id,
                    implementation_version=definition.evaluator_semantic_version,
                    candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
                    entry_evaluator=definition.entry_evaluator,
                    exit_evaluator=definition.exit_evaluator,
                    runtime_evaluator=definition.evaluator,
                    entry_event="entry_filled",
                    exit_rules=definition.exit_rules,
                )
                for definition in self._definitions.values()
            ),
        )


def _requirement(name: str, level: RequirementLevel) -> FeatureRequirement:
    return FeatureRequirement(
        name=name,
        level=level,
        min_contract_version=_FEATURE_CONTRACT_VERSION,
        allow_degraded=False,
    )


def _requirements(
    names: tuple[str, ...],
    level: RequirementLevel,
) -> tuple[FeatureRequirement, ...]:
    return tuple(_requirement(name, level) for name in names)


def _spec(
    *,
    strategy_id: str,
    producer_commit: str,
    required: tuple[str, ...],
    optional: tuple[str, ...],
    transitions: tuple[StateTransition, ...],
    parameters: Mapping[str, object],
    allowed_actions: tuple[SignalAction, ...],
) -> StrategySpec:
    return StrategySpec(
        strategy_id=strategy_id,
        version=1,
        feature_contract_id=_FEATURE_CONTRACT_ID,
        min_feature_contract_version=_FEATURE_CONTRACT_VERSION,
        required_features=_requirements(required, RequirementLevel.REQUIRED),
        optional_features=_requirements(optional, RequirementLevel.OPTIONAL),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=transitions,
        parameters=parameters,
        allowed_actions=tuple(action.value for action in allowed_actions),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit=producer_commit,
    )


def _definition(
    *,
    spec: StrategySpec,
    static_feature_schema: Mapping[str, StaticFeatureSemantic],
    allowed_actions: tuple[SignalAction, ...],
    entry_evaluator: StrategyEvaluator,
    evaluator: StrategyEvaluator,
) -> BuiltinStrategyDefinition:
    exit_rules = _builtin_exit_rules()
    return BuiltinStrategyDefinition(
        contract_schema_version=_CONTRACT_SCHEMA_VERSION,
        strategy_id=spec.strategy_id,
        strategy_version=spec.version,
        evaluator_semantic_version=_EVALUATOR_SEMANTIC_VERSION,
        spec=spec,
        static_feature_schema=static_feature_schema,
        allowed_actions=allowed_actions,
        producer_commit=spec.producer_commit,
        entry_evaluator=entry_evaluator,
        exit_evaluator=_position_exit_evaluator,
        exit_rules=exit_rules,
        evaluator=evaluator,
    )


def _builtin_exit_rules() -> tuple[StrategyExitRule, ...]:
    evaluator_id = _callable_identity(_position_exit_evaluator)
    common = {
        "evaluator_id": evaluator_id,
        "eligibility": StrategyExitEligibility(
            settlement_rule="a_share_t_plus_one",
            minimum_holding_trading_sessions=1,
            same_day_sell_allowed=False,
            sellable_position_required=True,
        ),
        "price_basis": StrategyExitPriceBasis(
            adjustment_basis="raw",
            decision_price="minute_close",
            execution_price="next_minute_open",
        ),
        "structure_stop": StrategyStructureStop(
            reference="signal_support",
            buffer_bps=0,
        ),
        "percent_stop": StrategyPercentStop(
            maximum_loss_bps=_STOP_LOSS_BPS,
            acts_as_fallback=True,
        ),
        "trailing_take_profit": StrategyTrailingTakeProfit(
            activation_gain_bps=_TRAILING_ACTIVATION_BPS,
            retracement_bps=_TRAILING_RETRACEMENT_BPS,
            high_watermark="eligible_intraday_high",
        ),
    }
    return (
        StrategyExitRule(
            event="take_profit_partial",
            action=SignalAction.REDUCE.value,
            sell_tranche=StrategySellTranche(
                sequence=1,
                position_fraction=0.5,
                reevaluate_after_fill=True,
                terminal_after_fill=False,
            ),
            **common,
        ),
        StrategyExitRule(
            event="exit",
            fill_event="exit_filled",
            action=SignalAction.S_INTENT.value,
            sell_tranche=StrategySellTranche(
                sequence=2,
                position_fraction=1.0,
                reevaluate_after_fill=False,
                terminal_after_fill=True,
            ),
            **common,
        ),
    )


def _ensure_entry_state(state: StrategyCandidateState) -> bool:
    if not isinstance(state, StrategyCandidateState):
        raise TypeError("state must be a StrategyCandidateState")
    return state.state in _ENTRY_STATES


def _ensure_features(features: Mapping[str, object]) -> None:
    if not isinstance(features, Mapping):
        raise TypeError("features must be a mapping")


def _required_value(features: Mapping[str, object], name: str) -> object:
    if name not in features or features[name] is None:
        raise ValueError(f"required feature {name} is missing or None")
    return features[name]


def _number(features: Mapping[str, object], name: str) -> float:
    value = _required_value(features, name)
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        raise TypeError(f"feature {name} must be a number, not {type(value).__name__}")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"feature {name} must be finite")
    return normalized


def _optional_number(features: Mapping[str, object], name: str) -> float | None:
    if name not in features or features[name] is None:
        return None
    return _number(features, name)


def _integer(features: Mapping[str, object], name: str) -> int:
    value = _required_value(features, name)
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise TypeError(f"feature {name} must be an integer, not {type(value).__name__}")
    return int(value)


def _optional_integer(features: Mapping[str, object], name: str) -> int | None:
    if name not in features or features[name] is None:
        return None
    return _integer(features, name)


def _require_positive(value: float, name: str) -> None:
    if value <= 0.0:
        raise ValueError(f"feature {name} must be positive")


def _require_nonnegative(value: float | int | None, name: str) -> None:
    if value is not None and value < 0:
        raise ValueError(f"feature {name} must be nonnegative")


def _validate_session_geometry(
    *,
    session_low: float,
    latest_close: float,
    session_high: float,
) -> None:
    if not session_low <= latest_close <= session_high:
        raise ValueError("session price geometry requires low <= latest_close <= high")


def _text(features: Mapping[str, object], name: str) -> str:
    value = _required_value(features, name)
    if not isinstance(value, str):
        raise TypeError(f"feature {name} must be a string, not {type(value).__name__}")
    if not value:
        raise ValueError(f"feature {name} cannot be empty")
    return value


def _boolean(features: Mapping[str, object], name: str) -> bool:
    value = _required_value(features, name)
    if type(value) is not bool:
        raise TypeError(f"feature {name} must be a bool, not {type(value).__name__}")
    return value


def _raw_session_basis(features: Mapping[str, object]) -> str:
    basis = _text(features, "candidate_price_basis")
    if basis != "raw_session":
        raise ValueError("feature candidate_price_basis must equal raw_session")
    return basis


def _signal(
    *,
    event: str,
    from_state: StrategyLifecycleState,
    to_state: StrategyLifecycleState,
    action: SignalAction,
    reason_codes: tuple[str, ...],
    evidence: Mapping[str, object],
    expires_seconds: int,
) -> StrategyDecision:
    return StrategyDecision(
        event=event,
        expected_from_state=from_state,
        expected_to_state=to_state,
        expected_action=action,
        action=action,
        reason_codes=reason_codes,
        evidence=evidence,  # type: ignore[arg-type]
        expires_after=timedelta(seconds=expires_seconds),
    )


def _transition_only(
    *,
    event: str,
    from_state: StrategyLifecycleState,
    to_state: StrategyLifecycleState,
) -> StrategyDecision:
    return StrategyDecision(
        event=event,
        expected_from_state=from_state,
        expected_to_state=to_state,
        expected_action=None,
        action=None,
    )


def _entry_fill_transition(
    state: StrategyCandidateState,
    features: Mapping[str, object],
) -> StrategyDecision | None:
    if state.state is not StrategyLifecycleState.ARMED:
        return None
    _ensure_features(features)
    raw_status = features.get("entry_fill_status")
    if raw_status is None or raw_status == "pending":
        return None
    if not isinstance(raw_status, str):
        raise TypeError("feature entry_fill_status must be a string")
    if raw_status == "filled":
        return _transition_only(
            event="entry_filled",
            from_state=StrategyLifecycleState.ARMED,
            to_state=StrategyLifecycleState.HOLDING,
        )
    if raw_status == "rejected":
        return _transition_only(
            event="entry_rejected",
            from_state=StrategyLifecycleState.ARMED,
            to_state=StrategyLifecycleState.TERMINAL,
        )
    raise ValueError("feature entry_fill_status must be pending, filled, or rejected")


def _position_exit_evaluator(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: Mapping[str, object],
) -> StrategyDecision | None:
    _ensure_strategy_identity(spec, spec.strategy_id)
    if state.state is not StrategyLifecycleState.HOLDING:
        return None
    _ensure_features(features)
    holding_sessions = _integer(features, "holding_trading_sessions")
    sellable = _boolean(features, "position_sellable")
    latest_close = _number(features, "latest_close")
    entry_price = _number(features, "entry_price_raw")
    structure_stop = _number(features, "structure_stop_price_raw")
    eligible_high = _number(features, "eligible_high_price_raw")
    remaining_fraction = _number(features, "remaining_position_fraction")
    exit_execution_status = _text(features, "exit_execution_status")
    position_closed = _boolean(features, "position_closed")
    for name, value in (
        ("latest_close", latest_close),
        ("entry_price_raw", entry_price),
        ("structure_stop_price_raw", structure_stop),
        ("eligible_high_price_raw", eligible_high),
    ):
        _require_positive(value, name)
    if holding_sessions < 0:
        raise ValueError("feature holding_trading_sessions must be nonnegative")
    if remaining_fraction > 1.0:
        raise ValueError("feature remaining_position_fraction cannot exceed one")
    if exit_execution_status not in {"none", "pending", "retryable", "filled"}:
        raise ValueError(
            "feature exit_execution_status must be none, pending, retryable, or filled"
        )
    if position_closed:
        if remaining_fraction != 0.0 or exit_execution_status != "filled":
            raise ValueError("position_closed requires zero remaining position and a verified fill")
        return _transition_only(
            event="exit_filled",
            from_state=StrategyLifecycleState.HOLDING,
            to_state=StrategyLifecycleState.TERMINAL,
        )
    if remaining_fraction <= 0.0:
        raise ValueError("open position requires positive remaining_position_fraction")
    if exit_execution_status == "pending":
        return None
    if eligible_high < latest_close:
        raise ValueError("eligible_high_price_raw cannot be below latest_close")
    if holding_sessions < 1 or not sellable:
        return None

    fallback_stop = entry_price * (1.0 - _STOP_LOSS_BPS / 10_000.0)
    effective_stop = max(structure_stop, fallback_stop)
    evidence = {
        "latest_close": latest_close,
        "entry_price_raw": entry_price,
        "structure_stop_price_raw": structure_stop,
        "effective_stop_price_raw": effective_stop,
        "eligible_high_price_raw": eligible_high,
        "holding_trading_sessions": holding_sessions,
        "position_sellable": sellable,
        "remaining_position_fraction": remaining_fraction,
        "exit_execution_status": exit_execution_status,
        "position_closed": position_closed,
    }
    expires_seconds = int(spec.parameters["expires_seconds"])
    if latest_close <= effective_stop:
        return _signal(
            event="exit",
            from_state=StrategyLifecycleState.HOLDING,
            to_state=StrategyLifecycleState.HOLDING,
            action=SignalAction.S_INTENT,
            reason_codes=("t_plus_one_stop",),
            evidence={**evidence, "sell_tranche_fraction": 1.0},
            expires_seconds=expires_seconds,
        )
    trailing_activated = eligible_high >= entry_price * (1.0 + _TRAILING_ACTIVATION_BPS / 10_000.0)
    trailing_hit = latest_close <= eligible_high * (1.0 - _TRAILING_RETRACEMENT_BPS / 10_000.0)
    if trailing_activated and trailing_hit:
        if remaining_fraction > 0.5:
            return _signal(
                event="take_profit_partial",
                from_state=StrategyLifecycleState.HOLDING,
                to_state=StrategyLifecycleState.HOLDING,
                action=SignalAction.REDUCE,
                reason_codes=("t_plus_one_trailing_take_profit",),
                evidence={**evidence, "sell_tranche_fraction": 0.5},
                expires_seconds=expires_seconds,
            )
        return _signal(
            event="exit",
            from_state=StrategyLifecycleState.HOLDING,
            to_state=StrategyLifecycleState.HOLDING,
            action=SignalAction.S_INTENT,
            reason_codes=("t_plus_one_trailing_exit",),
            evidence={**evidence, "sell_tranche_fraction": 1.0},
            expires_seconds=expires_seconds,
        )
    return None


def _ensure_strategy_identity(spec: StrategySpec, strategy_id: str) -> None:
    if not isinstance(spec, StrategySpec):
        raise TypeError("spec must be a StrategySpec")
    if spec.strategy_id != strategy_id or spec.version != 1:
        raise ValueError("strategy spec fingerprint does not match evaluator binding")


def _n_shape_evaluator(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: Mapping[str, object],
) -> StrategyDecision | None:
    _ensure_strategy_identity(spec, "n_shape")
    if state.state is StrategyLifecycleState.ARMED:
        return _entry_fill_transition(state, features)
    if not _ensure_entry_state(state):
        return None
    _ensure_features(features)
    latest_close = _number(features, "latest_close")
    session_low = _number(features, "session_low")
    session_high = _number(features, "session_high")
    price_over_vwap = _number(features, "price_over_vwap")
    t_close = _number(features, "t_close_session_raw")
    t_high = _number(features, "t_high_session_raw")
    limit_up = _number(features, "limit_up_price_session_raw")
    limit_pct = _number(features, "limit_pct")
    basis = _raw_session_basis(features)
    optional = {
        name: _optional_number(features, name)
        for name in (
            "rel_same_minute",
            "rel_cumulative",
            "amount_accel_5m",
            "amount_accel_10m",
            "tick_rule_buy_sell_ratio_proxy",
        )
    }
    optional["historical_sessions"] = _optional_integer(features, "historical_sessions")

    for name, value in (
        ("latest_close", latest_close),
        ("session_low", session_low),
        ("session_high", session_high),
        ("t_close_session_raw", t_close),
        ("t_high_session_raw", t_high),
        ("limit_up_price_session_raw", limit_up),
    ):
        _require_positive(value, name)
    _validate_session_geometry(
        session_low=session_low,
        latest_close=latest_close,
        session_high=session_high,
    )
    if t_high < t_close:
        raise ValueError("t_high_session_raw must be >= t_close_session_raw")
    _require_positive(price_over_vwap, "price_over_vwap")
    _require_positive(limit_pct, "limit_pct")
    if latest_close > limit_up:
        raise ValueError("latest_close cannot exceed limit_up_price_session_raw")
    if session_high > limit_up:
        raise ValueError("session_high cannot exceed limit_up_price_session_raw")
    for name, value in optional.items():
        _require_nonnegative(value, name)

    parameters = spec.parameters
    carry_low_ratio = float(parameters["carry_low_ratio"])
    break_high_ratio = float(parameters["break_high_ratio"])
    min_price_over_vwap = float(parameters["min_price_over_vwap"])
    tolerance = float(parameters["price_tolerance_ratio"])
    expires_seconds = int(parameters["expires_seconds"])
    carrying = session_low >= t_close * carry_low_ratio * (1.0 - tolerance)
    vwap_strong = price_over_vwap >= min_price_over_vwap
    breakout = latest_close >= t_high * break_high_ratio * (1.0 - tolerance)
    below_limit = latest_close < limit_up * (1.0 - tolerance)

    if state.state is StrategyLifecycleState.WATCHING and not carrying:
        return _transition_only(
            event="support_broken",
            from_state=state.state,
            to_state=StrategyLifecycleState.TERMINAL,
        )
    evidence = {
        "latest_close": latest_close,
        "session_low": session_low,
        "session_high": session_high,
        "t_close_session_raw": t_close,
        "t_high_session_raw": t_high,
        "limit_up_price_session_raw": limit_up,
        "limit_pct": limit_pct,
        "price_over_vwap": price_over_vwap,
        "candidate_price_basis": basis,
        **{name: value for name, value in optional.items() if value is not None},
    }
    if carrying and breakout and vwap_strong and below_limit:
        return _signal(
            event="entry_ready",
            from_state=state.state,
            to_state=StrategyLifecycleState.ARMED,
            action=SignalAction.B_INTENT,
            reason_codes=("n_shape_breakout", "structure_supported", "vwap_supported"),
            evidence=evidence,
            expires_seconds=expires_seconds,
        )
    if state.state is StrategyLifecycleState.IDLE and carrying and not breakout:
        return _signal(
            event="structure_supported",
            from_state=state.state,
            to_state=StrategyLifecycleState.WATCHING,
            action=SignalAction.WATCH,
            reason_codes=("structure_supported",),
            evidence=evidence,
            expires_seconds=expires_seconds,
        )
    return None


def _growth_board_surge_evaluator(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: Mapping[str, object],
) -> StrategyDecision | None:
    _ensure_strategy_identity(spec, "growth_board_surge")
    if state.state is StrategyLifecycleState.ARMED:
        return _entry_fill_transition(state, features)
    if not _ensure_entry_state(state):
        return None
    _ensure_features(features)
    latest_close = _number(features, "latest_close")
    opening_high = _number(features, "opening_bar_high")
    opening_low = _number(features, "opening_bar_low")
    rel_cumulative = _number(features, "rel_cumulative")
    price_over_vwap = _number(features, "price_over_vwap")
    historical_sessions = _integer(features, "historical_sessions")
    session_pre_close = _number(features, "session_pre_close_raw")
    limit_up = _number(features, "limit_up_price_session_raw")
    board_type = _text(features, "board_type")
    ma_alignment = _boolean(features, "ma_alignment")
    large_net_vol_t1 = _number(features, "large_net_vol_t1")
    basis = _raw_session_basis(features)
    optional = {
        name: _optional_number(features, name)
        for name in (
            "rel_same_minute",
            "amount_accel_5m",
            "amount_accel_10m",
            "tick_rule_buy_sell_ratio_proxy",
            "minute_volume",
            "cumulative_volume",
        )
    }

    for name, value in (
        ("latest_close", latest_close),
        ("opening_bar_high", opening_high),
        ("opening_bar_low", opening_low),
        ("session_pre_close_raw", session_pre_close),
        ("limit_up_price_session_raw", limit_up),
    ):
        _require_positive(value, name)
    if opening_low > opening_high:
        raise ValueError("opening price geometry requires low <= high")
    _require_positive(price_over_vwap, "price_over_vwap")
    _require_nonnegative(rel_cumulative, "rel_cumulative")
    _require_nonnegative(historical_sessions, "historical_sessions")
    for name, value in optional.items():
        _require_nonnegative(value, name)
    if limit_up <= session_pre_close:
        raise ValueError("limit_up_price_session_raw must exceed session_pre_close_raw")
    if latest_close > limit_up:
        raise ValueError("latest_close cannot exceed limit_up_price_session_raw")
    if opening_high > limit_up:
        raise ValueError("opening_bar_high cannot exceed limit_up_price_session_raw")

    parameters = spec.parameters
    min_rel_cumulative = float(parameters["min_rel_cumulative"])
    min_rel_same_minute = float(parameters["min_rel_same_minute"])
    min_accel_5m = float(parameters["min_amount_accel_5m"])
    min_price_over_vwap = float(parameters["min_price_over_vwap"])
    tolerance = float(parameters["price_tolerance_ratio"])
    expires_seconds = int(parameters["expires_seconds"])
    allowed_boards = tuple(str(item) for item in parameters["allowed_boards"])
    min_history = int(parameters["min_historical_sessions"])

    static_qualified = (
        board_type in allowed_boards
        and ma_alignment
        and large_net_vol_t1 > 0.0
        and historical_sessions >= min_history
    )
    cumulative_ready = rel_cumulative >= min_rel_cumulative
    same_minute = optional["rel_same_minute"]
    accel_5m = optional["amount_accel_5m"]
    burst_ready = bool(
        (same_minute is not None and same_minute >= min_rel_same_minute)
        or (accel_5m is not None and accel_5m >= min_accel_5m)
    )
    vwap_strong = price_over_vwap >= min_price_over_vwap
    below_limit = latest_close < limit_up * (1.0 - tolerance)
    one_price_limit = opening_high == opening_low and opening_high >= limit_up
    evidence = {
        "latest_close": latest_close,
        "opening_bar_high": opening_high,
        "opening_bar_low": opening_low,
        "rel_cumulative": rel_cumulative,
        "price_over_vwap": price_over_vwap,
        "historical_sessions": historical_sessions,
        "session_pre_close_raw": session_pre_close,
        "limit_up_price_session_raw": limit_up,
        "board_type": board_type,
        "ma_alignment": ma_alignment,
        "large_net_vol_t1": large_net_vol_t1,
        "large_net_vol_semantics": "t_minus_1_daily_proxy",
        "candidate_price_basis": basis,
        "tick_rule_is_proxy": True,
        **{name: value for name, value in optional.items() if value is not None},
    }
    if (
        static_qualified
        and cumulative_ready
        and burst_ready
        and vwap_strong
        and below_limit
        and not one_price_limit
    ):
        return _signal(
            event="entry_ready",
            from_state=state.state,
            to_state=StrategyLifecycleState.ARMED,
            action=SignalAction.B_INTENT,
            reason_codes=("board_surge", "t_minus_1_order_flow_proxy", "volume_burst"),
            evidence=evidence,
            expires_seconds=expires_seconds,
        )
    if (
        state.state is StrategyLifecycleState.IDLE
        and static_qualified
        and cumulative_ready
        and not burst_ready
        and below_limit
        and not one_price_limit
    ):
        return _signal(
            event="volume_building",
            from_state=state.state,
            to_state=StrategyLifecycleState.WATCHING,
            action=SignalAction.WATCH,
            reason_codes=("cumulative_volume_ready",),
            evidence=evidence,
            expires_seconds=expires_seconds,
        )
    return None


def _auction_gap_evaluator(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: Mapping[str, object],
) -> StrategyDecision | None:
    _ensure_strategy_identity(spec, "auction_gap")
    if state.state is StrategyLifecycleState.ARMED:
        return _entry_fill_transition(state, features)
    if not _ensure_entry_state(state):
        return None
    _ensure_features(features)
    latest_close = _number(features, "latest_close")
    session_low = _number(features, "session_low")
    session_high = _number(features, "session_high")
    price_over_vwap = _number(features, "price_over_vwap")
    auction_price = _number(features, "auction_price_raw")
    auction_ratio = _number(features, "auction_vol_ratio_5d")
    gap_pct = _number(features, "gap_pct_close")
    limit_up = _number(features, "limit_up_price_session_raw")
    basis = _raw_session_basis(features)
    optional = {
        name: _optional_number(features, name)
        for name in (
            "rel_same_minute",
            "rel_cumulative",
            "amount_accel_5m",
            "amount_accel_10m",
            "tick_rule_buy_sell_ratio_proxy",
        )
    }

    for name, value in (
        ("latest_close", latest_close),
        ("session_low", session_low),
        ("session_high", session_high),
        ("auction_price_raw", auction_price),
        ("limit_up_price_session_raw", limit_up),
    ):
        _require_positive(value, name)
    _validate_session_geometry(
        session_low=session_low,
        latest_close=latest_close,
        session_high=session_high,
    )
    _require_positive(price_over_vwap, "price_over_vwap")
    _require_nonnegative(auction_ratio, "auction_vol_ratio_5d")
    for name, value in optional.items():
        _require_nonnegative(value, name)
    if latest_close > limit_up:
        raise ValueError("latest_close cannot exceed limit_up_price_session_raw")
    if auction_price > limit_up:
        raise ValueError("auction_price_raw cannot exceed limit_up_price_session_raw")
    if session_high > limit_up:
        raise ValueError("session_high cannot exceed limit_up_price_session_raw")

    parameters = spec.parameters
    ratio_min = float(parameters["auction_ratio_min"])
    ratio_max = float(parameters["auction_ratio_max"])
    min_gap = float(parameters["min_gap_pct"])
    min_price_over_vwap = float(parameters["min_price_over_vwap"])
    min_hold_ratio = float(parameters["min_hold_auction_price_ratio"])
    expires_seconds = int(parameters["expires_seconds"])
    matched = (
        ratio_min <= auction_ratio <= ratio_max
        and gap_pct > min_gap
        and latest_close >= auction_price * min_hold_ratio
        and price_over_vwap >= min_price_over_vwap
        and latest_close < limit_up
    )
    if not matched:
        return None
    if state.state is StrategyLifecycleState.WATCHING:
        return _signal(
            event="entry_ready",
            from_state=state.state,
            to_state=StrategyLifecycleState.ARMED,
            action=SignalAction.B_INTENT,
            reason_codes=("auction_gap_confirmed", "vwap_supported"),
            evidence={
                "latest_close": latest_close,
                "auction_price_raw": auction_price,
                "auction_vol_ratio_5d": auction_ratio,
                "gap_pct_close": gap_pct,
                "candidate_price_basis": basis,
            },
            expires_seconds=expires_seconds,
        )
    return _signal(
        event="observer_match",
        from_state=state.state,
        to_state=StrategyLifecycleState.WATCHING,
        action=SignalAction.WATCH,
        reason_codes=("auction_gap_observer",),
        evidence={
            "latest_close": latest_close,
            "session_low": session_low,
            "session_high": session_high,
            "price_over_vwap": price_over_vwap,
            "auction_price_raw": auction_price,
            "auction_vol_ratio_5d": auction_ratio,
            "gap_pct_close": gap_pct,
            "limit_up_price_session_raw": limit_up,
            "candidate_price_basis": basis,
            "observer_only": True,
            "tick_rule_is_proxy": True,
            **{name: value for name, value in optional.items() if value is not None},
        },
        expires_seconds=expires_seconds,
    )


def _n_shape_runtime_evaluator(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: Mapping[str, object],
) -> StrategyDecision | None:
    if state.state is StrategyLifecycleState.HOLDING:
        _ensure_strategy_identity(spec, "n_shape")
        return _position_exit_evaluator(spec, state, features)
    return _n_shape_evaluator(spec, state, features)


def _growth_board_surge_runtime_evaluator(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: Mapping[str, object],
) -> StrategyDecision | None:
    if state.state is StrategyLifecycleState.HOLDING:
        _ensure_strategy_identity(spec, "growth_board_surge")
        return _position_exit_evaluator(spec, state, features)
    return _growth_board_surge_evaluator(spec, state, features)


def _auction_gap_runtime_evaluator(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: Mapping[str, object],
) -> StrategyDecision | None:
    if state.state is StrategyLifecycleState.HOLDING:
        _ensure_strategy_identity(spec, "auction_gap")
        return _position_exit_evaluator(spec, state, features)
    return _auction_gap_evaluator(spec, state, features)


def _execution_lifecycle_transitions() -> tuple[StateTransition, ...]:
    return (
        StateTransition(
            from_state=StrategyLifecycleState.ARMED,
            event="entry_filled",
            to_state=StrategyLifecycleState.HOLDING,
        ),
        StateTransition(
            from_state=StrategyLifecycleState.ARMED,
            event="entry_rejected",
            to_state=StrategyLifecycleState.TERMINAL,
        ),
        StateTransition(
            from_state=StrategyLifecycleState.HOLDING,
            event="take_profit_partial",
            to_state=StrategyLifecycleState.HOLDING,
        ),
        StateTransition(
            from_state=StrategyLifecycleState.HOLDING,
            event="exit",
            to_state=StrategyLifecycleState.HOLDING,
        ),
        StateTransition(
            from_state=StrategyLifecycleState.HOLDING,
            event="exit_filled",
            to_state=StrategyLifecycleState.TERMINAL,
        ),
    )


def _build_n_shape_definition(producer_commit: str) -> BuiltinStrategyDefinition:
    actions = (
        SignalAction.WATCH,
        SignalAction.B_INTENT,
        SignalAction.REDUCE,
        SignalAction.S_INTENT,
    )
    spec = _spec(
        strategy_id="n_shape",
        producer_commit=producer_commit,
        required=(
            "latest_close",
            "session_low",
            "session_high",
            "price_over_vwap",
            "t_close_session_raw",
            "t_high_session_raw",
            "limit_up_price_session_raw",
            "limit_pct",
            "candidate_price_basis",
        ),
        optional=(
            "rel_same_minute",
            "rel_cumulative",
            "amount_accel_5m",
            "amount_accel_10m",
            "tick_rule_buy_sell_ratio_proxy",
            "historical_sessions",
            *_LIFECYCLE_FEATURES,
        ),
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="structure_supported",
                to_state=StrategyLifecycleState.WATCHING,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.WATCHING,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.WATCHING,
                event="support_broken",
                to_state=StrategyLifecycleState.TERMINAL,
            ),
            *_execution_lifecycle_transitions(),
        ),
        parameters={
            "carry_low_ratio": 1.0,
            "break_high_ratio": 1.0,
            "min_price_over_vwap": 1.0,
            "price_tolerance_ratio": 0.001,
            "expires_seconds": 120,
        },
        allowed_actions=actions,
    )
    return _definition(
        spec=spec,
        static_feature_schema={
            "candidate_price_basis": StaticFeatureSemantic("string", "raw_session_only"),
            "t_close_session_raw": StaticFeatureSemantic(
                "number", "reference_close_rebased_to_current_raw_session"
            ),
            "t_high_session_raw": StaticFeatureSemantic(
                "number", "reference_high_rebased_to_current_raw_session"
            ),
            "limit_up_price_session_raw": StaticFeatureSemantic(
                "number", "current_session_authoritative_limit_up_price"
            ),
            "limit_pct": StaticFeatureSemantic(
                "number", "current_session_authoritative_limit_percentage"
            ),
        },
        allowed_actions=actions,
        entry_evaluator=_n_shape_evaluator,
        evaluator=_n_shape_runtime_evaluator,
    )


def _build_growth_board_surge_definition(
    producer_commit: str,
) -> BuiltinStrategyDefinition:
    actions = (
        SignalAction.WATCH,
        SignalAction.B_INTENT,
        SignalAction.REDUCE,
        SignalAction.S_INTENT,
    )
    spec = _spec(
        strategy_id="growth_board_surge",
        producer_commit=producer_commit,
        required=(
            "latest_close",
            "opening_bar_high",
            "opening_bar_low",
            "rel_cumulative",
            "price_over_vwap",
            "historical_sessions",
            "session_pre_close_raw",
            "limit_up_price_session_raw",
            "board_type",
            "ma_alignment",
            "large_net_vol_t1",
            "candidate_price_basis",
        ),
        optional=(
            "rel_same_minute",
            "amount_accel_5m",
            "amount_accel_10m",
            "tick_rule_buy_sell_ratio_proxy",
            "minute_volume",
            "cumulative_volume",
            *_LIFECYCLE_FEATURES,
        ),
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="volume_building",
                to_state=StrategyLifecycleState.WATCHING,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.WATCHING,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
            *_execution_lifecycle_transitions(),
        ),
        parameters={
            "min_rel_cumulative": 1.4,
            "min_rel_same_minute": 2.0,
            "min_amount_accel_5m": 2.0,
            "min_price_over_vwap": 1.0,
            "price_tolerance_ratio": 0.001,
            "expires_seconds": 120,
            "allowed_boards": ("gem", "star"),
            "min_historical_sessions": 5,
        },
        allowed_actions=actions,
    )
    return _definition(
        spec=spec,
        static_feature_schema={
            "candidate_price_basis": StaticFeatureSemantic("string", "raw_session_only"),
            "session_pre_close_raw": StaticFeatureSemantic(
                "number", "current_session_authoritative_previous_close"
            ),
            "limit_up_price_session_raw": StaticFeatureSemantic(
                "number", "current_session_authoritative_limit_up_price"
            ),
            "board_type": StaticFeatureSemantic("string", "point_in_time_listing_board"),
            "ma_alignment": StaticFeatureSemantic(
                "bool", "prior_session_close_confirmed_moving_average_alignment"
            ),
            "large_net_vol_t1": StaticFeatureSemantic("number", "t_minus_1_daily_proxy"),
        },
        allowed_actions=actions,
        entry_evaluator=_growth_board_surge_evaluator,
        evaluator=_growth_board_surge_runtime_evaluator,
    )


def _build_auction_gap_definition(producer_commit: str) -> BuiltinStrategyDefinition:
    actions = (
        SignalAction.WATCH,
        SignalAction.B_INTENT,
        SignalAction.REDUCE,
        SignalAction.S_INTENT,
    )
    spec = _spec(
        strategy_id="auction_gap",
        producer_commit=producer_commit,
        required=(
            "latest_close",
            "session_low",
            "session_high",
            "price_over_vwap",
            "auction_price_raw",
            "auction_vol_ratio_5d",
            "gap_pct_close",
            "limit_up_price_session_raw",
            "candidate_price_basis",
        ),
        optional=(
            "rel_same_minute",
            "rel_cumulative",
            "amount_accel_5m",
            "amount_accel_10m",
            "tick_rule_buy_sell_ratio_proxy",
            *_LIFECYCLE_FEATURES,
        ),
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="observer_match",
                to_state=StrategyLifecycleState.WATCHING,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.WATCHING,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
            *_execution_lifecycle_transitions(),
        ),
        parameters={
            "auction_ratio_min": 0.15,
            "auction_ratio_max": 5.0,
            "min_gap_pct": 0.0,
            "min_price_over_vwap": 1.0,
            "min_hold_auction_price_ratio": 1.0,
            "expires_seconds": 120,
        },
        allowed_actions=actions,
    )
    return _definition(
        spec=spec,
        static_feature_schema={
            "candidate_price_basis": StaticFeatureSemantic("string", "raw_session_only"),
            "auction_price_raw": StaticFeatureSemantic(
                "number", "current_session_call_auction_clearing_price"
            ),
            "auction_vol_ratio_5d": StaticFeatureSemantic(
                "number", "auction_volume_over_prior_five_session_daily_average_volume"
            ),
            "gap_pct_close": StaticFeatureSemantic(
                "number", "auction_gap_relative_to_current_session_pre_close"
            ),
            "limit_up_price_session_raw": StaticFeatureSemantic(
                "number", "current_session_authoritative_limit_up_price"
            ),
        },
        allowed_actions=actions,
        entry_evaluator=_auction_gap_evaluator,
        evaluator=_auction_gap_runtime_evaluator,
    )
