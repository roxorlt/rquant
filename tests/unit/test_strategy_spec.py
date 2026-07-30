from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from rquant.feature_contracts import FeatureRequirement, RequirementLevel
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)


def _requirement(
    name: str,
    level: RequirementLevel,
) -> FeatureRequirement:
    return FeatureRequirement(
        name=name,
        level=level,
        min_contract_version=1,
    )


def _transitions() -> tuple[StateTransition, ...]:
    return (
        StateTransition(
            from_state=StrategyLifecycleState.IDLE,
            event="candidate_seen",
            to_state=StrategyLifecycleState.WATCHING,
        ),
        StateTransition(
            from_state=StrategyLifecycleState.WATCHING,
            event="entry_ready",
            to_state=StrategyLifecycleState.ARMED,
        ),
        StateTransition(
            from_state=StrategyLifecycleState.ARMED,
            event="buy_filled",
            to_state=StrategyLifecycleState.HOLDING,
        ),
        StateTransition(
            from_state=StrategyLifecycleState.HOLDING,
            event="position_closed",
            to_state=StrategyLifecycleState.TERMINAL,
        ),
    )


def _spec(**changes: object) -> StrategySpec:
    payload: dict[str, object] = {
        "strategy_id": "n-shape-minute",
        "version": 2,
        "feature_contract_id": "intraday-volume",
        "min_feature_contract_version": 1,
        "required_features": (
            _requirement("same_minute_amount_ratio", RequirementLevel.REQUIRED),
            _requirement("vwap_position", RequirementLevel.REQUIRED),
        ),
        "optional_features": (
            _requirement("amount_accel_5m", RequirementLevel.OPTIONAL),
        ),
        "initial_state": StrategyLifecycleState.IDLE,
        "transitions": _transitions(),
        "parameters": {"entry": {"min_ratio": 2.0}, "top_n": 3},
        "allowed_actions": ("watch", "buy", "sell"),
        "run_mode": StrategyRunMode.SHADOW,
        "producer_commit": "c" * 40,
    }
    payload.update(changes)
    return StrategySpec(**payload)


def test_state_transition_requires_nonempty_event() -> None:
    with pytest.raises(ValidationError, match="at least 1 character"):
        StateTransition(
            from_state=StrategyLifecycleState.IDLE,
            event="",
            to_state=StrategyLifecycleState.WATCHING,
        )


def test_strategy_feature_requirements_match_declared_bucket() -> None:
    with pytest.raises(ValidationError, match="required_features"):
        _spec(
            required_features=(
                _requirement("same_minute_amount_ratio", RequirementLevel.OPTIONAL),
            )
        )
    with pytest.raises(ValidationError, match="optional_features"):
        _spec(
            optional_features=(
                _requirement("amount_accel_5m", RequirementLevel.REQUIRED),
            )
        )


def test_strategy_feature_names_are_unique_and_disjoint() -> None:
    repeated = _requirement("vwap_position", RequirementLevel.REQUIRED)
    with pytest.raises(ValidationError, match="unique"):
        _spec(required_features=(repeated, repeated))
    with pytest.raises(ValidationError, match="disjoint"):
        _spec(
            optional_features=(
                _requirement("vwap_position", RequirementLevel.OPTIONAL),
            )
        )
    with pytest.raises(ValidationError, match="allowed_actions.*unique"):
        _spec(allowed_actions=("buy", "buy"))


def test_strategy_transition_graph_must_be_reachable_from_initial_state() -> None:
    transitions = _transitions() + (
        StateTransition(
            from_state=StrategyLifecycleState.TERMINAL,
            event="impossible_restart",
            to_state=StrategyLifecycleState.IDLE,
        ),
    )
    valid = _spec(transitions=transitions)
    assert valid.initial_state is StrategyLifecycleState.IDLE

    unreachable = (
        StateTransition(
            from_state=StrategyLifecycleState.IDLE,
            event="candidate_seen",
            to_state=StrategyLifecycleState.WATCHING,
        ),
        StateTransition(
            from_state=StrategyLifecycleState.ARMED,
            event="buy_filled",
            to_state=StrategyLifecycleState.HOLDING,
        ),
    )
    with pytest.raises(ValidationError, match="reachable"):
        _spec(transitions=unreachable)


def test_strategy_transition_event_is_deterministic_per_state() -> None:
    ambiguous = _transitions() + (
        StateTransition(
            from_state=StrategyLifecycleState.IDLE,
            event="candidate_seen",
            to_state=StrategyLifecycleState.ARMED,
        ),
    )

    with pytest.raises(ValidationError, match="state and event"):
        _spec(transitions=ambiguous)


def test_strategy_fingerprints_are_stable_for_semantic_ordering() -> None:
    left = _spec()
    right = _spec(
        required_features=tuple(reversed(left.required_features)),
        optional_features=tuple(reversed(left.optional_features)),
        transitions=tuple(reversed(left.transitions)),
        parameters={"top_n": 3, "entry": {"min_ratio": 2.0}},
        allowed_actions=tuple(reversed(left.allowed_actions)),
    )

    assert left.parameter_fingerprint == right.parameter_fingerprint
    assert left.spec_fingerprint == right.spec_fingerprint
    assert len(left.spec_fingerprint) == 64


def test_strategy_spec_round_trips_through_json_without_thawing_parameters() -> None:
    spec = _spec(
        parameters={
            "threshold": Decimal("1.40"),
            "training_date": date(2026, 7, 30),
            "visible_at": datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
        }
    )

    restored = StrategySpec.model_validate_json(spec.model_dump_json())

    assert restored == spec
    assert restored.parameters["threshold"] == Decimal("1.40")
    assert restored.parameters["training_date"] == date(2026, 7, 30)
    with pytest.raises(TypeError):
        spec.parameters["threshold"] = Decimal("2")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_strategy_rejects_non_finite_parameter_values(value: float) -> None:
    with pytest.raises(ValidationError, match="finite"):
        _spec(parameters={"threshold": value})


def test_strategy_spec_is_frozen_and_rejects_unknown_fields() -> None:
    spec = _spec()
    with pytest.raises(ValidationError):
        spec.run_mode = StrategyRunMode.MONITOR
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        _spec(experimental_note="no")
