from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType

import pytest

import rquant.strategy_evaluators as strategy_evaluators_module
from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.feature_contracts import FeatureContract, FeatureDefinition
from rquant.runtime_contracts import canonical_sha256
from rquant.signal_contracts import SignalAction
from rquant.strategy_evaluators import (
    BuiltinStrategyEvaluatorRegistry,
    StaticFeatureSemantic,
    project_execution_lifecycle_features,
)
from rquant.strategy_runner import StrategyCandidateState, StrategyDecision
from rquant.strategy_spec import StrategyLifecycleState, StrategySpec

COMMIT = "a" * 40
NOW = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)


@pytest.fixture(scope="module")
def registry() -> BuiltinStrategyEvaluatorRegistry:
    return BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT)


def _state(
    registry: BuiltinStrategyEvaluatorRegistry,
    strategy_id: str,
    state: StrategyLifecycleState = StrategyLifecycleState.IDLE,
) -> StrategyCandidateState:
    spec = registry.load_spec(strategy_id, 1)
    return StrategyCandidateState(
        strategy_spec_fingerprint=spec.spec_fingerprint,
        candidate_id="300001.SZ",
        state=state,
        last_feature_sequence=-1,
        updated_at=NOW,
    )


def _evaluate(
    registry: BuiltinStrategyEvaluatorRegistry,
    strategy_id: str,
    features: dict[str, object],
    state: StrategyLifecycleState = StrategyLifecycleState.IDLE,
) -> StrategyDecision | None:
    definition = registry.load_definition(strategy_id, 1)
    return definition.evaluator(
        definition.spec,
        _state(registry, strategy_id, state),
        features,
    )


def _replacement_runtime_dispatcher(
    _spec: StrategySpec,
    _state: StrategyCandidateState,
    _features: dict[str, object],
) -> StrategyDecision | None:
    return None


def _replacement_session_geometry(
    *,
    session_low: float,
    latest_close: float,
    session_high: float,
) -> None:
    del session_low, latest_close, session_high


def _n_features(**changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "latest_close": 11.2,
        "session_low": 10.05,
        "session_high": 11.3,
        "price_over_vwap": 1.02,
        "t_close_session_raw": 10.0,
        "t_high_session_raw": 11.0,
        "limit_up_price_session_raw": 12.0,
        "limit_pct": 0.2,
        "candidate_price_basis": "raw_session",
        "rel_same_minute": 2.1,
        "rel_cumulative": 1.8,
        "amount_accel_5m": 2.2,
        "amount_accel_10m": 1.7,
        "tick_rule_buy_sell_ratio_proxy": 0.4,
        "historical_sessions": 60,
    }
    values.update(changes)
    return values


def _growth_features(**changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "latest_close": 11.2,
        "opening_bar_high": 10.6,
        "opening_bar_low": 10.2,
        "rel_cumulative": 1.4,
        "price_over_vwap": 1.01,
        "historical_sessions": 5,
        "session_pre_close_raw": 10.0,
        "limit_up_price_session_raw": 12.0,
        "board_type": "gem",
        "ma_alignment": True,
        "large_net_vol_t1": 1.0,
        "candidate_price_basis": "raw_session",
        "rel_same_minute": 2.0,
        "amount_accel_5m": None,
        "amount_accel_10m": None,
        "tick_rule_buy_sell_ratio_proxy": 0.2,
        "minute_volume": 1000,
        "cumulative_volume": 5000,
    }
    values.update(changes)
    return values


def _auction_features(**changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "latest_close": 10.8,
        "session_low": 10.4,
        "session_high": 10.9,
        "price_over_vwap": 1.01,
        "auction_price_raw": 10.5,
        "auction_vol_ratio_5d": 0.15,
        "gap_pct_close": 0.05,
        "limit_up_price_session_raw": 11.5,
        "candidate_price_basis": "raw_session",
        "rel_same_minute": 1.2,
        "rel_cumulative": 1.3,
        "amount_accel_5m": None,
        "amount_accel_10m": None,
        "tick_rule_buy_sell_ratio_proxy": 0.3,
    }
    values.update(changes)
    return values


def test_registry_exposes_complete_immutable_allow_list_and_specs(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    assert registry.identities == (
        ("auction_gap", 1),
        ("growth_board_surge", 1),
        ("n_shape", 1),
    )
    assert isinstance(registry.definitions, MappingProxyType)
    with pytest.raises(TypeError):
        registry.definitions[("other", 1)] = registry.load_definition("n_shape", 1)
    with pytest.raises(AttributeError, match="immutable"):
        registry._definitions = MappingProxyType({})

    lifecycle_features = {
        "entry_fill_status",
        "exit_execution_status",
        "position_closed",
        "holding_trading_sessions",
        "position_sellable",
        "entry_price_raw",
        "structure_stop_price_raw",
        "eligible_high_price_raw",
        "remaining_position_fraction",
    }
    lifecycle_transitions = {
        ("armed", "entry_filled", "holding"),
        ("armed", "entry_rejected", "terminal"),
        ("holding", "take_profit_partial", "holding"),
        ("holding", "exit", "holding"),
        ("holding", "exit_filled", "terminal"),
    }
    lifecycle_actions = (
        SignalAction.WATCH.value,
        SignalAction.B_INTENT.value,
        SignalAction.REDUCE.value,
        SignalAction.S_INTENT.value,
    )
    expected = {
        "n_shape": {
            "required": {
                "latest_close",
                "session_low",
                "session_high",
                "price_over_vwap",
                "t_close_session_raw",
                "t_high_session_raw",
                "limit_up_price_session_raw",
                "limit_pct",
                "candidate_price_basis",
            },
            "optional": {
                "rel_same_minute",
                "rel_cumulative",
                "amount_accel_5m",
                "amount_accel_10m",
                "tick_rule_buy_sell_ratio_proxy",
                "historical_sessions",
            }
            | lifecycle_features,
            "actions": lifecycle_actions,
            "parameters": {
                "carry_low_ratio": 1.0,
                "break_high_ratio": 1.0,
                "min_price_over_vwap": 1.0,
                "price_tolerance_ratio": 0.001,
                "expires_seconds": 120,
            },
            "transitions": {
                ("idle", "structure_supported", "watching"),
                ("idle", "entry_ready", "armed"),
                ("watching", "entry_ready", "armed"),
                ("watching", "support_broken", "terminal"),
            }
            | lifecycle_transitions,
        },
        "growth_board_surge": {
            "required": {
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
            },
            "optional": {
                "rel_same_minute",
                "amount_accel_5m",
                "amount_accel_10m",
                "tick_rule_buy_sell_ratio_proxy",
                "minute_volume",
                "cumulative_volume",
            }
            | lifecycle_features,
            "actions": lifecycle_actions,
            "parameters": {
                "min_rel_cumulative": 1.4,
                "min_rel_same_minute": 2.0,
                "min_amount_accel_5m": 2.0,
                "min_price_over_vwap": 1.0,
                "price_tolerance_ratio": 0.001,
                "expires_seconds": 120,
                "allowed_boards": ("gem", "star"),
                "min_historical_sessions": 5,
            },
            "transitions": {
                ("idle", "volume_building", "watching"),
                ("idle", "entry_ready", "armed"),
                ("watching", "entry_ready", "armed"),
            }
            | lifecycle_transitions,
        },
        "auction_gap": {
            "required": {
                "latest_close",
                "session_low",
                "session_high",
                "price_over_vwap",
                "auction_price_raw",
                "auction_vol_ratio_5d",
                "gap_pct_close",
                "limit_up_price_session_raw",
                "candidate_price_basis",
            },
            "optional": {
                "rel_same_minute",
                "rel_cumulative",
                "amount_accel_5m",
                "amount_accel_10m",
                "tick_rule_buy_sell_ratio_proxy",
            }
            | lifecycle_features,
            "actions": lifecycle_actions,
            "parameters": {
                "auction_ratio_min": 0.15,
                "auction_ratio_max": 5.0,
                "min_gap_pct": 0.0,
                "min_price_over_vwap": 1.0,
                "min_hold_auction_price_ratio": 1.0,
                "expires_seconds": 120,
            },
            "transitions": {
                ("idle", "observer_match", "watching"),
                ("watching", "entry_ready", "armed"),
            }
            | lifecycle_transitions,
        },
    }

    for strategy_id, contract in expected.items():
        spec = registry.load_spec(strategy_id, 1)
        assert spec.feature_contract_id == "intraday-pit"
        assert spec.min_feature_contract_version == 3
        assert spec.producer_commit == COMMIT
        assert {item.name for item in spec.required_features} == contract["required"]
        assert {item.name for item in spec.optional_features} == contract["optional"]
        assert all(item.min_contract_version == 3 for item in spec.required_features)
        assert all(item.min_contract_version == 3 for item in spec.optional_features)
        assert spec.allowed_actions == contract["actions"]
        assert dict(spec.parameters) == contract["parameters"]
        assert {
            (item.from_state.value, item.event, item.to_state.value) for item in spec.transitions
        } == contract["transitions"]


def test_registry_loaders_fail_closed_and_binding_matches_definition(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    definition = registry.load_definition("n_shape", 1)
    binding = registry.load_binding("n_shape", 1)

    assert registry.load_spec("n_shape", 1) is definition.spec
    assert binding.strategy_id == "n_shape"
    assert binding.strategy_version == 1
    assert binding.contract_fingerprint == definition.executable_fingerprint
    assert binding.evaluator is definition.evaluator
    with pytest.raises(KeyError, match="unknown built-in strategy"):
        registry.load_definition("n_shape", 2)
    with pytest.raises(KeyError, match="unknown built-in strategy"):
        registry.load_spec("unknown", 1)
    with pytest.raises(KeyError, match="unknown built-in strategy"):
        registry.load_binding("unknown", 1)
    with pytest.raises(ValueError, match="40-character"):
        BuiltinStrategyEvaluatorRegistry(producer_commit="short")
    with pytest.raises(TypeError, match="string"):
        BuiltinStrategyEvaluatorRegistry(producer_commit=1)  # type: ignore[arg-type]


def test_all_builtin_specs_round_trip_through_trusted_definition_registry(
    tmp_path: Path,
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    execution_registry = registry.trusted_executable_registry()
    definition_store = ImmutableDefinitionRegistry(
        tmp_path / "definitions",
        execution_registry=execution_registry,
    )
    feature_names = sorted(
        {
            requirement.name
            for definition in registry.definitions.values()
            for requirement in (
                *definition.spec.required_features,
                *definition.spec.optional_features,
            )
        }
    )
    parent = None
    for version in (1, 2, 3):
        contract = FeatureContract(
            contract_id="intraday-pit",
            version=version,
            features=tuple(
                FeatureDefinition(
                    name=name,
                    dtype="object",
                    source_datasets=("market_minute",),
                    lookback=90,
                    pit_rule="available_at <= decision_time",
                    price_basis="raw",
                    availability_contract={
                        "source_available_at_basis": "max_source_available_at",
                        "max_delay_seconds": 60,
                        "missing_policy": "mark_unavailable",
                        "late_policy": "mark_stale",
                        "decision_visibility_gate": "available_at_lte_decision_time",
                    },
                )
                for name in feature_names
            ),
            producer_commit=COMMIT,
        )
        parent = definition_store.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT,
            expected_fingerprint=contract.contract_fingerprint,
            parent_fingerprint=None if parent is None else parent.fingerprint,
            supersedes=None if parent is None else parent.version,
            replacement_reason=None if parent is None else "contract evolution",
        )
    assert parent is not None

    for definition in registry.definitions.values():
        spec = definition.spec
        transitions = {
            (transition.from_state, transition.event, transition.to_state)
            for transition in spec.transitions
        }
        assert (
            StrategyLifecycleState.ARMED,
            "entry_filled",
            StrategyLifecycleState.HOLDING,
        ) in transitions
        assert any(
            from_state is StrategyLifecycleState.HOLDING
            and to_state is StrategyLifecycleState.TERMINAL
            for from_state, _event, to_state in transitions
        )
        registered = definition_store.register_strategy_spec(
            spec,
            feature_contract_fingerprint=parent.fingerprint,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT,
            expected_fingerprint=spec.spec_fingerprint,
        )

        assert definition_store.read_strategy_spec(registered.fingerprint) == registered
        assert registered.executable_fingerprint == definition.executable_fingerprint


def test_lifecycle_features_bind_execution_projection_not_market_minute_compute(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    availability = {
        "source_available_at_basis": "authoritative_source_available_at",
        "max_delay_seconds": 1,
        "missing_policy": "fail_closed",
        "late_policy": "fail_closed",
        "decision_visibility_gate": "available_at_lte_decision_time",
    }
    contract = FeatureContract(
        contract_id="intraday-pit",
        version=3,
        features=(
            FeatureDefinition(
                name="latest_close",
                dtype="float64",
                source_datasets=("market_minute",),
                lookback=0,
                pit_rule="available_at <= decision_time",
                price_basis="raw",
                availability_contract=availability,
            ),
            FeatureDefinition(
                name="entry_fill_status",
                dtype="string",
                source_datasets=("paper_execution_state",),
                lookback=0,
                pit_rule="available_at <= decision_time",
                price_basis="raw",
                availability_contract=availability,
            ),
        ),
        producer_commit=COMMIT,
    )
    bindings = {
        binding.feature_name: binding
        for binding in registry.trusted_executable_registry().feature_bindings(contract)
    }

    assert (
        bindings["entry_fill_status"].implementation_id
        != bindings["latest_close"].implementation_id
    )


def test_execution_lifecycle_projection_validates_authoritative_state() -> None:
    projected = project_execution_lifecycle_features(
        {
            "entry_fill_status": "filled",
            "exit_execution_status": "pending",
            "position_closed": False,
            "holding_trading_sessions": 1,
            "position_sellable": True,
            "entry_price_raw": 10,
            "structure_stop_price_raw": 9.7,
            "eligible_high_price_raw": 11.2,
            "remaining_position_fraction": 0.5,
        }
    )

    assert projected == {
        "eligible_high_price_raw": 11.2,
        "entry_fill_status": "filled",
        "entry_price_raw": 10.0,
        "exit_execution_status": "pending",
        "holding_trading_sessions": 1,
        "position_closed": False,
        "position_sellable": True,
        "remaining_position_fraction": 0.5,
        "structure_stop_price_raw": 9.7,
    }
    with pytest.raises(ValueError, match="unsupported execution lifecycle field"):
        project_execution_lifecycle_features({"latest_close": 10.0})
    with pytest.raises(ValueError, match="entry_fill_status"):
        project_execution_lifecycle_features({"entry_fill_status": "unknown"})
    with pytest.raises(ValueError, match="exit_execution_status"):
        project_execution_lifecycle_features({"exit_execution_status": "unknown"})
    with pytest.raises(ValueError, match="remaining_position_fraction"):
        project_execution_lifecycle_features({"remaining_position_fraction": 1.1})


def test_executable_fingerprint_binds_actual_runtime_dispatcher(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    definition = registry.load_definition("n_shape", 1)
    replaced = replace(definition, evaluator=_replacement_runtime_dispatcher)

    assert replaced.executable_fingerprint != definition.executable_fingerprint


def test_executable_fingerprint_binds_referenced_helper_graph(
    registry: BuiltinStrategyEvaluatorRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = registry.load_definition("n_shape", 1)
    original = definition.executable_fingerprint

    monkeypatch.setattr(
        strategy_evaluators_module,
        "_validate_session_geometry",
        _replacement_session_geometry,
    )

    assert definition.executable_fingerprint != original


def test_executable_fingerprint_binds_referenced_global_constants(
    registry: BuiltinStrategyEvaluatorRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = registry.load_definition("n_shape", 1)
    original = definition.executable_fingerprint

    monkeypatch.setattr(strategy_evaluators_module, "_STOP_LOSS_BPS", 999)

    assert definition.executable_fingerprint != original


def test_definition_rejects_outer_identity_action_and_schema_drift(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    definition = registry.load_definition("n_shape", 1)

    with pytest.raises(ValueError, match="strategy_id.*spec"):
        replace(definition, strategy_id="other")
    with pytest.raises(ValueError, match="strategy_version.*spec"):
        replace(definition, strategy_version=2)
    with pytest.raises(ValueError, match="producer_commit.*spec"):
        replace(definition, producer_commit="b" * 40)
    with pytest.raises(TypeError, match="allowed_actions.*SignalAction"):
        replace(definition, allowed_actions=("watch",))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="allowed_actions.*unique"):
        replace(
            definition,
            allowed_actions=(SignalAction.WATCH, SignalAction.WATCH),
        )
    with pytest.raises(ValueError, match="allowed_actions.*spec"):
        replace(definition, allowed_actions=tuple(reversed(definition.allowed_actions)))
    with pytest.raises(ValueError, match="static feature schema cannot be empty"):
        replace(definition, static_feature_schema=MappingProxyType({}))
    with pytest.raises(ValueError, match="static feature schema.*declared"):
        replace(
            definition,
            static_feature_schema=MappingProxyType(
                {
                    **definition.static_feature_schema,
                    "undeclared": StaticFeatureSemantic("number", "invalid"),
                }
            ),
        )
    with pytest.raises(ValueError, match="static feature names cannot be empty"):
        replace(
            definition,
            static_feature_schema=MappingProxyType(
                {"": StaticFeatureSemantic("number", "invalid")}
            ),
        )


def test_contract_fingerprint_is_stable_and_semantically_sensitive(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    rebuilt = BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT)
    definition = registry.load_definition("growth_board_surge", 1)

    assert (
        definition.contract_fingerprint
        == rebuilt.load_definition("growth_board_surge", 1).contract_fingerprint
    )
    assert len(definition.contract_fingerprint) == 64
    assert definition.contract_fingerprint == canonical_sha256(definition.contract_payload())
    assert definition.candidate_schema_fingerprint == canonical_sha256(
        {
            "contract": "strategy-candidate-static-schema/v1",
            "strategy_id": definition.strategy_id,
            "strategy_version": definition.strategy_version,
            "static_feature_schema": {
                name: semantic.contract_payload()
                for name, semantic in sorted(definition.static_feature_schema.items())
            },
        }
    )

    reordered_spec = definition.spec.model_copy(
        update={"allowed_actions": tuple(reversed(definition.spec.allowed_actions))}
    )
    reordered = replace(
        definition,
        spec=reordered_spec,
        static_feature_schema=MappingProxyType(
            dict(reversed(tuple(definition.static_feature_schema.items())))
        ),
        allowed_actions=tuple(reversed(definition.allowed_actions)),
    )
    assert reordered.contract_fingerprint == definition.contract_fingerprint

    changed_spec = definition.spec.model_copy(
        update={
            "parameters": MappingProxyType(
                {**definition.spec.parameters, "min_rel_cumulative": 1.41}
            )
        }
    )
    changed_action_spec = definition.spec.model_copy(
        update={"allowed_actions": (SignalAction.WATCH.value,)}
    )
    changed_commit_spec = definition.spec.model_copy(update={"producer_commit": "b" * 40})

    changes = (
        replace(definition, contract_schema_version=2),
        replace(definition, evaluator_semantic_version="1.0.1"),
        replace(definition, spec=changed_spec),
        replace(
            definition,
            spec=changed_action_spec,
            allowed_actions=(SignalAction.WATCH,),
        ),
        replace(
            definition,
            spec=changed_commit_spec,
            producer_commit="b" * 40,
        ),
        replace(
            definition,
            static_feature_schema=MappingProxyType(
                {
                    **definition.static_feature_schema,
                    "large_net_vol_t1": StaticFeatureSemantic(
                        dtype="number",
                        semantic="same_day_live_order_flow",
                    ),
                }
            ),
        ),
    )
    assert all(
        changed.contract_fingerprint != definition.contract_fingerprint for changed in changes
    )
    assert changes[-1].candidate_schema_fingerprint != definition.candidate_schema_fingerprint
    assert definition.static_feature_schema["large_net_vol_t1"].semantic == "t_minus_1_daily_proxy"
    with pytest.raises(TypeError):
        definition.static_feature_schema["new"] = StaticFeatureSemantic(
            dtype="number",
            semantic="invalid mutation",
        )


def test_candidate_schema_fingerprint_delegates_to_snapshot_canonical_helper(
    registry: BuiltinStrategyEvaluatorRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = "f" * 64
    observed: dict[str, object] = {}

    def canonical_helper(**kwargs: object) -> str:
        observed.update(kwargs)
        return expected

    monkeypatch.setattr(
        strategy_evaluators_module,
        "strategy_candidate_schema_fingerprint",
        canonical_helper,
        raising=False,
    )
    definition = registry.load_definition("n_shape", 1)

    assert definition.candidate_schema_fingerprint == expected
    assert observed == {
        "strategy_id": "n_shape",
        "strategy_version": "1",
        "static_feature_schema": {
            name: semantic.contract_payload()
            for name, semantic in definition.static_feature_schema.items()
        },
    }


@pytest.mark.parametrize("dtype", ["float64", "boolean", "NUMBER", " number "])
def test_static_feature_semantic_rejects_noncanonical_dtype(dtype: str) -> None:
    with pytest.raises(ValueError, match="canonical static feature dtype"):
        StaticFeatureSemantic(dtype=dtype, semantic="candidate score")


def test_n_shape_positive_watch_entry_and_broken_support(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    watching = _evaluate(
        registry,
        "n_shape",
        _n_features(latest_close=10.8),
    )
    assert watching is not None
    assert watching.action is SignalAction.WATCH
    assert watching.expected_from_state is StrategyLifecycleState.IDLE
    assert watching.expected_to_state is StrategyLifecycleState.WATCHING
    assert watching.expires_after is not None
    assert watching.expires_after.total_seconds() == 120

    direct = _evaluate(registry, "n_shape", _n_features())
    armed = _evaluate(
        registry,
        "n_shape",
        _n_features(),
        StrategyLifecycleState.WATCHING,
    )
    for decision, from_state in (
        (direct, StrategyLifecycleState.IDLE),
        (armed, StrategyLifecycleState.WATCHING),
    ):
        assert decision is not None
        assert decision.action is SignalAction.B_INTENT
        assert decision.expected_from_state is from_state
        assert decision.expected_to_state is StrategyLifecycleState.ARMED
        assert "tick_rule_buy_sell_ratio_proxy" in decision.evidence

    broken = _evaluate(
        registry,
        "n_shape",
        _n_features(session_low=9.0),
        StrategyLifecycleState.WATCHING,
    )
    assert broken is not None
    assert broken.action is None
    assert broken.reason_codes == ()
    assert broken.expected_to_state is StrategyLifecycleState.TERMINAL


@pytest.mark.parametrize(
    ("changes", "state"),
    [
        ({"session_low": 9.98}, StrategyLifecycleState.IDLE),
        ({"latest_close": 11.2, "price_over_vwap": 0.99}, StrategyLifecycleState.IDLE),
        ({"latest_close": 12.0, "session_high": 12.0}, StrategyLifecycleState.IDLE),
    ],
)
def test_n_shape_negative_and_price_boundaries(
    registry: BuiltinStrategyEvaluatorRegistry,
    changes: dict[str, object],
    state: StrategyLifecycleState,
) -> None:
    assert _evaluate(registry, "n_shape", _n_features(**changes), state) is None


def test_n_shape_weak_vwap_still_watches_supported_unbroken_structure(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    decision = _evaluate(
        registry,
        "n_shape",
        _n_features(latest_close=10.8, price_over_vwap=0.99),
    )
    assert decision is not None
    assert decision.action is SignalAction.WATCH
    assert decision.expected_to_state is StrategyLifecycleState.WATCHING


def test_n_shape_tolerance_boundary_and_proxy_is_not_a_gate(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    decision = _evaluate(
        registry,
        "n_shape",
        _n_features(
            latest_close=11.0 * (1.0 - 0.001),
            session_low=10.0 * (1.0 - 0.001),
            tick_rule_buy_sell_ratio_proxy=0.0,
        ),
    )
    assert decision is not None
    assert decision.action is SignalAction.B_INTENT


@pytest.mark.parametrize(
    "changes",
    [
        {"latest_close": 0.0},
        {"session_low": 11.21},
        {"session_high": 11.19},
        {"t_high_session_raw": 9.99},
        {"price_over_vwap": 0.0},
        {"rel_same_minute": -0.01},
        {"amount_accel_5m": -0.01},
        {"tick_rule_buy_sell_ratio_proxy": -0.01},
        {"historical_sessions": -1},
        {"limit_pct": 0.0},
        {"limit_up_price_session_raw": 0.0},
        {"latest_close": 12.01, "session_high": 12.01},
    ],
)
def test_n_shape_rejects_invalid_financial_geometry(
    registry: BuiltinStrategyEvaluatorRegistry,
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        _evaluate(registry, "n_shape", _n_features(**changes))


def test_n_shape_authoritative_limit_equality_is_legal_but_never_buys(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    decision = _evaluate(
        registry,
        "n_shape",
        _n_features(latest_close=12.0, session_high=12.0),
    )
    assert decision is None


def test_n_shape_session_high_cannot_exceed_authoritative_limit(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    with pytest.raises(ValueError, match="session_high.*limit_up"):
        _evaluate(registry, "n_shape", _n_features(session_high=12.01))

    at_limit = _evaluate(
        registry,
        "n_shape",
        _n_features(latest_close=10.8, session_high=12.0),
    )
    assert at_limit is not None
    assert at_limit.action is SignalAction.WATCH


def test_growth_positive_optional_burst_and_watch_then_entry(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    same_minute = _evaluate(registry, "growth_board_surge", _growth_features())
    accelerated = _evaluate(
        registry,
        "growth_board_surge",
        _growth_features(rel_same_minute=None, amount_accel_5m=2.0),
    )
    for decision in (same_minute, accelerated):
        assert decision is not None
        assert decision.action is SignalAction.B_INTENT
        assert decision.expected_to_state is StrategyLifecycleState.ARMED
        assert decision.evidence["large_net_vol_semantics"] == "t_minus_1_daily_proxy"
        assert decision.evidence["tick_rule_is_proxy"] is True

    watch = _evaluate(
        registry,
        "growth_board_surge",
        _growth_features(rel_same_minute=None, amount_accel_5m=None),
    )
    assert watch is not None
    assert watch.action is SignalAction.WATCH
    assert watch.expected_to_state is StrategyLifecycleState.WATCHING

    later = _evaluate(
        registry,
        "growth_board_surge",
        _growth_features(rel_same_minute=None, amount_accel_5m=2.1),
        StrategyLifecycleState.WATCHING,
    )
    assert later is not None
    assert later.action is SignalAction.B_INTENT


@pytest.mark.parametrize(
    "changes",
    [
        {"board_type": "main"},
        {"ma_alignment": False},
        {"large_net_vol_t1": 0.0},
        {"historical_sessions": 4},
        {"rel_cumulative": 1.3999},
        {"price_over_vwap": 0.999},
        {"latest_close": 12.0},
        {"opening_bar_high": 12.0, "opening_bar_low": 12.0},
    ],
)
def test_growth_rejects_static_dynamic_and_one_price_failures(
    registry: BuiltinStrategyEvaluatorRegistry,
    changes: dict[str, object],
) -> None:
    assert _evaluate(registry, "growth_board_surge", _growth_features(**changes)) is None


def test_growth_never_watches_an_opening_one_price_limit(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    decision = _evaluate(
        registry,
        "growth_board_surge",
        _growth_features(
            opening_bar_high=12.0,
            opening_bar_low=12.0,
            rel_same_minute=None,
            amount_accel_5m=None,
        ),
    )
    assert decision is None


def test_growth_one_price_filter_requires_the_authoritative_limit_price(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    below_limit = _evaluate(
        registry,
        "growth_board_surge",
        _growth_features(opening_bar_high=11.99, opening_bar_low=11.99),
    )
    at_limit = _evaluate(
        registry,
        "growth_board_surge",
        _growth_features(opening_bar_high=12.0, opening_bar_low=12.0),
    )

    assert below_limit is not None
    assert below_limit.action is SignalAction.B_INTENT
    assert at_limit is None


@pytest.mark.parametrize(
    "changes",
    [
        {"latest_close": 0.0},
        {"opening_bar_low": 10.7},
        {"opening_bar_high": 0.0},
        {"price_over_vwap": 0.0},
        {"rel_cumulative": -0.01},
        {"rel_same_minute": -0.01},
        {"amount_accel_5m": -0.01},
        {"tick_rule_buy_sell_ratio_proxy": -0.01},
        {"minute_volume": -1.0},
        {"cumulative_volume": -1.0},
        {"historical_sessions": -1},
        {"session_pre_close_raw": 0.0},
        {"limit_up_price_session_raw": 0.0},
        {"limit_up_price_session_raw": 9.99},
        {"latest_close": 12.01},
    ],
)
def test_growth_rejects_invalid_financial_geometry(
    registry: BuiltinStrategyEvaluatorRegistry,
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        _evaluate(registry, "growth_board_surge", _growth_features(**changes))


def test_growth_limit_equality_is_legal_and_t1_proxy_stays_daily(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    at_limit = _evaluate(
        registry,
        "growth_board_surge",
        _growth_features(latest_close=12.0),
    )
    negative_proxy = _evaluate(
        registry,
        "growth_board_surge",
        _growth_features(large_net_vol_t1=-1.0),
    )
    positive = _evaluate(registry, "growth_board_surge", _growth_features())

    assert at_limit is None
    assert negative_proxy is None
    assert positive is not None
    assert "t_minus_1_order_flow_proxy" in positive.reason_codes
    assert positive.evidence["large_net_vol_semantics"] == "t_minus_1_daily_proxy"
    assert "realtime" not in str(positive.evidence).lower()


def test_growth_opening_high_cannot_exceed_authoritative_limit(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    with pytest.raises(ValueError, match="opening_bar_high.*limit_up"):
        _evaluate(
            registry,
            "growth_board_surge",
            _growth_features(opening_bar_high=12.01),
        )

    at_limit = _evaluate(
        registry,
        "growth_board_surge",
        _growth_features(
            opening_bar_high=12.0,
            rel_same_minute=None,
            amount_accel_5m=None,
        ),
    )
    assert at_limit is not None
    assert at_limit.action is SignalAction.WATCH


@pytest.mark.parametrize("board", ["gem", "star"])
def test_growth_accepts_both_allowed_boards_and_threshold_boundaries(
    registry: BuiltinStrategyEvaluatorRegistry,
    board: str,
) -> None:
    decision = _evaluate(
        registry,
        "growth_board_surge",
        _growth_features(
            board_type=board,
            rel_cumulative=1.4,
            rel_same_minute=2.0,
            historical_sessions=5,
        ),
    )
    assert decision is not None
    assert decision.action is SignalAction.B_INTENT


def test_auction_gap_observer_then_minute_confirmation_emits_buy_intent(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    for ratio in (0.15, 5.0):
        decision = _evaluate(
            registry,
            "auction_gap",
            _auction_features(auction_vol_ratio_5d=ratio),
        )
        assert decision is not None
        assert decision.action is SignalAction.WATCH
        assert decision.expected_to_state is StrategyLifecycleState.WATCHING
        assert decision.evidence["observer_only"] is True

    confirmed = _evaluate(
        registry,
        "auction_gap",
        _auction_features(),
        StrategyLifecycleState.WATCHING,
    )
    assert confirmed is not None
    assert confirmed.action is SignalAction.B_INTENT
    assert confirmed.expected_to_state is StrategyLifecycleState.ARMED
    assert SignalAction.B_INTENT.value in registry.load_spec("auction_gap", 1).allowed_actions


@pytest.mark.parametrize(
    "changes",
    [
        {"auction_vol_ratio_5d": 0.1499},
        {"auction_vol_ratio_5d": 5.0001},
        {"gap_pct_close": 0.0},
        {"latest_close": 10.49},
        {"price_over_vwap": 0.99},
        {"latest_close": 11.5, "session_high": 11.5},
    ],
)
def test_auction_gap_negative_cases(
    registry: BuiltinStrategyEvaluatorRegistry,
    changes: dict[str, object],
) -> None:
    assert _evaluate(registry, "auction_gap", _auction_features(**changes)) is None


@pytest.mark.parametrize(
    "changes",
    [
        {"latest_close": 0.0},
        {"session_low": 10.81},
        {"session_high": 10.79},
        {"price_over_vwap": 0.0},
        {"auction_price_raw": 0.0},
        {"auction_vol_ratio_5d": -0.01},
        {"rel_cumulative": -0.01},
        {"amount_accel_10m": -0.01},
        {"tick_rule_buy_sell_ratio_proxy": -0.01},
        {"limit_up_price_session_raw": 0.0},
        {"latest_close": 11.51, "session_high": 11.51},
        {"auction_price_raw": 11.51},
    ],
)
def test_auction_gap_rejects_invalid_financial_geometry(
    registry: BuiltinStrategyEvaluatorRegistry,
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        _evaluate(registry, "auction_gap", _auction_features(**changes))


def test_auction_limit_equality_is_legal_but_observer_does_not_fire(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    decision = _evaluate(
        registry,
        "auction_gap",
        _auction_features(
            latest_close=11.5,
            session_high=11.5,
            auction_price_raw=11.5,
        ),
    )
    assert decision is None


def test_auction_session_high_cannot_exceed_authoritative_limit(
    registry: BuiltinStrategyEvaluatorRegistry,
) -> None:
    with pytest.raises(ValueError, match="session_high.*limit_up"):
        _evaluate(registry, "auction_gap", _auction_features(session_high=11.51))

    at_limit = _evaluate(
        registry,
        "auction_gap",
        _auction_features(session_high=11.5),
    )
    assert at_limit is not None
    assert at_limit.action is SignalAction.WATCH


@pytest.mark.parametrize(
    ("strategy_id", "features"),
    [
        ("n_shape", _n_features()),
        ("growth_board_surge", _growth_features()),
        ("auction_gap", _auction_features()),
    ],
)
def test_all_evaluators_ignore_terminal_state_before_feature_validation(
    registry: BuiltinStrategyEvaluatorRegistry,
    strategy_id: str,
    features: dict[str, object],
) -> None:
    assert (
        _evaluate(
            registry,
            strategy_id,
            {**features, "latest_close": True},
            StrategyLifecycleState.TERMINAL,
        )
        is None
    )


@pytest.mark.parametrize("strategy_id", ("n_shape", "growth_board_surge", "auction_gap"))
def test_all_builtin_evaluators_confirm_fill_and_enforce_t_plus_one_exits(
    registry: BuiltinStrategyEvaluatorRegistry,
    strategy_id: str,
) -> None:
    filled = _evaluate(
        registry,
        strategy_id,
        {"entry_fill_status": "filled"},
        StrategyLifecycleState.ARMED,
    )
    assert filled is not None
    assert filled.action is None
    assert filled.expected_to_state is StrategyLifecycleState.HOLDING

    same_day = {
        "exit_execution_status": "none",
        "position_closed": False,
        "holding_trading_sessions": 0,
        "position_sellable": False,
        "latest_close": 9.6,
        "entry_price_raw": 10.0,
        "structure_stop_price_raw": 9.7,
        "eligible_high_price_raw": 10.0,
        "remaining_position_fraction": 1.0,
    }
    assert (
        _evaluate(
            registry,
            strategy_id,
            same_day,
            StrategyLifecycleState.HOLDING,
        )
        is None
    )

    stop = _evaluate(
        registry,
        strategy_id,
        {
            **same_day,
            "holding_trading_sessions": 1,
            "position_sellable": True,
        },
        StrategyLifecycleState.HOLDING,
    )
    assert stop is not None
    assert stop.action is SignalAction.S_INTENT
    assert stop.expected_to_state is StrategyLifecycleState.HOLDING
    assert stop.evidence["sell_tranche_fraction"] == 1.0

    trailing = _evaluate(
        registry,
        strategy_id,
        {
            **same_day,
            "holding_trading_sessions": 1,
            "position_sellable": True,
            "latest_close": 10.6,
            "structure_stop_price_raw": 9.5,
            "eligible_high_price_raw": 11.0,
        },
        StrategyLifecycleState.HOLDING,
    )
    assert trailing is not None
    assert trailing.action is SignalAction.REDUCE
    assert trailing.expected_to_state is StrategyLifecycleState.HOLDING
    assert trailing.evidence["sell_tranche_fraction"] == 0.5

    final_tranche = _evaluate(
        registry,
        strategy_id,
        {
            **trailing.evidence,
            "remaining_position_fraction": 0.5,
        },
        StrategyLifecycleState.HOLDING,
    )
    assert final_tranche is not None
    assert final_tranche.action is SignalAction.S_INTENT
    assert final_tranche.expected_to_state is StrategyLifecycleState.HOLDING
    assert final_tranche.evidence["sell_tranche_fraction"] == 1.0

    pending = _evaluate(
        registry,
        strategy_id,
        {**same_day, "exit_execution_status": "pending"},
        StrategyLifecycleState.HOLDING,
    )
    assert pending is None

    retryable = _evaluate(
        registry,
        strategy_id,
        {
            **same_day,
            "holding_trading_sessions": 1,
            "position_sellable": True,
            "exit_execution_status": "retryable",
        },
        StrategyLifecycleState.HOLDING,
    )
    assert retryable is not None
    assert retryable.action is SignalAction.S_INTENT
    assert retryable.expected_to_state is StrategyLifecycleState.HOLDING

    closed = _evaluate(
        registry,
        strategy_id,
        {
            **same_day,
            "position_closed": True,
            "remaining_position_fraction": 0.0,
            "exit_execution_status": "filled",
        },
        StrategyLifecycleState.HOLDING,
    )
    assert closed is not None
    assert closed.event == "exit_filled"
    assert closed.action is None
    assert closed.expected_to_state is StrategyLifecycleState.TERMINAL


@pytest.mark.parametrize(
    ("strategy_id", "features", "field", "bad_value", "error"),
    [
        ("n_shape", _n_features(), "latest_close", True, TypeError),
        ("n_shape", _n_features(), "latest_close", float("nan"), ValueError),
        ("n_shape", _n_features(), "latest_close", float("inf"), ValueError),
        ("n_shape", _n_features(), "candidate_price_basis", 1, TypeError),
        ("n_shape", _n_features(), "candidate_price_basis", "qfq", ValueError),
        ("growth_board_surge", _growth_features(), "ma_alignment", 1, TypeError),
        ("growth_board_surge", _growth_features(), "historical_sessions", True, TypeError),
        ("growth_board_surge", _growth_features(), "board_type", 1, TypeError),
        ("growth_board_surge", _growth_features(), "rel_same_minute", False, TypeError),
        ("auction_gap", _auction_features(), "auction_vol_ratio_5d", "0.15", TypeError),
        ("auction_gap", _auction_features(), "amount_accel_5m", float("-inf"), ValueError),
    ],
)
def test_evaluators_reject_malformed_nonfinite_and_type_confusion(
    registry: BuiltinStrategyEvaluatorRegistry,
    strategy_id: str,
    features: dict[str, object],
    field: str,
    bad_value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error, match=field):
        _evaluate(registry, strategy_id, {**features, field: bad_value})


@pytest.mark.parametrize(
    ("strategy_id", "features", "missing"),
    [
        ("n_shape", _n_features(), "session_low"),
        ("growth_board_surge", _growth_features(), "large_net_vol_t1"),
        ("auction_gap", _auction_features(), "auction_price_raw"),
    ],
)
def test_required_features_cannot_be_missing_or_none(
    registry: BuiltinStrategyEvaluatorRegistry,
    strategy_id: str,
    features: dict[str, object],
    missing: str,
) -> None:
    without = {key: value for key, value in features.items() if key != missing}
    with pytest.raises(ValueError, match=missing):
        _evaluate(registry, strategy_id, without)
    with pytest.raises(ValueError, match=missing):
        _evaluate(registry, strategy_id, {**features, missing: None})


@pytest.mark.parametrize(
    ("strategy_id", "features"),
    [
        ("n_shape", _n_features()),
        ("growth_board_surge", _growth_features()),
        ("auction_gap", _auction_features()),
    ],
)
def test_evaluation_is_deterministic_and_rejects_wrong_bound_spec(
    registry: BuiltinStrategyEvaluatorRegistry,
    strategy_id: str,
    features: dict[str, object],
) -> None:
    definition = registry.load_definition(strategy_id, 1)
    state = _state(registry, strategy_id)
    first = definition.evaluator(definition.spec, state, features)
    second = definition.evaluator(definition.spec, state, dict(reversed(features.items())))
    assert first == second
    assert first is None or first.model_dump(mode="json") == second.model_dump(mode="json")

    wrong_spec = registry.load_spec(
        "auction_gap" if strategy_id != "auction_gap" else "n_shape",
        1,
    )
    with pytest.raises(ValueError, match="spec fingerprint"):
        definition.evaluator(wrong_spec, state, features)
