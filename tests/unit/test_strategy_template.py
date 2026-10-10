"""Frozen ST-01/ST-03 template and trusted registration behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from rquant.definition_registry import (
    DefinitionReferenceError,
    ImmutableDefinitionRegistry,
    TrustedExecutableRegistry,
)
from rquant.strategy_spec import StrategyLifecycleState, StrategyRunMode
from rquant.strategy_template import StrategyTemplate, compile_strategy_template
from rquant.strategy_template_definition import (
    strategy_template_feature_contract,
    template_executables,
)

COMMIT = "0" * 40
ID = "template_" + "1" * 32


def template_payload() -> dict[str, object]:
    return {
        "entry": {"kind": "conditions", "conditions": [{"key": "not_st", "args": {}}]},
        "exit": {
            "stop_loss": "0.1",
            "take_profit": "0.2",
            "trailing_profit": "0.05",
            "max_holding_days": 20,
            "exit_time": "14:30",
        },
        "weight_rule": {"max_positions": 10, "cash_reserve": "0.2"},
        "rebalance_rule": {"kind": "weekly"},
        "index_filter": {"benchmark_code": "000300.SH", "ma_days": 20, "direction": "above"},
    }


def test_full_template_compiles_to_exact_spec_without_user_executable() -> None:
    rules = StrategyTemplate.model_validate(template_payload())
    spec = compile_strategy_template(rules, strategy_id=ID, version=2, producer_commit=COMMIT)
    assert spec.strategy_id == ID and spec.version == 2
    assert spec.feature_contract_id == "strategy-template-entry/v1"
    assert spec.run_mode is StrategyRunMode.PAPER
    assert spec.initial_state is StrategyLifecycleState.IDLE
    assert spec.parameters["template_contract"] == "strategy-template/v1"
    assert spec.parameters["rules"]["exit"]["stop_loss"] == Decimal("0.1")
    assert spec.parameters["rules"]["rebalance_rule"]["kind"] == "weekly"
    with pytest.raises(TypeError):
        spec.parameters["rules"]["exit"]["stop_loss"] = Decimal("0.9")


@pytest.mark.parametrize(
    "delta",
    [
        {"template_contract": "custom-python"},
        {"entry": {"kind": "code", "expression": "1"}},
        {
            "entry": {
                "kind": "conditions",
                "conditions": [{"key": "not_st", "args": {"path": "a.py"}}],
            }
        },
        {
            "entry": {
                "kind": "conditions",
                "conditions": [{"key": "above_ma", "args": {"period": True}}],
            }
        },
        {
            "entry": {
                "kind": "conditions",
                "conditions": [{"key": "gt", "args": {"left": "__import__('os')", "right": 1}}],
            }
        },
        {"exit": {"stop_loss": "NaN"}},
        {"exit": {"take_profit": "Infinity"}},
        {"exit": {"trailing_profit": "1"}},
        {"exit": {"max_holding_days": True}},
        {"exit": {"max_holding_days": 2521}},
        {"exit": {"exit_time": "09:30"}},
        {"exit": {"exit_time": "12:00"}},
        {"exit": {"exit_time": "14:30:01"}},
        {"weight_rule": {"max_positions": 501}},
        {"weight_rule": {"max_positions": 10, "min_target_amount": "1.001"}},
        {"rebalance_rule": {"kind": "every_n", "every_n_days": 253}},
        {"index_filter": {"benchmark_code": "custom", "ma_days": 10, "direction": "above"}},
        {"index_filter": {"benchmark_code": "000300.SH", "ma_days": True, "direction": "above"}},
    ],
)
def test_untrusted_or_out_of_range_fields_are_rejected(delta: dict[str, object]) -> None:
    with pytest.raises((ValidationError, ValueError)):
        StrategyTemplate.model_validate({**template_payload(), **delta})


def test_fixed_top_level_executables_publish_and_read_exact_version(tmp_path) -> None:
    rules = StrategyTemplate.model_validate(template_payload())
    spec = compile_strategy_template(rules, strategy_id=ID, version=1, producer_commit=COMMIT)
    features, strategies = template_executables((ID,))
    trusted = TrustedExecutableRegistry(features=features, strategies=strategies)
    registry = ImmutableDefinitionRegistry(tmp_path / "definitions", execution_registry=trusted)
    now = datetime(2026, 10, 5, tzinfo=UTC)
    contract = strategy_template_feature_contract(producer_commit=COMMIT)
    feature = registry.register_feature_contract(
        contract,
        registered_at=now,
        available_at=now,
        producer_commit=COMMIT,
        expected_fingerprint=contract.contract_fingerprint,
    )
    record = registry.register_strategy_spec(
        spec,
        feature_contract_fingerprint=feature.fingerprint,
        registered_at=now,
        available_at=now,
        producer_commit=COMMIT,
        expected_fingerprint=spec.spec_fingerprint,
    )
    assert registry.read_strategy_spec(record.fingerprint) == record
    assert record.execution_binding.runtime_evaluator_version == "strategy-template/v1"
    assert len(record.execution_binding.exit_rules) == 5
    other = compile_strategy_template(
        rules, strategy_id="template_" + "2" * 32, version=1, producer_commit=COMMIT
    )
    with pytest.raises(DefinitionReferenceError, match="missing"):
        registry.register_strategy_spec(
            other,
            feature_contract_fingerprint=feature.fingerprint,
            registered_at=now,
            available_at=now,
            producer_commit=COMMIT,
            expected_fingerprint=other.spec_fingerprint,
        )
