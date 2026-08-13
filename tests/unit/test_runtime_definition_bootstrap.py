from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.runtime_definition_bootstrap import (
    bootstrap_builtin_definitions,
    plan_builtin_definitions,
)
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

COMMIT = "a" * 40
NOW = datetime(2026, 8, 2, 8, 0, tzinfo=UTC)


def test_builtin_definition_plan_is_complete_and_content_addressed() -> None:
    plan = plan_builtin_definitions(producer_commit=COMMIT)
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT)

    assert plan.feature_contract_versions == (1, 2, 3)
    assert len(plan.feature_contract_fingerprints) == 3
    assert tuple(binding.strategy_id for binding in plan.strategies) == (
        "auction_gap",
        "growth_board_surge",
        "n_shape",
    )
    assert len({binding.registration_fingerprint for binding in plan.strategies}) == 3
    for binding in plan.strategies:
        definition = registry.load_definition(binding.strategy_id, binding.strategy_version)
        assert binding.candidate_schema_fingerprint == definition.candidate_schema_fingerprint
        assert binding.strategy_spec_fingerprint == definition.spec.spec_fingerprint
        assert binding.executable_fingerprint == definition.executable_fingerprint


def test_builtin_definition_bootstrap_is_idempotent_and_matches_plan(tmp_path: Path) -> None:
    root = tmp_path / "definitions"
    plan = plan_builtin_definitions(producer_commit=COMMIT)

    first = bootstrap_builtin_definitions(
        root,
        producer_commit=COMMIT,
        registered_at=NOW,
        available_at=NOW,
        expected_plan_id=plan.plan_id,
    )
    repeated = bootstrap_builtin_definitions(
        root,
        producer_commit=COMMIT,
        registered_at=NOW,
        available_at=NOW,
        expected_plan_id=plan.plan_id,
    )

    assert first == repeated == plan
    reader = ImmutableDefinitionRegistry(
        root,
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=COMMIT
        ).trusted_executable_registry(),
    )
    for binding in plan.strategies:
        record = reader.read_strategy_spec(binding.registration_fingerprint, as_of=NOW)
        assert record is not None
        assert record.candidate_schema_fingerprint == binding.candidate_schema_fingerprint
        assert record.executable_fingerprint == binding.executable_fingerprint


def test_builtin_definition_bootstrap_rejects_changed_plan_before_writing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "definitions"

    with pytest.raises(ValueError, match="plan id"):
        bootstrap_builtin_definitions(
            root,
            producer_commit=COMMIT,
            registered_at=NOW,
            available_at=NOW,
            expected_plan_id="f" * 64,
        )

    assert not root.exists()
