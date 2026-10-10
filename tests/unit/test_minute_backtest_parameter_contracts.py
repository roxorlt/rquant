from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.minute_backtest_contracts import MinuteReplayWork
from rquant.minute_backtest_parameter_contracts import (
    MinuteParameterStrategyBinding,
    MinuteParameterWork,
)
from rquant.minute_backtest_parameter_definition import (
    bootstrap_minute_parameter_definition, bootstrap_minute_parameter_research_definition,
    build_minute_parameter_research_definition,
)
from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet

COMMIT = "a" * 40
AT = datetime(2025, 1, 2, tzinfo=UTC)


def test_parameter_binding_requires_the_real_complete_registration(tmp_path: Path) -> None:
    recipe = MinuteParameterSet(parameters=MinuteNShapeParameters(paper={"stop_loss_pct": 0.012345}))
    registration = bootstrap_minute_parameter_definition(tmp_path / "definitions", recipe,
        producer_commit=COMMIT, registered_at=AT, available_at=AT)
    binding = MinuteParameterStrategyBinding.from_registration(registration, parameters=recipe,
        producer_commit=COMMIT)
    assert binding.strategy_id == recipe.definition_id
    assert binding.registration_fingerprint == registration.fingerprint
    assert binding.executable_fingerprint == registration.executable_fingerprint
    changed = MinuteParameterSet(parameters=MinuteNShapeParameters(paper={"stop_loss_pct": 0.012346}))
    with pytest.raises(ValueError, match="complete parameter"):
        MinuteParameterStrategyBinding.from_registration(registration, parameters=changed,
            producer_commit=COMMIT)


def test_parameter_projection_work_stays_inside_the_original_total_budget() -> None:
    runtime = MinuteReplayWork(raw_rows=3, warmup_rows=20, static_rows=7,
        market_batches=3, union_codes=1, daily_observations=1)
    value = MinuteParameterWork(runtime_work=runtime, prefix_rows=6,
        history_rows=60, derived_rows=4, lifecycle_rows=2)
    assert value.work_units == runtime.work_units + 72
    with pytest.raises(ValidationError, match="20000"):
        MinuteParameterWork(runtime_work=runtime, prefix_rows=20_000,
            history_rows=60, derived_rows=4, lifecycle_rows=2)


def test_parameter_research_wrapper_has_its_own_real_registration(tmp_path: Path) -> None:
    recipe = MinuteParameterSet(parameters=MinuteNShapeParameters())
    root = tmp_path / "definitions"
    native = bootstrap_minute_parameter_definition(root, recipe,
        producer_commit=COMMIT, registered_at=AT, available_at=AT)
    wrapper = bootstrap_minute_parameter_research_definition(root, recipe, producer_commit=COMMIT, now=AT)
    expected = build_minute_parameter_research_definition(COMMIT)
    assert wrapper.logical_id == "minute_parameter_replay"
    assert wrapper.logical_id != native.logical_id and wrapper.version == 1
    assert wrapper.feature_contract_fingerprint == native.feature_contract_fingerprint
    assert wrapper.spec.spec_fingerprint == expected.spec.spec_fingerprint
    assert wrapper.executable_fingerprint == expected.executable_fingerprint
