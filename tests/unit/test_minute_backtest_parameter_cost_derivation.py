"""Pure recipe cost derivation from the frozen original synthetic profile."""
from __future__ import annotations

import hashlib
import importlib
import json
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.minute_backtest_contracts import MinuteReplayExecutionProfile
from rquant.minute_backtest_parameters import MinuteGrowthParameters, MinuteParameterSet

FIXTURE = Path(__file__).resolve().parents[2] / (
    "data/verification/minute-engine-completion-20261007/"
    "core-implementation-01/behavior/daily-n_shape-bar_end.json"
)


def original_profile() -> MinuteReplayExecutionProfile:
    # Frozen raw profile only: this unit test grants no source/head authority.
    raw = FIXTURE.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == 'd670d16f860f0f11247c64ecf0985fc45316bb4e7dcaa6cb6f1180c618a958b8'
    return MinuteReplayExecutionProfile.model_validate_json(
        json.dumps(json.loads(raw)['frozen_input']['execution_profile']))


@pytest.mark.parametrize('entry_slippage_pct', [0.0005, 0.00012345])
def test_full_canonical_cost_is_rebuilt_for_exact_recipe_slippage(entry_slippage_pct: float) -> None:
    original = original_profile()
    parameters = MinuteParameterSet(parameters=MinuteGrowthParameters(
        paper={'entry_slippage_pct': entry_slippage_pct}))
    module = importlib.import_module('rquant.minute_backtest_parameter_fact_sources')
    build = getattr(module, '_parameter_execution_profile', None)
    assert build is not None, 'typed exact canonical cost derivation is absent'
    profile = build(original, parameters)
    assert profile.execution_costs.slippage.buy_bps == Decimal(str(entry_slippage_pct)) * 10000
    assert profile.execution_costs.cost_spec_id == hashlib.sha256(
        profile.execution_costs.canonical_json().encode()).hexdigest()
    exclude = {'cost_spec_id': True, 'slippage': {'buy_bps'}}
    assert profile.execution_costs.model_dump(exclude=exclude) == original.execution_costs.model_dump(exclude=exclude)
    assert profile.model_dump(exclude={'execution_costs'}) == original.model_dump(exclude={'execution_costs'})


def test_identical_slippage_preserves_original_complete_profile_and_cost_id() -> None:
    original = original_profile()
    value = float(original.execution_costs.slippage.buy_bps / 10000)
    parameters = MinuteParameterSet(parameters=MinuteGrowthParameters(paper={'entry_slippage_pct': value}))
    module = importlib.import_module('rquant.minute_backtest_parameter_fact_sources')
    build = getattr(module, '_parameter_execution_profile', None)
    assert build is not None, 'typed exact canonical cost derivation is absent'
    assert build(original, parameters) == original
