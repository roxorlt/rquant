"""auction_gap after its first entry: the lifecycle features' delay bound (package AI, F3).

The paper-execution lifecycle features were published with `max_delay_seconds=1`,
`fail_closed`, while their delay is the evidence's own `available_at - source_event_time`:
for a pending entry that is the entry signal minted from a minute bar (the fixture day
replay measured 14 s), for the eligible high it is the bar's `session_high` (11 s). So
from the first entry signal every later round of `strategy.auction_gap.v1` failed (333 of
350 in the fixture day) and no exit could be evaluated.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.feature_contracts import FeatureAvailability, FeatureFieldStatus
from rquant.intraday_feature_engine import MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS
from rquant.runtime_definition_bootstrap import (
    _LIFECYCLE_FEATURES,
    EXECUTION_LIFECYCLE_MAX_DELAY_SECONDS,
    _feature_contracts,
)
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_runner import StrategyRunnerStore

COMMIT = "a" * 40
CUTOFF = datetime(2026, 9, 28, 1, 40, 14, tzinfo=UTC)


def _contract():
    return _feature_contracts(
        BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT), producer_commit=COMMIT
    )[-1]


def test_the_lifecycle_bound_is_two_minute_bar_cadences_and_still_fails_closed() -> None:
    assert EXECUTION_LIFECYCLE_MAX_DELAY_SECONDS == 2 * MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS
    assert EXECUTION_LIFECYCLE_MAX_DELAY_SECONDS == 120
    lifecycle = [feature for feature in _contract().features if feature.name in _LIFECYCLE_FEATURES]
    assert {feature.name for feature in lifecycle} == set(_LIFECYCLE_FEATURES)
    for feature in lifecycle:
        availability = feature.availability_contract
        assert availability.max_delay_seconds == EXECUTION_LIFECYCLE_MAX_DELAY_SECONDS
        assert availability.late_policy.value == "fail_closed"
        assert availability.missing_policy.value == "fail_closed"


def _store(tmp_path: Path) -> StrategyRunnerStore:
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT)
    definition = registry.load_definition("auction_gap", 1)
    return StrategyRunnerStore(
        tmp_path / "runner.sqlite3",
        spec=definition.spec,
        evaluator_contract_fingerprint="e" * 64,
        feature_contract=_contract(),
    )


def _status(name: str, *, delay_seconds: float) -> FeatureFieldStatus:
    return FeatureFieldStatus(
        candidate_id="600000.SH",
        name=name,
        status=FeatureAvailability.AVAILABLE,
        source_event_time=CUTOFF - timedelta(seconds=delay_seconds),
        available_at=CUTOFF,
        decision_cutoff=CUTOFF,
        actual_delay_seconds=delay_seconds,
    )


@pytest.mark.parametrize(
    ("name", "delay_seconds"),
    [
        #: what the fixture day replay measured on the first two failing rounds
        ("entry_fill_status", 14.0),
        ("eligible_high_price_raw", 11.0),
        #: a signal minted from a bar at the market-minute bound, observed a loop later
        ("entry_fill_status", 62.0),
        ("structure_stop_price_raw", 120.0),
    ],
)
def test_lifecycle_evidence_at_minute_cadence_is_accepted(
    tmp_path: Path, name: str, delay_seconds: float
) -> None:
    _store(tmp_path)._validate_instance_availability(
        name, _status(name, delay_seconds=delay_seconds), observed_at=CUTOFF
    )


def test_lifecycle_evidence_beyond_the_bound_is_still_refused(tmp_path: Path) -> None:
    with pytest.raises(
        ValueError,
        match=r"feature entry_fill_status exceeds max_delay_seconds \(121 s > 120 s\)",
    ):
        _store(tmp_path)._validate_instance_availability(
            "entry_fill_status",
            _status("entry_fill_status", delay_seconds=121.0),
            observed_at=CUTOFF,
        )
