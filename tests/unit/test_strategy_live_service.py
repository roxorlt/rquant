from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
    FeatureRequirement,
    RequirementLevel,
)
from rquant.feature_spool import FeatureBatchSpool
from rquant.signal_contracts import SignalAction
from rquant.strategy_live_service import run_strategy_live_batch
from rquant.strategy_runner import (
    StrategyCandidateState,
    StrategyDecision,
    StrategyRunnerStore,
    canonical_feature_payload,
)
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)

NOW = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)


def _spec() -> StrategySpec:
    return StrategySpec(
        strategy_id="n-shape-live",
        version=1,
        feature_contract_id="intraday-pit",
        min_feature_contract_version=2,
        required_features=(
            FeatureRequirement(
                name="rel_same_minute",
                level=RequirementLevel.REQUIRED,
                min_contract_version=2,
            ),
        ),
        optional_features=(),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
        ),
        parameters={"threshold": 1.4},
        allowed_actions=(SignalAction.B_INTENT.value,),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit="a" * 40,
    )


def _runner(path: Path) -> StrategyRunnerStore:
    return StrategyRunnerStore(
        path,
        spec=_spec(),
        evaluator_contract_fingerprint="b" * 64,
    )


def _evaluator(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: dict[str, object],
) -> StrategyDecision | None:
    if float(features["rel_same_minute"]) <= float(spec.parameters["threshold"]):
        return None
    return StrategyDecision(
        event="entry_ready",
        expected_from_state=state.state,
        expected_to_state=StrategyLifecycleState.ARMED,
        expected_action=SignalAction.B_INTENT,
        action=SignalAction.B_INTENT,
        reason_codes=("same_minute_volume",),
        evidence={"rel_same_minute": float(features["rel_same_minute"])},
        expires_after=timedelta(minutes=2),
    )


def _publish(
    spool: FeatureBatchSpool,
    *,
    sequence: int = 0,
    available_at: datetime = NOW,
    rows: list[dict[str, object]] | None = None,
    status: FeatureAvailability = FeatureAvailability.AVAILABLE,
) -> None:
    frame = pd.DataFrame(
        rows if rows is not None else [{"ts_code": "600000.SH", "rel_same_minute": 2.0}]
    )
    if frame.empty:
        frame = pd.DataFrame(columns=["ts_code", "rel_same_minute"])
    payload = canonical_feature_payload(frame, schema_version=2)
    envelope = FeatureBatchEnvelope(
        schema_version=2,
        batch_id=f"feature-{sequence}",
        contract_id="intraday-pit",
        contract_version=2,
        input_batch_ids=(f"minute-{sequence}", "history-snapshot"),
        sequence=sequence,
        event_time=available_at,
        available_at=available_at,
        row_count=len(frame),
        content_hash=hashlib.sha256(payload).hexdigest(),
        field_statuses=(
            FeatureFieldStatus(
                name="rel_same_minute",
                status=status,
                available_at=available_at,
                reason=None if status is FeatureAvailability.AVAILABLE else "source_empty",
            ),
        ),
        producer_commit="c" * 40,
    )
    spool.publish(envelope, payload)


def test_service_processes_visible_feature_once_and_emits_runner_signal(tmp_path: Path) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features)
    runner = _runner(tmp_path / "runner.sqlite3")

    summary = run_strategy_live_batch(
        feature_spool=features,
        runner=runner,
        evaluator=_evaluator,
        observed_at=NOW,
        limit=10,
    )

    assert summary.processed_count == 1
    assert summary.signal_count == 1
    assert summary.last_feature_sequence == 0
    assert runner.signal_high_watermark() == 1
    assert runner.candidate_state("600000.SH").state is StrategyLifecycleState.ARMED  # type: ignore[union-attr]


def test_crash_after_runner_commit_replays_without_duplicate_signal(
    tmp_path: Path,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features)
    runner_path = tmp_path / "runner.sqlite3"
    runner = _runner(runner_path)

    with pytest.raises(RuntimeError, match="injected crash"):
        run_strategy_live_batch(
            feature_spool=features,
            runner=runner,
            evaluator=_evaluator,
            observed_at=NOW,
            limit=10,
            fault_hook=lambda stage: (
                (_ for _ in ()).throw(RuntimeError("injected crash"))
                if stage == "after_runner_commit"
                else None
            ),
        )

    consumer_id = f"strategy:{_spec().strategy_id}:{_spec().version}"
    assert features.load_cursor(consumer_id) is None
    recovered = run_strategy_live_batch(
        feature_spool=features,
        runner=_runner(runner_path),
        evaluator=lambda *_args: pytest.fail("idempotent replay must not evaluate"),
        observed_at=NOW + timedelta(seconds=1),
        limit=10,
    )
    assert recovered.replayed_count == 1
    assert _runner(runner_path).signal_high_watermark() == 1


def test_future_feature_is_deferred_without_advancing_strategy_cursor(tmp_path: Path) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features, available_at=NOW + timedelta(minutes=1))

    summary = run_strategy_live_batch(
        feature_spool=features,
        runner=_runner(tmp_path / "runner.sqlite3"),
        evaluator=_evaluator,
        observed_at=NOW,
        limit=10,
    )

    assert summary.processed_count == 0
    assert summary.has_deferred_batches is True


def test_empty_unavailable_batch_is_audited_without_evaluator_or_signal(
    tmp_path: Path,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(
        features,
        rows=[],
        status=FeatureAvailability.UNAVAILABLE,
    )
    runner = _runner(tmp_path / "runner.sqlite3")

    summary = run_strategy_live_batch(
        feature_spool=features,
        runner=runner,
        evaluator=lambda *_args: pytest.fail("empty batch must not evaluate"),
        observed_at=NOW,
        limit=10,
    )

    assert summary.processed_count == 1
    assert summary.signal_count == 0
    assert runner.last_batch_sequence() == 0
