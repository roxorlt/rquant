from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
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
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseIntegrityError,
    RuntimeCandidateUniverseLoader,
)
from rquant.signal_contracts import SignalAction
from rquant.strategy_candidate_feature_join import (
    StrategyCandidateFeatureJoinError,
    join_strategy_candidate_features,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshotSpool,
    asia_shanghai_trade_date,
)
from rquant.strategy_live_service import run_strategy_live_batch
from rquant.strategy_runner import (
    StrategyBatchConflictError,
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
COMMIT = "a" * 40


def _spec() -> StrategySpec:
    return StrategySpec(
        strategy_id="n-shape-live",
        version=1,
        feature_contract_id="intraday-pit",
        min_feature_contract_version=3,
        required_features=(
            FeatureRequirement(
                name="rel_same_minute",
                level=RequirementLevel.REQUIRED,
                min_contract_version=3,
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
        producer_commit=COMMIT,
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
    producer_commit: str = COMMIT,
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
        contract_version=3,
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
        producer_commit=producer_commit,
    )
    spool.publish(envelope, payload)


def _candidate_loader(
    tmp_path: Path,
    *,
    available_at: datetime = NOW,
    captured_at: datetime | None = None,
    snapshot_trade_date: date | None = None,
    candidate_id: str | None = "600000.SH",
    snapshot_commit: str = COMMIT,
    expected_commit: str = COMMIT,
    max_age_seconds: int = 60,
    root_name: str = "candidates",
) -> RuntimeCandidateUniverseLoader:
    captured = captured_at or available_at
    trade_date = snapshot_trade_date or asia_shanghai_trade_date(available_at)
    decision_at = available_at - timedelta(days=1)
    rows = (
        ()
        if candidate_id is None
        else (
            StrategyCandidateRecord(
                strategy_id=_spec().strategy_id,
                strategy_version=str(_spec().version),
                candidate_id=candidate_id,
                variant="default",
                decision_at=decision_at,
                available_at=decision_at + timedelta(minutes=1),
                effective_trade_date=trade_date,
                reference_trade_date=trade_date - timedelta(days=1),
                price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
                static_features={"candidate_score": 0.91},
                reference_snapshot_ids={"daily": "d" * 64},
            ),
        )
    )
    root = (tmp_path / root_name).resolve()
    StrategyCandidateSnapshotSpool(root).publish_strategy_records(
        strategy_id=_spec().strategy_id,
        strategy_version=str(_spec().version),
        source_snapshot_ids={"candidate_input": "e" * 64},
        trade_date=trade_date,
        captured_at=captured,
        producer_commit=snapshot_commit,
        rows=rows,
    )
    return RuntimeCandidateUniverseLoader(
        RuntimeCandidateUniverseConfig(
            expected_commit=expected_commit,
            authorities=(
                CandidateUniverseAuthority(
                    strategy_id=_spec().strategy_id,
                    strategy_version=str(_spec().version),
                    snapshot_root=root,
                    required=True,
                    max_age_seconds=max_age_seconds,
                ),
            ),
        )
    )


def test_service_processes_visible_feature_once_and_emits_runner_signal(tmp_path: Path) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features)
    runner = _runner(tmp_path / "runner.sqlite3")

    summary = run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=_candidate_loader(tmp_path),
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
    stored = features.read_result(features.list_after(sequence=-1, limit=1)[0])
    universe = _candidate_loader(tmp_path, root_name="expected-candidates").load(
        as_of=stored.envelope.available_at,
        required_trade_date=asia_shanghai_trade_date(stored.envelope.event_time),
    )
    joined = join_strategy_candidate_features(
        stored.envelope,
        stored.frame,
        universe,
        _spec().strategy_id,
        str(_spec().version),
    )
    assert runner.signals_after(sequence=0)[0].signal.dataset_snapshot_id == (
        joined.envelope.input_fingerprint
    )


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
            candidate_universe_loader=_candidate_loader(tmp_path),
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
        candidate_universe_loader=_candidate_loader(tmp_path),
        runner=_runner(runner_path),
        evaluator=lambda *_args: pytest.fail("idempotent replay must not evaluate"),
        observed_at=NOW + timedelta(seconds=1),
        limit=10,
    )
    assert recovered.replayed_count == 1
    assert _runner(runner_path).signal_high_watermark() == 1


def _publish_next_candidate_generation(root: Path) -> None:
    decision_at = NOW - timedelta(days=1)
    StrategyCandidateSnapshotSpool(root.resolve()).publish_strategy_records(
        strategy_id=_spec().strategy_id,
        strategy_version=str(_spec().version),
        source_snapshot_ids={"candidate_input": "f" * 64},
        trade_date=date(2026, 7, 31),
        captured_at=NOW,
        producer_commit=COMMIT,
        rows=(
            StrategyCandidateRecord(
                strategy_id=_spec().strategy_id,
                strategy_version=str(_spec().version),
                candidate_id="600000.SH",
                variant="default",
                decision_at=decision_at,
                available_at=decision_at + timedelta(minutes=1),
                effective_trade_date=date(2026, 7, 31),
                reference_trade_date=date(2026, 7, 30),
                price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
                static_features={"candidate_score": 0.99},
                reference_snapshot_ids={"daily": "d" * 64},
            ),
        ),
    )


def test_crash_replay_uses_durable_source_receipt_after_candidate_generation_changes(
    tmp_path: Path,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features)
    loader = _candidate_loader(tmp_path)
    runner_path = tmp_path / "runner.sqlite3"

    with pytest.raises(RuntimeError, match="injected crash"):
        run_strategy_live_batch(
            feature_spool=features,
            candidate_universe_loader=loader,
            runner=_runner(runner_path),
            evaluator=_evaluator,
            observed_at=NOW,
            limit=10,
            fault_hook=lambda _stage: (_ for _ in ()).throw(RuntimeError("injected crash")),
        )
    _publish_next_candidate_generation(tmp_path / "candidates")

    recovered = run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=loader,
        runner=_runner(runner_path),
        evaluator=lambda *_args: pytest.fail("receipt replay must not evaluate or rejoin"),
        observed_at=NOW + timedelta(seconds=1),
        limit=10,
    )

    cursor = features.load_cursor(f"strategy:{_spec().strategy_id}:{_spec().version}")
    assert recovered.replayed_count == 1
    assert recovered.signal_count == 0
    assert cursor is not None and cursor.last_sequence == 0
    assert _runner(runner_path).signal_high_watermark() == 1


def test_crash_replay_rejects_same_sequence_from_new_source_generation(
    tmp_path: Path,
) -> None:
    original = FeatureBatchSpool(tmp_path / "features-original")
    _publish(original)
    loader = _candidate_loader(tmp_path)
    runner_path = tmp_path / "runner.sqlite3"

    with pytest.raises(RuntimeError, match="injected crash"):
        run_strategy_live_batch(
            feature_spool=original,
            candidate_universe_loader=loader,
            runner=_runner(runner_path),
            evaluator=_evaluator,
            observed_at=NOW,
            limit=10,
            fault_hook=lambda _stage: (_ for _ in ()).throw(RuntimeError("injected crash")),
        )
    replacement = FeatureBatchSpool(tmp_path / "features-replacement")
    _publish(replacement)
    assert (
        original.source_descriptor().generation_id != replacement.source_descriptor().generation_id
    )

    with pytest.raises(StrategyBatchConflictError, match="source batch"):
        run_strategy_live_batch(
            feature_spool=replacement,
            candidate_universe_loader=loader,
            runner=_runner(runner_path),
            evaluator=lambda *_args: pytest.fail("mismatched source must fail before evaluation"),
            observed_at=NOW + timedelta(seconds=1),
            limit=10,
        )

    assert replacement.load_cursor(f"strategy:{_spec().strategy_id}:{_spec().version}") is None


def test_future_feature_is_deferred_without_advancing_strategy_cursor(tmp_path: Path) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features, available_at=NOW + timedelta(minutes=1))

    summary = run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=_candidate_loader(
            tmp_path,
            available_at=NOW + timedelta(minutes=1),
        ),
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
        candidate_universe_loader=_candidate_loader(tmp_path),
        runner=runner,
        evaluator=lambda *_args: pytest.fail("empty batch must not evaluate"),
        observed_at=NOW,
        limit=10,
    )

    assert summary.processed_count == 1
    assert summary.signal_count == 0
    assert runner.last_batch_sequence() == 0


def test_candidate_snapshot_after_common_available_at_is_not_visible(
    tmp_path: Path,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features)
    runner = _runner(tmp_path / "runner.sqlite3")
    loader = _candidate_loader(
        tmp_path,
        captured_at=NOW + timedelta(seconds=30),
    )

    with pytest.raises(RuntimeCandidateUniverseIntegrityError, match="not_visible"):
        run_strategy_live_batch(
            feature_spool=features,
            candidate_universe_loader=loader,
            runner=runner,
            evaluator=_evaluator,
            observed_at=NOW + timedelta(minutes=1),
            limit=10,
        )

    assert runner.last_batch_sequence() == -1
    assert features.load_cursor(f"strategy:{_spec().strategy_id}:{_spec().version}") is None


@pytest.mark.parametrize("failure", ["stale", "commit", "date"])
def test_candidate_authority_mismatch_fails_without_advancing_cursor(
    tmp_path: Path,
    failure: str,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features)
    kwargs: dict[str, object] = {"root_name": f"candidates-{failure}"}
    if failure == "stale":
        kwargs.update(captured_at=NOW - timedelta(seconds=61), max_age_seconds=60)
    elif failure == "commit":
        kwargs["snapshot_commit"] = "f" * 40
    else:
        kwargs["snapshot_trade_date"] = date(2026, 7, 30)
    runner = _runner(tmp_path / "runner.sqlite3")

    with pytest.raises(RuntimeCandidateUniverseIntegrityError):
        run_strategy_live_batch(
            feature_spool=features,
            candidate_universe_loader=_candidate_loader(tmp_path, **kwargs),
            runner=runner,
            evaluator=_evaluator,
            observed_at=NOW,
            limit=10,
        )

    assert runner.last_batch_sequence() == -1
    assert features.load_cursor(f"strategy:{_spec().strategy_id}:{_spec().version}") is None


def test_empty_candidate_feature_intersection_commits_empty_joined_batch(
    tmp_path: Path,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features)
    runner = _runner(tmp_path / "runner.sqlite3")

    summary = run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=_candidate_loader(
            tmp_path,
            candidate_id="600001.SH",
        ),
        runner=runner,
        evaluator=lambda *_args: pytest.fail("empty intersection must not evaluate"),
        observed_at=NOW,
        limit=10,
    )

    assert summary.processed_count == 1
    assert summary.signal_count == 0
    assert runner.last_batch_sequence() == 0
    assert runner.candidate_state("600000.SH") is None


def test_empty_required_candidate_authority_advances_empty_joined_batch(
    tmp_path: Path,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features)
    runner = _runner(tmp_path / "runner.sqlite3")

    summary = run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=_candidate_loader(tmp_path, candidate_id=None),
        runner=runner,
        evaluator=lambda *_args: pytest.fail("empty authority must not evaluate"),
        observed_at=NOW,
        limit=10,
    )

    cursor = features.load_cursor(f"strategy:{_spec().strategy_id}:{_spec().version}")
    assert summary.processed_count == 1
    assert summary.signal_count == 0
    assert runner.last_batch_sequence() == 0
    assert cursor is not None and cursor.last_sequence == 0


def test_join_failure_does_not_advance_common_cursor(tmp_path: Path) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features)
    runner = _runner(tmp_path / "runner.sqlite3")

    with pytest.raises(StrategyCandidateFeatureJoinError, match="commit"):
        run_strategy_live_batch(
            feature_spool=features,
            candidate_universe_loader=_candidate_loader(
                tmp_path,
                snapshot_commit="f" * 40,
                expected_commit="f" * 40,
            ),
            runner=runner,
            evaluator=_evaluator,
            observed_at=NOW,
            limit=10,
        )

    assert runner.last_batch_sequence() == -1
    assert features.load_cursor(f"strategy:{_spec().strategy_id}:{_spec().version}") is None


def test_prefix_live_processing_matches_single_pass_replay(tmp_path: Path) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features, sequence=0, available_at=NOW)
    _publish(features, sequence=1, available_at=NOW + timedelta(seconds=1))
    loader = _candidate_loader(tmp_path)

    def lifecycle_evaluator(
        spec: StrategySpec,
        state: StrategyCandidateState,
        feature_values: dict[str, object],
    ) -> StrategyDecision | None:
        if state.state is not StrategyLifecycleState.IDLE:
            return None
        return _evaluator(spec, state, feature_values)

    prefix = _runner(tmp_path / "prefix.sqlite3")
    run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=loader,
        runner=prefix,
        evaluator=lifecycle_evaluator,
        observed_at=NOW + timedelta(minutes=1),
        limit=1,
        consumer_id="strategy:prefix",
    )
    run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=loader,
        runner=prefix,
        evaluator=lifecycle_evaluator,
        observed_at=NOW + timedelta(minutes=1),
        limit=10,
        consumer_id="strategy:prefix",
    )

    single_pass = _runner(tmp_path / "single.sqlite3")
    run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=loader,
        runner=single_pass,
        evaluator=lifecycle_evaluator,
        observed_at=NOW + timedelta(minutes=1),
        limit=10,
        consumer_id="strategy:single",
    )

    assert prefix.signals_after(sequence=0) == single_pass.signals_after(sequence=0)
    occurrence_id = prefix.signals_after(sequence=0)[0].signal.evidence["runner_transition"][
        "candidate_occurrence_id"
    ]
    assert prefix.candidate_occurrence_state(occurrence_id) == (
        single_pass.candidate_occurrence_state(occurrence_id)
    )
