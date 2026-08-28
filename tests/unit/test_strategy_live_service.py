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
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_shadow_validation import HmacCompletionAttestationAuthority
from rquant.signal_contracts import SignalAction
from rquant.signal_router_runtime import SignalRouteBacklogError
from rquant.strategy_candidate_feature_join import (
    StrategyCandidateFeatureJoinError,
    join_strategy_candidate_features,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshotSpool,
    asia_shanghai_trade_date,
    strategy_candidate_schema_fingerprint,
)
from rquant.strategy_live_service import (
    StrategyCompletionAttestationConfig,
    run_strategy_live_batch,
)
from rquant.strategy_runner import (
    RunnerSignalRouteDrainEvidence,
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
DEFINITION_FINGERPRINT = hashlib.sha256(b"n-shape-live:definition:v1").hexdigest()
EXECUTABLE_FINGERPRINT = "b" * 64
STATIC_FEATURE_SCHEMA = {
    "candidate_score": {"dtype": "number", "semantic": "candidate ranking score"}
}
CANDIDATE_SCHEMA_FINGERPRINT = strategy_candidate_schema_fingerprint(
    strategy_id="n-shape-live",
    strategy_version="1",
    static_feature_schema=STATIC_FEATURE_SCHEMA,
)
SESSION_CLOSE = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)
SOURCE_ID = "strategy.n-shape-live.v1"
ATTESTATION_AUTHORITY = HmacCompletionAttestationAuthority(
    key_id="strategy-live-test-key-v1",
    secret=b"strategy-live-test-completion-attestation-key",
)


def _completion_attestation() -> StrategyCompletionAttestationConfig:
    return StrategyCompletionAttestationConfig(
        signer=ATTESTATION_AUTHORITY,
        strategy_registration_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        feature_registration_fingerprint="1" * 64,
        feature_contract_fingerprint="2" * 64,
        producer_manifest_fingerprint="3" * 64,
    )


class _RouteAuthority:
    def __init__(self, *, drained: bool = True) -> None:
        self.drained = drained
        self.requests: list[tuple[str, str, str, date, int, int, datetime]] = []

    def read_drain_evidence(
        self,
        *,
        source_id: str,
        runner_generation_id: str,
        strategy_spec_fingerprint: str,
        trade_date: date,
        segment_start_sequence: int,
        routed_through_sequence: int,
        observed_at: datetime,
    ) -> RunnerSignalRouteDrainEvidence:
        self.requests.append(
            (
                source_id,
                runner_generation_id,
                strategy_spec_fingerprint,
                trade_date,
                segment_start_sequence,
                routed_through_sequence,
                observed_at,
            )
        )
        if not self.drained:
            raise SignalRouteBacklogError("router backlog")
        return RunnerSignalRouteDrainEvidence(
            source_id=source_id,
            runner_generation_id=runner_generation_id,
            strategy_spec_fingerprint=strategy_spec_fingerprint,
            signal_authority_generation_id="a" * 64,
            routing_policy_fingerprint="9" * 64,
            trade_date=trade_date,
            segment_start_sequence=segment_start_sequence,
            segment_record_count=routed_through_sequence - segment_start_sequence,
            segment_raw_bytes=max(routed_through_sequence - segment_start_sequence, 0),
            segment_chain_hash="7" * 64,
            observed_high_watermark=routed_through_sequence,
            routed_through_sequence=routed_through_sequence,
            last_sequence=routed_through_sequence,
            route_receipts_sha256="b" * 64,
            observed_at=observed_at,
        )


def _calendar(*, open_dates: tuple[date, ...] = (date(2026, 7, 31),)) -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="c" * 40,
        coverage_start=date(2026, 7, 1),
        coverage_end=date(2026, 8, 31),
        open_dates=open_dates,
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )


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
        decision_cutoff=available_at,
        actual_delay_seconds=0.0,
        row_count=len(frame),
        content_hash=hashlib.sha256(payload).hexdigest(),
        field_statuses=(
            FeatureFieldStatus(
                name="rel_same_minute",
                status=status,
                source_event_time=available_at,
                available_at=available_at,
                decision_cutoff=available_at,
                actual_delay_seconds=0.0,
                reason=None if status is FeatureAvailability.AVAILABLE else "source_empty",
            ),
        ),
        producer_commit=producer_commit,
    )
    spool.publish(envelope, payload)


def _seal_feature_session(spool: FeatureBatchSpool) -> None:
    record = spool.list_after(sequence=-1)[-1]
    spool.publish_session_close_marker(
        trade_date=date(2026, 7, 31),
        session_close_at=SESSION_CLOSE,
        produced_at=SESSION_CLOSE + timedelta(seconds=1),
        calendar_generation_id=_calendar().content_sha256,
        complete_through=SESSION_CLOSE,
        upstream_source_generation_id="f" * 64,
        upstream_final_sequence=record.envelope.sequence,
        upstream_final_batch_id=f"raw-{record.envelope.sequence}",
        upstream_final_content_hash="e" * 64,
    )


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
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
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
                    definition_fingerprint=DEFINITION_FINGERPRINT,
                    executable_fingerprint=EXECUTABLE_FINGERPRINT,
                    candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
                    static_feature_names=("candidate_score",),
                    static_feature_schema=STATIC_FEATURE_SCHEMA,
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


def test_service_publishes_close_receipt_only_after_sse_close_and_route_drain(
    tmp_path: Path,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features, available_at=SESSION_CLOSE)
    _seal_feature_session(features)
    runner = _runner(tmp_path / "runner.sqlite3")
    route_authority = _RouteAuthority()

    summary = run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=_candidate_loader(
            tmp_path,
            available_at=SESSION_CLOSE,
        ),
        runner=runner,
        evaluator=_evaluator,
        observed_at=SESSION_CLOSE + timedelta(seconds=3),
        limit=10,
        calendar=_calendar(),
        route_authority=route_authority,
        completion_source_id=SOURCE_ID,
        producer_service_id="strategy-live",
        producer_instance_id="n-shape-live-primary",
        producer_version="0.27.0",
        completion_attestation=_completion_attestation(),
    )

    receipt = runner.session_close_receipt(date(2026, 7, 31))
    assert summary.completion_receipt_id == receipt.receipt_id
    assert receipt.calendar_generation_id == _calendar().content_sha256
    assert receipt.producer_commit == COMMIT
    assert route_authority.requests == [
        (
            SOURCE_ID,
            runner.source_generation_id,
            runner.spec.spec_fingerprint,
            date(2026, 7, 31),
            0,
            1,
            SESSION_CLOSE + timedelta(seconds=3),
        )
    ]


def test_service_does_not_self_declare_completion_without_feature_close_marker(
    tmp_path: Path,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features, available_at=SESSION_CLOSE)
    runner = _runner(tmp_path / "runner.sqlite3")
    route_authority = _RouteAuthority()

    summary = run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=_candidate_loader(
            tmp_path,
            available_at=SESSION_CLOSE,
        ),
        runner=runner,
        evaluator=_evaluator,
        observed_at=SESSION_CLOSE + timedelta(seconds=3),
        limit=10,
        calendar=_calendar(),
        route_authority=route_authority,
        completion_source_id=SOURCE_ID,
        producer_service_id="strategy-live",
        producer_instance_id="n-shape-live-primary",
        producer_version="0.27.0",
        completion_attestation=_completion_attestation(),
    )

    assert summary.completion_receipt_id is None
    assert route_authority.requests == []
    assert runner.session_close_receipt(date(2026, 7, 31)) is None


@pytest.mark.parametrize(
    ("observed_at", "open_dates", "drained"),
    [
        (SESSION_CLOSE - timedelta(minutes=1), (date(2026, 7, 31),), True),
        (SESSION_CLOSE + timedelta(seconds=3), (date(2026, 7, 31),), False),
        (SESSION_CLOSE + timedelta(days=1), (date(2026, 7, 31),), True),
    ],
)
def test_service_does_not_publish_for_1459_backlog_or_closed_day(
    tmp_path: Path,
    observed_at: datetime,
    open_dates: tuple[date, ...],
    drained: bool,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    feature_time = min(observed_at, SESSION_CLOSE)
    _publish(features, available_at=feature_time)
    runner = _runner(tmp_path / "runner.sqlite3")
    route_authority = _RouteAuthority(drained=drained)

    summary = run_strategy_live_batch(
        feature_spool=features,
        candidate_universe_loader=_candidate_loader(
            tmp_path,
            available_at=feature_time,
        ),
        runner=runner,
        evaluator=_evaluator,
        observed_at=observed_at,
        limit=10,
        calendar=_calendar(open_dates=open_dates),
        route_authority=route_authority,
        completion_source_id=SOURCE_ID,
        producer_service_id="strategy-live",
        producer_instance_id="n-shape-live-primary",
        producer_version="0.27.0",
        completion_attestation=_completion_attestation(),
    )

    assert summary.completion_receipt_id is None
    assert runner.session_close_receipt(date(2026, 7, 31)) is None
    assert runner.signal_high_watermark() == 1
    assert runner.candidate_state("600000.SH").state is StrategyLifecycleState.ARMED  # type: ignore[union-attr]
    stored = features.read_result(features.list_after(sequence=-1, limit=1)[0])
    universe = _candidate_loader(
        tmp_path,
        available_at=stored.envelope.available_at,
        root_name="expected-candidates",
    ).load(
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


def test_service_rejects_calendar_not_visible_at_completion_cutoff(
    tmp_path: Path,
) -> None:
    features = FeatureBatchSpool(tmp_path / "features")
    _publish(features, available_at=SESSION_CLOSE)
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="c" * 40,
        coverage_start=date(2026, 7, 1),
        coverage_end=date(2026, 8, 31),
        open_dates=(date(2026, 7, 31),),
        generated_at=SESSION_CLOSE + timedelta(minutes=1),
    )

    with pytest.raises(ValueError, match="calendar.*after|generated"):
        run_strategy_live_batch(
            feature_spool=features,
            candidate_universe_loader=_candidate_loader(
                tmp_path,
                available_at=SESSION_CLOSE,
            ),
            runner=_runner(tmp_path / "runner.sqlite3"),
            evaluator=_evaluator,
            observed_at=SESSION_CLOSE + timedelta(seconds=3),
            limit=10,
            calendar=calendar,
            route_authority=_RouteAuthority(),
            completion_source_id=SOURCE_ID,
            producer_service_id="strategy-live",
            producer_instance_id="n-shape-live-primary",
            producer_version="0.27.0",
            completion_attestation=_completion_attestation(),
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
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
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
