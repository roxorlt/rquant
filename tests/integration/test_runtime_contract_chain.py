from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxRecord, OutboxStatus
from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureContract,
    FeatureDefinition,
    FeatureFieldStatus,
    FeatureRequirement,
    RequirementLevel,
)
from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, LiveChannel
from rquant.paper_contracts import PaperOrderIntent, PaperOrderType, PaperSide
from rquant.research_run_spec import ResourceClass
from rquant.resource_admission import (
    AdmissionOutcome,
    AdmissionPolicy,
    AdmissionRequest,
    ResourceSnapshot,
    SourceQuotaLease,
    TradingSession,
    evaluate_admission,
)
from rquant.runtime_builder_candidate import (
    candidate_publisher_builder,
    serialize_candidate_input,
)
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseLoader,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.serving_contracts import (
    FreshnessStatus,
    ServingDatasetWatermark,
    ServingGenerationManifest,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.strategy_candidate_feature_join import join_strategy_candidate_features
from rquant.strategy_candidate_producers import (
    NShapePoolFact,
    PublishedCandidateInputAuthority,
)
from rquant.strategy_candidate_publish_service import NShapeCandidateBatch
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_runner import (
    StrategyCandidateState,
    canonical_feature_payload,
)
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)


def test_runtime_contract_chain_preserves_pit_identity_and_json_round_trips() -> None:
    event_time = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
    raw_available = event_time + timedelta(seconds=2)
    feature_available = raw_available + timedelta(seconds=1)
    signal_available = feature_available + timedelta(seconds=1)
    raw_payload = b"market-minute-parquet"
    raw_hash = hashlib.sha256(raw_payload).hexdigest()
    raw = BatchEnvelope(
        schema_version=1,
        channel=LiveChannel.MARKET_MINUTE,
        dataset_id="market_minute",
        source="tushare.rt_min",
        source_request_id="request-1",
        batch_id="raw-batch-1",
        sequence=1,
        revision=1,
        event_time_start=event_time,
        event_time_end=event_time,
        source_time=event_time + timedelta(seconds=1),
        received_at=raw_available,
        available_at=raw_available,
        row_count=1,
        content_sha256=raw_hash,
        quality_status=BatchQualityStatus.PUBLISHED,
        producer_version="market-minute-v1",
        producer_commit="a" * 40,
    )
    feature_contract = FeatureContract(
        contract_id="intraday-volume",
        version=1,
        features=(
            FeatureDefinition(
                name="rel_same_minute",
                dtype="float64",
                source_datasets=("market_minute",),
                lookback=20,
                pit_rule="input.available_at <= decision_time",
                price_basis="raw",
                availability_contract={
                    "source_available_at_basis": "max_source_available_at",
                    "max_delay_seconds": 60,
                    "missing_policy": "mark_unavailable",
                    "late_policy": "mark_stale",
                    "decision_visibility_gate": "available_at_lte_decision_time",
                },
            ),
        ),
        producer_commit="b" * 40,
    )
    feature_payload = b"feature-parquet"
    feature_hash = hashlib.sha256(feature_payload).hexdigest()
    feature = FeatureBatchEnvelope(
        schema_version=1,
        batch_id="feature-batch-1",
        contract_id=feature_contract.contract_id,
        contract_version=feature_contract.version,
        input_batch_ids=(raw.batch_id,),
        sequence=1,
        event_time=event_time,
        available_at=feature_available,
        decision_cutoff=feature_available,
        actual_delay_seconds=(feature_available - event_time).total_seconds(),
        row_count=1,
        content_hash=feature_hash,
        field_statuses=(
            FeatureFieldStatus(
                name="rel_same_minute",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=event_time,
                available_at=feature_available,
                decision_cutoff=feature_available,
                actual_delay_seconds=(feature_available - event_time).total_seconds(),
            ),
        ),
        producer_commit="b" * 40,
    )
    requirement = FeatureRequirement(
        name="rel_same_minute",
        level=RequirementLevel.REQUIRED,
        min_contract_version=1,
    )
    strategy = StrategySpec(
        strategy_id="growth-board-surge",
        version=1,
        feature_contract_id=feature_contract.contract_id,
        min_feature_contract_version=1,
        required_features=(requirement,),
        optional_features=(),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
        ),
        parameters={"min_ratio": Decimal("1.4")},
        allowed_actions=(SignalAction.B_INTENT.value,),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit="c" * 40,
    )
    signal = SignalEnvelope(
        schema_version=1,
        strategy_id=strategy.strategy_id,
        strategy_version=str(strategy.version),
        parameter_fingerprint=strategy.parameter_fingerprint,
        dataset_snapshot_id=raw.identity_sha256,
        feature_snapshot_id=feature_hash,
        event_time=event_time,
        available_at=signal_available,
        candidate_id="600000.SH",
        action=SignalAction.B_INTENT,
        reason_codes=("relative_volume_confirmed",),
        evidence={"rel_same_minute": 2.5},
        expires_at=signal_available + timedelta(minutes=5),
        producer_commit="c" * 40,
    )
    target = DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER)
    outbox = OutboxRecord(
        signal_id=signal.signal_id,
        target=target,
        status=OutboxStatus.PENDING,
        expires_at=signal.expires_at,
        attempt_count=0,
        next_attempt_at=signal_available,
        created_at=signal_available,
        updated_at=signal_available,
    )
    intent = PaperOrderIntent(
        signal_id=signal.signal_id,
        account_id="paper-main",
        ts_code=signal.candidate_id,
        side=PaperSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=100,
        event_time=signal.event_time,
        available_at=signal.available_at,
        expires_at=signal.expires_at,
        earliest_execution_at=signal.available_at,
        price_snapshot_id=feature_hash,
        producer_commit="c" * 40,
    )
    watermark = ServingDatasetWatermark(
        dataset_id="strategy_signal",
        generation_id=signal.signal_id,
        event_time=signal.event_time,
        published_at=signal.available_at,
        sequence=1,
        status=FreshnessStatus.FRESH,
    )
    serving = ServingGenerationManifest(
        schema_version=1,
        source_generations={"strategy_signal": signal.signal_id},
        watermarks=(watermark,),
        content_sha256="d" * 64,
        row_counts={"strategy_signal": 1},
        built_at=signal.available_at,
        producer_commit="e" * 40,
    )

    observed_at = signal.available_at
    request = AdmissionRequest(
        job_id="shadow-replay-1",
        resource_class=ResourceClass.STANDARD,
        expected_memory_bytes=100,
        expected_disk_bytes=100,
        expected_quota_units=1,
        source="tushare",
        preemptible=True,
        read_only=True,
        deadline=observed_at + timedelta(hours=1),
    )
    lease = SourceQuotaLease(
        source="tushare",
        owner=request.job_id,
        units=1,
        granted_at=observed_at - timedelta(seconds=1),
        expires_at=observed_at + timedelta(minutes=5),
        quota_reset_at=observed_at + timedelta(hours=1),
    )
    decision = evaluate_admission(
        request,
        ResourceSnapshot(
            observed_at=observed_at,
            session=TradingSession.POST_MARKET,
            live_backlog_age_seconds=0,
            live_p95_latency_seconds=0,
            available_memory_bytes=10_000,
            available_disk_bytes=10_000,
            io_pressure_pct=0,
            cpu_load_pct=0,
            source_quota_remaining=10,
            live_healthy=True,
        ),
        AdmissionPolicy(
            allow_live_session=False,
            max_live_backlog_age_seconds=10,
            max_live_p95_latency_seconds=5,
            min_available_memory_bytes=1_000,
            min_available_disk_bytes=1_000,
            max_io_pressure_pct=80,
            max_cpu_load_pct=80,
            max_expected_memory_bytes=1_000,
            max_expected_disk_bytes=1_000,
            max_expected_quota_units=10,
            retry_delay_seconds=60,
        ),
        lease,
    )

    assert raw.available_at <= feature.available_at <= signal.available_at
    assert intent.signal_id == outbox.signal_id == signal.signal_id
    assert serving.watermarks[0].generation_id == signal.signal_id
    assert decision.outcome is AdmissionOutcome.ADMITTED
    for model in (raw, feature_contract, feature, strategy, signal, outbox, intent, serving):
        assert type(model).model_validate_json(model.model_dump_json()) == model


def test_candidate_publisher_runtime_to_joined_builtin_evaluator_chain(
    tmp_path: Path,
) -> None:
    commit = "a" * 40
    captured_at = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
    event_time = captured_at + timedelta(minutes=1)
    available_at = event_time + timedelta(seconds=2)
    trade_date = date(2026, 7, 31)
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=commit)
    definition = registry.load_definition("n_shape", 1)
    static_schema = {
        name: semantic.contract_payload()
        for name, semantic in definition.static_feature_schema.items()
    }
    authority = PublishedCandidateInputAuthority(
        trade_date=trade_date,
        captured_at=captured_at,
        quality_status=BatchQualityStatus.PUBLISHED,
        authority_snapshot_id="1" * 64,
        producer_commit=commit,
    )
    batch = NShapeCandidateBatch(
        authority=authority,
        facts=(
            NShapePoolFact(
                ts_code="300001.SZ",
                variant="pool1",
                reference_trade_date=date(2026, 7, 29),
                t_close_raw=10.0,
                t_high_raw=11.0,
                reference_adj_factor=1.0,
                prior_session_trade_date=date(2026, 7, 30),
                expected_prior_session_trade_date=date(2026, 7, 30),
                prior_session_close_raw=10.0,
                prior_session_adj_factor=1.0,
                available_at=captured_at,
                reference_snapshot_ids={
                    "pool": "2" * 64,
                    "daily": "3" * 64,
                    "adj_factor": "4" * 64,
                    "session": "5" * 64,
                    "status": "6" * 64,
                    "limit": "7" * 64,
                    "trade_calendar": "8" * 64,
                },
                session_pre_close_raw=10.0,
                limit_pct=0.2,
                limit_up_price_session_raw=12.0,
                is_st=False,
                is_suspended=False,
                is_listed=True,
                limit_eligible=True,
            ),
        ),
    )
    input_path = (tmp_path / "n-shape-input.json").resolve()
    input_path.write_bytes(serialize_candidate_input(batch))
    input_path.chmod(0o600)
    snapshot_root = (tmp_path / "candidate-authority").resolve()
    manifest = RuntimeServiceManifest(
        service_id="candidate.n-shape.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=30,
        stale_after_seconds=90,
        producer_commit=commit,
        settings={
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "definition_fingerprint": definition.spec.spec_fingerprint,
            "executable_fingerprint": definition.executable_fingerprint,
            "candidate_schema_fingerprint": definition.candidate_schema_fingerprint,
            "static_feature_schema": static_schema,
            "candidate_input_path": str(input_path),
            "snapshot_root": str(snapshot_root),
        },
    )

    publish_result = candidate_publisher_builder()(manifest)()
    loader = RuntimeCandidateUniverseLoader(
        RuntimeCandidateUniverseConfig(
            expected_commit=commit,
            authorities=(
                CandidateUniverseAuthority(
                    strategy_id="n_shape",
                    strategy_version="1",
                    snapshot_root=snapshot_root,
                    required=True,
                    max_age_seconds=300,
                    definition_fingerprint=definition.spec.spec_fingerprint,
                    executable_fingerprint=definition.executable_fingerprint,
                    candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
                    static_feature_names=tuple(sorted(static_schema)),
                    static_feature_schema=static_schema,
                ),
            ),
        )
    )
    universe = loader.load(as_of=available_at, required_trade_date=trade_date)
    common = pd.DataFrame(
        (
            {
                "ts_code": "300001.SZ",
                "latest_close": 11.2,
                "session_low": 10.05,
                "session_high": 11.3,
                "price_over_vwap": 1.02,
                "rel_same_minute": 2.1,
                "rel_cumulative": 1.8,
                "amount_accel_5m": 2.2,
                "amount_accel_10m": 1.7,
                "tick_rule_buy_sell_ratio_proxy": 0.4,
                "historical_sessions": 60,
            },
        )
    )
    common_payload = canonical_feature_payload(common, schema_version=3)
    common_envelope = FeatureBatchEnvelope(
        schema_version=3,
        batch_id="common-20260731-0931",
        contract_id="intraday-pit",
        contract_version=3,
        input_batch_ids=("market-minute-1",),
        sequence=1,
        event_time=event_time,
        available_at=available_at,
        decision_cutoff=available_at,
        actual_delay_seconds=2.0,
        row_count=1,
        content_hash=hashlib.sha256(common_payload).hexdigest(),
        field_statuses=tuple(
            FeatureFieldStatus(
                name=name,
                status=FeatureAvailability.AVAILABLE,
                source_event_time=event_time,
                available_at=available_at,
                decision_cutoff=available_at,
                actual_delay_seconds=2.0,
            )
            for name in sorted(set(common.columns) - {"ts_code"})
        ),
        producer_commit=commit,
    )
    joined = join_strategy_candidate_features(
        common_envelope,
        common,
        universe,
        "n_shape",
        "1",
    )
    features = joined.frame.iloc[0].to_dict()
    decision = definition.evaluator(
        definition.spec,
        StrategyCandidateState(
            strategy_spec_fingerprint=definition.spec.spec_fingerprint,
            candidate_id="300001.SZ",
            state=StrategyLifecycleState.IDLE,
            last_feature_sequence=-1,
            updated_at=available_at,
        ),
        features,
    )

    assert publish_result.processed_count == 1
    assert universe.codes == ("300001.SZ",)
    assert joined.candidate_authority.definition_fingerprint == (definition.spec.spec_fingerprint)
    assert joined.candidate_authority.executable_fingerprint == (definition.executable_fingerprint)
    assert joined.candidate_authority.candidate_schema_fingerprint == (
        definition.candidate_schema_fingerprint
    )
    assert decision is not None
    assert decision.action is SignalAction.B_INTENT
