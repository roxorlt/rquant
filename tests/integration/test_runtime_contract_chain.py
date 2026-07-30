from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal

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
from rquant.serving_contracts import (
    FreshnessStatus,
    ServingDatasetWatermark,
    ServingGenerationManifest,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
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
        row_count=1,
        content_hash=feature_hash,
        field_statuses=(
            FeatureFieldStatus(
                name="rel_same_minute",
                status=FeatureAvailability.AVAILABLE,
                available_at=feature_available,
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
