from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxStatus
from rquant.feature_contracts import FeatureRequirement, RequirementLevel
from rquant.feature_live_service import run_feature_live_batch
from rquant.feature_spool import FeatureBatchSpool
from rquant.intraday_feature_engine import IntradayFeatureConfig
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.notification_worker import NotificationDelivery, run_notification_batch
from rquant.paper_broker import BrokerCostPolicy, BrokerExecutionContext, PaperBrokerStore
from rquant.paper_signal_consumer import (
    PaperSignalConsumerStateStore,
    consume_signal_bus_to_paper,
)
from rquant.paper_signal_worker import (
    PaperQuoteSnapshot,
    PaperSignalPolicy,
    PaperSignalQueueStore,
    run_paper_signal_batch,
)
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseLoader,
)
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher, ServingReader
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingReadModelInput,
    ServingSignalRecord,
    build_serving_read_models,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction
from rquant.signal_router_runtime import (
    RoutingDecision,
    SignalRouteCursorStore,
    StrategyRunnerSignalSource,
    route_runner_signals,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshotSpool,
    strategy_candidate_schema_fingerprint,
)
from rquant.strategy_live_service import run_strategy_live_batch
from rquant.strategy_runner import StrategyCandidateState, StrategyDecision, StrategyRunnerStore
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)

SHANGHAI = timezone(timedelta(hours=8))
OBSERVED = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)
EXECUTION_TIME = OBSERVED + timedelta(minutes=1)
TRADE_DATE = date(2026, 7, 31)
POLICY_FINGERPRINT = "9" * 64
DEFINITION_FINGERPRINT = "a" * 64
EXECUTABLE_FINGERPRINT = "4" * 64
STATIC_FEATURE_SCHEMA = {
    "candidate_score": {"dtype": "number", "semantic": "candidate ranking score"}
}
CANDIDATE_SCHEMA_FINGERPRINT = strategy_candidate_schema_fingerprint(
    strategy_id="isolated-e2e",
    strategy_version="1",
    static_feature_schema=STATIC_FEATURE_SCHEMA,
)


def _current_minute() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": "2026-07-31 09:40:00",
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 1_000.0,
                "amount": 10_000.0,
            }
        ]
    )


def _history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": datetime(2026, 7, day, 9, 40, tzinfo=SHANGHAI),
                "available_at": datetime(2026, 7, day, 9, 40, 2, tzinfo=SHANGHAI),
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.0,
                "vol": amount / 10.0,
                "amount": amount,
            }
            for day, amount in ((29, 4_000.0), (30, 6_000.0))
        ]
    )


def _spec() -> StrategySpec:
    return StrategySpec(
        strategy_id="isolated-e2e",
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
        parameters={"min_ratio": 1.4},
        allowed_actions=(SignalAction.B_INTENT.value,),
        run_mode=StrategyRunMode.PAPER,
        producer_commit="3" * 40,
    )


def _evaluate(
    spec: StrategySpec,
    state: StrategyCandidateState,
    features: dict[str, object],
) -> StrategyDecision | None:
    ratio = float(features["rel_same_minute"])
    if ratio < float(spec.parameters["min_ratio"]):
        return None
    return StrategyDecision(
        event="entry_ready",
        expected_from_state=state.state,
        expected_to_state=StrategyLifecycleState.ARMED,
        expected_action=SignalAction.B_INTENT,
        action=SignalAction.B_INTENT,
        reason_codes=("same_minute_volume",),
        evidence={"rel_same_minute": ratio},
        expires_after=timedelta(minutes=5),
    )


class _Provider:
    def __init__(self) -> None:
        self.calls: list[NotificationDelivery] = []

    def deliver(self, delivery: NotificationDelivery) -> str:
        self.calls.append(delivery)
        return "pushdeer:e2e-receipt"


def _paper_policy() -> PaperSignalPolicy:
    return PaperSignalPolicy(
        account_id="paper-main",
        execution_lag=timedelta(minutes=1),
        action_quantities={
            SignalAction.B_INTENT: 1_000,
            SignalAction.REDUCE: 500,
            SignalAction.S_INTENT: 1_000,
        },
        producer_commit="5" * 40,
    )


def _candidate_loader(tmp_path: Path) -> RuntimeCandidateUniverseLoader:
    root = (tmp_path / "candidate-snapshots").resolve()
    decision_at = OBSERVED - timedelta(days=1)
    StrategyCandidateSnapshotSpool(root).publish_strategy_records(
        strategy_id=_spec().strategy_id,
        strategy_version=str(_spec().version),
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate_input": "8" * 64},
        trade_date=TRADE_DATE,
        captured_at=OBSERVED,
        producer_commit="3" * 40,
        rows=(
            StrategyCandidateRecord(
                strategy_id=_spec().strategy_id,
                strategy_version=str(_spec().version),
                candidate_id="600000.SH",
                variant="default",
                decision_at=decision_at,
                available_at=decision_at + timedelta(minutes=1),
                effective_trade_date=TRADE_DATE,
                reference_trade_date=date(2026, 7, 30),
                price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
                static_features={"candidate_score": 0.95},
                reference_snapshot_ids={"daily": "8" * 64},
            ),
            StrategyCandidateRecord(
                strategy_id=_spec().strategy_id,
                strategy_version=str(_spec().version),
                candidate_id="600001.SH",
                variant="default",
                decision_at=decision_at,
                available_at=decision_at + timedelta(minutes=1),
                effective_trade_date=TRADE_DATE,
                reference_trade_date=date(2026, 7, 30),
                price_basis=StrategyCandidatePriceBasis.QFQ_PIT,
                static_features={"candidate_score": 0.85},
                reference_snapshot_ids={"daily": "8" * 64},
            ),
        ),
    )
    return RuntimeCandidateUniverseLoader(
        RuntimeCandidateUniverseConfig(
            expected_commit="3" * 40,
            authorities=(
                CandidateUniverseAuthority(
                    strategy_id=_spec().strategy_id,
                    strategy_version=str(_spec().version),
                    snapshot_root=root,
                    required=True,
                    max_age_seconds=300,
                    definition_fingerprint=DEFINITION_FINGERPRINT,
                    executable_fingerprint=EXECUTABLE_FINGERPRINT,
                    candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
                    static_feature_names=("candidate_score",),
                    static_feature_schema=STATIC_FEATURE_SCHEMA,
                ),
            ),
        )
    )


def test_pooled_candidate_loader_preserves_exact_strategy_identity(tmp_path: Path) -> None:
    result = _candidate_loader(tmp_path).load(
        as_of=OBSERVED,
        required_trade_date=TRADE_DATE,
    )

    assert result.codes == ("600000.SH", "600001.SH")
    assert len(result.authorities) == 1
    evidence = result.authorities[0]
    assert evidence.definition_fingerprint == DEFINITION_FINGERPRINT
    assert evidence.executable_fingerprint == EXECUTABLE_FINGERPRINT
    assert evidence.candidate_schema_fingerprint == CANDIDATE_SCHEMA_FINGERPRINT


def test_pipeline_is_end_to_end_and_exact_replay_is_idempotent(tmp_path: Path) -> None:
    live_spool = LiveBatchSpool(tmp_path / "live")
    gateway = MarketMinuteGateway(
        spool=live_spool,
        fetcher=_current_minute,
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-v1",
            producer_commit="1" * 40,
        ),
    )
    gateway.capture_once(received_at=OBSERVED)

    feature_spool = FeatureBatchSpool(tmp_path / "features")
    feature_summary = run_feature_live_batch(
        raw_spool=live_spool,
        feature_spool=feature_spool,
        historical_minutes=_history(),
        historical_snapshot_id="history-20260730",
        config=IntradayFeatureConfig(
            lookback_sessions=2,
            opening_acceleration_block_minutes=3,
            producer_commit="3" * 40,
        ),
        observed_at=OBSERVED,
        limit=10,
    )
    assert feature_summary.processed_count == 1

    runner_path = tmp_path / "runner.sqlite3"
    runner = StrategyRunnerStore(
        runner_path,
        spec=_spec(),
        evaluator_contract_fingerprint="4" * 64,
    )
    strategy_summary = run_strategy_live_batch(
        feature_spool=feature_spool,
        candidate_universe_loader=_candidate_loader(tmp_path),
        runner=runner,
        evaluator=_evaluate,
        observed_at=OBSERVED,
        limit=10,
    )
    assert strategy_summary.signal_count == 1

    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    source_id = "isolated-e2e/v1"
    route_summary = route_runner_signals(
        source_id=source_id,
        source=StrategyRunnerSignalSource(source_id=source_id, store=runner),
        bus=bus,
        cursors=SignalRouteCursorStore(
            tmp_path / "router-compat.sqlite3",
            routing_policy_fingerprint=POLICY_FINGERPRINT,
        ),
        routed_at=OBSERVED,
        target_resolver=lambda _signal: RoutingDecision.route(
            routing_policy_fingerprint=POLICY_FINGERPRINT,
            targets=(
                DeliveryTarget(
                    recipient_id="admin",
                    channel=DeliveryChannel.PUSHDEER,
                ),
            ),
        ),
        limit=10,
    )
    assert route_summary.routed_count == 1

    provider = _Provider()
    notification = run_notification_batch(
        bus,
        {DeliveryChannel.PUSHDEER: provider},
        worker_id="notifier-e2e",
        now=OBSERVED,
        lease_for=timedelta(seconds=30),
        limit=10,
        clock=lambda: OBSERVED + timedelta(seconds=1),
    )
    assert notification.succeeded_count == 1

    paper_queue_path = tmp_path / "paper-queue.sqlite3"
    paper_queue = PaperSignalQueueStore(
        paper_queue_path,
        policy=_paper_policy(),
    )
    paper_state_path = tmp_path / "paper-consumer.sqlite3"
    paper_state = PaperSignalConsumerStateStore(paper_state_path)
    consumed = consume_signal_bus_to_paper(
        bus,
        paper_queue,
        paper_state,
        observed_at=OBSERVED,
        limit=10,
    )
    assert consumed.delegated_count == 1

    broker = PaperBrokerStore(
        tmp_path / "paper-broker.sqlite3",
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        cost_policy=BrokerCostPolicy(
            commission_rate=Decimal("0.0003"),
            minimum_commission=Decimal("5"),
            sell_stamp_tax_rate=Decimal("0.001"),
        ),
    )
    paper_run = run_paper_signal_batch(
        paper_queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda signal, _now: PaperQuoteSnapshot(
            ts_code=signal.candidate_id,
            event_time=EXECUTION_TIME,
            available_at=EXECUTION_TIME,
            context=BrokerExecutionContext(
                executable_price=Decimal("10.10"),
                acquisition_available_date=date(2026, 8, 3),
            ),
            producer_commit="6" * 40,
        ),
        limit=10,
    )
    assert paper_run.completed_count == 1

    source_descriptor = bus.source_descriptor()
    signals = bus.signals_after_global_sequence(
        after_sequence=0,
        through_sequence=source_descriptor.high_watermark,
        observed_at=EXECUTION_TIME,
        limit=10,
    )
    serving_input = ServingReadModelInput(
        observed_at=EXECUTION_TIME,
        signals=tuple(
            ServingSignalRecord(global_sequence=item.global_sequence, signal=item.signal)
            for item in signals
        ),
        routes=bus.route_receipts(source_id),
        deliveries=bus.outbox_records(),
        paper_accounts=(
            broker.account_snapshot(
                as_of=EXECUTION_TIME,
                market_prices={"600000.SH": Decimal("10.20")},
            ),
        ),
    )
    serving_root = tmp_path / "serving"
    serving_manifest = ServingPublisher(
        serving_root,
        producer_commit="7" * 40,
        table_specs=SERVING_TABLE_SPECS,
    ).publish(
        build_serving_read_models(serving_input),
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="signal_bus",
                generation_id=source_descriptor.generation_id,
                event_time=OBSERVED,
                published_at=EXECUTION_TIME,
                sequence=source_descriptor.high_watermark,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"signal_bus": source_descriptor.generation_id},
        built_at=EXECUTION_TIME,
    )
    with ServingReader(serving_root).open_current_readonly() as connection:
        assert connection.execute("SELECT count(*) FROM signals").fetchone() == (1,)
        assert connection.execute("SELECT status FROM deliveries").fetchone() == (
            OutboxStatus.SUCCEEDED.value,
        )
        assert connection.execute("SELECT quantity FROM paper_holdings").fetchone() == (1_000,)
    assert serving_manifest.row_counts["paper_accounts"] == 1

    assert (
        run_feature_live_batch(
            raw_spool=live_spool,
            feature_spool=feature_spool,
            historical_minutes=_history(),
            historical_snapshot_id="history-20260730",
            config=IntradayFeatureConfig(
                lookback_sessions=2,
                opening_acceleration_block_minutes=3,
                producer_commit="3" * 40,
            ),
            observed_at=EXECUTION_TIME,
            limit=10,
        ).processed_count
        == 0
    )
    assert (
        run_strategy_live_batch(
            feature_spool=feature_spool,
            candidate_universe_loader=_candidate_loader(tmp_path),
            runner=StrategyRunnerStore(
                runner_path,
                spec=_spec(),
                evaluator_contract_fingerprint="4" * 64,
            ),
            evaluator=_evaluate,
            observed_at=EXECUTION_TIME,
            limit=10,
        ).processed_count
        == 0
    )
    assert (
        route_runner_signals(
            source_id=source_id,
            source=StrategyRunnerSignalSource(source_id=source_id, store=runner),
            bus=bus,
            cursors=SignalRouteCursorStore(
                tmp_path / "router-compat.sqlite3",
                routing_policy_fingerprint=POLICY_FINGERPRINT,
            ),
            routed_at=EXECUTION_TIME,
            target_resolver=lambda _signal: RoutingDecision.no_target(
                routing_policy_fingerprint=POLICY_FINGERPRINT,
                reason_code="must_not_re_evaluate",
            ),
            limit=10,
        ).routed_count
        == 0
    )
    assert (
        run_notification_batch(
            bus,
            {DeliveryChannel.PUSHDEER: provider},
            worker_id="notifier-e2e-replay",
            now=EXECUTION_TIME,
            lease_for=timedelta(seconds=30),
            limit=10,
            clock=lambda: EXECUTION_TIME,
        ).claimed_count
        == 0
    )
    assert (
        consume_signal_bus_to_paper(
            bus,
            PaperSignalQueueStore(
                paper_queue_path,
                policy=_paper_policy(),
            ),
            PaperSignalConsumerStateStore(paper_state_path),
            observed_at=EXECUTION_TIME,
            limit=10,
        ).delegated_count
        == 0
    )
    assert len(provider.calls) == 1
    assert len(broker.fills()) == 1
