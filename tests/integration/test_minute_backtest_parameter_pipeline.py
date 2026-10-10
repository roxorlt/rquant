from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from decimal import Decimal
import json

import pandas as pd

from rquant.feature_spool import FeatureBatchSpool
from rquant.minute_backtest_parameter_definition import (
    bootstrap_minute_parameter_definition, build_minute_parameter_definition,
    minute_parameter_feature_contract,
)
from rquant.minute_backtest_parameter_features import (
    PARAMETER_CANDIDATE_FEATURE, MinuteParameterCandidate, publish_minute_parameter_features,
)
from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet
from rquant.paper_broker import PaperBrokerStore
from rquant.strategy_paper_lifecycle import PaperBrokerLifecycleReader
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority, RuntimeCandidateUniverseConfig, RuntimeCandidateUniverseLoader,
)
from rquant.signal_contracts import SignalAction
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis, StrategyCandidateRecord, StrategyCandidateSnapshotSpool,
)
from rquant.strategy_live_service import run_strategy_live_batch
from rquant.strategy_runner import StrategyRunnerStore
from tests.paper_cost_fixtures import paper_cost_policy

COMMIT = "a" * 40
AT = datetime(2025, 1, 2, 2, 30, tzinfo=UTC)


def test_complete_parameter_definition_enters_original_physical_spool_loader_runner_and_recovers(tmp_path: Path) -> None:
    recipe = MinuteParameterSet(parameters=MinuteNShapeParameters(entry_mode="amount_surge",
        paper={"stop_loss_pct": 0.012345}))
    definition = build_minute_parameter_definition(recipe, producer_commit=COMMIT)
    registration = bootstrap_minute_parameter_definition(tmp_path / "definitions", recipe,
        producer_commit=COMMIT, registered_at=AT - timedelta(days=1), available_at=AT - timedelta(days=1))
    candidate = MinuteParameterCandidate(family="n_shape", parameter_hash=recipe.fingerprint,
        ts_code="600001.SH", pool="pool1", trade_date=date(2025, 1, 2), reference_date=date(2025, 1, 1),
        available_at=AT - timedelta(hours=1), t_close=10.0, t_high=10.2, limit_up_price=11.0)
    raw = pd.DataFrame([{"ts_code": candidate.ts_code, "trade_time": AT - timedelta(minutes=2-index),
        "available_at": AT - timedelta(minutes=2-index), "open": 10.3, "high": 10.6, "low": 10.15,
        "close": 10.5, "vol": amount/10.3, "amount": amount} for index, amount in enumerate((100.0, 100.0, 250.0))])
    schema = {name: value.contract_payload() for name, value in definition.static_feature_schema.items()}
    candidates = StrategyCandidateSnapshotSpool(tmp_path / "candidates")
    candidates.publish_strategy_records(strategy_id=definition.strategy_id, strategy_version="1",
        definition_fingerprint=registration.fingerprint, executable_fingerprint=registration.executable_fingerprint,
        candidate_schema_fingerprint=definition.candidate_schema_fingerprint, static_feature_schema=schema,
        source_snapshot_ids={"complete_parameter_set": recipe.fingerprint}, trade_date=candidate.trade_date,
        captured_at=candidate.available_at, producer_commit=COMMIT, rows=(StrategyCandidateRecord(
            strategy_id=definition.strategy_id, strategy_version="1", candidate_id=candidate.ts_code, variant=candidate.pool,
            decision_at=candidate.available_at, available_at=candidate.available_at,
            effective_trade_date=candidate.trade_date, reference_trade_date=candidate.reference_date,
            price_basis=StrategyCandidatePriceBasis.RAW, static_features={PARAMETER_CANDIDATE_FEATURE: candidate.model_dump_json()},
            reference_snapshot_ids={"complete_parameter_set": recipe.fingerprint}),))
    loader = RuntimeCandidateUniverseLoader(RuntimeCandidateUniverseConfig(expected_commit=COMMIT, authorities=(
        CandidateUniverseAuthority(strategy_id=definition.strategy_id, strategy_version="1", snapshot_root=candidates.root,
            required=True, max_age_seconds=7200, definition_fingerprint=registration.fingerprint,
            executable_fingerprint=registration.executable_fingerprint, candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
            static_feature_names=(PARAMETER_CANDIDATE_FEATURE,), static_feature_schema=schema),)))
    features = FeatureBatchSpool(tmp_path / "features")
    publish_minute_parameter_features(features, parameters=recipe, candidates=(candidate,), minutes=raw,
        historical_minutes=raw.iloc[:0], source_frequency="1min", decision_cutoff=AT,
        sequence=0, input_batch_ids=("original-synthetic-minute-prefix",), producer_commit=COMMIT)
    broker = PaperBrokerStore(tmp_path / "broker.sqlite3", account_id="parameter-test",
        initial_cash=Decimal("1000000"), cost_policy=paper_cost_policy())
    runner = StrategyRunnerStore(tmp_path / "runner.sqlite3", spec=definition.spec,
        evaluator_contract_fingerprint=definition.executable_fingerprint,
        feature_contract=minute_parameter_feature_contract(definition),
        lifecycle_feature_source=PaperBrokerLifecycleReader(broker.path, account_id=broker.account_id))
    summary = run_strategy_live_batch(feature_spool=features, candidate_universe_loader=loader,
        runner=runner, evaluator=definition.evaluator, observed_at=AT, limit=1)
    assert summary.processed_count == 1 and summary.signal_count == 1
    records = runner.signals_after(sequence=0)
    assert len(records) == 1 and records[0].signal.action is SignalAction.B_INTENT
    assert records[0].signal.evidence["minute_parameter_set_json"] == recipe.model_dump_json()
    recovered = StrategyRunnerStore(runner.path, spec=definition.spec,
        evaluator_contract_fingerprint=definition.executable_fingerprint,
        feature_contract=minute_parameter_feature_contract(definition),
        lifecycle_feature_source=PaperBrokerLifecycleReader(broker.path, account_id=broker.account_id))
    again = run_strategy_live_batch(feature_spool=features, candidate_universe_loader=loader,
        runner=recovered, evaluator=definition.evaluator, observed_at=AT, limit=1)
    assert again.processed_count == 0 and recovered.signals_after(sequence=0) == records


def test_complete_parameters_execute_original_queue_broker_t_plus_one_and_daily_nav(tmp_path: Path) -> None:
    from rquant.minute_backtest_parameter_runner import run_minute_parameter_replay
    from tests.support.minute_parameter_runtime_fixture import parameter_runtime_fixture

    recipe = MinuteParameterSet(parameters=MinuteNShapeParameters(
        paper={"stop_loss_pct": 0.012345, "entry_slippage_pct": 0.0002}))
    value, receipt = parameter_runtime_fixture(tmp_path / "fixture", recipe)
    result = run_minute_parameter_replay(value, expected=receipt, research_root=tmp_path / "actual-run")
    assert result.parameters == recipe and result.parameter_work == value.parameter_work
    assert result.status == "complete" and result.daily_status == "complete"
    assert any(signal.action is SignalAction.B_INTENT for signal in result.signals)
    assert any(signal.action is SignalAction.S_INTENT for signal in result.signals)
    assert len(result.fills) >= 2 and all(fill.total_fees > 0 for fill in result.fills)
    assert tuple(item.trade_date for item in result.daily_valuations) == value.daily_trade_dates
    buys = [order for order in result.orders if order.side.value == "BUY" and order.filled_quantity]
    sells = [order for order in result.orders if order.side.value == "SELL" and order.filled_quantity]
    assert buys and sells and all(order.created_at.date() > buys[0].created_at.date() for order in sells)
    assert all(signal.evidence["minute_parameter_set_hash"] == recipe.fingerprint for signal in result.signals)
    from rquant.minute_backtest_parameter_features import MinuteParameterLifecycle
    from rquant.minute_backtest_parameter_runner import minute_parameter_result_tables

    exit_signal = next(signal for signal in result.signals if signal.action is SignalAction.S_INTENT)
    lifecycle = MinuteParameterLifecycle.model_validate_json(exit_signal.evidence["minute_parameter_lifecycle"])
    assert lifecycle.entry_fill.price == Decimal("10.1220")
    assert lifecycle.position.entry_price == 10.12 and lifecycle.position.stop_loss_pct == 0.012345
    assert lifecycle.risk_basis == "original_paper_two_decimal"
    assert lifecycle.remaining_quantity == lifecycle.entry_fill.quantity
    cash = value.execution_profile.initial_cash
    by_order = {order.order_id: order for order in result.orders}
    for fill in result.fills:
        direction = Decimal(-1) if by_order[fill.order_id].side.value == "BUY" else Decimal(1)
        cash += direction * fill.price * fill.quantity - fill.total_fees
    assert result.account.cash == cash
    assert result.daily_valuations[-1].account.cash == cash
    tables = minute_parameter_result_tables(result)
    assert len(tables) == 8 and len(tables["fills"]) == len(result.fills)
    evidence = tmp_path / "actual-parameter-replay.json"
    evidence.write_text(json.dumps({"synthetic": True, "formal_worker": False,
        "frozen_input": value.model_dump(mode="json"), "independent_receipt": receipt.model_dump(mode="json"),
        "result": result.model_dump(mode="json"), "expected_cash_from_actual_fills": str(cash)},
        ensure_ascii=False, sort_keys=True, indent=2) + "\n")
