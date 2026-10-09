from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
import duckdb
import pytest
from pydantic import ValidationError

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.feature_live_service import run_feature_live_batch
from rquant.feature_spool import FeatureBatchSpool
from rquant.intraday_feature_engine import IntradayFeatureConfig
from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.minute_backtest_contracts import (
    FrozenMinuteRuntimeInput,
    MinuteReplayExecutionProfile,
    MinuteReplayMaterial,
    MinuteReplayWork,
    MinuteRuntimeSourceReceipt,
)
from rquant.minute_backtest_runner import MinuteRuntimeReplayResult, MinuteRuntimeReplayRunner, MinuteRuntimeReplaySummary, minute_runtime_result_tables, run_minute_runtime_replay
from rquant.minute_backtest_source import read_minute_runtime_input_table, restore_minute_runtime_source, write_minute_runtime_input_table
from rquant.paper_broker import BrokerCostPolicy, PaperBrokerStore
from rquant.paper_execution_constraints import (
    PaperExecutionConstraintBatch,
    PaperExecutionConstraintPublisher,
    PaperExecutionConstraintSnapshot,
)
from rquant.paper_signal_consumer import PaperSignalConsumerStateStore, consume_notification_events_to_paper
from rquant.paper_signal_worker import PaperSignalPolicy, PaperSignalQueueStatus, PaperSignalQueueStore, run_paper_signal_batch
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseLoader,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_definition_bootstrap import bootstrap_builtin_definitions, plan_builtin_definitions
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_paper_quote import PaperPitQuoteResolver, PaperQuoteResolverConfig
from rquant.runtime_routing_policy import FrozenRoutingPolicyResolver, RoutingPolicyDocument
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction
from rquant.signal_route_spool import (
    ReadonlyNotificationEventRouteSpool,
    SignalRouteSpool,
    publish_mixed_notification_bus_prefix,
)
from rquant.signal_router_runtime import (
    SignalRouteCursorStore,
    StrategyRunnerSignalSource,
    route_runner_signals,
)
from rquant.strategy_candidate_snapshot import StrategyCandidatePriceBasis, StrategyCandidateRecord, StrategyCandidateSnapshotSpool
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_live_service import run_strategy_live_batch
from rquant.strategy_paper_lifecycle import PaperBrokerLifecycleReader
from rquant.strategy_runner import StrategyRunnerStore
from tests.paper_cost_fixtures import paper_execution_cost_spec, paper_instrument_context

COMMIT = "a" * 40
FRIDAY = date(2026, 7, 31)
MONDAY = date(2026, 8, 3)
FROZEN_AT = datetime(2026, 7, 30, 23, tzinfo=UTC)


def _at(day: date, minute: int) -> datetime:
    return datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=1, minutes=minute)


def _static(strategy_id: str) -> dict[str, object]:
    values: dict[str, object] = {"candidate_price_basis": "raw_session", "limit_up_price_session_raw": 12.0}
    if strategy_id == "n_shape":
        values |= {"limit_pct": 10.0, "t_close_session_raw": 9.8, "t_high_session_raw": 10.0}
    elif strategy_id == "growth_board_surge":
        values |= {"board_type": "gem", "ma_alignment": True, "large_net_vol_t1": 1.0, "session_pre_close_raw": 9.8}
    else:
        values |= {"auction_price_raw": 9.8, "auction_vol_ratio_5d": 1.0, "gap_pct_close": 1.0}
    return values


def _input(tmp_path: Path, strategy_id: str, *, days: tuple[date, ...] = (FRIDAY, MONDAY),
           constraint_overrides: dict[date, dict[str, object]] | None = None,
           candidate_delay: timedelta = timedelta(0), daily_quotes: bool = False,
           delayed_daily_quotes: bool = False) -> tuple[FrozenMinuteRuntimeInput, MinuteRuntimeSourceReceipt]:
    source = tmp_path / "original-input"
    source.mkdir(mode=0o700)
    definition = BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT).load_definition(strategy_id, 1)
    plan = plan_builtin_definitions(producer_commit=COMMIT)
    binding = next(item for item in plan.strategies if item.strategy_id == strategy_id)
    code = "300001.SZ" if strategy_id == "growth_board_surge" else "600000.SH"
    history = []
    history_days = pd.bdate_range("2026-07-02", "2026-07-30")[-20:]
    for day in history_days:
        for minute in range(30, 38):
            history.append({"ts_code": code, "trade_date": day.date(), "trade_time": _at(day.date(), minute), "available_at": _at(day.date(), minute) + timedelta(seconds=5),
                            "open": 10.0, "high": 10.02, "low": 9.98, "close": 10.0, "vol": 100.0, "amount": 1000.0})
    pd.DataFrame(history).to_parquet(source / "warmup.parquet", index=False)
    calendar = MarketCalendarAuthority.create(schema_version=1, exchange="SSE", producer_commit=COMMIT,
        coverage_start=date(2026, 7, 1), coverage_end=days[-1] + timedelta(days=7),
        open_dates=tuple(day.date() for day in pd.bdate_range("2026-07-01", days[-1] + timedelta(days=7))), generated_at=FROZEN_AT)
    calendar_bytes = json.dumps({"rows": [{"exchange": "SSE", "cal_date": day.isoformat(), "is_open": True}
                                        for day in calendar.open_dates]}, sort_keys=True).encode()
    (source / "calendar.json").write_bytes(calendar_bytes)
    routing_bytes = RoutingPolicyDocument(default_no_target_reason="offline-paper", rules=()).model_dump_json().encode()
    (source / "routing-policy.json").write_bytes(routing_bytes)
    candidates = StrategyCandidateSnapshotSpool(source / "candidates")
    for day in days:
        captured = _at(day, 25) + candidate_delay
        record = StrategyCandidateRecord(strategy_id=strategy_id, strategy_version="1", candidate_id=code,
            variant="baseline", decision_at=captured, available_at=captured, effective_trade_date=day,
            reference_trade_date=date(2026, 7, 30) if day == FRIDAY else FRIDAY,
            price_basis=StrategyCandidatePriceBasis.RAW, static_features=_static(strategy_id),
            reference_snapshot_ids={"daily_state": "b" * 64, "trade_calendar": calendar.content_sha256})
        candidates.publish_strategy_records(strategy_id=strategy_id, strategy_version="1",
            definition_fingerprint=binding.registration_fingerprint, executable_fingerprint=binding.executable_fingerprint,
            candidate_schema_fingerprint=binding.candidate_schema_fingerprint, static_feature_schema={name: {"dtype": item.dtype, "semantic": item.semantic} for name, item in definition.static_feature_schema.items()},
            source_snapshot_ids=record.reference_snapshot_ids, trade_date=day, captured_at=captured,
            producer_commit=COMMIT, rows=(record,))
    constraints = []
    for day in days:
        fields = dict(ts_code=code, trade_date=day, available_at=_at(day, 25), expires_at=_at(day, 361) if daily_quotes else _at(day, 359),
            suspended=False, buy_limit_locked=False, sell_limit_locked=False, risk_rejected=False,
            instrument_context=paper_instrument_context(code), source_snapshot_ids={"security_listing_status": "b" * 64}, producer_commit=COMMIT)
        if constraint_overrides is not None:
            fields.update(constraint_overrides.get(day, {}))
        constraints.append(PaperExecutionConstraintSnapshot(**fields, content_hash=canonical_sha256(fields)))
    (source / "constraint-publications").mkdir(mode=0o700)
    for sequence, constraint in enumerate(constraints):
        fields = dict(schema_version=1, sequence=sequence, producer_commit=COMMIT, records=(constraint,))
        PaperExecutionConstraintPublisher(root=source / "constraints", producer_commit=COMMIT, clock=lambda: constraint.available_at).publish(
            PaperExecutionConstraintBatch(**fields, content_hash=canonical_sha256(fields)))
        (source / "constraint-publications" / f"{sequence}.json").write_bytes((source / "constraints/current.json").read_bytes())
    market = LiveBatchSpool(source / "market")
    ticks = []
    for day in days:
        minutes = tuple(range(30, 38)) + ((359, 360) if daily_quotes else ())
        for minute in minutes:
            t = _at(day, minute)
            price = (9.9 if minute == 30 else 10.1 + (min(minute, 37) - 31) * .02) if day == FRIDAY else 9.4
            vol = 150.0 if day == FRIDAY and minute == 30 else 1500.0
            frame = pd.DataFrame([dict(ts_code=code, trade_time=t, open=price, high=price + .01, low=price - .01,
                                       close=price, vol=vol, amount=vol * price)])
            payload = MarketMinuteGateway.encode_payload(MarketMinuteGateway.normalize_frame(frame))
            sequence = 0 if market.current(LiveChannel.MARKET_MINUTE) is None else market.current(LiveChannel.MARKET_MINUTE).sequence + 1
            available = (t if not delayed_daily_quotes else _at(day, 360) + timedelta(seconds=5 + minute - 359)) if minute >= 359 else t + timedelta(seconds=5)
            market.publish(BatchEnvelope(schema_version=1, channel=LiveChannel.MARKET_MINUTE, dataset_id="market_minute",
                source="offline-original-fixture", source_request_id=f"minute-{sequence}", batch_id=f"minute-{sequence}", sequence=sequence,
                revision=1, event_time_start=t, event_time_end=t, source_time=t, received_at=available, available_at=available,
                row_count=1, content_sha256=hashlib.sha256(payload).hexdigest(), quality_status=BatchQualityStatus.PUBLISHED,
                producer_version="1", producer_commit=COMMIT), payload)
            ticks.append(available)
        if delayed_daily_quotes:
            ticks.append(_at(day, 360))
    ticks = sorted(set(ticks))
    materials = []
    for path in sorted(source.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        payload = path.read_bytes()
        materials.append(MinuteReplayMaterial(relative_path=path.relative_to(source).as_posix(),
            content_base64=base64.b64encode(payload).decode("ascii"), content_sha256=hashlib.sha256(payload).hexdigest()))
    profile = MinuteReplayExecutionProfile(key="original-paper", version=1, initial_cash=Decimal("100000"),
        execution_costs=paper_execution_cost_spec(transfer_fee_bps=Decimal(".1"), buy_slippage_bps=Decimal("2"), sell_slippage_bps=Decimal("2")),
        paper_policy=PaperSignalPolicy(account_id="paper-main", execution_lag=timedelta(minutes=1),
            action_quantities={SignalAction.B_INTENT: 1000, SignalAction.REDUCE: 500, SignalAction.S_INTENT: 1000}, producer_commit=COMMIT),
        routing_policy_fingerprint=hashlib.sha256(routing_bytes).hexdigest(), timestamp_semantics="bar_end")
    value = FrozenMinuteRuntimeInput(source_key="fixture.closed-minute", source_version=1, owner_id="fixture-owner", producer_commit=COMMIT,
        available_at=FROZEN_AT, start_date=days[0], end_date=days[-1], complete_through=ticks[-1], warmup_available_at=FROZEN_AT,
        holding_tail_complete=True, warmup_complete=True, audit_run_id="fixture-input-audit", dataset_snapshot_id="d" * 64,
        strategy=binding, feature_config=IntradayFeatureConfig(producer_commit=COMMIT), market_calendar=calendar,
        execution_profile=profile, tick_times=tuple(ticks), materials=tuple(materials),
        work=MinuteReplayWork(raw_rows=market.current(LiveChannel.MARKET_MINUTE).sequence + 1, warmup_rows=len(history),
            static_rows=len(days) + len(constraints) + len(calendar.open_dates) + len(ticks),
            market_batches=market.current(LiveChannel.MARKET_MINUTE).sequence + 1, union_codes=1,
            daily_observations=sum(days[0] <= day <= days[-1] for day in calendar.open_dates)))
    receipt = MinuteRuntimeSourceReceipt(source_key=value.source_key, source_version=value.source_version, owner_id=value.owner_id,
        input_hash=value.input_hash, producer_commit=COMMIT, start_date=days[0], end_date=days[-1],
        audit_run_id=value.audit_run_id, dataset_snapshot_id=value.dataset_snapshot_id, work=value.work,
        profile_hash=profile.profile_hash, strategy_id=strategy_id, strategy_version=1)
    return value, receipt


def _original_replay(value: FrozenMinuteRuntimeInput, receipt: MinuteRuntimeSourceReceipt, root: Path) -> dict[str, object]:
    # This oracle calls the physical original services. It does not call the new driver.
    source = restore_minute_runtime_source(value, expected=receipt, research_root=root)
    state = root / "original-state"
    state.mkdir(mode=0o700)
    for material in value.materials:
        if material.relative_path.startswith("seed/"):
            destination = state / material.relative_path.removeprefix("seed/")
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            for parent in destination.parents:
                if parent == state:
                    break
                parent.chmod(0o700)
            destination.write_bytes(material.payload())
            destination.chmod(0o600)
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT)
    definition = registry.load_definition(value.strategy.strategy_id, 1)
    plan = plan_builtin_definitions(producer_commit=COMMIT)
    bootstrap_builtin_definitions(state / "definitions", producer_commit=COMMIT, registered_at=FROZEN_AT,
        available_at=FROZEN_AT, expected_plan_id=plan.plan_id)
    definitions = ImmutableDefinitionRegistry(state / "definitions", execution_registry=registry.trusted_executable_registry())
    contract = definitions.read_feature_contract(plan.feature_contract_fingerprints[2], as_of=FROZEN_AT).contract
    broker = PaperBrokerStore(state / "broker.sqlite3", account_id=value.execution_profile.paper_policy.account_id,
        initial_cash=value.execution_profile.initial_cash, cost_policy=BrokerCostPolicy.from_execution_cost_spec(value.execution_profile.execution_costs))
    runner = StrategyRunnerStore(state / "runner.sqlite3", spec=definition.spec, evaluator_contract_fingerprint=definition.executable_fingerprint,
        feature_contract=contract, lifecycle_feature_source=PaperBrokerLifecycleReader(broker.path, account_id=broker.account_id))
    features = FeatureBatchSpool(state / "features")
    archive = LiveBatchSpool(source.root / "market", read_only=True)
    (state / "market/sources").mkdir(mode=0o700, parents=True)
    (state / "market").chmod(0o700)
    (state / "market/sources/market_minute.json").write_bytes((source.root / "market/sources/market_minute.json").read_bytes())
    (state / "market/sources/market_minute.json").chmod(0o600)
    raw = LiveBatchSpool(state / "market", cursor_root=state / "raw-cursors")
    loader = RuntimeCandidateUniverseLoader(RuntimeCandidateUniverseConfig(expected_commit=COMMIT, authorities=(
        CandidateUniverseAuthority(strategy_id=value.strategy.strategy_id, strategy_version="1", snapshot_root=source.root / "candidates",
            required=True, max_age_seconds=value.execution_profile.candidate_max_age_seconds,
            definition_fingerprint=value.strategy.registration_fingerprint, executable_fingerprint=value.strategy.executable_fingerprint,
            candidate_schema_fingerprint=value.strategy.candidate_schema_fingerprint,
            static_feature_names=tuple(definition.static_feature_schema), static_feature_schema={name: {"dtype": item.dtype, "semantic": item.semantic} for name, item in definition.static_feature_schema.items()}),)))
    queue = PaperSignalQueueStore(state / "queue.sqlite3", policy=value.execution_profile.paper_policy)
    consumer = PaperSignalConsumerStateStore(state / "consumer.sqlite3")
    bus = SignalBusStore(state / "bus.sqlite3")
    routes = SignalRouteCursorStore(state / "route.sqlite3", routing_policy_fingerprint=value.execution_profile.routing_policy_fingerprint)
    spool = SignalRouteSpool(state / "route-spool")
    signal_source = StrategyRunnerSignalSource(source_id=f"strategy.{definition.strategy_id}.v1", store=runner)
    target_resolver = FrozenRoutingPolicyResolver.from_document(source_path=source.root / "routing-policy.json",
        content_sha256=value.execution_profile.routing_policy_fingerprint,
        policy=RoutingPolicyDocument.model_validate_json((source.root / "routing-policy.json").read_bytes()))
    quote = PaperPitQuoteResolver(PaperQuoteResolverConfig(raw_spool_root=raw.root, trade_calendar_path=source.root / "calendar.json",
        trade_calendar_sha256=hashlib.sha256((source.root / "calendar.json").read_bytes()).hexdigest(), execution_constraint_root=state / "constraints",
        expected_producer_commit=COMMIT, timestamp_semantics=value.execution_profile.timestamp_semantics))
    daily_accounts = []
    daily_quotes = []
    for tick in value.tick_times:
        pointer = raw.current(LiveChannel.MARKET_MINUTE)
        for record in archive.list_after(LiveChannel.MARKET_MINUTE, sequence=-1 if pointer is None else pointer.sequence):
            if record.envelope.available_at > tick:
                break
            raw.publish(record.envelope, archive.read_payload(record))
        for publication in source.constraint_publications:
            if publication.pointer.published_at <= tick:
                current = state / "constraints/current.json"
                if current.exists():
                    from rquant.paper_execution_constraints import PaperExecutionConstraintPointer
                    if PaperExecutionConstraintPointer.model_validate_json(current.read_bytes()).sequence > publication.pointer.sequence:
                        continue
                PaperExecutionConstraintPublisher(root=state / "constraints", producer_commit=COMMIT,
                    clock=lambda: publication.pointer.published_at).publish(publication.batch)
        run_feature_live_batch(raw_spool=raw, feature_spool=features, historical_minutes=source.historical_minutes,
            historical_snapshot_id=source.warmup_sha256, config=value.feature_config, observed_at=tick, limit=1)
        run_strategy_live_batch(feature_spool=features, candidate_universe_loader=loader, runner=runner, evaluator=definition.evaluator,
            observed_at=tick, limit=1)
        route_runner_signals(source_id=signal_source.source_id, source=signal_source, bus=bus, cursors=routes, routed_at=tick,
            target_resolver=target_resolver, limit=100)
        publish_mixed_notification_bus_prefix(bus=bus, spool=spool, observed_at=tick, limit=100)
        consume_notification_events_to_paper(ReadonlyNotificationEventRouteSpool(spool.paths.root), queue, consumer, observed_at=tick, limit=100)
        run_paper_signal_batch(queue, broker, now=tick, trade_date=quote.trade_date_at(tick), quote_resolver=quote, limit=100)
        if tick == _at(quote.trade_date_at(tick), 360):
            with sqlite3.connect(f"{broker.path.as_uri()}?mode=ro", uri=True) as connection:
                held = connection.execute("SELECT ts_code FROM paper_lot WHERE account_id = ? AND remaining_quantity > 0 GROUP BY ts_code ORDER BY ts_code", (broker.account_id,)).fetchall()
            original_signals = tuple(record.signal for record in runner.signals_after(sequence=0))
            quotes = tuple(quote.resolve(next(signal for signal in original_signals if signal.action is SignalAction.B_INTENT and signal.candidate_id == code), observed_at=tick) for (code,) in held)
            daily_quotes.extend(quotes)
            daily_accounts.append(broker.account_snapshot(as_of=tick, market_prices={item.ts_code: item.context.executable_price for item in quotes}))
    records = runner.signals_after(sequence=0)
    with sqlite3.connect(f"{broker.path.as_uri()}?mode=ro", uri=True) as connection:
        order_ids = connection.execute("SELECT order_id FROM paper_order WHERE account_id = ? ORDER BY created_at, order_id", (broker.account_id,)).fetchall()
    last = raw.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[-1]
    final = MarketMinuteGateway.decode_payload(raw.read_payload(last)).sort_values("trade_time").drop_duplicates("ts_code", keep="last")
    return dict(signals=tuple(item.signal for item in records), orders=tuple(broker.order(str(row[0])) for row in order_ids),
                fills=broker.fills(), queue=tuple(queue.record(item.signal.signal_id) for item in records),
                daily_accounts=tuple(daily_accounts), daily_quotes=tuple(daily_quotes),
                account=broker.account_snapshot(as_of=value.tick_times[-1], market_prices={str(row.ts_code): Decimal(str(row.close)) for row in final.itertuples()}))


def _save_parity_evidence(path: Path, value: FrozenMinuteRuntimeInput, receipt: MinuteRuntimeSourceReceipt,
                          expected: dict[str, object], result: MinuteRuntimeReplayResult) -> None:
    original = {key: [item.model_dump(mode="json") for item in values] if isinstance(values, tuple) else values.model_dump(mode="json")
                for key, values in expected.items()}
    document = {"kind": "Original physical paper chain vs minute replay synthetic acceptance/v1",
        "synthetic": True, "formal_snapshot_gate": False, "frozen_input": value.model_dump(mode="json"),
        "independent_test_fixture_receipt": receipt.model_dump(mode="json"), "original_paper": original,
        "minute_replay": result.model_dump(mode="json"), "zero_tolerance_fields": ["signals", "orders", "fills", "queue_records", "account"]}
    path.write_text(json.dumps(document, ensure_ascii=False, sort_keys=True, allow_nan=False, indent=2) + "\n")


@pytest.mark.parametrize("strategy_id", ("n_shape", "auction_gap", "growth_board_surge"))
@pytest.mark.parametrize("timestamp_semantics", ("bar_end", "provider_snapshot"))
def test_complete_original_chain_has_exact_nonzero_runtime_parity(tmp_path: Path, strategy_id: str, timestamp_semantics: str) -> None:
    value, receipt = _input(tmp_path, strategy_id)
    value, receipt = _replace_input(value, receipt, execution_profile=value.execution_profile.model_copy(update={"timestamp_semantics": timestamp_semantics}))
    expected = _original_replay(value, receipt, tmp_path / "paper")
    result = run_minute_runtime_replay(value, expected=receipt, research_root=tmp_path / "backtest")
    assert result.signals == expected["signals"]
    assert result.orders == expected["orders"]
    assert result.fills == expected["fills"]
    assert result.queue_records == expected["queue"]
    assert result.account == expected["account"]
    assert result.signals and result.fills
    assert any(signal.action is SignalAction.WATCH for signal in result.signals)
    assert any(signal.action is SignalAction.B_INTENT for signal in result.signals)
    assert any(fill.commission > 0 and fill.transfer_fee > 0 for fill in result.fills)
    assert any(fill.tax > 0 for fill in result.fills)
    tables = minute_runtime_result_tables(result)
    for name in ("signals", "orders", "fills"):
        values = getattr(result, name)
        assert tuple(type(item).model_validate_json(payload) for item, payload in zip(values, tables[name].payload, strict=True)) == values
    _save_parity_evidence(tmp_path / "parity-evidence.json", value, receipt, expected, result)


@pytest.mark.parametrize("timestamp_semantics", ("bar_end", "provider_snapshot"))
def test_daily_nav_uses_original_pit_quote_and_same_broker_at_1500(tmp_path: Path, timestamp_semantics: str) -> None:
    value, receipt = _input(tmp_path, "n_shape", daily_quotes=True)
    value, receipt = _replace_input(value, receipt, execution_profile=value.execution_profile.model_copy(update={"timestamp_semantics": timestamp_semantics}))
    expected = _original_replay(value, receipt, tmp_path / "paper")
    driver = MinuteRuntimeReplayRunner(restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest"))
    result = driver.run()
    assert result.status == "complete" and result.daily_status == "complete"
    assert tuple(item.trade_date for item in result.daily_valuations) == (FRIDAY, MONDAY)
    assert tuple(item.account for item in result.daily_valuations) == expected["daily_accounts"]
    assert tuple(proof.quote for item in result.daily_valuations for proof in item.price_proofs) == expected["daily_quotes"]
    assert result.execution_profile == value.execution_profile
    for item in result.daily_valuations:
        assert item.as_of == item.observed_at == _at(item.trade_date, 360)
        assert item.basis == "pit_asof_15:00"
        assert item.calendar_sha256 == next(material.content_sha256 for material in value.materials if material.relative_path == "calendar.json")
        assert item.market_pointer.published_at <= item.as_of
        assert all(proof.quote.event_time <= proof.quote.available_at <= item.as_of for proof in item.price_proofs)
    first = result.daily_valuations[0]
    assert first.account.holdings[0].frozen_quantity == 1000
    assert first.price_proofs[0].quote.event_time == _at(FRIDAY, 360 if timestamp_semantics == "bar_end" else 359)
    assert driver.run() == MinuteRuntimeReplayRunner(driver.source).run() == result
    tables = minute_runtime_result_tables(result)
    assert len(tables) == 8 and len(tables["daily_valuations"]) == 2 and len(tables["execution_profile"]) == 1
    assert tuple(type(item).model_validate_json(payload) for item, payload in zip(result.daily_valuations, tables["daily_valuations"].payload, strict=True)) == result.daily_valuations
    summary = MinuteRuntimeReplaySummary.model_validate_json(tables["replay_summary"].payload.iloc[0])
    assert summary.work == receipt.work and summary.result_budget == receipt.result_budget and summary.daily_status == "complete"
    _save_parity_evidence(tmp_path / "parity-evidence.json", value, receipt, expected, result)


def test_daily_nav_does_not_use_prices_published_after_day_end(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape", daily_quotes=True, delayed_daily_quotes=True)
    result = run_minute_runtime_replay(value, expected=receipt, research_root=tmp_path / "backtest")
    first = result.daily_valuations[0]
    assert first.observed_at == first.as_of == _at(FRIDAY, 360)
    assert first.status == "unavailable" and first.account is None and not first.price_proofs
    assert first.unavailable_reasons and result.daily_status == "unavailable"
    assert result.fills


def test_daily_nav_does_not_value_an_unobserved_closed_session(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    result = run_minute_runtime_replay(value, expected=receipt, research_root=tmp_path / "backtest")
    assert result.status == "incomplete" and result.daily_status == "unavailable"
    assert len(result.daily_valuations) == value.work.daily_observations == 2
    assert all(item.account is None and item.observed_at is None and item.unavailable_reasons == ("missing_original_15:00_clock",) for item in result.daily_valuations)
    assert tuple(item.trade_date for item in result.daily_valuations) == (FRIDAY, MONDAY)


def test_daily_nav_work_cannot_be_omitted_from_independent_admission(tmp_path: Path) -> None:
    from rquant.minute_backtest_adapter import MinuteRuntimeReplayAdapter, MinuteRuntimeReplayParameters
    value, receipt = _input(tmp_path, "n_shape")
    assert value.work.daily_price_bound == 2
    adapter = MinuteRuntimeReplayAdapter(expected=receipt)
    omitted = MinuteRuntimeReplayParameters.from_frozen(value, work_units=value.work.work_units - value.work.daily_observations - value.work.daily_price_bound)
    with pytest.raises(ValueError, match="independent.*physical work"):
        adapter.bound_parameters(omitted)
    understated = value.work.model_copy(update={"daily_observations": 1})
    with pytest.raises(ValueError, match="daily"):
        _replace_input(value, receipt, work=understated)


def test_daily_nav_before_clock_commit_recovers_without_extra_trade_or_fee(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape", daily_quotes=True)
    driver = MinuteRuntimeReplayRunner(restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest"))

    def crash(stage: str) -> None:
        if stage == "after_daily_valuation":
            raise RuntimeError("original daily NAV read before replay clock commit")

    with pytest.raises(RuntimeError, match="daily NAV"):
        driver.run(fault_hook=crash)
    buy = driver.broker.fills()
    assert len(buy) == 1 and driver.clock.daily_valuations == ()
    result = MinuteRuntimeReplayRunner(driver.source).run()
    clean = run_minute_runtime_replay(value, expected=receipt, research_root=tmp_path / "clean")
    assert result == clean and result.fills[0] == buy[0]
    assert len(result.daily_valuations) == 2 and len(result.fills) == 2


def test_daily_nav_retains_original_constraint_expiry_rejection(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape", daily_quotes=True,
        constraint_overrides={FRIDAY: {"expires_at": _at(FRIDAY, 359)}})
    result = run_minute_runtime_replay(value, expected=receipt, research_root=tmp_path / "backtest")
    first = result.daily_valuations[0]
    assert first.status == "unavailable" and first.account is None
    assert any("expired" in reason for reason in first.unavailable_reasons)


def test_one_sequential_runtime_preserves_account_beyond_twenty_day_bucket(tmp_path: Path) -> None:
    days = tuple(day.date() for day in pd.bdate_range(FRIDAY, periods=21))
    value, receipt = _input(tmp_path, "n_shape", days=days, daily_quotes=True)
    result = run_minute_runtime_replay(value, expected=receipt, research_root=tmp_path / "backtest")
    assert result.status == "complete"
    assert result.daily_status == "complete"
    assert len(result.daily_valuations) == 21
    assert all(record.account is not None and record.observed_at == _at(record.trade_date, 360) for record in result.daily_valuations)
    assert result.work.market_batches == 21 * 10
    assert result.fills and result.account.realized_pnl < 0
    assert result.account.holdings == ()
    assert len(result.fills) == 2


def test_runner_commit_before_cursor_recovers_original_receipt_once(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    source = restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest")
    runner = MinuteRuntimeReplayRunner(source)

    def crash(stage: str) -> None:
        if stage == "after_runner_commit":
            raise RuntimeError("original runner committed before consumer cursor")

    with pytest.raises(RuntimeError, match="committed"):
        runner.step(0, fault_hook=crash)
    before = runner.runner.signals_after(sequence=0)
    assert len(before) == 1
    restored = MinuteRuntimeReplayRunner(source)
    restored.step(0)
    assert restored.runner.signals_after(sequence=0) == before
    result = restored.run()
    assert len({signal.signal_id for signal in result.signals}) == len(result.signals)
    assert len(result.fills) == 2
    assert restored.run() == result


def test_broker_fill_before_queue_complete_recovers_without_second_fee(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    driver = MinuteRuntimeReplayRunner(restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest"))
    driver.step(0)
    driver.step(1)
    complete = driver.queue.complete
    failed = False

    def crash(*args: object, **kwargs: object) -> object:
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("original fill persisted before queue completion")
        return complete(*args, **kwargs)

    monkeypatch.setattr(driver.queue, "complete", crash)
    driver.step(2)
    buy = next(item.signal for item in driver.runner.signals_after(sequence=0) if item.signal.action is SignalAction.B_INTENT)
    prepared = driver.queue.record(buy.signal_id)
    assert prepared is not None and prepared.status is PaperSignalQueueStatus.PREPARED
    assert prepared.execution_id is not None
    execution = driver.broker.execution(prepared.execution_id)
    assert execution is not None and execution.fill is not None
    original_fill = execution.fill
    driver.step(3)
    recovered = driver.queue.record(buy.signal_id)
    assert recovered is not None and recovered.status is PaperSignalQueueStatus.COMPLETED
    assert driver.broker.fills() == (original_fill,)
    result = driver.run()
    assert len(result.fills) == 2
    assert len({fill.execution_id for fill in result.fills}) == 2


def test_friday_holding_is_frozen_until_next_open_session(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    driver = MinuteRuntimeReplayRunner(restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest"))
    for index in range(8):
        driver.step(index)
    account = driver.broker.account_snapshot(as_of=value.tick_times[7], market_prices={"600000.SH": Decimal("10.22")})
    assert account.holdings[0].quantity == 1000
    assert account.holdings[0].available_quantity == 0
    assert account.holdings[0].frozen_quantity == 1000
    result = driver.run()
    assert any(fill.executed_at.date() == MONDAY and fill.tax > 0 for fill in result.fills)
    assert all(fill.tax == 0 for fill in result.fills if fill.executed_at.date() == FRIDAY)


@pytest.mark.parametrize("flag", ("suspended", "buy_limit_locked", "risk_rejected"))
def test_original_broker_constraints_reject_buy(tmp_path: Path, flag: str) -> None:
    value, receipt = _input(tmp_path, "n_shape", constraint_overrides={FRIDAY: {flag: True}})
    result = run_minute_runtime_replay(value, expected=receipt, research_root=tmp_path / "backtest")
    assert any(signal.action is SignalAction.B_INTENT for signal in result.signals)
    assert result.orders and any(order.reject_reason is not None for order in result.orders)
    assert result.fills == ()


def test_independent_receipt_rejects_rebound_input_before_workspace_write(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    replacements = {"owner_id": "another-owner", "input_hash": "f" * 64, "producer_commit": "f" * 40,
                    "source_key": "another-source", "source_version": 2, "dataset_snapshot_id": "f" * 64,
                    "audit_run_id": "other-audit", "end_date": MONDAY + timedelta(days=1), "profile_hash": "f" * 64,
                    "strategy_id": "auction_gap", "work": value.work.model_copy(update={"static_rows": value.work.static_rows + 1})}
    for key, item in replacements.items():
        target = tmp_path / f"rejected-{key}"
        with pytest.raises(ValueError, match="independent source receipt"):
            restore_minute_runtime_source(value, expected=receipt.model_copy(update={key: item}), research_root=target)
        assert not target.exists()
    for key in ("hold_days", "entry_modes", "preset_name", "profile_variants", "factor_score_threshold"):
        with pytest.raises(ValidationError):
            FrozenMinuteRuntimeInput.model_validate(value.model_dump(mode="python") | {key: 1})


def test_immutable_input_mutation_and_workspace_alias_are_refused(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    source = restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest")
    source.root.joinpath("warmup.parquet").write_bytes(b"changed source")
    with pytest.raises(ValueError, match="changed"):
        MinuteRuntimeReplayRunner(source)
    with pytest.raises(FileExistsError):
        restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest")
    link = tmp_path / "alias"
    link.symlink_to(source.root.parent, target_is_directory=True)
    with pytest.raises(OSError):
        restore_minute_runtime_source(value, expected=receipt, research_root=link / "outside")
    assert not (source.root.parent / "outside").exists()


def test_future_candidate_is_rejected_by_original_loader(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "auction_gap", candidate_delay=timedelta(minutes=10))
    with pytest.raises(RuntimeError, match="candidate|authority|available"):
        run_minute_runtime_replay(value, expected=receipt, research_root=tmp_path / "backtest")


def test_future_market_prefix_remains_archived_until_original_publication(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    source = restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest")
    driver = MinuteRuntimeReplayRunner(source)
    driver.step(0)
    assert driver.raw.source_descriptor(LiveChannel.MARKET_MINUTE).high_watermark == 0
    archive = LiveBatchSpool(source.root / "market", read_only=True)
    assert archive.source_descriptor(LiveChannel.MARKET_MINUTE).high_watermark == 15
    source.verify_unchanged()


def _replace_input(value: FrozenMinuteRuntimeInput, receipt: MinuteRuntimeSourceReceipt, **changes: object) -> tuple[FrozenMinuteRuntimeInput, MinuteRuntimeSourceReceipt]:
    if "tick_times" in changes and "work" not in changes:
        changes["work"] = value.work.model_copy(update={"static_rows": value.work.static_rows + len(changes["tick_times"]) - len(value.tick_times)})
    updated = FrozenMinuteRuntimeInput.model_validate(value.model_dump(mode="python") | changes)
    return updated, MinuteRuntimeSourceReceipt.model_validate(receipt.model_dump(mode="python") | {"input_hash": updated.input_hash,
        "work": updated.work, "profile_hash": updated.execution_profile.profile_hash, "strategy_id": updated.strategy.strategy_id,
        "strategy_version": updated.strategy.strategy_version})


@pytest.mark.parametrize("strategy_id", ("n_shape", "auction_gap", "growth_board_surge"))
def test_prior_session_seed_uses_original_ledger_and_feature_cursor(tmp_path: Path, strategy_id: str) -> None:
    value, receipt = _input(tmp_path, strategy_id)
    driver = MinuteRuntimeReplayRunner(restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "prior"))
    for index in range(8):
        driver.step(index)
    original_buy = driver.broker.fills()[0]
    seed_materials = []
    seed_rows = 0
    for name in ("broker.sqlite3", "runner.sqlite3"):
        backup = tmp_path / name
        with sqlite3.connect(f"{(driver.root / name).as_uri()}?mode=ro", uri=True) as source_connection, sqlite3.connect(backup) as target:
            source_connection.backup(target)
            tables = ("paper_intent", "paper_order", "paper_fill", "paper_lot", "paper_lot_consumption", "paper_execution_receipt") if name == "broker.sqlite3" else ("candidate_state", "processed_batch", "runner_signal")
            seed_rows += sum(source_connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in tables)
        payload = backup.read_bytes()
        seed_materials.append(MinuteReplayMaterial(relative_path=f"seed/{name}", content_base64=base64.b64encode(payload).decode("ascii"), content_sha256=hashlib.sha256(payload).hexdigest()))
    for folder in ("features", "raw-cursors"):
        for path in sorted((driver.root / folder).rglob("*")):
            if path.is_file() and not path.name.startswith("."):
                payload = path.read_bytes()
                seed_materials.append(MinuteReplayMaterial(relative_path=f"seed/{path.relative_to(driver.root).as_posix()}",
                    content_base64=base64.b64encode(payload).decode("ascii"), content_sha256=hashlib.sha256(payload).hexdigest()))
    seed_rows += sum(record.envelope.row_count for record in driver.features.list_after(sequence=-1))
    value, receipt = _replace_input(value, receipt, tick_times=value.tick_times[8:],
        materials=tuple(sorted(value.materials + tuple(seed_materials), key=lambda item: item.relative_path)),
        work=value.work.model_copy(update={"static_rows": value.work.static_rows + seed_rows - 8}))
    expected = _original_replay(value, receipt, tmp_path / "seeded-paper")
    restored = MinuteRuntimeReplayRunner(restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "seeded"))
    result = restored.run()
    assert result.signals == expected["signals"]
    assert result.orders == expected["orders"]
    assert result.fills == expected["fills"]
    assert result.queue_records == expected["queue"]
    assert result.account == expected["account"]
    assert result.fills[0] == original_buy
    assert len(result.fills) > 1
    assert any(fill.executed_at.date() == MONDAY and fill.tax > 0 for fill in result.fills)
    _save_parity_evidence(tmp_path / "parity-evidence.json", value, receipt, expected, result)


def test_static_work_includes_the_actual_frozen_clock_records(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    source = restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest")
    assert source.work.static_rows == 2 + 2 + len(value.market_calendar.open_dates) + len(value.tick_times)


def test_exact_source_table_roundtrip_rejects_ambiguous_and_unexpected_fields(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    with duckdb.connect(":memory:") as connection:
        write_minute_runtime_input_table(connection, value)
        assert read_minute_runtime_input_table(connection, expected=receipt) == value
        original = value.model_dump_json()
        for payload in ('{"contract":"minute-runtime-replay-input/v1",' + original[1:], '{"entry_modes":[],' + original[1:], '{"x":NaN}', '[' * 10_000 + '0' + ']' * 10_000):
            connection.execute("UPDATE minute_runtime_replay_input SET payload = ?", [payload])
            with pytest.raises(ValueError):
                read_minute_runtime_input_table(connection, expected=receipt)
        connection.execute("UPDATE minute_runtime_replay_input SET payload = ?", [original])
        connection.execute("CREATE TABLE unexpected (value INTEGER)")
        with pytest.raises(ValueError, match="only its exact input"):
            read_minute_runtime_input_table(connection, expected=receipt)


def test_exact_raw_duplicate_is_accepted_and_same_sequence_conflict_is_refused(tmp_path: Path) -> None:
    from rquant.live_spool import LiveSpoolIntegrityError

    value, receipt = _input(tmp_path, "n_shape")
    driver = MinuteRuntimeReplayRunner(restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest"))
    driver.step(0)
    before = driver.runner.signals_after(sequence=0)
    first = driver.raw.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    pointer = driver.raw.current(LiveChannel.MARKET_MINUTE)
    assert driver.raw.publish(first.envelope, driver.raw.read_payload(first)) == pointer
    with pytest.raises(LiveSpoolIntegrityError):
        driver.raw.publish(first.envelope.model_copy(update={"source_request_id": "conflicting-request"}), driver.raw.read_payload(first))
    assert driver.runner.signals_after(sequence=0) == before


def test_original_sell_lock_keeps_position_and_does_not_charge_sell_cost(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape", constraint_overrides={MONDAY: {"sell_limit_locked": True}})
    result = run_minute_runtime_replay(value, expected=receipt, research_root=tmp_path / "backtest")
    assert len(result.fills) == 1
    assert result.account.holdings[0].quantity == 1000
    assert any(order.reject_reason is not None for order in result.orders)
    assert all(fill.tax == 0 for fill in result.fills)


def test_missing_or_expired_original_constraint_is_not_replaced_by_defaults(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape", constraint_overrides={FRIDAY: {"expires_at": _at(FRIDAY, 31)}})
    driver = MinuteRuntimeReplayRunner(restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest"))
    for index in range(3):
        driver.step(index)
    record = next(driver.queue.record(item.signal.signal_id) for item in driver.runner.signals_after(sequence=0) if item.signal.action is SignalAction.B_INTENT)
    assert record is not None and record.last_error and "expired" in record.last_error.lower()
    assert driver.broker.fills() == ()
    with pytest.raises(ValueError, match="publication receipts"):
        _replace_input(value, receipt, materials=tuple(item for item in value.materials if not item.relative_path.startswith("constraint-publications/")))


def test_holding_tail_and_future_warmup_cannot_be_self_declared_complete(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    with pytest.raises(ValueError, match="holding tail"):
        _replace_input(value, receipt, complete_through=value.tick_times[-2])
    frame = pd.read_parquet(tmp_path / "original-input/warmup.parquet")
    frame.loc[0, "available_at"] = value.tick_times[0]
    from io import BytesIO
    stream = BytesIO()
    frame.to_parquet(stream, index=False)
    payload = stream.getvalue()
    changed = MinuteReplayMaterial(relative_path="warmup.parquet", content_base64=base64.b64encode(payload).decode("ascii"), content_sha256=hashlib.sha256(payload).hexdigest())
    altered, altered_receipt = _replace_input(value, receipt, materials=tuple(changed if item.relative_path == "warmup.parquet" else item for item in value.materials))
    with pytest.raises(ValueError, match="warmup material was not visible"):
        restore_minute_runtime_source(altered, expected=altered_receipt, research_root=tmp_path / "future-warmup")


def test_expired_prepared_fill_recovers_the_original_execution_after_weekend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    value, receipt = _replace_input(value, receipt, tick_times=value.tick_times[:3] + value.tick_times[8:])
    driver = MinuteRuntimeReplayRunner(restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest"))
    driver.step(0)
    driver.step(1)
    complete = driver.queue.complete
    failed = False

    def crash(*args: object, **kwargs: object) -> object:
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("completed fill, interrupted original queue completion")
        return complete(*args, **kwargs)

    monkeypatch.setattr(driver.queue, "complete", crash)
    driver.step(2)
    before = driver.broker.fills()
    buy = next(item.signal for item in driver.runner.signals_after(sequence=0) if item.signal.action is SignalAction.B_INTENT)
    assert value.tick_times[3] >= buy.expires_at
    driver.step(3)
    record = driver.queue.record(buy.signal_id)
    assert record is not None and record.status is PaperSignalQueueStatus.COMPLETED
    assert driver.broker.fills() == before
    assert record.order is not None and record.order.order_id == before[0].order_id


def test_rebinding_a_seed_cost_spec_cannot_reuse_an_existing_ledger(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    driver = MinuteRuntimeReplayRunner(restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "backtest"))
    driver.step(0)
    incompatible = paper_execution_cost_spec(commission_bps=Decimal("4"))
    with pytest.raises(ValueError, match="cost|policy|fingerprint"):
        PaperBrokerStore(driver.broker.path, account_id=driver.broker.account_id, initial_cash=value.execution_profile.initial_cash,
            cost_policy=BrokerCostPolicy.from_execution_cost_spec(incompatible))


def test_parquet_row_budget_is_checked_before_decoding_warmup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from io import BytesIO

    value, receipt = _input(tmp_path, "n_shape")
    original = pd.read_parquet
    frame = original(tmp_path / "original-input/warmup.parquet")
    expanded = pd.concat([frame] * 126, ignore_index=True).iloc[:20_001]
    stream = BytesIO()
    expanded.to_parquet(stream, index=False)
    payload = stream.getvalue()
    material = MinuteReplayMaterial(relative_path="warmup.parquet", content_base64=base64.b64encode(payload).decode("ascii"), content_sha256=hashlib.sha256(payload).hexdigest())
    value, receipt = _replace_input(value, receipt, materials=tuple(material if item.relative_path == "warmup.parquet" else item for item in value.materials))

    def forbid_large_decode(data: object, *args: object, **kwargs: object) -> pd.DataFrame:
        if isinstance(data, BytesIO) and data.getvalue() == payload:
            raise AssertionError("source decoded rows before checking the frozen physical work limit")
        return original(data, *args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", forbid_large_decode)
    with pytest.raises(ValueError, match="Parquet|physical|work"):
        restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "over-budget")


def test_frozen_work_cannot_underreport_actual_restored_records(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    value, receipt = _replace_input(value, receipt, work=value.work.model_copy(update={"warmup_rows": 1}))
    with pytest.raises(ValueError, match="physical work differs"):
        restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "underreported")


def test_closed_calendar_records_remain_in_the_frozen_work_bound(tmp_path: Path) -> None:
    value, receipt = _input(tmp_path, "n_shape")
    material = next(item for item in value.materials if item.relative_path == "calendar.json")
    calendar = json.loads(material.payload())
    calendar["rows"].append({"exchange": "SSE", "cal_date": "2026-08-01", "is_open": False})
    payload = json.dumps(calendar, sort_keys=True).encode()
    changed = MinuteReplayMaterial(relative_path="calendar.json", content_base64=base64.b64encode(payload).decode("ascii"), content_sha256=hashlib.sha256(payload).hexdigest())
    value, receipt = _replace_input(value, receipt, materials=tuple(changed if item.relative_path == "calendar.json" else item for item in value.materials),
        work=value.work.model_copy(update={"static_rows": value.work.static_rows + 1}))
    source = restore_minute_runtime_source(value, expected=receipt, research_root=tmp_path / "calendar-work")
    assert source.work == receipt.work
