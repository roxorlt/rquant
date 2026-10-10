"""Sequential replay through the original feature, strategy and paper services."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, model_validator

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.feature_live_service import FeatureLiveBatchSummary, run_feature_live_batch
from rquant.feature_contracts import FeatureContract
from rquant.feature_spool import FeatureBatchSpool
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.minute_backtest_contracts import (FrozenMinuteRuntimeInput, MinuteReplayDailyPriceProof,
    MinuteReplayExecutionProfile, MinuteReplayModel, MinuteReplayResultBudget, MinuteReplayWork,
    MinuteRuntimeDailyValuation, MinuteRuntimeSourceReceipt, Sha256)
from rquant.minute_backtest_source import RestoredMinuteRuntimeSource, _read_private, restore_minute_runtime_source
from rquant.paper_broker import BrokerCostPolicy, PaperBrokerStore
from rquant.paper_contracts import PaperAccountSnapshot, PaperFill, PaperOrder
from rquant.paper_execution_constraints import (PaperExecutionConstraintPointer, PaperExecutionConstraintPublisher,
    PaperExecutionConstraintUnavailableError)
from rquant.paper_signal_consumer import PaperSignalConsumerStateStore, consume_notification_events_to_paper
from rquant.paper_signal_worker import PaperSignalQueueRecord, PaperSignalQueueStatus, PaperSignalQueueStore, run_paper_signal_batch
from rquant.runtime_candidate_universe import CandidateUniverseAuthority, RuntimeCandidateUniverseConfig, RuntimeCandidateUniverseLoader
from rquant.runtime_definition_bootstrap import bootstrap_builtin_definitions, plan_builtin_definitions
from rquant.runtime_paper_quote import PaperPitQuoteResolver, PaperQuoteResolutionError, PaperQuoteResolverConfig
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_routing_policy import FrozenRoutingPolicyResolver, RoutingPolicyDocument
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelopeFamily
from rquant.signal_route_spool import ReadonlyNotificationEventRouteSpool, SignalRouteSpool, publish_mixed_notification_bus_prefix
from rquant.signal_router_runtime import SignalRouteCursorStore, StrategyRunnerSignalSource, route_runner_signals
from rquant.strategy_evaluators import BuiltinStrategyDefinition, BuiltinStrategyEvaluatorRegistry
from rquant.strategy_live_service import run_strategy_live_batch
from rquant.strategy_paper_lifecycle import PaperBrokerLifecycleReader
from rquant.strategy_runner import StrategyRunnerStore


class MinuteRuntimeReplaySummary(MinuteReplayModel):
    input_hash: Sha256
    profile_hash: Sha256
    strategy_id: Literal["n_shape", "auction_gap", "growth_board_surge"]
    strategy_version: Literal[1]
    status: Literal["complete", "incomplete"]
    daily_status: Literal["complete", "unavailable"]
    result_budget: MinuteReplayResultBudget
    work: MinuteReplayWork
    incomplete_reasons: tuple[str, ...] = ()


class MinuteRuntimeReplayResult(MinuteRuntimeReplaySummary):
    daily_valuations: tuple[MinuteRuntimeDailyValuation, ...]
    execution_profile: MinuteReplayExecutionProfile
    signals: tuple[SignalEnvelopeFamily, ...]
    orders: tuple[PaperOrder, ...]
    fills: tuple[PaperFill, ...]
    queue_records: tuple[PaperSignalQueueRecord, ...]
    account: PaperAccountSnapshot

    @model_validator(mode="after")
    def bound_daily_results(self) -> Self:
        dates = tuple(item.trade_date for item in self.daily_valuations)
        if dates != tuple(sorted(set(dates))) or len(dates) != self.work.daily_observations:
            raise ValueError("minute result daily observations differ from the frozen work bound")
        if self.profile_hash != self.execution_profile.profile_hash or any(item.profile_hash != self.profile_hash
            or item.input_hash != self.input_hash for item in self.daily_valuations):
            raise ValueError("minute result daily execution profile differs")
        signals = {signal.signal_id: signal for signal in self.signals}
        for item in self.daily_valuations:
            for proof in item.price_proofs:
                signal = signals.get(proof.entry_signal_id)
                if (signal is None or signal.action is not SignalAction.B_INTENT or signal.candidate_id != proof.quote.ts_code
                    or proof.quote.producer_commit != self.execution_profile.paper_policy.producer_commit):
                    raise ValueError("minute result daily quote is detached from the original entry or code provenance")
        available = all(item.status == "complete" for item in self.daily_valuations)
        if (self.daily_status == "complete") != available or (self.status == "complete" and not available):
            raise ValueError("minute result cannot claim a complete curve with daily gaps")
        return self


class _ReplayClock(MinuteReplayModel):
    input_hash: Sha256
    last_completed_tick: int = Field(ge=-1)
    daily_valuations: tuple[MinuteRuntimeDailyValuation, ...] = ()


def _write_clock(path: Path, value: _ReplayClock) -> None:
    temporary = path.with_name(".clock.pending")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(value.model_dump_json().encode())
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class MinuteRuntimeReplayRunner:
    """Own private output state; all decisions and executions stay in original stores."""

    def __init__(self, source: RestoredMinuteRuntimeSource) -> None:
        if type(source) is not RestoredMinuteRuntimeSource:
            raise TypeError("minute runner requires its restored exact source")
        source.verify_unchanged()
        self.source = source
        self.value = value = source.value
        self._check_input_type()
        self.root = source.root.parent / "state"
        self.clock_path = self.root / "clock.json"
        if not self.root.exists():
            self.root.mkdir(mode=0o700)
            for material in value.materials:
                if material.relative_path.startswith("seed/"):
                    target = self.root / material.relative_path.removeprefix("seed/")
                    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    for parent in target.parents:
                        if parent == self.root:
                            break
                        parent.chmod(0o700)
                    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
                    with os.fdopen(descriptor, "wb") as stream:
                        stream.write(_read_private(source.root / material.relative_path))
                        stream.flush()
                        os.fsync(stream.fileno())
            _write_clock(self.clock_path, _ReplayClock(input_hash=value.input_hash, last_completed_tick=-1))
        self.clock = _ReplayClock.model_validate_json(_read_private(self.clock_path))
        if self.clock.input_hash != value.input_hash or self.clock.last_completed_tick >= len(value.tick_times):
            raise ValueError("minute replay recovery identity or clock differs")
        self.calendar_sha256 = next(item.content_sha256 for item in value.materials if item.relative_path == "calendar.json")
        dates = tuple(item.trade_date for item in self.clock.daily_valuations)
        if dates != tuple(sorted(set(dates))) or any(item.trade_date not in value.daily_trade_dates
            or item.input_hash != value.input_hash or item.profile_hash != value.execution_profile.profile_hash or item.calendar_sha256 != self.calendar_sha256
            or item.observed_at not in value.tick_times[:self.clock.last_completed_tick + 1]
            for item in self.clock.daily_valuations):
            raise ValueError("minute replay recovered daily proof differs from its original clock or source")
        self.registry, self.definition, contract = self._execution_definition()
        profile = value.execution_profile
        self.broker = PaperBrokerStore(self.root / "broker.sqlite3", account_id=profile.paper_policy.account_id,
            initial_cash=profile.initial_cash, cost_policy=BrokerCostPolicy.from_execution_cost_spec(profile.execution_costs))
        self.broker.require_trusted_ledger()
        self.runner = StrategyRunnerStore(self.root / "runner.sqlite3", spec=self.definition.spec,
            evaluator_contract_fingerprint=self.definition.executable_fingerprint, feature_contract=contract,
            lifecycle_feature_source=PaperBrokerLifecycleReader(self.broker.path, account_id=self.broker.account_id))
        self.features = FeatureBatchSpool(self.root / "features")
        self.archive_market = LiveBatchSpool(source.root / "market", read_only=True)
        source_identity = _read_private(source.root / "market/sources/market_minute.json")
        working_identity = self.root / "market/sources/market_minute.json"
        working_identity.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        working_identity.parent.parent.chmod(0o700)
        if working_identity.exists():
            if _read_private(working_identity) != source_identity:
                raise ValueError("minute replay recovered market identity differs from the original source")
        else:
            descriptor = os.open(working_identity, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(source_identity)
                stream.flush()
                os.fsync(stream.fileno())
        self.raw = LiveBatchSpool(self.root / "market", cursor_root=self.root / "raw-cursors")
        self.loader = RuntimeCandidateUniverseLoader(RuntimeCandidateUniverseConfig(expected_commit=value.producer_commit, authorities=(
            CandidateUniverseAuthority(strategy_id=value.strategy.strategy_id, strategy_version=str(value.strategy.strategy_version),
                snapshot_root=source.root / "candidates", required=True, max_age_seconds=profile.candidate_max_age_seconds,
                definition_fingerprint=value.strategy.registration_fingerprint, executable_fingerprint=value.strategy.executable_fingerprint,
                candidate_schema_fingerprint=value.strategy.candidate_schema_fingerprint,
                static_feature_names=tuple(self.definition.static_feature_schema), static_feature_schema={name: {"dtype": item.dtype, "semantic": item.semantic} for name, item in self.definition.static_feature_schema.items()}),)))
        self.queue = PaperSignalQueueStore(self.root / "queue.sqlite3", policy=profile.paper_policy)
        self.consumer = PaperSignalConsumerStateStore(self.root / "consumer.sqlite3")
        self.bus = SignalBusStore(self.root / "bus.sqlite3")
        self.routes = SignalRouteCursorStore(self.root / "route.sqlite3", routing_policy_fingerprint=profile.routing_policy_fingerprint)
        self.spool = SignalRouteSpool(self.root / "route-spool")
        self.signal_source = StrategyRunnerSignalSource(source_id=f"strategy.{self.definition.strategy_id}.v1", store=self.runner)
        routing_bytes = _read_private(source.root / "routing-policy.json")
        self.target_resolver = FrozenRoutingPolicyResolver.from_document(source_path=source.root / "routing-policy.json",
            content_sha256=profile.routing_policy_fingerprint, policy=RoutingPolicyDocument.model_validate_json(routing_bytes))
        self.quote = PaperPitQuoteResolver(PaperQuoteResolverConfig(raw_spool_root=self.raw.root, trade_calendar_path=source.root / "calendar.json",
            trade_calendar_sha256=hashlib.sha256(_read_private(source.root / "calendar.json")).hexdigest(),
            execution_constraint_root=self.root / "constraint-authority", expected_producer_commit=value.producer_commit,
            timestamp_semantics=profile.timestamp_semantics, quote_max_age_seconds=profile.quote_max_age_seconds,
            max_finalize_scan_batches=profile.max_finalize_scan_batches, max_visible_scan_batches=profile.max_visible_scan_batches))

    def _check_input_type(self) -> None:
        if type(self.value) is not FrozenMinuteRuntimeInput:
            raise TypeError("native minute runner requires its exact closed native input")

    def _execution_definition(self) -> tuple[BuiltinStrategyEvaluatorRegistry | None, BuiltinStrategyDefinition, FeatureContract]:
        value = self.value
        registry = BuiltinStrategyEvaluatorRegistry(producer_commit=value.producer_commit)
        definition = registry.load_definition(value.strategy.strategy_id, value.strategy.strategy_version)
        plan = plan_builtin_definitions(producer_commit=value.producer_commit)
        bootstrap_builtin_definitions(self.root / "definitions", producer_commit=value.producer_commit,
            registered_at=value.available_at, available_at=value.available_at, expected_plan_id=plan.plan_id)
        definitions = ImmutableDefinitionRegistry(self.root / "definitions", execution_registry=registry.trusted_executable_registry())
        contract = definitions.read_feature_contract(plan.feature_contract_fingerprints[2], as_of=value.available_at).contract
        return registry, definition, contract

    def _feature_step(self, observed: datetime, *, fault_hook: Callable[[str], None] | None) -> FeatureLiveBatchSummary:
        return run_feature_live_batch(raw_spool=self.raw, feature_spool=self.features,
            historical_minutes=self.source.historical_minutes, historical_snapshot_id=self.source.warmup_sha256,
            config=self.value.feature_config, observed_at=observed, limit=1)

    def _result_model(self) -> type[MinuteRuntimeReplayResult]:
        return MinuteRuntimeReplayResult

    def _result_fields(self) -> dict[str, object]:
        return {}

    def _after_paper_step(self, observed: datetime) -> None:
        pass

    def step(self, tick_index: int, *, fault_hook: Callable[[str], None] | None = None) -> None:
        if type(tick_index) is not int or tick_index != self.clock.last_completed_tick + 1 or tick_index >= len(self.value.tick_times):
            raise ValueError("minute replay must consume the next original clock observation")
        self.source.verify_unchanged()
        observed = self.value.tick_times[tick_index]
        published = self.raw.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
        after = -1 if not published else published[-1].envelope.sequence
        for record in self.archive_market.list_after(LiveChannel.MARKET_MINUTE, sequence=after):
            if record.envelope.available_at > observed:
                break
            self.raw.publish(record.envelope, self.archive_market.read_payload(record))
        for publication in self.source.constraint_publications:
            if publication.pointer.published_at <= observed:
                current_path = self.root / "constraint-authority" / "current.json"
                if current_path.exists():
                    current = PaperExecutionConstraintPointer.model_validate_json(_read_private(current_path))
                    if current.sequence > publication.pointer.sequence:
                        continue
                published = PaperExecutionConstraintPublisher(root=self.root / "constraint-authority", producer_commit=self.value.producer_commit,
                    clock=lambda: publication.pointer.published_at).publish(publication.batch)
                if published != publication.pointer:
                    raise ValueError("minute replay original constraint publication bytes differ")
        while True:
            summary = self._feature_step(observed, fault_hook=fault_hook)
            run_strategy_live_batch(feature_spool=self.features, candidate_universe_loader=self.loader, runner=self.runner,
                evaluator=self.definition.evaluator, observed_at=observed, limit=1, fault_hook=fault_hook)
            while True:
                routed = route_runner_signals(source_id=self.signal_source.source_id, source=self.signal_source, bus=self.bus, cursors=self.routes,
                    routed_at=observed, target_resolver=self.target_resolver, limit=100)
                if routed.last_sequence == routed.source_high_watermark or routed.deferred_count:
                    break
            while publish_mixed_notification_bus_prefix(bus=self.bus, spool=self.spool, observed_at=observed, limit=100).published_count == 100:
                pass
            reader = ReadonlyNotificationEventRouteSpool(self.spool.paths.root)
            while True:
                consumed = consume_notification_events_to_paper(reader, self.queue, self.consumer, observed_at=observed, limit=100)
                if consumed.ended_at_sequence == consumed.source_high_watermark or consumed.has_deferred_signals:
                    break
            run_paper_signal_batch(self.queue, self.broker, now=observed, trade_date=self.quote.trade_date_at(observed), quote_resolver=self.quote, limit=100)
            self._after_paper_step(observed)
            if fault_hook is not None:
                fault_hook("after_paper_step")
            cursor = self.raw.load_cursor("feature-live", LiveChannel.MARKET_MINUTE)
            after = -1 if cursor is None else cursor.last_sequence
            remaining = self.raw.list_after(LiveChannel.MARKET_MINUTE, sequence=after)
            if not remaining or remaining[0].envelope.available_at > observed:
                break
            if summary.processed_count == 0 and summary.replayed_count == 0:
                raise ValueError("minute replay source cursor made no progress")
        daily = self.clock.daily_valuations
        if observed.astimezone(ZoneInfo("Asia/Shanghai")).time().replace(tzinfo=None) == time(15):
            daily += (self._daily_valuation(observed),)
            if fault_hook is not None:
                fault_hook("after_daily_valuation")
        self.clock = _ReplayClock(input_hash=self.value.input_hash, last_completed_tick=tick_index, daily_valuations=daily)
        _write_clock(self.clock_path, self.clock)

    def _daily_valuation(self, observed: datetime) -> MinuteRuntimeDailyValuation:
        self.broker.require_trusted_ledger()
        with sqlite3.connect(f"{self.broker.path.as_uri()}?mode=ro", uri=True) as connection:
            # Read identities only; original broker computes all quantities, money and NAV.
            held = connection.execute("SELECT ts_code, MIN(entry_signal_id) FROM paper_lot WHERE account_id = ? AND remaining_quantity > 0 GROUP BY ts_code ORDER BY ts_code", (self.broker.account_id,)).fetchall()
        if len(held) > self.source.work.union_codes:
            raise ValueError("minute replay daily holdings exceed the independently bound code union")
        proofs = []
        unavailable = []
        with sqlite3.connect(f"{self.runner.path.as_uri()}?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            for code, entry_signal_id in held:
                row = connection.execute("SELECT * FROM runner_signal WHERE signal_id = ?", (entry_signal_id,)).fetchone()
                if row is None:
                    unavailable.append(f"{code}:missing_original_entry_signal")
                    continue
                signal = StrategyRunnerStore._runner_signal_from_row(row)
                if signal.candidate_id != code or signal.available_at > observed:
                    raise ValueError("minute replay daily holding entry is detached or future")
                try:
                    quote = self.quote.resolve(signal, observed_at=observed)
                except (PaperQuoteResolutionError, PaperExecutionConstraintUnavailableError) as error:
                    unavailable.append(f"{code}:{type(error).__name__}:{error}")
                else:
                    proofs.append(MinuteReplayDailyPriceProof(entry_signal_id=signal.signal_id, quote=quote))
        current_path = self.root / "constraint-authority/current.json"
        constraint_pointer = PaperExecutionConstraintPointer.model_validate_json(_read_private(current_path)) if current_path.exists() else None
        account = None if unavailable else self.broker.account_snapshot(as_of=observed,
            market_prices={proof.quote.ts_code: proof.quote.context.executable_price for proof in proofs})
        return MinuteRuntimeDailyValuation(input_hash=self.value.input_hash, trade_date=self.quote.trade_date_at(observed), as_of=observed,
            observed_at=observed, profile_hash=self.value.execution_profile.profile_hash, calendar_sha256=self.calendar_sha256,
            status="unavailable" if unavailable else "complete", market_pointer=self.raw.current(LiveChannel.MARKET_MINUTE),
            constraint_pointer=constraint_pointer, price_proofs=tuple(proofs), account=account, unavailable_reasons=tuple(unavailable))

    def run(self, *, fault_hook: Callable[[str], None] | None = None) -> MinuteRuntimeReplayResult:
        while self.clock.last_completed_tick + 1 < len(self.value.tick_times):
            self.step(self.clock.last_completed_tick + 1, fault_hook=fault_hook)
        return self.result()

    def result(self) -> MinuteRuntimeReplayResult:
        if self.clock.last_completed_tick != len(self.value.tick_times) - 1:
            raise ValueError("minute replay has not consumed its complete original clock")
        self.source.verify_unchanged()
        self.broker.reconcile()
        signals = tuple(item.signal for item in self.runner.signals_after(sequence=0))
        records = tuple(self.queue.record(signal.signal_id) for signal in signals)
        if any(record is None for record in records):
            raise ValueError("minute replay signal was not consumed through the original route spool")
        queue_records = tuple(record for record in records if record is not None)
        with sqlite3.connect(f"{self.broker.path.as_uri()}?mode=ro", uri=True) as connection:
            identities = connection.execute("SELECT order_id FROM paper_order WHERE account_id = ? ORDER BY created_at, order_id", (self.broker.account_id,)).fetchall()
        orders = tuple(self.broker.order(str(item[0])) for item in identities)
        if any(order is None for order in orders):
            raise ValueError("minute replay original order disappeared")
        last_tick = self.value.tick_times[-1]
        visible = [item for item in self.raw.list_after(LiveChannel.MARKET_MINUTE, sequence=-1) if item.envelope.available_at <= last_tick]
        final = visible[-1]
        frame = MarketMinuteGateway.decode_payload(self.raw.read_payload(final))
        frame = frame[frame.trade_time <= last_tick].sort_values("trade_time").drop_duplicates("ts_code", keep="last")
        prices = {str(row.ts_code): Decimal(str(row.close)) for row in frame.itertuples()}
        account = self.broker.account_snapshot(as_of=last_tick, market_prices=prices)
        incomplete = tuple(record.signal.signal_id for record in queue_records if record.status in {PaperSignalQueueStatus.PENDING, PaperSignalQueueStatus.PREPARED})
        observed_daily = {item.trade_date: item for item in self.clock.daily_valuations}
        daily = tuple(observed_daily.get(day) or MinuteRuntimeDailyValuation(input_hash=self.value.input_hash, trade_date=day,
            as_of=datetime.combine(day, time(15), ZoneInfo("Asia/Shanghai")).astimezone(UTC), observed_at=None,
            profile_hash=self.value.execution_profile.profile_hash, calendar_sha256=self.calendar_sha256,
            status="unavailable", unavailable_reasons=("missing_original_15:00_clock",)) for day in self.value.daily_trade_dates)
        incomplete += tuple(f"daily:{item.trade_date.isoformat()}:{reason}" for item in daily for reason in item.unavailable_reasons)
        return self._result_model()(**self._result_fields(), input_hash=self.value.input_hash, profile_hash=self.value.execution_profile.profile_hash,
            strategy_id=self.value.strategy.strategy_id, strategy_version=self.value.strategy.strategy_version,
            status="incomplete" if incomplete else "complete", incomplete_reasons=incomplete, work=self.source.work,
            daily_status="unavailable" if any(item.status == "unavailable" for item in daily) else "complete",
            daily_valuations=daily, execution_profile=self.value.execution_profile, result_budget=self.value.result_budget,
            signals=signals, orders=tuple(order for order in orders if order is not None), fills=self.broker.fills(), queue_records=queue_records, account=account)


def run_minute_runtime_replay(
    value: FrozenMinuteRuntimeInput, *, expected: MinuteRuntimeSourceReceipt, research_root: Path
) -> MinuteRuntimeReplayResult:
    source = restore_minute_runtime_source(value, expected=expected, research_root=research_root)
    return MinuteRuntimeReplayRunner(source).run()


def minute_runtime_result_tables(result: MinuteRuntimeReplayResult) -> dict[str, pd.DataFrame]:
    summary = MinuteRuntimeReplaySummary.model_validate({name: getattr(result, name) for name in MinuteRuntimeReplaySummary.model_fields})
    return _minute_runtime_result_tables(result, summary=summary, work_units=result.work.work_units)


def _minute_runtime_result_tables(result: MinuteRuntimeReplayResult, *, summary: MinuteRuntimeReplaySummary,
    work_units: int,
) -> dict[str, pd.DataFrame]:
    def rows(values: tuple[RuntimeContractModel, ...]) -> pd.DataFrame:
        return pd.DataFrame({"sequence": list(range(1, len(values) + 1)),
            "payload": [item.model_dump_json() for item in values]})
    return {"signals": rows(result.signals), "orders": rows(result.orders), "fills": rows(result.fills),
        "paper_queue": rows(result.queue_records), "account": rows((result.account,)),
        "daily_valuations": rows(result.daily_valuations), "execution_profile": rows((result.execution_profile,)),
        "replay_summary": pd.DataFrame([{"input_hash": result.input_hash, "profile_hash": result.profile_hash,
            "strategy_id": result.strategy_id, "strategy_version": result.strategy_version, "status": result.status, "daily_status": result.daily_status,
            "work_units": work_units, "incomplete_reasons": json.dumps(result.incomplete_reasons), "payload": summary.model_dump_json()}])}
