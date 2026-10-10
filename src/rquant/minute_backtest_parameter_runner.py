"""Parameter definitions on the original sequential strategy and paper chain."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Self
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, SerializerFunctionWrapHandler, model_serializer, model_validator

from rquant.auction_gap_strategy import (
    _b_day_strength, _next_auction_is_weak, auction_morning_vwap_break,
    resolve_auction_hold_policy,
)
from rquant.feature_contracts import FeatureContract
from rquant.feature_live_service import FeatureLiveBatchSummary, _next_feature_sequence
from rquant.live_contracts import ConsumerCursor, LiveChannel
from rquant.minute_backtest_parameter_contracts import (
    FrozenMinuteParameterInput, MinuteParameterRuntimeReceipt,
    MinuteParameterStrategyBinding, MinuteParameterWork,
)
from rquant.minute_backtest_parameter_definition import (
    bootstrap_minute_parameter_definition, build_minute_parameter_definition,
    minute_parameter_feature_contract,
)
from rquant.minute_backtest_parameter_features import (
    PARAMETER_BAR_FEATURE, PARAMETER_CANDIDATE_FEATURE, MinuteParameterBar,
    MinuteParameterCandidate, MinuteParameterLifecycle, MinuteParameterLifecycles,
    MinuteParameterPosition, MinuteParameterSessionFacts,
    project_minute_parameter_features, publish_minute_parameter_features,
)
from rquant.minute_backtest_parameter_source import (
    MinuteParameterProjection, parameter_archive_projections, restore_minute_parameter_source,
)
from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding
from rquant.minute_backtest_parameters import (
    MinuteAuctionGapParameters, MinuteNShapeParameters, MinuteParameterSet,
)
from rquant.minute_backtest_runner import (
    MinuteRuntimeReplayResult, MinuteRuntimeReplayRunner, MinuteRuntimeReplaySummary,
    _minute_runtime_result_tables,
)
from rquant.minute_replay import _MinuteQuote
from rquant.paper import adjust_open_position_price_basis, mark_position_to_quote, open_position_from_signal
from rquant.paper_contracts import PaperFill, PaperSide
from rquant.paper_signal_worker import PaperSignalQueueRecord
from rquant.runtime_contracts import canonical_sha256
from rquant.signal_contracts import SignalAction, SignalEnvelopeFamily
from rquant.strategy_evaluators import BuiltinStrategyDefinition, BuiltinStrategyEvaluatorRegistry
from rquant.strategy_runner import StrategyCandidateState
from rquant.strategy_spec import StrategyLifecycleState
from rquant.strict_json import strict_json_loads
from rquant.volume_profile import build_volume_profile_risk_plan

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class MinuteParameterReplaySummary(MinuteRuntimeReplaySummary):
    strategy_id: str = Field(pattern=r"^(?:np|ap|gp)\.[a-z2-7]{52}$")
    parameters: MinuteParameterSet
    parameter_work: MinuteParameterWork
    study_binding: MinuteParameterStudyBinding | None = None

    @model_serializer(mode="wrap")
    def original_default_fields(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        value = handler(self)
        if self.study_binding is None:
            value.pop("study_binding", None)
        return value

    @model_validator(mode="after")
    def complete_parameter_summary(self) -> Self:
        if (self.strategy_id != self.parameters.definition_id or self.strategy_version != self.parameters.definition_version
                or self.work != self.parameter_work.runtime_work):
            raise ValueError("parameter summary differs from its complete definition or work")
        return self


class MinuteParameterReplayResult(MinuteRuntimeReplayResult):
    strategy_id: str = Field(pattern=r"^(?:np|ap|gp)\.[a-z2-7]{52}$")
    parameters: MinuteParameterSet
    parameter_work: MinuteParameterWork
    study_binding: MinuteParameterStudyBinding | None = None

    @model_serializer(mode="wrap")
    def original_default_fields(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        value = handler(self)
        if self.study_binding is None:
            value.pop("study_binding", None)
        return value

    @model_validator(mode="after")
    def complete_parameter_result(self) -> Self:
        MinuteParameterReplaySummary.model_validate({name: getattr(self, name)
            for name in MinuteParameterReplaySummary.model_fields})
        if any(signal.strategy_id != self.strategy_id or signal.strategy_version != str(self.strategy_version)
                or signal.evidence.get("minute_parameter_set_hash") != self.parameters.fingerprint
                for signal in self.signals):
            raise ValueError("parameter result signal differs from its complete recipe or native head")
        if self.study_binding is not None and any(signal.action is SignalAction.B_INTENT
                and signal.evidence.get("minute_study_binding_hash") != self.study_binding.binding_hash
                for signal in self.signals):
            raise ValueError("parameter study result entry lacks its full source-bound selection")
        return self


@dataclass(frozen=True)
class _ParameterWatch:
    ts_code: str
    name: str
    pool: str
    entry_date: date
    reference_date: date
    limit_up_date: date
    t_close: float
    t_high: float | None
    limit_up_price_next: float
    stop_weak: float


class MinuteParameterReplayRunner(MinuteRuntimeReplayRunner):
    def _check_input_type(self) -> None:
        if type(self.value) is not FrozenMinuteParameterInput:
            raise TypeError("parameter runner requires the complete parameter input")

    def _execution_definition(self) -> tuple[BuiltinStrategyEvaluatorRegistry | None, BuiltinStrategyDefinition, FeatureContract]:
        value = self.value
        record = bootstrap_minute_parameter_definition(self.root / "definitions", value.parameters,
            producer_commit=value.producer_commit, registered_at=value.available_at, available_at=value.available_at)
        if MinuteParameterStrategyBinding.from_registration(record, parameters=value.parameters,
                producer_commit=value.producer_commit) != value.strategy:
            raise PermissionError("parameter replay bootstrap differs from the full original registration")
        definition = build_minute_parameter_definition(value.parameters, producer_commit=value.producer_commit)
        self.projections = parameter_archive_projections(value.parameters, materials=value.materials,
            tick_times=value.tick_times, warmup_available_at=value.warmup_available_at)
        self.session_facts = {(item.trade_date, item.ts_code): item for item in value.session_facts}
        costs = value.execution_profile.execution_costs.slippage
        paper = value.parameters.parameters.paper
        if Decimal(str(paper.entry_slippage_pct)) * 10000 != costs.buy_bps:
            raise PermissionError("parameter slippage differs from the complete original broker execution profile")
        return None, definition, minute_parameter_feature_contract(definition)

    def _result_model(self) -> type[MinuteParameterReplayResult]:
        return MinuteParameterReplayResult

    def _result_fields(self) -> dict[str, object]:
        return {"parameters": self.value.parameters, "parameter_work": self.value.parameter_work,
            "study_binding": self.value.study_binding}

    def _feature_step(self, observed: datetime, *, fault_hook: Callable[[str], None] | None) -> FeatureLiveBatchSummary:
        descriptor = self.raw.source_descriptor(LiveChannel.MARKET_MINUTE)
        cursor = self.raw.load_cursor("feature-live", LiveChannel.MARKET_MINUTE)
        after = -1 if cursor is None else cursor.last_sequence
        point = next((item for item in self.projections if item.raw_sequence is not None
            and item.raw_sequence > after and item.cutoff <= observed), None)
        if point is None:
            point = next((item for item in self.projections if item.raw_sequence is None and item.cutoff == observed), None)
        processed = replayed = stale = 0
        if point is not None:
            sequence, is_replay = _next_feature_sequence(self.features, input_batch_ids=point.input_batch_ids)
            if not is_replay:
                lifecycles = self._lifecycles(point)
                new_entry_codes = None
                if self.value.study_binding is not None:
                    universe = self.loader.load(as_of=point.cutoff,
                        required_trade_date=point.cutoff.astimezone(_SHANGHAI).date())
                    actual = {candidate.ts_code: candidate for candidate in point.candidates
                        if candidate.trade_date == universe.required_trade_date}
                    ready = set()
                    for code_evidence in universe.code_evidence:
                        for hit in code_evidence.hits:
                            candidate = actual.get(hit.candidate_id)
                            if candidate is None or hit.static_features.get(PARAMETER_CANDIDATE_FEATURE) != candidate.model_dump_json():
                                raise PermissionError("study new-entry prefix differs from the original candidate loader")
                            state = self.runner.candidate_occurrence_state(hit.occurrence_id)
                            if state is None or state.state is StrategyLifecycleState.IDLE:
                                ready.add(hit.candidate_id)
                    new_entry_codes = tuple(sorted(ready))
                publish_minute_parameter_features(self.features, parameters=self.value.parameters,
                    candidates=point.candidates, minutes=point.minutes, historical_minutes=point.historical_minutes,
                    source_frequency=self.value.source_frequency, decision_cutoff=point.cutoff, sequence=sequence,
                    input_batch_ids=point.input_batch_ids, producer_commit=self.value.producer_commit,
                    lifecycles=lifecycles, source_quality=point.quality,
                    study_binding=self.value.study_binding, new_entry_codes=new_entry_codes)
            if fault_hook is not None:
                fault_hook("after_feature_publish")
            if point.raw_sequence is not None:
                original = self.raw.list_after(LiveChannel.MARKET_MINUTE,
                    sequence=point.raw_sequence-1, limit=1)[0].envelope
                self.raw.commit_cursor(ConsumerCursor(consumer_id="feature-live", channel=LiveChannel.MARKET_MINUTE,
                    source_generation_id=descriptor.generation_id, last_sequence=original.sequence,
                    last_batch_id=original.batch_id, last_content_sha256=original.content_sha256,
                    updated_at=observed))
                after = original.sequence
            processed, replayed = int(not is_replay), int(is_replay)
            stale = int(point.quality.value == "STALE")
        feature = self.features.current()
        return FeatureLiveBatchSummary(observed_at=observed, source_generation_id=descriptor.generation_id,
            source_high_watermark=descriptor.high_watermark, started_after_sequence=-1 if cursor is None else cursor.last_sequence,
            last_raw_sequence=after, feature_high_watermark=-1 if feature is None else feature.sequence,
            processed_count=processed, replayed_count=replayed, stale_count=stale,
            has_deferred_batches=after < descriptor.high_watermark)

    def _lifecycles(self, point: MinuteParameterProjection) -> dict[str, MinuteParameterLifecycles]:
        self.broker.require_trusted_ledger()
        with sqlite3.connect(f"{self.broker.path.as_uri()}?mode=ro", uri=True) as connection:
            entries = dict(connection.execute("SELECT entry_signal_id, SUM(remaining_quantity) FROM paper_lot "
                "WHERE account_id = ? AND remaining_quantity > 0 GROUP BY entry_signal_id", (self.broker.account_id,)).fetchall())
        signals = {record.signal.signal_id: record.signal for record in self.runner.signals_after(sequence=0)}
        by_code: dict[str, list[MinuteParameterLifecycle]] = {}
        candidates = {item.ts_code: item for item in point.candidates}
        for identity in sorted(entries):
            signal = signals.get(identity)
            if signal is None or signal.action is not SignalAction.B_INTENT:
                raise PermissionError("parameter holding lacks its original entry signal")
            if signal.candidate_id not in candidates:
                raise PermissionError("parameter holding has no visible original market prefix")
            record = self.queue.record(signal.signal_id)
            if record is None or record.quote is None or record.order is None:
                raise PermissionError("parameter holding lacks its complete original prepared quote and order")
            order = self.broker.order(record.order.order_id)
            if order != record.order or order.side is not PaperSide.BUY:
                raise PermissionError("parameter holding queue differs from the original broker order")
            fills = tuple(fill for fill in self.broker.fills() if fill.order_id == order.order_id)
            if len(fills) != 1 or sum(fill.quantity for fill in fills) != order.filled_quantity:
                raise PermissionError("parameter risk basis requires the original complete single entry fill")
            fill = fills[0]
            if (fill.price_snapshot_id != record.quote.snapshot_id or fill.executed_at > point.cutoff
                    or record.quote.available_at > fill.executed_at):
                raise PermissionError("parameter entry quote differs from its actual visible broker fill")
            transition = signal.evidence.get("runner_transition", {})
            state_key = transition.get("candidate_occurrence_id")
            if not isinstance(state_key, str):
                raise PermissionError("parameter holding lacks its original candidate occurrence")
            state = self.runner.candidate_occurrence_state(state_key)
            if state is None or state.candidate_id != signal.candidate_id:
                raise PermissionError("parameter holding belongs to another original occurrence")
            raw = signal.evidence.get(PARAMETER_CANDIDATE_FEATURE)
            if not isinstance(raw, str):
                raise PermissionError("parameter holding lacks its complete original candidate facts")
            entry_candidate = MinuteParameterCandidate.model_validate(strict_json_loads(raw))
            values = project_minute_parameter_features(self.value.parameters, candidates[signal.candidate_id],
                point.minutes, point.historical_minutes, source_frequency=self.value.source_frequency,
                decision_cutoff=point.cutoff)
            bar = MinuteParameterBar.model_validate_json(values[PARAMETER_BAR_FEATURE])
            lifecycle = self._position_lifecycle(point, bar, entry_candidate, state, signal, record, fill, entries[identity])
            by_code.setdefault(signal.candidate_id, []).append(lifecycle)
        return {code: MinuteParameterLifecycles(parameter_hash=self.value.parameters.fingerprint,
            ts_code=code, positions=tuple(sorted(items, key=lambda item: item.candidate_state_key)))
            for code, items in by_code.items()}

    def _facts(self, code: str, day: date, cutoff: datetime) -> MinuteParameterSessionFacts:
        item = self.session_facts.get((day, code))
        if item is None or item.basis_available_at > cutoff:
            raise PermissionError("parameter holding lacks its original PIT session price basis")
        return item

    def _position_lifecycle(self, point: MinuteParameterProjection, bar: MinuteParameterBar,
        candidate: MinuteParameterCandidate, state: StrategyCandidateState, signal: SignalEnvelopeFamily,
        record: PaperSignalQueueRecord, fill: PaperFill, remaining_quantity: int,
    ) -> MinuteParameterLifecycle:
        config = self.value.parameters.parameters
        code = candidate.ts_code
        entry_day = fill.executed_at.astimezone(_SHANGHAI).date()
        actual_day = point.cutoff.astimezone(_SHANGHAI).date()
        opens = self.value.market_calendar.open_dates
        next_dates = tuple(day for day in opens if day > entry_day)
        if not next_dates:
            raise PermissionError("parameter position lacks its original next trading date")
        original = next((item for item in reversed(self.projections) if item.cutoff <= fill.executed_at
            and item.cutoff.astimezone(_SHANGHAI).date() == entry_day), None)
        if original is None or record.quote is None:
            raise PermissionError("parameter entry lacks its visible original raw quote prefix")
        entry_rows = original.minutes[(original.minutes.ts_code == code)
            & (pd.to_datetime(original.minutes.trade_time, utc=True) == record.quote.event_time)]
        if len(entry_rows) != 1:
            raise PermissionError("parameter entry quote is detached from its original raw row")
        row = entry_rows.iloc[0]
        if Decimal(str(row.close)) != record.quote.context.executable_price:
            raise PermissionError("parameter entry raw close differs from the broker quote")
        watch = _ParameterWatch(code, candidate.name, candidate.pool, candidate.trade_date, candidate.reference_date,
            candidate.reference_date, candidate.t_close, candidate.t_high, candidate.limit_up_price, candidate.stop_weak)
        risk = None
        if isinstance(config, MinuteNShapeParameters):
            risk = build_volume_profile_risk_plan(list(candidate.volume_profiles),
                entry_price=float(record.quote.context.executable_price) * (1 + config.paper.entry_slippage_pct),
                config=config.volume_profile)
            if not risk.entry_allowed:
                raise PermissionError("parameter actual fill violates its original volume-profile risk gate")
        position = open_position_from_signal(watch,
            _MinuteQuote(code, float(record.quote.context.executable_price), float(row.low), float(row.high)),
            {"level": config.entry_mode, "level_price": candidate.t_high if isinstance(config, MinuteNShapeParameters)
                else candidate.limit_up_price}, fill.executed_at.astimezone(_SHANGHAI), config.paper,
            earliest_exit_date=next_dates[0], risk_plan=risk)
        day = entry_day
        facts_used = []
        for observed in self.projections:
            if observed.cutoff < fill.executed_at or observed.cutoff >= point.cutoff:
                continue
            observed_day = observed.cutoff.astimezone(_SHANGHAI).date()
            raw = observed.minutes[observed.minutes.ts_code == code]
            if raw.empty:
                continue
            if observed_day != day:
                facts = self._facts(code, observed_day, observed.cutoff)
                position = adjust_open_position_price_basis(position, facts.previous_close, facts.session_pre_close)
                facts_used.append(canonical_sha256(facts.model_dump(mode="json")))
                day = observed_day
            latest = raw.sort_values("trade_time").iloc[-1]
            position = mark_position_to_quote(position,
                _MinuteQuote(code, float(latest.close), float(latest.low), float(latest.high)))
        if actual_day != day:
            facts = self._facts(code, actual_day, point.cutoff)
            position = adjust_open_position_price_basis(position, facts.previous_close, facts.session_pre_close)
            facts_used.append(canonical_sha256(facts.model_dump(mode="json")))
        held = len(tuple(day for day in opens if entry_day < day <= actual_day))
        max_hold, hold_policy, auction_reason = config.max_hold_days, "fixed", None
        if isinstance(config, MinuteAuctionGapParameters) and held:
            complete = tuple(item for item in self.projections if item.cutoff.astimezone(_SHANGHAI).date() == entry_day
                and item.cutoff.astimezone(_SHANGHAI).time().replace(tzinfo=None) == time(15)
                and item.cutoff <= point.cutoff)
            if len(complete) != 1:
                raise PermissionError("auction holding lacks its original completed entry-session observation")
            entry_minutes = complete[0].minutes[complete[0].minutes.ts_code == code].copy()
            entry_minutes["trade_time"] = pd.to_datetime(entry_minutes.trade_time, utc=True).dt.tz_convert(_SHANGHAI).dt.tz_localize(None)
            strength = _b_day_strength(entry_minutes, trading_date=entry_day,
                limit_up_price=candidate.limit_up_price, price_tol=config.price_tol)
            if config.seal_hold_enabled and strength["b_close_at_limit_up"]:
                seal = self._facts(code, entry_day, point.cutoff)
                if not seal.seal_query_complete or seal.seal_available_at > point.cutoff:
                    raise PermissionError("auction seal-hold lacks its original visible complete official query")
                official = None if seal.official_seal is None else pd.Series(seal.official_seal.model_dump())
                facts_used.append(canonical_sha256(seal.model_dump(mode="json")))
            else:
                official = None
            hold_policy, max_hold = resolve_auction_hold_policy(strength, config=config.owner_config(), official=official)
            if actual_day == next_dates[0]:
                auction = self._facts(code, actual_day, point.cutoff)
                if not auction.auction_query_complete or auction.auction_available_at > point.cutoff:
                    raise PermissionError("auction exit lacks its original visible complete auction query")
                facts_used.append(canonical_sha256(auction.model_dump(mode="json")))
                if auction.auction_price is not None and _next_auction_is_weak(position,
                        next_auction_price=auction.auction_price, b_day_close=float(entry_minutes.iloc[-1].close),
                        b_strength=strength, config=config.owner_config()):
                    auction_reason = "next_auction_weak"
            if auction_reason is None and auction_morning_vwap_break(position,
                    quote_time=bar.event_time.astimezone(_SHANGHAI).replace(tzinfo=None), price=bar.close,
                    day_vwap=bar.vwap, config=config.owner_config()):
                auction_reason = "next_morning_vwap_break"
        return MinuteParameterLifecycle(parameter_hash=self.value.parameters.fingerprint,
            candidate_state_key=state.state_key, entry_signal_id=signal.signal_id,
            runtime_input_hash=self.value.input_hash, account_id=self.broker.account_id,
            entry_record=record, entry_fill=fill, remaining_quantity=remaining_quantity,
            position=MinuteParameterPosition.model_validate(position.model_dump(mode="python")),
            position_available_at=max(fill.executed_at, record.quote.available_at),
            holding_trading_sessions=held, sellable=actual_day >= position.earliest_exit_date,
            bar=bar, max_hold_days=max_hold, hold_policy=hold_policy, auction_exit_reason=auction_reason,
            session_fact_hashes=tuple(sorted(set(facts_used))))


def run_minute_parameter_replay(value: FrozenMinuteParameterInput, *, expected: MinuteParameterRuntimeReceipt,
    research_root: Path,
) -> MinuteParameterReplayResult:
    source = restore_minute_parameter_source(value, expected=expected, research_root=research_root)
    return MinuteParameterReplayRunner(source).run()


def minute_parameter_result_tables(result: MinuteParameterReplayResult) -> dict[str, pd.DataFrame]:
    summary = MinuteParameterReplaySummary.model_validate({name: getattr(result, name)
        for name in MinuteParameterReplaySummary.model_fields})
    return _minute_runtime_result_tables(result, summary=summary, work_units=result.parameter_work.work_units)
