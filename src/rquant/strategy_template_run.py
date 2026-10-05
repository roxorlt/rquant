"""Exact template inputs and decisions around the original private paper ledger."""

from __future__ import annotations

import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self
from zoneinfo import ZoneInfo

from pydantic import Field, field_validator, model_validator

from rquant.backtest.contracts import (
    BacktestDayResult,
    BacktestDecision,
    BacktestInstrument,
    BacktestOrder,
    BacktestRequest,
    Sha256,
    SkippedTarget,
    TradeConditions,
)
from rquant.backtest.runner import _can_submit, _order, _should_rebalance
from rquant.definition_registry import StrategySpecRegistration
from rquant.paper_broker import BrokerCostPolicy, BrokerExecutionContext, PaperBrokerStore
from rquant.paper_contracts import PaperOrderIntent, PaperOrderType, PaperSide
from rquant.portfolio.drawdown import DrawdownDecision, DrawdownState, evaluate_drawdown
from rquant.portfolio.weights import allocate_target_weights
from rquant.portfolio_backtest_models import PortfolioBacktestConfig, PortfolioSourceManifest
from rquant.research_run_spec import _parse_decimal
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strategy_authoring_source import produce_template_entry, template_source_code_identity
from rquant.strategy_template import (
    StrategyTemplate,
    compile_strategy_template,
)
from rquant.strategy_template_execution import (
    TemplateEntryProjection,
    TemplatePosition,
    TemplatePrice,
    strategy_template_entry,
    strategy_template_exit,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _at(trade_date: date, hour: int, minute: int) -> datetime:
    return datetime.combine(trade_date, time(hour, minute), tzinfo=_SHANGHAI)


class TemplateIndexClose(RuntimeContractModel):
    trade_date: date
    price: Decimal = Field(gt=0, allow_inf_nan=False)
    observed_at: AwareUtcDatetime
    source_hash: Sha256
    benchmark_code: str

    @field_validator("price", mode="before")
    @classmethod
    def validate_price_representation(cls, value: object) -> Decimal:
        return _parse_decimal(value, field_name="template index close")


class TemplateMinuteExecution(RuntimeContractModel):
    quote: TemplatePrice
    execution_at: AwareUtcDatetime
    execution_price: Decimal = Field(gt=0, allow_inf_nan=False)
    execution_source_hash: Sha256
    conditions: TradeConditions

    @field_validator("execution_price", mode="before")
    @classmethod
    def validate_price_representation(cls, value: object) -> Decimal:
        return _parse_decimal(value, field_name="template minute execution price")

    @model_validator(mode="after")
    def validate_execution(self) -> Self:
        if (
            self.quote.basis != "minute_close"
            or self.execution_at <= self.quote.observed_at
            or self.quote.event_time > self.quote.observed_at
            or self.conditions.observed_at != self.execution_at
        ):
            raise ValueError("minute execution needs later price and matching status evidence")
        if self.execution_at != self.quote.event_time + timedelta(minutes=1):
            raise ValueError("minute execution date and time must be the exact next minute")
        return self


class TemplateDayEvidence(RuntimeContractModel):
    trade_date: date
    entry: TemplateEntryProjection
    index_closes: tuple[TemplateIndexClose, ...] = ()
    minutes: tuple[TemplateMinuteExecution, ...] = ()


def template_index_allows_entry(
    rules: StrategyTemplate, day: TemplateDayEvidence, request: BacktestRequest
) -> bool:
    condition = rules.index_filter
    if condition is None:
        if day.index_closes:
            raise ValueError("disabled index filter does not accept extra window")
        return True
    index = request.calendar.dates.index(day.trade_date)
    expected_dates = request.calendar.dates[max(0, index - condition.ma_days) : index]
    if (
        len(expected_dates) != condition.ma_days
        or tuple(item.trade_date for item in day.index_closes) != expected_dates
    ):
        raise ValueError("index window is missing exact preceding trading dates")
    cutoff = _at(day.trade_date, 9, 25)
    if any(
        item.benchmark_code != condition.benchmark_code or item.observed_at > cutoff
        for item in day.index_closes
    ):
        raise ValueError("index source differs or has future observations")
    average = sum((item.price for item in day.index_closes), Decimal("0")) / Decimal(
        condition.ma_days
    )
    return (
        day.index_closes[-1].price > average
        if condition.direction == "above"
        else day.index_closes[-1].price < average
    )


class FrozenStrategyTemplateInput(RuntimeContractModel):
    contract: Literal["strategy-template-input/v1"] = "strategy-template-input/v1"
    owner_id: str = Field(min_length=1, max_length=128)
    rules: StrategyTemplate
    definition: StrategySpecRegistration
    request: BacktestRequest
    source_code_identity: Sha256
    sources: PortfolioSourceManifest
    source_material_hash: Sha256
    catalog_generation_id: str = Field(min_length=1, max_length=128)
    days: tuple[TemplateDayEvidence, ...]
    input_hash: Sha256 | None = None

    @model_validator(mode="before")
    @classmethod
    def validate_request_numbers(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        request = value.get("request")
        data = (
            request.model_dump(mode="python") if isinstance(request, BacktestRequest) else request
        )
        if not isinstance(data, Mapping):
            return value
        PortfolioBacktestConfig.validate_numeric_admission(data)
        risk = data.get("drawdown_rule")
        risk = risk.model_dump(mode="python") if hasattr(risk, "model_dump") else risk
        if isinstance(risk, Mapping):
            for name in ("trigger_drawdown", "release_drawdown", "total_risk_weight_cap"):
                if risk.get(name) is not None:
                    _parse_decimal(risk[name], field_name=name)
        for day in data.get("days", ()):
            if not isinstance(day, Mapping):
                continue
            ranking = day.get("ranking", {})
            if isinstance(ranking, Mapping):
                for candidate in ranking.get("candidates", ()):
                    if isinstance(candidate, Mapping) and "rank_score" in candidate:
                        _parse_decimal(candidate["rank_score"], field_name="rank score")
            for instrument in day.get("instruments", ()):
                if isinstance(instrument, Mapping):
                    for name in ("decision_price", "open_price", "close_price"):
                        if instrument.get(name) is not None:
                            _parse_decimal(instrument[name], field_name=name)
        return value

    @model_validator(mode="after")
    def validate_complete_input(self) -> Self:
        expected_spec = compile_strategy_template(
            self.rules,
            strategy_id=self.definition.logical_id,
            version=self.definition.version,
            producer_commit=self.definition.producer_commit,
        )
        if expected_spec.spec_fingerprint != self.definition.spec.spec_fingerprint:
            raise ValueError("template rules differ from exact definition")
        if self.request.producer_commit != self.definition.producer_commit:
            raise ValueError("template code differs from original definition")
        if (self.request.weight_rule, self.request.rebalance_rule) != (
            self.rules.weight_rule,
            self.rules.rebalance_rule,
        ):
            raise ValueError("template weights or rebalance differ from frozen request")
        if self.source_code_identity != template_source_code_identity():
            raise ValueError("template source producer code identity differs")
        if tuple(day.trade_date for day in self.days) != tuple(
            day.trade_date for day in self.request.days
        ):
            raise ValueError("template evidence does not cover exact trading days")
        codes: set[str] = set()
        pairs = 0
        for day, evidence in zip(self.request.days, self.days, strict=True):
            raw = evidence.entry.evidence
            day_codes = {
                item.ts_code for item in (*day.instruments, *day.ranking.candidates, *raw.signals)
            }
            day_codes.update(raw.pool_codes)
            day_codes.update(item.quote.ts_code for item in evidence.minutes)
            day_codes.update(
                row["ts_code"] for row in raw.rows if isinstance(row.get("ts_code"), str)
            )
            codes.update(day_codes)
            pairs += len(day_codes) + len(evidence.minutes)
        if len(codes) > 500 or pairs > 20000:
            raise ValueError("template input exceeds code or pair budget")
        for day, evidence in zip(self.request.days, self.days, strict=True):
            cutoff = _at(day.trade_date, 9, 25)
            projection = produce_template_entry(
                self.rules, evidence.entry.evidence, decision_time=cutoff
            )
            if projection != evidence.entry:
                raise ValueError("template entry projection differs from raw source facts")
            selected = set(strategy_template_entry(self.rules, projection, decision_time=cutoff))
            if not selected <= {item.ts_code for item in day.instruments}:
                raise ValueError("template entry has unavailable instruments")
            template_index_allows_entry(self.rules, evidence, self.request)
            if self.rules.exit.exit_time is not None:
                hour, minute = map(int, self.rules.exit.exit_time.split(":"))
                required_time = _at(day.trade_date, hour, minute)
                if {
                    item.quote.ts_code
                    for item in evidence.minutes
                    if item.quote.event_time == required_time
                    and item.quote.observed_at == required_time
                } != {item.ts_code for item in day.instruments}:
                    raise ValueError(
                        "timed exit requires actual minute price/status observations for every instrument"
                    )
                if len(evidence.minutes) != len(day.instruments) or any(
                    item.quote.event_time != required_time
                    or item.quote.observed_at != required_time
                    or item.execution_at > _at(day.trade_date, 15, 1)
                    for item in evidence.minutes
                ):
                    raise ValueError(
                        "minute evidence differs from exact rule date or valuation window"
                    )
            elif evidence.minutes:
                raise ValueError("extra minute evidence requires a timed exit rule")
        expected_hash = canonical_sha256(self.model_dump(mode="python", exclude={"input_hash"}))
        if self.input_hash is None:
            object.__setattr__(self, "input_hash", expected_hash)
        elif self.input_hash != expected_hash:
            raise ValueError("template input hash differs from complete content")
        if len(self.model_dump_json().encode()) > 16 * 1024 * 1024:
            raise ValueError("template input exceeds byte budget")
        return self


class TemplateExitDecision(RuntimeContractModel):
    trade_date: date
    ts_code: str
    entry_signal_id: Sha256
    decision_id: Sha256
    reason: Literal[
        "stop_loss", "trailing_profit", "take_profit", "max_holding_days", "exit_time", "rebalance"
    ]
    decided_at: AwareUtcDatetime


class StrategyTemplateResult(RuntimeContractModel):
    contract: Literal["strategy-template-result/v1"] = "strategy-template-result/v1"
    execution_convention: Literal["SIMULATED_TEMPLATE_REPLAY"] = "SIMULATED_TEMPLATE_REPLAY"
    owner_id: str
    strategy_id: str
    version: int = Field(strict=True, ge=1)
    definition_fingerprint: Sha256
    definition_record_hash: Sha256
    input_hash: Sha256
    calendar_source_identity: Sha256
    cost_spec_id: Sha256
    status: Literal["complete", "incomplete"]
    days: tuple[BacktestDayResult, ...]
    exit_decisions: tuple[TemplateExitDecision, ...]
    content_hash: Sha256 | None = None

    @model_validator(mode="after")
    def bind_result(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash is None:
            object.__setattr__(self, "content_hash", expected)
        elif self.content_hash != expected:
            raise ValueError("template result hash differs")
        if not self.days or self.status != (
            "complete" if self.days[-1].account is not None else "incomplete"
        ):
            raise ValueError("template result completeness differs from ledger valuation")
        return self


@dataclass
class _Entry:
    ts_code: str
    signal_id: str
    remaining: int
    acquired_on: date
    price: Decimal
    high: Decimal


def _quantities(entries: list[_Entry]) -> dict[str, int]:
    result: dict[str, int] = {}
    for entry in entries:
        if entry.remaining:
            result[entry.ts_code] = result.get(entry.ts_code, 0) + entry.remaining
    return result


def _decision(
    value: FrozenStrategyTemplateInput,
    instrument: BacktestInstrument,
    *,
    quantity: int,
    side: PaperSide,
    at: datetime,
    entry_id: str | None = None,
    price_hash: str | None = None,
) -> BacktestDecision:
    return BacktestDecision(
        decided_at=at,
        producer_commit=value.request.producer_commit,
        strategy_config_id=value.definition.fingerprint,
        ranking_source_identity=value.input_hash,
        reference_price_snapshot_id=price_hash or instrument.decision_price_source_identity,
        ts_code=instrument.ts_code,
        side=side.value,
        quantity=quantity,
        entry_signal_id=entry_id,
    )


def _minute_order(
    broker: PaperBrokerStore,
    value: FrozenStrategyTemplateInput,
    instrument: BacktestInstrument,
    minute: TemplateMinuteExecution,
    decision: BacktestDecision,
    *,
    trade_date: date,
) -> BacktestOrder:
    authority = broker.sell_quantity_authority(
        exit_signal_id=decision.decision_id,
        entry_signal_id=decision.entry_signal_id,
        ts_code=instrument.ts_code,
        action="S_INTENT",
        tranche_fraction=Decimal("1"),
        decision_cutoff=minute.execution_at,
        trade_date=trade_date,
    )
    intent = PaperOrderIntent(
        signal_id=decision.decision_id,
        entry_signal_id=decision.entry_signal_id,
        sell_quantity_authority=authority,
        account_id=broker.account_id,
        ts_code=instrument.ts_code,
        side=PaperSide.SELL,
        order_type=PaperOrderType.MARKET,
        quantity=authority.requested_quantity,
        event_time=minute.quote.event_time,
        available_at=minute.quote.observed_at,
        earliest_execution_at=minute.execution_at,
        expires_at=minute.execution_at + timedelta(minutes=1),
        price_snapshot_id=minute.execution_source_hash,
        producer_commit=value.request.producer_commit,
    )
    execution_id = canonical_sha256({"template": value.input_hash, "intent_id": intent.intent_id})
    broker.submit_intent(
        intent,
        execution_id=execution_id,
        decision_time=minute.execution_at,
        persisted_at=minute.execution_at,
        trade_date=trade_date,
        quote=BrokerExecutionContext(
            executable_price=minute.execution_price,
            instrument_context=instrument.instrument_context,
            suspended=minute.conditions.suspended,
            limit_locked=minute.conditions.sell_limit_locked,
        ),
    )
    receipt = broker.execution(execution_id)
    if receipt is None:
        raise RuntimeError("template minute order lost its original broker receipt")
    return BacktestOrder(decision=decision, intent=intent, receipt=receipt)


def execute_strategy_template_input(
    value: FrozenStrategyTemplateInput, *, research_root: Path
) -> StrategyTemplateResult:
    value = FrozenStrategyTemplateInput.model_validate(value.model_dump(mode="python"))
    root = Path(research_root)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("template research root must be a real directory")
    entries: list[_Entry] = []
    results: list[BacktestDayResult] = []
    exit_log: list[TemplateExitDecision] = []
    previous_nav, previous_cash = value.request.initial_cash, value.request.initial_cash
    risk_state: DrawdownState | None = None
    risk: DrawdownDecision | None = None
    if value.request.drawdown_rule is not None:
        first = value.request.calendar.dates.index(value.request.days[0].trade_date)
        risk = evaluate_drawdown(
            previous_nav,
            _at(value.request.calendar.dates[first - 1], 15, 1),
            value.request.drawdown_rule,
        )
        risk_state = risk.state
    with tempfile.TemporaryDirectory(prefix="strategy-template-", dir=root) as private:
        broker = PaperBrokerStore(
            Path(private) / "ledger.sqlite",
            account_id="template-" + value.input_hash[:20],
            initial_cash=previous_cash,
            cost_policy=BrokerCostPolicy.from_execution_cost_spec(
                value.request.execution_cost_spec
            ),
        )
        for day_index, (day, evidence) in enumerate(
            zip(value.request.days, value.days, strict=True)
        ):
            instruments = {item.ts_code: item for item in day.instruments}
            calendar_index = value.request.calendar.dates.index(day.trade_date)
            next_trade_date = value.request.calendar.dates[calendar_index + 1]
            decisions: list[BacktestDecision] = []
            orders: list[BacktestOrder] = []
            skipped: list[SkippedTarget] = []
            exited: set[str] = set()
            at = _at(day.trade_date, 9, 30)
            for entry in entries:
                instrument = instruments.get(entry.ts_code)
                if (
                    not entry.remaining
                    or instrument is None
                    or instrument.decision_price is None
                    or instrument.conditions is None
                ):
                    continue
                probe_id = canonical_sha256(
                    {"input": value.input_hash, "entry": entry.signal_id, "cutoff": at}
                )
                authority = broker.sell_quantity_authority(
                    exit_signal_id=probe_id,
                    entry_signal_id=entry.signal_id,
                    ts_code=entry.ts_code,
                    action="S_INTENT",
                    tranche_fraction=Decimal("1"),
                    decision_cutoff=at,
                    trade_date=day.trade_date,
                )
                entry.high = max(entry.high, instrument.decision_price)
                position = TemplatePosition(
                    ts_code=entry.ts_code,
                    entry_price=entry.price,
                    eligible_high=entry.high,
                    holding_days=calendar_index
                    - value.request.calendar.dates.index(entry.acquired_on),
                    sellable_quantity=authority.available_quantity,
                )
                quote = TemplatePrice(
                    ts_code=entry.ts_code,
                    price=instrument.decision_price,
                    observed_at=instrument.conditions.observed_at,
                    event_time=instrument.decision_price_observed_at,
                    source_hash=instrument.decision_price_source_identity,
                    suspended=instrument.conditions.suspended,
                    sell_limit_locked=instrument.conditions.sell_limit_locked,
                    basis="daily_reference",
                )
                reason = strategy_template_exit(value.rules, position, quote, decision_time=at)
                if reason is None:
                    continue
                decision = _decision(
                    value,
                    instrument,
                    quantity=authority.available_quantity,
                    side=PaperSide.SELL,
                    at=at,
                    entry_id=entry.signal_id,
                )
                decisions.append(decision)
                skip = _can_submit(entry.ts_code, PaperSide.SELL, instruments)
                if skip is not None:
                    skipped.append(skip)
                    continue
                order = _order(
                    broker,
                    value.request,
                    day,
                    instrument,
                    decision=decision,
                    next_trade_date=next_trade_date,
                    entry_remaining=entry.remaining,
                )
                orders.append(order)
                exit_log.append(
                    TemplateExitDecision(
                        trade_date=day.trade_date,
                        ts_code=entry.ts_code,
                        entry_signal_id=entry.signal_id,
                        decision_id=decision.decision_id,
                        reason=reason,
                        decided_at=at,
                    )
                )
                exited.add(entry.ts_code)
                if order.receipt.fill is not None:
                    entry.remaining -= order.receipt.fill.quantity
            rebalanced = _should_rebalance(
                day_index, value.request.days, value.rules.rebalance_rule
            )
            if risk is not None and risk.max_total_risk_weight is not None:
                rebalanced = True
            if rebalanced:
                eligible = (
                    set(
                        strategy_template_entry(
                            value.rules, evidence.entry, decision_time=_at(day.trade_date, 9, 25)
                        )
                    )
                    - exited
                )
                index_allows = template_index_allows_entry(value.rules, evidence, value.request)
                held = _quantities(entries)
                candidates = tuple(
                    item
                    for item in day.ranking.candidates
                    if item.ts_code in eligible and (index_allows or item.ts_code in held)
                )
                targets: dict[str, int] = {}
                if candidates:
                    weight_rule = value.rules.weight_rule
                    if risk is not None and risk.max_total_risk_weight is not None:
                        weight_rule = weight_rule.model_copy(
                            update={
                                "cash_reserve": max(
                                    weight_rule.cash_reserve,
                                    Decimal("1") - risk.max_total_risk_weight,
                                )
                            }
                        )
                    allocation = allocate_target_weights(
                        candidates, weight_rule, capital=previous_nav
                    )
                    for target in allocation.positions:
                        instrument = instruments.get(target.ts_code)
                        if (
                            target.status == "selected"
                            and instrument is not None
                            and instrument.decision_price is not None
                        ):
                            targets[target.ts_code] = (
                                int(target.target_amount / instrument.decision_price) // 100 * 100
                            )
                for entry in entries:
                    excess = _quantities(entries).get(entry.ts_code, 0) - targets.get(
                        entry.ts_code, 0
                    )
                    instrument = instruments.get(entry.ts_code)
                    if (
                        not entry.remaining
                        or excess <= 0
                        or entry.ts_code in exited
                        or instrument is None
                        or instrument.decision_price is None
                    ):
                        continue
                    quantity = min(excess, entry.remaining)
                    decision = _decision(
                        value,
                        instrument,
                        quantity=quantity,
                        side=PaperSide.SELL,
                        at=_at(day.trade_date, 9, 25),
                        entry_id=entry.signal_id,
                    )
                    decisions.append(decision)
                    skip = _can_submit(entry.ts_code, PaperSide.SELL, instruments)
                    if skip is not None:
                        skipped.append(skip)
                        continue
                    order = _order(
                        broker,
                        value.request,
                        day,
                        instrument,
                        decision=decision,
                        next_trade_date=next_trade_date,
                        entry_remaining=entry.remaining,
                    )
                    orders.append(order)
                    if order.receipt.fill is not None:
                        entry.remaining -= order.receipt.fill.quantity
                for code, target in sorted(targets.items()):
                    deficit = target - _quantities(entries).get(code, 0)
                    if deficit <= 0 or code in exited or not index_allows:
                        continue
                    if risk is not None and not risk.allow_new_positions and held.get(code, 0) == 0:
                        skipped.append(
                            SkippedTarget(ts_code=code, side="BUY", reason="drawdown_blocked")
                        )
                        continue
                    instrument = instruments[code]
                    decision = _decision(
                        value,
                        instrument,
                        quantity=deficit,
                        side=PaperSide.BUY,
                        at=_at(day.trade_date, 9, 25),
                    )
                    decisions.append(decision)
                    skip = _can_submit(code, PaperSide.BUY, instruments)
                    if skip is not None:
                        skipped.append(skip)
                        continue
                    order = _order(
                        broker,
                        value.request,
                        day,
                        instrument,
                        decision=decision,
                        next_trade_date=next_trade_date,
                    )
                    orders.append(order)
                    if order.receipt.fill is not None:
                        entries.append(
                            _Entry(
                                code,
                                decision.decision_id,
                                order.receipt.fill.quantity,
                                day.trade_date,
                                order.receipt.fill.price,
                                order.receipt.fill.price,
                            )
                        )
            for minute in sorted(
                evidence.minutes, key=lambda item: (item.quote.event_time, item.quote.ts_code)
            ):
                for entry in entries:
                    if not entry.remaining or entry.ts_code != minute.quote.ts_code:
                        continue
                    authority = broker.sell_quantity_authority(
                        exit_signal_id=canonical_sha256(
                            {
                                "input": value.input_hash,
                                "entry": entry.signal_id,
                                "cutoff": minute.quote.observed_at,
                            }
                        ),
                        entry_signal_id=entry.signal_id,
                        ts_code=entry.ts_code,
                        action="S_INTENT",
                        tranche_fraction=Decimal("1"),
                        decision_cutoff=minute.quote.observed_at,
                        trade_date=day.trade_date,
                    )
                    entry.high = max(entry.high, minute.quote.price)
                    position = TemplatePosition(
                        ts_code=entry.ts_code,
                        entry_price=entry.price,
                        eligible_high=entry.high,
                        holding_days=calendar_index
                        - value.request.calendar.dates.index(entry.acquired_on),
                        sellable_quantity=authority.available_quantity,
                    )
                    reason = strategy_template_exit(
                        value.rules, position, minute.quote, decision_time=minute.quote.event_time
                    )
                    if reason is None:
                        continue
                    decision = _decision(
                        value,
                        instruments[entry.ts_code],
                        quantity=authority.available_quantity,
                        side=PaperSide.SELL,
                        at=minute.quote.event_time,
                        entry_id=entry.signal_id,
                        price_hash=minute.quote.source_hash,
                    )
                    decisions.append(decision)
                    order = _minute_order(
                        broker,
                        value,
                        instruments[entry.ts_code],
                        minute,
                        decision,
                        trade_date=day.trade_date,
                    )
                    orders.append(order)
                    exit_log.append(
                        TemplateExitDecision(
                            trade_date=day.trade_date,
                            ts_code=entry.ts_code,
                            entry_signal_id=entry.signal_id,
                            decision_id=decision.decision_id,
                            reason=reason,
                            decided_at=minute.quote.event_time,
                        )
                    )
                    if order.receipt.fill is not None:
                        entry.remaining -= order.receipt.fill.quantity
            held = _quantities(entries)
            fees = sum(
                (
                    order.receipt.fill.total_fees
                    for order in orders
                    if order.receipt.fill is not None
                ),
                Decimal("0"),
            )
            if any(
                code not in instruments or instruments[code].close_price is None for code in held
            ):
                results.append(
                    BacktestDayResult(
                        trade_date=day.trade_date,
                        rebalanced=rebalanced,
                        decisions=tuple(decisions),
                        orders=tuple(orders),
                        skipped=tuple(skipped),
                        fees=fees,
                        account=None,
                        risk=risk,
                        incomplete_reason="missing_held_close",
                    )
                )
                break
            account = broker.account_snapshot(
                as_of=_at(day.trade_date, 15, 1),
                market_prices={code: instruments[code].close_price for code in held},
            )
            cash = previous_cash
            for order in orders:
                fill = order.receipt.fill
                if fill is not None:
                    cash -= (
                        fill.notional if order.intent.side is PaperSide.BUY else -fill.notional
                    ) + fill.total_fees
            if (
                account.cash != cash
                or {item.code: item.quantity for item in account.holdings} != held
            ):
                raise RuntimeError("template account differs from original broker receipts")
            results.append(
                BacktestDayResult(
                    trade_date=day.trade_date,
                    rebalanced=rebalanced,
                    decisions=tuple(decisions),
                    orders=tuple(orders),
                    skipped=tuple(skipped),
                    fees=fees,
                    account=account,
                    market_value=account.nav - account.cash,
                    daily_return=account.nav / previous_nav - Decimal("1"),
                    normalized_nav=account.nav / value.request.initial_cash,
                    risk=risk,
                )
            )
            previous_nav, previous_cash = account.nav, account.cash
            if value.request.drawdown_rule is not None:
                risk = evaluate_drawdown(
                    account.nav, _at(day.trade_date, 15, 1), value.request.drawdown_rule, risk_state
                )
                risk_state = risk.state
    return StrategyTemplateResult(
        owner_id=value.owner_id,
        strategy_id=value.definition.logical_id,
        version=value.definition.version,
        definition_fingerprint=value.definition.fingerprint,
        definition_record_hash=value.definition.record_hash,
        input_hash=value.input_hash,
        calendar_source_identity=value.request.calendar.source_identity,
        cost_spec_id=value.request.execution_cost_spec.cost_spec_id,
        status="complete" if results[-1].account is not None else "incomplete",
        days=tuple(results),
        exit_decisions=tuple(exit_log),
    )
