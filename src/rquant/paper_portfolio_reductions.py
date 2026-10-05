"""Persistent risk signals routed through the original bus, spool and quantities."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, localcontext
from fractions import Fraction
from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, model_validator

from rquant.order_execution_costs import calculate_execution_costs
from rquant.paper_broker import NoExecutableSellQuantityError, PaperAccountAuthoritySnapshot, PaperBrokerStore
from rquant.paper_contracts import PaperSide
from rquant.paper_portfolio_models import PaperRiskObservation, Sha256
from rquant.paper_portfolio_ledger import PaperPortfolioLedgerFrame
from rquant.paper_portfolio_source import PaperPortfolioMarketSnapshot
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.portfolio.weights import PortfolioCandidate, PortfolioTarget, allocate_target_weights
from rquant.research_run_spec import ExecutionCostOrderInput
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_route_spool import SignalRouteSpool, publish_signal_bus_prefix
from rquant.signal_router_runtime import RouteSourceDescriptor, RoutingDecision, RunnerSignalBatch, SignalRouteCursorStore, SourceSnapshot, route_runner_signals
from rquant.strategy_runner import RunnerSignalRecord

if TYPE_CHECKING:
    from rquant.paper_signal_worker import PaperSignalQueueStore, QuoteResolver
    from rquant.paper_portfolio_runtime import PaperPortfolioRuntime


class PaperRiskReductionPlan(RuntimeContractModel):
    contract: Literal["paper-risk-reduction-plan/v1"] = "paper-risk-reduction-plan/v1"
    configuration_fingerprint: Sha256
    decision_at: AwareUtcDatetime
    trade_date: date
    risk: PaperRiskObservation
    account: PaperAccountAuthoritySnapshot
    material: PaperPortfolioMarketSnapshot
    worst_exit_fees: Decimal = Field(ge=0, allow_inf_nan=False)
    target: PortfolioTarget
    signals: tuple[SignalEnvelope, ...] = Field(max_length=5000)

    @model_validator(mode="after")
    def same_sources(self) -> Self:
        if (self.risk.configuration_fingerprint != self.configuration_fingerprint or self.material.configuration_fingerprint != self.configuration_fingerprint
                or self.material.available_at > self.decision_at or self.account.snapshot.as_of_time != self.decision_at
                or self.account.snapshot.account_id != self.material.binding.account_id
                or self.risk.account_id != self.account.snapshot.account_id or self.risk.observed_at != self.decision_at
                or self.risk.nav != self.account.snapshot.nav or self.risk.ledger_revision != self.account.revision
                or self.risk.source_fingerprint != self.account.state_fingerprint
                or any(signal.action not in (SignalAction.REDUCE, SignalAction.S_INTENT) or signal.event_time != self.decision_at
                       or signal.strategy_id != self.material.binding.strategy_id or signal.strategy_version != self.material.binding.strategy_version
                       or signal.parameter_fingerprint != self.material.binding.parameter_fingerprint for signal in self.signals)):
            raise ValueError("paper risk reduction plan is detached from its actual configured sources")
        if len(self.model_dump_json().encode()) > 5*1024*1024:
            raise ValueError("paper risk signal plan exceeds its fixed byte budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperReductionStatus(RuntimeContractModel):
    configuration_fingerprint: Sha256
    ledger_frame_fingerprint: Sha256
    plan_fingerprint: Sha256 | None = None
    as_of: AwareUtcDatetime
    status: Literal["not_required", "waiting", "complete", "incomplete"]
    risk_weight: Decimal = Field(ge=0, le=1, allow_inf_nan=False)
    target_risk_weight: Decimal | None = Field(default=None, ge=0, le=1, allow_inf_nan=False)
    signal_ids: tuple[Sha256, ...] = Field(max_length=5000)
    reason: str | None = Field(default=None, max_length=512)


class PaperRiskSignalPublisher:
    def __init__(self, state: PaperPortfolioStateStore, *, bus: SignalBusStore, spool: SignalRouteSpool,
                 cursors: SignalRouteCursorStore) -> None:
        if type(bus) is not SignalBusStore or type(spool) is not SignalRouteSpool or type(cursors) is not SignalRouteCursorStore:
            raise TypeError("paper risk requires the original concrete bus, route cursor and spool")
        self.state, self.bus, self.spool, self.cursors = state, bus, spool, cursors
        self.source_id = "paper-risk:"+state.instance_id
        self.generation_id = canonical_sha256({"contract": "paper-risk-source/v1", "identity": state.identity(), "binding": state.configuration.binding})
        with state._connection(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS portfolio_risk_plans(configuration TEXT NOT NULL,decision_at TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(configuration,decision_at))")
            connection.execute("CREATE TABLE IF NOT EXISTS portfolio_risk_signals(sequence INTEGER PRIMARY KEY AUTOINCREMENT,signal_id TEXT UNIQUE NOT NULL,body TEXT NOT NULL)")

    def plan_at(self, cutoff: datetime) -> PaperRiskReductionPlan | None:
        with self.state._connection() as connection:
            row = connection.execute("SELECT body FROM portfolio_risk_plans WHERE configuration=? AND decision_at=?",
                                     (self.state.configuration.fingerprint, normalize_aware_utc(cutoff).isoformat())).fetchone()
        return PaperRiskReductionPlan.model_validate_json(row[0]) if row else None

    def latest_plan(self, cutoff: datetime, *, any_configuration: bool = False) -> PaperRiskReductionPlan | None:
        with self.state._connection() as connection:
            if any_configuration:
                row = connection.execute("SELECT body FROM portfolio_risk_plans WHERE decision_at<=? ORDER BY decision_at DESC LIMIT 1",
                                         (normalize_aware_utc(cutoff).isoformat(),)).fetchone()
            else:
                row = connection.execute("SELECT body FROM portfolio_risk_plans WHERE configuration=? AND decision_at<=? ORDER BY decision_at DESC LIMIT 1",
                                         (self.state.configuration.fingerprint, normalize_aware_utc(cutoff).isoformat())).fetchone()
        return PaperRiskReductionPlan.model_validate_json(row[0]) if row else None

    def persist(self, value: PaperRiskReductionPlan) -> PaperRiskReductionPlan:
        value = PaperRiskReductionPlan.model_validate(value.model_dump(mode="python"))
        with self.state._connection(write=True) as connection:
            old = self.plan_at(value.decision_at)
            if old is not None:
                if old != value:
                    raise ValueError("original risk signal plan differs")
                return old
            if value.configuration_fingerprint != self.state.configuration.fingerprint:
                raise ValueError("risk signal configuration changed")
            if connection.execute("SELECT count(*) FROM portfolio_risk_signals").fetchone()[0]+len(value.signals) > 50000:
                raise ValueError("paper risk source exceeds its fixed 50000-signal budget")
            connection.execute("INSERT INTO portfolio_risk_plans VALUES(?,?,?)", (value.configuration_fingerprint, value.decision_at.isoformat(), value.model_dump_json()))
            for signal in value.signals:
                connection.execute("INSERT INTO portfolio_risk_signals(signal_id,body) VALUES(?,?)", (signal.signal_id, signal.model_dump_json()))
        return value

    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
        if type(after_sequence) is not int or after_sequence < 0 or type(limit) is not int or not 1 <= limit <= 5000:
            raise ValueError("paper risk route read exceeds its budget")
        with self.state._connection() as connection:
            high = int(connection.execute("SELECT coalesce(max(sequence),0) FROM portfolio_risk_signals").fetchone()[0])
            rows = connection.execute("SELECT sequence,body FROM portfolio_risk_signals WHERE sequence>? ORDER BY sequence LIMIT ?", (after_sequence, limit)).fetchall()
        descriptor = RouteSourceDescriptor(source_id=self.source_id, generation_id=self.generation_id,
                                           strategy_spec_fingerprint=canonical_sha256(self.state.configuration.binding), first_sequence=1, high_watermark=high)
        return RunnerSignalBatch(snapshot=SourceSnapshot(descriptor=descriptor), after_sequence=after_sequence, limit=limit,
                                 records=tuple(RunnerSignalRecord(sequence=row[0], signal=SignalEnvelope.model_validate_json(row[1])) for row in rows))

    def publish_pending(self, *, observed_at: datetime) -> None:
        route_runner_signals(source_id=self.source_id, source=self, bus=self.bus, cursors=self.cursors, routed_at=observed_at,
                             target_resolver=lambda signal: RoutingDecision.no_target(routing_policy_fingerprint=self.cursors.routing_policy_fingerprint,
                                                                                     reason_code="paper_risk_only"), limit=5000)
        publish_signal_bus_prefix(bus=self.bus, spool=self.spool, limit=5000)


def plan_paper_reductions(runtime: PaperPortfolioRuntime, broker: PaperBrokerStore, *, decision_at: datetime,
                          trade_date: date, quote_resolver: QuoteResolver,
                          queue: PaperSignalQueueStore | None = None) -> PaperRiskReductionPlan | None:
    cutoff = normalize_aware_utc(decision_at)
    configuration = runtime.state.refresh_configuration()
    if configuration.drawdown_rule is None:
        return None
    publisher = runtime.risk_publisher
    if publisher is not None:
        old = publisher.plan_at(cutoff)
        if old is not None:
            publisher.publish_pending(observed_at=cutoff)
            return old
    facts = runtime.materials.latest(decision_at=cutoff)
    if facts.observed_at < cutoff-timedelta(seconds=90):
        raise ValueError("回撤观测缺少当时可用的估值")
    prices = {item.ts_code: item.valuation_price for item in facts.facts if item.valuation_price is not None}
    runtime.require_broker(broker)
    account = runtime.account_authority(broker, cutoff=cutoff, prices=prices)
    risk = runtime.state.observe_nav(account.snapshot.nav, observed_at=cutoff, ledger_revision=account.revision,
                                     source_fingerprint=account.state_fingerprint)
    decision = risk.decision
    if decision is None or decision.max_total_risk_weight is None:
        return None
    cap = decision.max_total_risk_weight
    holdings = account.snapshot.holdings
    invested = sum((Fraction(item.quantity)*Fraction(item.market_price) for item in holdings), Fraction(0))
    if invested <= Fraction(account.snapshot.nav)*Fraction(cap):
        return None
    if publisher is None:
        raise ValueError("降仓未配置原信号路由")
    if queue is not None:
        previous_plan = publisher.latest_plan(cutoff, any_configuration=True)
        if previous_plan is not None:
            records = tuple(queue.record(str(signal.signal_id)) for signal in previous_plan.signals)
            awaiting = any((record is None and signal.expires_at >= cutoff) or (record is not None and record.status.value in ("pending", "prepared"))
                           for record, signal in zip(records, previous_plan.signals, strict=True))
            t1_today = any(record is not None and record.order is not None and record.order.reject_reason is not None
                           and record.order.reject_reason.value == "T_PLUS_ONE"
                           and previous_plan.trade_date == trade_date for record in records)
            if awaiting or t1_today:
                publisher.publish_pending(observed_at=cutoff)
                return previous_plan
    frame = runtime.ledger_source_for(broker).read(configuration=configuration, as_of=cutoff, prices=prices)
    plan_key = canonical_sha256({"configuration": configuration.fingerprint, "risk": risk.fingerprint})
    entries = []
    worst_fees = Decimal(0)
    for row in frame.history:
        if row.intent.side is not PaperSide.BUY or not row.order.filled_quantity:
            continue
        probe = canonical_sha256({"risk_plan": plan_key, "entry": row.intent.signal_id})
        try:
            authority = broker.sell_quantity_authority(exit_signal_id=probe, entry_signal_id=row.intent.signal_id, ts_code=row.intent.ts_code,
                                                       action="S_INTENT", tranche_fraction=Decimal(1), decision_cutoff=cutoff, trade_date=trade_date)
        except NoExecutableSellQuantityError:
            continue
        signal = SignalEnvelope(schema_version=1, strategy_id=configuration.binding.strategy_id, strategy_version=configuration.binding.strategy_version,
                                parameter_fingerprint=configuration.binding.parameter_fingerprint, dataset_snapshot_id=facts.dataset_snapshot_id,
                                feature_snapshot_id=facts.feature_snapshot_id, event_time=cutoff, available_at=cutoff, expires_at=cutoff+timedelta(minutes=5),
                                candidate_id=row.intent.ts_code, action=SignalAction.S_INTENT, producer_commit=runtime.producer_commit,
                                reason_codes=("paper_drawdown_reduction",), evidence={"entry_signal_id": row.intent.signal_id, "sell_tranche_fraction": "1",
                                                                                   "paper_risk_plan": plan_key, "configuration_fingerprint": configuration.fingerprint})
        quote = quote_resolver(signal, cutoff)
        if (quote.available_at > cutoff or quote.event_time > cutoff or quote.context.executable_price != prices.get(row.intent.ts_code)):
            raise ValueError("降仓报价与原估值不同步")
        costs = calculate_execution_costs(configuration.execution_cost_spec, ExecutionCostOrderInput(side="SELL", reference_price=quote.context.executable_price,
                                                                                                   quantity=authority.remaining_quantity), quote.context.instrument_context)
        worst_fees += costs.total_fees
        entries.append((row, authority, signal))
    capital = account.snapshot.nav-worst_fees
    if capital <= 0:
        raise ValueError("退出费用超过可核实净值，降仓未完成")
    rule = configuration.weight_rule.model_copy(update={"cash_reserve": max(configuration.weight_rule.cash_reserve, Decimal(1)-cap)})
    target = allocate_target_weights(tuple(PortfolioCandidate(ts_code=item.ts_code, industry_l1=item.industry_l1, rank_score=item.rank_score)
                                           for item in facts.facts if item.candidate), rule, capital=capital)
    amounts = {item.ts_code: item.target_amount for item in target.positions if item.status == "selected"}
    kept = {item.code: max(0, int(Fraction(amounts.get(item.code, Decimal(0)))/Fraction(item.market_price))//100*100) for item in holdings}
    needed = {item.code: max(0, item.quantity-kept[item.code]) for item in holdings}
    signals = []
    for row, authority, full in entries:
        quantity = min(authority.remaining_quantity, needed.get(row.intent.ts_code, 0))
        if not quantity:
            continue
        needed[row.intent.ts_code] -= quantity
        if quantity == authority.remaining_quantity:
            signals.append(full)
        else:
            with localcontext() as context:
                context.prec = 34
                fraction = (Decimal(quantity)/Decimal(authority.remaining_quantity)).quantize(Decimal("1e-24"), rounding=ROUND_CEILING)
            body = full.model_dump(mode="python", exclude={"signal_id"})
            body["action"] = SignalAction.REDUCE
            body["evidence"]["sell_tranche_fraction"] = str(fraction)
            signals.append(SignalEnvelope.model_validate(body))
    if any(needed.values()):
        raise ValueError("持仓缺少原入场来源，降仓未完成")
    value = publisher.persist(PaperRiskReductionPlan(configuration_fingerprint=configuration.fingerprint, decision_at=cutoff, trade_date=trade_date, risk=risk,
                                                     account=account, material=facts, worst_exit_fees=worst_fees, target=target, signals=tuple(signals)))
    publisher.publish_pending(observed_at=cutoff)
    return value


def paper_reduction_status(runtime: PaperPortfolioRuntime, broker: PaperBrokerStore, queue: PaperSignalQueueStore, *,
                           as_of: datetime, prices: dict[str, Decimal], frame: PaperPortfolioLedgerFrame | None = None) -> PaperReductionStatus:
    cutoff = normalize_aware_utc(as_of)
    configuration = runtime.state.configuration
    if frame is None:
        frame = runtime.ledger_source_for(broker).read(configuration=configuration, as_of=cutoff, prices=prices)
    if (frame.configuration_fingerprint != configuration.fingerprint or frame.account_id != configuration.binding.account_id
            or frame.as_of != cutoff or frame.account is None):
        raise ValueError("paper reduction status lacks its exact valued original ledger frame")
    risk = runtime.state.last_observation()
    cap = risk.decision.max_total_risk_weight if risk and risk.decision else None
    exact = sum((Fraction(item.quantity)*Fraction(item.market_price) for item in frame.account.holdings), Fraction(0))/Fraction(frame.account.nav)
    with localcontext() as context:
        context.prec = 34
        displayed = (Decimal(exact.numerator)/Decimal(exact.denominator)).quantize(Decimal("1e-18"), rounding=ROUND_DOWN)
    publisher = runtime.risk_publisher
    plan = publisher.latest_plan(cutoff) if publisher else None
    status, reason = "not_required", None
    if cap is not None:
        if exact <= Fraction(cap):
            status = "complete"
        else:
            status, reason = "incomplete", "降仓未完成"
            if plan:
                records = tuple(queue.record(str(signal.signal_id)) for signal in plan.signals)
                if any(record is None or record.status.value in ("pending", "prepared") for record in records):
                    status, reason = "waiting", "降仓指令等待处理"
                else:
                    reasons = tuple(str(record.order.reject_reason.value) for record in records if record and record.order and record.order.reject_reason)
                    if reasons:
                        reason = "降仓未完成："+", ".join(sorted(set(reasons)))
    return PaperReductionStatus(configuration_fingerprint=configuration.fingerprint, ledger_frame_fingerprint=frame.fingerprint,
                                plan_fingerprint=plan.fingerprint if plan else None, as_of=cutoff,
                                status=status, risk_weight=displayed, target_risk_weight=cap,
                                signal_ids=tuple(str(signal.signal_id) for signal in plan.signals) if plan else (), reason=reason)
