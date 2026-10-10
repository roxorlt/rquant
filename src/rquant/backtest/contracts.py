"""Immutable inputs and reconciled outputs for an offline daily portfolio replay."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import Field, SerializerFunctionWrapHandler, model_serializer, model_validator

from rquant.paper_contracts import PaperAccountSnapshot, PaperExecutionReceipt, PaperOrderIntent
from rquant.portfolio.drawdown import DrawdownDecision, DrawdownRule
from rquant.portfolio.weights import PortfolioCandidate, PortfolioWeightRule
from rquant.research_run_spec import ExecutionCostSpec, InstrumentContext
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

_SHANGHAI = ZoneInfo("Asia/Shanghai")
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
PositiveMoney = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
PositivePrice = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
NonNegativeMoney = Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]


def _local_at(trade_date: date, hour: int, minute: int) -> datetime:
    return datetime.combine(trade_date, time(hour, minute), tzinfo=_SHANGHAI)


class SSECalendar(RuntimeContractModel):
    exchange: Literal["SSE"] = "SSE"
    source_identity: Sha256
    coverage_start: date
    coverage_end: date
    dates: tuple[date, ...] = Field(min_length=3)

    @model_validator(mode="after")
    def validate_coverage(self) -> Self:
        if tuple(sorted(set(self.dates))) != self.dates:
            raise ValueError("SSE calendar dates must be ordered and unique")
        if self.coverage_start != self.dates[0] or self.coverage_end != self.dates[-1]:
            raise ValueError("SSE calendar coverage must bind its first and last date")
        return self


class RebalanceRule(RuntimeContractModel):
    kind: Literal["daily", "weekly", "monthly", "every_n"]
    every_n_days: int | None = Field(default=None, strict=True, gt=0)

    @model_validator(mode="after")
    def validate_period(self) -> Self:
        if (self.kind == "every_n") != (self.every_n_days is not None):
            raise ValueError("every_n_days is required only for every_n rebalance")
        return self


class RankingSnapshot(RuntimeContractModel):
    source_identity: Sha256
    source_trade_date: date
    observed_at: AwareUtcDatetime
    candidates: tuple[PortfolioCandidate, ...]

    @model_validator(mode="after")
    def validate_candidates(self) -> Self:
        codes = [item.ts_code for item in self.candidates]
        if len(codes) != len(set(codes)):
            raise ValueError("ranking candidates contain duplicate symbols")
        return self


class TradeConditions(RuntimeContractModel):
    source_identity: Sha256
    observed_at: AwareUtcDatetime
    suspended: bool
    buy_limit_locked: bool
    sell_limit_locked: bool


class BacktestInstrument(RuntimeContractModel):
    ts_code: str = Field(min_length=1)
    instrument_context: InstrumentContext
    classification_observed_at: AwareUtcDatetime
    decision_price: PositivePrice | None
    decision_price_observed_at: AwareUtcDatetime | None
    decision_price_source_identity: Sha256
    price_source_identity: Sha256
    open_price: PositivePrice | None
    open_observed_at: AwareUtcDatetime | None
    close_price: PositivePrice | None
    close_observed_at: AwareUtcDatetime | None
    conditions: TradeConditions | None

    @model_validator(mode="after")
    def validate_evidence(self) -> Self:
        if (self.decision_price is None) != (self.decision_price_observed_at is None):
            raise ValueError("decision price and observation time must appear together")
        if (self.open_price is None) != (self.open_observed_at is None):
            raise ValueError("open price and observation time must appear together")
        if (self.close_price is None) != (self.close_observed_at is None):
            raise ValueError("close price and observation time must appear together")
        if self.instrument_context.ts_code != self.ts_code:
            raise ValueError("instrument classification code does not match quote")
        if self.instrument_context.classification_provenance is None:
            raise ValueError("instrument classification requires source provenance")
        if (
            self.instrument_context.market != "CN"
            or self.instrument_context.security_class != "A_SHARE"
            or self.instrument_context.instrument_class != "EQUITY"
        ):
            raise ValueError("daily backtest requires an authoritative CN A-share classification")
        return self


class BacktestDayInput(RuntimeContractModel):
    trade_date: date
    ranking: RankingSnapshot
    instruments: tuple[BacktestInstrument, ...]

    @model_validator(mode="after")
    def validate_instruments(self) -> Self:
        codes = [item.ts_code for item in self.instruments]
        if len(codes) != len(set(codes)):
            raise ValueError("daily instruments contain duplicate symbols")
        return self


class BacktestRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    execution_convention: Literal["SIMULATED_DAILY_OPEN"] = "SIMULATED_DAILY_OPEN"
    producer_commit: CommitSha
    input_generation_id: Sha256
    calendar: SSECalendar
    days: tuple[BacktestDayInput, ...] = Field(min_length=1)
    initial_cash: PositiveMoney
    weight_rule: PortfolioWeightRule
    rebalance_rule: RebalanceRule
    execution_cost_spec: ExecutionCostSpec
    drawdown_rule: DrawdownRule | None = None

    @model_serializer(mode="wrap")
    def serialize_optional_risk(self, handler: SerializerFunctionWrapHandler) -> object:
        payload = handler(self)
        if self.drawdown_rule is None:
            payload.pop("drawdown_rule", None)
        return payload

    @model_validator(mode="after")
    def validate_timeline(self) -> Self:
        if self.initial_cash * 100 != (self.initial_cash * 100).to_integral_value():
            raise ValueError("initial cash must be exact to a cent")
        if not self.execution_cost_spec.is_alignment_eligible:
            raise ValueError("backtest requires the shared v3 execution cost spec")
        day_dates = tuple(item.trade_date for item in self.days)
        if tuple(sorted(set(day_dates))) != day_dates:
            raise ValueError("backtest trading days must be ordered and unique")
        try:
            first = self.calendar.dates.index(day_dates[0])
            last = self.calendar.dates.index(day_dates[-1])
        except ValueError as exc:
            raise ValueError("backtest day is absent from SSE calendar") from exc
        if first == 0 or last == len(self.calendar.dates) - 1:
            raise ValueError("SSE calendar needs preceding and following trading dates")
        if self.calendar.dates[first : last + 1] != day_dates:
            raise ValueError("backtest days must cover every SSE calendar trading date")
        for index, day in enumerate(self.days, start=first):
            previous_date = self.calendar.dates[index - 1]
            cutoff = _local_at(day.trade_date, 9, 25)
            execution = _local_at(day.trade_date, 9, 30)
            valuation = _local_at(day.trade_date, 15, 1)
            if day.ranking.source_trade_date != previous_date:
                raise ValueError("ranking source date must be previous SSE trading day")
            if not (_local_at(previous_date, 15, 0) <= day.ranking.observed_at < cutoff):
                raise ValueError("ranking observation must precede the 09:25 cutoff")
            for quote in day.instruments:
                # The reference publisher commits before 09:25 and makes classification
                # records visible exactly at the decision instant.
                if quote.classification_observed_at > cutoff:
                    raise ValueError("instrument classification is not visible at decision cutoff")
                if quote.decision_price_observed_at is not None and not (
                    _local_at(previous_date, 15, 0) <= quote.decision_price_observed_at < cutoff
                ):
                    raise ValueError("decision price must be visible before the 09:25 cutoff")
                if quote.open_observed_at is not None and quote.open_observed_at != execution:
                    raise ValueError("open price evidence must match the opening execution")
                if quote.close_observed_at is not None and not (
                    _local_at(day.trade_date, 15, 0) <= quote.close_observed_at <= valuation
                ):
                    raise ValueError("close price must be observed by valuation time")
                if quote.conditions is not None and quote.conditions.observed_at != execution:
                    raise ValueError("trade condition evidence must cover the open")
        return self

    @property
    def request_id(self) -> str:
        return canonical_sha256(self)


class SkippedTarget(RuntimeContractModel):
    ts_code: str = Field(min_length=1)
    side: Literal["BUY", "SELL"]
    reason: Literal[
        "unverified_conditions",
        "missing_decision_price",
        "missing_open_price",
        "below_lot",
        "drawdown_blocked",
    ]


class BacktestDecision(RuntimeContractModel):
    """A 09:25 target order fixed without using the current day's opening print."""

    decision_id: Sha256 | None = None
    decided_at: AwareUtcDatetime
    producer_commit: CommitSha
    strategy_config_id: Sha256
    ranking_source_identity: Sha256
    reference_price_snapshot_id: Sha256
    ts_code: str = Field(min_length=1)
    side: Literal["BUY", "SELL"]
    quantity: int = Field(gt=0, multiple_of=100)
    entry_signal_id: Sha256 | None = None

    @model_validator(mode="after")
    def validate_decision(self) -> Self:
        if (self.side == "SELL") != (self.entry_signal_id is not None):
            raise ValueError("SELL decision requires a BUY entry signal")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"decision_id"}))
        if self.decision_id is None:
            object.__setattr__(self, "decision_id", expected)
        elif self.decision_id != expected:
            raise ValueError("pre-open decision id does not match its content")
        return self


class BacktestOrder(RuntimeContractModel):
    decision: BacktestDecision
    intent: PaperOrderIntent
    receipt: PaperExecutionReceipt

    @model_validator(mode="after")
    def validate_order_binding(self) -> Self:
        if (
            self.intent.signal_id != self.decision.decision_id
            or self.intent.ts_code != self.decision.ts_code
            or self.intent.side.value != self.decision.side
            or self.intent.quantity != self.decision.quantity
            or self.intent.entry_signal_id != self.decision.entry_signal_id
            or self.receipt.intent_id != self.intent.intent_id
        ):
            raise ValueError("broker order does not bind the pre-open decision")
        return self


class BacktestDayResult(RuntimeContractModel):
    trade_date: date
    rebalanced: bool
    decisions: tuple[BacktestDecision, ...]
    orders: tuple[BacktestOrder, ...]
    skipped: tuple[SkippedTarget, ...]
    fees: NonNegativeMoney
    account: PaperAccountSnapshot | None
    market_value: NonNegativeMoney | None = None
    daily_return: Decimal | None = Field(default=None, allow_inf_nan=False)
    normalized_nav: Decimal | None = Field(default=None, allow_inf_nan=False)
    incomplete_reason: Literal["missing_held_close"] | None = None
    risk: DrawdownDecision | None = None

    @model_serializer(mode="wrap")
    def preserve_unconfigured_result(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, object]:
        value = handler(self)
        if self.risk is None:
            value.pop("risk", None)
        return value

    @model_validator(mode="after")
    def validate_account(self) -> Self:
        if (
            self.risk is not None
            and self.risk.state.last_at.astimezone(_SHANGHAI).date() >= self.trade_date
        ):
            raise ValueError("risk must use a prior-day NAV observation")
        decision_ids = {item.decision_id for item in self.decisions}
        if len(decision_ids) != len(self.decisions) or any(
            item.decision.decision_id not in decision_ids for item in self.orders
        ):
            raise ValueError("executed orders must bind distinct daily decisions")
        if (self.account is None) != (self.incomplete_reason is not None):
            raise ValueError("incomplete day must have no account valuation")
        if self.account is None and self.market_value is not None:
            raise ValueError("incomplete day cannot report a market value")
        if self.account is None and (
            self.daily_return is not None or self.normalized_nav is not None
        ):
            raise ValueError("incomplete day cannot report a return or normalized NAV")
        if self.account is not None:
            expected_value = sum(
                (item.quantity * item.market_price for item in self.account.holdings),
                Decimal("0"),
            )
            if (
                self.market_value != expected_value
                or self.account.cash + expected_value != self.account.nav
            ):
                raise ValueError("market value and NAV must reconcile to broker holdings")
        return self


class BacktestResult(RuntimeContractModel):
    schema_version: Literal[1] = 1
    execution_convention: Literal["SIMULATED_DAILY_OPEN"] = "SIMULATED_DAILY_OPEN"
    execution_assumption: Literal["按当日开盘价模拟撮合，不代表真实预挂单或保证成交"] = (
        "按当日开盘价模拟撮合，不代表真实预挂单或保证成交"
    )
    request_id: Sha256
    producer_commit: CommitSha
    input_generation_id: Sha256
    calendar_source_identity: Sha256
    cost_spec_id: Sha256
    status: Literal["complete", "incomplete"]
    days: tuple[BacktestDayResult, ...]
    content_hash: Sha256 | None = None

    @model_validator(mode="after")
    def validate_content_hash(self) -> Self:
        if not self.days:
            raise ValueError("backtest result must contain a trading day")
        if any(day.account is None for day in self.days[:-1]):
            raise ValueError("only the final replay day may be incomplete")
        expected_status = "incomplete" if self.days[-1].account is None else "complete"
        if self.status != expected_status:
            raise ValueError("backtest status must match the final day valuation")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash is None:
            object.__setattr__(self, "content_hash", expected)
        elif self.content_hash != expected:
            raise ValueError("backtest content hash does not match results")
        return self
