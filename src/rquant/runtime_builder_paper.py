"""Runtime builders for durable paper signal delegation and execution."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from pydantic import Field, StrictInt, field_validator

from rquant.paper_broker import BrokerCostPolicy, PaperBrokerStore
from rquant.paper_signal_consumer import (
    PaperSignalConsumerStateStore,
    consume_signal_bus_to_paper,
)
from rquant.paper_signal_worker import (
    PaperSignalPolicy,
    PaperSignalQueueStore,
    QuoteResolver,
    run_paper_signal_batch,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction


class PaperRuntimeSettings(RuntimeContractModel):
    signal_bus_path: Path
    queue_path: Path
    consumer_state_path: Path
    broker_path: Path
    account_id: str = Field(min_length=1)
    execution_lag_seconds: StrictInt = Field(gt=0)
    buy_quantity: StrictInt = Field(gt=0)
    reduce_quantity: StrictInt = Field(gt=0)
    sell_quantity: StrictInt = Field(gt=0)
    initial_cash: Decimal = Field(gt=0, allow_inf_nan=False)
    commission_rate: Decimal = Field(ge=0, lt=1, allow_inf_nan=False)
    minimum_commission: Decimal = Field(ge=0, allow_inf_nan=False)
    sell_stamp_tax_rate: Decimal = Field(ge=0, lt=1, allow_inf_nan=False)
    buy_slippage_bps: Decimal = Field(default=Decimal("0"), ge=0, lt=10_000)
    sell_slippage_bps: Decimal = Field(default=Decimal("0"), ge=0, lt=10_000)
    limit: StrictInt = Field(gt=0)
    busy_timeout_ms: StrictInt = Field(default=5_000, gt=0)

    @field_validator(
        "signal_bus_path",
        "queue_path",
        "consumer_state_path",
        "broker_path",
    )
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("paper runtime paths must be absolute")
        return value

    @field_validator("buy_quantity", "reduce_quantity", "sell_quantity")
    @classmethod
    def require_board_lot(cls, value: int) -> int:
        if value % 100:
            raise ValueError("paper quantities must be 100-share lots")
        return value

    def signal_policy(self, producer_commit: str) -> PaperSignalPolicy:
        return PaperSignalPolicy(
            account_id=self.account_id,
            execution_lag=timedelta(seconds=self.execution_lag_seconds),
            action_quantities={
                SignalAction.B_INTENT: self.buy_quantity,
                SignalAction.REDUCE: self.reduce_quantity,
                SignalAction.S_INTENT: self.sell_quantity,
            },
            producer_commit=producer_commit,
        )

    def cost_policy(self) -> BrokerCostPolicy:
        return BrokerCostPolicy(
            commission_rate=self.commission_rate,
            minimum_commission=self.minimum_commission,
            sell_stamp_tax_rate=self.sell_stamp_tax_rate,
            buy_slippage_bps=self.buy_slippage_bps,
            sell_slippage_bps=self.sell_slippage_bps,
        )


def _paper_settings(
    manifest: RuntimeServiceManifest,
    *,
    kind: RuntimeServiceKind,
) -> PaperRuntimeSettings:
    if manifest.service_kind is not kind:
        raise ValueError(f"runtime service kind must be {kind.value}")
    if manifest.plane is not RuntimeServicePlane.LIVE:
        raise ValueError(f"{kind.value} must run on the live plane")
    return PaperRuntimeSettings.model_validate(dict(manifest.settings))


def paper_consumer_builder(*, clock: Callable[[], datetime]) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        settings = _paper_settings(manifest, kind=RuntimeServiceKind.PAPER_CONSUMER)
        bus = SignalBusStore(
            settings.signal_bus_path,
            busy_timeout_ms=settings.busy_timeout_ms,
        )
        queue = PaperSignalQueueStore(
            settings.queue_path,
            policy=settings.signal_policy(manifest.producer_commit),
            busy_timeout_ms=settings.busy_timeout_ms,
        )
        state = PaperSignalConsumerStateStore(
            settings.consumer_state_path,
            busy_timeout_ms=settings.busy_timeout_ms,
        )

        def step() -> RuntimeStepResult:
            summary = consume_signal_bus_to_paper(
                bus,
                queue,
                state,
                observed_at=clock(),
                limit=settings.limit,
            )
            return RuntimeStepResult(
                input_sequence=summary.started_after_sequence,
                output_sequence=summary.ended_at_sequence,
                processed_count=summary.delegated_count + summary.replayed_count,
                backlog_count=max(
                    0,
                    summary.source_high_watermark - summary.ended_at_sequence,
                ),
                source_generations={"signal_bus": summary.source_generation_id},
            )

        return step

    return build


TradeDateResolver = Callable[[datetime], date]


def paper_broker_builder(
    *,
    clock: Callable[[], datetime],
    quote_resolver: QuoteResolver,
    trade_date_resolver: TradeDateResolver,
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        settings = _paper_settings(manifest, kind=RuntimeServiceKind.PAPER_BROKER)
        policy = settings.signal_policy(manifest.producer_commit)
        cost_policy = settings.cost_policy()
        queue = PaperSignalQueueStore(
            settings.queue_path,
            policy=policy,
            busy_timeout_ms=settings.busy_timeout_ms,
        )
        broker = PaperBrokerStore(
            settings.broker_path,
            account_id=settings.account_id,
            initial_cash=settings.initial_cash,
            cost_policy=cost_policy,
            busy_timeout_ms=settings.busy_timeout_ms,
        )

        def step() -> RuntimeStepResult:
            observed_at = clock()
            summary = run_paper_signal_batch(
                queue,
                broker,
                now=observed_at,
                trade_date=trade_date_resolver(observed_at),
                quote_resolver=quote_resolver,
                limit=settings.limit,
            )
            failures = summary.failed_count
            return RuntimeStepResult(
                processed_count=summary.completed_count,
                backlog_count=failures,
                source_generations={
                    "paper_cost_policy": cost_policy.fingerprint,
                    "paper_signal_policy": policy.fingerprint,
                },
                degraded_reasons=("paper_execution_failed",) if failures else (),
            )

        return step

    return build


__all__ = [
    "PaperRuntimeSettings",
    "TradeDateResolver",
    "paper_broker_builder",
    "paper_consumer_builder",
]
