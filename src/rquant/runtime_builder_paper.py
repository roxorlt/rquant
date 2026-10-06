"""Runtime builders for durable paper signal delegation and execution."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, StrictBool, StrictInt, field_validator, model_validator

from rquant.paper_broker import BrokerCostPolicy, PaperBrokerStore
from rquant.paper_portfolio_runtime import PaperPortfolioRuntimeCatalog
from rquant.paper_ledger_anchor import Ed25519PaperLedgerAnchorVerifier
from rquant.paper_signal_consumer import (
    PaperSignalConsumerStateStore,
    consume_notification_events_to_paper,
)
from rquant.paper_signal_worker import (
    PaperSignalPolicy,
    PaperSignalQueueStore,
    QuoteResolver,
    run_paper_signal_batch,
)
from rquant.research_run_spec import ExecutionCostSpec
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction
from rquant.signal_route_spool import ReadonlyNotificationEventRouteSpool


class PaperSignalPolicySettings(RuntimeContractModel):
    account_id: str = Field(min_length=1)
    execution_lag_seconds: StrictInt = Field(gt=0)
    buy_quantity: StrictInt = Field(gt=0)
    reduce_quantity: StrictInt = Field(gt=0)
    sell_quantity: StrictInt = Field(gt=0)

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


class PaperConsumerSettings(PaperSignalPolicySettings):
    condition_history_enabled: StrictBool = False
    signal_bus_path: Path
    queue_path: Path
    consumer_state_path: Path
    limit: StrictInt = Field(gt=0)
    busy_timeout_ms: StrictInt = Field(default=5_000, gt=0)
    paused: StrictBool = False

    @field_validator("signal_bus_path", "queue_path", "consumer_state_path")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("paper runtime paths must be absolute")
        return value


class PaperBrokerSettings(PaperSignalPolicySettings):
    condition_history_enabled: StrictBool = False
    signal_spool_root: Path
    queue_path: Path
    consumer_state_path: Path
    broker_path: Path
    initial_cash: Decimal = Field(gt=0, allow_inf_nan=False)
    execution_cost_spec: ExecutionCostSpec
    limit: StrictInt = Field(gt=0)
    busy_timeout_ms: StrictInt = Field(default=5_000, gt=0)
    raw_spool_root: Path | None = None
    trade_calendar_path: Path | None = None
    trade_calendar_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    execution_constraint_root: Path | None = None
    ledger_id: str | None = Field(default=None, min_length=1)
    ledger_anchor_path: Path | None = None
    ledger_anchor_public_key_path: Path | None = None
    ledger_anchor_key_id: str | None = Field(default=None, min_length=1)
    ledger_anchor_max_age_seconds: StrictInt | None = Field(default=None, gt=0)
    ledger_anchor_future_skew_seconds: StrictInt | None = Field(default=None, ge=0)
    timestamp_semantics: Literal["bar_end", "provider_snapshot"] = "provider_snapshot"
    quote_max_age_seconds: StrictInt = Field(default=90, gt=0, le=300)
    max_finalize_scan_batches: StrictInt = Field(default=32, gt=0, le=120)
    max_visible_scan_batches: StrictInt = Field(default=120, gt=0, le=1_000)
    serving_authority_root: Path | None = None
    paused: StrictBool = False

    @field_validator(
        "queue_path",
        "consumer_state_path",
        "broker_path",
        "signal_spool_root",
        "raw_spool_root",
        "trade_calendar_path",
        "execution_constraint_root",
        "serving_authority_root",
        "ledger_anchor_path",
        "ledger_anchor_public_key_path",
    )
    @classmethod
    def require_absolute_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute():
            raise ValueError("paper runtime paths must be absolute")
        return value

    @model_validator(mode="after")
    def validate_pit_authority_group(self) -> PaperBrokerSettings:
        configured = (
            self.raw_spool_root,
            self.trade_calendar_path,
            self.trade_calendar_sha256,
            self.execution_constraint_root,
        )
        if any(value is not None for value in configured) and not all(
            value is not None for value in configured
        ):
            raise ValueError("paper PIT authorities must be configured together")
        if not self.execution_cost_spec.is_alignment_eligible:
            raise ValueError("paper broker requires an explicit v3 execution_cost_spec")
        anchor_values = (
            self.ledger_id,
            self.ledger_anchor_path,
            self.ledger_anchor_public_key_path,
            self.ledger_anchor_key_id,
            self.ledger_anchor_max_age_seconds,
            self.ledger_anchor_future_skew_seconds,
        )
        if any(value is not None for value in anchor_values) and not all(
            value is not None for value in anchor_values
        ):
            raise ValueError("paper ledger anchor settings must be configured together")
        return self

    def cost_policy(self) -> BrokerCostPolicy:
        return BrokerCostPolicy.from_execution_cost_spec(self.execution_cost_spec)


def _paper_settings(
    manifest: RuntimeServiceManifest,
    *,
    kind: RuntimeServiceKind,
) -> PaperConsumerSettings | PaperBrokerSettings:
    if manifest.service_kind is not kind:
        raise ValueError(f"runtime service kind must be {kind.value}")
    if manifest.plane is not RuntimeServicePlane.LIVE:
        raise ValueError(f"{kind.value} must run on the live plane")
    model = {
        RuntimeServiceKind.PAPER_CONSUMER: PaperConsumerSettings,
        RuntimeServiceKind.PAPER_BROKER: PaperBrokerSettings,
    }[kind]
    return model.model_validate(dict(manifest.settings))


def paper_consumer_builder(*, clock: Callable[[], datetime]) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        settings = _paper_settings(manifest, kind=RuntimeServiceKind.PAPER_CONSUMER)
        if not isinstance(settings, PaperConsumerSettings):
            raise TypeError("paper consumer settings are unavailable")
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
        if settings.condition_history_enabled:
            state.install_condition_notification_history()

        def step() -> RuntimeStepResult:
            if settings.paused:
                descriptor = bus.source_descriptor()
                cursor = state.cursor()
                return RuntimeStepResult(
                    input_sequence=descriptor.high_watermark,
                    output_sequence=cursor.last_global_sequence,
                    backlog_count=(descriptor.high_watermark - cursor.last_global_sequence),
                    source_generations={"signal_bus": descriptor.generation_id},
                    degraded_reasons=("paper_consumer:paused",),
                    # A paused consumer never observes the source, so nothing moved.
                    watermark_advanced=False,
                )
            summary = consume_notification_events_to_paper(
                bus,
                queue,
                state,
                observed_at=clock(),
                limit=settings.limit,
            )
            return RuntimeStepResult(
                input_sequence=summary.started_after_sequence,
                output_sequence=summary.ended_at_sequence,
                processed_count=summary.delegated_count
                + summary.replayed_count
                + summary.ignored_non_trading_count,
                backlog_count=max(
                    0,
                    summary.source_high_watermark - summary.ended_at_sequence,
                ),
                source_generations={"signal_bus": summary.source_generation_id},
                watermark_advanced=summary.watermark_advanced,
            )

        return step

    return build


TradeDateResolver = Callable[[datetime], date]


def paper_broker_builder(
    *,
    clock: Callable[[], datetime],
    quote_resolver: QuoteResolver | None = None,
    trade_date_resolver: TradeDateResolver | None = None,
    portfolio_catalog: PaperPortfolioRuntimeCatalog | None = None,
) -> RuntimeServiceBuilder:
    if portfolio_catalog is not None and type(portfolio_catalog) is not PaperPortfolioRuntimeCatalog:
        raise TypeError("paper builder requires its finite concrete portfolio catalog")
    if (quote_resolver is None) != (trade_date_resolver is None):
        raise ValueError("paper broker dependencies must be provided together")

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        settings = _paper_settings(manifest, kind=RuntimeServiceKind.PAPER_BROKER)
        if not isinstance(settings, PaperBrokerSettings):
            raise TypeError("paper broker settings are unavailable")
        portfolio_runtime = portfolio_catalog.for_manifest(manifest) if portfolio_catalog else None
        if (portfolio_runtime is not None and portfolio_runtime.risk_publisher is not None
                and portfolio_runtime.risk_publisher.spool.paths.root != settings.signal_spool_root):
            raise ValueError("paper risk route differs from the original exact role spool")
        if quote_resolver is None and trade_date_resolver is None:
            if (
                settings.raw_spool_root is None
                or settings.trade_calendar_path is None
                or settings.trade_calendar_sha256 is None
                or settings.execution_constraint_root is None
            ):
                raise ValueError("default paper broker requires complete PIT authorities")
            from rquant.runtime_paper_quote import (
                PaperPitQuoteResolver,
                PaperQuoteResolverConfig,
            )

            pit_resolver = PaperPitQuoteResolver(
                PaperQuoteResolverConfig(
                    raw_spool_root=settings.raw_spool_root,
                    trade_calendar_path=settings.trade_calendar_path,
                    trade_calendar_sha256=settings.trade_calendar_sha256,
                    execution_constraint_root=settings.execution_constraint_root,
                    expected_producer_commit=manifest.producer_commit,
                    timestamp_semantics=settings.timestamp_semantics,
                    quote_max_age_seconds=settings.quote_max_age_seconds,
                    max_finalize_scan_batches=settings.max_finalize_scan_batches,
                    max_visible_scan_batches=settings.max_visible_scan_batches,
                )
            )
            resolved_quote_resolver = pit_resolver
            resolved_trade_date_resolver = pit_resolver.trade_date_at
            constraint_generation_resolver = pit_resolver.constraint_generation_at
            if portfolio_runtime is not None:
                from rquant.backtest.contracts import SSECalendar

                dates = pit_resolver._sse_open_dates
                portfolio_runtime.calendar = SSECalendar(source_identity=settings.trade_calendar_sha256,
                                                        coverage_start=dates[0], coverage_end=dates[-1], dates=dates)
        else:
            if quote_resolver is None or trade_date_resolver is None:
                raise RuntimeError("paper broker dependencies are unavailable")
            resolved_quote_resolver = quote_resolver
            resolved_trade_date_resolver = trade_date_resolver
            constraint_generation_resolver = None
        policy = settings.signal_policy(manifest.producer_commit)
        cost_policy = settings.cost_policy()
        source = ReadonlyNotificationEventRouteSpool(settings.signal_spool_root)
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
            **(
                {}
                if settings.ledger_anchor_public_key_path is None
                else {
                    "ledger_id": settings.ledger_id,
                    "ledger_anchor_path": settings.ledger_anchor_path,
                    "ledger_anchor_verifier": Ed25519PaperLedgerAnchorVerifier(
                        active_key_id=settings.ledger_anchor_key_id,
                        active_public_key=(settings.ledger_anchor_public_key_path.read_bytes()),
                        allowed_ledger_id=settings.ledger_id,
                        max_age=timedelta(seconds=settings.ledger_anchor_max_age_seconds),
                        future_skew=timedelta(seconds=settings.ledger_anchor_future_skew_seconds),
                        clock=clock,
                    ),
                }
            ),
        )
        state = PaperSignalConsumerStateStore(
            settings.consumer_state_path,
            busy_timeout_ms=settings.busy_timeout_ms,
        )
        if settings.condition_history_enabled:
            state.install_condition_notification_history()
        authority_publisher = None
        portfolio_view_source = None
        portfolio_sequence_floor = None
        if settings.serving_authority_root is not None:
            from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher

            authority_publisher = ServingSourceAuthorityPublisher(
                root=settings.serving_authority_root,
                producer_commit=manifest.producer_commit,
                dataset_id="paper_accounts",
                payload_kind="paper_accounts",
                clock=clock,
            )
            if portfolio_runtime is not None:
                from rquant.paper_portfolio_view_source import PaperPortfolioViewSource

                portfolio_view_source = PaperPortfolioViewSource(portfolio_runtime, broker=broker, queue=queue)

        def publish_account_authority(observed_at: datetime) -> str | None:
            nonlocal portfolio_sequence_floor
            if authority_publisher is None:
                return None
            from rquant.paper_history_serving import paper_history_projections
            from rquant.runtime_serving_snapshot import (
                PaperAccountsPayload,
                SourceReadResult,
            )
            from rquant.serving_contracts import FreshnessStatus

            marks = broker.latest_execution_prices(as_of=observed_at)
            authority_state = broker.account_authority_snapshot(
                as_of=observed_at,
                market_prices=marks,
                producer_commit=manifest.producer_commit,
            )
            history = broker.recent_order_history(as_of=observed_at)
            account = authority_state.snapshot
            reason = "paper account marks use last execution prices" if account.holdings else None
            projections = paper_history_projections(history)
            sequence = history.ledger_revision
            if portfolio_view_source is not None:
                from rquant.paper_portfolio_projection import PaperPortfolioSnapshot, paper_portfolio_projections
                from rquant.runtime_serving_authority import ServingSourceAuthorityReader, ServingSourceAuthorityUnavailableError

                portfolio_account = portfolio_view_source.read(as_of=observed_at)
                portfolio_snapshot = PaperPortfolioSnapshot(available_at=observed_at, accounts=(portfolio_account,))
                projections += paper_portfolio_projections(portfolio_snapshot)
                if portfolio_sequence_floor is None:
                    try:
                        previous = ServingSourceAuthorityReader(root=settings.serving_authority_root, expected_producer_commit=manifest.producer_commit,
                                                                expected_dataset_id="paper_accounts", expected_payload_kind="paper_accounts")(observed_at)
                        portfolio_sequence_floor = previous.sequence
                    except ServingSourceAuthorityUnavailableError:
                        portfolio_sequence_floor = 0
                sequence = portfolio_view_source.publication_sequence(portfolio_snapshot, minimum_sequence=max(sequence, portfolio_sequence_floor))
                if portfolio_account.reason:
                    reason = "; ".join(part for part in (reason, portfolio_account.reason) if part)
            values: dict[str, object] = {
                "dataset_id": "paper_accounts",
                "sequence": sequence,
                "event_time": observed_at,
                "published_at": observed_at,
                "status": (
                    FreshnessStatus.DEGRADED if reason is not None else FreshnessStatus.FRESH
                ),
                "reason": reason,
                "payload": PaperAccountsPayload(
                    paper_accounts=(account,),
                    projections=projections,
                ),
            }
            values["generation_id"] = canonical_sha256(values)

            def unchanged_identity(read: SourceReadResult) -> str:
                payload = read.payload
                if not isinstance(payload, PaperAccountsPayload):
                    raise ValueError("paper authority payload is inconsistent")
                tables = {item.table_name: item for item in payload.projections}
                if "paper_portfolio_state" in tables:
                    from rquant.paper_portfolio_projection import validate_paper_portfolio_projections
                    from rquant.paper_portfolio_view_source import paper_portfolio_publication_identity

                    return paper_portfolio_publication_identity(validate_paper_portfolio_projections(tables))
                if set(tables) != {
                    "paper_order_window",
                    "paper_order_history",
                    "paper_fill_history",
                }:
                    return canonical_sha256({"legacy_paper_payload": payload})
                window = tables["paper_order_window"].rows[0]
                return canonical_sha256(
                    {
                        "status": read.status,
                        "reason": read.reason,
                        "accounts": payload.paper_accounts,
                        "window": {
                            key: value for key, value in window.items() if key != "as_of_time"
                        },
                        "orders": tables["paper_order_history"].rows,
                        "fills": tables["paper_fill_history"].rows,
                    }
                )

            publication = authority_publisher.publish_if_changed(
                SourceReadResult.model_validate(values), unchanged_identity=unchanged_identity
            )
            return publication.pointer.generation_id

        def step() -> RuntimeStepResult:
            observed_at = clock()
            descriptor = source.source_descriptor()
            constraint_generation = (
                {"paper_execution_constraints": constraint_generation_resolver(observed_at)}
                if constraint_generation_resolver is not None
                else {}
            )
            if settings.paused:
                cursor = state.cursor()
                authority_generation = publish_account_authority(observed_at)
                return RuntimeStepResult(
                    input_sequence=descriptor.high_watermark,
                    output_sequence=cursor.last_global_sequence,
                    backlog_count=(descriptor.high_watermark - cursor.last_global_sequence),
                    source_generations={
                        "signal_route_spool": descriptor.generation_id,
                        "paper_cost_policy": cost_policy.fingerprint,
                        "paper_execution_cost_spec": cost_policy.cost_spec_id,
                        "paper_signal_policy": policy.fingerprint,
                        **constraint_generation,
                        **(
                            {"paper_accounts": authority_generation}
                            if authority_generation is not None
                            else {}
                        ),
                    },
                    degraded_reasons=("paper_broker:paused",),
                    # A paused broker never observes the spool source, so nothing moved.
                    watermark_advanced=False,
                )
            consumed = consume_notification_events_to_paper(
                source,
                queue,
                state,
                observed_at=observed_at,
                limit=settings.limit,
            )
            summary = run_paper_signal_batch(
                queue,
                broker,
                now=observed_at,
                trade_date=resolved_trade_date_resolver(observed_at),
                quote_resolver=resolved_quote_resolver,
                limit=settings.limit,
                portfolio_runtime=portfolio_runtime,
            )
            if portfolio_runtime is not None:
                try:
                    with portfolio_runtime.state._connection(write=True):
                        if portfolio_runtime.risk_publisher is not None:
                            portfolio_runtime.risk_publisher.publish_pending(observed_at=observed_at)
                        portfolio_runtime.plan_risk_reductions(broker, decision_at=observed_at,
                                                               trade_date=resolved_trade_date_resolver(observed_at),
                                                               quote_resolver=resolved_quote_resolver, queue=queue)
                    portfolio_runtime.risk_reason = None
                except ValueError as exc:
                    portfolio_runtime.risk_reason = str(exc)
            failures = summary.failed_count
            authority_generation = publish_account_authority(observed_at)
            portfolio_generations = {}
            if portfolio_runtime is not None:
                applied = portfolio_runtime.operator.current()
                portfolio_generations = {
                    "paper_portfolio_configuration": portfolio_runtime.state.configuration.fingerprint,
                    "paper_operator_application": canonical_sha256(applied),
                    **({"paper_operator_control": applied.control_fingerprint} if applied.control_fingerprint else {}),
                }
            return RuntimeStepResult(
                input_sequence=descriptor.high_watermark,
                output_sequence=consumed.ended_at_sequence,
                processed_count=summary.completed_count,
                backlog_count=(descriptor.high_watermark - consumed.ended_at_sequence + failures),
                source_generations={
                    "signal_route_spool": descriptor.generation_id,
                    "paper_cost_policy": cost_policy.fingerprint,
                    "paper_execution_cost_spec": cost_policy.cost_spec_id,
                    "paper_signal_policy": policy.fingerprint,
                    **portfolio_generations,
                    **constraint_generation,
                    **(
                        {"paper_accounts": authority_generation}
                        if authority_generation is not None
                        else {}
                    ),
                },
                degraded_reasons=(("paper_execution_failed",) if failures else ())
                                 + (("paper_portfolio_risk_unavailable",) if portfolio_runtime is not None and portfolio_runtime.risk_reason else ()),
                observations=({"paper_operator_applied_sequence": applied.sequence} if portfolio_runtime is not None else {}),
                watermark_advanced=consumed.watermark_advanced,
            )

        return step

    return build


__all__ = [
    "PaperBrokerSettings",
    "PaperConsumerSettings",
    "PaperSignalPolicySettings",
    "TradeDateResolver",
    "paper_broker_builder",
    "paper_consumer_builder",
]
