"""Built-in builders for isolated runtime services."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import pandas as pd
from pydantic import Field, StrictInt, field_validator

from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.market_minute_source_service import capture_market_minute_step
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceRegistry,
    RuntimeServiceStep,
)
from rquant.source_quota_store import SourceQuotaStore

if TYPE_CHECKING:
    from rquant.paper_signal_worker import QuoteResolver
    from rquant.runtime_builder_paper import TradeDateResolver
    from rquant.runtime_builder_serving import ServingSnapshotLoader
    from rquant.runtime_builder_signal import (
        ProviderLoader,
        SignalSourceLoader,
    )
    from rquant.runtime_builder_strategy import StrategyEvaluatorLoader
    from rquant.signal_router_runtime import TargetResolver

_TS_CODE_PATTERN = re.compile(r"^[0-9]{6}\.(?:BJ|SH|SZ)$")


class MarketMinuteAdapter(Protocol):
    def rt_min(self, codes: list[str], freq: str = "1min") -> pd.DataFrame: ...


class MarketMinuteSourceSettings(RuntimeContractModel):
    spool_root: Path
    quota_path: Path
    quota_units_per_window: StrictInt = Field(gt=0)
    quota_cost_per_request: StrictInt = Field(default=1, gt=0)
    max_codes_per_source_call: StrictInt = Field(default=300, gt=0, le=300)
    producer_version: str = Field(min_length=1)
    source: str = Field(default="tushare.rt_min", min_length=1)
    dataset_id: str = Field(default="market_minute", min_length=1)

    @field_validator("spool_root", "quota_path")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("runtime data paths must be absolute")
        return value


def _load_universe(loader: Callable[[], Iterable[str]]) -> tuple[str, ...]:
    raw = loader()
    if isinstance(raw, (str, bytes)):
        raise ValueError("market-minute universe must be an iterable of codes")
    normalized: set[str] = set()
    for code in raw:
        if not isinstance(code, str):
            raise ValueError("market-minute universe codes must be strings")
        candidate = code.strip().upper()
        if not _TS_CODE_PATTERN.fullmatch(candidate):
            raise ValueError(f"invalid market-minute universe code: {code!r}")
        normalized.add(candidate)
    if not normalized:
        raise ValueError("market-minute universe cannot be empty")
    return tuple(sorted(normalized))


def market_minute_source_builder(
    *,
    adapter_factory: Callable[[], MarketMinuteAdapter],
    universe_loader: Callable[[], Iterable[str]],
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.MARKET_MINUTE_SOURCE:
            raise ValueError("runtime service kind must be market_minute_source")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("market-minute source must run on the live plane")

        settings = MarketMinuteSourceSettings.model_validate(dict(manifest.settings))
        universe = _load_universe(universe_loader)
        adapter = adapter_factory()
        spool = LiveBatchSpool(settings.spool_root)
        quota_store = SourceQuotaStore(settings.quota_path)

        def fetch_current_universe() -> pd.DataFrame:
            call_count = (
                len(universe) + settings.max_codes_per_source_call - 1
            ) // settings.max_codes_per_source_call
            if call_count > settings.quota_cost_per_request:
                raise RuntimeError(
                    "market-minute source call budget is below the current universe"
                )
            frames: list[pd.DataFrame] = []
            for start in range(0, len(universe), settings.max_codes_per_source_call):
                batch = universe[start : start + settings.max_codes_per_source_call]
                frame = adapter.rt_min(list(batch), freq="1min")
                if not isinstance(frame, pd.DataFrame):
                    raise TypeError("market-minute adapter must return a DataFrame")
                frames.append(frame)
            if len(frames) == 1:
                return frames[0]
            return pd.concat(frames, ignore_index=True)

        gateway = MarketMinuteGateway(
            spool=spool,
            fetcher=fetch_current_universe,
            config=MarketMinuteGatewayConfig(
                source=settings.source,
                dataset_id=settings.dataset_id,
                producer_version=settings.producer_version,
                producer_commit=manifest.producer_commit,
                quota_units_per_window=settings.quota_units_per_window,
                quota_cost_per_request=settings.quota_cost_per_request,
            ),
            quota_store=quota_store,
        )

        first_step = True

        def step() -> RuntimeStepResult:
            nonlocal first_step, universe
            if first_step:
                first_step = False
            else:
                universe = _load_universe(universe_loader)
            return capture_market_minute_step(gateway, received_at=clock())

        return step

    return build


def _default_adapter_factory() -> MarketMinuteAdapter:
    from rquant.adapter.tushare import TushareAdapter

    return TushareAdapter()


def _default_universe_loader() -> tuple[str, ...]:
    from rquant.monitor import build_watchlist
    from rquant.storage.duckdb import open_readonly_store

    with open_readonly_store() as store:
        return tuple(item.ts_code for item in build_watchlist(store))


def build_builtin_registry(
    *,
    adapter_factory: Callable[[], MarketMinuteAdapter] | None = None,
    universe_loader: Callable[[], Iterable[str]] | None = None,
    clock: Callable[[], datetime] | None = None,
    evaluator_loader: StrategyEvaluatorLoader | None = None,
    signal_source_loader: SignalSourceLoader | None = None,
    target_resolver: TargetResolver | None = None,
    provider_loader: ProviderLoader | None = None,
    paper_quote_resolver: QuoteResolver | None = None,
    trade_date_resolver: TradeDateResolver | None = None,
    serving_snapshot_loader: ServingSnapshotLoader | None = None,
) -> RuntimeServiceRegistry:
    from rquant.runtime_builder_feature import feature_live_builder
    from rquant.runtime_builder_paper import paper_broker_builder, paper_consumer_builder
    from rquant.runtime_builder_serving import serving_publisher_builder
    from rquant.runtime_builder_signal import notifier_builder, signal_router_builder
    from rquant.runtime_builder_strategy import strategy_live_builder

    resolved_clock = clock or (lambda: datetime.now(UTC))
    if (signal_source_loader is None) != (target_resolver is None):
        raise ValueError("signal router dependencies must be provided together")
    if (paper_quote_resolver is None) != (trade_date_resolver is None):
        raise ValueError("paper broker dependencies must be provided together")
    registry = RuntimeServiceRegistry()
    registry.register(
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        market_minute_source_builder(
            adapter_factory=adapter_factory or _default_adapter_factory,
            universe_loader=universe_loader or _default_universe_loader,
            clock=resolved_clock,
        ),
    )
    registry.register(
        RuntimeServiceKind.FEATURE_LIVE,
        feature_live_builder(clock=resolved_clock),
    )
    if evaluator_loader is not None:
        registry.register(
            RuntimeServiceKind.STRATEGY_LIVE,
            strategy_live_builder(
                evaluator_loader=evaluator_loader,
                clock=resolved_clock,
            ),
        )
    if signal_source_loader is not None and target_resolver is not None:
        registry.register(
            RuntimeServiceKind.SIGNAL_ROUTER,
            signal_router_builder(
                source_loader=signal_source_loader,
                target_resolver=target_resolver,
                clock=resolved_clock,
            ),
        )
    if provider_loader is not None:
        registry.register(
            RuntimeServiceKind.NOTIFIER,
            notifier_builder(
                provider_loader=provider_loader,
                clock=resolved_clock,
            ),
        )
    registry.register(
        RuntimeServiceKind.PAPER_CONSUMER,
        paper_consumer_builder(clock=resolved_clock),
    )
    if paper_quote_resolver is not None and trade_date_resolver is not None:
        registry.register(
            RuntimeServiceKind.PAPER_BROKER,
            paper_broker_builder(
                clock=resolved_clock,
                quote_resolver=paper_quote_resolver,
                trade_date_resolver=trade_date_resolver,
            ),
        )
    if serving_snapshot_loader is not None:
        registry.register(
            RuntimeServiceKind.SERVING_PUBLISHER,
            serving_publisher_builder(
                snapshot_loader=serving_snapshot_loader,
                clock=resolved_clock,
            ),
        )
    return registry


__all__ = [
    "MarketMinuteAdapter",
    "MarketMinuteSourceSettings",
    "build_builtin_registry",
    "market_minute_source_builder",
]
