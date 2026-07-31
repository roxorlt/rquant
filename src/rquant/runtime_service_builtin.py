"""Built-in builders for isolated runtime services."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

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

_TS_CODE_PATTERN = re.compile(r"^[0-9]{6}\.(?:BJ|SH|SZ)$")


class MarketMinuteAdapter(Protocol):
    def rt_min(self, codes: list[str], freq: str = "1min") -> pd.DataFrame: ...


class MarketMinuteSourceSettings(RuntimeContractModel):
    spool_root: Path
    quota_path: Path
    quota_units_per_window: StrictInt = Field(gt=0)
    quota_cost_per_request: StrictInt = Field(default=1, gt=0)
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
        gateway = MarketMinuteGateway(
            spool=spool,
            fetcher=lambda: adapter.rt_min(list(universe), freq="1min"),
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

        def step() -> RuntimeStepResult:
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
) -> RuntimeServiceRegistry:
    registry = RuntimeServiceRegistry()
    registry.register(
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        market_minute_source_builder(
            adapter_factory=adapter_factory or _default_adapter_factory,
            universe_loader=universe_loader or _default_universe_loader,
            clock=clock or (lambda: datetime.now(UTC)),
        ),
    )
    return registry


__all__ = [
    "MarketMinuteAdapter",
    "MarketMinuteSourceSettings",
    "build_builtin_registry",
    "market_minute_source_builder",
]
