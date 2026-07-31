"""Built-in builders for isolated runtime services."""

from __future__ import annotations

import os
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
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseLoader,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_market_session import (
    decide_market_session,
    load_market_calendar_authority,
)
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
    calendar_path: Path | None = None
    candidate_authorities: tuple[CandidateUniverseAuthority, ...] = ()

    @field_validator("spool_root", "quota_path", "calendar_path")
    @classmethod
    def require_absolute_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
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
    universe_loader: Callable[[], Iterable[str]] | None,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.MARKET_MINUTE_SOURCE:
            raise ValueError("runtime service kind must be market_minute_source")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("market-minute source must run on the live plane")

        settings = MarketMinuteSourceSettings.model_validate(dict(manifest.settings))
        authoritative_universe = universe_loader is None
        if authoritative_universe:
            if settings.calendar_path is None or not settings.candidate_authorities:
                raise ValueError(
                    "default market-minute source requires calendar_path and candidate_authorities"
                )
            calendar = load_market_calendar_authority(
                settings.calendar_path,
                expected_commit=manifest.producer_commit,
            )
            candidate_loader = RuntimeCandidateUniverseLoader(
                RuntimeCandidateUniverseConfig(
                    expected_commit=manifest.producer_commit,
                    authorities=settings.candidate_authorities,
                )
            )
        else:
            if settings.calendar_path is not None or settings.candidate_authorities:
                raise ValueError(
                    "explicit universe_loader cannot be combined with manifest authorities"
                )
            calendar = None
            candidate_loader = None
        universe: tuple[str, ...] = ()
        adapter = adapter_factory()
        spool = LiveBatchSpool(settings.spool_root)
        quota_store = SourceQuotaStore(settings.quota_path)

        def source_call_count() -> int:
            call_count = (
                len(universe) + settings.max_codes_per_source_call - 1
            ) // settings.max_codes_per_source_call
            if call_count > settings.quota_cost_per_request:
                raise ValueError("market-minute source call budget is below the current universe")
            return call_count

        def fetch_current_universe() -> pd.DataFrame:
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

        def capture_current_universe(observed_at: datetime) -> RuntimeStepResult:
            call_count = source_call_count()
            gateway = MarketMinuteGateway(
                spool=spool,
                fetcher=fetch_current_universe,
                config=MarketMinuteGatewayConfig(
                    source=settings.source,
                    dataset_id=settings.dataset_id,
                    producer_version=settings.producer_version,
                    producer_commit=manifest.producer_commit,
                    quota_units_per_window=settings.quota_units_per_window,
                    quota_cost_per_request=call_count,
                ),
                quota_store=quota_store,
            )
            return capture_market_minute_step(gateway, received_at=observed_at)

        last_result = RuntimeStepResult()

        def step() -> RuntimeStepResult:
            nonlocal last_result, universe
            observed_at = clock()
            evidence: dict[str, str] = {}
            if candidate_loader is not None and calendar is not None:
                decision = decide_market_session(calendar, observed_at)
                evidence["market_calendar"] = calendar.content_sha256
                if not decision.may_fetch_market_minute:
                    return RuntimeStepResult(
                        input_sequence=last_result.input_sequence,
                        output_sequence=last_result.output_sequence,
                        backlog_count=last_result.backlog_count,
                        source_generations={
                            **dict(last_result.source_generations),
                            **evidence,
                        },
                        degraded_reasons=last_result.degraded_reasons,
                    )
                candidate_result = candidate_loader.load(
                    as_of=observed_at,
                    required_trade_date=decision.local_trade_date,
                )
                universe = _load_universe(lambda: candidate_result.codes)
                evidence["candidate_universe"] = candidate_result.content_fingerprint
            else:
                if universe_loader is None:
                    raise RuntimeError("market-minute universe loader is unavailable")
                universe = _load_universe(universe_loader)
            result = capture_current_universe(observed_at)
            if not evidence:
                last_result = result
                return result
            last_result = RuntimeStepResult(
                **{
                    **result.model_dump(mode="python"),
                    "source_generations": {
                        **dict(result.source_generations),
                        **evidence,
                    },
                }
            )
            return last_result

        return step

    return build


def _default_adapter_factory() -> MarketMinuteAdapter:
    from rquant.adapter.tushare import TushareAdapter

    token = os.environ.get("TUSHARE_TOKEN_MAIN", "").strip()
    if not token:
        raise RuntimeError("TUSHARE_TOKEN_MAIN capability is required")
    backup_token = os.environ.get("TUSHARE_TOKEN_BACKUP", "").strip()
    return TushareAdapter(token=token, backup_token=backup_token)


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
            universe_loader=universe_loader,
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
        from rquant.runtime_builder_serving import serving_publisher_builder

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
