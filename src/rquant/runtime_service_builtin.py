"""Built-in builders for isolated runtime services."""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, InstanceOf, StrictInt, field_validator, model_validator

from rquant.auction_match_gateway import AuctionMatchGateway, AuctionMatchGatewayConfig
from rquant.auction_match_source_service import capture_auction_match_step
from rquant.auction_universe_authority import (
    AuctionUniverseAuthorityIntegrityError,
    load_auction_universe_authority,
)
from rquant.auction_universe_source import (
    AuctionUniverseSourceError,
    auction_universe_publication_dates,
    publish_auction_universe_from_daily_snapshot,
)
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import (
    LiveBatchSpool,
    ReferenceSourceBatchSigner,
    ReferenceSourceBatchVerifier,
)
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.market_minute_source_service import capture_market_minute_step
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseLoader,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.runtime_market_session import (
    MarketCalendarAuthority,
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
from rquant.source_quota_store import (
    SourceQuotaAttemptOutcome,
    SourceQuotaConflictError,
    SourceQuotaStore,
)
from rquant.source_quota_transport import (
    QuotaBoundTransportObserver,
    SourceTransportUsageReceipt,
)
from rquant.watchlist_quote_gateway import WatchlistQuoteGateway, WatchlistQuoteGatewayConfig
from rquant.watchlist_quote_source_service import capture_watchlist_quote_step

if TYPE_CHECKING:
    from rquant.paper_signal_worker import QuoteResolver
    from rquant.reference_slow_publisher import ReferenceSlowSourceSnapshot
    from rquant.reference_slow_source import ReferenceSlowAdapter
    from rquant.runtime_artifact_terminal_lifecycle import ProductionArtifactTerminalLifecycle
    from rquant.runtime_builder_candidate import (
        AuctionCandidateInputLoader,
        CandidateInputLoader,
    )
    from rquant.runtime_builder_paper import TradeDateResolver
    from rquant.runtime_builder_serving import ServingSnapshotLoader
    from rquant.runtime_builder_signal import (
        ProviderLoader,
        SignalSourceLoader,
    )
    from rquant.runtime_builder_strategy import StrategyEvaluatorLoader
    from rquant.runtime_shadow_validation import CompletionAttestationSigner
    from rquant.signal_router_runtime import TargetResolver

_TS_CODE_PATTERN = re.compile(r"^[0-9]{6}\.(?:BJ|SH|SZ)$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class MarketMinuteAdapter(Protocol):
    def rt_min(self, codes: list[str], freq: str = "1min") -> pd.DataFrame: ...


class AuctionMatchAdapter(Protocol):
    def stk_auction(self, trade_date: date) -> pd.DataFrame: ...


class WatchlistQuoteProvider(Protocol):
    def __call__(self, codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame: ...


class ReferenceSlowSourceCaptureLimits(RuntimeContractModel):
    snapshot_max_bytes: StrictInt = Field(default=8 * 1024**3, gt=0)
    snapshot_min_free_bytes: StrictInt = Field(default=1024**3, ge=0)
    snapshot_copy_timeout_seconds: float = Field(default=60.0, gt=0, le=300)
    query_chunk_rows: StrictInt = Field(default=512, gt=0, le=10_000)
    max_response_rows: StrictInt = Field(default=10_000, gt=0, le=100_000)
    max_response_bytes: StrictInt = Field(default=8 * 1024**2, gt=0)


class ReferenceSlowSourceSettings(RuntimeContractModel):
    database_path: Path
    calendar_path: Path
    calendar_expected_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    spool_root: Path
    quota_path: Path
    quota_units_per_window: StrictInt = Field(gt=0)
    quota_accounting_mode: Literal["request", "transport"] = "request"
    quota_cost_per_capture: StrictInt | None = Field(default=6, gt=0)
    retry_ordinal: StrictInt = Field(default=0, ge=0)
    pending_recovery_min_age_seconds: StrictInt = Field(default=60, ge=30)
    revision_lookback_sessions: StrictInt = Field(default=5, ge=1, le=20)
    history_page_size: StrictInt = Field(default=64, ge=5, le=256)
    limits: ReferenceSlowSourceCaptureLimits = Field(
        default_factory=ReferenceSlowSourceCaptureLimits
    )
    consumer_cursor_root: Path | None = None
    retention_consumer_id: str = Field(default="reference-slow-publisher", min_length=1)
    retention_hot_batches: StrictInt = Field(default=128, ge=1, le=4096)
    retention_page_size: StrictInt = Field(default=32, ge=1, le=256)
    producer_version: str = Field(min_length=1)
    source: str = Field(default="tushare.reference_slow", min_length=1)

    @field_validator("database_path", "calendar_path", "spool_root", "quota_path")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("reference slow source paths must be absolute")
        return value

    @field_validator("consumer_cursor_root")
    @classmethod
    def require_absolute_optional_path(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("reference slow cursor root must be absolute")
        return value

    @model_validator(mode="after")
    def retain_revision_history_window(self) -> ReferenceSlowSourceSettings:
        if self.retention_hot_batches < self.history_page_size:
            raise ValueError("retention_hot_batches must cover the reference history page")
        if self.quota_accounting_mode == "request":
            if self.quota_cost_per_capture is None or self.quota_cost_per_capture < 6:
                raise ValueError("quota_cost_per_capture must cover six adapter requests")
        elif self.quota_cost_per_capture is not None:
            raise ValueError("transport quota accounting cannot declare a fixed capture cost")
        return self


class ReferenceSlowQuotaCapture(RuntimeContractModel):
    snapshot: InstanceOf[RuntimeContractModel]
    source_usage: SourceTransportUsageReceipt


class ReferenceSlowPublisherSettings(RuntimeContractModel):
    calendar_path: Path
    calendar_expected_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    spool_root: Path
    registry_path: Path
    cursor_root: Path
    consumer_id: str = Field(default="reference-slow-publisher", min_length=1)
    page_size: StrictInt = Field(default=16, ge=1, le=256)

    @field_validator("calendar_path", "spool_root", "registry_path", "cursor_root")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("reference slow publisher paths must be absolute")
        return value


def _capture_reference_with_quota(
    *,
    settings: ReferenceSlowSourceSettings,
    quota_store: SourceQuotaStore,
    adapter: ReferenceSlowAdapter,
    calendar: MarketCalendarAuthority,
    target_trade_date: date,
    observed_at: datetime,
    completion_clock: Callable[[], datetime],
    producer_commit: str,
    retry_ordinal: int = 0,
    transport_observer: QuotaBoundTransportObserver | None = None,
) -> ReferenceSlowSourceSnapshot | ReferenceSlowQuotaCapture:
    from rquant.reference_slow_source import capture_reference_slow_source_snapshot

    if type(retry_ordinal) is not int or retry_ordinal < 0:
        raise ValueError("retry_ordinal must be a nonnegative int")

    logical_request_id = canonical_sha256(
        {
            "protocol": "reference-source-attempt-v2",
            "source": settings.source,
            "target_trade_date": target_trade_date,
            "logical_revision": retry_ordinal,
        }
    )
    if settings.quota_accounting_mode == "transport":
        if transport_observer is None:
            raise SourceQuotaConflictError("reference transport quota observer is required")
        quota_store.recover_stale_attempts(
            source=settings.source,
            now=observed_at,
            min_age=timedelta(seconds=settings.pending_recovery_min_age_seconds),
        )
        existing_outcome = transport_observer.request_outcome(logical_request_id)
        if existing_outcome is not None:
            raise SourceQuotaConflictError(
                f"reference source attempt already exists: {existing_outcome.value}"
            )
        with transport_observer.scope(
            logical_request_id=logical_request_id,
            observed_at=observed_at,
        ):
            snapshot = capture_reference_slow_source_snapshot(
                database_path=settings.database_path,
                adapter=adapter,
                calendar=calendar,
                target_trade_date=target_trade_date,
                captured_at=observed_at,
                completion_clock=completion_clock,
                producer_commit=producer_commit,
                limits=settings.limits.model_dump(mode="python"),
            )
            receipts = transport_observer.current_receipts()
        return ReferenceSlowQuotaCapture(
            snapshot=snapshot,
            source_usage=SourceTransportUsageReceipt(
                source=settings.source,
                logical_request_id=logical_request_id,
                actual_call_count=len(receipts),
                call_receipts=receipts,
            ),
        )

    window_start = observed_at.replace(second=0, microsecond=0)
    window_reset = window_start + timedelta(minutes=1)
    window_id = window_start.strftime("%Y%m%dT%H%M")
    quota_store.declare_window(
        source=settings.source,
        window_id=window_id,
        starts_at=window_start,
        resets_at=window_reset,
        total_units=settings.quota_units_per_window,
    )
    quota_store.recover_stale_attempts(
        source=settings.source,
        now=observed_at,
        min_age=timedelta(seconds=settings.pending_recovery_min_age_seconds),
    )
    attempt_id = logical_request_id
    existing = quota_store.get_attempt(attempt_id)
    if existing is not None:
        raise SourceQuotaConflictError(
            f"reference source attempt already exists: {existing.outcome.value}"
        )
    attempt = quota_store.begin_attempt(
        source=settings.source,
        owner=f"reference-slow:{attempt_id}",
        attempt_id=attempt_id,
        units=settings.quota_cost_per_capture,
        now=observed_at,
        expires_at=min(observed_at + timedelta(seconds=30), window_reset),
    )
    quota_store.mark_dispatched(attempt.attempt_id, now=observed_at)
    try:
        result = capture_reference_slow_source_snapshot(
            database_path=settings.database_path,
            adapter=adapter,
            calendar=calendar,
            target_trade_date=target_trade_date,
            captured_at=observed_at,
            completion_clock=completion_clock,
            producer_commit=producer_commit,
            limits=settings.limits.model_dump(mode="python"),
        )
    except Exception:
        quota_store.commit_attempt(
            attempt.attempt_id,
            outcome=SourceQuotaAttemptOutcome.FAILURE,
            now=observed_at,
        )
        raise
    quota_store.commit_attempt(
        attempt.attempt_id,
        outcome=SourceQuotaAttemptOutcome.SUCCESS,
        now=observed_at,
    )
    return result


def reference_slow_source_builder(
    *,
    adapter_factory: Callable[[], ReferenceSlowAdapter],
    clock: Callable[[], datetime],
    runtime_capabilities: Mapping[str, str],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        from rquant.reference_slow_runtime import capture_reference_slow_batch

        if manifest.service_kind is not RuntimeServiceKind.REFERENCE_SLOW_SOURCE:
            raise ValueError("runtime service kind must be reference_slow_source")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("reference slow source must run on the live plane")
        settings = ReferenceSlowSourceSettings.model_validate(dict(manifest.settings))
        calendar = load_market_calendar_authority(
            settings.calendar_path,
            expected_commit=settings.calendar_expected_commit,
        )
        if calendar.content_sha256 != settings.calendar_content_sha256:
            raise ValueError("reference slow calendar content identity mismatch")
        adapter = adapter_factory()
        quota_store = SourceQuotaStore(settings.quota_path)
        transport_observer = (
            None
            if settings.quota_accounting_mode != "transport"
            else QuotaBoundTransportObserver(
                store=quota_store,
                source=settings.source,
                quota_units_per_window=settings.quota_units_per_window,
                window_kind="minute",
                clock=clock,
            )
        )
        if transport_observer is not None:
            binder = getattr(adapter, "bind_transport_observer", None)
            if not callable(binder):
                raise TypeError("reference transport adapter must bind a source quota observer")
            binder(transport_observer)
        source_key_id = runtime_capabilities.get("RQ_REFERENCE_SOURCE_SIGNING_KEY_ID", "").strip()
        private_key_base64 = runtime_capabilities.get(
            "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64", ""
        ).strip()
        source_public_key = runtime_capabilities.get("RQ_REFERENCE_SOURCE_PUBLIC_KEY", "").strip()
        if not source_key_id or not private_key_base64 or not source_public_key:
            raise ValueError("reference slow source requires its isolated signing credential")
        try:
            private_key = base64.b64decode(private_key_base64, validate=True).decode("ascii")
            source_signer = ReferenceSourceBatchSigner(
                key_id=source_key_id,
                private_key=private_key,
            )
            source_verifier = ReferenceSourceBatchVerifier(
                key_id=source_key_id,
                public_key=source_public_key,
            )
        except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
            raise ValueError("reference source signing credential is invalid") from exc
        spool = LiveBatchSpool(
            settings.spool_root,
            source_signer=source_signer,
            source_verifier=source_verifier,
        )
        cursor_reader = (
            None
            if settings.consumer_cursor_root is None
            else LiveBatchSpool(
                settings.spool_root,
                cursor_root=settings.consumer_cursor_root,
                read_only=True,
                source_verifier=source_verifier,
            )
        )

        def step() -> RuntimeStepResult:
            observed_at = clock()
            decision = decide_market_session(calendar, observed_at)

            def load_snapshot():
                captured = _capture_reference_with_quota(
                    settings=settings,
                    quota_store=quota_store,
                    adapter=adapter,
                    calendar=calendar,
                    target_trade_date=decision.local_trade_date,
                    observed_at=decision.observed_at,
                    completion_clock=clock,
                    producer_commit=manifest.producer_commit,
                    retry_ordinal=settings.retry_ordinal,
                    transport_observer=transport_observer,
                )
                return (
                    captured.snapshot
                    if isinstance(captured, ReferenceSlowQuotaCapture)
                    else captured
                )

            def load_revision(target_trade_date: date):
                captured = _capture_reference_with_quota(
                    settings=settings,
                    quota_store=quota_store,
                    adapter=adapter,
                    calendar=calendar,
                    target_trade_date=target_trade_date,
                    observed_at=decision.observed_at,
                    completion_clock=clock,
                    producer_commit=manifest.producer_commit,
                    retry_ordinal=settings.retry_ordinal + 1,
                    transport_observer=transport_observer,
                )
                return (
                    captured.snapshot
                    if isinstance(captured, ReferenceSlowQuotaCapture)
                    else captured
                )

            result = capture_reference_slow_batch(
                spool=spool,
                calendar=calendar,
                observed_at=decision.observed_at,
                producer_commit=manifest.producer_commit,
                producer_version=settings.producer_version,
                snapshot_loader=load_snapshot,
                revision_snapshot_loader=load_revision,
                revision_lookback_sessions=settings.revision_lookback_sessions,
                history_page_size=settings.history_page_size,
                completion_clock=clock,
            )
            if cursor_reader is not None:
                cursor = cursor_reader.load_cursor(
                    settings.retention_consumer_id,
                    LiveChannel.REFERENCE_SLOW,
                )
                if cursor is not None:
                    spool.retire_reference_batches_single_consumer(
                        cursor=cursor,
                        retain_hot_batches=settings.retention_hot_batches,
                        max_batches=settings.retention_page_size,
                        retired_at=clock(),
                    )
            return result

        return step

    return build


def reference_slow_publisher_builder(
    *,
    clock: Callable[[], datetime],
    runtime_capabilities: Mapping[str, str],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        from rquant.reference_data_registry import (
            ReferencePublicationAuthenticator,
            ReferenceRegistry,
        )
        from rquant.reference_slow_runtime import publish_reference_slow_batches

        if manifest.service_kind is not RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER:
            raise ValueError("runtime service kind must be reference_slow_publisher")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("reference slow publisher must run on the live plane")
        settings = ReferenceSlowPublisherSettings.model_validate(dict(manifest.settings))
        calendar = load_market_calendar_authority(
            settings.calendar_path,
            expected_commit=settings.calendar_expected_commit,
        )
        if calendar.content_sha256 != settings.calendar_content_sha256:
            raise ValueError("reference slow calendar content identity mismatch")
        key_id = runtime_capabilities.get("RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID", "").strip()
        secret_hex = runtime_capabilities.get(
            "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX", ""
        ).strip()
        if not key_id or not secret_hex:
            raise ValueError(
                "reference slow publisher requires its isolated publication credential"
            )
        try:
            authenticator = ReferencePublicationAuthenticator(
                key_id=key_id,
                secret=bytes.fromhex(secret_hex),
            )
        except ValueError as exc:
            raise ValueError("reference publication credential is invalid") from exc
        source_key_id = runtime_capabilities.get("RQ_REFERENCE_SOURCE_SIGNING_KEY_ID", "").strip()
        source_public_key = runtime_capabilities.get("RQ_REFERENCE_SOURCE_PUBLIC_KEY", "").strip()
        if not source_key_id or not source_public_key:
            raise ValueError("reference slow publisher requires the source verification key")
        try:
            source_verifier = ReferenceSourceBatchVerifier(
                key_id=source_key_id,
                public_key=source_public_key,
            )
        except ValueError as exc:
            raise ValueError("reference source verification credential is invalid") from exc
        spool = LiveBatchSpool(
            settings.spool_root,
            cursor_root=settings.cursor_root,
            source_read_only=True,
            publication_authenticator=authenticator,
            source_verifier=source_verifier,
        )
        registry = ReferenceRegistry(
            settings.registry_path,
            publication_authenticator=authenticator,
        )

        def step() -> RuntimeStepResult:
            return publish_reference_slow_batches(
                spool=spool,
                registry=registry,
                calendar=calendar,
                consumer_id=settings.consumer_id,
                observed_at=clock(),
                producer_commit=manifest.producer_commit,
                completion_clock=clock,
                page_size=settings.page_size,
            )

        return step

    return build


class AuctionMatchSourceSettings(RuntimeContractModel):
    spool_root: Path
    quota_path: Path
    quota_units_per_window: StrictInt = Field(gt=0)
    quota_cost_per_request: StrictInt = Field(default=1, gt=0)
    producer_version: str = Field(min_length=1)
    source: str = Field(default="tushare.stk_auction", min_length=1)
    dataset_id: str = Field(default="auction_match", min_length=1)
    min_coverage_ratio: float = Field(default=0.95, gt=0, le=1)
    calendar_path: Path
    calendar_expected_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    universe_path: Path
    max_attempts: StrictInt = Field(default=3, gt=0, le=10)

    @field_validator("spool_root", "quota_path", "calendar_path", "universe_path")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("runtime data paths must be absolute")
        return value


class AuctionUniversePublisherSettings(RuntimeContractModel):
    database_path: Path
    calendar_path: Path
    calendar_expected_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    authority_root: Path

    @field_validator("database_path", "calendar_path", "authority_root")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("auction universe publisher paths must be absolute")
        return value


def auction_universe_publisher_builder(
    *,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER:
            raise ValueError("runtime service kind must be auction_universe_publisher")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("auction universe publisher must run on the live plane")
        settings = AuctionUniversePublisherSettings.model_validate(dict(manifest.settings))
        calendar = load_market_calendar_authority(
            settings.calendar_path,
            expected_commit=settings.calendar_expected_commit,
        )
        if calendar.content_sha256 != settings.calendar_content_sha256:
            raise ValueError("auction universe calendar content identity mismatch")

        def step() -> RuntimeStepResult:
            observed_at = clock()
            calendar_evidence = {"market_calendar": calendar.content_sha256}
            try:
                effective_trade_date, _reference_trade_date = auction_universe_publication_dates(
                    calendar, observed_at
                )
            except AuctionUniverseSourceError as exc:
                if "protection window" not in str(exc):
                    raise
                return RuntimeStepResult(source_generations=calendar_evidence)

            current_path = settings.authority_root / "current.json"
            try:
                current = load_auction_universe_authority(
                    current_path,
                    expected_commit=manifest.producer_commit,
                    required_trade_date=effective_trade_date,
                    as_of=observed_at,
                )
            except AuctionUniverseAuthorityIntegrityError:
                current = None
            if current is not None:
                return RuntimeStepResult(
                    source_generations={
                        **calendar_evidence,
                        "daily_bar": current.source_snapshot_id,
                        "auction_universe": current.content_sha256,
                    }
                )

            receipt = publish_auction_universe_from_daily_snapshot(
                database_path=settings.database_path,
                authority_root=settings.authority_root,
                calendar=calendar,
                observed_at=observed_at,
                producer_commit=manifest.producer_commit,
            )
            return RuntimeStepResult(
                processed_count=receipt.code_count if receipt.published else 0,
                source_generations={
                    **calendar_evidence,
                    "daily_bar": receipt.source_snapshot_id,
                    "auction_universe": receipt.content_sha256,
                },
            )

        return step

    return build


def auction_match_source_builder(
    *,
    adapter_factory: Callable[[], AuctionMatchAdapter],
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.AUCTION_MATCH_SOURCE:
            raise ValueError("runtime service kind must be auction_match_source")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("auction-match source must run on the live plane")
        settings = AuctionMatchSourceSettings.model_validate(dict(manifest.settings))
        calendar = load_market_calendar_authority(
            settings.calendar_path,
            expected_commit=settings.calendar_expected_commit,
        )
        if calendar.content_sha256 != settings.calendar_content_sha256:
            raise ValueError("auction match calendar content identity mismatch")
        adapter = adapter_factory()
        gateway = AuctionMatchGateway(
            spool=LiveBatchSpool(settings.spool_root),
            fetcher=adapter.stk_auction,
            config=AuctionMatchGatewayConfig(
                source=settings.source,
                dataset_id=settings.dataset_id,
                producer_version=settings.producer_version,
                producer_commit=manifest.producer_commit,
                min_coverage_ratio=settings.min_coverage_ratio,
                quota_units_per_window=settings.quota_units_per_window,
                quota_cost_per_request=settings.quota_cost_per_request,
            ),
            quota_store=SourceQuotaStore(settings.quota_path),
            dispatch_clock=clock,
        )
        last_result = RuntimeStepResult()
        attempt_trade_date: date | None = None
        attempts = 0
        completed = False

        def step() -> RuntimeStepResult:
            nonlocal attempt_trade_date, attempts, completed, last_result
            observed_at = clock()
            decision = decide_market_session(calendar, observed_at)
            evidence = {"market_calendar": calendar.content_sha256}
            if attempt_trade_date != decision.local_trade_date:
                attempt_trade_date = decision.local_trade_date
                attempts = 0
                completed = False
                last_result = RuntimeStepResult(source_generations=evidence)
            local_time = decision.observed_at.astimezone(_SHANGHAI).timetz().replace(tzinfo=None)
            if (
                not decision.is_open_date
                or local_time < time(9, 26)
                or local_time > time(9, 30)
                or completed
                or attempts >= settings.max_attempts
            ):
                return RuntimeStepResult(
                    **{
                        **last_result.model_dump(mode="python"),
                        "processed_count": 0,
                        "source_generations": {
                            **dict(last_result.source_generations),
                            **evidence,
                        },
                    }
                )
            universe = load_auction_universe_authority(
                settings.universe_path,
                expected_commit=manifest.producer_commit,
                required_trade_date=decision.local_trade_date,
                as_of=observed_at,
            )
            retry_ordinal = attempts  # Retry ordinals are zero-based: 0, 1, ... max_attempts - 1.
            attempts += 1
            capture = gateway.capture_once(
                trade_date=decision.local_trade_date,
                received_at=observed_at,
                expected_codes=universe.codes,
                retry_ordinal=retry_ordinal,
            )
            result = capture_auction_match_step(gateway, capture=capture)
            records = gateway.spool.list_after(
                LiveChannel.AUCTION_MATCH,
                sequence=capture.pointer.sequence - 1,
            )
            if len(records) != 1:
                raise RuntimeError("auction-match current batch cannot be resolved")
            completed = records[0].envelope.quality_status is BatchQualityStatus.PUBLISHED
            last_result = RuntimeStepResult(
                **{
                    **result.model_dump(mode="python"),
                    "source_generations": {
                        **dict(result.source_generations),
                        **evidence,
                        "auction_universe": universe.content_sha256,
                    },
                }
            )
            return last_result

        return step

    return build


class MarketMinuteSourceSettings(RuntimeContractModel):
    spool_root: Path
    quota_path: Path
    quota_units_per_window: StrictInt = Field(gt=0)
    quota_cost_per_request: StrictInt = Field(default=1, gt=0)
    pending_recovery_min_age_seconds: StrictInt = Field(default=60, ge=30)
    max_codes_per_source_call: StrictInt = Field(default=300, gt=0, le=300)
    producer_version: str = Field(min_length=1)
    source: str = Field(default="tushare.rt_min", min_length=1)
    dataset_id: str = Field(default="market_minute", min_length=1)
    calendar_path: Path | None = None
    calendar_expected_commit: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{40}$",
    )
    calendar_content_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
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
            if (
                settings.calendar_path is None
                or settings.calendar_expected_commit is None
                or settings.calendar_content_sha256 is None
                or not settings.candidate_authorities
            ):
                raise ValueError(
                    "default market-minute source requires calendar_path and candidate_authorities"
                )
            calendar = load_market_calendar_authority(
                settings.calendar_path,
                expected_commit=settings.calendar_expected_commit,
            )
            if calendar.content_sha256 != settings.calendar_content_sha256:
                raise ValueError("market-minute calendar content identity mismatch")
            candidate_loader = RuntimeCandidateUniverseLoader(
                RuntimeCandidateUniverseConfig(
                    expected_commit=manifest.producer_commit,
                    authorities=settings.candidate_authorities,
                )
            )
        else:
            if (
                settings.calendar_path is not None
                or settings.calendar_expected_commit is not None
                or settings.calendar_content_sha256 is not None
                or settings.candidate_authorities
            ):
                raise ValueError(
                    "explicit universe_loader cannot be combined with manifest authorities"
                )
            calendar = None
            candidate_loader = None
        universe: tuple[str, ...] = ()
        adapter = adapter_factory()
        spool = LiveBatchSpool(settings.spool_root)
        quota_store = SourceQuotaStore(settings.quota_path)
        transport_observer = QuotaBoundTransportObserver(
            store=quota_store,
            source=settings.source,
            quota_units_per_window=settings.quota_units_per_window,
            window_kind="minute",
            clock=clock,
        )

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
                frame = transport_observer.observe(
                    "rt_min",
                    lambda batch=batch: adapter.rt_min(list(batch), freq="1min"),
                )
                if not isinstance(frame, pd.DataFrame):
                    raise TypeError("market-minute adapter must return a DataFrame")
                frames.append(frame)
            if len(frames) == 1:
                return frames[0]
            return pd.concat(frames, ignore_index=True)

        gateway = MarketMinuteGateway(
            spool=spool,
            fetcher=fetch_current_universe,
            completion_clock=clock,
            config=MarketMinuteGatewayConfig(
                source=settings.source,
                dataset_id=settings.dataset_id,
                producer_version=settings.producer_version,
                producer_commit=manifest.producer_commit,
                quota_units_per_window=settings.quota_units_per_window,
                quota_cost_per_request=settings.quota_cost_per_request,
                pending_recovery_min_age_seconds=(settings.pending_recovery_min_age_seconds),
            ),
            quota_store=quota_store,
            transport_observer=transport_observer,
        )

        def capture_current_universe(observed_at: datetime) -> RuntimeStepResult:
            call_count = source_call_count()
            return capture_market_minute_step(
                gateway,
                received_at=observed_at,
                quota_cost_units=call_count,
            )

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


class WatchlistQuoteSourceSettings(RuntimeContractModel):
    spool_root: Path
    quota_path: Path
    quota_units_per_window: StrictInt = Field(gt=0)
    quota_cost_per_request: StrictInt = Field(default=1, gt=0)
    producer_version: str = Field(min_length=1)
    source: str = Field(default="akshare.stock_zh_a_spot", min_length=1)
    dataset_id: str = Field(default="watchlist_quote", min_length=1)
    schema_version: StrictInt = Field(default=2, ge=2)
    rollout_mode: Literal["candidate", "published"] = "candidate"
    minimum_cadence_seconds: float = Field(default=5.0, gt=0, le=60)
    request_timeout_seconds: float = Field(default=2.5, gt=0, le=30)
    failure_threshold: StrictInt = Field(default=3, ge=1, le=20)
    circuit_cooldown_seconds: float = Field(default=30, gt=0, le=900)
    max_backoff_seconds: float = Field(default=60, gt=0, le=900)
    calendar_path: Path | None = None
    calendar_expected_commit: str | None = Field(default=None, pattern=r"^[0-9a-f]{40}$")
    calendar_content_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    candidate_authorities: tuple[CandidateUniverseAuthority, ...] = ()

    @field_validator("spool_root", "quota_path", "calendar_path")
    @classmethod
    def require_absolute_path(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("watchlist quote runtime paths must be absolute")
        return value


def _watchlist_quote_session_active(*, scheduled_at: datetime) -> bool:
    local = scheduled_at.astimezone(_SHANGHAI)
    local_time = local.timetz().replace(tzinfo=None)
    return time(9, 25) <= local_time <= time(11, 30) or time(13, 0) <= local_time <= time(15, 0)


def watchlist_quote_source_builder(
    *,
    provider_factory: Callable[[], WatchlistQuoteProvider],
    universe_loader: Callable[[], Iterable[str]] | None,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE:
            raise ValueError("runtime service kind must be watchlist_quote_source")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("watchlist quote source must run on the live plane")
        settings = WatchlistQuoteSourceSettings.model_validate(dict(manifest.settings))
        authoritative_universe = universe_loader is None
        if authoritative_universe:
            if (
                settings.calendar_path is None
                or settings.calendar_expected_commit is None
                or settings.calendar_content_sha256 is None
                or not settings.candidate_authorities
            ):
                raise ValueError(
                    "default watchlist quote source requires calendar_path and "
                    "candidate_authorities"
                )
            calendar = load_market_calendar_authority(
                settings.calendar_path,
                expected_commit=settings.calendar_expected_commit,
            )
            if calendar.content_sha256 != settings.calendar_content_sha256:
                raise ValueError("watchlist quote calendar content identity mismatch")
            candidate_loader = RuntimeCandidateUniverseLoader(
                RuntimeCandidateUniverseConfig(
                    expected_commit=manifest.producer_commit,
                    authorities=settings.candidate_authorities,
                )
            )
        else:
            if (
                settings.calendar_path is not None
                or settings.calendar_expected_commit is not None
                or settings.calendar_content_sha256 is not None
                or settings.candidate_authorities
            ):
                raise ValueError(
                    "explicit universe_loader cannot be combined with manifest authorities"
                )
            calendar = None
            candidate_loader = None
        gateway = WatchlistQuoteGateway(
            spool=LiveBatchSpool(settings.spool_root),
            provider=provider_factory(),
            config=WatchlistQuoteGatewayConfig(
                source=settings.source,
                dataset_id=settings.dataset_id,
                producer_version=settings.producer_version,
                producer_commit=manifest.producer_commit,
                schema_version=settings.schema_version,
                rollout_mode=settings.rollout_mode,
                minimum_cadence_seconds=settings.minimum_cadence_seconds,
                request_timeout_seconds=settings.request_timeout_seconds,
                failure_threshold=settings.failure_threshold,
                circuit_cooldown_seconds=settings.circuit_cooldown_seconds,
                max_backoff_seconds=settings.max_backoff_seconds,
                quota_units_per_window=settings.quota_units_per_window,
                quota_cost_per_request=settings.quota_cost_per_request,
            ),
            quota_store=SourceQuotaStore(settings.quota_path),
            clock=clock,
        )
        last_result = RuntimeStepResult()

        def step() -> RuntimeStepResult:
            nonlocal last_result
            scheduled_at = clock()
            evidence: dict[str, str] = {}
            if candidate_loader is not None and calendar is not None:
                decision = decide_market_session(calendar, scheduled_at)
                evidence["market_calendar"] = calendar.content_sha256
                if not decision.is_open_date or not _watchlist_quote_session_active(
                    scheduled_at=scheduled_at
                ):
                    return RuntimeStepResult(
                        **{
                            **last_result.model_dump(mode="python"),
                            "source_generations": {
                                **dict(last_result.source_generations),
                                **evidence,
                            },
                        }
                    )
                candidate_result = candidate_loader.load(
                    as_of=scheduled_at,
                    required_trade_date=decision.local_trade_date,
                )
                codes = _load_universe(lambda: candidate_result.codes)
                universe_as_of = candidate_result.as_of
                evidence["candidate_universe"] = candidate_result.content_fingerprint
                trade_date = decision.local_trade_date
            else:
                if universe_loader is None:
                    raise RuntimeError("watchlist quote universe loader is unavailable")
                codes = _load_universe(universe_loader)
                universe_as_of = scheduled_at
                trade_date = scheduled_at.astimezone(_SHANGHAI).date()
            result = capture_watchlist_quote_step(
                gateway,
                codes=codes,
                scheduled_at=scheduled_at,
                universe_as_of=universe_as_of,
                trade_date=trade_date,
            )
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


def _default_adapter_factory(
    runtime_capabilities: Mapping[str, str],
) -> MarketMinuteAdapter:
    from rquant.adapter.tushare import TushareAdapter

    token = runtime_capabilities.get("TUSHARE_TOKEN_MAIN", "").strip()
    if not token:
        raise RuntimeError("TUSHARE_TOKEN_MAIN capability is required")
    backup_token = runtime_capabilities.get("TUSHARE_TOKEN_BACKUP", "").strip()
    return TushareAdapter(token=token, backup_token=backup_token)


def _default_watchlist_quote_provider_factory() -> WatchlistQuoteProvider:
    from rquant.watchlist_quote_provider import AkshareSinaWatchlistQuoteProvider

    return AkshareSinaWatchlistQuoteProvider()


def build_builtin_registry(
    *,
    runtime_capabilities: Mapping[str, str] | None = None,
    reference_adapter_factory: Callable[[], ReferenceSlowAdapter] | None = None,
    auction_adapter_factory: Callable[[], AuctionMatchAdapter] | None = None,
    adapter_factory: Callable[[], MarketMinuteAdapter] | None = None,
    watchlist_quote_provider_factory: Callable[[], WatchlistQuoteProvider] | None = None,
    universe_loader: Callable[[], Iterable[str]] | None = None,
    clock: Callable[[], datetime] | None = None,
    evaluator_loader: StrategyEvaluatorLoader | None = None,
    signal_source_loader: SignalSourceLoader | None = None,
    target_resolver: TargetResolver | None = None,
    provider_loader: ProviderLoader | None = None,
    paper_quote_resolver: QuoteResolver | None = None,
    trade_date_resolver: TradeDateResolver | None = None,
    serving_snapshot_loader: ServingSnapshotLoader | None = None,
    daily_close_fetcher: Callable[[object], object] | None = None,
    shadow_input_loader: object | None = None,
    shadow_session_executor: Callable[..., object] | None = None,
    candidate_input_loader: CandidateInputLoader | None = None,
    auction_candidate_input_loader: AuctionCandidateInputLoader | None = None,
    artifact_retention_schema_resolver: Callable[[int], str] | None = None,
    artifact_terminal_lifecycle_factory: (
        Callable[[], ProductionArtifactTerminalLifecycle] | None
    ) = None,
    completion_attestation_signer: CompletionAttestationSigner | None = None,
    completion_attestation_active_key_id: str | None = None,
) -> RuntimeServiceRegistry:
    from rquant.runtime_builder_authority import (
        lab_jobs_publisher_builder,
        paper_execution_constraint_publisher_builder,
        promotions_publisher_builder,
        runtime_health_publisher_builder,
    )
    from rquant.runtime_builder_candidate import candidate_publisher_builder
    from rquant.runtime_builder_daily import daily_close_source_builder
    from rquant.runtime_builder_daily_orchestrator import (
        daily_pipeline_orchestrator_builder,
    )
    from rquant.runtime_builder_feature import feature_live_builder
    from rquant.runtime_builder_paper import paper_broker_builder
    from rquant.runtime_builder_shadow import shadow_session_builder
    from rquant.runtime_builder_strategy import strategy_live_builder

    resolved_clock = clock or (lambda: datetime.now(UTC))
    capabilities = runtime_capabilities or {}
    if (signal_source_loader is None) != (target_resolver is None):
        raise ValueError("signal router dependencies must be provided together")
    if (paper_quote_resolver is None) != (trade_date_resolver is None):
        raise ValueError("paper broker dependencies must be provided together")
    if (completion_attestation_signer is None) != (completion_attestation_active_key_id is None):
        raise ValueError("completion attestation signer and active key id must be paired")
    registry = RuntimeServiceRegistry(
        artifact_terminal_lifecycle_factory=artifact_terminal_lifecycle_factory,
    )
    registry.register(
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        reference_slow_source_builder(
            adapter_factory=reference_adapter_factory
            or (lambda: _default_adapter_factory(capabilities)),
            clock=resolved_clock,
            runtime_capabilities=capabilities,
        ),
    )
    registry.register(
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        reference_slow_publisher_builder(
            clock=resolved_clock,
            runtime_capabilities=capabilities,
        ),
    )
    registry.register(
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
        auction_universe_publisher_builder(clock=resolved_clock),
    )
    registry.register(
        RuntimeServiceKind.AUCTION_MATCH_SOURCE,
        auction_match_source_builder(
            adapter_factory=auction_adapter_factory
            or (lambda: _default_adapter_factory(capabilities)),
            clock=resolved_clock,
        ),
    )
    registry.register(
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        market_minute_source_builder(
            adapter_factory=adapter_factory or (lambda: _default_adapter_factory(capabilities)),
            universe_loader=universe_loader,
            clock=resolved_clock,
        ),
    )
    registry.register(
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        watchlist_quote_source_builder(
            provider_factory=(
                watchlist_quote_provider_factory or _default_watchlist_quote_provider_factory
            ),
            universe_loader=universe_loader,
            clock=resolved_clock,
        ),
    )
    registry.register(
        RuntimeServiceKind.DAILY_CLOSE_SOURCE,
        daily_close_source_builder(
            runtime_capabilities=capabilities,
            clock=resolved_clock,
            fetcher=daily_close_fetcher,
        ),
    )
    registry.register(
        RuntimeServiceKind.SHADOW_SESSION,
        shadow_session_builder(
            clock=resolved_clock,
            input_loader=shadow_input_loader,  # type: ignore[arg-type]
            session_executor=shadow_session_executor,  # type: ignore[arg-type]
        ),
    )
    registry.register(
        RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
        daily_pipeline_orchestrator_builder(clock=resolved_clock),
    )
    registry.register(
        RuntimeServiceKind.CANDIDATE_PUBLISHER,
        candidate_publisher_builder(
            candidate_input_loader=candidate_input_loader,
            auction_input_loader=auction_candidate_input_loader,
            clock=resolved_clock,
        ),
    )
    registry.register(
        RuntimeServiceKind.FEATURE_LIVE,
        feature_live_builder(clock=resolved_clock),
    )
    strategy_builder_kwargs: dict[str, object] = {
        "evaluator_loader": evaluator_loader,
        "clock": resolved_clock,
    }
    if completion_attestation_signer is not None:
        strategy_builder_kwargs["completion_attestation_signer"] = completion_attestation_signer
        strategy_builder_kwargs["completion_attestation_active_key_id"] = (
            completion_attestation_active_key_id
        )
    registry.register(
        RuntimeServiceKind.STRATEGY_LIVE,
        strategy_live_builder(**strategy_builder_kwargs),  # type: ignore[arg-type]
    )

    def build_signal_router(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        from rquant.runtime_builder_signal import signal_router_builder

        return signal_router_builder(
            source_loader=signal_source_loader,
            target_resolver=target_resolver,
            clock=resolved_clock,
        )(manifest)

    registry.register(RuntimeServiceKind.SIGNAL_ROUTER, build_signal_router)

    def build_notifier(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        from rquant.runtime_builder_signal import notifier_builder

        return notifier_builder(
            provider_loader=provider_loader,
            capability_environment=capabilities,
            clock=resolved_clock,
        )(manifest)

    registry.register(RuntimeServiceKind.NOTIFIER, build_notifier)
    registry.register(
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        paper_execution_constraint_publisher_builder(clock=resolved_clock),
    )
    registry.register(
        RuntimeServiceKind.PAPER_BROKER,
        paper_broker_builder(
            clock=resolved_clock,
            quote_resolver=paper_quote_resolver,
            trade_date_resolver=trade_date_resolver,
        ),
    )
    registry.register(
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        runtime_health_publisher_builder(clock=resolved_clock),
    )
    registry.register(
        RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        lab_jobs_publisher_builder(
            clock=resolved_clock,
            open_artifact_terminal_lifecycle=registry.open_artifact_terminal_lifecycle,
        ),
    )

    def build_artifact_catalog(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        from rquant.runtime_builder_artifact_catalog import artifact_catalog_builder

        return artifact_catalog_builder(
            clock=resolved_clock,
            open_artifact_terminal_lifecycle=registry.open_artifact_terminal_lifecycle,
        )(manifest)

    registry.register(RuntimeServiceKind.LAB_ARTIFACT_CATALOG, build_artifact_catalog)

    def build_artifact_retention(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        from rquant.runtime_builder_retention import artifact_retention_builder

        return artifact_retention_builder(
            clock=resolved_clock,
            schema_resolver=artifact_retention_schema_resolver,
            capability_environment=runtime_capabilities,
            open_artifact_terminal_lifecycle=registry.open_artifact_terminal_lifecycle,
        )(manifest)

    registry.register(RuntimeServiceKind.ARTIFACT_RETENTION, build_artifact_retention)
    registry.register(
        RuntimeServiceKind.PROMOTIONS_PUBLISHER,
        promotions_publisher_builder(
            clock=resolved_clock,
            open_artifact_terminal_lifecycle=registry.open_artifact_terminal_lifecycle,
        ),
    )

    def build_serving(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        from rquant.runtime_builder_serving import serving_publisher_builder

        return serving_publisher_builder(
            snapshot_loader=serving_snapshot_loader,
            clock=resolved_clock,
        )(manifest)

    registry.register(RuntimeServiceKind.SERVING_PUBLISHER, build_serving)
    return registry


__all__ = [
    "AuctionMatchAdapter",
    "AuctionMatchSourceSettings",
    "AuctionUniversePublisherSettings",
    "MarketMinuteAdapter",
    "MarketMinuteSourceSettings",
    "ReferenceSlowPublisherSettings",
    "ReferenceSlowSourceSettings",
    "auction_match_source_builder",
    "auction_universe_publisher_builder",
    "build_builtin_registry",
    "market_minute_source_builder",
    "watchlist_quote_source_builder",
    "reference_slow_publisher_builder",
    "reference_slow_source_builder",
]
