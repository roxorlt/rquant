"""Independent low-latency watchlist quote source gateway.

The gateway owns one provider request, quota ledger, circuit state, and immutable
spool.  It deliberately has no feature, strategy, notification, or DuckDB path.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import stat
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from typing import Annotated, Literal, Protocol
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, StringConstraints, model_validator

from rquant.live_contracts import (
    BatchEnvelope,
    BatchPointer,
    BatchQualityStatus,
    CurrentPointer,
    LiveChannel,
)
from rquant.live_spool import LiveBatchRecord, LiveBatchSpool
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.source_quota_store import SourceQuotaExhaustedError, SourceQuotaStore

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_COLUMNS = (
    "ts_code",
    "observed_at",
    "trade_date",
    "price",
    "open",
    "high",
    "low",
    "volume",
    "amount",
    "source",
    "scheduled_at",
    "universe_as_of",
    "requested_at",
    "response_received_at",
    "fetched_at",
    "source_timestamp_provenance",
    "producer_commit",
    "schema_version",
)


class WatchlistQuoteValidationError(ValueError):
    """A provider response cannot be represented as a PIT quote observation."""


class WatchlistQuoteStateError(RuntimeError):
    """Persistent retry state is incomplete or invalid and must not be ignored."""


class WatchlistQuoteGatewayConfig(RuntimeContractModel):
    source: str = Field(default="akshare.stock_zh_a_spot", min_length=1)
    dataset_id: str = Field(default="watchlist_quote", min_length=1)
    producer_version: str = Field(min_length=1)
    producer_commit: CommitSha
    schema_version: int = Field(default=2, ge=2)
    rollout_mode: Literal["candidate", "published"] = "candidate"
    minimum_cadence_seconds: float = Field(default=5.0, gt=0, le=60)
    request_timeout_seconds: float = Field(default=2.5, gt=0, le=30)
    failure_threshold: int = Field(default=3, ge=1, le=20)
    circuit_cooldown_seconds: float = Field(default=30, gt=0, le=900)
    max_backoff_seconds: float = Field(default=60, gt=0, le=900)
    quota_units_per_window: int | None = Field(default=None, gt=0)
    quota_cost_per_request: int = Field(default=1, gt=0)


class WatchlistQuoteCapture(RuntimeContractModel):
    pointer: CurrentPointer | BatchPointer
    published: bool


class _LegacyGatewayState(RuntimeContractModel):
    schema_version: Literal[1, 2] = 2
    consecutive_failures: int = Field(default=0, ge=0)
    retry_not_before: AwareUtcDatetime | None = None
    circuit_open_until: AwareUtcDatetime | None = None
    last_attempt_at: AwareUtcDatetime | None = None
    inflight_request_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_inflight_attempt(self) -> _LegacyGatewayState:
        if self.inflight_request_id is not None and self.last_attempt_at is None:
            raise ValueError("inflight request requires last_attempt_at")
        return self


class _GatewayState(RuntimeContractModel):
    schema_version: Literal[3] = 3
    consecutive_failures: int = Field(default=0, ge=0)
    retry_not_before: AwareUtcDatetime | None = None
    circuit_open_until: AwareUtcDatetime | None = None
    admitted_at: AwareUtcDatetime | None = None
    last_dispatch_at: AwareUtcDatetime | None = None
    inflight_request_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_inflight_admission(self) -> _GatewayState:
        if self.inflight_request_id is not None and self.admitted_at is None:
            raise ValueError("inflight request requires admitted_at")
        return self


class WatchlistQuoteProvider(Protocol):
    def __call__(
        self,
        codes: tuple[str, ...],
        *,
        timeout_seconds: float,
        on_started: Callable[[datetime], None],
    ) -> pd.DataFrame: ...


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": pd.Series(dtype="string"),
            "observed_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "trade_date": pd.Series(dtype="object"),
            "price": pd.Series(dtype="float64"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
            "amount": pd.Series(dtype="float64"),
            "source": pd.Series(dtype="string"),
            "scheduled_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "universe_as_of": pd.Series(dtype="datetime64[ns, UTC]"),
            "requested_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "response_received_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "fetched_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "source_timestamp_provenance": pd.Series(dtype="string"),
            "producer_commit": pd.Series(dtype="string"),
            "schema_version": pd.Series(dtype="int64"),
        }
    ).loc[:, _COLUMNS]


def encode_watchlist_quote_payload(frame: pd.DataFrame) -> bytes:
    output = BytesIO()
    frame.loc[:, _COLUMNS].to_parquet(output, index=False)
    return output.getvalue()


def decode_watchlist_quote_payload(payload: bytes) -> pd.DataFrame:
    frame = pd.read_parquet(BytesIO(payload))
    missing = sorted(set(_COLUMNS) - set(frame.columns))
    if missing:
        raise WatchlistQuoteValidationError(f"quote payload missing columns: {missing}")
    frame["ts_code"] = frame["ts_code"].astype("string")
    frame["observed_at"] = pd.to_datetime(frame["observed_at"], utc=True)
    frame["scheduled_at"] = pd.to_datetime(frame["scheduled_at"], utc=True)
    frame["universe_as_of"] = pd.to_datetime(frame["universe_as_of"], utc=True)
    frame["requested_at"] = pd.to_datetime(frame["requested_at"], utc=True)
    frame["response_received_at"] = pd.to_datetime(frame["response_received_at"], utc=True)
    frame["fetched_at"] = pd.to_datetime(frame["fetched_at"], utc=True)
    frame["source"] = frame["source"].astype("string")
    frame["source_timestamp_provenance"] = frame["source_timestamp_provenance"].astype("string")
    frame["producer_commit"] = frame["producer_commit"].astype("string")
    frame["schema_version"] = frame["schema_version"].astype("int64")
    return frame.loc[:, _COLUMNS]


class WatchlistQuoteGateway:
    """Single-writer gateway for true quote observations from the watchlist only."""

    def __init__(
        self,
        *,
        spool: LiveBatchSpool,
        provider: WatchlistQuoteProvider,
        config: WatchlistQuoteGatewayConfig,
        quota_store: SourceQuotaStore | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if config.quota_units_per_window is not None and quota_store is None:
            raise ValueError("quota_store is required when quota governance is enabled")
        self.spool = spool
        self._provider = provider
        self.config = config
        self._quota_store = quota_store
        self._clock = clock or (lambda: datetime.now(UTC))
        self._state_path = self.spool.root / "watchlist-quote-state.json"
        self._capture_lock_path = self.spool.root / ".watchlist-quote-capture.lock"

    def capture_once(
        self,
        *,
        codes: tuple[str, ...],
        scheduled_at: datetime,
        universe_as_of: datetime,
        trade_date: date,
    ) -> WatchlistQuoteCapture:
        scheduled = normalize_aware_utc(scheduled_at)
        universe = normalize_aware_utc(universe_as_of)
        normalized_codes = self._normalize_codes(codes)
        if universe > scheduled:
            raise WatchlistQuoteValidationError("universe_as_of cannot follow scheduled_at")
        if scheduled.astimezone(_SHANGHAI).date() != trade_date:
            raise WatchlistQuoteValidationError("trade_date conflicts with scheduled_at")
        with self._capture_lock():
            return self._capture_once_locked(
                codes=normalized_codes,
                scheduled_at=scheduled,
                universe_as_of=universe,
                trade_date=trade_date,
            )

    def _capture_once_locked(
        self,
        *,
        codes: tuple[str, ...],
        scheduled_at: datetime,
        universe_as_of: datetime,
        trade_date: date,
    ) -> WatchlistQuoteCapture:
        state = self._read_state()
        request_id = canonical_sha256(
            {
                "source": self.config.source,
                "codes": codes,
                "scheduled_at": scheduled_at,
                "universe_as_of": universe_as_of,
                "trade_date": trade_date,
                "schema_version": self.config.schema_version,
            }
        )
        had_inflight = state.inflight_request_id is not None
        if not had_inflight:
            existing = self._find_request(request_id)
            if existing is not None:
                return WatchlistQuoteCapture(
                    pointer=self._pointer_for(existing),
                    published=False,
                )
        gate_at = normalize_aware_utc(self._clock())
        if gate_at < scheduled_at:
            raise WatchlistQuoteValidationError("gateway request time precedes scheduled_at")
        if had_inflight:
            state = self._recover_inflight(state, recovered_at=gate_at)
            existing = self._find_request(request_id)
            if existing is not None:
                return WatchlistQuoteCapture(
                    pointer=self._pointer_for(existing),
                    published=False,
                )

        quality = (
            BatchQualityStatus.CANDIDATE
            if self.config.rollout_mode == "candidate"
            else BatchQualityStatus.PUBLISHED
        )
        reasons: tuple[str, ...] = ()
        frame = _empty_frame()
        raw: pd.DataFrame | None = None
        provider_failure = False
        dispatch_admitted = False
        actual_requested_at: datetime | None = None
        requested_at = gate_at
        response_received_at = gate_at
        try:
            if state.circuit_open_until is not None and gate_at < state.circuit_open_until:
                quality = BatchQualityStatus.STALE
                reasons = ("circuit_open",)
            elif state.retry_not_before is not None and gate_at < state.retry_not_before:
                quality = BatchQualityStatus.STALE
                reasons = ("backoff_active",)
            elif (
                state.last_dispatch_at is not None
                and gate_at
                < state.last_dispatch_at + timedelta(seconds=self.config.minimum_cadence_seconds)
            ):
                quality = BatchQualityStatus.STALE
                reasons = ("cadence_active",)
            else:
                admitted_at = normalize_aware_utc(self._clock())
                if admitted_at < gate_at:
                    raise WatchlistQuoteValidationError(
                        "gateway admission time precedes cadence gate"
                    )
                state = _GatewayState(
                    consecutive_failures=state.consecutive_failures,
                    retry_not_before=state.retry_not_before,
                    circuit_open_until=state.circuit_open_until,
                    admitted_at=admitted_at,
                    last_dispatch_at=state.last_dispatch_at,
                    inflight_request_id=request_id,
                )
                self._write_state(state)
                dispatch_admitted = True
                self._consume_quota_before_dispatch(
                    admitted_at=admitted_at,
                    request_id=request_id,
                )

                def record_provider_started(value: datetime) -> None:
                    nonlocal actual_requested_at, requested_at, state
                    started_at = normalize_aware_utc(value)
                    if started_at < admitted_at:
                        raise WatchlistQuoteValidationError(
                            "provider start time precedes admission"
                        )
                    if actual_requested_at is not None:
                        raise WatchlistQuoteValidationError(
                            "provider reported more than one start time"
                        )
                    started_state = _GatewayState(
                        consecutive_failures=state.consecutive_failures,
                        retry_not_before=state.retry_not_before,
                        circuit_open_until=state.circuit_open_until,
                        admitted_at=state.admitted_at,
                        last_dispatch_at=started_at,
                        inflight_request_id=state.inflight_request_id,
                    )
                    self._write_state(started_state)
                    state = started_state
                    requested_at = started_at
                    actual_requested_at = started_at

                try:
                    raw = self._provider(
                        codes,
                        timeout_seconds=self.config.request_timeout_seconds,
                        on_started=record_provider_started,
                    )
                    if actual_requested_at is None:
                        raise RuntimeError(
                            "watchlist quote provider returned without started handshake"
                        )
                finally:
                    response_received_at = normalize_aware_utc(self._clock())
        except WatchlistQuoteValidationError:
            raise
        except SourceQuotaExhaustedError:
            quality = BatchQualityStatus.STALE
            reasons = ("quota_exhausted",)
            state = self._clear_inflight(state)
        except TimeoutError:
            quality = BatchQualityStatus.STALE
            reasons = ("provider_timeout",)
            provider_failure = True
        except Exception as exc:
            quality = BatchQualityStatus.STALE
            reasons = (f"provider_error:{type(exc).__name__}",)
            provider_failure = True

        if response_received_at < requested_at:
            raise WatchlistQuoteValidationError("quote response time precedes request time")
        if raw is not None:
            if raw.empty:
                quality = BatchQualityStatus.STALE
                reasons = ("provider_empty",)
                provider_failure = True
            else:
                try:
                    frame = self._normalize_frame(
                        raw,
                        codes=codes,
                        response_received_at=response_received_at,
                        trade_date=trade_date,
                    )
                except WatchlistQuoteValidationError:
                    self._write_state(self._failed_state(state, response_received_at))
                    raise
                missing_codes = set(codes) - set(frame["ts_code"])
                if missing_codes:
                    quality = BatchQualityStatus.DEGRADED
                    reasons = ("partial_watchlist_response",)
        if provider_failure:
            state = self._failed_state(
                state,
                response_received_at,
                conservative_dispatch_at=(
                    response_received_at if actual_requested_at is None else None
                ),
            )
            self._write_state(state)
        elif dispatch_admitted and raw is None:
            state = self._clear_inflight(state)
            self._write_state(state)
        if frame.empty:
            event_start = event_end = response_received_at
            source_time = response_received_at
        else:
            event_start = frame["observed_at"].min().to_pydatetime()
            event_end = frame["observed_at"].max().to_pydatetime()
            source_time = event_end
        quality, reasons = self._classify_lateness(
            quality=quality,
            reasons=reasons,
            event_end=event_end,
        )
        frame = self._attach_provenance(
            frame,
            scheduled_at=scheduled_at,
            universe_as_of=universe_as_of,
            requested_at=requested_at,
            response_received_at=response_received_at,
            trade_date=trade_date,
        )
        payload = encode_watchlist_quote_payload(frame)
        content_sha256 = hashlib.sha256(payload).hexdigest()
        latest_by_window, latest = self._revision_index()
        event_window = (event_start, event_end)
        previous = latest_by_window.get(event_window)
        sequence = 0 if latest is None else latest.sequence + 1
        revision = 1 if previous is None else previous.revision + 1
        envelope = BatchEnvelope(
            schema_version=self.config.schema_version,
            channel=LiveChannel.WATCHLIST_QUOTE,
            dataset_id=self.config.dataset_id,
            source=self.config.source,
            source_request_id=request_id,
            batch_id=canonical_sha256(
                {
                    "channel": LiveChannel.WATCHLIST_QUOTE,
                    "sequence": sequence,
                    "revision": revision,
                    "event_time_start": event_start,
                    "event_time_end": event_end,
                    "content_sha256": content_sha256,
                }
            ),
            sequence=sequence,
            revision=revision,
            revises_batch_id=None if previous is None else previous.batch_id,
            event_time_start=event_start,
            event_time_end=event_end,
            source_time=source_time,
            received_at=response_received_at,
            available_at=max(response_received_at, event_end),
            row_count=len(frame),
            content_sha256=content_sha256,
            quality_status=quality,
            degraded_reasons=reasons,
            producer_version=self.config.producer_version,
            producer_commit=self.config.producer_commit,
        )
        pointer = self.spool.publish(envelope, payload)
        if dispatch_admitted and raw is not None and not provider_failure:
            self._write_state(
                _GatewayState(
                    admitted_at=state.admitted_at,
                    last_dispatch_at=state.last_dispatch_at,
                )
            )
        return WatchlistQuoteCapture(pointer=pointer, published=True)

    def _consume_quota_before_dispatch(
        self,
        *,
        admitted_at: datetime,
        request_id: str,
    ) -> None:
        if self._quota_store is None or self.config.quota_units_per_window is None:
            return
        window_start = admitted_at.replace(second=0, microsecond=0)
        window_reset = window_start + timedelta(minutes=1)
        self._quota_store.declare_window(
            source=self.config.source,
            window_id=window_start.strftime("%Y%m%dT%H%M"),
            starts_at=window_start,
            resets_at=window_reset,
            total_units=self.config.quota_units_per_window,
        )
        lease = self._quota_store.acquire(
            source=self.config.source,
            owner=f"watchlist-quote:{request_id}",
            units=self.config.quota_cost_per_request,
            now=admitted_at,
            expires_at=min(
                admitted_at + timedelta(seconds=self.config.request_timeout_seconds),
                window_reset,
            ),
        )
        self._quota_store.consume(
            lease.lease_id,
            usage_id=request_id,
            units=self.config.quota_cost_per_request,
            now=admitted_at,
        )
        self._quota_store.release(lease.lease_id, now=admitted_at)

    @staticmethod
    def _normalize_codes(codes: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({code.strip().upper() for code in codes if code.strip()}))
        if not normalized:
            raise WatchlistQuoteValidationError("watchlist quote codes cannot be empty")
        if any(len(code) != 9 or code[6] != "." for code in normalized):
            raise WatchlistQuoteValidationError("watchlist quote code is invalid")
        return normalized

    def _normalize_frame(
        self,
        raw: pd.DataFrame,
        *,
        codes: tuple[str, ...],
        response_received_at: datetime,
        trade_date: date,
    ) -> pd.DataFrame:
        if not isinstance(raw, pd.DataFrame):
            raise WatchlistQuoteValidationError("quote provider result must be a DataFrame")
        required = {"ts_code", "price", "open", "high", "low", "volume", "amount"}
        missing = sorted(required - set(raw.columns))
        if missing:
            raise WatchlistQuoteValidationError(f"quote provider missing columns: {missing}")
        frame = raw.loc[:, sorted(required)].copy().reset_index(drop=True)
        if frame.empty:
            raise WatchlistQuoteValidationError("quote provider returned no watchlist observations")
        frame["ts_code"] = frame["ts_code"].astype("string").str.strip().str.upper()
        if set(frame["ts_code"]) - set(codes):
            raise WatchlistQuoteValidationError(
                "quote provider returned code outside the watchlist"
            )
        if frame["ts_code"].duplicated().any():
            raise WatchlistQuoteValidationError(
                "quote provider returned duplicate code observations"
            )
        if "source_observed_at" in raw.columns:
            source_observed = pd.to_datetime(
                raw["source_observed_at"],
                errors="coerce",
                utc=True,
            ).reset_index(drop=True)
        else:
            source_observed = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
        frame["observed_at"] = source_observed.fillna(response_received_at)
        frame["source_timestamp_provenance"] = "response_received_at_fallback"
        frame.loc[source_observed.notna(), "source_timestamp_provenance"] = (
            "provider_source_timestamp"
        )
        observed = pd.to_datetime(frame["observed_at"], errors="raise", utc=True)
        if (observed > response_received_at).any():
            raise WatchlistQuoteValidationError("quote provider returned future observation")
        if any(value.date() != trade_date for value in observed.dt.tz_convert(_SHANGHAI)):
            raise WatchlistQuoteValidationError(
                "quote observation trade_date conflicts with request"
            )
        frame["observed_at"] = observed
        for column in ("price", "open", "high", "low", "volume", "amount"):
            frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float64")
        numeric = frame.loc[:, ["price", "open", "high", "low", "volume", "amount"]].to_numpy()
        if not math.isfinite(float(numeric.min())) or not math.isfinite(float(numeric.max())):
            raise WatchlistQuoteValidationError("quote provider returned non-finite numeric value")
        if (frame[["price", "open", "high", "low"]] <= 0).any().any():
            raise WatchlistQuoteValidationError("quote prices must be positive")
        if (frame[["volume", "amount"]] < 0).any().any():
            raise WatchlistQuoteValidationError("quote volume and amount cannot be negative")
        return frame.sort_values("ts_code", kind="stable", ignore_index=True)

    def _attach_provenance(
        self,
        frame: pd.DataFrame,
        *,
        scheduled_at: datetime,
        universe_as_of: datetime,
        requested_at: datetime,
        response_received_at: datetime,
        trade_date: date,
    ) -> pd.DataFrame:
        attached = frame.copy() if not frame.empty else _empty_frame()
        attached["trade_date"] = trade_date
        attached["source"] = self.config.source
        attached["scheduled_at"] = scheduled_at
        attached["universe_as_of"] = universe_as_of
        attached["requested_at"] = requested_at
        attached["response_received_at"] = response_received_at
        attached["fetched_at"] = response_received_at
        attached["producer_commit"] = self.config.producer_commit
        attached["schema_version"] = self.config.schema_version
        return attached.loc[:, _COLUMNS]

    @contextmanager
    def _capture_lock(self) -> Iterator[None]:
        descriptor = -1
        try:
            descriptor = os.open(
                self._capture_lock_path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            observed = os.fstat(descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or observed.st_nlink != 1
                or stat.S_IMODE(observed.st_mode) != 0o600
            ):
                raise WatchlistQuoteStateError("watchlist quote capture lock is unsafe")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        except OSError as exc:
            raise WatchlistQuoteStateError("watchlist quote capture lock is unavailable") from exc
        finally:
            if descriptor >= 0:
                with suppress(OSError):
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

    def _classify_lateness(
        self,
        *,
        quality: BatchQualityStatus,
        reasons: tuple[str, ...],
        event_end: datetime,
    ) -> tuple[BatchQualityStatus, tuple[str, ...]]:
        if quality is not BatchQualityStatus.PUBLISHED:
            return quality, reasons
        records = self.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)
        latest_observed = max(
            (record.envelope.event_time_end for record in records),
            default=None,
        )
        if latest_observed is not None and event_end < latest_observed:
            return BatchQualityStatus.DEGRADED, ("late_observation",)
        return quality, reasons

    def _revision_index(
        self,
    ) -> tuple[dict[tuple[datetime, datetime], BatchEnvelope], BatchEnvelope | None]:
        index: dict[tuple[datetime, datetime], BatchEnvelope] = {}
        latest: BatchEnvelope | None = None
        for record in self.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1):
            envelope = record.envelope
            key = (envelope.event_time_start, envelope.event_time_end)
            previous = index.get(key)
            if envelope.revision != (1 if previous is None else previous.revision + 1):
                raise WatchlistQuoteStateError("watchlist quote revision chain is not contiguous")
            if envelope.revises_batch_id != (None if previous is None else previous.batch_id):
                raise WatchlistQuoteStateError("watchlist quote revision parent is not contiguous")
            index[key] = envelope
            latest = envelope
        return index, latest

    def _find_request(self, request_id: str) -> LiveBatchRecord | None:
        matching = tuple(
            record
            for record in self.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)
            if record.envelope.source_request_id == request_id
        )
        if len(matching) > 1:
            raise WatchlistQuoteStateError("watchlist quote request digest is not unique")
        return None if not matching else matching[0]

    def _pointer_for(self, record: LiveBatchRecord) -> CurrentPointer | BatchPointer:
        current = self.spool.current(LiveChannel.WATCHLIST_QUOTE)
        if current is not None and current.sequence == record.envelope.sequence:
            return current
        return BatchPointer(
            channel=record.envelope.channel,
            source_generation_id=self.spool.source_descriptor(
                LiveChannel.WATCHLIST_QUOTE
            ).generation_id,
            batch_id=record.envelope.batch_id,
            sequence=record.envelope.sequence,
            revision=record.envelope.revision,
            content_sha256=record.envelope.content_sha256,
            quality_status=record.envelope.quality_status,
            published_at=record.envelope.available_at,
        )

    def _recover_inflight(
        self,
        state: _GatewayState,
        *,
        recovered_at: datetime,
    ) -> _GatewayState:
        request_id = state.inflight_request_id
        if request_id is None:
            return state
        if self._find_request(request_id) is not None:
            recovered = _GatewayState(
                admitted_at=state.admitted_at,
                last_dispatch_at=state.last_dispatch_at,
            )
        else:
            recovered = self._failed_state(
                state,
                recovered_at,
                conservative_dispatch_at=recovered_at,
            )
        self._write_state(recovered)
        return recovered

    @staticmethod
    def _clear_inflight(current: _GatewayState) -> _GatewayState:
        return _GatewayState(
            consecutive_failures=current.consecutive_failures,
            retry_not_before=current.retry_not_before,
            circuit_open_until=current.circuit_open_until,
            admitted_at=current.admitted_at,
            last_dispatch_at=current.last_dispatch_at,
        )

    def _failed_state(
        self,
        current: _GatewayState,
        fetched_at: datetime,
        *,
        conservative_dispatch_at: datetime | None = None,
    ) -> _GatewayState:
        failures = current.consecutive_failures + 1
        backoff = min(
            self.config.minimum_cadence_seconds * 2 ** max(failures - 1, 0),
            self.config.max_backoff_seconds,
        )
        retry_not_before = fetched_at + timedelta(seconds=backoff)
        circuit_open_until = (
            max(
                retry_not_before,
                fetched_at + timedelta(seconds=self.config.circuit_cooldown_seconds),
            )
            if failures >= self.config.failure_threshold
            else None
        )
        last_dispatch_at = current.last_dispatch_at
        if conservative_dispatch_at is not None and (
            last_dispatch_at is None or conservative_dispatch_at > last_dispatch_at
        ):
            last_dispatch_at = conservative_dispatch_at
        return _GatewayState(
            consecutive_failures=failures,
            retry_not_before=retry_not_before,
            circuit_open_until=circuit_open_until,
            admitted_at=current.admitted_at,
            last_dispatch_at=last_dispatch_at,
        )

    def _read_state(self) -> _GatewayState:
        if not self._state_path.exists():
            return _GatewayState()
        try:
            payload = json.loads(self._state_path.read_bytes())
            if not isinstance(payload, dict):
                raise ValueError("gateway state must be an object")
            schema_version = payload.get("schema_version", 2)
            if schema_version in (1, 2):
                legacy = _LegacyGatewayState.model_validate(payload)
                return _GatewayState(
                    consecutive_failures=legacy.consecutive_failures,
                    retry_not_before=legacy.retry_not_before,
                    circuit_open_until=legacy.circuit_open_until,
                    admitted_at=legacy.last_attempt_at,
                    last_dispatch_at=legacy.last_attempt_at,
                    inflight_request_id=legacy.inflight_request_id,
                )
            return _GatewayState.model_validate(payload)
        except (OSError, ValueError) as exc:
            raise WatchlistQuoteStateError("watchlist quote gateway state is incomplete") from exc

    def _write_state(self, state: _GatewayState) -> None:
        payload = json.dumps(
            state.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        descriptor = -1
        temporary = ""
        try:
            descriptor, temporary = tempfile.mkstemp(
                prefix=f".{self._state_path.name}.",
                dir=self._state_path.parent,
            )
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb", closefd=True) as stream:
                descriptor = -1
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self._state_path)
            directory = os.open(
                self._state_path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary:
                with suppress(FileNotFoundError):
                    os.unlink(temporary)


__all__ = [
    "WatchlistQuoteCapture",
    "WatchlistQuoteGateway",
    "WatchlistQuoteGatewayConfig",
    "WatchlistQuoteStateError",
    "WatchlistQuoteValidationError",
    "decode_watchlist_quote_payload",
    "encode_watchlist_quote_payload",
]
