"""Single-fetch market-minute gateway publishing immutable live batches."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import TYPE_CHECKING, Annotated

import numpy as np
import pandas as pd
from pydantic import Field, StringConstraints

from rquant.live_contracts import (
    BatchEnvelope,
    BatchPointer,
    BatchQualityStatus,
    CurrentPointer,
    LiveChannel,
)
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_contracts import (
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.source_quota_store import (
    SourceQuotaConflictError,
    SourceQuotaStore,
)
from rquant.source_quota_transport import QuotaBoundTransportObserver

if TYPE_CHECKING:
    from rquant.runtime_schema_registry import RuntimeSchemaDualWriter


def current_runtime_schema_dual_writer(
    channel_id: str,
    *,
    producer_commit: str,
) -> RuntimeSchemaDualWriter | None:
    from rquant.runtime_schema_registry import (
        current_runtime_schema_dual_writer as current_writer,
    )

    return current_writer(channel_id, producer_commit=producer_commit)


CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]

MARKET_MINUTE_COLUMNS = (
    "ts_code",
    "trade_time",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
)


class MarketMinuteValidationError(ValueError):
    pass


class MarketMinuteGatewayConfig(RuntimeContractModel):
    source: str = Field(default="tushare.rt_min", min_length=1)
    dataset_id: str = Field(default="market_minute", min_length=1)
    producer_version: str = Field(min_length=1)
    producer_commit: CommitSha
    quota_units_per_window: int | None = Field(default=None, gt=0)
    quota_cost_per_request: int = Field(default=1, gt=0)
    pending_recovery_min_age_seconds: int = Field(default=60, strict=True, ge=30)


class MarketMinuteCapture(RuntimeContractModel):
    pointer: CurrentPointer | BatchPointer
    published: bool


class MarketMinuteGateway:
    """The only owner of a market-minute source request and its live spool."""

    def __init__(
        self,
        *,
        spool: LiveBatchSpool,
        fetcher: Callable[[], pd.DataFrame],
        config: MarketMinuteGatewayConfig,
        completion_clock: Callable[[], datetime] | None = None,
        quota_store: SourceQuotaStore | None = None,
        transport_observer: QuotaBoundTransportObserver | None = None,
        schema_dual_writer: RuntimeSchemaDualWriter | None = None,
    ) -> None:
        self.spool = spool
        self._fetcher = fetcher
        self._completion_clock = completion_clock
        self.config = config
        self._quota_store = quota_store
        self._transport_observer = transport_observer
        self._schema_dual_writer = schema_dual_writer
        self._latest_by_event_window: dict[tuple[datetime, datetime], BatchEnvelope] | None = None
        self._latest_published: BatchEnvelope | None = None
        if config.quota_units_per_window is not None and quota_store is None:
            raise ValueError("quota_store is required when quota governance is enabled")
        if config.quota_units_per_window is not None and transport_observer is None:
            raise ValueError("transport_observer is required when quota governance is enabled")

    def _resolve_quota_cost(self, quota_cost_units: int | None) -> int:
        units = self.config.quota_cost_per_request
        if quota_cost_units is not None:
            if isinstance(quota_cost_units, bool) or not isinstance(quota_cost_units, int):
                raise ValueError("market-minute quota cost must be an integer")
            units = quota_cost_units
        if units <= 0 or units > self.config.quota_cost_per_request:
            raise ValueError("market-minute quota cost exceeds the configured call budget")
        return units

    def _fetch_with_quota(
        self,
        received: datetime,
        *,
        quota_cost_units: int,
        previous_batch_id: str | None,
    ) -> pd.DataFrame:
        if self._quota_store is None or self.config.quota_units_per_window is None:
            return self._fetcher()
        self._quota_store.recover_stale_attempts(
            source=self.config.source,
            now=received,
            min_age=timedelta(seconds=self.config.pending_recovery_min_age_seconds),
        )
        request_id = canonical_sha256(
            {
                "source": self.config.source,
                "received_at": received,
                "producer": self.config.producer_version,
                "quota_cost_units": quota_cost_units,
                "previous_batch_id": previous_batch_id,
            }
        )
        if self._transport_observer is None:
            raise SourceQuotaConflictError("market-minute transport quota observer is required")
        existing_outcome = self._transport_observer.request_outcome(request_id)
        if existing_outcome is not None:
            raise SourceQuotaConflictError(
                f"market-minute source attempt already exists: {existing_outcome.value}"
            )
        with self._transport_observer.scope(
            logical_request_id=request_id,
            observed_at=received,
        ):
            result = self._fetcher()
            receipts = self._transport_observer.current_receipts()
        if len(receipts) != quota_cost_units:
            raise SourceQuotaConflictError(
                "market-minute transport receipt count does not match expected source calls"
            )
        return result

    @staticmethod
    def _empty_frame() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "ts_code": pd.Series(dtype="string"),
                "trade_time": pd.Series(dtype="datetime64[ns, UTC]"),
                **{
                    column: pd.Series(dtype="float64")
                    for column in MARKET_MINUTE_COLUMNS
                    if column not in {"ts_code", "trade_time"}
                },
            }
        )

    @staticmethod
    def normalize_frame(raw: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(raw, pd.DataFrame):
            raise MarketMinuteValidationError("source result must be a DataFrame")
        missing = sorted(set(MARKET_MINUTE_COLUMNS) - set(raw.columns))
        if missing:
            raise MarketMinuteValidationError(f"missing columns: {missing}")
        frame = raw.loc[:, MARKET_MINUTE_COLUMNS].copy()
        if frame.empty:
            return MarketMinuteGateway._empty_frame()
        frame["ts_code"] = frame["ts_code"].astype("string").str.strip()
        if frame["ts_code"].isna().any() or (frame["ts_code"] == "").any():
            raise MarketMinuteValidationError("ts_code cannot be empty")
        try:
            trade_time = pd.to_datetime(frame["trade_time"], errors="raise")
            if trade_time.dt.tz is None:
                trade_time = trade_time.dt.tz_localize(
                    "Asia/Shanghai",
                    ambiguous="raise",
                    nonexistent="raise",
                )
            frame["trade_time"] = trade_time.dt.tz_convert(UTC)
            for column in MARKET_MINUTE_COLUMNS[2:]:
                frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float64")
        except (TypeError, ValueError) as exc:
            raise MarketMinuteValidationError("invalid market-minute value") from exc
        numeric = frame.loc[:, MARKET_MINUTE_COLUMNS[2:]].to_numpy(dtype="float64")
        if not np.isfinite(numeric).all():
            raise MarketMinuteValidationError("market-minute numeric values must be finite")
        if frame.duplicated(subset=["ts_code", "trade_time"]).any():
            raise MarketMinuteValidationError("duplicate ts_code and trade_time rows")
        return frame.sort_values(["trade_time", "ts_code"], kind="stable").reset_index(drop=True)

    @staticmethod
    def encode_payload(frame: pd.DataFrame) -> bytes:
        output = BytesIO()
        frame.to_parquet(output, index=False)
        return output.getvalue()

    @staticmethod
    def decode_payload(payload: bytes) -> pd.DataFrame:
        frame = pd.read_parquet(BytesIO(payload))
        if "trade_time" in frame:
            frame["trade_time"] = pd.to_datetime(frame["trade_time"], utc=True)
        return frame

    def _revision_index(self) -> dict[tuple[datetime, datetime], BatchEnvelope]:
        if self._latest_by_event_window is not None:
            return self._latest_by_event_window
        index: dict[tuple[datetime, datetime], BatchEnvelope] = {}
        records = self.spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
        for record in records:
            envelope = record.envelope
            key = (envelope.event_time_start, envelope.event_time_end)
            previous = index.get(key)
            expected_revision = 1 if previous is None else previous.revision + 1
            expected_parent = None if previous is None else previous.batch_id
            if (
                envelope.revision != expected_revision
                or envelope.revises_batch_id != expected_parent
            ):
                raise MarketMinuteValidationError(
                    "stored market-minute revision chain is not contiguous"
                )
            index[key] = envelope
        self._latest_by_event_window = index
        self._latest_published = None if not records else records[-1].envelope
        return index

    def capture_once(
        self,
        *,
        received_at: datetime,
        quota_cost_units: int | None = None,
    ) -> MarketMinuteCapture:
        received = normalize_aware_utc(received_at)
        resolved_quota_cost = self._resolve_quota_cost(quota_cost_units)
        revision_index = self._revision_index()
        latest = self._latest_published
        quality = BatchQualityStatus.PUBLISHED
        degraded_reasons: tuple[str, ...] = ()
        try:
            frame = self.normalize_frame(
                self._fetch_with_quota(
                    received,
                    quota_cost_units=resolved_quota_cost,
                    previous_batch_id=None if latest is None else latest.batch_id,
                )
            )
        except MarketMinuteValidationError:
            raise
        except Exception as exc:
            frame = self._empty_frame()
            quality = BatchQualityStatus.STALE
            degraded_reasons = (f"source_error:{type(exc).__name__}",)

        payload = self.encode_payload(frame)
        completed_at = (
            received
            if self._completion_clock is None
            else normalize_aware_utc(self._completion_clock())
        )
        if completed_at < received:
            raise MarketMinuteValidationError(
                "market-minute completion time precedes request receipt"
            )
        content_hash = hashlib.sha256(payload).hexdigest()
        if frame.empty:
            event_start = event_end = received
            source_time = received
        else:
            event_start = frame["trade_time"].min().to_pydatetime()
            event_end = frame["trade_time"].max().to_pydatetime()
            source_time = event_end
            if event_end > completed_at:
                raise MarketMinuteValidationError(
                    "market-minute source returned a future event window"
                )

        event_window = (event_start, event_end)
        previous_revision = revision_index.get(event_window)
        schema_writer = self._schema_dual_writer or current_runtime_schema_dual_writer(
            "runtime.market_minute.batch-envelope",
            producer_commit=self.config.producer_commit,
        )
        if (
            previous_revision is not None
            and previous_revision.content_sha256 == content_hash
            and previous_revision.quality_status is quality
            and previous_revision.degraded_reasons == degraded_reasons
        ):
            pointer: CurrentPointer | BatchPointer
            if self._is_current_eligible(previous_revision.quality_status):
                current = self.spool.current(LiveChannel.MARKET_MINUTE)
                if current is None:
                    raise MarketMinuteValidationError(
                        "market-minute current pointer conflicts with the revision index"
                    )
                pointer = current
            else:
                pointer = BatchPointer(
                    channel=previous_revision.channel,
                    source_generation_id=self.spool.source_descriptor(
                        LiveChannel.MARKET_MINUTE
                    ).generation_id,
                    batch_id=previous_revision.batch_id,
                    sequence=previous_revision.sequence,
                    revision=previous_revision.revision,
                    content_sha256=previous_revision.content_sha256,
                    quality_status=previous_revision.quality_status,
                    published_at=previous_revision.available_at,
                )
            if schema_writer is not None:
                prepared = schema_writer.prepare_payload(
                    previous_revision.model_dump(mode="json"),
                    observed_at=previous_revision.available_at,
                )
                if prepared is not None:
                    schema_writer.commit_payload(
                        prepared,
                        operation_id=f"market-minute:{previous_revision.batch_id}",
                    )
            return MarketMinuteCapture(
                pointer=pointer,
                published=False,
            )

        sequence = 0 if latest is None else latest.sequence + 1
        revision = 1 if previous_revision is None else previous_revision.revision + 1
        revises_batch_id = None if previous_revision is None else previous_revision.batch_id
        identity = {
            "channel": LiveChannel.MARKET_MINUTE,
            "sequence": sequence,
            "revision": revision,
            "event_time_start": event_start,
            "event_time_end": event_end,
            "content_sha256": content_hash,
        }
        batch_id = canonical_sha256(identity)
        envelope = BatchEnvelope(
            schema_version=1,
            channel=LiveChannel.MARKET_MINUTE,
            dataset_id=self.config.dataset_id,
            source=self.config.source,
            source_request_id=canonical_sha256(
                {
                    "source": self.config.source,
                    "event_time_end": event_end,
                    "received_at": received,
                    "content_sha256": content_hash,
                }
            ),
            batch_id=batch_id,
            sequence=sequence,
            revision=revision,
            revises_batch_id=revises_batch_id,
            event_time_start=event_start,
            event_time_end=event_end,
            source_time=source_time,
            received_at=received,
            available_at=max(completed_at, event_end),
            row_count=len(frame),
            content_sha256=content_hash,
            quality_status=quality,
            degraded_reasons=degraded_reasons,
            producer_version=self.config.producer_version,
            producer_commit=self.config.producer_commit,
        )
        prepared_schema_write = (
            None
            if schema_writer is None
            else schema_writer.prepare_payload(
                envelope.model_dump(mode="json"),
                observed_at=envelope.available_at,
            )
        )
        pointer = self.spool.publish(envelope, payload)
        if schema_writer is not None and prepared_schema_write is not None:
            schema_writer.commit_payload(
                prepared_schema_write,
                operation_id=f"market-minute:{envelope.batch_id}",
            )
        revision_index[event_window] = envelope
        self._latest_published = envelope
        return MarketMinuteCapture(pointer=pointer, published=True)

    @staticmethod
    def _is_current_eligible(quality_status: BatchQualityStatus) -> bool:
        return quality_status is BatchQualityStatus.PUBLISHED
