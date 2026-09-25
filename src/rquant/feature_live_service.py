"""Durable raw-minute to point-in-time feature service step."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from types import MappingProxyType
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
)
from rquant.feature_spool import FeatureBatchSpool
from rquant.intraday_feature_engine import (
    STATUS_COLUMNS,
    FeatureComputationMode,
    FeatureComputationResult,
    IntradayFeatureConfig,
    NormalizedHistoricalMinutes,
    live_compute,
)
from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    ConsumerCursor,
    LiveChannel,
)
from rquant.live_spool import LiveBatchRecord, LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_market_session import MarketCalendarAuthority

if TYPE_CHECKING:
    from rquant.runtime_schema_registry import RuntimeSchemaDualWriter

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_SESSION_CLOSE = time(15, 0)


def current_runtime_schema_dual_writer(
    channel_id: str,
    *,
    producer_commit: str,
) -> RuntimeSchemaDualWriter | None:
    from rquant.runtime_schema_registry import (
        current_runtime_schema_dual_writer as current_writer,
    )

    return current_writer(channel_id, producer_commit=producer_commit)


class FeatureLiveBatchSummary(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_high_watermark: int = Field(ge=-1)
    started_after_sequence: int = Field(ge=-1)
    last_raw_sequence: int = Field(ge=-1)
    feature_high_watermark: int = Field(ge=-1)
    processed_count: int = Field(ge=0)
    replayed_count: int = Field(ge=0)
    stale_count: int = Field(ge=0)
    has_deferred_batches: bool
    close_marker_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


FeatureLiveFaultHook = Callable[[str], None]


def _decoded_identity(envelope: BatchEnvelope) -> tuple[str, str, datetime, int]:
    #: everything the decoded rows depend on: the payload (its hash) and the two columns
    #: added to it; `batch_id` binds the sequence, revision and event window as well
    return (envelope.batch_id, envelope.content_sha256, envelope.available_at, envelope.sequence)


@dataclass(frozen=True)
class _DecodedMinuteBatch:
    identity: tuple[str, str, datetime, int]
    #: the batch's rows of each Asia/Shanghai trade date, each already carrying the
    #: `available_at` / `_source_sequence` columns the day frame is assembled from
    frames_by_date: Mapping[date, pd.DataFrame]


class FeatureLiveInputCache:
    """Market-minute batches one `feature_live` process has already read and decoded (#302).

    Every feature batch is computed from all of the day's minute batches up to its target,
    and until #302 each one re-read, re-hashed and re-decoded all of them: the per-batch
    cost grew with every minute of the day. A batch is decoded here once, on the first
    target that needs it, from bytes `read_payload` verified against its manifest's hash;
    later targets reuse the decoded rows as long as the spool still lists the batch with the
    same batch id, content hash and availability -- and `list_after`, which lists it, still
    checks every retained file on every call. Nothing here is persisted: a restart starts
    empty and rebuilds from the spool, exactly as every batch did before.

    Rows of a trade date before the newest target's are dropped as the day advances; a
    target of an earlier date than one already processed (which a single spool never
    produces) is served by decoding again, without keeping the result.
    """

    def __init__(self) -> None:
        self._source_generation_id: str | None = None
        self._batches: dict[int, _DecodedMinuteBatch] = {}
        self._floor: date | None = None

    def frame(
        self,
        raw_spool: LiveBatchSpool,
        record: LiveBatchRecord,
        *,
        source_generation_id: str,
        target_date: date,
    ) -> pd.DataFrame | None:
        if source_generation_id != self._source_generation_id:
            self._batches.clear()
            self._floor = None
            self._source_generation_id = source_generation_id
        envelope = record.envelope
        below_floor = self._floor is not None and target_date < self._floor
        cached = None if below_floor else self._batches.get(envelope.sequence)
        if cached is None or cached.identity != _decoded_identity(envelope):
            cached = _decode_minute_batch(raw_spool, record)
            if not below_floor:
                self._batches[envelope.sequence] = cached
        return cached.frames_by_date.get(target_date)

    def retire_before(self, target_date: date) -> None:
        if self._floor is not None and target_date <= self._floor:
            return
        self._floor = target_date
        for sequence, batch in tuple(self._batches.items()):
            if any(day < target_date for day in batch.frames_by_date):
                self._batches[sequence] = _DecodedMinuteBatch(
                    identity=batch.identity,
                    frames_by_date=MappingProxyType(
                        {
                            day: frame
                            for day, frame in batch.frames_by_date.items()
                            if day >= target_date
                        }
                    ),
                )


def _decode_minute_batch(raw_spool: LiveBatchSpool, record: LiveBatchRecord) -> _DecodedMinuteBatch:
    envelope = record.envelope
    frame = MarketMinuteGateway.decode_payload(raw_spool.read_payload(record))
    frames: dict[date, pd.DataFrame] = {}
    if not frame.empty:
        local_dates = (
            pd.to_datetime(frame["trade_time"], utc=True).dt.tz_convert("Asia/Shanghai").dt.date
        )
        for day in set(local_dates):
            if not isinstance(day, date):
                continue
            selected = frame.loc[local_dates == day].copy()
            if selected.empty:
                continue
            selected["available_at"] = envelope.available_at
            selected["_source_sequence"] = envelope.sequence
            frames[day] = selected
    return _DecodedMinuteBatch(
        identity=_decoded_identity(envelope),
        frames_by_date=MappingProxyType(frames),
    )


def _feature_input_identity(
    raw_spool: LiveBatchSpool,
    *,
    target: LiveBatchRecord,
    historical_snapshot_id: str,
    records: tuple[LiveBatchRecord, ...],
    source_generation_id: str,
    input_cache: FeatureLiveInputCache,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    target_date = pd.Timestamp(target.envelope.event_time_end).tz_convert("Asia/Shanghai").date()
    frames: list[pd.DataFrame] = []
    input_ids: list[str] = [historical_snapshot_id]
    for record in records:
        envelope = record.envelope
        if envelope.sequence > target.envelope.sequence:
            break
        if envelope.available_at > target.envelope.available_at:
            continue
        if envelope.quality_status is BatchQualityStatus.STALE:
            continue
        frame = input_cache.frame(
            raw_spool,
            record,
            source_generation_id=source_generation_id,
            target_date=target_date,
        )
        if frame is None:
            continue
        frames.append(frame)
        input_ids.append(envelope.batch_id)
    input_cache.retire_before(target_date)
    #: The target's own batch is always part of the identity. A batch that adds no row of its
    #: date -- the STALE batch of a source failure, an empty answer -- used to leave it equal
    #: to the previous feature batch's, which `_next_feature_sequence` reads as the crash
    #: replay of that batch: the publish then met different content at its sequence and
    #: `feature_live` refused every round for the rest of the day. A batch that does add rows
    #: is already in the identity, and so is every target of a day with no rows yet.
    input_ids.append(target.envelope.batch_id)
    if not frames:
        return pd.DataFrame(), tuple(sorted(set(input_ids)))
    current = pd.concat(frames, ignore_index=True)
    current = current.sort_values(
        ["ts_code", "trade_time", "_source_sequence"],
        kind="stable",
    )
    current = current.drop_duplicates(["ts_code", "trade_time"], keep="last")
    return current.drop(columns=["_source_sequence"]), tuple(sorted(set(input_ids)))


def _next_feature_sequence(
    feature_spool: FeatureBatchSpool,
    *,
    input_batch_ids: tuple[str, ...],
) -> tuple[int, bool]:
    current = feature_spool.current()
    if current is None:
        return 0, False
    record = feature_spool.list_after(
        sequence=current.sequence - 1,
        through_sequence=current.sequence,
        limit=1,
    )[0]
    if record.envelope.input_batch_ids == input_batch_ids:
        return current.sequence, True
    return current.sequence + 1, False


def _empty_result(
    record: LiveBatchRecord,
    *,
    input_batch_ids: tuple[str, ...],
    sequence: int,
    config: IntradayFeatureConfig,
    status: FeatureAvailability,
    reason: str,
) -> FeatureComputationResult:
    payload_json = json.dumps(
        {"rows": [], "schema_version": config.schema_version},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    content_hash = hashlib.sha256(payload_json.encode()).hexdigest()
    available_at = record.envelope.available_at
    envelope = FeatureBatchEnvelope(
        schema_version=config.schema_version,
        batch_id=canonical_sha256(
            {
                "contract_id": config.contract_id,
                "contract_version": config.contract_version,
                "input_batch_ids": input_batch_ids,
                "sequence": sequence,
                "event_time": available_at,
                "content_hash": content_hash,
                "producer_commit": config.producer_commit,
                "quality": status.value,
            }
        ),
        contract_id=config.contract_id,
        contract_version=config.contract_version,
        input_batch_ids=input_batch_ids,
        sequence=sequence,
        event_time=available_at,
        available_at=available_at,
        decision_cutoff=available_at,
        actual_delay_seconds=0.0,
        row_count=0,
        content_hash=content_hash,
        field_statuses=tuple(
            FeatureFieldStatus(
                name=name,
                status=status,
                source_event_time=available_at,
                available_at=available_at,
                decision_cutoff=available_at,
                actual_delay_seconds=0.0,
                reason=reason,
            )
            for name in STATUS_COLUMNS
        ),
        producer_commit=config.producer_commit,
    )
    return FeatureComputationResult(
        mode=FeatureComputationMode.LIVE,
        payload_json=payload_json,
        envelope=envelope,
    )


def _degrade_result(
    result: FeatureComputationResult,
    *,
    reasons: tuple[str, ...],
) -> FeatureComputationResult:
    source_reason = f"source_degraded:{','.join(reasons)}"

    def merge_reason(existing: str | None) -> str:
        parts = [] if existing is None else existing.split(";")
        if source_reason not in parts:
            parts.append(source_reason)
        return ";".join(parts)

    statuses = tuple(
        FeatureFieldStatus(
            candidate_id=status.candidate_id,
            name=status.name,
            status=(
                FeatureAvailability.DEGRADED
                if status.status is FeatureAvailability.AVAILABLE
                else status.status
            ),
            source_event_time=status.source_event_time,
            available_at=status.available_at,
            decision_cutoff=status.decision_cutoff,
            actual_delay_seconds=status.actual_delay_seconds,
            reason=merge_reason(status.reason),
        )
        for status in result.envelope.field_statuses
    )
    payload = result.envelope.model_dump(mode="python")
    payload["field_statuses"] = statuses
    return FeatureComputationResult(
        mode=result.mode,
        payload_json=result.payload_json,
        envelope=FeatureBatchEnvelope.model_validate(payload),
    )


def run_feature_live_batch(
    *,
    raw_spool: LiveBatchSpool,
    feature_spool: FeatureBatchSpool,
    historical_minutes: pd.DataFrame | NormalizedHistoricalMinutes,
    historical_snapshot_id: str,
    config: IntradayFeatureConfig,
    observed_at: datetime,
    limit: int,
    consumer_id: str = "feature-live",
    fault_hook: FeatureLiveFaultHook | None = None,
    schema_dual_writer: RuntimeSchemaDualWriter | None = None,
    calendar: MarketCalendarAuthority | None = None,
    input_cache: FeatureLiveInputCache | None = None,
) -> FeatureLiveBatchSummary:
    """Publish the feature batches of the raw minute batches after this consumer's cursor.

    `input_cache` and a `NormalizedHistoricalMinutes` history are what a long-lived caller
    (the runtime step) passes to keep one round's cost proportional to the batches that are
    new (#302); without them every call decodes the day and normalizes the history afresh,
    and the batches it publishes are the same bytes either way.
    """
    observed = normalize_aware_utc(observed_at)
    if not historical_snapshot_id:
        raise ValueError("historical_snapshot_id cannot be empty")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("limit must be a positive integer")
    if calendar is not None and calendar.generated_at > observed:
        raise ValueError("feature completion calendar was generated after observed_at")
    descriptor = raw_spool.source_descriptor(LiveChannel.MARKET_MINUTE)
    cursor = raw_spool.load_cursor(consumer_id, LiveChannel.MARKET_MINUTE)
    started_after = -1 if cursor is None else cursor.last_sequence
    cache = FeatureLiveInputCache() if input_cache is None else input_cache
    #: one listing per call: every feature batch of this call reads its inputs from it, where
    #: each one used to list (and so re-validate) the whole spool again
    day_records = raw_spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
    records = tuple(record for record in day_records if record.envelope.sequence > started_after)[
        :limit
    ]

    processed = 0
    replayed = 0
    stale = 0
    last_sequence = started_after
    for record in records:
        envelope = record.envelope
        if envelope.available_at > observed:
            break
        current_minutes, input_batch_ids = _feature_input_identity(
            raw_spool,
            target=record,
            historical_snapshot_id=historical_snapshot_id,
            records=day_records,
            source_generation_id=descriptor.generation_id,
            input_cache=cache,
        )
        feature_sequence, is_replay = _next_feature_sequence(
            feature_spool,
            input_batch_ids=input_batch_ids,
        )
        if envelope.quality_status is BatchQualityStatus.STALE:
            reasons = ",".join(envelope.degraded_reasons) or "source_stale"
            result = _empty_result(
                record,
                input_batch_ids=input_batch_ids,
                sequence=feature_sequence,
                config=config,
                status=FeatureAvailability.STALE,
                reason=f"source_stale:{reasons}",
            )
            stale += 1
        elif current_minutes.empty:
            result = _empty_result(
                record,
                input_batch_ids=input_batch_ids,
                sequence=feature_sequence,
                config=config,
                status=FeatureAvailability.UNAVAILABLE,
                reason="source_empty",
            )
        else:
            result = live_compute(
                current_minutes,
                (
                    historical_minutes
                    if isinstance(historical_minutes, NormalizedHistoricalMinutes)
                    else historical_minutes.copy(deep=True)
                ),
                decision_time=envelope.available_at,
                input_available_at=envelope.available_at,
                input_batch_ids=input_batch_ids,
                sequence=feature_sequence,
                config=config,
            )
            if envelope.quality_status is BatchQualityStatus.DEGRADED:
                result = _degrade_result(
                    result,
                    reasons=envelope.degraded_reasons,
                )
        schema_writer = schema_dual_writer or current_runtime_schema_dual_writer(
            "runtime.intraday_feature.batch-envelope",
            producer_commit=config.producer_commit,
        )
        prepared_schema_write = (
            None
            if schema_writer is None
            else schema_writer.prepare_payload(
                result.envelope.model_dump(mode="json"),
                observed_at=result.envelope.available_at,
            )
        )
        feature_spool.publish(result.envelope, result.payload_bytes)
        if schema_writer is not None and prepared_schema_write is not None:
            schema_writer.commit_payload(
                prepared_schema_write,
                operation_id=f"intraday-feature:{result.envelope.batch_id}",
            )
        if fault_hook is not None:
            fault_hook("after_feature_publish")
        raw_spool.commit_cursor(
            ConsumerCursor(
                consumer_id=consumer_id,
                channel=LiveChannel.MARKET_MINUTE,
                source_generation_id=descriptor.generation_id,
                last_sequence=envelope.sequence,
                last_batch_id=envelope.batch_id,
                last_content_sha256=envelope.content_sha256,
                updated_at=observed,
            )
        )
        processed += 1
        replayed += int(is_replay)
        last_sequence = envelope.sequence

    feature_descriptor = feature_spool.source_descriptor()
    has_deferred_batches = last_sequence < descriptor.high_watermark
    close_marker_id: str | None = None
    if calendar is not None and not has_deferred_batches and descriptor.high_watermark >= 0:
        local_observed = observed.astimezone(_SHANGHAI)
        trade_date = local_observed.date()
        if (
            calendar.coverage_start <= trade_date <= calendar.coverage_end
            and trade_date in calendar.open_dates
        ):
            session_close = datetime.combine(
                trade_date,
                _SESSION_CLOSE,
                tzinfo=_SHANGHAI,
            ).astimezone(observed.tzinfo)
            if observed >= session_close:
                final_records = raw_spool.list_after(
                    LiveChannel.MARKET_MINUTE,
                    sequence=descriptor.high_watermark - 1,
                    limit=1,
                )
                if len(final_records) != 1:
                    raise ValueError("raw minute final batch is unavailable at close")
                final_record = final_records[0]
                if final_record.envelope.event_time_end == session_close:
                    marker = feature_spool.publish_session_close_marker(
                        trade_date=trade_date,
                        session_close_at=session_close,
                        produced_at=observed,
                        calendar_generation_id=calendar.content_sha256,
                        complete_through=session_close,
                        upstream_source_generation_id=descriptor.generation_id,
                        upstream_final_sequence=final_record.envelope.sequence,
                        upstream_final_batch_id=final_record.envelope.batch_id,
                        upstream_final_content_hash=final_record.envelope.content_sha256,
                        fault_hook=fault_hook,
                    )
                    close_marker_id = marker.marker_id
    return FeatureLiveBatchSummary(
        observed_at=observed,
        source_generation_id=descriptor.generation_id,
        source_high_watermark=descriptor.high_watermark,
        started_after_sequence=started_after,
        last_raw_sequence=last_sequence,
        feature_high_watermark=feature_descriptor.high_watermark,
        processed_count=processed,
        replayed_count=replayed,
        stale_count=stale,
        has_deferred_batches=has_deferred_batches,
        close_marker_id=close_marker_id,
    )


__all__ = ["FeatureLiveBatchSummary", "FeatureLiveInputCache", "run_feature_live_batch"]
