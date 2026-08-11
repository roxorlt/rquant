"""Runtime handoff between the slow-reference source and registry publisher."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from time import monotonic
from zoneinfo import ZoneInfo

from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    ConsumerCursor,
    LiveChannel,
)
from rquant.live_spool import LiveBatchRecord, LiveBatchSpool, LiveSpoolIntegrityError
from rquant.reference_data_registry import (
    ReferencePublicationDeadlineError,
    ReferenceRegistry,
)
from rquant.reference_slow_publisher import (
    ReferenceSlowPublishReceipt,
    ReferenceSlowSourceSnapshot,
    _publish_reference_slow_snapshot_with_rollback,
    _reference_generation_revision,
    build_reference_slow_serving_result,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_control import RuntimeStepResult
from rquant.strict_json import canonical_json_bytes, strict_model_validate_canonical_json

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CAPTURE_START = time(9, 20)
_CAPTURE_END = time(9, 25)
_REVISION_SCAN_START = time(9, 24)
_COMMIT_VISIBILITY_GUARD = timedelta(seconds=5)

SnapshotLoader = Callable[[], ReferenceSlowSourceSnapshot]
RevisionSnapshotLoader = Callable[[date], ReferenceSlowSourceSnapshot]


class _ReferenceRevisionScanState(RuntimeContractModel):
    discovery_trade_date: date
    producer_commit: str
    scanned_target_dates: tuple[date, ...]
    updated_at: datetime


class ReferenceSlowRuntimeError(RuntimeError):
    """A slow-reference runtime boundary cannot be trusted."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _record_snapshot(spool: LiveBatchSpool, record: LiveBatchRecord) -> ReferenceSlowSourceSnapshot:
    payload = spool.read_payload(record)
    try:
        snapshot = strict_model_validate_canonical_json(ReferenceSlowSourceSnapshot, payload)
    except ValueError as exc:
        raise ReferenceSlowRuntimeError("reference slow source payload is invalid") from exc
    if snapshot.content_sha256 != record.envelope.batch_id:
        raise ReferenceSlowRuntimeError("reference slow batch identity does not match payload")
    if len(snapshot.security_facts) != record.envelope.row_count:
        raise ReferenceSlowRuntimeError("reference slow batch row_count does not match payload")
    return snapshot


def _current_record(spool: LiveBatchSpool) -> LiveBatchRecord | None:
    pointer = spool.current(LiveChannel.REFERENCE_SLOW)
    if pointer is None:
        return None
    records = spool.list_after(
        LiveChannel.REFERENCE_SLOW,
        sequence=pointer.sequence - 1,
        limit=1,
    )
    if len(records) != 1 or records[0].envelope.sequence != pointer.sequence:
        raise ReferenceSlowRuntimeError("reference slow current batch cannot be resolved")
    return records[0]


def _latest_records_by_trade_date(
    spool: LiveBatchSpool,
    *,
    history_page_size: int,
) -> dict[date, tuple[LiveBatchRecord, ReferenceSlowSourceSnapshot]]:
    pointer = spool.current(LiveChannel.REFERENCE_SLOW)
    if pointer is None:
        return {}
    latest: dict[date, tuple[LiveBatchRecord, ReferenceSlowSourceSnapshot]] = {}
    start = max(-1, pointer.sequence - history_page_size)
    for record in spool.list_after(
        LiveChannel.REFERENCE_SLOW,
        sequence=start,
        limit=history_page_size,
    ):
        snapshot = _record_snapshot(spool, record)
        existing = latest.get(snapshot.target_trade_date)
        if existing is None or record.envelope.revision > existing[0].envelope.revision:
            latest[snapshot.target_trade_date] = (record, snapshot)
    return latest


def _load_revision_scan_state(
    spool: LiveBatchSpool,
    *,
    discovery_trade_date: date,
    producer_commit: str,
    updated_at: datetime,
) -> _ReferenceRevisionScanState:
    payload = spool.load_source_state("reference-revision-scan")
    if payload is not None:
        try:
            state = strict_model_validate_canonical_json(_ReferenceRevisionScanState, payload)
        except ValueError as exc:
            raise ReferenceSlowRuntimeError("reference revision scan cursor is invalid") from exc
        if (
            state.discovery_trade_date == discovery_trade_date
            and state.producer_commit == producer_commit
        ):
            return state
    return _ReferenceRevisionScanState(
        discovery_trade_date=discovery_trade_date,
        producer_commit=producer_commit,
        scanned_target_dates=(),
        updated_at=updated_at,
    )


def _store_revision_scan_state(
    spool: LiveBatchSpool,
    state: _ReferenceRevisionScanState,
    *,
    scanned_target_date: date,
    updated_at: datetime,
) -> None:
    scanned = tuple(sorted({*state.scanned_target_dates, scanned_target_date}, reverse=True))
    updated = state.model_copy(update={"scanned_target_dates": scanned, "updated_at": updated_at})
    spool.store_source_state(
        "reference-revision-scan",
        canonical_json_bytes(updated.model_dump(mode="json")),
    )


def capture_reference_slow_batch(
    *,
    spool: LiveBatchSpool,
    calendar: MarketCalendarAuthority,
    observed_at: datetime,
    producer_commit: str,
    producer_version: str,
    snapshot_loader: SnapshotLoader,
    revision_snapshot_loader: RevisionSnapshotLoader | None = None,
    revision_lookback_sessions: int = 5,
    history_page_size: int = 64,
    completion_clock: Callable[[], datetime],
) -> RuntimeStepResult:
    """Capture today's source batch or one bounded historical revision candidate."""

    observed = normalize_aware_utc(observed_at)
    authority = MarketCalendarAuthority.model_validate(calendar)
    if authority.generated_at > observed:
        raise ReferenceSlowRuntimeError("calendar is future evidence")
    local = observed.astimezone(_SHANGHAI)
    if local.date() not in authority.open_dates:
        return RuntimeStepResult(source_generations={"market_calendar": authority.content_sha256})
    local_time = local.timetz().replace(tzinfo=None)
    if local_time < _CAPTURE_START or local_time > _CAPTURE_END:
        return RuntimeStepResult(source_generations={"market_calendar": authority.content_sha256})
    if revision_lookback_sessions < 1 or revision_lookback_sessions > 20:
        raise ReferenceSlowRuntimeError("revision lookback must be between 1 and 20 sessions")
    if history_page_size < revision_lookback_sessions or history_page_size > 256:
        raise ReferenceSlowRuntimeError("reference history page size is invalid")

    current_record = _current_record(spool)
    latest_records = (
        _latest_records_by_trade_date(spool, history_page_size=history_page_size)
        if current_record is not None
        else {}
    )
    today_record = latest_records.get(local.date())
    revision_state: _ReferenceRevisionScanState | None = None
    revision_target: date | None = None
    revision_mode = today_record is not None

    if revision_mode:
        if revision_snapshot_loader is None or local_time < _REVISION_SCAN_START:
            assert current_record is not None
            current_snapshot = today_record[1]
            return RuntimeStepResult(
                input_sequence=current_record.envelope.sequence,
                output_sequence=current_record.envelope.sequence,
                source_generations={
                    "market_calendar": authority.content_sha256,
                    "reference_slow": current_snapshot.content_sha256,
                },
            )
        revision_state = _load_revision_scan_state(
            spool,
            discovery_trade_date=local.date(),
            producer_commit=producer_commit,
            updated_at=observed,
        )
        candidates = tuple(
            sorted(
                (trade_date for trade_date in authority.open_dates if trade_date <= local.date()),
                reverse=True,
            )[:revision_lookback_sessions]
        )
        revision_target = next(
            (
                trade_date
                for trade_date in candidates
                if trade_date not in revision_state.scanned_target_dates
            ),
            None,
        )
        if revision_target is None:
            assert current_record is not None
            return RuntimeStepResult(
                input_sequence=current_record.envelope.sequence,
                output_sequence=current_record.envelope.sequence,
                source_generations={
                    "market_calendar": authority.content_sha256,
                    "reference_slow": today_record[1].content_sha256,
                },
            )
        snapshot = ReferenceSlowSourceSnapshot.model_validate(
            revision_snapshot_loader(revision_target)
        )
        if snapshot.target_trade_date != revision_target:
            raise ReferenceSlowRuntimeError(
                "revision snapshot trade date does not match scan target"
            )
    else:
        snapshot = ReferenceSlowSourceSnapshot.model_validate(snapshot_loader())
        if snapshot.target_trade_date != local.date():
            raise ReferenceSlowRuntimeError(
                "source snapshot trade date does not match capture date"
            )
    if snapshot.captured_at < observed:
        raise ReferenceSlowRuntimeError("source snapshot completion precedes runtime start")
    completed_local = snapshot.captured_at.astimezone(_SHANGHAI)
    if (
        completed_local.date() != local.date()
        or completed_local.timetz().replace(tzinfo=None) > _CAPTURE_END
    ):
        raise ReferenceSlowRuntimeError("source snapshot completed outside capture window")
    if snapshot.producer_commit != producer_commit:
        raise ReferenceSlowRuntimeError("source snapshot producer_commit does not match")
    if snapshot.source_snapshot_ids["calendar"] != authority.content_sha256:
        raise ReferenceSlowRuntimeError("source snapshot calendar generation does not match")

    previous = latest_records.get(snapshot.target_trade_date)
    if (
        revision_mode
        and previous is not None
        and snapshot.revision_content_sha256 == previous[1].revision_content_sha256
    ):
        assert revision_state is not None
        assert revision_target is not None
        _store_revision_scan_state(
            spool,
            revision_state,
            scanned_target_date=revision_target,
            updated_at=snapshot.captured_at,
        )
        assert current_record is not None
        return RuntimeStepResult(
            input_sequence=current_record.envelope.sequence,
            output_sequence=current_record.envelope.sequence,
            source_generations={
                "market_calendar": authority.content_sha256,
                "reference_slow": previous[1].content_sha256,
            },
        )

    payload = canonical_json_bytes(snapshot.model_dump(mode="json"))
    payload_sha256 = hashlib.sha256(payload).hexdigest()
    decision_cutoff = datetime.combine(
        local.date(),
        _CAPTURE_END,
        tzinfo=_SHANGHAI,
    )
    prepared_at = normalize_aware_utc(completion_clock())
    if prepared_at < snapshot.captured_at:
        raise ReferenceSlowRuntimeError(
            "reference slow atomic availability precedes source evidence"
        )
    if prepared_at > decision_cutoff:
        raise ReferenceSlowRuntimeError("reference slow atomic publication completed after 09:25")
    monotonic_deadline = monotonic() + max(
        0.0,
        (decision_cutoff - prepared_at).total_seconds(),
    )
    available_at = min(prepared_at + _COMMIT_VISIBILITY_GUARD, decision_cutoff)
    current = spool.current(LiveChannel.REFERENCE_SLOW)
    sequence = 0 if current is None else current.sequence + 1
    envelope = BatchEnvelope(
        schema_version=1,
        channel=LiveChannel.REFERENCE_SLOW,
        dataset_id="reference_slow_source",
        source="rquant.reference_slow_source",
        source_request_id=canonical_sha256(
            {
                "contract": "reference-slow-source-request/v1",
                "target_trade_date": snapshot.target_trade_date,
                "revision_content_sha256": snapshot.revision_content_sha256,
                "revision": 1 if previous is None else previous[0].envelope.revision + 1,
                "producer_commit": producer_commit,
            }
        ),
        batch_id=snapshot.content_sha256,
        sequence=sequence,
        revision=1 if previous is None else previous[0].envelope.revision + 1,
        revises_batch_id=None if previous is None else previous[0].envelope.batch_id,
        event_time_start=snapshot.captured_at,
        event_time_end=snapshot.captured_at,
        source_time=snapshot.captured_at,
        received_at=available_at,
        available_at=available_at,
        row_count=len(snapshot.security_facts),
        content_sha256=payload_sha256,
        quality_status=BatchQualityStatus.PUBLISHED,
        producer_version=producer_version,
        producer_commit=producer_commit,
    )
    try:
        pointer = spool.publish(
            envelope,
            payload,
            completion_clock=completion_clock,
            not_after=decision_cutoff,
            monotonic_deadline=monotonic_deadline,
        )
    except LiveSpoolIntegrityError as exc:
        if "deadline" in str(exc):
            raise ReferenceSlowRuntimeError(
                "reference slow atomic publication completed after 09:25"
            ) from exc
        raise
    if revision_mode:
        assert revision_state is not None
        assert revision_target is not None
        _store_revision_scan_state(
            spool,
            revision_state,
            scanned_target_date=revision_target,
            updated_at=pointer.published_at,
        )
    return RuntimeStepResult(
        input_sequence=pointer.sequence,
        output_sequence=pointer.sequence,
        processed_count=1,
        source_generations={
            "market_calendar": authority.content_sha256,
            "reference_slow": snapshot.content_sha256,
        },
    )


def publish_reference_slow_batches(
    *,
    spool: LiveBatchSpool,
    registry: ReferenceRegistry,
    calendar: MarketCalendarAuthority,
    consumer_id: str,
    observed_at: datetime,
    producer_commit: str,
    completion_clock: Callable[[], datetime] | None = None,
    page_size: int = 16,
) -> RuntimeStepResult:
    """Publish every unconsumed source batch and advance only a durable cursor."""

    observed = normalize_aware_utc(observed_at)
    if page_size < 1 or page_size > 256:
        raise ReferenceSlowRuntimeError("reference publisher page size is invalid")
    clock = completion_clock or _utc_now
    started = max(observed, normalize_aware_utc(clock()))
    authority = MarketCalendarAuthority.model_validate(calendar)
    local_started = started.astimezone(_SHANGHAI)
    decision_cutoff = datetime.combine(
        local_started.date(),
        _CAPTURE_END,
        tzinfo=_SHANGHAI,
    )
    if started > decision_cutoff:
        raise ReferenceSlowRuntimeError("reference slow publisher started after 09:25")
    monotonic_deadline = monotonic() + max(
        0.0,
        (decision_cutoff - started).total_seconds(),
    )
    if spool.current(LiveChannel.REFERENCE_SLOW) is None:
        return RuntimeStepResult(source_generations={"market_calendar": authority.content_sha256})
    descriptor = spool.source_descriptor(LiveChannel.REFERENCE_SLOW)
    with registry.publication_commit_lock():
        pending = registry.pending_publication()
        if pending is not None:
            expected_receipt = spool.completion_receipt_path(pending.publication_id)
            if pending.completion_receipt_path != str(expected_receipt):
                raise ReferenceSlowRuntimeError(
                    "pending reference publication receipt path changed"
                )
            if pending.receipt_is_committed:
                recovered_cursor = spool.load_cursor(
                    consumer_id,
                    LiveChannel.REFERENCE_SLOW,
                )
                if recovered_cursor != pending.target_cursor:
                    raise ReferenceSlowRuntimeError(
                        "completed reference publication cursor does not match"
                    )
                registry.commit_publication_stage(pending.rollback)
                registry.finalize_publication(pending.rollback)
            else:
                spool.abort_cursor_publication(consumer_id, LiveChannel.REFERENCE_SLOW)
                registry.compensate_publication(pending.rollback)
            spool.remove_completion_receipt(pending.publication_id)
        cursor = spool.load_cursor(consumer_id, LiveChannel.REFERENCE_SLOW)
    last_sequence = -1 if cursor is None else cursor.last_sequence
    processed = 0
    generation_id: str | None = None
    authority_snapshot: ReferenceSlowSourceSnapshot | None = None
    authority_receipt: ReferenceSlowPublishReceipt | None = None
    for record in spool.list_after(
        LiveChannel.REFERENCE_SLOW,
        sequence=last_sequence,
        limit=page_size,
    ):
        envelope = record.envelope
        if envelope.quality_status is not BatchQualityStatus.PUBLISHED:
            raise ReferenceSlowRuntimeError("reference slow publisher requires published batches")
        if envelope.producer_commit != producer_commit:
            raise ReferenceSlowRuntimeError("reference slow batch producer_commit does not match")
        spool.verify_reference_source_record(record)
        snapshot = _record_snapshot(spool, record)
        if envelope.available_at > started:
            raise ReferenceSlowRuntimeError("reference slow source batch is future evidence")
        publication_id = canonical_sha256(
            {
                "contract": "reference-slow-publication/v1",
                "consumer_id": consumer_id,
                "source_generation_id": descriptor.generation_id,
                "sequence": envelope.sequence,
                "batch_id": envelope.batch_id,
                "content_sha256": envelope.content_sha256,
            }
        )
        completion_receipt_path = spool.completion_receipt_path(publication_id)
        cursor = ConsumerCursor(
            consumer_id=consumer_id,
            channel=LiveChannel.REFERENCE_SLOW,
            source_generation_id=descriptor.generation_id,
            last_sequence=envelope.sequence,
            last_batch_id=envelope.batch_id,
            last_content_sha256=envelope.content_sha256,
            updated_at=started,
        )
        with registry.publication_commit_lock():
            try:
                receipt, rollback = _publish_reference_slow_snapshot_with_rollback(
                    registry=registry,
                    calendar=authority,
                    snapshot=snapshot,
                    completion_clock=clock,
                    started_at=started,
                    retain_intent=True,
                    publication_id=publication_id,
                    completion_receipt_path=completion_receipt_path,
                    target_cursor=cursor,
                )
            except ReferencePublicationDeadlineError as exc:
                raise ReferenceSlowRuntimeError(
                    "reference slow publisher completed after 09:25"
                ) from exc
            generation_id = receipt.generation_id
            stage_sha256 = registry.pending_publication_stage_sha256(rollback)
            registry_staged = False
            cursor_finalized = False
            try:
                current_descriptor = spool.source_descriptor(LiveChannel.REFERENCE_SLOW)
                if current_descriptor.generation_id != descriptor.generation_id:
                    raise ReferenceSlowRuntimeError("reference slow source generation changed")
                spool.commit_cursor_with_deadline(
                    cursor,
                    completion_clock=clock,
                    not_after=decision_cutoff,
                    retain_intent=True,
                    publication_id=publication_id,
                    completion_receipt_path=completion_receipt_path,
                    registry_generation_id=receipt.generation_id,
                )
                spool.write_completion_receipt(
                    publication_id=publication_id,
                    registry_generation_id=receipt.generation_id,
                    cursor=cursor,
                    stage_sha256=stage_sha256,
                    completion_clock=clock,
                    not_after=decision_cutoff,
                    monotonic_deadline=monotonic_deadline,
                )
                registry.commit_publication_stage(rollback)
                registry_staged = True
                spool.complete_cursor_publication(consumer_id, LiveChannel.REFERENCE_SLOW)
                cursor_finalized = True
                registry.finalize_publication(rollback)
                spool.remove_completion_receipt(publication_id)
            except BaseException as exc:
                if registry_staged:
                    recovered_cursor = spool.load_cursor(
                        consumer_id,
                        LiveChannel.REFERENCE_SLOW,
                    )
                    if recovered_cursor == cursor:
                        registry.finalize_publication(rollback)
                        cursor_finalized = True
                if not cursor_finalized:
                    registry.compensate_publication(rollback)
                    spool.abort_cursor_publication(
                        consumer_id,
                        LiveChannel.REFERENCE_SLOW,
                    )
                if isinstance(exc, LiveSpoolIntegrityError) and "deadline" in str(exc):
                    evidence_path = spool.completion_evidence_root / f"{publication_id}.json"
                    if evidence_path.exists():
                        spool.finalize_deadline_rollback_evidence(
                            publication_id,
                            durable_completed_at=clock(),
                        )
                    raise ReferenceSlowRuntimeError(
                        "reference slow publisher completed after 09:25"
                    ) from exc
                raise
        authority_snapshot = snapshot
        authority_receipt = receipt
        last_sequence = envelope.sequence
        processed += 1

    authority_root = spool.root / "serving-authority"
    authority_generation_id: str | None = None
    if authority_snapshot is None or authority_receipt is None:
        from rquant.runtime_serving_authority import (
            ServingSourceAuthorityReader,
            ServingSourceAuthorityUnavailableError,
        )
        from rquant.runtime_serving_snapshot import REFERENCE_SLOW_AUTHORITY_DATASET_ID

        current_manifest = registry.current_manifest()
        try:
            current_authority = ServingSourceAuthorityReader(
                root=authority_root,
                expected_producer_commit=producer_commit,
                expected_dataset_id=REFERENCE_SLOW_AUTHORITY_DATASET_ID,
                expected_payload_kind="reference_slow",
            )(started)
        except ServingSourceAuthorityUnavailableError:
            current_authority = None
        if (
            current_authority is not None
            and current_authority.payload.reference_generation_id == current_manifest.generation_id
        ):
            authority_generation_id = current_authority.generation_id
        else:
            cursor = spool.load_cursor(consumer_id, LiveChannel.REFERENCE_SLOW)
            if cursor is None:
                raise ReferenceSlowRuntimeError(
                    "reference serving authority cannot resolve its source cursor"
                )
            records = spool.list_after(
                LiveChannel.REFERENCE_SLOW,
                sequence=cursor.last_sequence - 1,
                limit=1,
            )
            if (
                len(records) != 1
                or records[0].envelope.sequence != cursor.last_sequence
                or records[0].envelope.batch_id != cursor.last_batch_id
            ):
                raise ReferenceSlowRuntimeError(
                    "reference serving authority source batch is unavailable"
                )
            authority_snapshot = _record_snapshot(spool, records[0])
            authority_receipt = ReferenceSlowPublishReceipt(
                target_trade_date=authority_snapshot.target_trade_date,
                generation_id=current_manifest.generation_id,
                source_snapshot_id=authority_snapshot.content_sha256,
                inserted_record_count=0,
                security_count=len(authority_snapshot.security_facts),
                revision=_reference_generation_revision(
                    registry,
                    current_manifest.generation_id,
                ),
                available_at=current_manifest.published_at,
            )
    if authority_generation_id is None:
        from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
        from rquant.runtime_serving_snapshot import REFERENCE_SLOW_AUTHORITY_DATASET_ID

        assert authority_snapshot is not None
        assert authority_receipt is not None
        authority_result = build_reference_slow_serving_result(
            snapshot=authority_snapshot,
            receipt=authority_receipt,
        )
        authority_pointer = ServingSourceAuthorityPublisher(
            root=authority_root,
            producer_commit=producer_commit,
            dataset_id=REFERENCE_SLOW_AUTHORITY_DATASET_ID,
            payload_kind="reference_slow",
            clock=lambda: authority_receipt.available_at,
        ).publish(authority_result)
        authority_generation_id = authority_pointer.generation_id

    generations = {
        "market_calendar": authority.content_sha256,
        "reference_slow_spool": descriptor.generation_id,
    }
    if generation_id is not None:
        generations["reference_registry"] = generation_id
    generations["reference_slow_authority"] = authority_generation_id
    return RuntimeStepResult(
        input_sequence=last_sequence,
        output_sequence=last_sequence,
        processed_count=processed,
        source_generations=generations,
    )


__all__ = [
    "ReferenceSlowRuntimeError",
    "SnapshotLoader",
    "capture_reference_slow_batch",
    "publish_reference_slow_batches",
]
