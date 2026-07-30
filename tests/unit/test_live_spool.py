from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    ConsumerCursor,
    LiveChannel,
)
from rquant.live_spool import LiveBatchSpool, LiveSpoolIntegrityError

NOW = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)


def _payload(sequence: int) -> bytes:
    return f"minute-{sequence}".encode()


def _envelope(
    sequence: int,
    *,
    quality: BatchQualityStatus = BatchQualityStatus.PUBLISHED,
    payload: bytes | None = None,
) -> BatchEnvelope:
    body = payload if payload is not None else _payload(sequence)
    return BatchEnvelope(
        schema_version=1,
        channel=LiveChannel.MARKET_MINUTE,
        dataset_id="market_minute",
        source="tushare.rt_min",
        source_request_id=f"request-{sequence}",
        batch_id=f"20260731-{sequence:06d}",
        sequence=sequence,
        revision=1,
        event_time_start=NOW + timedelta(minutes=sequence),
        event_time_end=NOW + timedelta(minutes=sequence),
        source_time=NOW + timedelta(minutes=sequence, seconds=1),
        received_at=NOW + timedelta(minutes=sequence, seconds=2),
        available_at=NOW + timedelta(minutes=sequence, seconds=2),
        row_count=1,
        content_sha256=hashlib.sha256(body).hexdigest(),
        quality_status=quality,
        degraded_reasons=("partial_source",)
        if quality in {BatchQualityStatus.DEGRADED, BatchQualityStatus.STALE}
        else (),
        producer_version="live-v1",
        producer_commit="a" * 40,
    )


def test_publish_is_immutable_ordered_and_replay_idempotent(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "live")

    first = spool.publish(_envelope(0), _payload(0))
    replayed = spool.publish(_envelope(0), _payload(0))
    second = spool.publish(_envelope(1), _payload(1))

    assert replayed == first
    assert second.sequence == 1
    assert spool.current(LiveChannel.MARKET_MINUTE) == second
    records = spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
    assert [record.envelope.sequence for record in records] == [0, 1]
    assert spool.read_payload(records[0]) == _payload(0)


def test_publish_rejects_sequence_gap_and_conflicting_replay(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))

    with pytest.raises(LiveSpoolIntegrityError, match="next sequence"):
        spool.publish(_envelope(2), _payload(2))
    conflicting = _payload(99)
    with pytest.raises(LiveSpoolIntegrityError, match="immutable"):
        spool.publish(_envelope(0, payload=conflicting), conflicting)


def test_publish_rejects_payload_hash_mismatch_and_non_publishable_status(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")

    with pytest.raises(LiveSpoolIntegrityError, match="content hash"):
        spool.publish(_envelope(0), b"different")
    with pytest.raises(LiveSpoolIntegrityError, match="current"):
        spool.publish(
            _envelope(0, quality=BatchQualityStatus.CANDIDATE),
            _payload(0),
        )


def test_read_detects_payload_corruption(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))
    record = spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    record.payload_path.write_bytes(b"tampered")

    with pytest.raises(LiveSpoolIntegrityError, match="content hash"):
        spool.read_payload(record)


def test_list_after_detects_a_gap_before_current(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))
    spool.publish(_envelope(1), _payload(1))
    first = spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    first.manifest_path.unlink()

    with pytest.raises(LiveSpoolIntegrityError, match="sequence gap"):
        spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)


def test_consumer_cursor_is_persisted_independently_and_cannot_regress(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    pointer = spool.publish(_envelope(0), _payload(0))
    cursor = ConsumerCursor(
        consumer_id="strategy-growth",
        channel=LiveChannel.MARKET_MINUTE,
        last_sequence=pointer.sequence,
        last_batch_id=pointer.batch_id,
        last_content_sha256=pointer.content_sha256,
        updated_at=NOW,
    )

    spool.commit_cursor(cursor)
    assert spool.load_cursor("strategy-growth", LiveChannel.MARKET_MINUTE) == cursor

    regressed = cursor.model_copy(
        update={
            "last_sequence": -1,
            "last_batch_id": None,
            "last_content_sha256": None,
        }
    )
    with pytest.raises(LiveSpoolIntegrityError, match="regress"):
        spool.commit_cursor(regressed)
