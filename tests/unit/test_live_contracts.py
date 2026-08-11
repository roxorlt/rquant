from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rquant.live_contracts import (
    BatchEnvelope,
    BatchPointer,
    BatchQualityStatus,
    ConsumerCursor,
    CurrentPointer,
    LiveChannel,
)

SHA256 = "a" * 64
COMMIT = "b" * 40


def _batch_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "channel": LiveChannel.MARKET_MINUTE,
        "dataset_id": "market_minute",
        "source": "tushare",
        "source_request_id": "request-1",
        "batch_id": "batch-1",
        "sequence": 0,
        "revision": 1,
        "event_time_start": "2026-07-31T09:30:00+08:00",
        "event_time_end": "2026-07-31T09:31:00+08:00",
        "source_time": "2026-07-31T09:31:01+08:00",
        "received_at": "2026-07-31T09:31:02+08:00",
        "available_at": "2026-07-31T09:31:03+08:00",
        "row_count": 12,
        "content_sha256": SHA256,
        "quality_status": BatchQualityStatus.PUBLISHED,
        "degraded_reasons": (),
        "producer_version": "0.30.0",
        "producer_commit": COMMIT,
    }
    payload.update(overrides)
    return payload


def test_batch_envelope_normalizes_timestamps_and_has_stable_identity() -> None:
    batch = BatchEnvelope.model_validate(_batch_payload())
    equivalent = BatchEnvelope.model_validate(
        _batch_payload(
            event_time_start=datetime(2026, 7, 31, 1, 30, tzinfo=UTC),
            event_time_end=datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
            source_time=datetime(2026, 7, 31, 1, 31, 1, tzinfo=UTC),
            received_at=datetime(2026, 7, 31, 1, 31, 2, tzinfo=UTC),
            available_at=datetime(2026, 7, 31, 1, 31, 3, tzinfo=UTC),
        )
    )

    assert batch.event_time_start == datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
    assert batch.event_time_end.tzinfo is UTC
    assert batch.identity_sha256 == equivalent.identity_sha256
    assert len(batch.identity_sha256) == 64

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        BatchEnvelope.model_validate(_batch_payload(unknown="value"))

    with pytest.raises(ValidationError, match="Instance is frozen"):
        batch.sequence = 2  # type: ignore[misc]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"event_time_end": "2026-07-31T09:29:00+08:00"}, "event_time_start"),
        ({"available_at": "2026-07-31T09:31:01+08:00"}, "available_at"),
        ({"revision": 1, "revises_batch_id": "batch-0"}, "revision=1"),
        ({"revision": 2}, "revision>1"),
    ],
)
def test_batch_envelope_rejects_invalid_time_and_revision_rules(
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        BatchEnvelope.model_validate(_batch_payload(**overrides))


def test_batch_envelope_accepts_revision_with_previous_batch() -> None:
    batch = BatchEnvelope.model_validate(_batch_payload(revision=2, revises_batch_id="batch-0"))

    assert batch.revision == 2
    assert batch.revises_batch_id == "batch-0"


@pytest.mark.parametrize("status", [BatchQualityStatus.DEGRADED, BatchQualityStatus.STALE])
def test_degraded_or_stale_batch_requires_unique_reasons(
    status: BatchQualityStatus,
) -> None:
    with pytest.raises(ValidationError, match="require degraded_reasons"):
        BatchEnvelope.model_validate(_batch_payload(quality_status=status))

    with pytest.raises(ValidationError, match="must be unique"):
        BatchEnvelope.model_validate(
            _batch_payload(
                quality_status=status,
                degraded_reasons=("late_source", "late_source"),
            )
        )

    batch = BatchEnvelope.model_validate(
        _batch_payload(
            quality_status=status,
            degraded_reasons=("late_source", "partial_rows"),
        )
    )
    assert batch.degraded_reasons == ("late_source", "partial_rows")


@pytest.mark.parametrize(
    "status",
    [
        BatchQualityStatus.CANDIDATE,
        BatchQualityStatus.PUBLISHED,
        BatchQualityStatus.QUARANTINED,
    ],
)
def test_non_degraded_batch_forbids_degraded_reasons(
    status: BatchQualityStatus,
) -> None:
    with pytest.raises(ValidationError, match="forbid degraded_reasons"):
        BatchEnvelope.model_validate(
            _batch_payload(quality_status=status, degraded_reasons=("unexpected",))
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("content_sha256", "A" * 64),
        ("content_sha256", "a" * 63),
        ("producer_commit", "B" * 40),
        ("producer_commit", "b" * 39),
    ],
)
def test_batch_envelope_rejects_invalid_hash_or_commit(
    field: str,
    value: str,
) -> None:
    with pytest.raises(ValidationError):
        BatchEnvelope.model_validate(_batch_payload(**{field: value}))


def test_consumer_cursor_requires_batch_and_hash_as_a_pair() -> None:
    base: dict[str, object] = {
        "consumer_id": "feature-live",
        "channel": LiveChannel.MARKET_MINUTE,
        "source_generation_id": "b" * 64,
        "last_sequence": -1,
        "updated_at": datetime(2026, 7, 31, 2, tzinfo=UTC),
    }
    empty = ConsumerCursor.model_validate(base)
    populated = ConsumerCursor.model_validate(
        {
            **base,
            "last_sequence": 8,
            "last_batch_id": "batch-8",
            "last_content_sha256": SHA256,
        }
    )

    assert empty.last_batch_id is None
    assert populated.last_content_sha256 == SHA256
    assert empty.identity_sha256 != populated.identity_sha256

    with pytest.raises(ValidationError, match="both set or both absent"):
        ConsumerCursor.model_validate({**base, "last_batch_id": "batch-8"})

    with pytest.raises(ValidationError, match="both set or both absent"):
        ConsumerCursor.model_validate({**base, "last_content_sha256": SHA256})


@pytest.mark.parametrize(
    "quality_status",
    [
        BatchQualityStatus.CANDIDATE,
        BatchQualityStatus.QUARANTINED,
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.STALE,
    ],
)
def test_current_pointer_rejects_unpublished_status(
    quality_status: BatchQualityStatus,
) -> None:
    with pytest.raises(ValidationError, match="cannot be current"):
        CurrentPointer(
            channel=LiveChannel.MARKET_MINUTE,
            source_generation_id="b" * 64,
            batch_id="batch-1",
            sequence=1,
            revision=1,
            content_sha256=SHA256,
            quality_status=quality_status,
            published_at=datetime(2026, 7, 31, 2, tzinfo=UTC),
        )


def test_batch_pointer_accepts_recorded_noncurrent_quality_and_normalizes_time() -> None:
    pointer = BatchPointer(
        channel=LiveChannel.MARKET_MINUTE,
        source_generation_id="b" * 64,
        batch_id="batch-1",
        sequence=1,
        revision=1,
        content_sha256=SHA256,
        quality_status=BatchQualityStatus.DEGRADED,
        published_at=datetime(
            2026,
            7,
            31,
            10,
            tzinfo=timezone(timedelta(hours=8)),
        ),
    )

    assert pointer.published_at == datetime(2026, 7, 31, 2, tzinfo=UTC)
    assert len(pointer.identity_sha256) == 64
