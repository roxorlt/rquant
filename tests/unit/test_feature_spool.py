from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
)
from rquant.feature_spool import (
    FeatureBatchSpool,
    FeatureConsumerCursor,
    FeatureSpoolIntegrityError,
)

NOW = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)


def _payload(sequence: int) -> bytes:
    return json.dumps(
        {
            "rows": [{"ts_code": "600000.SH", "score": float(sequence)}],
            "schema_version": 1,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _envelope(sequence: int, payload: bytes | None = None) -> FeatureBatchEnvelope:
    import hashlib

    content = payload if payload is not None else _payload(sequence)
    return FeatureBatchEnvelope(
        schema_version=1,
        batch_id=f"feature-{sequence}",
        contract_id="intraday-feature/v1",
        contract_version=1,
        input_batch_ids=(f"minute-{sequence}",),
        sequence=sequence,
        event_time=NOW + timedelta(minutes=sequence),
        available_at=NOW + timedelta(minutes=sequence),
        row_count=1,
        content_hash=hashlib.sha256(content).hexdigest(),
        field_statuses=(
            FeatureFieldStatus(
                name="score",
                status=FeatureAvailability.AVAILABLE,
                available_at=NOW + timedelta(minutes=sequence),
            ),
        ),
        producer_commit="a" * 40,
    )


def test_publish_is_immutable_consecutive_and_survives_reopen(tmp_path: Path) -> None:
    spool = FeatureBatchSpool(tmp_path)
    first = spool.publish(_envelope(0), _payload(0))
    retry = spool.publish(_envelope(0), _payload(0))

    assert retry == first
    assert first.sequence == 0
    assert FeatureBatchSpool(tmp_path).current() == first

    with pytest.raises(FeatureSpoolIntegrityError, match="next sequence"):
        spool.publish(_envelope(2), _payload(2))
    with pytest.raises(FeatureSpoolIntegrityError, match="different content"):
        spool.publish(_envelope(0, _payload(9)), _payload(9))


def test_publish_rejects_payload_hash_or_contract_mismatch(tmp_path: Path) -> None:
    spool = FeatureBatchSpool(tmp_path)

    with pytest.raises(FeatureSpoolIntegrityError, match="content hash"):
        spool.publish(_envelope(0), _payload(1))
    malformed = b'{"rows":[],"schema_version":2}'
    with pytest.raises(FeatureSpoolIntegrityError, match="schema_version"):
        spool.publish(_envelope(0, malformed), malformed)


def test_list_after_and_read_payload_fail_closed_on_gap_or_tamper(tmp_path: Path) -> None:
    spool = FeatureBatchSpool(tmp_path)
    spool.publish(_envelope(0), _payload(0))
    spool.publish(_envelope(1), _payload(1))
    records = spool.list_after(sequence=-1)

    assert [item.envelope.sequence for item in records] == [0, 1]
    assert spool.read_payload(records[1]) == _payload(1)

    records[1].payload_path.write_bytes(b"tampered")
    with pytest.raises(FeatureSpoolIntegrityError, match="hash"):
        spool.read_payload(records[1])
    records[0].manifest_path.unlink()
    with pytest.raises(FeatureSpoolIntegrityError, match="sequence gap"):
        spool.list_after(sequence=-1)


def test_consumer_cursor_is_monotonic_and_bound_to_existing_batch(tmp_path: Path) -> None:
    spool = FeatureBatchSpool(tmp_path)
    pointers = [spool.publish(_envelope(index), _payload(index)) for index in range(2)]
    second = pointers[1]
    cursor = FeatureConsumerCursor(
        consumer_id="strategy:n-shape",
        last_sequence=second.sequence,
        last_batch_id=second.batch_id,
        last_content_hash=second.content_hash,
        updated_at=NOW + timedelta(minutes=2),
    )
    spool.commit_cursor(cursor)

    assert spool.load_cursor("strategy:n-shape") == cursor
    with pytest.raises(FeatureSpoolIntegrityError, match="regress"):
        spool.commit_cursor(
            FeatureConsumerCursor(
                consumer_id="strategy:n-shape",
                last_sequence=0,
                last_batch_id=pointers[0].batch_id,
                last_content_hash=pointers[0].content_hash,
                updated_at=NOW + timedelta(minutes=3),
            )
        )
    with pytest.raises(FeatureSpoolIntegrityError, match="missing batch"):
        spool.commit_cursor(
            FeatureConsumerCursor(
                consumer_id="strategy:other",
                last_sequence=9,
                last_batch_id="missing",
                last_content_hash="b" * 64,
                updated_at=NOW,
            )
        )


def test_cursor_cannot_claim_wrong_batch_identity(tmp_path: Path) -> None:
    spool = FeatureBatchSpool(tmp_path)
    spool.publish(_envelope(0), _payload(0))

    with pytest.raises(FeatureSpoolIntegrityError, match="does not match"):
        spool.commit_cursor(
            FeatureConsumerCursor(
                consumer_id="strategy:n-shape",
                last_sequence=0,
                last_batch_id="wrong",
                last_content_hash="b" * 64,
                updated_at=NOW,
            )
        )
