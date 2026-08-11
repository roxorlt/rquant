from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Event

import pytest

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
)
from rquant.feature_spool import (
    FeatureBatchSpool,
    FeatureConsumerCursor,
    FeatureSessionCloseMarker,
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
        decision_cutoff=NOW + timedelta(minutes=sequence),
        actual_delay_seconds=0.0,
        row_count=1,
        content_hash=hashlib.sha256(content).hexdigest(),
        field_statuses=(
            FeatureFieldStatus(
                name="score",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=NOW + timedelta(minutes=sequence),
                available_at=NOW + timedelta(minutes=sequence),
                decision_cutoff=NOW + timedelta(minutes=sequence),
                actual_delay_seconds=0.0,
            ),
        ),
        producer_commit="a" * 40,
    )


def _envelope_at(
    sequence: int,
    event_time: datetime,
    payload: bytes | None = None,
) -> FeatureBatchEnvelope:
    envelope = _envelope(sequence, payload)
    return envelope.model_copy(
        update={
            "event_time": event_time,
            "available_at": event_time,
            "decision_cutoff": event_time,
            "field_statuses": tuple(
                status.model_copy(
                    update={
                        "source_event_time": event_time,
                        "available_at": event_time,
                        "decision_cutoff": event_time,
                    }
                )
                for status in envelope.field_statuses
            ),
        }
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


def test_feature_payload_rejects_excessive_json_depth_before_decode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nested: object = "leaf"
    for _ in range(80):
        nested = {"child": nested}
    payload = json.dumps(
        {"rows": [{"ts_code": "600000.SH", "nested": nested}], "schema_version": 1},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    real_loads = json.loads
    decoded = False

    def record_decode(value: object) -> object:
        nonlocal decoded
        decoded = True
        return real_loads(value)

    monkeypatch.setattr("rquant.feature_spool.json.loads", record_decode)

    with pytest.raises(FeatureSpoolIntegrityError, match="depth"):
        FeatureBatchSpool(tmp_path).publish(_envelope(0, payload), payload)
    assert decoded is False


def test_feature_payload_rejects_excessive_json_width_before_decode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = json.dumps(
        {
            "rows": [{"ts_code": "600000.SH", "wide": list(range(32))}],
            "schema_version": 1,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    monkeypatch.setattr("rquant.feature_spool._MAX_JSON_NODES", 8)
    monkeypatch.setattr(
        "rquant.feature_spool.json.loads",
        lambda _value: pytest.fail("wide payload must be rejected before json.loads"),
    )

    with pytest.raises(FeatureSpoolIntegrityError, match="node|width"):
        FeatureBatchSpool(tmp_path).publish(_envelope(0, payload), payload)


def test_source_generation_is_stable_and_cursor_is_bound_to_it(tmp_path: Path) -> None:
    first = FeatureBatchSpool(tmp_path / "first")
    reopened = FeatureBatchSpool(tmp_path / "first")
    rebuilt = FeatureBatchSpool(tmp_path / "rebuilt")

    assert reopened.source_descriptor() == first.source_descriptor()
    assert rebuilt.source_descriptor().generation_id != first.source_descriptor().generation_id
    assert first.source_descriptor().high_watermark == -1

    pointer = first.publish(_envelope(0), _payload(0))
    descriptor = first.source_descriptor()
    assert descriptor.high_watermark == 0
    assert pointer.source_generation_id == descriptor.generation_id

    with pytest.raises(FeatureSpoolIntegrityError, match="generation"):
        first.commit_cursor(
            FeatureConsumerCursor(
                consumer_id="strategy:n-shape",
                source_generation_id="b" * 64,
                last_sequence=0,
                last_batch_id=pointer.batch_id,
                last_content_hash=pointer.content_hash,
                updated_at=NOW,
            )
        )


def test_publish_rejects_payload_hash_or_contract_mismatch(tmp_path: Path) -> None:
    spool = FeatureBatchSpool(tmp_path)

    with pytest.raises(FeatureSpoolIntegrityError, match="content hash"):
        spool.publish(_envelope(0), _payload(1))
    malformed = b'{"rows":[],"schema_version":2}'
    with pytest.raises(FeatureSpoolIntegrityError, match="schema_version"):
        spool.publish(_envelope(0, malformed), malformed)


def test_exact_retry_recovers_missing_current_pointer_after_partial_publish(
    tmp_path: Path,
) -> None:
    spool = FeatureBatchSpool(tmp_path)
    pointer = spool.publish(_envelope(0), _payload(0))
    spool.current_path.unlink()

    with pytest.raises(FeatureSpoolIntegrityError, match="current pointer is missing"):
        spool.list_after(sequence=-1)

    assert spool.publish(_envelope(0), _payload(0)) == pointer
    assert spool.current() == pointer


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
        source_generation_id=spool.source_descriptor().generation_id,
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
                source_generation_id=spool.source_descriptor().generation_id,
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
                source_generation_id=spool.source_descriptor().generation_id,
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
                source_generation_id=spool.source_descriptor().generation_id,
                last_sequence=0,
                last_batch_id="wrong",
                last_content_hash="b" * 64,
                updated_at=NOW,
            )
        )


def test_readonly_strategy_keeps_cursor_outside_feature_spool(tmp_path: Path) -> None:
    source_root = tmp_path / "features"
    producer = FeatureBatchSpool(source_root)
    pointer = producer.publish(_envelope(0), _payload(0))
    source_entries_before = tuple(
        sorted(path.relative_to(source_root) for path in source_root.rglob("*"))
    )
    cursor_root = tmp_path / "strategy" / "cursors"
    consumer = FeatureBatchSpool(
        source_root,
        cursor_root=cursor_root,
        read_only=True,
    )
    cursor = FeatureConsumerCursor(
        consumer_id="strategy:n-shape",
        source_generation_id=consumer.source_descriptor().generation_id,
        last_sequence=0,
        last_batch_id=pointer.batch_id,
        last_content_hash=pointer.content_hash,
        updated_at=NOW,
    )

    consumer.commit_cursor(cursor)

    assert consumer.load_cursor("strategy:n-shape") == cursor
    assert (
        tuple(sorted(path.relative_to(source_root) for path in source_root.rglob("*")))
        == source_entries_before
    )
    assert tuple(cursor_root.glob("*.json"))
    with pytest.raises(FeatureSpoolIntegrityError, match="read-only"):
        consumer.publish(_envelope(1), _payload(1))


def test_session_close_marker_freezes_incremental_chain_and_rejects_late_append(
    tmp_path: Path,
) -> None:
    spool = FeatureBatchSpool(tmp_path)
    session_close = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)
    spool.publish(_envelope_at(0, NOW), _payload(0))
    final = _envelope_at(1, session_close)
    spool.publish(final, _payload(1))

    marker = spool.publish_session_close_marker(
        trade_date=date(2026, 7, 31),
        session_close_at=session_close,
        produced_at=session_close + timedelta(seconds=5),
        calendar_generation_id="c" * 64,
        complete_through=session_close,
        upstream_source_generation_id="d" * 64,
        upstream_final_sequence=1,
        upstream_final_batch_id="raw-1",
        upstream_final_content_hash="e" * 64,
    )

    assert isinstance(marker, FeatureSessionCloseMarker)
    assert marker.source_generation_id == spool.source_descriptor().generation_id
    assert marker.first_sequence == 0
    assert marker.final_sequence == 1
    assert marker.final_batch_id == final.batch_id
    assert marker.final_content_hash == final.content_hash
    assert marker.batch_count == 2
    assert len(marker.segment_chain_hash) == 64
    assert (
        FeatureBatchSpool(tmp_path, read_only=True).session_close_marker(marker.trade_date)
        == marker
    )

    late = _envelope_at(2, session_close - timedelta(minutes=1))
    with pytest.raises(FeatureSpoolIntegrityError, match="closed"):
        spool.publish(late, _payload(2))

    assert spool.publish(final, _payload(1)).sequence == 1


def test_session_close_marker_rejects_final_event_after_exact_close(tmp_path: Path) -> None:
    spool = FeatureBatchSpool(tmp_path)
    session_close = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)
    spool.publish(
        _envelope_at(0, session_close + timedelta(seconds=1)),
        _payload(0),
    )

    with pytest.raises(FeatureSpoolIntegrityError, match="15:00|exact close"):
        spool.publish_session_close_marker(
            trade_date=date(2026, 7, 31),
            session_close_at=session_close,
            produced_at=session_close + timedelta(seconds=5),
            calendar_generation_id="c" * 64,
            complete_through=session_close,
            upstream_source_generation_id="d" * 64,
            upstream_final_sequence=0,
            upstream_final_batch_id="raw-0",
            upstream_final_content_hash="e" * 64,
        )


def test_session_close_marker_serializes_concurrent_late_append(tmp_path: Path) -> None:
    spool = FeatureBatchSpool(tmp_path)
    session_close = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)
    spool.publish(_envelope_at(0, session_close), _payload(0))
    entered = Event()
    release = Event()

    def pause_before_commit(stage: str) -> None:
        if stage == "before_session_close_marker_commit":
            entered.set()
            assert release.wait(timeout=5)

    with ThreadPoolExecutor(max_workers=2) as executor:
        closing = executor.submit(
            spool.publish_session_close_marker,
            trade_date=date(2026, 7, 31),
            session_close_at=session_close,
            produced_at=session_close + timedelta(seconds=5),
            calendar_generation_id="c" * 64,
            complete_through=session_close,
            upstream_source_generation_id="d" * 64,
            upstream_final_sequence=0,
            upstream_final_batch_id="raw-0",
            upstream_final_content_hash="e" * 64,
            fault_hook=pause_before_commit,
        )
        assert entered.wait(timeout=5)
        appending = executor.submit(
            spool.publish,
            _envelope_at(1, session_close - timedelta(minutes=1)),
            _payload(1),
        )
        assert not appending.done()
        release.set()
        assert closing.result(timeout=5).final_sequence == 0
        with pytest.raises(FeatureSpoolIntegrityError, match="closed"):
            appending.result(timeout=5)


def test_session_close_marker_lost_return_retry_reuses_first_durable_value(
    tmp_path: Path,
) -> None:
    session_close = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)
    first_produced_at = session_close + timedelta(seconds=5)
    spool = FeatureBatchSpool(tmp_path)
    spool.publish(_envelope_at(0, session_close), _payload(0))

    def lose_return(stage: str) -> None:
        if stage == "after_session_close_marker_commit":
            raise RuntimeError("lost return")

    with pytest.raises(RuntimeError, match="lost return"):
        spool.publish_session_close_marker(
            trade_date=date(2026, 7, 31),
            session_close_at=session_close,
            produced_at=first_produced_at,
            calendar_generation_id="c" * 64,
            complete_through=session_close,
            upstream_source_generation_id="d" * 64,
            upstream_final_sequence=0,
            upstream_final_batch_id="raw-0",
            upstream_final_content_hash="e" * 64,
            fault_hook=lose_return,
        )

    reopened = FeatureBatchSpool(tmp_path)
    retried = reopened.publish_session_close_marker(
        trade_date=date(2026, 7, 31),
        session_close_at=session_close,
        produced_at=first_produced_at + timedelta(minutes=5),
        calendar_generation_id="c" * 64,
        complete_through=session_close,
        upstream_source_generation_id="d" * 64,
        upstream_final_sequence=0,
        upstream_final_batch_id="raw-0",
        upstream_final_content_hash="e" * 64,
    )
    assert retried.produced_at == first_produced_at
    assert reopened.session_close_marker(retried.trade_date) == retried

    with pytest.raises(FeatureSpoolIntegrityError, match="conflicts"):
        reopened.publish_session_close_marker(
            trade_date=date(2026, 7, 31),
            session_close_at=session_close,
            produced_at=first_produced_at + timedelta(minutes=6),
            calendar_generation_id="c" * 64,
            complete_through=session_close,
            upstream_source_generation_id="d" * 64,
            upstream_final_sequence=0,
            upstream_final_batch_id="raw-0",
            upstream_final_content_hash="f" * 64,
        )


def test_session_close_marker_rejects_oversized_control_json_before_validation(
    tmp_path: Path,
) -> None:
    session_close = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)
    spool = FeatureBatchSpool(tmp_path)
    spool.publish(_envelope_at(0, session_close), _payload(0))
    marker = spool.publish_session_close_marker(
        trade_date=date(2026, 7, 31),
        session_close_at=session_close,
        produced_at=session_close + timedelta(seconds=5),
        calendar_generation_id="c" * 64,
        complete_through=session_close,
        upstream_source_generation_id="d" * 64,
        upstream_final_sequence=0,
        upstream_final_batch_id="raw-0",
        upstream_final_content_hash="e" * 64,
    )
    marker_path = tmp_path / "sessions" / marker.trade_date.isoformat() / "close-marker.json"
    marker_path.write_bytes(b'{"padding":"' + b"x" * (1024 * 1024) + b'"}')

    with pytest.raises(FeatureSpoolIntegrityError, match="byte budget"):
        spool.session_close_marker(marker.trade_date)


def test_session_close_marker_rejects_deep_json_before_model_validation(
    tmp_path: Path,
) -> None:
    session_close = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)
    spool = FeatureBatchSpool(tmp_path)
    spool.publish(_envelope_at(0, session_close), _payload(0))
    marker = spool.publish_session_close_marker(
        trade_date=date(2026, 7, 31),
        session_close_at=session_close,
        produced_at=session_close + timedelta(seconds=5),
        calendar_generation_id="c" * 64,
        complete_through=session_close,
        upstream_source_generation_id="d" * 64,
        upstream_final_sequence=0,
        upstream_final_batch_id="raw-0",
        upstream_final_content_hash="e" * 64,
    )
    payload = marker.model_dump(mode="json")
    nested: object = "leaf"
    for _ in range(80):
        nested = {"child": nested}
    payload["unexpected"] = nested
    marker_path = tmp_path / "sessions" / marker.trade_date.isoformat() / "close-marker.json"
    marker_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(FeatureSpoolIntegrityError, match="depth budget"):
        spool.session_close_marker(marker.trade_date)
