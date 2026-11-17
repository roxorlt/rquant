from __future__ import annotations

import hashlib
import json
import struct
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.l2.models import RawL2Batch
from rquant.l2.segment_log import (
    ACTIVE_NAME, HEAD_NAME, HEADER, HEADER_SIZE, INDEX_NAME, KIND_COMMIT, KIND_FOOTER,
    KIND_PAYLOAD, MAGIC, PREFIX_SIZE, RECORD_PREFIX, SegmentEntry, SegmentError,
    SegmentWriter, append_index, audit, build_header, frame_record, iter_payloads,
    load_snapshot, read_head, read_index, read_layout, read_payload, scan_segment,
    read_segment_index, segment_name, segments_dir, write_layout,
)
from tests.unit.test_l2_cache import observed_trade_push_data


def make_batch(sequence: int) -> RawL2Batch:
    data = observed_trade_push_data()
    received = datetime(2026, 11, 6, 1, 30, tzinfo=UTC) + timedelta(milliseconds=sequence)
    data["local_batch_sequence"] = sequence
    data["received_at"] = received
    data["cached_at"] = received
    return RawL2Batch.model_validate(data)


def write(writer: SegmentWriter, sequences: range | list[int]) -> None:
    for sequence in sequences:
        writer.append(make_batch(sequence))


@pytest.fixture
def clock():
    now = [1000.0]

    def tick(step: float = 0.0) -> float:
        now[0] += step
        return now[0]

    tick.now = now  # type: ignore[attr-defined]
    return tick


def test_frame_record_roundtrip_and_validation():
    frame = frame_record(KIND_PAYLOAD, b"hello")
    total, kind, crc = RECORD_PREFIX.unpack(frame[:PREFIX_SIZE])
    assert total == len(frame) and kind == KIND_PAYLOAD
    with pytest.raises(SegmentError):
        frame_record(9, b"x")
    with pytest.raises(SegmentError):
        frame_record(KIND_PAYLOAD, b"x" * (24 * 1024 * 1024))


def test_segment_name_roundtrip():
    assert segment_name(1, 10) == "seg-00000000000000000001-00000000000000000010.l2s"
    from rquant.l2.segment_log import parse_segment_name

    assert parse_segment_name(segment_name(3, 9)) == (3, 9)
    assert parse_segment_name("seg-1-2.l2s") is None
    assert parse_segment_name("nope") is None


def test_writer_commits_and_reader_sees_only_committed(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=10, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 6))
    assert writer.committed_through == 0  # no commit yet
    assert list(iter_payloads(tmp_path)) == []
    assert writer.commit() == 5
    assert writer.committed_through == 5
    batches = list(iter_payloads(tmp_path))
    assert [b.local_batch_sequence for b in batches] == [1, 2, 3, 4, 5]
    snapshot = load_snapshot(tmp_path)
    assert snapshot.committed_through == 5
    assert snapshot.last_received_at is not None
    writer.close()


def test_uncommitted_tail_is_truncated_on_reopen(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=10, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 4))
    writer.commit()
    write(writer, range(4, 7))  # never committed: simulated crash
    writer._stream.close()  # type: ignore[union-attr]
    writer._stream = None
    assert len(list(iter_payloads(tmp_path))) == 3

    recovered = SegmentWriter(tmp_path, commit_every_batches=10, commit_interval_seconds=5)
    result = recovered.open()
    assert result.truncated_bytes > 0
    assert result.committed_through == 3
    assert [b.local_batch_sequence for b in iter_payloads(tmp_path)] == [1, 2, 3]
    recovered.append(make_batch(4))
    recovered.commit()
    assert [b.local_batch_sequence for b in iter_payloads(tmp_path)] == [1, 2, 3, 4]
    recovered.close()


def test_torn_tail_before_commit_is_dropped(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=100, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 4))
    writer.commit()
    write(writer, range(4, 8))
    writer._stream.flush()  # type: ignore[union-attr]
    active = segments_dir(tmp_path) / ACTIVE_NAME
    writer._stream.close()  # type: ignore[union-attr]
    writer._stream = None
    with active.open("r+b") as stream:  # tear the last record in half
        stream.truncate(active.stat().st_size - 5)
        stream.flush()
    recovered = SegmentWriter(tmp_path, commit_every_batches=100, commit_interval_seconds=5)
    result = recovered.open()
    assert result.committed_through == 3
    assert result.truncated_bytes > 0
    assert [b.local_batch_sequence for b in iter_payloads(tmp_path)] == [1, 2, 3]
    recovered.close()


def test_torn_commit_record_is_dropped(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=100, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 3))
    writer.commit()
    write(writer, range(3, 5))
    writer._stream.flush()  # type: ignore[union-attr]
    commit_offset = writer._stream.tell()  # type: ignore[union-attr]
    writer.commit()
    active = segments_dir(tmp_path) / ACTIVE_NAME
    writer._stream.close()  # type: ignore[union-attr]
    writer._stream = None
    with active.open("r+b") as stream:  # tear the last commit record
        stream.truncate(commit_offset + 3)
        stream.flush()
    recovered = SegmentWriter(tmp_path, commit_every_batches=100, commit_interval_seconds=5)
    result = recovered.open()
    assert result.committed_through == 2
    assert [b.local_batch_sequence for b in iter_payloads(tmp_path)] == [1, 2]
    recovered.close()


def test_rotation_and_cross_segment_reads(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=2, commit_interval_seconds=5,
                           segment_bytes=64 * 1024, segment_seconds=3600)
    writer.open()
    write(writer, range(1, 13))
    writer.commit()
    writer.rotate()
    write(writer, range(13, 18))
    writer.commit()
    writer.close()
    snapshot = load_snapshot(tmp_path)
    assert snapshot.committed_through == 17
    assert len(snapshot.entries) >= 1
    entries = read_index(tmp_path)
    assert entries
    assert entries[0].first_sequence == 1
    assert entries[-1].last_sequence == 17
    assert [b.local_batch_sequence for b in iter_payloads(tmp_path)] == list(range(1, 18))
    assert read_payload(tmp_path, 5).local_batch_sequence == 5  # type: ignore[union-attr]
    assert read_payload(tmp_path, 99) is None
    result = audit(tmp_path)
    assert result["committed_through"] == 17
    assert result["total_committed_records"] == 17
    # Footer digest must validate for every closed segment.
    for entry in entries:
        scan = scan_segment(segments_dir(tmp_path) / entry.file)
        assert scan.closed and scan.error is None and scan.digest


def test_missing_index_line_is_rebuilt(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=5, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 6))
    writer.close()
    index = segments_dir(tmp_path) / INDEX_NAME
    index.write_bytes(b"")  # crash between rename and index append
    assert read_index(tmp_path) == []
    recovered = SegmentWriter(tmp_path, commit_every_batches=5, commit_interval_seconds=5)
    result = recovered.open()
    assert "adopted_closed_segment:" in " ".join(result.notes)
    assert result.committed_through == 5
    assert read_index(tmp_path)[0].last_sequence == 5
    recovered.close()


def test_torn_final_index_line_is_ignored(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=5, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 6))
    writer.close()
    index = segments_dir(tmp_path) / INDEX_NAME
    with index.open("ab") as stream:
        stream.write(b'{"file":"seg-0000000000')
    assert len(read_index(tmp_path)) == 1
    assert load_snapshot(tmp_path).committed_through == 5


def test_stale_head_underreports_but_index_wins(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=5, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 6))
    writer.close()
    head = read_head(tmp_path)
    assert head is not None
    stale = dict(head, committed_through=0)
    (segments_dir(tmp_path) / HEAD_NAME).write_text(json.dumps(stale), encoding="utf-8")
    snapshot = load_snapshot(tmp_path)
    assert snapshot.committed_through == 5


def test_sequence_gap_is_rejected(tmp_path: Path):
    writer = SegmentWriter(tmp_path)
    writer.open()
    writer.append(make_batch(1))
    with pytest.raises(SegmentError) as failure:
        writer.append(make_batch(3))
    assert failure.value.code == "sequence_gap"
    writer.close()


def test_invalid_options_rejected(tmp_path: Path):
    for kwargs in ({"commit_every_batches": 0}, {"commit_every_batches": True},
                   {"commit_interval_seconds": -1}, {"commit_interval_seconds": float("nan")},
                   {"commit_interval_seconds": 10}, {"segment_bytes": 10},
                   {"segment_bytes": 2048 * 1024 * 1024}, {"segment_seconds": 0},
                   {"segment_seconds": float("inf")}):
        with pytest.raises(ValueError):
            SegmentWriter(tmp_path, **kwargs)


def test_layout_mismatch_rejected(tmp_path: Path):
    write_layout(tmp_path, "segments")
    assert read_layout(tmp_path) == "segments"
    payload = json.loads((tmp_path / ".layout").read_text(encoding="utf-8"))
    payload["mode"] = "batches"
    (tmp_path / ".layout").write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(SegmentError) as failure:
        SegmentWriter(tmp_path).open()
    assert failure.value.code == "layout_mismatch"


def test_short_garbage_active_is_truncated_on_open(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=5, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 3))
    writer.commit()
    writer._stream.close()  # type: ignore[union-attr]
    writer._stream = None
    active = segments_dir(tmp_path) / ACTIVE_NAME
    with active.open("ab") as stream:
        stream.write(b"\x00" * 7)
    recovered = SegmentWriter(tmp_path, commit_every_batches=5, commit_interval_seconds=5)
    result = recovered.open()
    assert result.committed_through == 2
    assert [b.local_batch_sequence for b in iter_payloads(tmp_path)] == [1, 2]
    recovered.close()


def test_corrupted_payload_record_fails_reads(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=5, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 4))
    writer.commit()
    writer._stream.close()  # type: ignore[union-attr]
    writer._stream = None
    active = segments_dir(tmp_path) / ACTIVE_NAME
    data = bytearray(active.read_bytes())
    data[HEADER_SIZE + PREFIX_SIZE + 10] ^= 0xFF  # flip a payload body byte
    active.write_bytes(data)
    with pytest.raises(SegmentError):
        list(iter_payloads(tmp_path))


def test_footer_digest_mismatch_detected(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=5, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 6))
    writer.close()
    entry = read_index(tmp_path)[0]
    segment = segments_dir(tmp_path) / entry.file
    data = bytearray(segment.read_bytes())
    offset = HEADER_SIZE
    while True:
        total, kind, _crc = struct.unpack("<IBI", bytes(data[offset:offset + PREFIX_SIZE]))
        if kind == KIND_FOOTER:
            break
        offset += total
    body_start = offset + PREFIX_SIZE
    digest_start = body_start + struct.calcsize("<QQQ")
    data[digest_start] ^= 0xFF
    # Rewrite the frame with a valid crc so only the digest is wrong.
    body = bytes(data[body_start:offset + total])
    framed = frame_record(KIND_FOOTER, body)
    data[offset:offset + total] = framed
    segment.write_bytes(data)
    assert scan_segment(segment).error == "invalid_footer_digest"


def test_close_finalizes_and_removes_active(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=10, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 4))
    assert writer.close() == 3
    assert not (segments_dir(tmp_path) / ACTIVE_NAME).exists()
    entries = read_index(tmp_path)
    assert len(entries) == 1 and entries[0].last_sequence == 3
    assert audit(tmp_path)["committed_through"] == 3


def test_duplicate_closed_segment_rejected(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=5, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 6))
    writer.close()
    entry = read_index(tmp_path)[0]
    target = segments_dir(tmp_path) / entry.file
    duplicate_entry = SegmentEntry(file=segment_name(1, 5), first_sequence=1,
                                   last_sequence=5, records=5,
                                   bytes=target.stat().st_size, closed_at_ns=0)
    append_index(tmp_path, duplicate_entry)
    with pytest.raises(SegmentError) as failure:
        SegmentWriter(tmp_path).open()
    assert failure.value.code == "sequence_gap"


def test_payload_records_are_zlib_compressed(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=1, commit_interval_seconds=5)
    writer.open()
    writer.append(make_batch(1))
    writer.commit()
    writer.close()
    entry = read_index(tmp_path)[0]
    segment = segments_dir(tmp_path) / entry.file
    payload = make_batch(1).model_dump_json().encode()
    import zlib

    compressed = zlib.compress(payload + b"\n", 1)
    raw = segment.read_bytes()
    assert compressed in raw or len(compressed) < len(payload) // 2
    assert len(raw) < len(payload) + 256  # header/frames only, no raw JSON
    assert read_payload(tmp_path, 1).model_dump_json().encode() == payload  # type: ignore[union-attr]


def test_head_carries_committed_offset_and_records(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=100, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 4))
    writer.commit()
    head = read_head(tmp_path)
    assert head is not None
    assert head["committed_through"] == 3
    assert head["committed_records"] == 3
    assert head["committed_offset"] > HEADER_SIZE
    committed_offset = head["committed_offset"]
    write(writer, range(4, 7))  # uncommitted tail
    head = read_head(tmp_path)
    assert head is not None and head["committed_offset"] == committed_offset
    assert [b.local_batch_sequence for b in iter_payloads(tmp_path)] == [1, 2, 3]
    writer.close()


def test_sparse_index_checkpoints_support_random_reads(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=2, commit_interval_seconds=5,
                           segment_bytes=64 * 1024, segment_seconds=3600)
    writer.open()
    write(writer, range(1, 21))
    writer.commit()
    writer.rotate()
    writer.close()
    entries = []
    for path in segments_dir(tmp_path).glob("seg-*"):
        entries.extend(read_segment_index(path))
    assert entries
    assert entries == sorted(entries)
    for sequence in (1, 2, 5, 9, 13, 18, 20):
        batch = read_payload(tmp_path, sequence)
        assert batch is not None and batch.local_batch_sequence == sequence


def test_torn_active_index_tail_is_ignored(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=2, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 3))
    writer.commit()
    index = segments_dir(tmp_path) / ".active.idx"
    with index.open("ab") as stream:
        stream.write(b"\x01\x02\x03")  # torn entry
    recovered = SegmentWriter(tmp_path, commit_every_batches=2, commit_interval_seconds=5)
    recovered.open()
    write(recovered, range(3, 5))
    recovered.commit()
    assert [b.local_batch_sequence for b in iter_payloads(tmp_path)] == [1, 2, 3, 4]
    recovered.close()


def test_commit_chain_tamper_detected(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=100, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 4))
    writer.commit()
    writer._stream.flush()  # type: ignore[union-attr]
    active = segments_dir(tmp_path) / ".active.l2s"
    offset = writer._stream.tell()  # type: ignore[union-attr]
    writer.commit = writer.commit  # noqa: PLW0127
    writer._stream.close()  # type: ignore[union-attr]
    writer._stream = None
    # Rewrite the commit frame with a valid crc but a bogus chain digest.
    data = bytearray(active.read_bytes())
    record_offset = HEADER_SIZE
    while True:
        total, kind, _crc = struct.unpack("<IBI", bytes(data[record_offset:record_offset + PREFIX_SIZE]))
        if kind == KIND_COMMIT:
            break
        record_offset += total
    body_start = record_offset + PREFIX_SIZE
    body = bytearray(data[body_start:record_offset + total])
    body[-1] ^= 0xFF
    framed = frame_record(KIND_COMMIT, bytes(body))
    data[record_offset:record_offset + total] = framed
    active.write_bytes(data)
    recovered = SegmentWriter(tmp_path, commit_every_batches=100, commit_interval_seconds=5)
    with pytest.raises(SegmentError) as failure:
        recovered.open()
    assert failure.value.code == "invalid_commit_chain"


def test_compressed_payload_corruption_detected(tmp_path: Path):
    writer = SegmentWriter(tmp_path, commit_every_batches=100, commit_interval_seconds=5)
    writer.open()
    write(writer, range(1, 3))
    writer.commit()
    writer._stream.flush()  # type: ignore[union-attr]
    writer._stream.close()  # type: ignore[union-attr]
    writer._stream = None
    active = segments_dir(tmp_path) / ".active.l2s"
    data = bytearray(active.read_bytes())
    body_start = HEADER_SIZE + PREFIX_SIZE
    body = bytearray(data[body_start:body_start + 40])
    body[5] ^= 0xFF
    framed = frame_record(KIND_PAYLOAD, bytes(body))
    data[HEADER_SIZE:HEADER_SIZE + PREFIX_SIZE + 40] = framed
    active.write_bytes(data)
    with pytest.raises(SegmentError):
        list(iter_payloads(tmp_path))
