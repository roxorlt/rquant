"""RawL2Cache segmented write backend integration tests."""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from rquant.l2.cache import CacheError, RawL2Cache, read_floor, write_floor
from rquant.l2.models import MarketObservation
from rquant.l2.segment_log import layout_path
from tests.unit.test_l2_cache import observed_trade_push_data


def observation(offset: int = 0) -> MarketObservation:
    data = observed_trade_push_data()
    data["received_at"] = data["received_at"] + timedelta(seconds=offset)
    return MarketObservation.model_validate(data)


def segments_cache(root: Path) -> RawL2Cache:
    return RawL2Cache(root, write_mode="segments")


def write_observations(cache: RawL2Cache, start: int, count: int) -> None:
    for index in range(count):
        cache.append(observation(start + index))


def test_segments_writer_roundtrip(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    writer = segments_cache(root)
    with writer.writer():
        write_observations(writer, 0, 5)
        writer.write_status(writer.read_status())
    assert layout_path(root).exists()
    assert not (root / "batches").exists()

    reader = segments_cache(root)
    page = reader.read_batches(after_sequence=0, limit=3)
    assert [batch.local_batch_sequence for batch in page.batches] == [1, 2, 3]
    assert page.next_sequence == 3
    assert page.status.last_sequence == 5
    tail = reader.read_batches(after_sequence=3, limit=10)
    assert [batch.local_batch_sequence for batch in tail.batches] == [4, 5]
    assert [b.local_batch_sequence for b in reader.iter_batches(after_sequence=1)] == [2, 3, 4, 5]
    assert [b.local_batch_sequence for b in reader.iter_batches(after_sequence=0, limit=2)] == [1, 2]
    batch = reader.read_one(4)
    assert batch is not None and batch.local_batch_sequence == 4
    assert reader.read_one(99) is None
    stored = reader.read_payload_bytes(2)
    assert stored is not None
    model = reader.read_one(2)
    assert model is not None and stored == model.model_dump_json().encode() + b"\n"
    assert reader.committed_head()[0] == 5


def test_uncommitted_segment_tail_is_invisible(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    writer = segments_cache(root)
    with writer.writer():
        write_observations(writer, 0, 3)
        reader = segments_cache(root)
        assert reader.read_batches(after_sequence=0, limit=5).batches == ()
        writer.write_status(writer.read_status())
        visible = reader.read_batches(after_sequence=0, limit=5)
        assert [b.local_batch_sequence for b in visible.batches] == [1, 2, 3]


def test_crash_without_commit_is_recovered(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    writer = segments_cache(root)
    with pytest.raises(RuntimeError):
        with writer.writer():
            write_observations(writer, 0, 3)
            writer.write_status(writer.read_status())
            write_observations(writer, 3, 3)  # never committed
            raise RuntimeError("crash")
    reader = segments_cache(root)
    assert [b.local_batch_sequence for b in reader.read_batches(after_sequence=0, limit=10).batches] == [1, 2, 3]
    recovered = segments_cache(root)
    with recovered.writer():
        recovered.append(observation(6))
        recovered.write_status(recovered.read_status())
    assert [b.local_batch_sequence for b in segments_cache(root).iter_batches()] == [1, 2, 3, 4]


def test_floor_retires_prefix_in_segments_layout(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    writer = segments_cache(root)
    with writer.writer():
        write_observations(writer, 0, 4)
        writer.write_status(writer.read_status())
    write_floor(root, 2, reason="test")
    assert read_floor(root) == 2
    reader = segments_cache(root)
    page = reader.read_batches(after_sequence=0, limit=10)
    assert [b.local_batch_sequence for b in page.batches] == [3, 4]


def test_layout_mismatch_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    with segments_cache(root).writer():
        pass
    assert layout_path(root).exists()
    with pytest.raises(CacheError) as failure:
        RawL2Cache(root, write_mode="batches")
    assert failure.value.code == "layout_mismatch"
    RawL2Cache(root)  # auto-detects segments


def test_status_restart_gap_is_written_for_segments(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    writer = segments_cache(root)
    with writer.writer():
        writer.append(observation())
        writer.write_status(writer.read_status().model_copy(update={"state": "listening_candidate"}))
    reopened = segments_cache(root)
    with reopened.writer():
        status = reopened.read_status()
        assert status.state == "disconnected" and status.gap and status.error_code == "restart_gap"
