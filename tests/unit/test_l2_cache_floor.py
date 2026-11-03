from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.unit.l2_cold_fixtures import build_cache, payload_bytes

START = datetime(2026, 11, 3, 1, 30, tzinfo=UTC)


def cache_module():
    import rquant.l2.cache as module

    return module


def test_floor_marker_paginates_after_retired_prefix(tmp_path: Path):
    module = cache_module()
    cache_root = build_cache(tmp_path, 20)
    module.write_floor(cache_root, 7, reason="test", now=START)
    assert module.read_floor(cache_root) == 7
    cache = module.RawL2Cache(cache_root)
    assert cache.floor_sequence() == 7
    page = cache.read_batches(after_sequence=0, limit=3)
    assert [batch.local_batch_sequence for batch in page.batches] == [8, 9, 10]
    assert page.next_sequence == 10
    assert [batch.local_batch_sequence for batch in cache.iter_batches()] == list(range(8, 21))
    assert [batch.local_batch_sequence for batch in cache.iter_batches(after_sequence=18)] == [19, 20]
    # A cursor inside the retired prefix resumes at the oldest surviving batch.
    assert cache.read_batches(after_sequence=3, limit=2).batches[0].local_batch_sequence == 8


def test_writer_recovery_continues_after_floor(tmp_path: Path):
    from tests.unit.l2_cold_fixtures import observations

    module = cache_module()
    cache_root = build_cache(tmp_path, 12)
    module.write_floor(cache_root, 9, reason="test", now=START)
    cache = module.RawL2Cache(cache_root)
    with cache.writer() as writer:
        appended = writer.append(observations(1, start=START + timedelta(hours=1))[0])
    assert appended.local_batch_sequence == 13
    assert module.RawL2Cache(cache_root).read_status().last_sequence == 13
    reader = module.RawL2Cache(cache_root)
    assert [batch.local_batch_sequence for batch in reader.iter_batches()] == list(range(10, 14))


def test_floor_survives_restart_gap_status(tmp_path: Path):
    module = cache_module()
    cache_root = build_cache(tmp_path, 8)
    module.write_floor(cache_root, 5, reason="test", now=START)
    with module.RawL2Cache(cache_root).writer():
        pass
    status = module.RawL2Cache(cache_root).read_status()
    assert status.state == "disconnected" and status.gap is True
    page = module.RawL2Cache(cache_root).read_batches(after_sequence=0, limit=2)
    assert [batch.local_batch_sequence for batch in page.batches] == [6, 7]


def test_missing_and_invalid_floor_markers(tmp_path: Path):
    module = cache_module()
    cache_root = build_cache(tmp_path, 4)
    assert module.read_floor(cache_root) == 0
    floor_path = cache_root / "batches" / module.FLOOR_NAME
    floor_path.write_text("not json")
    with pytest.raises(module.CacheError) as caught:
        module.read_floor(cache_root)
    assert caught.value.code == "invalid_cache"
    floor_path.write_text('{"schema": "x", "pruned_through": -1}')
    with pytest.raises(module.CacheError):
        module.read_floor(cache_root)
    floor_path.write_text('{"schema": "x", "pruned_through": 99}')
    with pytest.raises(module.CacheError) as caught:
        module.RawL2Cache(cache_root).read_batches(after_sequence=0, limit=1)
    assert caught.value.code == "invalid_cache"


def test_write_floor_rejects_invalid_sequences(tmp_path: Path):
    module = cache_module()
    cache_root = build_cache(tmp_path, 2)
    with pytest.raises(ValueError):
        module.write_floor(cache_root, 0, reason="test")
    with pytest.raises(ValueError):
        module.write_floor(cache_root, True, reason="test")


def test_archive_source_tolerates_retired_prefix(tmp_path: Path):
    from rquant.l2.archive import ArchiveError, Catalog, SourceObservation, _source

    module = cache_module()
    cache_root = build_cache(tmp_path, 12)
    first_sha = hashlib.sha256(payload_bytes(cache_root, 1)).hexdigest()
    last_sha = hashlib.sha256(payload_bytes(cache_root, 8)).hexdigest()
    catalog = Catalog(
        last_sequence=8,
        source_first_batch_sha256=first_sha,
        source_last_batch_sha256=last_sha,
        source_observation=SourceObservation(state="stopped", epoch_id="epoch_cold_a",
                                             gap=True, error_code=None),
        days={},
    )
    cache = module.RawL2Cache(cache_root)
    # Nothing pruned yet: the digest checks pass against the live batches.
    assert _source(cache, catalog, 12) == (first_sha, hashlib.sha256(
        payload_bytes(cache_root, 12)).hexdigest())
    # Retire through the archived boundary; batch 1 and batch 8 are gone.
    module.write_floor(cache_root, 8, reason="test", now=START)
    import shutil

    for sequence in range(1, 9):
        shutil.rmtree(cache_root / "batches" / f"{sequence:020d}")
    assert _source(cache, catalog, 12) == (first_sha, hashlib.sha256(
        payload_bytes(cache_root, 12)).hexdigest())
    # A floor beyond the archive progress means archive lost source data.
    module.write_floor(cache_root, 9, reason="test", now=START)
    with pytest.raises(ArchiveError) as caught:
        _source(cache, catalog, 12)
    assert caught.value.code == "source_truncated"
