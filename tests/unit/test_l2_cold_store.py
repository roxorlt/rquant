from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.unit.l2_cold_fixtures import build_cache, payload_bytes, payload_sha


def cold_store():
    import rquant.l2.cold_store as module

    return module


def test_roundtrip_is_byte_identical_and_shrinks(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path)
    cold_root = tmp_path / "cold"
    result = module.compact_cache(
        cache_root, cold_root, block_records=4, segment_bytes=12_000, verify=True,
    )
    assert result.records == 24 and result.verified
    assert result.first_sequence == 1 and result.last_sequence == 24
    assert len(result.segments) >= 2
    assert result.stored_bytes < result.source_batch_bytes / 4
    manifest = module.load_manifest(cold_root)
    assert len(manifest.segments) == len(result.segments)
    assert all(entry.verified_at is not None for entry in manifest.segments)
    destination = tmp_path / "restored"
    restored = module.restore(cold_root, destination)
    assert restored.records == 24
    for sequence in range(1, 25):
        assert payload_bytes(destination, sequence) == payload_bytes(cache_root, sequence)
        assert (destination / "batches" / f"{sequence:020d}" / "sha256").read_bytes() == (
            payload_sha(cache_root, sequence).encode() + b"\n")
    # The restored tree is a usable cache.
    from rquant.l2.cache import RawL2Cache

    page = RawL2Cache(destination).read_batches(after_sequence=0, limit=8)
    assert len(page.batches) == 8 and page.batches[0].local_batch_sequence == 1
    assert page.batches[-1].model_dump_json().encode() + b"\n" == payload_bytes(cache_root, 8)


def test_iter_batches_and_random_access(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 12)
    cold_root = tmp_path / "cold"
    module.compact_cache(cache_root, cold_root, block_records=5, verify=True)
    batches = list(module.iter_batches(cold_root))
    assert [batch.local_batch_sequence for batch in batches] == list(range(1, 13))
    assert all(
        batch.model_dump_json().encode() + b"\n" == payload_bytes(cache_root, batch.local_batch_sequence)
        for batch in batches)
    for sequence in (1, 5, 6, 12):
        batch = module.read_batch(cold_root, sequence)
        assert batch.model_dump_json().encode() + b"\n" == payload_bytes(cache_root, sequence)


def test_partial_compaction_restores_floor(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 20)
    cold_root = tmp_path / "cold"
    result = module.compact_cache(cache_root, cold_root, after_sequence=8, until_sequence=16)
    assert (result.first_sequence, result.last_sequence) == (9, 16)
    destination = tmp_path / "restored"
    restored = module.restore(cold_root, destination)
    assert (restored.first_sequence, restored.last_sequence) == (9, 16)
    from rquant.l2.cache import RawL2Cache, read_floor

    assert read_floor(destination) == 8
    page = RawL2Cache(destination).read_batches(after_sequence=0, limit=3)
    assert [batch.local_batch_sequence for batch in page.batches] == [9, 10, 11]
    assert RawL2Cache(destination).floor_sequence() == 8


def test_codecs_roundtrip(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 8)
    for codec in ("none", "zlib", "gzip"):
        cold_root = tmp_path / f"cold-{codec}"
        result = module.compact_cache(cache_root, cold_root, codec=codec, block_records=3)
        assert result.stored_bytes > 0
        destination = tmp_path / f"restored-{codec}"
        module.restore(cold_root, destination)
        for sequence in range(1, 9):
            assert payload_bytes(destination, sequence) == payload_bytes(cache_root, sequence)


def test_verify_detects_corruption(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 10)
    cold_root = tmp_path / "cold"
    result = module.compact_cache(cache_root, cold_root, block_records=4)
    segment_path = result.segments[0].path
    original = bytearray(segment_path.read_bytes())
    original[len(original) // 2] ^= 0xFF
    segment_path.write_bytes(bytes(original))
    with pytest.raises(module.ColdStoreError) as caught:
        module.open_segment(segment_path)
    assert caught.value.code == "segment_corrupt"
    segment_path.write_bytes(bytes(original[: len(original) - 8]))
    with pytest.raises(module.ColdStoreError):
        module.open_segment(segment_path)


def test_verify_detects_source_payload_mismatch(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 6)
    cold_root = tmp_path / "cold"
    result = module.compact_cache(cache_root, cold_root, block_records=6)
    # Replace the source payload of an unrelated committed sequence.
    other_root = build_cache(tmp_path / "other", 6, epoch="epoch_cold_b")
    with pytest.raises(module.ColdStoreError) as caught:
        module.verify_segment(result.segments[0].path, cache_root=other_root)
    assert caught.value.code == "payload_mismatch"
    assert module.verify_segment(result.segments[0].path, cache_root=cache_root)["records"] == 6


def test_sequence_gap_rejected_by_writer(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 6)
    from rquant.l2.cache import RawL2Cache

    cache = RawL2Cache(cache_root)
    batches = list(cache.iter_batches())
    writer = module._SegmentWriter(
        tmp_path / "cold", codec="zlib", level=6, block_records=4,
        source_cache_root=str(cache_root), created_at=datetime.now(UTC),
        reserve_bytes=0,
    )
    writer.add(batches[0])
    with pytest.raises(module.ColdStoreError) as caught:
        writer.add(batches[2])
    assert caught.value.code == "sequence_gap"
    writer._discard()


def test_manifest_rejects_overlap_and_stats(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 8)
    cold_root = tmp_path / "cold"
    first = module.compact_cache(cache_root, cold_root, after_sequence=0, until_sequence=4, block_records=2)
    stats = module.stats(cold_root)
    assert stats["segments"] == 1 and stats["records"] == 4
    with pytest.raises(module.ColdStoreError) as caught:
        module.compact_cache(cache_root, cold_root, after_sequence=2, until_sequence=6, block_records=2)
    assert caught.value.code == "sequence_overlap"
    assert first.records == 4


def test_empty_range_is_a_noop(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 4)
    result = module.compact_cache(cache_root, tmp_path / "cold", until_sequence=0)
    assert result.records == 0 and result.segments == ()
    with pytest.raises(module.ColdStoreError) as caught:
        module.compact_cache(cache_root, tmp_path / "cold", after_sequence=2, until_sequence=1)
    assert caught.value.code == "invalid_range"


def test_disk_budget_refuses_when_under_reserve(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 4)
    with pytest.raises(module.ColdStoreError) as caught:
        module.compact_cache(cache_root, tmp_path / "cold", reserve_bytes=1 << 62)
    assert caught.value.code == "disk_budget"


def test_restore_refuses_non_empty_destination(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 4)
    cold_root = tmp_path / "cold"
    module.compact_cache(cache_root, cold_root, block_records=4)
    destination = tmp_path / "occupied"
    destination.mkdir()
    (destination / "keep").write_text("x")
    with pytest.raises(module.ColdStoreError) as caught:
        module.restore(cold_root, destination)
    assert caught.value.code == "destination_not_empty"


def test_manifest_json_is_valid_and_verified_entry_has_source(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 6)
    cold_root = tmp_path / "cold"
    module.compact_cache(cache_root, cold_root, block_records=3)
    raw = json.loads((cold_root / "manifest.json").read_bytes())
    assert raw["schema"] == "rquant-l2-cold-manifest-v1"
    entry = raw["segments"][0]
    assert entry["source_cache_root"] == str(cache_root)
    assert entry["verified_at"] and entry["records"] > 0
    assert hashlib.sha256((cold_root / entry["file"]).read_bytes()).hexdigest()
    assert entry["file"].startswith("segments/l2cold-")


def test_unverified_store_is_not_eligible_for_eviction(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 4)
    cold_root = tmp_path / "cold"
    result = module.compact_cache(cache_root, cold_root, block_records=2, verify=False)
    manifest = module.load_manifest(cold_root)
    assert manifest.segments[0].verified_at is None
    assert result.verified is False


def test_unverified_segment_can_be_verified_later_and_pruned(tmp_path: Path):
    module = cold_store()
    retention = pytest.importorskip("rquant.l2.retention")
    cache_root = build_cache(tmp_path, 12)
    cold_root = tmp_path / "cold"
    module.compact_cache(cache_root, cold_root, block_records=4, verify=False)
    entry = module.load_manifest(cold_root).segments[0]
    assert entry.verified_at is None
    assert module.mark_verified(cold_root, [entry.file]) == 1
    assert module.load_manifest(cold_root).segments[0].verified_at is not None
    assert module.mark_verified(cold_root, [entry.file]) == 0  # already verified
    archive_root = tmp_path / "state"
    archive_root.mkdir()
    (archive_root / "last-run.json").write_text(json.dumps({
        "schema": "rquant-l2-archive-receipt-v1", "operation": "run", "status": "archived",
        "prefix": "l2/v1/rquant", "last_sequence": 12,
    }))
    report = retention.run_retention(
        cache_root, cold_root,
        policy=retention.RetentionPolicy(reserve_free_bytes=0, hot_keep_hours=0.5,
                                         archive_required=True),
        archive_state_root=archive_root,
        now=datetime(2026, 11, 3, 2, 30, tzinfo=UTC),
    )
    assert report.pruned_batches == 11
    with pytest.raises(module.ColdStoreError):
        module.mark_verified(cold_root, ["segments/l2cold-00000000000000000001-00000000000000000002-000000000000.l2c"])


def test_manifest_rejects_path_traversal(tmp_path: Path):
    module = cold_store()
    cache_root = build_cache(tmp_path, 4)
    cold_root = tmp_path / "cold"
    module.compact_cache(cache_root, cold_root, block_records=4)
    raw = json.loads((cold_root / "manifest.json").read_text())
    raw["segments"][0]["file"] = "../evil.l2c"
    (cold_root / "manifest.json").write_text(json.dumps(raw))
    with pytest.raises(module.ColdStoreError) as caught:
        module.load_manifest(cold_root)
    assert caught.value.code == "invalid_manifest"
