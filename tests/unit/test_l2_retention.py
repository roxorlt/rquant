from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.unit.l2_cold_fixtures import build_cache, observations, payload_bytes

START = datetime(2026, 11, 3, 1, 30, tzinfo=UTC)


def retention():
    import rquant.l2.retention as module

    return module


def policy(**overrides):
    module = retention()
    values = dict(reserve_free_bytes=0, hot_keep_hours=0.5, hot_max_bytes=1 << 40,
                  cold_keep_days=30.0, cold_max_bytes=1 << 40,
                  archive_required=True, verify_segments=True)
    values.update(overrides)
    return module.RetentionPolicy(**values)


def archive_receipt(state_root: Path, last_sequence: int, status: str = "archived") -> Path:
    state_root.mkdir(parents=True, exist_ok=True)
    path = state_root / "last-run.json"
    path.write_text(json.dumps({
        "schema": "rquant-l2-archive-receipt-v1",
        "operation": "run",
        "status": status,
        "prefix": "l2/v1/rquant",
        "last_sequence": last_sequence,
    }))
    return state_root


def test_allocated_bytes_and_hot_usage(tmp_path: Path):
    module = retention()
    cache_root = build_cache(tmp_path, 12)
    usage = module.hot_usage(cache_root)
    assert usage.floor == 0 and usage.head == 12 and usage.batches == 12
    assert usage.logical_bytes > 0
    assert usage.allocated_bytes > usage.logical_bytes  # two 4 KiB inodes per batch
    assert module.allocated_bytes(cache_root / "batches") >= usage.allocated_bytes


def test_archive_coverage(tmp_path: Path):
    module = retention()
    state = tmp_path / "archive-state"
    assert module.archive_coverage(state) is None
    archive_receipt(state, 42)
    assert module.archive_coverage(state) == 42
    state.joinpath("last-run.json").write_text(json.dumps({"status": "failed", "last_sequence": 7}))
    assert module.archive_coverage(state) is None
    state.joinpath("last-run.json").write_text("not json")
    assert module.archive_coverage(state) is None


def test_plan_compacts_only_older_than_hot_window(tmp_path: Path):
    module = retention()
    cache_root = build_cache(tmp_path, 24)
    plan = module.plan_retention(
        cache_root, tmp_path / "cold", policy=policy(hot_keep_hours=0.0),
        archive_state_root=archive_receipt(tmp_path / "state", 24),
        now=START + timedelta(seconds=12),
    )
    assert plan.compact_range is not None
    assert (plan.compact_range.after_sequence, plan.compact_range.until_sequence) == (0, 12)
    assert plan.prune_hot is False  # compaction has not run yet
    assert plan.archived_through == 24


def test_full_cycle_compacts_prunes_and_keeps_reading(tmp_path: Path):
    module = retention()
    from rquant.l2.cache import RawL2Cache, read_floor

    cache_root = build_cache(tmp_path, 24)
    expected = {sequence: payload_bytes(cache_root, sequence) for sequence in range(1, 25)}
    state = archive_receipt(tmp_path / "state", 24)
    cold_root = tmp_path / "cold"
    now = START + timedelta(hours=1)
    report = module.run_retention(
        cache_root, cold_root, policy=policy(), archive_state_root=state, now=now,
    )
    assert report.compacted_records == 24
    assert report.pruned_batches == 23
    assert report.pruned_through == 23
    assert report.evicted_segments == 0
    assert read_floor(cache_root) == 23
    # The newest batch stays hot so the published .head never dangles.
    assert [path.name for path in (cache_root / "batches").glob("0*")] == [f"{24:020d}"]
    # All payloads still come back byte-identically from the cold store.
    from rquant.l2.cold_store import load_manifest, restore

    assert len(load_manifest(cold_root).segments) >= 1
    destination = tmp_path / "restored"
    restore(cold_root, destination)
    for sequence in range(1, 25):
        assert payload_bytes(destination, sequence) == expected[sequence]
    # The live reader survives the retired prefix and shows nothing older.
    page = RawL2Cache(cache_root).read_batches(after_sequence=0, limit=5)
    assert [batch.local_batch_sequence for batch in page.batches] == [24]
    # The writer resumes after the floor on restart.
    with RawL2Cache(cache_root).writer() as cache:
        appended = cache.append(observations(1, start=START + timedelta(hours=2))[0])
    assert appended.local_batch_sequence == 25
    assert RawL2Cache(cache_root).read_status().last_sequence == 25


def test_missing_archive_blocks_prune(tmp_path: Path):
    module = retention()
    cache_root = build_cache(tmp_path, 16)
    report = module.run_retention(
        cache_root, tmp_path / "cold", policy=policy(), now=START + timedelta(hours=1),
    )
    assert report.compacted_records == 16
    assert report.pruned_batches == 0
    assert "archive_state_missing" in report.plan.blockers
    assert (cache_root / "batches" / f"{1:020d}").is_dir()


def test_archive_lag_clamps_prune_then_progresses(tmp_path: Path):
    module = retention()
    cache_root = build_cache(tmp_path, 20)
    cold_root = tmp_path / "cold"
    state = archive_receipt(tmp_path / "state", 8)
    now = START + timedelta(hours=1)
    first = module.run_retention(cache_root, cold_root, policy=policy(), archive_state_root=state, now=now)
    assert first.pruned_through == 8
    assert first.pruned_batches == 8
    archive_receipt(state, 20)
    second = module.run_retention(
        cache_root, cold_root, policy=policy(), archive_state_root=state,
        now=now + timedelta(minutes=1),
    )
    assert second.pruned_through == 19  # newest batch stays for the published head
    assert second.pruned_batches == 11
    from rquant.l2.cache import read_floor

    assert read_floor(cache_root) == 19


def test_writer_active_skips_prune(tmp_path: Path):
    module = retention()
    from rquant.l2.cache import RawL2Cache

    cache_root = build_cache(tmp_path, 12)
    state = archive_receipt(tmp_path / "state", 12)
    cold_root = tmp_path / "cold"
    cache = RawL2Cache(cache_root)
    with cache.writer():
        report = module.run_retention(
            cache_root, cold_root, policy=policy(), archive_state_root=state,
            now=START + timedelta(hours=1),
        )
    assert report.plan.writer_active is True
    assert report.pruned_batches == 0
    assert report.compacted_records == 12
    assert (cache_root / "batches" / f"{1:020d}").is_dir()


def test_unverified_cold_store_is_not_pruned(tmp_path: Path):
    module = retention()
    from rquant.l2.cold_store import compact_cache, load_manifest

    cache_root = build_cache(tmp_path, 12)
    cold_root = tmp_path / "cold"
    compact_cache(cache_root, cold_root, verify=False)
    assert load_manifest(cold_root).segments[0].verified_at is None
    report = module.run_retention(
        cache_root, cold_root, policy=policy(), archive_state_root=archive_receipt(tmp_path / "state", 12),
        now=START + timedelta(hours=1),
    )
    assert report.pruned_batches == 0
    assert report.compacted_records == 0  # already covered, no duplicate segment
    assert (cache_root / "batches" / f"{1:020d}").is_dir()


def test_second_run_is_a_noop(tmp_path: Path):
    module = retention()
    cache_root = build_cache(tmp_path, 12)
    state = archive_receipt(tmp_path / "state", 12)
    cold_root = tmp_path / "cold"
    now = START + timedelta(hours=1)
    module.run_retention(cache_root, cold_root, policy=policy(), archive_state_root=state, now=now)
    second = module.run_retention(
        cache_root, cold_root, policy=policy(), archive_state_root=state, now=now,
    )
    assert second.compacted_records == 0
    assert second.pruned_batches == 0
    assert second.evicted_segments == 0


def test_dry_run_makes_no_changes(tmp_path: Path):
    module = retention()
    from rquant.l2.cache import read_floor

    cache_root = build_cache(tmp_path, 12)
    state = archive_receipt(tmp_path / "state", 12)
    cold_root = tmp_path / "cold"
    report = module.run_retention(
        cache_root, cold_root, policy=policy(), archive_state_root=state,
        now=START + timedelta(hours=1), dry_run=True,
    )
    assert report.dry_run is True
    assert report.compacted_records == 0 and report.pruned_batches == 0
    assert read_floor(cache_root) == 0
    assert (cache_root / "batches" / f"{1:020d}").is_dir()
    assert not (cold_root / "manifest.json").exists()


def test_plan_cold_eviction_respects_age_and_reserve(tmp_path: Path):
    module = retention()
    from rquant.l2.cold_store import SegmentEntry

    def entry(file: str, first: int, last: int, created: datetime, verified: datetime | None,
              size: int) -> SegmentEntry:
        return SegmentEntry(
            file=file, first_sequence=first, last_sequence=last, records=last - first + 1,
            stored_bytes=size, body_bytes=size, sha256="0" * 64, codec="zlib",
            block_records=512, blocks=1, created_at=created,
            verified_at=verified, source_cache_root="/cache",
        )

    now = datetime(2026, 11, 3, 12, tzinfo=UTC)
    old = entry("segments/l2cold-00000000000000000001-00000000000000000010-aaaaaaaaaaaa.l2c",
                1, 10, now - timedelta(days=90), now - timedelta(days=90), 1000)
    young = entry("segments/l2cold-00000000000000000011-00000000000000000020-bbbbbbbbbbbb.l2c",
                  11, 20, now - timedelta(days=1), now - timedelta(days=1), 1000)
    unverified = entry("segments/l2cold-00000000000000000021-00000000000000000030-cccccccccccc.l2c",
                       21, 30, now - timedelta(days=90), None, 1000)
    module_policy = policy(cold_keep_days=30.0, cold_max_bytes=1500)
    notes: list[str] = []
    eviction = module._plan_cold_eviction(
        (old, young, unverified), policy=module_policy, now=now,
        disk=module.DiskUsage(total_bytes=10**12, free_bytes=10**11),
        incoming_bytes=0, notes=notes, hot_floor=1000,
    )
    assert eviction is not None
    # The hard cap wins: the older segment goes first, then the young one.
    assert eviction.files == (old.file, young.file)
    assert eviction.freed_bytes == 2000
    assert any("younger than cold_keep_days" in note for note in notes)
    assert not any("cap still exceeded" in note for note in notes)
    # Reserve floor alone can also trigger eviction of the oldest eligible segment.
    eviction = module._plan_cold_eviction(
        (old, young, unverified), policy=policy(cold_keep_days=0.0, cold_max_bytes=10**12,
                                                reserve_free_bytes=100_000),
        now=now, disk=module.DiskUsage(total_bytes=10**12, free_bytes=500),
        incoming_bytes=0, notes=notes, hot_floor=1000,
    )
    assert eviction is not None and eviction.files[0] == old.file
    # Unverified segments were never candidates.
    assert unverified.file not in eviction.files


def test_evict_cold_segments_updates_manifest(tmp_path: Path):
    module = retention()
    from rquant.l2.cold_store import ColdManifest, _write_manifest, compact_cache, load_manifest

    cache_root = build_cache(tmp_path, 12)
    cold_root = tmp_path / "cold"
    compact_cache(cache_root, cold_root, block_records=2, segment_bytes=3_000)
    # First retire the prefix with a normal cycle, then age the manifest.
    state = archive_receipt(tmp_path / "state", 12)
    module.run_retention(cache_root, cold_root, policy=policy(), archive_state_root=state,
                         now=START + timedelta(hours=1))
    manifest = load_manifest(cold_root)
    assert len(manifest.segments) >= 2
    old = [entry.model_copy(update={"created_at": START - timedelta(days=90),
                                    "verified_at": START - timedelta(days=90)})
           for entry in manifest.segments]
    _write_manifest(cold_root, ColdManifest(segments=tuple(old)))
    report = module.run_retention(
        cache_root, cold_root,
        policy=policy(cold_keep_days=30.0, cold_max_bytes=manifest.segments[0].stored_bytes),
        archive_state_root=state,
        now=START + timedelta(hours=2),
    )
    assert report.evicted_segments >= 1
    remaining = load_manifest(cold_root).segments
    assert len(remaining) == len(manifest.segments) - report.evicted_segments
    assert all((cold_root / entry.file).is_file() for entry in remaining)


def test_prune_refuses_live_writer_lock(tmp_path: Path):
    module = retention()
    from rquant.l2.cache import RawL2Cache

    cache_root = build_cache(tmp_path, 4)
    cache = RawL2Cache(cache_root)
    with cache.writer():
        assert module._writer_active(cache_root) is True
        with pytest.raises(module.RetentionError) as caught:
            module._prune_hot_prefix(cache_root, 2, reason="test", now=START)
        assert caught.value.code == "writer_active"
    assert module._writer_active(cache_root) is False


def test_prune_cleans_interrupted_leftovers(tmp_path: Path):
    module = retention()
    from rquant.l2.cache import write_floor

    cache_root = build_cache(tmp_path, 12)
    # Simulate a crash after the floor marker was published but before deletion.
    write_floor(cache_root, 5, reason="interrupted", now=START)
    assert (cache_root / "batches" / f"{1:020d}").is_dir()
    deleted, allocated = module._prune_hot_prefix(cache_root, 5, reason="retry", now=START)
    assert deleted == 5
    assert allocated > 0
    assert not (cache_root / "batches" / f"{1:020d}").exists()
    assert (cache_root / "batches" / f"{6:020d}").is_dir()
    # Idempotent: running again finds nothing to delete.
    assert module._prune_hot_prefix(cache_root, 5, reason="retry", now=START) == (0, 0)


def test_stale_published_head_is_never_pruned(tmp_path: Path):
    """A crash can leave .head pointing at an older batch than the newest one."""
    module = retention()
    import os

    from rquant.l2.cache import RawL2Cache, read_floor

    cache_root = build_cache(tmp_path, 12)
    # Publish batch 1..11 but leave batch 12 committed and unreferenced by .head.
    head = cache_root / "batches" / ".head"
    head.unlink()
    os.link(cache_root / "batches" / f"{11:020d}" / "batch.json", head)
    state = archive_receipt(tmp_path / "state", 12)
    report = module.run_retention(
        cache_root, tmp_path / "cold", policy=policy(), archive_state_root=state,
        now=START + timedelta(hours=1),
    )
    assert report.pruned_through <= 10
    assert read_floor(cache_root) <= 10
    assert (cache_root / "batches" / f"{11:020d}").is_dir()
    assert (cache_root / "batches" / f"{12:020d}").is_dir()
    # The reader and a fresh writer both still work.
    assert RawL2Cache(cache_root).read_batches(after_sequence=10, limit=1).batches[0].local_batch_sequence == 11
    with RawL2Cache(cache_root).writer() as writer:
        appended = writer.append(observations(1, start=START + timedelta(hours=2))[0])
    assert appended.local_batch_sequence == 13


def test_low_disk_does_not_deadlock_on_compaction(tmp_path: Path):
    """Cold eviction runs before compaction; a refused compaction is reported."""
    module = retention()
    from rquant.l2.cache import write_floor
    from rquant.l2.cold_store import compact_cache

    cache_root = build_cache(tmp_path, 24)
    cold_root = tmp_path / "cold"
    compact_cache(cache_root, cold_root, until_sequence=10)
    write_floor(cache_root, 10, reason="test", now=START)
    import shutil

    for sequence in range(1, 11):
        shutil.rmtree(cache_root / "batches" / f"{sequence:020d}")
    state = archive_receipt(tmp_path / "state", 24)
    report = module.run_retention(
        cache_root, cold_root,
        policy=policy(reserve_free_bytes=1 << 62, hot_keep_hours=0.0),
        archive_state_root=state, now=START + timedelta(hours=1),
    )
    assert "disk_budget" in report.plan.blockers
    # The retired cold segment was evicted before the refused compaction write.
    assert report.evicted_segments >= 1


def test_missing_cold_segment_blocks_prune(tmp_path: Path):
    module = retention()
    from rquant.l2.cold_store import compact_cache, load_manifest

    cache_root = build_cache(tmp_path, 12)
    cold_root = tmp_path / "cold"
    module.run_retention(cache_root, cold_root, policy=policy(),
                         archive_state_root=archive_receipt(tmp_path / "state", 12),
                         now=START + timedelta(hours=1))
    # Drop the segment behind the manifest's back; coverage must shrink and the
    # next pass must repair the manifest instead of raising sequence_overlap.
    segment = load_manifest(cold_root).segments[0]
    (cold_root / segment.file).unlink()
    plan = module.plan_retention(
        cache_root, cold_root, policy=policy(),
        archive_state_root=archive_receipt(tmp_path / "state2", 12),
        now=START + timedelta(hours=2),
    )
    assert plan.prune_hot is False
    report = module.run_retention(
        cache_root, cold_root, policy=policy(),
        archive_state_root=archive_receipt(tmp_path / "state2", 12),
        now=START + timedelta(hours=2),
    )
    assert report.pruned_batches == 0
    assert (cache_root / "batches" / f"{12:020d}").is_dir()


def test_hot_boundary_tolerates_non_monotonic_receive_times(tmp_path: Path):
    module = retention()
    from rquant.l2.cache import RawL2Cache
    from tests.unit.l2_cold_fixtures import observations

    cache_root = tmp_path / "cache"
    cache = RawL2Cache(cache_root)
    ordered = observations(10)
    with cache.writer():
        for index, observation in enumerate(ordered):
            # Reverse the receive order so binary search on sequence is invalid.
            cache.append(observation.model_copy(update={
                "received_at": START + timedelta(seconds=10 - index)}))
    boundary = module._hot_boundary(cache_root, START + timedelta(seconds=5))
    # The conservative forward scan stops at the first batch newer than the cutoff.
    assert boundary == 0
