"""Rolling retention policy for the L2 hot cache and cold segments.

The live cache grows one directory per batch (``batch.json`` + a 65-byte
checksum sidecar).  On the production host that is ~8 KiB of allocation per
batch, so a 40-stock opening burst alone writes ~1.4 GiB in 20 minutes and the
~29 GiB free disk fills within days.  The cold store shrinks the retired bytes
by roughly two orders of magnitude; this module decides *what* may be retired,
*when* it is safe, and *how much* disk must stay free.

Rules (all enforced by :func:`run_retention`):

1. **Compact before prune.** A hot prefix is compacted into verified cold
   segments first; the hot directories are deleted only afterwards.
2. **Archive before prune.** By default the prefix must also be covered by the
   last successful R2 archive receipt, because the hot cache is the only copy
   until then.
3. **Never prune a live writer.** Pruning takes the cache writer lock with a
   non-blocking exclusive flock; if the collector is running, pruning is
   skipped (the collector appends only at the head, so compaction itself is
   safe while it runs).
4. **Never cross the reserve.** Compaction and eviction stop when the free
   space would drop below ``reserve_free_bytes`` (the server's emergency
   headroom, default 6 GiB; the cache append path already refuses to write
   below 5 GiB).
5. **Never silently drop unverified or young cold data.** Only manifest
   segments with ``verified_at`` older than ``cold_keep_days`` are eviction
   candidates, oldest first.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field

from .cache import RawL2Cache, read_floor, write_floor
from .cold_store import (
    ColdManifest,
    ColdStoreError,
    SegmentEntry,
    _write_manifest,
    compact_cache,
    load_manifest,
)

DEFAULT_RESERVE_BYTES = 6 * 1024**3
DEFAULT_HOT_KEEP_HOURS = 2.0
DEFAULT_HOT_MAX_BYTES = 12 * 1024**3
DEFAULT_COLD_KEEP_DAYS = 30.0
DEFAULT_COLD_MAX_BYTES = 6 * 1024**3
ARCHIVE_RECEIPT = "last-run.json"
_ARCHIVED_STATUSES = frozenset({"archived", "unchanged"})


class RetentionError(RuntimeError):
    """A code-bearing retention failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class RetentionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    reserve_free_bytes: int = Field(default=DEFAULT_RESERVE_BYTES, ge=0)
    hot_keep_hours: float = Field(default=DEFAULT_HOT_KEEP_HOURS, ge=0.0)
    hot_max_bytes: int = Field(default=DEFAULT_HOT_MAX_BYTES, ge=0)
    cold_keep_days: float = Field(default=DEFAULT_COLD_KEEP_DAYS, ge=0.0)
    cold_max_bytes: int = Field(default=DEFAULT_COLD_MAX_BYTES, ge=0)
    archive_required: bool = True
    verify_segments: bool = True


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise RetentionError("invalid_clock")
    return value


def _to_us(clock: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = _utc(clock) - epoch
    return ((delta.days * 86400 + delta.seconds) * 1_000_000) + delta.microseconds


# ------------------------------------------------------------------- measuring


@dataclass(frozen=True)
class DiskUsage:
    total_bytes: int
    free_bytes: int

    @property
    def used_bytes(self) -> int:
        return self.total_bytes - self.free_bytes


def disk_usage(path: Path) -> DiskUsage:
    try:
        usage = shutil.disk_usage(Path(path))
    except OSError:
        raise RetentionError("disk_unreadable") from None
    return DiskUsage(total_bytes=usage.total, free_bytes=usage.free)


def allocated_bytes(path: Path) -> int:
    """Allocated bytes (stat blocks), matching what ``du`` reports."""
    root = Path(path)
    if not root.exists():
        return 0
    total = 0
    stack = [root]
    try:
        while stack:
            current = stack.pop()
            with os.scandir(current) as entries:
                for entry in entries:
                    info = entry.stat(follow_symlinks=False)
                    if entry.is_dir(follow_symlinks=False):
                        total += info.st_blocks * 512
                        stack.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        total += info.st_blocks * 512
                    elif entry.is_symlink():
                        raise RetentionError("unsafe_path")
    except OSError:
        raise RetentionError("io_error") from None
    return total


@dataclass(frozen=True)
class HotUsage:
    floor: int
    head: int
    batches: int
    allocated_bytes: int
    logical_bytes: int
    newest_received_at: datetime | None


def hot_usage(cache_root: Path) -> HotUsage:
    """Measure the hot cache tree; logical bytes are ``batch.json`` payloads."""
    root = Path(cache_root)
    batches = root / "batches"
    floor = read_floor(root)
    if not batches.is_dir():
        return HotUsage(floor=floor, head=floor, batches=0, allocated_bytes=0,
                        logical_bytes=0, newest_received_at=None)
    allocated = 0
    logical = 0
    newest: datetime | None = None
    names: list[int] = []
    try:
        with os.scandir(batches) as entries:
            for entry in entries:
                if not entry.name.isdigit() or len(entry.name) != 20:
                    continue
                info = entry.stat(follow_symlinks=False)
                if not entry.is_dir(follow_symlinks=False):
                    raise RetentionError("unsafe_path")
                sequence = int(entry.name)
                names.append(sequence)
                with os.scandir(entry.path) as files:
                    for item in files:
                        info = item.stat(follow_symlinks=False)
                        if item.is_dir(follow_symlinks=False):
                            raise RetentionError("unsafe_path")
                        allocated += info.st_blocks * 512
                        if item.name == "batch.json":
                            logical += info.st_size
    except OSError:
        raise RetentionError("io_error") from None
    head = max(names, default=floor)
    if names:
        newest = _try_read_received_at(root, head)
    return HotUsage(floor=floor, head=head, batches=len(names), allocated_bytes=allocated,
                    logical_bytes=logical, newest_received_at=newest)


def _try_read_received_at(cache_root: Path, sequence: int) -> datetime | None:
    try:
        return RawL2Cache(cache_root).read_batches(after_sequence=sequence - 1, limit=1).batches[0].received_at
    except Exception:  # noqa: BLE001 - measurement must not fail the plan
        return None


def archive_coverage(state_root: Path) -> int | None:
    """Return the highest sequence covered by a successful archive receipt."""
    path = Path(state_root) / ARCHIVE_RECEIPT
    if not path.is_file():
        return None
    try:
        receipt = json.loads(path.read_bytes())
    except (OSError, ValueError):
        return None
    if not isinstance(receipt, dict) or receipt.get("status") not in _ARCHIVED_STATUSES:
        return None
    sequence = receipt.get("last_sequence")
    if type(sequence) is not int or sequence < 0:
        return None
    return sequence


# ---------------------------------------------------------------------- plans


@dataclass(frozen=True)
class CompactRange:
    after_sequence: int
    until_sequence: int

    @property
    def records(self) -> int:
        return max(0, self.until_sequence - self.after_sequence)


@dataclass(frozen=True)
class ColdEviction:
    files: tuple[str, ...]
    freed_bytes: int
    reason: str


@dataclass(frozen=True)
class RetentionPlan:
    now: datetime
    policy: RetentionPolicy
    disk: DiskUsage
    hot_floor: int
    hot_head: int
    hot_batches: int
    hot_allocated_bytes: int
    compact_range: CompactRange | None
    archived_through: int | None
    cold_segments: int
    cold_bytes: int
    cold_eviction: ColdEviction | None
    prune_through: int
    prune_hot: bool
    writer_active: bool
    blockers: tuple[str, ...]
    notes: tuple[str, ...]


def _verified_intervals(segments: tuple[SegmentEntry, ...], *, include_unverified: bool = False
                       ) -> list[tuple[int, int]]:
    intervals = [
        (entry.first_sequence, entry.last_sequence)
        for entry in segments
        if include_unverified or entry.verified_at is not None
    ]
    intervals.sort()
    return intervals


def _covered_through(intervals: list[tuple[int, int]], floor: int, through: int) -> int:
    """Highest sequence covered contiguously from floor+1 up to ``through``."""
    cursor = floor
    for first, last in intervals:
        if first > cursor + 1:
            break
        if last > cursor:
            cursor = min(last, through)
        if cursor >= through:
            break
    return cursor


def _writer_active(cache_root: Path) -> bool:
    root = Path(cache_root)
    lock = root / ".writer.lock"
    if not lock.exists():
        return False
    try:
        fd = os.open(lock, os.O_RDWR | os.O_NOFOLLOW)
    except OSError:
        return True
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def _hot_boundary(cache_root: Path, cutoff: datetime) -> int:
    """Last committed sequence whose ``received_at`` is at or before ``cutoff``."""
    cache = RawL2Cache(Path(cache_root))
    floor = cache.floor_sequence()
    head = cache.read_status().last_sequence
    if head <= floor:
        return floor
    cutoff_us = _to_us(cutoff)
    low, high = floor + 1, head + 1
    while low < high:
        middle = (low + high) // 2
        page = cache.read_batches(after_sequence=middle - 1, limit=1)
        if not page.batches:
            low = middle + 1
            continue
        if _to_us(page.batches[0].received_at) <= cutoff_us:
            low = middle + 1
        else:
            high = middle
    return low - 1


def plan_retention(
    cache_root: Path,
    cold_root: Path,
    *,
    policy: RetentionPolicy | None = None,
    archive_state_root: Path | None = None,
    now: datetime | None = None,
    measure_hot: bool = True,
) -> RetentionPlan:
    policy = policy or RetentionPolicy()
    clock = _utc(now or datetime.now(UTC))
    cache_dir = Path(cache_root)
    cold_dir = Path(cold_root)
    disk = disk_usage(cold_dir if cold_dir.exists() else cache_dir)
    notes: list[str] = []
    blockers: list[str] = []
    try:
        floor = read_floor(cache_dir)
        cache = RawL2Cache(cache_dir)
        head = cache.read_status().last_sequence
    except Exception as exc:  # noqa: BLE001 - cache errors carry their own codes
        raise RetentionError(getattr(exc, "code", "invalid_cache")) from None
    usage = hot_usage(cache_dir) if measure_hot else HotUsage(
        floor=floor, head=head, batches=0, allocated_bytes=0, logical_bytes=0,
        newest_received_at=None,
    )
    floor = max(floor, usage.floor)
    head = max(head, usage.head)

    manifest = load_manifest(cold_dir) if cold_dir.exists() else None
    segments = manifest.segments if manifest is not None else ()
    covered_any = _covered_through(_verified_intervals(segments, include_unverified=True), floor, head)
    boundary = floor
    if head > floor:
        cutoff = clock - timedelta(hours=policy.hot_keep_hours)
        try:
            boundary = _hot_boundary(cache_dir, cutoff)
        except Exception as exc:  # noqa: BLE001 - cache errors carry their own codes
            raise RetentionError(getattr(exc, "code", "scan_failed")) from None

    compact: CompactRange | None = None
    if head > covered_any:
        start = max(floor, covered_any)
        if boundary > start:
            compact = CompactRange(after_sequence=start, until_sequence=boundary)
        else:
            notes.append("nothing older than the hot window")
    else:
        notes.append("hot prefix already covered by cold segments")
    if compact is None and usage.allocated_bytes > policy.hot_max_bytes:
        notes.append("hot cache exceeds its size cap but no older range is available")

    cold_bytes = sum(entry.stored_bytes for entry in segments)
    archived = archive_coverage(Path(archive_state_root)) if archive_state_root is not None else None
    if policy.archive_required and archive_state_root is None:
        blockers.append("archive_state_missing")
    if policy.archive_required and archive_state_root is not None and archived is None:
        blockers.append("archive_not_verified")

    writer_busy = _writer_active(cache_dir)
    if writer_busy:
        notes.append("collector holds the writer lock; pruning will be skipped")
    verified_covered = _covered_through(_verified_intervals(segments), floor, head)
    prune_through = min(verified_covered, boundary)
    if policy.archive_required:
        if archived is None:
            prune_through = floor
        else:
            if verified_covered > archived:
                notes.append("archive receipt lags verified cold coverage")
            prune_through = min(prune_through, archived)
    if verified_covered < boundary:
        notes.append("compaction must run before pruning")
    # The published head is a hardlink to the newest batch; keep that batch so
    # readers and restart recovery never see a dangling .head after retiring.
    if prune_through > head - 1:
        prune_through = max(floor, head - 1)
        if prune_through > floor:
            notes.append("newest batch kept for the published head")
    if writer_busy:
        prune_through = floor
    prune_hot = prune_through > floor and not blockers

    eviction = _plan_cold_eviction(
        segments, policy=policy, now=clock, disk=disk,
        incoming_bytes=(compact.records * 64 if compact is not None else 0),
        notes=notes,
    )
    return RetentionPlan(
        now=clock, policy=policy, disk=disk, hot_floor=floor, hot_head=head,
        hot_batches=usage.batches, hot_allocated_bytes=usage.allocated_bytes,
        compact_range=compact, archived_through=archived, cold_segments=len(segments),
        cold_bytes=cold_bytes, cold_eviction=eviction, prune_through=prune_through,
        prune_hot=prune_hot,
        writer_active=writer_busy, blockers=tuple(blockers), notes=tuple(notes),
    )


def _plan_cold_eviction(
    segments: tuple[SegmentEntry, ...],
    *,
    policy: RetentionPolicy,
    now: datetime,
    disk: DiskUsage,
    incoming_bytes: int,
    notes: list[str],
) -> ColdEviction | None:
    """Choose the oldest verified segments to delete, respecting both caps."""
    candidates = []
    cutoff = now - timedelta(days=policy.cold_keep_days)
    for entry in segments:
        if entry.verified_at is None:
            continue
        if entry.created_at > cutoff:
            continue
        candidates.append(entry)
    candidates.sort(key=lambda entry: (entry.first_sequence, entry.file))
    total = sum(entry.stored_bytes for entry in segments)
    projected = total + incoming_bytes
    must_evict = projected > policy.cold_max_bytes
    need = max(0, policy.reserve_free_bytes + incoming_bytes - disk.free_bytes)
    if not must_evict and need <= 0:
        return None
    chosen: list[str] = []
    freed = 0
    for entry in candidates:
        if freed >= need and (not must_evict or projected - freed <= policy.cold_max_bytes):
            break
        chosen.append(entry.file)
        freed += entry.stored_bytes
    if not chosen:
        if need > 0:
            notes.append(f"cold reserve shortfall {need} bytes with no evictable segment")
        return None
    still_over = projected - freed > policy.cold_max_bytes
    if still_over:
        notes.append("cold cap still exceeded after evicting young-eligible segments")
    return ColdEviction(files=tuple(chosen), freed_bytes=freed,
                        reason="cold_budget" if must_evict else "reserve_floor")


# -------------------------------------------------------------------- actions


@dataclass(frozen=True)
class RetentionReport:
    plan: RetentionPlan
    dry_run: bool
    compacted_records: int
    compacted_segments: int
    compacted_stored_bytes: int
    pruned_batches: int
    pruned_allocated_bytes: int
    pruned_through: int
    evicted_segments: int
    evicted_bytes: int
    hot_allocated_after: int
    cold_bytes_after: int
    disk_free_after: int
    elapsed_seconds: float


def _prune_hot_prefix(cache_root: Path, through: int, *, reason: str,
                      now: datetime) -> tuple[int, int]:
    """Delete ``batches/floor+1..through`` after publishing the new floor."""
    root = Path(cache_root)
    batches = root / "batches"
    floor = read_floor(root)
    if through <= floor:
        return 0, 0
    lock = os.open(root / ".writer.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    deleted = 0
    allocated = 0
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RetentionError("writer_active") from None
        write_floor(root, through, reason=reason, now=now)
        for sequence in range(floor + 1, through + 1):
            directory = batches / f"{sequence:020d}"
            try:
                info = directory.lstat()
            except FileNotFoundError:
                continue
            except OSError:
                raise RetentionError("io_error") from None
            if not directory.is_dir() or directory.is_symlink():
                raise RetentionError("unsafe_path")
            with os.scandir(directory) as entries:
                for entry in entries:
                    allocated += entry.stat(follow_symlinks=False).st_blocks * 512
            try:
                shutil.rmtree(directory)
            except OSError:
                raise RetentionError("io_error") from None
            deleted += 1
        fd = os.open(batches, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        os.close(lock)
    return deleted, allocated


def _evict_cold_segments(cold_root: Path, files: tuple[str, ...], *, now: datetime) -> int:
    root = Path(cold_root)
    segments_dir = (root / "segments").resolve()
    manifest = load_manifest(root)
    by_file = {entry.file: entry for entry in manifest.segments}
    removed = 0
    for name in files:
        entry = by_file.get(name)
        if entry is None:
            raise RetentionError("unknown_segment")
        path = (root / name).resolve()
        if segments_dir not in path.parents:
            raise RetentionError("unsafe_path")
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            raise RetentionError("io_error") from None
        removed += 1
    removed_files = set(files)
    remaining = tuple(entry for entry in manifest.segments if entry.file not in removed_files)
    _write_manifest(root, ColdManifest(segments=remaining))
    return removed


def run_retention(
    cache_root: Path,
    cold_root: Path,
    *,
    policy: RetentionPolicy | None = None,
    archive_state_root: Path | None = None,
    now: datetime | None = None,
    dry_run: bool = False,
) -> RetentionReport:
    """Compact, archive-gated prune, and cold eviction in one guarded pass."""
    started = time.perf_counter()
    policy = policy or RetentionPolicy()
    clock = _utc(now or datetime.now(UTC))
    plan = plan_retention(cache_root, cold_root, policy=policy,
                          archive_state_root=archive_state_root, now=clock)
    cache_dir = Path(cache_root)
    cold_dir = Path(cold_root)
    compacted_records = compacted_segments = compacted_stored = 0
    if plan.compact_range is not None and not dry_run:
        result = compact_cache(
            cache_dir, cold_dir,
            after_sequence=plan.compact_range.after_sequence,
            until_sequence=plan.compact_range.until_sequence,
            verify=policy.verify_segments,
            reserve_bytes=policy.reserve_free_bytes,
            now=clock,
        )
        compacted_records = result.records
        compacted_segments = len(result.segments)
        compacted_stored = result.stored_bytes
        plan = plan_retention(cache_dir, cold_dir, policy=policy,
                              archive_state_root=archive_state_root, now=clock)
    pruned_batches = pruned_allocated = pruned_through = 0
    if plan.prune_hot and plan.prune_through > plan.hot_floor and not dry_run:
        pruned_batches, pruned_allocated = _prune_hot_prefix(
            cache_dir, plan.prune_through, reason="retention", now=clock,
        )
        pruned_through = plan.prune_through
    evicted = evicted_bytes = 0
    if plan.cold_eviction is not None and not dry_run:
        evicted = _evict_cold_segments(cold_dir, plan.cold_eviction.files, now=clock)
        evicted_bytes = plan.cold_eviction.freed_bytes
    hot_after = allocated_bytes(Path(cache_dir) / "batches") if not dry_run else plan.hot_allocated_bytes
    cold_after = sum(entry.stored_bytes for entry in load_manifest(cold_dir).segments) if cold_dir.exists() else 0
    return RetentionReport(
        plan=plan, dry_run=dry_run,
        compacted_records=compacted_records, compacted_segments=compacted_segments,
        compacted_stored_bytes=compacted_stored, pruned_batches=pruned_batches,
        pruned_allocated_bytes=pruned_allocated, pruned_through=pruned_through,
        evicted_segments=evicted, evicted_bytes=evicted_bytes,
        hot_allocated_after=hot_after, cold_bytes_after=cold_after,
        disk_free_after=disk_usage(cold_dir if cold_dir.exists() else cache_dir).free_bytes,
        elapsed_seconds=time.perf_counter() - started,
    )


def _report_json(report: RetentionReport) -> dict:
    plan = report.plan
    return {
        "schema": "rquant-l2-retention-report-v1",
        "dry_run": report.dry_run,
        "now": plan.now.isoformat(),
        "policy": plan.policy.model_dump(),
        "disk_free_bytes": plan.disk.free_bytes,
        "hot": {
            "floor": plan.hot_floor, "head": plan.hot_head, "batches": plan.hot_batches,
            "allocated_bytes": plan.hot_allocated_bytes,
            "after_allocated_bytes": report.hot_allocated_after,
        },
        "compact_range": None if plan.compact_range is None else {
            "after_sequence": plan.compact_range.after_sequence,
            "until_sequence": plan.compact_range.until_sequence,
        },
        "archived_through": plan.archived_through,
        "cold": {
            "segments": plan.cold_segments, "bytes": plan.cold_bytes,
            "after_bytes": report.cold_bytes_after,
            "eviction": None if plan.cold_eviction is None else {
                "files": list(plan.cold_eviction.files),
                "freed_bytes": plan.cold_eviction.freed_bytes,
                "reason": plan.cold_eviction.reason,
            },
        },
        "writer_active": plan.writer_active,
        "prune_through": plan.prune_through,
        "prune_hot": plan.prune_hot,
        "blockers": list(plan.blockers),
        "notes": list(plan.notes),
        "result": {
            "compacted_records": report.compacted_records,
            "compacted_segments": report.compacted_segments,
            "compacted_stored_bytes": report.compacted_stored_bytes,
            "pruned_batches": report.pruned_batches,
            "pruned_allocated_bytes": report.pruned_allocated_bytes,
            "pruned_through": report.pruned_through,
            "evicted_segments": report.evicted_segments,
            "evicted_bytes": report.evicted_bytes,
            "disk_free_after": report.disk_free_after,
            "elapsed_seconds": round(report.elapsed_seconds, 3),
        },
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m rquant.l2.retention")
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--cold-root", required=True)
    parser.add_argument("--archive-state-root")
    parser.add_argument("--reserve-bytes", type=int, default=DEFAULT_RESERVE_BYTES)
    parser.add_argument("--hot-keep-hours", type=float, default=DEFAULT_HOT_KEEP_HOURS)
    parser.add_argument("--hot-max-bytes", type=int, default=DEFAULT_HOT_MAX_BYTES)
    parser.add_argument("--cold-keep-days", type=float, default=DEFAULT_COLD_KEEP_DAYS)
    parser.add_argument("--cold-max-bytes", type=int, default=DEFAULT_COLD_MAX_BYTES)
    parser.add_argument("--no-archive-required", action="store_true")
    parser.add_argument("--no-verify", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    policy = RetentionPolicy(
        reserve_free_bytes=args.reserve_bytes,
        hot_keep_hours=args.hot_keep_hours,
        hot_max_bytes=args.hot_max_bytes,
        cold_keep_days=args.cold_keep_days,
        cold_max_bytes=args.cold_max_bytes,
        archive_required=not args.no_archive_required,
        verify_segments=not args.no_verify,
    )
    try:
        report = run_retention(
            Path(args.cache_root), Path(args.cold_root), policy=policy,
            archive_state_root=Path(args.archive_state_root) if args.archive_state_root else None,
            dry_run=args.dry_run,
        )
    except RetentionError as exc:
        print(json.dumps({"ok": False, "error": exc.code}), file=sys.stderr)
        return 1
    except ColdStoreError as exc:
        print(json.dumps({"ok": False, "error": exc.code}), file=sys.stderr)
        return 1
    print(json.dumps(_report_json(report), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
