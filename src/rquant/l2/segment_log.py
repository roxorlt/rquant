"""Segmented append-only hot cache log (``rquant-l2-segmented-log-v1``).

The batch-directory writer pays two file fsyncs plus a directory fsync per market
batch (~2.4 ms/batch on the production cloud disk, ~290 batches/s ceiling).  The
2026-11-06 80-stock window showed the vendor pushing ~670 batches/s at the open,
so the pump queue backed up 179k frames in 328 s.  This module appends batches as
length-prefixed records into large segment files and fsyncs only at *commit
boundaries* (default every 200 ms or 256 batches), which reduces durability work
to a few fsyncs per second.

On-disk format (little-endian)
------------------------------
``segments/.active.l2s`` (being written) and ``segments/seg-<first>-<last>.l2s``
(closed, read-only).  File header (36 B)::

    magic "RQSEG001" (8s) | version u16 | reserved u16 |
    segment_index u64 | first_sequence u64 | created_at_ns u64

Records::

    total_len u32 (including this 9-byte prefix) | kind u8 | crc32 u32 | body

``crc32`` covers the kind byte plus the body.  Kinds:

* ``1`` payload — canonical JSON bytes identical to ``batches/<seq>/batch.json``
  (``RawL2Batch.model_dump_json() + "\\n"``), one market batch per record.
* ``2`` commit — ``committed_through u64 | wall_ns u64 | records u64``.  Only
  records up to the last valid commit are visible to readers; a crash loses at
  most the records after the last commit (≤ one commit interval).
* ``3`` footer (closed segment only) — ``records u64 | first_sequence u64 |
  last_sequence u64 | sha256(32)``; the digest covers header end → footer start.

Durability: ``append`` only writes into the page cache.  ``commit`` writes the
commit record, fsyncs the active segment and only then atomically replaces
``segments/head.json``; a reader can never observe an uncommitted record.  Writer
recovery truncates a torn tail back to the last valid commit — discarded batches
were never visible.  A complete-but-unrenamed segment (crash between footer and
index append) is adopted and indexed on the next open.

Startup cost is O(1) segment scans: closed segments are trusted through
``index.jsonl`` (+ existence/size checks) and never fully re-read; only the active
tail is scanned and repaired.  ``audit()`` provides the optional deep check.

Single-writer by construction: ``RawL2Cache.writer()`` owns the ``.writer.lock``
flock.  Readers open files independently and only trust committed records, which
are never rewritten in place.
"""
from __future__ import annotations

import bisect
import hashlib
import json
import math
import os
import stat
import struct
import tempfile
import time
import zlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import RawL2Batch

MAGIC = b"RQSEG001"
VERSION = 1
HEADER = struct.Struct("<8sHHQQQ")
HEADER_SIZE = HEADER.size
RECORD_PREFIX = struct.Struct("<IBI")
PREFIX_SIZE = RECORD_PREFIX.size
KIND_PAYLOAD = 1
KIND_COMMIT = 2
KIND_FOOTER = 3
COMMIT_BODY = struct.Struct("<QQQ")
FOOTER_BODY = struct.Struct("<QQQ")
DIGEST_SIZE = 32
SEGMENTS_DIR = "segments"
INDEX_NAME = "index.jsonl"
HEAD_NAME = "head.json"
ACTIVE_NAME = ".active.l2s"
LAYOUT_NAME = ".layout"
LAYOUT_SCHEMA = "rquant-l2-cache-layout-v1"
HEAD_SCHEMA = "rquant-l2-segment-head-v1"
SEGMENT_PREFIX = "seg-"
SEGMENT_SUFFIX = ".l2s"
MAX_SEGMENT_BYTES = 1024 * 1024 * 1024
MAX_RECORD_BYTES = 24 * 1024 * 1024
MAX_COMMIT_INTERVAL_SECONDS = 5.0
MAX_SEGMENT_SECONDS = 3600.0
MAX_COMMIT_EVERY = 1_000_000


class SegmentError(RuntimeError):
    """Fixed-code segmented-log failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _crc32(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _replace_synced(directory: Path, name: str, payload: bytes) -> None:
    if (directory / name).is_symlink():
        raise SegmentError("unsafe_path")
    fd, value = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    temporary = Path(value)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, directory / name)
        temporary = None
        _fsync_dir(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def segment_name(first_sequence: int, last_sequence: int) -> str:
    return f"{SEGMENT_PREFIX}{first_sequence:020d}-{last_sequence:020d}{SEGMENT_SUFFIX}"


def parse_segment_name(name: str) -> tuple[int, int] | None:
    if not (name.startswith(SEGMENT_PREFIX) and name.endswith(SEGMENT_SUFFIX)):
        return None
    body = name[len(SEGMENT_PREFIX):-len(SEGMENT_SUFFIX)]
    parts = body.split("-")
    if len(parts) != 2 or not all(part.isdigit() and len(part) == 20 for part in parts):
        return None
    first, last = int(parts[0]), int(parts[1])
    if first < 1 or last < first:
        return None
    return first, last


def frame_record(kind: int, body: bytes) -> bytes:
    if kind not in (KIND_PAYLOAD, KIND_COMMIT, KIND_FOOTER):
        raise SegmentError("invalid_record")
    if len(body) + PREFIX_SIZE > MAX_RECORD_BYTES:
        raise SegmentError("invalid_record")
    return (RECORD_PREFIX.pack(len(body) + PREFIX_SIZE, kind,
                               _crc32(bytes((kind,)) + body)) + body)


def build_header(segment_index: int, first_sequence: int, created_at_ns: int) -> bytes:
    return HEADER.pack(MAGIC, VERSION, 0, segment_index, first_sequence, created_at_ns)


def segments_dir(root: Path | str) -> Path:
    return Path(root) / SEGMENTS_DIR


def layout_path(root: Path | str) -> Path:
    return Path(root) / LAYOUT_NAME


def read_layout(root: Path | str) -> str | None:
    path = layout_path(root)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    except OSError:
        raise SegmentError("invalid_layout") from None
    if not stat.S_ISREG(info.st_mode) or info.st_size > 4096:
        raise SegmentError("invalid_layout")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise SegmentError("invalid_layout") from None
    if not isinstance(value, dict) or value.get("schema") != LAYOUT_SCHEMA:
        raise SegmentError("invalid_layout")
    if value.get("mode") not in ("batches", "segments"):
        raise SegmentError("invalid_layout")
    return value["mode"]


def write_layout(root: Path | str, mode: str) -> None:
    if mode != "segments":
        raise ValueError("only the segmented layout is written here")
    payload = json.dumps({"schema": LAYOUT_SCHEMA, "mode": mode,
                          "created_at": datetime.now(UTC).isoformat()},
                         sort_keys=True).encode() + b"\n"
    _replace_synced(Path(root), LAYOUT_NAME, payload)


@dataclass(frozen=True)
class SegmentEntry:
    file: str
    first_sequence: int
    last_sequence: int
    records: int
    bytes: int
    closed_at_ns: int


def read_index(root: Path | str) -> list[SegmentEntry]:
    path = segments_dir(root) / INDEX_NAME
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return []
    except OSError:
        raise SegmentError("invalid_index") from None
    entries: list[SegmentEntry] = []
    lines = data.splitlines()
    for number, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
            entry = SegmentEntry(
                file=str(value["file"]), first_sequence=int(value["first_sequence"]),
                last_sequence=int(value["last_sequence"]), records=int(value["records"]),
                bytes=int(value["bytes"]), closed_at_ns=int(value["closed_at_ns"]))
        except (ValueError, KeyError, TypeError):
            if number == len(lines) - 1:
                continue  # torn final line after a crash
            raise SegmentError("invalid_index") from None
        if parse_segment_name(entry.file) != (entry.first_sequence, entry.last_sequence):
            raise SegmentError("invalid_index")
        if entry.first_sequence < 1 or entry.last_sequence < entry.first_sequence:
            raise SegmentError("invalid_index")
        entries.append(entry)
    for left, right in zip(entries, entries[1:]):
        if right.first_sequence != left.last_sequence + 1:
            raise SegmentError("sequence_gap")
    return entries


def append_index(root: Path | str, entry: SegmentEntry) -> None:
    path = segments_dir(root) / INDEX_NAME
    line = json.dumps({
        "file": entry.file, "first_sequence": entry.first_sequence,
        "last_sequence": entry.last_sequence, "records": entry.records,
        "bytes": entry.bytes, "closed_at_ns": entry.closed_at_ns,
    }, sort_keys=True).encode() + b"\n"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)
    _fsync_dir(path.parent)


def read_head(root: Path | str) -> dict[str, Any] | None:
    path = segments_dir(root) / HEAD_NAME
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    except OSError:
        raise SegmentError("invalid_head") from None
    if not stat.S_ISREG(info.st_mode) or info.st_size > 64 * 1024:
        raise SegmentError("invalid_head")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise SegmentError("invalid_head") from None
    if not isinstance(value, dict) or value.get("schema") != HEAD_SCHEMA:
        raise SegmentError("invalid_head")
    return value


def _write_head(root: Path | str, *, committed_through: int,
                last_received_at: datetime | None, segment_index: int,
                active_first_sequence: int, active_bytes: int,
                active_records: int) -> None:
    payload = json.dumps({
        "schema": HEAD_SCHEMA,
        "committed_through": committed_through,
        "last_received_at": (last_received_at.isoformat() if last_received_at else None),
        "segment_index": segment_index,
        "active_first_sequence": active_first_sequence,
        "active_bytes": active_bytes,
        "active_records": active_records,
        "updated_at": datetime.now(UTC).isoformat(),
    }, sort_keys=True).encode() + b"\n"
    _replace_synced(segments_dir(root), HEAD_NAME, payload)


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


# --------------------------------------------------------------------------- #
# segment scanning
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SegmentScan:
    path: Path
    first_sequence: int | None
    segment_index: int | None
    payload_records: int
    committed_records: int
    last_sequence: int | None
    committed_through: int | None
    last_commit_offset: int
    valid_bytes: int
    footer_offset: int | None
    closed: bool
    file_bytes: int
    error: str | None = None
    digest: str | None = None


def scan_segment(path: Path) -> SegmentScan:
    """Validate one segment up to its last complete record; tolerate a torn tail."""
    try:
        info = path.lstat()
    except OSError:
        raise SegmentError("missing_segment") from None
    if not stat.S_ISREG(info.st_mode):
        raise SegmentError("unsafe_path")
    file_bytes = info.st_size
    if file_bytes < HEADER_SIZE:
        return SegmentScan(path, None, None, 0, 0, None, None, HEADER_SIZE, HEADER_SIZE,
                           None, False, file_bytes, error="short_header")
    with path.open("rb") as stream:
        raw = stream.read(HEADER_SIZE)
        if len(raw) != HEADER_SIZE:
            return SegmentScan(path, None, None, 0, 0, None, None, HEADER_SIZE,
                               HEADER_SIZE, None, False, file_bytes, error="short_header")
        magic, version, _reserved, segment_index, first_sequence, _created = (
            HEADER.unpack(raw))
        if magic != MAGIC or version != VERSION:
            raise SegmentError("invalid_segment")
        hasher = hashlib.sha256(raw)
        offset = HEADER_SIZE
        payloads = 0
        committed_records = 0
        last_sequence: int | None = None
        committed: int | None = None
        commit_offset = HEADER_SIZE
        error: str | None = None
        footer_offset: int | None = None
        digest: str | None = None
        while offset < file_bytes:
            stream.seek(offset)
            prefix = stream.read(PREFIX_SIZE)
            if len(prefix) != PREFIX_SIZE:
                error = "torn_record"
                break
            total, kind_raw, _crc = RECORD_PREFIX.unpack(prefix)
            if total < PREFIX_SIZE or total > MAX_RECORD_BYTES:
                error = "invalid_record"
                break
            if offset + total > file_bytes:
                error = "torn_record"
                break
            body = stream.read(total - PREFIX_SIZE)
            if len(body) != total - PREFIX_SIZE:
                error = "torn_record"
                break
            if _crc32(bytes((kind_raw,)) + body) != _crc or \
                    kind_raw not in (KIND_PAYLOAD, KIND_COMMIT, KIND_FOOTER):
                error = "invalid_record_crc"
                break
            kind = kind_raw
            frame = prefix + body
            if kind == KIND_PAYLOAD:
                payloads += 1
                last_sequence = first_sequence + payloads - 1
                hasher.update(frame)
                offset += total
                continue
            if kind == KIND_COMMIT:
                if len(body) != COMMIT_BODY.size:
                    error = "invalid_commit"
                    break
                value, _wall, records = COMMIT_BODY.unpack(body)
                expected = first_sequence + payloads - 1 if payloads else first_sequence - 1
                if value != expected or records != payloads:
                    error = "invalid_commit"
                    break
                committed = value
                committed_records = payloads
                hasher.update(frame)
                commit_offset = offset + total
                offset += total
                continue
            if len(body) != FOOTER_BODY.size + DIGEST_SIZE:
                error = "invalid_footer"
                break
            records, first, last = FOOTER_BODY.unpack_from(body)
            expected_last = last_sequence if last_sequence is not None else first_sequence - 1
            if records != payloads or first != first_sequence or last != expected_last:
                error = "invalid_footer"
                break
            actual = hasher.digest()
            if actual != body[FOOTER_BODY.size:]:
                error = "invalid_footer_digest"
                break
            footer_offset = offset
            digest = actual.hex()
            offset += total
            break
    if footer_offset is not None:
        valid_bytes = offset
    else:
        valid_bytes = commit_offset
    return SegmentScan(
        path=path, first_sequence=first_sequence, segment_index=segment_index,
        payload_records=payloads, committed_records=committed_records,
        last_sequence=last_sequence, committed_through=committed,
        last_commit_offset=commit_offset, valid_bytes=valid_bytes,
        footer_offset=footer_offset, closed=footer_offset is not None,
        file_bytes=file_bytes, error=error, digest=digest)


# --------------------------------------------------------------------------- #
# writer
# --------------------------------------------------------------------------- #
@dataclass
class WriterRecovery:
    closed_segments: int
    committed_through: int
    active_first_sequence: int
    active_segment_index: int
    truncated_bytes: int = 0
    adopted_active: bool = False
    notes: list[str] = field(default_factory=list)


class SegmentWriter:
    """Single-writer append-only segment log (see module docstring)."""

    def __init__(self, root: Path | str, *, commit_every_batches: int = 256,
                 commit_interval_seconds: float = 0.2,
                 segment_bytes: int = 64 * 1024 * 1024,
                 segment_seconds: float = 60.0,
                 clock=time.monotonic) -> None:
        if type(commit_every_batches) is not int or \
                not 1 <= commit_every_batches <= MAX_COMMIT_EVERY:
            raise ValueError("invalid commit batch size")
        if (type(commit_interval_seconds) not in (int, float)
                or not math.isfinite(commit_interval_seconds)
                or not 0 <= commit_interval_seconds <= MAX_COMMIT_INTERVAL_SECONDS):
            raise ValueError("invalid commit interval")
        if type(segment_bytes) is not int or \
                not 64 * 1024 <= segment_bytes <= MAX_SEGMENT_BYTES:
            raise ValueError("invalid segment bytes")
        if (type(segment_seconds) not in (int, float)
                or not math.isfinite(segment_seconds)
                or not 0 < segment_seconds <= MAX_SEGMENT_SECONDS):
            raise ValueError("invalid segment seconds")
        self.root = Path(root)
        self.commit_every_batches = commit_every_batches
        self.commit_interval_seconds = float(commit_interval_seconds)
        self.segment_bytes = segment_bytes
        self.segment_seconds = float(segment_seconds)
        self.clock = clock
        self._segments = segments_dir(self.root)
        self._stream = None
        self._entries: list[SegmentEntry] = []
        self._segment_index = 0
        self._first_sequence = 1
        self._next_sequence = 1
        self._records = 0
        self._committed_through = 0
        self._last_commit_offset = HEADER_SIZE
        self._segment_started_at = 0.0
        self._last_commit_at = 0.0
        self._last_received_at: datetime | None = None
        self._hasher = hashlib.sha256()
        self._closed = False
        self._startup_notes: list[str] = []
        self.recovery: WriterRecovery | None = None

    # -- lifecycle ------------------------------------------------------ #
    def open(self) -> WriterRecovery:
        self.root.mkdir(parents=True, exist_ok=True)
        self._segments.mkdir(parents=True, mode=0o700, exist_ok=True)
        if self.root.is_symlink() or self._segments.is_symlink():
            raise SegmentError("unsafe_path")
        mode = read_layout(self.root)
        if mode is None:
            write_layout(self.root, "segments")
        elif mode != "segments":
            raise SegmentError("layout_mismatch")
        self._entries = read_index(self.root)
        self._verify_closed_segments()
        recovery = WriterRecovery(
            closed_segments=len(self._entries),
            committed_through=self._entries[-1].last_sequence if self._entries else 0,
            active_first_sequence=(self._entries[-1].last_sequence + 1
                                   if self._entries else 1),
            active_segment_index=len(self._entries),
            notes=list(self._startup_notes))
        self._committed_through = recovery.committed_through
        self._first_sequence = recovery.active_first_sequence
        self._next_sequence = self._first_sequence
        self._segment_index = recovery.active_segment_index
        self._recover_active(recovery)
        self._open_active()
        self._segment_started_at = self.clock()
        self._last_commit_at = self.clock()
        self.recovery = recovery
        self._write_head_snapshot()
        return recovery

    def _verify_closed_segments(self) -> None:
        """Cheap trust check: every indexed file exists with the expected name.

        Full content validation of closed segments is deliberately *not* done on
        startup (a production day has thousands of 64 MiB segments); the footer
        digest + ``audit()`` provide the deep check on demand.
        """
        seen: set[str] = set()
        for path in self._segments.iterdir():
            if path.name in (INDEX_NAME, HEAD_NAME, ACTIVE_NAME):
                continue
            ranges = parse_segment_name(path.name)
            if ranges is None:
                raise SegmentError("invalid_segment")
            seen.add(path.name)
            if path.name in {entry.file for entry in self._entries}:
                if path.stat().st_size != next(
                        entry.bytes for entry in self._entries if entry.file == path.name):
                    raise SegmentError("invalid_index")
        indexed = {entry.file for entry in self._entries}
        missing = indexed - seen
        if missing:
            raise SegmentError("missing_segment")
        unindexed = sorted(seen - indexed)
        for name in unindexed:
            ranges = parse_segment_name(name)
            assert ranges is not None
            expected_first = self._entries[-1].last_sequence + 1 if self._entries else 1
            if ranges[0] != expected_first:
                raise SegmentError("sequence_gap")
            entries = read_index(self.root)
            path = self._segments / name
            scan = scan_segment(path)
            if not scan.closed or scan.error is not None:
                raise SegmentError("invalid_segment")
            entry = SegmentEntry(file=name, first_sequence=ranges[0],
                                 last_sequence=ranges[1],
                                 records=scan.payload_records,
                                 bytes=path.stat().st_size,
                                 closed_at_ns=time.time_ns())
            append_index(self.root, entry)
            self._entries.append(entry)
            self._startup_notes.append(f"adopted_closed_segment:{name}")
            del entries

    def _recover_active(self, recovery: WriterRecovery) -> None:
        active = self._segments / ACTIVE_NAME
        if not active.exists():
            return
        scan = scan_segment(active)
        if scan.error is not None and scan.error not in (
                "torn_record", "invalid_record", "invalid_record_crc", "short_header"):
            raise SegmentError(scan.error)
        if scan.first_sequence is None:
            recovery.truncated_bytes += scan.file_bytes
            with active.open("r+b") as stream:
                stream.truncate(0)
                stream.flush()
                os.fsync(stream.fileno())
            return
        if scan.segment_index != self._segment_index or \
                scan.first_sequence != self._first_sequence:
            raise SegmentError("invalid_segment")
        if scan.closed:
            self._adopt_closed_active(active, scan, recovery)
            return
        keep_through = scan.last_commit_offset
        if scan.file_bytes > keep_through:
            with active.open("r+b") as stream:
                stream.truncate(keep_through)
                stream.flush()
                os.fsync(stream.fileno())
            recovery.truncated_bytes += scan.file_bytes - keep_through
            recovery.notes.append(f"truncated_active:{scan.file_bytes - keep_through}B")
        committed = scan.committed_through if scan.committed_through is not None \
            else self._first_sequence - 1
        self._committed_through = committed
        self._next_sequence = committed + 1
        self._records = scan.committed_records
        self._last_commit_offset = keep_through
        recovery.committed_through = committed
        self._rehash_active(active, keep_through)
        self._last_received_at = self._last_committed_received_at(active, committed)

    def _rehash_active(self, path: Path, keep_through: int) -> None:
        hasher = hashlib.sha256()
        with path.open("rb") as stream:
            remaining = keep_through
            while remaining > 0:
                chunk = stream.read(min(1 << 20, remaining))
                if not chunk:
                    raise SegmentError("invalid_segment")
                hasher.update(chunk)
                remaining -= len(chunk)
        self._hasher = hasher

    def _last_committed_received_at(self, path: Path, committed: int) -> datetime | None:
        if committed <= 0:
            return None
        last: datetime | None = None
        with path.open("rb") as stream:
            stream.seek(HEADER_SIZE)
            sequence = self._first_sequence
            while True:
                prefix = stream.read(PREFIX_SIZE)
                if len(prefix) != PREFIX_SIZE:
                    break
                total, kind, _crc = RECORD_PREFIX.unpack(prefix)
                body = stream.read(total - PREFIX_SIZE)
                if len(body) != total - PREFIX_SIZE:
                    break
                if kind == KIND_PAYLOAD:
                    if sequence <= committed:
                        try:
                            last = RawL2Batch.model_validate_json(body).received_at
                        except ValueError:
                            raise SegmentError("invalid_batch") from None
                    sequence += 1
                elif kind == KIND_FOOTER:
                    break
        return last

    def _adopt_closed_active(self, active: Path, scan: SegmentScan,
                             recovery: WriterRecovery) -> None:
        footer_last = scan.committed_through
        if footer_last is None:
            raise SegmentError("invalid_segment")
        target = self._segments / segment_name(scan.first_sequence or 1, footer_last)
        if target.exists():
            raise SegmentError("duplicate_segment")
        os.replace(active, target)
        _fsync_dir(self._segments)
        entry = SegmentEntry(file=target.name, first_sequence=scan.first_sequence or 1,
                             last_sequence=footer_last, records=scan.payload_records,
                             bytes=target.stat().st_size, closed_at_ns=time.time_ns())
        append_index(self.root, entry)
        self._entries.append(entry)
        recovery.adopted_active = True
        recovery.notes.append(f"adopted_active:{target.name}")
        recovery.closed_segments = len(self._entries)
        recovery.committed_through = footer_last
        recovery.active_first_sequence = footer_last + 1
        recovery.active_segment_index = len(self._entries)
        self._committed_through = footer_last
        self._first_sequence = footer_last + 1
        self._next_sequence = self._first_sequence
        self._segment_index = recovery.active_segment_index
        self._records = 0
        self._last_commit_offset = HEADER_SIZE
        self._last_received_at = None
        self._hasher = hashlib.sha256()

    def _open_active(self) -> None:
        active = self._segments / ACTIVE_NAME
        if active.exists():
            self._stream = active.open("r+b")
            self._stream.seek(self._last_commit_offset)
            return
        self._stream = active.open("wb")
        header = build_header(self._segment_index, self._first_sequence, time.time_ns())
        self._stream.write(header)
        self._stream.flush()
        os.fsync(self._stream.fileno())
        self._hasher = hashlib.sha256(header)

    # -- writing -------------------------------------------------------- #
    def _write_frame(self, kind: int, body: bytes) -> None:
        if self._stream is None:
            raise SegmentError("writer_required")
        frame = frame_record(kind, body)
        self._stream.write(frame)
        if kind != KIND_FOOTER:
            self._hasher.update(frame)

    def append(self, batch: RawL2Batch) -> None:
        if self._stream is None or self._closed:
            raise SegmentError("writer_required")
        if not isinstance(batch, RawL2Batch):
            raise SegmentError("invalid_batch")
        if batch.local_batch_sequence != self._next_sequence:
            raise SegmentError("sequence_gap")
        payload = batch.model_dump_json().encode() + b"\n"
        if len(payload) + PREFIX_SIZE > MAX_RECORD_BYTES:
            raise SegmentError("invalid_batch")
        self._write_frame(KIND_PAYLOAD, payload)
        self._records += 1
        self._next_sequence += 1
        self._last_received_at = batch.received_at
        self._maybe_commit()
        self._maybe_rotate()

    def _maybe_commit(self, *, force: bool = False) -> None:
        if not force and self._records == 0:
            return
        elapsed = self.clock() - self._last_commit_at
        if force or self._records >= self.commit_every_batches or \
                elapsed >= self.commit_interval_seconds:
            self.commit()

    def commit(self) -> int:
        if self._stream is None:
            raise SegmentError("writer_required")
        if self._records == 0:
            return self._committed_through
        body = COMMIT_BODY.pack(self._next_sequence - 1, time.time_ns(), self._records)
        self._write_frame(KIND_COMMIT, body)
        self._stream.flush()
        os.fsync(self._stream.fileno())
        self._last_commit_offset = self._stream.tell()
        self._committed_through = self._next_sequence - 1
        self._last_commit_at = self.clock()
        self._write_head_snapshot()
        return self._committed_through

    def _maybe_rotate(self) -> None:
        if self._stream is None:
            return
        size = self._stream.tell()
        age = self.clock() - self._segment_started_at
        if size < self.segment_bytes and age < self.segment_seconds:
            return
        self.rotate()

    def rotate(self) -> None:
        if self._stream is None or self._closed:
            return
        if self._records:
            self.commit()
        if self._next_sequence - 1 < self._first_sequence:
            return
        self._finalize_segment()
        self._first_sequence = self._next_sequence
        self._segment_index += 1
        self._records = 0
        self._committed_through = self._next_sequence - 1
        self._last_commit_offset = HEADER_SIZE
        self._hasher = hashlib.sha256()
        self._open_active()
        self._segment_started_at = self.clock()
        self._last_commit_at = self.clock()
        self._write_head_snapshot()

    def _finalize_segment(self) -> None:
        first = self._first_sequence
        last = self._committed_through
        if self._stream is None or last < first:
            return
        footer = FOOTER_BODY.pack(self._records, first, last) + self._hasher.digest()
        frame = frame_record(KIND_FOOTER, footer)
        self._stream.write(frame)
        self._stream.flush()
        os.fsync(self._stream.fileno())
        closed_bytes = self._stream.tell()
        self._stream.close()
        self._stream = None
        active = self._segments / ACTIVE_NAME
        target = self._segments / segment_name(first, last)
        if target.exists():
            raise SegmentError("duplicate_segment")
        os.replace(active, target)
        _fsync_dir(self._segments)
        entry = SegmentEntry(file=target.name, first_sequence=first, last_sequence=last,
                             records=self._records, bytes=closed_bytes,
                             closed_at_ns=time.time_ns())
        append_index(self.root, entry)
        self._entries.append(entry)

    def close(self) -> int:
        if self._stream is None or self._closed:
            return self._committed_through
        if self._records:
            self.commit()
        self._closed = True
        if self._next_sequence - 1 >= self._first_sequence:
            self._finalize_segment()
        else:
            self._stream.close()
            self._stream = None
            (self._segments / ACTIVE_NAME).unlink(missing_ok=True)
        self._write_head_snapshot()
        return self._committed_through

    def _write_head_snapshot(self) -> None:
        _write_head(self.root, committed_through=self._committed_through,
                    last_received_at=self._last_received_at,
                    segment_index=self._segment_index,
                    active_first_sequence=self._first_sequence,
                    active_bytes=(self._stream.tell() if self._stream is not None
                                  else self._last_commit_offset),
                    active_records=self._records)

    # -- accessors ------------------------------------------------------ #
    @property
    def committed_through(self) -> int:
        return self._committed_through

    @property
    def last_received_at(self) -> datetime | None:
        return self._last_received_at


# --------------------------------------------------------------------------- #
# reader primitives
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SegmentSnapshot:
    entries: tuple[SegmentEntry, ...]
    committed_through: int
    last_received_at: datetime | None
    active_first_sequence: int


def load_snapshot(root: Path | str) -> SegmentSnapshot:
    """Cheap reader view: index + atomically-replaced head (no full scan)."""
    entries = tuple(read_index(root))
    head = read_head(root)
    if head is None:
        committed = entries[-1].last_sequence if entries else 0
        return SegmentSnapshot(entries=entries, committed_through=committed,
                               last_received_at=None,
                               active_first_sequence=committed + 1)
    try:
        committed = int(head["committed_through"])
        active_first = int(head["active_first_sequence"])
    except (KeyError, TypeError, ValueError):
        raise SegmentError("invalid_head") from None
    last_received = _parse_iso(head.get("last_received_at"))
    if entries:
        if committed < entries[-1].last_sequence:
            committed = entries[-1].last_sequence
        active_first = entries[-1].last_sequence + 1
    else:
        if committed < 0:
            raise SegmentError("invalid_head")
        active_first = committed + 1
    return SegmentSnapshot(entries=entries, committed_through=committed,
                           last_received_at=last_received,
                           active_first_sequence=active_first)


def _segment_position(entries: tuple[SegmentEntry, ...], sequence: int) -> int:
    """Index of the closed segment containing ``sequence``; -1 when it is in the active one."""
    if not entries or sequence < entries[0].first_sequence:
        return -1
    position = bisect.bisect_right([entry.first_sequence for entry in entries], sequence) - 1
    if position < 0:
        return -1
    entry = entries[position]
    if entry.first_sequence <= sequence <= entry.last_sequence:
        return position
    return -1


def _iter_segment_payloads(path: Path, *, stop_after: int, start_after: int) -> Iterator[RawL2Batch]:
    """Yield payload records with start_after < sequence <= stop_after."""
    with path.open("rb") as stream:
        raw = stream.read(HEADER_SIZE)
        if len(raw) != HEADER_SIZE:
            raise SegmentError("invalid_segment")
        magic, version, _reserved, _index, first_sequence, _created = HEADER.unpack(raw)
        if magic != MAGIC or version != VERSION:
            raise SegmentError("invalid_segment")
        sequence = first_sequence
        while True:
            prefix = stream.read(PREFIX_SIZE)
            if len(prefix) != PREFIX_SIZE:
                return
            total, kind, _crc = RECORD_PREFIX.unpack(prefix)
            if total < PREFIX_SIZE or total > MAX_RECORD_BYTES:
                raise SegmentError("invalid_record")
            body = stream.read(total - PREFIX_SIZE)
            if len(body) != total - PREFIX_SIZE:
                return
            if kind == KIND_PAYLOAD:
                if _crc32(bytes((kind,)) + body) != _crc:
                    raise SegmentError("invalid_record_crc")
                if sequence > stop_after:
                    return
                if sequence > start_after:
                    try:
                        yield RawL2Batch.model_validate_json(body)
                    except ValueError:
                        raise SegmentError("invalid_batch") from None
                sequence += 1
            elif kind == KIND_COMMIT:
                if _crc32(bytes((kind,)) + body) != _crc or len(body) != COMMIT_BODY.size:
                    raise SegmentError("invalid_commit")
                value, _wall, _records = COMMIT_BODY.unpack(body)
                if value >= stop_after:
                    return
            else:
                if _crc32(bytes((kind,)) + body) != _crc:
                    raise SegmentError("invalid_footer")
                return


def iter_payloads(root: Path | str, *, after_sequence: int = 0,
                  limit: int | None = None) -> Iterator[RawL2Batch]:
    """Read committed batches after ``after_sequence``; uncommitted records are invisible."""
    snapshot = load_snapshot(root)
    if snapshot.committed_through <= after_sequence:
        return
    emitted = 0
    segments = segments_dir(root)
    for entry in snapshot.entries:
        if entry.last_sequence <= after_sequence:
            continue
        for batch in _iter_segment_payloads(segments / entry.file,
                                            stop_after=snapshot.committed_through,
                                            start_after=after_sequence):
            if limit is not None and emitted >= limit:
                return
            emitted += 1
            yield batch
    active = segments / ACTIVE_NAME
    if active.exists():
        for batch in _iter_segment_payloads(active, stop_after=snapshot.committed_through,
                                            start_after=after_sequence):
            if limit is not None and emitted >= limit:
                return
            emitted += 1
            yield batch


def read_payload(root: Path | str, sequence: int) -> RawL2Batch | None:
    snapshot = load_snapshot(root)
    if sequence < 1 or sequence > snapshot.committed_through:
        return None
    segments = segments_dir(root)
    position = _segment_position(snapshot.entries, sequence)
    if position >= 0:
        path = segments / snapshot.entries[position].file
    else:
        path = segments / ACTIVE_NAME
        if not path.exists():
            return None
    for batch in _iter_segment_payloads(path, stop_after=snapshot.committed_through,
                                        start_after=sequence - 1):
        if batch.local_batch_sequence == sequence:
            return batch
        if batch.local_batch_sequence > sequence:
            return None
    return None


def audit(root: Path | str) -> dict[str, Any]:
    """Deep scan: validate all segment footers/continuity and the committed head."""
    snapshot = load_snapshot(root)
    segments = segments_dir(root)
    scanned: list[dict[str, Any]] = []
    expected = 1
    total_records = 0
    for entry in snapshot.entries:
        scan = scan_segment(segments / entry.file)
        if not scan.closed or scan.error is not None:
            raise SegmentError("invalid_segment")
        if scan.first_sequence != expected:
            raise SegmentError("sequence_gap")
        expected = entry.last_sequence + 1
        total_records += entry.records
        scanned.append({"file": entry.file, "records": entry.records,
                        "first_sequence": entry.first_sequence,
                        "last_sequence": entry.last_sequence,
                        "sha256": scan.digest})
    active = segments / ACTIVE_NAME
    active_block = None
    committed = expected - 1
    if active.exists():
        scan = scan_segment(active)
        if scan.error is not None and scan.error not in (
                "torn_record", "invalid_record", "invalid_record_crc"):
            raise SegmentError(scan.error)
        if scan.first_sequence is not None and scan.first_sequence != expected:
            raise SegmentError("sequence_gap")
        if scan.committed_through is not None:
            committed = max(committed, scan.committed_through)
            total_records += scan.committed_records
        active_block = {
            "first_sequence": scan.first_sequence,
            "committed_through": scan.committed_through,
            "committed_records": scan.committed_records,
            "file_bytes": scan.file_bytes,
            "torn_tail": scan.file_bytes > scan.last_commit_offset,
            "error": scan.error,
        }
    if snapshot.committed_through != committed:
        raise SegmentError("head_mismatch")
    last_received = None
    if committed > 0:
        last = read_payload(root, committed)
        if last is None:
            raise SegmentError("missing_committed_batch")
        last_received = last.received_at.isoformat()
    return {
        "schema": "rquant-l2-segment-audit-v1",
        "segments": scanned,
        "closed_segments": len(scanned),
        "active": active_block,
        "committed_through": committed,
        "total_committed_records": total_records,
        "last_received_at": last_received,
    }
