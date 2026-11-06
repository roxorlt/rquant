"""Zero-blocking raw-frame recorder for the L2 receive layer.

The tap copies complete wire frames (the 13-byte FD prefix plus body) into a
bounded process-local queue. One writer thread persists them as compact segment
files, so the receive loop never waits for disk I/O. Queue pressure drops the
newest frame and records a gap event instead of slowing market consumption.

Format ``rquant-l2-frame-tap-v1`` (little-endian):

file header (40 bytes)
    magic 8s b"RQL2TAP1", version u16, flags u16, meta_len u32,
    started_wall_us u64 (Unix epoch microseconds),
    started_mono_us u64 (boot-relative monotonic microseconds), reserved 8s
meta JSON (meta_len bytes, compact UTF-8)

record
    kind u8 (1 = received wire frame, 2 = sent wire frame, 3 = event JSON)
    delta_us uvarint (microseconds since the previous record; the first record
    is relative to the header's started_mono_us)
    payload_len uvarint
    payload bytes (kinds 1/2: exact FD-framed wire bytes; kind 3: event JSON)

footer (48 bytes)
    magic 8s b"RQL2END1", record_count u64, sha256(40 + meta + records) 32s

A raw frame is never decoded, normalized or re-wrapped here: the recorded bytes
are exactly the bytes observed on the socket (or handed to ``sendall``), which
keeps replay byte-identical and keeps vendor control frames such as the seed
subscription ack families in the sample.
"""
from __future__ import annotations

import hashlib
import json
import os
import struct
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

_MARKER = b"\xfd" * 4
WIRE_PREFIX_BYTES = 13
MAX_FRAME_BYTES = 1 << 20
MAX_WIRE_BYTES = MAX_FRAME_BYTES + WIRE_PREFIX_BYTES

TAP_MAGIC = b"RQL2TAP1"
END_MAGIC = b"RQL2END1"
FORMAT_NAME = "rquant-l2-frame-tap-v1"
FORMAT_VERSION = 1
_HEADER = struct.Struct("<8sHHIQQ8s")
_FOOTER = struct.Struct("<8sQ32s")
_HEADER_SIZE = _HEADER.size
_FOOTER_SIZE = _FOOTER.size
_RESERVED = b"\x00" * 8

KIND_RX = 1
KIND_TX = 2
KIND_EVENT = 3
KIND_NAMES = {KIND_RX: "rx", KIND_TX: "tx", KIND_EVENT: "event"}

MANIFEST_NAME = "manifest.json"
SEGMENT_PREFIX = "segment-"
SEGMENT_SUFFIX = ".tap"
MIN_SEGMENT_BYTES = 4096
MAX_SEGMENT_BYTES = 8 * 1024**3
MIN_QUEUE_BYTES = 64 * 1024
MAX_QUEUE_BYTES = 1024**3
MAX_QUEUE_RECORDS = 1_000_000
MIN_FSYNC_SECONDS = 0.1
MAX_FSYNC_SECONDS = 300.0

ENV_ENABLE = "RQUANT_L2_FRAME_TAP"
ENV_DIR = "RQUANT_L2_FRAME_TAP_DIR"
ENV_SEGMENT_BYTES = "RQUANT_L2_FRAME_TAP_SEGMENT_BYTES"
ENV_QUEUE_BYTES = "RQUANT_L2_FRAME_TAP_QUEUE_BYTES"
ENV_FSYNC_SECONDS = "RQUANT_L2_FRAME_TAP_FSYNC_SECONDS"

_WRITE_BATCH_RECORDS = 256
_WRITE_BATCH_BYTES = 1 << 20
_JOIN_TIMEOUT_SECONDS = 10.0
_TRUTHY = frozenset({"1", "on", "true", "yes"})
_FALSY = frozenset({"", "0", "off", "false", "no"})


class FrameTapError(RuntimeError):
    """Fixed local code; never carries raw frame bytes or upstream text."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class TapRecord:
    kind: int
    mono_us: int
    payload: bytes

    @property
    def name(self) -> str:
        return KIND_NAMES.get(self.kind, "unknown")


class FrameTapSink(Protocol):
    """Minimal sequential writer seam; tests inject stalls and failures."""

    def write(self, data: bytes) -> None: ...

    def flush(self) -> None: ...

    def fsync(self) -> None: ...

    def close(self) -> None: ...


def wire_frame(body: bytes) -> bytes:
    """Return the exact FD-framed wire bytes for one received frame body."""
    if not isinstance(body, bytes):
        raise FrameTapError("invalid_frame")
    if len(body) > MAX_FRAME_BYTES:
        raise FrameTapError("invalid_frame")
    return _MARKER + f"{len(body):08x}".encode("ascii") + b"\x00" + body


def split_wire_frame(wire: bytes) -> bytes:
    """Return the body of one FD-framed wire frame, rejecting malformed input."""
    if not isinstance(wire, bytes) or len(wire) < WIRE_PREFIX_BYTES:
        raise FrameTapError("invalid_frame")
    if (wire[:4] != _MARKER or wire[12] != 0
            or any(c not in b"0123456789abcdefABCDEF" for c in wire[4:12])):
        raise FrameTapError("invalid_frame")
    length = int(wire[4:12], 16)
    if length != len(wire) - WIRE_PREFIX_BYTES:
        raise FrameTapError("invalid_frame")
    return wire[WIRE_PREFIX_BYTES:]


def _default_clock_us() -> int:
    return time.monotonic_ns() // 1000


def _default_wall_clock_us() -> int:
    return time.time_ns() // 1000


def _encode_uvarint(value: int, out: bytearray) -> None:
    if type(value) is not int or value < 0 or value >= 1 << 64:
        raise FrameTapError("invalid_record")
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)


def _read_uvarint_stream(stream: Any, hasher: Any) -> int:
    value = shift = 0
    for _ in range(10):
        chunk = stream.read(1)
        if not chunk:
            raise FrameTapError("truncated")
        hasher.update(chunk)
        byte = chunk[0]
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            if value >= 1 << 64:
                raise FrameTapError("invalid_record")
            return value
        shift += 7
    raise FrameTapError("invalid_record")


def _read_exact(stream: Any, size: int) -> bytes:
    data = stream.read(size)
    if data is None or len(data) != size:
        raise FrameTapError("truncated")
    return data


def _meta_bytes(meta: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(meta, ensure_ascii=False, separators=(",", ":"),
                          sort_keys=True).encode("utf-8")
    except (TypeError, ValueError):
        raise FrameTapError("invalid_config") from None


class _FileSink:
    """Exclusive-created regular file sink; no symlink following."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fd: int | None = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)

    def write(self, data: bytes) -> None:
        if self._fd is None:
            raise FrameTapError("tap_io")
        view = memoryview(data)
        while view:
            written = os.write(self._fd, view)
            if written <= 0:
                raise FrameTapError("tap_io")
            view = view[written:]

    def flush(self) -> None:
        return None

    def fsync(self) -> None:
        if self._fd is None:
            raise FrameTapError("tap_io")
        os.fsync(self._fd)

    def close(self) -> None:
        if self._fd is not None:
            fd, self._fd = self._fd, None
            os.close(fd)


def _open_file_sink(path: Path) -> FrameTapSink:
    return _FileSink(path)


class _Segment:
    """One self-contained tap segment file (header + records + footer)."""

    def __init__(self, directory: Path, index: int, *, meta: bytes, started_wall_us: int,
                 started_mono_us: int, sink_factory: Callable[[Path], FrameTapSink]) -> None:
        self.path = directory / f"{SEGMENT_PREFIX}{index:06d}{SEGMENT_SUFFIX}"
        self.started_wall_us = started_wall_us
        self.records = 0
        self.size = 0
        self._hash = hashlib.sha256()
        self._sink = sink_factory(self.path)
        self._write(_HEADER.pack(TAP_MAGIC, FORMAT_VERSION, 0, len(meta),
                                 started_wall_us, started_mono_us, _RESERVED))
        self._write(meta)

    def _write(self, data: bytes) -> None:
        self._sink.write(data)
        self._hash.update(data)
        self.size += len(data)

    def add(self, kind: int, delta_us: int, payload: bytes) -> None:
        out = bytearray((kind,))
        _encode_uvarint(delta_us, out)
        _encode_uvarint(len(payload), out)
        self._write(bytes(out) + payload)
        self.records += 1

    def sync(self) -> None:
        self._sink.flush()
        self._sink.fsync()

    def close(self) -> dict[str, Any]:
        digest = self._hash.digest()
        self._sink.write(_FOOTER.pack(END_MAGIC, self.records, digest))
        self._sink.flush()
        self._sink.fsync()
        self._sink.close()
        return {"file": self.path.name, "records": self.records,
                "bytes": self.size + _FOOTER_SIZE, "sha256": digest.hex()}

    def abort(self) -> None:
        try:
            self._sink.close()
        except Exception:
            pass


class TapFileWriter:
    """Synchronous tap writer shared by the live tap and the replay-pack builder."""

    def __init__(self, directory: Path, *, meta: Mapping[str, Any], started_wall_us: int,
                 started_mono_us: int, segment_bytes: int = 256 * 1024 * 1024,
                 sink_factory: Callable[[Path], FrameTapSink] = _open_file_sink) -> None:
        if (type(segment_bytes) is not int
                or not MIN_SEGMENT_BYTES <= segment_bytes <= MAX_SEGMENT_BYTES):
            raise FrameTapError("invalid_config")
        if (type(started_wall_us) is not int or type(started_mono_us) is not int
                or started_wall_us < 0 or started_mono_us < 0):
            raise FrameTapError("invalid_config")
        self.directory = Path(directory)
        self._meta = _meta_bytes(meta)
        self._segment_bytes = segment_bytes
        self._sink_factory = sink_factory
        self._started_wall_us = started_wall_us
        self._segment_index = 0
        self._segment: _Segment | None = None
        self._segment_base_mono_us = started_mono_us
        self._last_mono_us = started_mono_us
        self._segments: list[dict[str, Any]] = []
        self._counts = {name: 0 for name in KIND_NAMES.values()}
        self._total_bytes = 0
        self._next_segment()

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    @property
    def total_bytes(self) -> int:
        return self._total_bytes + (self._segment.size if self._segment else 0)

    def _next_segment(self) -> None:
        self._segment_index += 1
        self._segment_base_mono_us = self._last_mono_us
        self._segment = _Segment(
            self.directory, self._segment_index, meta=self._meta,
            started_wall_us=self._started_wall_us if self._segment_index == 1
            else _default_wall_clock_us(),
            started_mono_us=self._segment_base_mono_us,
            sink_factory=self._sink_factory)

    def add(self, kind: int, mono_us: int, payload: bytes) -> None:
        if kind not in KIND_NAMES:
            raise FrameTapError("invalid_record")
        if not isinstance(payload, bytes) or len(payload) > MAX_WIRE_BYTES + 64:
            raise FrameTapError("invalid_record")
        if type(mono_us) is not int or mono_us < 0:
            raise FrameTapError("invalid_record")
        mono_us = max(mono_us, self._last_mono_us)
        self._finish_segment_if_needed(len(payload))
        assert self._segment is not None
        self._segment.add(kind, mono_us - self._last_mono_us, payload)
        self._last_mono_us = mono_us
        self._counts[KIND_NAMES[kind]] += 1

    def _finish_segment_if_needed(self, payload_size: int) -> None:
        if self._segment is None or self._segment.records == 0:
            return
        if self._segment.size + payload_size + 24 <= self._segment_bytes:
            return
        self._segments.append(self._segment.close())
        self._segment = None
        self._next_segment()

    def sync(self) -> None:
        if self._segment is not None:
            self._segment.sync()

    def close(self) -> dict[str, Any]:
        if self._segment is not None:
            self._segments.append(self._segment.close())
            self._segment = None
        return {"segments": list(self._segments), "records": dict(self._counts),
                "bytes": sum(segment["bytes"] for segment in self._segments)}

    def abort(self) -> None:
        if self._segment is not None:
            self._segment.abort()
            self._segment = None


def write_manifest(directory: Path, manifest: Mapping[str, Any]) -> Path:
    """Atomically publish ``manifest.json`` inside the tap directory."""
    target = Path(directory) / MANIFEST_NAME
    temporary = target.parent / f".pending-manifest-{os.getpid()}-{threading.get_ident()}"
    payload = _meta_bytes(manifest) + b"\n"
    fd: int | None = None
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise FrameTapError("tap_io")
            view = view[written:]
        os.fsync(fd)
        os.close(fd)
        fd = None
        os.replace(temporary, target)
        return target
    except OSError:
        raise FrameTapError("tap_io") from None
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def read_manifest(directory: Path) -> dict[str, Any] | None:
    path = Path(directory) / MANIFEST_NAME
    if not path.exists():
        return None
    try:
        return json.loads(path.read_bytes())
    except (OSError, ValueError):
        raise FrameTapError("invalid_manifest") from None


def iter_records(path: Path) -> Iterator[TapRecord]:
    """Yield records in order and verify the footer checksum at end of file."""
    with Path(path).open("rb") as stream:
        header = _read_exact(stream, _HEADER_SIZE)
        magic, version, flags, meta_len, started_wall_us, started_mono_us, reserved = (
            _HEADER.unpack(header))
        if magic != TAP_MAGIC:
            raise FrameTapError("invalid_header")
        if version != FORMAT_VERSION:
            raise FrameTapError("unsupported_version")
        if flags != 0 or reserved != _RESERVED or meta_len > 1 << 20:
            raise FrameTapError("invalid_header")
        meta = _read_exact(stream, meta_len)
        hasher = hashlib.sha256(header + meta)
        mono_us = started_mono_us
        count = 0
        while True:
            first = stream.read(1)
            if not first:
                raise FrameTapError("truncated")
            if first == END_MAGIC[:1]:
                rest = _read_exact(stream, 7)
                if first + rest != END_MAGIC:
                    raise FrameTapError("invalid_record")
                footer_magic, footer_count, digest = _FOOTER.unpack(
                    END_MAGIC + _read_exact(stream, 40))
                if footer_magic != END_MAGIC or footer_count != count:
                    raise FrameTapError("checksum_mismatch")
                if hasher.digest() != digest:
                    raise FrameTapError("checksum_mismatch")
                if stream.read(1):
                    raise FrameTapError("trailing_bytes")
                return
            kind = first[0]
            if kind not in KIND_NAMES:
                raise FrameTapError("invalid_record")
            hasher.update(first)
            delta_us = _read_uvarint_stream(stream, hasher)
            payload_len = _read_uvarint_stream(stream, hasher)
            if payload_len > MAX_WIRE_BYTES + 64:
                raise FrameTapError("invalid_record")
            payload = _read_exact(stream, payload_len)
            if payload_len:
                hasher.update(payload)
            mono_us += delta_us
            count += 1
            yield TapRecord(kind=kind, mono_us=mono_us, payload=payload)


def verify_tap_file(path: Path) -> dict[str, Any]:
    counts = {name: 0 for name in KIND_NAMES.values()}
    bytes_total = 0
    first_mono: int | None = None
    last_mono: int | None = None
    events = 0
    for record in iter_records(path):
        counts[record.name] += 1
        bytes_total += len(record.payload)
        first_mono = record.mono_us if first_mono is None else first_mono
        last_mono = record.mono_us
        events += record.kind == KIND_EVENT
    return {"file": Path(path).name, "records": sum(counts.values()),
            "rx": counts["rx"], "tx": counts["tx"], "event": counts["event"],
            "payload_bytes": bytes_total,
            "first_mono_us": first_mono, "last_mono_us": last_mono}


def tap_directory(root: Path, *, clock: Callable[[], datetime] | None = None) -> Path:
    """Unique child directory name for one recording run; caller creates it."""
    now = (clock or (lambda: datetime.now(UTC)))()
    stamp = now.strftime("%Y%m%dT%H%M%S") + f"{now.microsecond:06d}Z"
    return Path(root) / f"{stamp}-p{os.getpid()}"


@dataclass(frozen=True)
class FrameTapConfig:
    """Explicit switch surface; disabled configuration is ``None``."""

    directory: Path
    segment_bytes: int = 256 * 1024 * 1024
    queue_bytes: int = 8 * 1024 * 1024
    queue_records: int = 65536
    fsync_interval_seconds: float = 5.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "directory", Path(self.directory))
        if not isinstance(self.directory, Path) or not str(self.directory):
            raise FrameTapError("invalid_config")
        if (type(self.segment_bytes) is not int
                or not MIN_SEGMENT_BYTES <= self.segment_bytes <= MAX_SEGMENT_BYTES):
            raise FrameTapError("invalid_config")
        if (type(self.queue_bytes) is not int
                or not MIN_QUEUE_BYTES <= self.queue_bytes <= MAX_QUEUE_BYTES):
            raise FrameTapError("invalid_config")
        if (type(self.queue_records) is not int
                or not 1 <= self.queue_records <= MAX_QUEUE_RECORDS):
            raise FrameTapError("invalid_config")
        if (type(self.fsync_interval_seconds) not in (int, float)
                or not MIN_FSYNC_SECONDS <= self.fsync_interval_seconds <= MAX_FSYNC_SECONDS):
            raise FrameTapError("invalid_config")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> FrameTapConfig | None:
        env = os.environ if environ is None else environ
        raw = str(env.get(ENV_ENABLE, "")).strip().lower()
        directory = str(env.get(ENV_DIR, "")).strip()
        if raw in _TRUTHY:
            enabled = True
        elif raw in _FALSY and raw != "":
            return None
        elif raw == "":
            enabled = bool(directory)
        else:
            raise FrameTapError("invalid_config")
        if not enabled:
            return None
        if not directory:
            raise FrameTapError("invalid_config")
        options: dict[str, Any] = {"directory": Path(directory)}
        for name, key, converter in (
                (ENV_SEGMENT_BYTES, "segment_bytes", int),
                (ENV_QUEUE_BYTES, "queue_bytes", int),
                (ENV_FSYNC_SECONDS, "fsync_interval_seconds", float)):
            raw_value = str(env.get(name, "")).strip()
            if not raw_value:
                continue
            try:
                options[key] = converter(raw_value)
            except ValueError:
                raise FrameTapError("invalid_config") from None
        return cls(**options)


class RawFrameTap:
    """Bounded queue plus one writer thread in front of :class:`TapFileWriter`."""

    def __init__(self, config: FrameTapConfig, *,
                 clock_us: Callable[[], int] | None = None,
                 wall_clock_us: Callable[[], int] | None = None,
                 sink_factory: Callable[[Path], FrameTapSink] | None = None,
                 meta: Mapping[str, Any] | None = None) -> None:
        if not isinstance(config, FrameTapConfig):
            raise FrameTapError("invalid_config")
        self.config = config
        self._clock_us = clock_us or _default_clock_us
        self._wall_clock_us = wall_clock_us or _default_wall_clock_us
        self._sink_factory = sink_factory or _open_file_sink
        self._meta_extra = dict(meta or {})
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._queue: deque[tuple[int, int, bytes]] = deque()
        self._queue_bytes = 0
        self._dropped_frames = 0
        self._dropped_bytes = 0
        self._invalid_frames = 0
        self._gap_first_us: int | None = None
        self._closed = False
        self._active = False
        self._thread: threading.Thread | None = None
        self._writer: TapFileWriter | None = None
        self._error: str | None = None
        self._started_wall_us = 0
        self._started_mono_us = 0

    @property
    def active(self) -> bool:
        return self._active

    def start(self) -> None:
        if self._active:
            return
        try:
            self.config.directory.mkdir(parents=True, mode=0o700, exist_ok=True)
            if any(self.config.directory.iterdir()):
                raise FrameTapError("tap_dir_not_empty")
            os.chmod(self.config.directory, 0o700)
        except OSError:
            raise FrameTapError("tap_io") from None
        self._started_wall_us = int(self._wall_clock_us())
        self._started_mono_us = int(self._clock_us())
        with self._cond:
            self._closed = False
            self._active = True
        self._thread = threading.Thread(target=self._writer_loop, name="rquant-l2-frame-tap",
                                        daemon=True)
        self._thread.start()

    def _record(self, kind: int, payload: bytes) -> bool:
        if not self._active:
            return False
        now = int(self._clock_us())
        with self._cond:
            if self._closed or self._thread is None:
                return False
            if (len(self._queue) >= self.config.queue_records
                    or self._queue_bytes + len(payload) > self.config.queue_bytes):
                self._dropped_frames += 1
                self._dropped_bytes += len(payload)
                if self._gap_first_us is None:
                    self._gap_first_us = now
                return False
            self._queue.append((kind, now, payload))
            self._queue_bytes += len(payload)
            self._cond.notify()
            return True

    def _reject(self) -> bool:
        with self._cond:
            self._invalid_frames += 1
        return False

    def record_rx(self, body: bytes) -> bool:
        """Record one received frame body. Never raises, never blocks on disk."""
        try:
            return self._record(KIND_RX, wire_frame(body))
        except FrameTapError:
            return self._reject()

    def record_tx(self, wire: bytes) -> bool:
        """Record one sent wire frame exactly as handed to ``sendall``."""
        try:
            split_wire_frame(wire)
            return self._record(KIND_TX, wire)
        except FrameTapError:
            return self._reject()

    def record_event(self, event: Mapping[str, Any]) -> bool:
        """Record a control-plane marker (epoch start/end, gap note) as JSON."""
        try:
            payload = _meta_bytes(event)
            if len(payload) > 64 * 1024:
                return self._reject()
            return self._record(KIND_EVENT, payload)
        except FrameTapError:
            return self._reject()

    def _drain(self) -> list[tuple[int, int, bytes]]:
        with self._cond:
            if not self._queue:
                return []
            batch: list[tuple[int, int, bytes]] = []
            batch_bytes = 0
            while (self._queue and len(batch) < _WRITE_BATCH_RECORDS
                   and (not batch or batch_bytes < _WRITE_BATCH_BYTES)):
                kind, mono_us, payload = self._queue.popleft()
                self._queue_bytes -= len(payload)
                batch.append((kind, mono_us, payload))
                batch_bytes += len(payload)
            return batch

    def _take_gap_first_us(self) -> int | None:
        with self._cond:
            first, self._gap_first_us = self._gap_first_us, None
            return first

    def _write_batch(self, batch: list[tuple[int, int, bytes]]) -> None:
        writer = self._writer
        if writer is None:
            return
        gap_first = self._take_gap_first_us()
        if gap_first is not None:
            with self._cond:
                dropped = self._dropped_frames
            writer.add(KIND_EVENT, gap_first, _meta_bytes({
                "event": "gap", "dropped_frames": dropped}))
        for kind, mono_us, payload in batch:
            writer.add(kind, mono_us, payload)

    def _writer_loop(self) -> None:
        last_sync = time.monotonic()
        try:
            meta = {
                "format": FORMAT_NAME,
                "created_at": datetime.now(UTC).isoformat(),
                "clock": "monotonic-microseconds",
                "started_wall_us": self._started_wall_us,
                "pid": os.getpid(),
                **self._meta_extra,
            }
            self._writer = TapFileWriter(
                self.config.directory, meta=meta,
                started_wall_us=self._started_wall_us,
                started_mono_us=self._started_mono_us,
                segment_bytes=self.config.segment_bytes,
                sink_factory=self._sink_factory)
            while True:
                with self._cond:
                    if not self._queue and not self._closed:
                        self._cond.wait(timeout=self.config.fsync_interval_seconds)
                batch = self._drain()
                if batch:
                    self._write_batch(batch)
                now = time.monotonic()
                if now - last_sync >= self.config.fsync_interval_seconds:
                    self._writer.sync()
                    last_sync = now
                with self._cond:
                    if self._closed and not self._queue:
                        break
        except Exception:
            with self._cond:
                self._error = "writer_failed"
        finally:
            with self._cond:
                self._active = False
                leftover = len(self._queue)
                self._queue.clear()
                self._queue_bytes = 0
                if leftover:
                    self._dropped_frames += leftover
            self._finalize()

    def _finalize(self) -> None:
        writer = self._writer
        summary: dict[str, Any] = {"segments": [], "records": {}, "bytes": 0}
        if writer is not None:
            try:
                summary = writer.close()
            except Exception:
                with self._cond:
                    self._error = self._error or "segment_close_failed"
                writer.abort()
        manifest = {
            "format": FORMAT_NAME,
            "version": FORMAT_VERSION,
            "directory": str(self.config.directory),
            "created_at": datetime.now(UTC).isoformat(),
            "config": {
                "segment_bytes": self.config.segment_bytes,
                "queue_bytes": self.config.queue_bytes,
                "queue_records": self.config.queue_records,
                "fsync_interval_seconds": self.config.fsync_interval_seconds,
            },
            "meta": dict(self._meta_extra),
            "started_wall_us": self._started_wall_us,
            "started_mono_us": self._started_mono_us,
            "segments": summary["segments"],
            "records": summary["records"],
            "bytes": summary["bytes"],
            "dropped": {"frames": self._dropped_frames, "bytes": self._dropped_bytes},
            "invalid_frames": self._invalid_frames,
            "error": self._error,
        }
        try:
            write_manifest(self.config.directory, manifest)
        except FrameTapError:
            with self._cond:
                self._error = self._error or "manifest_failed"

    def close(self) -> dict[str, Any]:
        """Flush, join the writer thread and publish the manifest. Idempotent."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=_JOIN_TIMEOUT_SECONDS)
        if thread is not None and thread.is_alive():
            with self._cond:
                self._error = self._error or "writer_join_timeout"
        self._active = False
        manifest = read_manifest(self.config.directory)
        if manifest is not None:
            return manifest
        return {"records": {}, "segments": [], "dropped": {}, "error": self._error}

    @property
    def stats(self) -> dict[str, Any]:
        with self._cond:
            return {
                "active": self._active,
                "queued_records": len(self._queue),
                "queued_bytes": self._queue_bytes,
                "dropped_frames": self._dropped_frames,
                "dropped_bytes": self._dropped_bytes,
                "invalid_frames": self._invalid_frames,
                "error": self._error,
            }

    def __enter__(self) -> RawFrameTap:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
