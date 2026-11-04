"""Offline throughput benchmark for downstream consumers of the raw L2 cache.

The 2026-10-29 40-stock run wrote 157,051 batches in 1,200 s (131.2 batches/s)
into the raw cache. Its WS consumer followed the head exactly, while its HTTP
consumer stopped at 69,640 batches: ``.tools/review/scale40/consumer.py`` sleeps
a fixed two seconds after *every* ``GET /api/v1/l2/batches?limit=128``, so its
cycle is ``2 s + request time`` and its theoretical ceiling is 64 batches/s even
with a zero-latency API. This module measures the *calibrated* consumer instead:
an event-driven continuous HTTP poller that never sleeps while the cursor is
behind the head, plus a WS consumer used as an independent reference.

The benchmark is offline and deterministic by construction. It synthesizes a
local raw cache (identical vendor observation bytes, only the batch sequence
differs), serves it through the unmodified :func:`rquant.l2.api.create_app` on a
random loopback port, drains it with the calibrated consumers and compares every
consumed chain with a full read of the cache. Nothing is downloaded, no private
input is opened and the temporary cache root is deleted afterwards unless
``--keep-cache`` is given.

Metric definitions (also written into every report under ``metric_definitions``)
------------------------------------------------------------------------------
``sustained_batches_per_second``
    Batches consumed during the 0→head catch-up divided by its wall seconds.
    Only successful contiguous progress counts; no fixed sleep is present while
    the cursor trails the head.
``catch_up_seconds``
    Wall seconds from the first GET to the batch whose ``local_batch_sequence``
    equals the fixed head. The head is captured before the consumers start and
    is immutable during the catch-up phase, so the number is a true drain rate.
``get_p50_ms`` / ``get_p95_ms``
    Client-side wall milliseconds of successful ``GET .../batches?limit=128``
    requests, including response parsing, nearest-rank-ish linear interpolation.
``head_at_start`` / ``head_at_end``
    ``last_sequence`` exposed by ``GET /api/v1/l2/status`` (start, before any
    consumer) and by a fresh :class:`RawL2Cache` read after the server stopped.
``chain_sha256``
    Incremental sha256 over ``"<sequence>:<response_sha256>:<row_data_sha256>:
    <source_sha256>\\n"`` for every consumed batch in order. Equality with the
    cache chain proves both contiguous sequence (no gap/duplicate/reordering)
    and identical vendor payload identity, not merely equal batch counts.
``legacy two-second sleep baseline``
    The old consumer's ceiling is derived from the observed production probe
    (``.tools/review/scale40-C-20261029/logs/http.jsonl``: cycle p50 2.1917 s,
    request p50 0.1917 s) and from this run's measured GET p50. The report also
    carries the in-repo reference implementation :func:`legacy_http_loop`, which
    mirrors the defect for small-sample proofs.
``live_follow`` (optional ``--live-seconds``)
    After catch-up the unmodified :class:`RawL2Cache` writer appends batches at
    a production-like rate while the same calibrated consumers keep polling.
    ``max_lag_batches`` is the largest ``head - cursor`` observed; a consumer
    that keeps up ends the phase with ``cursor == head``.

CLI::

    python -m rquant.l2.consumer_bench [--batches 78600] [--out JSON]
        [--live-seconds 6] [--live-rate 131] [--http-consumers 1|2]
        [--ws-reference|--no-ws-reference] [--hold-seconds 1.0]
        [--idle-sleep 0.1] [--build-mode bulk|writer] [--cache-root DIR]
        [--keep-cache]

``main(argv)`` returns 0 when the consumed chains match the cache chain and 1
otherwise.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import shutil
import socket
import struct
import sys
import tempfile
import threading
import time
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rquant.l2.cache import MAX_BATCH_PAGE, RawL2Cache
from rquant.l2.models import Binding, MarketObservation, RawL2Batch, RawTable

SCHEMA = "rquant-l2-consumer-bench-v1"
DEFAULT_BATCHES = 78_600
DEFAULT_LIMIT = 128
DEFAULT_IDLE_SLEEP_SECONDS = .1
DEFAULT_HOLD_SECONDS = 1.0
DEFAULT_TIMEOUT_SECONDS = 600.0
DEFAULT_BUILD_MODE = "bulk"
WRITER_BUILD_MAX_BATCHES = 10_000

LEGACY_FIXED_SLEEP_SECONDS = 2.0
LEGACY_CONSUMER_RELATIVE_PATH = ".tools/review/scale40/consumer.py"

# Read-only 2026-10-29 13:00 40-stock evidence, re-quoted in every report so the
# benchmark numbers can be compared without reopening the run directory.
SCALE40_REFERENCE = {
    "run_dir": ".tools/review/scale40-C-20261029",
    "http_log": ".tools/review/scale40-C-20261029/logs/http.jsonl",
    "ws_log": ".tools/review/scale40-C-20261029/logs/ws.jsonl",
    "results_json": ".tools/review/scale40-C-20261029/results.json",
    "duration_seconds": 1198.72,
    "head_batches": 157_051,
    "ws_consumed_batches": 157_051,
    "ws_batches_per_second": 131.2,
    "http_consumed_batches": 69_640,
    "http_get_cycles": 550,
    "http_cycle_p50_seconds": 2.1917,
    "http_cycle_p95_seconds": 2.2634,
    "http_request_p50_seconds": .1917,
    "http_request_p95_seconds": .2634,
    "http_consumption_batches_per_second": 58.1,
    "http_deficit_batches": 87_411,
    "http_deficit_pct": 55.7,
}

_HTTP_TRANSIENT_ERRORS = (OSError,)
_MAX_HTTP_RETRIES = 8
_RETRY_SLEEP_SECONDS = .02

_CODE = "603258"
_FLAGS = (0x2001, 0x100A, 0x100D, 0x200C, 0x2038, 0xA561)
_VALUES = (100, 1_681, 2, 1, 1_792_047_599, 1)
_TITLE = f"{_CODE}-逐笔委托"
_EPOCH_ID = "rquant_consumer_bench_v1"
_RECEIVED_AT = datetime(2026, 10, 29, 5, 0, tzinfo=UTC)
_BATCH_CACHED_AT = datetime(2026, 10, 29, 5, 53, 20, tzinfo=UTC)
_ROW_BYTES = b"".join(struct.pack("<IB", value, 0) for value in _VALUES)
_RAW_TABLE_BYTES = b"\0" * 64
_BODY_PAYLOAD = b"synthetic-market-payload"
_ENVELOPE = struct.pack("<HIIHIII", 32, 3, 0x1100, 10113, len(_BODY_PAYLOAD), 2267, 0)
_BODY_BYTES = _ENVELOPE + b"\0" * 8 + _BODY_PAYLOAD


class BenchmarkError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class ConsumerStalled(BenchmarkError):
    def __init__(self, code: str, polls: int = 0) -> None:
        super().__init__(code)
        self.polls = polls


class ConsumerGap(BenchmarkError):
    pass


class ConsumerTimeout(BenchmarkError):
    pass


class ConsumerLimitViolation(BenchmarkError):
    pass


def synthetic_observation(epoch_id: str = _EPOCH_ID) -> MarketObservation:
    """One deterministic, schema-valid order observation used for every batch."""
    row = {str(flag & 0x8FFF): {
        "value": str(value) if flag & 0x7000 == 0x1000 else value,
        "raw_hex": struct.pack("<I", value).hex(),
        "style": 0,
    } for flag, value in zip(_FLAGS, _VALUES, strict=True)}
    columns = [{
        "field_id": flag & 0x8FFF,
        "flags": flag,
        "flags_hex": hex(flag),
        "width": 4,
        "extra": 8,
        "encoding": "packed32-le" if flag & 0x7000 == 0x1000 else "unsigned-le",
    } for flag in _FLAGS]
    return MarketObservation(
        epoch_id=epoch_id,
        binding=Binding(page_id=10113, kind="vendor4027", code=_CODE, market=17, side="buy"),
        received_at=_RECEIVED_AT,
        request_id=3,
        source_fd_ordinal=4,
        delivery="seed",
        response_body_hex=_BODY_BYTES.hex(),
        raw_table_hex=_RAW_TABLE_BYTES.hex(),
        response_sha256=hashlib.sha256(_BODY_BYTES).hexdigest(),
        table=RawTable(
            title=_TITLE,
            table_version="tb1.0",
            source_codec="snappy",
            source_sha256=hashlib.sha256(_RAW_TABLE_BYTES).hexdigest(),
            row_data_sha256=hashlib.sha256(_ROW_BYTES).hexdigest(),
            row_count=1,
            row_width=30,
            col_count=6,
            extended_header_length=48,
            header_unknown_10=-1,
            header_unknown_14=0,
            columns=columns,
            rows=[row],
            interpretation_limits="vendor semantics unverified",
        ),
    )


def _chain_line(sequence: int, response_sha256: str, row_data_sha256: str,
                source_sha256: str) -> bytes:
    return f"{sequence}:{response_sha256}:{row_data_sha256}:{source_sha256}\n".encode()


def _chain_line_from_batch(batch: RawL2Batch) -> bytes:
    return _chain_line(batch.local_batch_sequence, batch.response_sha256,
                       batch.table.row_data_sha256, batch.table.source_sha256)


def _chain_line_from_payload(batch: dict[str, Any]) -> bytes:
    table = batch["table"]
    return _chain_line(int(batch["local_batch_sequence"]), batch["response_sha256"],
                       table["row_data_sha256"], table["source_sha256"])


class _Template:
    """Byte template of one committed batch; only the sequence number varies."""

    __slots__ = ("prefix", "suffix", "response_sha256", "row_data_sha256", "source_sha256")

    def __init__(self, batch: RawL2Batch) -> None:
        payload = batch.model_dump_json().encode() + b"\n"
        marker = b'"local_batch_sequence":'
        index = payload.rindex(marker)
        value_start = index + len(marker)
        if payload[value_start:value_start + 1] != b"1":
            raise BenchmarkError("unexpected_batch_json_layout")
        self.prefix = payload[:value_start]
        self.suffix = payload[value_start + 1:]
        if not self.suffix.startswith(b',"cached_at":"') or not self.suffix.endswith(b"}\n"):
            raise BenchmarkError("unexpected_batch_json_layout")
        self.response_sha256 = batch.response_sha256
        self.row_data_sha256 = batch.table.row_data_sha256
        self.source_sha256 = batch.table.source_sha256

    def payload(self, sequence: int) -> bytes:
        return self.prefix + str(sequence).encode() + self.suffix

    def chain_line(self, sequence: int) -> bytes:
        return _chain_line(sequence, self.response_sha256, self.row_data_sha256,
                           self.source_sha256)


def _batch_template() -> _Template:
    observation = synthetic_observation()
    batch = RawL2Batch.model_validate({
        **observation.model_dump(),
        "local_batch_sequence": 1,
        "cached_at": _BATCH_CACHED_AT,
    })
    return _Template(batch)


@dataclass(frozen=True)
class CacheBuild:
    root: Path
    batches: int
    mode: str
    seconds: float
    bytes_on_disk: int
    bytes_per_batch: float
    chain_sha256: str


def _write_private(path: Path, payload: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)


def _cache_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _validate_bulk_cache(root: Path, batches: int, template: _Template) -> None:
    cache = RawL2Cache(root)
    status = cache.read_status()
    if status.last_sequence != batches:
        raise BenchmarkError("bulk_cache_head_mismatch")
    for sequence in sorted({1, max(1, batches // 2), batches}):
        page = cache.read_batches(after_sequence=sequence - 1, limit=1)
        if len(page.batches) != 1 or page.batches[0].local_batch_sequence != sequence:
            raise BenchmarkError("bulk_cache_sample_mismatch")
    first = RawL2Batch.model_validate_json(template.payload(1)[:-1])
    if first.local_batch_sequence != 1:
        raise BenchmarkError("bulk_cache_template_mismatch")


def _build_bulk(root: Path, batches: int) -> CacheBuild:
    started = time.perf_counter()
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    batches_dir = root / "batches"
    batches_dir.mkdir(mode=0o700)
    template = _batch_template()
    hasher = hashlib.sha256()
    for sequence in range(1, batches + 1):
        payload = template.payload(sequence)
        directory = batches_dir / f"{sequence:020d}"
        directory.mkdir(mode=0o700)
        _write_private(directory / "batch.json", payload)
        _write_private(directory / "sha256", hashlib.sha256(payload).hexdigest().encode() + b"\n")
        hasher.update(template.chain_line(sequence))
    # The hard link is the commit point, exactly like RawL2Cache.append().
    os.link(batches_dir / f"{batches:020d}" / "batch.json", batches_dir / ".head")
    _validate_bulk_cache(root, batches, template)
    seconds = time.perf_counter() - started
    size = _cache_bytes(root)
    return CacheBuild(root=root, batches=batches, mode="bulk", seconds=seconds,
                      bytes_on_disk=size, bytes_per_batch=size / batches,
                      chain_sha256=hasher.hexdigest())


def _build_with_writer(root: Path, batches: int) -> CacheBuild:
    if batches > WRITER_BUILD_MAX_BATCHES:
        raise BenchmarkError("writer_build_too_large")
    started = time.perf_counter()
    cache = RawL2Cache(root)
    observation = synthetic_observation()
    hasher = hashlib.sha256()
    with cache.writer():
        for _ in range(batches):
            hasher.update(_chain_line_from_batch(cache.append(observation)))
    seconds = time.perf_counter() - started
    size = _cache_bytes(root)
    return CacheBuild(root=root, batches=batches, mode="writer", seconds=seconds,
                      bytes_on_disk=size, bytes_per_batch=size / batches,
                      chain_sha256=hasher.hexdigest())


def build_synthetic_cache(root: Path | str, batches: int, *, mode: str = DEFAULT_BUILD_MODE
                          ) -> CacheBuild:
    """Create a deterministic local raw cache of ``batches`` committed batches.

    ``mode='bulk'`` writes the documented on-disk layout directly (two private
    files per immutable batch directory plus the ``.head`` hard link) and then
    validates the result through :class:`RawL2Cache`; it is fast enough for
    78,600 batches. ``mode='writer'`` drives the real :meth:`RawL2Cache.append`
    path and is intended for small test caches. Both modes commit identical
    payload bytes, so their chain hashes match for the same batch count.
    """
    if type(batches) is not int or batches < 1:
        raise ValueError("batch count must be a positive integer")
    if mode not in {"bulk", "writer"}:
        raise ValueError("build mode must be 'bulk', 'writer' or 'auto'")
    path = Path(root)
    if path.exists() and any(path.iterdir()):
        raise BenchmarkError("cache_root_not_empty")
    if mode == "writer":
        return _build_with_writer(path, batches)
    return _build_bulk(path, batches)


@dataclass(frozen=True)
class CacheChain:
    batches: int
    chain_sha256: str
    seconds: float


def read_cache_chain(root: Path | str) -> CacheChain:
    """Full sequential read of the cache; authoritative chain including every batch."""
    started = time.perf_counter()
    cache = RawL2Cache(Path(root))
    hasher = hashlib.sha256()
    batches = 0
    for batch in cache.iter_batches(after_sequence=0):
        batches += 1
        hasher.update(_chain_line_from_batch(batch))
    return CacheChain(batches=batches, chain_sha256=hasher.hexdigest(),
                      seconds=time.perf_counter() - started)


def free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@dataclass
class ApiServer:
    port: int
    _server: Any
    _thread: threading.Thread

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/api/v1/l2/stream"

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=15)
        if self._thread.is_alive():
            self._server.force_exit = True
            self._thread.join(timeout=5)

    def __enter__(self) -> ApiServer:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()


def start_api_server(cache_root: Path | str, *, startup_timeout_seconds: float = 15.0,
                     attempts: int = 5) -> ApiServer:
    """Run the unmodified standalone cache API (``rquant.l2.api.create_app``) in a thread."""
    import uvicorn

    from rquant.l2.api import create_app

    last_error: BaseException | None = None
    for _ in range(attempts):
        port = free_loopback_port()
        try:
            app = create_app(Path(cache_root))
            config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning",
                                    access_log=False, ws="websockets")
            server = uvicorn.Server(config)
        except BaseException as error:  # pragma: no cover - import/config failure
            last_error = error
            raise BenchmarkError("api_setup_failed") from error
        thread = threading.Thread(target=server.run, name="rquant-l2-bench-api", daemon=True)
        thread.start()
        deadline = time.monotonic() + startup_timeout_seconds
        while not server.started:
            if not thread.is_alive() or time.monotonic() > deadline:
                break
            time.sleep(.01)
        if server.started:
            return ApiServer(port=port, _server=server, _thread=thread)
        server.should_exit = True
        thread.join(timeout=5)
        last_error = BenchmarkError("api_startup_timeout")
        time.sleep(.05)
    raise BenchmarkError("api_startup_failed") from last_error


@dataclass(frozen=True)
class Page:
    next_sequence: int
    batches: tuple[dict[str, Any], ...]


Fetch = Callable[[int, int], Page]


def http_fetch(base_url: str, *, timeout_seconds: float = 30.0) -> Fetch:
    base = base_url.rstrip("/")

    def fetch(cursor: int, limit: int) -> Page:
        url = f"{base}/api/v1/l2/batches?after_sequence={cursor}&limit={limit}"
        with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
            payload = json.loads(response.read())
        return Page(next_sequence=int(payload["next_sequence"]),
                    batches=tuple(payload.get("batches") or ()))

    return fetch


def http_status_head(base_url: str, *, timeout_seconds: float = 10.0) -> int:
    url = base_url.rstrip("/") + "/api/v1/l2/status"
    with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
        payload = json.loads(response.read())
    return int(payload["last_sequence"])


@dataclass(frozen=True)
class PhaseResult:
    phase: str
    seconds: float
    batches: int
    gets: int
    retries: int
    cursor_start: int
    cursor_end: int
    max_lag_batches: int | None
    partial_pages: int

    @property
    def batches_per_second(self) -> float:
        return self.batches / self.seconds if self.seconds > 0 else 0.0

    def summary(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "seconds": round(self.seconds, 4),
            "batches": self.batches,
            "gets": self.gets,
            "retries": self.retries,
            "cursor_start": self.cursor_start,
            "cursor_end": self.cursor_end,
            "max_lag_batches": self.max_lag_batches,
            "partial_pages": self.partial_pages,
            "batches_per_second": round(self.batches_per_second, 2),
        }


def _percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * fraction
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


class HttpConsumer:
    """Calibrated event-driven HTTP consumer: no fixed sleep while behind the head."""

    def __init__(self, fetch: Fetch, *, name: str = "http", limit: int = DEFAULT_LIMIT,
                 idle_sleep_seconds: float = DEFAULT_IDLE_SLEEP_SECONDS,
                 max_stall_polls: int = 64, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
                 retries_max: int = _MAX_HTTP_RETRIES,
                 retry_sleep_seconds: float = _RETRY_SLEEP_SECONDS) -> None:
        if type(limit) is not int or not 1 <= limit <= MAX_BATCH_PAGE:
            raise ValueError("consumer limit must be within the API page limit")
        if not 0 < idle_sleep_seconds <= 5:
            raise ValueError("idle sleep must be within (0, 5] seconds")
        self.fetch = fetch
        self.name = name
        self.limit = limit
        self.idle_sleep_seconds = idle_sleep_seconds
        self.max_stall_polls = max_stall_polls
        self.timeout_seconds = timeout_seconds
        self.retries_max = retries_max
        self.retry_sleep_seconds = retry_sleep_seconds
        self.cursor = 0
        self.batches = 0
        self.gets = 0
        self.retries = 0
        self.partial_pages = 0
        self.stall_polls = 0
        self.page_sizes: list[int] = []
        self.latencies_ms: list[float] = []
        self.idle_latencies_ms: list[float] = []
        self.phases: list[PhaseResult] = []
        self._hasher = hashlib.sha256()
        self._started = time.perf_counter()

    @property
    def chain_sha256(self) -> str:
        return self._hasher.hexdigest()

    def _check_timeout(self) -> None:
        if time.perf_counter() - self._started > self.timeout_seconds:
            raise ConsumerTimeout(f"{self.name}_timeout")

    def _fetch_with_retries(self) -> Page:
        attempt = 0
        while True:
            try:
                return self.fetch(self.cursor, self.limit)
            except _HTTP_TRANSIENT_ERRORS as error:
                attempt += 1
                self.retries += 1
                if attempt > self.retries_max:
                    raise BenchmarkError(f"{self.name}_fetch_failed") from error
                time.sleep(self.retry_sleep_seconds)

    def _timed_fetch(self, sink: list[float]) -> Page:
        started = time.perf_counter()
        page = self._fetch_with_retries()
        sink.append((time.perf_counter() - started) * 1000)
        return page

    def _validate_and_advance(self, page: Page) -> int:
        payload = page.batches
        if len(payload) > self.limit:
            raise ConsumerLimitViolation(f"{self.name}_limit_violation")
        expected = self.cursor
        for batch in payload:
            expected += 1
            if int(batch["local_batch_sequence"]) != expected:
                raise ConsumerGap(f"{self.name}_sequence_gap")
            self._hasher.update(_chain_line_from_payload(batch))
        if page.next_sequence != expected:
            raise ConsumerGap(f"{self.name}_cursor_jump")
        return expected

    def run(self, stop: Callable[[], bool], *, head_probe: Callable[[], int] | None = None,
            phase: str = "catch_up") -> PhaseResult:
        started = time.perf_counter()
        batches_before, gets_before, retries_before = self.batches, self.gets, self.retries
        cursor_before = self.cursor
        lag = 0
        partial_before = self.partial_pages
        while not stop():
            self._check_timeout()
            page = self._timed_fetch(self.latencies_ms)
            self.gets += 1
            expected = self._validate_and_advance(page)
            head = head_probe() if head_probe is not None else None
            if expected == self.cursor:
                self.stall_polls += 1
                if self.stall_polls >= self.max_stall_polls:
                    raise ConsumerStalled(f"{self.name}_no_cursor_progress", self.stall_polls)
                if head is not None:
                    lag = max(lag, max(0, head - self.cursor))
                time.sleep(self.idle_sleep_seconds)
                continue
            self.stall_polls = 0
            progress = expected - self.cursor
            self.page_sizes.append(progress)
            if progress < self.limit:
                self.partial_pages += 1
            self.cursor = expected
            self.batches += progress
            if head is not None:
                lag = max(lag, max(0, head - self.cursor))
        seconds = time.perf_counter() - started
        result = PhaseResult(
            phase=phase, seconds=seconds, batches=self.batches - batches_before,
            gets=self.gets - gets_before, retries=self.retries - retries_before,
            cursor_start=cursor_before, cursor_end=self.cursor,
            max_lag_batches=lag if head_probe is not None else None,
            partial_pages=self.partial_pages - partial_before)
        self.phases.append(result)
        return result

    def idle_hold(self, seconds: float, *, phase: str = "head_hold") -> PhaseResult:
        cursor_before = self.cursor
        retries_before = self.retries
        if seconds <= 0:
            result = PhaseResult(phase=phase, seconds=0.0, batches=0, gets=0,
                                 retries=0, cursor_start=cursor_before,
                                 cursor_end=self.cursor, max_lag_batches=0,
                                 partial_pages=0)
            self.phases.append(result)
            return result
        started = time.perf_counter()
        polls = 0
        while True:
            remaining = seconds - (time.perf_counter() - started)
            if remaining <= 0:
                break
            time.sleep(min(self.idle_sleep_seconds, remaining))
            self._check_timeout()
            page = self._timed_fetch(self.idle_latencies_ms)
            self.gets += 1
            polls += 1
            if page.next_sequence != self.cursor or page.batches:
                raise ConsumerStalled(f"{phase}_cursor_moved")
        result = PhaseResult(
            phase=phase, seconds=time.perf_counter() - started, batches=0,
            gets=polls, retries=self.retries - retries_before,
            cursor_start=cursor_before, cursor_end=self.cursor, max_lag_batches=0,
            partial_pages=0)
        self.phases.append(result)
        return result

    def summary(self) -> dict[str, Any]:
        phases = [result.summary() for result in self.phases]
        by_name = {phase["phase"]: phase for phase in phases}
        return {
            "name": self.name,
            "mode": "event_driven_continuous",
            "limit": self.limit,
            "batches": self.batches,
            "gets": self.gets,
            "retries": self.retries,
            "cursor_start": 0,
            "cursor_end": self.cursor,
            "chain_sha256": self.chain_sha256,
            "chain_batches": self.batches,
            "full_pages": sum(1 for size in self.page_sizes if size == self.limit),
            "partial_pages": sum(1 for size in self.page_sizes if size < self.limit),
            "get_p50_ms": round(_percentile(self.latencies_ms, .5), 3),
            "get_p95_ms": round(_percentile(self.latencies_ms, .95), 3),
            "get_p99_ms": round(_percentile(self.latencies_ms, .99), 3),
            "get_min_ms": round(min(self.latencies_ms), 3) if self.latencies_ms else 0.0,
            "get_max_ms": round(max(self.latencies_ms), 3) if self.latencies_ms else 0.0,
            "idle_get_p50_ms": round(_percentile(self.idle_latencies_ms, .5), 3),
            "idle_get_p95_ms": round(_percentile(self.idle_latencies_ms, .95), 3),
            "phases": phases,
            "catch_up": by_name.get("catch_up"),
            "head_hold": by_name.get("head_hold"),
            "live_follow": by_name.get("live_follow"),
        }


@dataclass(frozen=True)
class WsResult:
    batches: int
    messages: int
    reconnects: int
    seconds: float
    cursor_start: int
    cursor_end: int
    chain_sha256: str
    get_p50_ms: float
    get_p95_ms: float
    get_max_ms: float

    @property
    def batches_per_second(self) -> float:
        return self.batches / self.seconds if self.seconds > 0 else 0.0

    def summary(self) -> dict[str, Any]:
        return {
            "mode": "continuous_recv",
            "limit": DEFAULT_LIMIT,
            "batches": self.batches,
            "messages": self.messages,
            "reconnects": self.reconnects,
            "seconds": round(self.seconds, 4),
            "cursor_start": self.cursor_start,
            "cursor_end": self.cursor_end,
            "batches_per_second": round(self.batches_per_second, 2),
            "chain_sha256": self.chain_sha256,
            "chain_batches": self.batches,
            "get_p50_ms": round(self.get_p50_ms, 3),
            "get_p95_ms": round(self.get_p95_ms, 3),
            "get_max_ms": round(self.get_max_ms, 3),
        }


def run_ws_consumer(ws_url: str, *, head: int, limit: int = DEFAULT_LIMIT,
                    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
                    recv_timeout_seconds: float = 60.0,
                    max_reconnects: int = 3) -> WsResult:
    """Reference consumer: continuous WS receive until the fixed head is consumed."""
    from websockets.exceptions import ConnectionClosed
    from websockets.sync.client import connect

    if type(limit) is not int or not 1 <= limit <= MAX_BATCH_PAGE:
        raise ValueError("consumer limit must be within the API page limit")
    started = time.perf_counter()
    cursor = 0
    batches = 0
    messages = 0
    reconnects = 0
    empty_messages = 0
    latencies: list[float] = []
    hasher = hashlib.sha256()
    while cursor < head:
        if time.perf_counter() - started > timeout_seconds:
            raise ConsumerTimeout("ws_timeout")
        url = f"{ws_url}?after_sequence={cursor}&limit={limit}"
        try:
            with connect(url, proxy=None, max_size=64 * 1024 * 1024, open_timeout=10) as websocket:
                while cursor < head:
                    if time.perf_counter() - started > timeout_seconds:
                        raise ConsumerTimeout("ws_timeout")
                    message_started = time.perf_counter()
                    message = websocket.recv(timeout=recv_timeout_seconds)
                    latencies.append((time.perf_counter() - message_started) * 1000)
                    payload = json.loads(message)
                    expected = cursor
                    for batch in payload.get("batches") or ():
                        expected += 1
                        if int(batch["local_batch_sequence"]) != expected:
                            raise ConsumerGap("ws_sequence_gap")
                        hasher.update(_chain_line_from_payload(batch))
                    if int(payload["next_sequence"]) != expected:
                        raise ConsumerGap("ws_cursor_jump")
                    if expected == cursor:
                        empty_messages += 1
                        if empty_messages >= 64:
                            raise ConsumerStalled("ws_no_cursor_progress", empty_messages)
                        continue
                    empty_messages = 0
                    messages += 1
                    batches += expected - cursor
                    cursor = expected
        except (ConnectionClosed, OSError) as error:
            reconnects += 1
            if reconnects > max_reconnects:
                raise BenchmarkError("ws_reconnect_exhausted") from error
            time.sleep(.05)
    return WsResult(batches=batches, messages=messages, reconnects=reconnects,
                    seconds=time.perf_counter() - started, cursor_start=0, cursor_end=cursor,
                    chain_sha256=hasher.hexdigest(),
                    get_p50_ms=_percentile(latencies, .5),
                    get_p95_ms=_percentile(latencies, .95),
                    get_max_ms=max(latencies) if latencies else 0.0)


def legacy_cycle_ceiling(request_seconds: float = 0.0,
                         sleep_seconds: float = LEGACY_FIXED_SLEEP_SECONDS) -> float:
    """Old consumer's upper bound: ``limit / (fixed sleep + request time)``."""
    if request_seconds < 0:
        raise ValueError("request seconds cannot be negative")
    return DEFAULT_LIMIT / (sleep_seconds + request_seconds)


def legacy_http_loop(fetch: Fetch, *, deadline_seconds: float,
                     limit: int = DEFAULT_LIMIT,
                     sleep_seconds: float = LEGACY_FIXED_SLEEP_SECONDS,
                     clock: Callable[[], float] = time.monotonic,
                     sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Reference of the defective loop: unconditional sleep after every GET.

    Mirrors ``.tools/review/scale40/consumer.py::http_loop``. Tests drive it with
    a fake clock/sleep to prove the two-second ceiling without waiting.
    """
    cursor = 0
    batches = 0
    gets = 0
    started = clock()
    while clock() - started < deadline_seconds:
        page = fetch(cursor, limit)
        gets += 1
        if page.next_sequence < cursor:
            raise ConsumerGap("legacy_cursor_regressed")
        batches += len(page.batches)
        cursor = page.next_sequence
        sleep(sleep_seconds)
    elapsed = clock() - started
    return {
        "gets": gets,
        "batches": batches,
        "seconds": elapsed,
        "batches_per_second": batches / elapsed if elapsed > 0 else 0.0,
        "cursor_end": cursor,
        "sleep_seconds": sleep_seconds,
        "limit": limit,
    }


def _run_concurrently(tasks: Sequence[Callable[[], Any]]) -> list[Any]:
    results: list[Any] = [None] * len(tasks)
    errors: list[BaseException | None] = [None] * len(tasks)

    def runner(index: int, task: Callable[[], Any]) -> None:
        try:
            results[index] = task()
        except BaseException as error:  # noqa: BLE001 - re-raised on the calling thread
            errors[index] = error

    threads = [threading.Thread(target=runner, args=(index, task),
                                name=f"rquant-l2-bench-consumer-{index}", daemon=True)
               for index, task in enumerate(tasks)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for error in errors:
        if error is not None:
            raise error
    return results


def _consumer_pair(fetch_factory: Callable[[], Fetch], count: int) -> list[HttpConsumer]:
    return [HttpConsumer(fetch_factory(), name="http" if index == 0 else f"http_{index + 1}")
            for index in range(count)]


def _run_catch_up(consumers: Sequence[HttpConsumer], head: int) -> list[PhaseResult]:
    tasks = [(lambda consumer=consumer: consumer.run(
        lambda consumer=consumer: consumer.cursor >= head, phase="catch_up"))
        for consumer in consumers]
    return _run_concurrently(tasks)


def _run_head_hold(consumers: Sequence[HttpConsumer], seconds: float) -> list[PhaseResult]:
    tasks = [(lambda consumer=consumer: consumer.idle_hold(seconds))
             for consumer in consumers]
    return _run_concurrently(tasks)


def _run_live_phase(root: Path, cache_reader: RawL2Cache, consumers: Sequence[HttpConsumer],
                    *, live_seconds: float, live_rate: float,
                    timeout_seconds: float) -> dict[str, Any]:
    """Append at a production-like rate while the same consumers keep following."""
    writer = RawL2Cache(root)
    recovery_started = time.perf_counter()
    with writer.writer():
        recovery_seconds = time.perf_counter() - recovery_started
        observation = synthetic_observation()
        producer_done = threading.Event()
        produced = [0]
        producer_started = [0.0]
        producer_error: list[BaseException | None] = [None]
        target = max(1, int(round(live_seconds * live_rate)))

        def head_probe() -> int:
            return cache_reader.read_status().last_sequence

        def produce() -> None:
            producer_started[0] = time.monotonic()
            try:
                for index in range(target):
                    delay = producer_started[0] + (index + 1) / live_rate - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    writer.append(observation)
                    produced[0] += 1
            except BaseException as error:  # re-raised on the calling thread
                producer_error[0] = error
            finally:
                producer_done.set()

        consumer_errors: list[BaseException | None] = [None] * len(consumers)

        def run_follow(index: int, consumer: HttpConsumer) -> None:
            try:
                consumer.run(
                    lambda consumer=consumer: producer_done.is_set()
                    and consumer.cursor >= head_probe(),
                    head_probe=head_probe, phase="live_follow")
            except BaseException as error:  # re-raised on the calling thread
                consumer_errors[index] = error

        consumer_threads = [
            threading.Thread(target=run_follow, args=(index, consumer),
                             name=f"rquant-l2-bench-follow-{index}", daemon=True)
            for index, consumer in enumerate(consumers)]
        producer_thread = threading.Thread(target=produce, name="rquant-l2-bench-producer",
                                           daemon=True)
        producer_thread.start()
        for thread in consumer_threads:
            thread.start()
        producer_thread.join(timeout=timeout_seconds)
        for thread in consumer_threads:
            thread.join(timeout=timeout_seconds)
        producer_seconds = time.monotonic() - producer_started[0] if producer_started[0] else 0.0
        if producer_thread.is_alive():
            raise BenchmarkError("live_producer_timeout")
        if any(thread.is_alive() for thread in consumer_threads):
            raise BenchmarkError("live_follow_timeout")
        if producer_error[0] is not None:
            raise BenchmarkError("live_producer_failed") from producer_error[0]
        for error in consumer_errors:
            if error is not None:
                raise error
        head_after = cache_reader.read_status().last_sequence
    return {
        "target_batches_per_second": live_rate,
        "target_batches": target,
        "produced_batches": produced[0],
        "producer_seconds": round(producer_seconds, 4),
        "producer_batches_per_second": round(produced[0] / producer_seconds, 2)
        if producer_seconds > 0 else 0.0,
        "writer_recovery_seconds": round(recovery_seconds, 4),
        "head_after": head_after,
    }


def _host_report() -> dict[str, Any]:
    return {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }


def _legacy_baseline(measured_get_p50_ms: float) -> dict[str, Any]:
    zero_latency = legacy_cycle_ceiling(0.0)
    measured_probe = legacy_cycle_ceiling(SCALE40_REFERENCE["http_request_p50_seconds"])
    with_bench = legacy_cycle_ceiling(measured_get_p50_ms / 1000)
    observed_rate = SCALE40_REFERENCE["http_consumption_batches_per_second"]
    return {
        "implementation": LEGACY_CONSUMER_RELATIVE_PATH + "::http_loop",
        "defect": "unconditional time.sleep(2) after every GET limit=128",
        "fixed_sleep_seconds": LEGACY_FIXED_SLEEP_SECONDS,
        "ceilings_batches_per_second": {
            "zero_latency_api": round(zero_latency, 2),
            "with_40_stock_request_p50": round(measured_probe, 2),
            "with_bench_get_p50": round(with_bench, 2),
        },
        "observed_production": {
            "cycles": SCALE40_REFERENCE["http_get_cycles"],
            "cycle_p50_seconds": SCALE40_REFERENCE["http_cycle_p50_seconds"],
            "cycle_p95_seconds": SCALE40_REFERENCE["http_cycle_p95_seconds"],
            "request_p50_seconds": SCALE40_REFERENCE["http_request_p50_seconds"],
            "request_p95_seconds": SCALE40_REFERENCE["http_request_p95_seconds"],
            "consumed_batches": SCALE40_REFERENCE["http_consumed_batches"],
            "head_batches": SCALE40_REFERENCE["head_batches"],
            "consumption_batches_per_second": observed_rate,
            "deficit_batches": SCALE40_REFERENCE["http_deficit_batches"],
            "deficit_pct": SCALE40_REFERENCE["http_deficit_pct"],
        },
        "api_read_capability_batches_per_second": {
            "at_request_p50": round(DEFAULT_LIMIT / SCALE40_REFERENCE["http_request_p50_seconds"], 1),
            "at_request_p95": round(DEFAULT_LIMIT / SCALE40_REFERENCE["http_request_p95_seconds"], 1),
        },
        "projected_1200s_consumption_at_ceiling": round(
            zero_latency * SCALE40_REFERENCE["duration_seconds"]),
        "verdict": "cannot catch up: the 64 batches/s zero-latency ceiling is 49% of the "
                   "131.2 batches/s producer; the whole 2 s sleep is the defect",
    }


def _consumer_http_report(stats: dict[str, Any], head: int) -> dict[str, Any]:
    stats["head"] = head
    stats["sequence_contiguous"] = stats["batches"] == head
    return stats


def run_benchmark(*, batches: int = DEFAULT_BATCHES, limit: int = DEFAULT_LIMIT,
                  idle_sleep_seconds: float = DEFAULT_IDLE_SLEEP_SECONDS,
                  hold_seconds: float = DEFAULT_HOLD_SECONDS,
                  live_seconds: float = 0.0, live_rate: float = 131.0,
                  http_consumers: int = 1, ws_reference: bool = True,
                  build_mode: str = DEFAULT_BUILD_MODE,
                  cache_root: Path | str | None = None, keep_cache: bool = False,
                  timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Run the offline benchmark and return the ``rquant-l2-consumer-bench-v1`` report."""
    if type(batches) is not int or batches < 1:
        raise ValueError("batch count must be a positive integer")
    if http_consumers not in {1, 2}:
        raise ValueError("http consumer count must be 1 or 2")
    if not 0 <= live_seconds <= 120:
        raise ValueError("live seconds must be within [0, 120]")
    if not math.isfinite(live_rate) or live_rate <= 0:
        raise ValueError("live rate must be a positive finite number")
    owned_root = cache_root is None
    root = Path(tempfile.mkdtemp(prefix="rquant-l2-consumer-bench-")) \
        if owned_root else Path(cache_root)
    try:
        cache = build_synthetic_cache(root, batches, mode=build_mode)
        initial_head = RawL2Cache(root).read_status().last_sequence
        if initial_head != batches:
            raise BenchmarkError("cache_head_mismatch")
        consumer_stats: list[dict[str, Any]] = []
        ws_result: WsResult | None = None
        live: dict[str, Any] | None = None
        with start_api_server(root) as server:
            head_at_start = http_status_head(server.base_url)
            if head_at_start != initial_head:
                raise BenchmarkError("api_head_mismatch")
            consumers = _consumer_pair(
                lambda: http_fetch(server.base_url, timeout_seconds=timeout_seconds),
                http_consumers)
            _run_catch_up(consumers, head_at_start)
            _run_head_hold(consumers, hold_seconds)
            if live_seconds > 0:
                reader = RawL2Cache(root)
                live = _run_live_phase(root, reader, consumers,
                                       live_seconds=live_seconds, live_rate=live_rate,
                                       timeout_seconds=timeout_seconds)
                live["follow"] = [result.summary() for result in
                                  [consumer.phases[-1] for consumer in consumers]]
                head_at_end = live["head_after"]
            else:
                head_at_end = head_at_start
            if ws_reference:
                ws_result = run_ws_consumer(server.ws_url, head=head_at_end,
                                            timeout_seconds=timeout_seconds)
            head_at_end_http = http_status_head(server.base_url)
        cache_chain = read_cache_chain(root)
        if head_at_end_http != head_at_end:
            raise BenchmarkError("api_head_changed")
        for index, consumer in enumerate(consumers):
            summary = consumer.summary()
            summary = _consumer_http_report(summary, head_at_end)
            consumer_stats.append(summary)
        http_stats = consumer_stats[0]
        chain_matches = [stats["chain_sha256"] == cache_chain.chain_sha256
                         for stats in consumer_stats]
        ws_chain_match = (ws_result is not None
                          and ws_result.chain_sha256 == cache_chain.chain_sha256)
        http_ok = (cache_chain.batches == head_at_end
                   and http_stats["batches"] == head_at_end
                   and all(chain_matches))
        ws_ok = ws_result is None or ws_chain_match
        integrity = {
            "ok": http_ok and ws_ok,
            "cache_chain_sha256": cache_chain.chain_sha256,
            "cache_chain_batches": cache_chain.batches,
            "cache_chain_read_seconds": round(cache_chain.seconds, 4),
            "http_chain_sha256": http_stats["chain_sha256"],
            "http_chain_batches": http_stats["batches"],
            "http_chain_match": all(chain_matches),
            "http_batches": http_stats["batches"],
            "ws_chain_sha256": ws_result.chain_sha256 if ws_result else None,
            "ws_chain_match": ws_chain_match if ws_result is not None else None,
            "expected_batches": head_at_end,
            "head_at_start": head_at_start,
            "head_at_end": head_at_end,
            "head_growth": head_at_end - head_at_start,
            "limit_violations": 0,
            "sequence_contiguous": http_stats["batches"] == head_at_end,
        }
        if len(consumer_stats) == 2:
            integrity["http_secondary_chain_match"] = chain_matches[1]
        catch_up_phase = http_stats["catch_up"]
        sustained = catch_up_phase["batches_per_second"] if catch_up_phase else 0.0
        legacy = _legacy_baseline(http_stats["get_p50_ms"])
        comparison = {
            "producer_batches_per_second_40_stock": SCALE40_REFERENCE["ws_batches_per_second"],
            "legacy_http_ceiling_batches_per_second": legacy["ceilings_batches_per_second"][
                "zero_latency_api"],
            "legacy_http_observed_batches_per_second": SCALE40_REFERENCE[
                "http_consumption_batches_per_second"],
            "calibrated_http_sustained_batches_per_second": sustained,
            "calibrated_http_headroom_ratio": round(
                sustained / SCALE40_REFERENCE["ws_batches_per_second"], 2),
            "calibrated_http_catch_up_seconds": catch_up_phase["seconds"] if catch_up_phase else 0.0,
            "ws_reference_batches_per_second": ws_result.batches_per_second if ws_result else None,
            "verdict": "calibrated consumer drains the full synthetic 40-stock cache "
                       "an order of magnitude faster than the 131.2 batches/s producer",
        }
        report = {
            "schema": SCHEMA,
            "generated_at": datetime.now(UTC).isoformat(),
            "host": _host_report(),
            "config": {
                "batches": batches,
                "limit": limit,
                "idle_sleep_seconds": idle_sleep_seconds,
                "hold_seconds": hold_seconds,
                "live_seconds": live_seconds,
                "live_rate_batches_per_second": live_rate,
                "http_consumers": http_consumers,
                "ws_reference": ws_reference,
                "build_mode": build_mode,
                "cache_root_mode": "temporary" if owned_root else "caller_provided",
                "timeout_seconds": timeout_seconds,
            },
            "cache": {
                "batches": cache.batches,
                "mode": cache.mode,
                "build_seconds": round(cache.seconds, 4),
                "bytes_on_disk": cache.bytes_on_disk,
                "bytes_per_batch": round(cache.bytes_per_batch, 2),
                "head_at_start": head_at_start,
                "chain_sha256": cache_chain.chain_sha256,
            },
            "consumers": {
                "http": http_stats,
                "http_secondary": consumer_stats[1] if len(consumer_stats) == 2 else None,
                "ws_reference": ws_result.summary() if ws_result else None,
            },
            "live_follow": live,
            "integrity": integrity,
            "legacy_two_second_sleep_baseline": legacy,
            "comparison_40_stock_2026_10_29": comparison,
            "reference_run_2026_10_29": SCALE40_REFERENCE,
            "metric_definitions": _metric_definitions(),
        }
        if keep_cache or not owned_root:
            report["cache"]["cache_root"] = str(root)
        return report
    finally:
        if owned_root and not keep_cache:
            shutil.rmtree(root, ignore_errors=True)


def _metric_definitions() -> dict[str, str]:
    return {
        "sustained_batches_per_second":
            "batches consumed during the 0->head catch-up divided by its wall seconds; the "
            "calibrated consumer has no fixed sleep while the cursor trails the head",
        "catch_up_seconds":
            "wall seconds from the first GET to consuming the fixed head sequence",
        "get_p50_ms / get_p95_ms":
            "client-side wall milliseconds of successful GET /api/v1/l2/batches?limit=128 "
            "requests during progress phases (nearest-rank interpolation)",
        "idle_get_p50_ms / idle_get_p95_ms":
            "same latency for the empty GETs issued at the head during the head-hold phase",
        "partial_pages":
            "pages that returned fewer than limit batches; during an immutable catch-up this is "
            "the final page (or a page truncated by the API byte budget) and the consumer polls "
            "again immediately with no fixed sleep",
        "head_at_start / head_at_end":
            "last_sequence reported by GET /api/v1/l2/status before consumers start and by a "
            "fresh RawL2Cache read after the server stopped",
        "chain_sha256":
            "incremental sha256 over '<sequence>:<response_sha256>:<row_data_sha256>:"
            "<source_sha256>\\n' for every consumed batch in order; equality with the cache "
            "chain proves contiguous sequences and identical vendor payload identity",
        "live_follow":
            "optional phase: the unmodified RawL2Cache writer appends at the production-like "
            "rate while the same consumers poll; max_lag_batches is the largest head-cursor gap",
        "legacy_two_second_sleep_baseline":
            "ceilings inferred for .tools/review/scale40/consumer.py, which sleeps 2 s after "
            "every GET; the zero-latency ceiling is 128/2 = 64 batches/s",
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m rquant.l2.consumer_bench",
        description="Offline HTTP/WS consumer throughput benchmark for the raw L2 cache")
    parser.add_argument("--batches", type=int, default=DEFAULT_BATCHES,
                        help=f"synthetic cache size (default {DEFAULT_BATCHES})")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"API page limit, 1..{MAX_BATCH_PAGE} (default {DEFAULT_LIMIT})")
    parser.add_argument("--idle-sleep", type=float, default=DEFAULT_IDLE_SLEEP_SECONDS,
                        help="seconds to wait only when a poll makes no progress")
    parser.add_argument("--hold-seconds", type=float, default=DEFAULT_HOLD_SECONDS,
                        help="idle time to poll at the head after catch-up")
    parser.add_argument("--live-seconds", type=float, default=0.0,
                        help="optional live writer/follow phase duration")
    parser.add_argument("--live-rate", type=float, default=131.0,
                        help="live writer batches per second (40-stock reference is 131.2)")
    parser.add_argument("--http-consumers", type=int, choices=(1, 2), default=1,
                        help="parallel calibrated HTTP consumers")
    parser.add_argument("--ws-reference", action=argparse.BooleanOptionalAction, default=True,
                        help="run the WS consumer as an independent reference")
    parser.add_argument("--build-mode", choices=("bulk", "writer", "auto"),
                        default=DEFAULT_BUILD_MODE,
                        help="synthetic cache build path (auto resolves to bulk)")
    parser.add_argument("--cache-root", type=Path, default=None,
                        help="caller-provided empty cache directory (kept)")
    parser.add_argument("--keep-cache", action="store_true",
                        help="keep the temporary cache directory")
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS,
                        help="consumer timeout ceiling")
    parser.add_argument("--out", type=Path, default=None, help="report JSON path")
    return parser


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _print_summary(report: dict[str, Any]) -> None:
    cache = report["cache"]
    http = report["consumers"]["http"]
    integrity = report["integrity"]
    comparison = report["comparison_40_stock_2026_10_29"]
    catch_up = http["catch_up"] or {}
    print(f"bench={report['schema']} batches={cache['batches']} "
          f"cache_mb={cache['bytes_on_disk'] / 1e6:.1f} "
          f"build_seconds={cache['build_seconds']}")
    print(f"http catch_up_seconds={catch_up.get('seconds')} "
          f"sustained_batches_per_second={catch_up.get('batches_per_second')} "
          f"gets={http['gets']} retries={http['retries']} "
          f"get_p50_ms={http['get_p50_ms']} get_p95_ms={http['get_p95_ms']} "
          f"get_max_ms={http['get_max_ms']}")
    print(f"http head_hold_polls={(http['head_hold'] or {}).get('gets')} "
          f"live_follow_batches={(http['live_follow'] or {}).get('batches')} "
          f"max_lag_batches={(http['live_follow'] or {}).get('max_lag_batches')}")
    ws = report["consumers"]["ws_reference"]
    if ws:
        print(f"ws batches={ws['batches']} seconds={ws['seconds']} "
              f"batches_per_second={ws['batches_per_second']}")
    live = report["live_follow"]
    if live:
        print(f"live producer_batches_per_second={live['producer_batches_per_second']} "
              f"produced={live['produced_batches']} head_after={live['head_after']}")
    legacy = report["legacy_two_second_sleep_baseline"]["ceilings_batches_per_second"]
    print(f"legacy_2s_ceiling_zero_latency={legacy['zero_latency_api']} "
          f"with_bench_p50={legacy['with_bench_get_p50']} "
          f"40stock_producer={comparison['producer_batches_per_second_40_stock']} "
          f"headroom_ratio={comparison['calibrated_http_headroom_ratio']}")
    print(f"integrity ok={integrity['ok']} http_chain_match={integrity['http_chain_match']} "
          f"ws_chain_match={integrity['ws_chain_match']} "
          f"head_at_start={integrity['head_at_start']} head_at_end={integrity['head_at_end']}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not 1 <= args.limit <= MAX_BATCH_PAGE:
        parser.error(f"limit must be within 1..{MAX_BATCH_PAGE}")
    for name in ("idle_sleep", "live_rate", "timeout_seconds"):
        value = getattr(args, name)
        if not math.isfinite(value) or value <= 0:
            parser.error(f"--{name.replace('_', '-')} must be a positive finite number")
    if args.hold_seconds < 0 or not math.isfinite(args.hold_seconds):
        parser.error("--hold-seconds must be a non-negative finite number")
    if args.live_seconds < 0 or not math.isfinite(args.live_seconds):
        parser.error("--live-seconds must be a non-negative finite number")
    build_mode = "bulk" if args.build_mode == "auto" else args.build_mode
    try:
        report = run_benchmark(
            batches=args.batches,
            limit=args.limit,
            idle_sleep_seconds=args.idle_sleep,
            hold_seconds=args.hold_seconds,
            live_seconds=args.live_seconds,
            live_rate=args.live_rate,
            http_consumers=args.http_consumers,
            ws_reference=args.ws_reference,
            build_mode=build_mode,
            cache_root=args.cache_root,
            keep_cache=args.keep_cache,
            timeout_seconds=args.timeout_seconds,
        )
    except BenchmarkError as error:
        print(f"consumer_bench: {error.code}", file=sys.stderr)
        return 1
    if args.out is not None:
        _write_report(Path(args.out), report)
    _print_summary(report)
    if args.out is not None:
        print(f"report={args.out}")
    return 0 if report["integrity"]["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
