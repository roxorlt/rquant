"""One-command offline benchmark gate for the deterministic L2 replay engine.

The gate drives the *unmodified* :class:`rquant.l2.feed.FeedCollector` from an
:class:`rquant.l2.replay_source.L2ReplaySource`, so every number it reports comes
from the same collector/cache path a live run would use. It writes a
``rquant-l2-replay-gate-v1`` JSON report that the L2 card can attach as evidence.

Metric definitions (also summarized in ``notes`` of every report)
------------------------------------------------------------------
``seed_first_packet_rate``
    Streams whose final subscription receipt (``RawL2Cache.read_subscription_status``)
    carries a non-null ``seed_sequence``, divided by the number of streams. A
    stream whose seed timed out and that later received a candidate push has
    ``state='first_push_observed'`` but no seed receipt, so state must never be
    used for this metric. The report also gives ``streams_total``,
    ``stocks_total``, ``seeded_streams``, ``stocks_seeded``, ``seed_queries`` and
    ``seed_timeout_streams`` (receipt streams without a seed receipt).
``consumption_latency``
    Per committed market frame: wall nanoseconds from ``ReplayConnection.recv()``
    returning the frame to ``RawL2Cache.append()`` having fsynced it
    (``perf_counter_ns``). The connection factory is wrapped to keep the live
    connection and read its ``last_delivery_wall_ns``; the cache is a
    :class:`RawL2Cache` subclass that times around ``super().append``. Percentiles
    use nearest rank and are rounded to the nearest whole microsecond. This is an
    in-process recv-to-disk latency, not a downstream HTTP/WS consumer latency.
``disk_write``
    Total bytes of every file below the scenario cache directory (walk sum,
    1 MB = 1e6 bytes) divided by the wall seconds of ``collector.run()``; also
    reports ``batches`` and ``cache_bytes``.
``memory``
    Resident-set samples of the gate process from a background thread that runs
    ``ps -o rss= -p <pid>`` through ``subprocess`` (no psutil), every 0.25 s and
    once more after the run. Where a sandbox cannot execute the setuid ``ps``
    binary the sampler falls back to ``/proc/<pid>/statm`` (Linux),
    ``libproc.proc_pidinfo`` (macOS) or ``resource.getrusage`` (peak envelope),
    and records the chosen ``method``/``fallback_reason`` in the report.

Scenarios
---------
``healthy-40`` (main metrics) / ``healthy-80`` / ``healthy-100`` /
``fault-seed-24of40`` / ``fault-single-silent`` / ``fault-single-stall`` /
``determinism-40`` (the same tier is replayed twice into separate cache roots and
the committed batch sequence hash must match). Every scenario runs in its own
``tempfile.mkdtemp`` cache root which is deleted afterwards unless ``--keep`` is
given; nothing is written into a project private directory.

The gate is offline by construction: it never opens a socket, never reads
``.env``/credentials/production state, and only consumes a local replay pack plus
either caller-provided private inputs (``--inputs-dir``) or locally synthesized
bootstrap/order/trade inputs (77-byte header + iPhone64 payload, mode 0600). The
synthesized inputs are marked ``synthetic`` in the report.

CLI::

    python -m rquant.l2.replay_gate gate --pack DIR --out JSON [--push-seconds F|none]
                                         [--speed 0|1|10] [--keep] [--inputs-dir DIR]
    python -m rquant.l2.replay_gate run  --pack DIR --stocks 40
                                         [--fault healthy|seed-24of40|single-silent|single-stall]
                                         [--out JSON]

``main(argv)`` returns 0 for a passing gate and 1 otherwise.
"""
from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import hashlib
import json
import math
import os
import platform
import resource
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ._codec import MAX_SECONDS
from ._request import build_order_query, build_trade_query, encode_payload
from .cache import CacheError, RawL2Cache
from .feed import FeedCollector
from .models import FeedStatus, MarketObservation, RawL2Batch, SubscriptionReceipt
from .replay_source import (MAX_PUSH_WINDOW_SECONDS, FaultInjection, L2ReplaySource, PackIndex,
                            ReplaySourceError, build_source, read_pack_index)
from .symbols import MAX_SYMBOLS

GATE_SCHEMA = "rquant-l2-replay-gate-v1"
DEFAULT_BUDGET_MS = 180_000
MEMORY_SAMPLE_INTERVAL_SECONDS = 0.25
MAIN_SCENARIO_NAME = "healthy-40"
SYNTHETIC_FILLER_BASE = 690000
SIMULATED_STOCK_UNIVERSE = 40

# 2026-10-29 scale40-C evidence: 120 seed queries, 72 seeded / 24 seeded stocks,
# 48 failed streams, run ends stopped/duration_limit (the 72/120 shape).
SCALE40_FAULT_BASELINE: dict[str, dict[str, int]] = {
    "fault-seed-24of40": {"seeded_streams": 72, "seeded_stocks": 24,
                          "failed_streams": 48, "seed_queries": 120},
    "fault-single-silent": {"seeded_streams": 117, "seeded_stocks": 39,
                            "failed_streams": 3, "seed_queries": 120},
    "fault-single-stall": {"seeded_streams": 120, "seeded_stocks": 40,
                           "failed_streams": 0, "seed_queries": 120},
}

# First two recorded stocks are the only real market evidence in the pack; the
# remaining 38 codes come from the 2026-10-28 scale-40 plan but are replayed as
# clones of 603258, so every cloned stream is reported synthetic.
REAL_TIER: tuple[str, ...] = tuple(
    "603258 300006 600000 600519 600036 601318 600900 601166 600276 601888 "
    "601398 600030 601899 688981 000001 000333 000858 002594 300750 300059 "
    "603861 605058 000823 603533 605287 300530 001300 002805 002838 002709 "
    "002733 001206 300769 000710 600789 002923 002728 002273 002913 603980".split()
)
SINGLE_FAULT_SYMBOL = REAL_TIER[1]
FAULT_CHOICES = ("healthy", "seed-24of40", "single-silent", "single-stall", "seed-latency")
INPUT_FILENAMES = {
    "bootstrap": "bootstrap.bin",
    "order": "order-template.bin",
    "trade": "trade-template.bin",
}


class GateError(RuntimeError):
    """Fixed local code; never carries private input bytes or upstream text."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _RssProbeError(RuntimeError):
    """Raised when the preferred ``ps`` RSS probe cannot run at all."""


@dataclass(frozen=True)
class TierSymbols:
    """One replay tier: requested symbols plus which of them are real vs synthetic."""

    symbols: tuple[str, ...]
    real: tuple[str, ...]
    synthetic: tuple[str, ...]


@dataclass(frozen=True)
class ScenarioSpec:
    """One gate scenario: a tier, an injected fault set and a repeat count.

    ``connections``/``flow_control`` exercise the batched-connection and
    subscription-flow-control collector modes; the legacy default stays a
    single connection with one-second pacing so the measured 2026-10-29
    baselines remain comparable.
    """

    name: str
    stocks: int
    faults: FaultInjection = field(default_factory=FaultInjection)
    runs: int = 1
    connections: int = 1
    flow_control: bool = False

    def __post_init__(self) -> None:
        if (not isinstance(self.name, str) or not self.name
                or any(not (character.isalnum() or character in "._-")
                       for character in self.name)):
            raise GateError("invalid_scenario")
        if type(self.stocks) is not int or not 1 <= self.stocks <= MAX_SYMBOLS:
            raise GateError("invalid_scenario")
        if type(self.runs) is not int or not 1 <= self.runs <= 4:
            raise GateError("invalid_scenario")
        if not isinstance(self.faults, FaultInjection):
            raise GateError("invalid_scenario")
        if type(self.connections) is not int or not 1 <= self.connections <= 16:
            raise GateError("invalid_scenario")
        if type(self.flow_control) is not bool:
            raise GateError("invalid_scenario")


@dataclass(frozen=True)
class GateInputs:
    """The three private collector inputs, synthetic or caller-provided."""

    bootstrap: Path
    order: Path
    trade: Path
    directory: Path
    synthetic: bool

    @property
    def source(self) -> str:
        return "synthetic" if self.synthetic else "external"

    def published(self) -> dict[str, Any]:
        notes = (["离线合成 bootstrap/order/trade（77 字节头 + iPhone64 载荷，mode 0600）；"
                  "合成输入不代表真实设备、账号或会话。"] if self.synthetic else
                 ["caller-provided private inputs; the gate passes their paths to the collector "
                  "and never reads or prints their contents."])
        return {
            "source": self.source,
            "synthetic": self.synthetic,
            "directory": str(self.directory),
            "files": {name: path.name for name, path in self._paths().items()},
            "notes": notes,
        }

    def _paths(self) -> dict[str, Path]:
        return {"bootstrap": self.bootstrap, "order": self.order, "trade": self.trade}


@dataclass(frozen=True)
class _RunArtifacts:
    """Everything one collector run produced that the report needs."""

    status: FeedStatus
    receipt: SubscriptionReceipt | None
    cache_root: Path
    batches: int
    cache_bytes: int
    wall_seconds: float
    latency_samples_ns: tuple[int, ...]
    memory: dict[str, Any]
    collector_metrics: dict[str, Any] = field(default_factory=dict)


def tier_symbols(stocks: int) -> TierSymbols:
    """Real 2026-10-28 codes first, then synthetic ``690000+i`` placeholders."""
    if type(stocks) is not int or not 1 <= stocks <= MAX_SYMBOLS:
        raise GateError("invalid_stocks")
    real = REAL_TIER[:stocks]
    symbols = list(real)
    synthetic: list[str] = []
    known = set(REAL_TIER)
    index = 0
    while len(symbols) < stocks:
        code = f"{SYNTHETIC_FILLER_BASE + index:06d}"
        index += 1
        if code in known:
            continue
        symbols.append(code)
        synthetic.append(code)
    return TierSymbols(symbols=tuple(symbols), real=tuple(real), synthetic=tuple(synthetic))


# Calibrated replay model of the 2026-10-29 seed wait curve (see
# evidence/rquant-l2-scale40-C-seed-latency-2026-11-04.json): the wait stays
# near zero through ~57 registrable pages and reaches 14.1 s at 72 pages,
# 16.0 s at 73 pages, which is exactly where scale40-C crossed the 15 s guard.
SEED_LATENCY_FAULT = FaultInjection(seed_latency_onset_pages=57,
                                    seed_latency_coefficient=0.0625)


def default_scenarios() -> tuple[ScenarioSpec, ...]:
    """The fixed gate scenario set; ``healthy-40`` supplies the card metrics."""
    return (
        ScenarioSpec(name=MAIN_SCENARIO_NAME, stocks=SIMULATED_STOCK_UNIVERSE),
        ScenarioSpec(name="healthy-80", stocks=80),
        ScenarioSpec(name="healthy-100", stocks=100),
        ScenarioSpec(name="fault-seed-24of40", stocks=SIMULATED_STOCK_UNIVERSE,
                     faults=FaultInjection(withhold_seed_from_stock=24)),
        ScenarioSpec(name="fault-single-silent", stocks=SIMULATED_STOCK_UNIVERSE,
                     faults=FaultInjection(withhold_seed_codes=(SINGLE_FAULT_SYMBOL,))),
        ScenarioSpec(name="fault-single-stall", stocks=SIMULATED_STOCK_UNIVERSE,
                     faults=FaultInjection(stall_codes=(SINGLE_FAULT_SYMBOL,))),
        ScenarioSpec(name="fault-seed-latency-40", stocks=SIMULATED_STOCK_UNIVERSE,
                     faults=SEED_LATENCY_FAULT),
        ScenarioSpec(name="fault-seed-latency-40-batched", stocks=SIMULATED_STOCK_UNIVERSE,
                     faults=SEED_LATENCY_FAULT, connections=4, flow_control=True),
        ScenarioSpec(name="determinism-40", stocks=SIMULATED_STOCK_UNIVERSE, runs=2),
    )


def fault_injection(name: str, symbols: Sequence[str]) -> FaultInjection:
    """Map one ``run`` fault name onto a deterministic gap on this tier."""
    if name == "healthy":
        return FaultInjection()
    if not symbols:
        raise GateError("invalid_symbols")
    if name == "seed-24of40":
        return FaultInjection(withhold_seed_from_stock=min(24, len(symbols)))
    if name == "seed-latency":
        return SEED_LATENCY_FAULT
    symbol = symbols[1] if len(symbols) > 1 else symbols[0]
    if name == "single-silent":
        return FaultInjection(withhold_seed_codes=(symbol,))
    if name == "single-stall":
        return FaultInjection(stall_codes=(symbol,))
    raise GateError("invalid_fault")


def synthetic_input_bytes() -> dict[str, bytes]:
    """Build structurally valid collector inputs without touching private state.

    Mirrors the reviewed synthetic fixtures: the bootstrap is a 77-byte header
    plus an opaque carrier, and each template is the observed 77-byte request
    header plus an iPhone64-encoded initial query. Nothing here authenticates,
    nor does it claim a device/session identity.
    """
    carrier = b"SYNTHETIC-REPLAY-GATE"
    bootstrap = struct.pack("<HIIHIII", 77, 0, 0x70000, 0, len(carrier), 0, 0)
    bootstrap += bytes(range(53)) + carrier
    order_query = build_order_query(code="603258", market=17, side="buy", rowcount=20,
                                    endtime=0, orderid=0, page_id=10117)
    trade_query = build_trade_query(code="603258", market=17, page_id=10117)
    return {
        "bootstrap": bootstrap,
        "order": _request_template(order_query, command=2267),
        "trade": _request_template(trade_query, command=2357),
    }


def _request_template(query: bytes, *, command: int) -> bytes:
    encoded = encode_payload(query)
    header = struct.pack("<HIIHIII", 77, 212, 0x01010000, 0xFF03, len(encoded), command,
                         len(query))
    return header + bytes(range(53)) + encoded


def write_synthetic_inputs(directory: Path | str) -> dict[str, Path]:
    """Write the synthetic inputs as owner-only regular files inside ``directory``."""
    root = Path(directory)
    try:
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
    except OSError as error:
        raise GateError("input_write_error") from error
    paths: dict[str, Path] = {}
    bodies = synthetic_input_bytes()
    for name, body in bodies.items():
        path = root / INPUT_FILENAMES[name]
        _write_private(path, body)
        paths[name] = path
    return paths


def _write_private(path: Path, body: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
        os.chmod(path, 0o600)
    except OSError as error:
        raise GateError("input_write_error") from error


def _prepare_inputs(inputs_dir: Path | str | None, root: Path) -> GateInputs:
    if inputs_dir is None:
        paths = write_synthetic_inputs(root / "inputs")
        return GateInputs(bootstrap=paths["bootstrap"], order=paths["order"], trade=paths["trade"],
                          directory=root / "inputs", synthetic=True)
    directory = Path(inputs_dir)
    paths = {name: directory / filename for name, filename in INPUT_FILENAMES.items()}
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise GateError("inputs_missing")
    return GateInputs(bootstrap=paths["bootstrap"], order=paths["order"], trade=paths["trade"],
                      directory=directory, synthetic=False)


class _ConnectionLatencyTap:
    """Wrap ``source.connect`` and remember every replay connection."""

    def __init__(self, factory: Callable[..., Any]) -> None:
        self._factory = factory
        self.connections: list[Any] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        connection = self._factory(*args, **kwargs)
        self.connections.append(connection)
        return connection

    def last_delivery_wall_ns(self) -> int:
        newest = 0
        for connection in self.connections:
            try:
                value = connection.last_delivery_wall_ns
            except AttributeError:
                continue
            if type(value) is int and value > newest:
                newest = value
        return newest


class _TimedCache(RawL2Cache):
    """RawL2Cache that records the recv-to-committed-append wall latency per frame."""

    def __init__(self, root: Path, tap: _ConnectionLatencyTap,
                 samples_ns: list[int]) -> None:
        super().__init__(root)
        self._tap = tap
        self._samples_ns = samples_ns

    def append(self, observation: MarketObservation) -> RawL2Batch:
        delivered_ns = self._tap.last_delivery_wall_ns()
        batch = super().append(observation)
        if delivered_ns:
            self._samples_ns.append(time.perf_counter_ns() - delivered_ns)
        return batch


def _ps_rss_kb(pid: int) -> int | None:
    """Read RSS in KiB with the portable ``ps -o rss= -p`` command."""
    try:
        completed = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                                   capture_output=True, text=True, timeout=5, check=False)
    except PermissionError as error:
        raise _RssProbeError("ps_exec_denied") from error
    except FileNotFoundError as error:
        raise _RssProbeError("ps_missing") from error
    except (OSError, subprocess.SubprocessError) as error:
        raise _RssProbeError("ps_unavailable") from error
    if completed.returncode != 0:
        return None
    for line in completed.stdout.splitlines():
        text = line.strip()
        if text.isdigit() and int(text) > 0:
            return int(text)
    return None


def _proc_statm_rss_kb() -> int | None:
    """Linux fallback: resident pages of this process from ``/proc/self/statm``."""
    try:
        fields = Path("/proc/self/statm").read_text(encoding="ascii").split()
        pages = int(fields[1])
    except (OSError, IndexError, ValueError):
        return None
    page_size = os.sysconf("SC_PAGE_SIZE")
    if type(page_size) is not int or page_size <= 0 or pages <= 0:
        return None
    return pages * page_size // 1024


def _proc_taskinfo_type() -> type[ctypes.Structure]:
    class ProcTaskInfo(ctypes.Structure):
        _fields_ = [
            ("pti_virtual_size", ctypes.c_uint64),
            ("pti_resident_size", ctypes.c_uint64),
            ("pti_total_user", ctypes.c_uint64),
            ("pti_total_system", ctypes.c_uint64),
            ("pti_threads_user", ctypes.c_uint64),
            ("pti_threads_system", ctypes.c_uint64),
            ("pti_policy", ctypes.c_int32),
            ("pti_faults", ctypes.c_int32),
            ("pti_pageins", ctypes.c_int32),
            ("pti_cow_faults", ctypes.c_int32),
            ("pti_messages_sent", ctypes.c_int32),
            ("pti_messages_received", ctypes.c_int32),
            ("pti_syscalls_mach", ctypes.c_int32),
            ("pti_syscalls_unix", ctypes.c_int32),
            ("pti_csw", ctypes.c_int32),
            ("pti_threadnum", ctypes.c_int32),
            ("pti_numrunning", ctypes.c_int32),
            ("pti_priority", ctypes.c_int32),
        ]

    return ProcTaskInfo


def _libproc_rss_kb(pid: int) -> int | None:
    """macOS fallback: current resident size through ``libproc.proc_pidinfo``."""
    try:
        library = ctypes.CDLL(ctypes.util.find_library("proc") or "/usr/lib/libproc.dylib")
    except OSError:
        return None
    info_type = _proc_taskinfo_type()
    probe = library.proc_pidinfo
    probe.restype = ctypes.c_int
    probe.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p,
                      ctypes.c_int]
    info = info_type()
    written = probe(pid, 4, 0, ctypes.byref(info), ctypes.sizeof(info))
    if written != ctypes.sizeof(info) or info.pti_resident_size <= 0:
        return None
    return int(info.pti_resident_size // 1024)


def _rusage_peak_kb() -> int | None:
    """Last-resort fallback: process peak RSS only (no current-RSS curve)."""
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if value <= 0:
        return None
    return value // 1024 if platform.system() == "Darwin" else value


def _resolve_rss_reader(pid: int) -> tuple[Callable[[], int | None], str, str | None]:
    """Pick the best available RSS reader and explain any fallback."""
    reason: str | None = None
    try:
        value = _ps_rss_kb(pid)
    except _RssProbeError as error:
        reason = str(error)
    else:
        if value is not None:
            def read_ps() -> int | None:
                return _ps_rss_kb(pid)

            return read_ps, "ps", None
        reason = "ps_no_value"
    if Path("/proc/self/statm").exists():
        def read_proc() -> int | None:
            return _proc_statm_rss_kb()

        return read_proc, "proc_statm", reason
    if platform.system() == "Darwin":
        def read_libproc() -> int | None:
            return _libproc_rss_kb(pid)

        if read_libproc() is not None:
            return read_libproc, "libproc", reason
    def read_rusage() -> int | None:
        return _rusage_peak_kb()

    if read_rusage() is not None:
        return read_rusage, "rusage_peak", reason

    def read_nothing() -> int | None:
        return None

    return read_nothing, "unavailable", reason or "no_rss_reader"


class _MemorySampler:
    """Background RSS sampler; ``ps`` first, documented fallbacks otherwise."""

    def __init__(self, *, interval_seconds: float = MEMORY_SAMPLE_INTERVAL_SECONDS) -> None:
        if type(interval_seconds) not in (int, float) or not 0 < interval_seconds <= 5:
            raise GateError("invalid_memory_interval")
        self.interval_seconds = float(interval_seconds)
        self.pid = os.getpid()
        self.method = "unavailable"
        self.fallback_reason: str | None = None
        self._reader: Callable[[], int | None] | None = None
        self._samples: list[tuple[int, float]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_ns = 0

    def start(self) -> None:
        self._started_ns = time.perf_counter_ns()
        self._reader, self.method, self.fallback_reason = _resolve_rss_reader(self.pid)
        self._thread = threading.Thread(target=self._run, name="rquant-l2-gate-memory",
                                        daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._sample()
            self._stop.wait(self.interval_seconds)

    def _sample(self) -> None:
        reader = self._reader
        if reader is None:
            return
        try:
            value_kb = reader()
        except Exception:  # a sampler failure must never break the measured run
            return
        if value_kb is None or value_kb <= 0:
            return
        elapsed_ms = (time.perf_counter_ns() - self._started_ns) // 1_000_000
        self._samples.append((int(elapsed_ms), value_kb / 1024.0))

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=10.0)
            self._thread = None
        self._sample()

    def summary(self) -> dict[str, Any]:
        return _summarize_memory(self._samples, method=self.method,
                                 fallback_reason=self.fallback_reason)


def _summarize_memory(samples: Sequence[tuple[int, float]], *, method: str,
                      fallback_reason: str | None) -> dict[str, Any]:
    values = sorted(rss_mb for _t_ms, rss_mb in samples)
    return {
        "curve": [[t_ms, round(rss_mb, 3)] for t_ms, rss_mb in samples],
        "max_mb": round(values[-1], 3) if values else 0.0,
        "p50_mb": round(_median(values), 3) if values else 0.0,
        "samples": len(values),
        "method": method,
        "fallback_reason": fallback_reason,
    }


def _median(ordered: Sequence[float]) -> float:
    count = len(ordered)
    if count == 0:
        return 0.0
    middle = count // 2
    if count % 2:
        return float(ordered[middle])
    return (ordered[middle - 1] + ordered[middle]) / 2


def _nearest_rank(ordered: Sequence[int], fraction: float) -> int:
    rank = max(1, math.ceil(fraction * len(ordered)))
    return int(ordered[rank - 1])


def _percentiles_us(samples_ns: Sequence[int]) -> dict[str, int]:
    if not samples_ns:
        return {"p50": 0, "p95": 0, "p99": 0, "max": 0, "samples": 0}
    ordered = sorted(samples_ns)

    def microseconds(value_ns: int) -> int:
        return (value_ns + 500) // 1000

    return {
        "p50": microseconds(_nearest_rank(ordered, 0.50)),
        "p95": microseconds(_nearest_rank(ordered, 0.95)),
        "p99": microseconds(_nearest_rank(ordered, 0.99)),
        "max": microseconds(ordered[-1]),
        "samples": len(ordered),
    }


def _committed_batches(root: Path) -> int:
    batches = root / "batches"
    if not batches.is_dir():
        return 0
    return sum(1 for entry in batches.iterdir()
               if entry.is_dir() and entry.name.isdigit() and len(entry.name) == 20)


def _cache_bytes(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file() and not path.is_symlink():
                total += path.stat().st_size
        except OSError:
            continue
    return total


def _read_receipt(cache: RawL2Cache) -> SubscriptionReceipt | None:
    try:
        return cache.read_subscription_status()
    except CacheError:
        return None


def _seed_metrics(receipt: SubscriptionReceipt | None, *, streams: int,
                  stocks: int) -> dict[str, Any]:
    if receipt is None:
        return {"streams_total": streams, "stocks_total": stocks, "seeded_streams": 0,
                "stocks_seeded": 0, "seed_timeout_streams": streams,
                "seed_first_packet_rate": 0.0}
    receipt_streams = receipt.streams
    total = len(receipt_streams) or streams
    seeded = [stream for stream in receipt_streams if stream.seed_sequence is not None]
    codes = {stream.binding.code for stream in receipt_streams}
    seeded_codes = {stream.binding.code for stream in seeded}
    return {
        "streams_total": total,
        "stocks_total": len(codes) or stocks,
        "seeded_streams": len(seeded),
        "stocks_seeded": len(seeded_codes),
        "seed_timeout_streams": total - len(seeded),
        "seed_first_packet_rate": round(len(seeded) / total, 6) if total else 0.0,
    }


def _status_metrics(status: FeedStatus) -> dict[str, Any]:
    return {
        "state": status.state,
        "error_code": status.error_code,
        "seed_queries": status.seed_queries,
        "market_batch_count": status.market_batch_count,
        "last_sequence": status.last_sequence,
        "heartbeat_count": status.heartbeat_count,
        "control_count": status.control_count,
        "unknown_push_count": status.unknown_push_count,
        "reconnect_attempts": status.reconnect_attempts,
        "gap": status.gap,
        "subscription_state": status.subscription_state,
    }


def _batch_sequence_digest(root: Path) -> str:
    """Hash the committed ``(page_id, code, kind, side, delivery, sha256)`` order."""
    cache = RawL2Cache(Path(root))
    digest = hashlib.sha256()
    for batch in cache.iter_batches():
        binding = batch.binding
        digest.update(f"{binding.page_id}|{binding.code}|{binding.kind}|{binding.side}|"
                      f"{batch.delivery}|{batch.response_sha256}\n".encode("utf-8"))
    return digest.hexdigest()


def _execute_run(pack: Path, symbols: Sequence[str], faults: FaultInjection, *,
                 cache_root: Path, inputs: GateInputs, push_window_seconds: float | None,
                 speed: float, connections: int = 1,
                 flow_control: bool = False) -> tuple[L2ReplaySource, _RunArtifacts]:
    source = build_source(pack, symbols, speed=speed,
                          push_window_seconds=push_window_seconds, faults=faults)
    samples_ns: list[int] = []
    tap = _ConnectionLatencyTap(source.connect)
    cache = _TimedCache(cache_root, tap, samples_ns)
    collector = FeedCollector(
        cache, bootstrap_path=inputs.bootstrap, order_template_path=inputs.order,
        trade_template_path=inputs.trade,
        duration_seconds=source.planned_duration_seconds(),
        symbols=source.config.symbols, max_symbols=len(source.config.symbols),
        clock=source.clock, sleep=source.clock.sleep, connection_factory=tap,
        connection_count=connections,
        subscribe_batch_stocks=5 if flow_control else 0,
        seed_retry_limit=1 if flow_control else 0)
    sampler = _MemorySampler()
    sampler.start()
    started = time.perf_counter()
    try:
        status = collector.run()
        wall_seconds = time.perf_counter() - started
    finally:
        sampler.stop()
    artifacts = _RunArtifacts(
        status=status, receipt=_read_receipt(cache), cache_root=cache_root,
        batches=_committed_batches(cache_root), cache_bytes=_cache_bytes(cache_root),
        wall_seconds=wall_seconds, latency_samples_ns=tuple(samples_ns),
        memory=sampler.summary(), collector_metrics=collector.metrics)
    return source, artifacts


def _scenario_metrics(source: L2ReplaySource, artifacts: _RunArtifacts,
                      stocks: int) -> dict[str, Any]:
    seed = _seed_metrics(artifacts.receipt, streams=len(source.streams), stocks=stocks)
    latency = _percentiles_us(artifacts.latency_samples_ns)
    disk = {
        "cache_bytes": artifacts.cache_bytes,
        "batches": artifacts.batches,
        "wall_seconds": round(artifacts.wall_seconds, 6),
        "mb_per_second": (round(artifacts.cache_bytes / artifacts.wall_seconds / 1_000_000, 4)
                          if artifacts.wall_seconds > 0 else 0.0),
    }
    return {**seed, "consumption_latency_us": latency, "disk_write": disk,
            "memory": artifacts.memory, "batches": artifacts.batches,
            "status": _status_metrics(artifacts.status),
            "collector": artifacts.collector_metrics,
            "replay": source.stats.published()}


def _windowed_push_frames(index: PackIndex, codes: Sequence[str],
                          window_seconds: float | None) -> int:
    limit_us = None if window_seconds is None else int(round(window_seconds * 1_000_000))
    total = 0
    for stream in index.streams.values():
        if stream.code not in codes:
            continue
        if limit_us is None:
            total += len(stream.pushes)
        else:
            total += sum(1 for offset_us, _body in stream.pushes if offset_us <= limit_us)
    return total


def _expected_synthetic_streams(stocks: int) -> int:
    """Streams cloned away from the 2 recorded stocks (3 streams per stock)."""
    recorded_stocks = len(REAL_TIER[:2])
    return 3 * (stocks - recorded_stocks) if stocks > recorded_stocks else 0


def _fault_baseline(spec: ScenarioSpec) -> dict[str, int] | None:
    """The measured 2026-10-29 baseline for the three default fault scenarios."""
    if spec.stocks != SIMULATED_STOCK_UNIVERSE:
        return None
    faults = spec.faults
    if (faults.withhold_seed_from_stock == 24
            and not faults.withhold_seed_codes and not faults.stall_codes):
        return SCALE40_FAULT_BASELINE["fault-seed-24of40"]
    if (faults.withhold_seed_codes == (SINGLE_FAULT_SYMBOL,)
            and faults.withhold_seed_from_stock is None and not faults.stall_codes):
        return SCALE40_FAULT_BASELINE["fault-single-silent"]
    if (faults.stall_codes == (SINGLE_FAULT_SYMBOL,)
            and faults.withhold_seed_from_stock is None and not faults.withhold_seed_codes):
        return SCALE40_FAULT_BASELINE["fault-single-stall"]
    return None


def _expected_seed_shape(spec: ScenarioSpec, tier: TierSymbols) -> tuple[int, int, int]:
    """Expected ``(seeded_streams, seeded_stocks, failed_streams)`` for one scenario."""
    baseline = _fault_baseline(spec)
    if baseline is not None:
        return (baseline["seeded_streams"], baseline["seeded_stocks"],
                baseline["failed_streams"])
    total = 3 * spec.stocks
    faults = spec.faults
    if faults.withhold_seed_from_stock is not None:
        seeded_stocks = min(faults.withhold_seed_from_stock, spec.stocks)
    elif faults.withhold_seed_codes:
        withheld = sum(1 for code in tier.symbols if code in faults.withhold_seed_codes)
        seeded_stocks = spec.stocks - withheld
    else:
        seeded_stocks = spec.stocks
    seeded_streams = 3 * seeded_stocks
    return seeded_streams, seeded_stocks, total - seeded_streams


def _expected_latency_seed_shape(spec: ScenarioSpec) -> tuple[int, int, int]:
    """Expected seed shape under the calibrated per-connection seed-latency fault."""
    links = spec.connections
    total = 3 * spec.stocks
    pages_per_link: dict[int, int] = {index: 0 for index in range(links)}
    seeded = 0
    for stream_index in range(total):
        stock = stream_index // 3
        chunk = min(links - 1, stock * links // spec.stocks)
        pages_per_link[chunk] += 1
        delay = spec.faults.seed_delay_seconds(pages_per_link[chunk])
        if delay < MAX_SECONDS:
            seeded += 1
    seeded_stocks = seeded // 3
    return seeded, seeded_stocks, total - seeded


def _latency_expectations(*, spec: ScenarioSpec, source: L2ReplaySource,
                          artifacts: _RunArtifacts) -> list[dict[str, Any]]:
    status = artifacts.status
    total = len(source.streams)
    seed = _seed_metrics(artifacts.receipt, streams=total, stocks=spec.stocks)
    expected_streams, expected_stocks, expected_failed = _expected_latency_seed_shape(spec)
    expected_rate = round(expected_streams / total, 6) if total else 0.0
    return [
        _expect("state", "stopped", status.state),
        _expect("error_code", "duration_limit", status.error_code),
        _expect("streams_total", total, seed["streams_total"]),
        _expect("seeded_streams", expected_streams, seed["seeded_streams"]),
        _expect("seeded_stocks", expected_stocks, seed["stocks_seeded"]),
        _expect("failed_streams", expected_failed, seed["seed_timeout_streams"]),
        _expect("seed_queries", total, status.seed_queries),
        _expect("seed_first_packet_rate", expected_rate, seed["seed_first_packet_rate"]),
        _expect("synthetic_streams", _expected_synthetic_streams(spec.stocks),
                source.synthetic_streams),
        _expect("memory_samples_positive", True, artifacts.memory["samples"] > 0),
        _expect("cache_bytes_positive", True, artifacts.cache_bytes > 0),
    ]


def _expect(name: str, expected: Any, actual: Any) -> dict[str, Any]:
    return {"name": name, "expected": expected, "actual": actual, "pass": actual == expected}


def _scenario_expectations(*, spec: ScenarioSpec, tier: TierSymbols,
                           source: L2ReplaySource, artifacts: _RunArtifacts,
                           determinism: dict[str, Any] | None,
                           reference_push_frames: int | None,
                           push_window_seconds: float | None) -> list[dict[str, Any]]:
    if spec.faults.seed_latency_onset_pages is not None:
        return _latency_expectations(spec=spec, source=source, artifacts=artifacts)
    status = artifacts.status
    total = len(source.streams)
    seed = _seed_metrics(artifacts.receipt, streams=total, stocks=spec.stocks)
    expected_streams, expected_stocks, expected_failed = _expected_seed_shape(spec, tier)
    expected_rate = round(expected_streams / total, 6) if total else 0.0
    # With every seed withheld no seed can validate, so the collector reports
    # the honest blocked/seed_timeout end instead of a normal duration stop.
    expected_state = "stopped" if expected_streams else "blocked"
    expected_error = "duration_limit" if expected_streams else "seed_timeout"
    expectations = [
        _expect("state", expected_state, status.state),
        _expect("error_code", expected_error, status.error_code),
        _expect("streams_total", total, seed["streams_total"]),
        _expect("seeded_streams", expected_streams, seed["seeded_streams"]),
        _expect("seeded_stocks", expected_stocks, seed["stocks_seeded"]),
        _expect("failed_streams", expected_failed, seed["seed_timeout_streams"]),
        _expect("seed_queries", total, status.seed_queries),
        _expect("seed_first_packet_rate", expected_rate, seed["seed_first_packet_rate"]),
        _expect("planned_seed_responses", expected_streams, source.planned_seed_responses),
        _expect("planned_seed_withheld", total - expected_streams, source.planned_seed_withheld),
        _expect("batches", expected_streams + source.planned_push_frames, artifacts.batches),
        _expect("market_batch_count", artifacts.batches, status.market_batch_count),
        _expect("synthetic_streams", _expected_synthetic_streams(spec.stocks),
                source.synthetic_streams),
        _expect("latency_samples", artifacts.batches, len(artifacts.latency_samples_ns)),
        _expect("memory_samples_positive", True, artifacts.memory["samples"] > 0),
        _expect("cache_bytes_positive", True, artifacts.cache_bytes > 0),
    ]
    if spec.faults.stall_codes and reference_push_frames is not None:
        removed = reference_push_frames - source.planned_push_frames
        expectations.append(_expect(
            "stalled_stock_push_frames_removed",
            _windowed_push_frames(source.index, spec.faults.stall_codes, push_window_seconds),
            removed))
    if spec.runs > 1:
        expectations.extend([
            _expect("determinism_runs", spec.runs,
                    determinism["runs"] if determinism is not None else 0),
            _expect("determinism_batch_sequence_sha256_match", True,
                    bool(determinism is not None and determinism["match"])),
            _expect("determinism_batches_match", True,
                    bool(determinism is not None
                         and determinism["batches"][0] == determinism["batches"][1])),
        ])
    return expectations


def _scenario_config(spec: ScenarioSpec, tier: TierSymbols, source: L2ReplaySource, *,
                     speed: float, push_window_seconds: float | None,
                     cache_dirs: Sequence[Path], inputs: GateInputs,
                     cache_kept: bool) -> dict[str, Any]:
    return {
        "name": spec.name,
        "stocks": spec.stocks,
        "streams": len(source.streams),
        "speed": speed,
        "push_window_seconds": push_window_seconds,
        "faults": spec.faults.published(),
        "runs": spec.runs,
        "connections": spec.connections,
        "flow_control": spec.flow_control,
        "real_symbols": list(tier.real),
        "synthetic_symbols": list(tier.synthetic),
        "synthetic_streams": source.synthetic_streams,
        "planned_seed_responses": source.planned_seed_responses,
        "planned_seed_withheld": source.planned_seed_withheld,
        "planned_push_frames": source.planned_push_frames,
        "planned_duration_seconds": round(source.planned_duration_seconds(), 3),
        "cache_dirs": [str(path) for path in cache_dirs],
        "cache_kept": cache_kept,
        "input_source": inputs.source,
    }


def run_scenario(pack: Path | str, spec: ScenarioSpec, *, work_dir: Path,
                 inputs: GateInputs, push_window_seconds: float | None, speed: float,
                 keep: bool) -> dict[str, Any]:
    """Run one scenario in its own temporary cache root and return its report block."""
    pack_path = Path(pack)
    tier = tier_symbols(spec.stocks)
    cache_dir = Path(tempfile.mkdtemp(prefix=f"cache-{spec.name}-", dir=work_dir))
    cache_dirs = [cache_dir]
    reference_push_frames: int | None = None
    if spec.faults.stall_codes:
        reference_push_frames = build_source(
            pack_path, tier.symbols, speed=speed,
            push_window_seconds=push_window_seconds).planned_push_frames
    started = time.perf_counter()
    try:
        source, artifacts = _execute_run(
            pack_path, tier.symbols, spec.faults, cache_root=cache_dir, inputs=inputs,
            push_window_seconds=push_window_seconds, speed=speed,
            connections=spec.connections, flow_control=spec.flow_control)
        determinism: dict[str, Any] | None = None
        if spec.runs > 1:
            first_digest = _batch_sequence_digest(artifacts.cache_root)
            second_cache = Path(tempfile.mkdtemp(
                prefix=f"cache-{spec.name}-run2-", dir=work_dir))
            cache_dirs.append(second_cache)
            _second_source, second = _execute_run(
                pack_path, tier.symbols, spec.faults, cache_root=second_cache, inputs=inputs,
                push_window_seconds=push_window_seconds, speed=speed,
                connections=spec.connections, flow_control=spec.flow_control)
            second_digest = _batch_sequence_digest(second.cache_root)
            determinism = {
                "runs": spec.runs,
                "sha256": [first_digest, second_digest],
                "batches": [artifacts.batches, second.batches],
                "match": first_digest == second_digest
                         and artifacts.batches == second.batches,
            }
        metrics = _scenario_metrics(source, artifacts, spec.stocks)
        if determinism is not None:
            metrics["determinism"] = determinism
        expectations = _scenario_expectations(
            spec=spec, tier=tier, source=source, artifacts=artifacts,
            determinism=determinism, reference_push_frames=reference_push_frames,
            push_window_seconds=push_window_seconds)
        passed = all(expectation["pass"] for expectation in expectations)
        return {
            "name": spec.name,
            "config": _scenario_config(
                spec, tier, source, speed=speed, push_window_seconds=push_window_seconds,
                cache_dirs=cache_dirs, inputs=inputs, cache_kept=keep),
            "status": "pass" if passed else "fail",
            "wall_ms": int(round((time.perf_counter() - started) * 1000)),
            "expectations": expectations,
            "metrics": metrics,
        }
    finally:
        if not keep:
            for directory in cache_dirs:
                shutil.rmtree(directory, ignore_errors=True)


def _empty_metrics() -> dict[str, Any]:
    return {
        "seed_first_packet_rate": 0.0,
        "streams_total": 0,
        "stocks_total": 0,
        "seeded_streams": 0,
        "stocks_seeded": 0,
        "seed_timeout_streams": 0,
        "consumption_latency_us": {"p50": 0, "p95": 0, "p99": 0, "max": 0, "samples": 0},
        "disk_write": {"cache_bytes": 0, "batches": 0, "wall_seconds": 0.0,
                       "mb_per_second": 0.0},
        "memory": {"curve": [], "max_mb": 0.0, "p50_mb": 0.0, "samples": 0,
                   "method": "unavailable", "fallback_reason": None},
        "batches": 0,
        "status": {},
    }


def _scenario_failure(spec: ScenarioSpec, error: Exception, *,
                      push_window_seconds: float | None, speed: float) -> dict[str, Any]:
    tier = tier_symbols(spec.stocks)
    message = f"{type(error).__name__}: {error}"
    return {
        "name": spec.name,
        "config": {
            "name": spec.name, "stocks": spec.stocks, "streams": 3 * spec.stocks,
            "speed": speed, "push_window_seconds": push_window_seconds,
            "faults": spec.faults.published(), "runs": spec.runs,
            "real_symbols": list(tier.real), "synthetic_symbols": list(tier.synthetic),
            "error": message,
        },
        "status": "fail",
        "wall_ms": 0,
        "expectations": [_expect("scenario_completed", True, message)],
        "metrics": _empty_metrics(),
    }


def _main_metrics(scenario: dict[str, Any]) -> dict[str, Any]:
    metrics = scenario["metrics"]
    memory = metrics["memory"]
    return {
        "seed_first_packet_rate": metrics["seed_first_packet_rate"],
        "consumption_latency_p99_us": metrics["consumption_latency_us"]["p99"],
        "disk_write_mb_per_second": metrics["disk_write"]["mb_per_second"],
        "memory": {"curve": memory["curve"], "max_mb": memory["max_mb"],
                   "p50_mb": memory["p50_mb"]},
    }


def _reference_directory() -> Path | None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / ".tools" / "review" / "scale40-C-20261029"
        if candidate.is_dir():
            return candidate
    return None


def _load_reference(directory: Path | str | None) -> dict[str, Any] | None:
    """Read the 2026-10-29 scale40-C evidence summary when it is available."""
    base = Path(directory) if directory is not None else _reference_directory()
    if base is None:
        return None
    try:
        results = json.loads((base / "results.json").read_text(encoding="utf-8"))
        receipt = json.loads((base / "cache" / "subscription-status.json").read_text(
            encoding="utf-8"))
    except (OSError, ValueError):
        return None
    streams = receipt.get("streams")
    if not isinstance(streams, list):
        return None
    seeded = [stream for stream in streams
              if isinstance(stream, dict) and stream.get("seed_sequence") is not None]
    seeded_codes = {stream["binding"]["code"] for stream in seeded
                    if isinstance(stream.get("binding"), dict)
                    and isinstance(stream["binding"].get("code"), str)}
    status = results.get("status") if isinstance(results.get("status"), dict) else {}
    return {
        "evidence": str(base),
        "schema": results.get("schema"),
        "state": status.get("state"),
        "error_code": status.get("error_code"),
        "seed_queries": status.get("seed_queries"),
        "streams": len(streams),
        "seeded_streams": len(seeded),
        "seeded_stocks": len(seeded_codes),
        "failed_streams": len(streams) - len(seeded),
        "batches": results.get("batches"),
        "cache_bytes": results.get("cache_bytes"),
    }


def _report_notes(inputs: GateInputs, scenarios: Sequence[dict[str, Any]],
                  reference: dict[str, Any] | None,
                  push_window_seconds: float | None) -> list[str]:
    notes = [
        "离线门禁：不联网、不读 .env/凭据/生产环境；数据只来自本地 replay 包与临时目录。",
        ("离线合成的 bootstrap/order/trade 私有输入（77 字节头 + iPhone64 载荷，mode 0600）；"
         "合成输入不代表真实设备、账号或会话。" if inputs.synthetic else
         "使用 --inputs-dir 指定的外部私有输入；门禁只把路径交给采集器，不读取或打印内容。"),
        "数据包只有 603258/300006 两只标的的 6 条原始流；40/80/100 档其余流全部是"
        "把录制流克隆到新 page/code 的合成流（逐场景 config.synthetic_streams），"
        "不是真实市场证据。",
        "80/100 档在真实 40 只代码之外用 690000+i 合成占位代码补满（逐场景 "
        "config.synthetic_symbols），这些代码不是真实市场标的。",
        "seed_first_packet_rate = 最终订阅回执中 seed_sequence 非空的流数 / 流总数；"
        "超时后又收到推送的流 state 会变成 first_push_observed，因此不看 state。",
        "consumption_latency 是进程内 recv→append 落盘的墙钟延迟，不是下游 HTTP/WS 消费延迟，"
        "也不代表产品端到端延迟。",
        "disk_write 与 memory 是本机瞬时测量（1 MB = 1e6 字节；RSS 采样 0.25s 一次），"
        "不是容量规划承诺。",
        "determinism-40 在独立临时目录连跑两次，比较 committed 批次的 "
        "(page_id, code, kind, side, delivery, response_sha256) 序列 SHA256。",
    ]
    notes.append("推送窗口：" + ("全窗口（push_window_seconds=none，未降窗）"
                              if push_window_seconds is None
                              else f"{push_window_seconds:g}s（为控制预算而降窗，报告中已注明）"))
    for scenario in scenarios:
        config = scenario.get("config") or {}
        synthetic_streams = config.get("synthetic_streams") or 0
        if synthetic_streams:
            recorded = int(config.get("streams") or 0) - int(synthetic_streams)
            notes.append(f"{config.get('name')}: {config.get('stocks')} 股 "
                         f"{config.get('streams')} 条流中只有 {recorded} 条是原始录制流，"
                         f"{synthetic_streams} 条为合成克隆流。")
        synthetic_symbols = config.get("synthetic_symbols") or []
        if synthetic_symbols:
            notes.append(f"{config.get('name')}: 真实 40 股之外补入 "
                         f"{len(synthetic_symbols)} 个合成占位代码 "
                         f"{synthetic_symbols[0]}..{synthetic_symbols[-1]}（非真实市场标的）。")
    methods = sorted({str((scenario.get("metrics") or {}).get("memory", {}).get("method",
                                                                               "unavailable"))
                      for scenario in scenarios})
    if methods and methods != ["ps"]:
        notes.append(f"memory 采样方法：{methods}；本机 `ps` 不可执行时自动改用 /proc、"
                     "libproc 或 rusage 峰值，具体 method/fallback_reason 见各场景 "
                     "metrics.memory。")
    if reference is not None:
        notes.append(
            f"对照 2026-10-29 scale40-C 真实 40 股运行（{reference.get('evidence')}）："
            f"seed_queries={reference.get('seed_queries')} "
            f"seeded_streams={reference.get('seeded_streams')} "
            f"seeded_stocks={reference.get('seeded_stocks')} "
            f"failed_streams={reference.get('failed_streams')} "
            f"batches={reference.get('batches')}；fault-seed-24of40 以 72/120 形态复现该结果。")
    return notes


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")
    except OSError as error:
        raise GateError("output_error") from error


def run_gate(pack: Path | str, *, out: Path | str | None = None,
             scenarios: Sequence[ScenarioSpec] | None = None,
             push_window_seconds: float | None = None, speed: float = 0.0,
             keep: bool = False, inputs_dir: Path | str | None = None,
             reference_dir: Path | str | None = None) -> dict[str, Any]:
    """Run the requested scenarios and return (and optionally write) the gate report."""
    pack_path = Path(pack)
    try:
        index = read_pack_index(pack_path)
    except ReplaySourceError as error:
        raise GateError(f"pack_{error.code}") from error
    specs = default_scenarios() if scenarios is None else tuple(scenarios)
    if not specs:
        raise GateError("invalid_scenarios")
    for spec in specs:
        if not isinstance(spec, ScenarioSpec):
            raise GateError("invalid_scenarios")
    if len({spec.name for spec in specs}) != len(specs):
        raise GateError("duplicate_scenario")
    if type(speed) not in (int, float) or not math.isfinite(speed) or speed < 0:
        raise GateError("invalid_speed")
    started_at = datetime.now(UTC)
    started = time.perf_counter()
    work_dir = Path(tempfile.mkdtemp(prefix="rquant-l2-replay-gate-"))
    try:
        inputs = _prepare_inputs(inputs_dir, work_dir)
        scenario_reports: list[dict[str, Any]] = []
        for spec in specs:
            try:
                scenario_reports.append(run_scenario(
                    pack_path, spec, work_dir=work_dir, inputs=inputs,
                    push_window_seconds=push_window_seconds, speed=float(speed), keep=keep))
            except Exception as error:  # a broken scenario must fail the gate, not abort it
                scenario_reports.append(_scenario_failure(
                    spec, error, push_window_seconds=push_window_seconds, speed=float(speed)))
        wall_ms = int(round((time.perf_counter() - started) * 1000))
        failed = [scenario["name"] for scenario in scenario_reports
                  if scenario["status"] != "pass"]
        budget_exceeded = wall_ms > DEFAULT_BUDGET_MS
        main_scenario = next((scenario for scenario in scenario_reports
                              if scenario["name"] == MAIN_SCENARIO_NAME),
                             scenario_reports[0])
        reference = _load_reference(reference_dir)
        report = {
            "schema": GATE_SCHEMA,
            "generated_at": started_at.isoformat(),
            "pack": index.published(),
            "inputs": inputs.published(),
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "pid": os.getpid(),
                "work_dir": str(work_dir),
                "work_dir_kept": keep,
            },
            "gate": {
                "status": "pass" if not failed and not budget_exceeded else "fail",
                "wall_ms": wall_ms,
                "budget_ms": DEFAULT_BUDGET_MS,
                "scenario_count": len(scenario_reports),
                "failed": failed,
                "budget_exceeded": budget_exceeded,
            },
            "metrics": _main_metrics(main_scenario),
            "scenarios": scenario_reports,
            "reference": reference,
            "notes": _report_notes(inputs, scenario_reports, reference,
                                   push_window_seconds),
        }
        if out is not None:
            _write_json(Path(out), report)
        return report
    finally:
        if not keep:
            shutil.rmtree(work_dir, ignore_errors=True)


def _push_seconds(value: str) -> float | None:
    if value.strip().lower() == "none":
        return None
    try:
        seconds = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("push seconds must be a number or 'none'") from None
    if not math.isfinite(seconds) or not 0 < seconds <= MAX_PUSH_WINDOW_SECONDS:
        raise argparse.ArgumentTypeError("push seconds must be positive and within the pack span")
    return seconds


def _single_scenario(stocks: int, fault: str, *, connections: int = 1,
                     flow_control: bool = False) -> ScenarioSpec:
    tier = tier_symbols(stocks)
    name = f"healthy-{stocks}" if fault == "healthy" else f"{fault}-{stocks}"
    return ScenarioSpec(name=name, stocks=stocks,
                        faults=fault_injection(fault, tier.symbols),
                        connections=connections, flow_control=flow_control)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m rquant.l2.replay_gate",
        description="Offline deterministic L2 replay benchmark gate")
    subparsers = parser.add_subparsers(dest="command", required=True)
    gate = subparsers.add_parser("gate", help="run the full offline benchmark gate")
    gate.add_argument("--pack", type=Path, required=True, help="replay pack directory")
    gate.add_argument("--out", type=Path, required=True, help="report JSON path")
    gate.add_argument("--push-seconds", type=_push_seconds, default=None,
                      help="push playback window in seconds, or 'none' for the whole recording")
    gate.add_argument("--speed", type=float, choices=(0.0, 1.0, 10.0), default=0.0,
                      help="replay speed; 0 is full speed (default)")
    gate.add_argument("--keep", action="store_true",
                      help="keep every scenario's temporary cache directory")
    gate.add_argument("--inputs-dir", type=Path, default=None,
                      help="directory with bootstrap.bin/order-template.bin/"
                           "trade-template.bin (mode 0600, owner current user)")
    run = subparsers.add_parser("run", help="run one scenario and print/write its report")
    run.add_argument("--pack", type=Path, required=True, help="replay pack directory")
    run.add_argument("--stocks", type=int, default=SIMULATED_STOCK_UNIVERSE)
    run.add_argument("--fault", choices=FAULT_CHOICES, default="healthy",
                     help="seed-24of40 withholds seeds from stock position 24 onward "
                          "(tiers with <=24 stocks are unaffected); seed-latency applies "
                          "the calibrated 2026-10-29 per-connection seed wait curve")
    run.add_argument("--connections", type=int, default=1,
                     help="batched upstream connections (contiguous stock chunks)")
    run.add_argument("--flow-control", action="store_true",
                     help="enable batched subscription pacing and one deferred seed retry")
    run.add_argument("--speed", type=float, choices=(0.0, 1.0, 10.0), default=0.0,
                     help="replay speed; 0 is full speed (default)")
    run.add_argument("--out", type=Path, default=None, help="report JSON path")
    return parser


def _print_gate_summary(report: dict[str, Any], out: Path | str | None) -> None:
    gate = report["gate"]
    metrics = report["metrics"]
    print(f"gate={gate['status']} wall_ms={gate['wall_ms']} budget_ms={gate['budget_ms']} "
          f"budget_exceeded={gate['budget_exceeded']} scenarios={gate['scenario_count']} "
          f"failed={gate['failed']}")
    print(f"seed_first_packet_rate={metrics['seed_first_packet_rate']} "
          f"consumption_latency_p99_us={metrics['consumption_latency_p99_us']} "
          f"disk_write_mb_per_second={metrics['disk_write_mb_per_second']} "
          f"memory_max_mb={metrics['memory']['max_mb']}")
    if out is not None:
        print(f"report={out}")
    if report["environment"].get("work_dir_kept"):
        print(f"kept_work_dir={report['environment']['work_dir']}", file=sys.stderr)


def _print_run_summary(report: dict[str, Any], out: Path | str | None) -> None:
    scenario = report["scenarios"][0]
    metrics = scenario["metrics"]
    print(f"scenario={scenario['name']} status={scenario['status']} "
          f"wall_ms={scenario['wall_ms']} batches={metrics['batches']} "
          f"seed_first_packet_rate={metrics['seed_first_packet_rate']} "
          f"consumption_latency_p99_us={metrics['consumption_latency_us']['p99']} "
          f"disk_write_mb_per_second={metrics['disk_write']['mb_per_second']} "
          f"memory_max_mb={metrics['memory']['max_mb']}")
    if out is not None:
        print(f"report={out}")


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point; returns 0 for a passing gate/run and 1 otherwise."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "gate":
            report = run_gate(args.pack, out=args.out,
                              push_window_seconds=args.push_seconds, speed=args.speed,
                              keep=args.keep, inputs_dir=args.inputs_dir)
            _print_gate_summary(report, args.out)
            return 0 if report["gate"]["status"] == "pass" else 1
        report = run_gate(args.pack, out=args.out, speed=args.speed,
                          scenarios=(_single_scenario(
                              args.stocks, args.fault,
                              connections=args.connections,
                              flow_control=args.flow_control),))
        _print_run_summary(report, args.out)
        return 0 if report["gate"]["status"] == "pass" else 1
    except GateError as error:
        print(f"replay_gate: {error.code}", file=sys.stderr)
        return 1
    except ReplaySourceError as error:
        print(f"replay_gate: {error.code}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
