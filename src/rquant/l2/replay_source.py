"""Deterministic local replay of recorded L2 raw-frame packs.

R1 records exact socket bytes (``rquant-l2-frame-tap-v1``) and can turn a
committed market cache into a replay pack (``rquant-l2-replay-pack-v1``).  This
module turns such a pack into a *source* that the unchanged
:class:`rquant.l2.feed.FeedCollector` can read from: a mock connection object
that honours the collector's request/response contract instead of talking to
the vendor.

Design constraints taken from the R1 handoff and the recorded packs:

* **Request-gated release.**  The collector issues one seed request per stream
  and waits for that request ID.  Recorded received frames are therefore only
  released when the matching request is sent, and their request ID is
  re-stamped to the live one.  A blind time-ordered replay would let a later
  stream's response arrive before its own request (``association_mismatch``).
* **Expansion by cloning.**  A pack may contain two recorded stocks; 40/80/100
  stock tiers are produced by cloning a recorded stock into new page IDs and
  copying its stream onto a different symbol code.  Order frames carry the
  symbol inside their table title, so the title is rewritten there and the
  Snappy payload is re-emitted literal-only, which can *lengthen* the body; the
  FD wire prefix is always rebuilt from the returned body.  Cloned streams are
  always reported as synthetic.
* **Speeds.**  ``speed=1.0`` reproduces the recorded microsecond offsets,
  ``speed=10.0`` divides them by ten, ``speed=0.0`` ("full") delivers frames as
  fast as the consumer asks.  The virtual clock makes 1x and 10x cost the same
  wall time offline while keeping the interleaving deterministic.
* **Envelope family.**  The collector issues every market query as frame family
  2357 and ``_market`` rejects any market response whose envelope command
  differs.  Packs recorded before the 2026-10-19 family upgrade carry 2267 in
  order responses, so the replay layer may normalize that single 4-byte
  envelope word (``adapt_envelope_family``, counted in
  ``stats.family_restamped``); the table payload is never touched.
* **Fault injection.**  "Seed receipt timeout" (a stream whose response is
  withheld so the collector's 15 s seed guard fires) and "single-stream stall"
  (seeds fine, then the stream goes silent without EOF) are expressed as
  deterministic rules over the tier's streams.

The module never decodes, invents or repairs market data: frames are released
byte-identical unless a request/page/stock re-stamp is required by expansion.
"""
from __future__ import annotations

import heapq
import math
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ._codec import MAX_SECONDS
from ._request import _parse_ini_route, _read_request_body, parse_request_body
from ._replay_rewrite import rewrite_response
from .frame_tap import (KIND_RX, FORMAT_NAME as TAP_FORMAT_NAME, iter_records, read_manifest,
                        split_wire_frame, wire_frame)
from .replay_pack import PACK_FORMAT
from .symbols import MAX_SYMBOLS, market_for_code, normalize_symbols

REPLAY_SOURCE_FORMAT = "rquant-l2-replay-source-v1"

# FeedCollector assigns page IDs 10117 + 10 * symbol_index, then 0/1/2 for
# (vendor4013 trade, vendor4027 buy, vendor4027 sell).  The replay side must use
# the same arithmetic to map a targeted page back to a recorded stream.
PAGE_BASE = 10117
PAGE_STRIDE = 10
STREAM_OFFSETS: tuple[tuple[int, str, str | None], ...] = (
    (0, "vendor4013", None),
    (1, "vendor4027", "buy"),
    (2, "vendor4027", "sell"),
)
STREAMS_PER_STOCK = len(STREAM_OFFSETS)
FULL_SPEED = 0.0
MAX_STOCKS = MAX_SYMBOLS
MAX_PUSH_WINDOW_SECONDS = 6 * 3600.0
_ACK_COMMANDS = frozenset({2201, 2202})
_PUSH_REQUEST_ID = 0xFFFFFFFF
# The collector issues every market query as frame family 2357 (build_common_order_frame
# upgrades the 2267 template), and _market rejects any market response whose envelope
# command differs.  Packs recorded before the 2026-10-19 family upgrade carry 2267 in
# order responses; only that envelope word is normalized, never the table payload.
MARKET_COMMAND = 2357
_MARKET_FAMILIES = frozenset({2267, 2357})
_COUNT_ROLLUP = 1.0


class ReplaySourceError(RuntimeError):
    """Fixed local code; never carries raw frame bytes or upstream text."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _synthetic_ack_body() -> bytes:
    """Structurally valid ack used only when a pack has no recorded control frame.

    Same observed page-0 profile R1's pack builder synthesizes: header 26,
    request 0, flags 6, page 0, command 2201.  Marked synthetic in every report.
    """
    return b"\x1a\x00" + b"\x00\x00\x00\x00" + b"\x06\x00\x00\x00" + b"\x00\x00" + \
        b"\x00\x00\x00\x00" + b"\x99\x08\x00\x00" + b"\x00" * 6


def _frame_header(body: bytes) -> tuple[int, int, int, int, int, int]:
    """Return (header_len, request_id, flags, page_id, payload_size, command)."""
    if not isinstance(body, bytes) or len(body) < 20:
        raise ReplaySourceError("invalid_frame")
    header_len = int.from_bytes(body[0:2], "little")
    if header_len not in (26, 32) or len(body) <= header_len:
        raise ReplaySourceError("invalid_frame")
    return (header_len,
            int.from_bytes(body[2:6], "little"),
            int.from_bytes(body[6:10], "little"),
            int.from_bytes(body[10:12], "little"),
            int.from_bytes(body[12:16], "little"),
            int.from_bytes(body[16:20], "little"))


def _is_control(body: bytes) -> bool:
    """True for empty heartbeats and page-0 vendor control frames.

    Mirrors the collector's own classification: market pages are always
    nonzero, so a page-0 frame can never be a valid market observation.
    """
    if not body:
        return True
    if len(body) < 26:
        return False
    return int.from_bytes(body[10:12], "little") == 0


def _is_ack(body: bytes) -> bool:
    if not body or len(body) < 26:
        return False
    return (int.from_bytes(body[2:6], "little") == 0
            and int.from_bytes(body[6:10], "little") == 6
            and int.from_bytes(body[16:20], "little") in _ACK_COMMANDS)


def request_identity(body: bytes) -> tuple[int, int]:
    """Return (request_id, target_page_id) for one collector TX request body."""
    if not isinstance(body, bytes) or len(body) < 26:
        raise ReplaySourceError("invalid_request")
    request_id = int.from_bytes(body[2:6], "little")
    frame_id = int.from_bytes(body[16:20], "little")
    try:
        if frame_id == 2357:
            parsed = _read_request_body(body, frame_id=2357)
            page_id = int(_parse_ini_route(parsed["plaintext"], frame_id=2357)["page_id"])
        elif frame_id == 2267:
            page_id = int(parse_request_body(body)["page_id"])
        else:
            raise ReplaySourceError("invalid_request")
    except ReplaySourceError:
        raise
    except (ValueError, KeyError, TypeError):
        raise ReplaySourceError("invalid_request") from None
    return request_id, page_id


@dataclass(frozen=True)
class RecordedStream:
    """One recorded page: its seed responses and candidate-push timeline."""

    source_page: int
    stock_index: int
    offset: int
    kind: str
    side: str | None
    code: str
    market: int
    seed_groups: tuple[tuple[bytes, ...], ...]
    pushes: tuple[tuple[int, bytes], ...]  # (microseconds after the page's first push, body)
    push_span_us: int


@dataclass(frozen=True)
class PackIndex:
    """Read-only index of one replay pack (or a recorded tap directory)."""

    directory: Path
    manifest: Mapping[str, Any]
    control: tuple[bytes, ...]
    streams: Mapping[int, RecordedStream]
    record_count: int
    rx_frames: int
    tx_frames: int
    synthetic_control: int
    duration_us: int
    synthetic_ack: bool

    @property
    def stock_indexes(self) -> tuple[int, ...]:
        return tuple(sorted({stream.stock_index for stream in self.streams.values()}))

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(self.streams[page].code for page in self._stock_pages())

    def _stock_pages(self) -> tuple[int, ...]:
        """One representative (trade) page per recorded stock, ordered by index."""
        return tuple(PAGE_BASE + PAGE_STRIDE * index + STREAM_OFFSETS[0][0]
                     for index in self.stock_indexes)

    @property
    def code_pages(self) -> dict[str, list[int]]:
        """Recorded page IDs grouped by symbol code, in page order."""
        pages: dict[str, list[int]] = {}
        for page in sorted(self.streams):
            pages.setdefault(self.streams[page].code, []).append(page)
        return pages

    def stream(self, page: int) -> RecordedStream | None:
        return self.streams.get(page)

    def published(self) -> dict[str, Any]:
        return {
            "directory": str(self.directory),
            "format": self.manifest.get("format"),
            "tap_format": TAP_FORMAT_NAME,
            "records": self.record_count,
            "rx_frames": self.rx_frames,
            "tx_frames": self.tx_frames,
            "synthetic_control": self.synthetic_control,
            "synthetic_ack": self.synthetic_ack,
            "duration_us": self.duration_us,
            "streams": len(self.streams),
            "codes": list(self.codes),
            "manifest_frames": dict(self.manifest.get("frames", {})),
            "manifest_timing": dict(self.manifest.get("timing", {})),
        }


def _manifest_streams(manifest: Mapping[str, Any]) -> dict[int, tuple[int, str, int, int]]:
    """Map recorded page -> (stock_index, code, offset, market) from the manifest."""
    symbols = manifest.get("symbols")
    if not isinstance(symbols, Mapping) or not symbols:
        raise ReplaySourceError("unsupported_pack")
    mapped: dict[int, tuple[int, str, int, int]] = {}
    for code, pages in symbols.items():
        if not isinstance(code, str) or not isinstance(pages, Sequence) or not pages:
            raise ReplaySourceError("unsupported_pack")
        try:
            market = market_for_code(code)
        except ValueError:
            raise ReplaySourceError("unsupported_pack") from None
        try:
            ordered = sorted(int(page) for page in pages)
        except (TypeError, ValueError):
            raise ReplaySourceError("unsupported_pack") from None
        base = ordered[0]
        if base < PAGE_BASE or (base - PAGE_BASE) % PAGE_STRIDE:
            raise ReplaySourceError("unsupported_pack")
        stock_index = (base - PAGE_BASE) // PAGE_STRIDE
        if stock_index >= MAX_STOCKS:
            raise ReplaySourceError("unsupported_pack")
        offsets = [page - base for page in ordered]
        if offsets != list(range(len(ordered))) or len(ordered) > STREAMS_PER_STOCK:
            raise ReplaySourceError("unsupported_pack")
        for page, offset in zip(ordered, offsets):
            mapped[page] = (stock_index, code, offset, market)
    return mapped


def read_pack_index(directory: Path | str) -> PackIndex:
    """Read and classify every RX frame of a replay pack."""
    path = Path(directory)
    manifest = read_manifest(path)
    if manifest is None:
        raise ReplaySourceError("manifest_missing")
    if manifest.get("format") != PACK_FORMAT:
        raise ReplaySourceError("unsupported_pack")
    if manifest.get("tap_format") != TAP_FORMAT_NAME:
        raise ReplaySourceError("unsupported_pack")
    declared = _manifest_streams(manifest)
    segments = manifest.get("segments")
    if not isinstance(segments, Sequence) or not segments:
        raise ReplaySourceError("unsupported_pack")

    control: list[bytes] = []
    push_records: dict[int, list[tuple[int, bytes]]] = {page: [] for page in declared}
    seed_records: dict[int, dict[int, list[tuple[int, bytes]]]] = {page: {} for page in declared}
    rx_frames = tx_frames = records = 0
    last_mono = 0
    for segment in segments:
        try:
            segment_path = path / segment["file"]
        except (KeyError, TypeError):
            raise ReplaySourceError("unsupported_pack") from None
        try:
            for record in iter_records(segment_path):
                records += 1
                last_mono = max(last_mono, record.mono_us)
                if record.kind != KIND_RX:
                    tx_frames += record.kind == 2
                    continue
                rx_frames += 1
                body = split_wire_frame(record.payload)
                if _is_control(body):
                    control.append(body)
                    continue
                _header_len, request_id, _flags, page_id, _size, _command = _frame_header(body)
                if page_id not in declared:
                    raise ReplaySourceError("unsupported_pack")
                if request_id == _PUSH_REQUEST_ID:
                    push_records[page_id].append((record.mono_us, body))
                else:
                    seed_records[page_id].setdefault(request_id, []).append(
                        (record.mono_us, body))
        except ReplaySourceError:
            raise
        except Exception:
            raise ReplaySourceError("invalid_pack") from None
    streams: dict[int, RecordedStream] = {}
    for page_id, (stock_index, code, offset, market) in declared.items():
        pushes = push_records[page_id]
        base_mono = pushes[0][0] if pushes else 0
        timeline = tuple((mono - base_mono, body) for mono, body in pushes)
        # Seed groups are released in request order; a multi-page backfill pack
        # would record one group per page request, so order by first arrival.
        groups = tuple(tuple(body for _mono, body in frames) for frames in
                       sorted(seed_records[page_id].values(), key=lambda frames: frames[0][0]))
        streams[page_id] = RecordedStream(
            source_page=page_id, stock_index=stock_index, offset=offset,
            kind=STREAM_OFFSETS[offset][1], side=STREAM_OFFSETS[offset][2],
            code=code, market=market,
            seed_groups=groups, pushes=timeline,
            push_span_us=timeline[-1][0] if timeline else 0)
    timing = manifest.get("timing")
    duration_us = 0
    if isinstance(timing, Mapping):
        declared_duration = timing.get("duration_us")
        if isinstance(declared_duration, int) and declared_duration >= 0:
            duration_us = declared_duration
    return PackIndex(
        directory=path, manifest=manifest, control=tuple(control), streams=streams,
        record_count=records, rx_frames=rx_frames, tx_frames=tx_frames,
        synthetic_control=int(manifest.get("frames", {}).get("synthetic_control", 0) or 0),
        duration_us=max(duration_us, last_mono),
        synthetic_ack=bool(manifest.get("synthetic", {}).get("bootstrap_ack", 0)))


@dataclass(frozen=True)
class FaultInjection:
    """Deterministic stream faults applied while replaying a tier.

    ``withhold_seed_from_stock`` reproduces the 2026-10-29 shape: every stock
    from that zero-based position onward never receives a seed response, so the
    collector's 15 s guard fires per stream while earlier stocks are seeded.
    ``withhold_seed_codes`` withholds a specific symbol (single-stock silent
    seed timeout).  ``stall_codes`` delivers seeds and then withholds every
    candidate push for that symbol without closing the connection.
    """

    withhold_seed_from_stock: int | None = None
    withhold_seed_codes: tuple[str, ...] = ()
    stall_codes: tuple[str, ...] = ()
    withhold_bootstrap_ack: bool = False
    seed_latency_onset_pages: int | None = None
    seed_latency_coefficient: float | None = None

    def __post_init__(self) -> None:
        if self.withhold_seed_from_stock is not None:
            if (type(self.withhold_seed_from_stock) is not int
                    or not 0 <= self.withhold_seed_from_stock <= MAX_STOCKS):
                raise ReplaySourceError("invalid_fault")
        object.__setattr__(self, "withhold_seed_codes", _normalized_codes(self.withhold_seed_codes))
        object.__setattr__(self, "stall_codes", _normalized_codes(self.stall_codes))
        if type(self.withhold_bootstrap_ack) is not bool:
            raise ReplaySourceError("invalid_fault")
        latency_set = (self.seed_latency_onset_pages is not None,
                       self.seed_latency_coefficient is not None)
        if latency_set[0] != latency_set[1]:
            raise ReplaySourceError("invalid_fault")
        if latency_set[0]:
            onset, coefficient = self.seed_latency_onset_pages, self.seed_latency_coefficient
            if (type(onset) is not int or not 0 <= onset <= MAX_STOCKS * 3
                    or type(coefficient) not in (int, float)
                    or not math.isfinite(coefficient) or not 0 < coefficient <= 100):
                raise ReplaySourceError("invalid_fault")
            object.__setattr__(self, "seed_latency_onset_pages", int(onset))
            object.__setattr__(self, "seed_latency_coefficient", float(coefficient))

    @property
    def active(self) -> bool:
        return bool(self.withhold_seed_from_stock is not None or self.withhold_seed_codes
                    or self.stall_codes or self.withhold_bootstrap_ack
                    or self.seed_latency_onset_pages is not None)

    def seed_delay_seconds(self, pages: int) -> float:
        """Calibrated 2026-10-29 seed latency model: quadratic past the onset.

        ``pages`` is the number of pages the connection has registered when the
        seed request is sent.  The 2026-10-29 scale40-C receipt shows the wait
        staying near zero through ~57 pages and crossing the 15 s guard at 73
        pages; ``coefficient=0.0625`` reproduces that boundary (72 pages ≈
        14.1 s, 73 pages ≈ 16.0 s).
        """
        onset, coefficient = self.seed_latency_onset_pages, self.seed_latency_coefficient
        if onset is None or coefficient is None:
            return 0.0
        excess = max(0, pages - onset)
        return coefficient * excess * excess

    def planned_seed_phase_seconds(self, stream_count: int) -> float:
        """Upper bound on the seed phase under the latency model, single connection."""
        if self.seed_latency_onset_pages is None:
            return 0.0
        total = 0.0
        for index in range(stream_count):
            pages = index + 1
            delay = self.seed_delay_seconds(pages)
            # A timed-out seed costs the full guard, then the next request waits
            # for the adaptive pace (ceiling) before it is even sent.
            total += MAX_SECONDS if delay > MAX_SECONDS else max(1.0, delay)
        return total

    def published(self) -> dict[str, Any]:
        return {
            "withhold_seed_from_stock": self.withhold_seed_from_stock,
            "withhold_seed_codes": list(self.withhold_seed_codes),
            "stall_codes": list(self.stall_codes),
            "withhold_bootstrap_ack": self.withhold_bootstrap_ack,
            "seed_latency_onset_pages": self.seed_latency_onset_pages,
            "seed_latency_coefficient": self.seed_latency_coefficient,
        }


def _normalized_codes(codes: Sequence[str]) -> tuple[str, ...]:
    if isinstance(codes, (str, bytes, bytearray)):
        raise ReplaySourceError("invalid_fault")
    try:
        return tuple(normalize_symbols(tuple(codes)))
    except ValueError:
        raise ReplaySourceError("invalid_fault") from None


class ReplayClock:
    """Clock shared by the replay connection and the collector.

    ``virtual`` advances only when somebody sleeps: 1x/10x playback therefore
    costs the same wall time and stays deterministic, while a real run can use
    ``wall`` to replay against the actual monotonic clock.
    """

    def __init__(self, *, wall: bool = False, start: float = 0.0) -> None:
        self._wall = bool(wall)
        self._now = float(start)
        self.virtual_seconds = 0.0

    @property
    def wall(self) -> bool:
        return self._wall

    def __call__(self) -> float:
        return time.monotonic() if self._wall else self._now

    def sleep(self, seconds: float) -> None:
        if self._wall:
            if seconds > 0:
                time.sleep(seconds)
            return
        if seconds > 0:
            self._now += seconds
            self.virtual_seconds += seconds

    def advance_to(self, moment: float) -> None:
        if self._wall:
            delta = moment - time.monotonic()
            if delta > 0:
                time.sleep(delta)
            return
        if moment > self._now:
            self.virtual_seconds += moment - self._now
            self._now = moment

    def published(self) -> dict[str, Any]:
        return {"mode": "wall" if self._wall else "virtual",
                "virtual_seconds": round(self.virtual_seconds, 6),
                "now": round(self(), 6)}


@dataclass(frozen=True)
class ReplayConfig:
    """Everything needed to build one deterministic replay tier."""

    pack: Path
    symbols: tuple[str, ...]
    speed: float = FULL_SPEED
    clock_mode: str = "virtual"
    push_window_seconds: float | None = None
    faults: FaultInjection = field(default_factory=FaultInjection)
    synthetic_ack: bool = True
    adapt_envelope_family: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "pack", Path(self.pack))
        raw_symbols = tuple(self.symbols)
        try:
            symbols = normalize_symbols(raw_symbols)
        except (ValueError, TypeError):
            raise ReplaySourceError("invalid_symbols") from None
        if not symbols:
            raise ReplaySourceError("invalid_symbols")
        # A tier is an explicit symbol list: normalize_symbols deduplicates
        # silently, which would make a "40 stock" gate run 39 and report a false
        # seed rate, so a repeated code is rejected before normalization.
        if len(set(raw_symbols)) != len(raw_symbols):
            raise ReplaySourceError("invalid_symbols")
        object.__setattr__(self, "symbols", symbols)
        if type(self.speed) not in (int, float) or not math.isfinite(self.speed) or self.speed < 0:
            raise ReplaySourceError("invalid_speed")
        object.__setattr__(self, "speed", float(self.speed))
        if self.clock_mode not in ("virtual", "wall"):
            raise ReplaySourceError("invalid_config")
        if self.push_window_seconds is not None:
            window = self.push_window_seconds
            if (type(window) not in (int, float) or not math.isfinite(window)
                    or not 0 < window <= MAX_PUSH_WINDOW_SECONDS):
                raise ReplaySourceError("invalid_config")
            object.__setattr__(self, "push_window_seconds", float(window))
        if type(self.synthetic_ack) is not bool:
            raise ReplaySourceError("invalid_config")
        if type(self.adapt_envelope_family) is not bool:
            raise ReplaySourceError("invalid_config")
        if not isinstance(self.faults, FaultInjection):
            raise ReplaySourceError("invalid_fault")

    @property
    def speed_label(self) -> str:
        return "full" if self.speed == FULL_SPEED else f"{self.speed:g}x"


@dataclass(frozen=True)
class TargetStream:
    """One stream the collector will request, mapped onto a recorded stream."""

    stock_index: int
    stock_code: str
    offset: int
    page_id: int
    kind: str
    side: str | None
    source: RecordedStream
    synthetic: bool
    seed_withheld: bool
    stalled: bool
    pushes: tuple[tuple[int, bytes], ...]

    @property
    def rewrites_code(self) -> bool:
        return self.stock_code != self.source.code

    def published(self) -> dict[str, Any]:
        return {
            "page_id": self.page_id, "kind": self.kind, "side": self.side,
            "stock_index": self.stock_index, "code": self.stock_code,
            "source_page": self.source.source_page, "source_code": self.source.code,
            "synthetic": self.synthetic, "seed_withheld": self.seed_withheld,
            "stalled": self.stalled, "pushes": len(self.pushes),
        }


def _windowed_pushes(stream: RecordedStream, window_seconds: float | None
                     ) -> tuple[tuple[int, bytes], ...]:
    if window_seconds is None:
        return stream.pushes
    limit_us = int(round(window_seconds * 1_000_000))
    return tuple((offset, body) for offset, body in stream.pushes if offset <= limit_us)


class ReplaySourceStats:
    """Mutable counters shared by the source and its connections."""

    def __init__(self) -> None:
        self.requests = 0
        self.bootstrap_sends = 0
        self.seed_responses = 0
        self.seed_withheld = 0
        self.seed_unavailable = 0
        self.control_frames = 0
        self.push_frames = 0
        self.synthetic_ack_frames = 0
        self.frames_returned = 0
        self.bytes_returned = 0
        self.idle_timeouts = 0
        self.activations = 0
        self.family_restamped = 0
        self.seed_delayed = 0
        self.seed_delay_max_seconds = 0.0

    def published(self) -> dict[str, Any]:
        return {
            "requests": self.requests,
            "bootstrap_sends": self.bootstrap_sends,
            "seed_responses": self.seed_responses,
            "seed_withheld": self.seed_withheld,
            "seed_unavailable": self.seed_unavailable,
            "control_frames": self.control_frames,
            "push_frames": self.push_frames,
            "synthetic_ack_frames": self.synthetic_ack_frames,
            "frames_returned": self.frames_returned,
            "bytes_returned": self.bytes_returned,
            "idle_timeouts": self.idle_timeouts,
            "activations": self.activations,
            "family_restamped": self.family_restamped,
            "seed_delayed": self.seed_delayed,
            "seed_delay_max_seconds": round(self.seed_delay_max_seconds, 6),
        }


class L2ReplaySource:
    """A deterministic, request-gated stand-in for the vendor L2 feed.

    Usage::

        source = L2ReplaySource(ReplayConfig(pack=pack, symbols=symbols, speed=10.0))
        collector = FeedCollector(cache, ..., clock=source.clock, sleep=source.clock.sleep,
                                  connection_factory=source.connect)
        status = collector.run()
        report = source.published()
    """

    def __init__(self, config: ReplayConfig, *, index: PackIndex | None = None) -> None:
        if not isinstance(config, ReplayConfig):
            raise ReplaySourceError("invalid_config")
        self.config = config
        self.index = index if index is not None else read_pack_index(config.pack)
        self.clock = ReplayClock(wall=config.clock_mode == "wall")
        self.stats = ReplaySourceStats()
        self._streams: dict[int, TargetStream] = {}
        self._build_streams()

    def _validate_faults(self) -> None:
        """Fault codes must address this tier; a typo must not silently pass."""
        faults = self.config.faults
        codes = set(self.config.symbols)
        if (set(faults.withhold_seed_codes) | set(faults.stall_codes)) - codes:
            raise ReplaySourceError("invalid_fault")
        threshold = faults.withhold_seed_from_stock
        if threshold is not None and threshold > len(self.config.symbols):
            raise ReplaySourceError("invalid_fault")

    def _build_streams(self) -> None:
        self._validate_faults()
        recorded_indexes = self.index.stock_indexes
        if not recorded_indexes:
            raise ReplaySourceError("unsupported_pack")
        by_stock: dict[int, dict[int, RecordedStream]] = {}
        for stream in self.index.streams.values():
            by_stock.setdefault(stream.stock_index, {})[stream.offset] = stream
        for stock_index, offsets in by_stock.items():
            if sorted(offsets) != [offset for offset, _kind, _side in STREAM_OFFSETS]:
                raise ReplaySourceError("unsupported_pack")
        for stock_position, code in enumerate(self.config.symbols):
            recorded_stock = (stock_position if stock_position < len(recorded_indexes)
                              else recorded_indexes[0])
            for offset, kind, side in STREAM_OFFSETS:
                source = by_stock[recorded_stock][offset]
                page_id = PAGE_BASE + PAGE_STRIDE * stock_position + offset
                if page_id > 0xFFFF:
                    raise ReplaySourceError("invalid_symbols")
                withheld = self._seed_withheld(stock_position, code)
                stalled = code in self.config.faults.stall_codes
                pushes = () if stalled else _windowed_pushes(
                    source, self.config.push_window_seconds)
                self._streams[page_id] = TargetStream(
                    stock_index=stock_position, stock_code=code, offset=offset,
                    page_id=page_id, kind=kind, side=side, source=source,
                    synthetic=page_id != source.source_page or code != source.code,
                    seed_withheld=withheld, stalled=stalled, pushes=pushes)

    def _seed_withheld(self, stock_position: int, code: str) -> bool:
        faults = self.config.faults
        if code in faults.withhold_seed_codes:
            return True
        threshold = faults.withhold_seed_from_stock
        return threshold is not None and stock_position >= threshold

    @property
    def streams(self) -> Mapping[int, TargetStream]:
        return dict(self._streams)

    def stream(self, page_id: int) -> TargetStream | None:
        return self._streams.get(page_id)

    def streams_for_stock(self, stock_index: int) -> tuple[TargetStream, ...]:
        return tuple(stream for stream in self._streams.values()
                     if stream.stock_index == stock_index)

    @property
    def planned_seed_responses(self) -> int:
        return sum(1 for stream in self._streams.values() if not stream.seed_withheld)

    @property
    def planned_seed_withheld(self) -> int:
        return sum(1 for stream in self._streams.values() if stream.seed_withheld)

    @property
    def planned_push_frames(self) -> int:
        return sum(len(stream.pushes) for stream in self._streams.values())

    @property
    def synthetic_streams(self) -> int:
        return sum(1 for stream in self._streams.values() if stream.synthetic)

    def planned_duration_seconds(self, *, pages_per_stream: int = 1,
                                 margin_seconds: float = 3.0) -> float:
        """Deterministic upper bound on the collector's virtual run time.

        Each request is paced one second apart by ``_issue_stream``; a withheld
        seed costs the collector's full 15 s guard; push playback for a stream
        starts when that stream is activated, so the last push lands at most
        ``window / speed`` after the seed phase ends.
        """
        if type(pages_per_stream) is not int or not 1 <= pages_per_stream <= 3:
            raise ReplaySourceError("invalid_config")
        requests = len(self._streams) * pages_per_stream
        seed_phase = requests * _COUNT_ROLLUP + self.planned_seed_withheld * MAX_SECONDS
        seed_phase += self.config.faults.planned_seed_phase_seconds(len(self._streams))
        if self.config.speed > 0:
            window = self.config.push_window_seconds
            if window is None:
                span_us = max((stream.source.push_span_us for stream in self._streams.values()),
                              default=0)
                window = span_us / 1_000_000
            seed_phase += window / self.config.speed
        return seed_phase + margin_seconds + 2.0

    def connect(self, endpoint: Any = None, timeout: Any = None) -> ReplayConnection:
        """``connection_factory`` compatible entry point."""
        return ReplayConnection(self)

    def open(self) -> ReplayConnection:
        return ReplayConnection(self)

    def published(self) -> dict[str, Any]:
        """Static plan plus live counters, safe to serialize into a report."""
        return {
            "format": REPLAY_SOURCE_FORMAT,
            "pack": self.index.published(),
            "config": {
                "symbols": list(self.config.symbols),
                "stocks": len(self.config.symbols),
                "speed": self.config.speed,
                "speed_label": self.config.speed_label,
                "clock": self.config.clock_mode,
                "push_window_seconds": self.config.push_window_seconds,
                "synthetic_ack": self.config.synthetic_ack,
                "adapt_envelope_family": self.config.adapt_envelope_family,
                "faults": self.config.faults.published(),
            },
            "plan": {
                "streams": len(self._streams),
                "stocks": len(self.config.symbols),
                "planned_seed_responses": self.planned_seed_responses,
                "planned_seed_withheld": self.planned_seed_withheld,
                "planned_push_frames": self.planned_push_frames,
                "synthetic_streams": self.synthetic_streams,
                "planned_duration_seconds": round(self.planned_duration_seconds(), 3),
            },
            "streams": [self._streams[page].published() for page in sorted(self._streams)],
            "stats": self.stats.published(),
            "clock": self.clock.published(),
        }


class ReplayConnection:
    """Mock socket that releases recorded frames gated by collector requests.

    Implements exactly what :class:`rquant.l2.feed.FrameReader` and
    ``FeedCollector._send`` use: ``settimeout``, ``sendall``, ``recv``, ``close``.
    """

    def __init__(self, source: L2ReplaySource) -> None:
        if not isinstance(source, L2ReplaySource):
            raise ReplaySourceError("invalid_config")
        self.source = source
        self.clock = source.clock
        self.timeout: float = 1.0
        self.closed = False
        self._ready: deque[bytes] = deque()
        self._scheduled: list[tuple[float, int, bytes]] = []
        self._sequence = 0
        self._first_send = True
        self._activated: dict[int, float] = {}
        self._seed_cursor: dict[int, int] = {}
        self._controls_sent = False
        self.stats = source.stats
        self.last_delivery_wall_ns = 0
        self.frames_scheduled = 0
        self.frames_dropped = 0

    def settimeout(self, value: Any) -> None:
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError("invalid timeout")
        self.timeout = float(value)

    def gettimeout(self) -> float:
        return self.timeout

    def sendall(self, data: bytes) -> None:
        if self.closed:
            raise OSError("replay connection closed")
        try:
            body = split_wire_frame(data)
        except Exception:
            raise ReplaySourceError("invalid_request") from None
        if self._first_send:
            self._first_send = False
            self.stats.bootstrap_sends += 1
            self._deliver_controls()
            return
        request_id, page_id = request_identity(body)
        source = self.source
        stream = source.stream(page_id)
        if stream is None:
            raise ReplaySourceError("unknown_page")
        self.stats.requests += 1
        self._activate(stream)
        if stream.seed_withheld:
            self.stats.seed_withheld += 1
            return
        cursor = self._seed_cursor.get(page_id, 0)
        groups = stream.source.seed_groups
        if cursor >= len(groups):
            self.stats.seed_unavailable += 1
            return
        self._seed_cursor[page_id] = cursor + 1
        delay = source.config.faults.seed_delay_seconds(len(self._activated))
        for recorded in groups[cursor]:
            rewritten = self._rewrite(recorded, stream, request_id=request_id,
                                      page_id=page_id)
            frame = wire_frame(rewritten)
            if delay > 0:
                self._sequence += 1
                heapq.heappush(self._scheduled, (self.clock() + delay, self._sequence, frame))
                self.stats.seed_delayed += 1
                self.stats.seed_delay_max_seconds = max(self.stats.seed_delay_max_seconds, delay)
            else:
                self._ready.append(frame)
            self.stats.seed_responses += 1

    def recv(self, size: int) -> bytes:
        if self.closed:
            raise ConnectionResetError("replay connection closed")
        if type(size) is not int or size <= 0:
            raise ValueError("invalid receive size")
        while True:
            if self._ready:
                frame = self._ready.popleft()
                if len(frame) > size:
                    self._ready.appendleft(frame[size:])
                    frame = frame[:size]
                self.stats.frames_returned += 1
                self.stats.bytes_returned += len(frame)
                self.last_delivery_wall_ns = time.perf_counter_ns()
                return frame
            self._drain_due()
            if self._ready:
                continue
            if self._scheduled:
                due = self._scheduled[0][0]
                wait = due - self.clock()
                if wait <= self.timeout:
                    self.clock.sleep(max(0.0, wait))
                    self._drain_due()
                    continue
            self.stats.idle_timeouts += 1
            self.clock.sleep(self.timeout)
            raise TimeoutError()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.frames_scheduled += len(self._scheduled)
        self.frames_dropped += len(self._scheduled)
        self._scheduled.clear()
        self._ready.clear()

    def published(self) -> dict[str, Any]:
        return {
            "activations": len(self._activated),
            "pending_ready": len(self._ready),
            "pending_scheduled": len(self._scheduled),
            "frames_dropped_at_close": self.frames_dropped,
            "closed": self.closed,
        }

    def _deliver_controls(self) -> None:
        if self._controls_sent:
            return
        self._controls_sent = True
        faults = self.source.config.faults
        for body in self.source.index.control:
            if faults.withhold_bootstrap_ack and _is_ack(body):
                continue
            self._ready.append(wire_frame(body))
            self.stats.control_frames += 1
        if not faults.withhold_bootstrap_ack and not any(
                _is_ack(body) for body in self.source.index.control):
            if self.source.config.synthetic_ack:
                self._ready.append(wire_frame(_synthetic_ack_body()))
                self.stats.synthetic_ack_frames += 1
                self.stats.control_frames += 1

    def _activate(self, stream: TargetStream) -> None:
        if stream.page_id in self._activated:
            return
        now = self.clock()
        self._activated[stream.page_id] = now
        self.stats.activations += 1
        speed = self.source.config.speed
        for offset_us, body in stream.pushes:
            due = now if speed == FULL_SPEED else now + offset_us / 1_000_000 / speed
            rewritten = self._rewrite(body, stream, request_id=_PUSH_REQUEST_ID,
                                      page_id=stream.page_id)
            self._sequence += 1
            heapq.heappush(self._scheduled, (due, self._sequence, wire_frame(rewritten)))
            self.stats.push_frames += 1

    def _drain_due(self) -> None:
        now = self.clock()
        while self._scheduled and self._scheduled[0][0] <= now:
            _due, _sequence, frame = heapq.heappop(self._scheduled)
            self._ready.append(frame)

    def _rewrite(self, body: bytes, stream: TargetStream, *, request_id: int,
                 page_id: int) -> bytes:
        """Re-stamp only what expansion changed; otherwise keep the bytes exact."""
        if len(body) < 26:
            return body
        command = int.from_bytes(body[16:20], "little")
        family_changed = False
        if (self.source.config.adapt_envelope_family and command in _MARKET_FAMILIES
                and command != MARKET_COMMAND):
            body = body[:16] + MARKET_COMMAND.to_bytes(4, "little") + body[20:]
            self.stats.family_restamped += 1
            family_changed = True
        code_pair = (stream.source.code, stream.stock_code) if (
            command == 2267 and stream.rewrites_code) else (None, None)
        recorded_request = int.from_bytes(body[2:6], "little")
        recorded_page = int.from_bytes(body[10:12], "little")
        if (not family_changed and recorded_request == request_id
                and recorded_page == page_id and code_pair[0] is None):
            return body
        old_code, new_code = code_pair
        try:
            return rewrite_response(body, request_id=request_id, page_id=page_id,
                                    old_code=old_code, new_code=new_code)
        except ValueError:
            raise ReplaySourceError("invalid_frame") from None

    @property
    def scheduled_frames(self) -> int:
        return len(self._scheduled)

    def __enter__(self) -> ReplayConnection:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def iter_pack_frames(directory: Path | str) -> Iterator[tuple[int, bytes]]:
    """Yield ``(request_id, body)`` for every RX market frame, for diagnostics."""
    index = read_pack_index(directory)
    for segment in index.manifest.get("segments", ()):
        for record in iter_records(index.directory / segment["file"]):
            if record.kind != KIND_RX:
                continue
            body = split_wire_frame(record.payload)
            if _is_control(body):
                continue
            yield int.from_bytes(body[2:6], "little"), body


def build_source(pack: Path | str, symbols: Sequence[str], *, speed: float = FULL_SPEED,
                 clock_mode: str = "virtual", push_window_seconds: float | None = None,
                 faults: FaultInjection | None = None,
                 adapt_envelope_family: bool = True) -> L2ReplaySource:
    """Small convenience wrapper used by the offline gate and its tests."""
    return L2ReplaySource(ReplayConfig(
        pack=Path(pack), symbols=tuple(symbols), speed=speed, clock_mode=clock_mode,
        push_window_seconds=push_window_seconds,
        faults=faults if faults is not None else FaultInjection(),
        adapt_envelope_family=adapt_envelope_family))


def collector_kwargs(source: L2ReplaySource) -> dict[str, Callable[..., Any]]:
    """Clock/sleep/connection_factory triple for ``FeedCollector(**kwargs)``."""
    return {"clock": source.clock, "sleep": source.clock.sleep,
            "connection_factory": source.connect}
