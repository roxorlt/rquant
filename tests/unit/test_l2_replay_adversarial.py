"""Adversarial and boundary checks for the deterministic L2 replay source.

This file is deliberately independent of ``test_l2_replay_source.py``: it drives
the unmodified :class:`FeedCollector` through the replay connection and pins the
failure classification, tier limits, fault edges, speed semantics, determinism
and protocol surface. It never asserts on replay internals beyond the published
plan/stats surface plus the connection's documented socket methods.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

import pytest

from rquant.l2._codec import (build_common_order_frame, build_trade_frame,
                              snappy_decompress)
from rquant.l2._request import build_order_query, build_trade_query
from rquant.l2.cache import RawL2Cache
from rquant.l2.feed import FeedCollector
from rquant.l2.frame_tap import (FORMAT_NAME as TAP_FORMAT_NAME, KIND_RX, TapFileWriter,
                                 split_wire_frame, wire_frame, write_manifest)
from rquant.l2.replay_pack import PACK_FORMAT
from rquant.l2.replay_source import (FaultInjection, L2ReplaySource, ReplayConfig,
                                     ReplaySourceError, TargetStream, build_source,
                                     read_pack_index)
from rquant.l2.symbols import market_for_code
from tests.unit.test_l2_codec import bootstrap, template

PACK = Path("/Users/roxor/Documents/ChatGPT/rea L2/.tools/replay-packs/20261016-live-6stream")
SMALL_TIER = ("603258", "300006")
PACK_PAGES = (10117, 10118, 10119, 10127, 10128, 10129)
REAL_TIER = tuple(
    "603258 300006 600000 600519 600036 601318 600900 601166 600276 601888 "
    "601398 600030 601899 688981 000001 000333 000858 002594 300750 300059 "
    "603861 605058 000823 603533 605287 300530 001300 002805 002838 002709 "
    "002733 001206 300769 000710 600789 002923 002728 002273 002913 603980".split())


def _inputs(directory: Path) -> dict[str, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, data in (("bootstrap", bootstrap()), ("order", template(False)),
                       ("trade", template(True))):
        path = directory / name
        path.write_bytes(data)
        path.chmod(0o600)
        paths[name] = path
    return paths


def _collector(source: L2ReplaySource, directory: Path, *,
               duration_seconds: float | None = None) -> tuple[FeedCollector, RawL2Cache]:
    inputs = _inputs(directory / "inputs")
    cache = RawL2Cache(directory / "cache")
    collector = FeedCollector(
        cache, bootstrap_path=inputs["bootstrap"], order_template_path=inputs["order"],
        trade_template_path=inputs["trade"],
        duration_seconds=(source.planned_duration_seconds() if duration_seconds is None
                          else duration_seconds),
        symbols=source.config.symbols, max_symbols=len(source.config.symbols),
        clock=source.clock, sleep=source.clock.sleep, connection_factory=source.connect)
    return collector, cache


def _run(source: L2ReplaySource, directory: Path, *,
         duration_seconds: float | None = None) -> tuple[Any, RawL2Cache]:
    collector, cache = _collector(source, directory, duration_seconds=duration_seconds)
    return collector.run(), cache


def _copy_pack(tmp_path: Path, name: str) -> Path:
    target = tmp_path / name
    shutil.copytree(PACK, target)
    return target


def _patch_manifest(pack: Path, mutate: Any) -> None:
    path = pack / "manifest.json"
    manifest = json.loads(path.read_text())
    mutate(manifest)
    path.write_text(json.dumps(manifest))


def _seed_only_pack(tmp_path: Path) -> Path:
    """Minimal valid pack with one recorded stock's three seed frames and no pushes."""
    index = read_pack_index(PACK)
    bodies = [index.streams[page].seed_groups[0][0] for page in PACK_PAGES[:3]]
    directory = tmp_path / "seed-only-pack"
    directory.mkdir()
    writer = TapFileWriter(directory, meta={"format": TAP_FORMAT_NAME},
                           started_wall_us=1_792_000_000_000_000, started_mono_us=0,
                           segment_bytes=4096)
    for mono_us, body in zip((0, 1_000_000, 2_000_000), bodies):
        writer.add(KIND_RX, mono_us, wire_frame(body))
    summary = writer.close()
    write_manifest(directory, {
        "format": PACK_FORMAT, "version": 1, "tap_format": TAP_FORMAT_NAME,
        "timing": {"unit": "microseconds", "duration_us": 2_000_000},
        "frames": {"rx": 3, "tx": 0, "event": 0, "market": 3, "synthetic_control": 0},
        "symbols": {"603258": list(PACK_PAGES[:3])},
        "segments": summary["segments"], "bytes": summary["bytes"],
        "synthetic": {"bootstrap_ack": 0},
    })
    return directory


def _request_wire(stream: TargetStream, request_id: int) -> bytes:
    market = market_for_code(stream.stock_code)
    if stream.kind == "vendor4013":
        query = build_trade_query(code=stream.stock_code, market=market,
                                  page_id=stream.page_id)
        return build_trade_frame(template(True), query, request_id=request_id)
    query = build_order_query(code=stream.stock_code, market=market, side=stream.side,
                              page_id=stream.page_id, rowcount=20, endtime=0, orderid=0)
    return build_common_order_frame(template(False), query, request_id=request_id)


def _drive(source: L2ReplaySource) -> dict[str, Any]:
    """Send one bootstrap and one seed per stream, then drain the whole timeline."""
    connection = source.open()
    connection.settimeout(10_000.0)
    connection.sendall(wire_frame(bootstrap()))
    for index, stream in enumerate(sorted(source.streams.values(), key=lambda s: s.page_id)):
        connection.sendall(_request_wire(stream, 3 + index))
    consumed = 0
    while True:
        pending = connection.published()
        if not pending["pending_ready"] and not pending["pending_scheduled"]:
            break
        connection.recv(1 << 20)
        consumed += 1
    return {
        "virtual_seconds": round(source.clock.virtual_seconds, 6),
        "consumed": consumed,
        "scheduled_left": connection.scheduled_frames,
        "control_frames": source.stats.control_frames,
        "seed_responses": source.stats.seed_responses,
        "push_frames": source.stats.push_frames,
    }


def _batch_digest(cache: RawL2Cache) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    for batch in cache.iter_batches():
        binding = batch.binding
        digest.update(f"{binding.page_id}|{binding.kind}|{binding.code}|{binding.side}|"
                      f"{batch.delivery}|{batch.response_sha256}\n".encode())
        count += 1
    return digest.hexdigest(), count


def _title_of(body: bytes) -> str:
    header_len = int.from_bytes(body[0:2], "little")
    table = snappy_decompress(body[header_len:])
    count = int.from_bytes(table[22:24], "little")
    cursor = 24 + 4 * count
    size = int.from_bytes(table[cursor:cursor + 2], "little") * 2
    return table[cursor + 2:cursor + 2 + size].decode("utf-16le")


# 1. Envelope family normalization is required, not decorative.

def test_strict_envelope_family_blocks_after_the_trade_seed(tmp_path: Path) -> None:
    source = build_source(PACK, SMALL_TIER, speed=0.0, push_window_seconds=0.5,
                          adapt_envelope_family=False)
    status, cache = _run(source, tmp_path)
    assert status.state == "blocked"
    assert status.error_code == "association_mismatch"
    assert source.stats.family_restamped == 0
    batches = list(cache.iter_batches())
    assert len(batches) == status.market_batch_count == 1
    assert batches[0].binding.kind == "vendor4013"
    assert status.unknown_push_count == 1


def test_adapted_family_keeps_the_same_run_healthy(tmp_path: Path) -> None:
    strict = build_source(PACK, SMALL_TIER, speed=0.0, push_window_seconds=0.5,
                          adapt_envelope_family=False)
    adapted = build_source(PACK, SMALL_TIER, speed=0.0, push_window_seconds=0.5)
    strict_status, _cache = _run(strict, tmp_path / "strict")
    adapted_status, _cache = _run(adapted, tmp_path / "adapted")
    assert strict_status.error_code == "association_mismatch"
    assert adapted_status.state == "stopped" and adapted_status.error_code == "duration_limit"
    assert adapted.stats.family_restamped > 0


# 2. Pack classification: every malformed pack lands on a fixed local code.

@pytest.mark.parametrize("fault,code", [
    ("manifest_missing", "manifest_missing"),
    ("empty_directory", "manifest_missing"),
    ("format", "unsupported_pack"),
    ("tap_format", "unsupported_pack"),
    ("no_symbols", "unsupported_pack"),
    ("page_base", "unsupported_pack"),
    ("page_gap", "unsupported_pack"),
    ("page_four_streams", "unsupported_pack"),
    ("truncated_segment", "invalid_pack"),
    ("tampered_footer", "invalid_pack"),
    ("missing_segment", "invalid_pack"),
])
def test_pack_classification_is_a_fixed_local_code(tmp_path: Path, fault: str, code: str) -> None:
    pack = _copy_pack(tmp_path, "pack")
    if fault == "manifest_missing":
        (pack / "manifest.json").unlink()
    elif fault == "empty_directory":
        shutil.rmtree(pack)
        pack.mkdir()
    elif fault == "format":
        _patch_manifest(pack, lambda manifest: manifest.update(
            {"format": "rquant-l2-replay-pack-v0"}))
    elif fault == "tap_format":
        _patch_manifest(pack, lambda manifest: manifest.update(
            {"tap_format": "rquant-l2-frame-tap-v0"}))
    elif fault == "no_symbols":
        _patch_manifest(pack, lambda manifest: manifest.pop("symbols"))
    elif fault == "page_base":
        _patch_manifest(pack, lambda manifest: manifest.update(
            {"symbols": {"603258": [10120, 10121, 10122]}}))
    elif fault == "page_gap":
        _patch_manifest(pack, lambda manifest: manifest.update(
            {"symbols": {"603258": [10117, 10119, 10120]}}))
    elif fault == "page_four_streams":
        _patch_manifest(pack, lambda manifest: manifest.update(
            {"symbols": {"603258": [10117, 10118, 10119, 10120]}}))
    elif fault == "missing_segment":
        _patch_manifest(pack, lambda manifest: manifest["segments"][0].update(
            {"file": "segment-999999.tap"}))
    else:
        manifest = json.loads((pack / "manifest.json").read_text())
        segment = pack / manifest["segments"][0]["file"]
        data = bytearray(segment.read_bytes())
        if fault == "truncated_segment":
            del data[-64:]
        else:
            data[-1] ^= 0xFF
        segment.write_bytes(data)
    with pytest.raises(ReplaySourceError) as error:
        read_pack_index(pack)
    assert error.value.code == code


# 3. Expansion boundaries: 1 seeded-only stock, 3 stocks, and the 100-stock ceiling.

def test_one_stock_seed_only_pack_plans_and_replays(tmp_path: Path) -> None:
    pack = _seed_only_pack(tmp_path)
    source = build_source(pack, ("603258",), speed=0.0)
    assert len(source.streams) == 3
    assert {stream.page_id for stream in source.streams.values()} == set(PACK_PAGES[:3])
    assert source.planned_seed_responses == 3
    assert source.planned_seed_withheld == 0
    assert source.planned_push_frames == 0
    assert source.synthetic_streams == 0
    status, cache = _run(source, tmp_path / "run")
    assert status.state == "stopped" and status.error_code == "duration_limit"
    assert status.market_batch_count == 3
    assert {batch.delivery for batch in cache.iter_batches()} == {"seed"}
    receipt = cache.read_subscription_status()
    assert [stream.seed_sequence for stream in receipt.streams] == [1, 2, 3]


def test_three_stock_tier_clones_only_the_third_stock() -> None:
    source = build_source(PACK, REAL_TIER[:3], speed=0.0, push_window_seconds=0.5)
    streams = source.streams.values()
    assert len(source.streams) == 9
    assert len({stream.page_id for stream in streams}) == 9
    assert {stream.stock_code for stream in streams} == set(REAL_TIER[:3])
    assert source.planned_seed_responses == 9
    assert source.synthetic_streams == 3
    assert {stream.page_id for stream in streams if stream.synthetic} == {10137, 10138, 10139}
    assert {stream.page_id for stream in streams if not stream.synthetic} == set(PACK_PAGES)
    assert source.planned_push_frames == sum(len(stream.pushes) for stream in streams)
    assert source.planned_push_frames > 0


def test_hundred_stock_tier_hits_the_page_and_request_id_ceiling() -> None:
    tier = REAL_TIER + tuple(f"6900{index:02d}" for index in range(60))
    source = build_source(PACK, tier, speed=0.0, push_window_seconds=0.5)
    streams = tuple(source.streams.values())
    assert len(tier) == 100 and len(source.streams) == 300
    assert len({stream.page_id for stream in streams}) == 300
    assert max(stream.page_id for stream in streams) == 11109
    assert max(stream.page_id for stream in streams) <= 0xFFFF
    assert len({stream.stock_code for stream in streams}) == 100
    assert source.planned_seed_responses == 300
    assert source.planned_seed_withheld == 0
    assert source.synthetic_streams == 300 - 6
    assert source.planned_push_frames == sum(len(stream.pushes) for stream in streams)
    last_request_id = 3 + len(source.streams) - 1
    collector_maximum = 2 + 100 * (1 + 2 * 1)
    assert last_request_id == collector_maximum == 302
    assert last_request_id <= 902
    assert [stream.page_id for stream in source.streams_for_stock(99)] == [
        10117 + 10 * 99 + offset for offset in range(3)]


@pytest.mark.parametrize("symbols", [("603258",) * 101, ("123456",), ()])
def test_invalid_symbol_lists_are_rejected(symbols: tuple[str, ...]) -> None:
    with pytest.raises(ReplaySourceError) as error:
        ReplayConfig(pack=PACK, symbols=symbols, speed=0.0)
    assert error.value.code == "invalid_symbols"


def test_duplicate_symbols_are_rejected_before_normalization() -> None:
    # A tier is an explicit symbol list: a silently deduplicated 40-stock gate
    # would really run 39 symbols and report a false seed rate.
    with pytest.raises(ReplaySourceError) as error:
        build_source(PACK, ("603258", "603258"), speed=0.0, push_window_seconds=0.5)
    assert error.value.code == "invalid_symbols"
    with pytest.raises(ReplaySourceError) as error:
        build_source(PACK, ("603258", "300006", "603258"), speed=0.0,
                     push_window_seconds=0.5)
    assert error.value.code == "invalid_symbols"


@pytest.mark.parametrize("faults", [
    FaultInjection(withhold_seed_codes=("600000",)),
    FaultInjection(stall_codes=("600000",)),
    FaultInjection(withhold_seed_from_stock=3),
])
def test_tier_level_fault_scope_is_validated(faults: FaultInjection) -> None:
    with pytest.raises(ReplaySourceError) as error:
        build_source(PACK, SMALL_TIER, speed=0.0, push_window_seconds=0.5, faults=faults)
    assert error.value.code == "invalid_fault"


def test_tier_level_fault_scope_accepts_a_code_present_in_the_tier() -> None:
    source = build_source(PACK, REAL_TIER, speed=0.0, push_window_seconds=0.5,
                          faults=FaultInjection(withhold_seed_codes=("600000",)))
    assert source.planned_seed_withheld == 3


# 4. Fault-injection edges.

def test_all_seeds_withheld_ends_blocked_as_seed_timeout(tmp_path: Path) -> None:
    source = build_source(PACK, SMALL_TIER, speed=0.0, push_window_seconds=0.5,
                          faults=FaultInjection(withhold_seed_from_stock=0))
    assert source.planned_seed_withheld == 6
    status, cache = _run(source, tmp_path)
    assert status.state == "blocked"
    assert status.error_code == "seed_timeout"
    assert source.stats.seed_withheld == 6 and source.stats.seed_responses == 0
    receipt = cache.read_subscription_status()
    assert receipt is not None
    assert not any(stream.seed_sequence for stream in receipt.streams)
    # Current semantics: withholding a seed suppresses only the seed frames; the
    # request-gated push timeline still activates, so every committed batch is a
    # candidate_push and never a seed.
    batches = list(cache.iter_batches())
    assert batches and {batch.delivery for batch in batches} == {"candidate_push"}
    assert status.market_batch_count == source.stats.push_frames


def test_withheld_bootstrap_ack_never_counts_a_control_frame(tmp_path: Path) -> None:
    source = build_source(PACK, SMALL_TIER, speed=0.0, push_window_seconds=0.5,
                          faults=FaultInjection(withhold_bootstrap_ack=True))
    status, cache = _run(source, tmp_path, duration_seconds=40.0)
    assert status.state == "blocked"
    assert status.error_code == "seed_timeout"
    assert source.stats.control_frames == 0
    assert status.seed_queries == 0 and status.market_batch_count == 0
    assert list(cache.iter_batches()) == []


def test_stall_and_withhold_on_the_same_code_stay_stable(tmp_path: Path) -> None:
    source = build_source(PACK, SMALL_TIER, speed=0.0, push_window_seconds=0.5,
                          faults=FaultInjection(withhold_seed_codes=("300006",),
                                                stall_codes=("300006",)))
    assert source.planned_seed_withheld == 3
    assert all(stream.pushes == () for stream in source.streams.values()
               if stream.stock_code == "300006")
    status, _cache = _run(source, tmp_path)
    assert status.state == "stopped" and status.error_code == "duration_limit"
    assert status.market_batch_count == source.planned_seed_responses + source.planned_push_frames
    assert source.stats.seed_withheld == 3


@pytest.mark.parametrize("kwargs", [
    {"withhold_seed_from_stock": -1},
    {"withhold_seed_from_stock": 101},
    {"withhold_seed_from_stock": 1.0},
    {"withhold_seed_from_stock": "24"},
    {"withhold_seed_codes": "603258"},
    {"withhold_seed_codes": ("not-a-code",)},
    {"withhold_seed_codes": ("123456",)},
    {"stall_codes": "603258"},
    {"stall_codes": ("dup",)},
    {"withhold_bootstrap_ack": 1},
    {"withhold_bootstrap_ack": "yes"},
])
def test_invalid_faults_are_classified(kwargs: dict) -> None:
    with pytest.raises(ReplaySourceError) as error:
        FaultInjection(**kwargs)
    assert error.value.code == "invalid_fault"


@pytest.mark.parametrize("kwargs,code", [
    ({"speed": -1.0}, "invalid_speed"),
    ({"speed": float("nan")}, "invalid_speed"),
    ({"speed": True}, "invalid_speed"),
    ({"clock_mode": "fast"}, "invalid_config"),
    ({"push_window_seconds": 0}, "invalid_config"),
    ({"push_window_seconds": -1.0}, "invalid_config"),
    ({"push_window_seconds": 6 * 3600.0 + 1}, "invalid_config"),
    ({"synthetic_ack": 1}, "invalid_config"),
    ({"adapt_envelope_family": 1}, "invalid_config"),
    ({"faults": object()}, "invalid_fault"),
])
def test_invalid_config_fields_are_classified(kwargs: dict, code: str) -> None:
    base = {"pack": PACK, "symbols": ("603258",), "speed": 0.0}
    with pytest.raises(ReplaySourceError) as error:
        ReplayConfig(**{**base, **kwargs})
    assert error.value.code == code


# 5. Speed semantics on one identical two-stock window.

def test_speed_ten_plays_ten_times_faster_than_speed_one() -> None:
    window = 30.0
    states = {}
    for speed in (1.0, 10.0):
        source = build_source(PACK, SMALL_TIER, speed=speed, push_window_seconds=window)
        states[speed] = _drive(source)
        assert states[speed]["scheduled_left"] == 0
        assert states[speed]["consumed"] == (states[speed]["control_frames"]
                                             + states[speed]["seed_responses"]
                                             + states[speed]["push_frames"])
    assert states[1.0]["push_frames"] == states[10.0]["push_frames"] > 0
    ratio = states[1.0]["virtual_seconds"] / states[10.0]["virtual_seconds"]
    assert 5 < ratio < 13
    assert states[1.0]["virtual_seconds"] <= window + 1
    assert states[10.0]["virtual_seconds"] <= window / 10 + 1


def test_full_speed_delivers_the_whole_window_within_the_planned_deadline(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = build_source(PACK, SMALL_TIER, speed=0.0, push_window_seconds=0.5)
    planned = source.planned_duration_seconds()
    collector, cache = _collector(source, tmp_path)
    delivered: list[tuple[str, float]] = []
    append = cache.append

    def observe(observation):
        delivered.append((observation.delivery, source.clock()))
        return append(observation)

    monkeypatch.setattr(cache, "append", observe)
    status = collector.run()
    assert status.state == "stopped" and status.error_code == "duration_limit"
    assert status.market_batch_count == (source.planned_seed_responses
                                         + source.planned_push_frames)
    assert status.market_batch_count == len(delivered)
    assert source.clock.virtual_seconds <= planned
    assert abs(source.clock.virtual_seconds - planned) <= 1.0
    pushes = [moment for delivery, moment in delivered if delivery == "candidate_push"]
    assert pushes and min(pushes) <= planned - 3.0


# 6. Determinism: identical configuration must replay identical batches.

def test_replay_is_deterministic_across_two_runs(tmp_path: Path) -> None:
    digests, sequences, counts = [], [], []
    for run in ("one", "two"):
        source = build_source(PACK, SMALL_TIER, speed=0.0, push_window_seconds=0.5)
        status, cache = _run(source, tmp_path / run)
        digest, count = _batch_digest(cache)
        digests.append(digest)
        counts.append(count)
        sequences.append(tuple(stream.seed_sequence
                               for stream in cache.read_subscription_status().streams))
        assert status.market_batch_count == count
    assert counts[0] == counts[1] > 0
    assert digests[0] == digests[1]
    assert sequences[0] == sequences[1]


# 7. Wall-clock mode stays a bounded real-time run.

def test_wall_clock_replays_a_small_window_in_real_time(tmp_path: Path) -> None:
    source = build_source(PACK, SMALL_TIER, speed=0.0, push_window_seconds=0.5,
                          clock_mode="wall")
    started = time.monotonic()
    status, _cache = _run(source, tmp_path, duration_seconds=6.5)
    elapsed = time.monotonic() - started
    assert source.clock.wall is True
    assert source.clock.virtual_seconds == 0.0
    assert status.state == "stopped" and status.error_code == "duration_limit"
    assert status.market_batch_count == (source.planned_seed_responses
                                         + source.planned_push_frames)
    assert elapsed < 30.0  # deliberately small wall-clock scenario


# 8. Connection protocol surface.

def test_connection_protocol_classification(tmp_path: Path) -> None:
    source = build_source(PACK, ("603258",), speed=0.0, push_window_seconds=0.5)
    connection = source.open()
    # recv() before settimeout must idle-timeout on the default timeout, not crash.
    with pytest.raises(TimeoutError):
        connection.recv(16)
    assert source.clock.virtual_seconds > 0
    for wire in (b"not-a-frame",
                 b"\xfd" * 4 + b"00000010" + b"\0" + b"short",
                 b"\xfd" * 4 + b"zzzzzzzz" + b"\0" + b"payload"):
        with pytest.raises(ReplaySourceError) as error:
            connection.sendall(wire)
        assert error.value.code == "invalid_request"
    connection.settimeout(0.5)
    connection.sendall(wire_frame(bootstrap()))
    ack = split_wire_frame(connection.recv(1 << 20))
    assert len(ack) == 26
    unknown = build_common_order_frame(
        template(False), build_order_query(code="603258", market=17, side="buy", page_id=65000,
                                           rowcount=20, endtime=0, orderid=0), request_id=9)
    with pytest.raises(ReplaySourceError) as error:
        connection.sendall(unknown)
    assert error.value.code == "unknown_page"
    connection.close()
    with pytest.raises(ConnectionResetError):
        connection.recv(16)
    with pytest.raises(OSError):
        connection.sendall(wire_frame(bootstrap()))


def test_order_titles_match_bindings_after_expansion(tmp_path: Path) -> None:
    source = build_source(PACK, REAL_TIER[:3], speed=0.0, push_window_seconds=0.5)
    status, cache = _run(source, tmp_path)
    assert status.state == "stopped" and status.error_code == "duration_limit"
    checked = 0
    for batch in cache.iter_batches():
        if batch.binding.kind != "vendor4027":
            continue
        assert _title_of(bytes.fromhex(batch.response_body_hex)) == (
            batch.binding.code + "-逐笔委托")
        checked += 1
    expected = sum(1 + len(stream.pushes) for stream in source.streams.values()
                   if stream.kind == "vendor4027")
    assert checked == expected > 0
