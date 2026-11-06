"""Deterministic L2 replay source: gate release, expansion, speeds and faults."""
from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path

import pytest

from rquant.l2._codec import (build_common_order_frame, build_trade_frame,
                              decode_order_response)
from rquant.l2._request import build_order_query, build_trade_query
from rquant.l2.cache import RawL2Cache
from rquant.l2.feed import FeedCollector
from rquant.l2.frame_tap import split_wire_frame, wire_frame
from rquant.l2.replay_source import (FaultInjection, ReplayConfig, ReplaySourceError,
                                     build_source, read_pack_index, request_identity)
from tests.unit.test_l2_codec import bootstrap, template

PACK = Path("/Users/roxor/Documents/ChatGPT/rea L2/.tools/replay-packs/20261016-live-6stream")
REAL_TIER = tuple(
    "603258 300006 600000 600519 600036 601318 600900 601166 600276 601888 "
    "601398 600030 601899 688981 000001 000333 000858 002594 300750 300059 "
    "603861 605058 000823 603533 605287 300530 001300 002805 002838 002709 "
    "002733 001206 300769 000710 600789 002923 002728 002273 002913 603980".split())
PACK_PAGES = (10117, 10118, 10119, 10127, 10128, 10129)


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


def run_replay(tmp_path: Path, source, *, duration_seconds: float | None = None) -> tuple:
    """Drive the unmodified FeedCollector from one replay source."""
    inputs = _inputs(tmp_path / "inputs")
    cache = RawL2Cache(tmp_path / "cache")
    collector = FeedCollector(
        cache, bootstrap_path=inputs["bootstrap"], order_template_path=inputs["order"],
        trade_template_path=inputs["trade"],
        duration_seconds=(source.planned_duration_seconds() if duration_seconds is None
                          else duration_seconds),
        symbols=source.config.symbols, max_symbols=len(source.config.symbols),
        clock=source.clock, sleep=source.clock.sleep, connection_factory=source.connect)
    status = collector.run()
    return status, cache


def _batch_digest(cache: RawL2Cache) -> str:
    digest = hashlib.sha256()
    for batch in cache.iter_batches():
        binding = batch.binding
        digest.update(f"{binding.page_id}|{binding.kind}|{binding.code}|{binding.side}|"
                      f"{batch.delivery}|{batch.response_sha256}\n".encode())
    return digest.hexdigest()


def _seeded(receipt) -> list:
    return [stream for stream in receipt.streams if stream.seed_sequence is not None]


def _payload(body: bytes) -> bytes:
    """Body with the 4-byte response-family word removed (the only replay diff)."""
    return body[:16] + body[20:] if len(body) >= 20 else body


def _only_family_differs(left: bytes, right: bytes) -> bool:
    return left == right or (
        len(left) == len(right) >= 20 and _payload(left) == _payload(right))


def _order_request(request_id: int, page_id: int) -> bytes:
    return build_common_order_frame(
        template(False), build_order_query(code="603258", market=17, side="buy", rowcount=20,
                                           endtime=0, orderid=0, page_id=page_id),
        request_id=request_id)


def test_pack_index_classifies_control_seed_groups_and_pushes() -> None:
    index = read_pack_index(PACK)
    assert len(index.control) == 1 and len(index.control[0]) == 26
    assert sorted(index.streams) == list(PACK_PAGES)
    assert index.rx_frames == 3383 and index.record_count == 3383
    assert index.synthetic_ack is True
    assert index.duration_us == 179745378
    assert sum(len(group) for stream in index.streams.values()
               for group in stream.seed_groups) == 6
    assert sum(len(stream.pushes) for stream in index.streams.values()) == 3376
    assert index.streams[10117].code == "603258" and index.streams[10117].market == 17
    assert index.streams[10127].code == "300006" and index.streams[10127].market == 33
    assert index.stock_indexes == (0, 1)
    assert index.codes == ("603258", "300006")


def test_two_stock_replay_is_payload_identical_and_fully_seeded(tmp_path: Path) -> None:
    source = build_source(PACK, ("603258", "300006"), speed=0.0)
    status, cache = run_replay(tmp_path, source)
    assert status.state == "stopped" and status.error_code == "duration_limit"
    assert status.market_batch_count == 3382
    assert status.seed_queries == 6
    receipt = cache.read_subscription_status()
    assert len(_seeded(receipt)) == 6
    index = read_pack_index(PACK)
    replayed: dict[int, list[bytes]] = {}
    for batch in cache.iter_batches():
        replayed.setdefault(batch.binding.page_id, []).append(
            bytes.fromhex(batch.response_body_hex))
    for page in PACK_PAGES:
        recorded = index.streams[page]
        expected = ([body for group in recorded.seed_groups for body in group]
                    + [body for _mono, body in recorded.pushes])
        actual = replayed.get(page, [])
        assert len(actual) == len(expected)
        assert all(_only_family_differs(one, other)
                   for one, other in zip(actual, expected))
        assert [_payload(body) for body in actual] == [_payload(body) for body in expected]
    assert source.stats.family_restamped == 3374


def test_expansion_rewrites_page_and_symbol_and_keeps_decoding(tmp_path: Path) -> None:
    source = build_source(PACK, REAL_TIER, speed=0.0, push_window_seconds=5.0)
    assert len(source.streams) == 120
    assert source.synthetic_streams == 114  # 40 stocks x 3 streams - 6 recorded streams
    assert len({stream.page_id for stream in source.streams.values()}) == 120
    assert max(stream.page_id for stream in source.streams.values()) == 10117 + 10 * 39 + 2
    status, cache = run_replay(tmp_path, source)
    assert status.state == "stopped" and status.error_code == "duration_limit"
    assert status.market_batch_count == (source.planned_seed_responses
                                         + source.planned_push_frames)
    assert status.seed_queries == 120
    receipt = cache.read_subscription_status()
    assert len(_seeded(receipt)) == 120
    assert {stream.binding.code for stream in receipt.streams} == set(REAL_TIER)
    cloned = next(batch for batch in cache.iter_batches()
                  if batch.binding.page_id == 10138 and batch.delivery == "seed")
    decoded = decode_order_response(
        bytes.fromhex(cloned.response_body_hex), request_id=cloned.request_id,
        page_id=10138, code="600000", source_fd_ordinal=cloned.source_fd_ordinal)
    assert decoded["table"]["title"] == "600000-逐笔委托"
    assert decoded["table"]["rows"]


def test_fault_seed_limit_reproduces_the_24_of_40_incident(tmp_path: Path) -> None:
    source = build_source(PACK, REAL_TIER, speed=0.0, push_window_seconds=5.0,
                          faults=FaultInjection(withhold_seed_from_stock=24))
    status, cache = run_replay(tmp_path, source)
    assert status.state == "stopped" and status.error_code == "duration_limit"
    assert status.seed_queries == 120
    receipt = cache.read_subscription_status()
    seeded = _seeded(receipt)
    assert len(seeded) == 72
    assert len({stream.binding.code for stream in seeded}) == 24
    assert len(receipt.streams) - len(seeded) == 48
    assert source.stats.seed_withheld == 48
    unseeded = {stream.binding.code for stream in receipt.streams
                if stream.seed_sequence is None}
    assert unseeded == set(REAL_TIER[24:])
    assert source.clock.virtual_seconds >= 48 * 15


def test_single_stock_silent_seed_timeout(tmp_path: Path) -> None:
    source = build_source(PACK, REAL_TIER, speed=0.0, push_window_seconds=5.0,
                          faults=FaultInjection(withhold_seed_codes=("300006",)))
    status, cache = run_replay(tmp_path, source)
    assert status.state == "stopped" and status.error_code == "duration_limit"
    receipt = cache.read_subscription_status()
    assert len(_seeded(receipt)) == 117
    assert {stream.binding.code for stream in receipt.streams
            if stream.seed_sequence is None} == {"300006"}
    assert source.stats.seed_withheld == 3


def test_single_stock_stall_after_seed_keeps_the_epoch_healthy(tmp_path: Path) -> None:
    healthy = build_source(PACK, REAL_TIER, speed=0.0, push_window_seconds=30.0)
    stalled = build_source(PACK, REAL_TIER, speed=0.0, push_window_seconds=30.0,
                           faults=FaultInjection(stall_codes=("300006",)))
    stalled_stock_pushes = sum(len(stream.pushes) for stream in healthy.streams.values()
                               if stream.stock_code == "300006")
    assert stalled_stock_pushes > 0
    assert (stalled.planned_push_frames
            == healthy.planned_push_frames - stalled_stock_pushes)
    status, cache = run_replay(tmp_path, stalled)
    assert status.state == "stopped" and status.error_code == "duration_limit"
    assert len(_seeded(cache.read_subscription_status())) == 120
    deliveries = Counter(batch.delivery for batch in cache.iter_batches())
    assert deliveries["seed"] == 120
    assert deliveries["candidate_push"] == stalled.planned_push_frames
    assert status.market_batch_count == 120 + stalled.planned_push_frames


def test_speed_modes_scale_the_recorded_timeline(tmp_path: Path) -> None:
    durations = {}
    for speed in (1.0, 10.0):
        source = build_source(PACK, ("603258", "300006"), speed=speed,
                              push_window_seconds=120.0)
        status, _cache = run_replay(tmp_path / f"speed-{speed:g}", source)
        assert status.error_code == "duration_limit"
        durations[speed] = source.clock.virtual_seconds
    assert durations[1.0] / 13 < durations[10.0] < durations[1.0] / 5
    full = build_source(PACK, ("603258", "300006"), speed=0.0, push_window_seconds=120.0)
    status, _cache = run_replay(tmp_path / "speed-full", full)
    assert status.error_code == "duration_limit"
    assert full.clock.virtual_seconds <= full.planned_duration_seconds()


def test_replay_is_deterministic_across_runs(tmp_path: Path) -> None:
    digests = []
    sequences = []
    for run in ("one", "two"):
        source = build_source(PACK, REAL_TIER, speed=0.0, push_window_seconds=5.0)
        _status, cache = run_replay(tmp_path / run, source)
        digests.append(_batch_digest(cache))
        sequences.append(tuple(stream.seed_sequence
                               for stream in cache.read_subscription_status().streams))
    assert digests[0] == digests[1]
    assert sequences[0] == sequences[1]


def test_withheld_bootstrap_ack_blocks_the_epoch(tmp_path: Path) -> None:
    source = build_source(PACK, ("603258", "300006"), speed=0.0, push_window_seconds=1.0,
                          faults=FaultInjection(withhold_bootstrap_ack=True))
    status, _cache = run_replay(tmp_path, source, duration_seconds=40.0)
    assert status.state == "blocked"
    assert status.error_code == "seed_timeout"
    assert source.stats.control_frames == 0


def test_strict_envelope_family_rejects_the_legacy_pack(tmp_path: Path) -> None:
    source = build_source(PACK, ("603258", "300006"), speed=0.0, push_window_seconds=1.0,
                          adapt_envelope_family=False)
    status, _cache = run_replay(tmp_path, source)
    assert status.state == "blocked"
    assert status.error_code == "association_mismatch"
    assert source.stats.family_restamped == 0


def test_request_identity_reads_order_and_trade_pages() -> None:
    order = _order_request(7, 10118)
    trade = build_trade_frame(template(True),
                              build_trade_query(code="300006", market=33, page_id=10127),
                              request_id=8)
    assert request_identity(order[13:]) == (7, 10118)
    assert request_identity(trade[13:]) == (8, 10127)
    with pytest.raises(ReplaySourceError):
        request_identity(b"\x00" * 26)


def test_config_and_fault_validation() -> None:
    with pytest.raises(ReplaySourceError):
        ReplayConfig(pack=PACK, symbols=(), speed=0.0)
    with pytest.raises(ReplaySourceError):
        ReplayConfig(pack=PACK, symbols=("603258",), speed=-1.0)
    with pytest.raises(ReplaySourceError):
        ReplayConfig(pack=PACK, symbols=("603258",), clock_mode="fast")
    with pytest.raises(ReplaySourceError):
        ReplayConfig(pack=PACK, symbols=("603258",), push_window_seconds=0)
    with pytest.raises(ReplaySourceError):
        FaultInjection(withhold_seed_from_stock=-1)
    with pytest.raises(ReplaySourceError):
        FaultInjection(withhold_seed_codes="603258")
    with pytest.raises(ReplaySourceError):
        FaultInjection(stall_codes=("dup",))
    with pytest.raises(ReplaySourceError):
        read_pack_index("/tmp/definitely-not-a-replay-pack")


def test_connection_protocol_surface() -> None:
    source = build_source(PACK, ("603258",), speed=0.0, push_window_seconds=1.0)
    connection = source.open()
    with pytest.raises(ReplaySourceError):
        connection.sendall(b"not-a-wire-frame")
    connection.settimeout(0.5)
    connection.sendall(wire_frame(bootstrap()))
    ack = connection.recv(1 << 20)
    assert len(split_wire_frame(ack)) == 26  # the recorded/synthetic ack family
    with pytest.raises(ReplaySourceError):
        connection.sendall(wire_frame(_order_request(9, 65000)[13:]))
    connection.close()
    with pytest.raises(ConnectionResetError):
        connection.recv(16)
