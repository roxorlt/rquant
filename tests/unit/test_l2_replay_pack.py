"""Replay pack builder: committed cache batches become byte-identical raw frames."""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.l2 import feed as feed_module
from rquant.l2._codec import decode_trade_response
from rquant.l2.cache import RawL2Cache
from rquant.l2.frame_tap import (KIND_EVENT, KIND_RX, iter_records, read_manifest,
                                 split_wire_frame, wire_frame)
from rquant.l2.models import MarketObservation
from rquant.l2.replay_pack import (PACK_FORMAT, ReplayPackConfig, ReplayPackError,
                                   build_replay_pack, verify_replay_pack)
from tests.unit.test_l2_codec import response


def _observation(body: bytes, *, request_id: int, page_id: int = 10117,
                 code: str = "603258", market: int = 17, delivery: str = "seed",
                 received_at: datetime, epoch_id: str = "epoch-test") -> MarketObservation:
    result = decode_trade_response(body, request_id=request_id, page_id=page_id,
                                   code=code, market=market, source_fd_ordinal=1)
    return MarketObservation(
        epoch_id=epoch_id,
        binding={"page_id": page_id, "kind": "vendor4013", "code": code, "market": market},
        received_at=received_at,
        request_id=request_id,
        source_fd_ordinal=1,
        delivery=delivery,
        response_body_hex=body.hex(),
        raw_table_hex=result["raw_table"].hex(),
        table=result["table"],
        response_sha256=hashlib.sha256(body).hexdigest(),
    )


def _make_cache(root: Path, *, batches: int = 3, start: datetime) -> tuple[RawL2Cache, list[bytes]]:
    cache = RawL2Cache(root)
    bodies: list[bytes] = []
    deliveries = ("seed", "backfill", "candidate_push")
    with cache.writer():
        for index in range(batches):
            page_id = 10117 + index
            request_id = 3 + index
            body = response(trade=True, request_id=request_id, page_id=page_id,
                            ids=(100 + index, 101 + index))
            bodies.append(body)
            cache.append(_observation(body, request_id=request_id, page_id=page_id,
                                      delivery=deliveries[index % len(deliveries)],
                                      received_at=start + timedelta(seconds=0.5 * index)))
    return cache, bodies


def _records(pack: Path) -> list:
    manifest = read_manifest(pack)
    assert manifest is not None
    records = []
    for segment in manifest["segments"]:
        records.extend(iter_records(pack / segment["file"]))
    return records


def test_pack_from_cache_roundtrips_raw_frames_and_timing(tmp_path: Path) -> None:
    start = datetime(2026, 10, 16, 1, 30, tzinfo=UTC)
    cache, bodies = _make_cache(tmp_path / "cache", start=start)
    out = tmp_path / "pack"
    manifest = build_replay_pack(ReplayPackConfig(cache_root=cache.root, output_dir=out))
    assert manifest["format"] == PACK_FORMAT
    assert manifest["frames"] == {"rx": 4, "tx": 0, "event": 0, "market": 3,
                                  "synthetic_control": 1, "total": 4}
    assert manifest["delivery"] == {"seed": 1, "backfill": 1, "candidate_push": 1}
    assert manifest["range"] == {"start_sequence": 1, "end_sequence": 3}
    assert manifest["symbols"] == {"603258": [10117, 10118, 10119]}
    assert manifest["synthetic"] == {"bootstrap_ack": 1}
    records = _records(out)
    assert [record.kind for record in records] == [KIND_RX] * 4
    ack_body = split_wire_frame(records[0].payload)
    assert feed_module._control_kind(ack_body) == "ack"
    assert [split_wire_frame(record.payload) for record in records[1:]] == bodies
    assert [record.mono_us for record in records] == [0, 0, 500_000, 1_000_000]
    summary = verify_replay_pack(out)
    assert summary["frames"] == {"rx": 4, "tx": 0, "event": 0}
    assert summary["segments"] == len(manifest["segments"])
    assert summary["synthetic_control"] == 1


def test_pack_wire_bytes_parse_through_the_receive_framer(tmp_path: Path) -> None:
    cache, bodies = _make_cache(tmp_path / "cache", start=datetime(2026, 10, 16, tzinfo=UTC))
    out = tmp_path / "pack"
    build_replay_pack(ReplayPackConfig(cache_root=cache.root, output_dir=out))
    data = b"".join(record.payload for record in _records(out))

    class ReplayConnection:
        def __init__(self, payload: bytes) -> None:
            self.data = bytearray(payload)
            self.timeout = 0.0

        def settimeout(self, value: float) -> None:
            self.timeout = value

        def recv(self, size: int) -> bytes:
            if not self.data:
                raise TimeoutError
            chunk = bytes(self.data[:size])
            del self.data[:size]
            return chunk

    reader = feed_module.FrameReader(ReplayConnection(data), clock=lambda: 0.0)
    replayed = []
    while replayed is not None:
        body = reader.read(deadline=1.0)
        if body is None:
            break
        replayed.append(body)
    assert replayed[0] == split_wire_frame(_records(out)[0].payload)
    assert replayed[1:] == bodies


def test_pack_from_real_captured_763_row_frame_stays_byte_identical(tmp_path: Path) -> None:
    fixture = Path(__file__).parent.parent / "fixtures" / "l2_trade_push_763.bin"
    meta = json.loads(fixture.with_suffix(".json").read_text())
    body = fixture.read_bytes()
    assert hashlib.sha256(body).hexdigest() == meta["body_sha256"]
    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.append(_observation(body, request_id=meta["request_id"],
                                  page_id=meta["binding"]["page_id"],
                                  code=meta["binding"]["code"],
                                  delivery="candidate_push",
                                  received_at=datetime(2026, 10, 19, 3, 14, tzinfo=UTC)))
    out = tmp_path / "pack"
    build_replay_pack(ReplayPackConfig(cache_root=cache.root, output_dir=out))
    records = _records(out)
    asserted = [split_wire_frame(record.payload) for record in records if record.kind == KIND_RX]
    assert asserted[1:] == [body]
    decoded = decode_trade_response(body, request_id=meta["request_id"],
                                    page_id=meta["binding"]["page_id"],
                                    code=meta["binding"]["code"], market=17,
                                    source_fd_ordinal=1)
    assert decoded["table"]["row_count"] == meta["row_count"] == 763


def test_pack_builder_is_deterministic_across_two_runs(tmp_path: Path) -> None:
    cache, _ = _make_cache(tmp_path / "cache", start=datetime(2026, 10, 16, tzinfo=UTC))
    first = tmp_path / "pack-a"
    second = tmp_path / "pack-b"
    build_replay_pack(ReplayPackConfig(cache_root=cache.root, output_dir=first))
    build_replay_pack(ReplayPackConfig(cache_root=cache.root, output_dir=second))
    first_records = _records(first)
    second_records = _records(second)
    assert [(record.kind, record.mono_us, record.payload) for record in first_records] == \
        [(record.kind, record.mono_us, record.payload) for record in second_records]
    first_manifest = read_manifest(first)
    second_manifest = read_manifest(second)
    assert [segment["sha256"] for segment in first_manifest["segments"]] == \
        [segment["sha256"] for segment in second_manifest["segments"]]
    for segment in first_manifest["segments"]:
        assert (first / segment["file"]).read_bytes() == (second / segment["file"]).read_bytes()


def test_pack_builder_rejects_empty_output_and_invalid_range(tmp_path: Path) -> None:
    cache, _ = _make_cache(tmp_path / "cache", batches=1,
                           start=datetime(2026, 10, 16, tzinfo=UTC))
    occupied = tmp_path / "occupied"
    occupied.mkdir()
    (occupied / "leftover").write_text("x")
    with pytest.raises(ReplayPackError, match="output_not_empty"):
        build_replay_pack(ReplayPackConfig(cache_root=cache.root, output_dir=occupied))
    with pytest.raises(ReplayPackError, match="invalid_range"):
        build_replay_pack(ReplayPackConfig(cache_root=cache.root,
                                           output_dir=tmp_path / "pack",
                                           start_sequence=5, end_sequence=2))


def test_verify_replay_pack_detects_tampered_segment(tmp_path: Path) -> None:
    cache, _ = _make_cache(tmp_path / "cache", start=datetime(2026, 10, 16, tzinfo=UTC))
    out = tmp_path / "pack"
    build_replay_pack(ReplayPackConfig(cache_root=cache.root, output_dir=out))
    manifest = read_manifest(out)
    assert manifest is not None
    segment = out / manifest["segments"][0]["file"]
    data = bytearray(segment.read_bytes())
    data[-12] ^= 0xFF
    segment.write_bytes(bytes(data))
    with pytest.raises(Exception):
        verify_replay_pack(out)


def test_pack_manifest_records_source_and_reader_contract(tmp_path: Path) -> None:
    cache, _ = _make_cache(tmp_path / "cache", start=datetime(2026, 10, 16, tzinfo=UTC))
    out = tmp_path / "pack"
    build_replay_pack(ReplayPackConfig(cache_root=cache.root, output_dir=out))
    manifest = read_manifest(out)
    assert manifest is not None
    assert manifest["reader"]["module"] == "rquant.l2.frame_tap"
    assert manifest["reader"]["callable"] == "iter_records"
    assert manifest["source"]["kind"] == "committed_l2_cache"
    assert manifest["source"]["epoch_ids"] == ["epoch-test"]
    assert manifest["tap_format"] == "rquant-l2-frame-tap-v1"
    assert manifest["timing"]["unit"] == "microseconds"
    assert manifest["timing"]["duration_us"] == 1_000_000
    assert all(segment["sha256"] for segment in manifest["segments"])
