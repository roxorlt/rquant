"""Synthetic, offline checks of the raw response rewriting helpers."""
from __future__ import annotations

import random
import struct

import pytest

from rquant.l2 import _codec
from tests.unit.test_l2_codec import frame, literal, module, response

_LITERAL_SIZES = (0, 1, 60, 61, 300, 5000)
_ORDER_TITLE = "-逐笔委托"


def _seed_body(*, request_id: int = 7, page_id: int = 10118, code: str = "603258") -> bytes:
    return response(trade=False, request_id=request_id, page_id=page_id, code=code)


def _push_body(*, page_id: int = 10117, code: str = "603258") -> bytes:
    payload = _seed_body(request_id=3, page_id=page_id, code=code)[32:]
    return (struct.pack("<HIIHIII", 26, 0xFFFFFFFF, 0x1100, page_id, len(payload), 2267, 0)
            + b"\0\0" + payload)


def _rebuilt_seed(table: bytes, *, request_id: int = 7, page_id: int = 10118) -> bytes:
    payload = literal(table)
    return (struct.pack("<HIIHIII", 32, request_id, 0x1100, page_id, len(payload), 2267, 0)
            + b"\0" * 8 + payload)


def _title_span(raw: bytes) -> tuple[int, int]:
    count = int.from_bytes(raw[22:24], "little")
    cursor = 24 + 4 * count
    size = int.from_bytes(raw[cursor:cursor + 2], "little") * 2
    return cursor + 2, cursor + 2 + size


@pytest.mark.parametrize("size", _LITERAL_SIZES)
def test_literal_compress_round_trips_and_matches_reference(size: int) -> None:
    rewrite = module("_replay_rewrite")
    data = bytes((index * 37 + 11) & 0xFF for index in range(size))
    encoded = rewrite.snappy_literal_compress(data)
    assert encoded == literal(data)
    assert _codec.snappy_decompress(encoded) == data


def test_literal_compress_round_trips_random_bytes() -> None:
    rewrite = module("_replay_rewrite")
    data = random.Random(20261016).randbytes(5000)
    assert _codec.snappy_decompress(rewrite.snappy_literal_compress(data)) == data


def test_empty_literal_stream_matches_the_existing_decoder() -> None:
    rewrite = module("_replay_rewrite")
    encoded = rewrite.snappy_literal_compress(b"")
    assert encoded == literal(b"") == b"\x00"
    assert _codec.snappy_decompress(encoded) == b""


def test_literal_compress_rejects_non_bytes_and_oversized_input() -> None:
    rewrite = module("_replay_rewrite")
    for value in (bytearray(b"x"), memoryview(b"x"), "x"):
        with pytest.raises(ValueError):
            rewrite.snappy_literal_compress(value)
    with pytest.raises(ValueError):
        rewrite.snappy_literal_compress(b"\0" * (_codec.MAX_BYTES + 1))


def test_frame_header_reads_both_observed_profiles() -> None:
    rewrite = module("_replay_rewrite")
    body = _seed_body()
    assert rewrite.frame_header(body) == (32, 7, 0x1100, 10118, len(body) - 32, 2267)
    push = _push_body()
    assert rewrite.frame_header(push) == (26, 0xFFFFFFFF, 0x1100, 10117, len(push) - 26, 2267)


@pytest.mark.parametrize("fault", ["short", "header_length", "payload_size"])
def test_frame_header_rejects_unobserved_envelopes(fault: str) -> None:
    rewrite = module("_replay_rewrite")
    body = bytearray(_seed_body())
    if fault == "short":
        body = body[:19]
    elif fault == "header_length":
        struct.pack_into("<H", body, 0, 30)
    else:
        struct.pack_into("<I", body, 12, len(body))
    with pytest.raises(ValueError):
        rewrite.frame_header(bytes(body))
    with pytest.raises(ValueError):
        rewrite.rewrite_response(bytes(body))


def test_rewrite_without_targets_is_byte_identical() -> None:
    rewrite = module("_replay_rewrite")
    body = _seed_body()
    assert rewrite.rewrite_response(body) == body


def test_retitle_rewrites_only_the_tb1_title_and_still_decodes() -> None:
    rewrite = module("_replay_rewrite")
    body = _seed_body()
    saved = body
    before = _codec.decode_order_response(body, request_id=7, page_id=10118, code="603258",
                                          source_fd_ordinal=1)
    rewritten = rewrite.rewrite_response(body, old_code="603258", new_code="690017")
    assert body == saved
    assert len(rewritten) == len(body)
    assert rewrite.frame_header(rewritten) == (32, 7, 0x1100, 10118, len(rewritten) - 32, 2267)
    after = _codec.decode_order_response(rewritten, request_id=7, page_id=10118, code="690017",
                                         source_fd_ordinal=1)
    assert after["table"]["title"] == "690017" + _ORDER_TITLE
    assert after["table"]["rows"] == before["table"]["rows"]
    assert after["table"]["row_data_sha256"] == before["table"]["row_data_sha256"]
    raw_before = _codec.snappy_decompress(body[32:])
    raw_after = _codec.snappy_decompress(rewritten[32:])
    start, end = _title_span(raw_before)
    assert raw_after[:start] == raw_before[:start]
    assert raw_after[end:] == raw_before[end:]
    assert raw_after[start:end] == ("690017" + _ORDER_TITLE).encode("utf-16le")


def test_retitle_preserves_the_opaque_envelope_bytes() -> None:
    rewrite = module("_replay_rewrite")
    body = bytearray(_seed_body())
    body[20:32] = b"\x01\x02\x03\x04\x05\xff\x11\x22\x0c\x00\x00\x00"
    body = bytes(body)
    rewritten = rewrite.rewrite_response(body, old_code="603258", new_code="690017")
    assert rewritten[20:32] == body[20:32]
    after = _codec.decode_order_response(rewritten, request_id=7, page_id=10118, code="690017",
                                         source_fd_ordinal=1)
    assert after["table"]["title"] == "690017" + _ORDER_TITLE


def test_relabel_keeps_payload_bytes_and_decodes() -> None:
    rewrite = module("_replay_rewrite")
    body = _seed_body()
    rewritten = rewrite.rewrite_response(body, request_id=424242, page_id=10119)
    assert len(rewritten) == len(body)
    assert rewrite.frame_header(rewritten) == (32, 424242, 0x1100, 10119, len(body) - 32, 2267)
    assert rewritten[32:] == body[32:]
    decoded = _codec.decode_order_response(rewritten, request_id=424242, page_id=10119,
                                           code="603258", source_fd_ordinal=1)
    assert decoded["table"]["title"] == "603258" + _ORDER_TITLE


def test_candidate_push_profile_can_be_retitled_and_decoded() -> None:
    rewrite = module("_replay_rewrite")
    push = _push_body()
    assert rewrite.frame_header(push) == (26, 0xFFFFFFFF, 0x1100, 10117, len(push) - 26, 2267)
    before = _codec.decode_order_response(push, request_id=0xFFFFFFFF, page_id=10117,
                                          code="603258", source_fd_ordinal=507)
    rewritten = rewrite.rewrite_response(push, request_id=0xFFFFFFFF, page_id=10117,
                                         old_code="603258", new_code="690017")
    assert len(rewritten) == len(push)
    assert rewrite.frame_header(rewritten) == (26, 0xFFFFFFFF, 0x1100, 10117, len(push) - 26, 2267)
    after = _codec.decode_order_response(rewritten, request_id=0xFFFFFFFF, page_id=10117,
                                         code="690017", source_fd_ordinal=507)
    assert after["table"]["title"] == "690017" + _ORDER_TITLE
    assert after["table"]["rows"] == before["table"]["rows"]


def test_rewritten_body_rewraps_into_a_fd_wire_frame() -> None:
    rewrite = module("_replay_rewrite")
    body = _seed_body()
    rewritten = rewrite.rewrite_response(body, old_code="603258", new_code="690017")
    wire = frame(rewritten)
    assert wire[:4] == b"\xfd" * 4
    assert int(wire[4:12], 16) == len(rewritten)
    assert wire[13:] == rewritten


@pytest.mark.parametrize("kwargs", [
    {"old_code": "603258"},
    {"new_code": "690017"},
    {"old_code": "603258", "new_code": "69001"},
    {"old_code": "603258", "new_code": "6900170"},
    {"old_code": "603258", "new_code": "69A017"},
    {"old_code": "603258", "new_code": "６９００１７"},
    {"old_code": 603258, "new_code": "690017"},
    {"old_code": "000001", "new_code": "690017"},
])
def test_rewrite_rejects_incomplete_or_invalid_code_pairs(kwargs: dict) -> None:
    rewrite = module("_replay_rewrite")
    with pytest.raises(ValueError):
        rewrite.rewrite_response(_seed_body(), **kwargs)


@pytest.mark.parametrize("kwargs", [
    {"request_id": 0},
    {"request_id": -1},
    {"request_id": 0x1_0000_0000},
    {"request_id": True},
    {"page_id": 0},
    {"page_id": -1},
    {"page_id": 0x1_0000},
])
def test_rewrite_rejects_out_of_range_request_and_page_ids(kwargs: dict) -> None:
    rewrite = module("_replay_rewrite")
    with pytest.raises(ValueError):
        rewrite.rewrite_response(_seed_body(), **kwargs)


@pytest.mark.parametrize("fault", ["version", "column_count", "extended", "title_prefix",
                                   "title_length", "title_content"])
def test_retitle_rejects_malformed_tb1_tables(fault: str) -> None:
    rewrite = module("_replay_rewrite")
    raw = bytearray(_codec.snappy_decompress(_seed_body()[32:]))
    if fault == "version":
        raw[:6] = b"tb2.0\0"
    elif fault == "column_count":
        struct.pack_into("<H", raw, 22, 0)
    elif fault == "extended":
        struct.pack_into("<H", raw, 18, len(raw) + 1)
    elif fault == "title_prefix":
        struct.pack_into("<H", raw, 18, 24 + 4 * 6 + 1)
    elif fault == "title_length":
        struct.pack_into("<H", raw, 24 + 4 * 6, 0xFFFF)
    else:
        start, _ = _title_span(bytes(raw))
        raw[start:start + 2] = b"A\0"
    with pytest.raises(ValueError):
        rewrite.rewrite_response(_rebuilt_seed(bytes(raw)), old_code="603258", new_code="690017")
