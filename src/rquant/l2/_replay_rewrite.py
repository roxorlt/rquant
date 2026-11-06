"""Pure raw-response rewriting for L2 replay: re-label and re-title frames.

Only the observed market response envelope is touched. The body layout is the
``<HIIHII`` prefix (header_len, request_id, flags, page_id, payload_size,
command) followed by ``header_len - 20`` opaque envelope bytes and one Snappy
compressed table. Two profiles are accepted: the 32-byte seed/ack profile
(request_id != 0xFFFFFFFF) and the 26-byte candidate-push profile
(request_id == 0xFFFFFFFF, flags 0x1100).

Title rewriting only edits the ``tb1.0`` title's six-digit symbol prefix and
never scans the row data, so a table's rows cannot be mistaken for the title.
Snappy output is literal-only on purpose: the recompressed payload is accepted
by the existing bounded decoder without implementing copy instructions. Vendor
payloads do use copies, so a title rewrite can lengthen a real body; the caller
must re-wrap the FD wire prefix from the returned body rather than reuse the
old one. Request/page-only rewrites keep the payload byte-identical.
"""
from __future__ import annotations

import struct

from . import _codec

__all__ = ["frame_header", "rewrite_response", "snappy_literal_compress"]

_HEADER = struct.Struct("<HIIHII")
_HEADER_SIZE = _HEADER.size
_HEADER_LENGTHS = (26, 32)
_TABLE_PREFIX = b"tb1.0\0"
_MAX_U16 = 0xFFFF
_MAX_U32 = 0xFFFFFFFF
_ORDER_TITLE_SUFFIX = "-逐笔委托"


def _varint(value: int) -> bytes:
    if type(value) is not int or not 0 <= value <= _MAX_U32:
        raise ValueError("snappy length must be a uint32")
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def snappy_literal_compress(data: bytes) -> bytes:
    """Return a valid Snappy stream that uses literal instructions only."""
    if not isinstance(data, bytes):
        raise ValueError("snappy input must be bytes")
    size = len(data)
    if size > _codec.MAX_BYTES:
        raise ValueError("snappy input exceeds the decoder limit")
    out = bytearray(_varint(size))
    if size:
        length = size - 1
        if length < 60:
            out.append(length << 2)
        else:
            count = (length.bit_length() + 7) // 8
            out.append((59 + count) << 2)
            out += length.to_bytes(count, "little")
        out += data
    return bytes(out)


def frame_header(body: bytes) -> tuple[int, int, int, int, int, int]:
    """Return (header_len, request_id, flags, page_id, payload_size, command)."""
    if not isinstance(body, bytes) or len(body) < _HEADER_SIZE:
        raise ValueError("market response body is too short")
    header_len, request_id, flags, page_id, payload_size, command = _HEADER.unpack_from(body)
    if header_len not in _HEADER_LENGTHS:
        raise ValueError("unsupported market response header length")
    if payload_size != len(body) - header_len:
        raise ValueError("market response payload size does not match its body")
    return header_len, request_id, flags, page_id, payload_size, command


def _code_digits(value: str, name: str) -> None:
    if (not isinstance(value, str) or len(value) != 6
            or any(character not in "0123456789" for character in value)):
        raise ValueError(f"{name} must be exactly six ASCII digits")


def _retitle_table(data: bytes, old_code: str, new_code: str) -> bytes:
    """Rewrite only the tb1.0 title symbol; every other table byte is kept."""
    if len(data) < 24 or data[:6] != _TABLE_PREFIX:
        raise ValueError("unsupported market table version")
    extended = int.from_bytes(data[18:20], "little")
    count = int.from_bytes(data[22:24], "little")
    if not 1 <= count <= 32 or extended > len(data):
        raise ValueError("market table header is out of bounds")
    cursor = 24 + 4 * count
    if cursor + 2 > extended:
        raise ValueError("market table title length is out of bounds")
    title_bytes = int.from_bytes(data[cursor:cursor + 2], "little") * 2
    start = cursor + 2
    if start + title_bytes > extended:
        raise ValueError("market table title crosses the header")
    old_title = (old_code + _ORDER_TITLE_SUFFIX).encode("utf-16le")
    if data[start:start + title_bytes] != old_title:
        raise ValueError("market table title does not match old_code")
    new_title = (new_code + _ORDER_TITLE_SUFFIX).encode("utf-16le")
    if len(new_title) != len(old_title):
        raise ValueError("market table title replacement changes its size")
    return data[:start] + new_title + data[start + title_bytes:]


def _restamped(value: int | None, recorded: int, *, maximum: int, name: str) -> int:
    if value is None:
        return recorded
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} is outside its observed wire width")
    return value


def rewrite_response(body: bytes, *, request_id: int | None = None, page_id: int | None = None,
                     old_code: str | None = None, new_code: str | None = None) -> bytes:
    """Return a rewritten copy of one market response body; the input is untouched."""
    header_len, actual_request_id, flags, actual_page_id, _payload_size, command = (
        frame_header(body))
    if (old_code is None) != (new_code is None):
        raise ValueError("old_code and new_code must be supplied together")
    new_request_id = _restamped(request_id, actual_request_id, maximum=_MAX_U32, name="request_id")
    new_page_id = _restamped(page_id, actual_page_id, maximum=_MAX_U16, name="page_id")
    if old_code is not None and new_code is not None:
        _code_digits(old_code, "old_code")
        _code_digits(new_code, "new_code")
        table = _codec.snappy_decompress(body[header_len:])
        payload = snappy_literal_compress(_retitle_table(table, old_code, new_code))
    else:
        payload = body[header_len:]
    return (_HEADER.pack(header_len, new_request_id, flags, new_page_id, len(payload), command)
            + body[_HEADER_SIZE:header_len] + payload)
