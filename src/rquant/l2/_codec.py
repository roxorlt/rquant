"""Reviewed market-only codec functions; no transport or file access."""
from __future__ import annotations
import hashlib
import struct
from typing import Any
from ._request import (build_trade_query, encode_payload, parse_market_ini, parse_request_body,
                       parse_trade_market_ini, parse_trade_request_body, decode_payload,
                       _parse_ini_route, _validate_order_parameters)
from ._table import parse_table
from .symbols import market_for_code

ALLOWED_ENDPOINT = ("122.9.43.156", 9528)
MAX_BYTES = 1 << 20
MAX_SECONDS = 15.0
_MARKER = b"\xfd" * 4

def _wrap_frame(body: bytes) -> bytes:
    if not 0 < len(body) <= MAX_BYTES:
        raise ValueError("outgoing body exceeds the bounded experiment")
    return _MARKER + f"{len(body):08x}".encode("ascii") + b"\x00" + body

def build_experimental_order_frame(template_body: bytes, query: bytes, *, request_id: int) -> bytes:
    """Use experimental_template_header; change only offsets 2, 12 and 20.

    Both the captured template and replacement INI must parse as order 4027.
    The template's unknown bytes are copied verbatim, not validated or invented.
    """
    if type(request_id) is not int or not 1 <= request_id <= 0xFFFFFFFF:
        raise ValueError("request_id must be a positive uint32")
    if not isinstance(template_body, bytes) or len(template_body) > MAX_BYTES:
        raise ValueError("invalid or oversized market template")
    if not isinstance(query, bytes) or len(query) > MAX_BYTES:
        raise ValueError("invalid or oversized order query")
    template = parse_request_body(template_body)
    market = parse_market_ini(query)
    if template["kind"] != "order_query" or market["kind"] != "order_query":
        raise ValueError("only confirmed order 4027 queries are allowed")
    if template["header_length"] != 77:
        raise ValueError("only the observed 77-byte market header is allowed")
    encoded = encode_payload(query)
    header = bytearray(template["header"])
    struct.pack_into("<I", header, 2, request_id)
    struct.pack_into("<I", header, 12, len(encoded))
    struct.pack_into("<I", header, 20, len(query))
    return _wrap_frame(bytes(header) + encoded)

def build_common_order_frame(template_body: bytes, query: bytes, *, request_id: int) -> bytes:
    """Keep the validated4027 query, coupling only its outer window to2357."""
    legacy = build_experimental_order_frame(template_body, query, request_id=request_id)
    source = parse_request_body(legacy[13:])
    parameters = source['parameters']
    if (str(market_for_code(parameters['stockcode'])) != parameters['marketcode']
            or any(int(p) > 65535 for p in source['frame']['pageList'].split(','))):
        raise ValueError('common order is outside validated symbol/page scope')
    plain = source['plaintext'].replace(b'[frame]\r\nid=2267\r\n', b'[frame]\r\nid=2357\r\n', 1)
    encoded = encode_payload(plain)
    header = bytearray(source['header'])
    struct.pack_into('<III', header, 12, len(encoded), 2357, len(plain))
    decoded = decode_payload(encoded, expected_length=int.from_bytes(header[20:24], 'little'))
    route = _parse_ini_route(decoded, frame_id=2357)
    _validate_order_parameters(route['parameters'])
    if (route['parameters'] != parameters or route['page_id'] != source['page_id']
            or route['frame'] != {**source['frame'], 'id': '2357'}
            or header[:12] != source['header'][:12] or header[24:] != source['header'][24:]):
        raise ValueError('common order changed unapproved fields')
    return _wrap_frame(bytes(header) + encoded)

def _validate_bootstrap(body: bytes) -> None:
    """Check only the observed opaque bootstrap profile; never decode it."""
    if not isinstance(body, bytes) or not 77 < len(body) <= MAX_BYTES:
        raise ValueError("invalid or oversized bootstrap body")
    header_length, request_id, flags, _unknown, encoded_length, command, plaintext_length = struct.unpack_from(
        "<HIIHIII", body
    )
    if (header_length, request_id, flags, command, plaintext_length) != (77, 0, 0x70000, 0, 0):
        raise ValueError("bootstrap is outside the observed header profile")
    if encoded_length != len(body) - header_length:
        raise ValueError("bootstrap encoded length does not match its body")

def snappy_decompress(source: bytes, *, max_output_bytes: int = MAX_BYTES) -> bytes:
    """Decode raw Snappy, bounding declared size, every instruction and copies."""
    if not isinstance(source, bytes) or len(source) > MAX_BYTES:
        raise ValueError("invalid Snappy input")
    if type(max_output_bytes) is not int or not 0 <= max_output_bytes <= MAX_BYTES:
        raise ValueError("invalid Snappy output limit")
    position = declared = 0
    for shift in range(0, 35, 7):
        if position >= len(source):
            raise ValueError("truncated Snappy length")
        byte = source[position]; position += 1
        if shift == 28 and byte & 0xF0:
            raise ValueError("overflowing Snappy length")
        declared |= (byte & 127) << shift
        if byte < 128:
            break
    else:
        raise ValueError("overflowing Snappy length")
    if declared > max_output_bytes:
        raise ValueError("Snappy output exceeds limit")
    output = bytearray()
    while position < len(source):
        tag = source[position]; position += 1
        kind = tag & 3
        if kind == 0:
            size = tag >> 2
            if size < 60:
                length = size + 1
            else:
                count = size - 59
                if position + count > len(source):
                    raise ValueError("truncated Snappy literal length")
                length = int.from_bytes(source[position:position + count], "little") + 1
                position += count
            if length > len(source) - position or length > declared - len(output):
                raise ValueError("invalid Snappy literal length")
            output.extend(source[position:position + length]); position += length
        else:
            count = {1: 1, 2: 2, 3: 4}[kind]
            if position + count > len(source):
                raise ValueError("truncated Snappy copy")
            offset = ((tag >> 5) << 8) + source[position] if kind == 1 else int.from_bytes(source[position:position + count], "little")
            position += count
            length = ((tag >> 2) & 7) + 4 if kind == 1 else (tag >> 2) + 1
            if offset == 0 or offset > len(output) or length > declared - len(output):
                raise ValueError("invalid Snappy copy bounds")
            for _ in range(length):
                output.append(output[-offset])
    if len(output) != declared:
        raise ValueError("Snappy output underfills declared length")
    return bytes(output)

def _response_payload(body: bytes, *, request_id: int, page_id: int, command: int,
                      allowed_flags: tuple[int, ...]) -> bytes:
    if not isinstance(body, bytes) or not 26 <= len(body) <= MAX_BYTES:
        raise ValueError("invalid market response size")
    header, actual_id, flags, actual_page, payload_size, actual_command = struct.unpack_from("<HIIHII", body)
    profile = ((header == 32 and actual_id != 0xFFFFFFFF)
               or (header, actual_id, flags) == (26, 0xFFFFFFFF, 0x1100))
    if (not profile or (actual_id, actual_page, actual_command) != (request_id, page_id, command)
            or flags not in allowed_flags):
        raise ValueError("market response association or format mismatch")
    if payload_size != len(body) - header:
        raise ValueError("invalid market response length")
    return snappy_decompress(body[header:])

def _table_metadata(data: bytes, *, version: str, max_rows: int) -> dict[str, Any]:
    """Bound dimensions before the shared table parser can allocate row output."""
    if len(data) < 24 or data[:6] != version.encode("ascii") + b"\0":
        raise ValueError("unsupported market table version")
    rows, _unknown10, _unknown14, ext, stride, count = struct.unpack_from("<iiiHHH", data, 6)
    if (not 0 <= rows <= max_rows or not 1 <= count <= 32 or stride == 0
            or rows * stride > MAX_BYTES or not 24 + count * 4 <= ext <= len(data)):
        raise ValueError("market table dimensions exceed bounds")
    columns = []
    for index in range(count):
        column_flags, width, extra = struct.unpack_from("<HBB", data, 24 + 4 * index)
        columns.append({"field_id": column_flags & 0x8FFF, "flags_hex": hex(column_flags),
                        "width": width, "extra": extra, "name": ""})
    cursor = 24 + count * 4
    def text() -> str:
        nonlocal cursor
        if cursor + 2 > ext:
            raise ValueError("truncated table text length")
        length = int.from_bytes(data[cursor:cursor + 2], "little") * 2
        cursor += 2
        if cursor + length > ext:
            raise ValueError("table text crosses header")
        try:
            value = data[cursor:cursor + length].decode("utf-16le")
        except UnicodeDecodeError:
            raise ValueError("invalid table UTF16") from None
        cursor += length
        return value
    title = text()
    for column in columns:
        column["name"] = text()
    return {
        "sha256": hashlib.sha256(data).hexdigest(), "decoded_bytes": len(data),
        "table_version": version, "row_count": rows, "row_width": stride,
        "col_count": count, "extended_header_length": ext, "data_bytes_after_header": len(data) - ext,
        "columns": columns, "title": title, "codec": "snappy", "selected_frame": 0,
    }

def _response_result(body: bytes, data: bytes, metadata: dict[str, Any], *,
                     request_id: int, page_id: int, source_fd_ordinal: int) -> dict[str, Any]:
    try:
        parsed = parse_table(data, metadata)
    except Exception:
        raise ValueError("invalid market table layout") from None
    parsed.pop("selected_frame", None); parsed.pop("selected_frames", None)
    return {"table": parsed, "raw_table": data, "response_body": body,
            "response_sha256": hashlib.sha256(body).hexdigest(), "source_fd_ordinal": source_fd_ordinal,
            "request_id": request_id, "page_id": page_id, "status": "partial", "coverage": "unknown_day_coverage"}

def decode_order_response(body: bytes, *, request_id: int, page_id: int, code: str,
                          source_fd_ordinal: int, max_rows: int = 20) -> dict[str, Any]:
    """Validate the known market envelope and existing tb1 order table."""
    if type(max_rows) is not int or not 1 <= max_rows <= 20:
        raise ValueError("invalid order row limit")
    command = int.from_bytes(body[16:20], 'little') if isinstance(body, bytes) and len(body) >= 20 else 0
    if command not in (2267, 2357):
        raise ValueError('unsupported order frame command')
    data = _response_payload(body, request_id=request_id, page_id=page_id,
                             command=command, allowed_flags=(0x1100,))
    metadata = _table_metadata(data, version="tb1.0", max_rows=500 if request_id == 0xFFFFFFFF else max_rows)
    if metadata["title"] != f"{code}-逐笔委托":
        raise ValueError("order table title mismatch")
    return _response_result(body, data, metadata, request_id=request_id, page_id=page_id,
                            source_fd_ordinal=source_fd_ordinal)

def decode_trade_response(body: bytes, *, request_id: int, page_id: int, code: str,
                          market: int, source_fd_ordinal: int) -> dict[str, Any]:
    """Decode observed vendor4013 tables, retaining unverified meaning.

    An empty table title provides no independent symbol. Association is solely
    the issued request or registered push page/command; no exchange-tick, unit
    or order-ID claim follows.
    """
    build_trade_query(code=code, market=market, page_id=page_id)
    if (type(request_id) is not int or not 1 <= request_id <= 0xFFFFFFFF
            or type(source_fd_ordinal) is not int or source_fd_ordinal < 1):
        raise ValueError("invalid trade response identity")
    data = _response_payload(body, request_id=request_id, page_id=page_id,
                             command=2357, allowed_flags=(0x1110, 0x1100, 0x1130))
    version = "tb1.0" if request_id == 0xFFFFFFFF and data[:6] == b"tb1.0\0" else "tb3.0"
    metadata = _table_metadata(data, version=version, max_rows=4096 if request_id == 0xFFFFFFFF else 500)
    expected_columns = [(0x2001, 4), (0x38, 30), (0x100A, 4), (0x100D, 4),
                        (0x104A, 4), (0x104B, 4), (0xA610, 4), (0x1012, 4)]
    actual_columns = [(int(c["flags_hex"], 16), c["width"]) for c in metadata["columns"]]
    if (metadata["title"] != "" or metadata["row_width"] != 66
            or actual_columns != expected_columns
            or any(c["extra"] != 165 for c in metadata["columns"])):
        raise ValueError("unsupported trade table schema")
    result = _response_result(body, data, metadata, request_id=request_id, page_id=page_id,
                              source_fd_ordinal=source_fd_ordinal)
    result.update(kind="vendor4013", granularity="granularity_unverified", code=code,
                  market=market, symbol_source=("registered_page_association_only" if request_id == 0xFFFFFFFF
                                                else "request_association_only"))
    return result

def build_trade_frame(template_body: bytes, query: bytes, *, request_id: int) -> bytes:
    """Copy only the observed template; change known ID and length fields."""
    if type(request_id) is not int or not 1 <= request_id <= 0xFFFFFFFF:
        raise ValueError("invalid request ID")
    if not isinstance(query, bytes) or len(query) > MAX_BYTES:
        raise ValueError("invalid trade query size")
    template = parse_trade_request_body(template_body)
    parse_trade_market_ini(query)
    encoded = encode_payload(query)
    header = bytearray(template["header"])
    struct.pack_into("<I", header, 2, request_id)
    struct.pack_into("<I", header, 12, len(encoded))
    struct.pack_into("<I", header, 20, len(query))
    return _wrap_frame(bytes(header) + encoded)
