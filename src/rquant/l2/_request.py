"""Offline codec for the observed iPhone Tonghuashun market requests.

The alphabet matches 27 captured, associated market requests byte for byte;
54 of its 64 characters occur in those captures. The other 10 positions are
inferred, not independently exercised by device traffic. This module neither
authenticates nor sends requests, and never constructs a new binary header.
"""

from __future__ import annotations

import base64
import binascii
import re
import struct
from typing import Any


IPHONE64_ALPHABET = b"aCcMeTKhxnwzmoPbsG4EvU8gyd02B3q6fIVWXYZjApRrDtuHkiLlN1O9F5S7JQ+/"
_STANDARD64 = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
_TO_STANDARD = bytes.maketrans(IPHONE64_ALPHABET, _STANDARD64)
_FROM_STANDARD = bytes.maketrans(_STANDARD64, IPHONE64_ALPHABET)
_FRAME_KEYS = {"id", "pageList", "reqPage", "reqPageCount"}
_ORDER_KEYS = {"id", "marketcode", "rowcount", "stockcode", "reqtype", "endtime", "orderid"}
_DETAIL_KEYS = {"id", "nopush", "isVolumeFloat", "stockcode", "rowcount", "marketcode", "endtime", "unixtime"}
_TRADE_KEYS = {"id", "ispush", "isVolumeFloat", "rowcount", "stockcode", "bishu", "marketcode", "currentpage", "endtime"}
_TRADE_MARKETS = {"603258": "17", "300006": "33"}


def encode_payload(plaintext: bytes) -> bytes:
    """Encode bytes with the capture-derived iPhone alphabet and standard padding."""
    if not isinstance(plaintext, bytes):
        raise TypeError("plaintext must be bytes")
    return base64.b64encode(plaintext).translate(_FROM_STANDARD)


def decode_payload(encoded: bytes | str, *, expected_length: int | None = None) -> bytes:
    """Reject invalid alphabet, whitespace, padding, pad bits and length mismatches."""
    if expected_length is not None and (type(expected_length) is not int or expected_length < 0):
        raise ValueError("expected_length must be a nonnegative integer")
    if isinstance(encoded, str):
        try:
            encoded = encoded.encode("ascii")
        except UnicodeEncodeError as error:
            raise ValueError("encoded payload must be ASCII") from error
    if not isinstance(encoded, bytes):
        raise TypeError("encoded payload must be bytes or ASCII text")
    try:
        plaintext = base64.b64decode(encoded.translate(_TO_STANDARD), validate=True)
    except (binascii.Error, ValueError) as error:
        raise ValueError("invalid iPhone Base64 payload") from error
    if encode_payload(plaintext) != encoded:
        raise ValueError("noncanonical iPhone Base64 padding or pad bits")
    if expected_length is not None and len(plaintext) != expected_length:
        raise ValueError("decoded payload length does not match header")
    return plaintext


def _decimal(value: str, label: str, *, positive: bool = False) -> int:
    if re.fullmatch(r"0|[1-9][0-9]*", value) is None:
        raise ValueError(f"{label} must be a canonical unsigned decimal")
    number = int(value)
    if positive and number == 0:
        raise ValueError(f"{label} must be positive")
    return number


def _validate_order_parameters(parameters: dict[str, str]) -> None:
    if set(parameters) != _ORDER_KEYS or parameters["id"] != "4027":
        raise ValueError("unsupported order query fields or command")
    if re.fullmatch(r"[0-9]{6}", parameters["stockcode"]) is None:
        raise ValueError("stockcode must contain six ASCII digits")
    if parameters["marketcode"] not in {"17", "33"}:
        raise ValueError("only the observed markets 17 and 33 are supported")
    if parameters["reqtype"] not in {"buy", "sell"}:
        raise ValueError("reqtype must be buy or sell")
    if not 1 <= _decimal(parameters["rowcount"], "rowcount") <= 20:
        raise ValueError("rowcount must be in 1..20")
    endtime = _decimal(parameters["endtime"], "endtime")
    orderid = _decimal(parameters["orderid"], "orderid")
    if (endtime == 0) != (orderid == 0):
        raise ValueError("endtime and orderid must both be zero or both be positive")


def _parse_ini_route(plaintext: bytes, *, frame_id: int) -> dict[str, Any]:
    """Read the common strict INI envelope without expanding public command scope."""
    if not isinstance(plaintext, bytes):
        raise TypeError("plaintext must be bytes")
    try:
        text = plaintext.decode("ascii")
    except UnicodeDecodeError as error:
        raise ValueError("market request must be ASCII") from error
    if not text.endswith("\r\n") or any(
        not 32 <= ord(char) <= 126 for char in text.replace("\r\n", "")
    ):
        raise ValueError("market request must use printable ASCII and CRLF")
    sections: dict[str, dict[str, str]] = {}
    current: dict[str, str] | None = None
    for line in text.split("\r\n")[:-1]:
        if re.fullmatch(r"\[(?:frame|[1-9][0-9]*)\]", line):
            section = line[1:-1]
            if section in sections:
                raise ValueError("duplicate request section")
            current = {}
            sections[section] = current
        elif "=" in line and current is not None:
            key, value = line.split("=", 1)
            if re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", key) is None or not value or key in current:
                raise ValueError("invalid or duplicate request parameter")
            current[key] = value
        else:
            raise ValueError("invalid request INI line")
    frame = sections.get("frame", {})
    if set(frame) != _FRAME_KEYS or frame["id"] != str(frame_id) or frame["reqPageCount"] != "1":
        raise ValueError("unsupported frame command or requested page count")
    page_id = frame["reqPage"]
    _decimal(page_id, "reqPage", positive=True)
    page_list = frame["pageList"].split(",")
    for page in page_list:
        _decimal(page, "pageList entry", positive=True)
    if len(set(page_list)) != len(page_list) or page_id not in page_list:
        raise ValueError("pageList must contain the unique requested page")
    if set(sections) != {"frame", page_id}:
        raise ValueError("requested page must have exactly one matching section")
    return {"page_id": page_id, "frame": frame, "parameters": sections[page_id]}


def parse_market_ini(plaintext: bytes) -> dict[str, Any]:
    """Parse only observed frame2267 order4027/ordinary detail1235 requests.

    pageList is the UI active-page set, not a historical page count. Stock
    codes remain text. Neither ordinary detail nor4013 is an exchange-tick claim.
    """
    market = _parse_ini_route(plaintext, frame_id=2267)
    parameters = market["parameters"]
    if parameters.get("id") == "4027":
        _validate_order_parameters(parameters)
        kind = "order_query"
    elif parameters.get("id") == "1235":
        if set(parameters) != _DETAIL_KEYS:
            raise ValueError("unsupported detail query fields")
        if re.fullmatch(r"[0-9]{6}", parameters["stockcode"]) is None:
            raise ValueError("stockcode must contain six ASCII digits")
        if parameters["marketcode"] not in {"17", "33"}:
            raise ValueError("unsupported detail market")
        if any(parameters[key] != value for key, value in (
            ("rowcount", "30"), ("nopush", "0"), ("isVolumeFloat", "1"), ("unixtime", "1")
        )):
            raise ValueError("unobserved detail query options")
        _decimal(parameters["endtime"], "endtime")
        kind = "detail"
    else:
        raise ValueError("unsupported market request command")
    return {"kind": kind, **market}


def _read_request_body(body: bytes, *, frame_id: int) -> dict[str, Any]:
    if not isinstance(body, bytes):
        raise TypeError("request body must be bytes")
    if len(body) < 24:
        raise ValueError("request header is truncated")
    header_length, request_id, flags, _unknown, encoded_length, actual_frame_id, plaintext_length = struct.unpack_from(
        "<HIIHIII", body
    )
    if not 24 <= header_length <= len(body):
        raise ValueError("invalid request header length")
    if flags != 0x01010000 or actual_frame_id != frame_id:
        raise ValueError("unsupported request header flags or frame command")
    encoded_payload = body[header_length:]
    if len(encoded_payload) != encoded_length:
        raise ValueError("encoded payload length does not match header")
    plaintext = decode_payload(encoded_payload, expected_length=plaintext_length)
    return {
        "header_length": header_length,
        "request_id": request_id,
        "flags": flags,
        "header": body[:header_length],
        "header_extension": body[24:header_length],
        "header_extension_verified": False,
        "encoded_payload": encoded_payload,
        "plaintext": plaintext,
    }


def parse_request_body(body: bytes) -> dict[str, Any]:
    """Read only the existing2267 request scope; preserve unknown header bytes.

    Unknown extension bytes are NOT verified as a reusable session identity.
    The new2357/4013 profile is deliberately parsed by a separate function.
    """
    parsed = _read_request_body(body, frame_id=2267)
    return {**parse_market_ini(parsed["plaintext"]), **parsed}


def _validate_trade_parameters(parameters: dict[str, str]) -> None:
    if set(parameters) != _TRADE_KEYS:
        raise ValueError("unsupported trade query fields")
    for key, value in (("id", "4013"), ("ispush", "1"), ("isVolumeFloat", "1"),
                       ("rowcount", "500"), ("bishu", "2500"), ("endtime", "0")):
        if parameters[key] != value:
            raise ValueError("unobserved initial4013 query options")
    if _TRADE_MARKETS.get(parameters["stockcode"]) != parameters["marketcode"]:
        raise ValueError("symbol outside initial trade validation scope")
    if not 1 <= _decimal(parameters["currentpage"], "currentpage") <= 3:
        raise ValueError("experimental currentpage must be in1..3")


def parse_trade_market_ini(plaintext: bytes) -> dict[str, Any]:
    """Parse only initial2357/4013; page2/3 remain experimental, not history proof."""
    market = _parse_ini_route(plaintext, frame_id=2357)
    if any(int(page) > 65535 for page in market["frame"]["pageList"].split(",")):
        raise ValueError("trade page ID must fit uint16")
    _validate_trade_parameters(market["parameters"])
    return {"kind": "vendor4013", **market}


def parse_trade_request_body(body: bytes) -> dict[str, Any]:
    """Read the captured h77 initial4013 template; never accept onlymatch/control."""
    if not isinstance(body, bytes) or len(body) > 1 << 20:
        raise ValueError("invalid trade template size")
    parsed = _read_request_body(body, frame_id=2357)
    if parsed["header_length"] != 77:
        raise ValueError("trade template must use observed77-byte header")
    return {**parse_trade_market_ini(parsed["plaintext"]), **parsed}


def build_trade_query(*, code: str, market: int, page_id: int, currentpage: int = 1) -> bytes:
    """Build the bounded initial4013 shape; no dates, onlymatch or push controls."""
    if not isinstance(code, str) or type(market) is not int:
        raise ValueError("trade symbol and market must retain their original types")
    if type(page_id) is not int or not 1 <= page_id <= 65535:
        raise ValueError("trade page ID must fit uint16")
    if type(currentpage) is not int:
        raise ValueError("currentpage must be an integer")
    parameters = {"id": "4013", "ispush": "1", "isVolumeFloat": "1", "rowcount": "500",
                  "stockcode": code, "bishu": "2500", "marketcode": str(market),
                  "currentpage": str(currentpage), "endtime": "0"}
    _validate_trade_parameters(parameters)
    lines = ["[frame]", "id=2357", f"pageList={page_id}", f"reqPage={page_id}",
             "reqPageCount=1", f"[{page_id}]"]
    lines.extend(f"{key}={value}" for key, value in parameters.items())
    return ("\r\n".join(lines) + "\r\n").encode("ascii")


def build_order_query(
    *, code: str, market: int, side: str, rowcount: int,
    endtime: int, orderid: int, page_id: int,
) -> bytes:
    """Build only observed order 4027 INI, without a session or binary header.

    The cursor is (endtime, orderid); (0, 0) requests the initial page. Neither
    cross-day history nor live subscription behavior is claimed by this builder.
    """
    if not isinstance(code, str) or re.fullmatch(r"[0-9]{6}", code) is None:
        raise ValueError("code must contain six ASCII digits")
    if type(market) is not int or market not in {17, 33}:
        raise ValueError("market must be integer 17 or 33")
    if not isinstance(side, str) or side not in {"buy", "sell"}:
        raise ValueError("side must be buy or sell")
    for name, value in (("rowcount", rowcount), ("endtime", endtime), ("orderid", orderid), ("page_id", page_id)):
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} must be a nonnegative integer")
    if page_id == 0:
        raise ValueError("page_id must be positive")
    parameters = {
        "id": "4027", "marketcode": str(market), "rowcount": str(rowcount),
        "stockcode": code, "reqtype": side, "endtime": str(endtime), "orderid": str(orderid),
    }
    _validate_order_parameters(parameters)
    lines = ["[frame]", "id=2267", f"pageList={page_id}", f"reqPage={page_id}",
             "reqPageCount=1", f"[{page_id}]"]
    lines.extend(f"{key}={value}" for key, value in parameters.items())
    return ("\r\n".join(lines) + "\r\n").encode("ascii")
