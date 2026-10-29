"""Synthetic, offline checks of the directly ported market codec."""
from __future__ import annotations

import base64
import importlib
from pathlib import Path
import struct

import pytest


def module(name: str):
    try:
        return importlib.import_module(f"rquant.l2.{name}")
    except ModuleNotFoundError:
        pytest.fail(f"L2 {name} is not implemented")


def frame(body: bytes) -> bytes:
    return b"\xfd" * 4 + f"{len(body):08x}".encode() + b"\0" + body


def literal(data: bytes) -> bytes:
    size = len(data)
    prefix = bytearray()
    while size >= 128:
        prefix.append((size & 127) | 128)
        size >>= 7
    prefix.append(size)
    if len(data) < 61:
        return bytes(prefix) + (bytes([(len(data) - 1) << 2]) + data if data else b"")
    count = ((len(data) - 1).bit_length() + 7) // 8
    return bytes(prefix) + bytes([(59 + count) << 2]) + (len(data) - 1).to_bytes(count, "little") + data


def bootstrap() -> bytes:
    secret = b"SYNTHETIC_PRIVATE_SENTINEL"
    return struct.pack("<HIIHIII", 77, 0, 0x70000, 0, len(secret), 0, 0) + b"x" * 53 + secret


def template(trade: bool = False) -> bytes:
    if trade:
        query = (b"[frame]\r\nid=2357\r\npageList=10117\r\nreqPage=10117\r\nreqPageCount=1\r\n"
                 b"[10117]\r\nid=4013\r\nispush=1\r\nisVolumeFloat=1\r\nrowcount=500\r\n"
                 b"stockcode=603258\r\nbishu=2500\r\nmarketcode=17\r\ncurrentpage=1\r\nendtime=0\r\n")
    else:
        query = (b"[frame]\r\nid=2267\r\npageList=10113\r\nreqPage=10113\r\nreqPageCount=1\r\n"
                 b"[10113]\r\nid=4027\r\nmarketcode=17\r\nrowcount=20\r\nstockcode=603258\r\n"
                 b"reqtype=buy\r\nendtime=0\r\norderid=0\r\n")
    alphabet = b"aCcMeTKhxnwzmoPbsG4EvU8gyd02B3q6fIVWXYZjApRrDtuHkiLlN1O9F5S7JQ+/"
    payload = base64.b64encode(query).translate(bytes.maketrans(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", alphabet))
    return struct.pack("<HIIHIII", 77, 212, 0x01010000, 0xFF03, len(payload), 2357 if trade else 2267, len(query)) + bytes(range(53)) + payload


def rle(raw: bytes) -> bytes:
    out = bytearray([0, raw[0]])
    control_at = bit_count = 0
    def bit(value: int) -> None:
        nonlocal control_at, bit_count
        out[control_at] |= value << (7 - bit_count)
        bit_count += 1
        if bit_count == 8:
            control_at, bit_count = len(out), 0
            out.append(0)
    position = 1
    while len(raw) - position >= 2:
        bit(0)
        out.extend(raw[position:position + 2])
        position += 2
    if position < len(raw):
        assert raw[-1] == raw[-2]
        bit(1); bit(1); bit(0)
    return bytes(out)


def response(*, trade: bool, request_id: int, page_id: int, code: str = "603258",
             side: str = "buy", ids: tuple[int, ...] = (100, 101)) -> bytes:
    if trade:
        columns = [(0x2001, 4), (0x38, 30), (0x100A, 4), (0x100D, 4),
                   (0x104A, 4), (0x104B, 4), (0xA610, 4), (0x1012, 4)]
        header = b"tb3.0\0" + struct.pack("<iiiHHH", len(ids), -1, 0, 156, 66, 8)
        header += b"".join(struct.pack("<HBB", flag, width, 165) for flag, width in columns)
        header += b"\0" * (156 - len(header))
        rows = []
        for uid in ids:
            rows.append(struct.pack("<I", uid) + b"\0" + "15:00:00       ".encode("utf-16le") + b"\xff"
                        + bytes.fromhex("910600a0") + b"\x0a" + bytes.fromhex("bc0200a0") + b"\x72"
                        + struct.pack("<I", 100) + b"\0" + struct.pack("<I", 200) + b"\0"
                        + struct.pack("<I", 1792047600) + b"\0" + struct.pack("<I", uid) + b"\0")
        row_data = b"".join(rows)
        column_major = bytes(row_data[row * 66 + col] for col in range(66) for row in range(len(rows)))
        packed = rle(column_major)
        data = header + struct.pack("<I", 4 + len(packed)) + struct.pack(">I", len(row_data)) + packed
    else:
        flags = [0x2001, 0x100A, 0x100D, 0x200C, 0x2038, 0xA561]
        title = (code + "-逐笔委托").encode("utf-16le")
        extra = struct.pack("<H", len(title) // 2) + title + b"\0\0" * 6
        header = b"tb1.0\0" + struct.pack("<iiiHHH", len(ids), -1, 0, 48 + len(extra), 30, 6)
        header += b"".join(struct.pack("<HBB", flag, 4, 220) for flag in flags) + extra
        data = header + b"".join(b"".join(struct.pack("<I", value) + b"\xff" for value in
                                          [uid, 100, 1, 1 if side == "buy" else 2, uid + 1000, 514]) for uid in ids)
    payload = literal(data)
    return struct.pack("<HIIHIII", 32, request_id, 0x1110 if trade else 0x1100,
                       page_id, len(payload), 2357 if trade else 2267, 0) + b"\0" * 8 + payload


def test_reviewed_request_codec_and_template_header_are_preserved() -> None:
    request, codec = module("_request"), module("_codec")
    assert request.encode_payload(b"foobar") == b"dZQHyZTL"
    for trade in (False, True):
        parse = request.parse_trade_request_body if trade else request.parse_request_body
        build = request.build_trade_query if trade else request.build_order_query
        query = build(code="603258", market=17, page_id=10117, **({} if trade else
                      {"side": "buy", "rowcount": 20, "endtime": 0, "orderid": 0}))
        wire = (codec.build_trade_frame if trade else codec.build_experimental_order_frame)(template(trade), query, request_id=3)
        actual = parse(wire[13:])
        assert actual["header_extension"] == bytes(range(53))
        assert actual["request_id"] == 3
    with pytest.raises(ValueError):
        request.decode_payload(b"df==\n")
    with pytest.raises(ValueError):
        request.build_trade_query(code="603258", market=33, page_id=10117)


@pytest.mark.parametrize('code,market', [('600000', 17), ('000001', 33), ('300001', 33)])
def test_dynamic_symbol_wire_retains_header_and_exact_routes(code, market):
    request, codec = module('_request'), module('_codec')
    for trade in (True, False):
        query = (request.build_trade_query(code=code, market=market, page_id=10137) if trade else
                 request.build_order_query(code=code, market=market, page_id=10138, side='sell',
                                           rowcount=20, endtime=0, orderid=0))
        wire = (codec.build_trade_frame if trade else codec.build_common_order_frame)(template(trade), query, request_id=62)
        raw = request._read_request_body(wire[13:], frame_id=2357)
        route = request._parse_ini_route(raw['plaintext'], frame_id=2357)
        assert route['parameters']['stockcode'] == code
        assert route['parameters']['marketcode'] == str(market)
        assert raw['header_extension'] == bytes(range(53))
    with pytest.raises(ValueError):
        bad = request.build_order_query(code=code, market=33 if market == 17 else 17, page_id=1, side='buy',
                                        rowcount=20, endtime=0, orderid=0)
        codec.build_common_order_frame(template(), bad, request_id=3)


def test_reviewed_snappy_bounds_and_raw_table_fidelity() -> None:
    codec = module("_codec")
    assert codec.snappy_decompress(b"\x09\x00a\x11\x01") == b"a" * 9
    for encoded in (b"", b"\x04\x01\0", b"\xff\xff\xff\xff\x10"):
        with pytest.raises(ValueError):
            codec.snappy_decompress(encoded)
    result = codec.decode_trade_response(response(trade=True, request_id=3, page_id=10117),
                                         request_id=3, page_id=10117, code="603258", market=17,
                                         source_fd_ordinal=1)
    assert result["table"]["extended_header_length"] == 156
    assert result["table"]["rows"][0]["56"]["style"] == 255
    assert result["table"]["rows"][0]["10"]["value"] == "16.81"
    assert result["granularity"] == "granularity_unverified"
    bad = bytearray(response(trade=True, request_id=3, page_id=10117))
    struct.pack_into("<I", bad, 6, 0x1150)
    with pytest.raises(ValueError):
        codec.decode_trade_response(bytes(bad), request_id=3, page_id=10117, code="603258", market=17, source_fd_ordinal=1)


# Root-filtered actual h26/FFFFFFFF market-only captures; no auth/control.
OBSERVED_PUSH4013 = [{'body_hex': '1a00ffffffff00110000852737010000350900000000000000ffe602e07462332e30000c000000ffffffff000000009a0042000800012004a538001ea50a1004a50d1004a54a1004a54b1004a510a604a5121004a500012b320100d806000400060036003000330032003500380022a611000000078d02003100370004a20100000008a5c409000097011000320034003100330d0618350030002e00301d0200c8055ea803180398a1aaaef32223242526272f1faf2021872213f500fd0b30f500f539f500f53af500f533f500f5310d0c2035f500f538f500f520660400f070fd0b35ab371f383606d700d7a0d702d56428f464bc8464e990000100020347000100f5a0f572ae6a1e72e1b8727ae808e63ffa0dfc000bb5bb0abfc25da79bcabfe973153e853f1b31323cf8fc400ddc0c7f0d00df0b8e5f7e5fd15f6a5f00013738393a3b3c3d3e3f4041427d227e0017', 'body_sha256': 'c1a4f231a176348970c7534199d72ae663b6f668bcbf4fbfc6afe0c43f2de703', 'table_sha256': 'f40e5a8a7ccb9b814480a2c5f6c5f38bd7b92f4c8e3ff367e0d9b1534ffccee9', 'row_count': 12, 'field_values': {'1': [1253272, 1253281, 1253290, 1253294, 1253619, 1253666, 1253667, 1253668, 1253669, 1253670, 1253671, 1253935], '18': ['8759', '8760', '8761', '8762', '8763', '8764', '8765', '8766', '8767', '8768', '8769', '8770'], '34320': [1792114318, 1792114318, 1792114318, 1792114318, 1792114318, 1792114318, 1792114318, 1792114318, 1792114318, 1792114318, 1792114318, 1792114318]}, 'styles_sha256': '537e3678b66f49bb15892a14b77c89942967fcda225134865c6e799c9aea9459', 'row_data_sha256': 'd4b20ee10f638c4a2736321446a3faf628e9376c13ac6669b4bb0e3d9e3d49ff'}, {'body_hex': '1a00ffffffff00110000852703010000350900000000000000ffb302e07462332e300006000000ffffffff000000009a0042000800012004a538001ea50a1004a50d1004a54a1004a54b1004a510a604a5121004a500012b320100d806000400060036003000330032003500380022a611000000078d02003100370004a20100000008a5c409000097011000320034003100330d0618350030002e00301d020095055e88018c1c8999c4dd2526236b2526138b00eb308b008b398b008b3a8b008b338b008b318b090c00350d1808208b005e0400f046eb363e35062e002ea02e02286475c89014000105710071a0717267e8e13f9f080d1700c4fe071d709c6a4041425c0d5f005c8f5c7e5cd15c6a5c000b434445464748228b00f005', 'body_sha256': 'ec9cf9ecc03ac7c2ff09742b700431d52130a8ff6e2fa7e094bdabc86e10fcc0', 'table_sha256': '36e8226edc89e69995df5fbb7f5f7d9cd64508fc4954071a823388dc0e61100a', 'row_count': 6, 'field_values': {'1': [1254281, 1254297, 1254340, 1254877, 1254949, 1254950], '18': ['8771', '8772', '8773', '8774', '8775', '8776'], '34320': [1792114319, 1792114319, 1792114319, 1792114319, 1792114319, 1792114319]}, 'styles_sha256': '75b43c667348ccb7cc39d978b830781940eb2cc3b93b76fee8bbb8f2b93385c7', 'row_data_sha256': '4143c9fca2d44b97ac2c2719847f116f09d17807bad149b8e985ae4381407e80'}, {'body_hex': '1a00ffffffff0011000085272a010000350900000000000000ffd902e07462332e300009000000ffffffff000000009a0042000800012004a538001ea50a1004a50d1004a54a1004a54b1004a510a604a5121004a500012b320100d806000400060036003000330032003500380022a611000000078d02003100370004a20100000008a5c409000097011000320034003100330d0618350030002e00301d0200bb055ea002520944ff3fb8b9ba21f30c2853292a2b2c13eefe0005ee30ee00ee39ee00ee3aee00ee33ee00ee310d0c00350d180820ee00560400f067fe0005c9353438347706770077a076026a64c8842c64335400030151000b00f777a07072d56a72e13516823982084d188b458b640d0b0d0bbf003805259db7fd7c7356684435fc460dc30c0d5f00dd058fdd7eddd1dd6add0083494a4b4c4d4e4f505122eefc000e', 'body_sha256': '7f7d83a16fbd4cd9ecf78f80d61364c6aa59ff892d6caf857e655b23fb07a267', 'table_sha256': '4366c93a608f4299bfb4eb82d2158e13ecfbde1b5dcd1a58e4a3db757792b75a', 'row_count': 9, 'field_values': {'1': [1255492, 1255679, 1255743, 1255864, 1255865, 1255866, 1255969, 1256435, 1256460], '18': ['8777', '8778', '8779', '8780', '8781', '8782', '8783', '8784', '8785'], '34320': [1792114319, 1792114319, 1792114319, 1792114319, 1792114319, 1792114319, 1792114319, 1792114319, 1792114319]}, 'styles_sha256': 'a3acbf3386614f561b8c461796c8fc51e0be9deb8e21d33f961361b2eea2f46b', 'row_data_sha256': '0710239bbfed2d6b81620327cf7cdfccfa10c7b02623802ea6de94e9b2b911ae'}, {'body_hex': '1a00ffffffff00110000852727010000350900000000000000ffd502e07462332e300009000000ffffffff000000009a0042000800012004a538001ea50a1004a50d1004a54a1004a54b1004a510a604a5121004a500012b320100d806000400060036003000330032003500380022a611000000078d02003100370004a20100000008a5c409000097011000320034003100330d0618350030002e00301d0200b7055ea002520b626fe8e9ea031a75332d332e2f3013eefe0005ee30ee00ee39ee00ee3aee00ee33ee00ee31ee090c00350d180820ee00560400f064fe000547343536371f3506770077a0770266b01464c8a164bc040500f0fb0200bba094726aa3726a825ca635218b47c3490b5c0ddf0086054551ca0a72b559a3854746474647482e0def00ee058fee7eeed1ee6aee00c152535455565758595a22f77e000e', 'body_sha256': '78eaadda0c5e34504723723e6e99b52147cc4d64326a3020514f6fa9b1e3e8a0', 'table_sha256': '18a75c56b35c95dff31d8ae4e16996c387b3765b33332430e9b7b300380c440d', 'row_count': 9, 'field_values': {'1': [1256802, 1256815, 1256936, 1256937, 1256938, 1256963, 1257242, 1257333, 1257523], '18': ['8786', '8787', '8788', '8789', '8790', '8791', '8792', '8793', '8794'], '34320': [1792114319, 1792114319, 1792114319, 1792114319, 1792114319, 1792114319, 1792114319, 1792114319, 1792114319]}, 'styles_sha256': 'a168ef7cccd67f7514a408b13152dc8fd9e99d207870680aeeb26d152861f7a8', 'row_data_sha256': 'a281b490180da1cb1d3f01d4855bd8c1eefa0faa41c81ac367c6cfd8316432e2'}]


@pytest.mark.parametrize("record", OBSERVED_PUSH4013)
def test_actual_sentinel_push_preserves_raw_header_table_rows_and_styles(record) -> None:
    import hashlib
    codec = module("_codec")
    body = bytes.fromhex(record["body_hex"])
    actual = codec.decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10117,
        code="603258", market=17, source_fd_ordinal=8)
    assert actual["response_body"] == body and actual["request_id"] == 0xFFFFFFFF
    assert actual["symbol_source"] == "registered_page_association_only"
    assert actual["response_sha256"] == record["body_sha256"]
    assert hashlib.sha256(actual["raw_table"]).hexdigest() == record["table_sha256"]
    table = actual["table"]
    assert table["row_count"] == record["row_count"]
    assert table["row_data_sha256"] == record["row_data_sha256"]
    for field, expected in record["field_values"].items():
        assert [row[field]["value"] for row in table["rows"]] == expected
    styles = bytes(cell["style"] for row in table["rows"] for cell in row.values())
    assert hashlib.sha256(styles).hexdigest() == record["styles_sha256"]


@pytest.mark.parametrize("fault", ["header", "id", "h32_sentinel", "flags", "page", "command", "length"])
def test_push_envelope_rejects_every_unobserved_identity_or_profile(fault) -> None:
    codec = module("_codec")
    body = bytearray.fromhex(OBSERVED_PUSH4013[0]["body_hex"])
    if fault == "header": struct.pack_into("<H", body, 0, 25)
    if fault == "id": struct.pack_into("<I", body, 2, 3)
    if fault == "h32_sentinel":
        body[26:26] = b"x" * 6
        struct.pack_into("<H", body, 0, 32)
    if fault == "flags": struct.pack_into("<I", body, 6, 0x1110)
    if fault == "page": struct.pack_into("<H", body, 10, 10127)
    if fault == "command": struct.pack_into("<I", body, 16, 2267)
    if fault == "length": struct.pack_into("<I", body, 12, 1)
    with pytest.raises(ValueError):
        codec.decode_trade_response(bytes(body), request_id=0xFFFFFFFF, page_id=10117,
            code="603258", market=17, source_fd_ordinal=8)


# Root-filtered actual tb1 trade push tables with independent complete parse.
OBSERVED_TB1_TRADE_PUSH = [{'body_hex': '1a00ffffffff001100008527d5000000350900000000000000ff9e02e07462312e300002000000ffffffff000000009a0042000800012004a538001ea50a1004a50d1004a54a1004a54b1004a510a604a5121004a500012b320100d806000400060036003000330032003500380022a611000000078d02003100370004a20100000008a5c409000097011000320034003100330d0618350030002e00301d0254d0353d0000300039003a00340034003a0030003100203202008000170600a002900100a06ad19927000091922700006181d16a004c520000001d369a420018bc0200a06a029a324200104d52000000',
  'body_sha256': 'a6dcb8b6a098ff8a39ee9ad84e35945ccf04715cc87b78d9cd6afe3cec2eb783',
  'raw_table_hex': '7462312e300002000000ffffffff000000009a0042000800012004a538001ea50a1004a50d1004a54a1004a54b1004a510a604a5121004a500000000000000000000000000000000000006000400060036003000330032003500380022a611000000078d02003100370004a20100000008a5c4090000970110003200340031003300340031003300350030002e00300030003000300030003000d0353d0000300039003a00340034003a0030003100200020002000200020002000200000170600a002900100a06ad19927000091922700006181d16a004c520000001d363d0000300039003a00340034003a0030003100200020002000200020002000200000170600a002bc0200a06a029a27000091922700006181d16a004d52000000',
  'table': {'title': '',
            'table_version': 'tb1.0',
            'source_codec': 'snappy',
            'source_sha256': '35cc2ae8a112160c419d4294d234b4b21b96088cd9b1ee803876324d696fb364',
            'row_data_sha256': '8c67e15d00bb9d0cb86c8cc344abae9460beee1114ba4376d138441a6af3ba9a',
            'row_count': 2,
            'row_width': 66,
            'col_count': 8,
            'extended_header_length': 154,
            'header_unknown_10': -1,
            'header_unknown_14': 0,
            'columns': [{'field_id': 1,
                         'flags': 8193,
                         'flags_hex': '0x2001',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'unsigned-le'},
                        {'field_id': 56,
                         'flags': 56,
                         'flags_hex': '0x38',
                         'width': 30,
                         'extra': 165,
                         'encoding': 'utf-16le'},
                        {'field_id': 10,
                         'flags': 4106,
                         'flags_hex': '0x100a',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'packed32-le'},
                        {'field_id': 13,
                         'flags': 4109,
                         'flags_hex': '0x100d',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'packed32-le'},
                        {'field_id': 74,
                         'flags': 4170,
                         'flags_hex': '0x104a',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'packed32-le'},
                        {'field_id': 75,
                         'flags': 4171,
                         'flags_hex': '0x104b',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'packed32-le'},
                        {'field_id': 34320,
                         'flags': 42512,
                         'flags_hex': '0xa610',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'unsigned-le'},
                        {'field_id': 18,
                         'flags': 4114,
                         'flags_hex': '0x1012',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'packed32-le'}],
            'rows': [{'1': {'value': 4011472, 'raw_hex': 'd0353d00', 'style': 0},
                      '56': {'value': '09:44:01       ',
                             'raw_hex': '300039003a00340034003a00300031002000200020002000200020002000',
                             'style': 0},
                      '10': {'value': '15.59', 'raw_hex': '170600a0', 'style': 2},
                      '13': {'value': '4.00', 'raw_hex': '900100a0', 'style': 106},
                      '74': {'value': '2595281', 'raw_hex': 'd1992700', 'style': 0},
                      '75': {'value': '2593425', 'raw_hex': '91922700', 'style': 0},
                      '34320': {'value': 1792115041, 'raw_hex': '6181d16a', 'style': 0},
                      '18': {'value': '21068', 'raw_hex': '4c520000', 'style': 0}},
                     {'1': {'value': 4011549, 'raw_hex': '1d363d00', 'style': 0},
                      '56': {'value': '09:44:01       ',
                             'raw_hex': '300039003a00340034003a00300031002000200020002000200020002000',
                             'style': 0},
                      '10': {'value': '15.59', 'raw_hex': '170600a0', 'style': 2},
                      '13': {'value': '7.00', 'raw_hex': 'bc0200a0', 'style': 106},
                      '74': {'value': '2595330', 'raw_hex': '029a2700', 'style': 0},
                      '75': {'value': '2593425', 'raw_hex': '91922700', 'style': 0},
                      '34320': {'value': 1792115041, 'raw_hex': '6181d16a', 'style': 0},
                      '18': {'value': '21069', 'raw_hex': '4d520000', 'style': 0}}],
            'interpretation_limits': '字段ID、flags、原始字节和style按供应商格式保留；未确认单位、买卖枚举或交易所channel。标题为明细的表仅标记为明细，尚不能据此认定是逐笔成交。'}},
 {'body_hex': '1a00ffffffff001100008527bf000000350900000000000000ffdc01e87462312e300001000000ffffffff000000009a0042000800012004a538001ea50a1004a50d1004a54a1004a54b1004a510a604a5121004a50000003a0200d806000400060036003000330032003500380022a611000000078d02003100370004a20100000008a5c409000097011000320034003100330d0618350030002e00301d0254163a3d0000300039003a00340034003a0030003100203202007800170600a0022c0100a06a649c27000091922700006181d16a004e52000000',
  'body_sha256': '77eb5a02a1c1d05caa1751e7865f4ddc6c9588fd0f50e867c3d0d9baf0472303',
  'raw_table_hex': '7462312e300001000000ffffffff000000009a0042000800012004a538001ea50a1004a50d1004a54a1004a54b1004a510a604a5121004a500000000000000000000000000000000000006000400060036003000330032003500380022a611000000078d02003100370004a20100000008a5c4090000970110003200340031003300340031003300350030002e00300030003000300030003000163a3d0000300039003a00340034003a0030003100200020002000200020002000200000170600a0022c0100a06a649c27000091922700006181d16a004e52000000',
  'table': {'title': '',
            'table_version': 'tb1.0',
            'source_codec': 'snappy',
            'source_sha256': 'ac6c83582e8aaaa2707e780cb3847e578612be6c59dc70f25cd94a38e34700bc',
            'row_data_sha256': 'bff75b41aaaa5bbe93e25dafe384c9877ae56c2a2ec308c66d769b598e1ddc30',
            'row_count': 1,
            'row_width': 66,
            'col_count': 8,
            'extended_header_length': 154,
            'header_unknown_10': -1,
            'header_unknown_14': 0,
            'columns': [{'field_id': 1,
                         'flags': 8193,
                         'flags_hex': '0x2001',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'unsigned-le'},
                        {'field_id': 56,
                         'flags': 56,
                         'flags_hex': '0x38',
                         'width': 30,
                         'extra': 165,
                         'encoding': 'utf-16le'},
                        {'field_id': 10,
                         'flags': 4106,
                         'flags_hex': '0x100a',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'packed32-le'},
                        {'field_id': 13,
                         'flags': 4109,
                         'flags_hex': '0x100d',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'packed32-le'},
                        {'field_id': 74,
                         'flags': 4170,
                         'flags_hex': '0x104a',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'packed32-le'},
                        {'field_id': 75,
                         'flags': 4171,
                         'flags_hex': '0x104b',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'packed32-le'},
                        {'field_id': 34320,
                         'flags': 42512,
                         'flags_hex': '0xa610',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'unsigned-le'},
                        {'field_id': 18,
                         'flags': 4114,
                         'flags_hex': '0x1012',
                         'width': 4,
                         'extra': 165,
                         'encoding': 'packed32-le'}],
            'rows': [{'1': {'value': 4012566, 'raw_hex': '163a3d00', 'style': 0},
                      '56': {'value': '09:44:01       ',
                             'raw_hex': '300039003a00340034003a00300031002000200020002000200020002000',
                             'style': 0},
                      '10': {'value': '15.59', 'raw_hex': '170600a0', 'style': 2},
                      '13': {'value': '3.00', 'raw_hex': '2c0100a0', 'style': 106},
                      '74': {'value': '2595940', 'raw_hex': '649c2700', 'style': 0},
                      '75': {'value': '2593425', 'raw_hex': '91922700', 'style': 0},
                      '34320': {'value': 1792115041, 'raw_hex': '6181d16a', 'style': 0},
                      '18': {'value': '21070', 'raw_hex': '4e520000', 'style': 0}}],
            'interpretation_limits': '字段ID、flags、原始字节和style按供应商格式保留；未确认单位、买卖枚举或交易所channel。标题为明细的表仅标记为明细，尚不能据此认定是逐笔成交。'}}]


@pytest.mark.parametrize("record", OBSERVED_TB1_TRADE_PUSH)
def test_actual_tb1_trade_push_preserves_complete_table_raw_values_and_styles(record) -> None:
    codec = module("_codec")
    body = bytes.fromhex(record["body_hex"])
    result = codec.decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10117,
        code="603258", market=17, source_fd_ordinal=4)
    assert result["response_body"] == body
    assert result["response_sha256"] == record["body_sha256"]
    assert result["raw_table"].hex() == record["raw_table_hex"]
    assert result["table"] == record["table"]


@pytest.mark.parametrize("fault", ["h32", "schema", "truncated", "version"])
def test_tb1_trade_push_keeps_normal_seed_version_and_strict_table_guards(fault) -> None:
    codec = module("_codec")
    record = OBSERVED_TB1_TRADE_PUSH[0]
    raw = bytearray.fromhex(record["raw_table_hex"])
    if fault == "schema": raw[24] ^= 1
    if fault == "truncated": del raw[-1]
    if fault == "version": raw[2] = ord("2")
    payload = literal(bytes(raw))
    request_id = 3 if fault == "h32" else 0xFFFFFFFF
    header = 32 if fault == "h32" else 26
    # Negative synthetic envelope only; captured headers remain unchanged above.
    body = struct.pack("<HIIHIII", header, request_id, 0x1100, 10117, len(payload), 2357, 0)
    body += b"\0" * (header - 24) + payload
    with pytest.raises(ValueError):
        codec.decode_trade_response(body, request_id=request_id, page_id=10117,
            code="603258", market=17, source_fd_ordinal=4)


# Root-filtered actual50row order push; canonical hash covers all values/raw/styles.
OBSERVED_ORDER_PUSH50 = {'body_hex': '1a00ffffffff001100009027f3010000db0800000000000000ffd40cd07462312e300032000000ffffffff0000000078001e000600012004dc0a1004dc0d1004dc0c2004dc382004dc61a504dc0b003300300d022c36002d001090147bd4595862013a110110020004000c3228000101003001051000000008a5016e78ae2d8d000fd85301c00a0f0000000f010000000ad482d16a0f010200000aaf151e000b4a1e0000b0151e000c4a1e0000b1151e00054a1e0000b2151e4e5a0000b3151e4e3c0000b4151e00084a5a0000b5151e000e4a1e0000b6151e4eb40000b7151e00044a3c0000b8151e4e5a0000b9151e00034a3c0000ba151e00154a1e0000bb151e4e960000bc151e00024a3c0000bd151e00064a1e0000be151e000d4a1e0000bf151e4e780000c0151e4e680100c1151e4e3a0200c3721e0000c4721e0000c5151e00074ab40000c6151e00124a1e0000c7151e4ea40100c8151e4ee00100c9151e4e940200ca151e000a4a780000cb151e00114a1e0000cc151e4e680100cd151e00094a3c0000ce151e4eb40000cf721e0000d0151e4e480300d1151e4e3a0200d2151e4e780000d3151e4e580200d4151e4ec20100d5151e00014af00000d6151e4e3c0000d7151e4efe0100d8151e4e4a0100d9721e0000da151e4ec00300db151e4e580200dd151e4e780000de151e4e680100df151e4e680100e0151e4e960000e1151e00104a4a01',
 'body_sha256': '16196305af0bd2e271895ef737c7f1dc7c9b5879c65fa134969a04c68b1c7e09',
 'raw_table_hex': '7462312e300032000000ffffffff0000000078001e000600012004dc0a1004dc0d1004dc0c2004dc382004dc61a504dc0b003300300030003000300036002d001090147bd4595862000000000000000000000000020004000c0033003000300030003000360000000000300000000000000008a532000000ae2d8d000fd85301c00a0f0000000f010000000ad482d16a0f010200000aaf2d8d000fd85301c00a0b0000000f010000000ad482d16a0f010200000ab02d8d000fd85301c00a0c0000000f010000000ad482d16a0f010200000ab12d8d000fd85301c00a050000000f010000000ad482d16a0f010200000ab22d8d000fd85301c00a0b0000000f010000000ad482d16a0f010200000ab32d8d000fd85301c00a050000000f010000000ad482d16a0f010200000ab42d8d000fd85301c00a080000000f010000000ad482d16a0f010200000ab52d8d000fd85301c00a0e0000000f010000000ad482d16a0f010200000ab62d8d000fd85301c00a0c0000000f010000000ad482d16a0f010200000ab72d8d000fd85301c00a040000000f010000000ad482d16a0f010200000ab82d8d000fd85301c00a0e0000000f010000000ad482d16a0f010200000ab92d8d000fd85301c00a030000000f010000000ad482d16a0f010200000aba2d8d000fd85301c00a150000000f010000000ad482d16a0f010200000abb2d8d000fd85301c00a0c0000000f010000000ad482d16a0f010200000abc2d8d000fd85301c00a020000000f010000000ad482d16a0f010200000abd2d8d000fd85301c00a060000000f010000000ad482d16a0f010200000abe2d8d000fd85301c00a0d0000000f010000000ad482d16a0f010200000abf2d8d000fd85301c00a0c0000000f010000000ad482d16a0f010200000ac02d8d000fd85301c00a080000000f010000000ad482d16a0f010200000ac12d8d000fd85301c00a0f0000000f010000000ad482d16a0f010200000ac32d8d000fd85301c00a0f0000000f010000000ad482d16a0f010200000ac42d8d000fd85301c00a0f0000000f010000000ad482d16a0f010200000ac52d8d000fd85301c00a070000000f010000000ad482d16a0f010200000ac62d8d000fd85301c00a120000000f010000000ad482d16a0f010200000ac72d8d000fd85301c00a0e0000000f010000000ad482d16a0f010200000ac82d8d000fd85301c00a040000000f010000000ad482d16a0f010200000ac92d8d000fd85301c00a0b0000000f010000000ad482d16a0f010200000aca2d8d000fd85301c00a0a0000000f010000000ad482d16a0f010200000acb2d8d000fd85301c00a110000000f010000000ad482d16a0f010200000acc2d8d000fd85301c00a0c0000000f010000000ad482d16a0f010200000acd2d8d000fd85301c00a090000000f010000000ad482d16a0f010200000ace2d8d000fd85301c00a040000000f010000000ad482d16a0f010200000acf2d8d000fd85301c00a040000000f010000000ad482d16a0f010200000ad02d8d000fd85301c00a050000000f010000000ad482d16a0f010200000ad12d8d000fd85301c00a060000000f010000000ad482d16a0f010200000ad22d8d000fd85301c00a040000000f010000000ad482d16a0f010200000ad32d8d000fd85301c00a0d0000000f010000000ad482d16a0f010200000ad42d8d000fd85301c00a070000000f010000000ad482d16a0f010200000ad52d8d000fd85301c00a010000000f010000000ad482d16a0f010200000ad62d8d000fd85301c00a070000000f010000000ad482d16a0f010200000ad72d8d000fd85301c00a120000000f010000000ad482d16a0f010200000ad82d8d000fd85301c00a090000000f010000000ad482d16a0f010200000ad92d8d000fd85301c00a090000000f010000000ad482d16a0f010200000ada2d8d000fd85301c00a030000000f010000000ad482d16a0f010200000adb2d8d000fd85301c00a0e0000000f010000000ad482d16a0f010200000add2d8d000fd85301c00a090000000f010000000ad482d16a0f010200000ade2d8d000fd85301c00a060000000f010000000ad482d16a0f010200000adf2d8d000fd85301c00a040000000f010000000ad482d16a0f010200000ae02d8d000fd85301c00a030000000f010000000ad482d16a0f010200000ae12d8d000fd85301c00a100000000f010000000ad482d16a0f010200000a',
 'canonical_table_sha256': '6032e143b7c4d872c67b6e4d6b6d3fd8eee2ce1a641dbe76a62fa5882f624dbb'}


def test_actual_fifty_row_order_push_is_lossless_and_keeps_default_seed_limit() -> None:
    import hashlib
    import json
    codec = module("_codec")
    record = OBSERVED_ORDER_PUSH50
    body = bytes.fromhex(record["body_hex"])
    result = codec.decode_order_response(body, request_id=0xFFFFFFFF, page_id=10128,
        code="300006", source_fd_ordinal=507)
    assert result["response_body"] == body
    assert result["response_sha256"] == record["body_sha256"]
    assert result["raw_table"].hex() == record["raw_table_hex"]
    assert result["table"]["row_count"] == 50
    canonical = json.dumps(result["table"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    assert hashlib.sha256(canonical).hexdigest() == record["canonical_table_sha256"]


@pytest.mark.parametrize("fault", ["normal21", "push501", "malformed"])
def test_order_push_cardinality_preserves_normal_and_resource_limits(fault) -> None:
    codec = module("_codec")
    if fault == "normal21":
        body = response(trade=False, request_id=3, page_id=10128, code="300006", ids=tuple(range(1, 22)))
        request = 3
    else:
        request = 0xFFFFFFFF
        if fault == "push501":
            seed = response(trade=False, request_id=3, page_id=10128, code="300006", ids=tuple(range(1, 502)))
            payload = seed[32:]
        else:
            raw = bytearray.fromhex(OBSERVED_ORDER_PUSH50["raw_table_hex"])
            struct.pack_into("<H", raw, 20, 31)
            payload = literal(bytes(raw))
        body = struct.pack("<HIIHIII", 26, request, 0x1100, 10128, len(payload), 2267, 0) + b"\0\0" + payload
    if fault == "push501":
        assert codec.decode_order_response(body, request_id=request, page_id=10128, code="300006", source_fd_ordinal=507)["table"]["row_count"] == 501
    else:
        with pytest.raises(ValueError):
            codec.decode_order_response(body, request_id=request, page_id=10128, code="300006", source_fd_ordinal=507)


# Actual commonframe2357 orders, original h32 seed/h26 push; no auth/control.
COMMON_ORDER_RESPONSES = [{'binding': {'code': '603258', 'market': 17, 'kind': 'vendor4027', 'side': 'buy', 'page_id': 10118},
  'request_id': 4,
  'source_fd_ordinal': 7,
  'response_sha256': '306a9bacc34dd439bc57b0d88ed3ae1e7e2d458f2c5482839583dd7552efb7df',
  'response_body_hex': '20000400000000110000862773010000350900000000000000ff000003000000d005f0497462312e300014000000ffffffff0000000078001e000600012004dc0a1004dc0d1004dc0c2004dc382004dc61a504dc0b003600300033003200350038002d001090147bd45958620000013c090110020004000c322800010104a67501060c000008a5016e5c7ec75b000f1a3b00b0020c0000000f010000000ae18bd16a090a1488ce5b000f38011e0119091400e3151e04eadc113c014a091e00e7151e043ce2113c0005155a00e9151e0097011e0024015a000f4a1e0004d3e3115a00224a1e001409e65b000f56013c195a00ea155a0483e9111e04040011d200eb151e00d6761e0000ee11b40003153c00ec153c14c9f55b000f2e01780002151e00ee151e00e3011e254a000a4a1e0004eaf711d2394a193c045df8117819b4191e0449fa111e193c00ef15780846055c0d9604070011f000f1151e00d8011e2568193c00f2151e083e065c2dfe394a191e00b4011e053c1996191e20e30a5c000f423b00b01d5a24f38bd16a0f010000000a',
  'raw_table_hex': '7462312e300014000000ffffffff0000000078001e000600012004dc0a1004dc0d1004dc0c2004dc382004dc61a504dc0b003600300033003200350038002d001090147bd4595862000000000000000000000000020004000c0036003000330032003500380000000000a67500000000000008a5140000007ec75b000f1a3b00b0020c0000000f010000000ae18bd16a0f010000000a88ce5b000f383b00b002010000000f010000000ae38bd16a0f010000000aeadc5b000f1a3b00b002140000000f010000000ae78bd16a0f010000000a3ce25b000f383b00b002050000000f010000000ae98bd16a0f010000000a97e25b000f243b00b0020f0000000f010000000ae98bd16a0f010000000ad3e35b000f1a3b00b002220000000f010000000ae98bd16a0f010000000a09e65b000f563b00b002050000000f010000000aea8bd16a0f010000000a83e95b000f563b00b002040000000f010000000aeb8bd16a0f010000000ad6e95b000f563b00b002040000000f010000000aeb8bd16a0f010000000ad6ee5b000f383b00b002030000000f010000000aec8bd16a0f010000000ac9f55b000f2e3b00b002020000000f010000000aee8bd16a0f010000000ae3f55b000f1a3b00b0020a0000000f010000000aee8bd16a0f010000000aeaf75b000f1a3b00b002010000000f010000000aee8bd16a0f010000000a5df85b000f383b00b002040000000f010000000aee8bd16a0f010000000a49fa5b000f383b00b002010000000f010000000aef8bd16a0f010000000a46055c000f2e3b00b002070000000f010000000af18bd16a0f010000000ad8055c000f243b00b002010000000f010000000af28bd16a0f010000000a3e065c000f1a3b00b002050000000f010000000af28bd16a0f010000000ab4065c000f243b00b002040000000f010000000af28bd16a0f010000000ae30a5c000f423b00b002010000000f010000000af38bd16a0f010000000a',
  'canonical_table_sha256': '3c615b7fdfcb52887e7787b07cbbdf9cb17eeeaf847b94a84d7c7c624c27c5e5'},
 {'binding': {'code': '603258', 'market': 17, 'kind': 'vendor4027', 'side': 'buy', 'page_id': 10118},
  'request_id': 4294967295,
  'source_fd_ordinal': 9,
  'response_sha256': 'dcf80aa2cb2695f8f7f485abecd059f5160720be55e85b78fc0e2852d32912cb',
  'response_body_hex': '1a00ffffffff00110000862797000000350900000000000000ffb401f0497462312e300002000000ffffffff0000000078001e000600012004dc0a1004dc0d1004dc0c2004dc382004dc61a504dc0b003600300033003200350038002d001090147bd45958620000013c090110020004000c32280001010ce0d6982001080408a5016e5c8b155c000f603b00b0020b0000000f010000000af68bd16a090a00a0151e0119091424f68bd16a0f010000000a',
  'raw_table_hex': '7462312e300002000000ffffffff0000000078001e000600012004dc0a1004dc0d1004dc0c2004dc382004dc61a504dc0b003600300033003200350038002d001090147bd4595862000000000000000000000000020004000c0036003000330032003500380000000000e0d698200000000008a5020000008b155c000f603b00b0020b0000000f010000000af68bd16a0f010000000aa0155c000f603b00b002010000000f010000000af68bd16a0f010000000a',
  'canonical_table_sha256': 'ec39d1c2a045eea3925b05a1ceddcb7d0e38c4005926000e74a67dea36962d19'}]


@pytest.mark.parametrize("record", COMMON_ORDER_RESPONSES)
def test_actual_common_frame_order_keeps_literal_header_full_table_and_styles(record) -> None:
    import hashlib
    import json
    codec = module("_codec")
    b = record["binding"]
    body = bytes.fromhex(record["response_body_hex"])
    actual = codec.decode_order_response(body, request_id=record["request_id"], page_id=b["page_id"],
        code=b["code"], source_fd_ordinal=record["source_fd_ordinal"])
    assert actual["response_body"] == body and actual["request_id"] == record["request_id"]
    assert actual["response_sha256"] == record["response_sha256"]
    assert actual["raw_table"].hex() == record["raw_table_hex"]
    canonical = json.dumps(actual["table"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    assert hashlib.sha256(canonical).hexdigest() == record["canonical_table_sha256"]


def test_common_order_wire_changes_only_coupled_frame_and_derived_encoding() -> None:
    request, codec = module("_request"), module("_codec")
    assert hasattr(codec, "build_common_order_frame"), "common-order builder missing"
    active = []
    for index, (code, market, side) in enumerate([(c,m,s) for c,m in [("603258",17),("300006",33)] for s in [None,"buy","sell"]]):
        page = [10117,10118,10119,10127,10128,10129][index]
        active.append(str(page))
        query = (request.build_trade_query(code=code,market=market,page_id=page) if side is None else
            request.build_order_query(code=code,market=market,side=side,page_id=page,rowcount=20,endtime=0,orderid=0))
        query = query.replace(f"pageList={page}\r\n".encode(), f"pageList={','.join(active)}\r\n".encode())
        builder = codec.build_trade_frame if side is None else codec.build_experimental_order_frame
        baseline = builder(template(side is None),query,request_id=3+index)
        actual = (builder if side is None else codec.build_common_order_frame)(template(side is None),query,request_id=3+index)
        old = request._read_request_body(baseline[13:],frame_id=2357 if side is None else 2267)
        new = request._read_request_body(actual[13:],frame_id=2357)
        assert new["header"][:12] == old["header"][:12] and new["header"][24:] == old["header"][24:]
        assert new["plaintext"] == old["plaintext"].replace(b"[frame]\r\nid=2267\r\n",b"[frame]\r\nid=2357\r\n",1)
        if side is None: assert actual == baseline
        else:
            route = request._parse_ini_route(new["plaintext"],frame_id=2357)
            assert route["parameters"] == request.parse_market_ini(old["plaintext"])["parameters"]
            assert route["frame"]["pageList"] == ','.join(active)


@pytest.mark.parametrize("flags", [0x1110,0x1130])
def test_common_order_frame_never_inherits_trade_flags(flags) -> None:
    codec = module("_codec")
    record = COMMON_ORDER_RESPONSES[0]
    body = bytearray.fromhex(record["response_body_hex"])
    struct.pack_into("<I",body,6,flags)
    with pytest.raises(ValueError):
        codec.decode_order_response(bytes(body),request_id=record["request_id"],page_id=10118,code="603258",source_fd_ordinal=4)


def test_actual_trade763_push_is_lossless() -> None:
    import hashlib
    from pathlib import Path

    body = (Path(__file__).resolve().parents[1] / "fixtures/l2_trade_push_763.bin").read_bytes()
    assert len(body) == 4543
    assert hashlib.sha256(body).hexdigest() == "f7c0734e1de817820745374ca311dab9ca936a52706179b89a9dd85d5f78024b"
    result = module("_codec").decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10237,
        code="601899", market=17, source_fd_ordinal=4953)
    assert result["response_body"] == body
    assert result["table"]["row_count"] == len(result["table"]["rows"]) == 763
    assert hashlib.sha256(result["raw_table"]).hexdigest() == "43be6a8dc34261bafa8e546c09b76ec590b021aff34687c2d6092869b59d4ffb"
    assert result["table"]["row_data_sha256"] == "5b967e2cdd8a92581e0be872c9971b28955bab09dbc86bd5b8f6dd785b7f59c6"


def test_actual_trade763_model_keeps_all_rows_and_original_hashes() -> None:
    from datetime import UTC, datetime
    from pathlib import Path
    from rquant.l2.models import MarketObservation, RawTable

    codec = module("_codec")
    body = (Path(__file__).resolve().parents[1] / "fixtures/l2_trade_push_763.bin").read_bytes()
    raw = codec.snappy_decompress(body[26:])
    # Isolate the existing model boundary independently of the codec row cap.
    result = codec._response_result(body, raw, codec._table_metadata(raw, version="tb3.0", max_rows=4096),
        request_id=0xFFFFFFFF, page_id=10237, source_fd_ordinal=4953)
    table = RawTable.model_validate(result["table"])
    observation = MarketObservation(epoch_id="actual_trade763", binding={"page_id": 10237,
        "kind": "vendor4013", "code": "601899", "market": 17},
        received_at=datetime(2026, 10, 19, 5, 55, tzinfo=UTC), request_id=0xFFFFFFFF,
        source_fd_ordinal=4953, delivery="candidate_push", response_body_hex=body.hex(),
        raw_table_hex=raw.hex(), table=table, response_sha256=result["response_sha256"])
    restored = MarketObservation.model_validate_json(observation.model_dump_json())
    assert restored == observation and restored.table.row_count == len(restored.table.rows) == 763
    assert restored.table.row_data_sha256 == "5b967e2cdd8a92581e0be872c9971b28955bab09dbc86bd5b8f6dd785b7f59c6"


def worst_trade_push(rows: int, version: str = "tb1.0") -> bytes:
    codec = module("_codec")
    seed = response(trade=True, request_id=3, page_id=10117)
    raw = codec.snappy_decompress(seed[32:])
    extended = int.from_bytes(raw[18:20], "little")
    header = bytearray(raw[:extended])
    header[:6] = version.encode() + b"\0"
    struct.pack_into("<i", header, 6, rows)
    row = bytearray()
    for index in range(8):
        flags, width, _ = struct.unpack_from("<HBB", header, 24 + index * 4)
        encoding = flags & 0x7000
        value = b"\x01\x00" * (width // 2) if encoding == 0 else bytes.fromhex("ffffff7f") if encoding == 0x1000 else b"\xff" * width
        row.extend(value + b"\xff")
    row_data = bytes(row) * rows
    if version == "tb3.0":
        packed = rle(bytes(row_data[row_index * 66 + column] for column in range(66) for row_index in range(rows)))
        data = bytes(header) + struct.pack("<I", 4 + len(packed)) + struct.pack(">I", len(row_data)) + packed
    else:
        data = bytes(header) + row_data
    payload = literal(data)
    return struct.pack("<HIIHIII", 26, 0xFFFFFFFF, 0x1100, 10117, len(payload), 2357, 0) + b"\0\0" + payload


def market_model_data(body: bytes, result: dict, *, delivery: str = "candidate_push", binding: dict | None = None) -> dict:
    from datetime import UTC, datetime
    return {"epoch_id": "large_trade_push", "binding": binding or {"kind": "vendor4013", "code": "603258", "market": 17, "page_id": 10117},
        "received_at": datetime(2026, 10, 19, tzinfo=UTC), "request_id": int.from_bytes(body[2:6], "little"),
        "source_fd_ordinal": 1, "delivery": delivery, "response_body_hex": body.hex(),
        "raw_table_hex": result["raw_table"].hex(), "table": result["table"], "response_sha256": result["response_sha256"]}


@pytest.mark.parametrize("version", ["tb1.0", "tb3.0"])
@pytest.mark.parametrize("rows", [4096, 4097])
def test_known_trade_push_keeps_complete_rows_and_serialized_batch_bound(rows: int, version: str) -> None:
    from datetime import UTC, datetime
    from rquant.l2.models import RawL2Batch

    codec = module("_codec")
    body = worst_trade_push(rows, version)
    result = codec.decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10117, code="603258", market=17, source_fd_ordinal=1)
    batch = RawL2Batch.model_validate({**market_model_data(body, result), "local_batch_sequence": 1,
        "cached_at": datetime(2026, 10, 19, tzinfo=UTC)})
    serialized = batch.model_dump_json().encode() + b"\n"
    assert batch.table.row_count == len(batch.table.rows) == rows
    assert len(body) <= 1024 * 1024 and len(result["raw_table"]) <= 1024 * 1024
    assert len(serialized) < 8 * 1024 * 1024
    assert RawL2Batch.model_validate_json(serialized) == batch


@pytest.mark.parametrize("rows", [500, 501])
def test_trade_seed_keeps_original_500_row_limit(rows: int) -> None:
    codec = module("_codec")
    push = worst_trade_push(rows, "tb3.0")
    payload = push[26:]
    seed = struct.pack("<HIIHIII", 32, 3, 0x1110, 10117, len(payload), 2357, 0) + b"\0" * 8 + payload
    if rows > 500:
        with pytest.raises(ValueError, match="dimensions exceed bounds"):
            codec.decode_trade_response(seed, request_id=3, page_id=10117, code="603258", market=17, source_fd_ordinal=1)
    else:
        assert codec.decode_trade_response(seed, request_id=3, page_id=10117, code="603258", market=17, source_fd_ordinal=1)["table"]["row_count"] == rows


@pytest.mark.parametrize("rows", [500, 501])
@pytest.mark.parametrize("delivery", ["seed", "backfill", "candidate_push"])
def test_trade_model_expansion_requires_candidate_push_delivery(rows: int, delivery: str) -> None:
    from rquant.l2.models import MarketObservation
    codec = module("_codec")
    body = worst_trade_push(rows)
    result = codec.decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10117, code="603258", market=17, source_fd_ordinal=1)
    data = market_model_data(body, result, delivery=delivery)
    if rows > 500 and delivery != "candidate_push":
        with pytest.raises(ValueError, match="market row count exceeds profile limit"):
            MarketObservation.model_validate(data)
    else:
        assert MarketObservation.model_validate(data).table.row_count == rows


def test_seed_envelope_model_cannot_claim_expanded_candidate_push() -> None:
    import hashlib
    from rquant.l2.models import MarketObservation
    codec = module("_codec")
    push = worst_trade_push(501, "tb3.0")
    result = codec.decode_trade_response(push, request_id=0xFFFFFFFF, page_id=10117, code="603258", market=17, source_fd_ordinal=1)
    seed = struct.pack("<HIIHIII", 32, 3, 0x1110, 10117, len(push) - 26, 2357, 0) + b"\0" * 8 + push[26:]
    result["response_sha256"] = hashlib.sha256(seed).hexdigest()
    with pytest.raises(ValueError, match="market row count exceeds profile limit"):
        MarketObservation.model_validate(market_model_data(seed, result))


def test_expanded_order_model_keeps_order_kind_and_side() -> None:
    from rquant.l2.models import MarketObservation
    codec = module("_codec")
    seed = response(trade=False, request_id=3, page_id=10117, ids=tuple(range(1, 502)))
    body = struct.pack("<HIIHIII", 26, 0xFFFFFFFF, 0x1100, 10117, len(seed) - 32, 2357, 0) + b"\0\0" + seed[32:]
    raw = codec.snappy_decompress(body[26:])
    result = codec._response_result(body, raw, codec._table_metadata(raw, version="tb1.0", max_rows=4096),
        request_id=0xFFFFFFFF, page_id=10117, source_fd_ordinal=1)
    binding = {"kind": "vendor4027", "code": "603258", "market": 17, "page_id": 10117, "side": "buy"}
    assert MarketObservation.model_validate(market_model_data(body, result, binding=binding)).table.row_count == 501
    with pytest.raises(ValueError, match="expanded order side mismatch"):
        MarketObservation.model_validate(market_model_data(body, result, binding={**binding, "side": "sell"}))


def test_raw_table_keeps_logical_byte_budget_before_rows() -> None:
    from rquant.l2.models import RawTable
    codec = module("_codec")
    body = worst_trade_push(2)
    raw = codec.snappy_decompress(body[26:])
    result = codec._response_result(body, raw, codec._table_metadata(raw, version="tb1.0", max_rows=4097),
        request_id=0xFFFFFFFF, page_id=10117, source_fd_ordinal=1)
    with pytest.raises(ValueError, match="table geometry exceeds byte bounds"):
        RawTable.model_validate({**result["table"], "row_count": (1 << 20) // 66 + 1})


@pytest.mark.parametrize("rows", [500, 501])
@pytest.mark.parametrize("target", ["model", "cache"])
@pytest.mark.parametrize("fault", ["flags", "widths", "extra"])
def test_expanded_trade_rejects_unrecognized_descriptor(tmp_path: Path, rows: int, target: str, fault: str) -> None:
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import Binding, MarketObservation, RawCell, RawColumn, RawTable

    codec = module("_codec")
    original = worst_trade_push(rows)
    raw = bytearray(codec.snappy_decompress(original[26:]))
    if fault == "flags":
        struct.pack_into("<H", raw, 24, 0x2002)
    elif fault == "widths":
        raw[26], raw[24 + 6 * 4 + 2] = 3, 5
    else:
        raw[27] = 164
    payload = literal(bytes(raw))
    body = struct.pack("<HIIHIII", 26, 0xFFFFFFFF, 0x1100, 10117, len(payload), 2357, 0) + b"\0\0" + payload
    with pytest.raises(ValueError, match="unsupported trade table schema"):
        codec.decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10117, code="603258", market=17, source_fd_ordinal=1)
    result = codec._response_result(body, bytes(raw), codec._table_metadata(bytes(raw), version="tb1.0", max_rows=4096),
        request_id=0xFFFFFFFF, page_id=10117, source_fd_ordinal=1)
    data = market_model_data(body, result)
    cache = RawL2Cache(tmp_path / "unknown-descriptor")
    with cache.writer():
        if target == "model":
            action = lambda: MarketObservation.model_validate(data)
        else:
            # Exercise append's own RawL2Batch revalidation independently of
            # observation construction, with valid typed cells and raw hashes.
            typed = RawTable.model_construct(**{**data["table"],
                "columns": tuple(RawColumn.model_validate(c) for c in data["table"]["columns"]),
                "rows": tuple({k: RawCell.model_validate(v) for k, v in row.items()}
                              for row in data["table"]["rows"])})
            unchecked = MarketObservation.model_construct(**{**data,
                "binding": Binding.model_validate(data["binding"]), "table": typed})
            action = lambda: cache.append(unchecked)
        if rows > 500:
            with pytest.raises(ValueError, match="table descriptor"):
                action()
            assert cache.read_status().last_sequence == 0
        else:
            accepted = action()
            assert accepted.table.row_count == 500
            assert cache.read_status().last_sequence == (1 if target == "cache" else 0)


def order_push(rows: int, side: str = "buy") -> bytes:
    seed = response(trade=False, request_id=3, page_id=10117, side=side, ids=tuple(range(rows)))
    payload = seed[32:]
    return struct.pack("<HIIHIII", 26, 0xFFFFFFFF, 0x1100, 10117, len(payload), 2357, 0) + b"\0\0" + payload


def zero_rle_trade(rows: int) -> bytes:
    codec = module("_codec")
    raw = codec.snappy_decompress(worst_trade_push(2)[26:])
    ext = int.from_bytes(raw[18:20], "little")
    header = bytearray(raw[:ext])
    header[:6] = b"tb3.0\0"
    struct.pack_into("<i", header, 6, rows)
    logical = rows * 66
    packed = bytearray((255, 0, 0))
    remaining = logical - 13
    while remaining >= 32767:
        packed.extend(b"\xff\xff")
        remaining -= 32767
    packed.extend(bytes((remaining,)) if remaining <= 127 else bytes((128 + (remaining >> 8), remaining & 255)))
    assert module("_table")._decode_rle(bytes(packed), logical) == bytes(logical)
    data = bytes(header) + struct.pack("<I", len(packed) + 4) + struct.pack(">I", logical) + packed
    payload = literal(data)
    return struct.pack("<HIIHIII", 26, 0xFFFFFFFF, 0x1100, 10117, len(payload), 2357, 0) + b"\0\0" + payload


@pytest.mark.parametrize("rows", [4097, 8192])
@pytest.mark.parametrize("version", ["tb1.0", "tb3.0"])
def test_byte_envelope_trade_uses_geometry_not_fixed_count(rows, version):
    from rquant.l2.models import MarketObservation
    codec = module("_codec")
    body = worst_trade_push(rows, version)
    result = codec.decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10117,
        code="603258", market=17, source_fd_ordinal=1)
    model = MarketObservation.model_validate(market_model_data(body, result))
    assert model.table.row_count == len(model.table.rows) == rows
    assert model.response_body_hex == body.hex()
    assert model.table.source_sha256 == result["table"]["source_sha256"]


@pytest.mark.parametrize("side", ["buy", "sell"])
def test_byte_envelope_order_uses_known_profile_and_geometry(side):
    from rquant.l2.models import MarketObservation
    codec = module("_codec")
    body = order_push(501, side)
    result = codec.decode_order_response(body, request_id=0xFFFFFFFF, page_id=10117,
        code="603258", source_fd_ordinal=1)
    binding = {"kind": "vendor4027", "code": "603258", "market": 17, "side": side, "page_id": 10117}
    model = MarketObservation.model_validate(market_model_data(body, result, binding=binding))
    assert model.table.row_count == len(model.table.rows) == 501
    assert {cell["12"].value for cell in model.table.rows} == {1 if side == "buy" else 2}


def test_byte_envelope_compressed_logical_boundary_and_preallocation(monkeypatch):
    from rquant.l2.models import MarketObservation
    codec = module("_codec")
    body = zero_rle_trade((1 << 20) // 66)
    result = codec.decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10117,
        code="603258", market=17, source_fd_ordinal=1)
    model = MarketObservation.model_validate(market_model_data(body, result))
    assert model.table.row_count == 15887 and len(body) < 512
    invalid = zero_rle_trade(15888)
    monkeypatch.setattr(codec, "parse_table", lambda *args: pytest.fail("row parser reached"))
    with pytest.raises(ValueError, match="dimensions exceed bounds"):
        codec.decode_trade_response(invalid, request_id=0xFFFFFFFF, page_id=10117,
            code="603258", market=17, source_fd_ordinal=1)


@pytest.mark.parametrize("fault", ["stride", "count", "length", "descriptor", "logical", "header"])
def test_byte_envelope_model_shape_precedes_nested_cell_validation(fault):
    from pydantic import field_validator
    from rquant.l2.models import RawTable
    codec = module("_codec")
    body = worst_trade_push(501)
    result = codec._response_result(body, codec.snappy_decompress(body[26:]),
        codec._table_metadata(codec.snappy_decompress(body[26:]), version="tb1.0", max_rows=8192),
        request_id=0xFFFFFFFF, page_id=10117, source_fd_ordinal=1)

    class NoCellAllocation(RawTable):
        @field_validator("rows", mode="before")
        @classmethod
        def prohibit_rows(cls, value):
            raise AssertionError("nested cell allocation reached")

    with pytest.raises(ValueError, match="nested cell allocation"):
        NoCellAllocation.model_validate(result["table"])
    data = dict(result["table"])
    if fault == "stride": data["row_width"] = 1
    elif fault == "count": data["col_count"] = 32
    elif fault == "length": data["rows"] = data["rows"][:-1]
    elif fault == "descriptor":
        data["columns"] = [dict(c) for c in data["columns"]]
        data["columns"][0]["extra"] = 164
    elif fault == "header": data["extended_header_length"] = 23
    else: data["row_count"] = 15888
    with pytest.raises(ValueError, match="table (geometry|shape|descriptor)"):
        NoCellAllocation.model_validate(data)


@pytest.mark.parametrize("fault", ["stride", "count", "descriptor", "logical", "header", "duplicate"])
def test_byte_envelope_codec_metadata_precedes_row_parser(fault, monkeypatch):
    from unittest.mock import Mock
    codec = module("_codec")
    body = order_push(501)
    row_parser = Mock(side_effect=ValueError("parser sentinel"))
    monkeypatch.setattr(codec, "parse_table", row_parser)
    with pytest.raises(ValueError, match="invalid market table layout"):
        codec.decode_order_response(body, request_id=0xFFFFFFFF, page_id=10117, code="603258", source_fd_ordinal=1)
    assert row_parser.call_count == 1
    row_parser.reset_mock()
    raw = bytearray(codec.snappy_decompress(body[26:]))
    if fault == "stride": struct.pack_into("<H", raw, 20, 1)
    elif fault == "count": struct.pack_into("<H", raw, 22, 32)
    elif fault == "descriptor": raw[27] = 219
    elif fault == "header": struct.pack_into("<H", raw, 18, 23)
    elif fault == "duplicate": raw[24:26] = raw[28:30]
    else: struct.pack_into("<i", raw, 6, (1 << 20) // 30 + 1)
    payload = literal(bytes(raw))
    invalid = struct.pack("<HIIHIII", 26, 0xFFFFFFFF, 0x1100, 10117, len(payload), 2357, 0) + b"\0\0" + payload
    with pytest.raises(ValueError):
        codec.decode_order_response(invalid, request_id=0xFFFFFFFF, page_id=10117, code="603258", source_fd_ordinal=1)
    row_parser.assert_not_called()


@pytest.mark.parametrize("fault", ["seed", "backfill", "ordinary_id", "wrong_page"])
def test_byte_envelope_context_precedes_nested_table_allocation(fault):
    from pydantic import field_validator
    from rquant.l2.models import MarketObservation
    codec = module("_codec")
    body = worst_trade_push(501)
    result = codec.decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10117,
        code="603258", market=17, source_fd_ordinal=1)
    data = market_model_data(body, result)

    class NoTableAllocation(MarketObservation):
        @field_validator("table", mode="before")
        @classmethod
        def prohibit_table(cls, value):
            raise AssertionError("nested table allocation reached")

    with pytest.raises(ValueError, match="nested table allocation"):
        NoTableAllocation.model_validate(data)
    if fault in {"seed", "backfill"}: data["delivery"] = fault
    elif fault == "ordinary_id": data["request_id"] = 3
    else: data["binding"] = {**data["binding"], "page_id": 10118}
    with pytest.raises(ValueError) as error:
        NoTableAllocation.model_validate(data)
    assert "nested table allocation" not in str(error.value)


@pytest.mark.parametrize("delivery", ["seed", "backfill"])
def test_byte_envelope_revalidated_model_copy_keeps_context_guard(delivery):
    from rquant.l2.models import MarketObservation
    codec = module("_codec")
    body = worst_trade_push(501)
    result = codec.decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10117,
        code="603258", market=17, source_fd_ordinal=1)
    model = MarketObservation.model_validate(market_model_data(body, result))
    with pytest.raises(ValueError, match="market row count exceeds profile limit"):
        MarketObservation.model_validate(model.model_copy(update={"delivery": delivery}))


def test_byte_envelope_known_value_and_header_extremes():
    from rquant.l2._table import decode_packed32
    from rquant.l2.models import MarketObservation
    longest = 0
    for mantissa in (0, 1, 0x07FFFFFF):
        for exponent in range(8):
            for sign in (0, 0x08000000):
                for exponent_sign in (0, 0x80000000):
                    raw = struct.pack("<I", mantissa | exponent << 28 | sign | exponent_sign)
                    value = decode_packed32(raw)
                    longest = max(longest, len(value or "null"))
    assert longest == 17
    assert decode_packed32(b"\xff" * 4) is None
    codec = module("_codec")
    body = worst_trade_push(2)
    raw = codec.snappy_decompress(body[26:])
    original_ext = int.from_bytes(raw[18:20], "little")
    header = bytearray(raw[:56])
    struct.pack_into("<H", header, 18, 65535)
    # Empty trade title, one maximal bounded UTF16 column label, seven empty
    # labels and one padding byte. Labels are retained in raw bytes, not DTOs.
    name_units = (65535 - 56 - 2 - 2 - 14) // 2
    header.extend(b"\0\0" + struct.pack("<H", name_units) + b"\x01\x00" * name_units + b"\0\0" * 7)
    header.extend(b"\0" * (65535 - len(header)))
    payload = literal(bytes(header) + raw[original_ext:])
    large_header = struct.pack("<HIIHIII", 26, 0xFFFFFFFF, 0x1100, 10117, len(payload), 2357, 0) + b"\0\0" + payload
    result = codec.decode_trade_response(large_header, request_id=0xFFFFFFFF, page_id=10117,
        code="603258", market=17, source_fd_ordinal=1)
    model = MarketObservation.model_validate(market_model_data(large_header, result))
    assert model.table.extended_header_length == 65535 and model.table.row_count == 2
    assert model.raw_table_hex == result["raw_table"].hex()
