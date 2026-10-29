from __future__ import annotations

import hashlib
import json
import struct
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from pathlib import Path

import pytest


# First market-only record in evidence/rquant-l2-live-push4013-observed.json.
OBSERVED_TRADE_PUSH = bytes.fromhex(
    "1a00ffffffff00110000852737010000350900000000000000ffe602e07462332e30000c000000"
    "ffffffff000000009a0042000800012004a538001ea50a1004a50d1004a54a1004a54b1004a5"
    "10a604a5121004a500012b320100d806000400060036003000330032003500380022a6110000"
    "00078d02003100370004a20100000008a5c409000097011000320034003100330d0618350030"
    "002e00301d0200c8055ea803180398a1aaaef32223242526272f1faf2021872213f500fd0b30"
    "f500f539f500f53af500f533f500f5310d0c2035f500f538f500f520660400f070fd0b35ab37"
    "1f383606d700d7a0d702d56428f464bc8464e990000100020347000100f5a0f572ae6a1e72e1"
    "b8727ae808e63ffa0dfc000bb5bb0abfc25da79bcabfe973153e853f1b31323cf8fc400ddc0c7"
    "f0d00df0b8e5f7e5fd15f6a5f00013738393a3b3c3d3e3f4041427d227e0017"
)


def observed_trade_push_data(body=OBSERVED_TRADE_PUSH):
    from rquant.l2._codec import _table_metadata, snappy_decompress
    from rquant.l2._table import parse_table

    raw_table = snappy_decompress(body[26:])
    version = raw_table[:6].rstrip(b"\0").decode("ascii")
    table = parse_table(raw_table, _table_metadata(raw_table, version=version, max_rows=500))
    table.pop("selected_frame")
    table.pop("selected_frames")
    return {
        "epoch_id": "observed_market_push",
        "binding": {"page_id": 10117, "kind": "vendor4013", "code": "603258", "market": 17},
        "received_at": datetime(2026, 10, 16, 1, 32, tzinfo=UTC),
        "request_id": 0xFFFFFFFF, "source_fd_ordinal": 1, "delivery": "candidate_push",
        "response_body_hex": body.hex(), "raw_table_hex": raw_table.hex(),
        "response_sha256": hashlib.sha256(body).hexdigest(), "table": table,
    }


def test_date_validators_do_not_run_on_raw_cell_fields():
    import sys
    from pydantic import TypeAdapter
    from rquant.l2.models import Model, RawCell

    date_callbacks = {Model.reject_numeric_datetime.__func__.__code__, Model.require_utc.__func__.__code__}
    calls = []
    previous = sys.getprofile()
    def record(frame, event, arg):
        if event == "call" and frame.f_code in date_callbacks:
            calls.append(frame.f_code.co_name)
    try:
        sys.setprofile(record)
        adapter = TypeAdapter(RawCell)
        cell = adapter.validate_python({"value": "123", "raw_hex": "7b000000", "style": 255})
    finally:
        sys.setprofile(previous)
    assert adapter.dump_python(cell) == {"value": "123", "raw_hex": "7b000000", "style": 255}
    assert calls == []
    for valid in (None, 1, "1"):
        assert adapter.dump_python(adapter.validate_python({"value": valid, "raw_hex": "00", "style": 0}))["value"] == valid
    for invalid in ({"value": True}, {"value": False}, {"value": b"1"}, {"raw_hex": "xyz"},
                    {"style": 256}, {"style": -1}, {"style": True}, {"extra": 1}):
        with pytest.raises(ValueError):
            adapter.validate_python({"value": "123", "raw_hex": "7b000000", "style": 255, **invalid})


def test_raw_cell_is_closed_and_cannot_acquire_unvalidated_fields():
    from dataclasses import FrozenInstanceError, dataclass
    from pydantic import TypeAdapter
    from rquant.l2.models import RawCell

    adapter = TypeAdapter(RawCell)
    data = {"value": "123", "raw_hex": "7b000000", "style": 255}
    cell = adapter.validate_python(data)
    assert RawCell(**data) == cell
    assert adapter.dump_python(cell) == data
    assert adapter.dump_python(adapter.validate_python(cell)) == data
    assert adapter.dump_python(adapter.validate_json(json.dumps(data))) == data
    with pytest.raises(TypeError, match="closed three-field"):
        class DerivedCell(RawCell):
            pass
    with pytest.raises(TypeError, match="closed three-field"):
        @dataclass(frozen=True, slots=True)
        class ExtraCell(RawCell):
            unknown: int = 1
    assert not hasattr(cell, "__dict__")
    with pytest.raises(AttributeError):
        object.__setattr__(cell, "unknown", 1)
    with pytest.raises(FrozenInstanceError):
        cell.value = "changed"


@pytest.mark.parametrize("field,value", [
    ("value", True), ("value", False), ("value", b"1"),
    ("style", True), ("raw_hex", "xyz"),
])
def test_raw_cell_forged_instance_is_revalidated_before_commit(tmp_path, field, value):
    from pydantic import TypeAdapter
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import MarketObservation, RawCell

    original = order640_observation()
    cache = RawL2Cache(tmp_path / "forged-cell")
    with cache.writer():
        cache.append(original)
        cell = original.table.rows[0]["1"]
        object.__setattr__(cell, field, value)
        with pytest.raises(ValueError):
            TypeAdapter(RawCell).validate_python(cell)
        with pytest.raises(ValueError):
            MarketObservation.model_validate(original.model_dump(mode="python", serialize_as_any=True))
        with pytest.raises(ValueError):
            cache.append(original)
        assert cache.read_status().last_sequence == 1
        assert not list((cache.root / "batches").glob(".pending-*"))


@pytest.mark.parametrize("field", ["received_at", "cached_at", "last_transport_received_at",
                                   "last_market_received_at", "seed_received_at"])
@pytest.mark.parametrize("value,valid", [
    (datetime(2026, 10, 19, tzinfo=UTC), True),
    ("2026-10-19T00:00:00Z", True),
    (0, False), (0.0, False),
    (datetime(2026, 10, 19), False),
    (datetime(2026, 10, 19, tzinfo=timezone(timedelta(hours=8))), False),
    (None, True),
])
def test_date_validators_preserve_all_current_datetime_fields(field, value, valid):
    from rquant.l2.models import FeedStatus, MarketObservation, RawL2Batch, SubscriptionStream

    if field in {"received_at", "cached_at"}:
        data = observed_trade_push_data()
        model = MarketObservation if field == "received_at" else RawL2Batch
        if field == "cached_at":
            data.update(local_batch_sequence=1, cached_at=datetime(2026, 10, 19, tzinfo=UTC))
        data[field] = value
        valid = valid and value is not None
    elif field == "seed_received_at":
        model = SubscriptionStream
        data = {"binding": {"page_id": 10117, "kind": "vendor4013", "code": "603258", "market": 17},
                "state": "sent" if value is None else "seeded", "request_id": 3,
                "seed_sequence": None if value is None else 1, field: value}
    else:
        model, data = FeedStatus, {field: value}
    if valid:
        assert getattr(model.model_validate(data), field) == (None if value is None else datetime(2026, 10, 19, tzinfo=UTC))
    else:
        with pytest.raises(ValueError):
            model.model_validate(data)


def test_market_model_observed_h26_push_retains_bytes_and_cache_roundtrip(tmp_path: Path):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import MarketObservation

    data = observed_trade_push_data()
    assert data["response_sha256"] == "c1a4f231a176348970c7534199d72ae663b6f668bcbf4fbfc6afe0c43f2de703"
    assert data["table"]["source_sha256"] == "f40e5a8a7ccb9b814480a2c5f6c5f38bd7b92f4c8e3ff367e0d9b1534ffccee9"
    assert [row["18"]["value"] for row in data["table"]["rows"]] == [str(n) for n in range(8759, 8771)]
    observed = MarketObservation.model_validate(data)
    assert MarketObservation.model_validate_json(observed.model_dump_json()) == observed
    cache = RawL2Cache(tmp_path / "observed-push")
    with cache.writer():
        batch = cache.append(observed)
    restored = cache.read_batches().batches[0]
    assert restored == batch
    assert restored.response_body_hex == OBSERVED_TRADE_PUSH.hex()
    assert restored.raw_table_hex == data["raw_table_hex"]
    assert restored.request_id == 0xFFFFFFFF
    assert struct.unpack_from("<H", bytes.fromhex(restored.response_body_hex))[0] == 26


@pytest.mark.parametrize("header,request_id,flags,page,command,size_delta", [
    (26, 3, 0x1100, 10117, 2357, 0),
    (26, 0xFFFFFFFD, 0x1100, 10117, 2357, 0),
    (26, 0xFFFFFFFF, 0x1110, 10117, 2357, 0),
    (26, 0xFFFFFFFF, 0x1100, 10118, 2357, 0),
    (26, 0xFFFFFFFF, 0x1100, 0, 2357, 0),
    (26, 0xFFFFFFFF, 0x1100, 10117, 2267, 0),
    (26, 0xFFFFFFFF, 0x1100, 10117, 2357, 1),
    (26, 0xFFFFFFFD, 11, 0, 0, 0),
    (32, 0xFFFFFFFF, 0x1100, 10117, 2357, 0),
    (25, 0xFFFFFFFF, 0x1100, 10117, 2357, 0),
])
def test_market_model_h26_profile_rejects_other_envelopes(header, request_id, flags, page, command, size_delta):
    from rquant.l2.models import MarketObservation

    data = observed_trade_push_data()
    payload = OBSERVED_TRADE_PUSH[26:]
    body = (struct.pack("<HIIHII", header, request_id, flags, page, len(payload) + size_delta, command)
            + b"\0" * (header - 20) + payload)
    data.update(request_id=request_id, response_body_hex=body.hex(), response_sha256=hashlib.sha256(body).hexdigest())
    with pytest.raises(ValueError, match="market record association mismatch"):
        MarketObservation.model_validate(data)


def test_market_model_h26_keeps_response_and_table_hash_checks():
    from rquant.l2.models import MarketObservation

    for field in ("response_body_hex", "raw_table_hex"):
        data = observed_trade_push_data()
        raw = bytes.fromhex(data[field])
        data[field] = (raw[:-1] + bytes([raw[-1] ^ 1])).hex()
        with pytest.raises(ValueError, match="market source hash mismatch"):
            MarketObservation.model_validate(data)



# Root-filtered public common2357 order seed/push fixtures; no session data.
COMMON_ORDER_FIXTURES = (
    {
        "body": bytes.fromhex(
        "20000400000000110000862773010000350900000000000000ff000003000000d005f0497462312e"
        "300014000000ffffffff0000000078001e000600012004dc0a1004dc0d1004dc0c2004dc382004dc"
        "61a504dc0b003600300033003200350038002d001090147bd45958620000013c090110020004000c"
        "322800010104a67501060c000008a5016e5c7ec75b000f1a3b00b0020c0000000f010000000ae18b"
        "d16a090a1488ce5b000f38011e0119091400e3151e04eadc113c014a091e00e7151e043ce2113c00"
        "05155a00e9151e0097011e0024015a000f4a1e0004d3e3115a00224a1e001409e65b000f56013c19"
        "5a00ea155a0483e9111e04040011d200eb151e00d6761e0000ee11b40003153c00ec153c14c9f55b"
        "000f2e01780002151e00ee151e00e3011e254a000a4a1e0004eaf711d2394a193c045df8117819b4"
        "191e0449fa111e193c00ef15780846055c0d9604070011f000f1151e00d8011e2568193c00f2151e"
        "083e065c2dfe394a191e00b4011e053c1996191e20e30a5c000f423b00b01d5a24f38bd16a0f0100"
        "00000a"
        ),
        "response_sha256": "306a9bacc34dd439bc57b0d88ed3ae1e7e2d458f2c5482839583dd7552efb7df",
        "table_sha256": "0b0f1c568a80b8ec9c2344b9f62cad7c3b4101d3abf9faba850785ff77a75798",
        "table_json_sha256": "3c615b7fdfcb52887e7787b07cbbdf9cb17eeeaf847b94a84d7c7c624c27c5e5",
        "row_count": 20, "request_id": 4, "source_fd_ordinal": 7,
        "received_at": "2026-10-16T02:29:09.886367+00:00", "delivery": "seed",
    },
    {
        "body": bytes.fromhex(
        "1a00ffffffff00110000862797000000350900000000000000ffb401f0497462312e300002000000"
        "ffffffff0000000078001e000600012004dc0a1004dc0d1004dc0c2004dc382004dc61a504dc0b00"
        "3600300033003200350038002d001090147bd45958620000013c090110020004000c32280001010c"
        "e0d6982001080408a5016e5c8b155c000f603b00b0020b0000000f010000000af68bd16a090a00a0"
        "151e0119091424f68bd16a0f010000000a"
        ),
        "response_sha256": "dcf80aa2cb2695f8f7f485abecd059f5160720be55e85b78fc0e2852d32912cb",
        "table_sha256": "08580a5755d993cf5990a4fbbb57c35af417e2f5cd11fff48fc698774a33e8fa",
        "table_json_sha256": "ec39d1c2a045eea3925b05a1ceddcb7d0e38c4005926000e74a67dea36962d19",
        "row_count": 2, "request_id": 4294967295, "source_fd_ordinal": 9,
        "received_at": "2026-10-16T02:29:10.874371+00:00", "delivery": "candidate_push",
    },
)


def common_order_data(index=0):
    from rquant.l2._codec import _table_metadata, snappy_decompress
    from rquant.l2._table import parse_table

    fixture = COMMON_ORDER_FIXTURES[index]
    body = fixture["body"]
    raw_table = snappy_decompress(body[int.from_bytes(body[:2], "little"):])
    table = parse_table(raw_table, _table_metadata(raw_table, version="tb1.0", max_rows=500))
    table.pop("selected_frame")
    table.pop("selected_frames")
    return {
        "epoch_id": "public_common_frame_fixture",
        "binding": {"page_id": 10118, "kind": "vendor4027", "code": "603258", "market": 17, "side": "buy"},
        "received_at": datetime.fromisoformat(fixture["received_at"]), "request_id": fixture["request_id"],
        "source_fd_ordinal": fixture["source_fd_ordinal"], "delivery": fixture["delivery"],
        "response_body_hex": body.hex(), "raw_table_hex": raw_table.hex(),
        "response_sha256": fixture["response_sha256"], "table": table,
    }


@pytest.mark.parametrize("index", [0, 1])
def test_common_frame_order_market_model_and_cache_preserve_complete_fixture(tmp_path: Path, index):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import MarketObservation

    fixture, data = COMMON_ORDER_FIXTURES[index], common_order_data(index)
    assert hashlib.sha256(fixture["body"]).hexdigest() == fixture["response_sha256"]
    assert data["table"]["source_sha256"] == fixture["table_sha256"]
    assert data["table"]["row_count"] == fixture["row_count"]
    assert hashlib.sha256(json.dumps(data["table"], ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest() == fixture["table_json_sha256"]
    observed = MarketObservation.model_validate(data)
    assert MarketObservation.model_validate_json(observed.model_dump_json()) == observed
    typed_table = json.loads(observed.table.model_dump_json())
    typed_table.pop("rle_algorithm_reference")
    assert hashlib.sha256(json.dumps(typed_table, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest() == fixture["table_json_sha256"]
    cache = RawL2Cache(tmp_path / "common2357")
    with cache.writer():
        batch = cache.append(observed)
    restored = cache.read_batches().batches[0]
    assert restored == batch
    assert restored.response_body_hex == fixture["body"].hex()
    assert struct.unpack_from("<I", bytes.fromhex(restored.response_body_hex), 16)[0] == 2357
    assert restored.raw_table_hex == data["raw_table_hex"]


@pytest.mark.parametrize("offset,fmt,value", [
    (6, "<I", 0x1110), (6, "<I", 0x1130),
    (16, "<I", 2358), (2, "<I", 5), (10, "<H", 10119), (12, "<I", 0),
])
def test_common_frame_order_market_model_rejects_wrong_association_and_trade_flags(offset, fmt, value):
    from rquant.l2.models import MarketObservation

    data = common_order_data()
    body = bytearray.fromhex(data["response_body_hex"])
    struct.pack_into(fmt, body, offset, value)
    data.update(response_body_hex=body.hex(), response_sha256=hashlib.sha256(body).hexdigest())
    with pytest.raises(ValueError, match="market record association mismatch"):
        MarketObservation.model_validate(data)


@pytest.mark.parametrize("index", [0, 1])
def test_common_frame_order_market_model_rejects_trade_binding(index):
    from rquant.l2.models import MarketObservation

    data = common_order_data(index)
    data["binding"] = {**data["binding"], "kind": "vendor4013", "side": None}
    with pytest.raises(ValueError):
        MarketObservation.model_validate(data)


def test_market_model_keeps_actual_tb1_trade_push():
    from rquant.l2.models import MarketObservation

    body = bytes.fromhex(
    "1a00ffffffff001100008527d5000000350900000000000000ff9e02e07462312e300002000000ff"
    "ffffff000000009a0042000800012004a538001ea50a1004a50d1004a54a1004a54b1004a510a604"
    "a5121004a500012b320100d806000400060036003000330032003500380022a611000000078d0200"
    "3100370004a20100000008a5c409000097011000320034003100330d0618350030002e00301d0254"
    "d0353d0000300039003a00340034003a0030003100203202008000170600a002900100a06ad19927"
    "000091922700006181d16a004c520000001d369a420018bc0200a06a029a324200104d52000000"
    )
    data = observed_trade_push_data(body)
    assert data["response_sha256"] == "a6dcb8b6a098ff8a39ee9ad84e35945ccf04715cc87b78d9cd6afe3cec2eb783"
    assert data["table"]["source_sha256"] == "35cc2ae8a112160c419d4294d234b4b21b96088cd9b1ee803876324d696fb364"
    observed = MarketObservation.model_validate(data)
    assert observed.table.table_version == "tb1.0" and observed.table.row_count == 2
    assert MarketObservation.model_validate_json(observed.model_dump_json()) == observed


def observation():
    from rquant.l2.models import Binding, MarketObservation, RawTable

    flags = (0x2001, 0x100A, 0x100D, 0x200C, 0x2038, 0xA561)
    values = (100, 1681, 2, 1, 1792047599, 1)
    row_bytes = b"".join(struct.pack("<IB", value, 0) for value in values)
    row = {
        str(flag & 0x8FFF): {"value": str(value) if flag & 0x7000 == 0x1000 else value,
                            "raw_hex": struct.pack("<I", value).hex(), "style": 0}
        for flag, value in zip(flags, values, strict=True)
    }
    title = "603258-逐笔委托"
    strings = struct.pack("<H", len(title)) + title.encode("utf-16le") + b"\0\0" * 6
    ext = 24 + 24 + len(strings)
    table = (b"tb1.0\0" + struct.pack("<iiiHHH", 1, -1, 0, ext, 30, 6)
             + b"".join(struct.pack("<HBB", flag, 4, 8) for flag in flags)
             + strings + row_bytes)
    columns = [{"field_id": flag & 0x8FFF, "flags": flag, "flags_hex": hex(flag),
                "width": 4, "extra": 8,
                "encoding": "packed32-le" if flag & 0x7000 == 0x1000 else "unsigned-le"}
               for flag in flags]
    payload = b"synthetic-market-payload"
    body = struct.pack("<HIIHIII", 32, 3, 0x1100, 10113, len(payload), 2267, 0) + b"\0" * 8 + payload
    return MarketObservation(
        epoch_id="synthetic_epoch", binding=Binding(page_id=10113, kind="vendor4027",
            code="603258", market=17, side="buy"), received_at=datetime(2026, 10, 15, 7, tzinfo=UTC),
        request_id=3, source_fd_ordinal=4, delivery="seed", response_body_hex=body.hex(),
        raw_table_hex=table.hex(), response_sha256=hashlib.sha256(body).hexdigest(),
        table=RawTable(title=title, table_version="tb1.0", source_codec="snappy",
            source_sha256=hashlib.sha256(table).hexdigest(),
            row_data_sha256=hashlib.sha256(row_bytes).hexdigest(), row_count=1, row_width=30,
            col_count=6, extended_header_length=ext, header_unknown_10=-1, header_unknown_14=0,
            columns=columns, rows=[row], interpretation_limits="vendor semantics unverified"),
    )


def test_market_model_retains_raw_cells_and_json_roundtrip():
    from rquant.l2.models import MarketObservation

    data = observation()
    restored = MarketObservation.model_validate_json(data.model_dump_json())
    assert restored == data
    assert restored.table.rows[0]["13"].value == "2"
    assert restored.table.rows[0]["13"].raw_hex == "02000000"


def test_binding_rejects_mismatched_market_and_side():
    from rquant.l2.models import Binding

    with pytest.raises(ValueError):
        Binding(page_id=1, kind="vendor4013", code="603258", market=33)
    with pytest.raises(ValueError):
        Binding(page_id=1, kind="vendor4027", code="603258", market=17)


def test_market_model_rejects_control_header_unknown_fields_and_bad_raw_hash():
    from rquant.l2.models import MarketObservation

    data = observation().model_dump()
    with pytest.raises(ValueError):
        MarketObservation.model_validate({**data, "token": "synthetic_secret"})
    with pytest.raises(ValueError):
        MarketObservation.model_validate({**data, "raw_table_hex": "00"})
    with pytest.raises(ValueError):
        MarketObservation.model_validate({**data, "response_body_hex": "00" * 131})
    with pytest.raises(ValueError):
        MarketObservation.model_validate({**data, "received_at": datetime(2026, 10, 15)})


def test_cache_constructor_and_unconfigured_reads_create_nothing(tmp_path: Path):
    from rquant.l2.cache import RawL2Cache

    root = tmp_path / "absent"
    cache = RawL2Cache(root)
    assert not root.exists()
    assert cache.read_status().state == "unconfigured"
    assert cache.read_batches().batches == ()
    assert not root.exists()


def test_writer_is_required_and_second_writer_is_refused(tmp_path: Path):
    from rquant.l2.cache import CacheError, RawL2Cache

    cache = RawL2Cache(tmp_path / "cache")
    with pytest.raises(CacheError, match="writer_required"):
        cache.append(observation())
    with cache.writer():
        with pytest.raises(CacheError, match="writer_busy"):
            with RawL2Cache(cache.root).writer():
                pytest.fail("second writer entered")


def test_identical_observations_remain_distinct_and_restart_sequence(tmp_path: Path):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import FeedStatus

    root = tmp_path / "cache"
    cache = RawL2Cache(root)
    data = observation()
    with cache.writer():
        assert cache.append(data).local_batch_sequence == 1
        assert cache.append(data).local_batch_sequence == 2
        cache.write_status(FeedStatus(state="stopped"))
    with RawL2Cache(root).writer() as reopened:
        assert reopened.append(data).local_batch_sequence == 3
    page = cache.read_batches(after_sequence=1, limit=2)
    assert [b.local_batch_sequence for b in page.batches] == [2, 3]
    assert page.batches[0].table.rows == page.batches[1].table.rows
    assert page.next_sequence == 3


def test_writer_retains_only_received_at_after_caller_releases_batch(tmp_path: Path, monkeypatch):
    import weakref
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import FeedStatus, RawL2Batch

    cache = RawL2Cache(tmp_path / "writer-receipt")
    created = []
    validate = RawL2Batch.model_validate
    def counted(value, **kwargs):
        batch = validate(value, **kwargs)
        created.append(weakref.ref(batch))
        return batch
    monkeypatch.setattr(RawL2Batch, "model_validate", counted)
    data = observation()
    with cache.writer():
        batch = cache.append(data)
        assert created[-1]() is batch
        assert batch.table == data.table
        reference, table = weakref.ref(batch), weakref.ref(batch.table)
        del batch
        assert reference() is None and table() is None
        assert cache._position() == (1, data.received_at)
        cache.write_status(FeedStatus(state="connected"))
        status = cache.read_status(now=data.received_at)
        assert status.last_sequence == 1
        assert status.last_market_received_at == data.received_at
        assert cache._last_received_at == data.received_at
    assert cache._last_received_at is None
    assert cache._writer_fd is None and cache._highwater == 0
    assert cache.read_status(now=data.received_at).last_market_received_at == data.received_at


def test_writer_recovery_releases_validated_batches_and_retains_last_time(tmp_path: Path, monkeypatch):
    import weakref
    from rquant.l2.cache import RawL2Cache

    cache = RawL2Cache(tmp_path / "recovery-receipt")
    data = observation()
    with cache.writer():
        for offset in range(3):
            cache.append(data.model_copy(update={"received_at": data.received_at + timedelta(seconds=offset)}))
    read_batch = RawL2Cache._read_batch
    calls, references = [], []
    def counted(directory, expected, **kwargs):
        batch = read_batch(directory, expected, **kwargs)
        calls.append(expected)
        references.append((weakref.ref(batch), weakref.ref(batch.table)))
        return batch
    monkeypatch.setattr(RawL2Cache, "_read_batch", staticmethod(counted))
    recovered = RawL2Cache(cache.root)
    last = data.received_at + timedelta(seconds=2)
    with recovered.writer():
        assert calls == [1, 2, 3]
        assert all(batch() is None and table() is None for batch, table in references)
        assert recovered._position() == (3, last)
        status = recovered.read_status(now=last)
        assert status.last_sequence == 3 and status.last_market_received_at == last
        assert status.gap and status.error_code == "restart_gap"
    assert recovered._last_received_at is None
    assert recovered.read_status(now=last).last_market_received_at == last


def test_prior_active_epoch_becomes_durable_gap_before_writer_yields(tmp_path: Path):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import FeedStatus

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.write_status(FeedStatus(state="connected", epoch_id="previous"))
    reopened = RawL2Cache(cache.root)
    with reopened.writer():
        assert RawL2Cache(cache.root).read_status().gap is True
        assert reopened.read_status().error_code == "restart_gap"
        reopened.write_status(FeedStatus(state="starting", epoch_id="next", gap=False))
        assert reopened.read_status().gap is True


def test_heartbeat_and_status_cannot_refresh_committed_market_receipt(tmp_path: Path):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import FeedStatus

    cache = RawL2Cache(tmp_path / "cache")
    data = observation()
    later = data.received_at + timedelta(seconds=90)
    with cache.writer():
        cache.append(data)
        cache.write_status(FeedStatus(state="seeded_waiting", heartbeat_count=2,
            last_transport_received_at=later, last_market_received_at=later))
    status = cache.read_status(now=later)
    assert status.last_market_received_at == data.received_at
    assert status.last_transport_received_at == later
    assert status.cache_age_seconds == 90
    assert status.cache_stale is True
    assert status.subscription_state == "requested_unverified"


def test_restart_after_clean_stop_preserves_a_gap_before_new_epoch(tmp_path: Path):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import FeedStatus

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.append(observation())
        cache.write_status(FeedStatus(state="stopped", gap=False))
    with cache.writer():
        assert cache.read_status().gap is True
        cache.write_status(FeedStatus(state="starting", epoch_id="next"))
        assert cache.read_status().gap is True


def test_pending_batch_ignored_and_corrupt_derived_value_rejected(tmp_path: Path):
    from rquant.l2.cache import CacheError, RawL2Cache

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.append(observation())
    pending = cache.root / "batches" / ".pending-synthetic"
    pending.mkdir()
    (pending / "batch.json").write_text("incomplete")
    assert len(cache.read_batches().batches) == 1
    committed = cache.root / "batches" / f"{1:020d}" / "batch.json"
    data = committed.read_bytes()
    assert b'"value":"2"' in data
    committed.write_bytes(data.replace(b'"value":"2"', b'"value":"3"', 1))
    with pytest.raises(CacheError, match="invalid_batch"):
        cache.read_batches()


def test_unrequested_missing_prefix_is_rejected_on_replay_and_full_audit(tmp_path: Path):
    from rquant.l2.cache import CacheError, RawL2Cache

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.append(observation())
        cache.append(observation())
    first = cache.root / "batches" / f"{1:020d}"
    for path in first.iterdir():
        path.unlink()
    first.rmdir()
    # A tail read certifies its selected range and head, not unrequested history.
    assert cache.read_batches(after_sequence=2).batches == ()
    with pytest.raises(CacheError):
        cache.read_batches(after_sequence=0)
    with pytest.raises(CacheError, match="sequence_gap"):
        cache._directories()
    with pytest.raises(CacheError, match="sequence_gap"):
        with RawL2Cache(cache.root).writer():
            pytest.fail('recovery accepted a missing historical batch')


@pytest.mark.parametrize("has_previous", [False, True])
def test_failed_directory_commit_exposes_no_torn_batch(tmp_path: Path, monkeypatch, has_previous):
    import rquant.l2.cache as module

    cache = module.RawL2Cache(tmp_path / "cache")
    original = module.os.rename

    def fail_commit(src, dst):
        if Path(src).name.startswith(".pending-"):
            raise OSError("synthetic secret must not leak")
        return original(src, dst)

    with cache.writer():
        data = observation()
        if has_previous:
            cache.append(data)
        monkeypatch.setattr(module.os, "rename", fail_commit)
        with pytest.raises(module.CacheError, match="^cache_io$"):
            cache.append(data.model_copy(update={"received_at": data.received_at + timedelta(seconds=1)}))
        expected = (1, data.received_at) if has_previous else (0, None)
        assert cache._position() == expected
        assert cache.read_status().last_market_received_at == expected[1]
    assert cache._last_received_at is None
    assert len(cache.read_batches().batches) == int(has_previous)


@pytest.mark.parametrize("after,limit", [(-1, 1), (True, 1), (0, 0), (0, 129)])
def test_local_cursor_validation(tmp_path: Path, after, limit):
    from rquant.l2.cache import RawL2Cache

    with pytest.raises(ValueError):
        RawL2Cache(tmp_path / "absent").read_batches(after_sequence=after, limit=limit)


def test_limited_reader_decodes_only_selected_and_last_batch(tmp_path: Path, monkeypatch):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import RawL2Batch

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        for _ in range(20):
            cache.append(observation())
    original = RawL2Batch.model_validate_json
    calls = []

    def counted(data, **kwargs):
        calls.append(len(data))
        return original(data, **kwargs)

    monkeypatch.setattr(RawL2Batch, "model_validate_json", counted)
    reader = RawL2Cache(cache.root)
    assert len(reader.read_batches(limit=1).batches) == 1
    assert reader.read_status().last_sequence == 20
    assert len(calls) <= 3


def test_writer_append_and_status_do_not_redecode_history(tmp_path: Path, monkeypatch):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import FeedStatus, RawL2Batch

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        for _ in range(20):
            cache.append(observation())
    with cache.writer():
        def unexpected(*args, **kwargs):
            pytest.fail("writer reparsed an old committed payload")

        monkeypatch.setattr(RawL2Batch, "model_validate_json", unexpected)
        assert cache.append(observation()).local_batch_sequence == 21
        cache.write_status(FeedStatus(state="connected"))
        assert cache.read_status().last_sequence == 21


def test_reader_snapshot_does_not_report_gap_when_writer_commits_during_scan(tmp_path: Path, monkeypatch):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import FeedStatus

    cache = RawL2Cache(tmp_path / "cache")
    original = Path.iterdir
    fired = False
    with cache.writer():
        cache.append(observation())
        cache.write_status(FeedStatus(state="connected"))
        (cache.root / 'batches' / '.head').unlink()  # Exercise legacy audit snapshot.

        def interleaved(path):
            nonlocal fired
            entries = list(original(path))
            if path == cache.root / "batches" and not fired:
                fired = True
                cache.append(observation())
                cache.write_status(FeedStatus(state="connected"))
            return iter(entries)

        monkeypatch.setattr(Path, "iterdir", interleaved)
        page = RawL2Cache(cache.root).read_batches()
        assert fired
        assert len(page.batches) == 1
        assert page.next_sequence == 1


def test_larger_published_page_is_bounded_and_history_audit_is_explicit(tmp_path: Path, monkeypatch):
    from rquant.l2.cache import RawL2Cache
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        for _ in range(129): cache.append(observation())
    original = Path.iterdir
    scans = []
    def counted(path):
        if path == cache.root / 'batches': scans.append(path)
        return original(path)
    monkeypatch.setattr(Path, 'iterdir', counted)
    page = RawL2Cache(cache.root).read_batches(limit=128)
    assert len(page.batches) == 128 and page.next_sequence == 128
    assert len(scans) == 0  # Published pages do not enumerate all history.
    assert len(RawL2Cache(cache.root).read_batches(after_sequence=128, limit=128).batches) == 1
    # Unrequested history is checked on replay or a complete audit/recovery.
    import shutil
    shutil.rmtree(cache.root / 'batches' / f'{1:020d}')
    assert len(RawL2Cache(cache.root).read_batches(after_sequence=128, limit=128).batches) == 1
    with pytest.raises(Exception, match='sequence_gap'):
        RawL2Cache(cache.root)._directories()


def test_page_payload_byte_budget_returns_contiguous_prefix(tmp_path: Path, monkeypatch):
    from rquant.l2 import cache as module
    cache = module.RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        for _ in range(4): cache.append(observation())
    size = (cache.root / 'batches' / f'{1:020d}' / 'batch.json').stat().st_size
    monkeypatch.setattr(module, 'MAX_BATCH_PAGE_BYTES', size * 2 + 2, raising=False)
    first = cache.read_batches(limit=8)
    assert len(first.batches) == 2 and first.next_sequence == 2
    second = cache.read_batches(after_sequence=2, limit=8)
    assert [b.local_batch_sequence for b in second.batches] == [3, 4]


def test_subscription_runtime_receipt_and_age_are_bounded_reads_without_history_scan(tmp_path, monkeypatch):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import Binding, FeedStatus, SubscriptionReceipt, SubscriptionStream
    cache = RawL2Cache(tmp_path / 'cache')
    assert hasattr(cache, 'read_runtime_status'), 'bounded runtime status reader is missing'
    assert cache.read_subscription_status() is None
    received = datetime.now(UTC) - timedelta(seconds=120)
    with cache.writer():
        batch = cache.append(observation().model_copy(update={'received_at': received}))
        cache.write_status(FeedStatus(state='connected', epoch_id='new_epoch'))
        receipt = SubscriptionReceipt(epoch_id='old_epoch', desired_revision=1, collector_state='stopped',
            streams=(SubscriptionStream(binding=Binding(page_id=10117, kind='vendor4013', code='600000', market=17),
                                        state='seeded', request_id=3, seed_sequence=batch.local_batch_sequence,
                                        seed_received_at=received),))
        cache.write_subscription_status(receipt)
    def forbidden(*args): pytest.fail('control GET scanned historical market batches')
    monkeypatch.setattr(cache, '_directories', forbidden)
    status = cache.read_runtime_status()
    assert status.cache_stale and status.cache_age_seconds >= 120
    assert status.epoch_id == 'new_epoch' and cache.read_subscription_status() == receipt
    assert cache.read_subscription_status().epoch_id != status.epoch_id
    runtime = cache.root / 'subscription-status.json'
    assert runtime.stat().st_size <= 256 * 1024
    runtime.write_bytes(b'x' * (256 * 1024 + 1))
    with pytest.raises(Exception): cache.read_subscription_status()


def test_actual_page_byte_budget_keeps_whole_batches_below_envelope_limit(tmp_path):
    from rquant.l2.cache import MAX_BATCH_PAGE_BYTES, RawL2Cache
    obs = observation()
    obs = obs.model_copy(update={'table': obs.table.model_copy(update={'interpretation_limits': 'x' * (1024 * 1024)})})
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        for _ in range(27): cache.append(obs)
    page = cache.read_batches(limit=128)
    committed_bytes = sum((cache.root / 'batches' / f'{b.local_batch_sequence:020d}' / 'batch.json').stat().st_size for b in page.batches)
    assert 0 < len(page.batches) < 27 and committed_bytes <= MAX_BATCH_PAGE_BYTES
    assert len(page.model_dump_json().encode()) < 32 * 1024 * 1024
    assert page.next_sequence == len(page.batches)
    assert cache.read_batches(after_sequence=page.next_sequence, limit=128).batches[0].local_batch_sequence == page.next_sequence + 1


def test_byte_envelope_single_large_batch_progress_and_exact_resource_boundary(tmp_path, monkeypatch):
    from datetime import UTC, datetime
    from rquant.l2 import cache as module
    from rquant.l2.models import RawL2Batch
    fixed = datetime(2026, 10, 19, tzinfo=UTC)
    monkeypatch.setattr(module, "datetime", type("FixedClock", (), {"now": staticmethod(lambda tz: fixed)}))
    original = observation()
    initial = RawL2Batch.model_validate({**original.model_dump(mode="python"),
        "local_batch_sequence": 1, "cached_at": fixed})
    padding = (20 << 20) - len(initial.model_dump_json().encode()) - 1
    large = original.model_copy(update={"table": original.table.model_copy(update={
        "interpretation_limits": original.table.interpretation_limits + "x" * padding})})
    source = module.RawL2Cache(tmp_path / "large")
    with source.writer():
        batch = source.append(large)
        assert len(batch.model_dump_json().encode()) + 1 == 20 << 20
        too_large = large.model_copy(update={"table": large.table.model_copy(update={
            "interpretation_limits": large.table.interpretation_limits + "x"})})
        with pytest.raises(module.CacheError, match="invalid_batch"):
            source.append(too_large)
        assert not list((source.root / "batches").glob(".pending-*"))
    reader = module.RawL2Cache(source.root)
    page = reader.read_batches(limit=1)
    assert page.next_sequence == 1 and len(page.batches) == 1
    assert page.batches[0] == batch
    assert len(page.model_dump_json().encode()) < 32 << 20
    assert reader.read_batches(after_sequence=1).batches == ()
    with reader.writer():
        assert reader._highwater == 1 and reader._last_received_at == batch.received_at


def test_published_head_is_same_inode_and_append_keeps_four_fsyncs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os
    from rquant.l2 import cache as module
    cache = module.RawL2Cache(tmp_path / 'cache')
    calls = []
    original = module.os.fsync
    def counted(fd: int) -> None:
        calls.append(fd)
        return original(fd)
    with cache.writer():
        monkeypatch.setattr(module.os, 'fsync', counted)
        cache.append(observation())
        assert len(calls) == 4
    parent = cache.root / 'batches'
    assert os.path.samefile(parent / '.head', parent / f'{1:020d}' / 'batch.json')
    assert list(parent.glob('.pending-head-*')) == []


def test_published_cursor_work_does_not_scan_history(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.l2.cache import RawL2Cache
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        for _ in range(65): cache.append(observation())
    def forbidden(*args: object) -> None:
        pytest.fail('published cursor enumerated historical prefix')
    monkeypatch.setattr(RawL2Cache, '_directories', forbidden)
    reader = RawL2Cache(cache.root)
    page = reader.read_batches(after_sequence=31, limit=2)
    assert [b.local_batch_sequence for b in page.batches] == [32, 33]
    assert page.status.last_sequence == 65 and reader.read_status().last_sequence == 65


def test_published_head_fd_remains_one_snapshot_across_replacement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os
    from rquant.l2 import cache as module
    from rquant.l2.models import FeedStatus
    cache = module.RawL2Cache(tmp_path / 'cache')
    original = module.os.open
    fired = False
    with cache.writer():
        cache.append(observation())
        cache.write_status(FeedStatus(state='connected'))
        reader = module.RawL2Cache(cache.root)
        assert reader.read_status().last_market_received_at == observation().received_at
        def interleaved(path: str | Path, flags: int, *args: int, **kwargs: int) -> int:
            nonlocal fired
            fd = original(path, flags, *args, **kwargs)
            if Path(path).name == '.head' and not fired:
                fired = True
                cache.append(observation().model_copy(update={
                    'received_at': observation().received_at + timedelta(seconds=1),
                }))
                cache.write_status(FeedStatus(state='connected'))
            return fd
        monkeypatch.setattr(module.os, 'open', interleaved)
        page = reader.read_batches(limit=8)
        assert fired and page.status.last_sequence == page.next_sequence == 1
        assert [b.local_batch_sequence for b in page.batches] == [1]
        latest = reader.read_status()
        assert latest.last_sequence == 2
        assert latest.last_market_received_at == observation().received_at + timedelta(seconds=1)


@pytest.mark.parametrize('legacy', [False, True])
def test_published_writer_repairs_only_fully_audited_stale_or_legacy_head(tmp_path: Path, legacy: bool) -> None:
    import os
    from rquant.l2.cache import RawL2Cache
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        cache.append(observation()); cache.append(observation())
    parent = cache.root / 'batches'; head = parent / '.head'
    head.unlink()
    if not legacy: os.link(parent / f'{1:020d}' / 'batch.json', head)
    if legacy: assert RawL2Cache(cache.root).read_batches(limit=8).next_sequence == 2
    with RawL2Cache(cache.root).writer() as restored:
        assert restored.read_status().gap
        assert os.path.samefile(head, parent / f'{2:020d}' / 'batch.json')
    assert len(RawL2Cache(cache.root)._directories()) == 2


@pytest.mark.parametrize('warm', [False, True])
@pytest.mark.parametrize('fault', ['symlink', 'directory', 'fifo', 'oversize', 'copy', 'corrupt',
                                  'missing_target', 'bad_sha', 'ahead_status', 'mode', 'owner',
                                  'same_size_invalid_model', 'unbounded_sequence'])
def test_published_bad_head_never_falls_back_or_is_silently_repaired(
        tmp_path: Path, monkeypatch, fault: str, warm: bool) -> None:
    import os
    from rquant.l2.cache import CacheError, RawL2Cache
    from rquant.l2.models import FeedStatus
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer(): cache.append(observation())
    reader = RawL2Cache(cache.root)
    if warm:
        assert reader.read_status().last_sequence == 1
    parent = cache.root / 'batches'; target = parent / f'{1:020d}' / 'batch.json'; head = parent / '.head'
    if fault in {'symlink', 'directory', 'fifo', 'oversize', 'copy'}:
        head.unlink()
        if fault == 'symlink': head.symlink_to(target)
        elif fault == 'directory': head.mkdir()
        elif fault == 'fifo': os.mkfifo(head, 0o600)
        elif fault == 'oversize':
            with head.open('wb') as stream: stream.truncate(20 * 1024 * 1024 + 1)
        else:
            head.write_bytes(target.read_bytes())
            head.chmod(0o600)
    elif fault == 'corrupt': target.write_bytes(b'bad head')
    elif fault == 'missing_target': target.unlink()
    elif fault == 'bad_sha': (target.parent / 'sha256').write_bytes(b'0' * 64 + b'\n')
    elif fault == 'mode': head.chmod(0o640)
    elif fault == 'owner':
        actual_owner = os.geteuid()
        monkeypatch.setattr(os, 'geteuid', lambda: actual_owner + 1)
    elif fault in {'same_size_invalid_model', 'unbounded_sequence'}:
        payload = target.read_bytes()
        if fault == 'same_size_invalid_model':
            changed = payload.replace(b'"side":"buy"', b'"side":"bad"', 1)
            assert changed != payload and len(changed) == len(payload)
        else:
            changed = payload.replace(b'"local_batch_sequence":1',
                                      b'"local_batch_sequence":100000000000000000000', 1)
            assert changed != payload
        target.write_bytes(changed)
        (target.parent / 'sha256').write_bytes(hashlib.sha256(changed).hexdigest().encode() + b'\n')
    else:
        (cache.root / 'status.json').write_text(FeedStatus(last_sequence=2).model_dump_json())
    with pytest.raises(CacheError): reader.read_batches(after_sequence=1)
    with pytest.raises(CacheError):
        with RawL2Cache(cache.root).writer(): pytest.fail('unsafe head was repaired')


@pytest.mark.parametrize('fault', ['link', 'replace', 'parent_fsync'])
def test_published_failure_does_not_acknowledge_or_remove_raw_batch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str) -> None:
    import os
    from rquant.l2 import cache as module
    cache = module.RawL2Cache(tmp_path / 'cache'); parent = cache.root / 'batches'
    with cache.writer():
        data = observation()
        original_link = module.os.link; original_replace = module.os.replace; original_fsync = module._fsync_dir
        def failed_link(*args: object, **kwargs: object) -> None: raise OSError('synthetic link failure')
        def failed_replace(src: Path, dst: Path) -> None:
            if Path(dst).name == '.head': raise OSError('synthetic publication failure')
            return original_replace(src, dst)
        def failed_fsync(path: Path) -> None:
            if path == parent: raise OSError('synthetic parent durability failure')
            return original_fsync(path)
        with monkeypatch.context() as patch:
            patch.setattr(module.os, 'link', failed_link if fault == 'link' else original_link)
            patch.setattr(module.os, 'replace', failed_replace if fault == 'replace' else original_replace)
            patch.setattr(module, '_fsync_dir', failed_fsync if fault == 'parent_fsync' else original_fsync)
            with pytest.raises(module.CacheError): cache.append(data)
        assert cache._position() == (1, data.received_at)
        assert cache.read_status().last_market_received_at == data.received_at
        assert (parent / f'{1:020d}' / 'batch.json').is_file()
        assert list(parent.glob('.pending-head-*')) == []
    with module.RawL2Cache(cache.root).writer() as recovered:
        assert recovered.read_status().gap
        assert recovered._position() == (1, data.received_at)
        assert os.path.samefile(parent / '.head', parent / f'{1:020d}' / 'batch.json')


@pytest.mark.parametrize('warm', [False, True])
def test_published_legacy_fallback_audits_payloads_before_old_cursor(tmp_path: Path, warm: bool) -> None:
    from rquant.l2.cache import CacheError, RawL2Cache
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        cache.append(observation()); cache.append(observation())
    reader = RawL2Cache(cache.root)
    if warm:
        assert reader.read_status().last_sequence == 2
    parent = cache.root / 'batches'; (parent / '.head').unlink()
    (parent / f'{1:020d}' / 'batch.json').write_bytes(b'corrupt unrequested legacy payload')
    with pytest.raises(CacheError): reader.read_batches(after_sequence=2)
    with pytest.raises(CacheError):
        with RawL2Cache(cache.root).writer(): pytest.fail('legacy corruption was repaired')


def test_published_full_audit_does_not_mistake_readdir_omission_for_a_gap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.l2.cache import RawL2Cache
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        cache.append(observation()); cache.append(observation())
    original = Path.iterdir
    def omitted(path: Path) -> Iterator[Path]:
        return (p for p in original(path) if not (path == cache.root / 'batches' and p.name == f'{1:020d}'))
    monkeypatch.setattr(Path, 'iterdir', omitted)
    assert len(RawL2Cache(cache.root)._directories()) == 2


@pytest.mark.parametrize('fault', ['low', 'zero_block', 'negative_blocks', 'wrong_type', 'absent', 'error'])
def test_published_disk_guard_rejects_before_new_allocations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str) -> None:
    from types import SimpleNamespace
    from rquant.l2 import cache as module
    cache = module.RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        cache.append(observation())
        before = {str(p.relative_to(cache.root)): p.read_bytes() for p in cache.root.rglob('*') if p.is_file()}
        def unavailable(path: Path) -> SimpleNamespace:
            if fault == 'error': raise OSError('synthetic unavailable disk stats')
            if fault == 'absent': return SimpleNamespace()
            return SimpleNamespace(f_bavail=-1 if fault == 'negative_blocks' else 'invalid' if fault == 'wrong_type' else 1,
                                   f_frsize=0 if fault == 'zero_block' else 4096)
        with monkeypatch.context() as patch:
            patch.setattr(module.os, 'statvfs', unavailable)
            with pytest.raises(module.CacheError, match='^cache_io$'): cache.append(observation())
        after = {str(p.relative_to(cache.root)): p.read_bytes() for p in cache.root.rglob('*') if p.is_file()}
        assert after == before
        assert len(cache._directories()) == 1


def test_published_disk_guard_exact_budget_and_one_byte_below(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace
    from rquant.l2 import cache as module
    from rquant.l2.models import RawL2Batch
    cache = module.RawL2Cache(tmp_path / 'cache')
    fixed = datetime(2026, 10, 19, 3, tzinfo=UTC)
    class FixedClock(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime: return fixed
    monkeypatch.setattr(module, 'datetime', FixedClock)
    data = observation()
    payload = RawL2Batch.model_validate({**data.model_dump(mode='python'),
        'local_batch_sequence': 1, 'cached_at': fixed}).model_dump_json().encode() + b'\n'
    minimum = 5 * 1024**3 + len(payload) + 65 + 1024**2
    with cache.writer():
        monkeypatch.setattr(module.os, 'statvfs', lambda path: SimpleNamespace(f_frsize=1, f_bavail=minimum - 1))
        with pytest.raises(module.CacheError, match='^cache_io$'): cache.append(data)
        assert list((cache.root / 'batches').iterdir()) == []
        monkeypatch.setattr(module.os, 'statvfs', lambda path: SimpleNamespace(f_frsize=1, f_bavail=minimum))
        assert cache.append(data).local_batch_sequence == 1


@pytest.mark.parametrize('fault', ['directory_symlink', 'batch_symlink', 'sha_directory'])
def test_published_tail_defers_unrequested_unsafe_history_to_replay_or_audit(
        tmp_path: Path, fault: str) -> None:
    from rquant.l2.cache import CacheError, RawL2Cache
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        cache.append(observation()); cache.append(observation())
    first = cache.root / 'batches' / f'{1:020d}'
    if fault == 'directory_symlink':
        held = cache.root / 'held'; first.rename(held); first.symlink_to(held, target_is_directory=True)
    elif fault == 'batch_symlink':
        held = first / 'held.json'; (first / 'batch.json').rename(held); (first / 'batch.json').symlink_to(held)
    else:
        (first / 'sha256').unlink(); (first / 'sha256').mkdir()
    assert RawL2Cache(cache.root).read_batches(after_sequence=2).next_sequence == 2
    with pytest.raises(CacheError): RawL2Cache(cache.root).read_batches(after_sequence=0)
    with pytest.raises(CacheError): RawL2Cache(cache.root)._directories()
    with pytest.raises(CacheError):
        with RawL2Cache(cache.root).writer(): pytest.fail('unsafe old prefix passed recovery')


def test_published_full_audit_ignores_only_exact_head_name(tmp_path: Path) -> None:
    from rquant.l2.cache import CacheError, RawL2Cache
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer(): cache.append(observation())
    (cache.root / 'batches' / '.head-copy').write_bytes(b'unknown entry')
    with pytest.raises(CacheError, match='invalid_cache'): cache._directories()


@pytest.mark.parametrize('name', ['status.json', 'subscription-status.json'])
def test_mutable_snapshot_replacement_between_lstat_and_open_is_valid(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    from rquant.l2 import cache as module
    from rquant.l2.models import FeedStatus, SubscriptionReceipt
    cache = module.RawL2Cache(tmp_path / 'cache')
    original = module.os.open
    fired = False
    with cache.writer():
        if name == 'status.json':
            cache.write_status(FeedStatus(state='connected', heartbeat_count=1))
            updated = FeedStatus(state='connected', heartbeat_count=2)
        else:
            cache.write_subscription_status(SubscriptionReceipt(
                epoch_id='snapshot_race', desired_revision=1, collector_state='connected', streams=()))
            updated = SubscriptionReceipt(epoch_id='snapshot_race', desired_revision=2,
                                          collector_state='connected', streams=())

        def interleaved(path: str | Path, flags: int, *args: int, **kwargs: int) -> int:
            nonlocal fired
            if Path(path) == cache.root / name and not fired:
                fired = True
                module._replace_snapshot(cache.root, name, updated.model_dump_json().encode() + b'\n')
            return original(path, flags, *args, **kwargs)

        monkeypatch.setattr(module.os, 'open', interleaved)
        reader = module.RawL2Cache(cache.root)
        if name == 'status.json':
            assert reader.read_runtime_status().heartbeat_count == 2
        else:
            assert reader.read_subscription_status().desired_revision == 2
        assert fired


def order640_observation():
    from rquant.l2._codec import decode_order_response
    from rquant.l2.models import MarketObservation

    body = (Path(__file__).parents[1] / "fixtures/l2-order640.body").read_bytes()
    assert hashlib.sha256(body).hexdigest() == "f5060e3f6c430aee2e199fcc026a4501f4d16177ca77f3e43e5e7a79969071a2"
    result = decode_order_response(body, request_id=0xFFFFFFFF, page_id=10199,
                                   code="600276", source_fd_ordinal=1)
    return MarketObservation(epoch_id="append_segments_only",
        binding={"page_id": 10199, "kind": "vendor4027", "code": "600276", "market": 17, "side": "sell"},
        received_at=datetime(2026, 10, 20, 5, 1, 33, 123456, tzinfo=UTC), request_id=0xFFFFFFFF,
        source_fd_ordinal=1, delivery="candidate_push", response_body_hex=body.hex(),
        raw_table_hex=result["raw_table"].hex(), table=result["table"], response_sha256=result["response_sha256"])


def test_order640_canonical_cache_roundtrip_and_receipt_overwrite(tmp_path, monkeypatch):
    from rquant.l2 import cache as module

    fixed = datetime(2026, 10, 20, 8, 0, 0, 123456, tzinfo=UTC)
    monkeypatch.setattr(module, "datetime", type("Clock", (), {"now": staticmethod(lambda tz: fixed)}))
    original = order640_observation()
    cache = module.RawL2Cache(tmp_path / "order640")
    with cache.writer():
        first = cache.append(original)
        payload = first.model_dump_json().encode() + b"\n"
        assert hashlib.sha256(payload).hexdigest() == "eb2918f0abd92dbeb8156282139fe6f84595886eeeead6cee4407a6f2aee9306"
        assert (cache.root / "batches/00000000000000000001/batch.json").read_bytes() == payload
        assert first.table.row_count == len(first.table.rows) == 640
        assert first.table.row_data_sha256 == "b4efcae2e9aabbf95c82cb37d20678580290807064b2b6e76cd367d3e37ae861"
        second = cache.append(original.model_copy(update={"received_at": original.received_at.isoformat()}))
        assert second.received_at == original.received_at
        third = cache.append(first.model_copy(update={"local_batch_sequence": True, "cached_at": 0}))
        assert third.local_batch_sequence == 3 and third.cached_at == fixed
    page = module.RawL2Cache(cache.root).read_batches(limit=128)
    assert page.next_sequence == 3 and page.batches[0].model_dump_json().encode() + b"\n" == payload


@pytest.mark.parametrize("fault", ["value_true", "value_false", "value_bytes", "style_bool", "extra",
    "row_key", "raw_hex", "numeric_time", "nonUTC", "geometry", "side", "delivery"])
def test_order640_typed_mutation_is_rejected_before_commit(tmp_path, fault):
    from pydantic import TypeAdapter
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import RawCell

    original = order640_observation()
    if fault in {"numeric_time", "nonUTC"}:
        value = 0 if fault == "numeric_time" else original.received_at.astimezone(timezone(timedelta(hours=1)))
        changed = original.model_copy(update={"received_at": value})
    elif fault == "geometry":
        changed = original.model_copy(update={"table": original.table.model_copy(update={"row_width": 31})})
    elif fault == "side":
        changed = original.model_copy(update={"binding": original.binding.model_copy(update={"side": "buy"})})
    elif fault == "delivery":
        changed = original.model_copy(update={"delivery": "seed"})
    else:
        rows = list(original.table.rows)
        row = dict(rows[0])
        cell = TypeAdapter(RawCell).dump_python(row["1"])
        if fault.startswith("value_"):
            cell["value"] = {"value_true": True, "value_false": False, "value_bytes": b"1"}[fault]
        elif fault == "style_bool": cell["style"] = True
        elif fault == "extra": cell["extra"] = 1
        elif fault == "raw_hex": cell["raw_hex"] = "xyz"
        row["1"] = cell
        if fault == "row_key": row[1] = row.pop("1")
        rows[0] = row
        changed = original.model_copy(update={"table": original.table.model_copy(update={"rows": tuple(rows)})})
    cache = RawL2Cache(tmp_path / "mutation")
    with cache.writer():
        cache.append(original)
        with pytest.raises(ValueError):
            cache.append(changed)
        assert cache.read_status().last_sequence == 1
        assert not list((cache.root / "batches").glob(".pending-*"))


def test_head_receipt_repeated_status_and_caught_up_reads_keep_only_scalars(tmp_path, monkeypatch):
    import weakref
    from rquant.l2 import cache as module
    from rquant.l2.models import RawL2Batch

    cache = module.RawL2Cache(tmp_path / 'receipt')
    with cache.writer(): cache.append(order640_observation())
    fixed = datetime(2026, 10, 20, 8, tzinfo=UTC)
    monkeypatch.setattr(module, 'datetime', type('Clock', (), {'now': staticmethod(lambda tz: fixed)}))
    reader = module.RawL2Cache(cache.root)
    original = RawL2Batch.model_validate_json
    calls, models = [], []
    def counted(payload, **kwargs):
        result = original(payload, **kwargs)
        calls.append(len(payload)); models.append(weakref.ref(result))
        return result
    monkeypatch.setattr(RawL2Batch, 'model_validate_json', counted)
    first = reader.read_status(now=fixed)
    for _ in range(3):
        assert reader.read_status(now=fixed) == first
        page = reader.read_batches(after_sequence=1)
        assert page.batches == () and page.next_sequence == 1 and page.status == first
    assert len(calls) == 1
    assert all(reference() is None for reference in models)
    def scalar(value):
        return isinstance(value, (str, int, datetime)) or isinstance(value, tuple) and all(scalar(v) for v in value)
    assert scalar(reader._head_receipt)
    assert reader._last_received_at is None


def test_head_receipt_concurrent_cold_readers_validate_once(tmp_path, monkeypatch):
    from threading import Barrier, Event, Lock, Thread
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import RawL2Batch

    cache = RawL2Cache(tmp_path / 'cold')
    with cache.writer(): cache.append(observation())
    reader = RawL2Cache(cache.root)
    original = RawL2Batch.model_validate_json
    entered, duplicate, release = Event(), Event(), Event()
    begin, lock = Barrier(4), Lock()
    calls, results, errors = [], [], []
    def counted(payload, **kwargs):
        with lock:
            calls.append(len(payload))
            if len(calls) > 1: duplicate.set()
        entered.set()
        assert release.wait(3)
        return original(payload, **kwargs)
    def read():
        try:
            begin.wait(3)
            results.append(reader.read_status(now=datetime(2026, 10, 20, tzinfo=UTC)))
        except BaseException as error:
            errors.append(error)
    monkeypatch.setattr(RawL2Batch, 'model_validate_json', counted)
    threads = [Thread(target=read, daemon=True) for _ in range(4)]
    try:
        for thread in threads: thread.start()
        assert entered.wait(3)
        duplicate.wait(.25)
    finally:
        release.set()
        for thread in threads: thread.join(3)
    assert not any(thread.is_alive() for thread in threads) and not errors
    assert len(results) == 4 and all(result == results[0] for result in results)
    assert len(calls) == 1


def test_head_receipt_selected_batches_are_revalidated_and_not_shared(tmp_path, monkeypatch):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import RawL2Batch

    cache = RawL2Cache(tmp_path / 'selected')
    with cache.writer(): committed = cache.append(observation())
    expected = committed.model_dump_json()
    reader = RawL2Cache(cache.root)
    original = RawL2Batch.model_validate_json
    calls = []
    def counted(payload, **kwargs):
        calls.append(len(payload))
        return original(payload, **kwargs)
    monkeypatch.setattr(RawL2Batch, 'model_validate_json', counted)
    assert reader.read_status().last_sequence == 1
    first = reader.read_batches(limit=1)
    object.__setattr__(first.batches[0].table.rows[0]['1'], 'value', 'caller mutation')
    second = reader.read_batches(limit=128)
    assert second.batches[0].model_dump_json() == expected
    assert first.batches[0] is not second.batches[0]
    assert len(calls) == 3  # One cold head; each selected read independently validates.


def test_head_receipt_checks_full_bytes_and_new_identity_then_revalidates(tmp_path, monkeypatch):
    import os
    from rquant.l2 import cache as module
    from rquant.l2.models import RawL2Batch

    cache = module.RawL2Cache(tmp_path / 'changed')
    with cache.writer(): cache.append(observation())
    parent = cache.root / 'batches'; target = parent / f'{1:020d}' / 'batch.json'
    payload = target.read_bytes()
    reader = module.RawL2Cache(cache.root)
    validate = RawL2Batch.model_validate_json
    digest = module.hashlib.sha256
    model_calls, payload_hashes = [], []
    def counted_model(raw, **kwargs):
        model_calls.append(len(raw)); return validate(raw, **kwargs)
    def counted_hash(raw=b'', *args, **kwargs):
        if raw == payload: payload_hashes.append(len(raw))
        return digest(raw, *args, **kwargs)
    monkeypatch.setattr(RawL2Batch, 'model_validate_json', counted_model)
    monkeypatch.setattr(module.hashlib, 'sha256', counted_hash)
    original_time = reader.read_status().last_market_received_at
    assert reader.read_status().last_market_received_at == original_time
    assert len(model_calls) == 1 and len(payload_hashes) == 2
    changed = payload.replace(b'2026-10-15T07:00:00Z', b'2026-10-15T07:00:01Z', 1)
    assert changed != payload and len(changed) == len(payload)
    target.write_bytes(changed)
    (target.parent / 'sha256').write_bytes(digest(changed).hexdigest().encode() + b'\n')
    assert reader.read_status().last_market_received_at == original_time + timedelta(seconds=1)
    assert len(model_calls) == 2
    replacement = target.parent / 'replacement'
    replacement.write_bytes(changed); replacement.chmod(0o600)
    os.replace(replacement, target)
    (parent / '.head').unlink(); os.link(target, parent / '.head')
    assert reader.read_status().last_market_received_at == original_time + timedelta(seconds=1)
    assert len(model_calls) == 3


def test_head_receipt_cold_exception_releases_lock_and_does_not_publish(tmp_path, monkeypatch):
    from threading import Thread
    from rquant.l2.cache import CacheError, RawL2Cache
    from rquant.l2.models import RawL2Batch

    cache = RawL2Cache(tmp_path / 'failure')
    with cache.writer(): cache.append(observation())
    reader = RawL2Cache(cache.root)
    original = RawL2Batch.model_validate_json
    calls, results, errors = [], [], []
    def fail_once(payload, **kwargs):
        calls.append(len(payload))
        if len(calls) == 1: raise ValueError('cold validation failed')
        return original(payload, **kwargs)
    monkeypatch.setattr(RawL2Batch, 'model_validate_json', fail_once)
    with pytest.raises(CacheError, match='invalid_batch'): reader.read_status()
    def read():
        try: results.append(reader.read_status())
        except BaseException as error: errors.append(error)
    thread = Thread(target=read, daemon=True)
    thread.start(); thread.join(3)
    assert not thread.is_alive() and not errors
    assert len(results) == 1 and results[0].last_sequence == 1 and len(calls) == 2


def test_cold_selected_head_reuses_only_its_own_model_after_full_reread(tmp_path, monkeypatch):
    from rquant.l2 import cache as module
    from rquant.l2.models import RawL2Batch

    cache = module.RawL2Cache(tmp_path / 'cold-selected')
    with cache.writer(): committed = cache.append(order640_observation())
    target = cache.root / 'batches' / f'{1:020d}' / 'batch.json'
    payload = target.read_bytes()
    validate, read = RawL2Batch.model_validate_json, module._read
    calls, rereads = [], []
    def counted(raw, **kwargs):
        calls.append(len(raw))
        return validate(raw, **kwargs)
    def reread(path, maximum, **kwargs):
        result = read(path, maximum, **kwargs)
        if path == target:
            assert result == payload
            rereads.append(len(result))
        return result
    monkeypatch.setattr(RawL2Batch, 'model_validate_json', counted)
    monkeypatch.setattr(module, '_read', reread)
    reader = module.RawL2Cache(cache.root)
    first = reader.read_batches(limit=1)
    assert first.batches[0].model_dump_json() == committed.model_dump_json()
    assert calls == [len(payload)] and rereads == [len(payload)]
    object.__setattr__(first.batches[0].table.rows[0]['1'], 'value', 'caller mutation')
    second = reader.read_batches(limit=128)
    assert second.batches[0].model_dump_json() == committed.model_dump_json()
    assert first.batches[0] is not second.batches[0]
    assert calls == [len(payload)] * 2 and rereads == [len(payload)] * 2


@pytest.mark.parametrize('fault', ['payload', 'inode', 'mode', 'owner', 'sidecar', 'sequence'])
def test_cold_selected_head_rejects_changes_at_fresh_open_and_closes_fds(tmp_path, monkeypatch, fault):
    import os
    from rquant.l2 import cache as module

    cache = module.RawL2Cache(tmp_path / fault)
    with cache.writer(): cache.append(observation())
    target = cache.root / 'batches' / f'{1:020d}' / 'batch.json'
    payload = target.read_bytes()
    original_open, original_fstat = os.open, os.fstat
    opened, selected, fired = [], set(), False
    def interleaved(path, flags, *args, **kwargs):
        nonlocal fired
        if Path(path) == target and not fired:
            fired = True
            if fault in {'payload', 'sequence'}:
                changed = (payload.replace(b'2026-10-15T07:00:00Z', b'2026-10-15T07:00:01Z', 1)
                    if fault == 'payload' else payload.replace(b'"local_batch_sequence":1', b'"local_batch_sequence":2', 1))
                assert changed != payload and len(changed) == len(payload)
                target.write_bytes(changed)
                (target.parent / 'sha256').write_bytes(hashlib.sha256(changed).hexdigest().encode() + b'\n')
            elif fault == 'inode':
                replacement = target.parent / 'replacement'
                replacement.write_bytes(payload); replacement.chmod(0o600)
                os.replace(replacement, target)
            elif fault == 'mode': target.chmod(0o640)
            elif fault == 'sidecar': (target.parent / 'sha256').write_bytes(b'0' * 64 + b'\n')
        fd = original_open(path, flags, *args, **kwargs)
        opened.append(fd)
        if Path(path) == target: selected.add(fd)
        return fd
    def fstat(fd):
        info = original_fstat(fd)
        if fault == 'owner' and fd in selected:
            fields = list(info); fields[4] += 1
            return os.stat_result(fields)
        return info
    reader = module.RawL2Cache(cache.root)
    with monkeypatch.context() as patch:
        patch.setattr(os, 'open', interleaved); patch.setattr(os, 'fstat', fstat)
        with pytest.raises(module.CacheError, match='invalid_batch'):
            reader.read_batches(limit=1)
    assert fired and selected
    for fd in opened:
        with pytest.raises(OSError): original_fstat(fd)
    # A failed selection must leave no held cold-validation lock.
    assert reader._head_lock.acquire(timeout=1)
    reader._head_lock.release()


def test_cold_selected_head_concurrent_readers_return_independent_models(tmp_path, monkeypatch):
    from threading import Barrier, Lock, Thread
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import RawL2Batch

    cache = RawL2Cache(tmp_path / 'selected-concurrent')
    with cache.writer(): committed = cache.append(observation())
    reader = RawL2Cache(cache.root)
    validate = RawL2Batch.model_validate_json
    begin, lock = Barrier(4), Lock()
    calls, pages, errors = [], [], []
    def counted(raw, **kwargs):
        with lock: calls.append(len(raw))
        return validate(raw, **kwargs)
    def read():
        try:
            begin.wait(3)
            pages.append(reader.read_batches(limit=1))
        except BaseException as error: errors.append(error)
    monkeypatch.setattr(RawL2Batch, 'model_validate_json', counted)
    threads = [Thread(target=read, daemon=True) for _ in range(4)]
    try:
        for thread in threads: thread.start()
    finally:
        for thread in threads: thread.join(3)
    assert not any(thread.is_alive() for thread in threads) and not errors
    assert len(pages) == len(calls) == 4
    assert len({id(page.batches[0]) for page in pages}) == 4
    object.__setattr__(pages[0].batches[0].table.rows[0]['1'], 'value', 'caller mutation')
    assert all(page.batches[0].model_dump_json() == committed.model_dump_json() for page in pages[1:])


@pytest.mark.parametrize('mode', ['older', 'legacy', 'excluded'])
def test_cold_head_model_is_not_carried_to_ineligible_selection(tmp_path, monkeypatch, mode):
    import weakref
    from rquant.l2 import cache as module
    from rquant.l2.models import RawL2Batch

    cache = module.RawL2Cache(tmp_path / mode)
    with cache.writer():
        cache.append(observation())
        if mode != 'excluded': cache.append(observation())
    if mode == 'legacy': (cache.root / 'batches' / '.head').unlink()
    validate, read = RawL2Batch.model_validate_json, module._read
    references = []
    def counted(raw, **kwargs):
        batch = validate(raw, **kwargs)
        references.append(weakref.ref(batch))
        return batch
    def reread(path, maximum, **kwargs):
        if mode == 'older' and path.name == 'batch.json':
            assert len(references) == 1 and references[0]() is None
        return read(path, maximum, **kwargs)
    monkeypatch.setattr(RawL2Batch, 'model_validate_json', counted)
    monkeypatch.setattr(module, '_read', reread)
    if mode == 'excluded': monkeypatch.setattr(module, 'MAX_BATCH_PAGE_BYTES', 1)
    reader = module.RawL2Cache(cache.root)
    page = reader.read_batches(limit=1)
    assert len(references) == {'older': 2, 'legacy': 3, 'excluded': 1}[mode]
    assert page.next_sequence == (0 if mode == 'excluded' else 1)
    assert all(reference() is None for reference in references[:-1] if mode != 'excluded')
    if mode == 'excluded': assert references[0]() is None
    assert reader._last_received_at is None


@pytest.mark.parametrize('fault', ['duplicate', 'keys', 'width', 'raw', 'style', 'hash'])
def test_table_column_reuse_preserves_every_row_and_byte_check(fault):
    from rquant.l2.models import RawTable
    table = observation().table
    if fault == 'duplicate':
        columns = list(table.columns)
        columns[1] = columns[1].model_copy(update={'field_id': columns[0].field_id})
        table = table.model_copy(update={'columns': tuple(columns)})
    elif fault == 'width':
        columns = list(table.columns)
        columns[0] = columns[0].model_copy(update={'width': columns[0].width + 1})
        columns[1] = columns[1].model_copy(update={'width': columns[1].width - 1})
        table = table.model_copy(update={'columns': tuple(columns)})
    elif fault == 'hash': table = table.model_copy(update={'row_data_sha256': '0' * 64})
    else:
        rows = list(table.rows); row = dict(rows[-1])
        if fault == 'keys': row['unexpected'] = row.pop(next(iter(row)))
        else:
            cell = row[next(iter(row))]
            object.__setattr__(cell, 'raw_hex' if fault == 'raw' else 'style', '00' if fault == 'raw' else 254)
        rows[-1] = row
        table = table.model_copy(update={'rows': tuple(rows)})
    with pytest.raises(ValueError): RawTable.validate_rows(table)
