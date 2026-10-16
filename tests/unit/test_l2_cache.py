from __future__ import annotations

import hashlib
import json
import struct
from datetime import UTC, datetime, timedelta
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


def test_missing_prefix_fails_even_when_cursor_is_after_the_gap(tmp_path: Path):
    from rquant.l2.cache import CacheError, RawL2Cache

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.append(observation())
        cache.append(observation())
    first = cache.root / "batches" / f"{1:020d}"
    for path in first.iterdir():
        path.unlink()
    first.rmdir()
    with pytest.raises(CacheError, match="sequence_gap"):
        cache.read_batches(after_sequence=1)


def test_failed_directory_commit_exposes_no_torn_batch(tmp_path: Path, monkeypatch):
    import rquant.l2.cache as module

    cache = module.RawL2Cache(tmp_path / "cache")
    original = module.os.rename

    def fail_commit(src, dst):
        if Path(src).name.startswith(".pending-"):
            raise OSError("synthetic secret must not leak")
        return original(src, dst)

    with cache.writer():
        monkeypatch.setattr(module.os, "rename", fail_commit)
        with pytest.raises(module.CacheError, match="^cache_io$"):
            cache.append(observation())
    assert cache.read_batches().batches == ()


@pytest.mark.parametrize("after,limit", [(-1, 1), (True, 1), (0, 0), (0, 9)])
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
        assert len(page.batches) == 1
        assert page.next_sequence == 1
