from __future__ import annotations

import importlib
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from rquant.screen.intraday_contracts import IntradayStockSnapshot


def test_reference_universe_is_complete_bounded_and_content_addressed() -> None:
    module = importlib.import_module("rquant.screen.intraday_reference")
    now = datetime(2026,7,31,1,40,tzinfo=UTC)
    reference = module.IntradayReferenceSnapshot(
        trade_date=date(2026,7,31),available_at=now,producer_commit="a"*40,
        universe_source_id="b"*64,universe_available_at=now,
        universe_codes=("600000.SH","600001.SH"),open_dates=(date(2026,7,30),date(2026,7,31)),
        calendar_available_at=now-timedelta(days=1),calendar_source_id="c"*64,observations=(),
    )
    assert len(reference.identity) == 64
    changed = reference.model_dump(exclude={"identity"}) | {"universe_codes":("600000.SH",)}
    assert module.IntradayReferenceSnapshot.model_validate(changed).identity != reference.identity
    with pytest.raises(ValueError):
        module.IntradayReferenceSnapshot.model_validate(changed | {"universe_codes":("600000.SH","600000.SH")})


def test_future_reference_cannot_supply_preclose_limits_or_float() -> None:
    module = importlib.import_module("rquant.screen.intraday_reference")
    now = datetime(2026,7,31,1,40,tzinfo=UTC)
    fact = module.IntradayReferenceObservation(ts_code="600000.SH",trade_date=date(2026,7,31),
        source_id="a"*64,source_event_time=now,available_at=now+timedelta(seconds=1),
        pre_close=10,up_limit=11,float_shares=1000)
    assert module.visible_intraday_reference(fact,trade_date=date(2026,7,31),cutoff=now) is None


def wire_stocks() -> tuple[IntradayStockSnapshot, ...]:
    from rquant.feature_contracts import FeatureAvailability
    from rquant.screen.intraday_contracts import (
        INTRADAY_FIELD_LABELS,
        IntradayFieldFact,
        IntradayStockSnapshot,
    )

    return tuple(
        IntradayStockSnapshot(
            ts_code=code,
            fields=tuple(
                IntradayFieldFact(
                    name=name, status=FeatureAvailability.UNAVAILABLE, reason="missing_source_field"
                )
                for name in sorted(INTRADAY_FIELD_LABELS)
            ),
        )
        for code in ("600000.SH", "600001.SH")
    )


def test_old_stock_json_and_canonical_digest_are_unchanged_without_closed_proof() -> None:
    from rquant.runtime_contracts import canonical_sha256
    from rquant.screen.intraday_contracts import (
        IntradayStockProjectionRow,
        decode_intraday_stock_rows,
        encode_intraday_stock_rows,
    )

    stocks = wire_stocks()
    for stock in stocks:
        assert stock.model_dump() == stock.model_dump(exclude={"closed_bar"})
        assert canonical_sha256(stock) == canonical_sha256(stock.model_dump(exclude={"closed_bar"}))
    legacy = tuple(
        IntradayStockProjectionRow(
            source_identity="a" * 64, ts_code=stock.ts_code, payload_json=stock.model_dump_json()
        )
        for stock in stocks
    )
    assert decode_intraday_stock_rows(legacy, source_identity="a" * 64) == stocks
    assert (
        decode_intraday_stock_rows(
            encode_intraday_stock_rows("a" * 64, stocks), source_identity="a" * 64
        )
        == stocks
    )


@pytest.mark.parametrize(
    "failure",
    [
        "unknown_type",
        "bad_zlib",
        "bad_hash",
        "wrong_length",
        "trailing_stream",
        "bad_reference",
        "foreign_source",
        "missing_member",
        "duplicate_member",
        "duplicate_json_key",
        "zip_expansion",
        "too_many_chunk_members",
    ],
)
def test_compact_wire_rejects_bad_prefix_without_partial_stocks(failure: str) -> None:
    import base64
    import hashlib
    import json
    import zlib
    from rquant.screen.intraday_contracts import (
        decode_intraday_stock_rows,
        encode_intraday_stock_rows,
    )

    rows = list(encode_intraday_stock_rows("a" * 64, wire_stocks()))
    header = json.loads(rows[0].payload_json)
    if failure == "unknown_type":
        header["contract"] = "unknown/v1"
    elif failure == "bad_zlib":
        header["encoded"] = base64.b64encode(b"bad").decode()
    elif failure == "bad_hash":
        header["sha256"] = "b" * 64
    elif failure == "wrong_length":
        header["decoded_bytes"] += 1
    elif failure == "trailing_stream":
        header["encoded"] = base64.b64encode(
            base64.b64decode(header["encoded"]) + zlib.compress(b"later")
        ).decode()
    elif failure == "bad_reference":
        rows[1] = rows[1].model_copy(
            update={"payload_json": rows[1].payload_json.replace('"index":1', '"index":2')}
        )
    elif failure == "foreign_source":
        rows[1] = rows[1].model_copy(update={"source_identity": "b" * 64})
    elif failure == "missing_member":
        rows.pop()
    elif failure == "duplicate_member":
        rows[1] = rows[0]
    elif failure == "duplicate_json_key":
        rows[0] = rows[0].model_copy(
            update={
                "payload_json": rows[0].payload_json.replace(
                    '"decoded_bytes":', '"decoded_bytes":1,"decoded_bytes":'
                )
            }
        )
    elif failure == "zip_expansion":
        header["encoded"] = base64.b64encode(zlib.compress(b"x" * (2 * 1024 * 1024 + 1))).decode()
    elif failure == "too_many_chunk_members":
        body = json.dumps(
            {"stocks": [wire_stocks()[0].model_dump(mode="json")] * 65}, separators=(",", ":")
        ).encode()
        header.update(
            decoded_bytes=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
            encoded=base64.b64encode(zlib.compress(body)).decode(),
        )
    if failure != "duplicate_json_key":
        rows[0] = rows[0].model_copy(update={"payload_json": json.dumps(header)})
    with pytest.raises(ValueError):
        decode_intraday_stock_rows(tuple(rows), source_identity="a" * 64)
