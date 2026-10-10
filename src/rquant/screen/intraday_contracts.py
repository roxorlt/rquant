"""One typed source contract for screening and condition alert evaluation."""

from __future__ import annotations

import base64
import hashlib
import zlib
from datetime import date
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from rquant.feature_contracts import FeatureAvailability, FeatureFieldStatus
from rquant.live_contracts import BatchEnvelope
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.schema_compatibility import ConsumerCapabilityReceipt
from rquant.screen.intraday_reference import Code, Sha256
from rquant.strict_json import strict_json_loads

INTRADAY_FIELD_LABELS = {
    "INTRADAY_PRICE[0]":"盘中最新价（元）",
    "INTRADAY_OPEN[0]":"盘中开盘价（元）",
    "INTRADAY_HIGH[0]":"盘中最高价（元）",
    "INTRADAY_LOW[0]":"盘中最低价（元）",
    "INTRADAY_VOLUME[0]":"盘中累计成交量（股）",
    "INTRADAY_AMOUNT[0]":"盘中累计成交额（元）",
    "INTRADAY_PCT_CHG[0]":"盘中涨跌幅（%）",
    "INTRADAY_SPEED_5M[0]":"5 分钟涨速（%）",
    "INTRADAY_REL_AMOUNT[0]":"累计相对成交额（倍）",
    "INTRADAY_SAME_MINUTE_AMOUNT[0]":"同分钟相对成交额（倍）",
    "INTRADAY_VOLUME_RATIO[0]":"盘中成交量比（倍）",
    "INTRADAY_TURNOVER_RATE[0]":"盘中换手率（%）",
    "INTRADAY_LIMIT_DISTANCE[0]":"距涨停（%）",
}


class IntradaySchemaEvidence(RuntimeContractModel):
    plan_id: Sha256
    revision: int = Field(ge=1)
    phase: Literal["cutover", "retire"]
    consumer_receipt: ConsumerCapabilityReceipt


class IntradayRawInputReceipt(RuntimeContractModel):
    source_generation_id: Sha256
    envelope: BatchEnvelope


class IntradayFieldFact(RuntimeContractModel):
    name: str
    value: float | None = Field(default=None, allow_inf_nan=False)
    status: FeatureAvailability
    source_event_time: AwareUtcDatetime | None = None
    available_at: AwareUtcDatetime | None = None
    source_id: Sha256 | None = None
    reason: str | None = None
    original_feature_status: FeatureFieldStatus | None = None

    @model_validator(mode="after")
    def require_observed_available_value(self) -> Self:
        if self.name not in INTRADAY_FIELD_LABELS:
            raise ValueError("unknown intraday field")
        if self.status is FeatureAvailability.AVAILABLE and (
            self.value is None or self.source_event_time is None or self.available_at is None
            or self.source_id is None or self.reason is not None
        ):
            raise ValueError("available intraday field lacks observed evidence")
        if self.status is not FeatureAvailability.AVAILABLE and not self.reason:
            raise ValueError("unknown intraday field requires a reason")
        if self.source_event_time is not None and (
            self.available_at is None or self.source_event_time > self.available_at
        ):
            raise ValueError("intraday field availability precedes its event")
        return self


class IntradayClosedBarSnapshot(RuntimeContractModel):
    bar_end: AwareUtcDatetime
    fields: tuple[IntradayFieldFact, ...]

    @model_validator(mode="after")
    def require_exact_closed_line(self) -> Self:
        if self.bar_end.second or self.bar_end.microsecond:
            raise ValueError("closed intraday bar is not an exact minute end")
        if tuple(sorted(item.name for item in self.fields)) != tuple(sorted(INTRADAY_FIELD_LABELS)):
            raise ValueError("closed intraday field coverage changed")
        if any(
            fact.source_event_time is not None and fact.source_event_time != self.bar_end
            for fact in self.fields
        ):
            raise ValueError("closed intraday field uses another source line")
        return self


class IntradayStockSnapshot(RuntimeContractModel):
    ts_code: Code
    name: str | None = None
    feature_time: AwareUtcDatetime | None = None
    fields: tuple[IntradayFieldFact, ...]
    # Omitting an absent addition preserves old v1 stock JSON and canonical digests.
    closed_bar: IntradayClosedBarSnapshot | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def require_one_fact_per_field(self) -> Self:
        if tuple(sorted(item.name for item in self.fields)) != tuple(sorted(INTRADAY_FIELD_LABELS)):
            raise ValueError("intraday stock field coverage changed")
        if self.closed_bar is not None and self.closed_bar.bar_end != self.feature_time:
            raise ValueError("closed intraday bar differs from the original feature time")
        return self

class IntradayMarketRow(RuntimeContractModel):
    as_of: AwareUtcDatetime
    ts_code: Code
    name: str | None = None
    price: float | None = Field(default=None, allow_inf_nan=False)
    open: float | None = Field(default=None, allow_inf_nan=False)
    high: float | None = Field(default=None, allow_inf_nan=False)
    low: float | None = Field(default=None, allow_inf_nan=False)
    pre_close: float | None = Field(default=None, allow_inf_nan=False)
    pct_chg: float | None = Field(default=None, allow_inf_nan=False)
    volume: float | None = Field(default=None, allow_inf_nan=False)
    amount: float | None = Field(default=None, allow_inf_nan=False)


class IntradayScreenSource(RuntimeContractModel):
    contract: Literal["intraday-screen-source/v1"] = "intraday-screen-source/v1"
    trade_date: date
    cutoff: AwareUtcDatetime
    published_at: AwareUtcDatetime
    daily_anchor_date: date
    reference_identity: Sha256
    reference_file_sha256: Sha256
    universe_source_id: Sha256
    universe_codes: tuple[Code, ...] = Field(min_length=1, max_length=8_000)
    universe_digest: Sha256
    feature_generation_id: Sha256
    feature_sequence: int = Field(ge=0)
    feature_batch_id: str
    feature_payload_sha256: Sha256
    feature_contract_version: Literal[3,4]
    feature_contract_fingerprint: Sha256
    feature_definition_fingerprint: Sha256
    feature_producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    raw_input_receipts: tuple[IntradayRawInputReceipt, ...] = Field(min_length=1,max_length=512)
    raw_prefix_digest: Sha256
    historical_snapshot_id: Sha256
    quote_kind: Literal["minute_snapshot","quote_snapshot"]
    quote_generation_id: Sha256 | None = None
    quote_envelope: BatchEnvelope | None = None
    quote_schema_evidence: IntradaySchemaEvidence | None = None
    quote_units_contract_id: Sha256 | None = None
    units_contract_id: Sha256
    volume_unit: Literal["shares"] = "shares"
    amount_unit: Literal["CNY"] = "CNY"
    missing_codes: tuple[Code, ...]
    coverage_digest: Sha256
    market_digest: Sha256
    stock_digest: Sha256
    schema_evidence: IntradaySchemaEvidence
    source_identity: Sha256 | None = None

    @model_validator(mode="after")
    def bind_source(self) -> Self:
        if self.published_at > self.cutoff or self.daily_anchor_date >= self.trade_date:
            raise ValueError("intraday source uses future publication or daily facts")
        if tuple(sorted(set(self.universe_codes))) != self.universe_codes:
            raise ValueError("intraday universe changed")
        if self.universe_digest != canonical_sha256(self.universe_codes):
            raise ValueError("intraday universe digest changed")
        if tuple(sorted(set(self.missing_codes))) != self.missing_codes or set(self.missing_codes)-set(self.universe_codes):
            raise ValueError("intraday coverage codes changed")
        if self.coverage_digest != canonical_sha256({"universe":self.universe_codes,"missing":self.missing_codes}):
            raise ValueError("intraday coverage digest changed")
        if self.raw_prefix_digest != canonical_sha256(self.raw_input_receipts):
            raise ValueError("intraday raw prefix changed")
        envelopes = [item.envelope for item in self.raw_input_receipts]
        if self.quote_envelope is not None:
            envelopes.append(self.quote_envelope)
        if any(max(item.event_time_end,item.source_time,item.received_at,item.available_at)>self.cutoff for item in envelopes):
            raise ValueError("intraday source contains future raw evidence")
        if self.schema_evidence.consumer_receipt.available_at > self.cutoff:
            raise ValueError("intraday schema capability is not visible")
        quote_values=(self.quote_generation_id,self.quote_envelope,self.quote_schema_evidence,self.quote_units_contract_id)
        if (self.quote_kind=="quote_snapshot") != all(value is not None for value in quote_values) or (
            self.quote_kind=="minute_snapshot" and any(value is not None for value in quote_values)
        ):
            raise ValueError("intraday quote source evidence is incomplete")
        if self.quote_schema_evidence is not None and self.quote_schema_evidence.consumer_receipt.available_at>self.cutoff:
            raise ValueError("quote schema capability is not visible")
        expected = canonical_sha256(self.model_dump(mode="python",exclude={"source_identity"}))
        if self.source_identity is None:
            object.__setattr__(self,"source_identity",expected)
        elif self.source_identity != expected:
            raise ValueError("intraday source identity changed")
        return self


class IntradayScreenSnapshot(RuntimeContractModel):
    source: IntradayScreenSource
    stocks: tuple[IntradayStockSnapshot, ...] = Field(max_length=8_000)
    market_rows: tuple[IntradayMarketRow, ...] = Field(max_length=8_000)

    @model_validator(mode="after")
    def require_same_cutoff_and_content(self) -> Self:
        if tuple(item.ts_code for item in self.stocks) != self.source.universe_codes:
            raise ValueError("intraday stock coverage differs from its universe")
        if canonical_sha256(self.stocks) != self.source.stock_digest or canonical_sha256(self.market_rows) != self.source.market_digest:
            raise ValueError("intraday snapshot content changed")
        if any(item.as_of > self.source.cutoff for item in self.market_rows):
            raise ValueError("intraday quote is later than its cutoff")
        for stock in self.stocks:
            if stock.feature_time is not None and stock.feature_time>self.source.cutoff:
                raise ValueError("intraday stock uses a future feature time")
            for fact in (*stock.fields, *(stock.closed_bar.fields if stock.closed_bar else ())):
                if fact.available_at is not None and fact.available_at > self.source.cutoff:
                    raise ValueError("intraday field is later than its cutoff")
        return self


_MAX_WIRE_BYTES = 60 * 1024
_MAX_CHUNK_BYTES = 2 * 1024 * 1024
_MAX_DECODED_STOCK_BYTES = 96 * 1024 * 1024


class IntradayEvidenceWire(RuntimeContractModel):
    contract: Literal["intraday-source-zlib/v1", "intraday-stock-chunk/v1"]
    decoded_bytes: int = Field(ge=1, le=_MAX_CHUNK_BYTES, strict=True)
    sha256: Sha256
    encoded: str = Field(min_length=1, max_length=_MAX_WIRE_BYTES)

    def inflate(self) -> bytes:
        try:
            compressed = base64.b64decode(self.encoded, validate=True)
            decoder = zlib.decompressobj()
            body = decoder.decompress(compressed, self.decoded_bytes + 1)
            if (
                len(body) != self.decoded_bytes
                or not decoder.eof
                or decoder.unused_data
                or decoder.unconsumed_tail
                or hashlib.sha256(body).hexdigest() != self.sha256
            ):
                raise ValueError("intraday wire length, prefix or hash changed")
            return body
        except (ValueError, zlib.error) as error:
            raise ValueError("intraday wire is invalid") from error


class IntradayStockChunk(RuntimeContractModel):
    stocks: tuple[IntradayStockSnapshot, ...] = Field(min_length=1, max_length=64)


class IntradayStockWireReference(RuntimeContractModel):
    contract: Literal["intraday-stock-ref/v1"] = "intraday-stock-ref/v1"
    chunk_code: Code
    index: int = Field(ge=1, le=63, strict=True)


class IntradayStockProjectionRow(RuntimeContractModel):
    source_identity: Sha256
    ts_code: Code
    payload_json: str = Field(min_length=2, max_length=64 * 1024)

    @field_validator("payload_json")
    @classmethod
    def require_original_cell_bound(cls, value: str) -> str:
        if len(value.encode()) > 64 * 1024:
            raise ValueError("intraday stock wire exceeds the original cell budget")
        return value


def _encode_evidence_wire(
    contract: Literal["intraday-source-zlib/v1", "intraday-stock-chunk/v1"], body: bytes
) -> str:
    if len(body) > _MAX_CHUNK_BYTES:
        raise ValueError("intraday decoded chunk exceeds its bound")
    wire = IntradayEvidenceWire(
        contract=contract,
        decoded_bytes=len(body),
        sha256=hashlib.sha256(body).hexdigest(),
        encoded=base64.b64encode(zlib.compress(body, level=9)).decode("ascii"),
    ).model_dump_json()
    if len(wire.encode()) > _MAX_WIRE_BYTES:
        raise ValueError("intraday wire exceeds the original cell budget")
    return wire


def encode_intraday_source_wire(source: IntradayScreenSource) -> str:
    return _encode_evidence_wire("intraday-source-zlib/v1", source.model_dump_json().encode())


def decode_intraday_source_wire(body: str) -> IntradayScreenSource:
    if len(body.encode()) > 2 * 1024 * 1024:
        raise ValueError("intraday source wire exceeds its bound")
    value = strict_json_loads(body)
    if isinstance(value, dict) and value.get("contract") == "intraday-source-zlib/v1":
        wire = IntradayEvidenceWire.model_validate(value)
        value = strict_json_loads(wire.inflate())
    return IntradayScreenSource.model_validate(value)


def encode_intraday_stock_rows(
    source_identity: str, stocks: tuple[IntradayStockSnapshot, ...]
) -> tuple[IntradayStockProjectionRow, ...]:
    if len(stocks) > 8000:
        raise ValueError("intraday stock wire exceeds its member bound")
    result: list[IntradayStockProjectionRow] = []
    decoded_bytes = 0

    def append_chunk(members: tuple[IntradayStockSnapshot, ...]) -> None:
        nonlocal decoded_bytes
        body = IntradayStockChunk(stocks=members).model_dump_json().encode()
        try:
            encoded = _encode_evidence_wire("intraday-stock-chunk/v1", body)
        except ValueError:
            if len(members) == 1:
                raise
            split = len(members) // 2
            append_chunk(members[:split])
            append_chunk(members[split:])
            return
        decoded_bytes += len(body)
        if decoded_bytes > _MAX_DECODED_STOCK_BYTES:
            raise ValueError("intraday decoded stocks exceed their bound")
        for index, stock in enumerate(members):
            payload = (
                encoded
                if index == 0
                else IntradayStockWireReference(
                    chunk_code=members[0].ts_code, index=index
                ).model_dump_json()
            )
            result.append(
                IntradayStockProjectionRow(
                    source_identity=source_identity, ts_code=stock.ts_code, payload_json=payload
                )
            )

    for start in range(0, len(stocks), 64):
        append_chunk(stocks[start : start + 64])
    return tuple(result)


def decode_intraday_stock_rows(
    rows: tuple[IntradayStockProjectionRow, ...], *, source_identity: str
) -> tuple[IntradayStockSnapshot, ...]:
    keys = tuple(row.ts_code for row in rows)
    if (
        len(rows) > 8000
        or keys != tuple(sorted(set(keys)))
        or any(row.source_identity != source_identity for row in rows)
    ):
        raise ValueError("intraday stock wire members or source changed")
    result: list[IntradayStockSnapshot] = []
    decoded_bytes = 0
    index = 0
    while index < len(rows):
        row = rows[index]
        value = strict_json_loads(row.payload_json)
        if isinstance(value, dict) and value.get("contract") == "intraday-stock-chunk/v1":
            wire = IntradayEvidenceWire.model_validate(value)
            decoded_bytes += wire.decoded_bytes
            if decoded_bytes > _MAX_DECODED_STOCK_BYTES:
                raise ValueError("intraday decoded stocks exceed their bound")
            chunk = IntradayStockChunk.model_validate(strict_json_loads(wire.inflate()))
            members = chunk.stocks
            if tuple(stock.ts_code for stock in members) != keys[index : index + len(members)]:
                raise ValueError("intraday stock chunk coverage changed")
            for position in range(1, len(members)):
                reference = IntradayStockWireReference.model_validate(
                    strict_json_loads(rows[index + position].payload_json)
                )
                if reference.chunk_code != row.ts_code or reference.index != position:
                    raise ValueError("intraday stock chunk reference changed")
        else:
            decoded_bytes += len(row.payload_json.encode())
            if decoded_bytes > _MAX_DECODED_STOCK_BYTES:
                raise ValueError("intraday decoded stocks exceed their bound")
            members = (IntradayStockSnapshot.model_validate(value),)
        if members[0].ts_code != row.ts_code:
            raise ValueError("intraday stock row differs from its evidence")
        result.extend(members)
        index += len(members)
    return tuple(result)

