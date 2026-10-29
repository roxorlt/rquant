"""Lossless vendor observations; source time and subscription remain unverified."""

from __future__ import annotations

import hashlib
import struct
from datetime import datetime, timedelta
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator, model_validator

from .symbols import MAX_SYMBOLS, market_for_code, normalize_symbols

PositiveInt = Annotated[StrictInt, Field(ge=1)]
Count = Annotated[StrictInt, Field(ge=0)]
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
HexBytes = Annotated[str, Field(pattern=r"^(?:[0-9a-f]{2})*$", max_length=2 * 1024 * 1024)]
EpochId = Annotated[str, Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")]
ErrorCode = Literal[
    "transport_error", "peer_closed", "invalid_framing", "frame_length_limit",
    "frame_timeout", "byte_limit", "frame_limit", "decode_error", "association_mismatch",
    "order_side_mismatch", "seed_timeout", "protocol_error", "cache_error", "writer_busy",
    "restart_gap", "input_error", "duration_limit", "signal_stop", "unknown_market",
    "invalid_status",
]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("received_at", "cached_at", "last_transport_received_at", "last_market_received_at",
                     "seed_received_at", mode="before", check_fields=False)
    @classmethod
    def reject_numeric_datetime(cls, value: object, info: object) -> object:
        if isinstance(value, (int, float)) and getattr(info, "field_name", "") in {
            "received_at", "cached_at", "last_transport_received_at", "last_market_received_at",
            "seed_received_at",
        }:
            raise ValueError("UTC timestamps require an explicit datetime")
        return value

    @field_validator("received_at", "cached_at", "last_transport_received_at", "last_market_received_at",
                     "seed_received_at", mode="after", check_fields=False)
    @classmethod
    def require_utc(cls, value: object) -> object:
        if isinstance(value, datetime) and (value.tzinfo is None or value.utcoffset() != timedelta(0)):
            raise ValueError("timestamp must be aware UTC")
        return value


class Binding(Model):
    page_id: Annotated[StrictInt, Field(ge=1, le=65535)]
    kind: Literal["vendor4013", "vendor4027"]
    code: StrictStr
    market: Literal[17, 33]
    side: Literal["buy", "sell"] | None = None

    @model_validator(mode="after")
    def validate_binding(self) -> Binding:
        if self.market != market_for_code(self.code):
            raise ValueError("stock and market mismatch")
        if (self.kind == "vendor4013") != (self.side is None):
            raise ValueError("side must be present only for vendor4027")
        return self


class SubscriptionTargets(Model):
    revision: Count
    symbols: tuple[StrictStr, ...]
    max_symbols: Annotated[StrictInt, Field(ge=1, le=MAX_SYMBOLS)]

    @model_validator(mode='after')
    def validate_targets(self) -> SubscriptionTargets:
        if normalize_symbols(self.symbols) != self.symbols or len(self.symbols) > self.max_symbols:
            raise ValueError('invalid desired symbol set')
        if (self.revision == 0) != (not self.symbols):
            raise ValueError('desired revision and membership disagree')
        return self


class SubscriptionUpdate(SubscriptionTargets):
    added: tuple[StrictStr, ...]

    @model_validator(mode='after')
    def validate_added(self) -> SubscriptionUpdate:
        if normalize_symbols(self.added) != self.added or any(s not in self.symbols for s in self.added):
            raise ValueError('invalid added symbols')
        return self


class CapacityPolicy(Model):
    max_symbols: Annotated[StrictInt, Field(ge=1, le=MAX_SYMBOLS)]
    source_tree: Annotated[StrictStr, Field(pattern=r'^[0-9a-f]{40}$')]
    acceptance_sha256: Sha256


class SubscriptionStream(Model):
    binding: Binding
    state: Literal['queued', 'sent', 'seeded', 'no_data', 'first_push_observed', 'failed']
    request_id: Annotated[StrictInt, Field(ge=3, le=902)] | None = None
    seed_sequence: PositiveInt | None = None
    seed_received_at: datetime | None = None
    first_push_sequence: PositiveInt | None = None

    @model_validator(mode='after')
    def validate_transition_receipt(self) -> SubscriptionStream:
        if (self.seed_sequence is None) != (self.seed_received_at is None):
            raise ValueError('seed receipt is incomplete')
        if self.state == 'queued' and any(v is not None for v in (
                self.request_id, self.seed_sequence, self.first_push_sequence)):
            raise ValueError('queued flow cannot have sent or market receipts')
        if self.state in {'sent', 'seeded', 'no_data', 'first_push_observed'} and self.request_id is None:
            raise ValueError('issued flow requires its request identity')
        if self.state == 'seeded' and self.seed_sequence is None:
            raise ValueError('seeded flow requires a market receipt')
        if self.state == 'no_data' and self.seed_sequence is not None:
            raise ValueError('no-data is not a seed')
        if self.state == 'first_push_observed' and self.first_push_sequence is None:
            raise ValueError('push transition requires a market receipt')
        return self


class SubscriptionReceipt(Model):
    epoch_id: EpochId
    desired_revision: Count
    collector_state: Literal['unconfigured', 'starting', 'connected', 'seeded_waiting',
                             'listening_candidate', 'disconnected', 'blocked', 'stopped']
    streams: tuple[SubscriptionStream, ...] = Field(max_length=3 * MAX_SYMBOLS)

    @model_validator(mode='after')
    def validate_bindings(self) -> SubscriptionReceipt:
        pages = [s.binding.page_id for s in self.streams]
        identities = [(s.binding.code, s.binding.kind, s.binding.side) for s in self.streams]
        if len(set(pages)) != len(pages) or len(set(identities)) != len(identities):
            raise ValueError('duplicate subscription binding')
        return self


class RawCell(Model):
    value: StrictStr | StrictInt | None
    raw_hex: HexBytes
    style: Annotated[StrictInt, Field(ge=0, le=255)]


class RawColumn(Model):
    field_id: Count
    flags: Annotated[StrictInt, Field(ge=0, le=65535)]
    flags_hex: Annotated[str, Field(pattern=r"^0x[0-9a-f]{1,4}$")]
    width: Annotated[StrictInt, Field(ge=1, le=255)]
    extra: Annotated[StrictInt, Field(ge=0, le=255)]
    encoding: Literal["unsigned-le", "packed32-le", "utf-16le", "raw-hex"]

    @model_validator(mode="after")
    def validate_flags(self) -> RawColumn:
        if int(self.flags_hex, 16) != self.flags or self.field_id != self.flags & 0x8FFF:
            raise ValueError("column flags mismatch")
        return self


class RawTable(Model):
    title: str
    table_version: Literal["tb1.0", "tb3.0"]
    source_codec: Literal["snappy"]
    source_sha256: Sha256
    row_data_sha256: Sha256
    row_count: Annotated[StrictInt, Field(ge=0,
        description="Must match the supported table geometry and rows; logical row data is at most 1048576 bytes.")]
    row_width: PositiveInt
    col_count: Annotated[StrictInt, Field(ge=1, le=32)]
    extended_header_length: Annotated[StrictInt, Field(ge=24, le=65535)]
    header_unknown_10: StrictInt
    header_unknown_14: StrictInt
    columns: tuple[RawColumn, ...]
    rows: tuple[dict[str, RawCell], ...]
    interpretation_limits: str
    rle_algorithm_reference: str | None = None

    @model_validator(mode="before")
    @classmethod
    def preflight_shape(cls, value: object) -> object:
        if isinstance(value, RawTable):
            shape = {name: getattr(value, name) for name in (
                "row_count", "row_width", "col_count", "columns", "rows", "extended_header_length")}
        elif isinstance(value, dict):
            shape = value
        else:
            return value
        rows, width, count = (shape.get(k) for k in ("row_count", "row_width", "col_count"))
        if (type(rows) is not int or type(width) is not int or type(count) is not int
                or rows < 0 or (count, width) not in {(8, 66), (6, 30)}
                or rows * width > 1024 * 1024):
            raise ValueError("table geometry exceeds byte bounds")
        extended = shape.get("extended_header_length")
        if type(extended) is not int or not 24 + count * 4 <= extended <= 65535:
            raise ValueError("table shape has an invalid extended header")
        columns, row_list = shape.get("columns"), shape.get("rows")
        if (not isinstance(columns, (list, tuple)) or len(columns) != count
                or not isinstance(row_list, (list, tuple)) or len(row_list) != rows):
            raise ValueError("table shape does not match declared counts")
        descriptor = []
        ids = []
        for column in columns:
            if isinstance(column, RawColumn):
                flags, cell_width, extra = column.flags, column.width, column.extra
            elif isinstance(column, dict):
                flags, cell_width, extra = (column.get(k) for k in ("flags", "width", "extra"))
            else:
                raise ValueError("table descriptor is invalid")
            if (type(flags) is not int or not 0 <= flags <= 65535
                    or type(cell_width) is not int or not 1 <= cell_width <= 255
                    or type(extra) is not int or not 0 <= extra <= 255):
                raise ValueError("table descriptor is invalid")
            descriptor.append((flags, cell_width, extra))
            ids.append(flags & 0x8FFF)
        if len(set(ids)) != count or sum(c[1] + 1 for c in descriptor) != width:
            raise ValueError("table descriptor widths or fields mismatch")
        if rows > 500:
            expected = (((0x2001, 4, 165), (0x38, 30, 165), (0x100A, 4, 165), (0x100D, 4, 165),
                         (0x104A, 4, 165), (0x104B, 4, 165), (0xA610, 4, 165), (0x1012, 4, 165))
                        if count == 8 else
                        ((0x2001, 4, 220), (0x100A, 4, 220), (0x100D, 4, 220),
                         (0x200C, 4, 220), (0x2038, 4, 220), (0xA561, 4, 220)))
            if tuple(descriptor) != expected:
                raise ValueError("table descriptor is unsupported for expanded rows")
        return value

    @model_validator(mode="after")
    def validate_rows(self) -> RawTable:
        if (len(self.rows) != self.row_count or len(self.columns) != self.col_count
                or sum(c.width + 1 for c in self.columns) != self.row_width
                or self.row_count * self.row_width > 1024 * 1024):
            raise ValueError("table dimensions mismatch")
        keys = {str(c.field_id) for c in self.columns}
        if len(keys) != self.col_count:
            raise ValueError("duplicate vendor fields")
        raw = bytearray()
        for row in self.rows:
            if set(row) != keys:
                raise ValueError("row vendor fields mismatch")
            for column in self.columns:
                cell = row[str(column.field_id)]
                data = bytes.fromhex(cell.raw_hex)
                if len(data) != column.width:
                    raise ValueError("cell width mismatch")
                raw.extend(data)
                raw.append(cell.style)
        if hashlib.sha256(raw).hexdigest() != self.row_data_sha256:
            raise ValueError("row data hash mismatch")
        return self


class MarketObservation(Model):
    epoch_id: EpochId
    binding: Binding
    received_at: datetime
    request_id: Annotated[StrictInt, Field(ge=1, le=0xFFFFFFFF)]
    source_fd_ordinal: PositiveInt
    delivery: Literal["seed", "backfill", "candidate_push"]
    response_body_hex: HexBytes
    raw_table_hex: HexBytes
    table: RawTable
    response_sha256: Sha256
    granularity: Literal["granularity_unverified"] = "granularity_unverified"
    coverage: Literal["unknown_day_coverage"] = "unknown_day_coverage"

    @model_validator(mode="before")
    @classmethod
    def preflight_market(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        table = value.get("table")
        RawTable.preflight_shape(table)
        if isinstance(table, RawTable):
            rows, count, width, title, version = (getattr(table, k) for k in (
                "row_count", "col_count", "row_width", "title", "table_version"))
        elif isinstance(table, dict):
            rows, count, width, title, version = (table.get(k) for k in (
                "row_count", "col_count", "row_width", "title", "table_version"))
        else:
            return value
        binding_value = value.get("binding")
        binding = Binding.model_validate(binding_value.model_dump() if isinstance(binding_value, Binding) else binding_value)
        kind, code = binding.kind, binding.code
        body, raw = value.get("response_body_hex"), value.get("raw_table_hex")
        if (not isinstance(body, str) or not 52 <= len(body) <= 2 * 1024 * 1024
                or not isinstance(raw, str) or not 48 <= len(raw) <= 2 * 1024 * 1024):
            raise ValueError("market byte bounds exceeded")
        extended = table.extended_header_length if isinstance(table, RawTable) else table.get("extended_header_length")
        if extended > len(raw) // 2:
            raise ValueError("table shape crosses raw table bytes")
        try:
            header, request, flags, page, size, command = struct.unpack("<HIIHII", bytes.fromhex(body[:40]))
        except (ValueError, struct.error):
            raise ValueError("invalid market record") from None
        trade = kind == "vendor4013"
        seed_envelope = header == 32 and request != 0xFFFFFFFF
        push_envelope = (header, request, flags) == (26, 0xFFFFFFFF, 0x1100)
        if (not (seed_envelope or push_envelope) or request != value.get("request_id")
                or page != binding.page_id or size != len(body) // 2 - header
                or command not in ((2357,) if trade else (2267, 2357))
                or flags not in ((0x1100, 0x1110, 0x1130) if trade else (0x1100,))):
            raise ValueError("market record association mismatch")
        if ((trade and ((count, width, title) != (8, 66, "")
                        or not (version == "tb3.0" or header == 26 and version == "tb1.0")))
                or (not trade and ((count, width, title, version) != (6, 30, f"{code}-逐笔委托", "tb1.0")))):
            raise ValueError("market table kind mismatch")
        if rows > 500 and not ((header, request, flags) == (26, 0xFFFFFFFF, 0x1100)
                              and value.get("delivery") == "candidate_push"):
            raise ValueError("market row count exceeds profile limit")
        return value

    @model_validator(mode="after")
    def validate_market_only(self) -> MarketObservation:
        body, table = bytes.fromhex(self.response_body_hex), bytes.fromhex(self.raw_table_hex)
        if len(body) < 26 or len(table) < 24:
            raise ValueError("invalid market record")
        h, request, flags, page, size, command = struct.unpack_from("<HIIHII", body)
        trade = self.binding.kind == "vendor4013"
        allowed_commands = (2357,) if trade else (2267, 2357)
        allowed_flags = (0x1100, 0x1110, 0x1130) if trade else (0x1100,)
        seed_envelope = h == 32 and request != 0xFFFFFFFF
        push_envelope = (h, request, flags) == (26, 0xFFFFFFFF, 0x1100)
        if (not (seed_envelope or push_envelope)
                or request != self.request_id or page != self.binding.page_id
                or command not in allowed_commands or flags not in allowed_flags or size != len(body) - h):
            raise ValueError("market record association mismatch")
        if self.table.row_count > 500:
            if not (push_envelope and self.delivery == "candidate_push"):
                raise ValueError("market row count exceeds profile limit")
            RawTable.preflight_shape(self.table)
            if not trade:
                expected_side = 1 if self.binding.side == "buy" else 2
                if any(type(row["12"].value) is not int or row["12"].value != expected_side for row in self.table.rows):
                    raise ValueError("expanded order side mismatch")
        layout = (self.table.col_count, self.table.row_width, self.table.title)
        if trade:
            valid_table = (layout == (8, 66, "") and (self.table.table_version == "tb3.0"
                           or h == 26 and self.table.table_version == "tb1.0"))
        else:
            valid_table = layout == (6, 30, f"{self.binding.code}-逐笔委托") and self.table.table_version == "tb1.0"
        if not valid_table:
            raise ValueError("market table kind mismatch")
        if (hashlib.sha256(body).hexdigest() != self.response_sha256
                or hashlib.sha256(table).hexdigest() != self.table.source_sha256):
            raise ValueError("market source hash mismatch")
        return self


class RawL2Batch(MarketObservation):
    local_batch_sequence: PositiveInt
    cached_at: datetime


class FeedStatus(Model):
    mode: Literal["seed_then_passive_listen_candidate"] = "seed_then_passive_listen_candidate"
    state: Literal["unconfigured", "starting", "connected", "seeded_waiting", "listening_candidate",
                   "disconnected", "blocked", "stopped"] = "unconfigured"
    epoch_id: EpochId | None = None
    subscription_state: Literal["requested_unverified", "candidate_push_observed"] = "requested_unverified"
    last_transport_received_at: datetime | None = None
    last_market_received_at: datetime | None = None
    heartbeat_count: Count = 0
    control_count: Count = 0
    market_batch_count: Count = 0
    seed_queries: Count = 0
    unknown_push_count: Count = 0
    reconnect_attempts: Annotated[StrictInt, Field(ge=0, le=2)] = 0
    gap: bool = False
    error_code: ErrorCode | None = None
    last_sequence: Count = 0
    cache_age_seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)] | None = None
    cache_stale: bool = True
    source_freshness: Literal["unknown"] = "unknown"


class BatchPage(Model):
    batches: tuple[RawL2Batch, ...]
    next_sequence: Count
    status: FeedStatus
