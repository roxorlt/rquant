"""Compact cold storage for committed L2 market batches.

The live cache stores every batch as ``batch.json`` plus a 65-byte ``sha256``
sidecar in its own directory.  That layout is correct for the live API but
expensive on disk: the two 4 KiB inodes per batch plus the three redundant
representations inside ``batch.json`` (hex wire body, hex raw table, typed
table JSON) cost roughly 30-40x the wire bytes.  This module compacts a
committed cache prefix into segment files that keep only the wire body and the
identity/timing fields, and derives every dropped field again with the reviewed
codec on read-back.  Restored batches are byte-identical canonical JSON.

Format ``rquant-l2-cold-v1`` (all integers little-endian unless noted)::

    header (4096 B)  magic RQL2COL1 | u16 version | u16 flags | u32 meta_len
                     | meta JSON | zero padding
    block*           u32 stored_len | u32 raw_len | u8 codec | u8 reserved
                     | u16 reserved | codec(raw block)
    index            index JSON
    sha256           32 bytes over header+blocks+index
    trailer          u32 index_len | magic RQL2CEND

A raw block is columnar::

    u32 record_count | u64 base_received_us
    u16 epoch_count  | (u8 len | utf-8 epoch) * epoch_count
    u16 binding_count| (u8 kind | 6 B code | u8 market | u8 side | u16 page) *
    column * 9       | u32 len | payload
    u32 bodies_len   | concatenated wire bodies

The nine columns are sequence delta, received-at delta (us), cached-at offset
from received-at (us), epoch index, binding index, request id, source fd
ordinal, delivery, and body length.  Bodies are already compressed by the
vendor envelope, so the block codec mostly compresses the metadata columns;
``zlib`` is used by default because it ships with CPython and the deployment
venv does not install zstd/lz4.

Compaction is offline (the collector must not be writing the same range); every
segment is verified against its source cache before the manifest entry is
published, and retention only deletes manifest entries that carry
``verified_at``.
"""

from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import struct
import sys
import tempfile
import time
import zlib
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from .models import RawL2Batch
from .cache import FLOOR_SCHEMA, RawL2Cache, read_floor, write_floor

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

MAGIC = b"RQL2COL1"
TRAILER_MAGIC = b"RQL2CEND"
FORMAT_SCHEMA = "rquant-l2-cold-v1"
MANIFEST_SCHEMA = "rquant-l2-cold-manifest-v1"
MANIFEST_NAME = "manifest.json"
SEGMENTS_DIR = "segments"
HEADER_BYTES = 4096
VERSION = 1
DEFAULT_BLOCK_RECORDS = 512
DEFAULT_SEGMENT_BYTES = 64 * 1024 * 1024
DEFAULT_CODEC = "zlib"
DEFAULT_LEVEL = 6
DEFAULT_RESERVE_BYTES = 6 * 1024**3
MAX_BODY_BYTES = 1 << 20
_SEGMENT_NAME = re.compile(r"^l2cold-([0-9]{20})-([0-9]{20})-([0-9a-f]{12})\.l2c$")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_DELIVERIES = ("seed", "backfill", "candidate_push")
_DELIVERY_ID = {name: index for index, name in enumerate(_DELIVERIES)}
_KINDS = ("vendor4013", "vendor4027")
_SIDES = (None, "buy", "sell")
_SIDE_IDS = {name: index for index, name in enumerate(_SIDES)}


class ColdStoreError(RuntimeError):
    """A code-bearing failure that never hides the original source error."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _to_us(clock: datetime) -> int:
    if clock.tzinfo is None or clock.utcoffset() != timedelta(0):
        raise ColdStoreError("invalid_clock")
    delta = clock - _EPOCH
    return ((delta.days * 86400 + delta.seconds) * 1_000_000) + delta.microseconds


def _from_us(value: int) -> datetime:
    return _EPOCH + timedelta(microseconds=value)


def _zigzag_encode(value: int) -> int:
    return value << 1 if value >= 0 else ((-value << 1) - 1)


def _zigzag_decode(value: int) -> int:
    return value >> 1 if not value & 1 else -((value + 1) >> 1)


def _encode_varints(values: Sequence[int]) -> bytes:
    out = bytearray()
    for value in values:
        if type(value) is not int or value < 0:
            raise ColdStoreError("invalid_block")
        while True:
            chunk = value & 0x7F
            value >>= 7
            if value:
                out.append(chunk | 0x80)
            else:
                out.append(chunk)
                break
    return bytes(out)


class _Reader:
    """Bounded cursor over a decoded block byte string."""

    def __init__(self, data: bytes) -> None:
        self.data = memoryview(data)
        self.pos = 0

    def take(self, count: int) -> memoryview:
        if count < 0 or self.pos + count > len(self.data):
            raise ColdStoreError("invalid_block")
        view = self.data[self.pos:self.pos + count]
        self.pos += count
        return view

    def u8(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return int.from_bytes(self.take(2), "little")

    def u32(self) -> int:
        return int.from_bytes(self.take(4), "little")

    def u64(self) -> int:
        return int.from_bytes(self.take(8), "little")

    def varint(self) -> int:
        shift = 0
        value = 0
        while True:
            byte = self.u8()
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return value
            shift += 7
            if shift > 63:
                raise ColdStoreError("invalid_block")

    def varints(self, count: int) -> list[int]:
        return [self.varint() for _ in range(count)]


def _encode_text_column(values: Sequence[str]) -> bytes:
    out = bytearray()
    for value in values:
        raw = value.encode("utf-8")
        if not raw or len(raw) > 64:
            raise ColdStoreError("invalid_epoch")
        out.append(len(raw))
        out += raw
    return bytes(out)


# ---------------------------------------------------------------- block codec


@dataclass(frozen=True)
class _Pending:
    sequence: int
    received_us: int
    cached_offset_us: int
    epoch: str
    binding: tuple[str, str, int, str | None, int]
    request_id: int
    source_fd: int
    delivery: str
    body: bytes


class _BlockBuilder:
    def __init__(self, limit: int) -> None:
        if type(limit) is not int or limit < 1:
            raise ColdStoreError("invalid_block_size")
        self.limit = limit
        self.records: list[_Pending] = []
        self.body_bytes = 0
        self.raw_bytes = 0

    def full(self) -> bool:
        return len(self.records) >= self.limit

    def add(self, batch: RawL2Batch) -> None:
        if not isinstance(batch, RawL2Batch):
            raise ColdStoreError("invalid_batch")
        sequence = batch.local_batch_sequence
        if type(sequence) is not int or sequence < 1:
            raise ColdStoreError("invalid_batch")
        if self.records:
            expected = self.records[-1].sequence + 1
            if sequence != expected:
                raise ColdStoreError("sequence_gap")
        body = bytes.fromhex(batch.response_body_hex)
        if not 26 <= len(body) <= MAX_BODY_BYTES:
            raise ColdStoreError("invalid_batch")
        if batch.delivery not in _DELIVERY_ID:
            raise ColdStoreError("invalid_batch")
        binding = batch.binding
        if binding.kind not in _KINDS or binding.market not in (17, 33):
            raise ColdStoreError("invalid_batch")
        if not 1 <= binding.page_id <= 65535 or len(binding.code.encode("ascii")) != 6:
            raise ColdStoreError("invalid_batch")
        kind = _KINDS.index(binding.kind)
        if (binding.side is None) != (kind == 0) or (binding.side is not None and binding.side not in _SIDES):
            raise ColdStoreError("invalid_batch")
        epoch = batch.epoch_id
        if not 1 <= len(epoch.encode("utf-8")) <= 64:
            raise ColdStoreError("invalid_batch")
        item = _Pending(
            sequence=sequence,
            received_us=_to_us(batch.received_at),
            cached_offset_us=_to_us(batch.cached_at) - _to_us(batch.received_at),
            epoch=epoch,
            binding=(binding.kind, binding.code, binding.market, binding.side, binding.page_id),
            request_id=batch.request_id,
            source_fd=batch.source_fd_ordinal,
            delivery=batch.delivery,
            body=body,
        )
        if not 1 <= item.request_id <= 0xFFFFFFFF or not 1 <= item.source_fd <= 2**32:
            raise ColdStoreError("invalid_batch")
        if abs(item.cached_offset_us) > 86_400_000_000:
            raise ColdStoreError("invalid_batch")
        self.records.append(item)
        self.body_bytes += len(body)
        self.raw_bytes += len(body) + 48

    def encode(self) -> bytes:
        if not self.records:
            raise ColdStoreError("empty_block")
        epochs: list[str] = []
        epoch_index: dict[str, int] = {}
        bindings: list[tuple[str, str, int, str | None, int]] = []
        binding_index: dict[tuple[str, str, int, str | None, int], int] = {}
        for record in self.records:
            if record.epoch not in epoch_index:
                if len(epochs) >= 0xFFFF:
                    raise ColdStoreError("invalid_block")
                epoch_index[record.epoch] = len(epochs)
                epochs.append(record.epoch)
            if record.binding not in binding_index:
                if len(bindings) >= 0xFFFF:
                    raise ColdStoreError("invalid_block")
                binding_index[record.binding] = len(bindings)
                bindings.append(record.binding)
        body = bytearray()
        body += struct.pack("<IQH", len(self.records), self.records[0].received_us, len(epochs))
        body += _encode_text_column(epochs)
        body += struct.pack("<H", len(bindings))
        for kind, code, market, side, page in bindings:
            body.append(_KINDS.index(kind))
            body += code.encode("ascii")
            body.append(market)
            body.append(_SIDE_IDS[side])
            body += struct.pack("<H", page)
        previous_sequence = 0
        previous_received = self.records[0].received_us
        columns = [[] for _ in range(9)]
        for index, record in enumerate(self.records):
            columns[0].append(record.sequence - previous_sequence if index else record.sequence)
            columns[1].append(_zigzag_encode(record.received_us - previous_received if index else 0))
            columns[2].append(_zigzag_encode(record.cached_offset_us))
            columns[3].append(epoch_index[record.epoch])
            columns[4].append(binding_index[record.binding])
            columns[5].append(record.request_id)
            columns[6].append(record.source_fd)
            columns[7].append(_DELIVERY_ID[record.delivery])
            columns[8].append(len(record.body))
            previous_sequence = record.sequence
            previous_received = record.received_us
        for values in columns:
            encoded = _encode_varints(values)
            body += struct.pack("<I", len(encoded)) + encoded
        bodies = b"".join(record.body for record in self.records)
        body += struct.pack("<I", len(bodies)) + bodies
        return bytes(body)


def decode_block(raw: bytes) -> list[RawL2Batch]:
    reader = _Reader(raw)
    count = reader.u32()
    base_received = reader.u64()
    epoch_count = reader.u16()
    if count < 1 or epoch_count < 1:
        raise ColdStoreError("invalid_block")
    epochs = []
    for _ in range(epoch_count):
        length = reader.u8()
        if not 1 <= length <= 64:
            raise ColdStoreError("invalid_block")
        epochs.append(bytes(reader.take(length)).decode("utf-8"))
    binding_count = reader.u16()
    if binding_count < 1:
        raise ColdStoreError("invalid_block")
    bindings = []
    for _ in range(binding_count):
        kind = _KINDS[reader.u8()]
        code = bytes(reader.take(6)).decode("ascii")
        market = reader.u8()
        side = _SIDES[reader.u8()]
        page = reader.u16()
        bindings.append((kind, code, market, side, page))
    columns = []
    for _ in range(9):
        length = reader.u32()
        columns.append(bytes(reader.take(length)))
    bodies_len = reader.u32()
    bodies = bytes(reader.take(bodies_len))
    if reader.pos != len(reader.data):
        raise ColdStoreError("invalid_block")
    column_readers = [_Reader(column) for column in columns]
    values = [column_readers[i].varints(count) for i in range(9)]
    if any(reader_.pos != len(reader_.data) for reader_ in column_readers):
        raise ColdStoreError("invalid_block")
    sequences: list[int] = []
    previous = 0
    for index in range(count):
        delta = values[0][index]
        if index == 0 and delta < 0:
            raise ColdStoreError("invalid_block")
        if index and delta < 1:
            raise ColdStoreError("invalid_block")
        previous += delta if index else delta
        sequences.append(previous)
    received: list[int] = [base_received]
    for index in range(1, count):
        received.append(received[-1] + _zigzag_decode(values[1][index]))
    if any(values[3][i] >= len(epochs) for i in range(count)):
        raise ColdStoreError("invalid_block")
    if any(values[4][i] >= len(bindings) for i in range(count)):
        raise ColdStoreError("invalid_block")
    if any(values[7][i] >= len(_DELIVERIES) for i in range(count)):
        raise ColdStoreError("invalid_block")
    lengths = values[8]
    if sum(lengths) != len(bodies):
        raise ColdStoreError("invalid_block")
    offset = 0
    batches = []
    for index in range(count):
        body = bodies[offset:offset + lengths[index]]
        offset += lengths[index]
        kind, code, market, side, page = bindings[values[4][index]]
        batches.append(_batch_from_parts(
            sequence=sequences[index],
            received_us=received[index],
            cached_us=received[index] + _zigzag_decode(values[2][index]),
            epoch=epochs[values[3][index]],
            kind=kind,
            code=code,
            market=market,
            side=side,
            page=page,
            request_id=values[5][index],
            source_fd=values[6][index],
            delivery=_DELIVERIES[values[7][index]],
            body=body,
        ))
    return batches


def _batch_from_parts(
    *,
    sequence: int,
    received_us: int,
    cached_us: int,
    epoch: str,
    kind: str,
    code: str,
    market: int,
    side: str | None,
    page: int,
    request_id: int,
    source_fd: int,
    delivery: str,
    body: bytes,
) -> RawL2Batch:
    from ._codec import decode_order_response, decode_trade_response

    kwargs = dict(request_id=request_id, page_id=page, code=code, source_fd_ordinal=source_fd)
    if kind == "vendor4013":
        result = decode_trade_response(body, market=market, **kwargs)
    else:
        result = decode_order_response(body, **kwargs)
    return RawL2Batch.model_validate({
        "epoch_id": epoch,
        "binding": {"page_id": page, "kind": kind, "code": code, "market": market, "side": side},
        "received_at": _from_us(received_us),
        "request_id": request_id,
        "source_fd_ordinal": source_fd,
        "delivery": delivery,
        "response_body_hex": body.hex(),
        "raw_table_hex": result["raw_table"].hex(),
        "table": result["table"],
        "response_sha256": result["response_sha256"],
        "granularity": "granularity_unverified",
        "coverage": "unknown_day_coverage",
        "local_batch_sequence": sequence,
        "cached_at": _from_us(cached_us),
    })


# ------------------------------------------------------------- segment format


@dataclass(frozen=True)
class BlockIndex:
    offset: int
    stored_len: int
    raw_len: int
    codec: int
    sha256: str
    first_sequence: int
    records: int


@dataclass(frozen=True)
class SegmentInfo:
    path: Path
    first_sequence: int
    last_sequence: int
    records: int
    stored_bytes: int
    body_bytes: int
    meta_bytes: int
    sha256: str
    codec: str
    block_records: int
    created_at: datetime
    source_cache_root: str
    blocks: int
    epoch_ids: tuple[str, ...] = field(default_factory=tuple)


_CODEC_IDS = {"none": 0, "zlib": 1, "gzip": 2}
_CODEC_NAMES = {value: key for key, value in _CODEC_IDS.items()}


def _compress(raw: bytes, codec: str, level: int) -> bytes:
    if codec == "none":
        return raw
    if codec == "zlib":
        return zlib.compress(raw, level)
    if codec == "gzip":
        return gzip.compress(raw, compresslevel=level, mtime=0)
    raise ColdStoreError("invalid_codec")


def _decompress(stored: bytes, codec: int, expected: int) -> bytes:
    if codec == _CODEC_IDS["none"]:
        raw = stored
    elif codec == _CODEC_IDS["zlib"]:
        try:
            raw = zlib.decompress(stored)
        except zlib.error:
            raise ColdStoreError("segment_corrupt") from None
    elif codec == _CODEC_IDS["gzip"]:
        try:
            raw = gzip.decompress(stored)
        except (OSError, EOFError):
            raise ColdStoreError("segment_corrupt") from None
    else:
        raise ColdStoreError("segment_corrupt")
    if len(raw) != expected:
        raise ColdStoreError("segment_corrupt")
    return raw


class _SegmentWriter:
    """Streams blocks into a pending file, then publishes a self-checking segment."""

    def __init__(
        self,
        cold_root: Path,
        *,
        codec: str,
        level: int,
        block_records: int,
        source_cache_root: str,
        created_at: datetime,
        reserve_bytes: int,
    ) -> None:
        if codec not in _CODEC_IDS:
            raise ColdStoreError("invalid_codec")
        self.codec = codec
        self.level = level
        self.block_records = block_records
        self.source_cache_root = source_cache_root
        self.created_at = created_at
        self.reserve_bytes = reserve_bytes
        self.directory = cold_root / SEGMENTS_DIR
        self.directory.mkdir(parents=True, exist_ok=True)
        self._fd: int | None = None
        self._path: Path | None = None
        self._blocks: list[BlockIndex] = []
        self._builder = _BlockBuilder(block_records)
        self._records = 0
        self._body_bytes = 0
        self._raw_bytes = 0
        self._first_sequence: int | None = None
        self._last_sequence: int | None = None
        self._epochs: set[str] = set()
        self._closed = False
        self._check_disk()
        fd, name = tempfile.mkstemp(prefix=".pending-", suffix=".l2c", dir=self.directory)
        self._fd = fd
        self._path = Path(name)
        with os.fdopen(os.dup(fd), "wb") as stream:
            stream.write(b"\0" * HEADER_BYTES)

    def _check_disk(self) -> None:
        free = shutil.disk_usage(self.directory).free
        # One pending segment plus the published segments must never cross the reserve.
        if free < self.reserve_bytes:
            raise ColdStoreError("disk_budget")

    @property
    def path(self) -> Path:
        if self._path is None:
            raise ColdStoreError("writer_closed")
        return self._path

    @property
    def records(self) -> int:
        return self._records + len(self._builder.records)

    @property
    def raw_bytes(self) -> int:
        return self._raw_bytes + self._builder.raw_bytes

    def add(self, batch: RawL2Batch) -> None:
        if self._closed:
            raise ColdStoreError("writer_closed")
        sequence = batch.local_batch_sequence
        if self._last_sequence is not None and sequence != self._last_sequence + 1:
            raise ColdStoreError("sequence_gap")
        self._builder.add(batch)
        if self._first_sequence is None:
            self._first_sequence = sequence
        self._last_sequence = sequence
        self._epochs.add(batch.epoch_id)
        if self._builder.full():
            self._flush_block()

    def should_rotate(self, segment_bytes: int) -> bool:
        return bool(self._blocks) and self.raw_bytes >= segment_bytes

    def _flush_block(self) -> None:
        if not self._builder.records or self._fd is None:
            return
        raw = self._builder.encode()
        stored = _compress(raw, self.codec, self.level)
        entry = BlockIndex(
            offset=os.lseek(self._fd, 0, os.SEEK_CUR),
            stored_len=len(stored),
            raw_len=len(raw),
            codec=_CODEC_IDS[self.codec],
            sha256=_sha256(raw),
            first_sequence=self._builder.records[0].sequence,
            records=len(self._builder.records),
        )
        header = struct.pack("<IIBBH", entry.stored_len, entry.raw_len, entry.codec, 0, 0)
        self._write(header + stored)
        self._blocks.append(entry)
        self._records += len(self._builder.records)
        self._body_bytes += self._builder.body_bytes
        self._raw_bytes += len(raw)
        self._builder = _BlockBuilder(self.block_records)

    def _write(self, payload: bytes) -> None:
        if self._fd is None:
            raise ColdStoreError("writer_closed")
        written = 0
        while written < len(payload):
            written += os.write(self._fd, payload[written:])

    def close(self) -> SegmentInfo:
        if self._closed:
            raise ColdStoreError("writer_closed")
        self._closed = True
        if self._last_sequence is None or self._first_sequence is None:
            self._discard()
            raise ColdStoreError("empty_segment")
        self._flush_block()
        if self._fd is None:
            raise ColdStoreError("writer_closed")
        index = {
            "blocks": [
                {
                    "offset": block.offset,
                    "stored_len": block.stored_len,
                    "raw_len": block.raw_len,
                    "codec": block.codec,
                    "sha256": block.sha256,
                    "first_sequence": block.first_sequence,
                    "records": block.records,
                }
                for block in self._blocks
            ],
            "records": self._records,
        }
        index_raw = _canonical(index)
        self._write(index_raw)
        index_end = os.lseek(self._fd, 0, os.SEEK_CUR)
        meta = {
            "schema": FORMAT_SCHEMA,
            "version": VERSION,
            "codec": self.codec,
            "level": self.level,
            "block_records": self.block_records,
            "first_sequence": self._first_sequence,
            "last_sequence": self._last_sequence,
            "records": self._records,
            "created_at": self.created_at.isoformat(),
            "source_cache_root": self.source_cache_root,
            "epoch_ids": sorted(self._epochs),
            "body_bytes": self._body_bytes,
            "raw_block_bytes": self._raw_bytes,
        }
        meta_raw = _canonical(meta)
        if len(meta_raw) > HEADER_BYTES - 16:
            self._discard()
            raise ColdStoreError("segment_meta_too_large")
        header = MAGIC + struct.pack("<HHI", VERSION, 0, len(meta_raw)) + meta_raw
        header += b"\0" * (HEADER_BYTES - len(header))
        os.lseek(self._fd, 0, os.SEEK_SET)
        self._write(header)
        os.lseek(self._fd, index_end, os.SEEK_SET)
        digest = self._file_sha(index_end)
        self._write(bytes.fromhex(digest))
        self._write(struct.pack("<I", len(index_raw)))
        self._write(TRAILER_MAGIC)
        os.fsync(self._fd)
        os.close(self._fd)
        self._fd = None
        final = self.directory / f"l2cold-{self._first_sequence:020d}-{self._last_sequence:020d}-{digest[:12]}.l2c"
        if final.exists():
            self.path.unlink(missing_ok=True)
            raise ColdStoreError("segment_exists")
        os.replace(self.path, final)
        _fsync_dir(self.directory)
        return SegmentInfo(
            path=final,
            first_sequence=self._first_sequence,
            last_sequence=self._last_sequence,
            records=self._records,
            stored_bytes=final.stat().st_size,
            body_bytes=self._body_bytes,
            meta_bytes=self._raw_bytes - self._body_bytes,
            sha256=digest,
            codec=self.codec,
            block_records=self.block_records,
            created_at=self.created_at,
            source_cache_root=self.source_cache_root,
            blocks=len(self._blocks),
            epoch_ids=tuple(sorted(self._epochs)),
        )

    def _file_sha(self, length: int) -> str:
        digest = hashlib.sha256()
        with os.fdopen(os.dup(self._fd), "rb") as stream:
            stream.seek(0)
            remaining = length
            while remaining:
                chunk = stream.read(min(1 << 20, remaining))
                if not chunk:
                    raise ColdStoreError("segment_corrupt")
                digest.update(chunk)
                remaining -= len(chunk)
        return digest.hexdigest()

    def _discard(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            finally:
                self._fd = None
        if self._path is not None:
            self._path.unlink(missing_ok=True)
            self._path = None


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ColdSegment:
    """Read-only handle over one published segment file."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.meta = self._read_meta()
        self.blocks, self.index_len = self._read_index()
        self.header_bytes = HEADER_BYTES

    def _read_meta(self) -> dict:
        with self.path.open("rb") as stream:
            header = stream.read(HEADER_BYTES)
            if len(header) != HEADER_BYTES or header[:8] != MAGIC:
                raise ColdStoreError("segment_corrupt")
            version, _flags, meta_len = struct.unpack_from("<HHI", header, 8)
            if version != VERSION or not 1 <= meta_len <= HEADER_BYTES - 16:
                raise ColdStoreError("segment_corrupt")
            raw = header[16:16 + meta_len]
        try:
            meta = json.loads(raw)
        except ValueError:
            raise ColdStoreError("segment_corrupt") from None
        if not isinstance(meta, dict) or meta.get("schema") != FORMAT_SCHEMA or meta.get("version") != VERSION:
            raise ColdStoreError("segment_corrupt")
        return meta

    def _read_index(self) -> tuple[list[BlockIndex], int]:
        size = self.path.stat().st_size
        if size < HEADER_BYTES + 12 + 32:
            raise ColdStoreError("segment_corrupt")
        with self.path.open("rb") as stream:
            stream.seek(size - 12)
            trailer = stream.read(12)
            if len(trailer) != 12 or trailer[4:] != TRAILER_MAGIC:
                raise ColdStoreError("segment_corrupt")
            (index_len,) = struct.unpack("<I", trailer[:4])
            index_end = size - 44
            if index_len < 2 or index_len > index_end - HEADER_BYTES:
                raise ColdStoreError("segment_corrupt")
            stream.seek(index_end - index_len)
            raw = stream.read(index_len)
            stored_sha = stream.read(32)
            if len(raw) != index_len or len(stored_sha) != 32:
                raise ColdStoreError("segment_corrupt")
            stream.seek(0)
            digest = hashlib.sha256()
            remaining = index_end
            while remaining:
                chunk = stream.read(min(1 << 20, remaining))
                if not chunk:
                    raise ColdStoreError("segment_corrupt")
                digest.update(chunk)
                remaining -= len(chunk)
        if digest.digest() != stored_sha:
            raise ColdStoreError("segment_corrupt")
        try:
            index = json.loads(raw)
            entries = index["blocks"]
            blocks = [
                BlockIndex(
                    offset=int(entry["offset"]),
                    stored_len=int(entry["stored_len"]),
                    raw_len=int(entry["raw_len"]),
                    codec=int(entry["codec"]),
                    sha256=str(entry["sha256"]),
                    first_sequence=int(entry["first_sequence"]),
                    records=int(entry["records"]),
                )
                for entry in entries
            ]
        except (ValueError, KeyError, TypeError):
            raise ColdStoreError("segment_corrupt") from None
        if not blocks or index.get("records") != sum(block.records for block in blocks):
            raise ColdStoreError("segment_corrupt")
        return blocks, index_len

    def read_block(self, block: BlockIndex) -> list[RawL2Batch]:
        if block.offset < HEADER_BYTES or block.stored_len < 0 or block.raw_len < 1:
            raise ColdStoreError("segment_corrupt")
        with self.path.open("rb") as stream:
            stream.seek(block.offset)
            header = stream.read(12)
            if len(header) != 12:
                raise ColdStoreError("segment_corrupt")
            stored_len, raw_len, codec, _reserved, _reserved16 = struct.unpack("<IIBBH", header)
            if stored_len != block.stored_len or raw_len != block.raw_len or codec != block.codec:
                raise ColdStoreError("segment_corrupt")
            stored = stream.read(stored_len)
            if len(stored) != stored_len:
                raise ColdStoreError("segment_corrupt")
        raw = _decompress(stored, codec, block.raw_len)
        if _sha256(raw) != block.sha256:
            raise ColdStoreError("segment_corrupt")
        return decode_block(raw)

    def iter_batches(self) -> Iterator[RawL2Batch]:
        for block in self.blocks:
            yield from self.read_block(block)

    def read_batch(self, sequence: int) -> RawL2Batch:
        for block in self.blocks:
            if block.first_sequence <= sequence < block.first_sequence + block.records:
                for batch in self.read_block(block):
                    if batch.local_batch_sequence == sequence:
                        return batch
                break
        raise ColdStoreError("unknown_sequence")


def open_segment(path: Path) -> ColdSegment:
    if _SEGMENT_NAME.match(Path(path).name) is None:
        raise ColdStoreError("invalid_segment_name")
    return ColdSegment(Path(path))


# ------------------------------------------------------------------- manifest


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


_SEGMENT_FILE = re.compile(r"^segments/l2cold-[0-9]{20}-[0-9]{20}-[0-9a-f]{12}\.l2c$")


class SegmentEntry(_Model):
    file: str
    first_sequence: int = Field(ge=1)
    last_sequence: int = Field(ge=1)
    records: int = Field(ge=1)
    stored_bytes: int = Field(ge=0)
    body_bytes: int = Field(ge=0)
    sha256: Sha256
    codec: Literal["none", "zlib", "gzip"]
    block_records: int = Field(ge=1)
    blocks: int = Field(ge=1)
    created_at: datetime
    verified_at: datetime | None = None
    source_cache_root: str

    @field_validator("file")
    @classmethod
    def validate_file(cls, value: str) -> str:
        if not _SEGMENT_FILE.match(value):
            raise ValueError("invalid segment file name")
        return value


class ColdManifest(_Model):
    schema_: Literal["rquant-l2-cold-manifest-v1"] = Field(
        default=MANIFEST_SCHEMA, alias="schema"
    )
    segments: tuple[SegmentEntry, ...] = ()


def _load_manifest(cold_root: Path) -> ColdManifest:
    path = Path(cold_root) / MANIFEST_NAME
    if not path.exists():
        return ColdManifest()
    try:
        return ColdManifest.model_validate_json(path.read_bytes())
    except (OSError, ValueError):
        raise ColdStoreError("invalid_manifest") from None


def _write_manifest(cold_root: Path, manifest: ColdManifest) -> None:
    root = Path(cold_root)
    root.mkdir(parents=True, exist_ok=True)
    payload = manifest.model_dump_json(by_alias=True, exclude_none=True).encode() + b"\n"
    fd, name = tempfile.mkstemp(prefix=".manifest-", dir=root)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, root / MANIFEST_NAME)
        _fsync_dir(root)
    finally:
        Path(name).unlink(missing_ok=True)


def _append_manifest(cold_root: Path, entry: SegmentEntry) -> ColdManifest:
    manifest = _load_manifest(cold_root)
    for existing in manifest.segments:
        if existing.file == entry.file:
            raise ColdStoreError("segment_exists")
        if existing.first_sequence <= entry.last_sequence and entry.first_sequence <= existing.last_sequence:
            raise ColdStoreError("sequence_overlap")
    segments = tuple(sorted(manifest.segments + (entry,), key=lambda item: item.first_sequence))
    updated = ColdManifest(segments=segments)
    _write_manifest(cold_root, updated)
    return updated


def load_manifest(cold_root: Path) -> ColdManifest:
    return _load_manifest(cold_root)


# ----------------------------------------------------------- compaction/verify


@dataclass(frozen=True)
class CompactionResult:
    cold_root: Path
    cache_root: Path
    first_sequence: int
    last_sequence: int
    records: int
    source_batch_bytes: int
    source_removed_files: int
    stored_bytes: int
    body_bytes: int
    segments: tuple[SegmentInfo, ...]
    elapsed_seconds: float
    verified: bool


def _payload_info(cache_root: Path, sequence: int) -> tuple[int, int]:
    """Return (batch.json bytes, sha256 sidecar bytes) for a committed sequence."""
    path = Path(cache_root) / "batches" / f"{sequence:020d}"
    try:
        batch = path / "batch.json"
        sidecar = path / "sha256"
        info = batch.lstat()
        side = sidecar.lstat()
    except OSError:
        raise ColdStoreError("invalid_batch") from None
    if not batch.is_file() or not sidecar.is_file():
        raise ColdStoreError("invalid_batch")
    return info.st_size, side.st_size


def _source_payload(cache_root: Path, sequence: int) -> bytes:
    """Read one committed batch payload and re-check its sidecar checksum."""
    path = Path(cache_root) / "batches" / f"{sequence:020d}"
    try:
        payload = (path / "batch.json").read_bytes()
        digest = (path / "sha256").read_bytes()
    except OSError:
        raise ColdStoreError("invalid_batch") from None
    if len(payload) > 20 * 1024 * 1024 or digest != _sha256(payload).encode() + b"\n":
        raise ColdStoreError("invalid_batch")
    return payload


def _segment_entry(info: SegmentInfo, verified_at: datetime | None) -> SegmentEntry:
    return SegmentEntry(
        file=f"{SEGMENTS_DIR}/{info.path.name}",
        first_sequence=info.first_sequence,
        last_sequence=info.last_sequence,
        records=info.records,
        stored_bytes=info.stored_bytes,
        body_bytes=info.body_bytes,
        sha256=info.sha256,
        codec=info.codec,
        block_records=info.block_records,
        blocks=info.blocks,
        created_at=info.created_at,
        verified_at=verified_at,
        source_cache_root=info.source_cache_root,
    )


def verify_segment(
    segment_path: Path,
    *,
    cache_root: Path | None = None,
) -> dict:
    """Verify a segment structurally and, when a cache is given, byte-for-byte."""
    segment = open_segment(Path(segment_path))
    first = segment.meta["first_sequence"]
    records = 0
    payload_bytes = 0
    for batch in segment.iter_batches():
        if batch.local_batch_sequence != first + records:
            raise ColdStoreError("sequence_gap")
        records += 1
        if cache_root is not None:
            payload = _source_payload(Path(cache_root), batch.local_batch_sequence)
            if batch.model_dump_json().encode() + b"\n" != payload:
                raise ColdStoreError("payload_mismatch")
            payload_bytes += len(payload)
    if records != segment.meta["records"]:
        raise ColdStoreError("segment_corrupt")
    return {
        "file": segment.path.name,
        "first_sequence": first,
        "last_sequence": segment.meta["last_sequence"],
        "records": records,
        "source_payload_bytes": payload_bytes,
        "stored_bytes": segment.path.stat().st_size,
        "sha256": _file_sha(segment.path),
    }


def _file_sha(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _lock_cold_writer(cold_root: Path):
    root = Path(cold_root)
    root.mkdir(parents=True, exist_ok=True)
    fd = os.open(root / ".writer.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise ColdStoreError("writer_busy") from None
    return fd


def compact_cache(
    cache_root: Path,
    cold_root: Path,
    *,
    after_sequence: int = 0,
    until_sequence: int | None = None,
    block_records: int = DEFAULT_BLOCK_RECORDS,
    codec: str = DEFAULT_CODEC,
    level: int = DEFAULT_LEVEL,
    segment_bytes: int = DEFAULT_SEGMENT_BYTES,
    verify: bool = True,
    reserve_bytes: int = DEFAULT_RESERVE_BYTES,
    now: datetime | None = None,
) -> CompactionResult:
    """Compact a committed cache prefix into cold segments.

    ``after_sequence`` is clamped to the cache floor; ``until_sequence`` defaults
    to the published head.  A segment enters the manifest only after a full
    read-back comparison against the source payload bytes.
    """
    clock = now or datetime.now(UTC)
    if clock.tzinfo is None:
        raise ColdStoreError("invalid_clock")
    cache_dir = Path(cache_root)
    cold_dir = Path(cold_root)
    cache = RawL2Cache(cache_dir)
    try:
        floor = cache.floor_sequence()
        head = cache.read_status().last_sequence
    except Exception as exc:  # noqa: BLE001 - cache errors carry their own codes
        raise ColdStoreError(getattr(exc, "code", "invalid_cache")) from None
    start = max(after_sequence, floor)
    stop = head if until_sequence is None else min(until_sequence, head)
    if type(start) is not int or type(stop) is not int or start < 0 or stop < start:
        raise ColdStoreError("invalid_range")
    if stop == start:
        return CompactionResult(
            cold_root=cold_dir, cache_root=cache_dir, first_sequence=0, last_sequence=0,
            records=0, source_batch_bytes=0, source_removed_files=0, stored_bytes=0,
            body_bytes=0, segments=(), elapsed_seconds=0.0, verified=verify,
        )
    existing = _load_manifest(cold_dir).segments if cold_dir.exists() else ()
    first_needed = start + 1
    for entry in existing:
        if entry.first_sequence <= stop and entry.last_sequence >= first_needed:
            raise ColdStoreError("sequence_overlap")
    lock = _lock_cold_writer(cold_dir)
    started = time.perf_counter()
    infos: list[SegmentInfo] = []
    records = 0
    source_batch_bytes = 0
    source_files = 0
    body_bytes = 0
    writer: _SegmentWriter | None = None
    try:
        if shutil.disk_usage(cold_dir).free < reserve_bytes:
            raise ColdStoreError("disk_budget")
        for batch in cache.iter_batches(after_sequence=start, limit=stop - start):
            if writer is None:
                writer = _SegmentWriter(
                    cold_dir, codec=codec, level=level, block_records=block_records,
                    source_cache_root=str(cache_dir), created_at=clock, reserve_bytes=reserve_bytes,
                )
            writer.add(batch)
            payload_size, _sidecar = _payload_info(cache_dir, batch.local_batch_sequence)
            source_batch_bytes += payload_size
            source_files += 2
            records += 1
            body_bytes += len(bytes.fromhex(batch.response_body_hex))
            if writer.should_rotate(segment_bytes):
                info = writer.close()
                infos.append(info)
                _publish_segment(cold_dir, info, cache_dir, verify)
                writer = None
        if writer is not None:
            info = writer.close()
            infos.append(info)
            _publish_segment(cold_dir, info, cache_dir, verify)
            writer = None
    except ColdStoreError:
        if writer is not None:
            writer._discard()
        raise
    finally:
        os.close(lock)
    stored = sum(info.stored_bytes for info in infos)
    return CompactionResult(
        cold_root=cold_dir, cache_root=cache_dir,
        first_sequence=infos[0].first_sequence if infos else 0,
        last_sequence=infos[-1].last_sequence if infos else 0,
        records=records, source_batch_bytes=source_batch_bytes,
        source_removed_files=source_files, stored_bytes=stored,
        body_bytes=body_bytes, segments=tuple(infos),
        elapsed_seconds=time.perf_counter() - started, verified=verify,
    )


def _publish_segment(cold_dir: Path, info: SegmentInfo, cache_dir: Path, verify: bool) -> None:
    if verify:
        verify_segment(info.path, cache_root=cache_dir)
    _append_manifest(cold_dir, _segment_entry(info, datetime.now(UTC) if verify else None))


# ------------------------------------------------------------------- restore


@dataclass(frozen=True)
class RestoreResult:
    destination: Path
    segments: int
    records: int
    first_sequence: int
    last_sequence: int
    payload_bytes: int
    elapsed_seconds: float


def _iter_manifest_segments(cold_root: Path) -> Iterator[tuple[ColdManifest, Path]]:
    manifest = _load_manifest(cold_root)
    for entry in manifest.segments:
        path = Path(cold_root) / entry.file
        if not path.is_file():
            raise ColdStoreError("missing_segment")
        yield entry, path


def iter_batches(cold_root: Path) -> Iterator[RawL2Batch]:
    """Yield restored batches across all manifest segments in sequence order."""
    previous = None
    for entry, path in _iter_manifest_segments(cold_root):
        segment = open_segment(path)
        if segment.meta["first_sequence"] != entry.first_sequence:
            raise ColdStoreError("segment_corrupt")
        count = 0
        for batch in segment.iter_batches():
            if previous is not None and batch.local_batch_sequence != previous + 1:
                raise ColdStoreError("sequence_gap")
            previous = batch.local_batch_sequence
            count += 1
            yield batch
        if count != entry.records:
            raise ColdStoreError("segment_corrupt")


def read_batch(cold_root: Path, sequence: int) -> RawL2Batch:
    for entry, path in _iter_manifest_segments(cold_root):
        if entry.first_sequence <= sequence <= entry.last_sequence:
            return open_segment(path).read_batch(sequence)
    raise ColdStoreError("unknown_sequence")


def mark_verified(cold_root: Path, files: Sequence[str], *, now: datetime | None = None) -> int:
    """Record a successful verification for existing segments (repair path)."""
    clock = now or datetime.now(UTC)
    root = Path(cold_root)
    manifest = _load_manifest(root)
    updated = 0
    entries = []
    wanted = set(files)
    for entry in manifest.segments:
        if entry.file in wanted and entry.verified_at is None:
            if not (root / entry.file).is_file():
                raise ColdStoreError("missing_segment")
            entries.append(entry.model_copy(update={"verified_at": clock}))
            updated += 1
        else:
            entries.append(entry)
    known = {entry.file for entry in manifest.segments}
    unknown = wanted - known
    if unknown:
        raise ColdStoreError("unknown_segment")
    if updated:
        _write_manifest(root, ColdManifest(segments=tuple(entries)))
    return updated


def restore(
    cold_root: Path,
    destination: Path,
    *,
    overwrite: bool = False,
    now: datetime | None = None,
) -> RestoreResult:
    """Restore the manifest into a cache-compatible directory tree.

    The destination keeps the same ``batches/<sequence>/batch.json + sha256``
    layout, writes ``.head`` and, when the first restored sequence is above 1,
    a ``.floor`` marker so the live reader accepts the pruned prefix.
    """
    started = time.perf_counter()
    dest = Path(destination)
    batches_dir = dest / "batches"
    if dest.exists() and any(dest.iterdir()):
        if not overwrite:
            raise ColdStoreError("destination_not_empty")
        if dest.is_symlink() or dest.resolve() == Path(cold_root).resolve():
            raise ColdStoreError("unsafe_path")
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True, mode=0o700)
    batches_dir.mkdir(mode=0o700, exist_ok=True)
    records = 0
    payload_bytes = 0
    first = 0
    last = 0
    clock = now or datetime.now(UTC)
    entries = [entry for entry, _ in _iter_manifest_segments(cold_root)]
    for entry, path in _iter_manifest_segments(cold_root):
        segment = open_segment(path)
        count = 0
        for batch in segment.iter_batches():
            sequence = batch.local_batch_sequence
            if records and sequence != last + 1:
                raise ColdStoreError("sequence_gap")
            if not records:
                first = sequence
            directory = batches_dir / f"{sequence:020d}"
            directory.mkdir(mode=0o700)
            payload = batch.model_dump_json().encode() + b"\n"
            _write_new(directory / "batch.json", payload)
            _write_new(directory / "sha256", _sha256(payload).encode() + b"\n")
            payload_bytes += len(payload)
            records += 1
            last = sequence
            count += 1
        if count != entry.records:
            raise ColdStoreError("segment_corrupt")
    if not records:
        raise ColdStoreError("empty_store")
    os.link(batches_dir / f"{last:020d}" / "batch.json", batches_dir / ".head")
    if first > 1:
        write_floor(dest, first - 1, reason="cold_store_restore", now=clock)
    _fsync_dir(batches_dir)
    return RestoreResult(
        destination=dest, segments=len(entries), records=records, first_sequence=first,
        last_sequence=last, payload_bytes=payload_bytes, elapsed_seconds=time.perf_counter() - started,
    )


def _write_new(path: Path, payload: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


# --------------------------------------------------------------------- stats


def stats(cold_root: Path) -> dict:
    manifest = _load_manifest(cold_root)
    segments = [entry for entry in manifest.segments]
    total_stored = sum(entry.stored_bytes for entry in segments)
    total_records = sum(entry.records for entry in segments)
    total_payload = 0
    missing = []
    for entry in segments:
        path = Path(cold_root) / entry.file
        if not path.is_file():
            missing.append(entry.file)
            continue
        total_payload += path.stat().st_size
    return {
        "schema": MANIFEST_SCHEMA,
        "segments": len(segments),
        "records": total_records,
        "first_sequence": segments[0].first_sequence if segments else 0,
        "last_sequence": segments[-1].last_sequence if segments else 0,
        "stored_bytes": total_stored,
        "file_bytes": total_payload,
        "verified_segments": sum(1 for entry in segments if entry.verified_at is not None),
        "missing_segments": missing,
        "bytes_per_batch": (total_stored / total_records) if total_records else 0.0,
    }


# ----------------------------------------------------------------------- CLI


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m rquant.l2.cold_store")
    commands = parser.add_subparsers(dest="command", required=True)

    compact = commands.add_parser("compact", help="compact a committed cache prefix")
    compact.add_argument("--cache-root", required=True)
    compact.add_argument("--cold-root", required=True)
    compact.add_argument("--after-sequence", type=int, default=0)
    compact.add_argument("--until-sequence", type=int)
    compact.add_argument("--block-records", type=int, default=DEFAULT_BLOCK_RECORDS)
    compact.add_argument("--codec", choices=sorted(_CODEC_IDS), default=DEFAULT_CODEC)
    compact.add_argument("--level", type=int, default=DEFAULT_LEVEL)
    compact.add_argument("--segment-bytes", type=int, default=DEFAULT_SEGMENT_BYTES)
    compact.add_argument("--reserve-bytes", type=int, default=DEFAULT_RESERVE_BYTES)
    compact.add_argument("--no-verify", action="store_true")

    verify = commands.add_parser("verify", help="verify segments against a cache")
    verify.add_argument("--cold-root", required=True)
    verify.add_argument("--cache-root")
    verify.add_argument("--mark", action="store_true",
                        help="record verified_at in the manifest for segments that pass")

    restore_cmd = commands.add_parser("restore", help="restore the store into a cache layout")
    restore_cmd.add_argument("--cold-root", required=True)
    restore_cmd.add_argument("--destination", required=True)
    restore_cmd.add_argument("--overwrite", action="store_true")

    commands.add_parser("stats", help="print store statistics").add_argument("--cold-root", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "compact":
            result = compact_cache(
                Path(args.cache_root), Path(args.cold_root),
                after_sequence=args.after_sequence, until_sequence=args.until_sequence,
                block_records=args.block_records, codec=args.codec, level=args.level,
                segment_bytes=args.segment_bytes, verify=not args.no_verify,
                reserve_bytes=args.reserve_bytes,
            )
            print(_canonical({
                "command": "compact", "records": result.records,
                "first_sequence": result.first_sequence, "last_sequence": result.last_sequence,
                "source_batch_bytes": result.source_batch_bytes,
                "stored_bytes": result.stored_bytes, "body_bytes": result.body_bytes,
                "segments": [segment.path.name for segment in result.segments],
                "elapsed_seconds": round(result.elapsed_seconds, 3),
                "verified": result.verified,
            }).decode())
        elif args.command == "verify":
            cache_root = Path(args.cache_root) if args.cache_root else None
            rows = []
            passed = []
            for entry, path in _iter_manifest_segments(Path(args.cold_root)):
                rows.append(verify_segment(path, cache_root=cache_root))
                passed.append(entry.file)
            marked = mark_verified(Path(args.cold_root), passed) if args.mark else 0
            print(_canonical({"command": "verify", "segments": rows,
                              "marked_verified": marked}).decode())
        elif args.command == "restore":
            result = restore(Path(args.cold_root), Path(args.destination), overwrite=args.overwrite)
            print(_canonical({
                "command": "restore", "destination": str(result.destination),
                "records": result.records, "first_sequence": result.first_sequence,
                "last_sequence": result.last_sequence, "payload_bytes": result.payload_bytes,
            }).decode())
        else:
            print(_canonical(stats(Path(args.cold_root))).decode())
    except ColdStoreError as exc:
        print(_canonical({"command": args.command, "error": exc.code}).decode(), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
