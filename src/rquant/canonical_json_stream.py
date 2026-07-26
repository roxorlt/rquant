"""Bounded-memory canonical JSON primitives used by Strategy Lab digests."""

from __future__ import annotations

import base64
import codecs
import json
import math
import struct
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from enum import Enum

import pandas as pd
import pyarrow as pa
from pandas._libs.json import ujson_dumps

CANONICAL_JSON_STRING_CHUNK_CHARACTERS = 1024
CANONICAL_JSON_STREAM_SCRATCH_BYTES = 128 * 1024
CANONICAL_JSON_BASE64_INPUT_CHUNK_BYTES = 12 * 1024
_MAX_SCALAR_TOKEN_BYTES = 64 * 1024


class CanonicalJsonStreamWriter:
    """Write canonical ASCII JSON directly to a bounded downstream sink."""

    def __init__(self, update: Callable[[bytes], object]) -> None:
        self._update = update

    def write_ascii(self, payload: bytes) -> None:
        self._update(payload)

    def write_string_content(
        self,
        value: str,
        *,
        escape_forward_slash: bool = False,
    ) -> None:
        for start in range(0, len(value), CANONICAL_JSON_STRING_CHUNK_CHARACTERS):
            source = value[start : start + CANONICAL_JSON_STRING_CHUNK_CHARACTERS]
            escaped = json.dumps(
                source,
                ensure_ascii=True,
                separators=(",", ":"),
            )[1:-1]
            if escape_forward_slash:
                escaped = escaped.replace("/", "\\/")
            self._update(escaped.encode("ascii"))

    def write_string(
        self,
        value: str,
        *,
        escape_forward_slash: bool = False,
    ) -> None:
        self._update(b'"')
        self.write_string_content(
            value,
            escape_forward_slash=escape_forward_slash,
        )
        self._update(b'"')

    def write_utf8_string_buffer(
        self,
        value: memoryview,
        *,
        escape_forward_slash: bool = False,
    ) -> None:
        self._update(b'"')
        decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
        for start in range(0, len(value), 4096):
            text = decoder.decode(value[start : start + 4096], final=False)
            self.write_string_content(
                text,
                escape_forward_slash=escape_forward_slash,
            )
        tail = decoder.decode(b"", final=True)
        self.write_string_content(
            tail,
            escape_forward_slash=escape_forward_slash,
        )
        self._update(b'"')

    def write_base64_bytes(self, value: bytes | bytearray | memoryview) -> None:
        """Write one canonical base64 JSON string with 3-byte-aligned chunks."""

        payload = memoryview(value).cast("B")
        self._update(b'"')
        for start in range(0, len(payload), CANONICAL_JSON_BASE64_INPUT_CHUNK_BYTES):
            chunk = payload[start : start + CANONICAL_JSON_BASE64_INPUT_CHUNK_BYTES]
            self._update(base64.b64encode(chunk))
        self._update(b'"')

    def write_value(self, value: object, *, sort_keys: bool = True) -> None:
        if value is None:
            self._update(b"null")
            return
        if isinstance(value, bool):
            self._update(b"true" if value else b"false")
            return
        if isinstance(value, int):
            self._update(str(value).encode("ascii"))
            return
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("canonical JSON numbers must be finite")
            self._update(json.dumps(value, ensure_ascii=True, allow_nan=False).encode("ascii"))
            return
        if isinstance(value, str):
            self.write_string(value)
            return
        if isinstance(value, Mapping):
            if any(not isinstance(key, str) for key in value):
                raise TypeError("canonical JSON mappings require string keys")
            keys = sorted(value) if sort_keys else tuple(value)
            self._update(b"{")
            for index, key in enumerate(keys):
                if index:
                    self._update(b",")
                self.write_string(key)
                self._update(b":")
                self.write_value(value[key], sort_keys=sort_keys)
            self._update(b"}")
            return
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            self._update(b"[")
            for index, item in enumerate(value):
                if index:
                    self._update(b",")
                self.write_value(item, sort_keys=sort_keys)
            self._update(b"]")
            return
        raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


class CanonicalJsonEscapedStringSink:
    """Escape an ASCII JSON stream as the content of another JSON string."""

    def __init__(
        self,
        update: Callable[[bytes], object],
        *,
        buffer_bytes: int = 16 * 1024,
    ) -> None:
        if buffer_bytes < 1 or buffer_bytes > CANONICAL_JSON_STREAM_SCRATCH_BYTES:
            raise ValueError("escaped JSON sink buffer is outside the scratch bound")
        self._writer = CanonicalJsonStreamWriter(update)
        self._buffer = bytearray()
        self._buffer_bytes = buffer_bytes
        self._finished = False

    def update(self, payload: bytes) -> None:
        if self._finished:
            raise RuntimeError("escaped JSON string sink is already finished")
        if not payload:
            return
        view = memoryview(payload)
        while view:
            remaining = self._buffer_bytes - len(self._buffer)
            self._buffer.extend(view[:remaining])
            view = view[remaining:]
            if len(self._buffer) == self._buffer_bytes:
                self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        try:
            text = self._buffer.decode("ascii")
        except UnicodeDecodeError as exc:  # pragma: no cover - producer contract guard
            raise ValueError("nested canonical JSON stream must be ASCII") from exc
        self._writer.write_string_content(text)
        self._buffer.clear()

    def finish(self) -> None:
        if self._finished:
            raise RuntimeError("escaped JSON string sink is already finished")
        self._flush()
        self._finished = True


class PandasJsonColumnAccessor:
    """Read frame cells while keeping Arrow string values descriptor-bound."""

    def __init__(self, series: pd.Series, *, table_context: bool = False) -> None:
        self._array = series.array
        self._table_context = table_context
        self._table_timedelta = table_context and pd.api.types.is_timedelta64_dtype(series.dtype)
        self._table_float = table_context and pd.api.types.is_float_dtype(series.dtype)
        categorical_dtype = series.dtype if isinstance(series.dtype, pd.CategoricalDtype) else None
        self._table_unsigned = table_context and (
            pd.api.types.is_unsigned_integer_dtype(series.dtype)
            or (
                categorical_dtype is not None
                and pd.api.types.is_unsigned_integer_dtype(categorical_dtype.categories.dtype)
            )
        )
        self._table_integer_categorical_with_missing = (
            table_context
            and categorical_dtype is not None
            and pd.api.types.is_integer_dtype(categorical_dtype.categories.dtype)
            and not pd.api.types.is_bool_dtype(categorical_dtype.categories.dtype)
            and series.hasnans
        )
        self._arrow_chunked: pa.ChunkedArray | None = None
        self._arrow_chunk: pa.Array | None = None
        self._arrow_chunk_index = 0
        self._arrow_chunk_start = 0
        self._arrow_chunk_end = 0
        dtype = series.dtype
        if isinstance(dtype, pd.StringDtype) and dtype.storage == "pyarrow":
            chunked = self._array.__arrow_array__()
            if not isinstance(chunked, pa.ChunkedArray):
                chunked = pa.chunked_array((chunked,))
            for chunk_index in range(chunked.num_chunks):
                chunk = chunked.chunk(chunk_index)
                if not (pa.types.is_string(chunk.type) or pa.types.is_large_string(chunk.type)):
                    raise TypeError("Arrow-backed pandas string column has invalid storage")
            self._arrow_chunked = chunked

    def _select_arrow_chunk(self, chunk_index: int, chunk_start: int) -> None:
        if self._arrow_chunked is None or chunk_index >= self._arrow_chunked.num_chunks:
            self._arrow_chunk = None
            self._arrow_chunk_index = chunk_index
            self._arrow_chunk_start = chunk_start
            self._arrow_chunk_end = chunk_start
            return
        chunk = self._arrow_chunked.chunk(chunk_index)
        self._arrow_chunk = chunk
        self._arrow_chunk_index = chunk_index
        self._arrow_chunk_start = chunk_start
        self._arrow_chunk_end = chunk_start + len(chunk)

    def _arrow_utf8_buffer(self, row_index: int) -> tuple[bool, memoryview | None]:
        if self._arrow_chunked is None:
            return False, None
        if row_index < 0 or row_index >= len(self._arrow_chunked):
            raise IndexError("pandas row index is outside the column")
        if self._arrow_chunk is None or row_index < self._arrow_chunk_start:
            self._select_arrow_chunk(0, 0)
        while self._arrow_chunk is None or row_index >= self._arrow_chunk_end:
            self._select_arrow_chunk(
                self._arrow_chunk_index + 1,
                self._arrow_chunk_end,
            )
        chunk = self._arrow_chunk
        local_index = row_index - self._arrow_chunk_start
        scalar = chunk[local_index]
        if not scalar.is_valid:
            return True, None
        _validity, offsets, data = chunk.buffers()
        if offsets is None:
            raise TypeError("Arrow string column is missing its offsets buffer")
        width = 8 if pa.types.is_large_string(chunk.type) else 4
        offset_index = chunk.offset + local_index
        start = struct.unpack_from("<q" if width == 8 else "<i", offsets, offset_index * width)[0]
        end = struct.unpack_from(
            "<q" if width == 8 else "<i",
            offsets,
            (offset_index + 1) * width,
        )[0]
        if start < 0 or end < start or (data is None and end != 0):
            raise TypeError("Arrow string column has invalid data offsets")
        payload = memoryview(data) if data is not None else memoryview(b"")
        if end > len(payload):
            raise TypeError("Arrow string column offset exceeds its data buffer")
        return True, payload[start:end]

    def value(self, row_index: int) -> object:
        return self._array[row_index]

    def write_valid_string(
        self,
        writer: CanonicalJsonStreamWriter,
        row_index: int,
        *,
        escape_forward_slash: bool,
    ) -> bool:
        handled, value = self._arrow_utf8_buffer(row_index)
        if not handled or value is None:
            return False
        writer.write_utf8_string_buffer(
            value,
            escape_forward_slash=escape_forward_slash,
        )
        return True

    def write_pandas_value(
        self,
        writer: CanonicalJsonStreamWriter,
        row_index: int,
        *,
        escape_forward_slash: bool,
        sort_mapping_keys: bool,
    ) -> None:
        handled, value = self._arrow_utf8_buffer(row_index)
        if handled:
            if value is None:
                writer.write_ascii(b"null")
            else:
                writer.write_utf8_string_buffer(
                    value,
                    escape_forward_slash=escape_forward_slash,
                )
            return
        write_pandas_json_value(
            writer,
            self.value(row_index),
            escape_forward_slash=escape_forward_slash,
            sort_mapping_keys=sort_mapping_keys,
        )

    def write_pandas_table_value(
        self,
        writer: CanonicalJsonStreamWriter,
        row_index: int,
        *,
        escape_forward_slash: bool,
        sort_mapping_keys: bool,
    ) -> None:
        """Preserve pandas orient=table column-context scalar semantics."""

        if not self._table_context:
            raise RuntimeError("table-context scalar encoding was not enabled")
        handled, value = self._arrow_utf8_buffer(row_index)
        if handled:
            if value is None:
                writer.write_ascii(b"null")
            else:
                writer.write_utf8_string_buffer(
                    value,
                    escape_forward_slash=escape_forward_slash,
                )
            return
        value = self.value(row_index)
        if self._table_timedelta and value is pd.NaT:
            writer.write_string("NaT", escape_forward_slash=escape_forward_slash)
            return
        if self._table_float and (bool(pd.isna(value)) or not math.isfinite(float(value))):
            writer.write_ascii(b"null")
            return
        if self._table_integer_categorical_with_missing and not pd.isna(value):
            value = float(value)
        elif self._table_unsigned and not pd.isna(value):
            value = int(value)
        write_pandas_json_value(
            writer,
            value,
            escape_forward_slash=escape_forward_slash,
            sort_mapping_keys=sort_mapping_keys,
        )


def write_pandas_json_value(
    writer: CanonicalJsonStreamWriter,
    value: object,
    *,
    escape_forward_slash: bool,
    sort_mapping_keys: bool,
) -> None:
    """Write one pandas-to_json-compatible scalar without materializing strings."""

    if isinstance(value, Enum):
        write_pandas_json_value(
            writer,
            value.value,
            escape_forward_slash=escape_forward_slash,
            sort_mapping_keys=sort_mapping_keys,
        )
        return
    if isinstance(value, str):
        writer.write_string(value, escape_forward_slash=escape_forward_slash)
        return
    if isinstance(value, bytes):
        writer.write_utf8_string_buffer(
            memoryview(value),
            escape_forward_slash=escape_forward_slash,
        )
        return
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("pandas JSON mappings require string keys")
        keys = sorted(value) if sort_mapping_keys else tuple(value)
        writer.write_ascii(b"{")
        for index, key in enumerate(keys):
            if index:
                writer.write_ascii(b",")
            writer.write_string(key, escape_forward_slash=escape_forward_slash)
            writer.write_ascii(b":")
            write_pandas_json_value(
                writer,
                value[key],
                escape_forward_slash=escape_forward_slash,
                sort_mapping_keys=sort_mapping_keys,
            )
        writer.write_ascii(b"}")
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        writer.write_ascii(b"[")
        for index, item in enumerate(value):
            if index:
                writer.write_ascii(b",")
            write_pandas_json_value(
                writer,
                item,
                escape_forward_slash=escape_forward_slash,
                sort_mapping_keys=sort_mapping_keys,
            )
        writer.write_ascii(b"]")
        return
    if isinstance(value, Decimal):
        writer.write_string(str(value), escape_forward_slash=escape_forward_slash)
        return
    token = ujson_dumps(
        value,
        ensure_ascii=True,
        double_precision=15,
        iso_dates=True,
        date_unit="us",
    )
    encoded = token.encode("ascii")
    if len(encoded) > _MAX_SCALAR_TOKEN_BYTES:
        raise TypeError("unsupported pandas scalar exceeds bounded JSON token size")
    writer.write_ascii(encoded)
