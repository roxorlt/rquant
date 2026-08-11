"""Bounded-memory canonical JSON primitives used by Strategy Lab digests."""

from __future__ import annotations

import base64
import codecs
import json
import math
import struct
from collections.abc import Callable, Iterator, Mapping, Sequence
from decimal import Decimal
from enum import Enum
from typing import Literal

import numpy as np
import pandas as pd
import pyarrow as pa
from pandas._libs.json import ujson_dumps

CANONICAL_JSON_STRING_CHUNK_CHARACTERS = 1024
CANONICAL_JSON_STREAM_SCRATCH_BYTES = 128 * 1024
CANONICAL_JSON_BASE64_INPUT_CHUNK_BYTES = 12 * 1024
_MAX_SCALAR_TOKEN_BYTES = 64 * 1024


class _LegacyPandasUtf8Decoder:
    """Incrementally reproduce pandas ujson's historical bytes decoder."""

    def __init__(self) -> None:
        self._codepoint = 0
        self._remaining = 0
        self._minimum = 0
        self._sequence_bytes = 0
        self._invalid_start_byte: int | None = None

    @staticmethod
    def _invalid_start(byte: int) -> UnicodeDecodeError:
        return UnicodeDecodeError("utf-8", bytes((byte,)), 0, 1, "invalid start byte")

    def _finish_codepoint(self) -> str:
        codepoint = self._codepoint
        sequence_bytes = self._sequence_bytes
        minimum = self._minimum
        self._codepoint = 0
        self._remaining = 0
        self._minimum = 0
        self._sequence_bytes = 0
        if codepoint < minimum:
            raise OverflowError(
                f"Overlong {sequence_bytes} byte UTF-8 sequence detected when encoding string"
            )
        if codepoint <= 0xFFFF:
            return chr(codepoint)
        adjusted = codepoint - 0x10000
        return chr(0xD800 + (adjusted >> 10)) + chr(0xDC00 + (adjusted & 0x3FF))

    def feed(self, payload: memoryview) -> Iterator[str]:
        chunk = payload.cast("B")
        if not self._remaining:
            copied = bytes(chunk)
            if copied.isascii():
                if copied:
                    yield copied.decode("ascii")
                return
        for byte in chunk:
            if self._remaining:
                self._codepoint = (self._codepoint << 6) | (byte & 0x3F)
                self._remaining -= 1
                if not self._remaining:
                    yield self._finish_codepoint()
                continue
            if byte < 0x80:
                yield chr(byte)
            elif byte < 0xC0 or byte >= 0xFE:
                if self._invalid_start_byte is None:
                    self._invalid_start_byte = byte
            elif byte < 0xE0:
                self._codepoint = byte & 0x1F
                self._remaining = 1
                self._minimum = 0x80
                self._sequence_bytes = 2
            elif byte < 0xF0:
                self._codepoint = byte & 0x0F
                self._remaining = 2
                self._minimum = 0x800
                self._sequence_bytes = 3
            elif byte < 0xF8:
                self._codepoint = byte & 0x07
                self._remaining = 3
                self._minimum = 0x10000
                self._sequence_bytes = 4
            else:
                raise OverflowError("Unsupported UTF-8 sequence length when encoding string")

    def finish(self) -> str:
        text = ""
        if self._remaining > 1:
            raise OverflowError("Unterminated UTF-8 sequence when encoding string")
        if self._remaining:
            self._codepoint <<= 6
            self._remaining = 0
            text = self._finish_codepoint()
        if self._invalid_start_byte is not None:
            raise self._invalid_start(self._invalid_start_byte)
        return text

    @property
    def requires_virtual_terminator(self) -> bool:
        return self._remaining == 1


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

    def write_legacy_pandas_bytes(
        self,
        value: bytes | memoryview,
        *,
        input_chunk_bytes: int = 4096,
    ) -> None:
        """Stream bytes with pandas ujson's legacy UTF-8 compatibility semantics."""

        if input_chunk_bytes < 1 or input_chunk_bytes > CANONICAL_JSON_STREAM_SCRATCH_BYTES:
            raise ValueError("legacy pandas bytes chunk is outside the scratch bound")
        payload = memoryview(value).cast("B")
        decoder = _LegacyPandasUtf8Decoder()
        self._update(b'"')
        for start in range(0, len(payload), input_chunk_bytes):
            for text in decoder.feed(payload[start : start + input_chunk_bytes]):
                self.write_string_content(text)
        tail = decoder.finish()
        if tail:
            self.write_string_content(tail)
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


def legacy_pandas_bytes_requires_virtual_terminator(
    value: bytes | memoryview,
    *,
    input_chunk_bytes: int = 4096,
) -> bool:
    """Return whether pandas ujson would consume its trailing C-string NUL."""

    if input_chunk_bytes < 1 or input_chunk_bytes > CANONICAL_JSON_STREAM_SCRATCH_BYTES:
        raise ValueError("legacy pandas bytes chunk is outside the scratch bound")
    payload = memoryview(value).cast("B")
    decoder = _LegacyPandasUtf8Decoder()
    for start in range(0, len(payload), input_chunk_bytes):
        for _text in decoder.feed(payload[start : start + input_chunk_bytes]):
            pass
    requires_virtual_terminator = decoder.requires_virtual_terminator
    decoder.finish()
    return requires_virtual_terminator


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
    """Read frame cells while keeping Arrow variable-width values buffer-bound."""

    def __init__(self, series: pd.Series) -> None:
        self._array = series.array
        self._arrow_chunked: pa.ChunkedArray | None = None
        self._arrow_chunk: pa.Array | None = None
        self._arrow_chunk_index = 0
        self._arrow_chunk_start = 0
        self._arrow_chunk_end = 0
        self._arrow_kind: Literal["string", "binary"] | None = None
        dtype = series.dtype
        if isinstance(dtype, pd.StringDtype) and dtype.storage == "pyarrow":
            chunked = self._array.__arrow_array__()
            arrow_kind: Literal["string", "binary"] = "string"
        elif isinstance(dtype, pd.ArrowDtype) and (
            pa.types.is_string(dtype.pyarrow_dtype)
            or pa.types.is_large_string(dtype.pyarrow_dtype)
            or pa.types.is_binary(dtype.pyarrow_dtype)
            or pa.types.is_large_binary(dtype.pyarrow_dtype)
        ):
            chunked = self._array.__arrow_array__()
            arrow_kind = (
                "string"
                if (
                    pa.types.is_string(dtype.pyarrow_dtype)
                    or pa.types.is_large_string(dtype.pyarrow_dtype)
                )
                else "binary"
            )
        else:
            return
        if not isinstance(chunked, pa.ChunkedArray):
            chunked = pa.chunked_array((chunked,))
        for chunk_index in range(chunked.num_chunks):
            chunk = chunked.chunk(chunk_index)
            valid_type = (
                pa.types.is_string(chunk.type) or pa.types.is_large_string(chunk.type)
                if arrow_kind == "string"
                else pa.types.is_binary(chunk.type) or pa.types.is_large_binary(chunk.type)
            )
            if not valid_type:
                raise TypeError("Arrow-backed pandas column has invalid variable-width storage")
        self._arrow_chunked = chunked
        self._arrow_kind = arrow_kind

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

    def _arrow_variable_buffer(self, row_index: int) -> tuple[bool, memoryview | None]:
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
        validity, offsets, data = chunk.buffers()
        offset_index = chunk.offset + local_index
        if validity is not None:
            validity_view = memoryview(validity).cast("B")
            if not validity_view[offset_index // 8] & (1 << (offset_index % 8)):
                return True, None
        if offsets is None:
            raise TypeError("Arrow variable-width column is missing its offsets buffer")
        is_large = pa.types.is_large_string(chunk.type) or pa.types.is_large_binary(chunk.type)
        width = 8 if is_large else 4
        offset_format = "<q" if width == 8 else "<i"
        start = struct.unpack_from(offset_format, offsets, offset_index * width)[0]
        end = struct.unpack_from(offset_format, offsets, (offset_index + 1) * width)[0]
        if start < 0 or end < start or (data is None and end != 0):
            raise TypeError("Arrow variable-width column has invalid data offsets")
        payload = memoryview(data).cast("B") if data is not None else memoryview(b"")
        if end > len(payload):
            raise TypeError("Arrow variable-width column offset exceeds its data buffer")
        return True, payload[start:end]

    def value(self, row_index: int) -> object:
        return self._array[row_index]

    def _write_arrow_pandas_value(
        self,
        writer: CanonicalJsonStreamWriter,
        row_index: int,
        *,
        escape_forward_slash: bool,
        legacy_binary: bool,
    ) -> bool:
        handled, value = self._arrow_variable_buffer(row_index)
        if not handled:
            return False
        if value is None:
            writer.write_ascii(b"null")
        elif self._arrow_kind == "binary" and legacy_binary:
            writer.write_legacy_pandas_bytes(value)
        else:
            writer.write_utf8_string_buffer(
                value,
                escape_forward_slash=escape_forward_slash,
            )
        return True

    def write_valid_string(
        self,
        writer: CanonicalJsonStreamWriter,
        row_index: int,
        *,
        escape_forward_slash: bool,
    ) -> bool:
        if self._arrow_kind != "string":
            return False
        handled, value = self._arrow_variable_buffer(row_index)
        if not handled or value is None:
            return False
        writer.write_utf8_string_buffer(
            value,
            escape_forward_slash=escape_forward_slash,
        )
        return True

    def write_legacy_table_value(
        self,
        writer: CanonicalJsonStreamWriter,
        row_index: int,
    ) -> bool:
        return self._write_arrow_pandas_value(
            writer,
            row_index,
            escape_forward_slash=False,
            legacy_binary=True,
        )

    def write_canonical_table_value(
        self,
        writer: CanonicalJsonStreamWriter,
        row_index: int,
    ) -> bool:
        handled, value = self._arrow_variable_buffer(row_index)
        if not handled:
            return False
        if value is None:
            writer.write_ascii(b"null")
        elif self._arrow_kind == "binary":
            writer.write_ascii(b'{"$bytes":')
            writer.write_base64_bytes(value)
            writer.write_ascii(b"}")
        else:
            writer.write_utf8_string_buffer(value)
        return True

    def write_pandas_value(
        self,
        writer: CanonicalJsonStreamWriter,
        row_index: int,
        *,
        escape_forward_slash: bool,
        sort_mapping_keys: bool,
    ) -> None:
        if self._write_arrow_pandas_value(
            writer,
            row_index,
            escape_forward_slash=escape_forward_slash,
            legacy_binary=False,
        ):
            return
        write_pandas_json_value(
            writer,
            self.value(row_index),
            escape_forward_slash=escape_forward_slash,
            sort_mapping_keys=sort_mapping_keys,
        )


def write_pandas_json_value(
    writer: CanonicalJsonStreamWriter,
    value: object,
    *,
    escape_forward_slash: bool,
    sort_mapping_keys: bool,
    scalar_normalizer: Callable[[object], object] | None = None,
    scalar_writer: Callable[[CanonicalJsonStreamWriter, object], bool] | None = None,
) -> None:
    """Write one pandas-to_json-compatible scalar without materializing strings."""

    if scalar_normalizer is not None:
        value = scalar_normalizer(value)
    if scalar_writer is not None and scalar_writer(writer, value):
        return
    if isinstance(value, Enum):
        write_pandas_json_value(
            writer,
            value.value,
            escape_forward_slash=escape_forward_slash,
            sort_mapping_keys=sort_mapping_keys,
            scalar_normalizer=scalar_normalizer,
            scalar_writer=scalar_writer,
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
                scalar_normalizer=scalar_normalizer,
                scalar_writer=scalar_writer,
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
                scalar_normalizer=scalar_normalizer,
                scalar_writer=scalar_writer,
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


def _legacy_table_schema_scalar(value: object) -> object:
    if isinstance(value, (float, np.floating)):
        normalized = float(value)
        return normalized if math.isfinite(normalized) else None
    return value


def _legacy_table_float_token(
    value: object,
    *,
    dtype: np.dtype[np.floating],
) -> bytes:
    probe = np.empty(1, dtype=dtype)
    probe[0] = value
    pandas_token = ujson_dumps(
        probe,
        ensure_ascii=True,
        double_precision=15,
        iso_dates=True,
        date_unit="us",
    )
    if len(pandas_token) > _MAX_SCALAR_TOKEN_BYTES:
        raise TypeError("legacy pandas float token exceeds bounded JSON token size")
    decoded = json.loads(pandas_token)
    if not isinstance(decoded, list) or len(decoded) != 1:
        raise TypeError("legacy pandas float token has an invalid shape")
    canonical = json.dumps(
        decoded[0],
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("ascii")
    if len(canonical) > _MAX_SCALAR_TOKEN_BYTES:
        raise TypeError("legacy canonical float token exceeds bounded JSON token size")
    return canonical


def _write_legacy_table_schema_scalar(
    writer: CanonicalJsonStreamWriter,
    value: object,
) -> bool:
    if not isinstance(value, float):
        return False
    writer.write_ascii(_legacy_table_float_token(value, dtype=np.dtype("float64")))
    return True


def _legacy_table_timedelta_iso(
    value: object,
    *,
    trim_fractional_zeros: bool,
) -> str:
    if not trim_fractional_zeros:
        pandas_token = ujson_dumps(
            value,
            ensure_ascii=True,
            double_precision=15,
            iso_dates=True,
            date_unit="us",
        )
        if len(pandas_token) > _MAX_SCALAR_TOKEN_BYTES:
            raise TypeError("legacy pandas timedelta token exceeds bounded JSON token size")
        decoded = json.loads(pandas_token)
        if not isinstance(decoded, str):
            raise TypeError("legacy pandas timedelta token is not a string")
        return decoded
    encoded = pd.Timedelta(value).isoformat()
    prefix, separator, suffix = encoded.partition(".")
    if not separator or not suffix.endswith("S"):
        return encoded
    fraction = suffix[:-1].rstrip("0")
    return f"{prefix}.{fraction}S" if fraction else f"{prefix}S"


class LegacyPandasTableColumnAccessor:
    """Encode one orient=table data column with pandas' legacy context rules."""

    def __init__(self, series: pd.Series, *, data_key: str) -> None:
        self.data_key = data_key
        self._values = PandasJsonColumnAccessor(series)
        categorical_dtype = series.dtype if isinstance(series.dtype, pd.CategoricalDtype) else None
        value_dtype = (
            categorical_dtype.categories.dtype if categorical_dtype is not None else series.dtype
        )
        self._duration = pd.api.types.is_timedelta64_dtype(value_dtype)
        self._duration_missing_as_nat = categorical_dtype is None and self._duration
        self._floating = pd.api.types.is_float_dtype(value_dtype)
        self._float_dtype = (
            np.dtype(getattr(value_dtype, "numpy_dtype", value_dtype)) if self._floating else None
        )
        self._unsigned = pd.api.types.is_unsigned_integer_dtype(value_dtype)
        self._integer_categorical_with_missing = (
            categorical_dtype is not None
            and pd.api.types.is_integer_dtype(value_dtype)
            and not pd.api.types.is_bool_dtype(value_dtype)
            and series.hasnans
        )

    def write_value(
        self,
        writer: CanonicalJsonStreamWriter,
        row_index: int,
    ) -> None:
        if self._values.write_legacy_table_value(writer, row_index):
            return
        value = self._values.value(row_index)
        if isinstance(value, bytes):
            writer.write_legacy_pandas_bytes(value)
            return
        if self._duration:
            if bool(pd.isna(value)):
                if self._duration_missing_as_nat:
                    writer.write_string("NaT")
                else:
                    writer.write_ascii(b"null")
            else:
                writer.write_string(
                    _legacy_table_timedelta_iso(
                        value,
                        trim_fractional_zeros=self._duration_missing_as_nat,
                    )
                )
            return
        if self._floating:
            if self._float_dtype is None:
                raise RuntimeError("legacy floating column dtype is unavailable")
            if value is pd.NA:
                writer.write_ascii(b"null")
                return
            writer.write_ascii(_legacy_table_float_token(value, dtype=self._float_dtype))
            return
        elif self._integer_categorical_with_missing and not pd.isna(value):
            value = float(value)
        elif self._unsigned and not pd.isna(value):
            value = int(value)
        if isinstance(value, (float, np.floating)):
            writer.write_ascii(
                _legacy_table_float_token(
                    value,
                    dtype=np.asarray(value).dtype,
                )
            )
            return
        write_pandas_json_value(
            writer,
            value,
            escape_forward_slash=False,
            sort_mapping_keys=True,
        )


class LegacyPandasTableCompatibility:
    """Stream the legacy canonical orient=table object without whole-frame JSON."""

    def __init__(self, frame: pd.DataFrame) -> None:
        columns = tuple(frame.columns)
        if any(not isinstance(column, str) for column in columns):
            raise ValueError("artifact DataFrame columns must be strings")
        data_keys = tuple(column.split("\x00", 1)[0] for column in columns)
        if len(data_keys) != len(set(data_keys)):
            raise ValueError("legacy orient=table NUL-truncated column names collide")
        self._frame = frame
        self._columns = tuple(
            LegacyPandasTableColumnAccessor(
                frame.iloc[:, position],
                data_key=data_keys[position],
            )
            for position in range(len(columns))
        )
        self._positions = tuple(sorted(range(len(columns)), key=data_keys.__getitem__))

    def write(self, writer: CanonicalJsonStreamWriter) -> None:
        writer.write_ascii(b'{"data":[')
        if self._columns:
            for row_index in range(len(self._frame)):
                if row_index:
                    writer.write_ascii(b",")
                writer.write_ascii(b"{")
                for field_index, position in enumerate(self._positions):
                    if field_index:
                        writer.write_ascii(b",")
                    column = self._columns[position]
                    writer.write_string(column.data_key)
                    writer.write_ascii(b":")
                    column.write_value(writer, row_index)
                writer.write_ascii(b"}")
        writer.write_ascii(b'],"schema":')
        write_pandas_json_value(
            writer,
            pd.io.json.build_table_schema(self._frame, index=False),
            escape_forward_slash=False,
            sort_mapping_keys=True,
            scalar_normalizer=_legacy_table_schema_scalar,
            scalar_writer=_write_legacy_table_schema_scalar,
        )
        writer.write_ascii(b"}")


def write_legacy_pandas_table_json(
    writer: CanonicalJsonStreamWriter,
    frame: pd.DataFrame,
) -> None:
    LegacyPandasTableCompatibility(frame).write(writer)
