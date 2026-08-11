from __future__ import annotations

import hashlib
import json
import random
import tracemalloc

import pandas as pd
import pyarrow as pa


def _encoded_string(value: str, *, escape_forward_slash: bool = False) -> bytes:
    from rquant.canonical_json_stream import CanonicalJsonStreamWriter

    payload = bytearray()
    writer = CanonicalJsonStreamWriter(payload.extend)
    writer.write_string(value, escape_forward_slash=escape_forward_slash)
    return bytes(payload)


def test_streaming_json_string_matches_stdlib_for_random_unicode() -> None:
    rng = random.Random(20260727)
    alphabet = [
        "\x00",
        "\b",
        "\t",
        "\n",
        "\f",
        "\r",
        '"',
        "\\",
        "/",
        "A",
        "~",
        "\u4e2d",
        "\u2028",
        "\ud800",
        "\udfff",
        "\U0001f600",
        "\U0010ffff",
    ]
    samples = ["", "".join(alphabet)]
    samples.extend(
        "".join(rng.choice(alphabet) for _ in range(rng.randrange(0, 256))) for _ in range(256)
    )

    for sample in samples:
        expected = json.dumps(
            sample,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        assert _encoded_string(sample) == expected


def test_streaming_json_string_can_match_pandas_forward_slash_escaping() -> None:
    value = '</script>/\u4e2d/"\\\n\U0001f600'
    expected = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
    ).replace("/", "\\/")

    assert _encoded_string(value, escape_forward_slash=True) == expected.encode("ascii")


def test_pandas_bytes_scalar_streams_utf8_with_bounded_scratch() -> None:
    from rquant.canonical_json_stream import (
        CanonicalJsonStreamWriter,
        write_pandas_json_value,
    )

    small = "中/\n".encode()
    encoded = bytearray()
    write_pandas_json_value(
        CanonicalJsonStreamWriter(encoded.extend),
        small,
        escape_forward_slash=True,
        sort_mapping_keys=True,
    )
    assert bytes(encoded) == json.dumps(
        small.decode(),
        ensure_ascii=True,
        separators=(",", ":"),
    ).replace("/", "\\/").encode("ascii")

    large = b"x" * (16 * 1024 * 1024)
    consumed = 0

    def consume(payload: bytes) -> None:
        nonlocal consumed
        consumed += len(payload)

    tracemalloc.start()
    write_pandas_json_value(
        CanonicalJsonStreamWriter(consume),
        large,
        escape_forward_slash=False,
        sort_mapping_keys=True,
    )
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert consumed == len(large) + 2
    assert peak <= 2 * 1024 * 1024


def test_arrow_string_accessor_streams_highly_fragmented_chunks_with_constant_memory() -> None:
    from rquant.canonical_json_stream import (
        CanonicalJsonStreamWriter,
        PandasJsonColumnAccessor,
    )

    values = [f"value-{index:05d}" for index in range(20_000)]
    chunked = pa.chunked_array([pa.array([value], type=pa.string()) for value in values])
    series = pd.Series(pd.arrays.ArrowStringArray(chunked), name="value")
    expected = hashlib.sha256(
        json.dumps(values, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    digest = hashlib.sha256()
    writer = CanonicalJsonStreamWriter(digest.update)

    tracemalloc.start()
    accessor = PandasJsonColumnAccessor(series)
    writer.write_ascii(b"[")
    for row_index in range(len(series)):
        if row_index:
            writer.write_ascii(b",")
        accessor.write_pandas_value(
            writer,
            row_index,
            escape_forward_slash=False,
            sort_mapping_keys=True,
        )
    writer.write_ascii(b"]")
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert digest.hexdigest() == expected
    assert peak <= 512 * 1024


def test_arrow_dtype_variable_buffers_stream_slices_nulls_and_large_variants() -> None:
    from rquant.canonical_json_stream import (
        CanonicalJsonStreamWriter,
        PandasJsonColumnAccessor,
    )

    cases = (
        (pa.string(), ["drop", "alpha", None, "omega"], ["alpha", None, "omega"]),
        (
            pa.large_string(),
            ["drop", "alpha", None, "omega"],
            ["alpha", None, "omega"],
        ),
        (pa.binary(), [b"drop", b"alpha", None, b"omega"], ["alpha", None, "omega"]),
        (
            pa.large_binary(),
            [b"drop", b"alpha", None, b"omega"],
            ["alpha", None, "omega"],
        ),
    )
    for arrow_type, source, expected_values in cases:
        sliced = pa.array(source, type=arrow_type).slice(1)
        chunked = pa.chunked_array((sliced.slice(0, 1), sliced.slice(1, 1), sliced.slice(2, 1)))
        series = pd.Series(pd.arrays.ArrowExtensionArray(chunked), name="value")
        encoded = bytearray()
        writer = CanonicalJsonStreamWriter(encoded.extend)
        accessor = PandasJsonColumnAccessor(series)

        writer.write_ascii(b"[")
        for row_index in range(len(series)):
            if row_index:
                writer.write_ascii(b",")
            accessor.write_pandas_value(
                writer,
                row_index,
                escape_forward_slash=False,
                sort_mapping_keys=True,
            )
        writer.write_ascii(b"]")

        assert bytes(encoded) == json.dumps(
            expected_values,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")


def test_arrow_dtype_fragmented_string_and_binary_use_constant_python_memory() -> None:
    from rquant.canonical_json_stream import (
        CanonicalJsonStreamWriter,
        PandasJsonColumnAccessor,
    )

    for arrow_type, make_value in (
        (pa.string(), lambda index: f"value-{index:05d}"),
        (pa.binary(), lambda index: f"value-{index:05d}".encode()),
    ):
        values = [make_value(index) for index in range(20_000)]
        chunked = pa.chunked_array([pa.array([value], type=arrow_type) for value in values])
        series = pd.Series(pd.arrays.ArrowExtensionArray(chunked), name="value")
        digest = hashlib.sha256()
        writer = CanonicalJsonStreamWriter(digest.update)

        tracemalloc.start()
        accessor = PandasJsonColumnAccessor(series)
        writer.write_ascii(b"[")
        for row_index in range(len(series)):
            if row_index:
                writer.write_ascii(b",")
            accessor.write_pandas_value(
                writer,
                row_index,
                escape_forward_slash=False,
                sort_mapping_keys=True,
            )
        writer.write_ascii(b"]")
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        expected = json.dumps(
            [value.decode() if isinstance(value, bytes) else value for value in values],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        assert digest.hexdigest() == hashlib.sha256(expected).hexdigest()
        assert peak <= 512 * 1024
