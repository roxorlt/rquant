from __future__ import annotations

import json
import random
import tracemalloc


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
