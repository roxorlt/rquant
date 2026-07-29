"""Duplicate-key rejecting JSON decoding for persistent protocol records."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, Protocol, TypeVar


class StrictJsonModel(Protocol):
    @classmethod
    def model_validate_json(cls, value: str | bytes | bytearray) -> StrictJsonModel: ...

    def model_dump(self, *, mode: str) -> Any: ...

    def model_dump_json(self, *, round_trip: bool = False) -> str: ...


ModelT = TypeVar("ModelT", bound=StrictJsonModel)


class StrictJsonError(ValueError):
    """JSON is syntactically invalid or contains an ambiguous object."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise StrictJsonError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def strict_json_loads(
    payload: str | bytes | bytearray,
    *,
    parse_float: Callable[[str], Any] | None = None,
    parse_constant: Callable[[str], Any] | None = None,
) -> Any:
    """Decode JSON while rejecting duplicate object keys at every depth."""

    options: dict[str, object] = {"object_pairs_hook": _unique_object}
    if parse_float is not None:
        options["parse_float"] = parse_float
    if parse_constant is not None:
        options["parse_constant"] = parse_constant
    try:
        return json.loads(payload, **options)
    except json.JSONDecodeError as exc:
        raise StrictJsonError(str(exc)) from exc


def canonical_json_bytes(value: Any, *, trailing_newline: bool = False) -> bytes:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return encoded + (b"\n" if trailing_newline else b"")


def strict_canonical_json_loads(
    payload: str | bytes | bytearray,
    *,
    trailing_newline: bool = False,
) -> Any:
    original = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    decoded = strict_json_loads(original)
    if original != canonical_json_bytes(decoded, trailing_newline=trailing_newline):
        raise StrictJsonError("persistent JSON is not canonical")
    return decoded


def strict_model_validate_json(model: type[ModelT], payload: str | bytes | bytearray) -> ModelT:
    """Strictly decode one persistent JSON record before Pydantic validation."""

    decoded = strict_json_loads(payload)
    canonical = json.dumps(
        decoded,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return model.model_validate_json(canonical)


def canonical_model_json_bytes(value: StrictJsonModel) -> bytes:
    """Serialize one authority model with the repository's byte-level contract."""

    encoded: bytes
    bytes_serializer = getattr(value, "canonical_json_bytes", None)
    if callable(bytes_serializer):
        encoded = bytes_serializer()
        if not isinstance(encoded, bytes):
            raise TypeError("canonical_json_bytes() must return bytes")
    else:
        text_serializer = getattr(value, "canonical_json", None)
        if callable(text_serializer):
            text = text_serializer()
            if not isinstance(text, str):
                raise TypeError("canonical_json() must return str")
            encoded = text.encode("utf-8")
        else:
            encoded = canonical_json_bytes(value.model_dump(mode="json", round_trip=True))
    try:
        if type(value).model_validate_json(encoded) == value:
            return encoded
    except Exception:
        pass
    fallback = value.model_dump_json(round_trip=True)
    if not isinstance(fallback, str):
        raise TypeError("model_dump_json() must return str")
    try:
        if type(value).model_validate_json(fallback) != value:
            raise TypeError("model authority JSON does not round-trip")
    except Exception as exc:
        if isinstance(exc, TypeError):
            raise
        raise TypeError("model authority JSON does not round-trip") from exc
    return fallback.encode("utf-8")


def strict_model_validate_canonical_json(
    model: type[ModelT],
    payload: str | bytes | bytearray,
    *,
    trailing_newline: bool = False,
) -> ModelT:
    """Reject duplicate keys and every non-canonical authority representation."""

    original = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    strict_json_loads(original)
    parsed = model.model_validate_json(original)
    candidate = canonical_model_json_bytes(parsed)
    try:
        candidate_model = model.model_validate_json(candidate)
    except Exception:
        candidate_model = None
    if candidate_model != parsed:
        candidate = parsed.model_dump_json(round_trip=True).encode("utf-8")
    expected = candidate + (b"\n" if trailing_newline else b"")
    if original != expected:
        raise StrictJsonError("persistent JSON is not canonical")
    return parsed
