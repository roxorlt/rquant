"""Strict JSON decoding shared by stdlib-only release authorities."""

from __future__ import annotations

import json
from typing import Any


class StrictJsonError(ValueError):
    """JSON is syntactically invalid or contains an ambiguous object."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise StrictJsonError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def strict_json_loads(payload: str | bytes | bytearray) -> Any:
    """Decode JSON while rejecting duplicate object keys at every depth."""

    try:
        return json.loads(payload, object_pairs_hook=_unique_object)
    except json.JSONDecodeError as exc:
        raise StrictJsonError(str(exc)) from exc
