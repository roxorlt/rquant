"""Duplicate-key rejecting JSON decoding for persistent protocol records."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, Protocol, TypeVar


class StrictJsonModel(Protocol):
    @classmethod
    def model_validate(cls, value: object) -> StrictJsonModel: ...


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


def strict_model_validate_json(model: type[ModelT], payload: str | bytes | bytearray) -> ModelT:
    """Strictly decode one persistent JSON record before Pydantic validation."""

    return model.model_validate(strict_json_loads(payload))
