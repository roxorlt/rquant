"""The byte-level JSON contract, restated with nothing but the standard library.

`rquant.strict_json` owns this contract for the rest of the repository, but the child may
not import generation code to decide whether the *root's* request is well formed: the shape
check has to survive a generation whose `strict_json` has been replaced. These four
functions therefore reproduce the same bytes independently, and
`tests/unit/test_signal_family_verifier_harness.py` pins them against the generation module
so the two can never drift.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Final

#: The exact `json.dumps` keyword set `rquant.strict_json.canonical_json_bytes` uses.
_DUMP_OPTIONS: Final[dict[str, Any]] = {
    "ensure_ascii": False,
    "allow_nan": False,
    "sort_keys": True,
    "separators": (",", ":"),
}


class CanonicalJsonError(ValueError):
    """A payload is not the canonical encoding of an unambiguous JSON value."""


def canonical_json_bytes(value: Any) -> bytes:
    """Encode one JSON value exactly the way every persisted rQuant record is encoded."""

    return json.dumps(value, **_DUMP_OPTIONS).encode("utf-8")


def sha256_hex(payload: bytes) -> str:
    if type(payload) is not bytes:
        raise TypeError("sha256_hex requires exact bytes")
    return hashlib.sha256(payload).hexdigest()


def canonical_sha256(value: Any) -> str:
    return sha256_hex(canonical_json_bytes(value))


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CanonicalJsonError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def strict_loads(payload: bytes) -> Any:
    """Decode JSON, rejecting duplicate object keys at every depth."""

    if type(payload) is not bytes:
        raise TypeError("strict_loads requires exact bytes")
    try:
        return json.loads(payload, object_pairs_hook=_unique_object)
    except json.JSONDecodeError as exc:
        raise CanonicalJsonError(str(exc)) from exc


def strict_canonical_loads(payload: bytes) -> Any:
    """Decode JSON and require the payload to be its own canonical encoding."""

    decoded = strict_loads(payload)
    if canonical_json_bytes(decoded) != payload:
        raise CanonicalJsonError("payload is not canonical JSON")
    return decoded


__all__ = [
    "CanonicalJsonError",
    "canonical_json_bytes",
    "canonical_sha256",
    "sha256_hex",
    "strict_canonical_loads",
    "strict_loads",
]
