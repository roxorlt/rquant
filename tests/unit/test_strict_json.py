from __future__ import annotations

import pytest
from pydantic import BaseModel, ConfigDict

from rquant.strict_json import StrictJsonError, strict_model_validate_canonical_json


class _AuthorityRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: int
    nested: dict[str, int]


def _canonical_payload() -> bytes:
    return b'{"nested":{"a":1,"b":2},"schema_version":1}'


def test_strict_model_canonical_json_accepts_only_exact_canonical_bytes() -> None:
    expected = _canonical_payload()

    parsed = strict_model_validate_canonical_json(_AuthorityRecord, expected)

    assert parsed.schema_version == 1
    assert parsed.nested == {"a": 1, "b": 2}


@pytest.mark.parametrize(
    "payload",
    (
        b'{"schema_version":1,"nested":{"a":1,"b":2}}',
        b'{ "schema_version":1,"nested":{"a":1,"b":2}}',
        b'{"schema_version":1,"nested":{"a":1,"b":2}}\n',
        b'{"schema_version":1,"nested":{"a":1.0,"b":2}}',
        b'{"schema_version":1,"nested":{"a":0,"a":1,"b":2}}',
        b'{"schema_version":false,"schema_version":1,"nested":{"a":1,"b":2}}',
    ),
)
def test_strict_model_canonical_json_rejects_ambiguous_or_noncanonical_bytes(
    payload: bytes,
) -> None:
    with pytest.raises((StrictJsonError, ValueError), match="canonical|duplicate|validation"):
        strict_model_validate_canonical_json(_AuthorityRecord, payload)
