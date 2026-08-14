from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant.runtime_contracts import (
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)


class _SampleContract(RuntimeContractModel):
    name: str
    observed_at: datetime


def test_runtime_contracts_are_frozen_and_reject_unknown_fields() -> None:
    contract = _SampleContract(name="minute", observed_at=datetime.now(tz=UTC))

    with pytest.raises(ValidationError):
        _SampleContract(
            name="minute",
            observed_at=datetime.now(tz=UTC),
            unexpected=True,
        )
    with pytest.raises(ValidationError):
        contract.name = "changed"


def test_normalize_aware_utc_rejects_naive_and_normalizes_offset() -> None:
    local = datetime(2026, 7, 31, 9, 30, tzinfo=timezone(timedelta(hours=8)))

    assert normalize_aware_utc(local) == datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
    with pytest.raises(ValueError, match="timezone-aware"):
        normalize_aware_utc(datetime(2026, 7, 31, 9, 30))


def test_canonical_sha256_is_stable_for_key_order_and_equivalent_timezones() -> None:
    utc_value = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
    local_value = datetime(
        2026,
        7,
        31,
        9,
        30,
        tzinfo=timezone(timedelta(hours=8)),
    )

    left = {"when": utc_value, "nested": {"b": 2, "a": 1}}
    right = {"nested": {"a": 1, "b": 2}, "when": local_value}

    assert canonical_sha256(left) == canonical_sha256(right)
    assert len(canonical_sha256(left)) == 64


def test_canonical_sha256_encodes_uuid_with_an_explicit_type_marker() -> None:
    value = UUID("12345678-1234-5678-1234-567812345678")

    assert canonical_sha256({"job_id": value}) == canonical_sha256({"job_id": UUID(str(value))})
    assert canonical_sha256({"job_id": value}) != canonical_sha256({"job_id": str(value)})


def test_canonical_sha256_encodes_paths_as_their_lexical_string() -> None:
    path = Path("/private/var/example")
    assert canonical_sha256({"path": path}) == canonical_sha256({"path": str(path)})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_canonical_sha256_rejects_non_finite_numbers(value: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        canonical_sha256({"value": value})
