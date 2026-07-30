from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureContract,
    FeatureDefinition,
    FeatureFieldStatus,
    FeatureRequirement,
    RequirementLevel,
)


def _definition(
    name: str = "same_minute_amount_ratio",
    *,
    source_datasets: tuple[str, ...] = ("minute_bar",),
) -> FeatureDefinition:
    return FeatureDefinition(
        name=name,
        dtype="float64",
        source_datasets=source_datasets,
        lookback=20,
        pit_rule="only rows with available_at <= decision_time",
        price_basis="raw",
    )


def _field_status(
    name: str = "same_minute_amount_ratio",
    *,
    status: FeatureAvailability = FeatureAvailability.AVAILABLE,
    reason: str | None = None,
) -> FeatureFieldStatus:
    return FeatureFieldStatus(
        name=name,
        status=status,
        available_at=datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
        reason=reason,
    )


def _batch(**changes: object) -> FeatureBatchEnvelope:
    payload: dict[str, object] = {
        "schema_version": 1,
        "batch_id": "minute-20260731-0931-0001",
        "contract_id": "intraday-volume",
        "contract_version": 2,
        "input_batch_ids": ("raw-0001", "reference-0007"),
        "sequence": 1,
        "event_time": datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
        "available_at": datetime(2026, 7, 31, 1, 31, 1, tzinfo=UTC),
        "row_count": 4,
        "content_hash": "a" * 64,
        "field_statuses": (_field_status(),),
        "producer_commit": "b" * 40,
    }
    payload.update(changes)
    return FeatureBatchEnvelope(**payload)


@pytest.mark.parametrize(
    ("status", "reason", "valid"),
    [
        (FeatureAvailability.AVAILABLE, None, True),
        (FeatureAvailability.AVAILABLE, "unexpected", False),
        (FeatureAvailability.DEGRADED, "source lag", True),
        (FeatureAvailability.UNAVAILABLE, "missing input", True),
        (FeatureAvailability.STALE, "watermark expired", True),
        (FeatureAvailability.DEGRADED, None, False),
        (FeatureAvailability.UNAVAILABLE, None, False),
        (FeatureAvailability.STALE, None, False),
    ],
)
def test_feature_field_status_reason_matches_availability(
    status: FeatureAvailability,
    reason: str | None,
    valid: bool,
) -> None:
    if valid:
        item = _field_status(status=status, reason=reason)
        assert item.status is status
        return

    with pytest.raises(ValidationError):
        _field_status(status=status, reason=reason)


def test_feature_definitions_and_contracts_require_unique_nonempty_features() -> None:
    with pytest.raises(ValidationError, match="source_datasets"):
        _definition(source_datasets=())
    with pytest.raises(ValidationError, match="unique"):
        _definition(source_datasets=("minute_bar", "minute_bar"))
    with pytest.raises(ValidationError, match="unique"):
        FeatureContract(
            contract_id="intraday-volume",
            version=1,
            features=(_definition(), _definition()),
            producer_commit="b" * 40,
        )


def test_feature_requirement_enforces_version_and_defaults() -> None:
    requirement = FeatureRequirement(
        name="same_minute_amount_ratio",
        level=RequirementLevel.REQUIRED,
        min_contract_version=1,
    )

    assert requirement.allow_degraded is False
    with pytest.raises(ValidationError):
        requirement.allow_degraded = True
    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        FeatureRequirement(
            name="same_minute_amount_ratio",
            level=RequirementLevel.REQUIRED,
            min_contract_version=0,
        )


def test_feature_contract_fingerprint_is_stable_for_semantic_ordering() -> None:
    left = FeatureContract(
        contract_id="intraday-volume",
        version=2,
        features=(
            _definition("same_minute_amount_ratio", source_datasets=("minute_bar", "daily_bar")),
            _definition("amount_accel_5m"),
        ),
        producer_commit="b" * 40,
    )
    right = FeatureContract(
        contract_id="intraday-volume",
        version=2,
        features=(
            _definition("amount_accel_5m"),
            _definition("same_minute_amount_ratio", source_datasets=("daily_bar", "minute_bar")),
        ),
        producer_commit="b" * 40,
    )

    assert left.contract_fingerprint == right.contract_fingerprint
    assert len(left.contract_fingerprint) == 64


def test_feature_batch_enforces_pit_time_and_unique_lineage() -> None:
    with pytest.raises(ValidationError, match="available_at"):
        _batch(
            event_time=datetime(2026, 7, 31, 1, 32, tzinfo=UTC),
            available_at=datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
        )
    with pytest.raises(ValidationError, match="input_batch_ids"):
        _batch(input_batch_ids=())
    with pytest.raises(ValidationError, match="unique"):
        _batch(input_batch_ids=("raw-0001", "raw-0001"))
    with pytest.raises(ValidationError, match="unique"):
        _batch(field_statuses=(_field_status(), _field_status()))
    future_field = _field_status().model_copy(
        update={"available_at": datetime(2026, 7, 31, 1, 32, tzinfo=UTC)}
    )
    with pytest.raises(ValidationError, match="field status.*available_at"):
        _batch(field_statuses=(future_field,))


def test_feature_batch_normalizes_time_and_exposes_deterministic_lookups() -> None:
    local = timezone(timedelta(hours=8))
    left = _batch(
        input_batch_ids=("raw-0001", "reference-0007"),
        event_time=datetime(2026, 7, 31, 9, 31, tzinfo=local),
        available_at=datetime(2026, 7, 31, 9, 31, 1, tzinfo=local),
    )
    right = _batch(input_batch_ids=("reference-0007", "raw-0001"))

    assert left.event_time == datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
    assert left.input_fingerprint == right.input_fingerprint
    assert left.field_status("same_minute_amount_ratio") == _field_status()
    assert left.field_status("missing") is None


def test_feature_contracts_reject_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        FeatureContract(
            contract_id="intraday-volume",
            version=1,
            features=(_definition(),),
            producer_commit="b" * 40,
            mutable_note="no",
        )
