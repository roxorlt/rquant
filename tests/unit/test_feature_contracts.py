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
        availability_contract={
            "source_available_at_basis": "max_source_available_at",
            "max_delay_seconds": 60,
            "missing_policy": "mark_unavailable",
            "late_policy": "mark_stale",
            "decision_visibility_gate": "available_at_lte_decision_time",
        },
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
        source_event_time=datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
        available_at=datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
        decision_cutoff=datetime(2026, 7, 31, 1, 31, 1, tzinfo=UTC),
        actual_delay_seconds=0.0,
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
        "decision_cutoff": datetime(2026, 7, 31, 1, 31, 1, tzinfo=UTC),
        "actual_delay_seconds": 1.0,
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


def test_feature_field_status_binds_source_cutoff_and_actual_delay() -> None:
    source_event_time = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
    available_at = source_event_time + timedelta(seconds=2)
    decision_cutoff = available_at + timedelta(seconds=1)

    status = FeatureFieldStatus(
        name="same_minute_amount_ratio",
        status=FeatureAvailability.AVAILABLE,
        source_event_time=source_event_time,
        available_at=available_at,
        decision_cutoff=decision_cutoff,
        actual_delay_seconds=2.0,
    )

    assert status.actual_delay_seconds == 2.0


@pytest.mark.parametrize(
    ("source_offset", "available_offset", "cutoff_offset", "delay"),
    ((2, 1, 3, 0.0), (0, 3, 2, 3.0), (0, 2, 3, 1.0)),
)
def test_feature_field_status_rejects_future_or_false_timing_evidence(
    source_offset: int,
    available_offset: int,
    cutoff_offset: int,
    delay: float,
) -> None:
    base = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
    with pytest.raises(ValidationError, match="source_event|decision_cutoff|actual_delay"):
        FeatureFieldStatus(
            name="same_minute_amount_ratio",
            status=FeatureAvailability.AVAILABLE,
            source_event_time=base + timedelta(seconds=source_offset),
            available_at=base + timedelta(seconds=available_offset),
            decision_cutoff=base + timedelta(seconds=cutoff_offset),
            actual_delay_seconds=delay,
        )


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


def test_feature_availability_contract_is_structured_and_fingerprinted() -> None:
    payload = _definition().model_dump(mode="python")
    availability = {
        "source_available_at_basis": "max_source_available_at",
        "max_delay_seconds": 60,
        "missing_policy": "mark_unavailable",
        "late_policy": "mark_stale",
        "decision_visibility_gate": "available_at_lte_decision_time",
    }
    feature = FeatureDefinition.model_validate({**payload, "availability_contract": availability})
    changed = FeatureDefinition.model_validate(
        {
            **payload,
            "availability_contract": {**availability, "max_delay_seconds": 120},
        }
    )
    first = FeatureContract(
        contract_id="intraday-volume",
        version=2,
        features=(feature,),
        producer_commit="b" * 40,
    )
    second = FeatureContract(
        contract_id="intraday-volume",
        version=2,
        features=(changed,),
        producer_commit="b" * 40,
    )

    assert feature.availability_contract.source_available_at_basis == ("max_source_available_at")
    assert feature.availability_contract.decision_visibility_gate == (
        "available_at_lte_decision_time"
    )
    assert first.contract_fingerprint != second.contract_fingerprint


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
    with pytest.raises(ValidationError, match="field_statuses|available_at|decision_cutoff"):
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


def test_pooled_field_status_requires_candidate_scope() -> None:
    first = _field_status().model_copy(update={"candidate_id": "000001.SZ"})
    second = _field_status().model_copy(update={"candidate_id": "000002.SZ"})
    batch = _batch(row_count=2, field_statuses=(first, second))

    assert batch.field_status("same_minute_amount_ratio") is None
    assert (
        batch.field_status(
            "same_minute_amount_ratio",
            candidate_id="000001.SZ",
        )
        == first
    )


def test_single_candidate_scoped_field_status_keeps_legacy_lookup() -> None:
    status = _field_status().model_copy(update={"candidate_id": "000001.SZ"})
    batch = _batch(row_count=1, field_statuses=(status,))

    assert batch.field_status("same_minute_amount_ratio") == status


def test_pooled_partial_scoped_field_status_still_requires_candidate_id() -> None:
    status = _field_status().model_copy(update={"candidate_id": "000001.SZ"})
    batch = _batch(row_count=2, field_statuses=(status,))

    assert batch.field_status("same_minute_amount_ratio") is None
    assert (
        batch.field_status(
            "same_minute_amount_ratio",
            candidate_id="000001.SZ",
        )
        == status
    )


def test_feature_contracts_reject_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        FeatureContract(
            contract_id="intraday-volume",
            version=1,
            features=(_definition(),),
            producer_commit="b" * 40,
            mutable_note="no",
        )
