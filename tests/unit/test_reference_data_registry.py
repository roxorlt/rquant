from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.reference_data_registry import (
    ReferenceDataConflictError,
    ReferenceDataIntegrityError,
    ReferenceDataset,
    ReferenceDataUnavailableError,
    ReferenceGenerationManifest,
    ReferenceRecord,
    ReferenceRegistry,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _record(
    *,
    dataset_id: str = ReferenceDataset.ST_STATUS,
    key: str = "600000.SH",
    effective_from: datetime = BASE,
    effective_to: datetime | None = BASE + timedelta(days=10),
    revision: int = 1,
    first_available_at: datetime = BASE + timedelta(hours=1),
    payload: dict[str, object] | None = None,
    replacement_reason: str | None = None,
) -> ReferenceRecord:
    return ReferenceRecord(
        dataset_id=dataset_id,
        key=key,
        effective_from=effective_from,
        effective_to=effective_to,
        revision=revision,
        source="tushare",
        first_available_at=first_available_at,
        replacement_reason=replacement_reason,
        payload=payload or {"is_st": False},
    )


def _registry(tmp_path: Path) -> ReferenceRegistry:
    return ReferenceRegistry(tmp_path / "reference.sqlite")


def test_record_derives_stable_payload_and_record_hashes() -> None:
    first = _record(payload={"reason": None, "is_st": False})
    reordered = _record(payload={"is_st": False, "reason": None})

    assert first.payload_sha256 == reordered.payload_sha256
    assert first.record_id == reordered.record_id
    assert len(first.payload_sha256) == 64
    assert len(first.record_id) == 64

    with pytest.raises(ValidationError, match="payload_sha256"):
        ReferenceRecord.model_validate(
            {**_record().model_dump(mode="python"), "payload_sha256": "0" * 64}
        )


def test_record_requires_aware_ordered_times_and_revision_reason() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        _record(effective_from=datetime(2026, 1, 1))
    with pytest.raises(ValidationError, match="effective_to"):
        _record(effective_to=BASE)
    with pytest.raises(ValidationError, match="replacement_reason"):
        _record(revision=2)
    with pytest.raises(ValidationError, match="revision 1"):
        _record(replacement_reason="not a replacement")
    payload = _record().model_dump(mode="python")
    payload.pop("effective_from")
    with pytest.raises(ValidationError, match="effective_from"):
        ReferenceRecord.model_validate(payload)


@pytest.mark.parametrize(
    ("dataset_id", "payload"),
    [
        (ReferenceDataset.ST_STATUS, {"is_st": True, "name": "ST sample"}),
        (ReferenceDataset.SUSPENSION_STATUS, {"is_suspended": True}),
        (ReferenceDataset.LISTING_STATUS, {"status": "listed", "board": "main"}),
        (ReferenceDataset.BOARD_MEMBERSHIP, {"boards": ["SSE", "large_cap"]}),
        (ReferenceDataset.ADJUSTMENT_FACTOR, {"adj_factor": 3.125}),
        (ReferenceDataset.PRICE_LIMIT_REGIME, {"limit_percent": 10}),
    ],
)
def test_registry_keeps_strategy_reference_payloads_generic(
    tmp_path: Path,
    dataset_id: str,
    payload: dict[str, object],
) -> None:
    registry = _registry(tmp_path)
    record = _record(dataset_id=dataset_id, payload=payload)
    registry.append(record)
    registry.publish(published_at=BASE + timedelta(hours=2))

    observed = registry.as_of(
        dataset_id=dataset_id,
        key=record.key,
        event_time=BASE + timedelta(days=1),
        decision_time=BASE + timedelta(hours=2),
    )

    assert observed.record.payload == payload


def test_append_is_idempotent_but_never_silently_overwrites(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    original = _record()

    assert registry.append(original).inserted is True
    assert registry.append(original).inserted is False

    changed = _record(payload={"is_st": True})
    with pytest.raises(ReferenceDataConflictError, match="revision 1"):
        registry.append(changed)


def test_revision_is_append_only_sequential_and_available_time_is_monotonic(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())

    with pytest.raises(ReferenceDataConflictError, match="next revision"):
        registry.append(
            _record(
                revision=3,
                first_available_at=BASE + timedelta(hours=3),
                payload={"is_st": True},
                replacement_reason="late correction",
            )
        )
    with pytest.raises(ReferenceDataConflictError, match="first_available_at"):
        registry.append(
            _record(
                revision=2,
                first_available_at=BASE + timedelta(minutes=30),
                payload={"is_st": True},
                replacement_reason="late correction",
            )
        )

    revised = _record(
        revision=2,
        first_available_at=BASE + timedelta(hours=3),
        payload={"is_st": True},
        replacement_reason="late correction",
    )
    registry.append(revised)
    assert registry.records(dataset_id=revised.dataset_id, key=revised.key) == (
        _record(),
        revised,
    )


def test_as_of_uses_effective_period_and_decision_time_without_future_revision(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    original = _record()
    correction = _record(
        revision=2,
        first_available_at=BASE + timedelta(days=3),
        payload={"is_st": True},
        replacement_reason="exchange correction",
    )
    registry.append(original)
    registry.append(correction)
    registry.publish(published_at=BASE + timedelta(days=4))

    historical = registry.as_of(
        dataset_id=original.dataset_id,
        key=original.key,
        event_time=BASE + timedelta(days=2),
        decision_time=BASE + timedelta(days=2),
    )
    revised = registry.as_of(
        dataset_id=original.dataset_id,
        key=original.key,
        event_time=BASE + timedelta(days=2),
        decision_time=BASE + timedelta(days=4),
    )

    assert historical.record.revision == 1
    assert historical.record.payload == {"is_st": False}
    assert revised.record.revision == 2
    assert revised.record.payload == {"is_st": True}


def test_as_of_fails_closed_for_unknown_or_boundary_gap(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    record = _record()
    registry.append(record)
    registry.publish(published_at=BASE + timedelta(hours=2))

    with pytest.raises(ReferenceDataUnavailableError, match="not available"):
        registry.as_of(
            dataset_id=record.dataset_id,
            key=record.key,
            event_time=BASE + timedelta(days=1),
            decision_time=BASE + timedelta(minutes=30),
        )
    with pytest.raises(ReferenceDataUnavailableError, match="not effective"):
        registry.as_of(
            dataset_id=record.dataset_id,
            key=record.key,
            event_time=record.effective_to,
            decision_time=BASE + timedelta(hours=2),
        )


def test_overlapping_business_periods_are_rejected(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())

    with pytest.raises(ReferenceDataConflictError, match="overlap"):
        registry.append(
            _record(
                effective_from=BASE + timedelta(days=5),
                effective_to=BASE + timedelta(days=20),
                first_available_at=BASE + timedelta(hours=2),
            )
        )

    registry.append(
        _record(
            effective_from=BASE + timedelta(days=10),
            effective_to=None,
            first_available_at=BASE + timedelta(hours=2),
        )
    )


def test_publish_creates_immutable_hashed_generation_and_current_pointer(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())

    first = registry.publish(published_at=BASE + timedelta(hours=2))
    retry = registry.publish(published_at=BASE + timedelta(hours=2))
    pointer = registry.current_pointer()

    assert retry == first
    assert pointer.generation_id == first.generation_id
    assert pointer.manifest_sha256 == first.manifest_sha256
    assert first.row_count == 1
    assert len(first.generation_id) == 64
    assert len(first.manifest_sha256) == 64

    registry.append(
        _record(
            key="000001.SZ",
            first_available_at=BASE + timedelta(hours=3),
        )
    )
    second = registry.publish(published_at=BASE + timedelta(hours=4))
    assert second.previous_generation_id == first.generation_id
    assert registry.generation(first.generation_id) == first

    with pytest.raises(ValidationError, match="generation_id"):
        ReferenceGenerationManifest.model_validate(
            {**first.model_dump(mode="python"), "generation_id": "0" * 64}
        )


def test_generation_membership_freezes_late_records(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    first_record = _record()
    registry.append(first_record)
    first = registry.publish(published_at=BASE + timedelta(hours=2))
    late = _record(key="000001.SZ", first_available_at=BASE + timedelta(hours=3))
    registry.append(late)
    registry.publish(published_at=BASE + timedelta(hours=4))

    with pytest.raises(ReferenceDataUnavailableError, match="not present"):
        registry.as_of(
            dataset_id=late.dataset_id,
            key=late.key,
            event_time=BASE + timedelta(days=1),
            decision_time=BASE + timedelta(hours=4),
            generation_id=first.generation_id,
        )


def test_rollback_switches_pointer_without_mutating_manifests(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())
    first = registry.publish(published_at=BASE + timedelta(hours=2))
    registry.append(_record(key="000001.SZ", first_available_at=BASE + timedelta(hours=3)))
    second = registry.publish(published_at=BASE + timedelta(hours=4))

    pointer = registry.rollback(
        first.generation_id,
        switched_at=BASE + timedelta(hours=5),
    )

    assert pointer.generation_id == first.generation_id
    assert pointer.previous_generation_id == second.generation_id
    assert registry.generation(first.generation_id) == first
    assert registry.generation(second.generation_id) == second
    assert ReferenceRegistry(registry.path).current_pointer() == pointer


def test_registry_reopens_with_wal_full_and_detects_manifest_tampering(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reference.sqlite"
    registry = ReferenceRegistry(path)
    registry.append(_record())
    manifest = registry.publish(published_at=BASE + timedelta(hours=2))

    reopened = ReferenceRegistry(path)
    assert reopened.current_manifest() == manifest
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        connection.execute(
            "UPDATE reference_generation SET row_count = row_count + 1 WHERE generation_id = ?",
            (manifest.generation_id,),
        )
        connection.commit()

    with pytest.raises(ReferenceDataIntegrityError, match="manifest hash"):
        ReferenceRegistry(path)


def test_reopen_recomputes_record_payload_hash(tmp_path: Path) -> None:
    path = tmp_path / "reference.sqlite"
    registry = ReferenceRegistry(path)
    registry.append(_record())
    registry.publish(published_at=BASE + timedelta(hours=2))

    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE reference_record SET payload_json = ?",
            ('{"is_st":true}',),
        )
        connection.commit()

    with pytest.raises(ReferenceDataIntegrityError, match="record"):
        ReferenceRegistry(path)


def test_reopen_fails_closed_if_a_bypassed_writer_created_overlap(tmp_path: Path) -> None:
    path = tmp_path / "reference.sqlite"
    registry = ReferenceRegistry(path)
    registry.append(_record())
    overlapping = _record(
        effective_from=BASE + timedelta(days=5),
        effective_to=BASE + timedelta(days=20),
        first_available_at=BASE + timedelta(hours=2),
    )

    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO reference_record(
                record_id, dataset_id, business_key, effective_from, effective_to,
                revision, source, first_available_at, replacement_reason,
                payload_json, payload_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                overlapping.record_id,
                overlapping.dataset_id,
                overlapping.key,
                overlapping.effective_from.isoformat(),
                overlapping.effective_to.isoformat(),
                overlapping.revision,
                overlapping.source,
                overlapping.first_available_at.isoformat(),
                overlapping.replacement_reason,
                json.dumps(dict(overlapping.payload), sort_keys=True),
                overlapping.payload_sha256,
            ),
        )
        connection.commit()

    with pytest.raises(ReferenceDataIntegrityError, match="overlapping"):
        ReferenceRegistry(path)


def test_concurrent_exact_append_is_idempotent(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    record = _record()

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = tuple(executor.map(lambda _: registry.append(record), range(24)))

    assert sum(result.inserted for result in results) == 1
    assert registry.records(dataset_id=record.dataset_id, key=record.key) == (record,)


def test_concurrent_publish_converges_and_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "reference.sqlite"
    registry = ReferenceRegistry(path)
    registry.append(_record())
    published_at = BASE + timedelta(hours=2)

    def publish(_: int) -> str:
        return ReferenceRegistry(path).publish(published_at=published_at).generation_id

    with ThreadPoolExecutor(max_workers=6) as executor:
        generation_ids = tuple(executor.map(publish, range(12)))

    assert len(set(generation_ids)) == 1
    reopened = ReferenceRegistry(path)
    assert reopened.current_pointer().generation_id == generation_ids[0]
