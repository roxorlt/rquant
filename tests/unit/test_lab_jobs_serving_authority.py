from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest

from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.lab_jobs_serving_authority import (
    LabJobsServingAuthorityIntegrityError,
    LabJobsServingAuthorityPublisher,
    LabJobsServingSourceReader,
)
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPublisher,
    ServingSourceAuthorityReader,
)
from rquant.runtime_serving_snapshot import LAB_JOBS_DATASET_ID, LabJobsPayload
from rquant.serving_contracts import FreshnessStatus

from .test_lab_jobs import NOW, _lease, _spec, _submit

COMMIT = "a" * 40
OBSERVED_AT = NOW + timedelta(seconds=20)
PUBLISHED_AT = OBSERVED_AT + timedelta(seconds=5)


def _store(tmp_path: Path) -> LabJobStore:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    return store


def _publisher(tmp_path: Path) -> ServingSourceAuthorityPublisher:
    return ServingSourceAuthorityPublisher(
        root=tmp_path / "authority",
        producer_commit=COMMIT,
        dataset_id=LAB_JOBS_DATASET_ID,
        payload_kind="lab_jobs",
        clock=lambda: PUBLISHED_AT,
    )


def _seed_jobs(store: LabJobStore, count: int) -> None:
    lease = _lease(store)
    for index in range(count):
        result = store.apply_command(
            _submit(job_id=UUID(int=index + 1), spec=_spec()),
            lease=lease,
            now=NOW + timedelta(seconds=index),
        )
        assert result.status == "applied"


def test_empty_database_publishes_fresh_idempotent_authority_without_writing_sqlite(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    database_before = store.path.read_bytes()
    source = LabJobsServingSourceReader(reader=LabJobReader(store.path), max_jobs=10)
    authority = LabJobsServingAuthorityPublisher(
        reader=source,
        publisher=_publisher(tmp_path),
    )

    first = authority.publish(OBSERVED_AT)
    repeated = authority.publish(OBSERVED_AT)
    loaded = ServingSourceAuthorityReader(
        root=tmp_path / "authority",
        expected_producer_commit=COMMIT,
        expected_dataset_id=LAB_JOBS_DATASET_ID,
        expected_payload_kind="lab_jobs",
    )(PUBLISHED_AT)

    assert repeated == first
    assert loaded.dataset_id == LAB_JOBS_DATASET_ID
    assert loaded.status is FreshnessStatus.FRESH
    assert loaded.reason is None
    assert loaded.event_time == OBSERVED_AT
    assert loaded.published_at == OBSERVED_AT
    assert loaded.sequence == int(OBSERVED_AT.timestamp() * 1_000_000)
    assert loaded.payload == LabJobsPayload()
    assert store.path.read_bytes() == database_before


def test_reader_selects_newest_jobs_then_emits_stable_order_with_pit_eta(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 4)
    source = LabJobsServingSourceReader(reader=LabJobReader(store.path), max_jobs=2)

    result = source(OBSERVED_AT)
    repeated = source(OBSERVED_AT)

    assert repeated == result
    assert result.status is FreshnessStatus.FRESH
    assert isinstance(result.payload, LabJobsPayload)
    records = result.payload.lab_jobs
    assert tuple(record.summary.job_id for record in records) == (
        UUID(int=4),
        UUID(int=3),
    )
    assert all(record.eta is not None for record in records)
    assert all(record.eta.as_of == OBSERVED_AT for record in records if record.eta)
    assert result.event_time == OBSERVED_AT


def test_reader_rejects_summary_created_or_updated_after_observed_at(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    result = store.apply_command(
        _submit(job_id=UUID(int=9), spec=_spec()),
        lease=lease,
        now=OBSERVED_AT + timedelta(microseconds=1),
    )
    assert result.status == "applied"

    with pytest.raises(
        LabJobsServingAuthorityIntegrityError,
        match="summary contains future evidence",
    ):
        LabJobsServingSourceReader(reader=LabJobReader(store.path))(OBSERVED_AT)


def test_reader_rejects_eta_as_of_after_observed_at(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    reader = LabJobReader(store.path)
    original = reader.estimate_eta

    def future_eta(job_id: UUID, *, as_of, completed_limit=256):  # type: ignore[no-untyped-def]
        estimate = original(job_id, as_of=as_of, completed_limit=completed_limit)
        assert estimate is not None
        return estimate.model_copy(update={"as_of": OBSERVED_AT + timedelta(microseconds=1)})

    monkeypatch.setattr(reader, "estimate_eta", future_eta)

    with pytest.raises(
        LabJobsServingAuthorityIntegrityError,
        match="ETA contains future evidence",
    ):
        LabJobsServingSourceReader(reader=reader)(OBSERVED_AT)


@pytest.mark.parametrize("max_jobs", [0, 101])
def test_reader_rejects_job_limits_outside_authoritative_reader_bound(
    tmp_path: Path,
    max_jobs: int,
) -> None:
    store = _store(tmp_path)

    with pytest.raises(ValueError, match="max_jobs must be between 1 and 100"):
        LabJobsServingSourceReader(
            reader=LabJobReader(store.path),
            max_jobs=max_jobs,
        )
