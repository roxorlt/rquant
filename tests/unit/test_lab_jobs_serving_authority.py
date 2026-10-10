from __future__ import annotations

import hashlib
import os
import sqlite3
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pandas as pd
import pytest

from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
from rquant.lab_jobs_serving_authority import (
    LabJobsServingAuthorityIntegrityError,
    LabJobsServingAuthorityPublisher,
    LabJobsServingSourceReader,
    _read_verified_parquet,
)
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPublisher,
    ServingSourceAuthorityReader,
)
from rquant.runtime_serving_snapshot import LAB_JOBS_DATASET_ID, LabJobsPayload
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    build_serving_read_models,
)

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


def test_verified_parquet_ignores_atime_only_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = tmp_path / "bundle"
    tables = bundle / "tables"
    tables.mkdir(parents=True)
    path = tables / "summary.parquet"
    expected = pd.DataFrame([{"value": 7}])
    expected.to_parquet(path, index=False)
    payload = path.read_bytes()
    physical = path.stat()
    real_fstat = os.fstat
    calls = 0

    def atime_changing_fstat(descriptor: int) -> os.stat_result | SimpleNamespace:
        nonlocal calls
        observed = real_fstat(descriptor)
        calls += 1
        if calls != 2:
            return observed
        return SimpleNamespace(
            st_mode=observed.st_mode,
            st_ino=observed.st_ino,
            st_dev=observed.st_dev,
            st_nlink=observed.st_nlink,
            st_uid=observed.st_uid,
            st_gid=observed.st_gid,
            st_size=observed.st_size,
            st_atime_ns=observed.st_atime_ns + 1,
            st_mtime_ns=observed.st_mtime_ns,
            st_ctime_ns=observed.st_ctime_ns,
        )

    monkeypatch.setattr(
        "rquant.lab_jobs_serving_authority.os.fstat",
        atime_changing_fstat,
    )

    actual = _read_verified_parquet(
        bundle,
        relative_path="tables/summary.parquet",
        expected_size=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
        expected_identity=(physical.st_dev, physical.st_ino),
    )

    pd.testing.assert_frame_equal(actual, expected)


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

    assert first.written is True
    # Same content, so the second call selects the same generation and writes nothing.
    assert repeated.written is False
    assert repeated.pointer == first.pointer
    assert loaded.dataset_id == LAB_JOBS_DATASET_ID
    assert loaded.status is FreshnessStatus.FRESH
    assert loaded.reason is None
    assert loaded.event_time == OBSERVED_AT
    assert loaded.published_at == OBSERVED_AT
    assert loaded.sequence == int(OBSERVED_AT.timestamp() * 1_000_000)
    assert isinstance(loaded.payload, LabJobsPayload)
    assert loaded.payload.lab_jobs == ()
    assert {item.table_name: item.rows for item in loaded.payload.projections} == {
        "lab_job_event_window": (),
        "lab_job_event": (),
    }
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


def test_reader_publishes_only_stable_trusted_strategy_projections(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    projection = ServingProjectionPayload(
        table_name="strategy_summary",
        available_at=OBSERVED_AT,
        rows=(
            {
                "run_id": "run-1",
                "computed_at": OBSERVED_AT.isoformat(),
                "start_date": "2026-04-01",
                "end_date": "2026-07-14",
                "max_hold_days": 1,
                "entry_mode": "first_break",
                "profile_variant": "baseline",
                "candidates": 1,
                "trades": 1,
                "trigger_rate_pct": 100.0,
                "mean_ret_pct": 2.0,
                "median_ret_pct": 2.0,
                "win_rate_pct": 100.0,
                "best_ret_pct": 2.0,
                "worst_ret_pct": 2.0,
                "gap_stop_rate_pct": 0.0,
            },
        ),
    )
    calls: list[tuple[tuple[UUID, ...], object]] = []

    def trusted_projection_reader(summaries, observed_at):  # type: ignore[no-untyped-def]
        calls.append((tuple(summary.job_id for summary in summaries), observed_at))
        return (projection,)

    source = LabJobsServingSourceReader(
        reader=LabJobReader(store.path),
        strategy_projection_reader=trusted_projection_reader,
    )

    result = source(OBSERVED_AT)

    assert isinstance(result.payload, LabJobsPayload)
    assert {item.table_name for item in result.payload.projections} == {
        "lab_job_event_window",
        "lab_job_event",
        "strategy_summary",
    }
    assert projection in result.payload.projections
    assert calls == [
        ((UUID(int=1),), OBSERVED_AT),
        ((UUID(int=1),), OBSERVED_AT),
    ]


def test_reader_rejects_projection_authority_that_changes_during_snapshot(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    calls = 0

    def unstable_projection_reader(_summaries, observed_at):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return (
            (
                ServingProjectionPayload(
                    table_name="strategy_summary",
                    available_at=observed_at,
                    rows=(),
                ),
            )
            if calls == 1
            else ()
        )

    source = LabJobsServingSourceReader(
        reader=LabJobReader(store.path),
        strategy_projection_reader=unstable_projection_reader,
    )

    with pytest.raises(
        LabJobsServingAuthorityIntegrityError,
        match="strategy projection authority changed",
    ):
        source(OBSERVED_AT)


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


def _add_job(store: LabJobStore, lease: object, index: int) -> None:
    result = store.apply_command(
        _submit(job_id=UUID(int=index + 1), spec=_spec()),
        lease=lease,
        now=NOW + timedelta(seconds=index),
    )
    assert result.status == "applied"


def _generation_files(authority_root: Path) -> set[str]:
    generations = authority_root / "generations"
    return {path.name for path in generations.iterdir()} if generations.exists() else set()


def test_sixty_idle_iterations_over_the_same_jobs_publish_one_generation(
    tmp_path: Path,
) -> None:
    """#271: the thirty-second loop that rebuilt `serving.duckdb` behind it all day.

    Every field this read used to hash a clock into moves on its own: `sequence`,
    `event_time` and `published_at` are the observation instant, and each job's ETA is
    restated as of it, so `as_of` and the whole `finish_at` window slide every thirty
    seconds over jobs nobody has touched since last week.
    """

    store = _store(tmp_path)
    _seed_jobs(store, 3)
    published_at = [PUBLISHED_AT]
    authority = LabJobsServingAuthorityPublisher(
        reader=LabJobsServingSourceReader(reader=LabJobReader(store.path), max_jobs=10),
        publisher=ServingSourceAuthorityPublisher(
            root=tmp_path / "authority",
            producer_commit=COMMIT,
            dataset_id=LAB_JOBS_DATASET_ID,
            payload_kind="lab_jobs",
            clock=lambda: published_at[0],
        ),
    )

    first = authority.publish(OBSERVED_AT)
    assert first.written is True
    settled = _generation_files(tmp_path / "authority")

    written = []
    for iteration in range(1, 61):
        observed = OBSERVED_AT + timedelta(seconds=30 * iteration)
        published_at[0] = observed + timedelta(seconds=5)
        result = authority.reader(observed)
        # The read really is different bytes every time -- that is the whole defect.
        assert result.generation_id != first.pointer.generation_id
        written.append(authority.publish(observed).written)

    assert written == [False] * 60
    assert _generation_files(tmp_path / "authority") == settled


def test_a_job_that_moves_publishes_exactly_one_more_generation(tmp_path: Path) -> None:
    """The ETA is only allowed to go stale while the jobs behind it are standing still."""

    store = _store(tmp_path)
    lease = _lease(store)
    _add_job(store, lease, 0)
    published_at = [PUBLISHED_AT]
    authority = LabJobsServingAuthorityPublisher(
        reader=LabJobsServingSourceReader(reader=LabJobReader(store.path), max_jobs=10),
        publisher=ServingSourceAuthorityPublisher(
            root=tmp_path / "authority",
            producer_commit=COMMIT,
            dataset_id=LAB_JOBS_DATASET_ID,
            payload_kind="lab_jobs",
            clock=lambda: published_at[0],
        ),
    )
    assert authority.publish(OBSERVED_AT).written is True

    moved = OBSERVED_AT + timedelta(seconds=30)
    published_at[0] = moved + timedelta(seconds=5)
    _add_job(store, lease, 1)
    assert authority.publish(moved).written is True

    written = []
    for iteration in range(2, 8):
        observed = OBSERVED_AT + timedelta(seconds=30 * iteration)
        published_at[0] = observed + timedelta(seconds=5)
        written.append(authority.publish(observed).written)
    assert written == [False] * 6


def test_the_lab_jobs_state_identity_drops_only_the_instant_it_was_asked(
    tmp_path: Path,
) -> None:
    """Directly on the function, so the property is not only an emergent one."""

    from rquant.lab_jobs_serving_authority import lab_jobs_state_identity

    store = _store(tmp_path)
    lease = _lease(store)
    _add_job(store, lease, 0)
    _add_job(store, lease, 1)
    reader = LabJobsServingSourceReader(reader=LabJobReader(store.path), max_jobs=10)
    first = reader(OBSERVED_AT)
    later = reader(OBSERVED_AT + timedelta(hours=3))

    assert later.generation_id != first.generation_id
    assert lab_jobs_state_identity(later) == lab_jobs_state_identity(first)

    _add_job(store, lease, 2)
    changed = reader(OBSERVED_AT + timedelta(hours=3))
    assert lab_jobs_state_identity(changed) != lab_jobs_state_identity(first)


def test_research_events_publish_only_fixed_labels_with_job_window(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    secret = "Bearer secret-token /private/operation request_id=abc"
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE lab_event SET reason = ?, request_id = ?",
            (secret, str(UUID(int=99))),
        )

    result = LabJobsServingSourceReader(reader=LabJobReader(store.path))(OBSERVED_AT)
    assert isinstance(result.payload, LabJobsPayload)
    projections = {item.table_name: item for item in result.payload.projections}
    assert set(projections) == {"lab_job_event_window", "lab_job_event"}
    assert projections["lab_job_event_window"].rows == (
        {
            "job_id": str(UUID(int=1)),
            "job_version": 0,
            "state": "available",
            "retained_count": 1,
            "truncated": False,
        },
    )
    event = dict(projections["lab_job_event"].rows[0])
    assert event["job_id"] == str(UUID(int=1))
    assert event["job_version"] == 0
    assert event["label"] == "任务已创建"
    assert event["new_status"] == "queued"
    assert secret not in result.model_dump_json()
    assert "request_id" not in result.model_dump_json()
    assert "fencing" not in result.model_dump_json()


def test_research_events_distinguish_published_empty_and_unincluded_jobs(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 2)
    with sqlite3.connect(store.path) as connection:
        connection.execute("DELETE FROM lab_event WHERE job_id = ?", (str(UUID(int=2)),))

    result = LabJobsServingSourceReader(reader=LabJobReader(store.path), max_jobs=1)(OBSERVED_AT)
    assert isinstance(result.payload, LabJobsPayload)
    projections = {item.table_name: item for item in result.payload.projections}
    assert projections["lab_job_event_window"].rows == (
        {
            "job_id": str(UUID(int=2)),
            "job_version": 0,
            "state": "empty",
            "retained_count": 0,
            "truncated": False,
        },
    )
    assert projections["lab_job_event"].rows == ()
    assert str(UUID(int=1)) not in str(projections["lab_job_event_window"].rows)


def test_research_event_reader_rejects_latest_event_version_conflict(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    with sqlite3.connect(store.path) as connection:
        connection.execute("UPDATE lab_event SET job_version = 7")

    with pytest.raises(Exception, match="event.*version|version.*event"):
        LabJobsServingSourceReader(reader=LabJobReader(store.path))(OBSERVED_AT)


def test_research_event_reader_rejects_job_timestamp_without_matching_event(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    with store._transaction() as connection:
        connection.execute(
            "UPDATE lab_job SET updated_at = ? WHERE job_id = ?",
            ((NOW + timedelta(microseconds=1)).isoformat(), str(UUID(int=1))),
        )

    with pytest.raises(Exception, match="event.*time|time.*event"):
        LabJobsServingSourceReader(reader=LabJobReader(store.path))(OBSERVED_AT)


def test_research_events_follow_real_job_transition_without_reason(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    submitted = store.apply_command(_submit(job_id=UUID(int=1), spec=_spec()), lease=lease, now=NOW)
    assert submitted.status == "applied"
    job = LabJobReader(store.path).get_job(UUID(int=1))
    assert job is not None
    store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="Bearer hidden-token",
        now=NOW + timedelta(seconds=1),
    )

    result = LabJobsServingSourceReader(reader=LabJobReader(store.path))(OBSERVED_AT)
    assert isinstance(result.payload, LabJobsPayload)
    events = next(
        projection.rows
        for projection in result.payload.projections
        if projection.table_name == "lab_job_event"
    )
    assert [(row["job_version"], row["new_status"]) for row in events] == [
        (1, "running"),
        (0, "queued"),
    ]
    assert "Bearer hidden-token" not in result.model_dump_json()


def test_research_events_reject_missing_history_after_job_advanced(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    receipt = store.apply_command(_submit(job_id=UUID(int=1), spec=_spec()), lease=lease, now=NOW)
    assert receipt.status == "applied"
    store.transition_job(
        UUID(int=1),
        expected_version=0,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="start",
        now=NOW + timedelta(seconds=1),
    )
    with sqlite3.connect(store.path) as connection:
        connection.execute("DELETE FROM lab_event WHERE job_id = ?", (str(UUID(int=1)),))

    with pytest.raises(Exception, match="event.*missing|missing.*event"):
        LabJobsServingSourceReader(reader=LabJobReader(store.path))(OBSERVED_AT)


@pytest.mark.parametrize("bad_version", [7, "broken"])
def test_corrupt_research_event_replaces_old_authority_with_unavailable_state(
    tmp_path: Path,
    bad_version: int | str,
) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    published_at = [PUBLISHED_AT]
    authority = LabJobsServingAuthorityPublisher(
        reader=LabJobsServingSourceReader(reader=LabJobReader(store.path)),
        publisher=ServingSourceAuthorityPublisher(
            root=tmp_path / "authority",
            producer_commit=COMMIT,
            dataset_id=LAB_JOBS_DATASET_ID,
            payload_kind="lab_jobs",
            clock=lambda: published_at[0],
        ),
    )
    first = authority.publish(OBSERVED_AT)
    assert first.written
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE lab_event SET job_version = ?, reason = ?",
            (bad_version, "Bearer private-secret"),
        )
    later = OBSERVED_AT + timedelta(minutes=1)
    published_at[0] = later + timedelta(seconds=5)

    second = authority.publish(later)
    loaded = ServingSourceAuthorityReader(
        root=tmp_path / "authority",
        expected_producer_commit=COMMIT,
        expected_dataset_id=LAB_JOBS_DATASET_ID,
        expected_payload_kind="lab_jobs",
    )(published_at[0])
    assert second.written
    assert second.pointer.generation_id != first.pointer.generation_id
    assert loaded.status is FreshnessStatus.UNAVAILABLE
    assert loaded.reason == "lab_event_snapshot_invalid"
    assert loaded.payload == LabJobsPayload()
    assert "private-secret" not in loaded.model_dump_json()
    assert authority.publish(later).written is False

    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE lab_event SET job_version = 0 WHERE job_id = ?",
            (str(UUID(int=1)),),
        )
    recovered_at = later + timedelta(minutes=1)
    published_at[0] = recovered_at + timedelta(seconds=5)
    recovered = authority.publish(recovered_at)
    assert recovered.written
    restored = ServingSourceAuthorityReader(
        root=tmp_path / "authority",
        expected_producer_commit=COMMIT,
        expected_dataset_id=LAB_JOBS_DATASET_ID,
        expected_payload_kind="lab_jobs",
    )(published_at[0])
    assert restored.status is FreshnessStatus.FRESH
    assert {item.table_name for item in restored.payload.projections} == {
        "lab_job_event_window",
        "lab_job_event",
    }


def test_unknown_lab_publisher_failure_does_not_replace_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    source = LabJobsServingSourceReader(reader=LabJobReader(store.path))
    authority = LabJobsServingAuthorityPublisher(reader=source, publisher=_publisher(tmp_path))
    first = authority.publish(OBSERVED_AT)

    def unknown_failure(_reader: LabJobReader, *, limit: int) -> None:
        raise RuntimeError("unclassified failure")

    monkeypatch.setattr("rquant.experiment_platform_projection.legacy_job_snapshot", unknown_failure)
    with pytest.raises(RuntimeError, match="unclassified failure"):
        authority.publish(OBSERVED_AT + timedelta(seconds=1))
    loaded = ServingSourceAuthorityReader(
        root=tmp_path / "authority",
        expected_producer_commit=COMMIT,
        expected_dataset_id=LAB_JOBS_DATASET_ID,
        expected_payload_kind="lab_jobs",
    )(PUBLISHED_AT)
    assert loaded.generation_id == first.pointer.generation_id


def test_research_event_reader_marks_recent_500_as_truncated(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    with store._transaction() as connection:
        for version in range(1, 502):
            event_at = NOW + timedelta(microseconds=version)
            connection.execute(
                "INSERT INTO lab_event (job_id, event_type, prior_status, new_status, "
                "job_version, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    str(UUID(int=1)),
                    "unrecognized_internal_type",
                    "queued",
                    "queued",
                    version,
                    "secret-not-for-page",
                    event_at.isoformat(timespec="microseconds"),
                ),
            )
        connection.execute(
            "UPDATE lab_job SET version = ?, updated_at = ? WHERE job_id = ?",
            (
                501,
                (NOW + timedelta(microseconds=501)).isoformat(timespec="microseconds"),
                str(UUID(int=1)),
            ),
        )

    result = LabJobsServingSourceReader(reader=LabJobReader(store.path))(OBSERVED_AT)
    assert isinstance(result.payload, LabJobsPayload)
    projections = {item.table_name: item for item in result.payload.projections}
    assert projections["lab_job_event_window"].rows[0]["state"] == "truncated"
    assert projections["lab_job_event_window"].rows[0]["retained_count"] == 500
    assert len(projections["lab_job_event"].rows) == 500
    assert all(item["label"] == "状态已更新" for item in projections["lab_job_event"].rows)
    assert "secret-not-for-page" not in result.model_dump_json()


def test_research_event_reader_uses_same_sqlite_snapshot_for_jobs_and_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    reader = LabJobReader(store.path)
    original = reader._summary_from_row
    changed = False

    def change_after_job_read(row):  # type: ignore[no-untyped-def]
        nonlocal changed
        summary = original(row)
        if not changed:
            changed = True
            with sqlite3.connect(store.path) as connection:
                connection.execute(
                    "UPDATE lab_event SET event_type = ? WHERE job_id = ?",
                    ("unrecognized_internal_type", str(summary.job_id)),
                )
        return summary

    monkeypatch.setattr(reader, "_summary_from_row", change_after_job_read)
    snapshot = reader.list_published_jobs_with_events(limit=1)

    assert changed
    assert snapshot.windows[0].events[0].event_type == "job_submitted"
    assert LabJobReader(store.path).list_published_jobs_with_events(
        limit=1
    ).windows[0].events[0].event_type == "unrecognized_internal_type"


def test_research_event_budget_marks_every_published_job_truncated(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 2)
    with store._transaction() as connection:
        for index in (1, 2):
            for version in (1, 2):
                event_at = NOW + timedelta(seconds=index - 1, microseconds=version)
                connection.execute(
                    "INSERT INTO lab_event (job_id, event_type, prior_status, new_status, "
                    "job_version, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        str(UUID(int=index)),
                        "job_transitioned",
                        "queued",
                        "queued",
                        version,
                        "hidden",
                        event_at.isoformat(timespec="microseconds"),
                    ),
                )
            connection.execute(
                "UPDATE lab_job SET version = 2, updated_at = ? WHERE job_id = ?",
                (event_at.isoformat(timespec="microseconds"), str(UUID(int=index))),
            )

    snapshot = LabJobReader(store.path).list_published_jobs_with_events(
        limit=2, max_total_events=2
    )
    assert len(snapshot.windows) == 2
    assert all(len(window.events) == 1 and window.truncated for window in snapshot.windows)


def test_research_event_payload_rejects_unregistered_label(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    payload = LabJobsServingSourceReader(reader=LabJobReader(store.path))(OBSERVED_AT).payload
    assert isinstance(payload, LabJobsPayload)
    window, event = payload.projections
    unsafe_event = ServingProjectionPayload(
        table_name="lab_job_event",
        available_at=event.available_at,
        rows=({**event.rows[0], "label": "Bearer secret"},),
    )

    with pytest.raises(ValueError, match="event.*label"):
        LabJobsPayload(lab_jobs=payload.lab_jobs, projections=(window, unsafe_event))


def test_research_event_payload_rejects_window_missing_published_job(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    payload = LabJobsServingSourceReader(reader=LabJobReader(store.path))(OBSERVED_AT).payload
    assert isinstance(payload, LabJobsPayload)
    window, event = payload.projections
    empty_window = ServingProjectionPayload(
        table_name="lab_job_event_window",
        available_at=window.available_at,
        rows=(),
    )

    with pytest.raises(ValueError, match="event.*job"):
        LabJobsPayload(lab_jobs=payload.lab_jobs, projections=(empty_window, event))


def test_research_events_enter_same_serving_read_model_generation(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed_jobs(store, 1)
    source = LabJobsServingSourceReader(reader=LabJobReader(store.path))(OBSERVED_AT)
    assert isinstance(source.payload, LabJobsPayload)
    read_model = ServingReadModelInput(
        observed_at=OBSERVED_AT,
        lab_jobs=source.payload.lab_jobs,
        projections=tuple(
            ServingProjectionInput.bind(
                projection,
                owner_dataset_id="lab_jobs",
                owner_generation_id=source.generation_id,
            )
            for projection in source.payload.projections
        ),
    )
    tables = build_serving_read_models(read_model)
    assert len(tables["lab_job_event"]) == 1
    assert len(tables["lab_job_event_window"]) == 1
    assert tables["lab_job_event"].iloc[0]["label"] == "任务已创建"
