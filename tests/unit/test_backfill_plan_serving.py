"""A sealed proposal is readable without becoming an executable task."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.backfill_plan_artifact import load_daily_bar_backfill_plan
from rquant.backfill_plan_jobs import BackfillPlanJobStore, BackfillPlanJobWorker
from rquant.runtime_builder_authority import LabJobsPublisherSettings
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_page_projection_source import (
    DuckDBLabPageProjectionSource,
    LabPageProjectionSnapshot,
    PageProjectionSourceIntegrityError,
)
from rquant.serving_publisher import ServingPublisher, ServingReader
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_backfill_plan_artifact import _publish, _snapshot
from tests.unit.test_backfill_plan_core import _rehash
from tests.unit.test_backfill_plan_jobs import _Clock, _request
from tests.unit.test_data_audit_report_serving import _production_report_file

OBSERVED = datetime(2026, 10, 1, 12, tzinfo=UTC)
TABLES = {
    "backfill_plan_catalog",
    "backfill_plan_index",
    "backfill_plan_preview",
    "backfill_plan_archive",
    "backfill_plan_progress",
    "backfill_plan_job",
    "backfill_plan_event",
}


@pytest.fixture(autouse=True)
def _current_observation(monkeypatch: pytest.MonkeyPatch) -> None:
    # Collection can precede the real inode publication in this test by hours.
    monkeypatch.setattr(f"{__name__}.OBSERVED", datetime.now(UTC) + timedelta(minutes=5))


def _source(
    tmp_path: Path,
    directory: Path | None = None,
    job_state: Path | None = None,
) -> DuckDBLabPageProjectionSource:
    database = tmp_path / "research_ro.duckdb"
    with DuckDBStore(database):
        pass
    return DuckDBLabPageProjectionSource(
        database,
        backfill_plan_directory=directory,
        backfill_plan_job_state_path=job_state,
    )


def _rows(source: DuckDBLabPageProjectionSource) -> dict[str, tuple[object, ...]]:
    return {item.table_name: item.rows for item in source(OBSERVED).projections}


def test_multiple_plans_publish_complete_preview_and_separate_unknown_progress(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    first_path = _publish(snapshot, directory, evidence_code_revision="rev-a")
    second_path = _publish(snapshot, directory, evidence_code_revision="rev-b")
    first = load_daily_bar_backfill_plan(first_path)
    second = load_daily_bar_backfill_plan(second_path)
    assert first.content_sha256 != second.content_sha256
    assert _publish(snapshot, directory, evidence_code_revision="rev-a") == first_path

    source = _source(tmp_path, directory)
    snapshot_projection = source(OBSERVED)
    rows = {item.table_name: item.rows for item in snapshot_projection.projections}
    assert rows.keys() >= TABLES
    catalog = rows["backfill_plan_catalog"][0]
    assert catalog["total_plan_count"] == 2
    assert catalog["indexed_plan_count"] == 2
    assert catalog["preview_plan_count"] == 2
    assert catalog["has_older_plans"] is False
    assert len(rows["backfill_plan_archive"]) == 2
    index = rows["backfill_plan_index"]
    assert [row["rank"] for row in index] == [0, 1]
    assert {row["plan_hash"] for row in index} == {
        first.content_sha256,
        second.content_sha256,
    }
    previews = rows["backfill_plan_preview"]
    assert {row["plan_hash"] for row in previews} == {
        first.content_sha256,
        second.content_sha256,
    }
    for row in previews:
        plan = first if row["plan_hash"] == first.content_sha256 else second
        assert json.loads(row["missing_dates_json"]) == [
            day.isoformat() for day in plan.missing_dates
        ]
        assert json.loads(row["monthly_json"]) == [
            month.model_dump(mode="json") for month in plan.monthly
        ]
        assert json.loads(row["estimate_json"]) == plan.estimate.model_dump(mode="json")
        assert json.loads(row["source_json"]) == plan.source.model_dump(mode="json")
    assert rows["backfill_plan_progress"] == (
        {"status_key": "current", "availability": "unavailable", "task_id": None},
    )
    assert rows["backfill_plan_job"][0]["availability"] == "unavailable"
    assert all(row["source_mode"] == "production_unverified" for row in index)
    assert all(row["identity_verified"] is False for row in index)
    assert all(row["collection_complete_verified"] is False for row in index)
    assert all(row["quota_status"] == "unverified" for row in index)
    assert all(row["executable"] is False for row in index)

    served = build_serving_read_models(
        ServingReadModelInput(
            observed_at=OBSERVED,
            projections=tuple(
                ServingProjectionInput.bind(
                    item, owner_dataset_id="lab_jobs", owner_generation_id="a" * 64
                )
                for item in snapshot_projection.projections
            ),
        )
    )
    assert len(served["backfill_plan_index"]) == 2
    assert len(served["backfill_plan_preview"]) == 2


def test_absent_configuration_and_empty_directory_are_distinct(tmp_path: Path) -> None:
    absent = _rows(_source(tmp_path))
    configured = _rows(_source(tmp_path, tmp_path / "plans"))
    assert TABLES.isdisjoint(absent)
    assert configured.keys() >= TABLES
    assert configured["backfill_plan_index"] == ()
    assert configured["backfill_plan_preview"] == ()
    assert configured["backfill_plan_archive"] == ()
    assert configured["backfill_plan_catalog"][0]["total_plan_count"] == 0
    assert configured["backfill_plan_job"][0]["availability"] == "unavailable"


def test_missing_job_file_is_unavailable_without_creating_it(tmp_path: Path) -> None:
    directory = tmp_path / "plans"
    directory.mkdir()
    state_path = tmp_path / "job-state.sqlite"

    rows = _rows(_source(tmp_path, directory, state_path))

    assert rows["backfill_plan_job"][0]["availability"] == "unavailable"
    assert rows["backfill_plan_event"] == ()
    assert not state_path.exists()


def test_existing_empty_job_store_and_queued_job_have_distinct_progress(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "plans"
    directory.mkdir()
    state_path = tmp_path / "job-state.sqlite"
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    source = _source(tmp_path, directory, state_path)

    empty = _rows(source)
    assert empty["backfill_plan_job"][0]["availability"] == "empty"
    assert empty["backfill_plan_event"] == ()

    task = store.submit(_request(_snapshot(tmp_path)))
    queued = _rows(source)
    progress = queued["backfill_plan_job"][0]
    assert progress["availability"] == "ready"
    assert progress["status"] == "queued"
    assert progress["task_id"] == task.task_id
    assert progress["attempts"] == 0
    assert progress["plan_hash"] is None
    assert queued["backfill_plan_event"] == (
        {
            "event_id": 1,
            "task_id": task.task_id,
            "event_type": "queued",
            "attempts": 0,
            "occurred_at": task.created_at.isoformat().replace("+00:00", "Z"),
            "error_code": None,
        },
    )


def test_succeeded_task_requires_its_plan_in_same_generation(tmp_path: Path) -> None:
    directory = tmp_path / "plans"
    state_path = tmp_path / "job-state.sqlite"
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    store.submit(_request(_snapshot(tmp_path)))
    completed = BackfillPlanJobWorker(store).run_one()
    assert completed is not None and completed.plan_hash is not None
    source = _source(tmp_path, directory, state_path)

    projected = _rows(source)
    progress = projected["backfill_plan_job"][0]
    assert progress["status"] == "succeeded"
    assert progress["plan_hash"] == completed.plan_hash
    assert progress["task_id"] == completed.task_id
    assert projected["backfill_plan_event"][-1]["event_type"] == "succeeded"
    assert completed.plan_hash in {row["plan_hash"] for row in projected["backfill_plan_archive"]}

    plan = directory / f"daily-bar-backfill-plan-v1-{completed.plan_hash}.json"
    plan.unlink()
    with pytest.raises(PageProjectionSourceIntegrityError, match="succeeded|same-generation|plan"):
        source(OBSERVED)


def test_job_status_and_events_use_one_live_sqlite_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import backfill_plan_job_projection as job_projection

    directory = tmp_path / "plans"
    state_path = tmp_path / "job-state.sqlite"
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    task = store.submit(_request(_snapshot(tmp_path)))
    source = _source(tmp_path, directory, state_path)
    real_connect = sqlite3.connect
    committed: list[bool] = []

    def connect_with_transition(*args: object, **kwargs: object) -> sqlite3.Connection:
        connection = real_connect(*args, **kwargs)

        def on_read(statement: str) -> None:
            if "FROM backfill_plan_job_event" not in statement or committed:
                return
            changed_at = "2026-02-06T02:01:00+00:00"
            with real_connect(state_path) as writer:
                writer.execute(
                    "UPDATE backfill_plan_job SET status='running', attempts=1, "
                    "updated_at=? WHERE task_id=?",
                    (changed_at, task.task_id),
                )
                writer.execute(
                    "INSERT INTO backfill_plan_job_event "
                    "(task_id,event_type,attempts,occurred_at) VALUES (?,?,?,?)",
                    (task.task_id, "started", 1, changed_at),
                )
            committed.append(True)

        connection.set_trace_callback(on_read)
        return connection

    monkeypatch.setattr(job_projection.sqlite3, "connect", connect_with_transition)
    during = _rows(source)
    assert committed == [True]
    assert during["backfill_plan_job"][0]["status"] == "queued"
    assert [item["event_type"] for item in during["backfill_plan_event"]] == ["queued"]
    monkeypatch.setattr(job_projection.sqlite3, "connect", real_connect)

    after = _rows(source)
    assert after["backfill_plan_job"][0]["status"] == "running"
    assert [item["event_type"] for item in after["backfill_plan_event"]] == [
        "queued",
        "started",
    ]


def test_tampered_or_future_task_state_refuses_generation(tmp_path: Path) -> None:
    directory = tmp_path / "plans"
    state_path = tmp_path / "job-state.sqlite"
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    task = store.submit(_request(_snapshot(tmp_path)))
    source = _source(tmp_path, directory, state_path)
    with sqlite3.connect(state_path) as connection:
        connection.execute(
            "UPDATE backfill_plan_job SET status='running', attempts=1 WHERE task_id=?",
            (task.task_id,),
        )
    with pytest.raises(PageProjectionSourceIntegrityError, match="progress"):
        source(OBSERVED)

    with sqlite3.connect(state_path) as connection:
        connection.execute(
            "UPDATE backfill_plan_job SET status='queued', attempts=0, "
            "updated_at=? WHERE task_id=?",
            ((OBSERVED + timedelta(days=1)).isoformat(), task.task_id),
        )
    with pytest.raises(PageProjectionSourceIntegrityError, match="newer than observation"):
        source(OBSERVED)


def test_unsupported_job_schema_and_excess_events_refuse_generation(tmp_path: Path) -> None:
    directory = tmp_path / "plans"
    state_path = tmp_path / "job-state.sqlite"
    directory.mkdir()
    with sqlite3.connect(state_path) as connection:
        connection.execute("CREATE TABLE old_job(id TEXT)")
    source = _source(tmp_path, directory, state_path)
    with pytest.raises(PageProjectionSourceIntegrityError, match="job state invalid"):
        source(OBSERVED)

    state_path.unlink()
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    task = store.submit(_request(_snapshot(tmp_path)))
    with sqlite3.connect(state_path) as connection:
        connection.executemany(
            "INSERT INTO backfill_plan_job_event "
            "(task_id,event_type,attempts,occurred_at) VALUES (?,?,?,?)",
            [(task.task_id, "queued", 0, task.created_at.isoformat())] * 64,
        )
    with pytest.raises(PageProjectionSourceIntegrityError, match="stored bound"):
        source(OBSERVED)


def test_legacy_job_store_without_event_table_keeps_status_without_invented_logs(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "plans"
    state_path = tmp_path / "job-state.sqlite"
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    task = store.submit(_request(_snapshot(tmp_path)))
    with sqlite3.connect(state_path) as connection:
        connection.execute("DROP TABLE backfill_plan_job_event")
        if any(
            row[1] == "event_history_complete"
            for row in connection.execute("PRAGMA table_info(backfill_plan_job)")
        ):
            connection.execute("ALTER TABLE backfill_plan_job DROP COLUMN event_history_complete")
    BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    source = _source(tmp_path, directory, state_path)

    queued = _rows(source)
    assert queued["backfill_plan_job"][0]["task_id"] == task.task_id
    assert queued["backfill_plan_job"][0]["status"] == "queued"
    assert queued["backfill_plan_job"][0]["event_history"] == "unavailable"
    assert queued["backfill_plan_event"] == ()


def test_legacy_success_without_events_still_requires_same_generation_plan(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "plans"
    state_path = tmp_path / "job-state.sqlite"
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    store.submit(_request(_snapshot(tmp_path)))
    completed = BackfillPlanJobWorker(store).run_one()
    assert completed is not None and completed.plan_hash is not None
    with sqlite3.connect(state_path) as connection:
        connection.execute("DROP TABLE backfill_plan_job_event")
        if any(
            row[1] == "event_history_complete"
            for row in connection.execute("PRAGMA table_info(backfill_plan_job)")
        ):
            connection.execute("ALTER TABLE backfill_plan_job DROP COLUMN event_history_complete")
    BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    source = _source(tmp_path, directory, state_path)

    served = _rows(source)
    assert served["backfill_plan_job"][0]["status"] == "succeeded"
    assert served["backfill_plan_job"][0]["event_history"] == "unavailable"
    assert served["backfill_plan_event"] == ()
    (directory / f"daily-bar-backfill-plan-v1-{completed.plan_hash}.json").unlink()
    with pytest.raises(PageProjectionSourceIntegrityError, match="succeeded|plan"):
        source(OBSERVED)


def test_intermediate_old_store_with_empty_event_table_keeps_legacy_status(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "plans"
    state_path = tmp_path / "job-state.sqlite"
    snapshot = _snapshot(tmp_path)
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    old = store.submit(_request(snapshot))
    with sqlite3.connect(state_path) as connection:
        connection.execute("DROP TABLE backfill_plan_job_event")
        connection.execute("ALTER TABLE backfill_plan_job DROP COLUMN event_history_complete")
        connection.execute(
            """
            CREATE TABLE backfill_plan_job_event (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                occurred_at TEXT NOT NULL,
                error_code TEXT
            )
            """
        )
        assert connection.execute("SELECT COUNT(*) FROM backfill_plan_job_event").fetchone()[0] == 0
        assert (
            connection.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='backfill_plan_job_event'"
            ).fetchone()
            is None
        )

    reopened = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    source = _source(tmp_path, directory, state_path)
    old_projection = _rows(source)
    assert old_projection["backfill_plan_job"][0]["task_id"] == old.task_id
    assert old_projection["backfill_plan_job"][0]["event_history"] == "unavailable"
    assert old_projection["backfill_plan_event"] == ()

    new = reopened.submit(_request(snapshot, key="request-00000002"))
    new_projection = _rows(source)
    assert new_projection["backfill_plan_job"][0]["task_id"] == new.task_id
    assert new_projection["backfill_plan_job"][0]["event_history"] == "available"
    assert [row["event_type"] for row in new_projection["backfill_plan_event"]] == ["queued"]
    with sqlite3.connect(state_path) as connection:
        connection.execute("DELETE FROM backfill_plan_job_event WHERE task_id=?", (new.task_id,))
    with pytest.raises(PageProjectionSourceIntegrityError, match="latest event"):
        source(OBSERVED)


def test_used_pre_marker_event_table_with_removed_events_is_not_legacy(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "plans"
    state_path = tmp_path / "job-state.sqlite"
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    task = store.submit(_request(_snapshot(tmp_path)))
    with sqlite3.connect(state_path) as connection:
        connection.execute("ALTER TABLE backfill_plan_job DROP COLUMN event_history_complete")
        connection.execute("DELETE FROM backfill_plan_job_event WHERE task_id=?", (task.task_id,))
        assert (
            connection.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='backfill_plan_job_event'"
            ).fetchone()[0]
            > 0
        )
    BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())

    with pytest.raises(PageProjectionSourceIntegrityError, match="latest event"):
        _source(tmp_path, directory, state_path)(OBSERVED)


def test_new_task_missing_its_events_refuses_generation(tmp_path: Path) -> None:
    directory = tmp_path / "plans"
    state_path = tmp_path / "job-state.sqlite"
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    task = store.submit(_request(_snapshot(tmp_path)))
    with sqlite3.connect(state_path) as connection:
        connection.execute("DELETE FROM backfill_plan_job_event WHERE task_id=?", (task.task_id,))

    with pytest.raises(PageProjectionSourceIntegrityError, match="latest event"):
        _source(tmp_path, directory, state_path)(OBSERVED)

    with sqlite3.connect(state_path) as connection:
        connection.execute("DROP TABLE backfill_plan_job_event")
    with pytest.raises(ValueError, match="event history|event table"):
        BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())


def test_read_only_job_reader_does_not_create_missing_wal_sidecar(tmp_path: Path) -> None:
    directory = tmp_path / "plans"
    state_path = tmp_path / "job-state.sqlite"
    store = BackfillPlanJobStore(state_path=state_path, plan_directory=directory, clock=_Clock())
    store.submit(_request(_snapshot(tmp_path)))
    writer = sqlite3.connect(state_path)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("UPDATE backfill_plan_job SET updated_at=updated_at")
    writer.commit()
    wal = Path(f"{state_path}-wal")
    shm = Path(f"{state_path}-shm")
    assert wal.is_file() and shm.is_file()
    shm.unlink()
    try:
        with pytest.raises(PageProjectionSourceIntegrityError, match="shared memory|sidecar"):
            _source(tmp_path, directory, state_path)(OBSERVED)
        assert not shm.exists()
    finally:
        writer.close()


def test_in_progress_lab_temp_file_does_not_hide_published_plans(tmp_path: Path) -> None:
    directory = tmp_path / "plans"
    plan = _publish(_snapshot(tmp_path), directory)
    (directory / (".backfill-plan-" + "a" * 32)).write_bytes(b"unfinished")

    rows = _rows(_source(tmp_path, directory))

    assert rows["backfill_plan_catalog"][0]["total_plan_count"] == 1
    assert rows["backfill_plan_index"][0]["plan_hash"] == (
        load_daily_bar_backfill_plan(plan).content_sha256
    )


def test_plan_directory_requires_an_explicit_absolute_research_reader(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="backfill_plan_directory|research_metadata_path"):
        LabJobsPublisherSettings(
            lab_jobs_path=tmp_path / "jobs.sqlite3",
            authority_root=tmp_path / "authority",
            backfill_plan_directory=tmp_path / "plans",
        )
    with pytest.raises(ValueError, match="absolute"):
        LabJobsPublisherSettings(
            lab_jobs_path=tmp_path / "jobs.sqlite3",
            research_metadata_path=tmp_path / "research.duckdb",
            authority_root=tmp_path / "authority",
            backfill_plan_directory=Path("plans"),
        )


def test_plan_and_audit_report_share_one_complete_lab_generation(tmp_path: Path) -> None:
    plans = tmp_path / "plans"
    _publish(_snapshot(tmp_path), plans)
    report = _production_report_file(tmp_path)
    prepared = _source(tmp_path, plans)
    source = DuckDBLabPageProjectionSource(
        prepared.database_path,
        audit_report_path=report,
        backfill_plan_directory=plans,
    )

    snapshot = source(OBSERVED)

    names = {item.table_name for item in snapshot.projections}
    assert names >= TABLES
    assert names >= {
        "audit_report_overview",
        "audit_report_month",
        "audit_report_rule",
        "audit_report_issue",
    }


def test_invalid_contents_and_filename_fail_the_whole_generation(tmp_path: Path) -> None:
    plan_file = _publish(_snapshot(tmp_path), tmp_path / "plans")
    source = _source(tmp_path, plan_file.parent)
    valid = plan_file.read_bytes()
    plan_file.chmod(0o600)
    plan_file.write_bytes(valid.replace(b"production_unverified", b"production_verified", 1))
    with pytest.raises(PageProjectionSourceIntegrityError, match="invalid|digest|canonical"):
        source(OBSERVED)

    plan_file.write_bytes(valid)
    alias = plan_file.with_name("daily-bar-backfill-plan-v1-" + "0" * 64 + ".json")
    alias.write_bytes(valid)
    with pytest.raises(PageProjectionSourceIntegrityError, match="filename|hash|invalid"):
        source(OBSERVED)

    alias.unlink()
    stray = plan_file.parent / "unaddressed-plan.json"
    stray.write_bytes(valid)
    with pytest.raises(PageProjectionSourceIntegrityError, match="filename|unexpected"):
        source(OBSERVED)

    stray.unlink()
    plan_file.unlink()
    plan_file.symlink_to(tmp_path / "elsewhere.json")
    with pytest.raises(PageProjectionSourceIntegrityError, match="symlink|regular"):
        source(OBSERVED)


def test_file_and_directory_rotation_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import serving_page_projection_source as page_source

    plan_file = _publish(_snapshot(tmp_path), tmp_path / "plans")
    source = _source(tmp_path, plan_file.parent)
    original = page_source._read_bound_optional_file

    def replace_file(*args: object, **kwargs: object):
        found = original(*args, **kwargs)
        replacement = tmp_path / "replacement.json"
        replacement.write_bytes(plan_file.read_bytes())
        os.replace(replacement, plan_file)
        return found

    monkeypatch.setattr(page_source, "_read_bound_optional_file", replace_file)
    with pytest.raises(PageProjectionSourceIntegrityError, match="rotated|changed"):
        source(OBSERVED)
    monkeypatch.setattr(page_source, "_read_bound_optional_file", original)

    def replace_directory(*args: object, **kwargs: object):
        found = original(*args, **kwargs)
        os.replace(plan_file.parent, tmp_path / "old-plans")
        plan_file.parent.mkdir()
        return found

    monkeypatch.setattr(page_source, "_read_bound_optional_file", replace_directory)
    with pytest.raises(PageProjectionSourceIntegrityError, match="rotated|changed"):
        source(OBSERVED)


def test_snapshot_contract_cannot_promote_plan_or_invent_progress(tmp_path: Path) -> None:
    directory = tmp_path / "plans"
    _publish(_snapshot(tmp_path), directory)
    projections = tuple(
        item
        for item in _source(tmp_path, directory)(OBSERVED).projections
        if item.table_name in TABLES
    )
    by_name = {item.table_name: item for item in projections}
    index = by_name["backfill_plan_index"]
    promoted = ServingProjectionPayload(
        table_name=index.table_name,
        available_at=index.available_at,
        rows=({**dict(index.rows[0]), "executable": True},),
    )
    with pytest.raises(ValueError, match="unverified|executable|backfill"):
        LabPageProjectionSnapshot.create(
            available_at=OBSERVED,
            backfill_plan_projections=tuple(
                promoted if item.table_name == index.table_name else item for item in projections
            ),
        )
    progress = by_name["backfill_plan_job"]
    invented = ServingProjectionPayload(
        table_name=progress.table_name,
        available_at=progress.available_at,
        rows=(
            {
                **dict(progress.rows[0]),
                "availability": "ready",
                "event_history": "available",
                "task_id": "a" * 32,
                "status": "queued",
                "attempts": 0,
                "created_at": progress.available_at.isoformat(),
                "updated_at": progress.available_at.isoformat(),
            },
        ),
    )
    with pytest.raises(ValueError, match="progress"):
        LabPageProjectionSnapshot.create(
            available_at=OBSERVED,
            backfill_plan_projections=tuple(
                invented if item.table_name == progress.table_name else item for item in projections
            ),
        )
    archive = by_name["backfill_plan_archive"]
    damaged = ServingProjectionPayload(
        table_name=archive.table_name,
        available_at=archive.available_at,
        rows=({**dict(archive.rows[0]), "detail_sha256": "0" * 64},),
    )
    with pytest.raises(ValueError, match="archive|hash|detail"):
        LabPageProjectionSnapshot.create(
            available_at=OBSERVED,
            backfill_plan_projections=tuple(
                damaged if item.table_name == archive.table_name else item for item in projections
            ),
        )


def test_old_plan_can_be_reopened_by_content_hash_after_preview_window(
    tmp_path: Path,
) -> None:
    from rquant.backfill_plan_projection import MAX_PREVIEW_BACKFILL_PLANS

    database = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    paths = [
        _publish(database, directory, evidence_code_revision=f"rev-{index:02}")
        for index in range(MAX_PREVIEW_BACKFILL_PLANS + 1)
    ]
    old_plan = load_daily_bar_backfill_plan(paths[0])
    source = _source(tmp_path, directory)
    rows = _rows(source)
    catalog = rows["backfill_plan_catalog"][0]
    assert catalog["total_plan_count"] == MAX_PREVIEW_BACKFILL_PLANS + 1
    assert catalog["has_older_plans"] is False
    assert len(rows["backfill_plan_index"]) == MAX_PREVIEW_BACKFILL_PLANS + 1
    assert len(rows["backfill_plan_preview"]) == MAX_PREVIEW_BACKFILL_PLANS
    assert old_plan.content_sha256 not in {
        row["plan_hash"] for row in rows["backfill_plan_preview"]
    }
    from rquant.backfill_plan_projection import read_backfill_plan_detail_from_serving

    projected = source(OBSERVED)
    tables = build_serving_read_models(
        ServingReadModelInput(
            observed_at=OBSERVED,
            projections=tuple(
                ServingProjectionInput.bind(
                    item, owner_dataset_id="lab_jobs", owner_generation_id="a" * 64
                )
                for item in projected.projections
            ),
        )
    )
    root = tmp_path / "serving"
    ServingPublisher(root, producer_commit="b" * 40, table_specs=SERVING_TABLE_SPECS).publish(
        tables,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="lab_jobs",
                generation_id="a" * 64,
                event_time=OBSERVED,
                published_at=OBSERVED,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"lab_jobs": "a" * 64},
        built_at=OBSERVED,
    )
    for path in paths:
        path.unlink()
    with ServingReader(root).acquire_generation() as lease:
        recovered = read_backfill_plan_detail_from_serving(
            lease.connection, old_plan.content_sha256
        )
        assert read_backfill_plan_detail_from_serving(lease.connection, "f" * 64) is None
    assert recovered is not None
    assert recovered.plan_hash == old_plan.content_sha256
    assert recovered.missing_dates == old_plan.missing_dates
    assert recovered.monthly == old_plan.monthly
    assert recovered.estimate == old_plan.estimate
    assert recovered.source == old_plan.source


def test_more_than_256_plans_have_no_unreachable_catalogued_tail(tmp_path: Path) -> None:
    directory = tmp_path / "plans"
    template = _publish(_snapshot(tmp_path), directory)
    body = json.loads(template.read_text())
    template.unlink()
    first_hash = ""
    for index in range(257):
        candidate = json.loads(json.dumps(body))
        candidate["evidence"]["code_revision"] = f"revision-{index:03}"
        sealed = _rehash(candidate)
        if index == 0:
            first_hash = str(sealed["content_sha256"])
        name = f"daily-bar-backfill-plan-v1-{sealed['content_sha256']}.json"
        (directory / name).write_text(
            json.dumps(sealed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        )

    source = _source(tmp_path, directory)
    snapshot = source(OBSERVED)
    rows = {item.table_name: item.rows for item in snapshot.projections}
    assert rows["backfill_plan_catalog"][0]["total_plan_count"] == 257
    assert rows["backfill_plan_catalog"][0]["has_older_plans"] is False
    assert len(rows["backfill_plan_index"]) == 257
    assert len(rows["backfill_plan_archive"]) == 257
    assert first_hash in {row["plan_hash"] for row in rows["backfill_plan_archive"]}
    tables = build_serving_read_models(
        ServingReadModelInput(
            observed_at=OBSERVED,
            projections=tuple(
                ServingProjectionInput.bind(
                    item, owner_dataset_id="lab_jobs", owner_generation_id="a" * 64
                )
                for item in snapshot.projections
            ),
        )
    )
    assert len(tables["backfill_plan_index"]) == 257
    assert len(tables["backfill_plan_archive"]) == 257
    root = tmp_path / "serving"
    ServingPublisher(root, producer_commit="b" * 40, table_specs=SERVING_TABLE_SPECS).publish(
        tables,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="lab_jobs",
                generation_id="a" * 64,
                event_time=OBSERVED,
                published_at=OBSERVED,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"lab_jobs": "a" * 64},
        built_at=OBSERVED,
    )
    for path in directory.iterdir():
        path.unlink()
    with ServingReader(root).acquire_generation() as lease:
        from rquant.backfill_plan_projection import read_backfill_plan_detail_from_serving

        old = read_backfill_plan_detail_from_serving(lease.connection, first_hash)
    assert old is not None
    assert old.plan_hash == first_hash


def test_more_than_preview_window_has_every_plan_in_same_generation(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    paths = [
        _publish(snapshot, directory, evidence_code_revision=f"rev-{index:02}")
        for index in range(3)
    ]
    oldest = load_daily_bar_backfill_plan(paths[0])
    newest = load_daily_bar_backfill_plan(paths[-1])
    source = _source(tmp_path, directory)
    first = _rows(source)
    catalog = first["backfill_plan_catalog"][0]
    assert catalog["total_plan_count"] == 3
    assert catalog["indexed_plan_count"] == 3
    assert catalog["has_older_plans"] is False
    assert first["backfill_plan_index"][0]["plan_hash"] == newest.content_sha256
    assert {row["plan_hash"] for row in first["backfill_plan_archive"]} == {
        row["plan_hash"] for row in first["backfill_plan_index"]
    }
    assert oldest.content_sha256 in {row["plan_hash"] for row in first["backfill_plan_archive"]}


def test_directory_capacity_is_explicit_and_not_silently_truncated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import serving_page_projection_source as page_source

    monkeypatch.setattr(page_source, "MAX_DISCOVERABLE_BACKFILL_PLANS", 2)
    snapshot = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    for index in range(3):
        _publish(snapshot, directory, evidence_code_revision=f"rev-{index:02}")
    source = _source(tmp_path, directory)

    with pytest.raises(PageProjectionSourceIntegrityError, match="bound"):
        source(OBSERVED)


def test_read_byte_budget_refuses_partial_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import serving_page_projection_source as page_source

    database = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    _publish(database, directory, evidence_code_revision="rev-a")
    second = _publish(database, directory, evidence_code_revision="rev-b")
    monkeypatch.setattr(page_source, "MAX_BACKFILL_CATALOG_READ_BYTES", second.stat().st_size)
    source = _source(tmp_path, directory)

    with pytest.raises(PageProjectionSourceIntegrityError, match="bound"):
        source(OBSERVED)


def test_archive_byte_budget_refuses_partial_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import backfill_plan_projection as projection

    directory = tmp_path / "plans"
    _publish(_snapshot(tmp_path), directory)
    monkeypatch.setattr(projection, "MAX_BACKFILL_ARCHIVE_BYTES", 1)

    with pytest.raises(PageProjectionSourceIntegrityError, match="archive|bound"):
        _source(tmp_path, directory)(OBSERVED)


def test_old_plan_tampering_refuses_a_partial_new_generation(tmp_path: Path) -> None:
    from rquant.backfill_plan_projection import MAX_PREVIEW_BACKFILL_PLANS

    snapshot = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    paths = [
        _publish(snapshot, directory, evidence_code_revision=f"rev-{index:02}")
        for index in range(MAX_PREVIEW_BACKFILL_PLANS + 1)
    ]
    source = _source(tmp_path, directory)
    assert len(_rows(source)["backfill_plan_archive"]) == len(paths)
    paths[0].chmod(0o600)
    paths[0].write_bytes(paths[0].read_bytes().replace(b"unverified", b"verified", 1))

    with pytest.raises(PageProjectionSourceIntegrityError, match="invalid|digest|canonical"):
        source(OBSERVED)


def test_backdated_file_time_cannot_publish_plan_into_past(tmp_path: Path) -> None:
    path = _publish(_snapshot(tmp_path), tmp_path / "plans")
    os.utime(path, (datetime(2026, 9, 15, tzinfo=UTC).timestamp(),) * 2)
    assert path.stat().st_ctime > datetime(2026, 9, 20, tzinfo=UTC).timestamp()
    with pytest.raises(PageProjectionSourceIntegrityError, match="available|time|past"):
        _source(tmp_path, path.parent)(datetime(2026, 9, 20, tzinfo=UTC))
