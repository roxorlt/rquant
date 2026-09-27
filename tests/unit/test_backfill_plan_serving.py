"""A sealed proposal is readable without becoming an executable task."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.backfill_plan_artifact import load_daily_bar_backfill_plan
from rquant.backfill_plan_core import DailyBarBackfillPlan
from rquant.runtime_builder_authority import LabJobsPublisherSettings
from rquant.serving_page_projection_source import (
    DuckDBLabPageProjectionSource,
    LabPageProjectionSnapshot,
    PageProjectionSourceIntegrityError,
)
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_backfill_plan_artifact import _publish, _snapshot
from tests.unit.test_data_audit_report_serving import _production_report_file

OBSERVED = datetime(2026, 10, 1, 12, tzinfo=UTC)
TABLES = {
    "backfill_plan_catalog",
    "backfill_plan_index",
    "backfill_plan_preview",
    "backfill_plan_progress",
}


def _source(tmp_path: Path, directory: Path | None = None) -> DuckDBLabPageProjectionSource:
    database = tmp_path / "research_ro.duckdb"
    with DuckDBStore(database):
        pass
    return DuckDBLabPageProjectionSource(database, backfill_plan_directory=directory)


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
    assert configured["backfill_plan_catalog"][0]["total_plan_count"] == 0
    assert configured["backfill_plan_progress"][0]["availability"] == "unavailable"


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
        item for item in _source(tmp_path, directory)(OBSERVED).projections
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
                promoted if item.table_name == index.table_name else item
                for item in projections
            ),
        )
    progress = by_name["backfill_plan_progress"]
    invented = ServingProjectionPayload(
        table_name=progress.table_name,
        available_at=progress.available_at,
        rows=({"status_key": "current", "availability": "running", "task_id": "x"},),
    )
    with pytest.raises(ValueError, match="progress|unavailable"):
        LabPageProjectionSnapshot.create(
            available_at=OBSERVED,
            backfill_plan_projections=tuple(
                invented if item.table_name == progress.table_name else item
                for item in projections
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
    recovered = source.backfill_plan_by_hash(old_plan.content_sha256, observed_at=OBSERVED)
    assert isinstance(recovered, DailyBarBackfillPlan)
    assert recovered == old_plan


def test_index_window_pages_older_plans_without_hiding_newest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import backfill_plan_projection as projection
    from rquant import serving_page_projection_source as page_source

    monkeypatch.setattr(projection, "MAX_INDEXED_BACKFILL_PLANS", 2)
    monkeypatch.setattr(page_source, "MAX_INDEXED_BACKFILL_PLANS", 2)
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
    assert catalog["indexed_plan_count"] == 2
    assert catalog["has_older_plans"] is True
    assert first["backfill_plan_index"][0]["plan_hash"] == newest.content_sha256
    second_page = {
        item.table_name: item.rows
        for item in source.backfill_plan_index_page(
            OBSERVED, cursor_hash=catalog["oldest_indexed_hash"], limit=2
        )
    }
    assert second_page["backfill_plan_index"][0]["plan_hash"] == oldest.content_sha256
    assert second_page["backfill_plan_catalog"][0]["has_older_plans"] is False
    assert source.backfill_plan_by_hash(oldest.content_sha256, observed_at=OBSERVED) == oldest


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


def test_read_byte_budget_keeps_latest_plan_and_exposes_older_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import serving_page_projection_source as page_source

    database = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    first = _publish(database, directory, evidence_code_revision="rev-a")
    second = _publish(database, directory, evidence_code_revision="rev-b")
    monkeypatch.setattr(page_source, "MAX_BACKFILL_INDEX_READ_BYTES", second.stat().st_size)
    source = _source(tmp_path, directory)

    first_page = _rows(source)
    catalog = first_page["backfill_plan_catalog"][0]
    assert catalog["total_plan_count"] == 2
    assert catalog["indexed_plan_count"] == 1
    assert catalog["has_older_plans"] is True
    assert first_page["backfill_plan_index"][0]["plan_hash"] == (
        load_daily_bar_backfill_plan(second).content_sha256
    )
    older = {
        item.table_name: item.rows
        for item in source.backfill_plan_index_page(
            OBSERVED, cursor_hash=catalog["oldest_indexed_hash"]
        )
    }
    assert older["backfill_plan_index"][0]["plan_hash"] == (
        load_daily_bar_backfill_plan(first).content_sha256
    )


def test_old_plan_tampering_is_rejected_when_reopened_by_hash(tmp_path: Path) -> None:
    from rquant.backfill_plan_projection import MAX_PREVIEW_BACKFILL_PLANS

    snapshot = _snapshot(tmp_path)
    directory = tmp_path / "plans"
    paths = [
        _publish(snapshot, directory, evidence_code_revision=f"rev-{index:02}")
        for index in range(MAX_PREVIEW_BACKFILL_PLANS + 1)
    ]
    old = load_daily_bar_backfill_plan(paths[0])
    source = _source(tmp_path, directory)
    assert source.backfill_plan_by_hash(old.content_sha256, observed_at=OBSERVED) == old
    paths[0].chmod(0o600)
    paths[0].write_bytes(paths[0].read_bytes().replace(b"unverified", b"verified", 1))

    with pytest.raises(PageProjectionSourceIntegrityError, match="invalid|digest|canonical"):
        source.backfill_plan_by_hash(old.content_sha256, observed_at=OBSERVED)


def test_backdated_file_time_cannot_publish_plan_into_past(tmp_path: Path) -> None:
    path = _publish(_snapshot(tmp_path), tmp_path / "plans")
    os.utime(path, (datetime(2026, 9, 15, tzinfo=UTC).timestamp(),) * 2)
    assert path.stat().st_ctime > datetime(2026, 9, 20, tzinfo=UTC).timestamp()
    with pytest.raises(PageProjectionSourceIntegrityError, match="available|time|past"):
        _source(tmp_path, path.parent)(datetime(2026, 9, 20, tzinfo=UTC))


def test_missing_hash_during_directory_rotation_is_not_a_stable_absence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import serving_page_projection_source as page_source

    directory = tmp_path / "plans"
    directory.mkdir()
    source = _source(tmp_path, directory)
    original_read = page_source._read_bound_optional_file

    def rotate_after_missing(*args: object, **kwargs: object) -> None:
        assert original_read(*args, **kwargs) is None
        os.replace(directory, tmp_path / "old-plans")
        directory.mkdir()

    monkeypatch.setattr(page_source, "_read_bound_optional_file", rotate_after_missing)
    with pytest.raises(PageProjectionSourceIntegrityError, match="rotated|changed"):
        source.backfill_plan_by_hash("0" * 64, observed_at=OBSERVED)
