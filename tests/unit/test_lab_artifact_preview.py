from __future__ import annotations

import os
from pathlib import Path

import pytest

from rquant.lab_artifact_preview import (
    ArtifactPreviewIntegrityError,
    ArtifactPreviewReader,
    ArtifactPreviewUnavailableError,
)
from rquant.lab_jobs import LabJobReader, LabJobStore

from .test_lab_finalizer import _ready_scenario
from .test_lab_jobs import _lease, _submit_job


def _sealed_scenario(tmp_path: Path):  # type: ignore[no-untyped-def]
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    assert scenario.finalizer().finalize(scenario.job_id).status == "published"
    assert scenario.scheduler.run_once().artifact_commits_accepted == 1
    return scenario


def test_preview_reads_only_verified_sealed_report_metrics_and_bounded_parquet(
    tmp_path: Path,
) -> None:
    scenario = _sealed_scenario(tmp_path)
    database_before = scenario.store.path.read_bytes()
    root_entries_before = tuple(sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")))
    preview = ArtifactPreviewReader(
        reader=LabJobReader(scenario.store.path),
        artifact_root=tmp_path / "job-artifacts",
    ).preview(
        scenario.job_id,
        row_limit=1,
        column_limit=1,
    )

    assert preview.job_id == scenario.job_id
    assert preview.report_markdown
    assert isinstance(preview.metrics, dict)
    assert preview.available_tables == ("trades",)
    assert preview.table is not None
    assert len(preview.table.columns) <= 1
    assert len(preview.table.rows) <= 1
    assert scenario.store.path.read_bytes() == database_before
    assert tuple(sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))) == (
        root_entries_before
    )


def test_preview_rejects_non_succeeded_or_unsealed_job_before_filesystem_access(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job = _submit_job(store, _lease(store))
    missing_root = tmp_path / "must-not-be-created"

    with pytest.raises(ArtifactPreviewUnavailableError, match="succeeded.*sealed"):
        ArtifactPreviewReader(
            reader=LabJobReader(store.path),
            artifact_root=missing_root,
        ).preview(job.job_id)

    assert not missing_root.exists()


def test_preview_rejects_corruption_and_unsafe_permissions(tmp_path: Path) -> None:
    scenario = _sealed_scenario(tmp_path)
    evidence = LabJobReader(scenario.store.path).get_result_artifact(scenario.job_id)
    assert evidence is not None
    scenario.artifact_store.close()
    report_path = evidence.sealed_path / "report.md"
    os.chmod(report_path, 0o600)
    report_path.write_bytes(report_path.read_bytes() + b"corrupt")
    os.chmod(report_path, 0o400)

    reader = ArtifactPreviewReader(
        reader=LabJobReader(scenario.store.path),
        artifact_root=tmp_path / "job-artifacts",
    )
    with pytest.raises(ArtifactPreviewIntegrityError, match="identity|hash|size"):
        reader.preview(scenario.job_id)


def test_preview_enforces_row_column_and_bundle_size_limits(tmp_path: Path) -> None:
    scenario = _sealed_scenario(tmp_path)
    reader = ArtifactPreviewReader(
        reader=LabJobReader(scenario.store.path),
        artifact_root=tmp_path / "job-artifacts",
        max_bundle_bytes=32,
    )

    with pytest.raises(ValueError, match="row_limit"):
        reader.preview(scenario.job_id, row_limit=0)
    with pytest.raises(ValueError, match="column_limit"):
        reader.preview(scenario.job_id, column_limit=0)
    with pytest.raises(ArtifactPreviewIntegrityError, match="size limit"):
        reader.preview(scenario.job_id)
