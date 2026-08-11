from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import rquant.lab_artifact_preview as artifact_preview
from rquant.lab_artifact_preview import (
    ArtifactPreviewIntegrityError,
    ArtifactPreviewReader,
    ArtifactPreviewUnavailableError,
)
from rquant.lab_jobs import LabJobReader, LabJobStore

from .test_lab_finalizer import _ready_scenario
from .test_lab_jobs import _lease, _submit_job


def _fd_count() -> int:
    return len(tuple(Path("/dev/fd").iterdir()))


def _non_sqlite_control_entries(root: Path, database: Path) -> tuple[Path, ...]:
    sqlite_control_paths = {
        database.with_name(f"{database.name}-shm"),
        database.with_name(f"{database.name}-wal"),
    }
    return tuple(
        sorted(
            path.relative_to(root) for path in root.rglob("*") if path not in sqlite_control_paths
        )
    )


def _preview_parquet_rows(
    tmp_path: Path,
    table: pa.Table,
    *,
    row_group_size: int | None = None,
    **limits: int,
):  # type: ignore[no-untyped-def]
    path = tmp_path / "preview.parquet"
    pq.write_table(
        table,
        path,
        compression="zstd",
        row_group_size=row_group_size,
    )
    reader = ArtifactPreviewReader(
        reader=LabJobReader(tmp_path / "unused.sqlite3"),
        artifact_root=tmp_path,
        **limits,
    )
    descriptor = os.open(path, os.O_RDONLY)
    try:
        return reader._read_parquet_preview_rows(
            descriptor,
            relative_path="tables/preview.parquet",
            expected_rows=table.num_rows,
            expected_columns=tuple(table.column_names),
            selected_columns=tuple(table.column_names),
            row_limit=table.num_rows,
        )
    finally:
        os.close(descriptor)


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
    root_entries_before = _non_sqlite_control_entries(tmp_path, scenario.store.path)
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
    assert _non_sqlite_control_entries(tmp_path, scenario.store.path) == root_entries_before


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


def test_preview_rejects_zstd_one_cell_and_row_group_uncompressed_budgets(
    tmp_path: Path,
) -> None:
    before = _fd_count()
    with pytest.raises(ArtifactPreviewIntegrityError, match="uncompressed"):
        _preview_parquet_rows(
            tmp_path,
            pa.table({"payload": ["x" * (32 * 1024 * 1024)]}),
            max_parquet_uncompressed_bytes=8 * 1024 * 1024,
        )
    assert _fd_count() == before

    with pytest.raises(ArtifactPreviewIntegrityError, match="uncompressed"):
        _preview_parquet_rows(
            tmp_path,
            pa.table({"payload": ["a" * 700_000, "b" * 700_000]}),
            row_group_size=1,
            max_parquet_uncompressed_bytes=1_000_000,
        )


def test_preview_enforces_arrow_budget_when_parquet_metadata_underreports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "preview.parquet"
    table = pa.table({"payload": ["x" * (2 * 1024 * 1024)]})
    pq.write_table(table, path, compression="zstd")
    real_parquet_file = pq.ParquetFile

    class UnderreportedMetadata:
        num_rows = table.num_rows
        num_row_groups = 1

        @staticmethod
        def row_group(_index: int):  # type: ignore[no-untyped-def]
            return type("RowGroup", (), {"total_byte_size": 1})()

    class UnderreportedParquetFile:
        def __init__(self, stream):  # type: ignore[no-untyped-def]
            self._delegate = real_parquet_file(stream)
            self.metadata = UnderreportedMetadata()
            self.schema_arrow = self._delegate.schema_arrow

        def iter_batches(self, **kwargs):  # type: ignore[no-untyped-def]
            return self._delegate.iter_batches(**kwargs)

    monkeypatch.setattr(artifact_preview.pq, "ParquetFile", UnderreportedParquetFile)
    reader = ArtifactPreviewReader(
        reader=LabJobReader(tmp_path / "unused.sqlite3"),
        artifact_root=tmp_path,
        max_parquet_uncompressed_bytes=8 * 1024 * 1024,
        max_preview_arrow_bytes=1024 * 1024,
    )
    descriptor = os.open(path, os.O_RDONLY)
    before = _fd_count()
    try:
        with pytest.raises(ArtifactPreviewIntegrityError, match="materialized"):
            reader._read_parquet_preview_rows(
                descriptor,
                relative_path="tables/preview.parquet",
                expected_rows=1,
                expected_columns=("payload",),
                selected_columns=("payload",),
                row_limit=1,
            )
        os.fstat(descriptor)
        assert _fd_count() == before
    finally:
        os.close(descriptor)


@pytest.mark.parametrize(
    ("array", "error"),
    [
        (pa.array([[1, 2]], type=pa.list_(pa.int64())), "unsupported"),
        (pa.array(["x" * 2048], type=pa.large_string()), "cell"),
        (pa.array([b"x" * 2048], type=pa.large_binary()), "cell"),
    ],
)
def test_preview_rejects_nested_and_oversized_variable_cells(
    tmp_path: Path,
    array: pa.Array,
    error: str,
) -> None:
    with pytest.raises(ArtifactPreviewIntegrityError, match=error):
        _preview_parquet_rows(
            tmp_path,
            pa.table({"payload": array}),
            max_preview_cell_bytes=1024,
        )


def test_preview_enforces_cumulative_serialized_output_budget(tmp_path: Path) -> None:
    with pytest.raises(ArtifactPreviewIntegrityError, match="serialized"):
        _preview_parquet_rows(
            tmp_path,
            pa.table({"payload": [f"value-{index:02d}-xxxxxxxx" for index in range(20)]}),
            max_preview_serialized_bytes=128,
        )
