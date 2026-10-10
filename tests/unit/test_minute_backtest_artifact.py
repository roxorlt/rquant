from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rquant.lab_artifact_preview import ArtifactCompleteTableBudget, ArtifactPreviewIntegrityError, ArtifactPreviewReader, ArtifactPreviewUnavailableError
from rquant.lab_jobs import LabJobReader
from rquant.lab_artifacts import LabArtifactFileIdentity
from rquant.minute_backtest_artifact import MinuteSealedReplayReader


def test_original_minute_reader_hook_defaults_keep_the_same_owners() -> None:
    from rquant.minute_backtest_artifact import MinuteSealedReplayResult
    from rquant.minute_backtest_formal_adapter import MinuteFormalParameters, MinuteFormalReplayResult
    from rquant.minute_backtest_producer import MinuteReplayCatalog

    assert MinuteSealedReplayReader._catalog_model() is MinuteReplayCatalog
    assert MinuteSealedReplayReader._parameter_model() is MinuteFormalParameters
    assert MinuteSealedReplayReader._formal_result_model() is MinuteFormalReplayResult
    assert MinuteSealedReplayReader._sealed_result_model() is MinuteSealedReplayResult


def test_complete_read_has_an_independent_bounded_backend_interface() -> None:
    budget = ArtifactCompleteTableBudget()
    assert (budget.max_table_count, budget.max_table_bytes, budget.max_total_bytes) == (
        8, 33_554_432, 62_128_104)
    assert callable(ArtifactPreviewReader.read_complete_tables)
    assert callable(MinuteSealedReplayReader.read)


@pytest.mark.parametrize("field,value", [("max_table_count", 9), ("max_table_bytes", 33_554_433),
    ("max_total_bytes", 62_128_105), ("max_table_bytes", 0), ("max_table_count", True)])
def test_complete_read_cannot_expand_the_original_result_budget(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        ArtifactCompleteTableBudget.model_validate({field: value})


def test_original_preview_row_and_column_boundaries(tmp_path: Path) -> None:
    reader = ArtifactPreviewReader(reader=LabJobReader(tmp_path / "absent.sqlite3"), artifact_root=tmp_path / "artifacts")
    assert (reader.max_preview_rows, reader.max_preview_columns, reader.max_preview_cell_bytes,
        reader.max_preview_serialized_bytes, reader.max_parquet_uncompressed_bytes) == (
            100, 40, 1_048_576, 2_097_152, 33_554_432)
    for fields in ({"row_limit": 101}, {"row_limit": 0}, {"column_limit": 41}, {"column_limit": 0}):
        with pytest.raises(ValueError):
            reader.preview(uuid4(), **fields)


def test_original_preview_decoder_remains_bounded(tmp_path: Path) -> None:
    table = pa.table({f"c{index}": list(range(150)) for index in range(41)})
    path = tmp_path / "table.parquet"
    pq.write_table(table, path)
    reader = ArtifactPreviewReader(reader=LabJobReader(tmp_path / "unused.sqlite3"), artifact_root=tmp_path / "artifacts")
    descriptor = os.open(path, os.O_RDONLY)
    try:
        rows = reader._read_parquet_preview_rows(descriptor, relative_path="tables/controlled.parquet", expected_rows=150,
            expected_columns=tuple(table.column_names), selected_columns=tuple(table.column_names[:40]), row_limit=100)
        assert len(rows) == 100 and all(len(row) == 40 for row in rows)
        assert rows[0][0] == 0 and rows[-1][-1] == 99
    finally:
        os.close(descriptor)


@pytest.mark.parametrize("kind", ["cell", "serialization", "uncompressed", "nonfinite", "nested", "row_count", "columns"])
def test_original_preview_failure_boundaries(tmp_path: Path, kind: str) -> None:
    values = {"cell": ["x" * 1_048_577], "serialization": ["x" * 1_000_000] * 3,
        "uncompressed": ["x" * 3000], "nonfinite": [float("inf")], "nested": [[1, 2]],
        "row_count": [1, 2], "columns": [1, 2]}[kind]
    table = pa.table({"value": values})
    path = tmp_path / "table.parquet"
    pq.write_table(table, path)
    options = {"max_parquet_uncompressed_bytes": 1024} if kind == "uncompressed" else {}
    reader = ArtifactPreviewReader(reader=LabJobReader(tmp_path / "unused.sqlite3"), artifact_root=tmp_path / "artifacts", **options)
    descriptor = os.open(path, os.O_RDONLY)
    try:
        with pytest.raises(ArtifactPreviewIntegrityError):
            reader._read_parquet_preview_rows(descriptor, relative_path="tables/controlled.parquet",
                expected_rows=len(values) + (1 if kind == "row_count" else 0),
                expected_columns=("different",) if kind == "columns" else ("value",),
                selected_columns=("value",), row_limit=20)
    finally:
        os.close(descriptor)


def test_complete_selection_is_exact_before_authority_lookup(tmp_path: Path) -> None:
    reader = ArtifactPreviewReader(reader=LabJobReader(tmp_path / "unused.sqlite3"), artifact_root=tmp_path / "artifacts")
    for names in ((), ("signals", "signals"), tuple(f"table{index}" for index in range(9))):
        with pytest.raises(ValueError):
            reader.read_complete_tables(uuid4(), table_names=names, budget=ArtifactCompleteTableBudget())


def test_complete_requires_a_real_sealed_job(tmp_path: Path) -> None:
    from rquant.lab_jobs import LabJobStore

    store = LabJobStore(tmp_path / "jobs.sqlite3")
    store.initialize()
    reader = ArtifactPreviewReader(reader=LabJobReader(store.path), artifact_root=tmp_path / "artifacts")
    with pytest.raises(ArtifactPreviewUnavailableError):
        reader.read_complete_tables(uuid4(), table_names=("signals",), budget=ArtifactCompleteTableBudget())


def test_minute_reader_cannot_mix_lab_authorities(tmp_path: Path) -> None:
    first = LabJobReader(tmp_path / "first.sqlite3")
    second = LabJobReader(tmp_path / "second.sqlite3")
    preview = ArtifactPreviewReader(reader=first, artifact_root=tmp_path / "artifacts")
    with pytest.raises(ValueError, match="one original Lab authority"):
        MinuteSealedReplayReader(reader=first, artifact_reader=preview, submission_facade=SimpleNamespace(reader=second), catalog=None)


@pytest.mark.parametrize("kind", ["regular", "mode", "symlink", "hardlink", "replacement"])
def test_original_bound_file_identity_rules(tmp_path: Path, kind: str) -> None:
    path = tmp_path / "controlled.parquet"
    path.write_bytes(b"controlled offline file identity")
    path.chmod(0o400)
    observed = path.stat()
    identity = LabArtifactFileIdentity(relative_path="tables/controlled.parquet", device=observed.st_dev,
        inode=observed.st_ino, size=observed.st_size, mtime_ns=observed.st_mtime_ns, ctime_ns=observed.st_ctime_ns)
    if kind == "mode":
        path.chmod(0o600)
    elif kind == "symlink":
        path.unlink()
        target = tmp_path / "controlled-target"
        target.write_bytes(b"controlled target")
        path.symlink_to(target)
    elif kind == "hardlink":
        os.link(path, tmp_path / "controlled-link")
    elif kind == "replacement":
        path.unlink()
        path.write_bytes(b"controlled replacement bytes")
        path.chmod(0o400)
    parent = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        if kind == "regular":
            descriptor = ArtifactPreviewReader._open_bound_file(parent, path.name, identity)
            assert os.fstat(descriptor).st_ino == identity.inode
            os.close(descriptor)
        else:
            with pytest.raises(ArtifactPreviewIntegrityError):
                ArtifactPreviewReader._open_bound_file(parent, path.name, identity)
    finally:
        os.close(parent)
