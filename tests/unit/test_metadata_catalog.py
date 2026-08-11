from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pytest

from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog


def _catalog(path: Path) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE metadata(key VARCHAR, value VARCHAR)")
        connection.execute("INSERT INTO metadata VALUES ('version', 'one')")


def test_metadata_catalog_reads_descriptor_bound_immutable_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "catalog.duckdb"
    _catalog(path)

    with ImmutableDuckDBMetadataCatalog.open(
        path,
        forbidden_paths=(tmp_path / "main.duckdb",),
        snapshot_root=tmp_path / "snapshots",
    ) as catalog:
        assert catalog.connection.execute("SELECT * FROM metadata").fetchall() == [
            ("version", "one")
        ]
        assert catalog.descriptor.sha256
        snapshot_path = catalog.snapshot_path
        assert snapshot_path.is_file()

    assert not snapshot_path.exists()


def test_metadata_catalog_rejects_symlink_and_operational_database_alias(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.duckdb"
    _catalog(path)
    alias = tmp_path / "alias.duckdb"
    alias.symlink_to(path)

    with pytest.raises(OSError):
        ImmutableDuckDBMetadataCatalog.open(alias, snapshot_root=tmp_path / "snapshots")
    with pytest.raises(ValueError, match="operational database"):
        ImmutableDuckDBMetadataCatalog.open(
            path,
            forbidden_paths=(path,),
            snapshot_root=tmp_path / "snapshots",
        )


def test_metadata_catalog_descriptor_detects_source_mutation_during_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "catalog.duckdb"
    _catalog(path)
    original_fstat = os.fstat
    calls = 0

    def changed_fstat(descriptor: int) -> os.stat_result:
        nonlocal calls
        observed = original_fstat(descriptor)
        calls += 1
        if calls >= 3:
            values = list(observed)
            values[6] = observed.st_size + 1
            return os.stat_result(values)
        return observed

    monkeypatch.setattr("rquant.metadata_catalog.os.fstat", changed_fstat)

    with pytest.raises(RuntimeError, match="changed while sealing"):
        ImmutableDuckDBMetadataCatalog.open(
            path,
            snapshot_root=tmp_path / "snapshots",
        )
