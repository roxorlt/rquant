from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from rquant.serving_contracts import (
    FreshnessStatus,
    ServingDatasetWatermark,
)
from rquant.serving_publisher import (
    ServingIntegrityError,
    ServingPublisher,
    ServingReader,
    ServingTableSpec,
)

NOW = datetime(2026, 7, 31, 8, 30, tzinfo=UTC)


def _published_root(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "serving"
    publisher = ServingPublisher(
        root,
        producer_commit="a" * 40,
        table_specs={"signals": ServingTableSpec(sort_keys=("sequence",))},
    )
    manifest = publisher.publish(
        {"signals": pd.DataFrame({"sequence": [1], "code": ["600000.SH"]})},
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="signal_bus",
                generation_id="b" * 64,
                event_time=NOW,
                published_at=NOW,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"signal_bus": "b" * 64},
        built_at=NOW,
    )
    return root, manifest.generation_id


def test_reader_has_no_filesystem_write_side_effects(tmp_path: Path) -> None:
    root, generation_id = _published_root(tmp_path)
    tracked = (
        root,
        root / "current.json",
        root / "generations",
        root / "generations" / generation_id,
        root / "generations" / generation_id / "manifest.json",
        root / "generations" / generation_id / "serving.duckdb",
    )
    before = {path: os.stat(path, follow_symlinks=False) for path in tracked}

    reader = ServingReader(root)
    assert reader.current_manifest().generation_id == generation_id
    with reader.open_current_readonly() as connection:
        assert connection.execute("SELECT code FROM signals").fetchone() == ("600000.SH",)

    after = {path: os.stat(path, follow_symlinks=False) for path in tracked}
    assert {
        path: (stat.st_mode, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size)
        for path, stat in after.items()
    } == {
        path: (stat.st_mode, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size)
        for path, stat in before.items()
    }


def test_reader_does_not_create_a_missing_serving_root(tmp_path: Path) -> None:
    root = tmp_path / "missing"

    with pytest.raises(ServingIntegrityError, match="root"):
        ServingReader(root)

    assert not root.exists()


def test_reader_rejects_symlinked_serving_root(tmp_path: Path) -> None:
    target, _ = _published_root(tmp_path)
    alias = tmp_path / "serving-alias"
    alias.symlink_to(target, target_is_directory=True)

    with pytest.raises(ServingIntegrityError, match="symlink"):
        ServingReader(alias)


def test_reader_detects_pointer_manifest_and_database_tamper(tmp_path: Path) -> None:
    root, generation_id = _published_root(tmp_path)
    pointer_path = root / "current.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = "f" * 64
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    with pytest.raises(ServingIntegrityError, match="manifest"):
        ServingReader(root).current_manifest()

    root, generation_id = _published_root(tmp_path / "manifest-case")
    manifest_path = root / "generations" / generation_id / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["row_counts"]["signals"] = 2
    manifest_path.chmod(0o600)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ServingIntegrityError, match="manifest"):
        ServingReader(root).current_manifest()

    root, generation_id = _published_root(tmp_path / "database-case")
    database_path = root / "generations" / generation_id / "serving.duckdb"
    database_path.chmod(0o600)
    with database_path.open("ab") as stream:
        stream.write(b"tamper")
    with pytest.raises(ServingIntegrityError, match="database content hash"):
        ServingReader(root).open_current_readonly()


def test_reader_connection_is_read_only(tmp_path: Path) -> None:
    root, _ = _published_root(tmp_path)

    with (
        ServingReader(root).open_current_readonly() as connection,
        pytest.raises(duckdb.InvalidInputException, match="read-only"),
    ):
        connection.execute("CREATE TABLE forbidden(value INTEGER)")
