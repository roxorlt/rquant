"""Content-bound generation metadata for read-only DuckDB replicas."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class _ReplicaGenerationModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ReplicaFileWatermark(_ReplicaGenerationModel):
    device: int = Field(ge=0)
    inode: int = Field(ge=0)
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)


class ReplicaDatabaseWatermark(_ReplicaGenerationModel):
    main: ReplicaFileWatermark
    wal: ReplicaFileWatermark | None = None


class ReplicaGenerationMetadata(_ReplicaGenerationModel):
    schema_version: Literal[1] = 1
    source_database: Path
    source_before: ReplicaDatabaseWatermark
    source_after: ReplicaDatabaseWatermark
    replica: ReplicaFileWatermark


def replica_generation_path(replica_path: Path) -> Path:
    return Path(f"{Path(replica_path)}.generation.json")


def capture_file_watermark(path: Path) -> ReplicaFileWatermark:
    stat = Path(path).stat()
    return ReplicaFileWatermark(
        device=stat.st_dev,
        inode=stat.st_ino,
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
    )


def capture_database_watermark(path: Path) -> ReplicaDatabaseWatermark:
    path = Path(path)
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"primary database is invalid: {path}")
    wal_path = Path(f"{path}.wal")
    wal_watermark = None
    if wal_path.exists():
        if not wal_path.is_file() or wal_path.is_symlink():
            raise ValueError(f"primary WAL is invalid: {wal_path}")
        wal_watermark = capture_file_watermark(wal_path)
    return ReplicaDatabaseWatermark(
        main=capture_file_watermark(path),
        wal=wal_watermark,
    )


def write_replica_generation_metadata(
    *,
    primary_path: Path,
    replica_path: Path,
    output_path: Path,
    source_before: ReplicaDatabaseWatermark,
) -> ReplicaGenerationMetadata:
    metadata = ReplicaGenerationMetadata(
        source_database=Path(primary_path).resolve(),
        source_before=source_before,
        source_after=capture_database_watermark(primary_path),
        replica=capture_file_watermark(replica_path),
    )
    payload = json.dumps(
        metadata.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    with Path(output_path).open("w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    return metadata


def validate_replica_generation(
    *,
    primary_path: Path,
    replica_path: Path,
    expected_primary_watermark: ReplicaDatabaseWatermark | None = None,
) -> ReplicaDatabaseWatermark:
    primary_path = Path(primary_path)
    replica_path = Path(replica_path)
    before = capture_database_watermark(primary_path)
    if expected_primary_watermark is not None and before != expected_primary_watermark:
        raise ValueError("primary database changed while planning")
    if not replica_path.is_file() or replica_path.is_symlink():
        raise ValueError(f"source database is invalid: {replica_path}")
    if replica_path.samefile(primary_path):
        raise ValueError("source must be a read-only replica")

    replica_watermark = capture_file_watermark(replica_path)
    metadata_path = replica_generation_path(replica_path)
    if not metadata_path.is_file() or metadata_path.is_symlink():
        raise ValueError("replica generation metadata is missing")
    try:
        metadata = ReplicaGenerationMetadata.model_validate_json(
            metadata_path.read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise ValueError("replica generation metadata is invalid") from exc

    after = capture_database_watermark(primary_path)
    if after != before:
        raise ValueError("primary database changed while planning")
    if metadata.source_database.resolve() != primary_path.resolve():
        raise ValueError("replica generation metadata names another primary")
    if metadata.source_before != metadata.source_after:
        raise ValueError("replica generation metadata captured a changing primary")
    if metadata.source_after != before or metadata.replica != replica_watermark:
        raise ValueError("replica generation metadata does not match current files")

    primary_mtime_ns = before.main.mtime_ns
    if before.wal is not None:
        primary_mtime_ns = max(primary_mtime_ns, before.wal.mtime_ns)
    if replica_watermark.mtime_ns < primary_mtime_ns:
        raise ValueError(
            "read-only replica is stale; refresh it after stopping writers "
            "before planning the repair"
        )
    return after
