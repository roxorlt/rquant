"""Freeze and verify explicitly named research partitions without broad lake scans."""

from __future__ import annotations

import hashlib
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import DatasetSnapshotArtifact
from rquant.factor.result_artifact import _root_path
from rquant.factor.source_prepare import FactorSourceFileIdentity, _identity
from rquant.research_lake import _quoted_literal
from rquant.research_snapshot import materialize_table_dependency, verify_snapshot_artifact
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_dependencies import StrategyTableDependency

_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True, revalidate_instances="always")
_SHA = r"^[0-9a-f]{64}$"


class NamedLakeFileEvidence(BaseModel):
    model_config = _MODEL
    path: Path
    sha256: str = Field(pattern=_SHA)
    byte_count: int = Field(gt=0, le=256 * 1024 * 1024)

    @model_validator(mode="after")
    def _path(self) -> NamedLakeFileEvidence:
        if _root_path(self.path) != self.path:
            raise ValueError("named lake evidence path must be canonical and absolute")
        return self


class NamedLakeFrozenFile(NamedLakeFileEvidence):
    identity: FactorSourceFileIdentity

    @model_validator(mode="after")
    def _size(self) -> NamedLakeFrozenFile:
        if self.identity.size != self.byte_count:
            raise ValueError("named lake frozen identity differs from byte count")
        return self


class NamedLakeInput(BaseModel):
    model_config = _MODEL
    lake_root: Path
    catalog_file: NamedLakeFileEvidence
    current_marker: NamedLakeFileEvidence
    candidate_marker: NamedLakeFileEvidence
    catalog_role: Literal["catalog", "readonly_catalog"] = "readonly_catalog"
    artifacts: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=1, max_length=4096)
    manifest_sha256: str = Field(pattern=_SHA)

    @model_validator(mode="after")
    def _named(self) -> NamedLakeInput:
        keys = tuple(a.partition_id for a in self.artifacts)
        if (
            _root_path(self.lake_root) != self.lake_root
            or keys != tuple(sorted(set(keys)))
            or any(a.artifact_type != "lake_partition" for a in self.artifacts)
            or self.manifest_sha256 != canonical_sha256(self.artifacts)
        ):
            raise ValueError("named lake requires unique ordered partition evidence")
        return self


class NamedLakeReceipt(BaseModel):
    model_config = _MODEL
    snapshot_id: str = Field(min_length=1, max_length=128)
    observation_id: str = Field(min_length=1, max_length=128)
    catalog_role: Literal["catalog", "readonly_catalog"]
    catalog_file: NamedLakeFrozenFile
    current_marker: NamedLakeFrozenFile
    candidate_marker: NamedLakeFrozenFile
    catalog_sha256: str = Field(pattern=_SHA)
    current_marker_sha256: str = Field(pattern=_SHA)
    candidate_marker_sha256: str = Field(pattern=_SHA)
    manifest_sha256: str = Field(pattern=_SHA)
    catalog_status: Literal["candidate", "degraded"]
    catalog_issues: tuple[str, ...] = Field(max_length=64)
    partition_count: int = Field(gt=0, le=4096)
    verification_scope: Literal[
        "named_partitions_current_observation_not_full_authority_chain_or_pit"
    ] = "named_partitions_current_observation_not_full_authority_chain_or_pit"
    artifact: DatasetSnapshotArtifact

    @model_validator(mode="after")
    def _binding(self) -> NamedLakeReceipt:
        if (self.catalog_sha256, self.current_marker_sha256, self.candidate_marker_sha256) != (
            self.catalog_file.sha256,
            self.current_marker.sha256,
            self.candidate_marker.sha256,
        ) or self.artifact.row_count != self.partition_count:
            raise ValueError("named lake receipt differs from frozen files or manifest")
        return self


def _capture_file(evidence: NamedLakeFileEvidence, target: Path, limit: int) -> NamedLakeFrozenFile:
    fd = os.open(evidence.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size != evidence.byte_count
            or before.st_size > limit
        ):
            raise ValueError("named lake evidence exceeds frozen byte bound")
        digest, size = hashlib.sha256(), 0
        with target.open("xb") as output:
            while chunk := os.read(fd, 1024 * 1024):
                size += len(chunk)
                if size > evidence.byte_count:
                    raise ValueError("named lake evidence grew beyond frozen byte bound")
                digest.update(chunk)
                output.write(chunk)
        if (
            digest.hexdigest() != evidence.sha256
            or _identity(before) != _identity(os.fstat(fd))
            or _identity(before) != _identity(evidence.path.stat(follow_symlinks=False))
        ):
            raise ValueError("named lake evidence identity or hash changed")
        return NamedLakeFrozenFile(
            **evidence.model_dump(),
            identity=FactorSourceFileIdentity(
                device=before.st_dev,
                inode=before.st_ino,
                size=before.st_size,
                mtime_ns=before.st_mtime_ns,
                ctime_ns=before.st_ctime_ns,
            ),
        )
    finally:
        os.close(fd)


def materialize_named_lake(
    source: NamedLakeInput,
    connection: duckdb.DuckDBPyConnection,
    scratch: Path,
    *,
    output_root: Path,
    as_of: datetime,
    dataset_id: str,
    table_name: str,
) -> NamedLakeReceipt:
    from rquant.research_catalog import ResearchPartitionRecord
    from rquant.research_ingest import _parse_research_observation
    from rquant.research_migration import ResearchAuthorityCandidate
    from rquant.research_snapshot import _manifest_from_record

    catalog = scratch / "named-catalog.duckdb"
    catalog_file = _capture_file(source.catalog_file, catalog, 256 * 1024 * 1024)
    current_file = _capture_file(
        source.current_marker, scratch / "named-current.json", 2 * 1024 * 1024
    )
    candidate_file = _capture_file(
        source.candidate_marker, scratch / "named-candidate.json", 2 * 1024 * 1024
    )
    current = _parse_research_observation((scratch / "named-current.json").read_bytes())
    candidate = ResearchAuthorityCandidate.model_validate_json(
        (scratch / "named-candidate.json").read_bytes()
    )
    if (
        current.status not in ("candidate", "degraded")
        or current.bootstrap_snapshot_id != candidate.snapshot_id
        or getattr(current, source.catalog_role + "_sha256") != source.catalog_file.sha256
    ):
        raise ValueError("named lake current catalog hash or bootstrap lineage differs")
    if not table_name.isidentifier():
        raise ValueError("invalid internal manifest table")
    connection.execute("ATTACH " + _quoted_literal(str(catalog)) + " AS named_catalog (READ_ONLY)")
    try:
        connection.execute(
            f"CREATE TEMP TABLE {table_name}(partition_id VARCHAR PRIMARY KEY,"
            "trade_date DATE,artifact_json VARCHAR)"
        )
        for artifact in source.artifacts:
            rows = connection.execute(
                "SELECT * FROM named_catalog.research_partition WHERE partition_id=?",
                [artifact.partition_id],
            ).fetchmany(2)
            if len(rows) != 1:
                raise ValueError("named partition is absent or repeated in frozen catalog")
            names = tuple(c[0] for c in connection.description)
            record = ResearchPartitionRecord.model_validate(dict(zip(names, rows[0], strict=True)))
            manifest = _manifest_from_record(record)
            if (
                any(
                    getattr(artifact, k) != getattr(manifest, k)
                    for k in (
                        "relative_path",
                        "row_count",
                        "schema_hash",
                        "content_hash",
                        "file_hash",
                        "file_size",
                        "source",
                        "primary_key",
                    )
                )
                or artifact.revision_created_at != manifest.created_at
                or artifact.catalog_updated_at != record.updated_at
            ):
                raise ValueError("named artifact differs from frozen catalog manifest")
            verify_snapshot_artifact(artifact, lake_root=source.lake_root, as_of_time=as_of)
            connection.execute(
                f"INSERT INTO {table_name} VALUES(?,?,?)",
                [artifact.partition_id, manifest.partition.trade_date, artifact.model_dump_json()],
            )
        first, last = connection.execute(
            f"SELECT min(trade_date),max(trade_date) FROM {table_name}"
        ).fetchone()
        exported = materialize_table_dependency(
            connection,
            dependency=StrategyTableDependency(
                dataset_id=dataset_id, table_name=table_name, date_column="trade_date"
            ),
            artifact_root=output_root,
            start_date=first,
            end_date=last,
            as_of_time=as_of,
        )
        return NamedLakeReceipt(
            snapshot_id=candidate.snapshot_id,
            observation_id=current.observation_id,
            catalog_role=source.catalog_role,
            catalog_file=catalog_file,
            current_marker=current_file,
            candidate_marker=candidate_file,
            catalog_sha256=catalog_file.sha256,
            current_marker_sha256=current_file.sha256,
            candidate_marker_sha256=candidate_file.sha256,
            manifest_sha256=source.manifest_sha256,
            catalog_status=current.status,
            catalog_issues=current.issues,
            partition_count=len(source.artifacts),
            artifact=exported,
        )
    finally:
        connection.execute("DETACH named_catalog")
