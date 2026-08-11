"""Single-writer runtime support for the Strategy Lab artifact catalog."""

from __future__ import annotations

import fcntl
import json
import os
import re
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Literal, Protocol, TypeVar
from urllib.parse import quote
from uuid import UUID

from pydantic import Field

from rquant.artifact_retention import (
    ArtifactReferenceStore,
    PrivateSqlitePathAuthority,
    execute_sqlite_setup_statement,
    verified_sqlite_connection_scope,
)
from rquant.lab_artifact_catalog import (
    LabArtifactCatalogIntegrityError,
    LabArtifactCatalogRegistrar,
    LabArtifactCatalogRunResult,
    LabArtifactDirectoryFrontier,
    LabArtifactDirectoryScanPage,
    LabArtifactDurableOwners,
    LabArtifactFrontierMissingError,
    TerminalOwnerReleaser,
)
from rquant.lab_worker import LabShardResultManifest
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_DISCOVERY_SCHEMA = """
CREATE TABLE IF NOT EXISTS artifact_catalog_discovery_metadata (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    revision INTEGER NOT NULL CHECK(revision >= 0),
    scan_generation INTEGER NOT NULL CHECK(scan_generation >= 0),
    last_scan_at TEXT
);
INSERT OR IGNORE INTO artifact_catalog_discovery_metadata(
    singleton, revision, scan_generation, last_scan_at
) VALUES (1, 0, 0, NULL);

CREATE TABLE IF NOT EXISTS artifact_catalog_discovery (
    discovery_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    relative_path TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK(status IN ('pending', 'completed')),
    discovered_at TEXT NOT NULL,
    completed_at TEXT,
    content_sha256 TEXT,
    CHECK (
        (status = 'pending' AND completed_at IS NULL AND content_sha256 IS NULL)
        OR
        (status = 'completed' AND completed_at IS NOT NULL AND content_sha256 IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS artifact_catalog_discovery_frontier (
    frontier_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_generation INTEGER NOT NULL CHECK(scan_generation >= 1),
    relative_directory TEXT NOT NULL,
    directory_kind TEXT NOT NULL CHECK(directory_kind IN ('jobs', 'shards', 'attempts')),
    directory_device INTEGER,
    directory_inode INTEGER,
    directory_offset INTEGER NOT NULL DEFAULT 0 CHECK(directory_offset >= 0),
    buffered_entry_names_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL CHECK(status IN ('active', 'completed')),
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision >= 0),
    UNIQUE(scan_generation, relative_directory),
    CHECK (
        (directory_device IS NULL AND directory_inode IS NULL)
        OR
        (directory_device IS NOT NULL AND directory_inode IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS artifact_catalog_active_frontier_round_robin_idx
ON artifact_catalog_discovery_frontier(scan_generation, frontier_sequence)
WHERE status = 'active';

CREATE INDEX IF NOT EXISTS artifact_catalog_pending_queue_idx
ON artifact_catalog_discovery(discovery_sequence)
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS artifact_catalog_completed_frontier_generation_idx
ON artifact_catalog_discovery_frontier(scan_generation)
WHERE status = 'completed';

CREATE TABLE IF NOT EXISTS artifact_catalog_discovery_frontier_failure (
    frontier_sequence INTEGER PRIMARY KEY,
    scan_generation INTEGER NOT NULL CHECK(scan_generation >= 1),
    relative_directory TEXT NOT NULL,
    failed_at TEXT NOT NULL,
    failure_reason TEXT NOT NULL
);
"""
_EvidenceT = TypeVar("_EvidenceT")


class LabArtifactCatalogAlreadyRunningError(RuntimeError):
    """Another registrar process already owns the catalog step."""


class LabArtifactDiscoveryConflictError(RuntimeError):
    """The durable discovery queue conflicts with a completed registration."""


class LabJobOwnerEvidence(RuntimeContractModel):
    job_id: UUID
    shard_id: UUID
    result_manifest_hash: str = Field(pattern=_HASH_PATTERN)
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    snapshot_id: str = Field(pattern=_HASH_PATTERN)
    snapshot_binding_hash: str = Field(pattern=_HASH_PATTERN)
    audit_run_id: str = Field(pattern=_HASH_PATTERN)
    experiment_id: str = Field(pattern=_HASH_PATTERN)
    experiment_attempt_identity: str = Field(pattern=_HASH_PATTERN)
    formal_plan_id: str = Field(pattern=_HASH_PATTERN)
    strategy_id: str = Field(min_length=1)
    strategy_version: int = Field(ge=1)
    strategy_execution_identity_hash: str = Field(pattern=_HASH_PATTERN)
    strategy_spec_fingerprint: str = Field(pattern=_HASH_PATTERN)
    strategy_definition_fingerprint: str = Field(pattern=_HASH_PATTERN)
    definition_registration_record_hash: str = Field(pattern=_HASH_PATTERN)
    definition_registered_at: AwareUtcDatetime
    definition_available_at: AwareUtcDatetime
    strategy_executable_fingerprint: str = Field(pattern=_HASH_PATTERN)
    candidate_schema_fingerprint: str = Field(pattern=_HASH_PATTERN)
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    recorded_at: AwareUtcDatetime


class DatasetSnapshotOwnerEvidence(RuntimeContractModel):
    snapshot_id: str = Field(pattern=_HASH_PATTERN)
    binding_hash: str = Field(pattern=_HASH_PATTERN)
    audit_run_id: str = Field(pattern=_HASH_PATTERN)
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    status: Literal["ready"]
    completed_at: AwareUtcDatetime
    binding_completed_at: AwareUtcDatetime
    audit_completed_at: AwareUtcDatetime


class ExperimentOwnerEvidence(RuntimeContractModel):
    experiment_id: str = Field(pattern=_HASH_PATTERN)
    formal_plan_id: str = Field(pattern=_HASH_PATTERN)
    dataset_snapshot_id: str = Field(pattern=_HASH_PATTERN)
    strategy_spec_fingerprint: str = Field(pattern=_HASH_PATTERN)
    strategy_definition_fingerprint: str = Field(pattern=_HASH_PATTERN)
    definition_registration_record_hash: str = Field(pattern=_HASH_PATTERN)
    strategy_executable_fingerprint: str = Field(pattern=_HASH_PATTERN)
    candidate_schema_fingerprint: str = Field(pattern=_HASH_PATTERN)
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    registered_at: AwareUtcDatetime


class DefinitionOwnerEvidence(RuntimeContractModel):
    strategy_id: str = Field(min_length=1)
    strategy_version: int = Field(ge=1)
    strategy_spec_fingerprint: str = Field(pattern=_HASH_PATTERN)
    strategy_definition_fingerprint: str = Field(pattern=_HASH_PATTERN)
    strategy_executable_fingerprint: str = Field(pattern=_HASH_PATTERN)
    candidate_schema_fingerprint: str = Field(pattern=_HASH_PATTERN)
    definition_registration_record_hash: str = Field(pattern=_HASH_PATTERN)
    producer_code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    registered_at: AwareUtcDatetime
    available_at: AwareUtcDatetime


class LabJobOwnerEvidenceReader(Protocol):
    def __call__(
        self,
        manifest: LabShardResultManifest,
        observed_at: datetime,
        /,
    ) -> tuple[LabJobOwnerEvidence, ...]: ...


class DatasetSnapshotOwnerEvidenceReader(Protocol):
    def __call__(
        self,
        job: LabJobOwnerEvidence,
        observed_at: datetime,
        /,
    ) -> tuple[DatasetSnapshotOwnerEvidence, ...]: ...


class ExperimentOwnerEvidenceReader(Protocol):
    def __call__(
        self,
        job: LabJobOwnerEvidence,
        snapshot: DatasetSnapshotOwnerEvidence,
        observed_at: datetime,
        /,
    ) -> tuple[ExperimentOwnerEvidence, ...]: ...


class DefinitionOwnerEvidenceReader(Protocol):
    def __call__(
        self,
        job: LabJobOwnerEvidence,
        experiment: ExperimentOwnerEvidence,
        observed_at: datetime,
        /,
    ) -> tuple[DefinitionOwnerEvidence, ...]: ...


class TrustedLabArtifactOwnerResolver:
    """Resolve durable owners only from three independently verified read models."""

    def __init__(
        self,
        *,
        job_evidence_reader: LabJobOwnerEvidenceReader,
        snapshot_evidence_reader: DatasetSnapshotOwnerEvidenceReader,
        experiment_evidence_reader: ExperimentOwnerEvidenceReader,
        definition_evidence_reader: DefinitionOwnerEvidenceReader,
        authority_rebinder: Callable[[], None] | None = None,
        terminal_owner_releaser_factory: (
            Callable[[ArtifactReferenceStore], TerminalOwnerReleaser] | None
        ) = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.job_evidence_reader = job_evidence_reader
        self.snapshot_evidence_reader = snapshot_evidence_reader
        self.experiment_evidence_reader = experiment_evidence_reader
        self.definition_evidence_reader = definition_evidence_reader
        self.authority_rebinder = authority_rebinder
        self.terminal_owner_releaser_factory = terminal_owner_releaser_factory
        self.clock = clock or (lambda: datetime.now(UTC))

    def build_terminal_owner_releaser(
        self,
        reference_store: ArtifactReferenceStore,
    ) -> TerminalOwnerReleaser | None:
        if self.terminal_owner_releaser_factory is None:
            return None
        return self.terminal_owner_releaser_factory(reference_store)

    def _rebind_shared_authorities(self) -> None:
        if self.authority_rebinder is not None:
            self.authority_rebinder()

    @contextmanager
    def batch(self) -> Iterator[None]:
        batch_factory = getattr(self.experiment_evidence_reader, "batch", None)
        if not callable(batch_factory):
            yield
            return
        with batch_factory():
            self._rebind_shared_authorities()
            yield

    def __call__(self, manifest: LabShardResultManifest) -> LabArtifactDurableOwners:
        observed_at = normalize_aware_utc(self.clock())
        self._rebind_shared_authorities()
        job = self._exactly_one(
            self.job_evidence_reader(manifest, observed_at),
            LabJobOwnerEvidence,
            label="lab job",
        )
        self._rebind_shared_authorities()
        if job.recorded_at > observed_at:
            raise LabArtifactCatalogIntegrityError("lab job owner evidence is from the future")
        if (
            job.job_id,
            job.shard_id,
            job.result_manifest_hash,
            job.spec_hash,
            job.plan_hash,
        ) != (
            manifest.job_id,
            manifest.shard_id,
            manifest.manifest_hash,
            manifest.spec_hash,
            manifest.plan_hash,
        ):
            raise LabArtifactCatalogIntegrityError(
                "lab job owner evidence conflicts with the sealed manifest"
            )

        snapshot = self._exactly_one(
            self.snapshot_evidence_reader(job, observed_at),
            DatasetSnapshotOwnerEvidence,
            label="dataset snapshot",
        )
        self._rebind_shared_authorities()
        if (
            max(
                snapshot.completed_at,
                snapshot.binding_completed_at,
                snapshot.audit_completed_at,
            )
            > observed_at
        ):
            raise LabArtifactCatalogIntegrityError(
                "dataset snapshot owner evidence is from the future"
            )
        if (
            snapshot.snapshot_id,
            snapshot.binding_hash,
            snapshot.audit_run_id,
            snapshot.code_commit,
        ) != (
            job.snapshot_id,
            job.snapshot_binding_hash,
            job.audit_run_id,
            job.code_commit,
        ):
            raise LabArtifactCatalogIntegrityError(
                "dataset snapshot owner evidence conflicts with the lab job"
            )

        experiment = self._exactly_one(
            self.experiment_evidence_reader(job, snapshot, observed_at),
            ExperimentOwnerEvidence,
            label="experiment",
        )
        self._rebind_shared_authorities()
        if experiment.registered_at > observed_at:
            raise LabArtifactCatalogIntegrityError("experiment owner evidence is from the future")
        if (
            experiment.experiment_id,
            experiment.formal_plan_id,
            experiment.dataset_snapshot_id,
            experiment.strategy_spec_fingerprint,
            experiment.strategy_definition_fingerprint,
            experiment.definition_registration_record_hash,
            experiment.strategy_executable_fingerprint,
            experiment.candidate_schema_fingerprint,
            experiment.code_commit,
        ) != (
            job.experiment_id,
            job.formal_plan_id,
            job.snapshot_id,
            job.strategy_spec_fingerprint,
            job.strategy_definition_fingerprint,
            job.definition_registration_record_hash,
            job.strategy_executable_fingerprint,
            job.candidate_schema_fingerprint,
            job.code_commit,
        ):
            raise LabArtifactCatalogIntegrityError(
                "experiment owner evidence conflicts with the lab job and snapshot"
            )
        definition = self._exactly_one(
            self.definition_evidence_reader(job, experiment, observed_at),
            DefinitionOwnerEvidence,
            label="strategy definition",
        )
        self._rebind_shared_authorities()
        if max(definition.registered_at, definition.available_at) > observed_at:
            raise LabArtifactCatalogIntegrityError(
                "strategy definition owner evidence is from the future"
            )
        if (
            definition.strategy_id,
            definition.strategy_version,
            definition.strategy_spec_fingerprint,
            definition.strategy_definition_fingerprint,
            definition.strategy_executable_fingerprint,
            definition.candidate_schema_fingerprint,
            definition.definition_registration_record_hash,
            definition.producer_code_commit,
            definition.registered_at,
            definition.available_at,
        ) != (
            job.strategy_id,
            job.strategy_version,
            job.strategy_spec_fingerprint,
            job.strategy_definition_fingerprint,
            job.strategy_executable_fingerprint,
            job.candidate_schema_fingerprint,
            job.definition_registration_record_hash,
            job.code_commit,
            job.definition_registered_at,
            job.definition_available_at,
        ):
            raise LabArtifactCatalogIntegrityError(
                "Definition Registry evidence conflicts with plan and lab job receipts"
            )
        return LabArtifactDurableOwners(
            job_id=job.job_id,
            spec_hash=job.spec_hash,
            plan_hash=job.plan_hash,
            snapshot_id=snapshot.snapshot_id,
            experiment_id=experiment.experiment_id,
            audit_run_id=snapshot.audit_run_id,
        )

    @staticmethod
    def _exactly_one(
        values: tuple[object, ...],
        expected_type: type[_EvidenceT],
        *,
        label: str,
    ) -> _EvidenceT:
        if not isinstance(values, tuple) or not values:
            raise LabArtifactCatalogIntegrityError(f"{label} owner evidence is missing")
        if len(values) != 1:
            raise LabArtifactCatalogIntegrityError(f"{label} owner evidence is ambiguous")
        value = values[0]
        if not isinstance(value, expected_type):
            raise LabArtifactCatalogIntegrityError(f"{label} owner evidence is invalid")
        return value


class LabArtifactDiscoveryState(RuntimeContractModel):
    revision: int = Field(ge=0)
    scan_generation: int = Field(ge=0)
    last_scan_at: AwareUtcDatetime | None = None


class LabArtifactDiscoveryResult(RuntimeContractModel):
    state: LabArtifactDiscoveryState
    discovered_bundles: int = Field(ge=0)
    scanned_directory_entries: int = Field(default=0, ge=0)
    scanned_directories: int = Field(default=0, ge=0)


class LabArtifactDiscoveryQueue:
    """Persistent discovery index; completed paths are never cursor-filtered away."""

    def __init__(
        self,
        path: Path,
        *,
        managed_trust_root: Path,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self._path_authority = PrivateSqlitePathAuthority(
            self.path,
            label="artifact discovery queue",
            create_if_missing=True,
            managed_trust_root=managed_trust_root,
        )
        with self._connect(read_only=False) as connection:
            try:
                connection.executescript(f"BEGIN IMMEDIATE;{_DISCOVERY_SCHEMA}")
                self._after_discovery_metadata_base_initialized(connection)
                columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(artifact_catalog_discovery_metadata)"
                    ).fetchall()
                }
                if "frontier_cursor" not in columns:
                    connection.execute(
                        """
                        ALTER TABLE artifact_catalog_discovery_metadata
                        ADD COLUMN frontier_cursor INTEGER NOT NULL DEFAULT 0
                        """
                    )
                if "frontier_round_ceiling" not in columns:
                    connection.execute(
                        """
                        ALTER TABLE artifact_catalog_discovery_metadata
                        ADD COLUMN frontier_round_ceiling INTEGER NOT NULL DEFAULT 0
                        """
                    )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    @staticmethod
    def _after_discovery_metadata_base_initialized(
        _connection: sqlite3.Connection,
    ) -> None:
        """Fault-injection boundary before additive metadata migrations."""

    @contextmanager
    def _connect(self, *, read_only: bool) -> Iterator[sqlite3.Connection]:
        def open_connection(path: Path) -> sqlite3.Connection:
            mode = "ro" if read_only else "rw"
            uri = f"file:{quote(str(path), safe='/')}?mode={mode}"
            return sqlite3.connect(
                uri,
                uri=True,
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
            )

        connection = self._path_authority.open_verified_connection(open_connection)
        with verified_sqlite_connection_scope(connection, self._path_authority):
            connection.row_factory = sqlite3.Row
            execute_sqlite_setup_statement(
                connection,
                f"PRAGMA busy_timeout = {self.busy_timeout_ms}",
            )
            execute_sqlite_setup_statement(connection, "PRAGMA foreign_keys = ON")
            if read_only:
                execute_sqlite_setup_statement(connection, "PRAGMA query_only = ON")
            else:
                execute_sqlite_setup_statement(connection, "PRAGMA journal_mode = WAL")
                execute_sqlite_setup_statement(connection, "PRAGMA synchronous = FULL")
            self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
            yield connection
            self._path_authority.rebind_ctime_after_trusted_sqlite_setup()

    def read_state(self) -> LabArtifactDiscoveryState:
        with self._connect(read_only=True) as connection:
            row = connection.execute(
                """
                SELECT revision, scan_generation, last_scan_at
                FROM artifact_catalog_discovery_metadata
                WHERE singleton = 1
                """
            ).fetchone()
        if row is None:
            raise LabArtifactCatalogIntegrityError("artifact discovery state is missing")
        return LabArtifactDiscoveryState(
            revision=int(row["revision"]),
            scan_generation=int(row["scan_generation"]),
            last_scan_at=(
                datetime.fromisoformat(str(row["last_scan_at"]))
                if row["last_scan_at"] is not None
                else None
            ),
        )

    def record_discovery(
        self,
        relative_paths: tuple[str, ...],
        *,
        discovered_at: datetime,
    ) -> LabArtifactDiscoveryResult:
        paths = tuple(sorted({_validated_discovery_path(path) for path in relative_paths}))
        timestamp = normalize_aware_utc(discovered_at).isoformat(timespec="microseconds")
        discovered = 0
        with self._connect(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                for path in paths:
                    result = connection.execute(
                        """
                        INSERT OR IGNORE INTO artifact_catalog_discovery(
                            relative_path, status, discovered_at
                        ) VALUES (?, 'pending', ?)
                        """,
                        (path, timestamp),
                    )
                    discovered += result.rowcount
                connection.execute(
                    """
                    UPDATE artifact_catalog_discovery_metadata
                    SET revision = revision + 1,
                        last_scan_at = ?
                    WHERE singleton = 1
                    """,
                    (timestamp,),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return LabArtifactDiscoveryResult(
            state=self.read_state(),
            discovered_bundles=discovered,
        )

    def scan_step(
        self,
        registrar: LabArtifactCatalogRegistrar,
        *,
        max_entries: int,
        max_directories: int,
        deadline: float,
        monotonic: Callable[[], float],
        discovered_at: datetime,
    ) -> LabArtifactDiscoveryResult:
        if isinstance(max_entries, bool) or max_entries < 1:
            raise ValueError("directory discovery budget must be positive")
        if isinstance(max_directories, bool) or max_directories < 1:
            raise ValueError("directory count budget must be positive")
        timestamp = normalize_aware_utc(discovered_at).isoformat(timespec="microseconds")
        remaining_entries = max_entries
        scanned_directories = 0
        scanned_entries = 0
        discovered_bundles = 0
        result: LabArtifactDiscoveryResult | None = None
        while remaining_entries > 0 and scanned_directories < max_directories:
            frontier = self._next_frontier(timestamp=timestamp)
            try:
                page = registrar.scan_directory_page(
                    frontier,
                    max_entries=remaining_entries,
                )
            except LabArtifactFrontierMissingError as exc:
                self._retire_missing_frontier(
                    frontier,
                    timestamp=timestamp,
                    failure_reason=str(exc),
                )
                raise
            if page.scanned_entries > remaining_entries:
                raise LabArtifactCatalogIntegrityError(
                    "artifact registrar exceeded the directory discovery budget"
                )
            result = self._apply_scan_page(page, timestamp=timestamp)
            scanned_directories += 1
            scanned_entries += page.scanned_entries
            discovered_bundles += result.discovered_bundles
            remaining_entries -= page.scanned_entries
            if not page.exhausted or monotonic() >= deadline:
                break
            if not self._has_active_frontier(frontier.scan_generation):
                break
        if result is None:
            raise LabArtifactCatalogIntegrityError("artifact discovery made no bounded progress")
        return LabArtifactDiscoveryResult(
            state=result.state,
            discovered_bundles=discovered_bundles,
            scanned_directory_entries=scanned_entries,
            scanned_directories=scanned_directories,
        )

    def _retire_missing_frontier(
        self,
        frontier: LabArtifactDirectoryFrontier,
        *,
        timestamp: str,
        failure_reason: str,
    ) -> None:
        with self._connect(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                updated = connection.execute(
                    """
                    UPDATE artifact_catalog_discovery_frontier
                    SET status = 'completed', revision = revision + 1
                    WHERE frontier_sequence = ? AND revision = ? AND status = 'active'
                    """,
                    (frontier.frontier_sequence, frontier.revision),
                )
                if updated.rowcount != 1:
                    raise LabArtifactDiscoveryConflictError(
                        "missing artifact frontier changed before retirement"
                    )
                connection.execute(
                    """
                    INSERT INTO artifact_catalog_discovery_frontier_failure(
                        frontier_sequence, scan_generation, relative_directory,
                        failed_at, failure_reason
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        frontier.frontier_sequence,
                        frontier.scan_generation,
                        frontier.relative_directory,
                        timestamp,
                        failure_reason,
                    ),
                )
                connection.execute(
                    """
                    UPDATE artifact_catalog_discovery_metadata
                    SET revision = revision + 1, last_scan_at = ?
                    WHERE singleton = 1
                    """,
                    (timestamp,),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _has_active_frontier(self, generation: int) -> bool:
        with self._connect(read_only=True) as connection:
            return (
                connection.execute(
                    """
                    SELECT 1
                    FROM artifact_catalog_discovery_frontier
                    WHERE scan_generation = ? AND status = 'active'
                    LIMIT 1
                    """,
                    (generation,),
                ).fetchone()
                is not None
            )

    def _next_frontier(self, *, timestamp: str) -> LabArtifactDirectoryFrontier:
        with self._connect(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                metadata = connection.execute(
                    """
                    SELECT scan_generation, frontier_cursor, frontier_round_ceiling
                    FROM artifact_catalog_discovery_metadata
                    WHERE singleton = 1
                    """
                ).fetchone()
                if metadata is None:
                    raise LabArtifactCatalogIntegrityError("artifact discovery metadata is missing")
                generation = int(metadata["scan_generation"])
                frontier_cursor = int(metadata["frontier_cursor"])
                round_ceiling = int(metadata["frontier_round_ceiling"])
                row = None
                if round_ceiling > 0:
                    row = connection.execute(
                        """
                        SELECT *
                        FROM artifact_catalog_discovery_frontier
                        WHERE scan_generation = ? AND status = 'active'
                          AND frontier_sequence > ?
                          AND frontier_sequence <= ?
                        ORDER BY frontier_sequence
                        LIMIT 1
                        """,
                        (generation, frontier_cursor, round_ceiling),
                    ).fetchone()
                if row is None:
                    active_maximum = connection.execute(
                        """
                        SELECT MAX(frontier_sequence)
                        FROM artifact_catalog_discovery_frontier
                        WHERE scan_generation = ? AND status = 'active'
                        """,
                        (generation,),
                    ).fetchone()[0]
                    if active_maximum is not None:
                        frontier_cursor = 0
                        round_ceiling = int(active_maximum)
                        row = connection.execute(
                            """
                            SELECT *
                            FROM artifact_catalog_discovery_frontier
                            WHERE scan_generation = ? AND status = 'active'
                              AND frontier_sequence <= ?
                            ORDER BY frontier_sequence
                            LIMIT 1
                            """,
                            (generation, round_ceiling),
                        ).fetchone()
                if row is None:
                    recovery_generation = generation
                    generation += 1
                    connection.execute(
                        """
                        DELETE FROM artifact_catalog_discovery_frontier
                        WHERE status = 'completed' AND scan_generation < ?
                        """,
                        (recovery_generation,),
                    )
                    connection.execute(
                        """
                        UPDATE artifact_catalog_discovery_metadata
                        SET revision = revision + 1,
                            scan_generation = ?,
                            frontier_cursor = 0,
                            frontier_round_ceiling = 0,
                            last_scan_at = ?
                        WHERE singleton = 1
                        """,
                        (generation, timestamp),
                    )
                    connection.execute(
                        """
                        INSERT INTO artifact_catalog_discovery_frontier(
                            scan_generation, relative_directory, directory_kind,
                            status
                        ) VALUES (?, 'jobs', 'jobs', 'active')
                        """,
                        (generation,),
                    )
                    row = connection.execute(
                        """
                        SELECT *
                        FROM artifact_catalog_discovery_frontier
                        WHERE scan_generation = ? AND relative_directory = 'jobs'
                        """,
                        (generation,),
                    ).fetchone()
                    if row is not None:
                        round_ceiling = int(row["frontier_sequence"])
                if row is not None:
                    connection.execute(
                        """
                        UPDATE artifact_catalog_discovery_metadata
                        SET frontier_cursor = ?, frontier_round_ceiling = ?
                        WHERE singleton = 1
                        """,
                        (int(row["frontier_sequence"]), round_ceiling),
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        if row is None:
            raise LabArtifactCatalogIntegrityError("artifact discovery frontier is missing")
        return _frontier_from_row(row)

    def _apply_scan_page(
        self,
        page: LabArtifactDirectoryScanPage,
        *,
        timestamp: str,
    ) -> LabArtifactDiscoveryResult:
        discovered = 0
        with self._connect(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM artifact_catalog_discovery_frontier
                    WHERE frontier_sequence = ?
                    """,
                    (page.frontier_sequence,),
                ).fetchone()
                if row is None or row["status"] != "active":
                    raise LabArtifactDiscoveryConflictError(
                        "artifact discovery frontier is no longer active"
                    )
                frontier = _frontier_from_row(row)
                if page.frontier_revision != frontier.revision:
                    raise LabArtifactDiscoveryConflictError(
                        "artifact discovery frontier revision changed"
                    )
                if (frontier.directory_device, frontier.directory_inode) not in {
                    (None, None),
                    (page.directory_device, page.directory_inode),
                }:
                    raise LabArtifactDiscoveryConflictError(
                        "artifact discovery directory identity changed"
                    )
                status = "completed" if page.exhausted else "active"
                connection.execute(
                    """
                    UPDATE artifact_catalog_discovery_frontier
                    SET directory_device = ?, directory_inode = ?,
                        directory_offset = ?, buffered_entry_names_json = ?,
                        status = ?, revision = revision + 1
                    WHERE frontier_sequence = ? AND revision = ? AND status = 'active'
                    """,
                    (
                        page.directory_device,
                        page.directory_inode,
                        page.directory_offset,
                        json.dumps(
                            page.buffered_entry_names,
                            ensure_ascii=True,
                            separators=(",", ":"),
                        ),
                        status,
                        frontier.frontier_sequence,
                        frontier.revision,
                    ),
                )
                for child in page.child_directories:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO artifact_catalog_discovery_frontier(
                            scan_generation, relative_directory, directory_kind,
                            status
                        ) VALUES (?, ?, ?, 'active')
                        """,
                        (
                            frontier.scan_generation,
                            child.relative_directory,
                            child.directory_kind,
                        ),
                    )
                for path in tuple(
                    sorted({_validated_discovery_path(value) for value in page.bundle_paths})
                ):
                    result = connection.execute(
                        """
                        INSERT OR IGNORE INTO artifact_catalog_discovery(
                            relative_path, status, discovered_at
                        ) VALUES (?, 'pending', ?)
                        """,
                        (path, timestamp),
                    )
                    discovered += result.rowcount
                connection.execute(
                    """
                    UPDATE artifact_catalog_discovery_metadata
                    SET revision = revision + 1, last_scan_at = ?
                    WHERE singleton = 1
                    """,
                    (timestamp,),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return LabArtifactDiscoveryResult(
            state=self.read_state(),
            discovered_bundles=discovered,
            scanned_directory_entries=page.scanned_entries,
            scanned_directories=1,
        )

    def list_pending(self, *, limit: int) -> tuple[str, ...]:
        if isinstance(limit, bool) or limit < 1:
            raise ValueError("pending discovery limit must be positive")
        with self._connect(read_only=True) as connection:
            rows = connection.execute(
                """
                SELECT relative_path
                FROM artifact_catalog_discovery
                WHERE status = 'pending'
                ORDER BY discovery_sequence
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(str(row["relative_path"]) for row in rows)

    def pending_count(self) -> int:
        with self._connect(read_only=True) as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS pending_count
                FROM artifact_catalog_discovery
                WHERE status = 'pending'
                """
            ).fetchone()
        assert row is not None
        return int(row["pending_count"])

    def mark_completed(
        self,
        *,
        relative_paths: tuple[str, ...],
        content_hashes: tuple[str, ...],
        completed_at: datetime,
    ) -> None:
        if len(relative_paths) != len(content_hashes):
            raise ValueError("completed paths and content hashes must have equal length")
        pairs = tuple(
            (_validated_discovery_path(path), content_hash)
            for path, content_hash in zip(relative_paths, content_hashes, strict=True)
        )
        if len({path for path, _hash in pairs}) != len(pairs):
            raise ValueError("completed discovery paths must be unique")
        if any(not re.fullmatch(_HASH_PATTERN, content_hash) for _path, content_hash in pairs):
            raise ValueError("completed discovery content hash is invalid")
        timestamp = normalize_aware_utc(completed_at).isoformat(timespec="microseconds")
        with self._connect(read_only=False) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                for path, content_hash in pairs:
                    row = connection.execute(
                        """
                        SELECT status, completed_at, content_sha256
                        FROM artifact_catalog_discovery
                        WHERE relative_path = ?
                        """,
                        (path,),
                    ).fetchone()
                    if row is None:
                        raise LabArtifactDiscoveryConflictError(
                            "completed bundle was never discovered"
                        )
                    if row["status"] == "completed":
                        if row["content_sha256"] != content_hash:
                            raise LabArtifactDiscoveryConflictError(
                                "completed bundle content identity changed"
                            )
                        continue
                    connection.execute(
                        """
                        UPDATE artifact_catalog_discovery
                        SET status = 'completed', completed_at = ?, content_sha256 = ?
                        WHERE relative_path = ? AND status = 'pending'
                        """,
                        (timestamp, content_hash, path),
                    )
                if pairs:
                    connection.execute(
                        """
                        UPDATE artifact_catalog_discovery_metadata
                        SET revision = revision + 1
                        WHERE singleton = 1
                        """
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise


class LabArtifactCatalogRuntimeStepResult(RuntimeContractModel):
    scan_performed: bool
    scan_generation: int = Field(ge=0)
    scanned_directory_entries: int = Field(ge=0)
    scanned_directories: int = Field(ge=0)
    discovered_bundles: int = Field(ge=0)
    processed_paths: tuple[str, ...]
    pending_bundles: int = Field(ge=0)
    batch: LabArtifactCatalogRunResult


class LabArtifactCatalogRuntime:
    """Run one bounded registration page while holding the sole writer lock."""

    def __init__(
        self,
        *,
        registrar: LabArtifactCatalogRegistrar,
        discovery_queue: LabArtifactDiscoveryQueue,
        max_bundles: int,
        max_discovery_entries: int = 128,
        max_directories_per_step: int = 8,
        max_discovery_seconds: float = 1.0,
        lock_path: Path | None = None,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        if isinstance(max_bundles, bool) or max_bundles < 1:
            raise ValueError("max_bundles must be a positive integer")
        if isinstance(max_discovery_entries, bool) or max_discovery_entries < 1:
            raise ValueError("max_discovery_entries must be a positive integer")
        if isinstance(max_directories_per_step, bool) or max_directories_per_step < 1:
            raise ValueError("max_directories_per_step must be a positive integer")
        if isinstance(max_discovery_seconds, bool) or max_discovery_seconds <= 0:
            raise ValueError("max_discovery_seconds must be positive")
        self.registrar = registrar
        self.discovery_queue = discovery_queue
        self.max_bundles = max_bundles
        self.max_discovery_entries = max_discovery_entries
        self.max_directories_per_step = max_directories_per_step
        self.max_discovery_seconds = max_discovery_seconds
        self.lock_path = lock_path or discovery_queue.path.with_suffix(
            f"{discovery_queue.path.suffix}.lock"
        )
        self.clock = clock or (lambda: datetime.now(UTC))
        self.monotonic = monotonic or time.monotonic

    def run_step(self) -> LabArtifactCatalogRuntimeStepResult:
        with _exclusive_writer_lock(self.lock_path):
            deadline = self.monotonic() + self.max_discovery_seconds
            discovery = self.discovery_queue.scan_step(
                self.registrar,
                max_entries=self.max_discovery_entries,
                max_directories=self.max_directories_per_step,
                deadline=deadline,
                monotonic=self.monotonic,
                discovered_at=normalize_aware_utc(self.clock()),
            )
            pending = self.discovery_queue.list_pending(limit=self.max_bundles)
            batch = self.registrar.run_once(bundle_paths=pending)
            processed = pending[: batch.scanned_bundles]
            self.discovery_queue.mark_completed(
                relative_paths=processed,
                content_hashes=batch.content_hashes,
                completed_at=batch.completed_at,
            )
            return LabArtifactCatalogRuntimeStepResult(
                scan_performed=True,
                scan_generation=discovery.state.scan_generation,
                scanned_directory_entries=discovery.scanned_directory_entries,
                scanned_directories=discovery.scanned_directories,
                discovered_bundles=discovery.discovered_bundles,
                processed_paths=processed,
                pending_bundles=self.discovery_queue.pending_count(),
                batch=batch,
            )


def _frontier_from_row(row: sqlite3.Row) -> LabArtifactDirectoryFrontier:
    try:
        decoded = json.loads(str(row["buffered_entry_names_json"]))
    except (TypeError, ValueError) as exc:
        raise LabArtifactCatalogIntegrityError(
            "artifact discovery frontier buffer is invalid"
        ) from exc
    if not isinstance(decoded, list) or any(not isinstance(item, str) for item in decoded):
        raise LabArtifactCatalogIntegrityError("artifact discovery frontier buffer is invalid")
    return LabArtifactDirectoryFrontier(
        frontier_sequence=int(row["frontier_sequence"]),
        revision=int(row["revision"]),
        scan_generation=int(row["scan_generation"]),
        relative_directory=str(row["relative_directory"]),
        directory_kind=str(row["directory_kind"]),
        directory_device=(
            int(row["directory_device"]) if row["directory_device"] is not None else None
        ),
        directory_inode=(
            int(row["directory_inode"]) if row["directory_inode"] is not None else None
        ),
        directory_offset=int(row["directory_offset"]),
        buffered_entry_names=tuple(decoded),
    )


def _validated_discovery_path(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or not value or any(part in {"", ".", ".."} for part in path.parts):
        raise LabArtifactCatalogIntegrityError("artifact discovery path is unsafe")
    return path.as_posix()


@contextmanager
def _exclusive_writer_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise LabArtifactCatalogAlreadyRunningError(
                "another artifact catalog runtime already owns the writer lock"
            ) from exc
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
