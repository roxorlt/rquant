"""Trusted read-only owner evidence readers for Strategy Lab artifacts."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from rquant.artifact_retention import (
    ArtifactReferenceStore,
    PrivateSqlitePathAuthority,
    execute_sqlite_setup_statement,
    verified_sqlite_connection_scope,
)
from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import (
    ExperimentRegistryReadonlyReader,
    ExperimentSpec,
    ExperimentStatus,
    FormalExperimentPlan,
)
from rquant.job_center_authority import (
    JobCenterAuthorityIntegrityError,
    load_job_center_authority,
)
from rquant.lab_artifact_catalog import (
    LabArtifactCatalogIntegrityError,
    LabArtifactDurableOwners,
    TerminalOwnerReleaser,
)
from rquant.lab_artifact_catalog_runtime import (
    DatasetSnapshotOwnerEvidence,
    DefinitionOwnerEvidence,
    ExperimentOwnerEvidence,
    LabJobOwnerEvidence,
    TrustedLabArtifactOwnerResolver,
)
from rquant.lab_jobs import (
    LAB_JOB_DETAIL_SHARD_LIMIT_MAX,
    JobStatus,
    LabJobReader,
    LabResultState,
    ShardStatus,
)
from rquant.lab_worker import LabShardResultManifest
from rquant.research_run_spec import ResearchExperimentIdentity, StrategyExecutionIdentity
from rquant.runtime_contracts import normalize_aware_utc
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry


class LabJobFinalResultOwnerEvidenceReader:
    """Bind a shard manifest to the ledger's succeeded sealed result graph."""

    def __init__(self, ledger: LabJobReader) -> None:
        self.ledger = ledger

    def __call__(
        self,
        manifest: LabShardResultManifest,
        observed_at: datetime,
        /,
    ) -> tuple[LabJobOwnerEvidence, ...]:
        observed = normalize_aware_utc(observed_at)
        detail = self.ledger.get_job_detail(
            manifest.job_id,
            as_of=observed,
            shard_limit=LAB_JOB_DETAIL_SHARD_LIMIT_MAX,
            event_limit=1,
            artifact_limit=1,
        )
        if detail is None:
            return ()
        if detail.shards_truncated:
            raise LabArtifactCatalogIntegrityError(
                "lab job final result shard evidence is truncated"
            )
        job = detail.job
        result = detail.result_evidence
        if (
            job.status is not JobStatus.SUCCEEDED
            or job.result_state is not LabResultState.SEALED
            or result is None
            or result.job_id != job.job_id
        ):
            raise LabArtifactCatalogIntegrityError(
                "lab job does not have a succeeded sealed final result"
            )
        matches = tuple(shard for shard in detail.shards if shard.shard_id == manifest.shard_id)
        if not matches:
            return ()
        if len(matches) != 1:
            raise LabArtifactCatalogIntegrityError("lab job shard evidence is ambiguous")
        shard = matches[0]
        snapshot = job.spec.dataset_snapshot
        if snapshot is None or snapshot.audit_run_id is None:
            raise LabArtifactCatalogIntegrityError(
                "lab job final result lacks snapshot audit authority"
            )
        if (
            job.job_id,
            job.spec_hash,
            job.spec.code_sha,
            shard.job_id,
            shard.status,
            shard.result_manifest_hash,
            shard.plan_hash,
            manifest.worker_code_sha,
        ) != (
            manifest.job_id,
            manifest.spec_hash,
            manifest.worker_code_sha,
            manifest.job_id,
            ShardStatus.SUCCEEDED,
            manifest.manifest_hash,
            manifest.plan_hash,
            job.spec.code_sha,
        ):
            raise LabArtifactCatalogIntegrityError(
                "lab job final result conflicts with the sealed shard manifest"
            )
        if shard.finished_at is None:
            raise LabArtifactCatalogIntegrityError("sealed lab shard has no finished_at evidence")
        execution = getattr(job.spec, "strategy_execution", None)
        experiment = getattr(job.spec, "experiment", None)
        if (
            getattr(job.spec, "schema_version", None) != 3
            or not isinstance(execution, StrategyExecutionIdentity)
            or not isinstance(experiment, ResearchExperimentIdentity)
            or experiment.schema_version != 2
            or experiment.formal_plan_id is None
        ):
            raise LabArtifactCatalogIntegrityError(
                "lab job owner requires a v3 first-class execution identity"
            )
        if (
            manifest.experiment_id,
            manifest.experiment_attempt_identity,
            manifest.strategy_execution_identity_hash,
            manifest.strategy_spec_fingerprint,
            manifest.strategy_executable_fingerprint,
            manifest.candidate_schema_fingerprint,
        ) != (
            experiment.experiment_id,
            experiment.attempt_identity,
            execution.identity_hash,
            execution.strategy_spec_fingerprint,
            execution.strategy_executable_fingerprint,
            execution.candidate_schema_fingerprint,
        ):
            raise LabArtifactCatalogIntegrityError(
                "sealed shard manifest conflicts with first-class execution ownership"
            )
        recorded_at = max(job.updated_at, shard.finished_at, result.indexed_at)
        if recorded_at > observed:
            raise LabArtifactCatalogIntegrityError("lab job owner evidence is from the future")
        return (
            LabJobOwnerEvidence(
                job_id=job.job_id,
                shard_id=shard.shard_id,
                result_manifest_hash=manifest.manifest_hash,
                spec_hash=job.spec_hash,
                plan_hash=shard.plan_hash,
                snapshot_id=snapshot.snapshot_id,
                snapshot_binding_hash=snapshot.binding_hash,
                audit_run_id=snapshot.audit_run_id,
                experiment_id=experiment.experiment_id,
                experiment_attempt_identity=experiment.attempt_identity,
                formal_plan_id=experiment.formal_plan_id,
                strategy_id=execution.strategy_id,
                strategy_version=execution.strategy_version,
                strategy_execution_identity_hash=execution.identity_hash,
                strategy_spec_fingerprint=execution.strategy_spec_fingerprint,
                strategy_definition_fingerprint=(execution.strategy_definition_fingerprint),
                definition_registration_record_hash=(execution.definition_registration_record_hash),
                definition_registered_at=execution.definition_registered_at,
                definition_available_at=execution.definition_available_at,
                strategy_executable_fingerprint=execution.strategy_executable_fingerprint,
                candidate_schema_fingerprint=execution.candidate_schema_fingerprint,
                code_commit=job.spec.code_sha,
                recorded_at=recorded_at,
            ),
        )


class DatasetSnapshotAuthorityOwnerEvidenceReader:
    """Read snapshot, binding, and audit authority from one read-only DuckDB generation."""

    def __init__(self, path: Path, *, managed_trust_root: Path) -> None:
        self._path_authority = PrivateSqlitePathAuthority(
            path,
            label="dataset snapshot authority",
            create_if_missing=False,
            managed_trust_root=managed_trust_root,
        )
        self.path = self._path_authority.path

    def __call__(
        self,
        job: LabJobOwnerEvidence,
        observed_at: datetime,
        /,
    ) -> tuple[DatasetSnapshotOwnerEvidence, ...]:
        observed = normalize_aware_utc(observed_at)
        try:
            self._path_authority.assert_current()
            with DuckDBStore(self.path, read_only=True) as store:
                self._path_authority.assert_current()
                snapshot = store.get_dataset_snapshot(job.snapshot_id)
                binding = store.get_dataset_snapshot_binding(job.snapshot_id)
                audit = store.get_data_audit_run(job.audit_run_id)
                self._path_authority.assert_current()
            self._path_authority.assert_current()
        except ValueError as exc:
            raise LabArtifactCatalogIntegrityError(
                "dataset snapshot authority path changed while reading"
            ) from exc
        if snapshot is None or binding is None or audit is None:
            return ()
        if (
            snapshot.status != "ready"
            or snapshot.completed_at is None
            or binding.status != "ready"
            or binding.completed_at is None
            or audit.status != "completed"
            or audit.completed_at is None
        ):
            raise LabArtifactCatalogIntegrityError(
                "dataset snapshot, binding, and audit must all be complete"
            )
        if (
            snapshot.snapshot_id,
            snapshot.code_commit,
            binding.snapshot_id,
            binding.binding_hash,
            binding.manifest.code_commit,
            audit.audit_run_id,
        ) != (
            job.snapshot_id,
            job.code_commit,
            job.snapshot_id,
            job.snapshot_binding_hash,
            job.code_commit,
            job.audit_run_id,
        ):
            raise LabArtifactCatalogIntegrityError(
                "dataset snapshot authority conflicts with lab job identity"
            )
        evidence = DatasetSnapshotOwnerEvidence(
            snapshot_id=snapshot.snapshot_id,
            binding_hash=binding.binding_hash,
            audit_run_id=audit.audit_run_id,
            code_commit=snapshot.code_commit,
            status="ready",
            completed_at=snapshot.completed_at,
            binding_completed_at=binding.completed_at,
            audit_completed_at=audit.completed_at,
        )
        if (
            max(
                evidence.completed_at,
                evidence.binding_completed_at,
                evidence.audit_completed_at,
            )
            > observed
        ):
            raise LabArtifactCatalogIntegrityError(
                "dataset snapshot authority contains future evidence"
            )
        return (evidence,)


class ExperimentRegistryOwnerEvidenceReader:
    """Resolve experiments by immutable snapshot, strategy, and code identities."""

    def __init__(
        self,
        path: Path,
        *,
        managed_trust_root: Path,
        busy_timeout_ms: int = 5_000,
        max_experiment_rows: int = 100_000,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        if isinstance(max_experiment_rows, bool) or max_experiment_rows < 1:
            raise ValueError("max_experiment_rows must be a positive integer")
        self._path_authority = PrivateSqlitePathAuthority(
            path,
            label="experiment registry",
            create_if_missing=False,
            managed_trust_root=managed_trust_root,
        )
        self.path = self._path_authority.path
        self.busy_timeout_ms = busy_timeout_ms
        self.max_experiment_rows = max_experiment_rows
        self._batch_index: (
            dict[
                tuple[str, str, str, str, str, str, str, str, str],
                tuple[ExperimentOwnerEvidence, ...],
            ]
            | None
        ) = None

    @contextmanager
    def batch(self) -> Iterator[None]:
        if self._batch_index is not None:
            raise LabArtifactCatalogIntegrityError("experiment owner batch is already active")
        self._batch_index = self._read_index()
        try:
            yield
        finally:
            self._batch_index = None

    def __call__(
        self,
        job: LabJobOwnerEvidence,
        snapshot: DatasetSnapshotOwnerEvidence,
        observed_at: datetime,
        /,
    ) -> tuple[ExperimentOwnerEvidence, ...]:
        observed = normalize_aware_utc(observed_at)
        index = self._batch_index if self._batch_index is not None else self._read_index()
        evidence = index.get(
            (
                job.experiment_id,
                job.formal_plan_id,
                snapshot.snapshot_id,
                job.strategy_spec_fingerprint,
                job.strategy_definition_fingerprint,
                job.definition_registration_record_hash,
                job.strategy_executable_fingerprint,
                job.candidate_schema_fingerprint,
                job.code_commit,
            ),
            (),
        )
        if any(item.registered_at > observed for item in evidence):
            raise LabArtifactCatalogIntegrityError(
                "experiment registry contains future owner evidence"
            )
        return evidence

    def _read_index(
        self,
    ) -> dict[
        tuple[str, str, str, str, str, str, str, str, str],
        tuple[ExperimentOwnerEvidence, ...],
    ]:
        uri = self._path_authority.readonly_uri()
        try:
            connection = self._path_authority.open_verified_connection(
                lambda _path: sqlite3.connect(
                    uri,
                    uri=True,
                    timeout=self.busy_timeout_ms / 1_000,
                    isolation_level=None,
                )
            )
        except (sqlite3.Error, ValueError) as exc:
            raise LabArtifactCatalogIntegrityError(
                "experiment registry path is unsafe while opening read-only"
            ) from exc
        with verified_sqlite_connection_scope(connection, self._path_authority):
            try:
                connection.row_factory = sqlite3.Row
                execute_sqlite_setup_statement(
                    connection,
                    f"PRAGMA busy_timeout = {self.busy_timeout_ms}",
                )
                execute_sqlite_setup_statement(connection, "PRAGMA query_only = ON")
                execute_sqlite_setup_statement(connection, "PRAGMA trusted_schema = OFF")
                self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                _require_experiment_schema(connection)
                connection.execute("BEGIN")
                self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                rows = connection.execute(
                    """
                    SELECT attempt.experiment_id, attempt.hypothesis_family,
                           attempt.spec_json, attempt.status, attempt.registered_at,
                           formal.plan_json
                    FROM experiment_attempt AS attempt
                    LEFT JOIN formal_experiment_plan AS formal
                      ON formal.experiment_id = attempt.experiment_id
                    ORDER BY attempt.experiment_id
                    LIMIT ?
                    """,
                    (self.max_experiment_rows + 1,),
                ).fetchall()
                self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                connection.execute("COMMIT")
                self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
            except ValueError as exc:
                if connection.in_transaction:
                    connection.rollback()
                raise LabArtifactCatalogIntegrityError(
                    "experiment registry path changed while reading"
                ) from exc
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

        if len(rows) > self.max_experiment_rows:
            raise LabArtifactCatalogIntegrityError(
                "experiment registry exceeds the owner index row budget"
            )
        index: dict[
            tuple[str, str, str, str, str, str, str, str, str],
            list[ExperimentOwnerEvidence],
        ] = {}
        for row in rows:
            try:
                spec = ExperimentSpec.model_validate_json(row["spec_json"])
                ExperimentStatus(str(row["status"]))
                if row["plan_json"] is None:
                    raise ValueError("formal plan is missing")
                plan = FormalExperimentPlan.model_validate_json(row["plan_json"])
                registered_at = normalize_aware_utc(
                    datetime.fromisoformat(str(row["registered_at"]))
                )
            except (TypeError, ValueError) as exc:
                raise LabArtifactCatalogIntegrityError(
                    "experiment registry contains invalid immutable evidence"
                ) from exc
            if (
                spec.experiment_id != row["experiment_id"]
                or spec.hypothesis_family != row["hypothesis_family"]
                or plan.schema_version != 2
                or plan.spec != spec
                or plan.strategy_definition_fingerprint is None
                or plan.definition_registration_record_hash is None
            ):
                raise LabArtifactCatalogIntegrityError(
                    "experiment registry index conflicts with immutable spec"
                )
            assert spec.experiment_id is not None
            evidence = ExperimentOwnerEvidence(
                experiment_id=spec.experiment_id,
                formal_plan_id=plan.plan_id,
                dataset_snapshot_id=spec.dataset_snapshot_id,
                strategy_spec_fingerprint=spec.strategy_spec_fingerprint,
                strategy_definition_fingerprint=(plan.strategy_definition_fingerprint),
                definition_registration_record_hash=(plan.definition_registration_record_hash),
                strategy_executable_fingerprint=spec.strategy_executable_fingerprint,
                candidate_schema_fingerprint=spec.candidate_schema_fingerprint,
                code_commit=spec.code_commit,
                registered_at=registered_at,
            )
            key = (
                evidence.experiment_id,
                evidence.formal_plan_id,
                evidence.dataset_snapshot_id,
                evidence.strategy_spec_fingerprint,
                evidence.strategy_definition_fingerprint,
                evidence.definition_registration_record_hash,
                evidence.strategy_executable_fingerprint,
                evidence.candidate_schema_fingerprint,
                evidence.code_commit,
            )
            index.setdefault(key, []).append(evidence)
        return {
            key: tuple(sorted(values, key=lambda item: item.experiment_id))
            for key, values in index.items()
        }


class DefinitionRegistryOwnerEvidenceReader:
    """Re-open the installed authority and exact-match Definition Registry receipts."""

    def __init__(
        self,
        *,
        authority_manifest_path: Path,
        runtime_root: Path,
        lab_jobs_path: Path,
        experiment_registry_path: Path,
        dataset_authority_path: Path,
    ) -> None:
        self.authority_manifest_path = authority_manifest_path
        self.runtime_root = runtime_root
        self.lab_jobs_path = lab_jobs_path
        self.experiment_registry_path = experiment_registry_path
        self.dataset_authority_path = dataset_authority_path

    def __call__(
        self,
        job: LabJobOwnerEvidence,
        experiment: ExperimentOwnerEvidence,
        observed_at: datetime,
        /,
    ) -> tuple[DefinitionOwnerEvidence, ...]:
        observed = normalize_aware_utc(observed_at)
        if (
            experiment.strategy_definition_fingerprint != job.strategy_definition_fingerprint
            or experiment.definition_registration_record_hash
            != job.definition_registration_record_hash
        ):
            raise LabArtifactCatalogIntegrityError(
                "formal plan and lab job Definition Registry receipts conflict"
            )
        try:
            authority = load_job_center_authority(
                self.authority_manifest_path,
                expected_code_sha=job.code_commit,
                runtime_root=self.runtime_root,
                lab_jobs_path=self.lab_jobs_path,
                experiment_registry_path=self.experiment_registry_path,
                dataset_authority_path=self.dataset_authority_path,
            )
            registry = ImmutableDefinitionRegistry(
                authority.definition_registry_root,
                execution_registry=BuiltinStrategyEvaluatorRegistry(
                    producer_commit=job.code_commit
                ).trusted_executable_registry(),
            )
            registration = registry.read_strategy_spec(
                job.strategy_definition_fingerprint,
                as_of=observed,
            )
        except (JobCenterAuthorityIntegrityError, TypeError, ValueError) as exc:
            raise LabArtifactCatalogIntegrityError(
                "Definition Registry authority cannot be verified"
            ) from exc
        if registration is None:
            return ()
        return (
            DefinitionOwnerEvidence(
                strategy_id=registration.logical_id,
                strategy_version=registration.version,
                strategy_spec_fingerprint=registration.spec.spec_fingerprint,
                strategy_definition_fingerprint=registration.fingerprint,
                strategy_executable_fingerprint=registration.executable_fingerprint,
                candidate_schema_fingerprint=registration.candidate_schema_fingerprint,
                definition_registration_record_hash=registration.record_hash,
                producer_code_commit=registration.producer_commit,
                registered_at=registration.registered_at,
                available_at=registration.available_at,
            ),
        )


@dataclass(frozen=True)
class LabArtifactOwnerReaderComposition:
    job_reader: LabJobFinalResultOwnerEvidenceReader
    snapshot_reader: DatasetSnapshotAuthorityOwnerEvidenceReader
    experiment_reader: ExperimentRegistryOwnerEvidenceReader
    definition_reader: DefinitionRegistryOwnerEvidenceReader
    owner_resolver: TrustedLabArtifactOwnerResolver


def build_lab_artifact_owner_reader_composition(
    *,
    lab_jobs_path: Path,
    lab_jobs_managed_trust_root: Path,
    dataset_authority_path: Path,
    dataset_authority_managed_trust_root: Path,
    experiment_registry_path: Path,
    experiment_registry_managed_trust_root: Path,
    max_experiment_rows: int = 100_000,
    clock: Callable[[], datetime] | None = None,
) -> LabArtifactOwnerReaderComposition:
    """Compose only concrete read-only authorities for a future runtime builder."""

    lab_jobs_authority = PrivateSqlitePathAuthority(
        lab_jobs_path,
        label="lab jobs owner ledger",
        create_if_missing=False,
        managed_trust_root=lab_jobs_managed_trust_root,
    )
    job_reader = LabJobFinalResultOwnerEvidenceReader(
        LabJobReader(lab_jobs_authority.path, identity_authority=lab_jobs_authority)
    )
    snapshot_reader = DatasetSnapshotAuthorityOwnerEvidenceReader(
        dataset_authority_path,
        managed_trust_root=dataset_authority_managed_trust_root,
    )
    experiment_reader = ExperimentRegistryOwnerEvidenceReader(
        experiment_registry_path,
        managed_trust_root=experiment_registry_managed_trust_root,
        max_experiment_rows=max_experiment_rows,
    )
    definition_reader = DefinitionRegistryOwnerEvidenceReader(
        authority_manifest_path=lab_jobs_authority.path.parent / "job-center-authority.json",
        runtime_root=lab_jobs_authority.path.parent,
        lab_jobs_path=lab_jobs_authority.path,
        experiment_registry_path=experiment_reader.path,
        dataset_authority_path=snapshot_reader.path,
    )

    def rebind_shared_authorities() -> None:
        for authority in (
            lab_jobs_authority,
            snapshot_reader._path_authority,
            experiment_reader._path_authority,
        ):
            authority.rebind_and_assert_current_after_trusted_sqlite_change()

    def terminal_owner_releaser_factory(
        reference_store: ArtifactReferenceStore,
    ) -> TerminalOwnerReleaser:
        from rquant.lab_jobs_serving_authority import (
            LabArtifactTerminalReleaseCoordinator,
        )

        coordinators: dict[str, LabArtifactTerminalReleaseCoordinator] = {}

        def release_terminal_owner(
            manifest: LabShardResultManifest,
            owners: LabArtifactDurableOwners,
            observed_at: datetime,
        ) -> None:
            coordinator = coordinators.get(manifest.worker_code_sha)
            if coordinator is None:
                authority = load_job_center_authority(
                    definition_reader.authority_manifest_path,
                    expected_code_sha=manifest.worker_code_sha,
                    runtime_root=definition_reader.runtime_root,
                    lab_jobs_path=definition_reader.lab_jobs_path,
                    experiment_registry_path=definition_reader.experiment_registry_path,
                    dataset_authority_path=definition_reader.dataset_authority_path,
                )
                coordinator = LabArtifactTerminalReleaseCoordinator(
                    reader=job_reader.ledger,
                    experiment_registry=ExperimentRegistryReadonlyReader(
                        authority.experiment_registry_path,
                        managed_trust_root=authority.runtime_root,
                    ),
                    definition_registry=ImmutableDefinitionRegistry(
                        authority.definition_registry_root,
                        execution_registry=BuiltinStrategyEvaluatorRegistry(
                            producer_commit=authority.code_sha
                        ).trusted_executable_registry(),
                    ),
                    reference_store=reference_store,
                )
                coordinators[manifest.worker_code_sha] = coordinator
            coordinator(manifest, owners, observed_at)

        return release_terminal_owner

    resolver = TrustedLabArtifactOwnerResolver(
        job_evidence_reader=job_reader,
        snapshot_evidence_reader=snapshot_reader,
        experiment_evidence_reader=experiment_reader,
        definition_evidence_reader=definition_reader,
        authority_rebinder=rebind_shared_authorities,
        terminal_owner_releaser_factory=terminal_owner_releaser_factory,
        clock=clock,
    )
    return LabArtifactOwnerReaderComposition(
        job_reader=job_reader,
        snapshot_reader=snapshot_reader,
        experiment_reader=experiment_reader,
        definition_reader=definition_reader,
        owner_resolver=resolver,
    )


def _require_experiment_schema(connection: sqlite3.Connection) -> None:
    columns = tuple(
        row[1] for row in connection.execute("PRAGMA table_info(experiment_attempt)").fetchall()
    )
    expected = (
        "experiment_id",
        "hypothesis_family",
        "spec_json",
        "status",
        "registered_at",
        "started_at",
        "completed_at",
        "first_error",
    )
    if columns != expected:
        raise LabArtifactCatalogIntegrityError("experiment attempt schema is incompatible")
    formal_columns = tuple(
        row[1] for row in connection.execute("PRAGMA table_info(formal_experiment_plan)").fetchall()
    )
    if formal_columns != (
        "plan_id",
        "experiment_id",
        "resolution_key",
        "preregistered_at",
        "plan_json",
    ):
        raise LabArtifactCatalogIntegrityError("formal experiment plan schema is incompatible")
