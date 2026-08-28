from __future__ import annotations

import os
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

import rquant.artifact_retention as artifact_retention_module
import rquant.lab_artifact_catalog_readers as catalog_readers_module
from rquant.data_metadata import (
    DataAuditRun,
    DataAuditRunFinalization,
    DatasetSnapshot,
    DatasetSnapshotArtifact,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingFinalization,
    DatasetSnapshotBindingManifest,
    DatasetSnapshotFinalization,
)
from rquant.experiment_registry import (
    DateRange,
    ExperimentRegistry,
    ExperimentSpec,
    FormalExperimentPlan,
    HypothesisFamilyManifest,
)
from rquant.lab_artifact_catalog import LabArtifactCatalogIntegrityError
from rquant.lab_artifact_catalog_readers import (
    DatasetSnapshotAuthorityOwnerEvidenceReader,
    ExperimentRegistryOwnerEvidenceReader,
    LabJobFinalResultOwnerEvidenceReader,
    build_lab_artifact_owner_reader_composition,
)
from rquant.lab_artifact_catalog_runtime import LabJobOwnerEvidence
from rquant.lab_jobs import JobStatus, LabJobReader, LabResultState, ShardStatus
from rquant.lab_worker import (
    CURRENT_CONTENT_DIGEST_ALGORITHM,
    LabShardArtifactManifest,
    LabShardResultManifest,
)
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ResearchExperimentIdentity,
    StrategyExecutionIdentity,
)
from rquant.storage.duckdb import DuckDBStore

NOW = datetime(2026, 8, 2, 3, 0, tzinfo=UTC)
JOB_ID = UUID("11111111-1111-4111-8111-111111111111")
SHARD_ID = UUID("22222222-2222-4222-8222-222222222222")
CLAIM_TOKEN = UUID("33333333-3333-4333-8333-333333333333")
SPEC_HASH = "a" * 64
PLAN_HASH = "b" * 64
CODE_SHA = "1" * 40
STRATEGY_FINGERPRINT = "f" * 64
EXECUTABLE_FINGERPRINT = "8" * 64
CANDIDATE_SCHEMA_FINGERPRINT = "9" * 64


def _strategy_identity() -> StrategyExecutionIdentity:
    return StrategyExecutionIdentity(
        strategy_id="n_shape",
        strategy_version=1,
        adapter_id="n-shape",
        adapter_version="1",
        strategy_spec_fingerprint=STRATEGY_FINGERPRINT,
        strategy_definition_fingerprint="2" * 64,
        strategy_executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        definition_registration_record_hash="3" * 64,
        definition_registered_at=NOW - timedelta(days=1),
        definition_available_at=NOW - timedelta(days=1),
        producer_code_commit=CODE_SHA,
    )


def _experiment_identity() -> ResearchExperimentIdentity:
    spec = ExperimentSpec(
        strategy_spec_fingerprint=STRATEGY_FINGERPRINT,
        strategy_executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        dataset_snapshot_id="4" * 64,
        code_commit=CODE_SHA,
        parameter_fingerprint="a" * 64,
        hypothesis_family="n-shape-family",
        metric_definition_fingerprint="b" * 64,
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 6, 30)),
        validation_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 12, 31)),
        frozen_outer_test_range=DateRange(start_date=date(2026, 1, 1), end_date=date(2026, 3, 31)),
        cost_model_fingerprint="c" * 64,
        execution_model_fingerprint="d" * 64,
        seed=7,
    )
    assert spec.experiment_id is not None
    return ResearchExperimentIdentity(
        schema_version=2,
        spec=spec,
        experiment_id=spec.experiment_id,
        hypothesis_family="n-shape-family",
        hypothesis_variant="baseline",
        formal_plan_id="0" * 64,
    )


def _exception_leaves(error: BaseException) -> tuple[BaseException, ...]:
    if isinstance(error, BaseExceptionGroup):
        return tuple(leaf for nested in error.exceptions for leaf in _exception_leaves(nested))
    return (error,)


def _manifest() -> LabShardResultManifest:
    execution = _strategy_identity()
    experiment = _experiment_identity()
    return LabShardResultManifest(
        worker_code_sha=CODE_SHA,
        content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
        job_id=JOB_ID,
        shard_id=SHARD_ID,
        claim_token=CLAIM_TOKEN,
        claim_generation=3,
        scheduler_fencing_token=7,
        spec_hash=SPEC_HASH,
        payload_hash="c" * 64,
        plan_hash=PLAN_HASH,
        adapter_id="n-shape",
        adapter_version="1",
        experiment_id=experiment.experiment_id,
        experiment_attempt_identity=experiment.attempt_identity,
        strategy_execution_identity_hash=execution.identity_hash,
        strategy_spec_fingerprint=execution.strategy_spec_fingerprint,
        strategy_executable_fingerprint=execution.strategy_executable_fingerprint,
        candidate_schema_fingerprint=execution.candidate_schema_fingerprint,
        artifacts=(
            LabShardArtifactManifest(
                name="result",
                file_name="000-result.parquet",
                row_count=1,
                columns=("ts_code",),
                file_size=10,
                file_sha256="d" * 64,
                content_sha256="e" * 64,
            ),
        ),
    )


def _job_evidence(
    *,
    snapshot_id: str = "4" * 64,
    binding_hash: str = "5" * 64,
    audit_run_id: str = "6" * 64,
    experiment_id: str | None = None,
    formal_plan_id: str = "0" * 64,
) -> LabJobOwnerEvidence:
    manifest = _manifest()
    return LabJobOwnerEvidence(
        job_id=JOB_ID,
        shard_id=SHARD_ID,
        result_manifest_hash=manifest.manifest_hash,
        spec_hash=SPEC_HASH,
        plan_hash=PLAN_HASH,
        snapshot_id=snapshot_id,
        snapshot_binding_hash=binding_hash,
        audit_run_id=audit_run_id,
        experiment_id=experiment_id or _experiment_identity().experiment_id,
        experiment_attempt_identity=_experiment_identity().attempt_identity,
        formal_plan_id=formal_plan_id,
        strategy_id=_strategy_identity().strategy_id,
        strategy_version=_strategy_identity().strategy_version,
        strategy_execution_identity_hash=_strategy_identity().identity_hash,
        strategy_spec_fingerprint=STRATEGY_FINGERPRINT,
        strategy_definition_fingerprint=(_strategy_identity().strategy_definition_fingerprint),
        definition_registration_record_hash=(
            _strategy_identity().definition_registration_record_hash
        ),
        definition_registered_at=_strategy_identity().definition_registered_at,
        definition_available_at=_strategy_identity().definition_available_at,
        strategy_executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        code_commit=CODE_SHA,
        recorded_at=NOW - timedelta(minutes=1),
    )


def test_lab_job_reader_requires_succeeded_sealed_final_result_graph() -> None:
    manifest = _manifest()
    snapshot = DatasetSnapshotIdentity(
        snapshot_id="4" * 64,
        binding_hash="5" * 64,
        audit_run_id="6" * 64,
    )
    detail = SimpleNamespace(
        job=SimpleNamespace(
            job_id=JOB_ID,
            status=JobStatus.SUCCEEDED,
            result_state=LabResultState.SEALED,
            spec_hash=SPEC_HASH,
            spec=SimpleNamespace(
                dataset_snapshot=snapshot,
                code_sha=CODE_SHA,
                schema_version=3,
                strategy_execution=_strategy_identity(),
                experiment=_experiment_identity(),
                parameters=SimpleNamespace(arguments=()),
            ),
            updated_at=NOW - timedelta(minutes=2),
        ),
        shards=(
            SimpleNamespace(
                shard_id=SHARD_ID,
                job_id=JOB_ID,
                status=ShardStatus.SUCCEEDED,
                result_manifest_hash=manifest.manifest_hash,
                plan_hash=PLAN_HASH,
                finished_at=NOW - timedelta(minutes=3),
            ),
        ),
        shards_truncated=False,
        result_evidence=SimpleNamespace(
            job_id=JOB_ID,
            indexed_at=NOW - timedelta(minutes=1),
        ),
    )

    class _ReadonlyLedger:
        def get_job_detail(self, job_id: UUID, **_kwargs: object) -> object:
            assert job_id == JOB_ID
            return detail

    evidence = LabJobFinalResultOwnerEvidenceReader(_ReadonlyLedger())(manifest, NOW)

    assert evidence == (_job_evidence(),)
    assert evidence[0].candidate_schema_fingerprint == CANDIDATE_SCHEMA_FINGERPRINT


def test_lab_job_reader_rejects_legacy_argument_fingerprint_smuggling() -> None:
    manifest = _manifest()
    snapshot = DatasetSnapshotIdentity(
        snapshot_id="4" * 64,
        binding_hash="5" * 64,
        audit_run_id="6" * 64,
    )
    detail = SimpleNamespace(
        job=SimpleNamespace(
            job_id=JOB_ID,
            status=JobStatus.SUCCEEDED,
            result_state=LabResultState.SEALED,
            spec_hash=SPEC_HASH,
            spec=SimpleNamespace(
                dataset_snapshot=snapshot,
                code_sha=CODE_SHA,
                schema_version=2,
                strategy_execution=None,
                experiment=None,
                parameters=SimpleNamespace(
                    arguments=(
                        SimpleNamespace(
                            name="strategy_spec_fingerprint",
                            value=STRATEGY_FINGERPRINT,
                        ),
                    )
                ),
            ),
            updated_at=NOW - timedelta(minutes=2),
        ),
        shards=(
            SimpleNamespace(
                shard_id=SHARD_ID,
                job_id=JOB_ID,
                status=ShardStatus.SUCCEEDED,
                result_manifest_hash=manifest.manifest_hash,
                plan_hash=PLAN_HASH,
                finished_at=NOW - timedelta(minutes=3),
            ),
        ),
        shards_truncated=False,
        result_evidence=SimpleNamespace(
            job_id=JOB_ID,
            indexed_at=NOW - timedelta(minutes=1),
        ),
    )

    class _ReadonlyLedger:
        def get_job_detail(self, _job_id: UUID, **_kwargs: object) -> object:
            return detail

    with pytest.raises(LabArtifactCatalogIntegrityError, match="v3|execution identity"):
        LabJobFinalResultOwnerEvidenceReader(_ReadonlyLedger())(manifest, NOW)


def _create_dataset_authority(path: Path) -> tuple[str, str, str]:
    with DuckDBStore(path) as store:
        audit = DataAuditRun.create(
            as_of_date=date(2026, 8, 1),
            range_start=date(2026, 7, 1),
            range_end=date(2026, 8, 1),
            rule_set_version="stage1-v2",
            observed_at=NOW - timedelta(hours=4),
        )
        store.begin_data_audit_run(audit)
        store.finalize_data_audit_run(
            audit.audit_run_id,
            DataAuditRunFinalization(
                p0_count=0,
                completed_at=NOW - timedelta(hours=3),
            ),
        )
        snapshot = DatasetSnapshot.create(
            strategy_name="n_shape",
            as_of_time=NOW - timedelta(hours=4),
            code_commit=CODE_SHA,
            origin="unit-test",
            created_at=NOW - timedelta(hours=4),
        )
        store.begin_dataset_snapshot(snapshot)
        store.finalize_dataset_snapshot(
            snapshot.snapshot_id,
            DatasetSnapshotFinalization(completed_at=NOW - timedelta(hours=3)),
        )
        manifest = DatasetSnapshotBindingManifest(
            snapshot_id=snapshot.snapshot_id,
            strategy_name="n_shape",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 8, 1),
            as_of_time=snapshot.as_of_time,
            code_commit=CODE_SHA,
            dependency_contract_version="stage1-v1",
            builder_version="snapshot-builder-v1",
            artifacts=(
                DatasetSnapshotArtifact(
                    artifact_type="materialized_table",
                    dataset_id="daily_bar",
                    table_name="daily_bar",
                    artifact_key="daily_bar:2026-07-01:2026-08-01",
                    relative_path="tables/daily_bar.parquet",
                    row_count=1,
                    schema_hash="7" * 64,
                    content_hash="8" * 64,
                    file_hash="9" * 64,
                ),
            ),
        )
        binding = DatasetSnapshotBinding.create(
            manifest=manifest,
            artifact_root="/srv/rquant/research-lake",
            manifest_relative_path="snapshots/manifest.json",
            created_at=NOW - timedelta(hours=3),
        )
        store.begin_dataset_snapshot_binding(binding)
        binding = store.finalize_dataset_snapshot_binding(
            snapshot.snapshot_id,
            DatasetSnapshotBindingFinalization(completed_at=NOW - timedelta(hours=2)),
        )
    path.chmod(0o600)
    return snapshot.snapshot_id, binding.binding_hash, audit.audit_run_id


def test_dataset_snapshot_reader_binds_snapshot_binding_audit_and_code_pit(
    tmp_path: Path,
) -> None:
    path = tmp_path / "authority.duckdb"
    snapshot_id, binding_hash, audit_run_id = _create_dataset_authority(path)
    job = _job_evidence(
        snapshot_id=snapshot_id,
        binding_hash=binding_hash,
        audit_run_id=audit_run_id,
    )

    evidence = DatasetSnapshotAuthorityOwnerEvidenceReader(
        path,
        managed_trust_root=tmp_path,
    )(job, NOW)

    assert len(evidence) == 1
    assert evidence[0].snapshot_id == snapshot_id
    assert evidence[0].binding_hash == binding_hash
    assert evidence[0].audit_run_id == audit_run_id
    assert evidence[0].code_commit == CODE_SHA
    with DuckDBStore(path, read_only=True) as store:
        assert store.get_dataset_snapshot(snapshot_id) is not None


def test_dataset_snapshot_reader_fails_closed_on_binding_hash_conflict(
    tmp_path: Path,
) -> None:
    path = tmp_path / "authority.duckdb"
    snapshot_id, _binding_hash, audit_run_id = _create_dataset_authority(path)
    job = _job_evidence(
        snapshot_id=snapshot_id,
        binding_hash="f" * 64,
        audit_run_id=audit_run_id,
    )

    with pytest.raises(LabArtifactCatalogIntegrityError, match="conflicts"):
        DatasetSnapshotAuthorityOwnerEvidenceReader(
            path,
            managed_trust_root=tmp_path,
        )(job, NOW)


def _experiment_spec(snapshot_id: str) -> ExperimentSpec:
    return ExperimentSpec(
        strategy_spec_fingerprint=STRATEGY_FINGERPRINT,
        strategy_executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint="9" * 64,
        dataset_snapshot_id=snapshot_id,
        code_commit=CODE_SHA,
        parameter_fingerprint="a" * 64,
        hypothesis_family="artifact-owner-test",
        metric_definition_fingerprint="b" * 64,
        train_range=DateRange(start_date=date(2026, 1, 1), end_date=date(2026, 2, 1)),
        validation_range=DateRange(start_date=date(2026, 3, 1), end_date=date(2026, 4, 1)),
        frozen_outer_test_range=DateRange(start_date=date(2026, 5, 1), end_date=date(2026, 6, 1)),
        cost_model_fingerprint="c" * 64,
        execution_model_fingerprint="d" * 64,
        seed=7,
    )


def _register_formal_attempts(
    registry: ExperimentRegistry,
    specs: tuple[ExperimentSpec, ...],
    *,
    registered_at: tuple[datetime, ...] | None = None,
) -> dict[str, FormalExperimentPlan]:
    attempts_at = registered_at or tuple(NOW - timedelta(minutes=1) for _ in specs)
    plans: dict[str, FormalExperimentPlan] = {}
    families = {spec.hypothesis_family for spec in specs}
    for family in families:
        family_specs = tuple(spec for spec in specs if spec.hypothesis_family == family)
        manifest = HypothesisFamilyManifest(
            hypothesis_family=family,
            experiment_ids=tuple(
                spec.experiment_id for spec in family_specs if spec.experiment_id is not None
            ),
            search_space_fingerprint="e" * 64,
            metric_definition_fingerprint=family_specs[0].metric_definition_fingerprint,
            preregistered_at=NOW - timedelta(minutes=3),
        )
        for spec in family_specs:
            plan = FormalExperimentPlan(
                schema_version=2,
                spec=spec,
                hypothesis_variant="baseline",
                strategy_definition_fingerprint=(
                    _strategy_identity().strategy_definition_fingerprint
                ),
                definition_registration_record_hash=(
                    _strategy_identity().definition_registration_record_hash
                ),
                preregistered_at=NOW - timedelta(minutes=2),
            )
            registry.register_formal_plan(plan, family_manifest=manifest)
            assert spec.experiment_id is not None
            plans[spec.experiment_id] = plan
    for spec, attempt_at in zip(specs, attempts_at, strict=True):
        registry.register_attempt(spec, registered_at=attempt_at)
    return plans


def test_experiment_reader_resolves_unique_pit_hash_identity(tmp_path: Path) -> None:
    path = tmp_path / "experiments.sqlite3"
    spec = _experiment_spec("4" * 64)
    assert spec.experiment_id is not None
    registry = ExperimentRegistry(path, managed_trust_root=tmp_path)
    plans = _register_formal_attempts(registry, (spec,))
    path.chmod(0o600)
    job = _job_evidence(
        snapshot_id=spec.dataset_snapshot_id,
        experiment_id=spec.experiment_id,
        formal_plan_id=plans[spec.experiment_id].plan_id,
    )

    evidence = ExperimentRegistryOwnerEvidenceReader(
        path,
        managed_trust_root=tmp_path,
    )(
        job,
        SimpleNamespace(snapshot_id=spec.dataset_snapshot_id),
        NOW,
    )

    assert len(evidence) == 1
    assert evidence[0].experiment_id == spec.experiment_id
    assert evidence[0].dataset_snapshot_id == spec.dataset_snapshot_id
    assert evidence[0].strategy_spec_fingerprint == STRATEGY_FINGERPRINT
    assert evidence[0].strategy_executable_fingerprint == EXECUTABLE_FINGERPRINT
    assert evidence[0].candidate_schema_fingerprint == CANDIDATE_SCHEMA_FINGERPRINT
    assert evidence[0].code_commit == CODE_SHA


def test_experiment_reader_does_not_bind_same_spec_to_different_executable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    exact = _experiment_spec("4" * 64)
    wrong_payload = exact.model_dump(mode="python", exclude={"experiment_id"})
    wrong_payload["strategy_executable_fingerprint"] = "7" * 64
    wrong = ExperimentSpec.model_validate(wrong_payload)
    assert exact.experiment_id is not None and wrong.experiment_id is not None
    registry = ExperimentRegistry(path, managed_trust_root=tmp_path)
    plans = _register_formal_attempts(registry, (exact, wrong))

    evidence = ExperimentRegistryOwnerEvidenceReader(
        path,
        managed_trust_root=tmp_path,
    )(
        _job_evidence(
            snapshot_id=exact.dataset_snapshot_id,
            experiment_id=exact.experiment_id,
            formal_plan_id=plans[exact.experiment_id].plan_id,
        ),
        SimpleNamespace(snapshot_id=exact.dataset_snapshot_id),
        NOW,
    )

    assert tuple(item.experiment_id for item in evidence) == (exact.experiment_id,)


def test_experiment_reader_does_not_cross_bind_different_candidate_schema(
    tmp_path: Path,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    exact = _experiment_spec("4" * 64)
    wrong_payload = exact.model_dump(mode="python", exclude={"experiment_id"})
    wrong_payload["candidate_schema_fingerprint"] = "7" * 64
    wrong = ExperimentSpec.model_validate(wrong_payload)
    assert exact.experiment_id is not None and wrong.experiment_id is not None
    registry = ExperimentRegistry(path, managed_trust_root=tmp_path)
    plans = _register_formal_attempts(registry, (exact, wrong))
    job = SimpleNamespace(
        experiment_id=exact.experiment_id,
        formal_plan_id=plans[exact.experiment_id].plan_id,
        snapshot_id=exact.dataset_snapshot_id,
        strategy_spec_fingerprint=STRATEGY_FINGERPRINT,
        strategy_definition_fingerprint=(_strategy_identity().strategy_definition_fingerprint),
        definition_registration_record_hash=(
            _strategy_identity().definition_registration_record_hash
        ),
        strategy_executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        code_commit=CODE_SHA,
    )

    evidence = ExperimentRegistryOwnerEvidenceReader(
        path,
        managed_trust_root=tmp_path,
    )(
        job,
        SimpleNamespace(snapshot_id=exact.dataset_snapshot_id),
        NOW,
    )

    assert tuple(item.experiment_id for item in evidence) == (exact.experiment_id,)


def test_experiment_reader_batch_builds_one_bounded_index_for_many_bundles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    registry = ExperimentRegistry(path, managed_trust_root=tmp_path)
    matching = _experiment_spec("4" * 64)
    specs = [matching]
    for index in range(1, 80):
        payload = matching.model_dump(mode="python", exclude={"experiment_id"})
        payload["dataset_snapshot_id"] = f"{index + 10:064x}"
        payload["parameter_fingerprint"] = f"{index + 100:064x}"
        payload["hypothesis_family"] = f"artifact-owner-{index}"
        specs.append(ExperimentSpec.model_validate(payload))
    plans = _register_formal_attempts(registry, tuple(specs))
    path.chmod(0o600)

    connect_count = 0
    experiment_select_count = 0
    original_connect = catalog_readers_module.sqlite3.connect

    def counted_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        nonlocal connect_count, experiment_select_count
        connect_count += 1
        connection = original_connect(*args, **kwargs)

        def trace(sql: str) -> None:
            nonlocal experiment_select_count
            if "FROM experiment_attempt" in sql:
                experiment_select_count += 1

        connection.set_trace_callback(trace)
        return connection

    monkeypatch.setattr(catalog_readers_module.sqlite3, "connect", counted_connect)
    reader = ExperimentRegistryOwnerEvidenceReader(
        path,
        managed_trust_root=tmp_path,
        max_experiment_rows=100,
    )
    job = _job_evidence(
        snapshot_id=matching.dataset_snapshot_id,
        experiment_id=matching.experiment_id,
        formal_plan_id=plans[matching.experiment_id].plan_id,
    )
    snapshot = SimpleNamespace(snapshot_id=matching.dataset_snapshot_id)

    with reader.batch():
        results = tuple(reader(job, snapshot, NOW) for _bundle in range(25))

    assert all(len(result) == 1 for result in results)
    assert connect_count == 1
    assert experiment_select_count == 1


def test_experiment_reader_rejects_corrupt_attempt_status(tmp_path: Path) -> None:
    path = tmp_path / "experiments.sqlite3"
    spec = _experiment_spec("4" * 64)
    assert spec.experiment_id is not None
    registry = ExperimentRegistry(path, managed_trust_root=tmp_path)
    plans = _register_formal_attempts(registry, (spec,))
    with registry._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE experiment_attempt SET status = ? WHERE experiment_id = ?",
            ("corrupt-status", spec.experiment_id),
        )
        connection.execute("COMMIT")
    path.chmod(0o600)

    with pytest.raises(LabArtifactCatalogIntegrityError, match="status|immutable evidence"):
        ExperimentRegistryOwnerEvidenceReader(
            path,
            managed_trust_root=tmp_path,
        )(
            _job_evidence(
                snapshot_id=spec.dataset_snapshot_id,
                experiment_id=spec.experiment_id,
                formal_plan_id=plans[spec.experiment_id].plan_id,
            ),
            SimpleNamespace(snapshot_id=spec.dataset_snapshot_id),
            NOW,
        )


def test_experiment_reader_fails_closed_on_ambiguous_or_future_matches(
    tmp_path: Path,
) -> None:
    ambiguous_path = tmp_path / "ambiguous.sqlite3"
    first = _experiment_spec("4" * 64)
    second_payload = first.model_dump(mode="python", exclude={"experiment_id"})
    second_payload["parameter_fingerprint"] = "f" * 64
    second = ExperimentSpec.model_validate(second_payload)
    registry = ExperimentRegistry(ambiguous_path, managed_trust_root=tmp_path)
    assert first.experiment_id is not None and second.experiment_id is not None
    plans = _register_formal_attempts(
        registry,
        (first, second),
        registered_at=(NOW - timedelta(minutes=2), NOW - timedelta(minutes=1)),
    )
    ambiguous_path.chmod(0o600)
    job = _job_evidence(
        snapshot_id=first.dataset_snapshot_id,
        experiment_id=first.experiment_id,
        formal_plan_id=plans[first.experiment_id].plan_id,
    )
    snapshot = SimpleNamespace(snapshot_id=first.dataset_snapshot_id)
    matches = ExperimentRegistryOwnerEvidenceReader(
        ambiguous_path,
        managed_trust_root=tmp_path,
    )(job, snapshot, NOW)
    assert tuple(item.experiment_id for item in matches) == (first.experiment_id,)

    future_path = tmp_path / "future.sqlite3"
    future = _experiment_spec("4" * 64)
    assert future.experiment_id is not None
    future_registry = ExperimentRegistry(future_path, managed_trust_root=tmp_path)
    future_plans = _register_formal_attempts(
        future_registry,
        (future,),
        registered_at=(NOW + timedelta(seconds=1),),
    )
    future_path.chmod(0o600)

    with pytest.raises(LabArtifactCatalogIntegrityError, match="future"):
        ExperimentRegistryOwnerEvidenceReader(
            future_path,
            managed_trust_root=tmp_path,
        )(
            _job_evidence(
                snapshot_id=future.dataset_snapshot_id,
                experiment_id=future.experiment_id,
                formal_plan_id=future_plans[future.experiment_id].plan_id,
            ),
            snapshot,
            NOW,
        )


def test_composition_api_constructs_only_real_readonly_owner_readers(tmp_path: Path) -> None:
    lab_path = tmp_path / "lab.sqlite3"
    experiment_path = tmp_path / "experiment.sqlite3"
    dataset_path = tmp_path / "dataset.duckdb"

    composition = build_lab_artifact_owner_reader_composition(
        lab_jobs_path=lab_path,
        lab_jobs_managed_trust_root=tmp_path,
        dataset_authority_path=dataset_path,
        dataset_authority_managed_trust_root=tmp_path,
        experiment_registry_path=experiment_path,
        experiment_registry_managed_trust_root=tmp_path,
        clock=lambda: NOW,
    )

    assert isinstance(composition.job_reader.ledger, LabJobReader)
    assert composition.job_reader.ledger.identity_authority is not None
    assert isinstance(composition.snapshot_reader, DatasetSnapshotAuthorityOwnerEvidenceReader)
    assert isinstance(composition.experiment_reader, ExperimentRegistryOwnerEvidenceReader)
    assert composition.owner_resolver.job_evidence_reader is composition.job_reader
    assert composition.owner_resolver.snapshot_evidence_reader is composition.snapshot_reader
    assert composition.owner_resolver.experiment_evidence_reader is composition.experiment_reader


@pytest.mark.parametrize(
    "reader_type",
    [DatasetSnapshotAuthorityOwnerEvidenceReader, ExperimentRegistryOwnerEvidenceReader],
)
def test_owner_readers_require_explicit_managed_trust_root(
    tmp_path: Path,
    reader_type: type[object],
) -> None:
    path = tmp_path / "authority.sqlite3"
    path.touch(mode=0o600)

    with pytest.raises(TypeError, match="managed_trust_root"):
        reader_type(path)


def test_owner_reader_composition_requires_all_explicit_managed_roots(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="managed_trust_root"):
        build_lab_artifact_owner_reader_composition(
            lab_jobs_path=tmp_path / "lab.sqlite3",
            dataset_authority_path=tmp_path / "dataset.duckdb",
            experiment_registry_path=tmp_path / "experiment.sqlite3",
            clock=lambda: NOW,
        )


@pytest.mark.parametrize(
    "reader_type",
    [DatasetSnapshotAuthorityOwnerEvidenceReader, ExperimentRegistryOwnerEvidenceReader],
)
def test_owner_reader_rejects_relative_authority_path(
    tmp_path: Path,
    reader_type: type[object],
) -> None:
    with pytest.raises(ValueError, match="exact absolute"):
        reader_type(
            Path("relative/authority.sqlite3"),
            managed_trust_root=tmp_path,
        )


@pytest.mark.parametrize(
    ("reader_type", "hazard"),
    [
        (DatasetSnapshotAuthorityOwnerEvidenceReader, "parent-symlink"),
        (DatasetSnapshotAuthorityOwnerEvidenceReader, "final-symlink"),
        (DatasetSnapshotAuthorityOwnerEvidenceReader, "hardlink"),
        (DatasetSnapshotAuthorityOwnerEvidenceReader, "mode"),
        (ExperimentRegistryOwnerEvidenceReader, "parent-symlink"),
        (ExperimentRegistryOwnerEvidenceReader, "final-symlink"),
        (ExperimentRegistryOwnerEvidenceReader, "hardlink"),
        (ExperimentRegistryOwnerEvidenceReader, "mode"),
    ],
)
def test_owner_reader_rejects_unsafe_authority_path(
    tmp_path: Path,
    reader_type: type[object],
    hazard: str,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "authority.sqlite3"
    seed = private / "seed.sqlite3"
    seed.touch(mode=0o600)
    if hazard == "parent-symlink":
        linked = tmp_path / "linked"
        linked.symlink_to(private, target_is_directory=True)
        path = linked / path.name
    elif hazard == "final-symlink":
        path.symlink_to(seed)
    elif hazard == "hardlink":
        os.link(seed, path)
    else:
        path.touch(mode=0o600)
        path.chmod(0o640)

    with pytest.raises(ValueError, match="symlink|hard link|mode|unsafe"):
        reader_type(path, managed_trust_root=tmp_path)


def test_experiment_reader_rejects_parent_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    spec = _experiment_spec("4" * 64)
    assert spec.experiment_id is not None
    registry = ExperimentRegistry(path, managed_trust_root=tmp_path)
    registry.register_hypothesis_family(
        HypothesisFamilyManifest(
            hypothesis_family=spec.hypothesis_family,
            experiment_ids=(spec.experiment_id,),
            search_space_fingerprint="e" * 64,
            metric_definition_fingerprint=spec.metric_definition_fingerprint,
            preregistered_at=NOW - timedelta(minutes=2),
        )
    )
    registry.register_attempt(spec, registered_at=NOW - timedelta(minutes=1))
    path.chmod(0o600)
    reader = ExperimentRegistryOwnerEvidenceReader(
        path,
        managed_trust_root=tmp_path,
    )
    original_connect = catalog_readers_module.sqlite3.connect
    retired = tmp_path / "retired"

    def swap_parent_after_connect(*args: object, **kwargs: object):
        connection = original_connect(*args, **kwargs)
        private.rename(retired)
        private.symlink_to(retired, target_is_directory=True)
        return connection

    monkeypatch.setattr(catalog_readers_module.sqlite3, "connect", swap_parent_after_connect)

    with pytest.raises(BaseExceptionGroup) as captured:
        reader(
            _job_evidence(snapshot_id=spec.dataset_snapshot_id),
            SimpleNamespace(snapshot_id=spec.dataset_snapshot_id),
            NOW,
        )
    leaves = _exception_leaves(captured.value)
    assert leaves
    assert any(
        any(marker in str(leaf) for marker in ("path", "symlink", "changed")) for leaf in leaves
    )


def test_experiment_owner_reader_verifies_identity_after_native_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    spec = _experiment_spec("4" * 64)
    assert spec.experiment_id is not None
    registry = ExperimentRegistry(path, managed_trust_root=tmp_path)
    registry.register_hypothesis_family(
        HypothesisFamilyManifest(
            hypothesis_family=spec.hypothesis_family,
            experiment_ids=(spec.experiment_id,),
            search_space_fingerprint="e" * 64,
            metric_definition_fingerprint=spec.metric_definition_fingerprint,
            preregistered_at=NOW - timedelta(minutes=2),
        )
    )
    registry.register_attempt(spec, registered_at=NOW - timedelta(minutes=1))
    path.chmod(0o600)
    reader = ExperimentRegistryOwnerEvidenceReader(
        path,
        managed_trust_root=tmp_path,
    )
    original_connect = catalog_readers_module.sqlite3.connect
    opened: list[object] = []
    closed: set[int] = set()

    class TrackingConnection(catalog_readers_module.sqlite3.Connection):
        def close(self) -> None:
            super().close()
            closed.add(id(self))

    def connect(*args: object, **kwargs: object):
        kwargs["factory"] = TrackingConnection
        connection = original_connect(*args, **kwargs)
        opened.append(connection)
        return connection

    original_assert_current = reader._path_authority.assert_current

    def fail_after_close() -> None:
        original_assert_current()
        if opened and id(opened[-1]) in closed:
            raise ValueError("owner registry identity changed after close")

    monkeypatch.setattr(catalog_readers_module.sqlite3, "connect", connect)
    monkeypatch.setattr(reader._path_authority, "assert_current", fail_after_close)

    with pytest.raises(ValueError, match="owner registry identity changed after close"):
        reader(
            _job_evidence(snapshot_id=spec.dataset_snapshot_id),
            SimpleNamespace(snapshot_id=spec.dataset_snapshot_id),
            NOW,
        )


def test_experiment_owner_reader_closes_when_row_factory_setup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    reader = ExperimentRegistryOwnerEvidenceReader(path, managed_trust_root=tmp_path)
    setup_error = RuntimeError("owner reader row factory failed")

    class FailingSetupConnection:
        def __init__(self) -> None:
            self.closed = False
            self.in_transaction = False

        @property
        def row_factory(self) -> object | None:
            return None

        @row_factory.setter
        def row_factory(self, _value: object) -> None:
            raise setup_error

        def close(self) -> None:
            self.closed = True

    connection = FailingSetupConnection()
    monkeypatch.setattr(
        reader._path_authority,
        "open_verified_connection",
        lambda _opener: connection,
    )

    with pytest.raises(RuntimeError, match="row factory"):
        reader(
            _job_evidence(),
            SimpleNamespace(snapshot_id="4" * 64),
            NOW,
        )

    assert connection.closed


def test_experiment_owner_reader_routes_business_error_into_verified_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    reader = ExperimentRegistryOwnerEvidenceReader(path, managed_trust_root=tmp_path)
    business_error = RuntimeError("owner schema inspection failed")
    observed_primary: list[BaseException | None] = []

    def record_close(
        connection: object,
        _authority: object,
        *,
        primary_error: BaseException | None = None,
        known_identity_failure: bool = False,
    ) -> None:
        del known_identity_failure
        observed_primary.append(primary_error)
        connection.close()  # type: ignore[attr-defined]

    monkeypatch.setattr(
        catalog_readers_module,
        "_require_experiment_schema",
        lambda _connection: (_ for _ in ()).throw(business_error),
    )
    monkeypatch.setattr(
        artifact_retention_module,
        "close_verified_sqlite_connection",
        record_close,
    )

    with pytest.raises(RuntimeError, match="schema inspection"):
        reader(
            _job_evidence(),
            SimpleNamespace(snapshot_id="4" * 64),
            NOW,
        )

    assert observed_primary == [business_error]
