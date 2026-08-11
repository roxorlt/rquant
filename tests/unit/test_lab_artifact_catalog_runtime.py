from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from rquant.lab_artifact_catalog import (
    LabArtifactCatalogIntegrityError,
    LabArtifactDurableOwners,
)
from rquant.lab_artifact_catalog_runtime import (
    DatasetSnapshotOwnerEvidence,
    DefinitionOwnerEvidence,
    ExperimentOwnerEvidence,
    LabJobOwnerEvidence,
    TrustedLabArtifactOwnerResolver,
)
from rquant.lab_worker import (
    CURRENT_CONTENT_DIGEST_ALGORITHM,
    LabShardArtifactManifest,
    LabShardResultManifest,
)

NOW = datetime(2026, 8, 1, 3, 0, tzinfo=UTC)
JOB_ID = UUID("11111111-1111-4111-8111-111111111111")
SHARD_ID = UUID("22222222-2222-4222-8222-222222222222")
CLAIM_TOKEN = UUID("33333333-3333-4333-8333-333333333333")
SPEC_HASH = "a" * 64
PAYLOAD_HASH = "b" * 64
PLAN_HASH = "c" * 64
SNAPSHOT_ID = "d" * 64
EXPERIMENT_ID = "e" * 64
AUDIT_RUN_ID = "9" * 64
STRATEGY_FINGERPRINT = "f" * 64
EXECUTABLE_FINGERPRINT = "8" * 64
CANDIDATE_SCHEMA_FINGERPRINT = "7" * 64
CODE_SHA = "1" * 40
BINDING_HASH = "4" * 64
EXPERIMENT_ATTEMPT_ID = "6" * 64
STRATEGY_EXECUTION_ID = "5" * 64
FORMAL_PLAN_ID = "0" * 64
DEFINITION_FINGERPRINT = "2" * 64
DEFINITION_RECORD_HASH = "1" * 64


def _manifest() -> LabShardResultManifest:
    artifact = LabShardArtifactManifest(
        name="result",
        file_name="000-result.parquet",
        row_count=1,
        columns=("ts_code",),
        file_size=10,
        file_sha256="2" * 64,
        content_sha256="3" * 64,
    )
    return LabShardResultManifest(
        worker_code_sha=CODE_SHA,
        content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
        job_id=JOB_ID,
        shard_id=SHARD_ID,
        claim_token=CLAIM_TOKEN,
        claim_generation=3,
        scheduler_fencing_token=7,
        spec_hash=SPEC_HASH,
        payload_hash=PAYLOAD_HASH,
        plan_hash=PLAN_HASH,
        adapter_id="n-shape",
        adapter_version="1",
        experiment_id=EXPERIMENT_ID,
        experiment_attempt_identity=EXPERIMENT_ATTEMPT_ID,
        strategy_execution_identity_hash=STRATEGY_EXECUTION_ID,
        strategy_spec_fingerprint=STRATEGY_FINGERPRINT,
        strategy_executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        artifacts=(artifact,),
    )


def _job_evidence(*, recorded_at: datetime = NOW - timedelta(minutes=3)) -> LabJobOwnerEvidence:
    manifest = _manifest()
    return LabJobOwnerEvidence(
        job_id=JOB_ID,
        shard_id=SHARD_ID,
        result_manifest_hash=manifest.manifest_hash,
        spec_hash=SPEC_HASH,
        plan_hash=PLAN_HASH,
        snapshot_id=SNAPSHOT_ID,
        snapshot_binding_hash=BINDING_HASH,
        audit_run_id=AUDIT_RUN_ID,
        experiment_id=EXPERIMENT_ID,
        experiment_attempt_identity=EXPERIMENT_ATTEMPT_ID,
        formal_plan_id=FORMAL_PLAN_ID,
        strategy_id="n_shape",
        strategy_version=1,
        strategy_execution_identity_hash=STRATEGY_EXECUTION_ID,
        strategy_spec_fingerprint=STRATEGY_FINGERPRINT,
        strategy_definition_fingerprint=DEFINITION_FINGERPRINT,
        definition_registration_record_hash=DEFINITION_RECORD_HASH,
        definition_registered_at=NOW - timedelta(minutes=5),
        definition_available_at=NOW - timedelta(minutes=4),
        strategy_executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        code_commit=CODE_SHA,
        recorded_at=recorded_at,
    )


def _snapshot_evidence(
    *, completed_at: datetime = NOW - timedelta(minutes=2)
) -> DatasetSnapshotOwnerEvidence:
    return DatasetSnapshotOwnerEvidence(
        snapshot_id=SNAPSHOT_ID,
        binding_hash=BINDING_HASH,
        audit_run_id=AUDIT_RUN_ID,
        code_commit=CODE_SHA,
        status="ready",
        completed_at=completed_at,
        binding_completed_at=completed_at,
        audit_completed_at=completed_at,
    )


def _experiment_evidence(
    *, registered_at: datetime = NOW - timedelta(minutes=1)
) -> ExperimentOwnerEvidence:
    return ExperimentOwnerEvidence(
        experiment_id=EXPERIMENT_ID,
        formal_plan_id=FORMAL_PLAN_ID,
        dataset_snapshot_id=SNAPSHOT_ID,
        strategy_spec_fingerprint=STRATEGY_FINGERPRINT,
        strategy_definition_fingerprint=DEFINITION_FINGERPRINT,
        definition_registration_record_hash=DEFINITION_RECORD_HASH,
        strategy_executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        code_commit=CODE_SHA,
        registered_at=registered_at,
    )


def _definition_evidence() -> DefinitionOwnerEvidence:
    return DefinitionOwnerEvidence(
        strategy_id="n_shape",
        strategy_version=1,
        strategy_spec_fingerprint=STRATEGY_FINGERPRINT,
        strategy_definition_fingerprint=DEFINITION_FINGERPRINT,
        strategy_executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        definition_registration_record_hash=DEFINITION_RECORD_HASH,
        producer_code_commit=CODE_SHA,
        registered_at=NOW - timedelta(minutes=5),
        available_at=NOW - timedelta(minutes=4),
    )


def _resolver(
    *,
    jobs: tuple[LabJobOwnerEvidence, ...] | None = None,
    snapshots: tuple[DatasetSnapshotOwnerEvidence, ...] | None = None,
    experiments: tuple[ExperimentOwnerEvidence, ...] | None = None,
    definitions: tuple[DefinitionOwnerEvidence, ...] | None = None,
) -> TrustedLabArtifactOwnerResolver:
    return TrustedLabArtifactOwnerResolver(
        job_evidence_reader=lambda _manifest, _cutoff: (_job_evidence(),) if jobs is None else jobs,
        snapshot_evidence_reader=lambda _job, _cutoff: (
            (_snapshot_evidence(),) if snapshots is None else snapshots
        ),
        experiment_evidence_reader=lambda _job, _snapshot, _cutoff: (
            (_experiment_evidence(),) if experiments is None else experiments
        ),
        definition_evidence_reader=lambda _job, _experiment, _cutoff: (
            (_definition_evidence(),) if definitions is None else definitions
        ),
        clock=lambda: NOW,
    )


def test_trusted_owner_resolver_requires_three_matching_readonly_evidence_sources() -> None:
    owners = _resolver()(_manifest())

    assert owners == LabArtifactDurableOwners(
        job_id=JOB_ID,
        spec_hash=SPEC_HASH,
        plan_hash=PLAN_HASH,
        snapshot_id=SNAPSHOT_ID,
        experiment_id=EXPERIMENT_ID,
        audit_run_id=AUDIT_RUN_ID,
    )


@pytest.mark.parametrize(
    ("field", "values"),
    [
        ("jobs", ()),
        ("snapshots", ()),
        ("experiments", ()),
        ("jobs", (_job_evidence(), _job_evidence())),
        ("snapshots", (_snapshot_evidence(), _snapshot_evidence())),
        ("experiments", (_experiment_evidence(), _experiment_evidence())),
    ],
)
def test_trusted_owner_resolver_fails_closed_on_missing_or_ambiguous_evidence(
    field: str,
    values: tuple[object, ...],
) -> None:
    kwargs = {field: values}

    with pytest.raises(LabArtifactCatalogIntegrityError, match="missing|ambiguous"):
        _resolver(**kwargs)(_manifest())  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "values"),
    [
        ("jobs", (_job_evidence(recorded_at=NOW + timedelta(seconds=1)),)),
        (
            "snapshots",
            (_snapshot_evidence(completed_at=NOW + timedelta(seconds=1)),),
        ),
        (
            "experiments",
            (_experiment_evidence(registered_at=NOW + timedelta(seconds=1)),),
        ),
    ],
)
def test_trusted_owner_resolver_rejects_future_evidence(
    field: str,
    values: tuple[object, ...],
) -> None:
    kwargs = {field: values}

    with pytest.raises(LabArtifactCatalogIntegrityError, match="future"):
        _resolver(**kwargs)(_manifest())  # type: ignore[arg-type]


def test_trusted_owner_resolver_rejects_cross_source_identity_disagreement() -> None:
    wrong = _experiment_evidence().model_copy(update={"dataset_snapshot_id": "9" * 64})

    with pytest.raises(LabArtifactCatalogIntegrityError, match="conflict"):
        _resolver(experiments=(wrong,))(_manifest())


def test_trusted_owner_resolver_rejects_executable_fingerprint_disagreement() -> None:
    wrong = _experiment_evidence().model_copy(update={"strategy_executable_fingerprint": "7" * 64})

    with pytest.raises(LabArtifactCatalogIntegrityError, match="conflict"):
        _resolver(experiments=(wrong,))(_manifest())


def test_trusted_owner_resolver_rejects_candidate_schema_disagreement() -> None:
    wrong = _experiment_evidence().model_copy(update={"candidate_schema_fingerprint": "6" * 64})

    with pytest.raises(LabArtifactCatalogIntegrityError, match="conflict"):
        _resolver(experiments=(wrong,))(_manifest())
