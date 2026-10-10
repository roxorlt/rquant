"""Original installed authorities; only the unavailable host sealer is synthetic."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.artifact_retention import ArtifactReferenceStore
from rquant.artifact_retention_catalog_authority import bootstrap_retention_catalog_authority
from rquant.experiment_registry import ExperimentRegistry
from rquant.job_center_authority import publish_install_current_job_center_authority, resolve_current_job_center_authority_binding
from rquant.lab_jobs import LabJobStore
from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog
from rquant.minute_backtest_installation import MinuteReplayInstallation, load_minute_replay_installation
from rquant.runtime_credential_sealer_client import (
    RuntimeCredentialControlRequest, RuntimeCredentialRecoveryReceipt, RuntimeCredentialRecoveryRequest,
    RuntimeCredentialSealReceipt, RuntimeCredentialSealRequest,
)
from rquant.runtime_deployment_profile import RuntimeDeploymentProfile, install_runtime_deployment_profile
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.storage.duckdb import DuckDBStore
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_minute_backtest_formal import publication
from tests.unit.test_minute_backtest_producer import NOW


def private_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


def synthetic_sealer(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    transactions: dict[str, tuple[str, ...]] = {}

    def transport(request: object) -> bytes:
        calls.append(request.operation)
        if isinstance(request, RuntimeCredentialRecoveryRequest):
            return canonical_json_bytes(RuntimeCredentialRecoveryReceipt(bundle_generation=request.bundle_generation,
                sealed_instances=request.instances, outcome="none", transaction_id=None).model_dump(mode="json"))
        if isinstance(request, RuntimeCredentialSealRequest):
            assert len(request.credentials) == 1
            transaction_id = hashlib.sha256(canonical_json_bytes(request.model_dump(mode="json"))).hexdigest()
            instances = tuple(sorted(request.credentials))
            transactions[transaction_id] = instances
            return canonical_json_bytes(RuntimeCredentialSealReceipt(operation="begin", transaction_id=transaction_id,
                sealed_instances=instances).model_dump(mode="json"))
        assert isinstance(request, RuntimeCredentialControlRequest)
        return canonical_json_bytes(RuntimeCredentialSealReceipt(operation=request.operation,
            transaction_id=request.transaction_id, sealed_instances=transactions[request.transaction_id]).model_dump(mode="json"))

    monkeypatch.setattr("rquant.runtime_credential_sealer_client._run_helper", transport)
    return calls


@pytest.fixture(scope="module")
def installed_minute(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SimpleNamespace]:
    patch = pytest.MonkeyPatch()
    try:
        # Original test storage fixture; it selects no operational primary writer.
        patch.setattr("rquant.storage.duckdb._settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
        calls = synthetic_sealer(patch)
        root = private_directory(tmp_path_factory.mktemp("minute-installed-authorities"))
        deployment = private_directory(root / "runtime")
        research = private_directory(deployment / "research")
        jobs_path = research / "lab_jobs.sqlite3"
        jobs = LabJobStore(jobs_path)
        jobs.initialize()
        jobs_path.chmod(0o600)
        experiments_path = research / "experiment_registry.sqlite3"
        ExperimentRegistry(experiments_path, managed_trust_root=research)
        experiments_path.chmod(0o600)
        metadata_path = research / "research_ro.duckdb"
        with DuckDBStore(metadata_path) as metadata:
            published, catalog, lake = publication(root, metadata)
        metadata_path.chmod(0o600)
        frozen = published.receipt.frozen
        code_sha = frozen.runtime.producer_commit
        definitions = root / "definitions"
        definitions.chmod(0o700)
        commands = private_directory(research / "commands")
        artifacts = private_directory(research / "final-artifacts")
        retention_id = "artifact-retention.primary.v1"
        retention = private_directory(research / "artifact-retention" / ("svc-" + hashlib.sha256(retention_id.encode()).hexdigest()))
        references = retention / "references.sqlite3"
        ArtifactReferenceStore(references, managed_trust_root=retention)
        references.chmod(0o600)
        retention_authority = bootstrap_retention_catalog_authority(state_root=retention,
            reference_store_path=references, producer_commit=code_sha)
        catalog_id = "artifact-catalog.primary.v1"
        artifact_catalog = private_directory(research / "artifact-catalogs" / ("svc-" + hashlib.sha256(catalog_id.encode()).hexdigest()))
        manifests = (
            RuntimeServiceManifest(service_id="lab-jobs.serving.v1", service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
                plane=RuntimeServicePlane.RESEARCH, interval_seconds=30, stale_after_seconds=120, producer_commit=code_sha,
                settings={"lab_jobs_path": str(jobs_path), "authority_root": str(research / "serving-authorities" / "lab-jobs")}),
            RuntimeServiceManifest(service_id=retention_id, service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
                plane=RuntimeServicePlane.RESEARCH, interval_seconds=300, stale_after_seconds=900, producer_commit=code_sha,
                settings={"managed_root": str(artifacts), "state_root": str(retention), "reference_store_path": str(references),
                    "catalog_authority_root": str(retention_authority.root), "recovery_publication_root": str(root / "recovery-publication"),
                    "recovery_restore_root": str(root / "recovery-restore")}),
            RuntimeServiceManifest(service_id=catalog_id, service_kind=RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
                plane=RuntimeServicePlane.RESEARCH, interval_seconds=30, stale_after_seconds=120, producer_commit=code_sha,
                settings={"artifact_root": str(artifacts), "state_root": str(artifact_catalog), "research_root": str(research),
                    "lab_jobs_path": str(jobs_path), "dataset_authority_path": str(metadata_path),
                    "experiment_registry_path": str(experiments_path), "definition_registry_root": str(definitions),
                    "location_id": "synthetic-offline-primary", "failure_domain": "synthetic-offline-local"}),
        )
        profile = RuntimeDeploymentProfile(producer_commit=code_sha, production_runtime_root=str(deployment),
            manifests=manifests, capability_environment={manifest.service_id: () for manifest in manifests})
        receipt = install_runtime_deployment_profile(profile, runtime_root=deployment, environ={},
            schema_bootstrap_reason="C6 offline synthetic installed fixture; credential sealer transport is synthetic")
        binding = resolve_current_job_center_authority_binding(deployment, expected_code_sha=code_sha,
            runtime_root=research, lab_jobs_path=jobs_path, command_spool_path=commands, final_artifact_root=artifacts)
        authority = publish_install_current_job_center_authority(code_sha=code_sha, current_code_sha=lambda: code_sha,
            runtime_deployment_root=deployment, runtime_root=binding.runtime_root, lab_jobs_path=binding.lab_jobs_path,
            command_spool_path=binding.command_spool_path, final_artifact_root=binding.final_artifact_root,
            definition_registry_root=binding.definition_registry_root, experiment_registry_path=binding.experiment_registry_path,
            dataset_authority_path=binding.dataset_authority_path, catalog_authority_root=binding.catalog_authority_root,
            catalog_authority_receipt_path=binding.catalog_authority_receipt_path, deployment_profile_id=binding.deployment_profile_id,
            deployment_generation_hash=binding.deployment_generation_hash)
        snapshot_root = private_directory(root / "metadata-copies")
        with ImmutableDuckDBMetadataCatalog.open(metadata_path, forbidden_paths=(), snapshot_root=snapshot_root) as metadata:
            metadata_identity = metadata.descriptor
        installation = MinuteReplayInstallation(code_sha=code_sha, deployment_profile_id=profile.profile_id,
            deployment_generation_hash=receipt.generation_hash, authority_manifest_hash=authority.manifest_hash,
            runtime_deployment_root=deployment, runtime_root=research, lab_jobs_path=jobs_path, command_spool_path=commands,
            final_artifact_root=artifacts, metadata_identity=metadata_identity, snapshot_root=snapshot_root,
            research_lake_root=lake, catalog=catalog)
        installation_path = root / "installed-minute.json"
        installation_path.write_text(installation.model_dump_json(exclude_computed_fields=True))
        installation_path.chmod(0o600)
        now = [NOW]
        writer = load_minute_replay_installation(installation_path, expected_code_sha=code_sha, writable=True, clock=lambda: now[0])
        readonly = load_minute_replay_installation(installation_path, expected_code_sha=code_sha, clock=lambda: now[0])
        assert calls == ["recover", "begin", "commit"]
        yield SimpleNamespace(root=root, profile=installation, path=installation_path, published=published,
            writer=writer, readonly=readonly, jobs=jobs, now=now, sealer_calls=calls)
    finally:
        patch.undo()
