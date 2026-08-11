from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from rquant.artifact_retention import ArtifactReferenceStore
from rquant.artifact_retention_catalog_authority import (
    bootstrap_retention_catalog_authority,
)
from rquant.experiment_registry import ExperimentRegistry
from rquant.job_center_authority import (
    JobCenterAuthorityIntegrityError,
    install_job_center_authority,
    load_job_center_authority,
    publish_install_current_job_center_authority,
    publish_job_center_authority_candidate,
    resolve_current_job_center_authority_binding,
)
from rquant.lab_jobs import LabJobStore
from rquant.runtime_definition_bootstrap import (
    bootstrap_builtin_definitions,
    plan_builtin_definitions,
)
from rquant.runtime_deployment_profile import (
    RuntimeDeploymentProfile,
    install_runtime_deployment_profile,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

from .test_lab_jobs import NOW

CODE_SHA = "1" * 40
DEPLOYMENT_PROFILE_ID = "2" * 64
DEPLOYMENT_GENERATION_HASH = "3" * 64


def _private_directory(path: Path) -> Path:
    path.mkdir(parents=True, mode=0o700)
    path.chmod(0o700)
    return path


def _authorities(tmp_path: Path) -> dict[str, object]:
    runtime_root = _private_directory(tmp_path / "research")
    jobs = runtime_root / "lab_jobs.sqlite3"
    LabJobStore(jobs).initialize()
    jobs.chmod(0o600)
    experiments = runtime_root / "experiment_registry.sqlite3"
    ExperimentRegistry(experiments, managed_trust_root=runtime_root)
    experiments.chmod(0o600)
    definitions = _private_directory(tmp_path / "definitions")
    plan = plan_builtin_definitions(producer_commit=CODE_SHA)
    bootstrap_builtin_definitions(
        definitions,
        producer_commit=CODE_SHA,
        registered_at=NOW,
        available_at=NOW,
        expected_plan_id=plan.plan_id,
    )
    retention = _private_directory(runtime_root / "artifact-retention")
    references = retention / "references.sqlite3"
    ArtifactReferenceStore(references, managed_trust_root=retention)
    references.chmod(0o600)
    catalog_authority = bootstrap_retention_catalog_authority(
        state_root=retention,
        reference_store_path=references,
        producer_commit=CODE_SHA,
    )
    commands = _private_directory(runtime_root / "commands")
    artifacts = _private_directory(runtime_root / "final-artifacts")
    dataset = runtime_root / "research_ro.duckdb"
    dataset.touch(mode=0o600)
    return {
        "runtime_deployment_root": tmp_path,
        "runtime_root": runtime_root,
        "lab_jobs_path": jobs,
        "command_spool_path": commands,
        "final_artifact_root": artifacts,
        "definition_registry_root": definitions,
        "experiment_registry_path": experiments,
        "dataset_authority_path": dataset,
        "catalog_authority_root": catalog_authority.root,
        "catalog_authority_receipt_path": catalog_authority.current_receipt_path,
        "deployment_profile_id": DEPLOYMENT_PROFILE_ID,
        "deployment_generation_hash": DEPLOYMENT_GENERATION_HASH,
    }


def _publish_and_install(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    paths = _authorities(tmp_path)
    staging = _private_directory(tmp_path / "staging")
    candidate = publish_job_center_authority_candidate(
        staging / "candidate.json",
        code_sha=CODE_SHA,
        **paths,
    )
    installed = install_job_center_authority(
        candidate,
        target=paths["runtime_root"] / "job-center-authority.json",
        expected_code_sha=CODE_SHA,
        expected_runtime_root=paths["runtime_root"],
        expected_runtime_deployment_root=paths["runtime_deployment_root"],
        expected_deployment_profile_id=DEPLOYMENT_PROFILE_ID,
        expected_deployment_generation_hash=DEPLOYMENT_GENERATION_HASH,
    )
    return installed, paths


def test_publish_install_load_binds_code_generation_and_physical_authorities(
    tmp_path: Path,
) -> None:
    installed, paths = _publish_and_install(tmp_path)

    loaded = load_job_center_authority(
        installed,
        expected_code_sha=CODE_SHA,
        **paths,
    )

    assert loaded.code_sha == CODE_SHA
    assert loaded.runtime_root == paths["runtime_root"]
    assert {binding.name for binding in loaded.authorities} == {
        "artifact_catalog",
        "definition_registry",
        "experiment_registry",
        "job_center",
    }
    assert all(binding.generation_id for binding in loaded.authorities)
    assert loaded.manifest_hash
    assert installed.stat().st_mode & 0o777 == 0o600


def test_current_authority_chain_guards_publish_install_and_exact_reload(
    tmp_path: Path,
) -> None:
    paths = _authorities(tmp_path)
    guard_calls: list[str] = []

    def current_sha() -> str:
        guard_calls.append(CODE_SHA)
        return CODE_SHA

    loaded = publish_install_current_job_center_authority(
        code_sha=CODE_SHA,
        current_code_sha=current_sha,
        **paths,
    )

    assert loaded == load_job_center_authority(
        paths["runtime_root"] / "job-center-authority.json",
        expected_code_sha=CODE_SHA,
        **paths,
    )
    assert len(guard_calls) >= 3
    assert not (paths["runtime_root"] / ".job-center-authority.candidate.json").exists()


def test_current_authority_chain_rejects_stale_runtime_before_publication(
    tmp_path: Path,
) -> None:
    paths = _authorities(tmp_path)

    with pytest.raises(JobCenterAuthorityIntegrityError, match="current code SHA"):
        publish_install_current_job_center_authority(
            code_sha=CODE_SHA,
            current_code_sha=lambda: "2" * 40,
            **paths,
        )

    assert not (paths["runtime_root"] / "job-center-authority.json").exists()
    assert not (paths["runtime_root"] / ".job-center-authority.candidate.json").exists()


def test_current_authority_chain_restores_previous_current_after_post_install_sha_drift(
    tmp_path: Path,
) -> None:
    paths = _authorities(tmp_path)
    current = paths["runtime_root"] / "job-center-authority.json"
    publish_install_current_job_center_authority(
        code_sha=CODE_SHA,
        current_code_sha=lambda: CODE_SHA,
        **paths,
    )
    previous = current.read_bytes()
    next_sha = "2" * 40
    observed = iter((next_sha, next_sha, CODE_SHA))

    with pytest.raises(JobCenterAuthorityIntegrityError, match="current code SHA"):
        publish_install_current_job_center_authority(
            code_sha=next_sha,
            current_code_sha=lambda: next(observed),
            **paths,
        )

    assert current.read_bytes() == previous
    assert not (paths["runtime_root"] / ".job-center-authority.candidate.json").exists()


def test_current_deployment_profile_resolves_exact_job_center_authorities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _CredentialTransaction:
        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    class _CredentialRecovery:
        outcome = "none"
        transaction_id = None

    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda _credentials: _CredentialTransaction(),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._recover_runtime_credentials",
        lambda **_kwargs: _CredentialRecovery(),
    )
    deployment_root = tmp_path / "production-runtime"
    research = deployment_root / "research"
    definitions = tmp_path / "definitions"
    jobs = research / "lab_jobs.sqlite3"
    final_artifacts = research / "final-artifacts"
    catalog_service_id = "artifact-catalog.primary.v1"
    catalog_instance = "svc-" + hashlib.sha256(catalog_service_id.encode()).hexdigest()
    catalog = research / "artifact-catalogs" / catalog_instance
    retention_service_id = "artifact-retention.primary.v1"
    retention_instance = "svc-" + hashlib.sha256(retention_service_id.encode()).hexdigest()
    retention = research / "artifact-retention" / retention_instance
    experiments = research / "experiment_registry.sqlite3"
    dataset = research / "research_ro.duckdb"
    research.mkdir(parents=True, mode=0o700)
    research.chmod(0o700)
    definitions.mkdir(mode=0o700)
    definitions.chmod(0o700)
    definition_plan = plan_builtin_definitions(producer_commit=CODE_SHA)
    bootstrap_builtin_definitions(
        definitions,
        producer_commit=CODE_SHA,
        registered_at=NOW,
        available_at=NOW,
        expected_plan_id=definition_plan.plan_id,
    )
    LabJobStore(jobs).initialize()
    jobs.chmod(0o600)
    ExperimentRegistry(experiments, managed_trust_root=research)
    experiments.chmod(0o600)
    dataset.touch(mode=0o600)
    commands = research / "commands"
    commands.mkdir(mode=0o700)
    commands.chmod(0o700)
    final_artifacts.mkdir(mode=0o700)
    final_artifacts.chmod(0o700)
    catalog.mkdir(parents=True, mode=0o700)
    catalog.chmod(0o700)
    retention.mkdir(parents=True, mode=0o700)
    retention.chmod(0o700)
    references = retention / "references.sqlite3"
    ArtifactReferenceStore(references, managed_trust_root=retention)
    references.chmod(0o600)
    bootstrap_retention_catalog_authority(
        state_root=retention,
        reference_store_path=references,
        producer_commit=CODE_SHA,
    )
    manifests = (
        RuntimeServiceManifest(
            service_id="lab-jobs.serving.v1",
            service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=30,
            stale_after_seconds=120,
            producer_commit=CODE_SHA,
            settings={
                "lab_jobs_path": str(jobs),
                "authority_root": str(research / "serving-authorities" / "lab-jobs"),
            },
        ),
        RuntimeServiceManifest(
            service_id=retention_service_id,
            service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=300,
            stale_after_seconds=900,
            producer_commit=CODE_SHA,
            settings={
                "managed_root": str(final_artifacts),
                "state_root": str(retention),
                "reference_store_path": str(references),
                "catalog_authority_root": str(retention / "catalog-authority"),
                "recovery_publication_root": str(tmp_path / "recovery-publication"),
                "recovery_restore_root": str(tmp_path / "recovery-restore"),
            },
        ),
        RuntimeServiceManifest(
            service_id=catalog_service_id,
            service_kind=RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=30,
            stale_after_seconds=120,
            producer_commit=CODE_SHA,
            settings={
                "artifact_root": str(final_artifacts),
                "state_root": str(catalog),
                "research_root": str(research),
                "lab_jobs_path": str(jobs),
                "dataset_authority_path": str(dataset),
                "experiment_registry_path": str(experiments),
                "definition_registry_root": str(definitions),
                "location_id": "test-primary",
                "failure_domain": "test-local",
            },
        ),
    )
    profile = RuntimeDeploymentProfile(
        producer_commit=CODE_SHA,
        production_runtime_root=str(deployment_root),
        manifests=manifests,
        capability_environment={manifest.service_id: () for manifest in manifests},
    )
    receipt = install_runtime_deployment_profile(
        profile,
        runtime_root=deployment_root,
        environ={},
        schema_bootstrap_reason="Job Center authority resolver test",
    )
    legacy_references = catalog / "references.sqlite3"
    legacy_references.write_bytes(b"deprecated catalog-local reference metadata")
    legacy_references.chmod(0o600)

    binding = resolve_current_job_center_authority_binding(
        deployment_root,
        expected_code_sha=CODE_SHA,
        runtime_root=research,
        lab_jobs_path=jobs,
        command_spool_path=commands,
        final_artifact_root=final_artifacts,
    )

    assert binding.deployment_profile_id == profile.profile_id
    assert binding.deployment_generation_hash == receipt.generation_hash
    assert binding.definition_registry_root == definitions
    assert binding.experiment_registry_path == experiments
    assert binding.dataset_authority_path == dataset
    assert binding.catalog_authority_root == retention / "catalog-authority"
    assert binding.catalog_authority_receipt_path == (
        retention / "catalog-authority" / "current.json"
    )
    assert legacy_references.exists()
    quarantine = tuple((retention / "legacy-catalog-quarantine").glob("*.json"))
    assert len(quarantine) == 1
    authority = load_job_center_authority(
        publish_install_current_job_center_authority(
            code_sha=CODE_SHA,
            current_code_sha=lambda: CODE_SHA,
            runtime_deployment_root=deployment_root,
            runtime_root=binding.runtime_root,
            lab_jobs_path=binding.lab_jobs_path,
            command_spool_path=binding.command_spool_path,
            final_artifact_root=binding.final_artifact_root,
            definition_registry_root=binding.definition_registry_root,
            experiment_registry_path=binding.experiment_registry_path,
            dataset_authority_path=binding.dataset_authority_path,
            catalog_authority_root=binding.catalog_authority_root,
            catalog_authority_receipt_path=binding.catalog_authority_receipt_path,
            deployment_profile_id=binding.deployment_profile_id,
            deployment_generation_hash=binding.deployment_generation_hash,
        ).runtime_root
        / "job-center-authority.json",
        expected_code_sha=CODE_SHA,
        runtime_deployment_root=deployment_root,
        runtime_root=binding.runtime_root,
        lab_jobs_path=binding.lab_jobs_path,
        command_spool_path=binding.command_spool_path,
        final_artifact_root=binding.final_artifact_root,
        definition_registry_root=binding.definition_registry_root,
        experiment_registry_path=binding.experiment_registry_path,
        dataset_authority_path=binding.dataset_authority_path,
        catalog_authority_root=binding.catalog_authority_root,
        catalog_authority_receipt_path=binding.catalog_authority_receipt_path,
        deployment_profile_id=binding.deployment_profile_id,
        deployment_generation_hash=binding.deployment_generation_hash,
    )
    artifact_binding = next(
        item for item in authority.authorities if item.name == "artifact_catalog"
    )
    assert all(identity.path != legacy_references for identity in artifact_binding.identities)


def test_loader_fails_closed_for_old_sha_tamper_and_replaced_authority_inode(
    tmp_path: Path,
) -> None:
    installed, paths = _publish_and_install(tmp_path)

    with pytest.raises(JobCenterAuthorityIntegrityError, match="code SHA"):
        load_job_center_authority(
            installed,
            expected_code_sha="2" * 40,
            **paths,
        )

    original = installed.read_bytes()
    installed.chmod(0o600)
    installed.write_bytes(original.replace(CODE_SHA.encode(), ("2" * 40).encode(), 1))
    with pytest.raises(JobCenterAuthorityIntegrityError, match="canonical|hash|code SHA"):
        load_job_center_authority(installed, expected_code_sha=CODE_SHA, **paths)
    installed.write_bytes(original)
    installed.chmod(0o600)

    replacement = paths["experiment_registry_path"].with_suffix(".replacement")
    replacement.write_bytes(paths["experiment_registry_path"].read_bytes())
    replacement.chmod(0o600)
    os.replace(replacement, paths["experiment_registry_path"])
    with pytest.raises(JobCenterAuthorityIntegrityError, match="identity|generation"):
        load_job_center_authority(installed, expected_code_sha=CODE_SHA, **paths)


def test_installer_failure_keeps_previous_authority_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed, paths = _publish_and_install(tmp_path)
    original = installed.read_bytes()
    staging = _private_directory(tmp_path / "next-staging")
    candidate = publish_job_center_authority_candidate(
        staging / "candidate.json",
        code_sha=CODE_SHA,
        **paths,
    )

    def fail_before_replace(_source: Path, _target: Path) -> None:
        raise OSError("injected atomic install failure")

    monkeypatch.setattr(
        "rquant.job_center_authority._atomic_replace",
        fail_before_replace,
    )
    with pytest.raises(OSError, match="atomic install failure"):
        install_job_center_authority(
            candidate,
            target=installed,
            expected_code_sha=CODE_SHA,
            expected_runtime_root=paths["runtime_root"],
            expected_runtime_deployment_root=paths["runtime_deployment_root"],
            expected_deployment_profile_id=DEPLOYMENT_PROFILE_ID,
            expected_deployment_generation_hash=DEPLOYMENT_GENERATION_HASH,
        )

    assert installed.read_bytes() == original
