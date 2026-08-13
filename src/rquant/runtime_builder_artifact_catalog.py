"""Runtime builder for the bounded Strategy Lab artifact catalog."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import Field, StrictFloat, StrictInt, field_validator, model_validator

from rquant.lab_artifact_catalog import LabArtifactCatalogRegistrar
from rquant.lab_artifact_catalog_readers import build_lab_artifact_owner_reader_composition
from rquant.lab_artifact_catalog_runtime import (
    LabArtifactCatalogRuntime,
    LabArtifactDiscoveryQueue,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    ArtifactTerminalOwnerStep,
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
)

if TYPE_CHECKING:
    from rquant.runtime_artifact_terminal_lifecycle import ProductionArtifactTerminalLifecycle


class ArtifactCatalogSettings(RuntimeContractModel):
    research_root: Path
    artifact_root: Path
    state_root: Path
    lab_jobs_path: Path
    dataset_authority_path: Path
    experiment_registry_path: Path
    location_id: str = Field(min_length=1)
    failure_domain: str = Field(min_length=1)
    max_bundles: StrictInt = Field(default=32, gt=0, le=10_000)
    max_discovery_entries: StrictInt = Field(default=128, gt=0, le=100_000)
    max_directories_per_step: StrictInt = Field(default=8, gt=0, le=10_000)
    max_discovery_seconds: float = Field(default=1.0, gt=0, le=60)
    max_experiment_rows: StrictInt = Field(default=100_000, gt=0, le=1_000_000)
    max_artifact_file_bytes: StrictInt = Field(
        default=2 * 1024**3,
        gt=0,
        le=1024**4,
    )
    max_bundle_bytes: StrictInt = Field(
        default=8 * 1024**3,
        gt=0,
        le=4 * 1024**4,
    )
    max_step_bytes: StrictInt = Field(
        default=16 * 1024**3,
        gt=0,
        le=16 * 1024**4,
    )
    max_artifact_file_verification_seconds: StrictFloat | None = Field(
        default=None,
        gt=0,
        le=3_600,
    )
    max_bundle_verification_seconds: StrictFloat | None = Field(
        default=None,
        gt=0,
        le=3_600,
    )
    max_verification_seconds: StrictFloat = Field(default=30.0, gt=0, le=3_600)

    @field_validator(
        "artifact_root",
        "research_root",
        "state_root",
        "lab_jobs_path",
        "dataset_authority_path",
        "experiment_registry_path",
    )
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("artifact catalog paths must be absolute")
        if value != Path(value.absolute()):
            raise ValueError("artifact catalog paths must be normalized")
        return value

    @model_validator(mode="after")
    def validate_nested_verification_budgets(self) -> ArtifactCatalogSettings:
        if self.max_artifact_file_bytes > self.max_bundle_bytes:
            raise ValueError("artifact file byte budget cannot exceed bundle byte budget")
        if self.max_bundle_bytes > self.max_step_bytes:
            raise ValueError("bundle byte budget cannot exceed step byte budget")
        file_seconds = self.max_artifact_file_verification_seconds
        bundle_seconds = self.max_bundle_verification_seconds
        if file_seconds is None:
            file_seconds = self.max_verification_seconds
            object.__setattr__(self, "max_artifact_file_verification_seconds", file_seconds)
        if bundle_seconds is None:
            bundle_seconds = self.max_verification_seconds
            object.__setattr__(self, "max_bundle_verification_seconds", bundle_seconds)
        if file_seconds > bundle_seconds:
            raise ValueError("artifact file time budget cannot exceed bundle time budget")
        if bundle_seconds > self.max_verification_seconds:
            raise ValueError("bundle time budget cannot exceed step time budget")
        return self


def artifact_catalog_builder(
    *,
    clock: Callable[[], datetime],
    open_artifact_terminal_lifecycle: (
        Callable[[], ProductionArtifactTerminalLifecycle] | None
    ) = None,
) -> RuntimeServiceBuilder:
    """Build the catalog scanner and its sole catalog-to-retention IPC producer."""

    def build(manifest: RuntimeServiceManifest):
        if manifest.service_kind is not RuntimeServiceKind.LAB_ARTIFACT_CATALOG:
            raise ValueError("runtime service kind must be lab artifact catalog")
        if manifest.plane is not RuntimeServicePlane.RESEARCH:
            raise ValueError("artifact catalog must run on the research plane")
        settings = ArtifactCatalogSettings.model_validate(dict(manifest.settings))
        if open_artifact_terminal_lifecycle is None:
            raise RuntimeError("artifact catalog requires the production terminal lifecycle")
        lifecycle = open_artifact_terminal_lifecycle()
        try:
            if lifecycle.catalog_registration_sink is None:
                raise RuntimeError("artifact catalog lifecycle registration capability is missing")
            composition = build_lab_artifact_owner_reader_composition(
                lab_jobs_path=settings.lab_jobs_path,
                lab_jobs_managed_trust_root=settings.research_root,
                dataset_authority_path=settings.dataset_authority_path,
                dataset_authority_managed_trust_root=settings.research_root,
                experiment_registry_path=settings.experiment_registry_path,
                experiment_registry_managed_trust_root=settings.research_root,
                max_experiment_rows=settings.max_experiment_rows,
                clock=clock,
            )
            registrar = LabArtifactCatalogRegistrar(
                artifact_root=settings.artifact_root,
                reference_store=lifecycle.catalog_registration_sink,
                owner_resolver=composition.owner_resolver,
                location_id=settings.location_id,
                failure_domain=settings.failure_domain,
                clock=clock,
                max_artifact_file_bytes=settings.max_artifact_file_bytes,
                max_bundle_bytes=settings.max_bundle_bytes,
                max_step_bytes=settings.max_step_bytes,
                max_artifact_file_verification_seconds=(
                    settings.max_artifact_file_verification_seconds
                ),
                max_bundle_verification_seconds=settings.max_bundle_verification_seconds,
                max_verification_seconds=settings.max_verification_seconds,
            )
            discovery = LabArtifactDiscoveryQueue(
                settings.state_root / "discovery.sqlite3",
                managed_trust_root=settings.state_root,
            )
            runtime = LabArtifactCatalogRuntime(
                registrar=registrar,
                discovery_queue=discovery,
                max_bundles=settings.max_bundles,
                max_discovery_entries=settings.max_discovery_entries,
                max_directories_per_step=settings.max_directories_per_step,
                max_discovery_seconds=settings.max_discovery_seconds,
                lock_path=settings.state_root / "catalog.lock",
                clock=clock,
            )

            def step() -> RuntimeStepResult:
                result = runtime.run_step()
                return RuntimeStepResult(
                    input_sequence=result.scan_generation,
                    output_sequence=result.scan_generation,
                    processed_count=len(result.processed_paths),
                    backlog_count=result.pending_bundles,
                    source_generations={"artifact_catalog": canonical_sha256(result)},
                )
        except BaseException:
            lifecycle.close()
            raise
        return ArtifactTerminalOwnerStep(
            step=step,
            artifact_terminal_lifecycle=lifecycle,
        )

    return build


__all__ = ["ArtifactCatalogSettings", "artifact_catalog_builder"]
