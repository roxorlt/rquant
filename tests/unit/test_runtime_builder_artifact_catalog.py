from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.artifact_catalog_registration_outbox import (
    ArtifactCatalogRegistrationOutbox,
    ArtifactCatalogRegistrationSink,
)
from rquant.experiment_registry import ExperimentRegistry
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.runtime_artifact_terminal_lifecycle import (
    artifact_retention_state_root,
    build_production_artifact_terminal_lifecycle,
)
from rquant.runtime_builder_artifact_catalog import (
    ArtifactCatalogSettings,
    artifact_catalog_builder,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.storage.duckdb import DuckDBStore

NOW = datetime(2026, 8, 2, 4, 0, tzinfo=UTC)
COMMIT = "1" * 40


def _private_dir(path: Path) -> Path:
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    path.chmod(0o700)
    return path.resolve()


def _manifest(tmp_path: Path) -> RuntimeServiceManifest:
    private = _private_dir(tmp_path / "private")
    artifact_root = _private_dir(private / "final-artifacts")
    _private_dir(artifact_root / "jobs")
    state_root = _private_dir(private / "artifact-catalog")
    return RuntimeServiceManifest(
        service_id="research.artifact-catalog",
        service_kind=RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=180,
        producer_commit=COMMIT,
        settings={
            "research_root": str(private),
            "artifact_root": str(artifact_root),
            "state_root": str(state_root),
            "lab_jobs_path": str(private / "lab_jobs.sqlite3"),
            "dataset_authority_path": str(private / "research_ro.duckdb"),
            "experiment_registry_path": str(private / "experiment_registry.sqlite3"),
            "location_id": "cloud-primary",
            "failure_domain": "tencent-shanghai",
            "max_bundles": 16,
            "max_discovery_entries": 64,
            "max_directories_per_step": 6,
            "max_discovery_seconds": 0.5,
            "max_experiment_rows": 50_000,
            "max_artifact_file_bytes": 64 * 1024 * 1024,
            "max_bundle_bytes": 256 * 1024 * 1024,
            "max_step_bytes": 512 * 1024 * 1024,
            "max_verification_seconds": 10.0,
            "max_artifact_file_verification_seconds": 2.0,
            "max_bundle_verification_seconds": 5.0,
        },
    )


def _lifecycle(manifest: RuntimeServiceManifest):
    settings = manifest.settings
    research_root = Path(str(settings["research_root"]))
    research_root.chmod(0o700)
    lab_jobs_path = Path(str(settings["lab_jobs_path"]))
    LabJobStore(lab_jobs_path).initialize()
    lab_jobs_path.chmod(0o600)
    registry = ExperimentRegistry(
        Path(str(settings["experiment_registry_path"])),
        managed_trust_root=research_root,
    )

    class Lifecycle:
        experiment_registry = registry
        lab_job_reader = LabJobReader(lab_jobs_path)
        catalog_registration_sink = ArtifactCatalogRegistrationSink(
            ArtifactCatalogRegistrationOutbox(
                research_root / "artifact-retention" / "catalog-registration-outbox"
            )
        )

        def close(self) -> None:
            return None

    return Lifecycle()


def test_artifact_catalog_builder_requires_lifecycle_and_stages_no_second_reference_store(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    with pytest.raises(RuntimeError, match="terminal lifecycle"):
        artifact_catalog_builder(clock=lambda: NOW)(manifest)

    lifecycle = _lifecycle(manifest)
    step = artifact_catalog_builder(
        clock=lambda: NOW,
        open_artifact_terminal_lifecycle=lambda: lifecycle,
    )(manifest)
    result = step()
    step.close()

    assert result.input_sequence == 1
    assert result.output_sequence == 1
    assert result.processed_count == 0
    assert result.backlog_count == 0
    assert set(result.source_generations) == {"artifact_catalog"}

    state_root = Path(str(manifest.settings["state_root"]))
    assert not (state_root / "references.sqlite3").exists()
    assert (state_root / "discovery.sqlite3").is_file()


def test_artifact_catalog_builder_opens_the_injected_terminal_lifecycle(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    lifecycle = _lifecycle(manifest)
    opened: list[object] = []

    def open_artifact_terminal_lifecycle() -> object:
        opened.append(lifecycle)
        return lifecycle

    step = artifact_catalog_builder(
        clock=lambda: NOW,
        open_artifact_terminal_lifecycle=open_artifact_terminal_lifecycle,
    )(manifest)

    assert callable(step)
    assert opened == [lifecycle]


def test_artifact_catalog_builder_uses_real_catalog_ipc_capability(
    tmp_path: Path,
) -> None:
    runtime_root = _private_dir(tmp_path / "runtime")
    research_root = _private_dir(runtime_root / "research")
    artifact_root = _private_dir(research_root / "final-artifacts")
    _private_dir(artifact_root / "jobs")
    state_root = _private_dir(research_root / "artifact-catalogs" / "catalog")
    jobs_path = research_root / "lab_jobs.sqlite3"
    LabJobStore(jobs_path).initialize()
    jobs_path.chmod(0o600)
    metadata_path = research_root / "research_ro.duckdb"
    with DuckDBStore(metadata_path):
        pass
    metadata_path.chmod(0o600)
    registry_path = research_root / "experiment_registry.sqlite3"
    ExperimentRegistry(registry_path, managed_trust_root=research_root)
    manifest = RuntimeServiceManifest(
        service_id="artifact-catalog.primary.v1",
        service_kind=RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=180,
        producer_commit=COMMIT,
        settings={
            "research_root": str(research_root),
            "artifact_root": str(artifact_root),
            "state_root": str(state_root),
            "lab_jobs_path": str(jobs_path),
            "dataset_authority_path": str(metadata_path),
            "experiment_registry_path": str(registry_path),
            "location_id": "cloud-primary",
            "failure_domain": "tencent-shanghai",
        },
    )
    lifecycle = build_production_artifact_terminal_lifecycle(
        runtime_root=runtime_root,
        experiment_registry_path=registry_path,
        service_kind=RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        clock=lambda: NOW,
    )
    step = artifact_catalog_builder(
        clock=lambda: NOW,
        open_artifact_terminal_lifecycle=lambda: lifecycle,
    )(manifest)
    try:
        result = step()
    finally:
        step.close()

    assert result.processed_count == 0
    assert lifecycle.catalog_registration_sink is not None
    assert lifecycle.catalog_registration_sink.outbox.pending_count() == 0
    assert not (artifact_retention_state_root(runtime_root) / "references.sqlite3").exists()


def test_artifact_catalog_builder_requires_research_plane_and_private_absolute_paths(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    wrong_plane = manifest.model_copy(update={"plane": RuntimeServicePlane.LIVE})
    relative_state = manifest.model_copy(
        update={"settings": {**dict(manifest.settings), "state_root": "relative/state"}}
    )

    with pytest.raises(ValueError, match="research plane"):
        artifact_catalog_builder(clock=lambda: NOW)(wrong_plane)
    with pytest.raises(ValueError, match="absolute"):
        artifact_catalog_builder(clock=lambda: NOW)(relative_state)


def test_artifact_catalog_builder_rejects_wrong_service_kind(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path).model_copy(
        update={"service_kind": RuntimeServiceKind.LAB_JOBS_PUBLISHER}
    )

    with pytest.raises(ValueError, match="artifact catalog"):
        artifact_catalog_builder(clock=lambda: NOW)(manifest)


def test_artifact_catalog_settings_bound_discovery_and_owner_index_budgets(
    tmp_path: Path,
) -> None:
    settings = ArtifactCatalogSettings.model_validate(dict(_manifest(tmp_path).settings))

    assert settings.max_directories_per_step == 6
    assert settings.max_discovery_seconds == 0.5
    assert settings.max_experiment_rows == 50_000
    assert settings.max_artifact_file_bytes == 64 * 1024 * 1024
    assert settings.max_bundle_bytes == 256 * 1024 * 1024
    assert settings.max_step_bytes == 512 * 1024 * 1024
    assert settings.max_verification_seconds == 10.0
    assert settings.max_artifact_file_verification_seconds == 2.0
    assert settings.max_bundle_verification_seconds == 5.0
    for field in (
        "max_directories_per_step",
        "max_discovery_seconds",
        "max_experiment_rows",
        "max_artifact_file_bytes",
        "max_bundle_bytes",
        "max_step_bytes",
        "max_verification_seconds",
        "max_artifact_file_verification_seconds",
        "max_bundle_verification_seconds",
    ):
        with pytest.raises(ValueError):
            ArtifactCatalogSettings.model_validate({**dict(_manifest(tmp_path).settings), field: 0})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_artifact_file_bytes", 1024**4 + 1),
        ("max_bundle_bytes", 4 * 1024**4 + 1),
        ("max_step_bytes", 16 * 1024**4 + 1),
        ("max_verification_seconds", 3_600.1),
    ],
)
def test_artifact_catalog_settings_reject_verification_budget_upper_bounds(
    tmp_path: Path,
    field: str,
    value: int | float,
) -> None:
    with pytest.raises(ValueError):
        ArtifactCatalogSettings.model_validate({**dict(_manifest(tmp_path).settings), field: value})


@pytest.mark.parametrize(
    "updates",
    [
        {"max_artifact_file_bytes": 257, "max_bundle_bytes": 256},
        {"max_bundle_bytes": 513, "max_step_bytes": 512},
    ],
)
def test_artifact_catalog_settings_require_nested_byte_budgets(
    tmp_path: Path,
    updates: dict[str, int],
) -> None:
    with pytest.raises(ValueError, match="file.*bundle|bundle.*step"):
        ArtifactCatalogSettings.model_validate({**dict(_manifest(tmp_path).settings), **updates})


def test_artifact_catalog_settings_reject_boolean_verification_time(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError):
        ArtifactCatalogSettings.model_validate(
            {
                **dict(_manifest(tmp_path).settings),
                "max_verification_seconds": True,
            }
        )


@pytest.mark.parametrize(
    ("file_seconds", "bundle_seconds", "step_seconds", "message"),
    [
        (3.0, 2.0, 4.0, "file time budget"),
        (1.0, 4.0, 3.0, "bundle time budget"),
    ],
)
def test_artifact_catalog_settings_require_nested_time_budgets(
    tmp_path: Path,
    file_seconds: float,
    bundle_seconds: float,
    step_seconds: float,
    message: str,
) -> None:
    payload = dict(_manifest(tmp_path).settings)
    payload.update(
        {
            "max_artifact_file_verification_seconds": file_seconds,
            "max_bundle_verification_seconds": bundle_seconds,
            "max_verification_seconds": step_seconds,
        }
    )

    with pytest.raises(ValueError, match=message):
        ArtifactCatalogSettings.model_validate(payload)


@pytest.mark.parametrize(
    "field",
    (
        "max_artifact_file_verification_seconds",
        "max_bundle_verification_seconds",
    ),
)
def test_artifact_catalog_settings_reject_boolean_nested_times(
    tmp_path: Path,
    field: str,
) -> None:
    payload = dict(_manifest(tmp_path).settings)
    payload[field] = True

    with pytest.raises(ValueError):
        ArtifactCatalogSettings.model_validate(payload)
