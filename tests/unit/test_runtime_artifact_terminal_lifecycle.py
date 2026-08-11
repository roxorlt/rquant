from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

import rquant.runtime_artifact_terminal_lifecycle as lifecycle_module
from rquant.experiment_registry import ExperimentRegistry
from rquant.lab_jobs import LabJobStore
from rquant.runtime_artifact_terminal_lifecycle import (
    artifact_retention_state_root,
    build_production_artifact_terminal_lifecycle,
    operational_database_path,
    operational_readonly_database_path,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.storage.duckdb import DuckDBStore

NOW = datetime(2026, 8, 3, 1, 0, tzinfo=UTC)


def _prepare_shared_read_authorities(runtime_root: Path) -> Path:
    research_root = runtime_root / "research"
    research_root.mkdir(parents=True, mode=0o700)
    research_root.chmod(0o700)
    LabJobStore(research_root / "lab_jobs.sqlite3").initialize()
    ExperimentRegistry(
        research_root / "experiment_registry.sqlite3",
        managed_trust_root=research_root,
    )
    database_path = operational_database_path(runtime_root)
    database_path.parent.mkdir(parents=True, mode=0o700)
    database_path.parent.chmod(0o700)
    with DuckDBStore(database_path):
        pass
    replica_path = operational_readonly_database_path(runtime_root)
    replica_path.write_bytes(database_path.read_bytes())
    replica_path.chmod(0o600)
    return research_root / "experiment_registry.sqlite3"


@pytest.mark.parametrize(
    ("service_kind", "available", "absent"),
    [
        (
            RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
            {"catalog_registration_sink"},
            {
                "reference_store",
                "catalog_registration_outbox",
                "audit_snapshot_store",
                "experiment_registry",
                "experiment_registry_reader",
                "lab_job_reader",
                "hooks",
            },
        ),
        (
            RuntimeServiceKind.LAB_JOBS_PUBLISHER,
            {"lab_job_reader"},
            {
                "reference_store",
                "catalog_registration_outbox",
                "catalog_registration_sink",
                "audit_snapshot_store",
                "experiment_registry",
                "experiment_registry_reader",
                "hooks",
            },
        ),
        (
            RuntimeServiceKind.PROMOTIONS_PUBLISHER,
            {"experiment_registry_reader"},
            {
                "reference_store",
                "catalog_registration_outbox",
                "catalog_registration_sink",
                "audit_snapshot_store",
                "experiment_registry",
                "lab_job_reader",
                "hooks",
            },
        ),
        (
            RuntimeServiceKind.ARTIFACT_RETENTION,
            {
                "reference_store",
                "catalog_registration_outbox",
                "audit_snapshot_store",
                "experiment_registry_reader",
                "hooks",
            },
            {"experiment_registry", "lab_job_reader", "catalog_registration_sink"},
        ),
    ],
)
def test_lifecycle_exposes_only_service_owned_capabilities(
    tmp_path: Path,
    service_kind: RuntimeServiceKind,
    available: set[str],
    absent: set[str],
) -> None:
    runtime_root = tmp_path / "runtime"
    experiment_registry_path = _prepare_shared_read_authorities(runtime_root)

    lifecycle = build_production_artifact_terminal_lifecycle(
        runtime_root=runtime_root,
        experiment_registry_path=experiment_registry_path,
        service_kind=service_kind,
        clock=lambda: NOW,
    )
    try:
        for name in available:
            assert getattr(lifecycle, name) is not None
        for name in absent:
            assert getattr(lifecycle, name) is None
    finally:
        lifecycle.close()

    state_root = artifact_retention_state_root(runtime_root)
    if service_kind is RuntimeServiceKind.LAB_ARTIFACT_CATALOG:
        assert (state_root / "catalog-registration-outbox").is_dir()
        assert not (state_root / "references.sqlite3").exists()
    elif service_kind is RuntimeServiceKind.ARTIFACT_RETENTION:
        assert (state_root / "references.sqlite3").is_file()
    else:
        assert not state_root.exists()


def test_retention_lifecycle_requires_and_reads_the_runtime_metadata_replica(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    experiment_registry_path = _prepare_shared_read_authorities(runtime_root)
    replica_path = operational_readonly_database_path(runtime_root)

    lifecycle = build_production_artifact_terminal_lifecycle(
        runtime_root=runtime_root,
        experiment_registry_path=experiment_registry_path,
        service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
        clock=lambda: NOW,
    )
    try:
        assert lifecycle.audit_snapshot_store is not None
        assert lifecycle.audit_snapshot_store.path == replica_path
    finally:
        lifecycle.close()

    replica_path.unlink()
    with pytest.raises(RuntimeError, match="read-only research metadata replica"):
        build_production_artifact_terminal_lifecycle(
            runtime_root=runtime_root,
            experiment_registry_path=experiment_registry_path,
            service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
            clock=lambda: NOW,
        )


def test_retention_lifecycle_closes_open_audit_handle_when_reader_construction_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    research_root = runtime_root / "research"
    research_root.mkdir(parents=True, mode=0o700)
    replica_path = operational_readonly_database_path(runtime_root)
    replica_path.touch(mode=0o600)
    replica_path.chmod(0o600)
    closed: list[bool] = []

    class TrackingAuditStore:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            return None

        def close(self) -> None:
            closed.append(True)

    def fail_reader(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("reader authority is unavailable")

    monkeypatch.setattr(lifecycle_module, "DuckDBStore", TrackingAuditStore)
    monkeypatch.setattr(lifecycle_module, "ExperimentRegistryReadonlyReader", fail_reader)

    with pytest.raises(RuntimeError, match="reader authority"):
        lifecycle_module.build_production_artifact_terminal_lifecycle(
            runtime_root=runtime_root,
            experiment_registry_path=research_root / "experiment_registry.sqlite3",
            service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
            clock=lambda: NOW,
        )

    assert closed == [True]


def test_direct_terminal_owner_closes_open_audit_handle_when_registry_construction_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    research_root = runtime_root / "research"
    research_root.mkdir(parents=True, mode=0o700)
    closed: list[bool] = []

    class TrackingAuditStore:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            return None

        def close(self) -> None:
            closed.append(True)

    def fail_registry(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("registry authority is unavailable")

    monkeypatch.setattr(lifecycle_module, "DuckDBStore", TrackingAuditStore)
    monkeypatch.setattr(lifecycle_module, "ExperimentRegistry", fail_registry)

    with pytest.raises(RuntimeError, match="registry authority"):
        lifecycle_module.build_production_artifact_terminal_lifecycle(
            runtime_root=runtime_root,
            experiment_registry_path=research_root / "experiment_registry.sqlite3",
            clock=lambda: NOW,
        )

    assert closed == [True]
