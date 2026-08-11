"""Production composition for durable artifact-owner terminal events.

The retention daemon owns publication, tier migration and deletion.  Business
writers only receive the deliberately narrow outbox capability below: after a
terminal state is durable they may enqueue immutable release evidence, but
cannot release, migrate, delete, or mutate artifact metadata.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from rquant.artifact_catalog_registration_outbox import (
    ArtifactCatalogRegistrationOutbox,
    ArtifactCatalogRegistrationSink,
)
from rquant.artifact_retention import ArtifactReferenceStore
from rquant.artifact_terminal_owners import (
    ArtifactTerminalLifecycleHooks,
    AuditTerminalReceiptProducer,
    ExperimentTerminalReceiptProducer,
    SnapshotTerminalReceiptProducer,
)
from rquant.experiment_registry import ExperimentRegistry, ExperimentRegistryReadonlyReader
from rquant.lab_jobs import LabJobReader
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.storage.duckdb import DuckDBStore

_RETENTION_SERVICE_ID = "artifact-retention.primary.v1"
_LINUX_PRODUCTION_RUNTIME_ROOT = Path("/home/lighthouse/rquant/data/runtime")


def artifact_retention_state_root(runtime_root: Path) -> Path:
    """Return the one profile-defined retention state root for ``runtime_root``."""

    root = Path(runtime_root)
    normalized = Path(os.path.abspath(root))
    if not root.is_absolute() or root != normalized:
        raise ValueError("artifact terminal runtime root must be absolute and normalized")
    instance = "svc-" + hashlib.sha256(_RETENTION_SERVICE_ID.encode("utf-8")).hexdigest()
    return root / "research" / "artifact-retention" / instance


def operational_database_path(runtime_root: Path) -> Path:
    """Resolve the fixed operational DuckDB location from the runtime root."""

    root = Path(runtime_root)
    artifact_retention_state_root(root)
    if root == _LINUX_PRODUCTION_RUNTIME_ROOT:
        return root.parent / "rquant.duckdb"
    # Local/test runtime roots deliberately isolate the operational authority per
    # generation.  They must not contend on one shared /tmp/rquant.duckdb file.
    return root / "operational" / "rquant.duckdb"


def operational_readonly_database_path(runtime_root: Path) -> Path:
    """Resolve the immutable research metadata replica consumed by retention."""

    root = Path(runtime_root)
    artifact_retention_state_root(root)
    return root / "research" / "research_ro.duckdb"


@dataclass
class ProductionArtifactTerminalLifecycle:
    """Narrow real capabilities for exactly one terminal-owner service.

    A context exposes only the filesystem handles its systemd unit owns.  This
    keeps the catalog and serving publishers out of the retention metadata
    database while still letting them participate in the one durable IPC loop.
    """

    reference_store: ArtifactReferenceStore | None = None
    catalog_registration_outbox: ArtifactCatalogRegistrationOutbox | None = None
    catalog_registration_sink: ArtifactCatalogRegistrationSink | None = None
    audit_snapshot_store: DuckDBStore | None = None
    experiment_registry: ExperimentRegistry | None = None
    experiment_registry_reader: ExperimentRegistryReadonlyReader | None = None
    lab_job_reader: LabJobReader | None = None
    hooks: ArtifactTerminalLifecycleHooks | None = None

    def close(self) -> None:
        errors: list[BaseException] = []
        for resource in (self.audit_snapshot_store, self.reference_store):
            if resource is None:
                continue
            try:
                resource.close()
            except BaseException as exc:
                errors.append(exc)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("artifact terminal lifecycle cleanup failed", errors)


def _private_state_root(runtime_root: Path) -> Path:
    state_root = artifact_retention_state_root(runtime_root)
    state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    state_root.chmod(0o700)
    return state_root


def _terminal_hooks(
    *,
    reference_store: ArtifactReferenceStore,
    audit_snapshot_store: DuckDBStore,
    experiment_registry: ExperimentRegistry | ExperimentRegistryReadonlyReader,
) -> ArtifactTerminalLifecycleHooks:
    return ArtifactTerminalLifecycleHooks(
        reference_store=reference_store,
        producers={
            "audit": AuditTerminalReceiptProducer(reference_store, audit_snapshot_store),
            "snapshot": SnapshotTerminalReceiptProducer(reference_store, audit_snapshot_store),
            "experiment": ExperimentTerminalReceiptProducer(
                reference_store,
                experiment_registry,
            ),
        },
    )


def build_production_artifact_terminal_lifecycle(
    *,
    runtime_root: Path,
    experiment_registry_path: Path,
    operational_store_path: Path | None = None,
    clock: Callable[[], datetime] | None = None,
    service_kind: RuntimeServiceKind | None = None,
) -> ProductionArtifactTerminalLifecycle:
    """Compose the sole real capability set admitted to a service kind.

    ``None`` is the direct terminal-owner composition used by genuine source
    writers.  Production runtime services must always pass their manifest kind;
    they then receive neither an accidental writer handle nor a second metadata
    authority.  No mode loads the retention GC credential.
    """
    if operational_store_path is not None:
        database_path = operational_store_path
    elif service_kind is RuntimeServiceKind.ARTIFACT_RETENTION:
        database_path = operational_readonly_database_path(runtime_root)
        if not database_path.is_file():
            raise RuntimeError("read-only research metadata replica is unavailable")
    else:
        database_path = operational_database_path(runtime_root)
    research_root = Path(runtime_root) / "research"

    if service_kind is RuntimeServiceKind.LAB_ARTIFACT_CATALOG:
        state_root = _private_state_root(runtime_root)
        return ProductionArtifactTerminalLifecycle(
            catalog_registration_sink=ArtifactCatalogRegistrationSink(
                ArtifactCatalogRegistrationOutbox(
                    state_root / "catalog-registration-outbox"
                )
            )
        )
    if service_kind is RuntimeServiceKind.LAB_JOBS_PUBLISHER:
        return ProductionArtifactTerminalLifecycle(
            lab_job_reader=LabJobReader(research_root / "lab_jobs.sqlite3")
        )
    if service_kind is RuntimeServiceKind.PROMOTIONS_PUBLISHER:
        return ProductionArtifactTerminalLifecycle(
            experiment_registry_reader=ExperimentRegistryReadonlyReader(
                experiment_registry_path,
                managed_trust_root=research_root,
            )
        )

    if service_kind is RuntimeServiceKind.ARTIFACT_RETENTION:
        state_root = _private_state_root(runtime_root)
        reference_store = ArtifactReferenceStore(
            state_root / "references.sqlite3",
            managed_trust_root=state_root,
            clock=clock,
            writer_owner="artifact-terminal-outbox",
            terminal_outbox_only=True,
        )
        audit_snapshot_store: DuckDBStore | None = None
        try:
            audit_snapshot_store = DuckDBStore(database_path, read_only=True)
            experiment_registry_reader = ExperimentRegistryReadonlyReader(
                experiment_registry_path,
                managed_trust_root=research_root,
            )
            return ProductionArtifactTerminalLifecycle(
                reference_store=reference_store,
                catalog_registration_outbox=ArtifactCatalogRegistrationOutbox(
                    state_root / "catalog-registration-outbox"
                ),
                audit_snapshot_store=audit_snapshot_store,
                experiment_registry_reader=experiment_registry_reader,
                hooks=_terminal_hooks(
                    reference_store=reference_store,
                    audit_snapshot_store=audit_snapshot_store,
                    experiment_registry=experiment_registry_reader,
                ),
            )
        except BaseException:
            if audit_snapshot_store is not None:
                audit_snapshot_store.close()
            reference_store.close()
            raise

    if service_kind is not None:
        raise ValueError(f"unsupported artifact terminal service kind: {service_kind.value}")

    state_root = _private_state_root(runtime_root)
    reference_store = ArtifactReferenceStore(
        state_root / "references.sqlite3",
        managed_trust_root=state_root,
        clock=clock,
        writer_owner="artifact-terminal-outbox",
        terminal_outbox_only=True,
    )
    if operational_store_path is None and Path(runtime_root) != _LINUX_PRODUCTION_RUNTIME_ROOT:
        database_path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        database_path.parent.chmod(0o700)
    hook_ref: dict[str, ArtifactTerminalLifecycleHooks] = {}

    def emit_terminal(owner_type: str, owner_id: str, observed_at: datetime) -> None:
        try:
            hook = hook_ref["hooks"]
        except KeyError as exc:  # pragma: no cover - construction cannot emit
            raise RuntimeError("artifact terminal lifecycle hook is not initialized") from exc
        hook(owner_type, owner_id, observed_at)

    audit_snapshot_store: DuckDBStore | None = None
    try:
        audit_snapshot_store = DuckDBStore(database_path, artifact_terminal_hook=emit_terminal)
        experiment_registry = ExperimentRegistry(
            experiment_registry_path,
            managed_trust_root=research_root,
            artifact_terminal_hook=emit_terminal,
        )
        hooks = _terminal_hooks(
            reference_store=reference_store,
            audit_snapshot_store=audit_snapshot_store,
            experiment_registry=experiment_registry,
        )
        hook_ref["hooks"] = hooks
        return ProductionArtifactTerminalLifecycle(
            reference_store=reference_store,
            catalog_registration_outbox=ArtifactCatalogRegistrationOutbox(
                state_root / "catalog-registration-outbox"
            ),
            audit_snapshot_store=audit_snapshot_store,
            experiment_registry=experiment_registry,
            experiment_registry_reader=ExperimentRegistryReadonlyReader(
                experiment_registry_path,
                managed_trust_root=research_root,
            ),
            lab_job_reader=LabJobReader(research_root / "lab_jobs.sqlite3"),
            hooks=hooks,
        )
    except BaseException:
        if audit_snapshot_store is not None:
            audit_snapshot_store.close()
        reference_store.close()
        raise


__all__ = [
    "ProductionArtifactTerminalLifecycle",
    "artifact_retention_state_root",
    "build_production_artifact_terminal_lifecycle",
    "operational_database_path",
    "operational_readonly_database_path",
]
