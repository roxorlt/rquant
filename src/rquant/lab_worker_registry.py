"""Closed child-side runtime registry for Strategy Lab workers."""

from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.research_gate import ResearchGateRequest, open_gated_research_store
from rquant.research_snapshot import ResearchExecutionSession
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_job_adapters import (
    LabShardExecutionResult,
    ValidatedStrategyShard,
    default_strategy_job_adapter_registry,
)


class BuiltinLabShardRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1] = 1
    configured: bool
    catalog_path: Path | None = None
    forbidden_paths: tuple[Path, ...] = ()
    snapshot_root: Path | None = None
    research_lake_root: Path | None = None
    adapter_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class _ImmutableLabStoreContext:
    def __init__(self, config: BuiltinLabShardRuntimeConfig) -> None:
        self._config = config
        self._stack = ExitStack()

    def __enter__(self) -> DuckDBStore:
        from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog

        catalog_path = self._config.catalog_path
        snapshot_root = self._config.snapshot_root
        if catalog_path is None or snapshot_root is None:
            raise LabDaemonConfigurationError("built-in shard store is not configured")
        catalog = self._stack.enter_context(
            ImmutableDuckDBMetadataCatalog.open(
                catalog_path,
                forbidden_paths=self._config.forbidden_paths,
                snapshot_root=snapshot_root,
            )
        )
        return self._stack.enter_context(DuckDBStore(catalog.snapshot_path, read_only=True))

    def __exit__(self, *exc_info: object) -> None:
        self._stack.__exit__(*exc_info)


class _ImmutableLabStoreFactory:
    def __init__(self, config: BuiltinLabShardRuntimeConfig) -> None:
        self._config = config

    def __call__(self) -> _ImmutableLabStoreContext:
        return _ImmutableLabStoreContext(self._config)


def builtin_lab_shard_configuration(
    *,
    catalog_path: Path,
    forbidden_paths: tuple[Path, ...],
    snapshot_root: Path,
    research_lake_root: Path,
) -> BuiltinLabShardRuntimeConfig:
    registry = default_strategy_job_adapter_registry()
    return BuiltinLabShardRuntimeConfig(
        configured=True,
        catalog_path=Path(catalog_path).resolve(),
        forbidden_paths=tuple(Path(path).resolve() for path in forbidden_paths),
        snapshot_root=Path(snapshot_root).resolve(),
        research_lake_root=Path(research_lake_root).resolve(),
        adapter_manifest_hash=registry.closed_descriptor().manifest_hash,
    )


def unconfigured_builtin_lab_shard_configuration() -> BuiltinLabShardRuntimeConfig:
    registry = default_strategy_job_adapter_registry()
    return BuiltinLabShardRuntimeConfig(
        configured=False,
        adapter_manifest_hash=registry.closed_descriptor().manifest_hash,
    )


def execute_builtin_lab_shard(
    configuration: object,
    validated: ValidatedStrategyShard,
    *,
    runtime_code_sha: str,
) -> LabShardExecutionResult:
    config = BuiltinLabShardRuntimeConfig.model_validate(configuration, strict=True)
    if not config.configured:
        raise LabDaemonConfigurationError("built-in shard runtime is not configured")
    registry = default_strategy_job_adapter_registry()
    if registry.closed_descriptor().manifest_hash != config.adapter_manifest_hash:
        raise LabDaemonConfigurationError("strategy adapter registry hash mismatch")
    store_factory = _ImmutableLabStoreFactory(config)
    spec = validated.spec
    if spec.research_status == "exploratory":
        with store_factory() as store:
            return registry.execute_shard(validated, store)

    identity = spec.dataset_snapshot
    if identity is None or config.research_lake_root is None:
        raise PermissionError("formal worker execution requires an immutable dataset snapshot")
    adapter = registry.for_spec(spec)
    request = ResearchGateRequest(
        mode="formal",
        strategy_name=adapter.snapshot_strategy_name,
        start_date=spec.parameters.start_date,
        end_date=spec.parameters.end_date,
        audit_run_id=identity.audit_run_id,
        dataset_snapshot_id=identity.snapshot_id,
        dataset_binding_hash=identity.binding_hash,
        code_commit=runtime_code_sha,
    )
    with open_gated_research_store(
        request,
        metadata_store_factory=store_factory,
        execution_session_factory=ResearchExecutionSession,
        lake_root=config.research_lake_root,
    ) as (store, _decision):
        return registry.execute_shard(validated, store)
