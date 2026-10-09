"""Closed child-side runtime registry for Strategy Lab workers."""

from __future__ import annotations

from contextlib import ExitStack
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SerializerFunctionWrapHandler, TypeAdapter, ValidatorFunctionWrapHandler, field_validator, model_serializer, model_validator

from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.research_gate import ResearchGateRequest, open_gated_research_store
from rquant.research_snapshot import ResearchExecutionSession
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_job_adapters import (
    LabShardExecutionResult,
    ValidatedStrategyShard,
    default_strategy_job_adapter_registry,
    StrategyJobAdapterRegistry,
)
from rquant.strategy_template_adapter import (
    StrategyTemplateAdapterCatalog,
    strategy_template_adapter_registry,
)
from rquant.strict_json import canonical_json_bytes
from rquant.paper_research import NativePaperResearchAdapterCatalog, PaperResearchAdapterCatalog, PaperResearchCatalog
from rquant.paper_research_adapter import paper_research_adapter_registry
from rquant.minute_backtest_formal_adapter import MinuteFormalReplayAdapter, minute_formal_adapter_registry
from rquant.minute_backtest_producer import MinuteReplayCatalog, open_gated_minute_store
from rquant.minute_backtest_publication_contracts import MAX_MINUTE_CONTROL_BYTES
from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalReplayAdapter
from rquant.minute_backtest_parameter_producer import MinuteParameterReplayCatalog


class BuiltinLabShardRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1] = 1
    configured: bool
    catalog_path: Path | None = None
    forbidden_paths: tuple[Path, ...] = ()
    snapshot_root: Path | None = None
    research_lake_root: Path | None = None
    adapter_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    template_catalog: StrategyTemplateAdapterCatalog | None = None
    paper_catalog: PaperResearchCatalog | None = None
    minute_catalog: MinuteReplayCatalog | None = None
    parameter_catalog: MinuteParameterReplayCatalog | None = None
    minute_registry_mode: Literal["isolated", "installed"] = "isolated"

    @field_validator("minute_catalog", mode="wrap")
    @classmethod
    def parse_minute_catalog(cls, value: object, handler: ValidatorFunctionWrapHandler) -> object:
        if value is None:
            return handler(value)
        if type(value) is MinuteReplayCatalog:
            return MinuteReplayCatalog.model_validate(value.model_dump(mode="python"))
        if isinstance(value, Mapping):
            return MinuteReplayCatalog.model_validate_json(canonical_json_bytes(value))
        raise ValueError("minute runtime catalog must be the installed typed reference")

    @field_validator("parameter_catalog", mode="wrap")
    @classmethod
    def parse_parameter_catalog(cls, value: object, handler: ValidatorFunctionWrapHandler) -> object:
        if value is None:
            return handler(value)
        if type(value) is MinuteParameterReplayCatalog:
            return MinuteParameterReplayCatalog.model_validate(value.model_dump(mode="python"))
        if isinstance(value, Mapping):
            return MinuteParameterReplayCatalog.model_validate_json(canonical_json_bytes(value))
        raise ValueError("parameter runtime catalog must be the complete installed typed reference")

    @model_serializer(mode="wrap")
    def serialize_original_configuration(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        data = handler(self)
        if self.minute_catalog is None:
            data.pop("minute_catalog", None)
        if self.parameter_catalog is None:
            data.pop("parameter_catalog", None)
        if self.minute_registry_mode == "isolated":
            data.pop("minute_registry_mode", None)
        return data

    @field_validator("paper_catalog", mode="wrap")
    @classmethod
    def parse_paper_catalog(cls, value: object, handler: ValidatorFunctionWrapHandler) -> object:
        if value is None:
            return handler(value)
        if type(value) is PaperResearchAdapterCatalog:
            return PaperResearchAdapterCatalog.model_validate(value.model_dump(mode="python"))
        if type(value) is NativePaperResearchAdapterCatalog:
            return NativePaperResearchAdapterCatalog.model_validate_json(value.model_dump_json())
        if isinstance(value, Mapping):
            return TypeAdapter(PaperResearchCatalog).validate_json(canonical_json_bytes(value))
        raise ValueError("paper runtime catalog must be the frozen typed object")

    @model_validator(mode="after")
    def one_owned_catalog(self) -> BuiltinLabShardRuntimeConfig:
        if self.minute_registry_mode == "installed" and self.minute_catalog is None and self.parameter_catalog is None:
            raise ValueError("installed minute mode requires its complete catalog")
        if sum((self.template_catalog is not None, self.paper_catalog is not None,
                self.minute_catalog is not None or self.parameter_catalog is not None)) > 1:
            raise ValueError("one original shard requires one exact owned catalog")
        if (self.minute_catalog is not None or self.parameter_catalog is not None) and len(self.model_dump_json().encode()) > MAX_MINUTE_CONTROL_BYTES:
            raise ValueError("minute worker configuration exceeds original 1 MiB control wire budget")
        return self

    @field_validator("template_catalog", mode="wrap")
    @classmethod
    def parse_template_catalog(cls, value: object, handler: ValidatorFunctionWrapHandler) -> object:
        if value is None:
            return handler(value)
        if isinstance(value, StrategyTemplateAdapterCatalog):
            return StrategyTemplateAdapterCatalog.model_validate(value.model_dump(mode="python"))
        if isinstance(value, Mapping):
            return StrategyTemplateAdapterCatalog.model_validate_json(canonical_json_bytes(value))
        raise ValueError("template runtime catalog must be the frozen typed object")


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
    template_catalog: StrategyTemplateAdapterCatalog | None = None,
    paper_catalog: PaperResearchCatalog | None = None,
    minute_catalog: MinuteReplayCatalog | None = None,
    parameter_catalog: MinuteParameterReplayCatalog | None = None,
    minute_registry_mode: Literal["isolated", "installed"] = "isolated",
) -> BuiltinLabShardRuntimeConfig:
    if sum((template_catalog is not None, paper_catalog is not None,
            minute_catalog is not None or parameter_catalog is not None)) > 1:
        raise ValueError("one original shard requires one exact owned catalog")
    registry = (
        (default_strategy_job_adapter_registry() if paper_catalog is None else paper_research_adapter_registry(paper_catalog))
        if template_catalog is None
        else strategy_template_adapter_registry(template_catalog)
    )
    if minute_catalog is not None or parameter_catalog is not None:
        registry = _minute_adapter_registry(minute_catalog, minute_registry_mode, parameter_catalog)
    return BuiltinLabShardRuntimeConfig(
        configured=True,
        catalog_path=Path(catalog_path).resolve(),
        forbidden_paths=tuple(Path(path).resolve() for path in forbidden_paths),
        snapshot_root=Path(snapshot_root).resolve(),
        research_lake_root=Path(research_lake_root).resolve(),
        adapter_manifest_hash=registry.closed_descriptor().manifest_hash,
        template_catalog=template_catalog,
        paper_catalog=paper_catalog,
        minute_catalog=minute_catalog,
        parameter_catalog=parameter_catalog,
        minute_registry_mode=minute_registry_mode,
    )


def unconfigured_builtin_lab_shard_configuration() -> BuiltinLabShardRuntimeConfig:
    registry = default_strategy_job_adapter_registry()
    return BuiltinLabShardRuntimeConfig(
        configured=False,
        adapter_manifest_hash=registry.closed_descriptor().manifest_hash,
    )


def resolve_builtin_adapter_registry(
    configuration: BuiltinLabShardRuntimeConfig,
) -> StrategyJobAdapterRegistry:
    config = BuiltinLabShardRuntimeConfig.model_validate(configuration, strict=True)
    registry = (
        (default_strategy_job_adapter_registry() if config.paper_catalog is None else paper_research_adapter_registry(config.paper_catalog))
        if config.template_catalog is None
        else strategy_template_adapter_registry(config.template_catalog)
    )
    if config.minute_catalog is not None or config.parameter_catalog is not None:
        registry = _minute_adapter_registry(config.minute_catalog, config.minute_registry_mode, config.parameter_catalog)
    if registry.closed_descriptor().manifest_hash != config.adapter_manifest_hash:
        raise LabDaemonConfigurationError("strategy adapter registry hash mismatch")
    return registry


def _minute_adapter_registry(
    catalog: MinuteReplayCatalog | None, mode: Literal["isolated", "installed"],
    parameter_catalog: MinuteParameterReplayCatalog | None = None,
) -> StrategyJobAdapterRegistry:
    minute = (() if catalog is None else (MinuteFormalReplayAdapter(catalog),))
    if parameter_catalog is not None:
        minute += (MinuteParameterFormalReplayAdapter(parameter_catalog),)
    if mode == "isolated":
        return StrategyJobAdapterRegistry(minute)
    original = default_strategy_job_adapter_registry()
    return StrategyJobAdapterRegistry(tuple(
        original.get(entry.adapter_id, entry.adapter_version)
        for entry in original.closed_descriptor().adapters
    ) + minute)


def _read_builtin_runtime_configuration(configuration: object) -> BuiltinLabShardRuntimeConfig:
    if isinstance(configuration, BuiltinLabShardRuntimeConfig):
        return BuiltinLabShardRuntimeConfig.model_validate(configuration, strict=True)
    # The child has already authenticated canonical JSON. JSON-mode strict
    # validation restores its Path/tuple representations without scalar coercion.
    return BuiltinLabShardRuntimeConfig.model_validate_json(
        canonical_json_bytes(configuration), strict=True
    )


def execute_builtin_lab_shard(
    configuration: object,
    validated: ValidatedStrategyShard,
    *,
    runtime_code_sha: str,
) -> LabShardExecutionResult:
    config = _read_builtin_runtime_configuration(configuration)
    if not config.configured:
        raise LabDaemonConfigurationError("built-in shard runtime is not configured")
    registry = resolve_builtin_adapter_registry(config)
    store_factory = _ImmutableLabStoreFactory(config)
    spec = validated.spec
    from rquant.strategy_template_adapter import StrategyTemplateAdapter

    adapter = registry.for_spec(spec)
    if type(adapter) is MinuteParameterFormalReplayAdapter:
        if config.parameter_catalog is None or config.research_lake_root is None or spec.dataset_snapshot is None:
            raise PermissionError("parameter worker requires its installed complete fact authority and snapshot")
        if runtime_code_sha != spec.code_sha:
            raise PermissionError("parameter original worker runtime code differs from formal input")
        parameters = adapter.parameters(spec)
        if parameters.prepared_publication_json is None:
            raise PermissionError("installed parameter worker requires its full prepared-publication carrier")
        from rquant.minute_backtest_parameter_producer import MinuteParameterPreparedPublication

        prepared = MinuteParameterPreparedPublication.model_validate_json(parameters.prepared_publication_json)
        if (config.research_lake_root, config.snapshot_root, config.forbidden_paths) != (
            config.parameter_catalog.research_lake_root, config.parameter_catalog.snapshot_root,
            config.parameter_catalog.forbidden_paths):
            raise PermissionError("parameter worker roots differ from the complete installed catalog")
        with config.parameter_catalog.open_prepared(prepared) as store:
            return registry.execute_shard(validated, store)
    if type(adapter) is MinuteFormalReplayAdapter:
        identity = spec.dataset_snapshot
        if config.minute_catalog is None or config.research_lake_root is None or identity is None:
            raise PermissionError("minute worker requires its installed formal source and snapshot")
        parameters = adapter.parameters(spec)
        if runtime_code_sha != spec.code_sha:
            raise PermissionError("minute original worker runtime code differs from formal input")
        request = ResearchGateRequest(mode="formal", strategy_name="minute_runtime_replay",
            start_date=spec.parameters.start_date, end_date=spec.parameters.end_date,
            audit_run_id=identity.audit_run_id, dataset_snapshot_id=identity.snapshot_id,
            dataset_binding_hash=identity.binding_hash, code_commit=runtime_code_sha)
        with open_gated_minute_store(request, metadata_store_factory=store_factory, lake_root=config.research_lake_root,
            catalog=config.minute_catalog, source_key=parameters.source_key, source_version=parameters.source_version,
            owner_id=parameters.owner_id) as (store, _decision):
            return registry.execute_shard(validated, store)
    from rquant.paper_research_adapter import PaperResearchAdapter
    if type(adapter) is PaperResearchAdapter:
        from rquant.paper_research_source import open_gated_paper_store

        identity = spec.dataset_snapshot
        if identity is None or config.research_lake_root is None or spec.research_status != "exploratory":
            raise PermissionError("paper worker requires its original exploratory immutable source")
        adapter.parameters(spec)
        request = ResearchGateRequest(mode="exploratory", strategy_name=adapter.snapshot_strategy_name,
                                      start_date=spec.parameters.start_date, end_date=spec.parameters.end_date,
                                      audit_run_id=identity.audit_run_id, dataset_snapshot_id=identity.snapshot_id,
                                      dataset_binding_hash=identity.binding_hash, code_commit=runtime_code_sha)
        with open_gated_paper_store(request, metadata_store_factory=store_factory, lake_root=config.research_lake_root,
                                    catalog=adapter.catalog) as store:
            return registry.execute_shard(validated, store)
    if type(adapter) is StrategyTemplateAdapter:
        from rquant.strategy_template_source import open_gated_template_store

        identity = spec.dataset_snapshot
        if identity is None or config.research_lake_root is None or spec.research_status != "exploratory":
            raise PermissionError("template worker requires its original exploratory immutable source")
        parameters = adapter.parameters(spec)
        version = next(item for item in adapter.catalog.versions if (item.strategy_id, item.head.version) == (parameters.strategy_id, parameters.version))
        request = ResearchGateRequest(mode="exploratory", strategy_name=adapter.snapshot_strategy_name, start_date=spec.parameters.start_date, end_date=spec.parameters.end_date, audit_run_id=identity.audit_run_id, dataset_snapshot_id=identity.snapshot_id, dataset_binding_hash=identity.binding_hash, code_commit=runtime_code_sha)
        with open_gated_template_store(request, metadata_store_factory=store_factory, lake_root=config.research_lake_root, version=version) as store:
            return registry.execute_shard(validated, store)
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
    if adapter.strategy_name == "portfolio_backtest":
        from rquant.portfolio_backtest_source import open_gated_portfolio_store

        with open_gated_portfolio_store(
            request, metadata_store_factory=store_factory, lake_root=config.research_lake_root
        ) as (store, _decision):
            return registry.execute_shard(validated, store)
    with open_gated_research_store(
        request,
        metadata_store_factory=store_factory,
        execution_session_factory=ResearchExecutionSession,
        lake_root=config.research_lake_root,
    ) as (store, _decision):
        return registry.execute_shard(validated, store)
