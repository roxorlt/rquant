"""A concrete private directory selects exact versions for the original Lab."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rquant.research_run_spec import ResearchRunSpec
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_job_adapters import (
    StrategyJobAdapterRegistry,
    default_strategy_job_adapter_registry,
)
from rquant.strategy_template_adapter import (
    StrategyTemplateAdapterCatalog,
    build_strategy_template_adapter_catalog,
    strategy_template_adapter_registry,
)
from rquant.strict_json import strict_canonical_json_loads

if TYPE_CHECKING:
    from rquant.lab_worker import LabShardRuntimeManifest


class StrategyTemplateRuntimeDirectory:
    def __init__(
        self, store: StrategyAuthoringStore, *, expected_identity: StrategyAuthoringIdentity
    ) -> None:
        if type(store) is not StrategyAuthoringStore or store.identity() != expected_identity:
            raise ValueError("template runtime requires the concrete original metadata identity")
        self.store, self.expected_identity = store, expected_identity

    def catalog_for_spec(self, spec: ResearchRunSpec) -> StrategyTemplateAdapterCatalog | None:
        original = default_strategy_job_adapter_registry()
        try:
            original.for_spec(spec)
        except ValueError:
            pass
        else:
            return None
        execution = spec.strategy_execution
        if execution is None:
            raise ValueError("template runtime requires the original exact definition")
        catalog = build_strategy_template_adapter_catalog(
            self.store,
            expected_identity=self.expected_identity,
            selected_keys=((spec.parameters.strategy_name, execution.strategy_version),),
        )
        adapter = strategy_template_adapter_registry(catalog).for_spec(spec)
        adapter.parameters(spec)
        return catalog

    def registry_for_spec(self, spec: ResearchRunSpec) -> StrategyJobAdapterRegistry:
        catalog = self.catalog_for_spec(spec)
        return (
            default_strategy_job_adapter_registry()
            if catalog is None
            else strategy_template_adapter_registry(catalog)
        )

    def manifest_for_spec(
        self, spec: ResearchRunSpec, original: LabShardRuntimeManifest
    ) -> LabShardRuntimeManifest:
        from rquant.lab_worker import (
            _BUILTIN_SHARD_REGISTRY_ID,
            build_builtin_shard_runtime_manifest,
        )
        from rquant.lab_worker_registry import _read_builtin_runtime_configuration

        catalog = self.catalog_for_spec(spec)
        if catalog is None:
            return original
        if original.registry.registry_id != _BUILTIN_SHARD_REGISTRY_ID:
            raise ValueError("template runtime requires the closed builtin manifest")
        config = _read_builtin_runtime_configuration(
            strict_canonical_json_loads(original.registry.configuration_json)
        )
        if not config.configured or any(
            path is None
            for path in (config.catalog_path, config.snapshot_root, config.research_lake_root)
        ):
            raise ValueError("template runtime immutable source is not configured")
        result = build_builtin_shard_runtime_manifest(
            catalog_path=config.catalog_path,
            forbidden_paths=config.forbidden_paths,
            snapshot_root=config.snapshot_root,
            research_lake_root=config.research_lake_root,
            template_catalog=catalog,
        )
        if len(result.registry.configuration_json.encode()) > 512 * 1024:
            raise ValueError("template runtime catalog exceeds the original wire budget")
        return result


def require_template_runtime_directory(
    value: StrategyTemplateRuntimeDirectory | None,
) -> StrategyTemplateRuntimeDirectory | None:
    if value is not None and type(value) is not StrategyTemplateRuntimeDirectory:
        raise TypeError("template runtime directory must be the installed concrete metadata reader")
    return value
