"""Finite private account metadata selects immutable versions for the original Lab."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rquant.paper_portfolio_models import PaperPortfolioStateIdentity
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.paper_research import PAPER_RESEARCH_TASKS, PaperResearchAdapterCatalog, PaperResearchRunParameters
from rquant.paper_research_adapter import paper_research_adapter_registry
from rquant.paper_research_source import paper_research_code_identity
from rquant.research_run_spec import ResearchRunSpec
from rquant.strategy_job_adapters import StrategyJobAdapterRegistry
from rquant.strict_json import strict_canonical_json_loads

if TYPE_CHECKING:
    from rquant.lab_worker import LabShardRuntimeManifest


class PaperResearchRuntimeDirectory:
    def __init__(self, *, states: tuple[PaperPortfolioStateStore, ...],
                 expected_identities: tuple[PaperPortfolioStateIdentity, ...]) -> None:
        if not 1 <= len(states) <= 64 or len(states) != len(expected_identities):
            raise ValueError("paper runtime requires its finite exact account directory")
        if any(type(state) is not PaperPortfolioStateStore or state.identity() != identity
               for state, identity in zip(states, expected_identities, strict=True)):
            raise TypeError("paper runtime requires the original concrete metadata sources")
        keys = tuple((state.configuration.binding.account_id, state.configuration.binding.owner_id) for state in states)
        if len(set(keys)) != len(keys):
            raise ValueError("paper runtime account sources must be unique")
        self._states = states
        self._identities = expected_identities

    def catalog_for_spec(self, spec: ResearchRunSpec) -> PaperResearchAdapterCatalog:
        if spec.parameters.strategy_name not in PAPER_RESEARCH_TASKS:
            raise ValueError("paper runtime requires one of its two exact task names")
        parameters = PaperResearchRunParameters.model_validate({item.name: item.value for item in spec.parameters.arguments})
        matches = tuple((state, identity) for state, identity in zip(self._states, self._identities, strict=True)
                        if (state.configuration.binding.account_id, state.configuration.binding.owner_id) == (parameters.account_id, parameters.owner_id))
        if len(matches) != 1:
            raise ValueError("paper runtime account or owner is not registered")
        state, identity = matches[0]
        if state.identity() != identity:
            raise ValueError("paper runtime original metadata identity was replaced")
        configuration = state.configuration_at(parameters.configuration_fingerprint, version=parameters.configuration_version)
        catalog = PaperResearchAdapterCatalog(metadata_identity=identity, configuration=configuration,
                                               source_code_identity=paper_research_code_identity())
        paper_research_adapter_registry(catalog).for_spec(spec).parameters(spec)
        return catalog

    def registry_for_spec(self, spec: ResearchRunSpec) -> StrategyJobAdapterRegistry:
        return paper_research_adapter_registry(self.catalog_for_spec(spec))

    def manifest_for_spec(self, spec: ResearchRunSpec, original: LabShardRuntimeManifest) -> LabShardRuntimeManifest:
        from rquant.lab_worker import _BUILTIN_SHARD_REGISTRY_ID, build_builtin_shard_runtime_manifest
        from rquant.lab_worker_registry import _read_builtin_runtime_configuration

        catalog = self.catalog_for_spec(spec)
        if original.registry.registry_id != _BUILTIN_SHARD_REGISTRY_ID:
            raise ValueError("paper runtime requires the closed builtin manifest")
        config = _read_builtin_runtime_configuration(strict_canonical_json_loads(original.registry.configuration_json))
        if not config.configured or any(path is None for path in (config.catalog_path, config.snapshot_root, config.research_lake_root)):
            raise ValueError("paper runtime immutable source is not configured")
        result = build_builtin_shard_runtime_manifest(catalog_path=config.catalog_path, forbidden_paths=config.forbidden_paths,
                                                      snapshot_root=config.snapshot_root, research_lake_root=config.research_lake_root,
                                                      paper_catalog=catalog)
        if len(result.registry.configuration_json.encode()) > 512*1024:
            raise ValueError("paper runtime catalog exceeds the original wire budget")
        return result


def require_paper_runtime_directory(value: PaperResearchRuntimeDirectory | None) -> PaperResearchRuntimeDirectory | None:
    if value is not None and type(value) is not PaperResearchRuntimeDirectory:
        raise TypeError("paper runtime directory must be the concrete installed metadata reader")
    return value
