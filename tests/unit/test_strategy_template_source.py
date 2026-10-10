"""Actual template material must pass the original snapshot/session boundary."""

from datetime import timedelta

import pytest

from rquant.research_catalog import ResearchCatalog
from rquant.research_snapshot import ResearchExecutionSession
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_dependencies import StrategyExecutionDependencies, StrategyTableDependency
from tests.unit.test_strategy_template_adapter import adapter_fixture, claimed, template_spec


def template_dependencies(catalog):
    version = catalog.versions[0]
    return StrategyExecutionDependencies.model_validate(
        {
            "strategy_id": version.strategy_id,
            "contract_version": "strategy-template-input/v1",
            "lake_datasets": (),
            "materialized_tables": (
                StrategyTableDependency(
                    dataset_id="strategy_template_input", table_name="strategy_template_input"
                ),
            ),
            "template_definition": version,
        }
    )


def test_exact_committed_template_requires_its_typed_source_proof(tmp_path) -> None:
    _, _, catalog, _ = adapter_fixture(tmp_path)
    dependencies = template_dependencies(catalog)
    assert dependencies.template_definition == catalog.versions[0]
    with pytest.raises(ValueError):
        StrategyExecutionDependencies.model_validate(
            {**dependencies.model_dump(mode="python"), "strategy_id": "template_" + "f" * 32}
        )
    with pytest.raises(ValueError):
        StrategyExecutionDependencies.model_validate(
            {**dependencies.model_dump(mode="python"), "template_definition": None}
        )


def test_real_publisher_original_session_and_original_adapter_replay(tmp_path) -> None:
    from rquant.strategy_template_source import (
        publish_strategy_template_input,
        verify_bound_template_input,
    )

    _, value, catalog, registry = adapter_fixture(tmp_path)
    now = value.definition.available_at + timedelta(seconds=1)
    lake = tmp_path / "lake"
    with DuckDBStore(tmp_path / "research.duckdb") as metadata:
        published = publish_strategy_template_input(
            value,
            metadata_store=metadata,
            source_path=tmp_path / "private" / "input.duckdb",
            catalog=ResearchCatalog(tmp_path / "catalog.duckdb"),
            lake_root=lake,
            version=catalog.versions[0],
            now=now,
        )
        binding = metadata.get_dataset_snapshot_binding(published.identity.snapshot_id)
        with ResearchExecutionSession(binding=binding, lake_root=lake) as session:
            assert (
                verify_bound_template_input(
                    metadata, published.request, session, version=catalog.versions[0]
                )
                == value
            )
            spec = template_spec(value, dataset_snapshot=published.identity)
            result = registry.execute_shard(claimed(registry, spec), session)
        assert result.tables[1].frame.iloc[0]["result_hash"]
        assert binding.manifest.dependency_contract_version == "strategy-template-input/v1"
        assert (
            published.gate_decision.allowed
            and published.gate_decision.research_status == "exploratory"
        )


def test_snapshot_asof_rejects_unavailable_definition_and_future_facts(tmp_path) -> None:
    import duckdb

    from rquant.strategy_template_adapter import write_strategy_template_input
    from rquant.strategy_template_source import verify_template_snapshot_source

    _, value, catalog, _ = adapter_fixture(tmp_path)
    with duckdb.connect(":memory:") as connection:
        write_strategy_template_input(connection, value)
        with pytest.raises(ValueError, match="available|future"):
            verify_template_snapshot_source(
                connection,
                version=catalog.versions[0],
                code_sha=value.request.producer_commit,
                start_date=value.request.days[0].trade_date,
                end_date=value.request.days[-1].trade_date,
                input_hash=value.input_hash,
                as_of=value.definition.available_at - timedelta(seconds=1),
            )


def test_original_child_entry_uses_verified_template_artifact(tmp_path) -> None:
    from rquant.lab_worker_registry import (
        builtin_lab_shard_configuration,
        execute_builtin_lab_shard,
    )
    from rquant.strategy_template_source import publish_strategy_template_input

    _, value, catalog, registry = adapter_fixture(tmp_path)
    now = value.definition.available_at + timedelta(seconds=1)
    metadata_path = tmp_path / "research.duckdb"
    lake = tmp_path / "lake"
    with DuckDBStore(metadata_path) as metadata:
        published = publish_strategy_template_input(
            value,
            metadata_store=metadata,
            source_path=tmp_path / "private" / "input.duckdb",
            catalog=ResearchCatalog(tmp_path / "catalog.duckdb"),
            lake_root=lake,
            version=catalog.versions[0],
            now=now,
        )
    spec = template_spec(value, dataset_snapshot=published.identity)
    config = builtin_lab_shard_configuration(
        catalog_path=metadata_path,
        forbidden_paths=(),
        snapshot_root=tmp_path / "snapshots",
        research_lake_root=lake,
        template_catalog=catalog,
    )
    result = execute_builtin_lab_shard(
        config, claimed(registry, spec), runtime_code_sha=spec.code_sha
    )
    assert result.tables[0].frame.iloc[0]["input_hash"] == value.input_hash
    artifact = next(lake.rglob("*.parquet"))
    artifact.write_bytes(b"changed source")
    with pytest.raises((ValueError, PermissionError), match="hash|size|artifact|source"):
        execute_builtin_lab_shard(config, claimed(registry, spec), runtime_code_sha=spec.code_sha)
