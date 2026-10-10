"""Complete immutable paper inputs pass the original snapshot and child entry."""

from datetime import timedelta

import duckdb
import pytest

from rquant.lab_worker_registry import builtin_lab_shard_configuration, execute_builtin_lab_shard
from rquant.paper_reconcile import freeze_paper_reconcile
from rquant.paper_research import FrozenPaperResearchInput, PaperResearchAdapterCatalog, PaperResearchRunParameters
from rquant.paper_research_adapter import paper_research_adapter_registry, write_paper_research_input
from rquant.paper_research_source import paper_research_code_identity, publish_paper_research_input, verify_paper_snapshot_source
from rquant.research_catalog import ResearchCatalog
from rquant.research_run_spec import ResearchParameter
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_paper_portfolio_ledger_views import filled, ledger_source
from tests.unit.test_paper_research_admission import spec_for
from tests.unit.test_paper_signal_worker import EXECUTION_TIME
from tests.unit.test_strategy_template_adapter import claimed


def input_fixture(tmp_path):
    broker, _, _, runtime = filled(tmp_path)
    configuration = runtime.state.configuration
    catalog = PaperResearchAdapterCatalog(metadata_identity=runtime.state.identity(), configuration=configuration,
                                          source_code_identity=paper_research_code_identity())
    source = freeze_paper_reconcile(ledger_source(broker), configuration, as_of=EXECUTION_TIME, prices={"600000.SH": 1})
    return runtime, FrozenPaperResearchInput(task_name="paper_reconcile", catalog=catalog, code_sha="a"*40,
                                            available_at=EXECUTION_TIME, reconcile=source)


def source_spec(value, identity):
    spec = spec_for(value.catalog)
    parameters = PaperResearchRunParameters.from_input(value, request_id="11111111-1111-4111-8111-111111111111")
    data = spec.model_dump(mode="python")
    data["dataset_snapshot"] = identity
    data["parameters"]["arguments"] = tuple(ResearchParameter(name=key, kind="integer" if type(item) is int else "text", value=item)
                                             for key, item in parameters.model_dump(mode="python").items())
    return type(spec).model_validate(data)


def test_real_original_materializer_and_child_entry_reconcile_private_copy(tmp_path) -> None:
    _, value = input_fixture(tmp_path)
    metadata_path, lake = tmp_path/"metadata.duckdb", tmp_path/"lake"
    with DuckDBStore(metadata_path) as metadata:
        published = publish_paper_research_input(value, metadata_store=metadata, source_path=tmp_path/"private"/"input.duckdb",
                                                 research_catalog=ResearchCatalog(tmp_path/"catalog.duckdb"), lake_root=lake, now=EXECUTION_TIME)
        assert published.gate_decision.research_status == "exploratory" and published.gate_decision.allowed
        binding = metadata.get_dataset_snapshot_binding(published.identity.snapshot_id)
        assert binding.manifest.dependency_contract_version == "paper-research-input/v1"
        assert len(binding.manifest.artifacts) == 1
    registry = paper_research_adapter_registry(value.catalog)
    spec = source_spec(value, published.identity)
    config = builtin_lab_shard_configuration(catalog_path=metadata_path, forbidden_paths=(), snapshot_root=tmp_path/"snapshots",
                                             research_lake_root=lake, paper_catalog=value.catalog)
    result = execute_builtin_lab_shard(config, claimed(registry, spec), runtime_code_sha=spec.code_sha)
    assert result.tables[0].frame.iloc[0]["input_hash"] == value.fingerprint
    from rquant.paper_reconcile import PaperReconcileResult
    content = PaperReconcileResult.model_validate_json(result.tables[1].frame.iloc[0]["payload"])
    assert content.status == "consistent" and content.account.cash == 195 and content.account.holdings[0].quantity == 800
    artifact = next(lake.rglob("*.parquet"))
    artifact.write_bytes(b"changed immutable source")
    with pytest.raises((ValueError, PermissionError), match="hash|size|artifact|source"):
        execute_builtin_lab_shard(config, claimed(registry, spec), runtime_code_sha=spec.code_sha)


def test_exact_source_rejects_future_code_owner_or_payload_exchange(tmp_path) -> None:
    _, value = input_fixture(tmp_path)
    with duckdb.connect(":memory:") as connection:
        write_paper_research_input(connection, value)
        args = dict(task_name=value.task_name, code_sha=value.code_sha, start_date=value.dates[0], end_date=value.dates[1],
                    input_hash=value.fingerprint, as_of=EXECUTION_TIME, catalog=value.catalog)
        for changed in ({"as_of": EXECUTION_TIME-timedelta(seconds=1)}, {"code_sha": "b"*40},
                        {"input_hash": "0"*64}, {"task_name": "paper_arbitrary"}):
            with pytest.raises(ValueError):
                verify_paper_snapshot_source(connection, **{**args, **changed})
        raw = value.model_dump(mode="json")
        raw["reconcile"]["expected"]["account"]["cash"] = "196"
        import json
        connection.execute("UPDATE paper_research_input SET payload=?", [json.dumps(raw)])
        with pytest.raises(ValueError):
            verify_paper_snapshot_source(connection, **args)
