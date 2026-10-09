from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest


@pytest.fixture(scope="module")
def prepared_source(tmp_path_factory: pytest.TempPathFactory) -> SimpleNamespace:
    import rquant.storage.duckdb as storage
    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog
    from rquant.minute_backtest_parameter_definition import minute_parameter_research_registry
    from rquant.minute_backtest_parameter_fact_sources import build_minute_parameter_source_seed
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterFactSourceReference, MinuteParameterPreparedPublication,
        MinuteParameterReplayCatalog, publish_minute_parameter_input,
    )
    from rquant.minute_backtest_parameters import MinuteParameterSet
    from rquant.research_catalog import ResearchCatalog
    from rquant.storage.duckdb import DuckDBStore
    from tests.support.minute_parameter_formal_fixture import parameter_source_seed

    original_settings = storage._settings
    storage._settings = lambda: SimpleNamespace(primary_writer_gate_path=None)
    root = tmp_path_factory.mktemp("complete-parameter-formal")
    try:
        seed = parameter_source_seed(root / "facts", days=(date(2026, 7, 31), date(2026, 8, 3), date(2026, 8, 4)), sparse=True)
        lake, snapshots, operations = (root / name for name in ("lake", "metadata-snapshots", "prepared"))
        for path in (lake, snapshots, operations):
            path.mkdir(mode=0o700)
        baseline_root = root / "baseline"
        baseline_root.mkdir(mode=0o700)
        with DuckDBStore(baseline_root / "metadata.duckdb") as store:
            store.path.chmod(0o600)
            baseline = publish_minute_parameter_input(seed, metadata_store=store,
                source_path=baseline_root / "source.duckdb", receipt_path=baseline_root / "receipt.json",
                catalog=ResearchCatalog(root / "catalog.duckdb"), lake_root=lake,
                installed_policies=(seed.provenance.visibility_policy,), now=seed.provenance.published_at)
        with ImmutableDuckDBMetadataCatalog.open(baseline_root / "metadata.duckdb", snapshot_root=snapshots) as metadata:
            identity = metadata.descriptor
        fact = MinuteParameterFactSourceReference(**baseline.reference.model_dump(mode="python"),
            full_input_hash=baseline.receipt.frozen.full_input_hash, metadata_identity=identity,
            display_name="三日完整合成参数事实", source_nature="synthetic_validation",
            supported_parameter_names=("paper.stop_loss_pct",))
        catalog = MinuteParameterReplayCatalog(fact_sources=(fact,), prepared_root=operations,
            snapshot_root=snapshots, research_lake_root=lake, installed_policies=(seed.provenance.visibility_policy,))
        params = MinuteParameterSet(parameters=seed.runtime.parameters.parameters.model_copy(update={
            "paper": seed.runtime.parameters.parameters.paper.model_copy(update={"stop_loss_pct": 0.012346})}))
        operation = operations / "original-backend-operation"
        operation.mkdir(mode=0o700)
        derived = build_minute_parameter_source_seed(baseline.receipt.frozen, parameters=params,
            definitions_root=root / "registered-definitions", candidate_root=operation / "candidate-originals",
            source_key="synthetic.complete-parameter-formal", now=seed.provenance.published_at,
            visibility_policy=seed.provenance.visibility_policy)
        with DuckDBStore(operation / "metadata.duckdb") as metadata:
            metadata.path.chmod(0o600)
            published = publish_minute_parameter_input(derived, metadata_store=metadata,
                source_path=operation / "source.duckdb", receipt_path=operation / "receipt.json",
                catalog=ResearchCatalog(root / "catalog.duckdb"), lake_root=lake,
                installed_policies=catalog.installed_policies, now=derived.provenance.published_at)
        with ImmutableDuckDBMetadataCatalog.open(operation / "metadata.duckdb", snapshot_root=snapshots) as metadata:
            identity = metadata.descriptor
        carrier = MinuteParameterPreparedPublication.from_published(published, baseline_reference=fact,
            baseline_receipt=baseline.receipt, metadata_identity=identity)
        definitions = ImmutableDefinitionRegistry(root / "registered-definitions",
            execution_registry=minute_parameter_research_registry(params, producer_commit=seed.runtime.producer_commit))
        yield SimpleNamespace(root=root, published=published, baseline=baseline, catalog=catalog,
            carrier=carrier, definitions=definitions, params=params, now=derived.provenance.published_at,
            synthetic=True)
    finally:
        storage._settings = original_settings


def _protocol(value: SimpleNamespace) -> object:
    from rquant.experiment_registry import DateRange
    from rquant.minute_backtest_formal import MinuteExperimentProtocol

    dates = value.published.receipt.frozen.runtime.daily_trade_dates
    return MinuteExperimentProtocol(train_range=DateRange(start_date=dates[0], end_date=dates[0]),
        validation_range=DateRange(start_date=dates[1], end_date=dates[1]),
        frozen_outer_test_range=DateRange(start_date=dates[2], end_date=dates[2]))


def test_new_parameter_plan_uses_complete_prepared_source_and_original_formal_identity(prepared_source: SimpleNamespace) -> None:
    from rquant.minute_backtest_parameter_formal import build_minute_parameter_plan

    value = prepared_source
    prepared = build_minute_parameter_plan(value.published.receipt.frozen, value.published,
        prepared_publication=value.carrier, catalog=value.catalog, definitions=value.definitions,
        protocol=_protocol(value), now=value.now + timedelta(seconds=5), deadline=value.now + timedelta(hours=1))
    submitted = prepared.submission(job_id=uuid4())
    assert submitted.spec.schema_version == 3
    assert submitted.spec.parameters.strategy_name == "minute_parameter_replay"
    assert submitted.spec.strategy_execution.adapter_id == "minute-parameter-replay"
    assert submitted.spec.strategy_execution.adapter_version == "1"
    assert submitted.spec.experiment.schema_version == 2
    assert submitted.spec.catalog_owner_eligible
    assert prepared.formal_plan.preregistered_at == value.now + timedelta(seconds=5)
    assert prepared.prepared_publication == value.carrier
    assert prepared.frozen.runtime.parameters == value.params
    assert prepared.frozen.runtime.strategy.strategy_id == value.params.definition_id
    assert prepared.frozen.formal_work.work_units < prepared.prepared_publication.work_units <= 20_000
    assert next(item.value for item in submitted.spec.parameters.arguments if item.name == "prepared_publication_json") == value.carrier.model_dump_json(exclude_computed_fields=True)


def test_new_parameter_plan_rejects_outside_protocol_and_future_publication(prepared_source: SimpleNamespace) -> None:
    from rquant.experiment_registry import DateRange
    from rquant.minute_backtest_parameter_formal import build_minute_parameter_plan

    value = prepared_source
    protocol = _protocol(value)
    outside = protocol.model_copy(update={"frozen_outer_test_range": DateRange(
        start_date=value.published.receipt.frozen.runtime.end_date + timedelta(days=1),
        end_date=value.published.receipt.frozen.runtime.end_date + timedelta(days=1))})
    common = dict(prepared_publication=value.carrier, catalog=value.catalog, definitions=value.definitions,
        now=value.now, deadline=value.now + timedelta(hours=1))
    with pytest.raises(PermissionError, match="protocol.*full.*source|source.*protocol"):
        build_minute_parameter_plan(value.published.receipt.frozen, value.published, protocol=outside, **common)
    with pytest.raises(PermissionError, match="preregistration/publication"):
        build_minute_parameter_plan(value.published.receipt.frozen, value.published, protocol=protocol,
            **(common | {"now": value.now - timedelta(seconds=1)}))


def test_parameter_publication_preparation_recovers_same_full_reference_without_writes(
    prepared_source: SimpleNamespace,
) -> None:
    from rquant.minute_backtest_commands import MinuteParameterRunConfig
    from rquant.minute_backtest_parameter_preparation import prepare_minute_parameter_publication
    from rquant.minute_backtest_parameters import MinuteParameterSet
    from rquant.runtime_contracts import canonical_sha256

    value = prepared_source
    parameters = MinuteParameterSet(parameters=value.params.parameters.model_copy(update={
        "paper": value.params.parameters.paper.model_copy(update={"stop_loss_pct": 0.012347})}))
    config = MinuteParameterRunConfig(source_key=value.baseline.reference.source_key,
        source_version=value.baseline.reference.source_version,
        full_input_hash=value.baseline.receipt.frozen.full_input_hash, parameters=parameters,
        protocol=_protocol(value), deadline=value.now + timedelta(hours=1))
    operation_key = canonical_sha256({"actual_authenticated_command": "synthetic-parameter-prepare"})
    kwargs = dict(owner_id=value.baseline.reference.owner_id, operation_key=operation_key, catalog=value.catalog,
        definitions_root=value.root / "registered-definitions", now=value.now + timedelta(seconds=10))
    first = prepare_minute_parameter_publication(config, **kwargs)
    assert first.published.receipt.frozen.runtime.parameters == parameters
    assert value.catalog.resolve_prepared(first.prepared_publication) == first.published.receipt
    directory = first.prepared_publication.source.path.parent
    before = {path.relative_to(directory).as_posix(): (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        for path in directory.rglob("*") if path.is_file()}
    restored = prepare_minute_parameter_publication(config,
        **(kwargs | {"now": value.now + timedelta(seconds=20)}))
    assert restored == first
    assert before == {path.relative_to(directory).as_posix(): (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        for path in directory.rglob("*") if path.is_file()}
    unsupported = MinuteParameterSet(parameters=parameters.parameters.model_copy(update={"max_hold_days": 2}))
    with pytest.raises(PermissionError, match="does not support every changed parameter"):
        prepare_minute_parameter_publication(config.model_copy(update={"parameters": unsupported}),
            **(kwargs | {"operation_key": "e" * 64}))
