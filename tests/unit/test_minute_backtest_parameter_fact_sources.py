from __future__ import annotations

from pathlib import Path
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest


def test_parameter_derivation_keeps_full_raw_and_facts_but_binds_a_fresh_complete_recipe(tmp_path: Path) -> None:
    from rquant.minute_backtest_parameter_fact_sources import (
        build_minute_parameter_source_seed, verify_minute_parameter_derivation,
    )
    from rquant.minute_backtest_parameters import MinuteParameterSet
    from rquant.minute_backtest_producer import minute_metadata_identities
    from tests.support.minute_parameter_formal_fixture import parameter_source_seed

    seed = parameter_source_seed(tmp_path / "baseline")
    audit, snapshot = minute_metadata_identities(seed)
    baseline = seed.freeze(audit_run_id=audit.audit_run_id, dataset_snapshot_id=snapshot.snapshot_id)
    parameters = MinuteParameterSet.model_validate_json(seed.runtime.parameters.model_dump_json())
    config = parameters.parameters.model_copy(update={"paper": parameters.parameters.paper.model_copy(
        update={"stop_loss_pct": 0.012346})})
    parameters = MinuteParameterSet(parameters=config)
    derived = build_minute_parameter_source_seed(baseline, parameters=parameters,
        definitions_root=tmp_path / "new-definitions", candidate_root=tmp_path / "new-candidates",
        source_key="synthetic.actual-parameter-derivative", now=seed.provenance.published_at,
        visibility_policy=seed.provenance.visibility_policy)
    derived_audit, derived_snapshot = minute_metadata_identities(derived)
    value = derived.freeze(audit_run_id=derived_audit.audit_run_id, dataset_snapshot_id=derived_snapshot.snapshot_id)
    verify_minute_parameter_derivation(value, baseline)
    assert value.runtime.parameters == parameters
    assert value.runtime.strategy.strategy_id != baseline.runtime.strategy.strategy_id
    assert value.full_input_hash != baseline.full_input_hash
    assert value.runtime.session_facts == baseline.runtime.session_facts
    common = {item.relative_path: item.payload() for item in baseline.runtime.materials
        if not item.relative_path.startswith("candidates/")}
    assert {item.relative_path: item.payload() for item in value.runtime.materials
        if not item.relative_path.startswith("candidates/")} == common
    assert value.provenance.source_kind == "reconstructed"
    assert value.native_registration.available_at == seed.provenance.published_at
    changed = value.model_copy(update={"runtime": value.runtime.model_copy(update={
        "warmup_available_at": value.runtime.warmup_available_at.replace(year=2025)})})
    with pytest.raises(PermissionError, match="original raw|runtime basis"):
        verify_minute_parameter_derivation(changed, baseline)


def test_prepared_dynamic_reference_opens_complete_baseline_metadata_and_original_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.storage.duckdb as storage
    from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog
    from rquant.minute_backtest_parameter_fact_sources import build_minute_parameter_source_seed
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterFactSourceReference, MinuteParameterPreparedPublication,
        MinuteParameterReplayCatalog, publish_minute_parameter_input,
    )
    from rquant.minute_backtest_parameters import MinuteParameterSet
    from rquant.research_catalog import ResearchCatalog
    from rquant.storage.duckdb import DuckDBStore
    from tests.support.minute_parameter_formal_fixture import parameter_source_seed

    monkeypatch.setattr(storage, "_settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
    seed = parameter_source_seed(tmp_path / "facts")
    lake, snapshots, prepared_root = tmp_path / "lake", tmp_path / "metadata-snapshots", tmp_path / "prepared"
    for directory in (lake, snapshots, prepared_root):
        directory.mkdir(mode=0o700)
    baseline_root = tmp_path / "baseline-publication"
    baseline_root.mkdir(mode=0o700)
    with DuckDBStore(baseline_root / "metadata.duckdb") as metadata:
        metadata.path.chmod(0o600)
        published = publish_minute_parameter_input(seed, metadata_store=metadata,
            source_path=baseline_root / "source.duckdb", receipt_path=baseline_root / "receipt.json",
            catalog=ResearchCatalog(tmp_path / "catalog.duckdb"), lake_root=lake,
            installed_policies=(seed.provenance.visibility_policy,), now=seed.provenance.published_at)
    with ImmutableDuckDBMetadataCatalog.open(baseline_root / "metadata.duckdb", snapshot_root=snapshots) as metadata:
        baseline_identity = metadata.descriptor
    reference = MinuteParameterFactSourceReference(**published.reference.model_dump(mode="python"),
        full_input_hash=published.receipt.frozen.full_input_hash, metadata_identity=baseline_identity,
        display_name="完整合成参数事实", source_nature="synthetic_validation", supported_parameter_names=("paper.stop_loss_pct",))
    installed = MinuteParameterReplayCatalog(fact_sources=(reference,), prepared_root=prepared_root,
        snapshot_root=snapshots, research_lake_root=lake, installed_policies=(seed.provenance.visibility_policy,))
    baseline = installed.resolve_fact(source_key=reference.source_key, source_version=reference.source_version,
        owner_id=reference.owner_id, full_input_hash=reference.full_input_hash)
    recipe = MinuteParameterSet(parameters=seed.runtime.parameters.parameters.model_copy(update={
        "paper": seed.runtime.parameters.parameters.paper.model_copy(update={"stop_loss_pct": 0.012346})}))
    derived = build_minute_parameter_source_seed(baseline.frozen, parameters=recipe,
        definitions_root=tmp_path / "derived-definitions", candidate_root=tmp_path / "derived-candidates",
        source_key="synthetic.new-complete-recipe", now=datetime.now(UTC), visibility_policy=seed.provenance.visibility_policy)
    output = prepared_root / "backend-created-operation"
    output.mkdir(mode=0o700)
    with DuckDBStore(output / "metadata.duckdb") as metadata:
        metadata.path.chmod(0o600)
        derivative = publish_minute_parameter_input(derived, metadata_store=metadata,
            source_path=output / "source.duckdb", receipt_path=output / "receipt.json",
            catalog=ResearchCatalog(tmp_path / "catalog.duckdb"), lake_root=lake,
            installed_policies=installed.installed_policies, now=derived.provenance.published_at)
    with ImmutableDuckDBMetadataCatalog.open(output / "metadata.duckdb", snapshot_root=snapshots) as metadata:
        identity = metadata.descriptor
    prepared = MinuteParameterPreparedPublication.from_published(derivative, baseline_reference=reference,
        baseline_receipt=baseline, metadata_identity=identity)
    assert prepared.loaded_bytes < 16 * 1024 * 1024
    assert prepared.work_units == baseline.frozen.formal_work.work_units + derivative.receipt.frozen.formal_work.work_units
    restored = MinuteParameterPreparedPublication.model_validate_json(prepared.model_dump_json())
    assert installed.resolve_prepared(restored) == derivative.receipt
    with installed.open_prepared(restored) as session:
        assert session._minute_gate_receipt == derivative.receipt
        source = derivative.receipt.binding.manifest.artifacts[0]
        copy, = session._session_dir.glob("*.parquet")
        assert copy.stat().st_ino != (lake / source.relative_path).stat().st_ino
    with pytest.raises(PermissionError, match="work/loaded bytes"):
        installed.resolve_prepared(prepared.model_copy(update={"loaded_bytes": prepared.loaded_bytes-1}))
    outside = prepared.model_copy(update={"source": prepared.source.model_copy(update={
        "path": tmp_path / "outside/source.duckdb"})})
    with pytest.raises(PermissionError, match="producer root"):
        installed.resolve_prepared(outside)
    (output / "prepared-reference.json").write_text(prepared.model_dump_json())
