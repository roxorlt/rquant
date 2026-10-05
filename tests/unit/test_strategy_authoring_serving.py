"""The original Lab source and S4 publisher retain exact template projections."""

from __future__ import annotations

import pytest

from rquant.serving_page_projection_source import DuckDBLabPageProjectionSource, LabPageProjectionSnapshot
from rquant.serving_read_models import ServingProjectionInput
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_authoring_projection import StrategyAuthoringProjectionSource, build_strategy_authoring_snapshot, project_strategy_authoring, validate_strategy_authoring_projections
from tests.unit.test_strategy_authoring import NOW, catalog, draft, store


def test_three_exact_tables_pass_original_serving_and_reject_mixed_generation(tmp_path) -> None:
    target = store(tmp_path)
    target.save(draft(), owner_id="alice", catalog=catalog())
    snapshot = build_strategy_authoring_snapshot(target, available_at=NOW, source_catalogs=(catalog(),))
    payloads = project_strategy_authoring(snapshot).serving_payloads()
    bound = tuple(ServingProjectionInput.bind(p, owner_dataset_id="lab_jobs", owner_generation_id="a" * 64) for p in payloads)
    assert validate_strategy_authoring_projections({p.table_name: p for p in bound}) == snapshot
    foreign = ServingProjectionInput.bind(payloads[-1], owner_dataset_id="lab_jobs", owner_generation_id="b" * 64)
    with pytest.raises(ValueError, match="generation"):
        validate_strategy_authoring_projections({p.table_name: p for p in (*bound[:-1], foreign)})
    with pytest.raises(ValueError, match="incomplete"):
        LabPageProjectionSnapshot.create(available_at=NOW, strategy_definition_projections=payloads[:-1])


def test_original_lab_page_source_uses_confirmed_metadata_source_and_keeps_default_empty(tmp_path) -> None:
    target = store(tmp_path)
    saved = target.save(draft(), owner_id="alice", catalog=catalog())
    research = tmp_path / "research-readonly.duckdb"
    with DuckDBStore(research):
        pass
    original = DuckDBLabPageProjectionSource(research)(NOW)
    assert {p.table_name for p in original.projections} == {"research_gate_metadata", "data_audit_status", "data_audit_issue"}
    source = StrategyAuthoringProjectionSource(target, expected_identity=target.identity(), source_catalog_provider=lambda observed: (catalog(),))
    actual = DuckDBLabPageProjectionSource(research, strategy_authoring_source=source)(NOW)
    decoded = validate_strategy_authoring_projections({p.table_name: p for p in actual.projections})
    assert decoded.versions[0].metadata.head == saved.head
    target.path.rename(tmp_path / "old-metadata.sqlite")
    store(tmp_path)
    with pytest.raises(RuntimeError, match="identity"):
        DuckDBLabPageProjectionSource(research, strategy_authoring_source=source)(NOW)
