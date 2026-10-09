from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import json

import pytest


def test_parameter_publication_has_a_separate_complete_receipt_contract() -> None:
    from rquant.minute_backtest_parameter_contracts import FrozenMinuteParameterResearchInput, MinuteParameterSourceSeed
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterPublicationReceipt, MinuteParameterPublicationReference,
        MinuteParameterReplayCatalog,
    )
    from rquant.minute_backtest_producer import MinutePublicationReceipt, MinutePublicationReference

    assert MinuteParameterPublicationReceipt.model_fields["seed"].annotation is MinuteParameterSourceSeed
    assert MinuteParameterPublicationReceipt.model_fields["frozen"].annotation is FrozenMinuteParameterResearchInput
    assert MinuteParameterPublicationReference._receipt_model is not MinutePublicationReference._receipt_model
    assert MinuteParameterReplayCatalog.model_fields["contract"].default == "minute-parameter-replay-catalog/v1"
    assert "contract" not in MinutePublicationReceipt.model_fields


@pytest.fixture(scope="module")
def complete_seed(tmp_path_factory: pytest.TempPathFactory):
    from tests.support.minute_parameter_formal_fixture import parameter_source_seed

    return parameter_source_seed(tmp_path_factory.mktemp("complete-parameter-source"))


def test_complete_parameter_source_session_facts_require_their_original_bytes(complete_seed) -> None:
    from rquant.minute_backtest_parameter_contracts import MinuteParameterSourceSeed

    data = complete_seed.model_dump(mode="json")
    data["runtime"]["session_facts"][0]["previous_close"] += 0.01
    with pytest.raises(ValueError, match="session fact original"):
        MinuteParameterSourceSeed.model_validate_json(json.dumps(data))


def test_parameter_publication_uses_original_metadata_snapshot_and_independent_receipt(
    complete_seed, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.storage.duckdb as storage
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterReplayCatalog, publish_minute_parameter_input,
    )
    from rquant.minute_backtest_producer import minute_metadata_identities, read_minute_formal_input_table
    from rquant.research_catalog import ResearchCatalog
    from rquant.storage.duckdb import DuckDBStore
    import duckdb

    monkeypatch.setattr(storage, "_settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
    private = tmp_path / "publication"
    private.mkdir(mode=0o700)
    with DuckDBStore(private / "metadata.duckdb") as metadata:
        published = publish_minute_parameter_input(complete_seed, metadata_store=metadata,
            source_path=private / "source.duckdb", receipt_path=private / "receipt.json",
            catalog=ResearchCatalog(private / "catalog.duckdb"), lake_root=private / "lake",
            installed_policies=(complete_seed.provenance.visibility_policy,), now=complete_seed.provenance.published_at)
        audit, snapshot = minute_metadata_identities(complete_seed)
        assert published.identity.snapshot_id == snapshot.snapshot_id
        assert published.identity.audit_run_id == audit.audit_run_id
        assert published.gate_decision.allowed
        catalog = MinuteParameterReplayCatalog(entries=(published.reference,),
            installed_policies=(complete_seed.provenance.visibility_policy,))
        recovered = MinuteParameterReplayCatalog.model_validate_json(catalog.model_dump_json()).resolve(
            source_key=complete_seed.runtime.source_key, source_version=complete_seed.runtime.source_version,
            owner_id=complete_seed.runtime.owner_id)
        assert recovered == published.receipt
        assert recovered.frozen.runtime.parameters == complete_seed.runtime.parameters
        assert recovered.frozen.runtime.parameter_work == complete_seed.runtime.parameter_work
        assert recovered.frozen.source_content_seed == complete_seed
        assert metadata.get_dataset_snapshot_binding(snapshot.snapshot_id) == recovered.binding
        with duckdb.connect(str(published.reference.source.path), read_only=True) as connection:
            with pytest.raises(PermissionError, match="exactly one input table"):
                read_minute_formal_input_table(connection)
        wrong = published.reference.model_copy(update={"owner_id": "another-owner"})
        with pytest.raises(PermissionError, match="key/version/owner"):
            wrong.load(installed_policies=catalog.installed_policies)
    (private / "published.json").write_text(published.model_dump_json())
