from __future__ import annotations

from types import FunctionType
from datetime import timedelta
from uuid import uuid4

import pytest

from tests.unit.test_minute_backtest_parameter_formal import prepared_source, _protocol
from rquant.minute_backtest_parameter_definition import minute_parameter_validation_request


def _input_semantics() -> str | None:
    from rquant import minute_backtest_parameter_study_projection as projection
    from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet

    complete = getattr(projection, "_minute_study_input_semantic_fingerprint", None)
    if complete is None:
        # The old certificate carries only result semantics. Exercise that real
        # proof rather than treating a missing import as the behavior failure.
        return projection.projection_complete_semantic_fingerprint()
    parameters = MinuteParameterSet(parameters=MinuteNShapeParameters())
    observed = complete(parameters, producer_commit="a" * 40)
    if observed is None:
        # Preserve an opaque fallback while reporting its exact proof boundary.
        projection._minute_study_input_semantic_components(parameters, producer_commit="a" * 40)
    return observed


def test_current_input_gate_live_binding_cannot_reuse_result_only_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.minute_backtest_parameter_producer import MinuteParameterReplayCatalog

    gate = MinuteParameterReplayCatalog._metadata_gate.__wrapped__
    assert type(gate) is FunctionType
    before = _input_semantics()
    assert before is not None, "the supported plain recipe has no complete current proof"
    monkeypatch.setattr(gate, "__kwdefaults__", {"retained_nested_gate_policy": [1]})
    assert _input_semantics() != before, "a current input gate binding changed without invalidating the proof"


def test_complete_input_semantics_is_stable_in_a_fresh_process() -> None:
    import os
    import subprocess
    import sys

    before = _input_semantics()
    assert before is not None and _input_semantics() == before
    code = (
        "from rquant.minute_backtest_parameter_study_projection import _minute_study_input_semantic_fingerprint\n"
        "from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet\n"
        "print(_minute_study_input_semantic_fingerprint(MinuteParameterSet(parameters=MinuteNShapeParameters()),producer_commit='a'*40))\n"
    )
    keys = ("PATH", "PYTHONPATH", "RQUANT_DISABLE_DOTENV", "TUSHARE_TOKEN_MAIN", "DATA_DIR", "DUCKDB_PATH", "PARQUET_DIR", "LOG_DIR")
    child = subprocess.run((sys.executable, "-B", "-c", code), env={key: os.environ[key] for key in keys},
        check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert child.stdout.strip() == before, "the complete input proof cannot cross its publishing process"


@minute_parameter_validation_request
def test_complete_prepared_input_proof_carries_original_records_plan_and_full_files(
    prepared_source: object,
) -> None:
    from rquant import minute_backtest_parameter_study_projection as projection
    from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalReplayAdapter
    from rquant.minute_backtest_parameter_formal import build_minute_parameter_plan
    from rquant.strategy_job_adapters import StrategyJobAdapterRegistry
    from rquant.runtime_contracts import canonical_sha256
    import hashlib

    value = prepared_source
    as_of = value.now + timedelta(seconds=5)
    prepared = build_minute_parameter_plan(value.published.receipt.frozen, value.published,
        prepared_publication=value.carrier, catalog=value.catalog, definitions=value.definitions,
        protocol=_protocol(value), now=as_of, deadline=as_of + timedelta(hours=1))
    spec = prepared.submission(job_id=uuid4()).spec
    adapter = MinuteParameterFormalReplayAdapter(value.catalog)
    parameters = adapter.parameters(spec)
    publication = adapter.expected(parameters)
    shard, = StrategyJobAdapterRegistry((adapter,)).plan(spec)
    capture = getattr(projection, "_capture_minute_study_input_verification", None)
    assert callable(capture), "the complete accepted input read has no typed reusable proof"
    proof = capture(catalog=value.catalog, prepared=value.carrier, publication=publication,
        parameters=parameters, definitions=value.definitions, shard=shard, as_of=as_of)
    assert proof is not None, "the original supported complete input proof is opaque"
    assert proof.prepared == value.carrier and proof.parameters == parameters
    assert proof.prepared_binding == publication.binding
    assert proof.baseline_binding == value.baseline.receipt.binding
    assert proof.catalog_sha256 == canonical_sha256(value.catalog.model_dump(mode="json"))
    assert proof.native_registration == publication.frozen.native_registration
    assert proof.parameter_registration == publication.frozen.wrapper_registration
    assert publication.frozen.feature_registration in proof.feature_registrations
    assert proof.result_budget == publication.frozen.result_budget
    assert proof.expected_publication_hash == canonical_sha256(publication.model_dump(mode="json"))
    assert proof.accepted_shard.payload_sha256 == hashlib.sha256(shard.payload_json.encode()).hexdigest()
    assert proof.accepted_shard.shard_id == shard.shard_id
    assert proof.accepted_shard.work_units == parameters.work_units
    roles = tuple(item.role for item in proof.files)
    assert all(role in roles for role in ("prepared_source", "prepared_receipt", "prepared_metadata",
        "prepared_manifest", "prepared_artifact", "baseline_source", "baseline_receipt",
        "baseline_metadata", "baseline_manifest", "baseline_artifact"))
    assert proof.input_semantic_fingerprint == projection._minute_study_input_semantic_fingerprint(
        value.params, producer_commit=spec.code_sha)
    assert type(proof).model_validate_json(proof.model_dump_json()) == proof
