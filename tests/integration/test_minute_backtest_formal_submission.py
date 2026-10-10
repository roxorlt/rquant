from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import DateRange, ExperimentRegistry
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.lab_worker_registry import builtin_lab_shard_configuration, execute_builtin_lab_shard
from rquant.minute_backtest_formal import MinuteExperimentProtocol, register_minute_plan
from rquant.minute_backtest_formal_adapter import MinuteFormalReplayResult, minute_formal_adapter_registry
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_job_adapters import LabShardExecutionWireResult
from tests.unit.test_minute_backtest_formal import publication
from tests.unit.test_minute_backtest_producer import NOW, original_fixture


def test_original_formal_submit_claim_worker_and_complete_results(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import rquant.storage.duckdb as storage

    monkeypatch.setattr(storage, "_settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
    metadata_path = tmp_path / "metadata.duckdb"
    with DuckDBStore(metadata_path) as metadata:
        published, catalog, lake = publication(tmp_path, metadata)
    value = published.receipt.frozen
    definitions = ImmutableDefinitionRegistry(tmp_path / "definitions", execution_registry=BuiltinStrategyEvaluatorRegistry(
        producer_commit=value.runtime.producer_commit).trusted_executable_registry())
    trust = tmp_path / "experiment-trust"
    trust.mkdir(mode=0o700)
    experiments = ExperimentRegistry(trust / "experiments.sqlite3", managed_trust_root=trust)
    prepared = register_minute_plan(value, published, catalog=catalog, definitions=definitions, experiments=experiments,
        protocol=MinuteExperimentProtocol(
            train_range=DateRange(start_date=value.runtime.start_date - timedelta(days=2), end_date=value.runtime.start_date - timedelta(days=2)),
            validation_range=DateRange(start_date=value.runtime.start_date - timedelta(days=1), end_date=value.runtime.start_date - timedelta(days=1)),
            frozen_outer_test_range=DateRange(start_date=value.runtime.start_date, end_date=value.runtime.end_date)),
        now=NOW, deadline=NOW + timedelta(hours=1))
    submitted = prepared.submission(job_id=uuid4())
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    spool = LabCommandSpool(tmp_path / "commands")
    facade = LabCommandSubmissionFacade(reader=reader, spool=spool, experiment_registry=experiments,
        definition_registry=definitions, clock=lambda: NOW)
    receipt = facade.submit_create(submitted.command, interaction_key="minute-formal-real-original-chain")
    assert receipt.result == "submitted"
    entry, = spool.pending()
    assert entry.envelope.command.spec == submitted.spec
    intent = experiments.get_submission_intent_for_job(submitted.command.job_id)
    assert intent is not None
    lease = jobs.acquire_scheduler_lease(owner_id="minute-synthetic-scheduler", lease_seconds=3600, now=NOW)
    accepted = jobs.apply_command(entry.envelope, lease=lease, now=NOW,
        submission_authority=lambda envelope, at: facade.validate_prepared_experiment_submission(envelope, observed_at=at))
    assert accepted.status == "applied"
    spool.ack(entry, accepted)
    registry = minute_formal_adapter_registry(catalog)
    plan = registry.plan(submitted.spec)
    jobs.plan_job(submitted.command.job_id, plan, lease=lease, now=NOW)
    claim = jobs.claim_next_shard(worker_id="minute-synthetic-worker", shard_lease_seconds=3600, lease=lease, now=NOW)
    assert claim is not None
    validated = registry.validate_claim(claim)
    assert validated.spec == submitted.spec
    assert validated.claim == claim
    config = builtin_lab_shard_configuration(catalog_path=metadata_path, forbidden_paths=(),
        snapshot_root=tmp_path / "metadata-copies", research_lake_root=lake, minute_catalog=catalog)
    encoded_config = json.loads(config.model_dump_json())
    assert len(config.model_dump_json().encode()) < 1_048_576
    with pytest.raises(PermissionError, match="runtime code"):
        execute_builtin_lab_shard(encoded_config, validated, runtime_code_sha="b" * 40)
    missing = dict(encoded_config)
    missing.pop("minute_catalog")
    with pytest.raises(Exception, match="registry hash mismatch"):
        execute_builtin_lab_shard(missing, validated, runtime_code_sha=submitted.spec.code_sha)
    actual = execute_builtin_lab_shard(encoded_config, validated, runtime_code_sha=submitted.spec.code_sha)
    wire = LabShardExecutionWireResult.from_result(actual)
    restored_wire = LabShardExecutionWireResult.model_validate_json(wire.model_dump_json()).to_result()
    assert sum(x.byte_size for x in wire.tables) <= value.result_budget.total_bytes
    assert len(wire.model_dump_json().encode()) <= value.result_budget.wire_bytes
    assert (restored_wire.spec_hash, restored_wire.payload_hash, restored_wire.plan_hash) == (actual.spec_hash, actual.payload_hash, actual.plan_hash)
    assert (actual.shard_id, actual.spec_hash, actual.payload_hash, actual.plan_hash, actual.adapter_id, actual.adapter_version) == (
        claim.shard_id, claim.spec_hash, claim.payload_hash, claim.plan_hash, "minute-runtime-replay", "2")
    tables = {x.name: x.frame for x in actual.tables}
    assert len(tables) == 8
    result = MinuteFormalReplayResult.model_validate_json(tables["replay_summary"].iloc[0]["payload"])
    assert result.publication == published.receipt
    assert (result.full_input_hash, result.core_input_hash, result.seed_hash) == (
        value.full_input_hash, value.core_input_hash, value.source_content_seed.seed_hash)
    assert result.replay.input_hash == value.core_input_hash
    assert result.replay.signals and result.replay.orders and result.replay.fills
    original = original_fixture()
    native = result.replay.model_dump(mode="json")
    for field in original["zero_tolerance_fields"]:
        assert native[field] == original["minute_replay"][field]
    for actual_day, expected_day in zip(native["daily_valuations"], original["minute_replay"]["daily_valuations"], strict=True):
        assert actual_day.pop("input_hash") == value.core_input_hash
        expected = dict(expected_day)
        expected.pop("input_hash")
        assert actual_day == expected
    assert len(result.replay.daily_valuations) == 2
    assert result.replay.daily_status == "complete"
    assert result.publication.frozen.provenance.source_kind == "reconstructed"
    assert result.publication.frozen.provenance.visibility_policy is not None
    (tmp_path / "formal-result.json").write_text(result.model_dump_json(exclude_computed_fields=True))
