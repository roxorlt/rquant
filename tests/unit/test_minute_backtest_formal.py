from __future__ import annotations

import json
import hashlib
import importlib.util
import sys
from types import ModuleType
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import DateRange, ExperimentRegistry
from rquant.minute_backtest_formal import MinuteExperimentProtocol, build_minute_plan, register_minute_plan
from rquant.minute_backtest_formal_adapter import MinuteFormalReplayResult, minute_formal_adapter_registry
from rquant.minute_backtest_producer import MinuteReplayCatalog, _secure_private_bytes, publish_minute_input
from rquant.minute_backtest_publication_contracts import MinuteSourceContentSeed
from rquant.minute_backtest_runner import MinuteRuntimeReplayResult
from rquant.research_catalog import ResearchCatalog
from rquant.research_snapshot import ResearchExecutionSession
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
from tests.unit.test_minute_backtest_producer import NOW, metadata, original_fixture, source_seed, template_seed


def publication(tmp_path: Path, metadata: object) -> tuple[object, object, object]:
    seed = source_seed(tmp_path)
    source_root = tmp_path / "published"
    source_root.mkdir(mode=0o700)
    lake = tmp_path / "lake"
    lake.mkdir(mode=0o700)
    published = publish_minute_input(seed, metadata_store=metadata,
        source_path=source_root / "input.duckdb", receipt_path=source_root / "publication.json",
        catalog=ResearchCatalog(tmp_path / "catalog.duckdb"), lake_root=lake,
        installed_policies=(seed.provenance.visibility_policy,), now=NOW)
    installed = MinuteReplayCatalog(entries=(published.reference,), installed_policies=(seed.provenance.visibility_policy,))
    return published, installed, lake


def test_formal_submission_keeps_original_schema_and_complete_identity(tmp_path: Path, metadata: object) -> None:
    published, installed, lake = publication(tmp_path, metadata)
    frozen = published.receipt.frozen
    registry = ImmutableDefinitionRegistry(tmp_path / "definitions", execution_registry=BuiltinStrategyEvaluatorRegistry(
        producer_commit=frozen.runtime.producer_commit).trusted_executable_registry())
    trust = tmp_path / "experiment-trust"
    trust.mkdir(mode=0o700)
    experiments = ExperimentRegistry(trust / "experiments.sqlite3", managed_trust_root=trust)
    prepared = register_minute_plan(frozen, published, catalog=installed, definitions=registry,
        experiments=experiments, protocol=MinuteExperimentProtocol(
            train_range=DateRange(start_date=frozen.runtime.start_date, end_date=frozen.runtime.start_date),
            validation_range=DateRange(start_date=frozen.runtime.end_date, end_date=frozen.runtime.end_date),
            frozen_outer_test_range=DateRange(start_date=frozen.runtime.end_date + timedelta(days=1),
                end_date=frozen.runtime.end_date + timedelta(days=1))), now=NOW, deadline=NOW + timedelta(hours=1))
    submitted = prepared.submission(job_id=uuid4())
    assert submitted.spec.schema_version == 3
    assert submitted.spec.strategy_execution.adapter_version == "2"
    assert submitted.spec.strategy_execution.strategy_id == "minute_runtime_replay"
    assert submitted.spec.strategy_execution.strategy_executable_fingerprint == frozen.wrapper_registration.executable_fingerprint
    assert submitted.spec.experiment.schema_version == 2
    assert submitted.spec.catalog_owner_eligible
    assert submitted.spec.dataset_snapshot.snapshot_id == frozen.runtime.dataset_snapshot_id
    planned = minute_formal_adapter_registry(installed).plan(submitted.spec)
    assert len(planned) == 1
    assert planned[0].work_plan.work_units == frozen.formal_work.work_units
    assert prepared.frozen.native_registration == frozen.native_registration
    assert prepared.frozen.provenance.source_kind == "reconstructed"
    delayed = build_minute_plan(frozen, published, catalog=installed, definitions=registry,
        protocol=MinuteExperimentProtocol(train_range=prepared.formal_plan.spec.train_range,
            validation_range=prepared.formal_plan.spec.validation_range, frozen_outer_test_range=prepared.formal_plan.spec.frozen_outer_test_range),
        now=NOW + timedelta(seconds=5), deadline=NOW + timedelta(hours=1))
    assert delayed.formal_plan.preregistered_at == NOW + timedelta(seconds=5)
    assert delayed.formal_plan.spec == prepared.formal_plan.spec
    assert delayed.published == published
    with pytest.raises(PermissionError, match="preregistration/publication"):
        build_minute_plan(frozen, published, catalog=installed, definitions=registry,
            protocol=MinuteExperimentProtocol(train_range=prepared.formal_plan.spec.train_range,
                validation_range=prepared.formal_plan.spec.validation_range, frozen_outer_test_range=prepared.formal_plan.spec.frozen_outer_test_range),
            now=NOW - timedelta(seconds=1), deadline=NOW + timedelta(hours=1))
    artifact = published.receipt.binding.manifest.artifacts[0]
    artifact_path = lake / artifact.relative_path
    with ResearchExecutionSession(binding=published.receipt.binding, lake_root=lake) as session:
        copied, = session._session_dir.glob("*.parquet")
        assert (copied.stat().st_dev, copied.stat().st_ino) != (artifact_path.stat().st_dev, artifact_path.stat().st_ino)
        assert session._conn.execute("SHOW TABLES").fetchall() == [("minute_runtime_replay_input",)]
        assert session._conn.execute("SELECT input_hash FROM minute_runtime_replay_input").fetchone()[0] == frozen.full_input_hash
    original_bytes = artifact_path.read_bytes()
    mode = artifact_path.stat().st_mode & 0o777
    artifact_path.chmod(0o600)
    artifact_path.write_bytes(b"modified artifact")
    try:
        with pytest.raises(ValueError, match="(file|artifact|hash|size)"):
            with ResearchExecutionSession(binding=published.receipt.binding, lake_root=lake):
                pytest.fail("modified physical artifact was accepted")
    finally:
        artifact_path.write_bytes(original_bytes)
        artifact_path.chmod(mode)
    for key, version, owner in ((frozen.runtime.source_key + "-wrong", frozen.runtime.source_version, frozen.runtime.owner_id),
        (frozen.runtime.source_key, frozen.runtime.source_version + 1, frozen.runtime.owner_id),
        (frozen.runtime.source_key, frozen.runtime.source_version, frozen.runtime.owner_id + "-wrong")):
        with pytest.raises(PermissionError, match="exact installed"):
            installed.resolve(source_key=key, source_version=version, owner_id=owner)
    reference = published.reference
    missing = reference.receipt.path.with_suffix(".missing")
    reference.receipt.path.rename(missing)
    try:
        with pytest.raises(FileNotFoundError):
            installed.resolve(source_key=frozen.runtime.source_key, source_version=frozen.runtime.source_version, owner_id=frozen.runtime.owner_id)
    finally:
        missing.rename(reference.receipt.path)
    with pytest.raises(ValueError, match="replay.*(input|hash)|input.*hash"):
        MinuteFormalReplayResult(full_input_hash=frozen.full_input_hash, core_input_hash=frozen.core_input_hash,
            seed_hash=frozen.source_content_seed.seed_hash, native_registration_hash=frozen.native_registration.record_hash,
            wrapper_registration_hash=frozen.wrapper_registration.record_hash, publication=published.receipt,
            replay=MinuteRuntimeReplayResult.model_validate_json(json.dumps(original_fixture()["minute_replay"])))
    changed = published.receipt.model_dump(mode="json", exclude_computed_fields=True)
    changed["seed"]["provenance"]["capture_lineage"][0]["acquisition_commit"] = "b" * 40
    reference.receipt.path.write_text(json.dumps(changed))
    _, replacement = _secure_private_bytes(reference.receipt.path)
    candidate = reference.model_copy(update={"receipt": replacement})
    with pytest.raises(ValueError, match="seed differs"):
        candidate.load(installed_policies=installed.installed_policies)


def test_old_default_registry_stays_exact() -> None:
    descriptor = default_strategy_job_adapter_registry().closed_descriptor()
    assert [(x.adapter_id, x.adapter_version) for x in descriptor.adapters] == [
        ("nshape-compare", "1"), ("nshape-optimize", "1"), ("auction-gap", "1"),
        ("growth-board-surge", "1"), ("portfolio-backtest", "1")]


def original_module(name: str, expected_sha: str) -> ModuleType:
    root = Path(__file__).resolve().parents[2]
    evidence = root.parent if (root.parent / "baseline-index.json").is_file() else root / "data/verification/minute-engine-completion-20261007/formal-implementation-03"
    path = evidence / "baseline/src/rquant" / (name + ".py")
    assert hashlib.sha256(path.read_bytes()).hexdigest() == expected_sha
    module_name = "rquant._minute_baseline_" + name
    if module_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(module_name, path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    return sys.modules[module_name]


def test_old_worker_configuration_bytes_and_descriptor_remain_exact() -> None:
    from rquant.lab_worker_registry import BuiltinLabShardRuntimeConfig, resolve_builtin_adapter_registry, unconfigured_builtin_lab_shard_configuration

    original = original_module("lab_worker_registry", "fb5970df0e370c3c2e8bfa713079c9ca824c65b8979c39362c2b47b20a2baa4a")
    old = original.unconfigured_builtin_lab_shard_configuration()
    current = unconfigured_builtin_lab_shard_configuration()
    assert current.model_dump_json() == old.model_dump_json()
    restored = BuiltinLabShardRuntimeConfig.model_validate_json(old.model_dump_json())
    assert restored.minute_catalog is None
    assert resolve_builtin_adapter_registry(restored).closed_descriptor().model_dump_json() == original.resolve_builtin_adapter_registry(old).closed_descriptor().model_dump_json()


@pytest.mark.parametrize("variant", ["compare", "optimize", "auction", "growth"])
def test_original_four_inputs_aliases_and_p13_claim_restore(tmp_path: Path, template_seed: MinuteSourceContentSeed, variant: str) -> None:
    from datetime import date
    from uuid import UUID
    import rquant.lab_job_center as current
    from rquant.lab_jobs import LabJobStore
    from rquant.research_gate import ResearchGateDecision
    from rquant.research_run_spec import ResearchRunSpec, ResourceClass
    from rquant.strategy_job_adapters import (AuctionGapParameters, GrowthBoardSurgeParameters,
        NShapeCompareParameters, NShapeOptimizeParameters, build_adapter_execution_contract)

    original = original_module("lab_job_center", "cccbb3ce12d0dbd99f0115cc8becdf0b762e8e581e8832ba266148057fb08f2e")
    frozen = original_module("strategy_job_adapters", "c2e713a02fc818f6f9eccf79ccfe6dd1e48710320356df49e23a724ecbf87d59")
    choices = {
        "compare": ("NShapeComparisonRunInput", NShapeCompareParameters(hold_days=(1,), entry_modes=("first_break",)), "nshape-compare", "NShapeCompare"),
        "optimize": ("NShapeOptimizationRunInput", NShapeOptimizeParameters(hold_days=(1,), entry_modes=("first_break",), profile_variants=("baseline",)), "nshape-optimize", "NShapeOptimize"),
        "auction": ("AuctionGapRunInput", AuctionGapParameters(max_hold_days=1), "auction-gap", "AuctionGap"),
        "growth": ("GrowthBoardSurgeRunInput", GrowthBoardSurgeParameters(variants=("full",), max_hold_days=1), "growth-board-surge", "GrowthBoardSurge"),
    }
    name, parameters, adapter_id, alias = choices[variant]
    run = getattr(current, name)(start_date=date(2026, 7, 1), end_date=date(2026, 7, 3), parameters=parameters)
    before = getattr(original, name).model_validate_json(run.model_dump_json())
    kwargs = dict(gate_decision=ResearchGateDecision(allowed=True, research_status="exploratory", audit_run_id=None,
        dataset_snapshot_id=None, coverage_counts={}, coverage_ratios={}, failures=()), code_sha="a" * 40,
        dataset_snapshot=None, feature_contract=build_adapter_execution_contract(adapter_id, "1", "a" * 40),
        execution_costs=template_seed.runtime.execution_profile.execution_costs, random_seed=0,
        resource_class=ResourceClass.STANDARD, deadline=NOW + timedelta(hours=1), job_id=UUID(int=100))
    old = original.build_research_job_submission(before, **kwargs)
    new = current.build_research_job_submission(run, **kwargs)
    assert new.model_dump_json() == old.model_dump_json()
    aliases = new.spec.model_dump(mode="json", exclude_computed_fields=True)
    aliases["parameters"]["strategy_name"] = alias
    spec = ResearchRunSpec.model_validate_json(json.dumps(aliases))
    registry = default_strategy_job_adapter_registry()
    reference = frozen.default_strategy_job_adapter_registry()
    legacy = reference._plan_p13_legacy(spec)
    assert [x.model_dump_json() for x in legacy] == [x.model_dump_json() for x in registry._plan_p13_legacy(spec)]
    jobs = LabJobStore(tmp_path / "original-p13.sqlite3")
    jobs.initialize()
    lease = jobs.acquire_scheduler_lease(owner_id="original-p13-test", lease_seconds=60, now=NOW)
    from rquant.lab_job_protocol import LabCommandEnvelope, SubmitJobCommand
    command = SubmitJobCommand(job_id=kwargs["job_id"], spec=spec, max_attempts=1)
    assert jobs.apply_command(LabCommandEnvelope(request_id=UUID(int=101), command=command), lease=lease, now=NOW).status == "applied"
    jobs.plan_job(command.job_id, legacy, lease=lease, now=NOW)
    claim = jobs.claim_next_shard(worker_id="original-p13-worker", shard_lease_seconds=60, lease=lease, now=NOW)
    assert claim is not None
    restored = registry.validate_claim(claim)
    assert restored.spec == spec
    assert restored.claim.definition.payload_json == legacy[0].payload_json
    assert restored.claim.definition.work_plan is None


@pytest.mark.parametrize("old_field", ["hold_days", "entry_modes", "variants", "score_profile_names", "walk_forward_folds"])
def test_new_minute_input_rejects_old_configuration_fields(template_seed: MinuteSourceContentSeed, old_field: str) -> None:
    from rquant.minute_backtest_formal_adapter import MinuteFormalRunInput
    from tests.unit.test_minute_backtest_producer import exact_freeze

    value = MinuteFormalRunInput.from_frozen(exact_freeze(template_seed))
    data = value.model_dump(mode="json")
    data["parameters"][old_field] = ["old-configuration"]
    with pytest.raises(ValueError, match="Extra inputs"):
        MinuteFormalRunInput.model_validate_json(json.dumps(data))
