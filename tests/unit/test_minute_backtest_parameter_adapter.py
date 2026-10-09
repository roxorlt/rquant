from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, NamedTuple

import pytest

if TYPE_CHECKING:
    from rquant.minute_backtest_parameter_adapter import (
        MinuteParameterFormalReplayAdapter,
        MinuteParameterFormalReplayResult,
    )
    from rquant.minute_backtest_parameter_contracts import (
        FrozenMinuteParameterResearchInput,
        MinuteParameterSourceSeed,
    )
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterPreparedPublication,
        MinuteParameterReplayCatalog,
        PublishedMinuteParameterInput,
    )
    from rquant.research_gate import ResearchGateRequest
    from rquant.strategy_job_adapters import LabShardExecutionWireResult, ValidatedStrategyShard


def adapter_module() -> ModuleType:
    return importlib.import_module("rquant.minute_backtest_parameter_adapter")


@pytest.fixture(scope="module")
def source_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("parameter-adapter-source")


@pytest.fixture(scope="module")
def complete_seed(source_root: Path) -> MinuteParameterSourceSeed:
    from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet
    from tests.support.minute_parameter_formal_fixture import parameter_source_seed

    recipe = MinuteParameterSet(
        parameters=MinuteNShapeParameters(
            max_hold_days=7,
            paper={"stop_loss_pct": 0.012345, "entry_slippage_pct": 0.0002},
        )
    )
    return parameter_source_seed(source_root, recipe)


@pytest.fixture(scope="module")
def complete_source(complete_seed: MinuteParameterSourceSeed) -> FrozenMinuteParameterResearchInput:
    from rquant.minute_backtest_producer import minute_metadata_identities

    audit, snapshot = minute_metadata_identities(complete_seed)
    return complete_seed.freeze(
        audit_run_id=audit.audit_run_id, dataset_snapshot_id=snapshot.snapshot_id
    )


def test_adapter_identity_and_inherited_physical_gate_are_distinct_from_formal2() -> None:
    current = adapter_module().MinuteParameterFormalReplayAdapter
    from rquant.minute_backtest_formal_adapter import MinuteFormalReplayAdapter
    from rquant.research_run_spec import ResearchJobType

    assert issubclass(current, MinuteFormalReplayAdapter)
    assert (
        current.adapter_id,
        current.adapter_version,
        current.strategy_name,
        current.job_type,
    ) == (
        "minute-parameter-replay",
        "1",
        "minute_parameter_replay",
        ResearchJobType.STRATEGY_REPLAY,
    )
    assert current.snapshot_strategy_name == "minute_parameter_replay"
    assert current.execute_shard is MinuteFormalReplayAdapter.execute_shard
    assert current.build_shard_inputs is MinuteFormalReplayAdapter.build_shard_inputs
    assert current.build_work_plan is MinuteFormalReplayAdapter.build_work_plan
    assert (MinuteFormalReplayAdapter.adapter_id, MinuteFormalReplayAdapter.adapter_version) == (
        "minute-runtime-replay",
        "2",
    )


def test_prepared_publication_is_optional_bounded_backend_material() -> None:
    current = adapter_module().MinuteParameterFormalParameters
    assert current.model_fields["prepared_publication_json"].default is None
    assert callable(current.from_prepared)


@pytest.mark.parametrize(
    "payload",
    ["{}", '{"bad":1,"bad":1}', "x" * 1_048_577],
    ids=["missing-fields", "duplicate-keys", "oversized"],
)
def test_invalid_prepared_control_material_is_rejected_without_source_fallback(
    complete_source: FrozenMinuteParameterResearchInput,
    payload: str,
) -> None:
    cls = adapter_module().MinuteParameterFormalParameters
    original = cls.from_frozen(complete_source)
    assert original.prepared_publication_json is None
    with pytest.raises(ValueError):
        cls.model_validate(
            original.model_dump(mode="python") | {"prepared_publication_json": payload}
        )


def test_complete_recipe_json_hash_id_frequency_and_original_source_fields_roundtrip(
    complete_source: FrozenMinuteParameterResearchInput,
) -> None:
    from rquant.minute_backtest_parameters import MinuteParameterSet

    cls = adapter_module().MinuteParameterFormalParameters
    value = cls.from_frozen(complete_source)
    recipe = complete_source.runtime.parameters
    assert value.parameter_set_json == recipe.model_dump_json()
    assert MinuteParameterSet.model_validate_json(value.parameter_set_json) == recipe
    assert value.parameter_hash == recipe.fingerprint
    assert value.native_strategy_id == recipe.definition_id
    assert len(value.native_strategy_id) == 55
    assert value.source_frequency == complete_source.runtime.source_frequency
    assert value.full_input_hash == complete_source.full_input_hash
    assert value.core_input_hash == complete_source.core_input_hash
    assert value.seed_hash == complete_source.source_content_seed.seed_hash
    assert value.work_units == complete_source.formal_work.work_units
    assert cls.model_validate_json(value.model_dump_json()) == value
    run_cls = adapter_module().MinuteParameterFormalRunInput
    run = run_cls.from_frozen(complete_source)
    assert run.kind == "minute_parameter_replay"
    assert run.parameters == value
    assert (run.start_date, run.end_date) == (
        complete_source.runtime.start_date,
        complete_source.runtime.end_date,
    )
    assert run_cls.model_validate_json(run.model_dump_json()) == run


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("parameter_hash", "0" * 64),
        ("native_strategy_id", "gp." + "a" * 52),
        ("native_strategy_id", "n_shape"),
        ("source_frequency", "5min"),
        ("source_frequency", "2min"),
    ],
)
def test_wrong_complete_parameter_identity_is_rejected(
    complete_source: FrozenMinuteParameterResearchInput,
    field: str,
    changed: str,
) -> None:
    cls = adapter_module().MinuteParameterFormalParameters
    data = cls.from_frozen(complete_source).model_dump(mode="json")
    data[field] = changed
    with pytest.raises(ValueError):
        cls.model_validate(data)


@pytest.mark.parametrize("missing", ["parameter_set_json", "parameter_hash", "source_frequency"])
def test_missing_parameter_material_is_not_an_old_native_or_default_recipe(
    complete_source: FrozenMinuteParameterResearchInput,
    missing: str,
) -> None:
    cls = adapter_module().MinuteParameterFormalParameters
    data = cls.from_frozen(complete_source).model_dump(mode="json")
    data.pop(missing)
    with pytest.raises(ValueError, match="Field required"):
        cls.model_validate(data)


@pytest.mark.parametrize("term", ["max_hold_days", "paper", "volume_profile"])
def test_recipe_changes_cannot_keep_the_original_complete_binding(
    complete_source: FrozenMinuteParameterResearchInput,
    term: str,
) -> None:
    from rquant.minute_backtest_parameters import MinuteParameterSet

    cls = adapter_module().MinuteParameterFormalParameters
    data = cls.from_frozen(complete_source).model_dump(mode="json")
    recipe = json.loads(data["parameter_set_json"])
    if term == "max_hold_days":
        recipe["parameters"][term] = 13
    elif term == "paper":
        recipe["parameters"][term]["stop_loss_pct"] = 0.02
    else:
        recipe["parameters"][term]["lookback_days"] = [30, 90]
    data["parameter_set_json"] = MinuteParameterSet.model_validate(recipe).model_dump_json()
    with pytest.raises(ValueError, match="(parameter|recipe|definition|frequency)"):
        cls.model_validate(data)


def test_full_json_rejects_omitted_defaults_unknown_terms_and_duplicate_keys(
    complete_source: FrozenMinuteParameterResearchInput,
) -> None:
    cls = adapter_module().MinuteParameterFormalParameters
    original = cls.from_frozen(complete_source).model_dump(mode="json")
    recipe = json.loads(original["parameter_set_json"])
    recipe["parameters"].pop("volume_profile")
    for encoded in [
        json.dumps(recipe),
        original["parameter_set_json"].replace(
            '"schema_version":1', '"schema_version":1,"schema_version":1'
        ),
        original["parameter_set_json"].replace(
            '"schema_version":1', '"schema_version":1,"invented_permission":true'
        ),
    ]:
        with pytest.raises(ValueError):
            cls.model_validate(original | {"parameter_set_json": encoded})


def test_legacy_scalar_fields_are_rejected_in_new_run_input(
    complete_source: FrozenMinuteParameterResearchInput,
) -> None:
    cls = adapter_module().MinuteParameterFormalRunInput
    data = cls.from_frozen(complete_source).model_dump(mode="json")
    for field in ["hold_days", "entry_modes", "variants", "score_profile_names"]:
        changed = json.loads(json.dumps(data))
        changed["parameters"][field] = ["old-scalar"]
        with pytest.raises(ValueError, match="Extra inputs"):
            cls.model_validate(changed)


class ParameterExecution(NamedTuple):
    published: PublishedMinuteParameterInput
    catalog: MinuteParameterReplayCatalog
    metadata_path: Path
    lake_root: Path
    request: ResearchGateRequest
    adapter: MinuteParameterFormalReplayAdapter
    validated: ValidatedStrategyShard
    wire: LabShardExecutionWireResult
    result: MinuteParameterFormalReplayResult


@pytest.fixture(scope="module")
def original_execution(
    complete_seed: MinuteParameterSourceSeed,
    source_root: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> ParameterExecution:
    from datetime import timedelta
    from types import SimpleNamespace
    from uuid import uuid4

    import rquant.storage.duckdb as storage
    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.experiment_registry import (
        DateRange,
        ExperimentRegistry,
        ExperimentSpec,
        FormalExperimentPlan,
        HypothesisFamilyManifest,
    )
    from rquant.lab_job_center import (
        LabCommandSubmissionFacade,
        _research_parameter,
        _validated_research_ownership,
    )
    from rquant.lab_job_protocol import LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobReader, LabJobStore
    from rquant.minute_backtest_parameter_adapter import (
        MinuteParameterFormalParameters,
        MinuteParameterFormalReplayAdapter,
        MinuteParameterFormalReplayResult,
    )
    from rquant.minute_backtest_parameter_definition import minute_parameter_research_registry
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterReplayCatalog,
        publish_minute_parameter_input,
    )
    from rquant.minute_backtest_producer import open_gated_minute_store
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_gate import ResearchGateRequest
    from rquant.research_run_spec import (
        ResearchJobType,
        ResearchRunParameters,
        ResearchRunSpec,
        ResourceClass,
    )
    from rquant.runtime_contracts import canonical_sha256
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_job_adapters import (
        LabShardExecutionWireResult,
        StrategyJobAdapterRegistry,
        build_adapter_execution_contract,
    )

    root = tmp_path_factory.mktemp("parameter-adapter-physical")
    root.chmod(0o700)
    patch = pytest.MonkeyPatch()
    patch.setattr(storage, "_settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
    try:
        metadata_path, lake = root / "metadata.duckdb", root / "lake"
        now = complete_seed.provenance.published_at
        with DuckDBStore(metadata_path) as metadata:
            published = publish_minute_parameter_input(
                complete_seed,
                metadata_store=metadata,
                source_path=root / "input.duckdb",
                receipt_path=root / "receipt.json",
                catalog=ResearchCatalog(root / "catalog.duckdb"),
                lake_root=lake,
                installed_policies=(complete_seed.provenance.visibility_policy,),
                now=now,
            )
        catalog = MinuteParameterReplayCatalog(
            entries=(published.reference,),
            installed_policies=(complete_seed.provenance.visibility_policy,),
        )
        value = published.receipt.frozen
        adapter = MinuteParameterFormalReplayAdapter(catalog)
        complete = MinuteParameterFormalParameters.from_frozen(value)
        parameters = ResearchRunParameters(
            strategy_name=adapter.strategy_name,
            start_date=value.runtime.start_date,
            end_date=value.runtime.end_date,
            arguments=tuple(
                _research_parameter(name, getattr(complete, name))
                for name in MinuteParameterFormalParameters.model_fields
                if name != "prepared_publication_json"
                or complete.prepared_publication_json is not None
            ),
        )
        contract = build_adapter_execution_contract(
            adapter.adapter_id, adapter.adapter_version, value.runtime.producer_commit
        )
        registration = value.wrapper_registration
        experiment = ExperimentSpec(
            strategy_spec_fingerprint=registration.spec.spec_fingerprint,
            strategy_executable_fingerprint=registration.executable_fingerprint,
            candidate_schema_fingerprint=registration.candidate_schema_fingerprint,
            dataset_snapshot_id=published.identity.snapshot_id,
            code_commit=value.runtime.producer_commit,
            parameter_fingerprint=canonical_sha256(parameters),
            hypothesis_family="explicit-synthetic-parameter-adapter",
            metric_definition_fingerprint=canonical_sha256(
                {"basis": "existing synthetic runner eight-table parity"}
            ),
            train_range=DateRange(
                start_date=value.runtime.start_date - timedelta(days=2),
                end_date=value.runtime.start_date - timedelta(days=2),
            ),
            validation_range=DateRange(
                start_date=value.runtime.start_date - timedelta(days=1),
                end_date=value.runtime.start_date - timedelta(days=1),
            ),
            frozen_outer_test_range=DateRange(
                start_date=value.runtime.start_date, end_date=value.runtime.end_date
            ),
            cost_model_fingerprint=canonical_sha256(
                value.runtime.execution_profile.execution_costs
            ),
            execution_model_fingerprint=canonical_sha256(
                {
                    "contract": "lab-adapter-execution/v1",
                    "adapter_id": adapter.adapter_id,
                    "adapter_version": adapter.adapter_version,
                    "feature_contract": contract,
                }
            ),
            seed=17,
        )
        formal = FormalExperimentPlan(
            schema_version=2,
            spec=experiment,
            hypothesis_variant="explicit-synthetic-adapter",
            strategy_definition_fingerprint=registration.fingerprint,
            definition_registration_record_hash=registration.record_hash,
            preregistered_at=now,
        )
        trust = root / "experiment-trust"
        trust.mkdir(mode=0o700)
        experiments = ExperimentRegistry(trust / "experiments.sqlite3", managed_trust_root=trust)
        experiments.register_formal_plan(
            formal,
            family_manifest=HypothesisFamilyManifest(
                hypothesis_family=experiment.hypothesis_family,
                experiment_ids=(experiment.experiment_id,),
                search_space_fingerprint=canonical_sha256(complete),
                metric_definition_fingerprint=experiment.metric_definition_fingerprint,
                preregistered_at=now,
            ),
        )
        deadline = now + timedelta(hours=1)
        schema, execution, identity = _validated_research_ownership(
            decision=published.gate_decision,
            strategy_name=adapter.strategy_name,
            adapter_id=adapter.adapter_id,
            adapter_version=adapter.adapter_version,
            code_sha=value.runtime.producer_commit,
            deadline=deadline,
            dataset_snapshot=published.identity,
            feature_contract=contract,
            execution_costs=value.runtime.execution_profile.execution_costs,
            parameters=parameters,
            random_seed=17,
            trusted_strategy_registration=registration,
            formal_experiment_plan=formal,
        )
        spec = ResearchRunSpec(
            schema_version=schema,
            job_type=ResearchJobType.STRATEGY_REPLAY,
            parameters=parameters,
            code_sha=value.runtime.producer_commit,
            dataset_snapshot=published.identity,
            feature_contract=contract,
            execution_costs=value.runtime.execution_profile.execution_costs,
            random_seed=17,
            resource_class=ResourceClass.STANDARD,
            deadline=deadline,
            research_status=published.gate_decision.research_status,
            strategy_execution=execution,
            experiment=identity,
        )
        definitions = ImmutableDefinitionRegistry(
            source_root / "runtime-fixture/actual-parameter-definitions",
            execution_registry=minute_parameter_research_registry(
                value.runtime.parameters, producer_commit=value.runtime.producer_commit
            ),
        )
        jobs = LabJobStore(root / "jobs.sqlite3")
        jobs.initialize()
        spool = LabCommandSpool(root / "commands")
        facade = LabCommandSubmissionFacade(
            reader=LabJobReader(jobs.path),
            spool=spool,
            experiment_registry=experiments,
            definition_registry=definitions,
            clock=lambda: now,
        )
        command = SubmitJobCommand(job_id=uuid4(), spec=spec, max_attempts=2)
        submitted = facade.submit_create(
            command, interaction_key="parameter-adapter-original-chain"
        )
        assert submitted.result == "submitted"
        (entry,) = spool.pending()
        lease = jobs.acquire_scheduler_lease(
            owner_id="synthetic-parameter-adapter", lease_seconds=3600, now=now
        )
        accepted = jobs.apply_command(
            entry.envelope,
            lease=lease,
            now=now,
            submission_authority=lambda envelope, at: (
                facade.validate_prepared_experiment_submission(envelope, observed_at=at)
            ),
        )
        assert accepted.status == "applied"
        spool.ack(entry, accepted)
        registry = StrategyJobAdapterRegistry((adapter,))
        plan = registry.plan(spec)
        jobs.plan_job(command.job_id, plan, lease=lease, now=now)
        claim = jobs.claim_next_shard(
            worker_id="synthetic-parameter-adapter", shard_lease_seconds=3600, lease=lease, now=now
        )
        assert claim is not None
        validated = registry.validate_claim(claim)
        request = ResearchGateRequest(
            mode="formal",
            strategy_name=adapter.strategy_name,
            start_date=value.runtime.start_date,
            end_date=value.runtime.end_date,
            code_commit=value.runtime.producer_commit,
            audit_run_id=value.runtime.audit_run_id,
            dataset_snapshot_id=value.runtime.dataset_snapshot_id,
            dataset_binding_hash=published.receipt.binding.binding_hash,
        )
        with open_gated_minute_store(
            request,
            metadata_store_factory=lambda: DuckDBStore(metadata_path, read_only=True),
            lake_root=lake,
            catalog=catalog,
            source_key=complete.source_key,
            source_version=complete.source_version,
            owner_id=complete.owner_id,
        ) as (session, decision):
            assert decision.allowed
            actual = adapter.execute_shard(validated, session)
        wire = LabShardExecutionWireResult.from_result(actual)
        restored = LabShardExecutionWireResult.model_validate_json(
            wire.model_dump_json()
        ).to_result()
        assert (restored.spec_hash, restored.payload_hash, restored.plan_hash) == (
            claim.spec_hash,
            claim.payload_hash,
            claim.plan_hash,
        )
        frames = {item.name: item.frame for item in restored.tables}
        result = MinuteParameterFormalReplayResult.model_validate_json(
            frames["replay_summary"].iloc[0]["payload"]
        )
        (root / "formal-result.json").write_text(
            result.model_dump_json(exclude_computed_fields=True)
        )
        (root / "real-execution.json").write_text(
            json.dumps(
                {
                    "source_kind": result.publication.frozen.provenance.source_kind,
                    "parameter_hash": complete.parameter_hash,
                    "full_input_hash": complete.full_input_hash,
                    "core_input_hash": complete.core_input_hash,
                    "seed_hash": complete.seed_hash,
                    "adapter_id": restored.adapter_id,
                    "adapter_version": restored.adapter_version,
                    "tables": {
                        item.name: {"rows": len(frames[item.name].index), "bytes": item.byte_size}
                        for item in wire.tables
                    },
                    "input_bytes": published.receipt.source_file_bytes
                    + published.receipt.snapshot_artifact_bytes,
                    "signals": len(result.replay.signals),
                    "orders": len(result.replay.orders),
                    "fills": len(result.replay.fills),
                    "limitation": (
                        "Synthetic one-shard adapter gate; "
                        "not full study/worker/install/real market"
                    ),
                },
                indent=2,
            )
            + "\n"
        )
        return ParameterExecution(
            published, catalog, metadata_path, lake, request, adapter, validated, wire, result
        )
    finally:
        patch.undo()


def test_original_publisher_formal_claim_physical_gate_runner_and_eight_tables(
    original_execution: ParameterExecution,
) -> None:
    value = original_execution.published.receipt.frozen
    result, wire = original_execution.result, original_execution.wire
    assert result.publication == original_execution.published.receipt
    assert result.replay.parameters == value.runtime.parameters
    assert result.replay.parameter_work == value.runtime.parameter_work
    assert result.replay.signals and result.replay.orders and result.replay.fills
    assert result.replay.input_hash == value.core_input_hash
    assert set(item.name for item in wire.tables) == {
        "signals",
        "orders",
        "fills",
        "paper_queue",
        "account",
        "daily_valuations",
        "execution_profile",
        "replay_summary",
    }
    assert len(wire.tables) == value.result_budget.table_count == 8
    assert all(item.byte_size <= value.result_budget.table_bytes for item in wire.tables)
    assert sum(item.byte_size for item in wire.tables) <= value.result_budget.total_bytes
    assert len(wire.model_dump_json().encode()) <= value.result_budget.wire_bytes


@pytest.mark.parametrize(
    "field",
    [
        "full_input_hash",
        "core_input_hash",
        "seed_hash",
        "profile_hash",
        "native_registration_hash",
        "wrapper_registration_hash",
        "work_units",
        "source_key",
        "source_version",
        "owner_id",
    ],
)
def test_original_complete_source_receipt_rejects_cross_bindings(
    original_execution: ParameterExecution,
    field: str,
) -> None:
    cls = adapter_module().MinuteParameterFormalParameters
    value = cls.from_frozen(original_execution.published.receipt.frozen).model_dump(mode="json")
    if field in {"work_units", "source_version"}:
        value[field] += 1
    elif field in {"source_key", "owner_id"}:
        value[field] = "foreign-parameter-source"
    else:
        value[field] = "0" * 64
    with pytest.raises((ValueError, PermissionError)):
        original_execution.adapter.expected(cls.model_validate(value))


def test_parameter_adapter_rejects_real_snapshot_session_without_original_gate(
    original_execution: ParameterExecution,
) -> None:
    from rquant.research_snapshot import ResearchExecutionSession

    with (
        ResearchExecutionSession(
            binding=original_execution.published.receipt.binding,
            lake_root=original_execution.lake_root,
        ) as session,
        pytest.raises(PermissionError, match="gate/session"),
    ):
        original_execution.adapter.execute_shard(original_execution.validated, session)


@pytest.mark.parametrize(
    "field",
    [
        "full_input_hash",
        "core_input_hash",
        "seed_hash",
        "native_registration_hash",
        "wrapper_registration_hash",
        "parameter_work",
    ],
)
def test_complete_formal_result_rejects_changed_source_or_parameter_work(
    original_execution: ParameterExecution,
    field: str,
) -> None:
    cls = adapter_module().MinuteParameterFormalReplayResult
    data = original_execution.result.model_dump(mode="json", exclude_computed_fields=True)
    if field == "parameter_work":
        data["replay"][field]["prefix_rows"] += 1
    else:
        data[field] = "0" * 64
    with pytest.raises(ValueError, match="(publication|parameter|work|hash)"):
        cls.model_validate(data)


class PreparedMaterial(NamedTuple):
    carrier: MinuteParameterPreparedPublication
    catalog: MinuteParameterReplayCatalog
    frozen: FrozenMinuteParameterResearchInput


@pytest.fixture(scope="module")
def prepared_material(
    tmp_path_factory: pytest.TempPathFactory,
    complete_seed: MinuteParameterSourceSeed,
) -> Iterator[PreparedMaterial]:
    from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterFactSourceReference,
        MinuteParameterPreparedPublication,
        MinuteParameterReplayCatalog,
    )
    from rquant.strict_json import strict_model_validate_json
    from tests.unit.test_minute_backtest_parameter_fact_sources import (
        test_prepared_dynamic_reference_opens_complete_baseline_metadata_and_original_gate,
    )

    root = tmp_path_factory.mktemp("parameter-adapter-prepared")
    root.chmod(0o700)
    patch = pytest.MonkeyPatch()
    try:
        test_prepared_dynamic_reference_opens_complete_baseline_metadata_and_original_gate(
            root, patch
        )
        output = root / "prepared/backend-created-operation"
        carrier = strict_model_validate_json(
            MinuteParameterPreparedPublication, (output / "prepared-reference.json").read_bytes()
        )
        with ImmutableDuckDBMetadataCatalog.open(
            root / "baseline-publication/metadata.duckdb", snapshot_root=root / "metadata-snapshots"
        ) as metadata:
            baseline_identity = metadata.descriptor
        reference = MinuteParameterFactSourceReference.model_validate(
            carrier.baseline.model_dump(mode="python")
            | {
                "metadata_identity": baseline_identity,
                "display_name": "完整合成参数事实",
                "source_nature": "synthetic_validation",
                "supported_parameter_names": ("paper.stop_loss_pct",),
            }
        )
        policy = complete_seed.provenance.visibility_policy
        receipt = carrier.load(installed_policies=(policy,))
        catalog = MinuteParameterReplayCatalog(
            fact_sources=(reference,),
            prepared_root=root / "prepared",
            snapshot_root=root / "metadata-snapshots",
            research_lake_root=root / "lake",
            installed_policies=(policy,),
        )
        yield PreparedMaterial(carrier, catalog, receipt.frozen)
    finally:
        patch.undo()


def test_dynamic_prepared_recipe_and_full_admission_identity_use_independent_catalog(
    prepared_material: PreparedMaterial,
) -> None:
    cls = adapter_module().MinuteParameterFormalParameters
    original = cls.from_frozen(prepared_material.frozen)
    prepared = cls.from_prepared(prepared_material.frozen, prepared_material.carrier)
    assert prepared.prepared_publication_json == prepared_material.carrier.model_dump_json(
        exclude_computed_fields=True
    )
    assert prepared.work_units == prepared_material.carrier.work_units > original.work_units
    assert prepared.model_dump(
        mode="python", exclude={"prepared_publication_json", "work_units"}
    ) == (original.model_dump(mode="python", exclude={"prepared_publication_json", "work_units"}))
    restored = cls.model_validate_json(prepared.model_dump_json())
    adapter = adapter_module().MinuteParameterFormalReplayAdapter(prepared_material.catalog)
    assert adapter.expected(restored).frozen == prepared_material.frozen


def test_dynamic_prepared_loaded_bytes_are_verified_from_both_actual_sources(
    prepared_material: PreparedMaterial,
) -> None:
    cls = adapter_module().MinuteParameterFormalParameters
    changed = prepared_material.carrier.model_copy(
        update={
            "loaded_bytes": prepared_material.carrier.loaded_bytes - 1,
        }
    )
    parameters = cls.from_prepared(prepared_material.frozen, changed)
    adapter = adapter_module().MinuteParameterFormalReplayAdapter(prepared_material.catalog)
    with pytest.raises(PermissionError, match="work/loaded bytes"):
        adapter.expected(parameters)


def test_dynamic_prepared_identity_and_complete_json_cannot_be_mutated(
    prepared_material: PreparedMaterial,
) -> None:
    cls = adapter_module().MinuteParameterFormalParameters
    value = cls.from_prepared(prepared_material.frozen, prepared_material.carrier).model_dump(
        mode="python"
    )
    for field in [
        "full_input_hash",
        "core_input_hash",
        "seed_hash",
        "parameter_hash",
    ]:
        body = prepared_material.carrier.model_dump(mode="python")
        body[field] = "0" * 64
        carrier = type(prepared_material.carrier).model_validate(body)
        with pytest.raises(ValueError, match="parameter identity/work"):
            cls.from_prepared(prepared_material.frozen, carrier)
    wrong_work = prepared_material.carrier.model_copy(
        update={"work_units": prepared_material.carrier.work_units - 1}
    )
    adapter = adapter_module().MinuteParameterFormalReplayAdapter(prepared_material.catalog)
    with pytest.raises(PermissionError, match="work/loaded bytes"):
        adapter.expected(cls.from_prepared(prepared_material.frozen, wrong_work))
    raw = value["prepared_publication_json"]
    duplicate = raw.replace('"work_units":', '"work_units":1,"work_units":', 1)
    with pytest.raises(ValueError):
        cls.model_validate(value | {"prepared_publication_json": duplicate})


def test_dynamic_prepared_descriptor_and_owner_cannot_replace_installation(
    prepared_material: PreparedMaterial,
) -> None:
    cls = adapter_module().MinuteParameterFormalParameters
    adapter = adapter_module().MinuteParameterFormalReplayAdapter(prepared_material.catalog)
    original = prepared_material.carrier
    wrong_owner = original.model_copy(
        update={
            "baseline": original.baseline.model_copy(update={"owner_id": "foreign-owner"}),
        }
    )
    with pytest.raises(PermissionError, match="installed full fact authority"):
        adapter.expected(cls.from_prepared(prepared_material.frozen, wrong_owner))
    wrong_metadata = original.model_copy(
        update={
            "metadata_identity": original.metadata_identity.model_copy(update={"sha256": "0" * 64}),
        }
    )
    with pytest.raises(
        (ValueError, PermissionError), match="(Metadata|metadata|identity|content|checksum)"
    ):
        adapter.expected(cls.from_prepared(prepared_material.frozen, wrong_metadata))
