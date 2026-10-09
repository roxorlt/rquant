"""Current-interpreter typed renderer data, independent of any physical sealed owner."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime
from io import BytesIO
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid5

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict

from rquant.data_metadata import (
    DataAuditRunFinalization,
    DatasetSnapshotArtifact,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingManifest,
    DatasetSnapshotFinalization,
)
from rquant.definition_registry import (
    DefinitionSchemaCompatibility,
    StrategySpecRegistration,
    _canonical_strategy_spec,
    _strategy_definition_fingerprint,
    _strategy_executable_fingerprint,
)
from rquant.experiment_registry import ExperimentSpec, FormalExperimentPlan
from rquant.lab_artifacts import (
    LabJobArtifactFile,
    LabJobArtifactManifest,
    LabParquetIdentity,
    _complete_result_hash_payload,
    _frame_dtype_identities,
    _table_content_hash,
    canonical_json_bytes,
)
from rquant.minute_backtest_artifact import MinuteSealedReplayResult
from rquant.minute_backtest_formal_adapter import MinuteFormalParameters, MinuteFormalReplayResult
from rquant.minute_backtest_producer import (
    MinutePublicationReceipt,
    minute_coverages,
    minute_metadata_identities,
    minute_watermarks,
)
from rquant.minute_backtest_publication_contracts import MinuteSourceContentSeed
from rquant.minute_backtest_runner import MinuteRuntimeReplayResult, minute_runtime_result_tables
from rquant.minute_backtest_validation import original_builtin_minute_plan
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ResearchExperimentIdentity,
    ResearchRunParameters,
    ResearchRunSpec,
    StrategyExecutionIdentity,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

REFERENCE_SHA256 = "8316a5db151347f73074fd84bb4939a007278400e7299c1706ac9c0ded292cdc"
BUILDER_VERSION = "synthetic-minute-renderer/v1"


class MinuteReportFixture(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    result: MinuteSealedReplayResult
    reference_sha256: str
    python_version: tuple[int, int, int]
    builder_version: str = BUILDER_VERSION
    reference_values_sha256: str
    compared_leaf_values: int
    physical_owner_verified: Literal[False] = False
    formal_history_passed: Literal[False] = False


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _registration(
    registry: BuiltinStrategyEvaluatorRegistry,
    reference: dict[str, Any],
    feature_fingerprint: str,
) -> StrategySpecRegistration:
    definition = registry.load_definition(reference["logical_id"], reference["version"])
    spec = _canonical_strategy_spec(definition.spec)
    binding = registry.trusted_executable_registry().strategy_binding(spec)
    values = dict(
        schema_version=5,
        schema_compatibility=DefinitionSchemaCompatibility(),
        kind="strategy_spec",
        logical_id=spec.strategy_id,
        version=spec.version,
        registered_at=_time(reference["registered_at"]),
        available_at=_time(reference["available_at"]),
        fingerprint=_strategy_definition_fingerprint(
            spec,
            binding,
            feature_contract_fingerprint=feature_fingerprint,
            parent_fingerprint=None,
            supersedes=None,
            replacement_reason=None,
        ),
        parent_fingerprint=None,
        supersedes=None,
        replacement_reason=None,
        producer_commit=spec.producer_commit,
        feature_contract_fingerprint=feature_fingerprint,
        feature_contract_version=3,
        feature_contract_producer_commit=spec.producer_commit,
        spec=spec,
        execution_binding=binding,
        candidate_schema_fingerprint=binding.candidate_schema_fingerprint,
        executable_fingerprint=_strategy_executable_fingerprint(spec, binding),
    )
    return StrategySpecRegistration(**values, record_hash=canonical_sha256(values))


def _reference_values(value: dict[str, Any]) -> dict[str, Any]:
    replay = json.loads(_json(value["result"]["replay"]))
    replay.pop("input_hash")
    for day in replay["daily_valuations"]:
        day.pop("input_hash")
    seed = value["result"]["publication"]["seed"]
    return {
        "replay": replay,
        "originals": seed["origin_materials"],
        "derivations": seed["derivations"],
        "provenance": seed["provenance"],
        "runtime_reference": {
            name: item
            for name, item in seed["runtime"].items()
            if name not in {"source_key", "strategy"}
        },
    }


def _leaf_count(value: object) -> int:
    if isinstance(value, dict):
        return sum(_leaf_count(item) for item in value.values())
    if isinstance(value, list):
        return sum(_leaf_count(item) for item in value)
    return 1


def build_minute_report_fixture(reference_raw: bytes) -> MinuteReportFixture:
    if hashlib.sha256(reference_raw).hexdigest() != REFERENCE_SHA256:
        raise ValueError("minute renderer reference bytes changed")
    reference = json.loads(reference_raw)
    old_publication = reference["result"]["publication"]
    old_seed = old_publication["seed"]
    commit = old_seed["runtime"]["producer_commit"]
    plan = original_builtin_minute_plan(producer_commit=commit)
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=commit)
    native = _registration(
        registry, old_seed["native_registration"], plan.feature_contract_fingerprints[2]
    )
    wrapper = _registration(
        registry, old_seed["wrapper_registration"], plan.feature_contract_fingerprints[2]
    )
    native_binding = next(item for item in plan.strategies if item.strategy_id == native.logical_id)
    seed_data = json.loads(_json(old_seed))
    seed_data["native_registration"] = native.model_dump(mode="json")
    seed_data["wrapper_registration"] = wrapper.model_dump(mode="json")
    seed_data["runtime"]["strategy"] = native_binding.model_dump(mode="json")
    seed_data["runtime"]["source_key"] = (
        f"synthetic-renderer-py{sys.version_info.major}{sys.version_info.minor}"
    )
    seed = MinuteSourceContentSeed.model_validate_json(_json(seed_data))
    audit, snapshot = minute_metadata_identities(seed)
    frozen = seed.freeze(audit_run_id=audit.audit_run_id, dataset_snapshot_id=snapshot.snapshot_id)
    now = seed.provenance.published_at
    frozen_bytes = frozen.model_dump_json(exclude_computed_fields=True).encode()
    source_table = pa.table(
        {"input_hash": [frozen.full_input_hash], "payload_json": [frozen_bytes.decode()]}
    )
    source_buffer = BytesIO()
    pq.write_table(source_table, source_buffer)
    source_bytes = source_buffer.getvalue()
    artifact = DatasetSnapshotArtifact(
        artifact_type="materialized_table",
        dataset_id="minute_runtime_replay_input",
        table_name="minute_runtime_replay_input",
        artifact_key="synthetic-renderer-input",
        relative_path="reference/minute_runtime_replay_input.parquet",
        row_count=1,
        schema_hash=hashlib.sha256(source_table.schema.serialize().to_pybytes()).hexdigest(),
        content_hash=canonical_sha256(
            {"input_hash": frozen.full_input_hash, "payload_json": frozen_bytes.decode()}
        ),
        file_hash=hashlib.sha256(source_bytes).hexdigest(),
        file_size=len(source_bytes),
        primary_key=("input_hash",),
        source=BUILDER_VERSION,
    )
    snapshot_manifest = DatasetSnapshotBindingManifest(
        snapshot_id=snapshot.snapshot_id,
        strategy_name="minute_runtime_replay",
        start_date=frozen.runtime.start_date,
        end_date=frozen.runtime.end_date,
        as_of_time=now,
        code_commit=commit,
        dependency_contract_version=frozen.contract,
        builder_version=BUILDER_VERSION,
        artifacts=(artifact,),
    )
    binding = DatasetSnapshotBinding(
        snapshot_id=snapshot.snapshot_id,
        manifest_hash=snapshot_manifest.manifest_hash,
        manifest=snapshot_manifest,
        artifact_root="synthetic-renderer-only",
        manifest_relative_path=f"snapshots/{snapshot.snapshot_id}/manifest.json",
        status="ready",
        created_at=now,
        completed_at=now,
    )
    published = MinutePublicationReceipt(
        seed=seed,
        frozen=frozen,
        audit=audit.finalize(DataAuditRunFinalization(p0_count=0, completed_at=now)),
        snapshot=snapshot.finalize(
            DatasetSnapshotFinalization(
                table_watermarks=minute_watermarks(frozen), completed_at=now
            )
        ),
        binding=binding,
        coverages=minute_coverages(frozen),
        source_file_bytes=len(frozen_bytes),
        snapshot_artifact_bytes=len(source_bytes),
    )
    replay_data = json.loads(_json(reference["result"]["replay"]))
    replay_data["input_hash"] = frozen.core_input_hash
    for day in replay_data["daily_valuations"]:
        day["input_hash"] = frozen.core_input_hash
    replay = MinuteRuntimeReplayResult.model_validate_json(_json(replay_data))
    result = MinuteFormalReplayResult(
        full_input_hash=frozen.full_input_hash,
        core_input_hash=frozen.core_input_hash,
        seed_hash=seed.seed_hash,
        native_registration_hash=native.record_hash,
        wrapper_registration_hash=wrapper.record_hash,
        publication=published,
        replay=replay,
    )
    parameters_data = json.loads(_json(reference["accepted_spec"]["parameters"]))
    parameter_values = MinuteFormalParameters.from_frozen(frozen).model_dump(mode="json")
    for argument in parameters_data["arguments"]:
        argument["value"] = parameter_values[argument["name"]]
    parameters = ResearchRunParameters.model_validate_json(_json(parameters_data))
    execution = StrategyExecutionIdentity(
        strategy_id=wrapper.logical_id,
        strategy_version=wrapper.version,
        adapter_id="minute-runtime-replay",
        adapter_version="2",
        strategy_spec_fingerprint=wrapper.spec.spec_fingerprint,
        strategy_definition_fingerprint=wrapper.fingerprint,
        strategy_executable_fingerprint=wrapper.executable_fingerprint,
        candidate_schema_fingerprint=wrapper.candidate_schema_fingerprint,
        definition_registration_record_hash=wrapper.record_hash,
        definition_registered_at=wrapper.registered_at,
        definition_available_at=wrapper.available_at,
        producer_code_commit=commit,
    )
    experiment_data = json.loads(_json(reference["formal_plan"]["spec"]))
    experiment_data.update(
        experiment_id=None,
        strategy_spec_fingerprint=wrapper.spec.spec_fingerprint,
        strategy_executable_fingerprint=wrapper.executable_fingerprint,
        candidate_schema_fingerprint=wrapper.candidate_schema_fingerprint,
        dataset_snapshot_id=snapshot.snapshot_id,
        parameter_fingerprint=canonical_sha256(parameters),
    )
    experiment = ExperimentSpec.model_validate_json(_json(experiment_data))
    formal = FormalExperimentPlan(
        schema_version=2,
        spec=experiment,
        hypothesis_variant=reference["formal_plan"]["hypothesis_variant"],
        strategy_definition_fingerprint=wrapper.fingerprint,
        definition_registration_record_hash=wrapper.record_hash,
        preregistered_at=_time(reference["formal_plan"]["preregistered_at"]),
    )
    spec_data = json.loads(_json(reference["accepted_spec"]))
    spec_data.update(
        parameters=parameters.model_dump(mode="json"),
        dataset_snapshot=DatasetSnapshotIdentity(
            snapshot_id=snapshot.snapshot_id,
            binding_hash=binding.binding_hash,
            audit_run_id=audit.audit_run_id,
        ).model_dump(mode="json"),
        strategy_execution=execution.model_dump(mode="json"),
        experiment=ResearchExperimentIdentity(
            schema_version=2,
            spec=experiment,
            experiment_id=experiment.experiment_id,
            hypothesis_family=experiment.hypothesis_family,
            hypothesis_variant=formal.hypothesis_variant,
            formal_plan_id=formal.plan_id,
        ).model_dump(mode="json"),
    )
    spec = ResearchRunSpec.model_validate_json(_json(spec_data))
    job_id = uuid5(NAMESPACE_URL, BUILDER_VERSION + ":" + frozen.full_input_hash)
    plan_hash = canonical_sha256(
        {"contract": BUILDER_VERSION, "spec_hash": spec.spec_hash, "work": frozen.formal_work}
    )
    files = []
    for name, payload, media in (
        ("spec.json", spec.canonical_json().encode(), "application/json"),
        (
            "metrics.json",
            canonical_json_bytes(
                {"renderer_reference": result.model_dump(mode="json", exclude_computed_fields=True)}
            ),
            "application/json",
        ),
        (
            "report.md",
            b"Synthetic renderer fixture only; no physical owner verification.\n",
            "text/markdown; charset=utf-8",
        ),
    ):
        files.append(
            LabJobArtifactFile(
                relative_path=name,
                media_type=media,
                size=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
            )
        )
    for name, frame in sorted(minute_runtime_result_tables(replay).items()):
        buffer = BytesIO()
        frame.to_parquet(buffer, index=False)
        payload = buffer.getvalue()
        identities = _frame_dtype_identities(frame)
        parquet = LabParquetIdentity(
            table_name=name,
            row_count=len(frame),
            columns=tuple(frame.columns),
            dtypes=tuple(item.pandas_dtype for item in identities),
            dtype_identities=identities,
            content_sha256=_table_content_hash(frame),
        )
        files.append(
            LabJobArtifactFile(
                relative_path=f"tables/{name}.parquet",
                media_type="application/vnd.apache.parquet",
                size=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
                parquet=parquet,
            )
        )
    manifest_values = dict(
        job_id=job_id,
        spec_hash=spec.spec_hash,
        plan_hash=plan_hash,
        adapter_id="minute-runtime-replay",
        adapter_version="2",
        result_contract_version=reference["manifest"]["result_contract_version"],
        code_sha=commit,
        dataset_snapshot=spec.dataset_snapshot,
        files=tuple(sorted(files, key=lambda item: item.relative_path)),
    )
    complete_hash = hashlib.sha256(
        canonical_json_bytes(_complete_result_hash_payload(**manifest_values))
    ).hexdigest()
    manifest = LabJobArtifactManifest(**manifest_values, complete_result_hash=complete_hash)
    sealed = MinuteSealedReplayResult(
        job_id=job_id,
        shard_id=uuid5(job_id, "synthetic-renderer-shard"),
        owner_id=frozen.runtime.owner_id,
        spec_hash=spec.spec_hash,
        payload_hash=hashlib.sha256(
            result.model_dump_json(exclude_computed_fields=True).encode()
        ).hexdigest(),
        plan_hash=plan_hash,
        manifest_hash=manifest.manifest_hash,
        complete_result_hash=complete_hash,
        completed_at=_time(reference["completed_at"]),
        accepted_spec=spec,
        formal_plan=formal,
        manifest=manifest,
        result=result,
    )
    reference_values = _reference_values(reference)
    current_values = _reference_values(sealed.model_dump(mode="json", exclude_computed_fields=True))
    if current_values != reference_values:
        raise ValueError("minute renderer fixture changed original market/replay/reference values")
    return MinuteReportFixture(
        result=sealed,
        reference_sha256=REFERENCE_SHA256,
        python_version=tuple(sys.version_info[:3]),
        reference_values_sha256=hashlib.sha256(_json(reference_values).encode()).hexdigest(),
        compared_leaf_values=_leaf_count(reference_values),
    )
