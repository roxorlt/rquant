"""A separate catalog and adapter2, retaining the actual formal shard identity."""

from __future__ import annotations

import json
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import Literal, Self

import pandas as pd
from pydantic import Field, model_validator

from rquant.lab_shard_protocol import LabShardWorkPlan
from rquant.minute_backtest_contracts import MAX_WORK_UNITS, MinuteReplayModel, Sha256
from rquant.minute_backtest_producer import (
    MinutePublicationReceipt, MinuteReplayCatalog, core_source_receipt,
    private_minute_workspace, read_minute_formal_input_table,
)
from rquant.minute_backtest_publication_contracts import FrozenMinuteResearchInput
from rquant.minute_backtest_runner import MinuteRuntimeReplayResult, minute_runtime_result_tables, run_minute_runtime_replay
from rquant.research_run_spec import ResearchJobType, ResearchRunSpec, ResourceClass
from rquant.research_snapshot import ResearchExecutionSession
from rquant.resource_admission import ResearchAdapterSourceUsage
from rquant.strategy_job_adapters import (
    DateBucketShardInput, LabShardExecutionResult, LabShardMetric, LabShardTable,
    StrategyJobAdapterRegistry, StrategyShardInput, ValidatedStrategyShard,
    build_adapter_execution_contract,
)


class MinuteFormalParameters(MinuteReplayModel):
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(ge=1)
    owner_id: str = Field(min_length=1, max_length=128)
    full_input_hash: Sha256
    core_input_hash: Sha256
    seed_hash: Sha256
    profile_hash: Sha256
    native_strategy_id: Literal["n_shape", "auction_gap", "growth_board_surge"]
    native_strategy_version: Literal[1]
    native_registration_hash: Sha256
    wrapper_registration_hash: Sha256
    work_units: int = Field(ge=1, le=MAX_WORK_UNITS)

    @classmethod
    def from_frozen(cls, value: FrozenMinuteResearchInput) -> MinuteFormalParameters:
        runtime = value.runtime
        return cls(source_key=runtime.source_key, source_version=runtime.source_version, owner_id=runtime.owner_id,
            full_input_hash=value.full_input_hash, core_input_hash=value.core_input_hash, seed_hash=value.source_content_seed.seed_hash,
            profile_hash=runtime.execution_profile.profile_hash, native_strategy_id=runtime.strategy.strategy_id,
            native_strategy_version=runtime.strategy.strategy_version, native_registration_hash=value.native_registration.record_hash,
            wrapper_registration_hash=value.wrapper_registration.record_hash, work_units=value.formal_work.work_units)


class MinuteFormalRunInput(MinuteReplayModel):
    kind: Literal["minute_runtime_replay"] = "minute_runtime_replay"
    start_date: date
    end_date: date
    parameters: MinuteFormalParameters

    @classmethod
    def from_frozen(cls, value: FrozenMinuteResearchInput) -> MinuteFormalRunInput:
        return cls(start_date=value.runtime.start_date, end_date=value.runtime.end_date,
            parameters=MinuteFormalParameters.from_frozen(value))


class MinuteFormalReplayResult(MinuteReplayModel):
    full_input_hash: Sha256
    core_input_hash: Sha256
    seed_hash: Sha256
    native_registration_hash: Sha256
    wrapper_registration_hash: Sha256
    publication: MinutePublicationReceipt
    replay: MinuteRuntimeReplayResult

    @model_validator(mode="after")
    def complete_result_binding(self) -> Self:
        value = self.publication.frozen
        if (self.full_input_hash, self.core_input_hash, self.seed_hash, self.native_registration_hash,
            self.wrapper_registration_hash) != (value.full_input_hash, value.core_input_hash,
            value.source_content_seed.seed_hash, value.native_registration.record_hash, value.wrapper_registration.record_hash):
            raise ValueError("minute formal result hashes differ from complete publication")
        if (self.replay.input_hash, self.replay.strategy_id, self.replay.strategy_version,
            self.replay.execution_profile, self.replay.profile_hash, self.replay.work, self.replay.result_budget) != (
            value.core_input_hash, value.runtime.strategy.strategy_id, value.runtime.strategy.strategy_version,
            value.runtime.execution_profile, value.runtime.execution_profile.profile_hash, value.runtime.work, value.result_budget):
            raise ValueError("minute formal replay input hash/profile/work differs from complete publication")
        return self


class MinuteFormalReplayAdapter:
    adapter_id = "minute-runtime-replay"
    adapter_version = "2"
    strategy_name = "minute_runtime_replay"
    snapshot_strategy_name = "minute_runtime_replay"
    job_type = ResearchJobType.STRATEGY_REPLAY

    def __init__(self, catalog: MinuteReplayCatalog) -> None:
        self.catalog = self._catalog_model().model_validate(catalog.model_dump(mode="python"))

    def _catalog_model(self) -> type[MinuteReplayCatalog]:
        return MinuteReplayCatalog

    def _parameter_model(self) -> type[MinuteFormalParameters]:
        return MinuteFormalParameters

    def _read_source(self, store: ResearchExecutionSession) -> FrozenMinuteResearchInput:
        return read_minute_formal_input_table(store._conn)

    def _replay_runtime(self, value: FrozenMinuteResearchInput, root: Path) -> MinuteRuntimeReplayResult:
        return run_minute_runtime_replay(value.runtime, expected=core_source_receipt(value), research_root=root / "replay")

    def _formal_result_model(self) -> type[MinuteFormalReplayResult]:
        return MinuteFormalReplayResult

    def _result_tables(self, replay: MinuteRuntimeReplayResult) -> dict[str, pd.DataFrame]:
        return minute_runtime_result_tables(replay)

    def source_usage(self) -> ResearchAdapterSourceUsage:
        return ResearchAdapterSourceUsage(adapter_id=self.adapter_id, external=False,
            immutable_snapshot=True, expected_calls=0, actual_calls=0)

    def expected(self, parameters: MinuteFormalParameters) -> MinutePublicationReceipt:
        expected = self.catalog.resolve(source_key=parameters.source_key, source_version=parameters.source_version, owner_id=parameters.owner_id)
        if parameters != self._parameter_model().from_frozen(expected.frozen):
            raise PermissionError("minute formal parameters differ from complete independent source receipt")
        return expected

    def parameters(self, spec: ResearchRunSpec) -> MinuteFormalParameters:
        if (spec.schema_version, spec.job_type, spec.parameters.strategy_name) != (3, self.job_type, self.strategy_name):
            raise PermissionError("minute runtime adapter2 requires its original formal schema3 identity")
        if spec.research_status != "comparable" or not spec.catalog_owner_eligible or spec.resource_class is not ResourceClass.STANDARD:
            raise PermissionError("minute runtime adapter2 requires formal ownership and STANDARD budget")
        arguments = {x.name: x.value for x in spec.parameters.arguments}
        if len(arguments) != len(spec.parameters.arguments):
            raise ValueError("minute formal parameters contain duplicate fields")
        parameters = self._parameter_model().model_validate(arguments)
        expected = self.expected(parameters)
        frozen = expected.frozen
        registration = frozen.wrapper_registration
        execution = spec.strategy_execution
        identity = spec.dataset_snapshot
        if execution is None or identity is None or spec.experiment is None:
            raise PermissionError("minute formal execution has missing original identities")
        if (spec.code_sha, spec.parameters.start_date, spec.parameters.end_date, spec.feature_contract,
            spec.execution_costs, identity.snapshot_id, identity.audit_run_id, identity.binding_hash,
            execution.strategy_id, execution.strategy_version, execution.adapter_id, execution.adapter_version,
            execution.strategy_spec_fingerprint, execution.strategy_definition_fingerprint,
            execution.strategy_executable_fingerprint, execution.candidate_schema_fingerprint,
            execution.definition_registration_record_hash, execution.definition_registered_at,
            execution.definition_available_at, execution.producer_code_commit) != (
            frozen.runtime.producer_commit, frozen.runtime.start_date, frozen.runtime.end_date,
            build_adapter_execution_contract(self.adapter_id, self.adapter_version, frozen.runtime.producer_commit),
            frozen.runtime.execution_profile.execution_costs, expected.snapshot.snapshot_id, expected.audit.audit_run_id,
            expected.binding.binding_hash, registration.logical_id, registration.version, self.adapter_id, self.adapter_version,
            registration.spec.spec_fingerprint, registration.fingerprint, registration.executable_fingerprint,
            registration.candidate_schema_fingerprint, registration.record_hash, registration.registered_at,
            registration.available_at, registration.producer_commit):
            raise PermissionError("minute actual formal RunSpec identity differs from complete source/definition receipts")
        return parameters

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        self.parameters(spec)
        return (DateBucketShardInput(start_date=spec.parameters.start_date, end_date=spec.parameters.end_date),)

    def build_work_plan(self, spec: ResearchRunSpec, shard: StrategyShardInput) -> LabShardWorkPlan:
        parameters = self.parameters(spec)
        if type(shard) is not DateBucketShardInput or (shard.start_date, shard.end_date) != (spec.parameters.start_date, spec.parameters.end_date):
            raise PermissionError("minute formal shard must cover the complete original selected range")
        return LabShardWorkPlan(phase=self.strategy_name, work_unit_name="complete_source_physical_input_bound",
            work_units=parameters.work_units, static_duration_ms=parameters.work_units * 1000)

    def execute_shard(self, validated: ValidatedStrategyShard, store: object) -> LabShardExecutionResult:
        parameters = self.parameters(validated.spec)
        self.build_work_plan(validated.spec, validated.shard)
        expected = self.expected(parameters)
        if type(store) is not ResearchExecutionSession or getattr(store, "_minute_gate_receipt", None) != expected:
            raise PermissionError("minute formal execution requires the original complete source gate/session")
        if store.binding != expected.binding:
            raise PermissionError("minute formal execution session differs from exact publication binding")
        value = self._read_source(store)
        if value != expected.frozen:
            raise PermissionError("minute execution full source differs from independent receipt")
        with private_minute_workspace() as root:
            replay = self._replay_runtime(value, root)
            result = self._formal_result_model()(full_input_hash=value.full_input_hash, core_input_hash=value.core_input_hash,
                seed_hash=value.source_content_seed.seed_hash, native_registration_hash=value.native_registration.record_hash,
                wrapper_registration_hash=value.wrapper_registration.record_hash, publication=expected, replay=replay)
            frames = self._result_tables(replay)
            summary = frames["replay_summary"]
            summary["full_input_hash"] = value.full_input_hash
            summary["core_input_hash"] = value.core_input_hash
            summary["seed_hash"] = value.source_content_seed.seed_hash
            summary["payload"] = result.model_dump_json(exclude_computed_fields=True)
            sizes = []
            for frame in frames.values():
                encoded = BytesIO()
                frame.to_parquet(encoded, index=False)
                sizes.append(len(encoded.getvalue()))
            budget = value.result_budget
            if len(frames) != budget.table_count or any(x > budget.table_bytes for x in sizes) or sum(sizes) > budget.total_bytes:
                raise PermissionError("minute complete formal results exceed the original eight-table budget")
            if sum(((x + 2) // 3) * 4 for x in sizes) > budget.wire_bytes:
                raise PermissionError("minute formal result exceeds the original wire budget")
        return LabShardExecutionResult.from_validated(validated,
            tables=tuple(LabShardTable(name=name, frame=frame) for name, frame in frames.items()),
            metrics=(LabShardMetric(name="minute_formal_full_input_hash", value=value.full_input_hash),
                LabShardMetric(name="minute_core_input_hash", value=value.core_input_hash),
                LabShardMetric(name="minute_native_strategy", value=value.runtime.strategy.strategy_id),
                LabShardMetric(name="minute_source_kind", value=value.provenance.source_kind),
                LabShardMetric(name="minute_daily_status", value=replay.daily_status),
                LabShardMetric(name="minute_full_result_bytes", value=sum(sizes))))


def minute_formal_adapter_registry(catalog: MinuteReplayCatalog) -> StrategyJobAdapterRegistry:
    return StrategyJobAdapterRegistry((MinuteFormalReplayAdapter(catalog),))
