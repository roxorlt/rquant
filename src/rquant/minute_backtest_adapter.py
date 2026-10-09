"""Additive one-shard adapter; registration and formal source admission are separate."""

from __future__ import annotations

import tempfile
from io import BytesIO
from pathlib import Path
from typing import Literal

import duckdb

from rquant.lab_shard_protocol import LabShardWorkPlan
from rquant.minute_backtest_contracts import (
    MAX_WORK_UNITS,
    FrozenMinuteRuntimeInput,
    MinuteReplayModel,
    MinuteRuntimeSourceReceipt,
    Sha256,
)
from rquant.minute_backtest_runner import MinuteRuntimeReplayRunner, minute_runtime_result_tables
from rquant.minute_backtest_source import read_minute_runtime_input_table, restore_minute_runtime_source
from rquant.research_run_spec import ResearchJobType, ResearchRunSpec
from rquant.resource_admission import ResearchAdapterSourceUsage
from rquant.strategy_job_adapters import (
    DateBucketShardInput,
    LabShardExecutionResult,
    LabShardMetric,
    LabShardTable,
    StrategyShardInput,
    ValidatedStrategyShard,
)
from pydantic import Field


class MinuteRuntimeReplayParameters(MinuteReplayModel):
    input_hash: Sha256
    profile_hash: Sha256
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(ge=1)
    strategy_id: Literal["n_shape", "auction_gap", "growth_board_surge"]
    strategy_version: Literal[1]
    work_units: int = Field(ge=1, le=MAX_WORK_UNITS)

    @classmethod
    def from_frozen(cls, value: FrozenMinuteRuntimeInput, *, work_units: int) -> MinuteRuntimeReplayParameters:
        return cls(input_hash=value.input_hash, profile_hash=value.execution_profile.profile_hash,
            source_key=value.source_key, source_version=value.source_version,
            strategy_id=value.strategy.strategy_id, strategy_version=value.strategy.strategy_version, work_units=work_units)


class MinuteRuntimeReplayAdapter:
    adapter_id = "minute-runtime-replay"
    adapter_version = "1"
    strategy_name = "minute_runtime_replay"
    snapshot_strategy_name = "minute_runtime_replay"
    job_type = ResearchJobType.STRATEGY_REPLAY

    def __init__(self, *, expected: MinuteRuntimeSourceReceipt) -> None:
        self.expected = MinuteRuntimeSourceReceipt.model_validate(expected.model_dump(mode="python"))

    def source_usage(self) -> ResearchAdapterSourceUsage:
        return ResearchAdapterSourceUsage(adapter_id=self.adapter_id, external=False,
            immutable_snapshot=True, expected_calls=0, actual_calls=0)

    def bound_parameters(self, value: MinuteRuntimeReplayParameters) -> MinuteRuntimeReplayParameters:
        checked = MinuteRuntimeReplayParameters.model_validate(value.model_dump(mode="python"))
        expected = MinuteRuntimeReplayParameters(input_hash=self.expected.input_hash, profile_hash=self.expected.profile_hash,
            source_key=self.expected.source_key, source_version=self.expected.source_version,
            strategy_id=self.expected.strategy_id, strategy_version=self.expected.strategy_version, work_units=self.expected.work.work_units)
        if checked != expected:
            raise ValueError("minute runtime parameters differ from independent source, profile, strategy or physical work")
        return checked

    def parameters(self, spec: ResearchRunSpec) -> MinuteRuntimeReplayParameters:
        if spec.job_type is not self.job_type or spec.parameters.strategy_name != self.strategy_name:
            raise ValueError("minute runtime adapter requires its own unique research identity")
        arguments = {argument.name: argument.value for argument in spec.parameters.arguments}
        if len(arguments) != len(spec.parameters.arguments):
            raise ValueError("minute runtime parameters contain duplicate fields")
        return self.bound_parameters(MinuteRuntimeReplayParameters.model_validate(arguments))

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        self.parameters(spec)
        return (DateBucketShardInput(start_date=spec.parameters.start_date, end_date=spec.parameters.end_date),)

    def build_work_plan(self, spec: ResearchRunSpec, shard: StrategyShardInput) -> LabShardWorkPlan:
        parameters = self.parameters(spec)
        if type(shard) is not DateBucketShardInput or (shard.start_date, shard.end_date) != (spec.parameters.start_date, spec.parameters.end_date):
            raise ValueError("minute runtime shard must cover the full exact input range")
        if (spec.code_sha, spec.parameters.start_date, spec.parameters.end_date, parameters.input_hash,
            parameters.source_key, parameters.source_version) != (self.expected.producer_commit, self.expected.start_date,
            self.expected.end_date, self.expected.input_hash, self.expected.source_key, self.expected.source_version):
            raise ValueError("minute runtime plan differs from independent source receipt")
        return LabShardWorkPlan(phase="minute_runtime_replay", work_unit_name="physical_input_bound",
            work_units=parameters.work_units, static_duration_ms=parameters.work_units * 1_000)

    def execute_shard(self, validated: ValidatedStrategyShard, store: object) -> LabShardExecutionResult:
        self.build_work_plan(validated.spec, validated.shard)
        connection = getattr(store, "_conn", None)
        if type(connection) is not duckdb.DuckDBPyConnection:
            raise TypeError("minute runtime adapter requires the independently bound source connection")
        value = read_minute_runtime_input_table(connection, expected=self.expected)
        if value.execution_profile.execution_costs != validated.spec.execution_costs:
            raise ValueError("minute runtime source costs differ from the frozen research execution")
        with tempfile.TemporaryDirectory(prefix="minute-runtime-replay-") as temporary:
            source = restore_minute_runtime_source(value, expected=self.expected, research_root=Path(temporary) / "replay")
            if self.parameters(validated.spec) != MinuteRuntimeReplayParameters.from_frozen(value, work_units=source.work.work_units):
                raise ValueError("minute runtime work, profile or selected strategy differs from actual source")
            result = MinuteRuntimeReplayRunner(source).run()
            frames = minute_runtime_result_tables(result)
            sizes = []
            for frame in frames.values():
                output = BytesIO()
                frame.to_parquet(output, index=False)
                sizes.append(len(output.getvalue()))
            budget = self.expected.result_budget
            if result.result_budget != budget or len(frames) > budget.table_count or any(size > budget.table_bytes for size in sizes) or sum(sizes) > budget.total_bytes:
                raise ValueError("minute runtime full results exceed the original output budget")
            if sum(((size + 2) // 3) * 4 for size in sizes) > budget.wire_bytes:
                raise ValueError("minute runtime full results exceed the original wire budget")
        return LabShardExecutionResult.from_validated(validated,
            tables=tuple(LabShardTable(name=name, frame=frame) for name, frame in frames.items()),
            metrics=(LabShardMetric(name="minute_replay_status", value=result.status),
                LabShardMetric(name="minute_replay_input_hash", value=result.input_hash),
                LabShardMetric(name="minute_replay_signals", value=len(result.signals)),
                LabShardMetric(name="minute_replay_fills", value=len(result.fills))))
