"""Two fixed adapters append to the original closed Lab registry."""

from __future__ import annotations

import duckdb
import pandas as pd

from rquant.lab_shard_protocol import LabShardWorkPlan
from rquant.paper_research import (FrozenPaperResearchInput, MAX_PAPER_RESEARCH_INPUT_BYTES, PAPER_RESEARCH_INPUT_TABLE,
                                    PaperResearchAdapterCatalog, PaperResearchRunParameters)
from rquant.research_run_spec import ResearchJobType, ResearchRunSpec
from rquant.resource_admission import ResearchAdapterSourceUsage
from rquant.strategy_job_adapters import (DateBucketShardInput, LabShardExecutionResult, LabShardTable, StrategyJobAdapterRegistry,
                                         StrategyShardInput, ValidatedStrategyShard, default_strategy_job_adapter_registry)


def write_paper_research_input(connection: duckdb.DuckDBPyConnection, value: FrozenPaperResearchInput) -> None:
    value = FrozenPaperResearchInput.model_validate(value.model_dump(mode="python"))
    connection.execute("CREATE TEMP TABLE paper_research_input(input_hash VARCHAR PRIMARY KEY,payload VARCHAR NOT NULL)")
    connection.execute("INSERT INTO paper_research_input VALUES(?,?)", [value.fingerprint, value.model_dump_json()])


def read_paper_research_input(connection: duckdb.DuckDBPyConnection, *, input_hash: str) -> FrozenPaperResearchInput:
    columns = connection.execute("DESCRIBE paper_research_input").fetchall()
    if tuple((item[0], item[1]) for item in columns) != (("input_hash", "VARCHAR"), ("payload", "VARCHAR")):
        raise ValueError("paper research source schema differs")
    rows = connection.execute("SELECT input_hash,octet_length(encode(payload)) FROM paper_research_input LIMIT 2").fetchall()
    if len(rows) != 1 or rows[0][0] != input_hash or not 1 <= rows[0][1] <= MAX_PAPER_RESEARCH_INPUT_BYTES:
        raise ValueError("paper research source identity or byte budget differs")
    raw = connection.execute("SELECT payload FROM paper_research_input WHERE input_hash=?", [input_hash]).fetchone()[0]
    value = FrozenPaperResearchInput.model_validate_json(raw)
    if value.fingerprint != input_hash:
        raise ValueError("paper research complete input was replaced")
    return value


class PaperResearchAdapter:
    adapter_version = "1"
    job_type = ResearchJobType.STRATEGY_REPLAY

    def __init__(self, task_name: str, *, catalog: PaperResearchAdapterCatalog) -> None:
        if type(catalog) is not PaperResearchAdapterCatalog or task_name not in ("paper_reconcile", "paper_backtest_band"):
            raise TypeError("paper adapter requires its exact finite task and typed catalog")
        self.strategy_name = self.snapshot_strategy_name = task_name
        self.adapter_id = "paper-reconcile" if task_name == "paper_reconcile" else "paper-backtest-band"
        self.catalog = catalog

    def source_usage(self) -> ResearchAdapterSourceUsage:
        return ResearchAdapterSourceUsage(adapter_id=self.adapter_id, external=False, immutable_snapshot=True, expected_calls=0, actual_calls=0)

    def parameters(self, spec: ResearchRunSpec) -> PaperResearchRunParameters:
        spec = ResearchRunSpec.model_validate(spec)
        if (spec.schema_version != 2 or spec.research_status != "exploratory" or spec.parameters.strategy_name != self.strategy_name
                or spec.dataset_snapshot is None or spec.dataset_snapshot.audit_run_id is None
                or spec.strategy_execution is not None or spec.experiment is not None
                or spec.execution_costs != self.catalog.configuration.execution_cost_spec or spec.random_seed != 20261005):
            raise ValueError("paper research requires its original exact exploratory source and cost")
        result = PaperResearchRunParameters.model_validate({item.name: item.value for item in spec.parameters.arguments})
        result.require_catalog(self.catalog)
        if self.strategy_name == "paper_backtest_band" and result.work_units > 2520:
            raise ValueError("paper band exceeds its fixed 2520-day work budget")
        return result

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        self.parameters(spec)
        return (DateBucketShardInput(start_date=spec.parameters.start_date, end_date=spec.parameters.end_date),)

    def build_work_plan(self, spec: ResearchRunSpec, shard: StrategyShardInput) -> LabShardWorkPlan:
        parameters = self.parameters(spec)
        if shard != self.build_shard_inputs(spec)[0]:
            raise ValueError("paper analysis requires one complete frozen source shard")
        band = self.strategy_name == "paper_backtest_band"
        return LabShardWorkPlan(phase="paper_bootstrap" if band else "paper_readonly_reconcile",
                                work_unit_name="bootstrap_day_2048_paths" if band else "ledger_record",
                                work_units=parameters.work_units, static_duration_ms=max(30000, parameters.work_units*(1000 if band else 20)))

    def execute_shard(self, validated: ValidatedStrategyShard, store: object) -> LabShardExecutionResult:
        from rquant.paper_reconcile import execute_paper_reconcile
        from rquant.paper_portfolio_band import execute_paper_backtest_band

        parameters = self.parameters(validated.spec)
        connection = getattr(store, "_conn", None)
        if not isinstance(connection, duckdb.DuckDBPyConnection) or validated.shard != self.build_shard_inputs(validated.spec)[0]:
            raise TypeError("paper execution requires its original immutable source session and exact shard")
        value = read_paper_research_input(connection, input_hash=parameters.input_hash)
        if (value.catalog != self.catalog or PaperResearchRunParameters.from_input(value, request_id=parameters.request_id) != parameters
                or value.code_sha != validated.spec.code_sha or value.task_name != self.strategy_name
                or value.dates != (validated.spec.parameters.start_date, validated.spec.parameters.end_date)):
            raise ValueError("paper frozen source differs from its original plan")
        result = execute_paper_reconcile(value.reconcile) if value.reconcile is not None else execute_paper_backtest_band(value.band)
        reference = {**parameters.model_dump(mode="python"), "task_name": value.task_name, "result_hash": result.fingerprint,
                     "source_hash": value.reconcile.fingerprint if value.reconcile is not None else value.band.fingerprint,
                     "configuration": value.catalog.configuration.model_dump_json(), "metadata_identity": value.catalog.metadata_identity.model_dump_json(),
                     "available_at": value.available_at.isoformat(), "complete": True}
        return LabShardExecutionResult.from_validated(validated, tables=(
            LabShardTable(name="paper_reference", frame=pd.DataFrame((reference,))),
            LabShardTable(name="paper_result", frame=pd.DataFrame(({"result_hash": result.fingerprint, "payload": result.model_dump_json()},))),))


def paper_research_adapter_registry(catalog: PaperResearchAdapterCatalog) -> StrategyJobAdapterRegistry:
    catalog = PaperResearchAdapterCatalog.model_validate(catalog.model_dump(mode="python"))
    original = default_strategy_job_adapter_registry()
    builtin = tuple(original.get(item.adapter_id, item.adapter_version) for item in original.closed_descriptor().adapters)
    return StrategyJobAdapterRegistry((*builtin, PaperResearchAdapter("paper_reconcile", catalog=catalog), PaperResearchAdapter("paper_backtest_band", catalog=catalog)))
