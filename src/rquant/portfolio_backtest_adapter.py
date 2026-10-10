"""One sequential portfolio replay over a single, immutable, typed source row."""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path
from typing import Literal, Self

import duckdb
from pydantic import ConfigDict, Field, model_validator

from rquant.backtest.contracts import Sha256
from rquant.lab_shard_protocol import LabShardWorkPlan
from rquant.portfolio_backtest_models import (
    MAX_BUNDLE_BYTES,
    MAX_DATE_SPAN,
    MAX_SOURCE_PAIRS,
    FrozenPortfolioInput,
)
from rquant.portfolio_backtest_product import bundle_tables, execute_portfolio_input
from rquant.research_run_spec import ResearchJobType, ResearchRunSpec
from rquant.resource_admission import ResearchAdapterSourceUsage
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strategy_job_adapters import (
    DateBucketShardInput,
    LabShardExecutionResult,
    LabShardMetric,
    LabShardTable,
    StrategyShardInput,
    ValidatedStrategyShard,
)

PORTFOLIO_SOURCE_CONTRACT = "portfolio-daily-v1"
PORTFOLIO_INPUT_TABLE = "portfolio_backtest_input"
PORTFOLIO_SOURCE_COLUMNS = (("input_hash", "VARCHAR"), ("payload", "VARCHAR"))


class PortfolioBacktestParameters(RuntimeContractModel):
    input_hash: Sha256
    config_hash: Sha256
    request_id: Sha256
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(strict=True, ge=1)
    work_units: int = Field(strict=True, ge=1, le=MAX_SOURCE_PAIRS)

    @classmethod
    def from_frozen(cls, value: FrozenPortfolioInput) -> PortfolioBacktestParameters:
        return cls(
            input_hash=value.input_hash,
            config_hash=value.config.config_hash,
            request_id=value.request.request_id,
            source_key=value.config.source_key,
            source_version=value.config.source_version,
            work_units=max(
                len(value.request.days), sum(len(day.instruments) for day in value.request.days)
            ),
        )


class PortfolioBacktestRunInput(RuntimeContractModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    kind: Literal["portfolio_backtest"] = "portfolio_backtest"
    start_date: date
    end_date: date
    parameters: PortfolioBacktestParameters

    @model_validator(mode="after")
    def validate_dates(self) -> Self:
        if not 1 <= (self.end_date - self.start_date).days + 1 <= MAX_DATE_SPAN:
            raise ValueError("portfolio range exceeds date budget")
        return self

    @classmethod
    def from_frozen(cls, value: FrozenPortfolioInput) -> PortfolioBacktestRunInput:
        return cls(
            start_date=value.config.start_date,
            end_date=value.config.end_date,
            parameters=PortfolioBacktestParameters.from_frozen(value),
        )


def write_portfolio_input_table(
    connection: duckdb.DuckDBPyConnection, value: FrozenPortfolioInput
) -> None:
    """Trusted producer writes only a new private source database, never a main store."""
    checked = FrozenPortfolioInput.model_validate(value.model_dump(mode="python"))
    connection.execute(
        "CREATE TABLE portfolio_backtest_input "
        "(input_hash VARCHAR PRIMARY KEY, payload VARCHAR NOT NULL)"
    )
    connection.execute(
        "INSERT INTO portfolio_backtest_input VALUES (?, ?)",
        [checked.input_hash, checked.model_dump_json()],
    )


def read_portfolio_input_table(
    connection: duckdb.DuckDBPyConnection, *, require_primary_key: bool = False
) -> FrozenPortfolioInput:
    try:
        schema = connection.execute("PRAGMA table_info('portfolio_backtest_input')").fetchall()
    except duckdb.Error as error:
        raise ValueError("portfolio source schema is unavailable") from error
    if tuple((row[1], row[2]) for row in schema) != PORTFOLIO_SOURCE_COLUMNS:
        raise ValueError("portfolio source schema must match the exact contract")
    if require_primary_key and tuple(row[1] for row in schema if row[5]) != ("input_hash",):
        raise ValueError("portfolio source schema requires the exact primary key")
    rows = connection.execute("SELECT input_hash, payload FROM portfolio_backtest_input").fetchmany(
        2
    )
    if len(rows) != 1:
        raise ValueError("portfolio source must contain exactly one frozen input")
    identity, payload = rows[0]
    if not isinstance(payload, str) or len(payload.encode()) > MAX_BUNDLE_BYTES:
        raise ValueError("portfolio frozen source exceeds byte budget")
    value = FrozenPortfolioInput.model_validate_json(payload)
    if identity != value.input_hash:
        raise ValueError("portfolio source identity differs from frozen content")
    return value


def verify_portfolio_snapshot_source(
    connection: duckdb.DuckDBPyConnection,
    *,
    code_sha: str,
    start_date: date,
    end_date: date,
    input_hash: str,
) -> FrozenPortfolioInput:
    value = read_portfolio_input_table(connection, require_primary_key=True)
    if (
        value.input_hash,
        value.request.producer_commit,
        value.config.start_date,
        value.config.end_date,
    ) != (input_hash, code_sha, start_date, end_date):
        raise ValueError("portfolio snapshot differs from the frozen source identity")
    return value


class PortfolioBacktestAdapter:
    adapter_id = "portfolio-backtest"
    adapter_version = "1"
    strategy_name = "portfolio_backtest"
    snapshot_strategy_name = "portfolio_backtest"
    job_type = ResearchJobType.STRATEGY_REPLAY

    def source_usage(self) -> ResearchAdapterSourceUsage:
        return ResearchAdapterSourceUsage(
            adapter_id=self.adapter_id,
            external=False,
            immutable_snapshot=True,
            expected_calls=0,
            actual_calls=0,
        )

    def parameters(self, spec: ResearchRunSpec) -> PortfolioBacktestParameters:
        return PortfolioBacktestParameters.model_validate(
            {argument.name: argument.value for argument in spec.parameters.arguments}
        )

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        self.parameters(spec)
        return (
            DateBucketShardInput(
                start_date=spec.parameters.start_date, end_date=spec.parameters.end_date
            ),
        )

    def build_work_plan(self, spec: ResearchRunSpec, shard: StrategyShardInput) -> LabShardWorkPlan:
        parameters = self.parameters(spec)
        if not isinstance(shard, DateBucketShardInput) or (shard.start_date, shard.end_date) != (
            spec.parameters.start_date,
            spec.parameters.end_date,
        ):
            raise ValueError("portfolio shard must cover the full frozen date range")
        return LabShardWorkPlan(
            phase="portfolio_replay",
            work_unit_name="instrument_day",
            work_units=parameters.work_units,
            static_duration_ms=parameters.work_units * 1000,
        )

    def execute_shard(
        self, validated: ValidatedStrategyShard, store: object
    ) -> LabShardExecutionResult:
        self.build_work_plan(validated.spec, validated.shard)
        connection = getattr(store, "_conn", None)
        if not isinstance(connection, duckdb.DuckDBPyConnection):
            raise TypeError("portfolio replay requires the bound source connection")
        value = read_portfolio_input_table(connection)
        if self.parameters(validated.spec) != PortfolioBacktestParameters.from_frozen(value):
            raise ValueError("portfolio frozen source identity or work estimate differs")
        spec = validated.spec
        if (
            value.request.producer_commit,
            value.request.execution_cost_spec,
            value.config.start_date,
            value.config.end_date,
        ) != (
            spec.code_sha,
            spec.execution_costs,
            spec.parameters.start_date,
            spec.parameters.end_date,
        ):
            raise ValueError("portfolio frozen source code, costs or dates differ")
        with tempfile.TemporaryDirectory(prefix="portfolio-replay-") as private:
            bundle = execute_portfolio_input(value, research_root=Path(private))
            tables = bundle_tables(bundle)
        return LabShardExecutionResult.from_validated(
            validated,
            tables=tuple(LabShardTable(name=name, frame=frame) for name, frame in tables.items()),
            metrics=(
                LabShardMetric(name="portfolio_days", value=len(bundle.result.days)),
                LabShardMetric(name="portfolio_status", value=bundle.result.status),
                LabShardMetric(name="portfolio_bundle_hash", value=bundle.bundle_hash),
            ),
        )
