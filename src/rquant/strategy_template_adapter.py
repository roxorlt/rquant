"""Finite exact adapters from committed metadata and original definition receipts."""

from __future__ import annotations

import re
import tempfile
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Literal, Self
from uuid import UUID

import duckdb
import pandas as pd
from pydantic import Field, field_validator, model_validator

from rquant.backtest.contracts import Sha256
from rquant.lab_shard_protocol import LabShardWorkPlan
from rquant.perf import performance_summary
from rquant.research_run_spec import ResearchJobType, ResearchRunSpec
from rquant.resource_admission import ResearchAdapterSourceUsage
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_authoring_source import template_source_code_identity
from rquant.strategy_job_adapters import (
    DateBucketShardInput,
    LabShardExecutionResult,
    LabShardTable,
    StrategyJobAdapterRegistry,
    StrategyShardInput,
    ValidatedStrategyShard,
    default_strategy_job_adapter_registry,
)
from rquant.strategy_template import (
    TEMPLATE_ID_PATTERN,
)
from rquant.strategy_template_definition import StrategyTemplateExecutionVersion
from rquant.strategy_template_run import (
    FrozenStrategyTemplateInput,
    StrategyTemplateResult,
    execute_strategy_template_input,
)

TEMPLATE_INPUT_TABLE = "strategy_template_input"
TEMPLATE_INPUT_MAX_BYTES = 16 * 1024 * 1024
TEMPLATE_INPUT_CONTRACT = "strategy-template-input/v1"


def template_adapter_id(strategy_id: str) -> str:
    if re.fullmatch(TEMPLATE_ID_PATTERN, strategy_id) is None:
        raise ValueError("template adapter requires an exact logical ID")
    return "strategy-template:" + strategy_id


def template_input_work_units(value: FrozenStrategyTemplateInput) -> int:
    total = 0
    for day, evidence in zip(value.request.days, value.days, strict=True):
        raw = evidence.entry.evidence
        codes = {item.ts_code for item in (*day.instruments, *day.ranking.candidates, *raw.signals)}
        codes.update(raw.pool_codes)
        codes.update(row["ts_code"] for row in raw.rows if isinstance(row.get("ts_code"), str))
        codes.update(item.quote.ts_code for item in evidence.minutes)
        total += len(codes) + len(evidence.minutes)
    return max(1, total)


class StrategyTemplateRunParameters(RuntimeContractModel):
    owner_id: str = Field(min_length=1, max_length=128)
    strategy_id: str = Field(pattern=TEMPLATE_ID_PATTERN)
    version: int = Field(strict=True, ge=1, le=4096)
    registration_fingerprint: Sha256
    record_hash: Sha256
    spec_fingerprint: Sha256
    rules_hash: Sha256
    source_code_identity: Sha256
    input_hash: Sha256
    request_id: str
    work_units: int = Field(strict=True, ge=1, le=20000)

    @field_validator("request_id")
    @classmethod
    def canonical_request_id(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("template request ID must be a canonical UUID")
        return value

    @classmethod
    def from_input(cls, value: FrozenStrategyTemplateInput, *, request_id: str) -> Self:
        definition = value.definition
        return cls(
            owner_id=value.owner_id,
            strategy_id=definition.logical_id,
            version=definition.version,
            registration_fingerprint=definition.fingerprint,
            record_hash=definition.record_hash,
            spec_fingerprint=definition.spec.spec_fingerprint,
            rules_hash=canonical_sha256(value.rules),
            source_code_identity=value.source_code_identity,
            input_hash=value.input_hash,
            request_id=request_id,
            work_units=template_input_work_units(value),
        )


class StrategyTemplateRunInput(RuntimeContractModel):
    kind: Literal["strategy_template"] = "strategy_template"
    start_date: date
    end_date: date
    parameters: StrategyTemplateRunParameters

    @model_validator(mode="after")
    def bounded_dates(self) -> Self:
        if not 1 <= (self.end_date - self.start_date).days + 1 <= 5 * 366:
            raise ValueError("template date range exceeds the research budget")
        return self


class StrategyTemplateAdapterCatalog(RuntimeContractModel):
    contract: Literal["strategy-template-catalog/v1"] = "strategy-template-catalog/v1"
    metadata_identity: StrategyAuthoringIdentity
    source_code_identity: Sha256
    versions: tuple[StrategyTemplateExecutionVersion, ...] = Field(max_length=4096)
    catalog_hash: Sha256 | None = None

    @model_validator(mode="after")
    def complete_catalog(self) -> Self:
        keys = tuple((item.strategy_id, item.head.version) for item in self.versions)
        if keys != tuple(sorted(set(keys))) or len({key[0] for key in keys}) > 500:
            raise ValueError("template catalog identities are duplicated, unordered or over budget")
        if self.source_code_identity != template_source_code_identity():
            raise ValueError("template catalog source implementation differs")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"catalog_hash"}))
        if self.catalog_hash is None:
            object.__setattr__(self, "catalog_hash", expected)
        elif self.catalog_hash != expected:
            raise ValueError("template catalog hash differs from original definitions")
        return self


def build_strategy_template_adapter_catalog(
    store: StrategyAuthoringStore,
    *,
    expected_identity: StrategyAuthoringIdentity,
    selected_keys: tuple[tuple[str, int], ...] | None = None,
) -> StrategyTemplateAdapterCatalog:
    if type(store) is not StrategyAuthoringStore:
        raise TypeError("template catalog requires the concrete original metadata store")
    versions: list[StrategyTemplateExecutionVersion] = []
    with store._connection(expected_identity=expected_identity) as connection:
        rows = connection.execute(
            "SELECT strategy_id,version,owner_id FROM versions ORDER BY strategy_id,version LIMIT 4097"
        ).fetchall()
        if len(rows) > 4096:
            raise ValueError("template catalog exceeds the committed version budget")
        if selected_keys is not None:
            available = {(row["strategy_id"], row["version"]) for row in rows}
            if (
                not selected_keys
                or len(set(selected_keys)) != len(selected_keys)
                or not set(selected_keys) <= available
            ):
                raise ValueError("template catalog selection is outside committed exact versions")
            rows = tuple(
                row for row in rows if (row["strategy_id"], row["version"]) in selected_keys
            )
        for row in rows:
            metadata = store._version(
                connection, row["strategy_id"], row["version"], row["owner_id"]
            )
            definition = store.definition_registry(metadata.strategy_id).read_strategy_spec(
                metadata.head.registration_fingerprint
            )
            if definition is None:
                raise ValueError("template catalog lost an original definition")
            versions.append(
                StrategyTemplateExecutionVersion(
                    owner_id=metadata.owner_id,
                    strategy_id=metadata.strategy_id,
                    head=metadata.head,
                    rules=metadata.rules,
                    definition=definition,
                )
            )
    if store.identity() != expected_identity:
        raise ValueError("template catalog metadata identity changed during the read")
    return StrategyTemplateAdapterCatalog(
        metadata_identity=expected_identity,
        source_code_identity=template_source_code_identity(),
        versions=tuple(versions),
    )


def write_strategy_template_input(
    connection: duckdb.DuckDBPyConnection, value: FrozenStrategyTemplateInput
) -> None:
    value = FrozenStrategyTemplateInput.model_validate(value.model_dump(mode="python"))
    connection.execute(
        "CREATE TEMP TABLE strategy_template_input (input_hash VARCHAR PRIMARY KEY, payload VARCHAR NOT NULL)"
    )
    connection.execute(
        "INSERT INTO strategy_template_input VALUES (?,?)",
        [value.input_hash, value.model_dump_json()],
    )


def read_strategy_template_input(
    connection: duckdb.DuckDBPyConnection, *, input_hash: str
) -> FrozenStrategyTemplateInput:
    columns = connection.execute("DESCRIBE strategy_template_input").fetchall()
    if tuple((row[0], row[1]) for row in columns) != (
        ("input_hash", "VARCHAR"),
        ("payload", "VARCHAR"),
    ):
        raise ValueError("template input source schema differs")
    sizes = connection.execute(
        "SELECT input_hash, octet_length(encode(payload)) FROM strategy_template_input LIMIT 2"
    ).fetchall()
    if (
        len(sizes) != 1
        or sizes[0][0] != input_hash
        or not 1 <= sizes[0][1] <= TEMPLATE_INPUT_MAX_BYTES
    ):
        raise ValueError("template input source binding or byte budget differs")
    payload = connection.execute(
        "SELECT payload FROM strategy_template_input WHERE input_hash=?", [input_hash]
    ).fetchone()
    value = FrozenStrategyTemplateInput.model_validate_json(payload[0])
    if value.input_hash != input_hash:
        raise ValueError("template input content differs from the bound source")
    return value


class StrategyTemplateAdapter:
    adapter_version = "1"
    job_type = ResearchJobType.STRATEGY_REPLAY

    def __init__(self, strategy_id: str, *, catalog: StrategyTemplateAdapterCatalog) -> None:
        self._versions = tuple(item for item in catalog.versions if item.strategy_id == strategy_id)
        if not self._versions:
            raise ValueError("template adapter requires a committed exact definition")
        self.adapter_id = template_adapter_id(strategy_id)
        self.strategy_name = strategy_id
        self.snapshot_strategy_name = strategy_id
        self.catalog = catalog

    def source_usage(self) -> ResearchAdapterSourceUsage:
        return ResearchAdapterSourceUsage(
            adapter_id=self.adapter_id,
            external=False,
            immutable_snapshot=True,
            expected_calls=0,
            actual_calls=0,
        )

    def parameters(self, spec: ResearchRunSpec) -> StrategyTemplateRunParameters:
        spec = ResearchRunSpec.model_validate(spec)
        parameters = StrategyTemplateRunParameters.model_validate(
            {item.name: item.value for item in spec.parameters.arguments}
        )
        matching = tuple(item for item in self._versions if item.head.version == parameters.version)
        if len(matching) != 1:
            raise ValueError("template definition version is outside the committed catalog")
        item = matching[0]
        definition, execution = item.definition, spec.strategy_execution
        if (
            spec.schema_version != 3
            or execution is None
            or spec.parameters.strategy_name != self.strategy_name
        ):
            raise ValueError("template input requires exact original v3 ownership")
        if (
            parameters.owner_id,
            parameters.strategy_id,
            parameters.registration_fingerprint,
            parameters.record_hash,
            parameters.spec_fingerprint,
            parameters.rules_hash,
            parameters.source_code_identity,
        ) != (
            item.owner_id,
            item.strategy_id,
            definition.fingerprint,
            definition.record_hash,
            definition.spec.spec_fingerprint,
            canonical_sha256(item.rules),
            self.catalog.source_code_identity,
        ):
            raise ValueError("template parameter binding differs from the original definition")
        if (
            execution.strategy_id,
            execution.strategy_version,
            execution.adapter_id,
            execution.adapter_version,
            execution.strategy_spec_fingerprint,
            execution.strategy_definition_fingerprint,
            execution.definition_registration_record_hash,
            execution.strategy_executable_fingerprint,
            execution.candidate_schema_fingerprint,
            execution.definition_registered_at,
            execution.definition_available_at,
            execution.producer_code_commit,
            spec.code_sha,
        ) != (
            self.strategy_name,
            definition.version,
            self.adapter_id,
            "1",
            definition.spec.spec_fingerprint,
            definition.fingerprint,
            definition.record_hash,
            definition.executable_fingerprint,
            definition.candidate_schema_fingerprint,
            definition.registered_at,
            definition.available_at,
            definition.producer_commit,
            definition.producer_commit,
        ):
            raise ValueError("template execution binding differs from the original definition")
        return parameters

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        self.parameters(spec)
        return (
            DateBucketShardInput(
                start_date=spec.parameters.start_date, end_date=spec.parameters.end_date
            ),
        )

    def build_work_plan(self, spec: ResearchRunSpec, shard: StrategyShardInput) -> LabShardWorkPlan:
        parameters = self.parameters(spec)
        if shard != self.build_shard_inputs(spec)[0]:
            raise ValueError("template input requires one complete date shard")
        return LabShardWorkPlan(
            phase="strategy_template_replay",
            work_unit_name="code_day",
            work_units=parameters.work_units,
            static_duration_ms=max(30000, parameters.work_units * 1000),
        )

    def execute_shard(
        self, validated: ValidatedStrategyShard, store: object
    ) -> LabShardExecutionResult:
        parameters = self.parameters(validated.spec)
        if validated.shard != self.build_shard_inputs(validated.spec)[0]:
            raise ValueError("template input shard differs from the complete plan")
        connection = getattr(store, "_conn", None)
        if not isinstance(connection, duckdb.DuckDBPyConnection):
            raise TypeError("template execution requires the original immutable DuckDB store")
        value = read_strategy_template_input(connection, input_hash=parameters.input_hash)
        if StrategyTemplateRunParameters.from_input(
            value, request_id=parameters.request_id
        ) != parameters or (
            value.request.days[0].trade_date,
            value.request.days[-1].trade_date,
            value.request.execution_cost_spec,
        ) != (
            validated.spec.parameters.start_date,
            validated.spec.parameters.end_date,
            validated.spec.execution_costs,
        ):
            raise ValueError("template input binding differs from the original run plan")
        with tempfile.TemporaryDirectory(prefix="rquant-template-replay-") as private:
            result = execute_strategy_template_input(value, research_root=Path(private).resolve())
        return LabShardExecutionResult.from_validated(
            validated, tables=template_result_tables(value, parameters, result)
        )


def template_result_tables(
    value: FrozenStrategyTemplateInput,
    parameters: StrategyTemplateRunParameters,
    result: StrategyTemplateResult,
) -> tuple[LabShardTable, ...]:
    result = StrategyTemplateResult.model_validate(result.model_dump(mode="python"))
    if (
        result.owner_id,
        result.strategy_id,
        result.version,
        result.definition_fingerprint,
        result.definition_record_hash,
        result.input_hash,
        result.calendar_source_identity,
        result.cost_spec_id,
    ) != (
        parameters.owner_id,
        parameters.strategy_id,
        parameters.version,
        parameters.registration_fingerprint,
        parameters.record_hash,
        parameters.input_hash,
        value.request.calendar.source_identity,
        value.request.execution_cost_spec.cost_spec_id,
    ):
        raise ValueError("template result binding differs from the original input and definition")
    reference = {
        name: getattr(parameters, name)
        for name in (
            "owner_id",
            "strategy_id",
            "version",
            "registration_fingerprint",
            "record_hash",
            "spec_fingerprint",
            "input_hash",
            "rules_hash",
            "source_code_identity",
            "request_id",
        )
    }
    reference.update(result_hash=result.content_hash, complete=result.status == "complete")
    equity = [
        {
            "trade_date": day.trade_date.isoformat(),
            "cash": None if day.account is None else str(day.account.cash),
            "nav": None if day.account is None else str(day.account.nav),
            "fees": str(day.fees),
            "daily_return": None if day.daily_return is None else str(day.daily_return),
            "normalized_nav": None if day.normalized_nav is None else str(day.normalized_nav),
        }
        for day in result.days
    ]
    orders = [
        {
            "trade_date": day.trade_date.isoformat(),
            "ts_code": order.intent.ts_code,
            "side": order.intent.side.value,
            "intent_id": order.intent.intent_id,
            "execution_id": order.receipt.execution_id,
            "entry_signal_id": order.intent.entry_signal_id,
            "quantity": 0 if order.receipt.fill is None else order.receipt.fill.quantity,
            "price": None if order.receipt.fill is None else str(order.receipt.fill.price),
            "fees": None if order.receipt.fill is None else str(order.receipt.fill.total_fees),
            "cost_spec_id": order.receipt.cost_spec_id,
            "status": order.receipt.order.status.value,
        }
        for day in result.days
        for order in day.orders
    ]
    complete = result.status == "complete" and all(
        day.daily_return is not None for day in result.days
    )
    returns = pd.Series(
        [float(day.daily_return) for day in result.days] if complete else [],
        index=pd.DatetimeIndex([day.trade_date for day in result.days] if complete else []),
        dtype="float64",
    )
    summary = asdict(performance_summary(returns))
    summary.update(
        complete=complete,
        execution_convention=result.execution_convention,
        cost_spec_id=result.cost_spec_id,
    )
    return (
        LabShardTable(name="template_reference", frame=pd.DataFrame([reference])),
        LabShardTable(
            name="template_result",
            frame=pd.DataFrame(
                [{"result_hash": result.content_hash, "payload": result.model_dump_json()}]
            ),
        ),
        LabShardTable(
            name="equity",
            frame=pd.DataFrame(
                equity,
                columns=("trade_date", "cash", "nav", "fees", "daily_return", "normalized_nav"),
            ),
        ),
        LabShardTable(
            name="orders",
            frame=pd.DataFrame(
                orders,
                columns=(
                    "trade_date",
                    "ts_code",
                    "side",
                    "intent_id",
                    "execution_id",
                    "entry_signal_id",
                    "quantity",
                    "price",
                    "fees",
                    "cost_spec_id",
                    "status",
                ),
            ),
        ),
        LabShardTable(
            name="exits",
            frame=pd.DataFrame(
                [item.model_dump(mode="json") for item in result.exit_decisions],
                columns=(
                    "trade_date",
                    "ts_code",
                    "entry_signal_id",
                    "decision_id",
                    "reason",
                    "decided_at",
                ),
            ),
        ),
        LabShardTable(name="summary", frame=pd.DataFrame([summary])),
    )


def strategy_template_adapter_registry(
    catalog: StrategyTemplateAdapterCatalog,
) -> StrategyJobAdapterRegistry:
    catalog = StrategyTemplateAdapterCatalog.model_validate(catalog.model_dump(mode="python"))
    original = default_strategy_job_adapter_registry()
    builtins = tuple(
        original.get(item.adapter_id, item.adapter_version)
        for item in original.closed_descriptor().adapters
    )
    ids = sorted({item.strategy_id for item in catalog.versions})
    return StrategyJobAdapterRegistry(
        (*builtins, *(StrategyTemplateAdapter(strategy_id, catalog=catalog) for strategy_id in ids))
    )
