"""Bounded read-only Serving projection of audited formula pools and latest daily evidence."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.formula_market_job_projection import (
    FormulaMarketVerifiedTask,
    read_formula_market_tasks_exact,
)
from rquant.formula_pool_daily import (
    FormulaPoolDailyResultStore,
    FormulaPoolDailyResultV1,
    _run_identity,
)
from rquant.formula_pool_definition import (
    _NAME,
    FormulaPoolDefinitionStore,
    FormulaPoolDefinitionV1,
    _canonical_path,
    _file_identity,
    _open_private_directory,
)
from rquant.pool_definition_projection import PoolMutation
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    normalize_aware_utc,
)
from rquant.screen.tdx.ast import SYNTAX_VERSION
from rquant.serving_publisher import ServingReader, quote_serving_table_identifier
from rquant.serving_read_models import (
    PAGE_PROJECTION_CONTRACTS,
    ServingProjectionInput,
    ServingProjectionPayload,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

FORMULA_POOL_PROJECTION_TABLES = frozenset(
    {"formula_pool_state", "formula_pool_definition", "formula_pool_latest_result"}
)
MAX_FORMULA_POOL_DEFINITIONS = 512
_MAX_DAILY_DAYS = 4096
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class FormulaPoolServingConfig(RuntimeContractModel):
    universe_root: Path
    projection_root: Path
    task_state_path: Path
    artifact_root: Path
    definition_root: Path
    daily_root: Path
    rule_root: Path

    @model_validator(mode="after")
    def validate_roots(self) -> FormulaPoolServingConfig:
        paths = (
            self.universe_root,
            self.projection_root,
            self.task_state_path,
            self.artifact_root,
            self.definition_root,
            self.daily_root,
            self.rule_root,
        )
        if any(_canonical_path(path) != path for path in paths) or len(set(paths)) != len(paths):
            raise ValueError("formula pool Serving paths must be distinct and canonical")
        return self


class FormulaPoolStateRow(RuntimeContractModel):
    status_key: Literal["current"] = "current"
    availability: Literal["empty", "ready"]
    pool_count: int = Field(ge=0, le=MAX_FORMULA_POOL_DEFINITIONS, strict=True)
    run_count: int = Field(ge=0, le=MAX_FORMULA_POOL_DEFINITIONS, strict=True)

    @model_validator(mode="after")
    def validate_state(self) -> FormulaPoolStateRow:
        if self.run_count > self.pool_count or (self.availability == "empty") != (
            self.pool_count == 0
        ):
            raise ValueError("formula pool state counts disagree")
        return self


class FormulaPoolDefinitionRow(RuntimeContractModel):
    pool_name: str = Field(min_length=6, max_length=85)
    display_name: str = Field(min_length=1, max_length=80)
    formula: str = Field(min_length=1, max_length=4096)
    syntax_version: Literal["tdx-v1"]
    version: str = Field(pattern=r"^[0-9a-f]{64}$")
    command_id: str = Field(min_length=1, max_length=128)
    command_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: AwareUtcDatetime
    creation_task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    creation_result_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    creation_trade_date: date

    @model_validator(mode="after")
    def validate_name(self) -> FormulaPoolDefinitionRow:
        if (
            not self.pool_name.startswith("user/")
            or _NAME.fullmatch(self.pool_name.removeprefix("user/")) is None
        ):
            raise ValueError("formula pool definition name is invalid")
        return self


class FormulaPoolLatestResultRow(RuntimeContractModel):
    pool_name: str = Field(min_length=6, max_length=85)
    definition_version: str = Field(pattern=r"^[0-9a-f]{64}$")
    trade_date: date
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    universe_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    projection_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    market_total: int = Field(ge=0, strict=True)
    match_count: int = Field(ge=0, strict=True)
    no_match_count: int = Field(ge=0, strict=True)
    unknown_count: int = Field(ge=0, strict=True)
    unknown_reasons_json: str
    member_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    relative_path: str
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    byte_count: int = Field(gt=0, le=2 * 1024 * 1024, strict=True)

    @model_validator(mode="after")
    def validate_index(self) -> FormulaPoolLatestResultRow:
        if not self.pool_name.startswith("user/"):
            raise ValueError("formula pool result name is invalid")
        base_name = self.pool_name.removeprefix("user/")
        if _NAME.fullmatch(base_name) is None or self.relative_path != (
            f"{base_name}/{self.trade_date.isoformat()}.json"
        ):
            raise ValueError("formula pool result path is not controlled")
        if self.market_total != self.match_count + self.no_match_count + self.unknown_count:
            raise ValueError("formula pool result counts disagree")
        reasons = strict_canonical_json_loads(self.unknown_reasons_json.encode("utf-8"))
        if (
            not isinstance(reasons, dict)
            or canonical_json_bytes(reasons).decode("utf-8") != self.unknown_reasons_json
            or any(
                not isinstance(key, str) or type(value) is not int or value < 0
                for key, value in reasons.items()
            )
            or sum(reasons.values()) != self.unknown_count
        ):
            raise ValueError("formula pool unknown reasons disagree")
        return self


def validate_formula_pool_projections(
    projections: Mapping[str, ServingProjectionPayload | ServingProjectionInput],
) -> None:
    present = FORMULA_POOL_PROJECTION_TABLES & projections.keys()
    if not present:
        return
    if present != FORMULA_POOL_PROJECTION_TABLES:
        raise ValueError("formula pool Serving projections are incomplete")
    selected = {name: projections[name] for name in FORMULA_POOL_PROJECTION_TABLES}
    bound = tuple(item for item in selected.values() if isinstance(item, ServingProjectionInput))
    if bound and (
        len(bound) != 3
        or len({item.owner_generation_id for item in bound}) != 1
        or {item.owner_dataset_id for item in bound} != {"signals"}
    ):
        raise ValueError("formula pool projections mix Serving generations")
    if len({item.available_at for item in selected.values()}) != 1:
        raise ValueError("formula pool projections have different availability")
    states = selected["formula_pool_state"].rows
    if len(states) != 1:
        raise ValueError("formula pool state is missing")
    state = FormulaPoolStateRow.model_validate(dict(states[0]))
    definitions = tuple(
        FormulaPoolDefinitionRow.model_validate(dict(row))
        for row in selected["formula_pool_definition"].rows
    )
    latest = tuple(
        FormulaPoolLatestResultRow.model_validate(dict(row))
        for row in selected["formula_pool_latest_result"].rows
    )
    definition_by_name = {row.pool_name: row for row in definitions}
    if (
        len(definition_by_name) != len(definitions)
        or len(definitions) != state.pool_count
        or len(latest) != state.run_count
        or len({row.pool_name for row in latest}) != len(latest)
    ):
        raise ValueError("formula pool Serving counts or names disagree")
    available = selected["formula_pool_state"].available_at
    for definition in definitions:
        if (
            definition.created_at > available
            or definition.creation_trade_date > available.astimezone(_SHANGHAI).date()
        ):
            raise ValueError("formula pool definition is newer than Serving availability")
    for result in latest:
        definition = definition_by_name.get(result.pool_name)
        if (
            definition is None
            or result.definition_version != definition.version
            or result.trade_date > available.astimezone(_SHANGHAI).date()
        ):
            raise ValueError("formula pool latest result mismatches its definition or date")


def _list_private(path: Path, *, limit: int) -> tuple[str, ...]:
    directory = _open_private_directory(path, create=False)
    try:
        before = _file_identity(os.fstat(directory))
        names: list[str] = []
        with os.scandir(directory) as entries:
            for entry in entries:
                if len(names) >= limit:
                    raise ValueError("formula pool directory exceeds read bound")
                names.append(entry.name)
        if _file_identity(os.fstat(directory)) != before or _file_identity(path.lstat()) != before:
            raise ValueError("formula pool directory changed while listing")
        return tuple(sorted(names))
    finally:
        os.close(directory)


def _verify_creation(
    definition: FormulaPoolDefinitionV1,
    mutation: PoolMutation,
    task: FormulaMarketVerifiedTask,
    config: FormulaPoolServingConfig,
) -> None:
    request, result, creation = task.request, task.result, definition.creation
    if (
        mutation.command_kind != "save_formula_pool_v1"
        or mutation.command_id != definition.command_id
        or mutation.command_hash != definition.command_hash
        or mutation.payload.get("base_name") != definition.pool_name.removeprefix("user/")
        or mutation.payload.get("display_name", "").strip() != definition.display_name
        or mutation.payload.get("task_id") != creation.task_id
        or mutation.payload.get("expected_version") is not None
        or mutation.result != {"pool_name": definition.pool_name, "version": definition.version}
        or request.formula != definition.formula
        or request.universe_root != config.universe_root
        or request.projection_root != config.projection_root
        or request.trade_date != creation.trade_date
        or request.decision_at != creation.decision_at
        or task.receipt.task_id != creation.task_id
        or result.request_sha256 != creation.request_sha256
        or result.content_sha256 != creation.result_sha256
        or result.formula_sha256 != creation.formula_sha256
        or result.formula_sha256 != hashlib.sha256(definition.formula.encode()).hexdigest()
        or request.expected_universe_sha256 != creation.universe_identity
        or request.expected_projection_identity != creation.projection_identity
        or definition.syntax_version != SYNTAX_VERSION
    ):
        raise ValueError("formula pool definition lacks matching audited creation evidence")


def _verify_daily(
    definition: FormulaPoolDefinitionV1,
    daily: FormulaPoolDailyResultV1,
    task: FormulaMarketVerifiedTask,
    config: FormulaPoolServingConfig,
) -> None:
    request = task.request
    if (
        request.idempotency_key
        != _run_identity(
            definition.pool_name,
            definition.version,
            daily.trade_date,
            daily.universe_identity,
            daily.projection_identity,
        )
        or request.formula != definition.formula
        or request.trade_date != daily.trade_date
        or request.universe_root != config.universe_root
        or request.projection_root != config.projection_root
        or request.expected_universe_sha256 != daily.universe_identity
        or request.expected_projection_identity != daily.projection_identity
        or FormulaPoolDailyResultV1.create(
            definition=definition, request=request, result=task.result
        )
        != daily
    ):
        raise ValueError("formula pool daily result disagrees with its sealed task")


def _index(daily: FormulaPoolDailyResultV1) -> FormulaPoolLatestResultRow:
    return FormulaPoolLatestResultRow(
        pool_name=daily.pool_name,
        definition_version=daily.definition_version,
        trade_date=daily.trade_date,
        task_id=daily.task_id,
        request_sha256=daily.request_sha256,
        result_sha256=daily.result_sha256,
        universe_identity=daily.universe_identity,
        projection_identity=daily.projection_identity,
        market_total=daily.market_total,
        match_count=daily.match_count,
        no_match_count=daily.no_match_count,
        unknown_count=daily.unknown_count,
        unknown_reasons_json=canonical_json_bytes(daily.unknown_reasons).decode("utf-8"),
        member_sha256=daily.member_sha256,
        relative_path=f"{daily.pool_name.removeprefix('user/')}/{daily.trade_date.isoformat()}.json",
        content_sha256=daily.content_sha256,
        byte_count=len(canonical_json_bytes(daily.model_dump(mode="json"))),
    )


def read_formula_pool_projections(
    config: FormulaPoolServingConfig,
    saves: Mapping[str, PoolMutation],
    *,
    observed_at: datetime,
) -> tuple[ServingProjectionPayload, ...]:
    """Read only configured authority; a missing configured catalog never means empty."""
    config = FormulaPoolServingConfig.model_validate(config)
    observed = normalize_aware_utc(observed_at)
    definition_names = _list_private(config.definition_root, limit=MAX_FORMULA_POOL_DEFINITIONS)
    daily_names = _list_private(config.daily_root, limit=MAX_FORMULA_POOL_DEFINITIONS)
    definitions_store = FormulaPoolDefinitionStore(
        definition_root=config.definition_root, rule_pool_root=config.rule_root
    )
    daily_store = FormulaPoolDailyResultStore(config.daily_root)
    definitions: dict[str, FormulaPoolDefinitionV1] = {}
    daily: dict[str, FormulaPoolDailyResultV1] = {}
    for name in definition_names:
        if not name.endswith(".json") or _NAME.fullmatch(name[:-5]) is None:
            raise ValueError("formula pool definition catalog has an unexpected file")
        base_name = name[:-5]
        definitions_store._reject_rule_name(base_name)
        definitions[base_name] = definitions_store.read(base_name)
    if set(definitions) != set(saves):
        raise ValueError("formula pool definitions disagree with audited saves")
    if not set(daily_names).issubset(definitions):
        raise ValueError("formula pool daily catalog contains an unknown pool")
    for base_name in daily_names:
        names = _list_private(config.daily_root / base_name, limit=_MAX_DAILY_DAYS)
        dates: list[date] = []
        for name in names:
            if not name.endswith(".json"):
                raise ValueError("formula pool daily catalog has an unexpected file")
            trade_date = date.fromisoformat(name[:-5])
            if name != f"{trade_date.isoformat()}.json":
                raise ValueError("formula pool daily filename is invalid")
            dates.append(trade_date)
        if dates:
            daily[base_name] = daily_store.read(definitions[base_name], max(dates))
    task_ids = {item.creation.task_id for item in definitions.values()}
    task_ids.update(item.task_id for item in daily.values())
    tasks = (
        read_formula_market_tasks_exact(
            config.task_state_path, config.artifact_root, task_ids, observed_at=observed
        )
        if task_ids
        else {}
    )
    definition_rows: list[FormulaPoolDefinitionRow] = []
    latest_rows: list[FormulaPoolLatestResultRow] = []
    for base_name, definition in sorted(definitions.items()):
        _verify_creation(definition, saves[base_name], tasks[definition.creation.task_id], config)
        if definition.created_at > observed:
            raise ValueError("formula pool definition is newer than observation")
        definition_rows.append(
            FormulaPoolDefinitionRow(
                pool_name=definition.pool_name,
                display_name=definition.display_name,
                formula=definition.formula,
                syntax_version=definition.syntax_version,
                version=definition.version,
                command_id=definition.command_id,
                command_hash=definition.command_hash,
                created_at=definition.created_at,
                creation_task_id=definition.creation.task_id,
                creation_result_sha256=definition.creation.result_sha256,
                creation_trade_date=definition.creation.trade_date,
            )
        )
        recent = daily.get(base_name)
        if recent is not None:
            _verify_daily(definition, recent, tasks[recent.task_id], config)
            latest_rows.append(_index(recent))
    projections = (
        ServingProjectionPayload(
            table_name="formula_pool_state",
            available_at=observed,
            rows=(
                FormulaPoolStateRow(
                    availability="ready" if definitions else "empty",
                    pool_count=len(definitions),
                    run_count=len(latest_rows),
                ).model_dump(mode="json"),
            ),
        ),
        ServingProjectionPayload(
            table_name="formula_pool_definition",
            available_at=observed,
            rows=tuple(row.model_dump(mode="json") for row in definition_rows),
        ),
        ServingProjectionPayload(
            table_name="formula_pool_latest_result",
            available_at=observed,
            rows=tuple(row.model_dump(mode="json") for row in latest_rows),
        ),
    )
    validate_formula_pool_projections({item.table_name: item for item in projections})
    return projections


def read_formula_pool_indexed_result(
    config: FormulaPoolServingConfig,
    index: FormulaPoolLatestResultRow | Mapping[str, object],
) -> FormulaPoolDailyResultV1:
    """Reopen a controlled result using its exact version and sealed task, never a supplied path."""
    config = FormulaPoolServingConfig.model_validate(config)
    index = FormulaPoolLatestResultRow.model_validate(index)
    base_name = index.pool_name.removeprefix("user/")
    definition = FormulaPoolDefinitionStore(
        definition_root=config.definition_root, rule_pool_root=config.rule_root
    ).read(base_name, expected_version=index.definition_version)
    daily = FormulaPoolDailyResultStore(config.daily_root).read(definition, index.trade_date)
    task = read_formula_market_tasks_exact(
        config.task_state_path,
        config.artifact_root,
        {daily.task_id},
        observed_at=datetime.now(UTC),
    )[daily.task_id]
    _verify_daily(definition, daily, task, config)
    if _index(daily) != index:
        raise ValueError("formula pool Serving index disagrees with controlled result")
    return daily


def read_formula_pool_serving_group(
    reader: ServingReader,
) -> Mapping[str, ServingProjectionInput] | None:
    """Read the trio and its availability from one verified Serving generation."""
    with reader.acquire_generation() as lease:
        manifest = lease.manifest
        names = FORMULA_POOL_PROJECTION_TABLES & manifest.row_counts.keys()
        if not names:
            if "projection_status" in manifest.row_counts:
                status_count = lease.connection.execute(
                    """SELECT count(*) FROM projection_status WHERE table_name IN (
                        'formula_pool_state', 'formula_pool_definition',
                        'formula_pool_latest_result'
                    )"""
                ).fetchone()[0]
                if status_count:
                    raise ValueError("formula pool status exists without its physical tables")
            return None  # Old generation, before the optional formula-pool contract.
        if (
            names != FORMULA_POOL_PROJECTION_TABLES
            or "projection_status" not in manifest.row_counts
        ):
            raise ValueError("formula pool Serving generation lacks its complete status group")
        statuses = lease.connection.execute(
            """SELECT table_name, available, owner_dataset_id, owner_generation_id,
                      available_at, row_count
               FROM projection_status
               WHERE table_name IN (
                   'formula_pool_state', 'formula_pool_definition', 'formula_pool_latest_result'
               )"""
        ).fetchall()
        if len(statuses) != 3 or {row[0] for row in statuses} != FORMULA_POOL_PROJECTION_TABLES:
            raise ValueError("formula pool Serving status is incomplete")
        if not any(row[1] for row in statuses):
            if any(
                manifest.row_counts[row[0]] != 0
                or row[2] != "signals"
                or row[3] is not None
                or row[4] is not None
                or row[5] != 0
                for row in statuses
            ):
                raise ValueError("unavailable formula pool Serving group contains rows")
            return None
        if not all(row[1] for row in statuses):
            raise ValueError("formula pool Serving status is partially available")
        expected_generation = manifest.source_generations.get("signals")
        projections: dict[str, ServingProjectionInput] = {}
        for name, _available, owner, generation, available_at, row_count in statuses:
            if (
                owner != "signals"
                or expected_generation is None
                or generation != expected_generation
                or row_count != manifest.row_counts[name]
                or row_count > PAGE_PROJECTION_CONTRACTS[name].max_rows
                or available_at is None
            ):
                raise ValueError("formula pool Serving status differs from its generation")
            cursor = lease.connection.execute(
                f"SELECT * FROM {quote_serving_table_identifier(name)}"
            )
            columns = tuple(column[0] for column in cursor.description)
            rows = tuple(
                {
                    column: value.isoformat() if isinstance(value, (date, datetime)) else value
                    for column, value in zip(columns, values, strict=True)
                }
                for values in cursor.fetchall()
            )
            projections[name] = ServingProjectionInput(
                table_name=name,
                available_at=available_at,
                rows=rows,
                owner_dataset_id=owner,
                owner_generation_id=generation,
            )
        validate_formula_pool_projections(projections)
        return projections
