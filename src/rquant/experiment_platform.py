"""Formal experiment facts in the original registry, with bounded C6 inputs."""

from __future__ import annotations

import calendar as month_calendar
import itertools
import json
import random
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal, Self
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from pydantic import Field, TypeAdapter, field_validator, model_validator

from rquant.backtest.contracts import SSECalendar
from rquant.experiment_platform_template_models import (
    ExperimentTemplateBaseline,
    ExperimentTemplatePublication,
    ExperimentTemplateSelection,
    ExperimentTemplateSlot,
    PreparedExperimentTemplate,
)
from rquant.experiment_registry import (
    DateRange,
    ExperimentRegistry,
    ExperimentStatus,
    ExperimentSubmissionIntent,
    FormalExperimentPlan,
    HypothesisFamilyManifest,
    _json_payload,
    _utc_iso,
)
from rquant.portfolio_backtest_models import PortfolioBacktestConfig, PortfolioSourceManifest
from rquant.portfolio_backtest_source import (
    PortfolioExperimentProtocol,
    PreparedPortfolioRequest,
    PublishedPortfolioInput,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strategy_authoring_commands import SaveStrategyTemplate, StrategyTemplateReceipt
from rquant.strategy_template import StrategyTemplate
from rquant.minute_backtest_contracts import MinuteReplayExecutionProfile
from rquant.minute_backtest_formal import MinuteExperimentProtocol, PreparedMinuteRequest
from rquant.minute_backtest_producer import PublishedMinuteInput
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.strategy_promotion_contracts import NativeMinuteConfiguration, NativeMinuteSelection

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Owner = Annotated[str, Field(pattern=r"^[A-Za-z0-9._@-]{1,64}$")]
Parameter = Literal[
    "weight_rule.max_positions",
    "weight_rule.max_stock_weight",
    "weight_rule.cash_reserve",
    "weight_rule.min_target_amount",
    "rebalance_rule.every_n_days",
]
MAX_SEARCH_ATTEMPTS = 64
MAX_RANDOM_SPACE = 4096
MAX_FAMILY_INPUT_BYTES = 512 * 1024 * 1024
PRIVATE_FAMILY_PREFIXES = ("experiment-search:", "experiment-outer:")
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class SearchDimension(RuntimeContractModel):
    parameter: Parameter
    values: tuple[Decimal, ...] = Field(min_length=1, max_length=16)

    @field_validator("values", mode="before")
    @classmethod
    def strict_values(cls, value: object) -> object:
        if not isinstance(value, (tuple, list)) or any(isinstance(v, bool) for v in value):
            raise ValueError("search values must be explicit finite numbers")
        try:
            numbers = tuple(Decimal(str(v)) for v in value)
        except (InvalidOperation, ValueError) as error:
            raise ValueError("search values must be explicit finite numbers") from error
        if any(not v.is_finite() for v in numbers) or len(set(numbers)) != len(numbers):
            raise ValueError("search values must be unique and finite")
        if numbers != tuple(sorted(numbers)):
            raise ValueError("search values must be in increasing order")
        return numbers

    @model_validator(mode="after")
    def parameter_bounds(self) -> Self:
        for value in self.values:
            if self.parameter in ("weight_rule.max_positions", "rebalance_rule.every_n_days"):
                maximum = 500 if self.parameter == "weight_rule.max_positions" else 252
                if value != value.to_integral_value() or not 1 <= value <= maximum:
                    raise ValueError("integer search value is outside its bounds")
            elif self.parameter == "weight_rule.min_target_amount":
                if not 0 <= value <= Decimal("1000000000000") or value.as_tuple().exponent < -2:
                    raise ValueError("minimum amount must be bounded and exact to a cent")
            elif not 0 <= value <= 1 or (
                self.parameter == "weight_rule.max_stock_weight" and value == 0
            ):
                raise ValueError("weight search value is outside its bounds")
        return self


class ExperimentSearchRequest(RuntimeContractModel):
    name: str = Field(min_length=1, max_length=60)
    template: ExperimentTemplateSelection | None = None
    base_config: PortfolioBacktestConfig
    protocol: PortfolioExperimentProtocol
    dimensions: tuple[SearchDimension, ...] = Field(min_length=1, max_length=5)
    method: Literal["grid", "random"] = "grid"
    random_count: int = Field(default=1, strict=True, ge=1, le=64)
    seed: int = Field(default=0, strict=True, ge=0, le=2**32 - 1)
    confidence: Decimal = Field(
        default=Decimal("0.95"), gt=Decimal("0.5"), lt=1, allow_inf_nan=False
    )
    target_period_sharpe: Decimal = Field(default=Decimal("0"), allow_inf_nan=False)
    pbo_slices: Literal[4, 6, 8, 10] = 4

    @model_validator(mode="after")
    def fixed_ranges_and_dimensions(self) -> Self:
        ranges = self.protocol
        if not (
            ranges.train_range.end_date < ranges.validation_range.start_date
            and ranges.validation_range.end_date < ranges.frozen_outer_test_range.start_date
        ):
            raise ValueError("experiment ranges overlap or are out of order")
        names = tuple(d.parameter for d in self.dimensions)
        if len(set(names)) != len(names):
            raise ValueError("search dimensions repeat")
        if (
            "rebalance_rule.every_n_days" in names
            and self.base_config.rebalance_rule.kind != "every_n"
        ):
            raise ValueError("every_n_days requires the every_n rebalance rule")
        if len(self.model_dump_json().encode()) > 32 * 1024:
            raise ValueError("search request exceeds byte budget")
        return self


class NativeMinuteExperimentRequest(RuntimeContractModel):
    kind: Literal["native_minute_experiment"] = "native_minute_experiment"
    name: str = Field(min_length=1, max_length=60)
    configurations: tuple[NativeMinuteConfiguration, ...] = Field(min_length=1, max_length=64)
    protocol: MinuteExperimentProtocol
    seed: int = Field(default=0, strict=True, ge=0, le=2**32 - 1)
    confidence: Decimal = Field(default=Decimal("0.95"), gt=Decimal("0.5"), lt=1, allow_inf_nan=False)
    target_period_sharpe: Decimal = Field(default=Decimal("0"), allow_inf_nan=False)
    pbo_slices: Literal[4, 6, 8, 10] = 4
    walk_forward_plan_hash: Sha256 | None = None
    walk_forward_command_id: UUID | None = None

    @model_validator(mode="after")
    def fixed_complete_array(self) -> Self:
        if (self.walk_forward_plan_hash is None) != (self.walk_forward_command_id is None):
            raise ValueError("native WF requires its paired original UUID and reference plan")
        if len({c.selection.target.owner_id for c in self.configurations}) != 1:
            raise PermissionError("native family contains another owner")
        if len({c.selection.profile_hash for c in self.configurations}) != 1:
            raise ValueError("native family must use one fixed full execution profile")
        for cfg in self.configurations:
            if not self.protocol.train_range.start_date <= cfg.start_date <= cfg.end_date <= self.protocol.validation_range.end_date:
                raise ValueError("native family inputs exceed the protected train/validation phase")
            if self.walk_forward_plan_hash is None and (cfg.start_date, cfg.end_date) != (
                self.protocol.train_range.start_date, self.protocol.validation_range.end_date
            ):
                raise ValueError("native parent requires the complete train/validation interval")
        if len(self.model_dump_json().encode()) > 32 * 1024:
            raise ValueError("native family request exceeds byte budget")
        return self

    @property
    def base_config(self) -> NativeMinuteConfiguration:
        return self.configurations[0]

    @property
    def template(self) -> None:
        return None


ExperimentConfiguration = PortfolioBacktestConfig | NativeMinuteConfiguration
ExperimentFamilyRequest = ExperimentSearchRequest | NativeMinuteExperimentRequest


def enumerate_search(request: ExperimentFamilyRequest) -> tuple[ExperimentConfiguration, ...]:
    if isinstance(request, NativeMinuteExperimentRequest):
        return NativeMinuteExperimentRequest.model_validate(request.model_dump(mode="python")).configurations
    checked = ExperimentSearchRequest.model_validate(request.model_dump(mode="python"))
    dimensions = tuple(sorted(checked.dimensions, key=lambda d: d.parameter))
    size = 1
    for dimension in dimensions:
        size *= len(dimension.values)
    maximum = 64 if checked.method == "grid" else MAX_RANDOM_SPACE
    if size > maximum:
        raise ValueError(f"search space exceeds {maximum} configurations")
    combinations = tuple(itertools.product(*(d.values for d in dimensions)))
    results: list[PortfolioBacktestConfig] = []
    for values in combinations:
        raw = checked.base_config.model_dump(mode="python")
        raw["start_date"] = checked.protocol.train_range.start_date
        raw["end_date"] = checked.protocol.validation_range.end_date
        for dimension, value in zip(dimensions, values, strict=True):
            section, field = dimension.parameter.split(".")
            raw[section][field] = (
                int(value) if field in ("max_positions", "every_n_days") else value
            )
        results.append(PortfolioBacktestConfig.model_validate(raw))
    if checked.method == "random":
        if checked.random_count > size:
            raise ValueError("random count exceeds complete search space")
        results = random.Random(checked.seed).sample(results, checked.random_count)
    return tuple(results)


def validate_experiment_dates(
    request: ExperimentFamilyRequest, *, calendar: tuple[date, ...], latest_complete: date
) -> None:
    if not calendar or tuple(sorted(set(calendar))) != calendar:
        raise ValueError("actual SSE calendar is missing or invalid")
    ranges = request.protocol
    for window in (ranges.train_range, ranges.validation_range, ranges.frozen_outer_test_range):
        if window.start_date not in calendar or window.end_date not in calendar:
            raise ValueError("experiment boundaries must be actual calendar dates")
        if window.end_date > latest_complete:
            raise ValueError("experiment dates exceed the complete source cutoff")
    search_dates = tuple(
        d
        for d in calendar
        if ranges.train_range.start_date <= d <= ranges.validation_range.end_date
    )
    covered = tuple(
        d
        for d in search_dates
        if d <= ranges.train_range.end_date or d >= ranges.validation_range.start_date
    )
    if search_dates != covered:
        raise ValueError("train and validation must form one continuous calendar replay")


class HoldoutPolicy(RuntimeContractModel):
    version: int = Field(strict=True, ge=1)
    months: int = Field(default=0, strict=True, ge=0, le=36)
    updated_at: AwareUtcDatetime


def month_cutoff(day: date, months: int) -> date:
    if isinstance(months, bool) or not isinstance(months, int) or not 0 <= months <= 36:
        raise ValueError("holdout months must be from zero through 36")
    index = day.year * 12 + day.month - 1 - months
    year, month = divmod(index, 12)
    return date(year, month + 1, min(day.day, month_calendar.monthrange(year, month + 1)[1]))


def holdout_cutoff(now: datetime, months: int, *, calendar: tuple[date, ...]) -> date:
    local = now.astimezone(_SHANGHAI)
    limit = month_cutoff(local.date(), months)
    complete = tuple(
        d
        for d in calendar
        if d <= limit and (d < local.date() or local.timetz().replace(tzinfo=None) >= time(15))
    )
    if not complete:
        raise ValueError("complete SSE calendar is unavailable")
    return max(complete)


class ExperimentFamilyRecord(RuntimeContractModel):
    contract: Literal["experiment-family/v1"] = "experiment-family/v1"
    owner: Owner
    request_id: UUID
    body_hash: Sha256
    family_id: str
    request: ExperimentFamilyRequest
    actual_configurations: tuple[ExperimentConfiguration, ...] = Field(min_length=1, max_length=64)
    enumeration_version: Literal["ordered-product-python-random-sample/v1"] = (
        "ordered-product-python-random-sample/v1"
    )
    registered_at: AwareUtcDatetime
    policy: HoldoutPolicy
    state: Literal["preparing", "ready", "cancelled"] = "preparing"
    phase: Literal["search", "outer"] = "search"
    parent_family_id: str | None = None
    template_baseline: ExperimentTemplateBaseline | None = None

    @model_validator(mode="after")
    def bind_template_baseline(self) -> Self:
        if isinstance(self.request, NativeMinuteExperimentRequest) and (
            any(not isinstance(c, NativeMinuteConfiguration) or c.selection.target.owner_id != self.owner for c in self.actual_configurations)
            or (self.phase == "search" and self.actual_configurations != self.request.configurations)
        ):
            raise PermissionError("native family differs from its exact owner or fixed array")
        selection, baseline = self.request.template, self.template_baseline
        if (selection is None) != (baseline is None):
            raise ValueError("template selection needs its original server baseline")
        if baseline is not None and (
            baseline.version.owner_id != self.owner
            or (baseline.version.strategy_id, baseline.version.head)
            != (selection.strategy_id, selection.head)
        ):
            raise PermissionError("template original baseline differs from its owner or head")
        return self


class ExperimentChildRegistration(RuntimeContractModel):
    config: ExperimentConfiguration
    plan: FormalExperimentPlan
    intent: ExperimentSubmissionIntent
    published: PublishedPortfolioInput | ExperimentTemplatePublication | PublishedMinuteInput


class ExperimentSourceProfile(RuntimeContractModel):
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(strict=True, ge=1)
    label: str = Field(min_length=1, max_length=60)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    sources: PortfolioSourceManifest
    calendar: SSECalendar
    coverage: DateRange
    latest_complete: date
    phase_slice_available: bool = Field(default=False, strict=True)
    source_identity: Sha256 | None = None

    @model_validator(mode="after")
    def bind_profile(self) -> Self:
        if self.latest_complete > self.coverage.end_date:
            raise ValueError("source completion exceeds actual coverage")
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"source_identity", "label"})
        )
        if self.source_identity is None:
            object.__setattr__(self, "source_identity", expected)
        elif self.source_identity != expected:
            raise ValueError("protected source identity differs")
        return self


class ExperimentPhaseRead(RuntimeContractModel):
    owner: Owner
    family_id: str
    source_identity: Sha256
    source_key: str
    source_version: int = Field(strict=True, ge=1)
    phase: Literal["search", "outer"]
    window: DateRange
    outer_grant_id: Sha256 | None = None

    @model_validator(mode="after")
    def exact_phase(self) -> Self:
        if (self.phase == "outer") != (self.outer_grant_id is not None):
            raise ValueError("outer phase requires its exact grant")
        return self


class NativeMinuteSourceProfile(RuntimeContractModel):
    selection: NativeMinuteSelection
    execution_profile: MinuteReplayExecutionProfile
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar: MarketCalendarAuthority
    coverage: DateRange
    latest_complete: date
    phase_slice_available: bool = Field(default=False, strict=True)
    source_identity: Sha256 | None = None

    @model_validator(mode="after")
    def bind_complete_profile(self) -> Self:
        if (self.selection.profile_hash, self.selection.target.cost_fingerprint) != (
            self.execution_profile.profile_hash, canonical_sha256(self.execution_profile.execution_costs)
        ):
            raise ValueError("native installed profile/cost differs from its fixed selection")
        if self.latest_complete > self.coverage.end_date:
            raise ValueError("native source completion exceeds actual coverage")
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"source_identity"}))
        if self.source_identity is None:
            object.__setattr__(self, "source_identity", expected)
        elif self.source_identity != expected:
            raise ValueError("native protected source identity differs")
        return self


class NativeMinutePhaseRead(ExperimentPhaseRead):
    index: int = Field(strict=True, ge=0, le=63)
    configuration: NativeMinuteConfiguration

    @property
    def publication_source_key(self) -> str:
        # Each actual private phase publication gets its own original catalog key.
        return "native-phase:" + canonical_sha256(self)


class ExperimentPreparationReceipt(RuntimeContractModel):
    family_id: str
    owner: Owner
    index: int = Field(strict=True, ge=0, le=63)
    source_identity: Sha256
    source_path: str = Field(max_length=1024)
    file_identity: tuple[int, int, int, int]
    file_sha256: Sha256
    prepared: PreparedPortfolioRequest | PreparedExperimentTemplate | PreparedMinuteRequest
    native_configuration: NativeMinuteConfiguration | None = None

    @model_validator(mode="after")
    def bind_native_preparation(self) -> Self:
        if isinstance(self.prepared, PreparedMinuteRequest):
            if self.native_configuration is None or self.native_configuration.selection.target.owner_id != self.owner:
                raise PermissionError("native preparation requires its exact owner/configuration")
            from rquant.experiment_platform_evidence import verify_native_preparation
            verify_native_preparation(self.prepared, self.native_configuration)
        elif self.native_configuration is not None:
            raise ValueError("native configuration cannot relabel a daily or template preparation")
        return self

    @property
    def configuration(self) -> ExperimentConfiguration:
        if isinstance(self.prepared, PreparedMinuteRequest):
            return self.native_configuration
        return (
            self.prepared.configuration
            if isinstance(self.prepared, PreparedExperimentTemplate)
            else self.prepared.frozen.config
        )


class ExperimentPreparationReservation(RuntimeContractModel):
    owner: Owner
    family_id: str
    index: int = Field(strict=True, ge=0, le=63)
    source_identity: Sha256
    source_path: str = Field(max_length=4096)
    input_hash: Sha256
    created_at: AwareUtcDatetime
    template_benchmark_closes: tuple[tuple[date, float], ...] | None = None


class ExperimentChildAdmission(RuntimeContractModel):
    owner: Owner
    family_id: str
    job_id: UUID
    request_id: UUID
    experiment_id: Sha256
    command_content_hash: Sha256
    publish_grant_seq: int | None = None
    cancel_seq: int | None = None
    cancel_state: Literal[
        "none", "before_publication", "pending", "confirmed", "already_completed", "failed"
    ] = "none"
    cancel_request_chain: tuple[UUID, ...] = ()
    cancel_job_version: int | None = None


def validate_experiment_publication_grant(
    child: ExperimentChildAdmission | None, intent: ExperimentSubmissionIntent
) -> None:
    if (
        child is None
        or child.publish_grant_seq is None
        or child.cancel_state == "before_publication"
        or (child.request_id, child.experiment_id, child.command_content_hash)
        != (intent.request_id, intent.experiment_id, intent.command_content_hash)
    ):
        raise PermissionError("formal publication has no exact persisted grant")


class ExperimentNote(RuntimeContractModel):
    family_id: str
    owner: Owner
    version: int = Field(strict=True, ge=0)
    text: str = Field(max_length=1024)
    updated_at: AwareUtcDatetime

    @field_validator("text")
    @classmethod
    def note_budget(cls, value: str) -> str:
        if len(value.encode()) > 4096:
            raise ValueError("note exceeds byte budget")
        return value


class ExperimentOuterGrant(RuntimeContractModel):
    grant_id: Sha256
    owner: Owner
    request_id: UUID
    family_id: str
    experiment_id: Sha256
    config: ExperimentConfiguration
    outer_range: DateRange
    policy: HoldoutPolicy
    admitted_at: AwareUtcDatetime
    source_key: str
    source_version: int
    body_hash: Sha256
    result_hash: Sha256
    source_identity: Sha256

    @model_validator(mode="after")
    def exact_native_selected_source(self) -> Self:
        if isinstance(self.config, NativeMinuteConfiguration) and (
            self.owner, self.source_key, self.source_version
        ) != (
            self.config.selection.target.owner_id,
            self.config.source_key,
            self.config.source_version,
        ):
            raise ValueError("native outer grant differs from the selected owner/source")
        return self


_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS experiment_template_slot(family_id TEXT NOT "
    "NULL,child_index INTEGER NOT NULL,payload_json TEXT NOT NULL,PRIMARY "
    "KEY(family_id,child_index));"
    "\nCREATE TABLE IF NOT EXISTS experiment_platform_metadata(key TEXT PRIMARY KEY,value "
    "TEXT NOT NULL);\nCREATE TABLE IF NOT EXISTS experiment_family_request(\n request_id "
    "TEXT PRIMARY KEY,owner TEXT NOT NULL,body_hash TEXT NOT NULL,\n family_id TEXT NOT "
    "NULL UNIQUE,state TEXT NOT NULL,payload_json TEXT NOT NULL);\nCREATE TABLE IF NOT "
    "EXISTS experiment_private_family(\n hypothesis_family TEXT PRIMARY KEY,owner TEXT "
    "NOT NULL,request_id TEXT NOT NULL,\n phase TEXT NOT NULL,parent_family_id "
    "TEXT);\nCREATE TABLE IF NOT EXISTS experiment_platform_operation(\n seq INTEGER "
    "PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,identity TEXT NOT NULL,at TEXT NOT "
    "NULL);\nCREATE TABLE IF NOT EXISTS experiment_child_admission(\n job_id TEXT PRIMARY "
    "KEY,experiment_id TEXT NOT NULL UNIQUE,hypothesis_family TEXT NOT NULL,\n owner TEXT "
    "NOT NULL,payload_json TEXT NOT NULL);\nCREATE TABLE IF NOT EXISTS "
    "experiment_outer_grant(\n grant_id TEXT PRIMARY KEY,owner TEXT NOT NULL,request_id "
    "TEXT NOT NULL UNIQUE,\n range_start TEXT NOT NULL,range_end TEXT NOT "
    "NULL,payload_json TEXT NOT NULL);\nCREATE INDEX IF NOT EXISTS "
    "experiment_outer_owner_range ON experiment_outer_grant(owner,range_start,range_end);"
    "\nCREATE TABLE IF NOT EXISTS experiment_note(\n family_id TEXT PRIMARY KEY,owner TEXT "
    "NOT NULL,version INTEGER NOT NULL,payload_json TEXT NOT NULL);\nCREATE TABLE IF NOT "
    "EXISTS experiment_platform_receipt(\n request_id TEXT PRIMARY KEY,owner TEXT NOT "
    "NULL,body_hash TEXT NOT NULL,payload_json TEXT NOT NULL);\nCREATE TABLE IF NOT "
    "EXISTS experiment_evidence(\n evidence_id TEXT PRIMARY KEY,owner TEXT NOT "
    "NULL,family_id TEXT NOT NULL,payload_json TEXT NOT NULL);\nCREATE TABLE IF NOT "
    "EXISTS experiment_evidence_head(\n family_id TEXT PRIMARY KEY,owner TEXT NOT "
    "NULL,evidence_id TEXT NOT NULL);\nCREATE TABLE IF NOT EXISTS "
    "experiment_prepared_child(\n family_id TEXT NOT NULL,child_index INTEGER NOT "
    "NULL,payload_json TEXT NOT NULL,\n PRIMARY KEY(family_id,child_index));\nCREATE TABLE "
    "IF NOT EXISTS experiment_preparation_reservation(\n family_id TEXT NOT "
    "NULL,child_index INTEGER NOT NULL,payload_json TEXT NOT NULL,\n PRIMARY "
    "KEY(family_id,child_index));\n"
)


class ExperimentPlatformStore:
    """No queue: these are admission facts beside the original attempt/outbox."""

    def __init__(
        self, registry: ExperimentRegistry, *, activate_private_schema: bool = False
    ) -> None:
        self.registry = registry
        if activate_private_schema:
            with registry._connect() as connection:
                connection.executescript("BEGIN IMMEDIATE;" + _SCHEMA)
                row = connection.execute(
                    "SELECT value FROM experiment_platform_metadata WHERE key='version'"
                ).fetchone()
                if row is not None and row[0] != "1":
                    connection.rollback()
                    raise ValueError("unknown private experiment schema")
                connection.execute(
                    "INSERT OR IGNORE INTO experiment_platform_metadata VALUES('version','1')"
                )
                connection.commit()
        with registry._connect() as connection:
            self._require_schema(connection)

    @staticmethod
    def _require_schema(connection: sqlite3.Connection) -> None:
        try:
            row = connection.execute(
                "SELECT value FROM experiment_platform_metadata WHERE key='version'"
            ).fetchone()
        except sqlite3.Error as error:
            raise ValueError("private experiment schema is not installed") from error
        if row is None or row[0] != "1":
            raise ValueError("unknown private experiment schema")

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.registry._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_schema(connection)
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _operation(connection: sqlite3.Connection, kind: str, identity: str, at: datetime) -> int:
        return int(
            connection.execute(
                "INSERT INTO experiment_platform_operation(kind,identity,at) VALUES(?,?,?)",
                (kind, identity, _utc_iso(at)),
            ).lastrowid
        )

    @staticmethod
    def _record(
        connection: sqlite3.Connection, owner: str, family_id: str
    ) -> ExperimentFamilyRecord:
        row = connection.execute(
            "SELECT owner,payload_json FROM experiment_family_request WHERE family_id=?",
            (family_id,),
        ).fetchone()
        if row is None or row["owner"] != owner:
            raise PermissionError("experiment family is unavailable")
        record = ExperimentFamilyRecord.model_validate_json(row["payload_json"])
        if record.owner != owner or record.family_id != family_id:
            raise ValueError("experiment family binding changed")
        return record

    def policy(self) -> HoldoutPolicy:
        with self.registry._connect() as connection:
            self._require_schema(connection)
            row = connection.execute(
                "SELECT value FROM experiment_platform_metadata WHERE key='holdout_policy'"
            ).fetchone()
        if row is None:
            raise ValueError("holdout policy is not installed")
        return HoldoutPolicy.model_validate_json(row[0])

    def install_policy(
        self, *, months: int, now: datetime, expected_version: int = 0
    ) -> HoldoutPolicy:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT value FROM experiment_platform_metadata WHERE key='holdout_policy'"
            ).fetchone()
            version = 0 if row is None else HoldoutPolicy.model_validate_json(row[0]).version
            if version != expected_version:
                raise ValueError("holdout policy version conflict")
            policy = HoldoutPolicy(version=version + 1, months=months, updated_at=now)
            connection.execute(
                "INSERT OR REPLACE INTO experiment_platform_metadata VALUES('holdout_policy',?)",
                (_json_payload(policy),),
            )
        return policy

    def begin_request(
        self,
        *,
        owner: str,
        request_id: UUID,
        body_hash: str,
        request: ExperimentFamilyRequest,
        registered_at: datetime,
        template_baseline: ExperimentTemplateBaseline | None = None,
    ) -> ExperimentFamilyRecord:
        if isinstance(request, NativeMinuteExperimentRequest) and request.walk_forward_command_id is not None:
            from rquant.strategy_promotion_walk_forward import NativeStrategyPromotionWalkForwardPlan

            plan = self.registry.walk_forward_plan_by_id(request.walk_forward_command_id, actor_id=owner)
            if (
                not isinstance(plan, NativeStrategyPromotionWalkForwardPlan)
                or request.walk_forward_command_id != request_id
                or plan.fingerprint != request.walk_forward_plan_hash
                or plan.family_request() != request
            ):
                raise ValueError("native WF differs from its immutable original reference plan")
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM experiment_family_request WHERE request_id=?", (str(request_id),)
            ).fetchone()
            if row is not None:
                if (row["owner"], row["body_hash"]) != (owner, body_hash):
                    raise ValueError("original request content conflicts")
                return ExperimentFamilyRecord.model_validate_json(row["payload_json"])
            policy_row = connection.execute(
                "SELECT value FROM experiment_platform_metadata WHERE key='holdout_policy'"
            ).fetchone()
            if policy_row is None:
                policy = HoldoutPolicy(version=1, months=0, updated_at=registered_at)
                connection.execute(
                    "INSERT INTO experiment_platform_metadata VALUES('holdout_policy',?)",
                    (_json_payload(policy),),
                )
            else:
                policy = HoldoutPolicy.model_validate_json(policy_row[0])
            record = ExperimentFamilyRecord(
                owner=owner,
                request_id=request_id,
                body_hash=body_hash,
                family_id="experiment-search:"
                + canonical_sha256({"owner": owner, "request": request_id}),
                request=request,
                actual_configurations=enumerate_search(request),
                registered_at=registered_at,
                policy=policy,
                template_baseline=template_baseline,
            )
            slots = (
                ()
                if template_baseline is None
                else tuple(
                    ExperimentTemplateSlot(
                        owner=owner,
                        family_id=record.family_id,
                        index=index,
                        baseline_hash=canonical_sha256(template_baseline),
                        request=SaveStrategyTemplate(
                            command_id=str(
                                uuid5(
                                    NAMESPACE_URL,
                                    f"template-save:{owner}:{record.family_id}:{index}",
                                )
                            ),
                            requested_at=registered_at,
                            generation_id=template_baseline.generation_id,
                            name=request.name,
                            change_note="实验参数",
                            rules=StrategyTemplate.model_validate(
                                template_baseline.version.rules.model_dump(mode="python")
                                | {
                                    "weight_rule": cfg.weight_rule,
                                    "rebalance_rule": cfg.rebalance_rule,
                                }
                            ),
                        ),
                    )
                    for index, cfg in enumerate(record.actual_configurations)
                )
            )
            connection.execute(
                "INSERT INTO experiment_family_request VALUES(?,?,?,?,?,?)",
                (
                    str(request_id),
                    owner,
                    body_hash,
                    record.family_id,
                    record.state,
                    _json_payload(record),
                ),
            )
            for slot in slots:
                connection.execute(
                    "INSERT INTO experiment_template_slot VALUES(?,?,?)",
                    (record.family_id, slot.index, _json_payload(slot)),
                )
        return record

    def template_slots(self, owner: str, family_id: str) -> tuple[ExperimentTemplateSlot, ...]:
        with self.registry._connect() as connection:
            record = self._record(connection, owner, family_id)
            rows = connection.execute(
                "SELECT payload_json FROM experiment_template_slot WHERE "
                "family_id=? ORDER BY child_index",
                (family_id,),
            ).fetchall()
        slots = tuple(ExperimentTemplateSlot.model_validate_json(row[0]) for row in rows)
        if record.template_baseline is None:
            if slots:
                raise ValueError("ordinary experiment cannot have template slots")
            return ()
        if len(slots) != len(record.actual_configurations) or any(
            (s.owner, s.family_id, s.index, s.baseline_hash)
            != (owner, family_id, i, canonical_sha256(record.template_baseline))
            for i, s in enumerate(slots)
        ):
            raise ValueError("complete original template slots differ")
        return slots

    def save_template_slot(
        self,
        original: ExperimentTemplateSlot,
        *,
        receipt: StrategyTemplateReceipt | None = None,
        failure: Literal["capacity", "source_changed", "invalid_definition"] | None = None,
    ) -> ExperimentTemplateSlot:
        with self.transaction() as connection:
            record = self._record(connection, original.owner, original.family_id)
            row = connection.execute(
                "SELECT payload_json FROM experiment_template_slot WHERE "
                "family_id=? AND child_index=?",
                (original.family_id, original.index),
            ).fetchone()
            current = None if row is None else ExperimentTemplateSlot.model_validate_json(row[0])
            if current is None or current.model_dump(
                exclude={"state", "receipt", "failure"}
            ) != original.model_dump(exclude={"state", "receipt", "failure"}):
                raise ValueError("original derived slot changed")
            if receipt is not None and (
                receipt.owner_id,
                receipt.command_id,
                receipt.original_request_hash,
                receipt.action,
                receipt.head.version,
            ) != (
                current.owner,
                current.request.command_id,
                current.request.request_hash,
                "save",
                1,
            ):
                raise ValueError("derived receipt differs from original save command")
            if current.state != "pending":
                if receipt == current.receipt and failure == current.failure:
                    return current
                raise ValueError("derived slot is immutable after completion")
            if record.state != "preparing":
                raise ValueError("template family no longer accepts preparation")
            updated = current.model_copy(
                update={
                    "state": "saved" if receipt is not None else "failed",
                    "receipt": receipt,
                    "failure": failure,
                }
            )
            if receipt is None and failure is None:
                raise ValueError("derived completion needs an original receipt or failure")
            connection.execute(
                "UPDATE experiment_template_slot SET payload_json=? WHERE "
                "family_id=? AND child_index=?",
                (_json_payload(updated), original.family_id, original.index),
            )
            return updated

    def get_request(self, owner: str, request_id: UUID) -> ExperimentFamilyRecord | None:
        with self.registry._connect() as connection:
            self._require_schema(connection)
            row = connection.execute(
                "SELECT owner,payload_json FROM experiment_family_request WHERE request_id=?",
                (str(request_id),),
            ).fetchone()
        if row is None:
            return None
        if row["owner"] != owner:
            raise PermissionError("experiment request is unavailable")
        return ExperimentFamilyRecord.model_validate_json(row["payload_json"])

    def get_family(self, owner: str, family_id: str) -> ExperimentFamilyRecord:
        with self.registry._connect() as connection:
            self._require_schema(connection)
            return self._record(connection, owner, family_id)

    def preparation(
        self, owner: str, family_id: str, index: int
    ) -> ExperimentPreparationReceipt | None:
        with self.registry._connect() as connection:
            self._record(connection, owner, family_id)
            row = connection.execute(
                (
                    "SELECT payload_json FROM experiment_prepared_child WHERE "
                    "family_id=? AND "
                    "child_index=?"
                ),
                (family_id, index),
            ).fetchone()
        return None if row is None else ExperimentPreparationReceipt.model_validate_json(row[0])

    def preparation_reservation(
        self, owner: str, family_id: str, index: int
    ) -> ExperimentPreparationReservation | None:
        with self.registry._connect() as connection:
            self._record(connection, owner, family_id)
            row = connection.execute(
                (
                    "SELECT payload_json FROM experiment_preparation_reservation WHERE "
                    "family_id=? AND "
                    "child_index=?"
                ),
                (family_id, index),
            ).fetchone()
        return None if row is None else ExperimentPreparationReservation.model_validate_json(row[0])

    def reserve_preparation(
        self, reservation: ExperimentPreparationReservation
    ) -> ExperimentPreparationReservation:
        checked = ExperimentPreparationReservation.model_validate(
            reservation.model_dump(mode="python")
        )
        with self.transaction() as connection:
            record = self._record(connection, checked.owner, checked.family_id)
            if record.state != "preparing" or checked.index >= len(record.actual_configurations):
                raise ValueError("preparation reservation has no preparing child")
            row = connection.execute(
                (
                    "SELECT payload_json FROM experiment_preparation_reservation WHERE "
                    "family_id=? AND "
                    "child_index=?"
                ),
                (checked.family_id, checked.index),
            ).fetchone()
            if row is not None:
                original = ExperimentPreparationReservation.model_validate_json(row[0])
                if original != checked:
                    raise ValueError("original preparation reservation is immutable")
                return original
            connection.execute(
                "INSERT INTO experiment_preparation_reservation VALUES(?,?,?)",
                (checked.family_id, checked.index, _json_payload(checked)),
            )
        return checked

    def save_preparation(self, receipt: ExperimentPreparationReceipt) -> None:
        checked = ExperimentPreparationReceipt.model_validate(receipt.model_dump(mode="python", exclude_computed_fields=isinstance(receipt.prepared, PreparedMinuteRequest)))
        with self.transaction() as connection:
            record = self._record(connection, checked.owner, checked.family_id)
            if (
                checked.index >= len(record.actual_configurations)
                or checked.configuration != record.actual_configurations[checked.index]
            ):
                raise ValueError("prepared child differs from fixed array")
            previous = connection.execute(
                (
                    "SELECT payload_json FROM experiment_prepared_child WHERE "
                    "family_id=? AND "
                    "child_index=?"
                ),
                (checked.family_id, checked.index),
            ).fetchone()
            raw = json.dumps(checked.model_dump(mode="json", exclude_computed_fields=True), ensure_ascii=True, separators=(",", ":"), sort_keys=True) if isinstance(checked.prepared, PreparedMinuteRequest) else _json_payload(checked)
            if previous is not None:
                if previous[0] != raw:
                    raise ValueError("original prepared child is immutable")
                return
            total = connection.execute(
                (
                    "SELECT COALESCE(sum(length(CAST(payload_json AS BLOB))),0) FROM "
                    "experiment_prepared_child WHERE "
                    "family_id=?"
                ),
                (checked.family_id,),
            ).fetchone()[0]
            if total + len(raw.encode()) > MAX_FAMILY_INPUT_BYTES:
                raise ValueError("complete family input exceeds 512 MiB")
            connection.execute(
                "INSERT INTO experiment_prepared_child VALUES(?,?,?)",
                (checked.family_id, checked.index, raw),
            )

    def set_policy(
        self,
        *,
        owner: str,
        request_id: UUID,
        months: int,
        expected_version: int,
        now: datetime,
        administrators: frozenset[str],
    ) -> HoldoutPolicy:
        if owner not in administrators:
            raise PermissionError("holdout policy requires the configured administrator")
        body = canonical_sha256({"kind": "policy", "months": months, "version": expected_version})
        with self.transaction() as connection:
            old = connection.execute(
                "SELECT * FROM experiment_platform_receipt WHERE request_id=?", (str(request_id),)
            ).fetchone()
            if old is not None:
                if (old["owner"], old["body_hash"]) != (owner, body):
                    raise ValueError("original policy request content conflicts")
                return HoldoutPolicy.model_validate_json(old["payload_json"])
            row = connection.execute(
                "SELECT value FROM experiment_platform_metadata WHERE key='holdout_policy'"
            ).fetchone()
            version = 0 if row is None else HoldoutPolicy.model_validate_json(row[0]).version
            if version != expected_version:
                raise ValueError("holdout policy version conflict")
            policy = HoldoutPolicy(version=version + 1, months=months, updated_at=now)
            connection.execute(
                "INSERT OR REPLACE INTO experiment_platform_metadata VALUES('holdout_policy',?)",
                (_json_payload(policy),),
            )
            connection.execute(
                "INSERT INTO experiment_platform_receipt VALUES(?,?,?,?)",
                (str(request_id), owner, body, _json_payload(policy)),
            )
        return policy

    def begin_outer_request(self, grant: ExperimentOuterGrant) -> ExperimentFamilyRecord:
        with self.transaction() as connection:
            actual = connection.execute(
                "SELECT payload_json FROM experiment_outer_grant WHERE grant_id=?",
                (grant.grant_id,),
            ).fetchone()
            if actual is None or ExperimentOuterGrant.model_validate_json(actual[0]) != grant:
                raise PermissionError("outer preparation requires the actual persisted grant")
            parent = self._record(connection, grant.owner, grant.family_id)
            cfg = type(grant.config).model_validate(
                grant.config.model_dump(mode="python")
                | {
                    "start_date": grant.outer_range.start_date,
                    "end_date": grant.outer_range.end_date,
                }
            )
            record = ExperimentFamilyRecord(
                owner=grant.owner,
                request_id=grant.request_id,
                body_hash=canonical_sha256(grant),
                family_id="experiment-outer:" + grant.grant_id,
                request=parent.request,
                actual_configurations=(cfg,),
                registered_at=grant.admitted_at,
                policy=grant.policy,
                phase="outer",
                parent_family_id=grant.family_id,
                template_baseline=parent.template_baseline,
            )
            old = connection.execute(
                "SELECT payload_json FROM experiment_family_request WHERE request_id=?",
                (str(grant.request_id),),
            ).fetchone()
            if old is not None:
                found = ExperimentFamilyRecord.model_validate_json(old[0])
                if found.model_dump(exclude={"state"}) != record.model_dump(exclude={"state"}):
                    raise ValueError("original outer preparation changed")
                return found
            connection.execute(
                "INSERT INTO experiment_family_request VALUES(?,?,?,?,?,?)",
                (
                    str(record.request_id),
                    record.owner,
                    record.body_hash,
                    record.family_id,
                    record.state,
                    _json_payload(record),
                ),
            )
            if parent.template_baseline is not None:
                selected = connection.execute(
                    "SELECT s.payload_json FROM experiment_template_slot s JOIN "
                    "experiment_prepared_child p ON p.family_id=s.family_id AND "
                    "p.child_index=s.child_index WHERE s.family_id=? AND "
                    "json_extract(p.payload_json,'$.prepared.formal_plan.spec.experiment_id')=?",
                    (parent.family_id, grant.experiment_id),
                ).fetchall()
                if len(selected) != 1:
                    raise ValueError("outer grant has no exact saved parent template slot")
                slot = ExperimentTemplateSlot.model_validate_json(selected[0][0])
                if slot.state != "saved" or slot.receipt is None:
                    raise ValueError("outer grant requires an originally saved template version")
                inherited = slot.model_copy(update={"family_id": record.family_id, "index": 0})
                connection.execute(
                    "INSERT INTO experiment_template_slot VALUES(?,?,?)",
                    (record.family_id, 0, _json_payload(inherited)),
                )
        return record

    def register_family_submission(
        self,
        *,
        owner: str,
        request_id: UUID,
        children: tuple[ExperimentChildRegistration, ...],
        fault: Callable[[str], None] | None = None,
    ) -> ExperimentFamilyRecord:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT family_id FROM experiment_family_request WHERE request_id=?",
                (str(request_id),),
            ).fetchone()
            if row is None:
                raise ValueError("family request is not prepared")
            record = self._record(connection, owner, row[0])
            if record.state == "cancelled":
                raise ValueError("original preparing family was cancelled")
            if tuple(child.config for child in children) != record.actual_configurations:
                raise ValueError("complete fixed configuration array differs")
            if not children or len({c.plan.spec.experiment_id for c in children}) != len(children):
                raise ValueError("formal children repeat or are missing")
            protocols = record.request.protocol
            for index, child in enumerate(children):
                spec = child.plan.spec
                if (
                    spec.hypothesis_family,
                    spec.train_range,
                    spec.validation_range,
                    spec.frozen_outer_test_range,
                    spec.seed,
                ) != (
                    record.family_id,
                    protocols.train_range,
                    protocols.validation_range,
                    protocols.frozen_outer_test_range,
                    record.request.seed,
                ):
                    raise ValueError("child formal identity differs from its family")
                if (
                    (not isinstance(child.published, PublishedMinuteInput) and child.config.config_hash != child.published.config_hash)
                    or spec.dataset_snapshot_id != child.published.identity.snapshot_id
                ):
                    raise ValueError("child actual input binding differs")
                prepared_row = connection.execute(
                    (
                        "SELECT payload_json FROM experiment_prepared_child WHERE "
                        "family_id=? AND "
                        "child_index=?"
                    ),
                    (record.family_id, index),
                ).fetchone()
                if prepared_row is None:
                    raise ValueError("formal child has no original preparation")
                prepared = ExperimentPreparationReceipt.model_validate_json(prepared_row[0])
                expected_job = experiment_family_job(record, index)
                from rquant.lab_job_center import LabCommandSubmissionFacade
                from rquant.lab_job_protocol import LabCommandEnvelope

                expected_envelope = LabCommandEnvelope(
                    request_id=LabCommandSubmissionFacade._request_id(
                        stable_experiment_interaction(owner, request_id, index)
                    ),
                    command=prepared.prepared.submission(job_id=expected_job).command,
                )
                if (
                    prepared.owner,
                    prepared.index,
                    prepared.family_id,
                    prepared.prepared.formal_plan,
                    prepared.prepared.published,
                ) != (
                    owner,
                    index,
                    record.family_id,
                    child.plan,
                    child.published,
                ) or child.intent != LabCommandSubmissionFacade._experiment_submission_intent(
                    expected_envelope
                ):
                    raise ValueError("formal child differs from the exact prepared command")
            manifest = HypothesisFamilyManifest(
                hypothesis_family=record.family_id,
                experiment_ids=tuple(sorted(c.plan.spec.experiment_id for c in children)),
                search_space_fingerprint=canonical_sha256(record.actual_configurations),
                metric_definition_fingerprint=children[0].plan.spec.metric_definition_fingerprint,
                preregistered_at=record.registered_at,
            )
            self.registry._insert_hypothesis_family(connection, manifest)
            connection.execute(
                "INSERT OR IGNORE INTO experiment_private_family VALUES(?,?,?,?,?)",
                (record.family_id, owner, str(request_id), record.phase, record.parent_family_id),
            )
            for child in children:
                self.registry._insert_formal_plan(connection, child.plan, manifest)
                self.registry._insert_attempt(
                    connection,
                    child.plan.spec,
                    registered_at=child.plan.preregistered_at if isinstance(child.published, PublishedMinuteInput) else record.registered_at,
                    submission=child.intent,
                )
                key = "config:" + child.intent.experiment_id
                config_row = connection.execute(
                    "SELECT value FROM experiment_platform_metadata WHERE key=?", (key,)
                ).fetchone()
                if config_row is not None and config_row[0] != _json_payload(child.config):
                    raise ValueError("actual child configuration is immutable")
                connection.execute(
                    "INSERT OR IGNORE INTO experiment_platform_metadata VALUES(?,?)",
                    (key, _json_payload(child.config)),
                )
                admission = ExperimentChildAdmission(
                    owner=owner,
                    family_id=record.family_id,
                    job_id=child.intent.job_id,
                    request_id=child.intent.request_id,
                    experiment_id=child.intent.experiment_id,
                    command_content_hash=child.intent.command_content_hash,
                )
                old = connection.execute(
                    "SELECT payload_json FROM experiment_child_admission WHERE job_id=?",
                    (str(admission.job_id),),
                ).fetchone()
                if old is None:
                    connection.execute(
                        "INSERT INTO experiment_child_admission VALUES(?,?,?,?,?)",
                        (
                            str(admission.job_id),
                            admission.experiment_id,
                            admission.family_id,
                            owner,
                            _json_payload(admission),
                        ),
                    )
                else:
                    existing = ExperimentChildAdmission.model_validate_json(old[0])
                    if (
                        existing.owner,
                        existing.family_id,
                        existing.request_id,
                        existing.experiment_id,
                        existing.command_content_hash,
                    ) != (
                        admission.owner,
                        admission.family_id,
                        admission.request_id,
                        admission.experiment_id,
                        admission.command_content_hash,
                    ):
                        raise ValueError("child admission immutable identity conflicts")
                if fault:
                    fault(child.intent.experiment_id)
            ready = record.model_copy(update={"state": "ready"})
            connection.execute(
                (
                    "UPDATE experiment_family_request SET state='ready',payload_json=? "
                    "WHERE request_id=?"
                ),
                (_json_payload(ready), str(request_id)),
            )
        return ready

    def child(self, job_id: UUID) -> ExperimentChildAdmission | None:
        with self.registry._connect() as connection:
            self._require_schema(connection)
            row = connection.execute(
                "SELECT payload_json FROM experiment_child_admission WHERE job_id=?", (str(job_id),)
            ).fetchone()
        return None if row is None else ExperimentChildAdmission.model_validate_json(row[0])

    def pending_cancellations(self, *, limit: int = 999) -> tuple[ExperimentChildAdmission, ...]:
        if type(limit) is not int or not 1 <= limit <= 999:
            raise ValueError("cancellation recovery limit is outside its budget")
        with self.registry._connect() as connection:
            self._require_schema(connection)
            rows = connection.execute(
                (
                    "SELECT payload_json FROM experiment_child_admission WHERE "
                    "json_extract(payload_json,'$.cancel_state')='pending' ORDER BY "
                    "job_id LIMIT "
                    "?"
                ),
                (limit + 1,),
            ).fetchall()
        if len(rows) > limit:
            raise ValueError("pending cancellation recovery exceeds its bounded budget")
        return tuple(ExperimentChildAdmission.model_validate_json(row[0]) for row in rows)

    def admit_publication(
        self, intent: ExperimentSubmissionIntent, *, now: datetime
    ) -> ExperimentChildAdmission:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT payload_json FROM experiment_child_admission WHERE job_id=?",
                (str(intent.job_id),),
            ).fetchone()
            if row is None:
                raise ValueError("formal child admission is missing")
            child = ExperimentChildAdmission.model_validate_json(row[0])
            record = self._record(connection, child.owner, child.family_id)
            if record.state != "ready" or (
                child.request_id,
                child.experiment_id,
                child.command_content_hash,
            ) != (intent.request_id, intent.experiment_id, intent.command_content_hash):
                raise ValueError("formal publication identity is not ready")
            if child.cancel_state == "before_publication":
                raise PermissionError("family cancellation preceded publication admission")
            if child.publish_grant_seq is None:
                child = child.model_copy(
                    update={
                        "publish_grant_seq": self._operation(
                            connection, "publish", str(child.job_id), now
                        )
                    }
                )
                connection.execute(
                    "UPDATE experiment_child_admission SET payload_json=? WHERE job_id=?",
                    (_json_payload(child), str(child.job_id)),
                )
        return child

    def validate_publication(self, intent: ExperimentSubmissionIntent) -> None:
        validate_experiment_publication_grant(self.child(intent.job_id), intent)

    def cancel_family(
        self, *, owner: str, family_id: str, request_id: UUID, now: datetime
    ) -> tuple[ExperimentChildAdmission, ...]:
        body_hash = canonical_sha256({"kind": "cancel", "family": family_id})
        with self.transaction() as connection:
            record = self._record(connection, owner, family_id)
            previous = connection.execute(
                "SELECT * FROM experiment_platform_receipt WHERE request_id=?", (str(request_id),)
            ).fetchone()
            if previous is not None:
                if (previous["owner"], previous["body_hash"]) != (owner, body_hash):
                    raise ValueError("original cancellation content conflicts")
                return tuple(
                    ExperimentChildAdmission.model_validate(v)
                    for v in json.loads(previous["payload_json"])
                )
            if record.state in ("preparing", "cancelled"):
                if record.state == "preparing":
                    connection.execute(
                        "UPDATE experiment_family_request SET "
                        "state='cancelled',payload_json=? WHERE family_id=?",
                        (
                            _json_payload(record.model_copy(update={"state": "cancelled"})),
                            family_id,
                        ),
                    )
                    slots = connection.execute(
                        "SELECT child_index,payload_json FROM "
                        "experiment_template_slot WHERE family_id=?",
                        (family_id,),
                    ).fetchall()
                    for slot_row in slots:
                        slot = ExperimentTemplateSlot.model_validate_json(slot_row[1])
                        if slot.state in ("pending", "saved"):
                            connection.execute(
                                "UPDATE experiment_template_slot SET "
                                "payload_json=? WHERE family_id=? AND child_index=?",
                                (
                                    _json_payload(slot.model_copy(update={"state": "cancelled"})),
                                    family_id,
                                    slot_row[0],
                                ),
                            )
                connection.execute(
                    "INSERT INTO experiment_platform_receipt VALUES(?,?,?,?)",
                    (str(request_id), owner, body_hash, "[]"),
                )
                return ()
            rows = connection.execute(
                (
                    "SELECT payload_json FROM experiment_child_admission WHERE "
                    "hypothesis_family=? ORDER BY "
                    "experiment_id"
                ),
                (family_id,),
            ).fetchall()
            children = []
            for row in rows:
                child = ExperimentChildAdmission.model_validate_json(row[0])
                if child.cancel_seq is None:
                    seq = self._operation(connection, "cancel", str(child.job_id), now)
                    state = "before_publication" if child.publish_grant_seq is None else "pending"
                    child = child.model_copy(update={"cancel_seq": seq, "cancel_state": state})
                    if state == "before_publication":
                        attempt = self.registry._required_attempt_row(
                            connection, child.experiment_id
                        )
                        if attempt["status"] != ExperimentStatus.REGISTERED:
                            raise ValueError("unadmitted child has an unexpected lifecycle")
                        connection.execute(
                            (
                                "UPDATE experiment_attempt SET "
                                "status=?,completed_at=?,first_error=? WHERE "
                                "experiment_id=?"
                            ),
                            (
                                ExperimentStatus.CANCELLED.value,
                                _utc_iso(now),
                                "cancelled before publication admission",
                                child.experiment_id,
                            ),
                        )
                    connection.execute(
                        "UPDATE experiment_child_admission SET payload_json=? WHERE job_id=?",
                        (_json_payload(child), str(child.job_id)),
                    )
                children.append(child)
            raw = json.dumps([c.model_dump(mode="json") for c in children], separators=(",", ":"))
            connection.execute(
                "INSERT INTO experiment_platform_receipt VALUES(?,?,?,?)",
                (str(request_id), owner, body_hash, raw),
            )
        return tuple(children)

    def record_cancel_progress(self, child: ExperimentChildAdmission) -> None:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT payload_json FROM experiment_child_admission WHERE job_id=?",
                (str(child.job_id),),
            ).fetchone()
            if row is None:
                raise ValueError("original cancellation admission is missing")
            current = ExperimentChildAdmission.model_validate_json(row[0])
            if (
                current.model_dump(
                    exclude={"cancel_request_chain", "cancel_job_version", "cancel_state"}
                )
                != child.model_dump(
                    exclude={"cancel_request_chain", "cancel_job_version", "cancel_state"}
                )
                or child.cancel_request_chain[: len(current.cancel_request_chain)]
                != current.cancel_request_chain
            ):
                raise ValueError("cancellation progress changed its immutable admission")
            if (
                current.cancel_state
                in ("before_publication", "confirmed", "already_completed", "failed")
                and child != current
            ):
                raise ValueError("cancellation fact is terminal")
            connection.execute(
                "UPDATE experiment_child_admission SET payload_json=? WHERE job_id=?",
                (_json_payload(child), str(child.job_id)),
            )

    def list_outer_grants(self, owner: str) -> tuple[ExperimentOuterGrant, ...]:
        with self.registry._connect() as connection:
            self._require_schema(connection)
            rows = connection.execute(
                (
                    "SELECT payload_json FROM experiment_outer_grant WHERE owner=? ORDER "
                    "BY range_start,grant_id"
                ),
                (owner,),
            ).fetchall()
        return tuple(ExperimentOuterGrant.model_validate_json(r[0]) for r in rows)

    def admit_outer(
        self,
        *,
        owner: str,
        family_id: str,
        request_id: UUID,
        experiment_id: str,
        now: datetime,
        body_hash: str | None = None,
        result_hash: str | None = None,
        source_identity: str | None = None,
        expected_policy_version: int | None = None,
        cutoff: date | None = None,
    ) -> ExperimentOuterGrant:
        with self.transaction() as connection:
            previous = connection.execute(
                "SELECT payload_json FROM experiment_outer_grant WHERE request_id=?",
                (str(request_id),),
            ).fetchone()
            if previous is not None:
                grant = ExperimentOuterGrant.model_validate_json(previous[0])
                if (
                    grant.owner,
                    grant.family_id,
                    grant.experiment_id,
                    grant.body_hash,
                    grant.result_hash,
                ) != (owner, family_id, experiment_id, body_hash, result_hash):
                    raise ValueError("original outer request content conflicts")
                return grant
            record = self._record(connection, owner, family_id)
            if record.state != "ready" or record.phase != "search":
                raise ValueError("outer requires a ready terminal search")
            rows = connection.execute(
                "SELECT experiment_id,status FROM experiment_attempt WHERE hypothesis_family=?",
                (family_id,),
            ).fetchall()
            terminal = {
                s.value
                for s in (
                    ExperimentStatus.EXECUTED,
                    ExperimentStatus.SUCCEEDED,
                    ExperimentStatus.FAILED,
                    ExperimentStatus.CANCELLED,
                )
            }
            if len(rows) != len(record.actual_configurations) or any(
                r["status"] not in terminal for r in rows
            ):
                raise ValueError("outer requires a complete terminal search")
            child_row = connection.execute(
                (
                    "SELECT payload_json FROM experiment_child_admission WHERE "
                    "hypothesis_family=? AND "
                    "experiment_id=?"
                ),
                (family_id, experiment_id),
            ).fetchone()
            if child_row is None or not any(
                r["experiment_id"] == experiment_id and r["status"] in ("executed", "succeeded")
                for r in rows
            ):
                raise ValueError("selected candidate has no actual completed result")
            manifest = self.registry._required_manifest(connection, family_id)
            index = next(
                i
                for i, c in enumerate(record.actual_configurations)
                if self._config_for_experiment(connection, family_id, experiment_id) == c
            )
            if experiment_id not in manifest.experiment_ids:
                raise ValueError("selected candidate differs from the complete family")
            window = record.request.protocol.frozen_outer_test_range
            if connection.execute(
                (
                    "SELECT 1 FROM experiment_outer_grant WHERE owner=? AND "
                    "range_start<=? AND range_end>=? LIMIT "
                    "1"
                ),
                (owner, window.end_date.isoformat(), window.start_date.isoformat()),
            ).fetchone():
                raise ValueError("overlapping outer interval was already unsealed")
            policy_row = connection.execute(
                "SELECT value FROM experiment_platform_metadata WHERE key='holdout_policy'"
            ).fetchone()
            policy = HoldoutPolicy.model_validate_json(policy_row[0])
            if (
                expected_policy_version != policy.version
                or cutoff is None
                or window.end_date > cutoff
            ):
                raise ValueError("current holdout policy changed or outer cutoff is invalid")
            if any(value is None for value in (body_hash, result_hash, source_identity)):
                raise ValueError("outer admission requires the complete selected result binding")
            grant = ExperimentOuterGrant(
                grant_id=canonical_sha256({"owner": owner, "request_id": request_id}),
                owner=owner,
                request_id=request_id,
                family_id=family_id,
                experiment_id=experiment_id,
                config=record.actual_configurations[index],
                outer_range=window,
                policy=policy,
                admitted_at=now,
                source_key=record.actual_configurations[index].source_key
                if isinstance(record.request, NativeMinuteExperimentRequest)
                else record.request.base_config.source_key,
                source_version=record.actual_configurations[index].source_version
                if isinstance(record.request, NativeMinuteExperimentRequest)
                else record.request.base_config.source_version,
                body_hash=body_hash,
                result_hash=result_hash,
                source_identity=source_identity,
            )
            connection.execute(
                "INSERT INTO experiment_outer_grant VALUES(?,?,?,?,?,?)",
                (
                    grant.grant_id,
                    owner,
                    str(request_id),
                    window.start_date.isoformat(),
                    window.end_date.isoformat(),
                    _json_payload(grant),
                ),
            )
        return grant

    @staticmethod
    def _config_for_experiment(
        connection: sqlite3.Connection, family_id: str, experiment_id: str
    ) -> ExperimentConfiguration:
        row = connection.execute(
            "SELECT value FROM experiment_platform_metadata WHERE key=?",
            ("config:" + experiment_id,),
        ).fetchone()
        if row is None:
            raise ValueError("actual candidate config is unavailable")
        return TypeAdapter(ExperimentConfiguration).validate_json(row[0])

    def set_note(
        self,
        *,
        owner: str,
        family_id: str,
        request_id: UUID,
        expected_version: int,
        text: str,
        now: datetime,
    ) -> ExperimentNote:
        body = canonical_sha256(
            {"kind": "note", "family": family_id, "version": expected_version, "text": text}
        )
        with self.transaction() as connection:
            self._record(connection, owner, family_id)
            old = connection.execute(
                "SELECT * FROM experiment_platform_receipt WHERE request_id=?", (str(request_id),)
            ).fetchone()
            if old is not None:
                if (old["owner"], old["body_hash"]) != (owner, body):
                    raise ValueError("original note content conflicts")
                return ExperimentNote.model_validate_json(old["payload_json"])
            row = connection.execute(
                "SELECT payload_json FROM experiment_note WHERE family_id=?", (family_id,)
            ).fetchone()
            version = 0 if row is None else ExperimentNote.model_validate_json(row[0]).version
            if version != expected_version:
                raise ValueError("note version conflict")
            note = ExperimentNote(
                family_id=family_id, owner=owner, version=version + 1, text=text, updated_at=now
            )
            connection.execute(
                "INSERT OR REPLACE INTO experiment_note VALUES(?,?,?,?)",
                (family_id, owner, note.version, _json_payload(note)),
            )
            connection.execute(
                "INSERT INTO experiment_platform_receipt VALUES(?,?,?,?)",
                (str(request_id), owner, body, _json_payload(note)),
            )
        return note

    def save_evidence(self, evidence: object) -> str:
        from rquant.experiment_platform_evidence import ExperimentOverfitEvidence

        checked = ExperimentOverfitEvidence.model_validate(evidence).seal()
        raw = checked.model_dump_json()
        if len(raw.encode()) > 8 * 1024 * 1024:
            raise ValueError("experiment evidence exceeds 8 MiB")
        with self.transaction() as connection:
            record = self._record(connection, checked.owner, checked.family_id)
            rows = connection.execute(
                "SELECT * FROM experiment_attempt WHERE hypothesis_family=? ORDER BY experiment_id",
                (checked.family_id,),
            ).fetchall()
            attempts = tuple(self.registry._attempt_from_row(connection, r) for r in rows)
            if (
                record.phase != "search"
                or checked.search_count != len(record.actual_configurations)
                or checked.attempt_digest != canonical_sha256(attempts)
                or any(
                    a.status in (ExperimentStatus.REGISTERED, ExperimentStatus.RUNNING)
                    for a in attempts
                )
            ):
                raise ValueError("sealed evidence does not match the complete terminal search")
            previous = connection.execute(
                "SELECT payload_json FROM experiment_evidence WHERE evidence_id=?",
                (checked.evidence_id,),
            ).fetchone()
            if previous is not None and previous[0] != raw:
                raise ValueError("sealed evidence identity conflicts")
            connection.execute(
                "INSERT OR IGNORE INTO experiment_evidence VALUES(?,?,?,?)",
                (checked.evidence_id, checked.owner, checked.family_id, raw),
            )
            connection.execute(
                "INSERT OR REPLACE INTO experiment_evidence_head VALUES(?,?,?)",
                (checked.family_id, checked.owner, checked.evidence_id),
            )
        return checked.evidence_id


def stable_experiment_job(owner: str, request_id: UUID, index: int) -> UUID:
    return uuid5(NAMESPACE_URL, f"rquant.experiment-job:{owner}:{request_id}:{index}")


def experiment_family_job(record: ExperimentFamilyRecord, index: int) -> UUID:
    if not 0 <= index < len(record.actual_configurations):
        raise ValueError("original family child index is outside the fixed array")
    if record.phase == "search" and isinstance(record.request, NativeMinuteExperimentRequest):
        original = record.request.walk_forward_command_id
        if original is not None:
            return uuid5(original, f"strategy-fixed-wf:{index + 1}")
    return stable_experiment_job(record.owner, record.request_id, index)


def stable_experiment_interaction(owner: str, request_id: UUID, index: int) -> str:
    return f"web.experiment:{owner}:{request_id}:{index}"
