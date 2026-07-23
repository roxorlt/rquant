"""Frozen, canonical contract for reproducible Strategy Lab work."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum, StrEnum
from typing import Literal, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)

from rquant.research_manifest import ResearchStatus


class ResearchJobType(StrEnum):
    STRATEGY_REPLAY = "strategy_replay"
    PARAMETER_SEARCH = "parameter_search"
    ABLATION = "ablation"


class ResourceClass(StrEnum):
    INTERACTIVE = "interactive"
    STANDARD = "standard"
    HEAVY = "heavy"


class ParameterKind(StrEnum):
    BOOLEAN = "boolean"
    INTEGER = "integer"
    DECIMAL = "decimal"
    TEXT = "text"
    DATE = "date"
    DATETIME = "datetime"


ParameterValue: TypeAlias = StrictBool | StrictInt | Decimal | datetime | date | str


class RunSpecModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


def _parse_decimal(value: object, *, field_name: str) -> Decimal:
    if isinstance(value, (bool, Mapping)):
        raise ValueError(f"{field_name} must be a finite decimal")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return Decimal(0) if parsed.is_zero() else parsed


def _normalize_datetime(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


class ResearchParameter(RunSpecModel):
    name: str = Field(min_length=1)
    kind: ParameterKind
    value: ParameterValue

    @model_validator(mode="before")
    @classmethod
    def parse_typed_value(cls, data: object) -> object:
        if not isinstance(data, Mapping):
            return data
        parsed = dict(data)
        value = parsed.get("value")
        if isinstance(value, (Mapping, list, tuple, set)):
            raise ValueError("parameter value must be a typed scalar")
        try:
            kind = ParameterKind(parsed.get("kind"))
        except (TypeError, ValueError):
            return parsed

        if kind is ParameterKind.BOOLEAN:
            if not isinstance(value, bool):
                raise ValueError("boolean parameter requires a bool")
        elif kind is ParameterKind.INTEGER:
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError("integer parameter requires an int")
        elif kind is ParameterKind.DECIMAL:
            parsed["value"] = _parse_decimal(value, field_name="decimal parameter")
        elif kind is ParameterKind.TEXT:
            if not isinstance(value, str):
                raise ValueError("text parameter requires a string")
        elif kind is ParameterKind.DATE:
            if isinstance(value, datetime):
                raise ValueError("date parameter requires a civil date")
            if isinstance(value, str):
                try:
                    parsed["value"] = date.fromisoformat(value)
                except ValueError as exc:
                    raise ValueError("date parameter requires an ISO date") from exc
            elif not isinstance(value, date):
                raise ValueError("date parameter requires a civil date")
        else:
            if isinstance(value, str):
                try:
                    value = datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError as exc:
                    raise ValueError("datetime parameter requires an ISO datetime") from exc
            if not isinstance(value, datetime):
                raise ValueError("datetime parameter requires a datetime")
            parsed["value"] = _normalize_datetime(
                value,
                field_name="datetime parameter",
            )
        return parsed

    @model_validator(mode="after")
    def validate_kind_matches_value(self) -> ResearchParameter:
        matches = {
            ParameterKind.BOOLEAN: type(self.value) is bool,
            ParameterKind.INTEGER: type(self.value) is int,
            ParameterKind.DECIMAL: isinstance(self.value, Decimal),
            ParameterKind.TEXT: type(self.value) is str,
            ParameterKind.DATE: type(self.value) is date,
            ParameterKind.DATETIME: type(self.value) is datetime,
        }
        if not matches[self.kind]:
            raise ValueError(f"parameter kind {self.kind} does not match its value")
        return self


class ResearchRunParameters(RunSpecModel):
    strategy_name: str = Field(min_length=1)
    start_date: date
    end_date: date
    arguments: tuple[ResearchParameter, ...] = ()

    @field_validator("arguments")
    @classmethod
    def validate_arguments(
        cls,
        values: tuple[ResearchParameter, ...],
    ) -> tuple[ResearchParameter, ...]:
        names = tuple(item.name for item in values)
        if len(names) != len(set(names)):
            raise ValueError("research parameter names must be unique")
        return tuple(sorted(values, key=lambda item: item.name))

    @model_validator(mode="after")
    def validate_date_range(self) -> ResearchRunParameters:
        if self.start_date > self.end_date:
            raise ValueError("research start_date cannot be after end_date")
        return self


class DatasetSnapshotIdentity(RunSpecModel):
    snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class FeatureContractIdentity(RunSpecModel):
    contract_id: str = Field(min_length=1)
    contract_version: str = Field(min_length=1)
    contract_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ExecutionCostSpec(RunSpecModel):
    commission_bps: Decimal = Field(ge=0, le=10_000)
    stamp_duty_bps: Decimal = Field(ge=0, le=10_000)
    transfer_fee_bps: Decimal = Field(ge=0, le=10_000)
    slippage_bps: Decimal = Field(ge=0, le=10_000)

    @field_validator(
        "commission_bps",
        "stamp_duty_bps",
        "transfer_fee_bps",
        "slippage_bps",
        mode="before",
    )
    @classmethod
    def validate_finite_decimal(cls, value: object) -> Decimal:
        return _parse_decimal(value, field_name="execution cost")


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("canonical numeric values must be finite")
    if value.is_zero():
        return "0"
    return format(value.normalize(), "f")


def _canonical_value(value: object) -> object:
    if isinstance(value, Enum):
        return _canonical_value(value.value)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical numeric values must be finite")
        raise TypeError("raw float values are not canonical; use Decimal")
    if isinstance(value, Decimal):
        return {"$decimal": _canonical_decimal(value)}
    if isinstance(value, datetime):
        normalized = _normalize_datetime(value, field_name="canonical datetime")
        return {
            "$datetime": normalized.isoformat(timespec="microseconds").replace(
                "+00:00",
                "Z",
            )
        }
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical mappings require string keys")
        return {key: _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


class ResearchRunSpec(RunSpecModel):
    schema_version: Literal[1] = 1
    job_type: ResearchJobType
    parameters: ResearchRunParameters
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    dataset_snapshot: DatasetSnapshotIdentity | None
    feature_contract: FeatureContractIdentity
    execution_costs: ExecutionCostSpec
    random_seed: int = Field(strict=True, ge=0, lt=2**63)
    resource_class: ResourceClass
    deadline: datetime
    research_status: ResearchStatus = "exploratory"

    @field_validator("deadline")
    @classmethod
    def validate_deadline(cls, value: datetime) -> datetime:
        return _normalize_datetime(value, field_name="deadline")

    @model_validator(mode="after")
    def enforce_snapshot_research_status(self) -> ResearchRunSpec:
        if self.dataset_snapshot is None and self.research_status != "exploratory":
            raise ValueError("an immutable dataset snapshot is required above exploratory status")
        return self

    def canonical_json(self) -> str:
        payload = _canonical_value(self.model_dump(mode="python"))
        return json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    @property
    def spec_hash(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
