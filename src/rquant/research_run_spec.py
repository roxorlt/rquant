"""Frozen, canonical contract for reproducible Strategy Lab work."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum, StrEnum
from typing import Literal, Self, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    StrictBool,
    StrictInt,
    field_validator,
    model_serializer,
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
    INTEGER_LIST = "integer_list"
    DECIMAL = "decimal"
    TEXT = "text"
    TEXT_LIST = "text_list"
    DATE = "date"
    DATETIME = "datetime"


ParameterValue: TypeAlias = (
    StrictBool
    | StrictInt
    | tuple[StrictInt, ...]
    | Decimal
    | datetime
    | date
    | str
    | tuple[str, ...]
)
MAX_DECIMAL_COEFFICIENT_DIGITS = 128
MAX_DECIMAL_ABS_EXPONENT = 384


class RunSpecModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )


def _decimal_components(
    value: Decimal,
    *,
    field_name: str,
) -> tuple[int, tuple[int, ...], int]:
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    parts = value.as_tuple()
    if not isinstance(parts.exponent, int):
        raise ValueError(f"{field_name} must have a finite integer exponent")
    if len(parts.digits) > MAX_DECIMAL_COEFFICIENT_DIGITS:
        raise ValueError(
            f"{field_name} coefficient digits cannot exceed {MAX_DECIMAL_COEFFICIENT_DIGITS}"
        )
    if abs(parts.exponent) > MAX_DECIMAL_ABS_EXPONENT:
        raise ValueError(
            f"{field_name} exponent magnitude cannot exceed {MAX_DECIMAL_ABS_EXPONENT}"
        )
    if value.is_zero():
        return 0, (0,), 0

    digits = list(parts.digits)
    exponent = parts.exponent
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1
    if abs(exponent) > MAX_DECIMAL_ABS_EXPONENT:
        raise ValueError(
            f"{field_name} normalized exponent magnitude cannot exceed {MAX_DECIMAL_ABS_EXPONENT}"
        )
    return parts.sign, tuple(digits), exponent


def _parse_decimal(value: object, *, field_name: str) -> Decimal:
    if isinstance(value, (bool, Mapping)):
        raise ValueError(f"{field_name} must be a finite decimal")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite decimal") from exc
    _decimal_components(parsed, field_name=field_name)
    return Decimal(0) if parsed.is_zero() else parsed


def _normalize_datetime(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_aware_datetime(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return _normalize_datetime(value, field_name=field_name)
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a timezone-aware datetime or ISO datetime string")
    text = value.strip()
    if "T" not in text and " " not in text:
        raise ValueError(f"{field_name} must be a timezone-aware datetime or ISO datetime string")
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} requires an ISO datetime string") from exc
    return _normalize_datetime(parsed, field_name=field_name)


def _parse_civil_date(value: object, *, field_name: str) -> date:
    if isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a civil date, not a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a civil date or ISO date string")
    text = value.strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} requires an ISO civil date string") from exc
    if parsed.isoformat() != text:
        raise ValueError(f"{field_name} requires an ISO civil date string")
    return parsed


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
        if isinstance(value, (Mapping, set)):
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
        elif kind is ParameterKind.INTEGER_LIST:
            if not isinstance(value, (list, tuple)) or not value:
                raise ValueError("integer_list parameter requires a non-empty integer list")
            if any(not isinstance(item, int) or isinstance(item, bool) for item in value):
                raise ValueError("integer_list parameter requires integer items")
            if len(value) != len(set(value)):
                raise ValueError("integer_list parameter items must be unique")
            parsed["value"] = tuple(sorted(value))
        elif kind is ParameterKind.DECIMAL:
            parsed["value"] = _parse_decimal(value, field_name="decimal parameter")
        elif kind is ParameterKind.TEXT:
            if not isinstance(value, str):
                raise ValueError("text parameter requires a string")
        elif kind is ParameterKind.TEXT_LIST:
            if not isinstance(value, (list, tuple)) or not value:
                raise ValueError("text_list parameter requires a non-empty string list")
            if any(not isinstance(item, str) for item in value):
                raise ValueError("text_list parameter requires string items")
            normalized = tuple(item.strip() for item in value)
            if any(not item for item in normalized):
                raise ValueError("text_list parameter items must not be empty")
            if len(normalized) != len(set(normalized)):
                raise ValueError("text_list parameter items must be unique")
            parsed["value"] = tuple(sorted(normalized))
        elif kind is ParameterKind.DATE:
            parsed["value"] = _parse_civil_date(value, field_name="date parameter")
        else:
            parsed["value"] = _parse_aware_datetime(
                value,
                field_name="datetime parameter",
            )
        return parsed

    @model_validator(mode="after")
    def validate_kind_matches_value(self) -> ResearchParameter:
        matches = {
            ParameterKind.BOOLEAN: type(self.value) is bool,
            ParameterKind.INTEGER: type(self.value) is int,
            ParameterKind.INTEGER_LIST: (
                isinstance(self.value, tuple)
                and bool(self.value)
                and all(type(item) is int for item in self.value)
            ),
            ParameterKind.DECIMAL: isinstance(self.value, Decimal),
            ParameterKind.TEXT: type(self.value) is str,
            ParameterKind.TEXT_LIST: (
                isinstance(self.value, tuple)
                and bool(self.value)
                and all(type(item) is str for item in self.value)
            ),
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

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def validate_civil_date(cls, value: object) -> date:
        return _parse_civil_date(value, field_name="research date")

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
    audit_run_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


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

    @model_validator(mode="after")
    def validate_round_trip_factors(self) -> ExecutionCostSpec:
        buy_total = self.commission_bps + self.transfer_fee_bps + self.slippage_bps
        sell_total = buy_total + self.stamp_duty_bps
        if buy_total >= 10_000:
            raise ValueError("buy-side execution costs must total less than 10000 bps")
        if sell_total >= 10_000:
            raise ValueError("sell-side execution costs must total less than 10000 bps")
        return self


def _canonical_decimal(value: Decimal) -> str:
    sign, digits, exponent = _decimal_components(
        value,
        field_name="canonical decimal",
    )
    if digits == (0,):
        return "0"
    coefficient = "".join(str(digit) for digit in digits)
    if exponent >= 0:
        magnitude = f"{coefficient}{'0' * exponent}"
    else:
        point = len(coefficient) + exponent
        if point > 0:
            magnitude = f"{coefficient[:point]}.{coefficient[point:]}"
        else:
            magnitude = f"0.{'0' * -point}{coefficient}"
    return f"{'-' if sign else ''}{magnitude}"


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
    schema_version: Literal[1, 2] = 2
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

    @field_validator("deadline", mode="before")
    @classmethod
    def validate_deadline(cls, value: object) -> datetime:
        return _parse_aware_datetime(value, field_name="deadline")

    @model_validator(mode="before")
    @classmethod
    def validate_versioned_input(cls, data: object) -> object:
        if not isinstance(data, Mapping):
            return data
        schema_version = data.get("schema_version", 2)
        if type(schema_version) is not int or schema_version not in {1, 2}:
            raise ValueError("schema_version must be integer 1 or 2")
        if schema_version != 1:
            return data
        snapshot = data.get("dataset_snapshot")
        if isinstance(snapshot, Mapping) and "audit_run_id" in snapshot:
            raise ValueError("v1 dataset_snapshot must not contain audit_run_id")
        if (
            isinstance(snapshot, DatasetSnapshotIdentity)
            and "audit_run_id" in snapshot.model_fields_set
        ):
            raise ValueError("v1 dataset_snapshot must not contain audit_run_id")
        return data

    @model_validator(mode="after")
    def enforce_snapshot_research_status(self) -> ResearchRunSpec:
        if (
            self.schema_version == 1
            and self.dataset_snapshot is not None
            and self.dataset_snapshot.audit_run_id is not None
        ):
            raise ValueError("v1 dataset_snapshot must not contain audit_run_id")
        if self.research_status != "exploratory":
            if self.dataset_snapshot is None:
                raise ValueError(
                    "an immutable dataset snapshot is required above exploratory status"
                )
            if self.schema_version == 2 and self.dataset_snapshot.audit_run_id is None:
                raise ValueError(
                    "dataset_snapshot.audit_run_id is required above exploratory status"
                )
        return self

    @model_serializer(mode="wrap")
    def serialize_versioned_contract(
        self,
        handler: SerializerFunctionWrapHandler,
    ) -> object:
        payload = handler(self)
        if self.schema_version == 1 and isinstance(payload, dict):
            snapshot = payload.get("dataset_snapshot")
            if isinstance(snapshot, dict):
                snapshot.pop("audit_run_id", None)
        return payload

    def model_copy(
        self,
        *,
        update: Mapping[str, object] | None = None,
        deep: bool = False,
    ) -> Self:
        if not update:
            return super().model_copy(deep=deep)
        payload = self.model_dump(mode="python", round_trip=True)
        payload.update(update)
        validated = type(self).model_validate(payload)
        validated_update = {field_name: getattr(validated, field_name) for field_name in update}
        return super().model_copy(update=validated_update, deep=deep)

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
