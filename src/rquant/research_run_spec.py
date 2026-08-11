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

from rquant.experiment_registry import ExperimentSpec
from rquant.research_manifest import ResearchStatus
from rquant.runtime_contracts import canonical_sha256


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


def _canonical_hash(value: object) -> str:
    payload = json.dumps(
        _canonical_value(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class StrategyExecutionIdentity(RunSpecModel):
    """Content-addressed Definition Registry evidence bound to one research run."""

    schema_version: Literal[1] = 1
    strategy_id: str = Field(min_length=1)
    strategy_version: int = Field(strict=True, ge=1)
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    strategy_spec_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    strategy_definition_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    strategy_executable_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_schema_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    definition_registration_record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    definition_registered_at: datetime
    definition_available_at: datetime
    producer_code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    identity_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @field_validator("definition_registered_at", "definition_available_at", mode="before")
    @classmethod
    def validate_definition_time(cls, value: object) -> datetime:
        return _parse_aware_datetime(value, field_name="definition registration time")

    @model_validator(mode="after")
    def validate_identity(self) -> StrategyExecutionIdentity:
        if self.definition_available_at < self.definition_registered_at:
            raise ValueError("definition_available_at cannot precede definition_registered_at")
        expected = _canonical_hash(self.model_dump(mode="python", exclude={"identity_hash"}))
        if self.identity_hash is None:
            object.__setattr__(self, "identity_hash", expected)
        elif self.identity_hash != expected:
            raise ValueError("identity_hash does not match canonical strategy execution identity")
        return self


class ResearchExperimentIdentity(RunSpecModel):
    """Stable experiment ownership; worker retries retain the same attempt identity."""

    schema_version: Literal[1, 2] = 1
    spec: ExperimentSpec
    experiment_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    hypothesis_family: str = Field(min_length=1)
    hypothesis_variant: str = Field(min_length=1)
    formal_plan_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    attempt_identity: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_attempt_identity(self) -> ResearchExperimentIdentity:
        if self.spec.experiment_id != self.experiment_id:
            raise ValueError("experiment_id does not match immutable ExperimentSpec")
        if self.spec.hypothesis_family != self.hypothesis_family:
            raise ValueError("hypothesis_family does not match immutable ExperimentSpec")
        if self.schema_version == 2 and self.formal_plan_id is None:
            raise ValueError("formal_plan_id is required for current experiment ownership")
        if self.schema_version == 1 and self.formal_plan_id is not None:
            raise ValueError("legacy experiment ownership cannot carry formal_plan_id")
        expected = _canonical_hash(
            {
                "contract": "research-experiment-attempt/v1",
                "experiment_id": self.experiment_id,
                "hypothesis_family": self.hypothesis_family,
                "hypothesis_variant": self.hypothesis_variant,
            }
        )
        if self.attempt_identity is None:
            object.__setattr__(self, "attempt_identity", expected)
        elif self.attempt_identity != expected:
            raise ValueError("attempt_identity does not match canonical experiment ownership")
        return self


class ResearchRunSpec(RunSpecModel):
    schema_version: Literal[1, 2, 3] = 2
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
    strategy_execution: StrategyExecutionIdentity | None = None
    experiment: ResearchExperimentIdentity | None = None

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
        if type(schema_version) is not int or schema_version not in {1, 2, 3}:
            raise ValueError("schema_version must be integer 1, 2, or 3")
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
        if self.schema_version < 3:
            if self.strategy_execution is not None or self.experiment is not None:
                raise ValueError("legacy run specs cannot carry v3 ownership identity")
            return self
        if self.strategy_execution is None:
            raise ValueError("v3 requires strategy_execution")
        if self.experiment is None:
            raise ValueError("v3 requires experiment")
        if self.dataset_snapshot is None or self.dataset_snapshot.audit_run_id is None:
            raise ValueError("v3 requires an audited immutable dataset snapshot")
        if self.strategy_execution.strategy_id != self.parameters.strategy_name:
            raise ValueError("strategy_execution.strategy_id must match parameters.strategy_name")
        if self.strategy_execution.producer_code_commit != self.code_sha:
            raise ValueError("strategy_execution.producer_code_commit must match code_sha")
        experiment_spec = self.experiment.spec
        assert self.dataset_snapshot is not None
        exact_experiment_identity = (
            experiment_spec.strategy_spec_fingerprint,
            experiment_spec.strategy_executable_fingerprint,
            experiment_spec.candidate_schema_fingerprint,
            experiment_spec.dataset_snapshot_id,
            experiment_spec.code_commit,
            experiment_spec.parameter_fingerprint,
            experiment_spec.cost_model_fingerprint,
            experiment_spec.execution_model_fingerprint,
            experiment_spec.seed,
        )
        expected_experiment_identity = (
            self.strategy_execution.strategy_spec_fingerprint,
            self.strategy_execution.strategy_executable_fingerprint,
            self.strategy_execution.candidate_schema_fingerprint,
            self.dataset_snapshot.snapshot_id,
            self.code_sha,
            canonical_sha256(self.parameters),
            canonical_sha256(self.execution_costs),
            canonical_sha256(
                {
                    "contract": "lab-adapter-execution/v1",
                    "adapter_id": self.strategy_execution.adapter_id,
                    "adapter_version": self.strategy_execution.adapter_version,
                    "feature_contract": self.feature_contract,
                }
            ),
            self.random_seed,
        )
        if exact_experiment_identity != expected_experiment_identity:
            raise ValueError("experiment spec does not exactly bind the v3 research run")
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
        if self.schema_version < 3 and isinstance(payload, dict):
            payload.pop("strategy_execution", None)
            payload.pop("experiment", None)
        return payload

    @property
    def catalog_owner_eligible(self) -> bool:
        return (
            self.schema_version == 3
            and self.strategy_execution is not None
            and self.experiment is not None
            and self.experiment.schema_version == 2
            and self.experiment.formal_plan_id is not None
            and self.dataset_snapshot is not None
            and self.dataset_snapshot.audit_run_id is not None
        )

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
