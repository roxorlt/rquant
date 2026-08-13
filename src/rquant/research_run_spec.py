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


class ExecutionCostSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class ExecutionCostApplicability(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    BOTH = "BOTH"


class InstrumentSelector(RunSpecModel):
    """One exact, ordered instrument-scope selector for a v3 cost contract."""

    selector_id: str = Field(min_length=1)
    market: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    instrument_class: str = Field(min_length=1)
    security_class: str = Field(min_length=1)

    @field_validator("market", "exchange", "instrument_class", "security_class", mode="before")
    @classmethod
    def normalize_scope_value(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("instrument selector scope values must be non-empty strings")
        return value.strip().upper()

    @property
    def scope_key(self) -> tuple[str, str, str, str]:
        return (self.market, self.exchange, self.instrument_class, self.security_class)


class InstrumentClassificationProvenance(RunSpecModel):
    """Immutable listing-authority evidence for one execution instrument context."""

    reference_dataset: Literal["security_listing_status"]
    reference_record_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    reference_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")


class InstrumentContext(RunSpecModel):
    """Normalized authoritative classification used to select one v3 cost rule set."""

    ts_code: str = Field(min_length=1)
    market: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    instrument_class: str = Field(min_length=1)
    security_class: str = Field(min_length=1)
    classification_provenance: InstrumentClassificationProvenance | None = None

    @field_validator(
        "ts_code",
        "market",
        "exchange",
        "instrument_class",
        "security_class",
        mode="before",
    )
    @classmethod
    def normalize_context_value(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("instrument context values must be non-empty strings")
        return value.strip().upper()

    @property
    def scope_key(self) -> tuple[str, str, str, str]:
        return (self.market, self.exchange, self.instrument_class, self.security_class)


class ExecutionCostFeeRule(RunSpecModel):
    rule_id: str = Field(min_length=1)
    selector_id: str = Field(min_length=1)
    rate_bps: Decimal = Field(ge=0, lt=10_000)
    minimum_amount: Decimal = Field(ge=0)
    applies_to: ExecutionCostApplicability

    @field_validator("rate_bps", "minimum_amount", mode="before")
    @classmethod
    def validate_finite_decimal(cls, value: object) -> Decimal:
        return _parse_decimal(value, field_name="execution cost rule")

    def applies_to_side(self, side: ExecutionCostSide) -> bool:
        return (
            self.applies_to is ExecutionCostApplicability.BOTH
            or self.applies_to.value == side.value
        )


class ExecutionCostSlippage(RunSpecModel):
    owner: Literal["shared_cost_engine"]
    buy_bps: Decimal = Field(ge=0, lt=10_000)
    sell_bps: Decimal = Field(ge=0, lt=10_000)
    price_tick: Decimal = Field(gt=0)
    price_rounding: Literal["HALF_UP"]

    @field_validator("buy_bps", "sell_bps", "price_tick", mode="before")
    @classmethod
    def validate_finite_decimal(cls, value: object) -> Decimal:
        return _parse_decimal(value, field_name="execution cost slippage")


class ExecutionCostMoney(RunSpecModel):
    quantum: Decimal = Field(gt=0)
    rounding: Literal["HALF_UP"]

    @field_validator("quantum", mode="before")
    @classmethod
    def validate_finite_decimal(cls, value: object) -> Decimal:
        return _parse_decimal(value, field_name="execution cost money")


class ExecutionCostOrderInput(RunSpecModel):
    side: ExecutionCostSide
    reference_price: Decimal = Field(gt=0)
    quantity: StrictInt = Field(gt=0)

    @field_validator("reference_price", mode="before")
    @classmethod
    def validate_reference_price(cls, value: object) -> Decimal:
        return _parse_decimal(value, field_name="execution cost reference_price")


class ExecutionCostCalculation(RunSpecModel):
    """Resolved v3 fill evidence returned by the one shared cost engine."""

    cost_spec_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    cost_spec_schema_version: Literal[3]
    cost_engine_version: str = Field(min_length=1)
    order_input: ExecutionCostOrderInput
    instrument_context: InstrumentContext
    executed_price: Decimal = Field(gt=0)
    reference_notional: Decimal = Field(gt=0)
    executed_notional: Decimal = Field(gt=0)
    commission: Decimal = Field(ge=0)
    transfer_fee: Decimal = Field(ge=0)
    stamp_duty: Decimal = Field(ge=0)
    slippage_amount: Decimal = Field(ge=0)
    total_fees: Decimal = Field(ge=0)
    selected_rule_ids: dict[str, str]
    cost_context_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_component_sum(self) -> ExecutionCostCalculation:
        if self.total_fees != self.commission + self.transfer_fee + self.stamp_duty:
            raise ValueError("execution total_fees must equal the rounded fee components")
        expected_rule_components = {
            "selector",
            "commission",
            "transfer_fee",
            "stamp_duty",
        }
        if set(self.selected_rule_ids) != expected_rule_components or any(
            not rule_id for rule_id in self.selected_rule_ids.values()
        ):
            raise ValueError("execution cost calculation requires complete selected rule evidence")
        return self

    @property
    def resolved_calculation_fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class ExecutionCostSpec(RunSpecModel):
    """Versioned cost contract; only v3 can bind research and paper executions."""

    schema_version: Literal[1, 2, 3] = 1

    # v1/v2 historical research fields. They are intentionally not upgraded to v3.
    commission_bps: Decimal | None = Field(default=None, ge=0, le=10_000)
    stamp_duty_bps: Decimal | None = Field(default=None, ge=0, le=10_000)
    transfer_fee_bps: Decimal | None = Field(default=None, ge=0, le=10_000)
    slippage_bps: Decimal | None = Field(default=None, ge=0, le=10_000)
    minimum_commission: Decimal | None = Field(default=Decimal("0"), ge=0)

    # v3 canonical contract fields.
    cost_engine_version: str | None = Field(default=None, min_length=1)
    instrument_selectors: tuple[InstrumentSelector, ...] = ()
    commission_rules: tuple[ExecutionCostFeeRule, ...] = ()
    transfer_fee_rules: tuple[ExecutionCostFeeRule, ...] = ()
    stamp_duty_rules: tuple[ExecutionCostFeeRule, ...] = ()
    fee_notional_basis: Literal["EXECUTED_NOTIONAL"] | None = None
    assessment_unit: Literal["FILL"] | None = None
    slippage: ExecutionCostSlippage | None = None
    money: ExecutionCostMoney | None = None
    cost_spec_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    # This research-only input affects replay topology, not the cost-contract identity.
    research_notional_per_trade: Decimal | None = Field(default=None, gt=0)

    @model_validator(mode="before")
    @classmethod
    def validate_versioned_input(cls, data: object) -> object:
        if not isinstance(data, Mapping):
            return data
        schema_version = data.get("schema_version", 1)
        if type(schema_version) is not int or schema_version not in {1, 2, 3}:
            raise ValueError("execution cost schema_version must be integer 1, 2, or 3")
        return data

    @field_validator(
        "commission_bps",
        "stamp_duty_bps",
        "transfer_fee_bps",
        "slippage_bps",
        "minimum_commission",
        "research_notional_per_trade",
        mode="before",
    )
    @classmethod
    def validate_finite_decimal(cls, value: object) -> Decimal | None:
        if value is None:
            return None
        return _parse_decimal(value, field_name="execution cost")

    @staticmethod
    def _validate_v3_rule_set(
        rules: tuple[ExecutionCostFeeRule, ...],
        *,
        selectors: tuple[InstrumentSelector, ...],
        component: str,
        seen_rule_ids: set[str],
    ) -> None:
        selector_ids = {selector.selector_id for selector in selectors}
        if len(rules) != len(selectors):
            raise ValueError(f"v3 {component} rules must contain one rule per selector")
        rule_selector_ids = tuple(rule.selector_id for rule in rules)
        if set(rule_selector_ids) != selector_ids or len(rule_selector_ids) != len(
            set(rule_selector_ids)
        ):
            raise ValueError(f"v3 {component} rules must map one-to-one to selectors")
        for rule in rules:
            if rule.rule_id in seen_rule_ids:
                raise ValueError("v3 execution cost rule_id values must be globally unique")
            seen_rule_ids.add(rule.rule_id)

    def _v3_identity_payload(self) -> dict[str, object]:
        if self.schema_version != 3:
            raise ValueError("only v3 execution costs have a canonical cost identity")
        return {
            "schema_version": 3,
            "cost_engine_version": self.cost_engine_version,
            "instrument_selectors": self.instrument_selectors,
            "commission_rules": self.commission_rules,
            "transfer_fee_rules": self.transfer_fee_rules,
            "stamp_duty_rules": self.stamp_duty_rules,
            "fee_notional_basis": self.fee_notional_basis,
            "assessment_unit": self.assessment_unit,
            "slippage": self.slippage,
            "money": self.money,
        }

    def canonical_json(self) -> str:
        """Return the exact JSON bytes whose SHA-256 is the v3 cost identity."""

        payload = self._v3_identity_payload()
        return json.dumps(
            _canonical_value(payload),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    @classmethod
    def from_canonical_json(cls, value: str | bytes | bytearray) -> Self:
        """Parse and verify one persisted v3 canonical cost-spec authority value."""

        try:
            text = (
                bytes(value).decode("utf-8")
                if isinstance(value, bytearray)
                else (value.decode("utf-8") if isinstance(value, bytes) else value)
            )
        except UnicodeDecodeError as exc:
            raise ValueError("canonical execution cost JSON must be UTF-8") from exc
        try:
            payload = json.loads(text)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("canonical execution cost JSON is invalid") from exc
        decoded = _decode_canonical_value(payload)
        parsed = cls.model_validate(decoded)
        if parsed.schema_version != 3 or parsed.canonical_json() != text:
            raise ValueError("execution cost canonical JSON does not match its v3 contract")
        return parsed

    @property
    def is_alignment_eligible(self) -> bool:
        return self.schema_version == 3

    @model_validator(mode="after")
    def validate_versioned_contract(self) -> ExecutionCostSpec:
        legacy_values = (
            self.commission_bps,
            self.stamp_duty_bps,
            self.transfer_fee_bps,
            self.slippage_bps,
            self.minimum_commission,
        )
        if self.schema_version in {1, 2}:
            if any(value is None for value in legacy_values):
                raise ValueError("v1/v2 execution costs require all legacy fee fields")
            if any(
                value is not None
                for value in (
                    self.cost_engine_version,
                    self.fee_notional_basis,
                    self.assessment_unit,
                    self.slippage,
                    self.money,
                    self.cost_spec_id,
                )
            ) or any(
                (
                    self.instrument_selectors,
                    self.commission_rules,
                    self.transfer_fee_rules,
                    self.stamp_duty_rules,
                )
            ):
                raise ValueError("v1/v2 execution costs cannot carry v3 alignment fields")
            assert all(value is not None for value in legacy_values)
            commission, stamp, transfer, slippage, minimum = legacy_values
            assert commission is not None and stamp is not None and transfer is not None
            assert slippage is not None and minimum is not None
            buy_total = commission + transfer + slippage
            sell_total = buy_total + stamp
            if buy_total >= 10_000:
                raise ValueError("buy-side execution costs must total less than 10000 bps")
            if sell_total >= 10_000:
                raise ValueError("sell-side execution costs must total less than 10000 bps")
            if self.schema_version == 1 and (
                minimum != 0 or self.research_notional_per_trade is not None
            ):
                raise ValueError("v1 execution costs cannot carry notional fee fields")
            if minimum > 0 and self.research_notional_per_trade is None:
                raise ValueError("minimum commission requires research notional per trade")
            return self

        legacy_field_names = {
            "commission_bps",
            "stamp_duty_bps",
            "transfer_fee_bps",
            "slippage_bps",
            "minimum_commission",
        }
        if legacy_field_names & self.model_fields_set:
            raise ValueError("v3 execution costs cannot carry legacy scalar fee fields")
        required = (
            self.cost_engine_version,
            self.fee_notional_basis,
            self.assessment_unit,
            self.slippage,
            self.money,
        )
        if any(value is None for value in required):
            raise ValueError("v3 execution costs require complete shared cost configuration")
        if not self.instrument_selectors:
            raise ValueError("v3 execution costs require at least one instrument selector")
        selector_ids = tuple(selector.selector_id for selector in self.instrument_selectors)
        if len(selector_ids) != len(set(selector_ids)):
            raise ValueError("v3 instrument selector_id values must be unique")
        scope_keys = tuple(selector.scope_key for selector in self.instrument_selectors)
        if len(scope_keys) != len(set(scope_keys)):
            raise ValueError("v3 instrument selectors must be non-overlapping")
        rule_ids: set[str] = set()
        self._validate_v3_rule_set(
            self.commission_rules,
            selectors=self.instrument_selectors,
            component="commission",
            seen_rule_ids=rule_ids,
        )
        self._validate_v3_rule_set(
            self.transfer_fee_rules,
            selectors=self.instrument_selectors,
            component="transfer fee",
            seen_rule_ids=rule_ids,
        )
        self._validate_v3_rule_set(
            self.stamp_duty_rules,
            selectors=self.instrument_selectors,
            component="stamp duty",
            seen_rule_ids=rule_ids,
        )
        expected_cost_spec_id = hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
        if self.cost_spec_id is None:
            object.__setattr__(self, "cost_spec_id", expected_cost_spec_id)
        elif self.cost_spec_id != expected_cost_spec_id:
            raise ValueError("cost_spec_id does not match canonical v3 execution cost JSON")
        return self

    @model_serializer(mode="wrap")
    def serialize_versioned_contract(
        self,
        handler: SerializerFunctionWrapHandler,
    ) -> object:
        payload = handler(self)
        if not isinstance(payload, dict):
            return payload
        v3_fields = {
            "cost_engine_version",
            "instrument_selectors",
            "commission_rules",
            "transfer_fee_rules",
            "stamp_duty_rules",
            "fee_notional_basis",
            "assessment_unit",
            "slippage",
            "money",
            "cost_spec_id",
        }
        legacy_fields = {
            "commission_bps",
            "stamp_duty_bps",
            "transfer_fee_bps",
            "slippage_bps",
            "minimum_commission",
        }
        if self.schema_version == 1:
            payload.pop("schema_version", None)
            payload.pop("minimum_commission", None)
            payload.pop("research_notional_per_trade", None)
            for field in v3_fields:
                payload.pop(field, None)
        elif self.schema_version == 2:
            for field in v3_fields:
                payload.pop(field, None)
        else:
            for field in legacy_fields:
                payload.pop(field, None)
        return payload


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
    if isinstance(value, BaseModel):
        return _canonical_value(value.model_dump(mode="python"))
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


def _decode_canonical_value(value: object) -> object:
    if isinstance(value, Mapping):
        if set(value) == {"$decimal"}:
            return _parse_decimal(value["$decimal"], field_name="canonical decimal")
        return {key: _decode_canonical_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode_canonical_value(item) for item in value]
    if isinstance(value, float):
        raise ValueError("canonical execution cost JSON cannot contain raw floats")
    return value


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
