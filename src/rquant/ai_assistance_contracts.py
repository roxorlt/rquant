"""Immutable AI context and facts; authority remains with the original owner service."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from typing import Annotated, Literal, Self
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import Field, field_validator, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
FactId = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_.:-]{0,127}$")]
MAX_FACTS = 256
MAX_INTERPRETATION_BYTES = 1024 * 1024
MAX_SOURCE_BODY_BYTES = 16 * 1024 * 1024
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _exact_identity(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError("identity must be exact nonempty text without control characters")
    return value


class AIRequestBinding(RuntimeContractModel):
    owner_uid: str = Field(min_length=1, max_length=128)
    request_id: UUID
    request_body_sha256: Sha256
    purpose: Literal["screen", "pool_edit", "interpretation", "news_digest"]
    account_id: str = Field(min_length=1, max_length=128)
    model_id: str = Field(min_length=1, max_length=128)
    template_version: str = Field(min_length=1, max_length=128)
    context_sha256: Sha256
    reserved_at: AwareUtcDatetime
    budget_date: date

    _identities = field_validator(
        "owner_uid", "account_id", "model_id", "template_version", mode="before"
    )(_exact_identity)

    @model_validator(mode="after")
    def bind_reservation_window(self) -> Self:
        if self.budget_date != self.reserved_at.astimezone(_SHANGHAI).date():
            raise ValueError("budget day must be the original Shanghai reservation day")
        return self

    @property
    def account_budget_key(self) -> str:
        # Model/template changes cannot create a new account allowance.
        return canonical_sha256((self.account_id, self.budget_date))

    @property
    def content_binding_sha256(self) -> str:
        return canonical_sha256(self)


class AIMeasuredUsage(RuntimeContractModel):
    input_tokens: int | None = Field(default=None, strict=True, ge=0, le=2**63 - 1)
    output_tokens: int | None = Field(default=None, strict=True, ge=0, le=2**63 - 1)

    @property
    def known(self) -> bool:
        return self.input_tokens is not None and self.output_tokens is not None

    @property
    def total_tokens(self) -> int | None:
        if self.input_tokens is None or self.output_tokens is None:
            return None
        return self.input_tokens + self.output_tokens


class AISealedResultBinding(RuntimeContractModel):
    owner_uid: str = Field(min_length=1, max_length=128)
    source_kind: Literal["portfolio", "strategy_template"]
    job_id: UUID
    spec_sha256: Sha256
    manifest_sha256: Sha256
    result_sha256: Sha256

    _owner = field_validator("owner_uid", mode="before")(_exact_identity)


class AIInterpretationBinding(RuntimeContractModel):
    result: AISealedResultBinding
    facts_sha256: Sha256
    model_id: str = Field(min_length=1, max_length=128)
    template_version: str = Field(min_length=1, max_length=128)

    _configuration = field_validator("model_id", "template_version", mode="before")(_exact_identity)

    @property
    def cache_key(self) -> str:
        return canonical_sha256(self)


class AISealedFact(RuntimeContractModel):
    fact_id: FactId
    label: str = Field(min_length=1, max_length=128)
    kind: Literal["number", "date", "text"]
    value: Decimal | date | str | None
    unit: Literal["", "%", "元", "天", "次", "倍", "亿元", "万元", "股", "年", "条"] = ""
    decimals: int = Field(default=2, strict=True, ge=0, le=8)
    source_path: str = Field(min_length=1, max_length=512)
    source_sha256: Sha256
    derivation_sha256: Sha256 | None = None

    @model_validator(mode="before")
    @classmethod
    def admit_typed_value(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        data = dict(value)
        raw = data.get("value")
        if raw is None:
            return data
        if data.get("kind") == "number":
            if isinstance(raw, bool) or not isinstance(raw, (Decimal, int, str)):
                raise ValueError("numeric facts require exact decimal values")
            try:
                number = Decimal(raw)
            except InvalidOperation as error:
                raise ValueError("numeric fact is not decimal") from error
            if not number.is_finite():
                raise ValueError("numeric facts must be finite")
            parts = number.as_tuple()
            if len(parts.digits) + abs(int(parts.exponent)) > 4096:
                raise ValueError("numeric fact exceeds its bounded display representation")
            data["value"] = number
        elif data.get("kind") == "date":
            if type(raw) is date:
                pass
            elif isinstance(raw, str):
                data["value"] = date.fromisoformat(raw)
            else:
                raise ValueError("date fact requires an exact calendar date")
        elif data.get("kind") == "text" and not isinstance(raw, str):
            raise ValueError("text fact requires original text")
        return data

    @model_validator(mode="after")
    def validate_kind(self) -> Self:
        expected = {"number": Decimal, "date": date, "text": str}[self.kind]
        if self.value is not None and type(self.value) is not expected:
            raise ValueError("fact value differs from its declared type")
        if self.kind != "number" and self.unit:
            raise ValueError("only numeric facts may have a display unit")
        if isinstance(self.value, str) and len(self.value.encode()) > 4096:
            raise ValueError("text fact exceeds its bounded display representation")
        return self

    @property
    def display_value(self) -> str:
        if self.value is None:
            return "未知"
        if isinstance(self.value, Decimal):
            with localcontext() as context:
                context.prec = 8192
                number = self.value * 100 if self.unit == "%" else self.value
                return f"{number:,.{self.decimals}f}{self.unit}"
        if type(self.value) is date:
            return self.value.isoformat()
        return str(self.value)
