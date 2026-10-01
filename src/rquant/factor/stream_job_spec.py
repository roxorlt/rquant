"""Explicit persisted v2 inputs: frozen raw scope and actual member manifest."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.factor.job_spec import FactorEvaluationJobSpec, _definition_sha256
from rquant.factor.member_archive import FactorMemberArchiveReference
from rquant.factor.stream_adapter import FactorStreamAdapterRequest
from rquant.factor.universe import Sha256
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads


class FactorStreamJobSpec(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    job_type: Literal["factor_eval"] = "factor_eval"
    schema_version: Literal[2] = 2
    research_status: Literal["exploratory"] = "exploratory"
    result_kind: Literal["research_diagnostic"] = "research_diagnostic"
    code_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    adapter_request: FactorStreamAdapterRequest
    member_archive: FactorMemberArchiveReference
    definition_content_sha256: Sha256
    deadline: AwareDatetime

    @field_validator("deadline")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _bindings(self) -> FactorStreamJobSpec:
        if self.definition_content_sha256 != _definition_sha256(
            self.adapter_request.formula.definition
        ):
            raise ValueError("factor definition content digest differs")
        if self.deadline <= self.adapter_request.formula.as_of:
            raise ValueError("factor deadline must follow research cutoff")
        return self

    def model_copy(self, *, update: Mapping[str, object] | None = None, deep: bool = False) -> Self:
        if not update:
            return super().model_copy(deep=deep)
        fields = self.model_dump(mode="python", round_trip=True)
        fields.update(update)
        return type(self).model_validate(fields)

    @property
    def spec_sha256(self) -> str:
        return hashlib.sha256(
            canonical_json_bytes(self.model_dump(mode="json", round_trip=True))
        ).hexdigest()


def decode_factor_job_spec(payload: object) -> FactorEvaluationJobSpec | FactorStreamJobSpec:
    """Version dispatch is explicit; missing or mixed payloads never fall back."""
    if not isinstance(payload, dict) or type(payload.get("schema_version")) is not int:
        raise ValueError("factor job spec requires an explicit version")
    version = payload["schema_version"]
    model = {1: FactorEvaluationJobSpec, 2: FactorStreamJobSpec}.get(version)
    if model is None:
        raise ValueError("unknown factor job spec version")
    return model.model_validate_json(canonical_json_bytes(payload))


def decode_factor_job_spec_json(data: str) -> FactorEvaluationJobSpec | FactorStreamJobSpec:
    if not 0 < len(data.encode("utf-8")) <= 2 * 1024 * 1024:
        raise ValueError("factor job spec exceeds byte budget")
    return decode_factor_job_spec(strict_canonical_json_loads(data))
