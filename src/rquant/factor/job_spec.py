"""Pure input contract for exploratory retrospective factor evaluation jobs."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.factor.definition import FactorDefinition
from rquant.factor.historical_adapter import HistoricalFactorAdapterRequest
from rquant.factor_snapshot_admission import FactorSnapshotAdmissionRequest
from rquant.strict_json import canonical_json_bytes

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")


def _definition_sha256(definition: FactorDefinition) -> str:
    return hashlib.sha256(
        canonical_json_bytes(definition.model_dump(mode="json", round_trip=True))
    ).hexdigest()


class FactorEvaluationJobSpec(BaseModel):
    """A bounded request; source availability is checked by a later runner."""

    model_config = _IMMUTABLE

    job_type: Literal["factor_eval"] = "factor_eval"
    schema_version: Literal[1] = 1
    research_status: Literal["exploratory"] = "exploratory"
    result_kind: Literal["research_diagnostic"] = "research_diagnostic"
    code_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    admission_request: FactorSnapshotAdmissionRequest
    adapter_request: HistoricalFactorAdapterRequest
    definition_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    deadline: AwareDatetime

    @field_validator("adapter_request")
    @classmethod
    def _utc_research_as_of(
        cls, value: HistoricalFactorAdapterRequest
    ) -> HistoricalFactorAdapterRequest:
        return value.model_copy(update={"as_of": value.as_of.astimezone(UTC)})

    @field_validator("deadline")
    @classmethod
    def _utc_deadline(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _validate_bound_inputs(self) -> FactorEvaluationJobSpec:
        admission = self.admission_request
        adapter = self.adapter_request
        if (
            admission.start_date != adapter.query_start_date
            or admission.end_date != adapter.query_end_date
        ):
            raise ValueError("factor admission must match the adapter query range")
        if self.deadline <= adapter.as_of:
            raise ValueError("factor deadline must follow the research as_of")
        if self.definition_content_sha256 != _definition_sha256(adapter.definition):
            raise ValueError("factor definition content SHA-256 differs from its version")
        return self

    def model_copy(self, *, update: Mapping[str, object] | None = None, deep: bool = False) -> Self:
        if not update:
            return super().model_copy(deep=deep)
        payload = self.model_dump(mode="python", round_trip=True)
        payload.update(update)
        validated = type(self).model_validate(payload)
        return validated.model_copy(deep=True) if deep else validated

    @property
    def spec_sha256(self) -> str:
        """Canonical digest of every declared job input and fixed research claim."""
        return hashlib.sha256(
            canonical_json_bytes(self.model_dump(mode="json", round_trip=True))
        ).hexdigest()
