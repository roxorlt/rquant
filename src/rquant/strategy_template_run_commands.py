"""Ownerless run intent and immutable original Lab plan references."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from rquant.backtest.contracts import Sha256
from rquant.experiment_registry import FormalExperimentPlan
from rquant.research_run_spec import ResearchRunSpec, _decimal_components, _parse_decimal
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strategy_authoring_commands import (
    ArchiveStrategyTemplate,
    OwnedArchiveStrategyTemplate,
    OwnedSaveStrategyTemplate,
    SaveStrategyTemplate,
    StrategyAuthoringIdentity,
    StrategyTemplateHead,
    _TemplateCommand,
)
from rquant.strategy_template import TEMPLATE_ID_PATTERN


class RunStrategyTemplate(_TemplateCommand):
    kind: Literal["run_strategy_template"] = "run_strategy_template"
    strategy_id: str = Field(pattern=TEMPLATE_ID_PATTERN)
    head: StrategyTemplateHead
    expected_head: StrategyTemplateHead
    start_date: date
    end_date: date
    initial_cash: Decimal = Field(gt=0, le=Decimal("1000000000000"), allow_inf_nan=False)

    @field_validator("initial_cash", mode="before")
    @classmethod
    def bounded_cash(cls, value: object) -> Decimal:
        parsed = _parse_decimal(value, field_name="initial cash")
        if _decimal_components(parsed, field_name="initial cash")[2] < -2:
            raise ValueError("initial cash must be exact to a cent")
        return parsed

    @model_validator(mode="after")
    def bounded_original(self) -> Self:
        if not 1 <= (self.end_date - self.start_date).days + 1 <= 5 * 366:
            raise ValueError("template run date range exceeds the research budget")
        if self.head.version > self.expected_head.version:
            raise ValueError("selected template version is ahead of the expected current head")
        if (
            len(
                self.model_dump_json(exclude={"owner_id", "metadata_identity", "accepted"}).encode()
            )
            > 4096
        ):
            raise ValueError("template run request exceeds byte budget")
        return self


class AcceptedStrategyTemplateRun(RuntimeContractModel):
    owner_id: str = Field(min_length=1, max_length=128)
    request: RunStrategyTemplate
    metadata_identity: StrategyAuthoringIdentity
    accepted_at: AwareUtcDatetime
    spec: ResearchRunSpec
    plan: FormalExperimentPlan

    @model_validator(mode="after")
    def original_plan_binding(self) -> Self:
        request, spec = self.request, self.spec
        parameters = {item.name: item.value for item in spec.parameters.arguments}
        execution, experiment = spec.strategy_execution, spec.experiment
        if spec.schema_version != 3 or execution is None or experiment is None:
            raise ValueError("template accepted run requires original v3 ownership")
        if (
            spec.parameters.strategy_name,
            spec.parameters.start_date,
            spec.parameters.end_date,
            parameters.get("owner_id"),
            parameters.get("request_id"),
            parameters.get("strategy_id"),
            parameters.get("version"),
            parameters.get("registration_fingerprint"),
            parameters.get("record_hash"),
            parameters.get("spec_fingerprint"),
        ) != (
            request.strategy_id,
            request.start_date,
            request.end_date,
            self.owner_id,
            request.command_id,
            request.strategy_id,
            request.head.version,
            request.head.registration_fingerprint,
            request.head.record_hash,
            request.head.spec_fingerprint,
        ):
            raise ValueError(
                "template accepted run differs from original owner, request or definition"
            )
        if (
            execution.strategy_id,
            execution.strategy_version,
            execution.strategy_definition_fingerprint,
            execution.definition_registration_record_hash,
            execution.strategy_spec_fingerprint,
        ) != (
            request.strategy_id,
            request.head.version,
            request.head.registration_fingerprint,
            request.head.record_hash,
            request.head.spec_fingerprint,
        ):
            raise ValueError("template original execution identity differs")
        if (
            self.plan.schema_version != 2
            or self.plan.plan_id != experiment.formal_plan_id
            or self.plan.spec != experiment.spec
            or (
                self.plan.strategy_definition_fingerprint,
                self.plan.definition_registration_record_hash,
                self.plan.hypothesis_variant,
            )
            != (
                request.head.registration_fingerprint,
                request.head.record_hash,
                experiment.hypothesis_variant,
            )
        ):
            raise ValueError("template original experiment plan differs")
        if self.accepted_at > spec.deadline or self.accepted_at < execution.definition_available_at:
            raise ValueError("template accepted run is outside its original time bounds")
        if len(self.model_dump_json().encode()) > 32 * 1024:
            raise ValueError("template accepted plan exceeds the original command byte budget")
        return self


class OwnedRunStrategyTemplate(RunStrategyTemplate):
    owner_id: str
    metadata_identity: StrategyAuthoringIdentity
    accepted: AcceptedStrategyTemplateRun

    @model_validator(mode="after")
    def owned_original(self) -> Self:
        original = RunStrategyTemplate.model_validate(
            self.model_dump(mode="python", exclude={"owner_id", "metadata_identity", "accepted"})
        )
        if (
            self.accepted.request != original
            or self.accepted.owner_id != self.owner_id
            or self.accepted.metadata_identity != self.metadata_identity
        ):
            raise ValueError("owned template run differs from the original accepted plan")
        return self

    def original(self) -> RunStrategyTemplate:
        return RunStrategyTemplate.model_validate(
            self.model_dump(mode="python", exclude={"owner_id", "metadata_identity", "accepted"})
        )


class StrategyTemplateRunReceipt(RuntimeContractModel):
    action: Literal["run"] = "run"
    owner_id: str
    command_id: str
    strategy_id: str = Field(pattern=TEMPLATE_ID_PATTERN)
    head: StrategyTemplateHead
    original_request_hash: Sha256
    job_id: UUID
    lab_request_id: UUID
    lab_content_hash: Sha256
    spec_hash: Sha256
    completed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def original_job_id(self) -> Self:
        if self.job_id != UUID(self.command_id):
            raise ValueError("template run receipt does not reference the original job")
        return self


StrategyTemplateCommandValue = SaveStrategyTemplate | ArchiveStrategyTemplate | RunStrategyTemplate
OwnedStrategyTemplateCommandValue = (
    OwnedSaveStrategyTemplate | OwnedArchiveStrategyTemplate | OwnedRunStrategyTemplate
)
