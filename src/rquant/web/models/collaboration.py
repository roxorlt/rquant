"""Typed C15 operations on the original verified private PageControl ingress."""

from __future__ import annotations

from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from rquant.collaboration_commands import SetUserRoleCommand, SetUserRoleRequest
from rquant.collaboration_roles import RoleName, Sha256, UserId
from rquant.command_audit_projection import CommandAuditQuery
from rquant.minute_backtest_parameter_study_commands import SubmitMinuteParameterStudy
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.sealed_result_ownership import SealedArtifactFact


class CollaborationMe(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    available: bool
    mode: Literal["legacy", "enforced"]
    username: UserId | None = None
    role: RoleName | None = None
    revision: int | None = None
    state_sha256: Sha256 | None = None
    can_manage_users: bool = False
    can_research: bool = False
    can_read_audit: bool = False
    message: str | None = None


class CollaborationRoleSubmit(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    command: SetUserRoleCommand
    issuance_proof: Sha256


class CollaborationRoleLookup(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    command: SetUserRoleCommand


class ResultOwnerQuery(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    domain: Literal["factor", "portfolio", "strategy", "minute"]
    job_id: str = Field(min_length=32, max_length=36)
    spec_hash: Sha256

    @model_validator(mode="after")
    def actual_original_id(self) -> Self:
        if (UUID(self.job_id).hex if self.domain == "factor" else str(UUID(self.job_id))) != self.job_id:
            raise ValueError("original result domain identifier differs")
        return self


class ResultOwnerProof(ResultOwnerQuery):
    owner_id: UserId
    command_id: str
    command_sha256: Sha256
    effect_sha256: Sha256
    worker_owner_id: str


class MinuteStudyJournalQuery(RuntimeContractModel):
    command_id: UUID
    include_admission: bool = True


class MinuteStudyJournalFact(RuntimeContractModel):
    command: SubmitMinuteParameterStudy
    command_hash: Sha256
    status: Literal["pending", "processing", "succeeded", "failed", "ambiguous"]
    enqueued_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime | None
    result: JsonValue | None
    admission_json: str | None = Field(default=None, max_length=1024 * 1024)


class MinuteReportOwnerQuery(RuntimeContractModel):
    job_id: str = Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    spec_hash: Sha256
    artifact: SealedArtifactFact | None = None

    @model_validator(mode="after")
    def exact_artifact(self) -> Self:
        if self.artifact is not None and (
            self.artifact.domain, self.artifact.job_id, self.artifact.spec_hash, self.artifact.complete
        ) != ("minute", self.job_id, self.spec_hash, True):
            raise ValueError("complete minute report artifact differs from its exact job")
        return self


class CollaborationPrivateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1]
    operation: Literal["me", "users", "prepare_role", "submit_role", "lookup_role", "authorize_command", "audit", "result_owner", "study_journal", "minute_report_owner"]
    authenticated_actor_id: UserId
    role_request: SetUserRoleRequest | None = None
    role_submission: CollaborationRoleSubmit | None = None
    role_lookup: CollaborationRoleLookup | None = None
    original_command: dict[str, JsonValue] | None = None
    audit_query: CommandAuditQuery | None = None
    owner_query: ResultOwnerQuery | None = None
    study_query: MinuteStudyJournalQuery | None = None
    minute_owner_query: MinuteReportOwnerQuery | None = None

    @field_validator("schema_version", mode="before")
    @classmethod
    def exact_private_version(cls, value: object) -> int:
        if type(value) is not int or value != 1:
            raise ValueError("unknown private collaboration request version")
        return value

    @model_validator(mode="after")
    def exact_operation(self) -> Self:
        fields = {"prepare_role": "role_request", "submit_role": "role_submission",
            "lookup_role": "role_lookup", "authorize_command": "original_command",
            "audit": "audit_query", "result_owner": "owner_query", "study_journal": "study_query",
            "minute_report_owner": "minute_owner_query"}
        required = fields.get(self.operation)
        for name in fields.values():
            if (getattr(self, name) is not None) != (name == required):
                raise ValueError("private collaboration operation fields differ")
        return self


class RoleLookupData(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    found: bool
    receipt: dict[str, JsonValue] | None = None
