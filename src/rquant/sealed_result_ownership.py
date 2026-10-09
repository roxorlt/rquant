"""Pure checks for original command/effect/job/sealed-result binding.

These typed projections and their hashes do not attest a signature, private UID,
trusted socket ingress, or a persisted owner. The original adapter must verify
those sources and recheck current roles before publishing and every private read.
An absent historic actor remains unknown; an effect worker is never the user.
"""

from __future__ import annotations

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.collaboration_roles import Sha256, UserId
from rquant.runtime_contracts import canonical_sha256

ResultDomain = Literal["factor", "portfolio", "strategy", "minute"]
CommandId = Annotated[str, Field(min_length=36, max_length=36)]
_SUBMIT_KINDS: dict[ResultDomain, str] = {
    "factor": "submit_factor_run",
    "portfolio": "submit_portfolio_backtest",
    "strategy": "run_strategy_template",
    "minute": "submit_minute_replay",
}


class _FactModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )
    schema_version: Literal[1] = 1

    @field_validator("schema_version", mode="before")
    @classmethod
    def exact_version(cls, value: object) -> int:
        if type(value) is not int or value != 1:
            raise ValueError("unknown original binding fact version")
        return value

    @field_validator("command_id", "job_id", "submitted_job_id", check_fields=False)
    @classmethod
    def canonical_identifier(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("original identifier must be a canonical UUID")
        return value


class OriginalSubmissionFact(_FactModel):
    domain: ResultDomain
    command_id: CommandId
    command_kind: str = Field(min_length=1, max_length=80)
    command_sha256: Sha256
    actor_id: UserId | None
    job_id: CommandId
    spec_hash: Sha256
    origin_verified: bool = False

    @model_validator(mode="after")
    def original_kind(self) -> Self:
        if self.command_kind != _SUBMIT_KINDS[self.domain]:
            raise ValueError("submission domain and original command kind differ")
        return self


class OriginalEffectFact(_FactModel):
    command_id: CommandId
    command_kind: str = Field(min_length=1, max_length=80)
    command_sha256: Sha256
    status: Literal["started", "succeeded", "failed", "ambiguous"]
    submitted_job_id: CommandId
    submitted_spec_hash: Sha256
    # Original PageControlEffectRecord.owner_id is a claim worker, not an actor.
    worker_owner_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._@:-]+$"
    )


class SealedJobFact(_FactModel):
    domain: ResultDomain
    job_id: CommandId
    spec_hash: Sha256
    status: str = Field(min_length=1, max_length=32)
    manifest_hash: Sha256
    complete_result_hash: Sha256


class SealedArtifactFact(_FactModel):
    """Full hashes already verified by the existing private sealed-result reader."""

    domain: ResultDomain
    job_id: CommandId
    spec_hash: Sha256
    manifest_hash: Sha256
    complete_result_hash: Sha256
    full_artifact_hash: Sha256
    result_payload_hash: Sha256
    input_hash: Sha256
    display_hash: Sha256 | None = None
    # From the original verified PortfolioBundle.html_sha256, never a caller URL.
    html_sha256: Sha256 | None = None
    complete: bool
    private_owner: UserId | None = None


class SealedOwnerBinding(SealedArtifactFact):
    owner_id: UserId
    command_id: CommandId
    command_kind: str = Field(min_length=1, max_length=80)
    command_sha256: Sha256
    original_chain_sha256: Sha256
    content_sha256: Sha256

    @model_validator(mode="after")
    def verify_owner_binding(self) -> Self:
        if not self.complete or self.command_kind != _SUBMIT_KINDS[self.domain]:
            raise ValueError("binding needs the complete original domain result")
        if self.private_owner is not None and self.private_owner != self.owner_id:
            raise ValueError("original private owner differs from submission actor")
        if self.content_sha256 != canonical_sha256(
            self.model_dump(mode="python", exclude={"content_sha256"})
        ):
            raise ValueError("sealed owner binding digest differs")
        return self


def bind_sealed_owner(
    submission: OriginalSubmissionFact | None,
    effect: OriginalEffectFact,
    job: SealedJobFact,
    artifact: SealedArtifactFact,
) -> SealedOwnerBinding | None:
    """Derive only from a proven original actor and a matching successful chain."""
    if submission is None:
        return None
    submission = OriginalSubmissionFact.model_validate(
        submission.model_dump(mode="python")
    )
    if submission.actor_id is None or not submission.origin_verified:
        return None
    effect = OriginalEffectFact.model_validate(effect.model_dump(mode="python"))
    job = SealedJobFact.model_validate(job.model_dump(mode="python"))
    artifact = SealedArtifactFact.model_validate(artifact.model_dump(mode="python"))
    original = (
        submission.command_id,
        submission.command_kind,
        submission.command_sha256,
        submission.job_id,
        submission.spec_hash,
    )
    accepted = (
        effect.command_id,
        effect.command_kind,
        effect.command_sha256,
        effect.submitted_job_id,
        effect.submitted_spec_hash,
    )
    if original != accepted or effect.status != "succeeded":
        raise ValueError("successful original effect does not bind the submission")
    if (submission.domain, submission.job_id, submission.spec_hash) != (
        job.domain,
        job.job_id,
        job.spec_hash,
    ):
        raise ValueError("completed job does not bind the original submission")
    if job.status != "succeeded" or not artifact.complete:
        raise ValueError("result has not been successfully sealed in full")
    sealed_job = (
        job.domain,
        job.job_id,
        job.spec_hash,
        job.manifest_hash,
        job.complete_result_hash,
    )
    sealed_artifact = (
        artifact.domain,
        artifact.job_id,
        artifact.spec_hash,
        artifact.manifest_hash,
        artifact.complete_result_hash,
    )
    if sealed_job != sealed_artifact:
        raise ValueError("full sealed result does not bind the completed job")
    if (
        artifact.private_owner is not None
        and artifact.private_owner != submission.actor_id
    ):
        raise ValueError("original private owner differs from original actor")
    body = artifact.model_dump(mode="python") | {
        "owner_id": submission.actor_id,
        "command_id": submission.command_id,
        "command_kind": submission.command_kind,
        "command_sha256": submission.command_sha256,
        "original_chain_sha256": canonical_sha256((submission, effect, job, artifact)),
    }
    return SealedOwnerBinding(**body, content_sha256=canonical_sha256(body))


def require_owned_sealed_result(
    binding: SealedOwnerBinding | None,
    *,
    requester: str,
    current_artifact: SealedArtifactFact,
) -> SealedOwnerBinding:
    """Owner intersection only; the caller must also enforce the current role."""
    try:
        if (
            type(binding) is not SealedOwnerBinding
            or type(current_artifact) is not SealedArtifactFact
        ):
            raise ValueError("original owner or current full artifact is absent")
        binding = SealedOwnerBinding.model_validate(binding.model_dump(mode="python"))
        current_artifact = SealedArtifactFact.model_validate(
            current_artifact.model_dump(mode="python")
        )
        if requester != binding.owner_id:
            raise ValueError("current user is not the original sealed-result owner")
        expected = {
            name: getattr(binding, name) for name in SealedArtifactFact.model_fields
        }
        if (
            not current_artifact.complete
            or current_artifact.model_dump(mode="python") != expected
        ):
            raise ValueError(
                "current full artifact differs from the bound original result"
            )
    except (ValueError, TypeError, AttributeError) as exc:
        raise PermissionError("current user cannot read this sealed result") from exc
    return binding
