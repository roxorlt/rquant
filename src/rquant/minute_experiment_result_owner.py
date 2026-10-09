"""Original private experiment provenance for a complete minute result.

Install only with the original Registry, Lab reader and bound role authority.
The caller still reads all eight tables with its installed minute sealed reader,
then rechecks this owner before releasing a private response. These projections
are internal facts, never browser permits or a replacement for that full read.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.collaboration_commands import PageControlRoleAuthority
from rquant.collaboration_roles import RoleName, Sha256, UserId
from rquant.experiment_platform import (
    MAX_FAMILY_INPUT_BYTES,
    ExperimentChildAdmission,
    ExperimentFamilyRecord,
    ExperimentOuterGrant,
    ExperimentPreparationReceipt,
    NativeMinuteExperimentRequest,
    experiment_family_job,
    stable_experiment_interaction,
)
from rquant.experiment_platform_commands import RegisterExperimentFamily
from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority
from rquant.experiment_registry import ExperimentRegistryReadonlyReader
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import SubmitJobCommand
from rquant.lab_jobs import LabJobReader
from rquant.minute_backtest_formal import PreparedMinuteRequest
from rquant.runtime_contracts import canonical_sha256
from rquant.sealed_result_ownership import SealedArtifactFact
from rquant.strategy_promotion_walk_forward import NativeStrategyPromotionWalkForwardPlan

MAX_OWNER_PROOF_BYTES = 64 * 1024
_REPORT_PATH = "/api/v1/backtests/minute-runtime/runs/{job_id}/report.html"


class MinuteExperimentProvenance(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )
    contract: Literal["minute-experiment-provenance/v1"] = "minute-experiment-provenance/v1"
    owner_id: UserId
    job_id: UUID
    spec_hash: Sha256
    family_id: str = Field(min_length=1, max_length=100)
    family_request_id: UUID
    family_body_hash: Sha256
    family_record_hash: Sha256
    child_request_id: UUID
    child_content_hash: Sha256
    child_record_hash: Sha256
    experiment_id: Sha256
    preparation_hash: Sha256
    full_input_hash: Sha256
    source_identity: Sha256
    source_kind: Literal["family_registration", "walk_forward", "outer_grant"]
    source_request_hash: Sha256
    source_record_hash: Sha256
    fold_index: int | None = Field(default=None, ge=1, le=6)
    outer_grant_id: Sha256 | None = None
    parent_family_id: str | None = Field(default=None, max_length=100)
    manifest_hash: Sha256
    complete_result_hash: Sha256
    role: RoleName
    role_revision: int = Field(ge=1, le=2**63 - 1)
    role_state_hash: Sha256
    outbox_identity: Sha256
    registry_revision_hash: Sha256
    lab_revision_hash: Sha256
    publication_revision_hash: Sha256
    content_sha256: Sha256

    @model_validator(mode="after")
    def exact_provenance(self) -> Self:
        if self.source_kind == "family_registration" and (
            self.source_request_hash != self.family_body_hash
            or self.source_record_hash != self.family_record_hash
            or self.fold_index is not None
            or self.outer_grant_id is not None
            or self.parent_family_id is not None
        ):
            raise ValueError("original family provenance differs")
        if self.source_kind == "walk_forward" and (
            self.fold_index is None
            or self.outer_grant_id is not None
            or self.parent_family_id is not None
        ):
            raise ValueError("original WF provenance needs its actual fold")
        if self.source_kind == "outer_grant" and (
            self.outer_grant_id is None
            or self.parent_family_id is None
            or self.fold_index is not None
            or self.source_record_hash != self.family_body_hash
        ):
            raise ValueError("original outer provenance needs its actual grant")
        _require_digest(self)
        return self


class MinuteExperimentSealedOwnerProof(SealedArtifactFact):
    """A separate owner branch; it is not a submit_minute_replay binding."""

    contract: Literal["minute-experiment-sealed-owner/v1"] = "minute-experiment-sealed-owner/v1"
    owner_id: UserId
    provenance: MinuteExperimentProvenance
    content_sha256: Sha256

    @model_validator(mode="after")
    def exact_sealed_owner(self) -> Self:
        _require_artifact(self.provenance, self)
        if self.owner_id != self.provenance.owner_id:
            raise ValueError("minute original provenance owner differs")
        _require_digest(self)
        return self


def _require_digest(value: MinuteExperimentProvenance | MinuteExperimentSealedOwnerProof) -> None:
    if value.content_sha256 != canonical_sha256(
        value.model_dump(mode="python", exclude={"content_sha256"})
    ):
        raise ValueError("minute experiment owner digest differs")
    if len(value.model_dump_json().encode()) > MAX_OWNER_PROOF_BYTES:
        raise ValueError("minute experiment owner exceeds the existing cell budget")


def _require_artifact(proof: MinuteExperimentProvenance, artifact: SealedArtifactFact) -> None:
    if not artifact.complete or (
        artifact.domain,
        artifact.job_id,
        artifact.spec_hash,
        artifact.private_owner,
        artifact.manifest_hash,
        artifact.complete_result_hash,
        artifact.full_artifact_hash,
        artifact.input_hash,
    ) != (
        "minute",
        str(proof.job_id),
        proof.spec_hash,
        proof.owner_id,
        proof.manifest_hash,
        proof.complete_result_hash,
        proof.complete_result_hash,
        proof.full_input_hash,
    ):
        raise PermissionError("complete minute artifact differs from its original experiment owner")


def require_minute_experiment_owner(
    binding: MinuteExperimentSealedOwnerProof,
    *,
    requester: str,
    current_artifact: SealedArtifactFact,
) -> MinuteExperimentSealedOwnerProof:
    """Pure equality checks; the installed caller must also recheck the original owner."""
    if (
        type(binding) is not MinuteExperimentSealedOwnerProof
        or type(current_artifact) is not SealedArtifactFact
    ):
        raise PermissionError("the exact independent minute experiment branch is required")
    checked = MinuteExperimentSealedOwnerProof.model_validate(binding.model_dump(mode="python"))
    artifact = SealedArtifactFact.model_validate(current_artifact.model_dump(mode="python"))
    if checked.owner_id != requester or any(
        getattr(checked, name) != getattr(artifact, name)
        for name in SealedArtifactFact.model_fields
    ):
        raise PermissionError("current requester or complete minute artifact differs")
    return checked


def _revision(path: Path) -> tuple[tuple[int, int, int, int, int] | None, ...]:
    values = []
    for suffix in ("", "-wal"):
        try:
            info = Path(f"{path}{suffix}").stat(follow_symlinks=False)
        except FileNotFoundError:
            values.append(None)
        else:
            values.append(
                (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            )
    return tuple(values)


def _publication_revision(prepared: PreparedMinuteRequest) -> str:
    reference = prepared.published.reference
    return canonical_sha256(
        tuple(_revision(item.path) for item in (reference.receipt, reference.source))
    )


def _source(
    connection: sqlite3.Connection,
    family: ExperimentFamilyRecord,
    child: ExperimentChildAdmission,
    prepared: ExperimentPreparationReceipt,
) -> dict[str, object]:
    if family.phase == "outer":
        rows = connection.execute(
            "SELECT * FROM experiment_outer_grant WHERE owner=? AND request_id=? LIMIT 2",
            (family.owner, str(family.request_id)),
        ).fetchall()
        if len(rows) != 1:
            raise PermissionError("original outer grant is unavailable")
        row = rows[0]
        grant = ExperimentOuterGrant.model_validate_json(row["payload_json"])
        expected_config = type(grant.config).model_validate(
            grant.config.model_dump(mode="python")
            | {
                "start_date": grant.outer_range.start_date,
                "end_date": grant.outer_range.end_date,
            }
        )
        if (
            (row["grant_id"], row["owner"], row["request_id"], row["range_start"], row["range_end"])
            != (
                grant.grant_id,
                family.owner,
                str(family.request_id),
                grant.outer_range.start_date.isoformat(),
                grant.outer_range.end_date.isoformat(),
            )
            or grant.owner != family.owner
            or grant.grant_id
            != canonical_sha256({"owner": grant.owner, "request_id": grant.request_id})
            or family.family_id != "experiment-outer:" + grant.grant_id
            or family.parent_family_id != grant.family_id
            or family.body_hash != canonical_sha256(grant)
            or family.actual_configurations != (expected_config,)
            or prepared.configuration != expected_config
            or family.policy != grant.policy
            or family.registered_at != grant.admitted_at
            or (grant.source_key, grant.source_version, grant.source_identity)
            != (
                expected_config.selection.source_key,
                expected_config.selection.source_version,
                prepared.source_identity,
            )
        ):
            raise PermissionError("original outer grant and admitted family differ")
        return {
            "source_kind": "outer_grant",
            "source_request_hash": grant.body_hash,
            "source_record_hash": canonical_sha256(grant),
            "outer_grant_id": grant.grant_id,
            "parent_family_id": grant.family_id,
        }
    request = family.request
    if (
        not isinstance(request, NativeMinuteExperimentRequest)
        or family.parent_family_id is not None
    ):
        raise PermissionError("only original native minute experiment families are supported")
    if family.family_id != "experiment-search:" + canonical_sha256(
        {"owner": family.owner, "request": family.request_id}
    ):
        raise PermissionError("original registration identity differs")
    if request.walk_forward_command_id is None:
        return {
            "source_kind": "family_registration",
            "source_request_hash": family.body_hash,
            "source_record_hash": canonical_sha256(family),
        }
    rows = connection.execute(
        "SELECT * FROM strategy_walk_forward_plan WHERE request_id=? LIMIT 2",
        (str(request.walk_forward_command_id),),
    ).fetchall()
    if len(rows) != 1:
        raise PermissionError("original native WF plan is unavailable")
    row = rows[0]
    if len(row["plan_json"].encode()) > 128 * 1024:
        raise ValueError("original WF plan exceeds its existing budget")
    plan = NativeStrategyPromotionWalkForwardPlan.model_validate_json(row["plan_json"])
    command = RegisterExperimentFamily(
        command_id=plan.request.command_id,
        requested_at=plan.request.requested_at,
        actor_id=family.owner,
        request=plan.family_request(),
    )
    folds = tuple(fold for fold in plan.folds if fold.job_id == child.job_id)
    if (
        (row["request_id"], row["actor_id"], row["request_hash"], row["plan_hash"])
        != (str(family.request_id), family.owner, plan.request.request_hash, plan.fingerprint)
        or plan.request.target.owner_id != family.owner
        or plan.family_request() != request
        or request.walk_forward_command_id != family.request_id
        or request.walk_forward_plan_hash != plan.fingerprint
        or family.body_hash != canonical_sha256(command)
        or len(folds) != 1
        or folds[0].configuration != prepared.configuration
        or folds[0].index != prepared.index + 1
    ):
        raise PermissionError("original native WF plan differs from this admitted fold")
    return {
        "source_kind": "walk_forward",
        "source_request_hash": plan.request.request_hash,
        "source_record_hash": plan.fingerprint,
        "fold_index": folds[0].index,
    }


class MinuteExperimentResultOwner:
    def __init__(
        self,
        *,
        private_authority: ExperimentPrivateResultAuthority,
        jobs: LabJobReader,
        roles: PageControlRoleAuthority,
    ) -> None:
        if (
            type(private_authority) is not ExperimentPrivateResultAuthority
            or type(private_authority.registry) is not ExperimentRegistryReadonlyReader
            or type(jobs) is not LabJobReader
            or type(roles) is not PageControlRoleAuthority
        ):
            raise TypeError("original installed experiment, Lab and role authorities are required")
        if roles.mode != "enforced" or roles.require_outbox_identity() is None:
            raise PermissionError("current roles must be bound to the original private outbox")
        self.private_authority, self.jobs, self.roles = private_authority, jobs, roles

    def _role(self, actor: str) -> dict[str, object]:
        with self.roles.locked(read_only=True):
            role = self.roles.require_operation(actor, "GET", _REPORT_PATH)
            state = self.roles.read_state()
            return {
                "role": role,
                "role_revision": state.revision,
                "role_state_hash": state.content_sha256,
                "outbox_identity": self.roles.require_outbox_identity(),
            }

    def read(
        self, job_id: UUID, *, authenticated_actor_id: str, expected_spec_hash: str
    ) -> MinuteExperimentProvenance:
        roles = self._role(authenticated_actor_id)
        registry = self.private_authority.registry
        self.private_authority.refresh_live_identity()
        # Original readonly setup may create an empty WAL; pin content after that setup.
        with self.jobs._read_snapshot(label="minute experiment owner setup"):
            lab_revision = self.jobs._storage_revision()
            authority = self.jobs.get_artifact_preview_authority(job_id)
            if authority is None:
                raise LookupError("original succeeded sealed minute job is unavailable")
            job = authority.job
            if (
                job.spec_hash != expected_spec_hash
                or job.spec.parameters.strategy_name != "minute_runtime_replay"
            ):
                raise PermissionError("original minute job/spec differs")
            with registry._read_snapshot() as connection:
                revision = _revision(registry.path)
                rows = connection.execute(
                    "SELECT a.job_id,a.experiment_id,a.hypothesis_family,a.owner,"
                    "a.payload_json AS child_json,r.request_id,r.owner AS family_owner,"
                    "r.body_hash,r.family_id,r.state,r.payload_json AS family_json "
                    "FROM experiment_child_admission a JOIN experiment_family_request r "
                    "ON r.family_id=a.hypothesis_family WHERE a.job_id=? LIMIT 2",
                    (str(job_id),),
                ).fetchall()
                if len(rows) != 1:
                    raise PermissionError("the original family child is unavailable")
                row = rows[0]
                family = ExperimentFamilyRecord.model_validate_json(row["family_json"])
                child = ExperimentChildAdmission.model_validate_json(row["child_json"])
                if (
                    (
                        row["job_id"],
                        row["experiment_id"],
                        row["hypothesis_family"],
                        row["owner"],
                        row["request_id"],
                        row["family_owner"],
                        row["body_hash"],
                        row["family_id"],
                        row["state"],
                    )
                    != (
                        str(child.job_id),
                        child.experiment_id,
                        child.family_id,
                        child.owner,
                        str(family.request_id),
                        family.owner,
                        family.body_hash,
                        family.family_id,
                        family.state,
                    )
                    or child.owner != authenticated_actor_id
                    or family.owner != authenticated_actor_id
                    or family.state != "ready"
                    or child.publish_grant_seq is None
                    or child.cancel_state == "before_publication"
                    or job.spec.experiment is None
                    or child.experiment_id != job.spec.experiment.spec.experiment_id
                    or not isinstance(family.request, NativeMinuteExperimentRequest)
                ):
                    raise PermissionError("original family, child and authenticated owner differ")
                prepared_rows = connection.execute(
                    "SELECT child_index,payload_json FROM experiment_prepared_child "
                    "WHERE family_id=? AND "
                    "json_extract(payload_json,'$.prepared.formal_plan.spec.experiment_id')=? "
                    "LIMIT 2",
                    (family.family_id, child.experiment_id),
                ).fetchall()
                if (
                    len(prepared_rows) != 1
                    or len(prepared_rows[0]["payload_json"].encode()) > MAX_FAMILY_INPUT_BYTES
                ):
                    raise PermissionError("original bounded preparation is unavailable")
                prepared = ExperimentPreparationReceipt.model_validate_json(
                    prepared_rows[0]["payload_json"]
                )
                if (
                    type(prepared.prepared) is not PreparedMinuteRequest
                    or (prepared.family_id, prepared.owner, prepared.index)
                    != (family.family_id, authenticated_actor_id, prepared_rows[0]["child_index"])
                    or prepared.index >= len(family.actual_configurations)
                    or prepared.configuration != family.actual_configurations[prepared.index]
                    or experiment_family_job(family, prepared.index) != job_id
                    or LabCommandSubmissionFacade._request_id(
                        stable_experiment_interaction(
                            child.owner, family.request_id, prepared.index
                        )
                    )
                    != child.request_id
                ):
                    raise PermissionError("original preparation identity differs")
                source = _source(connection, family, child, prepared)
                publication_revision = _publication_revision(prepared.prepared)
                command = self.jobs.get_command(child.request_id)
                if (
                    command is None
                    or type(command.envelope.command) is not SubmitJobCommand
                    or (
                        command.request_id,
                        command.content_hash,
                        command.job_id,
                        command.command_type,
                        command.receipt.status,
                        command.receipt.reason,
                    )
                    != (
                        child.request_id,
                        child.command_content_hash,
                        job_id,
                        "submit",
                        "applied",
                        "submitted_v3_owned",
                    )
                    or command.envelope.command.spec != job.spec
                ):
                    raise PermissionError("original Lab child has no matching applied submit")
                authorized = self.private_authority.authorize(job, authenticated_actor_id)
                if authorized != prepared:
                    raise PermissionError("original private authority read a different preparation")
                if (
                    _publication_revision(prepared.prepared) != publication_revision
                    or _revision(registry.path) != revision
                    or self.jobs._storage_revision() != lab_revision
                    or self._role(authenticated_actor_id) != roles
                ):
                    raise PermissionError("original owner, metadata or source changed during read")
                values = {
                    "owner_id": child.owner,
                    "job_id": job_id,
                    "spec_hash": job.spec_hash,
                    "family_id": family.family_id,
                    "family_request_id": family.request_id,
                    "family_body_hash": family.body_hash,
                    "family_record_hash": canonical_sha256(family),
                    "child_request_id": child.request_id,
                    "child_content_hash": command.content_hash,
                    "child_record_hash": canonical_sha256(child),
                    "experiment_id": child.experiment_id,
                    "preparation_hash": canonical_sha256(
                        prepared.model_dump(mode="json", exclude_computed_fields=True)
                    ),
                    "full_input_hash": prepared.prepared.frozen.full_input_hash,
                    "source_identity": prepared.source_identity,
                    "manifest_hash": authority.evidence.manifest_hash,
                    "complete_result_hash": authority.evidence.complete_result_hash,
                    "registry_revision_hash": canonical_sha256(revision),
                    "lab_revision_hash": canonical_sha256(lab_revision),
                    "publication_revision_hash": publication_revision,
                    **roles,
                    **source,
                }
        return MinuteExperimentProvenance.model_validate(
            values
            | {
                "content_sha256": canonical_sha256(
                    {
                        "contract": "minute-experiment-provenance/v1",
                        "fold_index": None,
                        "outer_grant_id": None,
                        "parent_family_id": None,
                        **values,
                    }
                )
            }
        )

    def recheck(
        self, provenance: MinuteExperimentProvenance, *, authenticated_actor_id: str
    ) -> None:
        if type(provenance) is not MinuteExperimentProvenance:
            raise PermissionError("the captured original minute provenance is required")
        checked = MinuteExperimentProvenance.model_validate(provenance.model_dump(mode="python"))
        if (
            self.read(
                checked.job_id,
                authenticated_actor_id=authenticated_actor_id,
                expected_spec_hash=checked.spec_hash,
            )
            != checked
        ):
            raise PermissionError("the original minute provenance changed after capture")

    def bind_sealed(
        self,
        provenance: MinuteExperimentProvenance,
        artifact: SealedArtifactFact,
        *,
        authenticated_actor_id: str,
    ) -> MinuteExperimentSealedOwnerProof:
        if (
            type(provenance) is not MinuteExperimentProvenance
            or type(artifact) is not SealedArtifactFact
        ):
            raise PermissionError("exact original provenance and complete artifact are required")
        checked = MinuteExperimentProvenance.model_validate(provenance.model_dump(mode="python"))
        artifact = SealedArtifactFact.model_validate(artifact.model_dump(mode="python"))
        _require_artifact(checked, artifact)
        self.recheck(checked, authenticated_actor_id=authenticated_actor_id)
        values = {
            **artifact.model_dump(mode="python"),
            "owner_id": checked.owner_id,
            "provenance": checked,
            "contract": "minute-experiment-sealed-owner/v1",
        }
        return MinuteExperimentSealedOwnerProof.model_validate(
            values | {"content_sha256": canonical_sha256(values)}
        )
