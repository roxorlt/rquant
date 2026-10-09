"""C15 typed client on the existing factor private peer; no filesystem writer."""

from __future__ import annotations

import http.client
from uuid import UUID
from collections.abc import Callable
from contextvars import ContextVar
from typing import TYPE_CHECKING

from pydantic import BaseModel

from rquant.collaboration_commands import CommandAuthorization, IssuedRolePreparation
from rquant.collaboration_roles import RoleState
from rquant.command_audit_projection import CommandAuditPage, CommandAuditQuery
from rquant.factor_definition_admission import FactorDefinitionAdmissionClient, _UnixHTTPConnection
from rquant.sealed_result_ownership import SealedArtifactFact, SealedOwnerBinding
from rquant.strict_json import strict_json_loads
from rquant.web.models.collaboration import (
    CollaborationMe,
    CollaborationPrivateRequest,
    CollaborationRoleLookup,
    CollaborationRoleSubmit,
    ResultOwnerProof,
    ResultOwnerQuery,
    RoleLookupData,
    MinuteStudyJournalFact,
    MinuteStudyJournalQuery,
    MinuteReportOwnerQuery,
)

if TYPE_CHECKING:
    from rquant.minute_experiment_result_owner import MinuteExperimentProvenance, MinuteExperimentSealedOwnerProof

COLLABORATION_ACTOR: ContextVar[str | None] = ContextVar("verified_web_collaboration_actor", default=None)
PrivateTransport = Callable[[CollaborationPrivateRequest], bytes]


class CollaborationUnavailableError(RuntimeError):
    pass


class CollaborationGateway(FactorDefinitionAdmissionClient):
    def __init__(self, *args: object, transport: PrivateTransport | None = None, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.private_transport = transport

    def _call(self, message: CollaborationPrivateRequest) -> bytes:
        if self.private_transport is not None:
            raw = self.private_transport(message)
            if type(raw) is not bytes or not 1 <= len(raw) <= 4 * 1024 * 1024:
                raise CollaborationUnavailableError("private response exceeds capacity")
            strict_json_loads(raw)
            return raw
        body = message.model_dump_json().encode()
        if len(body) > 1024 * 1024:
            raise ValueError("private request exceeds original command capacity")
        connection = _UnixHTTPConnection(self.socket_path, timeout_seconds=self.timeout_seconds,
            expected_service_uid=self.expected_service_uid, shared_gid=self.shared_gid, client_uid=self.client_uid)
        try:
            connection.request("POST", "/v1/collaboration", body=body, headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            lengths = response.headers.get_all("Content-Length", [])
            types = response.headers.get_all("Content-Type", [])
            if (len(lengths) != 1 or not lengths[0].isdecimal() or not 1 <= int(lengths[0]) <= 4 * 1024 * 1024
                    or types != ["application/json"] or response.getheader("Transfer-Encoding") is not None):
                raise ValueError("private response framing differs")
            raw = response.read(4 * 1024 * 1024 + 1)
            if len(raw) != int(lengths[0]):
                raise ValueError("private response length differs")
            value = strict_json_loads(raw)
            if response.status == 403 and value == {"error": "actor_forbidden"}:
                raise PermissionError("current permission is unavailable")
            if response.status == 404 and value == {"error": "not_found"}:
                raise LookupError("original command or owner is unavailable")
            if response.status == 409 and value in ({"error": "command_conflict"}, {"error": "rejected"}):
                raise ValueError("original request or current generation differs")
            if response.status != 200:
                raise CollaborationUnavailableError("private authority is unavailable")
            return raw
        except (OSError, TimeoutError, http.client.HTTPException) as exc:
            raise CollaborationUnavailableError("private authority is unavailable") from exc
        finally:
            connection.close()

    def request(self, operation: str, actor: str, **fields: object) -> bytes:
        message = CollaborationPrivateRequest(schema_version=1, operation=operation,
            authenticated_actor_id=actor, **fields)
        return self._call(message)

    def me(self, actor: str) -> CollaborationMe:
        result = CollaborationMe.model_validate_json(self.request("me", actor))
        if result.username != actor or not result.available or result.mode != "enforced" or result.role is None:
            raise CollaborationUnavailableError("private current role differs")
        return result

    def users(self, actor: str) -> RoleState:
        return RoleState.model_validate_json(self.request("users", actor))

    def prepare(self, actor: str, request: BaseModel) -> IssuedRolePreparation:
        return IssuedRolePreparation.model_validate_json(self.request("prepare_role", actor, role_request=request))

    def submit_role(self, actor: str, request: CollaborationRoleSubmit) -> bytes:
        return self.request("submit_role", actor, role_submission=request)

    def lookup_role(self, actor: str, request: CollaborationRoleLookup) -> RoleLookupData:
        return RoleLookupData.model_validate_json(self.request("lookup_role", actor, role_lookup=request))

    def audit(self, actor: str, query: CommandAuditQuery) -> CommandAuditPage:
        return CommandAuditPage.model_validate_json(self.request("audit", actor, audit_query=query))

    def study_journal(self, actor: str, *, command_id: UUID,
        include_admission: bool = True) -> MinuteStudyJournalFact:
        query = MinuteStudyJournalQuery(command_id=command_id, include_admission=include_admission)
        value = MinuteStudyJournalFact.model_validate_json(self.request("study_journal", actor, study_query=query))
        if (value.command.command_id, value.command.actor_id) != (str(command_id), actor):
            raise PermissionError("original study journal owner or UUID differs")
        if not include_admission and value.admission_json is not None:
            raise PermissionError("compact study journal unexpectedly contains its private admission")
        return value

    def result_owner(self, actor: str, *, domain: str, job_id: str, spec_hash: str) -> ResultOwnerProof:
        query = ResultOwnerQuery(domain=domain, job_id=job_id, spec_hash=spec_hash)
        proof = ResultOwnerProof.model_validate_json(self.request("result_owner", actor, owner_query=query))
        if proof.owner_id != actor or (proof.domain, proof.job_id, proof.spec_hash) != (domain, job_id, spec_hash):
            raise PermissionError("original result owner binding differs")
        return proof

    def minute_report_owner(self, actor: str, *, job_id: str, spec_hash: str,
        artifact: SealedArtifactFact | None = None,
    ) -> ResultOwnerProof | MinuteExperimentProvenance | MinuteExperimentSealedOwnerProof:
        from pydantic import TypeAdapter
        from rquant.minute_experiment_result_owner import MinuteExperimentProvenance, MinuteExperimentSealedOwnerProof

        query = MinuteReportOwnerQuery(job_id=job_id, spec_hash=spec_hash, artifact=artifact)
        proof = TypeAdapter(ResultOwnerProof | MinuteExperimentProvenance | MinuteExperimentSealedOwnerProof).validate_json(
            self.request("minute_report_owner", actor, minute_owner_query=query))
        if type(proof) is ResultOwnerProof:
            identity = (proof.domain, proof.job_id, proof.spec_hash, proof.owner_id)
        elif type(proof) is MinuteExperimentProvenance:
            if artifact is not None:
                raise PermissionError("complete family report requires its independent sealed owner proof")
            identity = ("minute", str(proof.job_id), proof.spec_hash, proof.owner_id)
        else:
            if artifact is None:
                raise PermissionError("family sealed proof lacks the caller's complete artifact")
            identity = (proof.domain, proof.job_id, proof.spec_hash, proof.owner_id)
            from rquant.minute_experiment_result_owner import require_minute_experiment_owner
            require_minute_experiment_owner(proof, requester=actor, current_artifact=artifact)
        if identity != ("minute", job_id, spec_hash, actor):
            raise PermissionError("exact original minute report ownership differs")
        return proof

    def bind_sealed_artifact(self, actor: str, artifact: SealedArtifactFact) -> SealedOwnerBinding:
        """Facts enter only after the original reader and this verified peer agree."""
        from uuid import UUID

        from rquant.sealed_result_ownership import (
            OriginalEffectFact,
            OriginalSubmissionFact,
            SealedArtifactFact,
            SealedJobFact,
            bind_sealed_owner,
        )
        if type(artifact) is not SealedArtifactFact:
            raise TypeError("exact original sealed artifact facts required")
        original_id = UUID(artifact.job_id).hex if artifact.domain == "factor" else artifact.job_id
        proof = self.result_owner(actor, domain=artifact.domain, job_id=original_id, spec_hash=artifact.spec_hash)
        kind = {"factor": "submit_factor_run", "portfolio": "submit_portfolio_backtest", "strategy": "run_strategy_template", "minute": "submit_minute_replay"}[artifact.domain]
        submission = OriginalSubmissionFact(domain=artifact.domain, command_id=proof.command_id,
            command_kind=kind, command_sha256=proof.command_sha256, actor_id=proof.owner_id,
            job_id=artifact.job_id, spec_hash=artifact.spec_hash, origin_verified=True)
        effect = OriginalEffectFact(command_id=proof.command_id, command_kind=kind,
            command_sha256=proof.command_sha256, status="succeeded", submitted_job_id=artifact.job_id,
            submitted_spec_hash=artifact.spec_hash, worker_owner_id=proof.worker_owner_id)
        job = SealedJobFact(domain=artifact.domain, job_id=artifact.job_id, spec_hash=artifact.spec_hash,
            status="succeeded", manifest_hash=artifact.manifest_hash,
            complete_result_hash=artifact.complete_result_hash)
        binding = bind_sealed_owner(submission=submission, effect=effect, job=job, artifact=artifact)
        if binding is None:
            raise PermissionError("original actor has no sealed owner binding")
        return binding

    def authorized_transport(self, original: Callable[[dict[str, object]], dict[str, object]]) -> Callable[[dict[str, object]], dict[str, object]]:
        def submit(body: dict[str, object]) -> dict[str, object]:
            actor = COLLABORATION_ACTOR.get()
            if actor is None:
                raise PermissionError("original verified web actor is unavailable")
            proof = CommandAuthorization.model_validate_json(self.request("authorize_command", actor, original_command=body))
            result = original({"command": body, "authorization": proof.model_dump(mode="json")})
            self.me(actor)
            return result
        return submit
