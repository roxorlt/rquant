"""Original paper commands use the existing private UID-bound transport."""

from __future__ import annotations

import hashlib
import os
import socket
import stat
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from threading import Lock
from collections.abc import Callable
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, TypeAdapter, model_validator

from rquant.factor_definition_admission import FactorDefinitionAdmissionClient, FactorDefinitionAdmissionServer, _ACTOR_ADAPTER, _UnixHTTPConnection, _peer_uid, build_factor_definition_admission_server
from rquant.lab_artifact_export import LabJobZipExportFacade
from rquant.paper_operator_commands import PaperPortfolioCommand, SetPaperAccountPaused, SavePaperPortfolioConfiguration, PaperOperatorConfirmation, PaperOperatorControl
from rquant.paper_research_commands import RunPaperPortfolioResearch, PaperResearchSubmissionReceipt
from rquant.paper_portfolio_models import PaperPortfolioConfiguration, PaperPortfolioStateIdentity
from rquant.page_control import PageControlService, PageControlReceipt, PageControlStatus, PageControlCommandConflictError
from rquant.paper_portfolio_commands import PaperPortfolioPageControlBackend
from rquant.paper_research_artifact import PaperResearchResultReader
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strict_json import canonical_json_bytes, strict_json_loads

MAX_PAPER_PRIVATE_BYTES = 34*1024
MAX_PAPER_PRIVATE_RESPONSE_BYTES = 64*1024
MAX_PAPER_ZIP_BYTES = 16*1024*1024
_PREFIX = "/v1/paper-portfolio-admission"
_COMMAND = TypeAdapter(Annotated[PaperPortfolioCommand, Field(discriminator="kind")])


class PaperPortfolioAdmissionUnavailableError(RuntimeError):
    pass


class PaperPortfolioAdmissionRejectedError(ValueError):
    pass


class PaperPortfolioAdmissionNotFoundError(KeyError):
    pass


class PaperPortfolioAdmissionResult(RuntimeContractModel):
    original_request: PaperPortfolioCommand
    owner_id: str
    metadata_identity: PaperPortfolioStateIdentity
    receipt: PageControlReceipt

    @model_validator(mode="after")
    def original_receipt(self) -> Self:
        request, receipt = self.original_request, self.receipt
        if (receipt.command_id, receipt.enqueued_at) != (request.command_id, request.requested_at):
            raise ValueError("paper journal receipt differs from original request")
        if receipt.status is PageControlStatus.SUCCEEDED:
            if type(request) is SetPaperAccountPaused:
                result = PaperOperatorControl.model_validate(receipt.result)
                if (result.binding.owner_id, result.binding.account_id, result.original_command_id, result.original_request_fingerprint,
                        result.configuration_fingerprint, result.sequence, result.paused) != (
                        self.owner_id, request.account_id, request.command_id, request.fingerprint, request.configuration_fingerprint,
                        request.expected_sequence+1, request.paused):
                    raise ValueError("paper successful control differs from original owner/request")
            elif type(request) is SavePaperPortfolioConfiguration:
                result = PaperPortfolioConfiguration.model_validate(receipt.result)
                if (result.binding.owner_id, result.binding.account_id, result.weight_rule, result.drawdown_rule) != (
                        self.owner_id, request.account_id, request.weight_rule, request.drawdown_rule):
                    raise ValueError("paper successful configuration differs from original owner/request")
            else:
                result = PaperResearchSubmissionReceipt.model_validate(receipt.result)
                if (result.command_id, result.account_id, result.configuration_fingerprint, result.task_name) != (
                        request.command_id, request.account_id, request.configuration_fingerprint, request.task_name):
                    raise ValueError("paper successful research differs from original request")
        return self


class PaperPortfolioZip(RuntimeContractModel):
    content: bytes = Field(max_length=MAX_PAPER_ZIP_BYTES)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    filename: Literal["paper-research.zip"] = "paper-research.zip"

    @model_validator(mode="after")
    def verified_bytes(self) -> Self:
        if not self.content or hashlib.sha256(self.content).hexdigest() != self.sha256:
            raise ValueError("paper ZIP bytes differ from original receipt")
        return self


class PaperPortfolioAdmission:
    def __init__(self, service: PageControlService, *, backend: PaperPortfolioPageControlBackend,
                 result_reader: PaperResearchResultReader | None = None, zip_export: LabJobZipExportFacade | None = None) -> None:
        if type(service) is not PageControlService or type(backend) is not PaperPortfolioPageControlBackend or service.consumer.paper_portfolio_backend is not backend:
            raise TypeError("paper admission requires the concrete original journal and backend")
        if result_reader is not None and (type(result_reader) is not PaperResearchResultReader or result_reader.backend is not backend.research_backend):
            raise TypeError("paper admission results belong to another original backend")
        if zip_export is not None and (type(zip_export) is not LabJobZipExportFacade or result_reader is None or zip_export.reader is not result_reader.reader):
            raise TypeError("paper download requires the same concrete original sealed authority")
        self.service, self.backend, self.result_reader, self.zip_export = service, backend, result_reader, zip_export
        self.editor_users = backend.editor_users
        self._lock = Lock()

    def run_available(self, *, authenticated_actor_id: str) -> bool:
        self.backend.authorize(authenticated_actor_id)
        return self.backend.research_backend is not None and self.backend.admission_source is not None

    def _bound(self, request: PaperPortfolioCommand, actor: str, receipt: PageControlReceipt) -> PaperPortfolioAdmissionResult:
        match = self.service.outbox.lookup_paper_portfolio_command(request, authenticated_actor_id=actor)
        if match is None or match[1] != receipt:
            raise ValueError("original paper receipt changed during admission")
        return PaperPortfolioAdmissionResult(original_request=request, owner_id=actor, metadata_identity=match[0].metadata_identity, receipt=receipt)

    def lookup(self, request: PaperPortfolioCommand, *, authenticated_actor_id: str) -> PaperPortfolioAdmissionResult | None:
        self.backend.authorize(authenticated_actor_id)
        with self._lock:
            receipt = self.service._lookup_trusted_paper_portfolio(request, authenticated_actor_id=authenticated_actor_id)
            return self._bound(request, authenticated_actor_id, receipt) if receipt is not None else None

    def resume(self, request: PaperPortfolioCommand, *, authenticated_actor_id: str) -> PaperPortfolioAdmissionResult:
        self.backend.authorize(authenticated_actor_id)
        with self._lock:
            if self.service.outbox.lookup_paper_portfolio_command(request, authenticated_actor_id=authenticated_actor_id) is None:
                raise PaperPortfolioAdmissionNotFoundError("original_not_found")
            receipt = self.service._resume_trusted_paper_portfolio(request, authenticated_actor_id=authenticated_actor_id)
            return self._bound(request, authenticated_actor_id, receipt)

    def prepare(self, request: SetPaperAccountPaused, *, authenticated_actor_id: str,
                verified_metadata_identity: PaperPortfolioStateIdentity) -> PaperOperatorConfirmation:
        if self.backend.admission_source is None:
            raise PermissionError("paper source admission is not configured")
        with self._lock:
            return self.backend.prepare_confirmation(request, authenticated_actor_id=authenticated_actor_id, expected_identity=verified_metadata_identity)

    def submit(self, request: PaperPortfolioCommand, *, authenticated_actor_id: str,
               verified_metadata_identity: PaperPortfolioStateIdentity, confirmation_id: str | None = None) -> PaperPortfolioAdmissionResult:
        self.backend.authorize(authenticated_actor_id)
        with self._lock:
            old = self.service.outbox.lookup_paper_portfolio_command(request, authenticated_actor_id=authenticated_actor_id)
            if old is None and self.backend.admission_source is None:
                raise PermissionError("paper source admission is not configured")
            receipt = self.service._submit_trusted_paper_portfolio(request, authenticated_actor_id=authenticated_actor_id,
                                                                  verified_metadata_identity=verified_metadata_identity, confirmation_id=confirmation_id)
            return self._bound(request, authenticated_actor_id, receipt)

    def download(self, *, account_id: str, job_id: UUID, authenticated_actor_id: str) -> PaperPortfolioZip:
        self.backend.authorize(authenticated_actor_id)
        if self.result_reader is None or self.zip_export is None:
            raise PaperPortfolioAdmissionUnavailableError("paper download is not configured")
        if self.result_reader.read(account_id=account_id, job_id=job_id, owner_id=authenticated_actor_id, as_of=self.backend.clock()) is None:
            raise PaperPortfolioAdmissionNotFoundError("paper result is not sealed")
        receipt = self.zip_export.export(job_id)
        if not 1 <= receipt.byte_size <= MAX_PAPER_ZIP_BYTES:
            raise ValueError("paper ZIP exceeds the bounded download budget")
        with self.zip_export._locked_export_root() as root:
            job = self.zip_export._open_private_child(root, job_id.hex, label="paper job export")
            try:
                request = self.zip_export._open_private_child(job, receipt.request_id.hex, label="paper request export")
                try:
                    descriptor = os.open("result.zip", os.O_RDONLY|os.O_NOFOLLOW, dir_fd=request)
                    try:
                        before = os.fstat(descriptor)
                        if (not stat.S_ISREG(before.st_mode) or stat.S_IMODE(before.st_mode) != 0o600
                                or before.st_size != receipt.byte_size or before.st_nlink != 1 or before.st_uid != os.geteuid()):
                            raise ValueError("paper ZIP source identity differs")
                        with os.fdopen(descriptor, "rb", closefd=False) as source:
                            content = source.read(MAX_PAPER_ZIP_BYTES+1)
                        after = os.fstat(descriptor)
                        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) != (
                                before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns):
                            raise ValueError("paper ZIP source changed during bounded read")
                    finally:
                        os.close(descriptor)
                finally:
                    os.close(request)
            finally:
                os.close(job)
        return PaperPortfolioZip(content=content, sha256=receipt.sha256)


class PaperPrivateRequest(RuntimeContractModel):
    authenticated_actor_id: str
    command: PaperPortfolioCommand
    verified_metadata_identity: PaperPortfolioStateIdentity | None = None
    confirmation_id: str | None = Field(default=None, max_length=64)


def _handler() -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            return

        def _json(self, status: int, payload: object) -> None:
            body = canonical_json_bytes(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:
            mode = self.path.removeprefix(_PREFIX+"/")
            if self.path != _PREFIX+"/"+mode or mode not in {"lookup", "resume", "submit", "prepare", "run-availability", "download"}:
                self._json(404, {"error": "not_found"})
                return
            try:
                lengths = self.headers.get_all("Content-Length", [])
                if (len(lengths) != 1 or not lengths[0].isdecimal() or self.headers.get_all("Content-Type", []) != ["application/json"]
                        or self.headers.get("Transfer-Encoding") is not None or self.headers.get("Content-Encoding") is not None
                        or not 1 <= int(lengths[0]) <= MAX_PAPER_PRIVATE_BYTES):
                    raise ValueError("invalid private framing")
                body = self.rfile.read(int(lengths[0]))
                if len(body) != int(lengths[0]):
                    raise ValueError("truncated private body")
                raw = strict_json_loads(body)
                if not isinstance(raw, dict):
                    raise ValueError("invalid private envelope")
                actor = _ACTOR_ADAPTER.validate_python(raw.get("authenticated_actor_id"))
                if mode == "run-availability":
                    if set(raw) != {"authenticated_actor_id"}:
                        raise ValueError("invalid capability envelope")
                elif mode == "download":
                    if set(raw) != {"authenticated_actor_id", "account_id", "job_id"} or not isinstance(raw["account_id"], str) or not 1 <= len(raw["account_id"]) <= 128:
                        raise ValueError("invalid download envelope")
                    job_id = UUID(raw["job_id"])
                else:
                    parsed = PaperPrivateRequest.model_validate(raw)
                    if mode in {"submit", "prepare"} and parsed.verified_metadata_identity is None:
                        raise ValueError("missing original metadata identity")
                    if mode in {"lookup", "resume"} and (parsed.verified_metadata_identity is not None or parsed.confirmation_id is not None):
                        raise ValueError("original lookup cannot provide fresh metadata")
                    if mode == "prepare" and (type(parsed.command) is not SetPaperAccountPaused or parsed.confirmation_id is not None):
                        raise ValueError("invalid two-step preparation")
            except (ValueError, TypeError, KeyError):
                self._json(400, {"error": "invalid_command"})
                return
            try:
                admission = self.server.admission
                if mode == "run-availability":
                    self._json(200, {"authenticated_actor_id": actor, "available": admission.run_available(authenticated_actor_id=actor)})
                    return
                if mode == "download":
                    result = admission.download(account_id=raw["account_id"], job_id=job_id, authenticated_actor_id=actor)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/zip")
                    self.send_header("Content-Length", str(len(result.content)))
                    self.send_header("X-Rquant-Download-Hash", result.sha256)
                    self.send_header("X-Rquant-Download-Binding", canonical_sha256({"actor": actor, "account": raw["account_id"], "job": str(job_id)}))
                    self.end_headers()
                    self.wfile.write(result.content)
                    return
                if mode == "lookup":
                    result = admission.lookup(parsed.command, authenticated_actor_id=actor)
                elif mode == "resume":
                    result = admission.resume(parsed.command, authenticated_actor_id=actor)
                elif mode == "prepare":
                    result = admission.prepare(parsed.command, authenticated_actor_id=actor, verified_metadata_identity=parsed.verified_metadata_identity)
                else:
                    result = admission.submit(parsed.command, authenticated_actor_id=actor, verified_metadata_identity=parsed.verified_metadata_identity, confirmation_id=parsed.confirmation_id)
            except PermissionError:
                self._json(403, {"error": "actor_forbidden"})
            except PaperPortfolioAdmissionNotFoundError:
                self._json(404, {"error": "original_not_found"})
            except (ValueError, KeyError, PageControlCommandConflictError):
                self._json(409, {"error": "rejected"})
            except Exception:
                self._json(503, {"error": "unavailable"})
            else:
                self._json(200, {"found": result is not None, "result": result.model_dump(mode="json") if result else None}
                           if mode == "lookup" else result.model_dump(mode="json"))
    return Handler


def build_paper_portfolio_admission_server(admission: PaperPortfolioAdmission, *, socket_path: Path | None,
                                           trusted_web_uid: int | None = None, shared_gid: int | None = None,
                                           peer_uid: Callable[[socket.socket], int] = _peer_uid) -> FactorDefinitionAdmissionServer | None:
    return build_factor_definition_admission_server(admission, socket_path=socket_path, trusted_web_uid=trusted_web_uid,
                                                    shared_gid=shared_gid, peer_uid=peer_uid, _handler_type=_handler())


class PaperPortfolioAdmissionClient(FactorDefinitionAdmissionClient):
    def _call(self, mode: str, raw: dict[str, object]) -> object:
        _ACTOR_ADAPTER.validate_python(raw.get("authenticated_actor_id"))
        body = canonical_json_bytes(raw)
        if len(body) > MAX_PAPER_PRIVATE_BYTES:
            raise PaperPortfolioAdmissionRejectedError("private body budget")
        connection = _UnixHTTPConnection(self.socket_path, timeout_seconds=self.timeout_seconds, expected_service_uid=self.expected_service_uid,
                                          shared_gid=self.shared_gid, client_uid=self.client_uid)
        try:
            connection.request("POST", _PREFIX+"/"+mode, body=body, headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            if response.status in (400, 403, 409):
                raise PaperPortfolioAdmissionRejectedError("paper admission rejected")
            if response.status == 404:
                raise PaperPortfolioAdmissionNotFoundError("original_not_found")
            maximum = MAX_PAPER_ZIP_BYTES if mode == "download" else MAX_PAPER_PRIVATE_RESPONSE_BYTES
            lengths = response.headers.get_all("Content-Length", [])
            if (response.status != 200 or len(lengths) != 1 or not lengths[0].isdecimal() or not 1 <= int(lengths[0]) <= maximum
                    or response.headers.get_all("Content-Type", []) != ["application/zip" if mode == "download" else "application/json"]
                    or response.getheader("Transfer-Encoding") is not None or response.getheader("Content-Encoding") is not None):
                raise ValueError("private paper response framing differs")
            content = response.read(maximum+1)
            if len(content) != int(lengths[0]):
                raise ValueError("private paper response length differs")
            if mode == "download":
                if response.getheader("X-Rquant-Download-Binding") != canonical_sha256({"actor": raw["authenticated_actor_id"], "account": raw["account_id"], "job": str(raw["job_id"])}):
                    raise ValueError("private paper download identity differs")
                return PaperPortfolioZip(content=content, sha256=response.getheader("X-Rquant-Download-Hash"))
            return strict_json_loads(content)
        except (PaperPortfolioAdmissionRejectedError, PaperPortfolioAdmissionNotFoundError):
            raise
        except (OSError, HTTPException, ValueError) as exc:
            raise PaperPortfolioAdmissionUnavailableError("原请求结果暂无法核验。") from exc
        finally:
            connection.close()

    def run_available(self, *, authenticated_actor_id: str) -> bool:
        result = self._call("run-availability", {"authenticated_actor_id": authenticated_actor_id})
        if (not isinstance(result, dict) or set(result) != {"authenticated_actor_id", "available"}
                or result["authenticated_actor_id"] != authenticated_actor_id or type(result["available"]) is not bool):
            raise PaperPortfolioAdmissionUnavailableError("paper capability identity differs")
        return result["available"]

    def _result(self, raw: object, request: PaperPortfolioCommand, actor: str) -> PaperPortfolioAdmissionResult:
        try:
            result = PaperPortfolioAdmissionResult.model_validate(raw)
            if result.original_request != request or result.owner_id != actor:
                raise ValueError("paper original response identity differs")
            return result
        except ValueError as exc:
            raise PaperPortfolioAdmissionUnavailableError("原请求结果暂无法核验。") from exc

    def lookup(self, request: PaperPortfolioCommand, *, authenticated_actor_id: str) -> PaperPortfolioAdmissionResult | None:
        raw = self._call("lookup", {"authenticated_actor_id": authenticated_actor_id, "command": request.model_dump(mode="json")})
        if not isinstance(raw, dict) or set(raw) != {"found", "result"} or type(raw["found"]) is not bool or raw["found"] != (raw["result"] is not None):
            raise PaperPortfolioAdmissionUnavailableError("paper original lookup differs")
        return self._result(raw["result"], request, authenticated_actor_id) if raw["found"] else None

    def resume(self, request: PaperPortfolioCommand, *, authenticated_actor_id: str) -> PaperPortfolioAdmissionResult:
        return self._result(self._call("resume", {"authenticated_actor_id": authenticated_actor_id, "command": request.model_dump(mode="json")}), request, authenticated_actor_id)

    def prepare(self, request: SetPaperAccountPaused, *, authenticated_actor_id: str, verified_metadata_identity: PaperPortfolioStateIdentity) -> PaperOperatorConfirmation:
        raw = self._call("prepare", {"authenticated_actor_id": authenticated_actor_id, "command": request.model_dump(mode="json"), "verified_metadata_identity": verified_metadata_identity.model_dump(mode="json")})
        try:
            result = PaperOperatorConfirmation.model_validate(raw)
        except ValueError as exc:
            raise PaperPortfolioAdmissionUnavailableError("原请求结果暂无法核验。") from exc
        if (result.request, result.owner_id, result.metadata_identity) != (request, authenticated_actor_id, verified_metadata_identity):
            raise PaperPortfolioAdmissionUnavailableError("paper confirmation identity differs")
        return result

    def submit(self, request: PaperPortfolioCommand, *, authenticated_actor_id: str, verified_metadata_identity: PaperPortfolioStateIdentity,
               confirmation_id: str | None = None) -> PaperPortfolioAdmissionResult:
        return self._result(self._call("submit", {"authenticated_actor_id": authenticated_actor_id, "command": request.model_dump(mode="json"),
                                                   "verified_metadata_identity": verified_metadata_identity.model_dump(mode="json"), "confirmation_id": confirmation_id}), request, authenticated_actor_id)

    def download(self, *, account_id: str, job_id: UUID, authenticated_actor_id: str) -> PaperPortfolioZip:
        result = self._call("download", {"authenticated_actor_id": authenticated_actor_id, "account_id": account_id, "job_id": str(job_id)})
        if not isinstance(result, PaperPortfolioZip):
            raise PaperPortfolioAdmissionUnavailableError("原请求结果暂无法核验。")
        return result
