"""The existing distinct-UID Unix transport admits only original template commands."""

from __future__ import annotations

import socket
from collections.abc import Callable
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from threading import Lock
from typing import Annotated, Literal, Self

from pydantic import Field, TypeAdapter, model_validator

from rquant.factor_definition_admission import (
    _ACTOR_ADAPTER,
    FactorDefinitionAdmissionClient,
    FactorDefinitionAdmissionServer,
    _peer_uid,
    _UnixHTTPConnection,
    build_factor_definition_admission_server,
)
from rquant.page_control import (
    PageControlCommandConflictError,
    PageControlReceipt,
    PageControlService,
    PageControlStatus,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strategy_authoring_commands import (
    AcceptedStrategyTemplateArchive,
    AcceptedStrategyTemplateCommand,
    ArchiveStrategyTemplate,
    SaveStrategyTemplate,
    StrategyAuthoringIdentity,
    StrategyTemplateReceipt,
)
from rquant.strategy_authoring_source import StrategySourceCatalog
from rquant.strategy_template_run_commands import (
    AcceptedStrategyTemplateRun,
    RunStrategyTemplate,
    StrategyTemplateCommandValue,
    StrategyTemplateRunReceipt,
)
from rquant.strategy_template_run_commands import (
    StrategyTemplateCommandValue as StrategyTemplateCommand,
)
from rquant.strict_json import canonical_json_bytes, strict_json_loads

MAX_STRATEGY_PRIVATE_BYTES = 34 * 1024
_MAX_RESPONSE_BYTES = 40 * 1024
_PREFIX = "/v1/strategy-authoring-admission"
_PUBLIC_REQUEST_ADAPTER = TypeAdapter(
    Annotated[StrategyTemplateCommandValue, Field(discriminator="kind")]
)


class StrategyAuthoringAdmissionUnavailableError(RuntimeError):
    """The original request must be retained after an unconfirmed response."""


class StrategyAuthoringAdmissionRejectedError(ValueError):
    """A trusted listener proved rejection before a new effect."""


class StrategyAuthoringAdmissionNotFoundError(KeyError):
    """Neither the original journal nor its accepted metadata has this request."""


class StrategyAuthoringAdmissionResult(RuntimeContractModel):
    original_request: StrategyTemplateCommandValue
    owner_id: str
    metadata_identity: StrategyAuthoringIdentity
    receipt: PageControlReceipt

    @model_validator(mode="after")
    def validate_original_receipt(self) -> Self:
        original = self.original_request
        if (
            self.receipt.command_id != original.command_id
            or self.receipt.enqueued_at != original.requested_at
        ):
            raise ValueError("strategy journal receipt differs from original request")
        if self.receipt.status is PageControlStatus.SUCCEEDED:
            effect = (
                StrategyTemplateRunReceipt
                if isinstance(original, RunStrategyTemplate)
                else StrategyTemplateReceipt
            ).model_validate(self.receipt.result)
            expected_action = (
                "run"
                if isinstance(original, RunStrategyTemplate)
                else "save"
                if isinstance(original, SaveStrategyTemplate)
                else "archive"
            )
            if (
                effect.owner_id,
                effect.command_id,
                effect.action,
                effect.original_request_hash,
            ) != (self.owner_id, original.command_id, expected_action, original.request_hash):
                raise ValueError("strategy successful effect differs from original owner/request")
            if original.strategy_id is not None and effect.strategy_id != original.strategy_id:
                raise ValueError("strategy result logical ID differs")
            if (
                isinstance(original, ArchiveStrategyTemplate)
                and effect.head != original.expected_head
            ):
                raise ValueError("strategy archive result head differs")
            if isinstance(original, RunStrategyTemplate) and effect.head != original.head:
                raise ValueError("strategy run result version differs")
            if isinstance(original, SaveStrategyTemplate) and effect.head.version != (
                1 if original.expected_head is None else original.expected_head.version + 1
            ):
                raise ValueError("strategy save result version differs")
            if self.receipt.completed_at is None:
                raise ValueError("strategy successful journal result lacks completion time")
        return self


class _PrivateStrategyEnvelope(RuntimeContractModel):
    authenticated_actor_id: str
    request: StrategyTemplateCommandValue
    verified_metadata_identity: StrategyAuthoringIdentity | None = None


def decode_strategy_authoring_request(
    body: bytes, *, mode: Literal["submit", "lookup", "resume"]
) -> _PrivateStrategyEnvelope:
    if not 1 <= len(body) <= MAX_STRATEGY_PRIVATE_BYTES:
        raise ValueError("strategy admission body exceeds bound")
    payload = strict_json_loads(body)
    fields = {"authenticated_actor_id", "request"} | (
        {"verified_metadata_identity"} if mode == "submit" else set()
    )
    if not isinstance(payload, dict) or set(payload) != fields:
        raise ValueError("strategy private envelope fields differ")
    _ACTOR_ADAPTER.validate_python(payload["authenticated_actor_id"])
    request = _PUBLIC_REQUEST_ADAPTER.validate_python(payload["request"])
    parsed = _PrivateStrategyEnvelope.model_validate({**payload, "request": request})
    if mode == "submit" and parsed.verified_metadata_identity is None:
        raise ValueError("strategy submit requires verified metadata identity")
    return parsed


class StrategyAuthoringAdmission:
    def __init__(
        self,
        service: PageControlService,
        *,
        source_catalog_provider: Callable[[str, str], StrategySourceCatalog],
    ) -> None:
        self.service = service
        self.source_catalog_provider = source_catalog_provider
        self._lock = Lock()

    @property
    def editor_users(self) -> frozenset[str]:
        backend = self.service.consumer._strategy_authoring_backend()
        return frozenset(backend.editor_users) if backend.enabled else frozenset()

    def _authorize(self, owner: str) -> None:
        _ACTOR_ADAPTER.validate_python(owner)
        self.service.consumer._strategy_authoring_backend().authorize(owner)

    def run_available(self, *, authenticated_actor_id: str) -> bool:
        self._authorize(authenticated_actor_id)
        return self.service.consumer._strategy_authoring_backend().run_backend is not None

    def _bound(
        self, request: StrategyTemplateCommand, owner: str, receipt: PageControlReceipt
    ) -> StrategyAuthoringAdmissionResult:
        matched = self.service.outbox.lookup_strategy_authoring_command(
            request, authenticated_actor_id=owner
        )
        if matched is None or matched[1] != receipt:
            raise StrategyAuthoringAdmissionUnavailableError("strategy original journal changed")
        return StrategyAuthoringAdmissionResult(
            original_request=request,
            owner_id=owner,
            metadata_identity=matched[0].metadata_identity,
            receipt=receipt,
        )

    def lookup(
        self, request: StrategyTemplateCommand, *, authenticated_actor_id: str
    ) -> StrategyAuthoringAdmissionResult | None:
        self._authorize(authenticated_actor_id)
        with self._lock:
            receipt = self.service._lookup_trusted_strategy_authoring(
                request, authenticated_actor_id=authenticated_actor_id
            )
            if receipt is not None and receipt.status is PageControlStatus.SUCCEEDED:
                receipt = self.service._resume_trusted_strategy_authoring(
                    request, authenticated_actor_id=authenticated_actor_id
                )
            return (
                None if receipt is None else self._bound(request, authenticated_actor_id, receipt)
            )

    def resume(
        self, request: StrategyTemplateCommand, *, authenticated_actor_id: str
    ) -> StrategyAuthoringAdmissionResult:
        self._authorize(authenticated_actor_id)
        with self._lock:
            matched = self.service._lookup_trusted_strategy_authoring(
                request, authenticated_actor_id=authenticated_actor_id
            )
            if matched is None:
                store = self.service.consumer._strategy_authoring_backend().store
                with store._connection() as connection:
                    row = store._command_row(connection, request, authenticated_actor_id)
                if row is None or row["frozen"] is None:
                    raise StrategyAuthoringAdmissionNotFoundError(
                        "strategy original admission not found"
                    )
                model = (
                    AcceptedStrategyTemplateRun
                    if isinstance(request, RunStrategyTemplate)
                    else AcceptedStrategyTemplateCommand
                    if isinstance(request, SaveStrategyTemplate)
                    else AcceptedStrategyTemplateArchive
                )
                accepted = model.model_validate_json(row["frozen"])
                receipt = self.service._submit_trusted_strategy_authoring(
                    request,
                    authenticated_actor_id=authenticated_actor_id,
                    verified_metadata_identity=accepted.metadata_identity,
                    catalog=StrategySourceCatalog(
                        owner_id=authenticated_actor_id,
                        generation_id=request.generation_id,
                        pools=(),
                        signals=(),
                    ),
                )
            else:
                receipt = self.service._resume_trusted_strategy_authoring(
                    request, authenticated_actor_id=authenticated_actor_id
                )
            return self._bound(request, authenticated_actor_id, receipt)

    def submit(
        self,
        request: StrategyTemplateCommand,
        *,
        authenticated_actor_id: str,
        verified_metadata_identity: StrategyAuthoringIdentity,
    ) -> StrategyAuthoringAdmissionResult:
        self._authorize(authenticated_actor_id)
        if type(request) not in (
            SaveStrategyTemplate,
            ArchiveStrategyTemplate,
            RunStrategyTemplate,
        ):
            raise TypeError("strategy admission requires an ownerless original")
        with self._lock:
            if (
                self.service._lookup_trusted_strategy_authoring(
                    request, authenticated_actor_id=authenticated_actor_id
                )
                is not None
            ):
                return self._bound(
                    request,
                    authenticated_actor_id,
                    self.service._resume_trusted_strategy_authoring(
                        request, authenticated_actor_id=authenticated_actor_id
                    ),
                )
            store = self.service.consumer._strategy_authoring_backend().store
            with store._connection(expected_identity=verified_metadata_identity) as connection:
                old = store._command_row(connection, request, authenticated_actor_id)
            catalog = (
                StrategySourceCatalog(
                    owner_id=authenticated_actor_id,
                    generation_id=request.generation_id,
                    pools=(),
                    signals=(),
                )
                if old is not None or isinstance(request, RunStrategyTemplate)
                else self.source_catalog_provider(authenticated_actor_id, request.generation_id)
            )
            receipt = self.service._submit_trusted_strategy_authoring(
                request,
                authenticated_actor_id=authenticated_actor_id,
                verified_metadata_identity=verified_metadata_identity,
                catalog=catalog,
            )
            return self._bound(request, authenticated_actor_id, receipt)


def _handler() -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            routes = {f"{_PREFIX}/{mode}": mode for mode in ("submit", "lookup", "resume")}
            routes[f"{_PREFIX}/run-availability"] = "run-availability"
            if self.path not in routes:
                self._json(404, {"error": "not_found"})
                return
            try:
                lengths = self.headers.get_all("Content-Length", [])
                types = self.headers.get_all("Content-Type", [])
                if (
                    len(lengths) != 1
                    or not lengths[0].isdecimal()
                    or types != ["application/json"]
                    or self.headers.get("Transfer-Encoding") is not None
                    or self.headers.get("Content-Encoding") is not None
                ):
                    raise ValueError("invalid private framing")
                size = int(lengths[0])
                if not 1 <= size <= MAX_STRATEGY_PRIVATE_BYTES:
                    raise ValueError("invalid private body size")
                body = self.rfile.read(size)
                if len(body) != size:
                    raise ValueError("truncated private body")
                mode = routes[self.path]
                if mode == "run-availability":
                    payload = strict_json_loads(body)
                    if (
                        not isinstance(payload, dict)
                        or set(payload) != {"authenticated_actor_id"}
                        or size > 1024
                    ):
                        raise ValueError("invalid template capability envelope")
                    actor = _ACTOR_ADAPTER.validate_python(payload["authenticated_actor_id"])
                else:
                    parsed = decode_strategy_authoring_request(body, mode=mode)
            except (ValueError, TypeError):
                self._json(400, {"error": "invalid_command"})
                return
            try:
                admission = self.server.admission
                if mode == "run-availability":
                    available = admission.run_available(authenticated_actor_id=actor)
                    self._json(200, {"authenticated_actor_id": actor, "available": available})
                    return
                if mode == "lookup":
                    result = admission.lookup(
                        parsed.request, authenticated_actor_id=parsed.authenticated_actor_id
                    )
                elif mode == "resume":
                    result = admission.resume(
                        parsed.request, authenticated_actor_id=parsed.authenticated_actor_id
                    )
                else:
                    result = admission.submit(
                        parsed.request,
                        authenticated_actor_id=parsed.authenticated_actor_id,
                        verified_metadata_identity=parsed.verified_metadata_identity,
                    )
            except PermissionError:
                self._json(403, {"error": "actor_forbidden"})
            except StrategyAuthoringAdmissionNotFoundError:
                self._json(404, {"error": "original_not_found"})
            except (PageControlCommandConflictError, ValueError, KeyError):
                self._json(409, {"error": "rejected"})
            except Exception:
                self._json(503, {"error": "unavailable"})
            else:
                self._json(
                    200,
                    {
                        "found": result is not None,
                        "result": None if result is None else result.model_dump(mode="json"),
                    }
                    if mode == "lookup"
                    else result.model_dump(mode="json"),
                )

        def _json(self, status: int, payload: object) -> None:
            body = canonical_json_bytes(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def build_strategy_authoring_admission_server(
    admission: StrategyAuthoringAdmission,
    *,
    socket_path: Path | None,
    trusted_web_uid: int | None = None,
    shared_gid: int | None = None,
    peer_uid: Callable[[socket.socket], int] = _peer_uid,
) -> FactorDefinitionAdmissionServer | None:
    return build_factor_definition_admission_server(
        admission,
        socket_path=socket_path,
        trusted_web_uid=trusted_web_uid,
        shared_gid=shared_gid,
        peer_uid=peer_uid,
        _handler_type=_handler(),
    )


class StrategyAuthoringAdmissionClient(FactorDefinitionAdmissionClient):
    def run_available(self, *, authenticated_actor_id: str) -> bool:
        _ACTOR_ADAPTER.validate_python(authenticated_actor_id)
        connection = _UnixHTTPConnection(
            self.socket_path,
            timeout_seconds=self.timeout_seconds,
            expected_service_uid=self.expected_service_uid,
            shared_gid=self.shared_gid,
            client_uid=self.client_uid,
        )
        try:
            body = canonical_json_bytes({"authenticated_actor_id": authenticated_actor_id})
            connection.request(
                "POST",
                f"{_PREFIX}/run-availability",
                body=body,
                headers={"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            lengths = response.headers.get_all("Content-Length", [])
            if (
                response.status != 200
                or len(lengths) != 1
                or not lengths[0].isdecimal()
                or not 1 <= int(lengths[0]) <= 1024
                or response.headers.get_all("Content-Type", []) != ["application/json"]
                or response.getheader("Transfer-Encoding") is not None
                or response.getheader("Content-Encoding") is not None
            ):
                raise ValueError("template capability response framing differs")
            data = response.read(1025)
            if len(data) != int(lengths[0]):
                raise ValueError("template capability response length differs")
            parsed = strict_json_loads(data)
            if (
                not isinstance(parsed, dict)
                or set(parsed) != {"authenticated_actor_id", "available"}
                or parsed["authenticated_actor_id"] != authenticated_actor_id
                or type(parsed["available"]) is not bool
            ):
                raise ValueError("template capability identity differs")
            return parsed["available"]
        except (OSError, HTTPException, ValueError) as exc:
            raise StrategyAuthoringAdmissionUnavailableError("回测暂不可用。") from exc
        finally:
            connection.close()

    def _call(
        self,
        mode: str,
        request: StrategyTemplateCommand,
        *,
        authenticated_actor_id: str,
        verified_metadata_identity: StrategyAuthoringIdentity | None = None,
    ) -> StrategyAuthoringAdmissionResult | None:
        payload = {
            "authenticated_actor_id": authenticated_actor_id,
            "request": request.model_dump(mode="json"),
        }
        if mode == "submit":
            payload["verified_metadata_identity"] = verified_metadata_identity.model_dump(
                mode="json"
            )
        body = canonical_json_bytes(payload)
        decode_strategy_authoring_request(body, mode=mode)
        connection = _UnixHTTPConnection(
            self.socket_path,
            timeout_seconds=self.timeout_seconds,
            expected_service_uid=self.expected_service_uid,
            shared_gid=self.shared_gid,
            client_uid=self.client_uid,
        )
        try:
            connection.request(
                "POST", f"{_PREFIX}/{mode}", body=body, headers={"Content-Type": "application/json"}
            )
            response = connection.getresponse()
            lengths = response.headers.get_all("Content-Length", [])
            types = response.headers.get_all("Content-Type", [])
            if (
                len(lengths) != 1
                or not lengths[0].isdecimal()
                or not 1 <= int(lengths[0]) <= _MAX_RESPONSE_BYTES
                or types != ["application/json"]
                or response.getheader("Transfer-Encoding") is not None
                or response.getheader("Content-Encoding") is not None
            ):
                raise ValueError("strategy response framing differs")
            data = response.read(_MAX_RESPONSE_BYTES + 1)
            if len(data) != int(lengths[0]):
                raise ValueError("strategy response length differs")
            parsed = strict_json_loads(data)
            if response.status == 404 and parsed == {"error": "original_not_found"}:
                raise StrategyAuthoringAdmissionNotFoundError("原操作尚未受理。")
            if (
                response.status in (400, 403, 404, 409)
                and isinstance(parsed, dict)
                and set(parsed) == {"error"}
            ):
                raise StrategyAuthoringAdmissionRejectedError("原操作未受理。")
            if response.status != 200:
                raise StrategyAuthoringAdmissionUnavailableError("原操作结果待确认。")
            if mode == "lookup":
                if parsed == {"found": False, "result": None}:
                    return None
                if (
                    not isinstance(parsed, dict)
                    or set(parsed) != {"found", "result"}
                    or parsed["found"] is not True
                ):
                    raise ValueError("strategy lookup result differs")
                parsed = parsed["result"]
            result = StrategyAuthoringAdmissionResult.model_validate(parsed)
            if result.original_request != request or result.owner_id != authenticated_actor_id:
                raise ValueError("strategy response belongs to another original or owner")
            return result
        except StrategyAuthoringAdmissionRejectedError:
            raise
        except (OSError, HTTPException, ValueError) as exc:
            raise StrategyAuthoringAdmissionUnavailableError("原操作结果待确认。") from exc
        finally:
            connection.close()

    def submit(
        self,
        request: StrategyTemplateCommand,
        *,
        authenticated_actor_id: str,
        verified_metadata_identity: StrategyAuthoringIdentity,
    ) -> StrategyAuthoringAdmissionResult:
        result = self._call(
            "submit",
            request,
            authenticated_actor_id=authenticated_actor_id,
            verified_metadata_identity=verified_metadata_identity,
        )
        assert result is not None
        return result

    def lookup(
        self, request: StrategyTemplateCommand, *, authenticated_actor_id: str
    ) -> StrategyAuthoringAdmissionResult | None:
        return self._call("lookup", request, authenticated_actor_id=authenticated_actor_id)

    def resume(
        self, request: StrategyTemplateCommand, *, authenticated_actor_id: str
    ) -> StrategyAuthoringAdmissionResult:
        result = self._call("resume", request, authenticated_actor_id=authenticated_actor_id)
        assert result is not None
        return result
