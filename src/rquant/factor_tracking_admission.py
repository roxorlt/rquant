"""Private tracking admission reuses the fixed UID Unix transport and PageControl authority."""

from __future__ import annotations

import json
import socket
from collections.abc import Callable
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from threading import Lock

from rquant.factor.tracking import (
    FactorTrackingOperationResult,
    FactorTrackingReceipt,
    FactorTrackingRequest,
)
from rquant.factor_definition_admission import (
    _ACTOR_ADAPTER,
    _INSTANCE_PATTERN,
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
from rquant.strict_json import canonical_json_bytes, strict_json_loads

_PREFIX = "/v1/factor-tracking-admission"
_MAX_BYTES = 8192


class FactorTrackingAdmissionUnavailableError(RuntimeError):
    pass


class FactorTrackingAdmissionRejectedError(ValueError):
    pass


class FactorTrackingAdmission:
    def __init__(
        self, service: PageControlService, *, tracking_users: frozenset[str], enabled: bool = False
    ) -> None:
        if len(tracking_users) > 16:
            raise ValueError("tracking users exceed bounded allowlist")
        for actor in tracking_users:
            _ACTOR_ADAPTER.validate_python(actor)
        self.service, self.tracking_users, self.enabled = service, tracking_users, enabled
        self._lock = Lock()

    @property
    def editor_users(self) -> frozenset[str]:
        return self.tracking_users if self.enabled else frozenset()

    def _authorize(self, actor: str) -> None:
        if not self.enabled or actor not in self.tracking_users:
            raise PermissionError("当前账号不能管理因子跟踪")
        self.service.consumer._factor_tracking_backend().authorize(actor)

    @staticmethod
    def _result(
        request: FactorTrackingRequest, receipt: PageControlReceipt
    ) -> FactorTrackingOperationResult:
        if receipt.status is PageControlStatus.SUCCEEDED:
            return FactorTrackingOperationResult(
                original_request=request,
                status="applied",
                receipt=FactorTrackingReceipt.model_validate(receipt.result),
            )
        status = (
            "rejected"
            if receipt.status is PageControlStatus.FAILED
            else "pending"
            if receipt.status in (PageControlStatus.PENDING, PageControlStatus.PROCESSING)
            else "uncertain"
        )
        return FactorTrackingOperationResult(
            original_request=request, status=status, reason="原跟踪操作暂未确认，请核对原请求。"
        )

    def lookup(
        self, request: FactorTrackingRequest, *, authenticated_actor_id: str
    ) -> FactorTrackingOperationResult | None:
        self._authorize(authenticated_actor_id)
        with self._lock:
            receipt = self.service._lookup_trusted_factor_tracking(
                request, authenticated_actor_id=authenticated_actor_id
            )
            return None if receipt is None else self._result(request, receipt)

    def resume(
        self, request: FactorTrackingRequest, *, authenticated_actor_id: str
    ) -> FactorTrackingOperationResult:
        self._authorize(authenticated_actor_id)
        with self._lock:
            return self._result(
                request,
                self.service._resume_trusted_factor_tracking(
                    request, authenticated_actor_id=authenticated_actor_id
                ),
            )

    def submit(
        self,
        request: FactorTrackingRequest,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str,
    ) -> FactorTrackingOperationResult:
        self._authorize(authenticated_actor_id)
        with self._lock:
            receipt = self.service._submit_trusted_factor_tracking(
                request,
                authenticated_actor_id=authenticated_actor_id,
                verified_registry_instance_id=verified_registry_instance_id,
            )
            return self._result(request, receipt)


def _tracking_handler() -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server: FactorDefinitionAdmissionServer

        def do_POST(self) -> None:  # noqa: N802
            mode = self.path.removeprefix(_PREFIX)
            if self.path != _PREFIX + mode or mode not in (
                "/lookup",
                "/resume",
                "/submit",
            ):
                self._json(404, {"error": "not_found"})
                return
            self.connection.settimeout(2.0)
            try:
                lengths, types = (
                    self.headers.get_all("Content-Length", []),
                    self.headers.get_all("Content-Type", []),
                )
                if (
                    len(lengths) != 1
                    or len(types) != 1
                    or types[0] != "application/json"
                    or self.headers.get("Transfer-Encoding") is not None
                    or self.headers.get("Content-Encoding") is not None
                    or not lengths[0].isdecimal()
                    or not 0 < int(lengths[0]) <= _MAX_BYTES
                ):
                    raise ValueError("invalid framing")
                data = self.rfile.read(int(lengths[0]))
                if len(data) != int(lengths[0]):
                    raise ValueError("truncated body")
                payload = strict_json_loads(data)
                expected = {"authenticated_actor_id"}
                expected.add("request")
                if mode == "/submit":
                    expected.add("verified_registry_instance_id")
                if not isinstance(payload, dict) or set(payload) != expected:
                    raise ValueError("invalid envelope")
                actor = _ACTOR_ADAPTER.validate_python(payload["authenticated_actor_id"])
                request = FactorTrackingRequest.model_validate_json(
                    canonical_json_bytes(payload["request"])
                )
                instance = payload.get("verified_registry_instance_id")
                if mode == "/submit" and (
                    not isinstance(instance, str) or _INSTANCE_PATTERN.fullmatch(instance) is None
                ):
                    raise ValueError("invalid registry identity")
            except (ValueError, TypeError, OSError):
                self._json(400, {"error": "invalid_request"})
                return
            try:
                admission = self.server.admission
                if mode == "/lookup":
                    result = admission.lookup(request, authenticated_actor_id=actor)
                    self._json(
                        200,
                        {
                            "found": result is not None,
                            "result": None if result is None else result.model_dump(mode="json"),
                        },
                    )
                    return
                elif mode == "/resume":
                    result = admission.resume(request, authenticated_actor_id=actor)
                else:
                    result = admission.submit(
                        request,
                        authenticated_actor_id=actor,
                        verified_registry_instance_id=instance,
                    )
            except PermissionError:
                self._json(403, {"error": "actor_forbidden"})
                return
            except KeyError:
                self._json(404, {"error": "not_found"})
                return
            except (PageControlCommandConflictError, ValueError):
                self._json(409, {"error": "request_rejected"})
                return
            except Exception:
                self._json(503, {"error": "unavailable"})
                return
            self._json(200, result.model_dump(mode="json"))

        def _json(self, status: int, payload: object) -> None:
            data = canonical_json_bytes(payload)
            if len(data) > 16384:
                status, data = 503, b'{"error":"unavailable"}'
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def build_factor_tracking_admission_server(
    admission: FactorTrackingAdmission,
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
        _handler_type=_tracking_handler(),
    )


class FactorTrackingAdmissionClient(FactorDefinitionAdmissionClient):
    def _call(
        self,
        mode: str,
        actor: str,
        request: FactorTrackingRequest | None = None,
        instance: str | None = None,
    ) -> object:
        payload = {"authenticated_actor_id": actor}
        if request is not None:
            payload["request"] = request.model_dump(mode="json")
        if instance is not None:
            payload["verified_registry_instance_id"] = instance
        body = canonical_json_bytes(payload)
        if len(body) > _MAX_BYTES:
            raise FactorTrackingAdmissionRejectedError("运行请求过大")
        connection = _UnixHTTPConnection(
            self.socket_path,
            timeout_seconds=self.timeout_seconds,
            expected_service_uid=self.expected_service_uid,
            shared_gid=self.shared_gid,
            client_uid=self.client_uid,
        )
        try:
            connection.request(
                "POST", _PREFIX + mode, body=body, headers={"Content-Type": "application/json"}
            )
            response = connection.getresponse()
            data = response.read(16385)
            if len(data) > 16384 or response.getheader("Content-Type") != "application/json":
                raise FactorTrackingAdmissionUnavailableError("运行回执暂不可用")
            if response.status in (400, 403, 404, 409):
                raise FactorTrackingAdmissionRejectedError("原请求暂不可推进")
            if response.status != 200:
                raise FactorTrackingAdmissionUnavailableError("运行入口暂不可用")
            try:
                return strict_json_loads(data)
            except ValueError as exc:
                raise FactorTrackingAdmissionUnavailableError("运行回执暂不可用") from exc
        except (OSError, HTTPException, json.JSONDecodeError) as exc:
            raise FactorTrackingAdmissionUnavailableError("运行入口暂不可用") from exc
        finally:
            connection.close()

    def lookup(
        self, request: FactorTrackingRequest, *, authenticated_actor_id: str
    ) -> FactorTrackingOperationResult | None:
        payload = self._call("/lookup", authenticated_actor_id, request)
        if (
            not isinstance(payload, dict)
            or set(payload) != {"found", "result"}
            or type(payload["found"]) is not bool
        ):
            raise FactorTrackingAdmissionUnavailableError("运行回执暂不可用")
        if payload["found"]:
            return self._bound(payload["result"], request)
        if payload["result"] is not None:
            raise FactorTrackingAdmissionUnavailableError("运行回执暂不可用")
        return None

    @staticmethod
    def _bound(payload: object, request: FactorTrackingRequest) -> FactorTrackingOperationResult:
        try:
            result = FactorTrackingOperationResult.model_validate_json(
                canonical_json_bytes(payload)
            )
        except ValueError as exc:
            raise FactorTrackingAdmissionUnavailableError("运行回执暂不可用") from exc
        if result.original_request != request:
            raise FactorTrackingAdmissionUnavailableError("运行回执与原请求不同")
        return result

    def resume(
        self, request: FactorTrackingRequest, *, authenticated_actor_id: str
    ) -> FactorTrackingOperationResult:
        return self._bound(self._call("/resume", authenticated_actor_id, request), request)

    def submit(
        self,
        request: FactorTrackingRequest,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str,
    ) -> FactorTrackingOperationResult:
        return self._bound(
            self._call("/submit", authenticated_actor_id, request, verified_registry_instance_id),
            request,
        )
