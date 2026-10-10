"""Verified proxy identity and the cross-site guard for write endpoints.

nginx overwrites both the user and proxy-proof headers after Basic Auth. An unproved
user header never creates an authenticated owner, including on loopback connections.

Cross-site guard (``require_csrf``): a write must be JSON, carry ``X-Rquant-Csrf: 1`` and
come from the same site. The custom header forces a CORS preflight that this API never
answers, so another site cannot send it. This guard does not grant identity.
"""

from __future__ import annotations

import re
from typing import Annotated
from collections.abc import AsyncIterator
from urllib.parse import urlsplit

from fastapi import Depends, HTTPException, Request, status

from rquant.web.proxy_identity import PROXY_PROOF_HEADER, ProxyIdentityVerifier
from rquant.collaboration_commands import COLLABORATION_POLICY
from rquant.web.collaboration_gateway import COLLABORATION_ACTOR, CollaborationGateway, CollaborationUnavailableError
from rquant.web.models.collaboration import CollaborationMe

USER_HEADER = "x-rquant-user"
CSRF_HEADER = "x-rquant-csrf"
_USER_PATTERN = re.compile(r"^[A-Za-z0-9._@-]{1,64}$")


def current_user(request: Request) -> str | None:
    context = getattr(request.app.state, "web", None)
    if context is None or context.settings.ingress_socket_path is None:
        return None
    verifier = context.proxy_identity
    if not isinstance(verifier, ProxyIdentityVerifier):
        return None
    headers = request.scope.get("headers", ())
    users = [value for key, value in headers if key.lower() == USER_HEADER.encode("ascii")]
    proofs = [value for key, value in headers if key.lower() == PROXY_PROOF_HEADER]
    if len(users) != 1 or len(proofs) != 1 or len(users[0]) > 64:
        return None
    try:
        value = users[0].decode("ascii")
    except UnicodeDecodeError:
        return None
    if _USER_PATTERN.fullmatch(value) is None or not verifier.verify(proofs[0]):
        return None
    return value


def require_current_user(viewer: Annotated[str | None, Depends(current_user)]) -> str:
    if viewer is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="请先登录")
    return viewer


def collaboration_me(request: Request, actor: str) -> CollaborationMe:
    web = request.app.state.web
    if web.settings.collaboration_mode != "enforced":
        return CollaborationMe(available=False, mode="legacy", username=actor,
            message="协作权限尚未启用。")
    gateway = web.collaboration
    if not isinstance(gateway, CollaborationGateway):
        raise HTTPException(503, "权限服务暂不可用。")
    try:
        return gateway.me(actor)
    except PermissionError as exc:
        raise HTTPException(403, "当前账号没有访问权限。") from exc
    except (ValueError, OSError, CollaborationUnavailableError) as exc:
        raise HTTPException(503, "权限服务暂不可用。") from exc


async def require_collaboration_access(request: Request) -> AsyncIterator[None]:
    """Match the original APIRoute descriptor; unknown operations fail closed."""
    web = request.app.state.web
    if web.settings.collaboration_mode != "enforced":
        yield
        return
    actor = current_user(request)
    if actor is None:
        raise HTTPException(401, "请先登录。")
    import anyio.to_thread
    me = await anyio.to_thread.run_sync(collaboration_me, request, actor)
    route = request.scope.get("route")
    descriptor = getattr(route, "path", None)
    # FastAPI's included routers keep the original child APIRoute in scope.
    # Its server-created effective context carries the actual prefixed template.
    effective = request.scope.get("fastapi", {}).get("effective_route_context")
    if effective is not None and getattr(effective, "original_route", None) is route:
        descriptor = getattr(effective, "path", None)
    allowed = next((rule.roles for rule in COLLABORATION_POLICY.entries
        if (rule.method, rule.path, rule.command_kind) == (request.method, descriptor, None)), ())
    if me.role not in allowed:
        raise HTTPException(403, "当前角色不能执行此操作。")
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        require_csrf(request)
    request.state.collaboration = me
    token = COLLABORATION_ACTOR.set(actor)
    try:
        yield
    finally:
        COLLABORATION_ACTOR.reset(token)


_DEFAULT_PORTS = {"http": 80, "https": 443}


def _origin_authority(value: str) -> tuple[str, str, int] | None:
    """``(scheme, host, port)`` of an Origin header, with the scheme's default port."""

    try:
        parts = urlsplit(value)
        port = parts.port
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if parts.hostname is None or scheme not in _DEFAULT_PORTS:
        return None
    return scheme, parts.hostname, port if port is not None else _DEFAULT_PORTS[scheme]


def _host_authority(value: str, scheme: str) -> tuple[str, int] | None:
    """``(host, port)`` of a Host header; a missing port is the scheme's default."""

    try:
        parts = urlsplit(f"//{value}")
        port = parts.port
    except ValueError:
        return None
    if parts.hostname is None:
        return None
    return parts.hostname, port if port is not None else _DEFAULT_PORTS[scheme]


def require_csrf(request: Request) -> None:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="写接口只接受 application/json",
        )
    if request.headers.get(CSRF_HEADER) != "1":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="缺少 X-Rquant-Csrf 头")
    fetch_site = request.headers.get("sec-fetch-site")
    if fetch_site is not None and fetch_site != "same-origin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="跨站请求被拒绝")
    origin = request.headers.get("origin")
    if origin is not None:
        # nginx forwards `Host $host:$server_port` (deploy/nginx/rquant-backup.conf), so the
        # port the browser put in Origin (8081) survives and host *and* port are compared.
        origin_authority = _origin_authority(origin)
        if origin_authority is None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="跨站请求被拒绝")
        scheme, origin_host, origin_port = origin_authority
        request_authority = _host_authority(request.headers.get("host", ""), scheme)
        if request_authority != (origin_host, origin_port):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="跨站请求被拒绝")
