"""Request identity and the cross-site guard for write endpoints.

Identity: nginx basic auth writes the authenticated user into ``X-Rquant-User``
(``proxy_set_header X-Rquant-User $remote_user``), overwriting anything the browser sent.

Cross-site guard (``require_csrf``): a write must be JSON, carry ``X-Rquant-Csrf: 1`` and
come from the same site. The custom header forces a CORS preflight that this API never
answers, so another site cannot send it. M0 has no write endpoint; the dependency is here
so the first one (M2) cannot be added without it.
"""

from __future__ import annotations

import re
from urllib.parse import urlsplit

from fastapi import HTTPException, Request, status

USER_HEADER = "x-rquant-user"
CSRF_HEADER = "x-rquant-csrf"
_USER_PATTERN = re.compile(r"^[A-Za-z0-9._@-]{1,64}$")


def current_user(request: Request) -> str | None:
    value = request.headers.get(USER_HEADER)
    if value is None or not _USER_PATTERN.fullmatch(value):
        return None
    return value


def _hostname(value: str) -> str | None:
    try:
        return urlsplit(f"//{value}").hostname
    except ValueError:
        return None


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
        # nginx forwards `Host $host`, which drops the port the browser put in Origin
        # (8081), so the host names are compared, not host:port.
        origin_host = urlsplit(origin).hostname
        request_host = _hostname(request.headers.get("host", ""))
        if origin_host is None or request_host is None or origin_host != request_host:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="跨站请求被拒绝")
