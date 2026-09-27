"""Protected, read-only projection of registered service journal events."""

from __future__ import annotations

import os
import re
import socket
import stat
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from rquant.ops_status import load_signed_ops_manifest
from rquant.unit_log_reader import JournalPage
from rquant.unit_log_service import UnitLogServiceError
from rquant.web.security import current_user
from rquant.web.service_log_access_audit import ServiceLogAccessRecord

router = APIRouter(prefix="/tasks")

_SEVEN_DAYS = timedelta(days=7)
_QUERY_FIELDS = frozenset({"since", "level", "page_size", "cursor"})
_SINCE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})\Z"
)
_PAGE_SIZE = re.compile(r"[1-9][0-9]{0,2}\Z")
_UNAVAILABLE = "运行日志暂不可用，请稍后重试。"
_INVALID = "日志筛选条件有误，请检查后重试。"

LogLevel = Literal["emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"]


class _LogQuery(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    since: AwareDatetime
    level: LogLevel | None = None
    page_size: int = Field(default=100, ge=1, le=498)
    cursor: str | None = Field(default=None, min_length=1, max_length=4096)


def _parse_query(request: Request, *, now: datetime) -> _LogQuery:
    pairs = request.query_params.multi_items()
    values = dict(pairs)
    raw_since = values.get("since", "")
    raw_size = values.get("page_size", "100")
    if (
        len(pairs) != len(values)
        or set(values) - _QUERY_FIELDS
        or _SINCE.fullmatch(raw_since) is None
        or _PAGE_SIZE.fullmatch(raw_size) is None
    ):
        raise HTTPException(status_code=422, detail=_INVALID)
    try:
        query = _LogQuery.model_validate(values)
    except (TypeError, ValueError, ValidationError):
        raise HTTPException(status_code=422, detail=_INVALID) from None
    if now.tzinfo is None or not now - _SEVEN_DAYS <= query.since <= now:
        raise HTTPException(status_code=422, detail=_INVALID)
    return query


def _read_public_key(path: Path) -> bytes:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= 8192:
            raise ValueError("invalid public key file")
        payload = os.read(descriptor, 8193)
        after = os.fstat(descriptor)
        if len(payload) != before.st_size or (
            before.st_dev,
            before.st_ino,
            before.st_mtime_ns,
            before.st_size,
        ) != (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_size):
            raise ValueError("public key file changed during read")
        return payload
    finally:
        os.close(descriptor)


def _installed_unit(request: Request, unit: str) -> bool:
    settings = request.app.state.web.settings
    assert settings.unit_log_manifest_path is not None
    assert settings.unit_log_public_key_path is not None
    assert settings.unit_log_expected_host is not None
    try:
        if socket.gethostname() != settings.unit_log_expected_host:
            raise ValueError("service log host changed")
        public_key = _read_public_key(settings.unit_log_public_key_path)
        manifest, _digest = load_signed_ops_manifest(
            settings.unit_log_manifest_path,
            public_key_pem=public_key,
            expected_host=settings.unit_log_expected_host,
        )
    except Exception:
        raise HTTPException(status_code=503, detail=_UNAVAILABLE) from None
    return any(item.service == unit for item in manifest.units)


@contextmanager
def _admitted(gate: threading.BoundedSemaphore) -> Iterator[None]:
    if not gate.acquire(blocking=False):
        raise HTTPException(
            status_code=429,
            detail="请求较多，请稍后重试。",
            headers={"Retry-After": "1"},
        )
    try:
        yield
    finally:
        gate.release()


@router.get(
    "/services/{unit}/logs",
    response_model=JournalPage,
    summary="服务运行日志",
    openapi_extra={
        "parameters": [
            {
                "name": "since",
                "in": "query",
                "required": True,
                "schema": {"type": "string", "format": "date-time"},
            },
            {
                "name": "level",
                "in": "query",
                "required": False,
                "schema": {
                    "type": "string",
                    "enum": ["emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"],
                },
            },
            {
                "name": "page_size",
                "in": "query",
                "required": False,
                "schema": {"type": "integer", "minimum": 1, "maximum": 498, "default": 100},
            },
            {
                "name": "cursor",
                "in": "query",
                "required": False,
                "schema": {"type": "string", "minLength": 1, "maxLength": 4096},
            },
        ]
    },
)
def get_service_logs(
    unit: str,
    request: Request,
    viewer: Annotated[str | None, Depends(current_user)],
) -> JournalPage:
    web = request.app.state.web
    settings = web.settings
    if (
        settings.ingress_socket_path is None
        or not settings.log_admin_users
        or settings.unit_log_socket_path is None
        or web.unit_log_client is None
        or web.unit_log_access_audit is None
        or not settings.unit_log_verified_units
    ):
        raise HTTPException(status_code=503, detail="运行日志尚未开放。")
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if viewer not in settings.log_admin_users:
        raise HTTPException(status_code=403, detail="当前账号不能查看运行日志。")
    if unit not in settings.unit_log_verified_units:
        raise HTTPException(status_code=403, detail="当前服务的运行日志尚未开放。")
    now = web.clock()
    query = _parse_query(request, now=now)
    with _admitted(web.unit_log_gate):
        if not _installed_unit(request, unit):
            raise HTTPException(status_code=403, detail="当前服务的运行日志尚未开放。")
        try:
            recorded = web.unit_log_access_audit.record(
                ServiceLogAccessRecord(operator=viewer, unit=unit, at=now)
            )
            if recorded is not None:
                raise ValueError("access audit did not acknowledge the record")
        except Exception:
            raise HTTPException(status_code=503, detail=_UNAVAILABLE) from None
        try:
            page = web.unit_log_client.read(
                unit=unit,
                since=query.since.astimezone(UTC),
                level=query.level,
                page_size=query.page_size,
                cursor=query.cursor,
            )
            if type(page) is not JournalPage:
                raise ValueError("invalid journal page")
            return page
        except UnitLogServiceError as error:
            code = error.code
            if code == "cursor_changed":
                raise HTTPException(status_code=409, detail="日志已更新，请重新查看。") from None
            if code == "busy":
                raise HTTPException(
                    status_code=429,
                    detail="请求较多，请稍后重试。",
                    headers={"Retry-After": "1"},
                ) from None
            if code == "forbidden":
                raise HTTPException(status_code=403, detail="当前账号不能查看运行日志。") from None
            if code == "invalid_request":
                raise HTTPException(status_code=422, detail=_INVALID) from None
            raise HTTPException(status_code=503, detail=_UNAVAILABLE) from None
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=_UNAVAILABLE
            ) from None
