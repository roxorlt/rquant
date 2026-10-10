"""Current collaboration roles and bounded original command audit."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Annotated, TypeVar

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel

from rquant.collaboration_commands import IssuedRolePreparation, SetUserRoleRequest
from rquant.collaboration_roles import RoleState
from rquant.command_audit_projection import CommandAuditPage, CommandAuditQuery
from rquant.page_control import PageControlReceipt
from rquant.web.collaboration_gateway import CollaborationGateway, CollaborationUnavailableError
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.models.collaboration import (
    CollaborationMe,
    CollaborationRoleLookup,
    CollaborationRoleSubmit,
    RoleLookupData,
)
from rquant.web.security import collaboration_me, current_user, require_csrf

router = APIRouter(prefix="/collaboration")
ModelT = TypeVar("ModelT", bound=BaseModel)


def _actor(viewer: str | None) -> str:
    if viewer is None:
        raise HTTPException(401, "请先登录。")
    return viewer


def _gateway(request: Request) -> CollaborationGateway:
    web = request.app.state.web
    if web.settings.collaboration_mode != "enforced" or type(web.collaboration) is not CollaborationGateway:
        raise HTTPException(503, "协作权限尚未启用。")
    return web.collaboration


def _read(operation: Callable[[], ModelT]) -> ModelT:
    try:
        return operation()
    except PermissionError as exc:
        raise HTTPException(403, "当前角色不能执行此操作。") from exc
    except LookupError as exc:
        raise HTTPException(404, "找不到原请求。") from exc
    except ValueError as exc:
        raise HTTPException(409, "权限或原请求已变化，请刷新后重试。") from exc
    except (OSError, CollaborationUnavailableError) as exc:
        raise HTTPException(503, "权限服务暂不可用。") from exc


def _envelope(request: Request, data: ModelT) -> Envelope[ModelT]:
    digest = getattr(data, "content_sha256", getattr(data, "source_generation", getattr(data, "state_sha256", None)))
    return Envelope(data=data, serving=ServingMeta(generation_id=digest,
        built_at=request.app.state.web.clock(), age_seconds=0, state=ServingState.READY,
        message=None, detail=""))


@router.get("/me", response_model=Envelope[CollaborationMe], summary="当前角色与协作能力")
def me(request: Request, viewer: Annotated[str | None, Depends(current_user)]) -> Envelope[CollaborationMe]:
    actor = _actor(viewer)
    if request.app.state.web.settings.collaboration_mode == "legacy":
        return _envelope(request, CollaborationMe(available=False, mode="legacy", username=actor,
            message="协作权限尚未启用。"))
    return _envelope(request, collaboration_me(request, actor))


@router.get("/users", response_model=Envelope[RoleState], summary="已安装协作者与角色")
def users(request: Request, viewer: Annotated[str | None, Depends(current_user)]) -> Envelope[RoleState]:
    actor = _actor(viewer)
    return _envelope(request, _read(lambda: _gateway(request).users(actor)))


@router.post("/roles/prepare", response_model=Envelope[IssuedRolePreparation], summary="准备角色调整", dependencies=[Depends(require_csrf)])
def prepare(request: Request, body: SetUserRoleRequest, viewer: Annotated[str | None, Depends(current_user)]) -> Envelope[IssuedRolePreparation]:
    actor = _actor(viewer)
    return _envelope(request, _read(lambda: _gateway(request).prepare(actor, body)))


@router.post("/roles/commands", response_model=Envelope[PageControlReceipt], summary="确认角色调整", dependencies=[Depends(require_csrf)])
def submit(request: Request, body: CollaborationRoleSubmit, viewer: Annotated[str | None, Depends(current_user)]) -> Envelope[PageControlReceipt]:
    actor = _actor(viewer)
    def original() -> PageControlReceipt:
        return PageControlReceipt.model_validate_json(_gateway(request).submit_role(actor, body))
    return _envelope(request, _read(original))


@router.post("/roles/lookup", response_model=Envelope[RoleLookupData], summary="查看原角色请求", dependencies=[Depends(require_csrf)])
def lookup(request: Request, body: CollaborationRoleLookup, viewer: Annotated[str | None, Depends(current_user)]) -> Envelope[RoleLookupData]:
    actor = _actor(viewer)
    return _envelope(request, _read(lambda: _gateway(request).lookup_role(actor, body)))


@router.get("/audit", response_model=Envelope[CommandAuditPage], summary="原命令操作记录")
def audit(request: Request, response: Response, viewer: Annotated[str | None, Depends(current_user)],
          limit: Annotated[int, Query(ge=1, le=100)] = 50,
          actor_id: Annotated[str | None, Query(max_length=64)] = None,
          command_kind: Annotated[str | None, Query(max_length=80)] = None,
          time_from: datetime | None = None, time_until: datetime | None = None,
          cursor: Annotated[str | None, Query(max_length=4096)] = None) -> Envelope[CommandAuditPage]:
    actor = _actor(viewer)
    def original() -> CommandAuditPage:
        query = CommandAuditQuery(limit=limit, actor_id=actor_id, command_kind=command_kind,
            time_from=time_from, time_until=time_until, cursor=cursor)
        return _gateway(request).audit(actor, query)
    response.headers["Cache-Control"] = "no-store"
    return _envelope(request, _read(original))
