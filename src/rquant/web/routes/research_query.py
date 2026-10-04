"""Authenticated forwarding only: SQL never opens a database in this API."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request

from rquant.page_control import SaveResearchQuery
from rquant.research_query.contracts import MAX_RESULT_BYTES, QueryRequest, QueryResult
from rquant.research_query.service import (
    MAX_REQUEST_BYTES as MAX_REQUEST_BYTES,
)
from rquant.research_query.service import (
    QueryAdmissionRejectedError,
    QueryAdmissionUnavailableError,
    QueryCatalogData,
    QuerySaveData,
    QuerySavedList,
)
from rquant.web.envelope import Envelope, ServingMeta
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import serving_meta

router = APIRouter(prefix="/research")


def _actor(request: Request, viewer: str | None) -> str:
    if viewer is None:
        raise HTTPException(401, "请先登录")
    if viewer not in request.app.state.web.settings.research_query_users:
        raise HTTPException(403, "你没有查询权限。")
    return viewer


def _meta(request: Request) -> ServingMeta:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        return serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )


@router.get("/catalog", response_model=Envelope[QueryCatalogData], summary="可用查询表")
def catalog(
    request: Request, viewer: Annotated[str | None, Depends(current_user)]
) -> Envelope[QueryCatalogData]:
    actor = _actor(request, viewer)
    web = request.app.state.web
    if web.research_query_client is None:
        return Envelope(
            data=QueryCatalogData(available=False, message="查询服务尚未启用。"),
            serving=_meta(request),
        )
    try:
        data = web.research_query_client.catalog(authenticated_actor_id=actor)
        data = data.model_copy(update={"save_enabled": web.research_query_save_client is not None})
    except QueryAdmissionRejectedError as exc:
        raise HTTPException(403, "你没有查询权限。") from exc
    except QueryAdmissionUnavailableError:
        data = QueryCatalogData(available=False, message="查询数据暂时不可用，请稍后重试。")
    return Envelope(data=data, serving=_meta(request))


@router.post(
    "/query",
    response_model=Envelope[QueryResult],
    dependencies=[Depends(require_csrf)],
    summary="运行只读查询或执行计划",
)
def execute(
    query: QueryRequest, request: Request, viewer: Annotated[str | None, Depends(current_user)]
) -> Envelope[QueryResult]:
    actor = _actor(request, viewer)
    client = request.app.state.web.research_query_client
    if client is None:
        raise HTTPException(503, "查询服务尚未启用。")
    try:
        data = client.execute(query, authenticated_actor_id=actor)
    except QueryAdmissionRejectedError as exc:
        raise HTTPException(403, "查询权限未通过核验。") from exc
    except QueryAdmissionUnavailableError as exc:
        raise HTTPException(503, "查询服务暂时不可用，请稍后重试。") from exc
    result = Envelope(data=data, serving=_meta(request))
    if len(result.model_dump_json().encode("utf-8")) > MAX_RESULT_BYTES:
        raise HTTPException(503, "结果超过响应限制，请减少返回内容。")
    return result


@router.get("/queries", response_model=Envelope[QuerySavedList], summary="我的已保存查询")
def saved(
    request: Request, viewer: Annotated[str | None, Depends(current_user)]
) -> Envelope[QuerySavedList]:
    actor = _actor(request, viewer)
    client = request.app.state.web.research_query_save_client
    if client is None:
        return Envelope(
            data=QuerySavedList(available=False, message="查询保存尚未启用。"),
            serving=_meta(request),
        )
    try:
        data = client.saved(authenticated_actor_id=actor)
    except QueryAdmissionRejectedError as exc:
        raise HTTPException(403, "查询保存权限未通过核验。") from exc
    except QueryAdmissionUnavailableError as exc:
        raise HTTPException(503, "已保存查询暂时无法读取，请重试。") from exc
    return Envelope(data=data, serving=_meta(request))


def _save(
    command: SaveResearchQuery, request: Request, viewer: str | None, *, resume: bool
) -> Envelope[QuerySaveData]:
    actor = _actor(request, viewer)
    client = request.app.state.web.research_query_save_client
    if client is None:
        raise HTTPException(503, "查询保存尚未启用。")
    try:
        data = client.save(command, authenticated_actor_id=actor, resume=resume)
    except QueryAdmissionRejectedError as exc:
        raise HTTPException(409, "保存身份或原命令不匹配，请重新载入查询。") from exc
    except QueryAdmissionUnavailableError as exc:
        raise HTTPException(503, "保存结果暂时无法确认，请按原命令恢复。") from exc
    return Envelope(data=data, serving=_meta(request))


@router.post(
    "/queries/save",
    response_model=Envelope[QuerySaveData],
    dependencies=[Depends(require_csrf)],
    summary="保存常用查询",
)
def save(
    command: SaveResearchQuery,
    request: Request,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[QuerySaveData]:
    return _save(command, request, viewer, resume=False)


@router.post(
    "/queries/resume",
    response_model=Envelope[QuerySaveData],
    dependencies=[Depends(require_csrf)],
    summary="恢复原保存命令",
)
def resume(
    command: SaveResearchQuery,
    request: Request,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[QuerySaveData]:
    return _save(command, request, viewer, resume=True)
