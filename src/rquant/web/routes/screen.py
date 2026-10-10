"""Validated HTTP boundary for manual screening and formula syntax checks."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from threading import BoundedSemaphore
from typing import Annotated, Literal

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Request, Response

from rquant.screen.tdx import parse_formula
from rquant.screen.tdx.tokens import MAX_SOURCE_BYTES
from rquant.web.envelope import Envelope
from rquant.web.models.screen import (
    ScreenCatalogData,
    ScreenNlPreviewData,
    ScreenNlPreviewRequest,
    ScreenRunData,
    ScreenRunRequest,
    TdxParseData,
    TdxParseRequest,
    TdxPreviewData,
    TdxPreviewRequest,
    TdxPreviewSourceData,
)
from rquant.web.nl_parser import NlClarificationNeededError, NlParserUnavailableError
from rquant.web.screen_nl_preview import (
    InvalidScreenDraftError,
    ScreenDateMismatchError,
    validate_screen_draft,
)
from rquant.web.screen_service import ScreenApplicationError
from rquant.web.security import current_user, require_csrf, require_current_user
from rquant.web.serving import serving_meta

router = APIRouter(prefix="/screen")
_MAX_REQUEST_BYTES = 8_192
MAX_NL_REQUEST_BYTES = 4_096


def _catalog_matches(body: ScreenNlPreviewRequest, catalog: ScreenCatalogData, state: str) -> bool:
    return (
        catalog.source_kind == body.source_kind
        and catalog.available
        and catalog.source is not None
        and catalog.source.identity == body.source_identity
        and body.trade_date in catalog.dates
        and (body.source_kind == "replica" or state == "ready")
    )


@router.post("/tdx/parse", response_model=TdxParseData, summary="检查通达信公式")
def parse_tdx_formula(
    body: TdxParseRequest,
    _viewer: Annotated[str, Depends(require_current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> TdxParseData:
    if len(body.source) > MAX_SOURCE_BYTES:
        raise HTTPException(status_code=413, detail="公式太长，请删减后重试。")
    try:
        source_bytes = body.source.encode("utf-8")
    except UnicodeEncodeError:
        return TdxParseData.model_validate(parse_formula(body.source).model_dump())
    if len(source_bytes) > MAX_SOURCE_BYTES:
        raise HTTPException(status_code=413, detail="公式太长，请删减后重试。")
    return TdxParseData.model_validate(parse_formula(body.source).model_dump())


@router.post("/tdx/preview", response_model=TdxPreviewData, summary="单股预览通达信公式")
def preview_tdx_formula(
    request: Request,
    body: TdxPreviewRequest,
    _viewer: Annotated[str, Depends(require_current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> TdxPreviewData:
    try:
        source_bytes = body.source.encode("utf-8")
    except UnicodeEncodeError as error:
        raise HTTPException(status_code=422, detail="公式输入有误，请修改后重试。") from error
    if len(source_bytes) > MAX_SOURCE_BYTES:
        raise HTTPException(status_code=413, detail="公式太长，请删减后重试。")
    if len(body.model_dump_json().encode("utf-8")) > _MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="预览输入太长，请删减后重试。")
    web = request.app.state.web
    with _screen_slot(web.screen_gate):
        try:
            return web.screen_service.preview(body, decision_at=web.clock())
        except ScreenApplicationError as error:
            raise HTTPException(status_code=error.status_code, detail=error.detail) from error


@router.get("/tdx/preview/source", response_model=TdxPreviewSourceData, summary="公式预览数据")
def get_tdx_preview_source(
    request: Request,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> TdxPreviewSourceData:
    web = request.app.state.web
    with _screen_slot(web.screen_gate):
        return web.screen_service.preview_source()


@contextmanager
def _screen_slot(gate: BoundedSemaphore) -> Iterator[None]:
    if not gate.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="正在筛选，请稍后再试。")
    try:
        yield
    finally:
        gate.release()


@router.get("/blocks", response_model=Envelope[ScreenCatalogData], summary="选股条件目录")
def get_blocks(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    mode: Literal["daily", "intraday"] = "daily",
) -> Envelope[ScreenCatalogData]:
    web = request.app.state.web
    with _screen_slot(web.screen_gate), web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        data = web.screen_service.catalog(borrowed, mode=mode)
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    data = data.model_copy(
        update={
            "nl_generate_available": (
                web.ai_assistance_gateway is not None
                and _viewer in web.settings.ai_users
                and mode == "daily"
                and data.available
                and data.source is not None
                and bool(data.dates)
                and (data.source_kind == "replica" or meta.state == "ready")
            )
        }
    )
    return Envelope[ScreenCatalogData](data=data, serving=meta)


@router.post("/nl-preview", response_model=ScreenNlPreviewData, summary="预览一句话选股条件")
async def preview_screen_natural_language(
    request: Request,
    body: ScreenNlPreviewRequest,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> ScreenNlPreviewData:
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if len(await request.body()) > MAX_NL_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="描述过长，请删减后重试。")
    from functools import partial
    from rquant.web.models.ai_assistance import AIScreenRequest, AIScreenDraft
    from rquant.web.models.screen import ScreenCondition
    from rquant.web.routes.ai_assistance import generate_ai, original_header_id
    if request.app.state.web.ai_assistance_gateway is None:
        raise HTTPException(503, "暂不能生成，仍可手动添加条件。")
    original = AIScreenRequest(request_id=original_header_id(request), include_ranking=False, **body.model_dump())
    def new_request_preflight() -> None:
        web = request.app.state.web
        web.tracker.refresh()
        with _screen_slot(web.screen_gate), web.tracker.borrow() as borrowed:
            meta = serving_meta(borrowed, now=web.clock(), stale_after=web.settings.stale_after,
                failure=web.tracker.failure)
            catalog = web.screen_service.catalog(borrowed)
            if not _catalog_matches(body, catalog, meta.state):
                raise HTTPException(409, "选股数据已更新，请刷新条件后重试。")
    data = await anyio.to_thread.run_sync(partial(generate_ai, request, viewer, original,
        new_request_preflight=new_request_preflight))
    view = data.request
    if view is None or view.state != "completed":
        raise HTTPException(503, "调用结果未知，请继续查看原请求。")
    if not isinstance(view.result, AIScreenDraft):
        raise HTTPException(422, "没能生成可用条件，请换一种说法。")
    definition = view.result.definition
    return ScreenNlPreviewData(source_kind=definition.source_kind, source_identity=definition.source_identity,
        trade_date=definition.trade_date, conditions=[ScreenCondition(key=c.name, args=c.args) for c in definition.conditions])


@router.post("/run", response_model=Envelope[ScreenRunData], summary="运行选股条件")
def run_screen(
    request: Request,
    response: Response,
    body: ScreenRunRequest,
    _viewer: Annotated[str, Depends(require_current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> Envelope[ScreenRunData]:
    web = request.app.state.web
    if len(body.model_dump_json().encode("utf-8")) > _MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="条件太多，请减少后重试。")
    with _screen_slot(web.screen_gate), web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        try:
            data = web.screen_service.run(
                body,
                borrowed=borrowed,
                serving_unavailable=meta.state == "unavailable",
            )
        except ScreenApplicationError as error:
            raise HTTPException(status_code=error.status_code, detail=error.detail) from error
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[ScreenRunData](data=data, serving=meta)
