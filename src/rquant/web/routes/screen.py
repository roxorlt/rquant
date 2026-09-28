"""Validated HTTP boundary for manual screening and formula syntax checks."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from threading import BoundedSemaphore
from typing import Annotated

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
) -> Envelope[ScreenCatalogData]:
    web = request.app.state.web
    with _screen_slot(web.screen_gate), web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        data = web.screen_service.catalog(borrowed)
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    data = data.model_copy(
        update={
            "nl_generate_available": (
                web.nl_parser is not None
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
    web = request.app.state.web
    parser = web.nl_parser
    if parser is None:
        raise HTTPException(status_code=503, detail="暂不能生成，仍可手动添加条件。")
    with _screen_slot(web.screen_gate), web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        catalog = web.screen_service.catalog(borrowed)
        if not _catalog_matches(body, catalog, meta.state):
            raise HTTPException(status_code=409, detail="选股数据已更新，请刷新条件后重试。")
    if not web.nl_gate.acquire(blocking=False):
        raise HTTPException(
            status_code=429, detail="正在生成，请稍后再试。", headers={"Retry-After": "1"}
        )
    try:
        if not web.nl_rate_limiter.admit(viewer):
            raise HTTPException(
                status_code=429,
                detail="操作太频繁，请一分钟后再试。",
                headers={"Retry-After": "60"},
            )
        try:
            raw = await anyio.to_thread.run_sync(
                parser.parse_new, body.instruction.strip(), body.trade_date.isoformat()
            )
        except NlClarificationNeededError as error:
            raise HTTPException(status_code=422, detail="请说清想筛选的股票条件。") from error
        except (NlParserUnavailableError, TimeoutError) as error:
            raise HTTPException(status_code=503, detail="暂不能生成，请稍后重试。") from error
        with _screen_slot(web.screen_gate), web.tracker.borrow() as borrowed:
            meta = serving_meta(
                borrowed,
                now=web.clock(),
                stale_after=web.settings.stale_after,
                failure=web.tracker.failure,
            )
            current = web.screen_service.catalog(borrowed)
            if not _catalog_matches(body, current, meta.state) or current.blocks != catalog.blocks:
                raise HTTPException(status_code=409, detail="选股数据已更新，请刷新条件后重试。")
        try:
            conditions = validate_screen_draft(raw, current, body.trade_date)
        except ScreenDateMismatchError as error:
            raise HTTPException(status_code=422, detail="请先选择想筛选的日期。") from error
        except InvalidScreenDraftError as error:
            raise HTTPException(
                status_code=422, detail="没能生成可用条件，请换一种说法。"
            ) from error
        return ScreenNlPreviewData(
            source_kind=body.source_kind,
            source_identity=body.source_identity,
            trade_date=body.trade_date,
            conditions=conditions,
        )
    finally:
        web.nl_gate.release()


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
