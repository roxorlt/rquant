"""Validated HTTP boundary for manual screening and formula syntax checks."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from threading import BoundedSemaphore
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from rquant.screen.tdx import parse_formula
from rquant.screen.tdx.tokens import MAX_SOURCE_BYTES
from rquant.web.envelope import Envelope
from rquant.web.models.screen import (
    ScreenCatalogData,
    ScreenRunData,
    ScreenRunRequest,
    TdxParseData,
    TdxParseRequest,
    TdxPreviewData,
    TdxPreviewRequest,
)
from rquant.web.screen_service import ScreenApplicationError
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import serving_meta

router = APIRouter(prefix="/screen")
_MAX_REQUEST_BYTES = 8_192


@router.post("/tdx/parse", response_model=TdxParseData, summary="检查通达信公式")
def parse_tdx_formula(
    body: TdxParseRequest,
    _viewer: Annotated[str | None, Depends(current_user)],
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
    _viewer: Annotated[str | None, Depends(current_user)],
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
    return Envelope[ScreenCatalogData](data=data, serving=meta)


@router.post("/run", response_model=Envelope[ScreenRunData], summary="运行选股条件")
def run_screen(
    request: Request,
    response: Response,
    body: ScreenRunRequest,
    _viewer: Annotated[str | None, Depends(current_user)],
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
