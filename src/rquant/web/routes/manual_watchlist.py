"""Private, read-only manual watchlist endpoints."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Request
from loguru import logger

from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_manual_watchlist_projection import ManualWatchlistProjectionRow
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.manual_watchlist_read import read_manual_watchlist, read_manual_watchlist_item
from rquant.web.models.manual_watchlist import (
    ManualWatchlistExactData,
    ManualWatchlistItemData,
    ManualWatchlistListData,
)
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/watchlist")
_UNAVAILABLE = "名单暂不可用，请稍后重试。"


def _viewer(request: Request, viewer: Annotated[str | None, Depends(current_user)]) -> str:
    if request.app.state.web.settings.ingress_socket_path is None:
        raise HTTPException(status_code=503, detail="名单暂未开放。")
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if request.query_params:
        raise HTTPException(status_code=422, detail="查询条件有误，请刷新后重试。")
    return viewer


def _meta(request: Request, borrowed: BorrowedGeneration | None, now: datetime) -> ServingMeta:
    web = request.app.state.web
    meta = serving_meta(
        borrowed,
        now=now,
        stale_after=web.settings.stale_after,
        failure=web.tracker.failure,
    )
    # Technical diagnostics belong in server logs, never in private page responses.
    return meta.model_copy(update={"detail": ""})


def _item(row: ManualWatchlistProjectionRow) -> ManualWatchlistItemData:
    assert row.source is not None and row.updated_at is not None
    return ManualWatchlistItemData(
        ts_code=row.ts_code,
        version=row.version,
        source=row.source,
        price_levels=json.loads(row.price_levels_json),
        expires_at=row.expires_at,
        updated_at=row.updated_at,
    )


@router.get("", response_model=Envelope[ManualWatchlistListData], summary="我的盯盘名单")
def list_manual_watchlist(
    request: Request,
    owner_id: Annotated[str, Depends(_viewer)],
) -> Envelope[ManualWatchlistListData]:
    web = request.app.state.web
    now = normalize_aware_utc(web.clock())
    with web.tracker.borrow() as borrowed:
        meta = _meta(request, borrowed, now)
        if borrowed is not None and meta.state is ServingState.READY:
            try:
                available_at, rows = read_manual_watchlist(borrowed, owner_id=owner_id, now=now)
            except Exception:
                logger.exception("Private manual watchlist list read failed")
                available_at, rows = None, ()
            if available_at is not None:
                items = [_item(row) for row in rows]
                return Envelope(
                    data=ManualWatchlistListData(
                        availability="ready",
                        message="名单为空。" if not items else "",
                        available_at=available_at,
                        items=items,
                    ),
                    serving=meta,
                )
    return Envelope(
        data=ManualWatchlistListData(
            availability="unavailable", message=_UNAVAILABLE, available_at=None, items=[]
        ),
        serving=meta,
    )


@router.get("/{ts_code}", response_model=Envelope[ManualWatchlistExactData], summary="盯盘状态")
def get_manual_watchlist_item(
    request: Request,
    owner_id: Annotated[str, Depends(_viewer)],
    ts_code: Annotated[str, Path(min_length=9, max_length=9, pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")],
) -> Envelope[ManualWatchlistExactData]:
    web = request.app.state.web
    now = normalize_aware_utc(web.clock())
    with web.tracker.borrow() as borrowed:
        meta = _meta(request, borrowed, now)
        if borrowed is not None and meta.state is ServingState.READY:
            try:
                available_at, row = read_manual_watchlist_item(
                    borrowed, owner_id=owner_id, ts_code=ts_code
                )
            except Exception:
                logger.exception("Private manual watchlist exact read failed")
                available_at, row = None, None
            if available_at is not None:
                status = "absent"
                if row is not None:
                    status = (
                        "deleted"
                        if row.deleted
                        else "expired"
                        if row.expires_at is not None and row.expires_at <= now
                        else "active"
                    )
                return Envelope(
                    data=ManualWatchlistExactData(
                        availability="ready",
                        message="",
                        available_at=available_at,
                        ts_code=ts_code,
                        status=status,
                        version=None if row is None else row.version,
                        source=None if row is None else row.source,
                        price_levels=[] if row is None else json.loads(row.price_levels_json),
                        expires_at=None if row is None else row.expires_at,
                        updated_at=None if row is None else row.updated_at,
                    ),
                    serving=meta,
                )
    return Envelope(
        data=ManualWatchlistExactData(
            availability="unavailable",
            message=_UNAVAILABLE,
            available_at=None,
            ts_code=ts_code,
            status=None,
            version=None,
            source=None,
            price_levels=[],
            expires_at=None,
            updated_at=None,
        ),
        serving=meta,
    )
