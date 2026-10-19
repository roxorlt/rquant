"""Local cache reads only; this module never imports the upstream collector."""

from __future__ import annotations

from typing import Annotated, cast

from fastapi import APIRouter, HTTPException, Query, Request

from rquant.l2.cache import MAX_BATCH_PAGE, CacheError, RawL2Cache
from rquant.l2.models import BatchPage, FeedStatus

router = APIRouter(prefix="/api/v1/l2", tags=["l2 raw cache"])


@router.get("/status", response_model=FeedStatus)
def status(request: Request) -> FeedStatus:
    cache = cast(RawL2Cache, request.app.state.l2_cache)
    try:
        return cache.read_status()
    except CacheError as error:
        raise HTTPException(503, detail={"code": error.code}) from None


@router.get("/batches", response_model=BatchPage)
def batches(request: Request, after_sequence: Annotated[int, Query(ge=0)] = 0,
            limit: Annotated[int, Query(ge=1, le=MAX_BATCH_PAGE)] = 1) -> BatchPage:
    cache = cast(RawL2Cache, request.app.state.l2_cache)
    try:
        return cache.read_batches(after_sequence=after_sequence, limit=limit)
    except ValueError:
        raise HTTPException(400, detail={"code": "invalid_cursor"}) from None
    except CacheError as error:
        raise HTTPException(503, detail={"code": error.code}) from None
