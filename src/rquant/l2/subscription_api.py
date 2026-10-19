"""Explicit local target admission; this router never imports the collector."""
from __future__ import annotations

from typing import Annotated, cast

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator

from rquant.l2.cache import CacheError, RawL2Cache
from rquant.l2.models import FeedStatus, SubscriptionReceipt, SubscriptionTargets, SubscriptionUpdate
from rquant.l2.subscriptions import SubscriptionError, SubscriptionStore
from rquant.l2.symbols import MAX_SYMBOLS, normalize_symbols

router = APIRouter(prefix="/api/v1/l2", tags=["local L2 subscriptions"])


class SubscriptionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    symbols: Annotated[tuple[StrictStr, ...], Field(min_length=1, max_length=MAX_SYMBOLS,
        description="Six ASCII digits starting with 0, 3 or 6; additions form a durable union.")]

    @field_validator("symbols")
    @classmethod
    def normalize(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return normalize_symbols(value)


class SubscriptionView(BaseModel):
    desired: SubscriptionTargets
    runtime: SubscriptionReceipt | None
    runtime_current_epoch: bool
    status: FeedStatus


def control_error(error: SubscriptionError | CacheError) -> HTTPException:
    status = 429 if error.code == "capacity_exceeded" else 422 if error.code == "invalid_symbols" else 503
    return HTTPException(status, detail={"code": error.code})


@router.post("/subscriptions", response_model=SubscriptionUpdate, responses={
    200: {"description": "All symbols already admitted; revision is unchanged."},
    202: {"model": SubscriptionUpdate, "description": "New symbols durably queued; market readiness is separate."},
    429: {"description": "The entire union would exceed the administrator's capacity."},
    503: {"description": "Control state is unavailable; only a fixed error code is returned."},
})
def add_subscriptions(body: SubscriptionRequest, request: Request, response: Response) -> SubscriptionUpdate:
    store = cast(SubscriptionStore, request.app.state.l2_subscriptions)
    try:
        result = store.add(body.symbols)
    except SubscriptionError as error:
        raise control_error(error) from None
    response.status_code = 202 if result.added else 200
    return result


@router.get("/subscriptions", response_model=SubscriptionView, responses={
    503: {"description": "A control or runtime snapshot is unavailable; only a fixed error code is returned."},
})
def subscriptions(request: Request) -> SubscriptionView:
    store = cast(SubscriptionStore, request.app.state.l2_subscriptions)
    cache = cast(RawL2Cache, request.app.state.l2_cache)
    try:
        desired = store.read()
        runtime = cache.read_subscription_status()
        status = cache.read_runtime_status()
    except (SubscriptionError, CacheError) as error:
        raise control_error(error) from None
    return SubscriptionView(desired=desired, runtime=runtime, status=status,
                            runtime_current_epoch=runtime is not None and runtime.epoch_id == status.epoch_id)
