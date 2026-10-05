"""Private Serving-only price runtime and recent-event reads."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger

from rquant.runtime_contracts import normalize_aware_utc
from rquant.web.envelope import Envelope, ServingState
from rquant.web.models.price_alert_runtime import PriceAlertRecentEventsData, PriceAlertRuntimeData
from rquant.web.price_alert_commands import private_meta
from rquant.web.price_alert_runtime_read import PriceAlertRuntimeDrift, read_price_alert_runtime
from rquant.web.routes.price_alert_rules import _viewer

router = APIRouter(prefix="/monitor/price-rules")


def _read(
    request: Request, owner: str, *, events: bool
) -> Envelope[PriceAlertRuntimeData] | Envelope[PriceAlertRecentEventsData]:
    web = request.app.state.web
    now = normalize_aware_utc(web.clock())
    with web.tracker.borrow() as borrowed:
        meta = private_meta(request, borrowed, now)
        runtime = PriceAlertRuntimeData(
            availability="unavailable",
            generation_id=None,
            status="error",
            status_label="异常",
            message="运行状态暂不可用，请稍后重试。",
            evaluated_at=None,
            quote_updated_at=None,
            applied_at=None,
            mode="disabled",
            items=[],
        )
        recent = PriceAlertRecentEventsData(
            availability="unavailable",
            generation_id=None,
            message="提醒记录暂不可用，请稍后重试。",
            items=[],
        )
        if borrowed is not None and meta.state is ServingState.READY:
            try:
                runtime, recent = read_price_alert_runtime(borrowed, owner_id=owner, now=now)
            except PriceAlertRuntimeDrift as error:
                raise HTTPException(
                    409, "设置已更新，等待运行端同步。已排队的提醒可能发送。"
                ) from error
            except Exception:
                logger.exception("Private price runtime source unavailable")
        return Envelope(serving=meta, data=recent if events else runtime)


@router.get(
    "/runtime", response_model=Envelope[PriceAlertRuntimeData], summary="我的到价提醒运行状态"
)
def price_runtime(
    request: Request, owner: Annotated[str, Depends(_viewer)]
) -> Envelope[PriceAlertRuntimeData]:
    return _read(request, owner, events=False)


@router.get(
    "/events", response_model=Envelope[PriceAlertRecentEventsData], summary="我的最近到价提醒"
)
def price_events(
    request: Request, owner: Annotated[str, Depends(_viewer)]
) -> Envelope[PriceAlertRecentEventsData]:
    return _read(request, owner, events=True)
