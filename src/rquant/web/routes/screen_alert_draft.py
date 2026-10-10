"""Private C12 import draft boundary, with the original trusted actor protocol."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Request, Response

from rquant.screen.alert_draft import ScreenAlertDraftRequest
from rquant.web.models.screen_alert_draft import (
    ScreenAlertDraftCreateAction,
    ScreenAlertDraftReadAction,
)
from rquant.web.models.screen_history import ScreenQueryReadData
from rquant.web.routes.screen_history import _Csrf, _private_action, _Viewer

router = APIRouter()
MAX_REQUEST_BYTES = 4096


@router.post(
    "/screen/query/alert-draft", response_model=ScreenQueryReadData, summary="带入选股条件提醒草稿"
)
def create_alert_draft(
    request: Request,
    response: Response,
    body: ScreenAlertDraftRequest,
    _viewer: _Viewer,
    _same_site: _Csrf,
) -> ScreenQueryReadData:
    data = _private_action(request, response, _viewer, ScreenAlertDraftCreateAction(request=body))
    if data.alert_draft is None:
        raise HTTPException(
            503, "提醒草稿待确认，请查询原请求。", headers={"Cache-Control": "no-store"}
        )
    return data


@router.get(
    "/screen/query/alert-drafts/{draft_id}",
    response_model=ScreenQueryReadData,
    summary="读取本人提醒草稿",
)
def get_alert_draft(
    request: Request,
    response: Response,
    draft_id: Annotated[str, Path(pattern=r"^[0-9a-f]{24}$")],
    _viewer: _Viewer,
) -> ScreenQueryReadData:
    data = _private_action(
        request, response, _viewer, ScreenAlertDraftReadAction(draft_id=draft_id)
    )
    if data.alert_draft is None:
        raise HTTPException(
            404, "草稿已过期或不可用，请重新带入条件。", headers={"Cache-Control": "no-store"}
        )
    return data
