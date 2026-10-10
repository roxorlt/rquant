"""Private screening commands and server-owned execution history."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from rquant.page_control import PageControlCommandConflictError
from rquant.screen.query_admission import (
    MAX_REQUEST_BYTES,
    ScreenQueryAdmissionRejectedError,
    ScreenQueryAdmissionUnavailableError,
)
from rquant.screen.query_contracts import ExecuteScreenQuery
from rquant.web.models.screen_history import (
    ScreenExecuteAction,
    ScreenExecutionAction,
    ScreenHistoryAction,
    ScreenLookupAction,
    ScreenPresetsAction,
    ScreenPresetSaveAction,
    ScreenPresetSaveRequest,
    ScreenQueryAction,
    ScreenQueryReadData,
    ScreenResultsAction,
    ScreenResumeAction,
)
from rquant.web.security import require_csrf, require_current_user

router = APIRouter()
_Viewer = Annotated[str, Depends(require_current_user)]
_Csrf = Annotated[None, Depends(require_csrf)]


def _private_action(
    request: Request, response: Response, actor: str, action: ScreenQueryAction
) -> ScreenQueryReadData:
    response.headers["Cache-Control"] = "no-store"
    web = request.app.state.web

    def reject(status: int, detail: str) -> None:
        raise HTTPException(
            status_code=status, detail=detail, headers={"Cache-Control": "no-store"}
        )

    if web.screen_query_client is None:
        reject(503, "选股历史暂不可用，请稍后重试。")
    if actor not in web.settings.screen_query_users:
        reject(403, "你没有选股权限。")
    if len(action.model_dump_json().encode()) > MAX_REQUEST_BYTES:
        reject(413, "条件太多，请减少后重试。")
    try:
        data = web.screen_query_client.request(action, authenticated_actor_id=actor)
    except PageControlCommandConflictError:
        reject(409, "原请求已改变，请重新运行。")
    except ScreenQueryAdmissionRejectedError:
        reject(422, "选股请求有误，请检查后重试。")
    except ScreenQueryAdmissionUnavailableError:
        reject(503, "结果待确认，请查询原请求。")
    if type(action) in (ScreenLookupAction, ScreenResumeAction) and data.receipt is None:
        reject(404, "未找到原请求。")
    if (
        type(action) in (ScreenExecuteAction, ScreenPresetSaveAction)
        and data.receipt is not None
        and data.receipt.status.value == "failed"
    ):
        code = data.receipt.result.get("code") if isinstance(data.receipt.result, dict) else None
        if code in {"version_conflict", "source_expired"}:
            reject(409, "条件或数据已更新，请刷新后重试。")
        reject(503, "本次结果未确认，请查看原请求。")
    return data


@router.get("/screen/query/history", response_model=ScreenQueryReadData, summary="读取本人选股历史")
def get_screen_history(
    request: Request,
    response: Response,
    _viewer: _Viewer,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: Annotated[str | None, Query(max_length=1024)] = None,
) -> ScreenQueryReadData:
    return _private_action(
        request, response, _viewer, ScreenHistoryAction(limit=limit, cursor=cursor)
    )


@router.get("/screen/query/presets", response_model=ScreenQueryReadData, summary="读取本人常用条件")
def get_screen_presets(
    request: Request, response: Response, _viewer: _Viewer
) -> ScreenQueryReadData:
    return _private_action(request, response, _viewer, ScreenPresetsAction())


@router.get(
    "/screen/query/executions/{execution_id}",
    response_model=ScreenQueryReadData,
    summary="读取原选股请求",
)
def get_screen_execution(
    request: Request, response: Response, execution_id: str, _viewer: _Viewer
) -> ScreenQueryReadData:
    data = _private_action(
        request, response, _viewer, ScreenExecutionAction(execution_id=execution_id)
    )
    if data.execution is None:
        raise HTTPException(404, "未找到这次选股。", headers={"Cache-Control": "no-store"})
    return data


@router.get(
    "/screen/query/executions/{execution_id}/results",
    response_model=ScreenQueryReadData,
    summary="读取原选股结果",
)
def get_screen_results(
    request: Request,
    response: Response,
    execution_id: str,
    _viewer: _Viewer,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: Annotated[str | None, Query(max_length=1024)] = None,
) -> ScreenQueryReadData:
    data = _private_action(
        request,
        response,
        _viewer,
        ScreenResultsAction(execution_id=execution_id, limit=limit, cursor=cursor),
    )
    if data.results is None:
        raise HTTPException(404, "这次结果尚未确认。", headers={"Cache-Control": "no-store"})
    return data


@router.post("/screen/query/execute", response_model=ScreenQueryReadData, summary="执行并记录选股")
def execute_screen_query(
    request: Request,
    response: Response,
    body: ExecuteScreenQuery,
    _viewer: _Viewer,
    _same_site: _Csrf,
) -> ScreenQueryReadData:
    return _private_action(request, response, _viewer, ScreenExecuteAction(command=body))


@router.post("/screen/query/lookup", response_model=ScreenQueryReadData, summary="查询原选股命令")
def lookup_screen_query(
    request: Request,
    response: Response,
    body: ScreenLookupAction,
    _viewer: _Viewer,
    _same_site: _Csrf,
) -> ScreenQueryReadData:
    return _private_action(request, response, _viewer, body)


@router.post("/screen/query/resume", response_model=ScreenQueryReadData, summary="恢复原选股命令")
def resume_screen_query(
    request: Request,
    response: Response,
    body: ScreenResumeAction,
    _viewer: _Viewer,
    _same_site: _Csrf,
) -> ScreenQueryReadData:
    return _private_action(request, response, _viewer, body)


@router.post(
    "/screen/query/presets/save", response_model=ScreenQueryReadData, summary="保存本人常用条件"
)
def save_screen_preset(
    request: Request,
    response: Response,
    body: ScreenPresetSaveRequest,
    _viewer: _Viewer,
    _same_site: _Csrf,
) -> ScreenQueryReadData:
    return _private_action(request, response, _viewer, ScreenPresetSaveAction(request=body))
