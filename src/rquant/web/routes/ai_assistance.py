"""Private AI reads and suggestions all enter the one persisted owner budget."""

from __future__ import annotations

from datetime import date, timedelta
from collections.abc import Callable
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from rquant.ai_usage import AIBudgetExceeded, AIRequestConflict, AIRequestNotFound
from rquant.web.ai_assistance_gateway import AIAdmissionRejected, AIAdmissionUnavailable
from rquant.web.envelope import Envelope
from rquant.web.models.ai_assistance import (
    AIGenerateRequest, AIAction, AIReadData, AIGenerateAction, AILookupAction,
    AICapabilitiesAction, AIUsageAction, AIRequestView, AICapabilities, AIUsageView,
    AIBacktestPrepareRequest, AIBacktestConfirmRequest, AIBacktestPreparation,
    AIBacktestConfirmation, AIBacktestPrepareAction, AIBacktestLookupAction,
    AIBacktestConfirmAction,
    AIStockNewsView, AIStockNewsAction, AIInterpretationRequest, AIInterpretationView,
    AIInterpretationReadAction, AIInterpretationContextRequest,
)
from rquant.web.portfolio_backtest_service import portfolio_meta
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/ai")
MAX_REQUEST_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024


def call_ai(request: Request, viewer: str | None, action: AIAction) -> AIReadData:
    if viewer is None:
        raise HTTPException(401, "请先登录。")
    web = request.app.state.web
    if viewer not in web.settings.ai_users:
        raise HTTPException(403, "当前账号不能使用助手。")
    if web.ai_assistance_gateway is None:
        raise HTTPException(503, "助手尚未配置，可继续手动编辑。")
    try:
        return web.ai_assistance_gateway.request(action, authenticated_actor_id=viewer)
    except AIRequestConflict:
        raise HTTPException(409, "原请求或数据已改变，请刷新后新建请求。") from None
    except AIRequestNotFound:
        raise HTTPException(404, "找不到原请求。") from None
    except AIBudgetExceeded:
        raise HTTPException(429, "今日调用次数已用完，或调用尚未启用。") from None
    except AIAdmissionRejected:
        raise HTTPException(403, "当前账号不能使用助手。") from None
    except AIAdmissionUnavailable:
        raise HTTPException(503, "助手暂不可用，请稍后查看原请求。") from None
    except (ValueError, TypeError):
        raise HTTPException(409, "数据已更新或请求有误，请刷新后重试。") from None


def _response(request: Request, value: BaseModel) -> JSONResponse:
    from rquant.runtime_contracts import canonical_sha256
    data = Envelope(data=value, serving=portfolio_meta(result_hash=canonical_sha256(value), built_at=request.app.state.web.clock()))
    raw = data.model_dump_json().encode()
    if len(raw) > MAX_RESPONSE_BYTES:
        raise HTTPException(503, "摘要范围过大，请缩小范围。")
    return JSONResponse(data.model_dump(mode="json"), headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"})


def original_header_id(request: Request) -> UUID:
    raw = request.headers.get("x-rquant-ai-request-id")
    try:
        value = UUID(raw)
        if raw != str(value):
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(422, "缺少原请求，请重新打开建议入口。") from None


def generate_ai(request: Request, viewer: str | None, body: AIGenerateRequest, *,
                new_request_preflight: Callable[[], None] | None = None) -> AIReadData:
    reserved = False
    try:
        known = call_ai(request, viewer, AILookupAction(original=body))
        if known.request is None or known.request.state != "reserved":
            return known
        reserved = True
    except HTTPException as error:
        if error.status_code != 404:
            raise
    if not reserved and new_request_preflight is not None:
        new_request_preflight()
    web = request.app.state.web
    if not web.nl_gate.acquire(blocking=False):
        raise HTTPException(429, "正在生成，请稍后再试。", headers={"Retry-After": "1"})
    try:
        if not reserved and not web.nl_rate_limiter.admit(viewer):
            raise HTTPException(429, "操作太频繁，请一分钟后再试。", headers={"Retry-After": "60"})
        return call_ai(request, viewer, AIGenerateAction(request=body))
    finally:
        web.nl_gate.release()


@router.get("/capabilities", response_model=Envelope[AICapabilities], summary="助手状态与当日可用次数")
def capabilities(request: Request, viewer: Annotated[str | None, Depends(current_user)]) -> JSONResponse:
    web = request.app.state.web
    if viewer is None:
        raise HTTPException(401, "请先登录。")
    if web.ai_assistance_gateway is None or viewer not in web.settings.ai_users:
        return _response(request, AICapabilities(available=False, can_generate=False, message="助手尚未配置，可继续手动编辑。"))
    data = call_ai(request, viewer, AICapabilitiesAction()).capabilities
    if data is None:
        raise HTTPException(503, "助手状态暂不可用。")
    return _response(request, data)


@router.post("/requests", response_model=Envelope[AIRequestView], summary="生成已绑定原请求的建议")
def generate(request: Request, body: AIGenerateRequest, viewer: Annotated[str | None, Depends(current_user)], _csrf: Annotated[None, Depends(require_csrf)]) -> JSONResponse:
    data = generate_ai(request, viewer, body).request
    if data is None:
        raise HTTPException(503, "原请求暂不可用。")
    return _response(request, data)


@router.post("/requests/lookup", response_model=Envelope[AIRequestView], summary="查看原请求，不重复调用")
def lookup(request: Request, body: AIGenerateRequest, viewer: Annotated[str | None, Depends(current_user)], _csrf: Annotated[None, Depends(require_csrf)]) -> JSONResponse:
    data = call_ai(request, viewer, AILookupAction(original=body)).request
    if data is None:
        raise HTTPException(503, "原请求暂不可用。")
    return _response(request, data)


@router.get("/usage", response_model=Envelope[AIUsageView], summary="本人实际模型用量")
def usage(request: Request, start_date: date, end_date: date, viewer: Annotated[str | None, Depends(current_user)]) -> JSONResponse:
    data = call_ai(request, viewer, AIUsageAction(start_date=start_date, end_date=end_date)).usage
    if data is None:
        raise HTTPException(503, "用量暂不可用。")
    return _response(request, data)


@router.post("/backtests/prepare", response_model=Envelope[AIBacktestPreparation], summary="准备原选股条件的完整历史回测")
def prepare_backtest(request: Request, body: AIBacktestPrepareRequest, viewer: Annotated[str | None, Depends(current_user)], _csrf: Annotated[None, Depends(require_csrf)]) -> JSONResponse:
    data = call_ai(request, viewer, AIBacktestPrepareAction(request=body)).preparation
    if data is None:
        raise HTTPException(503, "完整回测来源尚未准备。")
    return _response(request, data)


@router.post("/backtests/prepare/lookup", response_model=Envelope[AIBacktestPreparation], summary="恢复原历史准备请求")
def lookup_backtest(request: Request, body: AIBacktestPrepareRequest, viewer: Annotated[str | None, Depends(current_user)], _csrf: Annotated[None, Depends(require_csrf)]) -> JSONResponse:
    data = call_ai(request, viewer, AIBacktestLookupAction(original=body)).preparation
    if data is None:
        raise HTTPException(503, "原准备请求暂不可用。")
    return _response(request, data)


@router.post("/backtests/confirm", response_model=Envelope[AIBacktestConfirmation], summary="确认默认策略并提交原组合回测任务")
def confirm_backtest(request: Request, body: AIBacktestConfirmRequest, viewer: Annotated[str | None, Depends(current_user)], _csrf: Annotated[None, Depends(require_csrf)]) -> JSONResponse:
    data = call_ai(request, viewer, AIBacktestConfirmAction(request=body)).confirmation
    if data is None:
        raise HTTPException(503, "回测提交暂不可用。")
    return _response(request, data)


@router.get('/news/{stock_code}',response_model=Envelope[AIStockNewsView],summary='本人原文覆盖与新闻摘要')
def stock_news(request:Request,stock_code:str,viewer:Annotated[str|None,Depends(current_user)]) -> JSONResponse:
    try:
        action=AIStockNewsAction(stock_code=stock_code)
    except ValueError:
        raise HTTPException(422,'股票代码无效。') from None
    data=call_ai(request,viewer,action).stock_news
    if data is None:
        raise HTTPException(503,'摘要暂不可用。')
    if data.content is not None:
        from rquant.web.readers import table_states
        from rquant.web.serving import serving_meta
        with request.app.state.web.tracker.borrow() as borrowed:
            web=request.app.state.web
            meta=serving_meta(borrowed,now=web.clock(),stale_after=timedelta(seconds=web.settings.stale_after_seconds),failure=web.tracker.failure)
            table=None if borrowed is None else table_states(borrowed.cursor).get('ai_news_digest')
            rows=[]
            if meta.state=='ready' and table is not None and table.available:
                rows=borrowed.cursor.execute('SELECT payload_json FROM ai_news_digest WHERE owner_uid=? AND stock_code=? AND context_sha256=? AND content_sha256=? LIMIT 2',(viewer,stock_code,data.context_sha256,data.content.digest.content_sha256)).fetchall()
            if len(rows)==1:
                from rquant.web.models.ai_assistance import AINewsContent
                published=AINewsContent.model_validate_json(rows[0][0])
                if published!=data.content:
                    raise HTTPException(503,'摘要已变化，请刷新后重试。')
            else:
                data=data.model_copy(update={'state':'collected','content':None,'message':'摘要已保存，等待页面更新。'})
    return _response(request,data)


@router.post('/interpretations/read',response_model=Envelope[AIInterpretationView],summary='查看已绑定完整封存结果的解读')
def interpretation(request:Request,body:AIInterpretationRequest | AIInterpretationContextRequest,viewer:Annotated[str|None,Depends(current_user)],_csrf:Annotated[None,Depends(require_csrf)]) -> JSONResponse:
    data=call_ai(request,viewer,AIInterpretationReadAction(original=body)).interpretation
    if data is None:
        raise HTTPException(503,'解读暂不可用。')
    return _response(request,data)
