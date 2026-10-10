"""Same-generation paper reads and original private two-step commands."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

import pandas as pd
from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request, Response

from rquant.page_control import PageControlStatus, PageControlCommandConflictError
from rquant.paper_operator_commands import PaperPortfolioCommand, SavePaperPortfolioConfiguration, SetPaperAccountPaused, PaperOperatorControl
from rquant.paper_portfolio_admission import PaperPortfolioAdmissionResult, PaperPortfolioAdmissionRejectedError, PaperPortfolioAdmissionUnavailableError, PaperPortfolioAdmissionNotFoundError
from rquant.paper_portfolio_history import PaperHistoryPage, paper_history_page
from rquant.paper_contracts import PaperSide
from rquant.paper_portfolio_models import PaperPortfolioConfiguration, PaperPortfolioStateIdentity
from rquant.paper_portfolio_projection import PaperPortfolioPublishedAccount, PaperPortfolioSnapshot
from rquant.paper_portfolio_view_source import publish_paper_band_position
from rquant.paper_research_commands import RunPaperPortfolioResearch, PaperResearchSubmissionReceipt
from rquant.paper_research_artifact import PaperResearchSummary
from rquant.perf import equity_curve
from rquant.runtime_contracts import canonical_sha256
from rquant.serving_publisher import ServingReader
from rquant.strategy_template import TEMPLATE_CONTRACT
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.models.paper_portfolio import (PaperConfigurationView, PaperPortfolioItem, PaperPortfolioMetrics, PaperPortfolioCatalogData,
    PaperPortfolioDetailData, PaperPeriodAttributionView, PaperPauseConfirmBody, PaperPausePreparationData, PaperPortfolioCommandData, PaperBacktestChoice,
    PaperPortfolioHistoryPageView, PaperPortfolioHistoryRecordView)
from rquant.web.paper_history import _STATUS_LABELS, _REJECT_MESSAGES
from rquant.web.paper_portfolio_reader import read_paper_portfolios
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.strategy_authoring_reader import read_strategy_authoring

router = APIRouter(prefix="/paper-portfolios")
MAX_CONFIGURATION_BYTES = 16*1024
MAX_PAPER_CONTROL_BYTES = 4*1024
MAX_PAPER_RECOVERY_BYTES = 16*1024
_Account = Annotated[str, Path(min_length=1, max_length=128)]
_Generation = Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")]
_Viewer = Annotated[str | None, Depends(current_user)]
_CSRF = Annotated[None, Depends(require_csrf)]
_UNAVAILABLE = "账户暂时无法核验，请稍后重试。"


def _meta(web: object, borrowed: BorrowedGeneration | None) -> ServingMeta:
    return serving_meta(borrowed, now=web.clock(), stale_after=web.settings.stale_after, failure=web.tracker.failure).model_copy(update={"detail":""})


def _snapshot(borrowed: BorrowedGeneration | None) -> PaperPortfolioSnapshot | None:
    try:
        return read_paper_portfolios(borrowed)
    except Exception as exc:
        raise HTTPException(503, detail=_UNAVAILABLE) from exc


def _generation(meta: ServingMeta, generation: str | None, response: Response) -> None:
    if generation is not None and generation != meta.generation_id:
        raise HTTPException(409, detail="数据已更新，请重新查看账户。")
    if meta.generation_id:
        response.headers["X-Rquant-Generation"] = meta.generation_id


def _pointer_matches(web: object, borrowed: BorrowedGeneration | None) -> bool:
    if borrowed is None or borrowed.pointer is None:
        return False
    pointer = ServingReader(web.settings.serving_root).current_pointer()
    return (pointer.generation_id, pointer.manifest_sha256) == (borrowed.pointer.generation_id, borrowed.pointer.manifest_sha256)


def _account(snapshot: PaperPortfolioSnapshot | None, account: str, actor: str | None) -> PaperPortfolioPublishedAccount:
    if snapshot is None:
        raise HTTPException(503, detail=_UNAVAILABLE)
    found = next((row for row in snapshot.for_owner(actor) if row.configuration.binding.account_id == account), None)
    if found is None:
        raise HTTPException(404, detail="账户不存在。")
    return found


def _can_write(web: object, actor: str | None, meta: ServingMeta, row: PaperPortfolioPublishedAccount) -> bool:
    return bool(web.settings.paper_portfolio_enabled and actor in web.settings.paper_portfolio_users
                and web.paper_portfolio_gateway is not None and row.metadata_identity is not None and meta.state is ServingState.READY)


def _metrics(row: PaperPortfolioPublishedAccount) -> PaperPortfolioMetrics:
    nav, calendar = row.nav, row.calendar
    verified = sum(item.status == "complete" for item in nav)
    days = () if not nav or calendar is None else tuple(day for day in calendar.dates if nav[0].trade_date <= day <= nav[-1].trade_date)
    if row.complete_comparison_dates() is None:
        return PaperPortfolioMetrics(status="unavailable", running_days=len(days), verified_days=verified, reason="逐日净值尚未完整发布。")
    result = equity_curve(pd.Series([float(item.daily_return) for item in nav], index=pd.to_datetime(days), dtype=float))
    return PaperPortfolioMetrics(status="complete", running_days=len(days), verified_days=verified, total_return=float(result.nav.iloc[-1]-1), max_drawdown=result.max_drawdown)


def _item(row: PaperPortfolioPublishedAccount, writable: bool) -> PaperPortfolioItem:
    cfg = row.configuration
    names = {"auction_gap":"竞价缺口", "growth_board_surge":"创业板冲高", "n_shape":"N形突破"}
    public = PaperConfigurationView(account_id=cfg.binding.account_id, strategy_id=cfg.binding.strategy_id, strategy_version=cfg.binding.strategy_version,
                strategy_name=names.get(cfg.binding.strategy_id,"自选策略"), fingerprint=cfg.fingerprint, version=cfg.version,
                configured_at=cfg.configured_at, weight_rule=cfg.weight_rule, drawdown_rule=cfg.drawdown_rule, execution_cost_spec=cfg.execution_cost_spec)
    return PaperPortfolioItem(configuration=public, status=row.status, reason=row.reason, account=None if row.frame is None else row.frame.account,
                              operator=row.operator, metrics=_metrics(row), can_configure=writable, can_pause=writable and row.operator.status=="applied")


@router.get("", response_model=Envelope[PaperPortfolioCatalogData], summary="我的模拟账户")
def accounts(request: Request, response: Response, viewer: _Viewer, generation_id: _Generation = None) -> Envelope[PaperPortfolioCatalogData]:
    web=request.app.state.web; web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta=_meta(web,borrowed); _generation(meta,generation_id,response); snapshot=_snapshot(borrowed)
        rows=() if snapshot is None else snapshot.for_owner(viewer)
        data=PaperPortfolioCatalogData(availability="unavailable" if snapshot is None else "populated" if rows else "empty",
                available_at=None if snapshot is None else snapshot.available_at, accounts=tuple(_item(row,_can_write(web,viewer,meta,row)) for row in rows))
    return Envelope(data=data,serving=meta)


@router.get("/{account_id}", response_model=Envelope[PaperPortfolioDetailData], summary="模拟账户与组合风控")
def detail(account_id: _Account, request: Request, response: Response, viewer: _Viewer, generation_id: _Generation = None) -> Envelope[PaperPortfolioDetailData]:
    web=request.app.state.web; web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta=_meta(web,borrowed); _generation(meta,generation_id,response); row=_account(_snapshot(borrowed),account_id,viewer)
        writable=_can_write(web,viewer,meta,row); runnable=False; choices=()
        if writable:
            try:
                runnable=web.paper_portfolio_gateway.run_available(authenticated_actor_id=viewer)
            except (PaperPortfolioAdmissionUnavailableError,PermissionError,OSError):
                pass
        try:
            templates=read_strategy_authoring(borrowed)
            if templates is not None:
                choices=tuple(PaperBacktestChoice(job_id=entry.latest_run.job_id,completed_at=entry.latest_run.completed_at,name=entry.metadata.name)
                    for entry in templates.for_owner(viewer) if entry.latest_run is not None and (entry.metadata.strategy_id,str(entry.metadata.head.version)) ==
                    (row.configuration.binding.strategy_id,row.configuration.binding.strategy_version) and canonical_sha256({"template_contract":TEMPLATE_CONTRACT,"rules":entry.metadata.rules.model_dump(mode="python")})==row.configuration.binding.parameter_fingerprint)
        except ValueError:
            choices=()
        period=row.attribution
        attribution=None if period is None else PaperPeriodAttributionView(start_at=period.source.period.start_at,end_at=period.source.period.end_at,
            status=period.view.status,result=period.view.attribution,reason=period.view.reason)
        if row.band_position is None:
            row=publish_paper_band_position(row)
        position="unavailable"
        comparison_dates=row.complete_comparison_dates()
        band=row.band if row.band is not None and comparison_dates==row.band.dates else None
        if band is not None and row.band_position is not None:
            position=row.band_position
        item=_item(row,writable)
        data=PaperPortfolioDetailData(**item.model_dump(mode="python",exclude={"can_reconcile","can_band"}), can_reconcile=runnable and row.status=="complete",
            can_band=runnable and bool(choices) and comparison_dates is not None, nav=row.nav,risk=row.risk,
            exposure=None if row.exposure is None else row.exposure.exposure,exposure_reason=row.exposure_reason,attribution=attribution,
            reduction=row.reduction,band=band,band_position=position,recent_research=row.recent_research,history_available=row.frame is not None,backtests=choices)
    return Envelope(data=data,serving=meta)


@router.get("/{account_id}/history", response_model=Envelope[PaperPortfolioHistoryPageView], summary="完整模拟指令历史")
def history(account_id: _Account, request: Request, response: Response, viewer: _Viewer, generation_id: _Generation = None,
            cursor: Annotated[str | None,Query(max_length=2048)]=None,limit: Annotated[int,Query(ge=1,le=200)]=200) -> Envelope[PaperPortfolioHistoryPageView]:
    web=request.app.state.web; web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta=_meta(web,borrowed); _generation(meta,generation_id,response); row=_account(_snapshot(borrowed),account_id,viewer)
        if row.frame is None:
            raise HTTPException(503,detail="完整历史尚未发布。")
        try:
            data=paper_history_page(row.frame,configuration=row.configuration,authenticated_actor_id=viewer,generation_id=meta.generation_id,cursor=cursor,limit=limit)
        except ValueError as exc:
            raise HTTPException(409,detail="历史已更新，请从第一页查看。") from exc
        data = PaperPortfolioHistoryPageView(**data.model_dump(mode="python", exclude={"records"}), records=tuple(
            PaperPortfolioHistoryRecordView(**record.model_dump(mode="python"),
                side_label="买入" if record.order.side is PaperSide.BUY else "卖出",
                status_label=_STATUS_LABELS[record.order.status],
                reject_message=None if record.order.reject_reason is None else _REJECT_MESSAGES[record.order.reject_reason])
            for record in data.records))
    return Envelope(data=data,serving=meta)


@router.get("/{account_id}/research/{job_id}", response_model=Envelope[PaperResearchSummary], summary="模拟账户研究结果")
def research(account_id: _Account, job_id: UUID, request: Request, response: Response, viewer: _Viewer,generation_id:_Generation=None) -> Envelope[PaperResearchSummary]:
    web=request.app.state.web;web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta=_meta(web,borrowed);_generation(meta,generation_id,response);row=_account(_snapshot(borrowed),account_id,viewer)
        result=next((item for item in row.recent_research if item.job_id==job_id),None)
        if result is None:
            raise HTTPException(404,detail="研究结果尚未发布。")
    return Envelope(data=result,serving=meta)


def _editor(request:Request,viewer:_Viewer) -> str:
    web=request.app.state.web
    if viewer is None:
        raise HTTPException(401,detail="请先登录。")
    if not web.settings.paper_portfolio_enabled or web.paper_portfolio_gateway is None:
        raise HTTPException(503,detail="账户操作暂未开放。")
    if viewer not in web.settings.paper_portfolio_users:
        raise HTTPException(403,detail="当前账号不能操作账户。")
    return viewer


_Actor=Annotated[str,Depends(_editor)]


@router.get("/{account_id}/research/{job_id}/download", summary="下载已封存模拟研究", response_class=Response)
def download(account_id:_Account,job_id:UUID,request:Request,response:Response,actor:_Actor,generation_id:_Generation=None) -> Response:
    web=request.app.state.web;web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta=_meta(web,borrowed);_generation(meta,generation_id,response);_account(_snapshot(borrowed),account_id,actor)
    try:
        result=web.paper_portfolio_gateway.download(account_id=account_id,job_id=job_id,authenticated_actor_id=actor)
    except PaperPortfolioAdmissionNotFoundError as exc:
        raise HTTPException(404,detail="结果尚未封存。") from exc
    except (PaperPortfolioAdmissionRejectedError,PermissionError) as exc:
        raise HTTPException(403,detail="当前账号不能下载此结果。") from exc
    except (PaperPortfolioAdmissionUnavailableError,OSError,RuntimeError,ValueError) as exc:
        raise HTTPException(503,detail="下载暂不可用，请稍后重试。") from exc
    return Response(content=result.content,media_type="application/zip",headers={"Content-Disposition":"attachment; filename=paper-research.zip","X-Rquant-Download-Hash":result.sha256})


def _preflight(request:Request,body:PaperPortfolioCommand,actor:str) -> PaperPortfolioStateIdentity:
    web=request.app.state.web;web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta=_meta(web,borrowed);row=_account(_snapshot(borrowed),body.account_id,actor)
        if not _can_write(web,actor,meta,row):
            raise HTTPException(503,detail=_UNAVAILABLE)
        fingerprint=body.expected_configuration_fingerprint if type(body) is SavePaperPortfolioConfiguration else body.configuration_fingerprint
        if meta.generation_id!=body.generation_id or row.configuration.fingerprint!=fingerprint:
            raise HTTPException(409,detail="账户已更新，请刷新后重试。")
        if type(body) is SetPaperAccountPaused and (row.operator.status!="applied" or row.operator.sequence!=body.expected_sequence or row.operator.paused!=body.expected_paused):
            raise HTTPException(409,detail="运行状态已变化，请重新查看。")
        if not _pointer_matches(web,borrowed):
            raise HTTPException(409,detail="数据已更新，请刷新后重试。")
        return row.metadata_identity


def _result(request:Request,body:PaperPortfolioCommand,actor:str,result:PaperPortfolioAdmissionResult|None,*,rejected:bool=False) -> Envelope[PaperPortfolioCommandData]:
    web=request.app.state.web;web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta=_meta(web,borrowed)
        data={"command_id":body.command_id,"account_id":body.account_id,"status":"rejected" if rejected else "uncertain",
              "message":"操作未受理，请检查后重试。" if rejected else "结果待确认，请保留原操作继续查看。"}
        if result is not None:
            result=PaperPortfolioAdmissionResult.model_validate(result.model_dump(mode="python"))
            if result.owner_id!=actor or result.original_request!=body:
                raise HTTPException(503,detail="原操作回执暂无法核验。")
            receipt=result.receipt
            if receipt.status in {PageControlStatus.PENDING,PageControlStatus.PROCESSING}:
                data.update(status="pending",message="正在处理，请稍后查看。")
            elif receipt.status is PageControlStatus.FAILED:
                data.update(status="rejected",message="操作未完成，请检查后重试。")
            elif receipt.status is PageControlStatus.SUCCEEDED:
                if type(body) is RunPaperPortfolioResearch:
                    effect=PaperResearchSubmissionReceipt.model_validate(receipt.result)
                    data.update(status="submitted",job_id=effect.job_id,configuration_fingerprint=effect.configuration_fingerprint,message="研究已提交，等待封存。")
                else:
                    effect=PaperOperatorControl.model_validate(receipt.result) if type(body) is SetPaperAccountPaused else PaperPortfolioConfiguration.model_validate(receipt.result)
                    paused=type(body) is SetPaperAccountPaused
                    data.update(status="waiting_application" if paused else "waiting_publication", message="已提交，等待应用。" if paused else "已保存，等待数据更新。",
                        configuration_fingerprint=effect.configuration_fingerprint if paused else effect.fingerprint, configuration_version=effect.configuration_version if paused else effect.version,
                        sequence=effect.sequence if paused else None)
                    if meta.state is ServingState.READY:
                        snapshot=_snapshot(borrowed);rows=() if snapshot is None else snapshot.for_owner(actor)
                        published=next((row for row in rows if row.configuration.binding.account_id==body.account_id and row.metadata_identity==result.metadata_identity),None)
                        if published is not None and _pointer_matches(web,borrowed):
                            if paused and (published.operator.status,published.operator.sequence,published.operator.control_fingerprint,published.operator.paused)==("applied",effect.sequence,effect.fingerprint,effect.paused):
                                data.update(status="applied",message="已暂停。" if effect.paused else "已恢复。")
                            elif not paused and published.configuration==effect:
                                data.update(status="published",message="已保存。")
    return Envelope(data=PaperPortfolioCommandData(**data),serving=meta)


def _command(request:Request,body:PaperPortfolioCommand,actor:str,*,resume:bool=False,confirmation_id:str|None=None) -> Envelope[PaperPortfolioCommandData]:
    if request.query_params:
        raise HTTPException(422,detail="请刷新后重试。")
    gateway=request.app.state.web.paper_portfolio_gateway
    try:
        found=gateway.lookup(body,authenticated_actor_id=actor)
        try:
            result=gateway.resume(body,authenticated_actor_id=actor)
        except PaperPortfolioAdmissionNotFoundError:
            if found is not None:
                raise PaperPortfolioAdmissionUnavailableError("original request vanished")
            if resume:
                return _result(request,body,actor,None)
            identity=_preflight(request,body,actor)
            result=gateway.submit(body,authenticated_actor_id=actor,verified_metadata_identity=identity,confirmation_id=confirmation_id)
    except (PaperPortfolioAdmissionRejectedError,PermissionError,PageControlCommandConflictError,ValueError):
        return _result(request,body,actor,None,rejected=True)
    except (PaperPortfolioAdmissionUnavailableError,RuntimeError,OSError,TimeoutError):
        return _result(request,body,actor,None)
    return _result(request,body,actor,result)


async def _body(request:Request,account_id:str,body:PaperPortfolioCommand,maximum:int) -> None:
    if len(await request.body())>maximum:
        raise HTTPException(413,detail="操作内容过长。")
    if body.account_id!=account_id:
        raise HTTPException(422,detail="请重新选择账户。")


@router.post("/{account_id}/configuration",response_model=Envelope[PaperPortfolioCommandData],summary="保存模拟仓位与回撤限制")
async def configuration(account_id:_Account,request:Request,body:SavePaperPortfolioConfiguration,actor:_Actor,_csrf:_CSRF) -> Envelope[PaperPortfolioCommandData]:
    await _body(request,account_id,body,MAX_CONFIGURATION_BYTES)
    return _command(request,body,actor)


@router.post("/{account_id}/pause/prepare",response_model=Envelope[PaperPausePreparationData],summary="核对模拟账户暂停或恢复")
async def prepare(account_id:_Account,request:Request,body:SetPaperAccountPaused,actor:_Actor,_csrf:_CSRF) -> Envelope[PaperPausePreparationData]:
    await _body(request,account_id,body,MAX_PAPER_CONTROL_BYTES)
    if request.query_params:
        raise HTTPException(422,detail="请刷新后重试。")
    identity=_preflight(request,body,actor)
    try:
        result=request.app.state.web.paper_portfolio_gateway.prepare(body,authenticated_actor_id=actor,verified_metadata_identity=identity)
    except (PaperPortfolioAdmissionRejectedError,PermissionError,ValueError) as exc:
        raise HTTPException(409,detail="运行状态已变化，请重新核对。") from exc
    except (PaperPortfolioAdmissionUnavailableError,OSError,RuntimeError) as exc:
        raise HTTPException(503,detail="确认暂不可用，请保留原操作重试。") from exc
    web=request.app.state.web;web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta=_meta(web,borrowed)
    return Envelope(data=PaperPausePreparationData(confirmation_id=result.confirmation_id,command=result.request,expires_at=result.expires_at),serving=meta)


@router.post("/{account_id}/pause/confirm",response_model=Envelope[PaperPortfolioCommandData],summary="确认模拟账户暂停或恢复")
async def confirm(account_id:_Account,request:Request,body:PaperPauseConfirmBody,actor:_Actor,_csrf:_CSRF) -> Envelope[PaperPortfolioCommandData]:
    await _body(request,account_id,body.request,MAX_PAPER_CONTROL_BYTES)
    return _command(request,body.request,actor,confirmation_id=body.confirmation_id)


@router.post("/{account_id}/recover",response_model=Envelope[PaperPortfolioCommandData],summary="续查原模拟账户操作")
async def recover(account_id:_Account,request:Request,body:Annotated[PaperPortfolioCommand,Body(discriminator="kind")],actor:_Actor,_csrf:_CSRF) -> Envelope[PaperPortfolioCommandData]:
    await _body(request,account_id,body,MAX_PAPER_RECOVERY_BYTES)
    return _command(request,body,actor,resume=True)


@router.post("/{account_id}/reconcile",response_model=Envelope[PaperPortfolioCommandData],summary="运行只读模拟账户对账")
async def reconcile(account_id:_Account,request:Request,body:RunPaperPortfolioResearch,actor:_Actor,_csrf:_CSRF) -> Envelope[PaperPortfolioCommandData]:
    await _body(request,account_id,body,MAX_PAPER_CONTROL_BYTES)
    if body.task_name!="paper_reconcile":
        raise HTTPException(422,detail="请选择对账任务。")
    return _command(request,body,actor)


@router.post("/{account_id}/band",response_model=Envelope[PaperPortfolioCommandData],summary="计算同版本回测对照区间")
async def band(account_id:_Account,request:Request,body:RunPaperPortfolioResearch,actor:_Actor,_csrf:_CSRF) -> Envelope[PaperPortfolioCommandData]:
    await _body(request,account_id,body,MAX_PAPER_CONTROL_BYTES)
    if body.task_name!="paper_backtest_band":
        raise HTTPException(422,detail="请选择区间任务。")
    return _command(request,body,actor)
