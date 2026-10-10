"""Minute submissions use the original PageControl gateway and sealed Lab graph."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from typing import Annotated, TypeVar
from uuid import NAMESPACE_URL, UUID, uuid5

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, TypeAdapter

from rquant.lab_artifact_preview import ArtifactPreviewIntegrityError, ArtifactPreviewUnavailableError
from rquant.lab_job_center import CommandSubmissionResult
from rquant.minute_backtest_commands import ExportMinuteReplayZip, MinuteCommand, SubmitMinuteReplay, minute_interaction, minute_job_id, minute_zip_request_id
from rquant.minute_backtest_export import MinuteZipReceipt
from rquant.minute_backtest_parameter_study_commands import SubmitMinuteParameterStudy
from rquant.strict_json import strict_model_validate_json
from rquant.web.envelope import Envelope
from rquant.web.lab_control_gateway import LabControlConflictError, LabControlInvalidReceiptError, LabControlUnavailableError, LabControlWireReceipt
from rquant.web.minute_backtest_service import LazyMinuteWebService, MinuteWebService
from rquant.web.models.minute_backtests import (
    MinuteCapabilities, MinuteCommandReceipt, MinuteCreateRequest, MinuteExportRequest, MinuteJobsData, MinuteNavData,
    MinuteRowsData, MinuteSourcesData, MinuteSummaryData, MinuteTableName, MinuteParameterSourcesData,
    MinuteStudyCapabilitiesData, MinuteStudiesData, MinuteStudyCreateRequest, MinuteStudyCommandReceipt,
    MinuteStudySubmittedJob, MinuteStudyResultData, MinuteStudyHeatmapData,
)
from rquant.web.portfolio_backtest_service import bounded_portfolio_response, portfolio_meta
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/backtests/minute-runtime")
MAX_RUN_REQUEST_BYTES = 32 * 1024
MAX_EXPORT_REQUEST_BYTES = 32 * 1024
_Model = TypeVar("_Model")
_SUBMISSION_RESULT = TypeAdapter(CommandSubmissionResult)


def _can_write(request: Request, viewer: str | None) -> bool:
    web = request.app.state.web
    role = getattr(request.state, "collaboration", None)
    return bool(viewer is not None and viewer in web.settings.lab_control_users
        and web.settings.ingress_socket_path is not None and web.proxy_identity is not None
        and (web.settings.collaboration_mode != "enforced" or (role is not None and role.username == viewer and role.can_research)))


def _service(request: Request) -> MinuteWebService:
    configured = request.app.state.web.minute_backtests
    if configured is None:
        raise HTTPException(503, "尚无已安装的分钟回测来源。")
    try:
        return configured.load() if isinstance(configured, LazyMinuteWebService) else configured
    except (OSError, PermissionError, ValueError, RuntimeError) as error:
        raise HTTPException(503, "分钟回测来源校验未通过，请重新准备来源。") from error


def _read(operation: Callable[[], _Model]) -> _Model:
    try:
        return operation()
    except LookupError as error:
        raise HTTPException(404, "找不到这次分钟回测。") from error
    except ArtifactPreviewUnavailableError as error:
        raise HTTPException(409, "结果尚未保存完成，请稍后刷新。") from error
    except (ArtifactPreviewIntegrityError, OSError, PermissionError, RuntimeError) as error:
        raise HTTPException(503, "来源或结果校验未通过，请重新准备来源。") from error
    except ValueError as error:
        raise HTTPException(409, "结果已变化，请刷新后重试。") from error


def _response(data: BaseModel, *, result_hash: str | None = None, built_at: datetime | None = None) -> Response:
    try:
        return bounded_portfolio_response(Envelope(data=data, serving=portfolio_meta(result_hash=result_hash,
            built_at=built_at, available=getattr(data, "available", True), message=getattr(data, "message", None))))
    except ValueError as error:
        raise HTTPException(503, "结果范围过大，请缩小范围后重试。") from error


@router.get("/capabilities", response_model=Envelope[MinuteCapabilities], summary="分钟回测操作权限")
def capabilities(request: Request, viewer: Annotated[str, Depends(current_user)]) -> Response:
    if request.app.state.web.minute_backtests is None:
        return _response(MinuteCapabilities(available=False, can_run=False, message="尚无已安装的分钟回测来源。"))
    return _response(_read(lambda: _service(request).capabilities(owner_id=viewer, can_write=_can_write(request, viewer))))


@router.get("/sources", response_model=Envelope[MinuteSourcesData], summary="可验证的分钟输入与原策略版本")
def sources(request: Request, viewer: Annotated[str, Depends(current_user)]) -> Response:
    if request.app.state.web.minute_backtests is None:
        return _response(MinuteSourcesData(available=False, message="尚无已安装的分钟回测来源。"))
    return _response(_read(lambda: _service(request).sources(owner_id=viewer)))


@router.get("/runs", response_model=Envelope[MinuteJobsData], summary="原Lab分钟回测任务")
def runs(request: Request, viewer: Annotated[str, Depends(current_user)],
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    cursor: Annotated[str | None, Query(max_length=4096)] = None) -> Response:
    if request.app.state.web.minute_backtests is None:
        return _response(MinuteJobsData(available=False, message="尚无已安装的分钟回测来源。"))
    return _response(_read(lambda: _service(request).jobs(owner_id=viewer, limit=limit, cursor=cursor)))


@router.get("/parameter-sources", response_model=Envelope[MinuteParameterSourcesData], summary="完整分钟参数研究事实来源")
def parameter_sources(request: Request, viewer: Annotated[str, Depends(current_user)]) -> Response:
    if request.app.state.web.minute_backtests is None:
        return _response(MinuteParameterSourcesData(available=False, message="尚无已安装的完整参数研究来源。"))
    return _response(_read(lambda: _service(request).parameter_sources(owner_id=viewer)))


@router.get("/studies/capabilities", response_model=Envelope[MinuteStudyCapabilitiesData], summary="当前完整来源支持的分钟参数研究")
def study_capabilities(request: Request, viewer: Annotated[str, Depends(current_user)]) -> Response:
    if request.app.state.web.minute_backtests is None:
        return _response(MinuteStudyCapabilitiesData(available=False, can_run=False, message="尚无已安装的完整参数研究来源。"))
    return _response(_read(lambda: _service(request).study_capabilities(owner_id=viewer,
        can_write=_can_write(request, viewer))))


@router.get("/studies", response_model=Envelope[MinuteStudiesData], summary="原请求中的分钟参数研究")
def studies(request: Request, viewer: Annotated[str, Depends(current_user)],
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    cursor: Annotated[str | None, Query(max_length=4096)] = None) -> Response:
    if request.app.state.web.minute_backtests is None:
        return _response(MinuteStudiesData(available=False, message="尚无已安装的完整参数研究来源。"))
    return _response(_read(lambda: _service(request).studies(owner_id=viewer, limit=limit, cursor=cursor,
        collaboration=request.app.state.web.collaboration)))


@router.get("/studies/{command_id}", response_model=Envelope[MinuteStudyResultData], summary="原分钟研究状态和三段完整结果")
def study_result(request: Request, command_id: UUID, viewer: Annotated[str, Depends(current_user)]) -> Response:
    data = _read(lambda: _service(request).study(command_id, owner_id=viewer,
        collaboration=request.app.state.web.collaboration))
    return _response(data, built_at=data.read_at)


@router.get("/studies/{command_id}/heatmap", response_model=Envelope[MinuteStudyHeatmapData], summary="原训练结果的两参数热图与周围一圈最低值")
def study_heatmap(request: Request, command_id: UUID, viewer: Annotated[str, Depends(current_user)],
    current_trial_index: Annotated[int, Query(ge=0, lt=20_000)],
    x_parameter: Annotated[str, Query(max_length=128, pattern=r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)?$")],
    y_parameter: Annotated[str, Query(max_length=128, pattern=r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)?$")]) -> Response:
    data = _read(lambda: _service(request).study_heatmap(command_id, owner_id=viewer,
        current_trial_index=current_trial_index, x_parameter=x_parameter, y_parameter=y_parameter,
        collaboration=request.app.state.web.collaboration))
    return _response(data, result_hash=data.heatmap.trial_set_hash, built_at=data.read_at)


def _public_study(command: SubmitMinuteParameterStudy, wire: LabControlWireReceipt) -> MinuteStudyCommandReceipt:
    from rquant.minute_backtest_parameter_study_journal import MinuteParameterStudySubmissionReceipt

    command_id = UUID(command.command_id)
    if wire.status in ("pending", "processing", "ambiguous"):
        if wire.result is not None:
            raise LabControlInvalidReceiptError("unfinished study command has a result")
        return MinuteStudyCommandReceipt(command_id=command_id,
            status="unknown" if wire.status == "ambiguous" else wire.status, message="研究正在准备，状态待确认时请重试原请求。")
    if wire.status == "failed":
        if wire.completed_at is None or wire.result is not None or not wire.error:
            raise LabControlInvalidReceiptError("failed study receipt differs")
        return MinuteStudyCommandReceipt(command_id=command_id, status="failed", message="研究请求未完成，请检查完整来源和参数。")
    if wire.completed_at is None or wire.error is not None or not isinstance(wire.result, dict):
        raise LabControlInvalidReceiptError("completed study receipt differs")
    try:
        result = strict_model_validate_json(MinuteParameterStudySubmissionReceipt, json.dumps(wire.result))
        if result.parent_command_id != command.command_id:
            raise ValueError("study terminal receipt belongs to another complete parent")
        jobs = []
        for index, item in enumerate(result.receipts):
            child_id = str(uuid5(command.request.request_id, f"rquant.minute-study:{index}"))
            if (item.job_id, item.command_type, item.expected_version) != (
                minute_job_id(command.actor_id, child_id), "submit", None):
                raise ValueError("study original ordered child receipt differs")
            jobs.append(MinuteStudySubmittedJob(index=index, job_id=item.job_id))
        return MinuteStudyCommandReceipt(command_id=command_id, status=result.state, plan_id=result.plan_id,
            jobs=tuple(jobs), unavailable_reasons=result.unavailable_reasons,
            message="已提交研究，完成后显示原结果。" if result.state == "submitted" else "来源日期不足，无法组成研究窗口。")
    except (TypeError, ValueError) as error:
        raise LabControlInvalidReceiptError("study complete terminal receipt differs") from error


@router.post("/studies", response_model=MinuteStudyCommandReceipt,
    responses={409: {"model": MinuteStudyCommandReceipt}}, summary="保存完整分钟参数研究请求")
async def submit_study(request: Request, body: MinuteStudyCreateRequest,
    viewer: Annotated[str, Depends(current_user)], _same_site: Annotated[None, Depends(require_csrf)]) -> MinuteStudyCommandReceipt | JSONResponse:
    if not _can_write(request, viewer):
        raise HTTPException(403, "当前账号不能运行分钟参数研究。")
    raw = await request.body()
    if len(raw) > MAX_RUN_REQUEST_BYTES:
        raise HTTPException(413, "请求内容过长，请缩小研究范围后重试。")
    try:
        original = strict_model_validate_json(MinuteStudyCreateRequest, raw)
        if original != body:
            raise ValueError("study parsed request differs from its original wire")
        command = body.to_command(authenticated_actor_id=viewer)
    except (TypeError, ValueError) as error:
        raise HTTPException(422, "研究请求有误，请检查完整参数、评分和三个区间。") from error
    _service(request)
    try:
        wire = await anyio.to_thread.run_sync(request.app.state.web.lab_controls.submit, command.model_dump(mode="json"))
        return _public_study(command, wire)
    except LabControlConflictError:
        return JSONResponse(status_code=409, content=MinuteStudyCommandReceipt(command_id=body.command_id,
            status="conflict", message="请求编号已用于其他内容，请核对原请求。").model_dump(mode="json"))
    except LabControlUnavailableError as error:
        raise HTTPException(503, "研究提交状态待确认，请重试原请求。") from error
    except LabControlInvalidReceiptError as error:
        raise HTTPException(502, "研究回执待核对，请重试原请求。") from error


@router.get("/runs/{job_id}", response_model=Envelope[MinuteSummaryData], summary="完整封存的分钟回测摘要")
def summary(request: Request, job_id: UUID, viewer: Annotated[str, Depends(current_user)]) -> Response:
    data = _read(lambda: _service(request).summary(job_id, owner_id=viewer))
    return _response(data, result_hash=data.result_hash, built_at=data.job.updated_at)


@router.get("/runs/{job_id}/nav", response_model=Envelope[MinuteNavData], summary="15:00已确认行情的每日净值")
def nav(request: Request, job_id: UUID, viewer: Annotated[str, Depends(current_user)],
    result_hash: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")]) -> Response:
    data = _read(lambda: _service(request).nav(job_id, owner_id=viewer, result_hash=result_hash))
    return _response(data, result_hash=data.result_hash)


@router.get("/runs/{job_id}/rows", response_model=Envelope[MinuteRowsData], summary="原分钟结果八表明细")
def rows(request: Request, job_id: UUID, viewer: Annotated[str, Depends(current_user)],
    result_hash: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")], table: MinuteTableName,
    offset: Annotated[int, Query(ge=0, lt=80_000)] = 0, limit: Annotated[int, Query(ge=1, le=50)] = 20) -> Response:
    data = _read(lambda: _service(request).rows(job_id, owner_id=viewer, result_hash=result_hash,
        table=table, offset=offset, limit=limit))
    return _response(data, result_hash=data.result_hash)


def _public(command: MinuteCommand, wire: LabControlWireReceipt) -> MinuteCommandReceipt:
    command_id = UUID(command.command_id)
    if wire.status in ("pending", "processing", "ambiguous"):
        if wire.result is not None:
            raise LabControlInvalidReceiptError("unfinished minute command has a result")
        return MinuteCommandReceipt(command_id=command_id, status="unknown" if wire.status == "ambiguous" else wire.status,
            message="提交状态待确认，请重试原请求。")
    if wire.status == "failed":
        if wire.completed_at is None or wire.result is not None or not wire.error:
            raise LabControlInvalidReceiptError("failed minute receipt differs")
        return MinuteCommandReceipt(command_id=command_id, status="failed", message="请求未完成，请检查来源与正式登记区间后重试。")
    if wire.completed_at is None or wire.error is not None or not isinstance(wire.result, dict):
        raise LabControlInvalidReceiptError("completed minute receipt differs")
    try:
        if type(command) is ExportMinuteReplayZip:
            result = MinuteZipReceipt.model_validate_json(json.dumps(wire.result))
            if (result.job_id, result.request_id, result.result_hash) != (
                command.job_id, minute_zip_request_id(command), command.result_hash):
                raise ValueError("minute original export identity differs")
            return MinuteCommandReceipt(command_id=command_id, status="exported", job_id=result.job_id,
                zip_request_id=result.request_id, result_hash=result.result_hash, sha256=result.sha256,
                byte_size=result.byte_size, message="完整报告已准备好。")
        result = _SUBMISSION_RESULT.validate_json(json.dumps(wire.result))
        job_id = minute_job_id(command.actor_id, command.command_id)
        request_id = uuid5(NAMESPACE_URL, "rquant.lab-job-center.interaction:" + minute_interaction(command))
        if result.job_id != job_id or result.request_id != request_id:
            raise ValueError("minute original submit identity differs")
        if result.result == "submitted" and (result.command_type != "submit" or result.expected_version is not None):
            raise ValueError("minute original submit action differs")
        return MinuteCommandReceipt(command_id=command_id, status="submitted" if result.result == "submitted" else "conflict",
            job_id=job_id if result.result == "submitted" else None,
            message="已提交分钟回测，完成后显示结果。" if result.result == "submitted" else "请求未被接受，请刷新后查看。")
    except (TypeError, ValueError) as error:
        raise LabControlInvalidReceiptError("minute result receipt differs") from error


@router.post("/runs", response_model=MinuteCommandReceipt, responses={409: {"model": MinuteCommandReceipt}}, summary="提交原策略规范的分钟回测")
async def submit_run(request: Request, body: MinuteCreateRequest,
    viewer: Annotated[str, Depends(current_user)], _same_site: Annotated[None, Depends(require_csrf)]) -> MinuteCommandReceipt | JSONResponse:
    if not _can_write(request, viewer):
        raise HTTPException(403, "当前账号不能运行分钟回测。")
    raw = await request.body()
    if len(raw) > MAX_RUN_REQUEST_BYTES:
        raise HTTPException(413, "请求内容过长，请缩小范围后重试。")
    try:
        original = strict_model_validate_json(MinuteCreateRequest, raw)
        if original != body:
            raise ValueError("minute parsed request differs from original wire")
    except (TypeError, ValueError) as error:
        raise HTTPException(422, "分钟回测请求有误，请检查来源、策略版本和正式登记区间。") from error
    _service(request)
    command = SubmitMinuteReplay(command_id=str(body.command_id), requested_at=body.requested_at,
        actor_id=viewer, config=body.config)
    try:
        wire = await anyio.to_thread.run_sync(request.app.state.web.lab_controls.submit, command.model_dump(mode="json"))
        receipt = _public(command, wire)
        return JSONResponse(status_code=409, content=receipt.model_dump(mode="json")) if receipt.status == "conflict" else receipt
    except LabControlConflictError:
        return JSONResponse(status_code=409, content=MinuteCommandReceipt(command_id=body.command_id,
            status="conflict", message="请求编号已用于其他内容，请核对原请求。").model_dump(mode="json"))
    except LabControlUnavailableError as error:
        raise HTTPException(503, "提交状态待确认，请重试原请求。") from error
    except LabControlInvalidReceiptError as error:
        raise HTTPException(502, "回执待核对，请重试原请求。") from error


@router.get("/runs/{job_id}/report.html", response_class=Response, summary="下载分钟回测完整HTML报告")
def report_html(request: Request, job_id: UUID, viewer: Annotated[str, Depends(current_user)],
    result_hash: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")]) -> Response:
    report = _read(lambda: _service(request).report(job_id, owner_id=viewer, result_hash=result_hash,
        collaboration=request.app.state.web.collaboration))
    return Response(report.html_bytes(), media_type="text/html; charset=utf-8", headers={
        "Content-Disposition": 'attachment; filename="minute-report.html"', "Cache-Control": "no-store",
        "X-Rquant-Generation": result_hash,
        "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'"})


@router.get("/runs/{job_id}/exports/{request_id}.zip", response_class=Response, summary="下载分钟回测完整八表ZIP")
def download_zip(request: Request, job_id: UUID, request_id: UUID, viewer: Annotated[str, Depends(current_user)],
    result_hash: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")]) -> Response:
    content = _read(lambda: _service(request).export_bytes(job_id, request_id=request_id,
        owner_id=viewer, result_hash=result_hash, collaboration=request.app.state.web.collaboration))
    return Response(content, media_type="application/zip", headers={
        "Content-Disposition": 'attachment; filename="minute-result.zip"', "Cache-Control": "no-store",
        "X-Rquant-Generation": result_hash})


@router.post("/exports", response_model=MinuteCommandReceipt, responses={409: {"model": MinuteCommandReceipt}},
    summary="准备分钟回测完整HTML及八表ZIP")
async def submit_export(request: Request, body: MinuteExportRequest, viewer: Annotated[str, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)]) -> MinuteCommandReceipt | JSONResponse:
    if not _can_write(request, viewer):
        raise HTTPException(403, "当前账号不能准备分钟回测报告。")
    raw = await request.body()
    if len(raw) > MAX_EXPORT_REQUEST_BYTES:
        raise HTTPException(413, "请求内容过长，请缩小范围后重试。")
    try:
        original = strict_model_validate_json(MinuteExportRequest, raw)
        if original != body:
            raise ValueError("minute export parsed request differs from original wire")
    except (TypeError, ValueError) as error:
        raise HTTPException(422, "报告请求有误，请核对原结果。") from error
    _service(request)
    command = ExportMinuteReplayZip(command_id=str(body.command_id), requested_at=body.requested_at,
        actor_id=viewer, job_id=body.job_id, result_hash=body.result_hash)
    try:
        wire = await anyio.to_thread.run_sync(request.app.state.web.lab_controls.submit, command.model_dump(mode="json"))
        receipt = _public(command, wire)
        return JSONResponse(status_code=409, content=receipt.model_dump(mode="json")) if receipt.status == "conflict" else receipt
    except LabControlConflictError:
        return JSONResponse(status_code=409, content=MinuteCommandReceipt(command_id=body.command_id,
            status="conflict", message="请求编号已用于其他内容，请核对原请求。").model_dump(mode="json"))
    except LabControlUnavailableError as error:
        raise HTTPException(503, "报告准备状态待确认，请重试原请求。") from error
    except LabControlInvalidReceiptError as error:
        raise HTTPException(502, "回执待核对，请重试原请求。") from error
