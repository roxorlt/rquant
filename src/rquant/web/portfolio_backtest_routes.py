"""Portfolio UI commands use PageControl; reads use verified sealed artifacts."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from typing import Annotated, Literal, TypeVar
from uuid import NAMESPACE_URL, UUID, uuid5

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, TypeAdapter

from rquant.lab_artifact_preview import (
    ArtifactPreviewIntegrityError,
    ArtifactPreviewUnavailableError,
)
from rquant.lab_job_center import CommandSubmissionResult
from rquant.portfolio_backtest_artifact import PortfolioZipReceipt
from rquant.portfolio_backtest_commands import (
    ExportPortfolioBacktestZip,
    PortfolioCommand,
    SubmitPortfolioBacktest,
    portfolio_interaction,
    portfolio_job_id,
)
from rquant.web.envelope import Envelope
from rquant.web.lab_control_gateway import (
    LabControlConflictError,
    LabControlInvalidReceiptError,
    LabControlUnavailableError,
    LabControlWireReceipt,
)
from rquant.web.models.backtests import (
    PortfolioCapabilities,
    PortfolioCommandReceipt,
    PortfolioCreateRequest,
    PortfolioExportRequest,
    PortfolioJobsData,
    PortfolioNavData,
    PortfolioRowsData,
    PortfolioSummaryData,
)
from rquant.web.portfolio_backtest_service import (
    PortfolioWebService,
    bounded_portfolio_response,
    portfolio_meta,
)
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/backtests/portfolio")
MAX_RUN_REQUEST_BYTES = 32 * 1024
MAX_EXPORT_REQUEST_BYTES = 1024
_RESULT = TypeAdapter(CommandSubmissionResult)
_Model = TypeVar("_Model", bound=BaseModel)


def _can_write(request: Request, viewer: str | None) -> bool:
    web = request.app.state.web
    return bool(
        viewer is not None
        and web.settings.ingress_socket_path is not None
        and web.proxy_identity is not None
        and viewer in web.settings.lab_control_users
    )


def _writer(request: Request, viewer: str | None) -> str:
    if viewer is None:
        raise HTTPException(401, "请先登录。")
    if not _can_write(request, viewer):
        raise HTTPException(403, "当前账号不能运行或导出回测。")
    return viewer


def _service(request: Request) -> PortfolioWebService:
    service = request.app.state.web.portfolio_backtests
    if service is None:
        raise HTTPException(503, "尚无可用回测来源，请先准备完整候选与行情。")
    return service


def _read(operation: Callable[[], _Model]) -> _Model:
    try:
        return operation()
    except LookupError as error:
        raise HTTPException(404, "找不到这次组合回测。") from error
    except ArtifactPreviewUnavailableError as error:
        raise HTTPException(409, "结果尚未保存完成，请稍后刷新。") from error
    except ArtifactPreviewIntegrityError as error:
        raise HTTPException(503, "结果校验未通过，请重新运行回测。") from error
    except ValueError as error:
        raise HTTPException(409, "结果已变化，请重新选择这次回测。") from error


def _response(
    data: BaseModel,
    *,
    result_hash: str | None = None,
    built_at: datetime | None = None,
    available: bool = True,
    message: str | None = None,
) -> Response:
    value = Envelope(
        data=data,
        serving=portfolio_meta(
            result_hash=result_hash, built_at=built_at, available=available, message=message
        ),
    )
    try:
        return bounded_portfolio_response(value)
    except ValueError as error:
        raise HTTPException(503, "结果范围过大，请缩小范围后重试。") from error


@router.get(
    "/capabilities",
    response_model=Envelope[PortfolioCapabilities],
    summary="组合回测来源与操作权限",
)
def capabilities(
    request: Request, viewer: Annotated[str | None, Depends(current_user)]
) -> Response:
    service = request.app.state.web.portfolio_backtests
    if service is None:
        data = PortfolioCapabilities(
            available=False,
            can_run=False,
            can_export=False,
            message="尚无可用回测来源，请先准备完整候选与行情。",
        )
    else:
        data = service.capabilities(can_write=_can_write(request, viewer))
    return _response(data, available=data.available, message=data.message)


@router.get("/runs", response_model=Envelope[PortfolioJobsData], summary="组合回测记录")
def list_runs(
    request: Request,
    _viewer: Annotated[str | None, Depends(current_user)],
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    cursor: Annotated[str | None, Query(max_length=4096)] = None,
) -> Response:
    service = request.app.state.web.portfolio_backtests
    if service is None:
        return _response(
            PortfolioJobsData(available=False, jobs=(), next_cursor=None),
            available=False,
            message="尚无可用组合回测记录。",
        )
    return _response(_read(lambda: service.jobs(limit=limit, cursor=cursor)))


@router.get("/runs/{job_id}", response_model=Envelope[PortfolioSummaryData], summary="组合回测绩效")
def summary(
    request: Request, job_id: UUID, _viewer: Annotated[str | None, Depends(current_user)]
) -> Response:
    data = _read(lambda: _service(request).summary(job_id))
    return _response(
        data, result_hash=data.result_hash, built_at=data.job.updated_at, message=data.message
    )


@router.get(
    "/runs/{job_id}/nav", response_model=Envelope[PortfolioNavData], summary="组合净值与回撤"
)
def nav(
    request: Request,
    job_id: UUID,
    _viewer: Annotated[str | None, Depends(current_user)],
    result_hash: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
) -> Response:
    data = _read(lambda: _service(request).nav(job_id, result_hash=result_hash))
    return _response(data, result_hash=result_hash)


@router.get(
    "/runs/{job_id}/rows", response_model=Envelope[PortfolioRowsData], summary="组合回测明细"
)
def rows(
    request: Request,
    job_id: UUID,
    _viewer: Annotated[str | None, Depends(current_user)],
    result_hash: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
    view: Literal["trades", "holdings", "daily", "monthly", "log"],
    offset: Annotated[int, Query(ge=0, lt=80_000)] = 0,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
) -> Response:
    data = _read(
        lambda: _service(request).rows(
            job_id, result_hash=result_hash, view=view, offset=offset, limit=limit
        )
    )
    return _response(data, result_hash=result_hash)


@router.get("/runs/{job_id}/report.html", response_class=Response, summary="下载组合回测 HTML 报告")
def report(
    request: Request,
    job_id: UUID,
    _viewer: Annotated[str | None, Depends(current_user)],
    result_hash: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
) -> Response:
    service = _service(request)
    _read(lambda: service.job(job_id))
    read = _read(lambda: service.results.read(job_id, expected_result_hash=result_hash))
    try:
        html = read.html_bytes()
    except ArtifactPreviewUnavailableError as error:
        raise HTTPException(409, "结果不完整，暂不能下载报告。") from error
    return Response(
        html,
        media_type="text/html; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="portfolio-report.html"',
            "Content-Security-Policy": (
                "default-src 'none'; style-src 'unsafe-inline'; img-src data:; "
                "base-uri 'none'; frame-ancestors 'none'"
            ),
            "X-Rquant-Generation": result_hash,
            "Cache-Control": "no-store",
        },
    )


@router.get(
    "/runs/{job_id}/exports/{request_id}.zip",
    response_class=Response,
    summary="下载组合回测完整 ZIP",
)
def download_zip(
    request: Request,
    job_id: UUID,
    request_id: UUID,
    _viewer: Annotated[str | None, Depends(current_user)],
    result_hash: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
) -> Response:
    service = _service(request)
    _read(lambda: service.job(job_id))
    exports = service.exports
    if exports is None:
        raise HTTPException(503, "报告下载暂不可用，请稍后重试。")
    try:
        receipt = exports.recover_portfolio(
            job_id, request_id=request_id, expected_result_hash=result_hash
        )
        if receipt is None:
            raise HTTPException(404, "报告尚未准备好，请重试原导出请求。")
        content = exports.read_bytes(receipt)
    except (ValueError, ArtifactPreviewIntegrityError) as error:
        raise HTTPException(409, "报告校验未通过，请重试原导出请求。") from error
    return Response(
        content,
        media_type="application/zip",
        headers={
            "Content-Disposition": 'attachment; filename="portfolio-result.zip"',
            "X-Rquant-Generation": result_hash,
            "Cache-Control": "no-store",
        },
    )


def _public(command: PortfolioCommand, wire: LabControlWireReceipt) -> PortfolioCommandReceipt:
    command_id = UUID(command.command_id)
    if wire.status in ("pending", "processing", "ambiguous"):
        if wire.result is not None:
            raise LabControlInvalidReceiptError("unfinished portfolio command contains a result")
        return PortfolioCommandReceipt(
            command_id=command_id,
            status="unknown" if wire.status == "ambiguous" else wire.status,
            message="提交状态待确认，请重试原请求。",
        )
    if wire.status == "failed":
        if wire.completed_at is None or wire.result is not None or not wire.error:
            raise LabControlInvalidReceiptError("failed portfolio receipt is invalid")
        return PortfolioCommandReceipt(
            command_id=command_id, status="failed", message="请求未完成，请检查来源与配置后重试。"
        )
    if wire.completed_at is None or wire.error is not None or not isinstance(wire.result, dict):
        raise LabControlInvalidReceiptError("completed portfolio receipt is invalid")
    try:
        if isinstance(command, SubmitPortfolioBacktest):
            result = _RESULT.validate_json(json.dumps(wire.result))
            job_id = portfolio_job_id(command.actor_id, command.command_id)
            request_id = uuid5(
                NAMESPACE_URL, "rquant.lab-job-center.interaction:" + portfolio_interaction(command)
            )
            if result.job_id != job_id or result.request_id != request_id:
                raise ValueError("portfolio run identity differs")
            if result.result != "submitted":
                return PortfolioCommandReceipt(
                    command_id=command_id, status="conflict", message="请求未被接受，请刷新后查看。"
                )
            if result.command_type != "submit" or result.expected_version is not None:
                raise ValueError("portfolio submit receipt has another action")
            return PortfolioCommandReceipt(
                command_id=command_id,
                status="submitted",
                job_id=job_id,
                message="已提交回测，完成后显示结果。",
            )
        result = PortfolioZipReceipt.model_validate_json(json.dumps(wire.result))
        request_id = uuid5(
            NAMESPACE_URL, f"rquant.portfolio-zip:{command.actor_id}:{command.command_id}"
        )
        if (result.job_id, result.request_id, result.result_hash) != (
            command.job_id,
            request_id,
            command.result_hash,
        ):
            raise ValueError("portfolio ZIP identity differs")
        return PortfolioCommandReceipt(
            command_id=command_id,
            status="exported",
            message="报告已准备好。",
            job_id=result.job_id,
            zip_request_id=result.request_id,
            result_hash=result.result_hash,
            sha256=result.sha256,
            byte_size=result.byte_size,
        )
    except (TypeError, ValueError) as error:
        raise LabControlInvalidReceiptError("portfolio command result is invalid") from error


async def _submit(
    request: Request, command: PortfolioCommand, *, max_bytes: int
) -> PortfolioCommandReceipt | JSONResponse:
    if len(await request.body()) > max_bytes:
        raise HTTPException(413, "请求内容过长，请删减后重试。")
    try:
        wire = await anyio.to_thread.run_sync(
            request.app.state.web.lab_controls.submit, command.model_dump(mode="json")
        )
        receipt = _public(command, wire)
        return (
            JSONResponse(status_code=409, content=receipt.model_dump(mode="json"))
            if receipt.status == "conflict"
            else receipt
        )
    except LabControlConflictError:
        return JSONResponse(
            status_code=409,
            content=PortfolioCommandReceipt(
                command_id=UUID(command.command_id),
                status="conflict",
                message="请求编号已用于其他内容，请核对原请求。",
            ).model_dump(mode="json"),
        )
    except LabControlUnavailableError as error:
        raise HTTPException(503, "提交状态待确认，请重试原请求。") from error
    except LabControlInvalidReceiptError as error:
        raise HTTPException(502, "回执待核对，请重试原请求。") from error


@router.post(
    "/runs",
    response_model=PortfolioCommandReceipt,
    responses={409: {"model": PortfolioCommandReceipt}},
    summary="运行组合回测",
)
async def submit_run(
    request: Request,
    body: PortfolioCreateRequest,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> PortfolioCommandReceipt | JSONResponse:
    actor = _writer(request, viewer)
    _service(request)
    command = SubmitPortfolioBacktest(
        command_id=str(body.command_id),
        requested_at=body.requested_at,
        actor_id=actor,
        config=body.config.to_domain(),
    )
    return await _submit(request, command, max_bytes=MAX_RUN_REQUEST_BYTES)


@router.post(
    "/exports",
    response_model=PortfolioCommandReceipt,
    responses={409: {"model": PortfolioCommandReceipt}},
    summary="准备组合回测 ZIP",
)
async def submit_export(
    request: Request,
    body: PortfolioExportRequest,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> PortfolioCommandReceipt | JSONResponse:
    actor = _writer(request, viewer)
    _service(request)
    command = ExportPortfolioBacktestZip(
        command_id=str(body.command_id),
        requested_at=body.requested_at,
        actor_id=actor,
        job_id=body.job_id,
        result_hash=body.result_hash,
    )
    return await _submit(request, command, max_bytes=MAX_EXPORT_REQUEST_BYTES)
