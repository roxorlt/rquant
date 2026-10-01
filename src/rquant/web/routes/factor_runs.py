"""Authenticated typed requests; source compilation stays in the private writer."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from rquant.factor.run_request import (
    FactorRunAvailability,
    FactorRunOperationResult,
    FactorRunRequest,
)
from rquant.factor_run_admission import (
    FactorRunAdmissionRejectedError,
    FactorRunAdmissionUnavailableError,
)
from rquant.web.envelope import Envelope
from rquant.web.models.factors import FactorArchiveCommandRequest
from rquant.web.routes.factors import _archive_preflight
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import serving_meta

router = APIRouter(prefix="/factors")
MAX_RUN_REQUEST_BYTES = 8192


def _operator(request: Request, viewer: str | None) -> str:
    web = request.app.state.web
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if viewer not in web.settings.factor_run_users:
        raise HTTPException(status_code=403, detail="当前账号不能运行检验。")
    if not web.settings.factor_run_enabled or web.factor_run_admission is None:
        raise HTTPException(status_code=503, detail="运行入口尚未开启。")
    if request.query_params:
        raise HTTPException(status_code=422, detail="请使用原请求重试。")
    return viewer


def _envelope(request: Request, response: Response, data: object) -> Envelope:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope(data=data, serving=meta)


@router.get(
    "/run-availability", response_model=Envelope[FactorRunAvailability], summary="因子检验参数"
)
def run_availability(
    request: Request,
    response: Response,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FactorRunAvailability]:
    web = request.app.state.web
    if (
        not web.settings.factor_run_enabled
        or web.factor_run_admission is None
        or viewer not in web.settings.factor_run_users
    ):
        data = FactorRunAvailability(enabled=False, reason="运行入口尚未开放给当前账号", pools=())
    else:
        try:
            data = web.factor_run_admission.availability(authenticated_actor_id=viewer)
        except (
            FactorRunAdmissionRejectedError,
            FactorRunAdmissionUnavailableError,
            PermissionError,
        ):
            data = FactorRunAvailability(enabled=False, reason="可信来源暂不可用", pools=())
    return _envelope(request, response, data)


def _run(
    request: Request,
    response: Response,
    body: FactorRunRequest,
    viewer: str | None,
    *,
    resume_only: bool,
) -> Envelope[FactorRunOperationResult]:
    actor = _operator(request, viewer)
    client = request.app.state.web.factor_run_admission
    try:
        # Exact original lookup comes before even the first POST's Serving preflight.
        original = client.lookup(body, authenticated_actor_id=actor)
        if original is not None:
            result = client.resume(body, authenticated_actor_id=actor)
        elif resume_only:
            raise HTTPException(status_code=404, detail="原请求尚未确认，请重试原请求。")
        else:
            instance = _archive_preflight(
                request,
                body.parameters.factor_id,
                FactorArchiveCommandRequest(
                    generation_id=body.serving_generation_id,
                    command_id=body.command_id,
                    requested_at=body.requested_at,
                    expected_head=body.parameters.expected_head,
                ),
            )
            result = client.submit(
                body, authenticated_actor_id=actor, verified_registry_instance_id=instance
            )
    except FactorRunAdmissionRejectedError as exc:
        raise HTTPException(status_code=409, detail="原请求暂不可推进，请核对参数和来源。") from exc
    except FactorRunAdmissionUnavailableError:
        result = FactorRunOperationResult(
            original_request=body, status="uncertain", reason="暂未确认，请刷新原请求"
        )
    checked = FactorRunOperationResult.model_validate(result)
    if checked.original_request != body:
        raise HTTPException(status_code=503, detail="原请求回执暂不可核验。")
    return _envelope(request, response, checked)


@router.post(
    "/runs",
    response_model=Envelope[FactorRunOperationResult],
    dependencies=[Depends(require_csrf)],
    summary="运行因子检验",
)
def submit_factor_run(
    request: Request,
    response: Response,
    body: FactorRunRequest,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FactorRunOperationResult]:
    return _run(request, response, body, viewer, resume_only=False)


@router.post(
    "/runs/resume",
    response_model=Envelope[FactorRunOperationResult],
    dependencies=[Depends(require_csrf)],
    summary="核对原因子检验",
)
def resume_factor_run(
    request: Request,
    response: Response,
    body: FactorRunRequest,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FactorRunOperationResult]:
    return _run(request, response, body, viewer, resume_only=True)


@router.post(
    "/runs/retry",
    response_model=Envelope[FactorRunOperationResult],
    dependencies=[Depends(require_csrf)],
    summary="重试原因子检验",
)
def retry_factor_run(
    request: Request,
    response: Response,
    body: FactorRunRequest,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FactorRunOperationResult]:
    return _run(request, response, body, viewer, resume_only=False)
