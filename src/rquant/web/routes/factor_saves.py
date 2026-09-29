"""Authenticated factor drafts through the private admission, never the registry."""

from __future__ import annotations

from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Request, Response

from rquant.factor.capability import HISTORICAL_DAILY_V1
from rquant.factor.draft import FactorSaveDraft, draft_factor_id
from rquant.factor.registry import FactorDefinitionReceipt
from rquant.factor_definition_admission import (
    FactorArchiveAdmissionResult,
    FactorDefinitionAdmissionRejectedError,
    FactorDefinitionAdmissionUnavailableError,
)
from rquant.page_control import PageControlStatus
from rquant.serving_publisher import ServingReader
from rquant.web.envelope import Envelope, ServingState
from rquant.web.models.factors import FactorCapabilitiesData, FactorSaveCommandData
from rquant.web.routes.factors import _read_catalog, _verified_registry_instance_id
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/factors")
MAX_SAVE_REQUEST_BYTES = 4096
_UNREADABLE = "因子库暂时无法核验，请稍后重试。"


def _save_editor(request: Request, viewer: Annotated[str | None, Depends(current_user)]) -> str:
    web = request.app.state.web
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if not web.settings.factor_save_enabled or web.factor_admission is None:
        raise HTTPException(status_code=503, detail="保存因子暂未开放。")
    if viewer not in web.settings.factor_editor_users:
        raise HTTPException(status_code=403, detail="当前账号不能保存因子。")
    if request.query_params:
        raise HTTPException(status_code=422, detail="请刷新后重试。")
    return viewer


def _pointer_matches(web: object, borrowed: BorrowedGeneration) -> bool:
    pointer = ServingReader(web.settings.serving_root).current_pointer()
    return (
        borrowed.pointer is not None
        and pointer.generation_id == borrowed.pointer.generation_id
        and pointer.manifest_sha256 == borrowed.pointer.manifest_sha256
    )


def _save_preflight(
    request: Request,
    draft: FactorSaveDraft,
    *,
    actor: str,
) -> str:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if borrowed is None or meta.state is not ServingState.READY:
            raise HTTPException(status_code=503, detail=_UNREADABLE)
        if meta.generation_id != draft.generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请刷新因子后重试。")
        catalog = _read_catalog(borrowed)
        if catalog.availability not in {"empty", "populated"}:
            raise HTTPException(status_code=409, detail="当前因子库尚未发布。")
        factor_id = draft_factor_id(draft, authenticated_actor_id=actor)
        row = next((item for item in catalog.definitions if item.factor_id == factor_id), None)
        if draft.mode == "create":
            if row is not None:
                raise HTTPException(status_code=409, detail="该因子已存在，请刷新后查看。")
        elif (
            row is None
            or row.archived
            or draft.expected_head is None
            or row.version != draft.expected_head.version
            or row.content_sha256 != draft.expected_head.content_sha256
        ):
            raise HTTPException(status_code=409, detail="当前因子已变化，请刷新后重试。")
        try:
            instance_id = _verified_registry_instance_id(borrowed)
            pointer_matches = _pointer_matches(web, borrowed)
        except Exception as exc:
            raise HTTPException(status_code=503, detail=_UNREADABLE) from exc
        if not pointer_matches:
            raise HTTPException(status_code=409, detail="数据已更新，请刷新因子后重试。")
        return instance_id


def _save_response(
    request: Request,
    draft: FactorSaveDraft,
    result: FactorArchiveAdmissionResult | None,
    *,
    actor: str,
    fallback_status: str = "uncertain",
) -> Envelope[FactorSaveCommandData]:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        ).model_copy(update={"detail": ""})
        state = fallback_status
        message = (
            "公式未受理，请修改后重试。"
            if fallback_status == "rejected"
            else "保存结果待确认，请保留这次操作并继续查看。"
        )
        factor_id: str | None = None
        version: int | None = None
        digest: str | None = None
        current_head_updated = False
        if result is not None:
            receipt = result.receipt
            if receipt.command_id != draft.command_id or receipt.enqueued_at != draft.requested_at:
                raise HTTPException(status_code=503, detail="保存回执无法核对。")
            if receipt.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}:
                state, message = "pending", "正在保存，请稍后查看。"
            elif receipt.status is PageControlStatus.FAILED:
                state, message = "rejected", "保存未完成，请检查因子后重试。"
            elif receipt.status is PageControlStatus.SUCCEEDED:
                try:
                    effect = FactorDefinitionReceipt.model_validate(receipt.result)
                except ValueError as exc:
                    raise HTTPException(status_code=503, detail="保存回执无法核对。") from exc
                expected_id = draft_factor_id(draft, authenticated_actor_id=actor)
                expected_version = (
                    1 if draft.expected_head is None else draft.expected_head.version + 1
                )
                if (
                    effect.command_id != draft.command_id
                    or effect.action != "save"
                    or effect.factor_id != expected_id
                    or effect.version != expected_version
                    or effect.content_sha256 != result.definition_sha256
                    or effect.archived
                    or receipt.completed_at is None
                ):
                    raise HTTPException(status_code=503, detail="保存回执无法核对。")
                factor_id, version, digest = (
                    effect.factor_id,
                    effect.version,
                    effect.content_sha256,
                )
                state, message = "succeeded_waiting_publication", "已提交，等待数据更新。"
                if (
                    borrowed is not None
                    and meta.state is ServingState.READY
                    and meta.generation_id != draft.generation_id
                    and borrowed.manifest.built_at > receipt.completed_at
                ):
                    try:
                        catalog = _read_catalog(borrowed)
                        same_registry = (
                            _verified_registry_instance_id(borrowed) == result.registry_instance_id
                        )
                        current_pointer = _pointer_matches(web, borrowed)
                    except (HTTPException, OSError, ValueError, duckdb.Error):
                        catalog = None
                        same_registry = False
                        current_pointer = False
                    if catalog is not None and same_registry and current_pointer:
                        row = next(
                            (item for item in catalog.definitions if item.factor_id == factor_id),
                            None,
                        )
                        if (
                            row is not None
                            and row.version == version
                            and row.content_sha256 == digest
                            and not row.archived
                        ):
                            state, message = "published", "已保存。"
                        elif row is not None and row.version > version:
                            state = "published"
                            current_head_updated = True
                            message = "已保存，当前已有新版本。"
        return Envelope[FactorSaveCommandData](
            data=FactorSaveCommandData(
                status=state,
                command_id=draft.command_id,
                factor_id=factor_id,
                version=version,
                content_sha256=digest,
                current_head_updated=current_head_updated,
                message=message,
            ),
            serving=meta,
        )


@router.get(
    "/capabilities",
    response_model=Envelope[FactorCapabilitiesData],
    summary="可用因子字段与算子",
)
def factor_capabilities(
    request: Request,
    response: Response,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FactorCapabilitiesData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        can_save = False
        if (
            viewer in web.settings.factor_editor_users
            and web.settings.factor_save_enabled
            and web.factor_admission is not None
            and borrowed is not None
            and meta.state is ServingState.READY
        ):
            try:
                catalog = _read_catalog(borrowed)
                _verified_registry_instance_id(borrowed)
                can_save = catalog.availability in {"empty", "populated"} and _pointer_matches(
                    web, borrowed
                )
            except (HTTPException, OSError, ValueError, duckdb.Error):
                pass
        data = FactorCapabilitiesData.model_validate(
            {**HISTORICAL_DAILY_V1.model_dump(mode="python"), "can_save": can_save}
        )
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[FactorCapabilitiesData](data=data, serving=meta)


@router.post(
    "/definitions/save",
    response_model=Envelope[FactorSaveCommandData],
    summary="保存因子草稿",
)
def save_factor_draft(
    request: Request,
    body: FactorSaveDraft,
    actor: Annotated[str, Depends(_save_editor)],
    _csrf: Annotated[None, Depends(require_csrf)],
) -> Envelope[FactorSaveCommandData]:
    instance_id = _save_preflight(request, body, actor=actor)
    try:
        result = request.app.state.web.factor_admission.submit_save(
            body,
            authenticated_actor_id=actor,
            verified_registry_instance_id=instance_id,
        )
    except FactorDefinitionAdmissionRejectedError:
        return _save_response(request, body, None, actor=actor, fallback_status="rejected")
    except (FactorDefinitionAdmissionUnavailableError, OSError, TimeoutError):
        return _save_response(request, body, None, actor=actor)
    return _save_response(request, body, result, actor=actor)


@router.post(
    "/definitions/save/resume",
    response_model=Envelope[FactorSaveCommandData],
    summary="续查原保存命令",
)
def resume_factor_save(
    request: Request,
    body: FactorSaveDraft,
    actor: Annotated[str, Depends(_save_editor)],
    _csrf: Annotated[None, Depends(require_csrf)],
) -> Envelope[FactorSaveCommandData]:
    try:
        client = request.app.state.web.factor_admission
        result = client.lookup_save(body, authenticated_actor_id=actor)
        if result is None:
            return _save_response(request, body, None, actor=actor)
        if result.receipt.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}:
            result = client.resume_save(body, authenticated_actor_id=actor)
    except FactorDefinitionAdmissionRejectedError:
        return _save_response(request, body, None, actor=actor, fallback_status="rejected")
    except (FactorDefinitionAdmissionUnavailableError, OSError, TimeoutError):
        return _save_response(request, body, None, actor=actor)
    return _save_response(request, body, result, actor=actor)


@router.post(
    "/definitions/save/retry",
    response_model=Envelope[FactorSaveCommandData],
    summary="重试原保存命令",
)
def retry_original_factor_save(
    request: Request,
    body: FactorSaveDraft,
    actor: Annotated[str, Depends(_save_editor)],
    _csrf: Annotated[None, Depends(require_csrf)],
) -> Envelope[FactorSaveCommandData]:
    client = request.app.state.web.factor_admission
    try:
        found = client.lookup_save(body, authenticated_actor_id=actor)
    except FactorDefinitionAdmissionRejectedError:
        return _save_response(request, body, None, actor=actor, fallback_status="rejected")
    except (FactorDefinitionAdmissionUnavailableError, OSError, TimeoutError):
        return _save_response(request, body, None, actor=actor)
    if found is not None:
        try:
            if found.receipt.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}:
                found = client.resume_save(body, authenticated_actor_id=actor)
        except (FactorDefinitionAdmissionUnavailableError, OSError, TimeoutError):
            return _save_response(request, body, None, actor=actor)
        return _save_response(request, body, found, actor=actor)
    try:
        instance_id = _save_preflight(request, body, actor=actor)
        result = client.submit_save(
            body,
            authenticated_actor_id=actor,
            verified_registry_instance_id=instance_id,
        )
    except (
        HTTPException,
        FactorDefinitionAdmissionRejectedError,
        FactorDefinitionAdmissionUnavailableError,
        OSError,
        TimeoutError,
    ):
        return _save_response(request, body, None, actor=actor)
    return _save_response(request, body, result, actor=actor)
