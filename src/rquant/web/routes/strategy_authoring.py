"""Private template reads and original commands through the existing journal."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request, Response

from rquant.llm.registry import REGISTRY
from rquant.page_control import PageControlCommandConflictError, PageControlStatus
from rquant.serving_publisher import ServingReader
from rquant.strategy_authoring_admission import StrategyAuthoringAdmissionNotFoundError, StrategyAuthoringAdmissionRejectedError, StrategyAuthoringAdmissionResult, StrategyAuthoringAdmissionUnavailableError
from rquant.strategy_authoring_commands import ArchiveStrategyTemplate, SaveStrategyTemplate, StrategyAuthoringIdentity, StrategyTemplateCommand, StrategyTemplateReceipt
from rquant.strategy_authoring_projection import StrategyAuthoringSnapshot, StrategyTemplatePublishedVersion
from rquant.strategy_template import TEMPLATE_ID_PATTERN, _COLUMN_NAMES
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.models.strategy_authoring import StrategyTemplateCatalogData, StrategyTemplateCommandData, StrategyTemplateConditionChoice, StrategyTemplateDetailData, StrategyTemplateItem, StrategyTemplateSourcesData, StrategyTemplateVersionItem, StrategyTemplateVersionsData
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.strategy_authoring_reader import read_strategy_authoring

router = APIRouter(prefix="/strategy-templates")
MAX_TEMPLATE_REQUEST_BYTES = 32 * 1024
MAX_ARCHIVE_REQUEST_BYTES = 4 * 1024
_UNREADABLE = "策略暂时无法核验，请稍后重试。"
_Generation = Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")]
_StrategyId = Annotated[str, Path(pattern=TEMPLATE_ID_PATTERN)]


def _pointer_matches(web: object, borrowed: BorrowedGeneration | None) -> bool:
    if borrowed is None or borrowed.pointer is None:
        return False
    pointer = ServingReader(web.settings.serving_root).current_pointer()
    return (pointer.generation_id, pointer.manifest_sha256) == (borrowed.pointer.generation_id, borrowed.pointer.manifest_sha256)


def _meta(web: object, borrowed: BorrowedGeneration | None) -> ServingMeta:
    return serving_meta(borrowed, now=web.clock(), stale_after=web.settings.stale_after, failure=web.tracker.failure).model_copy(update={"detail": ""})


def _snapshot(borrowed: BorrowedGeneration | None) -> StrategyAuthoringSnapshot | None:
    try:
        return read_strategy_authoring(borrowed)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=_UNREADABLE) from exc


def _can_write(web: object, actor: str | None, meta: ServingMeta, snapshot: StrategyAuthoringSnapshot | None) -> bool:
    return bool(web.settings.strategy_authoring_enabled and actor in web.settings.strategy_authoring_users and web.strategy_authoring_gateway is not None and meta.state is ServingState.READY and snapshot is not None)


def _generation(meta: ServingMeta, generation_id: str | None, response: Response) -> None:
    if generation_id is not None and generation_id != meta.generation_id:
        raise HTTPException(status_code=409, detail="数据已更新，请重新查看策略。")
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id


def _item(row: StrategyTemplatePublishedVersion) -> StrategyTemplateItem:
    value = row.metadata
    return StrategyTemplateItem(strategy_id=value.strategy_id, name=value.name, head=value.head, saved_at=value.saved_at, entry_kind=value.rules.entry.kind, archived=row.archived, phase="未评估" if row.latest_run is None else "已有回测", latest_run=row.latest_run)


@router.get("", response_model=Envelope[StrategyTemplateCatalogData], summary="我的策略模板")
def templates(request: Request, response: Response, viewer: Annotated[str | None, Depends(current_user)], generation_id: _Generation = None) -> Envelope[StrategyTemplateCatalogData]:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = _meta(web, borrowed)
        _generation(meta, generation_id, response)
        snapshot = _snapshot(borrowed)
        rows = () if snapshot is None else tuple(row for row in snapshot.for_owner(viewer) if row.is_head)
        data = StrategyTemplateCatalogData(availability="unavailable" if snapshot is None else "populated" if rows else "empty", available_at=None if snapshot is None else snapshot.available_at, templates=tuple(_item(row) for row in rows), can_create=_can_write(web, viewer, meta, snapshot))
    return Envelope(data=data, serving=meta)


@router.get("/sources", response_model=Envelope[StrategyTemplateSourcesData], summary="策略入场来源")
def sources(request: Request, response: Response, viewer: Annotated[str | None, Depends(current_user)], generation_id: _Generation = None) -> Envelope[StrategyTemplateSourcesData]:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = _meta(web, borrowed)
        _generation(meta, generation_id, response)
        snapshot = _snapshot(borrowed)
        selected = None if snapshot is None else snapshot.sources_for(viewer, generation_id=meta.generation_id)
        data = StrategyTemplateSourcesData(availability="unavailable" if selected is None else "populated" if selected.pools or selected.signals else "empty", pools=() if selected is None else selected.pools, signals=() if selected is None else selected.signals, conditions=tuple(StrategyTemplateConditionChoice(key=rule.name, label=rule.description, parameter_schema=rule.args_model.model_json_schema()) for rule in REGISTRY), comparison_fields=tuple(sorted(_COLUMN_NAMES)), can_create=_can_write(web, viewer, meta, snapshot))
    return Envelope(data=data, serving=meta)


def _owned_versions(snapshot: StrategyAuthoringSnapshot | None, strategy_id: str, viewer: str | None) -> tuple[StrategyTemplatePublishedVersion, ...]:
    if snapshot is None:
        raise HTTPException(status_code=503, detail=_UNREADABLE)
    rows = tuple(row for row in snapshot.for_owner(viewer) if row.metadata.strategy_id == strategy_id)
    if not rows:
        raise HTTPException(status_code=404, detail="策略不存在。")
    return rows


@router.get("/{strategy_id}", response_model=Envelope[StrategyTemplateDetailData], summary="策略规则与版本")
def detail(strategy_id: _StrategyId, request: Request, response: Response, viewer: Annotated[str | None, Depends(current_user)], generation_id: _Generation = None, version: Annotated[int | None, Query(ge=1, le=4096)] = None) -> Envelope[StrategyTemplateDetailData]:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = _meta(web, borrowed)
        _generation(meta, generation_id, response)
        snapshot = _snapshot(borrowed)
        rows = _owned_versions(snapshot, strategy_id, viewer)
        current = rows[-1]
        selected = current if version is None else next((row for row in rows if row.metadata.head.version == version), None)
        if selected is None:
            raise HTTPException(status_code=404, detail="版本不存在。")
        value = selected.metadata
        writable = _can_write(web, viewer, meta, snapshot) and not current.archived
        data = StrategyTemplateDetailData(strategy_id=value.strategy_id, name=value.name, head=value.head, current_head=current.metadata.head, rules=value.rules, saved_at=value.saved_at, change_note=value.change_note, archived=current.archived, latest_run=selected.latest_run, can_save=writable, can_archive=writable)
    return Envelope(data=data, serving=meta)


@router.get("/{strategy_id}/versions", response_model=Envelope[StrategyTemplateVersionsData], summary="策略版本历史")
def versions(strategy_id: _StrategyId, request: Request, response: Response, viewer: Annotated[str | None, Depends(current_user)], generation_id: _Generation = None, before_version: Annotated[int | None, Query(ge=1, le=4097)] = None, limit: Annotated[int, Query(ge=1, le=100)] = 50) -> Envelope[StrategyTemplateVersionsData]:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = _meta(web, borrowed)
        _generation(meta, generation_id, response)
        snapshot = _snapshot(borrowed)
        rows = _owned_versions(snapshot, strategy_id, viewer)
        filtered = tuple(row for row in reversed(rows) if before_version is None or row.metadata.head.version < before_version)
        selected = filtered[:limit]
        data = StrategyTemplateVersionsData(strategy_id=strategy_id, current_head=rows[-1].metadata.head, versions=tuple(StrategyTemplateVersionItem(head=row.metadata.head, saved_at=row.metadata.saved_at, change_note=row.metadata.change_note, is_head=row.is_head, latest_run=row.latest_run) for row in selected), next_before_version=selected[-1].metadata.head.version if len(filtered) > limit else None)
    return Envelope(data=data, serving=meta)


def _editor(request: Request, viewer: Annotated[str | None, Depends(current_user)]) -> str:
    web = request.app.state.web
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if not web.settings.strategy_authoring_enabled or web.strategy_authoring_gateway is None:
        raise HTTPException(status_code=503, detail="策略编辑暂未开放。")
    if viewer not in web.settings.strategy_authoring_users:
        raise HTTPException(status_code=403, detail="当前账号不能编辑策略。")
    if request.query_params:
        raise HTTPException(status_code=422, detail="请刷新后重试。")
    return viewer


def _preflight(request: Request, body: StrategyTemplateCommand, actor: str) -> StrategyAuthoringIdentity:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = _meta(web, borrowed)
        snapshot = _snapshot(borrowed)
        if not _can_write(web, actor, meta, snapshot):
            raise HTTPException(status_code=503, detail=_UNREADABLE)
        if meta.generation_id != body.generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请刷新后重试。")
        if body.strategy_id is not None:
            rows = _owned_versions(snapshot, body.strategy_id, actor)
            if rows[-1].archived or rows[-1].metadata.head != body.expected_head:
                raise HTTPException(status_code=409, detail="策略已变化，请刷新后重试。")
        if isinstance(body, SaveStrategyTemplate):
            try:
                snapshot.sources_for(actor, generation_id=body.generation_id).validate_rules(body.rules, owner_id=actor, generation_id=body.generation_id)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail="入场来源已变化，请重新选择。") from exc
        try:
            matches = _pointer_matches(web, borrowed)
        except Exception as exc:
            raise HTTPException(status_code=503, detail=_UNREADABLE) from exc
        if not matches:
            raise HTTPException(status_code=409, detail="数据已更新，请刷新后重试。")
        return snapshot.identity


def _result(request: Request, original: StrategyTemplateCommand, actor: str, result: StrategyAuthoringAdmissionResult | None, *, rejected: bool = False) -> Envelope[StrategyTemplateCommandData]:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = _meta(web, borrowed)
        status, message = ("rejected", "操作未受理，请检查后重试。") if rejected else ("uncertain", "结果待确认，请保留这次操作并继续查看。")
        effect = None
        advanced = False
        if result is not None:
            checked = StrategyAuthoringAdmissionResult.model_validate(result.model_dump(mode="python"))
            if checked.owner_id != actor or checked.original_request != original:
                raise HTTPException(status_code=503, detail="操作回执暂时无法核验。")
            receipt = checked.receipt
            if receipt.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}:
                status, message = "pending", "正在处理，请稍后查看。"
            elif receipt.status is PageControlStatus.FAILED:
                status, message = "rejected", "操作未完成，请检查后重试。"
            elif receipt.status is PageControlStatus.SUCCEEDED:
                effect = StrategyTemplateReceipt.model_validate(receipt.result)
                status, message = "succeeded_waiting_publication", "已提交，等待数据更新。"
                if meta.state is ServingState.READY and borrowed is not None and borrowed.manifest.built_at >= effect.completed_at:
                    try:
                        snapshot = read_strategy_authoring(borrowed)
                        published = () if snapshot is None else snapshot.for_owner(actor)
                        exact = next((row for row in published if row.metadata.strategy_id == effect.strategy_id and row.metadata.head == effect.head), None)
                        current = next((row for row in published if row.metadata.strategy_id == effect.strategy_id and row.is_head), None)
                        if snapshot is not None and snapshot.identity == checked.metadata_identity and exact is not None and current is not None and _pointer_matches(web, borrowed) and (effect.action == "save" or current.archived):
                            advanced = current.metadata.head != effect.head
                            status, message = "published", "已保存。" if effect.action == "save" else "已归档。"
                    except Exception:
                        pass
        data = StrategyTemplateCommandData(command_id=original.command_id, status=status, strategy_id=None if effect is None else effect.strategy_id, head=None if effect is None else effect.head, current_head_updated=advanced, message=message)
    return Envelope(data=data, serving=meta)


def _command(request: Request, body: StrategyTemplateCommand, actor: str, *, resume: bool) -> Envelope[StrategyTemplateCommandData]:
    if isinstance(body, ArchiveStrategyTemplate) and len(body.model_dump_json().encode()) > MAX_ARCHIVE_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="归档内容过长。")
    gateway = request.app.state.web.strategy_authoring_gateway
    try:
        found = gateway.lookup(body, authenticated_actor_id=actor)
        try:
            result = gateway.resume(body, authenticated_actor_id=actor)
        except StrategyAuthoringAdmissionNotFoundError:
            if found is not None:
                raise StrategyAuthoringAdmissionUnavailableError("original lookup lost accepted request")
            if resume:
                return _result(request, body, actor, None)
            identity = _preflight(request, body, actor)
            result = gateway.submit(body, authenticated_actor_id=actor, verified_metadata_identity=identity)
    except (PageControlCommandConflictError, StrategyAuthoringAdmissionRejectedError, PermissionError):
        return _result(request, body, actor, None, rejected=True)
    except (StrategyAuthoringAdmissionUnavailableError, OSError, TimeoutError, RuntimeError):
        return _result(request, body, actor, None)
    return _result(request, body, actor, result)


@router.post("/commands", response_model=Envelope[StrategyTemplateCommandData], summary="保存版本或归档策略")
def command(request: Request, body: Annotated[StrategyTemplateCommand, Body(discriminator="kind")], actor: Annotated[str, Depends(_editor)], _csrf: Annotated[None, Depends(require_csrf)]) -> Envelope[StrategyTemplateCommandData]:
    return _command(request, body, actor, resume=False)


@router.post("/commands/resume", response_model=Envelope[StrategyTemplateCommandData], summary="续查原策略操作")
def resume_command(request: Request, body: Annotated[StrategyTemplateCommand, Body(discriminator="kind")], actor: Annotated[str, Depends(_editor)], _csrf: Annotated[None, Depends(require_csrf)]) -> Envelope[StrategyTemplateCommandData]:
    return _command(request, body, actor, resume=True)
