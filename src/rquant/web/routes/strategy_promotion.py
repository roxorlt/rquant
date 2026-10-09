"""One-generation reads and original private manual promotion commands."""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request, Response
from pydantic import TypeAdapter

from rquant.experiment_platform_projection import ExperimentAttemptFact, ExperimentFamilyFact
from rquant.page_control import PageControlCommandConflictError, PageControlStatus
from rquant.serving_publisher import ServingReader
from rquant.strategy_authoring_admission import (
    StrategyAuthoringAdmissionNotFoundError,
    StrategyAuthoringAdmissionRejectedError,
    StrategyAuthoringAdmissionUnavailableError,
    StrategyPromotionAdmissionResult,
)
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity, StrategyTemplateHead
from rquant.strategy_promotion_commands import (
    ApprovePromotion,
    PreparePromotionApproval,
    RequestPromotionReview,
    RunStrategyWalkForward,
    StrategyPromotionCommand,
    StrategyPromotionRateLimitError,
)
from rquant.strategy_promotion_contracts import (
    PreparedPromotionApproval,
    StrategyPromotionApproval,
    StrategyPromotionCandidateReference,
    StrategyPromotionContext,
    StrategyPromotionReview,
)
from rquant.strategy_promotion_walk_forward import PromotionWalkForwardSubmission
from rquant.web import readers
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.experiment_platform_service import ExperimentWebService
from rquant.web.models.strategy_promotion import StrategyPromotionCommandData, StrategyPromotionData
from rquant.web.security import collaboration_me, current_user, require_csrf
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.strategy_authoring_reader import read_strategy_authoring
from rquant.web.strategy_promotion_reader import (
    StrategyPromotionPublishedFacts,
    read_strategy_promotion,
)

router = APIRouter(prefix="/strategy-promotions")
MAX_PROMOTION_REQUEST_BYTES = 32 * 1024
_Generation = Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")]
_StrategyId = Annotated[str, Path(pattern=r"^[A-Za-z0-9_.:-]{1,128}$")]
_SourceKind = Literal["template", "builtin"]
_UNREADABLE = "评估记录暂不可用，请稍后重试。"


def _meta(web: object, borrowed: BorrowedGeneration | None) -> ServingMeta:
    return serving_meta(
        borrowed, now=web.clock(), stale_after=web.settings.stale_after, failure=web.tracker.failure
    ).model_copy(update={"detail": ""})


def _pointer_matches(web: object, borrowed: BorrowedGeneration | None) -> bool:
    if borrowed is None or borrowed.pointer is None:
        return False
    pointer = ServingReader(web.settings.serving_root).current_pointer()
    return (pointer.generation_id, pointer.manifest_sha256) == (
        borrowed.pointer.generation_id,
        borrowed.pointer.manifest_sha256,
    )


def _facts(
    borrowed: BorrowedGeneration | None, actor: str
) -> StrategyPromotionPublishedFacts | None:
    try:
        return read_strategy_promotion(borrowed, owner_id=actor)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=_UNREADABLE) from exc


def _published_choice(
    borrowed: BorrowedGeneration | None, candidate: StrategyPromotionCandidateReference
) -> bool:
    if not ExperimentWebService._published(borrowed):
        return False
    owner, selection = candidate.target.owner_id, candidate.selection
    rows = borrowed.cursor.execute(
        "SELECT payload_json FROM experiment_private_family WHERE owner=? AND family_id=? LIMIT 2",
        (owner, selection.family_id),
    ).fetchall()
    if not rows:
        return False
    if len(rows) != 1:
        raise ValueError("original parent index differs")
    family = ExperimentFamilyFact.model_validate_json(rows[0][0])
    if (
        family.owner,
        family.family_id,
        family.phase,
        family.preparation_state,
        family.planned_count,
    ) != (owner, selection.family_id, "search", "ready", candidate.parent_count):
        raise ValueError("original parent owner/count differs")
    rows = borrowed.cursor.execute(
        "SELECT payload_json FROM experiment_private_attempt "
        "WHERE owner=? AND family_id=? ORDER BY experiment_id LIMIT 65",
        (owner, selection.family_id),
    ).fetchall()
    facts = tuple(ExperimentAttemptFact.model_validate_json(row[0]) for row in rows)
    if (
        len(facts) != family.planned_count
        or sorted(f.index for f in facts) != list(range(family.planned_count))
        or any(
            (f.owner, f.family_id, f.child.owner, f.child.family_id, f.child.experiment_id)
            != (owner, family.family_id, owner, family.family_id, f.attempt.spec.experiment_id)
            for f in facts
        )
    ):
        raise ValueError("complete original parent differs")
    selected = next(
        (f for f in facts if f.attempt.spec.experiment_id == selection.experiment_id), None
    )
    return (
        selected is not None
        and family.request.template == candidate.template_parent
        and (
            selected.child.job_id,
            selected.attempt.spec.cost_model_fingerprint,
            selected.attempt.spec.strategy_spec_fingerprint,
            selected.attempt.spec.train_range,
            selected.attempt.spec.validation_range,
            selected.input_hash,
            selected.spec_hash,
            selected.manifest_hash,
            selected.result_hash,
        )
        == (
            candidate.job_id,
            candidate.target.cost_fingerprint,
            candidate.target.head.spec_fingerprint,
            candidate.train_window,
            candidate.validation_window,
            candidate.input_hash,
            candidate.spec_hash,
            candidate.manifest_hash,
            candidate.result_hash,
        )
        and (selected.result_hash is not None and selected.manifest_hash is not None)
        == candidate.has_sealed_reference
    )


def _context(
    web: object,
    borrowed: BorrowedGeneration | None,
    actor: str,
    strategy_id: str,
    source_kind: _SourceKind,
    *,
    version: int | None = None,
    expected_head: StrategyTemplateHead | None = None,
) -> StrategyPromotionContext | None:
    gateway = web.strategy_promotion_gateway
    if gateway is None or borrowed is None:
        return None
    head = expected_head
    identity = None
    if source_kind == "template":
        snapshot = read_strategy_authoring(borrowed)
        rows = (
            ()
            if snapshot is None
            else tuple(
                row
                for row in snapshot.for_owner(actor)
                if row.metadata.strategy_id == strategy_id
                and (row.is_head if version is None else row.metadata.head.version == version)
            )
        )
        if len(rows) > 1:
            raise HTTPException(status_code=404, detail="策略版本不存在。")
        if rows:
            if head is not None and head != rows[0].metadata.head:
                raise HTTPException(status_code=409, detail="策略版本已更新，请重新查看。")
            head, identity = rows[0].metadata.head, snapshot.identity
    else:
        states = readers.table_states(borrowed.cursor)
        if not (states.get("strategy_catalog") and states["strategy_catalog"].available):
            return None
        rows = borrowed.cursor.execute(
            "SELECT version FROM strategy_catalog WHERE strategy_id=? LIMIT 2", (strategy_id,)
        ).fetchall()
        if len(rows) != 1 or version is not None and rows[0][0] != version:
            raise HTTPException(status_code=404, detail="策略版本不存在。")
    context = gateway.promotion_context(
        authenticated_actor_id=actor, source_kind=source_kind, strategy_id=strategy_id, head=head
    )
    if (
        (context.owner_id, context.source_kind, context.requested_strategy_id)
        != (actor, source_kind, strategy_id)
        or head is not None
        and context.requested_head != head
        or identity is not None
        and (context.original_metadata_identity or context.metadata_identity) != identity
    ):
        raise HTTPException(status_code=409, detail="策略已更新，请等待页面更新后重试。")
    if version is not None and context.requested_head.version != version:
        raise HTTPException(status_code=404, detail="策略版本不存在。")
    if source_kind == "builtin" and any(
        candidate.target.head.version != rows[0][0] for candidate in context.candidates
    ):
        raise HTTPException(status_code=409, detail="策略版本已更新，请重新查看。")
    try:
        choices = tuple(
            candidate for candidate in context.candidates if _published_choice(borrowed, candidate)
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="原验证证据暂不可用。") from exc
    keys = {item.target.version_key for item in choices}
    return StrategyPromotionContext.model_validate(
        context.model_dump(mode="python")
        | {
            "candidates": choices,
            "walk_forward": tuple(item for item in context.walk_forward if item.target_key in keys),
            "paper_accounts": tuple(
                item for item in context.paper_accounts if item.target_key in keys
            ),
        }
    )


@router.get(
    "/{strategy_id}", response_model=Envelope[StrategyPromotionData], summary="策略阶段与评估"
)
def detail(
    request: Request,
    response: Response,
    strategy_id: _StrategyId,
    viewer: Annotated[str | None, Depends(current_user)],
    source_kind: _SourceKind = "template",
    version: Annotated[int | None, Query(ge=1, le=4096)] = None,
    generation_id: _Generation = None,
    offset: Annotated[int, Query(ge=0, le=1000)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> Envelope[StrategyPromotionData]:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = _meta(web, borrowed)
        if generation_id is not None and generation_id != meta.generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请重新查看评估记录。")
        if offset and generation_id is None:
            raise HTTPException(status_code=409, detail="请从第一页重新查看评估记录。")
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        facts = _facts(borrowed, viewer)
        context, reason = None, "人工晋级未启用。"
        if facts is not None and web.strategy_promotion_gateway is not None:
            try:
                context = _context(web, borrowed, viewer, strategy_id, source_kind, version=version)
            except StrategyAuthoringAdmissionNotFoundError:
                reason = "此账号没有此策略的评估记录。"
            except (StrategyAuthoringAdmissionRejectedError, PermissionError):
                reason = "此策略尚未配置完整的评估来源。"
            except StrategyAuthoringAdmissionUnavailableError:
                reason = "评估来源暂不可用。"
            except (ValueError, OSError, RuntimeError) as exc:
                raise HTTPException(status_code=503, detail="评估来源暂不可用。") from exc
        role = collaboration_me(request, viewer)
        permitted = bool(
            web.settings.strategy_promotion_enabled
            and viewer in web.settings.strategy_promotion_users
            and role.available
            and role.can_research
            and context is not None
            and meta.state is ServingState.READY
            and _pointer_matches(web, borrowed)
        )
        keys = (
            set() if context is None else {item.target.version_key for item in context.candidates}
        )
        states = (
            ()
            if facts is None
            else tuple(
                item
                for item in facts.states
                if item.state.target.version_key in keys
                or item.state.target.strategy_id == strategy_id
                and item.state.target.source_kind == source_kind
                and (version is None or item.state.target.head.version == version)
            )
        )
        reviews = (
            ()
            if facts is None
            else tuple(
                sorted(
                    (
                        item.review
                        for item in facts.reviews
                        if item.review.target.version_key in keys
                        or item.review.target.strategy_id == strategy_id
                        and item.review.target.source_kind == source_kind
                        and (version is None or item.review.target.head.version == version)
                    ),
                    key=lambda value: (value.observed_at, value.review_id),
                    reverse=True,
                )
            )
        )
        page = reviews[offset : offset + limit]
        if context is not None and not context.candidates:
            reason = "还没有同版本的预登记验证结果。"
        elif context is not None and permitted:
            reason = ""
        elif context is not None and not role.can_research:
            reason = "当前角色只能查看评估记录。"
        data = StrategyPromotionData(
            availability="unavailable"
            if facts is None
            else "populated"
            if states or page or context is not None and context.candidates
            else "empty",
            source_kind=source_kind,
            strategy_id=strategy_id,
            available_at=None if facts is None else facts.available_at,
            states=states,
            reviews=page,
            next_offset=offset + limit if offset + limit < len(reviews) else None,
            candidates=() if context is None else context.candidates,
            walk_forward=() if context is None else context.walk_forward,
            paper_accounts=() if context is None else context.paper_accounts,
            can_evaluate=permitted and context.can_evaluate,
            can_prepare_approval=permitted and role.role == "admin" and context.can_approve,
            can_run_walk_forward=permitted and context.can_run_walk_forward,
            reason=reason,
        )
    return Envelope(data=data, serving=meta)


def _preflight(
    request: Request, body: StrategyPromotionCommand, actor: str
) -> StrategyAuthoringIdentity:
    web = request.app.state.web
    role = collaboration_me(request, actor)
    if (
        not web.settings.strategy_promotion_enabled
        or actor not in web.settings.strategy_promotion_users
        or not role.available
        or not role.can_research
        or isinstance(body, (ApprovePromotion, PreparePromotionApproval))
        and role.role != "admin"
    ):
        raise HTTPException(status_code=403, detail="当前角色不能执行此操作。")
    if body.target.owner_id != actor:
        raise HTTPException(status_code=403, detail="此策略不属于当前账号。")
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = _meta(web, borrowed)
        if meta.state is not ServingState.READY or meta.generation_id != body.generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请重新查看策略。")
        facts = _facts(borrowed, actor)
        context = _context(
            web,
            borrowed,
            actor,
            body.target.strategy_id,
            body.target.source_kind,
            version=body.target.head.version,
            expected_head=body.target.head,
        )
        if (
            facts is None
            or context is None
            or facts.metadata_identity is not None
            and facts.metadata_identity != context.metadata_identity
            or not context.can_evaluate
        ):
            raise HTTPException(status_code=409, detail="策略或权限已更新，请重新核对。")
        if not any(
            candidate.target == body.target and candidate.is_current
            for candidate in context.candidates
        ):
            raise HTTPException(status_code=409, detail="原验证结果尚未发布，请稍后重试。")
        if isinstance(body, (RequestPromotionReview, RunStrategyWalkForward)) and not any(
            candidate.target == body.target
            and candidate.selection.family_id == body.selection.family_id
            and candidate.selection.experiment_id == body.selection.experiment_id
            for candidate in context.candidates
        ):
            raise HTTPException(status_code=409, detail="原证据选择已变化，请重新核对。")
        if isinstance(body, RunStrategyWalkForward) and not context.can_run_walk_forward:
            raise HTTPException(status_code=409, detail="固定参数验证尚未配置。")
        if isinstance(body, PreparePromotionApproval) and not any(
            item.review_id == body.review_id and item.target == body.target
            for item in (fact.review for fact in facts.reviews)
        ):
            raise HTTPException(status_code=409, detail="评估记录尚未发布，请稍后重试。")
        if not _pointer_matches(web, borrowed):
            raise HTTPException(status_code=409, detail="数据已更新，请重新查看策略。")
        return context.metadata_identity


def _result(
    request: Request,
    body: StrategyPromotionCommand,
    actor: str,
    checked: StrategyPromotionAdmissionResult | None,
    *,
    status: Literal["not_registered", "uncertain", "rejected"] = "uncertain",
) -> Envelope[StrategyPromotionCommandData]:
    web = request.app.state.web
    web.tracker.refresh()
    values = {
        "original_request": body,
        "status": status,
        "message": {
            "uncertain": "结果待确认，请查询原操作。",
            "rejected": "操作未执行，请重新核对。",
            "not_registered": "原操作尚未登记。",
        }[status],
    }
    with web.tracker.borrow() as borrowed:
        meta = _meta(web, borrowed)
        if checked is not None:
            checked.bind(body, actor_id=actor)
            receipt = checked.receipt
            values["receipt"] = receipt
            if receipt.status is PageControlStatus.SUCCEEDED:
                values.update(status="completed", message="操作已完成。")
                facts = _facts(borrowed, actor)
                bound = facts is not None and facts.metadata_identity == checked.metadata_identity
                if isinstance(body, RequestPromotionReview):
                    review = StrategyPromotionReview.model_validate(receipt.result)
                    values.update(
                        review=review,
                        status="completed_waiting_publication",
                        message="评估已完成，等待页面更新。",
                    )
                    if bound and any(fact.review == review for fact in facts.reviews):
                        values.update(status="published", message="评估已完成。")
                elif isinstance(body, PreparePromotionApproval):
                    values.update(
                        preparation=PreparedPromotionApproval.model_validate(receipt.result),
                        message="请核对评估，并输入策略名称确认。",
                    )
                elif isinstance(body, ApprovePromotion):
                    approval = StrategyPromotionApproval.model_validate(receipt.result)
                    values.update(
                        approval=approval,
                        status="completed_waiting_publication",
                        message="阶段已批准，等待页面更新。",
                    )
                    if bound and any(
                        fact.state == approval.after and fact.applied_at == approval.applied_at
                        for fact in facts.states
                    ):
                        values.update(status="published", message="阶段已批准。")
                else:
                    values.update(
                        walk_forward=TypeAdapter(PromotionWalkForwardSubmission).validate_python(
                            receipt.result
                        ),
                        message="验证任务已提交，请等待全部结果。",
                    )
            elif receipt.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}:
                values.update(status="pending", message="操作进行中，请查询原操作。")
            elif receipt.status is PageControlStatus.AMBIGUOUS:
                values.update(status="uncertain", message="结果待确认，请保留原操作。")
            else:
                values.update(status="rejected", message="操作未完成，请核对当前权限与证据。")
        data = StrategyPromotionCommandData.model_validate(values)
    return Envelope(data=data, serving=meta)


def _command(
    request: Request,
    body: StrategyPromotionCommand,
    actor: str,
    *,
    mode: Literal["submit", "lookup", "resume"],
) -> Envelope[StrategyPromotionCommandData]:
    gateway = request.app.state.web.strategy_promotion_gateway
    if gateway is None:
        raise HTTPException(status_code=503, detail="人工晋级暂不可用。")
    try:
        checked = gateway.promotion_lookup(body, authenticated_actor_id=actor)
        if mode == "lookup":
            return _result(request, body, actor, checked, status="not_registered")
        if checked is not None:
            checked = gateway.promotion_resume(body, authenticated_actor_id=actor)
        elif mode == "resume":
            return _result(request, body, actor, None, status="not_registered")
        else:
            identity = _preflight(request, body, actor)
            checked = gateway.promotion_submit(
                body, authenticated_actor_id=actor, verified_metadata_identity=identity
            )
    except StrategyPromotionRateLimitError as exc:
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后重试。") from exc
    except (
        StrategyAuthoringAdmissionRejectedError,
        PageControlCommandConflictError,
        PermissionError,
    ):
        return _result(request, body, actor, None, status="rejected")
    except StrategyAuthoringAdmissionNotFoundError:
        return _result(request, body, actor, None, status="uncertain")
    except (StrategyAuthoringAdmissionUnavailableError, OSError, TimeoutError):
        return _result(request, body, actor, None, status="uncertain")
    return _result(request, body, actor, checked)


@router.post(
    "/commands",
    response_model=Envelope[StrategyPromotionCommandData],
    summary="评估或批准策略阶段",
    dependencies=[Depends(require_csrf)],
)
def commands(
    request: Request,
    body: Annotated[StrategyPromotionCommand, Body(discriminator="kind")],
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[StrategyPromotionCommandData]:
    return _command(request, body, viewer, mode="submit")


@router.post(
    "/commands/lookup",
    response_model=Envelope[StrategyPromotionCommandData],
    summary="查询原阶段操作",
    dependencies=[Depends(require_csrf)],
)
def lookup(
    request: Request,
    body: Annotated[StrategyPromotionCommand, Body(discriminator="kind")],
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[StrategyPromotionCommandData]:
    return _command(request, body, viewer, mode="lookup")


@router.post(
    "/commands/resume",
    response_model=Envelope[StrategyPromotionCommandData],
    summary="恢复原阶段操作",
    dependencies=[Depends(require_csrf)],
)
def resume(
    request: Request,
    body: Annotated[StrategyPromotionCommand, Body(discriminator="kind")],
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[StrategyPromotionCommandData]:
    return _command(request, body, viewer, mode="resume")
