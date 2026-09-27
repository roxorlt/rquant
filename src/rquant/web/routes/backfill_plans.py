"""Read sealed daily-bar proposals from one borrowed Serving generation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Literal, TypeVar

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response

from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS
from rquant.web.envelope import Envelope
from rquant.web.models.backfill_plans import (
    BackfillPlanCatalogRow,
    BackfillPlanDetail,
    BackfillPlanDetailData,
    BackfillPlanItem,
    BackfillPlanProgress,
    BackfillPlanProgressRow,
    BackfillPlansData,
    PlanSourceState,
)
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

if TYPE_CHECKING:
    from rquant.backfill_plan_projection import BackfillPlanServingDetail

router = APIRouter(prefix="/data")
_TABLES = tuple(
    sorted(name for name in PAGE_PROJECTION_CONTRACTS if name.startswith("backfill_plan_"))
)
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_CHANGED = "回补计划已更新，请从列表重新查看。"
_UNREADABLE = "回补计划暂时无法读取，请稍后重试。"
_CatalogOrProgress = TypeVar(
    "_CatalogOrProgress", BackfillPlanCatalogRow, BackfillPlanProgressRow
)


def _source_state(borrowed: BorrowedGeneration | None) -> PlanSourceState:
    if borrowed is None:
        return "unavailable"
    marks = borrowed.cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, "
        "owner_generation_id, available_at FROM projection_status "
        f"WHERE table_name IN ({', '.join('?' for _ in _TABLES)}) "
        "ORDER BY table_name LIMIT ?",
        (*_TABLES, len(_TABLES) + 1),
    ).fetchall()
    if not marks:
        if any(borrowed.manifest.row_counts.get(name, 0) for name in _TABLES):
            raise ValueError("plan tables are missing from projection status")
        return "not_published"
    if len(marks) != len(_TABLES) or tuple(item[0] for item in marks) != _TABLES:
        raise ValueError("plan projection status is incomplete")
    if any(
        type(available) is not bool or type(count) is not int
        for _, available, count, *_ in marks
    ):
        raise ValueError("plan projection status types are invalid")
    if not any(item[1] for item in marks):
        if any(count or borrowed.manifest.row_counts.get(name, 0) for name, _, count, *_ in marks):
            raise ValueError("unpublished plans have rows")
        return "not_published"
    watermark = next(
        (item for item in borrowed.manifest.watermarks if item.dataset_id == "lab_jobs"), None
    )
    if watermark is None:
        raise ValueError("plan owner watermark is absent")
    available_at = marks[0][5]
    if any(
        not available
        or owner != "lab_jobs"
        or generation != watermark.generation_id
        or at != available_at
        or at is None
        or at > borrowed.manifest.built_at
        or count != borrowed.manifest.row_counts.get(name)
        for name, available, count, owner, generation, at in marks
    ):
        raise ValueError("plan projection status disagrees with the generation")
    return "ready"


def _one_row(
    borrowed: BorrowedGeneration, table: str, model: type[_CatalogOrProgress]
) -> _CatalogOrProgress:
    contract = PAGE_PROJECTION_CONTRACTS[table]
    columns = contract.column_names
    rows = borrowed.cursor.execute(
        f"SELECT {', '.join(columns)} FROM {table} LIMIT 2"
    ).fetchall()
    if len(rows) != 1 or borrowed.manifest.row_counts.get(table) != 1:
        raise ValueError("plan singleton row count differs from manifest")
    return model.model_validate(dict(zip(columns, rows[0], strict=True)))


def _catalog_progress(
    borrowed: BorrowedGeneration,
) -> tuple[BackfillPlanCatalogRow, BackfillPlanProgress]:
    catalog = _one_row(borrowed, "backfill_plan_catalog", BackfillPlanCatalogRow)
    progress_row = _one_row(borrowed, "backfill_plan_progress", BackfillPlanProgressRow)
    indexed = borrowed.manifest.row_counts["backfill_plan_index"]
    preview = borrowed.manifest.row_counts["backfill_plan_preview"]
    if (
        catalog.total_plan_count != indexed
        or catalog.indexed_plan_count != indexed
        or catalog.preview_plan_count != preview
        or preview != min(indexed, 8)
        or catalog.has_older_plans
        or bool(catalog.oldest_indexed_hash) != bool(indexed)
    ):
        raise ValueError("plan catalog is incomplete")
    if "backfill_plan_archive" in _TABLES and (
        borrowed.manifest.row_counts["backfill_plan_archive"] != indexed
    ):
        raise ValueError("plan archive is incomplete")
    return catalog, BackfillPlanProgress(
        availability=progress_row.availability,
        task_id=progress_row.task_id,
        logs=[],
    )


def _empty(state: Literal["not_published", "unavailable"], page_size: int) -> BackfillPlansData:
    return BackfillPlansData(
        source_state=state,
        total=None,
        page_size=page_size,
        items=[],
        next_cursor=None,
        progress=None,
    )


def _index_row(borrowed: BorrowedGeneration, plan_hash: str) -> BackfillPlanItem | None:
    columns = PAGE_PROJECTION_CONTRACTS["backfill_plan_index"].column_names
    rows = borrowed.cursor.execute(
        f"SELECT {', '.join(columns)} FROM backfill_plan_index "
        "WHERE plan_hash = ? LIMIT 2",
        (plan_hash,),
    ).fetchall()
    if len(rows) > 1:
        raise ValueError("plan hash is duplicated")
    return (
        BackfillPlanItem.model_validate(dict(zip(columns, rows[0], strict=True)))
        if rows
        else None
    )


def _published_detail(
    borrowed: BorrowedGeneration, plan_hash: str
) -> BackfillPlanServingDetail | None:
    # This heavier module is loaded only for a detail request, after the app starts.
    from rquant.backfill_plan_projection import read_backfill_plan_detail_from_serving

    return read_backfill_plan_detail_from_serving(borrowed.cursor, plan_hash)


def _page(
    borrowed: BorrowedGeneration,
    *,
    page_size: int,
    cursor: str | None,
) -> BackfillPlansData:
    catalog, progress = _catalog_progress(borrowed)
    if cursor is None:
        first_rank = 0
    else:
        after = _index_row(borrowed, cursor)
        if after is None:
            raise HTTPException(status_code=409, detail=_CHANGED)
        first_rank = after.rank + 1
    columns = PAGE_PROJECTION_CONTRACTS["backfill_plan_index"].column_names
    rows = borrowed.cursor.execute(
        f"SELECT {', '.join(columns)} FROM backfill_plan_index "
        "WHERE rank >= ? ORDER BY rank LIMIT ?",
        (first_rank, page_size + 1),
    ).fetchall()
    items = [
        BackfillPlanItem.model_validate(dict(zip(columns, row, strict=True)))
        for row in rows[:page_size]
    ]
    if any(item.rank != first_rank + offset for offset, item in enumerate(items)):
        raise ValueError("plan index has a rank gap")
    if first_rank + len(items) < catalog.total_plan_count and len(rows) <= page_size:
        raise ValueError("plan index ended before catalog count")
    return BackfillPlansData(
        source_state="ready" if catalog.total_plan_count else "empty",
        total=catalog.total_plan_count,
        page_size=page_size,
        items=items,
        next_cursor=items[-1].plan_hash if len(rows) > page_size else None,
        progress=progress,
    )


@router.get(
    "/backfill-plans", response_model=Envelope[BackfillPlansData], summary="历史日线回补计划"
)
def get_backfill_plans(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    page_size: Annotated[int, Query(ge=1, le=50)] = 20,
    generation: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    cursor: Annotated[str | None, Query(pattern=_HASH_PATTERN)] = None,
) -> Envelope[BackfillPlansData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if (generation is not None and generation != meta.generation_id) or (
            cursor is not None and generation is None
        ):
            raise HTTPException(status_code=409, detail=_CHANGED)
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        try:
            state = _source_state(None if meta.state == "unavailable" else borrowed)
            if state != "ready" or borrowed is None:
                if cursor is not None:
                    raise HTTPException(status_code=409, detail=_CHANGED)
                data = _empty(state, page_size)
            else:
                data = _page(borrowed, page_size=page_size, cursor=cursor)
        except HTTPException:
            raise
        except Exception as error:
            raise HTTPException(status_code=503, detail=_UNREADABLE) from error
    return Envelope[BackfillPlansData](data=data, serving=meta)


@router.get(
    "/backfill-plans/{plan_hash}",
    response_model=Envelope[BackfillPlanDetailData],
    summary="历史日线回补计划详情",
)
def get_backfill_plan_detail(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    plan_hash: Annotated[str, Path(pattern=_HASH_PATTERN)],
    generation: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
) -> Envelope[BackfillPlanDetailData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if generation is not None and generation != meta.generation_id:
            raise HTTPException(status_code=409, detail=_CHANGED)
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        try:
            state = _source_state(None if meta.state == "unavailable" else borrowed)
            if state != "ready" or borrowed is None:
                data = BackfillPlanDetailData(source_state=state, plan=None, progress=None)
            else:
                catalog, progress = _catalog_progress(borrowed)
                if catalog.total_plan_count == 0:
                    data = BackfillPlanDetailData(
                        source_state="empty", plan=None, progress=progress
                    )
                else:
                    index = _index_row(borrowed, plan_hash)
                    if index is None:
                        raise HTTPException(
                            status_code=404, detail="这份计划不在当前列表中，请返回列表查看。"
                        )
                    sealed = _published_detail(borrowed, plan_hash)
                    if sealed is None:
                        raise ValueError("indexed plan is missing from the published archive")
                    if (
                        sealed.plan_hash != index.plan_hash
                        or sealed.audit_start != index.audit_start
                        or sealed.completed_through != index.completed_through
                        or sealed.cutoff_observed_at != index.cutoff_observed_at
                        or sealed.published_at != index.published_at
                        or len(sealed.missing_dates) != index.missing_day_count
                        or str(sealed.estimate.estimated_seconds) != index.estimated_seconds
                        or sealed.source.mode != index.source_mode
                        or sealed.source.snapshot_label != index.snapshot_label
                        or sealed.source.identity_verified != index.identity_verified
                        or sealed.source.collection_complete_verified
                        != index.collection_complete_verified
                        or sealed.estimate.quota_status != index.quota_status
                        or sealed.executable != index.executable
                    ):
                        raise ValueError("plan detail disagrees with the index")
                    plan = BackfillPlanDetail.model_validate(
                        {
                            **index.model_dump(exclude={"rank"}),
                            "missing_dates": list(sealed.missing_dates),
                            "monthly": [month.model_dump() for month in sealed.monthly],
                            "estimate": sealed.estimate.model_dump(),
                            "source": sealed.source.model_dump(),
                            "gap_count": sealed.gap_count,
                            "coverage_scope": sealed.coverage_scope,
                        }
                    )
                    data = BackfillPlanDetailData(
                        source_state="ready", plan=plan, progress=progress
                    )
        except HTTPException:
            raise
        except Exception as error:
            raise HTTPException(status_code=503, detail=_UNREADABLE) from error
    return Envelope[BackfillPlanDetailData](data=data, serving=meta)
