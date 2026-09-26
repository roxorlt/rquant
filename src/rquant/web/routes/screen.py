"""Manual screening of one immutable, published Serving universe."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from threading import BoundedSemaphore
from typing import Annotated

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import ValidationError

from rquant.llm.compile import compile_screen_plan
from rquant.llm.schemas import RuleCall, ScreenPlan, Stage
from rquant.serving_read_models import (
    PAGE_PROJECTION_CONTRACTS,
    NlScreenPageError,
    paginate_nl_screen_projection,
)
from rquant.web import readers
from rquant.web.envelope import Envelope
from rquant.web.models.screen import (
    ScreenCatalogData,
    ScreenRow,
    ScreenRunData,
    ScreenRunRequest,
    ScreenStep,
)
from rquant.web.screen_catalog import screen_blocks, validate_screen_choices
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import serving_meta

router = APIRouter(prefix="/screen")
_MAX_REQUEST_BYTES = 8_192


@contextmanager
def _screen_slot(gate: BoundedSemaphore) -> Iterator[None]:
    if not gate.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="正在筛选，请稍后再试。")
    try:
        yield
    finally:
        gate.release()


@router.get("/blocks", response_model=Envelope[ScreenCatalogData], summary="选股条件目录")
def get_blocks(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[ScreenCatalogData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        available = False
        dates: list[date] = []
        if borrowed is not None:
            state = readers.table_states(borrowed.cursor).get("nl_screen_universe")
            available = state is not None and state.available
            if available:
                dates = [
                    row[0]
                    for row in borrowed.cursor.execute(
                        "SELECT DISTINCT trade_date FROM nl_screen_universe "
                        "ORDER BY trade_date DESC LIMIT 30"
                    ).fetchall()
                ]
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[ScreenCatalogData](
        data=ScreenCatalogData(blocks=screen_blocks(), dates=dates, available=available),
        serving=meta,
    )


def _empty_run(trade_date: date, status: str) -> ScreenRunData:
    return ScreenRunData(
        trade_date=trade_date,
        status=status,
        base_count=None,
        total=None,
        steps=[],
        rows=[],
        next_cursor=None,
    )


def _number(value: object) -> float | None:
    return None if value is None or bool(pd.isna(value)) else float(value)


@router.post("/run", response_model=Envelope[ScreenRunData], summary="运行选股条件")
def run_screen(
    request: Request,
    response: Response,
    body: ScreenRunRequest,
    _viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> Envelope[ScreenRunData]:
    web = request.app.state.web
    if len(body.model_dump_json().encode("utf-8")) > _MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="条件太多，请减少后重试。")
    try:
        validate_screen_choices(body.conditions)
    except ValueError as error:
        raise HTTPException(status_code=422, detail="请从条件目录选择数据项或板块。") from error
    labels = {block.key: block.label for block in screen_blocks()}
    try:
        plan = ScreenPlan(
            trade_date=body.trade_date.isoformat(),
            stages=[
                Stage(
                    label="条件",
                    rules=[
                        RuleCall(name=condition.key, args=condition.args)
                        for condition in body.conditions
                    ],
                )
            ],
        )
        compiled = compile_screen_plan(plan)
        rule_labels = [labels[condition.key] for condition in body.conditions]
    except KeyError as error:
        raise HTTPException(status_code=422, detail="没有找到这条条件，请重新选择。") from error
    except ValidationError as error:
        raise HTTPException(status_code=422, detail="条件填写有误，请检查后重试。") from error
    except ValueError as error:
        raise HTTPException(status_code=422, detail="没有找到这条条件，请重新选择。") from error

    with _screen_slot(web.screen_gate), web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        if body.cursor is not None and (borrowed is None or meta.state == "unavailable"):
            raise HTTPException(status_code=409, detail="数据已更新，请重新筛选。")
        if borrowed is None or meta.state == "unavailable":
            data = _empty_run(body.trade_date, "unavailable")
        else:
            state = readers.table_states(borrowed.cursor).get("nl_screen_universe")
            if state is None or not state.available:
                if body.cursor is not None:
                    raise HTTPException(status_code=409, detail="数据已更新，请重新筛选。")
                data = _empty_run(body.trade_date, "unavailable")
            else:
                max_rows = PAGE_PROJECTION_CONTRACTS["nl_screen_universe"].max_rows
                universe = borrowed.cursor.execute(
                    'SELECT * FROM nl_screen_universe WHERE trade_date = ? '
                    'ORDER BY trade_date, ts_code LIMIT ?',
                    (body.trade_date, max_rows + 1),
                ).fetchdf()
                if len(universe) > max_rows:
                    raise HTTPException(
                        status_code=503,
                        detail="可筛选股票暂时过多，请稍后重试。",
                    )
                if universe.empty:
                    if body.cursor is not None:
                        raise HTTPException(status_code=409, detail="数据已更新，请重新筛选。")
                    data = _empty_run(body.trade_date, "no_date")
                else:
                    try:
                        page = paginate_nl_screen_projection(
                            universe,
                            generation_id=borrowed.manifest.generation_id,
                            trade_date=body.trade_date.isoformat(),
                            rules=compiled.rules,
                            rule_labels=rule_labels,
                            normalized_plan=compiled.normalized_plan,
                            page_size=body.page_size,
                            signing_key=web.cursor_key,
                            cursor=body.cursor,
                        )
                    except NlScreenPageError as error:
                        raise HTTPException(
                            status_code=409,
                            detail="数据已更新，请重新筛选。",
                        ) from error
                    except ValueError as error:
                        raise HTTPException(
                            status_code=422,
                            detail="当前数据还不支持这个条件，请换一条或稍后重试。",
                        ) from error
                    rows = [
                        ScreenRow(
                            ts_code=str(row["ts_code"]),
                            name=str(row["name"]) if pd.notna(row["name"]) else None,
                            close=_number(row["CLOSE[0]"]),
                            pct_chg=_number(row["PCT_CHG[0]"]),
                        )
                        for row in page.rows.to_dict(orient="records")
                    ]
                    steps = [
                        ScreenStep(label=label, count=count)
                        for label, count in page.diagnostics
                    ]
                    data = ScreenRunData(
                        trade_date=body.trade_date,
                        status="ready",
                        base_count=len(universe),
                        total=steps[-1].count if steps else len(universe),
                        steps=steps,
                        rows=rows,
                        next_cursor=page.next_cursor,
                    )
    return Envelope[ScreenRunData](data=data, serving=meta)
