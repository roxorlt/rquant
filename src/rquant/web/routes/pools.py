"""Published pool canvas: one bounded response from one borrowed Serving generation."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request, Response

from rquant.web import readers
from rquant.web.envelope import Envelope
from rquant.web.labels import PRESET_LABELS
from rquant.web.models.pools import PoolMember, PoolsData, PoolStep, PublishedPool, SavedCanvas
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter()

_MAX_CANVASES = 32
_MAX_REFS = 64
_MAX_POOLS = 64
_MAX_MEMBERS = 100
_MAX_STEPS = 32


def _available(tables: dict[str, readers.TableState], name: str) -> bool:
    table = tables.get(name)
    return table is not None and table.available


def _refs(value: str) -> list[str]:
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return []
    if not isinstance(decoded, list):
        return []
    return list(dict.fromkeys(item for item in decoded if isinstance(item, str) and item))


def _canvases(cursor: Any, tables: dict[str, readers.TableState]) -> tuple[list[SavedCanvas], bool]:
    if not _available(tables, "canvas_definition"):
        return [], False
    rows = cursor.execute(
        "SELECT name, description, pool_refs_json FROM canvas_definition ORDER BY name LIMIT ?",
        (_MAX_CANVASES + 1,),
    ).fetchall()
    return [
        SavedCanvas(
            name=str(name),
            description=str(description or ""),
            pool_keys=_refs(str(refs))[:_MAX_REFS],
            refs_truncated=len(_refs(str(refs))) > _MAX_REFS,
        )
        for name, description, refs in rows[:_MAX_CANVASES]
    ], len(rows) > _MAX_CANVASES


def build_pools(borrowed: BorrowedGeneration | None) -> PoolsData:
    if borrowed is None:
        return PoolsData(
            state="unavailable",
            latest_trade_date=None,
            definitions_available=False,
            canvases=[],
            canvases_truncated=False,
            pools=[],
            pools_truncated=False,
        )
    cursor = borrowed.cursor
    tables = readers.table_states(cursor)
    definitions_available = _available(tables, "canvas_definition")
    canvases, canvases_truncated = _canvases(cursor, tables)
    results_available = all(
        _available(tables, name) for name in ("canvas_latest_trade_date", "canvas_hit")
    )
    latest = readers.screen_latest(cursor, tables)
    if not results_available or latest is None:
        return PoolsData(
            state="unavailable" if not results_available else "no_data",
            latest_trade_date=latest,
            definitions_available=definitions_available,
            canvases=canvases,
            canvases_truncated=canvases_truncated,
            pools=[],
            pools_truncated=False,
        )

    bounds: dict[str, date] = {}
    if _available(tables, "screen_bounds"):
        bounds = {
            str(key): max_date
            for key, max_date in cursor.execute(
                "SELECT preset_name, max_date FROM screen_bounds ORDER BY preset_name LIMIT 257"
            ).fetchall()
        }
    hits: dict[str, list[PoolMember]] = defaultdict(list)
    counts: dict[str, int] = defaultdict(int)
    for key, code, row_json in cursor.execute(
        "SELECT preset_name, ts_code, row_json FROM canvas_hit "
        "WHERE trade_date = ? ORDER BY preset_name, ts_code LIMIT 20001",
        (latest,),
    ).fetchall():
        pool_key = str(key)
        counts[pool_key] += 1
        if len(hits[pool_key]) >= _MAX_MEMBERS:
            continue
        try:
            payload = json.loads(row_json) if row_json else {}
        except (TypeError, ValueError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        name = payload.get("name")
        hits[pool_key].append(
            PoolMember(
                code=str(code),
                name=name if isinstance(name, str) and name else None,
                close=readers._number(payload.get("close")),
                pct_chg=readers._number(payload.get("pct_chg")),
            )
        )

    steps: dict[str, list[PoolStep]] = defaultdict(list)
    step_counts: dict[str, int] = defaultdict(int)
    if _available(tables, "canvas_diagnostic"):
        for key, step_index, label, count in cursor.execute(
            "SELECT preset_name, step_index, rule_label, remaining_count FROM canvas_diagnostic "
            "WHERE trade_date = ? ORDER BY preset_name, step_index LIMIT 20001",
            (latest,),
        ).fetchall():
            pool_key = str(key)
            step_counts[pool_key] += 1
            if len(steps[pool_key]) < _MAX_STEPS:
                steps[pool_key].append(
                    PoolStep(step_index=int(step_index), label=str(label), count=int(count))
                )

    known = set(bounds) | set(counts) | set(step_counts)
    referenced = {key for canvas in canvases for key in canvas.pool_keys}
    ordered = sorted(
        known | referenced, key=lambda key: (key not in referenced, key not in counts, key)
    )
    pools: list[PublishedPool] = []
    for key in ordered[:_MAX_POOLS]:
        pool_steps = steps[key]
        if len(pool_steps) == 1 and pool_steps[0].label.lower() in {"final", "最终命中"}:
            pool_steps = [
                PoolStep(
                    step_index=pool_steps[0].step_index,
                    label="最终命中",
                    count=pool_steps[0].count,
                )
            ]
        if key not in known:
            state = "missing" if _available(tables, "screen_bounds") else "unavailable"
            trade_date = None
            member_count = None
        elif (
            bounds.get(key) is not None
            and bounds[key] < latest
            and counts[key] == 0
            and step_counts[key] == 0
        ):
            state = "older"
            trade_date = bounds[key]
            member_count = None
        elif counts[key] > 0 or (pool_steps and pool_steps[-1].count == 0):
            state = "current"
            trade_date = latest
            member_count = counts[key]
        else:
            state = "no_data"
            trade_date = latest if step_counts[key] else bounds.get(key)
            member_count = None
        pools.append(
            PublishedPool(
                key=key,
                name=PRESET_LABELS.get(key, "选股池"),
                state=state,
                trade_date=trade_date,
                member_count=member_count,
                steps=pool_steps if state == "current" else [],
                steps_truncated=state == "current" and step_counts[key] > _MAX_STEPS,
                members=hits[key] if state == "current" else [],
                members_truncated=counts[key] > _MAX_MEMBERS,
            )
        )
    return PoolsData(
        state="ready" if ordered or canvases else "no_data",
        latest_trade_date=latest,
        definitions_available=definitions_available,
        canvases=canvases,
        canvases_truncated=canvases_truncated,
        pools=pools,
        pools_truncated=len(ordered) > _MAX_POOLS,
    )


@router.get("/pools", response_model=Envelope[PoolsData], summary="已发布池子画布")
def get_pools(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[PoolsData]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=now,
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        data = build_pools(borrowed)
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[PoolsData](data=data, serving=meta)
