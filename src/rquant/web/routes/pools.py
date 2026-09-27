"""Published pool canvas: one bounded response from one borrowed Serving generation."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request, Response

from rquant.web import readers
from rquant.web.envelope import Envelope
from rquant.web.labels import PRESET_LABELS
from rquant.web.market import shanghai_trade_date
from rquant.web.models.pools import (
    PoolDefinitionView,
    PoolMember,
    PoolResultView,
    PoolsData,
    PoolStep,
    PublishedPool,
    SavedCanvas,
)
from rquant.web.pool_rule_view import pool_definition_view
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter()

_MAX_CANVASES = 32
_MAX_REFS = 64
_MAX_POOLS = 64
_MAX_MEMBERS = 100
_MAX_STEPS = 32
_MAX_RULE_ROWS = 512
_MAX_RECEIPTS = 512


@dataclass(frozen=True)
class _RunReceipt:
    trade_date: date
    definition_version: str
    hit_count: int
    lineage_complete: bool
    current_definition: bool


def _available(tables: dict[str, readers.TableState], name: str) -> bool:
    table = tables.get(name)
    return table is not None and table.available


def _diagnostic_label(value: str, step_index: int) -> str:
    label = value.strip()
    if label.lower() in {"final", "最终命中"}:
        return "最终命中"
    if not label or re.search(r"[A-Za-z_{}\[\]()/\\]", label):
        return f"筛选步骤 {step_index}"
    return label


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


def _rule_definitions(
    cursor: Any, tables: dict[str, readers.TableState]
) -> tuple[dict[str, PoolDefinitionView], dict[str, str | None]]:
    if not _available(tables, "pool_definition"):
        return {}, {}
    rows = cursor.execute(
        "SELECT pool_name, display_name, description, source_kind, state, reason, "
        "version, depends_on, delay_mode, delay_days, rules_json FROM pool_definition "
        "ORDER BY pool_name LIMIT ?",
        (_MAX_RULE_ROWS,),
    ).fetchall()
    fields = (
        "pool_name",
        "display_name",
        "description",
        "source_kind",
        "state",
        "reason",
        "version",
        "depends_on",
        "delay_mode",
        "delay_days",
        "rules_json",
    )
    return (
        {str(row[0]): pool_definition_view(dict(zip(fields, row, strict=True))) for row in rows},
        {str(row[0]): str(row[6]) if row[6] is not None else None for row in rows},
    )


def _run_receipts(cursor: Any, tables: dict[str, readers.TableState]) -> dict[str, _RunReceipt]:
    if not _available(tables, "screen_run_receipt"):
        return {}
    rows = cursor.execute(
        "SELECT trade_date, preset_name, definition_version, hit_count, "
        "lineage_complete, current_definition FROM screen_run_receipt "
        "ORDER BY preset_name LIMIT ?",
        (_MAX_RECEIPTS,),
    ).fetchall()
    return {
        str(name): _RunReceipt(
            trade_date=trade_date,
            definition_version=str(version),
            hit_count=int(count),
            lineage_complete=bool(lineage_complete),
            current_definition=bool(current_definition),
        )
        for trade_date, name, version, count, lineage_complete, current_definition in rows
    }


def _result_view(
    *,
    receipt: _RunReceipt | None,
    definition: PoolDefinitionView | None,
    definition_version: str | None,
    latest: date | None,
    latest_pool_result_date: date | None,
    today: date,
    member_state: str,
    member_count: int,
    has_result_record: bool,
    results_available: bool,
) -> PoolResultView:
    if not results_available:
        return PoolResultView(
            state="unavailable", status_label="结果暂不可用", trade_date=None, hit_count=None
        )
    if receipt is None:
        state = "unverified" if has_result_record else "not_run"
        return PoolResultView(
            state=state,
            status_label="结果版本待确认" if has_result_record else "尚无选股结果",
            trade_date=None,
            hit_count=None,
        )
    unverified = PoolResultView(
        state="unverified",
        status_label="结果版本待确认",
        trade_date=receipt.trade_date if receipt.trade_date == latest_pool_result_date else None,
        hit_count=None,
    )
    if (
        latest is None
        or receipt.trade_date > latest
        or (latest_pool_result_date is not None and receipt.trade_date < latest_pool_result_date)
        or not receipt.lineage_complete
    ):
        return unverified
    if member_state == "current" and receipt.trade_date != latest:
        return unverified
    if receipt.trade_date == latest and receipt.hit_count != member_count:
        return unverified
    if definition is None or definition.state != "available" or definition_version is None:
        return unverified
    if receipt.definition_version != definition_version:
        return PoolResultView(
            state="rules_changed",
            status_label="规则已更新，等待下次选股",
            trade_date=receipt.trade_date,
            hit_count=receipt.hit_count,
        )
    if not receipt.current_definition:
        return unverified
    current = receipt.trade_date == latest
    return PoolResultView(
        state="current_rules" if current else "older_rules",
        status_label="结果已按当前规则更新" if current else "上次结果与当前规则一致",
        trade_date=receipt.trade_date,
        hit_count=receipt.hit_count,
        zero_hit_label=(
            "今天没有符合条件的股票"
            if receipt.hit_count == 0 and current and receipt.trade_date == today
            else "该交易日没有符合条件的股票"
            if receipt.hit_count == 0 and current
            else None
        ),
    )


def _selected_keys(ordered: list[str], canvases: list[SavedCanvas], known: set[str]) -> list[str]:
    selected: set[str] = set()
    for canvas in canvases:
        if len(selected) >= _MAX_POOLS:
            break
        if not canvas.pool_keys:
            continue
        first_published = next((key for key in canvas.pool_keys if key in known), None)
        selected.add(first_published or canvas.pool_keys[0])
    for key in ordered:
        if len(selected) >= _MAX_POOLS:
            break
        selected.add(key)
    return [key for key in ordered if key in selected]


def build_pools(borrowed: BorrowedGeneration | None, *, today: date) -> PoolsData:
    if borrowed is None:
        return PoolsData(
            state="unavailable",
            latest_trade_date=None,
            definitions_available=False,
            rules_available=False,
            canvases=[],
            canvases_truncated=False,
            pools=[],
            pools_truncated=False,
        )
    cursor = borrowed.cursor
    tables = readers.table_states(cursor)
    definitions_available = _available(tables, "canvas_definition")
    canvases, canvases_truncated = _canvases(cursor, tables)
    rules_available = _available(tables, "pool_definition")
    rule_definitions, rule_versions = _rule_definitions(cursor, tables)
    receipts = _run_receipts(cursor, tables)
    results_available = all(
        _available(tables, name) for name in ("canvas_latest_trade_date", "canvas_hit")
    )
    latest = readers.screen_latest(cursor, tables) if results_available else None

    bounds: dict[str, date] = {}
    if latest is not None and _available(tables, "screen_bounds"):
        bounds = {
            str(key): max_date
            for key, max_date in cursor.execute(
                "SELECT preset_name, max_date FROM screen_bounds ORDER BY preset_name LIMIT 257"
            ).fetchall()
        }
    hits: dict[str, list[PoolMember]] = defaultdict(list)
    counts: dict[str, int] = defaultdict(int)
    if latest is not None:
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
    if latest is not None and _available(tables, "canvas_diagnostic"):
        for key, step_index, label, count in cursor.execute(
            "SELECT preset_name, step_index, rule_label, remaining_count FROM canvas_diagnostic "
            "WHERE trade_date = ? ORDER BY preset_name, step_index LIMIT 20001",
            (latest,),
        ).fetchall():
            pool_key = str(key)
            step_counts[pool_key] += 1
            if len(steps[pool_key]) < _MAX_STEPS:
                steps[pool_key].append(
                    PoolStep(
                        step_index=int(step_index),
                        label=_diagnostic_label(str(label), int(step_index)),
                        count=int(count),
                    )
                )

    known = set(bounds) | set(counts) | set(step_counts) | set(receipts)
    referenced = {key for canvas in canvases for key in canvas.pool_keys}
    ordered = sorted(
        known | referenced | set(rule_definitions),
        key=lambda key: (key not in referenced, key not in counts, key not in known, key),
    )
    selected_keys = _selected_keys(ordered, canvases, known)
    unknown_names = {
        key: f"选股池 {index}"
        for index, key in enumerate(
            sorted(
                key
                for key in known | referenced | set(rule_definitions)
                if key not in PRESET_LABELS and not key.startswith("user/")
            ),
            start=1,
        )
    }
    pools: list[PublishedPool] = []
    for key in selected_keys:
        pool_steps = steps[key]
        if len(pool_steps) == 1 and pool_steps[0].label.lower() in {"final", "最终命中"}:
            pool_steps = [
                PoolStep(
                    step_index=pool_steps[0].step_index,
                    label="最终命中",
                    count=pool_steps[0].count,
                )
            ]
        receipt = receipts.get(key)
        latest_pool_result_date = bounds.get(key)
        if receipt is not None:
            latest_pool_result_date = max(
                latest_pool_result_date or receipt.trade_date, receipt.trade_date
            )
        verified_zero = (
            receipt is not None
            and receipt.lineage_complete
            and receipt.trade_date == latest
            and receipt.hit_count == 0
            and counts[key] == 0
        )
        if verified_zero:
            pool_steps = []
        if not results_available:
            state = "unavailable"
            trade_date = None
            member_count = None
        elif latest is None:
            state = "no_data"
            trade_date = None
            member_count = None
        elif key not in known:
            state = "unpublished" if _available(tables, "screen_bounds") else "unavailable"
            trade_date = None
            member_count = None
        elif verified_zero:
            state = "current"
            trade_date = latest
            member_count = 0
        elif (
            latest_pool_result_date is not None
            and latest_pool_result_date < latest
            and counts[key] == 0
            and step_counts[key] == 0
        ):
            state = "older"
            trade_date = latest_pool_result_date
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
                name=rule_definitions[key].name
                if key in rule_definitions
                else PRESET_LABELS.get(key)
                or (key.removeprefix("user/") if key.startswith("user/") else unknown_names[key]),
                state=state,
                trade_date=trade_date,
                member_count=member_count,
                steps=pool_steps if state == "current" else [],
                steps_truncated=state == "current" and step_counts[key] > _MAX_STEPS,
                members=hits[key] if state == "current" else [],
                members_truncated=counts[key] > _MAX_MEMBERS,
                definition=rule_definitions.get(key),
                result=_result_view(
                    receipt=receipt,
                    definition=rule_definitions.get(key),
                    definition_version=rule_versions.get(key),
                    latest=latest,
                    latest_pool_result_date=latest_pool_result_date,
                    today=today,
                    member_state=state,
                    member_count=counts[key],
                    has_result_record=key in bounds or counts[key] > 0 or step_counts[key] > 0,
                    results_available=results_available,
                ),
            )
        )
    return PoolsData(
        state=(
            "unavailable"
            if not results_available
            else "no_data"
            if latest is None or not (known or canvases)
            else "ready"
        ),
        latest_trade_date=latest,
        definitions_available=definitions_available,
        rules_available=rules_available,
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
        data = build_pools(borrowed, today=shanghai_trade_date(now))
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[PoolsData](data=data, serving=meta)
