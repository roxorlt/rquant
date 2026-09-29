"""Published pool canvas: one bounded response from one borrowed Serving generation."""

from __future__ import annotations

import json
import math
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
_MAX_MEMBERSHIP_ROWS = 4_608
_MAX_RETURN_ROWS = 4_096


@dataclass(frozen=True)
class _RunReceipt:
    trade_date: date
    definition_version: str
    result_version: str | None
    hit_count: int
    lineage_complete: bool
    current_definition: bool


@dataclass(frozen=True)
class _MembershipRow:
    trade_date: date | None
    result_version: str | None
    row_kind: str
    code: str
    status: str
    entry_trade_date: date | None
    entry_close: float | None
    entry_result_version: str | None
    unknown_reason: str | None


@dataclass(frozen=True)
class _EntryEvidence:
    trade_date: date | None = None
    close: float | None = None


@dataclass(frozen=True)
class _ReturnRow:
    trade_date: date
    result_version: str
    code: str
    entry_trade_date: date
    entry_result_version: str
    gain_pct: float | None
    entry_line_price: float | None


@dataclass(frozen=True)
class _RankClaim:
    position: int
    score: float
    result_version: str


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
    cursor.execute("SELECT * FROM pool_definition LIMIT 0")
    has_ranking = any(column[0] == "ranking_json" for column in cursor.description)
    ranking_column = "ranking_json" if has_ranking else "NULL AS ranking_json"
    rows = cursor.execute(
        "SELECT pool_name, display_name, description, source_kind, state, reason, "
        "version, depends_on, delay_mode, delay_days, rules_json, "
        f"{ranking_column} FROM pool_definition ORDER BY pool_name LIMIT ?",
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
        "ranking_json",
    )
    return (
        {str(row[0]): pool_definition_view(dict(zip(fields, row, strict=True))) for row in rows},
        {str(row[0]): str(row[6]) if row[6] is not None else None for row in rows},
    )


def _run_receipts(cursor: Any, tables: dict[str, readers.TableState]) -> dict[str, _RunReceipt]:
    if not _available(tables, "screen_run_receipt"):
        return {}
    rows = cursor.execute(
        "SELECT trade_date, preset_name, definition_version, result_version, hit_count, "
        "lineage_complete, current_definition FROM screen_run_receipt "
        "ORDER BY preset_name LIMIT ?",
        (_MAX_RECEIPTS,),
    ).fetchall()
    receipts: dict[str, _RunReceipt] = {}
    for day, name, version, result_version, count, lineage, current in rows:
        receipts[str(name)] = _RunReceipt(
            trade_date=day,
            definition_version=str(version),
            result_version=None if result_version is None else str(result_version),
            hit_count=int(count),
            lineage_complete=bool(lineage),
            current_definition=bool(current),
        )
    return receipts


def _membership_rows(
    cursor: Any, tables: dict[str, readers.TableState]
) -> dict[str, list[_MembershipRow]]:
    if not _available(tables, "pool_membership"):
        return {}
    rows = cursor.execute(
        "SELECT pool_name, trade_date, result_version, row_kind, ts_code, status, "
        "entry_trade_date, entry_close, entry_result_version, unknown_reason "
        "FROM pool_membership ORDER BY pool_name, row_kind, ts_code LIMIT ?",
        (_MAX_MEMBERSHIP_ROWS + 1,),
    ).fetchall()
    if len(rows) > _MAX_MEMBERSHIP_ROWS:
        return {}
    by_pool: dict[str, list[_MembershipRow]] = defaultdict(list)
    for (
        name,
        trade_date,
        result_version,
        row_kind,
        code,
        status,
        entry_trade_date,
        entry_close,
        entry_result_version,
        unknown_reason,
    ) in rows:
        by_pool[str(name)].append(
            _MembershipRow(
                trade_date=trade_date,
                result_version=None if result_version is None else str(result_version),
                row_kind=str(row_kind),
                code=str(code),
                status=str(status),
                entry_trade_date=entry_trade_date,
                entry_close=readers._number(entry_close),
                entry_result_version=(
                    None if entry_result_version is None else str(entry_result_version)
                ),
                unknown_reason=None if unknown_reason is None else str(unknown_reason),
            )
        )
    return by_pool


def _return_rows(cursor: Any, tables: dict[str, readers.TableState]) -> dict[str, list[_ReturnRow]]:
    if not _available(tables, "pool_member_return"):
        return {}
    rows = cursor.execute(
        "SELECT pool_name, trade_date, result_version, ts_code, entry_trade_date, "
        "entry_result_version, gain_pct, entry_line_price FROM pool_member_return "
        "ORDER BY pool_name, ts_code LIMIT ?",
        (_MAX_RETURN_ROWS + 1,),
    ).fetchall()
    if len(rows) > _MAX_RETURN_ROWS:
        return {}
    by_pool: dict[str, list[_ReturnRow]] = defaultdict(list)
    for name, day, version, code, entry_day, entry_version, gain, line in rows:
        by_pool[str(name)].append(
            _ReturnRow(
                trade_date=day,
                result_version=str(version),
                code=str(code),
                entry_trade_date=entry_day,
                entry_result_version=str(entry_version),
                gain_pct=readers._number(gain),
                entry_line_price=readers._number(line),
            )
        )
    return by_pool


def _rank_claim(payload: dict[str, object]) -> _RankClaim | None:
    position = payload.get("rank_position")
    score = payload.get("ranking_score")
    result_version = payload.get("rank_result_version")
    if (
        type(position) is not int
        or position < 1
        or isinstance(score, bool)
        or not isinstance(score, (int, float))
        or not math.isfinite(score)
        or not 0 <= score <= 100
        or not isinstance(result_version, str)
    ):
        return None
    return _RankClaim(position=position, score=float(score), result_version=result_version)


def _ranked_members(
    members: list[PoolMember],
    claims: dict[str, _RankClaim | None],
    *,
    receipt: _RunReceipt | None,
    result: PoolResultView,
    latest: date | None,
    complete_group: bool,
) -> list[PoolMember] | None:
    if (
        receipt is None
        or receipt.result_version is None
        or result.state != "current_rules"
        or latest is None
        or receipt.trade_date != latest
        or not complete_group
        or len(members) != receipt.hit_count
        or len(claims) != receipt.hit_count
        or len({member.code for member in members}) != receipt.hit_count
    ):
        return None
    positions: set[int] = set()
    ranked: list[PoolMember] = []
    for member in members:
        claim = claims.get(member.code)
        if (
            claim is None
            or claim.result_version != receipt.result_version
            or claim.position in positions
        ):
            return None
        positions.add(claim.position)
        ranked.append(
            member.model_copy(
                update={"rank_position": claim.position, "ranking_score": claim.score}
            )
        )
    if positions != set(range(1, receipt.hit_count + 1)):
        return None
    return sorted(ranked, key=lambda member: member.rank_position or 0)


def _entry_evidence(row: _MembershipRow, *, result_date: date) -> _EntryEvidence:
    if (
        row.entry_trade_date is None
        or row.entry_trade_date > result_date
        or row.entry_result_version is None
        or re.fullmatch(r"[0-9a-f]{64}", row.entry_result_version) is None
    ):
        return _EntryEvidence()
    if row.unknown_reason == "entry_price_missing" and row.entry_close is None:
        return _EntryEvidence(trade_date=row.entry_trade_date)
    if (
        row.unknown_reason is not None
        or row.entry_close is None
        or not math.isfinite(row.entry_close)
        or row.entry_close <= 0
    ):
        return _EntryEvidence()
    return _EntryEvidence(trade_date=row.entry_trade_date, close=row.entry_close)


def _member_entries(
    *,
    rows: list[_MembershipRow] | None,
    receipt: _RunReceipt | None,
    result: PoolResultView,
    latest: date | None,
    member_codes: set[str],
    member_count: int,
) -> dict[str, _EntryEvidence]:
    if (
        rows is None
        or receipt is None
        or receipt.result_version is None
        or result.state != "current_rules"
        or latest is None
        or receipt.trade_date != latest
        or len(member_codes) != member_count
        or len(rows) != member_count + 1
    ):
        return {}
    status_rows = [row for row in rows if row.row_kind == "status"]
    if len(status_rows) != 1:
        return {}
    status = status_rows[0]
    if (
        status.trade_date != receipt.trade_date
        or status.result_version != receipt.result_version
        or status.code != ""
        or status.status != "verified"
        or status.entry_trade_date is not None
        or status.entry_close is not None
        or status.entry_result_version is not None
        or status.unknown_reason is not None
    ):
        return {}
    members = [row for row in rows if row.row_kind == "member"]
    if (
        len(members) != member_count
        or {row.code for row in members} != member_codes
        or any(
            row.trade_date != receipt.trade_date
            or row.result_version != receipt.result_version
            or row.status != "verified"
            for row in members
        )
    ):
        return {}
    return {row.code: _entry_evidence(row, result_date=receipt.trade_date) for row in members}


def _member_returns(
    *,
    rows: list[_ReturnRow] | None,
    receipt: _RunReceipt | None,
    membership: list[_MembershipRow] | None,
    entries: dict[str, _EntryEvidence],
    member_codes: set[str],
    member_count: int,
) -> dict[str, _ReturnRow]:
    if (
        not rows
        or receipt is None
        or receipt.result_version is None
        or membership is None
        or len(entries) != member_count
        or len(member_codes) != member_count
    ):
        return {}
    members = {row.code: row for row in membership if row.row_kind == "member"}
    if len(members) != member_count:
        return {}
    verified: dict[str, _ReturnRow] = {}
    for row in rows:
        entry = entries.get(row.code)
        member = members.get(row.code)
        if (
            row.code in verified
            or row.code not in member_codes
            or row.trade_date != receipt.trade_date
            or row.result_version != receipt.result_version
            or entry is None
            or entry.trade_date is None
            or entry.close is None
            or member is None
            or member.entry_result_version is None
            or row.entry_trade_date != entry.trade_date
            or row.entry_result_version != member.entry_result_version
            or row.gain_pct is None
            or not math.isfinite(row.gain_pct)
            or (
                row.entry_line_price is not None
                and (not math.isfinite(row.entry_line_price) or row.entry_line_price != entry.close)
            )
        ):
            return {}
        verified[row.code] = row
    return verified


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
    memberships = _membership_rows(cursor, tables)
    returns = _return_rows(cursor, tables)
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
    rank_claims: dict[str, dict[str, _RankClaim | None]] = defaultdict(dict)
    hit_codes: dict[str, set[str]] = defaultdict(set)
    counts: dict[str, int] = defaultdict(int)
    complete_hits = True
    if latest is not None:
        hit_rows = cursor.execute(
            "SELECT preset_name, ts_code, row_json FROM canvas_hit "
            "WHERE trade_date = ? ORDER BY preset_name, ts_code LIMIT 20001",
            (latest,),
        ).fetchall()
        complete_hits = len(hit_rows) <= 20_000
        for key, code, row_json in hit_rows:
            pool_key = str(key)
            counts[pool_key] += 1
            hit_codes[pool_key].add(str(code))
            try:
                payload = json.loads(row_json) if row_json else {}
            except (TypeError, ValueError):
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            name = payload.get("name")
            rank_claims[pool_key][str(code)] = _rank_claim(payload)
            hits[pool_key].append(
                PoolMember(
                    code=str(code),
                    name=name if isinstance(name, str) and name else None,
                    close=readers._number(payload.get("close")),
                    pct_chg=readers._number(payload.get("pct_chg")),
                    entry_trade_date=None,
                    entry_close=None,
                    gain_pct=None,
                    gain_through_date=None,
                    entry_line_price=None,
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
        result_view = _result_view(
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
        )
        entries = _member_entries(
            rows=memberships.get(key),
            receipt=receipt,
            result=result_view,
            latest=latest,
            member_codes=hit_codes[key],
            member_count=counts[key],
        )
        member_returns = _member_returns(
            rows=returns.get(key),
            receipt=receipt,
            membership=memberships.get(key),
            entries=entries,
            member_codes=hit_codes[key],
            member_count=counts[key],
        )
        gains = [row.gain_pct for row in member_returns.values() if row.gain_pct is not None]
        try:
            sample_avg = math.fsum(gains) / len(gains) if gains else None
        except OverflowError:
            sample_avg = None
        if sample_avg is not None and not math.isfinite(sample_avg):
            sample_avg = None
        if gains and sample_avg is None:
            member_returns = {}
        visible_members = [
            member.model_copy(
                update={
                    "entry_trade_date": entries[member.code].trade_date,
                    "entry_close": entries[member.code].close,
                    "gain_pct": member_returns[member.code].gain_pct
                    if member.code in member_returns
                    else None,
                    "gain_through_date": member_returns[member.code].trade_date
                    if member.code in member_returns
                    else None,
                    "entry_line_price": member_returns[member.code].entry_line_price
                    if member.code in member_returns
                    else None,
                }
            )
            if member.code in entries
            else member
            for member in hits[key]
        ]
        ranked_members = _ranked_members(
            visible_members,
            rank_claims[key],
            receipt=receipt,
            result=result_view,
            latest=latest,
            complete_group=complete_hits and counts[key] == len(hits[key]),
        )
        display_members = ranked_members if ranked_members is not None else visible_members
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
                gain_verified_count=len(member_returns),
                gain_sample_avg_pct=sample_avg if member_returns else None,
                steps=pool_steps if state == "current" else [],
                steps_truncated=state == "current" and step_counts[key] > _MAX_STEPS,
                members=display_members[:_MAX_MEMBERS] if state == "current" else [],
                members_truncated=counts[key] > _MAX_MEMBERS,
                definition=rule_definitions.get(key),
                result=result_view,
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
