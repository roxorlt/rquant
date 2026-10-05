"""Read and validate bounded price rules and watchlist facts from one borrowed generation."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Literal

from rquant.manual_watchlist import MAX_ACTIVE_MEMBERS
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc
from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistProjectionRow,
    validate_manual_watchlist_projections,
)
from rquant.serving_price_alert_rule_projection import (
    PriceAlertRuleProjectionRow,
    validate_price_alert_rule_projections,
)
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.web.models.price_alert_rules import PriceAlertRuleItem
from rquant.web.serving import BorrowedGeneration

_TABLES = (
    "manual_watchlist",
    "manual_watchlist_state",
    "price_alert_rule",
    "price_alert_rule_state",
)
PRIORITY_LABELS = {"P0": "紧急", "P1": "重要", "P2": "普通", "P3": "提示"}


class PriceAlertRuleView(RuntimeContractModel):
    availability: Literal["ready", "not_activated", "unavailable"]
    available_at: AwareUtcDatetime | None = None
    members_ready: bool = False
    rules: tuple[PriceAlertRuleProjectionRow, ...] = ()
    members: tuple[ManualWatchlistProjectionRow, ...] = ()


def read_price_alert_rules(
    borrowed: BorrowedGeneration, *, owner_id: str, now: datetime
) -> PriceAlertRuleView:
    manifest = borrowed.manifest
    source = manifest.source_generations.get("signals")
    watermark = next(
        (value for value in manifest.watermarks if value.dataset_id == "signals"), None
    )
    if source is None or watermark is None or watermark.generation_id != source:
        raise ValueError("price rules lack their signals source")
    marks = borrowed.cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, "
        "owner_generation_id, available_at "
        "FROM projection_status WHERE table_name IN (?, ?, ?, ?) ORDER BY table_name LIMIT 5",
        _TABLES,
    ).fetchall()
    if tuple(mark[0] for mark in marks) != _TABLES:
        raise ValueError("price rules and watchlist status are incomplete")
    projections: dict[str, ServingProjectionPayload] = {}
    for name, available, count, owner, generation, at in marks:
        contract = PAGE_PROJECTION_CONTRACTS[name]
        if (
            type(available) is not bool
            or type(count) is not int
            or owner != "signals"
            or count != manifest.row_counts.get(name)
            or not 0 <= count <= contract.max_rows
        ):
            raise ValueError("price rule manifest and status differ")
        if not available:
            if count or generation is not None or at is not None:
                raise ValueError("unpublished price rule projection has facts")
            continue
        if (
            generation != source
            or not isinstance(at, datetime)
            or normalize_aware_utc(at) > manifest.built_at
        ):
            raise ValueError("price rule source identity or time differs")
        values = borrowed.cursor.execute(
            f"SELECT {', '.join(contract.column_names)} FROM {name} "
            f"ORDER BY {', '.join(contract.sort_keys)} LIMIT ?",
            (contract.max_rows + 1,),
        ).fetchall()
        if len(values) != count:
            raise ValueError("price rule physical row count differs")
        projections[name] = ServingProjectionPayload(
            table_name=name,
            available_at=at,
            rows=tuple(
                dict(
                    zip(
                        contract.column_names,
                        (
                            normalize_aware_utc(value).isoformat()
                            if isinstance(value, datetime)
                            else value
                            for value in row
                        ),
                        strict=True,
                    )
                )
                for row in values
            ),
        )
    validate_price_alert_rule_projections(projections)
    validate_manual_watchlist_projections(projections)
    state = projections.get("price_alert_rule_state")
    if state is None or state.rows[0]["state"] == "unavailable":
        return PriceAlertRuleView(availability="unavailable")
    if state.rows[0]["state"] == "not_activated":
        return PriceAlertRuleView(availability="not_activated")
    rules = tuple(
        PriceAlertRuleProjectionRow.model_validate(row)
        for row in projections["price_alert_rule"].rows
        if row["owner_id"] == owner_id
    )
    member_state = projections.get("manual_watchlist_state")
    ready = member_state is not None and member_state.rows[0]["state"] == "ready"
    all_members = (
        tuple(
            ManualWatchlistProjectionRow.model_validate(row)
            for row in projections["manual_watchlist"].rows
        )
        if ready
        else ()
    )
    active_counts = Counter(
        row.owner_id
        for row in all_members
        if not row.deleted and (row.expires_at is None or row.expires_at > now)
    )
    if any(count > MAX_ACTIVE_MEMBERS for count in active_counts.values()):
        raise ValueError("price rule scope exceeds owner watchlist capacity")
    return PriceAlertRuleView(
        availability="ready",
        available_at=state.available_at,
        members_ready=ready,
        rules=rules,
        members=tuple(row for row in all_members if row.owner_id == owner_id),
    )


def rule_item(
    row: PriceAlertRuleProjectionRow, view: PriceAlertRuleView, *, now: datetime
) -> PriceAlertRuleItem:
    if row.deleted:
        raise ValueError("tombstone is not a live rule item")
    member = next((value for value in view.members if value.ts_code == row.ts_code), None)
    status = "bound"
    if not view.members_ready:
        status = "unavailable"
    elif member is None or member.deleted:
        status = "removed"
    elif member.expires_at is not None and member.expires_at <= now:
        status = "expired"
    elif member.version != row.membership_version:
        status = "changed"
    elif not row.enabled:
        status = "disabled"
    return PriceAlertRuleItem.model_validate(
        {
            **{
                name: getattr(row, name)
                for name in (
                    "rule_id",
                    "version",
                    "ts_code",
                    "membership_version",
                    "name",
                    "priority",
                    "enabled",
                    "comparison",
                    "threshold",
                    "valid_from",
                    "valid_until",
                    "updated_at",
                )
            },
            "priority_label": PRIORITY_LABELS[row.priority],
            "scope_status": status,
            "scope_message": {
                "bound": "行情评估接通后才会提醒。",
                "disabled": "规则已停用。",
                "removed": "已移出盯盘，可停用或删除规则。",
                "expired": "盯盘已到期，可停用或删除规则。",
                "changed": "盯盘已更新，请重新选择股票。",
                "unavailable": "盯盘名单暂不可用，请稍后重试。",
            }[status],
        }
    )
