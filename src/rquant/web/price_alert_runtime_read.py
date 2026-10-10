"""One verified Serving generation provides all owner-filtered runtime facts."""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from rquant.price_alert_route import PriceAlertBusRoutedRecord
from rquant.price_alert_runtime_projection import (
    PRICE_RUNTIME_TABLES,
    PriceAlertAttemptFact,
    PriceAlertRuntimeState,
    validate_price_runtime_projections,
)
from rquant.price_alert_runtime_store import PriceRuntimeRuleFact
from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.web.models.price_alert_runtime import (
    PriceAlertNotificationFact,
    PriceAlertRecentEvent,
    PriceAlertRecentEventsData,
    PriceAlertRuntimeData,
    PriceAlertRuntimeItem,
)
from rquant.web.price_alert_read import read_price_alert_rules, rule_item
from rquant.web.serving import BorrowedGeneration


class PriceAlertRuntimeDrift(ValueError):  # noqa: N818 - Keep the typed v1 outcome name.
    """Current rule/member facts differ from the evaluated or applied authority."""


def _read_domain(borrowed: BorrowedGeneration) -> dict[str, ServingProjectionPayload] | None:
    source = borrowed.manifest.source_generations.get("signals")
    if source is None:
        return None
    result = {}
    for name in PRICE_RUNTIME_TABLES:
        status = borrowed.cursor.execute(
            "SELECT available,row_count,owner_dataset_id,owner_generation_id,available_at "
            "FROM projection_status WHERE table_name=?",
            (name,),
        ).fetchall()
        if len(status) != 1:
            raise ValueError("price runtime projection status differs")
        available, count, owner, generation, at = status[0]
        if not available:
            if count or generation is not None or at is not None:
                raise ValueError("unavailable price runtime projection has facts")
            continue
        contract = PAGE_PROJECTION_CONTRACTS[name]
        if (
            owner != "signals"
            or generation != source
            or count != borrowed.manifest.row_counts.get(name)
            or not 0 <= count <= contract.max_rows
        ):
            raise ValueError("price runtime projection belongs to another source")
        rows = borrowed.cursor.execute(
            f"SELECT {','.join(contract.column_names)} FROM {name} "
            f"ORDER BY {','.join(contract.sort_keys)} LIMIT ?",
            (contract.max_rows + 1,),
        ).fetchall()
        if len(rows) != count or normalize_aware_utc(at) > borrowed.manifest.built_at:
            raise ValueError("price runtime actual rows or time differ")
        result[name] = ServingProjectionPayload(
            table_name=name,
            available_at=at,
            rows=tuple(dict(zip(contract.column_names, row, strict=True)) for row in rows),
        )
    if not result:
        return None
    validate_price_runtime_projections(result)
    return result


def _notification(value: PriceAlertAttemptFact) -> PriceAlertNotificationFact:
    state, label, message = "pending", "等待处理", "提醒已排队。"
    if value.unknown:
        state, label, message = "unknown", "结果未知", "不会自动重发。"
    elif value.cancellation is not None:
        state, label, message = "cancelled", "已取消", "设置已更新，提醒未准入发送。"
    elif value.attempts and value.attempts[-1].succeeded:
        recorded = value.attempts[-1].shadow
        state, label, message = (
            ("recorded", "仅记录", "已记录提醒，没有调用通知通道。")
            if recorded
            else ("accepted", "已提交", "通知通道已接收请求。")
        )
    elif value.status.value == "retry":
        state, label, message = "rejected", "待重试", "通知通道拒绝了本次请求。"
    elif value.status.value == "expired":
        state, label, message = "expired", "已过期", "提醒已超过有效时间。"
    elif value.admission is not None and value.status.value == "leased":
        state, label, message = "admitted", "已准入发送", "可能仍会发送；尚无通知结果。"
    elif value.status.value == "dead_letter":
        state, label, message = "rejected", "未提交", "提醒未完成，请检查通知设置。"
    return PriceAlertNotificationFact(
        channel=value.target.channel.value,
        state=state,
        label=label,
        message=message,
        updated_at=value.updated_at,
    )


def read_price_alert_runtime(
    borrowed: BorrowedGeneration, *, owner_id: str, now: datetime
) -> tuple[PriceAlertRuntimeData, PriceAlertRecentEventsData]:
    current = read_price_alert_rules(borrowed, owner_id=owner_id, now=now)
    domain = _read_domain(borrowed)
    generation = borrowed.manifest.generation_id
    if domain is None:
        return (
            PriceAlertRuntimeData(
                availability="not_running",
                generation_id=generation,
                status="not_running",
                status_label="未运行",
                message="价格提醒尚未运行。",
                evaluated_at=None,
                quote_updated_at=None,
                applied_at=None,
                mode="disabled",
                items=[],
            ),
            PriceAlertRecentEventsData(
                availability="not_running",
                generation_id=generation,
                message="运行后会在这里显示最近提醒。",
                items=[],
            ),
        )
    state = PriceAlertRuntimeState.model_validate_json(
        domain[PRICE_RUNTIME_TABLES[0]].rows[0]["body_json"]
    )
    if current.availability != "ready" or not current.members_ready:
        raise PriceAlertRuntimeDrift("price rule authority is unavailable")
    # These hashes describe the entire domain, including other owners and tombstones.
    # They are checked without exposing other owners through the Web DTO.
    hashes = {}
    for name in ("price_alert_rule_state", "manual_watchlist_state"):
        raw = borrowed.cursor.execute(
            f"SELECT rows_sha256 FROM {name} WHERE snapshot_key=?", ("current",)
        ).fetchall()
        if len(raw) != 1:
            raise PriceAlertRuntimeDrift("current price source receipt is incomplete")
        hashes[name] = raw[0][0]
    producer = state.producer
    metadata = None if producer is None or producer.round is None else producer.round.input_metadata
    if (
        metadata is not None
        and metadata.availability == "ready"
        and (metadata.rule_rows_sha256, metadata.member_rows_sha256)
        != (hashes["price_alert_rule_state"], hashes["manual_watchlist_state"])
    ):
        raise PriceAlertRuntimeDrift(
            "current price rules differ from the last evaluated complete source"
        )
    if (
        state.authority is not None
        and state.authority.available
        and (state.authority.rule_rows_sha256, state.authority.member_rows_sha256)
        != (hashes["price_alert_rule_state"], hashes["manual_watchlist_state"])
    ):
        raise PriceAlertRuntimeDrift("notifier has not applied this complete price source")
    facts = {}
    for row in domain[PRICE_RUNTIME_TABLES[1]].rows:
        if row["owner_id"] == owner_id:
            fact = PriceRuntimeRuleFact.model_validate_json(row["body_json"])
            actual = next(
                (
                    item
                    for item in current.rules
                    if item.rule_id == fact.rule_id and not item.deleted
                ),
                None,
            )
            if actual is None or (actual.version, actual.membership_version) != (
                fact.rule_version,
                fact.membership_version,
            ):
                raise PriceAlertRuntimeDrift("price evaluated rule version changed")
            facts[fact.rule_id] = fact
    active = tuple(
        rule
        for rule in current.rules
        if not rule.deleted and rule_item(rule, current, now=now).scope_status == "bound"
    )
    waiting = bool(active) and all(
        rule.rule_id in facts
        and facts[rule.rule_id].reason in {"outside_continuous_session", "market_closed"}
        for rule in active
    )
    status, label, message = "normal", "正常", ""
    availability = state.availability
    evaluated = None if producer is None or producer.round is None else producer.round.evaluated_at
    mode = (
        "record_only"
        if state.shadow
        else "notification"
        if state.authority and state.authority.delivery_enabled
        else "disabled"
    )
    if availability == "not_running":
        status, label, message = "not_running", "未运行", "价格提醒尚未运行。"
    elif (
        availability != "ready"
        or evaluated is None
        or not timedelta(0) <= now - evaluated <= timedelta(seconds=15)
    ):
        availability, status, label, message = (
            "unavailable",
            "error",
            "异常",
            "价格评估暂不可用，请稍后重试。",
        )
    elif not active or not metadata.requested_codes:
        status, label, message = "not_running", "未运行", "当前没有可检查的到价规则。"
    elif waiting:
        status, label, message = "not_running", "未运行", "等待交易时段。"
    elif mode != "notification":
        status, label, message = (
            "attention",
            "注意",
            "当前仅记录提醒。" if mode == "record_only" else "通知尚未开放。",
        )
    elif metadata.requested_codes and (
        metadata.quote_available_at is None
        or not timedelta(0) <= now - metadata.quote_available_at <= timedelta(seconds=15)
    ):
        availability, status, label, message = (
            "unavailable",
            "attention",
            "注意",
            "行情已过期，等待更新。",
        )
    elif state.authority and not any(
        item.owner_id == owner_id and item.target_count > 0
        for item in state.authority.owner_targets
    ):
        status, label, message = "attention", "注意", "未配置通知接收人。"
    items = []
    for rule in current.rules:
        if rule.deleted:
            continue
        fact = facts.get(rule.rule_id)
        item_status, item_label, item_message = status, label, message
        if not rule.enabled:
            item_status, item_label, item_message = "not_running", "未运行", "规则已停用。"
        elif fact is None:
            item_status, item_label, item_message = "not_running", "未运行", "等待本轮评估。"
        elif fact.state == "unavailable":
            item_status, item_label, item_message = "attention", "注意", "行情暂不可用，等待更新。"
        elif fact.reason in {"outside_continuous_session", "market_closed"}:
            local = now.astimezone(ZoneInfo("Asia/Shanghai")).time()
            item_status = "not_running"
            item_label = (
                "等待开盘"
                if local.hour < 9 or (local.hour == 9 and local.minute < 30)
                else "午间休市"
                if local.hour in {11, 12}
                else "已收盘"
            )
            item_message = ""
        items.append(
            PriceAlertRuntimeItem(
                rule_id=rule.rule_id,
                version=rule.version,
                membership_version=rule.membership_version,
                status=item_status,
                status_label=item_label,
                message=item_message,
                evaluated_at=None if fact is None or availability != "ready" else fact.evaluated_at,
                last_triggered_at=None
                if fact is None or availability != "ready"
                else fact.last_triggered_at,
                next_allowed_at=None
                if fact is None or availability != "ready"
                else fact.next_allowed_at,
                state=None if fact is None or availability != "ready" else fact.state,
            )
        )
    if status == "normal":
        if any(rule.rule_id not in facts for rule in active):
            status, label, message = "attention", "注意", "部分规则等待本轮评估。"
        elif any(facts[rule.rule_id].state == "unavailable" for rule in active):
            status, label, message = "attention", "注意", "部分行情暂不可用，等待更新。"
    all_attempts = [
        PriceAlertAttemptFact.model_validate_json(row["body_json"])
        for row in domain[PRICE_RUNTIME_TABLES[3]].rows
        if row["owner_id"] == owner_id
    ]
    events = []
    for row in sorted(
        domain[PRICE_RUNTIME_TABLES[2]].rows, key=lambda item: item["global_sequence"], reverse=True
    ):
        if row["owner_id"] != owner_id:
            continue
        record = PriceAlertBusRoutedRecord.model_validate_json(row["body_json"])
        event = record.event
        events.append(
            PriceAlertRecentEvent(
                event_id=event.event_id,
                rule_id=event.rule_id,
                rule_version=event.rule_version,
                membership_version=event.membership_version,
                rule_name=event.rule_name,
                ts_code=event.ts_code,
                comparison=event.comparison,
                threshold=event.threshold,
                price=event.price,
                triggered_at=event.evaluated_at,
                notifications=[
                    _notification(value)
                    for value in all_attempts
                    if value.event_id == event.event_id
                ],
                route_message="未配置通知接收人。"
                if record.receipt.disposition == "no_target"
                else "提醒已过期。"
                if record.receipt.disposition == "expired"
                else "",
            )
        )
    return (
        PriceAlertRuntimeData(
            availability=availability,
            generation_id=generation,
            status=status,
            status_label=label,
            message=message,
            evaluated_at=evaluated if availability == "ready" else None,
            quote_updated_at=None
            if metadata is None or availability != "ready"
            else metadata.quote_available_at,
            applied_at=None if state.authority is None else state.authority.applied_at,
            mode=mode,
            items=items,
        ),
        PriceAlertRecentEventsData(
            availability=availability,
            generation_id=generation,
            message="还没有提醒。" if not events else "",
            items=events,
        ),
    )
