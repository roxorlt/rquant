"""Bounded read-only alert timeline from one borrowed Serving generation.

Signals carry new-runtime delivery receipts. Legacy notifications are independent
per-channel submission attempts. Acknowledgments use the same borrowed Serving generation.
"""

from __future__ import annotations

import hashlib
import hmac
from base64 import b64decode, urlsafe_b64encode
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from typing import TYPE_CHECKING, Annotated, Any, Literal
from zoneinfo import ZoneInfo

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rquant.alert_ack import stable_alert_id, stable_signal_alert_id
from rquant.alert_ack_read import read_alert_ack as read_domain_alert_ack
from rquant.condition_alert_runtime_projection import (
    MonitorRuntimeProjectionSnapshot,
    read_condition_triggers,
    read_monitor_runtime,
)
from rquant.dashboard.runtime_console_data import DeliveryRow, SignalRow
from rquant.page_control import AckAlert, PageControlReceipt, PageControlStatus
from rquant.pulse_watch import PulseAlert
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.serving_contracts import FreshnessStatus
from rquant.web import readers
from rquant.web.alert_ack_gateway import (
    AckLookupConflictError,
    AckLookupGateway,
    AckLookupInvalidResponseError,
    AckLookupUnavailableError,
)
from rquant.web.alert_ack_read import AlertReadModel, read_alert_ack
from rquant.web.calendar import calendar_day
from rquant.web.envelope import Envelope
from rquant.web.labels import ACTION_LABELS, CHANNEL_LABELS, DELIVERY_LABELS, strategy_label
from rquant.web.market import MarketPhase, market_phase, shanghai_trade_date
from rquant.web.models.alert_ack import (
    AckCommandConflict,
    AckCommandReceipt,
    AckCommandRequest,
    AlertAcknowledgmentView,
    UnacknowledgedSummary,
)
from rquant.web.models.monitor import (
    MonitorBuiltinStatus,
    MonitorBuiltinTrigger,
    MonitorChannelAttempt,
    MonitorChannelsData,
    MonitorChannelSubmission,
    MonitorConditionTrigger,
    MonitorNotification,
    MonitorReceipt,
    MonitorRuntimeChannel,
    MonitorRuntimeData,
    MonitorSignal,
    MonitorSurge,
    MonitorTimelineData,
    MonitorTimelineItem,
    MonitorTrigger,
)
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.signal_display import DELIVERY_STATE_LABELS, delivery_state, signal_reasons
from rquant.web.status import DeliveryMode, delivery_mode

if TYPE_CHECKING:
    from rquant.alert_ack_admission import AckAdmissionClient

router = APIRouter(prefix="/monitor")

_SURGE_TIME = "try_strptime(trade_date::VARCHAR || ' ' || confirmed_at, '%Y-%m-%d %H:%M')"
_SURGE_VALID = (
    "COALESCE(regexp_full_match(confirmed_at, '[0-2][0-9]:[0-5][0-9]'), FALSE) "
    f"AND {_SURGE_TIME} IS NOT NULL"
)
_SIGNALS_SELECT = (
    "SELECT printf('%020d', global_sequence) AS sort_key, "
    "event_time::TIMESTAMPTZ AS event_at, "
    "global_sequence, signal_id, strategy_id, strategy_version, candidate_id, action, "
    "available_at::TIMESTAMPTZ, expires_at::TIMESTAMPTZ, reason_codes_json FROM signals"
)
_MONITOR_SELECT = (
    "SELECT sha256(to_json([trade_date::VARCHAR, ts_code, level])) AS sort_key, "
    "trigger_time AS event_at, trade_date, ts_code, level, trigger_price, level_price, "
    "trigger_type, pool "
    "FROM monitor_event"
)
_SURGE_SELECT = (
    "SELECT sha256(to_json([trade_date::VARCHAR, confirmed_at, ts_code])) AS sort_key, "
    f"{_SURGE_TIME} AT TIME ZONE 'Asia/Shanghai' AS event_at, "
    "trade_date, confirmed_at, ts_code, name, price, pct_chg, status, theme, "
    "cum_amount, rel_cum, room_to_limit_pct "
    f"FROM surge_event WHERE {_SURGE_VALID}"
)
_NOTIFICATION_SELECT = (
    "SELECT record_key AS sort_key, sent_at AS event_at, "
    "scene_label, channel_label, submitted FROM legacy_notification"
)
_RECEIPTS = (
    "SELECT outbox_id, signal_id, recipient_id, channel, status, attempt_count, "
    "updated_at, NULL AS last_error "
    "FROM deliveries WHERE signal_id IN (SELECT unnest(?)) "
    "ORDER BY updated_at DESC, outbox_id DESC LIMIT 1001"
)
_NOTIFIERS = (
    "SELECT status, stale, consecutive_failures, last_error FROM runtime_services "
    "WHERE service_id LIKE 'notifier.%' ORDER BY service_id LIMIT 51"
)
_MAX_RECEIPTS = 1000
_HISTORICAL_RECEIPT_NOTE = "回执没有保存当时的推送方式，无法确认是否到达手机"
_UNKNOWN_MODE = DeliveryMode("unknown", "未确认", _HISTORICAL_RECEIPT_NOTE)
_TRIGGER_LABELS = {
    "attack_strong_carry": "强势承接",
    "attack_break_high": "上攻突破",
    "level_40": "回踩四成档",
    "level_30": "回踩三成档",
    "level_20": "回踩两成档",
    "stop_strong": "强势止损档",
    "stop_weak": "弱势止损档",
}
_SURGE_STATUS = {"confirmed": "已确认", "unbuyable": "临近涨停"}
_BUILTIN_LABELS = {"pool2_levels": "回踩档位", "pool_attack": "攻击信号", "surge": "爆量", "pulse": "市场异动"}
_SOURCE_LABELS = {"ready": "正常", "waiting": "等待", "stale": "陈旧", "disconnected": "断开", "unknown": "未知", "disabled": "未运行"}
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CHANNELS = (("pushdeer", CHANNEL_LABELS["pushdeer"]), ("pushplus", CHANNEL_LABELS["pushplus"]))
_MAX_LEGACY_NOTIFICATIONS = 10_000
MAX_ACK_REQUEST_BYTES = 4096


def _unverified_ack() -> AlertAcknowledgmentView:
    return AlertAcknowledgmentView(
        state="unavailable",
        eligible=False,
        label="确认状态暂不可用",
        note="这条告警尚未核对。",
    )


def _event_ack(
    alerts: AlertReadModel,
    source: Literal["signal", "monitor_event", "surge_event"],
    facts: object,
) -> AlertAcknowledgmentView:
    try:
        alert_id = (
            stable_signal_alert_id(facts)
            if source == "signal" and isinstance(facts, str)
            else stable_alert_id(source, facts)
        )
    except (TypeError, ValueError):
        return _unverified_ack()
    return alerts.status_for(source, alert_id)


class _Cursor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["monitor_timeline_v1"]
    generation_id: str = Field(min_length=1, max_length=128)
    last_at: AwareUtcDatetime
    last_rank: int = Field(ge=1, le=4)
    last_key: str = Field(min_length=1, max_length=64)
    page_size: int = Field(ge=1, le=50)


class _OwnerCursor(_Cursor):
    kind: Literal["monitor_timeline_v2"]
    last_rank: int = Field(ge=1, le=7)
    actor_key: str = Field(pattern=r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class _Event:
    kind: Literal["signal", "monitor", "surge", "notification", "builtin", "condition", "channel_attempt"]
    at: datetime
    rank: int
    sort_key: str
    values: tuple[Any, ...]


def _segment(raw: bytes) -> str:
    return urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _encode_cursor(cursor: _Cursor | _OwnerCursor, key: bytes) -> str:
    payload = cursor.model_dump_json().encode("utf-8")
    signature = hmac.new(key, payload, hashlib.sha256).digest()
    return f"{_segment(payload)}.{_segment(signature)}"


def _decode_cursor(token: str, key: bytes) -> _Cursor | _OwnerCursor:
    try:
        if len(token) > 512:
            raise ValueError("cursor too long")
        payload_text, signature_text = token.split(".")
        payload = b64decode(
            payload_text + "=" * (-len(payload_text) % 4), altchars=b"-_", validate=True
        )
        signature = b64decode(
            signature_text + "=" * (-len(signature_text) % 4), altchars=b"-_", validate=True
        )
        if _segment(payload) != payload_text or _segment(signature) != signature_text:
            raise ValueError("non-canonical cursor encoding")
        if not hmac.compare_digest(hmac.new(key, payload, hashlib.sha256).digest(), signature):
            raise ValueError("cursor signature differs")
        try:
            return _Cursor.model_validate_json(payload)
        except ValidationError:
            return _OwnerCursor.model_validate_json(payload)
    except (UnicodeError, ValueError, TypeError, ValidationError) as error:
        raise HTTPException(status_code=409, detail="数据已更新，请重新查看告警时间线。") from error


def _actor_key(actor_id: str | None) -> str:
    return hashlib.sha256(("monitor-viewer/v1:" + (actor_id or "")).encode()).hexdigest()


def _runtime_or_none(borrowed: BorrowedGeneration, *, now: datetime) -> MonitorRuntimeProjectionSnapshot | None:
    try:
        return read_monitor_runtime(borrowed, now=now)
    except (TypeError, ValueError, RuntimeError):
        return None


def _runtime_data(runtime: MonitorRuntimeProjectionSnapshot | None, *, viewer: str | None,
    now: datetime,
) -> MonitorRuntimeData:
    if runtime is None or viewer is None:
        return MonitorRuntimeData(state="unavailable", source_label="监控事实未就绪", source_note="尚未核对原运行来源。")
    builtins = []
    for row in runtime.builtin_heads:
        head = row.head
        if head.owner_id != viewer:
            continue
        state = head.source_state
        if state == "ready" and (not row.current_source_ready or row.source_valid_until is None or now > row.source_valid_until):
            state = "stale"
        builtins.append(MonitorBuiltinStatus(builtin_id=head.builtin_id, label=_BUILTIN_LABELS[head.builtin_id],
            enabled=head.definition.enabled, state=state, state_label=_SOURCE_LABELS[state], source_note=head.reason,
            observed_at=head.observed_at, evaluated_at=head.evaluated_at, source_valid_until=row.source_valid_until,
            last_triggered_at=head.last_triggered_at, matched_count=head.matched_count,
            channels=[channel.value for channel in head.definition.channels],
            applied_revision=head.applied_revision, applied_command_id=head.applied_command_id,
            monitor_installation_sha256=head.monitor_installation_sha256))
    fields = set(MonitorRuntimeChannel.model_fields) - {"channel_label"}
    channels = [MonitorRuntimeChannel(**row.model_dump(include=fields), channel_label=CHANNEL_LABELS[row.channel.value])
        for row in runtime.channels if row.owner_id == viewer]
    window = runtime.notification_window
    mode = "unknown" if window.binding is None else window.binding.mode
    ready = runtime.builtin_window.state == "ready" or window.complete
    return MonitorRuntimeData(state="ready" if ready else "unavailable", source_label="监控运行事实" if ready else "监控事实未就绪",
        source_note="统计仅覆盖标注的原账本窗口；通道接受不代表手机送达。", observed_at=window.observed_at,
        mode=mode, mode_label={"unknown": "未确认", "shadow": "影子", "live": "正式"}[mode],
        applied_revision=window.applied_revision, applied_command_id=window.applied_command_id,
        monitor_installation_sha256=window.monitor_installation_sha256,
        builtins=builtins, channels=channels)


def _builtin_summary(alerts: AlertReadModel) -> UnacknowledgedSummary:
    if alerts.builtin_count_as_of is None:
        return UnacknowledgedSummary(note="内置告警的完整原窗口尚未核对。")
    count = sum(alerts.is_eligible(source, alert_id) for source, alert_id in alerts.events if source == "monitor_builtin_event")
    return UnacknowledgedSummary(state="ready", count=count, count_as_of=alerts.builtin_count_as_of,
        label="内置待确认", note="仅统计当前账号可见的完整内置告警窗口。")


def _private_events(borrowed: BorrowedGeneration, runtime: MonitorRuntimeProjectionSnapshot | None, *,
    actor_id: str | None, now: datetime, window_start: datetime,
) -> list[_Event]:
    if actor_id is None:
        return []
    result = []
    if runtime is not None:
        for row in runtime.builtin_events:
            if row.event.owner_id == actor_id:
                result.append(_Event("builtin", row.event.event_time, 7, row.event.event_id, (row,)))
        for row in runtime.groups:
            group = row.group
            if group.binding.owner_id != actor_id:
                continue
            if not row.attempts:
                state = "shadow" if group.status == "shadow" else "waiting" if group.status == "waiting" else "unknown"
                result.append(_Event("channel_attempt", group.opened_at, 5, group.group_id, (row, None, state)))
            for attempt in row.attempts:
                observed = attempt.observation
                at = attempt.intent.issued_at if observed is None else observed.called_at
                key = attempt.intent.physical_id(0 if observed is None else observed.key_slot)
                state = "possible" if observed is None else observed.disposition
                result.append(_Event("channel_attempt", at, 5, key, (row, attempt, state)))
    for row in read_condition_triggers(borrowed, owner_id=actor_id, now=now):
        result.append(_Event("condition", row.event.event_time, 6, row.event.event_id, (row,)))
    return [row for row in result if window_start <= row.at <= now]


def _private_item(event: _Event, *, alerts: AlertReadModel) -> MonitorTimelineItem:
    if event.kind == "builtin":
        original = event.values[0].event
        detection = original.detection
        market = detection.subject == "market"
        label = (
            PulseAlert.model_validate_json(detection.original_result_json).kind_label
            if market else _BUILTIN_LABELS[original.builtin_id]
        )
        comparison_unit = (
            ("percent" if detection.kind == "ratio_jump" else "count") if market else None
        )
        threshold = getattr(detection, "threshold", None)
        threshold_unit = (
            None if threshold is None else "multiple" if original.builtin_id == "surge" else "CNY"
        )
        return MonitorBuiltinTrigger(event_key="builtin:" + original.event_id, at=event.at,
            builtin_id=original.builtin_id, event_label=label,
            subject=detection.subject, code=original.ts_code, name=original.stock_name,
            price=getattr(detection, "trigger_price", None), threshold=threshold,
            threshold_unit=threshold_unit,
            before=getattr(detection, "before", None), after=getattr(detection, "after", None),
            comparison_unit=comparison_unit,
            source_note=detection.kind, acknowledgment=alerts.status_for("monitor_builtin_event", stable_alert_id("monitor_builtin_event", original)))
    if event.kind == "condition":
        row = event.values[0]
        return MonitorConditionTrigger(event_key="condition:" + row.event.event_id, at=event.at,
            code=row.event.ts_code, name=row.event.stock_name, event_label=row.event.rule_name,
            status_label="已恢复" if row.event.trigger_kind == "recovered" else "已触发",
            source_note="原通用条件事件；" + row.delivery_state)
    row, attempt, state = event.values
    return MonitorChannelAttempt(event_key="channel:" + event.sort_key, at=event.at,
        channel_label=CHANNEL_LABELS[row.group.target.channel.value], mode=row.group.binding.mode,
        state=state, state_label={"waiting": "等待合并", "sending": "正在提交", "shadow": "影子记录", "accepted": "通道接受",
            "rejected": "通道拒绝", "unknown": "结果未知", "possible": "可能已请求"}[state],
        logical_count=len(row.group.members), attempt_no=None if attempt is None else max(member.attempt_no for member in attempt.intent.members),
        source_note="原通知账本；提交结果不代表手机送达。")


def _source_published(borrowed: BorrowedGeneration) -> bool:
    return any(
        mark.dataset_id == "signals" and mark.status is not FreshnessStatus.UNAVAILABLE
        for mark in borrowed.manifest.watermarks
    )


def _channels_unavailable() -> MonitorChannelsData:
    return MonitorChannelsData(state="unavailable", channels=[])


def _channels(borrowed: BorrowedGeneration, *, now: datetime) -> MonitorChannelsData:
    cursor = borrowed.cursor
    states = readers.table_states(cursor)
    if any(
        states.get(name) is None or not states[name].available
        for name in ("legacy_notification", "legacy_notification_status")
    ):
        return _channels_unavailable()
    status_rows = cursor.execute(
        "SELECT snapshot_key, state, skipped FROM legacy_notification_status LIMIT 2"
    ).fetchall()
    if status_rows != [("current", "complete", 0)]:
        return _channels_unavailable()
    rows = cursor.execute(
        "SELECT sent_at, channel_label, submitted FROM legacy_notification LIMIT ?",
        (_MAX_LEGACY_NOTIFICATIONS + 1,),
    ).fetchall()
    if len(rows) > _MAX_LEGACY_NOTIFICATIONS:
        return _channels_unavailable()

    now = now.astimezone(UTC)
    today = now.astimezone(_SHANGHAI).date()
    today_start = datetime.combine(today, time.min, tzinfo=_SHANGHAI).astimezone(UTC)
    seven_day_start = datetime.combine(
        today - timedelta(days=6), time.min, tzinfo=_SHANGHAI
    ).astimezone(UTC)
    history_start = datetime.combine(
        today - timedelta(days=29), time.min, tzinfo=_SHANGHAI
    ).astimezone(UTC)
    counts: dict[str, dict[str, Any]] = {
        name: {"today": 0, "attempts": 0, "submitted": 0, "last": None} for name, _ in _CHANNELS
    }
    by_label = {label: name for name, label in _CHANNELS}
    for sent_at, label, submitted in rows:
        if (
            not isinstance(sent_at, datetime)
            or sent_at.tzinfo is None
            or sent_at.utcoffset() is None
            or label not in by_label
            or type(submitted) is not bool
        ):
            return _channels_unavailable()
        event_at = sent_at.astimezone(UTC)
        if event_at > now:
            return _channels_unavailable()
        current = counts[by_label[label]]
        if event_at >= seven_day_start:
            current["attempts"] += 1
            if submitted:
                current["submitted"] += 1
        if submitted:
            if event_at >= today_start:
                current["today"] += 1
            if event_at >= history_start and (
                current["last"] is None or event_at > current["last"]
            ):
                current["last"] = event_at
    return MonitorChannelsData(
        state="ready",
        channels=[
            MonitorChannelSubmission(
                channel=name,
                channel_label=label,
                today_submitted=counts[name]["today"],
                seven_day_attempts=counts[name]["attempts"],
                seven_day_submitted=counts[name]["submitted"],
                seven_day_success_pct=(
                    round(100 * counts[name]["submitted"] / counts[name]["attempts"], 1)
                    if counts[name]["attempts"]
                    else None
                ),
                last_success_at=counts[name]["last"],
            )
            for name, label in _CHANNELS
        ],
    )


def _mode(cursor: Any) -> DeliveryMode:
    # Empty optional Serving tables may infer a non-string service_id type.
    if cursor.execute("SELECT 1 FROM runtime_services LIMIT 1").fetchone() is None:
        return delivery_mode(())
    rows = cursor.execute(_NOTIFIERS).fetchall()
    if len(rows) > 50:
        return DeliveryMode("unknown", "未确认", "推送服务太多，无法确认手机是否收到")
    return delivery_mode([(str(s), bool(st), int(n), e) for s, st, n, e in rows])


def _market_note(cursor: Any, now: datetime) -> str | None:
    day = calendar_day(cursor, shanghai_trade_date(now))
    phase = market_phase(now, day.is_trading_day)
    if phase is MarketPhase.NON_TRADING_DAY:
        return "今天休市，显示历史告警"
    if phase is MarketPhase.AFTER_CLOSE:
        return "已收盘，显示最近告警"
    if phase is MarketPhase.PRE_OPEN:
        return "等待开盘，显示历史告警"
    return None


def _empty_data(
    source_state: Literal["unavailable", "not_published"], page_size: int
) -> MonitorTimelineData:
    return MonitorTimelineData(
        source_state=source_state,
        source_label="暂时读不到页面数据" if source_state == "unavailable" else "告警来源暂未发布",
        source_note=None,
        receipt_state="not_published",
        receipt_label="通知来源暂未发布",
        total=None,
        page_size=page_size,
        items=[],
        next_cursor=None,
        mode="unknown",
        mode_label="未确认",
        mode_note=None,
        market_note=None,
    )


def _read_page(
    cursor: Any,
    *,
    select: str,
    kind: Literal["signal", "monitor", "surge", "notification"],
    rank: int,
    page_size: int,
    after: tuple[datetime, int, str] | None,
    window_start: datetime,
    now: datetime,
) -> list[_Event]:
    # Select statements are module constants. Only keyset values enter SQL as parameters.
    statement = (
        f"WITH events AS ({select}) SELECT * FROM events WHERE event_at >= ? AND event_at <= ?"
    )
    params: tuple[object, ...] = (window_start, now)
    if after is not None:
        statement += " AND (event_at < ? OR (event_at = ? AND (? < ? OR (? = ? AND sort_key < ?))))"
        params = (*params, after[0], after[0], rank, after[1], rank, after[1], after[2])
    statement += " ORDER BY event_at DESC, sort_key DESC LIMIT ?"
    rows = cursor.execute(statement, (*params, page_size + 1)).fetchall()
    result: list[_Event] = []
    for key, at, *values in rows:
        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("published event time must have an explicit timezone")
        result.append(_Event(kind, at.astimezone(UTC), rank, str(key), tuple(values)))
    return result


def _signal_item(
    event: _Event,
    *,
    names: dict[str, str],
    by_signal: dict[str, list[DeliveryRow]],
    truncated: bool,
    alerts: AlertReadModel,
) -> MonitorSignal:
    row = SignalRow.model_validate(dict(zip(SignalRow.model_fields, event.values, strict=True)))
    related = by_signal[row.signal_id]
    # Historical success only proves the outbox recorded success. It does not prove
    # delivery to a device; the current heartbeat cannot establish an old mode.
    state = "unconfirmed" if truncated else delivery_state(related, _UNKNOWN_MODE)
    note = _HISTORICAL_RECEIPT_NOTE if any(r.status == "succeeded" for r in related) else None
    return MonitorSignal(
        event_key=f"signal:{row.signal_id}",
        signal_id=row.signal_id,
        sequence=row.global_sequence,
        at=event.at,
        code=row.candidate_id,
        name=names.get(row.candidate_id),
        strategy_id=row.strategy_id,
        strategy_name=strategy_label(row.strategy_id),
        action=row.action,
        action_label=ACTION_LABELS.get(row.action, "其他"),
        reasons=signal_reasons(row),
        delivery=state,
        delivery_label=(
            "送达未确认"
            if state == "unconfirmed"
            else "暂无回执"
            if state == "none"
            else DELIVERY_STATE_LABELS[state]
        ),
        delivery_note=note,
        receipts=[
            MonitorReceipt(
                outbox_id=receipt.outbox_id,
                recipient_id=receipt.recipient_id,
                channel=receipt.channel,
                channel_label=CHANNEL_LABELS.get(receipt.channel, "其他通道"),
                status=receipt.status,
                status_label=(
                    "送达未确认"
                    if receipt.status == "succeeded"
                    else DELIVERY_LABELS.get(receipt.status, "未确认")
                ),
                updated_at=receipt.updated_at,
                attempt_count=receipt.attempt_count,
            )
            for receipt in related
        ],
        acknowledgment=_event_ack(alerts, "signal", row.signal_id),
    )


def _page(
    borrowed: BorrowedGeneration,
    *,
    page_size: int,
    after: tuple[datetime, int, str] | None,
    key: bytes,
    now: datetime,
    alerts: AlertReadModel | None = None,
    actor_id: str | None = None,
    runtime: MonitorRuntimeProjectionSnapshot | None = None,
) -> MonitorTimelineData:
    if alerts is None:
        alerts = AlertReadModel(UnacknowledgedSummary(), None, {}, {})
    cursor = borrowed.cursor
    local_start = shanghai_trade_date(now) - timedelta(days=29)
    window_start = datetime.combine(local_start, time.min, tzinfo=_SHANGHAI).astimezone(UTC)
    states = readers.table_states(cursor)
    private_events = _private_events(borrowed, runtime, actor_id=actor_id, now=now, window_start=window_start)
    private = runtime is not None or bool(private_events)
    has_monitor = states.get("monitor_event") is not None and states["monitor_event"].available
    has_surge = states.get("surge_event") is not None and states["surge_event"].available
    has_notification = (
        states.get("legacy_notification") is not None and states["legacy_notification"].available
    )
    has_notification_status = (
        states.get("legacy_notification_status") is not None
        and states["legacy_notification_status"].available
    )
    total = int(
        cursor.execute(
            "SELECT count(*) FROM signals WHERE event_time::TIMESTAMPTZ BETWEEN ? AND ?",
            (window_start, now),
        ).fetchone()[0]
    )
    events = _read_page(
        cursor,
        select=_SIGNALS_SELECT,
        kind="signal",
        rank=4,
        page_size=page_size,
        after=after,
        window_start=window_start,
        now=now,
    )
    missing: list[str] = []
    notification_note: str | None = None
    bad_times = 0
    if has_monitor:
        total += int(
            cursor.execute(
                "SELECT count(*) FROM monitor_event WHERE trigger_time BETWEEN ? AND ?",
                (window_start, now),
            ).fetchone()[0]
        )
        events.extend(
            _read_page(
                cursor,
                select=_MONITOR_SELECT,
                kind="monitor",
                rank=3,
                page_size=page_size,
                after=after,
                window_start=window_start,
                now=now,
            )
        )
    else:
        missing.append("盯盘触发记录")
    if has_surge:
        total += int(
            cursor.execute(
                f"SELECT count(*) FROM surge_event WHERE {_SURGE_VALID} "
                f"AND trade_date >= ? AND ({_SURGE_TIME} AT TIME ZONE 'Asia/Shanghai') <= ?",
                (local_start, now),
            ).fetchone()[0]
        )
        bad_times = int(
            cursor.execute(
                f"SELECT count(*) FROM surge_event WHERE NOT ({_SURGE_VALID}) AND trade_date >= ?",
                (local_start,),
            ).fetchone()[0]
        )
        events.extend(
            _read_page(
                cursor,
                select=_SURGE_SELECT,
                kind="surge",
                rank=2,
                page_size=page_size,
                after=after,
                window_start=window_start,
                now=now,
            )
        )
    else:
        missing.append("爆量记录")
    if has_notification_status:
        status_row = cursor.execute(
            "SELECT state, skipped FROM legacy_notification_status WHERE snapshot_key = 'current'"
        ).fetchone()
        if (
            status_row is None
            or status_row[0] not in {"complete", "partial"}
            or not has_notification
        ):
            notification_note = "通知记录暂不可用，仅显示其他告警"
        else:
            if status_row[0] == "partial":
                notification_note = f"{int(status_row[1])} 条通知记录无法识别，已略过"
            total += int(
                cursor.execute(
                    "SELECT count(*) FROM legacy_notification WHERE sent_at BETWEEN ? AND ?",
                    (window_start, now),
                ).fetchone()[0]
            )
            events.extend(
                _read_page(
                    cursor,
                    select=_NOTIFICATION_SELECT,
                    kind="notification",
                    rank=1,
                    page_size=page_size,
                    after=after,
                    window_start=window_start,
                    now=now,
                )
            )
    else:
        notification_note = "通知记录尚未接入，仅显示其他告警"
    total += len(private_events)
    events.extend(row for row in private_events if after is None or (row.at, row.rank, row.sort_key) < after)
    if private and (missing or notification_note or bad_times
            or runtime is not None and (runtime.builtin_window.state != "ready"
                or runtime.builtin_window.truncated or runtime.notification_window.truncated)
            or any(row.kind == "condition" for row in private_events)):
        # The legacy projections and retained generic history do not prove a full joint window.
        total = None
    events.sort(key=lambda event: (event.at, event.rank, event.sort_key), reverse=True)
    selected = events[:page_size]
    signals = [
        SignalRow.model_validate(dict(zip(SignalRow.model_fields, event.values, strict=True)))
        for event in selected
        if event.kind == "signal"
    ]
    ids = [row.signal_id for row in signals]
    raw_receipts = cursor.execute(_RECEIPTS, (ids,)).fetchall() if ids else []
    truncated = len(raw_receipts) > _MAX_RECEIPTS
    receipts = [
        DeliveryRow.model_validate(dict(zip(DeliveryRow.model_fields, item, strict=True)))
        for item in raw_receipts[:_MAX_RECEIPTS]
    ]
    by_signal: dict[str, list[DeliveryRow]] = defaultdict(list)
    for receipt in receipts:
        by_signal[receipt.signal_id].append(receipt)
    codes = [
        str(
            event.values[4]
            if event.kind == "signal"
            else event.values[1]
            if event.kind == "monitor"
            else event.values[2]
        )
        for event in selected
        if event.kind in {"signal", "monitor", "surge"}
    ]
    names = readers.stock_names(cursor, states, codes)
    mode = _mode(cursor)
    items: list[MonitorTimelineItem] = []
    for event in selected:
        if event.kind == "signal":
            items.append(
                _signal_item(
                    event,
                    names=names,
                    by_signal=by_signal,
                    truncated=truncated,
                    alerts=alerts,
                )
            )
        elif event.kind == "monitor":
            day, code, level, price, level_price, trigger_type, pool = event.values
            facts = {
                "trade_date": day,
                "trigger_time": event.at,
                "ts_code": code,
                "level": level,
                "trigger_price": price,
                "level_price": level_price,
                "trigger_type": trigger_type,
                "pool": pool,
            }
            items.append(
                MonitorTrigger(
                    event_key=f"monitor:{event.sort_key}",
                    at=event.at,
                    code=str(code),
                    name=names.get(str(code)),
                    event_label=_TRIGGER_LABELS.get(str(level), "盯盘触发"),
                    price=price,
                    level_price=level_price,
                    status_label="已触发",
                    acknowledgment=_event_ack(alerts, "monitor_event", facts),
                )
            )
        elif event.kind == "surge":
            (
                day,
                confirmed_at,
                code,
                source_name,
                price,
                pct_chg,
                status,
                theme,
                cum_amount,
                rel_cum,
                room_to_limit_pct,
            ) = event.values
            facts = {
                "trade_date": day,
                "confirmed_at": confirmed_at,
                "ts_code": code,
                "name": source_name,
                "theme": theme,
                "price": price,
                "pct_chg": pct_chg,
                "cum_amount": cum_amount,
                "rel_cum": rel_cum,
                "room_to_limit_pct": room_to_limit_pct,
                "status": status,
            }
            items.append(
                MonitorSurge(
                    event_key=f"surge:{event.sort_key}",
                    at=event.at,
                    code=str(code),
                    name=str(source_name) if source_name else names.get(str(code)),
                    event_label="爆量",
                    price=price,
                    pct_chg=pct_chg,
                    status_label=_SURGE_STATUS.get(str(status), "已确认"),
                    acknowledgment=_event_ack(alerts, "surge_event", facts),
                )
            )
        elif event.kind in {"builtin", "condition", "channel_attempt"}:
            items.append(_private_item(event, alerts=alerts))
        else:
            scene_label, channel_label, submitted = event.values
            items.append(
                MonitorNotification(
                    event_key=f"notification:{event.sort_key}",
                    at=event.at,
                    scene_label=str(scene_label),
                    channel_label=str(channel_label),
                    submitted=bool(submitted),
                    submission_label="提交成功" if submitted else "提交失败",
                )
            )
    next_cursor = (
        _encode_cursor(
            (_OwnerCursor if private else _Cursor)(
                kind="monitor_timeline_v2" if private else "monitor_timeline_v1",
                generation_id=borrowed.manifest.generation_id,
                last_at=selected[-1].at,
                last_rank=selected[-1].rank,
                last_key=selected[-1].sort_key,
                page_size=page_size,
                **({"actor_key": _actor_key(actor_id)} if private else {}),
            ),
            key,
        )
        if len(events) > page_size and selected
        else None
    )
    receipt_state = "truncated" if truncated else "has_receipts" if receipts else "no_receipts"
    source_notes = []
    if missing:
        source_notes.append(f"当前数据缺少{'、'.join(missing)}，仅显示已有记录")
    if notification_note:
        source_notes.append(notification_note)
    if bad_times:
        source_notes.append(f"{bad_times} 条爆量记录时间无效，未纳入时间线")
    return MonitorTimelineData(
        source_state="ready" if total or events else "empty",
        source_label=(
            "告警时间线" if total or events else "告警数据暂不完整" if source_notes else "最近 30 天没有告警"
        ),
        source_note="；".join(source_notes) if source_notes else None,
        receipt_state=receipt_state,
        receipt_label={
            "truncated": "回执较多，仅显示部分，状态未确认",
            "has_receipts": "通知回执已更新",
            "no_receipts": "这些信号还没有通知回执" if signals else "本页没有新运行时通知回执",
        }[receipt_state],
        total=total,
        page_size=page_size,
        items=items,
        next_cursor=next_cursor,
        mode=mode.mode,
        mode_label=mode.label,
        mode_note=mode.note,
        market_note=_market_note(cursor, now),
        unacknowledged=alerts.summary,
        builtin_unacknowledged=_builtin_summary(alerts),
    )


@router.get("/runtime", response_model=Envelope[MonitorRuntimeData], summary="监控原运行事实")
def get_runtime(
    request: Request, response: Response,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[MonitorRuntimeData]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure)
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        runtime = None if borrowed is None or meta.state != "ready" else _runtime_or_none(borrowed, now=now)
        data = _runtime_data(runtime, viewer=viewer, now=now)
    return Envelope[MonitorRuntimeData](data=data, serving=meta)


@router.get("/channels", response_model=Envelope[MonitorChannelsData], summary="推送通道提交状态")
def get_channels(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[MonitorChannelsData]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure
        )
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        if borrowed is None or meta.state == "unavailable":
            data = _channels_unavailable()
        else:
            try:
                data = _channels(borrowed, now=now)
            except Exception:
                # A corrupt or unreadable published projection cannot become a precise zero.
                data = _channels_unavailable()
    return Envelope[MonitorChannelsData](data=data, serving=meta)


@router.get("/timeline", response_model=Envelope[MonitorTimelineData], summary="告警时间线")
def get_timeline(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    page_size: Annotated[int, Query(ge=1, le=50)] = 20,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> Envelope[MonitorTimelineData]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure
        )
        alerts = read_domain_alert_ack(borrowed, serving_ready=meta.state == "ready", now=now,
            stale_after=web.settings.stale_after, actor_id=_viewer)
        runtime = None if borrowed is None else _runtime_or_none(borrowed, now=now)
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        decoded = _decode_cursor(cursor, web.cursor_key) if cursor is not None else None
        if decoded is not None and (
            decoded.generation_id != meta.generation_id or decoded.page_size != page_size
            or isinstance(decoded, _OwnerCursor) and decoded.actor_key != _actor_key(_viewer)
            or runtime is not None and not isinstance(decoded, _OwnerCursor)
        ):
            raise HTTPException(status_code=409, detail="数据已更新，请重新查看告警时间线。")
        if borrowed is None or meta.state == "unavailable":
            if decoded is not None:
                raise HTTPException(status_code=409, detail="数据已更新，请重新查看告警时间线。")
            data = _empty_data("unavailable", page_size)
        elif not _source_published(borrowed):
            if decoded is not None:
                raise HTTPException(status_code=409, detail="数据已更新，请重新查看告警时间线。")
            data = _empty_data("not_published", page_size)
        else:
            data = _page(
                borrowed,
                page_size=page_size,
                after=(decoded.last_at, decoded.last_rank, decoded.last_key)
                if decoded is not None
                else None,
                key=web.cursor_key,
                now=now,
                alerts=alerts,
                actor_id=_viewer,
                runtime=runtime,
            )
    return Envelope[MonitorTimelineData](data=data, serving=meta)


@router.post(
    "/ack",
    response_model=AckCommandReceipt,
    responses={409: {"model": AckCommandConflict}},
    summary="确认一条告警",
)
async def acknowledge_alert(
    request: Request,
    body: AckCommandRequest,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> AckCommandReceipt | JSONResponse:
    from rquant.alert_ack_admission import AckAdmissionStaleGenerationError

    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if len(await request.body()) > MAX_ACK_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="请求内容过长，请重试。")
    web = request.app.state.web
    command = AckAlert.model_validate({**body.model_dump(mode="python"), "actor_id": viewer})
    original = await _lookup_ack_command(web.ack_lookup, command)
    if original is not None:
        if (
            original.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}
            and web.ack_admission is not None
        ):
            try:
                resumed = await _submit_ack_admission(web.ack_admission, command)
                return _ack_response(command, resumed)
            except (HTTPException, AckAdmissionStaleGenerationError) as admission_error:
                durable = await _lookup_ack_command(web.ack_lookup, command)
                if durable is None:
                    raise HTTPException(
                        status_code=502, detail="回执无法核对，请使用原请求重试。"
                    ) from admission_error
                return _ack_response(command, durable)
        return _ack_response(command, original)
    rejection: HTTPException | None = None
    web_stale = False
    with web.tracker.borrow() as borrowed:
        now = web.clock()
        meta = serving_meta(
            borrowed,
            now=now,
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if borrowed is None:
            rejection = HTTPException(status_code=409, detail="数据已更新，请刷新告警时间线。")
        elif meta.generation_id == body.generation_id:
            alerts = read_domain_alert_ack(
                borrowed, serving_ready=meta.state == "ready", now=now,
                stale_after=web.settings.stale_after, actor_id=viewer,
            )
            if not any(
                item.alert_id == body.alert_id and alerts.is_eligible(item.source, item.alert_id)
                for item in alerts.events.values()
            ):
                rejection = HTTPException(status_code=409, detail="确认状态暂不可用，请稍后重试。")
        else:
            web_stale = True
        # For an old generation, only the serialized PageControl admission can
        # distinguish an absent command from another tab still enqueuing it.
    if rejection is not None:
        durable = await _lookup_ack_command(web.ack_lookup, command)
        if durable is not None:
            return _ack_response(command, durable)
        raise rejection
    if web_stale:
        durable = await _lookup_ack_command(web.ack_lookup, command)
        if durable is not None:
            return _ack_response(command, durable)
    if web.ack_admission is None:
        raise HTTPException(status_code=503, detail="确认服务尚未就绪，请稍后重试。")
    try:
        receipt = await _submit_ack_admission(web.ack_admission, command)
    except (HTTPException, AckAdmissionStaleGenerationError) as admission_error:
        durable = await _lookup_ack_command(web.ack_lookup, command)
        if durable is not None:
            return _ack_response(command, durable)
        if isinstance(admission_error, AckAdmissionStaleGenerationError):
            conflict = AckCommandConflict(
                detail="数据已更新，请刷新告警时间线。", code="stale_generation_no_effect"
            )
            return JSONResponse(status_code=409, content=conflict.model_dump(exclude_none=True))
        raise admission_error
    return _ack_response(command, receipt)


async def _lookup_ack_command(
    gateway: AckLookupGateway, command: AckAlert
) -> PageControlReceipt | None:
    try:
        return await anyio.to_thread.run_sync(gateway.lookup, command)
    except AckLookupConflictError as error:
        raise HTTPException(
            status_code=409, detail="命令内容与已有记录不一致，请保留原请求。"
        ) from error
    except AckLookupUnavailableError as error:
        raise HTTPException(status_code=503, detail="连接暂不可用，请使用原请求重试。") from error
    except AckLookupInvalidResponseError as error:
        raise HTTPException(status_code=502, detail="回执无法核对，请使用原请求重试。") from error


async def _submit_ack_admission(
    admission: AckAdmissionClient, command: AckAlert
) -> PageControlReceipt:
    from rquant.alert_ack_admission import (
        AckAdmissionRejectedError,
        AckAdmissionStaleGenerationError,
        AckAdmissionUnavailableError,
    )

    try:
        return await anyio.to_thread.run_sync(admission.submit, command)
    except AckAdmissionStaleGenerationError:
        raise
    except AckAdmissionRejectedError as error:
        raise HTTPException(status_code=409, detail="告警状态已变化，请刷新后重试。") from error
    except AckAdmissionUnavailableError as error:
        raise HTTPException(status_code=503, detail="连接暂不可用，请使用原请求重试。") from error


def _ack_response(command: AckAlert, receipt: PageControlReceipt) -> AckCommandReceipt:
    if receipt.command_id != command.command_id:
        raise HTTPException(status_code=502, detail="回执无法核对，请使用原请求重试。")
    if receipt.status is PageControlStatus.SUCCEEDED:
        result = receipt.result
        confirmation_id = result.get("confirmation_id") if isinstance(result, dict) else None
        if not isinstance(confirmation_id, str) or not 1 <= len(confirmation_id) <= 128:
            raise HTTPException(status_code=502, detail="回执无法核对，请使用原请求重试。")
        return AckCommandReceipt(
            command_id=command.command_id,
            status="succeeded",
            confirmation_id=confirmation_id,
            message="已受理，正在同步",
        )
    return AckCommandReceipt(
        command_id=command.command_id,
        status=receipt.status.value,
        message={
            "pending": "已受理，等待处理",
            "processing": "正在处理",
            "failed": "确认未完成，请检查后重试。",
            "ambiguous": "状态待确认，请使用原请求重试。",
        }[receipt.status.value],
    )
