"""Bounded read-only alert timeline from one borrowed Serving generation.

Signals carry new-runtime delivery receipts. Legacy notifications are independent
per-channel submission attempts. Acknowledgments, rule writes and channel tests remain
separate later capabilities.
"""

from __future__ import annotations

import hashlib
import hmac
from base64 import b64decode, urlsafe_b64encode
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rquant.dashboard.runtime_console_data import DeliveryRow, SignalRow
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.serving_contracts import FreshnessStatus
from rquant.web import readers
from rquant.web.calendar import calendar_day
from rquant.web.envelope import Envelope
from rquant.web.labels import ACTION_LABELS, CHANNEL_LABELS, DELIVERY_LABELS, strategy_label
from rquant.web.market import MarketPhase, market_phase, shanghai_trade_date
from rquant.web.models.monitor import (
    MonitorNotification,
    MonitorReceipt,
    MonitorSignal,
    MonitorSurge,
    MonitorTimelineData,
    MonitorTimelineItem,
    MonitorTrigger,
)
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.signal_display import DELIVERY_STATE_LABELS, delivery_state, signal_reasons
from rquant.web.status import DeliveryMode, delivery_mode

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
    "trigger_time AS event_at, trade_date, ts_code, level, trigger_price, level_price "
    "FROM monitor_event"
)
_SURGE_SELECT = (
    "SELECT sha256(to_json([trade_date::VARCHAR, confirmed_at, ts_code])) AS sort_key, "
    f"{_SURGE_TIME} AT TIME ZONE 'Asia/Shanghai' AS event_at, "
    "trade_date, confirmed_at, ts_code, name, price, pct_chg, status "
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
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class _Cursor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["monitor_timeline_v1"]
    generation_id: str = Field(min_length=1, max_length=128)
    last_at: AwareUtcDatetime
    last_rank: int = Field(ge=1, le=4)
    last_key: str = Field(min_length=1, max_length=64)
    page_size: int = Field(ge=1, le=50)


@dataclass(frozen=True)
class _Event:
    kind: Literal["signal", "monitor", "surge", "notification"]
    at: datetime
    rank: int
    sort_key: str
    values: tuple[Any, ...]


def _segment(raw: bytes) -> str:
    return urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _encode_cursor(cursor: _Cursor, key: bytes) -> str:
    payload = cursor.model_dump_json().encode("utf-8")
    signature = hmac.new(key, payload, hashlib.sha256).digest()
    return f"{_segment(payload)}.{_segment(signature)}"


def _decode_cursor(token: str, key: bytes) -> _Cursor:
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
        return _Cursor.model_validate_json(payload)
    except (UnicodeError, ValueError, TypeError, ValidationError) as error:
        raise HTTPException(status_code=409, detail="数据已更新，请重新查看告警时间线。") from error


def _source_published(borrowed: BorrowedGeneration) -> bool:
    return any(
        mark.dataset_id == "signals" and mark.status is not FreshnessStatus.UNAVAILABLE
        for mark in borrowed.manifest.watermarks
    )


def _mode(cursor: Any) -> DeliveryMode:
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
    )


def _page(
    borrowed: BorrowedGeneration,
    *,
    page_size: int,
    after: tuple[datetime, int, str] | None,
    key: bytes,
    now: datetime,
) -> MonitorTimelineData:
    cursor = borrowed.cursor
    local_start = shanghai_trade_date(now) - timedelta(days=29)
    window_start = datetime.combine(local_start, time.min, tzinfo=_SHANGHAI).astimezone(UTC)
    states = readers.table_states(cursor)
    has_monitor = states.get("monitor_event") is not None and states["monitor_event"].available
    has_surge = states.get("surge_event") is not None and states["surge_event"].available
    has_notification = (
        states.get("legacy_notification") is not None
        and states["legacy_notification"].available
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
        if event.kind != "notification"
    ]
    names = readers.stock_names(cursor, states, codes)
    mode = _mode(cursor)
    items: list[MonitorTimelineItem] = []
    for event in selected:
        if event.kind == "signal":
            items.append(_signal_item(event, names=names, by_signal=by_signal, truncated=truncated))
        elif event.kind == "monitor":
            _day, code, level, price, level_price = event.values
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
                )
            )
        elif event.kind == "surge":
            _day, _confirmed_at, code, source_name, price, pct_chg, status = event.values
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
                )
            )
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
            _Cursor(
                kind="monitor_timeline_v1",
                generation_id=borrowed.manifest.generation_id,
                last_at=selected[-1].at,
                last_rank=selected[-1].rank,
                last_key=selected[-1].sort_key,
                page_size=page_size,
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
        source_state="ready" if total else "empty",
        source_label=(
            "告警时间线" if total else "告警数据暂不完整" if source_notes else "最近 30 天没有告警"
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
    )


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
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        decoded = _decode_cursor(cursor, web.cursor_key) if cursor is not None else None
        if decoded is not None and (
            decoded.generation_id != meta.generation_id or decoded.page_size != page_size
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
            )
    return Envelope[MonitorTimelineData](data=data, serving=meta)
