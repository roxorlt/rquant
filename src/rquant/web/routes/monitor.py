"""C12.2 Serving-only signals and receipts, with bounded read-only pagination.

Legacy notification_log, acknowledgment writes, rule settings, and channel tests
remain later work; this route exposes none of those actions.
"""

from __future__ import annotations

import hashlib
import hmac
from base64 import b64decode, urlsafe_b64encode
from collections import defaultdict
from datetime import datetime
from typing import Annotated, Any, Literal

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
from rquant.web.models.monitor import MonitorReceipt, MonitorSignal, MonitorSignalsData
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.signal_display import DELIVERY_STATE_LABELS, delivery_state, signal_reasons
from rquant.web.status import DeliveryMode, delivery_mode

router = APIRouter(prefix="/monitor")

_SIGNALS_FIRST = (
    "SELECT global_sequence, signal_id, strategy_id, strategy_version, candidate_id, "
    "action, available_at, expires_at, reason_codes_json FROM signals "
    "ORDER BY available_at DESC, global_sequence DESC LIMIT ?"
)
_SIGNALS_AFTER = (
    "SELECT global_sequence, signal_id, strategy_id, strategy_version, candidate_id, "
    "action, available_at, expires_at, reason_codes_json FROM signals "
    "WHERE available_at < ? OR (available_at = ? AND global_sequence < ?) "
    "ORDER BY available_at DESC, global_sequence DESC LIMIT ?"
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


class _Cursor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["monitor_signals_v2"]
    generation_id: str = Field(min_length=1, max_length=128)
    last_available_at: AwareUtcDatetime
    last_sequence: int = Field(ge=1)
    page_size: int = Field(ge=1, le=50)


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
        if not hmac.compare_digest(hmac.new(key, payload, hashlib.sha256).digest(), signature):
            raise ValueError("cursor signature differs")
        return _Cursor.model_validate_json(payload)
    except (UnicodeError, ValueError, TypeError, ValidationError) as error:
        raise HTTPException(status_code=409, detail="数据已更新，请重新查看最近信号。") from error


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
        return "今天休市，显示历史信号"
    if phase is MarketPhase.AFTER_CLOSE:
        return "已收盘，显示最近信号"
    if phase is MarketPhase.PRE_OPEN:
        return "等待开盘，显示历史信号"
    return None


def _empty_data(
    source_state: Literal["unavailable", "not_published"], page_size: int
) -> MonitorSignalsData:
    return MonitorSignalsData(
        source_state=source_state,
        source_label="暂时读不到页面数据" if source_state == "unavailable" else "信号来源暂未发布",
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


def _page(
    borrowed: BorrowedGeneration,
    *,
    page_size: int,
    after: tuple[datetime, int] | None,
    key: bytes,
    now: datetime,
) -> MonitorSignalsData:
    cursor = borrowed.cursor
    total = int(cursor.execute("SELECT count(*) FROM signals").fetchone()[0])
    raw = cursor.execute(
        _SIGNALS_FIRST if after is None else _SIGNALS_AFTER,
        (page_size + 1,) if after is None else (after[0], after[0], after[1], page_size + 1),
    ).fetchall()
    rows = [
        SignalRow.model_validate(dict(zip(SignalRow.model_fields, item, strict=True)))
        for item in raw[:page_size]
    ]
    ids = [row.signal_id for row in rows]
    raw_receipts = cursor.execute(_RECEIPTS, (ids,)).fetchall() if ids else []
    truncated = len(raw_receipts) > _MAX_RECEIPTS
    receipts = [
        DeliveryRow.model_validate(dict(zip(DeliveryRow.model_fields, item, strict=True)))
        for item in raw_receipts[:_MAX_RECEIPTS]
    ]
    by_signal: dict[str, list[DeliveryRow]] = defaultdict(list)
    for receipt in receipts:
        by_signal[receipt.signal_id].append(receipt)
    names = readers.stock_names(
        cursor, readers.table_states(cursor), (row.candidate_id for row in rows)
    )
    mode = _mode(cursor)
    items: list[MonitorSignal] = []
    for row in rows:
        related = by_signal[row.signal_id]
        # A succeeded outbox row does not retain the mode at send time. The current
        # notifier heartbeat cannot establish delivery for older signals.
        state = "unconfirmed" if truncated else delivery_state(related, _UNKNOWN_MODE)
        note = _HISTORICAL_RECEIPT_NOTE if any(r.status == "succeeded" for r in related) else None
        items.append(
            MonitorSignal(
                signal_id=row.signal_id,
                sequence=row.global_sequence,
                at=row.available_at,
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
        )
    next_cursor = (
        _encode_cursor(
            _Cursor(
                kind="monitor_signals_v2",
                generation_id=borrowed.manifest.generation_id,
                last_available_at=rows[-1].available_at,
                last_sequence=rows[-1].global_sequence,
                page_size=page_size,
            ),
            key,
        )
        if len(raw) > page_size and rows
        else None
    )
    receipt_state = "truncated" if truncated else "has_receipts" if receipts else "no_receipts"
    return MonitorSignalsData(
        source_state="ready" if total else "empty",
        source_label="最近信号" if total else "还没有信号",
        receipt_state=receipt_state,
        receipt_label={
            "truncated": "回执较多，仅显示部分，状态未确认",
            "has_receipts": "通知回执已更新",
            "no_receipts": "这些信号还没有通知回执" if rows else "尚无通知回执",
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


@router.get("/signals", response_model=Envelope[MonitorSignalsData], summary="最近信号与通知回执")
def get_signals(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    page_size: Annotated[int, Query(ge=1, le=50)] = 20,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> Envelope[MonitorSignalsData]:
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
            raise HTTPException(status_code=409, detail="数据已更新，请重新查看最近信号。")
        if borrowed is None or meta.state == "unavailable":
            if decoded is not None:
                raise HTTPException(status_code=409, detail="数据已更新，请重新查看最近信号。")
            data = _empty_data("unavailable", page_size)
        elif not _source_published(borrowed):
            if decoded is not None:
                raise HTTPException(status_code=409, detail="数据已更新，请重新查看最近信号。")
            data = _empty_data("not_published", page_size)
        else:
            data = _page(
                borrowed,
                page_size=page_size,
                after=(decoded.last_available_at, decoded.last_sequence)
                if decoded is not None
                else None,
                key=web.cursor_key,
                now=now,
            )
    return Envelope[MonitorSignalsData](data=data, serving=meta)
