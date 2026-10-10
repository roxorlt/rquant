"""Plain labels shared by the overview and the recent-signal timeline."""

from __future__ import annotations

import json
from collections.abc import Sequence

from rquant.dashboard.runtime_console_data import DeliveryRow, SignalRow
from rquant.web.models.overview import DeliveryState
from rquant.web.status import DeliveryMode

SENDING = frozenset({"pending", "leased", "retry"})
DELIVERY_STATE_LABELS: dict[DeliveryState, str] = {
    "delivered": "已送达",
    "recorded": "仅记录",
    "unconfirmed": "未确认",
    "sending": "发送中",
    "failed": "失败",
    "expired": "已过期",
    "none": "未推送",
}
_FINISHED: dict[str, DeliveryState] = {
    "live": "delivered",
    "shadow": "recorded",
    "unknown": "unconfirmed",
}
_REASON_LABELS = {
    "auction_gap_observer": "竞价跳空观察",
    "auction_gap_confirmed": "竞价跳空确认",
    "vwap_supported": "均价线支撑",
}


def delivery_state(rows: Sequence[DeliveryRow], mode: DeliveryMode) -> DeliveryState:
    statuses = {row.status for row in rows}
    if not statuses:
        return "none"
    if "dead_letter" in statuses:
        return "failed"
    if statuses & SENDING:
        return "sending"
    if "succeeded" in statuses:
        return _FINISHED[mode.mode]
    return "expired"


def signal_reasons(signal: SignalRow) -> list[str]:
    try:
        codes = json.loads(signal.reason_codes_json)
    except (TypeError, ValueError):
        return []
    if not isinstance(codes, list):
        return []
    return [
        _REASON_LABELS[code] for code in codes if isinstance(code, str) and code in _REASON_LABELS
    ]
