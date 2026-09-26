"""Recent published signals and notification receipts."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from rquant.web.models.overview import DeliveryMode, DeliveryState

SignalSourceState = Literal["unavailable", "not_published", "empty", "ready"]
ReceiptSourceState = Literal["not_published", "no_receipts", "has_receipts", "truncated"]


class MonitorReceipt(BaseModel):
    model_config = ConfigDict(frozen=True)

    outbox_id: str
    recipient_id: str
    channel: str
    channel_label: str
    status: str
    status_label: str
    updated_at: datetime
    attempt_count: int


class MonitorSignal(BaseModel):
    model_config = ConfigDict(frozen=True)

    signal_id: str
    sequence: int
    at: datetime
    code: str
    name: str | None
    strategy_id: str
    strategy_name: str
    action: str
    action_label: str
    reasons: list[str]
    delivery: DeliveryState
    delivery_label: str
    delivery_note: str | None
    receipts: list[MonitorReceipt]


class MonitorSignalsData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_state: SignalSourceState
    source_label: str
    receipt_state: ReceiptSourceState
    receipt_label: str
    total: int | None
    page_size: int
    items: list[MonitorSignal]
    next_cursor: str | None
    mode: DeliveryMode
    mode_label: str
    mode_note: str | None
    market_note: str | None
