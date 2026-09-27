"""One read-only timeline of published alerts and notification receipts."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.web.models.alert_ack import AlertAcknowledgmentView, UnacknowledgedSummary
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

    kind: Literal["signal"] = "signal"
    event_key: str
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
    acknowledgment: AlertAcknowledgmentView = Field(
        default_factory=lambda: AlertAcknowledgmentView(
            state="unavailable", eligible=False, label="确认状态暂不可用"
        )
    )


class MonitorTrigger(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["monitor"] = "monitor"
    event_key: str
    at: datetime
    code: str
    name: str | None
    event_label: str
    price: float | None
    level_price: float | None
    status_label: str
    acknowledgment: AlertAcknowledgmentView = Field(
        default_factory=lambda: AlertAcknowledgmentView(
            state="unavailable", eligible=False, label="确认状态暂不可用"
        )
    )


class MonitorSurge(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["surge"] = "surge"
    event_key: str
    at: datetime
    code: str
    name: str | None
    event_label: str
    price: float | None
    pct_chg: float | None
    status_label: str
    acknowledgment: AlertAcknowledgmentView = Field(
        default_factory=lambda: AlertAcknowledgmentView(
            state="unavailable", eligible=False, label="确认状态暂不可用"
        )
    )


class MonitorNotification(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["notification"] = "notification"
    event_key: str
    at: datetime
    scene_label: str
    channel_label: str
    submitted: bool
    submission_label: str


MonitorTimelineItem = Annotated[
    MonitorSignal | MonitorTrigger | MonitorSurge | MonitorNotification,
    Field(discriminator="kind"),
]


class MonitorTimelineData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_state: SignalSourceState
    source_label: str
    source_note: str | None
    receipt_state: ReceiptSourceState
    receipt_label: str
    total: int | None
    page_size: int
    items: list[MonitorTimelineItem]
    next_cursor: str | None
    mode: DeliveryMode
    mode_label: str
    mode_note: str | None
    market_note: str | None
    unacknowledged: UnacknowledgedSummary = Field(default_factory=UnacknowledgedSummary)
