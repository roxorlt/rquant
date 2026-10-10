"""One read-only timeline of published alerts and notification receipts."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.runtime_contracts import AwareUtcDatetime
from rquant.monitor_builtin_contracts import BuiltinId
from rquant.web.models.alert_ack import AlertAcknowledgmentView, UnacknowledgedSummary
from rquant.web.models.overview import DeliveryMode, DeliveryState

SignalSourceState = Literal["unavailable", "not_published", "empty", "ready"]
ReceiptSourceState = Literal["not_published", "no_receipts", "has_receipts", "truncated"]


class MonitorChannelSubmission(BaseModel):
    model_config = ConfigDict(frozen=True)

    channel: Literal["pushdeer", "pushplus"]
    channel_label: str
    today_submitted: int = Field(ge=0)
    seven_day_attempts: int = Field(ge=0)
    seven_day_submitted: int = Field(ge=0)
    seven_day_success_pct: float | None = Field(ge=0, le=100)
    last_success_at: AwareUtcDatetime | None


class MonitorChannelsData(BaseModel):
    model_config = ConfigDict(frozen=True)

    state: Literal["ready", "unavailable"]
    channels: list[MonitorChannelSubmission]


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


class MonitorBuiltinStatus(BaseModel):
    model_config = ConfigDict(frozen=True)

    builtin_id: BuiltinId
    label: str
    enabled: bool
    state: Literal["ready", "waiting", "stale", "disconnected", "unknown", "disabled"]
    state_label: str
    source_note: str
    observed_at: AwareUtcDatetime | None
    evaluated_at: AwareUtcDatetime
    source_valid_until: AwareUtcDatetime | None
    last_triggered_at: AwareUtcDatetime | None
    matched_count: int | None = Field(ge=0)
    channels: list[Literal["pushdeer", "pushplus"]]
    applied_revision: int | None = Field(default=None, ge=0)
    applied_command_id: str | None = Field(default=None, min_length=1, max_length=128)
    monitor_installation_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class MonitorRuntimeChannel(BaseModel):
    model_config = ConfigDict(frozen=True)

    channel: Literal["pushdeer", "pushplus"]
    channel_label: str
    mode: Literal["shadow", "live"]
    covered_from: AwareUtcDatetime
    covered_through: AwareUtcDatetime
    logical_count: int = Field(ge=0)
    member_attempts: int = Field(ge=0)
    member_retries: int = Field(ge=0)
    physical_requests: int = Field(ge=0)
    accepted_count: int = Field(ge=0)
    rejected_count: int = Field(ge=0)
    physical_unknown_count: int = Field(ge=0)
    possible_requests: int = Field(ge=0)
    accepted_pct: float | None = Field(ge=0, le=100)
    last_accepted_at: AwareUtcDatetime | None


class MonitorRuntimeData(BaseModel):
    model_config = ConfigDict(frozen=True)

    state: Literal["ready", "unavailable"]
    source_label: str
    source_note: str
    observed_at: AwareUtcDatetime | None = None
    mode: Literal["shadow", "live", "unknown"] = "unknown"
    mode_label: str = "未确认"
    applied_revision: int | None = Field(default=None, ge=0)
    applied_command_id: str | None = Field(default=None, min_length=1, max_length=128)
    monitor_installation_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    builtins: list[MonitorBuiltinStatus] = Field(default_factory=list, max_length=4)
    channels: list[MonitorRuntimeChannel] = Field(default_factory=list, max_length=2)


class MonitorBuiltinTrigger(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["builtin"] = "builtin"
    event_key: str
    at: AwareUtcDatetime
    builtin_id: BuiltinId
    event_label: str
    subject: Literal["stock", "market"]
    code: str | None
    name: str | None
    price: float | None = None
    threshold: float | None = None
    threshold_unit: Literal["CNY", "multiple"] | None = None
    before: float | None = None
    after: float | None = None
    comparison_unit: Literal["count", "percent"] | None = None
    source_note: str
    acknowledgment: AlertAcknowledgmentView


class MonitorConditionTrigger(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["condition"] = "condition"
    event_key: str
    at: AwareUtcDatetime
    code: str
    name: str | None
    event_label: str
    status_label: str
    source_note: str
    acknowledgment: AlertAcknowledgmentView = Field(default_factory=lambda: AlertAcknowledgmentView(
        state="unavailable", eligible=False, label="确认状态暂不可用"))


class MonitorChannelAttempt(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["channel_attempt"] = "channel_attempt"
    event_key: str
    at: AwareUtcDatetime
    channel_label: str
    mode: Literal["shadow", "live"]
    state: Literal["waiting", "sending", "shadow", "accepted", "rejected", "unknown", "possible"]
    state_label: str
    logical_count: int = Field(ge=1, le=100)
    attempt_no: int | None = Field(default=None, ge=1, le=5)
    source_note: str


MonitorTimelineItem = Annotated[
    MonitorSignal | MonitorTrigger | MonitorSurge | MonitorNotification
    | MonitorBuiltinTrigger | MonitorConditionTrigger | MonitorChannelAttempt,
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
    builtin_unacknowledged: UnacknowledgedSummary = Field(default_factory=UnacknowledgedSummary)
