"""Private full-condition editor contracts from the shared domain definition."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, model_validator

from rquant.alert_rule_contracts import ConditionAlertRuleDefinition, ConditionAlertScope
from rquant.condition_alert_route import ConditionAlertRouteReceipt
from rquant.condition_alert_runtime_contracts import ConditionAlertEventEnvelope
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.web.models.screen import ScreenBlock, ScreenOption


class _PrivateModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ConditionAlertRuleCommandRequest(_PrivateModel):
    command_id: StrictStr = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime
    generation_id: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    rule_id: StrictStr = Field(min_length=1, max_length=128)
    action: Literal["save", "set_enabled", "delete"]
    expected_version: StrictInt | None = Field(default=None, ge=1)
    rule: ConditionAlertRuleDefinition | None = None
    enabled: StrictBool | None = None

    @model_validator(mode="after")
    def closed_intent(self) -> Self:
        if self.action == "save":
            if (
                self.rule is None
                or self.rule.rule_id != self.rule_id
                or "enabled" in self.model_fields_set
            ):
                raise ValueError("condition save lacks its exact complete definition")
        elif (
            self.expected_version is None
            or "rule" in self.model_fields_set
            or (self.action == "set_enabled" and self.enabled is None)
            or (self.action == "delete" and "enabled" in self.model_fields_set)
        ):
            raise ValueError("condition action has missing or extra intent fields")
        return self


class ConditionAlertScopeOption(_PrivateModel):
    label: str
    scope: ConditionAlertScope
    available: bool
    member_count: int | None = Field(default=None, ge=0)
    message: str = ""


class ConditionAlertRuleItem(_PrivateModel):
    rule_id: str
    version: int = Field(ge=1)
    rule: ConditionAlertRuleDefinition
    updated_at: datetime
    scope_status: Literal["bound", "unavailable"]
    scope_message: str
    status_label: Literal["正常", "注意", "异常", "未运行", "等待开盘"] = "未运行"
    evaluated_at: datetime | None = None
    matched_count: int | None = Field(default=None, ge=0)
    unknown_count: int | None = Field(default=None, ge=0)
    ranking_digest: str | None = None
    last_triggered_at: datetime | None = None


class ConditionAlertTargetReceipt(_PrivateModel):
    channel: Literal["pushdeer", "pushplus"]
    recipient_id: str
    state: Literal["pending", "succeeded", "failed", "unknown", "cancelled"]
    attempted_at: datetime | None
    provider_receipt: str | None


class ConditionAlertTriggerItem(_PrivateModel):
    event: ConditionAlertEventEnvelope
    route: ConditionAlertRouteReceipt
    global_sequence: int = Field(ge=1)
    delivery_state: Literal["pending", "succeeded", "failed", "unknown", "cancelled"]
    attempted_at: datetime | None = None
    provider_receipt: str | None = None
    targets: list[ConditionAlertTargetReceipt] = Field(default_factory=list, max_length=2)

    @model_validator(mode="after")
    def exact_original_trigger(self) -> Self:
        event, route = self.event, self.route
        if (
            event.event_id,
            event.owner_id,
            event.rule_id,
            event.rule_version,
            event.scope_version,
        ) != (
            route.event_id,
            route.owner_id,
            route.rule_id,
            route.rule_version,
            route.scope_version,
        ):
            raise ValueError("condition trigger differs from its actual original route")
        targets = tuple((t.channel, t.recipient_id) for t in self.targets)
        if targets != tuple(sorted(set(targets))) or targets != tuple(
            sorted((t.channel.value, t.recipient_id) for t in route.targets)
        ):
            raise ValueError("condition trigger delivery targets differ from its original route")
        return self


class ConditionAlertRuleListData(_PrivateModel):
    availability: Literal["ready", "not_activated", "unavailable"]
    message: str
    available_at: datetime | None
    items: list[ConditionAlertRuleItem] = Field(max_length=100)
    scopes: list[ConditionAlertScopeOption] = Field(max_length=1000)
    blocks: list[ScreenBlock] = Field(max_length=26)
    ranking_metrics: list[ScreenOption]
    can_write: bool
    write_message: str
    can_enable: bool
    enable_message: str
    triggers: list[ConditionAlertTriggerItem] = Field(max_length=100)


class ConditionAlertRuleHeadData(_PrivateModel):
    availability: Literal["ready", "not_activated", "unavailable"]
    rule_id: str
    status: Literal["live", "deleted", "absent"] | None
    version: int | None
    item: ConditionAlertRuleItem | None


class ConditionAlertRuleCommandReceipt(_PrivateModel):
    command_id: str
    rule_id: str
    action: Literal["save", "set_enabled", "delete"]
    status: Literal[
        "pending",
        "processing",
        "saved_syncing",
        "published",
        "superseded",
        "conflict",
        "capacity",
        "scope_invalid",
        "failed",
        "rejected",
        "uncertain",
        "not_found",
    ]
    version: int | None = Field(default=None, ge=1)
    message: str
    consumer_state: Literal["active", "unknown"] = "unknown"
