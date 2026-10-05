"""Ownerless price-rule Web contracts. Decimal input remains text."""

from __future__ import annotations

from datetime import datetime, time
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, model_validator

from rquant.alert_price_rule import PriceAlertRule
from rquant.manual_watchlist import TsCode
from rquant.price_alert_rule_store import RuleId
from rquant.runtime_contracts import AwareUtcDatetime


class _PrivateModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class PriceAlertRuleInput(_PrivateModel):
    name: StrictStr = Field(min_length=1, max_length=80)
    priority: Literal["P0", "P1", "P2", "P3"]
    enabled: StrictBool
    comparison: Literal["gte", "lte"]
    threshold: StrictStr = Field(min_length=1, max_length=1024)
    valid_from: time
    valid_until: time

    def domain_rule(self, rule_id: str) -> PriceAlertRule:
        return PriceAlertRule(rule_id=rule_id, **self.model_dump())

    @model_validator(mode="after")
    def validate_domain_fields(self) -> Self:
        self.domain_rule("validation-only")
        return self


class PriceAlertRuleCommandRequest(_PrivateModel):
    command_id: StrictStr = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime
    generation_id: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    rule_id: RuleId
    action: Literal["save", "set_enabled", "delete"]
    expected_version: StrictInt | None = Field(default=None, ge=1)
    ts_code: TsCode | None = None
    membership_version: StrictInt | None = Field(default=None, ge=1)
    rule: PriceAlertRuleInput | None = None
    enabled: StrictBool | None = None

    @model_validator(mode="after")
    def require_action_fields(self) -> Self:
        if self.action == "save":
            if self.ts_code is None or self.membership_version is None or self.rule is None:
                raise ValueError("save lacks its full rule and scope")
            if "enabled" in self.model_fields_set:
                raise ValueError("save has enable-only fields")
        else:
            if self.expected_version is None:
                raise ValueError("existing rule action requires a version")
            if {"ts_code", "membership_version", "rule"} & self.model_fields_set:
                raise ValueError("non-save action has save-only fields")
            if self.action == "set_enabled" and self.enabled is None:
                raise ValueError("enable action lacks its intent")
            if self.action == "delete" and "enabled" in self.model_fields_set:
                raise ValueError("delete has enable-only fields")
        return self


class PriceAlertRuleItem(_PrivateModel):
    rule_id: str
    version: int = Field(ge=1)
    ts_code: str
    membership_version: int = Field(ge=1)
    name: str
    priority: Literal["P0", "P1", "P2", "P3"]
    priority_label: str
    enabled: bool
    comparison: Literal["gte", "lte"]
    threshold: str
    valid_from: str
    valid_until: str
    updated_at: datetime
    scope_status: Literal["bound", "disabled", "removed", "expired", "changed", "unavailable"]
    scope_message: str
    status_label: Literal["未运行"] = "未运行"


class PriceAlertRuleMember(_PrivateModel):
    ts_code: str
    version: int = Field(ge=1)
    expires_at: datetime | None


class PriceAlertRulePriorityOption(_PrivateModel):
    value: Literal["P0", "P1", "P2", "P3"]
    label: str


class PriceAlertRuleListData(_PrivateModel):
    availability: Literal["ready", "not_activated", "unavailable"]
    message: str
    available_at: datetime | None
    items: list[PriceAlertRuleItem] = Field(max_length=100)
    members: list[PriceAlertRuleMember] = Field(max_length=500)
    can_write: bool
    write_message: str
    priority_options: list[PriceAlertRulePriorityOption] = Field(max_length=4)


class PriceAlertRuleHeadData(_PrivateModel):
    availability: Literal["ready", "not_activated", "unavailable"]
    message: str
    rule_id: str
    status: Literal["live", "deleted", "absent"] | None
    version: int | None
    item: PriceAlertRuleItem | None


class PriceAlertRuleCommandReceipt(_PrivateModel):
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
