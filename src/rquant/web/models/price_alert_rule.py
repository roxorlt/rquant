"""Private price-rule read and command contracts."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt

from rquant.alert_price_rule import PriceAlertRule
from rquant.manual_watchlist import TsCode
from rquant.price_alert_rule_store import RuleId
from rquant.runtime_contracts import AwareUtcDatetime


class _PrivateModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class PriceAlertRuleItemData(_PrivateModel):
    rule_id: RuleId
    version: int = Field(ge=1)
    deleted: bool
    ts_code: TsCode | None
    membership_version: int | None = Field(ge=1)
    name: str | None
    priority: Literal["P0", "P1", "P2", "P3"] | None
    enabled: bool | None
    comparison: Literal["gte", "lte"] | None
    threshold: str | None
    valid_from: str | None
    valid_until: str | None
    scope_status: Literal["valid", "expired", "removed", "changed", "deleted"]
    updated_at: datetime


class PriceAlertRuleListData(_PrivateModel):
    availability: Literal["ready", "not_ready", "unavailable"]
    message: str
    available_at: datetime | None
    evaluation_running: Literal[False] = False
    items: list[PriceAlertRuleItemData] = Field(max_length=10_000)


class _CommandBase(_PrivateModel):
    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")


class SavePriceAlertRuleRequest(_CommandBase):
    kind: Literal["save_price_alert_rule"]
    ts_code: TsCode
    membership_version: StrictInt = Field(ge=1)
    expected_version: StrictInt | None = Field(default=None, ge=1)
    rule: PriceAlertRule


class SetPriceAlertRuleEnabledRequest(_CommandBase):
    kind: Literal["set_price_alert_rule_enabled"]
    rule_id: RuleId
    expected_version: StrictInt = Field(ge=1)
    enabled: StrictBool


class DeletePriceAlertRuleRequest(_CommandBase):
    kind: Literal["delete_price_alert_rule"]
    rule_id: RuleId
    expected_version: StrictInt = Field(ge=1)


PriceAlertRuleCommandRequest = Annotated[
    SavePriceAlertRuleRequest | SetPriceAlertRuleEnabledRequest | DeletePriceAlertRuleRequest,
    Field(discriminator="kind"),
]

PriceAlertRuleConflictReason = Literal[
    "command_conflict",
    "generation_changed",
    "version_conflict",
    "membership_changed",
    "capacity_exceeded",
]


class PriceAlertRuleCommandReceipt(_PrivateModel):
    command_id: str
    kind: Literal[
        "save_price_alert_rule", "set_price_alert_rule_enabled", "delete_price_alert_rule"
    ]
    rule_id: RuleId
    status: Literal[
        "pending",
        "processing",
        "saved_syncing",
        "published",
        "conflict",
        "capacity",
        "failed",
        "uncertain",
    ]
    version: int | None = Field(default=None, ge=1)
    reason: PriceAlertRuleConflictReason | None = None
    message: str
