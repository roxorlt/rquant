"""Bounded, read-only view of published daily pools and saved canvas references."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict


class PoolMember(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: str
    name: str | None
    close: float | None
    pct_chg: float | None


class PoolStep(BaseModel):
    model_config = ConfigDict(frozen=True)

    step_index: int
    label: str
    count: int


class PoolRuleParameter(BaseModel):
    model_config = ConfigDict(frozen=True)

    label: str
    value: str


class PoolRuleItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    label: str
    parameters: list[PoolRuleParameter]


class PoolDefinitionView(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    state: Literal["available", "migration_required", "unavailable", "deleted", "limit_exceeded"]
    status_label: str
    reason_label: str | None
    source_label: str
    description: str | None
    depends_on: str | None
    delay_label: str | None
    rules: list[PoolRuleItem]


class PoolResultView(BaseModel):
    model_config = ConfigDict(frozen=True)

    state: Literal[
        "current_rules", "older_rules", "rules_changed", "unverified", "not_run", "unavailable"
    ]
    status_label: str
    trade_date: date | None
    hit_count: int | None
    zero_hit_label: str | None = None


class PublishedPool(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    name: str
    state: Literal["current", "older", "no_data", "unpublished", "unavailable"]
    trade_date: date | None
    member_count: int | None
    steps: list[PoolStep]
    steps_truncated: bool
    members: list[PoolMember]
    members_truncated: bool
    definition: PoolDefinitionView | None = None
    result: PoolResultView


class SavedCanvas(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    description: str
    pool_keys: list[str]
    refs_truncated: bool


class PoolsData(BaseModel):
    model_config = ConfigDict(frozen=True)

    state: Literal["ready", "no_data", "unavailable"]
    latest_trade_date: date | None
    definitions_available: bool
    rules_available: bool = False
    canvases: list[SavedCanvas]
    canvases_truncated: bool
    pools: list[PublishedPool]
    pools_truncated: bool
