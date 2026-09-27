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
    canvases: list[SavedCanvas]
    canvases_truncated: bool
    pools: list[PublishedPool]
    pools_truncated: bool
