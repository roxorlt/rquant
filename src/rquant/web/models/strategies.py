"""Published strategy definitions and parameters, with no execution authority."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class StrategyParameter(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    label: str
    display_value: str


class StrategyCatalogItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str
    name: str
    version: int
    registered_at: datetime
    parameters: list[StrategyParameter]


class StrategyCatalogData(BaseModel):
    model_config = ConfigDict(frozen=True)

    available: bool
    strategies: list[StrategyCatalogItem]
