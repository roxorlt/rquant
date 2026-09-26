"""Read-only stock search and stock summary for the global top bar."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class StockSearchRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    ts_code: str
    name: str


class StockSearchData(BaseModel):
    model_config = ConfigDict(frozen=True)

    query: str
    available: bool
    rows: list[StockSearchRow]
    truncated: bool


class StockSummaryData(BaseModel):
    model_config = ConfigDict(frozen=True)

    ts_code: str
    name: str | None
    price: float | None
    as_of: datetime | None
    pools: list[str]
