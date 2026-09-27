"""Published paper accounts for the first read-only web view."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.runtime_contracts import AwareUtcDatetime


class PaperHoldingItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: str
    name: str | None
    quantity: int = Field(ge=0)
    available_quantity: int = Field(ge=0)
    average_cost: float = Field(ge=0, allow_inf_nan=False)
    market_price: float = Field(ge=0, allow_inf_nan=False)
    market_value: float = Field(ge=0, allow_inf_nan=False)
    unrealized_pnl: float = Field(allow_inf_nan=False)
    unrealized_pct: float | None = Field(allow_inf_nan=False)


class PaperAccountItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    account_id: str
    as_of: AwareUtcDatetime
    nav: float = Field(ge=0, allow_inf_nan=False)
    cash: float = Field(ge=0, allow_inf_nan=False)
    market_value: float = Field(ge=0, allow_inf_nan=False)
    unrealized_pnl: float = Field(allow_inf_nan=False)
    holdings: list[PaperHoldingItem]


class PaperAccountsData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_state: Literal["ready", "empty", "not_published", "unavailable"]
    source_updated_at: AwareUtcDatetime | None
    source_note: str | None
    valuation_note: str | None
    accounts: list[PaperAccountItem]
