"""Published paper accounts for the first read-only web view."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.paper_contracts import (
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectReason,
    PaperSide,
)
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


class PaperFillItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    fill_id: str
    sequence: int = Field(ge=1)
    quantity: int = Field(gt=0)
    price: Decimal = Field(gt=0, allow_inf_nan=False)
    commission: Decimal = Field(ge=0, allow_inf_nan=False)
    transfer_fee: Decimal | None = Field(ge=0, allow_inf_nan=False)
    tax: Decimal = Field(ge=0, allow_inf_nan=False)
    total_fees: Decimal | None = Field(ge=0, allow_inf_nan=False)
    executed_at: AwareUtcDatetime
    persisted_at: AwareUtcDatetime


class PaperOrderItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: str
    code: str
    name: str | None
    side: PaperSide
    side_label: str
    order_type: PaperOrderType
    quantity: int = Field(gt=0)
    filled_quantity: int = Field(ge=0)
    average_fill_price: Decimal | None = Field(gt=0, allow_inf_nan=False)
    status: PaperOrderStatus
    status_label: str
    reject_reason: PaperRejectReason | None
    reject_message: str | None
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    fills: list[PaperFillItem] = Field(max_length=1_000)


class PaperHistoryData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_state: Literal["ready", "empty", "not_published", "unavailable"]
    source_updated_at: AwareUtcDatetime | None
    source_note: str | None
    account_id: str | None
    total_orders: int | None = Field(ge=0)
    has_more: bool
    newest_updated_at: AwareUtcDatetime | None
    oldest_updated_at: AwareUtcDatetime | None
    orders: list[PaperOrderItem] = Field(max_length=200)


class PaperAccountsData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_state: Literal["ready", "empty", "not_published", "unavailable"]
    source_updated_at: AwareUtcDatetime | None
    source_note: str | None
    valuation_note: str | None
    accounts: list[PaperAccountItem]
    history: PaperHistoryData
