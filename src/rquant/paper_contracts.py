"""Immutable contracts for an independent paper-broker ledger."""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Self

from pydantic import Field, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
PositiveLot = Annotated[int, Field(gt=0, multiple_of=100)]
NonNegativeLot = Annotated[int, Field(ge=0, multiple_of=100)]
PositiveDecimal = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
NonNegativeDecimal = Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
FiniteDecimal = Annotated[Decimal, Field(allow_inf_nan=False)]


class PaperSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PaperOrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class PaperOrderStatus(StrEnum):
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class PaperRejectReason(StrEnum):
    T_PLUS_ONE = "T_PLUS_ONE"
    SUSPENDED = "SUSPENDED"
    LIMIT_LOCKED = "LIMIT_LOCKED"
    INSUFFICIENT_CASH = "INSUFFICIENT_CASH"
    INSUFFICIENT_POSITION = "INSUFFICIENT_POSITION"
    INVALID_LOT = "INVALID_LOT"
    EXPIRED = "EXPIRED"
    RISK_REJECTED = "RISK_REJECTED"


class PaperOrderIntent(RuntimeContractModel):
    intent_id: Sha256 | None = None
    signal_id: Sha256
    account_id: str = Field(min_length=1)
    ts_code: str = Field(min_length=1)
    side: PaperSide
    order_type: PaperOrderType
    quantity: PositiveLot
    limit_price: PositiveDecimal | None = None
    event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    earliest_execution_at: AwareUtcDatetime
    price_snapshot_id: Sha256
    producer_commit: CommitSha

    @model_validator(mode="after")
    def validate_intent(self) -> Self:
        if self.order_type is PaperOrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit_price is required for LIMIT orders")
        if self.order_type is PaperOrderType.MARKET and self.limit_price is not None:
            raise ValueError("limit_price is only valid for LIMIT orders")
        if self.event_time > self.available_at:
            raise ValueError("event_time must be before or equal to available_at")
        if self.available_at >= self.expires_at:
            raise ValueError("expires_at must be later than available_at")
        if self.earliest_execution_at < self.available_at:
            raise ValueError("earliest_execution_at must be after or equal to available_at")
        if self.earliest_execution_at >= self.expires_at:
            raise ValueError("earliest_execution_at must be earlier than expires_at")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"intent_id"}))
        if self.intent_id is None:
            object.__setattr__(self, "intent_id", expected)
        elif self.intent_id != expected:
            raise ValueError("intent_id does not match canonical intent content")
        return self


class PaperOrder(RuntimeContractModel):
    order_id: Sha256 | None = None
    intent_id: Sha256
    account_id: str = Field(min_length=1)
    ts_code: str = Field(min_length=1)
    side: PaperSide
    order_type: PaperOrderType
    quantity: PositiveLot
    filled_quantity: NonNegativeLot = 0
    average_fill_price: PositiveDecimal | None = None
    status: PaperOrderStatus
    reject_reason: PaperRejectReason | None = None
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.filled_quantity > self.quantity:
            raise ValueError("filled_quantity cannot exceed quantity")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at cannot be earlier than created_at")

        has_fills = self.filled_quantity > 0
        if has_fills != (self.average_fill_price is not None):
            raise ValueError("average_fill_price must be present iff fills exist")

        if self.status in {PaperOrderStatus.PENDING, PaperOrderStatus.ACCEPTED}:
            if self.filled_quantity != 0:
                raise ValueError(f"{self.status.value} order cannot have fills")
        elif self.status is PaperOrderStatus.PARTIALLY_FILLED:
            if not 0 < self.filled_quantity < self.quantity:
                raise ValueError(
                    "PARTIALLY_FILLED requires filled_quantity between zero and quantity"
                )
        elif self.status is PaperOrderStatus.FILLED:
            if self.filled_quantity != self.quantity:
                raise ValueError("FILLED requires filled_quantity equal to quantity")
        elif self.status is PaperOrderStatus.REJECTED:
            if self.filled_quantity != 0:
                raise ValueError("REJECTED order cannot have fills")
            if self.reject_reason is None:
                raise ValueError("reject_reason is required for REJECTED orders")
        elif self.filled_quantity >= self.quantity:
            raise ValueError(f"{self.status.value} requires unfilled quantity to remain")

        if self.status is not PaperOrderStatus.REJECTED and self.reject_reason is not None:
            raise ValueError("reject_reason is only valid for REJECTED orders")

        expected = canonical_sha256({"account_id": self.account_id, "intent_id": self.intent_id})
        if self.order_id is None:
            object.__setattr__(self, "order_id", expected)
        elif self.order_id != expected:
            raise ValueError("order_id does not match intent_id and account_id")
        return self


class PaperFill(RuntimeContractModel):
    fill_id: Sha256 | None = None
    order_id: Sha256
    sequence: int = Field(ge=1)
    quantity: PositiveLot
    price: PositiveDecimal
    commission: NonNegativeDecimal
    tax: NonNegativeDecimal
    executed_at: AwareUtcDatetime
    price_snapshot_id: Sha256

    @model_validator(mode="after")
    def validate_fill_id(self) -> Self:
        expected = canonical_sha256({"order_id": self.order_id, "sequence": self.sequence})
        if self.fill_id is None:
            object.__setattr__(self, "fill_id", expected)
        elif self.fill_id != expected:
            raise ValueError("fill_id does not match order_id and sequence")
        return self

    @property
    def notional(self) -> Decimal:
        return self.price * self.quantity


class PaperHolding(RuntimeContractModel):
    code: str = Field(min_length=1)
    quantity: NonNegativeLot
    available_quantity: NonNegativeLot
    frozen_quantity: NonNegativeLot
    average_cost: NonNegativeDecimal
    market_price: NonNegativeDecimal

    @model_validator(mode="after")
    def reconcile_quantities(self) -> Self:
        if self.quantity != self.available_quantity + self.frozen_quantity:
            raise ValueError("quantity must equal available_quantity plus frozen_quantity")
        if self.quantity > 0 and self.average_cost <= 0:
            raise ValueError("average_cost must be positive for a non-empty holding")
        if self.quantity > 0 and self.market_price <= 0:
            raise ValueError("market_price must be positive for a non-empty holding")
        return self


class PaperAccountSnapshot(RuntimeContractModel):
    snapshot_id: Sha256 | None = None
    account_id: str = Field(min_length=1)
    as_of_time: AwareUtcDatetime
    cash: NonNegativeDecimal
    available_cash: NonNegativeDecimal
    frozen_cash: NonNegativeDecimal
    holdings: tuple[PaperHolding, ...] = ()
    realized_pnl: FiniteDecimal
    unrealized_pnl: FiniteDecimal
    nav: NonNegativeDecimal

    @model_validator(mode="after")
    def reconcile_account(self) -> Self:
        if self.cash != self.available_cash + self.frozen_cash:
            raise ValueError("cash must equal available_cash plus frozen_cash")

        codes = [holding.code for holding in self.holdings]
        if len(codes) != len(set(codes)):
            raise ValueError("holdings must contain unique code values")
        ordered_holdings = tuple(sorted(self.holdings, key=lambda item: item.code))
        if ordered_holdings != self.holdings:
            object.__setattr__(self, "holdings", ordered_holdings)

        expected_unrealized = sum(
            (
                (holding.market_price - holding.average_cost) * holding.quantity
                for holding in self.holdings
            ),
            Decimal("0"),
        )
        if self.unrealized_pnl != expected_unrealized:
            raise ValueError("unrealized_pnl must reconcile to holding market values and costs")

        holdings_value = sum(
            (holding.market_price * holding.quantity for holding in self.holdings),
            Decimal("0"),
        )
        expected_nav = self.cash + holdings_value
        if self.nav != expected_nav:
            raise ValueError("nav must equal cash plus holdings market value")

        expected_id = canonical_sha256(self.model_dump(mode="python", exclude={"snapshot_id"}))
        if self.snapshot_id is None:
            object.__setattr__(self, "snapshot_id", expected_id)
        elif self.snapshot_id != expected_id:
            raise ValueError("snapshot_id does not match reconciled account content")
        return self
