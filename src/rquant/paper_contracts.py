"""Immutable contracts for an independent paper-broker ledger."""

from __future__ import annotations

from decimal import ROUND_FLOOR, Decimal
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import Field, computed_field, model_validator

from rquant.research_run_spec import ExecutionCostCalculation
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
TrancheFraction = Annotated[Decimal, Field(gt=0, le=1, allow_inf_nan=False)]


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


class PaperCostProvenanceState(StrEnum):
    KNOWN_V3 = "KNOWN_V3"
    LEGACY_UNKNOWN = "LEGACY_UNKNOWN"


class PaperLedgerAnchorClaims(RuntimeContractModel):
    contract: Literal["rquant-paper-ledger-head/v2"] = "rquant-paper-ledger-head/v2"
    ledger_id: str = Field(min_length=1)
    schema_version: Literal[5]
    migration_attestation_digest: Sha256
    head_revision: int = Field(ge=1)
    head_marker_fingerprint: Sha256
    attestation_fingerprint: Sha256
    financial_state_digest: Sha256
    key_id: str = Field(min_length=1)
    issued_at: AwareUtcDatetime


class PaperLedgerAnchor(RuntimeContractModel):
    claims: PaperLedgerAnchorClaims
    signature: str = Field(min_length=1)


class PaperLedgerArchiveTableBinding(RuntimeContractModel):
    table: str = Field(min_length=1)
    columns: tuple[str, ...] = Field(min_length=1)
    source_key_ordering: tuple[str, ...] = Field(min_length=1)


class PaperLedgerArchiveBinding(RuntimeContractModel):
    source_sha256: Sha256
    archive_tables: tuple[PaperLedgerArchiveTableBinding, ...] = Field(min_length=1)
    predecessor_v4_schema_fingerprint: Sha256
    predecessor_v4_attestation_fingerprint: Sha256
    predecessor_v4_head_marker_fingerprint: Sha256
    source_schema_identity: str = Field(min_length=1)


class PaperLedgerMigrationAttestation(RuntimeContractModel):
    contract: Literal["rquant-paper-ledger-migration/v2"] = "rquant-paper-ledger-migration/v2"
    source_sha256: Sha256
    predecessor_v4_schema_fingerprint: Sha256
    predecessor_v4_attestation_fingerprint: Sha256
    predecessor_v4_head_marker_fingerprint: Sha256
    archive_binding_fingerprint: Sha256
    archive_digest: Sha256
    v4_reconciliation_report_digest: Sha256
    migration_code_identity: str = Field(min_length=1)
    migration_algorithm_id: Literal["paper-ledger-v4-to-v5-archive-v2"] = (
        "paper-ledger-v4-to-v5-archive-v2"
    )
    source_schema_identity: str = Field(min_length=1)
    target_schema_identity: str = Field(min_length=1)
    target_schema_version: Literal[5] = 5
    target_internal_migration_version: Literal[4] = 4

    @computed_field  # type: ignore[prop-decorator]
    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="python", exclude={"digest"}))


class PaperExecutionCostComparison(RuntimeContractModel):
    """Audit output from one reconciled and externally anchored ledger snapshot."""

    is_comparable: bool
    reason: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    execution_ids: tuple[str, ...]
    ledger_generation: Sha256 | None = None
    head_revision: int | None = Field(default=None, ge=1)
    head_marker_fingerprint: Sha256 | None = None
    attestation_fingerprint: Sha256 | None = None
    migration_attestation_digest: Sha256 | None = None
    financial_state_digest: Sha256 | None = None
    reconciliation_digest: Sha256 | None = None


class PaperSellQuantityAuthority(RuntimeContractModel):
    snapshot_id: Sha256 | None = None
    exit_signal_id: Sha256
    entry_signal_id: Sha256
    account_id: str = Field(min_length=1)
    ts_code: str = Field(min_length=1)
    action: Literal["REDUCE", "S_INTENT"]
    decision_cutoff: AwareUtcDatetime
    remaining_quantity: PositiveLot
    available_quantity: NonNegativeLot
    tranche_fraction: TrancheFraction
    requested_quantity: PositiveLot
    source_lot_fingerprint: Sha256

    @model_validator(mode="after")
    def validate_quantity_semantics(self) -> Self:
        if self.available_quantity > self.remaining_quantity:
            raise ValueError("available_quantity cannot exceed remaining_quantity")
        if self.action == "S_INTENT":
            if self.tranche_fraction != Decimal("1"):
                raise ValueError("S_INTENT tranche_fraction must equal one")
            expected = self.remaining_quantity
        else:
            if self.tranche_fraction >= Decimal("1"):
                raise ValueError("REDUCE tranche_fraction must be below one")
            shares = int(
                (Decimal(self.remaining_quantity) * self.tranche_fraction).to_integral_value(
                    rounding=ROUND_FLOOR
                )
            )
            expected = shares // 100 * 100
            if expected <= 0 or expected >= self.remaining_quantity:
                raise ValueError("REDUCE has no legal partial 100-share lot")
        if self.requested_quantity != expected:
            raise ValueError("requested_quantity does not match tranche semantics")
        expected_id = canonical_sha256(self.model_dump(mode="python", exclude={"snapshot_id"}))
        if self.snapshot_id is None:
            object.__setattr__(self, "snapshot_id", expected_id)
        elif self.snapshot_id != expected_id:
            raise ValueError("sell quantity snapshot_id does not match authority content")
        return self


class PaperOrderIntent(RuntimeContractModel):
    intent_id: Sha256 | None = None
    signal_id: Sha256
    entry_signal_id: Sha256 | None = None
    sell_quantity_authority: PaperSellQuantityAuthority | None = None
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
        if self.side is PaperSide.BUY and self.entry_signal_id is not None:
            raise ValueError("BUY intent entry_signal_id must be absent")
        if self.side is PaperSide.BUY and self.sell_quantity_authority is not None:
            raise ValueError("BUY intent sell_quantity_authority must be absent")
        if self.side is PaperSide.SELL and self.entry_signal_id is None:
            raise ValueError("SELL intent entry_signal_id is required")
        if self.side is PaperSide.SELL and self.sell_quantity_authority is None:
            raise ValueError("SELL intent sell_quantity_authority is required")
        if self.sell_quantity_authority is not None and (
            self.sell_quantity_authority.exit_signal_id != self.signal_id
            or self.sell_quantity_authority.entry_signal_id != self.entry_signal_id
            or self.sell_quantity_authority.account_id != self.account_id
            or self.sell_quantity_authority.ts_code != self.ts_code
            or self.sell_quantity_authority.requested_quantity != self.quantity
        ):
            raise ValueError("SELL intent does not match sell quantity authority")
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
    execution_id: Sha256
    order_id: Sha256
    sequence: int = Field(ge=1)
    quantity: PositiveLot
    price: PositiveDecimal
    commission: NonNegativeDecimal
    transfer_fee: NonNegativeDecimal | None = None
    tax: NonNegativeDecimal
    total_fees: NonNegativeDecimal | None = None
    cost_spec_id: Sha256 | None = None
    cost_spec_schema_version: Literal[3] | None = None
    cost_context_fingerprint: Sha256 | None = None
    cost_provenance_state: PaperCostProvenanceState = PaperCostProvenanceState.LEGACY_UNKNOWN
    executed_at: AwareUtcDatetime
    price_snapshot_id: Sha256

    @model_validator(mode="after")
    def validate_fill_id(self) -> Self:
        expected = canonical_sha256({"order_id": self.order_id, "execution_id": self.execution_id})
        if self.fill_id is None:
            object.__setattr__(self, "fill_id", expected)
        elif self.fill_id != expected:
            raise ValueError("fill_id does not match order_id and execution_id")
        return self

    @model_validator(mode="after")
    def validate_cost_provenance(self) -> Self:
        values = (
            self.transfer_fee,
            self.total_fees,
            self.cost_spec_id,
            self.cost_spec_schema_version,
            self.cost_context_fingerprint,
        )
        if self.cost_provenance_state is PaperCostProvenanceState.LEGACY_UNKNOWN:
            if any(value is not None for value in values):
                raise ValueError("legacy paper fill must not invent v3 cost provenance")
            return self
        if any(value is None for value in values):
            raise ValueError("known v3 paper fill requires complete cost provenance")
        assert self.transfer_fee is not None
        assert self.total_fees is not None
        if self.total_fees != self.commission + self.transfer_fee + self.tax:
            raise ValueError("paper fill total_fees must equal its fee components")
        return self

    @property
    def notional(self) -> Decimal:
        return self.price * self.quantity


class PaperExecutionReceipt(RuntimeContractModel):
    execution_id: Sha256
    request_fingerprint: Sha256
    intent_id: Sha256
    order: PaperOrder
    fill: PaperFill | None = None
    cost_spec_id: Sha256 | None = None
    cost_spec_schema_version: Literal[3] | None = None
    cost_context_fingerprint: Sha256 | None = None
    cost_provenance_state: PaperCostProvenanceState = PaperCostProvenanceState.LEGACY_UNKNOWN
    cost_calculation: ExecutionCostCalculation | None = None
    persisted_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_execution(self) -> Self:
        if self.order.intent_id != self.intent_id:
            raise ValueError("execution receipt does not bind its intent and order")
        if self.fill is not None and (
            self.fill.execution_id != self.execution_id or self.fill.order_id != self.order.order_id
        ):
            raise ValueError("execution receipt does not bind its order and fill")
        values = (
            self.cost_spec_id,
            self.cost_spec_schema_version,
            self.cost_context_fingerprint,
        )
        if self.cost_provenance_state is PaperCostProvenanceState.LEGACY_UNKNOWN:
            if any(value is not None for value in values) or self.cost_calculation is not None:
                raise ValueError("legacy execution receipt must not invent v3 cost provenance")
            return self
        if any(value is None for value in values):
            raise ValueError("known v3 execution receipt requires complete cost provenance")
        if self.fill is not None:
            if self.cost_calculation is None:
                raise ValueError("known v3 fill receipt requires resolved cost calculation")
            if (
                self.fill.cost_provenance_state is not PaperCostProvenanceState.KNOWN_V3
                or self.fill.cost_spec_id != self.cost_spec_id
                or self.fill.cost_spec_schema_version != self.cost_spec_schema_version
                or self.fill.cost_context_fingerprint != self.cost_context_fingerprint
                or self.fill.price != self.cost_calculation.executed_price
                or self.fill.commission != self.cost_calculation.commission
                or self.fill.transfer_fee != self.cost_calculation.transfer_fee
                or self.fill.tax != self.cost_calculation.stamp_duty
                or self.fill.total_fees != self.cost_calculation.total_fees
            ):
                raise ValueError("known v3 execution receipt does not match its fill")
        if self.cost_calculation is not None and (
            self.cost_calculation.cost_spec_id != self.cost_spec_id
            or self.cost_calculation.cost_spec_schema_version != self.cost_spec_schema_version
            or self.cost_calculation.cost_context_fingerprint != self.cost_context_fingerprint
        ):
            raise ValueError("known v3 execution receipt does not match cost calculation")
        return self


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
