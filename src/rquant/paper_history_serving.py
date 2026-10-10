"""Complete, bounded Serving projections from a trusted paper ledger window."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import Field

from rquant.paper_broker import PaperOrderHistorySnapshot
from rquant.paper_contracts import PaperOrderStatus
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.serving_read_models import ServingProjectionPayload


class PaperRetainedOrderSummary(RuntimeContractModel):
    window_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    retained_count: int = Field(strict=True, ge=0)
    rejected_count: int = Field(strict=True, ge=0)
    total_orders: int = Field(strict=True, ge=0)
    rejection_ratio: Decimal | None
    oldest_updated_at: datetime
    newest_updated_at: datetime


def paper_history_summary(snapshot: PaperOrderHistorySnapshot) -> PaperRetainedOrderSummary:
    snapshot = PaperOrderHistorySnapshot.model_validate(snapshot.model_dump(mode="python"))
    if any(
        order.updated_at > snapshot.as_of or order.created_at > snapshot.as_of
        for order in snapshot.orders
    ):
        raise ValueError("paper retained order window contains future material")
    retained = len(snapshot.orders)
    rejected = sum(order.status is PaperOrderStatus.REJECTED for order in snapshot.orders)
    return PaperRetainedOrderSummary(
        window_identity=canonical_sha256(snapshot),
        retained_count=retained,
        rejected_count=rejected,
        total_orders=snapshot.total_orders,
        rejection_ratio=None if not retained else Decimal(rejected) / Decimal(retained),
        oldest_updated_at=min(
            (order.updated_at for order in snapshot.orders), default=snapshot.as_of
        ),
        newest_updated_at=max(
            (order.updated_at for order in snapshot.orders), default=snapshot.as_of
        ),
    )


def paper_history_projections(
    snapshot: PaperOrderHistorySnapshot,
) -> tuple[ServingProjectionPayload, ServingProjectionPayload, ServingProjectionPayload]:
    at = snapshot.as_of
    newest = None if not snapshot.orders else snapshot.orders[0].updated_at
    oldest = None if not snapshot.orders else snapshot.orders[-1].updated_at
    window = ServingProjectionPayload(
        table_name="paper_order_window",
        available_at=at,
        rows=(
            {
                "snapshot_key": "recent",
                "account_id": snapshot.account_id,
                "as_of_time": at.isoformat().replace("+00:00", "Z"),
                "price_tick": str(snapshot.price_tick),
                "total_orders": snapshot.total_orders,
                "retained_orders": len(snapshot.orders),
                "retained_fills": len(snapshot.fills),
                "has_more": snapshot.has_more,
                "newest_updated_at": (
                    None if newest is None else newest.isoformat().replace("+00:00", "Z")
                ),
                "oldest_updated_at": (
                    None if oldest is None else oldest.isoformat().replace("+00:00", "Z")
                ),
            },
        ),
    )
    orders = ServingProjectionPayload(
        table_name="paper_order_history",
        available_at=at,
        rows=tuple(order.model_dump(mode="json") for order in snapshot.orders),
    )
    fills = ServingProjectionPayload(
        table_name="paper_fill_history",
        available_at=at,
        rows=tuple(fill.model_dump(mode="json") for fill in snapshot.fills),
    )
    return window, orders, fills
