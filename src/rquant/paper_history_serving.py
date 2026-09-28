"""Complete, bounded Serving projections from a trusted paper ledger window."""

from __future__ import annotations

from rquant.paper_broker import PaperOrderHistorySnapshot
from rquant.serving_read_models import ServingProjectionPayload


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
