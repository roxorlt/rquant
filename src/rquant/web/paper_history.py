"""Reconcile the paper order window inside the accounts' Serving lease."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Literal

from rquant.paper_broker import PaperHistoryFill
from rquant.paper_contracts import (
    PaperCostProvenanceState,
    PaperOrder,
    PaperOrderStatus,
    PaperRejectReason,
    PaperSide,
)
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingQueryError
from rquant.web import readers
from rquant.web.models.paper import PaperFillItem, PaperHistoryData, PaperOrderItem
from rquant.web.serving import BorrowedGeneration

_TABLES = ("paper_fill_history", "paper_order_history", "paper_order_window")
_MAX_ORDERS = 200
_MAX_FILLS = 1_000
_ORDER_COLUMNS = (
    "order_id",
    "intent_id",
    "account_id",
    "ts_code",
    "side",
    "order_type",
    "quantity",
    "filled_quantity",
    "average_fill_price",
    "status",
    "reject_reason",
    "created_at",
    "updated_at",
)
_FILL_COLUMNS = (
    "fill_id",
    "execution_id",
    "order_id",
    "sequence",
    "quantity",
    "price",
    "commission",
    "transfer_fee",
    "tax",
    "total_fees",
    "cost_spec_id",
    "cost_spec_schema_version",
    "cost_context_fingerprint",
    "cost_provenance_state",
    "executed_at",
    "price_snapshot_id",
    "persisted_at",
)
_STATUS_LABELS = {
    PaperOrderStatus.PENDING: "等待处理",
    PaperOrderStatus.ACCEPTED: "待成交",
    PaperOrderStatus.PARTIALLY_FILLED: "部分成交",
    PaperOrderStatus.FILLED: "已成交",
    PaperOrderStatus.REJECTED: "未接受",
    PaperOrderStatus.CANCELLED: "已取消",
    PaperOrderStatus.EXPIRED: "已失效",
}
_REJECT_MESSAGES = {
    PaperRejectReason.T_PLUS_ONE: "今天不可卖出",
    PaperRejectReason.SUSPENDED: "股票停牌",
    PaperRejectReason.LIMIT_LOCKED: "暂不可成交",
    PaperRejectReason.INSUFFICIENT_CASH: "可用资金不足",
    PaperRejectReason.INSUFFICIENT_POSITION: "可卖数量不足",
    PaperRejectReason.INVALID_LOT: "指令数量不符合规则",
    PaperRejectReason.EXPIRED: "指令已过期",
    PaperRejectReason.RISK_REJECTED: "未通过风险检查",
}


def empty_history(state: Literal["not_published", "unavailable"]) -> PaperHistoryData:
    return PaperHistoryData(
        source_state=state,
        source_updated_at=None,
        source_note=("指令记录尚未发布。" if state == "not_published" else None),
        account_id=None,
        total_orders=None,
        has_more=False,
        newest_updated_at=None,
        oldest_updated_at=None,
        orders=[],
    )


def _published_at(borrowed: BorrowedGeneration, mark: ServingDatasetWatermark) -> datetime | None:
    statuses = borrowed.cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, "
        "owner_generation_id, available_at FROM projection_status "
        "WHERE table_name IN (?, ?, ?) ORDER BY table_name LIMIT 4",
        _TABLES,
    ).fetchall()
    counts = borrowed.manifest.row_counts
    if not statuses and all(counts.get(name, 0) == 0 for name in _TABLES):
        return None
    if len(statuses) != 3 or tuple(row[0] for row in statuses) != _TABLES:
        raise ValueError("paper history projection status is incomplete")
    if all(not row[1] for row in statuses):
        if any(
            row[1] is not False
            or row[2] != 0
            or row[3] != "paper_accounts"
            or row[4] is not None
            or row[5] is not None
            or counts.get(str(row[0]), 0) != 0
            for row in statuses
        ):
            raise ValueError("unpublished paper history has inconsistent status")
        return None
    at = statuses[0][5]
    if (
        mark.status is FreshnessStatus.UNAVAILABLE
        or not isinstance(at, datetime)
        or at.tzinfo is None
        or at > borrowed.manifest.built_at
        or at > mark.published_at
        or any(
            type(available) is not bool
            or not available
            or type(count) is not int
            or count != counts.get(str(name))
            or owner != "paper_accounts"
            or owner_generation != mark.generation_id
            or available_at != at
            for name, available, count, owner, owner_generation, available_at in statuses
        )
    ):
        raise ValueError("paper history is not from this source generation")
    return at


def _model_rows(
    borrowed: BorrowedGeneration,
    table: str,
    columns: tuple[str, ...],
    limit: int,
    ordering: str,
) -> list[tuple[Any, ...]]:
    return borrowed.cursor.execute(
        f"SELECT {', '.join(columns)} FROM {table} ORDER BY {ordering} LIMIT ?",
        (limit + 1,),
    ).fetchall()


def read_paper_history(
    borrowed: BorrowedGeneration,
    mark: ServingDatasetWatermark,
    account_ids: set[str],
) -> PaperHistoryData:
    at = _published_at(borrowed, mark)
    if at is None:
        return empty_history("not_published")
    counts = borrowed.manifest.row_counts
    if (
        counts.get("paper_order_window") != 1
        or not 0 <= counts.get("paper_order_history", -1) <= _MAX_ORDERS
        or not 0 <= counts.get("paper_fill_history", -1) <= _MAX_FILLS
    ):
        raise ValueError("paper history row budget is inconsistent")
    window = borrowed.cursor.execute(
        "SELECT snapshot_key, account_id, as_of_time, total_orders, retained_orders, "
        "retained_fills, has_more, newest_updated_at, oldest_updated_at "
        "FROM paper_order_window LIMIT 2"
    ).fetchall()
    if len(window) != 1:
        raise ValueError("paper history window is missing")
    key, account_id, as_of, total, retained, retained_fills, has_more, newest, oldest = window[0]
    order_rows = _model_rows(
        borrowed,
        "paper_order_history",
        _ORDER_COLUMNS,
        _MAX_ORDERS,
        "updated_at DESC, order_id DESC",
    )
    fill_rows = _model_rows(
        borrowed,
        "paper_fill_history",
        _FILL_COLUMNS,
        _MAX_FILLS,
        "order_id, sequence, fill_id",
    )
    if (
        key != "recent"
        or account_id not in account_ids
        or as_of != at
        or type(total) is not int
        or type(retained) is not int
        or type(retained_fills) is not int
        or type(has_more) is not bool
        or total < 0
        or retained != min(total, _MAX_ORDERS)
        or has_more != (total > retained)
        or len(order_rows) != retained
        or len(fill_rows) != retained_fills
        or len(order_rows) != counts["paper_order_history"]
        or len(fill_rows) != counts["paper_fill_history"]
    ):
        raise ValueError("paper history window does not match its tables")
    orders = tuple(PaperOrder(**dict(zip(_ORDER_COLUMNS, row, strict=True))) for row in order_rows)
    fills = tuple(
        PaperHistoryFill(**dict(zip(_FILL_COLUMNS, row, strict=True))) for row in fill_rows
    )
    if (
        newest != (orders[0].updated_at if orders else None)
        or oldest != (orders[-1].updated_at if orders else None)
        or len({order.order_id for order in orders}) != len(orders)
        or len({fill.fill_id for fill in fills}) != len(fills)
        or any(
            order.account_id != account_id or order.created_at > at or order.updated_at > at
            for order in orders
        )
    ):
        raise ValueError("paper history order evidence is inconsistent")
    fills_by_order: dict[str, list[PaperHistoryFill]] = defaultdict(list)
    order_ids = {order.order_id for order in orders}
    for fill in fills:
        if (
            fill.order_id not in order_ids
            or fill.cost_provenance_state is not PaperCostProvenanceState.KNOWN_V3
            or fill.persisted_at > at
            or fill.executed_at > at
        ):
            raise ValueError("paper history fill evidence is inconsistent")
        fills_by_order[fill.order_id].append(fill)
    for order in orders:
        parts = fills_by_order[order.order_id]
        if (
            [fill.sequence for fill in parts] != list(range(1, len(parts) + 1))
            or sum(fill.quantity for fill in parts) != order.filled_quantity
            or any(fill.persisted_at > order.updated_at for fill in parts)
        ):
            raise ValueError("paper history fill summary is inconsistent")
    try:
        names = readers.stock_names(
            borrowed.cursor,
            readers.table_states(borrowed.cursor),
            (order.ts_code for order in orders),
        )
    except ServingQueryError:
        names = {}
    items = [
        PaperOrderItem(
            order_id=str(order.order_id),
            code=order.ts_code,
            name=names.get(order.ts_code),
            side=order.side,
            side_label="买入" if order.side is PaperSide.BUY else "卖出",
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=order.filled_quantity,
            average_fill_price=order.average_fill_price,
            status=order.status,
            status_label=_STATUS_LABELS[order.status],
            reject_reason=order.reject_reason,
            reject_message=(
                None if order.reject_reason is None else _REJECT_MESSAGES[order.reject_reason]
            ),
            created_at=order.created_at,
            updated_at=order.updated_at,
            fills=[
                PaperFillItem(
                    fill_id=str(fill.fill_id),
                    sequence=fill.sequence,
                    quantity=fill.quantity,
                    price=fill.price,
                    commission=fill.commission,
                    transfer_fee=fill.transfer_fee,
                    tax=fill.tax,
                    total_fees=fill.total_fees,
                    executed_at=fill.executed_at,
                    persisted_at=fill.persisted_at,
                )
                for fill in fills_by_order[order.order_id]
            ],
        )
        for order in orders
    ]
    note = "仅显示最近 200 条指令。" if has_more else "还没有指令记录。" if not orders else None
    return PaperHistoryData(
        source_state="ready" if orders else "empty",
        source_updated_at=at,
        source_note=note,
        account_id=account_id,
        total_orders=total,
        has_more=has_more,
        newest_updated_at=newest,
        oldest_updated_at=oldest,
        orders=items,
    )
