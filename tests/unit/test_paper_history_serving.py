"""The paper owner publishes a complete recent-history projection set."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from rquant.paper_broker import PaperBrokerStore
from rquant.paper_contracts import PaperOrderType
from rquant.paper_history_serving import paper_history_projections
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS
from tests.paper_cost_fixtures import paper_cost_policy
from tests.unit.test_paper_broker import BUY_DATE, BUY_TIME, _intent, _quote, _store


def test_empty_window_still_publishes_one_window_row(tmp_path: Path) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())

    projections = paper_history_projections(store.recent_order_history(as_of=BUY_TIME))

    assert {item.table_name for item in projections} == {
        "paper_order_window",
        "paper_order_history",
        "paper_fill_history",
    }
    assert all(item.available_at == BUY_TIME for item in projections)
    assert all(
        PAGE_PROJECTION_CONTRACTS[item.table_name].owner_dataset_id == "paper_accounts"
        for item in projections
    )
    window = next(item for item in projections if item.table_name == "paper_order_window")
    assert len(window.rows) == 1
    assert window.rows[0]["total_orders"] == 0
    assert window.rows[0]["retained_orders"] == 0
    assert window.rows[0]["retained_fills"] == 0
    assert window.rows[0]["has_more"] is False
    assert window.rows[0]["price_tick"] == "0.0001"


def test_history_projection_keeps_exact_money_and_all_order_evidence(tmp_path: Path) -> None:
    store: PaperBrokerStore = _store(tmp_path / "paper.sqlite3", paper_cost_policy())
    order = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("11.00")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.0001"),
    )

    projections = paper_history_projections(
        store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=1))
    )

    orders = next(item for item in projections if item.table_name == "paper_order_history")
    fills = next(item for item in projections if item.table_name == "paper_fill_history")
    assert orders.rows[0]["order_id"] == order.order_id
    assert orders.rows[0]["average_fill_price"] == "10.0001"
    assert fills.rows[0]["price"] == "10.0001"
    assert fills.rows[0]["persisted_at"] == BUY_TIME.isoformat().replace("+00:00", "Z")
