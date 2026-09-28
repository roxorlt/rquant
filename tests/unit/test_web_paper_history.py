"""The paper page reads history only from a complete same-generation projection set."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.paper_broker import PaperHistoryFill, PaperOrderHistorySnapshot
from rquant.paper_contracts import PaperOrder, PaperOrderStatus, PaperSide
from rquant.paper_history_serving import paper_history_projections
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

AT = FIXTURE_BUILT_AT - timedelta(seconds=30)


def _app(root: Path):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def _history(*, empty: bool = False) -> PaperOrderHistorySnapshot:
    if empty:
        return PaperOrderHistorySnapshot(
            account_id="shadow-main",
            as_of=AT,
            ledger_revision=1,
            price_tick=Decimal("0.0001"),
            total_orders=0,
            has_more=False,
            orders=(),
            fills=(),
        )
    updated = AT - timedelta(minutes=1)
    order = PaperOrder(
        intent_id="a" * 64,
        account_id="shadow-main",
        ts_code="600005.SH",
        side=PaperSide.BUY,
        order_type="MARKET",
        quantity=100,
        filled_quantity=100,
        average_fill_price=Decimal("10.0001"),
        status=PaperOrderStatus.FILLED,
        created_at=updated - timedelta(minutes=1),
        updated_at=updated,
    )
    assert order.order_id is not None
    fill = PaperHistoryFill(
        execution_id="b" * 64,
        order_id=order.order_id,
        sequence=1,
        quantity=100,
        price=Decimal("10.0001"),
        commission=Decimal("5.00"),
        transfer_fee=Decimal("0"),
        tax=Decimal("0"),
        total_fees=Decimal("5.00"),
        cost_spec_id="c" * 64,
        cost_spec_schema_version=3,
        cost_context_fingerprint="d" * 64,
        cost_provenance_state="KNOWN_V3",
        executed_at=updated,
        persisted_at=updated,
        price_snapshot_id="e" * 64,
    )
    return PaperOrderHistorySnapshot(
        account_id="shadow-main",
        as_of=AT,
        ledger_revision=2,
        price_tick=Decimal("0.0001"),
        total_orders=1,
        has_more=False,
        orders=(order,),
        fills=(fill,),
    )


def test_old_generation_keeps_accounts_and_says_history_not_published(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/paper/accounts")

    assert response.status_code == 200
    assert response.json()["data"]["accounts"][0]["account_id"] == "shadow-main"
    assert response.json()["data"]["history"]["source_state"] == "not_published"
    assert response.json()["data"]["history"]["total_orders"] is None


def test_pre_history_physical_generation_still_serves_accounts(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", legacy_paper_schema=True)

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/paper/accounts")

    assert response.status_code == 200
    assert response.json()["data"]["source_state"] == "ready"
    assert response.json()["data"]["history"]["source_state"] == "not_published"


def test_published_history_exposes_exact_fill_and_window_from_one_lease(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root, "baseline", paper_history_projections=paper_history_projections(_history())
    )

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/paper/accounts")

    assert response.status_code == 200, response.text
    body = response.json()
    assert response.headers["X-Rquant-Generation"] == body["serving"]["generation_id"]
    history = body["data"]["history"]
    assert history["source_state"] == "ready"
    assert history["source_updated_at"] == AT.isoformat().replace("+00:00", "Z")
    assert history["account_id"] == "shadow-main"
    assert history["total_orders"] == 1
    assert history["has_more"] is False
    order = history["orders"][0]
    assert order["code"] == "600005.SH"
    assert order["side_label"] == "买入"
    assert order["status_label"] == "已成交"
    assert order["average_fill_price"] == "10.0001"
    assert order["fills"][0]["price"] == "10.0001"
    assert order["fills"][0]["total_fees"] == "5.00"


def test_published_empty_history_is_distinct_from_unpublished(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root, "baseline", paper_history_projections=paper_history_projections(_history(empty=True))
    )

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/paper/accounts")

    assert response.status_code == 200
    history = response.json()["data"]["history"]
    assert history["source_state"] == "empty"
    assert history["total_orders"] == 0
    assert history["orders"] == []


def test_published_history_rejects_average_price_unrelated_to_fills(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    snapshot = _history()
    changed = snapshot.orders[0].model_copy(update={"average_fill_price": Decimal("99.9999")})
    corrupt = snapshot.model_copy(update={"orders": (changed,)})
    build_web_fixture(
        root, "baseline", paper_history_projections=paper_history_projections(corrupt)
    )

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/paper/accounts")

    assert response.status_code == 503


def test_published_history_uses_broker_price_tick(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    snapshot = _history()
    order = snapshot.orders[0].model_copy(update={"average_fill_price": Decimal("10.00")})
    fill = snapshot.fills[0].model_copy(update={"price": Decimal("10.0049")})
    rounded = snapshot.model_copy(
        update={"price_tick": Decimal("0.01"), "orders": (order,), "fills": (fill,)}
    )
    build_web_fixture(
        root, "baseline", paper_history_projections=paper_history_projections(rounded)
    )

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/paper/accounts")

    assert response.status_code == 200, response.text
    assert response.json()["data"]["history"]["orders"][0]["average_fill_price"] == "10.00"


def test_partial_projection_set_is_503_instead_of_zero_history(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    window = paper_history_projections(_history(empty=True))[0]
    build_web_fixture(root, "baseline", paper_history_projections=(window,))

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/paper/accounts")

    assert response.status_code == 503
    assert "paper_order_window" not in response.text


@pytest.mark.parametrize(
    "fault",
    ["orphan_fill", "window_count", "missing_tick", "status_owner", "future_fill", "legacy_fill"],
)
def test_corrupt_history_evidence_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root, "baseline", paper_history_projections=paper_history_projections(_history())
    )
    app = _app(root)

    class FaultCursor:
        def __init__(self, cursor: object) -> None:
            self.cursor = cursor
            self.rows: list[tuple[object, ...]] = []

        def execute(self, sql: str, params: tuple[object, ...] = ()) -> FaultCursor:
            self.rows = self.cursor.execute(sql, params).fetchall()  # type: ignore[attr-defined]
            if not self.rows:
                return self
            first = self.rows[0]
            if fault == "orphan_fill" and "FROM paper_fill_history" in sql:
                self.rows[0] = (*first[:2], "f" * 64, *first[3:])
            elif fault == "window_count" and "FROM paper_order_window" in sql:
                self.rows[0] = (*first[:4], 0, *first[5:])
            elif fault == "missing_tick" and "FROM paper_order_window" in sql:
                self.rows[0] = (*first[:3], None, *first[4:])
            elif fault == "status_owner" and "FROM projection_status" in sql:
                self.rows[0] = (*first[:4], "f" * 64, *first[5:])
            elif fault == "future_fill" and "FROM paper_fill_history" in sql:
                self.rows[0] = (*first[:16], AT + timedelta(minutes=1))
            elif fault == "legacy_fill" and "FROM paper_fill_history" in sql:
                self.rows[0] = (
                    *first[:7],
                    None,
                    first[8],
                    None,
                    None,
                    None,
                    None,
                    "LEGACY_UNKNOWN",
                    *first[14:],
                )
            return self

        def fetchall(self) -> list[tuple[object, ...]]:
            return self.rows

    with TestClient(app) as client:
        original_borrow = app.state.web.tracker.borrow

        @contextmanager
        def broken_borrow():
            with original_borrow() as borrowed:
                assert borrowed is not None
                yield replace(borrowed, cursor=FaultCursor(borrowed.cursor))

        monkeypatch.setattr(app.state.web.tracker, "borrow", broken_borrow)
        response = client.get("/api/v1/paper/accounts")

    assert response.status_code == 503
    assert "paper_fill_history" not in response.text
