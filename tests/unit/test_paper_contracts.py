from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from rquant.paper_contracts import (
    PaperAccountSnapshot,
    PaperFill,
    PaperHolding,
    PaperOrder,
    PaperOrderIntent,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectReason,
    PaperSide,
)
from rquant.runtime_contracts import canonical_sha256

NOW = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
SIGNAL_ID = "a" * 64
PRICE_SNAPSHOT_ID = "b" * 64
PRODUCER_COMMIT = "c" * 40


def _intent(**overrides: object) -> PaperOrderIntent:
    values: dict[str, object] = {
        "signal_id": SIGNAL_ID,
        "account_id": "paper-main",
        "ts_code": "600000.SH",
        "side": PaperSide.BUY,
        "order_type": PaperOrderType.MARKET,
        "quantity": 1_000,
        "event_time": NOW,
        "available_at": NOW + timedelta(seconds=1),
        "expires_at": NOW + timedelta(minutes=5),
        "earliest_execution_at": NOW + timedelta(seconds=1),
        "price_snapshot_id": PRICE_SNAPSHOT_ID,
        "producer_commit": PRODUCER_COMMIT,
    }
    values.update(overrides)
    return PaperOrderIntent.model_validate(values)


def _order(**overrides: object) -> PaperOrder:
    intent = _intent()
    values: dict[str, object] = {
        "intent_id": intent.intent_id,
        "account_id": intent.account_id,
        "ts_code": intent.ts_code,
        "side": intent.side,
        "order_type": intent.order_type,
        "quantity": intent.quantity,
        "filled_quantity": 0,
        "status": PaperOrderStatus.ACCEPTED,
        "created_at": NOW + timedelta(seconds=2),
        "updated_at": NOW + timedelta(seconds=2),
    }
    values.update(overrides)
    return PaperOrder.model_validate(values)


def test_order_intent_builds_and_verifies_deterministic_identity() -> None:
    intent = _intent()
    same = _intent(
        event_time=datetime(2026, 7, 31, 9, 30, tzinfo=timezone(timedelta(hours=8))),
        available_at=datetime(2026, 7, 31, 9, 30, 1, tzinfo=timezone(timedelta(hours=8))),
        expires_at=datetime(2026, 7, 31, 9, 35, tzinfo=timezone(timedelta(hours=8))),
        earliest_execution_at=datetime(2026, 7, 31, 9, 30, 1, tzinfo=timezone(timedelta(hours=8))),
        intent_id=intent.intent_id,
    )

    assert same.intent_id == intent.intent_id
    assert same.event_time == NOW
    assert len(intent.intent_id) == 64
    with pytest.raises(ValidationError, match="intent_id"):
        _intent(intent_id="f" * 64)


@pytest.mark.parametrize("quantity", [0, -100, 1, 50, 150])
def test_order_intent_rejects_invalid_lot_sizes(quantity: int) -> None:
    with pytest.raises(ValidationError, match="quantity"):
        _intent(quantity=quantity)


def test_limit_price_is_required_only_for_limit_orders() -> None:
    with pytest.raises(ValidationError, match="limit_price"):
        _intent(order_type=PaperOrderType.LIMIT)
    with pytest.raises(ValidationError, match="limit_price"):
        _intent(limit_price=Decimal("10.20"))

    intent = _intent(
        order_type=PaperOrderType.LIMIT,
        limit_price=Decimal("10.20"),
    )
    assert intent.limit_price == Decimal("10.20")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"event_time": NOW + timedelta(seconds=2)}, "event_time"),
        ({"expires_at": NOW + timedelta(seconds=1)}, "expires_at"),
        ({"earliest_execution_at": NOW}, "earliest_execution_at"),
        (
            {"earliest_execution_at": NOW + timedelta(minutes=6)},
            "earliest_execution_at",
        ),
    ],
)
def test_order_intent_enforces_visibility_and_expiry_order(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        _intent(**overrides)


def test_order_can_represent_a_t_plus_one_rejection() -> None:
    order = _order(
        status=PaperOrderStatus.REJECTED,
        reject_reason=PaperRejectReason.T_PLUS_ONE,
    )

    assert order.reject_reason is PaperRejectReason.T_PLUS_ONE
    assert order.filled_quantity == 0
    assert order.average_fill_price is None


def test_order_partial_fill_relationships_and_identity_are_enforced() -> None:
    partial = _order(
        status=PaperOrderStatus.PARTIALLY_FILLED,
        filled_quantity=400,
        average_fill_price=Decimal("10.18"),
    )
    same = _order(
        status=PaperOrderStatus.PARTIALLY_FILLED,
        filled_quantity=400,
        average_fill_price=Decimal("10.18"),
        order_id=partial.order_id,
        updated_at=NOW + timedelta(minutes=1),
    )

    assert same.order_id == partial.order_id
    with pytest.raises(ValidationError, match="order_id"):
        _order(order_id="d" * 64)
    with pytest.raises(ValidationError, match="PARTIALLY_FILLED"):
        _order(
            status=PaperOrderStatus.PARTIALLY_FILLED,
            filled_quantity=1_000,
            average_fill_price=Decimal("10.18"),
        )
    with pytest.raises(ValidationError, match="average_fill_price"):
        _order(status=PaperOrderStatus.FILLED, filled_quantity=1_000)
    with pytest.raises(ValidationError, match="reject_reason"):
        _order(status=PaperOrderStatus.REJECTED)


def test_fill_identity_notional_and_cost_rules_are_deterministic() -> None:
    order = _order()
    fill = PaperFill(
        order_id=order.order_id,
        execution_id="9" * 64,
        sequence=1,
        quantity=400,
        price=Decimal("10.25"),
        commission=Decimal("5.00"),
        tax=Decimal("0"),
        executed_at=NOW + timedelta(seconds=3),
        price_snapshot_id=PRICE_SNAPSHOT_ID,
    )
    same = PaperFill.model_validate({**fill.model_dump(mode="python"), "fill_id": fill.fill_id})

    assert fill.notional == Decimal("4100.00")
    assert same.fill_id == fill.fill_id
    with pytest.raises(ValidationError, match="fill_id"):
        PaperFill.model_validate({**fill.model_dump(mode="python"), "fill_id": "d" * 64})
    with pytest.raises(ValidationError, match="commission"):
        PaperFill.model_validate({**fill.model_dump(mode="python"), "commission": Decimal("-0.01")})


def test_holding_enforces_lot_and_quantity_reconciliation() -> None:
    holding = PaperHolding(
        code="600000.SH",
        quantity=1_000,
        available_quantity=600,
        frozen_quantity=400,
        average_cost=Decimal("9.80"),
        market_price=Decimal("10.20"),
    )

    assert holding.quantity == holding.available_quantity + holding.frozen_quantity
    with pytest.raises(ValidationError, match="available_quantity"):
        PaperHolding(
            code="600000.SH",
            quantity=1_000,
            available_quantity=500,
            frozen_quantity=400,
            average_cost=Decimal("9.80"),
            market_price=Decimal("10.20"),
        )


def test_account_snapshot_reconciles_cash_holdings_pnl_and_nav() -> None:
    holding = PaperHolding(
        code="600000.SH",
        quantity=1_000,
        available_quantity=600,
        frozen_quantity=400,
        average_cost=Decimal("9.80"),
        market_price=Decimal("10.20"),
    )
    snapshot = PaperAccountSnapshot(
        account_id="paper-main",
        as_of_time=NOW,
        cash=Decimal("90000"),
        available_cash=Decimal("85000"),
        frozen_cash=Decimal("5000"),
        holdings=(holding,),
        realized_pnl=Decimal("125.50"),
        unrealized_pnl=Decimal("400"),
        nav=Decimal("100200"),
    )
    expected_id = canonical_sha256(snapshot.model_dump(mode="python", exclude={"snapshot_id"}))

    assert snapshot.snapshot_id == expected_id
    assert isinstance(snapshot.holdings, tuple)
    with pytest.raises(ValidationError, match="holdings"):
        PaperAccountSnapshot.model_validate(
            {
                **snapshot.model_dump(mode="python", exclude={"snapshot_id"}),
                "holdings": (holding, holding),
            }
        )
    with pytest.raises(ValidationError, match="cash"):
        PaperAccountSnapshot.model_validate(
            {
                **snapshot.model_dump(mode="python", exclude={"snapshot_id"}),
                "available_cash": Decimal("84999.99"),
            }
        )
    with pytest.raises(ValidationError, match="unrealized_pnl"):
        PaperAccountSnapshot.model_validate(
            {
                **snapshot.model_dump(mode="python", exclude={"snapshot_id"}),
                "unrealized_pnl": Decimal("399.99"),
            }
        )
    with pytest.raises(ValidationError, match="nav"):
        PaperAccountSnapshot.model_validate(
            {
                **snapshot.model_dump(mode="python", exclude={"snapshot_id"}),
                "nav": Decimal("100199.99"),
            }
        )
    with pytest.raises(ValidationError, match="snapshot_id"):
        PaperAccountSnapshot.model_validate(
            {**snapshot.model_dump(mode="python"), "snapshot_id": "d" * 64}
        )


def test_contracts_forbid_unknown_fields_and_are_frozen() -> None:
    intent = _intent()

    with pytest.raises(ValidationError, match="extra_forbidden"):
        PaperOrderIntent.model_validate({**intent.model_dump(mode="python"), "unexpected": True})
    with pytest.raises(ValidationError, match="frozen_instance"):
        intent.quantity = 2_000


def test_paper_contracts_round_trip_through_json() -> None:
    intent = _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("10.20"))
    order = _order(
        status=PaperOrderStatus.PARTIALLY_FILLED,
        filled_quantity=400,
        average_fill_price=Decimal("10.18"),
    )

    assert PaperOrderIntent.model_validate_json(intent.model_dump_json()) == intent
    assert PaperOrder.model_validate_json(order.model_dump_json()) == order
