from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import rquant.paper_broker as paper_broker_module
from rquant.paper_broker import (
    BrokerCostPolicy,
    BrokerExecutionContext,
    DuplicateExecutionConflictError,
    DuplicateIntentConflictError,
    PaperBrokerReconciliationError,
    PaperBrokerStore,
)
from rquant.paper_contracts import (
    PaperOrderIntent,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectReason,
    PaperSellQuantityAuthority,
    PaperSide,
)
from rquant.strategy_paper_lifecycle import PaperBrokerLifecycleReader
from tests.paper_cost_fixtures import (
    paper_cost_policy,
    paper_execution_cost_spec,
    paper_instrument_context,
)

ACCOUNT_ID = "paper-main"
BUY_TIME = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
BUY_DATE = date(2026, 7, 31)
NEXT_TRADE_DATE = date(2026, 8, 3)
PRICE_SNAPSHOT_ID = "b" * 64
PRODUCER_COMMIT = "c" * 40


@pytest.fixture
def cost_policy() -> BrokerCostPolicy:
    return paper_cost_policy()


def _intent(
    *,
    side: PaperSide = PaperSide.BUY,
    quantity: int = 1_000,
    order_type: PaperOrderType = PaperOrderType.MARKET,
    limit_price: Decimal | None = None,
    signal_seed: str = "a",
    event_time: datetime = BUY_TIME - timedelta(seconds=2),
    entry_signal_id: str | None = None,
    sell_quantity_authority: PaperSellQuantityAuthority | None = None,
    account_id: str = ACCOUNT_ID,
) -> PaperOrderIntent:
    return PaperOrderIntent(
        signal_id=signal_seed * 64,
        entry_signal_id=entry_signal_id,
        sell_quantity_authority=sell_quantity_authority,
        account_id=account_id,
        ts_code="600000.SH",
        side=side,
        order_type=order_type,
        quantity=quantity,
        limit_price=limit_price,
        event_time=event_time,
        available_at=event_time + timedelta(seconds=1),
        expires_at=event_time + timedelta(minutes=5),
        earliest_execution_at=event_time + timedelta(seconds=1),
        price_snapshot_id=PRICE_SNAPSHOT_ID,
        producer_commit=PRODUCER_COMMIT,
    )


def _sell_intent(
    store: PaperBrokerStore,
    *,
    entry_signal_id: str,
    decision_time: datetime,
    quantity: int = 1_000,
    signal_seed: str = "d",
    order_type: PaperOrderType = PaperOrderType.MARKET,
    limit_price: Decimal | None = None,
    trade_date: date = NEXT_TRADE_DATE,
) -> PaperOrderIntent:
    authority = store.sell_quantity_authority(
        exit_signal_id=signal_seed * 64,
        entry_signal_id=entry_signal_id,
        ts_code="600000.SH",
        action="S_INTENT",
        tranche_fraction=Decimal("1"),
        decision_cutoff=decision_time,
        trade_date=trade_date,
    )
    assert authority.requested_quantity == quantity
    return _intent(
        side=PaperSide.SELL,
        quantity=quantity,
        signal_seed=signal_seed,
        event_time=decision_time - timedelta(seconds=2),
        entry_signal_id=entry_signal_id,
        sell_quantity_authority=authority,
        order_type=order_type,
        limit_price=limit_price,
    )


def _quote(
    price: str,
    *,
    available_date: date | None = NEXT_TRADE_DATE,
    executable_quantity: int | None = None,
    suspended: bool = False,
    limit_locked: bool = False,
    risk_rejected: bool = False,
) -> BrokerExecutionContext:
    return BrokerExecutionContext(
        executable_price=Decimal(price),
        acquisition_available_date=available_date,
        instrument_context=paper_instrument_context(),
        **({} if executable_quantity is None else {"executable_quantity": executable_quantity}),
        suspended=suspended,
        limit_locked=limit_locked,
        risk_rejected=risk_rejected,
    )


def _store(
    path: Path,
    cost_policy: BrokerCostPolicy,
    *,
    initial_cash: str = "100000",
) -> PaperBrokerStore:
    return PaperBrokerStore(
        path,
        account_id=ACCOUNT_ID,
        initial_cash=Decimal(initial_cash),
        cost_policy=cost_policy,
    )


def _replace_schema_metadata_with_v3(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        DROP TRIGGER IF EXISTS paper_ledger_head_marker_update_immutable;
        DROP TRIGGER IF EXISTS paper_ledger_head_marker_delete_immutable;
        DROP TRIGGER IF EXISTS paper_ledger_tamper_marker_update_immutable;
        DROP TRIGGER IF EXISTS paper_ledger_tamper_marker_delete_immutable;
        DROP TRIGGER IF EXISTS paper_ledger_attestation_update_immutable;
        DROP TRIGGER IF EXISTS paper_ledger_attestation_delete_immutable;
        DROP TRIGGER IF EXISTS paper_ledger_attestation_delete_tamper;
        DROP TABLE IF EXISTS paper_ledger_head_marker;
        DROP TABLE IF EXISTS paper_ledger_tamper_marker;
        DROP TABLE IF EXISTS paper_ledger_attestation;
        DROP TABLE paper_ledger_schema;
        CREATE TABLE paper_ledger_schema (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            schema_version INTEGER NOT NULL CHECK(schema_version = 3),
            migrated_at TEXT NOT NULL,
            unknown_fill_availability_count INTEGER NOT NULL,
            unknown_lot_availability_count INTEGER NOT NULL,
            unknown_consumption_availability_count INTEGER NOT NULL,
            unknown_lot_provenance_count INTEGER NOT NULL,
            unknown_intent_identity_count INTEGER NOT NULL,
            unknown_execution_identity_count INTEGER NOT NULL,
            unknown_lot_timeline_count INTEGER NOT NULL
        );
        INSERT INTO paper_ledger_schema VALUES (
            1, 3, '2026-07-31T01:31:00Z', 0, 0, 0, 0, 0, 0, 0
        );
        """
    )


def _downgrade_v3_initial_execution_evidence(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            PRAGMA foreign_keys = OFF;
            DROP TABLE paper_execution_receipt;
            CREATE TABLE paper_intent_v3 (
                intent_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES broker_account(account_id),
                signal_id TEXT,
                entry_signal_id TEXT,
                ts_code TEXT,
                side TEXT,
                payload_json TEXT NOT NULL,
                persisted_at TEXT NOT NULL
            );
            INSERT INTO paper_intent_v3(
                intent_id, account_id, signal_id, entry_signal_id,
                ts_code, side, payload_json, persisted_at
            )
            SELECT
                intent_id, account_id, signal_id, entry_signal_id,
                ts_code, side, payload_json, persisted_at
            FROM paper_intent;
            DROP TABLE paper_intent;
            ALTER TABLE paper_intent_v3 RENAME TO paper_intent;
            """
        )
        _replace_schema_metadata_with_v3(connection)


def _ledger_rows(path: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    tables = (
        "broker_account",
        "paper_intent",
        "paper_order",
        "paper_fill",
        "paper_lot",
        "paper_lot_consumption",
        "paper_execution_receipt",
        "paper_account_authority",
    )
    with sqlite3.connect(path) as connection:
        return {
            table: tuple(connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid'))
            for table in tables
        }


def test_buy_fills_atomically_and_accounts_for_minimum_commission(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)

    order = store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    snapshot = store.account_snapshot(
        as_of=BUY_TIME,
        market_prices={"600000.SH": Decimal("10.20")},
    )

    assert order.status is PaperOrderStatus.FILLED
    assert order.average_fill_price == Decimal("10.0000")
    assert len(store.fills(order.order_id)) == 1
    assert store.fills(order.order_id)[0].commission == Decimal("5.00")
    assert snapshot.cash == Decimal("89995.00")
    assert snapshot.holdings[0].quantity == 1_000
    assert snapshot.holdings[0].available_quantity == 0
    assert snapshot.holdings[0].frozen_quantity == 1_000
    assert snapshot.holdings[0].average_cost == Decimal("10.005")
    assert snapshot.unrealized_pnl == Decimal("195.000")
    assert snapshot.nav == Decimal("100195.00")


def test_buy_commission_uses_rate_above_minimum_threshold(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy, initial_cash="2000000")

    order = store.submit_intent(
        _intent(quantity=100_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    fill = store.fills(order.order_id)[0]
    assert fill.notional == Decimal("1000000.0000")
    assert fill.commission == Decimal("300.00")


def test_account_authority_revision_is_persistent_and_not_driven_by_wall_clock(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)

    initial = store.account_authority_snapshot(
        as_of=BUY_TIME,
        market_prices={},
        producer_commit=PRODUCER_COMMIT,
    )
    unchanged = store.account_authority_snapshot(
        as_of=BUY_TIME + timedelta(seconds=5),
        market_prices={},
        producer_commit=PRODUCER_COMMIT,
    )
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME + timedelta(seconds=10),
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    changed = store.account_authority_snapshot(
        as_of=BUY_TIME + timedelta(seconds=10),
        market_prices={"600000.SH": Decimal("10.00")},
        producer_commit=PRODUCER_COMMIT,
    )
    reopened = _store(path, cost_policy).account_authority_snapshot(
        as_of=BUY_TIME + timedelta(seconds=15),
        market_prices={"600000.SH": Decimal("10.00")},
        producer_commit=PRODUCER_COMMIT,
    )
    upgraded = _store(path, cost_policy).account_authority_snapshot(
        as_of=BUY_TIME + timedelta(seconds=20),
        market_prices={"600000.SH": Decimal("10.00")},
        producer_commit="d" * 40,
    )

    assert initial.revision == 1
    assert unchanged == initial
    assert changed.revision == 2
    assert changed.snapshot.holdings[0].quantity == 1_000
    assert reopened == changed
    assert upgraded.revision == 3
    assert upgraded.snapshot.holdings == changed.snapshot.holdings
    assert upgraded.producer_commit == "d" * 40


def test_retry_after_process_reopen_is_idempotent(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    path = tmp_path / "paper.sqlite3"
    intent = _intent()
    first = _store(path, cost_policy).submit_intent(
        intent,
        execution_id="f" * 64,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    reopened = _store(path, cost_policy)
    second = reopened.submit_intent(
        intent,
        execution_id="f" * 64,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    assert second == first
    assert len(reopened.fills(first.order_id)) == 1
    assert reopened.account_snapshot(
        as_of=BUY_TIME,
        market_prices={"600000.SH": Decimal("10.00")},
    ).cash == Decimal("89995.00")


def test_same_day_sell_is_rejected_by_t_plus_one(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    order = store.submit_intent(
        _sell_intent(
            store,
            entry_signal_id="a" * 64,
            decision_time=BUY_TIME + timedelta(minutes=1),
            trade_date=BUY_DATE,
        ),
        decision_time=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quote=_quote("10.50", available_date=None),
    )

    assert order.status is PaperOrderStatus.REJECTED
    assert order.reject_reason is PaperRejectReason.T_PLUS_ONE
    assert store.fills(order.order_id) == ()


def test_next_trading_date_sell_fills_and_realizes_pnl(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    sell_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)

    order = store.submit_intent(
        _sell_intent(
            store,
            entry_signal_id="a" * 64,
            decision_time=sell_time,
        ),
        decision_time=sell_time,
        trade_date=NEXT_TRADE_DATE,
        quote=_quote("11.00", available_date=None),
    )
    snapshot = store.account_snapshot(as_of=sell_time, market_prices={})

    assert order.status is PaperOrderStatus.FILLED
    assert store.fills(order.order_id)[0].tax == Decimal("11.00")
    assert snapshot.cash == Decimal("100979.00")
    assert snapshot.realized_pnl == Decimal("979.000")
    assert snapshot.holdings == ()
    assert snapshot.nav == Decimal("100979.00")


def test_insufficient_cash_and_absent_entry_position_fail_closed(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy, initial_cash="1000")

    buy = store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    with pytest.raises(PaperBrokerReconciliationError, match="BUY fill"):
        _sell_intent(
            store,
            entry_signal_id="a" * 64,
            decision_time=BUY_TIME,
            trade_date=BUY_DATE,
        )

    assert buy.reject_reason is PaperRejectReason.INSUFFICIENT_CASH
    assert store.account_snapshot(as_of=BUY_TIME, market_prices={}).cash == Decimal("1000")


def test_limit_price_miss_remains_accepted_without_reserving_cash(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)

    order = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    assert order.status is PaperOrderStatus.ACCEPTED
    assert order.filled_quantity == 0
    assert store.fills(order.order_id) == ()
    assert store.account_snapshot(as_of=BUY_TIME, market_prices={}).cash == Decimal("100000")


@pytest.mark.parametrize(
    ("quote", "reason"),
    [
        (_quote("10.00", suspended=True), PaperRejectReason.SUSPENDED),
        (_quote("10.00", limit_locked=True), PaperRejectReason.LIMIT_LOCKED),
        (_quote("10.00", risk_rejected=True), PaperRejectReason.RISK_REJECTED),
    ],
)
def test_market_constraints_are_persisted_as_rejections(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
    quote: BrokerExecutionContext,
    reason: PaperRejectReason,
) -> None:
    store = _store(tmp_path / f"{reason.value}.sqlite3", cost_policy)

    order = store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=quote,
    )

    assert order.status is PaperOrderStatus.REJECTED
    assert order.reject_reason is reason


def test_same_intent_id_with_different_payload_is_a_conflict(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    intent = _intent()
    store.submit_intent(
        intent,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    conflicting = PaperOrderIntent.model_construct(
        **{**intent.model_dump(mode="python"), "quantity": 2_000}
    )

    with pytest.raises(DuplicateIntentConflictError, match="intent_id"):
        store.submit_intent(
            conflicting,
            decision_time=BUY_TIME,
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )


def test_injected_failure_rolls_back_intent_order_fill_cash_and_lot(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    intent = _intent()
    with sqlite3.connect(path) as connection:
        trust_evidence_before = {
            table: tuple(connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid'))
            for table in (
                "paper_ledger_attestation",
                "paper_ledger_head_marker",
                "paper_ledger_tamper_marker",
            )
        }

    def explode(_connection: sqlite3.Connection) -> None:
        raise RuntimeError("injected failure")

    monkeypatch.setattr(store, "_before_commit", explode)
    with pytest.raises(RuntimeError, match="injected failure"):
        store.submit_intent(
            intent,
            decision_time=BUY_TIME,
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )

    reopened = _store(path, cost_policy)
    assert reopened.order_for_intent(intent.intent_id) is None
    assert reopened.fills() == ()
    assert reopened.account_snapshot(as_of=BUY_TIME, market_prices={}).cash == Decimal("100000")
    with sqlite3.connect(path) as connection:
        trust_evidence_after = {
            table: tuple(connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid'))
            for table in (
                "paper_ledger_attestation",
                "paper_ledger_head_marker",
                "paper_ledger_tamper_marker",
            )
        }
    assert trust_evidence_after == trust_evidence_before


def test_reconcile_independently_checks_end_of_day_ledger(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    sell_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)
    store.submit_intent(
        _sell_intent(
            store,
            entry_signal_id="a" * 64,
            decision_time=sell_time,
        ),
        decision_time=sell_time,
        trade_date=NEXT_TRADE_DATE,
        quote=_quote("11.00", available_date=None),
    )

    report = store.reconcile()

    assert report.is_consistent is True
    assert report.order_count == 2
    assert report.fill_count == 2
    assert report.open_lot_quantity == 0
    assert report.cash == Decimal("100979.00")
    assert report.realized_pnl == Decimal("979.000")


def test_reconcile_detects_a_missing_buy_lot(tmp_path: Path, cost_policy: BrokerCostPolicy) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM paper_lot")

    with pytest.raises(PaperBrokerReconciliationError, match="buy fill.*lot"):
        store.reconcile()


@pytest.mark.parametrize(
    ("damage", "message"),
    (
        ("fill_availability", "availability|persisted_at"),
        ("lot_provenance", "provenance|entry_signal_id"),
        ("sell_provenance", "provenance|entry_signal_id"),
    ),
)
def test_reconcile_rejects_pit_or_entry_provenance_corruption(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
    damage: str,
    message: str,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    buy = _intent()
    buy_order = store.submit_intent(
        buy,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    sell_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)
    sell = store.submit_intent(
        _sell_intent(
            store,
            entry_signal_id=buy.signal_id,
            decision_time=sell_time,
        ),
        decision_time=sell_time,
        trade_date=NEXT_TRADE_DATE,
        quote=_quote("11.00", available_date=None),
    )
    with sqlite3.connect(path) as connection:
        if damage == "fill_availability":
            trigger_sql = [
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT sql FROM sqlite_master
                    WHERE type = 'trigger' AND name IN (
                        'paper_fill_persisted_at_immutable',
                        'paper_fill_row_immutable'
                    )
                    ORDER BY name
                    """
                ).fetchall()
            ]
            connection.execute("DROP TRIGGER paper_fill_persisted_at_immutable")
            connection.execute("DROP TRIGGER paper_fill_row_immutable")
            connection.execute(
                "UPDATE paper_fill SET persisted_at = ? WHERE order_id = ?",
                ((BUY_TIME - timedelta(seconds=1)).isoformat(), buy_order.order_id),
            )
            for statement in trigger_sql:
                connection.execute(statement)
        elif damage == "lot_provenance":
            trigger_sql = str(
                connection.execute(
                    """
                    SELECT sql FROM sqlite_master
                    WHERE type = 'trigger'
                      AND name = 'paper_lot_entry_signal_id_immutable'
                    """
                ).fetchone()[0]
            )
            connection.execute("DROP TRIGGER paper_lot_entry_signal_id_immutable")
            connection.execute(
                "UPDATE paper_lot SET entry_signal_id = ?",
                ("f" * 64,),
            )
            connection.execute(trigger_sql)
        else:
            connection.execute(
                "UPDATE paper_order SET entry_signal_id = ? WHERE order_id = ?",
                ("f" * 64, sell.order_id),
            )

    with pytest.raises(PaperBrokerReconciliationError, match=message):
        store.reconcile()


def test_latest_execution_prices_obeys_independent_fill_availability(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    persisted_at = BUY_TIME + timedelta(minutes=10)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        persisted_at=persisted_at,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    assert store.latest_execution_prices(as_of=persisted_at - timedelta(microseconds=1)) == {}
    assert store.latest_execution_prices(as_of=persisted_at) == {"600000.SH": Decimal("10.0000")}


def test_store_initialization_is_idempotent_and_enables_wal(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    path = tmp_path / "paper.sqlite3"
    first = _store(path, cost_policy)
    second = _store(path, cost_policy)

    assert first.account_snapshot(as_of=BUY_TIME, market_prices={}) == second.account_snapshot(
        as_of=BUY_TIME,
        market_prices={},
    )
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        money_types = connection.execute(
            "SELECT typeof(initial_cash), typeof(cash), typeof(realized_pnl) FROM broker_account"
        ).fetchone()
    assert money_types == ("text", "text", "text")


def test_reopen_rejects_cost_policy_drift(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    _store(path, cost_policy)
    changed = BrokerCostPolicy.from_execution_cost_spec(
        paper_execution_cost_spec(commission_bps=Decimal("5"))
    )

    with pytest.raises(ValueError, match="cost binding"):
        _store(path, changed)


def test_submit_rejects_previsible_decision_and_mismatched_trade_date(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    intent = _intent()

    with pytest.raises(ValueError, match="available_at"):
        store.submit_intent(
            intent,
            decision_time=intent.available_at - timedelta(microseconds=1),
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )
    with pytest.raises(ValueError, match="trade_date"):
        store.submit_intent(
            intent,
            decision_time=BUY_TIME,
            trade_date=NEXT_TRADE_DATE,
            quote=_quote("10.00"),
        )


def test_account_snapshot_rejects_as_of_before_latest_ledger_event(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    with pytest.raises(ValueError, match="as_of.*latest ledger event"):
        store.account_snapshot(
            as_of=BUY_TIME - timedelta(seconds=1),
            market_prices={"600000.SH": Decimal("10.00")},
        )


def test_sell_intent_requires_entry_signal_provenance() -> None:
    with pytest.raises(ValueError, match="entry_signal_id"):
        _intent(side=PaperSide.SELL, signal_seed="d")

    with pytest.raises(ValueError, match="entry_signal_id"):
        _intent(entry_signal_id="f" * 64)


def test_sell_consumes_only_lots_from_declared_entry_signal(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    first = _intent(signal_seed="a", quantity=1_000)
    second = _intent(
        signal_seed="b",
        quantity=500,
        event_time=BUY_TIME - timedelta(seconds=1),
    )
    store.submit_intent(
        first,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    store.submit_intent(
        second,
        decision_time=BUY_TIME + timedelta(seconds=1),
        trade_date=BUY_DATE,
        quote=_quote("12.00"),
    )
    sell_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)

    sell = store.submit_intent(
        _sell_intent(
            store,
            quantity=500,
            entry_signal_id=second.signal_id,
            decision_time=sell_time,
        ),
        decision_time=sell_time,
        trade_date=NEXT_TRADE_DATE,
        quote=_quote("13.00", available_date=None),
    )

    assert sell.status is PaperOrderStatus.FILLED
    with sqlite3.connect(path) as connection:
        consumed = connection.execute(
            """
            SELECT l.entry_signal_id, c.quantity
            FROM paper_lot_consumption AS c
            JOIN paper_lot AS l ON l.lot_id = c.lot_id
            WHERE c.fill_id = ?
            """,
            (store.fills(sell.order_id)[0].fill_id,),
        ).fetchall()
        remaining = connection.execute(
            """
            SELECT entry_signal_id, remaining_quantity
            FROM paper_lot ORDER BY entry_signal_id
            """
        ).fetchall()
    assert consumed == [(second.signal_id, 500)]
    assert remaining == [(first.signal_id, 1_000), (second.signal_id, 0)]


def test_submit_persists_independent_immutable_ledger_availability(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    persisted_at = BUY_TIME + timedelta(minutes=10)
    order = store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        persisted_at=persisted_at,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    with sqlite3.connect(path) as connection:
        fill_row = connection.execute(
            "SELECT executed_at, persisted_at FROM paper_fill WHERE order_id = ?",
            (order.order_id,),
        ).fetchone()
        lot_row = connection.execute(
            "SELECT persisted_at, entry_signal_id FROM paper_lot"
        ).fetchone()
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE paper_fill SET persisted_at = ? WHERE order_id = ?",
                ((persisted_at + timedelta(seconds=1)).isoformat(), order.order_id),
            )

    assert datetime.fromisoformat(fill_row[0].replace("Z", "+00:00")) == BUY_TIME
    assert datetime.fromisoformat(fill_row[1].replace("Z", "+00:00")) == persisted_at
    assert datetime.fromisoformat(lot_row[0].replace("Z", "+00:00")) == persisted_at
    assert lot_row[1] == "a" * 64


def test_fill_and_consumption_rows_are_append_only(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    buy = _intent()
    store.submit_intent(
        buy,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    sell_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)
    sell = store.submit_intent(
        _sell_intent(
            store,
            entry_signal_id=buy.signal_id,
            decision_time=sell_time,
        ),
        decision_time=sell_time,
        trade_date=NEXT_TRADE_DATE,
        quote=_quote("11.00", available_date=None),
    )
    sell_fill_id = str(store.fills(sell.order_id)[0].fill_id)

    with sqlite3.connect(path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE paper_fill SET quantity = 900 WHERE fill_id = ?",
                (sell_fill_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "DELETE FROM paper_fill WHERE fill_id = ?",
                (sell_fill_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE paper_lot_consumption SET quantity = 900 WHERE fill_id = ?",
                (sell_fill_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "DELETE FROM paper_lot_consumption WHERE fill_id = ?",
                (sell_fill_id,),
            )


def test_submit_rejects_persistence_before_decision(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    with pytest.raises(ValueError, match="persisted_at"):
        store.submit_intent(
            _intent(),
            decision_time=BUY_TIME,
            persisted_at=BUY_TIME - timedelta(microseconds=1),
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )


def test_partial_execution_appends_immutable_fills_and_reconciles(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    intent = _intent(quantity=1_000)

    partial = store.submit_intent(
        intent,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=400),
    )
    completed_receipt = store.apply_execution(
        partial.order_id,
        execution_id="9" * 64,
        executed_at=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quantity=600,
        quote=_quote("11.00", executable_quantity=600),
        price_snapshot_id="9" * 64,
    )
    completed = completed_receipt.order

    fills = store.fills(completed.order_id)
    assert partial.status is PaperOrderStatus.PARTIALLY_FILLED
    assert partial.filled_quantity == 400
    assert completed.status is PaperOrderStatus.FILLED
    assert completed.filled_quantity == 1_000
    assert completed.average_fill_price == Decimal("10.6000")
    assert [(fill.sequence, fill.quantity) for fill in fills] == [(1, 400), (2, 600)]
    assert store.reconcile().fill_count == 2


def test_incremental_execution_id_is_idempotent_and_conflict_checked(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    partial = store.submit_intent(
        _intent(quantity=300),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=100),
    )
    execution_id = "7" * 64
    execution_time = BUY_TIME + timedelta(minutes=1)
    persisted_at = execution_time + timedelta(seconds=1)

    first = store.apply_execution(
        partial.order_id,
        execution_id=execution_id,
        executed_at=execution_time,
        persisted_at=persisted_at,
        trade_date=BUY_DATE,
        quantity=100,
        quote=_quote("10.10", executable_quantity=100),
        price_snapshot_id="8" * 64,
    )
    retried = store.apply_execution(
        partial.order_id,
        execution_id=execution_id,
        executed_at=execution_time,
        persisted_at=persisted_at,
        trade_date=BUY_DATE,
        quantity=100,
        quote=_quote("10.10", executable_quantity=100),
        price_snapshot_id="8" * 64,
    )

    assert retried == first
    assert first.execution_id == execution_id
    assert first.fill.execution_id == execution_id
    assert first.order.filled_quantity == 200
    assert len(store.fills(partial.order_id)) == 2

    with pytest.raises(ValueError, match="execution_id|different|conflict"):
        store.apply_execution(
            partial.order_id,
            execution_id=execution_id,
            executed_at=execution_time,
            persisted_at=persisted_at,
            trade_date=BUY_DATE,
            quantity=100,
            quote=_quote("10.20", executable_quantity=100),
            price_snapshot_id="8" * 64,
        )
    with pytest.raises(ValueError, match="execution_id|different|conflict"):
        store.apply_execution(
            partial.order_id,
            execution_id=execution_id,
            executed_at=execution_time,
            persisted_at=persisted_at + timedelta(seconds=1),
            trade_date=BUY_DATE,
            quantity=100,
            quote=_quote("10.10", executable_quantity=100),
            price_snapshot_id="8" * 64,
        )


def test_initial_execution_identity_is_immutable_even_without_a_fill(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    intent = _intent(
        order_type=PaperOrderType.LIMIT,
        limit_price=Decimal("9.90"),
    )
    first = store.submit_intent(
        intent,
        execution_id="1" * 64,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    retried = store.submit_intent(
        intent,
        execution_id="1" * 64,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    receipt = store.execution("1" * 64)

    assert retried == first
    assert receipt is not None
    assert receipt.order == first
    assert receipt.fill is None
    with pytest.raises(ValueError, match="execution_id|identity|different|conflict"):
        store.submit_intent(
            intent,
            execution_id="2" * 64,
            decision_time=BUY_TIME,
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )

    reopened = _store(path, cost_policy)
    assert reopened.execution("1" * 64) == receipt
    assert reopened.order_for_execution("2" * 64) is None


def test_delayed_execution_retry_returns_original_persisted_order_snapshot(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    initial = store.submit_intent(
        _intent(quantity=300),
        execution_id="1" * 64,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=100),
    )
    second_time = BUY_TIME + timedelta(minutes=1)
    second = store.apply_execution(
        initial.order_id,
        execution_id="2" * 64,
        executed_at=second_time,
        trade_date=BUY_DATE,
        quantity=100,
        quote=_quote("10.10", executable_quantity=100),
        price_snapshot_id="8" * 64,
    )
    store.apply_execution(
        initial.order_id,
        execution_id="3" * 64,
        executed_at=BUY_TIME + timedelta(minutes=2),
        trade_date=BUY_DATE,
        quantity=100,
        quote=_quote("10.20", executable_quantity=100),
        price_snapshot_id="9" * 64,
    )

    delayed_retry = store.apply_execution(
        initial.order_id,
        execution_id="2" * 64,
        executed_at=second_time,
        trade_date=BUY_DATE,
        quantity=100,
        quote=_quote("10.10", executable_quantity=100),
        price_snapshot_id="8" * 64,
    )

    assert delayed_retry == second
    assert delayed_retry.order.filled_quantity == 200
    assert store.order(initial.order_id).filled_quantity == 300
    assert _store(path, cost_policy).execution("2" * 64) == second


def test_concurrent_initial_execution_identity_is_applied_exactly_once(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    intent = _intent(
        order_type=PaperOrderType.LIMIT,
        limit_price=Decimal("9.90"),
    )

    def submit() -> object:
        return store.submit_intent(
            intent,
            execution_id="4" * 64,
            decision_time=BUY_TIME,
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(lambda _index: submit(), range(2)))

    assert results[0] == results[1]
    receipt = store.execution("4" * 64)
    assert receipt is not None and receipt.fill is None
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM paper_intent").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM paper_order").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM paper_execution_receipt").fetchone()[0] == 1
        assert (
            connection.execute("SELECT COUNT(*) FROM paper_ledger_attestation").fetchone()[0] == 3
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM paper_ledger_head_marker").fetchone()[0] == 3
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM paper_ledger_tamper_marker").fetchone()[0] == 0
        )


def test_competing_initial_execution_ids_commit_exactly_one_identity(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    intent = _intent(
        order_type=PaperOrderType.LIMIT,
        limit_price=Decimal("9.90"),
    )

    def submit(execution_id: str) -> str:
        try:
            store.submit_intent(
                intent,
                execution_id=execution_id,
                decision_time=BUY_TIME,
                trade_date=BUY_DATE,
                quote=_quote("10.00"),
            )
        except DuplicateExecutionConflictError:
            return "conflict"
        return "committed"

    execution_ids = ("4" * 64, "5" * 64)
    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(submit, execution_ids))

    assert sorted(outcomes) == ["committed", "conflict"]
    receipts = tuple(store.execution(execution_id) for execution_id in execution_ids)
    assert sum(receipt is not None for receipt in receipts) == 1
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM paper_intent").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM paper_order").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM paper_execution_receipt").fetchone()[0] == 1


def test_concurrent_incremental_execution_id_applies_exactly_once(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    partial = store.submit_intent(
        _intent(quantity=300),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=100),
    )
    execution_time = BUY_TIME + timedelta(minutes=1)

    def execute() -> object:
        return store.apply_execution(
            partial.order_id,
            execution_id="6" * 64,
            executed_at=execution_time,
            trade_date=BUY_DATE,
            quantity=100,
            quote=_quote("10.10", executable_quantity=100),
            price_snapshot_id="8" * 64,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(lambda _index: execute(), range(2)))

    assert results[0] == results[1]
    assert results[0].order.filled_quantity == 200
    assert len(store.fills(partial.order_id)) == 2
    assert store.reconcile().fill_count == 2
    with sqlite3.connect(store.path) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM paper_ledger_attestation").fetchone()[0] == 4
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM paper_ledger_head_marker").fetchone()[0] == 4
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM paper_ledger_tamper_marker").fetchone()[0] == 0
        )


def test_weighted_average_uses_price_tick_round_half_up(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    partial = store.submit_intent(
        _intent(quantity=300),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.0000", executable_quantity=100),
    )

    receipt = store.apply_execution(
        partial.order_id,
        execution_id="5" * 64,
        executed_at=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quantity=200,
        quote=_quote("10.0001", executable_quantity=200),
        price_snapshot_id="8" * 64,
    )

    assert receipt.order.average_fill_price == Decimal("10.0001")
    assert store.order(partial.order_id).average_fill_price == Decimal("10.0001")
    assert store.reconcile().is_consistent is True


def test_sell_consumes_buy_lots_in_real_acquisition_execution_order(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    intent = _intent(quantity=200)
    partial = store.submit_intent(
        intent,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=100),
    )
    first_fill = store.fills(partial.order_id)[0]
    store.apply_execution(
        partial.order_id,
        execution_id="4" * 64,
        executed_at=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quantity=100,
        quote=_quote("20.00", executable_quantity=100),
        price_snapshot_id="8" * 64,
    )
    sell_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)
    sell = store.submit_intent(
        _sell_intent(
            store,
            entry_signal_id=intent.signal_id,
            decision_time=sell_time,
            quantity=200,
        ),
        decision_time=sell_time,
        trade_date=NEXT_TRADE_DATE,
        quote=_quote("30.00", available_date=None, executable_quantity=100),
    )

    with sqlite3.connect(path) as connection:
        consumed = connection.execute(
            "SELECT lot_id, quantity FROM paper_lot_consumption WHERE fill_id = ?",
            (store.fills(sell.order_id)[0].fill_id,),
        ).fetchall()
    assert consumed == [(first_fill.fill_id, 100)]
    assert store.reconcile().realized_pnl == Decimal("1987.00")


def test_schema_v5_has_first_class_v3_cost_receipts_and_indexed_lookup_plans(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    _store(path, cost_policy)
    with sqlite3.connect(path) as connection:
        schema_version = connection.execute(
            "SELECT schema_version FROM paper_ledger_schema WHERE singleton = 1"
        ).fetchone()[0]
        intent_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(paper_intent)").fetchall()
        }
        fill_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(paper_fill)").fetchall()
        }
        receipt_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(paper_execution_receipt)").fetchall()
        }
        migration_version = connection.execute(
            "SELECT migration_version FROM paper_ledger_attestation WHERE revision = 1"
        ).fetchone()[0]
        attestation_fks = {
            (row[2], row[3], row[4], row[6])
            for row in connection.execute(
                "PRAGMA foreign_key_list(paper_ledger_attestation)"
            ).fetchall()
        }
        head_fks = {
            (row[2], row[3], row[4], row[6])
            for row in connection.execute(
                "PRAGMA foreign_key_list(paper_ledger_head_marker)"
            ).fetchall()
        }
        intent_plan = " ".join(
            str(row[3])
            for row in connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT intent_id FROM paper_intent
                WHERE account_id = ? AND signal_id = ?
                """,
                (ACCOUNT_ID, "a" * 64),
            ).fetchall()
        )
        lot_plan = " ".join(
            str(row[3])
            for row in connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT lot_id FROM paper_lot
                WHERE account_id = ? AND ts_code = ? AND entry_signal_id = ?
                ORDER BY available_date, acquisition_trade_date,
                         buy_executed_at, buy_persisted_at, buy_fill_sequence, lot_id
                """,
                (ACCOUNT_ID, "600000.SH", "a" * 64),
            ).fetchall()
        )

    assert schema_version == 5
    assert migration_version == 3
    assert {
        "signal_id",
        "entry_signal_id",
        "ts_code",
        "side",
        "initial_execution_id",
        "initial_execution_request_fingerprint",
    } <= intent_columns
    assert {
        "execution_id",
        "transfer_fee",
        "total_fees",
        "cost_spec_id",
        "cost_spec_schema_version",
        "cost_context_fingerprint",
        "cost_provenance_state",
    } <= fill_columns
    assert {
        "execution_id",
        "request_fingerprint",
        "request_json",
        "receipt_json",
        "transfer_fee",
        "total_fees",
        "cost_spec_id",
        "cost_spec_schema_version",
        "cost_context_fingerprint",
        "cost_provenance_state",
        "persisted_at",
    } <= receipt_columns
    assert "idx_paper_intent_account_signal" in intent_plan
    assert "idx_paper_lot_position_fifo" in lot_plan
    assert (
        "paper_ledger_attestation",
        "previous_attestation_fingerprint",
        "attestation_fingerprint",
        "RESTRICT",
    ) in attestation_fks
    assert (
        "paper_ledger_attestation",
        "attestation_fingerprint",
        "attestation_fingerprint",
        "RESTRICT",
    ) in head_fks
    assert (
        "paper_ledger_head_marker",
        "previous_head_marker_fingerprint",
        "head_marker_fingerprint",
        "RESTRICT",
    ) in head_fks


def test_schema_v5_rejects_unbound_new_account_and_fill_rows(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    partial = store.submit_intent(
        _intent(quantity=200),
        execution_id="1" * 64,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=100),
    )
    spec = cost_policy.execution_cost_spec
    assert spec.cost_spec_id is not None

    with sqlite3.connect(path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="known v3 cost binding"):
            connection.execute(
                """
                INSERT INTO broker_account(
                    account_id, initial_cash, cash, realized_pnl, cost_policy_fingerprint,
                    cost_spec_id, cost_spec_schema_version, cost_provenance_state
                ) VALUES (?, '1000', '1000', '0', ?, ?, 3, NULL)
                """,
                ("paper-unbound", spec.cost_spec_id, spec.cost_spec_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="requires KNOWN_V3 cost evidence"):
            connection.execute(
                """
                INSERT INTO paper_fill(
                    fill_id, execution_id, order_id, sequence, quantity, price, commission,
                    transfer_fee, tax, total_fees, cost_spec_id, cost_spec_schema_version,
                    cost_context_fingerprint, cost_provenance_state,
                    executed_at, persisted_at, price_snapshot_id
                ) VALUES (?, ?, ?, 2, 100, '10', '0', '0', '0', '0', ?, 3, ?, NULL, ?, ?, ?)
                """,
                (
                    "f" * 64,
                    "e" * 64,
                    partial.order_id,
                    spec.cost_spec_id,
                    "d" * 64,
                    BUY_TIME.isoformat().replace("+00:00", "Z"),
                    BUY_TIME.isoformat().replace("+00:00", "Z"),
                    "9" * 64,
                ),
            )


def test_trusted_v5_reopen_uses_only_bounded_attestation_checks(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    statements: list[str] = []
    real_connect = paper_broker_module.sqlite3.connect

    def traced_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(paper_broker_module.sqlite3, "connect", traced_connect)
    reopened = _store(path, cost_policy)

    assert reopened.ledger_trust_status().state == "trusted"
    traced = "\n".join(statements).lower()
    assert "begin immediate" not in traced
    assert "select intent_id, payload_json from paper_intent" not in traced
    assert "update paper_lot" not in traced
    assert "count(*) from paper_intent" not in traced
    assert "count(*) from paper_order" not in traced
    assert "count(*) from paper_fill" not in traced
    assert "count(*) from paper_lot" not in traced
    assert "count(*) from paper_execution_receipt" not in traced
    assert "foreign_key_check" not in traced
    for table in ("paper_ledger_attestation", "paper_ledger_head_marker"):
        reads = [
            statement
            for statement in statements
            if statement.lower().lstrip().startswith("select")
            and f"from {table}" in statement.lower()
        ]
        assert reads
        assert all("limit 1" in statement.lower() for statement in reads)


def test_attestation_is_immutable_and_missing_or_rolled_back_head_quarantines(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    with sqlite3.connect(path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE paper_ledger_attestation SET event_kind = 'tampered' WHERE revision = 1"
            )
        connection.execute("DROP TRIGGER paper_ledger_attestation_delete_immutable")
        connection.execute(
            "DELETE FROM paper_ledger_attestation "
            "WHERE revision = (SELECT MAX(revision) FROM paper_ledger_attestation)"
        )

    reopened = _store(path, cost_policy)
    assert reopened.ledger_trust_status().state == "quarantined"
    with pytest.raises(PaperBrokerReconciliationError, match="quarantin|untrusted"):
        reopened.submit_intent(
            _intent(signal_seed="b", event_time=BUY_TIME + timedelta(seconds=1)),
            decision_time=BUY_TIME + timedelta(seconds=2),
            trade_date=BUY_DATE,
            quote=_quote("11.00"),
        )


def _seed_five_attestation_revisions(
    path: Path,
    cost_policy: BrokerCostPolicy,
) -> PaperBrokerStore:
    store = _store(path, cost_policy)
    intent = _intent(quantity=600)
    order = store.submit_intent(
        intent,
        execution_id="0" * 64,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=200),
    )
    store.apply_execution(
        order.order_id,
        execution_id="1" * 64,
        executed_at=BUY_TIME + timedelta(minutes=1),
        persisted_at=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quantity=200,
        quote=_quote("10.10", executable_quantity=200),
        price_snapshot_id="8" * 64,
    )
    store.account_authority_snapshot(
        as_of=BUY_TIME + timedelta(minutes=2),
        market_prices={"600000.SH": Decimal("10.10")},
        producer_commit=PRODUCER_COMMIT,
    )
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("SELECT MAX(revision) FROM paper_ledger_attestation").fetchone()[0]
            == 5
        )
    return store


def _delete_attestation_with_protection_restored(
    path: Path,
    revision: int,
    *,
    suppress_tamper_marker: bool = False,
) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        trigger_names = ["paper_ledger_attestation_delete_immutable"]
        if suppress_tamper_marker:
            trigger_names.append("paper_ledger_attestation_delete_tamper")
        trigger_sql = [
            str(
                connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
                    (name,),
                ).fetchone()[0]
            )
            for name in trigger_names
        ]
        for name in trigger_names:
            connection.execute(f'DROP TRIGGER "{name}"')
        connection.execute(
            "DELETE FROM paper_ledger_attestation WHERE revision = ?",
            (revision,),
        )
        for statement in trigger_sql:
            connection.execute(statement)


def test_selective_attestation_head_rollback_quarantines_after_ddl_is_restored(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    _seed_five_attestation_revisions(path, cost_policy)
    before = _ledger_rows(path)

    _delete_attestation_with_protection_restored(
        path,
        revision=5,
        suppress_tamper_marker=True,
    )

    reopened = _store(path, cost_policy)
    assert reopened.ledger_trust_status().state == "quarantined"
    assert _ledger_rows(path) == before
    with pytest.raises(PaperBrokerReconciliationError, match="quarantin|untrusted"):
        reopened.submit_intent(
            _intent(signal_seed="b", event_time=BUY_TIME + timedelta(minutes=3)),
            execution_id="2" * 64,
            decision_time=BUY_TIME + timedelta(minutes=3, seconds=2),
            trade_date=BUY_DATE,
            quote=_quote("10.20"),
        )
    assert _ledger_rows(path) == before
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "SELECT target_revision, reason FROM paper_ledger_tamper_marker"
            ).fetchall()
            == []
        )
        assert (
            connection.execute("SELECT MAX(revision) FROM paper_ledger_head_marker").fetchone()[0]
            == 5
        )


def test_selective_attestation_middle_chain_deletion_quarantines_after_ddl_is_restored(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    _seed_five_attestation_revisions(path, cost_policy)
    before = _ledger_rows(path)

    _delete_attestation_with_protection_restored(path, revision=3)

    reopened = _store(path, cost_policy)
    assert reopened.ledger_trust_status().state == "quarantined"
    assert _ledger_rows(path) == before
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT target_revision, reason FROM paper_ledger_tamper_marker"
        ).fetchall() == [(3, "attestation_deleted")]
        assert (
            connection.execute("SELECT MAX(revision) FROM paper_ledger_head_marker").fetchone()[0]
            == 5
        )


def test_business_writes_advance_attested_counts_once_and_idempotent_retry_does_not(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    intent = _intent(quantity=600)
    first = store.submit_intent(
        intent,
        execution_id="0" * 64,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=200),
    )
    retry = store.submit_intent(
        intent,
        execution_id="0" * 64,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=200),
    )
    store.apply_execution(
        first.order_id,
        execution_id="1" * 64,
        executed_at=BUY_TIME + timedelta(minutes=1),
        persisted_at=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quantity=200,
        quote=_quote("10.10", executable_quantity=200),
        price_snapshot_id="8" * 64,
    )
    store.account_authority_snapshot(
        as_of=BUY_TIME + timedelta(minutes=2),
        market_prices={"600000.SH": Decimal("10.10")},
        producer_commit=PRODUCER_COMMIT,
    )

    assert retry == first
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT * FROM paper_ledger_attestation ORDER BY revision"
        ).fetchall()
        head_rows = connection.execute(
            "SELECT * FROM paper_ledger_head_marker ORDER BY revision"
        ).fetchall()

    assert [row["event_kind"] for row in rows] == [
        "migration_audit",
        "account_bootstrap",
        "intent_execution",
        "incremental_execution",
        "account_authority",
    ]
    latest = rows[-1]
    assert [row["revision"] for row in head_rows] == [1, 2, 3, 4, 5]
    assert [row["attestation_fingerprint"] for row in head_rows] == [
        row["attestation_fingerprint"] for row in rows
    ]
    assert {
        "broker_account_count": latest["broker_account_count"],
        "intent_count": latest["intent_count"],
        "order_count": latest["order_count"],
        "fill_count": latest["fill_count"],
        "lot_count": latest["lot_count"],
        "consumption_count": latest["consumption_count"],
        "receipt_count": latest["receipt_count"],
        "authority_count": latest["authority_count"],
    } == {
        "broker_account_count": 1,
        "intent_count": 1,
        "order_count": 1,
        "fill_count": 2,
        "lot_count": 2,
        "consumption_count": 0,
        "receipt_count": 2,
        "authority_count": 1,
    }


def test_v3_unknown_execution_evidence_quarantines_every_broker_write(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    seeded = _store(path, cost_policy)
    partial = seeded.submit_intent(
        _intent(quantity=1_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=500),
    )
    _downgrade_v3_initial_execution_evidence(path)

    store = _store(path, cost_policy)
    diagnostic = store.ledger_trust_status()
    unknown = {item.field: item.count for item in diagnostic.unknown_evidence}
    assert diagnostic.state == "quarantined"
    assert unknown["unknown_initial_execution_identity_count"] == 1
    assert unknown["unknown_execution_receipt_count"] == 1
    before = _ledger_rows(path)

    assert store.order(partial.order_id) == partial
    assert len(store.fills(partial.order_id)) == 1
    with pytest.raises(PaperBrokerReconciliationError, match="quarantin|untrusted|audit-only"):
        store.fills()
    with pytest.raises(PaperBrokerReconciliationError, match="quarantin|untrusted|audit-only"):
        store.account_snapshot(
            as_of=BUY_TIME,
            market_prices={"600000.SH": Decimal("10.00")},
        )
    with pytest.raises(PaperBrokerReconciliationError, match="quarantin|untrusted|audit-only"):
        store.latest_execution_prices(as_of=BUY_TIME)

    with pytest.raises(PaperBrokerReconciliationError, match="quarantin|untrusted|audit-only"):
        store.submit_intent(
            _intent(signal_seed="b", event_time=BUY_TIME),
            execution_id="b" * 64,
            decision_time=BUY_TIME + timedelta(seconds=2),
            trade_date=BUY_DATE,
            quote=_quote("11.00"),
        )
    with pytest.raises(PaperBrokerReconciliationError, match="quarantin|untrusted|audit-only"):
        store.apply_execution(
            partial.order_id,
            execution_id="c" * 64,
            executed_at=BUY_TIME + timedelta(minutes=1),
            trade_date=BUY_DATE,
            quantity=500,
            quote=_quote("10.10", executable_quantity=500),
            price_snapshot_id="8" * 64,
        )
    with pytest.raises(PaperBrokerReconciliationError, match="quarantin|untrusted|audit-only"):
        store.close_open_order(
            partial.order_id,
            status=PaperOrderStatus.CANCELLED,
            decided_at=BUY_TIME + timedelta(minutes=1),
        )
    with pytest.raises(PaperBrokerReconciliationError, match="quarantin|untrusted|audit-only"):
        store.account_authority_snapshot(
            as_of=BUY_TIME,
            market_prices={"600000.SH": Decimal("10.00")},
            producer_commit=PRODUCER_COMMIT,
        )
    with pytest.raises(PaperBrokerReconciliationError, match="quarantin|untrusted|audit-only"):
        store.reconcile()

    assert _ledger_rows(path) == before


def test_v4_migration_keeps_legacy_cost_evidence_null_and_requires_a_fresh_binding(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    new_store = _store(tmp_path / "new.sqlite3", cost_policy)
    assert new_store.ledger_trust_status().state == "trusted"

    path = tmp_path / "migratable.sqlite3"
    seeded = _store(path, cost_policy)
    seeded.submit_intent(
        _intent(),
        execution_id="1" * 64,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    with sqlite3.connect(path) as connection:
        before_account = connection.execute(
            "SELECT cash, realized_pnl FROM broker_account WHERE account_id = ?",
            (ACCOUNT_ID,),
        ).fetchone()
        _replace_schema_metadata_with_v3(connection)

    migrated = _store(path, cost_policy)
    diagnostic = migrated.ledger_trust_status()
    assert diagnostic.state == "quarantined"
    unknown = {item.field: item.count for item in diagnostic.unknown_evidence}
    assert unknown["unknown_cost_provenance_count"] == 3
    with sqlite3.connect(path) as connection:
        account = connection.execute(
            "SELECT cash, realized_pnl, cost_spec_id, cost_spec_schema_version, "
            "cost_provenance_state FROM broker_account WHERE account_id = ?",
            (ACCOUNT_ID,),
        ).fetchone()
        fill = connection.execute(
            "SELECT transfer_fee, total_fees, cost_spec_id, cost_spec_schema_version, "
            "cost_context_fingerprint, cost_provenance_state FROM paper_fill"
        ).fetchone()
        receipt = connection.execute(
            "SELECT transfer_fee, total_fees, cost_spec_id, cost_spec_schema_version, "
            "cost_context_fingerprint, cost_provenance_state FROM paper_execution_receipt"
        ).fetchone()
        assert account == (*before_account, None, None, "LEGACY_UNKNOWN")
        assert fill == (None, None, None, None, None, "LEGACY_UNKNOWN")
        assert receipt == (None, None, None, None, None, "LEGACY_UNKNOWN")
        assert (
            connection.execute("SELECT COUNT(*) FROM paper_ledger_attestation").fetchone()[0] == 1
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM paper_ledger_head_marker").fetchone()[0] == 1
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM paper_ledger_tamper_marker").fetchone()[0] == 0
        )
    with pytest.raises(PaperBrokerReconciliationError, match="audit-only"):
        migrated.submit_intent(
            _intent(
                signal_seed="b",
                event_time=BUY_TIME + timedelta(seconds=1),
            ),
            execution_id="2" * 64,
            decision_time=BUY_TIME + timedelta(seconds=2),
            trade_date=BUY_DATE,
            quote=_quote("11.00"),
        )

    fresh = PaperBrokerStore(
        path,
        account_id="paper-v5-fresh",
        initial_cash=Decimal("100000"),
        cost_policy=cost_policy,
    )
    fresh.submit_intent(
        _intent(
            signal_seed="b",
            event_time=BUY_TIME + timedelta(seconds=1),
            account_id="paper-v5-fresh",
        ),
        execution_id="2" * 64,
        decision_time=BUY_TIME + timedelta(seconds=2),
        trade_date=BUY_DATE,
        quote=_quote("11.00"),
    )
    assert fresh.reconcile().order_count == 1
    assert (
        PaperBrokerLifecycleReader(path, account_id="paper-v5-fresh").account_id == "paper-v5-fresh"
    )


def test_incremental_execution_rejects_backdated_sequence_without_mutation(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    partial = store.submit_intent(
        _intent(quantity=1_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=200),
    )
    store.apply_execution(
        partial.order_id,
        execution_id="1" * 64,
        executed_at=BUY_TIME + timedelta(minutes=2),
        trade_date=BUY_DATE,
        quantity=200,
        quote=_quote("10.10", executable_quantity=200),
        price_snapshot_id="8" * 64,
    )
    before_order = store.order(partial.order_id)
    before_fills = store.fills(partial.order_id)
    with sqlite3.connect(path) as connection:
        before_rows = tuple(
            connection.execute(
                "SELECT * FROM paper_fill WHERE order_id = ? ORDER BY sequence",
                (partial.order_id,),
            ).fetchall()
        )
        before_account = connection.execute(
            "SELECT cash, realized_pnl FROM broker_account WHERE account_id = ?",
            (ACCOUNT_ID,),
        ).fetchone()

    with pytest.raises(ValueError, match="executed_at|execution time|monotonic"):
        store.apply_execution(
            partial.order_id,
            execution_id="2" * 64,
            executed_at=BUY_TIME + timedelta(minutes=1),
            trade_date=BUY_DATE,
            quantity=200,
            quote=_quote("10.20", executable_quantity=200),
            price_snapshot_id="9" * 64,
        )

    assert store.order(partial.order_id) == before_order
    assert store.fills(partial.order_id) == before_fills
    with sqlite3.connect(path) as connection:
        after_rows = tuple(
            connection.execute(
                "SELECT * FROM paper_fill WHERE order_id = ? ORDER BY sequence",
                (partial.order_id,),
            ).fetchall()
        )
        after_account = connection.execute(
            "SELECT cash, realized_pnl FROM broker_account WHERE account_id = ?",
            (ACCOUNT_ID,),
        ).fetchone()
    assert after_rows == before_rows
    assert after_account == before_account
    assert store.reconcile().fill_count == 2


def test_incremental_execution_rejects_availability_before_previous_fill(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    partial = store.submit_intent(
        _intent(quantity=1_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=200),
    )
    second_execution = BUY_TIME + timedelta(minutes=1)
    second_persistence = BUY_TIME + timedelta(minutes=3)
    store.apply_execution(
        partial.order_id,
        execution_id="1" * 64,
        executed_at=second_execution,
        persisted_at=second_persistence,
        trade_date=BUY_DATE,
        quantity=200,
        quote=_quote("10.10", executable_quantity=200),
        price_snapshot_id="8" * 64,
    )

    with pytest.raises(ValueError, match="persisted_at|availability|monotonic"):
        store.apply_execution(
            partial.order_id,
            execution_id="2" * 64,
            executed_at=BUY_TIME + timedelta(minutes=2),
            persisted_at=BUY_TIME + timedelta(minutes=2),
            trade_date=BUY_DATE,
            quantity=200,
            quote=_quote("10.20", executable_quantity=200),
            price_snapshot_id="9" * 64,
        )

    assert [(fill.sequence, fill.quantity) for fill in store.fills(partial.order_id)] == [
        (1, 200),
        (2, 200),
    ]
    assert store.reconcile().fill_count == 2


@pytest.mark.parametrize(
    ("table", "column", "value"),
    (
        ("paper_fill", "executed_at", "2026-07-31T01:31:30+00:00"),
        ("paper_fill", "persisted_at", "2026-07-31T01:33:00+00:00"),
        ("paper_order", "updated_at", "2026-07-31T01:33:00+00:00"),
    ),
)
def test_reconcile_rejects_nonmonotonic_incremental_fill_timestamps(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
    table: str,
    column: str,
    value: str,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    partial = store.submit_intent(
        _intent(quantity=1_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=200),
    )
    store.apply_execution(
        partial.order_id,
        execution_id="1" * 64,
        executed_at=BUY_TIME + timedelta(minutes=1),
        persisted_at=BUY_TIME + timedelta(minutes=3),
        trade_date=BUY_DATE,
        quantity=200,
        quote=_quote("10.10", executable_quantity=200),
        price_snapshot_id="8" * 64,
    )
    store.apply_execution(
        partial.order_id,
        execution_id="2" * 64,
        executed_at=BUY_TIME + timedelta(minutes=2),
        persisted_at=BUY_TIME + timedelta(minutes=4),
        trade_date=BUY_DATE,
        quantity=200,
        quote=_quote("10.20", executable_quantity=200),
        price_snapshot_id="9" * 64,
    )
    assert store.reconcile().fill_count == 3

    with sqlite3.connect(path) as connection:
        if table == "paper_fill":
            trigger_sql = [
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT sql FROM sqlite_master
                    WHERE type = 'trigger' AND name IN (
                        'paper_fill_persisted_at_immutable',
                        'paper_fill_row_immutable'
                    )
                    ORDER BY name
                    """
                ).fetchall()
            ]
            connection.execute("DROP TRIGGER paper_fill_persisted_at_immutable")
            connection.execute("DROP TRIGGER paper_fill_row_immutable")
            connection.execute(
                f"UPDATE {table} SET {column} = ? WHERE order_id = ? AND sequence = 3",
                (value, partial.order_id),
            )
            for statement in trigger_sql:
                connection.execute(statement)
        else:
            connection.execute(
                f"UPDATE {table} SET {column} = ? WHERE order_id = ?",
                (value, partial.order_id),
            )

    with pytest.raises(
        PaperBrokerReconciliationError,
        match="sequence|monotonic|availability|updated_at|timestamp",
    ):
        store.reconcile()


def test_partially_filled_order_can_cancel_only_unfilled_remainder(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    partial = store.submit_intent(
        _intent(quantity=1_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=400),
    )

    cancelled = store.close_open_order(
        partial.order_id,
        status=PaperOrderStatus.CANCELLED,
        decided_at=BUY_TIME + timedelta(minutes=1),
    )

    assert cancelled.status is PaperOrderStatus.CANCELLED
    assert cancelled.filled_quantity == 400
    with pytest.raises(ValueError, match="open|CANCELLED"):
        store.apply_execution(
            partial.order_id,
            execution_id="3" * 64,
            executed_at=BUY_TIME + timedelta(minutes=2),
            trade_date=BUY_DATE,
            quantity=600,
            quote=_quote("11.00", executable_quantity=600),
            price_snapshot_id="9" * 64,
        )
    assert store.reconcile().open_lot_quantity == 400


def test_zero_fill_sell_order_provenance_is_reconciled(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    buy = _intent()
    store.submit_intent(
        buy,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    sell_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)
    accepted = store.submit_intent(
        _sell_intent(
            store,
            entry_signal_id=buy.signal_id,
            decision_time=sell_time,
            order_type=PaperOrderType.LIMIT,
            limit_price=Decimal("12.00"),
        ),
        decision_time=sell_time,
        trade_date=NEXT_TRADE_DATE,
        quote=_quote("11.00", available_date=None),
    )
    assert accepted.status is PaperOrderStatus.ACCEPTED
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE paper_order SET entry_signal_id = ? WHERE order_id = ?",
            ("f" * 64, accepted.order_id),
        )

    with pytest.raises(PaperBrokerReconciliationError, match="entry_signal_id|provenance"):
        store.reconcile()
