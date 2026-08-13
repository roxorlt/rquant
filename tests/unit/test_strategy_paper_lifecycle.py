from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.feature_contracts import FeatureAvailability, FeatureFieldStatus
from rquant.paper_broker import (
    BrokerExecutionContext,
    PaperBrokerReconciliationError,
    PaperBrokerStore,
)
from rquant.paper_contracts import (
    PaperOrderIntent,
    PaperOrderStatus,
    PaperOrderType,
    PaperSide,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.strategy_paper_lifecycle import (
    PaperBrokerLifecycleReader,
    PaperLifecycleIntegrityError,
)
from tests.paper_cost_fixtures import paper_cost_policy, paper_instrument_context

COMMIT = "a" * 40
BUY_AT = datetime(2026, 7, 31, 1, 40, tzinfo=UTC)
T1_AT = datetime(2026, 8, 3, 1, 40, tzinfo=UTC)
CODE = "600000.SH"


def _broker(
    path: Path,
    *,
    price_tick: Decimal = Decimal("0.0001"),
) -> PaperBrokerStore:
    return PaperBrokerStore(
        path,
        account_id="paper-main",
        initial_cash=Decimal("200000"),
        cost_policy=paper_cost_policy(price_tick=price_tick),
    )


def _entry_signal(*, strategy_id: str = "n_shape", event_time: datetime = BUY_AT) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id=strategy_id,
        strategy_version="1",
        parameter_fingerprint="b" * 64,
        dataset_snapshot_id="c" * 64,
        feature_snapshot_id="d" * 64,
        event_time=event_time,
        available_at=event_time,
        candidate_id=CODE,
        action=SignalAction.B_INTENT,
        reason_codes=("entry",),
        evidence={"session_low": 10.5},
        expires_at=event_time + timedelta(minutes=5),
        producer_commit=COMMIT,
    )


def _exit_signal(
    entry_signal: SignalEnvelope,
    *,
    action: SignalAction = SignalAction.S_INTENT,
    event_time: datetime = T1_AT,
    seed: str = "9",
) -> SignalEnvelope:
    tranche_fraction = 0.5 if action is SignalAction.REDUCE else 1.0
    return SignalEnvelope(
        schema_version=1,
        strategy_id=entry_signal.strategy_id,
        strategy_version=entry_signal.strategy_version,
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="c" * 64,
        feature_snapshot_id="d" * 64,
        event_time=event_time,
        available_at=event_time,
        candidate_id=CODE,
        action=action,
        reason_codes=("exit",),
        evidence={
            "entry_signal_id": entry_signal.signal_id,
            "sell_tranche_fraction": tranche_fraction,
        },
        expires_at=event_time + timedelta(minutes=5),
        producer_commit=COMMIT,
    )


def _submit_buy(
    broker: PaperBrokerStore,
    signal: SignalEnvelope,
    *,
    quantity: int = 1000,
    price: str = "11.00",
    executed_at: datetime = BUY_AT,
    persisted_at: datetime | None = None,
    executable_quantity: int | None = None,
) -> str:
    intent = PaperOrderIntent(
        signal_id=signal.signal_id,
        account_id="paper-main",
        ts_code=CODE,
        side=PaperSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=quantity,
        event_time=executed_at,
        available_at=executed_at,
        expires_at=executed_at + timedelta(minutes=5),
        earliest_execution_at=executed_at,
        price_snapshot_id="e" * 64,
        producer_commit=COMMIT,
    )
    order = broker.submit_intent(
        intent,
        decision_time=executed_at,
        persisted_at=persisted_at,
        trade_date=executed_at.astimezone().date(),
        quote=BrokerExecutionContext(
            executable_price=Decimal(price),
            acquisition_available_date=date(2026, 8, 3),
            instrument_context=paper_instrument_context(CODE),
            executable_quantity=executable_quantity,
        ),
    )
    return str(order.order_id)


def _submit_sell(
    broker: PaperBrokerStore,
    entry_signal: SignalEnvelope,
    *,
    quantity: int,
    executed_at: datetime = T1_AT,
    signal: SignalEnvelope | None = None,
    persisted_at: datetime | None = None,
    order_type: PaperOrderType = PaperOrderType.MARKET,
    limit_price: Decimal | None = None,
    executable_price: str = "12.00",
    executable_quantity: int | None = None,
    suspended: bool = False,
) -> SignalEnvelope:
    signal = signal or _exit_signal(entry_signal, event_time=executed_at)
    authority = broker.sell_quantity_authority(
        exit_signal_id=str(signal.signal_id),
        entry_signal_id=str(entry_signal.signal_id),
        ts_code=CODE,
        action=signal.action.name,
        tranche_fraction=Decimal(str(signal.evidence["sell_tranche_fraction"])),
        decision_cutoff=executed_at,
        trade_date=date(2026, 8, 3),
    )
    assert authority.requested_quantity == quantity
    intent = PaperOrderIntent(
        signal_id=signal.signal_id,
        entry_signal_id=entry_signal.signal_id,
        sell_quantity_authority=authority,
        account_id="paper-main",
        ts_code=CODE,
        side=PaperSide.SELL,
        order_type=order_type,
        quantity=quantity,
        limit_price=limit_price,
        event_time=executed_at,
        available_at=executed_at,
        expires_at=executed_at + timedelta(minutes=5),
        earliest_execution_at=executed_at,
        price_snapshot_id="1" * 64,
        producer_commit=COMMIT,
    )
    broker.submit_intent(
        intent,
        decision_time=executed_at,
        persisted_at=persisted_at,
        trade_date=date(2026, 8, 3),
        quote=BrokerExecutionContext(
            executable_price=Decimal(executable_price),
            executable_quantity=executable_quantity,
            suspended=suspended,
            instrument_context=paper_instrument_context(CODE),
        ),
    )
    return signal


def _resolve(
    path: Path,
    signal: SignalEnvelope,
    *,
    cutoff: datetime,
    latest_close: float = 11.5,
    session_high: float = 12.0,
    previous_high: float | None = None,
    previous_high_at: datetime | None = None,
    exit_signals: tuple[SignalEnvelope, ...] = (),
) -> dict[str, object]:
    market_event_time = cutoff - timedelta(seconds=1)
    result = PaperBrokerLifecycleReader(path, account_id="paper-main").resolve(
        candidate_id=CODE,
        entry_signal=signal,
        exit_signals=exit_signals,
        decision_cutoff=cutoff,
        market_features={"latest_close": latest_close, "session_high": session_high},
        market_feature_statuses={
            "session_high": FeatureFieldStatus(
                candidate_id=CODE,
                name="session_high",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=market_event_time,
                available_at=cutoff,
                decision_cutoff=cutoff,
                actual_delay_seconds=1.0,
            )
        },
        previous_eligible_high_price_raw=previous_high,
        previous_high_source_event_time=previous_high_at,
        previous_high_available_at=previous_high_at,
    )
    return dict(result.values)


@pytest.mark.parametrize("damage", ("missing_fill", "weighted_price", "lot_source"))
def test_entry_lifecycle_fails_closed_when_fill_ledger_does_not_reconcile(
    tmp_path: Path,
    damage: str,
) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    signal = _entry_signal()
    order_id = _submit_buy(broker, signal)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        if damage == "missing_fill":
            connection.execute("DROP TRIGGER paper_fill_delete_immutable")
            connection.execute("DELETE FROM paper_fill WHERE order_id = ?", (order_id,))
        elif damage == "weighted_price":
            connection.execute("DROP TRIGGER paper_fill_row_immutable")
            connection.execute(
                "UPDATE paper_fill SET price = '12.00' WHERE order_id = ?",
                (order_id,),
            )
        else:
            connection.execute(
                "UPDATE paper_lot SET original_quantity = 900 WHERE lot_id IN "
                "(SELECT fill_id FROM paper_fill WHERE order_id = ?)",
                (order_id,),
            )

    with pytest.raises(PaperLifecycleIntegrityError, match="fill|price|lot|reconcile"):
        _resolve(path, signal, cutoff=BUY_AT + timedelta(seconds=1))


def test_position_is_rebuilt_at_cutoff_before_future_sell_consumption(
    tmp_path: Path,
) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    signal = _entry_signal()
    _submit_buy(broker, signal)
    exit_signal = _exit_signal(signal, action=SignalAction.REDUCE)
    _submit_sell(
        broker,
        signal,
        quantity=500,
        executed_at=T1_AT,
        signal=exit_signal,
    )

    before_sell = _resolve(path, signal, cutoff=T1_AT - timedelta(seconds=1))
    after_sell = _resolve(
        path,
        signal,
        cutoff=T1_AT + timedelta(seconds=1),
        exit_signals=(exit_signal,),
    )

    assert before_sell["remaining_position_fraction"] == pytest.approx(1.0)
    assert after_sell["remaining_position_fraction"] == pytest.approx(0.5)


def test_same_account_code_is_isolated_by_entry_fill_provenance(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    first = _entry_signal(strategy_id="n_shape")
    second = _entry_signal(
        strategy_id="growth_board_surge",
        event_time=BUY_AT + timedelta(seconds=1),
    )
    _submit_buy(broker, first, quantity=1000)
    _submit_buy(
        broker,
        second,
        quantity=500,
        price="12.00",
        executed_at=BUY_AT + timedelta(seconds=1),
    )

    first_values = _resolve(path, first, cutoff=BUY_AT + timedelta(seconds=2))
    second_values = _resolve(path, second, cutoff=BUY_AT + timedelta(seconds=2))

    assert first_values["entry_price_raw"] == pytest.approx(11.0)
    assert first_values["remaining_position_fraction"] == pytest.approx(1.0)
    assert second_values["entry_price_raw"] == pytest.approx(12.0)
    assert second_values["remaining_position_fraction"] == pytest.approx(1.0)


def test_previous_pit_high_survives_lower_session_and_partial_sell(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    signal = _entry_signal()
    _submit_buy(broker, signal)
    exit_signal = _exit_signal(signal, action=SignalAction.REDUCE)
    _submit_sell(
        broker,
        signal,
        quantity=500,
        executed_at=T1_AT,
        signal=exit_signal,
    )
    previous_high_at = T1_AT - timedelta(minutes=5)

    values = _resolve(
        path,
        signal,
        cutoff=T1_AT + timedelta(seconds=1),
        latest_close=11.5,
        session_high=12.0,
        previous_high=14.0,
        previous_high_at=previous_high_at,
        exit_signals=(exit_signal,),
    )

    assert values["eligible_high_price_raw"] == pytest.approx(14.0)
    assert values["remaining_position_fraction"] == pytest.approx(0.5)


def test_structure_fill_and_high_keep_independent_source_timestamps(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    signal = _entry_signal(event_time=BUY_AT - timedelta(minutes=2))
    _submit_buy(broker, signal, executed_at=BUY_AT)
    cutoff = BUY_AT + timedelta(minutes=3)
    market_event_time = cutoff - timedelta(seconds=1)
    result = PaperBrokerLifecycleReader(path, account_id="paper-main").resolve(
        candidate_id=CODE,
        entry_signal=signal,
        exit_signals=(),
        decision_cutoff=cutoff,
        market_features={"latest_close": 12.5, "session_high": 13.0},
        market_feature_statuses={
            "session_high": FeatureFieldStatus(
                candidate_id=CODE,
                name="session_high",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=market_event_time,
                available_at=cutoff,
                decision_cutoff=cutoff,
                actual_delay_seconds=1.0,
            )
        },
        previous_eligible_high_price_raw=None,
        previous_high_source_event_time=None,
        previous_high_available_at=None,
    )

    structure = result.field_status("structure_stop_price_raw")
    entry_price = result.field_status("entry_price_raw")
    eligible_high = result.field_status("eligible_high_price_raw")
    assert structure is not None and entry_price is not None and eligible_high is not None
    assert structure.source_event_time == signal.event_time
    assert structure.available_at == signal.available_at
    assert entry_price.source_event_time == BUY_AT
    assert eligible_high.source_event_time == market_event_time


def test_entry_and_position_fields_bind_their_actual_ledger_events(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    signal = _entry_signal(event_time=BUY_AT - timedelta(minutes=2))
    _submit_buy(broker, signal, executed_at=BUY_AT)
    exit_signal = _exit_signal(signal, action=SignalAction.REDUCE)
    _submit_sell(
        broker,
        signal,
        quantity=500,
        executed_at=T1_AT,
        signal=exit_signal,
    )
    cutoff = T1_AT + timedelta(seconds=1)
    result = PaperBrokerLifecycleReader(path, account_id="paper-main").resolve(
        candidate_id=CODE,
        entry_signal=signal,
        decision_cutoff=cutoff,
        market_features={"latest_close": 12.5, "session_high": 13.0},
        market_feature_statuses={
            "session_high": FeatureFieldStatus(
                candidate_id=CODE,
                name="session_high",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=cutoff - timedelta(seconds=1),
                available_at=cutoff,
                decision_cutoff=cutoff,
                actual_delay_seconds=1.0,
            )
        },
        previous_eligible_high_price_raw=None,
        previous_high_source_event_time=None,
        previous_high_available_at=None,
        exit_signals=(exit_signal,),
    )

    entry = result.field_status("entry_price_raw")
    remaining = result.field_status("remaining_position_fraction")
    sellable = result.field_status("position_sellable")
    assert entry is not None and remaining is not None and sellable is not None
    assert entry.source_event_time == BUY_AT
    assert remaining.source_event_time == T1_AT
    assert sellable.source_event_time == T1_AT


def test_pending_exit_suppresses_duplicate_and_rejected_exit_is_retryable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    entry = _entry_signal()
    _submit_buy(broker, entry)
    pending_signal = _exit_signal(entry, seed="8")
    _submit_sell(
        broker,
        entry,
        quantity=1_000,
        signal=pending_signal,
        order_type=PaperOrderType.LIMIT,
        limit_price=Decimal("13.00"),
        executable_price="12.00",
    )

    pending = _resolve(
        path,
        entry,
        cutoff=T1_AT + timedelta(seconds=1),
        exit_signals=(pending_signal,),
    )
    assert pending["exit_execution_status"] == "pending"
    assert pending["remaining_position_fraction"] == pytest.approx(1.0)

    rejected_signal = _exit_signal(
        entry,
        event_time=T1_AT + timedelta(minutes=1),
        seed="7",
    )
    _submit_sell(
        broker,
        entry,
        quantity=1_000,
        executed_at=T1_AT + timedelta(minutes=1),
        signal=rejected_signal,
        suspended=True,
    )
    retryable = _resolve(
        path,
        entry,
        cutoff=T1_AT + timedelta(minutes=1, seconds=1),
        exit_signals=(pending_signal, rejected_signal),
    )
    assert retryable["exit_execution_status"] == "retryable"
    assert retryable["remaining_position_fraction"] == pytest.approx(1.0)


def test_future_order_update_without_visible_fill_uses_cutoff_visible_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    entry = _entry_signal()
    _submit_buy(broker, entry)
    exit_signal = _exit_signal(entry, seed="7")
    _submit_sell(
        broker,
        entry,
        quantity=1_000,
        signal=exit_signal,
        order_type=PaperOrderType.LIMIT,
        limit_price=Decimal("20.00"),
        executable_price="12.00",
    )
    with sqlite3.connect(path) as connection:
        order_id = connection.execute(
            "SELECT order_id FROM paper_order WHERE side = 'SELL'"
        ).fetchone()[0]
    broker.close_open_order(
        order_id,
        status=PaperOrderStatus.CANCELLED,
        decided_at=T1_AT + timedelta(minutes=2),
    )
    cutoff = T1_AT + timedelta(minutes=1)

    result = PaperBrokerLifecycleReader(path, account_id="paper-main").resolve(
        candidate_id=CODE,
        entry_signal=entry,
        exit_signals=(exit_signal,),
        decision_cutoff=cutoff,
        market_features={"latest_close": 12.5, "session_high": 13.0},
        market_feature_statuses={
            "session_high": FeatureFieldStatus(
                candidate_id=CODE,
                name="session_high",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=cutoff - timedelta(seconds=1),
                available_at=cutoff,
                decision_cutoff=cutoff,
                actual_delay_seconds=1.0,
            )
        },
        previous_eligible_high_price_raw=None,
        previous_high_source_event_time=None,
        previous_high_available_at=None,
    )

    evidence = result.field_status("exit_execution_status")
    assert evidence is not None
    assert result.values["exit_execution_status"] == "pending"
    assert evidence.source_event_time <= cutoff
    assert evidence.available_at <= cutoff


def test_expired_unfilled_exit_is_retryable_without_consuming_lot(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    entry = _entry_signal()
    _submit_buy(broker, entry)
    exit_signal = _exit_signal(entry, seed="3")
    _submit_sell(
        broker,
        entry,
        quantity=1_000,
        signal=exit_signal,
        order_type=PaperOrderType.LIMIT,
        limit_price=Decimal("13.00"),
        executable_price="12.00",
    )

    expired = _resolve(
        path,
        entry,
        cutoff=exit_signal.expires_at + timedelta(seconds=1),
        exit_signals=(exit_signal,),
    )

    assert expired["exit_execution_status"] == "retryable"
    assert expired["remaining_position_fraction"] == pytest.approx(1.0)
    assert expired["position_closed"] is False


def test_partial_sell_stays_open_and_full_consumption_closes_only_its_entry(
    tmp_path: Path,
) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    entry = _entry_signal()
    other = _entry_signal(
        strategy_id="growth_board_surge",
        event_time=BUY_AT + timedelta(seconds=1),
    )
    _submit_buy(broker, entry, quantity=1_000)
    _submit_buy(
        broker,
        other,
        quantity=500,
        executed_at=BUY_AT + timedelta(seconds=1),
    )
    reduce_signal = _exit_signal(entry, action=SignalAction.REDUCE, seed="6")
    _submit_sell(
        broker,
        entry,
        quantity=500,
        signal=reduce_signal,
    )
    prior_high_at = T1_AT - timedelta(minutes=1)

    partial = _resolve(
        path,
        entry,
        cutoff=T1_AT + timedelta(seconds=1),
        previous_high=14.0,
        previous_high_at=prior_high_at,
        exit_signals=(reduce_signal,),
    )
    assert partial["position_closed"] is False
    assert partial["exit_execution_status"] == "filled"
    assert partial["remaining_position_fraction"] == pytest.approx(0.5)
    assert partial["eligible_high_price_raw"] == pytest.approx(14.0)

    close_at = T1_AT + timedelta(minutes=1)
    close_signal = _exit_signal(entry, event_time=close_at, seed="5")
    _submit_sell(
        broker,
        entry,
        quantity=500,
        executed_at=close_at,
        signal=close_signal,
    )
    closed = _resolve(
        path,
        entry,
        cutoff=close_at + timedelta(seconds=1),
        previous_high=14.0,
        previous_high_at=prior_high_at,
        exit_signals=(reduce_signal, close_signal),
    )
    other_open = _resolve(
        path,
        other,
        cutoff=close_at + timedelta(seconds=1),
    )
    assert closed["position_closed"] is True
    assert closed["remaining_position_fraction"] == pytest.approx(0.0)
    assert other_open["remaining_position_fraction"] == pytest.approx(1.0)


def test_tampered_sell_fill_quantity_cannot_close_lifecycle(
    tmp_path: Path,
) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    entry = _entry_signal()
    _submit_buy(broker, entry, quantity=1_000)
    exit_signal = _exit_signal(entry, seed="2")
    _submit_sell(
        broker,
        entry,
        quantity=1_000,
        signal=exit_signal,
    )
    with sqlite3.connect(path) as connection:
        trigger_sql = str(
            connection.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'trigger' AND name = 'paper_fill_row_immutable'
                """
            ).fetchone()[0]
        )
        connection.execute("DROP TRIGGER paper_fill_row_immutable")
        connection.execute(
            """
            UPDATE paper_fill SET quantity = 900
            WHERE order_id IN (
                SELECT order_id FROM paper_order WHERE side = 'SELL'
            )
            """
        )
        connection.execute(trigger_sql)

    with pytest.raises(PaperLifecycleIntegrityError, match="fill|consum|reconcile"):
        _resolve(
            path,
            entry,
            cutoff=T1_AT + timedelta(seconds=1),
            exit_signals=(exit_signal,),
        )
    with pytest.raises(PaperBrokerReconciliationError, match="fill|allocation"):
        broker.reconcile()


def test_late_backdated_buy_and_sell_are_hidden_until_persisted_at(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    entry = _entry_signal()
    buy_persisted = BUY_AT + timedelta(minutes=3)
    _submit_buy(broker, entry, persisted_at=buy_persisted)

    before_buy = _resolve(path, entry, cutoff=BUY_AT + timedelta(minutes=2))
    after_buy = _resolve(path, entry, cutoff=buy_persisted + timedelta(seconds=1))
    assert before_buy["entry_fill_status"] == "pending"
    assert after_buy["entry_fill_status"] == "filled"

    exit_signal = _exit_signal(entry, seed="4")
    sell_persisted = T1_AT + timedelta(minutes=3)
    _submit_sell(
        broker,
        entry,
        quantity=1_000,
        signal=exit_signal,
        persisted_at=sell_persisted,
    )
    before_sell = _resolve(
        path,
        entry,
        cutoff=T1_AT + timedelta(minutes=2),
        exit_signals=(exit_signal,),
    )
    after_sell = _resolve(
        path,
        entry,
        cutoff=sell_persisted + timedelta(seconds=1),
        exit_signals=(exit_signal,),
    )
    assert before_sell["remaining_position_fraction"] == pytest.approx(1.0)
    assert before_sell["position_closed"] is False
    assert after_sell["remaining_position_fraction"] == pytest.approx(0.0)
    assert after_sell["position_closed"] is True


def test_late_incremental_sell_fill_is_hidden_until_its_own_availability(
    tmp_path: Path,
) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path)
    entry = _entry_signal()
    _submit_buy(broker, entry)
    exit_signal = _exit_signal(entry, seed="1")
    _submit_sell(
        broker,
        entry,
        quantity=1_000,
        signal=exit_signal,
        executable_quantity=500,
    )
    partial_order_id = str(broker.fills()[-1].order_id)
    second_execution_at = T1_AT + timedelta(minutes=1)
    second_persisted_at = T1_AT + timedelta(minutes=3)
    broker.apply_execution(
        partial_order_id,
        execution_id="8" * 64,
        executed_at=second_execution_at,
        persisted_at=second_persisted_at,
        trade_date=date(2026, 8, 3),
        quantity=500,
        quote=BrokerExecutionContext(
            executable_price=Decimal("12.50"),
            executable_quantity=500,
            instrument_context=paper_instrument_context(CODE),
        ),
        price_snapshot_id="8" * 64,
    )

    before = _resolve(
        path,
        entry,
        cutoff=T1_AT + timedelta(minutes=2),
        previous_high=14.0,
        previous_high_at=T1_AT - timedelta(minutes=1),
        exit_signals=(exit_signal,),
    )
    after = _resolve(
        path,
        entry,
        cutoff=second_persisted_at + timedelta(seconds=1),
        previous_high=14.0,
        previous_high_at=T1_AT - timedelta(minutes=1),
        exit_signals=(exit_signal,),
    )

    assert before["exit_execution_status"] == "pending"
    assert before["remaining_position_fraction"] == pytest.approx(0.5)
    assert before["eligible_high_price_raw"] == pytest.approx(14.0)
    assert after["exit_execution_status"] == "filled"
    assert after["position_closed"] is True


def test_lifecycle_reconstructs_multi_fill_entry_price_at_the_v3_price_tick(
    tmp_path: Path,
) -> None:
    path = tmp_path / "paper.sqlite3"
    broker = _broker(path, price_tick=Decimal("0.01"))
    entry = _entry_signal()
    order_id = _submit_buy(
        broker,
        entry,
        quantity=200,
        price="10.004",
        executable_quantity=100,
    )
    broker.apply_execution(
        order_id,
        execution_id="6" * 64,
        executed_at=BUY_AT + timedelta(minutes=1),
        trade_date=date(2026, 7, 31),
        quantity=100,
        quote=BrokerExecutionContext(
            executable_price=Decimal("10.005"),
            executable_quantity=100,
            acquisition_available_date=date(2026, 8, 3),
            instrument_context=paper_instrument_context(CODE),
        ),
        price_snapshot_id="7" * 64,
    )

    values = _resolve(path, entry, cutoff=BUY_AT + timedelta(minutes=2))

    assert values["entry_price_raw"] == pytest.approx(10.01)


def test_unknown_legacy_fill_availability_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE paper_fill (
                fill_id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL,
                sequence INTEGER NOT NULL CHECK(sequence >= 1),
                quantity INTEGER NOT NULL CHECK(quantity > 0 AND quantity % 100 = 0),
                price TEXT NOT NULL,
                commission TEXT NOT NULL,
                tax TEXT NOT NULL,
                executed_at TEXT NOT NULL,
                price_snapshot_id TEXT NOT NULL,
                UNIQUE(order_id, sequence)
            )
            """
        )
    broker = _broker(path)
    entry = _entry_signal()
    order_id = _submit_buy(broker, entry)
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER paper_fill_persisted_at_immutable")
        connection.execute("DROP TRIGGER paper_fill_row_immutable")
        connection.execute(
            "UPDATE paper_fill SET persisted_at = NULL WHERE order_id = ?",
            (order_id,),
        )

    with pytest.raises(PaperLifecycleIntegrityError, match="availability|persisted_at"):
        _resolve(path, entry, cutoff=BUY_AT + timedelta(seconds=1))
