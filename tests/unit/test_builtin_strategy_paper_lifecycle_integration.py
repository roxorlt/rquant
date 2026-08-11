from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.feature_spool import FeatureBatchSpool
from rquant.paper_broker import BrokerCostPolicy, BrokerExecutionContext, PaperBrokerStore
from rquant.paper_contracts import PaperOrderStatus
from rquant.paper_signal_worker import (
    PaperQuoteSnapshot,
    PaperSignalPolicy,
    PaperSignalQueueStore,
    run_paper_signal_batch,
)
from rquant.runtime_builder_strategy import strategy_live_builder
from rquant.runtime_service_entrypoint import RuntimeServiceManifest
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.strategy_spec import StrategyLifecycleState
from tests.unit.test_runtime_builder_strategy import (
    COMMIT,
    NOW,
    _manifest,
    _publish,
    _publish_candidates,
)


@pytest.mark.parametrize(
    "strategy_id",
    ("n_shape", "growth_board_surge", "auction_gap"),
)
def test_builtin_exit_reaches_terminal_only_after_exact_sell_fill(
    tmp_path: Path,
    strategy_id: str,
) -> None:
    payload = _manifest(
        tmp_path,
        batch_limit=1,
        strategy_id=strategy_id,
    ).model_dump(mode="json")
    payload["settings"]["candidate_max_age_seconds"] = 900
    manifest = RuntimeServiceManifest.model_validate(payload)
    feature_spool = FeatureBatchSpool(tmp_path / "features")
    broker = PaperBrokerStore(
        tmp_path / "paper-broker.sqlite3",
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        cost_policy=BrokerCostPolicy(
            commission_rate=Decimal("0.0003"),
            minimum_commission=Decimal("5"),
            sell_stamp_tax_rate=Decimal("0.001"),
        ),
    )
    queue = PaperSignalQueueStore(
        tmp_path / "paper-signal.sqlite3",
        policy=PaperSignalPolicy(
            account_id="paper-main",
            execution_lag=timedelta(minutes=1),
            action_quantities={
                SignalAction.B_INTENT: 1_000,
                SignalAction.REDUCE: 500,
                SignalAction.S_INTENT: 1_000,
            },
            producer_commit=COMMIT,
        ),
    )
    _publish(feature_spool, sequence=0, strategy_id=strategy_id)
    _publish_candidates(
        tmp_path / "candidates",
        strategy_id=strategy_id,
        definition_fingerprint=str(manifest.settings["strategy_registration_fingerprint"]),
        executable_fingerprint=str(manifest.settings["strategy_executable_fingerprint"]),
        candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
    )
    current_time = [NOW + timedelta(seconds=1)]
    step = strategy_live_builder(clock=lambda: current_time[0])(manifest)

    step()
    entry_feature_sequence = 0
    if strategy_id == "auction_gap":
        _publish(feature_spool, sequence=1, strategy_id=strategy_id)
        current_time[0] = NOW + timedelta(seconds=2)
        step()
        entry_feature_sequence = 1
    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        entry_payload = connection.execute(
            "SELECT payload_json FROM runner_signal ORDER BY sequence DESC LIMIT 1"
        ).fetchone()[0]
    entry_signal = SignalEnvelope.model_validate_json(entry_payload)
    assert entry_signal.action is SignalAction.B_INTENT

    queue.ingest(entry_signal, received_at=entry_signal.available_at)
    buy_at = entry_signal.event_time + timedelta(minutes=1)
    buy_summary = run_paper_signal_batch(
        queue,
        broker,
        now=buy_at,
        trade_date=date(2026, 7, 31),
        quote_resolver=lambda *_args: PaperQuoteSnapshot(
            ts_code="600000.SH",
            event_time=buy_at,
            available_at=buy_at,
            context=BrokerExecutionContext(
                executable_price=Decimal("11.00"),
                acquisition_available_date=date(2026, 8, 3),
            ),
            producer_commit=COMMIT,
        ),
        limit=10,
    )
    entry_record = queue.record(entry_signal.signal_id)
    assert buy_summary.completed_count == 1
    assert entry_record is not None and entry_record.order is not None
    assert entry_record.order.status is PaperOrderStatus.FILLED

    next_trade_time = NOW.replace(day=3, month=8) + timedelta(minutes=10)
    _publish_candidates(
        tmp_path / "candidates",
        strategy_id=strategy_id,
        trade_date=date(2026, 8, 3),
        captured_at=next_trade_time,
        definition_fingerprint=str(manifest.settings["strategy_registration_fingerprint"]),
        executable_fingerprint=str(manifest.settings["strategy_executable_fingerprint"]),
        candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
    )
    holding_sequence = entry_feature_sequence + 1
    _publish(
        feature_spool,
        sequence=holding_sequence,
        strategy_id=strategy_id,
        available_at=next_trade_time,
        session_high=14.0,
    )
    current_time[0] = next_trade_time + timedelta(seconds=1)
    step()

    exit_time = next_trade_time + timedelta(seconds=30)
    exit_close = 13.40
    _publish(
        feature_spool,
        sequence=holding_sequence + 1,
        strategy_id=strategy_id,
        latest_close=exit_close,
        available_at=exit_time,
        session_high=14.0,
    )
    current_time[0] = exit_time + timedelta(seconds=1)
    step()
    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        exit_payload = connection.execute(
            "SELECT payload_json FROM runner_signal ORDER BY sequence DESC LIMIT 1"
        ).fetchone()[0]
        holding_state = connection.execute(
            "SELECT state FROM candidate_state ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()[0]
    exit_signal = SignalEnvelope.model_validate_json(exit_payload)
    assert exit_signal.action is SignalAction.REDUCE
    assert exit_signal.evidence["entry_signal_id"] == entry_signal.signal_id
    assert holding_state == StrategyLifecycleState.HOLDING.value

    queue.ingest(exit_signal, received_at=exit_signal.available_at)
    sell_at = exit_signal.available_at + timedelta(minutes=1)
    summary = run_paper_signal_batch(
        queue,
        broker,
        now=sell_at,
        trade_date=date(2026, 8, 3),
        quote_resolver=lambda *_args: PaperQuoteSnapshot(
            ts_code="600000.SH",
            event_time=sell_at,
            available_at=sell_at,
            context=BrokerExecutionContext(
                executable_price=Decimal(str(exit_close)),
                executable_quantity=(200 if strategy_id == "n_shape" else None),
            ),
            producer_commit=COMMIT,
        ),
        limit=10,
    )
    assert summary.completed_count == 1
    exit_record = queue.record(exit_signal.signal_id)
    assert exit_record is not None and exit_record.order is not None

    terminal_sequence = holding_sequence + 3
    reduce_completion_time = sell_at
    if strategy_id == "n_shape":
        assert exit_record.order.status is PaperOrderStatus.PARTIALLY_FILLED
        assert exit_record.order.filled_quantity == 200
        partial_time = sell_at + timedelta(seconds=1)
        _publish(
            feature_spool,
            sequence=holding_sequence + 2,
            strategy_id=strategy_id,
            latest_close=13.40,
            available_at=partial_time,
            session_high=13.40,
        )
        current_time[0] = partial_time + timedelta(seconds=1)
        partial_step = step()
        with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
            partial_state = connection.execute(
                "SELECT state FROM candidate_state ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()[0]
            signal_count_before_completion = connection.execute(
                "SELECT COUNT(*) FROM runner_signal"
            ).fetchone()[0]
        assert partial_step.processed_count == 1
        assert partial_state == StrategyLifecycleState.HOLDING.value

        completed_at = sell_at + timedelta(seconds=10)
        completed_receipt = broker.apply_execution(
            str(exit_record.order.order_id),
            execution_id="9" * 64,
            executed_at=completed_at,
            trade_date=date(2026, 8, 3),
            quantity=300,
            quote=BrokerExecutionContext(
                executable_price=Decimal("13.35"),
                executable_quantity=300,
            ),
            price_snapshot_id="9" * 64,
        )
        completed_order = completed_receipt.order
        assert completed_order.status is PaperOrderStatus.FILLED
        reduce_completion_time = completed_at
        final_signal_sequence = holding_sequence + 3
        terminal_sequence = holding_sequence + 4
    else:
        assert exit_record.order.status is PaperOrderStatus.FILLED
        assert exit_record.order.filled_quantity == 500
        final_signal_sequence = holding_sequence + 2

    final_signal_time = reduce_completion_time + timedelta(seconds=1)
    _publish(
        feature_spool,
        sequence=final_signal_sequence,
        strategy_id=strategy_id,
        latest_close=13.30,
        available_at=final_signal_time,
        session_high=13.40,
    )
    current_time[0] = final_signal_time + timedelta(seconds=1)
    step()
    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        final_payload = connection.execute(
            "SELECT payload_json FROM runner_signal ORDER BY sequence DESC LIMIT 1"
        ).fetchone()[0]
        signal_count_after_completion = connection.execute(
            "SELECT COUNT(*) FROM runner_signal"
        ).fetchone()[0]
    final_exit_signal = SignalEnvelope.model_validate_json(final_payload)
    if strategy_id == "n_shape":
        assert signal_count_after_completion == signal_count_before_completion + 1
    assert final_exit_signal.action is SignalAction.S_INTENT
    assert final_exit_signal.evidence["eligible_high_price_raw"] == pytest.approx(14.0)

    queue.ingest(final_exit_signal, received_at=final_exit_signal.available_at)
    final_sell_at = final_exit_signal.available_at + timedelta(minutes=1)
    final_summary = run_paper_signal_batch(
        queue,
        broker,
        now=final_sell_at,
        trade_date=date(2026, 8, 3),
        quote_resolver=lambda *_args: PaperQuoteSnapshot(
            ts_code="600000.SH",
            event_time=final_sell_at,
            available_at=final_sell_at,
            context=BrokerExecutionContext(executable_price=Decimal("13.25")),
            producer_commit=COMMIT,
        ),
        limit=10,
    )
    assert final_summary.completed_count == 1
    sell_at = final_sell_at

    with sqlite3.connect(tmp_path / "paper-broker.sqlite3") as connection:
        consumption = connection.execute(
            """
            SELECT l.entry_signal_id, c.quantity
            FROM paper_lot_consumption AS c
            JOIN paper_lot AS l ON l.lot_id = c.lot_id
            ORDER BY c.rowid
            """
        ).fetchall()
    if strategy_id == "n_shape":
        assert consumption == [
            (entry_signal.signal_id, 200),
            (entry_signal.signal_id, 300),
            (entry_signal.signal_id, 500),
        ]
    else:
        assert consumption == [
            (entry_signal.signal_id, 500),
            (entry_signal.signal_id, 500),
        ]
    assert broker.reconcile().open_lot_quantity == 0

    terminal_time = sell_at + timedelta(seconds=1)
    _publish(
        feature_spool,
        sequence=terminal_sequence,
        strategy_id=strategy_id,
        latest_close=(13.25 if strategy_id == "n_shape" else 9.50),
        available_at=terminal_time,
        session_high=14.0,
    )
    current_time[0] = terminal_time + timedelta(seconds=1)
    terminal = step()
    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        terminal_state = connection.execute(
            "SELECT state FROM candidate_state ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()[0]
        signal_payloads = connection.execute(
            "SELECT payload_json FROM runner_signal ORDER BY sequence"
        ).fetchall()

    assert terminal.processed_count == 1
    assert terminal_state == StrategyLifecycleState.TERMINAL.value
    actions = [SignalEnvelope.model_validate_json(row[0]).action for row in signal_payloads]
    assert actions.count(SignalAction.B_INTENT) == 1
    assert actions.count(SignalAction.REDUCE) == 1
    assert actions.count(SignalAction.S_INTENT) == 1
