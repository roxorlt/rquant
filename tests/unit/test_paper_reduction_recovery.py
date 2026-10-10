"""In-flight original reductions must settle before another target is published."""

from datetime import timedelta
from pathlib import Path

import pytest

from rquant.paper_signal_consumer import PaperSignalConsumerStateStore, consume_signal_bus_to_paper
from rquant.paper_signal_worker import PaperSignalQueueStore, run_paper_signal_batch
from rquant.signal_route_spool import ReadonlySignalRouteSpool
from tests.unit.test_paper_portfolio_reductions import execute_plan, fixture, material
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, TRADE_DATE, NEXT_TRADE_DATE, _policy, _quote


def test_t1_rejection_is_not_reissued_with_every_same_day_observation(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    broker, runtime, at = fixture(tmp_path)
    owner_connection = broker._connect()
    request.addfinalizer(owner_connection.close)
    assert not owner_connection.in_transaction
    at += timedelta(minutes=1)
    material(runtime, at, ".75")
    plan = runtime.plan_risk_reductions(broker, decision_at=at, trade_date=TRADE_DATE,
                                       quote_resolver=lambda _signal, cutoff: _quote(price=".75", available_at=cutoff))
    queue, _ = execute_plan(tmp_path, broker, runtime, at)
    later = at + timedelta(minutes=2)
    material(runtime, later, ".75")
    recovered = runtime.plan_risk_reductions(broker, decision_at=later, trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: pytest.fail("same-day rejected plan must not prepare a new order"), queue=queue)
    assert recovered == plan and broker.reconcile().order_count == 2
    with runtime.state._connection() as connection:
        assert connection.execute("SELECT count(*) FROM portfolio_risk_plans").fetchone()[0] == 1


@pytest.mark.parametrize("boundary", ["pending", "prepared", "broker_submitted"])
def test_original_reduction_keeps_one_plan_through_prepare_submit_and_recovery(
    tmp_path: Path, boundary: str, request: pytest.FixtureRequest
) -> None:
    broker, runtime, _ = fixture(tmp_path)
    owner_connection = broker._connect()
    request.addfinalizer(owner_connection.close)
    assert not owner_connection.in_transaction
    at = EXECUTION_TIME + timedelta(days=3, minutes=2)
    material(runtime, at, ".75")
    plan = runtime.plan_risk_reductions(broker, decision_at=at, trade_date=NEXT_TRADE_DATE,
        quote_resolver=lambda _signal, cutoff: _quote(price=".75", available_at=cutoff))
    queue = PaperSignalQueueStore(tmp_path / "risk-queue.sqlite", policy=_policy())
    cursor = PaperSignalConsumerStateStore(tmp_path / "risk-consumer.sqlite")
    consume_signal_bus_to_paper(ReadonlySignalRouteSpool(tmp_path / "risk-spool"), queue, cursor, observed_at=at, limit=50)
    signal = plan.signals[0]
    due = at + timedelta(minutes=1)
    if boundary != "pending":
        authority = broker.sell_quantity_authority(exit_signal_id=signal.signal_id,
            entry_signal_id=signal.evidence["entry_signal_id"], ts_code=signal.candidate_id,
            action=signal.action.name, tranche_fraction=signal.evidence["sell_tranche_fraction"],
            decision_cutoff=due, trade_date=NEXT_TRADE_DATE)
        prepared = queue.prepare(signal.signal_id, quote=_quote(price=".75", available_at=due),
                                 prepared_at=due, sell_quantity_authority=authority)
        if boundary == "broker_submitted":
            broker.submit_intent(prepared.intent, execution_id=prepared.execution_id,
                                 decision_time=due, trade_date=NEXT_TRADE_DATE, quote=prepared.quote.context)
    later = due + timedelta(seconds=30)
    material(runtime, later, ".75")
    old = runtime.plan_risk_reductions(broker, decision_at=later, trade_date=NEXT_TRADE_DATE,
        quote_resolver=lambda *_args: pytest.fail("in-flight signal must settle before another plan"), queue=queue)
    # A committed reduction may already satisfy the cap and need no new plan.
    assert old is None if boundary == "broker_submitted" else old == plan
    result = run_paper_signal_batch(queue, broker, now=later, trade_date=NEXT_TRADE_DATE,
        quote_resolver=lambda _signal, cutoff: _quote(price=".75", available_at=cutoff), limit=50, portfolio_runtime=runtime)
    assert result.completed_count == 1 and broker.reconcile().order_count == 2
    assert broker.reconcile().open_lot_quantity == 500
    with runtime.state._connection() as connection:
        assert connection.execute("SELECT count(*) FROM portfolio_risk_plans").fetchone()[0] == 1
