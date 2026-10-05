"""Drawdown reductions use original routes, quantities and actual execution outcomes."""

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.paper_signal_consumer import PaperSignalConsumerStateStore, consume_signal_bus_to_paper
from rquant.paper_signal_worker import PaperSignalQueueStore, run_paper_signal_batch
from rquant.signal_bus import SignalBusStore
from rquant.signal_route_spool import ReadonlySignalRouteSpool, SignalRouteSpool
from rquant.signal_router_runtime import SignalRouteCursorStore
from tests.unit.test_paper_portfolio_ledger_views import filled
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, TRADE_DATE, NEXT_TRADE_DATE, _policy, _quote


def material(runtime, at, price="1"):
    from rquant.paper_portfolio_source import PaperPortfolioMarketSnapshot, PaperPortfolioRawFact

    configuration = runtime.state.configuration
    value = PaperPortfolioMarketSnapshot(binding=configuration.binding, configuration_fingerprint=configuration.fingerprint,
                                        dataset_snapshot_id="c"*64, feature_snapshot_id="d"*64, observed_at=at, available_at=at,
                                        facts=(PaperPortfolioRawFact(ts_code="600000.SH", rank_score="1", industry_l1="银行", valuation_price=price,
                                                                    trading_status="normal", observed_at=at, available_at=at, source_snapshot_id="e"*64),))
    runtime.materials.publish(value)


def fixture(tmp_path: Path):
    from rquant.paper_portfolio_models import PaperPortfolioConfiguration
    from rquant.paper_portfolio_reductions import PaperRiskSignalPublisher

    broker, basis, operator, runtime = filled(tmp_path)
    body = basis.configuration.model_dump(mode="python")
    body.update(version=2, configured_at=EXECUTION_TIME,
                drawdown_rule={"trigger_drawdown": ".1", "release_drawdown": ".02", "action": "cap_total_risk_weight", "total_risk_weight_cap": ".5"})
    runtime.state.start_configuration(PaperPortfolioConfiguration.model_validate(body))
    publisher = PaperRiskSignalPublisher(runtime.state, bus=SignalBusStore(tmp_path/"risk-bus.sqlite"), spool=SignalRouteSpool(tmp_path/"risk-spool"),
                                          cursors=SignalRouteCursorStore(tmp_path/"risk-cursor.sqlite", routing_policy_fingerprint="a"*64))
    runtime.risk_publisher = publisher
    at = EXECUTION_TIME+timedelta(minutes=1)
    material(runtime, at)
    assert runtime.plan_risk_reductions(broker, decision_at=at, trade_date=TRADE_DATE,
                                        quote_resolver=lambda signal, cutoff: _quote(price="1", available_at=cutoff)) is None
    return broker, runtime, at


def execute_plan(tmp_path, broker, runtime, at, *, date=TRADE_DATE, suspended=False):
    queue = PaperSignalQueueStore(tmp_path/"risk-queue.sqlite", policy=_policy())
    cursor = PaperSignalConsumerStateStore(tmp_path/"risk-consumer.sqlite")
    consume_signal_bus_to_paper(ReadonlySignalRouteSpool(tmp_path/"risk-spool"), queue, cursor, observed_at=at, limit=50)
    def quote(signal, cutoff):
        value = _quote(price=".75", available_at=cutoff)
        return type(value).model_validate({**value.model_dump(mode="python"), "context": value.context.model_copy(update={"suspended": suspended}), "snapshot_id": None}) if suspended else value
    summary = run_paper_signal_batch(queue, broker, now=at+timedelta(minutes=1), trade_date=date, quote_resolver=quote, limit=50,
                                     portfolio_runtime=runtime)
    return queue, summary


def test_drawdown_t1_remains_incomplete_and_next_open_day_reduces_on_original_chain(tmp_path: Path) -> None:
    broker, runtime, at = fixture(tmp_path)
    at += timedelta(minutes=1)
    material(runtime, at, ".75")
    plan = runtime.plan_risk_reductions(broker, decision_at=at, trade_date=TRADE_DATE,
                                        quote_resolver=lambda signal, cutoff: _quote(price=".75", available_at=cutoff))
    assert plan is not None and plan.signals and plan.signals[0].evidence["entry_signal_id"]
    assert runtime.plan_risk_reductions(broker, decision_at=at, trade_date=TRADE_DATE,
                                        quote_resolver=lambda signal, cutoff: _quote(price=".75", available_at=cutoff)) == plan
    queue, first = execute_plan(tmp_path, broker, runtime, at)
    assert first.completed_count == 1 and broker.reconcile().open_lot_quantity == 800
    status = runtime.reduction_status(broker, queue, as_of=at+timedelta(minutes=1), prices={"600000.SH": Decimal(".75")})
    assert status.status == "incomplete" and "t_plus_one" in status.reason.lower()
    assert status.risk_weight > Decimal(".5")
    later = EXECUTION_TIME+timedelta(days=3, minutes=3)
    material(runtime, later, ".75")
    next_plan = runtime.plan_risk_reductions(broker, decision_at=later, trade_date=NEXT_TRADE_DATE,
                                             quote_resolver=lambda signal, cutoff: _quote(price=".75", available_at=cutoff))
    assert next_plan is not None and next_plan.fingerprint != plan.fingerprint
    _, final = execute_plan(tmp_path, broker, runtime, later, date=NEXT_TRADE_DATE)
    assert final.completed_count == 1
    status = runtime.reduction_status(broker, queue, as_of=later+timedelta(minutes=1), prices={"600000.SH": Decimal(".75")})
    assert status.status == "complete" and status.risk_weight <= Decimal(".5")
    assert broker.reconcile().order_count == 3 and broker.reconcile().open_lot_quantity == 500
    print("ORIGINAL_DD_ROUTE_QUEUE_BROKER=True; T1_INCOMPLETE=True; ACTUAL_REDUCTION=300")


def test_suspended_reduction_is_a_real_reject_not_achieved_risk_target(tmp_path: Path) -> None:
    broker, runtime, _ = fixture(tmp_path)
    at = EXECUTION_TIME+timedelta(days=3, minutes=2)
    material(runtime, at, ".75")
    assert runtime.plan_risk_reductions(broker, decision_at=at, trade_date=NEXT_TRADE_DATE,
                                       quote_resolver=lambda signal, cutoff: _quote(price=".75", available_at=cutoff)) is not None
    queue, value = execute_plan(tmp_path, broker, runtime, at, date=NEXT_TRADE_DATE, suspended=True)
    status = runtime.reduction_status(broker, queue, as_of=at+timedelta(minutes=1), prices={"600000.SH": Decimal(".75")})
    assert value.completed_count == 1 and status.status == "incomplete" and "suspend" in status.reason.lower()
    assert broker.reconcile().open_lot_quantity == 800


def test_role_without_configured_original_risk_route_does_not_fake_reduction(tmp_path: Path) -> None:
    broker, runtime, at = fixture(tmp_path)
    runtime.risk_publisher = None
    at += timedelta(minutes=1)
    material(runtime, at, ".75")
    with pytest.raises(ValueError, match="路由"):
        runtime.plan_risk_reductions(broker, decision_at=at, trade_date=TRADE_DATE,
                                     quote_resolver=lambda signal, cutoff: _quote(price=".75", available_at=cutoff))
    assert broker.reconcile().order_count == 1
