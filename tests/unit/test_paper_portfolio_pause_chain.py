"""The two original pause recovery boundaries must produce different outcomes."""

from datetime import timedelta
from pathlib import Path

from rquant.paper_signal_worker import PaperSignalQueueStore, run_paper_signal_batch
from rquant.signal_contracts import SignalAction
from tests.unit.test_paper_portfolio_admission import confirm, request
from tests.unit.test_paper_portfolio_core import materials
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, NEXT_TRADE_DATE, TRADE_DATE, _policy, _quote, _signal


def runtime_fixture(tmp_path: Path):
    from rquant.paper_operator import PaperOperatorControlStore
    from rquant.paper_portfolio_runtime import PaperPortfolioRuntime
    from rquant.paper_portfolio_source import PaperPortfolioMaterialStore, PaperPortfolioMarketSnapshot, PaperPortfolioRawFact
    from rquant.paper_portfolio_state import PaperPortfolioStateStore

    broker, basis = materials(tmp_path)
    state = PaperPortfolioStateStore(tmp_path / "state.sqlite", configuration=basis.configuration)
    operator = PaperOperatorControlStore(state, root=tmp_path / "control" / "operator", clock=lambda: EXECUTION_TIME)
    source = PaperPortfolioMaterialStore(state)
    source.publish(PaperPortfolioMarketSnapshot(
        binding=basis.configuration.binding, configuration_fingerprint=basis.configuration.fingerprint,
        dataset_snapshot_id=basis.signal.dataset_snapshot_id, feature_snapshot_id=basis.signal.feature_snapshot_id,
        observed_at=EXECUTION_TIME, available_at=EXECUTION_TIME,
        facts=(PaperPortfolioRawFact(ts_code="600000.SH", rank_score="1", industry_l1="银行", valuation_price="1",
                                    trading_status="normal", observed_at=EXECUTION_TIME, available_at=EXECUTION_TIME,
                                    source_snapshot_id="f" * 64),)))
    resume = confirm(operator, request(operator))
    operator.publish(resume)
    operator.apply(observed_at=EXECUTION_TIME)
    return broker, basis, operator, PaperPortfolioRuntime(state, operator=operator, materials=source, producer_commit="a" * 40)


def batch(queue, broker, runtime, *, now=EXECUTION_TIME, trade_date=TRADE_DATE):
    return run_paper_signal_batch(queue, broker, now=now, trade_date=trade_date,
                                 quote_resolver=lambda signal, cutoff: _quote(price="1", available_at=cutoff),
                                 limit=10, portfolio_runtime=runtime)


def test_prepared_unsubmitted_pause_is_terminal_and_resume_does_not_buy(tmp_path: Path) -> None:
    from rquant.paper_portfolio_target import prepare_paper_target_quantity

    broker, basis, operator, runtime = runtime_fixture(tmp_path)
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)
    queue.prepare(signal.signal_id, quote=_quote(price="1"), prepared_at=EXECUTION_TIME,
                  target_quantity_authority=prepare_paper_target_quantity(basis))
    pause = confirm(operator, request(operator, sequence=1, expected_paused=False, paused=True))
    operator.publish(pause)
    result = batch(queue, broker, runtime)
    record = queue.record(signal.signal_id)
    assert record.status.value == "rejected" and record.intent.quantity == 800 and record.order is None
    assert result.completed_count == 0 and result.failed_count == 0
    assert broker.reconcile().order_count == 0 and operator.current().paused
    resume = confirm(operator, request(operator, sequence=2, expected_paused=True, paused=False))
    operator.publish(resume)
    assert batch(queue, broker, runtime).due_count == 0
    assert broker.reconcile().order_count == 0


def test_broker_submitted_prepared_recovers_before_new_control_or_configuration(tmp_path: Path) -> None:
    from rquant.paper_portfolio_target import prepare_paper_target_quantity

    broker, basis, operator, runtime = runtime_fixture(tmp_path)
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)
    prepared = queue.prepare(signal.signal_id, quote=_quote(price="1"), prepared_at=EXECUTION_TIME,
                             target_quantity_authority=prepare_paper_target_quantity(basis))
    original = broker.submit_intent(prepared.intent, execution_id=prepared.execution_id,
                                    decision_time=EXECUTION_TIME, trade_date=TRADE_DATE, quote=prepared.quote.context)
    operator.path.unlink()
    runtime.state.start_configuration(basis.configuration.model_copy(update={"version": 2}))
    assert batch(queue, broker, runtime).completed_count == 1
    recovered = queue.record(signal.signal_id)
    assert recovered.order == original and recovered.intent == prepared.intent
    assert recovered.target_quantity_authority == prepared.target_quantity_authority
    assert broker.reconcile().order_count == 1 and broker.reconcile().cash == 195


def test_pause_consumes_new_buy_but_keeps_original_sell_and_t1_constraints(tmp_path: Path) -> None:
    broker, _, operator, runtime = runtime_fixture(tmp_path)
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())
    entry = _signal()
    queue.ingest(entry, received_at=entry.available_at)
    assert batch(queue, broker, runtime).completed_count == 1
    pause = confirm(operator, request(operator, sequence=1, expected_paused=False, paused=True))
    operator.publish(pause)
    event = EXECUTION_TIME + timedelta(days=3)
    exit_signal = _signal(SignalAction.S_INTENT, event_time=event, entry_signal_id=entry.signal_id)
    new_buy = _signal(event_time=event)
    for signal in (exit_signal, new_buy):
        queue.ingest(signal, received_at=signal.available_at)
    result = batch(queue, broker, runtime, now=event + timedelta(minutes=1), trade_date=NEXT_TRADE_DATE)
    assert result.completed_count == 1 and result.failed_count == 0
    assert queue.record(new_buy.signal_id).status.value == "ignored"
    assert queue.record(exit_signal.signal_id).order.quantity == 800
    assert broker.reconcile().open_lot_quantity == 0
