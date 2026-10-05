"""Direct remaining frozen input/file/cash cases; all sources are private synthetic."""

from decimal import Decimal
from datetime import timedelta
import json
from pathlib import Path

import pytest

from rquant.paper_portfolio_models import PaperPortfolioConfiguration, PaperTargetMaterials
from tests.unit.test_paper_portfolio_core import config_data, materials
from tests.unit.test_paper_portfolio_admission import confirm, operator_fixture, request
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, TRADE_DATE, _quote, _signal


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "1e1000000000"])
def test_bad_rule_or_naked_quantity_refuses_before_allocation(value: str, tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        PaperPortfolioConfiguration(**config_data(weight_rule={"max_positions": 1, "max_stock_weight": value}))
    _, basis = materials(tmp_path)
    with pytest.raises(ValueError):
        PaperTargetMaterials.model_validate({**basis.model_dump(mode="python"), "quantity": 100})


@pytest.mark.parametrize("failure", ["symlink", "oversized", "extra", "contract", "same-sequence-change"])
def test_untrusted_control_file_keeps_buy_closed_and_original_applied_pointer(tmp_path: Path, failure: str) -> None:
    _, operator = operator_fixture(tmp_path)
    control = confirm(operator, request(operator))
    operator.publish(control)
    original = operator.apply(observed_at=EXECUTION_TIME)
    assert original.status == "applied" and not original.paused
    if failure == "symlink":
        previous = operator.path.with_suffix(".original")
        operator.path.rename(previous)
        operator.path.symlink_to(previous)
    elif failure == "oversized":
        operator.path.write_bytes(b" " * 16385)
    else:
        body = json.loads(operator.path.read_bytes())
        body.update({"untrusted": True} if failure == "extra" else {"contract": "unknown/v3"} if failure == "contract" else {"paused": True})
        operator.path.write_text(json.dumps(body))
    actual = operator.apply(observed_at=EXECUTION_TIME)
    assert actual.status == "unavailable" and actual.paused
    assert (actual.sequence, actual.control_fingerprint) == (original.sequence, original.control_fingerprint)


def test_next_signal_reuses_actual_post_fee_cash_without_a_fixed_lot_fallback(tmp_path: Path) -> None:
    from rquant.paper_portfolio_target import PaperPortfolioAdmissionError, prepare_paper_target_quantity
    from rquant.paper_signal_worker import PaperSignalQueueStore
    from tests.unit.test_paper_signal_worker import _policy
    broker, basis = materials(tmp_path)
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())
    first = prepare_paper_target_quantity(basis)
    queue.ingest(basis.signal, received_at=basis.signal.available_at)
    prepared = queue.prepare(basis.signal.signal_id, quote=_quote(price="1"), prepared_at=EXECUTION_TIME, target_quantity_authority=first)
    broker.submit_intent(prepared.intent, execution_id=prepared.execution_id, decision_time=EXECUTION_TIME,
                         trade_date=TRADE_DATE, quote=prepared.quote.context)
    account = broker.account_authority_snapshot(as_of=EXECUTION_TIME, market_prices={"600000.SH": Decimal(1)}, producer_commit="a" * 40)
    assert account.snapshot.cash == 195 and account.snapshot.nav == 995
    second = PaperTargetMaterials.model_validate(basis.model_copy(update={"signal": _signal(event_time=basis.signal.event_time - timedelta(seconds=1)), "account": account}).model_dump(mode="python"))
    with pytest.raises(PaperPortfolioAdmissionError, match="一手"):
        prepare_paper_target_quantity(second)
    assert len(broker.recent_order_history(as_of=EXECUTION_TIME).orders) == 1
