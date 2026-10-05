"""Original v5 reconciliation must open without migrations, WAL writes or snapshots."""

from hashlib import sha256
from pathlib import Path
from datetime import timedelta
from decimal import Decimal

import pytest

from rquant.paper_broker import PaperBrokerStore, PaperBrokerReconciliationError
from tests.paper_cost_fixtures import paper_cost_policy
from tests.unit.test_paper_portfolio_core import materials
from tests.unit.test_paper_signal_worker import EXECUTION_TIME


def ledger_bytes(path: Path) -> dict[str, str | None]:
    paths = (path, Path(str(path) + "-wal"))
    return {str(item): sha256(item.read_bytes()).hexdigest() if item.exists() else None for item in paths}


@pytest.mark.parametrize("bad_account", [False, True])
def test_original_v5_readonly_reconcile_preserves_ledger_bytes(tmp_path: Path, bad_account: bool) -> None:
    broker, _ = materials(tmp_path)
    before = ledger_bytes(broker.path)
    try:
        with PaperBrokerStore.open_readonly(broker.path, account_id="other" if bad_account else broker.account_id,
                                            initial_cash=broker.initial_cash, cost_policy=broker.cost_policy) as reader:
            if bad_account:
                pytest.fail("unknown account must fail before readonly facts")
            assert reader.reconcile().is_consistent
            snapshot = reader.account_snapshot(as_of=EXECUTION_TIME, market_prices={})
            assert snapshot.cash == broker.initial_cash and not snapshot.holdings
            with pytest.raises(Exception):
                reader.account_authority_snapshot(as_of=EXECUTION_TIME, market_prices={},
                                                  producer_commit="e" * 40)
    except AttributeError:
        raise
    except PaperBrokerReconciliationError:
        if not bad_account:
            raise
    assert ledger_bytes(broker.path) == before


@pytest.mark.parametrize("wrong", ["initial_cash", "cost"])
def test_readonly_exact_account_configuration_rejects_without_writes(tmp_path: Path, wrong: str) -> None:
    broker, _ = materials(tmp_path)
    before = ledger_bytes(broker.path)
    with pytest.raises(PaperBrokerReconciliationError):
        with PaperBrokerStore.open_readonly(broker.path, account_id=broker.account_id,
                                            initial_cash=Decimal("999") if wrong == "initial_cash" else broker.initial_cash,
                                            cost_policy=paper_cost_policy(minimum_commission=Decimal("6")) if wrong == "cost" else broker.cost_policy):
            pytest.fail("wrong source configuration must be rejected")
    assert ledger_bytes(broker.path) == before


def test_readonly_does_not_create_a_missing_source(tmp_path: Path) -> None:
    path = tmp_path / "must-not-create.sqlite"
    with pytest.raises(PaperBrokerReconciliationError):
        with PaperBrokerStore.open_readonly(path, account_id="paper-main", initial_cash=Decimal("1000"), cost_policy=paper_cost_policy()):
            pytest.fail("new source must not be initialized")
    assert not path.exists() and not Path(str(path) + "-wal").exists()


@pytest.mark.parametrize("bad_anchor", [False, True])
def test_readonly_anchor_success_failure_and_exception_keep_bytes_and_close(tmp_path: Path, bad_anchor: bool) -> None:
    from tests.paper_ledger_anchor_support import create_paper_ledger_test_authority

    broker, _ = materials(tmp_path)
    authority = create_paper_ledger_test_authority(tmp_path / "synthetic-keys", as_of=EXECUTION_TIME,
                                                  max_age=timedelta(minutes=5), future_skew=timedelta(seconds=30))
    anchor_path = tmp_path / "anchor.json"
    anchor = authority.write_current_anchor(broker.path, anchor_path, issued_at=EXECUTION_TIME)
    if bad_anchor:
        changed = anchor.model_copy(update={"claims": anchor.claims.model_copy(update={"financial_state_digest": "0" * 64})})
        anchor_path.write_text(changed.model_dump_json())
    before = {**ledger_bytes(broker.path), "anchor": sha256(anchor_path.read_bytes()).hexdigest()}
    kwargs = {"account_id": broker.account_id, "initial_cash": broker.initial_cash, "cost_policy": broker.cost_policy,
              "ledger_id": authority.ledger_id, "ledger_anchor_path": anchor_path, "ledger_anchor_verifier": authority.verifier}
    if bad_anchor:
        with pytest.raises(PaperBrokerReconciliationError):
            with PaperBrokerStore.open_readonly(broker.path, **kwargs):
                pytest.fail("wrong signed source must fail")
    else:
        with pytest.raises(RuntimeError, match="lab interrupted"):
            with PaperBrokerStore.open_readonly(broker.path, **kwargs) as reader:
                assert reader.reconcile().is_consistent
                raise RuntimeError("lab interrupted")
        assert reader._readonly_connection is None
        with pytest.raises(PaperBrokerReconciliationError, match="closed"):
            reader.reconcile()
    after = {**ledger_bytes(broker.path), "anchor": sha256(anchor_path.read_bytes()).hexdigest()}
    assert after == before
    print(f"READONLY_DB_WAL_ANCHOR_UNCHANGED=True; bad_anchor={bad_anchor}; sources={before}; connection_reaped=True")


def test_readonly_session_keeps_one_original_v5_transaction(tmp_path: Path) -> None:
    from tests.unit.test_paper_broker import _intent
    from tests.unit.test_paper_signal_worker import TRADE_DATE, _quote

    broker, _ = materials(tmp_path)
    with PaperBrokerStore.open_readonly(broker.path, account_id=broker.account_id, initial_cash=broker.initial_cash,
                                        cost_policy=broker.cost_policy) as reader:
        assert reader.reconcile().order_count == 0
        broker.submit_intent(_intent(quantity=100), decision_time=EXECUTION_TIME, trade_date=TRADE_DATE,
                             quote=_quote(price="1").context)
        assert reader.reconcile().order_count == 0
        assert reader.account_snapshot(as_of=EXECUTION_TIME, market_prices={}).cash == 1000
    with PaperBrokerStore.open_readonly(broker.path, account_id=broker.account_id, initial_cash=broker.initial_cash,
                                        cost_policy=broker.cost_policy) as next_reader:
        assert next_reader.reconcile().order_count == 1
        assert next_reader.account_snapshot(as_of=EXECUTION_TIME, market_prices={"600000.SH": Decimal("1")}).cash == 895
