"""Reconciliation replays a legal readonly v5 copy and reports actual differences."""

from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from tests.unit.test_paper_portfolio_ledger_views import filled, ledger_source
from tests.unit.test_paper_signal_worker import EXECUTION_TIME


def hashes(broker):
    return tuple(sha256(path.read_bytes()).hexdigest() if path.exists() else None for path in
                 (broker.path, broker.path.with_name(broker.path.name+"-wal")))


def test_actual_frozen_v5_reconcile_reads_cash_holdings_orders_fills_without_source_writes(tmp_path: Path) -> None:
    from rquant.paper_reconcile import execute_paper_reconcile, freeze_paper_reconcile

    broker, basis, _, _ = filled(tmp_path)
    before = hashes(broker)
    value = freeze_paper_reconcile(ledger_source(broker), basis.configuration, as_of=EXECUTION_TIME, prices={"600000.SH": Decimal(1)})
    assert value.expected.account.cash == 195 and len(value.expected.history) == 1
    result = execute_paper_reconcile(value)
    assert result.status == "consistent" and result.difference_count == 0
    assert result.ledger_revision == value.ledger_revision and result.head_fingerprint == value.head_fingerprint
    assert result.account.cash == 195 and result.account.holdings[0].quantity == 800
    assert hashes(broker) == before


def test_changed_cash_comparison_is_sealed_difference_and_never_repaired(tmp_path: Path) -> None:
    from rquant.paper_reconcile import PaperReconcileComparison, execute_paper_reconcile, freeze_paper_reconcile
    from rquant.paper_contracts import PaperAccountSnapshot

    broker, basis, _, _ = filled(tmp_path)
    before = hashes(broker)
    frame = ledger_source(broker).read(configuration=basis.configuration, as_of=EXECUTION_TIME, prices={"600000.SH": Decimal(1)})
    account = PaperAccountSnapshot.model_validate({**frame.account.model_dump(mode="python"), "snapshot_id": None,
                                                   "cash": Decimal(196), "available_cash": Decimal(196), "nav": Decimal(996)})
    comparison = PaperReconcileComparison(account=account, history=frame.history)
    value = freeze_paper_reconcile(ledger_source(broker), basis.configuration, as_of=EXECUTION_TIME,
                                  prices={"600000.SH": Decimal(1)}, expected=comparison)
    result = execute_paper_reconcile(value)
    assert result.status == "differences" and result.difference_count >= 2
    assert any(item.path.endswith(".cash") and Decimal(item.expected) == 196 and Decimal(item.actual) == 195 for item in result.differences)
    assert result.account.cash == 195 and hashes(broker) == before


def test_copy_tamper_or_detached_sequence_refuses_without_touching_original(tmp_path: Path) -> None:
    from rquant.paper_reconcile import PaperFrozenLedgerCopy, execute_paper_reconcile, freeze_paper_reconcile

    broker, basis, _, _ = filled(tmp_path)
    before = hashes(broker)
    value = freeze_paper_reconcile(ledger_source(broker), basis.configuration, as_of=EXECUTION_TIME, prices={"600000.SH": Decimal(1)})
    with pytest.raises(ValueError):
        PaperFrozenLedgerCopy.model_validate({**value.frozen.model_dump(mode="python"), "copy_sha256": "0"*64})
    with pytest.raises(ValueError):
        execute_paper_reconcile(value.model_copy(update={"ledger_revision": value.ledger_revision+1}))
    assert hashes(broker) == before
