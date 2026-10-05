"""Independent publication sequence preserves receipts through restart and CAS."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from tests.unit.test_paper_portfolio_ledger_views import filled
from tests.unit.test_paper_portfolio_view_source import market
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, _policy


def fixture(tmp_path: Path):
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
    from rquant.paper_portfolio_projection import PaperPortfolioSnapshot
    from rquant.paper_signal_worker import PaperSignalQueueStore

    broker, _, _, runtime = filled(tmp_path)
    at = EXECUTION_TIME+timedelta(seconds=1)
    market(runtime, at=at)
    source = PaperPortfolioViewSource(runtime, broker=broker, queue=PaperSignalQueueStore(tmp_path/"queue.sqlite", policy=_policy()))
    value = PaperPortfolioSnapshot(available_at=at, accounts=(source.read(as_of=at),))
    return source, value


def test_same_material_lost_receipt_restart_and_concurrent_retry_keep_publication_sequence(tmp_path: Path) -> None:
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource

    source, value = fixture(tmp_path)
    revision = value.accounts[0].frame.ledger_revision
    first = source.publication_sequence(value, minimum_sequence=revision)
    assert first > revision
    assert source.publication_sequence(value, minimum_sequence=revision) == first
    from rquant.paper_portfolio_state import PaperPortfolioStateStore
    from rquant.paper_operator import PaperOperatorControlStore
    from rquant.paper_portfolio_source import PaperPortfolioMaterialStore
    from rquant.paper_portfolio_runtime import PaperPortfolioRuntime
    state = PaperPortfolioStateStore.open_existing(source.runtime.state.path, expected_identity=source.runtime.state.identity())
    operator = PaperOperatorControlStore(state, root=source.runtime.operator.root, clock=lambda: value.available_at)
    runtime = PaperPortfolioRuntime(state, operator=operator, materials=PaperPortfolioMaterialStore(state), producer_commit=source.runtime.producer_commit)
    reopened = PaperPortfolioViewSource(runtime, broker=source.broker, queue=source.queue)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = tuple(pool.map(lambda _: reopened.publication_sequence(value, minimum_sequence=revision), range(8)))
    assert results == (first,)*8
    assert value.accounts[0].frame.ledger_revision == revision


def test_different_complete_valuation_increments_publication_without_changing_ledger_sequence(tmp_path: Path) -> None:
    from rquant.paper_portfolio_projection import PaperPortfolioSnapshot

    source, value = fixture(tmp_path)
    revision = value.accounts[0].frame.ledger_revision
    first = source.publication_sequence(value, minimum_sequence=revision)
    at = value.available_at+timedelta(seconds=1)
    market(source.runtime, at=at, price="3")
    changed = PaperPortfolioSnapshot(available_at=at, accounts=(source.read(as_of=at),))
    assert changed.accounts[0].frame.ledger_revision == revision
    assert changed.accounts[0].frame.account.nav == 2595
    second = source.publication_sequence(changed, minimum_sequence=revision)
    assert second == first+1
    assert source.publication_sequence(changed, minimum_sequence=revision) == second


def test_corrupt_publication_state_and_sequence_budget_fail_closed(tmp_path: Path) -> None:
    source, value = fixture(tmp_path)
    source.publication_sequence(value, minimum_sequence=0)
    with source.runtime.state._connection(write=True) as connection:
        connection.execute("UPDATE portfolio_publication SET sequence=-1,identity='bad'")
    with pytest.raises(ValueError):
        source.publication_sequence(value, minimum_sequence=0)
    with pytest.raises(ValueError):
        source.publication_sequence(value, minimum_sequence=2**63-1)


def test_old_financial_authority_is_reobserved_at_current_cutoff_without_new_revision(tmp_path: Path) -> None:
    source, value = fixture(tmp_path)
    old = source.broker.account_authority_snapshot(as_of=value.available_at, market_prices={"600000.SH": Decimal(2)},
                                                 producer_commit=source.runtime.producer_commit)
    new = source.runtime.account_authority(source.broker, cutoff=value.available_at+timedelta(seconds=1), prices={"600000.SH": Decimal(2)})
    assert new.revision == old.revision and new.state_fingerprint == old.state_fingerprint
    assert new.snapshot.as_of_time == value.available_at+timedelta(seconds=1)
    assert new.snapshot.nav == old.snapshot.nav and new.snapshot.cash == old.snapshot.cash
