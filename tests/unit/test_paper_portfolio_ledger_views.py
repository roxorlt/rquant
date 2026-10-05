"""Original immutable history and actual close prices form complete private views."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from tests.paper_cost_fixtures import paper_cost_policy
from tests.unit.test_paper_portfolio_pause_chain import runtime_fixture, batch
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, TRADE_DATE, _policy, _signal
from rquant.paper_signal_worker import PaperSignalQueueStore


def filled(tmp_path: Path):
    broker, basis, operator, runtime = runtime_fixture(tmp_path)
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())
    entry = _signal()
    queue.ingest(entry, received_at=entry.available_at)
    assert batch(queue, broker, runtime).completed_count == 1
    return broker, basis, operator, runtime


def ledger_source(broker):
    from rquant.paper_portfolio_ledger import PaperPortfolioLedgerSource

    return PaperPortfolioLedgerSource(path=broker.path, account_id=broker.account_id, initial_cash=Decimal("1000"),
                                      cost_policy=paper_cost_policy())


def close_material(configuration, *, day=TRADE_DATE, price="2", status="normal"):
    from rquant.backtest.contracts import SSECalendar
    from rquant.paper_portfolio_views import PaperCloseMaterials, PaperClosePrice

    at = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=7)
    dates = tuple(sorted({day - timedelta(days=1), day, day + timedelta(days=1)}))
    return PaperCloseMaterials(configuration=configuration, trade_date=day, close_at=at, available_at=at,
                               calendar=SSECalendar(source_identity="c" * 64, coverage_start=dates[0], coverage_end=dates[-1], dates=dates),
                               prices=(PaperClosePrice(ts_code="600000.SH", close_price=price, trading_status=status,
                                                       observed_at=at, available_at=at, source_snapshot_id="e" * 64, industry_l1="银行"),))


def test_complete_reader_pins_original_revision_and_does_not_write_ledger(tmp_path: Path) -> None:
    broker, basis, _, _ = filled(tmp_path)
    paths = (broker.path, broker.path.with_name(broker.path.name + "-wal"))
    before = tuple(sha256(path.read_bytes()).hexdigest() if path.exists() else None for path in paths)
    source = ledger_source(broker)
    frame = source.read(configuration=basis.configuration, as_of=EXECUTION_TIME, prices={"600000.SH": Decimal("2")})
    assert frame.account.cash == 195 and frame.account.nav == 1795
    assert frame.reconciliation.is_consistent and len(frame.history) == 1
    item = frame.history[0]
    assert item.sequence <= frame.ledger_revision and item.intent.signal_id == _signal().signal_id
    assert item.order.filled_quantity == 800 and len(item.fills) == 1
    assert source.read(configuration=basis.configuration, as_of=EXECUTION_TIME, prices={"600000.SH": Decimal("2")}) == frame
    assert tuple(sha256(path.read_bytes()).hexdigest() if path.exists() else None for path in paths) == before


def test_close_nav_uses_contemporaneous_close_and_original_fee_cash(tmp_path: Path) -> None:
    from rquant.paper_portfolio_views import PaperPortfolioViewStore

    broker, basis, _, runtime = filled(tmp_path)
    source = ledger_source(broker)
    store = PaperPortfolioViewStore(runtime.state)
    material = close_material(basis.configuration)
    value = store.record_close(source, material, published_at=material.available_at)
    assert value.status == "complete" and value.account.nav == 1795
    assert value.normalized_nav == Decimal("1.795") and value.daily_return == Decimal(".795")
    assert value.account.holdings[0].market_price == 2
    assert value.ledger_revision >= 2 and value.material_fingerprint == material.fingerprint
    assert store.record_close(source, material, published_at=material.available_at) == value
    reopened = PaperPortfolioViewStore(runtime.state)
    assert reopened.nav_series() == (value,)
    with pytest.raises(ValueError):
        store.record_close(source, close_material(basis.configuration, price="3"), published_at=material.available_at)


@pytest.mark.parametrize("status,price", [("missing", None), ("error", None)])
def test_missing_close_is_a_gap_without_last_fill_price_or_zero(tmp_path: Path, status: str, price: None) -> None:
    from rquant.paper_portfolio_views import PaperPortfolioViewStore

    broker, basis, _, runtime = filled(tmp_path)
    value = close_material(basis.configuration, status=status, price=price)
    result = PaperPortfolioViewStore(runtime.state).record_close(ledger_source(broker), value, published_at=value.available_at)
    assert result.status == "unavailable" and result.account is None and result.normalized_nav is None
    assert result.daily_return is None and "600000.SH" in result.reason


def test_future_and_retrospective_close_refuse_without_storing_nav(tmp_path: Path) -> None:
    from rquant.paper_portfolio_views import PaperPortfolioViewStore

    broker, basis, _, runtime = filled(tmp_path)
    material = close_material(basis.configuration)
    store = PaperPortfolioViewStore(runtime.state)
    with pytest.raises(ValueError):
        store.record_close(ledger_source(broker), material, published_at=material.available_at - timedelta(seconds=1))
    with pytest.raises(ValueError):
        store.record_close(ledger_source(broker), material, published_at=material.available_at + timedelta(days=1))
    assert store.nav_series() == ()
