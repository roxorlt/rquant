"""Complete original history and same-source industry facts are published honestly."""

from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from rquant.paper_broker import PaperBrokerStore
from rquant.paper_contracts import PaperOrderIntent
from tests.paper_cost_fixtures import paper_cost_policy
from tests.unit.test_paper_broker import _intent, _quote
from tests.unit.test_paper_portfolio_ledger_views import filled, ledger_source
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, TRADE_DATE


def full_history(tmp_path: Path, count: int = 200):
    broker, basis, _, _ = filled(tmp_path)
    for index in range(count):
        intent = _intent(account_id=broker.account_id, quantity=100, event_time=EXECUTION_TIME-timedelta(seconds=2))
        body = intent.model_dump(mode="python", exclude={"intent_id"})
        body["signal_id"] = sha256(f"history-{index}".encode()).hexdigest()
        intent = PaperOrderIntent(**body)
        broker.submit_intent(intent, execution_id=sha256(f"history-execution-{index}".encode()).hexdigest(),
                             decision_time=EXECUTION_TIME, trade_date=TRADE_DATE, quote=_quote("1"))
    return broker, basis


def test_original_201_orders_page_by_attested_sequence_and_keep_old_200_window(tmp_path: Path) -> None:
    from rquant.paper_portfolio_history import paper_history_page

    broker, basis = full_history(tmp_path)
    frame = ledger_source(broker).read(configuration=basis.configuration, as_of=EXECUTION_TIME, prices={"600000.SH": Decimal("1")})
    first = paper_history_page(frame, configuration=basis.configuration, authenticated_actor_id="alice", generation_id="a"*64)
    assert first.total_orders == 201 and len(first.records) == 200 and first.next_cursor
    last = paper_history_page(frame, configuration=basis.configuration, authenticated_actor_id="alice", generation_id="a"*64, cursor=first.next_cursor)
    assert len(last.records) == 1 and last.next_cursor is None
    combined = first.records + last.records
    assert len({item.order.order_id for item in combined}) == 201
    assert tuple(item.sequence for item in combined) == tuple(sorted((item.sequence for item in frame.history), reverse=True))
    old = broker.recent_order_history(as_of=EXECUTION_TIME)
    assert len(old.orders) == 200 and old.total_orders == 201 and old.has_more
    print("ORIGINAL_HISTORY_201_COMPLETE=True; OLD_RECENT_WINDOW=200")


def test_history_cursor_cannot_cross_account_generation_or_owner(tmp_path: Path) -> None:
    from rquant.paper_portfolio_history import paper_history_page

    broker, basis = full_history(tmp_path, count=1)
    frame = ledger_source(broker).read(configuration=basis.configuration, as_of=EXECUTION_TIME, prices={"600000.SH": Decimal("1")})
    page = paper_history_page(frame, configuration=basis.configuration, authenticated_actor_id="alice", generation_id="a"*64, limit=1)
    with pytest.raises(ValueError):
        paper_history_page(frame, configuration=basis.configuration, authenticated_actor_id="alice", generation_id="b"*64, cursor=page.next_cursor)
    with pytest.raises(PermissionError):
        paper_history_page(frame, configuration=basis.configuration, authenticated_actor_id="bob", generation_id="a"*64, cursor=page.next_cursor)
    different = basis.configuration.model_copy(update={"binding": basis.configuration.binding.model_copy(update={"account_id": "other"})})
    with pytest.raises(ValueError):
        paper_history_page(frame, configuration=different, authenticated_actor_id="alice", generation_id="a"*64, cursor=page.next_cursor)
    with pytest.raises(ValueError):
        paper_history_page(frame, configuration=basis.configuration, authenticated_actor_id="alice", generation_id="a"*64, cursor="/tmp/arbitrary")


def industry_material(configuration, frame, *, industry="银行", future=False):
    from rquant.paper_portfolio_exposure import PaperIndustryMaterials, PaperBenchmarkIndustryWeight
    from rquant.paper_portfolio_source import PaperPortfolioRawFact

    cutoff = frame.as_of + (timedelta(seconds=1) if future else timedelta(0))
    return PaperIndustryMaterials(configuration_fingerprint=configuration.fingerprint, ledger_frame_fingerprint=frame.fingerprint,
                                  observed_at=cutoff, available_at=cutoff, benchmark_source_identity="f"*64,
                                  benchmark_weights=(PaperBenchmarkIndustryWeight(industry_l1="银行", weight=".5"),), benchmark_cash_weight=".5",
                                  facts=(PaperPortfolioRawFact(ts_code="600000.SH", rank_score="1", industry_l1=industry, valuation_price="1",
                                                              trading_status="normal", observed_at=cutoff, available_at=cutoff, source_snapshot_id="e"*64),))


def test_industry_weights_use_actual_cash_and_original_bf_period_source(tmp_path: Path) -> None:
    from rquant.paper_portfolio_exposure import calculate_paper_exposure, PaperAttributionMaterials, PaperIndustryReturnFact

    broker, basis, _, _ = filled(tmp_path)
    frame = ledger_source(broker).read(configuration=basis.configuration, as_of=EXECUTION_TIME, prices={"600000.SH": Decimal("1")})
    material = industry_material(basis.configuration, frame)
    end = EXECUTION_TIME+timedelta(days=1)
    period = PaperAttributionMaterials(configuration_fingerprint=basis.configuration.fingerprint, start_frame_fingerprint=frame.fingerprint,
                                      start_at=frame.as_of, end_at=end, available_at=end, source_identity="d"*64,
                                      returns=(PaperIndustryReturnFact(industry_l1="银行", portfolio_return=".1", benchmark_return=".04",
                                                                       observed_at=end, available_at=end, source_identity="c"*64),))
    value = calculate_paper_exposure(frame, material, attribution=period, as_of=end)
    assert value.status == "complete" and value.exposure.rows[-1].kind == "cash"
    expected_stock = (Decimal("800")/Decimal("995")).quantize(Decimal("1e-18"))
    assert value.exposure.rows[0].portfolio_weight == expected_stock
    assert value.exposure.rows[-1].portfolio_weight == 1-expected_stock
    assert value.attribution.active_return == (expected_stock*Decimal(".1")-Decimal(".02")).quantize(Decimal("1e-18"))
    assert abs(value.attribution.residual) <= Decimal("1e-12")
    assert value.attribution.rows[-1].selection_and_interaction == 0


def test_unknown_industry_has_explicit_exposure_gap_and_no_estimated_attribution(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    from rquant.paper_portfolio_exposure import calculate_paper_exposure

    broker, basis, _, _ = filled(tmp_path)
    owner_connection = broker._connect()
    request.addfinalizer(owner_connection.close)
    assert not owner_connection.in_transaction
    frame = ledger_source(broker).read(configuration=basis.configuration, as_of=EXECUTION_TIME, prices={"600000.SH": Decimal("1")})
    value = calculate_paper_exposure(frame, industry_material(basis.configuration, frame, industry=None), as_of=frame.as_of)
    assert value.status == "unavailable" and value.attribution is None and "行业" in value.reason
    assert any(item.kind == "unknown" for item in value.exposure.rows)


def test_future_or_detached_industry_material_is_rejected(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    from rquant.paper_portfolio_exposure import calculate_paper_exposure

    broker, basis, _, _ = filled(tmp_path)
    owner_connection = broker._connect()
    request.addfinalizer(owner_connection.close)
    assert not owner_connection.in_transaction
    frame = ledger_source(broker).read(configuration=basis.configuration, as_of=EXECUTION_TIME, prices={"600000.SH": Decimal("1")})
    with pytest.raises(ValueError):
        calculate_paper_exposure(frame, industry_material(basis.configuration, frame, future=True), as_of=frame.as_of)
    detached = industry_material(basis.configuration, frame).model_copy(update={"ledger_frame_fingerprint": "b"*64})
    with pytest.raises(ValueError):
        calculate_paper_exposure(frame, detached, as_of=frame.as_of)
