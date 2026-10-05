"""Direct target-quantity and persistent drawdown acceptance boundaries."""

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.paper_broker import PaperBrokerStore
from rquant.portfolio.weights import PortfolioCandidate
from tests.paper_cost_fixtures import paper_cost_policy, paper_execution_cost_spec
from tests.unit.test_paper_signal_worker import (
    ACCOUNT_ID, EXECUTION_TIME, TRADE_DATE, _quote, _signal,
)


def config_data(**changes: object) -> dict:
    costs = paper_execution_cost_spec()
    data = {
        "binding": {
            "role_id": "paper.main.v1", "account_id": ACCOUNT_ID, "owner_id": "alice",
            "strategy_id": "n-shape", "strategy_version": "1", "parameter_fingerprint": "b" * 64,
            "cost_spec_id": costs.cost_spec_id, "ledger_id": "synthetic-paper-main",
            "manifest_fingerprint": "a" * 64,
        },
        "version": 1, "configured_at": EXECUTION_TIME - timedelta(days=1),
        "weight_rule": {"method": "equal", "max_positions": 1, "cash_reserve": ".1"},
        "execution_cost_spec": costs,
    }
    data.update(changes)
    return data


def materials(tmp_path: Path, *, weight: dict | None = None):
    from rquant.paper_portfolio_models import PaperPortfolioConfiguration, PaperTargetMaterials

    configuration = PaperPortfolioConfiguration(**config_data(**({"weight_rule": weight} if weight else {})))
    broker = PaperBrokerStore(tmp_path / "broker.sqlite", account_id=ACCOUNT_ID,
                             initial_cash=Decimal("1000"), cost_policy=paper_cost_policy())
    account = broker.account_authority_snapshot(as_of=EXECUTION_TIME, market_prices={}, producer_commit="a" * 40)
    quote = _quote(price="1.00")
    candidates = (PortfolioCandidate(ts_code="600000.SH", industry_l1="银行", rank_score=Decimal("1")),)
    value = PaperTargetMaterials(
        configuration=configuration, signal=_signal(), account=account, decision_at=EXECUTION_TIME,
        candidates=candidates, industry_by_code={"600000.SH": "银行"},
        ranking_source_fingerprint="f" * 64, ranking_available_at=EXECUTION_TIME,
        quote_snapshot_id=quote.snapshot_id, quote=quote.context,
        quote_event_time=quote.event_time, quote_available_at=quote.available_at,
    )
    return broker, value


def test_target_quantity_keeps_original_fees_cash_reserve_and_board_lots(tmp_path: Path) -> None:
    from rquant.paper_portfolio_target import prepare_paper_target_quantity

    _, value = materials(tmp_path)
    authority = prepare_paper_target_quantity(value)
    assert authority.target.positions[0].target_amount == Decimal("900.00")
    assert authority.quantity == 800
    assert authority.costs.executed_notional + authority.costs.total_fees == Decimal("805.00")
    assert authority.basis == value
    assert prepare_paper_target_quantity(value) == authority


def test_existing_holding_at_target_does_not_fall_back_to_fixed_quantity(tmp_path: Path) -> None:
    from rquant.paper_portfolio_target import PaperPortfolioAdmissionError, prepare_paper_target_quantity
    from tests.unit.test_paper_broker import _intent

    broker, value = materials(tmp_path)
    broker.submit_intent(_intent(quantity=900), decision_time=EXECUTION_TIME, trade_date=TRADE_DATE,
                         quote=_quote(price="1").context)
    account = broker.account_authority_snapshot(as_of=EXECUTION_TIME, market_prices={"600000.SH": Decimal("1")},
                                               producer_commit="a" * 40)
    value = value.model_copy(update={"account": account})
    with pytest.raises(PaperPortfolioAdmissionError):
        prepare_paper_target_quantity(value)


def test_three_continuous_caps_survive_actual_cost_and_rounding(tmp_path: Path) -> None:
    from rquant.paper_portfolio_target import prepare_paper_target_quantity

    _, value = materials(tmp_path, weight={"method": "rank_score", "max_positions": 3,
                                           "max_stock_weight": ".35", "max_industry_weight": ".60",
                                           "cash_reserve": ".10"})
    candidates = tuple(PortfolioCandidate(ts_code=code, industry_l1=industry, rank_score=Decimal(score))
                       for code, industry, score in [("600000.SH", "X", "6"), ("000001.SZ", "X", "3"), ("000002.SZ", "Y", "1")])
    value = value.model_copy(update={"candidates": candidates,
                                     "industry_by_code": {c.ts_code: c.industry_l1 for c in candidates}})
    authority = prepare_paper_target_quantity(value)
    amounts = {item.ts_code: item.target_amount for item in authority.target.positions}
    assert amounts == {"600000.SH": Decimal("350"), "000001.SZ": Decimal("250"), "000002.SZ": Decimal("300")}
    assert authority.quantity == 300
    assert authority.costs.total_fees == Decimal("5")


def test_future_ranking_or_foreign_strategy_rejected_before_allocation(tmp_path: Path) -> None:
    from rquant.paper_portfolio_target import prepare_paper_target_quantity

    _, value = materials(tmp_path)
    for invalid in [value.model_copy(update={"ranking_available_at": EXECUTION_TIME + timedelta(seconds=1)}),
                    value.model_copy(update={"signal": _signal(seed="9")})]:
        with pytest.raises(ValueError):
            prepare_paper_target_quantity(invalid)


@pytest.mark.parametrize("weight", [{"max_positions": 501}, {"max_positions": 1, "min_target_amount": "1e1000000000"},
                                    {"max_positions": 1, "cash_reserve": "NaN"}])
def test_invalid_rule_representation_refused_before_original_fraction(weight: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.paper_portfolio_models import PaperPortfolioConfiguration
    import rquant.portfolio.weights as original

    data = config_data(weight_rule=weight)
    monkeypatch.setattr(original, "Fraction", lambda *_: pytest.fail("unsafe representation reached Fraction"))
    with pytest.raises(ValueError):
        PaperPortfolioConfiguration(**data)


def test_drawdown_hysteresis_restart_original_observation_and_new_configuration(tmp_path: Path) -> None:
    from rquant.paper_portfolio_models import PaperPortfolioConfiguration
    from rquant.paper_portfolio_state import PaperPortfolioStateStore

    configuration = PaperPortfolioConfiguration(**config_data(drawdown_rule={"trigger_drawdown": ".20",
                                                                          "release_drawdown": ".05",
                                                                          "action": "block_new_positions"}))
    path = tmp_path / "state.sqlite"
    state = PaperPortfolioStateStore(path, configuration=configuration)
    active = []
    for index, nav in enumerate(["100", "90", "80", "85", "95"]):
        if index == 3:
            state = PaperPortfolioStateStore(path, configuration=configuration)
        instant = EXECUTION_TIME + timedelta(days=index)
        result = state.observe_nav(Decimal(nav), observed_at=instant, ledger_revision=index + 1,
                                   source_fingerprint=str(index) * 64)
        active.append(result.decision.state.active)
        assert state.observe_nav(Decimal(nav), observed_at=instant, ledger_revision=index + 1,
                                 source_fingerprint=str(index) * 64) == result
    assert active == [False, False, True, True, False]
    assert result.decision.state.peak_nav == Decimal("100")
    with pytest.raises(ValueError):
        state.observe_nav(Decimal("94"), observed_at=instant, ledger_revision=5, source_fingerprint="4" * 64)
    with pytest.raises(ValueError):
        state.observe_nav(Decimal("100"), observed_at=EXECUTION_TIME, ledger_revision=6, source_fingerprint="5" * 64)
    assert len(state.observations()) == 5
    second = PaperPortfolioConfiguration(**config_data(version=2, drawdown_rule={"trigger_drawdown": ".1",
                                                                               "release_drawdown": ".02",
                                                                               "action": "cap_total_risk_weight",
                                                                               "total_risk_weight_cap": ".4"}))
    state.start_configuration(second)
    fresh = state.observe_nav(Decimal("90"), observed_at=instant, ledger_revision=5, source_fingerprint="4" * 64)
    assert fresh.decision.state.peak_nav == Decimal("90") and not fresh.decision.state.active
