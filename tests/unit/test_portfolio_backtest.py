from __future__ import annotations

from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from rquant.backtest import (
    BacktestDayInput,
    BacktestInstrument,
    BacktestRequest,
    BacktestResult,
    RankingSnapshot,
    RebalanceRule,
    SSECalendar,
    TradeConditions,
    run_portfolio_backtest,
)
from rquant.paper_broker import BrokerCostPolicy, BrokerExecutionContext, PaperBrokerStore
from rquant.paper_contracts import (
    PaperOrderIntent,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectReason,
    PaperSide,
)
from rquant.portfolio.weights import PortfolioCandidate, PortfolioWeightRule
from tests.paper_cost_fixtures import paper_execution_cost_spec, paper_instrument_context

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CODES = ("600000.SH", "000001.SZ", "600001.SH")
_TRADE_DATES = (
    date(2026, 8, 10),
    date(2026, 8, 11),
    date(2026, 8, 12),
    date(2026, 8, 13),
    date(2026, 8, 14),
    date(2026, 8, 17),
    date(2026, 8, 18),
    date(2026, 8, 19),
    date(2026, 8, 20),
    date(2026, 8, 21),
)
_CALENDAR_DATES = (date(2026, 8, 7), *_TRADE_DATES, date(2026, 8, 24))


def _at(trade_date: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(trade_date, time(hour, minute), tzinfo=_SHANGHAI).astimezone(UTC)


def _day(
    trade_date: date,
    previous_trade_date: date,
    selected_code: str | None,
    *,
    locked_buy: str | None = None,
    locked_sell: str | None = None,
    missing_condition: str | None = None,
    missing_close: str | None = None,
) -> BacktestDayInput:
    candidates = (
        ()
        if selected_code is None
        else (PortfolioCandidate(ts_code=selected_code, rank_score=Decimal("1")),)
    )
    return BacktestDayInput(
        trade_date=trade_date,
        ranking=RankingSnapshot(
            source_identity="a" * 64,
            source_trade_date=previous_trade_date,
            observed_at=_at(trade_date, 9, 24),
            candidates=candidates,
        ),
        instruments=tuple(
            BacktestInstrument(
                ts_code=code,
                instrument_context=paper_instrument_context(code),
                classification_observed_at=_at(previous_trade_date, 15),
                open_price=Decimal("10"),
                open_observed_at=_at(trade_date, 9, 30),
                close_price=None if code == missing_close else Decimal("10"),
                close_observed_at=None if code == missing_close else _at(trade_date, 15),
                price_source_identity="b" * 64,
                conditions=None
                if code == missing_condition
                else TradeConditions(
                    source_identity="c" * 64,
                    observed_at=_at(trade_date, 9, 30),
                    suspended=False,
                    buy_limit_locked=code == locked_buy,
                    sell_limit_locked=code == locked_sell,
                ),
            )
            for code in _CODES
        ),
    )


def _request(
    selections: tuple[str | None, ...],
    *,
    locked_buy_day: int | None = None,
    locked_sell_day: int | None = None,
    missing_condition_day: int | None = None,
    missing_close_day: int | None = None,
    rebalance: RebalanceRule | None = None,
    cash: str = "3000.00",
    reserve: str = "0.50",
) -> BacktestRequest:
    dates = _TRADE_DATES[: len(selections)]
    calendar_dates = _CALENDAR_DATES[: len(selections) + 2]
    return BacktestRequest(
        schema_version=1,
        producer_commit="d" * 40,
        input_generation_id="e" * 64,
        calendar=SSECalendar(
            source_identity="f" * 64,
            dates=calendar_dates,
            coverage_start=calendar_dates[0],
            coverage_end=calendar_dates[-1],
        ),
        days=tuple(
            _day(
                trade_date,
                calendar_dates[index],
                selected,
                locked_buy=selected if index == locked_buy_day else None,
                locked_sell=_CODES[0] if index == locked_sell_day else None,
                missing_condition=selected if index == missing_condition_day else None,
                missing_close=selected if index == missing_close_day else None,
            )
            for index, (trade_date, selected) in enumerate(zip(dates, selections, strict=True))
        ),
        initial_cash=Decimal(cash),
        weight_rule=PortfolioWeightRule(
            method="equal", max_positions=1, cash_reserve=Decimal(reserve)
        ),
        rebalance_rule=rebalance or RebalanceRule(kind="daily"),
        execution_cost_spec=paper_execution_cost_spec(),
    )


def test_ten_day_three_stock_manual_cash_and_nav_replay(tmp_path: Path) -> None:
    request = _request(
        (
            _CODES[0],
            _CODES[0],
            _CODES[1],
            _CODES[1],
            _CODES[2],
            _CODES[2],
            None,
            None,
            _CODES[0],
            None,
        ),
        locked_buy_day=4,
    )
    result = run_portfolio_backtest(request, research_root=tmp_path)

    assert result.schema_version == 1
    assert result.producer_commit == request.producer_commit
    assert result.input_generation_id == request.input_generation_id
    assert result.calendar_source_identity == request.calendar.source_identity
    assert result.cost_spec_id == request.execution_cost_spec.cost_spec_id
    assert result.status == "complete"
    assert len(result.days) == 10
    assert [day.account.nav for day in result.days] == list(
        map(
            Decimal,
            ("2995", "2995", "2984", "2984", "2978", "2973", "2967", "2967", "2962", "2956"),
        )
    )
    assert [day.account.cash for day in result.days] == list(
        map(
            Decimal,
            ("1995", "1995", "1984", "1984", "2978", "1973", "2967", "2967", "1962", "2956"),
        )
    )
    assert result.days[4].orders[1].receipt.order.status is PaperOrderStatus.REJECTED
    assert result.days[4].orders[1].receipt.order.reject_reason is PaperRejectReason.LIMIT_LOCKED
    assert result.days[0].account.holdings[0].available_quantity == 0
    assert result.days[1].account.holdings[0].available_quantity == 100
    assert result.days[9].account.holdings == ()
    assert [sum(day.fees for day in result.days)] == [Decimal("44")]
    assert all(
        day.account.cash
        + sum(
            (holding.quantity * holding.market_price for holding in day.account.holdings),
            Decimal("0"),
        )
        == day.account.nav
        for day in result.days
    )
    assert [day.market_value for day in result.days] == list(
        map(Decimal, ("1000", "1000", "1000", "1000", "0", "1000", "0", "0", "1000", "0"))
    )
    assert not tuple(tmp_path.iterdir())


def test_repeated_run_is_content_identical_despite_private_ephemeral_ledger(tmp_path: Path) -> None:
    request = _request((_CODES[0], _CODES[1], None))
    first = run_portfolio_backtest(request, research_root=tmp_path)
    second = run_portfolio_backtest(request, research_root=tmp_path)
    assert first == second
    assert first.content_hash == second.content_hash
    assert not tuple(tmp_path.iterdir())


def test_future_ranking_and_missing_calendar_day_are_rejected() -> None:
    request = _request((_CODES[0], _CODES[1], None))
    future = request.model_dump(mode="python")
    future["days"][0]["ranking"]["observed_at"] = _at(_TRADE_DATES[0], 9, 26)
    with pytest.raises(ValidationError, match="ranking.*cutoff"):
        BacktestRequest.model_validate(future)

    missing = request.model_dump(mode="python")
    missing["days"] = (missing["days"][0], missing["days"][2])
    with pytest.raises(ValidationError, match="calendar"):
        BacktestRequest.model_validate(missing)


def test_unverified_conditions_skip_order_without_claiming_suspension(tmp_path: Path) -> None:
    result = run_portfolio_backtest(
        _request((_CODES[0],), missing_condition_day=0), research_root=tmp_path
    )
    assert result.days[0].orders == ()
    assert result.days[0].skipped[0].reason == "unverified_conditions"
    assert result.days[0].account.nav == Decimal("3000")


def test_missing_held_close_makes_day_and_result_incomplete(tmp_path: Path) -> None:
    result = run_portfolio_backtest(
        _request((_CODES[0], _CODES[0], _CODES[0]), missing_close_day=1),
        research_root=tmp_path,
    )
    assert result.status == "incomplete"
    assert len(result.days) == 2
    assert result.days[1].account is None
    assert result.days[1].incomplete_reason == "missing_held_close"


def test_missing_held_close_on_last_day_remains_incomplete(tmp_path: Path) -> None:
    result = run_portfolio_backtest(
        _request((_CODES[0],), missing_close_day=0), research_root=tmp_path
    )
    assert result.status == "incomplete"
    assert result.days[0].account is None

    forged = result.model_dump(mode="python", exclude={"content_hash"})
    forged["status"] = "complete"
    with pytest.raises(ValidationError, match="status"):
        BacktestResult.model_validate(forged)


def test_partial_rebalance_uses_broker_sell_authority_and_fifo_cash(tmp_path: Path) -> None:
    request_data = _request((_CODES[0], _CODES[0]), cash="8000").model_dump(mode="python")
    second_quote = request_data["days"][1]["instruments"][0]
    second_quote["open_price"] = Decimal("15")
    second_quote["close_price"] = Decimal("15")
    result = run_portfolio_backtest(
        BacktestRequest.model_validate(request_data), research_root=tmp_path
    )

    assert result.days[0].account.holdings[0].quantity == 400
    sell = result.days[1].orders[0]
    assert sell.intent.side.value == "SELL"
    assert sell.intent.quantity == 200
    assert sell.intent.sell_quantity_authority.action == "REDUCE"
    assert sell.receipt.fill.quantity == 200
    assert sell.receipt.fill.total_fees == Decimal("8")
    assert result.days[1].account.holdings[0].quantity == 200
    assert result.days[1].account.cash == Decimal("6987")
    assert result.days[1].account.nav == Decimal("9987")


@pytest.mark.parametrize(
    ("state", "reason"),
    [
        ("suspended", PaperRejectReason.SUSPENDED),
        ("buy_limit_locked", PaperRejectReason.LIMIT_LOCKED),
    ],
)
def test_known_non_tradable_buy_is_broker_rejected(
    tmp_path: Path, state: str, reason: PaperRejectReason
) -> None:
    request_data = _request((_CODES[0],)).model_dump(mode="python")
    request_data["days"][0]["instruments"][0]["conditions"][state] = True
    result = run_portfolio_backtest(
        BacktestRequest.model_validate(request_data), research_root=tmp_path
    )
    assert result.days[0].orders[0].receipt.order.reject_reason is reason
    assert result.days[0].account.cash == Decimal("3000")


def test_directional_sell_limit_and_insufficient_cash_are_broker_rejections(tmp_path: Path) -> None:
    sell_result = run_portfolio_backtest(
        _request((_CODES[0], None), locked_sell_day=1), research_root=tmp_path
    )
    assert (
        sell_result.days[1].orders[0].receipt.order.reject_reason is PaperRejectReason.LIMIT_LOCKED
    )
    assert sell_result.days[1].account.holdings[0].quantity == 100

    cash_result = run_portfolio_backtest(
        _request((_CODES[0],), cash="1000", reserve="0"), research_root=tmp_path
    )
    assert (
        cash_result.days[0].orders[0].receipt.order.reject_reason
        is PaperRejectReason.INSUFFICIENT_CASH
    )
    assert cash_result.days[0].account.nav == Decimal("1000")


def test_sub_lot_and_missing_open_do_not_invent_fills(tmp_path: Path) -> None:
    sub_lot = run_portfolio_backtest(
        _request((_CODES[0],), cash="1000", reserve="0.50"), research_root=tmp_path
    )
    assert sub_lot.days[0].orders == ()
    assert sub_lot.days[0].skipped[0].reason == "below_lot"

    missing = _request((_CODES[0],)).model_dump(mode="python")
    first_quote = missing["days"][0]["instruments"][0]
    first_quote["open_price"] = None
    first_quote["open_observed_at"] = None
    no_open = run_portfolio_backtest(
        BacktestRequest.model_validate(missing), research_root=tmp_path
    )
    assert no_open.days[0].orders == ()
    assert no_open.days[0].skipped[0].reason == "missing_open_price"


def test_each_backtest_execution_matches_direct_paper_broker(tmp_path: Path) -> None:
    request = _request((_CODES[0], _CODES[1], None))
    result = run_portfolio_backtest(request, research_root=tmp_path)
    broker = PaperBrokerStore(
        tmp_path / "direct.sqlite3",
        account_id=result.days[0].orders[0].intent.account_id,
        initial_cash=request.initial_cash,
        cost_policy=BrokerCostPolicy.from_execution_cost_spec(request.execution_cost_spec),
    )
    for day_index, day in enumerate(result.days):
        for replay_order in day.orders:
            quote = next(
                item
                for item in request.days[day_index].instruments
                if item.ts_code == replay_order.intent.ts_code
            )
            conditions = quote.conditions
            direct_order = broker.submit_intent(
                replay_order.intent,
                execution_id=replay_order.receipt.execution_id,
                decision_time=_at(day.trade_date, 9, 31),
                persisted_at=_at(day.trade_date, 9, 31),
                trade_date=day.trade_date,
                quote=BrokerExecutionContext(
                    executable_price=quote.open_price,
                    instrument_context=quote.instrument_context,
                    acquisition_available_date=(
                        request.calendar.dates[day_index + 2]
                        if replay_order.intent.side is PaperSide.BUY
                        else None
                    ),
                    suspended=conditions.suspended,
                    limit_locked=(
                        conditions.buy_limit_locked
                        if replay_order.intent.side is PaperSide.BUY
                        else conditions.sell_limit_locked
                    ),
                ),
            )
            assert direct_order == replay_order.receipt.order
            assert broker.execution(replay_order.receipt.execution_id) == replay_order.receipt
        direct_account = broker.account_snapshot(
            as_of=_at(day.trade_date, 15, 1),
            market_prices={holding.code: Decimal("10") for holding in day.account.holdings},
        )
        assert direct_account == day.account
    assert result.days[0].account.holdings[0].available_quantity == 0


def test_same_day_sell_is_blocked_by_shared_broker_t_plus_one(tmp_path: Path) -> None:
    request = _request((_CODES[0],))
    replay = run_portfolio_backtest(request, research_root=tmp_path)
    buy = replay.days[0].orders[0]
    broker = PaperBrokerStore(
        tmp_path / "t-plus-one.sqlite3",
        account_id=buy.intent.account_id,
        initial_cash=request.initial_cash,
        cost_policy=BrokerCostPolicy.from_execution_cost_spec(request.execution_cost_spec),
    )
    quote = request.days[0].instruments[0]
    broker.submit_intent(
        buy.intent,
        execution_id=buy.receipt.execution_id,
        decision_time=_at(_TRADE_DATES[0], 9, 31),
        trade_date=_TRADE_DATES[0],
        quote=BrokerExecutionContext(
            executable_price=Decimal("10"),
            instrument_context=quote.instrument_context,
            acquisition_available_date=_TRADE_DATES[1],
        ),
    )
    sell_at = _at(_TRADE_DATES[0], 9, 31).replace(second=30)
    authority = broker.sell_quantity_authority(
        exit_signal_id="1" * 64,
        entry_signal_id=buy.intent.signal_id,
        ts_code=_CODES[0],
        action="S_INTENT",
        tranche_fraction=Decimal("1"),
        decision_cutoff=sell_at,
        trade_date=_TRADE_DATES[0],
    )
    sell = PaperOrderIntent(
        signal_id="1" * 64,
        entry_signal_id=buy.intent.signal_id,
        sell_quantity_authority=authority,
        account_id=buy.intent.account_id,
        ts_code=_CODES[0],
        side=PaperSide.SELL,
        order_type=PaperOrderType.MARKET,
        quantity=100,
        event_time=_at(_TRADE_DATES[0], 9, 25),
        available_at=_at(_TRADE_DATES[0], 9, 25),
        earliest_execution_at=sell_at,
        expires_at=_at(_TRADE_DATES[0], 9, 32),
        price_snapshot_id=buy.intent.price_snapshot_id,
        producer_commit=request.producer_commit,
    )
    rejected = broker.submit_intent(
        sell,
        decision_time=sell_at,
        trade_date=_TRADE_DATES[0],
        quote=BrokerExecutionContext(
            executable_price=Decimal("10"), instrument_context=quote.instrument_context
        ),
    )
    assert rejected.reject_reason is PaperRejectReason.T_PLUS_ONE


def test_pit_requires_authoritative_source_and_visible_classification() -> None:
    data = _request((_CODES[0],)).model_dump(mode="python")
    data["days"][0]["instruments"][0]["classification_observed_at"] = _at(_TRADE_DATES[0], 9, 26)
    with pytest.raises(ValidationError, match="classification.*cutoff"):
        BacktestRequest.model_validate(data)
    data = _request((_CODES[0],)).model_dump(mode="python")
    del data["days"][0]["instruments"][0]["price_source_identity"]
    with pytest.raises(ValidationError, match="price_source_identity"):
        BacktestRequest.model_validate(data)

    data = _request((_CODES[0],)).model_dump(mode="python")
    data["days"][0]["instruments"][0]["conditions"]["observed_at"] = _at(_TRADE_DATES[0], 9, 29)
    with pytest.raises(ValidationError, match="condition.*open"):
        BacktestRequest.model_validate(data)


def test_initial_cash_requires_whole_cents() -> None:
    data = _request((_CODES[0],)).model_dump(mode="python")
    data["initial_cash"] = Decimal("3000.001")
    with pytest.raises(ValidationError, match="cent"):
        BacktestRequest.model_validate(data)


def test_weekly_rebalance_follows_first_open_session_of_next_week(tmp_path: Path) -> None:
    result = run_portfolio_backtest(
        _request((_CODES[0],) * 6, rebalance=RebalanceRule(kind="weekly")),
        research_root=tmp_path,
    )
    assert tuple(day.rebalanced for day in result.days) == (
        True,
        False,
        False,
        False,
        False,
        True,
    )


def test_monthly_rebalance_follows_first_open_session_of_next_month(tmp_path: Path) -> None:
    dates = (date(2026, 8, 28), date(2026, 8, 31), date(2026, 9, 1))
    calendar_dates = (date(2026, 8, 27), *dates, date(2026, 9, 2))
    template = _request((_CODES[0],) * 3)
    request = BacktestRequest(
        producer_commit=template.producer_commit,
        input_generation_id=template.input_generation_id,
        calendar=SSECalendar(
            source_identity="f" * 64,
            coverage_start=calendar_dates[0],
            coverage_end=calendar_dates[-1],
            dates=calendar_dates,
        ),
        days=tuple(
            _day(trade_date, calendar_dates[index], _CODES[0])
            for index, trade_date in enumerate(dates)
        ),
        initial_cash=template.initial_cash,
        weight_rule=template.weight_rule,
        rebalance_rule=RebalanceRule(kind="monthly"),
        execution_cost_spec=template.execution_cost_spec,
    )
    result = run_portfolio_backtest(request, research_root=tmp_path)
    assert tuple(day.rebalanced for day in result.days) == (True, False, True)


@pytest.mark.parametrize(
    ("rule", "expected"),
    [
        (RebalanceRule(kind="daily"), (True, True, True)),
        (RebalanceRule(kind="weekly"), (True, False, False)),
        (RebalanceRule(kind="monthly"), (True, False, False)),
        (RebalanceRule(kind="every_n", every_n_days=2), (True, False, True)),
    ],
)
def test_rebalance_periods_use_trading_day_index(
    tmp_path: Path, rule: RebalanceRule, expected: tuple[bool, ...]
) -> None:
    result = run_portfolio_backtest(
        _request((_CODES[0], _CODES[1], _CODES[2]), rebalance=rule),
        research_root=tmp_path,
    )
    assert tuple(day.rebalanced for day in result.days) == expected
