"""Deterministic daily replay around the existing paper execution ledger."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from rquant.backtest.contracts import (
    BacktestDayInput,
    BacktestDayResult,
    BacktestInstrument,
    BacktestOrder,
    BacktestRequest,
    BacktestResult,
    RebalanceRule,
    SkippedTarget,
)
from rquant.paper_broker import BrokerCostPolicy, BrokerExecutionContext, PaperBrokerStore
from rquant.paper_contracts import PaperOrderIntent, PaperOrderType, PaperSide
from rquant.portfolio.weights import allocate_target_weights
from rquant.runtime_contracts import canonical_sha256

_SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass
class _Entry:
    ts_code: str
    signal_id: str
    remaining: int


def _local_at(trade_date: date, hour: int, minute: int) -> datetime:
    return datetime.combine(trade_date, time(hour, minute), tzinfo=_SHANGHAI)


def _should_rebalance(index: int, days: tuple[BacktestDayInput, ...], rule: RebalanceRule) -> bool:
    if index == 0 or rule.kind == "daily":
        return True
    if rule.kind == "every_n":
        assert rule.every_n_days is not None
        return index % rule.every_n_days == 0
    today = days[index].trade_date
    previous = days[index - 1].trade_date
    if rule.kind == "weekly":
        return today.isocalendar()[:2] != previous.isocalendar()[:2]
    return (today.year, today.month) != (previous.year, previous.month)


def _quantity_by_code(entries: list[_Entry]) -> dict[str, int]:
    quantities: dict[str, int] = {}
    for entry in entries:
        if entry.remaining:
            quantities[entry.ts_code] = quantities.get(entry.ts_code, 0) + entry.remaining
    return quantities


def _executable_quote(
    instrument: BacktestInstrument,
    *,
    side: PaperSide,
    next_trade_date: date,
) -> BrokerExecutionContext:
    assert instrument.open_price is not None
    assert instrument.conditions is not None
    return BrokerExecutionContext(
        executable_price=instrument.open_price,
        instrument_context=instrument.instrument_context,
        acquisition_available_date=next_trade_date if side is PaperSide.BUY else None,
        suspended=instrument.conditions.suspended,
        limit_locked=(
            instrument.conditions.buy_limit_locked
            if side is PaperSide.BUY
            else instrument.conditions.sell_limit_locked
        ),
    )


def _can_submit(
    ts_code: str,
    side: PaperSide,
    instruments: dict[str, BacktestInstrument],
) -> SkippedTarget | None:
    instrument = instruments.get(ts_code)
    if instrument is None or instrument.open_price is None:
        return SkippedTarget(ts_code=ts_code, side=side.value, reason="missing_open_price")
    if instrument.conditions is None:
        return SkippedTarget(ts_code=ts_code, side=side.value, reason="unverified_conditions")
    return None


def _order(
    broker: PaperBrokerStore,
    request: BacktestRequest,
    day: BacktestDayInput,
    instrument: BacktestInstrument,
    *,
    side: PaperSide,
    quantity: int,
    next_trade_date: date,
    ordinal: int,
    entry_signal_id: str | None = None,
    entry_remaining: int | None = None,
) -> BacktestOrder:
    execution_at = _local_at(day.trade_date, 9, 31)
    signal_id = canonical_sha256(
        {
            "request_id": request.request_id,
            "trade_date": day.trade_date,
            "ts_code": instrument.ts_code,
            "side": side,
            "entry_signal_id": entry_signal_id,
            "ordinal": ordinal,
        }
    )
    authority = None
    if side is PaperSide.SELL:
        assert entry_signal_id is not None
        assert entry_remaining is not None
        # A point inside this 100-share interval lets the broker's own floor rule
        # authorize exactly the requested partial tranche, including thirds.
        action = "S_INTENT" if quantity == entry_remaining else "REDUCE"
        fraction = (
            Decimal("1")
            if action == "S_INTENT"
            else Decimal(quantity + 50) / Decimal(entry_remaining)
        )
        authority = broker.sell_quantity_authority(
            exit_signal_id=signal_id,
            entry_signal_id=entry_signal_id,
            ts_code=instrument.ts_code,
            action=action,
            tranche_fraction=fraction,
            decision_cutoff=execution_at,
            trade_date=day.trade_date,
        )
        if authority.requested_quantity != quantity:
            raise RuntimeError("broker sell authority disagrees with planned quantity")
    intent = PaperOrderIntent(
        signal_id=signal_id,
        entry_signal_id=entry_signal_id,
        sell_quantity_authority=authority,
        account_id=broker.account_id,
        ts_code=instrument.ts_code,
        side=side,
        order_type=PaperOrderType.MARKET,
        quantity=quantity,
        event_time=_local_at(day.trade_date, 9, 25),
        available_at=_local_at(day.trade_date, 9, 25),
        earliest_execution_at=execution_at,
        expires_at=_local_at(day.trade_date, 9, 32),
        price_snapshot_id=canonical_sha256(
            {
                "source_identity": instrument.price_source_identity,
                "trade_date": day.trade_date,
                "ts_code": instrument.ts_code,
                "open_price": instrument.open_price,
                "open_observed_at": instrument.open_observed_at,
            }
        ),
        producer_commit=request.producer_commit,
    )
    execution_id = canonical_sha256({"source": "backtest", "intent_id": intent.intent_id})
    order = broker.submit_intent(
        intent,
        execution_id=execution_id,
        decision_time=execution_at,
        persisted_at=execution_at,
        trade_date=day.trade_date,
        quote=_executable_quote(instrument, side=side, next_trade_date=next_trade_date),
    )
    receipt = broker.execution(execution_id)
    if receipt is None or receipt.order != order:
        raise RuntimeError("broker did not persist the expected execution receipt")
    return BacktestOrder(intent=intent, receipt=receipt)


def _replay_day(
    broker: PaperBrokerStore,
    request: BacktestRequest,
    day: BacktestDayInput,
    *,
    next_trade_date: date,
    previous_nav: Decimal,
    entries: list[_Entry],
) -> tuple[tuple[BacktestOrder, ...], tuple[SkippedTarget, ...]]:
    instruments = {item.ts_code: item for item in day.instruments}
    target_quantities: dict[str, int] = {}
    protected: set[str] = set()
    skipped: list[SkippedTarget] = []
    if day.ranking.candidates:
        allocation = allocate_target_weights(
            day.ranking.candidates, request.weight_rule, capital=previous_nav
        )
        for target in allocation.positions:
            if target.status != "selected" or target.target_amount == 0:
                continue
            instrument = instruments.get(target.ts_code)
            if instrument is None or instrument.open_price is None:
                protected.add(target.ts_code)
                skipped.append(
                    SkippedTarget(ts_code=target.ts_code, side="BUY", reason="missing_open_price")
                )
                continue
            quantity = int(target.target_amount / instrument.open_price) // 100 * 100
            if quantity == 0:
                skipped.append(
                    SkippedTarget(ts_code=target.ts_code, side="BUY", reason="below_lot")
                )
            target_quantities[target.ts_code] = quantity

    orders: list[BacktestOrder] = []
    for ts_code, held_quantity in sorted(_quantity_by_code(entries).items()):
        if ts_code in protected:
            continue
        excess = held_quantity - target_quantities.get(ts_code, 0)
        if excess <= 0:
            continue
        skip = _can_submit(ts_code, PaperSide.SELL, instruments)
        if skip is not None:
            skipped.append(skip)
            continue
        instrument = instruments[ts_code]
        for entry in entries:
            if entry.ts_code != ts_code or entry.remaining == 0 or excess == 0:
                continue
            quantity = min(excess, entry.remaining)
            order = _order(
                broker,
                request,
                day,
                instrument,
                side=PaperSide.SELL,
                quantity=quantity,
                next_trade_date=next_trade_date,
                ordinal=len(orders),
                entry_signal_id=entry.signal_id,
                entry_remaining=entry.remaining,
            )
            orders.append(order)
            fill = order.receipt.fill
            if fill is not None:
                entry.remaining -= fill.quantity
                excess -= fill.quantity

    for ts_code, target_quantity in sorted(target_quantities.items()):
        deficit = target_quantity - _quantity_by_code(entries).get(ts_code, 0)
        if deficit <= 0:
            continue
        skip = _can_submit(ts_code, PaperSide.BUY, instruments)
        if skip is not None:
            skipped.append(skip)
            continue
        order = _order(
            broker,
            request,
            day,
            instruments[ts_code],
            side=PaperSide.BUY,
            quantity=deficit,
            next_trade_date=next_trade_date,
            ordinal=len(orders),
        )
        orders.append(order)
        fill = order.receipt.fill
        if fill is not None:
            entries.append(
                _Entry(ts_code=ts_code, signal_id=order.intent.signal_id, remaining=fill.quantity)
            )
    return tuple(orders), tuple(skipped)


def run_portfolio_backtest(request: BacktestRequest, *, research_root: Path) -> BacktestResult:
    """Replay one typed daily input in a new private ledger, then discard it."""

    request = BacktestRequest.model_validate(request)
    root = Path(research_root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("research_root must be an existing real directory")
    account_id = f"backtest-{request.request_id[:20]}"
    entries: list[_Entry] = []
    results: list[BacktestDayResult] = []
    previous_nav = request.initial_cash
    previous_cash = request.initial_cash
    with tempfile.TemporaryDirectory(prefix="portfolio-backtest-", dir=root) as private_dir:
        broker = PaperBrokerStore(
            Path(private_dir) / "ledger.sqlite3",
            account_id=account_id,
            initial_cash=request.initial_cash,
            cost_policy=BrokerCostPolicy.from_execution_cost_spec(request.execution_cost_spec),
        )
        for index, day in enumerate(request.days):
            rebalanced = _should_rebalance(index, request.days, request.rebalance_rule)
            if rebalanced:
                calendar_index = request.calendar.dates.index(day.trade_date)
                orders, skipped = _replay_day(
                    broker,
                    request,
                    day,
                    next_trade_date=request.calendar.dates[calendar_index + 1],
                    previous_nav=previous_nav,
                    entries=entries,
                )
            else:
                orders, skipped = (), ()
            fees = sum(
                (
                    order.receipt.fill.total_fees
                    for order in orders
                    if order.receipt.fill is not None and order.receipt.fill.total_fees is not None
                ),
                Decimal("0"),
            )
            quotes = {item.ts_code: item for item in day.instruments}
            held = _quantity_by_code(entries)
            if any(code not in quotes or quotes[code].close_price is None for code in held):
                results.append(
                    BacktestDayResult(
                        trade_date=day.trade_date,
                        rebalanced=rebalanced,
                        orders=orders,
                        skipped=skipped,
                        fees=fees,
                        account=None,
                        incomplete_reason="missing_held_close",
                    )
                )
                break
            account = broker.account_snapshot(
                as_of=_local_at(day.trade_date, 15, 1),
                market_prices={code: quotes[code].close_price for code in held},
            )
            if {item.code: item.quantity for item in account.holdings} != held:
                raise RuntimeError("broker holdings disagree with execution receipts")
            expected_cash = previous_cash
            for order in orders:
                fill = order.receipt.fill
                if fill is None:
                    continue
                assert fill.total_fees is not None
                amount = (
                    fill.notional + fill.total_fees
                    if order.intent.side is PaperSide.BUY
                    else -fill.notional + fill.total_fees
                )
                expected_cash -= amount
            if account.cash != expected_cash:
                raise RuntimeError("broker cash disagrees with execution receipts")
            results.append(
                BacktestDayResult(
                    trade_date=day.trade_date,
                    rebalanced=rebalanced,
                    orders=orders,
                    skipped=skipped,
                    fees=fees,
                    account=account,
                    market_value=sum(
                        (holding.quantity * holding.market_price for holding in account.holdings),
                        Decimal("0"),
                    ),
                    daily_return=account.nav / previous_nav - Decimal("1"),
                    normalized_nav=account.nav / request.initial_cash,
                )
            )
            previous_nav = account.nav
            previous_cash = account.cash
    return BacktestResult(
        request_id=request.request_id,
        producer_commit=request.producer_commit,
        input_generation_id=request.input_generation_id,
        calendar_source_identity=request.calendar.source_identity,
        cost_spec_id=request.execution_cost_spec.cost_spec_id,
        status=(
            "complete"
            if len(results) == len(request.days) and all(day.account is not None for day in results)
            else "incomplete"
        ),
        days=tuple(results),
    )
