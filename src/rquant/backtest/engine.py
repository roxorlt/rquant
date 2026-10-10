"""Pure daily portfolio backtest with A-share execution rules.

Signal on day t (the candidate list known after t's close) → orders at the open
of the next trading day. Rules: T+1 (a lot bought today cannot be sold today;
since all trades happen at the open this means "sell only what was held
yesterday"), no buy at limit-up open, no sell at limit-down open, no trade when
suspended (no bar), whole lots of 100, versioned simple costs. Prices are
unadjusted closes; corporate actions show up as price gaps (known limitation).
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.portfolio import (
    PortfolioCandidate,
    PortfolioWeightRule,
    allocate_target_weights,
)

LOT = 100


class Bar(BaseModel):
    model_config = ConfigDict(frozen=True)

    open: float
    close: float
    pre_close: float
    limit_pct: float = 0.10


class CostModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: str = "cn-a-2023"
    commission_rate: float = 0.00025
    commission_min: float = 5.0
    stamp_tax_sell: float = 0.0005
    transfer_rate: float = 0.00001

    def fee(self, side: Literal["buy", "sell"], notional: float) -> float:
        commission = max(self.commission_min, notional * self.commission_rate)
        stamp = notional * self.stamp_tax_sell if side == "sell" else 0.0
        return round(commission + stamp + notional * self.transfer_rate, 2)


class BacktestConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    capital: float = Field(default=1_000_000, gt=0)
    rebalance_every: int = Field(default=1, ge=1)
    weights: PortfolioWeightRule = PortfolioWeightRule(max_positions=10)
    costs: CostModel = CostModel()


class Order(BaseModel):
    trade_date: date
    ts_code: str
    side: Literal["buy", "sell"]
    quantity: int
    price: float | None
    fee: float
    status: Literal["filled", "rejected"]
    reason: str | None = None


class DayRecord(BaseModel):
    trade_date: date
    cash: float
    market_value: float
    nav: float
    positions: dict[str, int]
    #: market value per held code at the close
    values: dict[str, float] = {}


class BacktestResult(BaseModel):
    config: BacktestConfig
    days: list[DayRecord]
    orders: list[Order]


def _limit_price(pre_close: float, pct: float, up: bool) -> float:
    return round(pre_close * (1 + pct if up else 1 - pct) + 1e-9, 2)


def run_backtest(
    trading_days: Sequence[date],
    bars: Mapping[date, Mapping[str, Bar]],
    signals: Mapping[date, Sequence[PortfolioCandidate]],
    config: BacktestConfig | None = None,
) -> BacktestResult:
    config = config or BacktestConfig()
    cash = float(config.capital)
    held: dict[str, int] = {}
    last_close: dict[str, float] = {}
    orders: list[Order] = []
    records: list[DayRecord] = []
    pending: Sequence[PortfolioCandidate] | None = None
    signal_count = 0

    for day in trading_days:
        today = bars.get(day, {})
        if pending is not None:
            equity = cash + sum(q * last_close.get(c, 0.0) for c, q in held.items())
            target = allocate_target_weights(
                pending, config.weights, capital=Decimal(str(round(equity, 2)))
            )
            want = {p.ts_code: float(p.target_amount) for p in target.positions
                    if p.status == "selected"}
            # sells first, so the cash is there for buys
            for code in sorted(held):
                bar = today.get(code)
                if bar is None:
                    if code not in want:
                        orders.append(Order(trade_date=day, ts_code=code, side="sell",
                                            quantity=held[code], price=None, fee=0,
                                            status="rejected", reason="停牌"))
                    continue
                qty = held[code] - LOT * int(want.get(code, 0.0) / bar.open // LOT)
                if qty <= 0:
                    continue
                if bar.open <= _limit_price(bar.pre_close, bar.limit_pct, up=False):
                    orders.append(Order(trade_date=day, ts_code=code, side="sell", quantity=qty,
                                        price=bar.open, fee=0, status="rejected",
                                        reason="跌停开盘"))
                    continue
                notional = qty * bar.open
                fee = config.costs.fee("sell", notional)
                cash += notional - fee
                held[code] -= qty
                if not held[code]:
                    del held[code]
                orders.append(Order(trade_date=day, ts_code=code, side="sell", quantity=qty,
                                    price=bar.open, fee=fee, status="filled"))
            for code, amount in sorted(want.items()):
                bar = today.get(code)
                if bar is None:
                    orders.append(Order(trade_date=day, ts_code=code, side="buy", quantity=0,
                                        price=None, fee=0, status="rejected", reason="停牌"))
                    continue
                qty = LOT * int(amount / bar.open // LOT) - held.get(code, 0)
                if qty <= 0:
                    continue
                if bar.open >= _limit_price(bar.pre_close, bar.limit_pct, up=True):
                    orders.append(Order(trade_date=day, ts_code=code, side="buy", quantity=qty,
                                        price=bar.open, fee=0, status="rejected",
                                        reason="涨停开盘"))
                    continue
                while qty > 0 and qty * bar.open + config.costs.fee("buy", qty * bar.open) > cash:
                    qty -= LOT
                if qty <= 0:
                    orders.append(Order(trade_date=day, ts_code=code, side="buy", quantity=0,
                                        price=bar.open, fee=0, status="rejected",
                                        reason="现金不足"))
                    continue
                notional = qty * bar.open
                fee = config.costs.fee("buy", notional)
                cash -= notional + fee
                held[code] = held.get(code, 0) + qty
                orders.append(Order(trade_date=day, ts_code=code, side="buy", quantity=qty,
                                    price=bar.open, fee=fee, status="filled"))
            pending = None
        for code, bar in today.items():
            last_close[code] = bar.close
        value = sum(q * last_close.get(c, 0.0) for c, q in held.items())
        records.append(DayRecord(trade_date=day, cash=round(cash, 2),
                                 market_value=round(value, 2),
                                 nav=(cash + value) / config.capital, positions=dict(held),
                                 values={c: round(q * last_close.get(c, 0.0), 2)
                                         for c, q in held.items()}))
        if day in signals:
            if signal_count % config.rebalance_every == 0:
                pending = signals[day]
            signal_count += 1
    if any(not math.isfinite(r.nav) for r in records):
        raise ValueError("non-finite NAV")
    return BacktestResult(config=config, days=records, orders=orders)
