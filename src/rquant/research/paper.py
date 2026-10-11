"""Simulated paper accounts: each strategy template run over the recent N trading days
from a fresh capital, reported in the same shape as live paper accounts."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from rquant.backtest import run_backtest
from rquant.backtest.store import load_delisted, load_inputs
from rquant.web.models import HoldingItem, NavPoint, PaperAccount


def simulate(con, slug: str, preset: str, config, last: date, days: int = 60,
             names: dict[str, str] | None = None) -> PaperAccount:
    start = last - timedelta(days=int(days * 1.6))
    tdays, bars, signals = load_inputs(con, preset, start, last, "daily_bar_qfq")
    tdays = tdays[-days:]
    result = run_backtest(tdays, bars, signals, config, load_delisted(con))
    rec = result.days[-1]
    cost: dict[str, tuple[float, int]] = {}
    for o in result.orders:
        if o.status != "filled":
            continue
        amt, qty = cost.get(o.ts_code, (0.0, 0))
        if o.side == "buy":
            cost[o.ts_code] = (amt + o.quantity * (o.price or 0) + o.fee, qty + o.quantity)
        elif qty:
            keep = qty - o.quantity
            cost[o.ts_code] = (amt * keep / qty, keep)
    industries: dict[str, str] = {}
    if rec.positions:
        try:
            codes = list(rec.positions)
            marks = ",".join("?" * len(codes))
            industries = dict(con.execute(
                f"SELECT ts_code, industry FROM stock_basic WHERE ts_code IN ({marks})",
                codes).fetchall())
        except Exception:  # noqa: BLE001 - industry is decoration; never fail the account
            industries = {}
    holdings = []
    for code, q in rec.positions.items():
        amt, qty = cost.get(code, (0.0, q))
        avg = amt / qty if qty else 0.0
        mv = rec.values.get(code, 0.0)
        holdings.append(HoldingItem(code=code, name=(names or {}).get(code), quantity=q,
                                    average_cost=round(avg, 3), market_price=round(mv / q, 3),
                                    market_value=mv, unrealized_pnl=round(mv - avg * q, 2),
                                    industry=industries.get(code)))
    unreal = sum(h.unrealized_pnl for h in holdings)
    equity = rec.cash + rec.market_value
    return PaperAccount(
        account_id=f"sim-{slug}", as_of=datetime.combine(rec.trade_date, datetime.min.time(), UTC),
        nav=round(rec.nav, 4), cash=rec.cash, unrealized_pnl=round(unreal, 2),
        realized_pnl=round(equity - config.capital - unreal, 2),
        holdings=sorted(holdings, key=lambda h: -h.market_value),
        nav_series=_nav_series(result.days))


def _nav_series(days) -> list[NavPoint]:
    out, peak = [], 0.0
    for d in days:
        peak = max(peak, d.nav)
        out.append(NavPoint(date=d.trade_date, nav=round(d.nav, 6),
                            drawdown=round(d.nav / peak - 1, 6) if peak else 0.0))
    return out


def path(root: Path) -> Path:
    return root / "paper" / "accounts.json"


def save(root: Path, accounts: list[PaperAccount]) -> None:
    p = path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps([a.model_dump(mode="json") for a in accounts], ensure_ascii=False))
    tmp.replace(p)


def read(root: Path) -> list[PaperAccount]:
    p = path(root)
    if not p.exists():
        return []
    return [PaperAccount.model_validate(a) for a in json.loads(p.read_text())]
