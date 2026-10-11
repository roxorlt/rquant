"""The research job: refresh data, rebuild screens, recompute factor tests, strategy
backtests, factor tracking and the coverage audit. Idempotent; safe to rerun.

    python -m rquant.research daily [--years 5] [--minutes 300] [--no-ingest]
"""

from __future__ import annotations

import argparse
import logging
import shutil
from datetime import date, timedelta
from pathlib import Path

log = logging.getLogger("rquant.research")

FACTORS = {
    "20 日动量": "cs_rank(ts_delta(close, 20) / delay(close, 20))",
    "5 日反转": "cs_rank(0 - ts_delta(close, 5) / delay(close, 5))",
    "20 日低波动": "cs_rank(0 - ts_std(pct_chg, 20))",
    "成交额放大": "cs_rank(ts_mean(amount, 5) / ts_mean(amount, 60))",
    "距 60 日高点": "cs_rank(close / ts_max(high, 60))",
    "均线偏离": "cs_rank(close / ts_mean(close, 20) - 1)",
}

STRATEGIES = [
    ("mom20-top10", "20 日动量 Top10 周调仓", "research_mom20", 10, 5),
    ("breakout55-top10", "55 日新高 Top10 周调仓", "research_breakout55", 10, 5),
    ("lowvol-top20", "低波动 Top20 月调仓", "research_lowvol", 20, 20),
    ("value-top20", "低 PB Top20 月调仓", "research_value", 20, 20),
    ("trend-top10", "趋势 Top10 周调仓", "research_trend", 10, 5),
]


def _root() -> Path:
    from rquant.backtest.store import research_root

    return research_root()


def run(years: int = 5, minutes: int = 300, ingest: bool = True, end: date | None = None) -> None:
    import duckdb

    from rquant.backtest import BacktestConfig
    from rquant.backtest.store import run_and_save
    from rquant.backtest.strategy import StrategySpec, resolve, save_version
    from rquant.data_catalog.audit import audit, write_report
    from rquant.factor.store import list_factors, load_panel, run_factor, save, save_tracking, track
    from rquant.portfolio import PortfolioWeightRule
    from rquant.research import ingest as ing
    from rquant.research import screens

    root = _root()
    end = end or date.today()
    if ingest:
        log.info("ingest: %s", ing.run(root, years=years, end=end, minutes=minutes))
    db = ing.db_path(root)
    con = ing.connect(root)
    try:
        last = con.execute("SELECT MAX(trade_date) FROM daily_bar").fetchone()[0]
        first = con.execute("SELECT MIN(trade_date) FROM daily_bar").fetchone()[0]
        bt_start = max(first + timedelta(days=200), last - timedelta(days=365 * 3))
        log.info("screens: %d rows", screens.build(con, bt_start - timedelta(days=10), last))
        write_report(audit(con, last, 30), root)
    finally:
        con.close()

    for slug, title, preset, n, every in STRATEGIES:
        spec = StrategySpec(slug=slug, title=title, preset=preset, config=BacktestConfig(
            rebalance_every=every, weights=PortfolioWeightRule(max_positions=n)))
        save_version(spec, root)
        version = resolve(slug, root)
        run_and_save(db, preset, bt_start, last, version.spec.config, root=root,
                     title=f"{title} {bt_start}~{last}", strategy=f"{slug}@{version.version}",
                     bar_table="daily_bar_hfq")
        log.info("backtest %s done", slug)

    con = duckdb.connect(str(db), read_only=True)
    try:
        f_start = last - timedelta(days=365 * 2)
        panel = load_panel(con, f_start, last, table="daily_bar_hfq")
    finally:
        con.close()
    existing = {f.name: f for f in list_factors(root)}
    for name, expr in FACTORS.items():
        old = existing.get(name)
        if old is not None and (old.start, old.end) != (f_start, last):
            shutil.rmtree(root / "factor" / old.factor_id, ignore_errors=True)
        run = save(name, expr, f_start, last, run_factor(panel, expr, f_start, last), root=root)
        existing[name] = run
        log.info("factor %s done", name)
    for run in existing.values():
        save_tracking(track(run, panel, last, lookback_days=60), root)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(prog="python -m rquant.research")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("daily")
    d.add_argument("--years", type=int, default=5)
    d.add_argument("--minutes", type=int, default=300)
    d.add_argument("--no-ingest", action="store_true")
    a = p.parse_args(argv)
    run(years=a.years, minutes=a.minutes, ingest=not a.no_ingest)
    return 0
