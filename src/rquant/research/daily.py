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

from rquant.research.tdx_strategy import HJK

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
    from rquant.factor.store import (
        list_factors,
        load_panel,
        run_factor,
        save,
        save_latest,
        save_tracking,
        track,
    )
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
        write_report(audit(con, last, 30, extra=ing.RESEARCH_TABLES), root)
    finally:
        con.close()

    for slug, title, preset, n, every in STRATEGIES:
        spec = StrategySpec(slug=slug, title=title, preset=preset, config=BacktestConfig(
            rebalance_every=every, weights=PortfolioWeightRule(max_positions=n)))
        save_version(spec, root)
        version = resolve(slug, root)
        run_and_save(db, preset, bt_start, last, version.spec.config, root=root,
                     title=f"{title} {bt_start}~{last}", strategy=f"{slug}@{version.version}",
                     bar_table="daily_bar_qfq")
        log.info("backtest %s done", slug)
    from rquant.research import tdx_strategy

    for slug, title, _cname, formula, n, hold in tdx_strategy.run(root, db, first, last):
        spec = StrategySpec(slug=slug, title=title, preset=tdx_strategy.preset_name(slug),
                            note=formula, config=BacktestConfig(
                                hold_days=hold, weights=PortfolioWeightRule(max_positions=n)))
        save_version(spec, root)
        version = resolve(slug, root)
        tdx_start = first + timedelta(days=60)
        run_and_save(db, spec.preset, tdx_start, last, version.spec.config, root=root,
                     title=f"{title} {tdx_start}~{last}", strategy=f"{slug}@{version.version}",
                     bar_table="daily_bar_qfq")
        log.info("backtest %s done", slug)
    prune_backtests(root)

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
        save_latest(run, panel, root)
        log.info("factor %s done", name)
    for run in existing.values():
        save_tracking(track(run, panel, last, lookback_days=60), root)
    run_conditions(root, panel)
    run_paper(root, db, last)


CONDITIONS = {
    "站上 20 日线且放量": "C>MA(C,20) AND V>MA(V,5)*1.5",
    "5 日均线上穿 20 日均线": "MA(C,5)>MA(C,20) AND REF(MA(C,5),1)<=REF(MA(C,20),1)",
    "55 日新高": "C>=HHV(H,55)",
    "缩量回踩 10 日线": "C>MA(C,10) AND L<=MA(C,10)*1.01 AND V<MA(V,5)",
    "黄金坑": HJK,
}


def run_conditions(root: Path, panel) -> None:
    from rquant.factor.condition import KIND, list_conditions, screen
    from rquant.factor.condition import save as save_condition
    from rquant.factor.tdx import translate

    for old in list_conditions(root):  # keep only the latest run per condition
        if old.name in CONDITIONS:
            shutil.rmtree(root / KIND / old.run_id, ignore_errors=True)
    for name, tdx in CONDITIONS.items():
        try:
            expr = translate(tdx)
            save_condition(name, expr, tdx, screen(panel, expr), root=root)
        except Exception as exc:  # noqa: BLE001 - one bad formula must not stop the job
            log.warning("condition %s failed: %s", name, exc)


def run_paper(root: Path, db: Path, last: date, days: int = 60) -> None:
    import duckdb

    from rquant.backtest.strategy import resolve
    from rquant.research import paper

    con = duckdb.connect(str(db), read_only=True)
    try:
        names = dict(con.execute("SELECT ts_code, name FROM stock_basic").fetchall())
        accounts = []
        from rquant.research.tdx_strategy import TDX_STRATEGIES

        for slug in [s[0] for s in STRATEGIES] + [s[0] for s in TDX_STRATEGIES]:
            spec = resolve(slug, root).spec
            accounts.append(paper.simulate(con, slug, spec.preset, spec.config, last, days, names))
    finally:
        con.close()
    paper.save(root, accounts)


def prune_backtests(root: Path, keep: int = 5) -> None:
    """Keep the newest `keep` runs per strategy (and per preset for ad-hoc runs)."""
    from rquant.backtest.store import KIND, list_runs

    seen: dict[str, int] = {}
    for run in list_runs(root):  # newest first
        key = (run.strategy or run.preset).split("@")[0]
        seen[key] = seen.get(key, 0) + 1
        if seen[key] > keep:
            shutil.rmtree(root / KIND / run.run_id, ignore_errors=True)


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
