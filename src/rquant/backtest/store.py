"""Run a portfolio backtest from the read replica and keep the result as a file.

Results live under ``$RQUANT_RESEARCH_ROOT/portfolio_backtest/<run_id>/result.json``
(default ``data/research``). The web API only reads these files.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
from pydantic import BaseModel

from rquant.backtest.engine import BacktestConfig, BacktestResult, Bar, run_backtest
from rquant.portfolio import PortfolioCandidate

KIND = "portfolio_backtest"


class StoredRun(BaseModel):
    run_id: str
    created_at: datetime
    title: str
    preset: str
    start: date
    end: date
    result: BacktestResult
    #: ts_code → industry (stock_basic) for the codes that were candidates
    industries: dict[str, str] = {}
    #: candidate codes of the last signal day (the pool the portfolio picked from)
    pool_last: list[str] = []
    #: "slug@version" when the run was made from a saved strategy spec
    strategy: str | None = None


def research_root() -> Path:
    return Path(os.environ.get("RQUANT_RESEARCH_ROOT") or "data/research")


def load_inputs(
    con: duckdb.DuckDBPyConnection, preset: str, start: date, end: date
) -> tuple[list[date], dict[date, dict[str, Bar]], dict[date, list[PortfolioCandidate]]]:
    signal_rows = con.execute(
        "SELECT trade_date, ts_code, extra FROM screen_result "
        "WHERE preset_name = ? AND trade_date BETWEEN ? AND ? ORDER BY trade_date, ts_code",
        [preset, start, end],
    ).fetchall()
    signals: dict[date, list[PortfolioCandidate]] = defaultdict(list)
    for day, code, extra in signal_rows:
        try:
            score = float(json.loads(extra or "{}").get("score", 0))
        except (ValueError, AttributeError):
            score = 0.0
        signals[day].append(PortfolioCandidate(ts_code=code, rank_score=max(score, 0)))
    codes = sorted({c.ts_code for items in signals.values() for c in items})
    days = [r[0] for r in con.execute(
        "SELECT DISTINCT trade_date FROM daily_bar WHERE trade_date BETWEEN ? AND ? "
        "ORDER BY trade_date", [start, end]).fetchall()]
    bars: dict[date, dict[str, Bar]] = defaultdict(dict)
    if codes:
        marks = ",".join("?" * len(codes))
        for day, code, o, c, pre, pct in con.execute(
            f"SELECT b.trade_date, b.ts_code, b.open, b.close, b.pre_close, "
            f"COALESCE(s.limit_pct, 0.10) FROM daily_bar b LEFT JOIN daily_state s "
            f"ON s.ts_code = b.ts_code AND s.trade_date = b.trade_date "
            f"WHERE b.ts_code IN ({marks}) AND b.trade_date BETWEEN ? AND ? "
            f"AND b.open IS NOT NULL AND b.close IS NOT NULL AND b.pre_close IS NOT NULL",
            [*codes, start, end],
        ).fetchall():
            bars[day][code] = Bar(open=o, close=c, pre_close=pre, limit_pct=pct)
    return days, dict(bars), dict(signals)


def load_industries(con: duckdb.DuckDBPyConnection, codes: list[str]) -> dict[str, str]:
    if not codes:
        return {}
    try:
        rows = con.execute(
            f"SELECT ts_code, COALESCE(NULLIF(industry, ''), '未分类') FROM stock_basic "
            f"WHERE ts_code IN ({','.join('?' * len(codes))})", codes).fetchall()
    except duckdb.CatalogException:  # replica without stock_basic
        return {}
    return {str(code): str(industry) for code, industry in rows}


def run_and_save(
    database: Path, preset: str, start: date, end: date, config: BacktestConfig,
    *, root: Path | None = None, title: str | None = None, strategy: str | None = None,
) -> StoredRun:
    con = duckdb.connect(str(database), read_only=True)
    try:
        days, bars, signals = load_inputs(con, preset, start, end)
        industries = load_industries(
            con, sorted({c.ts_code for items in signals.values() for c in items}))
    finally:
        con.close()
    result = run_backtest(days, bars, signals, config)
    pool = [c.ts_code for c in signals[max(signals)]] if signals else []
    return save(result, preset=preset, start=start, end=end, root=root, title=title,
                industries=industries, pool_last=pool, strategy=strategy)


def save(result: BacktestResult, *, preset: str, start: date, end: date,
         root: Path | None = None, title: str | None = None,
         industries: dict[str, str] | None = None,
         pool_last: list[str] | None = None, strategy: str | None = None) -> StoredRun:
    params = json.dumps([preset, str(start), str(end), result.config.model_dump(mode="json"),
                         strategy], sort_keys=True)
    run_id = hashlib.sha256(params.encode()).hexdigest()[:12]
    run = StoredRun(run_id=run_id, created_at=datetime.now(UTC),
                    title=title or f"{preset} {start}~{end}", preset=preset,
                    start=start, end=end, result=result, industries=industries or {},
                    pool_last=pool_last or [], strategy=strategy)
    folder = (root or research_root()) / KIND / run_id
    folder.mkdir(parents=True, exist_ok=True)
    tmp = folder / "result.json.tmp"
    tmp.write_text(run.model_dump_json())
    tmp.replace(folder / "result.json")
    return run


def list_runs(root: Path | None = None) -> list[StoredRun]:
    base = (root or research_root()) / KIND
    if not base.is_dir():
        return []
    runs = [StoredRun.model_validate_json(p.read_text()) for p in base.glob("*/result.json")]
    return sorted(runs, key=lambda r: r.created_at, reverse=True)


def read_run(run_id: str, root: Path | None = None) -> StoredRun | None:
    if not run_id.isalnum():
        return None
    path = (root or research_root()) / KIND / run_id / "result.json"
    return StoredRun.model_validate_json(path.read_text()) if path.is_file() else None


def _main(argv: list[str] | None = None) -> int:
    import argparse
    import sys
    from decimal import Decimal

    from rquant.portfolio import PortfolioWeightRule

    args = list(argv if argv is not None else sys.argv[1:])
    if args[:2] == ["strategy", "save"]:
        return _strategy_save(args[2:])
    p = argparse.ArgumentParser(prog="python -m rquant.backtest")
    p.add_argument("--db", type=Path, default=Path(os.environ.get("DUCKDB_READONLY_PATH", "")))
    p.add_argument("--strategy", help="slug or slug@N; overrides preset/weights/rebalance")
    p.add_argument("--preset")
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--max-positions", type=int, default=10)
    p.add_argument("--method", choices=["equal", "rank_score"], default="equal")
    p.add_argument("--max-stock-weight", type=Decimal, default=Decimal("1"))
    p.add_argument("--rebalance-every", type=int, default=1)
    p.add_argument("--capital", type=float, default=1_000_000)
    a = p.parse_args(args)
    strategy_ref = None
    if a.strategy:
        from rquant.backtest.strategy import resolve

        version = resolve(a.strategy)
        preset, config = version.spec.preset, version.spec.config
        strategy_ref = f"{version.spec.slug}@{version.version}"
    elif a.preset:
        preset = a.preset
        config = BacktestConfig(
            capital=a.capital, rebalance_every=a.rebalance_every,
            weights=PortfolioWeightRule(method=a.method, max_positions=a.max_positions,
                                        max_stock_weight=a.max_stock_weight),
        )
    else:
        p.error("--preset or --strategy is required")
    run = run_and_save(a.db, preset, a.start, a.end, config, strategy=strategy_ref,
                       title=f"{strategy_ref} {a.start}~{a.end}" if strategy_ref else None)
    last: Any = run.result.days[-1].nav if run.result.days else None
    print(f"{run.run_id} days={len(run.result.days)} orders={len(run.result.orders)} nav={last}")
    return 0


def _strategy_save(argv: list[str]) -> int:
    import argparse
    from decimal import Decimal

    from rquant.backtest.strategy import StrategySpec, save_version
    from rquant.portfolio import PortfolioWeightRule

    p = argparse.ArgumentParser(prog="python -m rquant.backtest strategy save")
    p.add_argument("--slug", required=True)
    p.add_argument("--title", required=True)
    p.add_argument("--preset", required=True)
    p.add_argument("--max-positions", type=int, default=10)
    p.add_argument("--method", choices=["equal", "rank_score"], default="equal")
    p.add_argument("--max-stock-weight", type=Decimal, default=Decimal("1"))
    p.add_argument("--max-industry-weight", type=Decimal)
    p.add_argument("--cash-reserve", type=Decimal, default=Decimal("0"))
    p.add_argument("--rebalance-every", type=int, default=1)
    p.add_argument("--capital", type=float, default=1_000_000)
    p.add_argument("--note", default="")
    a = p.parse_args(argv)
    spec = StrategySpec(
        slug=a.slug, title=a.title, preset=a.preset, note=a.note,
        config=BacktestConfig(
            capital=a.capital, rebalance_every=a.rebalance_every,
            weights=PortfolioWeightRule(
                method=a.method, max_positions=a.max_positions,
                max_stock_weight=a.max_stock_weight,
                max_industry_weight=a.max_industry_weight, cash_reserve=a.cash_reserve),
        ),
    )
    version = save_version(spec)
    print(f"{spec.slug}@{version.version} {version.content_hash}")
    return 0
