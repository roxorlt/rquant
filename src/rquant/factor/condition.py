"""Condition screen on the latest replica day (roadmap item 13).

    python -m rquant.factor screen --name 放量突破 --tdx "C>HHV(REF(H,1),20) AND V>MA(V,5)*2"
    python -m rquant.factor screen --name … --expr "close > ts_max(delay(high, 1), 20)"

Uses end-of-day bars from the read replica (intraday snapshots are not in the
replica). Result: ``$RQUANT_RESEARCH_ROOT/condition/<id>/result.json``.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
from pydantic import BaseModel

from rquant.backtest.store import research_root
from rquant.factor.expr import evaluate, validate
from rquant.factor.store import load_panel
from rquant.factor.tdx import translate

KIND = "condition"


class ConditionHit(BaseModel):
    code: str
    close: float | None
    pct_chg: float | None


class ConditionRun(BaseModel):
    run_id: str
    name: str
    source_formula: str | None
    expression: str
    trade_date: date | None
    created_at: datetime
    universe: int
    hits: list[ConditionHit]


def screen(panel: dict[str, pd.DataFrame], expression: str) -> tuple[date | None, int,
                                                                     list[ConditionHit]]:
    out = evaluate(expression, panel)
    if out.empty:
        return None, 0, []
    last = out.index.max()
    row = out.loc[last]
    close, pct = panel["close"].loc[last], panel["pct_chg"].loc[last]
    codes = sorted(row[row > 0.5].index)
    hits = [ConditionHit(code=c, close=None if pd.isna(close[c]) else float(close[c]),
                         pct_chg=None if pd.isna(pct[c]) else float(pct[c])) for c in codes]
    return last.date(), int(close.notna().sum()), hits


def save(name: str, expression: str, source_formula: str | None, result: tuple,
         root: Path | None = None) -> ConditionRun:
    trade_date, universe, hits = result
    run_id = hashlib.sha256(f"{expression}|{trade_date}".encode()).hexdigest()[:12]
    run = ConditionRun(run_id=run_id, name=name, source_formula=source_formula,
                       expression=expression, trade_date=trade_date,
                       created_at=datetime.now(UTC), universe=universe, hits=hits[:2000])
    folder = (root or research_root()) / KIND / run_id
    folder.mkdir(parents=True, exist_ok=True)
    tmp = folder / "result.json.tmp"
    tmp.write_text(run.model_dump_json())
    tmp.replace(folder / "result.json")
    return run


def list_conditions(root: Path | None = None) -> list[ConditionRun]:
    base = (root or research_root()) / KIND
    runs = [ConditionRun.model_validate_json(p.read_text()) for p in base.glob("*/result.json")]
    return sorted(runs, key=lambda r: r.created_at, reverse=True)


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog="python -m rquant.factor screen")
    p.add_argument("--db", type=Path, default=Path(os.environ.get("DUCKDB_READONLY_PATH", "")))
    p.add_argument("--name", required=True)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--expr")
    group.add_argument("--tdx")
    p.add_argument("--date", type=date.fromisoformat, default=date.today())
    a = p.parse_args(argv)
    expression = translate(a.tdx) if a.tdx else a.expr
    validate(expression)
    con = duckdb.connect(str(a.db), read_only=True)
    try:
        panel = load_panel(con, a.date - timedelta(days=1), a.date)
    finally:
        con.close()
    for key in panel:  # load_panel pads forward for forward returns; a screen must not look ahead
        panel[key] = panel[key].loc[: pd.Timestamp(a.date)]
    run = save(a.name, expression, a.tdx, screen(panel, expression))
    print(f"{run.run_id} {run.trade_date} hits={len(run.hits)}/{run.universe}")
    return 0
