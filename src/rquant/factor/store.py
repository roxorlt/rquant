"""Run a factor test from the read replica; keep the result as a file.

    python -m rquant.factor --name 动量5 --expr "ts_delta(close, 5) / delay(close, 5)" \\
        --start 2025-01-01 --end 2026-09-30 [--horizon 5]

Results: ``$RQUANT_RESEARCH_ROOT/factor/<id>/result.json``.
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
from rquant.factor.evaluation import FactorTestResult, factor_test
from rquant.factor.expr import FIELDS, evaluate, validate

KIND = "factor"
WARMUP_DAYS = 400  # calendar days, covers the 250-day max window


class FactorRun(BaseModel):
    factor_id: str
    name: str
    expression: str
    start: date
    end: date
    created_at: datetime
    result: FactorTestResult


def load_panel(con: duckdb.DuckDBPyConnection, start: date, end: date) -> dict[str, pd.DataFrame]:
    frame = con.execute(
        f"SELECT trade_date, ts_code, {', '.join(FIELDS)} FROM daily_bar "
        "WHERE trade_date BETWEEN ? AND ?",
        [start - timedelta(days=WARMUP_DAYS), end + timedelta(days=45)],
    ).df()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return {f: frame.pivot(index="trade_date", columns="ts_code", values=f).sort_index()
            for f in FIELDS}


def run_factor(panel: dict[str, pd.DataFrame], expression: str, start: date, end: date,
               horizon: int = 5) -> FactorTestResult:
    factor = evaluate(expression, panel)
    window = (factor.index >= pd.Timestamp(start)) & (factor.index <= pd.Timestamp(end))
    # forward returns may look past `end` (that is what a forward return is); the
    # factor itself is only scored inside the window
    return factor_test(factor.where(pd.Series(window, index=factor.index), axis=0)
                       .loc[factor.index >= pd.Timestamp(start)],
                       panel["close"].loc[factor.index >= pd.Timestamp(start)], horizon)


def save(name: str, expression: str, start: date, end: date, result: FactorTestResult,
         root: Path | None = None) -> FactorRun:
    factor_id = hashlib.sha256(
        f"{expression}|{start}|{end}|{result.horizon}".encode()).hexdigest()[:12]
    run = FactorRun(factor_id=factor_id, name=name, expression=expression, start=start,
                    end=end, created_at=datetime.now(UTC), result=result)
    folder = (root or research_root()) / KIND / factor_id
    folder.mkdir(parents=True, exist_ok=True)
    tmp = folder / "result.json.tmp"
    tmp.write_text(run.model_dump_json())
    tmp.replace(folder / "result.json")
    return run


def list_factors(root: Path | None = None) -> list[FactorRun]:
    base = (root or research_root()) / KIND
    runs = [FactorRun.model_validate_json(p.read_text()) for p in base.glob("*/result.json")]
    return sorted(runs, key=lambda r: r.created_at, reverse=True)


def read_factor(factor_id: str, root: Path | None = None) -> FactorRun | None:
    if not factor_id.isalnum():
        return None
    path = (root or research_root()) / KIND / factor_id / "result.json"
    return FactorRun.model_validate_json(path.read_text()) if path.is_file() else None


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m rquant.factor")
    p.add_argument("--db", type=Path, default=Path(os.environ.get("DUCKDB_READONLY_PATH", "")))
    p.add_argument("--name", required=True)
    p.add_argument("--expr", required=True)
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--horizon", type=int, default=5)
    a = p.parse_args(argv)
    validate(a.expr)
    if (a.end - a.start).days > 3 * 366:
        p.error("区间最长 3 年")
    con = duckdb.connect(str(a.db), read_only=True)
    try:
        panel = load_panel(con, a.start, a.end)
    finally:
        con.close()
    run = save(a.name, a.expr, a.start, a.end, run_factor(panel, a.expr, a.start, a.end,
                                                           a.horizon))
    print(f"{run.factor_id} days={run.result.days} IC={run.result.mean_ic}")
    return 0
