"""Run a factor test from the read replica; keep the result as a file.

    python -m rquant.factor --name 动量5 --expr "ts_delta(close, 5) / delay(close, 5)" \\
        --start 2025-01-01 --end 2026-09-30 [--horizon 5]

Results: ``$RQUANT_RESEARCH_ROOT/factor/<id>/result.json``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
from pydantic import BaseModel

from rquant.backtest.store import research_root
from rquant.factor.chips import CHIP_FIELDS, chip_fields
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


def load_panel(con: duckdb.DuckDBPyConnection, start: date, end: date,
               table: str = "daily_bar") -> dict[str, pd.DataFrame]:
    frame = con.execute(
        f"SELECT trade_date, ts_code, {', '.join(FIELDS)} FROM {table} "
        "WHERE trade_date BETWEEN ? AND ?",
        [start - timedelta(days=WARMUP_DAYS), end + timedelta(days=45)],
    ).df()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    panel = {f: frame.pivot(index="trade_date", columns="ts_code", values=f).sort_index()
             for f in FIELDS}
    turnover = _turnover(con, start, end)
    if turnover is not None:
        panel["turnover"] = turnover.reindex_like(panel["close"])
    panel.update(_moneyflow(con, start, end, panel["close"], panel["amount"]))
    return panel


def _moneyflow(con: duckdb.DuckDBPyConnection, start: date, end: date, like: pd.DataFrame,
               amount: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """mf_io = active-sell / active-buy amount (>1: 内盘>外盘); mf_lg = (large + extra-large)
    net buy / turnover amount. Empty dict when the moneyflow table is absent."""
    try:
        frame = con.execute(
            "SELECT trade_date, ts_code,"
            " sell_sm_amount+sell_md_amount+sell_lg_amount+sell_elg_amount AS s,"
            " buy_sm_amount+buy_md_amount+buy_lg_amount+buy_elg_amount AS b,"
            " buy_lg_amount+buy_elg_amount-sell_lg_amount-sell_elg_amount AS lg"
            " FROM moneyflow WHERE trade_date BETWEEN ? AND ?",
            [start - timedelta(days=WARMUP_DAYS), end + timedelta(days=45)]).df()
    except duckdb.Error:
        return {}
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    piv = {c: frame.pivot(index="trade_date", columns="ts_code", values=c).reindex_like(like)
           for c in ("s", "b", "lg")}
    # moneyflow amounts are 万元, daily bar amount is 千元
    return {"mf_io": piv["s"] / piv["b"].where(piv["b"] > 0),
            "mf_lg": piv["lg"] / (amount / 10).where(amount > 0)}


def _turnover(con: duckdb.DuckDBPyConnection, start: date, end: date) -> pd.DataFrame | None:
    """Free-float turnover (%), total-share turnover where the former is missing."""
    sql = ("SELECT trade_date, ts_code, {t} AS v FROM daily_basic WHERE trade_date BETWEEN ? AND ?")
    span = [start - timedelta(days=WARMUP_DAYS), end + timedelta(days=45)]
    for t in ("COALESCE(turnover_rate_f, turnover_rate)", "turnover_rate"):
        try:
            frame = con.execute(sql.format(t=t), span).df()
            break
        except duckdb.Error:
            continue
    else:
        return None
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame.pivot(index="trade_date", columns="ts_code", values="v").sort_index()


def with_chips(panel: dict[str, pd.DataFrame], expression: str) -> dict[str, pd.DataFrame]:
    """Compute the chip fields once per panel, only when an expression uses them."""
    if "chip_" in expression and not any(f in panel for f in CHIP_FIELDS):
        panel.update(chip_fields(panel))
    return panel


def run_factor(panel: dict[str, pd.DataFrame], expression: str, start: date, end: date,
               horizon: int = 5) -> FactorTestResult:
    factor = evaluate(expression, with_chips(panel, expression))
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


class TrackingPoint(BaseModel):
    date: date
    ic: float
    rolling: float | None


class FactorTracking(BaseModel):
    factor_id: str
    updated_at: datetime
    window_start: date
    window_end: date
    latest_date: date | None
    recent_ic: float | None        # mean IC over the last 20 scored days
    research_ic: float | None      # the stored test's mean IC, for comparison
    points: list[TrackingPoint]


def track(run: FactorRun, panel: dict[str, pd.DataFrame], end: date,
          lookback_days: int = 180) -> FactorTracking:
    start = end - timedelta(days=lookback_days)
    result = run_factor(panel, run.expression, start, end, run.result.horizon)
    ic = pd.Series({p.date: p.ic for p in result.ic_series}, dtype="float64")
    rolling = ic.rolling(20, min_periods=5).mean()
    return FactorTracking(
        factor_id=run.factor_id, updated_at=datetime.now(UTC), window_start=start,
        window_end=end, latest_date=ic.index.max() if len(ic) else None,
        recent_ic=float(ic.tail(20).mean()) if len(ic) else None,
        research_ic=run.result.mean_ic,
        points=[TrackingPoint(date=d, ic=float(v),
                              rolling=None if pd.isna(rolling[d]) else float(rolling[d]))
                for d, v in ic.items()],
    )


def save_tracking(tracking: FactorTracking, root: Path | None = None) -> None:
    folder = (root or research_root()) / KIND / tracking.factor_id
    tmp = folder / "tracking.json.tmp"
    tmp.write_text(tracking.model_dump_json())
    tmp.replace(folder / "tracking.json")


def read_tracking(factor_id: str, root: Path | None = None) -> FactorTracking | None:
    if not factor_id.isalnum():
        return None
    path = (root or research_root()) / KIND / factor_id / "tracking.json"
    return FactorTracking.model_validate_json(path.read_text()) if path.is_file() else None


def track_all(database: Path, end: date, root: Path | None = None) -> int:
    runs = list_factors(root)
    if not runs:
        return 0
    con = duckdb.connect(str(database), read_only=True)
    try:
        panel = load_panel(con, end - timedelta(days=180), end)
    finally:
        con.close()
    for run in runs:
        save_tracking(track(run, panel, end), root)
    return len(runs)


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else __import__("sys").argv[1:])
    if args[:1] == ["screen"]:
        from rquant.factor.condition import main as screen_main

        return screen_main(args[1:])
    if args[:1] == ["track"]:
        p = argparse.ArgumentParser(prog="python -m rquant.factor track")
        p.add_argument("--db", type=Path,
                       default=Path(os.environ.get("DUCKDB_READONLY_PATH", "")))
        p.add_argument("--end", type=date.fromisoformat, default=date.today())
        a = p.parse_args(args[1:])
        print(f"tracked {track_all(a.db, a.end)} factors")
        return 0
    return _test_main(args)


def _test_main(argv: list[str]) -> int:
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


def save_latest(run: FactorRun, panel: dict[str, pd.DataFrame], root: Path | None = None) -> None:
    """Cross-sectional percentile (0-100) of the factor on the last panel day, per code,
    so the screener can filter / rank on factor values without recomputing them."""
    factor = evaluate(run.expression, with_chips(panel, run.expression))
    if factor.empty:
        return
    last = factor.index[-1]
    pct = factor.loc[last].dropna().rank(pct=True) * 100
    doc = {"date": str(last.date() if hasattr(last, "date") else last),
           "values": {str(k): round(float(v), 2) for k, v in pct.items()}}
    folder = (root or research_root()) / KIND / run.factor_id
    folder.mkdir(parents=True, exist_ok=True)
    tmp = folder / "latest.json.tmp"
    tmp.write_text(json.dumps(doc))
    tmp.replace(folder / "latest.json")


def read_latest(root: Path | None = None) -> dict[str, dict[str, float]]:
    """factor name -> {code: percentile} for every factor that has a latest.json."""
    out: dict[str, dict[str, float]] = {}
    for run in list_factors(root):
        p = (root or research_root()) / KIND / run.factor_id / "latest.json"
        if p.exists():
            try:
                out[run.name] = json.loads(p.read_text())["values"]
            except (ValueError, KeyError):
                continue
    return out
