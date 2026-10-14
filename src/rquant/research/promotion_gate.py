"""Promotion gate: a strategy may enter a pool / push only when all of these hold.

1. after-cost mean per trade > 0 on the TEST period, and its 95% CI lower bound > 0
2. rolling monthly walk-forward over the test period: >= 70% of months positive
3. fixed train/test split date (``studies.SPLIT_DATE``), never a moving fraction
4. >= 20 trading days of paper record (``auction_paper`` ledger)

    python -m rquant.research.promotion_gate [--strategy auction-ths] [--exit r_n_open]

Writes ``$RQUANT_RESEARCH_ROOT/gates/<strategy>.json``; ``gate_passed()`` reads it.
Exit code 0 = pass, 1 = fail.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from rquant.research.auction_bt import net_trades, select_top, summary

log = logging.getLogger("rquant.promotion_gate")
MIN_MONTH_POS = 0.70
MIN_PAPER_DAYS = 20


def evaluate(trades: pd.DataFrame, ret_col: str, split: pd.Timestamp,
             paper_days: int) -> dict:
    """trades: one row per buyable trade, columns trade_date + ret_col (after cost)."""
    t = trades[pd.to_datetime(trades["trade_date"]) >= split]
    s = summary(t[ret_col])
    months = t.groupby(pd.to_datetime(t["trade_date"]).dt.to_period("M"))[ret_col].mean()
    month_pos = float((months > 0).mean()) if len(months) else 0.0
    checks = {
        "mean_after_cost_gt_0": s["mean"] > 0,
        "ci95_low_gt_0": s["ci_low"] > 0,
        "months_positive_ge_70pct": month_pos >= MIN_MONTH_POS,
        "fixed_split": True,
        "paper_days_ge_20": paper_days >= MIN_PAPER_DAYS,
    }
    return {"passed": all(checks.values()), "checks": checks, "split": str(split.date()),
            "test": s, "months": int(len(months)), "months_positive": month_pos,
            "monthly": {str(k): round(float(v), 5) for k, v in months.items()},
            "paper_days": paper_days, "ret_col": ret_col}


def gate_path(root: Path, strategy: str) -> Path:
    return Path(root) / "gates" / f"{strategy}.json"


def gate_passed(root: Path, strategy: str) -> bool:
    try:
        return bool(json.loads(gate_path(root, strategy).read_text("utf-8"))["passed"])
    except (OSError, ValueError, KeyError):
        return False


def run_auction(root: Path, exit_col: str = "r_n_open") -> dict:
    from rquant.research import auction_paper, studies

    df = studies.auction_frame(root)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    trades = net_trades(select_top(df, "vol_x"))
    res = evaluate(trades, f"{exit_col}_net", studies.SPLIT_DATE,
                   auction_paper.paper_days(root))
    res |= {"strategy": "auction-ths", "evaluated_at": datetime.now(UTC).isoformat()}
    p = gate_path(root, "auction-ths")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str), "utf-8")
    return res


def main(argv: list[str] | None = None) -> int:
    from rquant.research.auction_pool import research_root

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(prog="python -m rquant.research.promotion_gate")
    p.add_argument("--strategy", default="auction-ths", choices=["auction-ths"])
    p.add_argument("--exit", default="r_n_open", choices=["r_n_open", "r_n_close"])
    a = p.parse_args(argv)
    res = run_auction(research_root(), a.exit)
    print(json.dumps({k: res[k] for k in ("passed", "checks", "test", "months_positive",
                                          "paper_days")}, ensure_ascii=False, default=str))
    return 0 if res["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
