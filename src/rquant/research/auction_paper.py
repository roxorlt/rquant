"""Paper ledger for the 竞价选股 watchlist: every pool day, a simulated buy of each
buyable watchlist stock at the open (= auction price; 竞价涨停 opens skipped as
unbuyable), exits at T+1 open and T+1 close, after COST (auction_bt). Unadjusted prices
(one-night hold; an ex-date in between is rare and accepted).

    python -m rquant.research.auction_paper [--rebuild]   # after close; idempotent

Ledger: ``$RQUANT_RESEARCH_ROOT/paper/auction_watch.jsonl`` (one line per trade).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from rquant.research.auction_bt import COST, summary

log = logging.getLogger("rquant.auction_paper")


def ledger_path(root: Path) -> Path:
    return Path(root) / "paper" / "auction_watch.jsonl"


def read_ledger(root: Path) -> list[dict]:
    p = ledger_path(root)
    if not p.exists():
        return []
    return [json.loads(x) for x in p.read_text("utf-8").splitlines() if x.strip()]


def paper_days(root: Path) -> int:
    return len({r["date"] for r in read_ledger(root)})


def trades_for_pool(pool: dict, bars: pd.DataFrame) -> list[dict] | None:
    """bars: ts_code, trade_date (str), open, close for the pool day and later days.
    None when T+1 is not available yet."""
    day = pool["date"]
    later = sorted(d for d in bars["trade_date"].unique() if d > day)
    if not later:
        return None
    nxt = later[0]
    b = bars.set_index(["ts_code", "trade_date"])
    out = []
    for p in pool.get("picks") or []:
        if p.get("limit_open"):
            continue  # 竞价涨停·买不到
        code = p["ts_code"]
        try:
            entry = float(b.loc[(code, day), "open"]) or float(p["auction_price"])
            n_open, n_close = (float(b.loc[(code, nxt), c]) for c in ("open", "close"))
        except KeyError:
            continue  # suspended / no bar
        out.append({"date": day, "exit_date": nxt, "ts_code": code, "name": p.get("name"),
                    "rank": p.get("rank"), "entry": entry,
                    "r_n_open": round(n_open / entry - 1 - COST, 5),
                    "r_n_close": round(n_close / entry - 1 - COST, 5)})
    return out


def update(root: Path, rebuild: bool = False) -> int:
    import duckdb

    from rquant.research.auction_pool import read_pool

    done = set() if rebuild else {r["date"] for r in read_ledger(root)}
    pdir = Path(root) / "pools" / "auction"
    pools = [read_pool(root, pd.Timestamp(f.stem).date())
             for f in sorted(pdir.glob("20??-??-??.json"))]
    # only watchlist-mode pools (the old rule-3 picks are a different strategy)
    pools = [p for p in pools if p and p.get("kind") == "watchlist"
             and p.get("stage") == "final" and p["date"] not in done]
    if not pools:
        return 0
    con = duckdb.connect(str(Path(root) / "market.duckdb"), read_only=True)
    try:
        codes = sorted({x["ts_code"] for p in pools for x in p.get("picks") or []})
        bars = con.execute(
            "SELECT ts_code, strftime(trade_date, '%Y-%m-%d') AS trade_date, open, close "
            "FROM daily_bar WHERE trade_date >= ? AND ts_code IN (SELECT unnest(?))",
            [min(p["date"] for p in pools), codes]).df() if codes else pd.DataFrame(
                columns=["ts_code", "trade_date", "open", "close"])
    finally:
        con.close()
    new = []
    for p in pools:
        t = trades_for_pool(p, bars)
        if t is None:
            continue
        new += t or [{"date": p["date"], "ts_code": None, "note": "no buyable pick"}]
    if rebuild:
        rows = new
        ledger_path(root).parent.mkdir(parents=True, exist_ok=True)
        ledger_path(root).write_text("".join(json.dumps(r, ensure_ascii=False) + "\n"
                                             for r in rows), "utf-8")
    elif new:
        ledger_path(root).parent.mkdir(parents=True, exist_ok=True)
        with ledger_path(root).open("a", encoding="utf-8") as f:
            f.writelines(json.dumps(r, ensure_ascii=False) + "\n" for r in new)
    return len(new)


def stats(root: Path) -> dict:
    rows = [r for r in read_ledger(root) if r.get("ts_code")]
    df = pd.DataFrame(rows)
    out = {"days": paper_days(root), "trades": len(rows)}
    for c in ("r_n_open", "r_n_close"):
        out[c] = summary(df[c]) if len(df) else summary(pd.Series(dtype=float))
    return out


def stats_text(root: Path) -> str:
    s = stats(root)
    if not s["days"]:
        return "模拟账本：尚无记录"
    o, c = s["r_n_open"], s["r_n_close"]
    return (f"模拟账本（扣费{COST:.2%}，跳过竞价涨停）：{s['days']} 天 {s['trades']} 笔；"
            f"次开卖 均{o['mean']:+.2%} 胜{o['win_rate']:.0%}；"
            f"次收卖 均{c['mean']:+.2%} 胜{c['win_rate']:.0%}")


def main(argv: list[str] | None = None) -> int:
    from rquant.research.auction_pool import research_root

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(prog="python -m rquant.research.auction_paper")
    ap.add_argument("--rebuild", action="store_true")
    a = ap.parse_args(argv)
    root = research_root()
    n = update(root, a.rebuild)
    log.info("paper ledger: +%d rows; %s", n, stats_text(root))
    return 0


if __name__ == "__main__":
    sys.exit(main())
