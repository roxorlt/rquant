"""Auto-maintained pool 「竞价选股」 for the auction-ths strategy (rule 3 of the study).

Rule (``event_study/auction-ths.json`` rule 3, chosen on train win rate):
  T-1: closed at limit-up, not a one-price board (high > low), not ST/退, not BJ,
       封单/流通市值 (seal_ratio) <= SEAL_MAX
  T  : 09:25 auction gap vs T-1 close >= GAP_MIN; ranked by auction volume, top TOP_N.
       Stocks whose auction price is already the limit-up price (一字开盘) are KEPT (the
       #412 skip was removed); they carry ``limit_open: true`` and the page badges them.
No sentiment filter (optional in the study; not applied here).

Alongside the rule-3 picks the pool keeps ``ths_screen``: the raw 同花顺 screen the study
replicates (T-1 limit-up non-one-word, gap > 4%, no ST/BJ, by auction volume, no seal
filter), so the page can show where our refined rule deviates from the 同花顺 list.

Stages, both written to ``$RQUANT_RESEARCH_ROOT/pools/auction/YYYY-MM-DD.json``:
    python -m rquant.research.auction_pool candidates [--day D]   # after T-1 close / pre-open
    python -m rquant.research.auction_pool pick [--day D]          # 09:26:30, Tushare rt_k

The pick runs at 09:26:30 (not 09:25:30) so the 09:25 match has settled in rt_k everywhere.

The web 池子 page, the snapshot collector universe (top priority) and the surge push read it.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

log = logging.getLogger("rquant.auction_pool")
TZ = ZoneInfo("Asia/Shanghai")
POOL_NAME = "竞价选股"
PRESET = "auction-pick"
SEAL_MAX = float(os.environ.get("AUCTION_POOL_SEAL_MAX", "0.0094"))
GAP_MIN = float(os.environ.get("AUCTION_POOL_GAP_MIN", "0.0537"))
TOP_N = int(os.environ.get("AUCTION_POOL_TOP", "3"))
THS_GAP_MIN = 0.04  # the original 同花顺 screen: 竞价涨幅 > 4%
THS_TOP = 5
RULE_TEXT = (f"昨日涨停非一字 & 封单/流通市值≤{SEAL_MAX:.2%} & 非ST/退/北交所；"
             f"今日竞价涨幅≥{GAP_MIN:.2%}（含竞价涨停），按竞价量降序前{TOP_N}")


def research_root() -> Path:
    return Path(os.environ.get("RQUANT_RESEARCH_ROOT") or "data/research")


def pool_path(root: Path, day: date) -> Path:
    return root / "pools" / "auction" / f"{day:%Y-%m-%d}.json"


def read_pool(root: Path, day: date | None = None) -> dict[str, Any] | None:
    """Pool for `day`, or the newest one when day is None."""
    d = root / "pools" / "auction"
    if day is not None:
        p = pool_path(root, day)
    else:
        files = sorted(d.glob("20??-??-??.json")) if d.is_dir() else []
        if not files:
            return None
        p = files[-1]
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def priority_codes(root: Path, day: date) -> list[str]:
    """Today's picks first, then the remaining candidates (empty if no pool for `day`)."""
    pool = read_pool(root, day)
    if not pool:
        return []
    out = [p["ts_code"] for p in pool.get("picks") or []]
    out += [c["ts_code"] for c in pool.get("candidates") or [] if c["ts_code"] not in out]
    return out


def _write(root: Path, day: date, pool: dict[str, Any]) -> Path:
    p = pool_path(root, day)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(pool, ensure_ascii=False, indent=1, default=str), "utf-8")
    tmp.replace(p)
    return p


def _now() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")


# --------------------------------------------------------------------------- candidates
def filter_base(df: pd.DataFrame) -> pd.DataFrame:
    """同花顺 base screen (T-1 part). df: one row per T-1 limit-up stock with high, low,
    fd_amount, circ_mv (万元), is_st."""
    df = df.copy()
    df["seal_ratio"] = df["fd_amount"] / (df["circ_mv"] * 1e4)
    m = ((df["high"] > df["low"]) & ~df["is_st"].astype(bool)
         & ~df["ts_code"].str.endswith(".BJ"))
    return df[m].sort_values("seal_ratio")


def filter_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """Rule 3: the base screen plus 封单/流通市值 <= SEAL_MAX."""
    df = filter_base(df)
    return df[df["seal_ratio"] <= SEAL_MAX]


def limit_price(pre_close: float, ts_code: str) -> float:
    """Exchange limit-up price (ST names are filtered out upstream)."""
    pct = 0.2 if ts_code[:3] in ("300", "301", "688", "689") else 0.1
    return float(int(pre_close * (1 + pct) * 100 + 0.5 + 1e-9)) / 100


def build_candidates(root: Path, day: date) -> dict[str, Any]:
    import duckdb

    from rquant.research.pit import is_st

    icon = duckdb.connect(str(root / "intraday.duckdb"), read_only=True)
    try:
        prior = icon.execute("SELECT max(trade_date) FROM limit_list WHERE trade_date < ?",
                             [day]).fetchone()[0]
        ll = icon.execute("SELECT ts_code, limit_times, first_time, open_times, fd_amount, "
                          "float_mv, industry FROM limit_list WHERE trade_date = ? AND lim = 'U'",
                          [prior]).df()
    finally:
        icon.close()
    con = duckdb.connect(str(root / "market.duckdb"), read_only=True)
    try:
        bars = con.execute("SELECT ts_code, high, low, close FROM daily_bar WHERE trade_date = ?",
                           [prior]).df()
        mv = con.execute("SELECT ts_code, circ_mv FROM daily_basic WHERE trade_date = ?",
                         [prior]).df()
        names = con.execute("SELECT ts_code, name FROM stock_basic").df()
        df = ll.merge(bars, on="ts_code").merge(names, on="ts_code", how="left")
        df = df.merge(mv, on="ts_code", how="left")
        df["circ_mv"] = df["circ_mv"].fillna(df["float_mv"] / 1e4)
        days = pd.Series([pd.Timestamp(day)] * len(df))
        df["is_st"] = is_st(con, df["ts_code"], days).to_numpy() if len(df) else []
    finally:
        con.close()
    base = filter_base(df)
    rows = [{"ts_code": r.ts_code, "name": r.name, "y_close": float(r.close),
             "seal_ratio": round(float(r.seal_ratio), 6), "boards": int(r.limit_times or 1),
             "open_times": int(r.open_times or 0), "first_time": r.first_time,
             "industry": r.industry} for r in base.itertuples()]
    return {"pool": POOL_NAME, "date": f"{day:%Y-%m-%d}", "prior_date": str(prior),
            "rule": RULE_TEXT, "stage": "candidates", "limit_ups": int(len(ll)),
            "candidates": [r for r in rows if r["seal_ratio"] <= SEAL_MAX],
            "base": rows, "picks": [], "ths_screen": [], "updated_at": _now()}


# --------------------------------------------------------------------------- picks
def _auction_rows(cands: list[dict], quotes: pd.DataFrame, gap_min: float,
                  strict: bool = False) -> list[dict]:
    q = quotes.drop_duplicates("ts_code", keep="last").set_index("ts_code")
    rows = []
    for c in cands:
        if c["ts_code"] not in q.index:
            continue
        r = q.loc[c["ts_code"]]
        pre = float(r.get("pre_close") or 0) or c["y_close"]
        px, vol = float(r.get("price") or 0), float(r.get("volume") or 0)
        if px <= 0 or vol <= 0:
            continue
        gap = px / pre - 1
        if gap > gap_min if strict else gap >= gap_min:
            rows.append({**c, "auction_price": px, "gap": round(gap, 5), "auction_vol": vol,
                         "auction_amount": round(px * vol, 2),
                         "limit_open": px >= limit_price(pre, c["ts_code"]) - 1e-6})
    rows.sort(key=lambda x: -x["auction_vol"])
    return rows


def rank_picks(cands: list[dict], quotes: pd.DataFrame) -> list[dict]:
    """quotes: ts_code, price (auction price), pre_close, volume (auction volume).
    Rule 3 picks; 一字开盘 (auction at limit-up) are kept and flagged ``limit_open``."""
    rows = _auction_rows(cands, quotes, GAP_MIN)
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return rows[:TOP_N]


def ths_screen(base: list[dict], quotes: pd.DataFrame) -> list[dict]:
    """The raw 同花顺 screen (gap > 4%, by auction volume, 一字 kept but flagged)."""
    rows = _auction_rows(base, quotes, THS_GAP_MIN, strict=True)[:THS_TOP]
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return rows


def fetch_auction(codes: list[str]) -> pd.DataFrame:
    from rquant.research.snapshots import TS_K_FIELDS, norm_rt_k, tushare_call, tushare_token

    tok = tushare_token()
    if not tok:
        raise RuntimeError("no tushare token")
    frames = [norm_rt_k(tushare_call("rt_k", {"ts_code": ",".join(codes[i:i + 50])},
                                     TS_K_FIELDS, tok)) for i in range(0, len(codes), 50)]
    return pd.concat(frames, ignore_index=True)


def snapshot_quotes(root: Path, day: date, codes: list[str]) -> pd.DataFrame:
    """Fallback: the collector's own part files for today (latest row <= 09:30)."""
    parts = sorted((root / "snapshots" / f"{day:%Y-%m-%d}" / "quotes").glob("part-*.parquet"))
    frames = []
    for p in parts:
        try:
            f = pd.read_parquet(p, columns=["ts_code", "time", "price", "pre_close", "volume"])
        except Exception:  # noqa: BLE001
            continue
        frames.append(f[f["ts_code"].isin(codes) & (f["time"] < "09:30:00")])
    if not frames:
        return pd.DataFrame(columns=["ts_code", "price", "pre_close", "volume"])
    return pd.concat(frames).sort_values("time")


def make_picks(root: Path, day: date) -> dict[str, Any]:
    pool = read_pool(root, day) or build_candidates(root, day)
    cands = pool.get("candidates") or []
    base = pool.get("base") or cands
    codes = list(dict.fromkeys([c["ts_code"] for c in cands] + [c["ts_code"] for c in base]))
    src = "rt_k"
    quotes = pd.DataFrame()
    if codes:
        try:
            quotes = fetch_auction(codes)
        except Exception as exc:  # noqa: BLE001
            log.warning("rt_k failed (%s); using snapshot parts", str(exc)[:160])
        if not len(quotes):
            src, quotes = "snapshots", snapshot_quotes(root, day, codes)
    picks = rank_picks(cands, quotes) if codes else []
    ths = ths_screen(base, quotes) if codes else []
    pool.update(stage="final", picks=picks, ths_screen=ths, quote_source=src,
                picked_at=_now(), updated_at=_now())
    return pool


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(prog="python -m rquant.research.auction_pool")
    p.add_argument("cmd", choices=["candidates", "pick", "show"])
    p.add_argument("--day", type=date.fromisoformat, default=None)
    p.add_argument("--force", action="store_true", help="run on non-trading days too")
    a = p.parse_args(argv)
    root, day = research_root(), a.day or datetime.now(TZ).date()
    if a.cmd == "show":
        print(json.dumps(read_pool(root, day), ensure_ascii=False, indent=1))
        return 0
    if not a.force and a.day is None:
        from rquant.research.snapshots import is_trading_day

        if not is_trading_day(day):
            log.info("%s not a trading day; skip", day)
            return 0
    pool = build_candidates(root, day) if a.cmd == "candidates" else make_picks(root, day)
    path = _write(root, day, pool)
    log.info("%s %s: %d candidates, picks %s -> %s", a.cmd, day, len(pool["candidates"]),
             [(x["rank"], x["ts_code"], x["name"]) for x in pool["picks"]], path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
