# ruff: noqa: E501
"""Daily call-auction (09:15-09:25) + post-open absorption factors from L1 snapshots.

Input: ``snapshots/YYYY-MM-DD/quotes.parquet`` (or parts) written by the collector, which
polls from 09:15 so the auction's indicative matched price/volume and book are captured.
Output: ``snapshots/YYYY-MM-DD/auction.parquet`` one row per stock (all A seen in snapshots):

  auc_price/auc_vol/auc_amount   last matched price/vol/amount at or before 09:25:59
  gap                            auc_price / pre_close - 1 (open gap)
  p20, v20, p_range_15_20, p_range_20_25, v_20_25_share
                                 path: price/volume at 09:20 (cancellable phase end) and ranges
  auc_vol_prev1, auc_vol_avg5, auc_vol_vs_prev1, auc_vol_vs_avg5
                                 vs previous days' auction.parquet (if present)
  circ_mv, auc_amount_to_float   auction amount / float mv (daily_basic.circ_mv, 万元 -> CNY)
  ask5_0925, ask5_to_auc_vol, ask5_to_float_pct
                                 09:25 ask-side pressure (sum ask1-5 qty, rt_tick rows)
  for W in 5/15/30 min after 09:30:
    buy_delta_W      tick-rule signed volume (up-tick +, down-tick -) / window volume
    above_auc_W      share of snapshots with price >= auc_price
    above_vwap_W     share of snapshots with price >= running VWAP
    ask_consumed_W   window up-tick volume / ask5_0925 (ask wall eaten)
    ask_withdrawn_W  max(0, ask5_0925 - ask5_end - uptick vol) / ask5_0925
    dip_vol_W        share of window volume traded on down-ticks
    ret_W            close of window / auc_price - 1
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

log = logging.getLogger("rquant.auction")
ASK5 = "coalesce(a1_v,0)+coalesce(a2_v,0)+coalesce(a3_v,0)+coalesce(a4_v,0)+coalesce(a5_v,0)"


def _files(d: Path) -> list[str]:
    q = d / "quotes.parquet"
    return [str(q)] if q.is_file() else [str(p) for p in sorted((d / "quotes").glob("part-*.parquet"))]


def compute(d: Path, market_db: Path | None = None) -> pd.DataFrame:
    import duckdb

    files = _files(d)
    if not files:
        return pd.DataFrame()
    con = duckdb.connect()
    con.execute("SET memory_limit='150MB'; SET threads=1; SET enable_progress_bar=false")
    lst = "[" + ",".join(f"'{f}'" for f in files) + "]"
    con.execute(f"""CREATE TEMP TABLE q AS SELECT ts_code, any_value(name) AS name, time,
        max(price) AS price, max(pre_close) AS pre_close, max(volume) AS volume, max(amount) AS amount,
        max(CASE WHEN a5_p IS NOT NULL OR a2_p IS NOT NULL THEN {ASK5} END) AS ask5
      FROM read_parquet({lst}) WHERE time BETWEEN '09:15:00' AND '10:00:59' AND price > 0
      GROUP BY ts_code, time""")
    auc = con.execute("""
      WITH a AS (SELECT * FROM q WHERE time <= '09:25:59'),
      last AS (SELECT ts_code, arg_max(price,time) AS auc_price, arg_max(volume,time) AS auc_vol,
                 arg_max(amount,time) AS auc_amount, max(pre_close) AS pre_close, any_value(name) AS name
               FROM a GROUP BY 1),
      p20 AS (SELECT ts_code, arg_max(price,time) AS p20, arg_max(volume,time) AS v20,
                max(price)-min(price) AS r1 FROM a WHERE time < '09:20:00' GROUP BY 1),
      p25 AS (SELECT ts_code, max(price)-min(price) AS r2 FROM a WHERE time >= '09:20:00' GROUP BY 1),
      ask AS (SELECT ts_code, arg_max(ask5,time) AS ask5_0925 FROM q
              WHERE time BETWEEN '09:25:00' AND '09:29:59' AND ask5 IS NOT NULL GROUP BY 1)
      SELECT last.*, p20.p20, p20.v20, p20.r1/last.pre_close p_range_15_20,
             p25.r2/last.pre_close p_range_20_25, ask.ask5_0925
      FROM last LEFT JOIN p20 USING(ts_code) LEFT JOIN p25 USING(ts_code)
      LEFT JOIN ask USING(ts_code)""").df()
    if not len(auc):
        return auc
    auc["gap"] = auc["auc_price"] / auc["pre_close"] - 1
    auc["v_20_25_share"] = 1 - auc["v20"].fillna(0) / auc["auc_vol"].where(auc["auc_vol"] > 0)
    auc["ask5_to_auc_vol"] = auc["ask5_0925"] / auc["auc_vol"].where(auc["auc_vol"] > 0)
    con.register("auc", auc[["ts_code", "auc_price", "auc_vol", "auc_amount", "ask5_0925"]])
    for w in (5, 15, 30):
        end = f"{9 + (30 + w) // 60:02d}:{(30 + w) % 60:02d}:59"
        f = con.execute(f"""
          WITH s AS (SELECT q.ts_code, q.time, q.price, q.volume, q.amount, q.ask5,
                 q.volume - lag(q.volume, 1, auc.auc_vol) OVER w dv,
                 q.price - lag(q.price, 1, auc.auc_price) OVER w dp,
                 auc.auc_price, auc.ask5_0925
               FROM q JOIN auc USING(ts_code)
               WHERE q.time BETWEEN '09:30:00' AND '{end}'
               WINDOW w AS (PARTITION BY q.ts_code ORDER BY q.time))
          SELECT ts_code,
            sum(CASE WHEN dp>0 THEN dv WHEN dp<0 THEN -dv ELSE 0 END)/nullif(sum(dv),0) AS buy_delta,
            avg(CASE WHEN price >= auc_price THEN 1 ELSE 0 END) AS above_auc,
            avg(CASE WHEN volume>0 AND price >= amount/volume*0.999 AND amount/volume <
                     price*1.5 THEN 1 ELSE 0 END) AS above_vwap,
            sum(CASE WHEN dp>0 THEN dv ELSE 0 END)/nullif(any_value(ask5_0925),0) AS ask_consumed,
            greatest(0, any_value(ask5_0925) - arg_max(ask5,time) - sum(CASE WHEN dp>0 THEN dv ELSE 0 END))
              /nullif(any_value(ask5_0925),0) AS ask_withdrawn,
            sum(CASE WHEN dp<0 THEN dv ELSE 0 END)/nullif(sum(dv),0) AS dip_vol,
            arg_max(price,time)/any_value(auc_price)-1 ret
          FROM s WHERE dv >= 0 GROUP BY 1""").df()
        auc = auc.merge(f.rename(columns={c: f"{c}_{w}" for c in f.columns if c != "ts_code"}),
                        on="ts_code", how="left")
    con.close()
    if market_db is not None and market_db.is_file():
        try:
            m = duckdb.connect(str(market_db), read_only=True)
            mv = m.execute("SELECT ts_code, circ_mv FROM daily_basic WHERE trade_date = "
                           "(SELECT max(trade_date) FROM daily_basic WHERE trade_date < ?)",
                           [date.fromisoformat(d.name)]).df()
            m.close()
            auc = auc.merge(mv, on="ts_code", how="left")
            float_cny = auc["circ_mv"] * 1e4
            auc["auc_amount_to_float"] = auc["auc_amount"] / float_cny
            auc["ask5_to_float_pct"] = auc["ask5_0925"] * auc["auc_price"] / float_cny * 100
        except Exception as exc:  # noqa: BLE001
            log.warning("auction: circ_mv unavailable: %s", str(exc)[:160])
    prev = sorted(p for p in d.parent.glob("20??-??-??/auction.parquet") if p.parent.name < d.name)[-5:]
    if prev:
        hist = pd.concat([pd.read_parquet(p, columns=["ts_code", "auc_vol"]).assign(day=p.parent.name)
                          for p in prev])
        last_day = hist["day"].max()
        p1 = hist[hist.day == last_day].set_index("ts_code")["auc_vol"]
        avg5 = hist.groupby("ts_code")["auc_vol"].mean()
        auc["auc_vol_prev1"] = auc["ts_code"].map(p1)
        auc["auc_vol_avg5"] = auc["ts_code"].map(avg5)
        auc["auc_vol_vs_prev1"] = auc["auc_vol"] / auc["auc_vol_prev1"]
        auc["auc_vol_vs_avg5"] = auc["auc_vol"] / auc["auc_vol_avg5"]
    auc.insert(0, "date", d.name)
    return auc


def write_auction_factors(snapshots: Path, day: date) -> Path | None:
    d = snapshots / f"{day:%Y-%m-%d}"
    df = compute(d, snapshots.parent / "market.duckdb")
    if not len(df):
        log.info("auction: no auction rows in %s", d)
        return None
    p = d / "auction.parquet"
    df.to_parquet(p, index=False)
    log.info("auction factors: %d stocks -> %s", len(df), p)
    return p


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    root = Path(sys.argv[1])
    print(write_auction_factors(root.parent, date.fromisoformat(root.name)))
