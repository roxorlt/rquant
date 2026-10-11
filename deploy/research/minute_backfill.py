"""Resumable 5-min backfill for all A-shares into market.duckdb.minute_bar.
Fetch -> per-stock parquet in staging/ (done marker = file) -> periodic short merge into
market.duckdb (one INSERT OR REPLACE from parquet, retried while locked)."""
import os, sys, time, glob, logging, shutil
from datetime import date, timedelta
from pathlib import Path
import duckdb, pandas as pd, tushare as ts

ROOT = Path(os.environ.get("RQUANT_RESEARCH_ROOT", "/home/lighthouse/rquant-research"))
DB = ROOT / "market.duckdb"
W = ROOT / "minute_backfill"; STG = W / "staging"
END = date.fromisoformat(os.environ.get("MB_END", str(date.today())))
# daily incremental runs (MB_DAILY=1) get a fresh done-set per date; the initial backfill uses done/
DONE = W / (f"done-daily/{END}" if os.environ.get("MB_DAILY") else "done")
for d in (STG, DONE): d.mkdir(parents=True, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                    handlers=[logging.FileHandler(W / "backfill.log"), logging.StreamHandler()])
log = logging.getLogger()
START = END - timedelta(days=365)
CHUNK = 200          # calendar days per call (<8000 rows)
PAUSE = float(os.environ.get("MB_PAUSE", "0.15"))
MERGE_EVERY = 200    # stocks
COLS = ["ts_code", "trade_time", "freq", "trade_date", "open", "high", "low", "close", "vol", "amount"]

def db(read_only):
    for i in range(600):
        try:
            return duckdb.connect(str(DB), read_only=read_only)
        except duckdb.IOException:
            time.sleep(5)
    raise RuntimeError("market.duckdb locked too long")

def research_running():
    return os.system("systemctl is-active --quiet rquant-research.service") == 0

def merge():
    files = sorted(glob.glob(str(STG / "*.parquet")))
    if not files:
        return
    while research_running():
        log.info("rquant-research.service running; delay merge"); time.sleep(120)
    con = db(False)
    try:
        t = time.time()
        lst = "[" + ",".join(f"'{f}'" for f in files) + "]"
        con.execute(f"INSERT OR REPLACE INTO minute_bar ({','.join(COLS)}) "
                    f"SELECT {','.join(COLS)} FROM read_parquet({lst})")
        n = con.execute(f"SELECT count(*) FROM read_parquet({lst})").fetchone()[0]
    finally:
        con.close()
    for f in files:
        shutil.move(f, DONE / Path(f).name)
    log.info("merged %d files / %d rows in %.1fs", len(files), n, time.time() - t)

def main():
    con = db(True)
    try:
        codes = [r[0] for r in con.execute(
            "SELECT DISTINCT ts_code FROM daily_bar WHERE trade_date >= ? ORDER BY 1", [START]).fetchall()]
        have = {c: (a, b) for c, a, b in con.execute(
            "SELECT ts_code, min(trade_date), max(trade_date) FROM minute_bar WHERE freq='5min' GROUP BY 1").fetchall()}
        days = [r[0] for r in con.execute(
            "SELECT DISTINCT trade_date FROM daily_bar WHERE trade_date BETWEEN ? AND ?", [START, END]).fetchall()]
    finally:
        con.close()
    last_day = max(days)
    tok = os.environ.get("TUSHARE_TOKEN_MAIN") or os.environ["TUSHARE_TOKEN"]
    pro = ts.pro_api(tok)
    todo = [c for c in codes if not (DONE / f"{c}.parquet").exists() and not (STG / f"{c}.parquet").exists()]
    log.info("universe %d, todo %d, window %s..%s", len(codes), len(todo), START, END)
    since = 0
    for i, code in enumerate(todo):
        ranges = [(START, END)]
        if code in have:
            a, b = have[code]
            ranges = []
            if a > START + timedelta(days=7): ranges.append((START, a - timedelta(days=1)))
            if b < last_day: ranges.append((b + timedelta(days=1), END))
        frames = []
        for s, e in ranges:
            while s <= e:
                x = min(e, s + timedelta(days=CHUNK))
                for att in range(8):
                    try:
                        time.sleep(PAUSE)
                        df = pro.query("stk_mins", ts_code=code, freq="5min",
                                       start_date=f"{s} 09:00:00", end_date=f"{x} 15:30:00")
                        break
                    except Exception as exc:
                        msg = str(exc); log.warning("%s %s..%s: %s", code, s, x, msg[:120])
                        if "总量" in msg or "每天" in msg or "权限" in msg:
                            merge(); log.error("quota/permission stop"); sys.exit(3)
                        time.sleep(min(60, 5 * (att + 1)))
                else:
                    raise RuntimeError(f"{code} failed")
                if df is not None and len(df): frames.append(df)
                s = x + timedelta(days=1)
        if frames:
            df = pd.concat(frames, ignore_index=True)
            df["trade_time"] = pd.to_datetime(df["trade_time"])
            df = df.assign(freq="5min", trade_date=df["trade_time"].dt.date).drop_duplicates(["trade_time"])
        else:
            (DONE / f"{code}.parquet").touch(); continue
        tmp = STG / f".{code}.tmp"
        df[COLS].to_parquet(tmp, index=False); os.replace(tmp, STG / f"{code}.parquet")
        since += 1
        if since >= MERGE_EVERY:
            merge(); since = 0
            log.info("progress %d/%d", i + 1, len(todo))
    merge()
    log.info("DONE")

if __name__ == "__main__":
    main()
