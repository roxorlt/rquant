# ruff: noqa: E501
"""Intraday volume-surge scanner fed by the snapshot collector (~3s cycles).

Factors per stock at exchange time t (trading-minute axis m: 09:30=0 .. 11:30=120=13:00 .. 15:00=240):
  ratio = today's cumulative volume / prior trading day's cumulative volume at the same m
  acc1 / acc5 = today's volume over the last ~1 / ~5 minutes / prior day's volume over the
  same interval (prior curve interpolated per minute).
Prior-day curve: the prior day's snapshot dir (if it covers the market), else 5-min
``minute_bar`` in ``market.duckdb`` linearly interpolated.

Events (from 09:35, cooldown per stock): ``ratio`` (ratio>=RATIO_MIN, cum amount>=AMOUNT_MIN)
and ``accel`` (acc5>=ACC5_MIN, 5-min amount>=WIN_AMOUNT_MIN, ratio>=ACC_RATIO_MIN). ST, BJ-less filters:
names containing ST are skipped; limit-locked stocks (at limit with empty opposite book) skipped.

Output under ``snapshots/YYYY-MM-DD/surge/``: ``events.jsonl`` (append, read by the web),
``factors-HHMM.parquet`` (one snapshot of all factors per minute) and ``events.parquet`` at close.
Push: batched PushDeer (<=1 msg / PUSH_EVERY s, top PUSH_TOP), in a background thread.
Everything here is best effort: the collector wraps every call and disables us on errors.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

log = logging.getLogger("rquant.surge")

RATIO_MIN = float(os.environ.get("SURGE_RATIO_MIN", 8.0))
AMOUNT_MIN = float(os.environ.get("SURGE_AMOUNT_MIN", 1e8))      # CNY, cumulative
ACC5_MIN = float(os.environ.get("SURGE_ACC5_MIN", 10.0))
ACC_RATIO_MIN = float(os.environ.get("SURGE_ACC_RATIO_MIN", 3.0))  # ratio floor for accel events
WIN_AMOUNT_MIN = float(os.environ.get("SURGE_WIN_AMOUNT_MIN", 5e7))  # CNY in the 5-min window
MIN_PRIOR_VOL = 1000.0     # shares at m in prior curve; below -> no ratio
START_M = 5.0              # events from 09:35
COOLDOWN_S = float(os.environ.get("SURGE_COOLDOWN_S", 900))
# 2026-10-12 replay: old 3/30M + 5/10M/1.5 gave ~5.5k events; these give ~490 (139 stocks),
# 42.6% closed above event price vs 37.5% for all events.
PUSH_EVERY = 60.0
PUSH_TOP = 10
NM = 241


def trading_minute(t: str) -> float:
    """'HH:MM:SS' -> minutes since 09:30 on the continuous-session axis (clipped 0..240)."""
    try:
        h, mi, s = (int(x) for x in t.split(":"))
    except Exception:  # noqa: BLE001
        return float("nan")
    x = h * 60 + mi + s / 60.0
    if x < 570:
        return 0.0
    if x <= 690:
        return x - 570
    if x < 780:
        return 120.0
    return min(240.0, 120 + x - 780)


def _tm_vec(times: pd.Series) -> np.ndarray:
    s = times.astype(str).str.slice(0, 8)
    h = pd.to_numeric(s.str.slice(0, 2), errors="coerce")
    mi = pd.to_numeric(s.str.slice(3, 5), errors="coerce")
    se = pd.to_numeric(s.str.slice(6, 8), errors="coerce").fillna(0)
    x = (h * 60 + mi + se / 60.0).to_numpy(dtype=float)
    out = np.where(x < 570, 0.0, np.where(x <= 690, x - 570,
                   np.where(x < 780, 120.0, np.minimum(240.0, 120 + x - 780))))
    return out


# --------------------------------------------------------------------------- prior curve
class PriorCurve:
    def __init__(self, codes: list[str], cum: np.ndarray, day: str, source: str) -> None:
        self.codes = codes
        self.idx = {c: i for i, c in enumerate(codes)}
        self.cum = cum  # (n, 241) cumulative shares at integer minute m
        self.day, self.source = day, source

    def at(self, rows: np.ndarray, m: np.ndarray) -> np.ndarray:
        m = np.clip(m, 0, 240)
        lo = np.floor(m).astype(int)
        hi = np.minimum(lo + 1, 240)
        w = m - lo
        return self.cum[rows, lo] * (1 - w) + self.cum[rows, hi] * w


def _interp_cum(groups: dict[str, tuple[np.ndarray, np.ndarray]]) -> tuple[list[str], np.ndarray]:
    codes = sorted(groups)
    grid = np.arange(NM, dtype=float)
    arr = np.zeros((len(codes), NM))
    for i, c in enumerate(codes):
        xs, ys = groups[c]
        arr[i] = np.interp(grid, xs, ys)
    return codes, arr


def prior_from_minute_bar(db: Path, today: date) -> PriorCurve | None:
    import duckdb

    con = duckdb.connect(str(db), read_only=True)
    try:
        con.execute("SET memory_limit='120MB'; SET threads=1; SET enable_progress_bar=false")
        row = con.execute("SELECT max(trade_date) FROM minute_bar WHERE freq='5min' AND "
                          "trade_time >= ? AND trade_date < ?",
                          [pd.Timestamp(today) - pd.Timedelta(days=15), today]).fetchone()
        if not row or row[0] is None:
            return None
        d = row[0]
        df = con.execute("SELECT ts_code, strftime(trade_time, '%H:%M:%S') AS t, vol FROM minute_bar "
                         "WHERE freq='5min' AND trade_date=? ORDER BY ts_code, trade_time",
                         [d]).df()
    finally:
        con.close()
    df["m"] = _tm_vec(df["t"])
    df["cum"] = df.groupby("ts_code")["vol"].cumsum()
    groups = {}
    for c, g in df.groupby("ts_code", sort=False):
        xs = np.concatenate([[0.0], g["m"].to_numpy()])
        ys = np.concatenate([[0.0], g["cum"].to_numpy()])
        groups[c] = (xs, ys)
    codes, arr = _interp_cum(groups)
    return PriorCurve(codes, arr, str(d), "minute_bar_5min")


def prior_from_snapshots(day_dir: Path) -> PriorCurve | None:
    import duckdb

    files = [p for p in [day_dir / "quotes.parquet"] if p.is_file()]
    if not files:
        files = sorted((day_dir / "quotes").glob("part-*.parquet"))
    if not files:
        return None
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='120MB'; SET threads=1; SET enable_progress_bar=false")
        lst = ",".join(f"'{p}'" for p in files)
        df = con.execute(f"SELECT ts_code, substr(time,1,5) AS hm, max(volume) AS v "
                         f"FROM read_parquet([{lst}]) WHERE volume > 0 "
                         f"GROUP BY 1,2 ORDER BY 1,2").df()
    finally:
        con.close()
    if df["ts_code"].nunique() < 3000:
        return None
    df["m"] = np.minimum(_tm_vec(df["hm"] + ":59"), 240.0)
    groups = {}
    for c, g in df.groupby("ts_code", sort=False):
        xs, ys = g["m"].to_numpy(), np.maximum.accumulate(g["v"].to_numpy())
        xs, keep = np.unique(xs, return_index=True)
        ys = np.maximum.reduceat(ys, keep) if len(keep) else ys
        if xs[0] > 0:
            xs, ys = np.concatenate([[0.0], xs]), np.concatenate([[ys[0]], ys])
        groups[c] = (xs, ys)
    codes, arr = _interp_cum(groups)
    return PriorCurve(codes, arr, day_dir.name, "snapshots")


def load_prior(research: Path, today: date) -> PriorCurve | None:
    snaps = research / "snapshots"
    mb_day = None
    try:
        mb = prior_from_minute_bar(research / "market.duckdb", today)
        mb_day = mb.day if mb else None
    except Exception as exc:  # noqa: BLE001
        log.warning("surge: minute_bar prior failed: %s", str(exc)[:200])
        mb = None
    prev_dirs = sorted(p for p in snaps.glob("20??-??-??") if p.name < f"{today:%Y-%m-%d}")
    if prev_dirs and (mb_day is None or prev_dirs[-1].name >= mb_day):
        try:
            sp = prior_from_snapshots(prev_dirs[-1])
            if sp is not None:
                return sp
        except Exception as exc:  # noqa: BLE001
            log.warning("surge: snapshot prior failed: %s", str(exc)[:200])
    return mb


# --------------------------------------------------------------------------- push
def _env_file_values(path: str | None, names: tuple[str, ...]) -> dict[str, str]:
    out = {n: os.environ[n] for n in names if os.environ.get(n)}
    if path and Path(path).is_file():
        for line in Path(path).read_text("utf-8", "ignore").splitlines():
            k, sep, v = line.strip().partition("=")
            if sep and k in names and k not in out:
                out[k] = v.strip().strip('"').strip("'")
    return out


class Pusher:
    """Batches events; at most one PushDeer message per PUSH_EVERY seconds (top N by ratio)."""

    def __init__(self, send: Callable[[str, str], Any] | None, nopush_file: Path | None = None):
        self.send = send
        self.pending: list[dict] = []
        self.last = 0.0
        self.nopush_file = nopush_file
        self.lock = threading.Lock()

    @staticmethod
    def priority() -> set[str]:
        """竞价选股 pool codes for today: pushed first, marked ★ (never raises).

        Paused since 2026-10-14 (pool is a watchlist, not a buy signal) unless
        AUCTION_POOL_PUSH=1 and the promotion gate passed; surge alerts are unaffected."""
        if os.environ.get("AUCTION_POOL_PUSH", "0") != "1":
            return set()
        try:
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo

            from rquant.research.auction_pool import priority_codes, research_root
            from rquant.research.promotion_gate import gate_passed

            if not gate_passed(research_root(), "auction-ths"):
                return set()
            return set(priority_codes(research_root(), _dt.now(ZoneInfo("Asia/Shanghai")).date()))
        except Exception:  # noqa: BLE001
            return set()

    @classmethod
    def from_env(cls, nopush_file: Path | None = None) -> Pusher:
        if os.environ.get("SURGE_PUSH", "1") == "0":
            return cls(None, nopush_file)
        vals = _env_file_values(os.environ.get("SURGE_PUSH_ENV_FILE"),
                                ("PUSHDEER_KEYS", "PUSHDEER_ENDPOINT"))
        keys = [k.strip() for k in vals.get("PUSHDEER_KEYS", "").split(",") if k.strip()]
        if not keys:
            log.info("surge: no PUSHDEER_KEYS; push disabled")
            return cls(None, nopush_file)
        from rquant.notify.client import PushDeerClient

        client = PushDeerClient(keys, vals.get("PUSHDEER_ENDPOINT")
                                or "https://api2.pushdeer.com/message/push")
        log.info("surge: PushDeer push enabled (%d key(s))", len(keys))
        return cls(client.push, nopush_file)

    def add(self, events: list[dict]) -> None:
        if self.send is not None:
            self.pending.extend(events)

    def maybe_flush(self, now_s: float | None = None) -> dict | None:
        now_s = time.monotonic() if now_s is None else now_s
        if self.send is None or not self.pending or now_s - self.last < PUSH_EVERY:
            return None
        if self.nopush_file is not None and self.nopush_file.exists():
            self.pending.clear()
            return None
        evs, self.pending = self.pending, []
        self.last = now_s
        title, body = format_push(evs, self.priority())
        threading.Thread(target=self._send, args=(title, body), daemon=True).start()
        return {"title": title, "body": body}

    def _send(self, title: str, body: str) -> None:
        try:
            self.send(title, body)
        except Exception as exc:  # noqa: BLE001
            log.warning("surge push failed: %s", str(exc)[:200])


def format_push(evs: list[dict], priority: set[str] | None = None) -> tuple[str, str]:
    priority = priority or set()
    best: dict[str, dict] = {}
    for e in evs:
        if e["ts_code"] not in best or e["ratio"] > best[e["ts_code"]]["ratio"]:
            best[e["ts_code"]] = e
    top = sorted(best.values(), key=lambda e: (e["ts_code"] not in priority, -e["ratio"]))[:PUSH_TOP]
    t = max(e["time"] for e in evs)[:5]
    title = f"异动 {len(best)}只 {t}"
    lines = ["| 时间 | 股票 | 量比 | 5分加速 | 涨跌 | 成交额 |", "|---|---|---|---|---|---|"]
    for e in top:
        star = "★" if e["ts_code"] in priority else ""
        lines.append(f"| {e['time'][:5]} | {star}{e['name']} {e['ts_code'][:6]} | {e['ratio']:.1f} | "
                     f"{e['acc5']:.1f} | {e['pct'] * 100:+.1f}% | {e['amount'] / 1e8:.2f}亿 |")
    if len(best) > len(top):
        lines.append(f"\n另 {len(best) - len(top)} 只见网页「盯盘与告警 · 量能异动」")
    return title, "\n".join(lines)


# --------------------------------------------------------------------------- scanner
def limit_pct(code: np.ndarray, name: np.ndarray) -> np.ndarray:
    code = code.astype(str)
    st = np.char.find(np.char.upper(name.astype(str)), "ST") >= 0
    star_gem = np.char.startswith(code, "688") | np.char.startswith(code, "30")
    bj = np.char.endswith(code, ".BJ")
    return np.where(bj, 0.30, np.where(star_gem, 0.20, np.where(st, 0.05, 0.10)))


class SurgeScanner:
    def __init__(self, prior: PriorCurve, out_dir: Path, pusher: Pusher | None = None) -> None:
        self.prior = prior
        self.n = len(prior.codes)
        self.today = np.full((self.n, NM), np.nan)   # last cum vol seen in minute bucket j
        self.amt = np.full((self.n, NM), np.nan)
        self.last: pd.DataFrame | None = None
        self.cool: dict[tuple[str, str], float] = {}
        self.out = out_dir
        self.out.mkdir(parents=True, exist_ok=True)
        self.pusher = pusher
        self.factor_minute: int | None = None
        self.factors: pd.DataFrame | None = None
        self.unit = 1.0  # multiply volume to get shares (auto-detected)
        self.unit_checked = False
        self.events: list[dict] = []

    def _check_unit(self, df: pd.DataFrame) -> None:
        x = df[(df.volume > 0) & (df.price > 0) & (df.amount > 0)]
        if len(x) < 200:
            return
        r = float(np.median(x.amount / (x.volume * x.price)))
        self.unit = 100.0 if 50 < r < 200 else 1.0
        self.unit_checked = True
        log.info("surge: volume unit factor %.0f (median amount/(vol*px)=%.2f)", self.unit, r)

    def update(self, df: pd.DataFrame, wall: float | None = None) -> pd.DataFrame:
        """Feed one collector cycle; returns the new events (also persisted/pushed)."""
        if df is None or not len(df):
            return pd.DataFrame()
        df = df[df["ts_code"].isin(self.prior.idx)]
        df = df.dropna(subset=["volume", "time"])
        df = df[df["volume"] > 0]
        if not len(df):
            return pd.DataFrame()
        if not self.unit_checked:
            self._check_unit(df)
        df = df.sort_values("time").drop_duplicates("ts_code", keep="last").copy()
        rows = df["ts_code"].map(self.prior.idx).to_numpy()
        m = _tm_vec(df["time"])
        vol = df["volume"].to_numpy(float) * self.unit
        amt = df["amount"].to_numpy(float)
        b = np.minimum(np.floor(m).astype(int), 240)
        self.today[rows, b] = np.fmax(self.today[rows, b], vol)
        self.amt[rows, b] = np.fmax(self.amt[rows, b], amt)

        pc = self.prior.at(rows, m)
        ratio = np.where(pc >= MIN_PRIOR_VOL, vol / np.maximum(pc, 1), np.nan)
        res = {"ratio": ratio}
        for w in (1, 5):
            j = b - w
            ok = j >= 0
            jj = np.maximum(j, 0)
            # forward-fill: latest bucket <= j that has data
            prev_v = self._ffill(rows, jj)
            prev_a = self._ffill(rows, jj, self.amt)
            prev_v = np.where(ok, prev_v, 0.0)
            prev_a = np.where(ok, prev_a, 0.0)
            pw = pc - self.prior.at(rows, np.where(ok, jj + 1.0, 0.0))
            tw = vol - prev_v
            res[f"acc{w}"] = np.where(pw > 0, tw / np.maximum(pw, 1), np.nan)
            res[f"amt{w}"] = amt - prev_a
        f = pd.DataFrame({
            "ts_code": df["ts_code"].to_numpy(), "name": df["name"].astype(str).to_numpy(),
            "time": df["time"].astype(str).to_numpy(), "m": m,
            "price": df["price"].to_numpy(float), "pre_close": df["pre_close"].to_numpy(float),
            "volume": vol, "amount": amt, "prior_cum_vol": pc, **res})
        f["pct"] = f["price"] / f["pre_close"] - 1
        lp = limit_pct(f["ts_code"].to_numpy(), f["name"].to_numpy())
        up = np.round(f["pre_close"] * (1 + lp) + 1e-9, 2)
        dn = np.round(f["pre_close"] * (1 - lp) + 1e-9, 2)
        a1 = df["a1_v"].to_numpy(float) if "a1_v" in df else np.full(len(df), np.nan)
        b1 = df["b1_v"].to_numpy(float) if "b1_v" in df else np.full(len(df), np.nan)
        locked = ((f["price"] >= up - 0.001) & ~(a1 > 0)) | ((f["price"] <= dn + 0.001) & ~(b1 > 0))
        f["locked"] = locked.to_numpy()
        f["st"] = f["name"].str.upper().str.contains("ST").to_numpy()
        self._keep_factors(f)
        return self._events(f, time.time() if wall is None else wall)

    def _ffill(self, rows: np.ndarray, j: np.ndarray, arr: np.ndarray | None = None) -> np.ndarray:
        arr = self.today if arr is None else arr
        out = arr[rows, j].copy()
        miss = np.isnan(out)
        k = 0
        while miss.any() and k < 30:
            k += 1
            jj = np.maximum(j - k, 0)
            out[miss] = arr[rows[miss], jj[miss]]
            miss = np.isnan(out)
        return np.nan_to_num(out, nan=0.0)

    def _events(self, f: pd.DataFrame, wall: float) -> pd.DataFrame:
        base = (f["m"] >= START_M) & ~f["st"] & ~f["locked"] & (f["pre_close"] > 0)
        r = base & (f["ratio"] >= RATIO_MIN) & (f["amount"] >= AMOUNT_MIN)
        a = base & (f["acc5"] >= ACC5_MIN) & (f["amt5"] >= WIN_AMOUNT_MIN) & (f["ratio"] >= ACC_RATIO_MIN)
        out = []
        for kind, mask in (("ratio", r), ("accel", a)):
            for rec in f[mask].to_dict("records"):
                key = (rec["ts_code"], kind)
                if wall - self.cool.get(key, -1e18) < COOLDOWN_S:
                    continue
                self.cool[key] = wall
                rec = {k: (None if isinstance(v, float) and np.isnan(v) else v)
                       for k, v in rec.items() if k not in ("locked", "st")}
                rec |= {"kind": kind, "date": f"{datetime.now():%Y-%m-%d}",
                        "at": datetime.now().isoformat(timespec="seconds")}
                out.append(rec)
        if out:
            with (self.out / "events.jsonl").open("a", encoding="utf-8") as fh:
                for e in out:
                    fh.write(json.dumps(e, ensure_ascii=False, default=float) + "\n")
            self.events.extend(out)
            if self.pusher:
                self.pusher.add([e for e in out if e.get("ratio") is not None])
        if self.pusher:
            self.pusher.maybe_flush()
        return pd.DataFrame(out)

    def _keep_factors(self, f: pd.DataFrame) -> None:
        minute = int(datetime.now().strftime("%H%M"))
        if self.factor_minute is not None and minute != self.factor_minute:
            self.flush_factors()
        self.factor_minute = minute
        # latest full-market cycle of the minute (rt_k/rt_tick each cover all A)
        if self.factors is None or len(f) >= 0.5 * len(self.factors):
            self.factors = f.drop(columns=["name"])

    def flush_factors(self) -> None:
        if self.factors is None or not len(self.factors) or self.factor_minute is None:
            return
        p = self.out / f"factors-{self.factor_minute:04d}.parquet"
        tmp = p.with_suffix(".tmp")
        self.factors.assign(minute=self.factor_minute).to_parquet(tmp, index=False)
        tmp.rename(p)
        self.factors = None

    def close(self) -> None:
        self.flush_factors()
        parts = sorted(self.out.glob("factors-*.parquet"))
        if parts:
            pd.concat([pd.read_parquet(p) for p in parts]).to_parquet(
                self.out / "factors.parquet", index=False)
            for p in parts:
                p.unlink()
        ev = self.out / "events.jsonl"
        if ev.is_file():
            pd.read_json(ev, lines=True).to_parquet(self.out / "events.parquet", index=False)
        if self.pusher:
            self.pusher.last = -1e18
            self.pusher.maybe_flush()


def save_prior(p: PriorCurve, path: Path) -> None:
    tmp = path.with_suffix(".tmp.npz")
    np.savez_compressed(tmp, codes=np.array(p.codes), cum=p.cum, day=p.day, source=p.source)
    tmp.rename(path)


def load_prior_file(path: Path) -> PriorCurve:
    z = np.load(path, allow_pickle=False)
    return PriorCurve([str(c) for c in z["codes"]], z["cum"], str(z["day"]), str(z["source"]))


def prior_in_subprocess(research: Path, today: date, dest: Path, timeout: float = 600) -> PriorCurve | None:
    """Build the prior curve in a child process so DuckDB's peak memory is freed afterwards."""
    import subprocess
    import sys

    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.is_file():
        subprocess.run([sys.executable, "-m", "rquant.research.surge", "prior", str(research),
                        today.isoformat(), str(dest)], check=True, timeout=timeout)
    return load_prior_file(dest) if dest.is_file() else None


def main(argv: list[str] | None = None) -> int:
    import sys

    a = sys.argv[1:] if argv is None else argv
    logging.basicConfig(level=logging.INFO)
    if a and a[0] == "prior":
        p = load_prior(Path(a[1]), date.fromisoformat(a[2]))
        if p is None:
            return 1
        save_prior(p, Path(a[3]))
        print(p.day, p.source, len(p.codes))
        return 0
    if a and a[0] == "prior-today":  # rquant-surge-prior.service (before the collector)
        root = Path(os.environ.get("RQUANT_RESEARCH_ROOT") or Path.home() / "rquant-research")
        today = date.today()
        dest = root / "snapshots" / f"{today:%Y-%m-%d}" / "surge" / "prior.npz"
        dest.parent.mkdir(parents=True, exist_ok=True)
        return main(["prior", str(root), today.isoformat(), str(dest)])
    print("usage: python -m rquant.research.surge prior <research_root> <YYYY-MM-DD> <out.npz>")
    return 2


def read_events(research: Path, day: str | None = None, limit: int = 300) -> tuple[str | None, list[dict]]:
    snaps = research / "snapshots"
    if day is None:
        days = sorted((p.parent.parent.name for p in snaps.glob("20??-??-??/surge/events.jsonl")),
                      reverse=True)
        if not days:
            return None, []
        day = days[0]
    p = snaps / day / "surge" / "events.jsonl"
    if not p.is_file():
        return day, []
    rows = []
    for line in p.read_text("utf-8").splitlines()[-limit * 2:]:
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return day, rows[-limit:][::-1]


if __name__ == "__main__":
    raise SystemExit(main())
