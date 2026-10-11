"""Level-1 snapshot collector: ~3s 5-level quotes for a small universe, plus after-close ticks.

Polls Tushare Pro ``rt_tick`` (5-level) / ``rt_k`` (level 1) for the FULL A-share market
(one call each), falling back to ``realtime_quote`` sina/dc for the priority universe, during the
A-share sessions (09:15-11:30, 13:00-15:00 Asia/Shanghai, trading days only) for a bounded
universe (manual watchlist + pools + latest screen/condition hits, capped), and appends
small parquet part files under ``$RQUANT_RESEARCH_ROOT/snapshots/YYYY-MM-DD/quotes/``.
After the close it fetches ``tushare.realtime_tick`` (same-day aggregated ticks) for the
same universe and compacts the parts into ``quotes.parquet``.

Read-only towards everything else: it never writes Serving, the main DB or page control.
Kill switch: create ``$RQUANT_RESEARCH_ROOT/snapshots/STOP`` (checked every cycle).

    python -m rquant.research.snapshots run              # the systemd entry point
    python -m rquant.research.snapshots universe         # print the universe, no fetch
    python -m rquant.research.snapshots run --force --max-cycles 3 --out /tmp/x  # dry run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import socket
import sys
import time
from collections.abc import Callable, Iterable
from datetime import date, datetime
from datetime import time as dtime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

log = logging.getLogger("rquant.snapshots")

TZ = ZoneInfo("Asia/Shanghai")
SESSIONS = ((dtime(9, 15), dtime(11, 30, 30)), (dtime(13, 0), dtime(15, 0, 30)))
TICK_AT = dtime(15, 10)
CODE_RE = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")
DEFAULT_CAP = 300       # priority universe: fallback sources + after-close ticks
TICK_CAP = 80           # realtime_tick (tx pages, ~25s/symbol) after the close
SINA_CAP = 300          # sina fallback: 3 requests of 100 per ~3s cycle
DC_CAP = 30             # eastmoney fallback: one request per symbol
BATCH = 100
INTERVAL = 3.25         # rt_tick every 2nd cycle (6.5s, <=10/min), rt_k in between
TS_TICK_MIN_GAP = 6.4
FLUSH_EVERY = 30.0
MAX_FAILED_CYCLES = 30
LEVEL_COLS = [f"{s}{i}_{k}" for s in ("b", "a") for i in range(1, 6) for k in ("p", "v")]
QUOTE_COLS = ["ts_code", "name", "date", "time", "price", "open", "high", "low", "pre_close",
              "bid", "ask", "volume", "amount", *LEVEL_COLS]
NUM_COLS = [*QUOTE_COLS[4:], "num"]


def now() -> datetime:
    return datetime.now(TZ)


def research_root() -> Path:
    return Path(os.environ.get("RQUANT_RESEARCH_ROOT") or "data/research")


# --------------------------------------------------------------------------- universe
def _codes(values: Iterable[object]) -> list[str]:
    out = []
    for v in values:
        s = str(v or "").strip().upper()
        if CODE_RE.match(s):
            out.append(s)
    return out


def _watchlist_log(commands_dir: Path) -> list[str]:
    path = commands_dir / "watchlist" / "items.jsonl"
    if not path.is_file():
        return []
    codes = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict):
            codes.append(rec.get("ts_code") or (rec.get("item") or {}).get("ts_code"))
    return list(reversed(_codes(codes)))  # newest first


def _serving(root: str | None) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    if not root:
        return out
    try:
        from rquant.web.source import ServingSource
    except Exception as exc:  # noqa: BLE001
        log.warning("serving source unavailable: %s", exc)
        return out
    src = ServingSource(root)
    queries = {
        "serving_watchlist": "SELECT ts_code FROM manual_watchlist "
                             "ORDER BY added_at DESC LIMIT 200",
        "pools": "SELECT DISTINCT ts_code FROM canvas_hit WHERE trade_date = "
                 "(SELECT max(trade_date) FROM canvas_hit) LIMIT 2000",
        "screen": "SELECT DISTINCT ts_code FROM screen_result WHERE trade_date = "
                  "(SELECT max(trade_date) FROM screen_result) LIMIT 2000",
    }
    for key, sql in queries.items():
        try:
            out[key] = _codes(r["ts_code"] for r in src.query(sql))
        except Exception as exc:  # noqa: BLE001 - a missing projection is just empty
            log.info("serving %s skipped: %s", key, str(exc)[:160])
    return out


def _condition_hits(root: Path) -> list[str]:
    results = []
    for f in (root / "condition").glob("*/result.json"):
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(d, dict):
            results.append((str(d.get("trade_date") or ""), str(d.get("created_at") or ""), d))
    if not results:
        return []
    latest = max(r[0] for r in results)
    codes: list[str] = []
    for _, _, d in sorted((r for r in results if r[0] == latest), key=lambda r: r[1], reverse=True):
        codes += _codes(h.get("code") or h.get("ts_code") for h in d.get("hits") or []
                        if isinstance(h, dict))
    return codes


def build_universe(cap: int = DEFAULT_CAP) -> dict[str, Any]:
    """Priority: extra env > watchlist (commands log, Serving) > pools > screen > condition."""
    commands_dir = Path(os.environ.get("RQUANT_WEB_COMMANDS_DIR")
                       or Path.home() / "rquant-web-commands")
    serving = _serving(os.environ.get("RQUANT_SERVING_ROOT"))
    sources = {
        "extra": _codes(os.environ.get("RQUANT_SNAPSHOT_EXTRA", "").split(",")),
        "watchlist": _watchlist_log(commands_dir),
        "serving_watchlist": serving.get("serving_watchlist", []),
        "pools": serving.get("pools", []),
        "screen": serving.get("screen", []),
        "condition": _condition_hits(research_root()),
    }
    chosen: list[str] = []
    for codes in sources.values():
        for c in codes:
            if c not in chosen and len(chosen) < cap:
                chosen.append(c)
    return {"codes": chosen, "cap": cap, "counts": {k: len(v) for k, v in sources.items()},
            "built_at": now().isoformat(timespec="seconds")}


# --------------------------------------------------------------------------- calendar
def is_trading_day(day: date) -> bool:
    if day.weekday() >= 5:
        return False
    db = research_root() / "market.duckdb"
    try:
        import duckdb

        with duckdb.connect(str(db), read_only=True) as con:
            row = con.execute("SELECT is_open, (SELECT max(cal_date) FROM trade_calendar) "
                              "FROM trade_calendar WHERE cal_date = ?", [day]).fetchone()
        if row is not None:
            return bool(row[0])
        log.warning("trade_calendar has no row for %s; assuming weekday is open", day)
    except Exception as exc:  # noqa: BLE001
        log.warning("trade calendar unreadable (%s); assuming weekday is open", exc)
    return True


def in_session(t: dtime) -> bool:
    return any(a <= t <= b for a, b in SESSIONS)


def next_session_start(t: dtime) -> dtime | None:
    for a, _ in SESSIONS:
        if t < a:
            return a
    return None


# --------------------------------------------------------------------------- fetch
TUSHARE_URL = "http://api.tushare.pro"
TS_TICK_FIELDS = ["ts_code", "name", "trade_time", "price", "pre_price", "high_price",
                  "open_price", "low_price", "volume", "amount", "num",
                  *[f"{s}_{k}{i}" for i in range(1, 6) for s in ("ask", "bid")
                    for k in ("price", "volume")]]
TS_K_FIELDS = ["ts_code", "name", "trade_time", "close", "pre_close", "high", "open", "low",
               "vol", "amount", "num", "ask_price1", "ask_volume1", "bid_price1", "bid_volume1"]
FULL_MARKET = "3*.SZ,6*.SH,0*.SZ,9*.BJ"


def tushare_token() -> str | None:
    """TUSHARE_TOKEN_MAIN from the environment or the rQuant .env (never logged)."""
    tok = os.environ.get("TUSHARE_TOKEN_MAIN")
    if tok:
        return tok
    path = Path(os.environ.get("RQUANT_TUSHARE_ENV_FILE") or Path.home() / "rquant" / ".env")
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            m = re.match(r"\s*(?:export\s+)?TUSHARE_TOKEN_MAIN\s*=\s*[\"']?([^\"'\s#]+)", line)
            if m:
                return m.group(1)
    except OSError:
        pass
    return None


class TushareError(RuntimeError):
    pass


def tushare_call(api: str, params: dict[str, Any], fields: list[str], token: str,
                 timeout: float = 10.0) -> pd.DataFrame:
    import urllib.request

    body = json.dumps({"api_name": api, "token": token, "params": params,
                       "fields": ",".join(fields)}).encode()
    req = urllib.request.Request(TUSHARE_URL, body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - fixed URL
        r = json.loads(resp.read())
    if r.get("code"):
        raise TushareError(f"{api} code={r.get('code')} {str(r.get('msg'))[:120]}")
    data = r.get("data") or {}
    return pd.DataFrame(data.get("items") or [], columns=data.get("fields") or fields)


def _split_time(df: pd.DataFrame, col: str) -> pd.DataFrame:
    ts = df[col].astype(str)
    df["date"] = ts.str.slice(0, 10).str.replace("-", "", regex=False)
    df["time"] = ts.str.slice(11, 19)
    return df


def norm_rt_tick(df: pd.DataFrame) -> pd.DataFrame:
    """Tushare rt_tick: full market, 5-level book. Volumes in shares."""
    ren = {"price": "price", "pre_price": "pre_close", "high_price": "high",
           "open_price": "open", "low_price": "low"}
    for i in range(1, 6):
        ren |= {f"bid_price{i}": f"b{i}_p", f"bid_volume{i}": f"b{i}_v",
                f"ask_price{i}": f"a{i}_p", f"ask_volume{i}": f"a{i}_v"}
    df = _split_time(df.rename(columns=ren), "trade_time")
    df["bid"], df["ask"] = df.get("b1_p"), df.get("a1_p")
    return _normalize(df, "ts_tick")


def norm_rt_k(df: pd.DataFrame) -> pd.DataFrame:
    """Tushare rt_k: full market, level 1 only (levels 2-5 null)."""
    df = _split_time(df.rename(columns={"close": "price", "vol": "volume",
                                        "bid_price1": "b1_p", "bid_volume1": "b1_v",
                                        "ask_price1": "a1_p", "ask_volume1": "a1_v"}),
                     "trade_time")
    df["bid"], df["ask"] = df["b1_p"], df["a1_p"]
    return _normalize(df, "ts_k")


def _normalize(df: pd.DataFrame | None, src: str) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=[*QUOTE_COLS, "num", "src"])
    df = df.rename(columns=str.lower)
    for c in [*QUOTE_COLS, "num"]:
        if c not in df.columns:
            df[c] = None
    df = df[[*QUOTE_COLS, "num"]].copy()
    for c in ("ts_code", "name", "date", "time"):
        df[c] = df[c].astype(str)
    df["date"] = df["date"].str.replace("-", "", regex=False)
    for c in NUM_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    df["src"] = src
    return df


def fetch_sina(codes: list[str]) -> pd.DataFrame:
    import tushare as ts

    df = _normalize(ts.realtime_quote(ts_code=",".join(codes), src="sina"), "sina")
    vcols = [c for c in LEVEL_COLS if c.endswith("_v")]
    df[vcols] = df[vcols] * 100  # sina book volumes come in lots; store shares
    return df


def fetch_dc(codes: list[str]) -> pd.DataFrame:
    """Eastmoney endpoint is one symbol per request: space them out."""
    import tushare as ts

    frames = []
    for c in codes:
        frames.append(_normalize(ts.realtime_quote(ts_code=c, src="dc"), "dc"))
        time.sleep(0.1)
    return pd.concat(frames, ignore_index=True) if frames else _normalize(None, "dc")


class Poller:
    """Tushare first: full market per call, alternating rt_tick (5 levels, our account is
    limited to 10 calls/min) and rt_k (level 1, 50/min). On Tushare failure fall back to
    sina for the priority universe (capped) and then eastmoney (per symbol, small cap)."""

    def __init__(self, codes: list[str], *, token: str | None = None,
                 sina: Callable[[list[str]], pd.DataFrame] = fetch_sina,
                 dc: Callable[[list[str]], pd.DataFrame] = fetch_dc,
                 ts_call: Callable[..., pd.DataFrame] | None = None,
                 sina_cap: int = SINA_CAP, dc_cap: int = DC_CAP) -> None:
        self.codes = codes
        self.sina_batches = [codes[:sina_cap][i:i + BATCH]
                             for i in range(0, min(len(codes), sina_cap), BATCH)]
        self.dc_codes = codes[:dc_cap]
        self.sina, self.dc = sina, dc
        self.token = token
        self.ts_call = ts_call or tushare_call
        self.failed_cycles = 0
        self.slot = 0
        self.last_tick_call = 0.0
        self.ts_cooldown_until = 0.0
        self.sina_cooldown_until = 0.0

    def _tushare(self) -> pd.DataFrame | None:
        if not self.token or time.monotonic() < self.ts_cooldown_until:
            return None
        use_tick = time.monotonic() - self.last_tick_call >= TS_TICK_MIN_GAP
        attempts = (["tick", "k"] if use_tick else ["k"])
        for kind in attempts:
            try:
                if kind == "tick":
                    self.last_tick_call = time.monotonic()
                    df = norm_rt_tick(self.ts_call("rt_tick", {"ts_code": FULL_MARKET},
                                                   TS_TICK_FIELDS, self.token))
                else:
                    df = norm_rt_k(self.ts_call("rt_k", {"ts_code": FULL_MARKET},
                                                TS_K_FIELDS, self.token))
                if len(df):
                    return df
                raise TushareError(f"empty {kind}")
            except Exception as exc:  # noqa: BLE001
                log.warning("tushare %s failed: %s", kind, str(exc)[:160])
        self.ts_cooldown_until = time.monotonic() + 30
        log.warning("tushare unavailable; fallback sources for 30s")
        return None

    def _fallback(self) -> list[pd.DataFrame]:
        frames = []
        for batch in self.sina_batches:
            if time.monotonic() < self.sina_cooldown_until:
                break
            try:
                df = self.sina(batch)
                if df is None or len(df) == 0:
                    raise RuntimeError("empty sina response")
                frames.append(df)
            except Exception as exc:  # noqa: BLE001
                log.warning("sina failed (%s); cooldown 60s", str(exc)[:160])
                self.sina_cooldown_until = time.monotonic() + 60
        if not frames and self.dc_codes:
            try:
                df = self.dc(self.dc_codes)
                if df is not None and len(df):
                    frames.append(df)
            except Exception as exc:  # noqa: BLE001
                log.warning("dc failed: %s", str(exc)[:160])
        return frames

    def cycle(self) -> pd.DataFrame:
        ts_df = self._tushare()
        frames = [ts_df] if ts_df is not None else self._fallback()
        self.failed_cycles = 0 if frames else self.failed_cycles + 1
        if not frames:
            return _normalize(None, "none")
        out = pd.concat(frames, ignore_index=True)
        out["local_ts"] = pd.Timestamp(now().replace(tzinfo=None))
        return out

    def backoff(self) -> float:
        if self.failed_cycles == 0:
            return 0.0
        return min(60.0, INTERVAL * 2 ** min(self.failed_cycles, 5))


class Deduper:
    """Drop a symbol's row when nothing changed since its last kept row from the same
    source (same exchange time, volume, amount and book). Keeps memory O(symbols)."""

    def __init__(self) -> None:
        self.last: dict[tuple[str, str], tuple] = {}

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        if not len(df):
            return df
        cols = ["time", "price", "volume", "amount", *LEVEL_COLS]
        keys = df[cols].astype(object).where(df[cols].notna(), None).itertuples(
            index=False, name=None)
        keep = []
        for code, src, k in zip(df["ts_code"], df["src"], keys, strict=True):
            prev = self.last.get((code, src))
            keep.append(prev != k)
            if prev != k:
                self.last[(code, src)] = k
        return df[keep]


# --------------------------------------------------------------------------- storage
def day_dir(out: Path, day: date) -> Path:
    return out / f"{day:%Y-%m-%d}"


def _schema():
    import pyarrow as pa

    fields = [(c, pa.string()) for c in ("ts_code", "name", "date", "time")]
    fields += [(c, pa.float64()) for c in NUM_COLS]
    fields += [("src", pa.string()), ("local_ts", pa.timestamp("us"))]
    return pa.schema(fields)


def write_part(d: Path, frames: list[pd.DataFrame]) -> Path | None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    frames = [f for f in frames if len(f)]
    if not frames:
        return None
    parts = d / "quotes"
    parts.mkdir(parents=True, exist_ok=True)
    df = pd.concat(frames, ignore_index=True)
    schema = _schema()
    df = df[[f.name for f in schema]]
    path = parts / f"part-{now():%H%M%S%f}-{os.getpid()}.parquet"
    tmp = path.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pandas(df, schema=schema, preserve_index=False), tmp,
                   compression="zstd")
    tmp.rename(path)
    return path


def compact(d: Path) -> dict[str, Any]:
    """Stream parts into one quotes.parquet (row groups = parts; bounded memory)."""
    import pyarrow.parquet as pq

    parts = sorted((d / "quotes").glob("part-*.parquet"))
    target = d / "quotes.parquet"
    if not parts:
        return {"rows": pq.ParquetFile(target).metadata.num_rows if target.is_file() else 0}
    schema = _schema()
    tmp = target.with_suffix(".tmp")
    rows = 0
    with pq.ParquetWriter(tmp, schema, compression="zstd") as w:
        sources = ([target] if target.is_file() else []) + parts
        for src in sources:
            pf = pq.ParquetFile(src)
            for i in range(pf.num_row_groups):
                tbl = pf.read_row_group(i).cast(schema)
                rows += tbl.num_rows
                w.write_table(tbl)
    tmp.rename(target)
    for p in parts:
        p.unlink()
    info = {"rows": rows, "bytes": target.stat().st_size}
    log.info("compacted %s: %s", target, info)
    return info


def fetch_ticks(d: Path, codes: list[str], fetch: Callable[[str], pd.DataFrame] | None = None,
                pause: float = 1.0) -> dict[str, Any]:
    if fetch is None:
        import tushare as ts

        def fetch(code: str) -> pd.DataFrame:
            return ts.realtime_tick(ts_code=code)
    frames, failed = [], []
    consecutive = 0
    for code in codes:
        if stop_requested():
            break
        try:
            df = fetch(code)
            if df is not None and len(df):
                df = df.rename(columns=str.lower).copy()
                df["ts_code"] = code
                frames.append(df)
            consecutive = 0
        except Exception as exc:  # noqa: BLE001
            failed.append(code)
            consecutive += 1
            log.warning("tick %s failed: %s", code, str(exc)[:160])
            if consecutive >= 5:
                log.error("5 consecutive tick failures; giving up")
                break
            time.sleep(min(60, pause * 2 ** consecutive))
        time.sleep(pause + random.uniform(0, pause / 2))
    info: dict[str, Any] = {"symbols": len(frames), "failed": failed}
    if frames:
        df = pd.concat(frames, ignore_index=True)
        df["fetched_on"] = d.name
        path = d / "ticks.parquet"
        tmp = path.with_suffix(".tmp")
        df.to_parquet(tmp, index=False, compression="zstd")
        tmp.rename(path)
        info |= {"rows": len(df), "bytes": path.stat().st_size}
    log.info("ticks: %s", {k: v for k, v in info.items() if k != "failed"}
             | {"failed": len(failed)})
    return info


_STOP_FILE: Path | None = None


def stop_requested() -> bool:
    return _STOP_FILE is not None and _STOP_FILE.exists()


def _sleep_until(target: datetime) -> bool:
    while (left := (target - now()).total_seconds()) > 0:
        if stop_requested():
            return False
        time.sleep(min(left, 30))
    return True


# --------------------------------------------------------------------------- main loop
def run(out: Path, *, force: bool = False, max_cycles: int | None = None, cap: int = DEFAULT_CAP,
        ticks: bool = True, tick_limit: int | None = None) -> int:
    global _STOP_FILE
    _STOP_FILE = out / "STOP"
    today = now().date()
    if not force and not is_trading_day(today):
        log.info("%s is not a trading day; exit", today)
        return 0
    if stop_requested():
        log.info("STOP file present (%s); exit", _STOP_FILE)
        return 0
    uni = build_universe(cap)
    codes = uni["codes"]
    d = day_dir(out, today)
    d.mkdir(parents=True, exist_ok=True)
    (d / "universe.json").write_text(json.dumps(uni, ensure_ascii=False, indent=1), "utf-8")
    log.info("universe %d symbols %s -> %s", len(codes), uni["counts"], d)
    token = tushare_token()
    log.info("primary source: %s", "tushare rt_tick/rt_k (full market)" if token
             else "NO TUSHARE TOKEN -> sina/dc fallback only")
    if not codes and not token:
        log.warning("empty universe and no tushare token; nothing to collect")
        return 0
    socket.setdefaulttimeout(10)  # the SDK passes no timeout to requests
    poller = Poller(codes, token=token)
    dedup = Deduper()
    scanner = _start_scanner(out, today, d)
    scan_errors = 0
    buf: list[pd.DataFrame] = []
    last_flush, cycles, rc = time.monotonic(), 0, 0
    end_of_day = datetime.combine(today, SESSIONS[-1][1], TZ)
    try:
        while not stop_requested():
            t = now()
            if not force:
                if t >= end_of_day:
                    break
                if not in_session(t.time()):
                    nxt = next_session_start(t.time())
                    write_part(d, buf)
                    buf = []
                    if nxt is None or not _sleep_until(datetime.combine(today, nxt, TZ)):
                        break
                    continue
            started = time.monotonic()
            raw = poller.cycle()
            if scanner is not None:
                try:
                    scanner.update(raw)
                except Exception as exc:  # noqa: BLE001 - never break collection
                    scan_errors += 1
                    log.warning("surge scanner error %d: %s", scan_errors, str(exc)[:200])
                    if scan_errors >= 20:
                        log.error("surge scanner disabled after 20 errors")
                        scanner = None
            buf.append(dedup(raw))
            cycles += 1
            if poller.failed_cycles >= MAX_FAILED_CYCLES:
                log.error("%d consecutive failed cycles; stopping collector", poller.failed_cycles)
                rc = 3
                break
            if time.monotonic() - last_flush >= FLUSH_EVERY:
                write_part(d, buf)
                buf, last_flush = [], time.monotonic()
            if max_cycles is not None and cycles >= max_cycles:
                break
            wait = max(INTERVAL - (time.monotonic() - started), 0.2) + poller.backoff()
            time.sleep(wait + random.uniform(-0.3, 0.3))
    finally:
        write_part(d, buf)
        if scanner is not None:
            try:
                scanner.close()
            except Exception as exc:  # noqa: BLE001
                log.warning("surge close failed: %s", str(exc)[:200])
    log.info("polling done: %d cycles", cycles)
    if ticks and not stop_requested() and (
            force or _sleep_until(datetime.combine(today, TICK_AT, TZ))):
        fetch_ticks(d, codes[:tick_limit or TICK_CAP])
    compact(d)
    try:
        from rquant.research.auction import write_auction_factors

        write_auction_factors(out, today)
    except Exception as exc:  # noqa: BLE001
        log.warning("auction factors failed: %s", str(exc)[:200])
    return rc


def _start_scanner(out: Path, today: date, d: Path):
    """Surge scanner (rquant.research.surge); None when disabled or the prior curve is missing."""
    if os.environ.get("SURGE_SCAN", "1") == "0":
        return None
    try:
        from rquant.research.surge import Pusher, SurgeScanner, prior_in_subprocess

        t0 = time.monotonic()
        prior = prior_in_subprocess(out.parent, today, d / "surge" / "prior.npz")
        if prior is None:
            log.warning("surge: no prior-day curve; scanner off")
            return None
        log.info("surge: prior %s from %s (%d codes, %.1fs)", prior.day, prior.source,
                 len(prior.codes), time.monotonic() - t0)
        return SurgeScanner(prior, d / "surge", Pusher.from_env(out / "SURGE_NOPUSH"))
    except Exception as exc:  # noqa: BLE001
        log.warning("surge scanner init failed: %s", str(exc)[:200])
        return None


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(prog="rquant.research.snapshots")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--out", type=Path, default=None)
    r.add_argument("--force", action="store_true", help="ignore calendar/session (dry run)")
    r.add_argument("--max-cycles", type=int, default=None)
    r.add_argument("--cap", type=int, default=DEFAULT_CAP)
    r.add_argument("--no-ticks", action="store_true")
    r.add_argument("--tick-limit", type=int, default=None)
    u = sub.add_parser("universe")
    u.add_argument("--cap", type=int, default=DEFAULT_CAP)
    c = sub.add_parser("compact")
    c.add_argument("day_dir", type=Path)
    a = p.parse_args(argv)
    if a.cmd == "universe":
        print(json.dumps(build_universe(a.cap), ensure_ascii=False, indent=1))
        return 0
    if a.cmd == "compact":
        print(json.dumps(compact(a.day_dir)))
        return 0
    out = a.out or research_root() / "snapshots"
    return run(out, force=a.force, max_cycles=a.max_cycles, cap=a.cap, ticks=not a.no_ticks,
               tick_limit=a.tick_limit)


if __name__ == "__main__":
    raise SystemExit(main())
