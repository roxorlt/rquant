"""rQuant 五虎 live endpoint (standalone, read-only on research files).

GET /api/v1/wuhu/live   (Authorization: Bearer <WUHU_TOKEN>)
GET /api/v1/wuhu/health (no auth, no data)

Sources (all read-only):
  pools/auction/<date>.json          -> picks (top 5 by rank = auction volume)
  snapshots/<date>/quotes/part-*.parquet  -> open / backfill minute series (30s parts)
  snapshots/<date>/stream/d-*.parquet     -> ~3s per-round deltas (price,time) for freshness
  alerts/<date>.jsonl                -> latest alerts (time,ts_code,name,type,level,text,pushed)
"""
import glob, hmac, json, os, threading, time, datetime as dt
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pyarrow.parquet as pq

ROOT = os.path.expanduser(os.environ.get("WUHU_RESEARCH_ROOT", "~/rquant-research"))
ALERTS_DIR = os.environ.get("WUHU_ALERTS_DIR", os.path.join(ROOT, "alerts"))
TOKEN = os.environ.get("WUHU_TOKEN", "")
PORT = int(os.environ.get("WUHU_PORT", "8775"))
N = 5
BULL = {"收复支撑", "突破阻力", "大单托底", "压单撤离", "主动买转强", "逼近涨停", "封涨停", "回封"}
BEAR = {"跌破支撑", "放量下杀", "大单压盘", "托单撤离", "主动卖转强", "炸板", "封跌停"}
SHORT = {"压单撤离": "压单撤", "托单撤离": "托单撤", "主动买转强": "主买强", "主动卖转强": "主卖强"}


def bias_of(t):
    if t in BULL: return "多"
    if t in BEAR: return "空"
    return "中"


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.date = None
        self.reset(None)

    def reset(self, date):
        self.date = date
        self.pool_mtime = 0
        self.picks = []          # list of dicts
        self.codes = []
        self.seen_parts = set()
        self.last_stream = ""
        self.q = {}              # code -> dict(price,open,pre_close,time,minutes{HH:MM:price})
        self.feed_time = None    # latest tick time seen (HH:MM:SS)
        self.feed_wall = 0.0     # wall clock when last new data ingested

    def _pool_path(self, date):
        return os.path.join(ROOT, "pools", "auction", f"{date}.json")

    def load_pool(self):
        p = self._pool_path(self.date)
        try:
            m = os.path.getmtime(p)
        except OSError:
            return
        if m == self.pool_mtime: return
        d = json.load(open(p, encoding="utf-8"))
        picks = sorted(d.get("picks") or [], key=lambda x: x.get("rank", 99))[:N]
        codes = [x["ts_code"] for x in picks]
        self.pool_mtime = m
        self.pool_meta = {k: d.get(k) for k in ("date", "stage", "locked_at", "frozen", "updated_at")}
        if codes != self.codes:
            self.picks, self.codes = picks, codes
            self.seen_parts, self.last_stream, self.q = set(), "", {}
        else:
            self.picks = picks

    def _ingest(self, rows, has_open):
        changed = False
        for r in rows:
            c, t, px = r["ts_code"], r["time"] or "", r["price"]
            if not px or px <= 0 or not t: continue
            s = self.q.setdefault(c, {"price": None, "open": None, "pre_close": None, "time": "", "minutes": {}})
            if has_open and r.get("open") and not s["open"]: s["open"] = r["open"]
            if r.get("pre_close"): s["pre_close"] = r["pre_close"]
            if t >= s["time"]:
                s["price"], s["time"] = px, t
                changed = True
            hm = t[:5]
            if "09:30" <= hm <= "15:00" and not ("11:30" < hm < "13:00"):
                s["minutes"][hm] = px  # rows in time order -> last wins
            if self.feed_time is None or t > self.feed_time: self.feed_time = t
        if changed: self.feed_wall = time.time()

    def tick(self):
        today = dt.date.today().isoformat()
        if self.date != today:
            # fall back to latest pool file (weekends/holidays)
            if not os.path.exists(self._pool_path(today)):
                fs = sorted(glob.glob(os.path.join(ROOT, "pools", "auction", "????-??-??.json")))
                today = os.path.basename(fs[-1])[:10] if fs else today
            if self.date != today: self.reset(today)
        self.load_pool()
        if not self.codes: return
        snap = os.path.join(ROOT, "snapshots", self.date)
        flt = [("ts_code", "in", self.codes)]
        for f in sorted(glob.glob(os.path.join(snap, "quotes", "part-*.parquet"))):
            if f in self.seen_parts: continue
            try:
                t = pq.read_table(f, columns=["ts_code", "time", "price", "open", "pre_close"], filters=flt)
            except Exception:
                continue  # possibly being written; retry next tick
            rows = sorted(t.to_pylist(), key=lambda r: r["time"] or "")
            self._ingest(rows, True)
            self.seen_parts.add(f)
        for f in sorted(glob.glob(os.path.join(snap, "stream", "d-*.parquet"))):
            b = os.path.basename(f)
            if b <= self.last_stream: continue
            try:
                t = pq.read_table(f, columns=["ts_code", "time", "price", "pre_close"], filters=flt)
            except Exception:
                break
            self._ingest(sorted(t.to_pylist(), key=lambda r: r["time"] or ""), False)
            self.last_stream = b

    def alerts(self, limit=20):
        p = os.path.join(ALERTS_DIR, f"{self.date}.jsonl")
        out = []
        try:
            with open(p, "rb") as fh:
                fh.seek(0, 2); size = fh.tell(); fh.seek(max(0, size - 256 * 1024))
                lines = fh.read().decode("utf-8", "replace").splitlines()
        except OSError:
            return out, False
        for ln in lines:
            try: a = json.loads(ln)
            except Exception: continue
            ty = a.get("type", "")
            out.append({"time": str(a.get("time", ""))[-8:], "ts_code": a.get("ts_code"), "name": a.get("name"),
                        "type": ty, "short": SHORT.get(ty, ty), "level": a.get("level"), "bias": bias_of(ty),
                        "text": a.get("text", ""), "in_wuhu": a.get("ts_code") in self.codes})
        out.sort(key=lambda a: a["time"])
        return out[-limit:][::-1], True

    def replay_alerts(self):
        """Today's real alerts (alerts/<date>.jsonl, else engine backtest bt_wuhu_summary.json) for replay."""
        raw = []
        p = os.path.join(ALERTS_DIR, f"{self.date}.jsonl")
        if os.path.exists(p):
            for ln in open(p, encoding="utf-8", errors="replace"):
                try: raw.append(json.loads(ln))
                except Exception: pass
        if not raw:
            try:
                b = json.load(open(os.path.join(ROOT, "intraday_alerts", "bt_wuhu_summary.json"), encoding="utf-8"))
                raw = [a for a in b.get("alerts", []) if a.get("day") == self.date]
            except Exception: raw = []
        raw = [a for a in raw if a.get("ts_code") in self.codes] or raw
        raw.sort(key=lambda a: str(a.get("time", "")))
        return raw

    def payload(self, replay=False, step_s=8.0):
        rows = []
        clock = None
        if replay:
            raw = self.replay_alerts()
            if raw:
                step = int(time.time() // step_s); n = len(raw)
                loop, k = divmod(step, n)
                shown = raw[:k + 1]
                clock = str(shown[-1].get("time", ""))[-8:]
                al = []
                for i, a in enumerate(shown):
                    ty = a.get("type", "")
                    al.append({"time": str(a.get("time", ""))[-8:], "ts_code": a.get("ts_code"), "name": a.get("name"),
                               "type": ty, "short": SHORT.get(ty, ty), "level": a.get("level"), "bias": bias_of(ty),
                               "text": a.get("text", ""), "in_wuhu": True, "seq": loop * 10000 + i})
                al = al[::-1][:20]
            else:
                al = []
            have_alerts = bool(raw)
        else:
            al, have_alerts = self.alerts()
        latest_bias = {}
        for a in reversed(al):  # oldest -> newest so newest wins
            if a["bias"] != "中": latest_bias[a["ts_code"]] = a["bias"]
        for p in self.picks:
            c = p["ts_code"]; s = self.q.get(c, {})
            px, op, pc = s.get("price"), s.get("open"), s.get("pre_close") or p.get("y_close")
            mins = s.get("minutes", {})
            if clock:
                mins = {k: v for k, v in mins.items() if k <= clock[:5]}
                if mins: px = mins[max(mins)]
            rows.append({
                "rank": p.get("rank"), "name": p.get("name"), "ts_code": c,
                "gap": round(p["gap"] * 100, 2) if p.get("gap") is not None else None,
                "price": px, "open": op, "pre_close": pc,
                "pct_chg": round((px / pc - 1) * 100, 2) if px and pc else None,
                "intraday": round((px / op - 1) * 100, 2) if px and op else None,
                "tick_time": clock or s.get("time"),
                "spark": [mins[k] for k in sorted(mins)],
                "bias": latest_bias.get(c),
            })
        now = dt.datetime.now()
        age = None
        if self.feed_time and self.date == now.date().isoformat():
            try:
                ft = dt.datetime.combine(now.date(), dt.time.fromisoformat(self.feed_time))
                age = round((now - ft).total_seconds(), 1)
            except ValueError: pass
        if clock: self_feed = clock
        else: self_feed = self.feed_time
        return {"replay": bool(clock), "date": self.date, "server_time": now.strftime("%H:%M:%S"), "feed_time": self_feed,
                "feed_age_s": age, "pool": getattr(self, "pool_meta", {}), "wuhu": rows,
                "alerts": al, "alerts_available": have_alerts,
                "app_url": "http://82.156.0.68:8081/app/"}


S = State()


def loop():
    while True:
        t0 = time.time()
        try:
            with S.lock: S.tick()
        except Exception as e:
            print("tick error:", repr(e), flush=True)
        time.sleep(max(0.2, 1.0 - (time.time() - t0)))


class H(BaseHTTPRequestHandler):
    server_version = "wuhu/1"
    def log_message(self, *a): pass
    def _send(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        if path == "/api/v1/wuhu/health":
            return self._send(200, {"ok": True, "date": S.date, "feed_time": S.feed_time})
        if path != "/api/v1/wuhu/live":
            return self._send(404, {"error": "not found"})
        auth = self.headers.get("Authorization", "")
        tok = auth[7:] if auth.startswith("Bearer ") else ""
        if not TOKEN or not hmac.compare_digest(tok.encode(), TOKEN.encode()):
            return self._send(401, {"error": "unauthorized"})
        q = self.path.partition("?")[2]
        with S.lock:
            body = S.payload(replay="replay=1" in q)
        self._send(200, body)


if __name__ == "__main__":
    if not TOKEN: raise SystemExit("WUHU_TOKEN not set")
    with S.lock: S.tick()
    threading.Thread(target=loop, daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
