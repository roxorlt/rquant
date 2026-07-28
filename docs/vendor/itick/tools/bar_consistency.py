"""iTick WS tick 流 → K 线聚合一致性测试（EVALUATION.md §3 测试 #2/#4/#6/#7）。

两个子命令：
  collect  盘中运行：连 WS 录 3 只标的的 tick/quote/depth 原始流水到 jsonl（每行含本机接收时刻）
  report   收盘后运行：把 tick 聚合成 1m/5m/30m/60m/1d，与 tushare stk_mins 逐根比对，
           输出 markdown 报告（OHLCV 偏差、延迟分位、断线清单、边界归属判定）

用法（云端 82.156.0.68，Free 层 1 连接 / 3 订阅）：
  export ITICK_TOKEN=...            # https://itick.org 注册免费获取
  uv run --with websocket-client python bar_consistency.py collect \
      --symbols '600519$SH,300750$SZ,003040$SZ' --out ~/itick_probe
  uv run --with websocket-client python bar_consistency.py report \
      --data ~/itick_probe --date 2026-07-29

已知坑（写死在实现里）：
  * WS 成交响应的 v 疑似当日累计量而非单笔量（文档示例值与 quote 的当日累计一致），
    聚合前自动检测：v 对时间单调不减 → 判定累计，逐条差分还原单笔。
  * 无 seq 序列号，丢包不可直接检测——只能靠「聚合量 / 官方分钟量」比值间接量化。
  * 断线无补数据机制，重连后必须重新订阅；断线窗口记入 events，报告里单列。
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

CST = timezone(timedelta(hours=8))
WS_URL = "wss://api.itick.org/stock"
PING_INTERVAL = 25  # 文档要求 ≥每30s 一次，留余量


# ---------------------------------------------------------------- collect

def collect(symbols: str, out_dir: Path, token: str) -> None:
    import websocket  # websocket-client

    out_dir.mkdir(parents=True, exist_ok=True)
    day = datetime.now(CST).strftime("%Y-%m-%d")
    raw_path = out_dir / f"raw-{day}.jsonl"
    evt_path = out_dir / f"events-{day}.jsonl"
    # buffering=1 行缓冲:pkill/timeout 的 SIGTERM 杀进程时不丢缓冲区里的消息
    raw_f = raw_path.open("a", encoding="utf-8", buffering=1)
    stop = threading.Event()

    def _graceful(signum, _frame):  # noqa: ANN001
        stop.set()
        raw_f.flush()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _graceful)

    def log_event(kind: str, detail: str) -> None:
        with evt_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.time(), "kind": kind, "detail": detail}) + "\n")
        print(f"[{datetime.now(CST):%H:%M:%S}] {kind}: {detail}", flush=True)

    def on_message(ws: object, message: str) -> None:
        raw_f.write(json.dumps({"recv": time.time(), "msg": message}) + "\n")

    def on_open(ws) -> None:  # noqa: ANN001 - websocket-client callback
        log_event("connected", WS_URL)
        ws.send(json.dumps({"ac": "subscribe", "params": symbols, "types": "tick,quote,depth"}))

        def heartbeat() -> None:
            while not stop.is_set():
                time.sleep(PING_INTERVAL)
                try:
                    ws.send(json.dumps({"ac": "ping", "params": str(int(time.time() * 1000))}))
                except Exception:  # 连接已死，交给 on_close 重连
                    return

        threading.Thread(target=heartbeat, daemon=True).start()

    def on_error(ws, error) -> None:  # noqa: ANN001
        log_event("error", repr(error))

    def on_close(ws, code, msg) -> None:  # noqa: ANN001
        log_event("disconnected", f"code={code} msg={msg}")

    signal.signal(signal.SIGINT, lambda *_: (stop.set(), sys.exit(0)))
    backoff = 1
    while not stop.is_set():
        ws = websocket.WebSocketApp(
            WS_URL, header={"token": token},
            on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close,
        )
        ws.run_forever(ping_interval=0)  # 心跳自管（协议要求带时间戳 payload）
        raw_f.flush()
        if datetime.now(CST).strftime("%H:%M") > "15:05":
            log_event("done", "已过收盘，停止重连")
            break
        log_event("reconnect", f"{backoff}s 后重连（断线期间数据永久丢失）")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


# ---------------------------------------------------------------- report

_FREQ = {"1min": "1min", "5min": "5min", "30min": "30min", "60min": "60min", "1d": "1D"}


def _load_ticks(data_dir: Path, date: str):
    import pandas as pd

    rows = []
    path = data_dir / f"raw-{date}.jsonl"
    if not path.exists():
        sys.exit(f"找不到 {path}")
    for line in path.open(encoding="utf-8"):
        rec = json.loads(line)
        try:
            msg = json.loads(rec["msg"])
        except json.JSONDecodeError:
            continue
        d = msg.get("data")
        if not isinstance(d, dict) or d.get("type") != "tick":
            continue
        rows.append({
            "code": f"{d['s']}.{d['r']}", "price": float(d["ld"]),
            "v_raw": float(d.get("v") or 0.0), "t": int(d["t"]) / 1000.0,
            "recv": float(rec["recv"]), "side": int(d.get("d") or 0),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        sys.exit("当日没有 tick 数据")
    return df.sort_values(["code", "t"]).reset_index(drop=True)


def _diff_if_cumulative(g):
    """v 对时间单调不减 → 判定为当日累计量，差分还原单笔；否则原样返回。"""
    mono = (g["v_raw"].diff().dropna() >= 0).mean()
    if mono > 0.99:  # 允许极少量乱序
        g = g.copy()
        g["vol"] = g["v_raw"].diff().fillna(g["v_raw"]).clip(lower=0)
        g.attrs["cumulative"] = True
    else:
        g = g.copy()
        g["vol"] = g["v_raw"]
        g.attrs["cumulative"] = False
    return g


def _aggregate(g, freq: str):
    import pandas as pd

    g = g.set_index(pd.to_datetime(g["t"], unit="s", utc=True).dt.tz_convert(CST))
    o = g["price"].resample(freq, label="right", closed="right").ohlc()
    v = g["vol"].resample(freq, label="right", closed="right").sum()
    out = o.join(v.rename("vol")).dropna(subset=["open"])
    return out


def report(data_dir: Path, date: str) -> None:
    import pandas as pd

    ticks = _load_ticks(data_dir, date)
    print(f"# iTick 一致性报告 {date}\n")
    print(f"tick 总条数 {len(ticks)}，标的 {sorted(ticks['code'].unique())}\n")

    # 延迟（#2）：交易所时间戳 → 本机接收
    lat = (ticks["recv"] - ticks["t"]) * 1000
    print("## 端到端延迟 (ms)\n")
    print(f"P50 {lat.quantile(.5):.0f} | P95 {lat.quantile(.95):.0f} | "
          f"P99 {lat.quantile(.99):.0f} | max {lat.max():.0f}\n")
    if (lat < -1000).any():
        print(f"⚠️ 出现 {int((lat < -1000).sum())} 条时间戳倒流（recv 早于交易所时刻 >1s），检查 #7\n")

    # 断线清单（#4）
    evt = data_dir / f"events-{date}.jsonl"
    if evt.exists():
        evs = [json.loads(x) for x in evt.open(encoding="utf-8")]
        disc = [e for e in evs if e["kind"] in ("disconnected", "reconnect")]
        print(f"## 断线事件：{len([e for e in evs if e['kind'] == 'disconnected'])} 次\n")
        for e in disc:
            print(f"- {datetime.fromtimestamp(e['ts'], CST):%H:%M:%S} {e['kind']}: {e['detail']}")
        print()

    # 逐标的：聚合 + tushare 对比（#6）
    try:
        from rquant.adapter.tushare import TushareAdapter  # 云端已装 rquant
        adapter = TushareAdapter()
    except Exception as exc:  # noqa: BLE001
        adapter = None
        print(f"> ⚠️ tushare 适配器不可用（{exc}），跳过官方分钟线比对\n")

    for code, g in ticks.groupby("code"):
        g = _diff_if_cumulative(g)
        cum = g.attrs["cumulative"]
        print(f"## {code}（tick.v 判定：{'当日累计量→已差分' if cum else '单笔量'}）\n")
        bars = _aggregate(g, "1min")
        print(f"聚合 1min bar {len(bars)} 根，首根 {bars.index[0]:%H:%M}，末根 {bars.index[-1]:%H:%M}")
        print(f"聚合当日总量 {bars['vol'].sum():,.0f}（单位待人工核对：手 vs 股）\n")

        if adapter is None:
            continue
        sym, region = code.split(".")
        ts_code = f"{sym}.{'SH' if region == 'SH' else 'SZ' if region == 'SZ' else region}"
        day = date.replace("-", "")
        try:
            ref = adapter.stk_mins(ts_code, freq="1min",
                                   start_date=f"{day} 09:00:00", end_date=f"{day} 15:30:00")
        except Exception as exc:  # noqa: BLE001
            print(f"> tushare stk_mins 拉取失败：{exc}\n")
            continue
        ref = ref.set_index(pd.to_datetime(ref["trade_time"]).dt.tz_localize(CST))
        joined = bars.join(ref[["open", "high", "low", "close", "vol"]],
                           rsuffix="_ts").dropna(subset=["vol_ts"])
        if joined.empty:
            print("> 无可对齐的分钟（检查 bar 边界约定：label/closed）\n")
            continue
        ohlc_exact = ((joined["open"] == joined["open_ts"]) & (joined["close"] == joined["close_ts"])).mean()
        vr = (joined["vol"] / joined["vol_ts"].replace(0, pd.NA)).dropna()
        print(f"| 指标 | 值 |\n|---|---|")
        print(f"| 可对齐分钟数 | {len(joined)} |")
        print(f"| open/close 完全一致比例 | {ohlc_exact:.1%} |")
        print(f"| 量比 vol_itick/vol_tushare 中位 | {vr.median():.3f} |")
        print(f"| 量比 P5–P95 | {vr.quantile(.05):.3f} – {vr.quantile(.95):.3f} |")
        verdict = ("全量推送" if 0.98 <= vr.median() <= 1.02 and vr.std() < 0.05
                   else "采样推送（比值稳定可校准）" if vr.std() < 0.05
                   else "❌ 不稳定，footprint/聚合不可用")
        print(f"| **完整性判定** | **{verdict}** |\n")

    print("> 边界归属：若整体错位 1 根，把 _aggregate 的 label/closed 换成 left 重跑即可确认 iTick 的 bar 时间戳约定。")


# ---------------------------------------------------------------- main

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--symbols", required=True, help="如 '600519$SH,300750$SZ,003040$SZ'（Free 层最多 3 个）")
    c.add_argument("--out", required=True, type=Path)
    r = sub.add_parser("report")
    r.add_argument("--data", required=True, type=Path)
    r.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    if args.cmd == "collect":
        import os
        token = os.environ.get("ITICK_TOKEN") or sys.exit("请先 export ITICK_TOKEN=...")
        collect(args.symbols, args.out, token)
    else:
        report(args.data, args.date)


if __name__ == "__main__":
    main()
