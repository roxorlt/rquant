# ruff: noqa: N806, E741  - C/H/O/V panels mirror the TDX names
"""Two intraday studies on the research DBs, saved in the event-study JSON shape.

1. 黄金坑 at 14:50 (``hjk-1450``): the 黄金坑 formula with today's C/H/V replaced by
   14:50 proxies (price at 14:50, high so far, volume so far scaled to a full day by the
   stock's own trailing-20-day median share of volume traded by 14:50, lagged one day).
   Entry at the 14:50 price; exits at T+1/T+2/T+3/T+5 close and T+1 open.
2. 竞价选股 (``auction-ths``): yesterday closed at limit-up and was not a one-price board
   (high > low), today's 09:25 auction price > +4% vs yesterday's close, no ST/退市;
   ranked by auction volume. Buy at the open (= auction price).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from rquant.research.ingest import db_path
from rquant.research.intraday import intraday_path
from rquant.research.tdx_strategy import BENCH, study_path

TOP_N = 5


def _stats(r: pd.Series) -> dict:
    r = r.dropna()
    if r.empty:
        return {"n": 0, "mean": 0.0, "median": 0.0, "win_rate": 0.0}
    return {"n": int(len(r)), "mean": float(r.mean()), "median": float(r.median()),
            "win_rate": float((r > 0).mean())}


def _pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def _limit(pre: pd.Series, pct: pd.Series) -> pd.Series:
    return (pre * (1 + pct) + 1e-9).round(2)


def _panel(con, sql: str, params: list, value: str) -> pd.DataFrame:
    df = con.execute(sql, params).df()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.pivot(index="trade_date", columns="ts_code", values=value).sort_index()


def _save(root: Path, slug: str, body: dict) -> dict:
    body |= {"slug": slug, "generated_at": datetime.now(UTC).isoformat()}
    p = study_path(root, slug)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(body, ensure_ascii=False, indent=1))
    return body


# ---- 1. 黄金坑 at 14:50 ---------------------------------------------------------


WINDOW_ROWS = 140  # history rows per evaluation (MA60/LLV/EMA warm-up)


def intraday_study(root: Path, slug: str, title: str, formula: str, eod_preset: str) -> dict:
    """Any TDX formula evaluated as of 14:50: for each day the panel is the real history
    plus that day's 14:50 proxies (close/high/low so far, volume/amount scaled to a full
    day), so nothing after 14:50 leaks in. Entry at the 14:50 price."""
    from rquant.factor.expr import FIELDS, evaluate
    from rquant.factor.tdx import translate

    expr = translate(formula)
    icon = duckdb.connect(str(intraday_path(root)), read_only=True)
    b = icon.execute("SELECT * FROM bar_1450").df()
    icon.close()
    b["trade_date"] = pd.to_datetime(b["trade_date"])
    start, end = b["trade_date"].min(), b["trade_date"].max()
    con = duckdb.connect(str(db_path(root)), read_only=True)
    w0 = start - pd.Timedelta(days=250)
    q = "FROM {t} WHERE trade_date BETWEEN ? AND ? AND ts_code NOT LIKE '%.BJ'"
    panel = {f: _panel(con, f"SELECT trade_date, ts_code, {f} {q.format(t='daily_bar_hfq')}",
                       [w0, end], f) for f in FIELDS}
    C = panel["close"]
    for f in FIELDS:
        panel[f] = panel[f].reindex_like(C)
    A = _panel(con, f"SELECT trade_date, ts_code, adj_factor {q.format(t='adj_factor')}",
               [w0, end], "adj_factor").reindex_like(C)
    raw = {f: _panel(con, f"SELECT trade_date, ts_code, {f} {q.format(t='daily_bar')}",
                     [w0, end], f).reindex_like(C) for f in ("pre_close", "vol", "amount")}
    lim = _panel(con, f"SELECT trade_date, ts_code, limit_pct {q.format(t='daily_state')}",
                 [w0, end], "limit_pct").reindex_like(C).fillna(0.1)
    st = {c for (c,) in con.execute(
        "SELECT ts_code FROM stock_basic WHERE name LIKE '%ST%' OR name LIKE '%退%'").fetchall()}
    idx = _panel(con, "SELECT trade_date, ts_code, close FROM index_daily "
                 "WHERE trade_date BETWEEN ? AND ?", [w0, end], "close").reindex(C.index)
    eod = con.execute("SELECT trade_date, ts_code FROM screen_result WHERE preset_name = ?",
                      [eod_preset]).df()
    con.close()
    panel["vol"], panel["amount"] = raw["vol"], raw["amount"]  # volume is not adjusted

    def piv(col: str) -> pd.DataFrame:
        return b.pivot(index="trade_date", columns="ts_code", values=col).reindex_like(C)

    p, h, lo, v, amt, vday = (piv(c) for c in ("p1450", "h1450", "l1450", "v1450", "a1450",
                                                "vday"))
    share = (v / vday).where(vday > 0)
    frac = share.shift(1).rolling(20, min_periods=5).median()
    frac = frac.fillna(float(np.nanmedian(share.to_numpy())))
    vunit = float(np.nanmedian((vday / raw["vol"]).to_numpy()))
    aunit = float(np.nanmedian((amt / share / raw["amount"]).to_numpy()))
    proxy = {"close": p * A, "high": h * A, "low": lo * A, "vol": v / frac / vunit,
             "amount": amt / frac / aunit}
    proxy["pct_chg"] = (p / raw["pre_close"] - 1) * 100
    sealed = p >= _limit(raw["pre_close"], lim) - 1e-6
    days = [d for d in C.index if start <= d <= end]
    pos = {d: i for i, d in enumerate(C.index)}
    fired: list[tuple] = []
    for d in days:
        i = pos[d]
        sl = {f: x.iloc[max(0, i - WINDOW_ROWS + 1): i + 1].copy() for f, x in panel.items()}
        have = proxy["close"].loc[d].notna()
        for f, x in proxy.items():
            sl[f].iloc[-1] = x.loc[d].where(have)
        row = evaluate(expr, sl).iloc[-1]
        ok = row[(row > 0.5) & have & ~sealed.loc[d].fillna(False)]
        fired += [(d, c) for c in ok.index if c not in st]
    hits = pd.DataFrame(fired, columns=["trade_date", "ts_code"])
    rows = []
    for d, code in fired:
        i, e = pos[d], proxy["close"].at[d, code]
        rec = {"trade_date": d, "ts_code": code}
        for k in (1, 2, 3, 5):
            if i + k < len(C.index):
                rec[f"c{k}"] = C[code].iat[i + k] / e - 1
                for name, ic in BENCH.items():
                    if ic in idx:
                        bench = idx[ic].iat[i + k] / idx[ic].iat[i] - 1
                        rec[f"x{k}_{name}"] = rec[f"c{k}"] - bench
        if i + 1 < len(C.index):
            rec["o1"] = panel["open"][code].iat[i + 1] / e - 1
        rows.append(rec)
    r = pd.DataFrame(rows)
    eod_set = {(pd.Timestamp(d), c) for d, c in zip(eod["trade_date"], eod["ts_code"],
                                                    strict=True)}
    in_window = {k for k in eod_set if start <= k[0] <= end}
    both = sum((d, c) in eod_set for d, c in fired)
    horizons = {}
    for label, col in (("T+1 开盘", "o1"), ("T+1 收盘", "c1"), ("T+2 收盘", "c2"),
                       ("T+3 收盘", "c3"), ("T+5 收盘", "c5")):
        if col not in r:
            continue
        hz = _stats(r[col])
        k = col[1:]
        if col.startswith("c"):
            hz["excess_mean"] = {n: float(r[f"x{k}_{n}"].mean()) for n in BENCH
                                 if f"x{k}_{n}" in r}
            hz["excess_win_rate"] = {n: float((r[f"x{k}_{n}"] > 0).mean()) for n in BENCH
                                     if f"x{k}_{n}" in r}
        horizons[label] = hz
    return _save(root, slug, {
        "title": title, "formula": formula,
        "window": [str(start.date()), str(end.date())], "signals": int(len(hits)),
        "per_year": {str(y): int(n) for y, n in
                     hits.groupby(hits["trade_date"].dt.year).size().items()} if len(hits) else {},
        "horizons": horizons,
        "notes": [f"覆盖：{b['ts_code'].nunique()} 只（全 A 非北交所）5 分钟线 "
                  f"{start.date()}~{end.date()}",
                  "量能代理：14:50 累计量 ÷ 该股前 20 日（滞后一日）14:50 累计量占全天比例中位数",
                  f"与收盘版信号重合：{both}/{len(hits)}（收盘版同期 {len(in_window)} 个）",
                  "14:50 已封涨停的样本剔除（买不进）"],
        "caveats": ["ST/退市按当前名称过滤（前视/幸存者偏差）",
                    "按 14:50 收盘价成交，未计冲击成本与费用",
                    "指数超额以信号日收盘为基准", "成交量未复权",
                    f"EMA 每次只用最近 {WINDOW_ROWS} 根日线预热"]})


INTRADAY_STUDIES = [("hjk-1450", "黄金坑 14:50 盘中版", "HJK", "tdx_hjk-hold10"),
                    ("lyt-1450", "老鸭头 14:50 盘中版", "LYT", "tdx_lyt-hold10")]


def run_intraday(root: Path) -> None:
    from rquant.research import tdx_strategy

    for slug, title, attr, preset in INTRADAY_STUDIES:
        intraday_study(root, slug, title, getattr(tdx_strategy, attr), preset)


# ---- 2. auction screen ------------------------------------------------------------


FEATURES = {
    "auc_vol_ratio": "竞价量/昨日量", "auc_turnover": "竞价换手率", "float_mv": "流通市值",
    "boards": "连板数", "first_min": "昨日首次封板时间(分)", "open_times": "昨日开板次数",
    "seal_ratio": "昨日封单/流通市值", "sector_ups": "昨日同行业涨停数",
    "market_ups": "昨日全市场涨停数", "gap": "竞价涨幅",
}


def auction_frame(root: Path) -> pd.DataFrame:
    icon = duckdb.connect(str(intraday_path(root)), read_only=True)
    auc = icon.execute("SELECT * FROM auction").df()
    ll = icon.execute("SELECT * FROM limit_list").df()
    icon.close()
    con = duckdb.connect(str(db_path(root)), read_only=True)
    d0 = auc["trade_date"].min()
    bars = con.execute(
        "SELECT b.ts_code, b.trade_date, b.open, b.high, b.low, b.close, b.pre_close, b.vol, "
        "COALESCE(s.limit_pct, 0.1) AS lp, a.adj_factor FROM daily_bar b "
        "LEFT JOIN daily_state s USING (ts_code, trade_date) "
        "LEFT JOIN adj_factor a USING (ts_code, trade_date) "
        "WHERE b.trade_date >= ? - INTERVAL 10 DAY", [d0]).df()
    basic = con.execute("SELECT ts_code, name, industry FROM stock_basic").df()
    mv = con.execute("SELECT ts_code, trade_date, circ_mv FROM daily_basic "
                     "WHERE trade_date >= ? - INTERVAL 10 DAY", [d0]).df()
    con.close()
    bars = bars.sort_values(["ts_code", "trade_date"])
    g = bars.groupby("ts_code")
    for col in ("high", "low", "close", "pre_close", "vol", "lp"):
        bars[f"y_{col}"] = g[col].shift(1)
    bars["n_open"], bars["n_close"] = g["open"].shift(-1), g["close"].shift(-1)
    bars["n_adj"], bars["y_trade_date"] = g["adj_factor"].shift(-1), g["trade_date"].shift(1)
    bars["limit"] = _limit(bars["pre_close"], bars["lp"])
    bars["y_limit"] = _limit(bars["y_pre_close"], bars["y_lp"])
    df = auc.merge(bars, on=["ts_code", "trade_date"], how="inner").merge(basic, on="ts_code")
    df = df[(df["y_close"] >= df["y_limit"] - 1e-6) & (df["y_high"] > df["y_low"])
            & (df["price"] / df["y_close"] - 1 > 0.04)
            & ~df["name"].str.contains("ST|退", na=False)
            & ~df["ts_code"].str.endswith(".BJ")]
    ll["first_min"] = pd.to_numeric(ll["first_time"].astype(str).str[:4].where(
        ll["first_time"].notna()), errors="coerce")
    ll["first_min"] = (ll["first_min"] // 100) * 60 + ll["first_min"] % 100 - 570
    ups = ll[ll["lim"] == "U"]
    mkt = ups.groupby("trade_date").size().rename("market_ups")
    sec = ups.groupby(["trade_date", "industry"]).size().rename("sector_ups")
    y = ups.rename(columns={"trade_date": "y_trade_date"})[
        ["ts_code", "y_trade_date", "limit_times", "first_min", "open_times", "fd_amount",
         "float_mv"]]
    df = df.merge(y, on=["ts_code", "y_trade_date"], how="left")
    df = df.merge(mkt.rename_axis("y_trade_date").reset_index(), on="y_trade_date", how="left")
    df = df.merge(sec.reset_index().rename(columns={"trade_date": "y_trade_date"}),
                  on=["y_trade_date", "industry"], how="left")
    df = df.merge(mv.rename(columns={"trade_date": "y_trade_date"}),
                  on=["ts_code", "y_trade_date"], how="left")
    df["auc_vol_ratio"] = df["vol_x"] / (df["y_vol"] * 100) if "vol_x" in df else np.nan
    df["auc_turnover"] = df["turnover_rate"]
    df["float_mv"] = df["circ_mv"].fillna(df["float_mv"] / 1e4)  # 万元
    df["boards"] = df["limit_times"]
    df["seal_ratio"] = df["fd_amount"] / (df["float_mv"] * 1e4)
    df["gap"] = df["price"] / df["y_close"] - 1
    df["rank"] = df.groupby("trade_date")["vol_x"].rank(ascending=False, method="first")
    # outcomes: buy at the open (auction price)
    df["hit_up"] = df["high"] >= df["limit"] - 1e-6
    df["close_up"] = df["close"] >= df["limit"] - 1e-6
    df["r_close"] = df["close"] / df["open"] - 1
    adj_ratio = df["n_adj"] / df["adj_factor"]
    df["r_n_open"] = df["n_open"] * adj_ratio / df["open"] - 1
    df["r_n_close"] = df["n_close"] * adj_ratio / df["open"] - 1
    df["one_word"] = df["open"] >= df["limit"] - 1e-6  # opened sealed: not buyable
    return df


def _outcome(df: pd.DataFrame) -> dict:
    b = df[~df["one_word"]]
    return {"n": int(len(b)), "days": int(b["trade_date"].nunique()),
            "hit_up": float(b["hit_up"].mean()) if len(b) else 0.0,
            "close_up": float(b["close_up"].mean()) if len(b) else 0.0,
            "r_close": float(b["r_close"].mean()) if len(b) else 0.0,
            "r_n_open": float(b["r_n_open"].mean()) if len(b) else 0.0,
            "r_n_close": float(b["r_n_close"].mean()) if len(b) else 0.0,
            "win_n_open": float((b["r_n_open"] > 0).mean()) if len(b) else 0.0,
            "win_n_close": float((b["r_n_close"] > 0).mean()) if len(b) else 0.0}


def _row(name: str, o: dict) -> list[str]:
    return [name, str(o["n"]), _pct(o["hit_up"]), _pct(o["close_up"]), _pct(o["r_close"]),
            _pct(o["r_n_open"]), _pct(o["win_n_open"]), _pct(o["r_n_close"]),
            _pct(o["win_n_close"])]


COLS = ["规则", "样本", "盘中触板", "收盘涨停", "开→收", "开→次开", "次开胜率", "开→次收",
        "次收胜率"]


def _apply(df: pd.DataFrame, rule: list[tuple[str, str, float]]) -> pd.DataFrame:
    m = pd.Series(True, index=df.index)
    for f, op, x in rule:
        m &= (df[f] >= x) if op == ">=" else (df[f] <= x)
    out = df[m].copy()
    out["rank"] = out.groupby("trade_date")["vol_x"].rank(ascending=False, method="first")
    return out[out["rank"] <= TOP_N]


def search_rules(df: pd.DataFrame, split: pd.Timestamp, min_n: int = 120) -> list[dict]:
    """Single and paired tertile thresholds, chosen on train win rate (T+1 open); a rule
    must keep >= min_n train picks and is reported on the untouched test period."""
    train = df[df["trade_date"] < split]
    cands: list[list[tuple[str, str, float]]] = []
    for f in FEATURES:
        x = train[f].dropna()
        if x.nunique() < 3:
            continue
        for q in (1 / 3, 2 / 3):
            t = float(x.quantile(q))
            cands += [[(f, ">=", t)], [(f, "<=", t)]]
    singles = cands[:]
    for i, a in enumerate(singles):
        for bb in singles[i + 1:]:
            if a[0][0] != bb[0][0]:
                cands.append(a + bb)
    scored = []
    for rule in cands:
        o = _outcome(_apply(train, rule))
        if o["n"] >= min_n:
            scored.append((o["win_n_open"], o["r_n_open"], rule))
    scored.sort(key=lambda s: (s[0], s[1]), reverse=True)
    picked: list[dict] = []
    for _w, _r, rule in scored:
        feats = {f for f, _, _ in rule}
        if any(feats == {f for f, _, _ in p["rule"]} for p in picked):
            continue
        picked.append({"rule": rule, "train": _outcome(_apply(train, rule)),
                       "test": _outcome(_apply(df[df["trade_date"] >= split], rule))})
        if len(picked) == 3:
            break
    return picked


def _rule_text(rule: list[tuple[str, str, float]]) -> str:
    return " 且 ".join(f"{FEATURES[f]}{op}{x:.4g}" for f, op, x in rule)


def auction_study(root: Path) -> dict:
    df = auction_frame(root)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    days = sorted(df["trade_date"].unique())
    split = days[int(len(days) * 0.6)] if days else pd.Timestamp("2100-01-01")
    top = df[df["rank"] <= TOP_N]
    rules = search_rules(df, split)
    base_rows = [_row("全部候选", _outcome(df)), _row(f"成交量前 {TOP_N}", _outcome(top)),
                 _row(f"前 {TOP_N}·训练", _outcome(top[top["trade_date"] < split])),
                 _row(f"前 {TOP_N}·测试", _outcome(top[top["trade_date"] >= split]))]
    rule_rows = []
    for i, r in enumerate(rules, 1):
        rule_rows += [_row(f"规则{i} 训练：{_rule_text(r['rule'])}", r["train"]),
                      _row(f"规则{i} 测试", r["test"])]
    one_word = float(top["one_word"].mean()) if len(top) else 0.0
    return _save(root, "auction-ths", {
        "title": "竞价选股（同花顺条件复刻）", "formula":
            "昨日涨停且非一字 + 今日竞价涨幅>4% + 非ST/退市，按竞价量降序",
        "window": [str(days[0].date()) if days else "", str(days[-1].date()) if days else ""],
        "signals": int(len(top)),
        "per_year": {str(y): int(n) for y, n in top.groupby(top["trade_date"].dt.year).size()
                     .items()},
        "horizons": {"T+1 开盘": _stats(top[~top["one_word"]]["r_n_open"]),
                     "T+1 收盘": _stats(top[~top["one_word"]]["r_n_close"])},
        "tables": [{"title": f"基线（每日前 {TOP_N}，开盘买入；一字开盘剔除 {_pct(one_word)}）",
                    "columns": COLS, "rows": base_rows},
                   {"title": f"加条件（训练 < {split.date()} ≤ 测试；"
                             "三分位阈值，按训练次开胜率选）",
                    "columns": COLS, "rows": rule_rows}],
        "rules": [{"rule": [list(x) for x in r["rule"]], "train": r["train"],
                   "test": r["test"]} for r in rules],
        "notes": ["竞价数据：Tushare stk_auction（09:25 集合竞价真实成交）",
                  "连板/封板时间/封单/开板：limit_list_d", "开→收 当日不可卖（T+1），仅作参考"],
        "caveats": ["ST/退市按当前名称过滤", "未计费用与冲击成本（竞价量小的股票尤其明显）",
                    "阈值在训练期选出，测试期只看一次；候选规则数多，仍有数据挖掘偏差",
                    "开盘价=竞价价，一字开盘视为买不进"]})


def latest_picks(root: Path, rule: list | None = None) -> tuple:
    """(trade_date, universe, hits) for the newest auction day, for the condition panel."""
    from rquant.factor.condition import ConditionHit

    df = auction_frame(root)
    if df.empty:
        return None, 0, []
    last = df["trade_date"].max()
    day = df[df["trade_date"] == last]
    day = _apply(day, [tuple(x) for x in rule]) if rule else day[day["rank"] <= TOP_N]
    hits = [ConditionHit(code=r.ts_code, close=float(r.close), pct_chg=float(r.gap * 100))
            for r in day.sort_values("rank").itertuples()]
    return pd.Timestamp(last).date(), int(len(df[df["trade_date"] == last])), hits


def signals_for_backtest(root: Path) -> pd.DataFrame:
    """Daily top-N picks keyed on the PREVIOUS trading day, so the engine's next-open
    fill lands on the auction day's open."""
    df = auction_frame(root)
    top = df[df["rank"] <= TOP_N]
    return pd.DataFrame({"trade_date": top["y_trade_date"], "ts_code": top["ts_code"],
                         "score": TOP_N + 1 - top["rank"]})
