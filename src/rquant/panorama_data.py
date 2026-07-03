"""盘中市场全景页数据获取层（P0）。

所有外部源调用集中在本模块，每个 fetcher 独立 try/except + 空态返回：
akshare 断了页面不炸，UI 侧对空 DataFrame 渲染灰态。

数据源与刷新节奏（TTL 由 UI 层 st.cache_data 控制）：
- S1 新浪全市场快照 ``ak.stock_zh_a_spot``：30s（云端可用，但本页只在本地跑）
- S2 东财板块资金流 ``ak.stock_sector_fund_flow_rank``：60s（云端被屏蔽，仅本地）
- L  本地只读副本（dc_board / dc_board_member / screen_result / pool2_watch）：300s

DuckDB 只走 ``open_readonly_store()``（副本优先），本模块绝不写主库。
"""

from __future__ import annotations

import pandas as pd
from loguru import logger
from pydantic import BaseModel

from rquant.limit_up_pool import to_ts_code
from rquant.state.derive import _classify_board, _detect_st, _limit_pct, _round_half_up
from rquant.storage.duckdb import DuckDBStore, open_readonly_store

PRICE_TOL = 0.01

# sina 快照中文列 → 标准列。缺可选列时补 NaN，缺必需列时整轮返回空。
_SPOT_COLUMN_MAP: dict[str, str] = {
    "名称": "name",
    "最新价": "price",
    "今开": "open",
    "最高": "high",
    "最低": "low",
    "昨收": "pre_close",
    "涨跌幅": "pct_chg",
    "成交量": "volume",
    "成交额": "amount",
}
_SPOT_REQUIRED = ("代码", "名称", "最新价", "昨收")
_SPOT_NUMERIC = ("price", "open", "high", "low", "pre_close", "pct_chg", "volume", "amount")

# 东财资金流列按「包含匹配」定位：列名带 indicator 前缀（今日/5日/10日），
# 前缀漂移不应打断解析（limit_up_pool.py 缺列防御先例）。
_FLOW_FIELD_PATTERNS: dict[str, str] = {
    "board_name": "名称",
    "pct_chg": "涨跌幅",
    "main_net_amount": "主力净流入-净额",
    "main_net_rate": "主力净流入-净占比",
    "leading_stock": "主力净流入最大股",
}
_FLOW_REQUIRED = ("board_name", "main_net_amount")
_FLOW_NUMERIC = ("pct_chg", "main_net_amount", "main_net_rate")


class MarketPulse(BaseModel):
    """市场脉搏计数（全部基于 S1 快照 + 本地涨停价推算）。"""

    total_count: int = 0
    up_count: int = 0
    down_count: int = 0
    flat_count: int = 0
    limit_up_count: int = 0
    limit_down_count: int = 0
    broken_count: int = 0
    up_ratio_pct: float | None = None


# ── S1 新浪全市场快照 ──────────────────────────────────────────────────────────


def _fetch_spot() -> pd.DataFrame:
    import akshare as ak

    return ak.stock_zh_a_spot()


def fetch_market_snapshot() -> pd.DataFrame:
    """拉取全市场快照并归一化为英文列。

    返回列：ts_code, name, price, open, high, low, pre_close, pct_chg,
    volume(股), amount(元)。失败 / 缺必需列 → 空 DataFrame。

    与 ``monitor.fetch_realtime_quotes`` 同源（sina），但那边绑定 watchlist
    语义（入参 ts_codes、返回 dict），全景页需要全市场 DataFrame，故独立实现。
    """
    try:
        raw = _fetch_spot()
    except Exception as e:
        logger.warning(f"全市场快照获取失败（sina 源）: {type(e).__name__}: {e}")
        return pd.DataFrame()
    if raw is None or raw.empty:
        logger.warning("全市场快照返回空")
        return pd.DataFrame()

    missing = set(_SPOT_REQUIRED) - set(raw.columns)
    if missing:
        logger.warning(f"全市场快照列异常，缺 {sorted(missing)}，本轮跳过")
        return pd.DataFrame()

    out = pd.DataFrame(index=raw.index)
    # sina 代码带前缀 sh/sz/bj，取后 6 位映射 ts_code
    out["ts_code"] = raw["代码"].astype(str).str[-6:].map(to_ts_code)
    for src, dst in _SPOT_COLUMN_MAP.items():
        out[dst] = raw[src] if src in raw.columns else None

    dropped = int(out["ts_code"].isna().sum())
    if dropped:
        logger.debug(f"全市场快照 {dropped} 行代码段无法映射交易所，已丢弃")
    out = out[out["ts_code"].notna()].copy()

    for col in _SPOT_NUMERIC:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    # pct_chg 缺失时用 price/pre_close 兜底
    fallback = (out["price"] / out["pre_close"] - 1) * 100
    out["pct_chg"] = out["pct_chg"].fillna(fallback)
    return out.reset_index(drop=True)


# ── L1 涨停价推算（复用 state/derive 的规则，昨收来自快照） ─────────────────────


def add_limit_prices(snapshot: pd.DataFrame) -> pd.DataFrame:
    """按昨收 + 板块/ST 分档推算今日涨停/跌停价。

    daily_state 副本只有到昨日的行，今日涨停价必须现算：
    limit_up_price = ROUND_HALF_UP(pre_close × (1 + limit_pct))。
    _classify_board/_detect_st/_limit_pct/_round_half_up 全部复用 state/derive，
    保证与日终 daily_state 同一套规则。
    """
    if snapshot.empty:
        return snapshot.copy()
    df = snapshot.copy()
    boards = df["ts_code"].map(_classify_board)
    is_st = df["name"].map(_detect_st)
    limit_pct = pd.Series(
        [_limit_pct(s, b) for s, b in zip(is_st, boards, strict=True)],
        index=df.index,
        dtype="float64",
    )
    df["limit_pct"] = limit_pct
    df["limit_up_price"] = _round_half_up(df["pre_close"] * (1 + limit_pct))
    df["limit_down_price"] = _round_half_up(df["pre_close"] * (1 - limit_pct))
    return df


def compute_market_pulse(snapshot: pd.DataFrame, price_tol: float = PRICE_TOL) -> MarketPulse:
    """从带涨停价的快照算市场脉搏。

    - 停牌票（price/pre_close 缺失或 ≤0）不计入任何分母
    - 涨停：price ≥ limit_up_price − tol（容差 1 分，对齐 derive_state）
    - 炸板：日内最高触过涨停价但现价回落（high ≥ limit_up_price − tol 且未涨停）
    """
    required = {"price", "pre_close", "limit_up_price", "limit_down_price"}
    if snapshot.empty or not required.issubset(snapshot.columns):
        return MarketPulse()

    df = snapshot
    valid = (
        df["price"].notna() & (df["price"] > 0) & df["pre_close"].notna() & (df["pre_close"] > 0)
    )
    d = df[valid]
    if d.empty:
        return MarketPulse()

    is_limit_up = d["price"] >= d["limit_up_price"] - price_tol
    is_limit_down = d["price"] <= d["limit_down_price"] + price_tol
    if "high" in d.columns:
        high = pd.to_numeric(d["high"], errors="coerce")
    else:
        high = pd.Series(float("nan"), index=d.index)
    touched = high.notna() & (high >= d["limit_up_price"] - price_tol)
    broken = touched & ~is_limit_up

    up = int((d["price"] > d["pre_close"]).sum())
    down = int((d["price"] < d["pre_close"]).sum())
    total = int(len(d))
    return MarketPulse(
        total_count=total,
        up_count=up,
        down_count=down,
        flat_count=total - up - down,
        limit_up_count=int(is_limit_up.sum()),
        limit_down_count=int(is_limit_down.sum()),
        broken_count=int(broken.sum()),
        up_ratio_pct=round(up / total * 100, 2) if total else None,
    )


# ── S2 东财板块资金流 ──────────────────────────────────────────────────────────


def _fetch_sector_fund_flow_raw(sector_type: str) -> pd.DataFrame:
    import akshare as ak

    return ak.stock_sector_fund_flow_rank(indicator="今日", sector_type=sector_type)


def fetch_sector_fund_flow(sector_type: str = "行业资金流") -> pd.DataFrame:
    """东财板块资金流排行（今日口径）。

    返回列：board_name, pct_chg(%), main_net_amount(元), main_net_rate(%),
    leading_stock。失败 / 缺必需列 → 空 DataFrame（东财反爬敏感，列名可能漂移）。
    """
    try:
        raw = _fetch_sector_fund_flow_raw(sector_type)
    except Exception as e:
        logger.warning(f"板块资金流获取失败（东财源）: {sector_type} {type(e).__name__}: {e}")
        return pd.DataFrame()
    if raw is None or raw.empty:
        logger.warning(f"板块资金流返回空: {sector_type}")
        return pd.DataFrame()

    resolved: dict[str, str] = {}
    for dst, pattern in _FLOW_FIELD_PATTERNS.items():
        matched = [c for c in raw.columns if pattern in str(c)]
        if matched:
            resolved[dst] = matched[0]
    missing = set(_FLOW_REQUIRED) - set(resolved)
    if missing:
        logger.warning(f"板块资金流列异常，缺 {sorted(missing)}（东财列名可能变更），本轮跳过")
        return pd.DataFrame()

    out = pd.DataFrame(index=raw.index)
    for dst in _FLOW_FIELD_PATTERNS:
        out[dst] = raw[resolved[dst]] if dst in resolved else None
    for col in _FLOW_NUMERIC:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out[out["main_net_amount"].notna()].copy()
    return out.sort_values("main_net_amount", ascending=False).reset_index(drop=True)


# ── L2 东财板块成分（dc_board / dc_board_member，只读副本） ────────────────────


def load_board_members(store: DuckDBStore | None = None) -> pd.DataFrame:
    """读板块成分映射（行业板块 + 概念板块，地域板块噪音大不取）。

    返回列：board_code, board_name, idx_type, con_code。
    副本缺表 / 撞锁 → 空 DataFrame（页面降级提示）。
    """
    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("dc_board", "dc_board_member"))
    except Exception as e:
        logger.warning(f"板块成分只读库打开失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    try:
        return store._conn.execute(
            """
            SELECT m.board_code, b.name AS board_name, b.idx_type, m.con_code
            FROM dc_board_member m
            JOIN dc_board b ON m.board_code = b.ts_code
            WHERE b.idx_type IN ('行业板块', '概念板块')
            """
        ).fetchdf()
    except Exception as e:
        logger.warning(f"板块成分查询失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    finally:
        if owns:
            store.close()


def industry_fallback_members(store: DuckDBStore | None = None) -> pd.DataFrame:
    """stock_basic.industry 粗分兜底（dc 成分表不可用时），列结构对齐 load_board_members。"""
    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("stock_basic",))
    except Exception as e:
        logger.warning(f"行业兜底只读库打开失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    try:
        return store._conn.execute(
            """
            SELECT industry AS board_code, industry AS board_name,
                   '行业板块' AS idx_type, ts_code AS con_code
            FROM stock_basic
            WHERE industry IS NOT NULL AND industry != ''
            """
        ).fetchdf()
    except Exception as e:
        logger.warning(f"行业兜底查询失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    finally:
        if owns:
            store.close()


# ── 板块聚合 / 下钻 ────────────────────────────────────────────────────────────


def aggregate_board_amount(
    snapshot: pd.DataFrame,
    members: pd.DataFrame,
    idx_type: str = "行业板块",
    price_tol: float = PRICE_TOL,
) -> pd.DataFrame:
    """全市场快照按板块成分聚合成交额排行。

    返回列：board_code, board_name, amount(元), pct_chg_median(%),
    limit_up_count, stock_count，按 amount 降序。
    概念板块成分互相重叠，聚合值只在板块内解释、不跨板块求和。
    """
    if snapshot.empty or members.empty:
        return pd.DataFrame()
    m = members[members["idx_type"] == idx_type]
    if m.empty:
        return pd.DataFrame()

    merged = m.merge(snapshot, left_on="con_code", right_on="ts_code", how="inner")
    if merged.empty:
        return pd.DataFrame()
    if "limit_up_price" in merged.columns:
        merged["_is_limit_up"] = (
            merged["price"].notna()
            & (merged["price"] > 0)
            & merged["limit_up_price"].notna()
            & (merged["price"] >= merged["limit_up_price"] - price_tol)
        )
    else:
        merged["_is_limit_up"] = False

    agg = (
        merged.groupby(["board_code", "board_name"], as_index=False)
        .agg(
            amount=("amount", "sum"),
            pct_chg_median=("pct_chg", "median"),
            limit_up_count=("_is_limit_up", "sum"),
            stock_count=("ts_code", "count"),
        )
        .sort_values("amount", ascending=False)
        .reset_index(drop=True)
    )
    agg["limit_up_count"] = agg["limit_up_count"].astype(int)
    return agg


def load_pool_flags(store: DuckDBStore | None = None) -> dict[str, str]:
    """池内标记：{ts_code: 'pool1:预设名' / 'pool2' / 两者拼接}。

    pool1 = screen_result 最新 trade_date 的入选票；pool2 = pool2_watch active。
    只读库不可用 → 空 dict（下钻表池内列全空，不炸页面）。
    """
    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("screen_result", "pool2_watch"))
    except Exception as e:
        logger.warning(f"池内标记只读库打开失败: {type(e).__name__}: {e}")
        return {}
    flags: dict[str, list[str]] = {}
    try:
        pool1 = store._conn.execute(
            """
            SELECT ts_code, preset_name FROM screen_result
            WHERE trade_date = (SELECT MAX(trade_date) FROM screen_result)
            """
        ).fetchdf()
        for _, row in pool1.iterrows():
            flags.setdefault(str(row["ts_code"]), []).append(f"pool1:{row['preset_name']}")
        pool2 = store.query_pool2_active()
        for code in pool2["ts_code"]:
            flags.setdefault(str(code), []).append("pool2")
    except Exception as e:
        logger.warning(f"池内标记查询失败: {type(e).__name__}: {e}")
        return {}
    finally:
        if owns:
            store.close()
    return {code: " + ".join(labels) for code, labels in flags.items()}


def board_constituents(
    board_code: str,
    members: pd.DataFrame,
    snapshot: pd.DataFrame,
    pool_flags: dict[str, str] | None = None,
    liquidity: pd.DataFrame | None = None,
    price_tol: float = PRICE_TOL,
) -> pd.DataFrame:
    """板块下钻成分股表。

    返回列含板块内强度分 strength（默认排序键，见 add_strength_score）、
    换手强度、相对放量。B 信号因子矩阵是 P2 范畴。
    """
    if members.empty or snapshot.empty:
        return pd.DataFrame()
    codes = members.loc[members["board_code"] == board_code, "con_code"]
    if codes.empty:
        return pd.DataFrame()

    df = snapshot[snapshot["ts_code"].isin(set(codes))].copy()
    if df.empty:
        return pd.DataFrame()
    if "limit_up_price" in df.columns:
        df["is_limit_up"] = (
            df["price"].notna()
            & (df["price"] > 0)
            & df["limit_up_price"].notna()
            & (df["price"] >= df["limit_up_price"] - price_tol)
        )
    else:
        df["is_limit_up"] = False
    flags = pool_flags or {}
    df["pools"] = df["ts_code"].map(flags).fillna("")
    df = add_strength_score(df, liquidity)
    cols = [
        "ts_code", "name", "price", "pct_chg", "amount",
        "strength", "turnover_pct", "rel_volume_5d",
        "is_limit_up", "pools",
    ]
    cols = [c for c in cols if c in df.columns]
    return df[cols].sort_values("strength", ascending=False).reset_index(drop=True)


def load_liquidity_baseline(store: DuckDBStore | None = None) -> pd.DataFrame:
    """流通市值 + 5 日均成交额基准（板块内强度分的分母）。

    circ_mv 取 daily_basic 最新交易日（单位万元）；avg_amount_5d 取 daily_bar
    最近 5 个交易日均值（tushare 单位千元 → 换算成元，与快照成交额同单位）。
    只读库不可用 → 空表（强度分退化为涨幅+涨停进度两项）。
    """
    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("daily_basic", "daily_bar"))
    except Exception as e:
        logger.warning(f"流动性基准只读库打开失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    try:
        return store._conn.execute(
            """
            WITH latest AS (
              SELECT ts_code, circ_mv FROM daily_basic
              WHERE trade_date = (SELECT MAX(trade_date) FROM daily_basic)
            ),
            recent AS (
              SELECT ts_code, AVG(amount) * 1000 AS avg_amount_5d
              FROM (
                SELECT ts_code, amount,
                       ROW_NUMBER() OVER (
                         PARTITION BY ts_code ORDER BY trade_date DESC
                       ) AS rn
                FROM daily_bar
              ) WHERE rn <= 5 GROUP BY ts_code
            )
            SELECT latest.ts_code, latest.circ_mv, recent.avg_amount_5d
            FROM latest LEFT JOIN recent USING (ts_code)
            """
        ).fetchdf()
    except Exception as e:
        logger.warning(f"流动性基准查询失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    finally:
        if owns:
            store.close()


def add_strength_score(
    df: pd.DataFrame, liquidity: pd.DataFrame | None = None
) -> pd.DataFrame:
    """板块内强度分（0-100）：中和市值/体量后的「谁真正强」。

    四个分量在板块内做百分位排名后取均值（排名法抗极值）：
    - 涨幅 pct_chg
    - 换手强度 turnover_pct = 快照成交额 / 流通市值（中和市值）
    - 相对放量 rel_volume_5d = 快照成交额 / 自身 5 日均额（中和自身体量；
      盘中口径天然偏低，但板块内同一时刻横向可比）
    - 涨停进度 = (price-pre_close)/(limit_up_price-pre_close)（中和 10/20cm 差异）
    缺失分量不计入均值（新股无基准时退化为可得分量的均值）。
    """
    out = df.copy()
    if liquidity is not None and not liquidity.empty:
        out = out.merge(liquidity, on="ts_code", how="left")
    if "circ_mv" in out.columns:
        # circ_mv 单位万元 → 元
        out["turnover_pct"] = out["amount"] / (out["circ_mv"] * 10000) * 100
    if "avg_amount_5d" in out.columns:
        out["rel_volume_5d"] = out["amount"] / out["avg_amount_5d"]
    if {"limit_up_price", "pre_close"}.issubset(out.columns):
        denom = out["limit_up_price"] - out["pre_close"]
        out["_limit_progress"] = (out["price"] - out["pre_close"]) / denom.where(denom > 0)

    parts = [
        c for c in ("pct_chg", "turnover_pct", "rel_volume_5d", "_limit_progress")
        if c in out.columns
    ]
    if parts:
        ranks = pd.concat(
            [out[c].rank(pct=True, na_option="keep") for c in parts], axis=1
        )
        out["strength"] = (ranks.mean(axis=1, skipna=True) * 100).round(1)
    else:
        out["strength"] = float("nan")
    out = out.drop(columns=["circ_mv", "avg_amount_5d", "_limit_progress"], errors="ignore")
    for c in ("turnover_pct", "rel_volume_5d"):
        if c in out.columns:
            out[c] = out[c].round(2)
    return out
