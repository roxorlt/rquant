"""盘中市场全景页数据获取层（P0）。

所有外部源调用集中在本模块，每个 fetcher 独立 try/except + 空态返回：
akshare 断了页面不炸，UI 侧对空 DataFrame 渲染灰态。

数据源与刷新节奏（TTL 由 UI 层 st.cache_data 控制）：
- S1 新浪全市场快照 ``ak.stock_zh_a_spot``：30s（云端可用，但本页只在本地跑）
- S2 板块资金流：60s，三级路由 东财直连 → 东财·SOCKS 云端出口 → 同花顺即时兜底
  （办公网出口 IP 被东财拉黑、同花顺云端 403，两者互补，见 fetch_sector_fund_flow）
- L  本地只读副本（dc_board / dc_board_member / kpl_concept_member /
  screen_result / pool2_watch）：300s

DuckDB 只走 ``open_readonly_store()``（副本优先），本模块绝不写主库。
"""

from __future__ import annotations

import os

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

# 东财 push2 clist 字段映射（2026-07-03 实测 JSON）：
#   f12 板块代码(BKxxxx) / f14 板块名称 / f2 板块指数点位 / f3 涨跌幅(%)
#   f62 主力净流入额(元) / f184 主力净占比(%) / f204 主力净流入最大股 / f205 最大股代码
_EM_CLIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
_EM_FIELDS = "f12,f14,f3,f62,f184,f204"
_EM_FS: dict[str, str] = {"行业资金流": "m:90+t:2", "概念资金流": "m:90+t:3"}
_EM_PAGE_SIZE = 100  # 实测 pz=500 也只回 100 行（total≈496），须按 pn 翻页
_EM_MAX_PAGES = 10
_EM_DIRECT_TIMEOUT = 5.0
_EM_SOCKS_TIMEOUT = 10.0
_DEFAULT_SOCKS_PROXY = "socks5h://127.0.0.1:1086"

# fetch_sector_fund_flow 的 df.attrs["route"] 取值 → UI 展示名
ROUTE_LABELS: dict[str, str] = {
    "em_direct": "东财直连",
    "em_socks": "东财·云端出口",
    "ths": "同花顺",
    "none": "不可用",
}


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


# ── S2 板块资金流（三级路由：东财直连 → 东财·SOCKS 出口 → 同花顺兜底） ─────────


def _em_fetch_rows(
    sector_type: str, proxies: dict[str, str] | None, timeout: float
) -> list[dict]:
    """东财 push2 clist 分页拉取，返回 diff 行列表（fid=f62 主力净流入降序）。"""
    import requests

    fs = _EM_FS.get(sector_type)
    if fs is None:
        raise ValueError(f"未知板块资金流类型: {sector_type}")
    rows: list[dict] = []
    for pn in range(1, _EM_MAX_PAGES + 1):
        params = {
            "pn": pn, "pz": _EM_PAGE_SIZE, "po": 1, "np": 1,
            "fltt": 2, "invt": 2, "fid": "f62", "fs": fs, "fields": _EM_FIELDS,
        }
        resp = requests.get(_EM_CLIST_URL, params=params, timeout=timeout, proxies=proxies)
        resp.raise_for_status()
        data = (resp.json() or {}).get("data") or {}
        diff = data.get("diff") or []
        if not diff:
            break
        rows.extend(diff)
        if len(rows) >= int(data.get("total") or 0):
            break
    return rows


def _normalize_em_flow(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    raw = pd.DataFrame(rows)
    if "f14" not in raw.columns or "f62" not in raw.columns:
        logger.warning("东财资金流关键字段缺失（f14/f62，接口字段可能变更）")
        return pd.DataFrame()
    out = pd.DataFrame(index=raw.index)
    out["board_name"] = raw["f14"].astype(str)
    # 盘前/停牌时数值字段可能给 "-"，coerce 成 NaN
    for src, dst in (("f3", "pct_chg"), ("f62", "main_net_amount"), ("f184", "main_net_rate")):
        out[dst] = pd.to_numeric(raw[src], errors="coerce") if src in raw.columns else float("nan")
    out["leading_stock"] = raw["f204"] if "f204" in raw.columns else None
    out = out[out["main_net_amount"].notna()].copy()
    return out.sort_values("main_net_amount", ascending=False).reset_index(drop=True)


def _fetch_ths_flow_raw(sector_type: str) -> pd.DataFrame:
    import akshare as ak

    if sector_type == "概念资金流":
        return ak.stock_fund_flow_concept(symbol="即时")
    return ak.stock_fund_flow_industry(symbol="即时")


def _normalize_ths_flow(raw: pd.DataFrame) -> pd.DataFrame:
    """同花顺即时资金流归一化。

    实测列（2026-07-03，概念接口列名同样用「行业」）：序号/行业/行业指数/
    行业-涨跌幅/流入资金/流出资金/净额/公司家数/领涨股/领涨股-涨跌幅/当前价。
    资金列单位亿元；无主力净占比 → main_net_rate 置 NaN；领涨股是涨幅口径
    （非东财的主力流入最大股）。
    """
    if raw is None or raw.empty:
        return pd.DataFrame()
    board_col = next((c for c in ("行业", "概念", "板块") if c in raw.columns), None)
    net_col = next((c for c in raw.columns if "净额" in str(c)), None)
    if board_col is None or net_col is None:
        logger.warning("同花顺资金流列异常（板块/净额列缺失，列名可能变更）")
        return pd.DataFrame()
    pct_col = next((c for c in raw.columns if "涨跌幅" in str(c) and "领涨" not in str(c)), None)
    lead_col = next((c for c in raw.columns if "领涨股" in str(c) and "涨跌幅" not in str(c)), None)

    out = pd.DataFrame(index=raw.index)
    out["board_name"] = raw[board_col].astype(str)
    out["pct_chg"] = pd.to_numeric(raw[pct_col], errors="coerce") if pct_col else float("nan")
    out["main_net_amount"] = pd.to_numeric(raw[net_col], errors="coerce") * 1e8
    out["main_net_rate"] = float("nan")
    out["leading_stock"] = raw[lead_col] if lead_col is not None else None
    out = out[out["main_net_amount"].notna()].copy()
    return out.sort_values("main_net_amount", ascending=False).reset_index(drop=True)


def _with_route(df: pd.DataFrame, route: str) -> pd.DataFrame:
    df.attrs["route"] = route
    return df


def fetch_sector_fund_flow(sector_type: str = "行业资金流") -> pd.DataFrame:
    """板块资金流排行（今日口径），三级路由。

    东财直连（办公网出口 IP 被拉黑时 0.1s 内 RST）→ 东财走 SOCKS 云端出口
    （环境变量 RQUANT_PANORAMA_SOCKS 可覆盖代理地址，置空禁用该级）→
    同花顺即时兜底（云端 403 但本地可用，与东财互补）→ 全失败返回空。

    返回列：board_name, pct_chg(%), main_net_amount(元), main_net_rate(%),
    leading_stock；``df.attrs["route"]`` 标记实际数据路由（ROUTE_LABELS 的 key）。
    """
    try:
        rows = _em_fetch_rows(sector_type, proxies=None, timeout=_EM_DIRECT_TIMEOUT)
        df = _normalize_em_flow(rows)
        if not df.empty:
            return _with_route(df, "em_direct")
        logger.warning(f"东财直连返回空: {sector_type}")
    except Exception as e:
        logger.warning(f"东财直连失败: {sector_type} {type(e).__name__}: {e}")

    socks = os.environ.get("RQUANT_PANORAMA_SOCKS", _DEFAULT_SOCKS_PROXY).strip()
    if socks:
        try:
            proxies = {"http": socks, "https": socks}
            rows = _em_fetch_rows(sector_type, proxies=proxies, timeout=_EM_SOCKS_TIMEOUT)
            df = _normalize_em_flow(rows)
            if not df.empty:
                return _with_route(df, "em_socks")
            logger.warning(f"东财 SOCKS 出口返回空: {sector_type}")
        except Exception as e:
            logger.warning(f"东财 SOCKS 出口失败: {sector_type} {type(e).__name__}: {e}")

    try:
        df = _normalize_ths_flow(_fetch_ths_flow_raw(sector_type))
        if not df.empty:
            return _with_route(df, "ths")
        logger.warning(f"同花顺即时资金流返回空: {sector_type}")
    except Exception as e:
        logger.warning(f"同花顺即时资金流失败: {sector_type} {type(e).__name__}: {e}")

    logger.warning(f"板块资金流三级路由全部失败: {sector_type}")
    return _with_route(pd.DataFrame(), "none")


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


def load_kpl_concept_members(store: DuckDBStore | None = None) -> pd.DataFrame:
    """读开盘啦题材成分（kpl_concept_member 快照，打板语境粒度）。

    返回列：board_code, board_name, con_code（与 load_board_members 对齐，
    无 idx_type——开盘啦题材没有行业/概念之分）。
    副本缺表（日终采集未首跑）/ 撞锁 → 空 DataFrame（UI 渲染灰态）。
    """
    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("kpl_concept_member",))
    except Exception as e:
        logger.warning(f"开盘啦题材成分只读库打开失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    try:
        return store._conn.execute(
            "SELECT board_code, board_name, con_code FROM kpl_concept_member"
        ).fetchdf()
    except Exception as e:
        logger.warning(f"开盘啦题材成分查询失败: {type(e).__name__}: {e}")
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


def aggregate_board_limit_ups(
    snapshot: pd.DataFrame,
    members: pd.DataFrame,
    idx_type: str | None = None,
    price_tol: float = PRICE_TOL,
) -> pd.DataFrame:
    """板块涨停排行：带涨停价的快照 × 板块成分聚合。

    涨停/炸板判定与 compute_market_pulse 同口径（同一 price_tol / 同一
    limit_up_price 列，不另造口径）：
    - 涨停：price ≥ limit_up_price − tol
    - 炸板：high 触过涨停价但现价回落
    - 停牌票（price 缺失或 ≤0）不计入任何计数

    members 需要 board_code / board_name / con_code 三列（开盘啦
    load_kpl_concept_members 与东财 load_board_members 均满足）；idx_type
    只对带 idx_type 列的东财成分生效（地域板块已在 load_board_members 排除）。

    返回列：board_code, board_name, limit_up_count, broken_count,
    stock_count, limit_up_ratio_pct，按涨停数降序（同数按占比降序），
    涨停数为 0 的板块不返回。概念成分互相重叠：计数只在板块内解释，
    不跨板块求和。快照缺 limit_up_price 列 → 空 DataFrame（降级）。
    """
    if snapshot.empty or members.empty:
        return pd.DataFrame()
    if not {"price", "limit_up_price"}.issubset(snapshot.columns):
        return pd.DataFrame()
    m = members
    if idx_type is not None and "idx_type" in members.columns:
        m = members[members["idx_type"] == idx_type]
    if m.empty:
        return pd.DataFrame()

    merged = m.merge(snapshot, left_on="con_code", right_on="ts_code", how="inner")
    if merged.empty:
        return pd.DataFrame()

    valid = merged["price"].notna() & (merged["price"] > 0) & merged["limit_up_price"].notna()
    is_limit_up = valid & (merged["price"] >= merged["limit_up_price"] - price_tol)
    if "high" in merged.columns:
        high = pd.to_numeric(merged["high"], errors="coerce")
    else:
        high = pd.Series(float("nan"), index=merged.index)
    touched = valid & high.notna() & (high >= merged["limit_up_price"] - price_tol)
    merged["_is_limit_up"] = is_limit_up
    merged["_is_broken"] = touched & ~is_limit_up

    agg = merged.groupby(["board_code", "board_name"], as_index=False).agg(
        limit_up_count=("_is_limit_up", "sum"),
        broken_count=("_is_broken", "sum"),
        stock_count=("ts_code", "count"),
    )
    for c in ("limit_up_count", "broken_count", "stock_count"):
        agg[c] = agg[c].astype(int)
    agg = agg[agg["limit_up_count"] > 0].copy()
    if agg.empty:
        return pd.DataFrame()
    agg["limit_up_ratio_pct"] = (
        agg["limit_up_count"] / agg["stock_count"] * 100
    ).round(1)
    return agg.sort_values(
        ["limit_up_count", "limit_up_ratio_pct"], ascending=False
    ).reset_index(drop=True)


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
