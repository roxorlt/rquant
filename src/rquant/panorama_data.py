"""盘中市场全景页数据获取层（P0）。

所有外部源调用集中在本模块，每个 fetcher 独立 try/except + 空态返回：
akshare 断了页面不炸，UI 侧对空 DataFrame 渲染灰态。

数据源与刷新节奏（快照/资金流由 panorama_poller 后台拉取，其余 TTL 由 UI 层
st.cache_data 控制）：
- S1 全市场快照：三级路由 东财 push2 clist 直连 → 同一实现走 SOCKS 云端出口 →
  新浪 ``ak.stock_zh_a_spot`` 兜底（sina 逐页 70 页，盘外被限速单次可 >90s，
  只作最后兜底；``allow_sina=False`` 可跳过该级，熔断由 SourcePoller 管）
- S2 板块资金流：三级路由 东财直连 → 东财·SOCKS 云端出口 → 同花顺即时兜底
  （办公网出口 IP 被东财拉黑、同花顺云端 403，两者互补，见 fetch_sector_fund_flow）
- L  本地只读副本（dc_board / dc_board_member / kpl_concept_member /
  screen_result / pool2_watch）：300s

东财全部请求（clist 快照/资金流 + trends2 分时）走 ``_em_session()``：每次全新
Session + trust_env=False + 桌面 UA/Referer + Connection:close。2026-07-06 盘中
事故：东财风控钉住长驻进程的连接状态（同进程连续 RST、新进程三路秒通），必须
强制每次新 TCP/TLS 且不吸系统代理环境。

DuckDB 只走 ``open_readonly_store()``（副本优先），本模块绝不写主库。
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from loguru import logger
from pydantic import BaseModel

if TYPE_CHECKING:
    import requests

from rquant.limit_up_pool import to_ts_code
from rquant.state.derive import _classify_board, _detect_st, _limit_pct, _round_half_up
from rquant.storage.duckdb import DuckDBStore, open_readonly_store

_CST = timezone(timedelta(hours=8))  # A 股墙钟（当日 events 文件按此取「今日」）

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

# 东财 push2 clist 全市场股票快照（直连/SOCKS 两级同一套自实现分页——akshare 的
# stock_zh_a_spot_em 无法注入加固 headers，已弃用）：
#   fs 覆盖 沪主板/科创 + 深主板/创业 + 北交所；f5 成交量单位是**手**（sina 是股），
#   归一时 ×100 保持 snapshot 列契约不变（volume=股, amount=元）
_EM_SPOT_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
_EM_SPOT_FIELDS = "f12,f14,f2,f17,f15,f16,f18,f3,f5,f6"
_EM_SPOT_FIELD_MAP: dict[str, str] = {
    "f14": "name", "f2": "price", "f17": "open", "f15": "high", "f16": "low",
    "f18": "pre_close", "f3": "pct_chg", "f5": "volume", "f6": "amount",
}
_EM_SPOT_PAGE_SIZE = 1000  # 先请求 1000，服务端可能钳制单页行数，按 total 累积翻页
_EM_SPOT_MAX_PAGES = 80  # 全市场 ~5700 只，即便被钳到 100/页也 60 页内取完

# 东财请求加固 headers：桌面 Chrome UA + 行情站 Referer + Connection:close
# （每请求新 TCP，配合每次全新 Session 防进程级连接状态被风控钉住）
_EM_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/",
    "Connection": "close",
}

# 东财 push2his 分时（trends2）：data.trends 是逗号分隔字符串数组，
# fields2=f51(时间) f53(价) f56(量) f58(均价) → 每行 "time,price,volume,avg"
_EM_TRENDS_URL = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
_EM_TRENDS_FIELDS2 = "f51,f53,f56,f58"

# 合并总表体系 → 东财成分 idx_type（开盘啦题材走 kpl 成分，无 idx_type）
BOARD_SYSTEMS = ("东财行业", "东财概念", "开盘啦题材")
_SYSTEM_IDX_TYPE: dict[str, str] = {"东财行业": "行业板块", "东财概念": "概念板块"}
_KPL_SYSTEM = "开盘啦题材"

# fetch_market_snapshot / fetch_sector_fund_flow / fetch_intraday_trend 的
# df.attrs["route"] 取值 → UI 展示名
ROUTE_LABELS: dict[str, str] = {
    "em_direct": "东财直连",
    "em_socks": "东财·云端出口",
    "ths": "同花顺",
    "sina": "新浪",
    "cloud_feed": "云端feed",
    "none": "不可用",
}


def _fake_enabled() -> bool:
    """RQUANT_PANORAMA_FAKE=1 时全数据层返回确定性 fixture（e2e 可测性）。"""
    return os.environ.get("RQUANT_PANORAMA_FAKE", "").strip() == "1"


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


# ── S1 全市场快照（三级路由：东财直连 → 东财·SOCKS 出口 → 新浪兜底） ──────────


def _fetch_spot() -> pd.DataFrame:
    import akshare as ak

    return ak.stock_zh_a_spot()


def _em_session() -> requests.Session:
    """东财专用加固 Session：每次全新 + trust_env=False + UA/Referer/Connection:close。

    trust_env=False 不吸系统代理环境；Connection:close 每请求新 TCP——
    避免长驻进程的连接指纹被东财风控钉住（2026-07-06 盘中事故根因）。
    """
    import requests

    session = requests.Session()
    session.trust_env = False
    session.headers.update(_EM_HEADERS)
    return session


def _finalize_snapshot(out: pd.DataFrame) -> pd.DataFrame:
    """快照归一化公共尾部：丢弃无法映射的代码、数值 coerce、pct_chg 兜底、去重。

    ts_code 去重防御东财翻页在服务端钳制页长时的重复行（重复会导致板块聚合双计）。
    """
    dropped = int(out["ts_code"].isna().sum())
    if dropped:
        logger.debug(f"全市场快照 {dropped} 行代码段无法映射交易所，已丢弃")
    out = out[out["ts_code"].notna()].copy()
    for col in _SPOT_NUMERIC:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    # pct_chg 缺失时用 price/pre_close 兜底
    fallback = (out["price"] / out["pre_close"] - 1) * 100
    out["pct_chg"] = out["pct_chg"].fillna(fallback)
    return out.drop_duplicates("ts_code", keep="first").reset_index(drop=True)


def _normalize_sina_spot(raw: pd.DataFrame) -> pd.DataFrame:
    """新浪快照归一化（volume 单位已是股，不换算）。缺必需列 → 空。"""
    if raw is None or raw.empty:
        return pd.DataFrame()
    missing = set(_SPOT_REQUIRED) - set(raw.columns)
    if missing:
        logger.warning(f"新浪快照列异常，缺 {sorted(missing)}，本轮跳过")
        return pd.DataFrame()
    out = pd.DataFrame(index=raw.index)
    # sina 代码带前缀 sh/sz/bj，取后 6 位映射 ts_code
    out["ts_code"] = raw["代码"].astype(str).str[-6:].map(to_ts_code)
    for src, dst in _SPOT_COLUMN_MAP.items():
        out[dst] = raw[src] if src in raw.columns else None
    return _finalize_snapshot(out)


def _em_spot_fetch_rows(proxies: dict[str, str] | None, timeout: float) -> list[dict]:
    """东财 push2 clist 全市场股票分页拉取（按 total 累积翻页防深页）。

    直连（proxies=None）与 SOCKS 两级共用；加固 Session 见 _em_session。
    """
    rows: list[dict] = []
    with _em_session() as session:
        for pn in range(1, _EM_SPOT_MAX_PAGES + 1):
            params = {
                "pn": pn, "pz": _EM_SPOT_PAGE_SIZE, "po": 1, "np": 1,
                "fltt": 2, "invt": 2, "fid": "f6", "fs": _EM_SPOT_FS,
                "fields": _EM_SPOT_FIELDS,
            }
            resp = session.get(_EM_CLIST_URL, params=params, timeout=timeout, proxies=proxies)
            resp.raise_for_status()
            data = (resp.json() or {}).get("data") or {}
            diff = data.get("diff") or []
            if not diff:
                break
            rows.extend(diff)
            if len(rows) >= int(data.get("total") or 0):
                break
    return rows


def _normalize_em_spot_rows(rows: list[dict]) -> pd.DataFrame:
    """东财 clist diff 行归一化（f5 成交量单位手 → ×100 成股）。"""
    if not rows:
        return pd.DataFrame()
    raw = pd.DataFrame(rows)
    if "f12" not in raw.columns or "f2" not in raw.columns:
        logger.warning("东财快照关键字段缺失（f12/f2，接口字段可能变更）")
        return pd.DataFrame()
    out = pd.DataFrame(index=raw.index)
    out["ts_code"] = raw["f12"].astype(str).str[-6:].map(to_ts_code)
    for src, dst in _EM_SPOT_FIELD_MAP.items():
        out[dst] = raw[src] if src in raw.columns else None
    out = _finalize_snapshot(out)
    if not out.empty:
        out["volume"] = out["volume"] * 100
    return out


def fetch_market_snapshot(allow_sina: bool = True) -> pd.DataFrame:
    """拉取全市场快照并归一化为英文列，三级路由。

    东财 push2 clist 直连（自实现分页 + 加固 Session，办公网被拉黑时秒败）→
    同一实现走 SOCKS 云端出口（环境变量 RQUANT_PANORAMA_SOCKS 可覆盖代理地址，
    置空禁用该级）→ 新浪 ``ak.stock_zh_a_spot`` 兜底（逐页限速单次可 10-90s）→
    全失败返回空。``allow_sina=False`` 跳过 sina 级——SourcePoller 对 sina 单独
    熔断，不允许它反复吊死后台拉取循环。

    返回列：ts_code, name, price, open, high, low, pre_close, pct_chg,
    volume(股), amount(元)——东财成交量单位手，归一 ×100 保持列契约。
    ``df.attrs["route"]`` ∈ {'em_direct','em_socks','sina','none'}。

    与 ``monitor.fetch_realtime_quotes`` 的 sina 源不同，全景页需要全市场
    DataFrame（那边绑定 watchlist 语义），故独立实现。
    """
    if _fake_enabled():
        return _with_route(_fake_snapshot(), "em_direct")

    try:
        rows = _em_spot_fetch_rows(proxies=None, timeout=_EM_DIRECT_TIMEOUT)
        df = _normalize_em_spot_rows(rows)
        if not df.empty:
            return _with_route(df, "em_direct")
        logger.warning("东财快照直连返回空")
    except Exception as e:
        logger.warning(f"东财快照直连失败: {type(e).__name__}: {e}")

    socks = os.environ.get("RQUANT_PANORAMA_SOCKS", _DEFAULT_SOCKS_PROXY).strip()
    if socks:
        try:
            proxies = {"http": socks, "https": socks}
            rows = _em_spot_fetch_rows(proxies=proxies, timeout=_EM_SOCKS_TIMEOUT)
            df = _normalize_em_spot_rows(rows)
            if not df.empty:
                return _with_route(df, "em_socks")
            logger.warning("东财快照 SOCKS 出口返回空")
        except Exception as e:
            logger.warning(f"东财快照 SOCKS 出口失败: {type(e).__name__}: {e}")

    if allow_sina:
        try:
            df = _normalize_sina_spot(_fetch_spot())
            if not df.empty:
                return _with_route(df, "sina")
            logger.warning("新浪快照兜底返回空")
        except Exception as e:
            logger.warning(f"新浪快照兜底失败: {type(e).__name__}: {e}")
    else:
        logger.warning("sina 兜底级被跳过（熔断冷却中）")

    logger.warning("全市场快照三级路由全部失败")
    return _with_route(pd.DataFrame(), "none")


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
    fs = _EM_FS.get(sector_type)
    if fs is None:
        raise ValueError(f"未知板块资金流类型: {sector_type}")
    rows: list[dict] = []
    with _em_session() as session:
        for pn in range(1, _EM_MAX_PAGES + 1):
            params = {
                "pn": pn, "pz": _EM_PAGE_SIZE, "po": 1, "np": 1,
                "fltt": 2, "invt": 2, "fid": "f62", "fs": fs, "fields": _EM_FIELDS,
            }
            resp = session.get(_EM_CLIST_URL, params=params, timeout=timeout, proxies=proxies)
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
    # f12 是东财 BK 板块码（如 "BK1749"）：留列用于合并总表按 f"{f12}.DC" 精确
    # join dc_board.ts_code；缺列时置 None（降级 board_name join）
    out["board_code"] = raw["f12"].astype(str) if "f12" in raw.columns else None
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
    # 同花顺无东财 BK 码 → board_code 置 None，合并总表降级按 board_name join
    out["board_code"] = None
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

    返回列：board_code(BK 码|None), board_name, pct_chg(%), main_net_amount(元),
    main_net_rate(%), leading_stock；``df.attrs["route"]`` 标记实际数据路由
    （ROUTE_LABELS 的 key）。
    """
    if _fake_enabled():
        return _fake_sector_fund_flow(sector_type)
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
    if _fake_enabled():
        return _fake_board_members()
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
    if _fake_enabled():
        return _fake_kpl_members()
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


def _mark_limit_flags(merged: pd.DataFrame, price_tol: float = PRICE_TOL) -> pd.DataFrame:
    """在 merge 后的成分×快照表上就地标注 _is_limit_up / _is_broken。

    与 compute_market_pulse / aggregate_board_limit_ups 同口径（同一 price_tol、
    同一 limit_up_price 列）：
    - 涨停：price ≥ limit_up_price − tol
    - 炸板：high 触过涨停价但现价回落（未涨停）
    - 停牌票（price 缺失或 ≤0）不计入任一标记
    快照缺 limit_up_price 列 → 两列全 False（降级，不炸）。
    """
    if "limit_up_price" not in merged.columns:
        merged["_is_limit_up"] = False
        merged["_is_broken"] = False
        return merged
    valid = merged["price"].notna() & (merged["price"] > 0) & merged["limit_up_price"].notna()
    is_limit_up = valid & (merged["price"] >= merged["limit_up_price"] - price_tol)
    if "high" in merged.columns:
        high = pd.to_numeric(merged["high"], errors="coerce")
    else:
        high = pd.Series(float("nan"), index=merged.index)
    touched = valid & high.notna() & (high >= merged["limit_up_price"] - price_tol)
    merged["_is_limit_up"] = is_limit_up
    merged["_is_broken"] = touched & ~is_limit_up
    return merged


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
    merged = _mark_limit_flags(merged, price_tol)

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

    merged = _mark_limit_flags(merged, price_tol)

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


# 合并总表输出列（顺序 pinned，UI/测试按此契约取列）
_OVERVIEW_COLUMNS = [
    "board_code", "board_name", "amount", "main_net_amount", "main_net_rate",
    "pct_chg_median", "limit_up_count", "broken_count", "stock_count",
    "limit_up_ratio_pct", "leading_stock",
]
_OVERVIEW_FLOW_COLUMNS = ["main_net_amount", "main_net_rate", "leading_stock"]


def build_board_overview(
    snapshot: pd.DataFrame,
    members: pd.DataFrame,
    kpl_members: pd.DataFrame,
    flow: pd.DataFrame,
    system: str,
) -> pd.DataFrame:
    """合并板块总表：成交额 / 资金流 / 涨停炸板一张表，默认 amount 降序。

    列：board_code, board_name, amount(元), main_net_amount(元|NaN),
    main_net_rate(%|NaN), pct_chg_median(%), limit_up_count, broken_count,
    stock_count, limit_up_ratio_pct, leading_stock(str|None)。

    - 成分×快照**只 merge 一次**，一次 groupby 出成交额/中位涨幅/涨停/炸板/成分数
      （涨停·炸板口径复用 _mark_limit_flags，与 aggregate_board_limit_ups 一致）；
    - 资金流 join：东财体系优先按 board_code 精确 join（flow 的 BK 码 + ".DC" ==
      dc_board.ts_code），board_code 全缺（同花顺路由）降级按 board_name join；
      开盘啦体系资金流三列全 NaN；
    - 涨停数为 0 的板块保留（成交额仍有意义）；
    - 快照空 / 对应体系成分空 → 空 DataFrame。
    """
    if system not in BOARD_SYSTEMS:
        raise ValueError(f"未知板块体系: {system}（应为 {BOARD_SYSTEMS} 之一）")

    is_kpl = system == _KPL_SYSTEM
    if is_kpl:
        m = kpl_members
    elif not members.empty and "idx_type" in members.columns:
        m = members[members["idx_type"] == _SYSTEM_IDX_TYPE[system]]
    else:
        m = members.iloc[0:0]

    if snapshot.empty or m.empty:
        return pd.DataFrame()

    merged = m.merge(snapshot, left_on="con_code", right_on="ts_code", how="inner")
    if merged.empty:
        return pd.DataFrame()
    merged = _mark_limit_flags(merged)

    agg = merged.groupby(["board_code", "board_name"], as_index=False).agg(
        amount=("amount", "sum"),
        pct_chg_median=("pct_chg", "median"),
        limit_up_count=("_is_limit_up", "sum"),
        broken_count=("_is_broken", "sum"),
        stock_count=("ts_code", "count"),
    )
    for c in ("limit_up_count", "broken_count", "stock_count"):
        agg[c] = agg[c].astype(int)
    agg["limit_up_ratio_pct"] = (agg["limit_up_count"] / agg["stock_count"] * 100).round(1)

    agg = _join_board_flow(agg, flow, is_kpl)
    return agg.sort_values("amount", ascending=False).reset_index(drop=True)[_OVERVIEW_COLUMNS]


def _join_board_flow(agg: pd.DataFrame, flow: pd.DataFrame, is_kpl: bool) -> pd.DataFrame:
    """把资金流三列 join 进板块级聚合表；开盘啦体系或 flow 空时补全 NaN 列。"""
    joinable = (
        not is_kpl
        and flow is not None
        and not flow.empty
        and set(_OVERVIEW_FLOW_COLUMNS).issubset(flow.columns)
    )
    if joinable:
        use_code = "board_code" in flow.columns and flow["board_code"].notna().any()
        if use_code:
            code_cols = ["board_code", *_OVERVIEW_FLOW_COLUMNS]
            fj = flow.loc[flow["board_code"].notna(), code_cols].copy()
            fj["board_code"] = fj["board_code"].astype(str) + ".DC"
            fj = fj.drop_duplicates("board_code", keep="first")
            agg = agg.merge(fj, on="board_code", how="left")
        else:
            fj = flow[["board_name", *_OVERVIEW_FLOW_COLUMNS]].drop_duplicates(
                "board_name", keep="first"
            )
            agg = agg.merge(fj, on="board_name", how="left")
    for c in _OVERVIEW_FLOW_COLUMNS:
        if c not in agg.columns:
            agg[c] = None if c == "leading_stock" else float("nan")
    return agg


def load_pool_flags(store: DuckDBStore | None = None) -> dict[str, str]:
    """池内标记：{ts_code: 'pool1:预设名' / 'pool2' / 两者拼接}。

    pool1 = screen_result 最新 trade_date 的入选票；pool2 = pool2_watch active。
    只读库不可用 → 空 dict（下钻表池内列全空，不炸页面）。
    """
    if _fake_enabled():
        return _fake_pool_flags()
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
    if _fake_enabled():
        return _fake_liquidity_baseline()
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


# ── 个股图表数据（分时/5日 trends2 + 日K 只读副本） ────────────────────────────


def _trends_secid(ts_code: str) -> str:
    """ts_code → 东财 trends2 的 secid：沪市（6 开头）1.，深/北 0.。"""
    code = ts_code.split(".")[0]
    market = "1" if code.startswith("6") else "0"
    return f"{market}.{code}"


def _trends_get(
    secid: str, ndays: int, proxies: dict[str, str] | None, timeout: float
) -> dict:
    """东财 push2his trends2 单次请求（加固 Session），返回原始 JSON dict。"""
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f7",
        "fields2": _EM_TRENDS_FIELDS2,
        "iscca": 0,
        "ndays": ndays,
        "iscr": 0,
    }
    with _em_session() as session:
        resp = session.get(_EM_TRENDS_URL, params=params, timeout=timeout, proxies=proxies)
    resp.raise_for_status()
    return resp.json() or {}


def _parse_trends(payload: dict) -> pd.DataFrame:
    """解析 trends2 JSON：data.trends 逗号分隔字符串 → dt/price/avg_price/volume。"""
    data = (payload or {}).get("data") or {}
    trends = data.get("trends") or []
    if not trends:
        return pd.DataFrame()
    recs = [str(line).split(",")[:4] for line in trends if len(str(line).split(",")) >= 4]
    if not recs:
        return pd.DataFrame()
    df = pd.DataFrame(recs, columns=["dt", "price", "volume", "avg_price"])
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    for c in ("price", "volume", "avg_price"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["dt"]).reset_index(drop=True)
    return df[["dt", "price", "avg_price", "volume"]]


def _fetch_sina_minute_raw(ts_code: str) -> pd.DataFrame:
    """新浪 1 分钟兜底原始拉取（ak.stock_zh_a_minute，symbol 需 sh/sz/bj 前缀）。"""
    import akshare as ak

    code, _, suffix = ts_code.partition(".")
    prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(suffix, "sh")
    return ak.stock_zh_a_minute(symbol=f"{prefix}{code}", period="1", adjust="")


def _normalize_sina_minute(raw: pd.DataFrame) -> pd.DataFrame:
    """新浪分钟归一化为 dt/price/avg_price(NaN)/volume（无均价线）。"""
    if raw is None or raw.empty or "day" not in raw.columns or "close" not in raw.columns:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["dt"] = pd.to_datetime(raw["day"], errors="coerce")
    out["price"] = pd.to_numeric(raw["close"], errors="coerce")
    out["avg_price"] = float("nan")
    out["volume"] = (
        pd.to_numeric(raw["volume"], errors="coerce") if "volume" in raw.columns else float("nan")
    )
    return out.dropna(subset=["dt"]).reset_index(drop=True)[["dt", "price", "avg_price", "volume"]]


def _trim_to_last_ndays(trend: pd.DataFrame, ndays: int) -> pd.DataFrame:
    """只保留最近 ndays 个交易日的分钟数据。

    新浪 ak.stock_zh_a_minute 不接受 ndays 参数、固定返回约 5 天，不裁剪会让分时
    （ndays=1）被当成多日渲染成 5 日图。东财 trends2 天然按 ndays 返回，无需裁剪。
    """
    if trend.empty or ndays <= 0:
        return trend
    days = pd.to_datetime(trend["dt"]).dt.normalize()
    keep = set(days.drop_duplicates().nlargest(ndays))
    return trend[days.isin(keep)].reset_index(drop=True)


def fetch_intraday_trend(ts_code: str, ndays: int = 1) -> pd.DataFrame:
    """个股分时（ndays=1）/ 5 日线（ndays=5）。

    列：dt(datetime), price, avg_price(float|NaN), volume。三级路由：
    东财 trends2 直连 → SOCKS 云端出口 → 新浪 ak.stock_zh_a_minute 兜底
    （新浪无均价线，avg_price=NaN）。``df.attrs["route"]`` ∈
    {'em_direct','em_socks','sina','none'}，全失败 → 空表 route='none'。
    """
    if _fake_enabled():
        return _fake_intraday_trend(ts_code, ndays)

    secid = _trends_secid(ts_code)
    try:
        df = _parse_trends(_trends_get(secid, ndays, proxies=None, timeout=_EM_DIRECT_TIMEOUT))
        if not df.empty:
            return _with_route(df, "em_direct")
        logger.warning(f"trends2 直连返回空: {ts_code}")
    except Exception as e:
        logger.warning(f"trends2 直连失败: {ts_code} {type(e).__name__}: {e}")

    socks = os.environ.get("RQUANT_PANORAMA_SOCKS", _DEFAULT_SOCKS_PROXY).strip()
    if socks:
        try:
            proxies = {"http": socks, "https": socks}
            payload = _trends_get(secid, ndays, proxies=proxies, timeout=_EM_SOCKS_TIMEOUT)
            df = _parse_trends(payload)
            if not df.empty:
                return _with_route(df, "em_socks")
            logger.warning(f"trends2 SOCKS 出口返回空: {ts_code}")
        except Exception as e:
            logger.warning(f"trends2 SOCKS 出口失败: {ts_code} {type(e).__name__}: {e}")

    try:
        df = _trim_to_last_ndays(_normalize_sina_minute(_fetch_sina_minute_raw(ts_code)), ndays)
        if not df.empty:
            return _with_route(df, "sina")
        logger.warning(f"新浪分钟兜底返回空: {ts_code}")
    except Exception as e:
        logger.warning(f"新浪分钟兜底失败: {ts_code} {type(e).__name__}: {e}")

    logger.warning(f"个股分时三级路由全部失败: {ts_code}")
    return _with_route(pd.DataFrame(), "none")


def load_daily_kline(
    ts_code: str, n: int = 120, store: DuckDBStore | None = None
) -> pd.DataFrame:
    """日 K（只读副本 daily_bar 最近 n 根，就地滚动 MA）。

    列：trade_date, open, high, low, close, volume, ma5, ma10, ma20
    （MA 在取回的 n 根窗口内滚动，不足窗口为 NaN——首 4 根无 ma5，以此类推）。
    副本缺表 / 撞锁 / 无数据 → 空 DataFrame。
    """
    if _fake_enabled():
        return _fake_daily_kline(ts_code, n)

    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("daily_bar",))
    except Exception as e:
        logger.warning(f"日K 只读库打开失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    try:
        df = store._conn.execute(
            """
            SELECT trade_date, open, high, low, close, vol AS volume
            FROM daily_bar
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [ts_code, n],
        ).fetchdf()
    except Exception as e:
        logger.warning(f"日K 查询失败: {ts_code} {type(e).__name__}: {e}")
        return pd.DataFrame()
    finally:
        if owns:
            store.close()
    if df.empty:
        return df
    df = df.sort_values("trade_date").reset_index(drop=True)
    for w in (5, 10, 20):
        df[f"ma{w}"] = df["close"].rolling(window=w, min_periods=w).mean()
    return df


# ── surge-watch 当日爆量台账（只读 events jsonl，绝不写） ──────────────────────

# events jsonl 每行 = 一只票的 SurgeConfirmed.model_dump()（见 surge_watch.SurgeConfirmed）。
# 与 surge_watch.LIVE_DIR_NAME 对齐；此处硬编码字面量避免为一个只读消费者反向依赖
# surge_watch（后者会拉进 tushare/state.derive 等重依赖）。
_SURGE_LIVE_DIR_NAME = "surge_live"
_SURGE_LOG_COLUMNS = [
    "confirmed_at", "ts_code", "name", "theme", "pct_chg",
    "cum_amount", "rel_cum", "room_to_limit_pct", "status",
]
_PULSE_LOG_COLUMNS = ["t", "limit_up", "limit_down", "broken", "up", "down",
                      "up_ratio_pct", "total"]
_PULSE_ALERT_COLUMNS = ["t", "kind", "kind_label", "before", "after",
                        "window_minutes", "message"]
_RUNTIME_CONFIG_NAME = "runtime_config.json"


def _surge_live_dir(live_dir: Path | None) -> Path:
    if live_dir is not None:
        return live_dir
    from rquant.config import settings

    return settings.data_dir / _SURGE_LIVE_DIR_NAME


def _read_jsonl_records(path: Path, required_key: str) -> list[dict]:
    """逐行读 jsonl，跳过坏行/缺关键字段的行；文件缺失 → 空。"""
    if not path.exists():
        return []
    records: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(obj, dict) and obj.get(required_key):
            records.append(obj)
    return records


def load_surge_log(day: date | None = None, *, live_dir: Path | None = None) -> pd.DataFrame:
    """读当日 surge-watch events jsonl，**每标的只保留 confirmed_at 最早的一行**，按时间升序。

    默认读 ``settings.data_dir/surge_live/events-{day}.jsonl``（day 缺省今日 CST）；
    ``live_dir`` 可注入（单测）。文件缺失/空 → 空表（带标准列）；坏行（非法 JSON /
    非 dict / 缺 ts_code）逐行跳过，不让单条脏数据拖垮整表。confirmed_at 为定长
    ``HH:MM``，字典序即时间序，故排序后按 ts_code 保留首行即当日最早识别时刻。
    """
    if _fake_enabled():
        return _fake_surge_log()
    if day is None:
        day = datetime.now(_CST).date()
    path = _surge_live_dir(live_dir) / f"events-{day.isoformat()}.jsonl"
    records = _read_jsonl_records(path, "ts_code")
    if not records:
        return pd.DataFrame(columns=_SURGE_LOG_COLUMNS)

    df = pd.DataFrame(records)
    if "confirmed_at" not in df.columns:
        df["confirmed_at"] = ""
    df["confirmed_at"] = df["confirmed_at"].fillna("").astype(str)
    return (
        df.sort_values("confirmed_at", kind="stable")
        .drop_duplicates(subset="ts_code", keep="first")
        .reset_index(drop=True)
    )


def load_pulse_history(
    day: date | None = None, *, live_dir: Path | None = None
) -> pd.DataFrame:
    """当日脉搏分钟历史（surge-watch 落盘）。缺失/坏行降级，列契约恒定。"""
    if _fake_enabled():
        return _fake_pulse_history()
    if day is None:
        day = datetime.now(_CST).date()
    path = _surge_live_dir(live_dir) / f"pulse-{day.isoformat()}.jsonl"
    records = _read_jsonl_records(path, "t")
    if not records:
        return pd.DataFrame(columns=_PULSE_LOG_COLUMNS)
    df = pd.DataFrame(records)
    for col in _PULSE_LOG_COLUMNS:
        if col not in df.columns:
            df[col] = float("nan")
    return df[_PULSE_LOG_COLUMNS]


def load_pulse_alerts(
    day: date | None = None, *, live_dir: Path | None = None
) -> pd.DataFrame:
    """当日脉搏异动事件（surge-watch 落盘），按文件顺序（即时间序）。"""
    if _fake_enabled():
        return _fake_pulse_alerts()
    if day is None:
        day = datetime.now(_CST).date()
    path = _surge_live_dir(live_dir) / f"pulse_alerts-{day.isoformat()}.jsonl"
    records = _read_jsonl_records(path, "t")
    if not records:
        return pd.DataFrame(columns=_PULSE_ALERT_COLUMNS)
    df = pd.DataFrame(records)
    for col in _PULSE_ALERT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[_PULSE_ALERT_COLUMNS]


def load_surge_runtime_config(*, live_dir: Path | None = None) -> dict | None:
    """surge-watch 当前生效口径（启动时原子写）。缺失/损坏 → None。"""
    if _fake_enabled():
        return {"boards": ["main", "gem", "star", "bj"], "k_rough": 1.2,
                "k_cum": 2.5, "ratio_cap": 8.0, "tushare_rate_per_min": 2}
    path = _surge_live_dir(live_dir) / _RUNTIME_CONFIG_NAME
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def load_surge_marks(
    ts_code: str, dates: list[date], *, live_dir: Path | None = None
) -> pd.DataFrame:
    """指定交易日集合内该票每天最早爆量确认（图表标记源）。

    逐日复用 load_surge_log（每票保留最早行的语义一致）；某日文件缺失自然跳过。
    """
    rows: list[dict] = []
    for d in dates:
        df = load_surge_log(d, live_dir=live_dir)
        if df.empty or "ts_code" not in df.columns:
            continue
        sub = df[df["ts_code"].astype(str) == ts_code]
        if sub.empty:
            continue
        r = sub.iloc[0]
        rows.append({
            "date": d,
            "confirmed_at": str(r.get("confirmed_at", "")),
            "rel_cum": pd.to_numeric(pd.Series([r.get("rel_cum")]), errors="coerce").iloc[0],
        })
    return pd.DataFrame(rows, columns=["date", "confirmed_at", "rel_cum"])


# ── A4 Fake 模式确定性 fixture（RQUANT_PANORAMA_FAKE=1，e2e 可测性） ────────────
#
# 全部固定 seed / 硬编码，列与类型必须与真实路径完全一致。核心样本：
# 30 只主板票（600001..600030.SH），涨停 600001/600002（price==limit_up_price），
# 炸板 600003（high 触涨停价但现价回落）；东财成分含行业+概念两 idx_type；
# 开盘啦成分含「人形机器人」（6 只，含两只涨停）；资金流 BK 码可精确 join。

_FAKE_CODES = [f"6000{i:02d}.SH" for i in range(1, 31)]


def _fake_snapshot() -> pd.DataFrame:
    """30 只主板快照（未过 add_limit_prices），涨停/炸板价与 10% 分档吻合。"""
    rng = np.random.RandomState(20260706)
    n = len(_FAKE_CODES)
    pre_close = np.round(np.linspace(8.0, 60.0, n), 2)
    limit_up = _round_half_up(pd.Series(pre_close) * 1.10).to_numpy()  # 主板 10%，half-up
    price = np.round(pre_close * (1 + rng.uniform(-0.03, 0.06, n)), 2)
    open_ = np.round(pre_close * (1 + rng.uniform(-0.02, 0.02, n)), 2)
    high = np.round(np.maximum(price, pre_close * (1 + rng.uniform(0.0, 0.05, n))), 2)
    low = np.round(np.minimum(price, pre_close * (1 - rng.uniform(0.0, 0.03, n))), 2)
    # 强制两只涨停（price 精确 == limit_up_price）
    for i in (0, 1):
        price[i] = limit_up[i]
        high[i] = limit_up[i]
        low[i] = round(pre_close[i] * 0.98, 2)
        open_[i] = round(pre_close[i] * 1.01, 2)
    # 强制一只炸板（high 触涨停价、price 回落未涨停）
    price[2] = round(limit_up[2] - 0.30, 2)
    high[2] = limit_up[2]
    low[2] = round(pre_close[2] * 0.99, 2)
    open_[2] = round(pre_close[2] * 1.02, 2)

    pct_chg = np.round((price / pre_close - 1) * 100, 2)
    volume = np.round(rng.uniform(1e6, 5e7, n), 0)
    amount = np.round(price * volume, 0)
    return pd.DataFrame(
        {
            "ts_code": _FAKE_CODES,
            "name": [f"样本{i:02d}" for i in range(1, n + 1)],
            "price": price,
            "open": open_,
            "high": high,
            "low": low,
            "pre_close": pre_close,
            "pct_chg": pct_chg,
            "volume": volume,
            "amount": amount,
        }
    )


def _fake_board_members() -> pd.DataFrame:
    """东财成分：半导体（行业，600001..600012）+ AI算力（概念，600010..600021）。"""
    rows: list[tuple[str, str, str, str]] = []
    for i in range(1, 13):
        rows.append(("BK0001.DC", "半导体", "行业板块", f"6000{i:02d}.SH"))
    for i in range(10, 22):
        rows.append(("BK0002.DC", "AI算力", "概念板块", f"6000{i:02d}.SH"))
    return pd.DataFrame(rows, columns=["board_code", "board_name", "idx_type", "con_code"])


def _fake_kpl_members() -> pd.DataFrame:
    """开盘啦成分：人形机器人（6 只，含涨停 600001/600002）+ 存储芯片（3 只）。"""
    rows: list[tuple[str, str, str]] = []
    humanoid = ["600001.SH", "600002.SH", "600005.SH", "600010.SH", "600015.SH", "600020.SH"]
    for c in humanoid:
        rows.append(("000001.KP", "人形机器人", c))
    for c in ["600011.SH", "600012.SH", "600013.SH"]:
        rows.append(("000002.KP", "存储芯片", c))
    return pd.DataFrame(rows, columns=["board_code", "board_name", "con_code"])


def _fake_sector_fund_flow(sector_type: str = "行业资金流") -> pd.DataFrame:
    """资金流：board_code（BK 码）可精确 join 成分板块（BK0001/BK0002），route=em_direct。"""
    if sector_type == "概念资金流":
        rows = [
            {"board_code": "BK0002", "board_name": "AI算力", "pct_chg": 5.20,
             "main_net_amount": 8.80e8, "main_net_rate": 6.10, "leading_stock": "样本10"},
            {"board_code": "BK0301", "board_name": "云计算", "pct_chg": 2.10,
             "main_net_amount": 3.20e8, "main_net_rate": 3.30, "leading_stock": "某云股"},
        ]
    else:
        rows = [
            {"board_code": "BK0001", "board_name": "半导体", "pct_chg": 4.30,
             "main_net_amount": 1.23e9, "main_net_rate": 7.20, "leading_stock": "样本01"},
            {"board_code": "BK0201", "board_name": "银行", "pct_chg": -0.80,
             "main_net_amount": -5.60e8, "main_net_rate": -2.10, "leading_stock": "某行"},
        ]
    df = pd.DataFrame(rows)[
        ["board_code", "board_name", "pct_chg", "main_net_amount", "main_net_rate", "leading_stock"]
    ]
    return _with_route(df, "em_direct")


def _fake_pool_flags() -> dict[str, str]:
    """池内标记 fixture：pool1 / pool2 / 两者拼接各覆盖一只。"""
    return {
        "600001.SH": "pool1:fake_growth",
        "600005.SH": "pool2",
        "600010.SH": "pool1:fake_growth + pool2",
    }


def _fake_liquidity_baseline() -> pd.DataFrame:
    """流动性基准 fixture：circ_mv(万元) + avg_amount_5d(元)，覆盖全部快照票。"""
    n = len(_FAKE_CODES)
    circ_mv = np.round(np.linspace(200_000.0, 8_000_000.0, n), 0)  # 20亿..800亿（万元）
    avg_amount_5d = np.round(np.linspace(2.0e8, 3.0e9, n), 0)  # 元
    return pd.DataFrame(
        {"ts_code": _FAKE_CODES, "circ_mv": circ_mv, "avg_amount_5d": avg_amount_5d}
    )


def _session_minute_stamps(day: pd.Timestamp) -> pd.DatetimeIndex:
    """单个交易日的 240 根分钟时间戳：上午 09:30–11:29 + 下午 13:00–14:59。

    含真实午休断裂（不产出 11:30–12:59 任一分钟），fake 分时/5日图才能复现空档，
    修复（idx 序号轴消空档）方可视觉验证。
    """
    base = pd.Timestamp(day).normalize()
    morning = pd.date_range(base + pd.Timedelta("9h30min"), periods=120, freq="min")
    afternoon = pd.date_range(base + pd.Timedelta("13h"), periods=120, freq="min")
    return morning.append(afternoon)


def _fake_intraday_trend(ts_code: str, ndays: int = 1) -> pd.DataFrame:
    """分时/5日 fixture：每交易日 240 根真实时段时间戳（含午休断裂），5 日=5 组。

    数值走连续 x=arange(count) 的平滑正弦（与旧版一致，只把 dt 换成真实交易时段），
    route=em_direct。5 日锚定最近 ndays 个工作日（bdate_range），故 5 组各自独立日期。
    """
    per_day = 240
    count = per_day * ndays
    days = pd.bdate_range(end="2026-07-06", periods=ndays)
    dt = pd.DatetimeIndex(
        np.concatenate([_session_minute_stamps(day).to_numpy() for day in days])
    )
    x = np.arange(count)
    price = np.round(20.0 + 2.0 * np.sin(x / 30.0) + x * 0.001, 2)
    avg_price = np.round(pd.Series(price).expanding().mean().to_numpy(), 2)
    volume = np.round(10_000.0 + (np.sin(x / 10.0) + 1.0) * 5_000.0, 0)
    df = pd.DataFrame({"dt": dt, "price": price, "avg_price": avg_price, "volume": volume})
    return _with_route(df, "em_direct")


def _fake_daily_kline(ts_code: str, n: int = 120) -> pd.DataFrame:
    """日K fixture：120 根 + 就地滚动 MA（首 4 根 ma5 NaN），列同真实路径。"""
    count = 120
    # start 锚定周一，periods 保证恰好 120 根（end 锚在周末会少一根）
    dates = pd.bdate_range(start="2026-01-05", periods=count)
    x = np.arange(count)
    close = np.round(20.0 + 3.0 * np.sin(x / 15.0) + x * 0.02, 2)
    # 阳/阴线按 index 奇偶交替（偶数根 open 低于 close 收阳、奇数根收阴），
    # 确定性无随机——保证红/绿两条蜡烛渲染路径都有视觉覆盖
    direction = np.where(x % 2 == 0, -1.0, 1.0)
    df = pd.DataFrame(
        {
            "trade_date": dates,
            "open": np.round(close + 0.20 * direction, 2),
            "high": np.round(close + 0.50, 2),
            "low": np.round(close - 0.50, 2),
            "close": close,
            # 量纲=手（对齐真实路径 daily_bar.vol）：与快照 volume(股)÷100 同量级，
            # UI 当日拼接 bar 的量柱在 fake 模式下才有代表性
            "volume": np.round(1.0e5 + (np.sin(x / 8.0) + 1.0) * 1.0e5, 0),
        }
    )
    for w in (5, 10, 20):
        df[f"ma{w}"] = df["close"].rolling(window=w, min_periods=w).mean()
    return df


def _fake_surge_log() -> pd.DataFrame:
    """3 条确定性爆量台账：与 _FAKE_CODES 网格对齐（600001 涨停、600003 炸板）。"""
    rows = [
        {"confirmed_at": "09:47", "ts_code": "600001.SH", "name": "假票600001",
         "theme": "人形机器人", "price": 11.0, "pct_chg": 10.0, "rel_cum": 3.2,
         "cum_amount": 5.2e8, "room_to_limit_pct": 0.0, "status": "unbuyable"},
        {"confirmed_at": "10:12", "ts_code": "600003.SH", "name": "假票600003",
         "theme": "人形机器人", "price": 10.4, "pct_chg": 4.0, "rel_cum": 5.1,
         "cum_amount": 3.1e8, "room_to_limit_pct": 5.8, "status": "confirmed"},
        {"confirmed_at": "13:05", "ts_code": "600010.SH", "name": "假票600010",
         "theme": "算力", "price": 12.6, "pct_chg": 6.3, "rel_cum": 2.7,
         "cum_amount": 2.4e8, "room_to_limit_pct": 3.5, "status": "confirmed"},
    ]
    return pd.DataFrame(rows)


def _fake_pulse_history() -> pd.DataFrame:
    """09:30 起 120 分钟确定性脉搏序列（涨停缓升、炸板阶梯、占比爬升）。"""
    rows: list[dict] = []
    minute = 9 * 60 + 30
    for i in range(120):
        rows.append({
            "t": f"{minute // 60:02d}:{minute % 60:02d}",
            "limit_up": 20 + i // 6, "limit_down": 3 + i // 40, "broken": 2 + i // 15,
            "up": 2600 + i * 5, "down": 2400 - i * 5,
            "up_ratio_pct": round(48 + i * 0.1, 2), "total": 5400,
        })
        minute += 1
        if minute == 11 * 60 + 31:
            minute = 13 * 60 + 1
    return pd.DataFrame(rows, columns=_PULSE_LOG_COLUMNS)


def _fake_pulse_alerts() -> pd.DataFrame:
    """1 条炸板潮；t 取当前时间 − 5 分钟（唯一非硬编码字段，保证提示条恒可见）。"""
    t = (datetime.now(_CST) - timedelta(minutes=5)).strftime("%H:%M")
    return pd.DataFrame([{
        "t": t, "kind": "broken_surge", "kind_label": "炸板潮",
        "before": 2.0, "after": 6.0, "window_minutes": 10,
        "message": "炸板 10 分钟 2 → 6（+4）",
    }], columns=_PULSE_ALERT_COLUMNS)
