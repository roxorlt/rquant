"""涨停池每日采集（东方财富，via akshare）。

`ak.stock_zt_pool_em` 只有**当天**有数据，历史日期返回空——封板资金 /
首次封板时间 / 炸板次数这些盘口口径字段无法事后回补，必须每日采集落库。
云服务器上东财源被屏蔽，本采集只在本地跑（sync-from-cloud.sh 日终触发）。

封单/成交额、封单/流通市值等派生比值在查询侧算，不落库。
"""

from __future__ import annotations

from datetime import date

import pandas as pd
from loguru import logger

from rquant.storage.duckdb import DuckDBStore

# akshare stock_zt_pool_em 中文列 → limit_up_pool_daily 列
_COLUMN_MAP: dict[str, str] = {
    "名称": "name",
    "涨跌幅": "pct_chg",
    "最新价": "close",
    "成交额": "amount",
    "流通市值": "circ_mv",
    "总市值": "total_mv",
    "换手率": "turnover_rate",
    "封板资金": "seal_amount",
    "首次封板时间": "first_seal_time",
    "最后封板时间": "last_seal_time",
    "炸板次数": "break_count",
    "涨停统计": "limit_up_stat",
    "连板数": "consecutive_boards",
    "所属行业": "industry",
}

_INT_COLUMNS = ("break_count", "consecutive_boards")
_SEAL_TIME_COLUMNS = ("first_seal_time", "last_seal_time")


def to_ts_code(symbol: str) -> str | None:
    """'002273' → '002273.SZ'。6 开头 .SH，0/3 开头 .SZ，4/8/9 开头 .BJ。"""
    sym = str(symbol).strip()
    if len(sym) != 6 or not sym.isdigit():
        return None
    head = sym[0]
    if head == "6":
        return f"{sym}.SH"
    if head in ("0", "3"):
        return f"{sym}.SZ"
    if head in ("4", "8", "9"):
        return f"{sym}.BJ"
    return None


def _normalize_seal_time(value: object) -> str | None:
    """东财封板时间统一存 VARCHAR。数值型（如 int 92500）丢首位 0，补齐 6 位。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    if s.isdigit() and len(s) < 6:
        s = s.zfill(6)
    return s


def _as_optional_str(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)


def normalize_zt_pool(raw: pd.DataFrame, trade_date: date) -> pd.DataFrame:
    """东财原始中文列映射为 limit_up_pool_daily 行。

    代码段无法映射交易所（非 6/0/3/4/8/9 开头）的行丢弃并 warning。
    """
    if raw.empty:
        return pd.DataFrame()
    if "代码" not in raw.columns:
        raise ValueError("stock_zt_pool_em 返回缺少 '代码' 列，东财列名可能变更")

    out = pd.DataFrame(index=raw.index)
    out["ts_code"] = raw["代码"].map(to_ts_code)
    for src, dst in _COLUMN_MAP.items():
        out[dst] = raw[src] if src in raw.columns else None

    dropped = int(out["ts_code"].isna().sum())
    if dropped:
        logger.warning(f"涨停池 {dropped} 行代码段无法映射交易所，已丢弃")
    out = out[out["ts_code"].notna()].copy()

    out["trade_date"] = trade_date
    for col in _SEAL_TIME_COLUMNS:
        out[col] = out[col].map(_normalize_seal_time)
    for col in _INT_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    out["limit_up_stat"] = out["limit_up_stat"].map(_as_optional_str)
    out["source"] = "eastmoney"
    return out.reset_index(drop=True)


def _fetch_zt_pool(ds: str) -> pd.DataFrame:
    import akshare as ak

    return ak.stock_zt_pool_em(date=ds)


def capture_zt_pool(
    trade_date: date | None = None, store: DuckDBStore | None = None
) -> int:
    """抓当日涨停池并落库，返回写入行数。

    东财源失败 / 空返回 / 列名变更都不炸（warning + return 0）：日终链路
    容忍缺采，缺一天就少一天样本，不值得触发运维告警。
    """
    trading_date = trade_date or date.today()
    ds = trading_date.strftime("%Y%m%d")

    try:
        raw = _fetch_zt_pool(ds)
    except Exception as e:
        logger.warning(f"涨停池抓取失败（东财源偶发不可用）: date={ds} err={e}")
        return 0
    if raw is None or raw.empty:
        logger.warning(
            f"涨停池返回空: date={ds}（历史日期无数据 / 当日无涨停 / 源不可用）"
        )
        return 0

    try:
        df = normalize_zt_pool(raw, trading_date)
    except Exception as e:
        logger.warning(f"涨停池字段归一化失败: date={ds} err={e}")
        return 0
    if df.empty:
        logger.warning(f"涨停池归一化后为空: date={ds}")
        return 0

    owns_store = store is None
    store = store or DuckDBStore()
    try:
        rows = store.upsert_limit_up_pool(df)
    finally:
        if owns_store:
            store.close()
    logger.info(f"limit_up_pool_daily 写入 {rows} 行: date={ds}")
    return rows
