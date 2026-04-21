"""盘中实时监控：加载 watchlist → 轮询 akshare → 档位检测 → 弹窗 + 存库。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import akshare as ak
import pandas as pd
from loguru import logger

from rquant.pipeline import _compute_levels
from rquant.storage.duckdb import DuckDBStore


@dataclass
class WatchItem:
    """单只监控标的。"""
    ts_code: str
    pool: str  # 'pool1' or 'pool2'
    limit_up_date: date
    body_upper: float
    body_lower: float
    body: float
    level_40: float
    level_30: float
    level_20: float
    stop_strong: float
    stop_weak: float
    triggered: dict[str, bool] = field(default_factory=lambda: {
        "40": False, "30": False, "20": False,
        "strong": False, "weak": False,
    })


def _get_latest_screen_date(store: DuckDBStore) -> str | None:
    """screen_result 中最新的 Pool 1 筛选日期。"""
    row = store._conn.execute(
        """
        SELECT strftime(MAX(trade_date), '%Y-%m-%d')
        FROM screen_result
        WHERE preset_name = 'n-shape-pool1'
        """
    ).fetchone()
    return row[0] if row and row[0] else None


def build_watchlist(
    store: DuckDBStore,
    screen_date: str | None = None,
) -> list[WatchItem]:
    """加载 Pool 2 active + 指定日期 Pool 1，去重后返回 watchlist。"""
    items: dict[str, WatchItem] = {}

    # 1. Pool 2 active（优先级高）
    p2_df = store.query_pool2_active()
    for _, row in p2_df.iterrows():
        code = row["ts_code"]
        bu, bl = float(row["body_upper"]), float(row["body_lower"])
        items[code] = WatchItem(
            ts_code=code,
            pool="pool2",
            limit_up_date=row["limit_up_date"],
            body_upper=bu,
            body_lower=bl,
            body=bu - bl,
            level_40=float(row["level_40"]),
            level_30=float(row["level_30"]),
            level_20=float(row["level_20"]),
            stop_strong=float(row["stop_strong"]),
            stop_weak=float(row["stop_weak"]),
        )

    # 2. Pool 1（screen_date 当天的，补充不在 Pool 2 中的）
    sd = screen_date or _get_latest_screen_date(store)
    if sd is None:
        logger.warning("无 Pool 1 数据")
        return list(items.values())

    p1_df = store.query_screen_result(sd, "n-shape-pool1")
    for _, row in p1_df.iterrows():
        code = row["ts_code"]
        if code in items:
            continue  # Pool 2 优先

        # 查涨停日 body
        state_df = store._conn.execute(
            """
            SELECT trade_date, body_upper, body_lower
            FROM daily_state
            WHERE ts_code = ? AND is_first_limit_up = true
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            [code],
        ).fetchdf()

        if state_df.empty:
            logger.warning(f"跳过 Pool 1 {code}：找不到涨停日")
            continue

        bu = float(state_df.iloc[0]["body_upper"])
        bl = float(state_df.iloc[0]["body_lower"])
        levels = _compute_levels(bu, bl)

        items[code] = WatchItem(
            ts_code=code,
            pool="pool1",
            limit_up_date=state_df.iloc[0]["trade_date"],
            body_upper=bu,
            body_lower=bl,
            body=bu - bl,
            level_40=levels["level_40"],
            level_30=levels["level_30"],
            level_20=levels["level_20"],
            stop_strong=levels["stop_strong"],
            stop_weak=levels["stop_weak"],
        )

    logger.info(
        f"Watchlist: {len(items)} 只 "
        f"(pool2={sum(1 for i in items.values() if i.pool == 'pool2')}, "
        f"pool1={sum(1 for i in items.values() if i.pool == 'pool1')})"
    )
    return list(items.values())


def is_trading_day(check_date: date) -> bool:
    """通过 akshare 交易日历检查是否为 A 股交易日。"""
    try:
        df = ak.tool_trade_date_hist_sina()
        trade_dates = set(
            pd.to_datetime(df["trade_date"]).dt.date
        )
        return check_date in trade_dates
    except Exception:
        logger.error("获取交易日历失败，默认当作交易日")
        return True


def fetch_realtime_prices(
    ts_codes: list[str],
) -> dict[str, dict[str, float]]:
    """批量获取实时行情，返回 {ts_code: {price, low}}。

    akshare 代码格式 "002415"，rQuant 用 "002415.SZ"。
    """
    try:
        df = ak.stock_zh_a_spot_em()
    except Exception:
        logger.error("akshare 实时行情获取失败")
        return {}

    # ts_code -> akshare 代码映射
    code_map = {c.split(".")[0]: c for c in ts_codes}
    wanted = set(code_map.keys())

    result = {}
    for _, row in df.iterrows():
        ak_code = str(row["代码"])
        if ak_code in wanted:
            ts_code = code_map[ak_code]
            price = row["最新价"]
            low = row["最低"]
            if pd.notna(price) and pd.notna(low):
                result[ts_code] = {
                    "price": float(price),
                    "low": float(low),
                }
    return result


def check_levels(
    item: WatchItem,
    current_price: float,
    daily_low: float,
) -> list[dict]:
    """检查实时价/当日最低是否触达各档位，返回新触发的事件列表。"""
    levels = [
        ("40", item.level_40),
        ("30", item.level_30),
        ("20", item.level_20),
        ("strong", item.stop_strong),
        ("weak", item.stop_weak),
    ]

    events = []
    for level_name, level_price in levels:
        if item.triggered[level_name]:
            continue

        if current_price <= level_price:
            item.triggered[level_name] = True
            events.append({
                "level": level_name,
                "trigger_price": current_price,
                "level_price": level_price,
                "trigger_type": "realtime",
            })
        elif daily_low <= level_price:
            item.triggered[level_name] = True
            events.append({
                "level": level_name,
                "trigger_price": daily_low,
                "level_price": level_price,
                "trigger_type": "daily_low",
            })

    return events
