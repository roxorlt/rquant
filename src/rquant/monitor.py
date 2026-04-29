"""盘中实时监控：加载 watchlist → 轮询 akshare → 档位检测 → 弹窗 + 存库。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, datetime

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
    name: str = ""
    entry_date: date | None = None  # pool2 入池日；pool1 用 limit_up_date
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
        raw_entry = row["entry_date"]
        entry_d = raw_entry.date() if hasattr(raw_entry, "date") else raw_entry
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
            entry_date=entry_d,
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

        lu_raw = state_df.iloc[0]["trade_date"]
        lu_date = lu_raw.date() if hasattr(lu_raw, "date") else lu_raw
        items[code] = WatchItem(
            ts_code=code,
            pool="pool1",
            limit_up_date=lu_date,
            body_upper=bu,
            body_lower=bl,
            body=bu - bl,
            level_40=levels["level_40"],
            level_30=levels["level_30"],
            level_20=levels["level_20"],
            stop_strong=levels["stop_strong"],
            stop_weak=levels["stop_weak"],
            entry_date=lu_date,  # pool1 用涨停日做参考
        )

    # 3. 批量填充股票名称
    if items:
        codes = list(items.keys())
        placeholders = ",".join("?" * len(codes))
        name_df = store._conn.execute(
            f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})",
            codes,
        ).fetchdf()
        name_map = dict(zip(name_df["ts_code"], name_df["name"]))
        for code, item in items.items():
            item.name = name_map.get(code, "")

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

    用 akshare sina 源 stock_zh_a_spot——东方财富源 stock_zh_a_spot_em
    在云服务器（如腾讯云）被屏蔽（Remote end closed）。

    sina 代码格式 'sh600519' / 'sz000001' / 'bj920000'，按后 6 位匹配
    rQuant ts_code（'600519.SH'）。
    """
    try:
        df = ak.stock_zh_a_spot()
    except Exception:
        logger.error("akshare 实时行情获取失败")
        return {}

    code_map = {c.split(".")[0]: c for c in ts_codes}
    wanted = set(code_map.keys())

    result = {}
    for _, row in df.iterrows():
        raw_code = str(row["代码"])
        # sina 代码带前缀 sh/sz/bj，取后 6 位；em 源是纯 6 位也兼容
        ak_code = raw_code[-6:] if len(raw_code) >= 6 else raw_code
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


_LEVEL_LABELS = {
    "40": "40%", "30": "30%", "20": "20%",
    "strong": "强止", "weak": "弱止",
}


def _count_trading_days_since(
    store: DuckDBStore, entry_date: date, today: date
) -> int:
    """entry_date 到 today 之间有多少个交易日（含两端）。"""
    row = store._conn.execute(
        """
        SELECT COUNT(DISTINCT trade_date) FROM daily_bar
        WHERE trade_date >= ? AND trade_date <= ?
        """,
        [entry_date, today],
    ).fetchone()
    return row[0] if row else 0


def check_exits(store: DuckDBStore, today: date) -> int:
    """收盘后检查 Pool 2 退出。

    breakdown（跌破止损）→ 自动 update_pool2_exit
    expired（超期 ≥3 日）→ 保留 active，加入待决策列表，由用户次日 CLI 处理

    末尾推 notify('pool2_exit', ...) 汇总（无事件不推）。
    Returns: 自动踢出数量。
    """
    from rquant.notify import notify

    active = store.query_pool2_active()
    if active.empty:
        return 0

    # 批量取股票名
    codes = active["ts_code"].tolist()
    placeholders = ",".join("?" * len(codes))
    name_df = store._conn.execute(
        f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})",
        codes,
    ).fetchdf()
    name_map = dict(zip(name_df["ts_code"], name_df["name"]))

    auto_kicked: list[dict] = []
    expired_held: list[dict] = []

    for _, row in active.iterrows():
        code = row["ts_code"]
        raw_entry = row["entry_date"]
        entry_date = raw_entry.date() if hasattr(raw_entry, "date") else raw_entry

        stop_s = float(row["stop_strong"])
        stop_w = float(row["stop_weak"])

        close_row = store._conn.execute(
            "SELECT close FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            [code, today],
        ).fetchone()
        if close_row is None:
            continue

        close_price = float(close_row[0])
        days = _count_trading_days_since(store, entry_date, today)

        if close_price < stop_w:
            store.update_pool2_exit(code, today, "breakdown")
            auto_kicked.append({
                "ts_code": code,
                "name": name_map.get(code, ""),
                "close": close_price,
                "threshold": stop_w,
                "reason_label": "弱止",
            })
            logger.info(f"Pool 2 自动踢出: {code} 跌破弱止 ¥{stop_w:.2f}")
        elif close_price < stop_s:
            store.update_pool2_exit(code, today, "breakdown")
            auto_kicked.append({
                "ts_code": code,
                "name": name_map.get(code, ""),
                "close": close_price,
                "threshold": stop_s,
                "reason_label": "强止",
            })
            logger.info(f"Pool 2 自动踢出: {code} 跌破强止 ¥{stop_s:.2f}")
        elif days >= 3:
            expired_held.append({
                "ts_code": code,
                "name": name_map.get(code, ""),
                "entry_date": entry_date,
                "days_in_pool": days,
            })
            logger.info(f"Pool 2 超期保留: {code} 第 {days} 日")

    if auto_kicked or expired_held:
        notify(
            "pool2_exit",
            trade_date=today,
            auto_kicked=auto_kicked,
            expired_held=expired_held,
        )

    return len(auto_kicked)


def _now() -> datetime:
    """当前时间（方便测试 mock）。"""
    return datetime.now()


def _is_trading_hours() -> bool:
    """当前是否在交易时段（09:30-11:30 或 13:00-15:00）。"""
    now = _now()
    t = now.hour * 100 + now.minute
    return (930 <= t <= 1130) or (1300 <= t <= 1500)


def _wait_for_market_open() -> None:
    """如果当前时间在 09:30 前 10 分钟内，sleep 到 09:30 开盘。

    用于 launchd 09:29 触发后等到 09:30 进轮询。超过 10 分钟提前的不等
    （RunAtLoad 早晨开机/手动早执行场景），避免长时间空转。
    """
    now = _now()
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if now >= market_open:
        return
    wait_seconds = (market_open - now).total_seconds()
    if wait_seconds > 600:
        return
    logger.info(f"等待 {wait_seconds:.0f} 秒到 09:30 开盘")
    time.sleep(wait_seconds)


def run_monitor(interval: int = 5) -> int:
    """盘中监控主循环。"""
    from rquant.notify import notify

    today = date.today()

    if not is_trading_day(today):
        logger.info(f"{today} 非交易日，退出")
        return 0

    with DuckDBStore() as store:
        watchlist = build_watchlist(store)

        if not watchlist:
            logger.warning("Watchlist 为空，退出")
            return 0

        logger.info(f"开始监控 {len(watchlist)} 只，间隔 {interval} 秒")

        ts_codes = [item.ts_code for item in watchlist]
        item_map = {item.ts_code: item for item in watchlist}
        today_str = today.isoformat()
        triggers_summary: dict[str, int] = {}

        notify(
            "heartbeat",
            event="start",
            watchlist_count=len(watchlist),
            pool1_count=sum(1 for i in watchlist if i.pool == "pool1"),
            pool2_count=sum(1 for i in watchlist if i.pool == "pool2"),
        )

        _wait_for_market_open()

        while _is_trading_hours():
            prices = fetch_realtime_prices(ts_codes)

            for code, pdata in prices.items():
                item = item_map.get(code)
                if item is None:
                    continue

                events = check_levels(
                    item, pdata["price"], pdata["low"]
                )

                for evt in events:
                    # 存库
                    evt_df = pd.DataFrame([{
                        "trade_date": today,
                        "ts_code": code,
                        "level": evt["level"],
                        "trigger_price": evt["trigger_price"],
                        "level_price": evt["level_price"],
                        "trigger_time": _now(),
                        "trigger_type": evt["trigger_type"],
                        "pool": item.pool,
                        "body_upper": item.body_upper,
                        "body_lower": item.body_lower,
                    }])
                    store.upsert_monitor_event(evt_df)

                    triggers_summary[evt["level"]] = (
                        triggers_summary.get(evt["level"], 0) + 1
                    )

                    days = _count_trading_days_since(
                        store,
                        item.entry_date or item.limit_up_date,
                        today,
                    )
                    ref_date = item.entry_date or item.limit_up_date

                    notify(
                        "price_level",
                        ts_code=code,
                        name=item.name,
                        level=evt["level"],
                        trigger_price=evt["trigger_price"],
                        body_upper=item.body_upper,
                        body_lower=item.body_lower,
                        level_40=item.level_40,
                        level_30=item.level_30,
                        level_20=item.level_20,
                        stop_strong=item.stop_strong,
                        stop_weak=item.stop_weak,
                        pool=item.pool,
                        entry_date=ref_date,
                        days_in_pool=days,
                    )

            time.sleep(interval)

        # 收盘后：事件汇总
        all_events = store.query_monitor_events(today_str)
        if not all_events.empty:
            logger.info(f"当日事件汇总: {len(all_events)} 条")
            for _, e in all_events.iterrows():
                logger.info(
                    f"  {e['ts_code']} {e['level']} "
                    f"¥{e['trigger_price']:.2f} @ {e['trigger_time']}"
                )
        else:
            logger.info("当日无事件触发")

        # 收盘后：退出检查（Phase 4 改造为自动踢出 + 推送）
        auto_kicked_count = check_exits(store, today)

        notify(
            "heartbeat",
            event="stop",
            triggers_summary=triggers_summary,
            auto_kicked_count=auto_kicked_count or 0,
        )

    logger.info("监控结束")
    return 0
