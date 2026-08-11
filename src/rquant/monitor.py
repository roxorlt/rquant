"""盘中实时监控：加载 watchlist → 轮询分钟行情 → 信号检测 → 推送 + 存库。"""

from __future__ import annotations

import re
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd
from loguru import logger
from pydantic import BaseModel

from rquant.config import settings
from rquant.pipeline import _compute_levels
from rquant.price_adjustment import resolve_price_basis_adjustment
from rquant.risk.blacklist import load_active_blacklist
from rquant.storage.duckdb import DuckDBStore


class RealtimeQuote(BaseModel):
    """盘中实时行情快照。"""

    ts_code: str
    price: float
    low: float
    open: float | None = None
    high: float | None = None
    pre_close: float | None = None
    pct_chg: float | None = None
    volume: float | None = None
    amount: float | None = None
    source: str = "sina"


class IntradayMinuteAdapter(Protocol):
    """监控所需的 Tushare 实时分钟接口。"""

    def rt_min(self, ts_codes: list[str], freq: str = "1min") -> pd.DataFrame:
        """返回最新一根实时分钟 K。"""
        ...

    def rt_min_daily(self, ts_codes: list[str], freq: str = "1min") -> pd.DataFrame:
        """返回当日开盘以来分钟 K。"""
        ...


@dataclass
class _CachedMinuteBar:
    ts_code: str
    trade_time: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    vol: float | None = None
    amount: float | None = None


class IntradayMinuteQuoteProvider:
    """用 Tushare 实时分钟线合成监控所需的当日行情快照。"""

    def __init__(
        self,
        *,
        adapter: IntradayMinuteAdapter | None = None,
        store: DuckDBStore | None = None,
        freq: str = "1min",
        daily_refresh_seconds: int = 300,
        rt_min_poll_seconds: int | None = None,
    ) -> None:
        if adapter is None:
            from rquant.adapter.tushare import TushareAdapter

            adapter = TushareAdapter()
        self._adapter = adapter
        self._store = store
        self._freq = freq
        self._daily_refresh_seconds = daily_refresh_seconds
        self._rt_min_poll_seconds = (
            settings.rt_min_poll_seconds
            if rt_min_poll_seconds is None
            else rt_min_poll_seconds
        )
        self._last_daily_refresh: datetime | None = None
        self._last_rt_min_success: datetime | None = None
        self._bars: dict[tuple[str, pd.Timestamp], _CachedMinuteBar] = {}
        self.used_fresh_tushare = False

    def fetch(self, ts_codes: list[str]) -> dict[str, RealtimeQuote]:
        """拉取分钟线、落库，并合成 {ts_code: RealtimeQuote}。

        rt_min 按 rt_min_poll_seconds 节流：主循环 interval（5s）比分钟级数据
        更新快，距上次成功拉取不足节流间隔时跳过 API 调用、用内存缓存合成。
        """
        if not ts_codes:
            return {}

        self.used_fresh_tushare = False
        poll_due = self._should_poll_rt_min()
        rt_min_ok = self._fetch_rt_min(ts_codes) if poll_due else False
        daily_ok = False
        if self._should_refresh_daily():
            daily_ok = self._fetch_rt_min_daily(ts_codes)
        # 节流跳过 rt_min 时，上次成功拉取仍在新鲜窗口（2×节流间隔）内视为新鲜；
        # 否则 run_monitor 会把全部 code 转 akshare 全量 fallback，节流反而放大调用量
        rt_min_fresh = rt_min_ok or (not poll_due and self._rt_min_cache_fresh())
        self.used_fresh_tushare = rt_min_fresh or daily_ok

        return {
            code: quote
            for code in ts_codes
            if (quote := self._quote_from_cache(code)) is not None
        }

    def _should_refresh_daily(self) -> bool:
        if self._last_daily_refresh is None:
            return True
        elapsed = (_now() - self._last_daily_refresh).total_seconds()
        return elapsed >= self._daily_refresh_seconds

    def _should_poll_rt_min(self) -> bool:
        if self._last_rt_min_success is None:
            return True
        elapsed = (_now() - self._last_rt_min_success).total_seconds()
        return elapsed >= self._rt_min_poll_seconds

    def _rt_min_cache_fresh(self) -> bool:
        """上次成功 rt_min 拉取是否仍在新鲜窗口（2×节流间隔）内。"""
        if self._last_rt_min_success is None:
            return False
        elapsed = (_now() - self._last_rt_min_success).total_seconds()
        return elapsed < self._rt_min_poll_seconds * 2

    def _fetch_rt_min(self, ts_codes: list[str]) -> bool:
        try:
            df = self._adapter.rt_min(ts_codes, freq=self._freq)
        except Exception:
            logger.exception("Tushare rt_min 获取失败，本轮保留已有分钟缓存")
            return False
        self._ingest_minute_frame(df)
        ok = df is not None and not df.empty
        if ok:
            self._last_rt_min_success = _now()
        return ok

    def _fetch_rt_min_daily(self, ts_codes: list[str]) -> bool:
        self._last_daily_refresh = _now()
        try:
            df = self._adapter.rt_min_daily(ts_codes, freq=self._freq)
        except Exception:
            logger.exception("Tushare rt_min_daily 获取失败，本轮仅使用最新分钟缓存")
            return False
        self._ingest_minute_frame(df)
        return df is not None and not df.empty

    def _ingest_minute_frame(self, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return

        required = {
            "ts_code", "trade_time", "freq", "open", "high",
            "low", "close", "vol", "amount",
        }
        missing = required - set(df.columns)
        if missing:
            logger.error(f"Tushare 分钟行情缺字段 {sorted(missing)}，本批跳过")
            return

        payload = df.copy()
        if "source" not in payload.columns:
            payload["source"] = "tushare"

        if self._store is not None:
            try:
                self._store.upsert_minute_bars(payload)
            except Exception:
                logger.exception("minute_bar 写入失败，继续使用内存行情")

        for _, row in payload.iterrows():
            if any(pd.isna(row[col]) for col in ("open", "high", "low", "close")):
                continue
            trade_time = pd.Timestamp(row["trade_time"])
            code = str(row["ts_code"])
            self._bars[(code, trade_time)] = _CachedMinuteBar(
                ts_code=code,
                trade_time=trade_time,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                vol=None if pd.isna(row["vol"]) else float(row["vol"]),
                amount=None if pd.isna(row["amount"]) else float(row["amount"]),
            )

    def _quote_from_cache(self, ts_code: str) -> RealtimeQuote | None:
        bars = sorted(
            (bar for (code, _), bar in self._bars.items() if code == ts_code),
            key=lambda bar: bar.trade_time,
        )
        if not bars:
            return None

        latest = bars[-1]
        volume = _sum_optional(bar.vol for bar in bars)
        amount = _sum_optional(bar.amount for bar in bars)
        return RealtimeQuote(
            ts_code=ts_code,
            price=latest.close,
            low=min(bar.low for bar in bars),
            open=bars[0].open,
            high=max(bar.high for bar in bars),
            volume=volume,
            amount=amount,
            source="tushare_rt_minute",
        )


def _sum_optional(values: Iterable[float | None]) -> float | None:
    nums = [float(v) for v in values if v is not None]
    if not nums:
        return None
    return float(sum(nums))


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
    reference_date: date | None = None  # 用于上攻监控的 T 日（pool2=entry_date, pool1=screen_date）
    t_high: float | None = None
    t_close: float | None = None
    limit_up_price_next: float | None = None
    blacklist_label: str | None = None
    blacklist_categories: list[str] = field(default_factory=list)
    triggered: dict[str, bool] = field(default_factory=lambda: {
        "attack_open_strength": False,
        "attack_strong_carry": False,
        "attack_break_high": False,
        "attack_near_limit": False,
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
    else:
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

    _hydrate_attack_references(store, items, screen_date=sd)

    # 3. 批量填充股票名称
    if items:
        codes = list(items.keys())
        placeholders = ",".join("?" * len(codes))
        name_df = store._conn.execute(
            f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})",
            codes,
        ).fetchdf()
        name_map = dict(zip(name_df["ts_code"], name_df["name"], strict=False))
        for code, item in items.items():
            item.name = name_map.get(code, "")

    # 4. 黑名单标签（保留全部，命中加 tag——已持仓不剔除）
    blacklist = load_active_blacklist(store)
    if blacklist:
        tagged = 0
        for code, item in items.items():
            hit = blacklist.get(code)
            if hit:
                item.blacklist_label = hit.list_label
                item.blacklist_categories = hit.sub_categories
                tagged += 1
        if tagged:
            logger.warning(f"Watchlist 中 {tagged} 只命中黑名单（保留+标签）")

    logger.info(
        f"Watchlist: {len(items)} 只 "
        f"(pool2={sum(1 for i in items.values() if i.pool == 'pool2')}, "
        f"pool1={sum(1 for i in items.values() if i.pool == 'pool1')})"
    )
    return list(items.values())


def _publish_research_watchlist(
    items: Sequence[WatchItem], trade_date: date
) -> Path | None:
    """Persist the pre-open expected universe when cloud research shadowing is on."""
    if not settings.research_cloud_ingest_enabled:
        return None
    from rquant.research_ingest import (
        ResearchWatchlistItem,
        write_research_watchlist_snapshot,
    )
    from rquant.research_manifest import detect_code_commit

    commit = detect_code_commit()
    if commit is None or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise RuntimeError("cannot publish research watchlist without a clean code commit")
    captured_at = _now()
    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        captured_at = captured_at.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return write_research_watchlist_snapshot(
        settings.research_staging_dir_resolved,
        trade_date=trade_date,
        items=tuple(
            ResearchWatchlistItem(ts_code=item.ts_code, pool=item.pool)
            for item in items
        ),
        captured_at=captured_at,
        code_commit=commit,
    )


def _as_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    return None


def _round_limit_price(value: float) -> float:
    return float(
        Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    )


def _hydrate_attack_references(
    store: DuckDBStore,
    items: dict[str, WatchItem],
    *,
    screen_date: str | None,
) -> None:
    """补齐上攻监控需要的 T 日高点、收盘价和次日涨停价。"""
    for item in items.values():
        reference_date = item.entry_date if item.pool == "pool2" else _as_date(screen_date)
        if reference_date is None:
            continue

        row = store._conn.execute(
            """
            SELECT db.high, db.close, ds.limit_pct
            FROM daily_bar db
            LEFT JOIN daily_state ds
              ON db.ts_code = ds.ts_code AND db.trade_date = ds.trade_date
            WHERE db.ts_code = ? AND db.trade_date = ?
            """,
            [item.ts_code, reference_date],
        ).fetchone()
        if row is None:
            continue

        high, close, limit_pct = row
        if pd.isna(high) or pd.isna(close) or pd.isna(limit_pct):
            continue

        item.reference_date = reference_date
        item.t_high = float(high)
        item.t_close = float(close)
        item.limit_up_price_next = _round_limit_price(
            item.t_close * (1 + float(limit_pct))
        )


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


def _optional_float(row: pd.Series, column: str) -> float | None:
    if column not in row.index:
        return None
    value = row[column]
    if pd.isna(value):
        return None
    return float(value)


def fetch_realtime_quotes(
    ts_codes: list[str],
) -> dict[str, RealtimeQuote]:
    """批量获取实时行情，返回 {ts_code: RealtimeQuote}。

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

    # 列存在性校验：akshare 改列名时（"代码"/"最新价"/"最低"）整轮返回空而非
    # 裸 KeyError 崩掉 monitor 进程。下一轮重试。
    required = {"代码", "最新价", "最低"}
    missing = required - set(df.columns)
    if missing:
        logger.error(f"akshare 实时行情列异常，缺 {missing}，本轮跳过")
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
                result[ts_code] = RealtimeQuote(
                    ts_code=ts_code,
                    price=float(price),
                    low=float(low),
                    open=_optional_float(row, "今开"),
                    high=_optional_float(row, "最高"),
                    pre_close=_optional_float(row, "昨收"),
                    pct_chg=_optional_float(row, "涨跌幅"),
                    volume=_optional_float(row, "成交量"),
                    amount=_optional_float(row, "成交额"),
                    source="sina",
                )
    return result


def fetch_realtime_prices(
    ts_codes: list[str],
) -> dict[str, dict[str, float]]:
    """兼容旧调用：批量获取实时行情，返回 {ts_code: {price, low, ...}}。"""
    quotes = fetch_realtime_quotes(ts_codes)
    result: dict[str, dict[str, float]] = {}
    for code, quote in quotes.items():
        payload = quote.model_dump(exclude_none=True)
        payload.pop("ts_code", None)
        payload.pop("source", None)
        result[code] = payload
    return result


def check_attack_signals(
    item: WatchItem,
    quote: RealtimeQuote,
) -> list[dict]:
    """检查次日上攻信号，返回新触发的事件列表。"""
    events: list[dict] = []

    def _append(
        level: str,
        trigger_price: float,
        level_price: float,
        trigger_type: str,
    ) -> None:
        if item.triggered.get(level, False):
            return
        item.triggered[level] = True
        events.append({
            "level": level,
            "trigger_price": trigger_price,
            "level_price": level_price,
            "trigger_type": trigger_type,
        })

    if item.t_close is None:
        return events

    adjustment = resolve_price_basis_adjustment(item.t_close, quote.pre_close)
    t_close = adjustment.adjust(item.t_close)
    t_high = adjustment.adjust(item.t_high)
    limit_up_price_next = adjustment.adjust(item.limit_up_price_next)
    if t_close is None:
        return events

    if quote.open is not None and t_close > 0:
        open_pct = (quote.open / t_close - 1) * 100
        if 2 <= open_pct <= 4:
            _append(
                "attack_open_strength",
                quote.open,
                t_close,
                "open_strength",
            )

    if t_high is not None:
        intraday_high = quote.high if quote.high is not None else quote.price
        if intraday_high > t_high:
            _append(
                "attack_break_high",
                intraday_high,
                t_high,
                "break_t_high",
            )

    if quote.low >= t_close and quote.price >= t_close:
        _append(
            "attack_strong_carry",
            quote.low,
            t_close,
            "strong_carry",
        )

    if (
        limit_up_price_next is not None
        and limit_up_price_next > 0
        and t_high is not None
    ):
        intraday_high = quote.high if quote.high is not None else quote.price
        distance_to_limit = (
            (limit_up_price_next - quote.price)
            / limit_up_price_next
            * 100
        )
        if (
            distance_to_limit <= 1.5
            and intraday_high > t_high
            and quote.low >= t_close
        ):
            _append(
                "attack_near_limit",
                quote.price,
                limit_up_price_next,
                "near_limit_up",
            )

    return events

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

    breakdown（跌破止损）→ 自动 update_pool2_exit(reason='breakdown')
    aged_out（入池超过 pool2_max_age_days 个交易日）→ 自动 update_pool2_exit(reason='aged_out')
    expired（超期 ≥3 日且未触发以上）→ 保留 active，加入待决策列表，由用户次日 CLI 处理

    末尾推 notify('pool2_exit', ...) 汇总（无事件不推）。
    Returns: 自动踢出数量（breakdown + aged_out 合计）。
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
    name_map = dict(zip(name_df["ts_code"], name_df["name"], strict=False))
    blacklist = load_active_blacklist(store)

    auto_kicked: list[dict] = []
    expired_held: list[dict] = []

    for _, row in active.iterrows():
        # 单票故障隔离：某只票数据异常 / update_pool2_exit 写入异常不应中断整批退出检查
        try:
            code = row["ts_code"]
            raw_entry = row["entry_date"]
            entry_date = raw_entry.date() if hasattr(raw_entry, "date") else raw_entry
            bl_label = blacklist[code].list_label if code in blacklist else None

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
                    "kind": "breakdown",
                    "close": close_price,
                    "threshold": stop_w,
                    "reason_label": "弱止",
                    "blacklist_label": bl_label,
                })
                logger.info(f"Pool 2 自动踢出: {code} 跌破弱止 ¥{stop_w:.2f}")
            elif close_price < stop_s:
                store.update_pool2_exit(code, today, "breakdown")
                auto_kicked.append({
                    "ts_code": code,
                    "name": name_map.get(code, ""),
                    "kind": "breakdown",
                    "close": close_price,
                    "threshold": stop_s,
                    "reason_label": "强止",
                    "blacklist_label": bl_label,
                })
                logger.info(f"Pool 2 自动踢出: {code} 跌破强止 ¥{stop_s:.2f}")
            elif days > settings.pool2_max_age_days:
                store.update_pool2_exit(code, today, "aged_out")
                auto_kicked.append({
                    "ts_code": code,
                    "name": name_map.get(code, ""),
                    "kind": "aged_out",
                    "entry_date": entry_date,
                    "days_in_pool": days,
                    "threshold_days": settings.pool2_max_age_days,
                    "blacklist_label": bl_label,
                })
                logger.info(
                    f"Pool 2 自动踢出: {code} 已入池 {days} 日，"
                    f"超过阈值 {settings.pool2_max_age_days}"
                )
            elif days >= 3:
                expired_held.append({
                    "ts_code": code,
                    "name": name_map.get(code, ""),
                    "entry_date": entry_date,
                    "days_in_pool": days,
                    "blacklist_label": bl_label,
                })
                logger.info(f"Pool 2 超期保留: {code} 第 {days} 日")
        except Exception:
            logger.exception(
                f"check_exits 单票处理失败，跳过: {row.get('ts_code', '?')}"
            )
            continue

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


def _market_phase(now: datetime | None = None) -> str:
    """A 股盘中阶段。

    - pre        ≤ 09:29  开盘前
    - morning    09:30-11:29  上午盘
    - lunch      11:30-12:59  午休（不轮询行情，避免 akshare 限频且静态价无意义）
    - afternoon  13:00-14:59  下午盘
    - closed     ≥ 15:00      收盘后
    """
    now = now or _now()
    t = now.hour * 100 + now.minute
    if t < 930:
        return "pre"
    if t < 1130:
        return "morning"
    if t < 1300:
        return "lunch"
    if t < 1500:
        return "afternoon"
    return "closed"


def _is_trading_hours() -> bool:
    """是否处于实盘可轮询时段（morning 或 afternoon）。"""
    return _market_phase() in ("morning", "afternoon")


def _wait_for_market_open() -> None:
    """如果当前时间在 09:30 前 10 分钟内，sleep 到 09:30 开盘。

    用于 timer 09:25 触发后等到 09:30 进轮询。超过 10 分钟提前的不等
    （早晨开机 / 手动早执行场景），避免长时间空转。
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


def _wait_for_afternoon_open() -> None:
    """午休期间睡到 13:00 下午开盘。每次 sleep 至多 60s，便于循环响应中断。"""
    now = _now()
    afternoon_open = now.replace(hour=13, minute=0, second=0, microsecond=0)
    if now >= afternoon_open:
        return
    wait_seconds = min((afternoon_open - now).total_seconds(), 60.0)
    time.sleep(wait_seconds)


def _publish_legacy_shadow_export(
    *,
    spool: object,
    trade_date: date,
) -> Path:
    """Mirror the closed legacy monitor output without reopening DuckDB."""

    from rquant.legacy_shadow_export import (
        LegacyMonitorCaptureSpool,
        publish_legacy_monitor_production_export,
    )
    if not isinstance(spool, LegacyMonitorCaptureSpool):
        raise TypeError("legacy monitor spool has an invalid contract")

    return publish_legacy_monitor_production_export(
        data_dir=settings.data_dir,
        trade_date=trade_date,
        spool=spool,
    )


def _prepare_legacy_shadow_spool(
    *,
    store: DuckDBStore,
    trade_date: date,
) -> object:
    from rquant.legacy_shadow_export import prepare_legacy_monitor_production_spool

    return prepare_legacy_monitor_production_spool(
        data_dir=settings.data_dir,
        trade_date=trade_date,
        rows=store.iter_monitor_events(trade_date.isoformat(), batch_size=1_000),
    )


def _recover_legacy_shadow_export(trade_date: date) -> None:
    from rquant.legacy_shadow_export import recover_production_legacy_shadow_exports

    recover_production_legacy_shadow_exports(
        data_dir=settings.data_dir,
        trade_date=trade_date,
        source="monitor",
    )


def run_monitor(interval: int = 5) -> int:
    """盘中监控主循环。"""
    from rquant.notify import notify

    today = date.today()

    if not is_trading_day(today):
        logger.info(f"{today} 非交易日，退出")
        return 0

    # 收盘后启动（launchd RunAtLoad / 晚间手动）不打开写库：DuckDB 写连接期间
    # 拒绝一切新连接，整晚占锁会挡日终 research-sync / 本地研究写入。
    # 9:25 systemd 正常启动时 phase 是 pre，不受影响，_wait_for_market_open 会等到 9:30。
    if _market_phase() == "closed":
        logger.info("已过收盘（>=15:00），仅尝试恢复既有 shadow staging")
        try:
            _recover_legacy_shadow_export(today)
        except Exception:
            logger.warning("legacy shadow monitor recovery unavailable; shadow will degrade")
        return 0

    with DuckDBStore() as store:
        watchlist = build_watchlist(store)

        if not watchlist:
            logger.warning("Watchlist 为空，退出")
            return 0

        try:
            snapshot_path = _publish_research_watchlist(watchlist, today)
            if snapshot_path is not None:
                logger.info(f"研究分钟预期清单已固化: {snapshot_path}")
        except Exception:
            # 监控继续运行；日终 research-ingest 会因缺少清单 fail closed。
            logger.exception("研究分钟预期清单固化失败")

        logger.info(f"开始监控 {len(watchlist)} 只，间隔 {interval} 秒")

        ts_codes = [item.ts_code for item in watchlist]
        item_map = {item.ts_code: item for item in watchlist}
        triggers_summary: dict[str, int] = {}
        quote_provider: IntradayMinuteQuoteProvider | None = None
        if settings.intraday_quote_source == "akshare":
            # 紧急回退开关：rt_min 付费权限到期 / 接口故障时不改代码切回纯 akshare
            logger.info("INTRADAY_QUOTE_SOURCE=akshare，主动关闭 Tushare 分钟行情主源")
        else:
            try:
                quote_provider = IntradayMinuteQuoteProvider(store=store)
            except Exception:
                logger.exception("Tushare 分钟行情 provider 初始化失败，回退 akshare")

        notify(
            "heartbeat",
            event="start",
            watchlist_count=len(watchlist),
            pool1_count=sum(1 for i in watchlist if i.pool == "pool1"),
            pool2_count=sum(1 for i in watchlist if i.pool == "pool2"),
        )

        _wait_for_market_open()

        last_phase: str | None = None
        while True:
            phase = _market_phase()

            if phase != last_phase:
                logger.info(f"phase: {last_phase or 'init'} → {phase}")
                last_phase = phase

            if phase == "closed":
                break

            if phase == "pre":
                # 极少进入（_wait_for_market_open 已 sleep 到 09:30）
                # 兜底：再等到开盘
                _wait_for_market_open()
                continue

            if phase == "lunch":
                # 午休不调 akshare（限频 + 静态价无意义），轻量 sleep 等下午开盘
                _wait_for_afternoon_open()
                continue

            # phase in ("morning", "afternoon")
            quotes = quote_provider.fetch(ts_codes) if quote_provider else {}
            if quote_provider is not None and not quote_provider.used_fresh_tushare:
                fallback_codes = ts_codes
            else:
                fallback_codes = [code for code in ts_codes if code not in quotes]
            if fallback_codes:
                fallback_quotes = fetch_realtime_quotes(fallback_codes)
                quotes.update(fallback_quotes)

            for code, quote in quotes.items():
                # 单票故障隔离：某只票的存库 / notify / 日期计算异常（盘中写锁竞争、
                # DuckDB 临时 IOError、单条数据异常等）不应终止整个盯盘进程。
                try:
                    item = item_map.get(code)
                    if item is None:
                        continue

                    events = check_attack_signals(item, quote)

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
                            blacklist_label=item.blacklist_label,
                            blacklist_categories=item.blacklist_categories,
                        )
                except Exception:
                    logger.exception(f"monitor 单票处理失败，跳过: {code}")
                    continue

            time.sleep(interval)

        # 收盘后：退出检查（Phase 4 改造为自动踢出 + 推送）
        auto_kicked_count = check_exits(store, today)

        notify(
            "heartbeat",
            event="stop",
            triggers_summary=triggers_summary,
            auto_kicked_count=auto_kicked_count or 0,
        )

        # DuckDB 内只分页写有界 spool；签名、rename、发布在 writer context 外执行。
        try:
            monitor_spool = _prepare_legacy_shadow_spool(store=store, trade_date=today)
        except Exception:
            monitor_spool = None
            logger.exception("legacy shadow monitor spool unavailable; shadow will degrade")

    if monitor_spool is not None:
        try:
            shadow_export = _publish_legacy_shadow_export(
                spool=monitor_spool,
                trade_date=today,
            )
            logger.info(f"legacy shadow monitor export: {shadow_export}")
        except Exception:
            logger.exception("legacy shadow monitor export unavailable; shadow will degrade")

    event_count = int(getattr(monitor_spool, "records_count", 0))
    if event_count:
        logger.info(f"当日事件汇总: {event_count} 条")
    else:
        logger.info("当日无事件触发")

    logger.info("监控结束")
    return 0
