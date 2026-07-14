"""历史分钟级强承接/突破回放。"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field

from rquant.paper import (
    PaperPosition,
    PaperTradeConfig,
    adjust_open_position_price_basis,
    check_position_exit,
    mark_position_to_quote,
)
from rquant.price_adjustment import resolve_price_basis_adjustment
from rquant.replay_entry_cache import (
    PriceBasisCheck,
    ReplayEntrySnapshot,
    ReplayWatchItem,
    RiskReplayQuote,
    replay_entry_snapshots_to_trades,
)
from rquant.signal_provenance import N_SHAPE_V1, build_signal_factors
from rquant.stock_features import (
    build_daily_stock_features,
    build_intraday_relative_volume_features,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.topn_selection import FeatureScoreTerm, score_feature_terms
from rquant.volume_profile import (
    VolumeProfile,
    VolumeProfileRuleConfig,
    build_volume_profile_risk_plan,
    calculate_volume_profile,
    scale_volume_profile,
)

MinuteFreq = Literal["1min", "5min", "15min", "30min", "60min"]
EntryMode = Literal[
    "first_break",
    "break_retest",
    "late_confirm",
    "vwap_confirm",
    "amount_surge",
    "factor_confirm",
]
INDEX_CONTEXT_LABELS = {
    "000001.SH": "sse",
    "399001.SZ": "szse",
    "399006.SZ": "chinext",
    "000300.SH": "csi300",
    "000905.SH": "csi500",
    "000852.SH": "csi1000",
}

# factor_confirm 评分阈值默认档（训练段网格 28/35/42 的中位档 ≈ 训练段首信号分钟得分
# 中位数，见 docs/analysis/2026-07-03-nshape-factor-confirm.md）
DEFAULT_FACTOR_SCORE_THRESHOLD = 35.0

# 日线 vol 单位为手，竞价 vol 为股，对齐 auction_gap_strategy.daily_vol_unit_factor
_DAILY_VOL_UNIT_FACTOR = 100.0

# N 字首板日回看窗口：pool1 首板在 T-1，pool2 在 T-2，留 1 天冗余
_SEAL_LOOKBACK_TRADING_DAYS = 3

# n_shape_b_v1 权重表：确认层评分（满分 100），与 N_SHAPE_V1_FACTORS 命中矩阵同键名。
# 只放已验证方向的弹药（低位百分位/竞价强度/封板质量/VWAP 位置）；rel_amount 相对放量
# 与市场温度已证伪收益方向，只进命中矩阵观察，不进权重表。全部 skip_if_missing：
# 静态因子缺数据（limit_list 缺行/新股无 250 日历史）降级为 0 贡献而非满分/半分。
# 数值区间按 N 字池训练段实测分布标定（B 日竞价量比中位数 ~0.013，非竞价跳空策略量级）。
N_SHAPE_B_V1_SCORE_TERMS: tuple[FeatureScoreTerm, ...] = (
    FeatureScoreTerm(
        name="auction_vol_ratio_5d", group="auction", weight=20.0,
        transform="log_ratio", cap=0.05, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="auction_gap_pct", group="auction", weight=10.0,
        transform="linear", low=0.0, high=6.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="seal_open_times_t", group="seal", weight=15.0,
        transform="inverse_linear", low=0.0, high=3.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="seal_fd_to_circ_pct_t", group="seal", weight=10.0,
        transform="linear", low=0.0, high=10.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="price_percentile_250d", group="trend", weight=20.0,
        transform="inverse_linear", low=0.0, high=1.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="ma_alignment", group="trend", weight=10.0,
        transform="linear", low=0.0, high=1.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="vwap_position", group="minute", weight=15.0,
        transform="linear", low=1.0, high=1.02, skip_if_missing=True,
    ),
)


class MinuteReplayConfig(BaseModel):
    """分钟 replay 参数。"""

    preset_name: str = "n-shape-pool1"
    freq: MinuteFreq = "1min"
    entry_mode: EntryMode = "first_break"
    max_hold_days: int = Field(default=5, ge=1, le=20)
    carry_low_ratio: float = Field(default=1.0, gt=0)
    carry_close_ratio: float = Field(default=1.0, gt=0)
    break_high_ratio: float = Field(default=1.0, gt=0)
    retest_tolerance_pct: float = Field(default=0.005, ge=0, lt=0.05)
    late_confirm_at: time = time(10, 30)
    vwap_buffer_pct: float = Field(default=0.0, ge=0, lt=0.02)
    amount_surge_lookback: int = Field(default=5, ge=1, le=30)
    amount_surge_min_prior_minutes: int = Field(default=2, ge=1, le=30)
    amount_surge_ratio: float = Field(default=2.0, gt=1, le=20)
    factor_score_threshold: float = Field(default=DEFAULT_FACTOR_SCORE_THRESHOLD, ge=0)
    price_discontinuity_pct: float = Field(default=0.01, gt=0, lt=1)
    paper: PaperTradeConfig = Field(default_factory=PaperTradeConfig)
    volume_profile: VolumeProfileRuleConfig = Field(
        default_factory=VolumeProfileRuleConfig
    )


@dataclass(frozen=True)
class _ReplayItem:
    ts_code: str
    pool: str
    name: str
    entry_date: date | None
    reference_date: date | None
    limit_up_date: date
    t_close: float | None
    t_high: float | None
    limit_up_price_next: float | None
    stop_weak: float


@dataclass(frozen=True)
class _MinuteQuote:
    ts_code: str
    price: float
    low: float
    high: float | None


def _parse_date(value: str | date | pd.Timestamp) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value[:10])


def _as_date(value: object) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    msg = f"无法转换为日期: {value!r}"
    raise ValueError(msg)


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    msg = f"无法转换为时间: {value!r}"
    raise ValueError(msg)


def _trading_dates(store: DuckDBStore) -> list[date]:
    df = store._conn.execute(
        """
        SELECT DISTINCT trade_date
        FROM daily_bar
        ORDER BY trade_date
        """
    ).fetchdf()
    return [_as_date(value) for value in df["trade_date"].tolist()]


def _next_trading_date(calendar: list[date], current: date) -> date | None:
    for trading_date in calendar:
        if trading_date > current:
            return trading_date
    return None


def _window_trading_dates(
    calendar: list[date],
    start: date,
    max_hold_days: int,
) -> list[date]:
    dates = [trading_date for trading_date in calendar if trading_date >= start]
    return dates[: max_hold_days + 1]


def _day_bounds(trading_date: date) -> tuple[datetime, datetime]:
    return (
        datetime.combine(trading_date, time(9, 30)),
        datetime.combine(trading_date, time(15, 0)),
    )


def _query_candidates(
    store: DuckDBStore,
    start: date,
    end: date,
    preset_name: str,
) -> pd.DataFrame:
    if preset_name == "n-shape-combined":
        return store._conn.execute(
            """
            WITH ranked AS (
                SELECT trade_date,
                       preset_name,
                       ts_code,
                       name,
                       close,
                       pct_chg,
                       CASE
                           WHEN preset_name = 'n-shape-pool2' THEN 2
                           ELSE 1
                       END AS pool_priority,
                       row_number() OVER (
                           PARTITION BY trade_date, ts_code
                           ORDER BY
                               CASE
                                   WHEN preset_name = 'n-shape-pool2' THEN 2
                                   ELSE 1
                               END DESC
                       ) AS rn
                FROM screen_result
                WHERE trade_date >= ?
                  AND trade_date <= ?
                  AND preset_name IN ('n-shape-pool1', 'n-shape-pool2')
            )
            SELECT trade_date, preset_name, ts_code, name, close, pct_chg
            FROM ranked
            WHERE rn = 1
            ORDER BY trade_date, pool_priority DESC, ts_code
            """,
            [start, end],
        ).fetchdf()
    return store._conn.execute(
        """
        SELECT trade_date, preset_name, ts_code, name, close, pct_chg
        FROM screen_result
        WHERE trade_date >= ?
          AND trade_date <= ?
          AND preset_name = ?
        ORDER BY trade_date, ts_code
        """,
        [start, end, preset_name],
    ).fetchdf()


def _query_daily_bar(
    store: DuckDBStore,
    ts_code: str,
    trading_date: date,
) -> pd.Series | None:
    df = store._conn.execute(
        """
        SELECT ts_code, trade_date, open, high, low, close, pre_close, pct_chg
        FROM daily_bar
        WHERE ts_code = ?
          AND trade_date = ?
        """,
        [ts_code, trading_date],
    ).fetchdf()
    if df.empty:
        return None
    return df.iloc[0]


def _query_open_auction_bar(
    store: DuckDBStore,
    ts_code: str,
    trading_date: date,
) -> pd.Series | None:
    """B 日开盘竞价行（tushare 优先于分钟回退源；price<=0 坏行按缺数据处理）。"""
    df = store._conn.execute(
        """
        SELECT price, vol,
               CASE WHEN source = 'tushare' THEN 0 ELSE 1 END AS source_priority
        FROM auction_bar
        WHERE ts_code = ?
          AND trade_date = ?
          AND auction_type = 'open_realtime'
        ORDER BY source_priority
        LIMIT 1
        """,
        [ts_code, trading_date],
    ).fetchdf()
    if df.empty:
        return None
    row = df.iloc[0]
    if pd.isna(row["price"]) or float(row["price"]) <= 0:
        return None
    return row


def _query_avg_vol_5d(
    store: DuckDBStore,
    ts_code: str,
    t_date: date,
) -> float | None:
    """截至 T 日的 5 日均量（股），不足 5 根日线视为缺数据（对齐 prev5_count==5 口径）。"""
    df = store._conn.execute(
        """
        SELECT vol
        FROM daily_bar
        WHERE ts_code = ?
          AND trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT 5
        """,
        [ts_code, t_date],
    ).fetchdf()
    vols = pd.to_numeric(df["vol"], errors="coerce").dropna() if not df.empty else df
    if len(vols) < 5:
        return None
    return float(vols.mean()) * _DAILY_VOL_UNIT_FACTOR


def _query_circ_mv(
    store: DuckDBStore,
    ts_code: str,
    trading_date: date,
) -> float | None:
    """daily_basic 流通市值，单位万元。"""
    row = store._conn.execute(
        """
        SELECT circ_mv
        FROM daily_basic
        WHERE ts_code = ?
          AND trade_date = ?
        """,
        [ts_code, trading_date],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    value = float(row[0])
    return value if value > 0 else None


def _query_above_ma20_ratio(
    store: DuckDBStore,
    trading_date: date,
) -> float | None:
    row = store._conn.execute(
        """
        SELECT above_ma20_ratio_pct
        FROM market_sentiment_daily
        WHERE trade_date = ?
        """,
        [trading_date],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def _previous_trading_date(calendar: list[date], current: date) -> date | None:
    previous: date | None = None
    for trading_date in calendar:
        if trading_date >= current:
            break
        previous = trading_date
    return previous


def _query_nshape_seal_quality(
    store: DuckDBStore,
    ts_code: str,
    t_date: date,
    calendar: list[date],
) -> pd.Series | None:
    """N 字首板日的官方封板质量（limit_list_daily limit_status='U'）。

    首板不在 T 日（pool1 在 T-1，pool2 在 T-2），取 [T-3 个交易日, T] 内最近一条
    涨停行；窗口内无行按缺数据处理。
    """
    prior = [d for d in calendar if d <= t_date]
    if not prior:
        return None
    window = prior[-(_SEAL_LOOKBACK_TRADING_DAYS + 1):]
    df = store._conn.execute(
        """
        SELECT trade_date, open_times, fd_amount, float_mv
        FROM limit_list_daily
        WHERE ts_code = ?
          AND trade_date >= ?
          AND trade_date <= ?
          AND limit_status = 'U'
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        [ts_code, window[0], t_date],
    ).fetchdf()
    if df.empty:
        return None
    return df.iloc[0]


def _prefetch_nshape_static_factors(
    store: DuckDBStore,
    *,
    ts_code: str,
    t_date: date,
    buy_date: date,
    calendar: list[date],
    t_close_raw: float,
) -> dict[str, float | int | None]:
    """factor_confirm 的静态因子（daily tier），每个候选只取一次，缺数据记 None。

    - 竞价强度/跳空：B 日 09:26 已知（auction_bar + B 日 pre_close）
    - 封板质量：N 字首板日官方涨停榜（[T-3, T] 内最近 'U' 行，B 日已知）
    - 低位/均线：T-1 收盘；温度：T-1 市场面（已证伪，只观察）
    """
    out: dict[str, float | int | None] = {
        "auction_vol_ratio_5d": None,
        "auction_gap_pct": None,
        "seal_open_times_t": None,
        "seal_fd_to_circ_pct_t": None,
        "price_percentile_250d": None,
        "ma_alignment": None,
        "market_above_ma20_ratio_pct": None,
    }

    auction = _query_open_auction_bar(store, ts_code, buy_date)
    if auction is not None:
        buy_daily = _query_daily_bar(store, ts_code, buy_date)
        pre_close = (
            float(buy_daily["pre_close"])
            if buy_daily is not None and pd.notna(buy_daily["pre_close"])
            else t_close_raw
        )
        if pre_close > 0:
            out["auction_gap_pct"] = round(
                (float(auction["price"]) / pre_close - 1) * 100, 4
            )
        avg_vol_5d = _query_avg_vol_5d(store, ts_code, t_date)
        if avg_vol_5d is not None and avg_vol_5d > 0 and pd.notna(auction["vol"]):
            out["auction_vol_ratio_5d"] = round(float(auction["vol"]) / avg_vol_5d, 4)

    seal = _query_nshape_seal_quality(store, ts_code, t_date, calendar)
    if seal is not None:
        if pd.notna(seal["open_times"]):
            out["seal_open_times_t"] = int(seal["open_times"])
        if pd.notna(seal["fd_amount"]):
            fd_amount = float(seal["fd_amount"])
            if pd.notna(seal["float_mv"]) and float(seal["float_mv"]) > 0:
                circ_value = float(seal["float_mv"])
            else:
                circ_wan = _query_circ_mv(store, ts_code, _as_date(seal["trade_date"]))
                circ_value = circ_wan * 1e4 if circ_wan is not None else 0.0
            if circ_value > 0:
                out["seal_fd_to_circ_pct_t"] = round(fd_amount / circ_value * 100, 4)

    t_minus_1 = _previous_trading_date(calendar, t_date)
    if t_minus_1 is not None:
        trend = build_daily_stock_features(store, ts_code, t_minus_1)
        out["price_percentile_250d"] = trend.get("price_percentile_250d")
        out["ma_alignment"] = trend.get("ma_alignment")
        out["market_above_ma20_ratio_pct"] = _query_above_ma20_ratio(store, t_minus_1)
    return out


def _query_market_sentiment(
    store: DuckDBStore,
    trading_date: date,
) -> dict[str, object]:
    df = store.query_market_sentiment(trading_date)
    if df is None or df.empty:
        return {}
    row = df.iloc[0]
    return {
        "market_stock_count": row["stock_count"],
        "market_up_count": row["up_count"],
        "market_down_count": row["down_count"],
        "market_limit_up_count": row["limit_up_count"],
        "market_first_limit_up_count": row["first_limit_up_count"],
        "market_limit_down_count": row["limit_down_count"],
        "market_yiziban_count": row["yiziban_count"],
        "market_max_consecutive_limit_ups": row["max_consecutive_limit_ups"],
        "market_high_board_count": row["high_board_count"],
        "market_up_ratio_pct": row["up_ratio_pct"],
        "market_limit_up_ratio_pct": row["limit_up_ratio_pct"],
        "market_avg_pct_chg": row["avg_pct_chg"],
        "market_median_pct_chg": row["median_pct_chg"],
        "market_total_amount": row["total_amount"],
    }


def _query_index_context(
    store: DuckDBStore,
    trading_date: date,
) -> dict[str, object]:
    df = store.query_index_daily(trading_date)
    if df.empty:
        return {}

    out: dict[str, object] = {}
    for _, row in df.iterrows():
        label = INDEX_CONTEXT_LABELS.get(str(row["ts_code"]))
        if label is None:
            continue
        out[f"index_{label}_pct_chg"] = row["pct_chg"]
        out[f"index_{label}_close"] = row["close"]
    return out


def _minute_quote(row: pd.Series) -> _MinuteQuote:
    return _MinuteQuote(
        ts_code=str(row["ts_code"]),
        price=float(row["close"]),
        low=float(row["low"]),
        high=float(row["high"]) if pd.notna(row["high"]) else None,
    )


def _execution_quote(row: pd.Series) -> _MinuteQuote:
    price = float(row["open"])
    return _MinuteQuote(
        ts_code=str(row["ts_code"]),
        price=price,
        low=price,
        high=price,
    )


def _risk_replay_quote(row: pd.Series) -> RiskReplayQuote:
    return RiskReplayQuote(
        ts_code=str(row["ts_code"]),
        trade_time=_as_datetime(row["trade_time"]),
        price=float(row["close"]),
        low=float(row["low"]),
        high=float(row["high"]) if pd.notna(row["high"]) else None,
    )


def _execution_replay_quote(row: pd.Series) -> RiskReplayQuote:
    price = float(row["open"])
    return RiskReplayQuote(
        ts_code=str(row["ts_code"]),
        trade_time=_as_datetime(row["trade_time"]),
        price=price,
        low=price,
        high=price,
    )


def _replay_watch_item(item: _ReplayItem) -> ReplayWatchItem:
    return ReplayWatchItem(
        ts_code=item.ts_code,
        pool=item.pool,
        name=item.name,
        entry_date=item.entry_date,
        reference_date=item.reference_date,
        limit_up_date=item.limit_up_date,
        t_close=item.t_close,
        t_high=item.t_high,
        limit_up_price_next=item.limit_up_price_next,
        stop_weak=item.stop_weak,
    )


def _basis_checks_for_window(
    store: DuckDBStore,
    ts_code: str,
    window_dates: list[date],
) -> tuple[PriceBasisCheck, ...]:
    checks: list[PriceBasisCheck] = []
    for trading_date in window_dates[1:]:
        previous_date = _previous_window_date(window_dates, trading_date)
        if previous_date is None:
            continue
        previous_daily = _query_daily_bar(store, ts_code, previous_date)
        current_daily = _query_daily_bar(store, ts_code, trading_date)
        if previous_daily is None or current_daily is None:
            continue
        checks.append(
            PriceBasisCheck(
                trade_date=trading_date,
                previous_close=(
                    float(previous_daily["close"])
                    if pd.notna(previous_daily["close"])
                    else None
                ),
                current_pre_close=(
                    float(current_daily["pre_close"])
                    if pd.notna(current_daily["pre_close"])
                    else None
                ),
            )
        )
    return tuple(checks)


def _find_entry_snapshot(
    store: DuckDBStore,
    item: _ReplayItem,
    minutes: pd.DataFrame,
    buy_date: date,
    earliest_exit_date: date,
    window_dates: list[date],
    config: MinuteReplayConfig,
    volume_profiles: list[VolumeProfile],
    static_factors: dict[str, float | int | None] | None = None,
) -> ReplayEntrySnapshot | None:
    if item.t_close is None or item.t_high is None:
        return None

    day_minutes = minutes[
        pd.to_datetime(minutes["trade_time"]).dt.date == buy_date
    ].sort_values("trade_time").reset_index(drop=True)
    if day_minutes.empty:
        return None

    cum_low = float("inf")
    cum_high = float("-inf")
    cum_vol = 0.0
    cum_amount = 0.0
    signal_seen = False
    signal_time: datetime | None = None
    amount_history: list[float] = []
    clocked_amount_history: list[tuple[time, float]] = []

    def _build_snapshot(
        level: str,
        signal_index: int,
        signal_quote: _MinuteQuote,
        signal_features: dict[str, object] | None = None,
    ) -> ReplayEntrySnapshot | None:
        execution_index = signal_index + 1
        if execution_index >= len(day_minutes):
            return None
        execution_row = day_minutes.iloc[execution_index]
        execution_quote = _execution_quote(execution_row)
        execution_time = _as_datetime(execution_row["trade_time"])
        planned_entry_price = execution_quote.price * (
            1 + config.paper.entry_slippage_pct
        )
        risk_plan = build_volume_profile_risk_plan(
            volume_profiles,
            entry_price=planned_entry_price,
            config=config.volume_profile,
        )
        if not risk_plan.entry_allowed:
            return None
        if signal_features:
            risk_plan = risk_plan.model_copy(update={
                "payload": {**risk_plan.payload, **signal_features}
            })
        signal = {
            "level": level,
            "trigger_type": level,
            "level_price": item.t_high,
            "signal_price": signal_quote.price,
        }
        all_minutes = minutes.sort_values("trade_time")
        exit_rows = all_minutes[
            pd.to_datetime(all_minutes["trade_time"]) >= execution_time
        ]
        return ReplayEntrySnapshot(
            item=_replay_watch_item(item),
            execution_quote=_execution_replay_quote(execution_row),
            signal=signal,
            entry_time=execution_time,
            earliest_exit_date=earliest_exit_date,
            window_dates=tuple(window_dates),
            exit_quotes=tuple(
                _risk_replay_quote(exit_row) for _, exit_row in exit_rows.iterrows()
            ),
            risk_plan=risk_plan,
            basis_checks=_basis_checks_for_window(store, item.ts_code, window_dates),
        )

    for idx, row in day_minutes.iterrows():
        quote_time = _as_datetime(row["trade_time"])
        quote = _minute_quote(row)
        minute_amount = float(row["amount"]) if pd.notna(row["amount"]) else 0.0
        prior_amounts = [
            amount
            for amount in amount_history[-config.amount_surge_lookback:]
            if amount > 0
        ]
        average_prior_amount = (
            sum(prior_amounts) / len(prior_amounts) if prior_amounts else None
        )
        is_amount_surge = (
            average_prior_amount is not None
            and len(prior_amounts) >= config.amount_surge_min_prior_minutes
            and minute_amount >= average_prior_amount * config.amount_surge_ratio
        )
        cum_low = min(cum_low, float(row["low"]))
        cum_high = max(cum_high, float(row["high"]))
        if pd.notna(row["vol"]) and pd.notna(row["amount"]):
            cum_vol += float(row["vol"])
            cum_amount += float(row["amount"])
        vwap = cum_amount / cum_vol if cum_vol > 0 else None
        is_strong_carry = (
            cum_low >= item.t_close * config.carry_low_ratio
            and quote.price >= item.t_close * config.carry_close_ratio
        )
        is_break_high = cum_high > item.t_high * config.break_high_ratio
        is_signal = is_strong_carry and is_break_high
        is_above_vwap = (
            vwap is None or quote.price >= vwap * (1 + config.vwap_buffer_pct)
        )

        if is_signal:
            if not signal_seen:
                signal_seen = True
                signal_time = quote_time
            if config.entry_mode == "first_break":
                signal_features = build_intraday_relative_volume_features(
                    store,
                    item.ts_code,
                    quote_time,
                    current_minute_amount=minute_amount,
                    current_cum_amount=cum_amount,
                    current_day_amounts=clocked_amount_history,
                    freq=config.freq,
                )
                snapshot = _build_snapshot(
                    "minute_strong_carry_break_t_high",
                    idx,
                    quote,
                    signal_features,
                )
                if snapshot is not None:
                    return snapshot
            if config.entry_mode == "vwap_confirm" and is_above_vwap:
                signal_features = build_intraday_relative_volume_features(
                    store,
                    item.ts_code,
                    quote_time,
                    current_minute_amount=minute_amount,
                    current_cum_amount=cum_amount,
                    current_day_amounts=clocked_amount_history,
                    freq=config.freq,
                )
                snapshot = _build_snapshot(
                    "minute_vwap_confirm",
                    idx,
                    quote,
                    signal_features,
                )
                if snapshot is not None:
                    return snapshot
            if config.entry_mode == "factor_confirm":
                # 宽门（is_signal）沿用 first_break 同门；确认层由 n_shape_b_v1
                # 加权分过阈值决定。观察因子（rel_amount/温度）不进权重表，
                # 得分只由静态因子 + vwap_position 决定，故过阈值后才查相对放量库。
                vwap_position = (
                    quote.price / vwap if vwap is not None and vwap > 0 else None
                )
                raw_factor_values: dict[str, object] = {
                    **(static_factors or {}),
                    "vwap_position": vwap_position,
                }
                factor_score = score_feature_terms(
                    raw_factor_values, N_SHAPE_B_V1_SCORE_TERMS
                )
                if factor_score >= config.factor_score_threshold:
                    rel_features = build_intraday_relative_volume_features(
                        store,
                        item.ts_code,
                        quote_time,
                        current_minute_amount=minute_amount,
                        current_cum_amount=cum_amount,
                        current_day_amounts=clocked_amount_history,
                        freq=config.freq,
                    )
                    raw_factor_values["rel_amount_same_minute_20d"] = rel_features.get(
                        "signal_rel_amount_same_minute_20d"
                    )
                    matrix = build_signal_factors(
                        raw_factor_values, factor_set=N_SHAPE_V1
                    )
                    signal_features = {
                        **rel_features,
                        **raw_factor_values,
                        "n_shape_factor_score": factor_score,
                        "n_shape_factor_score_threshold": config.factor_score_threshold,
                        "n_shape_factor_hit_count": matrix.hit_count,
                        "n_shape_factor_evaluated_count": matrix.evaluated_count,
                        "n_shape_signal_factors": matrix.to_json_payload(),
                    }
                    snapshot = _build_snapshot(
                        "minute_factor_confirm",
                        idx,
                        quote,
                        signal_features,
                    )
                    if snapshot is not None:
                        return snapshot
            if (
                config.entry_mode == "amount_surge"
                and is_above_vwap
                and is_amount_surge
            ):
                signal_features = build_intraday_relative_volume_features(
                    store,
                    item.ts_code,
                    quote_time,
                    current_minute_amount=minute_amount,
                    current_cum_amount=cum_amount,
                    current_day_amounts=clocked_amount_history,
                    freq=config.freq,
                )
                snapshot = _build_snapshot(
                    "minute_amount_surge",
                    idx,
                    quote,
                    signal_features,
                )
                if snapshot is not None:
                    return snapshot

        if not signal_seen or signal_time is None:
            amount_history.append(minute_amount)
            clocked_amount_history.append((quote_time.time(), minute_amount))
            continue

        if config.entry_mode == "break_retest":
            touched_break_level = float(row["low"]) <= item.t_high * (
                1 + config.retest_tolerance_pct
            )
            reclaimed_break_level = quote.price >= item.t_high
            if (
                quote_time > signal_time
                and touched_break_level
                and reclaimed_break_level
                and is_above_vwap
            ):
                signal_features = build_intraday_relative_volume_features(
                    store,
                    item.ts_code,
                    quote_time,
                    current_minute_amount=minute_amount,
                    current_cum_amount=cum_amount,
                    current_day_amounts=clocked_amount_history,
                    freq=config.freq,
                )
                snapshot = _build_snapshot(
                    "minute_break_retest",
                    idx,
                    quote,
                    signal_features,
                )
                if snapshot is not None:
                    return snapshot

        if (
            config.entry_mode == "late_confirm"
            and quote_time.time() >= config.late_confirm_at
            and quote.price >= item.t_high
            and is_above_vwap
        ):
            signal_features = build_intraday_relative_volume_features(
                store,
                item.ts_code,
                quote_time,
                current_minute_amount=minute_amount,
                current_cum_amount=cum_amount,
                current_day_amounts=clocked_amount_history,
                freq=config.freq,
            )
            snapshot = _build_snapshot(
                "minute_late_confirm",
                idx,
                quote,
                signal_features,
            )
            if snapshot is not None:
                return snapshot

        amount_history.append(minute_amount)
        clocked_amount_history.append((quote_time.time(), minute_amount))

    return None


def _volume_profiles_for_item(
    store: DuckDBStore,
    item: _ReplayItem,
    config: MinuteReplayConfig,
    price_basis_ratio: float,
    buy_date: date,
) -> list[VolumeProfile]:
    if not config.volume_profile.enabled:
        return []
    if item.reference_date is None:
        return []

    profiles: list[VolumeProfile] = []
    for lookback_days in config.volume_profile.lookback_days:
        profile = calculate_volume_profile(
            store,
            item.ts_code,
            reference_date=item.reference_date,
            lookback_days=lookback_days,
            freq=config.freq,
        )
        if profile is None:
            continue
        profiles.append(
            scale_volume_profile(
                profile,
                price_basis_ratio,
                target_reference_date=buy_date,
            )
        )
    return profiles


def _adjust_item_to_buy_basis(
    store: DuckDBStore,
    item: _ReplayItem,
    buy_date: date,
    threshold: float,
) -> _ReplayItem:
    if item.t_close is None:
        return item

    buy_daily = _query_daily_bar(store, item.ts_code, buy_date)
    if buy_daily is None or pd.isna(buy_daily["pre_close"]):
        return item

    adjustment = resolve_price_basis_adjustment(
        item.t_close,
        float(buy_daily["pre_close"]),
        threshold_pct=threshold,
    )
    if not adjustment.adjusted:
        return item

    return replace(
        item,
        t_close=adjustment.adjust(item.t_close),
        t_high=adjustment.adjust(item.t_high),
        limit_up_price_next=adjustment.adjust(item.limit_up_price_next),
        stop_weak=float(adjustment.adjust(item.stop_weak) or 0),
    )


def _previous_window_date(window_dates: list[date], trading_date: date) -> date | None:
    try:
        idx = window_dates.index(trading_date)
    except ValueError:
        return None
    if idx <= 0:
        return None
    return window_dates[idx - 1]


def _close_position(
    position: PaperPosition,
    exit_time: datetime,
    exit_price: float,
    exit_reason: str,
    holding_trading_days: int,
) -> PaperPosition:
    pnl_pct = round((exit_price / position.entry_price - 1) * 100, 4)
    return position.model_copy(update={
        "status": "closed",
        "exit_time": exit_time,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "holding_trading_days": holding_trading_days,
        "pnl_pct": pnl_pct,
    })


def _run_exit_scan(
    store: DuckDBStore,
    position: PaperPosition,
    minutes: pd.DataFrame,
    window_dates: list[date],
    config: MinuteReplayConfig,
) -> PaperPosition | None:
    if minutes.empty:
        return None

    scan_minutes = minutes.sort_values("trade_time")
    active_date = position.entry_time.date()
    for _, row in scan_minutes.iterrows():
        quote_time = _as_datetime(row["trade_time"])
        if quote_time < position.entry_time:
            continue

        quote_date = quote_time.date()
        if quote_date != active_date:
            previous_date = _previous_window_date(window_dates, quote_date)
            previous_daily = (
                _query_daily_bar(store, position.ts_code, previous_date)
                if previous_date is not None
                else None
            )
            current_daily = _query_daily_bar(store, position.ts_code, quote_date)
            if (
                previous_daily is not None
                and current_daily is not None
                and pd.notna(previous_daily["close"])
                and pd.notna(current_daily["pre_close"])
            ):
                position = adjust_open_position_price_basis(
                    position,
                    float(previous_daily["close"]),
                    float(current_daily["pre_close"]),
                )
            active_date = quote_date

        quote = _minute_quote(row)
        exit_event = check_position_exit(position, quote, quote_time, config.paper)
        if exit_event is None:
            position = mark_position_to_quote(position, quote)
            continue

        holding_days = sum(
            1
            for trading_date in window_dates
            if position.trade_date < trading_date <= exit_event.exit_time.date()
        )
        return _close_position(
            position,
            exit_event.exit_time,
            exit_event.exit_price,
            exit_event.exit_reason,
            holding_days,
        )

    if scan_minutes.empty:
        return None

    last = scan_minutes.iloc[-1]
    last_time = _as_datetime(last["trade_time"])
    holding_days = sum(
        1
        for trading_date in window_dates
        if position.trade_date < trading_date <= last_time.date()
    )
    return _close_position(
        position,
        last_time,
        float(last["close"]),
        f"time_{config.max_hold_days}d",
        holding_days,
    )


def _position_to_row(position: PaperPosition) -> dict[str, object]:
    risk_payload = position.risk_payload or {}
    return {
        "ts_code": position.ts_code,
        "name": position.name,
        "pool": position.pool,
        "signal_date": position.entry_t_date,
        "buy_date": position.trade_date,
        "entry_time": position.entry_time,
        "entry_price_raw": position.entry_price_raw,
        "entry_price": position.entry_price,
        "entry_signal": position.entry_signal,
        "stop_loss_price": position.stop_loss_price,
        "stop_loss_basis": position.stop_loss_basis,
        "take_profit_price": position.take_profit_price,
        "take_profit_basis": position.take_profit_basis,
        "trailing_stop_price": position.trailing_stop_price,
        "exit_time": position.exit_time,
        "exit_price": position.exit_price,
        "exit_reason": position.exit_reason,
        "holding_trading_days": position.holding_trading_days,
        "ret_pct": position.pnl_pct,
        "candidate_id": position.candidate_id,
        "volume_profile_lookbacks": ",".join(
            str(value) for value in risk_payload.get("lookbacks_used", [])
        ),
        "volume_profile_rr": risk_payload.get("reward_risk"),
        "volume_profile_poc_reclaimed": risk_payload.get("reclaimed_poc_count"),
        "signal_minute_amount": risk_payload.get("signal_minute_amount"),
        "signal_cum_amount_asof": risk_payload.get("signal_cum_amount_asof"),
        "hist_same_minute_amount_median_20d": risk_payload.get(
            "hist_same_minute_amount_median_20d"
        ),
        "hist_cum_amount_asof_median_20d": risk_payload.get(
            "hist_cum_amount_asof_median_20d"
        ),
        "signal_rel_amount_same_minute_20d": risk_payload.get(
            "signal_rel_amount_same_minute_20d"
        ),
        "signal_rel_cum_amount_asof_20d": risk_payload.get(
            "signal_rel_cum_amount_asof_20d"
        ),
        "hist_intraday_days_20d": risk_payload.get("hist_intraday_days_20d"),
        "signal_opening_segment": risk_payload.get("signal_opening_segment"),
        "signal_opening_segment_amount": risk_payload.get(
            "signal_opening_segment_amount"
        ),
        "signal_amount_accel_5m": risk_payload.get("signal_amount_accel_5m"),
        "signal_amount_accel_10m": risk_payload.get("signal_amount_accel_10m"),
    }


def build_minute_replay_entry_snapshots(
    store: DuckDBStore,
    *,
    start_date: str | date,
    end_date: str | date,
    preset_name: str = "n-shape-pool1",
    freq: MinuteFreq = "1min",
    entry_mode: EntryMode = "first_break",
    max_hold_days: int = 5,
    paper_config: PaperTradeConfig | None = None,
    volume_profile_config: VolumeProfileRuleConfig | None = None,
    factor_score_threshold: float = DEFAULT_FACTOR_SCORE_THRESHOLD,
) -> list[ReplayEntrySnapshot]:
    """生成分钟 replay 的入场快照，供后续风控参数重放。"""
    config = MinuteReplayConfig(
        preset_name=preset_name,
        freq=freq,
        entry_mode=entry_mode,
        max_hold_days=max_hold_days,
        factor_score_threshold=factor_score_threshold,
        paper=paper_config or PaperTradeConfig(),
        volume_profile=volume_profile_config or VolumeProfileRuleConfig(),
    )
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    calendar = _trading_dates(store)
    candidates = _query_candidates(store, start, end, config.preset_name)
    if candidates.empty or not calendar:
        return []

    snapshots: list[ReplayEntrySnapshot] = []
    for _, candidate in candidates.iterrows():
        t_date = _as_date(candidate["trade_date"])
        buy_date = _next_trading_date(calendar, t_date)
        if buy_date is None:
            continue
        earliest_exit_date = _next_trading_date(calendar, buy_date)
        if earliest_exit_date is None:
            continue

        window_dates = _window_trading_dates(calendar, buy_date, config.max_hold_days)
        if len(window_dates) <= 1:
            continue
        last_window_date = window_dates[-1]

        daily_t = _query_daily_bar(store, str(candidate["ts_code"]), t_date)
        if daily_t is None:
            continue

        buy_start, _ = _day_bounds(buy_date)
        _, window_end = _day_bounds(last_window_date)
        minutes = store.query_minute_bars(
            str(candidate["ts_code"]),
            buy_start,
            window_end,
            freq=config.freq,
        )
        if minutes.empty:
            continue

        raw_item = _ReplayItem(
            ts_code=str(candidate["ts_code"]),
            name=str(candidate["name"] or ""),
            pool="pool1" if str(candidate["preset_name"]).endswith("pool1") else "pool2",
            entry_date=t_date,
            reference_date=t_date,
            limit_up_date=t_date,
            t_close=float(daily_t["close"]),
            t_high=float(daily_t["high"]),
            limit_up_price_next=None,
            stop_weak=0,
        )
        item = _adjust_item_to_buy_basis(
            store,
            raw_item,
            buy_date,
            config.price_discontinuity_pct,
        )
        price_basis_ratio = (
            item.t_close / raw_item.t_close
            if item.t_close is not None and raw_item.t_close not in (None, 0)
            else 1.0
        )
        volume_profiles = _volume_profiles_for_item(
            store,
            raw_item,
            config,
            float(price_basis_ratio),
            buy_date,
        )
        static_factors: dict[str, float | int | None] | None = None
        if config.entry_mode == "factor_confirm":
            static_factors = _prefetch_nshape_static_factors(
                store,
                ts_code=item.ts_code,
                t_date=t_date,
                buy_date=buy_date,
                calendar=calendar,
                t_close_raw=float(daily_t["close"]),
            )
        snapshot = _find_entry_snapshot(
            store,
            item,
            minutes,
            buy_date,
            earliest_exit_date,
            window_dates,
            config,
            volume_profiles,
            static_factors,
        )
        if snapshot is None:
            continue

        feature_payload: dict[str, object] = {}
        if item.reference_date is not None:
            feature_payload.update(
                build_daily_stock_features(store, item.ts_code, item.reference_date)
            )
            feature_payload.update(_query_market_sentiment(store, item.reference_date))
            feature_payload.update(_query_index_context(store, item.reference_date))
        if config.entry_mode == "factor_confirm":
            risk_payload = snapshot.risk_plan.payload
            for key in (
                "n_shape_factor_score",
                "n_shape_factor_score_threshold",
                "n_shape_factor_hit_count",
                "n_shape_factor_evaluated_count",
            ):
                feature_payload[key] = risk_payload.get(key)
        snapshots.append(snapshot.model_copy(update={"feature_payload": feature_payload}))

    return snapshots


def run_minute_strong_carry_replay(
    store: DuckDBStore,
    *,
    start_date: str | date,
    end_date: str | date,
    preset_name: str = "n-shape-pool1",
    freq: MinuteFreq = "1min",
    entry_mode: EntryMode = "first_break",
    max_hold_days: int = 5,
    paper_config: PaperTradeConfig | None = None,
    volume_profile_config: VolumeProfileRuleConfig | None = None,
    factor_score_threshold: float = DEFAULT_FACTOR_SCORE_THRESHOLD,
) -> pd.DataFrame:
    """回放 Pool 候选在次交易日盘中的强承接突破信号。

    信号定义为：从 B 日开盘到当前分钟的累计最低价不破 T 日收盘价，
    当前价仍不低于 T 日收盘价，且累计最高价突破 T 日高点。
    """
    paper = paper_config or PaperTradeConfig()
    snapshots = build_minute_replay_entry_snapshots(
        store,
        start_date=start_date,
        end_date=end_date,
        preset_name=preset_name,
        freq=freq,
        entry_mode=entry_mode,
        max_hold_days=max_hold_days,
        paper_config=paper,
        volume_profile_config=volume_profile_config,
        factor_score_threshold=factor_score_threshold,
    )
    return replay_entry_snapshots_to_trades(snapshots, paper)
